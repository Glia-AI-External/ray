"""Comprehensive semantic-equivalence regression tests for M5.

Master's `ResourceManager.update_usages()` (called after every dispatch on
master) is the ground truth for what every consumer would see post-dispatch.
v5's M5 replaces that with a `on_task_dispatched(op)` incremental hook
which only updates a subset of state. This file pins every divergence we
know about, by running parallel ResourceManagers — one driven via M5's
hook, the other via the pristine per-dispatch full recompute — and
comparing the fields each consumer reads.

Each test:
  - PASSES on master tree (trivially: no `on_task_dispatched` to diverge)
  - FAILS on v5 HEAD (catches the divergence)
  - Should PASS after the fix

Run:
    /opt/venv/bin/python glia-bench/regression_repro/m5_semantic_equivalence_tests.py
"""
import sys
import traceback
from unittest.mock import MagicMock, PropertyMock, patch

from ray.data._internal.execution.interfaces.execution_options import (
    ExecutionOptions,
    ExecutionResources,
)
from ray.data._internal.execution.operators.input_data_buffer import (
    InputDataBuffer,
)
from ray.data._internal.execution.resource_manager import ResourceManager
from ray.data._internal.execution.streaming_executor_state import (
    build_streaming_topology,
)
from ray.data.context import DataContext
from ray.data.tests.test_resource_manager import mock_map_op


# ---- helpers ----------------------------------------------------------------


def _build_rm(terminal_op, *, cpu_limit=16, gpu_limit=16, obj_store_limit=10 ** 11):
    """Build a ResourceManager with a given topology and ample limits."""
    topo = build_streaming_topology(terminal_op, ExecutionOptions())
    return ResourceManager(
        topo,
        ExecutionOptions(),
        MagicMock(
            return_value=ExecutionResources(
                cpu=cpu_limit,
                gpu=gpu_limit,
                object_store_memory=obj_store_limit,
            )
        ),
        DataContext.get_current(),
    )


def _per_dispatch_hook(rm):
    """Return the per-dispatch callable for whichever tree we're on.

    - On v5: `rm.on_task_dispatched`.
    - On master (no M5): fall back to `rm.update_usages` (a full recompute
      per dispatch), which is master's actual semantic.
    """
    hook = getattr(rm, "on_task_dispatched", None)
    if hook is None:
        return lambda _op: rm.update_usages()
    return hook


# ---- tests ------------------------------------------------------------------


def test_A_global_usage_actor_pool_size_drift(ctx, input_op):
    """Divergence A: `_global_usage.{cpu,gpu,memory}` doesn't pick up
    `op.current_logical_usage()` changes between dispatches.

    When the cluster autoscaler brings a new GPU actor online mid-step,
    `op.current_logical_usage()` grows. Master's per-dispatch update_usages
    re-reads it; M5's hook applies the dispatch delta `(0,0,0)` for actor
    ops and doesn't touch the running actor count.

    Consumer: `RollingLogicalUtilizationGauge.observe()` in
    `cluster_autoscaler/`. It computes `util = global_usage / global_limits`
    and the cluster autoscaler v2 only requests scale-up when util ≥ 0.75.
    Stale `_global_usage.gpu` keeps the cluster from scaling up.
    """
    actor_count = {"running": 4}
    op = mock_map_op(input_op=input_op, ray_remote_args={"num_cpus": 0, "num_gpus": 1})
    op.incremental_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=0, gpu=0)
    )
    op.current_logical_usage = MagicMock(
        side_effect=lambda: ExecutionResources(
            cpu=0, gpu=float(actor_count["running"])
        )
    )
    op.running_logical_usage = MagicMock(
        side_effect=lambda: ExecutionResources(
            cpu=0, gpu=float(actor_count["running"])
        )
    )
    op.pending_logical_usage = MagicMock(return_value=ExecutionResources.zero())

    rm = _build_rm(op)
    rm.update_usages()
    assert rm.get_global_usage().gpu == 4.0, rm.get_global_usage()

    # Cluster brings up 4 more actors mid-step.
    actor_count["running"] = 8

    per_dispatch = _per_dispatch_hook(rm)
    per_dispatch(op)
    per_dispatch(op)
    per_dispatch(op)

    observed = rm.get_global_usage().gpu
    assert observed == 8.0, (
        f"A: _global_usage.gpu after dispatches with 8 running actors "
        f"should be 8.0, got {observed}. The cluster-autoscaler util "
        f"gauge reads this and would suppress scale-up."
    )


def test_B_upstream_object_store_memory_after_downstream_dispatch(ctx, input_op):
    """Divergence B: `_op_usages[upstream].object_store_memory` doesn't
    grow when downstream dispatches and `obj_store_mem_pending_task_inputs`
    grows on downstream.

    `_estimate_object_store_memory_usage(upstream)` includes the cross-op
    term `sum(downstream_op.obj_store_mem_pending_task_inputs)`. When
    downstream dispatches, that term grows. Master's update_usages
    recomputes this; M5's hook only updates the dispatching op.

    Consumers (within step): DefaultRanker reads
    `_op_usages[op].object_store_memory` when picking which op to dispatch
    next. DownstreamCapacityBackpressurePolicy and
    ConcurrencyCapBackpressurePolicy also read `_op_usages[upstream]`.
    Stale value → wrong scheduling and backpressure decisions.
    """
    upstream = mock_map_op(input_op=input_op, ray_remote_args={"num_cpus": 1})
    downstream = mock_map_op(input_op=upstream, ray_remote_args={"num_cpus": 1})

    upstream.incremental_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=1, gpu=0)
    )
    upstream.current_logical_usage = MagicMock(
        return_value=ExecutionResources(cpu=0, gpu=0)
    )
    upstream.running_logical_usage = MagicMock(
        return_value=ExecutionResources(cpu=0, gpu=0)
    )
    upstream.pending_logical_usage = MagicMock(
        return_value=ExecutionResources.zero()
    )

    downstream.incremental_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=1, gpu=0)
    )
    downstream.current_logical_usage = MagicMock(
        return_value=ExecutionResources(cpu=0, gpu=0)
    )
    downstream.running_logical_usage = MagicMock(
        return_value=ExecutionResources(cpu=0, gpu=0)
    )
    downstream.pending_logical_usage = MagicMock(
        return_value=ExecutionResources.zero()
    )

    # downstream's pending_task_inputs grows when we "dispatch" — simulate
    # by mutating the underlying counter.
    downstream_input_bytes = {"v": 0}
    with patch.object(
        type(downstream.metrics),
        "obj_store_mem_pending_task_inputs",
        new_callable=PropertyMock,
        side_effect=lambda: downstream_input_bytes["v"],
    ), patch.object(
        type(downstream.metrics),
        "obj_store_mem_max_pending_output_per_task",
        new_callable=PropertyMock,
        return_value=4 * 1024 * 1024,  # 4 MiB
    ):
        rm = _build_rm(downstream)
        rm.update_usages()
        before_upstream_obj = rm._op_usages[upstream].object_store_memory

        # Simulate the dispatch: downstream's pending_task_inputs grows
        # by per-task-input estimate (using 32 MiB for a clear signal).
        per_task_input = 32 * 1024 * 1024
        downstream_input_bytes["v"] += per_task_input

        per_dispatch = _per_dispatch_hook(rm)
        per_dispatch(downstream)

        observed_upstream_obj = rm._op_usages[upstream].object_store_memory

        # Pristine recompute as ground truth for comparison.
        rm.update_usages()
        fresh_upstream_obj = rm._op_usages[upstream].object_store_memory

        assert fresh_upstream_obj > before_upstream_obj, (
            f"sanity: after downstream input grew by {per_task_input}, "
            f"pristine recompute should attribute more to upstream. "
            f"before={before_upstream_obj}, fresh={fresh_upstream_obj}"
        )
        assert observed_upstream_obj == fresh_upstream_obj, (
            f"B: After downstream dispatch, _op_usages[upstream]"
            f".object_store_memory should grow to reflect downstream's "
            f"new pending_task_inputs. Fresh: {fresh_upstream_obj}, "
            f"observed via M5 hook: {observed_upstream_obj}. "
            f"DefaultRanker / DownstreamCapacityBackpressurePolicy / "
            f"ConcurrencyCapBackpressurePolicy all read this for "
            f"upstream and see the stale value."
        )


def test_D_global_pending_usage_not_updated(ctx, input_op):
    """Divergence D: `_global_pending_usage` is never updated by the M5
    hook. Master's update_usages clears and rebuilds it from each op's
    `pending_logical_usage()`. M5 only touches `_global_usage` and
    `_global_running_usage`.

    Public API `ResourceManager.get_global_pending_usage()` returns this.
    """
    pending_state = {"cpu": 0.0}
    op = mock_map_op(input_op=input_op, ray_remote_args={"num_cpus": 1})
    op.incremental_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=1, gpu=0)
    )
    op.current_logical_usage = MagicMock(
        return_value=ExecutionResources(cpu=0, gpu=0)
    )
    op.running_logical_usage = MagicMock(
        return_value=ExecutionResources(cpu=0, gpu=0)
    )
    op.pending_logical_usage = MagicMock(
        side_effect=lambda: ExecutionResources(
            cpu=float(pending_state["cpu"]), gpu=0
        )
    )

    rm = _build_rm(op)
    rm.update_usages()
    assert rm.get_global_pending_usage().cpu == 0.0

    pending_state["cpu"] = 3.0  # 3 tasks queued, awaiting dispatch

    per_dispatch = _per_dispatch_hook(rm)
    per_dispatch(op)

    observed = rm.get_global_pending_usage().cpu
    assert observed == 3.0, (
        f"D: _global_pending_usage.cpu should reflect op.pending_logical_usage() "
        f"after the per-dispatch hook. Expected 3.0, got {observed}. M5's hook "
        f"never touches _global_pending_usage."
    )


def test_G_upstream_obj_store_mem_used_dashboard_metric(ctx, input_op):
    """Divergence G: `op._metrics.obj_store_mem_used` is set by master's
    update_usages for every op in topology. M5's hook only sets it for
    the dispatching op. Upstream ops' dashboard / DatasetStats metric is
    stale until the next outer boundary.

    Not a gating consumer, but visible to users via the Ray Data dashboard
    and DatasetStats.
    """
    upstream = mock_map_op(input_op=input_op, ray_remote_args={"num_cpus": 1})
    downstream = mock_map_op(input_op=upstream, ray_remote_args={"num_cpus": 1})

    for op in (upstream, downstream):
        op.incremental_resource_usage = MagicMock(
            return_value=ExecutionResources(cpu=1, gpu=0)
        )
        op.current_logical_usage = MagicMock(
            return_value=ExecutionResources(cpu=0, gpu=0)
        )
        op.running_logical_usage = MagicMock(
            return_value=ExecutionResources(cpu=0, gpu=0)
        )
        op.pending_logical_usage = MagicMock(
            return_value=ExecutionResources.zero()
        )

    downstream_input_bytes = {"v": 0}
    with patch.object(
        type(downstream.metrics),
        "obj_store_mem_pending_task_inputs",
        new_callable=PropertyMock,
        side_effect=lambda: downstream_input_bytes["v"],
    ), patch.object(
        type(downstream.metrics),
        "obj_store_mem_max_pending_output_per_task",
        new_callable=PropertyMock,
        return_value=4 * 1024 * 1024,
    ):
        rm = _build_rm(downstream)
        rm.update_usages()
        before_upstream_dashboard = upstream._metrics.obj_store_mem_used

        downstream_input_bytes["v"] = 32 * 1024 * 1024

        per_dispatch = _per_dispatch_hook(rm)
        per_dispatch(downstream)

        observed_upstream_dashboard = upstream._metrics.obj_store_mem_used

        rm.update_usages()
        fresh_upstream_dashboard = upstream._metrics.obj_store_mem_used

        assert fresh_upstream_dashboard > before_upstream_dashboard, (
            f"sanity: upstream dashboard metric should grow after downstream input grew"
        )
        assert observed_upstream_dashboard == fresh_upstream_dashboard, (
            f"G: upstream._metrics.obj_store_mem_used should reflect the "
            f"new cross-op term after downstream dispatch. Fresh: "
            f"{fresh_upstream_dashboard}, observed via M5: "
            f"{observed_upstream_dashboard}. Dashboard/DatasetStats "
            f"observability drifts behind reality."
        )


# ---- runner -----------------------------------------------------------------


def _show_tree_label():
    rm_has_hook = hasattr(ResourceManager, "on_task_dispatched")
    print(f"# tree under test: {'v5 (has on_task_dispatched)' if rm_has_hook else 'master (no on_task_dispatched)'}")
    return rm_has_hook


def main():
    _show_tree_label()
    ctx = DataContext.get_current()
    input_op = InputDataBuffer(ctx, MagicMock())

    tests = [
        ("A_global_usage_actor_pool_size_drift",
         test_A_global_usage_actor_pool_size_drift),
        ("B_upstream_object_store_memory_after_downstream_dispatch",
         test_B_upstream_object_store_memory_after_downstream_dispatch),
        ("D_global_pending_usage_not_updated",
         test_D_global_pending_usage_not_updated),
        ("G_upstream_obj_store_mem_used_dashboard_metric",
         test_G_upstream_obj_store_mem_used_dashboard_metric),
    ]

    results = []
    for name, fn in tests:
        # Fresh input_op per test to avoid topology reuse.
        fresh_input = InputDataBuffer(ctx, MagicMock())
        try:
            fn(ctx, fresh_input)
            results.append((name, "PASS", None))
        except AssertionError as e:
            results.append((name, "FAIL", str(e).splitlines()[0]))
        except Exception as e:
            results.append((name, "ERROR", f"{type(e).__name__}: {e}"))

    print()
    print(f"{'Divergence':70s} {'Result'}")
    print("-" * 90)
    for name, status, summary in results:
        print(f"{name:70s} {status}")
        if summary:
            print(f"  → {summary[:140]}")

    n_fail = sum(1 for _, s, _ in results if s != "PASS")
    return 1 if n_fail else 0


if __name__ == "__main__":
    sys.exit(main())
