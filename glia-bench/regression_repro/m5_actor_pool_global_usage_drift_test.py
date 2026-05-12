"""Regression test for M5 / cluster-autoscaler interaction.

The cluster autoscaler v2 (`RollingLogicalUtilizationGauge`) reads
`ResourceManager.get_global_usage()` to compute GPU/CPU utilization. M5's
incremental `on_task_dispatched(op)` hook updates `_global_usage` by
`op.incremental_resource_usage()`, which is `(0, 0, 0)` for actor-pool ops
because submitting to an existing actor reserves nothing new.

That's fine *if* nothing else changes between scheduling-step boundaries.
But when new actors come online mid-step (cluster scaled up,
ActorPoolMapOperator brought up another worker), `op.current_logical_usage()`
grows while `_global_usage` stays stale until the next outer
`update_usages()` snapshot.

Pristine master called `update_usages()` after every dispatch, so it picked
up actor-pool size changes immediately. v5 only refreshes at scheduling-step
boundaries.

This test pins the divergence as a single-file invariant violation. Run:

    /opt/venv/bin/python glia-bench/regression_repro/m5_actor_pool_global_usage_drift_test.py

Expected: AssertionError on the current v5 HEAD. Should pass after the fix.
"""
import sys
from unittest.mock import MagicMock

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


def main():
    ctx = DataContext.get_current()
    input_op = InputDataBuffer(ctx, MagicMock())

    # Actor-pool-like op:
    #   - incremental_resource_usage = (0,0,0): dispatching to an existing
    #     actor reserves no Ray-core resources (this is the actor-pool case)
    #   - current_logical_usage = (0, num_actors * 1.0 gpu, 0): one GPU
    #     per running actor in the pool
    actor_count = {"running_actors": 4}
    op = mock_map_op(input_op=input_op, ray_remote_args={"num_cpus": 0, "num_gpus": 1})
    op.incremental_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=0, gpu=0)
    )
    op.current_logical_usage = MagicMock(
        side_effect=lambda: ExecutionResources(
            cpu=0, gpu=float(actor_count["running_actors"])
        )
    )
    op.running_logical_usage = MagicMock(
        side_effect=lambda: ExecutionResources(
            cpu=0, gpu=float(actor_count["running_actors"])
        )
    )
    op.pending_logical_usage = MagicMock(return_value=ExecutionResources.zero())

    topo = build_streaming_topology(op, ExecutionOptions())
    rm = ResourceManager(
        topo,
        ExecutionOptions(),
        MagicMock(
            return_value=ExecutionResources(
                cpu=16, gpu=16, object_store_memory=10 ** 11
            )
        ),
        ctx,
    )

    # --- Step 1: outer update_usages snapshots the initial state. ---
    rm.update_usages()
    assert rm.get_global_usage().gpu == 4.0, (
        f"Expected initial global GPU usage = 4.0, got {rm.get_global_usage().gpu}"
    )

    # --- Step 2: simulate cluster scale-up: 4 more actors come online. ---
    # On master this would happen between dispatches, and the NEXT
    # update_usages() (which master calls per dispatch) would pick it up.
    # On v5, update_usages only runs at scheduling-step boundaries; the
    # incremental on_task_dispatched hook is the only thing updating
    # _global_usage between those.
    actor_count["running_actors"] = 8

    # --- Step 3: dispatch a few tasks (mid-scheduling-step). ---
    # The per-dispatch hook differs by tree:
    #  - master: after every dispatch the scheduler calls `update_usages()`,
    #    which walks the topology and refreshes `_global_usage` from
    #    `op.current_logical_usage()`. So master picks up the new actors.
    #  - v5 (M5): after every dispatch the scheduler calls
    #    `on_task_dispatched(op)`, which for an actor-pool op applies a
    #    delta of (0,0,0) to `_global_usage`. So v5 does NOT pick up the
    #    new actors until the next scheduling-step boundary fires
    #    `update_usages()`.
    # We dispatch three times via whichever per-dispatch hook the tree has,
    # so this same test exercises master's behavior and v5's behavior with
    # no other changes.
    per_dispatch = getattr(rm, "on_task_dispatched", None)
    if per_dispatch is None:
        per_dispatch = lambda _op: rm.update_usages()  # master path
        tree_label = "master (per-dispatch update_usages)"
    else:
        tree_label = "v5 (M5 on_task_dispatched)"
    print(f"# tree under test: {tree_label}")
    per_dispatch(op)
    per_dispatch(op)
    per_dispatch(op)

    # --- Step 4: what does the consumer (cluster autoscaler) see now? ---
    # The cluster autoscaler reads get_global_usage() at this point, divides
    # by global_limits, and observes the rolling-window average. If we're
    # stale here, the autoscaler under-reports utilization.
    observed_global_gpu = rm.get_global_usage().gpu

    # The structural invariant: _global_usage should reflect the topology's
    # current logical usage. The full-recompute path (update_usages) would
    # produce the correct value; the incremental path should converge to
    # the same value within the same scheduling step. Anything else is
    # observable drift to external readers.
    rm.update_usages()
    fresh_global_gpu = rm.get_global_usage().gpu

    assert fresh_global_gpu == 8.0, (
        f"sanity: after update_usages with 8 actors running, expected "
        f"global GPU = 8.0, got {fresh_global_gpu}"
    )

    assert observed_global_gpu == fresh_global_gpu, (
        f"REGRESSION: _global_usage.gpu after on_task_dispatched on an "
        f"actor-pool op should reflect the topology's current logical GPU "
        f"usage ({fresh_global_gpu}). Got {observed_global_gpu}. "
        f"Cluster-autoscaler readers (RollingLogicalUtilizationGauge) "
        f"observe the stale value between scheduling-step boundaries, "
        f"under-reporting GPU utilization and suppressing scale-up."
    )

    print("PASS: _global_usage stays in sync with actor-pool size changes "
          "between scheduling-step boundaries.")


if __name__ == "__main__":
    try:
        main()
    except AssertionError as e:
        print(f"FAIL: {e}")
        sys.exit(1)
