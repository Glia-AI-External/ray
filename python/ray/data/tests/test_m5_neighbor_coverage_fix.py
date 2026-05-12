"""End-to-end test for the M5 neighbor-coverage fix.

Verifies that after each call to ``ResourceManager.on_task_dispatched(op)``,
``_global_usage`` reflects the topology's current ground-truth resource
usage — specifically, the sum of each op's ``current_logical_usage()``.
This is the field the cluster autoscaler's ``ResourceUtilizationGauge``
reads, and the field that the friend's sf100 cluster runs showed v5
under-reporting (driving the autoscaler to under-provision).

The pre-fix M5 hook applied a static ``_dispatch_delta(op)`` to
``_global_usage``, which zeroes out for actor-pool ops and never picks
up actor-pool size changes between scheduling-step boundaries. The fix
walks ``{op} ∪ op.input_dependencies`` and re-reads each one's
``current_logical_usage()``, then updates the globals by the actual
delta.
"""
from unittest.mock import MagicMock

import pytest

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


def _ground_truth_global_usage(ops):
    """Sum each op's current_logical_usage() — the value `update_usages()`
    would compute for `_global_usage` (modulo the object_store_memory
    term, which depends on real op-runtime-metric state not mocked here).
    """
    total = ExecutionResources.zero()
    for op in ops:
        total = total.add(op.current_logical_usage())
    return total


def test_global_usage_tracks_actor_pool_growth_under_realistic_dispatch_sequence():
    """End-to-end: 3-op pipeline, mixed task/actor pools, with actor
    pool growing between dispatches. After each call to the fix's
    ``on_task_dispatched(op)``, ``_global_usage`` (CPU and GPU)
    matches the ground truth computed from each op's
    ``current_logical_usage()``.

    Pipeline: Input -> Read (task pool, 1 cpu/task)
                    -> Preprocess (task pool, 1 cpu/task)
                    -> Predictor (actor pool, 1 gpu/actor)

    Sequence:
      1. Read dispatch (read_running: 0 -> 1).
      2. Preprocess dispatch (pre_running: 0 -> 1).
      3. Predictor actor pool scales 4 -> 6, then Predictor dispatches.
      4. Preprocess dispatch #2.

    After each step the test asserts:
      - ``rm.get_global_usage().cpu`` equals sum of ops' current CPU.
      - ``rm.get_global_usage().gpu`` equals sum of ops' current GPU.

    The actor-pool-growth event in step 3 is the case the pre-fix
    M5 missed.
    """
    ctx = DataContext.get_current()
    input_op = InputDataBuffer(ctx, MagicMock())
    read = mock_map_op(input_op=input_op, ray_remote_args={"num_cpus": 1})
    pre = mock_map_op(input_op=read, ray_remote_args={"num_cpus": 1})
    predictor = mock_map_op(
        input_op=pre, ray_remote_args={"num_cpus": 0, "num_gpus": 1}
    )

    state = {
        "read_running": 0,
        "pre_running": 0,
        "predictor_actors": 4,
        "predictor_running": 0,
    }

    read.incremental_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=1, gpu=0)
    )
    read.current_logical_usage = MagicMock(
        side_effect=lambda: ExecutionResources(cpu=float(state["read_running"]), gpu=0)
    )
    read.running_logical_usage = MagicMock(
        side_effect=lambda: ExecutionResources(cpu=float(state["read_running"]), gpu=0)
    )
    read.pending_logical_usage = MagicMock(return_value=ExecutionResources.zero())

    pre.incremental_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=1, gpu=0)
    )
    pre.current_logical_usage = MagicMock(
        side_effect=lambda: ExecutionResources(cpu=float(state["pre_running"]), gpu=0)
    )
    pre.running_logical_usage = MagicMock(
        side_effect=lambda: ExecutionResources(cpu=float(state["pre_running"]), gpu=0)
    )
    pre.pending_logical_usage = MagicMock(return_value=ExecutionResources.zero())

    predictor.incremental_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=0, gpu=0)
    )
    predictor.current_logical_usage = MagicMock(
        side_effect=lambda: ExecutionResources(
            cpu=0, gpu=float(state["predictor_actors"])
        )
    )
    predictor.running_logical_usage = MagicMock(
        side_effect=lambda: ExecutionResources(
            cpu=0, gpu=float(state["predictor_actors"])
        )
    )
    predictor.pending_logical_usage = MagicMock(return_value=ExecutionResources.zero())

    rm = ResourceManager(
        build_streaming_topology(predictor, ExecutionOptions()),
        ExecutionOptions(),
        MagicMock(
            return_value=ExecutionResources(
                cpu=64, gpu=64, object_store_memory=10**12
            )
        ),
        ctx,
    )

    ops = [read, pre, predictor]

    def assert_invariant(step_name):
        truth = _ground_truth_global_usage(ops)
        observed = rm.get_global_usage()
        assert observed.cpu == truth.cpu, (
            f"After step '{step_name}', _global_usage.cpu drifted: "
            f"observed={observed.cpu}, ground_truth={truth.cpu}. "
            f"State: {state}"
        )
        assert observed.gpu == truth.gpu, (
            f"After step '{step_name}', _global_usage.gpu drifted: "
            f"observed={observed.gpu}, ground_truth={truth.gpu}. "
            f"State: {state}"
        )

    # Initial alignment via update_usages.
    rm.update_usages()
    assert_invariant("initial update_usages")
    assert rm.get_global_usage().gpu == 4.0
    assert rm.get_global_usage().cpu == 0.0

    # Step 1: Read dispatch.
    state["read_running"] = 1
    rm.on_task_dispatched(read)
    assert_invariant("read dispatch")
    assert rm.get_global_usage().cpu == 1.0
    assert rm.get_global_usage().gpu == 4.0

    # Step 2: Preprocess dispatch.
    state["pre_running"] = 1
    rm.on_task_dispatched(pre)
    assert_invariant("preprocess dispatch")
    assert rm.get_global_usage().cpu == 2.0
    assert rm.get_global_usage().gpu == 4.0

    # Step 3: actor pool scaled 4 -> 6 between boundaries, then Predictor
    # dispatches. The fix re-reads predictor.current_logical_usage() and
    # picks up the new 6-actor count.
    state["predictor_actors"] = 6
    state["predictor_running"] = 1
    rm.on_task_dispatched(predictor)
    assert_invariant("predictor dispatch + actor scale-up 4->6")
    assert rm.get_global_usage().gpu == 6.0, (
        f"actor pool growth not picked up: got "
        f"{rm.get_global_usage().gpu}, expected 6.0"
    )

    # Step 4: Preprocess dispatch #2.
    state["pre_running"] = 2
    rm.on_task_dispatched(pre)
    assert_invariant("preprocess dispatch #2")
    assert rm.get_global_usage().cpu == 3.0
    assert rm.get_global_usage().gpu == 6.0


def test_actor_pool_growth_visible_via_downstream_dispatch():
    """If an actor pool grows between scheduling-step boundaries and a
    downstream task-pool op (not the actor pool itself) dispatches next,
    the fix's neighbor walk must still pick up the actor pool's new
    size. This is because the fix walks the dispatching op's
    ``input_dependencies`` — and the actor pool is one of those for the
    downstream op.

    Without this, the cluster autoscaler's util gauge could observe a
    stale ``_global_usage.gpu`` for arbitrarily long (until the next
    scheduling-step boundary on the actor pool itself), which is the
    bug pattern the friend's sf100 logs surfaced.
    """
    ctx = DataContext.get_current()
    input_op = InputDataBuffer(ctx, MagicMock())
    predictor = mock_map_op(
        input_op=input_op, ray_remote_args={"num_cpus": 0, "num_gpus": 1}
    )
    post = mock_map_op(input_op=predictor, ray_remote_args={"num_cpus": 1})

    state = {"actors": 4, "post_running": 0}

    predictor.incremental_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=0, gpu=0)
    )
    predictor.current_logical_usage = MagicMock(
        side_effect=lambda: ExecutionResources(cpu=0, gpu=float(state["actors"]))
    )
    predictor.running_logical_usage = MagicMock(
        side_effect=lambda: ExecutionResources(cpu=0, gpu=float(state["actors"]))
    )
    predictor.pending_logical_usage = MagicMock(return_value=ExecutionResources.zero())

    post.incremental_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=1, gpu=0)
    )
    post.current_logical_usage = MagicMock(
        side_effect=lambda: ExecutionResources(cpu=float(state["post_running"]), gpu=0)
    )
    post.running_logical_usage = MagicMock(
        side_effect=lambda: ExecutionResources(cpu=float(state["post_running"]), gpu=0)
    )
    post.pending_logical_usage = MagicMock(return_value=ExecutionResources.zero())

    rm = ResourceManager(
        build_streaming_topology(post, ExecutionOptions()),
        ExecutionOptions(),
        MagicMock(
            return_value=ExecutionResources(
                cpu=64, gpu=64, object_store_memory=10**12
            )
        ),
        ctx,
    )
    rm.update_usages()
    assert rm.get_global_usage().gpu == 4.0

    # Actor pool grows from 4 to 8 between scheduling-step boundaries.
    # No dispatch on predictor itself happens.
    state["actors"] = 8

    # `post` dispatches a task. Because predictor is in
    # `post.input_dependencies`, the fix's neighbor walk refreshes
    # predictor's logical usage in the same call.
    state["post_running"] = 1
    rm.on_task_dispatched(post)

    assert rm.get_global_usage().gpu == 8.0, (
        f"actor pool growth not propagated through downstream dispatch: "
        f"got {rm.get_global_usage().gpu}, expected 8.0"
    )
    assert rm.get_global_usage().cpu == 1.0


if __name__ == "__main__":
    pytest.main([__file__, "-vs"])
