from unittest.mock import MagicMock

import pytest

import ray
from ray.data._internal.execution.interfaces import ExecutionResources
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.task_pool_map_operator import (
    TaskPoolMapOperator,
)


def test_min_max_resource_requirements(ray_start_regular_shared, restore_data_context):
    data_context = ray.data.DataContext.get_current()
    op = TaskPoolMapOperator(
        map_transformer=MagicMock(),
        input_op=InputDataBuffer(data_context, input_data=MagicMock()),
        data_context=data_context,
        ray_remote_args={"num_cpus": 1},
    )
    op._metrics = MagicMock(obj_store_mem_max_pending_output_per_task=3)

    (
        min_resource_usage_bound,
        max_resource_usage_bound,
    ) = op.min_max_resource_requirements()

    # At a minimum, you need enough processors to run one task and enough object
    # store memory for a pending task.
    assert min_resource_usage_bound == ExecutionResources(
        cpu=1, gpu=0, object_store_memory=3
    )
    # For CPU-only operators, max GPU/memory is 0 (not inf) to prevent hoarding.
    assert max_resource_usage_bound == ExecutionResources.for_limits(gpu=0, memory=0)


def test_cached_dispatch_options_enabled_without_remote_args_fn(
    ray_start_regular_shared, restore_data_context
):
    """Without a dynamic `ray_remote_args_fn`, the per-bundle-size dispatch
    options should be pre-built once at __init__ rather than rebuilt per
    task. This avoids a per-dispatch `copy.deepcopy(self._ray_remote_args)`
    plus a fresh `self._map_task.options(...)` wrapping.
    """
    data_context = ray.data.DataContext.get_current()
    op = TaskPoolMapOperator(
        map_transformer=MagicMock(),
        input_op=InputDataBuffer(data_context, input_data=MagicMock()),
        data_context=data_context,
        ray_remote_args={"num_cpus": 1},
    )
    assert op._cached_small_options is not None
    assert op._cached_large_options is not None
    # Distinct wrappers for the two paths — different scheduling strategies.
    small = op._cached_small_args
    large = op._cached_large_args
    assert small["scheduling_strategy"] != large["scheduling_strategy"]
    assert small["name"] == op.name
    assert large["name"] == op.name


def test_cached_dispatch_options_disabled_with_remote_args_fn(
    ray_start_regular_shared, restore_data_context
):
    """When the user supplies a dynamic `ray_remote_args_fn`, the callback
    may return different args on each call, so the per-task rebuild path
    must still apply. The cache must not be populated in that case.
    """
    data_context = ray.data.DataContext.get_current()
    op = TaskPoolMapOperator(
        map_transformer=MagicMock(),
        input_op=InputDataBuffer(data_context, input_data=MagicMock()),
        data_context=data_context,
        ray_remote_args={"num_cpus": 1},
        ray_remote_args_fn=lambda: {"num_cpus": 2},
    )
    assert op._cached_small_options is None
    assert op._cached_large_options is None


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
