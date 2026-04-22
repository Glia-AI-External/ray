"""Curated list of Ray Data tests to gate scheduling-loop optimizations.

Each entry is a pytest nodeid (file path, optionally with ``::classname``
or ``::function_name`` suffix). Paths are relative to the artifact root.

The list covers test files that exercise code the agent may modify
(scheduling loop, resource manager, backpressure policies, operators,
issue detection, ranker, progress manager, autoscaler, stats) and tests
that indirectly depend on those code paths (e2e dataset iteration, limits,
object GC, schema export).

Intentionally excluded: format-specific datasource tests (CSV/Parquet/etc.),
cloud integration tests, pure-data aggregations, shuffle-internal tests,
and logical optimization tests. These do not exercise scheduling.
"""

FAST_TEST_NODES = [
    # Fast subset (~5-6 min) for mid-session correctness pre-checks.
    # Curated to catch the classes of regressions we've observed in real
    # agent runs, not just generic breakage. Each file maps to a distinct
    # failure mode:
    #
    # - streaming_executor: scheduling loop structure, inner-dispatch
    #   ordering, process_completed_tasks plumbing.
    # - backpressure_policies: per-policy decisions (resource budget, queue
    #   size, output capacity). Catches regressions to backpressure
    #   semantics independent of the resource allocator.
    # - ranker: operator-selection ordering. Cheap and directly exercised
    #   by any change to select_operator_to_run.
    # - reservation_based_resource_allocator: catches budget-math
    #   regressions such as the _create_raw/safe_round rounding bug
    #   (opt-002 class). Includes test_basic and
    #   test_reservation_accounts_for_completed_ops_complex_graph.
    # - resource_manager: catches changes to update_usages, completed-ops
    #   accounting, and the ExecutionResources arithmetic that the hot
    #   path depends on.
    # - dataset_iter: catches scheduling-determinism regressions via
    #   test_iter_batches_local_shuffle (seeded shuffle requires
    #   deterministic block arrival order — timing-sensitive optimizations
    #   like reducing ray.wait timeout break this).
    # - actor_pool_map_operator: catches dispatch-path changes in the
    #   actor-pool branch (opt-018 class: cached dispatch options).
    #
    # Intentionally excluded from fast:
    # - issue_detection_manager: not relevant to scheduler correctness.
    # - backpressure_e2e: slow (full pipeline) and needs the state API.
    # - streaming_integration: long-running integration checks.
    #
    # Note: `tests/unit/test_resource_manager.py` is also included because
    # it shares a basename with `tests/test_resource_manager.py`. The gate's
    # nodeid normalizer collapses both to `test_resource_manager::*`, so if
    # only one of the two files is run the other's tests appear as
    # "missing" in current, and the gate reports them as false-positive
    # regressions. Running both files in fast keeps keys populated.
    "python/ray/data/tests/test_streaming_executor.py",
    "python/ray/data/tests/test_backpressure_policies.py",
    "python/ray/data/tests/test_ranker.py",
    "python/ray/data/tests/test_reservation_based_resource_allocator.py",
    "python/ray/data/tests/test_resource_manager.py",
    "python/ray/data/tests/unit/test_resource_manager.py",
    "python/ray/data/tests/test_dataset_iter.py",
    "python/ray/data/tests/test_actor_pool_map_operator.py",
]


# Tests that either (a) flake probabilistically on this hardware due to
# timing sensitivity that is not under Ray Data's control (e.g.
# ``test_spilled_stats`` asserts on a backpressure-time string that depends on
# per-host timing precision), or (b) encode user-facing determinism contracts
# whose failure mode is probabilistic rather than deterministic (shuffle
# determinism). Both classes must be retried at full depth on BOTH the
# baseline and the gate so that the comparison is symmetric; a single-run
# pass on the baseline against a 10-run-retry on the gate is a
# retry-policy artifact, not a real regression.
#
# Normalized nodeid form: ``<test_file_basename_without_ext>::<function_name>``
# (matches the ``_parse_junit`` output).
KNOWN_FLAKY_TESTS = {
    "test_stats::test_spilled_stats[True]",
    "test_stats::test_spilled_stats[False]",
    "test_consumption::test_read_write_local_node_ray_client",
    "test_dataset_iter::test_iter_batches_local_shuffle[pandas]",
    "test_dataset_iter::test_iter_batches_local_shuffle[arrow]",
}


TEST_NODES = [
    # Full Ray Data test corpus — every test_*.py under
    # python/ray/data/tests/ and tests/unit/, with the cuDF-format file
    # excluded (cuDF install downgrades cuda-bindings in a way that
    # segfaults test_backpressure_e2e in the same pytest session).
    "python/ray/data/tests/test_actor_pool_map_operator.py",
    "python/ray/data/tests/test_agg_e2e.py",
    "python/ray/data/tests/test_aggregations.py",
    "python/ray/data/tests/test_arrow_block.py",
    "python/ray/data/tests/test_arrow_serialization.py",
    "python/ray/data/tests/test_auto_parallelism.py",
    "python/ray/data/tests/test_autoscaler.py",
    "python/ray/data/tests/test_autoscaling_coordinator.py",
    "python/ray/data/tests/test_backpressure_e2e.py",
    "python/ray/data/tests/test_backpressure_policies.py",
    "python/ray/data/tests/test_batcher.py",
    "python/ray/data/tests/test_block_ref_bundler.py",
    "python/ray/data/tests/test_block_sizing.py",
    "python/ray/data/tests/test_bundle_queue.py",
    "python/ray/data/tests/test_checkpoint.py",
    "python/ray/data/tests/test_consumption.py",
    "python/ray/data/tests/test_context.py",
    "python/ray/data/tests/test_context_propagation.py",
    "python/ray/data/tests/test_dataset_aggregrations.py",
    "python/ray/data/tests/test_dataset_creation.py",
    "python/ray/data/tests/test_dataset_iter.py",
    "python/ray/data/tests/test_dataset_limits.py",
    "python/ray/data/tests/test_dataset_stats.py",
    "python/ray/data/tests/test_dataset_validation.py",
    "python/ray/data/tests/test_default_cluster_autoscaler_v2.py",
    "python/ray/data/tests/test_download_expression.py",
    "python/ray/data/tests/test_downstream_capacity_backpressure_policy.py",
    "python/ray/data/tests/test_dynamic_block_split.py",
    "python/ray/data/tests/test_ecosystem_dask.py",
    "python/ray/data/tests/test_ecosystem_modin.py",
    "python/ray/data/tests/test_exceptions.py",
    "python/ray/data/tests/test_execution_optimizer_advanced.py",
    "python/ray/data/tests/test_execution_optimizer_basic.py",
    "python/ray/data/tests/test_execution_optimizer_integrations.py",
    "python/ray/data/tests/test_execution_optimizer_limit_pushdown.py",
    "python/ray/data/tests/test_executor_resource_management.py",
    "python/ray/data/tests/test_filter.py",
    "python/ray/data/tests/test_gpu_shuffle.py",
    "python/ray/data/tests/test_groupby_e2e.py",
    "python/ray/data/tests/test_hash_shuffle.py",
    "python/ray/data/tests/test_hash_shuffle_aggregator.py",
    "python/ray/data/tests/test_import.py",
    "python/ray/data/tests/test_issue_detection.py",
    "python/ray/data/tests/test_issue_detection_manager.py",
    "python/ray/data/tests/test_iterator.py",
    "python/ray/data/tests/test_join.py",
    "python/ray/data/tests/test_jumbo_arrow_block.py",
    "python/ray/data/tests/test_limit_operator.py",
    "python/ray/data/tests/test_logging.py",
    "python/ray/data/tests/test_logging_dataset.py",
    "python/ray/data/tests/test_map.py",
    "python/ray/data/tests/test_map_batches.py",
    "python/ray/data/tests/test_map_operator.py",
    "python/ray/data/tests/test_map_transformer.py",
    "python/ray/data/tests/test_metadata_provider.py",
    "python/ray/data/tests/test_monotonically_increasing_id.py",
    "python/ray/data/tests/test_numpy_support.py",
    "python/ray/data/tests/test_object_gc.py",
    "python/ray/data/tests/test_op_runtime_metrics.py",
    "python/ray/data/tests/test_operator_fusion.py",
    "python/ray/data/tests/test_operator_schema_export.py",
    "python/ray/data/tests/test_operators.py",
    "python/ray/data/tests/test_optimize.py",
    "python/ray/data/tests/test_output_splitter.py",
    "python/ray/data/tests/test_pandas_block.py",
    "python/ray/data/tests/test_partitioning.py",
    "python/ray/data/tests/test_predicate_pushdown.py",
    "python/ray/data/tests/test_preserve_hash_shuffle_blocks.py",
    "python/ray/data/tests/test_progress_bar.py",
    "python/ray/data/tests/test_progress_manager.py",
    "python/ray/data/tests/test_projection_fusion.py",
    "python/ray/data/tests/test_push_based_shuffle.py",
    "python/ray/data/tests/test_random_access.py",
    "python/ray/data/tests/test_random_api.py",
    "python/ray/data/tests/test_random_e2e.py",
    "python/ray/data/tests/test_randomize_block_order.py",
    "python/ray/data/tests/test_ranker.py",
    "python/ray/data/tests/test_read_datasource.py",
    "python/ray/data/tests/test_ref_bundle.py",
    "python/ray/data/tests/test_repartition_e2e.py",
    "python/ray/data/tests/test_reservation_based_resource_allocator.py",
    "python/ray/data/tests/test_resource_manager.py",
    "python/ray/data/tests/test_shuffle_diagnostics.py",
    "python/ray/data/tests/test_size_estimation.py",
    "python/ray/data/tests/test_sort.py",
    "python/ray/data/tests/test_split.py",
    "python/ray/data/tests/test_splitblocks.py",
    "python/ray/data/tests/test_state_export.py",
    "python/ray/data/tests/test_stats.py",
    "python/ray/data/tests/test_streaming_executor.py",
    "python/ray/data/tests/test_streaming_executor_errored_blocks.py",
    "python/ray/data/tests/test_streaming_integration.py",
    "python/ray/data/tests/test_strict_mode.py",
    "python/ray/data/tests/test_synthetic_expression.py",
    "python/ray/data/tests/test_task_pool_map_operator.py",
    "python/ray/data/tests/test_telemetry.py",
    "python/ray/data/tests/test_tensor.py",
    "python/ray/data/tests/test_tensor_extension.py",
    "python/ray/data/tests/test_torch_iter_batches.py",
    "python/ray/data/tests/test_torch_tensor_utils.py",
    "python/ray/data/tests/test_transform_pyarrow.py",
    "python/ray/data/tests/test_unify_schemas_performance.py",
    "python/ray/data/tests/test_union.py",
    "python/ray/data/tests/test_unique_e2e.py",
    "python/ray/data/tests/test_util.py",
    "python/ray/data/tests/test_with_column.py",
    "python/ray/data/tests/test_zip.py",
    "python/ray/data/tests/unit/test_arrow_block.py",
    "python/ray/data/tests/unit/test_arrow_type_conversion.py",
    "python/ray/data/tests/unit/test_average_calculator.py",
    "python/ray/data/tests/unit/test_block.py",
    "python/ray/data/tests/unit/test_block_boundaries.py",
    "python/ray/data/tests/unit/test_bundler.py",
    "python/ray/data/tests/unit/test_data_batch_conversion.py",
    "python/ray/data/tests/unit/test_dataset_repr.py",
    "python/ray/data/tests/unit/test_datatype.py",
    "python/ray/data/tests/unit/test_deduping_schema.py",
    "python/ray/data/tests/unit/test_expression_evaluator.py",
    "python/ray/data/tests/unit/test_fifo_bundle_queue.py",
    "python/ray/data/tests/unit/test_filename_provider.py",
    "python/ray/data/tests/unit/test_logical_plan.py",
    "python/ray/data/tests/unit/test_object_extension.py",
    "python/ray/data/tests/unit/test_parquet_predicate_split.py",
    "python/ray/data/tests/unit/test_path_util.py",
    "python/ray/data/tests/unit/test_reordering_bundle_queue.py",
    "python/ray/data/tests/unit/test_resource_manager.py",
    "python/ray/data/tests/unit/test_ruleset.py",
    "python/ray/data/tests/unit/test_throughput_solver.py",
    "python/ray/data/tests/unit/test_transform_pyarrow.py",
]
