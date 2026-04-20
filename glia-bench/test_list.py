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


# Tests that encode user-facing determinism contracts and are therefore
# SENSITIVE to non-deterministic scheduling changes. They are NOT flaky in the
# usual sense — they encode real contracts Ray Data advertises, and they will
# pass reliably against a scheduler that keeps block arrival order
# deterministic. They fail probabilistically under timing-sensitive
# optimizations (e.g. aggressive ray.wait timeout reductions, partial
# completion sets).
#
# Run these tests multiple times in both baseline-record and gate modes. A
# single pass is not evidence of correctness — probabilistic bugs pass a
# non-trivial fraction of runs. Requiring N consecutive passes is the only way
# to distinguish a deterministic success from a lucky probabilistic one.
#
# Baseline: if any of N runs fails, the test is recorded as "failed" in
# baseline (treated as pre-existing and ignored).
# Gate: if any of N runs fails, the gate reports the test as regressed.
#
# Agents can pre-check with ``./evaluator/run_sensitive_tests [N]`` which runs
# the same list directly.
SENSITIVE_TESTS = [
    "python/ray/data/tests/test_dataset_iter.py::test_iter_batches_local_shuffle[pandas]",
    "python/ray/data/tests/test_dataset_iter.py::test_iter_batches_local_shuffle[arrow]",
]

# Default number of runs for sensitive tests. Chosen empirically: for a bug
# with ~50% per-run pass rate, 3 runs give ~12.5% probability of passing by
# luck. Combined with the gate's inability to retry without a fresh commit,
# slip-through becomes very unlikely.
SENSITIVE_TEST_RUNS = 3


TEST_NODES = [
    # === Core scheduling & execution ===
    "python/ray/data/tests/test_streaming_executor.py",
    "python/ray/data/tests/test_streaming_integration.py",

    # === Backpressure ===
    "python/ray/data/tests/test_backpressure_e2e.py",
    "python/ray/data/tests/test_backpressure_policies.py",
    "python/ray/data/tests/test_downstream_capacity_backpressure_policy.py",

    # === Resource management ===
    "python/ray/data/tests/test_resource_manager.py",
    "python/ray/data/tests/test_reservation_based_resource_allocator.py",
    "python/ray/data/tests/unit/test_resource_manager.py",

    # === Bundle queue ===
    "python/ray/data/tests/test_bundle_queue.py",
    "python/ray/data/tests/unit/test_fifo_bundle_queue.py",
    "python/ray/data/tests/unit/test_reordering_bundle_queue.py",

    # === Scheduling support systems ===
    "python/ray/data/tests/test_progress_manager.py",
    "python/ray/data/tests/test_ranker.py",
    "python/ray/data/tests/test_autoscaler.py",
    "python/ray/data/tests/test_issue_detection_manager.py",

    # === Metrics & stats ===
    "python/ray/data/tests/test_stats.py",
    "python/ray/data/tests/test_op_runtime_metrics.py",

    # === Operators ===
    "python/ray/data/tests/test_map_operator.py",
    "python/ray/data/tests/test_limit_operator.py",
    "python/ray/data/tests/test_actor_pool_map_operator.py",

    # === Dataset-level e2e (indirect coverage) ===
    "python/ray/data/tests/test_consumption.py",
    "python/ray/data/tests/test_dataset_iter.py",
    "python/ray/data/tests/test_dataset_limits.py",
    "python/ray/data/tests/test_object_gc.py",
    "python/ray/data/tests/test_operator_schema_export.py",
]
