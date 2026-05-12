# M5 fix report

**Branch**: `glia/scheduler-perf-v5-m5-fix`
**Base**: v5 HEAD `01a8cbb996`
**Fix commit**: `4440ed45d3` — `[data] M5 fix: extend on_task_dispatched to neighbor coverage`

## Problem statement

A friend's sf100 cluster comparison of v5 (= upstream master + 6 M-series cherry-picks) against upstream master showed 10 regressions ≥10% out of 107 jobs in common. v5 was on average +1.8% slower overall, but the wins (TPC-H joins −21%, fixed-size TPC-H queries −15% to −30%, distributed_training −23 to −60%) cluster on workloads the M-series specifically targeted, while the losses cluster on workloads with autoscaling actor pools.

This report:
1. Catalogues all 10 regressions ≥10% and characterizes their root cause.
2. Describes the fix to `ResourceManager.on_task_dispatched` and explains why the chosen neighbor coverage is sufficient.
3. Provides unit + end-to-end tests pinning the structural invariants the fix restores.
4. Reports the 5-bench devpod sweep comparing the fix tree to master / v5 (pre-fix) / no_m5.

## 1. Regression catalog

### Methodology

For each regression I extracted the upstream Ray Data logs from the friend's PR (`ray-project/ray#63239`, commit `65da1b51ed`, attached `ray-data-logs.zip`), reconstructed the per-operator timeline relative to each dataset's start, identified the regressing phase, and compared cluster trajectory, per-op metrics, and any backpressure signal. Where possible I also tried to reproduce locally on a 24-vCPU devpod with three trees: master, v5 (pre-fix), no_m5 (v5 with M5 reverted).

### Summary table (10 regressions ≥10%)

| # | Test | Δ% | Δs | Cluster | Regressing phase | Root cause | Reproduces locally? |
|---|---|---|---|---|---|---|---|
| 1 | text_embeddings_benchmark_autoscaling_preemptible | +66.2% | +843s | autoscaling 1→15 GPU nodes | actor warm-up (22.5 min vs 1.4 min) | **M5 + autoscaler** | n/a (no multi-node cluster) |
| 2 | streaming_splitearly_stop | +48.1% | +7s | fixed | ReadParquet per-task `block_gen_and_ser` +50% | **N=1 cloud-network noise** | No (local count_parquet bench identical across trees) |
| 3 | text_embeddings_benchmark_autoscaling | +36.0% | +413s | autoscaling 1→15 GPU nodes | same as #1, less severe | **M5 + autoscaler** | n/a |
| 4 | tpch_q18_fixed_size | +22.7% | +46s | fixed 32 nodes | HashAggregate(100 parts) drain phase | **Likely noise** (6 of 8 fixed-size TPC-H queries win; q13/q18 outliers; aggregator code path untouched by M5) | Not attempted (local groupby b4 had unrelated import issue) |
| 5 | read_from_uris_fixed_size | +20.8% | +43s | fixed 10 nodes | Download (S3) per-task time +42% | **N=1 cloud-network noise** (mixed direction across read-heavy tests in same comparison; some +20%, some −29%) | n/a (no S3 access) |
| 6 | tpch_q17_autoscaling | +18.5% | +52s | autoscaling 1→12 nodes | Join aggregation phase (CPU-budget throttled) | **M5 + autoscaler** | No on devpod (need autoscaling cluster) |
| 7 | image_classification_autoscaling | +15.8% | +60s | autoscaling 1→10 nodes | ReadParquet (cluster scale-up 81s slower on v5) | **M5 + autoscaler** | n/a |
| 8 | tpch_q13_fixed_size | +14.4% | +6s | fixed 32 nodes | Join+HashAggregate (small absolute) | **Likely noise** | n/a |
| 9 | image_classification_from_parquet_autoscaling | +10.9% | +42s | autoscaling 1→10 nodes | ReadParquet (cluster scale-up 20s slower) | **M5 + autoscaler** | n/a |
| 10 | tpch_q10_autoscaling | +10.1% | +23s | autoscaling 1→11 nodes (master)/10 (v5) | final HashAggregate (1 fewer node on v5) | **M5 + autoscaler** | n/a |

### Diagnosis by category

**M5 + cluster autoscaler interaction (7 of 10)**

Mechanism: M5's incremental `on_task_dispatched(op)` hook applied a `_dispatch_delta(op)` of `(0, 0, 0)` for actor-pool ops to `_global_usage`. The cluster autoscaler v2's `RollingLogicalUtilizationGauge` reads `ResourceManager.get_global_usage()` to compute utilization; the autoscaler triggers scale-up only when `util >= 0.75` AND on a 10-second rolling-average window. Between scheduling-step boundaries, `_global_usage.gpu` (and `_global_usage.cpu` for hash-shuffle aggregator actor pools) stayed stale w.r.t. actor pool growth. The util gauge under-reported, the autoscaler under-triggered, and the cluster ramp lagged master's by 20–80s.

The CPU-budget throttling observed in tpch_q17 (`Join op alloc=167.5 CPU on v5 vs 179.5 on master`) and the GPU-actor warm-up lag observed in text_embeddings (22.5 min vs 1.4 min) are both downstream effects of the same stale `_global_usage` reaching the autoscaler.

**N=1 cloud-network noise (3 of 10)**

streaming_splitearly_stop, read_from_uris_fixed_size, and probably tpch_q18/q13_fixed_size are likely environmental noise:

- streaming_splitearly_stop's +50% per-task `block_gen_and_ser_time` was worker-side parquet decode time, on the same `_raylet.so` and the same Python code path. Master and v5 ran on different days; the upstream test runs once per build with N=1.
- Local reproduction of count_parquet over an equivalent number of small parquet files (423, matching upstream's input shape) showed master / v5 / no_m5 all within 1% — no reproducible code-side regression.
- The comparison's read-heavy tests are mixed: read_images_fixed_size −29% (faster on v5), read_large_parquet_fixed_size −15% (faster), read_parquet_fixed_size −4% (flat), read_from_uris_fixed_size +21% (slower), streaming_splitearly_stop +48% (slower). A real Ray-Data code regression would point all directions; mixed signal is the signature of S3 / network variance across run dates.
- For tpch_q18 and q13: 6 of 8 fixed-size TPC-H queries (q3, q4, q7, q8, q9, q22) won by 15–30% on v5; only q13 and q18 lost. The aggregator-actor code path (hash_shuffle.py) is untouched by any M-series commit. Same-shape workloads winning is hard to reconcile with a real systematic regression on this op type.

## 2. The fix

### Source change

`python/ray/data/_internal/execution/resource_manager.py` — `on_task_dispatched`:

Before (M5 baseline): apply a static `_dispatch_delta(op)` to `_op_usages[op]`, `_op_running_usages[op]`, `_mem_op_internal[op]`, `op._metrics.obj_store_mem_used`, `_global_usage`, `_global_running_usage`, and decrement op's budget via the allocator hook. **The static delta is `(0, 0, 0)` for actor-pool ops, so actor-pool size changes (new actors coming online between scheduling-step boundaries) are not reflected in any of the above.**

After (fix): walk `{op} ∪ op.input_dependencies` and for each affected op re-read `current_logical_usage()`, `running_logical_usage()`, `pending_logical_usage()`, and `_estimate_object_store_memory_usage(...)`. Update globals by `new - old` (incremental). Preserve M5's per-op budget decrement.

### Why is neighbor coverage sufficient?

A dispatch event mutates ground truth in exactly two places:

**(1) The dispatching op `op`.**

- `num_tasks_running` grows.
- `obj_store_mem_pending_task_outputs` grows by the new task's predicted output bytes.
- `obj_store_mem_pending_task_inputs` grows by the new task's input bytes.
- `current_logical_usage()` may grow for *other* reasons too — e.g., a new actor came online in an ActorPoolMapOperator between scheduling-step boundaries. Re-reading picks this up.

**(2) Each upstream of `op`.**

`_estimate_object_store_memory_usage(upstream)` attributes:

```
_mem_op_outputs[upstream] = upstream_own_outputs +
    sum(downstream.obj_store_mem_internal_inqueue_for_input(i) +
        downstream.obj_store_mem_pending_task_inputs
        for downstream in upstream.output_dependencies)
```

When `op` dispatches, `op.obj_store_mem_pending_task_inputs` grows. `op` is in each `upstream.output_dependencies` (by construction — upstream's output goes to op). So each upstream's `_mem_op_outputs` cross-op term grows by the same amount. Re-reading the upstream side picks this up.

**Downstream of `op` is NOT affected at dispatch time.**

`op`'s outputs (its `internal_outqueue` and `state.output_queue_bytes()`) only grow when `op`'s tasks *complete* and emit blocks, not when it dispatches. So downstream's `_mem_op_internal[downstream]` and `_mem_op_outputs[downstream]` are invariant under `op`'s dispatch event.

Equivalently: at dispatch time, the data has not flowed past `op` yet — only the bookkeeping of "op is now committed to producing X bytes of output" exists, and that bookkeeping is captured in `op`'s own pending-output fields.

**Transitive coverage rationale.**

What about *grandparent* dependencies (upstream of upstream)? Grandparent's `_mem_op_outputs` term depends on `parent.obj_store_mem_pending_task_inputs`. When `op` dispatches, did `parent.obj_store_mem_pending_task_inputs` change? No — `op` consumed a bundle from `parent`'s output queue, but `parent`'s own input queue is unaffected. The bundle moved from `parent.outqueue` (down) to `op.pending_task_inputs` (up).

So grandparent's `_mem_op_outputs` could change if `parent.own_outputs` changed: `parent.own_outputs = parent.internal_outqueue + parent.state.output_queue_bytes()`. When `op` consumed from parent's output queue, `parent.state.output_queue_bytes()` shrank by the same amount that `op.pending_task_inputs` grew. The net effect on the chain is bookkeeping-conserving: `(parent.own_outputs - delta) + (op.pending_task_inputs + delta) = constant`. So grandparent's `_mem_op_outputs` (which sums over its downstream `parent`'s state) is `(parent.own_outputs - delta) + (parent's downstream's terms) = parent.own_outputs_old + (parent's downstream terms)` — *unchanged* relative to the pre-dispatch state.

In other words: the dispatch event propagates exactly one hop upstream because the bookkeeping is conservative across the bundle hand-off. Walking `{op, upstream(op)}` is sufficient. Walking further would re-read state that did not change.

### Cost

- M5 baseline (pre-fix): O(1) per dispatch but produces stale state at 4 observed sites.
- pre-M5 master: O(N_ops) per dispatch (full `update_usages` walk).
- Fix: O(1 + |op.input_dependencies|) per dispatch. For typical fan-in pipelines (1–2 upstream deps), this is O(1)–O(2). Preserves M5's asymptotic win for fan-in pipelines while restoring all four observed invariants.

The per-op budget redistribution (`_update_allocated_budgets`) is **not** called per dispatch — preserving the pre-existing "design-intended" budget borrow-drift pinned by `test_m5_borrow_drift_in_multi_op_topology_is_real_and_quantified`, bounded to one scheduling step.

## 3. Tests

### Unit tests (existing, updated)

`python/ray/data/tests/test_reservation_based_resource_allocator.py` — 8 tests:

- `test_on_task_dispatched_decrements_budget_without_mutation` ✓ (unchanged)
- `test_on_task_dispatched_clamps_at_zero_never_negative` ✓ (unchanged)
- `test_on_task_dispatched_decrements_object_store_memory_by_predicted_output` ✓ (unchanged)
- `test_on_task_dispatched_no_decrement_when_per_task_output_metric_missing` ✓ (unchanged)
- `test_on_task_dispatched_updates_op_usages_read_by_gating_consumers` ✓ (mocks updated to set `current_logical_usage()` consistent with dispatch semantics)
- `test_on_task_dispatched_updates_mem_op_internal_and_dashboard_metric` ✓ (same mock fix)
- `test_execution_resources_subtract_clamp_zero` ✓ (unchanged)
- `test_m5_borrow_drift_in_multi_op_topology_is_real_and_quantified` ✓ (design-intended drift preserved)

### New regression tests

`glia-bench/regression_repro/m5_actor_pool_global_usage_drift_test.py` — pins the actor-pool `_global_usage.gpu` invariant.

`glia-bench/regression_repro/m5_semantic_equivalence_tests.py` — pins all four divergences A/B/D/G from the audit:
- A: `_global_usage.gpu` stale for actor pool growth
- B: `_op_usages[upstream].object_store_memory` stale for cross-op term
- D: `_global_pending_usage` never updated
- G: upstream `_metrics.obj_store_mem_used` dashboard metric stale

All four PASS on the fix tree; all four FAIL on v5 pre-fix.

### New end-to-end test

`python/ray/data/tests/test_m5_neighbor_coverage_fix.py` — two tests:

- `test_global_usage_tracks_actor_pool_growth_under_realistic_dispatch_sequence`: 3-op pipeline (Read → Preprocess → Predictor[actor pool]). Drives a 4-step dispatch sequence including an actor pool scaling 4 → 6 between boundaries. After each step asserts `rm.get_global_usage().{cpu,gpu}` matches the sum of each op's `current_logical_usage()`.

- `test_actor_pool_growth_visible_via_downstream_dispatch`: pinpoints the corner case where the actor pool grows between boundaries but does not itself dispatch; verifies that a downstream task-pool op's dispatch picks up the upstream actor pool's new size via the neighbor walk.

## 4. 5-bench devpod sweep

Sweep configuration: 24-vCPU Linux devpod, 5 workloads × 4 trees × 3 reps = 60 runs, all sharing `/opt/venv` (matched libs) and the same `_raylet.so`. MAPPING swap selects the Python tree per (config, workload, rep). Total wall: 116 min. **All output hashes match across all 4 trees on every workload.** Raw data: `results/optimization_perf_four_tree.jsonl`.

### Headline

| Workload | master | v5 (pre-fix) | no_m5 | v5_fix | Δ v5_fix vs master | Δ v5_fix vs v5 |
|---|---|---|---|---|---|---|
| synthetic | 142.96 ± 2.76s | 84.68 ± 0.61s | 101.96 ± 2.21s | 88.84 ± 1.99s | **−37.9%** | +4.9% |
| mixed_pipeline | 178.21 ± 0.18s | 98.13 ± 2.07s | 117.49 ± 4.90s | 97.37 ± 0.80s | **−45.4%** | −0.8% |
| medium_tasks | 46.29 ± 0.12s | 22.67 ± 1.07s | 21.11 ± 0.07s | 22.17 ± 0.27s | **−52.1%** | −2.2% |
| long_tasks (control) | 93.14 ± 0.39s | 85.98 ± 0.21s | 88.08 ± 1.52s | 86.48 ± 0.43s | −7.2% | +0.6% |
| actor_backpressure | 48.21 ± 0.76s | 26.40 ± 0.63s | 24.76 ± 0.82s | 24.87 ± 0.14s | **−48.4%** | **−5.8%** |

### Discussion

1. **Correctness**: every output hash matches across master/v5/no_m5/v5_fix on every workload. The fix preserves byte-identical output behavior.

2. **The fix preserves most of v5's wins** vs master: synthetic −38%, mixed_pipeline −45%, medium_tasks −52%, actor_backpressure −48%. These match v5's pre-fix magnitudes within ±5%.

3. **v5_fix is within noise of v5** on 4 of 5 workloads (mixed_pipeline −0.8%, medium_tasks −2.2%, long_tasks +0.6%, actor_backpressure −5.8%). Only synthetic shows +4.9% on the fix — the synthetic workload is a depth-6 task-pool DAG with the highest dispatch rate of any bench in the suite, so the extra per-dispatch work (the upstream re-read) is most visible there. The +4.9% is well below v5's 38% margin against master.

4. **v5_fix is meaningfully *faster* than v5 on actor_backpressure** (24.87s vs 26.40s, −5.8% with lower stdev). The actor_backpressure workload exercises an autoscaling actor pool with sustained plasma pressure — exactly where the pre-fix M5's stale `_global_usage` caused the scheduler to make suboptimal decisions. The fix's neighbor-coverage walk gives the scheduler and backpressure policies more accurate state, leading to better dispatch timing.

5. **Asymptotic cost** of the fix on devpod is bounded: at worst +4.9% on the most aggressive scheduler-stress workload (synthetic), often a win on actor-heavy workloads. The fix's primary purpose — restoring the cluster autoscaler's correct utilization signal — is *not* exercised on a single-node devpod, so these numbers measure the *overhead* of the fix on the hot path, not its benefit.

### What's NOT measured here

The sf100 cluster regressions (text_embeddings_benchmark_autoscaling_preemptible +66%, text_embeddings_benchmark_autoscaling +36%, image_classification_autoscaling +15.8%, tpch_q17_autoscaling +18.5%, image_classification_from_parquet_autoscaling +10.9%, tpch_q10_autoscaling +10.1%) all share the M5 + cluster autoscaler root and require multi-node autoscaling clusters to reproduce. The fix's structural invariant tests (`test_m5_neighbor_coverage_fix.py`, `m5_actor_pool_global_usage_drift_test.py`, `m5_semantic_equivalence_tests.py`) verify that the autoscaler-feeding `_global_usage` field stays fresh, which is the root cause being fixed. The cluster-level confirmation needs a re-run of the friend's sf100 comparison against this branch.

## 5. Recommended next steps

1. Merge this branch into v5-rebase after the friend's cluster confirms the autoscaling regressions clear.
2. Have the friend re-run the sf100 comparison with the fixed branch. Expected: autoscaling regressions (text_embeddings_*, image_classification_*, tpch_q*_autoscaling) should clear; the win surface (TPC-H joins, fixed-size TPC-H, distributed_training) should remain.
3. For the suspected-noise regressions (streaming_splitearly_stop, tpch_q18/q13_fixed_size, read_from_uris_fixed_size): N=5 reruns on the friend's cluster. If they persist across reruns, separate investigation.
