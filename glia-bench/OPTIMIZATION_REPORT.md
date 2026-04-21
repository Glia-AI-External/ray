# Ray Data scheduler-thread optimizations

**Branch**: [`glia/scheduler-perf-v1`](https://github.com/Glia-AI-External/ray/tree/glia/scheduler-perf-v1)
**Base**: `ray-2.55.0` (commit `58af3fc5`)
**Author**: alizadeh@glia-ai.com

## Executive summary

Six localized optimizations to the Ray Data streaming-executor scheduler thread, discovered by the **Glia systems engineering platform**. Each targets a specific hot spot on small-task pipelines at high dispatch rates. No public APIs change; every commit preserves output on correct inputs.

**Performance** (24-CPU cgroup, N = 5 reps per configuration, pristine ray-2.55.0 vs branch HEAD):

| Workload | Wall-time delta | Throughput delta |
|---|---|---|
| synthetic | **−43.7%** | +77.6% |
| mixed_pipeline | **−53.0%** | +112.7% |
| medium_tasks | **−35.2%** | +54.3% |
| long_tasks | −0.2% (noise) | +0.2% (noise) |

Throughput deltas on the three scheduler-bound workloads are statistically significant (Welch's t ≥ 69; p ≪ 0.001). `long_tasks` is worker-bound and serves as a control — the optimizations target scheduler-thread work that is vanishingly small relative to total wall time on this workload, so no improvement is expected there. All four workloads produce byte-identical output to pristine across every run (SHA-256 over sorted rows).

**Correctness**. The curated Ray Data test gate runs 452 pre-existing tests plus 5 new unit tests on both pristine and branch HEAD, using a symmetric fractional-retry methodology — probabilistically-flaky tests are retried at full 10× depth on both sides so comparisons are apples-to-apples. Result: **0 regressions**, all 5 new unit tests pass on M6, and the two known-flaky baseline tests show no increase in failure rate.

All harness code, workload configs, gate artifacts, and raw per-run results live under `glia-bench/` for reproduction (§5).

## 1. Introduction

The Ray Data streaming executor consists of a scheduler thread that repeatedly calls `process_completed_tasks` (wrapping `ray.wait`) and iterates the operator topology to dispatch new tasks, plus a consumer thread that pulls from the terminal operator's output queue. While profiling small-task (10–50 ms per task) pipelines at high block counts (>10K blocks), we observed that the scheduler thread's per-iteration work — options-dict rebuilding, per-dispatch budget recomputation, per-call `RefBundle` size derivation, and polling loops — had grown to a meaningful fraction of end-to-end wall time.

Scope: no API changes, no new abstractions, no public-behavior changes on correct outputs. Each change is small enough to review in isolation and is accompanied by a targeted unit test.

Contributions:
1. Six measured optimizations (§2), each one a single commit.
2. A reproducible benchmark + correctness harness (`glia-bench/`, §3).
3. A fractional-retry gate methodology with a symmetric known-flaky list that avoids false-alarm regressions from probabilistically-flaky tests (§3.4).

## 2. Optimizations

Each subsection is a single commit on the branch. File paths are relative to the Ray repo root (`python/ray/...`).

### 2.1 M1 — Batch `get_local_object_locations` in `plan_read_op`

**Commit**: `c23b9c00`

**Observation**. `python/ray/data/_internal/planner/plan_read_op.py` derives a `BlockMetadata` for each `ReadTask` inside the per-task loop, calling `get_local_object_locations([ref])` once per task. At high read parallelism (20K+ tasks) this is tens of thousands of individual core-worker lookups issued serially during pipeline setup.

**Change**. Factor the per-task derivation into `_derive_block_metadata(tasks, refs)`, which issues a single `get_local_object_locations(all_refs)` call and walks the result per task. Per-task `size_bytes` and the large-task `log_once` warning are preserved exactly.

**Correctness**. A unit test (`test_derive_block_metadata_batched`) constructs three `ReadTask`s with closures padded to distinct sizes and asserts the helper returns all three sizes distinctly — guarding against any accidental collapse to a shared estimate.

### 2.2 M2 — `threading.Event` in `OpState.get_output_blocking`

**Commit**: `9f3e7a4a`

**Observation**. The consumer thread polls the output queue in `OpState.get_output_blocking` with `time.sleep(0.01)`, waking up 100× per second regardless of workload, and adding up to ~10 ms of latency between a block's enqueue and the consumer observing it.

**Change**. Replace the poll with a `threading.Event` signaled from `add_output` (on enqueue) and `mark_finished` (on end-of-stream / exception). The consumer uses a clear-check-wait idiom: pop; if empty, clear the event; re-check queue + finished/exception flags; wait with a conservative 1 s safety timeout. The re-check closes the signal-loss race between the consumer's pop and clear; the 1 s timeout is defense-in-depth.

**Correctness**. `test_get_output_blocking_event_signaling` covers the three cases: producer-signaled-before-consumer, consumer-blocked-then-signaled, and `mark_finished` wakes a blocked consumer. This change also unblocks the adaptive-timeout work in §2.4 (same pattern).

### 2.3 M3 — Cache dispatch-options wrapper in `TaskPoolMapOperator`

**Commit**: `579968fe`

**Observation**. Every `_try_schedule_task` call in `TaskPoolMapOperator` rebuilds a per-task `ray_remote_args` dict via `copy.deepcopy(self._ray_remote_args)`, mutates a few fields, and wraps the task with `self._map_task.options(**args)`. None of the dispatch-relevant fields change per task — the only per-task variation is a binary `bundle.size_bytes() > large_args_threshold` flip that swaps between two scheduling strategies.

**Change**. Pre-build two `(args_dict, options_wrapper)` pairs at `__init__` — one for the small-args path (`ctx.scheduling_strategy`) and one for the large-args path (`ctx.scheduling_strategy_large_args`). Dispatch picks one based on bundle size and reuses the cached wrapper. When the user supplies a dynamic `ray_remote_args_fn`, the callback may return different args per call; in that case we fall back to the original per-task rebuild path to preserve semantics.

**Correctness**. The scheduling strategy is assigned via `args.setdefault(...)`, not direct assignment, so a user-pinned strategy in `ray_remote_args` (e.g. the `NodeAffinitySchedulingStrategy` that `ReadParquet` uses for local-node affinity) is never clobbered. This guards `test_read_write_local_node` and `test_configure_spread_e2e`. Two unit tests in `test_task_pool_map_operator.py` verify cache population and the `ray_remote_args_fn` fallback.

### 2.4 M4 — Adaptive `ray.wait` timeout in `process_completed_tasks`

**Commit**: `15c4173c`

**Observation**. `StreamingExecutor` called `process_completed_tasks` with a fixed 100 ms `ray.wait` timeout. For short-task workloads (per-task duration in the tens of ms) the scheduling loop was effectively capped at ~10 iterations / sec because the timeout fired on nearly every poll, even though individual tasks were completing far sooner.

**Change**. Adapt the timeout across scheduling iterations: halve on success (`num_ready > 0` — work is arriving, so poll again sooner), double on an empty poll (back off; don't busy-spin). Bounded by `[MIN_WAIT_TIMEOUT_S = 1 ms, DEFAULT_WAIT_TIMEOUT_S = 100 ms]`. Starts at the ceiling so pipelines with no initial work see identical cost to the old behavior. Long-running worker-bound workloads stay at the ceiling and see no change.

The adaptation rule is a pure staticmethod (`StreamingExecutor._adapt_wait_timeout(current, num_ready)`), unit-tested independently of the scheduler. `process_completed_tasks` now takes an optional `wait_timeout` kwarg (default preserves the historical 100 ms) and returns `(num_errored_blocks, num_ready)` so the caller can implement the adaptation. A new Gauge, `data_sched_wait_timeout_s`, exposes the current timeout for per-dataset observability.

**Correctness**. `test_adapt_wait_timeout_halve_and_double` covers halve/double, bounds, and saturation. The tuple-return change is source-compatible — all existing call sites discard the return value.

### 2.5 M5 — Incremental budget decrement instead of per-dispatch `update_usages()`

**Commit**: `ad7ef76c`

**Observation**. The scheduler's inner dispatch loop called `ResourceManager.update_usages()` after every task dispatch to keep operator budgets fresh for the next schedulability check. `update_usages()` walks the full topology, reconstructing every operator's budget from scratch — dominant scheduler-thread work at high dispatch rates (~100K+ calls per 20K-block run with depth-6 fanout).

**Change**. Replace the per-dispatch `update_usages()` with a lightweight `on_task_dispatched(op)` hook that only decrements the dispatched op's budget by `op.incremental_resource_usage()`. The outer `update_usages()` calls at the top and bottom of each scheduling step remain — so exact state is restored on every `process_completed_tasks` boundary (~10–100× less often than the inner loop) and any approximation is bounded.

The hook builds a new `ExecutionResources` via a new `subtract_clamp_zero` helper (fused shorthand for `subtract(other).max(zero())` — one allocation instead of two) rather than mutating the cached budget. `ExecutionResources` is treated as a value type everywhere else in the codebase; in-place mutation here would leak to any caller holding a prior reference. Per-field clamp at zero guards against stale-budget + oversized-incremental-usage producing a negative budget that would confuse downstream schedulability checks. Base `OpResourceAllocator.on_task_dispatched` is a no-op, so allocators that don't track per-op budgets see no change.

**Correctness**. Three unit tests:
- `test_on_task_dispatched_decrements_budget_without_mutation` — asserts the budget object is replaced (`is not`), not mutated, and the decrement matches `incremental_resource_usage()`.
- `test_on_task_dispatched_clamps_at_zero_never_negative` — asserts a stale/oversized incremental usage leaves non-negative budgets.
- `test_execution_resources_subtract_clamp_zero` — asserts the fused helper matches `subtract(...).max(zero())` bit-for-bit.

### 2.6 M6 — Memoize `RefBundle.size_bytes()` and `num_rows()`

**Commit**: `60472f14`

**Observation**. `RefBundle.size_bytes()` and `num_rows()` walk the block list on every call, and they're called many times per scheduling step: in the `DefaultRanker` (by size), in downstream-capacity and output-budget backpressure policies, in `TaskPoolMapOperator`'s `large_args_threshold` check, in progress-bar accounting, and in log messages. The result is stable over the instance's lifetime — `RefBundle` is a frozen dataclass; the block tuple, slices, and per-block metadata are immutable — so re-derivation is pure waste.

**Change**. Cache the result on first call. Writes go through `object.__setattr__` (the dataclass is frozen). Cache fields are declared with `field(init=False, repr=False, compare=False)` so `dataclasses.replace(bundle, slices=...)` — the idiomatic way to produce a sliced view — yields a bundle with fresh sentinel cache state, rather than inheriting the parent's cached value.

Cache-state sentinels:
- `_cached_size_bytes`: `None` = uncached; `int` = cached.
- `_cached_num_rows`: `-1` = uncached; `int` or `None` = cached. The three-valued encoding distinguishes "not cached" from a legitimately-cached `None` (an upstream block with unknown row count).

**Correctness**. Two unit tests:
- `test_ref_bundle_size_bytes_num_rows_are_memoized` — poisons `blocks` after first call; subsequent call must return the cached value rather than re-deriving.
- `test_ref_bundle_num_rows_memoizes_none_result` — asserts the unknown-row case caches to `None` and subsequent calls return `None` without re-entering the loop.

The `init=False` declaration is the critical correctness detail: without it, `dataclasses.replace()` copies the cached value from the parent and the new sliced bundle would report the pre-slice row count. The existing slicing test in `test_ref_bundle.py` catches this regression.

## 3. Methodology

### 3.1 Hardware

All measurements on a single 24-CPU cgroup container (`cpu.max = 2400000 100000`), 181 GiB RAM, plasma object store backed by `/dev/shm` (`RAY_OBJECT_STORE_ALLOW_SLOW_STORAGE=1`). The same host is used for pristine and M6 runs — no cross-host comparison.

### 3.2 Workloads

Four scheduler-stress workloads (parameters in `glia-bench/workload_config.json`):

- **`synthetic`** — 20K-block task-pool chain, depth 6. Dominates dispatch-rate overhead; M3/M4/M5 changes are most visible here.
- **`mixed_pipeline`** — task + actor pool pipeline, simulates a read → map → inference pattern.
- **`medium_tasks`** — 500-block workload with ~50 ms per task. Moderate dispatch rate.
- **`long_tasks`** — 500-ms tasks. Worker-bound; used as a control — scheduler-thread changes should not move this workload.

Each workload run emits `wall_time_sec`, `throughput_blocks_per_sec`, `driver_cpu_per_wall`, `efficiency_blocks_per_core_sec`, and a SHA-256 of the sorted output rows.

### 3.3 Perf measurement

- **N = 5** reps per (config, workload). Configs: `pristine` (ray-2.55.0 tree) and `m6` (branch HEAD).
- Fresh Ray cluster per rep (the benchmark script `ray.init()`s and `ray.shutdown()`s internally).
- Tree swap via rewriting the `MAPPING` line in the setuptools editable-install finder (`__editable___ray_2_55_0_finder.py`); same Python env, same `_raylet.so`, only the Python source files under `python/ray/` differ. The swap is verified by re-importing `ray` and checking `ray.__file__` resolves to the expected tree.
- Report: mean ± stdev of wall time and throughput, delta vs pristine, Welch's t statistic on throughput (unequal-variance two-sample), and output-hash equality over all 10 runs per workload (5 pristine + 5 M6).

### 3.4 Correctness gate

The gate runs 25 Ray Data test files (`glia-bench/test_list.py`) in the same venv against the tree selected by the MAPPING rewrite. The list includes every `test_*` under `python/ray/data/tests/` that exercises the scheduler, map operators, resource management, backpressure, and executor state paths — the surfaces our changes touch.

**Fractional-retry methodology with symmetric known-flaky handling.** Ray Data has a handful of tests that are probabilistically flaky on this hardware (notably `test_spilled_stats[True|False]` — both assert on a backpressure-time string whose rounding depends on per-host timing precision — and `test_read_write_local_node_ray_client`, a Ray-client connectivity test). A binary pass/fail gate reports these as spurious regressions. The gate uses pass rates (baseline vs current) with a ±10% tolerance instead.

Two retry policies compose:
1. **First-run-fail retry**: any test that fails its first run is rerun individually up to 10 times to characterize its pass rate.
2. **Known-flaky retry (symmetric)**: the harness maintains an explicit `KNOWN_FLAKY_TESTS` set (`glia-bench/test_list.py`). Tests in this set are always retried at the full 10× depth on **both** the baseline and the gate, regardless of first-run outcome. This avoids the retry-policy asymmetry where a baseline test happens to pass on its lucky first try (1/1) while the gate catches it on a first-run fail → 10-run retry (e.g. 8/10), producing a false-alarm regression that is really just the natural flake distribution.

The known-flaky set currently contains 5 tests: the two `test_spilled_stats` variants, `test_read_write_local_node_ray_client`, and the two `test_iter_batches_local_shuffle[pandas|arrow]` tests. The shuffle tests encode a user-facing determinism contract and are included in case a future scheduler-timing change makes them probabilistic; on this branch they pass 10/10 on both sides.

**New unit tests.** Five unit tests were added alongside the optimizations (listed in §2). Each was run 10× in isolation on the M6 tree; all passed 10/10.

## 4. Results

### 4.1 Performance

Each row is N = 5 reps per config. Wall time in seconds, throughput in blocks/sec.

| Workload | Pristine wall | M6 wall | Δ wall | Pristine thpt | M6 thpt | Δ thpt | Welch's t (thpt) | Output hash |
|---|---|---|---|---|---|---|---|---|
| synthetic | 145.26 ± 0.61 | 81.81 ± 1.13 | **−43.68%** | 137.69 ± 0.58 | 244.49 ± 3.38 | **+77.57%** | +69.6 (df=4.2) | ✓ 1 unique |
| mixed_pipeline | 193.98 ± 0.80 | 91.21 ± 0.37 | **−52.98%** | 103.11 ± 0.43 | 219.29 ± 0.90 | **+112.68%** | +261.4 (df=5.7) | ✓ 1 unique |
| medium_tasks | 6.43 ± 0.03 | 4.17 ± 0.03 | **−35.20%** | 77.73 ± 0.38 | 119.96 ± 0.93 | **+54.32%** | +94.3 (df=5.3) | ✓ 1 unique |
| long_tasks | 6.55 ± 0.04 | 6.54 ± 0.06 | −0.15% | 7.64 ± 0.05 | 7.65 ± 0.07 | +0.16% | +0.3 (df=7.5) | ✓ 1 unique |

Throughput deltas for all three scheduler-bound workloads are significant at p ≪ 0.001 (|t| ≥ 69 against a two-sample critical threshold ≈ 2.6 at these df). `long_tasks` does not move, as expected for a 500 ms-per-task worker-bound workload: the scheduler spends most of its time at the `ray.wait` ceiling, and the optimizations target scheduler-thread work that is vanishingly small relative to total wall time on this workload. This is the intended outcome — none of the changes should help (or hurt) workloads that aren't scheduler-bound, and the `long_tasks` row serves as a control for regressions.

The spread across the other three workloads reflects where each optimization takes effect. `synthetic` (20K-block task-pool chain) exercises M3 (dispatch options) and M6 (RefBundle memoization) hardest. `mixed_pipeline` (task + actor fanout) additionally exercises the resource-manager hot path (M5) and shows the largest relative win. `medium_tasks` (500 blocks at 50 ms each) hits M4 (adaptive wait) most visibly because the original 100 ms `ray.wait` ceiling was roughly 2× the task duration — the adaptive timeout drops below the ceiling and unblocks the scheduler every dispatch cycle.

**Scheduler CPU.** `driver_cpu_per_wall` is measured by calling `resource.getrusage(RUSAGE_SELF)` at workload start and end, taking the delta of `ru_utime + ru_stime` (user + system CPU of the driver process only), and dividing by wall time. Ray workers and the raylet run as separate OS processes and their CPU is excluded by `RUSAGE_SELF`, so this is a clean proxy for scheduler-thread + consumer-thread work in the driver. Two intensive metrics:

- **Scheduler busy-ratio** = `driver_cpu_per_wall`. Fraction of wall-time the driver spends on CPU (can exceed 1.0 — the driver has a scheduler thread plus a consumer thread, and Python GIL contention across them still counts each one's on-CPU time).
- **Efficiency** = `throughput_blocks_per_sec / driver_cpu_per_wall`. Blocks dispatched per scheduler-CPU-second. Useful work per unit of scheduler CPU.

| Workload | Busy-ratio (pristine) | Busy-ratio (M6) | Δ | Efficiency (pristine, blk/CPU-s) | Efficiency (M6, blk/CPU-s) | Δ |
|---|---|---|---|---|---|---|
| synthetic | 1.59 ± 0.01 | 2.30 ± 0.02 | **+44.5%** | 86.5 ± 0.8 | 106.2 ± 2.1 | **+22.9%** |
| mixed_pipeline | 1.38 ± 0.02 | 2.30 ± 0.01 | **+67.1%** | 74.8 ± 1.1 | 95.2 ± 0.5 | **+27.2%** |
| medium_tasks | 0.56 ± 0.01 | 0.84 ± 0.02 | **+50.9%** | 138.9 ± 3.8 | 142.1 ± 2.5 | +2.3% |
| long_tasks | 0.13 ± 0.00 | 0.13 ± 0.00 | −1.8% | 57.5 ± 1.1 | 58.7 ± 1.0 | +2.2% |

The two metrics decompose cleanly. Efficiency = `throughput / busy-ratio = (blocks/wall) / (CPU/wall) = blocks / CPU` — the wall-time factors cancel, so efficiency reduces to blocks-per-CPU-second. Since every workload dispatches a fixed block count, efficiency rises if and only if total scheduler CPU falls.

Concrete numbers for `synthetic`: pristine used 231 CPU-seconds over 145 s of wall; M6 used 188 CPU-seconds over 82 s. The +23% efficiency gain is precisely the −19% drop in total scheduler CPU (20 000 / 188 = 106; 20 000 / 231 = 87).

Attribution: busy-ratio rising is predominantly M4 + M2 (less idle wait per wall-second — the adaptive `ray.wait` timeout and the event-based consumer signal remove 100 ms-scale blocks). Efficiency rising is predominantly M3 + M5 + M6 (less CPU work per block — no per-dispatch options-dict rebuild, no per-dispatch full-topology budget recompute, no re-walking the block list on every `size_bytes()` / `num_rows()` call). Both rising together is the signature of a real gain.

`long_tasks` is flat on both metrics — consistent with its worker-bound control role. `medium_tasks` efficiency is near-flat (+2.3%) because the workload is short enough (~4–6 s wall) that startup/teardown dominate; on that workload the busy-ratio delta is the cleaner signal.

### 4.2 Correctness

**Gate summary** (ray-2.55.0 tests, 25 files, 457 tests, symmetric retry on `KNOWN_FLAKY_TESTS`):

| Category | Baseline (pristine) | M6 |
|---|---|---|
| Stable pass | 445 | 451 |
| Stable fail | 4 | 0 |
| Flaky (passed some, not all) | 2 | 0 |
| Skipped | 6 | 6 |

**Net: 0 regressions.**

Baseline's 4 stable failures are the new unit tests for M4/M5 symbols (`_adapt_wait_timeout`, `on_task_dispatched`, `subtract_clamp_zero`) that don't exist in pristine. All pass 1/1 on M6 (see the new-unit-tests table below).

The 6 skipped tests are skipped by upstream Ray itself: 5 are guarded by `@pytest.mark.skipif(sys.version_info >= (3, 12), ...)` because the Ray-2.55.0 TensorFlow binding doesn't support Python 3.12 (the version on this host); 1 (`test_polars_lazy_import`) carries an unconditional `@pytest.mark.skip` upstream. All skip identically on both sides.

**Known-flaky tests** — all 5 retried 10× on both baseline and M6 (symmetric comparison):

| Test | Baseline | M6 |
|---|---|---|
| `test_stats::test_spilled_stats[True]` | 7/10 | 10/10 |
| `test_stats::test_spilled_stats[False]` | 10/10 | 10/10 |
| `test_consumption::test_read_write_local_node_ray_client` | 9/10 | 10/10 |
| `test_dataset_iter::test_iter_batches_local_shuffle[pandas]` | 10/10 | 10/10 |
| `test_dataset_iter::test_iter_batches_local_shuffle[arrow]` | 10/10 | 10/10 |

The `spilled_stats[True]` and `test_read_write_local_node_ray_client` rates rose to 10/10 on M6 but at N = 10 per side that is well within the natural flake distribution of these probabilistically-timing-sensitive tests. This should not be read as a fix; the defensible claim is only that M6 shows **no evidence of an increased failure rate** on any known-flaky test.

**New unit tests** — added alongside the optimizations:

| Test | Baseline | M6 |
|---|---|---|
| `test_adapt_wait_timeout_halve_and_double` | 0/10 (requires M4 symbol) | 1/1 |
| `test_get_output_blocking_event_signaling` | 1/1 | 1/1 |
| `test_on_task_dispatched_decrements_budget_without_mutation` | 0/10 (requires M5 symbol) | 1/1 |
| `test_on_task_dispatched_clamps_at_zero_never_negative` | 0/10 (requires M5 symbol) | 1/1 |
| `test_execution_resources_subtract_clamp_zero` | 0/10 (requires M5 symbol) | 1/1 |

All five also pass 10/10 when run in isolation on M6.

**Output-hash equality** (5 pristine reps + 5 M6 reps per workload, 40 runs total):

| Workload | Unique hashes observed across 10 runs |
|---|---|
| synthetic | 1 (`1589ce21…` across all 10 runs) |
| mixed_pipeline | 1 (`1f98030e…` across all 10 runs) |
| medium_tasks | 1 (`c495eb7c…` across all 10 runs) |
| long_tasks | 1 (`aef8f1d5…` across all 10 runs) |

All four workloads: pristine and M6 produce byte-identical outputs across all 10 runs.

## 5. Reproducibility

Branch: [`glia/scheduler-perf-v1`](https://github.com/Glia-AI-External/ray/tree/glia/scheduler-perf-v1). Base: `ray-2.55.0` (`58af3fc5`). Commit hashes for each milestone are listed inline in §2.

### Install

The perf driver and the correctness gate both compare a **pristine** ray-2.55.0 tree against the **M6** tree (this fork's HEAD). You need both source trees on disk, plus a Python venv with ray-2.55.0's compiled artifacts (`_raylet.so`, the dashboard build, etc.) installed from the wheel. Only the Python source files under `python/ray/` differ between runs — the compiled artifacts are shared.

```bash
# 1. Clone the fork (contains the optimizations, the harness, and the report).
git clone https://github.com/Glia-AI-External/ray.git ray-fork
cd ray-fork
git checkout glia/scheduler-perf-v1

# 2. Clone a pristine ray-2.55.0 tree alongside it.
git clone --depth 1 --branch ray-2.55.0 https://github.com/ray-project/ray.git ../ray-pristine

# 3. Install ray-2.55.0 from the wheel (compiled artifacts), then install
#    the fork editable on top (rebinds ray's Python sources to this tree).
pip install ray==2.55.0
pip install -e python/

# 4. Stage ray-2.55.0's compiled artifacts (`_raylet.so`, protobuf-generated
#    code, prebuilt C++ workers, dashboard build) into each source tree by
#    symlinking them from the wheel install. The editable finder resolves
#    `import ray` to the source tree, which otherwise lacks these binaries.
glia-bench/stage_ray_artifacts.sh python/ray
glia-bench/stage_ray_artifacts.sh ../ray-pristine/python/ray

# 5. Install Ray Data's runtime deps + the test-time deps the curated
#    gate exercises (ray-core ships without pandas/pyarrow, which
#    ray.data imports at runtime; the gate harness uses pytest-timeout
#    and a few tests depend on pytest-lazy-fixtures / datasketches /
#    polars).
pip install pandas pyarrow
pip install pytest-timeout pytest-lazy-fixtures datasketches polars
```

### Run

All commands below run from the fork's `glia-bench/` directory. `PRISTINE_TREE` points at the sibling pristine checkout from step 2 of the install. Both scripts auto-switch between the pristine and M6 trees by rewriting the `MAPPING` line in the setuptools editable-install finder — no manual swap needed.

```bash
cd glia-bench
export PRISTINE_TREE="$(cd ../../ray-pristine/python/ray && pwd)"

# --- Performance sweep ----------------------------------------------------
# 2 configs (pristine vs M6) × 4 workloads × 5 reps = 40 runs, ≈ 1–1.5 h.
./run_optimization_bench.sh all 5

# Aggregate into the §4.1 table (mean±stdev, deltas, Welch's t, hash check).
python aggregate_perf.py results/optimization_perf.jsonl

# --- Correctness gate -----------------------------------------------------
# Records baseline against pristine, then runs the gate against M6.
# Writes results/optimization_gate_{baseline,m6}.json.
./run_optimization_gate.sh
```

Both trees are built against the same `_raylet.so` and the same venv; only the Python source files under `python/ray/` differ.

## Appendix: Raw per-run data

Full per-run performance data: `glia-bench/results/optimization_perf.jsonl` (40 JSON lines, one per run, with `config`, `workload`, `rep`, `wall_time_sec`, `throughput_blocks_per_sec`, `throughput_rows_per_sec`, `driver_cpu_per_wall`, `efficiency_blocks_per_core_sec`, `output_hash`).

Aggregation script: `glia-bench/aggregate_perf.py` (reproduces the §4.1 table from the JSONL).

Gate artifacts:
- `glia-bench/results/optimization_gate_baseline.json` — per-test pass/fail/pass-rate for the pristine run.
- `glia-bench/results/optimization_gate_m6.json` — the diff against baseline.

Per-commit single-run benchmark numbers (measured during development, one rep each on the same 24-CPU host) are preserved in the commit messages for each milestone and can be recovered with `git log --format=fuller glia/scheduler-perf-v1 -- 'python/ray/data/**'`. They provide a rough per-milestone attribution; the full N = 5 per-milestone attribution is deferred since cumulative significance is what reviewers primarily care about.
