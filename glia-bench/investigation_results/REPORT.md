# Investigation: reviewer-reported actor-pool backpressure regression on M6

**Branch:** `glia/investigate-m5-backpressure` (local only — NOT pushed to origin per instruction)
**Trigger:** Reviewer running `map_benchmark.py --api map_batches --batch-format numpy --compute actors --sf 1000 --repeat-inputs 1 --concurrency 1024 2048` on a ~100-node GCE cluster reports "issues with the backpressure policy." Hypothesis going in: M5's `on_task_dispatched` hook is a no-op for actor-pool ops (`ActorPoolMapOperator.incremental_resource_usage()` returns all zeros), so budgets only refresh on scheduling-step boundaries; on a large actor pool with autoscaling + long sleeps this could cause `ReservationOpResourceAllocator` drift.

## Setup

- Devpod: 24-CPU cgroup, 1.8 TiB RAM available, 8 GB /dev/shm (plasma spills to /tmp).
- `glia-bench/map_benchmark_local.py` — new synthetic repro that mirrors `map_benchmark.py`'s pipeline shape: `range → map_batches(task: to_column00) → map_batches(IncrementBatch, actors, autoscale) → map_batches(dummy_write) → drain`. `IncrementBatch` class is byte-for-byte identical to map_benchmark.py's. Knobs: `--sleep-ms`, `--model-gb`, `--num-rows`, `--num-blocks`, `--concurrency-min/max`.

## Runs

### Config 1 — baseline: 5M rows / 500 blocks, 100 MB model, 50 ms sleep, concurrency [1, 16]

| config | N | wall mean | wall stdev | busy-ratio | notes |
|---|---|---|---|---|---|
| pristine | 3 | 10.37 s | 0.45 | 0.625 | clean logs |
| M6 | 3 | **8.02 s** | 0.08 | 0.822 | clean logs |

**Δ wall −22.6%, Δ busy +31.5%.** Consistent with the scheduler optimizations' intended effect; no backpressure regression.

### Config 2 — stress: 20M rows / 2000 blocks, 100 MB model, 50 ms sleep, concurrency [1, 32] (pool caps at ~22 due to 24-CPU cgroup)

| config | N | wall mean | busy-ratio | notes |
|---|---|---|---|---|
| pristine | 2 | 29.27 s | 0.84 | clean logs |
| M6 | 2 | **15.01 s** | 1.44 | clean logs |

**Δ wall −48.7%.** M6 wins even more decisively when there's more work to schedule. Still no backpressure signals.

## Log inspection

Grepped stderr of every run for `backpressur`, `can_add_input`, `budget`, `reservation`, `over-provision`, `spilled`, `warn`, `error`. Only two classes of message appeared, in both pristine and M6 runs:

- `services.py:2213 WARNING: The object store is using /tmp/ray instead of /dev/shm because /dev/shm has only 8589910016 bytes available.` Environmental, present on every run on this devpod regardless of config.
- `util.py:642 WARNING: The argument concurrency is deprecated in Ray 2.51. Please specify argument compute instead.` Ray 2.55 deprecation of the API we're calling; not relevant to scheduling.

No `ReservationOpResourceAllocator`, `DownstreamCapacityBackpressurePolicy`, budget, or backpressure-policy messages in either tree, at either config.

## Conclusion

**The reviewer's reported bug does not reproduce on a single-node (24-CPU cgroup) device with a close synthetic analog of his workload.** M6 is materially *faster* than pristine on this machine with the same actor-pool pipeline shape, scaling from [1, 16] to [1, 32] actor pool, 5M to 20M rows, and 50 ms per-task sleep. No backpressure policy fires on either tree.

This implies the bug is **scale-dependent** and likely needs multi-node conditions to reproduce. Plausible scale-only mechanisms:

1. **Cross-node object-store accounting.** On a 100-node cluster, each actor's 1 GB model is stored per-node; object-store pressure accounting involves cross-raylet coordination. Single-node can't stress this.
2. **Autoscaling actor pool at scale.** His config uses `--concurrency 1024 2048`. Going from 1 to 1024 actors on a real cluster takes many minutes of autoscaler decisions and cross-node actor creation; each step changes `current_resource_usage()` visibly. Our 1→22 local autoscale is too small to expose whatever timing-sensitive drift scale triggers.
3. **Autoscaler + resource-manager feedback.** `ReservationOpResourceAllocator` gates on budgets that are approximate between scheduling-step boundaries. On a large cluster, scheduling steps can be many-dispatches long (more drift per step) and the autoscaler makes its own decisions between steps. Potential oscillation.
4. **Object-store spilling under memory pressure.** SF=1000 is ~1 TB; on 100 nodes at 8 CPU each, per-node object store is fractional. Spilling triggers backpressure differently than on a single-node setup with 1.8 TiB RAM.

## Recommendation

Bug is not reproducible with the tooling we have on a single node. Two paths forward:

1. **Ask the reviewer for logs + symptom specifics** (which policy fires, hang vs slow vs spilling, whether `--compute tasks` also fails on his cluster). His cluster logs are the most efficient path to a root cause.
2. **Run the actual `map_benchmark.py`** with scaled-down params (SF=1, concurrency up to 32) on the bigger 64-vCPU EC2 box we have. That's closer to his workload but still single-node; would rule out whether the bug is data-shape-related (parquet vs range, S3 reads) vs scale-only.

Do NOT push any of this to `glia/scheduler-perf-v1` until we have a confirmed cause. This branch (`glia/investigate-m5-backpressure`) stays local-only.

## Artifacts

- `glia-bench/map_benchmark_local.py` — the synthetic repro script.
- `glia-bench/investigation_results/repro_{pristine,m6}_{1,2,3}.json` — per-run JSON for Config 1.
- `glia-bench/investigation_results/repro_{pristine,m6}_1.log` — full stderr of one run per tree (the log-search source of truth).
