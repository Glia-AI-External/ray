# glia-bench

Reproducible benchmark + correctness harness for evaluating the
scheduler optimizations proposed in this branch against stock Ray 2.55.0.

## What's here

- `benchmark_scheduler.py` — runs one of four Ray Data workloads against
  the currently installed `ray`. Reports throughput, driver CPU per
  wall-second, scheduler efficiency, and a deterministic SHA-256 of the
  dataset's output rows (for correctness validation).
- `workload_config.json` — parameters for each of the four workloads.
- `run_bench.sh` — wrapper that runs all four workloads, one run each
  by default; `./run_bench.sh all 5` for N=5.
- `run_gated_tests.py` — runs a curated subset of the Ray Data test
  suite and compares per-test pass/fail against a baseline, so
  regressions in the scheduler path surface as concrete failed tests.
  Tests in `test_list.KNOWN_FLAKY_TESTS` are always retried at the
  full flaky-retry depth (10×) on both the baseline and the gate, so
  comparisons on probabilistically-flaky tests (spilled-stats timing,
  shuffle determinism) are symmetric and don't produce retry-policy
  false alarms.
- `test_list.py` — test file lists for the gates plus the
  `KNOWN_FLAKY_TESTS` set.

## The four workloads

1. **synthetic** — `range(20K blocks, 320M rows)` through a depth-6
   chain of cheap task-pool maps. Scheduler-bound; any op reordered or
   skipped changes the output hash.
2. **mixed_pipeline** — `range()` → task → actor-pool → task → materialize.
   Exercises actor-pool dispatch and asymmetric backpressure.
3. **medium_tasks** — 500 blocks of `sleep(50ms) + compute`.
   Representative of a production map-transform stage; scheduler
   latency matters.
4. **long_tasks** — 50 blocks of `sleep(500ms) + compute`. Workers
   dominate; scheduler should be idle. Used to catch optimizations
   that buy throughput by burning scheduler CPU.

## How to run

First, ensure Ray is installed editable from this checkout so that
changes in `python/ray/` are live:
```
pip install -e python/
python3 -c "import ray; print(ray.__file__)"   # should point inside this repo
```

Then:
```
./glia-bench/run_bench.sh                   # all four workloads, 1 run each
./glia-bench/run_bench.sh synthetic         # one workload
./glia-bench/run_bench.sh all 5             # all four, N=5 runs each
```
