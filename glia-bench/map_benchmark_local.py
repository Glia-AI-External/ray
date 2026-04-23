"""Local synthetic repro of the map_benchmark.py actor-pool regime.

Targets the same scheduler/backpressure code paths that
release/nightly_tests/dataset/map_benchmark.py --api map_batches
--batch-format numpy --compute actors exercises on a multi-node cluster,
but without S3 I/O so it runs on a single machine in minutes.

Pipeline shape (matches map_benchmark.py with a synthetic source):
    range(num_rows, num_blocks)
      -> map_batches(task, converts to {"column00": ...})
      -> map_batches(IncrementBatch, actors, concurrency=(min, max),
                     1 GB-configurable model, per-batch sleep_ms)
      -> map_batches(dummy_write, tasks)                # counts rows
      -> iter_internal_ref_bundles()                     # drain

What this exercises and why it matters for the M5 investigation:
  - Autoscaling actor pool. ActorPoolMapOperator.incremental_resource_usage
    returns all zeros, so M5's on_task_dispatched() is a no-op for actor
    ops — budget only refreshes on scheduling-step boundaries.
  - Long-ish per-task work (sleep_ms defaults to 50). This is exactly the
    regime where M4's adaptive ray.wait timeout drops below the 100 ms
    ceiling, so scheduling loops run 10-100x more often per wall-second
    and any backpressure decision is re-evaluated many times per second.
  - Model size per actor. 1 GB matches map_benchmark.py's MODEL_SIZE;
    100 MB is a lighter variant that fits in an 8 GB /dev/shm without
    spilling (useful for separating the backpressure-mechanism signal
    from object-store-pressure noise).

Knobs for sweeping the suspicious axes:
  --sleep-ms       0, 10, 50, 200   (M4 adaptive-wait interaction)
  --model-gb       0.1, 1, 4         (object store pressure)
  --concurrency    min max           (autoscaling range)
  --num-blocks     100-10000         (inqueue depth)

Output: prints one JSON line with wall time, throughput, driver CPU,
output row count, and the actor-pool scaling bounds. Captures any
backpressure-related log lines via a writer that tees the streaming
executor logger to stderr at INFO level.
"""

import argparse
import json
import logging
import resource
import sys
import time

import numpy


def _driver_cpu_snapshot():
    ru = resource.getrusage(resource.RUSAGE_SELF)
    return time.perf_counter(), ru.ru_utime + ru.ru_stime


def main(sleep_ms: int,
         model_bytes: int,
         num_rows: int,
         num_blocks: int,
         concurrency_min: int,
         concurrency_max: int,
         batch_size: int,
         object_store_gb: float = 0.0) -> dict:
    import ray
    import ray.data

    # Tee data/scheduler logs to stderr at INFO so any backpressure
    # messages land in the captured stderr of the run.
    for name in ("ray.data._internal.execution.streaming_executor",
                 "ray.data._internal.execution.resource_manager",
                 "ray.data._internal.execution.backpressure_policy"):
        logging.getLogger(name).setLevel(logging.INFO)

    init_kwargs = {}
    if object_store_gb > 0:
        # Constrain the plasma object store to force memory pressure. When
        # the sum (models + in-flight data + pending outputs) approaches the
        # budget, the ReservationOpResourceAllocator starts applying
        # backpressure — this is the regime the reviewer saw on his 100-node
        # cluster (481 GiB / 448 GiB object store, sustained spilling).
        init_kwargs["object_store_memory"] = int(object_store_gb * 1024**3)
    ray.init(ignore_reinit_error=True, **init_kwargs)

    dummy_model = numpy.zeros(model_bytes, dtype=numpy.int8)
    model_ref = ray.put(dummy_model)
    del dummy_model

    class IncrementBatch:
        """Mirror of map_benchmark.py's IncrementBatch class."""
        def __init__(self, model_ref, sleep_ms):
            self.model = ray.get(model_ref)   # realizes model_bytes in actor heap
            self.sleep_ms = sleep_ms

        def __call__(self, batch):
            if self.sleep_ms > 0:
                time.sleep(self.sleep_ms / 1000.0)
            batch["column00"] = batch["column00"] + 1
            return batch

    def to_column00(batch):
        # `ray.data.range()` produces rows with an `id` column. Rename to
        # `column00` so IncrementBatch (which matches map_benchmark.py
        # exactly) can operate on it.
        return {"column00": numpy.asarray(batch["id"])}

    def dummy_write(batch):
        return {"num_rows": [len(batch["column00"])]}

    cpu_w0, cpu_c0 = _driver_cpu_snapshot()
    start = time.perf_counter()

    ds = ray.data.range(num_rows, override_num_blocks=num_blocks)
    ds = ds.map_batches(to_column00, batch_format="numpy", batch_size=batch_size)
    ds = ds.map_batches(
        IncrementBatch,
        fn_constructor_args=[model_ref, sleep_ms],
        batch_format="numpy",
        batch_size=batch_size,
        concurrency=(concurrency_min, concurrency_max),
    )
    ds = ds.map_batches(dummy_write, batch_format="numpy", batch_size=batch_size)

    total_rows = 0
    for ref_bundle in ds.iter_internal_ref_bundles():
        for block_ref, meta in ref_bundle.blocks:
            total_rows += meta.num_rows or 0

    wall_time = time.perf_counter() - start
    cpu_w1, cpu_c1 = _driver_cpu_snapshot()

    driver_cpu_per_wall = (cpu_c1 - cpu_c0) / (cpu_w1 - cpu_w0) if cpu_w1 > cpu_w0 else 0.0

    return {
        "wall_time_sec": round(wall_time, 3),
        "throughput_blocks_per_sec": round(num_blocks / wall_time, 2),
        "throughput_rows_per_sec": round(total_rows / wall_time, 2),
        "driver_cpu_per_wall": round(driver_cpu_per_wall, 3),
        "total_rows_observed": total_rows,
        "num_blocks": num_blocks,
        "num_rows": num_rows,
        "concurrency": [concurrency_min, concurrency_max],
        "sleep_ms": sleep_ms,
        "model_bytes": model_bytes,
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--sleep-ms", type=int, default=50)
    ap.add_argument("--model-gb", type=float, default=0.1,
                    help="Model size per actor in GB. 0.1 fits comfortably "
                         "in an 8 GB /dev/shm; 1.0 matches map_benchmark.py.")
    ap.add_argument("--num-rows", type=int, default=5_000_000)
    ap.add_argument("--num-blocks", type=int, default=500)
    ap.add_argument("--concurrency-min", type=int, default=1)
    ap.add_argument("--concurrency-max", type=int, default=16)
    ap.add_argument("--batch-size", type=int, default=10_000)
    ap.add_argument("--object-store-gb", type=float, default=0.0,
                    help="If > 0, constrains Ray's object_store_memory to "
                         "this many GB. Used to force memory pressure that "
                         "matches what the reviewer saw on his cluster. "
                         "0 = use Ray's default (~30%% of RAM).")
    args = ap.parse_args()

    result = main(
        sleep_ms=args.sleep_ms,
        model_bytes=int(args.model_gb * 1024**3),
        num_rows=args.num_rows,
        num_blocks=args.num_blocks,
        concurrency_min=args.concurrency_min,
        concurrency_max=args.concurrency_max,
        batch_size=args.batch_size,
        object_store_gb=args.object_store_gb,
    )
    # Last line is machine-readable JSON; everything else goes to stderr.
    print(json.dumps(result))
