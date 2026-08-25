"""Measure event-file -> successful Structured Streaming micro-batch latency."""

import argparse
import json
import shutil
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pipeline.config import StreamingConfig
from pipeline.spark_session import get_spark
from pipeline.streaming_job import build_and_start_stream


def percentile(values, p):
    """Linear-interpolation percentile."""
    ordered = sorted(values)
    if not ordered:
        raise ValueError("No latency measurements available")

    if len(ordered) == 1:
        return ordered[0]

    rank = (len(ordered) - 1) * p
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = rank - lower

    return ordered[lower] + (ordered[upper] - ordered[lower]) * fraction


def run_once(events_count, trigger, timeout, run_number):
    root = Path(tempfile.mkdtemp(prefix="streaming-e2e-"))
    config = StreamingConfig(base_path=root)
    config.trigger_interval = trigger
    config.max_files_per_trigger = 1

    spark = get_spark(f"streaming-e2e-latency-{run_number}")
    query = build_and_start_stream(spark, config)

    try:
        events = []

        for i in range(events_count):
            events.append({
                "event_id": str(uuid.uuid4()),
                "account_id": f"ACC-{i % 10:05d}",
                "event_time": datetime.now(timezone.utc).isoformat(),
                "account_status": "ACTIVE" if i % 2 == 0 else "SUSPENDED",
                "account_tier": "PREMIUM",
                "branch_region": "SOUTH",
                "customer_segment": "RETAIL",
            })

        first_sent_at = time.perf_counter()

        path = config.stream_source_path / "benchmark_burst.json"
        path.write_text(
            "\n".join(json.dumps(e) for e in events) + "\n",
            encoding="utf-8",
        )

        deadline = time.perf_counter() + timeout
        committed_rows = 0
        last_batch_id = -1
        committed_latency = None

        while time.perf_counter() < deadline:
            if not query.isActive:
                raise RuntimeError(
                    f"streaming query stopped: {query.exception()}"
                )

            progress = query.lastProgress

            if progress:
                batch_id = int(progress.get("batchId", -1))
                input_rows = int(progress.get("numInputRows", 0))

                if batch_id > last_batch_id and input_rows > 0:
                    last_batch_id = batch_id
                    committed_rows += input_rows

                    print(
                        f"run={run_number} "
                        f"batch_id={batch_id} "
                        f"input_rows={input_rows} "
                        f"cumulative_rows={committed_rows}"
                    )

                    if committed_rows >= events_count:
                        committed_latency = (
                            time.perf_counter() - first_sent_at
                        )
                        break

            time.sleep(0.1)

        if committed_latency is None:
            raise RuntimeError(
                f"workload of {events_count} events was not committed "
                f"within {timeout}s"
            )

        print(
            f"run={run_number} "
            f"events={events_count} "
            f"last_batch_id={last_batch_id} "
            f"committed_rows={committed_rows} "
            f"e2e_latency={committed_latency:.2f}s"
        )

        return committed_latency

    finally:
        query.stop()
        spark.stop()
        shutil.rmtree(root, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--events", type=int, default=20)
    parser.add_argument("--trigger", default="5 seconds")
    parser.add_argument("--timeout", type=int, default=120)
    parser.add_argument("--runs", type=int, default=30)

    args = parser.parse_args()

    if args.runs < 1:
        raise ValueError("--runs must be at least 1")

    print("=" * 60)
    print("STREAMING E2E LATENCY BENCHMARK")
    print("=" * 60)
    print(f"events per run : {args.events}")
    print(f"trigger        : {args.trigger}")
    print(f"runs           : {args.runs}")
    print(f"timeout/run    : {args.timeout}s")
    print("=" * 60)

    latencies = []

    for run_number in range(1, args.runs + 1):
        print(f"\n--- RUN {run_number}/{args.runs} ---")

        latency = run_once(
            events_count=args.events,
            trigger=args.trigger,
            timeout=args.timeout,
            run_number=run_number,
        )

        latencies.append(latency)

    print("\n" + "=" * 60)
    print("LATENCY RESULTS")
    print("=" * 60)

    print(f"runs     : {len(latencies)}")
    print(f"events   : {args.events}")
    print(f"min      : {min(latencies):.2f}s")
    print(f"average  : {sum(latencies) / len(latencies):.2f}s")
    print(f"p50      : {percentile(latencies, 0.50):.2f}s")
    print(f"p95      : {percentile(latencies, 0.95):.2f}s")
    print(f"max      : {max(latencies):.2f}s")

    print("=" * 60)


if __name__ == "__main__":
    main()
