"""Measure event-file -> successful Structured Streaming micro-batch latency.

The benchmark deliberately avoids polling Delta with separate Spark actions while
the streaming query is running. A successful StreamingQuery progress record means
the foreachBatch callback completed successfully, including its Delta writes.
"""

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


def _latest_progress(query):
    progress = query.lastProgress
    return progress if progress else None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=10)
    parser.add_argument("--trigger", default="5 seconds")
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args()

    root = Path(tempfile.mkdtemp(prefix="streaming-e2e-"))
    config = StreamingConfig(base_path=root)
    config.trigger_interval = args.trigger

    spark = get_spark("streaming-e2e-latency")
    query = build_and_start_stream(spark, config)

    latencies = []

    try:
        previous_batch_id = -1

        for i in range(args.events):
            event_id = str(uuid.uuid4())

            event = {
                "event_id": event_id,
                "account_id": f"ACC-{i % 10:05d}",
                "event_time": datetime.now(timezone.utc).isoformat(),
                "account_status": "ACTIVE" if i % 2 == 0 else "SUSPENDED",
                "account_tier": "PREMIUM",
                "branch_region": "SOUTH",
                "customer_segment": "RETAIL",
            }

            path = config.stream_source_path / f"benchmark_{i:06d}.json"

            sent_at = time.perf_counter()
            path.write_text(
                json.dumps(event) + "\n",
                encoding="utf-8",
            )

            deadline = time.perf_counter() + args.timeout
            committed = False

            while time.perf_counter() < deadline:
                if not query.isActive:
                    exc = query.exception()
                    raise RuntimeError(
                        f"streaming query stopped before event {event_id} "
                        f"was committed: {exc}"
                    )

                progress = _latest_progress(query)

                if progress:
                    batch_id = int(progress.get("batchId", -1))
                    input_rows = int(progress.get("numInputRows", 0))

                    if batch_id > previous_batch_id and input_rows > 0:
                        latency = time.perf_counter() - sent_at
                        latencies.append(latency)

                        print(
                            f"event={i + 1} "
                            f"batch_id={batch_id} "
                            f"input_rows={input_rows} "
                            f"latency={latency:.2f}s"
                        )

                        previous_batch_id = batch_id
                        committed = True
                        break

                time.sleep(0.1)

            if not committed:
                raise RuntimeError(
                    f"event {event_id} was not committed within "
                    f"{args.timeout} seconds"
                )

        ordered = sorted(latencies)

        p50 = ordered[len(ordered) // 2]
        p95 = ordered[max(0, int(len(ordered) * 0.95) - 1)]

        print()
        print(
            f"events={len(ordered)} "
            f"p50={p50:.2f}s "
            f"p95={p95:.2f}s "
            f"max={max(ordered):.2f}s"
        )

    finally:
        query.stop()
        spark.stop()
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
