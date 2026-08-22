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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=20)
    parser.add_argument("--trigger", default="5 seconds")
    parser.add_argument("--timeout", type=int, default=120)
    args = parser.parse_args()

    root = Path(tempfile.mkdtemp(prefix="streaming-e2e-"))
    config = StreamingConfig(base_path=root)
    config.trigger_interval = args.trigger
    config.max_files_per_trigger = 1

    spark = get_spark("streaming-e2e-latency")
    query = build_and_start_stream(spark, config)

    try:
        events = []
        for i in range(args.events):
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
        path.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")

        deadline = time.perf_counter() + args.timeout
        committed_rows = 0
        last_batch_id = -1
        committed_latency = None

        while time.perf_counter() < deadline:
            if not query.isActive:
                raise RuntimeError(f"streaming query stopped: {query.exception()}")
            progress = query.lastProgress
            if progress:
                batch_id = int(progress.get("batchId", -1))
                input_rows = int(progress.get("numInputRows", 0))
                if batch_id > last_batch_id and input_rows > 0:
                    last_batch_id = batch_id
                    committed_rows += input_rows
                    print(f"batch_id={batch_id} input_rows={input_rows} cumulative_rows={committed_rows}")
                    if committed_rows >= args.events:
                        committed_latency = time.perf_counter() - first_sent_at
                        break
            time.sleep(0.1)

        if committed_latency is None:
            raise RuntimeError(f"workload of {args.events} events was not committed within {args.timeout}s")

        print(f"events={args.events} last_batch_id={last_batch_id} committed_rows={committed_rows} e2e_latency={committed_latency:.2f}s")
    finally:
        query.stop()
        spark.stop()
        shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
