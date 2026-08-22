"""Generate banking account-state events for the local Structured Streaming job."""
import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

STATUSES = ["ACTIVE", "SUSPENDED", "DORMANT", "CLOSED"]
TIERS = ["STANDARD", "PREMIUM", "PRIORITY"]
REGIONS = ["NORTH", "SOUTH", "EAST", "WEST"]
ACCOUNT_POOL = [f"ACC-{i:05d}" for i in range(1, 51)]


def make_event(late: bool):
    now = datetime.now(timezone.utc)
    event_time = now - timedelta(minutes=random.randint(1, 8)) if late else now

    operation = random.choices(
        ["INSERT", "UPDATE", "DELETE"],
        weights=[0.20, 0.70, 0.10],
        k=1,
    )[0]

    return {
        "event_id": str(uuid.uuid4()),
        "account_id": random.choice(ACCOUNT_POOL),
        "operation": operation,
        "event_time": event_time.isoformat(),
        "account_status": random.choice(STATUSES),
        "account_tier": random.choice(TIERS),
        "branch_region": random.choice(REGIONS),
        "source": "debezium",
        "schema_version": "1",
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--events-per-second", type=int, default=5)
    parser.add_argument("--late-event-rate", type=float, default=0.1)
    parser.add_argument("--duration-seconds", type=int, default=60)
    parser.add_argument("--out-dir", type=str, default="data/stream_source")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    end_time = time.time() + args.duration_seconds
    tick = 0

    while time.time() < end_time:
        events = [
            make_event(late=random.random() < args.late_event_rate)
            for _ in range(args.events_per_second)
        ]
        file_path = out_dir / f"events_{tick:06d}.json"
        with open(file_path, "w", encoding="utf-8") as f:
            for event in events:
                f.write(json.dumps(event) + "\n")
        tick += 1
        time.sleep(1)

    print(f"Simulator finished after {tick} seconds, wrote to {out_dir}")


if __name__ == "__main__":
    main()
