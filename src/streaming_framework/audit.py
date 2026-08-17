"""Low-overhead streaming audit writer.

Unlike the old batch framework, this writer does not create Spark tables or
launch Spark jobs. One completed micro-batch produces one atomic JSON record.
The same file format can be shipped to a centralized observability sink later.
"""
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .context import StreamingBatchContext
from .metrics import BatchMetrics

class StreamingAudit:
    def __init__(self, context: StreamingBatchContext):
        self.context = context

    def record(self, metrics: BatchMetrics, *, duration_seconds: float) -> dict[str, Any]:
        record = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "pipeline_name": self.context.pipeline_name,
            "execution_mode": "micro_batch",
            "trigger_type": "processing_time",
            "batch_id": self.context.batch_id,
            "status": metrics.status,
            "input_rows": metrics.input_rows,
            "new_rows": metrics.new_rows,
            "impacted_accounts": metrics.impacted_accounts,
            "history_rows": metrics.history_rows,
            "current_rows": metrics.current_rows,
            "duration_seconds": round(duration_seconds, 3),
            "checkpoint_path": str(self.context.checkpoint_path),
            "target_path": str(self.context.target_path),
        }
        path = Path(self.context.audit_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(prefix=".audit-", suffix=".tmp", dir=str(path.parent))
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(json.dumps(record) + "\n")
                handle.flush()
                os.fsync(handle.fileno())
            with path.open("a", encoding="utf-8") as target, open(tmp, "r", encoding="utf-8") as source:
                target.write(source.read())
                target.flush()
                os.fsync(target.fileno())
        finally:
            try:
                os.unlink(tmp)
            except FileNotFoundError:
                pass
        return record
