"""Low-overhead streaming audit writer.

One completed micro-batch produces one JSON audit record.
Audit records are stored as individual JSON files so the writer works
with Databricks Unity Catalog Volumes without relying on append/seek
semantics of a single file.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .context import StreamingBatchContext
from .metrics import BatchMetrics


class StreamingAudit:
    def __init__(self, context: StreamingBatchContext):
        self.context = context

    def record(
        self,
        metrics: BatchMetrics,
        *,
        duration_seconds: float,
    ) -> dict[str, Any]:

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

        audit_dir = Path(self.context.audit_path)
        audit_dir.mkdir(parents=True, exist_ok=True)

        audit_file = audit_dir / f"batch_{self.context.batch_id}.json"

        # Write one independent file per completed micro-batch.
        audit_file.write_text(
            json.dumps(record) + "\n",
            encoding="utf-8",
        )

        return record
