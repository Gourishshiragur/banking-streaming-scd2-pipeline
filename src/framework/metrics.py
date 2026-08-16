"""
Enterprise Pipeline Metrics

Reusable across:
- Incremental Batch
- Micro Batch
- Streaming
"""

from __future__ import annotations

from typing import Any


class PipelineMetrics:
    """
    Collects reusable execution metrics for a pipeline run.
    """

    def __init__(self):

        self.reset()

    ####################################################################
    # Public API
    ####################################################################

    def reset(self) -> None:

        self.rows_read = 0

        self.rows_written = 0

        self.rows_updated = 0

        self.rows_inserted = 0

        self.rows_deleted = 0

        self.rows_rejected = 0

        self.files_processed = 0

        self.files_skipped = 0

        self.duplicates = 0

        self.invalid_records = 0

        self.partitions_processed = 0

        self.execution_time_seconds = 0.0

        self.custom_metrics = {}

    ####################################################################
    # Additional Metrics
    ####################################################################

    def set_execution_time(self, seconds: float) -> None:
        self.execution_time_seconds = seconds

    def set_metric(self, name: str, value: int | float) -> None:
        self.custom_metrics[name] = value

    def get_metric(self, name: str) -> int | float | None:
        return self.custom_metrics.get(name)

    def clear_custom_metrics(self) -> None:
        self.custom_metrics.clear()

    ####################################################################
    # Increment Methods
    ####################################################################

    def add_rows_read(self, value: int) -> None:

        self.rows_read += value

    def add_rows_written(self, value: int) -> None:

        self.rows_written += value

    def add_rows_inserted(self, value: int) -> None:

        self.rows_inserted += value

    def add_rows_updated(self, value: int) -> None:

        self.rows_updated += value

    def add_rows_deleted(self, value: int) -> None:

        self.rows_deleted += value

    def add_rows_rejected(self, value: int) -> None:

        self.rows_rejected += value

    def add_files_processed(self, value: int = 1) -> None:

        self.files_processed += value

    def add_files_skipped(self, value: int = 1) -> None:

        self.files_skipped += value

    def add_duplicates(self, value: int) -> None:

        self.duplicates += value

    def add_invalid_records(self, value: int) -> None:

        self.invalid_records += value

    def add_partitions_processed(self, value: int = 1) -> None:

        self.partitions_processed += value

    ####################################################################
    # Export
    ####################################################################

    def as_dict(self) -> dict[str, Any]:

        metrics = {
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "rows_inserted": self.rows_inserted,
            "rows_updated": self.rows_updated,
            "rows_deleted": self.rows_deleted,
            "rows_rejected": self.rows_rejected,
            "files_processed": self.files_processed,
            "files_skipped": self.files_skipped,
            "duplicates": self.duplicates,
            "invalid_records": self.invalid_records,
            "partitions_processed": self.partitions_processed,
            "execution_time_seconds": self.execution_time_seconds,
        }

        metrics.update(self.custom_metrics)

        return metrics

    ####################################################################
    # Summary
    ####################################################################

    def summary(self) -> str:
        return (
            f"Rows Read={self.rows_read}, "
            f"Rows Written={self.rows_written}, "
            f"Files Processed={self.files_processed}, "
            f"Execution Time={self.execution_time_seconds:.2f}s"
        )

    def __repr__(self) -> str:
        return f"PipelineMetrics({self.summary()})"
