"""
Enterprise Audit Framework

Reusable across:
- Incremental Batch
- Micro Batch
- Streaming

Supports:
- Local Spark
- Databricks
"""

from __future__ import annotations
from datetime import datetime
from pyspark.sql import Row


from typing import Any, Optional
import json
from .audit_schema import AUDIT_SCHEMA
from .constants import (
    DEFAULT_FILES_PROCESSED,
    DEFAULT_RETRY_COUNT,
    DEFAULT_DURATION,
    STATUS_SUCCESS,
    STATUS_FAILED,
    COL_CREATED_AT,
    COL_BATCH_ID,
)
from .utils import (
    cluster_name,
    detect_environment,
    elapsed_seconds,
    execution_engine,
    framework_version,
    generate_batch_id,
    generate_execution_id,
    generate_run_id,
    hostname,
    job_name,
    user_name,
    utc_now,
    workspace_name,
    workspace_user,
)

from pyspark.sql import SparkSession


class AuditFramework:
    """
    Enterprise reusable audit framework.

    One object represents one pipeline execution.
    """

    def __init__(
        self,
        spark: SparkSession,
        pipeline_name: str,
        pipeline_type: str,
        execution_mode: Optional[str] = None,
        job_name_value: Optional[str] = None,
        trigger_type: Optional[str] = None,
        run_id: Optional[str] = None,
        execution_id: Optional[str] = None,
    ):

        self.pipeline_name = pipeline_name

        self.spark = spark

        self.pipeline_type = pipeline_type

        self.job_name = job_name_value or job_name(self.spark)

        self.trigger_type = trigger_type

        self.execution_mode = execution_mode

        self.batch_id = generate_batch_id()

        self.run_id = run_id or generate_run_id()

        self.execution_id = execution_id or generate_execution_id()

        self.environment = detect_environment(self.spark)

        self.execution_engine = execution_engine()

        self.workspace_name = workspace_name(self.spark)

        self.cluster_name = cluster_name(self.spark)

        self.hostname = hostname()

        self.user_name = workspace_user(self.spark) or user_name()

        self.started_at: Optional[datetime] = None

        self.completed_at: Optional[datetime] = None

        self.retry_count = DEFAULT_RETRY_COUNT

    ####################################################################
    # Public API
    ####################################################################

    def start_run(self) -> str:
        """
        Start pipeline execution.
        """

        self.started_at = utc_now()

        return self.run_id

    def get_run_id(self) -> str:

        return self.run_id

    def get_execution_id(self) -> str:
        return self.execution_id

    ####################################################################
    # Internal Helpers
    ####################################################################

    def _build_record(
        self,
        stage: str,
        status: str,
        rows_read: int = 0,
        rows_written: int = 0,
        rows_rejected: int = 0,
        rows_skipped: int = 0,
        snapshot_day: int | None = None,
        files_processed: int = DEFAULT_FILES_PROCESSED,
        source_name: str | None = None,
        source_path: str | None = None,
        source_file: str | None = None,
        target_name: str | None = None,
        target_path: str | None = None,
        metadata: dict[str, Any] | None = None,
        schema_version: str = "1",
        duration_seconds: float = DEFAULT_DURATION,
        error_type: str | None = None,
        error_message: str | None = None,
    ) -> dict[str, Any]:
        """
        Build a single audit record.
        """
        return {
            # Run Information
            "run_id": self.run_id,
            "execution_id": self.execution_id,
            COL_BATCH_ID: self.batch_id,
            "pipeline_name": self.pipeline_name,
            "pipeline_type": self.pipeline_type,
            "stage": stage,
            "status": status,
            # Pipeline Information
            "job_name": self.job_name or "",
            "trigger_type": self.trigger_type or "",
            "execution_mode": self.execution_mode or "",
            "retry_count": self.retry_count,
            # Data Statistics
            "rows_read": int(rows_read),
            "rows_written": int(rows_written),
            "rows_rejected": int(rows_rejected),
            "rows_skipped": int(rows_skipped),
            "files_processed": int(files_processed),
            "snapshot_day": snapshot_day,
            # Source / Target
            "source_name": source_name or "",
            "source_path": source_path or "",
            "source_file": source_file or "",
            "target_name": target_name or "",
            "target_path": target_path or "",
            # Execution Context
            "metadata": json.dumps(metadata or {}),
            # Environment
            "environment": self.environment or "",
            "execution_engine": self.execution_engine or "",
            "workspace_name": self.workspace_name or "",
            "cluster_name": self.cluster_name or "",
            "hostname": self.hostname or "",
            "user_name": self.user_name or "",
            # Performance
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "duration_seconds": float(duration_seconds),
            # Errors
            "error_type": error_type or "",
            "error_message": error_message or "",
            # Metadata
            COL_CREATED_AT: utc_now(),
            "framework_version": framework_version(),
            "schema_version": schema_version,
        }

    ####################################################################
    # Audit API
    ####################################################################

    def log_stage(
        self,
        stage: str,
        status: str,
        rows_read: int = 0,
        rows_written: int = 0,
        rows_rejected: int = 0,
        rows_skipped: int = 0,
        snapshot_day: int | None = None,
        files_processed: int = DEFAULT_FILES_PROCESSED,
        source_name: Optional[str] = None,
        source_path: Optional[str] = None,
        source_file: str | None = None,
        target_name: Optional[str] = None,
        target_path: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
        schema_version: str = "1",
        duration_seconds: float = 0.0,
        error_type: Optional[str] = None,
        error_message: Optional[str] = None,
    ) -> dict[str, Any]:
        """
        Write one audit record.
        """
        record = self._build_record(
            stage=stage,
            status=status,
            rows_read=rows_read,
            rows_written=rows_written,
            rows_rejected=rows_rejected,
            rows_skipped=rows_skipped,
            snapshot_day=snapshot_day,
            source_file=source_file,
            schema_version=schema_version,
            files_processed=files_processed,
            source_name=source_name,
            source_path=source_path,
            target_name=target_name,
            target_path=target_path,
            metadata=metadata,
            duration_seconds=duration_seconds,
            error_type=error_type,
            error_message=error_message,
        )

        return record

    def finish_run(
        self,
        stage: str,
        rows_read: int = 0,
        rows_written: int = 0,
        rows_rejected: int = 0,
        rows_skipped: int = 0,
        snapshot_day: int | None = None,
        files_processed: int = DEFAULT_FILES_PROCESSED,
        source_name: Optional[str] = None,
        source_path: Optional[str] = None,
        source_file: str | None = None,
        target_name: Optional[str] = None,
        target_path: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
        schema_version: str = "1",
        timer_start: Optional[float] = None,
    ) -> dict[str, Any]:
        """
        Write SUCCESS audit record.
        """
        duration = elapsed_seconds(timer_start) if timer_start is not None else 0.0

        self.completed_at = utc_now()
        return self.log_stage(
            stage=stage,
            status=STATUS_SUCCESS,
            rows_read=rows_read,
            rows_written=rows_written,
            rows_rejected=rows_rejected,
            rows_skipped=rows_skipped,
            snapshot_day=snapshot_day,
            source_file=source_file,
            schema_version=schema_version,
            files_processed=files_processed,
            source_name=source_name,
            source_path=source_path,
            target_name=target_name,
            target_path=target_path,
            metadata=metadata,
            duration_seconds=duration,
        )

    def fail_run(
        self,
        stage: str,
        exception: Exception,
        timer_start: Optional[float] = None,
    ) -> dict[str, Any]:
        """
        Write FAILED audit record.
        """

        duration = elapsed_seconds(timer_start) if timer_start is not None else 0.0

        self.completed_at = utc_now()

        return self.log_stage(
            stage=stage,
            status=STATUS_FAILED,
            duration_seconds=duration,
            error_type=type(exception).__name__,
            error_message=str(exception),
        )

    def write_record(
        self,
        audit_path: str,
        record: dict[str, Any],
        is_databricks: bool = False,
    ) -> None:
        """
        Persist one audit record.
        """

        audit_df = self.spark.createDataFrame(
            [Row(**record)],
            schema=AUDIT_SCHEMA,
        )

        if is_databricks:
            (
                audit_df.write.mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(audit_path)
            )
        else:
            (
                audit_df.write.format("delta")
                .option("mergeSchema", "true")
                .mode("append")
                .save(audit_path)
            )
