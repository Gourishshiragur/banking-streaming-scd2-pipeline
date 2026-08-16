"""Streaming adapter for the enterprise audit framework."""

from pyspark.sql import SparkSession

from pipeline.config import StreamingConfig
from framework.audit import AuditFramework


def record_batch_audit(
    spark: SparkSession,
    config: StreamingConfig,
    batch_id: int,
    input_rows: int,
    impacted_accounts: int,
    history_rows: int,
    current_rows: int,
    status: str = "SUCCESS",
):
    """Record one Streaming micro-batch using the enterprise audit framework."""

    audit = AuditFramework(
        spark=spark,
        pipeline_name="banking_scd2_streaming",
        pipeline_type="streaming",
        execution_mode="micro_batch",
        trigger_type="EVENT",
    )

    audit.start_run()

    metadata = {
        "spark_batch_id": int(batch_id),
        "impacted_accounts": int(impacted_accounts),
        "history_rows": int(history_rows),
        "current_rows": int(current_rows),
    }

    if status == "SUCCESS":
        record = audit.finish_run(
            stage="scd2_merge",
            rows_read=int(input_rows),
            rows_written=int(history_rows),
            metadata=metadata,
            target_path=str(config.target_table_path),
        )
    else:
        record = audit.log_stage(
            stage="scd2_merge",
            status=status,
            rows_read=int(input_rows),
            rows_written=int(history_rows),
            metadata=metadata,
            target_path=str(config.target_table_path),
        )

    audit.write_record(
        audit_path=str(config.audit_path),
        record=record,
        is_databricks=False,
    )
