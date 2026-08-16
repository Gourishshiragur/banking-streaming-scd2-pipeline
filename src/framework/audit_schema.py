"""
Enterprise Audit Schema

Reusable across:
- Incremental Batch
- Micro Batch
- Streaming

Author: Gourish
"""

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)

AUDIT_SCHEMA = StructType(
    [
        ####################################################################
        # Run Information
        ####################################################################
        StructField("run_id", StringType(), False),
        StructField("execution_id", StringType(), False),
        StructField("batch_id", StringType(), True),
        StructField("pipeline_name", StringType(), False),
        StructField("pipeline_type", StringType(), False),
        StructField("stage", StringType(), False),
        StructField("status", StringType(), False),
        ####################################################################
        # Pipeline Information
        ####################################################################
        StructField("job_name", StringType(), True),
        StructField("trigger_type", StringType(), True),
        StructField("execution_mode", StringType(), True),
        StructField("retry_count", IntegerType(), False),
        ####################################################################
        # Data Statistics
        ####################################################################
        StructField("rows_read", IntegerType(), False),
        StructField("rows_written", IntegerType(), False),
        StructField("rows_rejected", IntegerType(), False),
        StructField("rows_skipped", IntegerType(), False),
        StructField("files_processed", IntegerType(), False),
        StructField("snapshot_day", IntegerType(), True),
        ####################################################################
        # Source / Target
        ####################################################################
        StructField("source_name", StringType(), True),
        StructField("source_path", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("target_name", StringType(), True),
        StructField("target_path", StringType(), True),
        ####################################################################
        # Execution Context
        ####################################################################
        StructField(
            "metadata",
            StringType(),
            True,
        ),
        ####################################################################
        # Environment
        ####################################################################
        StructField("environment", StringType(), False),
        StructField("execution_engine", StringType(), False),
        StructField("workspace_name", StringType(), True),
        StructField("cluster_name", StringType(), True),
        StructField("hostname", StringType(), True),
        StructField("user_name", StringType(), True),
        ####################################################################
        # Performance
        ####################################################################
        StructField("started_at", TimestampType(), False),
        StructField("completed_at", TimestampType(), True),
        StructField("duration_seconds", DoubleType(), False),
        ####################################################################
        # Errors
        ####################################################################
        StructField("error_type", StringType(), True),
        StructField("error_message", StringType(), True),
        ####################################################################
        # Metadata
        ####################################################################
        StructField("created_at", TimestampType(), False),
        StructField("framework_version", StringType(), False),
        StructField("schema_version", StringType(), False),
    ]
)
