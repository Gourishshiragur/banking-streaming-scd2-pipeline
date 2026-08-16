"""
Enterprise Control Table Schema

Reusable across:
- Incremental Batch
- Micro Batch
- Streaming
"""

from pyspark.sql.types import TimestampType
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
)

CONTROL_SCHEMA = StructType(
    [
        StructField("pipeline_name", StringType(), False),
        StructField("pipeline_type", StringType(), False),
        StructField("stage", StringType(), False),
        StructField("run_id", StringType(), False),
        StructField("execution_id", StringType(), False),
        StructField("execution_mode", StringType(), False),
        StructField("trigger_type", StringType(), False),
        StructField("batch_id", StringType(), True),
        StructField("snapshot_date", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("watermark", StringType(), True),
        StructField("checkpoint", StringType(), True),
        StructField("status", StringType(), False),
        StructField("rows_read", LongType(), True),
        StructField("rows_written", LongType(), True),
        StructField("rows_skipped", LongType(), True),
        StructField("duration_seconds", LongType(), True),
        StructField("error_message", StringType(), True),
        StructField("start_time", TimestampType(), True),
        StructField("end_time", TimestampType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)
