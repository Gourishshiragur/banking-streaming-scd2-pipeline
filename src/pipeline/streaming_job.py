"""Structured Streaming entrypoint for banking account-state events."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from pipeline.config import StreamingConfig
from pipeline.logger import get_logger
from pipeline.scd2_merge import apply_scd2_batch
from pipeline.schema_evolution import split_valid_and_quarantined

logger = get_logger(__name__)

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("account_id", StringType(), nullable=False),
        StructField("event_time", TimestampType(), nullable=False),
        StructField("account_status", StringType(), nullable=True),
        StructField("account_tier", StringType(), nullable=True),
        StructField("branch_region", StringType(), nullable=True),
        StructField("customer_segment", StringType(), nullable=True),
    ]
)


def _write_quarantine(df, config: StreamingConfig):
    if df.limit(1).count() > 0:
        df.write.format("delta").mode("append").option("mergeSchema", "true").save(
            str(config.dead_letter_path)
        )


def _process_micro_batch(batch_df, batch_id, spark, config):
    if batch_df.limit(1).count() == 0:
        return
    valid, quarantined = split_valid_and_quarantined(batch_df, config)
    _write_quarantine(quarantined, config)
    if valid.limit(1).count() == 0:
        logger.warning("micro-batch quarantined; no valid rows", extra={"fields": {"batch_id": batch_id}})
        return
    apply_scd2_batch(valid, batch_id, spark, config)


def build_and_start_stream(spark: SparkSession, config: StreamingConfig):
    raw_stream = (
        spark.readStream.schema(EVENT_SCHEMA)
        .option("maxFilesPerTrigger", config.max_files_per_trigger)
        .json(str(config.stream_source_path))
    )

    watermarked = (
        raw_stream
        .withWatermark(config.event_time_col, config.watermark_duration)
        .dropDuplicates(["event_id"])
    )

    query = (
        watermarked.writeStream
        .foreachBatch(lambda batch_df, batch_id: _process_micro_batch(batch_df, batch_id, spark, config))
        .option("checkpointLocation", str(config.checkpoint_path))
        .trigger(processingTime=config.trigger_interval)
        .start()
    )
    logger.info(
        "streaming query started",
        extra={"fields": {"checkpoint": str(config.checkpoint_path), "watermark": config.watermark_duration}},
    )
    return query
