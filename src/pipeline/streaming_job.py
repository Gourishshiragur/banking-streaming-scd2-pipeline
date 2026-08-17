"""Structured Streaming entrypoint using the streaming-native framework."""
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from pipeline.config import StreamingConfig
from pipeline.logger import get_logger
from pipeline.schema_evolution import split_valid_and_quarantined
from pipeline.scd2_merge import apply_scd2_batch
from streaming_framework.checkpoint import validate_checkpoint_path

logger = get_logger(__name__)

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("account_id", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("account_status", StringType(), True),
    StructField("account_tier", StringType(), True),
    StructField("branch_region", StringType(), True),
    StructField("customer_segment", StringType(), True),
])

def _write_quarantine(df, config: StreamingConfig):
    if not df.isEmpty():
        df.write.format("delta").mode("append").option("mergeSchema", "true").save(str(config.dead_letter_path))

def _process_micro_batch(batch_df, batch_id, spark, config):
    valid, quarantined = split_valid_and_quarantined(batch_df, config)
    _write_quarantine(quarantined, config)
    apply_scd2_batch(valid, batch_id, spark, config)

def build_and_start_stream(spark: SparkSession, config: StreamingConfig):
    checkpoint = validate_checkpoint_path(config.checkpoint_path)
    raw = (
        spark.readStream.schema(EVENT_SCHEMA)
        .option("maxFilesPerTrigger", config.max_files_per_trigger)
        .json(str(config.stream_source_path))
    )
    stream = raw.withWatermark(config.event_time_col, config.watermark_duration).dropDuplicates(["event_id"])
    query = (
        stream.writeStream.foreachBatch(lambda df, bid: _process_micro_batch(df, bid, spark, config))
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=config.trigger_interval)
        .start()
    )
    logger.info("streaming query started", extra={"fields": {"checkpoint": checkpoint, "watermark": config.watermark_duration}})
    return query
