"""Structured Streaming entrypoint using the streaming-native framework."""
import os
from pyspark.sql import SparkSession

from pipeline.config import StreamingConfig
from pipeline.logger import get_logger
from pipeline.schema_evolution import split_valid_and_quarantined
from cdc.event_contract import CDC_EVENT_SCHEMA, validate_cdc_events, quarantine_cdc_events
from pipeline.scd2_merge import apply_scd2_batch
from streaming_framework.checkpoint import validate_checkpoint_path

logger = get_logger(__name__)

EVENT_SCHEMA = CDC_EVENT_SCHEMA

def _write_quarantine(df, config: StreamingConfig):
    if not df.isEmpty():
        df.write.format("delta").mode("append").option("mergeSchema", "true").save(str(config.dead_letter_path))

def _process_micro_batch(batch_df, batch_id, spark, config):
    # First enforce the canonical CDC event contract.
    cdc_valid = validate_cdc_events(batch_df)
    cdc_quarantined = quarantine_cdc_events(batch_df)

    _write_quarantine(cdc_quarantined, config)

    # Apply existing schema-evolution validation to structurally valid CDC.
    valid, quarantined = split_valid_and_quarantined(cdc_valid, config)
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
    writer = (
        stream.writeStream
        .foreachBatch(lambda df, bid: _process_micro_batch(df, bid, spark, config))
        .option("checkpointLocation", checkpoint)
    )

    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        query = writer.trigger(availableNow=True).start()
    else:
        query = writer.trigger(processingTime=config.trigger_interval).start()
    logger.info("streaming query started", extra={"fields": {"checkpoint": checkpoint, "watermark": config.watermark_duration}})
    return query
