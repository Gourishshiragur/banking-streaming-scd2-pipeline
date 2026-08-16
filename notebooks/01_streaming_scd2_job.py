# Databricks notebook source
# MAGIC %md
# MAGIC # Banking Account-State Streaming — SCD Type 2
# MAGIC
# MAGIC Long-running Spark Structured Streaming job. Databricks Jobs owns restart semantics;
# MAGIC the query resumes from its checkpoint. ADF is used only to start/monitor the job.

# COMMAND ----------

from pyspark.sql import SparkSession
from pathlib import Path

spark = SparkSession.getActiveSession()
if spark is None:
    raise RuntimeError("No active Databricks SparkSession found")

dbutils.widgets.text("stream_source_path", "/Volumes/workspace/default/streaming_scd2/landing", "Streaming source volume")
dbutils.widgets.text("target_table_path", "/Volumes/workspace/default/streaming_scd2/account_state_scd2", "SCD2 Delta path")
dbutils.widgets.text("event_ledger_path", "/Volumes/workspace/default/streaming_scd2/account_event_ledger", "Event ledger Delta path")
dbutils.widgets.text("checkpoint_path", "/Volumes/workspace/default/streaming_scd2/checkpoint", "Checkpoint path")
dbutils.widgets.text("dead_letter_path", "/Volumes/workspace/default/streaming_scd2/quarantine", "Quarantine Delta path")
dbutils.widgets.text("watermark_duration", "10 minutes", "Event-time watermark")

a = {k: dbutils.widgets.get(k) for k in [
    "stream_source_path", "target_table_path", "event_ledger_path",
    "checkpoint_path", "dead_letter_path", "watermark_duration"
]}

# Provision the managed Volume directories once. The paths are configurable via widgets.
for p in [a["stream_source_path"], a["target_table_path"], a["event_ledger_path"],
          a["checkpoint_path"], a["dead_letter_path"]]:
    dbutils.fs.mkdirs(p)

# COMMAND ----------

import sys
sys.path.append("../src")

from pipeline.config import StreamingConfig
from pipeline.streaming_job import (
    EVENT_SCHEMA,
    _process_micro_batch,
)

config = StreamingConfig(base_path=Path("/Volumes/workspace/default/streaming_scd2"), create_local_dirs=False)
config.stream_source_path = Path(a["stream_source_path"])
config.target_table_path = Path(a["target_table_path"])
config.event_ledger_path = Path(a["event_ledger_path"])
config.checkpoint_path = Path(a["checkpoint_path"])
config.dead_letter_path = Path(a["dead_letter_path"])
config.watermark_duration = a["watermark_duration"]

# COMMAND ----------

raw_stream = (
    spark.readStream
    .schema(EVENT_SCHEMA)
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
    .foreachBatch(
        lambda batch_df, batch_id:
            _process_micro_batch(batch_df, batch_id, spark, config)
    )
    .option("checkpointLocation", str(config.checkpoint_path))
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

print("Databricks AvailableNow streaming run completed.")