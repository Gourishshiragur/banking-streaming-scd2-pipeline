"""Streaming-native SCD Type 2 writer.

The legacy incremental-batch audit framework is deliberately not used here.
The hot path performs:
  1. one target slice read,
  2. one deterministic SCD2 rebuild,
  3. one Delta transaction for impacted accounts,
  4. one non-Spark operational audit append.

The implementation keeps replay idempotency and late-arriving event ordering.
"""
import time

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pipeline.config import StreamingConfig
from streaming_framework.audit import StreamingAudit
from streaming_framework.context import StreamingBatchContext
from streaming_framework.metrics import BatchMetrics
from pipeline.logger import get_logger

logger = get_logger(__name__)


def _ensure_delta(path, df):
    if not DeltaTable.isDeltaTable(df.sparkSession, str(path)):
        df.limit(0).write.format("delta").save(str(path))


def _build_scd2_history(events: DataFrame, config: StreamingConfig) -> DataFrame:
    w = Window.partitionBy(config.entity_key).orderBy(
        F.col(config.event_time_col).asc(),
        F.col("event_id").asc(),
    )

    x = (
        events
        .withColumn("previous_status", F.lag("account_status").over(w))
        .withColumn("previous_tier", F.lag("account_tier").over(w))
        .withColumn("previous_region", F.lag("branch_region").over(w))
    )

    changed = x.filter(
        F.col("previous_status").isNull()
        | ~F.col("account_status").eqNullSafe(F.col("previous_status"))
        | ~F.col("account_tier").eqNullSafe(F.col("previous_tier"))
        | ~F.col("branch_region").eqNullSafe(F.col("previous_region"))
    ).drop("previous_status", "previous_tier", "previous_region")

    vw = Window.partitionBy(config.entity_key).orderBy(
        F.col(config.event_time_col).asc(),
        F.col("event_id").asc(),
    )

    return (
        changed
        .withColumn("effective_from", F.col(config.event_time_col))
        .withColumn("effective_to", F.lead(config.event_time_col).over(vw))
        .withColumn("is_current", F.col("effective_to").isNull())
        .withColumn(
            "version_id",
            F.sha2(
                F.concat_ws("||", F.col(config.entity_key), F.col("event_id")),
                256,
            ),
        )
        .withColumnRenamed("event_id", "source_event_id")
        .select(
            "version_id",
            config.entity_key,
            "source_event_id",
            config.event_time_col,
            "account_status",
            "account_tier",
            "branch_region",
            "effective_from",
            "effective_to",
            "is_current",
        )
    )


def apply_scd2_batch(micro_batch_df, batch_id, spark, config):
    started = time.perf_counter()
    incoming = micro_batch_df.dropDuplicates(["event_id"])
    existing = None
    new_events = None
    rebuilt = None

    try:
        incoming_count = incoming.count()
        if incoming_count == 0:
            return

        impacted = [
            r[config.entity_key]
            for r in incoming.select(config.entity_key).distinct().collect()
        ]
        if not impacted:
            return

        # One target scan. The selected columns are exactly those needed by SCD2.
        if DeltaTable.isDeltaTable(spark, str(config.target_table_path)):
            existing = (
                spark.read.format("delta")
                .load(str(config.target_table_path))
                .where(F.col(config.entity_key).isin(impacted))
                .select(
                    "source_event_id",
                    config.entity_key,
                    config.event_time_col,
                    "account_status",
                    "account_tier",
                    "branch_region",
                )
            )
            existing.count()
            seen = existing.select(
                F.col("source_event_id").alias("event_id")
            )
            prior = existing.select(
                F.col("source_event_id").alias("event_id"),
                config.entity_key,
                config.event_time_col,
                "account_status",
                "account_tier",
                "branch_region",
            )
        else:
            seen = spark.createDataFrame([], "event_id string")
            prior = spark.createDataFrame(
                [],
                "event_id string, account_id string, event_time timestamp, "
                "account_status string, account_tier string, branch_region string",
            )

        new_events = incoming.join(seen, "event_id", "left_anti")
        new_count = new_events.count()

        if new_count == 0:
            logger.info(
                "streaming SCD2 replay skipped",
                extra={"fields": {"spark_batch_id": batch_id, "incoming_rows": incoming_count}},
            )
            return

        all_events = prior.unionByName(
            new_events.select(
                "event_id",
                config.entity_key,
                config.event_time_col,
                "account_status",
                "account_tier",
                "branch_region",
            )
        )

        rebuilt = _build_scd2_history(all_events, config)

        # Fast path: an empty target has no prior versions to replace.
        # Append is materially cheaper than replaceWhere on local Delta.
        target_exists = DeltaTable.isDeltaTable(spark, str(config.target_table_path))
        if not target_exists:
            (
                rebuilt.write.format("delta")
                .mode("append")
                .save(str(config.target_table_path))
            )
        else:
            # Existing/late-arriving accounts require an atomic targeted
            # replacement to preserve effective_from/effective_to semantics.
            predicate = (
                f"{config.entity_key} IN "
                f"({', '.join(repr(str(a)) for a in impacted)})"
            )
            (
                rebuilt.write.format("delta")
                .mode("overwrite")
                .option("replaceWhere", predicate)
                .save(str(config.target_table_path))
            )

        metrics = rebuilt.agg(
            F.count("*").alias("history_rows"),
            F.sum(F.when(F.col("is_current"), 1).otherwise(0)).alias("current_rows"),
        ).first()
        rebuilt_count = int(metrics["history_rows"])
        current_rows = int(metrics["current_rows"] or 0)

        context = StreamingBatchContext(
            pipeline_name="banking_scd2_streaming",
            batch_id=int(batch_id),
            checkpoint_path=config.checkpoint_path,
            target_path=config.target_table_path,
            audit_path=config.audit_path,
        )
        audit_metrics = BatchMetrics(
            input_rows=incoming_count,
            new_rows=new_count,
            impacted_accounts=len(impacted),
            history_rows=rebuilt_count,
            current_rows=current_rows,
        )
        StreamingAudit(context).record(
            audit_metrics,
            duration_seconds=time.perf_counter() - started,
        )

        logger.info(
            "streaming SCD2 batch committed",
            extra={"fields": {
                "spark_batch_id": batch_id,
                "incoming_rows": incoming_count,
                "impacted_accounts": len(impacted),
                "history_rows_rebuilt": rebuilt_count,
            }},
        )
    finally:
        pass
