"""Idempotent SCD Type 2 processing for banking account events.

The event ledger is the durable source of truth for valid events.  Each
foreachBatch call merges new event_ids into that ledger, then deterministically
rebuilds the SCD2 history for only the impacted accounts.  This makes replay
safe even when a Spark checkpoint is replayed after a crash.
"""
import time

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pipeline.audit import record_batch_audit
from pipeline.config import StreamingConfig
from pipeline.logger import get_logger

logger = get_logger(__name__)


def _ensure_delta(path, df: DataFrame):
    if not DeltaTable.isDeltaTable(df.sparkSession, str(path)):
        df.limit(0).write.format("delta").save(str(path))


def _merge_event_ledger(events: DataFrame, spark: SparkSession, config: StreamingConfig):
    _ensure_delta(config.event_ledger_path, events)

    existing = (
        spark.read.format("delta")
        .load(str(config.event_ledger_path))
        .select("event_id")
    )

    new_events = events.join(
        existing,
        on="event_id",
        how="left_anti",
    )

    new_events.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).save(str(config.event_ledger_path))


def _build_scd2_history(events: DataFrame, config: StreamingConfig) -> DataFrame:
    order_window = Window.partitionBy(config.entity_key).orderBy(
        F.col(config.event_time_col).asc(), F.col("event_id").asc()
    )

    with_previous = events.withColumn("previous_status", F.lag("account_status").over(order_window)) \
        .withColumn("previous_tier", F.lag("account_tier").over(order_window)) \
        .withColumn("previous_region", F.lag("branch_region").over(order_window))

    is_change = (
        F.col("previous_status").isNull()
        | ~F.col("account_status").eqNullSafe(F.col("previous_status"))
        | ~F.col("account_tier").eqNullSafe(F.col("previous_tier"))
        | ~F.col("branch_region").eqNullSafe(F.col("previous_region"))
    )

    changes = with_previous.filter(is_change).drop(
        "previous_status", "previous_tier", "previous_region"
    )

    version_window = Window.partitionBy(config.entity_key).orderBy(
        F.col(config.event_time_col).asc(), F.col("event_id").asc()
    )
    return (
        changes
        .withColumn("effective_from", F.col(config.event_time_col))
        .withColumn("effective_to", F.lead(config.event_time_col).over(version_window))
        .withColumn("is_current", F.col("effective_to").isNull())
        .withColumn(
            "version_id",
            F.sha2(F.concat_ws("||", F.col(config.entity_key), F.col("event_id")), 256),
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


def apply_scd2_batch(
    micro_batch_df: DataFrame,
    batch_id: int,
    spark: SparkSession,
    config: StreamingConfig,
):
    """Process one Structured Streaming micro-batch idempotently."""
    incoming = micro_batch_df.dropDuplicates(["event_id"]).cache()
    incoming_count = incoming.count()
    if incoming_count == 0:
        incoming.unpersist()
        return

    stage_start = time.perf_counter()
    _merge_event_ledger(incoming, spark, config)
    logger.info(
        "scd2 stage timing",
        extra={
            "fields": {
                "stage": "event_ledger_merge",
                "duration_seconds": round(time.perf_counter() - stage_start, 3),
                "batch_id": batch_id,
            }
        },
    )

    stage_start = time.perf_counter()
    impacted_accounts = [
        r[config.entity_key]
        for r in incoming.select(config.entity_key).distinct().collect()
    ]
    logger.info(
        "scd2 stage timing",
        extra={
            "fields": {
                "stage": "impacted_account_collect",
                "duration_seconds": round(time.perf_counter() - stage_start, 3),
                "batch_id": batch_id,
            }
        },
    )
    if not impacted_accounts:
        return

    stage_start = time.perf_counter()

    ledger_events = (
        spark.read.format("delta").load(str(config.event_ledger_path))
        .filter(F.col(config.entity_key).isin(impacted_accounts))
    )
    rebuilt = _build_scd2_history(ledger_events, config).cache()

    logger.info(
        "scd2 stage timing",
        extra={
            "fields": {
                "stage": "ledger_read_and_scd2_rebuild",
                "duration_seconds": round(time.perf_counter() - stage_start, 3),
                "batch_id": batch_id,
            }
        },
    )

    _ensure_delta(config.target_table_path, rebuilt)
    target = DeltaTable.forPath(spark, str(config.target_table_path))

    # Replace only impacted account histories.  The rebuild is deterministic,
    # so replaying the same micro-batch produces the exact same rows.
    stage_start = time.perf_counter()
    target.delete(F.col(config.entity_key).isin(impacted_accounts))
    logger.info(
        "scd2 stage timing",
        extra={
            "fields": {
                "stage": "target_delete",
                "duration_seconds": round(time.perf_counter() - stage_start, 3),
                "batch_id": batch_id,
            }
        },
    )
    stage_start = time.perf_counter()
    rebuilt.write.format("delta").mode("append").save(
        str(config.target_table_path)
    )
    logger.info(
        "scd2 stage timing",
        extra={
            "fields": {
                "stage": "target_append",
                "duration_seconds": round(time.perf_counter() - stage_start, 3),
                "batch_id": batch_id,
            }
        },
    )

    stage_start = time.perf_counter()

    metrics = rebuilt.agg(
        F.count("*").alias("history_rows"),
        F.sum(F.when(F.col("is_current"), 1).otherwise(0)).alias("current_rows"),
    ).first()

    history_rows = metrics["history_rows"]
    current_rows = metrics["current_rows"] or 0
    impacted_count = len(impacted_accounts)

    record_batch_audit(
        spark,
        config,
        batch_id,
        incoming_count,
        impacted_count,
        history_rows,
        current_rows,
    )

    logger.info(
        "scd2 stage timing",
        extra={
            "fields": {
                "stage": "metrics_and_audit",
                "duration_seconds": round(time.perf_counter() - stage_start, 3),
                "batch_id": batch_id,
            }
        },
    )

    logger.info(
        "streaming SCD2 batch committed",
        extra={
            "fields": {
                "spark_batch_id": batch_id,
                "incoming_rows": incoming_count,
                "impacted_accounts": impacted_count,
                "history_rows_rebuilt": history_rows,
            }
        },
    )

    rebuilt.unpersist()
    incoming.unpersist()
