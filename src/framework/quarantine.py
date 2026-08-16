"""
Enterprise Quarantine Framework

Reusable across:
- Incremental Batch
- Micro Batch
- Streaming

Stores invalid records for later analysis.
"""

from __future__ import annotations
from delta.tables import DeltaTable

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit


class QuarantineManager:
    """
    Enterprise reusable quarantine manager.
    """

    def __init__(
        self,
        quarantine_path: str,
    ):

        self.quarantine_path = quarantine_path

    ####################################################################
    # Public API
    ####################################################################

    def quarantine(
        self,
        df: DataFrame,
        error_code: str,
        error_message: str,
        stage: str,
        batch_id: str | None = None,
        pipeline_name: str | None = None,
    ) -> int:
        """
        Write invalid records into quarantine storage.

        Returns:
            Number of quarantined records.
        """

        quarantine_df = (
            df.withColumn(
                "error_code",
                lit(error_code),
            )
            .withColumn(
                "error_message",
                lit(error_message),
            )
            .withColumn(
                "pipeline_name",
                lit(pipeline_name),
            )
            .withColumn(
                "batch_id",
                lit(batch_id),
            )
            .withColumn(
                "stage",
                lit(stage),
            )
            .withColumn(
                "quarantined_at",
                current_timestamp(),
            )
        )

        (
            quarantine_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(self.quarantine_path)
        )

        return quarantine_df.count()

    ####################################################################
    # Utilities
    ####################################################################

    def initialize(self, spark: SparkSession) -> None:
        """
        Create an empty quarantine Delta table if it doesn't exist.
        """

        if DeltaTable.isDeltaTable(spark, self.quarantine_path):
            return

        empty_df = spark.createDataFrame(
            [],
            schema="""
            error_code STRING,
            error_message STRING,
            pipeline_name STRING,
            batch_id STRING,
            stage STRING,
            quarantined_at TIMESTAMP
        """,
        )

        (empty_df.write.format("delta").mode("overwrite").save(self.quarantine_path))

    def exists(self, spark: SparkSession) -> bool:
        """
        Return True if the quarantine Delta table exists.
        """

        return DeltaTable.isDeltaTable(spark, self.quarantine_path)
