"""SparkSession factory for local PySpark and Databricks."""
import os
from pyspark.sql import SparkSession

def get_spark(app_name: str = "streaming-scd2-pipeline") -> SparkSession:
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    from delta import configure_spark_with_delta_pip
    builder = (
        SparkSession.builder.appName(app_name).master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
