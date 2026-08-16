"""Schema validation and quarantine for the banking event stream."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from pipeline.config import StreamingConfig
from pipeline.logger import get_logger

logger = get_logger(__name__)

CRITICAL_TYPED_COLUMNS = {
    "event_id": "string",
    "account_id": "string",
    "event_time": "timestamp",
    "account_status": "string",
    "account_tier": "string",
    "branch_region": "string",
}


def split_valid_and_quarantined(df: DataFrame, config: StreamingConfig) -> tuple[DataFrame, DataFrame]:
    """Allow additive columns but quarantine incompatible critical type drift."""
    schema: StructType = df.schema
    incompatible_cols = []

    for col_name, expected_type in CRITICAL_TYPED_COLUMNS.items():
        if col_name not in schema.fieldNames():
            continue
        actual_type = schema[col_name].dataType.simpleString()
        if actual_type != expected_type:
            incompatible_cols.append((col_name, actual_type, expected_type))

    if not incompatible_cols:
        return df, df.limit(0)

    logger.error(
        "schema drift detected, quarantining batch",
        extra={"fields": {"incompatible_columns": str(incompatible_cols)}},
    )
    quarantined = df.withColumn("quarantine_reason", F.lit(str(incompatible_cols)))
    return df.limit(0), quarantined
