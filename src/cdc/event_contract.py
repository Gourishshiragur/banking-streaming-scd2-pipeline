"""Canonical CDC event contract for the banking streaming pipeline."""

from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    TimestampType,
)


CDC_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("operation", StringType(), True),
    StructField("event_time", TimestampType(), True),
    StructField("account_status", StringType(), True),
    StructField("account_tier", StringType(), True),
    StructField("branch_region", StringType(), True),
    StructField("source", StringType(), True),
    StructField("schema_version", StringType(), True),
])


VALID_OPERATIONS = ("INSERT", "UPDATE", "DELETE")


def validate_cdc_events(df):
    """Return CDC events that satisfy the canonical event contract."""

    required = {
        "event_id",
        "account_id",
        "operation",
        "event_time",
    }

    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"CDC event is missing required columns: {sorted(missing)}"
        )

    return df.filter(
        df.event_id.isNotNull()
        & df.account_id.isNotNull()
        & df.operation.isNotNull()
        & df.event_time.isNotNull()
        & df.operation.isin(*VALID_OPERATIONS)
    )


def quarantine_cdc_events(df):
    """Return CDC events that fail the canonical validation rules."""

    required_invalid = (
        df.event_id.isNull()
        | df.account_id.isNull()
        | df.operation.isNull()
        | df.event_time.isNull()
        | ~df.operation.isin(*VALID_OPERATIONS)
    )

    return df.filter(required_invalid)
