"""Streaming idempotency helpers."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def new_event_ids(incoming: DataFrame, existing_ids: DataFrame) -> DataFrame:
    """Return only events not already represented by the target's event IDs."""
    return incoming.join(existing_ids.select(F.col("event_id")), "event_id", "left_anti")
