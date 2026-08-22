from datetime import datetime

import pytest

from cdc.event_contract import (
    CDC_EVENT_SCHEMA,
    quarantine_cdc_events,
    validate_cdc_events,
)
from pipeline.spark_session import get_spark


@pytest.fixture(scope="module")
def spark():
    s = get_spark("test-cdc-event-contract")
    yield s
    s.stop()


def _events(spark, rows):
    return spark.createDataFrame(rows, schema=CDC_EVENT_SCHEMA)


def test_valid_cdc_events_are_accepted(spark):
    df = _events(
        spark,
        [
            (
                "e1", "a1", "INSERT",
                datetime(2026, 8, 22, 12, 0, 0),
                "ACTIVE", "GOLD", "BLR", "debezium", "1",
            ),
            (
                "e2", "a1", "UPDATE",
                datetime(2026, 8, 22, 12, 1, 0),
                "SUSPENDED", "GOLD", "BLR", "debezium", "1",
            ),
        ],
    )

    result = validate_cdc_events(df)

    assert result.count() == 2


def test_invalid_operation_is_quarantined(spark):
    df = _events(
        spark,
        [
            (
                "e1", "a1", "UPSERT",
                datetime(2026, 8, 22, 12, 0, 0),
                "ACTIVE", "GOLD", "BLR", "debezium", "1",
            ),
        ],
    )

    valid = validate_cdc_events(df)
    quarantine = quarantine_cdc_events(df)

    assert valid.count() == 0
    assert quarantine.count() == 1


def test_missing_required_value_is_quarantined(spark):
    df = _events(
        spark,
        [
            (
                "e1", None, "UPDATE",
                datetime(2026, 8, 22, 12, 0, 0),
                "ACTIVE", "GOLD", "BLR", "debezium", "1",
            ),
        ],
    )

    valid = validate_cdc_events(df)
    quarantine = quarantine_cdc_events(df)

    assert valid.count() == 0
    assert quarantine.count() == 1
