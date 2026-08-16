from datetime import datetime

from pipeline.config import StreamingConfig
from pipeline.schema_evolution import split_valid_and_quarantined
from pipeline.spark_session import get_spark


def test_additive_customer_segment_column_is_allowed(tmp_path):
    spark = get_spark("test-schema-evolution")
    try:
        cfg = StreamingConfig(base_path=tmp_path)
        df = spark.createDataFrame([{
            "event_id": "e1", "account_id": "ACC-1", "event_time": datetime(2026, 1, 1),
            "account_status": "ACTIVE", "account_tier": "STANDARD", "branch_region": "SOUTH",
            "customer_segment": "RETAIL",
        }])
        valid, bad = split_valid_and_quarantined(df, cfg)
        assert valid.count() == 1
        assert bad.count() == 0
    finally:
        spark.stop()


def test_incompatible_critical_type_is_quarantined(tmp_path):
    spark = get_spark("test-schema-drift")
    try:
        cfg = StreamingConfig(base_path=tmp_path)
        df = spark.createDataFrame([{
            "event_id": "e2", "account_id": "ACC-2", "event_time": datetime(2026, 1, 1),
            "account_status": 123, "account_tier": "STANDARD", "branch_region": "SOUTH",
        }])
        valid, bad = split_valid_and_quarantined(df, cfg)
        assert valid.count() == 0
        assert bad.count() == 1
        assert "quarantine_reason" in bad.columns
    finally:
        spark.stop()
