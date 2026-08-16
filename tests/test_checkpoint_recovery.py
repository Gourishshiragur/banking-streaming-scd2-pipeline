"""Defense-in-depth test for a micro-batch replay after checkpoint recovery."""
from datetime import datetime

import pytest

from pipeline.config import StreamingConfig
from pipeline.scd2_merge import apply_scd2_batch
from pipeline.spark_session import get_spark


@pytest.fixture(scope="module")
def spark():
    s = get_spark("test-checkpoint-recovery")
    yield s
    s.stop()


def test_replayed_batch_produces_no_duplicate_scd2_versions(tmp_path, spark):
    config = StreamingConfig(base_path=tmp_path)
    t0 = datetime(2026, 1, 1, 12, 0, 0)
    batch = spark.createDataFrame([
        {"event_id": "evt-restart-1", "account_id": "ACC-00010", "event_time": t0,
         "account_status": "ACTIVE", "account_tier": "PREMIUM", "branch_region": "WEST"},
        {"event_id": "evt-restart-2", "account_id": "ACC-00010", "event_time": t0,
         "account_status": "ACTIVE", "account_tier": "PREMIUM", "branch_region": "WEST"},
    ])

    apply_scd2_batch(batch, 7, spark, config)
    apply_scd2_batch(batch, 7, spark, config)

    rows = spark.read.format("delta").load(str(config.target_table_path)).collect()
    assert len(rows) == 1
    assert rows[0]["source_event_id"] == "evt-restart-1"
    assert rows[0]["is_current"] is True
