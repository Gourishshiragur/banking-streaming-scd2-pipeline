"""SCD2 commit-path smoke test. Performance is measured by the separate benchmark."""
from datetime import datetime, timedelta

import pytest

from pipeline.config import StreamingConfig
from pipeline.scd2_merge import apply_scd2_batch
from pipeline.spark_session import get_spark


@pytest.fixture(scope="module")
def spark():
    s = get_spark("test-scd2-commit-path")
    yield s
    s.stop()


def test_scd2_commit_path_completes(spark, tmp_path):
    """Exercise one realistic micro-batch without turning pytest into a benchmark."""
    config = StreamingConfig(base_path=tmp_path)
    t0 = datetime(2026, 1, 1, 9, 0, 0)
    rows = [
        {
            "event_id": f"evt-lat-{i}",
            "account_id": f"ACC-{i % 15:05d}",
            "event_time": t0 + timedelta(seconds=i),
            "account_status": "ACTIVE" if i % 2 == 0 else "SUSPENDED",
            "account_tier": "PREMIUM",
            "branch_region": "SOUTH",
        }
        for i in range(20)
    ]

    batch = spark.createDataFrame(rows)
    apply_scd2_batch(batch, 0, spark, config)

    result = spark.read.format("delta").load(str(config.target_table_path))
    assert result.count() >= 15
    assert result.filter("is_current = true").count() == 15
