import shutil
from datetime import datetime, timedelta

import pytest

from pipeline.config import StreamingConfig
from pipeline.scd2_merge import apply_scd2_batch
from pipeline.spark_session import get_spark


@pytest.fixture(scope="module")
def spark():
    s = get_spark("test-banking-scd2")
    yield s
    s.stop()


@pytest.fixture
def config(tmp_path):
    cfg = StreamingConfig(base_path=tmp_path)
    yield cfg
    shutil.rmtree(tmp_path, ignore_errors=True)


def _events(spark, rows):
    return spark.createDataFrame(rows)


def test_first_event_creates_current_account_version(spark, config):
    t0 = datetime(2026, 1, 1, 10, 0, 0)
    batch = _events(spark, [{
        "event_id": "e1", "account_id": "ACC-00001", "event_time": t0,
        "account_status": "ACTIVE", "account_tier": "STANDARD", "branch_region": "SOUTH",
    }])
    apply_scd2_batch(batch, 0, spark, config)

    rows = spark.read.format("delta").load(str(config.target_table_path)).collect()
    assert len(rows) == 1
    assert rows[0]["account_id"] == "ACC-00001"
    assert rows[0]["is_current"] is True
    assert rows[0]["effective_to"] is None


def test_status_change_closes_old_version_and_opens_new(spark, config):
    t0 = datetime(2026, 1, 1, 10, 0, 0)
    t1 = t0 + timedelta(minutes=5)
    apply_scd2_batch(_events(spark, [{
        "event_id": "e1", "account_id": "ACC-00002", "event_time": t0,
        "account_status": "ACTIVE", "account_tier": "STANDARD", "branch_region": "SOUTH",
    }]), 0, spark, config)
    apply_scd2_batch(_events(spark, [{
        "event_id": "e2", "account_id": "ACC-00002", "event_time": t1,
        "account_status": "SUSPENDED", "account_tier": "STANDARD", "branch_region": "SOUTH",
    }]), 1, spark, config)

    rows = spark.read.format("delta").load(str(config.target_table_path)).orderBy("effective_from").collect()
    assert len(rows) == 2
    assert rows[0]["account_status"] == "ACTIVE"
    assert rows[0]["effective_to"] == t1
    assert rows[0]["is_current"] is False
    assert rows[1]["account_status"] == "SUSPENDED"
    assert rows[1]["is_current"] is True


def test_same_account_multiple_changes_in_one_micro_batch(spark, config):
    t0 = datetime(2026, 1, 1, 10, 0, 0)
    rows = [
        {"event_id": "e1", "account_id": "ACC-00003", "event_time": t0,
         "account_status": "ACTIVE", "account_tier": "STANDARD", "branch_region": "SOUTH"},
        {"event_id": "e2", "account_id": "ACC-00003", "event_time": t0 + timedelta(minutes=1),
         "account_status": "SUSPENDED", "account_tier": "STANDARD", "branch_region": "SOUTH"},
        {"event_id": "e3", "account_id": "ACC-00003", "event_time": t0 + timedelta(minutes=2),
         "account_status": "ACTIVE", "account_tier": "PREMIUM", "branch_region": "SOUTH"},
    ]
    apply_scd2_batch(_events(spark, rows), 0, spark, config)

    result = spark.read.format("delta").load(str(config.target_table_path)).orderBy("effective_from").collect()
    assert len(result) == 3
    assert sum(1 for r in result if r["is_current"]) == 1
    assert [r["account_status"] for r in result] == ["ACTIVE", "SUSPENDED", "ACTIVE"]
    assert result[-1]["account_tier"] == "PREMIUM"


def test_late_arriving_event_is_inserted_in_effective_history(spark, config):
    t0 = datetime(2026, 1, 1, 10, 0, 0)
    apply_scd2_batch(_events(spark, [
        {"event_id": "e1", "account_id": "ACC-00004", "event_time": t0,
         "account_status": "ACTIVE", "account_tier": "STANDARD", "branch_region": "WEST"},
        {"event_id": "e3", "account_id": "ACC-00004", "event_time": t0 + timedelta(minutes=10),
         "account_status": "ACTIVE", "account_tier": "PREMIUM", "branch_region": "WEST"},
    ]), 0, spark, config)
    apply_scd2_batch(_events(spark, [{
        "event_id": "e2", "account_id": "ACC-00004", "event_time": t0 + timedelta(minutes=5),
        "account_status": "SUSPENDED", "account_tier": "STANDARD", "branch_region": "WEST",
    }]), 1, spark, config)

    result = spark.read.format("delta").load(str(config.target_table_path)).orderBy("effective_from").collect()
    assert len(result) == 3
    assert [r["account_status"] for r in result] == ["ACTIVE", "SUSPENDED", "ACTIVE"]
    assert result[-1]["is_current"] is True


def test_duplicate_replay_is_idempotent(spark, config):
    t0 = datetime(2026, 1, 1, 12, 0, 0)
    batch = _events(spark, [{
        "event_id": "replay-1", "account_id": "ACC-00005", "event_time": t0,
        "account_status": "ACTIVE", "account_tier": "PRIORITY", "branch_region": "NORTH",
    }, {
        "event_id": "replay-2", "account_id": "ACC-00005", "event_time": t0 + timedelta(minutes=1),
        "account_status": "SUSPENDED", "account_tier": "PRIORITY", "branch_region": "NORTH",
    }])
    apply_scd2_batch(batch, 0, spark, config)
    apply_scd2_batch(batch, 0, spark, config)

    result = spark.read.format("delta").load(str(config.target_table_path)).filter("account_id = 'ACC-00005'").collect()
    assert len(result) == 2
    assert sum(1 for r in result if r["is_current"]) == 1

def test_delete_event_closes_current_scd2_version(spark, config):
    t0 = datetime(2026, 1, 1, 10, 0, 0)
    t1 = t0 + timedelta(minutes=5)

    apply_scd2_batch(_events(spark, [{
        "event_id": "e-delete-1",
        "account_id": "ACC-00006",
        "event_time": t0,
        "operation": "INSERT",
        "account_status": "ACTIVE",
        "account_tier": "STANDARD",
        "branch_region": "SOUTH",
    }]), 0, spark, config)

    apply_scd2_batch(_events(spark, [{
        "event_id": "e-delete-2",
        "account_id": "ACC-00006",
        "event_time": t1,
        "operation": "DELETE",
        "account_status": "ACTIVE",
        "account_tier": "STANDARD",
        "branch_region": "SOUTH",
    }]), 1, spark, config)

    result = (
        spark.read.format("delta")
        .load(str(config.target_table_path))
        .filter("account_id = 'ACC-00006'")
        .orderBy("effective_from")
        .collect()
    )

    assert len(result) == 1
    assert result[0]["is_current"] is False
    assert result[0]["effective_to"] == t1
