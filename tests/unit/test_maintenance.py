from datetime import UTC, datetime

import pytest

from orchestration.maintenance import (
    POLICIES,
    compaction_sql,
    delete_cleanup_sql,
    expire_sql,
    missing_columns,
    orphan_sql,
    policy_for,
    properties_sql,
)

NOW = datetime(2026, 11, 18, 3, 0, tzinfo=UTC)


def test_bronze_compacts_only_days_before_today():
    sql = compaction_sql(policy_for("bronze.trust_movement"), NOW)
    assert "table => 'bronze.trust_movement'" in sql
    assert "where => \"kafka_ts < '2026-11-18 00:00:00'\"" in sql  # today is still being written
    assert "'min-input-files', '2'" in sql and "'partial-progress.enabled', 'true'" in sql


def test_current_calls_compact_everything_before_today():
    # Three uncompacted days cost every MERGE ~2,900 delete files (findings F35).
    sql = compaction_sql(policy_for("silver.trust_movement"), NOW)
    assert "actual_ts < '2026-11-18 00:00:00'" in sql


def test_dangling_delete_files_are_rewritten_for_the_compacted_days_only():
    sql = delete_cleanup_sql(policy_for("silver.trust_movement"), NOW)
    assert "rewrite_position_delete_files(table => 'silver.trust_movement'" in sql
    assert "actual_ts < '2026-11-18 00:00:00'" in sql  # today's live deletes untouched
    assert "'rewrite-all', 'true'" in sql


@pytest.mark.parametrize(
    "table", ["bronze.trust_movement", "silver.movement_revisions", "gold.delay_hotspots"]
)
def test_only_merge_on_read_silver_rewrites_delete_files(table):
    assert delete_cleanup_sql(policy_for(table), NOW) is None


def test_new_measurements_become_typed_columns():
    record = {"table_name": "x", "delete_files_before": 3, "duration_s": 1.5}
    assert missing_columns(record, {"table_name", "duration_s"}) == [
        ("delete_files_before", "BIGINT")
    ]


@pytest.mark.parametrize("table", ["silver.rejected_messages", "ref.locations"])
def test_tiny_tables_are_not_compacted(table):
    assert compaction_sql(policy_for(table), NOW) is None


@pytest.mark.parametrize(
    "table", ["gold.punctuality_hourly", "gold.delay_hotspots", "gold.reactionary_delay"]
)
def test_gold_is_compacted_whole_with_its_delete_files(table):
    # Each build replaces recent rows (ADR-018); no partitions, so no where clause.
    sql = compaction_sql(policy_for(table), NOW)
    assert f"table => '{table}'" in sql and "where =>" not in sql
    assert "'delete-file-threshold', '1'" in sql


def test_bronze_snapshots_kept_three_days_and_at_least_ten():
    sql = expire_sql(policy_for("bronze.trust_movement"), NOW)
    assert "older_than => TIMESTAMP '2026-11-15 03:00:00'" in sql  # silver's position survives
    assert "retain_last => 10" in sql


@pytest.mark.parametrize("table", ["silver.trust_movement", "silver.movement_revisions"])
def test_silver_keeps_only_twelve_hours_of_old_versions(table):
    # Nothing streams from silver; three days of its versions filled the disk (F28).
    assert "older_than => TIMESTAMP '2026-11-17 15:00:00'" in expire_sql(policy_for(table), NOW)


def test_compaction_folds_in_merge_on_read_delete_files():
    assert "'delete-file-threshold', '1'" in compaction_sql(
        policy_for("silver.trust_movement"), NOW
    )


def test_orphans_use_prefix_listing_and_a_three_day_window():
    sql = orphan_sql(policy_for("silver.movement_revisions"), NOW)
    assert "older_than => TIMESTAMP '2026-11-15 03:00:00'" in sql  # Iceberg refuses under 24 h
    assert "prefix_listing => true" in sql  # the default Hadoop listing has no s3 FileSystem


def test_metadata_cleanup_is_switched_on():
    sql = properties_sql("silver.trust_movement")
    assert "'write.metadata.delete-after-commit.enabled' = 'true'" in sql
    assert "'write.metadata.previous-versions-max' = '100'" in sql


def test_every_written_table_has_a_policy():
    assert {p.table for p in POLICIES} == {
        "bronze.trust_movement",
        "silver.trust_movement",
        "silver.movement_revisions",
        "silver.rejected_messages",
        "ref.locations",
        "gold.punctuality_hourly",
        "gold.delay_hotspots",
        "gold.reactionary_delay",
    }


def test_procedure_arguments_are_literals_not_expressions():
    # Spark could not bind `current_timestamp() - INTERVAL ...` inside CALL arguments.
    for policy in POLICIES:
        for sql in (expire_sql(policy, NOW), orphan_sql(policy, NOW)):
            assert "current_timestamp" not in sql and "INTERVAL" not in sql
