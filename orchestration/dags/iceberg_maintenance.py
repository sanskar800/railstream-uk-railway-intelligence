"""Nightly Iceberg maintenance: compact, expire snapshots, remove orphans, record before/after.

One task per table, in sequence (the streaming jobs keep committing meanwhile, so tables are done
one at a time). Spark work runs on the Spark Connect server (ADR-015); rules in
orchestration/maintenance.py. Results accumulate in ops.maintenance_runs.
"""

from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, task

TABLES = [
    "bronze.trust_movement",
    "silver.movement_revisions",
    "silver.trust_movement",
    "silver.rejected_messages",
    "ref.locations",
]


@dag(
    dag_id="iceberg_maintenance",
    schedule="0 3 * * *",  # 03:00 UTC: quiet hours, before the Mon/Thu 04:00 CORPUS refresh
    start_date=datetime(2026, 11, 17, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    dagrun_timeout=timedelta(hours=1),
    tags=["railstream", "maintenance"],
)
def iceberg_maintenance():
    previous = None
    for table in TABLES:

        @task(
            task_id=f"maintain_{table.replace('.', '_')}", execution_timeout=timedelta(minutes=20)
        )
        def maintain(table_name: str = table):
            from orchestration.maintenance import connect, maintain_table, policy_for, record_run

            spark = connect()
            record = maintain_table(spark, policy_for(table_name))
            record_run(spark, record)
            print(
                f"MAINTENANCE {table_name}: files {record['data_files_before']} -> "
                f"{record['data_files_after']}, snapshots {record['snapshots_before']} -> "
                f"{record['snapshots_after']}, {record['duration_s']}s"
            )
            return {k: v for k, v in record.items() if k != "started_at"}

        current = maintain()
        if previous is not None:
            previous >> current
        previous = current


iceberg_maintenance()
