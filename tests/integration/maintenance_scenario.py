"""DAG wiring plus a real maintenance run on a throwaway table (in the Airflow container)."""

import json
import uuid
from datetime import UTC, datetime, timedelta

from airflow.dag_processing.dagbag import DagBag

from orchestration.maintenance import TablePolicy, connect, maintain_table, record_run

bag = DagBag(dag_folder="/opt/airflow/dags")
assert not bag.import_errors, bag.import_errors
dag = bag.dags["iceberg_maintenance"]
order = ["maintain_bronze_trust_movement", "maintain_silver_movement_revisions",
         "maintain_silver_trust_movement", "maintain_silver_rejected_messages",
         "maintain_ref_locations"]  # fmt: skip
assert list(dag.task_ids) == order, dag.task_ids
for upstream, downstream in zip(order, order[1:], strict=False):
    assert dag.get_task(upstream).downstream_task_ids == {downstream}
assert dag.max_active_runs == 1 and not dag.catchup

spark = connect()
ns = f"maint_it_{uuid.uuid4().hex[:8]}"
table = f"{ns}.events"
spark.sql(f"CREATE NAMESPACE {ns}")
spark.sql(f"CREATE TABLE {table} (id BIGINT, ts TIMESTAMP) USING iceberg PARTITIONED BY (days(ts))")
now = datetime.now(UTC)
yesterday, today = now - timedelta(days=1), now
for i in range(12):  # 12 small commits into yesterday's partition (finished: may be compacted)
    spark.sql(f"INSERT INTO {table} VALUES ({i}, TIMESTAMP '{yesterday:%Y-%m-%d %H:%M:%S}')")
for i in range(12, 15):  # 3 into today's partition (still being written: must be left alone)
    spark.sql(f"INSERT INTO {table} VALUES ({i}, TIMESTAMP '{today:%Y-%m-%d %H:%M:%S}')")


def files_in(day):
    return spark.sql(
        f"SELECT count(*) FROM {table}.files WHERE partition.ts_day = DATE '{day:%Y-%m-%d}'"
    ).first()[0]


try:
    record = maintain_table(spark, TablePolicy(table, "ts", 0), now)
    record_run(spark, record, runs_table=f"{ns}.maintenance_runs")
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES {table}").collect()}
    summary = {
        "files_before": record["data_files_before"], "files_after": record["data_files_after"],
        "yesterday_files": files_in(yesterday), "today_files": files_in(today),
        "rows": spark.table(table).count(), "rewritten": record["rewritten_files"],
        "metadata_cleanup": props.get("write.metadata.delete-after-commit.enabled"),
        "recorded_runs": spark.table(f"{ns}.maintenance_runs").count(),
    }  # fmt: skip
    print("MAINTENANCE_SCENARIO " + json.dumps(summary), flush=True)
finally:
    for t in (table, f"{ns}.maintenance_runs"):
        spark.sql(f"DROP TABLE IF EXISTS {t}")  # plain DROP: Lakekeeper deletes the files
    spark.sql(f"DROP NAMESPACE IF EXISTS {ns}")
