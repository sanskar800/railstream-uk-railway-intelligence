"""Rebuild the gold layer every 15 minutes: check silver's freshness, then dbt build (3 models,
all tests). dbt runs in the image's own environment, inside this container (ADR-017)."""

from datetime import UTC, datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

DBT = "mkdir -p /opt/airflow/dbt && /opt/dbt/bin/dbt"


@dag(
    dag_id="build_gold",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 11, 23, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,  # never two builds writing gold at once
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    dagrun_timeout=timedelta(minutes=14),  # finish before the next slot
    tags=["railstream", "gold"],
)
def build_gold():
    # Fails (error_after: 2 h) if silver has stalled, so gold is never rebuilt on stale data
    # without anyone noticing; warns after 30 min (transform/models/staging/sources.yml).
    freshness = BashOperator(task_id="silver_freshness", bash_command=f"{DBT} source freshness")
    build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build",
        execution_timeout=timedelta(minutes=12),
    )
    freshness >> build


build_gold()
