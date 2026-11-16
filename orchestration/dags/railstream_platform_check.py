"""Manual, read-only smoke workflow: prove scheduler -> executor -> task -> logs."""

from datetime import UTC, datetime

from airflow.sdk import dag, task


@dag(
    dag_id="railstream_platform_check",
    schedule=None,
    start_date=datetime(2026, 11, 16, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["railstream", "setup"],
)
def platform_check():
    @task
    def check_task_execution():
        print("RAILSTREAM_PLATFORM_CHECK: task execution succeeded")
        return {"platform": "railstream", "task_execution": "ok"}

    check_task_execution()


platform_check()
