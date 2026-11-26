"""Daily data-quality checks on yesterday's data (task 11.1): results kept in ops.quality_checks.

Runs after nightly maintenance (03:00) and the Mon/Thu CORPUS refresh (04:00). The task records
every result first, then fails if any check failed, so the history is complete either way.
Rules in orchestration/quality.py.
"""

from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, task


@dag(
    dag_id="quality_checks",
    schedule="30 4 * * *",
    start_date=datetime(2026, 11, 26, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    dagrun_timeout=timedelta(minutes=30),
    tags=["railstream", "quality"],
)
def quality_checks():
    @task(execution_timeout=timedelta(minutes=20))
    def run_quality_checks():
        from orchestration.maintenance import connect
        from orchestration.quality import record_results, run_checks

        spark = connect()
        results = run_checks(spark)
        record_results(spark, results)
        for r in results:
            print(
                f"{'PASS' if r['passed'] else 'FAIL'} {r['check_name']}: {r['value']} ({r['rule']})"
            )
        failed = [r["check_name"] for r in results if not r["passed"]]
        if failed:
            raise RuntimeError(f"data-quality checks failed: {', '.join(failed)}")

    run_quality_checks()


quality_checks()
