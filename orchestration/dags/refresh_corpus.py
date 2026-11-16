"""Daily station reference refresh; the movement stream runs independently."""

import os
from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, get_current_context, task


@dag(
    dag_id="refresh_corpus",
    schedule="0 4 * * *",
    start_date=datetime(2026, 11, 16, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
    dagrun_timeout=timedelta(minutes=20),
    tags=["railstream", "reference"],
)
def refresh_corpus():
    @task(execution_timeout=timedelta(minutes=3))
    def download():
        from orchestration.corpus import download_corpus, run_directory

        return download_corpus(
            run_directory(get_current_context()["run_id"]),
            os.environ.get("NR_USERNAME", ""),
            os.environ.get("NR_PASSWORD", ""),
        )

    @task
    def validate(path):
        from orchestration.corpus import validate_corpus

        return validate_corpus(path)

    @task(execution_timeout=timedelta(minutes=5))
    def load(manifest):
        from orchestration.corpus import load_corpus

        result = load_corpus(manifest)
        print(
            "CORPUS_LOAD: "
            f"version={result['reference_version']} locations={result['locations']} "
            f"inserted={result['inserted']}"
        )
        return result

    load(validate(download()))


refresh_corpus()
