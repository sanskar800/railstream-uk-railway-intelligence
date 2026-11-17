"""Twice-weekly station reference refresh; the movement stream runs independently."""

import os
from datetime import UTC, datetime, timedelta

from airflow.sdk import dag, get_current_context, task


@dag(
    dag_id="refresh_corpus",
    # Monday and Thursday 04:00 UTC. CORPUS changes every few weeks (11,118 -> 11,133 locations
    # between 2 Oct and 17 Nov 2026); an unchanged file is a no-op, so this only bounds staleness.
    schedule="0 4 * * 1,4",
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
