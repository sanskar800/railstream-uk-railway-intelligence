# CORPUS reference workflow verification

Generated development correctness evidence, not a throughput/capacity benchmark.

- `workflow-runs.json`: actual Airflow run states and the final repeated run's
  three successful tasks, from the CLI. One early scheduled run failed before
  the private `.env` wiring was corrected; its history is retained. The first
  manual run subsequently succeeded on retry, followed by successful fresh runs.
- `load-results.json`: only safe aggregate `CORPUS_LOAD` log messages. The first
  successful load inserted 11,133 locations for the new content version. Repeats
  reported zero inserted rows. No passwords, tokens or signed URLs are saved.
- `spark-read.json`: the existing Spark reference reader observed both versions,
  22,251 total rows and equally many distinct `(stanox, reference_version)` keys.
  It selected the 11,133-location version and resolved Oxford correctly. The
  original 11,118-location version was preserved.
- `integration.json`: a focused isolated-table scenario verified DAG import,
  task wiring/schedule, deterministic duplicate-key selection, a repeated load
  without another snapshot, immutable old versions and changed-input rejection.
  The test table is retained; no deletes or kills.
- `memory-samples.json`: six Docker samples during/after another real workflow
  run. The largest sampled value was 1.272 GiB under a 1.5 GiB cap; sampling may
  miss an instantaneous peak. No OOM or automatic restart was observed.

Reproduce from the project root, with private `.env` populated:

```powershell
$env:DOCKER_CONTEXT = "desktop-linux"
make airflow
uv run pytest -m integration -s tests/integration/test_corpus.py
$runId = "corpus_check_" + [DateTime]::UtcNow.ToString("yyyyMMddTHHmmss")
docker compose --env-file .env -f infra/compose/compose.yaml --profile airflow exec -T airflow airflow dags trigger refresh_corpus --run-id $runId --output json
```

After completion:

```powershell
docker compose --env-file .env -f infra/compose/compose.yaml --profile airflow exec -T airflow airflow tasks states-for-dag-run refresh_corpus $runId --output json
```

The integration test needs no external account access: it writes a tiny synthetic
file to a unique retained reference table. The triggered daily DAG downloads the
actual file using credentials already in `.env`. Rebuild via `make airflow` after
editing shared workflow code; DAG definitions are mounted read-only, while shared
logic and dependencies are baked into the image.
