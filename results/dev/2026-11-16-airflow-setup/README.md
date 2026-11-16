# Airflow setup verification

Development evidence for Day 7.1, not a performance benchmark or a completed
reference-data pipeline. `summary.json` was generated from Docker stats,
Airflow CLI task/run queries and `/api/v2/monitor/health` after startup.

Reproduce with Docker Desktop running:

```powershell
$env:DOCKER_CONTEXT = "desktop-linux"
make airflow
docker compose -f infra/compose/compose.yaml --profile airflow exec -T airflow airflow dags list-import-errors --output json
docker compose -f infra/compose/compose.yaml --profile airflow exec -T airflow airflow dags unpause railstream_platform_check
$runId = "setup_" + [DateTime]::UtcNow.ToString("yyyyMMddTHHmmss")
docker compose -f infra/compose/compose.yaml --profile airflow exec -T airflow airflow dags trigger railstream_platform_check --run-id $runId --output json
```

After the run finishes, query its state:

```powershell
docker compose -f infra/compose/compose.yaml --profile airflow exec -T airflow airflow tasks states-for-dag-run railstream_platform_check $runId --output json
Invoke-RestMethod http://localhost:8086/api/v2/monitor/health
docker stats --no-stream
```

The initial startup exposed a `_CMD` connection-string newline issue, fixed by
writing the URI without a trailing newline. A unit regression test also checks
password URI encoding. Final setup passed; the real scheduler executed the task
successfully. DAG import errors were empty, and local login/token access to the
protected DAG API was verified without saving passwords or tokens.

Rerunning `make airflow` and the database bootstrap preserved metadata and passed.
Only missing Airflow resources were created. No volumes, tables, checkpoints or
cloud resources were deleted, reset or redeployed. Only the new Airflow service
and shared Postgres were running at the end of setup.
