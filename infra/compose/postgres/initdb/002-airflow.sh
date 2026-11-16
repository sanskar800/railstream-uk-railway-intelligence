#!/bin/sh
# Safe to run on an existing volume too: create only missing Airflow resources.
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
    --set=airflow_password="$AIRFLOW_DB_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE airflow LOGIN PASSWORD %L', :'airflow_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow')
\gexec
SELECT 'CREATE DATABASE airflow OWNER airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')
\gexec
DO $$ BEGIN
    IF EXISTS (SELECT FROM pg_database
               WHERE datname = 'airflow' AND pg_get_userbyid(datdba) <> 'airflow') THEN
        RAISE EXCEPTION 'Existing airflow database has an unexpected owner; not changing it';
    END IF;
END $$;
SQL
