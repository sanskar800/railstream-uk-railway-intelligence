#!/bin/sh
# Runs once, when the Postgres volume is first created: one database and owner per service.
set -eu

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres <<SQL
CREATE USER lakekeeper WITH PASSWORD '${LAKEKEEPER_DB_PASSWORD}';
CREATE DATABASE lakekeeper OWNER lakekeeper;
SQL
