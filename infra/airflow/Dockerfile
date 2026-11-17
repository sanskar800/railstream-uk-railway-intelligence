FROM apache/airflow:3.4.0-python3.12

# Keep Airflow fixed while adding the Python Iceberg writer. Constrain transitive
# packages to the tested Airflow/Python combination, rather than upgrading at boot.
# pyspark-client: the pure-Python Spark Connect client (no JVM) for maintenance jobs (ADR-015).
RUN pip install --no-cache-dir "apache-airflow==3.4.0" "pyiceberg[s3fs,pyarrow]==0.12.0" \
    "pyspark-client==4.1.4" \
    --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.12.txt \
    && pip check

ENV PYTHONPATH=/opt/railstream
COPY --chown=airflow:root trust /opt/railstream/trust
COPY --chown=airflow:root orchestration /opt/railstream/orchestration
