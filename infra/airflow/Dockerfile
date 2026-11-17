FROM apache/airflow:3.4.0-python3.12

# Keep Airflow fixed while adding the Python Iceberg writer. Constrain transitive
# packages to the tested Airflow/Python combination, rather than upgrading at boot.
# pyspark-client: the pure-Python Spark Connect client (no JVM) for maintenance jobs (ADR-015).
# It must match our Spark server (4.1.4); Airflow's constraint file pins 4.3.0 for its own Spark
# provider, so that single line is dropped and every other package keeps Airflow's tested pin.
RUN curl -fsSL -o /tmp/constraints.txt \
      https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.12.txt \
    && sed -i '/^pyspark-client==/d' /tmp/constraints.txt \
    && pip install --no-cache-dir "apache-airflow==3.4.0" "pyiceberg[s3fs,pyarrow]==0.12.0" \
      "pyspark-client==4.1.4" --constraint /tmp/constraints.txt \
    && pip check \
    && rm /tmp/constraints.txt

ENV PYTHONPATH=/opt/railstream
COPY --chown=airflow:root trust /opt/railstream/trust
COPY --chown=airflow:root orchestration /opt/railstream/orchestration
