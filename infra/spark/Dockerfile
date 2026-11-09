# Spark for the streaming jobs: Python 3.12 (same as the project) + Java 17 + pyspark from uv.lock.
# The official apache/spark image ships Python 3.10, too old for the shared code in trust/ (ADR-007).
FROM python:3.12-slim-bookworm

# Spark 4.1 runs on Java 17 or 21; Debian bookworm ships OpenJDK 17. procps: Spark's scripts use ps.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Connector jars, pinned and checked against Maven Central's SHA-1 (downloaded at build time only).
# Iceberg 1.12.0 is the newest release with a Spark 4.1 runtime; there is none yet for Spark 4.2+.
ARG MAVEN=https://repo1.maven.org/maven2
RUN mkdir -p /opt/jars && cd /opt/jars && for path in \
      org/apache/iceberg/iceberg-spark-runtime-4.1_2.13/1.12.0/iceberg-spark-runtime-4.1_2.13-1.12.0.jar \
      org/apache/iceberg/iceberg-aws-bundle/1.12.0/iceberg-aws-bundle-1.12.0.jar \
      org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.4/spark-sql-kafka-0-10_2.13-4.1.4.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.4/spark-token-provider-kafka-0-10_2.13-4.1.4.jar \
      org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar \
      org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar \
    ; do \
      jar="$(basename "$path")"; \
      curl -fsSL -o "$jar" "$MAVEN/$path"; \
      echo "$(curl -fsSL "$MAVEN/$path.sha1" | cut -c1-40)  $jar" | sha1sum -c -; \
    done

ENV PYTHONUNBUFFERED=1 \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN pip install --no-cache-dir uv==0.9.9
WORKDIR /app
COPY pyproject.toml uv.lock ./
# --no-cache: pyspark is a source distribution; its build cache (~1.5 GB) must not stay in the layer.
RUN uv sync --locked --no-dev --group spark --no-install-project --no-cache

COPY ingest ./ingest
COPY trust ./trust
COPY contracts ./contracts
COPY streaming ./streaming

# Non-root; /checkpoints is a named volume, which takes this ownership on first mount.
RUN useradd --uid 10001 --create-home spark && mkdir -p /checkpoints && chown spark /checkpoints
USER spark
