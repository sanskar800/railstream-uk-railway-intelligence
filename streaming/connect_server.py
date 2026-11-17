"""Spark Connect server: a long-running Spark that clients drive over gRPC (port 15002).

Airflow's batch jobs (Iceberg maintenance, ADR-015) send SQL to it with the lightweight
`pyspark-client`, so Airflow needs neither Java nor access to the Docker socket. Uses exactly the
same Iceberg/Lakekeeper settings as the streaming jobs (streaming.session.spark_conf).

    python -m streaming.connect_server     (the spark-connect service, profile `airflow`)
"""

from __future__ import annotations

import os
from pathlib import Path

import pyspark

from streaming.session import master, spark_conf

SERVER_CLASS = "org.apache.spark.sql.connect.service.SparkConnectServer"


def command() -> list[str]:
    submit = str(Path(pyspark.__file__).parent / "bin" / "spark-submit")
    conf = {**spark_conf(), "spark.connect.grpc.binding.port": "15002"}
    args = [submit, "--class", SERVER_CLASS, "--name", "railstream-connect", "--master", master()]
    for key, value in conf.items():
        args += ["--conf", f"{key}={value}"]
    return [*args, "spark-internal"]  # placeholder primary resource, as start-connect-server.sh


def main() -> None:
    args = command()
    os.execv(args[0], args)  # replace this process: signals reach the JVM directly


if __name__ == "__main__":
    main()
