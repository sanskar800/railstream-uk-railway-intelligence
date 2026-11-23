"""The Spark session for the streaming jobs: local mode, capped memory, Iceberg via Lakekeeper.

Storage access: Spark holds no S3 keys. Lakekeeper (the REST catalog) answers each table load
with the storage endpoint and signs every S3 request for the client ("remote signing"), so
credentials stay in the catalog. Checkpoints are on a local volume, not S3 (ADR-007).

Dropping tables: use DROP TABLE without PURGE. Lakekeeper drops the table and deletes its files
itself; with PURGE, Spark then also tries to delete them and the catalog (rightly) refuses to sign
requests for a table that no longer exists, so the statement errors after succeeding.
"""

from __future__ import annotations

import glob
import os
import shutil

from pyspark.sql import SparkSession

CATALOG = "lake"


def spark_conf() -> dict[str, str]:
    """Spark settings shared by the streaming jobs and the Spark Connect server (one source)."""
    env = os.environ.get
    return {
        "spark.driver.memory": env("SPARK_DRIVER_MEMORY", "1g"),
        "spark.jars": ",".join(sorted(glob.glob("/opt/jars/*.jar"))),
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": env("SPARK_SHUFFLE_PARTITIONS", "4"),
        "spark.ui.port": "4040",
        "spark.ui.showConsoleProgress": "false",  # progress bars garble logs
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        f"spark.sql.catalog.{CATALOG}": "org.apache.iceberg.spark.SparkCatalog",
        f"spark.sql.catalog.{CATALOG}.type": "rest",
        f"spark.sql.catalog.{CATALOG}.uri": env("CATALOG_URI", "http://lakekeeper:8181/catalog"),
        f"spark.sql.catalog.{CATALOG}.warehouse": env("WAREHOUSE", "railstream"),
        f"spark.sql.catalog.{CATALOG}.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "spark.sql.defaultCatalog": CATALOG,
    }


def master() -> str:
    return os.environ.get("SPARK_MASTER", "local[2]")  # 2 cores: the VM's whole CPU


def remove_stale_local_dirs(root: str = "/tmp") -> int:
    """Delete Spark scratch directories left by a previous run of this container.

    Spark removes them on a clean exit, not after a crash. `restart: unless-stopped` restarts the
    same container, so crash-looping silver piled them up inside its writable layer (~11 GB,
    findings F28). One Spark application per container, so at startup none of them is in use.
    """
    removed = 0
    for path in glob.glob(f"{root}/spark-*") + glob.glob(f"{root}/blockmgr-*"):
        shutil.rmtree(path, ignore_errors=True)
        removed += 1
    return removed


def build_session(app_name: str) -> SparkSession:
    remove_stale_local_dirs()
    builder = SparkSession.builder.appName(app_name).master(master())
    for key, value in spark_conf().items():
        builder = builder.config(key, value)
    return builder.getOrCreate()
