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

from pyspark.sql import SparkSession

CATALOG = "lake"


def build_session(app_name: str) -> SparkSession:
    env = os.environ.get
    jars = ",".join(sorted(glob.glob("/opt/jars/*.jar")))
    builder = (
        SparkSession.builder.appName(app_name)
        .master(env("SPARK_MASTER", "local[2]"))  # 2 cores: the laptop also runs the stack
        .config("spark.driver.memory", env("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.jars", jars)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")  # default 200 is for clusters
        .config("spark.ui.port", "4040")
        .config("spark.ui.showConsoleProgress", "false")  # progress bars garble logs
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "rest")
        .config(
            f"spark.sql.catalog.{CATALOG}.uri", env("CATALOG_URI", "http://lakekeeper:8181/catalog")
        )
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", env("WAREHOUSE", "railstream"))
        .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.defaultCatalog", CATALOG)
    )
    return builder.getOrCreate()
