"""Join one deterministic CORPUS row per location (read side).

The reference table has one writer, orchestration/corpus.py (Airflow DAG and make load-corpus).
"""

from pyspark.sql import functions as F
from pyspark.sql import types as T

SCHEMA = T.StructType(
    [
        T.StructField(k, T.TimestampType() if k == "loaded_at" else T.StringType())
        for k in ("stanox", "location_name", "crs", "tiploc", "reference_version", "loaded_at")
    ]
)


def reference_snapshot(spark, table):
    spark.catalog.refreshTable(table)
    source = spark.table(table)
    latest = source.orderBy(F.col("loaded_at").desc(), F.col("reference_version").asc()).first()
    if latest is None:
        raise ValueError("reference table is empty; load CORPUS first")
    result = source.where(F.col("reference_version") == latest.reference_version)
    if result.groupBy("stanox").count().where("count > 1").limit(1).count():
        raise ValueError("duplicate STANOX in selected CORPUS version")
    return result


def enrich(movements, locations):
    names = ["location_name", "crs", "tiploc", "location_matched", "location_reference_version"]
    base = movements.drop(*names)
    ref = locations.select(
        F.col("stanox").alias("_ref_stanox"),
        "location_name",
        "crs",
        "tiploc",
        F.col("reference_version").alias("location_reference_version"),
    )
    version = locations.select("reference_version").first()[0]
    return (
        base.join(F.broadcast(ref), base.loc_stanox == ref._ref_stanox, "left")
        .withColumn("location_matched", F.col("_ref_stanox").isNotNull())
        .withColumn("location_reference_version", F.lit(version))
        .drop("_ref_stanox")
    )
