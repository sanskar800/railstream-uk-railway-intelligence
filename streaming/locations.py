"""Load immutable CORPUS versions and join one deterministic reference row per location."""

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql import types as T

from streaming.session import build_session
from trust.locations import read_corpus

SCHEMA = T.StructType(
    [
        T.StructField(k, T.TimestampType() if k == "loaded_at" else T.StringType())
        for k in ("stanox", "location_name", "crs", "tiploc", "reference_version", "loaded_at")
    ]
)


def load_locations(spark, path: Path, table="ref.locations"):
    from streaming.silver import identifier

    identifier(table)
    version, rows = read_corpus(path)
    now = datetime.now(UTC)
    data = spark.createDataFrame(
        [dict(row, reference_version=version, loaded_at=now) for row in rows],
        SCHEMA,
    )
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table.split('.')[0]}")
    if not spark.catalog.tableExists(table):
        data.writeTo(table).using("iceberg").create()
    else:
        spark.catalog.refreshTable(table)
        data.createOrReplaceTempView("corpus_load")
        spark.sql(
            f"MERGE INTO {table} t USING corpus_load s ON "
            "t.stanox=s.stanox AND t.reference_version=s.reference_version "
            "WHEN NOT MATCHED THEN INSERT *"
        )
    spark.catalog.refreshTable(table)
    count = spark.table(table).where(F.col("reference_version") == version).count()
    if count != len(rows):
        raise ValueError(f"CORPUS version has {count} rows; expected {len(rows)}")
    return {"table": table, "reference_version": version, "locations": count}


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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("file", type=Path)
    parser.add_argument("--table", default="ref.locations")
    args = parser.parse_args()
    spark = build_session("load_corpus")
    spark.sparkContext.setLogLevel("WARN")
    print("CORPUS_LOAD " + json.dumps(load_locations(spark, args.file, args.table)), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
