"""Exactly-once audit of a Kafka-fed Iceberg table: prints one JSON line.

For a table with kafka_partition and kafka_offset columns (bronze): row count, distinct
(partition, offset) pairs, and per partition the offset range and whether it has gaps.
Rows == distinct means no duplicates; contiguous offsets per partition mean no losses.

`python -m streaming.audit TABLE [--drop]` (in the Spark container). --drop removes the table
afterwards (plain DROP: Lakekeeper deletes the files, see streaming/session.py).
"""

from __future__ import annotations

import argparse
import json

from streaming.session import build_session


def audit(spark, table: str) -> dict:
    t = spark.table(table)
    partitions = {
        str(r["p"]): {
            "lo": r["lo"],
            "hi": r["hi"],
            "rows": r["n"],
            "no_gaps": r["hi"] - r["lo"] + 1 == r["n"],
        }
        for r in spark.sql(
            f"SELECT kafka_partition p, min(kafka_offset) lo, max(kafka_offset) hi, count(*) n "
            f"FROM {table} GROUP BY 1 ORDER BY 1"
        ).collect()
    }
    snapshots = spark.sql(f"SELECT count(*) FROM {table}.snapshots").first()[0]
    return {
        "table": table,
        "rows": t.count(),
        "distinct_offsets": t.select("kafka_partition", "kafka_offset").distinct().count(),
        "snapshots": snapshots,
        "partitions": partitions,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("table")
    parser.add_argument("--drop", action="store_true")
    args = parser.parse_args()
    spark = build_session("audit")
    spark.sparkContext.setLogLevel("ERROR")
    result = audit(spark, args.table)
    if args.drop:
        spark.sql(f"DROP TABLE {args.table}")
    print("AUDIT " + json.dumps(result), flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
