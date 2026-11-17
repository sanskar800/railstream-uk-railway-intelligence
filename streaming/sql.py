"""Run SQL from stdin against the lakehouse and print the result (in the Spark container).

    echo "SELECT count(*) FROM silver.trust_movement" | python -m streaming.sql

Several statements may be separated by ";". Used by `make sql` (scripts/cloud_sql.py), which runs
this on the processing VM. For ad-hoc reading; tables are written only by the pipeline.
"""

from __future__ import annotations

import os
import sys

from streaming.session import build_session


def main() -> None:
    statements = [s.strip() for s in sys.stdin.read().split(";") if s.strip()]
    if not statements:
        sys.exit("no SQL on stdin")
    spark = build_session("sql")
    spark.sparkContext.setLogLevel("ERROR")
    rows = int(os.environ.get("SQL_ROWS", "50"))
    for statement in statements:
        print(f"\n> {statement}")
        spark.sql(statement).show(rows, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
