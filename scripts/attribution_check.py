"""Pattern check of gold against official delay attribution: `make attribution-check`.

Official attribution (Rail Data Marketplace "Historic Delay Attribution", one railway period,
downloaded by hand: no marketplace credentials live in this project) lags ~3 months (findings
F4), so the periods differ: this compares PATTERNS, never the same days.

- Official: delay-minute rows only (EVENT_TYPE 'M'; cancellations carry penalty minutes, not
  lateness), placed at END_STANOX (where the delay was seen, like gold's "later report"),
  reactionary = REACT_REASON filled in. Per day = period total / days in the period.
- Ours: passenger columns of gold.delay_hotspots and gold.reactionary_delay (the file covers
  passenger operators only) over complete capture days, read with DuckDB inside the Airflow
  container on the processing VM (the dbt connection, ADR-017).

Writes results/dev/<date>-attribution-check/{summary,locations}.parquet and prints both.
"""

from __future__ import annotations

import os
import subprocess
import sys
from datetime import date
from pathlib import Path

import duckdb
from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_FILE = ROOT / "data" / "reference" / "Franchise Data Query P05 results.csv"
FIRST_DAY, LAST_DAY = "2026-11-06", "2026-11-19"  # two full weeks of complete capture
TOP_N = 100  # locations ranked by official delay minutes, for the rank correlations

# Runs on the VM inside the Airflow container; prints one CSV row per location.
REMOTE_QUERY = f"""
import duckdb, os, sys
c = duckdb.connect()
c.sql("load iceberg; load httpfs; set TimeZone='UTC'")
c.sql(f\"\"\"create secret (type s3, key_id '{{os.environ["DBT_S3_ACCESS_KEY"]}}',
  secret '{{os.environ["DBT_S3_SECRET_KEY"]}}', endpoint 'seaweedfs:8333', url_style 'path',
  use_ssl false, region 'us-east-1')\"\"\")
c.sql(f\"\"\"attach 'railstream' as lake (type iceberg, endpoint '{{os.environ["CATALOG_URI"]}}',
  authorization_type 'none', access_delegation_mode 'none')\"\"\")
c.sql(\"\"\"
with hot as (
    select loc_stanox, any_value(location_name) as location_name,
           sum(passenger_delay_gained_min) as delay_min
    from lake.gold.delay_hotspots
    where local_date between DATE '{FIRST_DAY}' and DATE '{LAST_DAY}'
    group by 1
), react as (
    select loc_stanox, sum(passenger_reactionary_delay_min) as reactionary_min
    from lake.gold.reactionary_delay
    where local_date between DATE '{FIRST_DAY}' and DATE '{LAST_DAY}'
    group by 1
)
select hot.loc_stanox, hot.location_name, hot.delay_min::double as delay_min,
       coalesce(react.reactionary_min, 0)::double as reactionary_min
from hot left join react using (loc_stanox)
\"\"\").write_csv('/dev/stdout')
"""


def official(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    con.sql(
        f"create view raw as select * from read_csv('{path.as_posix()}', header = true, "
        "all_varchar = true)"
    )
    days = con.sql("select count(distinct ORIGIN_DEPARTURE_DATE) from raw").fetchone()[0]
    con.sql(f"""
        create table official as
        select END_STANOX as loc_stanox,
               sum(PFPI_MINUTES::double) / {days} as delay_min,
               coalesce(sum(PFPI_MINUTES::double)
                   filter (where trim(coalesce(REACT_REASON, '')) <> ''), 0) / {days}
                   as reactionary_min
        from raw
        where EVENT_TYPE = 'M'
        group by 1
    """)


def ours(con: duckdb.DuckDBPyConnection) -> None:
    env = {**dotenv_values(ROOT / ".env"), **os.environ}
    host = env.get("CORE_HOST")
    if not host:
        sys.exit("Set CORE_HOST=ubuntu@<processing-vm-public-ip> in .env")
    key = Path(env.get("CAPTURE_SSH_KEY", Path.home() / ".ssh" / "railstream_oci")).expanduser()
    remote = "docker exec -i railstream-airflow-1 /opt/dbt/bin/python -"
    out = subprocess.run(
        ["ssh", "-i", str(key), host, remote],
        input=REMOTE_QUERY,
        text=True,
        capture_output=True,
        check=True,
    ).stdout
    tmp = ROOT / "data" / "reference" / ".gold_locations.csv"
    tmp.write_text(out, encoding="utf-8")
    days = (date.fromisoformat(LAST_DAY) - date.fromisoformat(FIRST_DAY)).days + 1
    con.sql(f"""
        create table ours as
        select loc_stanox::varchar as loc_stanox, location_name,
               delay_min / {days} as delay_min, reactionary_min / {days} as reactionary_min
        from read_csv('{tmp.as_posix()}', header = true, types = {{'loc_stanox': 'VARCHAR'}})
    """)
    tmp.unlink()


def compare(con: duckdb.DuckDBPyConnection) -> tuple[duckdb.DuckDBPyRelation, ...]:
    con.sql(f"""
        create table locations as
        with top as (
            select * from official order by delay_min desc limit {TOP_N}
        )
        select top.loc_stanox, ours.location_name,
               round(top.delay_min, 1) as official_delay_min_per_day,
               round(coalesce(ours.delay_min, 0), 1) as our_delay_min_per_day,
               round(top.reactionary_min, 1) as official_reactionary_min_per_day,
               round(coalesce(ours.reactionary_min, 0), 1) as our_reactionary_min_per_day,
               rank() over (order by top.delay_min desc) as official_delay_rank,
               rank() over (order by coalesce(ours.delay_min, 0) desc) as our_delay_rank,
               rank() over (order by top.reactionary_min desc) as official_reactionary_rank,
               rank() over (order by coalesce(ours.reactionary_min, 0) desc)
                   as our_reactionary_rank
        from top left join ours using (loc_stanox)
    """)
    summary = con.sql(f"""
        select
            (select round(100 * sum(reactionary_min) / sum(delay_min), 1) from official)
                as official_reactionary_pct,
            (select round(100 * sum(reactionary_min) / sum(delay_min), 1) from ours)
                as our_reactionary_pct,
            round(corr(official_delay_rank, our_delay_rank), 3) as delay_rank_spearman,
            round(corr(official_reactionary_rank, our_reactionary_rank), 3)
                as reactionary_rank_spearman,
            count(*) filter (where official_delay_rank <= 20 and our_delay_rank <= 20)
                as delay_top20_overlap,
            count(*) filter (where official_reactionary_rank <= 20
                             and our_reactionary_rank <= 20) as reactionary_top20_overlap,
            count(*) filter (where location_name is null) as top_locations_missing_in_ours,
            {TOP_N} as top_n,
            DATE '{FIRST_DAY}' as our_first_day,
            DATE '{LAST_DAY}' as our_last_day
        from locations
    """)
    return summary, con.sql("select * from locations order by official_delay_rank")


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")  # DuckDB draws tables with box characters
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_FILE
    if not path.exists():
        sys.exit(f"Official attribution file not found: {path}")
    con = duckdb.connect()
    print(f"reading {path.name} ...", flush=True)
    official(con, path)
    print("reading gold on the cloud VM ...", flush=True)
    ours(con)
    summary, locations = compare(con)
    out = ROOT / "results" / "dev" / f"{date.today().isoformat()}-attribution-check"
    out.mkdir(parents=True, exist_ok=True)
    summary.write_parquet(str(out / "summary.parquet"))
    locations.write_parquet(str(out / "locations.parquet"))
    print(summary)
    print(locations.limit(15))
    print(f"saved to {out}")


if __name__ == "__main__":
    main()
