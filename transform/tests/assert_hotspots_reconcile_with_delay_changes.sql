-- Total delay gained in gold.delay_hotspots equals the total in the shared per-report model:
-- no gain is lost or double-counted by the hourly aggregation. Compared inside the rebuild
-- window this build wrote (ADR-018); returns a row when they differ.
-- Silver is live: only reports received up to the newest one gold has seen are compared, so
-- the check is exact apart from one uncommitted micro-batch: allowed 25 minutes or 0.1%.
with cutoff as (
    select max(latest_received_at) as ts from {{ ref('delay_hotspots') }}
    where {{ in_rebuild_window('local_date') }}
),
detail as (
    select sum(delay_gained_min) as n from {{ ref('int_delay_changes') }}
    where {{ in_rebuild_window('cast(actual_local as date)') }}
      and received_at <= (select ts from cutoff)
),
gold as (
    select sum(delay_gained_min) as n from {{ ref('delay_hotspots') }}
    where {{ in_rebuild_window('local_date') }}
)
select detail.n as detail_gained, gold.n as gold_gained
from detail, gold
where abs(detail.n - gold.n) > greatest(25, 0.001 * detail.n)
