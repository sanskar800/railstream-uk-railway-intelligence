-- Total delay gained in gold.delay_hotspots equals the total in the shared per-report model:
-- no gain is lost or double-counted by the hourly aggregation. Returns a row when they differ
-- beyond live drift.
-- Silver is live: rows arrive between building a model and testing it (~1,400 a minute), so
-- an exact comparison is flaky (confirmed: it passes when run immediately after the build).
-- Real bugs (a duplicating join, a lost filter) differ by thousands of rows or percent; the
-- tolerance of 0.1% (minimum 10) covers about three minutes of live drift.
with detail as (select sum(greatest(delay_change, 0)) as n from {{ ref('int_delay_changes') }}),
gold as (select sum(delay_gained_min) as n from {{ ref('delay_hotspots') }})
select detail.n as detail_gained, gold.n as gold_gained
from detail, gold
where abs(detail.n - gold.n) > greatest(10, 0.001 * detail.n)
