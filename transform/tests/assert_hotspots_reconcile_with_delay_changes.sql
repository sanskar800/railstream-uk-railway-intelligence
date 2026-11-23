-- Total delay gained in gold.delay_hotspots equals the total in the shared per-report model:
-- no gain is lost or double-counted by the hourly aggregation. Returns a row when they differ.
with detail as (select sum(greatest(delay_change, 0)) as n from {{ ref('int_delay_changes') }}),
gold as (select sum(delay_gained_min) as n from {{ ref('delay_hotspots') }})
select detail.n as detail_gained, gold.n as gold_gained
from detail, gold
where detail.n <> gold.n
