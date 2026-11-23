-- Every qualifying destination arrival in staging is counted exactly once in gold
-- (docs/metrics.md §2, §4). Returns a row when the totals differ beyond live drift.
-- Silver is live: rows arrive between building a model and testing it (~1,400 a minute), so
-- an exact comparison is flaky (confirmed: it passes when run immediately after the build).
-- Real bugs (a duplicating join, a lost filter) differ by thousands of rows or percent; the
-- tolerance of 0.1% (minimum 10) covers about three minutes of live drift.
with staged as (
    select count(*) as n
    from {{ ref('stg_movements') }} as m
    join {{ ref('operators') }} as o on o.sector_code = m.toc_id
    where m.planned_event_type = 'DESTINATION' and m.event_type = 'ARRIVAL'
      and m.headcode_class in ('1', '2', '9') and o.is_passenger
),
gold as (select sum(arrivals) as n from {{ ref('punctuality_hourly') }})
select staged.n as staged_arrivals, gold.n as gold_arrivals
from staged, gold
where abs(staged.n - gold.n) > greatest(10, 0.001 * staged.n)
