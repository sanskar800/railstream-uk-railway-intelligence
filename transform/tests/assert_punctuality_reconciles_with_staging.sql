-- Every qualifying destination arrival in staging is counted exactly once in gold
-- (docs/metrics.md §2, §4). Returns a row when the totals differ.
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
where staged.n <> gold.n
