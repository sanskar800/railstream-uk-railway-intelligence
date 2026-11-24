-- Every qualifying destination arrival in staging is counted exactly once in gold
-- (docs/metrics.md §2, §4), inside the rebuild window this build wrote (ADR-018). Returns a
-- row when the totals differ.
-- Silver is live: rows keep arriving after the model is built. Only events received up to the
-- newest one gold has seen are compared, so the check is exact apart from rows silver had
-- received but not yet committed when the model was built (one micro-batch, a few dozen at
-- most): allowed 25 or 0.1%. A duplicating join or a lost filter differs by far more.
with cutoff as (
    select max(latest_received_at) as ts from {{ ref('punctuality_hourly') }}
    where {{ in_rebuild_window('local_date') }}
),
staged as (
    select count(*) as n
    from {{ ref('stg_movements') }} as m
    join {{ ref('operators') }} as o on o.sector_code = m.toc_id
    where m.planned_event_type = 'DESTINATION' and m.event_type = 'ARRIVAL'
      and m.headcode_class in ('1', '2', '9') and o.is_passenger
      and {{ silver_window('m.actual_ts') }}
      and {{ in_rebuild_window("cast(timezone('Europe/London', m.actual_ts) as date)") }}
      and m.received_at <= (select ts from cutoff)
),
gold as (
    select sum(arrivals) as n from {{ ref('punctuality_hourly') }}
    where {{ in_rebuild_window('local_date') }}
)
select staged.n as staged_arrivals, gold.n as gold_arrivals
from staged, gold
where abs(staged.n - gold.n) > greatest(25, 0.001 * staged.n)
