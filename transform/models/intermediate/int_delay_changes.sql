-- Delay gained or recovered at each report, relative to the same train's previous report
-- (docs/metrics.md §3). Shared by gold.delay_hotspots and gold.reactionary_delay, so both answers
-- use one definition. Ephemeral: inlined into each consumer.
-- Covers the rebuild window plus one day before it (reactionary clusters crossing midnight)
-- and after an explicit end; reads one more day of silver so lag() sees each train's
-- previous report (ADR-018).
with ordered as (
    select
        m.*,
        coalesce(o.is_passenger, false) and m.headcode_class in ('1', '2', '9') as is_passenger,
        lag(m.delay_wtt_min) over train as previous_delay,
        lag(m.actual_ts) over train as previous_ts
    from {{ ref('stg_movements') }} as m
    left join {{ ref('operators') }} as o on o.sector_code = m.toc_id
    where {{ silver_window('m.actual_ts', before_days=2) }}
    window train as (partition by m.train_id order by m.actual_ts, m.planned_ts)
)

, changes as (
select
    *,
    -- The first report of a train gains nothing. A gap of more than 6 hours means the train_id
    -- was reused (its last two characters are the day of month): start afresh.
    case
        when previous_delay is null or actual_ts - previous_ts > interval 6 hours then 0
        else delay_wtt_min - previous_delay
    end as delay_change,
    timezone('Europe/London', actual_ts) as actual_local
from ordered
)

select * from changes
where cast(actual_local as date) >= {{ rebuild_from() }} - interval 1 day
{%- if rebuild_to() %}
  and cast(actual_local as date) < {{ rebuild_to() }} + interval 1 day
{%- endif %}
