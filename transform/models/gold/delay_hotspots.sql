-- gold.delay_hotspots: where delay is CREATED, not where late trains are seen
-- (docs/metrics.md §3, §5). Grain: UK local date x local hour x location. All trains count
-- (freight occupies the same track), with a passenger-only column alongside.
with ordered as (
    select
        m.*,
        coalesce(o.is_passenger, false) and m.headcode_class in ('1', '2', '9') as is_passenger,
        lag(m.delay_wtt_min) over train as previous_delay,
        lag(m.actual_ts) over train as previous_ts
    from {{ ref('stg_movements') }} as m
    left join {{ ref('operators') }} as o on o.sector_code = m.toc_id
    window train as (partition by m.train_id order by m.actual_ts, m.planned_ts)
),

changes as (
    select
        *,
        -- The first report of a train gains nothing. A gap of more than 6 hours means the
        -- train_id was reused (its last two characters are the day of month): start afresh.
        case
            when previous_delay is null or actual_ts - previous_ts > interval 6 hours then 0
            else delay_wtt_min - previous_delay
        end as delay_change,
        timezone('Europe/London', actual_ts) as actual_local
    from ordered
),

per_location as (
    select
        cast(actual_local as date) as local_date,
        extract(hour from actual_local) as local_hour,
        loc_stanox,
        any_value(location_name) as location_name,
        any_value(crs) as crs,
        sum(greatest(delay_change, 0)) as delay_gained_min,
        count(distinct train_id) filter (where delay_change > 0) as trains_gaining_delay,
        sum(greatest(delay_change, 0)) filter (where is_passenger) as passenger_delay_gained_min,
        sum(greatest(-delay_change, 0)) as delay_recovered_min,
        max(received_at) as latest_received_at
    from changes
    group by local_date, local_hour, loc_stanox
)

select
    *,
    rank() over (partition by local_date, local_hour order by delay_gained_min desc) as rank_in_hour
from per_location
where delay_gained_min > 0 or delay_recovered_min > 0
