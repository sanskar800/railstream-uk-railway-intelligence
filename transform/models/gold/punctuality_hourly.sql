-- gold.punctuality_hourly: PPM-style punctuality of passenger trains at their destination
-- (docs/metrics.md §4). Grain: UK local date x local hour x operator x destination.
-- Labelled "PPM-style", never "PPM": cancellations are reported separately (§7).
with arrivals as (
    select
        m.*,
        o.operator_name,
        o.service_group,
        timezone('Europe/London', m.actual_ts) as actual_local,
        coalesce(m.delay_gbtt_min, m.delay_wtt_min) as delay_min,      -- public timetable first
        m.delay_gbtt_min is null as wtt_fallback,                      -- §4: count the fallbacks
        case when o.service_group = 'long_distance' then 10 else 5 end as threshold_min
    from {{ ref('stg_movements') }} as m
    join {{ ref('operators') }} as o on o.sector_code = m.toc_id
    where m.planned_event_type = 'DESTINATION'
      and m.event_type = 'ARRIVAL'
      and m.headcode_class in ('1', '2', '9')   -- §2: passenger headcode classes ...
      and o.is_passenger                         -- ... and a passenger operator (both needed)
      and {{ silver_window('m.actual_ts') }}     -- prune silver to the rebuild window
)
select
    cast(actual_local as date) as local_date,
    extract(hour from actual_local) as local_hour,
    toc_id,
    any_value(operator_name) as operator_name,
    any_value(service_group) as service_group,
    loc_stanox as destination_stanox,
    any_value(location_name) as destination_name,
    any_value(crs) as destination_crs,
    count(*) as arrivals,
    count(*) filter (where delay_min <= threshold_min) as on_time_arrivals,
    round(100.0 * count(*) filter (where delay_min <= threshold_min) / count(*), 1) as on_time_pct,
    round(avg(delay_min), 2) as avg_delay_min,
    sum(delay_min) as sum_delay_min,                     -- lets readers re-aggregate the average
    quantile_cont(delay_min, 0.9) as p90_delay_min,
    count(*) filter (where wtt_fallback) as arrivals_wtt_fallback,
    max(received_at) as latest_received_at               -- freshness (§8)
from arrivals
where {{ in_rebuild_window('cast(actual_local as date)') }}
group by local_date, local_hour, toc_id, loc_stanox
