-- gold.delay_hotspots: where delay is CREATED, not where late trains are seen
-- (docs/metrics.md §3, §5). Grain: UK local date x local hour x location. All trains count
-- (freight occupies the same track), with a passenger-only column alongside.
with per_location as (
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
    from {{ ref('int_delay_changes') }}
    where {{ in_rebuild_window('cast(actual_local as date)') }}
    group by local_date, local_hour, loc_stanox
)

select
    *,
    rank() over (partition by local_date, local_hour order by delay_gained_min desc) as rank_in_hour
from per_location
where delay_gained_min > 0 or delay_recovered_min > 0
