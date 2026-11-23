-- gold.reactionary_delay: incident clusters per location (docs/metrics.md §6, heuristic v1).
-- An INFERENCE, not official attribution: TRUST records where time was lost, never why.
--   1. a delay event is a report gaining >= 3 minutes;
--   2. a cluster at a location starts with a delay event and stays open while further delay
--      events occur there within 30 minutes of the previous one;
--   3. the first train's gain is the primary delay; other trains' gains are reactionary.
-- Limitations (shown wherever the metric is): misses knock-on delay at OTHER locations, can
-- merge independent problems at busy junctions, ignores unit and crew links between services.
{% set min_gain_min = 3 %}
{% set gap_minutes = 30 %}

with events as (
    select
        loc_stanox,
        location_name,
        crs,
        train_id,
        is_passenger,
        actual_ts,
        actual_local,
        delay_change as gain_min
    from {{ ref('int_delay_changes') }}
    where delay_change >= {{ min_gain_min }}
),

marked as (
    select
        *,
        case
            when lag(actual_ts) over loc is null
                or actual_ts - lag(actual_ts) over loc > interval {{ gap_minutes }} minutes
                then 1
            else 0
        end as starts_cluster
    from events
    window loc as (partition by loc_stanox order by actual_ts, train_id)
),

numbered as (
    select
        *,
        sum(starts_cluster) over (
            partition by loc_stanox order by actual_ts, train_id rows unbounded preceding
        ) as cluster_seq
    from marked
),

clusters as (
    select
        loc_stanox,
        cluster_seq,
        any_value(location_name) as location_name,
        any_value(crs) as crs,
        min(actual_ts) as cluster_start,
        max(actual_ts) as cluster_end,
        min(actual_local) as cluster_start_local,
        arg_min(train_id, actual_ts) as primary_train_id,
        arg_min(gain_min, actual_ts) as primary_delay_min,
        count(*) as delay_events
    from numbered
    group by loc_stanox, cluster_seq
)

select
    c.loc_stanox,
    c.location_name,
    c.crs,
    c.cluster_start,
    c.cluster_end,
    cast(c.cluster_start_local as date) as local_date,
    extract(hour from c.cluster_start_local) as local_hour,
    c.primary_train_id,
    c.primary_delay_min,
    count(distinct n.train_id) filter (where n.train_id <> c.primary_train_id) as reactionary_trains,
    coalesce(sum(n.gain_min) filter (where n.train_id <> c.primary_train_id), 0)
        as reactionary_delay_min,
    coalesce(sum(n.gain_min) filter (where n.train_id <> c.primary_train_id and n.is_passenger), 0)
        as passenger_reactionary_delay_min,
    c.delay_events
from clusters as c
join numbered as n on n.loc_stanox = c.loc_stanox and n.cluster_seq = c.cluster_seq
group by all
