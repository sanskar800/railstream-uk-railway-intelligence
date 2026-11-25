-- gold.reactionary_delay: incident clusters per location (docs/metrics.md §6, heuristic v2).
-- An INFERENCE, not official attribution: TRUST records where time was lost, never why.
--   1. a delay event is a report gaining >= 3 minutes;
--   2. a cluster at a location starts with a delay event and stays open while further delay
--      events occur there within 30 minutes of the previous one;
--   2b. (v2) a cluster lasts at most 2 hours: it is split into 2-hour windows counted from its
--      first event, each window a cluster with its own primary train (a busy station otherwise
--      never closes its cluster; findings F30);
--   3. the first train's gain is the primary delay; other trains' gains are reactionary.
-- Clusters are computed over the window plus the day before it, and only those starting
-- inside the window are kept, so a cluster crossing midnight is never cut in two (ADR-018).
-- Limitations (shown wherever the metric is): misses knock-on delay at OTHER locations, can
-- merge independent problems at busy junctions, ignores unit and crew links between services.
{% set gap_minutes = 30 %}
{% set max_cluster_minutes = 120 %}

with events as (
    select
        loc_stanox,
        location_name,
        crs,
        train_id,
        is_passenger,
        actual_ts,
        actual_local,
        delay_gained_min as gain_min
    from {{ ref('int_delay_changes') }}
    where delay_gained_min > 0  -- a step of 3+ minutes (var delay_step_threshold_min)
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

windowed as (
    select
        *,
        floor(
            epoch(actual_ts - min(actual_ts) over (partition by loc_stanox, cluster_seq))
            / ({{ max_cluster_minutes }} * 60)
        ) as window_seq
    from numbered
),

clusters as (
    select
        loc_stanox,
        cluster_seq,
        window_seq,
        any_value(location_name) as location_name,
        any_value(crs) as crs,
        min(actual_ts) as cluster_start,
        max(actual_ts) as cluster_end,
        min(actual_local) as cluster_start_local,
        -- One deterministic choice for both: two trains can open a cluster in the same minute,
        -- and two separate arg_min calls could pick different ones (findings F34).
        first(train_id order by actual_ts, train_id) as primary_train_id,
        first(gain_min order by actual_ts, train_id) as primary_delay_min,
        count(*) as delay_events
    from windowed
    group by loc_stanox, cluster_seq, window_seq
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
join windowed as n
    on n.loc_stanox = c.loc_stanox and n.cluster_seq = c.cluster_seq and n.window_seq = c.window_seq
group by all
having {{ in_rebuild_window('cast(c.cluster_start_local as date)') }}
