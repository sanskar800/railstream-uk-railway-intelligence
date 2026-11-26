-- Golden replay (task 11.4): gold for one fixed, final day must equal committed values.
-- 2026-11-18 was recomputed independently in Spark and matched (findings F34). Its silver is
-- final and its gold changes only when a backfill rewrites it, so any difference here means a
-- logic change or a bad backfill: fix it, or regenerate seeds/golden_2026_11_18.csv in the same
-- commit and say why. Returns every metric whose value differs or is missing on either side.
{% set day = "DATE '2026-11-18'" %}
with actual as (
    select 'punctuality_arrivals' as metric, toc_id as key, sum(arrivals) as value
    from {{ ref('punctuality_hourly') }} where local_date = {{ day }} group by toc_id
    union all
    select 'punctuality_on_time', toc_id, sum(on_time_arrivals)
    from {{ ref('punctuality_hourly') }} where local_date = {{ day }} group by toc_id
    union all
    select 'punctuality_sum_delay', toc_id, sum(sum_delay_min)
    from {{ ref('punctuality_hourly') }} where local_date = {{ day }} group by toc_id
    union all
    select unnest(['hotspots_delay_gained', 'hotspots_delay_recovered', 'hotspots_origin_delay',
                   'hotspots_passenger_delay_gained', 'hotspots_rows']),
           'all',
           unnest([sum(delay_gained_min), sum(delay_recovered_min), sum(origin_delay_min),
                   sum(passenger_delay_gained_min), count(*)])
    from {{ ref('delay_hotspots') }} where local_date = {{ day }}
    union all
    select unnest(['reactionary_clusters', 'reactionary_trains', 'reactionary_delay',
                   'reactionary_passenger_delay', 'reactionary_primary_delay']),
           'all',
           unnest([count(*), sum(reactionary_trains), sum(reactionary_delay_min),
                   sum(passenger_reactionary_delay_min), sum(primary_delay_min)])
    from {{ ref('reactionary_delay') }} where local_date = {{ day }}
)

select
    coalesce(a.metric, g.metric) as metric,
    coalesce(a.key, g.key) as key,
    a.value as actual,
    g.value as expected
from actual as a
full outer join {{ ref('golden_2026_11_18') }} as g on a.metric = g.metric and a.key = g.key
where a.value is distinct from g.value
