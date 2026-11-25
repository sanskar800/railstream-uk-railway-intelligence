-- Silver movements with the delays and flags every gold model needs (docs/metrics.md §1–3).
-- Times stay UTC here; gold converts to UK local time for reporting.
select
    m.train_id,
    m.toc_id,
    substr(m.train_id, 3, 1) as headcode_class,
    m.event_type,
    m.planned_event_type,
    m.actual_ts,
    m.planned_ts,
    m.gbtt_ts,
    m.loc_stanox,
    m.location_name,
    m.crs,
    m.variation_status,
    m.delay_min as delay_wtt_min,  -- signed, against the working timetable (silver)
    case
        when m.gbtt_ts is not null
            then cast(round((epoch(m.actual_ts) - epoch(m.gbtt_ts)) / 60) as integer)
    end as delay_gbtt_min,  -- against the public timetable, passenger stops only
    m.is_offroute,
    m.identity_unresolved,
    m.revision_conflict,
    m.received_at,
    m.msg_queue_ts,  -- with event_key, makes each train's report order total (int_delay_changes)
    m.event_key
from {{ source('silver', 'trust_movement') }} as m
-- metrics.md §2: off-route reports and calls without a plan have no meaningful delay.
where m.variation_status <> 'OFF ROUTE'
  and m.planned_ts is not null
  and not m.identity_unresolved
