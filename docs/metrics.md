# Metric definitions

*Version 1, 2026-11-05.* Every gold table implements a definition here; if the SQL and this document disagree, one of them is a bug. Field names refer to the TRUST movement message (data-sources.md). Parameters marked **v1** are first choices to be revisited with captured data; changes are logged at the bottom.

## 1. Conventions

- **Time:** stored in UTC (TRUST timestamps converted with the BST rule in data-sources.md); **reported in UK local time** (`Europe/London`), because controllers think in local time. "Hour" and "date" in gold tables are local.
- **Delay is in whole minutes.** TRUST rounds to the minute, so 1 minute late may be 31 s; no metric claims sub-minute precision.
- **Sign:** positive = late, negative = early.
- **One row per real event:** silver removes redeliveries and applies corrections (latest report for the same train, location and planned event wins) before any metric is computed.

## 2. Scope filters

| Filter | Rule | Evidence / reason |
|---|---|---|
| **Passenger train** | Headcode class (3rd character of `train_id`) is `1`, `2` or `9` **and** the operator is a passenger operator (`ref.operators.is_passenger`, i.e. ATOC code ≠ `ZZ`) | In one hour, passenger operators reported almost only classes 1/2/9 (+5 = empty stock) and freight operators mostly 0/3/4/6/7; each rule alone has exceptions (e.g. 6 class-1 movements by GB Railfreight), so both are required |
| **Off-route reports** | Excluded (`variation_status = 'OFF ROUTE'`) | TRUST reports them with variation 0, which would count as on time |
| **Unplanned locations** | Excluded from delay metrics when `planned_timestamp` is empty | No plan, no delay |

Punctuality uses passenger trains only (as official measures do). Hotspots include all trains, because freight delays occupy the same track, with a passenger-only column alongside.

## 3. Delay

| Name | Definition |
|---|---|
| `delay_wtt_min` | `timetable_variation` with sign: `LATE` → +n, `EARLY` → −n, `ON TIME` → 0. Against the **working timetable** (`planned_timestamp`), which includes passing points |
| `delay_gbtt_min` | `actual_ts − gbtt_ts` in whole minutes, only where `gbtt_timestamp` is present (passenger stops). Against the **public timetable** passengers see |
| `delay_gained_min` | For consecutive reports of the same `train_id` ordered by `actual_ts`: `max(0, delay_wtt_min − previous delay_wtt_min)`, attributed to the later report's location. The first report of a train gains nothing |
| `delay_recovered_min` | The negative part of the same difference (time made up) |

**Why two timetables:** punctuality is about what passengers were promised (public timetable). Where delay is *created* is an operational question, and passing points (junctions) only exist in the working timetable.

## 4. Punctuality (brief Q3): `gold.punctuality_hourly`

- **Unit:** one passenger train arriving at its destination: a movement with `planned_event_type = 'DESTINATION'` and `event_type = 'ARRIVAL'`.
- **On time (PPM-style):** `delay_gbtt_min ≤ 5` for regional and London & South East operators, `≤ 10` for long-distance operators. The operator's group is in `ref.operators.service_group` (v1 classification by operator; operators running both kinds of service are treated as regional, a documented simplification).
- **If `gbtt_timestamp` is missing at the destination:** use `delay_wtt_min` and count how often this happens (`arrivals_wtt_fallback`).
- **Grain:** local date × local hour of arrival × operator.
- **Columns:** `arrivals`, `on_time_arrivals`, `on_time_pct`, `avg_delay_min`, `p90_delay_min`, `arrivals_wtt_fallback`.

**Known difference from official PPM:** PPM counts cancelled and part-cancelled trains as failures. v1 reports punctuality over arrivals and cancellations separately (§7), because matching a cancellation to the hour its train would have arrived needs the schedule. Labelled "PPM-style", never "PPM".

## 5. Delay hotspots (brief Q2): `gold.delay_hotspots`

- **Grain:** local date × local hour × location (`loc_stanox`, with name and CRS from CORPUS).
- **Columns:** `delay_gained_min` (sum of `delay_gained_min` at the location), `trains_gaining_delay` (distinct trains with a gain > 0), `passenger_delay_gained_min`, `delay_recovered_min`, `rank_in_hour` (1 = most delay gained).
- **Reading:** a hotspot is where delay is **created**, not where late trains are seen. A late train passing a location without losing more time adds nothing.

## 6. Reactionary delay (brief Q1): `gold.reactionary_delay`

TRUST records what happened, not why, so reactionary delay is an **inference**. Official attribution is done by people using the Delay Attribution Guide; this heuristic approximates one part of it: trains queuing behind a problem at the same place.

**Heuristic v1 (same-location clustering):**
1. A **delay event** is a report with `delay_gained_min ≥ 3` (v1).
2. An **incident cluster** at a location starts with a delay event when no cluster is open there, and stays open while further delay events occur at that location within **30 minutes** (v1) of the previous one.
3. The first train's gain is the **primary delay**; gains by other trains in the cluster are **reactionary delay**.

- **Columns:** `location`, `cluster_start`, `cluster_end`, `primary_train_id`, `primary_delay_min`, `reactionary_trains`, `reactionary_delay_min`.
- **Q1 answer:** locations ranked by `reactionary_delay_min` for clusters starting 06:00–10:00 local.
- **Limitations (stated wherever the metric is shown):** misses delay that spreads to *other* locations along the line; can merge independent problems at busy junctions; ignores train-unit and crew links between services (those need schedule associations). Validated only as a pattern check against official attribution (findings F4).

## 7. Cancellations (brief Q4)

- A train is **cancelled** if its latest status message is a cancellation (`0002`) not followed by a reinstatement (`0005`) for the same `train_id`.
- **Columns:** count by local date × hour × operator × `canx_type` (e.g. `AT ORIGIN`, `EN ROUTE`) × `canx_reason_code`, plus location.

## 8. Data health (brief Q5)

| Metric | Definition |
|---|---|
| **Freshness** | Now − latest `received_at` that has reached each table; shown next to every number |
| **End-to-end latency** (per event) | Time the event is queryable in gold − `actual_ts`. Target: **≤ 5 min at p95** (brief; measured on Day 13) |
| **Feed gap** | A period with no frames while connected longer than N s (v1: N from observed traffic, excluding the Sunday ~02:00 outage), or a disconnection logged as `DATA GAP` |
| **Message rate drop** | Frames per minute falling to about half the recent rate (findings F6: a second bridge splits the feed) |
| **Dead-letter rate** | Messages in `dlq.*` ÷ all messages, by rejection reason |
| **Duplicate rate** | Raw movement messages ÷ distinct event keys |

## 9. Locations and cities

- Metrics are computed per location (STANOX). Each STANOX maps to exactly one CORPUS row (rule fixed on Day 6; findings F3).
- **City groups** (`ref.city_groups`, a small hand-maintained seed: CRS → city) let a controller ask about "London" or "Birmingham". v1: London = its 13 main termini (EUS, KGX, STP, PAD, LST, WAT, VIC, LBG, CHX, FST, MYB, CST, BFR); Birmingham = BHM, BMO, BSW, BHI; other cities their main station(s). Junctions and freight points belong to no city.

## Change log

- v1 (2026-11-05): first definitions.
