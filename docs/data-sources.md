# Data sources

RailStream uses Network Rail open data under the [Open Government Licence v3](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/). It is an **unofficial, independent project**: no Network Rail branding, and it does not present itself as official.

| Source | Kind | Used for | Access |
|---|---|---|---|
| TRUST train movements (`TRAIN_MVT_ALL_TOC`) | Live, STOMP | The event stream: every reported arrival, departure, activation, cancellation | Network Rail data feeds account (development.md §2) |
| CORPUS | Static file, HTTP | Location names and codes for enrichment | Same account |
| SCHEDULE | Static file, HTTP | Planned timetable (later: planned-vs-actual) | Same account |
| Historic delay attribution | Static files, per railway period | One-off sanity check of the gold delay models | Rail Data Marketplace (development.md §3); ~3 months behind (findings F4) |

Facts about the TRUST feed below are from the Open Rail Data wiki page [Train Movements](https://wiki.openraildata.com/index.php/Train_Movements) (last edited 2025-04-23; read 2026-11-05) unless marked as **observed**, meaning seen in our own capture (findings F1–F3).

## TRUST train movements

TRUST is Network Rail's internal train-reporting system. It reports events that have happened; it makes no predictions. Not every location generates a report, only TRUST timing points.

### Delivery

- One topic: `TRAIN_MVT_ALL_TOC` (the old per-operator topics are historical).
- Each STOMP frame body is a **JSON array** of zero or more messages, sent every 5 s or when 32 messages are waiting. **An empty array is legitimate.** Observed: ≈27 messages per frame at evening peak (F1).
- **Weekly planned outage:** about 5 minutes, normally between 02:00 and 02:15 early on Sunday. Missed messages are sent when the feed resumes, so a short gap followed by a burst of late events is expected, not an incident.

### Message types

Each message is `{"header": {...}, "body": {...}}`; `header.msg_type` says which kind.

| `msg_type` | Meaning | Use in RailStream |
|---|---|---|
| 0001 | Train activation (a scheduled train is "called" into running) | Links `train_id` to its schedule (`train_uid`, origin, departure) |
| 0002 | Train cancellation | Cancellations (counted separately from lateness) |
| **0003** | **Train movement** (arrival/departure at a timing point) | **Core fact table** |
| 0004 | Unidentified train | Not used in production |
| 0005 | Train reinstatement | Undo of a cancellation |
| 0006 | Change of origin | Train starts somewhere other than planned |
| 0007 | Change of identity | `train_id` changes for the same train |
| 0008 | Change of location | Location of a report corrected |

Observed in one peak minute: 0003, 0001 and 0002 only (F1). The other types are rarer; the contract must accept all of them.

### Movement (0003) fields used

All values arrive as **strings**, including numbers and booleans (observed).

| Field | Example | Meaning |
|---|---|---|
| `train_id` | `872L09MX03` | 10-character TRUST id of this running of the train |
| `event_type` / `planned_event_type` | `DEPARTURE` | `ARRIVAL` or `DEPARTURE` |
| `loc_stanox` | `87261` | Location (STANOX code) → CORPUS for the name |
| `actual_timestamp` | `1793731860000` | When it happened (see timestamps) |
| `planned_timestamp` | `1793731980000` | Working-timetable time; can be empty for unplanned locations |
| `timetable_variation` | `2` | Whole minutes early/late |
| `variation_status` | `EARLY` | `EARLY`, `ON TIME`, `LATE`, `OFF ROUTE` |
| `toc_id` | `84` | Operator code (needs a TOC lookup; not in CORPUS) |
| `event_source` | `AUTOMATIC` | `AUTOMATIC` (signalling) or `MANUAL` (entered by staff) |
| `correction_ind` | `false` | `true` when this message corrects an earlier report |
| `next_report_stanox` | `87169` | Next timing point expected |
| `train_terminated` | `false` | Last report of the journey |
| `platform`, `line_ind`, `direction_ind` | `" 7"`, `F`, `DOWN` | Infrastructure detail (note the padded platform) |

### Timestamps: the BST trap

All timestamps are milliseconds since the Unix epoch, **but during British Summer Time the following fields are one hour in the future**: `gbtt_timestamp`, `planned_timestamp`, `actual_timestamp`, `canx_timestamp`, `dep_timestamp`, `orig_dep_timestamp`, `original_loc_timestamp`. In effect they hold UK local wall-clock time encoded as if it were UTC.

Parsing rule for RailStream:
1. Read the epoch value as a naive wall-clock time (as if UTC).
2. Interpret that wall-clock time in `Europe/London`.
3. Convert to UTC for storage.

Edge case: at the autumn change (last Sunday of October) 01:00–02:00 local happens twice, so a wall-clock time in that hour is **ambiguous**. The rule for it must be chosen and tested (the BST unit tests). `msg_queue_timestamp` is not on the affected list; to be confirmed against a summer capture.

Observed in November (UK time = UTC), the sample decodes correctly either way (F2), so the rule cannot be validated on live data until 2027-03-28.

### Correctness properties that shape the pipeline

- **Rounding:** TRUST reports at 30-second intervals and rounds to whole minutes; "1 minute late" may be 31 seconds. Small negative lags (event time after receive time) are expected; the observed −219 s is not explained by rounding (F2, open).
- **Late and amended data:** TRUST can be updated "after the fact". A manually input delay can overwrite an earlier automatic report, **possibly days later**. "TRUST should always be historically correct." So the late-data policy must handle corrections, not only late arrivals, and silver cannot treat the first report as final.
- **Duplicates:** at-least-once delivery from STOMP plus feed resends after outages means the same event can arrive twice; silver dedupes on a natural key.

## CORPUS

Location reference data: one JSON object `{"TIPLOCDATA": [...]}`. Observed (F3): 55,981 rows; 11,118 distinct STANOX, 356 of which appear on more than one row.

| Field | Example | Meaning |
|---|---|---|
| `STANOX` | `72410` | Code used by TRUST (`loc_stanox`); the join key |
| `TIPLOC` | `EUSTON` | Code used by SCHEDULE |
| `3ALPHA` | `EUS` | Passenger station code (blank for junctions, yards) |
| `NLC` | `144400` | National Location Code |
| `NLCDESC` | `EUSTON LONDON` | Human-readable name |

Enrichment must pick exactly one row per STANOX (deterministic rule) and is tested to preserve row counts.
