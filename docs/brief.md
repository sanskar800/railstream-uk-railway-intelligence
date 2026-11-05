# Brief: near-real-time delay visibility for route control

*Version 1, 2026-11-05. Written as if for a real customer; RailStream is an unofficial, independent project.*

## Who it is for

A **route control manager** (and the duty controllers on their shift) responsible for train running across a region. They manage disruption as it happens: deciding where to send staff, which services to hold or re-plan, and what to tell operators. Today they piece the picture together from several screens and phone calls; they want **one trustworthy view of where delay is happening now and how it is spreading**.

## The questions they ask (in their words)

| # | Question | Answered by |
|---|---|---|
| Q1 | "Which three locations caused the most reactionary delay this morning?" | `gold.reactionary_delay` |
| Q2 | "Where are trains losing time right now?" | `gold.delay_hotspots` (last hour) |
| Q3 | "How punctual is each operator today, and when did it go wrong?" | `gold.punctuality_hourly` |
| Q4 | "How many trains have been cancelled, and where?" | cancellations in silver → gold |
| Q5 | "Can I trust what I'm looking at right now?" | data-health view: freshness, feed gaps, rejected messages |

Q5 is not an afterthought: a controller acting on a stale screen is worse off than one with no screen. Every view shows how fresh its data is.

## Freshness

- **Target:** an event that happens on the railway appears in the gold tables within **5 minutes at p95**. *(Target, not a measured result; measured on Day 13.)*
- **Why minutes, not seconds:** TRUST itself delivers events about 23 s after they happen at the median and 92 s at p95 (findings F2), and rounds to whole minutes. A seconds-level target would be fiction.
- **Why minutes, not hours:** decisions about holding or re-routing trains are made within the hour; an hourly batch would arrive after the decision.

## Constraints

- **The feed is unreliable:** it goes quiet (weekly outage, network problems), redelivers duplicates, delivers some events late and amends records days later. The system must keep running, say when data is missing, and correct itself.
- **TRUST reports facts, not causes.** It says where a train gained delay, not why. Reactionary delay is an *inference* (definition in metrics.md) and is labelled as such.
- **£0 budget, one laptop** (16 GB RAM, ~7.6 GiB for Docker).

## Out of scope

- Official performance statistics or delay attribution (that is Network Rail's process; we compare against it, we do not replace it).
- Predictions of future delay.
- Passenger information.
- Network Rail branding or any claim to be official.

## What "useful" means (acceptance)

1. Q1–Q5 can be answered from the dashboard or API without writing SQL.
2. Freshness and feed gaps are always visible next to the numbers.
3. A disconnection, a malformed message or a crashed job never silently changes the numbers: it is either recovered from or shown.

## Iteration log

Changes to this brief after seeing the first dashboard are recorded here (spec: "what the first dashboard got wrong").

- v1 (2026-11-05): initial brief.
