# ADR-014: One freshness metric and two Oracle alarms, measured from outside the capture VM

- **Status:** Accepted
- **Date:** 2026-11-17

## Context

The capture VM is the data single point of failure (ADR-012): the feed keeps 5 minutes, so an unnoticed capture failure is permanent loss. The bridge already logs `FEED QUIET` and `DATA GAP`, but nobody is notified, and a failed VM cannot report its own failure. Spec Day 12 plans Prometheus and Grafana for pipeline internals; detection of a dead ingestion path was needed now.

## Options considered

1. **One alarm per machine** (instance health): misses a crashed bridge on a healthy VM, a blocked private network, or an upstream feed outage.
2. **One end-to-end freshness metric, measured on the processing VM** (chosen): `feed_age_seconds` = seconds since the newest record reached `raw.trust.movement`.
3. **Airflow email on a failed freshness task:** needs SMTP credentials and depends on Airflow being up.

## Decision

`ops/feed_age.py`, run every minute by a systemd timer on the processing VM, reads the newest record time across the topic's partitions and publishes `feed_age_seconds` (dimension `topic`) to Oracle Monitoring namespace `railstream`, authenticating as the instance (dynamic group `railstream-core`, policy allowing only that namespace). If Kafka cannot be read, nothing is published. Two alarms email the Notifications topic `railstream-alerts`:

- **railstream feed stale:** max > 300 s for 3 minutes (capture VM, bridge, network, publishing or the feed itself). **Changed 2026-11-26** to `feed_age_seconds[2m].max() > 120` with a 1-minute trigger delay (findings F36): the 1-minute series had holes, so a 9-minute outage never fired, and ~8 minutes to alert was longer than the broker's 5-minute hold.
- **railstream monitoring silent:** `feed_age_seconds[1m]{topic = "raw.trust.movement"}.absent()` for 5 minutes (the processing VM or the job is down).

## Consequences

- Verified 2026-11-17: pausing the job made the absence query read 1 from 16:18 and the alarm fire; the first FIRING email was dropped because the email subscription was still pending, which is why alerting is tested end to end; after confirming it, the recovery (OK) email arrived. A one-minute late datapoint (16:15) did not alarm, as the 5-minute delay intends.
- The Sunday ~02:00 upstream outage will alarm every week: correct (data is missing), and documented in the alarm body.
- **Gap found the same day (findings F25):** the metric watches Kafka, not the lakehouse. When SeaweedFS was down for ~10 minutes, Kafka stayed fresh and no alarm fired while bronze and silver crash-looped. Bronze/silver freshness is added to the Day 12 monitoring scope.
- Free tier: custom metrics, alarms and up to 1,000 email notifications a month.

## Update 2026-11-26

Tested end to end with deliberate outages (findings F36). With the original definition a 9-minute capture outage did not alarm: the publisher runs once a minute but its start drifts by seconds, so a 1-minute window sometimes held two samples and the next none, and one empty minute reset the 3-minute pending count. The alarm is now `feed_age_seconds[2m].max() > 120`, trigger delay 1 minute: a 4-minute outage produced the FIRING and the OK emails. It fires after about 3-4 minutes of silence, inside the broker's 5-minute hold, so a person or a restart can still act before data is lost.
