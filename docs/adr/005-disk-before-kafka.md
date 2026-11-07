# ADR-005: The live bridge writes to disk before acknowledging, then publishes to Kafka

- **Status:** Accepted
- **Date:** 2026-11-07

## Context

The STOMP broker keeps unacknowledged and missed messages for 5 minutes only (findings F11), and redelivers unacknowledged messages only after a reconnect. A bridge must therefore acknowledge a frame only once it sits in a store that outlives the broker's copy. On the laptop, Kafka is a single Redpanda broker that is stopped with `make down`, restarted by Docker Desktop, and paused when the laptop sleeps: the least reliable component in the chain. The capture VM (1 GB RAM) cannot run Kafka, and a laptop Kafka must not be exposed to the internet for the VM to reach it.

## Options considered

1. **STOMP → disk → ACK → Kafka (asynchronous).** The ACK depends only on a local fsync. Kafka failures are counted and logged with the receive-time window of the records it did not take; that window is refilled from disk by the replay tool. Costs a second copy of the feed on the laptop (~40 MB/day compressed).
2. **STOMP → Kafka, ACK on delivery.** No disk copy. But the broker's ACKs then depend on the laptop Kafka: during an outage messages stay unacknowledged and are redelivered only after the bridge disconnects and resubscribes (extra failure handling), and an outage over 5 minutes loses them until the VM's archive hour is complete and pulled.
3. **The VM publishes to Kafka.** Rejected: Kafka would have to be reachable from the internet, or run on a 1 GB VM.

## Decision

Option 1, while Kafka runs on the laptop. The rule behind it: **acknowledge only once the data is in the most reliable store we control.** Today that is the local disk.

## Consequences

- Kafka is a derived, replayable stream; the raw files are the source of truth. A Kafka outage never stops capture. Verified: a 41 s Redpanda outage during a live run lost nothing and duplicated nothing (findings F12).
- Two copies exist on the laptop side (the VM archive and `data/raw/trust-live`); the VM archive remains canonical.
- Bridge → Kafka is at least once: a STOMP redelivery after a reconnect, or a replay over an already-published window, publishes a message again. Silver dedupes (metrics.md §1).
- **Cloud evolution:** if the whole stack is hosted with an always-on, replicated Kafka, Kafka becomes the most reliable store and the rule selects option 2: one bridge, acknowledging on Kafka delivery; the bronze Iceberg table (raw payload) becomes the long-term archive, and the disk step, the VM archive and `make pull-raw` go away. Downstream of Kafka nothing changes. The listener already takes the writer and the Kafka sink as separate parts, so this is a new acknowledgement mode, not a rewrite.
- Wrong if: the laptop's disk becomes a bottleneck for the ACK (fsync per frame; ~30 frames/min, so not expected), or Kafka failures go unnoticed because the `NOT IN KAFKA` warning is not monitored (Day 12 alerting).
