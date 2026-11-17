# ADR-013: Hourly off-VM backup of the raw archive to Object Storage, append-only

- **Status:** Accepted
- **Date:** 2026-11-17

## Context

The raw archive on the capture VM is the source of truth: the feed keeps only 5 minutes, and every derived table can be rebuilt from it (ADR-012). Its only other copy was the laptop's, refreshed by hand (`make pull-raw`), so losing the capture VM's boot volume (or the VM itself) would lose history. Single-point-of-failure review (2026-11-17), in priority order: off-VM archive backup; alerting from outside the capture VM; an independent second collector; processing redundancy last (its data is derived and rebuildable). A backup protects past data only; it does not prevent future ingestion gaps.

## Options considered

1. **Oracle Object Storage bucket** (Always Free: 10 GB Standard during the trial, 20 GB after; the archive is ~0.4 GB and grows ~1.1 GB/month), written by the capture VM.
2. **Boot-volume backups** (5 free): restore the whole VM, but coarse (snapshots, not continuous) and they would count against the free backup quota for both VMs.
3. **Laptop copy only**: manual and irregular.

## Decision

Option 1. A systemd timer on the capture VM (`infra/capture/railstream-backup.{service,timer}`, `install-backup.sh`) runs `backup-archive.sh` hourly at :15: rclone (v1.75.2, checksum-verified) copies completed hour files (`hour=*.jsonl.gz`, never the open hour) to `railstream-raw-archive/trust/` and then verifies every completed local hour is present with the same size.

- **No stored credentials:** the VM authenticates as itself (instance principal, dynamic group `railstream-capture`).
- **Append-only:** policy `railstream-archive-backup` grants only `OBJECT_INSPECT`, `OBJECT_READ` and `OBJECT_CREATE` on this one bucket, so a compromised VM cannot delete or overwrite backups. Copies use `--ignore-existing` (hour files are immutable once compressed).

## Consequences

- At most about one hour of history is at risk if the capture VM's disk is lost. Measured 2026-11-17: first run 288 hours / 397.5 MiB in 19 s; re-run copied 0 files; a delete attempt was refused (404) and the object remained; a restored hour was byte-identical and held 52,386 messages (findings F24).
- Deleting backups needs a person in the console; old objects can move to Infrequent Access or Archive tiers when the free 20 GB nears (around 1.5 years at current growth).
- A failing backup is visible only in the VM's journal until alerting exists (next item of the review).
- Not addressed here: live-capture continuity (a second collector) and alerting.
