# Day 6: bounded state and CORPUS verification

Development correctness evidence, not capacity or latency benchmarks.

`summary.json` is extracted from the `SILVER_STATE_SCENARIO` JSON printed by:

```powershell
$env:DOCKER_CONTEXT = 'desktop-linux'
uv run pytest -m integration -s tests/integration/test_silver.py
```

The state scenario uses a synthetic ten-second horizon and controlled batch-time
jumps to exercise the actual Spark watermark and state store. Production uses
900 seconds and Spark's own batch timestamp. Both focused scenarios passed;
they retain their isolated tables, topic and checkpoints, with no drops or kills.
The original sink scenario also verified correction precedence and reject retry.

`real-silver.json` is a read-only Spark inspection after the v2 query reread the
existing bronze table. It records row/key counts, enrichment coverage and station
samples. It is a snapshot of this development archive, not an estimate for all
Network Rail data. Source batch status counts were 107,222 movements and 4,212
other valid TRUST messages; route counts were 54,424 replay, 21 correction and
52,777 late movements. This old backlog does not exercise steady live state;
the synthetic scenario supplies the state correctness evidence.

`after-restart.json` repeats the same read-only inspection after recreating only
silver with its 2 GiB limit. Current, distinct-key, revision and distinct-revision
counts were unchanged. The existing v2 checkpoint was reused; v1 was preserved.

`memory-samples.json` holds timestamped Docker stats samples during and after the
reread. The maximum sampled display value was 1.462 GiB, with no OOM or automatic
restart observed. Sampling can miss a higher instantaneous peak. The original
1.5 GiB cap was therefore raised to 2 GiB; heap remains 768 MiB, with two shuffle
partitions and disk-backed materialization. This is not evidence that every
backlog fits: larger-history batch sizing remains a deployment concern.

Spark reports summed scan input for the live/bypass union. Its progress count
222,868 is twice the 111,434 physical bronze records in this batch; do not call
that unique-message throughput. The sink's status/route counts are the relevant
business input counts.
