# Benchmarks

This file records measured results only. No number here is an estimate or a target
restated as a fact — every figure below came from an actual run, and the run
conditions are described alongside it so the numbers stay comparable over time.

## Baseline architecture (pre-optimization)

Original design: a separate durable event-ledger Delta table (read + left-anti-join +
append) plus a `delete()` then `append()` write to the SCD2 target — roughly 6
separate Delta/Spark actions per micro-batch.

Local WSL, 8 events, 5s trigger:

```
events=8 last_batch_id=1 committed_rows=8 e2e_latency=100.14s
```

Local WSL, 20 events, 5s trigger (p50/p95/max across the run):

```
p50=15.63s p95=25.15s max=46.09s
```

Stage breakdown showed `target_replace_where`/`target_delete` and
`ledger_read_and_scd2_rebuild` as the dominant per-batch costs (7-13s combined per
batch), consistent with the 6-transaction-per-batch design.

## Optimized architecture (current, `main`)

Replaced the separate event ledger + delete/append pattern with a single read of the
target table as prior state, and one atomic `replaceWhere` write — cutting per-batch
Delta transactions from ~6 to ~3. Removed `.cache()` calls that aren't reliably
supported under Spark Connect/serverless. Added a validated CDC event contract
(`operation`: INSERT/UPDATE/DELETE).

### Local WSL, native filesystem — current 30-run benchmark

The benchmark was rerun from the clean Git working copy using
`benchmarks/measure_e2e_latency.py` with 20 events per run and a 5s processing
trigger.

30 runs produced:

| Metric | Result |
|---|---:|
| Runs | 30 |
| Events per run | 20 |
| Minimum | 1.93s |
| Average | 2.88s |
| p50 | 2.50s |
| p95 | 3.73s |
| Maximum | 12.70s |

The p50/p95 figures above are the current measured local benchmark results.
The 12.70s maximum is retained to show observed tail behavior rather than hiding
the outlier.

The earlier optimized 5-run result is retained below as historical evidence.

### Historical local optimized result

Before the current 30-run benchmark, five optimized local runs were recorded:

| Run | Events | e2e_latency |
|---|---:|---:|
| 1 | 20 | 15.07s |
| 2 | 20 | 14.53s |
| 3 | 20 | 10.23s |
| 4 | 20 | 14.35s |
| 5 | 40 | 6.99s |

Mean ≈ 12.2s. This historical result is retained for comparison with the
pre-optimization baseline; it is not the current benchmark claim.

### Databricks Free Edition (serverless, Spark Connect)

Measured via the per-batch operational audit log (`streaming_audit`), which times
only the actual `_process_micro_batch` processing — not query/session startup. This
is the correct metric for comparing against the local per-batch numbers above, since
wall-clock timing around `.start()`/`awaitTermination()` in a test notebook also
includes query-initialization cost that does not repeat per batch in a real
long-running or scheduled job.

5 batches, 20 events each, `spark.sql.shuffle.partitions=4` (default of 200 caused
each empty/small batch to spin up 200 RocksDB state store instances, adding ~13s per
batch — set this explicitly on Databricks; the local session already does this via
`spark_session.py`):

| batch_id | duration_seconds | input_rows | impacted_accounts |
|---|---|---|---|
| 0 | 8.85 | 20 | 10 |
| 2 | 11.46 | 20 | 10 |
| 0 (rerun, fresh checkpoint) | 8.18 | 20 | 10 |
| 1 | 7.93 | 20 | 10 |
| 2 | 8.84 | 20 | 10 |

Mean ≈ 9.05s, range 7.93s-11.46s. All 5 batches: zero data-quality rejections
(`dead_letter` empty), correct SCD2 output (10/10 accounts, exactly 1 current row
each).

### Current Databricks validation run

A fresh controlled validation was run on Databricks Free Edition / Serverless with
`spark.sql.shuffle.partitions=4`, a fresh checkpoint, and a fresh 20-event input
file. The operational audit recorded:

| Metric | Result |
|---|---:|
| Input events | 20 |
| New events | 20 |
| Impacted accounts | 20 |
| History rows | 20 |
| Current rows | 20 |
| Status | SUCCESS |
| SCD2 processing time | 9.272s |

The corresponding validation produced 40 total target rows and 40 current rows after
the earlier 20-account validation run plus this second run using 20 new account IDs.
Quarantine remained empty.
A separate initial validation with `spark.sql.shuffle.partitions=auto` measured
47.121s. That result is retained as an observed configuration-specific run and is
not combined with the controlled `shuffle.partitions=4` benchmark.

**Known artifact, not a production number:** repeatedly calling `.start()` on the same
checkpoint within one interactive test session (as opposed to a job that starts once
and runs continuously, or is triggered once per schedule) adds session/query
initialization overhead on top of `duration_seconds` — observed wall-clock figures in
that scenario ranged 24-44s for the same batches shown above. That gap is testing
methodology, not pipeline performance, and should not be quoted as the pipeline's
latency.

## Reproducing these numbers

Local:
```bash
PYTHONPATH=src SPARK_LOCAL_IP=127.0.0.1 python benchmarks/measure_e2e_latency.py --events 20 --trigger "5 seconds" --runs 30
```

Databricks: run `notebooks/01_streaming_scd2_job.py` (or an equivalent cell) against
a Unity Catalog Volume, then read the per-batch `duration_seconds` from the audit
Volume path (`<base_path>/streaming_audit`), not wall-clock time around the query
start/stop calls.
