# Real-Time Banking Account Streaming Pipeline: SCD Type 2

A reference implementation of a Spark Structured Streaming pipeline that tracks banking account state changes as effective-dated SCD Type 2 history in Delta Lake. It runs locally for development and on Azure Databricks (serverless, Unity Catalog Volumes) for deployment.

Events arrive as JSON files and are processed as micro-batches. Each batch is validated against a CDC event contract, bad rows are quarantined, and the history of every affected account is rebuilt deterministically and written to the SCD2 target in one Delta transaction.

> **Status:** working reference project. Performance figures below are measured; the [Known limitations](#known-limitations) section lists what the current design does not yet guarantee.

## Business use case

A banking platform receives a continuous stream of account-state changes:

```
event_id | account_id | operation | account_status | account_tier | branch_region
e1       | ACC-00001  | INSERT    | ACTIVE         | STANDARD     | SOUTH
e2       | ACC-00001  | UPDATE    | SUSPENDED      | STANDARD     | SOUTH
e3       | ACC-00001  | UPDATE    | ACTIVE         | PREMIUM      | SOUTH
```

SCD Type 2 keeps the full history of each account:

```
account_id | status    | tier     | effective_from | effective_to | is_current
ACC-00001  | ACTIVE    | STANDARD | 10:00          | 10:05        | false
ACC-00001  | SUSPENDED | STANDARD | 10:05          | 10:12        | false
ACC-00001  | ACTIVE    | PREMIUM  | 10:12          | null         | true
```

## Architecture

```
  JSON event files (landing)
            |
            v
  Structured Streaming readStream (CDC_EVENT_SCHEMA, maxFilesPerTrigger)
            |
            v
  withWatermark(event_time) + dropDuplicates(event_id)
            |
            v
        foreachBatch
            |
   +--------+-------------------------------+
   |                                        |
   v                                        v
 CDC contract + schema guard         valid events
   |                                        |
   v                                        v
 quarantine (Delta)               apply_scd2_batch
                                            |
                          +-----------------+------------------+
                          |                 |                  |
                    read target slice   rebuild history   atomic write
                    for impacted        (window functions) (append or
                    accounts                               replaceWhere)
                                            |
                                            v
                                  operational audit append
```

## What happens in each micro-batch

1. **Validate.** `validate_cdc_events` / `quarantine_cdc_events` (in `cdc/event_contract.py`) enforce the canonical CDC contract, including `operation` in `INSERT | UPDATE | DELETE`. Rows that fail go to the quarantine Delta table.
2. **Schema guard.** `split_valid_and_quarantined` checks critical column types and quarantines on incompatible type drift.
3. **De-duplicate.** The batch is de-duplicated on `event_id`.
4. **Find impacted accounts.** The distinct `account_id` values in the batch.
5. **Read prior state.** Existing history rows for the impacted accounts are read from the SCD2 target. Events whose `event_id` is already stored as a `source_event_id` are dropped as replays. If nothing is new, the batch is skipped.
6. **Rebuild.** Prior events and new events are combined and the history of each impacted account is recomputed, ordered by `event_time, event_id`.
7. **Write.** If the target does not exist, the rebuilt rows are appended. Otherwise they replace the impacted accounts' rows with `replaceWhere` in a single Delta transaction.
8. **Audit.** One operational audit record per batch (input rows, new rows, impacted accounts, history rows, current rows, duration).

## State mechanisms

| Mechanism | Purpose |
|---|---|
| Spark checkpoint | Streaming progress and restart recovery |
| Stream-level `dropDuplicates(event_id)` with `withWatermark` | Drop duplicate events inside the stream |
| Target table as prior state | Events already stored (`source_event_id`) are skipped on replay, and the rebuild is deterministic, so a replayed batch produces the same result |

## SCD2 design

- A new version is created when `account_status`, `account_tier` or `branch_region` changes (null-safe comparison) or when an account's first event arrives. Events that change nothing do not create versions.
- Versions are ordered by `event_time, event_id`. `effective_to` is the next version's `event_time`; the latest version has `effective_to = null` and `is_current = true`.
- `version_id` is `sha2(account_id || event_id)`.
- `DELETE` events take part in the ordering and in change detection but are not written as versions. The version before a `DELETE` is closed and is not current.
- Events within a batch that touch the same account are all applied, so several changes to one account in one micro-batch produce several versions and exactly one current row.
- Late events for an account are placed in the correct position because the account's history is rebuilt from its stored versions plus the new events.

Target columns: `version_id, account_id, source_event_id, event_time, account_status, account_tier, branch_region, effective_from, effective_to, is_current`.

## Data quality and quarantine

- Contract violations and critical-type drift are written to the dead-letter Delta table (`mergeSchema=true`), with a `quarantine_reason`.
- The stream reads with a fixed schema (`CDC_EVENT_SCHEMA`), so mistyped values are handled by the reader and the contract checks rather than by a schema change at the source.
- `customer_segment` is accepted as an optional field in the event contract.

## Execution modes

| Environment | Trigger | Why |
|---|---|---|
| Local | `processingTime` (default `5 seconds`) | Continuous streaming for development and benchmarking |
| Databricks | `availableNow` | Selected automatically when `DATABRICKS_RUNTIME_VERSION` is set. Serverless compute supports this incremental trigger, so the job drains the available files and exits |

On Databricks the job is therefore run on a schedule or by ADF, not as an always-on query, and end-to-end latency includes the schedule interval.

## Local run

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Terminal 1, event simulator:

```bash
PYTHONPATH=src python src/data_generator/event_stream_simulator.py \
  --events-per-second 5 --late-event-rate 0.10 --duration-seconds 60
```

Terminal 2, streaming query:

```bash
PYTHONPATH=src python src/jobs/run_streaming_job.py
```

Tests:

```bash
PYTHONPATH=src pytest -v
```

## Databricks deployment

`notebooks/01_streaming_scd2_job.py` is a thin wrapper over the same `src/` modules. It reads these widgets and creates the Volume directories:

| Widget | Default |
|---|---|
| `stream_source_path` | `/Volumes/workspace/default/streaming_scd2/landing` |
| `target_table_path` | `/Volumes/workspace/default/streaming_scd2/account_state_scd2` |
| `checkpoint_path` | `/Volumes/workspace/default/streaming_scd2/checkpoint` |
| `dead_letter_path` | `/Volumes/workspace/default/streaming_scd2/quarantine` |
| `watermark_duration` | `10 minutes` |

Notes for serverless / Spark Connect:

- Set `spark.sql.shuffle.partitions` explicitly (the benchmarks used `4`). The default of 200 created 200 state-store instances per batch and added about 13 seconds per batch.
- `DataFrame.cache()` / `persist()` are not reliably supported, so the code avoids them.
- Tables are Delta paths inside a Unity Catalog Volume. Unity Catalog grants therefore apply at the Volume level; table-level grants and column masks require registering UC tables (see the roadmap).

ADF (`adf/`) is used to start and monitor the Databricks Job (`databricks_job_definition.json`, `pipeline_start_streaming_job.json`). The Databricks Job owns retry behavior.

## Measured performance

Measured results only. Details, run conditions and raw output are in `benchmarks/`.

**Local (WSL, native filesystem).** `measure_e2e_latency.py`: 30 runs, 20 events per run, 5-second trigger, file write to first committed micro-batch, each run in a fresh session and an empty target.

| Metric | Result |
|---|---:|
| p50 | 2.50 s |
| p95 | 3.73 s |
| Min / average / max | 1.93 s / 2.88 s / 12.70 s |

**Databricks Free Edition (serverless).** Per-batch SCD2 processing time from the operational audit log, 20 events per batch, `shuffle.partitions=4`: mean about 9 s (range 7.9 to 11.5 s over 5 batches). This excludes query start-up and the schedule interval.

How to read these numbers:

- The local run is a cold-start burst into an empty target. It exercises the append path only, not history rebuilds, state changes or late events, so it is not a steady-state latency.
- The local figure contains the wait for the next trigger and first-batch overhead. It is not the same quantity as the Databricks per-batch processing time.
- The benchmark script runs locally only. On Databricks the `availableNow` trigger ends the query before the benchmark can write its file.

## Known limitations

These describe the current code, not intended behavior. Remove an item when it is fixed and covered by a test.

1. **History is rebuilt from the target table only.** The target stores state-changing versions, not every event. A late event that arrives after a non-changing event, or around a `DELETE`, can therefore produce a wrong current state or lose the deleted interval. A durable event ledger (every event, including `DELETE`) would remove this; an earlier version of the project had one, and it was removed to reduce Delta transactions.
2. **Stream-level de-duplication runs before validation.** `dropDuplicates(event_id)` happens before quarantine, so rows with a null `event_id` collapse into one, and a corrected resend of a quarantined event can be dropped as a duplicate. The watermark only bounds state if the de-duplication key includes the event-time column; with `event_id` alone, state growth should be checked in `lastProgress`.
3. **Quarantine writes are plain appends.** A replayed batch can append the same rejected rows twice.
4. **Schema-drift detection is limited on the streaming path.** The stream reads with a fixed schema, so the type check in `schema_evolution.py` only fires for DataFrames that already carry a different schema. The CDC contract's null and value checks are the effective gate.
5. **`customer_segment` is not propagated to the SCD2 target.** It is accepted in the event contract but is not selected into the rebuilt history.
6. **Source is files.** The event source is a JSON landing folder, not Kafka or Event Hubs. The processing layer is source-agnostic, but the ingest function would change.
7. **Local and Databricks entrypoints are separate.** The notebook rebuilds the stream definition instead of calling `build_and_start_stream`, and the trigger is selected by an environment variable.
8. **Single writer.** The design assumes one running writer per target.

## Roadmap

- Restore a durable event ledger (one idempotent `MERGE` by `event_id`) and rebuild history from it.
- Move validation before stream-level de-duplication, or replace it with `dropDuplicatesWithinWatermark`.
- Make quarantine writes idempotent (`txnAppId` / `txnVersion`) and write once per batch.
- Parameterize the trigger and call `build_and_start_stream` from the notebook.
- Register the SCD2 target and quarantine as Unity Catalog tables.
- Add a Kafka or Event Hubs source option.
- Extend the benchmark with pre-loaded history, state changes and late events.

## Project structure

```
src/
  cdc/
    event_contract.py          # CDC event schema, validation and quarantine
  data_generator/
    event_stream_simulator.py
  jobs/
    run_streaming_job.py
  pipeline/
    config.py
    logger.py
    scd2_merge.py              # apply_scd2_batch, history rebuild
    schema_evolution.py        # critical-type guard
    spark_session.py
    streaming_job.py           # build_and_start_stream, micro-batch handler
  streaming_framework/
    audit.py  checkpoint.py  context.py  metrics.py
benchmarks/
  measure_e2e_latency.py
tests/
  test_checkpoint_recovery.py
  test_latency.py
  test_scd2_merge.py
  test_schema_evolution.py
notebooks/
  01_streaming_scd2_job.py
adf/
  databricks_job_definition.json
  pipeline_start_streaming_job.json
docs/
  architecture.md
```

## Stack

Python, PySpark, Spark Structured Streaming, Delta Lake, pytest, Azure Databricks, Azure Data Factory, Unity Catalog Volumes.

## Companion projects

- `increment_batch_pipeline`: metadata-driven incremental snapshot processing
- `micro-batch-pipeline`: idempotent batch, replay and recovery processing
- this repository: Structured Streaming with banking SCD Type 2
