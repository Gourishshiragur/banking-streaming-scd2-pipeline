# Real-Time Banking Account Streaming Pipeline — SCD Type 2

A production-style **Spark Structured Streaming** pipeline for banking/fintech account-state tracking. Continuous account events are processed as Spark micro-batches, protected by an event-time watermark and checkpoint recovery, and maintained as effective-dated SCD Type 2 history in Delta Lake.

This project extends the reusable operational patterns from the companion Incremental Batch and Micro-Batch projects into a true streaming execution model rather than duplicating their batch runners.

## Business use case

A banking platform receives continuous customer-account state changes:

```text
account_id | account_status | account_tier | branch_region
ACC-00001  | ACTIVE         | STANDARD    | SOUTH
ACC-00001  | SUSPENDED      | STANDARD    | SOUTH
ACC-00001  | ACTIVE         | PREMIUM     | SOUTH
```

SCD Type 2 preserves the complete account-state history:

```text
account_id | status    | tier     | effective_from | effective_to | is_current
ACC-00001  | ACTIVE    | STANDARD | 10:00          | 10:05        | false
ACC-00001  | SUSPENDED | STANDARD | 10:05          | 10:12        | false
ACC-00001  | ACTIVE    | PREMIUM  | 10:12          | null         | true
```

## Architecture

```text
             Banking account events
                       │
                       ▼
              JSON/file event source
                       │
                       ▼
          Spark Structured Streaming
                       │
          ┌────────────┼────────────┐
          │            │            │
      watermark    checkpoint    dedup
          │            │            │
          └────────────┼────────────┘
                       ▼
                 DQ / quarantine
                       │
                       ▼
                     Bronze
                       │
                       ▼
                 foreachBatch
                       │
                       ▼
               durable event ledger
                       │
                       ▼
               deterministic SCD2
                       │
                       ▼
             Delta account history
                       │
                       ▼
             audit / reconciliation
```

## Resume-claim acceptance criteria

The project is deliberately built around measurable evidence for the intended resume bullets.

### Streaming latency and recovery

Target claim:

> Built Spark Structured Streaming pipelines with event-time watermarking and checkpoint recovery, achieving measured ~5–10 second end-to-end latency with replay-safe/idempotent Delta processing.

The five-second trigger is a configuration target. **The repository does not hard-code a 5–10 second result as a fact.** Run:

```bash
PYTHONPATH=src python benchmarks/measure_e2e_latency.py --events 20 --trigger "5 seconds"
```

The benchmark reports p50/p95/max event-file-to-Delta-commit latency. Only use the measured result on the resume.

### SCD Type 2 and schema evolution

Target claim:

> Implemented full SCD Type 2 history tracking with effective dating, handling late/out-of-order events and additive schema evolution while quarantining incompatible schema drift.

The test suite covers:

- new account creation
- attribute changes
- multiple changes for the same account in one micro-batch
- late-arriving events inserted into effective history
- replayed micro-batches
- additive `customer_segment` schema evolution
- incompatible critical-field type quarantine

## Reusable framework boundary

The streaming project reuses the strongest common patterns from the Incremental Batch framework:

- configuration and metadata concepts
- operational control/audit patterns
- data-quality validation
- reconciliation
- idempotent Delta writing
- per-micro-batch audit metrics
- structured logging

It does **not** treat the batch control state as a Spark streaming watermark.

```text
Reusable operational framework
       │
       ├── configuration / metadata
       ├── audit / control
       ├── DQ
       ├── reconciliation
       └── idempotent Delta write
                    │
                    ▼
             Streaming adapter
                    │
       ┌────────────┼────────────┐
       ▼            ▼            ▼
  checkpoint   event-time     foreachBatch
                watermark          │
                                   ▼
                                SCD2
```

## Three different state mechanisms

| Mechanism | Purpose |
|---|---|
| Spark checkpoint | Streaming progress and restart recovery |
| `withWatermark()` | Event-time lateness and bounded streaming state |
| Event ledger | Durable application-level event history keyed by `event_id` |

Keeping these responsibilities separate makes the pipeline easier to reason about and safer to replay.

## SCD2 design

The event ledger is merged by `event_id`, so a replayed event is not duplicated. For each affected account, history is rebuilt deterministically from the durable ledger ordered by `event_time, event_id`. Only state-changing events create SCD2 versions.

This specifically protects the difficult case where one account changes several times inside the same Spark micro-batch:

```text
10:00 ACTIVE / STANDARD
10:01 SUSPENDED / STANDARD
10:02 ACTIVE / PREMIUM
```

The result contains three historical versions and exactly one `is_current = true` row.

A late event that arrives within the configured watermark is inserted into its correct effective-date position and causes subsequent versions to be re-derived.

## Schema evolution

Additive banking attributes are supported through Delta `mergeSchema=true`. `customer_segment` is included as an optional evolved field. The reusable schema guard quarantines incompatible types for critical columns.

For Databricks deployments where automatic source-schema discovery is required, the same processing layer can be connected to Auto Loader; the local implementation keeps the source deterministic for testing.

## Local run

Create a virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Terminal 1 — start the event simulator:

```bash
PYTHONPATH=src python src/data_generator/event_stream_simulator.py \
  --events-per-second 5 \
  --late-event-rate 0.10 \
  --duration-seconds 60
```

Terminal 2 — start the streaming query:

```bash
PYTHONPATH=src python src/jobs/run_streaming_job.py
```

Run tests:

```bash
PYTHONPATH=src pytest -v
```

Run the end-to-end latency benchmark separately (do not treat the pytest suite as the performance benchmark):

```bash
PYTHONPATH=src python benchmarks/measure_e2e_latency.py --events 20 --trigger "5 seconds"
```

The benchmark reports the measured p50/p95/max event-file-to-Delta-commit latency. The resume latency number must come from this output.

## Databricks / Unity Catalog deployment

The notebook is intentionally thin and imports the same tested `src/pipeline` modules. It uses the active Databricks SparkSession and Unity Catalog Volume paths. The Databricks runtime supplies Spark and Delta; the notebook does not install the local `delta-spark` wheel.

Default example paths:

```text
/Volumes/main/default/streaming_scd2/landing
/Volumes/main/default/streaming_scd2/account_state_scd2
/Volumes/main/default/streaming_scd2/account_event_ledger
/Volumes/main/default/streaming_scd2/checkpoint
/Volumes/main/default/streaming_scd2/quarantine
```

Databricks Jobs owns restart/retry of the long-running streaming process. ADF is used as an orchestration/monitoring layer rather than as the streaming engine.

## Project structure

```text
src/
  data_generator/
    event_stream_simulator.py
  jobs/
    run_streaming_job.py
  pipeline/
    config.py
    audit.py
    logger.py
    scd2_merge.py
    schema_evolution.py
    spark_session.py
    streaming_job.py
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

Python · PySpark · Spark Structured Streaming · Delta Lake · pytest · Azure Databricks · ADF · Unity Catalog Volumes

---

Companion projects:

- `increment_batch_pipeline` — metadata-driven incremental snapshot processing
- `micro-batch-pipeline` — idempotent batch/replay/recovery processing
- this repository — continuous Structured Streaming + banking SCD Type 2
