# Architecture Notes

## Banking domain

The stream represents customer-account state changes rather than telemetry. The business key is
`account_id`; tracked SCD2 attributes are `account_status`, `account_tier`, and `branch_region`.

Example lifecycle:

```text
ACTIVE / STANDARD / SOUTH
        ↓
SUSPENDED / STANDARD / SOUTH
        ↓
ACTIVE / PREMIUM / SOUTH
```

## Structured Streaming and micro-batches

Spark Structured Streaming continuously discovers new event files and processes them as
micro-batches. `foreachBatch` hands each micro-batch to the tested SCD2 engine. A five-second
trigger is the target configuration; actual end-to-end latency is measured by
`benchmarks/measure_e2e_latency.py` and should not be claimed until the benchmark is run.

## Three state mechanisms

These mechanisms are deliberately not conflated:

1. **Checkpoint** — Spark's recovery/progress state for the streaming query.
2. **Event-time watermark** — bounds streaming state and determines how late an event can be
   considered by the streaming engine.
3. **Event ledger** — application-level durable event history keyed by `event_id`. It makes the
   `foreachBatch` SCD2 sink replay-safe even when a batch is redelivered after a failure.

This separates Spark execution semantics from reusable operational/control semantics inherited
from the broader pipeline framework.

## Idempotent SCD2 design

The event ledger is merged by `event_id`. For each affected account, the SCD2 history is rebuilt
from the durable event ledger in deterministic `event_time, event_id` order. Only state-changing
events create SCD2 versions. `effective_to` is the next state-change event time and exactly one
row per account is current.

This design explicitly handles the difficult case where the same account changes multiple times
inside one micro-batch, and it also handles an event that arrives late but still falls within the
streaming watermark.

## Schema evolution

Additive fields are allowed by the Delta writer (`mergeSchema=true`). The stream schema includes
an optional `customer_segment` field to exercise an additive banking attribute. The schema guard
also provides a reusable validation function that quarantines incompatible types for critical
fields. In Databricks, Auto Loader can be substituted as the source if automatic schema discovery
is required.

## Reusable framework boundary

The project reuses/adapts the operational patterns from the Incremental Batch framework:
configuration, audit/control concepts, DQ, reconciliation and idempotent Delta writes. It does
not reuse the batch watermark as a Structured Streaming watermark.

```text
Reusable framework
        ↓
metadata / config / audit / DQ / reconciliation / idempotent write
        ↓
Streaming adapter
        ↓
checkpoint + event-time watermark + foreachBatch
        ↓
Banking SCD Type 2
```

## Databricks execution

The Databricks notebook uses Unity Catalog Volume paths and the active Databricks SparkSession.
Databricks Jobs owns restart/retry semantics for the long-running query; the checkpoint provides
stream progress recovery. ADF is an orchestration/monitoring layer rather than the streaming
engine.
