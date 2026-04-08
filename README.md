# Slotwise MVP

Phase 1 implementation for the demand-aware pricing engine MVP includes:

- repository scaffolding for wrapper, pipeline, models, optimizer, app, config, data, tests
- deterministic config loading and run identity model
- synthetic Medscheduler wrapper extraction + normalization with stable IDs
- DuckDB bootstrap for core/supporting tables
- idempotent load stage for normalized entities
- canonical availability window derivation (`visible_at`, `unavailable_at`, `current_status`)
- phase-one tests for determinism, idempotency, bootstrap, and end-to-end run

## Slot status assumptions (MVP)

The Phase 1 availability stage assumes the following status semantics:

- `removed`: slot has a `removed` lifecycle event and is no longer bookable
- `booked`: latest lifecycle event for the slot is `booked`
- `canceled`: latest lifecycle event for the slot is `canceled` (slot may be reopened later in future phases)
- `expired`: no lifecycle event overrides status and `slot_start_at` is in the past
- `open`: no lifecycle event overrides status and `slot_start_at` is in the future

`unavailable_at` is computed as the earliest of `booked`, `removed`, or `slot_start_at`.

## Run pipeline

```bash
python -m pipeline.run_pipeline
```

## Run tests

```bash
pytest
```
