# Slotwise MVP

Phase 1 implementation for the demand-aware pricing engine MVP includes:

- repository scaffolding for wrapper, pipeline, models, optimizer, app, config, data, tests
- deterministic config loading and run identity model
- synthetic Medscheduler wrapper extraction + normalization with stable IDs
- DuckDB bootstrap for core/supporting tables
- idempotent load stage for normalized entities
- canonical availability window derivation (`visible_at`, `unavailable_at`, `current_status`)
- phase-one tests for determinism, idempotency, bootstrap, and end-to-end run

## Run pipeline

```bash
python -m pipeline.run_pipeline
```

## Run tests

```bash
pytest
```
