# Slotwise MVP

Slotwise is a simulation-first MVP for a demand-aware pricing engine focused on underbooked appointment slots.

The current codebase implements the Phase 1 data foundation:

- deterministic synthetic extraction via the Medscheduler wrapper
- normalization into stable IDs and canonical entities
- DuckDB schema bootstrap and compatibility migrations
- idempotent loading for core tables scoped by `scenario_id` + `run_id`
- availability derivation for each slot (`visible_at`, `unavailable_at`, `current_status`)
- repeatable pipeline execution and regression tests

> Scope note: this repository currently prepares the analytical data model and slot availability state. Pricing recommendations and UI surfaces are scaffolded but not yet implemented.

## Repository layout

```text
medscheduler_wrapper/   # synthetic source generation + normalization
pipeline/               # config, DB bootstrap, stages, runner
models/                 # placeholder for scoring/calibration models
optimizer/              # placeholder for eligibility/recommendation logic
app/                    # placeholder for Streamlit UI
config/default.yaml     # default local run configuration
tests/                  # phase-1 and availability behavior tests
```

## Requirements

- Python 3.11+
- pip (or another PEP 517/518-capable installer)

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

## Run the pipeline

Default config:

```bash
python -m pipeline.run_pipeline
```

With an explicit config path:

```bash
python -c "from pipeline.run_pipeline import run; run('path/to/config.yaml')"
```

Default outputs are written to `data/mvp.duckdb`.

## Slot availability semantics (Phase 1)

`pipeline.stages.availability_stage.apply_availability` computes `unavailable_at` and `current_status` per slot using booking lifecycle events.

Status resolution behavior:

- `removed`, `completed`, `no_show`, `rescheduled`: slot becomes unavailable at the first event time
- `booked`: slot becomes unavailable at booking time unless a later `canceled` event exists
- latest event `canceled`: status returns to `open` if the slot start is still in the future
- no events and slot start in the past: `expired`
- no events and slot start in the future: `open`

`unavailable_at` always falls back to `slot_start_at` to prevent null availability windows.

## Development checks

Run these before committing:

```bash
pytest
ruff check .
black --check .
```

## Notes on idempotency

- Dimension tables (`businesses`, `providers`, `services`, `locations`, `customers`) are replaced per `scenario_id`.
- Fact-like tables (`slots`, `booking_events`) are replaced per `scenario_id` + `run_id`.
- Re-running the same configuration should produce stable identifiers and deterministic slot state.
