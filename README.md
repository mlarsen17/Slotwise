# Slotwise MVP

Slotwise is a simulation-first MVP for a demand-aware pricing engine focused on underbooked appointment slots.

The current codebase now includes Phase 4 pipeline execution, evaluation metrics, and a Streamlit analytics surface:

- deterministic synthetic extraction via the Medscheduler wrapper
- normalization into stable IDs and canonical entities
- DuckDB schema bootstrap and compatibility migrations
- idempotent loading for core tables scoped by `scenario_id` + `run_id`
- availability derivation for each slot (`visible_at`, `unavailable_at`, `current_status`)
- repeatable pipeline execution and regression tests
- cohort baseline computation at snapshot time (`effective_ts`)
- snapshot-safe feature materialization with trailing-window metrics (7d/14d/28d)
- deterministic underbooking detection with sparse-cohort fallback controls
- pooled logistic demand scoring with a frozen feature contract
- snapshot-safe scoring labels (as-of `effective_ts`) with minimum-row fallback guardrails
- deterministic business-level calibration factors
- eligibility-filtered recommendation generation from a fixed action ladder
- deterministic exploration override and rationale code generation
- idempotent persistence of `scoring_outputs`, `business_calibrations`, and `pricing_actions`
- runner-level `pipeline_runs` auditability (`started_at`, `ended_at`, `duration_ms`, failure metadata)
- stage-selective pipeline runner modes (`--stage`, `--from-stage`)
- persisted `run_metadata` + `evaluation_results` artifacts
- Streamlit recommendation explorer and summary diagnostics

## Repository layout

```text
medscheduler_wrapper/   # synthetic source generation + normalization
pipeline/               # config, DB bootstrap, stages, runner
models/                 # scoring + calibration model logic
optimizer/              # eligibility, recommendation, rationale, exploration logic
app/                    # Streamlit analytics UI + DuckDB data access
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
python -m pipeline.run_pipeline --config config/default.yaml
```

With an explicit config path:

```bash
python -c "from pipeline.run_pipeline import run; run('path/to/config.yaml')"
```

Default outputs are written to `data/mvp.duckdb`.

After a successful run you should see rows scoped by `run_id` in:

- `cohort_baselines`
- `feature_snapshots`
- `underbooking_outputs`
- `scoring_outputs`
- `business_calibrations`
- `pricing_actions`
- `pipeline_runs` (terminal `status` of `success`)

Current limitations are intentionally explicit:

- calibration remains a bounded heuristic factor (not full probabilistic calibration)
- model artifacts are not serialized; scoring metadata is persisted instead

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


## Run Streamlit UI

```bash
streamlit run app/streamlit_app.py
```

The UI reads precomputed tables only (`pricing_actions`, `underbooking_outputs`, `feature_snapshots`, `evaluation_results`) and supports run/scenario + business/provider/service/lead-time filters.
