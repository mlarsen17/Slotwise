Below is a detailed implementation checklist designed for an AI agent to execute against and track progress. It is aligned with the product concept, MVP design, and TDD: a simulation-first, analytics-only pricing engine using a Medscheduler wrapper, DuckDB, Streamlit, a plain Python pipeline runner, and idempotent stages.   

# Demand-Aware Pricing Engine MVP

## Step-by-Step Implementation Checklist

## Phase 1 — Data Foundation and Synthetic Environment

### Feature 1.1 — Repository and project scaffolding

* [x] Create the top-level repository structure

  * [x] Create `medscheduler_wrapper/`
  * [x] Create `pipeline/`
  * [x] Create `pipeline/stages/`
  * [x] Create `sql/`
  * [x] Create `models/`
  * [x] Create `optimizer/`
  * [x] Create `app/`
  * [x] Create `config/`
  * [x] Create `data/`
  * [x] Create `tests/`
* [x] Create Python packaging and environment files

  * [x] Add `pyproject.toml` or `requirements.txt`
  * [x] Add `.python-version` if needed
  * [x] Add `.gitignore`
  * [x] Add `README.md`
* [x] Define core dependency set

  * [x] Add `duckdb`
  * [x] Add `pandas`
  * [x] Add `pyyaml`
  * [x] Add `pydantic`
  * [x] Add `streamlit`
  * [x] Add `scikit-learn`
  * [x] Add `pytest`
* [x] Establish coding conventions

  * [x] Choose formatting tool
  * [x] Choose linting tool
  * [x] Decide logging format
  * [x] Decide config loading pattern

### Feature 1.2 — Core configuration and run model

* [x] Create a default config file for the MVP

  * [x] Add DuckDB path
  * [x] Add scenario identifiers
  * [x] Add random seed
  * [x] Add effective timestamp for deterministic runs
  * [x] Add action ladder configuration
  * [x] Add lead-time windows
  * [x] Add global discount limits
* [x] Define a deterministic run identity model

  * [x] Create `run_id` format
  * [x] Create `scenario_id` format
  * [x] Create `feature_snapshot_version` format
  * [x] Create `model_version` format
* [x] Build shared config loader

  * [x] Validate config schema on startup
  * [x] Fail fast on missing required fields
  * [x] Normalize paths and defaults
  * [x] Surface config values to logs at run start

### Feature 1.3 — Medscheduler wrapper

* [x] Define the wrapper interface

  * [x] Create a scenario configuration class
  * [x] Create extract interface for slots
  * [x] Create extract interface for booking lifecycle events
  * [x] Create normalization interface for internal records
* [x] Implement scenario setup support

  * [x] Represent businesses
  * [x] Represent providers
  * [x] Represent services
  * [x] Represent locations
  * [x] Represent customers
  * [x] Represent scenario knobs such as demand shifts and cancellation behavior
* [x] Implement extraction logic

  * [x] Extract raw slot records from Medscheduler
  * [x] Extract booking lifecycle records from Medscheduler
  * [x] Preserve source IDs and timestamps
  * [x] Capture enough information to reconstruct bookable windows
* [x] Implement normalization logic

  * [x] Map source slot records into internal slot schema
  * [x] Map source booking records into internal booking event schema
  * [x] Generate stable internal IDs
  * [x] Attach `source_system`, `source_run_id`, and `scenario_id`
* [x] Make extraction rerunnable

  * [x] Ensure extraction does not append duplicate raw records on rerun
  * [x] Ensure normalization produces the same internal IDs for the same source inputs
  * [x] Add source-level integrity checks

### Feature 1.4 — DuckDB database initialization

* [x] Create database bootstrap module

  * [x] Open or create the DuckDB file
  * [x] Create required schemas or naming conventions
  * [x] Register initialization logs
* [x] Implement core table DDL

  * [x] Create `slots`
  * [x] Create `booking_events`
  * [x] Create `pricing_actions`
* [x] Implement supporting table DDL

  * [x] Create `businesses`
  * [x] Create `providers`
  * [x] Create `feature_snapshots`
  * [x] Create `cohort_baselines`
  * [x] Create `optimizer_configs`
  * [x] Create `scoring_outputs`
  * [x] Create `evaluation_results`
* [x] Add indexes or optimization choices only if necessary for DuckDB MVP

  * [x] Avoid premature optimization
  * [x] Prefer materialized tables and views

### Feature 1.5 — Data loading pipeline

* [x] Implement raw-to-core load stage

  * [x] Load normalized businesses into DuckDB
  * [x] Load normalized providers into DuckDB
  * [x] Load normalized slots into DuckDB
  * [x] Load normalized booking events into DuckDB
* [x] Enforce idempotent writes

  * [x] Use replace-or-merge patterns
  * [x] Delete scoped records by `run_id` or `scenario_id` before insert where appropriate
  * [x] Ensure no duplicate primary keys
* [x] Add row-count validation

  * [x] Validate expected slot count
  * [x] Validate expected booking event count
  * [x] Validate required fields are populated
  * [x] Fail on nulls in mandatory keys

### Feature 1.6 — Availability window logic

* [x] Implement canonical bookable window computation

  * [x] Derive `visible_at`
  * [x] Derive `unavailable_at`
  * [x] Ensure `unavailable_at` is the earliest of booked, removed, start, or expiration events
* [x] Validate bookable window correctness

  * [x] Verify `visible_at <= unavailable_at`
  * [x] Flag malformed slot histories
  * [x] Add tests for booked-before-start
  * [x] Add tests for never-booked slots
  * [x] Add tests for blocked or removed slots
* [x] Persist final slot status

  * [x] Set `current_status`
  * [x] Set derived status if missing from source
  * [x] Document status enum assumptions

### Feature 1.7 — Phase 1 validation and exit criteria

* [x] Add tests for database bootstrap
* [x] Add tests for normalization determinism
* [x] Add tests for stable internal IDs
* [x] Add tests for idempotent loading
* [x] Add a sample end-to-end run on one scenario
* [x] Verify the following before closing Phase 1

  * [x] DuckDB initializes cleanly
  * [x] Medscheduler wrapper extracts and normalizes data
  * [x] Core tables are populated
  * [x] Availability logic works
  * [x] Rerunning Phase 1 does not duplicate data

---

## Phase 1.1 — Pipeline Hardening & Determinism

### Goal

Stabilize the Phase 1 foundation to ensure:

* [x] fully deterministic pipeline runs
* [x] strong idempotency guarantees
* [x] production-grade schema evolution readiness
* [x] reliable downstream feature computation

This phase does not introduce new product capabilities. It makes the existing pipeline correct, reproducible, and extensible.

### Feature 1.1.1 — Deterministic time handling

* [x] Eliminate dependence on wall-clock time

  * [x] Introduce a single source of time truth
  * [x] Use `cfg.effective_ts` as the canonical pipeline timestamp
  * [x] Parse it once and pass it to all stages
* [x] Remove implicit system time usage

  * [x] Remove `CURRENT_TIMESTAMP` in SQL
  * [x] Remove `datetime.now()` or equivalent in Python
* [x] Update availability logic

  * [x] Replace `CURRENT_TIMESTAMP` with `cfg.effective_ts`
  * [x] Pass `effective_ts` explicitly into `apply_availability()`
* [x] Enforce deterministic feature computation

  * [x] Any lead-time or time-until-slot calculation must use `effective_ts`
  * [x] No implicit system time usage is allowed
* [x] Add validation test

  * [x] Running pipeline twice with identical config produces identical outputs

### Feature 1.1.2 — Transactional load stage

* [x] Guarantee atomic DuckDB writes

  * [x] Wrap load stage in a transaction
  * [x] Use `BEGIN TRANSACTION`
  * [x] Execute all deletes and inserts
  * [x] `COMMIT` on success
  * [x] `ROLLBACK` on failure
* [x] Ensure failure safety

  * [x] If any insert fails, no tables are partially updated
  * [x] Scenario data remains consistent
* [x] Add test

  * [x] Simulate failure mid-load and verify no partial writes

### Feature 1.1.3 — Run identity and versioning

* [x] Make pipeline executions identifiable and comparable

  * [x] Promote `run_id` to a first-class concept
  * [x] Generate deterministic or user-provided `run_id`
  * [x] Pass `run_id` through all stages
* [x] Persist run metadata

  * [x] Add `run_id` column to `slots`
  * [x] Add `run_id` column to `booking_events`
  * [x] Add `run_id` column to `pricing_actions`
  * [x] Optionally introduce `pipeline_runs` table
* [x] Update load semantics

  * [x] Scope deletes by `run_id` + `scenario_id`
  * [x] Allow multiple runs of the same scenario to coexist
* [x] Add reproducibility fields

  * [x] Add `effective_ts`
  * [x] Add optional `config_hash`
* [x] Add test

  * [x] Multiple runs of same scenario do not overwrite each other
  * [x] Results are queryable by `run_id`

### Feature 1.1.4 — Schema expansion (forward-compatible core tables)

* [x] Expand `slots` schema

  * [x] Add `standard_price`
  * [x] Add `slot_duration_minutes`
  * [x] Add `integration_id`
  * [x] Add `external_slot_id`
* [x] Expand `booking_events` schema

  * [x] Add `business_id`
  * [x] Add `provider_id`
  * [x] Add `service_type`
* [x] Expand `pricing_actions` schema

  * [x] Add `eligible_action_set`
  * [x] Add `decision_reason`
  * [x] Add `was_exploration`
  * [x] Add `exploration_policy`
  * [x] Add `decision_timestamp`
  * [x] Add `feature_snapshot_version`
  * [x] Add `confidence_score`
  * [x] Add `rationale_codes`
* [x] Ensure backward compatibility

  * [x] Allow default values for Phase 1 outputs
* [x] Add migration logic

  * [x] Use a safe table rebuild or `ALTER TABLE` strategy

### Feature 1.1.5 — Strong schema validation

* [x] Enforce strict extraction-normalization-load contracts

  * [x] Define explicit schema for `slots` dataframe
  * [x] Define explicit schema for `booking_events` dataframe
* [x] Validate structure and types

  * [x] Required columns exist
  * [x] No unexpected columns in optional strict mode
  * [x] Data types are correct
  * [x] Timestamps are valid and parseable
  * [x] Required fields are non-null
* [x] Validate event types

  * [x] Enforce allowed enum set
* [x] Improve error handling

  * [x] Raise descriptive validation errors instead of `KeyError`
* [x] Add test

  * [x] Invalid schema fails fast with clear message

### Feature 1.1.6 — Slot lifecycle and availability semantics

* [x] Define canonical slot lifecycle states

  * [x] `open`
  * [x] `booked`
  * [x] `canceled`
  * [x] `expired`
  * [x] `removed`
* [x] Clarify cancellation behavior

  * [x] Explicitly decide whether canceled slots remain unavailable or reopen inventory
* [x] Align lifecycle-derived fields

  * [x] `current_status` reflects final state
  * [x] `unavailable_at` aligns with lifecycle decision
* [x] Document lifecycle rules

  * [x] Define how `unavailable_at` is derived
  * [x] Define precedence rules between events
* [x] Add lifecycle tests

  * [x] `book` → `cancel` transition correctness
  * [x] `no event` → expiration behavior
  * [x] `removed` precedence behavior

### Feature 1.1.7 — Expanded event model

* [x] Extend allowed event types

  * [x] `booked`
  * [x] `canceled`
  * [x] `removed`
  * [x] `completed`
  * [x] `no_show`
  * [x] `rescheduled`
* [x] Update normalization

  * [x] Accept and map new event types
  * [x] Maintain backward compatibility
* [x] Update integrity checks

  * [x] Validate event sequences where applicable
* [x] Add test

  * [x] Pipeline handles extended events without failure

### Feature 1.1.8 — Pipeline observability

* [x] Add structured logging per stage

  * [x] Stage name
  * [x] `run_id`
  * [x] `scenario_id`
  * [x] Input row counts
  * [x] Output row counts
  * [x] Execution time
* [x] Log failures with stage context

  * [x] Clear stage context
  * [x] Root cause
* [x] Optional persistence

  * [x] Persist run summary table

### Feature 1.1.9 — Test coverage expansion

* [x] Add determinism test

  * [x] Same config produces identical outputs
* [x] Add transaction test

  * [x] Simulated failure produces no partial writes
* [x] Add run isolation test

  * [x] Multiple `run_id` values coexist
* [x] Add lifecycle test

  * [x] Cancellation and expiration correctness
* [x] Add schema validation test

  * [x] Missing or invalid columns fail fast

### Feature 1.1.10 — Phase 1.1 validation and exit criteria

* [x] Verify the following before closing Phase 1.1

  * [x] Pipeline runs are fully deterministic for identical config
  * [x] No stage depends on wall-clock time
  * [x] Load stage is atomic and failure-safe
  * [x] Core schemas support Phase 2 and Phase 3 requirements without redesign
  * [x] Slot lifecycle semantics are explicit and consistent
  * [x] Extended event types are supported
  * [x] Pipeline execution is observable and debuggable
  * [x] Test suite covers determinism, failure safety, and lifecycle correctness

---

## Phase 2 — Baselines, Features, and Underbooking Detection

### Feature 2.1 — Cohort baseline framework

* [x] Define MVP cohort dimensions

  * [x] Day of week
  * [x] Time-of-day bucket
  * [x] Service type
* [x] Implement time-of-day bucketing

  * [x] Define bucket boundaries in config
  * [x] Compute bucket for each slot
  * [x] Test edge cases around bucket boundaries
* [x] Build cohort baseline SQL models

  * [x] Compute historical fill rate by cohort
  * [x] Compute expected booking pace by cohort
  * [x] Compute average booking lead time by cohort
  * [x] Compute realized completion rate by cohort
* [x] Persist cohort baselines

  * [x] Write to `cohort_baselines`
  * [x] Attach `feature_snapshot_version`
  * [x] Attach `run_id` and `scenario_id`
* [x] Validate cohort quality

  * [x] Check for cohorts with too few observations
  * [x] Decide fallback behavior for sparse cohorts
  * [x] Log sparse cohort counts

### Feature 2.2 — Feature materialization framework

* [x] Define feature snapshot contract

  * [x] Primary key should include `internal_slot_id` and `feature_snapshot_version`
  * [x] Every feature row should carry `run_id` and `scenario_id`
  * [x] Feature generation must use deterministic effective time
* [x] Implement feature table creation pattern

  * [x] Use `CREATE OR REPLACE TABLE` or equivalent idempotent write pattern
  * [x] Separate intermediate SQL models from final materialized feature table
* [x] Create shared feature generation utilities

  * [x] Time delta calculations
  * [x] Null handling
  * [x] Safe rate computation
  * [x] Cohort joins
  * [x] Fallback logic for sparse data

### Feature 2.3 — Slot demand history features

* [x] Implement historical fill features

  * [x] Historical fill rate for similar slots
  * [x] Same-provider same-service fill rate
  * [x] Same-business same-service trailing fill
* [x] Implement booking pace features

  * [x] Expected booking pace for similar slots
  * [x] Observed booking pace for current slot state
  * [x] Pace deviation from cohort baseline
* [x] Implement lead-time features

  * [x] Average lead time to booking for cohort
  * [x] Hours until slot
  * [x] Days until slot
* [x] Validate history features

  * [x] Confirm rates fall within valid ranges
  * [x] Confirm no divide-by-zero behavior
  * [x] Confirm fallback logic for sparse history

### Feature 2.4 — Temporal and scarcity features

* [x] Implement temporal features

  * [x] Day of week
  * [x] Time-of-day bucket
  * [x] Slot duration
  * [x] Effective lead-time band
* [x] Implement scarcity features

  * [x] Remaining slots for same provider on same day
  * [x] Remaining similar-service slots in comparable time window
  * [x] Inventory density around slot start
* [x] Validate scarcity features

  * [x] Confirm current slot is excluded or included consistently in counts
  * [x] Confirm same-day counts are correct
  * [x] Confirm window definitions are stable and documented

### Feature 2.5 — Operational features

* [x] Implement provider utilization features

  * [x] Trailing 7-day utilization
  * [x] Trailing 14-day utilization
  * [x] Trailing 28-day utilization
* [x] Implement booking volume features

  * [x] Trailing 7-day bookings
  * [x] Trailing 14-day bookings
  * [x] Trailing 28-day bookings
* [x] Implement disruption features

  * [x] Cancellation rate by slot pattern
  * [x] No-show rate by slot pattern
  * [x] Reschedule rate by slot pattern
* [x] Implement business trend features

  * [x] Business-level fill trend
  * [x] Business-level booking trend
* [x] Validate operational features

  * [x] Check valid ranges
  * [x] Check sparse-history fallback
  * [x] Check window boundaries

### Feature 2.6 — Underbooking detection logic

* [x] Define underbooking detection inputs

  * [x] Cohort expected pace
  * [x] Observed pace
  * [x] Cohort expected fill
  * [x] Predicted or baseline fill by start
* [x] Implement pace gap calculation

  * [x] Compute observed pace
  * [x] Compute cohort expected pace at comparable lead time
  * [x] Compute normalized pace gap
* [x] Implement fill gap calculation

  * [x] Compute expected fill baseline
  * [x] Compute slot-level projected fill
  * [x] Compute normalized fill gap
* [x] Implement severity score

  * [x] Define initial weighted formula
  * [x] Make thresholds configurable
  * [x] Clamp score to valid range
* [x] Implement underbooked classification

  * [x] Set boolean `underbooked`
  * [x] Set `severity_score`
  * [x] Attach detection reason fields for debugging
* [x] Persist underbooking outputs

  * [x] Create or update a dedicated table or view
  * [x] Join back to feature snapshot keys
* [x] Validate underbooking outputs

  * [x] Ensure known low-demand scenarios are flagged
  * [x] Ensure healthy slots are not overwhelmingly flagged
  * [x] Review severity score distribution

### Feature 2.7 — Phase 2 validation and exit criteria

* [x] Add SQL or Python tests for cohort baselines
* [x] Add tests for feature materialization idempotency
* [x] Add tests for time-of-day bucket assignment
* [x] Add tests for severity score bounds
* [x] Run one scenario with intentionally depressed demand
* [x] Verify the following before closing Phase 2

  * [x] Cohort baselines are computed
  * [x] Feature snapshots materialize cleanly
  * [x] Underbooking flags are produced
  * [x] Severity scores look directionally correct
  * [x] Reruns do not create duplicate outputs

---

## Phase 2.1 — Feature Correctness, Snapshot Semantics, and Underbooking Reliability

### Goal

Tighten Phase 2 so the feature layer is trustworthy for downstream scoring and optimization by:

* [x] removing future leakage
* [x] replacing placeholder rolling/window metrics with true time-bounded calculations
* [x] improving idempotency and reproducibility
* [x] adding phase-2-specific tests

### Feature 2.1.1 — Snapshot-safe baseline computation

* [x] Update `compute_cohort_baselines()` so all inputs are computed as of `effective_ts`, not from the full run history
* [x] Filter booking events to `event_at <= effective_ts` before deriving:

  * [x] first booked timestamp
  * [x] completion flags
  * [x] booking pace inputs
* [x] Exclude slots not yet visible by `effective_ts` from cohort observations
* [x] Decide whether slots already started before `effective_ts` remain in baseline training data and document the rule explicitly
* [x] Recompute `fill_rate`, `expected_booking_pace_per_day`, `avg_lead_time_hours`, and `completion_rate` using only snapshot-available data
* [x] Add an explicit fallback strategy for sparse cohorts instead of silently relying on whatever rows happen to remain
* [x] Document baseline semantics in code comments:

  * [x] “historical cohort metrics are computed from information available at snapshot time”
  * [x] “no post-snapshot events may affect cohort baselines”

### Feature 2.1.2 — Snapshot-safe feature materialization

* [x] Update `materialize_feature_snapshot()` so every feature is derived only from data available at `effective_ts`
* [x] Filter events to `event_at <= effective_ts` before calculating:

  * [x] `booked_flag`
  * [x] `lead_time_hours`
  * [x] utilization metrics
  * [x] booking volumes
  * [x] cancellation / no-show / reschedule rates
* [x] Revisit `booked_flag` semantics for future slots:

  * [x] booked by snapshot time
  * [x] not ever booked in the full run
* [x] Ensure `hours_until_slot` and `days_until_slot` remain relative to `effective_ts`
* [x] Rename or clarify fields where semantics are ambiguous:

  * [x] either replace `service_type` with `service_id`
  * [x] or carry both fields explicitly throughout the pipeline
* [x] Add docstrings describing each feature as either:

  * [x] historical feature
  * [x] current-state feature
  * [x] cohort-derived feature

### Feature 2.1.3 — Replace fake rolling windows with true calculations

* [x] Replace placeholder rolling-window calculations in `feature_stage.py`
* [x] Define precise window semantics for each metric:

  * [x] trailing 7 days from `effective_ts`
  * [x] trailing 14 days from `effective_ts`
  * [x] trailing 28 days from `effective_ts`
* [x] For `provider_utilization_{window}d`, compute:

  * [x] numerator = booked or completed slots for that provider within the trailing window
  * [x] denominator = eligible visible/bookable slots for that provider within the same window
* [x] For `booking_volume_{window}d`, compute:

  * [x] count of booking events or first-booked slots within the trailing window
  * [x] choose one definition and document it
* [x] Replace constant pattern-rate features with true pattern-level rates:

  * [x] `cancel_rate_pattern`
  * [x] `no_show_rate_pattern`
  * [x] `reschedule_rate_pattern`
* [x] Define the grouping key for “pattern”:

  * [x] day of week
  * [x] time-of-day bucket
  * [x] service
  * [x] optionally provider or business
* [x] Replace `inventory_density_2h = remaining_service_slots_window` with a true local density calculation:

  * [x] count comparable slots within ±2 hours of the slot start
  * [x] or within a forward-looking 2-hour neighborhood
  * [x] choose and document one approach
* [x] Replace `remaining_provider_slots_same_day` so it counts only slots still remaining relative to `effective_ts`, not all provider-day slots
* [x] Add helper utilities for window filtering so logic is reusable and testable
* [x] Add comments clarifying whether each rolling feature is:

  * [x] event-time-based
  * [x] slot-start-time-based
  * [x] visibility-window-based

### Feature 2.1.4 — Improve underbooking signal quality

* [x] Rework `detect_underbooking()` so the fill component is not just a blend of closely related historical aggregates
* [x] Define a more explicit projected fill estimate using:

  * [x] current slot state at snapshot
  * [x] lead time remaining
  * [x] cohort booking pace trajectory
* [x] Remove or justify the hardcoded `0.6` fallback baseline
* [x] Make sparse-cohort fallback configurable in `config/default.yaml`
* [x] Preserve `severity_score` as a continuous value in `[0, 1]`
* [x] Keep `detection_reason`, but expand reason taxonomy if helpful:

  * [x] `pace_gap`
  * [x] `fill_gap`
  * [x] `pace_gap_and_fill_gap`
  * [x] `sparse_cohort_fallback`
  * [x] `healthy`

### Feature 2.1.5 — Reproducibility and idempotency hardening

* [x] Fix `pipeline_runs` write semantics with uniqueness protection or upsert behavior
* [x] Decide one model:

  * [x] append-only execution log with separate execution ID
  * [x] or unique `(run_id, scenario_id)` with update/upsert
* [x] Expand `config_hash()` to include all Phase 2 behavior settings, especially:

  * [x] `time_of_day_buckets`
  * [x] `underbooking`
* [x] Add explicit duplicate checks after inserts for:

  * [x] `cohort_baselines`
  * [x] `feature_snapshots`
  * [x] `underbooking_outputs`
* [x] Consider adding logical uniqueness assertions in code even if DuckDB constraints remain minimal

### Feature 2.1.6 — Phase 2 validation and tests

* [x] Add a dedicated `tests/test_phase2.py`
* [x] Add a test proving no future-event leakage:

  * [x] create a slot
  * [x] create booking/completion events after `effective_ts`
  * [x] assert baselines/features ignore them
* [x] Add a test for true rolling-window utilization:

  * [x] events inside 7d count toward 7d/14d/28d
  * [x] older events count only toward larger windows
* [x] Add a test for true rolling-window booking volume with the same structure
* [x] Add a test for pattern-rate calculations:

  * [x] cancellation/no-show/reschedule rates differ by cohort pattern when inputs differ
* [x] Add a test for `remaining_provider_slots_same_day`:

  * [x] only future or still-relevant slots are counted
* [x] Add a test for sparse cohort fallback behavior
* [x] Add a test for deterministic reruns:

  * [x] same config
  * [x] same snapshot time
  * [x] same outputs
* [x] Add a test for changed config hash when underbooking settings or bucket boundaries change

### Feature 2.1.7 — Code hygiene and semantics cleanup

* [x] Replace direct model defaults in `AppConfig` with `default_factory` for cleaner Pydantic usage
* [x] Add docstrings to:

  * [x] `compute_cohort_baselines()`
  * [x] `materialize_feature_snapshot()`
  * [x] `detect_underbooking()`
* [x] Add a small internal metrics summary after each stage:

  * [x] rows processed
  * [x] sparse cohorts
  * [x] underbooked slot count
  * [x] null fallback counts
* [x] Update README/project status language so it no longer describes the repo as only Phase 1

### Feature 2.1.8 — Exit criteria

* [x] No phase-two feature uses data after `effective_ts`
* [x] Rolling-window metrics are truly time-bounded
* [x] Placeholder window features are removed
* [x] Underbooking outputs are deterministic and explainable
* [x] New tests cover leakage, rolling windows, sparse fallback, and determinism
* [x] Phase 2 outputs are trustworthy enough to support Phase 3 scoring and optimization

---

## Phase 3 — Scoring, Optimization, Explanations, and Exploration

### Feature 3.1 — Scoring data contract

* [x] Define the model input dataset

  * [x] Select final feature columns
  * [x] Define target label for MVP scoring
  * [x] Exclude leakage fields
  * [x] Freeze feature ordering and names
* [x] Define scoring output contract

  * [x] `booking_probability`
  * [x] `predicted_fill_by_start`
  * [x] `shortfall_score`
  * [x] `confidence_score`
  * [x] `model_version`
* [x] Persist scoring outputs

  * [x] Create `scoring_outputs`
  * [x] Key by `internal_slot_id`, `run_id`, `feature_snapshot_version`

### Feature 3.2 — Pooled demand scoring model

* [x] Build initial training dataset

  * [x] Pull feature snapshots
  * [x] Join labels from booking outcomes
  * [x] Filter invalid or incomplete training examples
* [x] Implement baseline model training

  * [x] Start with logistic regression or similar lightweight model
  * [x] Add train/validation split
  * [x] Train on pooled data across businesses
  * [x] Persist model artifact and metadata
* [x] Implement scoring logic

  * [x] Load model artifact
  * [x] Score current eligible slots
  * [x] Write outputs to `scoring_outputs`
* [x] Validate scoring quality

  * [x] Inspect score distributions
  * [x] Confirm higher-risk slots have lower booking probability
  * [x] Confirm outputs are stable on rerun

### Feature 3.3 — Business-level calibration

* [x] Define business calibration strategy

  * [x] Choose adjustment based on business baseline fill or residual trend
  * [x] Keep it simple and transparent for MVP
* [x] Implement calibration calculation

  * [x] Compute business-level adjustment factors
  * [x] Version and persist the factors
* [x] Apply calibration to pooled outputs

  * [x] Adjust booking probability or shortfall score
  * [x] Clamp calibrated outputs to valid ranges
* [x] Validate calibration behavior

  * [x] Confirm business-specific differences are reflected
  * [x] Confirm calibration does not swamp the pooled model
  * [x] Compare calibrated vs uncalibrated outputs

### Feature 3.4 — Optimizer configuration and eligibility rules

* [x] Implement optimizer config model

  * [x] Global config
  * [x] Business config
  * [x] Provider-level placeholder support
* [x] Implement fixed action ladder

  * [x] 0%
  * [x] 5%
  * [x] 10%
  * [x] 15%
  * [x] 20%
* [x] Implement eligibility rules

  * [x] Excluded services cannot be discounted
  * [x] Discount cannot exceed configured maximum
  * [x] Discount only allowed within lead-time windows
  * [x] Discount cannot violate price floor
  * [x] Healthy slots should usually remain at 0%
* [x] Implement eligible action set generation

  * [x] Produce candidate actions per slot
  * [x] Persist or log `eligible_action_set`
* [x] Validate eligibility logic

  * [x] Test premium or excluded service behavior
  * [x] Test max-discount enforcement
  * [x] Test price-floor enforcement
  * [x] Test lead-time window enforcement

### Feature 3.5 — Discount recommendation engine

* [x] Define recommendation policy

  * [x] If slot is not underbooked, recommend 0%
  * [x] If slot is underbooked, map severity and score to best action
* [x] Implement initial severity-to-action mapping

  * [x] Add config-driven breakpoints
  * [x] Allow business overrides where appropriate
* [x] Compute final implied price

  * [x] Apply recommended action to standard price
  * [x] Confirm price floor rules are respected
* [x] Create recommendation output record

  * [x] Set `recommended_action_type`
  * [x] Set `recommended_action_value`
  * [x] Set `confidence_score`
  * [x] Set `decision_reason`
* [x] Validate recommendations

  * [x] Ensure only eligible actions are selected
  * [x] Ensure higher severity generally maps to higher discounts
  * [x] Ensure healthy slots are mostly 0%

### Feature 3.6 — Rationale code engine

* [x] Define rationale code taxonomy

  * [x] Historically underbooked weekday afternoon
  * [x] Booking pace below baseline
  * [x] Short lead-time low fill
  * [x] Provider utilization below target
  * [x] Any other initial codes needed for explainability
* [x] Implement rule-based rationale generation

  * [x] Generate codes from feature values
  * [x] Generate codes from baseline deviations
  * [x] Generate codes from optimizer decisions
* [x] Persist rationale codes

  * [x] Attach to recommendation output
  * [x] Store as JSON array in `pricing_actions`
* [x] Validate rationale quality

  * [x] Confirm every discounted slot has at least one rationale code
  * [x] Confirm codes match actual slot context
  * [x] Eliminate redundant or contradictory codes

### Feature 3.7 — Exploration policy and logging

* [x] Define exploration policy

  * [x] Configure exploration share
  * [x] Restrict exploration to eligible actions
  * [x] Seed randomness deterministically
* [x] Implement slot-level exploration override

  * [x] For eligible slots, randomly choose from allowed actions when exploration applies
  * [x] Otherwise use the optimizer result
* [x] Log exploration metadata

  * [x] `was_exploration`
  * [x] `exploration_policy`
  * [x] `decision_reason`
  * [x] `eligible_action_set`
* [x] Validate exploration behavior

  * [x] Confirm exploration fraction is within expected range
  * [x] Confirm reruns with same seed produce same exploratory choices
  * [x] Confirm no policy violations occur

### Feature 3.8 — Pricing action persistence

* [x] Implement final `pricing_actions` write stage

  * [x] Delete existing records for current `run_id` if needed
  * [x] Insert final recommendations
  * [x] Ensure primary keys are stable
* [x] Enforce idempotency

  * [x] Reruns replace or reinsert the same logical action rows
  * [x] No duplicate pricing actions for the same run
* [x] Validate stored records

  * [x] Confirm all required fields are populated
  * [x] Confirm JSON fields serialize correctly
  * [x] Confirm confidence and rationale values are present

### Feature 3.9 — Phase 3 validation and exit criteria

* [x] Add model training and scoring tests
* [x] Add optimizer rule tests
* [x] Add rationale generation tests
* [x] Add exploration determinism tests
* [x] Run a full scenario and inspect pricing actions
* [x] Verify the following before closing Phase 3

  * [x] Scoring outputs are generated
  * [x] Business calibration works
  * [x] Eligible action sets are correct
  * [x] Discount recommendations are written
  * [x] Rationale codes and exploration metadata are present
  * [x] Reruns remain idempotent

---

## Phase 3.1 — Phase 3 Stabilization, Runner Integration, and Recommendation Reliability

### Goal

Close the gap between the intended Phase 3 design and the current repository state by:

* [ ] wiring the scoring and optimization stack into the real pipeline runner
* [ ] fixing current runner-breaking issues
* [ ] hardening persistence and failure semantics
* [ ] replacing Phase 3 placeholders with fully operational recommendation outputs
* [ ] adding end-to-end tests that validate the actual runnable system

This phase is not about expanding scope. It is about making Phase 3 real, stable, and trustworthy in the main execution path. The need for this phase follows from the current repo state: the spec says Phase 3 is complete, but the pipeline runner and project status do not yet reflect a finished scoring and recommendation flow.

### Feature 3.1.1 — Runner correctness and stage wiring

* [ ] Fix the current underbooking runner call

  * [ ] Update `run_pipeline.py` to pass `sparse_baseline_fill_rate` into `detect_underbooking()`
  * [ ] Verify the value is sourced from `cfg.underbooking.sparse_baseline_fill_rate`
  * [ ] Confirm the pipeline no longer fails with a missing-argument error
* [ ] Audit runner stage order

  * [ ] Confirm extraction runs before load
  * [ ] Confirm availability runs before baselines and features
  * [ ] Confirm underbooking runs after feature materialization
  * [ ] Insert scoring stage after underbooking
  * [ ] Insert calibration stage after scoring
  * [ ] Insert optimization stage after calibration
  * [ ] Insert pricing action persistence after optimization
* [ ] Standardize runner stage contracts

  * [ ] Each stage should accept explicit inputs
  * [ ] Each stage should return a deterministic summary or dataframe where appropriate
  * [ ] Each stage should log row counts and output location
* [ ] Validate full runner path

  * [ ] Confirm a full pipeline run reaches pricing action outputs
  * [ ] Confirm no Phase 3 logic is “implemented” only in isolated modules or tests

### Feature 3.1.2 — Phase 3 module implementation alignment

* [ ] Reconcile repo structure with Phase 3 requirements

  * [ ] Confirm `models/` contains actual scoring implementation
  * [ ] Confirm `optimizer/` contains actual eligibility, recommendation, rationale, and exploration modules
  * [ ] Remove empty placeholder files or replace them with working code
* [ ] Make module boundaries explicit

  * [ ] Define scoring entrypoint
  * [ ] Define calibration entrypoint
  * [ ] Define optimizer entrypoint
  * [ ] Define rationale generation entrypoint
  * [ ] Define exploration override entrypoint
* [ ] Enforce interface consistency

  * [ ] Shared identifiers should use one naming convention
  * [ ] Inputs should carry `run_id`, `scenario_id`, and `feature_snapshot_version`
  * [ ] Outputs should be shaped for direct persistence into DuckDB tables
* [ ] Validate implementation completeness

  * [ ] Ensure no spec-marked-complete Phase 3 feature still exists only as a stub
  * [ ] Ensure README and repo status match actual implementation state

### Feature 3.1.3 — Scoring pipeline hardening

* [ ] Make the scoring data contract executable, not just documented

  * [ ] Freeze the exact feature column list used by the model
  * [ ] Fail fast if required feature columns are missing
  * [ ] Fail fast if feature order differs from training order
* [ ] Harden training and inference semantics

  * [ ] Document the label definition clearly
  * [ ] Ensure no leakage fields are included in training inputs
  * [ ] Persist model metadata alongside outputs
  * [ ] Version model artifacts deterministically
* [ ] Harden scoring output writes

  * [ ] Delete scoped rows before insert for the current `run_id`
  * [ ] Enforce one scoring output row per slot per snapshot version
  * [ ] Validate all probability-like outputs are clamped to valid ranges
* [ ] Validate scoring quality mechanically

  * [ ] Confirm score distributions are non-degenerate
  * [ ] Confirm booking probability and shortfall score move in opposite directions where expected
  * [ ] Confirm reruns with same inputs produce identical scoring outputs outside explicitly seeded behavior

### Feature 3.1.4 — Business calibration hardening

* [ ] Make business calibration fully deterministic

  * [ ] Seed or eliminate any non-deterministic logic
  * [ ] Persist calibration factors by `business_id`, `run_id`, and `feature_snapshot_version`
* [ ] Tighten calibration semantics

  * [ ] Define exactly what base signal is being adjusted
  * [ ] Define whether calibration applies to booking probability, shortfall score, or both
  * [ ] Clamp calibrated values to valid numeric ranges
* [ ] Improve calibration observability

  * [ ] Log the number of calibrated businesses
  * [ ] Log factor ranges
  * [ ] Log fallback behavior for sparse business history
* [ ] Validate calibration behavior

  * [ ] Confirm calibration changes outputs where business history differs materially
  * [ ] Confirm calibration remains a bounded adjustment rather than replacing pooled model behavior

### Feature 3.1.5 — Optimizer and eligibility reliability

* [ ] Make eligibility logic fully testable and explicit

  * [ ] Define one function to produce the eligible action set
  * [ ] Define one function to apply price-floor constraints
  * [ ] Define one function to enforce lead-time window rules
  * [ ] Define one function to enforce excluded-service rules
* [ ] Tighten action ladder semantics

  * [ ] Ensure `0%` is always representable
  * [ ] Ensure configured ladder and configured max discount cannot diverge silently
  * [ ] Fail fast if severity breakpoints and discount steps are misaligned
* [ ] Improve optimizer policy clarity

  * [ ] Document exactly how underbooking status, severity, and calibrated score interact
  * [ ] Document the precedence of rules vs model-informed action mapping
  * [ ] Ensure healthy slots default to `0%` unless explicitly overridden by exploration policy
* [ ] Validate optimizer outputs

  * [ ] Final action must always be a member of `eligible_action_set`
  * [ ] Final implied price must always respect price floor
  * [ ] Severity increases should generally not map to lower discounts without a documented rule reason

### Feature 3.1.6 — Rationale code quality and consistency

* [ ] Make rationale generation deterministic and rule-based

  * [ ] Define a central rationale taxonomy
  * [ ] Define threshold logic for each rationale code
  * [ ] Prevent duplicate rationale codes per slot
* [ ] Improve rationale coverage

  * [ ] Ensure every discounted slot has at least one rationale code
  * [ ] Ensure every non-zero recommendation has a decision reason
  * [ ] Ensure exploration decisions can still surface operational rationale plus exploration metadata
* [ ] Eliminate contradictory rationale combinations

  * [ ] Review code combinations for conflicts
  * [ ] Add tests for incompatible rationale pairs
* [ ] Validate rationale usefulness

  * [ ] Confirm rationale codes are traceable to actual feature values
  * [ ] Confirm rationale codes explain both underbooking state and optimizer action where appropriate

### Feature 3.1.7 — Exploration determinism and policy safety

* [ ] Make exploration implementation reproducible

  * [ ] Seed exploration deterministically from stable identifiers such as `run_id`, `slot_id`, and global seed
  * [ ] Ensure repeated runs with same seed yield same exploratory choices
* [ ] Constrain exploration safely

  * [ ] Exploration may only select from `eligible_action_set`
  * [ ] Exploration may not violate max discount or price floor
  * [ ] Exploration share must be bounded to `[0, 1]`
* [ ] Improve exploration logging

  * [ ] Persist `was_exploration`
  * [ ] Persist `exploration_policy`
  * [ ] Persist final `decision_reason`
  * [ ] Persist eligible action set used for the decision
* [ ] Validate exploration behavior

  * [ ] Confirm empirical exploration rate is close to configured rate
  * [ ] Confirm exploratory and exploitative decisions are distinguishable in stored outputs

### Feature 3.1.8 — Pricing action persistence and table integrity

* [ ] Harden `pricing_actions` writes

  * [ ] Delete scoped rows for current `run_id` before insert
  * [ ] Ensure one logical pricing action row per slot per run
  * [ ] Serialize JSON-like fields consistently
* [ ] Strengthen database-level integrity where practical

  * [ ] Add logical uniqueness checks for slots
  * [ ] Add logical uniqueness checks for booking_events
  * [ ] Add logical uniqueness checks for scoring_outputs
  * [ ] Add logical uniqueness checks for pricing_actions
* [ ] Improve persistence validation

  * [ ] Check for duplicate slot recommendations after insert
  * [ ] Check for null required fields after insert
  * [ ] Check action values are within configured limits
* [ ] Validate recommendation record completeness

  * [ ] `recommended_action_type` present
  * [ ] `recommended_action_value` present
  * [ ] `decision_timestamp` present
  * [ ] `confidence_score` present
  * [ ] `rationale_codes` present
  * [ ] exploration metadata present when applicable

### Feature 3.1.9 — Pipeline run failure semantics and observability

* [ ] Fix `pipeline_runs` failure handling

  * [ ] Ensure failed runs are still recorded
  * [ ] Do not lose run metadata on transaction rollback
  * [ ] Persist terminal status as success or failed
* [ ] Improve stage-level observability

  * [ ] Log stage start
  * [ ] Log stage end
  * [ ] Log row counts in and out
  * [ ] Log table names written
  * [ ] Log failure location and exception context
* [ ] Add end-of-run summary logging

  * [ ] Total slots processed
  * [ ] Underbooked slots detected
  * [ ] Slots scored
  * [ ] Pricing actions written
  * [ ] Exploration share observed
* [ ] Validate operational debuggability

  * [ ] A failed run should still be diagnosable from DB state and logs
  * [ ] A successful run should be auditable from `run_id`

### Feature 3.1.10 — Performance cleanup for Phase 3-critical paths

* [ ] Replace obvious quadratic feature computations before scaling further

  * [ ] Rework `inventory_density_2h` to avoid row-by-row Python scans
  * [ ] Prefer DuckDB SQL or vectorized Pandas operations for slot neighborhood calculations
* [ ] Audit other expensive transformations

  * [ ] Identify repeated dataframe scans inside loops
  * [ ] Push large joins and aggregations into DuckDB where practical
* [ ] Validate performance directionally

  * [ ] Confirm the pipeline remains responsive on larger synthetic scenarios
  * [ ] Confirm correctness does not change after vectorization or SQL rewrite

### Feature 3.1.11 — End-to-end test coverage for actual runnable behavior

* [ ] Add a top-level runner smoke test

  * [ ] Execute the real pipeline entrypoint against a temporary DuckDB database
  * [ ] Assert that the run completes successfully
  * [ ] Assert that pricing actions are produced
* [ ] Add a Phase 3 integration test suite

  * [ ] scoring outputs are produced
  * [ ] calibration outputs are produced
  * [ ] eligible action sets are valid
  * [ ] pricing actions are produced
  * [ ] rationale codes are attached
  * [ ] exploration metadata is correct
* [ ] Add failure-path tests

  * [ ] broken stage marks run as failed
  * [ ] no partial recommendation outputs survive for a failed run unless explicitly intended
* [ ] Add idempotency tests

  * [ ] repeated full runs with same config produce same outputs
  * [ ] no duplicate pricing action rows appear
* [ ] Add recommendation-policy tests

  * [ ] healthy slots map to `0%` by default
  * [ ] underbooked higher-severity slots usually map to larger discounts
  * [ ] excluded services remain undiscounted

### Feature 3.1.12 — Documentation and status alignment

* [ ] Update `README.md`

  * [ ] Reflect actual implemented phase accurately
  * [ ] Document how to run the full pipeline
  * [ ] Document what outputs should exist after a successful run
* [ ] Update spec completion markers honestly

  * [ ] Mark only the truly completed Phase 3 items as complete
  * [ ] Move partially implemented items into Phase 3.1 if still in progress
* [ ] Document Phase 3 runtime contracts

  * [ ] scoring inputs
  * [ ] scoring outputs
  * [ ] optimizer inputs
  * [ ] pricing action outputs
* [ ] Validate contributor clarity

  * [ ] A new engineer or AI agent should be able to tell what is implemented, what is partial, and what remains

### Feature 3.1.13 — Exit criteria

* [ ] The real pipeline runner executes end to end without argument or wiring errors
* [ ] Scoring, calibration, optimization, rationale generation, and exploration all run in the main path
* [ ] `pricing_actions` is populated with stable, complete, idempotent outputs
* [ ] Failed runs are persisted and diagnosable
* [ ] End-to-end tests cover the actual runner, not just isolated stage functions
* [ ] README and spec status match the real repository state
* [ ] Phase 3 is trustworthy enough that Phase 4 can focus on UI and evaluation instead of backfilling core recommendation logic

---

## Phase 4 — Pipeline Runner, Analytics UI, and Evaluation Suite

### Feature 4.1 — Plain Python pipeline runner

* [ ] Implement `run_pipeline.py`

  * [ ] Load config
  * [ ] Resolve run metadata
  * [ ] Open database connection
  * [ ] Execute stages sequentially
* [ ] Implement stage abstraction

  * [ ] Standard stage interface
  * [ ] Standard success and failure logging
  * [ ] Standard stage timing
  * [ ] Standard row-count reporting
* [ ] Wire all stages into the runner

  * [ ] Extraction stage
  * [ ] Load stage
  * [ ] Baseline stage
  * [ ] Feature stage
  * [ ] Underbooking stage
  * [ ] Scoring stage
  * [ ] Optimization stage
  * [ ] Evaluation stage
* [ ] Add stage selection options

  * [ ] Full pipeline mode
  * [ ] Single-stage mode
  * [ ] Resume-from-stage mode if desired
* [ ] Validate runner behavior

  * [ ] Ensure failures stop the pipeline
  * [ ] Ensure logs clearly identify failing stage
  * [ ] Ensure repeated full runs are safe

### Feature 4.2 — Structured logging and reproducibility

* [ ] Implement structured logging

  * [ ] Stage name
  * [ ] Run ID
  * [ ] Scenario ID
  * [ ] Row counts
  * [ ] Duration
  * [ ] Failure details
* [ ] Implement run metadata persistence

  * [ ] Effective timestamp
  * [ ] Random seed
  * [ ] Config version
  * [ ] Model version
  * [ ] Feature snapshot version
* [ ] Validate reproducibility

  * [ ] Same config + same seed + same inputs produce same outputs
  * [ ] Exploration choices remain stable under same seed

### Feature 4.3 — Streamlit analytics UI foundation

* [ ] Create Streamlit app entrypoint

  * [ ] Connect to DuckDB
  * [ ] Load latest run metadata
  * [ ] Add sidebar filters
* [ ] Implement app data access layer

  * [ ] Query precomputed tables only
  * [ ] Avoid expensive recomputation in UI
  * [ ] Handle missing or empty-state runs
* [ ] Add global filters

  * [ ] Run ID
  * [ ] Scenario ID
  * [ ] Business
  * [ ] Provider
  * [ ] Service type
  * [ ] Lead-time band

### Feature 4.4 — Slot recommendation explorer

* [ ] Build slot-level recommendation view

  * [ ] Show slot metadata
  * [ ] Show underbooked status
  * [ ] Show severity score
  * [ ] Show recommended discount
  * [ ] Show implied price
  * [ ] Show rationale codes
  * [ ] Show exploration flag
* [ ] Add filtering and sorting

  * [ ] Sort by severity
  * [ ] Sort by discount
  * [ ] Filter discounted-only slots
  * [ ] Filter exploratory recommendations
* [ ] Validate explorer usefulness

  * [ ] Confirm rows match `pricing_actions`
  * [ ] Confirm all key fields render correctly

### Feature 4.5 — Cohort diagnostics and recommendation summaries

* [ ] Build cohort diagnostics view

  * [ ] Selected slot vs cohort expected pace
  * [ ] Selected slot vs expected fill
  * [ ] Severity score distribution
* [ ] Build recommendation summary view

  * [ ] Counts by action bucket
  * [ ] Counts by service type
  * [ ] Counts by provider
  * [ ] Counts by lead-time band
* [ ] Add scenario comparison support if feasible

  * [ ] Compare runs side by side
  * [ ] Compare action distributions
  * [ ] Compare flag rates
* [ ] Validate summary accuracy

  * [ ] Reconcile counts with DuckDB queries
  * [ ] Ensure filters affect summaries correctly

### Feature 4.6 — Evaluation suite

* [ ] Implement pipeline validation checks

  * [ ] Confirm all core tables populated
  * [ ] Confirm feature table populated
  * [ ] Confirm scoring table populated
  * [ ] Confirm pricing actions populated
* [ ] Implement underbooking validation checks

  * [ ] Measure underbooked flag rate
  * [ ] Verify known low-demand scenarios are flagged
  * [ ] Verify healthy scenarios are not over-flagged
* [ ] Implement recommendation validation checks

  * [ ] Only eligible underbooked slots receive discounts
  * [ ] Healthy slots receive 0% more often
  * [ ] Larger discounts are associated with larger shortfalls
  * [ ] Rationale coverage is high
* [ ] Implement stability checks

  * [ ] Repeated unchanged runs are deterministic
  * [ ] Similar slots generally receive similar recommendations
  * [ ] Exploration-only differences are explained by seed and policy
* [ ] Persist evaluation outputs

  * [ ] Store metrics in `evaluation_results`
  * [ ] Tag with `run_id` and `scenario_id`

### Feature 4.7 — Final MVP hardening

* [ ] Review all stage interfaces

  * [ ] Remove dead code
  * [ ] Tighten contracts
  * [ ] Improve naming consistency
* [ ] Review idempotency end to end

  * [ ] Rerun full pipeline multiple times
  * [ ] Confirm no duplicate outputs
  * [ ] Confirm stable record counts and keys
* [ ] Review config-driven behavior

  * [ ] Ensure action ladder is configurable
  * [ ] Ensure thresholds are configurable
  * [ ] Ensure cohort bucketing is configurable
* [ ] Review documentation

  * [ ] Add setup instructions
  * [ ] Add run instructions
  * [ ] Add Streamlit launch instructions
  * [ ] Add known limitations
* [ ] Define MVP completion criteria

  * [ ] AI agent can run the full pipeline from config
  * [ ] DuckDB contains all expected outputs
  * [ ] Streamlit shows recommendations and diagnostics
  * [ ] Evaluation checks pass at an acceptable level

### Feature 4.8 — Phase 4 validation and exit criteria

* [ ] Run full end-to-end pipeline on at least one standard scenario
* [ ] Run full end-to-end pipeline on at least one intentionally depressed-demand scenario
* [ ] Open Streamlit and inspect outputs manually
* [ ] Review evaluation metrics
* [ ] Verify the following before closing Phase 4

  * [ ] Pipeline runner works end to end
  * [ ] Streamlit is operational
  * [ ] Evaluation suite produces useful metrics
  * [ ] The MVP proves the full pipeline on synthetic data
  * [ ] Underbooked slots are identified
  * [ ] Discounts are recommended coherently
  * [ ] Outputs are explainable and stable

---

# Cross-Phase Rules the AI Agent Must Follow

## Idempotency rules

* [ ] Never append blindly to a materialized output table
* [ ] Always write with replace, merge, or scoped delete-and-insert semantics
* [ ] Always use stable internal IDs
* [ ] Always attach `run_id`, `scenario_id`, and version metadata
* [ ] Always seed randomness used for exploration

## Implementation discipline rules

* [ ] Do not build UI-only logic into the pipeline
* [ ] Do not put heavy computation inside Streamlit
* [ ] Do not couple internal schema to Medscheduler semantics
* [ ] Do not introduce orchestration infrastructure in the MVP
* [ ] Keep action representation generic, even though V1 only uses discounts

## Testing rules

* [ ] Add tests as each feature is implemented
* [ ] Validate one feature before moving to the next
* [ ] Re-run the full pipeline at every phase boundary
* [ ] Record known gaps rather than silently skipping them

# Suggested AI execution order inside each phase

* [ ] Implement schema/contracts first
* [ ] Implement the write path second
* [ ] Implement validations/tests third
* [ ] Run the stage end to end fourth
* [ ] Only then move to the next feature
