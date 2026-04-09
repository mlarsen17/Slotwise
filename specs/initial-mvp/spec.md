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

## Phase 3 — Scoring, Optimization, Explanations, and Exploration

### Feature 3.1 — Scoring data contract

* [ ] Define the model input dataset

  * [ ] Select final feature columns
  * [ ] Define target label for MVP scoring
  * [ ] Exclude leakage fields
  * [ ] Freeze feature ordering and names
* [ ] Define scoring output contract

  * [ ] `booking_probability`
  * [ ] `predicted_fill_by_start`
  * [ ] `shortfall_score`
  * [ ] `confidence_score`
  * [ ] `model_version`
* [ ] Persist scoring outputs

  * [x] Create `scoring_outputs`
  * [ ] Key by `internal_slot_id`, `run_id`, `feature_snapshot_version`

### Feature 3.2 — Pooled demand scoring model

* [ ] Build initial training dataset

  * [ ] Pull feature snapshots
  * [ ] Join labels from booking outcomes
  * [ ] Filter invalid or incomplete training examples
* [ ] Implement baseline model training

  * [ ] Start with logistic regression or similar lightweight model
  * [ ] Add train/validation split
  * [ ] Train on pooled data across businesses
  * [ ] Persist model artifact and metadata
* [ ] Implement scoring logic

  * [ ] Load model artifact
  * [ ] Score current eligible slots
  * [ ] Write outputs to `scoring_outputs`
* [ ] Validate scoring quality

  * [ ] Inspect score distributions
  * [ ] Confirm higher-risk slots have lower booking probability
  * [ ] Confirm outputs are stable on rerun

### Feature 3.3 — Business-level calibration

* [ ] Define business calibration strategy

  * [ ] Choose adjustment based on business baseline fill or residual trend
  * [ ] Keep it simple and transparent for MVP
* [ ] Implement calibration calculation

  * [ ] Compute business-level adjustment factors
  * [ ] Version and persist the factors
* [ ] Apply calibration to pooled outputs

  * [ ] Adjust booking probability or shortfall score
  * [ ] Clamp calibrated outputs to valid ranges
* [ ] Validate calibration behavior

  * [ ] Confirm business-specific differences are reflected
  * [ ] Confirm calibration does not swamp the pooled model
  * [ ] Compare calibrated vs uncalibrated outputs

### Feature 3.4 — Optimizer configuration and eligibility rules

* [ ] Implement optimizer config model

  * [ ] Global config
  * [ ] Business config
  * [ ] Provider-level placeholder support
* [ ] Implement fixed action ladder

  * [ ] 0%
  * [ ] 5%
  * [ ] 10%
  * [ ] 15%
  * [ ] 20%
* [ ] Implement eligibility rules

  * [ ] Excluded services cannot be discounted
  * [ ] Discount cannot exceed configured maximum
  * [ ] Discount only allowed within lead-time windows
  * [ ] Discount cannot violate price floor
  * [ ] Healthy slots should usually remain at 0%
* [ ] Implement eligible action set generation

  * [ ] Produce candidate actions per slot
  * [ ] Persist or log `eligible_action_set`
* [ ] Validate eligibility logic

  * [ ] Test premium or excluded service behavior
  * [ ] Test max-discount enforcement
  * [ ] Test price-floor enforcement
  * [ ] Test lead-time window enforcement

### Feature 3.5 — Discount recommendation engine

* [ ] Define recommendation policy

  * [ ] If slot is not underbooked, recommend 0%
  * [ ] If slot is underbooked, map severity and score to best action
* [ ] Implement initial severity-to-action mapping

  * [ ] Add config-driven breakpoints
  * [ ] Allow business overrides where appropriate
* [ ] Compute final implied price

  * [ ] Apply recommended action to standard price
  * [ ] Confirm price floor rules are respected
* [ ] Create recommendation output record

  * [ ] Set `recommended_action_type`
  * [ ] Set `recommended_action_value`
  * [ ] Set `confidence_score`
  * [ ] Set `decision_reason`
* [ ] Validate recommendations

  * [ ] Ensure only eligible actions are selected
  * [ ] Ensure higher severity generally maps to higher discounts
  * [ ] Ensure healthy slots are mostly 0%

### Feature 3.6 — Rationale code engine

* [ ] Define rationale code taxonomy

  * [ ] Historically underbooked weekday afternoon
  * [ ] Booking pace below baseline
  * [ ] Short lead-time low fill
  * [ ] Provider utilization below target
  * [ ] Any other initial codes needed for explainability
* [ ] Implement rule-based rationale generation

  * [ ] Generate codes from feature values
  * [ ] Generate codes from baseline deviations
  * [ ] Generate codes from optimizer decisions
* [ ] Persist rationale codes

  * [ ] Attach to recommendation output
  * [ ] Store as JSON array in `pricing_actions`
* [ ] Validate rationale quality

  * [ ] Confirm every discounted slot has at least one rationale code
  * [ ] Confirm codes match actual slot context
  * [ ] Eliminate redundant or contradictory codes

### Feature 3.7 — Exploration policy and logging

* [ ] Define exploration policy

  * [ ] Configure exploration share
  * [ ] Restrict exploration to eligible actions
  * [ ] Seed randomness deterministically
* [ ] Implement slot-level exploration override

  * [ ] For eligible slots, randomly choose from allowed actions when exploration applies
  * [ ] Otherwise use the optimizer result
* [ ] Log exploration metadata

  * [ ] `was_exploration`
  * [ ] `exploration_policy`
  * [ ] `decision_reason`
  * [ ] `eligible_action_set`
* [ ] Validate exploration behavior

  * [ ] Confirm exploration fraction is within expected range
  * [ ] Confirm reruns with same seed produce same exploratory choices
  * [ ] Confirm no policy violations occur

### Feature 3.8 — Pricing action persistence

* [ ] Implement final `pricing_actions` write stage

  * [ ] Delete existing records for current `run_id` if needed
  * [ ] Insert final recommendations
  * [ ] Ensure primary keys are stable
* [ ] Enforce idempotency

  * [ ] Reruns replace or reinsert the same logical action rows
  * [ ] No duplicate pricing actions for the same run
* [ ] Validate stored records

  * [ ] Confirm all required fields are populated
  * [ ] Confirm JSON fields serialize correctly
  * [ ] Confirm confidence and rationale values are present

### Feature 3.9 — Phase 3 validation and exit criteria

* [ ] Add model training and scoring tests
* [ ] Add optimizer rule tests
* [ ] Add rationale generation tests
* [ ] Add exploration determinism tests
* [ ] Run a full scenario and inspect pricing actions
* [ ] Verify the following before closing Phase 3

  * [ ] Scoring outputs are generated
  * [ ] Business calibration works
  * [ ] Eligible action sets are correct
  * [ ] Discount recommendations are written
  * [ ] Rationale codes and exploration metadata are present
  * [ ] Reruns remain idempotent

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
