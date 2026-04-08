Below is a detailed implementation checklist designed for an AI agent to execute against and track progress. It is aligned with the product concept, MVP design, and TDD: a simulation-first, analytics-only pricing engine using a Medscheduler wrapper, DuckDB, Streamlit, a plain Python pipeline runner, and idempotent stages.   

# Demand-Aware Pricing Engine MVP

## Step-by-Step Implementation Checklist

## Phase 1 — Data Foundation and Synthetic Environment

### Feature 1.1 — Repository and project scaffolding

* [ ] Create the top-level repository structure

  * [ ] Create `medscheduler_wrapper/`
  * [ ] Create `pipeline/`
  * [ ] Create `pipeline/stages/`
  * [ ] Create `sql/`
  * [ ] Create `models/`
  * [ ] Create `optimizer/`
  * [ ] Create `app/`
  * [ ] Create `config/`
  * [ ] Create `data/`
  * [ ] Create `tests/`
* [ ] Create Python packaging and environment files

  * [ ] Add `pyproject.toml` or `requirements.txt`
  * [ ] Add `.python-version` if needed
  * [ ] Add `.gitignore`
  * [ ] Add `README.md`
* [ ] Define core dependency set

  * [ ] Add `duckdb`
  * [ ] Add `pandas`
  * [ ] Add `pyyaml`
  * [ ] Add `pydantic`
  * [ ] Add `streamlit`
  * [ ] Add `scikit-learn`
  * [ ] Add `pytest`
* [ ] Establish coding conventions

  * [ ] Choose formatting tool
  * [ ] Choose linting tool
  * [ ] Decide logging format
  * [ ] Decide config loading pattern

### Feature 1.2 — Core configuration and run model

* [ ] Create a default config file for the MVP

  * [ ] Add DuckDB path
  * [ ] Add scenario identifiers
  * [ ] Add random seed
  * [ ] Add effective timestamp for deterministic runs
  * [ ] Add action ladder configuration
  * [ ] Add lead-time windows
  * [ ] Add global discount limits
* [ ] Define a deterministic run identity model

  * [ ] Create `run_id` format
  * [ ] Create `scenario_id` format
  * [ ] Create `feature_snapshot_version` format
  * [ ] Create `model_version` format
* [ ] Build shared config loader

  * [ ] Validate config schema on startup
  * [ ] Fail fast on missing required fields
  * [ ] Normalize paths and defaults
  * [ ] Surface config values to logs at run start

### Feature 1.3 — Medscheduler wrapper

* [ ] Define the wrapper interface

  * [ ] Create a scenario configuration class
  * [ ] Create extract interface for slots
  * [ ] Create extract interface for booking lifecycle events
  * [ ] Create normalization interface for internal records
* [ ] Implement scenario setup support

  * [ ] Represent businesses
  * [ ] Represent providers
  * [ ] Represent services
  * [ ] Represent locations
  * [ ] Represent customers
  * [ ] Represent scenario knobs such as demand shifts and cancellation behavior
* [ ] Implement extraction logic

  * [ ] Extract raw slot records from Medscheduler
  * [ ] Extract booking lifecycle records from Medscheduler
  * [ ] Preserve source IDs and timestamps
  * [ ] Capture enough information to reconstruct bookable windows
* [ ] Implement normalization logic

  * [ ] Map source slot records into internal slot schema
  * [ ] Map source booking records into internal booking event schema
  * [ ] Generate stable internal IDs
  * [ ] Attach `source_system`, `source_run_id`, and `scenario_id`
* [ ] Make extraction rerunnable

  * [ ] Ensure extraction does not append duplicate raw records on rerun
  * [ ] Ensure normalization produces the same internal IDs for the same source inputs
  * [ ] Add source-level integrity checks

### Feature 1.4 — DuckDB database initialization

* [ ] Create database bootstrap module

  * [ ] Open or create the DuckDB file
  * [ ] Create required schemas or naming conventions
  * [ ] Register initialization logs
* [ ] Implement core table DDL

  * [ ] Create `slots`
  * [ ] Create `booking_events`
  * [ ] Create `pricing_actions`
* [ ] Implement supporting table DDL

  * [ ] Create `businesses`
  * [ ] Create `providers`
  * [ ] Create `feature_snapshots`
  * [ ] Create `cohort_baselines`
  * [ ] Create `optimizer_configs`
  * [ ] Create `scoring_outputs`
  * [ ] Create `evaluation_results`
* [ ] Add indexes or optimization choices only if necessary for DuckDB MVP

  * [ ] Avoid premature optimization
  * [ ] Prefer materialized tables and views

### Feature 1.5 — Data loading pipeline

* [ ] Implement raw-to-core load stage

  * [ ] Load normalized businesses into DuckDB
  * [ ] Load normalized providers into DuckDB
  * [ ] Load normalized slots into DuckDB
  * [ ] Load normalized booking events into DuckDB
* [ ] Enforce idempotent writes

  * [ ] Use replace-or-merge patterns
  * [ ] Delete scoped records by `run_id` or `scenario_id` before insert where appropriate
  * [ ] Ensure no duplicate primary keys
* [ ] Add row-count validation

  * [ ] Validate expected slot count
  * [ ] Validate expected booking event count
  * [ ] Validate required fields are populated
  * [ ] Fail on nulls in mandatory keys

### Feature 1.6 — Availability window logic

* [ ] Implement canonical bookable window computation

  * [ ] Derive `visible_at`
  * [ ] Derive `unavailable_at`
  * [ ] Ensure `unavailable_at` is the earliest of booked, removed, start, or expiration events
* [ ] Validate bookable window correctness

  * [ ] Verify `visible_at <= unavailable_at`
  * [ ] Flag malformed slot histories
  * [ ] Add tests for booked-before-start
  * [ ] Add tests for never-booked slots
  * [ ] Add tests for blocked or removed slots
* [ ] Persist final slot status

  * [ ] Set `current_status`
  * [ ] Set derived status if missing from source
  * [ ] Document status enum assumptions

### Feature 1.7 — Phase 1 validation and exit criteria

* [ ] Add tests for database bootstrap
* [ ] Add tests for normalization determinism
* [ ] Add tests for stable internal IDs
* [ ] Add tests for idempotent loading
* [ ] Add a sample end-to-end run on one scenario
* [ ] Verify the following before closing Phase 1

  * [ ] DuckDB initializes cleanly
  * [ ] Medscheduler wrapper extracts and normalizes data
  * [ ] Core tables are populated
  * [ ] Availability logic works
  * [ ] Rerunning Phase 1 does not duplicate data

---

## Phase 2 — Baselines, Features, and Underbooking Detection

### Feature 2.1 — Cohort baseline framework

* [ ] Define MVP cohort dimensions

  * [ ] Day of week
  * [ ] Time-of-day bucket
  * [ ] Service type
* [ ] Implement time-of-day bucketing

  * [ ] Define bucket boundaries in config
  * [ ] Compute bucket for each slot
  * [ ] Test edge cases around bucket boundaries
* [ ] Build cohort baseline SQL models

  * [ ] Compute historical fill rate by cohort
  * [ ] Compute expected booking pace by cohort
  * [ ] Compute average booking lead time by cohort
  * [ ] Compute realized completion rate by cohort
* [ ] Persist cohort baselines

  * [ ] Write to `cohort_baselines`
  * [ ] Attach `feature_snapshot_version`
  * [ ] Attach `run_id` and `scenario_id`
* [ ] Validate cohort quality

  * [ ] Check for cohorts with too few observations
  * [ ] Decide fallback behavior for sparse cohorts
  * [ ] Log sparse cohort counts

### Feature 2.2 — Feature materialization framework

* [ ] Define feature snapshot contract

  * [ ] Primary key should include `internal_slot_id` and `feature_snapshot_version`
  * [ ] Every feature row should carry `run_id` and `scenario_id`
  * [ ] Feature generation must use deterministic effective time
* [ ] Implement feature table creation pattern

  * [ ] Use `CREATE OR REPLACE TABLE` or equivalent idempotent write pattern
  * [ ] Separate intermediate SQL models from final materialized feature table
* [ ] Create shared feature generation utilities

  * [ ] Time delta calculations
  * [ ] Null handling
  * [ ] Safe rate computation
  * [ ] Cohort joins
  * [ ] Fallback logic for sparse data

### Feature 2.3 — Slot demand history features

* [ ] Implement historical fill features

  * [ ] Historical fill rate for similar slots
  * [ ] Same-provider same-service fill rate
  * [ ] Same-business same-service trailing fill
* [ ] Implement booking pace features

  * [ ] Expected booking pace for similar slots
  * [ ] Observed booking pace for current slot state
  * [ ] Pace deviation from cohort baseline
* [ ] Implement lead-time features

  * [ ] Average lead time to booking for cohort
  * [ ] Hours until slot
  * [ ] Days until slot
* [ ] Validate history features

  * [ ] Confirm rates fall within valid ranges
  * [ ] Confirm no divide-by-zero behavior
  * [ ] Confirm fallback logic for sparse history

### Feature 2.4 — Temporal and scarcity features

* [ ] Implement temporal features

  * [ ] Day of week
  * [ ] Time-of-day bucket
  * [ ] Slot duration
  * [ ] Effective lead-time band
* [ ] Implement scarcity features

  * [ ] Remaining slots for same provider on same day
  * [ ] Remaining similar-service slots in comparable time window
  * [ ] Inventory density around slot start
* [ ] Validate scarcity features

  * [ ] Confirm current slot is excluded or included consistently in counts
  * [ ] Confirm same-day counts are correct
  * [ ] Confirm window definitions are stable and documented

### Feature 2.5 — Operational features

* [ ] Implement provider utilization features

  * [ ] Trailing 7-day utilization
  * [ ] Trailing 14-day utilization
  * [ ] Trailing 28-day utilization
* [ ] Implement booking volume features

  * [ ] Trailing 7-day bookings
  * [ ] Trailing 14-day bookings
  * [ ] Trailing 28-day bookings
* [ ] Implement disruption features

  * [ ] Cancellation rate by slot pattern
  * [ ] No-show rate by slot pattern
  * [ ] Reschedule rate by slot pattern
* [ ] Implement business trend features

  * [ ] Business-level fill trend
  * [ ] Business-level booking trend
* [ ] Validate operational features

  * [ ] Check valid ranges
  * [ ] Check sparse-history fallback
  * [ ] Check window boundaries

### Feature 2.6 — Underbooking detection logic

* [ ] Define underbooking detection inputs

  * [ ] Cohort expected pace
  * [ ] Observed pace
  * [ ] Cohort expected fill
  * [ ] Predicted or baseline fill by start
* [ ] Implement pace gap calculation

  * [ ] Compute observed pace
  * [ ] Compute cohort expected pace at comparable lead time
  * [ ] Compute normalized pace gap
* [ ] Implement fill gap calculation

  * [ ] Compute expected fill baseline
  * [ ] Compute slot-level projected fill
  * [ ] Compute normalized fill gap
* [ ] Implement severity score

  * [ ] Define initial weighted formula
  * [ ] Make thresholds configurable
  * [ ] Clamp score to valid range
* [ ] Implement underbooked classification

  * [ ] Set boolean `underbooked`
  * [ ] Set `severity_score`
  * [ ] Attach detection reason fields for debugging
* [ ] Persist underbooking outputs

  * [ ] Create or update a dedicated table or view
  * [ ] Join back to feature snapshot keys
* [ ] Validate underbooking outputs

  * [ ] Ensure known low-demand scenarios are flagged
  * [ ] Ensure healthy slots are not overwhelmingly flagged
  * [ ] Review severity score distribution

### Feature 2.7 — Phase 2 validation and exit criteria

* [ ] Add SQL or Python tests for cohort baselines
* [ ] Add tests for feature materialization idempotency
* [ ] Add tests for time-of-day bucket assignment
* [ ] Add tests for severity score bounds
* [ ] Run one scenario with intentionally depressed demand
* [ ] Verify the following before closing Phase 2

  * [ ] Cohort baselines are computed
  * [ ] Feature snapshots materialize cleanly
  * [ ] Underbooking flags are produced
  * [ ] Severity scores look directionally correct
  * [ ] Reruns do not create duplicate outputs

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

  * [ ] Create `scoring_outputs`
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
