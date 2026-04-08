Below is a complete polished **Technical Design Document (v1)** for the MVP, grounded in your original concept and MVP design, with the architecture decisions we aligned on: Medscheduler-backed synthetic data, DuckDB as the analytical store, Streamlit as the analytics UI, a plain Python pipeline runner instead of orchestration, and idempotent stages throughout.  

---

# Technical Design Document (v1)

## Demand-Aware Pricing Engine for Underbooked Appointment Slots

## 1. Document Purpose

This document defines the technical design for the MVP of a demand-aware pricing engine for appointment-based businesses. The MVP is intended to prove that the system can ingest synthetic appointment data, identify underbooked slots relative to appropriate baselines, and generate coherent, explainable discount recommendations. The MVP is analytics-only and does not write prices back to an external scheduler. 

This document translates the MVP product design into an implementation-ready technical architecture.

---

## 2. Product and MVP Context

The broader product vision is a demand-aware pricing engine that predicts unbooked appointment slots and applies profit-optimized discounts to maximize utilization and revenue. For the MVP, that long-term vision is intentionally narrowed into a simulation-first recommendation system. The system focuses on discount recommendation quality and explainability, not live price execution.  

The MVP must prove four things:

1. the end-to-end pipeline works on synthetic data
2. the system identifies underbooked slots
3. the system recommends discounts for eligible underbooked slots
4. the outputs are explainable and operationally sensible 

---

## 3. Scope

### In scope

The MVP will:

* operate on synthetic data in a simulated environment
* model appointment slot inventory and booking behavior
* score slots for likelihood of underbooking
* recommend a discount from a fixed ladder
* expose rationale codes for trust and debugging
* support controlled randomized recommendations for experimentation
* provide analytics output only 

### Out of scope

The MVP will not:

* write back prices to schedulers
* change live customer-facing prices
* require real-time integrations
* perform true causal learning from production discount exposures 

---

## 4. Core Technical Decisions

### 4.1 Synthetic data source

The synthetic data environment will be powered by **Medscheduler**, with a wrapper layer built around it. This wrapper is responsible for configuring scenarios, extracting generated scheduling and booking data, and normalizing that data into the pricing engine’s internal schema.

This replaces a bespoke synthetic-world simulator. The pricing engine remains scheduler-agnostic internally even though Medscheduler is the MVP’s synthetic data source.

### 4.2 Analytical data store

The MVP will use **DuckDB** as the primary analytical store. DuckDB will hold normalized slot data, booking events, feature tables, cohort baselines, recommendation outputs, and evaluation artifacts.

### 4.3 Analytics UI

The MVP will use **Streamlit** as the internal analytics and inspection surface. Streamlit will read directly from DuckDB and provide slot-level recommendation inspection, cohort diagnostics, and recommendation summaries.

### 4.4 Pipeline execution model

The MVP will use a **plain Python pipeline runner** rather than an orchestration engine. The pipeline is linear, batch-oriented, and small enough that Airflow, Dagster, or Prefect would add overhead without enough return at this stage.

### 4.5 Idempotency requirement

All pipeline stages must be **idempotent**. Running the same stage multiple times with the same inputs must produce the same outputs without duplicating or corrupting state. This is a hard design requirement and a substitute for orchestration-era operational safeguards.

---

## 5. Goals and Non-Goals

### Goals

* Prove the end-to-end analytical pipeline
* Detect underbooked appointment slots using contextual baselines
* Generate discount recommendations from a fixed ladder
* Provide clear rationale codes
* Support repeatable experimentation using synthetic scenarios
* Keep the system simple to run, inspect, and evolve

### Non-goals

* Production deployment
* Real-time inference
* Online learning
* Customer-facing UX
* Automated scheduler write-back
* Perfect economic optimization in the MVP

---

## 6. System Overview

The MVP system has five logical layers, consistent with the product design:

1. synthetic data generation
2. core data model and storage
3. feature generation
4. scoring and recommendation
5. analytics output surface 

In implementation terms, the architecture becomes:

```text
[Medscheduler]
    ↓
[Medscheduler Wrapper / Extract-Transform Layer]
    ↓
[DuckDB Raw + Normalized Tables]
    ↓
[Feature and Baseline Computation]
    ↓
[Demand Scoring Service]
    ↓
[Discount Optimization Service]
    ↓
[Pricing Actions Table]
    ↓
[Streamlit Analytics UI]
```

---

## 7. System Components

## 7.1 Medscheduler wrapper

### Responsibilities

The wrapper is the synthetic data system boundary. It has three responsibilities:

1. **Scenario setup**

   * configure synthetic businesses, providers, services, locations, schedules, and customers
   * define scenario knobs such as lead-time patterns, demand depressions, cancellation rates, provider differences

2. **Data extraction**

   * read Medscheduler-generated slots and booking lifecycle records
   * extract enough metadata to reconstruct slot availability and booking history

3. **Normalization**

   * map Medscheduler outputs into internal scheduler-agnostic tables
   * generate stable internal IDs
   * enforce canonical event semantics

### Design principles

* internal schema must not depend on Medscheduler naming or object model
* extraction must be rerunnable
* scenario generation should be reproducible through config and seeds
* wrapper should support scenario labels for evaluation

---

## 7.2 DuckDB analytical store

DuckDB is the canonical analytical store for the MVP.

### Responsibilities

* store normalized raw data
* store derived feature tables
* store cohort baselines
* store pricing recommendations
* support evaluation queries
* serve Streamlit queries directly

### Why DuckDB fits

DuckDB is appropriate because the MVP workload is offline analytics-heavy:

* batch ingestion
* SQL transformations
* cohort aggregations
* model input assembly
* recommendation inspection
* scenario evaluation

It also minimizes infrastructure while preserving future migration flexibility.

---

## 7.3 Feature computation layer

This layer computes derived slot-level features and baseline aggregates from the normalized DuckDB tables. Feature computation is batch-based and recomputed periodically, which is aligned with the MVP assumption that recommendations are stable outputs rather than dynamically changing live prices. 

### Implementation style

* SQL-first where practical
* Python for feature assembly where SQL is awkward
* materialized outputs written back to DuckDB
* deterministic and idempotent transformations

---

## 7.4 Demand scoring service

This service estimates how likely a slot is to remain underfilled or underbooked relative to peers.

### MVP model design

The scoring layer follows the intended hybrid approach:

* pooled global model across synthetic businesses
* business-level calibration factors
* slot-level feature inputs 

The output is not a final discount. It is an intermediate demand or shortfall signal used by the optimizer.

---

## 7.5 Discount optimization service

This service chooses the recommended action from the fixed action ladder:

* 0%
* 5%
* 10%
* 15%
* 20% 

The optimizer is rules-based plus model-informed in the MVP. It receives the demand score, applies eligibility constraints, and returns the best allowed action. The design uses generic “actions” rather than hardcoding discount-only logic so that future incentive types can be added later. 

---

## 7.6 Rationale engine

This component turns the slot state and recommendation context into human-readable rationale codes. These explanations are rule-derived rather than opaque model explainability outputs.

Examples include:

* historically_underbooked_weekday_afternoon
* booking_pace_below_baseline
* short_lead_time_low_fill
* provider_utilization_below_target 

---

## 7.7 Streamlit analytics UI

Streamlit is the operator-facing surface for the MVP.

### Responsibilities

* view slot recommendations
* inspect underbooked status
* inspect rationale codes
* compare baseline vs current booking pace
* review candidate discounts and suggested action 

### Constraints

* read-only
* internal-facing
* no workflow controls beyond filtering and inspection
* no write-back actions

---

## 8. Data Model

The MVP product design identifies three minimum logical tables: slot inventory, booking events, and pricing actions. Those remain the core of the implementation. 

## 8.1 Core entity model

### Slots

One row per appointment slot offered.

### Booking events

One row per booking lifecycle event.

### Pricing actions

One row per pricing recommendation event.

These are the source-of-truth analytical entities.

---

## 8.2 Identity and tenancy

The system must remain multi-scheduler capable in the long term, so identity is normalized even in the MVP. Customer records use a globally unique internal row identifier while preserving tenant boundaries through business and integration IDs. Slot identity similarly uses an internal platform-generated slot ID and preserves the scheduler’s external slot ID when available. 

### Identity requirements

#### Customer

* `customer_pk`
* `business_id`
* `integration_id`
* `external_customer_id`
* optional future `person_group_id`

#### Slot

* `internal_slot_id`
* `external_slot_id`

No cross-business global person resolution is assumed.

---

## 8.3 Availability model

The canonical bookable window for a slot is:

`visible_at` → `unavailable_at`

Where `unavailable_at` is the earliest of:

* booked time
* blocked or removed time
* slot start time
* expiration time 

This definition matters because fill and pace metrics should be computed against the actual bookable window, not the slot’s full lifecycle.

---

## 8.4 DuckDB schemas

### slots

```sql
CREATE TABLE slots (
  internal_slot_id VARCHAR PRIMARY KEY,
  integration_id VARCHAR,
  external_slot_id VARCHAR,
  business_id VARCHAR NOT NULL,
  provider_id VARCHAR NOT NULL,
  service_type VARCHAR NOT NULL,
  location_id VARCHAR,
  slot_start_at TIMESTAMP NOT NULL,
  slot_end_at TIMESTAMP NOT NULL,
  slot_duration_minutes INTEGER NOT NULL,
  created_at TIMESTAMP,
  visible_at TIMESTAMP,
  unavailable_at TIMESTAMP,
  standard_price DOUBLE,
  current_status VARCHAR NOT NULL,
  scenario_id VARCHAR,
  source_system VARCHAR DEFAULT 'medscheduler',
  source_run_id VARCHAR,
  inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### booking_events

```sql
CREATE TABLE booking_events (
  booking_event_id VARCHAR PRIMARY KEY,
  internal_slot_id VARCHAR NOT NULL,
  internal_customer_id VARCHAR,
  event_type VARCHAR NOT NULL,
  event_at TIMESTAMP NOT NULL,
  business_id VARCHAR NOT NULL,
  provider_id VARCHAR NOT NULL,
  service_type VARCHAR NOT NULL,
  scenario_id VARCHAR,
  source_system VARCHAR DEFAULT 'medscheduler',
  source_run_id VARCHAR,
  inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### pricing_actions

```sql
CREATE TABLE pricing_actions (
  pricing_action_id VARCHAR PRIMARY KEY,
  internal_slot_id VARCHAR NOT NULL,
  business_id VARCHAR NOT NULL,
  provider_id VARCHAR NOT NULL,
  recommended_action_type VARCHAR NOT NULL,
  recommended_action_value DOUBLE NOT NULL,
  eligible_action_set JSON,
  decision_reason VARCHAR,
  was_exploration BOOLEAN NOT NULL,
  exploration_policy VARCHAR,
  decision_timestamp TIMESTAMP NOT NULL,
  feature_snapshot_version VARCHAR NOT NULL,
  confidence_score DOUBLE,
  rationale_codes JSON,
  run_id VARCHAR NOT NULL,
  scenario_id VARCHAR,
  inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Optional supporting tables

#### businesses

configuration and metadata per tenant

#### providers

provider metadata and service mappings

#### feature_snapshots

materialized feature sets by run

#### cohort_baselines

expected pace and fill statistics by cohort and lead-time bucket

#### optimizer_configs

global, business, and later provider-level rules

---

## 9. Feature Design

The MVP design specifies three feature groups: slot demand history, temporal and scarcity signals, and operational signals. That structure is retained here. 

## 9.1 Baseline cohort definition

For MVP, “similar slots” are defined by:

* day of week
* time of day bucket
* service type 

These dimensions determine expected fill and booking pace baselines.

### Time-of-day buckets

Recommended initial buckets:

* early_morning
* morning
* midday
* afternoon
* evening

These should be configurable.

---

## 9.2 Feature groups

### Demand history features

* historical fill rate for similar slots
* expected booking pace for similar slots
* average lead time to booking
* same-provider same-service historical fill
* same-business same-service trailing fill
* completed utilization rate for cohort

### Temporal and scarcity signals

* day_of_week
* time_of_day_bucket
* hours_until_slot
* days_until_slot
* slot_duration_minutes
* remaining slots for provider on same day
* remaining similar-service slots in same lead-time band
* inventory density around slot start time

### Operational signals

* provider utilization trend
* trailing 7/14/28 day bookings
* cancellation rate by slot pattern
* no-show rate by slot pattern
* reschedule rate by slot pattern
* business-level fill trend

### Price and action context

* standard_price
* minimum_allowed_price
* max_discount_allowed
* eligible_action_count
* discounted_recently_for_cohort flag

---

## 9.3 Feature materialization strategy

Create materialized feature tables keyed by `internal_slot_id` and `feature_snapshot_version`.

Example:

```sql
CREATE OR REPLACE TABLE feature_snapshots AS
SELECT
  s.internal_slot_id,
  s.business_id,
  s.provider_id,
  s.service_type,
  EXTRACT(DOW FROM s.slot_start_at) AS day_of_week,
  CASE
    WHEN EXTRACT(HOUR FROM s.slot_start_at) < 9 THEN 'early_morning'
    WHEN EXTRACT(HOUR FROM s.slot_start_at) < 12 THEN 'morning'
    WHEN EXTRACT(HOUR FROM s.slot_start_at) < 15 THEN 'midday'
    WHEN EXTRACT(HOUR FROM s.slot_start_at) < 18 THEN 'afternoon'
    ELSE 'evening'
  END AS time_of_day_bucket,
  DATE_DIFF('hour', CURRENT_TIMESTAMP, s.slot_start_at) AS hours_until_slot,
  s.standard_price
FROM slots s;
```

In practice, feature jobs should operate against a deterministic effective timestamp rather than wall-clock `CURRENT_TIMESTAMP`, to preserve rerun reproducibility.

---

## 10. Underbooking Detection

The product definition says a slot is underbooked when its current booking pace or expected fill is materially below the baseline for similar slots, not below a naive fixed threshold. 

That principle is central.

## 10.1 Detection objective

Identify slots whose current state indicates they are likely to underperform relative to comparable slots before appointment time.

## 10.2 Detection logic

A slot can be flagged underbooked if any of the following hold:

1. current observed booking pace is materially below cohort pace at comparable lead time
2. expected fill by slot start is materially below cohort expected fill
3. provider- or business-calibrated shortfall score exceeds threshold

## 10.3 Severity score

Recommend a continuous severity score in the range 0.0 to 1.0:

* 0.0 = healthy
* 1.0 = severe underbooking risk

This allows downstream action mapping without forcing a hard binary classification too early.

## 10.4 Example formula

```text
pace_gap = cohort_expected_pace_at_lead_time - observed_pace
fill_gap = cohort_expected_fill_by_start - predicted_fill_by_start

severity_score =
  w1 * normalized(pace_gap) +
  w2 * normalized(fill_gap) +
  w3 * business_calibration_adjustment
```

Thresholds should be configuration-driven.

---

## 11. Demand Scoring Design

## 11.1 Purpose

Estimate slot-level shortfall or booking probability so the optimizer can decide whether discounting is warranted.

## 11.2 Recommended MVP approach

Start with a lightweight model:

* logistic regression
* gradient boosted trees
* or a calibrated heuristic model if data volume is initially limited

The MVP design explicitly supports a pooled global model plus business-level calibration. That is the preferred approach because it balances cold-start behavior and tenant-specific adjustment. 

## 11.3 Model inputs

* slot feature snapshot
* cohort baseline deltas
* provider and business utilization trends
* slot availability and lead-time signals
* price and configuration constraints

## 11.4 Model outputs

Recommended outputs:

* `booking_probability`
* `predicted_fill_by_start`
* `shortfall_score`
* `confidence_score`

These should be written to an intermediate scoring table or joined into the optimizer input frame.

---

## 12. Discount Optimization Design

## 12.1 Objective

Choose the best allowed recommendation from the fixed ladder while respecting business rules and safety constraints.

## 12.2 Action ladder

Allowed actions for V1:

* 0%
* 5%
* 10%
* 15%
* 20% 

Even though these are discount percentages, represent them internally as generic action types and action values.

## 12.3 Configuration hierarchy

Configuration precedence:

`provider > business > global`

with hard global safety limits that cannot be overridden. For the MVP, build global and business-level controls and design the schema to support provider-level controls later. 

## 12.4 Eligibility rules

Examples:

* never discount excluded or premium services
* never exceed configured max discount
* only discount slots within configured lead-time windows
* respect minimum price floors
* do not discount healthy slots already above expected fill threshold 

## 12.5 Decision policy

MVP recommendation logic:

1. compute eligible actions
2. if slot is not underbooked, recommend 0%
3. if slot is underbooked, map severity and model outputs to best action
4. optionally override with exploration policy
5. emit rationale codes and confidence score

## 12.6 Example mapping

```text
severity < 0.10  -> 0%
0.10–0.25        -> 5%
0.25–0.45        -> 10%
0.45–0.65        -> 15%
> 0.65           -> 20%
```

This is a starting heuristic and should be made configurable.

---

## 13. Exploration Strategy

The MVP design calls for controlled randomized recommendations, with slot-level exploration and explicit logging. 

## 13.1 Purpose

* avoid fully deterministic overexploitation
* support future learning
* create varied recommendation traces for evaluation

## 13.2 Mechanics

For a configured fraction of eligible slots:

* choose a random action from `eligible_action_set`
* respect all policy limits
* record that the decision was exploratory

For the remaining slots:

* choose the best predicted action

## 13.3 Required logging

* `was_exploration`
* `exploration_policy`
* `decision_reason`
* `eligible_action_set` 

## 13.4 Reproducibility

Exploration must be seeded and deterministic per run. Randomness without seeding is not allowed because it breaks idempotent evaluation and debugging.

---

## 14. Outcome Model

The MVP design distinguishes between conversion and realization. That distinction should be preserved in the data model even if recommendation logic initially focuses on booking conversion risk. 

### Definitions

* booked then completed = success
* booked then cancelled = conversion but not realized utilization
* booked then no-show = conversion but not realized utilization
* never booked = failure 

This is important because future optimization may shift from pure conversion to realized utilization or contribution margin.

---

## 15. Internal API Shape

The MVP design suggests an internal API shape centered on a `POST /price-slot` operation. 

For the MVP implementation, this can exist as either:

* a Python function boundary
* a local FastAPI endpoint for testing
* or both

## 15.1 Request

```json
{
  "service": "haircut",
  "provider": "provider_123",
  "location": "location_1",
  "slot_datetime": "2026-04-10T14:00:00",
  "lead_time_hours": 18,
  "current_occupancy": 0.15,
  "standard_price": 100.0
}
```

## 15.2 Response

```json
{
  "recommended_action_type": "percentage_discount",
  "recommended_action_value": 10,
  "final_implied_price": 90.0,
  "confidence_score": 0.78,
  "underbooked": true,
  "severity_score": 0.46,
  "rationale_codes": [
    "booking_pace_below_baseline",
    "short_lead_time_low_fill"
  ]
}
```

---

## 16. Pipeline Design

## 16.1 Execution model

The MVP uses a plain Python runner, for example:

```bash
python run_pipeline.py
```

This runner executes stages sequentially.

## 16.2 Stages

1. Medscheduler extraction and scenario normalization
2. raw table load into DuckDB
3. baseline cohort computation
4. feature computation
5. underbooking detection
6. demand scoring
7. discount optimization
8. pricing action write
9. evaluation artifact generation
10. Streamlit reads latest outputs

## 16.3 Module structure

Recommended repository structure:

```text
mvp/
  medscheduler_wrapper/
    scenario_config.py
    extract.py
    normalize.py

  pipeline/
    run_pipeline.py
    stages/
      extract_stage.py
      load_stage.py
      baseline_stage.py
      feature_stage.py
      underbooking_stage.py
      scoring_stage.py
      optimize_stage.py
      evaluate_stage.py

  sql/
    base_slots.sql
    base_booking_events.sql
    cohort_baselines.sql
    feature_snapshots.sql
    underbooking.sql

  models/
    demand_scoring.py
    calibration.py

  optimizer/
    eligibility.py
    recommend.py
    rationale.py
    exploration.py

  app/
    streamlit_app.py

  data/
    mvp.duckdb

  config/
    default.yaml
```

---

## 17. Idempotency Design

Idempotency is mandatory.

## 17.1 Requirements

Running the same stage twice with the same source inputs and config must not:

* duplicate rows
* alter previously correct outputs unexpectedly
* produce different outputs unless seeded randomness or inputs changed

## 17.2 Techniques

### Stable run identity

Every pipeline invocation should have a deterministic `run_id` or explicit user-supplied `run_id`.

### Replace or merge writes

Use `CREATE OR REPLACE TABLE` for fully materialized tables, or delete-and-insert scoped to a run/version.

### Stable keys

Avoid unseeded random UUID generation inside transformations. Internal IDs must be stable or derivable.

### Snapshot versioning

Feature materializations and pricing actions must include `feature_snapshot_version` and `run_id`.

### Seeded exploration

Exploration uses deterministic seeds tied to run and slot.

## 17.3 Example write pattern

```sql
DELETE FROM pricing_actions WHERE run_id = '2026-04-08-scenario-a';
INSERT INTO pricing_actions
SELECT ...
```

---

## 18. Streamlit UI Design

## 18.1 Primary views

### Slot recommendation explorer

* slot metadata
* severity score
* recommended discount
* implied price
* rationale codes
* exploration flag

### Cohort baseline diagnostics

* selected slot vs cohort expected pace
* selected slot vs expected fill
* histogram of severity scores

### Recommendation distribution

* counts by action bucket
* counts by service type
* counts by provider
* counts by lead-time band

### Scenario evaluation dashboard

* flagged underbooked slots by scenario
* false positive and false negative review
* stability metrics between runs

## 18.2 Streamlit data access model

Streamlit should query DuckDB directly using precomputed tables or views. Avoid heavy recomputation in the UI layer.

---

## 19. Evaluation Plan

The MVP design already defines the right evaluation categories: pipeline validation, underbooking detection validation, recommendation validation, and stability checks. 

## 19.1 Pipeline validation

Confirm that:

* synthetic data lands in all core tables
* features are computed successfully
* scores are produced
* recommendations are generated without failures 

## 19.2 Underbooking detection validation

Confirm that:

* intentionally low-demand slots are flagged
* healthy slots are not over-flagged
* booking pace deviations map to underbooked classifications sensibly 

## 19.3 Recommendation validation

Confirm that:

* only eligible underbooked slots receive discounts
* healthier slots receive 0% more often
* larger discounts are reserved for more severe shortfalls
* rationale codes match the observed slot context 

## 19.4 Stability checks

Confirm that:

* similar slots usually receive similar recommendations
* repeated runs with unchanged inputs are deterministic outside exploration 

## 19.5 Recommended MVP metrics

* underbooked flag rate
* discount recommendation rate
* recommendation distribution by ladder step
* cohort-relative pace error
* recommendation consistency score
* percentage of recommendations with at least one rationale code
* exploration share
* successful pipeline run rate

---

## 20. Operational Considerations

## 20.1 Logging

Every stage should emit structured logs containing:

* stage name
* run_id
* scenario_id
* row counts in and out
* elapsed time
* failure reason if applicable

## 20.2 Reproducibility

Store:

* scenario config
* model version
* feature snapshot version
* optimizer config version
* random seed

## 20.3 Error handling

Fail fast on schema mismatches, missing required fields, or inconsistent booking event sequences.

## 20.4 Performance

This MVP is not expected to need complex optimization, but good hygiene still matters:

* keep wide denormalized analytical tables materialized
* use DuckDB views for light joins
* avoid recomputing full history inside Streamlit

---

## 21. Risks and Mitigations

### Risk: synthetic patterns are too clean

Mitigation: inject variability, noise, and scenario perturbations.

### Risk: cohort definitions are too coarse

Mitigation: start with day-of-week, time bucket, and service type, then add provider or location refinements later.

### Risk: optimizer over-discounts

Mitigation: enforce strong eligibility rules, floors, and conservative default mapping.

### Risk: model complexity outruns data realism

Mitigation: begin with simple pooled scoring and calibration.

### Risk: randomness hurts debugging

Mitigation: seed all exploration and log decision context.

### Risk: scheduler coupling leaks into core model

Mitigation: keep Medscheduler-specific logic isolated in the wrapper.

---

## 22. Security and Privacy Assumptions

Because the MVP uses synthetic data, no real customer PII is required. Even so, internal schemas should be designed as though sensitive production data may eventually flow through them:

* avoid hardcoding assumptions that require PII
* use internal identifiers
* keep source extraction boundaries explicit
* separate business tenancy cleanly

---

## 23. Future Evolution

The MVP design deliberately leaves room for:

* live scheduler integrations
* write-back pricing execution
* provider approvals and overrides
* provider-level controls and UI
* non-discount incentives
* contextual bandits or uplift modeling
* customer-level cohorting and personalization
* event-triggered recomputation
* mutable post-publication pricing 

This TDD supports those extensions by:

* keeping core IDs scheduler-agnostic
* representing recommendations as generic actions
* logging recommendation decisions explicitly
* separating scoring from optimization
* requiring deterministic stage boundaries

---

## 24. Recommended Implementation Order

Consistent with the MVP sequencing, implementation should proceed in this order, adjusted for the Medscheduler and DuckDB choices:

### Phase 1

* Medscheduler wrapper
* DuckDB schemas
* normalized slot and booking ingestion
* availability window logic

### Phase 2

* cohort baseline aggregations
* underbooking detection logic
* feature materialization

### Phase 3

* pooled demand scoring model
* business-level calibration
* discount optimizer
* rationale engine
* exploration logging

### Phase 4

* pricing action persistence
* Streamlit analytics dashboard
* evaluation suite on synthetic scenarios

---

## 25. Final MVP Summary

This MVP is a simulation-first, analytics-only pricing engine. It uses Medscheduler as a synthetic scheduling environment, normalizes that data into DuckDB, computes contextual cohort baselines and slot features, scores slots for underbooking risk, recommends discount actions from a fixed ladder, and surfaces those recommendations through a Streamlit analytics UI. It runs through a plain Python pipeline with idempotent stages and is intentionally structured to evolve into a real execution system later. That stays faithful to the MVP design while making the implementation tighter and more concrete. 

---

If you want, the next useful step is to turn this into a **doc-style artifact** with title page, version block, and cleaner formatting for sharing.
