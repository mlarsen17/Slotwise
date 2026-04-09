# agents.md

## Purpose

This file defines how AI coding agents should operate in this repository.

The project is an MVP for a **demand-aware pricing engine for underbooked appointment slots**. The MVP is **simulation-first** and **analytics-only**. It uses **synthetic appointment data**, identifies underbooked slots relative to contextual baselines, and recommends discounts from a fixed ladder. It does **not** write prices back to external schedulers in the MVP. :contentReference[oaicite:0]{index=0} :contentReference[oaicite:1]{index=1}

Agents should optimize for:
- correctness
- clarity
- deterministic behavior
- idempotent pipeline stages
- testability
- minimal operational complexity
- architectural consistency with the MVP design

---

## Product context

The system exists to prove four things:
1. the end-to-end pipeline works on synthetic data
2. the system identifies underbooked slots
3. the system recommends discounts for eligible underbooked slots
4. the outputs are explainable and operationally sensible :contentReference[oaicite:2]{index=2}

The MVP is intentionally narrow:
- synthetic data only
- recommendation and analytics only
- no live scheduler write-back
- no real-time orchestration
- no production-grade online learning :contentReference[oaicite:3]{index=3} :contentReference[oaicite:4]{index=4}

---

## Non-negotiable architecture decisions

Agents must preserve these decisions unless explicitly instructed to revise the architecture:

- **Synthetic data source:** Medscheduler, wrapped behind an internal normalization layer
- **Analytical store:** DuckDB
- **Analytics UI:** Streamlit
- **Pipeline execution model:** plain Python runner, not Airflow / Dagster / Prefect
- **All stages must be idempotent** :contentReference[oaicite:5]{index=5}

Do not introduce:
- a workflow orchestration platform
- a production microservice architecture
- external infrastructure dependencies unless they are clearly necessary
- scheduler-specific assumptions inside core pricing logic

Keep Medscheduler-specific logic isolated in the wrapper boundary. :contentReference[oaicite:6]{index=6} :contentReference[oaicite:7]{index=7}

---

## Repository operating principles for agents

### 1. Respect the MVP boundary
Do not expand scope casually. Prefer finishing the analytics-only synthetic MVP before proposing:
- real scheduler integrations
- live pricing execution
- customer-facing UX
- contextual bandits
- advanced personalization
- production deployment features

### 2. Keep components loosely coupled
Use clear boundaries between:
- synthetic data wrapper
- normalized storage
- feature generation
- underbooking detection
- scoring
- optimization
- rationale generation
- analytics UI

### 3. Prefer simple, inspectable systems
Favor:
- straightforward Python modules
- explicit SQL
- materialized DuckDB tables
- typed configuration
- deterministic functions

Avoid needless abstraction layers.

### 4. Make every stage rerunnable
A stage must be safe to rerun with the same inputs and config without duplicating or corrupting outputs. This is a hard requirement. :contentReference[oaicite:8]{index=8} :contentReference[oaicite:9]{index=9}

### 5. Design for future extension without overbuilding
Represent recommendations as **generic actions** rather than hardcoding discount-only semantics everywhere, because future incentive types may be added later. :contentReference[oaicite:10]{index=10} :contentReference[oaicite:11]{index=11}

### 6. Required local validation checks
Before considering implementation work complete, agents must run all of the following checks:
- `pytest`
- `ruff check .`
- `black --check .`

Enforcement rules for every coding turn:
- Treat these checks as mandatory and blocking.
- Run all three commands after code changes and before commit.
- If any check fails, fix the issue and rerun the full set until all pass.
- Do not open a PR, report completion, or claim success without showing these checks were executed.
- If a command cannot run due environment limitations, clearly report the exact failure and why it was not fixable locally.

---

## Expected codebase shape

Agents should generally keep code aligned with this structure unless there is a strong reason to change it:

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
