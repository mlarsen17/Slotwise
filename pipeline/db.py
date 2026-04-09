from __future__ import annotations

import logging
from pathlib import Path

import duckdb

LOGGER = logging.getLogger(__name__)


def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(db_path))


def bootstrap_db(conn: duckdb.DuckDBPyConnection) -> None:
    LOGGER.info("Initializing DuckDB schema")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS businesses (
          business_id TEXT PRIMARY KEY,
          source_business_id TEXT,
          name TEXT,
          scenario_id TEXT
        );

        CREATE TABLE IF NOT EXISTS providers (
          provider_id TEXT PRIMARY KEY,
          business_id TEXT,
          location_id TEXT,
          source_provider_id TEXT,
          source_location_id TEXT,
          source_business_id TEXT,
          name TEXT,
          scenario_id TEXT
        );

        CREATE TABLE IF NOT EXISTS services (
          service_id TEXT PRIMARY KEY,
          business_id TEXT,
          source_service_id TEXT,
          source_business_id TEXT,
          name TEXT,
          duration_minutes INTEGER,
          scenario_id TEXT
        );

        CREATE TABLE IF NOT EXISTS locations (
          location_id TEXT PRIMARY KEY,
          business_id TEXT,
          source_location_id TEXT,
          source_business_id TEXT,
          name TEXT,
          scenario_id TEXT
        );

        CREATE TABLE IF NOT EXISTS customers (
          customer_id TEXT PRIMARY KEY,
          business_id TEXT,
          source_customer_id TEXT,
          source_business_id TEXT,
          first_name TEXT,
          last_name TEXT,
          scenario_id TEXT
        );

        CREATE TABLE IF NOT EXISTS slots (
          slot_id TEXT,
          provider_id TEXT,
          business_id TEXT,
          service_id TEXT,
          location_id TEXT,
          standard_price DOUBLE,
          slot_duration_minutes INTEGER,
          integration_id TEXT,
          external_slot_id TEXT,
          source_slot_id TEXT,
          source_provider_id TEXT,
          source_business_id TEXT,
          source_service_id TEXT,
          source_location_id TEXT,
          slot_start_at TIMESTAMP,
          slot_end_at TIMESTAMP,
          visible_at TIMESTAMP,
          unavailable_at TIMESTAMP,
          current_status TEXT,
          created_at TIMESTAMP,
          source_system TEXT,
          source_run_id TEXT,
          scenario_id TEXT,
          run_id TEXT,
          effective_ts TIMESTAMP,
          config_hash TEXT
        );

        CREATE TABLE IF NOT EXISTS booking_events (
          event_id TEXT,
          slot_id TEXT,
          customer_id TEXT,
          business_id TEXT,
          provider_id TEXT,
          service_type TEXT,
          source_event_id TEXT,
          source_slot_id TEXT,
          source_customer_id TEXT,
          event_type TEXT,
          event_at TIMESTAMP,
          source_system TEXT,
          source_run_id TEXT,
          scenario_id TEXT,
          run_id TEXT,
          effective_ts TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS pricing_actions (
          action_id TEXT,
          slot_id TEXT,
          action_type TEXT,
          action_value DOUBLE,
          eligible_action_set TEXT,
          decision_reason TEXT,
          was_exploration BOOLEAN,
          exploration_policy TEXT,
          decision_timestamp TIMESTAMP,
          feature_snapshot_version TEXT,
          confidence_score DOUBLE,
          rationale_codes TEXT,
          run_id TEXT,
          scenario_id TEXT
        );
        
        CREATE TABLE IF NOT EXISTS pipeline_runs (
          run_id TEXT,
          scenario_id TEXT,
          effective_ts TIMESTAMP,
          config_hash TEXT,
          started_at TIMESTAMP,
          status TEXT
        );

        CREATE TABLE IF NOT EXISTS feature_snapshots (
          feature_snapshot_version TEXT,
          slot_id TEXT,
          scenario_id TEXT
        );

        CREATE TABLE IF NOT EXISTS cohort_baselines (
          cohort_id TEXT,
          metric_name TEXT,
          metric_value DOUBLE,
          feature_snapshot_version TEXT,
          run_id TEXT,
          scenario_id TEXT
        );

        CREATE TABLE IF NOT EXISTS optimizer_configs (
          run_id TEXT,
          scenario_id TEXT,
          model_version TEXT,
          config_json TEXT
        );

        CREATE TABLE IF NOT EXISTS scoring_outputs (
          run_id TEXT,
          slot_id TEXT,
          score DOUBLE,
          scenario_id TEXT
        );

        CREATE TABLE IF NOT EXISTS evaluation_results (
          run_id TEXT,
          metric_name TEXT,
          metric_value DOUBLE,
          scenario_id TEXT
        );
        """
    )
    _ensure_columns(conn)
    LOGGER.info("DuckDB schema initialization complete")


def _ensure_columns(conn: duckdb.DuckDBPyConnection) -> None:
    migrations: dict[str, list[str]] = {
        "slots": [
            "ADD COLUMN IF NOT EXISTS standard_price DOUBLE",
            "ADD COLUMN IF NOT EXISTS slot_duration_minutes INTEGER",
            "ADD COLUMN IF NOT EXISTS integration_id TEXT",
            "ADD COLUMN IF NOT EXISTS external_slot_id TEXT",
            "ADD COLUMN IF NOT EXISTS run_id TEXT",
            "ADD COLUMN IF NOT EXISTS effective_ts TIMESTAMP",
            "ADD COLUMN IF NOT EXISTS config_hash TEXT",
        ],
        "booking_events": [
            "ADD COLUMN IF NOT EXISTS business_id TEXT",
            "ADD COLUMN IF NOT EXISTS provider_id TEXT",
            "ADD COLUMN IF NOT EXISTS service_type TEXT",
            "ADD COLUMN IF NOT EXISTS run_id TEXT",
            "ADD COLUMN IF NOT EXISTS effective_ts TIMESTAMP",
        ],
        "pricing_actions": [
            "ADD COLUMN IF NOT EXISTS eligible_action_set TEXT",
            "ADD COLUMN IF NOT EXISTS decision_reason TEXT",
            "ADD COLUMN IF NOT EXISTS was_exploration BOOLEAN",
            "ADD COLUMN IF NOT EXISTS exploration_policy TEXT",
            "ADD COLUMN IF NOT EXISTS decision_timestamp TIMESTAMP",
            "ADD COLUMN IF NOT EXISTS feature_snapshot_version TEXT",
            "ADD COLUMN IF NOT EXISTS confidence_score DOUBLE",
            "ADD COLUMN IF NOT EXISTS rationale_codes TEXT",
        ],
    }
    for table, stmts in migrations.items():
        for stmt in stmts:
            conn.execute(f"ALTER TABLE {table} {stmt}")
