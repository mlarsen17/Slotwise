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
          slot_id TEXT PRIMARY KEY,
          provider_id TEXT,
          business_id TEXT,
          service_id TEXT,
          location_id TEXT,
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
          scenario_id TEXT
        );

        CREATE TABLE IF NOT EXISTS booking_events (
          event_id TEXT PRIMARY KEY,
          slot_id TEXT,
          customer_id TEXT,
          source_event_id TEXT,
          source_slot_id TEXT,
          source_customer_id TEXT,
          event_type TEXT,
          event_at TIMESTAMP,
          source_system TEXT,
          source_run_id TEXT,
          scenario_id TEXT
        );

        CREATE TABLE IF NOT EXISTS pricing_actions (
          action_id TEXT,
          slot_id TEXT,
          action_type TEXT,
          action_value DOUBLE,
          run_id TEXT,
          scenario_id TEXT
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
    LOGGER.info("DuckDB schema initialization complete")
