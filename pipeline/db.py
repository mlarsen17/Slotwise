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

        CREATE TABLE IF NOT EXISTS cohort_baselines (
          cohort_id TEXT,
          day_of_week TEXT,
          time_of_day_bucket TEXT,
          service_type TEXT,
          observation_count INTEGER,
          fill_rate DOUBLE,
          expected_booking_pace_per_day DOUBLE,
          avg_lead_time_hours DOUBLE,
          completion_rate DOUBLE,
          is_sparse BOOLEAN,
          feature_snapshot_version TEXT,
          run_id TEXT,
          scenario_id TEXT
        );

        CREATE TABLE IF NOT EXISTS feature_snapshots (
          slot_id TEXT,
          feature_snapshot_version TEXT,
          run_id TEXT,
          scenario_id TEXT,
          day_of_week TEXT,
          time_of_day_bucket TEXT,
          service_type TEXT,
          slot_duration_minutes INTEGER,
          effective_lead_time_band TEXT,
          hours_until_slot DOUBLE,
          days_until_slot DOUBLE,
          hist_fill_rate_similar DOUBLE,
          fill_rate_provider_service DOUBLE,
          fill_rate_business_service DOUBLE,
          expected_booking_pace_per_day DOUBLE,
          observed_booking_pace_per_day DOUBLE,
          pace_deviation DOUBLE,
          avg_lead_time_hours DOUBLE,
          remaining_provider_slots_same_day INTEGER,
          remaining_service_slots_window INTEGER,
          inventory_density_2h DOUBLE,
          provider_utilization_7d DOUBLE,
          provider_utilization_14d DOUBLE,
          provider_utilization_28d DOUBLE,
          booking_volume_7d DOUBLE,
          booking_volume_14d DOUBLE,
          booking_volume_28d DOUBLE,
          cancel_rate_pattern DOUBLE,
          no_show_rate_pattern DOUBLE,
          reschedule_rate_pattern DOUBLE,
          business_fill_trend DOUBLE,
          business_booking_trend DOUBLE,
          cohort_fill_rate DOUBLE,
          cohort_completion_rate DOUBLE,
          cohort_is_sparse BOOLEAN
        );

        CREATE TABLE IF NOT EXISTS underbooking_outputs (
          slot_id TEXT,
          feature_snapshot_version TEXT,
          run_id TEXT,
          scenario_id TEXT,
          pace_gap DOUBLE,
          pace_gap_normalized DOUBLE,
          fill_gap DOUBLE,
          fill_gap_normalized DOUBLE,
          severity_score DOUBLE,
          underbooked BOOLEAN,
          detection_reason TEXT
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
          scenario_id TEXT,
          feature_snapshot_version TEXT,
          business_id TEXT,
          booking_probability DOUBLE,
          calibrated_booking_probability DOUBLE,
          predicted_fill_by_start DOUBLE,
          shortfall_score DOUBLE,
          confidence_score DOUBLE,
          model_version TEXT
        );

        CREATE TABLE IF NOT EXISTS business_calibrations (
          run_id TEXT,
          scenario_id TEXT,
          feature_snapshot_version TEXT,
          business_id TEXT,
          calibration_factor DOUBLE,
          model_version TEXT
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
        "cohort_baselines": [
            "ADD COLUMN IF NOT EXISTS day_of_week TEXT",
            "ADD COLUMN IF NOT EXISTS time_of_day_bucket TEXT",
            "ADD COLUMN IF NOT EXISTS service_type TEXT",
            "ADD COLUMN IF NOT EXISTS observation_count INTEGER",
            "ADD COLUMN IF NOT EXISTS fill_rate DOUBLE",
            "ADD COLUMN IF NOT EXISTS expected_booking_pace_per_day DOUBLE",
            "ADD COLUMN IF NOT EXISTS avg_lead_time_hours DOUBLE",
            "ADD COLUMN IF NOT EXISTS completion_rate DOUBLE",
            "ADD COLUMN IF NOT EXISTS is_sparse BOOLEAN",
        ],
        "feature_snapshots": [
            "ADD COLUMN IF NOT EXISTS run_id TEXT",
            "ADD COLUMN IF NOT EXISTS day_of_week TEXT",
            "ADD COLUMN IF NOT EXISTS time_of_day_bucket TEXT",
            "ADD COLUMN IF NOT EXISTS service_type TEXT",
            "ADD COLUMN IF NOT EXISTS slot_duration_minutes INTEGER",
            "ADD COLUMN IF NOT EXISTS effective_lead_time_band TEXT",
            "ADD COLUMN IF NOT EXISTS hours_until_slot DOUBLE",
            "ADD COLUMN IF NOT EXISTS days_until_slot DOUBLE",
            "ADD COLUMN IF NOT EXISTS hist_fill_rate_similar DOUBLE",
            "ADD COLUMN IF NOT EXISTS fill_rate_provider_service DOUBLE",
            "ADD COLUMN IF NOT EXISTS fill_rate_business_service DOUBLE",
            "ADD COLUMN IF NOT EXISTS expected_booking_pace_per_day DOUBLE",
            "ADD COLUMN IF NOT EXISTS observed_booking_pace_per_day DOUBLE",
            "ADD COLUMN IF NOT EXISTS pace_deviation DOUBLE",
            "ADD COLUMN IF NOT EXISTS avg_lead_time_hours DOUBLE",
            "ADD COLUMN IF NOT EXISTS remaining_provider_slots_same_day INTEGER",
            "ADD COLUMN IF NOT EXISTS remaining_service_slots_window INTEGER",
            "ADD COLUMN IF NOT EXISTS inventory_density_2h DOUBLE",
            "ADD COLUMN IF NOT EXISTS provider_utilization_7d DOUBLE",
            "ADD COLUMN IF NOT EXISTS provider_utilization_14d DOUBLE",
            "ADD COLUMN IF NOT EXISTS provider_utilization_28d DOUBLE",
            "ADD COLUMN IF NOT EXISTS booking_volume_7d DOUBLE",
            "ADD COLUMN IF NOT EXISTS booking_volume_14d DOUBLE",
            "ADD COLUMN IF NOT EXISTS booking_volume_28d DOUBLE",
            "ADD COLUMN IF NOT EXISTS cancel_rate_pattern DOUBLE",
            "ADD COLUMN IF NOT EXISTS no_show_rate_pattern DOUBLE",
            "ADD COLUMN IF NOT EXISTS reschedule_rate_pattern DOUBLE",
            "ADD COLUMN IF NOT EXISTS business_fill_trend DOUBLE",
            "ADD COLUMN IF NOT EXISTS business_booking_trend DOUBLE",
            "ADD COLUMN IF NOT EXISTS cohort_fill_rate DOUBLE",
            "ADD COLUMN IF NOT EXISTS cohort_completion_rate DOUBLE",
            "ADD COLUMN IF NOT EXISTS cohort_is_sparse BOOLEAN",
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
        "scoring_outputs": [
            "ADD COLUMN IF NOT EXISTS scenario_id TEXT",
            "ADD COLUMN IF NOT EXISTS feature_snapshot_version TEXT",
            "ADD COLUMN IF NOT EXISTS business_id TEXT",
            "ADD COLUMN IF NOT EXISTS booking_probability DOUBLE",
            "ADD COLUMN IF NOT EXISTS calibrated_booking_probability DOUBLE",
            "ADD COLUMN IF NOT EXISTS predicted_fill_by_start DOUBLE",
            "ADD COLUMN IF NOT EXISTS shortfall_score DOUBLE",
            "ADD COLUMN IF NOT EXISTS confidence_score DOUBLE",
            "ADD COLUMN IF NOT EXISTS model_version TEXT",
        ],
    }
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS underbooking_outputs (
          slot_id TEXT,
          feature_snapshot_version TEXT,
          run_id TEXT,
          scenario_id TEXT,
          pace_gap DOUBLE,
          pace_gap_normalized DOUBLE,
          fill_gap DOUBLE,
          fill_gap_normalized DOUBLE,
          severity_score DOUBLE,
          underbooked BOOLEAN,
          detection_reason TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS business_calibrations (
          run_id TEXT,
          scenario_id TEXT,
          feature_snapshot_version TEXT,
          business_id TEXT,
          calibration_factor DOUBLE,
          model_version TEXT
        )
        """
    )
    for table, stmts in migrations.items():
        for stmt in stmts:
            conn.execute(f"ALTER TABLE {table} {stmt}")
