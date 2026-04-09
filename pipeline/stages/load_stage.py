from __future__ import annotations

from datetime import datetime

import duckdb
import pandas as pd

ALLOWED_EVENT_TYPES = {"booked", "canceled", "removed", "completed", "no_show", "rescheduled"}
SLOT_REQUIRED = {"slot_id", "provider_id", "business_id", "slot_start_at", "scenario_id", "run_id"}
EVENT_REQUIRED = {"event_id", "slot_id", "event_type", "event_at", "scenario_id", "run_id"}
SLOT_COLUMNS = {
    "slot_id",
    "provider_id",
    "business_id",
    "service_id",
    "location_id",
    "standard_price",
    "slot_duration_minutes",
    "integration_id",
    "external_slot_id",
    "source_slot_id",
    "source_provider_id",
    "source_business_id",
    "source_service_id",
    "source_location_id",
    "slot_start_at",
    "slot_end_at",
    "visible_at",
    "created_at",
    "source_system",
    "source_run_id",
    "scenario_id",
    "run_id",
}
EVENT_COLUMNS = {
    "event_id",
    "slot_id",
    "customer_id",
    "business_id",
    "provider_id",
    "service_type",
    "source_event_id",
    "source_slot_id",
    "source_customer_id",
    "event_type",
    "event_at",
    "source_system",
    "source_run_id",
    "scenario_id",
    "run_id",
}


def _require_columns(df: pd.DataFrame, required: set[str], name: str, strict: bool = True) -> None:
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"{name} missing required columns: {missing}")
    if strict:
        allowed = SLOT_COLUMNS if name == "slots" else EVENT_COLUMNS
        unexpected = sorted(set(df.columns) - allowed)
        if unexpected:
            raise ValueError(f"{name} has unexpected columns: {unexpected}")


def _validate_non_null(df: pd.DataFrame, required: set[str], name: str) -> None:
    for col in sorted(required):
        if df[col].isna().any():
            raise ValueError(f"{name} contains nulls in required column: {col}")


def _validate_types(slots: pd.DataFrame, events: pd.DataFrame) -> None:
    for col in ("slot_start_at", "slot_end_at", "visible_at", "created_at"):
        parsed = pd.to_datetime(slots[col], errors="coerce")
        if parsed.isna().any():
            raise ValueError(f"slots has invalid timestamp values in column: {col}")
    if (pd.to_numeric(slots["slot_duration_minutes"], errors="coerce").isna()).any():
        raise ValueError("slots.slot_duration_minutes must be numeric")
    if (pd.to_numeric(slots["standard_price"], errors="coerce").isna()).any():
        raise ValueError("slots.standard_price must be numeric")
    parsed_events = pd.to_datetime(events["event_at"], errors="coerce")
    if parsed_events.isna().any():
        raise ValueError("booking_events has invalid timestamp values in column: event_at")
    bad_event_types = sorted(set(events["event_type"].dropna()) - ALLOWED_EVENT_TYPES)
    if bad_event_types:
        raise ValueError(f"booking_events has unsupported event_type values: {bad_event_types}")


def load_core_tables(
    conn: duckdb.DuckDBPyConnection,
    normalized: dict[str, pd.DataFrame],
    *,
    scenario_id: str,
    run_id: str,
    effective_ts: datetime,
    config_hash: str,
    strict_schema: bool = True,
    simulate_failure: bool = False,
) -> None:
    slots = normalized["slots"]
    events = normalized["booking_events"]
    _require_columns(slots, SLOT_REQUIRED, "slots", strict=strict_schema)
    _require_columns(events, EVENT_REQUIRED, "booking_events", strict=strict_schema)
    _validate_non_null(slots, SLOT_REQUIRED, "slots")
    _validate_non_null(events, EVENT_REQUIRED, "booking_events")
    _validate_types(slots, events)

    conn.register("tmp_businesses", normalized["businesses"])
    conn.register("tmp_providers", normalized["providers"])
    conn.register("tmp_services", normalized["services"])
    conn.register("tmp_locations", normalized["locations"])
    conn.register("tmp_customers", normalized["customers"])
    conn.register("tmp_slots", slots)
    conn.register("tmp_events", events)

    conn.execute("BEGIN TRANSACTION")
    try:
        for table in ["businesses", "providers", "services", "locations", "customers"]:
            conn.execute(f"DELETE FROM {table} WHERE scenario_id = ?", [scenario_id])
        for table in ["slots", "booking_events"]:
            conn.execute(
                f"DELETE FROM {table} WHERE scenario_id = ? AND run_id = ?", [scenario_id, run_id]
            )

        conn.execute(
            "DELETE FROM pipeline_runs WHERE run_id = ? AND scenario_id = ?", [run_id, scenario_id]
        )
        conn.execute(
            """
            INSERT INTO pipeline_runs (run_id, scenario_id, effective_ts, config_hash, started_at, status)
            VALUES (?, ?, ?, ?, ?, 'running')
            """,
            [run_id, scenario_id, effective_ts, config_hash, effective_ts],
        )

        conn.execute(
            "INSERT INTO businesses SELECT business_id, source_business_id, name, scenario_id FROM tmp_businesses"
        )
        conn.execute(
            """
            INSERT INTO providers
            SELECT provider_id, business_id, location_id, source_provider_id, source_location_id,
                   source_business_id, name, scenario_id
            FROM tmp_providers
            """
        )
        conn.execute(
            """
            INSERT INTO services
            SELECT service_id, business_id, source_service_id, source_business_id, name, duration_minutes,
                   scenario_id
            FROM tmp_services
            """
        )
        conn.execute(
            """
            INSERT INTO locations
            SELECT location_id, business_id, source_location_id, source_business_id, name, scenario_id
            FROM tmp_locations
            """
        )
        conn.execute(
            """
            INSERT INTO customers
            SELECT customer_id, business_id, source_customer_id, source_business_id, first_name, last_name,
                   scenario_id
            FROM tmp_customers
            """
        )

        if simulate_failure:
            raise RuntimeError("Simulated load failure")

        conn.execute(
            """
            INSERT INTO slots (
              slot_id, provider_id, business_id, service_id, location_id, standard_price,
              slot_duration_minutes, integration_id, external_slot_id, source_slot_id,
              source_provider_id, source_business_id, source_service_id, source_location_id,
              slot_start_at, slot_end_at, visible_at, unavailable_at, current_status, created_at,
              source_system, source_run_id, scenario_id, run_id, effective_ts, config_hash
            )
            SELECT
              slot_id, provider_id, business_id, service_id, location_id, standard_price,
              slot_duration_minutes::INTEGER, integration_id, external_slot_id, source_slot_id,
              source_provider_id, source_business_id, source_service_id, source_location_id,
              slot_start_at::TIMESTAMP, slot_end_at::TIMESTAMP, visible_at::TIMESTAMP, NULL, NULL,
              created_at::TIMESTAMP, source_system, source_run_id, scenario_id, run_id, ?, ?
            FROM tmp_slots
            """,
            [effective_ts, config_hash],
        )
        conn.execute(
            """
            INSERT INTO booking_events (
              event_id, slot_id, customer_id, business_id, provider_id, service_type, source_event_id,
              source_slot_id, source_customer_id, event_type, event_at, source_system, source_run_id,
              scenario_id, run_id, effective_ts
            )
            SELECT
              event_id, slot_id, customer_id, business_id, provider_id, service_type, source_event_id,
              source_slot_id, source_customer_id, event_type, event_at::TIMESTAMP, source_system,
              source_run_id, scenario_id, run_id, ?
            FROM tmp_events
            """,
            [effective_ts],
        )
        conn.execute(
            "UPDATE pipeline_runs SET status='success' WHERE run_id = ? AND scenario_id = ?",
            [run_id, scenario_id],
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    slot_count = conn.execute(
        "SELECT COUNT(*) FROM slots WHERE scenario_id = ? AND run_id = ?", [scenario_id, run_id]
    ).fetchone()[0]
    if slot_count == 0:
        raise ValueError("No slots loaded")
