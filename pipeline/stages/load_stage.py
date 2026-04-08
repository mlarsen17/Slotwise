from __future__ import annotations

import duckdb
import pandas as pd


MANDATORY_SLOT_COLS = ["slot_id", "provider_id", "business_id", "slot_start_at", "scenario_id"]
MANDATORY_EVENT_COLS = ["event_id", "slot_id", "event_type", "event_at", "scenario_id"]


def _validate(df: pd.DataFrame, cols: list[str], label: str) -> None:
    for col in cols:
        if df[col].isna().any():
            raise ValueError(f"{label} contains nulls in mandatory key: {col}")


def load_core_tables(
    conn: duckdb.DuckDBPyConnection, normalized: dict[str, pd.DataFrame], scenario_id: str
) -> None:
    _validate(normalized["slots"], MANDATORY_SLOT_COLS, "slots")
    _validate(normalized["booking_events"], MANDATORY_EVENT_COLS, "booking_events")

    conn.register("tmp_businesses", normalized["businesses"])
    conn.register("tmp_providers", normalized["providers"])
    conn.register("tmp_services", normalized["services"])
    conn.register("tmp_locations", normalized["locations"])
    conn.register("tmp_customers", normalized["customers"])
    conn.register("tmp_slots", normalized["slots"])
    conn.register("tmp_events", normalized["booking_events"])

    for table in [
        "businesses",
        "providers",
        "services",
        "locations",
        "customers",
        "slots",
        "booking_events",
    ]:
        conn.execute(f"DELETE FROM {table} WHERE scenario_id = ?", [scenario_id])

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
    conn.execute(
        """
        INSERT INTO slots (
          slot_id, provider_id, business_id, service_id, location_id, source_slot_id, source_provider_id,
          source_business_id, source_service_id, source_location_id, slot_start_at, slot_end_at,
          visible_at, unavailable_at, current_status, created_at, source_system, source_run_id, scenario_id
        )
        SELECT
          slot_id, provider_id, business_id, service_id, location_id, source_slot_id, source_provider_id,
          source_business_id, source_service_id, source_location_id, slot_start_at::TIMESTAMP,
          slot_end_at::TIMESTAMP, visible_at::TIMESTAMP, NULL, NULL, created_at::TIMESTAMP, source_system,
          source_run_id, scenario_id
        FROM tmp_slots
        """
    )
    conn.execute(
        """
        INSERT INTO booking_events
        SELECT
          event_id, slot_id, customer_id, source_event_id, source_slot_id, source_customer_id, event_type,
          event_at::TIMESTAMP, source_system, source_run_id, scenario_id
        FROM tmp_events
        """
    )

    slot_count = conn.execute(
        "SELECT COUNT(*) FROM slots WHERE scenario_id = ?", [scenario_id]
    ).fetchone()[0]
    event_count = conn.execute(
        "SELECT COUNT(*) FROM booking_events WHERE scenario_id = ?", [scenario_id]
    ).fetchone()[0]

    if slot_count == 0:
        raise ValueError("No slots loaded")
    if event_count < 0:
        raise ValueError("Invalid event count")
