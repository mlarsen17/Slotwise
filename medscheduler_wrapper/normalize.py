from __future__ import annotations

import hashlib

import pandas as pd


def stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]
    return f"{prefix}_{digest}"


def normalize_records(
    raw: dict[str, pd.DataFrame], scenario_id: str, source_run_id: str
) -> dict[str, pd.DataFrame]:
    businesses = raw["businesses"].copy()
    businesses["business_id"] = businesses["source_business_id"].map(
        lambda x: stable_id("biz", scenario_id, x)
    )
    businesses["scenario_id"] = scenario_id

    providers = raw["providers"].copy()
    providers["provider_id"] = providers.apply(
        lambda row: stable_id("prov", scenario_id, row["source_provider_id"]), axis=1
    )
    providers["business_id"] = providers["source_business_id"].map(
        lambda x: stable_id("biz", scenario_id, x)
    )
    providers["location_id"] = providers["source_location_id"].map(
        lambda x: stable_id("loc", scenario_id, x)
    )
    providers["scenario_id"] = scenario_id

    services = raw["services"].copy()
    services["service_id"] = services["source_service_id"].map(
        lambda x: stable_id("svc", scenario_id, x)
    )
    services["business_id"] = services["source_business_id"].map(
        lambda x: stable_id("biz", scenario_id, x)
    )
    services["scenario_id"] = scenario_id

    locations = raw["locations"].copy()
    locations["location_id"] = locations["source_location_id"].map(
        lambda x: stable_id("loc", scenario_id, x)
    )
    locations["business_id"] = locations["source_business_id"].map(
        lambda x: stable_id("biz", scenario_id, x)
    )
    locations["scenario_id"] = scenario_id

    customers = raw["customers"].copy()
    customers["customer_id"] = customers["source_customer_id"].map(
        lambda x: stable_id("cus", scenario_id, x)
    )
    customers["business_id"] = customers["source_business_id"].map(
        lambda x: stable_id("biz", scenario_id, x)
    )
    customers["scenario_id"] = scenario_id

    slots = raw["slots"].copy()
    slots["slot_id"] = slots["source_slot_id"].map(lambda x: stable_id("slot", scenario_id, x))
    slots["provider_id"] = slots["source_provider_id"].map(
        lambda x: stable_id("prov", scenario_id, x)
    )
    slots["business_id"] = slots["source_business_id"].map(
        lambda x: stable_id("biz", scenario_id, x)
    )
    slots["service_id"] = slots["source_service_id"].map(lambda x: stable_id("svc", scenario_id, x))
    slots["location_id"] = slots["source_location_id"].map(
        lambda x: stable_id("loc", scenario_id, x)
    )
    slots["source_system"] = "medscheduler"
    slots["source_run_id"] = source_run_id
    slots["scenario_id"] = scenario_id

    events = raw["booking_events"].copy()
    if events.empty:
        events = pd.DataFrame(
            columns=["source_event_id", "source_slot_id", "event_type", "event_at"]
        )
    events["event_id"] = events["source_event_id"].map(lambda x: stable_id("evt", scenario_id, x))
    events["slot_id"] = events["source_slot_id"].map(lambda x: stable_id("slot", scenario_id, x))
    events["customer_id"] = (
        events["source_customer_id"]
        .fillna("")
        .map(lambda x: stable_id("cus", scenario_id, x) if x else None)
    )
    events["source_system"] = "medscheduler"
    events["source_run_id"] = source_run_id
    events["scenario_id"] = scenario_id

    return {
        "businesses": businesses,
        "providers": providers,
        "services": services,
        "locations": locations,
        "customers": customers,
        "slots": slots,
        "booking_events": events,
    }
