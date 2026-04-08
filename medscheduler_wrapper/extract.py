from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import random

import pandas as pd

from medscheduler_wrapper.scenario_config import ScenarioConfig


def _as_utc(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).isoformat()


def extract_synthetic_data(config: ScenarioConfig) -> dict[str, pd.DataFrame]:
    rng = random.Random(config.random_seed)
    start = datetime.fromisoformat(config.effective_ts.replace("Z", "+00:00"))

    businesses = []
    providers = []
    services = []
    locations = []
    customers = []
    slots = []
    booking_events = []

    for b in range(config.business_count):
        business_id = f"biz_{b+1}"
        businesses.append({"source_business_id": business_id, "name": f"Business {b+1}"})
        for svc in range(config.services_per_business):
            service_id = f"{business_id}_service_{svc+1}"
            services.append(
                {
                    "source_service_id": service_id,
                    "source_business_id": business_id,
                    "name": f"Service {svc+1}",
                    "duration_minutes": 60,
                }
            )
        for loc in range(config.locations_per_business):
            location_id = f"{business_id}_location_{loc+1}"
            locations.append(
                {
                    "source_location_id": location_id,
                    "source_business_id": business_id,
                    "name": f"Location {loc+1}",
                }
            )
        for c in range(config.customers_per_business):
            customer_id = f"{business_id}_customer_{c+1}"
            customers.append(
                {
                    "source_customer_id": customer_id,
                    "source_business_id": business_id,
                    "first_name": f"Customer{c+1}",
                    "last_name": "Synthetic",
                }
            )

        for p in range(config.providers_per_business):
            provider_id = f"{business_id}_provider_{p+1}"
            provider_location_id = (
                f"{business_id}_location_{(p % config.locations_per_business) + 1}"
            )
            providers.append(
                {
                    "source_provider_id": provider_id,
                    "source_business_id": business_id,
                    "source_location_id": provider_location_id,
                    "name": f"Provider {p+1}",
                }
            )

            for d in range(config.days):
                day = start + timedelta(days=d)
                for s in range(config.slots_per_provider_per_day):
                    slot_start = day + timedelta(hours=9 + s * 2)
                    slot_end = slot_start + timedelta(hours=1)
                    slot_id = f"{provider_id}_{slot_start.strftime('%Y%m%d%H%M')}"
                    visible_at = slot_start - timedelta(days=7)
                    service_id = f"{business_id}_service_{(s % config.services_per_business) + 1}"
                    status = "open"

                    slots.append(
                        {
                            "source_slot_id": slot_id,
                            "source_provider_id": provider_id,
                            "source_business_id": business_id,
                            "source_service_id": service_id,
                            "source_location_id": provider_location_id,
                            "slot_start_at": _as_utc(slot_start),
                            "slot_end_at": _as_utc(slot_end),
                            "visible_at": _as_utc(visible_at),
                            "created_at": _as_utc(start),
                        }
                    )

                    if rng.random() < config.removal_rate:
                        removed_at = slot_start - timedelta(hours=6)
                        booking_events.append(
                            {
                                "source_event_id": f"{slot_id}_removed",
                                "source_slot_id": slot_id,
                                "event_type": "removed",
                                "event_at": _as_utc(removed_at),
                            }
                        )
                        status = "removed"

                    if status == "open" and rng.random() < 0.65:
                        booked_at = slot_start - timedelta(hours=rng.randint(1, 96))
                        customer_id = f"{business_id}_customer_{rng.randint(1, config.customers_per_business)}"
                        booking_events.append(
                            {
                                "source_event_id": f"{slot_id}_booked",
                                "source_slot_id": slot_id,
                                "source_customer_id": customer_id,
                                "event_type": "booked",
                                "event_at": _as_utc(booked_at),
                            }
                        )
                        if rng.random() < config.cancellation_rate:
                            canceled_at = booked_at + timedelta(hours=1)
                            booking_events.append(
                                {
                                    "source_event_id": f"{slot_id}_canceled",
                                    "source_slot_id": slot_id,
                                    "source_customer_id": customer_id,
                                    "event_type": "canceled",
                                    "event_at": _as_utc(canceled_at),
                                }
                            )

    _run_integrity_checks(
        businesses=businesses,
        providers=providers,
        services=services,
        locations=locations,
        customers=customers,
        slots=slots,
        booking_events=booking_events,
    )

    return {
        "businesses": pd.DataFrame(businesses),
        "providers": pd.DataFrame(providers),
        "services": pd.DataFrame(services),
        "locations": pd.DataFrame(locations),
        "customers": pd.DataFrame(customers),
        "slots": pd.DataFrame(slots),
        "booking_events": pd.DataFrame(booking_events),
        "meta": pd.DataFrame([asdict(config)]),
    }


def _run_integrity_checks(
    *,
    businesses: list[dict],
    providers: list[dict],
    services: list[dict],
    locations: list[dict],
    customers: list[dict],
    slots: list[dict],
    booking_events: list[dict],
) -> None:
    business_ids = {row["source_business_id"] for row in businesses}
    service_ids = {row["source_service_id"] for row in services}
    location_ids = {row["source_location_id"] for row in locations}
    customer_ids = {row["source_customer_id"] for row in customers}
    slot_ids = {row["source_slot_id"] for row in slots}

    if len(slot_ids) != len(slots):
        raise ValueError("Duplicate slot IDs detected in extraction output")

    provider_ids = set()
    for row in providers:
        if row["source_business_id"] not in business_ids:
            raise ValueError("Provider references unknown business")
        if row["source_location_id"] not in location_ids:
            raise ValueError("Provider references unknown location")
        provider_ids.add(row["source_provider_id"])

    for row in services + locations + customers:
        if row["source_business_id"] not in business_ids:
            raise ValueError("Entity references unknown business")

    for row in slots:
        if row["source_provider_id"] not in provider_ids:
            raise ValueError("Slot references unknown provider")
        if row["source_service_id"] not in service_ids:
            raise ValueError("Slot references unknown service")
        if row["source_location_id"] not in location_ids:
            raise ValueError("Slot references unknown location")
        if row["visible_at"] > row["slot_start_at"]:
            raise ValueError("Slot visible_at occurs after slot_start_at")

    valid_event_types = {"booked", "canceled", "removed"}
    seen_events = set()
    for row in booking_events:
        event_id = row["source_event_id"]
        if event_id in seen_events:
            raise ValueError("Duplicate event IDs detected in extraction output")
        seen_events.add(event_id)
        if row["source_slot_id"] not in slot_ids:
            raise ValueError("Booking event references unknown slot")
        if row["event_type"] not in valid_event_types:
            raise ValueError(f"Unknown event type: {row['event_type']}")
        if "source_customer_id" in row and row["source_customer_id"] not in customer_ids:
            raise ValueError("Booking event references unknown customer")
