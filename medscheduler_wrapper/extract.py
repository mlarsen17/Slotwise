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
    slots = []
    booking_events = []

    for b in range(config.business_count):
        business_id = f"biz_{b+1}"
        businesses.append({"source_business_id": business_id, "name": f"Business {b+1}"})

        for p in range(config.providers_per_business):
            provider_id = f"{business_id}_provider_{p+1}"
            providers.append(
                {
                    "source_provider_id": provider_id,
                    "source_business_id": business_id,
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
                    status = "open"

                    slots.append(
                        {
                            "source_slot_id": slot_id,
                            "source_provider_id": provider_id,
                            "source_business_id": business_id,
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
                        booking_events.append(
                            {
                                "source_event_id": f"{slot_id}_booked",
                                "source_slot_id": slot_id,
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
                                    "event_type": "canceled",
                                    "event_at": _as_utc(canceled_at),
                                }
                            )

    return {
        "businesses": pd.DataFrame(businesses),
        "providers": pd.DataFrame(providers),
        "slots": pd.DataFrame(slots),
        "booking_events": pd.DataFrame(booking_events),
        "meta": pd.DataFrame([asdict(config)]),
    }
