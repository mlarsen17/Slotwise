from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ScenarioConfig:
    scenario_id: str
    source_run_id: str
    random_seed: int
    effective_ts: str
    business_count: int
    providers_per_business: int
    services_per_business: int
    locations_per_business: int
    customers_per_business: int
    days: int
    slots_per_provider_per_day: int
    cancellation_rate: float
    removal_rate: float
