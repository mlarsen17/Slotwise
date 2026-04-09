from __future__ import annotations

from medscheduler_wrapper.extract import extract_synthetic_data
from medscheduler_wrapper.normalize import normalize_records
from medscheduler_wrapper.scenario_config import ScenarioConfig
from pipeline.config import AppConfig


def run_extract(cfg: AppConfig, *, run_id: str) -> dict:
    scenario = ScenarioConfig(
        scenario_id=cfg.scenario_id,
        source_run_id=cfg.source_run_id,
        random_seed=cfg.random_seed,
        effective_ts=cfg.effective_ts.isoformat().replace("+00:00", "Z"),
        business_count=cfg.scenario.business_count,
        providers_per_business=cfg.scenario.providers_per_business,
        services_per_business=cfg.scenario.services_per_business,
        locations_per_business=cfg.scenario.locations_per_business,
        customers_per_business=cfg.scenario.customers_per_business,
        days=cfg.scenario.days,
        slots_per_provider_per_day=cfg.scenario.slots_per_provider_per_day,
        cancellation_rate=cfg.scenario.cancellation_rate,
        removal_rate=cfg.scenario.removal_rate,
    )
    raw = extract_synthetic_data(scenario)
    return normalize_records(raw, cfg.scenario_id, cfg.source_run_id, run_id=run_id)
