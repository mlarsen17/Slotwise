from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, field_validator

LOGGER = logging.getLogger(__name__)


class GlobalDiscountLimits(BaseModel):
    min_pct: int = 0
    max_pct: int = 20


class ScenarioSettings(BaseModel):
    business_count: int = 2
    providers_per_business: int = 2
    services_per_business: int = 3
    locations_per_business: int = 1
    customers_per_business: int = 50
    days: int = 7
    slots_per_provider_per_day: int = 4
    cancellation_rate: float = 0.1
    removal_rate: float = 0.05


class AppConfig(BaseModel):
    duckdb_path: Path
    scenario_id: str
    source_run_id: str
    random_seed: int
    effective_ts: datetime
    action_ladder: list[int]
    lead_time_windows_hours: list[int]
    global_discount_limits: GlobalDiscountLimits
    scenario: ScenarioSettings

    @field_validator("duckdb_path", mode="before")
    @classmethod
    def normalize_duckdb_path(cls, value: str | Path) -> Path:
        return Path(value)

    @field_validator("effective_ts", mode="before")
    @classmethod
    def normalize_effective_ts(cls, value: str | datetime) -> datetime:
        if isinstance(value, datetime):
            return value.astimezone(timezone.utc)
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)

    def run_id(self) -> str:
        base = f"{self.scenario_id}:{self.effective_ts.isoformat()}:{self.random_seed}"
        digest = hashlib.sha256(base.encode()).hexdigest()[:12]
        return f"run_{digest}"

    def feature_snapshot_version(self) -> str:
        ts = self.effective_ts.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return f"fsv_{ts}"

    def model_version(self) -> str:
        return "model_v0"


def load_config(path: str | Path = "config/default.yaml") -> AppConfig:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as handle:
        payload: dict[str, Any] = yaml.safe_load(handle)
    cfg = AppConfig(**payload)
    LOGGER.info(
        "Loaded config scenario=%s run_id=%s duckdb=%s",
        cfg.scenario_id,
        cfg.run_id(),
        cfg.duckdb_path,
    )
    return cfg
