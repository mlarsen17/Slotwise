from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, Field, field_validator

LOGGER = logging.getLogger(__name__)


class GlobalDiscountLimits(BaseModel):
    min_pct: int = 0
    max_pct: int = 20


class TimeOfDayBuckets(BaseModel):
    boundaries_hours: list[int] = [0, 12, 17, 24]


class UnderbookingSettings(BaseModel):
    pace_weight: float = 0.6
    fill_weight: float = 0.4
    underbooking_threshold: float = 0.35
    sparse_baseline_fill_rate: float = 0.6


class ScoringSettings(BaseModel):
    training_min_rows: int = 10
    l2_c: float = 1.0


class OptimizerSettings(BaseModel):
    excluded_services: list[str] = Field(default_factory=list)
    price_floor_pct: float = 0.7
    healthy_zero_only: bool = True
    severity_breakpoints: list[float] = Field(default_factory=lambda: [0.2, 0.4, 0.7])
    discount_steps: list[int] = Field(default_factory=lambda: [5, 10, 15, 20])
    exploration_share: float = 0.1


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
    run_id: str | None = None
    effective_ts: datetime
    action_ladder: list[int]
    lead_time_windows_hours: list[int]
    global_discount_limits: GlobalDiscountLimits
    scenario: ScenarioSettings
    time_of_day_buckets: TimeOfDayBuckets = Field(default_factory=TimeOfDayBuckets)
    underbooking: UnderbookingSettings = Field(default_factory=UnderbookingSettings)
    scoring: ScoringSettings = Field(default_factory=ScoringSettings)
    optimizer: OptimizerSettings = Field(default_factory=OptimizerSettings)

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

    def resolved_run_id(self) -> str:
        if self.run_id:
            return self.run_id
        base = f"{self.scenario_id}:{self.effective_ts.isoformat()}:{self.random_seed}"
        digest = hashlib.sha256(base.encode()).hexdigest()[:12]
        return f"run_{digest}"

    def config_hash(self) -> str:
        base = (
            f"{self.scenario_id}|{self.source_run_id}|{self.random_seed}|"
            f"{self.effective_ts.isoformat()}|{self.action_ladder}|"
            f"{self.lead_time_windows_hours}|{self.global_discount_limits.model_dump()}|"
            f"{self.scenario.model_dump()}|{self.time_of_day_buckets.model_dump()}|"
            f"{self.underbooking.model_dump()}|{self.scoring.model_dump()}|"
            f"{self.optimizer.model_dump()}"
        )
        return hashlib.sha256(base.encode()).hexdigest()[:16]

    def feature_snapshot_version(self) -> str:
        ts = self.effective_ts.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        return f"fsv_{ts}"

    def model_version(self) -> str:
        return "model_v1_pooled_logistic"


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
        cfg.resolved_run_id(),
        cfg.duckdb_path,
    )
    return cfg
