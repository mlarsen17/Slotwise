from __future__ import annotations

from dataclasses import dataclass
import hashlib

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

FEATURE_COLUMNS = [
    "hours_until_slot",
    "hist_fill_rate_similar",
    "fill_rate_provider_service",
    "fill_rate_business_service",
    "expected_booking_pace_per_day",
    "observed_booking_pace_per_day",
    "pace_deviation",
    "provider_utilization_7d",
    "provider_utilization_14d",
    "provider_utilization_28d",
    "business_fill_trend",
    "cohort_fill_rate",
]


@dataclass
class ModelBundle:
    model: LogisticRegression | None
    fallback_rate: float
    used_fallback: bool
    trained: bool


def validate_feature_contract(df: pd.DataFrame) -> None:
    missing = [col for col in FEATURE_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required scoring features: {missing}")


def _ordered_features(df: pd.DataFrame) -> pd.DataFrame:
    validate_feature_contract(df)
    return df[FEATURE_COLUMNS]


def train_model(dataset: pd.DataFrame, *, l2_c: float) -> ModelBundle:
    if dataset.empty:
        return ModelBundle(model=None, fallback_rate=0.5, used_fallback=True, trained=False)
    y = dataset["label"].astype(int)
    fallback = float(y.mean()) if len(y) else 0.5
    if y.nunique() < 2:
        return ModelBundle(model=None, fallback_rate=fallback, used_fallback=True, trained=False)
    model = LogisticRegression(solver="liblinear", C=l2_c, random_state=0)
    model.fit(_ordered_features(dataset), y)
    return ModelBundle(model=model, fallback_rate=fallback, used_fallback=False, trained=True)


def train_model_with_guardrail(dataset: pd.DataFrame, *, l2_c: float, min_rows: int) -> ModelBundle:
    if len(dataset) < min_rows:
        fallback = float(dataset["label"].mean()) if len(dataset) else 0.5
        return ModelBundle(model=None, fallback_rate=fallback, used_fallback=True, trained=False)
    return train_model(dataset, l2_c=l2_c)


def predict_booking_probability(bundle: ModelBundle, scoring_df: pd.DataFrame) -> np.ndarray:
    features = _ordered_features(scoring_df)
    if bundle.model is None:
        return np.repeat(bundle.fallback_rate, len(scoring_df))
    return bundle.model.predict_proba(features)[:, 1]


def feature_contract_hash() -> str:
    return hashlib.sha256("|".join(FEATURE_COLUMNS).encode()).hexdigest()[:16]
