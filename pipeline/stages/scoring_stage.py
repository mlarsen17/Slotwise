from __future__ import annotations

from dataclasses import dataclass

import duckdb
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression

from pipeline.stages.phase2_utils import clamp

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
class _ModelBundle:
    model: LogisticRegression | None
    fallback_rate: float


def _build_training_dataset(
    conn: duckdb.DuckDBPyConnection,
    *,
    scenario_id: str,
    run_id: str,
    feature_snapshot_version: str,
) -> pd.DataFrame:
    train = conn.execute(
        """
        SELECT f.slot_id, s.business_id,
               f.hours_until_slot, f.hist_fill_rate_similar, f.fill_rate_provider_service,
               f.fill_rate_business_service, f.expected_booking_pace_per_day,
               f.observed_booking_pace_per_day, f.pace_deviation,
               f.provider_utilization_7d, f.provider_utilization_14d,
               f.provider_utilization_28d, f.business_fill_trend, f.cohort_fill_rate,
               CASE
                   WHEN EXISTS (
                       SELECT 1
                       FROM booking_events e
                       WHERE e.slot_id = f.slot_id
                         AND e.run_id = f.run_id
                         AND e.scenario_id = f.scenario_id
                         AND e.event_type IN ('booked', 'completed')
                   ) THEN 1 ELSE 0
               END AS label
        FROM feature_snapshots f
        JOIN slots s
          ON s.slot_id = f.slot_id
         AND s.run_id = f.run_id
         AND s.scenario_id = f.scenario_id
        WHERE f.scenario_id = ? AND f.run_id = ? AND f.feature_snapshot_version = ?
        """,
        [scenario_id, run_id, feature_snapshot_version],
    ).fetchdf()
    return train.dropna(subset=FEATURE_COLUMNS)


def _train_model(dataset: pd.DataFrame, *, l2_c: float) -> _ModelBundle:
    if dataset.empty:
        return _ModelBundle(model=None, fallback_rate=0.5)

    y = dataset["label"].astype(int)
    fallback = float(y.mean()) if len(y) else 0.5
    if y.nunique() < 2:
        return _ModelBundle(model=None, fallback_rate=fallback)

    model = LogisticRegression(solver="liblinear", C=l2_c, random_state=0)
    model.fit(dataset[FEATURE_COLUMNS], y)
    return _ModelBundle(model=model, fallback_rate=fallback)


def score_slots(
    conn: duckdb.DuckDBPyConnection,
    *,
    scenario_id: str,
    run_id: str,
    feature_snapshot_version: str,
    model_version: str,
    l2_c: float,
) -> pd.DataFrame:
    train = _build_training_dataset(
        conn,
        scenario_id=scenario_id,
        run_id=run_id,
        feature_snapshot_version=feature_snapshot_version,
    )
    bundle = _train_model(train, l2_c=l2_c)

    scoring = conn.execute(
        """
        SELECT f.slot_id, s.business_id, f.cohort_fill_rate, u.severity_score,
               f.hours_until_slot, f.hist_fill_rate_similar, f.fill_rate_provider_service,
               f.fill_rate_business_service, f.expected_booking_pace_per_day,
               f.observed_booking_pace_per_day, f.pace_deviation,
               f.provider_utilization_7d, f.provider_utilization_14d,
               f.provider_utilization_28d, f.business_fill_trend,
               COALESCE(u.underbooked, FALSE) AS underbooked
        FROM feature_snapshots f
        JOIN slots s
          ON s.slot_id = f.slot_id
         AND s.run_id = f.run_id
         AND s.scenario_id = f.scenario_id
        LEFT JOIN underbooking_outputs u
          ON u.slot_id = f.slot_id
         AND u.run_id = f.run_id
         AND u.scenario_id = f.scenario_id
         AND u.feature_snapshot_version = f.feature_snapshot_version
        WHERE f.scenario_id = ?
          AND f.run_id = ?
          AND f.feature_snapshot_version = ?
          AND s.slot_start_at >= s.effective_ts
          AND s.current_status = 'open'
        """,
        [scenario_id, run_id, feature_snapshot_version],
    ).fetchdf()

    if scoring.empty:
        output = pd.DataFrame()
    else:
        if bundle.model is None:
            booking_prob = np.repeat(bundle.fallback_rate, len(scoring))
        else:
            booking_prob = bundle.model.predict_proba(scoring[FEATURE_COLUMNS])[:, 1]

        scoring["booking_probability"] = np.clip(booking_prob, 0.0, 1.0)

        calibration = (
            scoring.groupby("business_id")["business_fill_trend"]
            .mean()
            .rename("local_fill")
            .reset_index()
        )
        global_fill = float(scoring["business_fill_trend"].mean()) if len(scoring) else 0.5
        if global_fill <= 0:
            global_fill = 0.5
        calibration["calibration_factor"] = calibration["local_fill"].apply(
            lambda x: clamp(float(x) / global_fill, 0.8, 1.2)
        )

        conn.execute(
            "DELETE FROM business_calibrations WHERE scenario_id = ? AND run_id = ? AND feature_snapshot_version = ?",
            [scenario_id, run_id, feature_snapshot_version],
        )
        conn.register("tmp_calibration", calibration)
        conn.execute(
            """
            INSERT INTO business_calibrations (
              run_id, scenario_id, feature_snapshot_version, business_id, calibration_factor, model_version
            )
            SELECT ?, ?, ?, business_id, calibration_factor, ?
            FROM tmp_calibration
            """,
            [run_id, scenario_id, feature_snapshot_version, model_version],
        )

        scoring = scoring.merge(
            calibration[["business_id", "calibration_factor"]], on="business_id", how="left"
        )
        scoring["calibrated_booking_probability"] = scoring.apply(
            lambda row: clamp(
                float(row["booking_probability"]) * float(row["calibration_factor"]), 0.0, 1.0
            ),
            axis=1,
        )
        scoring["predicted_fill_by_start"] = (
            0.5 * scoring["cohort_fill_rate"].fillna(0.0)
            + 0.5 * scoring["calibrated_booking_probability"]
        ).clip(0.0, 1.0)
        scoring["shortfall_score"] = (
            scoring["severity_score"].fillna(0.0)
            * (1.0 - scoring["calibrated_booking_probability"])
        ).clip(0.0, 1.0)
        scoring["confidence_score"] = (
            2.0 * np.abs(scoring["calibrated_booking_probability"] - 0.5)
        ).clip(0.0, 1.0)
        scoring["run_id"] = run_id
        scoring["scenario_id"] = scenario_id
        scoring["feature_snapshot_version"] = feature_snapshot_version
        scoring["model_version"] = model_version

        output = scoring[
            [
                "run_id",
                "slot_id",
                "scenario_id",
                "feature_snapshot_version",
                "business_id",
                "booking_probability",
                "calibrated_booking_probability",
                "predicted_fill_by_start",
                "shortfall_score",
                "confidence_score",
                "model_version",
            ]
        ]

    conn.execute(
        "DELETE FROM scoring_outputs WHERE scenario_id = ? AND run_id = ? AND feature_snapshot_version = ?",
        [scenario_id, run_id, feature_snapshot_version],
    )
    if not output.empty:
        conn.register("tmp_scoring_outputs", output)
        conn.execute(
            """
            INSERT INTO scoring_outputs (
              run_id, slot_id, scenario_id, feature_snapshot_version, business_id,
              booking_probability, calibrated_booking_probability, predicted_fill_by_start,
              shortfall_score, confidence_score, model_version
            )
            SELECT * FROM tmp_scoring_outputs
            """
        )
    return output
