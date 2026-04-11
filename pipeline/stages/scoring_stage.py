from __future__ import annotations

import logging

import duckdb
import numpy as np
import pandas as pd

from models.calibration import build_business_calibration
from models.demand_scoring import (
    FEATURE_COLUMNS,
    feature_contract_hash,
    predict_booking_probability,
    train_model_with_guardrail,
)
from pipeline.stages.phase2_utils import clamp

LOGGER = logging.getLogger(__name__)


def _build_training_dataset(
    conn: duckdb.DuckDBPyConnection,
    *,
    scenario_id: str,
    run_id: str,
    feature_snapshot_version: str,
    effective_ts: pd.Timestamp,
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
                         AND e.event_at <= ?
                   ) THEN 1 ELSE 0
               END AS label
        FROM feature_snapshots f
        JOIN slots s
          ON s.slot_id = f.slot_id
         AND s.run_id = f.run_id
         AND s.scenario_id = f.scenario_id
        WHERE f.scenario_id = ? AND f.run_id = ? AND f.feature_snapshot_version = ?
          AND s.slot_start_at < ?
        """,
        [effective_ts, scenario_id, run_id, feature_snapshot_version, effective_ts],
    ).fetchdf()
    return train.dropna(subset=FEATURE_COLUMNS)


def score_slots(
    conn: duckdb.DuckDBPyConnection,
    *,
    scenario_id: str,
    run_id: str,
    feature_snapshot_version: str,
    model_version: str,
    l2_c: float,
    effective_ts,
    training_min_rows: int,
) -> pd.DataFrame:
    label_definition = "booked_or_completed_as_of_effective_ts_for_started_slots"
    train = _build_training_dataset(
        conn,
        scenario_id=scenario_id,
        run_id=run_id,
        feature_snapshot_version=feature_snapshot_version,
        effective_ts=effective_ts,
    )
    bundle = train_model_with_guardrail(train, l2_c=l2_c, min_rows=training_min_rows)
    training_row_count = len(train)
    positive_label_rate = float(train["label"].mean()) if training_row_count else 0.5
    LOGGER.info(
        "stage=scoring training_rows=%d positive_label_rate=%.4f used_fallback=%s trained=%s",
        training_row_count,
        positive_label_rate,
        bundle.used_fallback,
        bundle.trained,
    )

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
        booking_prob = predict_booking_probability(bundle, scoring)

        scoring["booking_probability"] = np.clip(booking_prob, 0.0, 1.0)

        calibration = build_business_calibration(scoring)

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
        scoring["score_margin"] = scoring["confidence_score"]
        scoring["run_id"] = run_id
        scoring["scenario_id"] = scenario_id
        scoring["feature_snapshot_version"] = feature_snapshot_version
        scoring["model_version"] = model_version
        scoring["training_row_count"] = int(training_row_count)
        scoring["positive_label_rate"] = float(positive_label_rate)
        scoring["used_fallback"] = bool(bundle.used_fallback)
        scoring["label_definition"] = label_definition
        scoring["feature_contract_hash"] = feature_contract_hash()

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
                "score_margin",
                "training_row_count",
                "positive_label_rate",
                "used_fallback",
                "label_definition",
                "feature_contract_hash",
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
              shortfall_score, confidence_score, score_margin, training_row_count,
              positive_label_rate, used_fallback, label_definition, feature_contract_hash,
              model_version
            )
            SELECT * FROM tmp_scoring_outputs
            """
        )
    return output
