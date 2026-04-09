from __future__ import annotations

import duckdb
import pandas as pd

from pipeline.stages.phase2_utils import clamp


def detect_underbooking(
    conn: duckdb.DuckDBPyConnection,
    *,
    scenario_id: str,
    run_id: str,
    feature_snapshot_version: str,
    pace_weight: float,
    fill_weight: float,
    underbooking_threshold: float,
) -> pd.DataFrame:
    features = conn.execute(
        """
        SELECT slot_id, feature_snapshot_version, run_id, scenario_id,
               expected_booking_pace_per_day, observed_booking_pace_per_day,
               cohort_fill_rate, hist_fill_rate_similar
        FROM feature_snapshots
        WHERE scenario_id = ? AND run_id = ? AND feature_snapshot_version = ?
        """,
        [scenario_id, run_id, feature_snapshot_version],
    ).fetchdf()

    if features.empty:
        output = pd.DataFrame()
    else:
        features["pace_gap"] = (
            features["expected_booking_pace_per_day"] - features["observed_booking_pace_per_day"]
        ).clip(lower=0.0)
        features["pace_gap_normalized"] = features["pace_gap"] / features[
            "expected_booking_pace_per_day"
        ].replace(0.0, 1.0)

        features["projected_fill"] = (
            features[["hist_fill_rate_similar", "cohort_fill_rate"]].fillna(0.0).mean(axis=1)
        )
        features["expected_fill_baseline"] = features["cohort_fill_rate"].where(
            features["cohort_fill_rate"] > 0,
            0.6,
        )
        features["fill_gap"] = (
            features["expected_fill_baseline"] - features["projected_fill"]
        ).clip(lower=0.0)
        features["fill_gap_normalized"] = features["fill_gap"] / features[
            "expected_fill_baseline"
        ].replace(0.0, 1.0)

        features["pace_gap_normalized"] = features["pace_gap_normalized"].fillna(0.0)
        features["fill_gap_normalized"] = features["fill_gap_normalized"].fillna(0.0)
        features["severity_score"] = (
            (
                pace_weight * features["pace_gap_normalized"]
                + fill_weight * features["fill_gap_normalized"]
            )
            .fillna(0.0)
            .map(lambda v: clamp(float(v), 0.0, 1.0))
        )
        features["underbooked"] = features["severity_score"] >= underbooking_threshold
        features["detection_reason"] = features.apply(
            lambda row: (
                "pace_gap_and_fill_gap"
                if row["pace_gap_normalized"] > 0 and row["fill_gap_normalized"] > 0
                else (
                    "pace_gap"
                    if row["pace_gap_normalized"] > 0
                    else "fill_gap" if row["fill_gap_normalized"] > 0 else "healthy"
                )
            ),
            axis=1,
        )
        output = features[
            [
                "slot_id",
                "feature_snapshot_version",
                "run_id",
                "scenario_id",
                "pace_gap",
                "pace_gap_normalized",
                "fill_gap",
                "fill_gap_normalized",
                "severity_score",
                "underbooked",
                "detection_reason",
            ]
        ]

    conn.execute(
        "DELETE FROM underbooking_outputs WHERE scenario_id = ? AND run_id = ? AND feature_snapshot_version = ?",
        [scenario_id, run_id, feature_snapshot_version],
    )
    if not output.empty:
        conn.register("tmp_underbooking", output)
        conn.execute(
            """
            INSERT INTO underbooking_outputs (
              slot_id, feature_snapshot_version, run_id, scenario_id, pace_gap,
              pace_gap_normalized, fill_gap, fill_gap_normalized, severity_score,
              underbooked, detection_reason
            )
            SELECT * FROM tmp_underbooking
            """
        )
    return output
