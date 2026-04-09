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
    sparse_baseline_fill_rate: float,
) -> pd.DataFrame:
    """Detect underbooked slots from snapshot features with sparse-cohort fallback."""
    features = conn.execute(
        """
        SELECT slot_id, feature_snapshot_version, run_id, scenario_id,
               expected_booking_pace_per_day, observed_booking_pace_per_day,
               cohort_fill_rate, hist_fill_rate_similar, cohort_is_sparse, hours_until_slot
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

        urgency = (1.0 - (features["hours_until_slot"] / 168.0).clip(lower=0.0, upper=1.0)).fillna(
            1.0
        )
        projected_from_pace = (
            features["observed_booking_pace_per_day"]
            * (features["hours_until_slot"].clip(lower=0.0) / 24.0)
        ).clip(upper=1.0)
        projected_fill = (
            0.7 * projected_from_pace + 0.3 * features["hist_fill_rate_similar"]
        ).clip(lower=0.0, upper=1.0)
        features["projected_fill"] = projected_fill * urgency + features["cohort_fill_rate"].fillna(
            0.0
        ) * (1 - urgency)

        sparse_mask = features["cohort_is_sparse"].fillna(True)
        features["expected_fill_baseline"] = features["cohort_fill_rate"].where(
            ~sparse_mask,
            sparse_baseline_fill_rate,
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

        def reason(row: pd.Series) -> str:
            if bool(row.get("cohort_is_sparse", False)):
                return "sparse_cohort_fallback"
            has_pace = row["pace_gap_normalized"] > 0
            has_fill = row["fill_gap_normalized"] > 0
            if has_pace and has_fill:
                return "pace_gap_and_fill_gap"
            if has_pace:
                return "pace_gap"
            if has_fill:
                return "fill_gap"
            return "healthy"

        features["detection_reason"] = features.apply(reason, axis=1)
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
        dupes = conn.execute(
            """
            SELECT COUNT(*) FROM (
              SELECT slot_id, COUNT(*) AS c
              FROM underbooking_outputs
              WHERE scenario_id = ? AND run_id = ? AND feature_snapshot_version = ?
              GROUP BY 1
              HAVING COUNT(*) > 1
            )
            """,
            [scenario_id, run_id, feature_snapshot_version],
        ).fetchone()[0]
        if dupes:
            raise ValueError("Duplicate underbooking rows detected")
    return output
