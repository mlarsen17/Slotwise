from __future__ import annotations

import duckdb
import pandas as pd

METRIC_NAMES = (
    "core_slots_count",
    "feature_snapshot_count",
    "scoring_output_count",
    "pricing_action_count",
    "underbooked_rate",
    "discounted_action_rate",
    "healthy_zero_rate",
    "rationale_coverage",
    "eligible_discount_compliance_rate",
    "discount_shortfall_correlation",
    "low_demand_flag_rate",
    "healthy_overflag_rate",
    "eligible_underbooked_discount_rate",
    "similar_slot_action_consistency",
)


def run_evaluation_suite(
    conn: duckdb.DuckDBPyConnection,
    *,
    scenario_id: str,
    run_id: str,
    feature_snapshot_version: str,
) -> pd.DataFrame:
    slots_count = conn.execute(
        "SELECT COUNT(*) FROM slots WHERE run_id = ? AND scenario_id = ?",
        [run_id, scenario_id],
    ).fetchone()[0]
    feature_count = conn.execute(
        """
        SELECT COUNT(*) FROM feature_snapshots
        WHERE run_id = ? AND scenario_id = ? AND feature_snapshot_version = ?
        """,
        [run_id, scenario_id, feature_snapshot_version],
    ).fetchone()[0]
    pricing_count = conn.execute(
        "SELECT COUNT(*) FROM pricing_actions WHERE run_id = ? AND scenario_id = ?",
        [run_id, scenario_id],
    ).fetchone()[0]
    if slots_count == 0 or feature_count == 0 or pricing_count == 0:
        raise ValueError(
            "Evaluation requires populated slots, feature_snapshots, and pricing_actions"
        )

    metrics: list[tuple[str, float]] = []

    def metric(name: str, query: str, params: list | None = None) -> float:
        row = conn.execute(query, params or []).fetchone()
        value = float(row[0] or 0.0)
        metrics.append((name, value))
        return value

    metric(
        "core_slots_count",
        "SELECT COUNT(*) FROM slots WHERE run_id = ? AND scenario_id = ?",
        [run_id, scenario_id],
    )
    metric(
        "feature_snapshot_count",
        """
        SELECT COUNT(*) FROM feature_snapshots
        WHERE run_id = ? AND scenario_id = ? AND feature_snapshot_version = ?
        """,
        [run_id, scenario_id, feature_snapshot_version],
    )
    metric(
        "scoring_output_count",
        """
        SELECT COUNT(*) FROM scoring_outputs
        WHERE run_id = ? AND scenario_id = ? AND feature_snapshot_version = ?
        """,
        [run_id, scenario_id, feature_snapshot_version],
    )
    metric(
        "pricing_action_count",
        "SELECT COUNT(*) FROM pricing_actions WHERE run_id = ? AND scenario_id = ?",
        [run_id, scenario_id],
    )
    metric(
        "underbooked_rate",
        """
        SELECT COALESCE(AVG(CASE WHEN underbooked THEN 1.0 ELSE 0.0 END), 0.0)
        FROM underbooking_outputs
        WHERE run_id = ? AND scenario_id = ? AND feature_snapshot_version = ?
        """,
        [run_id, scenario_id, feature_snapshot_version],
    )
    metric(
        "discounted_action_rate",
        """
        SELECT COALESCE(AVG(CASE WHEN action_value > 0 THEN 1.0 ELSE 0.0 END), 0.0)
        FROM pricing_actions
        WHERE run_id = ? AND scenario_id = ?
        """,
        [run_id, scenario_id],
    )
    metric(
        "healthy_zero_rate",
        """
        SELECT COALESCE(AVG(CASE WHEN COALESCE(u.underbooked, FALSE)=FALSE AND p.action_value = 0 THEN 1.0 ELSE 0.0 END), 0.0)
        FROM pricing_actions p
        LEFT JOIN underbooking_outputs u
          ON p.slot_id = u.slot_id
         AND p.run_id = u.run_id
         AND p.scenario_id = u.scenario_id
         AND u.feature_snapshot_version = ?
        WHERE p.run_id = ? AND p.scenario_id = ?
        """,
        [feature_snapshot_version, run_id, scenario_id],
    )
    metric(
        "rationale_coverage",
        """
        SELECT COALESCE(AVG(CASE WHEN rationale_codes IS NOT NULL AND rationale_codes <> '[]' THEN 1.0 ELSE 0.0 END), 0.0)
        FROM pricing_actions
        WHERE run_id = ? AND scenario_id = ?
        """,
        [run_id, scenario_id],
    )
    metric(
        "eligible_discount_compliance_rate",
        """
        SELECT COALESCE(AVG(
            CASE
              WHEN list_contains(
                from_json(eligible_action_set, '["INTEGER"]'),
                CAST(action_value AS INTEGER)
              ) THEN 1.0
              ELSE 0.0
            END
        ), 0.0)
        FROM pricing_actions
        WHERE run_id = ? AND scenario_id = ?
        """,
        [run_id, scenario_id],
    )
    metric(
        "discount_shortfall_correlation",
        """
        SELECT COALESCE(
          corr(CAST(p.action_value AS DOUBLE), CAST(s.shortfall_score AS DOUBLE)),
          0.0
        )
        FROM pricing_actions p
        JOIN scoring_outputs s
          ON p.slot_id = s.slot_id
         AND p.run_id = s.run_id
         AND p.scenario_id = s.scenario_id
         AND p.feature_snapshot_version = s.feature_snapshot_version
        WHERE p.run_id = ? AND p.scenario_id = ? AND p.action_value > 0
        """,
        [run_id, scenario_id],
    )
    metric(
        "low_demand_flag_rate",
        """
        SELECT COALESCE(AVG(CASE WHEN pace_gap_normalized >= 0.30 AND underbooked THEN 1.0 ELSE 0.0 END), 0.0)
        FROM underbooking_outputs
        WHERE run_id = ? AND scenario_id = ? AND feature_snapshot_version = ?
        """,
        [run_id, scenario_id, feature_snapshot_version],
    )
    metric(
        "healthy_overflag_rate",
        """
        SELECT COALESCE(AVG(CASE WHEN pace_gap_normalized <= 0.05 AND underbooked THEN 1.0 ELSE 0.0 END), 0.0)
        FROM underbooking_outputs
        WHERE run_id = ? AND scenario_id = ? AND feature_snapshot_version = ?
        """,
        [run_id, scenario_id, feature_snapshot_version],
    )
    metric(
        "eligible_underbooked_discount_rate",
        """
        SELECT COALESCE(AVG(
          CASE
            WHEN u.underbooked
                 AND list_contains(
                   from_json(p.eligible_action_set, '["INTEGER"]'),
                   CAST(p.action_value AS INTEGER)
                 )
                 AND p.action_value > 0
            THEN 1.0 ELSE 0.0
          END
        ), 0.0)
        FROM pricing_actions p
        JOIN underbooking_outputs u
          ON p.slot_id = u.slot_id
         AND p.run_id = u.run_id
         AND p.scenario_id = u.scenario_id
         AND p.feature_snapshot_version = u.feature_snapshot_version
        WHERE p.run_id = ? AND p.scenario_id = ? AND p.feature_snapshot_version = ?
        """,
        [run_id, scenario_id, feature_snapshot_version],
    )
    metric(
        "similar_slot_action_consistency",
        """
        WITH grp AS (
          SELECT s.provider_id, s.service_id, f.time_of_day_bucket,
                 AVG(p.action_value) AS avg_action,
                 STDDEV_SAMP(p.action_value) AS std_action,
                 COUNT(*) AS n
          FROM pricing_actions p
          JOIN slots s
            ON p.slot_id = s.slot_id AND p.run_id = s.run_id AND p.scenario_id = s.scenario_id
          JOIN feature_snapshots f
            ON p.slot_id = f.slot_id AND p.run_id = f.run_id AND p.scenario_id = f.scenario_id
           AND p.feature_snapshot_version = f.feature_snapshot_version
          WHERE p.run_id = ? AND p.scenario_id = ? AND p.feature_snapshot_version = ?
          GROUP BY 1, 2, 3
          HAVING COUNT(*) >= 2
        )
        SELECT COALESCE(AVG(
          CASE
            WHEN avg_action = 0 THEN CASE WHEN COALESCE(std_action, 0) = 0 THEN 1.0 ELSE 0.0 END
            ELSE GREATEST(0.0, 1.0 - (COALESCE(std_action, 0.0) / avg_action))
          END
        ), 1.0)
        FROM grp
        """,
        [run_id, scenario_id, feature_snapshot_version],
    )

    output = pd.DataFrame(metrics, columns=["metric_name", "metric_value"])
    present_metrics = {name for name, _ in metrics}
    if set(METRIC_NAMES) != present_metrics:
        missing = sorted(set(METRIC_NAMES) - present_metrics)
        extra = sorted(present_metrics - set(METRIC_NAMES))
        raise ValueError(f"Unexpected evaluation metric set. missing={missing} extra={extra}")
    output["run_id"] = run_id
    output["scenario_id"] = scenario_id

    conn.execute(
        "DELETE FROM evaluation_results WHERE run_id = ? AND scenario_id = ?", [run_id, scenario_id]
    )
    conn.register("tmp_evaluation_results", output)
    conn.execute(
        """
        INSERT INTO evaluation_results (run_id, metric_name, metric_value, scenario_id)
        SELECT run_id, metric_name, metric_value, scenario_id
        FROM tmp_evaluation_results
        """
    )
    return output
