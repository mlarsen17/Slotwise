from __future__ import annotations

import hashlib
import json
from datetime import datetime

import duckdb
import pandas as pd

from optimizer.eligibility import build_eligible_action_set
from optimizer.exploration import maybe_apply_exploration
from optimizer.rationale import generate_rationale_codes
from optimizer.recommend import select_discount


def recommend_pricing_actions(
    conn: duckdb.DuckDBPyConnection,
    *,
    scenario_id: str,
    run_id: str,
    feature_snapshot_version: str,
    effective_ts: datetime,
    random_seed: int,
    action_ladder: list[int],
    lead_time_windows_hours: list[int],
    max_discount_pct: int,
    excluded_services: list[str],
    price_floor_pct: float,
    healthy_zero_only: bool,
    severity_breakpoints: list[float],
    discount_steps: list[int],
    exploration_share: float,
) -> pd.DataFrame:
    if not 0.0 <= exploration_share <= 1.0:
        raise ValueError("exploration_share must be in [0, 1]")

    df = conn.execute(
        """
        SELECT s.slot_id, s.business_id, s.provider_id, s.service_id, s.standard_price, s.slot_start_at,
               f.hours_until_slot, f.provider_utilization_7d,
               u.underbooked, u.severity_score, u.pace_gap_normalized,
               sc.shortfall_score, sc.confidence_score
        FROM slots s
        JOIN feature_snapshots f
          ON f.slot_id = s.slot_id
         AND f.run_id = s.run_id
         AND f.scenario_id = s.scenario_id
         AND f.feature_snapshot_version = ?
        LEFT JOIN underbooking_outputs u
          ON u.slot_id = s.slot_id
         AND u.run_id = s.run_id
         AND u.scenario_id = s.scenario_id
         AND u.feature_snapshot_version = ?
        LEFT JOIN scoring_outputs sc
          ON sc.slot_id = s.slot_id
         AND sc.run_id = s.run_id
         AND sc.scenario_id = s.scenario_id
         AND sc.feature_snapshot_version = ?
        WHERE s.scenario_id = ?
          AND s.run_id = ?
          AND s.current_status = 'open'
          AND s.slot_start_at >= ?
        """,
        [
            feature_snapshot_version,
            feature_snapshot_version,
            feature_snapshot_version,
            scenario_id,
            run_id,
            effective_ts,
        ],
    ).fetchdf()

    if df.empty:
        output = pd.DataFrame()
    else:
        allowed_lead_time = max(lead_time_windows_hours) if lead_time_windows_hours else 0
        floor_multiplier = max(min(price_floor_pct, 1.0), 0.0)

        records: list[dict] = []
        for _, row in df.iterrows():
            eligible = build_eligible_action_set(
                action_ladder=action_ladder,
                max_discount_pct=max_discount_pct,
                service_id=str(row["service_id"]),
                excluded_services=excluded_services,
                hours_until_slot=float(row["hours_until_slot"]),
                allowed_lead_time=allowed_lead_time,
                standard_price=float(row["standard_price"]),
                floor_multiplier=floor_multiplier,
            )

            underbooked = bool(row["underbooked"]) if pd.notna(row["underbooked"]) else False
            severity = float(
                row["shortfall_score"]
                if pd.notna(row["shortfall_score"])
                else row["severity_score"] or 0.0
            )
            chosen = select_discount(
                underbooked=underbooked,
                healthy_zero_only=healthy_zero_only,
                severity=severity,
                breakpoints=severity_breakpoints,
                discounts=discount_steps,
                eligible_actions=eligible,
            )
            chosen, was_exploration, policy, reason = maybe_apply_exploration(
                random_seed=random_seed,
                run_id=run_id,
                slot_id=str(row["slot_id"]),
                exploration_share=exploration_share,
                underbooked=underbooked,
                chosen_discount=chosen,
                eligible_actions=eligible,
            )
            rationale = generate_rationale_codes(
                underbooked=underbooked,
                hours_until_slot=float(row["hours_until_slot"]),
                provider_utilization_7d=float(row["provider_utilization_7d"] or 0.0),
                chosen_discount=int(chosen),
            )

            action_id = hashlib.sha256(
                f"{run_id}|{feature_snapshot_version}|{row['slot_id']}".encode()
            ).hexdigest()[:16]
            records.append(
                {
                    "action_id": f"act_{action_id}",
                    "slot_id": row["slot_id"],
                    "action_type": "discount_pct",
                    "action_value": float(chosen),
                    "eligible_action_set": json.dumps(sorted(set(eligible))),
                    "decision_reason": reason,
                    "was_exploration": was_exploration,
                    "exploration_policy": policy,
                    "decision_timestamp": effective_ts,
                    "feature_snapshot_version": feature_snapshot_version,
                    "confidence_score": float(row["confidence_score"] or 0.0),
                    "rationale_codes": json.dumps(rationale),
                    "run_id": run_id,
                    "scenario_id": scenario_id,
                }
            )

        output = pd.DataFrame.from_records(records)

    conn.execute(
        "DELETE FROM pricing_actions WHERE scenario_id = ? AND run_id = ?", [scenario_id, run_id]
    )
    if not output.empty:
        conn.register("tmp_pricing_actions", output)
        conn.execute(
            """
            INSERT INTO pricing_actions (
              action_id, slot_id, action_type, action_value, eligible_action_set,
              decision_reason, was_exploration, exploration_policy, decision_timestamp,
              feature_snapshot_version, confidence_score, rationale_codes, run_id, scenario_id
            )
            SELECT * FROM tmp_pricing_actions
            """
        )
    return output
