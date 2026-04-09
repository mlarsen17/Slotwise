from __future__ import annotations

import hashlib
import json
import random
from datetime import datetime

import duckdb
import pandas as pd


def _deterministic_rng(seed: int, *parts: str) -> random.Random:
    digest = hashlib.sha256(f"{seed}|{'|'.join(parts)}".encode()).hexdigest()[:16]
    return random.Random(int(digest, 16))


def _discount_from_severity(severity: float, breakpoints: list[float], discounts: list[int]) -> int:
    for idx, threshold in enumerate(breakpoints):
        if severity <= threshold:
            return discounts[idx]
    return discounts[-1]


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
        [feature_snapshot_version, feature_snapshot_version, feature_snapshot_version, scenario_id, run_id, effective_ts],
    ).fetchdf()

    if df.empty:
        output = pd.DataFrame()
    else:
        allowed_lead_time = max(lead_time_windows_hours) if lead_time_windows_hours else 0
        floor_multiplier = max(min(price_floor_pct, 1.0), 0.0)

        records: list[dict] = []
        for _, row in df.iterrows():
            ladder = sorted(set(int(v) for v in action_ladder if 0 <= int(v) <= max_discount_pct))
            if 0 not in ladder:
                ladder.insert(0, 0)

            eligible = []
            for discount in ladder:
                if row["service_id"] in excluded_services and discount > 0:
                    continue
                if row["hours_until_slot"] > allowed_lead_time and discount > 0:
                    continue
                implied_price = float(row["standard_price"]) * (1 - discount / 100.0)
                if implied_price < float(row["standard_price"]) * floor_multiplier:
                    continue
                eligible.append(discount)
            if not eligible:
                eligible = [0]

            underbooked = bool(row["underbooked"]) if pd.notna(row["underbooked"]) else False
            severity = float(row["shortfall_score"] if pd.notna(row["shortfall_score"]) else row["severity_score"] or 0.0)
            target = 0 if (healthy_zero_only and not underbooked) else _discount_from_severity(
                severity,
                breakpoints=severity_breakpoints,
                discounts=discount_steps,
            )
            chosen = max([v for v in eligible if v <= target], default=min(eligible))
            reason = "underbooked_optimizer" if underbooked else "healthy_no_discount"
            was_exploration = False
            policy = "none"

            rng = _deterministic_rng(random_seed, run_id, str(row["slot_id"]))
            if underbooked and len(eligible) > 1 and rng.random() < exploration_share:
                chosen = rng.choice([x for x in eligible if x != chosen] or eligible)
                was_exploration = True
                policy = "epsilon_greedy_deterministic"
                reason = "exploration_override"

            rationale = []
            if underbooked:
                rationale.append("booking_pace_below_baseline")
            if float(row["hours_until_slot"]) <= 24 and underbooked:
                rationale.append("short_lead_time_low_fill")
            if float(row["provider_utilization_7d"] or 0.0) < 0.5 and underbooked:
                rationale.append("provider_utilization_below_target")
            if not rationale and chosen > 0:
                rationale.append("historically_underbooked_weekday_afternoon")
            if not rationale:
                rationale.append("healthy_slot_no_discount")

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

    conn.execute("DELETE FROM pricing_actions WHERE scenario_id = ? AND run_id = ?", [scenario_id, run_id])
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
