from __future__ import annotations

from datetime import datetime

import duckdb
import pandas as pd

from pipeline.stages.phase2_utils import assign_time_of_day_bucket, safe_rate


MIN_COHORT_OBS = 3


def compute_cohort_baselines(
    conn: duckdb.DuckDBPyConnection,
    *,
    scenario_id: str,
    run_id: str,
    feature_snapshot_version: str,
    effective_ts: datetime,
    bucket_boundaries: list[int],
) -> pd.DataFrame:
    slots = conn.execute(
        """
        SELECT slot_id, business_id, provider_id, service_id, slot_start_at, visible_at, unavailable_at,
               current_status, scenario_id, run_id
        FROM slots
        WHERE scenario_id = ? AND run_id = ?
        """,
        [scenario_id, run_id],
    ).fetchdf()
    events = conn.execute(
        """
        SELECT slot_id, event_type, event_at
        FROM booking_events
        WHERE scenario_id = ? AND run_id = ?
        """,
        [scenario_id, run_id],
    ).fetchdf()

    if slots.empty:
        baselines = pd.DataFrame(
            columns=[
                "cohort_id",
                "day_of_week",
                "time_of_day_bucket",
                "service_type",
                "observation_count",
                "fill_rate",
                "expected_booking_pace_per_day",
                "avg_lead_time_hours",
                "completion_rate",
                "is_sparse",
                "feature_snapshot_version",
                "run_id",
                "scenario_id",
            ]
        )
    else:
        slots["slot_start_at"] = pd.to_datetime(slots["slot_start_at"], utc=True)
        slots["visible_at"] = pd.to_datetime(slots["visible_at"], utc=True)
        slots["unavailable_at"] = pd.to_datetime(slots["unavailable_at"], utc=True)
        slots["day_of_week"] = slots["slot_start_at"].dt.day_name()
        slots["time_of_day_bucket"] = slots["slot_start_at"].map(
            lambda ts: assign_time_of_day_bucket(ts.to_pydatetime(), bucket_boundaries)
        )
        slots["service_type"] = slots["service_id"]

        booked = events[events["event_type"] == "booked"].copy()
        completed = events[events["event_type"] == "completed"].copy()

        if not booked.empty:
            booked["event_at"] = pd.to_datetime(booked["event_at"], utc=True)
            first_booked = booked.sort_values("event_at").drop_duplicates("slot_id")
            first_booked = first_booked[["slot_id", "event_at"]].rename(
                columns={"event_at": "first_booked_at"}
            )
        else:
            first_booked = pd.DataFrame(columns=["slot_id", "first_booked_at"])

        if not completed.empty:
            completed_slots = set(completed["slot_id"].tolist())
        else:
            completed_slots = set()

        slots = slots.merge(first_booked, on="slot_id", how="left")
        slots["booked_flag"] = slots["first_booked_at"].notna().astype(int)
        slots["completed_flag"] = slots["slot_id"].isin(completed_slots).astype(int)
        slots["lead_time_hours"] = (
            pd.to_datetime(slots["slot_start_at"], utc=True)
            - pd.to_datetime(slots["first_booked_at"], utc=True)
        ).dt.total_seconds() / 3600
        slots["lead_time_hours"] = slots["lead_time_hours"].fillna(0.0)

        active_hours = (
            pd.to_datetime(slots["unavailable_at"], utc=True)
            - pd.to_datetime(slots["visible_at"], utc=True)
        ).dt.total_seconds() / 3600
        slots["active_days"] = (active_hours.clip(lower=0.0) / 24.0).replace(0, 1 / 24)

        group_cols = ["day_of_week", "time_of_day_bucket", "service_type"]
        grouped = slots.groupby(group_cols, dropna=False)
        baselines = grouped.agg(
            observation_count=("slot_id", "count"),
            fill_rate=("booked_flag", "mean"),
            avg_lead_time_hours=("lead_time_hours", "mean"),
            completion_rate=("completed_flag", "mean"),
            bookings=("booked_flag", "sum"),
            active_days=("active_days", "sum"),
        ).reset_index()
        baselines["expected_booking_pace_per_day"] = baselines.apply(
            lambda row: safe_rate(row["bookings"], row["active_days"]), axis=1
        )
        baselines["is_sparse"] = baselines["observation_count"] < MIN_COHORT_OBS
        baselines["cohort_id"] = baselines.apply(
            lambda row: f"{row['day_of_week']}|{row['time_of_day_bucket']}|{row['service_type']}",
            axis=1,
        )
        baselines["feature_snapshot_version"] = feature_snapshot_version
        baselines["run_id"] = run_id
        baselines["scenario_id"] = scenario_id
        baselines = baselines[
            [
                "cohort_id",
                "day_of_week",
                "time_of_day_bucket",
                "service_type",
                "observation_count",
                "fill_rate",
                "expected_booking_pace_per_day",
                "avg_lead_time_hours",
                "completion_rate",
                "is_sparse",
                "feature_snapshot_version",
                "run_id",
                "scenario_id",
            ]
        ]

    conn.register("tmp_cohort_baselines", baselines)
    conn.execute(
        "DELETE FROM cohort_baselines WHERE scenario_id = ? AND run_id = ?", [scenario_id, run_id]
    )
    conn.execute(
        """
        INSERT INTO cohort_baselines (
          cohort_id, day_of_week, time_of_day_bucket, service_type, observation_count,
          fill_rate, expected_booking_pace_per_day, avg_lead_time_hours, completion_rate,
          is_sparse, feature_snapshot_version, run_id, scenario_id
        )
        SELECT * FROM tmp_cohort_baselines
        """
    )
    return baselines
