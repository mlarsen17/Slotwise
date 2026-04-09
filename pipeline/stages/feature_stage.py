from __future__ import annotations

from datetime import datetime

import duckdb
import pandas as pd

from pipeline.stages.phase2_utils import (
    assign_time_of_day_bucket,
    filter_events_at_or_before,
    safe_rate,
    to_utc_ts,
    trailing_window_mask,
)


def _remaining_provider_slots_same_day(slots: pd.DataFrame, snapshot_ts: pd.Timestamp) -> pd.Series:
    same_day = slots["slot_start_at"].dt.date
    mask_remaining = slots["slot_start_at"] >= snapshot_ts
    return (
        slots[mask_remaining]
        .groupby([slots.loc[mask_remaining, "provider_id"], same_day[mask_remaining]])["slot_id"]
        .transform("count")
        .reindex(slots.index)
        .fillna(0)
        .astype(int)
    )


def materialize_feature_snapshot(
    conn: duckdb.DuckDBPyConnection,
    *,
    scenario_id: str,
    run_id: str,
    feature_snapshot_version: str,
    effective_ts: datetime,
    bucket_boundaries: list[int],
) -> pd.DataFrame:
    """Materialize snapshot-safe features from data available at effective_ts.

    Feature categories:
    - historical features: trailing-window aggregates prior to snapshot
    - current-state features: slot status/pace at snapshot time
    - cohort-derived features: joins from cohort_baselines
    """
    slots = conn.execute(
        """
        SELECT slot_id, provider_id, business_id, service_id, slot_duration_minutes,
               slot_start_at, visible_at, unavailable_at, current_status, scenario_id, run_id
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
    baselines = conn.execute(
        """
        SELECT day_of_week, time_of_day_bucket, service_type, fill_rate, expected_booking_pace_per_day,
               avg_lead_time_hours, completion_rate, is_sparse
        FROM cohort_baselines
        WHERE scenario_id = ? AND run_id = ?
        """,
        [scenario_id, run_id],
    ).fetchdf()

    if slots.empty:
        features = pd.DataFrame()
    else:
        snapshot_ts = to_utc_ts(effective_ts)
        slots["slot_start_at"] = pd.to_datetime(slots["slot_start_at"], utc=True)
        slots["visible_at"] = pd.to_datetime(slots["visible_at"], utc=True)
        slots["unavailable_at"] = pd.to_datetime(slots["unavailable_at"], utc=True)
        slots = slots[slots["visible_at"] <= snapshot_ts].copy()

        events = filter_events_at_or_before(events, effective_ts)

        slots["day_of_week"] = slots["slot_start_at"].dt.day_name()
        slots["time_of_day_bucket"] = slots["slot_start_at"].map(
            lambda ts: assign_time_of_day_bucket(ts.to_pydatetime(), bucket_boundaries)
        )
        slots["service_type"] = slots["service_id"]
        slots["hours_until_slot"] = (slots["slot_start_at"] - snapshot_ts).dt.total_seconds() / 3600
        slots["days_until_slot"] = slots["hours_until_slot"] / 24
        slots["effective_lead_time_band"] = pd.cut(
            slots["hours_until_slot"],
            bins=[-1_000_000, 24, 72, 168, 1_000_000],
            labels=["lt_24h", "24_72h", "72_168h", "gt_168h"],
        ).astype(str)

        booked = events[events["event_type"] == "booked"].copy()
        if not booked.empty:
            first_booked = booked.sort_values("event_at").drop_duplicates("slot_id")
            first_booked = first_booked[["slot_id", "event_at"]].rename(
                columns={"event_at": "first_booked_at"}
            )
        else:
            first_booked = pd.DataFrame(columns=["slot_id", "first_booked_at"])

        completed_slots = set(events[events["event_type"] == "completed"]["slot_id"].tolist())
        slots = slots.merge(first_booked, on="slot_id", how="left")
        slots["booked_flag"] = slots["first_booked_at"].notna().astype(int)
        slots["booked_or_completed"] = (
            (slots["booked_flag"] == 1) | slots["slot_id"].isin(completed_slots)
        ).astype(int)
        slots["lead_time_hours"] = (
            pd.to_datetime(slots["slot_start_at"], utc=True)
            - pd.to_datetime(slots["first_booked_at"], utc=True)
        ).dt.total_seconds() / 3600
        slots["lead_time_hours"] = slots["lead_time_hours"].fillna(0.0)

        active_hours_to_effective = (snapshot_ts - slots["visible_at"]).dt.total_seconds() / 3600
        slots["active_days_elapsed"] = (active_hours_to_effective.clip(lower=0.0) / 24.0).replace(
            0, 1 / 24
        )
        slots["observed_booking_pace_per_day"] = slots.apply(
            lambda row: safe_rate(row["booked_flag"], row["active_days_elapsed"]), axis=1
        )

        same_provider_service = slots.groupby(["provider_id", "service_id"])
        same_business_service = slots.groupby(["business_id", "service_id"])
        slots["hist_fill_rate_similar"] = slots.groupby(
            ["day_of_week", "time_of_day_bucket", "service_id"]
        )["booked_flag"].transform("mean")
        slots["fill_rate_provider_service"] = same_provider_service["booked_flag"].transform("mean")
        slots["fill_rate_business_service"] = same_business_service["booked_flag"].transform("mean")

        slots["remaining_provider_slots_same_day"] = _remaining_provider_slots_same_day(
            slots, snapshot_ts
        )
        slots["remaining_service_slots_window"] = (
            slots[(slots["slot_start_at"] >= snapshot_ts)]
            .groupby(["business_id", "service_id", "day_of_week", "time_of_day_bucket"])["slot_id"]
            .transform("count")
            .reindex(slots.index)
            .fillna(0)
            .astype(int)
        )

        local_density = []
        for _, row in slots.iterrows():
            neighborhood_start = row["slot_start_at"] - pd.Timedelta(hours=2)
            neighborhood_end = row["slot_start_at"] + pd.Timedelta(hours=2)
            density = slots[
                (slots["business_id"] == row["business_id"])
                & (slots["service_id"] == row["service_id"])
                & (slots["slot_start_at"] >= neighborhood_start)
                & (slots["slot_start_at"] <= neighborhood_end)
            ]["slot_id"].nunique()
            local_density.append(float(density))
        slots["inventory_density_2h"] = local_density

        hist_slots = slots[slots["slot_start_at"] <= snapshot_ts].copy()
        for window in (7, 14, 28):
            hist_window = hist_slots[
                trailing_window_mask(hist_slots["slot_start_at"], effective_ts, window)
            ]
            util = hist_window.groupby("provider_id")[
                "booked_or_completed"
            ].sum() / hist_window.groupby("provider_id")["slot_id"].count().replace(0, 1)
            booked_in_window = first_booked.copy()
            booked_in_window = booked_in_window[
                trailing_window_mask(booked_in_window["first_booked_at"], effective_ts, window)
            ]
            provider_booking_volume = (
                slots.merge(booked_in_window[["slot_id"]], on="slot_id", how="inner")
                .groupby("provider_id")["slot_id"]
                .nunique()
            )

            slots[f"provider_utilization_{window}d"] = slots["provider_id"].map(util).fillna(0.0)
            slots[f"booking_volume_{window}d"] = (
                slots["provider_id"].map(provider_booking_volume).fillna(0.0)
            )

        pattern_cols = ["business_id", "day_of_week", "time_of_day_bucket", "service_type"]
        first_events = events.sort_values("event_at").drop_duplicates("slot_id")
        event_by_slot = first_events.set_index("slot_id")["event_type"]
        slots["first_event_type"] = slots["slot_id"].map(event_by_slot)

        slots["cancel_rate_pattern"] = slots.groupby(pattern_cols)["first_event_type"].transform(
            lambda s: safe_rate((s == "canceled").sum(), len(s))
        )
        slots["no_show_rate_pattern"] = slots.groupby(pattern_cols)["first_event_type"].transform(
            lambda s: safe_rate((s == "no_show").sum(), len(s))
        )
        slots["reschedule_rate_pattern"] = slots.groupby(pattern_cols)[
            "first_event_type"
        ].transform(lambda s: safe_rate((s == "rescheduled").sum(), len(s)))
        slots["business_fill_trend"] = slots.groupby("business_id")["booked_flag"].transform("mean")
        slots["business_booking_trend"] = slots.groupby("business_id")[
            "observed_booking_pace_per_day"
        ].transform("mean")

        features = slots.merge(
            baselines,
            left_on=["day_of_week", "time_of_day_bucket", "service_type"],
            right_on=["day_of_week", "time_of_day_bucket", "service_type"],
            how="left",
        )
        features["fill_rate"] = features["fill_rate"].fillna(features["hist_fill_rate_similar"])
        features["expected_booking_pace_per_day"] = features[
            "expected_booking_pace_per_day"
        ].fillna(features["observed_booking_pace_per_day"])
        features["avg_lead_time_hours"] = features["avg_lead_time_hours"].fillna(
            features["lead_time_hours"]
        )
        features["completion_rate"] = features["completion_rate"].fillna(0.0)
        features["is_sparse"] = features["is_sparse"].fillna(True)
        features["pace_deviation"] = (
            features["observed_booking_pace_per_day"] - features["expected_booking_pace_per_day"]
        )
        features["cohort_fill_rate"] = features["fill_rate"]
        features["cohort_completion_rate"] = features["completion_rate"]
        features["cohort_is_sparse"] = features["is_sparse"]

        features["feature_snapshot_version"] = feature_snapshot_version
        features["run_id"] = run_id
        features["scenario_id"] = scenario_id

        features = features[
            [
                "slot_id",
                "feature_snapshot_version",
                "run_id",
                "scenario_id",
                "day_of_week",
                "time_of_day_bucket",
                "service_type",
                "slot_duration_minutes",
                "effective_lead_time_band",
                "hours_until_slot",
                "days_until_slot",
                "hist_fill_rate_similar",
                "fill_rate_provider_service",
                "fill_rate_business_service",
                "expected_booking_pace_per_day",
                "observed_booking_pace_per_day",
                "pace_deviation",
                "avg_lead_time_hours",
                "remaining_provider_slots_same_day",
                "remaining_service_slots_window",
                "inventory_density_2h",
                "provider_utilization_7d",
                "provider_utilization_14d",
                "provider_utilization_28d",
                "booking_volume_7d",
                "booking_volume_14d",
                "booking_volume_28d",
                "cancel_rate_pattern",
                "no_show_rate_pattern",
                "reschedule_rate_pattern",
                "business_fill_trend",
                "business_booking_trend",
                "cohort_fill_rate",
                "cohort_completion_rate",
                "cohort_is_sparse",
            ]
        ]

    conn.execute(
        "DELETE FROM feature_snapshots WHERE scenario_id = ? AND run_id = ? AND feature_snapshot_version = ?",
        [scenario_id, run_id, feature_snapshot_version],
    )
    if not features.empty:
        conn.register("tmp_features", features)
        conn.execute(
            """
            INSERT INTO feature_snapshots (
              slot_id, feature_snapshot_version, run_id, scenario_id, day_of_week, time_of_day_bucket,
              service_type, slot_duration_minutes, effective_lead_time_band, hours_until_slot,
              days_until_slot, hist_fill_rate_similar, fill_rate_provider_service,
              fill_rate_business_service, expected_booking_pace_per_day, observed_booking_pace_per_day,
              pace_deviation, avg_lead_time_hours, remaining_provider_slots_same_day,
              remaining_service_slots_window, inventory_density_2h, provider_utilization_7d,
              provider_utilization_14d, provider_utilization_28d, booking_volume_7d,
              booking_volume_14d, booking_volume_28d, cancel_rate_pattern, no_show_rate_pattern,
              reschedule_rate_pattern, business_fill_trend, business_booking_trend, cohort_fill_rate,
              cohort_completion_rate, cohort_is_sparse
            )
            SELECT * FROM tmp_features
            """
        )
        dupes = conn.execute(
            """
            SELECT COUNT(*) FROM (
              SELECT slot_id, COUNT(*) AS c
              FROM feature_snapshots
              WHERE scenario_id = ? AND run_id = ? AND feature_snapshot_version = ?
              GROUP BY 1
              HAVING COUNT(*) > 1
            )
            """,
            [scenario_id, run_id, feature_snapshot_version],
        ).fetchone()[0]
        if dupes:
            raise ValueError("Duplicate feature snapshot rows detected")
    return features
