from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

from pipeline.db import connect


APPOINTMENT_STATUSES = ["booked", "completed", "no_show", "rescheduled"]


@st.cache_data(show_spinner=False)
def load_scenarios(db_path: str) -> list[str]:
    with connect(Path(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT scenario_id
            FROM slots
            WHERE scenario_id IS NOT NULL
            ORDER BY scenario_id
            """
        ).fetchall()
    return [row[0] for row in rows]


@st.cache_data(show_spinner=False)
def load_run_ids(db_path: str, scenario_id: str) -> list[str]:
    with connect(Path(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT run_id
            FROM slots
            WHERE scenario_id = ? AND run_id IS NOT NULL
            ORDER BY run_id DESC
            """,
            [scenario_id],
        ).fetchall()
    return [row[0] for row in rows]


@st.cache_data(show_spinner=False)
def load_week_bounds(db_path: str, scenario_id: str, run_id: str) -> tuple[date, date]:
    with connect(Path(db_path)) as conn:
        min_date, max_date = conn.execute(
            """
            SELECT
              CAST(date_trunc('day', MIN(slot_start_at)) AS DATE),
              CAST(date_trunc('day', MAX(slot_start_at)) AS DATE)
            FROM slots
            WHERE scenario_id = ? AND run_id = ?
            """,
            [scenario_id, run_id],
        ).fetchone()

    if min_date is None or max_date is None:
        today = date.today()
        return today, today

    start = min_date - timedelta(days=min_date.weekday())
    end = max_date - timedelta(days=max_date.weekday())
    return start, end


@st.cache_data(show_spinner=False)
def load_weekly_appointments(
    db_path: str,
    scenario_id: str,
    run_id: str,
    week_start: date,
) -> pd.DataFrame:
    week_end = week_start + timedelta(days=7)
    status_list = ", ".join(f"'{status}'" for status in APPOINTMENT_STATUSES)

    with connect(Path(db_path)) as conn:
        return conn.execute(
            f"""
            SELECT
              s.slot_id,
              s.provider_id,
              s.service_id,
              s.location_id,
              s.current_status,
              s.slot_start_at,
              s.slot_end_at,
              s.standard_price,
              CAST(date_trunc('day', s.slot_start_at) AS DATE) AS appointment_day
            FROM slots s
            WHERE s.scenario_id = ?
              AND s.run_id = ?
              AND s.slot_start_at >= ?
              AND s.slot_start_at < ?
              AND s.current_status IN ({status_list})
            ORDER BY s.slot_start_at
            """,
            [scenario_id, run_id, week_start, week_end],
        ).df()


def render_dashboard() -> None:
    st.set_page_config(page_title="Slotwise Weekly Appointments", layout="wide")
    st.title("Slotwise: Weekly Appointments")
    st.caption("View a week of appointment inventory and average full-rate pricing.")

    with st.sidebar:
        st.header("Filters")
        db_path = st.text_input("DuckDB path", value="data/mvp.duckdb")

    if not Path(db_path).exists():
        st.error(f"Database file not found: {db_path}")
        st.stop()

    scenarios = load_scenarios(db_path)
    if not scenarios:
        st.warning("No scenarios found in slots table. Run the pipeline first.")
        st.stop()

    with st.sidebar:
        scenario_id = st.selectbox("Scenario", scenarios, index=len(scenarios) - 1)

    run_ids = load_run_ids(db_path, scenario_id)
    if not run_ids:
        st.warning(f"No run_ids found for scenario '{scenario_id}'.")
        st.stop()

    with st.sidebar:
        run_id = st.selectbox("Run ID", run_ids, index=0)

    week_min, week_max = load_week_bounds(db_path, scenario_id, run_id)
    with st.sidebar:
        week_start = st.date_input(
            "Week start (Monday)",
            value=week_max,
            min_value=week_min,
            max_value=week_max,
        )

    week_start = week_start - timedelta(days=week_start.weekday())
    weekly_df = load_weekly_appointments(db_path, scenario_id, run_id, week_start)

    st.subheader(f"Week of {week_start.isoformat()} (appointments only)")

    if weekly_df.empty:
        st.info("No appointments found for this week and filter set.")
        return

    avg_full_rate = float(weekly_df["standard_price"].mean())
    total_appts = int(len(weekly_df))
    unique_providers = int(weekly_df["provider_id"].nunique())

    col1, col2, col3 = st.columns(3)
    col1.metric("Appointments", total_appts)
    col2.metric("Average Full Rate", f"${avg_full_rate:,.2f}")
    col3.metric("Providers", unique_providers)

    day_summary = (
        weekly_df.groupby("appointment_day", as_index=False)
        .agg(appointments=("slot_id", "count"), avg_full_rate=("standard_price", "mean"))
        .sort_values("appointment_day")
    )
    day_summary["avg_full_rate"] = day_summary["avg_full_rate"].map(lambda v: round(float(v), 2))

    st.markdown("#### Daily summary")
    st.dataframe(day_summary, use_container_width=True, hide_index=True)

    detail_view = weekly_df.copy()
    detail_view["slot_start_at"] = pd.to_datetime(detail_view["slot_start_at"]).dt.strftime(
        "%Y-%m-%d %H:%M"
    )
    detail_view["slot_end_at"] = pd.to_datetime(detail_view["slot_end_at"]).dt.strftime(
        "%Y-%m-%d %H:%M"
    )
    detail_view = detail_view.rename(
        columns={
            "slot_start_at": "start",
            "slot_end_at": "end",
            "current_status": "status",
            "standard_price": "full_rate",
        }
    )

    st.markdown("#### Appointment detail")
    st.dataframe(
        detail_view[
            [
                "appointment_day",
                "start",
                "end",
                "provider_id",
                "service_id",
                "location_id",
                "status",
                "full_rate",
                "slot_id",
            ]
        ],
        use_container_width=True,
        hide_index=True,
    )


if __name__ == "__main__":
    render_dashboard()
