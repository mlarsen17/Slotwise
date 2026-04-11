from __future__ import annotations

import streamlit as st

from app.data_access import AppDataAccess

st.set_page_config(page_title="Slotwise Analytics", layout="wide")
st.title("Slotwise — Recommendation Explorer")

db_path = st.sidebar.text_input("DuckDB path", "data/mvp.duckdb")
da = AppDataAccess(db_path)
runs = da.latest_runs()
if runs.empty:
    st.warning("No runs found. Execute pipeline first.")
    st.stop()

run_options = [f"{r.run_id} | {r.scenario_id}" for r in runs.itertuples(index=False)]
selected = st.sidebar.selectbox("Run", run_options)
selected_run_id, selected_scenario_id = [part.strip() for part in selected.split("|")]

recs = da.recommendations(selected_run_id, selected_scenario_id)
if recs.empty:
    st.warning("No recommendations for selected run/scenario.")
    st.stop()

businesses = ["All"] + sorted(recs["business_id"].dropna().unique().tolist())
providers = ["All"] + sorted(recs["provider_id"].dropna().unique().tolist())
services = ["All"] + sorted(recs["service_id"].dropna().unique().tolist())
bands = ["All"] + sorted(recs["effective_lead_time_band"].dropna().unique().tolist())

biz = st.sidebar.selectbox("Business", businesses)
provider = st.sidebar.selectbox("Provider", providers)
service = st.sidebar.selectbox("Service", services)
lead_band = st.sidebar.selectbox("Lead-time band", bands)
discounted_only = st.sidebar.checkbox("Discounted only", False)
exploration_only = st.sidebar.checkbox("Exploration only", False)

filtered = recs.copy()
if biz != "All":
    filtered = filtered[filtered["business_id"] == biz]
if provider != "All":
    filtered = filtered[filtered["provider_id"] == provider]
if service != "All":
    filtered = filtered[filtered["service_id"] == service]
if lead_band != "All":
    filtered = filtered[filtered["effective_lead_time_band"] == lead_band]
if discounted_only:
    filtered = filtered[filtered["recommended_discount"] > 0]
if exploration_only:
    filtered = filtered[filtered["was_exploration"]]

st.subheader("Slot-level recommendations")
st.dataframe(
    filtered.sort_values(["severity_score", "recommended_discount"], ascending=[False, False])
)

left, right = st.columns(2)
with left:
    st.subheader("Action distribution")
    st.bar_chart(filtered.groupby("recommended_discount").size())
with right:
    st.subheader("By provider")
    st.bar_chart(filtered.groupby("provider_id").size())

st.subheader("Severity score distribution")
severity_distribution = da.severity_distribution(selected_run_id, selected_scenario_id)
if severity_distribution.empty:
    st.info("No severity rows available for selected run.")
else:
    st.bar_chart(severity_distribution.set_index("severity_band")["slot_count"])

st.subheader("Recommendation summaries")
summary = da.summary_counts(selected_run_id, selected_scenario_id)
sum_left, sum_mid, sum_right = st.columns(3)
with sum_left:
    st.caption("By action bucket")
    st.dataframe(summary["by_action"], use_container_width=True)
with sum_mid:
    st.caption("By provider")
    st.dataframe(summary["by_provider"], use_container_width=True)
with sum_right:
    st.caption("By service")
    st.dataframe(summary["by_service"], use_container_width=True)
st.caption("By lead-time band")
st.dataframe(summary["by_lead_time_band"], use_container_width=True)

st.subheader("Evaluation metrics")
st.dataframe(da.evaluation(selected_run_id, selected_scenario_id))
