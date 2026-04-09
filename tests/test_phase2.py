from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from medscheduler_wrapper.extract import extract_synthetic_data
from medscheduler_wrapper.normalize import normalize_records
from medscheduler_wrapper.scenario_config import ScenarioConfig
from pipeline.db import bootstrap_db, connect
from pipeline.stages.availability_stage import apply_availability
from pipeline.stages.baseline_stage import compute_cohort_baselines
from pipeline.stages.feature_stage import materialize_feature_snapshot
from pipeline.stages.load_stage import load_core_tables
from pipeline.stages.phase2_utils import assign_time_of_day_bucket
from pipeline.stages.underbooking_stage import detect_underbooking


def _norm_phase2(run_id: str = "run_phase2") -> dict:
    cfg = ScenarioConfig(
        scenario_id="phase2_scenario",
        source_run_id="source_2",
        random_seed=222,
        effective_ts="2026-01-10T00:00:00Z",
        business_count=1,
        providers_per_business=1,
        services_per_business=2,
        locations_per_business=1,
        customers_per_business=10,
        days=5,
        slots_per_provider_per_day=3,
        cancellation_rate=0.1,
        removal_rate=0.0,
    )
    return normalize_records(
        extract_synthetic_data(cfg), "phase2_scenario", "source_2", run_id=run_id
    )


def _setup_phase2(conn, run_id: str = "run_phase2") -> str:
    norm = _norm_phase2(run_id)
    load_core_tables(
        conn,
        norm,
        scenario_id="phase2_scenario",
        run_id=run_id,
        effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
        config_hash="cfg_phase2",
    )
    apply_availability(
        conn,
        "phase2_scenario",
        run_id=run_id,
        effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
    )
    fsv = "fsv_test"
    compute_cohort_baselines(
        conn,
        scenario_id="phase2_scenario",
        run_id=run_id,
        feature_snapshot_version=fsv,
        effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
        bucket_boundaries=[0, 12, 17, 24],
    )
    return fsv


def test_time_of_day_bucket_edges() -> None:
    assert assign_time_of_day_bucket(datetime(2026, 1, 1, 11, 59), [0, 12, 17, 24]) == "00-12"
    assert assign_time_of_day_bucket(datetime(2026, 1, 1, 12, 0), [0, 12, 17, 24]) == "12-17"
    assert assign_time_of_day_bucket(datetime(2026, 1, 1, 17, 0), [0, 12, 17, 24]) == "17-24"


def test_cohort_baselines_materialize(tmp_path: Path) -> None:
    with connect(tmp_path / "phase2_baselines.duckdb") as conn:
        bootstrap_db(conn)
        fsv = _setup_phase2(conn)
        count = conn.execute(
            "SELECT COUNT(*) FROM cohort_baselines WHERE run_id = 'run_phase2' AND feature_snapshot_version = ?",
            [fsv],
        ).fetchone()[0]
    assert count > 0


def test_feature_materialization_is_idempotent(tmp_path: Path) -> None:
    with connect(tmp_path / "phase2_features.duckdb") as conn:
        bootstrap_db(conn)
        fsv = _setup_phase2(conn)
        materialize_feature_snapshot(
            conn,
            scenario_id="phase2_scenario",
            run_id="run_phase2",
            feature_snapshot_version=fsv,
            effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
            bucket_boundaries=[0, 12, 17, 24],
        )
        first = conn.execute(
            "SELECT COUNT(*) FROM feature_snapshots WHERE run_id = 'run_phase2'"
        ).fetchone()[0]
        materialize_feature_snapshot(
            conn,
            scenario_id="phase2_scenario",
            run_id="run_phase2",
            feature_snapshot_version=fsv,
            effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
            bucket_boundaries=[0, 12, 17, 24],
        )
        second = conn.execute(
            "SELECT COUNT(*) FROM feature_snapshots WHERE run_id = 'run_phase2'"
        ).fetchone()[0]
    assert first == second


def test_underbooking_severity_bounds(tmp_path: Path) -> None:
    with connect(tmp_path / "phase2_underbooking.duckdb") as conn:
        bootstrap_db(conn)
        fsv = _setup_phase2(conn)
        materialize_feature_snapshot(
            conn,
            scenario_id="phase2_scenario",
            run_id="run_phase2",
            feature_snapshot_version=fsv,
            effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
            bucket_boundaries=[0, 12, 17, 24],
        )
        detect_underbooking(
            conn,
            scenario_id="phase2_scenario",
            run_id="run_phase2",
            feature_snapshot_version=fsv,
            pace_weight=0.6,
            fill_weight=0.4,
            underbooking_threshold=0.2,
        )
        low, high = conn.execute(
            "SELECT MIN(severity_score), MAX(severity_score) FROM underbooking_outputs WHERE run_id='run_phase2'"
        ).fetchone()
    assert 0.0 <= low <= 1.0
    assert 0.0 <= high <= 1.0


def test_depressed_demand_scenario_flags_underbooked(tmp_path: Path) -> None:
    with connect(tmp_path / "phase2_depressed.duckdb") as conn:
        bootstrap_db(conn)
        norm = _norm_phase2("run_depressed")
        norm["booking_events"] = norm["booking_events"].iloc[0:0].copy()
        load_core_tables(
            conn,
            norm,
            scenario_id="phase2_scenario",
            run_id="run_depressed",
            effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
            config_hash="cfg_depressed",
        )
        apply_availability(
            conn,
            "phase2_scenario",
            run_id="run_depressed",
            effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
        )
        fsv = "fsv_depressed"
        compute_cohort_baselines(
            conn,
            scenario_id="phase2_scenario",
            run_id="run_depressed",
            feature_snapshot_version=fsv,
            effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
            bucket_boundaries=[0, 12, 17, 24],
        )
        materialize_feature_snapshot(
            conn,
            scenario_id="phase2_scenario",
            run_id="run_depressed",
            feature_snapshot_version=fsv,
            effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
            bucket_boundaries=[0, 12, 17, 24],
        )
        detect_underbooking(
            conn,
            scenario_id="phase2_scenario",
            run_id="run_depressed",
            feature_snapshot_version=fsv,
            pace_weight=0.7,
            fill_weight=0.3,
            underbooking_threshold=0.05,
        )
        flagged, total = conn.execute(
            "SELECT SUM(CASE WHEN underbooked THEN 1 ELSE 0 END), COUNT(*) FROM underbooking_outputs WHERE run_id='run_depressed'"
        ).fetchone()
    assert total > 0
    assert flagged > 0
