from __future__ import annotations

from pathlib import Path

import yaml

from app.data_access import AppDataAccess
from pipeline.db import connect
from pipeline.run_pipeline import run


def _write_config(tmp_path: Path, *, run_id: str) -> Path:
    payload = {
        "duckdb_path": str(tmp_path / "phase4.duckdb"),
        "scenario_id": "phase4_scenario",
        "source_run_id": "synthetic_run_phase4",
        "random_seed": 11,
        "run_id": run_id,
        "effective_ts": "2026-01-10T00:00:00Z",
        "action_ladder": [0, 5, 10, 15, 20],
        "max_discount_lead_time_hours": 168,
        "global_discount_limits": {"min_pct": 0, "max_pct": 20},
        "time_of_day_buckets": {"boundaries_hours": [0, 12, 17, 24]},
        "underbooking": {
            "pace_weight": 0.6,
            "fill_weight": 0.4,
            "underbooking_threshold": 0.35,
            "sparse_baseline_fill_rate": 0.6,
        },
        "scoring": {"training_min_rows": 2, "l2_c": 1.0},
        "optimizer": {
            "excluded_services": [],
            "price_floor_pct": 0.7,
            "healthy_zero_only": True,
            "severity_breakpoints": [0.2, 0.4, 0.7],
            "discount_steps": [5, 10, 15, 20],
            "exploration_share": 0.1,
        },
        "scenario": {
            "business_count": 1,
            "providers_per_business": 1,
            "services_per_business": 2,
            "locations_per_business": 1,
            "customers_per_business": 10,
            "days": 3,
            "slots_per_provider_per_day": 3,
            "cancellation_rate": 0.1,
            "removal_rate": 0.05,
        },
    }
    config_path = tmp_path / f"{run_id}.yaml"
    config_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    return config_path


def test_phase4_runner_persists_evaluation_and_metadata(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, run_id="phase4_full")
    results = run(str(config_path))

    assert results[-1].stage == "evaluation"

    with connect(tmp_path / "phase4.duckdb") as conn:
        eval_count = conn.execute(
            "SELECT COUNT(*) FROM evaluation_results WHERE run_id = 'phase4_full' AND scenario_id = 'phase4_scenario'"
        ).fetchone()[0]
        metadata = conn.execute(
            "SELECT random_seed, model_version, feature_snapshot_version FROM run_metadata WHERE run_id = 'phase4_full' AND scenario_id = 'phase4_scenario'"
        ).fetchone()

    assert eval_count > 0
    assert metadata is not None
    assert metadata[0] == 11


def test_phase4_runner_single_stage_mode(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, run_id="phase4_single")
    results = run(str(config_path), stages=["extract", "load"])

    assert [r.stage for r in results] == ["extract", "load"]

    with connect(tmp_path / "phase4.duckdb") as conn:
        feature_count = conn.execute(
            "SELECT COUNT(*) FROM feature_snapshots WHERE run_id = 'phase4_single' AND scenario_id = 'phase4_scenario'"
        ).fetchone()[0]

    assert feature_count == 0


def test_phase4_precomputed_tables_populated_for_ui_queries(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, run_id="phase4_ui_tables")
    run(str(config_path))

    with connect(tmp_path / "phase4.duckdb") as conn:
        feature_count = conn.execute(
            "SELECT COUNT(*) FROM feature_snapshots WHERE run_id = 'phase4_ui_tables' AND scenario_id = 'phase4_scenario'"
        ).fetchone()[0]
        underbooking_count = conn.execute(
            "SELECT COUNT(*) FROM underbooking_outputs WHERE run_id = 'phase4_ui_tables' AND scenario_id = 'phase4_scenario'"
        ).fetchone()[0]
        action_count = conn.execute(
            "SELECT COUNT(*) FROM pricing_actions WHERE run_id = 'phase4_ui_tables' AND scenario_id = 'phase4_scenario'"
        ).fetchone()[0]
        evaluation_count = conn.execute(
            "SELECT COUNT(*) FROM evaluation_results WHERE run_id = 'phase4_ui_tables' AND scenario_id = 'phase4_scenario'"
        ).fetchone()[0]

    assert feature_count > 0
    assert underbooking_count > 0
    assert action_count > 0
    assert evaluation_count > 0


def test_phase4_data_access_recommendations_and_evaluation_are_reasonable(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, run_id="phase4_ui_reasonable")
    run(str(config_path))

    da = AppDataAccess(tmp_path / "phase4.duckdb")
    recommendations = da.recommendations("phase4_ui_reasonable", "phase4_scenario")
    evaluation = da.evaluation("phase4_ui_reasonable", "phase4_scenario")

    assert not recommendations.empty
    assert not evaluation.empty

    assert set(
        [
            "slot_id",
            "business_id",
            "provider_id",
            "service_id",
            "effective_lead_time_band",
            "underbooked",
            "severity_score",
            "recommended_discount",
            "standard_price",
            "implied_price",
            "rationale_codes",
            "was_exploration",
        ]
    ).issubset(recommendations.columns)

    assert recommendations["recommended_discount"].between(0, 20).all()
    assert (recommendations["implied_price"] <= recommendations["standard_price"]).all()
    assert (recommendations["implied_price"] >= 0).all()

    metrics = set(evaluation["metric_name"].tolist())
    expected_metrics = {
        "core_slots_count",
        "feature_snapshot_count",
        "scoring_output_count",
        "pricing_action_count",
        "underbooked_rate",
        "discounted_action_rate",
        "healthy_zero_rate",
        "rationale_coverage",
    }
    assert expected_metrics.issubset(metrics)
    assert (evaluation["metric_value"] >= 0).all()
