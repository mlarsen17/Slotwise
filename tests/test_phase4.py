from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml
from pandas.testing import assert_frame_equal

from app.data_access import AppDataAccess
from app.recommendation_view import sort_recommendations
from pipeline.db import connect
from pipeline.run_pipeline import run


def _write_config(
    tmp_path: Path,
    *,
    run_id: str,
    scenario_overrides: dict | None = None,
) -> Path:
    scenario = {
        "business_count": 1,
        "providers_per_business": 1,
        "services_per_business": 2,
        "locations_per_business": 1,
        "customers_per_business": 10,
        "days": 3,
        "slots_per_provider_per_day": 3,
        "cancellation_rate": 0.1,
        "removal_rate": 0.05,
    }
    if scenario_overrides:
        scenario.update(scenario_overrides)

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
        "scenario": scenario,
    }
    config_path = tmp_path / f"{run_id}.yaml"
    config_path.write_text(yaml.safe_dump(payload), encoding="utf-8")
    return config_path


def _reference_recommendations(da: AppDataAccess, run_id: str, scenario_id: str) -> pd.DataFrame:
    with da._conn() as conn:  # noqa: SLF001 - direct reconciliation query in tests
        return conn.execute(
            """
            SELECT p.slot_id, s.business_id, s.provider_id, s.service_id,
                   f.effective_lead_time_band, u.underbooked, u.severity_score,
                   p.action_value AS recommended_discount,
                   s.standard_price,
                   ROUND(s.standard_price * (1 - p.action_value / 100.0), 2) AS implied_price,
                   p.rationale_codes, p.was_exploration
            FROM pricing_actions p
            JOIN slots s
              ON p.slot_id = s.slot_id AND p.run_id = s.run_id AND p.scenario_id = s.scenario_id
            LEFT JOIN underbooking_outputs u
              ON p.slot_id = u.slot_id AND p.run_id = u.run_id AND p.scenario_id = u.scenario_id
            LEFT JOIN feature_snapshots f
              ON p.slot_id = f.slot_id AND p.run_id = f.run_id AND p.scenario_id = f.scenario_id
             AND p.feature_snapshot_version = f.feature_snapshot_version
            WHERE p.run_id = ? AND p.scenario_id = ?
            ORDER BY u.severity_score DESC, p.action_value DESC, p.slot_id
            """,
            [run_id, scenario_id],
        ).fetchdf()


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
        "eligible_discount_compliance_rate",
        "discount_shortfall_correlation",
    }
    assert expected_metrics.issubset(metrics)
    non_negative_metrics = evaluation[evaluation["metric_name"] != "discount_shortfall_correlation"]
    assert (non_negative_metrics["metric_value"] >= 0).all()


def test_phase4_data_access_summary_and_distribution_queries(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, run_id="phase4_ui_summary")
    run(str(config_path))

    da = AppDataAccess(tmp_path / "phase4.duckdb")
    severity_distribution = da.severity_distribution("phase4_ui_summary", "phase4_scenario")
    summary = da.summary_counts("phase4_ui_summary", "phase4_scenario")

    assert not severity_distribution.empty
    assert set(["severity_band", "slot_count"]).issubset(severity_distribution.columns)
    assert set(summary.keys()) == {
        "by_action",
        "by_provider",
        "by_service",
        "by_lead_time_band",
    }
    for frame in summary.values():
        assert not frame.empty


def test_phase4_recommendation_explorer_reconciles_exactly_with_duckdb(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, run_id="phase4_ui_reconcile")
    run(str(config_path))

    da = AppDataAccess(tmp_path / "phase4.duckdb")
    actual = da.recommendations("phase4_ui_reconcile", "phase4_scenario")
    expected = _reference_recommendations(da, "phase4_ui_reconcile", "phase4_scenario")

    assert len(actual) == len(expected)
    assert_frame_equal(
        actual.reset_index(drop=True), expected.reset_index(drop=True), check_dtype=False
    )


def test_phase4_summary_views_reconcile_exactly_with_duckdb(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, run_id="phase4_summary_reconcile")
    run(str(config_path))

    da = AppDataAccess(tmp_path / "phase4.duckdb")
    summary = da.summary_counts("phase4_summary_reconcile", "phase4_scenario")

    with da._conn() as conn:  # noqa: SLF001
        expected_by_action = conn.execute(
            """
            SELECT action_value AS action_bucket, COUNT(*) AS recommendation_count
            FROM pricing_actions
            WHERE run_id = 'phase4_summary_reconcile' AND scenario_id = 'phase4_scenario'
            GROUP BY 1
            ORDER BY 1
            """
        ).fetchdf()
        expected_by_provider = conn.execute(
            """
            SELECT s.provider_id, COUNT(*) AS recommendation_count
            FROM pricing_actions p
            JOIN slots s ON s.slot_id = p.slot_id AND s.run_id = p.run_id AND s.scenario_id = p.scenario_id
            WHERE p.run_id = 'phase4_summary_reconcile' AND p.scenario_id = 'phase4_scenario'
            GROUP BY 1
            ORDER BY recommendation_count DESC
            """
        ).fetchdf()
        expected_by_service = conn.execute(
            """
            SELECT s.service_id, COUNT(*) AS recommendation_count
            FROM pricing_actions p
            JOIN slots s ON s.slot_id = p.slot_id AND s.run_id = p.run_id AND s.scenario_id = p.scenario_id
            WHERE p.run_id = 'phase4_summary_reconcile' AND p.scenario_id = 'phase4_scenario'
            GROUP BY 1
            ORDER BY recommendation_count DESC
            """
        ).fetchdf()
        expected_by_lead = conn.execute(
            """
            SELECT f.effective_lead_time_band, COUNT(*) AS recommendation_count
            FROM pricing_actions p
            JOIN feature_snapshots f
              ON f.slot_id = p.slot_id
             AND f.run_id = p.run_id
             AND f.scenario_id = p.scenario_id
             AND f.feature_snapshot_version = p.feature_snapshot_version
            WHERE p.run_id = 'phase4_summary_reconcile' AND p.scenario_id = 'phase4_scenario'
            GROUP BY 1
            ORDER BY 1
            """
        ).fetchdf()

    assert_frame_equal(summary["by_action"], expected_by_action, check_dtype=False)
    assert_frame_equal(summary["by_provider"], expected_by_provider, check_dtype=False)
    assert_frame_equal(summary["by_service"], expected_by_service, check_dtype=False)
    assert_frame_equal(summary["by_lead_time_band"], expected_by_lead, check_dtype=False)


def test_phase4_filter_aware_recommendation_queries(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        run_id="phase4_filtered",
        scenario_overrides={
            "business_count": 2,
            "providers_per_business": 2,
            "services_per_business": 2,
        },
    )
    run(str(config_path))

    da = AppDataAccess(tmp_path / "phase4.duckdb")
    all_rows = da.recommendations("phase4_filtered", "phase4_scenario")
    assert len(all_rows) > 0

    sample = all_rows.iloc[0]
    business_rows = da.recommendations(
        "phase4_filtered",
        "phase4_scenario",
        business_id=str(sample["business_id"]),
    )
    assert not business_rows.empty
    assert (business_rows["business_id"] == sample["business_id"]).all()
    assert len(business_rows) < len(all_rows)

    provider_rows = da.recommendations(
        "phase4_filtered",
        "phase4_scenario",
        provider_id=str(sample["provider_id"]),
    )
    assert not provider_rows.empty
    assert (provider_rows["provider_id"] == sample["provider_id"]).all()
    assert len(provider_rows) < len(all_rows)

    service_rows = da.recommendations(
        "phase4_filtered",
        "phase4_scenario",
        service_id=str(sample["service_id"]),
    )
    assert not service_rows.empty
    assert (service_rows["service_id"] == sample["service_id"]).all()
    assert len(service_rows) < len(all_rows)

    band_rows = da.recommendations(
        "phase4_filtered",
        "phase4_scenario",
        lead_time_band=str(sample["effective_lead_time_band"]),
    )
    assert not band_rows.empty
    assert (band_rows["effective_lead_time_band"] == sample["effective_lead_time_band"]).all()
    assert len(band_rows) <= len(all_rows)

    discounted_rows = da.recommendations(
        "phase4_filtered",
        "phase4_scenario",
        discounted_only=True,
    )
    assert (discounted_rows["recommended_discount"] > 0).all()
    assert len(discounted_rows) < len(all_rows)

    exploration_rows = da.recommendations(
        "phase4_filtered",
        "phase4_scenario",
        exploration_only=True,
    )
    if not exploration_rows.empty:
        assert exploration_rows["was_exploration"].all()
        assert len(exploration_rows) < len(all_rows)


def test_phase4_filter_aware_summary_counts(tmp_path: Path) -> None:
    config_path = _write_config(
        tmp_path,
        run_id="phase4_summary_filtered",
        scenario_overrides={
            "business_count": 2,
            "providers_per_business": 2,
            "services_per_business": 2,
        },
    )
    run(str(config_path))

    da = AppDataAccess(tmp_path / "phase4.duckdb")
    all_summary = da.summary_counts("phase4_summary_filtered", "phase4_scenario")
    provider = da.recommendations("phase4_summary_filtered", "phase4_scenario").iloc[0][
        "provider_id"
    ]
    provider_summary = da.summary_counts(
        "phase4_summary_filtered",
        "phase4_scenario",
        provider_id=str(provider),
    )

    assert provider_summary["by_provider"]["provider_id"].nunique() == 1
    assert provider_summary["by_provider"]["provider_id"].iloc[0] == provider
    assert (
        provider_summary["by_action"]["recommendation_count"].sum()
        < all_summary["by_action"]["recommendation_count"].sum()
    )


def test_phase4_recommendation_sorting_helper() -> None:
    frame = pd.DataFrame(
        [
            {"slot_id": "s1", "severity_score": 0.6, "recommended_discount": 10},
            {"slot_id": "s2", "severity_score": 0.8, "recommended_discount": 5},
            {"slot_id": "s3", "severity_score": 0.8, "recommended_discount": 20},
        ]
    )

    by_severity_desc = sort_recommendations(frame, sort_field="severity_score", sort_desc=True)
    assert by_severity_desc["slot_id"].tolist() == ["s3", "s2", "s1"]

    by_discount_asc = sort_recommendations(
        frame, sort_field="recommended_discount", sort_desc=False
    )
    assert by_discount_asc["slot_id"].tolist() == ["s2", "s1", "s3"]
