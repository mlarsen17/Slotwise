from __future__ import annotations

from pathlib import Path

import yaml

from pipeline.db import connect
from pipeline.run_pipeline import run


def _write_config(tmp_path: Path, *, run_id: str) -> Path:
    payload = {
        "duckdb_path": str(tmp_path / "runner.duckdb"),
        "scenario_id": "phase3_runner_scenario",
        "source_run_id": "synthetic_run_runner",
        "random_seed": 7,
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


def test_runner_smoke_writes_phase3_outputs(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, run_id="run_smoke")
    run(str(config_path))

    with connect(tmp_path / "runner.duckdb") as conn:
        status = conn.execute(
            "SELECT status FROM pipeline_runs WHERE run_id = 'run_smoke' AND scenario_id = 'phase3_runner_scenario'"
        ).fetchone()[0]
        assert status == "success"
        action_count = conn.execute(
            "SELECT COUNT(*) FROM pricing_actions WHERE run_id = 'run_smoke' AND scenario_id = 'phase3_runner_scenario'"
        ).fetchone()[0]
        scoring_count = conn.execute(
            "SELECT COUNT(*) FROM scoring_outputs WHERE run_id = 'run_smoke' AND scenario_id = 'phase3_runner_scenario'"
        ).fetchone()[0]
        assert action_count > 0
        assert scoring_count > 0


def test_runner_failure_persists_failed_status(tmp_path: Path, monkeypatch) -> None:
    from pipeline import run_pipeline

    config_path = _write_config(tmp_path, run_id="run_fail")

    def _boom(*args, **kwargs):
        raise RuntimeError("forced scoring failure")

    monkeypatch.setattr(run_pipeline, "score_slots", _boom)

    try:
        run(str(config_path))
    except RuntimeError:
        pass
    else:
        raise AssertionError("Expected forced scoring failure")

    with connect(tmp_path / "runner.duckdb") as conn:
        status = conn.execute(
            "SELECT status FROM pipeline_runs WHERE run_id = 'run_fail' AND scenario_id = 'phase3_runner_scenario'"
        ).fetchone()[0]
        assert status == "failed"


def test_runner_failure_does_not_persist_partial_pricing_actions(
    tmp_path: Path, monkeypatch
) -> None:
    from pipeline import run_pipeline

    config_path = _write_config(tmp_path, run_id="run_fail_partial")

    def _boom(*args, **kwargs):
        raise RuntimeError("forced optimization failure")

    monkeypatch.setattr(run_pipeline, "recommend_pricing_actions", _boom)

    try:
        run(str(config_path))
    except RuntimeError:
        pass
    else:
        raise AssertionError("Expected forced optimization failure")

    with connect(tmp_path / "runner.duckdb") as conn:
        action_count = conn.execute(
            """
            SELECT COUNT(*)
            FROM pricing_actions
            WHERE run_id = 'run_fail_partial' AND scenario_id = 'phase3_runner_scenario'
            """
        ).fetchone()[0]
        assert action_count == 0


def test_runner_idempotent_rerun_keeps_stable_pricing_actions(tmp_path: Path) -> None:
    config_path = _write_config(tmp_path, run_id="run_idempotent")
    run(str(config_path))
    run(str(config_path))

    with connect(tmp_path / "runner.duckdb") as conn:
        duplicate_slots = conn.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT slot_id
                FROM pricing_actions
                WHERE run_id = 'run_idempotent' AND scenario_id = 'phase3_runner_scenario'
                GROUP BY slot_id
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
        assert duplicate_slots == 0

        action_rows = conn.execute(
            """
            SELECT slot_id, action_value, was_exploration
            FROM pricing_actions
            WHERE run_id = 'run_idempotent' AND scenario_id = 'phase3_runner_scenario'
            ORDER BY slot_id
            """
        ).fetchall()
        assert len(action_rows) > 0
