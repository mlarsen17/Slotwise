from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import yaml

from pipeline.db import bootstrap_db, connect
from pipeline.run_pipeline import run
from pipeline.stages.baseline_stage import compute_cohort_baselines
from pipeline.stages.feature_stage import materialize_feature_snapshot
from pipeline.stages.optimization_stage import recommend_pricing_actions
from pipeline.stages.scoring_stage import score_slots
from pipeline.stages.underbooking_stage import detect_underbooking


def _seed(conn) -> None:
    conn.execute(
        """
        INSERT INTO businesses VALUES ('b1','sb1','Business 1','s');
        INSERT INTO providers VALUES ('p1','b1','l1','sp1','sl1','sb1','Provider 1','s');
        INSERT INTO services VALUES ('svc1','b1','ss1','sb1','Service 1',30,'s');
        INSERT INTO locations VALUES ('l1','b1','sl1','sb1','Location 1','s');
        INSERT INTO customers VALUES ('c1','b1','sc1','sb1','A','B','s');
        """
    )


def _slot(conn, slot_id: str, start: str) -> None:
    conn.execute(
        """
        INSERT INTO slots (
          slot_id, provider_id, business_id, service_id, location_id, standard_price,
          slot_duration_minutes, integration_id, external_slot_id, source_slot_id,
          source_provider_id, source_business_id, source_service_id, source_location_id,
          slot_start_at, slot_end_at, visible_at, unavailable_at, current_status, created_at,
          source_system, source_run_id, scenario_id, run_id, effective_ts, config_hash
        ) VALUES (?, 'p1', 'b1', 'svc1', 'l1', 100, 30, 'm', ?, ?, 'sp1', 'sb1', 'ss1', 'sl1',
        ?, ?, TIMESTAMP '2026-01-01T00:00:00Z', NULL, 'open', TIMESTAMP '2026-01-01T00:00:00Z',
        'm','src','s','r', TIMESTAMP '2026-01-10T00:00:00Z', 'h')
        """,
        [slot_id, slot_id, slot_id, start, start],
    )


def _event(conn, event_id: str, slot_id: str, event_type: str, event_at: str) -> None:
    conn.execute(
        """
        INSERT INTO booking_events (
          event_id, slot_id, customer_id, business_id, provider_id, service_type,
          source_event_id, source_slot_id, source_customer_id, event_type, event_at,
          source_system, source_run_id, scenario_id, run_id, effective_ts
        ) VALUES (?, ?, 'c1', 'b1', 'p1', 'svc1', ?, ?, 'sc1', ?, ?, 'm','src','s','r', TIMESTAMP '2026-01-10T00:00:00Z')
        """,
        [event_id, slot_id, event_id, slot_id, event_type, event_at],
    )


def _prep(conn) -> None:
    _seed(conn)
    _slot(conn, "past_slot", "2026-01-08T10:00:00Z")
    _slot(conn, "future_slot", "2026-01-11T10:00:00Z")
    _event(conn, "e1", "past_slot", "booked", "2026-01-07T10:00:00Z")
    _event(conn, "e2", "future_slot", "booked", "2026-01-10T12:00:00Z")
    compute_cohort_baselines(
        conn,
        scenario_id="s",
        run_id="r",
        feature_snapshot_version="f",
        effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
        bucket_boundaries=[0, 12, 17, 24],
    )
    materialize_feature_snapshot(
        conn,
        scenario_id="s",
        run_id="r",
        feature_snapshot_version="f",
        effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
        bucket_boundaries=[0, 12, 17, 24],
    )
    detect_underbooking(
        conn,
        scenario_id="s",
        run_id="r",
        feature_snapshot_version="f",
        pace_weight=0.6,
        fill_weight=0.4,
        underbooking_threshold=0.2,
        sparse_baseline_fill_rate=0.7,
    )


def test_scoring_excludes_unresolved_future_slots_from_training(tmp_path: Path) -> None:
    with connect(tmp_path / "p32.duckdb") as conn:
        bootstrap_db(conn)
        _prep(conn)
        out = score_slots(
            conn,
            scenario_id="s",
            run_id="r",
            feature_snapshot_version="f",
            model_version="m",
            l2_c=1.0,
            effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
            training_min_rows=5,
        )
        assert not out.empty
        assert int(out["training_row_count"].iloc[0]) == 1
        assert bool(out["used_fallback"].iloc[0]) is True


def test_optimizer_lead_time_threshold_blocks_far_future_discounts(tmp_path: Path) -> None:
    with connect(tmp_path / "p32_opt.duckdb") as conn:
        bootstrap_db(conn)
        _prep(conn)
        score_slots(
            conn,
            scenario_id="s",
            run_id="r",
            feature_snapshot_version="f",
            model_version="m",
            l2_c=1.0,
            effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
            training_min_rows=1,
        )
        out = recommend_pricing_actions(
            conn,
            scenario_id="s",
            run_id="r",
            feature_snapshot_version="f",
            effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
            random_seed=3,
            action_ladder=[0, 5, 10],
            max_discount_lead_time_hours=12,
            max_discount_pct=10,
            excluded_services=[],
            price_floor_pct=0.7,
            healthy_zero_only=False,
            severity_breakpoints=[0.2, 0.4],
            discount_steps=[5, 10, 10],
            exploration_share=0.0,
        )
        far = out[out["slot_id"] == "future_slot"].iloc[0]
        assert far["action_value"] == 0.0


def test_pipeline_run_audit_fields_on_success_and_failure(tmp_path: Path, monkeypatch) -> None:
    payload = {
        "duckdb_path": str(tmp_path / "runner.duckdb"),
        "scenario_id": "phase3_runner_scenario",
        "source_run_id": "synthetic_run_runner",
        "random_seed": 7,
        "run_id": "audit_ok",
        "effective_ts": "2026-01-10T00:00:00Z",
        "action_ladder": [0, 5, 10, 15, 20],
        "max_discount_lead_time_hours": 168,
        "global_discount_limits": {"min_pct": 0, "max_pct": 20},
        "scenario": {
            "business_count": 1,
            "providers_per_business": 1,
            "services_per_business": 1,
            "locations_per_business": 1,
            "customers_per_business": 4,
            "days": 2,
            "slots_per_provider_per_day": 2,
            "cancellation_rate": 0.1,
            "removal_rate": 0.05,
        },
    }
    cfg = tmp_path / "cfg.yaml"
    cfg.write_text(yaml.safe_dump(payload), encoding="utf-8")
    run(str(cfg))
    with connect(tmp_path / "runner.duckdb") as conn:
        started_at, ended_at, status = conn.execute(
            "SELECT started_at, ended_at, status FROM pipeline_runs WHERE run_id='audit_ok'"
        ).fetchone()
        assert status == "success"
        assert started_at is not None and ended_at is not None and ended_at >= started_at

    from pipeline import run_pipeline

    def _boom(*args, **kwargs):
        raise RuntimeError("boom stage")

    monkeypatch.setattr(run_pipeline, "score_slots", _boom)
    payload["run_id"] = "audit_fail"
    cfg.write_text(yaml.safe_dump(payload), encoding="utf-8")
    try:
        run(str(cfg))
    except RuntimeError:
        pass

    with connect(tmp_path / "runner.duckdb") as conn:
        status, msg, ended_at = conn.execute(
            "SELECT status, failure_message, ended_at FROM pipeline_runs WHERE run_id='audit_fail'"
        ).fetchone()
        assert status == "failed"
        assert "RuntimeError" in msg
        assert ended_at is not None
