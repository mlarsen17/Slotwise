from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from pipeline.db import bootstrap_db, connect
from pipeline.stages.baseline_stage import compute_cohort_baselines
from pipeline.stages.feature_stage import materialize_feature_snapshot
from pipeline.stages.optimization_stage import recommend_pricing_actions
from pipeline.stages.scoring_stage import score_slots
from pipeline.stages.underbooking_stage import detect_underbooking


def _seed_minimal_dataset(conn) -> None:
    conn.execute(
        """
        INSERT INTO businesses VALUES ('b1','sb1','Business 1','phase3_scenario');
        INSERT INTO providers VALUES ('p1','b1','l1','sp1','sl1','sb1','Provider 1','phase3_scenario');
        INSERT INTO services VALUES ('svc1','b1','ss1','sb1','Service 1',30,'phase3_scenario');
        INSERT INTO locations VALUES ('l1','b1','sl1','sb1','Location 1','phase3_scenario');
        INSERT INTO customers VALUES ('c1','b1','sc1','sb1','A','B','phase3_scenario');
        """
    )


def _insert_slot(
    conn, slot_id: str, start_at: str, visible_at: str, run_id: str = "run_phase3"
) -> None:
    conn.execute(
        """
        INSERT INTO slots (
          slot_id, provider_id, business_id, service_id, location_id, standard_price,
          slot_duration_minutes, integration_id, external_slot_id, source_slot_id,
          source_provider_id, source_business_id, source_service_id, source_location_id,
          slot_start_at, slot_end_at, visible_at, unavailable_at, current_status, created_at,
          source_system, source_run_id, scenario_id, run_id, effective_ts, config_hash
        )
        VALUES (?, 'p1', 'b1', 'svc1', 'l1', 100.0, 30, 'medscheduler', ?, ?,
                'sp1', 'sb1', 'ss1', 'sl1', ?, ?, ?, NULL, 'open', ?,
                'medscheduler', 'source_3', 'phase3_scenario', ?, TIMESTAMP '2026-01-10 00:00:00', 'cfg')
        """,
        [slot_id, slot_id, slot_id, start_at, start_at, visible_at, visible_at, run_id],
    )


def _insert_event(
    conn, event_id: str, slot_id: str, event_type: str, event_at: str, run_id: str = "run_phase3"
) -> None:
    conn.execute(
        """
        INSERT INTO booking_events (
          event_id, slot_id, customer_id, business_id, provider_id, service_type,
          source_event_id, source_slot_id, source_customer_id, event_type, event_at,
          source_system, source_run_id, scenario_id, run_id, effective_ts
        )
        VALUES (?, ?, 'c1', 'b1', 'p1', 'svc1', ?, ?, 'sc1', ?, ?,
                'medscheduler', 'source_3', 'phase3_scenario', ?, TIMESTAMP '2026-01-10 00:00:00')
        """,
        [event_id, slot_id, event_id, slot_id, event_type, event_at, run_id],
    )


def _prepare_phase3_base(conn) -> None:
    _seed_minimal_dataset(conn)
    _insert_slot(conn, "slot_future_a", "2026-01-12T12:00:00Z", "2026-01-01T00:00:00Z")
    _insert_slot(conn, "slot_future_b", "2026-01-11T09:00:00Z", "2026-01-01T00:00:00Z")
    _insert_slot(conn, "slot_past", "2026-01-09T09:00:00Z", "2026-01-01T00:00:00Z")
    _insert_event(conn, "e1", "slot_past", "booked", "2026-01-08T08:00:00Z")
    _insert_event(conn, "e2", "slot_past", "completed", "2026-01-09T10:00:00Z")

    compute_cohort_baselines(
        conn,
        scenario_id="phase3_scenario",
        run_id="run_phase3",
        feature_snapshot_version="fsv_test",
        effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
        bucket_boundaries=[0, 12, 17, 24],
    )
    materialize_feature_snapshot(
        conn,
        scenario_id="phase3_scenario",
        run_id="run_phase3",
        feature_snapshot_version="fsv_test",
        effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
        bucket_boundaries=[0, 12, 17, 24],
    )
    detect_underbooking(
        conn,
        scenario_id="phase3_scenario",
        run_id="run_phase3",
        feature_snapshot_version="fsv_test",
        pace_weight=0.6,
        fill_weight=0.4,
        underbooking_threshold=0.2,
        sparse_baseline_fill_rate=0.7,
    )


def test_phase3_scoring_contract_and_calibration(tmp_path: Path) -> None:
    with connect(tmp_path / "phase3_scoring.duckdb") as conn:
        bootstrap_db(conn)
        _prepare_phase3_base(conn)

        output = score_slots(
            conn,
            scenario_id="phase3_scenario",
            run_id="run_phase3",
            feature_snapshot_version="fsv_test",
            model_version="model_v1",
            l2_c=1.0,
            effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
            training_min_rows=2,
        )
        assert not output.empty
        expected_cols = {
            "booking_probability",
            "predicted_fill_by_start",
            "shortfall_score",
            "confidence_score",
            "model_version",
            "feature_snapshot_version",
        }
        assert expected_cols.issubset(set(output.columns))
        rows = conn.execute(
            "SELECT COUNT(*) FROM business_calibrations WHERE run_id='run_phase3'"
        ).fetchone()[0]
        assert rows >= 1


def test_phase3_optimizer_rules_and_exploration_determinism(tmp_path: Path) -> None:
    with connect(tmp_path / "phase3_optimizer.duckdb") as conn:
        bootstrap_db(conn)
        _prepare_phase3_base(conn)
        score_slots(
            conn,
            scenario_id="phase3_scenario",
            run_id="run_phase3",
            feature_snapshot_version="fsv_test",
            model_version="model_v1",
            l2_c=1.0,
            effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
            training_min_rows=2,
        )

        first = recommend_pricing_actions(
            conn,
            scenario_id="phase3_scenario",
            run_id="run_phase3",
            feature_snapshot_version="fsv_test",
            effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
            random_seed=123,
            action_ladder=[0, 5, 10, 15, 20],
            max_discount_lead_time_hours=168,
            max_discount_pct=20,
            excluded_services=[],
            price_floor_pct=0.8,
            healthy_zero_only=True,
            severity_breakpoints=[0.2, 0.4, 0.7],
            discount_steps=[5, 10, 15, 20],
            exploration_share=0.6,
        )
        second = recommend_pricing_actions(
            conn,
            scenario_id="phase3_scenario",
            run_id="run_phase3",
            feature_snapshot_version="fsv_test",
            effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
            random_seed=123,
            action_ladder=[0, 5, 10, 15, 20],
            max_discount_lead_time_hours=168,
            max_discount_pct=20,
            excluded_services=[],
            price_floor_pct=0.8,
            healthy_zero_only=True,
            severity_breakpoints=[0.2, 0.4, 0.7],
            discount_steps=[5, 10, 15, 20],
            exploration_share=0.6,
        )

        assert first.equals(second)
        assert (first["confidence_score"].notna()).all()
        assert (first["rationale_codes"].str.len() > 0).all()
        dupes = conn.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT slot_id, COUNT(*) c
                FROM pricing_actions
                WHERE run_id='run_phase3'
                GROUP BY 1
                HAVING COUNT(*) > 1
            )
            """
        ).fetchone()[0]
        assert dupes == 0
