from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from pipeline.config import AppConfig
from pipeline.db import bootstrap_db, connect
from pipeline.stages.baseline_stage import compute_cohort_baselines
from pipeline.stages.feature_stage import materialize_feature_snapshot
from pipeline.stages.phase2_utils import assign_time_of_day_bucket
from pipeline.stages.underbooking_stage import detect_underbooking


def _seed_minimal_dataset(conn) -> None:
    conn.execute(
        """
        INSERT INTO businesses VALUES ('b1','sb1','Business 1','phase2_scenario');
        INSERT INTO providers VALUES ('p1','b1','l1','sp1','sl1','sb1','Provider 1','phase2_scenario');
        INSERT INTO services VALUES ('svc1','b1','ss1','sb1','Service 1',30,'phase2_scenario');
        INSERT INTO locations VALUES ('l1','b1','sl1','sb1','Location 1','phase2_scenario');
        INSERT INTO customers VALUES ('c1','b1','sc1','sb1','A','B','phase2_scenario');
        """
    )


def _insert_slot(
    conn, slot_id: str, start_at: str, visible_at: str, run_id: str = "run_phase2"
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
                'medscheduler', 'source_2', 'phase2_scenario', ?, TIMESTAMP '2026-01-10 00:00:00', 'cfg')
        """,
        [slot_id, slot_id, slot_id, start_at, start_at, visible_at, visible_at, run_id],
    )


def _insert_event(
    conn, event_id: str, slot_id: str, event_type: str, event_at: str, run_id: str = "run_phase2"
) -> None:
    conn.execute(
        """
        INSERT INTO booking_events (
          event_id, slot_id, customer_id, business_id, provider_id, service_type,
          source_event_id, source_slot_id, source_customer_id, event_type, event_at,
          source_system, source_run_id, scenario_id, run_id, effective_ts
        )
        VALUES (?, ?, 'c1', 'b1', 'p1', 'svc1', ?, ?, 'sc1', ?, ?,
                'medscheduler', 'source_2', 'phase2_scenario', ?, TIMESTAMP '2026-01-10 00:00:00')
        """,
        [event_id, slot_id, event_id, slot_id, event_type, event_at, run_id],
    )


def test_time_of_day_bucket_edges() -> None:
    assert assign_time_of_day_bucket(datetime(2026, 1, 1, 11, 59), [0, 12, 17, 24]) == "00-12"
    assert assign_time_of_day_bucket(datetime(2026, 1, 1, 12, 0), [0, 12, 17, 24]) == "12-17"
    assert assign_time_of_day_bucket(datetime(2026, 1, 1, 17, 0), [0, 12, 17, 24]) == "17-24"


def test_no_future_event_leakage(tmp_path: Path) -> None:
    with connect(tmp_path / "phase2_leakage.duckdb") as conn:
        bootstrap_db(conn)
        _seed_minimal_dataset(conn)
        _insert_slot(conn, "slot_past", "2026-01-09T10:00:00Z", "2026-01-05T00:00:00Z")
        _insert_event(conn, "e1", "slot_past", "booked", "2026-01-09T09:00:00Z")
        _insert_event(conn, "e2", "slot_past", "completed", "2026-01-10T01:00:00Z")

        compute_cohort_baselines(
            conn,
            scenario_id="phase2_scenario",
            run_id="run_phase2",
            feature_snapshot_version="fsv_test",
            effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
            bucket_boundaries=[0, 12, 17, 24],
        )
        completion_rate = conn.execute(
            "SELECT MAX(completion_rate) FROM cohort_baselines"
        ).fetchone()[0]
    assert completion_rate == 0.0


def test_true_rolling_window_utilization_and_volume(tmp_path: Path) -> None:
    with connect(tmp_path / "phase2_windows.duckdb") as conn:
        bootstrap_db(conn)
        _seed_minimal_dataset(conn)
        _insert_slot(conn, "slot_1d", "2026-01-09T10:00:00Z", "2026-01-01T00:00:00Z")
        _insert_slot(conn, "slot_10d", "2025-12-31T10:00:00Z", "2025-12-20T00:00:00Z")
        _insert_slot(conn, "slot_20d", "2025-12-21T10:00:00Z", "2025-12-10T00:00:00Z")
        _insert_event(conn, "e1", "slot_1d", "booked", "2026-01-09T09:00:00Z")
        _insert_event(conn, "e2", "slot_10d", "booked", "2025-12-30T09:00:00Z")
        _insert_event(conn, "e3", "slot_20d", "booked", "2025-12-20T09:00:00Z")

        compute_cohort_baselines(
            conn,
            scenario_id="phase2_scenario",
            run_id="run_phase2",
            feature_snapshot_version="fsv_test",
            effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
            bucket_boundaries=[0, 12, 17, 24],
        )
        materialize_feature_snapshot(
            conn,
            scenario_id="phase2_scenario",
            run_id="run_phase2",
            feature_snapshot_version="fsv_test",
            effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
            bucket_boundaries=[0, 12, 17, 24],
        )
        util7, util14, util28, vol7, vol14, vol28 = conn.execute(
            """
            SELECT provider_utilization_7d, provider_utilization_14d, provider_utilization_28d,
                   booking_volume_7d, booking_volume_14d, booking_volume_28d
            FROM feature_snapshots
            WHERE slot_id='slot_1d'
            """
        ).fetchone()
    assert util7 <= util14 <= util28
    assert vol7 <= vol14 <= vol28


def test_remaining_provider_slots_same_day_counts_only_future(tmp_path: Path) -> None:
    with connect(tmp_path / "phase2_remaining.duckdb") as conn:
        bootstrap_db(conn)
        _seed_minimal_dataset(conn)
        _insert_slot(conn, "slot_past_day", "2026-01-10T08:00:00Z", "2026-01-01T00:00:00Z")
        _insert_slot(conn, "slot_future_day", "2026-01-10T12:00:00Z", "2026-01-01T00:00:00Z")

        compute_cohort_baselines(
            conn,
            scenario_id="phase2_scenario",
            run_id="run_phase2",
            feature_snapshot_version="fsv_test",
            effective_ts=datetime(2026, 1, 10, 10, 0, tzinfo=timezone.utc),
            bucket_boundaries=[0, 12, 17, 24],
        )
        materialize_feature_snapshot(
            conn,
            scenario_id="phase2_scenario",
            run_id="run_phase2",
            feature_snapshot_version="fsv_test",
            effective_ts=datetime(2026, 1, 10, 10, 0, tzinfo=timezone.utc),
            bucket_boundaries=[0, 12, 17, 24],
        )
        remaining_past = conn.execute(
            "SELECT remaining_provider_slots_same_day FROM feature_snapshots WHERE slot_id='slot_past_day'"
        ).fetchone()[0]
        remaining_future = conn.execute(
            "SELECT remaining_provider_slots_same_day FROM feature_snapshots WHERE slot_id='slot_future_day'"
        ).fetchone()[0]
    assert remaining_past == 0
    assert remaining_future == 1


def test_sparse_cohort_fallback_and_determinism(tmp_path: Path) -> None:
    with connect(tmp_path / "phase2_sparse.duckdb") as conn:
        bootstrap_db(conn)
        _seed_minimal_dataset(conn)
        _insert_slot(conn, "slot_sparse", "2026-01-10T12:00:00Z", "2026-01-09T00:00:00Z")

        compute_cohort_baselines(
            conn,
            scenario_id="phase2_scenario",
            run_id="run_phase2",
            feature_snapshot_version="fsv_test",
            effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
            bucket_boundaries=[0, 12, 17, 24],
        )
        materialize_feature_snapshot(
            conn,
            scenario_id="phase2_scenario",
            run_id="run_phase2",
            feature_snapshot_version="fsv_test",
            effective_ts=datetime(2026, 1, 10, tzinfo=timezone.utc),
            bucket_boundaries=[0, 12, 17, 24],
        )
        first = detect_underbooking(
            conn,
            scenario_id="phase2_scenario",
            run_id="run_phase2",
            feature_snapshot_version="fsv_test",
            pace_weight=0.6,
            fill_weight=0.4,
            underbooking_threshold=0.2,
            sparse_baseline_fill_rate=0.8,
        )
        second = detect_underbooking(
            conn,
            scenario_id="phase2_scenario",
            run_id="run_phase2",
            feature_snapshot_version="fsv_test",
            pace_weight=0.6,
            fill_weight=0.4,
            underbooking_threshold=0.2,
            sparse_baseline_fill_rate=0.8,
        )
    assert first.equals(second)
    assert (first["detection_reason"] == "sparse_cohort_fallback").all()


def test_config_hash_changes_with_phase2_settings() -> None:
    base = {
        "duckdb_path": "data/mvp.duckdb",
        "scenario_id": "s",
        "source_run_id": "src",
        "random_seed": 1,
        "effective_ts": "2026-01-01T00:00:00Z",
        "action_ladder": [0, 5, 10],
        "lead_time_windows_hours": [24, 72],
        "global_discount_limits": {"min_pct": 0, "max_pct": 20},
        "scenario": {
            "business_count": 1,
            "providers_per_business": 1,
            "services_per_business": 1,
            "locations_per_business": 1,
            "customers_per_business": 1,
            "days": 1,
            "slots_per_provider_per_day": 1,
            "cancellation_rate": 0.1,
            "removal_rate": 0.1,
        },
        "time_of_day_buckets": {"boundaries_hours": [0, 12, 17, 24]},
        "underbooking": {
            "pace_weight": 0.6,
            "fill_weight": 0.4,
            "underbooking_threshold": 0.35,
            "sparse_baseline_fill_rate": 0.6,
        },
    }
    cfg1 = AppConfig(**base)
    changed = dict(base)
    changed["underbooking"] = {
        "pace_weight": 0.6,
        "fill_weight": 0.4,
        "underbooking_threshold": 0.35,
        "sparse_baseline_fill_rate": 0.75,
    }
    cfg2 = AppConfig(**changed)
    assert cfg1.config_hash() != cfg2.config_hash()
