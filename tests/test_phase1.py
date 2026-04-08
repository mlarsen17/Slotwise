from __future__ import annotations

from pathlib import Path

from medscheduler_wrapper.extract import extract_synthetic_data
from medscheduler_wrapper.normalize import normalize_records
from medscheduler_wrapper.scenario_config import ScenarioConfig
from pipeline.config import load_config
from pipeline.db import bootstrap_db, connect
from pipeline.run_pipeline import run
from pipeline.stages.availability_stage import apply_availability
from pipeline.stages.load_stage import load_core_tables


def make_scenario() -> ScenarioConfig:
    return ScenarioConfig(
        scenario_id="test_scenario",
        source_run_id="source_1",
        random_seed=123,
        effective_ts="2026-01-01T00:00:00Z",
        business_count=1,
        providers_per_business=1,
        services_per_business=2,
        locations_per_business=1,
        customers_per_business=5,
        days=3,
        slots_per_provider_per_day=2,
        cancellation_rate=0.2,
        removal_rate=0.1,
    )


def test_db_bootstrap(tmp_path: Path) -> None:
    db = tmp_path / "test.duckdb"
    with connect(db) as conn:
        bootstrap_db(conn)
        tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    assert {
        "slots",
        "booking_events",
        "pricing_actions",
        "businesses",
        "providers",
        "services",
        "locations",
        "customers",
    }.issubset(tables)


def test_normalization_determinism_and_stable_ids() -> None:
    raw = extract_synthetic_data(make_scenario())
    one = normalize_records(raw, "test_scenario", "source_1")
    two = normalize_records(raw, "test_scenario", "source_1")

    assert one["slots"]["slot_id"].tolist() == two["slots"]["slot_id"].tolist()
    assert one["booking_events"]["event_id"].tolist() == two["booking_events"]["event_id"].tolist()


def test_idempotent_loading_and_availability(tmp_path: Path) -> None:
    db = tmp_path / "load.duckdb"
    raw = extract_synthetic_data(make_scenario())
    norm = normalize_records(raw, "test_scenario", "source_1")

    with connect(db) as conn:
        bootstrap_db(conn)
        load_core_tables(conn, norm, "test_scenario")
        apply_availability(conn, "test_scenario")
        first_slots = conn.execute(
            "SELECT COUNT(*) FROM slots WHERE scenario_id='test_scenario'"
        ).fetchone()[0]
        first_events = conn.execute(
            "SELECT COUNT(*) FROM booking_events WHERE scenario_id='test_scenario'"
        ).fetchone()[0]

        load_core_tables(conn, norm, "test_scenario")
        apply_availability(conn, "test_scenario")
        second_slots = conn.execute(
            "SELECT COUNT(*) FROM slots WHERE scenario_id='test_scenario'"
        ).fetchone()[0]
        second_events = conn.execute(
            "SELECT COUNT(*) FROM booking_events WHERE scenario_id='test_scenario'"
        ).fetchone()[0]

        malformed = conn.execute(
            "SELECT COUNT(*) FROM slots WHERE scenario_id='test_scenario' AND visible_at > unavailable_at"
        ).fetchone()[0]

    assert first_slots == second_slots
    assert first_events == second_events
    assert malformed == 0


def test_sample_end_to_end_run(tmp_path: Path) -> None:
    cfg = tmp_path / "cfg.yaml"
    db = tmp_path / "mvp.duckdb"
    cfg.write_text(
        "\n".join(
            [
                f"duckdb_path: {db}",
                "scenario_id: e2e_phase1",
                "source_run_id: synthetic_run_001",
                "random_seed: 1",
                "effective_ts: 2026-01-01T00:00:00Z",
                "action_ladder: [0, 5, 10, 15, 20]",
                "lead_time_windows_hours: [24, 72, 168]",
                "global_discount_limits:",
                "  min_pct: 0",
                "  max_pct: 20",
                "scenario:",
                "  business_count: 1",
                "  providers_per_business: 1",
                "  services_per_business: 2",
                "  locations_per_business: 1",
                "  customers_per_business: 5",
                "  days: 2",
                "  slots_per_provider_per_day: 2",
                "  cancellation_rate: 0.1",
                "  removal_rate: 0.1",
            ]
        ),
        encoding="utf-8",
    )

    run(str(cfg))
    with connect(db) as conn:
        slot_count = conn.execute(
            "SELECT COUNT(*) FROM slots WHERE scenario_id='e2e_phase1'"
        ).fetchone()[0]
        assert slot_count > 0


def test_default_config_loads() -> None:
    cfg = load_config("config/default.yaml")
    assert cfg.run_id().startswith("run_")


def test_availability_booked_before_start() -> None:
    db = Path(":memory:")
    with connect(db) as conn:
        bootstrap_db(conn)
        conn.execute(
            """
            INSERT INTO slots (
              slot_id, provider_id, business_id, service_id, location_id, source_slot_id, source_provider_id,
              source_business_id, source_service_id, source_location_id, slot_start_at, slot_end_at, visible_at,
              unavailable_at, current_status, created_at, source_system, source_run_id, scenario_id
            ) VALUES
            ('slot_1', 'prov_1', 'biz_1', 'svc_1', 'loc_1', 'src_slot_1', 'src_prov_1', 'src_biz_1',
             'src_svc_1', 'src_loc_1', '2026-02-01 10:00:00', '2026-02-01 11:00:00', '2026-01-25 10:00:00',
             NULL, NULL, '2026-01-01 00:00:00', 'medscheduler', 'run_1', 'scenario_1')
            """
        )
        conn.execute(
            """
            INSERT INTO booking_events (
              event_id, slot_id, customer_id, source_event_id, source_slot_id, source_customer_id, event_type,
              event_at, source_system, source_run_id, scenario_id
            ) VALUES
            ('evt_1', 'slot_1', 'cus_1', 'src_evt_1', 'src_slot_1', 'src_cus_1', 'booked',
             '2026-02-01 08:00:00', 'medscheduler', 'run_1', 'scenario_1')
            """
        )
        apply_availability(conn, "scenario_1")
        unavailable_at = conn.execute(
            "SELECT unavailable_at FROM slots WHERE slot_id='slot_1' AND scenario_id='scenario_1'"
        ).fetchone()[0]
        assert str(unavailable_at) == "2026-02-01 08:00:00"


def test_availability_never_booked_slot_uses_start_time() -> None:
    db = Path(":memory:")
    with connect(db) as conn:
        bootstrap_db(conn)
        conn.execute(
            """
            INSERT INTO slots (
              slot_id, provider_id, business_id, service_id, location_id, source_slot_id, source_provider_id,
              source_business_id, source_service_id, source_location_id, slot_start_at, slot_end_at, visible_at,
              unavailable_at, current_status, created_at, source_system, source_run_id, scenario_id
            ) VALUES
            ('slot_2', 'prov_1', 'biz_1', 'svc_1', 'loc_1', 'src_slot_2', 'src_prov_1', 'src_biz_1',
             'src_svc_1', 'src_loc_1', '2026-02-02 10:00:00', '2026-02-02 11:00:00', '2026-01-25 10:00:00',
             NULL, NULL, '2026-01-01 00:00:00', 'medscheduler', 'run_1', 'scenario_2')
            """
        )
        apply_availability(conn, "scenario_2")
        unavailable_at = conn.execute(
            "SELECT unavailable_at FROM slots WHERE slot_id='slot_2' AND scenario_id='scenario_2'"
        ).fetchone()[0]
        assert str(unavailable_at) == "2026-02-02 10:00:00"


def test_availability_removed_slot_marks_removed_and_uses_removed_time() -> None:
    db = Path(":memory:")
    with connect(db) as conn:
        bootstrap_db(conn)
        conn.execute(
            """
            INSERT INTO slots (
              slot_id, provider_id, business_id, service_id, location_id, source_slot_id, source_provider_id,
              source_business_id, source_service_id, source_location_id, slot_start_at, slot_end_at, visible_at,
              unavailable_at, current_status, created_at, source_system, source_run_id, scenario_id
            ) VALUES
            ('slot_3', 'prov_1', 'biz_1', 'svc_1', 'loc_1', 'src_slot_3', 'src_prov_1', 'src_biz_1',
             'src_svc_1', 'src_loc_1', '2026-02-03 10:00:00', '2026-02-03 11:00:00', '2026-01-25 10:00:00',
             NULL, NULL, '2026-01-01 00:00:00', 'medscheduler', 'run_1', 'scenario_3')
            """
        )
        conn.execute(
            """
            INSERT INTO booking_events (
              event_id, slot_id, customer_id, source_event_id, source_slot_id, source_customer_id, event_type,
              event_at, source_system, source_run_id, scenario_id
            ) VALUES
            ('evt_3', 'slot_3', NULL, 'src_evt_3', 'src_slot_3', NULL, 'removed',
             '2026-02-03 05:00:00', 'medscheduler', 'run_1', 'scenario_3')
            """
        )
        apply_availability(conn, "scenario_3")
        unavailable_at, status = conn.execute(
            "SELECT unavailable_at, current_status FROM slots WHERE slot_id='slot_3' AND scenario_id='scenario_3'"
        ).fetchone()
        assert str(unavailable_at) == "2026-02-03 05:00:00"
        assert status == "removed"
