from __future__ import annotations

from datetime import datetime, timezone
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


def _norm(run_id: str = "run_1") -> dict:
    raw = extract_synthetic_data(make_scenario())
    return normalize_records(raw, "test_scenario", "source_1", run_id=run_id)


def test_db_bootstrap(tmp_path: Path) -> None:
    with connect(tmp_path / "test.duckdb") as conn:
        bootstrap_db(conn)
        tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    assert {"slots", "booking_events", "pricing_actions", "pipeline_runs"}.issubset(tables)


def test_normalization_determinism_and_stable_ids() -> None:
    raw = extract_synthetic_data(make_scenario())
    one = normalize_records(raw, "test_scenario", "source_1", run_id="run_x")
    two = normalize_records(raw, "test_scenario", "source_1", run_id="run_x")
    assert one["slots"]["slot_id"].tolist() == two["slots"]["slot_id"].tolist()
    assert one["booking_events"]["event_id"].tolist() == two["booking_events"]["event_id"].tolist()


def test_idempotent_loading_and_availability(tmp_path: Path) -> None:
    with connect(tmp_path / "load.duckdb") as conn:
        bootstrap_db(conn)
        norm = _norm("run_1")
        load_core_tables(
            conn,
            norm,
            scenario_id="test_scenario",
            run_id="run_1",
            effective_ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
            config_hash="cfg_a",
        )
        apply_availability(
            conn,
            "test_scenario",
            run_id="run_1",
            effective_ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        first_slots = conn.execute(
            "SELECT COUNT(*) FROM slots WHERE scenario_id='test_scenario' AND run_id='run_1'"
        ).fetchone()[0]

        load_core_tables(
            conn,
            norm,
            scenario_id="test_scenario",
            run_id="run_1",
            effective_ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
            config_hash="cfg_a",
        )
        apply_availability(
            conn,
            "test_scenario",
            run_id="run_1",
            effective_ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        second_slots = conn.execute(
            "SELECT COUNT(*) FROM slots WHERE scenario_id='test_scenario' AND run_id='run_1'"
        ).fetchone()[0]
        malformed = conn.execute(
            "SELECT COUNT(*) FROM slots WHERE scenario_id='test_scenario' AND run_id='run_1' AND visible_at > unavailable_at"
        ).fetchone()[0]
    assert first_slots == second_slots
    assert malformed == 0


def test_transaction_rolls_back_on_failure(tmp_path: Path) -> None:
    with connect(tmp_path / "load_fail.duckdb") as conn:
        bootstrap_db(conn)
        try:
            load_core_tables(
                conn,
                _norm("run_fail"),
                scenario_id="test_scenario",
                run_id="run_fail",
                effective_ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
                config_hash="cfg_fail",
                simulate_failure=True,
            )
        except RuntimeError:
            pass
        slot_count = conn.execute(
            "SELECT COUNT(*) FROM slots WHERE scenario_id='test_scenario' AND run_id='run_fail'"
        ).fetchone()[0]
    assert slot_count == 0


def test_multiple_runs_same_scenario_coexist(tmp_path: Path) -> None:
    with connect(tmp_path / "multi.duckdb") as conn:
        bootstrap_db(conn)
        for run_id in ("run_a", "run_b"):
            load_core_tables(
                conn,
                _norm(run_id),
                scenario_id="test_scenario",
                run_id=run_id,
                effective_ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
                config_hash=f"cfg_{run_id}",
            )
        runs = conn.execute(
            "SELECT COUNT(DISTINCT run_id) FROM slots WHERE scenario_id='test_scenario'"
        ).fetchone()[0]
    assert runs == 2


def test_schema_validation_fails_fast(tmp_path: Path) -> None:
    with connect(tmp_path / "schema.duckdb") as conn:
        bootstrap_db(conn)
        norm = _norm("run_schema")
        broken = {**norm, "slots": norm["slots"].drop(columns=["slot_id"])}
        try:
            load_core_tables(
                conn,
                broken,
                scenario_id="test_scenario",
                run_id="run_schema",
                effective_ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
                config_hash="cfg_schema",
            )
            assert False, "expected ValueError"
        except ValueError as exc:
            assert "missing required columns" in str(exc)


def test_availability_canceled_slot_reopens() -> None:
    with connect(Path(":memory:")) as conn:
        bootstrap_db(conn)
        conn.execute(
            """
            INSERT INTO slots (
              slot_id, provider_id, business_id, service_id, location_id, standard_price, slot_duration_minutes,
              integration_id, external_slot_id, source_slot_id, source_provider_id, source_business_id,
              source_service_id, source_location_id, slot_start_at, slot_end_at, visible_at, unavailable_at,
              current_status, created_at, source_system, source_run_id, scenario_id, run_id, effective_ts, config_hash
            ) VALUES
            ('slot_1', 'prov_1', 'biz_1', 'svc_1', 'loc_1', 100.0, 60, 'medscheduler', 'src_slot_1', 'src_slot_1',
             'src_prov_1', 'src_biz_1', 'src_svc_1', 'src_loc_1', '2026-02-01 10:00:00', '2026-02-01 11:00:00',
             '2026-01-25 10:00:00', NULL, NULL, '2026-01-01 00:00:00', 'medscheduler', 'run_1', 'scenario_1',
             'run_1', '2026-01-01 00:00:00', 'cfg')
            """
        )
        conn.execute(
            """
            INSERT INTO booking_events (
              event_id, slot_id, customer_id, business_id, provider_id, service_type, source_event_id, source_slot_id,
              source_customer_id, event_type, event_at, source_system, source_run_id, scenario_id, run_id, effective_ts
            ) VALUES
            ('evt_1', 'slot_1', 'cus_1', 'biz_1', 'prov_1', 'svc_1', 'src_evt_1', 'src_slot_1',
             'src_cus_1', 'booked', '2026-02-01 08:00:00', 'medscheduler', 'run_1', 'scenario_1', 'run_1', '2026-01-01 00:00:00'),
            ('evt_2', 'slot_1', 'cus_1', 'biz_1', 'prov_1', 'svc_1', 'src_evt_2', 'src_slot_1',
             'src_cus_1', 'canceled', '2026-02-01 08:30:00', 'medscheduler', 'run_1', 'scenario_1', 'run_1', '2026-01-01 00:00:00')
            """
        )
        apply_availability(
            conn,
            "scenario_1",
            run_id="run_1",
            effective_ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        unavailable_at, status = conn.execute(
            "SELECT unavailable_at, current_status FROM slots WHERE slot_id='slot_1' AND scenario_id='scenario_1' AND run_id='run_1'"
        ).fetchone()
    assert str(unavailable_at) == "2026-02-01 10:00:00"
    assert status == "open"


def test_default_config_loads() -> None:
    cfg = load_config("config/default.yaml")
    assert cfg.resolved_run_id().startswith("run_")


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
                "max_discount_lead_time_hours: 168",
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
    run(str(cfg))
    with connect(db) as conn:
        summary = conn.execute(
            "SELECT md5(string_agg(slot_id || current_status || CAST(unavailable_at AS VARCHAR), '|' ORDER BY slot_id)) FROM slots WHERE scenario_id='e2e_phase1'"
        ).fetchone()[0]
    assert summary is not None
