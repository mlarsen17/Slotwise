from __future__ import annotations

import logging
import time

import duckdb

from pipeline.config import load_config
from pipeline.db import bootstrap_db, connect
from pipeline.stages.availability_stage import apply_availability
from pipeline.stages.baseline_stage import compute_cohort_baselines
from pipeline.stages.extract_stage import run_extract
from pipeline.stages.feature_stage import materialize_feature_snapshot
from pipeline.stages.load_stage import load_core_tables
from pipeline.stages.optimization_stage import recommend_pricing_actions
from pipeline.stages.scoring_stage import score_slots
from pipeline.stages.underbooking_stage import detect_underbooking

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


def _upsert_pipeline_run(
    conn: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    scenario_id: str,
    effective_ts,
    config_hash: str,
    status: str,
) -> None:
    conn.execute(
        "DELETE FROM pipeline_runs WHERE run_id = ? AND scenario_id = ?", [run_id, scenario_id]
    )
    conn.execute(
        """
        INSERT INTO pipeline_runs (run_id, scenario_id, effective_ts, config_hash, started_at, status)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [run_id, scenario_id, effective_ts, config_hash, effective_ts, status],
    )


def run(config_path: str = "config/default.yaml") -> None:
    logger = logging.getLogger(__name__)
    cfg = load_config(config_path)
    run_id = cfg.resolved_run_id()
    effective_ts = cfg.effective_ts
    config_hash = cfg.config_hash()
    t0 = time.perf_counter()
    normalized = run_extract(cfg, run_id=run_id)
    logger.info(
        "stage=extract run_id=%s scenario_id=%s output_rows=%s elapsed_ms=%d",
        run_id,
        cfg.scenario_id,
        {k: len(v) for k, v in normalized.items()},
        int((time.perf_counter() - t0) * 1000),
    )
    with connect(cfg.duckdb_path) as conn:
        bootstrap_db(conn)
        _upsert_pipeline_run(
            conn,
            run_id=run_id,
            scenario_id=cfg.scenario_id,
            effective_ts=effective_ts,
            config_hash=config_hash,
            status="running",
        )
        feature_snapshot_version = cfg.feature_snapshot_version()
        try:
            t1 = time.perf_counter()
            load_core_tables(
                conn,
                normalized,
                scenario_id=cfg.scenario_id,
                run_id=run_id,
                effective_ts=effective_ts,
                config_hash=config_hash,
            )
            logger.info(
                "stage=load run_id=%s scenario_id=%s elapsed_ms=%d",
                run_id,
                cfg.scenario_id,
                int((time.perf_counter() - t1) * 1000),
            )

            t2 = time.perf_counter()
            apply_availability(conn, cfg.scenario_id, run_id=run_id, effective_ts=effective_ts)
            logger.info(
                "stage=availability run_id=%s scenario_id=%s elapsed_ms=%d",
                run_id,
                cfg.scenario_id,
                int((time.perf_counter() - t2) * 1000),
            )

            t3 = time.perf_counter()
            compute_cohort_baselines(
                conn,
                scenario_id=cfg.scenario_id,
                run_id=run_id,
                feature_snapshot_version=feature_snapshot_version,
                effective_ts=effective_ts,
                bucket_boundaries=cfg.time_of_day_buckets.boundaries_hours,
            )
            logger.info(
                "stage=baselines run_id=%s scenario_id=%s elapsed_ms=%d",
                run_id,
                cfg.scenario_id,
                int((time.perf_counter() - t3) * 1000),
            )

            t4 = time.perf_counter()
            materialize_feature_snapshot(
                conn,
                scenario_id=cfg.scenario_id,
                run_id=run_id,
                feature_snapshot_version=feature_snapshot_version,
                effective_ts=effective_ts,
                bucket_boundaries=cfg.time_of_day_buckets.boundaries_hours,
            )
            logger.info(
                "stage=features run_id=%s scenario_id=%s elapsed_ms=%d",
                run_id,
                cfg.scenario_id,
                int((time.perf_counter() - t4) * 1000),
            )

            t5 = time.perf_counter()
            underbooking_output = detect_underbooking(
                conn,
                scenario_id=cfg.scenario_id,
                run_id=run_id,
                feature_snapshot_version=feature_snapshot_version,
                pace_weight=cfg.underbooking.pace_weight,
                fill_weight=cfg.underbooking.fill_weight,
                underbooking_threshold=cfg.underbooking.underbooking_threshold,
                sparse_baseline_fill_rate=cfg.underbooking.sparse_baseline_fill_rate,
            )
            logger.info(
                "stage=underbooking run_id=%s scenario_id=%s output_rows=%d elapsed_ms=%d",
                run_id,
                cfg.scenario_id,
                len(underbooking_output),
                int((time.perf_counter() - t5) * 1000),
            )

            t6 = time.perf_counter()
            scoring_output = score_slots(
                conn,
                scenario_id=cfg.scenario_id,
                run_id=run_id,
                feature_snapshot_version=feature_snapshot_version,
                model_version=cfg.model_version(),
                l2_c=cfg.scoring.l2_c,
            )
            logger.info(
                "stage=scoring run_id=%s scenario_id=%s output_rows=%d elapsed_ms=%d",
                run_id,
                cfg.scenario_id,
                len(scoring_output),
                int((time.perf_counter() - t6) * 1000),
            )

            t7 = time.perf_counter()
            actions_output = recommend_pricing_actions(
                conn,
                scenario_id=cfg.scenario_id,
                run_id=run_id,
                feature_snapshot_version=feature_snapshot_version,
                effective_ts=effective_ts,
                random_seed=cfg.random_seed,
                action_ladder=cfg.action_ladder,
                lead_time_windows_hours=cfg.lead_time_windows_hours,
                max_discount_pct=cfg.global_discount_limits.max_pct,
                excluded_services=cfg.optimizer.excluded_services,
                price_floor_pct=cfg.optimizer.price_floor_pct,
                healthy_zero_only=cfg.optimizer.healthy_zero_only,
                severity_breakpoints=cfg.optimizer.severity_breakpoints,
                discount_steps=cfg.optimizer.discount_steps,
                exploration_share=cfg.optimizer.exploration_share,
            )
            logger.info(
                "stage=optimization run_id=%s scenario_id=%s output_rows=%d elapsed_ms=%d",
                run_id,
                cfg.scenario_id,
                len(actions_output),
                int((time.perf_counter() - t7) * 1000),
            )
            _upsert_pipeline_run(
                conn,
                run_id=run_id,
                scenario_id=cfg.scenario_id,
                effective_ts=effective_ts,
                config_hash=config_hash,
                status="success",
            )
            summary = conn.execute(
                """
                SELECT
                  (SELECT COUNT(*) FROM slots WHERE run_id = ? AND scenario_id = ?) AS slots_processed,
                  (SELECT COUNT(*) FROM underbooking_outputs WHERE run_id = ? AND scenario_id = ? AND underbooked) AS underbooked_slots,
                  (SELECT COUNT(*) FROM scoring_outputs WHERE run_id = ? AND scenario_id = ?) AS slots_scored,
                  (SELECT COUNT(*) FROM pricing_actions WHERE run_id = ? AND scenario_id = ?) AS pricing_actions_written,
                  COALESCE((SELECT AVG(CASE WHEN was_exploration THEN 1.0 ELSE 0.0 END)
                            FROM pricing_actions WHERE run_id = ? AND scenario_id = ?), 0.0) AS exploration_share_observed
                """,
                [run_id, cfg.scenario_id] * 5,
            ).fetchone()
            logger.info(
                "pipeline_summary run_id=%s scenario_id=%s slots_processed=%d underbooked_slots=%d slots_scored=%d pricing_actions_written=%d exploration_share_observed=%.4f",
                run_id,
                cfg.scenario_id,
                summary[0],
                summary[1],
                summary[2],
                summary[3],
                summary[4],
            )
        except Exception:
            _upsert_pipeline_run(
                conn,
                run_id=run_id,
                scenario_id=cfg.scenario_id,
                effective_ts=effective_ts,
                config_hash=config_hash,
                status="failed",
            )
            raise


if __name__ == "__main__":
    run()
