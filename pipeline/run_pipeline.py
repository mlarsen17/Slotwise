from __future__ import annotations

import argparse
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import duckdb

from pipeline.config import AppConfig, load_config
from pipeline.db import bootstrap_db, connect
from pipeline.stages.availability_stage import apply_availability
from pipeline.stages.baseline_stage import compute_cohort_baselines
from pipeline.stages.evaluation_stage import run_evaluation_suite
from pipeline.stages.extract_stage import run_extract
from pipeline.stages.feature_stage import materialize_feature_snapshot
from pipeline.stages.load_stage import load_core_tables
from pipeline.stages.optimization_stage import recommend_pricing_actions
from pipeline.stages.scoring_stage import score_slots
from pipeline.stages.underbooking_stage import detect_underbooking

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)


@dataclass(frozen=True)
class StageContext:
    cfg: AppConfig
    run_id: str
    scenario_id: str
    effective_ts: datetime
    feature_snapshot_version: str
    model_version: str
    config_hash: str
    started_at: datetime


@dataclass(frozen=True)
class StageResult:
    stage: str
    duration_ms: int
    row_count: int | None = None


StageCallable = Callable[[duckdb.DuckDBPyConnection, StageContext, dict[str, Any]], int | None]


def _insert_pipeline_run(conn: duckdb.DuckDBPyConnection, *, ctx: StageContext) -> None:
    conn.execute(
        "DELETE FROM pipeline_runs WHERE run_id = ? AND scenario_id = ?",
        [ctx.run_id, ctx.scenario_id],
    )
    conn.execute(
        """
        INSERT INTO pipeline_runs (run_id, scenario_id, effective_ts, config_hash, started_at, status)
        VALUES (?, ?, ?, ?, ?, 'running')
        """,
        [ctx.run_id, ctx.scenario_id, ctx.effective_ts, ctx.config_hash, ctx.started_at],
    )


def _upsert_run_metadata(conn: duckdb.DuckDBPyConnection, *, ctx: StageContext) -> None:
    conn.execute(
        "DELETE FROM run_metadata WHERE run_id = ? AND scenario_id = ?",
        [ctx.run_id, ctx.scenario_id],
    )
    conn.execute(
        """
        INSERT INTO run_metadata (
            run_id, scenario_id, effective_ts, random_seed, config_hash,
            config_version, model_version, feature_snapshot_version
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ctx.run_id,
            ctx.scenario_id,
            ctx.effective_ts,
            ctx.cfg.random_seed,
            ctx.config_hash,
            "default_v1",
            ctx.model_version,
            ctx.feature_snapshot_version,
        ],
    )


def _finalize_pipeline_run(
    conn: duckdb.DuckDBPyConnection,
    *,
    ctx: StageContext,
    status: str,
    ended_at: datetime,
    failure_message: str | None,
) -> None:
    conn.execute(
        """
        UPDATE pipeline_runs
        SET status = ?,
            ended_at = ?,
            duration_ms = CAST((epoch_ms(?) - epoch_ms(started_at)) AS BIGINT),
            failure_message = ?
        WHERE run_id = ? AND scenario_id = ?
        """,
        [status, ended_at, ended_at, failure_message, ctx.run_id, ctx.scenario_id],
    )


def _run_stage(
    conn: duckdb.DuckDBPyConnection,
    *,
    stage_name: str,
    stage_fn: StageCallable,
    ctx: StageContext,
    state: dict[str, Any],
    logger: logging.Logger,
) -> StageResult:
    logger.info(
        "stage=%s event=start run_id=%s scenario_id=%s",
        stage_name,
        ctx.run_id,
        ctx.scenario_id,
    )
    t0 = time.perf_counter()
    row_count = stage_fn(conn, ctx, state)
    duration_ms = int((time.perf_counter() - t0) * 1000)
    logger.info(
        "stage=%s event=end run_id=%s scenario_id=%s row_count=%s duration_ms=%d",
        stage_name,
        ctx.run_id,
        ctx.scenario_id,
        row_count,
        duration_ms,
    )
    return StageResult(stage=stage_name, duration_ms=duration_ms, row_count=row_count)


def _build_stages() -> dict[str, StageCallable]:
    def extract_stage(_conn, ctx, state):
        state["normalized"] = run_extract(ctx.cfg, run_id=ctx.run_id)
        return sum(len(v) for v in state["normalized"].values())

    def load_stage(conn, ctx, state):
        load_core_tables(
            conn,
            state["normalized"],
            scenario_id=ctx.scenario_id,
            run_id=ctx.run_id,
            effective_ts=ctx.effective_ts,
            config_hash=ctx.config_hash,
        )
        return int(
            conn.execute(
                "SELECT COUNT(*) FROM slots WHERE run_id = ? AND scenario_id = ?",
                [ctx.run_id, ctx.scenario_id],
            ).fetchone()[0]
        )

    def baseline_stage(conn, ctx, _state):
        compute_cohort_baselines(
            conn,
            scenario_id=ctx.scenario_id,
            run_id=ctx.run_id,
            feature_snapshot_version=ctx.feature_snapshot_version,
            effective_ts=ctx.effective_ts,
            bucket_boundaries=ctx.cfg.time_of_day_buckets.boundaries_hours,
        )
        return int(
            conn.execute(
                "SELECT COUNT(*) FROM cohort_baselines WHERE run_id = ? AND scenario_id = ?",
                [ctx.run_id, ctx.scenario_id],
            ).fetchone()[0]
        )

    def feature_stage(conn, ctx, _state):
        materialize_feature_snapshot(
            conn,
            scenario_id=ctx.scenario_id,
            run_id=ctx.run_id,
            feature_snapshot_version=ctx.feature_snapshot_version,
            effective_ts=ctx.effective_ts,
            bucket_boundaries=ctx.cfg.time_of_day_buckets.boundaries_hours,
        )
        return int(
            conn.execute(
                "SELECT COUNT(*) FROM feature_snapshots WHERE run_id = ? AND scenario_id = ? AND feature_snapshot_version = ?",
                [ctx.run_id, ctx.scenario_id, ctx.feature_snapshot_version],
            ).fetchone()[0]
        )

    def underbooking_stage(conn, ctx, _state):
        output = detect_underbooking(
            conn,
            scenario_id=ctx.scenario_id,
            run_id=ctx.run_id,
            feature_snapshot_version=ctx.feature_snapshot_version,
            pace_weight=ctx.cfg.underbooking.pace_weight,
            fill_weight=ctx.cfg.underbooking.fill_weight,
            underbooking_threshold=ctx.cfg.underbooking.underbooking_threshold,
            sparse_baseline_fill_rate=ctx.cfg.underbooking.sparse_baseline_fill_rate,
        )
        return len(output)

    def scoring_stage(conn, ctx, _state):
        output = score_slots(
            conn,
            scenario_id=ctx.scenario_id,
            run_id=ctx.run_id,
            feature_snapshot_version=ctx.feature_snapshot_version,
            model_version=ctx.model_version,
            l2_c=ctx.cfg.scoring.l2_c,
            effective_ts=ctx.effective_ts,
            training_min_rows=ctx.cfg.scoring.training_min_rows,
        )
        return len(output)

    def optimization_stage(conn, ctx, _state):
        output = recommend_pricing_actions(
            conn,
            scenario_id=ctx.scenario_id,
            run_id=ctx.run_id,
            feature_snapshot_version=ctx.feature_snapshot_version,
            effective_ts=ctx.effective_ts,
            random_seed=ctx.cfg.random_seed,
            action_ladder=ctx.cfg.action_ladder,
            max_discount_lead_time_hours=ctx.cfg.max_discount_lead_time_hours,
            max_discount_pct=ctx.cfg.global_discount_limits.max_pct,
            excluded_services=ctx.cfg.optimizer.excluded_services,
            price_floor_pct=ctx.cfg.optimizer.price_floor_pct,
            healthy_zero_only=ctx.cfg.optimizer.healthy_zero_only,
            severity_breakpoints=ctx.cfg.optimizer.severity_breakpoints,
            discount_steps=ctx.cfg.optimizer.discount_steps,
            exploration_share=ctx.cfg.optimizer.exploration_share,
        )
        return len(output)

    def evaluation_stage(conn, ctx, _state):
        output = run_evaluation_suite(
            conn,
            scenario_id=ctx.scenario_id,
            run_id=ctx.run_id,
            feature_snapshot_version=ctx.feature_snapshot_version,
        )
        return len(output)

    return {
        "extract": extract_stage,
        "load": load_stage,
        "availability": lambda conn, ctx, _state: (
            apply_availability(
                conn, ctx.scenario_id, run_id=ctx.run_id, effective_ts=ctx.effective_ts
            ),
            int(
                conn.execute(
                    "SELECT COUNT(*) FROM slots WHERE run_id = ? AND scenario_id = ?",
                    [ctx.run_id, ctx.scenario_id],
                ).fetchone()[0]
            ),
        )[1],
        "baseline": baseline_stage,
        "feature": feature_stage,
        "underbooking": underbooking_stage,
        "scoring": scoring_stage,
        "optimization": optimization_stage,
        "evaluation": evaluation_stage,
    }


def run(
    config_path: str = "config/default.yaml",
    *,
    stages: list[str] | None = None,
    from_stage: str | None = None,
) -> list[StageResult]:
    logger = logging.getLogger(__name__)
    cfg = load_config(config_path)
    ctx = StageContext(
        cfg=cfg,
        run_id=cfg.resolved_run_id(),
        scenario_id=cfg.scenario_id,
        effective_ts=cfg.effective_ts,
        feature_snapshot_version=cfg.feature_snapshot_version(),
        model_version=cfg.model_version(),
        config_hash=cfg.config_hash(),
        started_at=datetime.now(timezone.utc),
    )

    stage_map = _build_stages()
    ordered = list(stage_map)
    if stages:
        unknown = [s for s in stages if s not in stage_map]
        if unknown:
            raise ValueError(f"Unknown stages requested: {unknown}")
        selected = stages
    elif from_stage:
        if from_stage not in stage_map:
            raise ValueError(f"Unknown from_stage: {from_stage}")
        selected = ordered[ordered.index(from_stage) :]
    else:
        selected = ordered

    results: list[StageResult] = []
    state: dict[str, Any] = {}
    current_stage: str | None = None
    with connect(cfg.duckdb_path) as conn:
        bootstrap_db(conn)
        _insert_pipeline_run(conn, ctx=ctx)
        _upsert_run_metadata(conn, ctx=ctx)
        try:
            for stage in selected:
                current_stage = stage
                results.append(
                    _run_stage(
                        conn,
                        stage_name=stage,
                        stage_fn=stage_map[stage],
                        ctx=ctx,
                        state=state,
                        logger=logger,
                    )
                )
            slot_count = conn.execute(
                "SELECT COUNT(*) FROM slots WHERE run_id = ? AND scenario_id = ?",
                [ctx.run_id, ctx.scenario_id],
            ).fetchone()[0]
            underbooked_count = conn.execute(
                "SELECT COUNT(*) FROM underbooking_outputs WHERE run_id = ? AND scenario_id = ? AND underbooked = TRUE",
                [ctx.run_id, ctx.scenario_id],
            ).fetchone()[0]
            scored_count = conn.execute(
                "SELECT COUNT(*) FROM scoring_outputs WHERE run_id = ? AND scenario_id = ? AND feature_snapshot_version = ?",
                [ctx.run_id, ctx.scenario_id, ctx.feature_snapshot_version],
            ).fetchone()[0]
            action_count = conn.execute(
                "SELECT COUNT(*) FROM pricing_actions WHERE run_id = ? AND scenario_id = ?",
                [ctx.run_id, ctx.scenario_id],
            ).fetchone()[0]
            exploration_share_observed = conn.execute(
                """
                SELECT COALESCE(AVG(CASE WHEN was_exploration THEN 1.0 ELSE 0.0 END), 0.0)
                FROM pricing_actions
                WHERE run_id = ? AND scenario_id = ?
                """,
                [ctx.run_id, ctx.scenario_id],
            ).fetchone()[0]
            logger.info(
                "run_summary run_id=%s scenario_id=%s stages=%d total_duration_ms=%d total_slots=%d underbooked_slots=%d scored_slots=%d pricing_actions=%d exploration_share_observed=%.4f",
                ctx.run_id,
                ctx.scenario_id,
                len(results),
                sum(result.duration_ms for result in results),
                slot_count,
                underbooked_count,
                scored_count,
                action_count,
                float(exploration_share_observed),
            )
            _finalize_pipeline_run(
                conn,
                ctx=ctx,
                status="success",
                ended_at=datetime.now(timezone.utc),
                failure_message=None,
            )
        except Exception as exc:
            logger.exception(
                "pipeline_failed run_id=%s scenario_id=%s failed_stage=%s",
                ctx.run_id,
                ctx.scenario_id,
                current_stage,
            )
            _finalize_pipeline_run(
                conn,
                ctx=ctx,
                status="failed",
                ended_at=datetime.now(timezone.utc),
                failure_message=f"{type(exc).__name__}: {exc}",
            )
            raise
    return results


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Slotwise pipeline")
    parser.add_argument("--config", default="config/default.yaml", help="Path to config yaml")
    parser.add_argument(
        "--stage", action="append", dest="stages", help="Run one stage (repeatable)"
    )
    parser.add_argument("--from-stage", dest="from_stage", help="Resume from this stage")
    args = parser.parse_args()
    run(args.config, stages=args.stages, from_stage=args.from_stage)


if __name__ == "__main__":
    main()
