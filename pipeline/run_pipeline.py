from __future__ import annotations

import logging
import time

from pipeline.config import load_config
from pipeline.db import bootstrap_db, connect
from pipeline.stages.availability_stage import apply_availability
from pipeline.stages.baseline_stage import compute_cohort_baselines
from pipeline.stages.extract_stage import run_extract
from pipeline.stages.feature_stage import materialize_feature_snapshot
from pipeline.stages.load_stage import load_core_tables
from pipeline.stages.underbooking_stage import detect_underbooking

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)


def run(config_path: str = "config/default.yaml") -> None:
    cfg = load_config(config_path)
    run_id = cfg.resolved_run_id()
    effective_ts = cfg.effective_ts
    config_hash = cfg.config_hash()
    t0 = time.perf_counter()
    normalized = run_extract(cfg, run_id=run_id)
    logging.getLogger(__name__).info(
        "stage=extract run_id=%s scenario_id=%s output_rows=%s elapsed_ms=%d",
        run_id,
        cfg.scenario_id,
        {k: len(v) for k, v in normalized.items()},
        int((time.perf_counter() - t0) * 1000),
    )
    with connect(cfg.duckdb_path) as conn:
        bootstrap_db(conn)
        t1 = time.perf_counter()
        load_core_tables(
            conn,
            normalized,
            scenario_id=cfg.scenario_id,
            run_id=run_id,
            effective_ts=effective_ts,
            config_hash=config_hash,
        )
        logging.getLogger(__name__).info(
            "stage=load run_id=%s scenario_id=%s elapsed_ms=%d",
            run_id,
            cfg.scenario_id,
            int((time.perf_counter() - t1) * 1000),
        )
        t2 = time.perf_counter()
        apply_availability(conn, cfg.scenario_id, run_id=run_id, effective_ts=effective_ts)
        logging.getLogger(__name__).info(
            "stage=availability run_id=%s scenario_id=%s elapsed_ms=%d",
            run_id,
            cfg.scenario_id,
            int((time.perf_counter() - t2) * 1000),
        )

        t3 = time.perf_counter()
        feature_snapshot_version = cfg.feature_snapshot_version()
        compute_cohort_baselines(
            conn,
            scenario_id=cfg.scenario_id,
            run_id=run_id,
            feature_snapshot_version=feature_snapshot_version,
            effective_ts=effective_ts,
            bucket_boundaries=cfg.time_of_day_buckets.boundaries_hours,
        )
        logging.getLogger(__name__).info(
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
        logging.getLogger(__name__).info(
            "stage=features run_id=%s scenario_id=%s elapsed_ms=%d",
            run_id,
            cfg.scenario_id,
            int((time.perf_counter() - t4) * 1000),
        )

        t5 = time.perf_counter()
        detect_underbooking(
            conn,
            scenario_id=cfg.scenario_id,
            run_id=run_id,
            feature_snapshot_version=feature_snapshot_version,
            pace_weight=cfg.underbooking.pace_weight,
            fill_weight=cfg.underbooking.fill_weight,
            underbooking_threshold=cfg.underbooking.underbooking_threshold,
            sparse_baseline_fill_rate=cfg.underbooking.sparse_baseline_fill_rate,
        )
        logging.getLogger(__name__).info(
            "stage=underbooking run_id=%s scenario_id=%s elapsed_ms=%d",
            run_id,
            cfg.scenario_id,
            int((time.perf_counter() - t5) * 1000),
        )


if __name__ == "__main__":
    run()
