from __future__ import annotations

import logging
import time

from pipeline.config import load_config
from pipeline.db import bootstrap_db, connect
from pipeline.stages.availability_stage import apply_availability
from pipeline.stages.extract_stage import run_extract
from pipeline.stages.load_stage import load_core_tables

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


if __name__ == "__main__":
    run()
