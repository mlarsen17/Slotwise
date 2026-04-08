from __future__ import annotations

import logging

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
    normalized = run_extract(cfg)
    with connect(cfg.duckdb_path) as conn:
        bootstrap_db(conn)
        load_core_tables(conn, normalized, cfg.scenario_id)
        apply_availability(conn, cfg.scenario_id)


if __name__ == "__main__":
    run()
