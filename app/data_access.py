from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd


class AppDataAccess:
    def __init__(self, db_path: str | Path) -> None:
        self.db_path = str(db_path)

    def _conn(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(self.db_path, read_only=True)

    def latest_runs(self) -> pd.DataFrame:
        with self._conn() as conn:
            return conn.execute(
                """
                SELECT run_id, scenario_id, started_at, status
                FROM pipeline_runs
                ORDER BY started_at DESC
                """
            ).fetchdf()

    def recommendations(self, run_id: str, scenario_id: str) -> pd.DataFrame:
        with self._conn() as conn:
            return conn.execute(
                """
                SELECT p.slot_id, s.business_id, s.provider_id, s.service_id,
                       f.effective_lead_time_band, u.underbooked, u.severity_score,
                       p.action_value AS recommended_discount,
                       s.standard_price,
                       ROUND(s.standard_price * (1 - p.action_value / 100.0), 2) AS implied_price,
                       p.rationale_codes, p.was_exploration
                FROM pricing_actions p
                JOIN slots s
                  ON p.slot_id = s.slot_id AND p.run_id = s.run_id AND p.scenario_id = s.scenario_id
                LEFT JOIN underbooking_outputs u
                  ON p.slot_id = u.slot_id AND p.run_id = u.run_id AND p.scenario_id = u.scenario_id
                LEFT JOIN feature_snapshots f
                  ON p.slot_id = f.slot_id AND p.run_id = f.run_id AND p.scenario_id = f.scenario_id
                 AND p.feature_snapshot_version = f.feature_snapshot_version
                WHERE p.run_id = ? AND p.scenario_id = ?
                """,
                [run_id, scenario_id],
            ).fetchdf()

    def evaluation(self, run_id: str, scenario_id: str) -> pd.DataFrame:
        with self._conn() as conn:
            return conn.execute(
                "SELECT metric_name, metric_value FROM evaluation_results WHERE run_id = ? AND scenario_id = ?",
                [run_id, scenario_id],
            ).fetchdf()
