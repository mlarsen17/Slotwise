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

    @staticmethod
    def _build_filter_clause(
        *,
        business_id: str | None = None,
        provider_id: str | None = None,
        service_id: str | None = None,
        lead_time_band: str | None = None,
        discounted_only: bool = False,
        exploration_only: bool = False,
    ) -> tuple[str, list[object]]:
        conditions: list[str] = []
        params: list[object] = []
        if business_id:
            conditions.append("s.business_id = ?")
            params.append(business_id)
        if provider_id:
            conditions.append("s.provider_id = ?")
            params.append(provider_id)
        if service_id:
            conditions.append("s.service_id = ?")
            params.append(service_id)
        if lead_time_band:
            conditions.append("f.effective_lead_time_band = ?")
            params.append(lead_time_band)
        if discounted_only:
            conditions.append("p.action_value > 0")
        if exploration_only:
            conditions.append("p.was_exploration = TRUE")
        if not conditions:
            return "", params
        return " AND " + " AND ".join(conditions), params

    def recommendations(
        self,
        run_id: str,
        scenario_id: str,
        *,
        business_id: str | None = None,
        provider_id: str | None = None,
        service_id: str | None = None,
        lead_time_band: str | None = None,
        discounted_only: bool = False,
        exploration_only: bool = False,
        sort_field: str = "severity_score",
        sort_desc: bool = True,
    ) -> pd.DataFrame:
        sort_field_map = {
            "severity_score": "u.severity_score",
            "recommended_discount": "p.action_value",
        }
        order_expr = sort_field_map.get(sort_field, "u.severity_score")
        direction = "DESC" if sort_desc else "ASC"
        filter_clause, filter_params = self._build_filter_clause(
            business_id=business_id,
            provider_id=provider_id,
            service_id=service_id,
            lead_time_band=lead_time_band,
            discounted_only=discounted_only,
            exploration_only=exploration_only,
        )
        with self._conn() as conn:
            return conn.execute(
                f"""
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
                {filter_clause}
                ORDER BY {order_expr} {direction}, p.action_value DESC, p.slot_id
                """,
                [run_id, scenario_id, *filter_params],
            ).fetchdf()

    def evaluation(self, run_id: str, scenario_id: str) -> pd.DataFrame:
        with self._conn() as conn:
            return conn.execute(
                "SELECT metric_name, metric_value FROM evaluation_results WHERE run_id = ? AND scenario_id = ?",
                [run_id, scenario_id],
            ).fetchdf()

    def severity_distribution(self, run_id: str, scenario_id: str) -> pd.DataFrame:
        with self._conn() as conn:
            return conn.execute(
                """
                SELECT
                  CASE
                    WHEN severity_score < 0.25 THEN '0.00-0.24'
                    WHEN severity_score < 0.50 THEN '0.25-0.49'
                    WHEN severity_score < 0.75 THEN '0.50-0.74'
                    ELSE '0.75-1.00'
                  END AS severity_band,
                  COUNT(*) AS slot_count
                FROM underbooking_outputs
                WHERE run_id = ? AND scenario_id = ?
                GROUP BY 1
                ORDER BY 1
                """,
                [run_id, scenario_id],
            ).fetchdf()

    def summary_counts(
        self,
        run_id: str,
        scenario_id: str,
        *,
        business_id: str | None = None,
        provider_id: str | None = None,
        service_id: str | None = None,
        lead_time_band: str | None = None,
        discounted_only: bool = False,
        exploration_only: bool = False,
    ) -> dict[str, pd.DataFrame]:
        filter_clause, filter_params = self._build_filter_clause(
            business_id=business_id,
            provider_id=provider_id,
            service_id=service_id,
            lead_time_band=lead_time_band,
            discounted_only=discounted_only,
            exploration_only=exploration_only,
        )
        with self._conn() as conn:
            by_action = conn.execute(
                f"""
                SELECT action_value AS action_bucket, COUNT(*) AS recommendation_count
                FROM pricing_actions p
                JOIN slots s
                  ON p.slot_id = s.slot_id AND p.run_id = s.run_id AND p.scenario_id = s.scenario_id
                LEFT JOIN feature_snapshots f
                  ON p.slot_id = f.slot_id AND p.run_id = f.run_id AND p.scenario_id = f.scenario_id
                 AND p.feature_snapshot_version = f.feature_snapshot_version
                WHERE p.run_id = ? AND p.scenario_id = ?
                {filter_clause}
                GROUP BY 1
                ORDER BY 1
                """,
                [run_id, scenario_id, *filter_params],
            ).fetchdf()
            by_provider = conn.execute(
                f"""
                SELECT s.provider_id, COUNT(*) AS recommendation_count
                FROM pricing_actions p
                JOIN slots s
                  ON s.slot_id = p.slot_id AND s.run_id = p.run_id AND s.scenario_id = p.scenario_id
                LEFT JOIN feature_snapshots f
                  ON p.slot_id = f.slot_id AND p.run_id = f.run_id AND p.scenario_id = f.scenario_id
                 AND p.feature_snapshot_version = f.feature_snapshot_version
                WHERE p.run_id = ? AND p.scenario_id = ?
                {filter_clause}
                GROUP BY 1
                ORDER BY recommendation_count DESC
                """,
                [run_id, scenario_id, *filter_params],
            ).fetchdf()
            by_service = conn.execute(
                f"""
                SELECT s.service_id, COUNT(*) AS recommendation_count
                FROM pricing_actions p
                JOIN slots s
                  ON s.slot_id = p.slot_id AND s.run_id = p.run_id AND s.scenario_id = p.scenario_id
                LEFT JOIN feature_snapshots f
                  ON p.slot_id = f.slot_id AND p.run_id = f.run_id AND p.scenario_id = f.scenario_id
                 AND p.feature_snapshot_version = f.feature_snapshot_version
                WHERE p.run_id = ? AND p.scenario_id = ?
                {filter_clause}
                GROUP BY 1
                ORDER BY recommendation_count DESC
                """,
                [run_id, scenario_id, *filter_params],
            ).fetchdf()
            by_lead_time = conn.execute(
                f"""
                SELECT f.effective_lead_time_band, COUNT(*) AS recommendation_count
                FROM pricing_actions p
                JOIN slots s
                  ON s.slot_id = p.slot_id AND s.run_id = p.run_id AND s.scenario_id = p.scenario_id
                JOIN feature_snapshots f
                  ON f.slot_id = p.slot_id
                 AND f.run_id = p.run_id
                 AND f.scenario_id = p.scenario_id
                 AND f.feature_snapshot_version = p.feature_snapshot_version
                WHERE p.run_id = ? AND p.scenario_id = ?
                {filter_clause}
                GROUP BY 1
                ORDER BY 1
                """,
                [run_id, scenario_id, *filter_params],
            ).fetchdf()
            return {
                "by_action": by_action,
                "by_provider": by_provider,
                "by_service": by_service,
                "by_lead_time_band": by_lead_time,
            }
