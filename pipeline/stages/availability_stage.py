from __future__ import annotations

from datetime import datetime

import duckdb


def apply_availability(
    conn: duckdb.DuckDBPyConnection, scenario_id: str, *, run_id: str, effective_ts: datetime
) -> None:
    conn.execute(
        """
        UPDATE slots s
        SET unavailable_at = COALESCE(
          (
            SELECT MIN(candidate_at)
            FROM (
              SELECT MIN(event_at) AS candidate_at
              FROM booking_events e
              WHERE e.slot_id = s.slot_id
                AND e.scenario_id = s.scenario_id
                AND e.run_id = s.run_id
                AND (
                  e.event_type IN ('removed', 'completed', 'no_show', 'rescheduled')
                  OR (
                    e.event_type = 'booked'
                    AND NOT EXISTS (
                      SELECT 1 FROM booking_events c
                      WHERE c.slot_id = e.slot_id
                        AND c.scenario_id = e.scenario_id
                        AND c.run_id = e.run_id
                        AND c.event_type = 'canceled'
                        AND c.event_at > e.event_at
                    )
                  )
                )
              UNION ALL
              SELECT s.slot_start_at
            ) x
          ),
          s.slot_start_at
        ),
        current_status = COALESCE(
          (
            SELECT
              CASE
                WHEN e2.event_type = 'canceled' AND s.slot_start_at > ? THEN 'open'
                ELSE e2.event_type
              END
            FROM booking_events e2
            WHERE e2.slot_id = s.slot_id
              AND e2.scenario_id = s.scenario_id
              AND e2.run_id = s.run_id
            ORDER BY e2.event_at DESC
            LIMIT 1
          ),
          CASE WHEN s.slot_start_at <= ? THEN 'expired' ELSE 'open' END
        )
        WHERE s.scenario_id = ? AND s.run_id = ?
        """,
        [effective_ts, effective_ts, scenario_id, run_id],
    )

    bad_rows = conn.execute(
        "SELECT COUNT(*) FROM slots WHERE scenario_id = ? AND run_id = ? AND visible_at > unavailable_at",
        [scenario_id, run_id],
    ).fetchone()[0]
    if bad_rows > 0:
        raise ValueError("Malformed slot histories: visible_at > unavailable_at")
