from __future__ import annotations

import duckdb


def apply_availability(conn: duckdb.DuckDBPyConnection, scenario_id: str) -> None:
    conn.execute(
        """
        UPDATE slots s
        SET unavailable_at = (
            SELECT MIN(candidate_at)
            FROM (
                SELECT MIN(event_at) AS candidate_at
                FROM booking_events e
                WHERE e.slot_id = s.slot_id
                  AND e.scenario_id = s.scenario_id
                  AND e.event_type IN ('booked', 'removed')
                UNION ALL
                SELECT s.slot_start_at
            ) x
        ),
        current_status = COALESCE(
            (
                SELECT event_type
                FROM booking_events e2
                WHERE e2.slot_id = s.slot_id
                  AND e2.scenario_id = s.scenario_id
                ORDER BY e2.event_at DESC
                LIMIT 1
            ),
            CASE WHEN s.slot_start_at <= CURRENT_TIMESTAMP THEN 'expired' ELSE 'open' END
        )
        WHERE s.scenario_id = ?
        """,
        [scenario_id],
    )

    bad_rows = conn.execute(
        "SELECT COUNT(*) FROM slots WHERE scenario_id = ? AND visible_at > unavailable_at", [scenario_id]
    ).fetchone()[0]
    if bad_rows > 0:
        raise ValueError("Malformed slot histories: visible_at > unavailable_at")
