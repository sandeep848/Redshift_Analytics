from __future__ import annotations

from typing import Optional

import duckdb


def _relation_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    # DuckDB keeps tables/views in information_schema.tables
    row = con.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_name = ?
        LIMIT 1
        """,
        [name],
    ).fetchone()
    return row is not None


def fetch_recent_processed(
    con: duckdb.DuckDBPyConnection,
    *,
    processed_table: str,
    limit: int = 500,
    deployment_type: Optional[str] = None,
) -> "duckdb.DuckDBPyRelation":
    if deployment_type and deployment_type != "all":
        return con.execute(
            f"""
            SELECT
                arrival_timestamp,
                query_id,
                deployment_type,
                instance_id,
                duration_seconds,
                queue_duration_ms,
                compile_duration_ms,
                execution_duration_ms,
                scanned_mb,
                spilled_mb,
                spill_pressure,
                queued
            FROM {processed_table}
            WHERE deployment_type = ?
            ORDER BY arrival_timestamp DESC
            LIMIT ?
            """,
            [deployment_type, limit],
        )

    return con.execute(
        f"""
        SELECT
            arrival_timestamp,
            query_id,
            deployment_type,
            instance_id,
            duration_seconds,
            queue_duration_ms,
            compile_duration_ms,
            execution_duration_ms,
            scanned_mb,
            spilled_mb,
            spill_pressure,
            queued
        FROM {processed_table}
        ORDER BY arrival_timestamp DESC
        LIMIT ?
        """,
        [limit],
    )


def fetch_rollups_last_minutes(
    con: duckdb.DuckDBPyConnection,
    *,
    rollups_table: str,
    minutes: int = 60,
    deployment_type: Optional[str] = None,
) -> "duckdb.DuckDBPyRelation":
    # Prefer always-up-to-date view if available
    rollups_source = "v_rollups_minute" if _relation_exists(con, "v_rollups_minute") else rollups_table

    if deployment_type and deployment_type != "all":
        return con.execute(
            f"""
            SELECT
                window_start,
                deployment_type,
                query_count,
                avg_duration_seconds,
                avg_spill_pressure,
                queued_ratio
            FROM {rollups_source}
            WHERE window_start >= now() - INTERVAL '{minutes} minutes'
              AND deployment_type = ?
            ORDER BY window_start ASC
            """,
            [deployment_type],
        )

    return con.execute(
        f"""
        SELECT
            window_start,
            deployment_type,
            query_count,
            avg_duration_seconds,
            avg_spill_pressure,
            queued_ratio
        FROM {rollups_source}
        WHERE window_start >= now() - INTERVAL '{minutes} minutes'
        ORDER BY window_start ASC
        """
    )


def fetch_distinct_deployment_types(
    con: duckdb.DuckDBPyConnection,
    *,
    processed_table: str,
) -> list[str]:
    rows = con.execute(f"SELECT DISTINCT deployment_type FROM {processed_table} ORDER BY 1").fetchall()
    return [r[0] for r in rows]

SQL_HISTORICAL_DEPLOYMENT = """
SELECT
  date_trunc('hour', arrival_timestamp) AS window_start,
  deployment_type,
  COUNT(*) AS query_count,
  AVG(duration_seconds) AS avg_duration_seconds,
  AVG(spill_pressure) AS avg_spill_pressure,
  AVG(CASE WHEN queued THEN 1 ELSE 0 END) AS queued_ratio
FROM query_metrics_processed
WHERE arrival_timestamp >= now() - INTERVAL '7 days'
GROUP BY 1, 2
ORDER BY window_start ASC;
"""

SQL_TOP_QUERIES = """
SELECT
  query_id,
  deployment_type,
  COUNT(*) AS occurrences,
  AVG(duration_seconds) AS avg_duration_seconds,
  AVG(spill_pressure) AS avg_spill_pressure,
  MAX({metric}) AS metric_value,
  MAX(arrival_timestamp) AS last_seen
FROM {table}
WHERE arrival_timestamp BETWEEN ? AND ?
  AND (? = 'all' OR deployment_type = ?)
GROUP BY 1, 2
ORDER BY metric_value DESC, last_seen DESC
LIMIT ?;
"""
