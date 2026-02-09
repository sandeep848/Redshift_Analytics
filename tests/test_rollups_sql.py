from __future__ import annotations

from pathlib import Path


def test_rollups_sql_present() -> None:
    root = Path(__file__).resolve().parents[1]
    sql_path = root / "configs" / "sql" / "duckdb" / "002_rollups.sql"
    assert sql_path.exists()
    content = sql_path.read_text(encoding="utf-8").lower()
    assert "rollup" in content
