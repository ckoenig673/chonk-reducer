from __future__ import annotations

import sqlite3
from pathlib import Path


def connect_settings_db(
    db_path: Path,
    *,
    qsv_quality_default: int,
    qsv_preset_default: int,
    min_savings_percent_default: float,
    skip_codecs_default: str,
    skip_resolution_tags_default: str,
    skip_min_height_default: int,
) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS libraries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            path TEXT NOT NULL UNIQUE,
            enabled INTEGER NOT NULL DEFAULT 1,
            schedule TEXT NOT NULL DEFAULT '',
            min_size_gb REAL NOT NULL DEFAULT 0.0,
            max_files INTEGER NOT NULL DEFAULT 1,
            priority INTEGER NOT NULL DEFAULT 100,
            qsv_quality INTEGER,
            qsv_preset INTEGER,
            min_savings_percent REAL,
            max_savings_percent REAL,
            skip_codecs TEXT NOT NULL DEFAULT '',
            skip_min_height INTEGER NOT NULL DEFAULT 0,
            skip_resolution_tags TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    library_columns = {str(row[1]).strip().lower() for row in conn.execute("PRAGMA table_info(libraries)").fetchall()}
    if "min_size_gb" not in library_columns:
        conn.execute("ALTER TABLE libraries ADD COLUMN min_size_gb REAL NOT NULL DEFAULT 0.0")
    if "max_files" not in library_columns:
        conn.execute("ALTER TABLE libraries ADD COLUMN max_files INTEGER NOT NULL DEFAULT 1")
    if "priority" not in library_columns:
        conn.execute("ALTER TABLE libraries ADD COLUMN priority INTEGER NOT NULL DEFAULT 100")
    if "qsv_quality" not in library_columns:
        conn.execute("ALTER TABLE libraries ADD COLUMN qsv_quality INTEGER")
    if "qsv_preset" not in library_columns:
        conn.execute("ALTER TABLE libraries ADD COLUMN qsv_preset INTEGER")
    if "min_savings_percent" not in library_columns:
        conn.execute("ALTER TABLE libraries ADD COLUMN min_savings_percent REAL")
    if "max_savings_percent" not in library_columns:
        conn.execute("ALTER TABLE libraries ADD COLUMN max_savings_percent REAL")
    if "skip_codecs" not in library_columns:
        conn.execute("ALTER TABLE libraries ADD COLUMN skip_codecs TEXT NOT NULL DEFAULT ''")
    if "skip_min_height" not in library_columns:
        conn.execute("ALTER TABLE libraries ADD COLUMN skip_min_height INTEGER NOT NULL DEFAULT 0")
    if "skip_resolution_tags" not in library_columns:
        conn.execute("ALTER TABLE libraries ADD COLUMN skip_resolution_tags TEXT NOT NULL DEFAULT ''")

    conn.execute("UPDATE libraries SET min_size_gb = COALESCE(min_size_gb, 0.0)")
    conn.execute("UPDATE libraries SET max_files = CASE WHEN max_files IS NULL OR max_files < 1 THEN 1 ELSE max_files END")
    conn.execute("UPDATE libraries SET priority = COALESCE(priority, 100)")
    conn.execute(
        "UPDATE libraries SET qsv_quality = CASE WHEN qsv_quality IS NULL OR qsv_quality < 0 THEN ? ELSE qsv_quality END",
        (qsv_quality_default,),
    )
    conn.execute(
        "UPDATE libraries SET qsv_preset = CASE WHEN qsv_preset IS NULL OR qsv_preset < 0 THEN ? ELSE qsv_preset END",
        (qsv_preset_default,),
    )
    conn.execute(
        "UPDATE libraries SET min_savings_percent = CASE WHEN min_savings_percent IS NULL OR min_savings_percent < 0 THEN ? ELSE min_savings_percent END",
        (min_savings_percent_default,),
    )
    conn.execute(
        "UPDATE libraries SET max_savings_percent = CASE WHEN max_savings_percent IS NOT NULL AND max_savings_percent < 0 THEN NULL ELSE max_savings_percent END"
    )
    conn.execute(
        "UPDATE libraries SET skip_codecs = CASE WHEN skip_codecs IS NULL OR trim(skip_codecs) = '' THEN ? ELSE skip_codecs END",
        (skip_codecs_default,),
    )
    conn.execute(
        "UPDATE libraries SET skip_resolution_tags = CASE WHEN skip_resolution_tags IS NULL OR trim(skip_resolution_tags) = '' THEN ? ELSE skip_resolution_tags END",
        (skip_resolution_tags_default,),
    )
    conn.execute(
        "UPDATE libraries SET skip_min_height = CASE WHEN skip_min_height IS NULL OR skip_min_height < 0 THEN ? ELSE skip_min_height END",
        (max(0, int(skip_min_height_default)),),
    )

    rows = conn.execute("SELECT id, skip_codecs, skip_resolution_tags FROM libraries").fetchall()
    for row in rows:
        conn.execute(
            "UPDATE libraries SET skip_codecs = ?, skip_resolution_tags = ? WHERE id = ?",
            (
                _normalize_csv_text(str(row["skip_codecs"] or "")),
                _normalize_csv_text(str(row["skip_resolution_tags"] or "")),
                int(row["id"]),
            ),
        )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS activity_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            level TEXT NOT NULL,
            library TEXT,
            run_id TEXT,
            event_type TEXT NOT NULL,
            message TEXT NOT NULL
        )
        """
    )
    return conn


def _normalize_csv_text(value: str) -> str:
    parts: list[str] = []
    seen: set[str] = set()
    for raw in str(value or "").split(","):
        token = raw.strip().lower()
        if not token or token in seen:
            continue
        parts.append(token)
        seen.add(token)
    return ",".join(parts)
