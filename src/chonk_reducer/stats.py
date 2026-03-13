from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from .config import Config
from .logging_utils import Logger


SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,
    ts_start TEXT NOT NULL,
    ts_end TEXT NOT NULL,
    mode TEXT,
    library TEXT,
    version TEXT,
    encoder TEXT,
    quality INTEGER,
    preset INTEGER,
    success_count INTEGER NOT NULL DEFAULT 0,
    failed_count INTEGER NOT NULL DEFAULT 0,
    skipped_count INTEGER NOT NULL DEFAULT 0,
    before_bytes INTEGER NOT NULL DEFAULT 0,
    after_bytes INTEGER NOT NULL DEFAULT 0,
    saved_bytes INTEGER NOT NULL DEFAULT 0,
    duration_seconds REAL NOT NULL DEFAULT 0.0,
    candidates_found INTEGER NOT NULL DEFAULT 0,
    prefiltered_count INTEGER NOT NULL DEFAULT 0,
    evaluated_count INTEGER NOT NULL DEFAULT 0,
    processed_count INTEGER NOT NULL DEFAULT 0,
    prefiltered_marker_count INTEGER NOT NULL DEFAULT 0,
    prefiltered_backup_count INTEGER NOT NULL DEFAULT 0,
    skipped_codec_count INTEGER NOT NULL DEFAULT 0,
    skipped_resolution_count INTEGER NOT NULL DEFAULT 0,
    skipped_min_savings_count INTEGER NOT NULL DEFAULT 0,
    skipped_max_savings_count INTEGER NOT NULL DEFAULT 0,
    skipped_dry_run_count INTEGER NOT NULL DEFAULT 0,
    ignored_folder_count INTEGER NOT NULL DEFAULT 0,
    ignored_file_count INTEGER NOT NULL DEFAULT 0,
    raw_log_path TEXT
);

CREATE TABLE IF NOT EXISTS encodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TEXT NOT NULL,
    run_id TEXT NOT NULL,
    version TEXT,
    library TEXT,
    mode TEXT,
    encoder TEXT,
    quality INTEGER,
    preset INTEGER,
    status TEXT NOT NULL,
    stage TEXT,
    fail_stage TEXT,
    skip_reason TEXT,
    skip_detail TEXT,
    path TEXT,
    filename TEXT,
    size_before_bytes INTEGER,
    size_after_bytes INTEGER,
    saved_bytes INTEGER,
    saved_pct REAL,
    codec_from TEXT,
    codec_to TEXT,
    duration_seconds REAL,
    error_type TEXT,
    error_msg TEXT,
    encoded_path TEXT,
    encoded_bytes INTEGER,
    bak_path TEXT,
    FOREIGN KEY(run_id) REFERENCES runs(run_id)
);
CREATE INDEX IF NOT EXISTS idx_encodes_ts ON encodes(ts);
CREATE INDEX IF NOT EXISTS idx_encodes_library_ts ON encodes(library, ts);
CREATE INDEX IF NOT EXISTS idx_encodes_status_ts ON encodes(status, ts);

CREATE TABLE IF NOT EXISTS policy_skip_cache (
    library TEXT NOT NULL,
    path TEXT NOT NULL,
    skip_reason TEXT NOT NULL,
    savings_percent REAL NOT NULL,
    ts_cached TEXT NOT NULL,
    PRIMARY KEY (library, path, skip_reason)
);
CREATE INDEX IF NOT EXISTS idx_policy_skip_cache_reason ON policy_skip_cache(skip_reason);
"""


RUNS_COUNTER_COLUMNS = {
    "candidates_found": "INTEGER NOT NULL DEFAULT 0",
    "prefiltered_count": "INTEGER NOT NULL DEFAULT 0",
    "evaluated_count": "INTEGER NOT NULL DEFAULT 0",
    "processed_count": "INTEGER NOT NULL DEFAULT 0",
    "prefiltered_marker_count": "INTEGER NOT NULL DEFAULT 0",
    "prefiltered_backup_count": "INTEGER NOT NULL DEFAULT 0",
    "skipped_codec_count": "INTEGER NOT NULL DEFAULT 0",
    "skipped_resolution_count": "INTEGER NOT NULL DEFAULT 0",
    "skipped_min_savings_count": "INTEGER NOT NULL DEFAULT 0",
    "skipped_max_savings_count": "INTEGER NOT NULL DEFAULT 0",
    "skipped_dry_run_count": "INTEGER NOT NULL DEFAULT 0",
    "ignored_folder_count": "INTEGER NOT NULL DEFAULT 0",
    "ignored_file_count": "INTEGER NOT NULL DEFAULT 0",
    "raw_log_path": "TEXT",
}


def _iso_ts() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())


def _safe_str(e: Exception) -> str:
    s = str(e).replace("\n", " ").strip()
    return (s[:500] + "…") if len(s) > 500 else s


def infer_library(cfg: Config) -> str:
    if getattr(cfg, "library", ""):
        return cfg.library
    s = str(cfg.media_root).lower()
    if "tv" in s:
        return "tv"
    if "movie" in s:
        return "movies"
    return "media"


def build_base(cfg: Config, run_id: str, mode: str) -> dict[str, Any]:
    return {
        "ts": _iso_ts(),
        "run_id": run_id,
        "version": getattr(cfg, "version", "") or "unknown",
        "library": infer_library(cfg),
        "mode": mode,
        "encoder": getattr(cfg, "encoder", "hevc_qsv"),
        "quality": int(cfg.qsv_quality),
        "preset": int(cfg.qsv_preset),
    }


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.executescript(SCHEMA)
    _ensure_runs_columns(conn)
    _ensure_policy_skip_cache_columns(conn)
    return conn


def _ensure_runs_columns(conn: sqlite3.Connection) -> None:
    existing = {
        str(row[1])
        for row in conn.execute("PRAGMA table_info(runs)").fetchall()
    }
    for col, col_type in RUNS_COUNTER_COLUMNS.items():
        if col not in existing:
            conn.execute(f"ALTER TABLE runs ADD COLUMN {col} {col_type}")


def _ensure_policy_skip_cache_columns(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS policy_skip_cache (
            library TEXT NOT NULL,
            path TEXT NOT NULL,
            skip_reason TEXT NOT NULL,
            savings_percent REAL NOT NULL,
            ts_cached TEXT NOT NULL,
            PRIMARY KEY (library, path, skip_reason)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_policy_skip_cache_reason ON policy_skip_cache(skip_reason)")


def _legacy_stats_path(cfg: Config, db_path: Path) -> Path:
    if db_path.suffix.lower() == ".ndjson":
        return db_path
    return Path(cfg.media_root) / ".chonkstats.ndjson"


def _parse_ts(ts: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def _iter_ndjson(path: Path) -> Iterable[Dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                if isinstance(row, dict):
                    yield row
    except Exception:
        return


def _event_exists(conn: sqlite3.Connection, obj: Dict[str, Any]) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM encodes
        WHERE run_id = ?
          AND ts = ?
          AND status = ?
          AND COALESCE(stage, '') = COALESCE(?, '')
          AND COALESCE(path, '') = COALESCE(?, '')
          AND COALESCE(filename, '') = COALESCE(?, '')
          AND COALESCE(size_before_bytes, -1) = COALESCE(?, -1)
          AND COALESCE(size_after_bytes, -1) = COALESCE(?, -1)
          AND COALESCE(saved_bytes, -1) = COALESCE(?, -1)
        LIMIT 1
        """,
        (
            str(obj.get("run_id") or ""),
            str(obj.get("ts") or ""),
            str(obj.get("status") or ""),
            obj.get("stage"),
            obj.get("path"),
            obj.get("filename"),
            obj.get("size_before_bytes"),
            obj.get("size_after_bytes"),
            obj.get("saved_bytes"),
        ),
    ).fetchone()
    return row is not None


def _update_run_summary(conn: sqlite3.Connection, run_id: str) -> None:
    conn.execute(
        """
        UPDATE runs
        SET
            ts_start = COALESCE((SELECT MIN(ts) FROM encodes WHERE run_id = ?), ts_start),
            ts_end = COALESCE((SELECT MAX(ts) FROM encodes WHERE run_id = ?), ts_end),
            success_count = COALESCE((SELECT COUNT(*) FROM encodes WHERE run_id = ? AND status = 'success'), 0),
            failed_count = COALESCE((SELECT COUNT(*) FROM encodes WHERE run_id = ? AND status = 'failed'), 0),
            skipped_count = COALESCE((SELECT COUNT(*) FROM encodes WHERE run_id = ? AND status = 'skipped'), 0),
            before_bytes = COALESCE((SELECT SUM(COALESCE(size_before_bytes, 0)) FROM encodes WHERE run_id = ? AND status = 'success'), 0),
            after_bytes = COALESCE((SELECT SUM(COALESCE(size_after_bytes, 0)) FROM encodes WHERE run_id = ? AND status = 'success'), 0),
            saved_bytes = COALESCE((SELECT SUM(COALESCE(saved_bytes, 0)) FROM encodes WHERE run_id = ? AND status = 'success'), 0),
            duration_seconds = COALESCE((SELECT SUM(COALESCE(duration_seconds, 0.0)) FROM encodes WHERE run_id = ?), 0.0)
        WHERE run_id = ?
        """,
        (run_id, run_id, run_id, run_id, run_id, run_id, run_id, run_id, run_id, run_id),
    )


def _insert_event(conn: sqlite3.Connection, obj: Dict[str, Any]) -> None:
    run_id = str(obj.get("run_id") or "")
    ts = str(obj.get("ts") or _iso_ts())
    conn.execute(
        """
        INSERT INTO runs(run_id, ts_start, ts_end, mode, library, version, encoder, quality, preset)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(run_id) DO UPDATE SET
            ts_start = MIN(runs.ts_start, excluded.ts_start),
            ts_end = MAX(runs.ts_end, excluded.ts_end)
        """,
        (
            run_id,
            ts,
            ts,
            obj.get("mode"),
            obj.get("library"),
            obj.get("version"),
            obj.get("encoder"),
            obj.get("quality"),
            obj.get("preset"),
        ),
    )
    conn.execute(
        """
        INSERT INTO encodes(
            ts, run_id, version, library, mode, encoder, quality, preset,
            status, stage, fail_stage, skip_reason, skip_detail,
            path, filename, size_before_bytes, size_after_bytes,
            saved_bytes, saved_pct, codec_from, codec_to, duration_seconds,
            error_type, error_msg, encoded_path, encoded_bytes, bak_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ts,
            run_id,
            obj.get("version"),
            obj.get("library"),
            obj.get("mode"),
            obj.get("encoder"),
            obj.get("quality"),
            obj.get("preset"),
            obj.get("status"),
            obj.get("stage"),
            obj.get("fail_stage"),
            obj.get("skip_reason"),
            obj.get("skip_detail"),
            obj.get("path"),
            obj.get("filename"),
            obj.get("size_before_bytes"),
            obj.get("size_after_bytes"),
            obj.get("saved_bytes"),
            obj.get("saved_pct"),
            obj.get("codec_from"),
            obj.get("codec_to"),
            obj.get("duration_seconds"),
            obj.get("error_type"),
            obj.get("error_msg"),
            obj.get("encoded_path"),
            obj.get("encoded_bytes"),
            obj.get("bak_path"),
        ),
    )
    _update_run_summary(conn, run_id)


def _migrate_ndjson_if_needed(cfg: Config, db_path: Path, logger: Logger) -> None:
    legacy = _legacy_stats_path(cfg, db_path)
    migrated_marker = Path(str(legacy) + ".migrated")
    if not legacy.exists() or migrated_marker.exists():
        return

    logger.log(f"Migrating legacy stats file: {legacy}")
    conn = _connect(db_path)
    try:
        imported = 0
        with conn:
            unknown_index = 0
            for row in _iter_ndjson(legacy):
                run_id = str(row.get("run_id") or "").strip()
                if not run_id:
                    run_id = "legacy-unknown-%d" % unknown_index
                    unknown_index += 1
                row["run_id"] = run_id
                if not _event_exists(conn, row):
                    _insert_event(conn, row)
                    imported += 1
        logger.log(f"Imported {imported} legacy records")
        legacy.rename(migrated_marker)
        logger.log(f"Renamed file to {migrated_marker.name}")
    except Exception as e:
        logger.log(f"WARN: NDJSON migration failed safely: {legacy} ({e})")
    finally:
        conn.close()


def ensure_database(cfg: Config, logger: Logger) -> Path:
    db_path = Path(cfg.stats_path)
    _migrate_ndjson_if_needed(cfg, db_path, logger)
    conn = _connect(db_path)
    conn.close()
    return db_path


def _record_event(cfg: Config, logger: Logger, obj: Dict[str, Any]) -> None:
    if not getattr(cfg, "stats_enabled", False):
        return
    try:
        db_path = ensure_database(cfg, logger)
        conn = _connect(db_path)
        with conn:
            _insert_event(conn, obj)
        conn.close()
    except Exception as e:
        logger.log(f"WARN: stats insert failed: {cfg.stats_path} ({e})")


def record_success(
    cfg: Config,
    logger: Logger,
    run_id: str,
    mode: str,
    stage: str,
    src: Path,
    before_bytes: int,
    after_bytes: int,
    codec_from: str | None,
    codec_to: str | None,
    duration_seconds: float,
    bak_path: Path | None = None,
) -> None:
    saved = int(before_bytes) - int(after_bytes)
    pct = (saved / before_bytes) * 100.0 if before_bytes else 0.0

    obj = build_base(cfg, run_id, mode)
    obj.update(
        {
            "status": "success",
            "stage": stage,
            "path": str(src),
            "filename": src.name,
            "size_before_bytes": int(before_bytes),
            "size_after_bytes": int(after_bytes),
            "saved_bytes": int(saved),
            "saved_pct": round(pct, 3),
            "codec_from": codec_from or "",
            "codec_to": codec_to or "",
            "duration_seconds": round(float(duration_seconds), 3),
        }
    )
    if bak_path is not None:
        obj["bak_path"] = str(bak_path)

    _record_event(cfg, logger, obj)


def record_failure(
    cfg: Config,
    logger: Logger,
    run_id: str,
    mode: str,
    stage: str,
    src: Path,
    before_bytes: int,
    duration_seconds: float,
    err: Exception,
    encoded_path: Path | None = None,
) -> None:
    obj = build_base(cfg, run_id, mode)
    obj.update(
        {
            "status": "failed",
            "stage": stage,
            "fail_stage": stage,
            "path": str(src),
            "filename": src.name,
            "size_before_bytes": int(before_bytes),
            "duration_seconds": round(float(duration_seconds), 3),
            "error_type": type(err).__name__,
            "error_msg": _safe_str(err),
        }
    )

    if encoded_path is not None:
        obj["encoded_path"] = str(encoded_path)
        try:
            obj["encoded_bytes"] = int(encoded_path.stat().st_size)
        except Exception:
            pass

    _record_event(cfg, logger, obj)


def record_skip(
    cfg: Config,
    logger: Logger,
    run_id: str,
    mode: str,
    skip_reason: str,
    src: Path,
    before_bytes: int,
    codec_from: str | None = None,
    detail: str | None = None,
) -> None:
    obj = build_base(cfg, run_id, mode)
    obj.update(
        {
            "status": "skipped",
            "stage": "skip",
            "skip_reason": (skip_reason or "unknown").lower(),
            "path": str(src),
            "filename": src.name,
            "size_before_bytes": int(before_bytes),
            "codec_from": codec_from or "",
        }
    )
    if detail:
        obj["skip_detail"] = str(detail)[:500]

    _record_event(cfg, logger, obj)


def record_dry_run(
    cfg: Config,
    logger: Logger,
    run_id: str,
    src: Path,
    before_bytes: int,
) -> None:
    obj = build_base(cfg, run_id, "dry_run")
    obj.update(
        {
            "status": "skipped",
            "stage": "dry_run",
            "skip_reason": "dry_run",
            "path": str(src),
            "filename": src.name,
            "size_before_bytes": int(before_bytes),
        }
    )
    _record_event(cfg, logger, obj)


def record_run_counters(
    cfg: Config,
    logger: Logger,
    *,
    run_id: str,
    candidates_found: int,
    prefiltered_count: int,
    evaluated_count: int,
    processed_count: int,
    prefiltered_marker_count: int,
    prefiltered_backup_count: int,
    skipped_codec_count: int,
    skipped_resolution_count: int,
    skipped_min_savings_count: int,
    skipped_max_savings_count: int,
    skipped_dry_run_count: int,
    ignored_folder_count: int,
    ignored_file_count: int,
) -> None:
    if not getattr(cfg, "stats_enabled", False):
        return
    try:
        db_path = ensure_database(cfg, logger)
        conn = _connect(db_path)
        with conn:
            conn.execute(
                """
                UPDATE runs
                SET
                    candidates_found = ?,
                    prefiltered_count = ?,
                    evaluated_count = ?,
                    processed_count = ?,
                    prefiltered_marker_count = ?,
                    prefiltered_backup_count = ?,
                    skipped_codec_count = ?,
                    skipped_resolution_count = ?,
                    skipped_min_savings_count = ?,
                    skipped_max_savings_count = ?,
                    skipped_dry_run_count = ?,
                    ignored_folder_count = ?,
                    ignored_file_count = ?
                WHERE run_id = ?
                """,
                (
                    int(candidates_found),
                    int(prefiltered_count),
                    int(evaluated_count),
                    int(processed_count),
                    int(prefiltered_marker_count),
                    int(prefiltered_backup_count),
                    int(skipped_codec_count),
                    int(skipped_resolution_count),
                    int(skipped_min_savings_count),
                    int(skipped_max_savings_count),
                    int(skipped_dry_run_count),
                    int(ignored_folder_count),
                    int(ignored_file_count),
                    str(run_id),
                ),
            )
        conn.close()
    except Exception as e:
        logger.log(f"WARN: run summary stats update failed: {cfg.stats_path} ({e})")


def record_run_log_path(
    cfg: Config,
    logger: Logger,
    *,
    run_id: str,
    mode: str,
    raw_log_path: Path,
) -> None:
    if not getattr(cfg, "stats_enabled", False):
        return

    now = _iso_ts()
    try:
        db_path = ensure_database(cfg, logger)
        conn = _connect(db_path)
        with conn:
            conn.execute(
                """
                INSERT INTO runs(
                    run_id, ts_start, ts_end, mode, library, version, encoder, quality, preset, raw_log_path
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    ts_start = MIN(runs.ts_start, excluded.ts_start),
                    ts_end = MAX(runs.ts_end, excluded.ts_end),
                    raw_log_path = excluded.raw_log_path
                """,
                (
                    str(run_id),
                    now,
                    now,
                    str(mode),
                    infer_library(cfg),
                    getattr(cfg, "version", "") or "unknown",
                    getattr(cfg, "encoder", "hevc_qsv"),
                    int(cfg.qsv_quality),
                    int(cfg.qsv_preset),
                    str(raw_log_path),
                ),
            )
        conn.close()
    except Exception as e:
        logger.log(f"WARN: run log path update failed: {cfg.stats_path} ({e})")


def fetch_encodes_since(db_paths: List[Path], start: datetime) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    start_iso = start.strftime("%Y-%m-%dT%H:%M:%S")
    for db_path in db_paths:
        if not db_path.exists():
            continue
        try:
            conn = _connect(db_path)
            cur = conn.execute(
                "SELECT * FROM encodes WHERE ts >= ? ORDER BY ts ASC",
                (start_iso,),
            )
            rows.extend(dict(r) for r in cur.fetchall())
            conn.close()
        except Exception:
            continue
    return rows


def fetch_run_summaries(db_path: Path) -> List[Dict[str, Any]]:
    if not db_path.exists():
        return []
    conn = _connect(db_path)
    cur = conn.execute("SELECT * FROM runs ORDER BY ts_start ASC")
    out = [dict(r) for r in cur.fetchall()]
    conn.close()
    return out


def upsert_policy_skip_cache(
    cfg: Config,
    logger: Logger,
    *,
    src: Path,
    skip_reason: str,
    savings_percent: float,
) -> None:
    if not getattr(cfg, "stats_enabled", False):
        return
    try:
        db_path = ensure_database(cfg, logger)
        conn = _connect(db_path)
        with conn:
            conn.execute(
                """
                INSERT INTO policy_skip_cache (library, path, skip_reason, savings_percent, ts_cached)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(library, path, skip_reason) DO UPDATE SET
                    savings_percent = excluded.savings_percent,
                    ts_cached = excluded.ts_cached
                """,
                (
                    infer_library(cfg),
                    str(src),
                    str(skip_reason or "").strip().lower(),
                    float(savings_percent),
                    _iso_ts(),
                ),
            )
        conn.close()
    except Exception as e:
        logger.log(f"WARN: policy skip cache upsert failed: {cfg.stats_path} ({e})")


def get_policy_skip_cache(
    cfg: Config,
    logger: Logger,
    *,
    src: Path,
    skip_reason: str,
) -> Optional[Dict[str, Any]]:
    if not getattr(cfg, "stats_enabled", False):
        return None
    try:
        db_path = ensure_database(cfg, logger)
        conn = _connect(db_path)
        row = conn.execute(
            """
            SELECT library, path, skip_reason, savings_percent, ts_cached
            FROM policy_skip_cache
            WHERE library = ? AND path = ? AND skip_reason = ?
            LIMIT 1
            """,
            (infer_library(cfg), str(src), str(skip_reason or "").strip().lower()),
        ).fetchone()
        conn.close()
        return dict(row) if row is not None else None
    except Exception as e:
        logger.log(f"WARN: policy skip cache lookup failed: {cfg.stats_path} ({e})")
        return None


def delete_policy_skip_cache(
    cfg: Config,
    logger: Logger,
    *,
    src: Path,
    skip_reason: str,
) -> None:
    if not getattr(cfg, "stats_enabled", False):
        return
    try:
        db_path = ensure_database(cfg, logger)
        conn = _connect(db_path)
        with conn:
            conn.execute(
                "DELETE FROM policy_skip_cache WHERE library = ? AND path = ? AND skip_reason = ?",
                (infer_library(cfg), str(src), str(skip_reason or "").strip().lower()),
            )
        conn.close()
    except Exception as e:
        logger.log(f"WARN: policy skip cache delete failed: {cfg.stats_path} ({e})")
