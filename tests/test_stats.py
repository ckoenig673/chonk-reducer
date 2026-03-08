from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

from chonk_reducer.stats import (
    ensure_database,
    fetch_run_summaries,
    record_failure,
    record_skip,
    record_success,
    record_run_log_path,
)


class StubLogger:
    def __init__(self) -> None:
        self.messages = []

    def log(self, msg: str) -> None:
        self.messages.append(msg)


def _cfg(tmp_path: Path):
    return SimpleNamespace(
        stats_enabled=True,
        stats_path=tmp_path / "chonk.db",
        version="test",
        media_root=tmp_path / "movies",
        library="movies",
        encoder="hevc_qsv",
        qsv_quality=21,
        qsv_preset=7,
    )




def _write_legacy_event(path: Path, *, run_id: str = "run-legacy") -> None:
    row = {
        "ts": "2026-01-01T00:00:01",
        "run_id": run_id,
        "version": "test",
        "library": "movies",
        "mode": "live",
        "encoder": "hevc_qsv",
        "quality": 21,
        "preset": 7,
        "status": "success",
        "stage": "swap",
        "path": "/movies/a.mkv",
        "filename": "a.mkv",
        "size_before_bytes": 100,
        "size_after_bytes": 60,
        "saved_bytes": 40,
        "saved_pct": 40.0,
        "duration_seconds": 1.2,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(row) + "\n", encoding="utf-8")


def _count_rows(db_path: Path, table: str) -> int:
    conn = sqlite3.connect(str(db_path))
    cur = conn.execute(f"SELECT COUNT(*) FROM {table}")
    val = int(cur.fetchone()[0])
    conn.close()
    return val


def test_database_initialization_creates_schema_and_wal(tmp_path):
    cfg = _cfg(tmp_path)
    db_path = ensure_database(cfg, StubLogger())

    assert db_path.exists()
    conn = sqlite3.connect(str(db_path))
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    mode = conn.execute("PRAGMA journal_mode;").fetchone()[0].lower()
    conn.close()

    assert "runs" in tables
    assert "encodes" in tables
    assert mode == "wal"


def test_migration_from_ndjson(tmp_path):
    cfg = _cfg(tmp_path)
    legacy = cfg.media_root / ".chonkstats.ndjson"
    legacy.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "ts": "2026-01-01T00:00:01",
            "run_id": "run-1",
            "version": "test",
            "library": "movies",
            "mode": "live",
            "encoder": "hevc_qsv",
            "quality": 21,
            "preset": 7,
            "status": "success",
            "stage": "swap",
            "path": "/movies/a.mkv",
            "filename": "a.mkv",
            "size_before_bytes": 100,
            "size_after_bytes": 60,
            "saved_bytes": 40,
            "saved_pct": 40.0,
            "duration_seconds": 1.2,
        },
        {
            "ts": "2026-01-01T00:00:03",
            "run_id": "run-1",
            "version": "test",
            "library": "movies",
            "mode": "live",
            "encoder": "hevc_qsv",
            "quality": 21,
            "preset": 7,
            "status": "failed",
            "stage": "encode",
            "fail_stage": "encode",
            "path": "/movies/b.mkv",
            "filename": "b.mkv",
            "size_before_bytes": 200,
            "duration_seconds": 0.3,
            "error_type": "RuntimeError",
            "error_msg": "boom",
        },
    ]
    legacy.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")

    ensure_database(cfg, StubLogger())

    migrated = legacy.with_suffix(legacy.suffix + ".migrated")
    assert migrated.exists()
    assert not legacy.exists()
    assert _count_rows(cfg.stats_path, "encodes") == 2
    summaries = fetch_run_summaries(cfg.stats_path)
    assert summaries[0]["run_id"] == "run-1"
    assert summaries[0]["success_count"] == 1
    assert summaries[0]["failed_count"] == 1




def test_migration_runs_when_db_exists(tmp_path):
    cfg = _cfg(tmp_path)
    logger = StubLogger()
    ensure_database(cfg, logger)

    legacy = cfg.media_root / ".chonkstats.ndjson"
    _write_legacy_event(legacy, run_id="run-db-exists")

    ensure_database(cfg, logger)

    migrated = legacy.with_suffix(legacy.suffix + ".migrated")
    assert migrated.exists()
    assert _count_rows(cfg.stats_path, "encodes") == 1


def test_migration_is_idempotent_and_renames_file(tmp_path):
    cfg = _cfg(tmp_path)
    logger = StubLogger()
    legacy = cfg.media_root / ".chonkstats.ndjson"
    _write_legacy_event(legacy, run_id="run-idempotent")

    ensure_database(cfg, logger)
    first_count = _count_rows(cfg.stats_path, "encodes")

    ensure_database(cfg, logger)

    migrated = legacy.with_suffix(legacy.suffix + ".migrated")
    assert migrated.exists()
    assert not legacy.exists()
    assert first_count == 1
    assert _count_rows(cfg.stats_path, "encodes") == 1


def test_migration_skips_duplicate_records(tmp_path):
    cfg = _cfg(tmp_path)
    logger = StubLogger()
    ensure_database(cfg, logger)

    src = tmp_path / "movie.mkv"
    src.write_bytes(b"x")
    record_success(
        cfg,
        logger,
        run_id="run-dup",
        mode="live",
        stage="swap",
        src=src,
        before_bytes=100,
        after_bytes=60,
        codec_from="h264",
        codec_to="hevc",
        duration_seconds=1.2,
    )

    conn = sqlite3.connect(str(cfg.stats_path))
    ts = conn.execute("SELECT ts FROM encodes WHERE run_id = ?", ("run-dup",)).fetchone()[0]
    conn.close()

    legacy = cfg.media_root / ".chonkstats.ndjson"
    row = {
        "ts": ts,
        "run_id": "run-dup",
        "version": "test",
        "library": "movies",
        "mode": "live",
        "encoder": "hevc_qsv",
        "quality": 21,
        "preset": 7,
        "status": "success",
        "stage": "swap",
        "path": str(src),
        "filename": src.name,
        "size_before_bytes": 100,
        "size_after_bytes": 60,
        "saved_bytes": 40,
        "saved_pct": 40.0,
        "duration_seconds": 1.2,
    }
    legacy.parent.mkdir(parents=True, exist_ok=True)
    legacy.write_text(json.dumps(row) + "\n", encoding="utf-8")

    ensure_database(cfg, logger)

    assert _count_rows(cfg.stats_path, "encodes") == 1




def test_runs_table_migration_adds_summary_counter_columns(tmp_path):
    cfg = _cfg(tmp_path)
    db_path = cfg.stats_path
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE runs (
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
            duration_seconds REAL NOT NULL DEFAULT 0.0
        )
        """
    )
    conn.commit()
    conn.close()

    ensure_database(cfg, StubLogger())

    conn = sqlite3.connect(str(db_path))
    cols = {row[1] for row in conn.execute("PRAGMA table_info(runs)").fetchall()}
    conn.close()

    expected = {
        "candidates_found",
        "prefiltered_count",
        "evaluated_count",
        "processed_count",
        "prefiltered_marker_count",
        "prefiltered_backup_count",
        "skipped_codec_count",
        "skipped_resolution_count",
        "skipped_min_savings_count",
        "skipped_max_savings_count",
        "skipped_dry_run_count",
        "ignored_folder_count",
        "ignored_file_count",
        "raw_log_path",
    }
    assert expected.issubset(cols)

def test_record_run_log_path_persists_value(tmp_path):
    cfg = _cfg(tmp_path)
    src = tmp_path / "movie.mkv"
    src.write_bytes(b"x")

    record_run_log_path(
        cfg,
        StubLogger(),
        run_id="run-log-path",
        mode="live",
        raw_log_path=tmp_path / "work" / "logs" / "transcode_20260101_000000.log",
    )

    conn = sqlite3.connect(str(cfg.stats_path))
    row = conn.execute("SELECT raw_log_path FROM runs WHERE run_id = ?", ("run-log-path",)).fetchone()
    conn.close()

    assert row is not None
    assert row[0].endswith("transcode_20260101_000000.log")


def test_encode_insertion(tmp_path):
    cfg = _cfg(tmp_path)
    src = tmp_path / "movie.mkv"
    src.write_bytes(b"x")

    record_success(
        cfg,
        StubLogger(),
        run_id="r1",
        mode="live",
        stage="swap",
        src=src,
        before_bytes=1000,
        after_bytes=600,
        codec_from="h264",
        codec_to="hevc",
        duration_seconds=1.2,
    )

    conn = sqlite3.connect(str(cfg.stats_path))
    row = conn.execute("SELECT status, saved_bytes FROM encodes").fetchone()
    conn.close()
    assert row[0] == "success"
    assert row[1] == 400


def test_run_summaries(tmp_path):
    cfg = _cfg(tmp_path)
    src = tmp_path / "movie.mkv"
    src.write_bytes(b"x")

    record_skip(
        cfg,
        StubLogger(),
        run_id="r2",
        mode="live",
        skip_reason="min_savings",
        src=src,
        before_bytes=1000,
        detail="too small",
    )
    record_failure(
        cfg,
        StubLogger(),
        run_id="r2",
        mode="live",
        stage="encode",
        src=src,
        before_bytes=1000,
        duration_seconds=0.3,
        err=RuntimeError("boom"),
    )

    summaries = fetch_run_summaries(cfg.stats_path)
    assert len(summaries) == 1
    summary = summaries[0]
    assert summary["run_id"] == "r2"
    assert summary["skipped_count"] == 1
    assert summary["failed_count"] == 1
    assert summary["candidates_found"] == 0
    assert summary["processed_count"] == 0
