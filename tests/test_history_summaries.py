from __future__ import annotations

import sqlite3
from pathlib import Path

from chonk_reducer.services.history_summaries import HistorySummariesService


def _seed_db(db_path: Path) -> None:
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE encodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            library TEXT,
            codec_from TEXT,
            path TEXT,
            filename TEXT,
            status TEXT,
            saved_pct REAL
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO encodes (library, codec_from, path, filename, status, saved_pct)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            ("movies", "h264", "/movies/A.2160p.mkv", "A.2160p.mkv", "success", 30.0),
            ("movies", "h264", "/movies/B.1080p.mkv", "B.1080p.mkv", "success", 50.0),
            ("tv", "mpeg2", "/tv/C.720p.mkv", "C.720p.mkv", "success", 25.0),
            ("tv", "h264", "/tv/D.4k.mkv", "D.4k.mkv", "success", 35.0),
            ("tv", "h264", "/tv/E.unknown.mkv", "E.unknown.mkv", "failed", 99.0),
        ],
    )
    conn.commit()
    conn.close()


def test_history_summaries_aggregates_by_codec_library_and_resolution(tmp_path: Path):
    db_path = tmp_path / "chonk.db"
    _seed_db(db_path)

    service = HistorySummariesService(cache_ttl_seconds=300)
    out = service.get_summaries(db_path, now_ts=1_000)

    assert out["generated_at"] == 1_000
    assert out["sample_size"] == 4
    assert out["by_codec"] == [
        {"codec": "h264", "sample_count": 3, "avg_savings_pct": 38.333},
        {"codec": "mpeg2", "sample_count": 1, "avg_savings_pct": 25.0},
    ]
    assert out["by_library"] == [
        {"library": "movies", "sample_count": 2, "avg_savings_pct": 40.0},
        {"library": "tv", "sample_count": 2, "avg_savings_pct": 30.0},
    ]
    assert out["by_resolution_bucket"] == [
        {"resolution_bucket": "1080p", "sample_count": 1, "avg_savings_pct": 50.0},
        {"resolution_bucket": "2160p", "sample_count": 2, "avg_savings_pct": 32.5},
        {"resolution_bucket": "720p", "sample_count": 1, "avg_savings_pct": 25.0},
    ]


def test_history_summaries_uses_cache_until_ttl_or_db_signature_change(tmp_path: Path):
    db_path = tmp_path / "chonk.db"
    _seed_db(db_path)
    service = HistorySummariesService(cache_ttl_seconds=30)

    initial = service.get_summaries(db_path, now_ts=100)

    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO encodes (library, codec_from, path, filename, status, saved_pct) VALUES (?, ?, ?, ?, ?, ?)",
        ("movies", "hevc", "/movies/new.1080p.mkv", "new.1080p.mkv", "success", 60.0),
    )
    conn.commit()
    conn.close()

    before_ttl = service.get_summaries(db_path, now_ts=110)
    assert before_ttl["sample_size"] == initial["sample_size"]

    after_ttl = service.get_summaries(db_path, now_ts=140)
    assert after_ttl["sample_size"] == 5
    assert any(item["codec"] == "hevc" for item in after_ttl["by_codec"])


def test_history_summaries_handles_missing_database(tmp_path: Path):
    missing = tmp_path / "missing.db"
    service = HistorySummariesService(cache_ttl_seconds=30)

    out = service.get_summaries(missing, now_ts=123)

    assert out == {
        "generated_at": 123,
        "sample_size": 0,
        "by_codec": [],
        "by_resolution_bucket": [],
        "by_library": [],
    }
