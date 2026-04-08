from __future__ import annotations

import re
import sqlite3
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


_RESOLUTION_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"(?<!\d)(4320)(?:p)?(?!\d)", re.IGNORECASE), "4320p+"),
    (re.compile(r"(?<!\d)(2160)(?:p)?(?!\d)|\b4k\b", re.IGNORECASE), "2160p"),
    (re.compile(r"(?<!\d)(1440)(?:p)?(?!\d)", re.IGNORECASE), "1440p"),
    (re.compile(r"(?<!\d)(1080)(?:p)?(?!\d)", re.IGNORECASE), "1080p"),
    (re.compile(r"(?<!\d)(720)(?:p)?(?!\d)", re.IGNORECASE), "720p"),
    (re.compile(r"(?<!\d)(576)(?:p)?(?!\d)", re.IGNORECASE), "576p"),
    (re.compile(r"(?<!\d)(480)(?:p)?(?!\d)", re.IGNORECASE), "480p"),
)


@dataclass(frozen=True)
class _CacheEntry:
    signature: tuple[int, ...]
    computed_at: float
    value: dict[str, Any]


class HistorySummariesService:
    """Cheap, cached historical encode summary aggregates for scoring groundwork."""

    def __init__(self, *, cache_ttl_seconds: int = 900) -> None:
        self._cache_ttl_seconds = max(1, int(cache_ttl_seconds))
        self._lock = threading.Lock()
        self._cache: dict[str, _CacheEntry] = {}

    def get_summaries(self, db_path: str | Path, *, now_ts: float | None = None) -> dict[str, Any]:
        db_file = Path(db_path)
        if not db_file.exists():
            return {
                "generated_at": int(time.time() if now_ts is None else now_ts),
                "sample_size": 0,
                "by_codec": [],
                "by_resolution_bucket": [],
                "by_library": [],
            }

        now_value = time.time() if now_ts is None else float(now_ts)
        cache_key = str(db_file.resolve())

        with self._lock:
            existing = self._cache.get(cache_key)
            if existing is not None:
                is_fresh = (now_value - existing.computed_at) < self._cache_ttl_seconds
                if is_fresh:
                    return existing.value

            signature = self._build_signature(db_file)
            if existing is not None and existing.signature == signature:
                self._cache[cache_key] = _CacheEntry(signature=signature, computed_at=now_value, value=existing.value)
                return existing.value

            computed = self._compute(db_file, generated_at=int(now_value))
            self._cache[cache_key] = _CacheEntry(signature=signature, computed_at=now_value, value=computed)
            return computed

    def _build_signature(self, db_file: Path) -> tuple[int, ...]:
        db_stat = db_file.stat()
        wal_file = Path(f"{db_file}-wal")
        shm_file = Path(f"{db_file}-shm")

        wal_mtime_ns = 0
        wal_size = 0
        if wal_file.exists():
            wal_stat = wal_file.stat()
            wal_mtime_ns = int(wal_stat.st_mtime_ns)
            wal_size = int(wal_stat.st_size)

        shm_mtime_ns = 0
        shm_size = 0
        if shm_file.exists():
            shm_stat = shm_file.stat()
            shm_mtime_ns = int(shm_stat.st_mtime_ns)
            shm_size = int(shm_stat.st_size)

        data_version = 0
        encode_row_count = 0
        encode_max_rowid = 0
        try:
            conn = sqlite3.connect(str(db_file))
            try:
                data_version_row = conn.execute("PRAGMA data_version").fetchone()
                if data_version_row:
                    data_version = int(data_version_row[0] or 0)
                count_row = conn.execute("SELECT COUNT(*), COALESCE(MAX(rowid), 0) FROM encodes").fetchone()
                if count_row:
                    encode_row_count = int(count_row[0] or 0)
                    encode_max_rowid = int(count_row[1] or 0)
            finally:
                conn.close()
        except Exception:
            data_version = 0
            encode_row_count = 0
            encode_max_rowid = 0

        return (
            int(db_stat.st_mtime_ns),
            int(db_stat.st_size),
            wal_mtime_ns,
            wal_size,
            shm_mtime_ns,
            shm_size,
            data_version,
            encode_row_count,
            encode_max_rowid,
        )

    def _compute(self, db_file: Path, *, generated_at: int) -> dict[str, Any]:
        conn = sqlite3.connect(str(db_file))
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                """
                SELECT
                    library,
                    codec_from,
                    path,
                    filename,
                    saved_pct
                FROM encodes
                WHERE status = 'success'
                  AND saved_pct IS NOT NULL
                """
            ).fetchall()
        finally:
            conn.close()

        codec_buckets: dict[str, list[float]] = defaultdict(list)
        resolution_buckets: dict[str, list[float]] = defaultdict(list)
        library_buckets: dict[str, list[float]] = defaultdict(list)

        for row in rows:
            try:
                saved_pct = float(row["saved_pct"])
            except Exception:
                continue

            codec = (str(row["codec_from"] or "unknown").strip().lower() or "unknown")
            library = (str(row["library"] or "unknown").strip().lower() or "unknown")
            path_text = str(row["path"] or row["filename"] or "")
            resolution_bucket = _resolution_bucket(path_text)

            codec_buckets[codec].append(saved_pct)
            resolution_buckets[resolution_bucket].append(saved_pct)
            library_buckets[library].append(saved_pct)

        return {
            "generated_at": generated_at,
            "sample_size": sum(len(values) for values in codec_buckets.values()),
            "by_codec": _to_rows(codec_buckets, "codec"),
            "by_resolution_bucket": _to_rows(resolution_buckets, "resolution_bucket"),
            "by_library": _to_rows(library_buckets, "library"),
        }


def _to_rows(bucket_values: dict[str, list[float]], label_key: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for label in sorted(bucket_values):
        values = bucket_values[label]
        if not values:
            continue
        out.append(
            {
                label_key: label,
                "sample_count": len(values),
                "avg_savings_pct": round(sum(values) / len(values), 3),
            }
        )
    return out


def _resolution_bucket(path_text: str) -> str:
    text = str(path_text or "")
    for pattern, bucket in _RESOLUTION_PATTERNS:
        if pattern.search(text):
            return bucket
    return "unknown"


_DEFAULT_HISTORY_SUMMARIES = HistorySummariesService()


def get_history_summaries(db_path: str | Path, *, now_ts: float | None = None) -> dict[str, Any]:
    return _DEFAULT_HISTORY_SUMMARIES.get_summaries(db_path, now_ts=now_ts)
