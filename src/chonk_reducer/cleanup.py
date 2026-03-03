from __future__ import annotations

import re
import time
from dataclasses import dataclass
from pathlib import Path

from .logging_utils import Logger

# Matches: something.mkv.bak.20260302_010925
_BAK_TS_RE = re.compile(r"\.bak\.(\d{8}_\d{6})$")

# Matches: prefix_transcode_20260302_010925.log (or wrapper_..., candidates_...)
_LOG_TS_RE = re.compile(r"_(\d{8}_\d{6})\.log$")


def _parse_ts_from_suffix(p: Path, pattern: re.Pattern[str]) -> float | None:
    """If regex matches a YYYYMMDD_HHMMSS group, return epoch seconds."""
    m = pattern.search(p.name)
    if not m:
        return None
    ts = m.group(1)
    try:
        st = time.strptime(ts, "%Y%m%d_%H%M%S")
        return time.mktime(st)
    except Exception:
        return None


def _file_epoch(p: Path, ts_pattern: re.Pattern[str] | None = None) -> float:
    """Prefer timestamp embedded in filename; otherwise fall back to filesystem mtime."""
    if ts_pattern is not None:
        ts = _parse_ts_from_suffix(p, ts_pattern)
        if ts is not None:
            return ts
    try:
        return p.stat().st_mtime
    except Exception:
        return 0.0


@dataclass(frozen=True)
class CleanupResult:
    deleted: int

def _path_has_excluded_part(p: Path, exclude_path_parts: tuple[str, ...]) -> bool:
    parts_lower = [part.lower() for part in p.parts]
    for ex in exclude_path_parts:
        ex_l = (ex or "").lower()
        if not ex_l:
            continue
        if any(ex_l == part for part in parts_lower):
            return True
        if any(ex_l in part for part in parts_lower):
            return True
    return False



def cleanup_baks(media_root: Path, bak_retention_days: float | int, logger: Logger) -> CleanupResult:
    """Delete backup files (*.bak.*) under media_root.

    Retention logic:
      - bak_retention_days > 0 : delete older than N days
      - bak_retention_days <= 0: delete ALL *.bak.* (force-clean)
    """
    days = int(bak_retention_days)
    cutoff = time.time() - (days * 86400) if days > 0 else time.time()

    deleted = 0
    for p in media_root.rglob("*"):
        if not p.is_file():
            continue
        if ".bak." not in p.name:
            continue

        if _file_epoch(p, _BAK_TS_RE) < cutoff:
            try:
                logger.log(f"Cleanup: deleting bak: {p}")
                p.unlink()
                deleted += 1
            except Exception as e:
                logger.log(f"Cleanup: failed delete: {p} ({e})")

    logger.log(f"Cleanup: deleted {deleted} files for pattern *.bak.* under {media_root}")
    return CleanupResult(deleted=deleted)


def cleanup_logs(log_dir: Path, log_retention_days: float | int, logger: Logger) -> CleanupResult:
    """Delete old *.log under log_dir.

    Retention logic:
      - log_retention_days > 0 : delete older than N days
      - log_retention_days <= 0: delete ALL *.log (force-clean)
    """
    if not log_dir.exists():
        return CleanupResult(deleted=0)

    days = int(log_retention_days)
    cutoff = time.time() - (days * 86400) if days > 0 else time.time()

    deleted = 0
    for p in log_dir.glob("*.log"):
        if not p.is_file():
            continue
        if _file_epoch(p, _LOG_TS_RE) < cutoff:
            try:
                logger.log(f"Cleanup: deleting log: {p}")
                p.unlink()
                deleted += 1
            except Exception as e:
                logger.log(f"Cleanup: failed delete: {p} ({e})")

    logger.log(f"Cleanup: deleted {deleted} log files under {log_dir}")
    return CleanupResult(deleted=deleted)


def cleanup_work_dir(work_root: Path, work_cleanup_hours: float | int, logger: Logger) -> CleanupResult:
    """Delete old encoded/temp artifacts in work_root.

    Retention logic:
      - work_cleanup_hours > 0 : delete older than N hours
      - work_cleanup_hours <= 0: delete ALL known temp artifacts (force-clean)
    """
    hours = int(work_cleanup_hours)
    cutoff = time.time() - (hours * 3600) if hours > 0 else time.time()

    deleted = 0
    patterns = ("*.encoded.mkv", "*.tmp", "*.partial")

    for pat in patterns:
        for p in work_root.glob(pat):
            if not p.is_file():
                continue
            if _file_epoch(p) < cutoff:
                try:
                    logger.log(f"Cleanup: deleting work file: {p}")
                    p.unlink()
                    deleted += 1
                except Exception as e:
                    logger.log(f"Cleanup: failed delete: {p} ({e})")

    logger.log(f"Cleanup: deleted {deleted} work files under {work_root}")
    return CleanupResult(deleted=deleted)


def cleanup_media_temp(media_root: Path, work_cleanup_hours: float | int, exclude_path_parts: tuple[str, ...], logger: Logger) -> CleanupResult:
    """Delete old in-place encoded/temp artifacts under media_root.

    This is for the in-place encode strategy where temp outputs live next to the source file,
    e.g. Episode.mkv.<stamp>.encoded.mkv.

    Retention logic (same as cleanup_work_dir):
      - work_cleanup_hours > 0 : delete older than N hours
      - work_cleanup_hours <= 0: delete ALL known temp artifacts (force-clean)
    """
    hours = int(work_cleanup_hours)
    cutoff = time.time() - (hours * 3600) if hours > 0 else time.time()

    deleted = 0
    patterns = ("*.encoded.mkv", "*.tmp", "*.partial")

    for pat in patterns:
        for p in media_root.rglob(pat):
            if not p.is_file():
                continue
            if _path_has_excluded_part(p, exclude_path_parts):
                continue
            # Don't touch real backups/markers even if they match patterns (extra safety)
            if ".bak." in p.name or p.name.endswith(".optimized"):
                continue
            if _file_epoch(p) < cutoff:
                try:
                    logger.log(f"Cleanup: deleting media temp file: {p}")
                    p.unlink()
                    deleted += 1
                except Exception as e:
                    logger.log(f"Cleanup: failed delete: {p} ({e})")

    logger.log(f"Cleanup: deleted {deleted} media temp files under {media_root}")
    return CleanupResult(deleted=deleted)
