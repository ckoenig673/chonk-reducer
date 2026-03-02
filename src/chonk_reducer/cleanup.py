from __future__ import annotations

import re
import time
from dataclasses import dataclass
from pathlib import Path

from .config import Config
from .logging_utils import Logger

# Matches: something.mkv.bak.20260302_010925
_BAK_TS_RE = re.compile(r"\.bak\.(\d{8}_\d{6})$")


def _parse_bak_ts_from_name(p: Path) -> float | None:
    """
    If filename ends with .bak.YYYYMMDD_HHMMSS, return epoch seconds.
    Otherwise None.
    """
    m = _BAK_TS_RE.search(p.name)
    if not m:
        return None
    ts = m.group(1)
    try:
        # time.strptime returns localtime struct; consistent with how we stamp names
        st = time.strptime(ts, "%Y%m%d_%H%M%S")
        return time.mktime(st)
    except Exception:
        return None


def _file_age_epoch(p: Path) -> float:
    """
    Prefer timestamp embedded in .bak filename (if present),
    otherwise fall back to filesystem mtime.
    """
    ts = _parse_bak_ts_from_name(p)
    if ts is not None:
        return ts
    try:
        return p.stat().st_mtime
    except Exception:
        return 0.0


@dataclass(frozen=True)
class CleanupResult:
    deleted: int


def cleanup_baks(media_root: Path, cfg: Config, logger: Logger) -> CleanupResult:
    """
    Delete old backup files (*.bak.*) under media_root.
    Uses timestamp-in-filename when available; otherwise uses mtime.
    """
    days = int(cfg.bak_retention_days)
    if days <= 0:
        logger.log("Cleanup: BAK retention <= 0, skipping")
        return CleanupResult(deleted=0)

    cutoff = time.time() - (days * 86400)
    deleted = 0

    for p in media_root.rglob("*"):
        if not p.is_file():
            continue
        if ".bak." not in p.name:
            continue

        age = _file_age_epoch(p)
        if age < cutoff:
            try:
                logger.log(f"Cleanup: deleting old bak: {p}")
                p.unlink()
                deleted += 1
            except Exception as e:
                logger.log(f"Cleanup: failed delete: {p} ({e})")

    logger.log(f"Cleanup: deleted {deleted} files for pattern *.bak.* under {media_root}")
    return CleanupResult(deleted=deleted)


def cleanup_work_dir(work_root: Path, cfg: Config, logger: Logger) -> CleanupResult:
    """
    Delete old encoded/temp artifacts in work_root.
    NOTE: If hours <= 0, we skip (safer than 'delete everything older than now').
    """
    hours = int(cfg.work_cleanup_hours)
    if hours <= 0:
        logger.log("Cleanup: WORK_CLEANUP_HOURS <= 0, skipping work cleanup")
        return CleanupResult(deleted=0)

    cutoff = time.time() - (hours * 3600)
    deleted = 0

    patterns = ("*.encoded.mkv", "*.encoded.*.mkv", "*.tmp", "*.partial", "*.log")

    for pat in patterns:
        for p in work_root.glob(pat):
            if not p.is_file():
                continue
            try:
                if p.stat().st_mtime < cutoff:
                    logger.log(f"Cleanup: deleting work file: {p}")
                    p.unlink()
                    deleted += 1
            except Exception as e:
                logger.log(f"Cleanup: failed delete: {p} ({e})")

    logger.log(f"Cleanup: deleted {deleted} work files under {work_root}")
    return CleanupResult(deleted=deleted)