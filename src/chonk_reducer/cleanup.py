from __future__ import annotations

import time
from pathlib import Path

from .logging_utils import Logger


def _delete_older_than(root: Path, glob_pat: str, cutoff_epoch: float, logger: Logger) -> int:
    n = 0
    for p in root.rglob(glob_pat):
        try:
            if p.stat().st_mtime < cutoff_epoch:
                p.unlink(missing_ok=True)
                n += 1
        except Exception:
            continue
    if n:
        logger.log(f"Cleanup: deleted {n} files for pattern {glob_pat} under {root}")
    return n


def cleanup_logs(log_dir: Path, retention_days: int, logger: Logger) -> None:
    cutoff = time.time() - (retention_days * 86400)
    _delete_older_than(log_dir, "*.log", cutoff, logger)


def cleanup_baks(media_root: Path, retention_days: int, logger: Logger) -> None:
    cutoff = time.time() - (retention_days * 86400)
    _delete_older_than(media_root, "*.bak.*", cutoff, logger)


def cleanup_work_dir(work_root: Path, cleanup_hours: int, logger: Logger) -> None:
    cutoff = time.time() - (cleanup_hours * 3600) if cleanup_hours > 0 else time.time()
    _delete_older_than(work_root, "*.encoded.mkv", cutoff, logger)
    _delete_older_than(work_root, "*.encoded.mp4", cutoff, logger)
