from __future__ import annotations

import os
import time
from pathlib import Path

from .logging_utils import Logger


def is_lock_stale(lock_path: Path, stale_hours: int) -> bool:
    try:
        age_hours = (time.time() - lock_path.stat().st_mtime) / 3600.0
        return age_hours > stale_hours
    except FileNotFoundError:
        return False


def acquire_lock(lock_path: Path, stale_hours: int, self_heal: bool, logger: Logger) -> bool:
    if lock_path.exists():
        if is_lock_stale(lock_path, stale_hours):
            if self_heal:
                logger.log(f"Lock is stale. Removing: {lock_path}")
                try:
                    lock_path.unlink()
                except Exception:
                    return False
            else:
                logger.log(f"Lock is stale ({lock_path}). Self-heal disabled; skipping.")
                return False
        else:
            logger.log(f"Lock exists ({lock_path}). Skipping.")
            return False

    try:
        lock_path.write_text(str(os.getpid()), encoding="utf-8", newline="\n")
        return True
    except Exception:
        return False


def release_lock(lock_path: Path, logger: Logger) -> None:
    try:
        lock_path.unlink(missing_ok=True)
    except Exception:
        logger.log(f"WARN: failed to remove lock: {lock_path}")
