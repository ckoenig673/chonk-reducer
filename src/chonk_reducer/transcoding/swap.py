from __future__ import annotations

import os
import shutil
from pathlib import Path

from ..config import Config
from ..core.logging_utils import Logger, make_run_stamp


def _touch(path: Path) -> None:
    """
    Set mtime/atime to now without changing contents.
    """
    try:
        os.utime(path, None)
    except Exception:
        pass


def _chmod(path: Path, mode: int) -> None:
    try:
        os.chmod(path, mode)
    except Exception:
        pass


def _chown(path: Path, uid: int, gid: int) -> None:
    try:
        os.chown(path, uid, gid)
    except Exception:
        pass


def make_bak_path(src: Path, stamp: str) -> Path:
    return src.with_name(f"{src.name}.bak.{stamp}")


def make_optimized_marker(src: Path) -> Path:
    return src.with_name(f"{src.name}.optimized")


def swap_in_encoded(
    original: Path,
    encoded: Path,
    cfg: Config,
    logger: Logger,
) -> tuple[Path, Path]:
    """
    1) Move original -> .bak.STAMP
    2) Move encoded -> original path
    3) Write .optimized marker
    4) Apply perms + touch files so timestamps reflect *now*
    """
    stamp = make_run_stamp()

    bak = make_bak_path(original, stamp)
    marker = make_optimized_marker(original)

    # Backup original (rename is atomic on same filesystem)
    logger.log(f"Backup: {bak}")
    original.rename(bak)

    # IMPORTANT: the bak keeps the OLD mtime unless we touch it
    _touch(bak)

    # Move encoded into place
    logger.log(f"Swap in: {encoded} -> {original}")
    encoded.rename(original)

    # Apply ownership/mode (best-effort)
    _chown(original, int(cfg.out_uid), int(cfg.out_gid))
    _chmod(original, int(cfg.out_mode))
    _touch(original)

    # Marker (idempotent)
    try:
        marker.write_text("", encoding="utf-8")
        _chown(marker, int(cfg.out_uid), int(cfg.out_gid))
        _chmod(marker, int(cfg.out_mode))
        _touch(marker)
    except Exception as e:
        logger.log(f"WARN: failed to write marker {marker}: {e}")

    return bak, marker


def restore_from_bak(original: Path, bak: Path, logger: Logger) -> None:
    """
    If something goes sideways after backup, restore.
    """
    try:
        if original.exists():
            logger.log(f"Restore: removing broken output {original}")
            original.unlink()
    except Exception:
        pass

    try:
        logger.log(f"Restore: {bak} -> {original}")
        bak.rename(original)
    except Exception as e:
        logger.log(f"Restore FAILED: {e}")

# Backward compatibility alias
swap_in = swap_in_encoded