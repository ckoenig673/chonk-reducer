from __future__ import annotations

import os
from pathlib import Path

from .config import Config
from .logging_utils import Logger


def apply_perms(path: Path, cfg: Config) -> None:
    try:
        os.chown(path, cfg.out_uid, cfg.out_gid)
    except Exception:
        pass
    try:
        os.chmod(path, cfg.out_mode)
    except Exception:
        pass


def apply_dir_perms(dir_path: Path, cfg: Config) -> None:
    try:
        os.chown(dir_path, cfg.out_uid, cfg.out_gid)
    except Exception:
        pass
    try:
        os.chmod(dir_path, cfg.out_dir_mode)
    except Exception:
        pass


def swap_in(src: Path, encoded: Path, cfg: Config, logger: Logger) -> Path:
    stamp = "swap"
    # encoded name: <src>.YYYYMMDD_HHMMSS.encoded.mkv
    parts = encoded.name.split(".")
    if len(parts) >= 4:
        stamp = parts[-3]

    bak = src.with_name(src.name + f".bak.{stamp}")
    logger.log(f"Backup: {bak}")
    src.rename(bak)

    logger.log(f"Swap in: {encoded} -> {src}")
    try:
        encoded.replace(src)
    except OSError:
        import shutil
        tmp = src.with_name(src.name + f".tmp.{stamp}")
        shutil.copy2(encoded, tmp)
        tmp.replace(src)
        encoded.unlink(missing_ok=True)

    marker = src.with_suffix(src.suffix + ".optimized")
    marker.write_text("", encoding="utf-8", newline="\n")

    apply_dir_perms(src.parent, cfg)
    apply_perms(src, cfg)
    apply_perms(marker, cfg)

    return bak
