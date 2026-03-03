from __future__ import annotations

from pathlib import Path
from collections import defaultdict

from .config import Config
from .logging_utils import Logger


def is_excluded(path: Path, cfg: Config) -> bool:
    parts_lower = [p.lower() for p in path.parts]
    for ex in cfg.exclude_path_parts:
        ex_l = ex.lower()
        if any(ex_l == part for part in parts_lower):
            return True
        if any(ex_l in part for part in parts_lower):
            return True
    return False


def find_ignore_root(path: Path, media_root: Path) -> Path | None:
    """
    Walk upward from file path to media_root looking for .chonkignore.
    Returns the folder containing the ignore marker if found.
    """
    current = path.parent
    while True:
        if (current / ".chonkignore").exists():
            return current
        if current == media_root or current.parent == current:
            break
        current = current.parent
    return None


def gather_candidates(cfg: Config, logger: Logger):
    """
    Returns:
        candidates: list[Path]
        ignored_folders: dict[Path, int]  # folder -> count of skipped files
    """
    min_bytes = int(cfg.min_size_gb * 1024**3)

    candidates: list[Path] = []
    ignored_folders = defaultdict(int)
    failed_skipped: list[Path] = []

    for p in cfg.media_root.rglob("*.mkv"):
        try:
            if is_excluded(p, cfg):
                continue

            ignore_root = find_ignore_root(p, cfg.media_root)
            if ignore_root:
                ignored_folders[ignore_root] += 1
                continue


            # Skip files previously marked as failed/quarantined
            failed_marker = p.with_suffix(p.suffix + ".failed")
            if failed_marker.exists():
                failed_skipped.append(p)
                continue

            if ".bak." in p.name:
                continue
            if p.name.endswith(".encoded.mkv"):
                continue
            if p.stat().st_size < min_bytes:
                continue

            candidates.append(p)

        except FileNotFoundError:
            continue
        except Exception:
            continue

    candidates.sort(key=lambda x: x.stat().st_size, reverse=True)

    # Log skipped failed files summary
    if failed_skipped:
        logger.log("===== SKIPPED FAILED FILES (.failed marker) =====")
        logger.log(f"SKIPPED FAILED: {len(failed_skipped)}")
        for p in failed_skipped[:10]:
            logger.log(f"FAILED-MARKED: {p}")
        if len(failed_skipped) > 10:
            logger.log(f"...and {len(failed_skipped) - 10} more")
        logger.log("==============================================")

    # Log ignored folders summary
    if ignored_folders:
        logger.log("===== IGNORED FOLDERS (.chonkignore) =====")
        for folder, count in sorted(ignored_folders.items()):
            logger.log(f"IGNORED: {folder}  ({count} files)")
        logger.log("==========================================")

    return candidates, ignored_folders