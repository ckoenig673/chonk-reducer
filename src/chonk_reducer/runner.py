from __future__ import annotations

from pathlib import Path
import sys

from .cleanup import cleanup_baks, cleanup_logs, cleanup_work_dir, cleanup_media_temp
from .config import load_config
from .discovery import gather_candidates
from .encode import encode_qsv
from .lock import acquire_lock, release_lock
from .logging_utils import Logger, make_run_stamp
from .swap import swap_in
from .validation import validate_post_encode


def run() -> int:
    cfg = load_config()

    prefix = (cfg.log_prefix + "_") if cfg.log_prefix else ""
    stamp = make_run_stamp()

    log_dir = cfg.work_root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    run_log = log_dir / f"{prefix}transcode_{stamp}.log"
    cand_log = log_dir / f"{prefix}candidates_{stamp}.log"

    logger = Logger(str(run_log))

    logger.log("===== TRANSCODE START =====")
    logger.log(f"FAIL_FAST={cfg.fail_fast}")
    logger.log(f"DRY_RUN={cfg.dry_run}")

    lock_path = cfg.work_root / f"{prefix}chonkreducer.lock"
    if not acquire_lock(lock_path, cfg.lock_stale_hours, logger):
        return 0

    processed = failed = considered = skipped_marker = skipped_backup = 0

    try:
        cleanup_work_dir(cfg.work_root, cfg.work_cleanup_hours, logger)
        cleanup_media_temp(cfg.media_root, cfg.work_cleanup_hours, cfg.exclude_path_parts, logger)
        cleanup_logs(log_dir, cfg.log_retention_days, logger)
        cleanup_baks(cfg.media_root, cfg.bak_retention_days, logger)

        # UPDATED: discovery now returns ignored info
        cands, ignored_folders = gather_candidates(cfg, logger)
        logger.log(f"Found {len(cands)} candidates")

        with open(cand_log, "w", encoding="utf-8", newline="\n") as f:
            for p in cands:
                f.write(str(p) + "\n")

        for src in cands:
            if processed >= cfg.max_files:
                break

            if src.with_suffix(src.suffix + ".optimized").exists():
                skipped_marker += 1
                continue

            if list(src.parent.glob(src.name + ".bak.*")):
                skipped_backup += 1
                continue

            considered += 1
            logger.log(f"Processing: {src}")
            
            if cfg.dry_run:
                logger.log(f"DRY_RUN: would encode + swap: {src}")
                processed += 1
                continue
            
            stamp2 = make_run_stamp()
            encoded = src.parent / f"{src.name}.{stamp2}.encoded.mkv"

            try:
                encode_qsv(src, encoded, cfg, logger)

                if not validate_post_encode(encoded, cfg, logger):
                    raise RuntimeError("Post-encode validation failed")

                swap_in(src, encoded, cfg, logger)

                logger.log(f"OK: swapped + marked: {src}")
                processed += 1

            except Exception as e:
                logger.log(f"FAILED: {src} ({e})")
                failed += 1

                try:
                    encoded.unlink(missing_ok=True)
                except Exception:
                    pass

                if cfg.fail_fast:
                    logger.log("FAIL_FAST enabled — exiting immediately.")
                    return 1

        logger.log("===== SUMMARY =====")
        logger.log(f"Processed: {processed}")
        logger.log(f"Failed:    {failed}")
        logger.log(f"Ignored folders: {len(ignored_folders)}")
        logger.log("===== END =====")

        return 0 if failed == 0 else 2

    finally:
        release_lock(lock_path, logger)