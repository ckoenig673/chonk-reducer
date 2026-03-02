from __future__ import annotations

from pathlib import Path

from .cleanup import cleanup_baks, cleanup_logs, cleanup_work_dir
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
    logger.log(f"MEDIA_ROOT={cfg.media_root}")
    logger.log(f"WORK_ROOT={cfg.work_root}")
    logger.log(f"MIN_SIZE_GB={cfg.min_size_gb} MAX_FILES={cfg.max_files}")
    logger.log(f"QSV_QUALITY={cfg.qsv_quality} QSV_PRESET={cfg.qsv_preset}")
    logger.log(f"POST_ENCODE_VALIDATE={1 if cfg.post_encode_validate else 0} VALIDATE_MODE={cfg.validate_mode} VALIDATE_SECONDS={cfg.validate_seconds}")
    logger.log(f"OUT_UID={cfg.out_uid} OUT_GID={cfg.out_gid} OUT_MODE=0o{cfg.out_mode:o} OUT_DIR_MODE=0o{cfg.out_dir_mode:o}")
    logger.log(f"EXCLUDE_PATH_PARTS={','.join(cfg.exclude_path_parts)}")
    logger.log(f"Run log: {run_log}")

    lock_path = cfg.work_root / f"{prefix}chonkreducer.lock"
    if not acquire_lock(lock_path, cfg.lock_stale_hours, logger):
        return 0

    processed = failed = considered = skipped_marker = skipped_backup = 0

    try:
        cleanup_work_dir(cfg.work_root, cfg.work_cleanup_hours, logger)
        cleanup_logs(log_dir, cfg.log_retention_days, logger)
        cleanup_baks(cfg.media_root, cfg.bak_retention_days, logger)

        cands = gather_candidates(cfg)
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

            stamp2 = make_run_stamp()
            encoded = cfg.work_root / f"{src.name}.{stamp2}.encoded.mkv"

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

        logger.log("===== SUMMARY =====")
        logger.log(f"Candidates found:     {len(cands)}")
        logger.log(f"Considered:           {considered}")
        logger.log(f"Processed:            {processed}")
        logger.log(f"Skipped (marker):     {skipped_marker}")
        logger.log(f"Skipped (backup):     {skipped_backup}")
        logger.log(f"Failed:               {failed}")
        logger.log("===== END =====")

        return 0 if failed == 0 else 2

    finally:
        release_lock(lock_path, logger)
