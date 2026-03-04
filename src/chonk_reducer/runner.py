from __future__ import annotations

from pathlib import Path
import sys
import time
import uuid

from .cleanup import cleanup_baks, cleanup_logs, cleanup_work_dir, cleanup_media_temp
from .config import load_config
from .discovery import gather_candidates
from .encode import encode_qsv
from .lock import acquire_lock, release_lock
from .logging_utils import Logger, make_run_stamp
from .swap import swap_in
from .validation import validate_post_encode
from .ffmpeg_utils import probe_video_stream
from .skip_policy import evaluate_skip
from .stats import record_success, record_failure, record_dry_run
from . import __version__ as PKG_VERSION


def _fmt_hms(seconds: float) -> str:
    s = int(seconds)
    hh = s // 3600
    mm = (s % 3600) // 60
    ss = s % 60
    return f"{hh:02d}:{mm:02d}:{ss:02d}"


def _validate_config(cfg, logger: Logger) -> bool:
    errors = []

    def add(msg: str):
        errors.append(msg)

    if cfg.max_files <= 0:
        add("MAX_FILES must be >= 1")
    if cfg.min_size_gb < 0:
        add("MIN_SIZE_GB must be >= 0")
    if cfg.min_savings_percent < 0 or cfg.min_savings_percent > 100:
        add("MIN_SAVINGS_PERCENT must be between 0 and 100")
    if cfg.qsv_quality <= 0 or cfg.qsv_quality > 51:
        add("QSV_QUALITY must be between 1 and 51")
    if cfg.qsv_preset <= 0 or cfg.qsv_preset > 9:
        add("QSV_PRESET must be between 1 and 9")
    if cfg.validate_seconds <= 0:
        add("VALIDATE_SECONDS must be >= 1")
    if cfg.top_candidates < 0:
        add("TOP_CANDIDATES must be >= 0")
    if cfg.retry_count < 0:
        add("RETRY_COUNT must be >= 0")
    if cfg.retry_backoff_secs < 0:
        add("RETRY_BACKOFF_SECS must be >= 0")

    if errors:
        logger.log("===== CONFIG VALIDATION FAILED =====")
        for e in errors:
            logger.log(f"CONFIG ERROR: {e}")
        logger.log("===================================")
        return False
    return True



def run() -> int:
    cfg = load_config()

    prefix = (cfg.log_prefix + "_") if cfg.log_prefix else ""
    stamp = make_run_stamp()
    run_id = uuid.uuid4().hex[:8]

    run_start = time.monotonic()

    log_dir = cfg.work_root / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    run_log = log_dir / f"{prefix}transcode_{stamp}.log"
    cand_log = log_dir / f"{prefix}candidates_{stamp}.log"

    logger = Logger(str(run_log))

    # Start banner (keep it verbose like your current logs)
    logger.log("===== TRANSCODE START =====")
    mode = "LIVE"
    if cfg.preview:
        mode = "PREVIEW"
    if cfg.dry_run:
        mode = "DRY_RUN"
    logger.log(f"RUN_ID={run_id} MODE={mode}")
    logger.log(f"VERSION={PKG_VERSION}")
    logger.log(f"STATS_ENABLED={getattr(cfg, 'stats_enabled', False)}")
    logger.log(f"STATS_PATH={getattr(cfg, 'stats_path', '')}")
    logger.log(f"MEDIA_ROOT={cfg.media_root}")
    logger.log(f"WORK_ROOT={cfg.work_root}")
    logger.log(f"MIN_SIZE_GB={cfg.min_size_gb} MAX_FILES={cfg.max_files}")
    logger.log(f"MIN_SAVINGS_PERCENT={cfg.min_savings_percent}")
    logger.log(f"QSV_QUALITY={cfg.qsv_quality} QSV_PRESET={cfg.qsv_preset}")
    logger.log(
        f"POST_ENCODE_VALIDATE={1 if cfg.post_encode_validate else 0} "
        f"VALIDATE_MODE={cfg.validate_mode} VALIDATE_SECONDS={cfg.validate_seconds}"
    )
    logger.log(
        f"OUT_UID={cfg.out_uid} OUT_GID={cfg.out_gid} "
        f"OUT_MODE={oct(cfg.out_mode)} OUT_DIR_MODE={oct(cfg.out_dir_mode)}"
    )
    logger.log(f"EXCLUDE_PATH_PARTS={','.join(cfg.exclude_path_parts)}")
    logger.log(f"FAIL_FAST={cfg.fail_fast}")
    logger.log(f"DRY_RUN={cfg.dry_run}")
    logger.log(f"LOG_SKIPS={cfg.log_skips}")
    logger.log(f"TOP_CANDIDATES={cfg.top_candidates}")
    logger.log(f"RETRY_COUNT={cfg.retry_count} RETRY_BACKOFF_SECS={cfg.retry_backoff_secs}")
    logger.log(f"PREVIEW={cfg.preview}")
    logger.log(f"SKIP_CODECS={','.join(getattr(cfg,'skip_codecs',()))}")
    logger.log(f"SKIP_MIN_HEIGHT={getattr(cfg,'skip_min_height',0)}")
    logger.log(f"SKIP_RESOLUTION_TAGS={','.join(getattr(cfg,'skip_resolution_tags',()))}")
    logger.log(f"Run log: {run_log}")

    if not _validate_config(cfg, logger):
        run_duration = time.monotonic() - run_start
        logger.log(f"RUN DURATION: {_fmt_hms(run_duration)}")
        logger.log("===== END =====")
        return 1

    # Global pause (no cleanup/discovery/processing)
    pause_file = cfg.media_root / ".chonkpause"
    if pause_file.exists():
        mode = "PAUSED"
        reason_file = cfg.media_root / ".chonkpause.reason"
        if reason_file.exists():
            try:
                reason = reason_file.read_text(encoding="utf-8").strip()
            except Exception:
                reason = ""
            if reason:
                logger.log(f"PAUSE reason: {reason}")
        logger.log(f"PAUSE detected at {pause_file} — exiting without processing.")
        run_duration = time.monotonic() - run_start
        logger.log(f"RUN DURATION: {_fmt_hms(run_duration)}")
        logger.log("===== END =====")
        return 0

    lock_path = cfg.work_root / f"{prefix}chonkreducer.lock"
    if not acquire_lock(lock_path, cfg.lock_stale_hours, logger):
        return 0

    done = 0
    processed = 0
    failed = 0
    considered = 0
    skipped_marker = 0
    skipped_backup = 0
    skipped_min_savings = 0
    skipped_codec = 0
    skipped_resolution = 0

    bytes_before_total = 0
    bytes_after_total = 0
    elapsed_total = 0.0

    # define for summary even if discovery fails early
    cands: list[Path] = []
    ignored_folders = {}
    marked_failed: list[Path] = []

    show_stats = {}  # show -> {files,before,after,elapsed}

    try:
        # Cleanup first
        cleanup_work_dir(cfg.work_root, cfg.work_cleanup_hours, logger)
        cleanup_media_temp(cfg.media_root, cfg.work_cleanup_hours, cfg.exclude_path_parts, logger)
        cleanup_logs(log_dir, cfg.log_retention_days, logger)
        cleanup_baks(cfg.media_root, cfg.bak_retention_days, logger)

        # Discovery (returns candidates + ignored folder counts)
        cands, ignored_folders = gather_candidates(cfg, logger)
        logger.log(f"Found {len(cands)} candidates")

        # Log top candidates by size (quick sanity)
        if cfg.top_candidates and cands:
            logger.log("===== TOP CANDIDATES =====")
            for p in cands[: max(0, int(cfg.top_candidates))]:
                try:
                    sz = p.stat().st_size
                    logger.log(f"{sz/1024**3:.2f}GB  {p}")
                except Exception:
                    logger.log(f"?GB  {p}")
            logger.log("==========================")

        # Log candidates list to file
        with open(cand_log, "w", encoding="utf-8", newline="\n") as f:
            for p in cands:
                f.write(str(p) + "\n")

        for src in cands:
            if done >= cfg.max_files:
                break

            # Skip if already optimized
            if src.with_suffix(src.suffix + ".optimized").exists():
                skipped_marker += 1
                if cfg.log_skips:
                    logger.log(f"SKIP(marker): {src}")
                continue

            # Skip if backup exists for same filename in folder
            if list(src.parent.glob(src.name + ".bak.*")):
                skipped_backup += 1
                if cfg.log_skips:
                    logger.log(f"SKIP(backup): {src}")
                continue

            considered += 1
            logger.log(f"Processing: {src}")

            try:
                before_bytes = src.stat().st_size
            except Exception:
                before_bytes = 0

            # DRY RUN: don’t encode/swap/validate, just log intent
            if cfg.dry_run:
                if before_bytes:
                    logger.log(
                        f"DRY_RUN: would encode + swap: {src} "
                        f"(size={before_bytes/1024**3:.2f}GB)"
                    )
                else:
                    logger.log(f"DRY_RUN: would encode + swap: {src}")

                # Optional stats entry for dry run
                record_dry_run(cfg, logger, run_id, src, before_bytes)

                processed += 1
                done += 1
                break


            stamp2 = make_run_stamp()
            encoded = src.parent / f"{src.name}.{stamp2}.encoded.mkv"

            before_probe = None
            try:
                before_probe = probe_video_stream(
                    src,
                    cfg.ffprobe_analyzeduration,
                    cfg.ffprobe_probesize,
                    logger,
                    timeout=cfg.probe_timeout_secs,
                )
            except Exception as e:
                logger.log(f"Probe (before) failed: {e}")

            # Pre-encode skip evaluation (codec/resolution policies)
            skip = evaluate_skip(src, before_probe, cfg)
            if skip:
                cat, reason = skip
                if cat == 'codec':
                    skipped_codec += 1
                elif cat == 'resolution':
                    skipped_resolution += 1
                if cfg.log_skips:
                    logger.log(f"SKIP({cat}): {reason} :: {src}")
                continue

            attempt_errors: list[str] = []
            for attempt in range(cfg.retry_count + 1):
                if attempt > 0:
                    logger.log(f"RETRY {attempt}/{cfg.retry_count}: {src}")
                    if cfg.retry_backoff_secs:
                        time.sleep(cfg.retry_backoff_secs * attempt)

                t0 = time.monotonic()
                try:
                    stage = "encode"
                    encode_qsv(src, encoded, cfg, logger)

                    stage = "validate"
                    if not validate_post_encode(encoded, cfg, logger):
                        raise RuntimeError("Post-encode validation failed")

                    # Min savings guard (skip swaps that are not worth it)
                    try:
                        encoded_bytes = encoded.stat().st_size
                    except Exception:
                        encoded_bytes = 0
                    if cfg.min_savings_percent and before_bytes and encoded_bytes:
                        saved_tmp = before_bytes - encoded_bytes
                        pct_tmp = (saved_tmp / before_bytes) * 100.0 if before_bytes > 0 else 0.0
                        if pct_tmp < cfg.min_savings_percent:
                            logger.log(
                                f"SKIP: savings {pct_tmp:.1f}% < MIN_SAVINGS_PERCENT {cfg.min_savings_percent:.1f}% "
                                f"(before={before_bytes/1024**3:.2f}GB encoded={encoded_bytes/1024**3:.2f}GB)"
                            )
                            try:
                                encoded.unlink(missing_ok=True)
                            except Exception:
                                pass
                            skipped_min_savings += 1
                            done += 1
                            break

                    stage = "swap"
                    bak_path, marker_path = swap_in(src, encoded, cfg, logger)

                    after_probe = None
                    try:
                        after_probe = probe_video_stream(
                            src,
                            cfg.ffprobe_analyzeduration,
                            cfg.ffprobe_probesize,
                            logger,
                            timeout=cfg.probe_timeout_secs,
                        )
                    except Exception as e:
                        logger.log(f"Probe (after) failed: {e}")

                    if before_probe and after_probe:
                        def _mbps(br):
                            return (br / 1_000_000.0) if br else None
                        b = before_probe
                        a = after_probe
                        b_br = _mbps(b.get('bit_rate'))
                        a_br = _mbps(a.get('bit_rate'))
                        b_br_s = f"{b_br:.1f}Mbps" if b_br is not None else "?Mbps"
                        a_br_s = f"{a_br:.1f}Mbps" if a_br is not None else "?Mbps"
                        logger.log(
                            f"VIDEO: {b.get('codec')} {b.get('width')}x{b.get('height')} {b_br_s} "
                            f"→ {a.get('codec')} {a.get('width')}x{a.get('height')} {a_br_s}"
                        )

                    # Post-swap metrics (src now points to the new file at original path)
                    try:
                        after_bytes = src.stat().st_size
                    except Exception:
                        after_bytes = 0

                    elapsed = time.monotonic() - t0
                    elapsed_total += elapsed

                    # Per-show aggregation
                    try:
                        rel = src.relative_to(cfg.media_root)
                        show = rel.parts[0] if rel.parts else src.parent.name
                    except Exception:
                        show = src.parent.name
                    st = show_stats.get(show)
                    if not st:
                        st = {"files": 0, "before": 0, "after": 0, "elapsed": 0.0}
                        show_stats[show] = st
                    st["files"] += 1
                    st["before"] += int(before_bytes or 0)
                    st["after"] += int(after_bytes or 0)
                    st["elapsed"] += float(elapsed)

                    if before_bytes:
                        bytes_before_total += before_bytes
                    if after_bytes:
                        bytes_after_total += after_bytes

                    if before_bytes and after_bytes:
                        saved = before_bytes - after_bytes
                        pct = (saved / before_bytes) * 100.0 if before_bytes > 0 else 0.0
                        logger.log(
                            f"METRICS: before={before_bytes/1024**3:.2f}GB "
                            f"after={after_bytes/1024**3:.2f}GB "
                            f"saved={saved/1024**3:.2f}GB ({pct:.1f}%) "
                            f"elapsed={_fmt_hms(elapsed)} "
                            f"rate={(before_bytes/1024**2)/(elapsed/60.0):.1f}MB/min"
                        )
                    else:
                        logger.log(f"METRICS: elapsed={_fmt_hms(elapsed)}")

                    logger.log(f"OK: swapped + marked: {src}")
                    # Stats (success) - append after marker write
                    record_success(
                        cfg,
                        logger,
                        run_id=run_id,
                        mode=mode.lower(),
                        stage="swap",
                        src=src,
                        before_bytes=int(before_bytes or 0),
                        after_bytes=int(after_bytes or 0),
                        codec_from=(before_probe.get("codec") if before_probe else None),
                        codec_to=(after_probe.get("codec") if after_probe else None),
                        duration_seconds=float(elapsed),
                        bak_path=bak_path,
                    )
                    processed += 1
                    done += 1
                    break

                except Exception as e:
                    attempt_errors.append(str(e))
                    logger.log(f"FAILED (attempt {attempt+1}/{cfg.retry_count+1}): {src} ({e})")

                    # Best-effort cleanup of temp encoded file
                    try:
                        encoded.unlink(missing_ok=True)
                    except Exception:
                        pass

                    # If we have more retries, continue
                    if attempt < cfg.retry_count:
                        continue

                    # Final failure: record stats + mark file as failed/quarantined
                    record_failure(
                        cfg,
                        logger,
                        run_id=run_id,
                        mode=mode.lower(),
                        stage=stage if "stage" in locals() else "unknown",
                        src=src,
                        before_bytes=int(before_bytes or 0),
                        duration_seconds=float(time.monotonic() - t0),
                        err=e,
                        encoded_path=encoded,
                    )
                    # Final failure: mark file as failed/quarantined
                    failed += 1
                    fail_marker = src.with_suffix(src.suffix + ".failed")
                    try:
                        msg = "\n".join(attempt_errors[-5:])
                        fail_marker.write_text(f"FAILED {make_run_stamp()}\n{msg}\n", encoding="utf-8")
                        logger.log(f"MARKED FAILED: {src} -> {fail_marker}")
                        marked_failed.append(src)
                    except Exception as me:
                        logger.log(f"FAILED to write marker for {src}: {me}")
                    if cfg.fail_fast:
                        logger.log("FAIL_FAST enabled — exiting immediately.")
                        return 1

                    # Count toward MAX_FILES so we don't run forever
                    done += 1

        # Summary
        logger.log("===== SUMMARY =====")
        ignored_files = sum(ignored_folders.values()) if ignored_folders else 0
        logger.log(f"Candidates found:     {len(cands)}")
        logger.log(f"Considered:           {considered}")
        logger.log(f"Processed:            {processed}")
        logger.log(f"Skipped (marker):     {skipped_marker}")
        logger.log(f"Skipped (backup):     {skipped_backup}")
        logger.log(f"Skipped (codec):      {skipped_codec}")
        logger.log(f"Skipped (resolution): {skipped_resolution}")
        logger.log(f"Skipped (min savings): {skipped_min_savings}")
        logger.log(f"Failed:               {failed}")
        logger.log(f"Ignored folders:      {len(ignored_folders)}")
        logger.log(f"Ignored files:        {ignored_files}")

        if marked_failed:
            logger.log("===== FAILED FILES MARKED (.failed) =====")
            for p in marked_failed:
                logger.log(f"FAILED: {p}")
            logger.log("========================================")

        if bytes_before_total:
            saved_total = bytes_before_total - bytes_after_total
            pct_total = (saved_total / bytes_before_total) * 100.0 if bytes_before_total > 0 else 0.0
            logger.log(f"TOTAL BEFORE (run):    {bytes_before_total/1024**3:.2f}GB")
            logger.log(f"TOTAL AFTER (run):     {bytes_after_total/1024**3:.2f}GB")
            logger.log(f"TOTAL SAVED (run):     {saved_total/1024**3:.2f}GB")
            logger.log(f"TOTAL SAVED PCT (run): {pct_total:.1f}%")

        if elapsed_total:
            logger.log(f"TOTAL TIME: {_fmt_hms(elapsed_total)}")
            if bytes_before_total:
                logger.log(f"TOTAL RATE: {(bytes_before_total/1024**2)/(elapsed_total/60.0):.1f}MB/min")

        if show_stats:
            logger.log("===== PER-SHOW SAVINGS =====")
            for show, st in sorted(show_stats.items(), key=lambda kv: kv[1].get('before', 0), reverse=True):
                b = st.get('before', 0)
                a = st.get('after', 0)
                saved = b - a
                pct = (saved / b) * 100.0 if b else 0.0
                logger.log(
                    f"SHOW TOTAL: {show} files={st.get('files',0)} "
                    f"saved={saved/1024**3:.2f}GB ({pct:.1f}%) elapsed={_fmt_hms(st.get('elapsed',0.0))}"
                )
            logger.log("============================")

        run_duration = time.monotonic() - run_start
        logger.log(f"RUN DURATION: {_fmt_hms(run_duration)}")
        logger.log("===== END =====")
        return 0 if failed == 0 else 2

    finally:
        release_lock(lock_path, logger)
