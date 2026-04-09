from __future__ import annotations

from pathlib import Path
import json
import logging
import sys
import time
import uuid
import shutil
from typing import Callable, Optional

from ..cleanup import cleanup_baks, cleanup_logs, cleanup_work_dir, cleanup_media_temp
from ..config import load_config
from ..discovery import gather_candidates
from .encode import encode_qsv
from ..core.lock import acquire_lock, release_lock
from ..core.logging_utils import Logger, make_run_stamp
from .swap import swap_in
from .validation import validate_post_encode
from .ffmpeg_utils import probe_video_stream
from .candidate_scoring import build_candidate_score_inputs, calculate_candidate_score
from .run_budget import RunBudgetType
from ..services.history_summaries import get_history_summaries
from ..skip_policy import evaluate_skip
from ..stats import ensure_database, record_success, record_failure, record_dry_run, record_skip, record_run_counters, record_run_log_path, get_policy_skip_cache, upsert_policy_skip_cache, delete_policy_skip_cache


LOGGER = logging.getLogger("chonk_reducer.runner")


def _fmt_hms(seconds: float) -> str:
    s = int(seconds)
    hh = s // 3600
    mm = (s % 3600) // 60
    ss = s % 60
    return f"{hh:02d}:{mm:02d}:{ss:02d}"


def _display_name(src: Path) -> str:
    return src.stem or src.name


def _rank_candidates_by_score(cfg, cands: list[Path], *, cached_max_savings_by_path: dict[Path, float | None] | None = None):
    """
    Rank candidates by descending score while preserving deterministic tie behavior.

    Tie-breaker strategy:
    1) keep the existing discovery order (already deterministic from prior logic)
    2) path string as a final deterministic fallback
    """
    cache_lookup = cached_max_savings_by_path or {}
    ranked_rows: list[tuple[float, int, str, Path, tuple[str, ...], str, bool, float]] = []

    for idx, src in enumerate(cands):
        try:
            file_size_bytes = int(src.stat().st_size)
        except Exception:
            file_size_bytes = 0
        score_inputs = build_candidate_score_inputs(
            cfg=cfg,
            src=src,
            file_size_bytes=file_size_bytes,
            cached_max_savings_percent=cache_lookup.get(src),
        )
        score_result = calculate_candidate_score(score_inputs)
        ranked_rows.append(
            (
                float(score_result.score),
                idx,
                str(src),
                src,
                tuple(score_result.reasons),
                str(getattr(score_result, "confidence_label", "medium") or "medium"),
                bool(getattr(score_result, "history_influenced", False)),
                float(getattr(score_result, "confidence_adjustment_points", 0.0) or 0.0),
            )
        )

    ranked_rows.sort(key=lambda row: (-row[0], row[1], row[2]))
    sorted_candidates = [row[3] for row in ranked_rows]
    ranking_meta = {
        row[3]: {
            "score": row[0],
            "reasons": row[4],
            "confidence_label": row[5],
            "history_influenced": row[6],
            "confidence_adjustment_points": row[7],
        }
        for row in ranked_rows
    }
    return sorted_candidates, ranking_meta


def _preview_score_band(score: float) -> str:
    score_value = max(0.0, float(score or 0.0))
    if score_value >= 70.0:
        return "High value"
    if score_value >= 30.0:
        return "Medium value"
    return "Low confidence"


def _resolution_bucket_for_candidate(src: Path, before_probe: dict | None) -> str:
    if before_probe:
        try:
            height = int(before_probe.get("height")) if before_probe.get("height") is not None else 0
        except Exception:
            height = 0
        if height >= 4320:
            return "4320p+"
        if height >= 2160:
            return "2160p"
        if height >= 1440:
            return "1440p"
        if height >= 1080:
            return "1080p"
        if height >= 720:
            return "720p"
        if height >= 576:
            return "576p"
        if height >= 480:
            return "480p"

    text = src.name.lower()
    if "4320" in text or "8k" in text:
        return "4320p+"
    if "2160" in text or "4k" in text:
        return "2160p"
    if "1440" in text:
        return "1440p"
    if "1080" in text:
        return "1080p"
    if "720" in text:
        return "720p"
    if "576" in text:
        return "576p"
    if "480" in text:
        return "480p"
    return "unknown"


def _select_historical_signal(
    *,
    history_summaries: dict[str, object] | None,
    src: Path,
    before_probe: dict | None,
    library_name: str,
) -> tuple[float | None, str | None]:
    if not history_summaries:
        return None, None

    by_codec = {
        str(row.get("codec", "")).strip().lower(): float(row.get("avg_savings_pct"))
        for row in (history_summaries.get("by_codec") or [])
        if row.get("codec") and row.get("avg_savings_pct") is not None
    }
    by_resolution = {
        str(row.get("resolution_bucket", "")).strip().lower(): float(row.get("avg_savings_pct"))
        for row in (history_summaries.get("by_resolution_bucket") or [])
        if row.get("resolution_bucket") and row.get("avg_savings_pct") is not None
    }
    by_library = {
        str(row.get("library", "")).strip().lower(): float(row.get("avg_savings_pct"))
        for row in (history_summaries.get("by_library") or [])
        if row.get("library") and row.get("avg_savings_pct") is not None
    }

    codec = str((before_probe or {}).get("codec") or "").strip().lower()
    if codec and codec in by_codec:
        return by_codec[codec], f"codec:{codec}"

    resolution_bucket = _resolution_bucket_for_candidate(src, before_probe)
    if resolution_bucket in by_resolution:
        return by_resolution[resolution_bucket], f"resolution:{resolution_bucket}"

    library_key = str(library_name or "").strip().lower()
    if library_key and library_key in by_library:
        return by_library[library_key], f"library:{library_key}"

    return None, None


def _effective_max_files(cfg) -> int:
    budget = getattr(cfg, "run_budget", None)
    if budget is not None and hasattr(budget, "max_files_limit"):
        return int(budget.max_files_limit(fallback_max_files=getattr(cfg, "max_files", 1)))
    return max(1, int(getattr(cfg, "max_files", 1) or 1))


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

    if getattr(cfg, "max_savings_percent", 0) and (cfg.max_savings_percent < 0 or cfg.max_savings_percent > 100):
        add("MAX_SAVINGS_PERCENT must be between 0 and 100")
    if getattr(cfg, "max_savings_percent", 0) and cfg.min_savings_percent and cfg.max_savings_percent and cfg.max_savings_percent < cfg.min_savings_percent:
        add("MAX_SAVINGS_PERCENT must be >= MIN_SAVINGS_PERCENT")
    if getattr(cfg, "min_media_free_gb", 0) < 0:
        add("MIN_MEDIA_FREE_GB must be >= 0")
    if getattr(cfg, "max_gb_per_run", 0) < 0:
        add("MAX_GB_PER_RUN must be >= 0")
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
    if cfg.retry_backoff_seconds < 0:
        add("RETRY_BACKOFF_SECONDS must be >= 0")

    if errors:
        logger.log("===== CONFIG VALIDATION FAILED =====")
        for e in errors:
            logger.log(f"CONFIG ERROR: {e}")
        logger.log("===================================")
        return False
    return True


def _estimate_size_bytes(before_bytes: int, cfg, probe: dict | None) -> int:
    """Estimate encoded size using existing encode settings (heuristic)."""
    if before_bytes <= 0:
        return 0

    # Lower quality values typically preserve more detail (larger output).
    quality = int(getattr(cfg, "qsv_quality", 21) or 21)
    preset = int(getattr(cfg, "qsv_preset", 7) or 7)
    ratio = 0.62 + ((21 - quality) * 0.01) + ((preset - 7) * 0.015)

    bit_rate = (probe or {}).get("bit_rate")
    try:
        bit_rate_i = int(bit_rate) if bit_rate is not None else 0
    except Exception:
        bit_rate_i = 0
    if bit_rate_i >= 8_000_000:
        ratio -= 0.05

    ratio = max(0.25, min(0.95, ratio))
    return max(1, int(before_bytes * ratio))


def _estimate_candidate_savings_bytes_for_budget(cfg, src: Path, logger: Logger) -> int | None:
    """Estimate candidate savings bytes for budget-mode selection."""
    try:
        before_bytes = int(src.stat().st_size)
    except Exception:
        return None
    if before_bytes <= 0:
        return None

    before_probe = None
    try:
        before_probe = probe_video_stream(
            src,
            cfg.ffprobe_analyzeduration,
            cfg.ffprobe_probesize,
            logger,
            timeout=cfg.probe_timeout_secs,
        )
    except Exception:
        before_probe = None

    estimated_encoded_bytes = _estimate_size_bytes(before_bytes, cfg, before_probe)
    savings_bytes = int(before_bytes) - int(estimated_encoded_bytes or 0)
    if savings_bytes <= 0:
        return None
    return savings_bytes


def _apply_estimated_savings_budget_selection(
    cfg,
    cands: list[Path],
    logger: Logger,
    selection_meta: dict[Path, dict[str, object]] | None = None,
) -> list[Path]:
    """Select ranked candidates until cumulative estimated savings meets/exceeds budget."""
    run_budget = getattr(cfg, "run_budget", None)
    if run_budget is None or getattr(run_budget, "budget_type", None) is not RunBudgetType.ESTIMATED_SAVINGS_BYTES:
        return cands

    budget_bytes = run_budget.estimated_savings_bytes_limit() if hasattr(run_budget, "estimated_savings_bytes_limit") else None
    if budget_bytes is None:
        logger.log("RUN_BUDGET(estimated_savings_bytes): invalid budget value; using ranked candidate list unchanged.")
        return cands

    selected: list[Path] = []
    excluded: list[tuple[Path, str]] = []
    cumulative = 0
    cut_line_marked = False

    logger.log(
        "RUN_BUDGET(estimated_savings_bytes): target=%d bytes (%.2f GiB)"
        % (int(budget_bytes), float(budget_bytes) / float(1024 ** 3))
    )
    for src in cands:
        cumulative_before = int(cumulative)
        estimated_savings_bytes = _estimate_candidate_savings_bytes_for_budget(cfg, src, logger)
        if estimated_savings_bytes is None:
            excluded.append((src, "missing_estimated_savings"))
            if selection_meta is not None:
                selection_meta[src] = {
                    "included_by_budget": False,
                    "budget_status": "excluded_missing_estimate",
                    "budget_reason": "excluded due to missing estimated savings",
                    "estimated_savings_bytes": None,
                    "cumulative_estimated_savings_bytes": cumulative_before,
                    "budget_target_bytes": int(budget_bytes),
                }
            continue
        if cumulative >= budget_bytes:
            excluded.append((src, "below_cut_line"))
            if selection_meta is not None:
                budget_status = "excluded_budget_limit"
                budget_reason = "excluded due to budget limit"
                if not cut_line_marked:
                    budget_status = "excluded_budget_cut_line"
                    cut_line_marked = True
                selection_meta[src] = {
                    "included_by_budget": False,
                    "budget_status": budget_status,
                    "budget_reason": budget_reason,
                    "estimated_savings_bytes": int(estimated_savings_bytes),
                    "cumulative_estimated_savings_bytes": cumulative_before,
                    "budget_target_bytes": int(budget_bytes),
                }
            continue
        selected.append(src)
        cumulative += int(estimated_savings_bytes)
        if selection_meta is not None:
            selection_meta[src] = {
                "included_by_budget": True,
                "budget_status": "selected_by_budget",
                "budget_reason": "selected by budget",
                "estimated_savings_bytes": int(estimated_savings_bytes),
                "cumulative_estimated_savings_bytes": int(cumulative),
                "budget_target_bytes": int(budget_bytes),
            }
        logger.log(
            "RUN_BUDGET include savings=%d cumulative=%d/%d :: %s"
            % (estimated_savings_bytes, cumulative, budget_bytes, src)
        )

    logger.log(
        "RUN_BUDGET selected=%d excluded=%d cumulative=%d/%d"
        % (len(selected), len(excluded), cumulative, budget_bytes)
    )
    for src, reason in excluded[:10]:
        meta = selection_meta.get(src, {}) if selection_meta is not None else {}
        logger.log(
            "RUN_BUDGET exclude(%s) cumulative=%d/%d :: %s"
            % (
                reason,
                int(meta.get("cumulative_estimated_savings_bytes", cumulative)),
                int(meta.get("budget_target_bytes", budget_bytes)),
                src,
            )
        )
    if len(excluded) > 10:
        logger.log(f"RUN_BUDGET ...and {len(excluded) - 10} more excluded candidates")

    return selected


def _apply_score_cutoff_budget_selection(
    cfg,
    cands: list[Path],
    ranking_meta: dict[Path, dict[str, object]],
    logger: Logger,
    selection_meta: dict[Path, dict[str, object]] | None = None,
) -> list[Path]:
    """Select ranked candidates whose score is at or above the configured cutoff."""
    run_budget = getattr(cfg, "run_budget", None)
    if run_budget is None or getattr(run_budget, "budget_type", None) is not RunBudgetType.SCORE_CUTOFF:
        return cands

    cutoff = run_budget.score_cutoff_value() if hasattr(run_budget, "score_cutoff_value") else None
    if cutoff is None:
        logger.log("RUN_BUDGET(score_cutoff): invalid budget value; using ranked candidate list unchanged.")
        return cands

    selected: list[Path] = []
    excluded: list[Path] = []

    logger.log("RUN_BUDGET(score_cutoff): min_score=%.3f" % float(cutoff))
    for src in cands:
        rank_row = ranking_meta.get(src, {})
        score_value = float(rank_row.get("score", 0.0) or 0.0)
        if score_value >= cutoff:
            selected.append(src)
            if selection_meta is not None:
                selection_meta[src] = {
                    "included_by_budget": True,
                    "budget_status": "selected_score_cutoff",
                    "budget_reason": "included by score cutoff",
                }
            logger.log("RUN_BUDGET include score=%.3f cutoff=%.3f :: %s" % (score_value, cutoff, src))
            continue

        excluded.append(src)
        if selection_meta is not None:
            selection_meta[src] = {
                "included_by_budget": False,
                "budget_status": "excluded_score_cutoff",
                "budget_reason": "excluded due to score cutoff",
            }
        logger.log("RUN_BUDGET exclude(score_cutoff) score=%.3f cutoff=%.3f :: %s" % (score_value, cutoff, src))

    logger.log("RUN_BUDGET(score_cutoff) selected=%d excluded=%d cutoff=%.3f" % (len(selected), len(excluded), cutoff))
    return selected



def run(progress_callback=None, cancel_requested: Optional[Callable[[], bool]] = None, on_cancelled: Optional[Callable[[str], None]] = None) -> int:
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

    def _progress(**values):
        if not callable(progress_callback):
            return
        try:
            progress_callback(values)
        except Exception:
            pass

    # Start banner (keep it verbose like your current logs)
    logger.log("===== TRANSCODE START =====")
    mode = "LIVE"
    if cfg.preview:
        mode = "PREVIEW"
    if cfg.dry_run:
        mode = "DRY_RUN"
    record_run_log_path(cfg, logger, run_id=run_id, mode=mode.lower(), raw_log_path=run_log)
    logger.log(f"RUN_ID={run_id} MODE={mode}")
    logger.log(f"VERSION={cfg.version}")
    logger.log(f"STATS_ENABLED={getattr(cfg, 'stats_enabled', False)}")
    logger.log(f"STATS_PATH={getattr(cfg, 'stats_path', '')}")
    logger.log(f"MEDIA_ROOT={cfg.media_root}")
    logger.log(f"WORK_ROOT={cfg.work_root}")
    effective_max_files = _effective_max_files(cfg)
    run_budget = getattr(cfg, "run_budget", None)
    budget_type = getattr(run_budget, "budget_type", "max_files")
    logger.log(f"MIN_SIZE_GB={cfg.min_size_gb} MAX_FILES={effective_max_files} RUN_BUDGET_TYPE={budget_type}")
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
    logger.log(f"RETRY_COUNT={cfg.retry_count} RETRY_BACKOFF_SECONDS={cfg.retry_backoff_seconds}")
    logger.log(f"PREVIEW={cfg.preview}")
    logger.log(f"SKIP_CODECS={','.join(getattr(cfg,'skip_codecs',()))}")
    logger.log(f"SKIP_MIN_HEIGHT={getattr(cfg,'skip_min_height',0)}")
    logger.log(f"SKIP_RESOLUTION_TAGS={','.join(getattr(cfg,'skip_resolution_tags',()))}")
    logger.log(f"MIN_FILE_AGE_MINUTES={getattr(cfg,'min_file_age_minutes',0)}")
    logger.log(f"Run log: {run_log}")

    if not _validate_config(cfg, logger):
        run_duration = time.monotonic() - run_start
        logger.log(f"RUN DURATION: {_fmt_hms(run_duration)}")
        logger.log("===== END =====")
        return 1

    history_summaries: dict[str, object] | None = None
    if getattr(cfg, "stats_enabled", False):
        ensure_database(cfg, logger)
        history_summaries = get_history_summaries(cfg.stats_path)

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
    if not acquire_lock(lock_path, cfg.lock_stale_hours, cfg.lock_self_heal, logger):
        return 0

    done = 0
    evaluated = 0
    processed = 0  # files where an encode attempt occurred
    succeeded = 0
    failed = 0
    skipped_marker = 0
    skipped_backup = 0
    skipped_recent = 0
    skipped_min_savings = 0
    skipped_max_savings = 0
    skipped_codec = 0
    skipped_resolution = 0
    skipped_dry_run = 0
    prefiltered_policy_cache = 0

    bytes_before_total = 0
    bytes_after_total = 0
    saved_bytes_run = 0
    elapsed_total = 0.0

    # define for summary even if discovery fails early
    cands: list[Path] = []
    ignored_folders = {}
    recent_skipped: list[tuple[Path, int]] = []
    marked_failed: list[Path] = []
    ranking_meta: dict[Path, dict[str, object]] = {}

    show_stats = {}  # show -> {files,before,after,elapsed}
    cancelled = False
    cancel_recorded = False
    active_ffmpeg = None

    def _cancel_check(stage: str) -> bool:
        nonlocal cancelled
        if callable(cancel_requested) and cancel_requested():
            cancelled = True
            logger.log(f"Cancellation requested during {stage}; stopping run.")
            if callable(on_cancelled):
                try:
                    on_cancelled(stage)
                except Exception:
                    pass
            return True
        return False

    def _set_active_ffmpeg(proc) -> None:
        nonlocal active_ffmpeg
        active_ffmpeg = proc

    try:
        if _cancel_check("startup"):
            return 0

        # Cleanup first
        cleanup_work_dir(cfg.work_root, cfg.work_cleanup_hours, logger)
        cleanup_media_temp(cfg.media_root, cfg.work_cleanup_hours, cfg.exclude_path_parts, logger)
        cleanup_logs(log_dir, cfg.log_retention_days, logger)
        cleanup_baks(cfg.media_root, cfg.bak_retention_days, logger)


        # Free space guard (Story 42) - abort if MEDIA_ROOT volume is low on space
        if getattr(cfg, "min_media_free_gb", 0):
            try:
                free_bytes = shutil.disk_usage(str(cfg.media_root)).free
            except Exception:
                free_bytes = 0
            need_bytes = int(float(cfg.min_media_free_gb) * (1024 ** 3))
            if free_bytes and free_bytes < need_bytes:
                logger.log("===== FREE SPACE GUARD TRIGGERED =====")
                logger.log(f"MEDIA_ROOT free space: {free_bytes/1024**3:.2f} GB")
                logger.log(f"Required minimum: {float(cfg.min_media_free_gb):.2f} GB (MIN_MEDIA_FREE_GB)")
                logger.log("Aborting run to protect filesystem.")
                logger.log("====================================")
                run_duration = time.monotonic() - run_start
                logger.log(f"RUN DURATION: {_fmt_hms(run_duration)}")
                logger.log("===== END =====")
                return 2

        if _cancel_check("cleanup"):
            return 0

        # Discovery (returns candidates + ignored folder counts)
        cands, ignored_folders, recent_skipped = gather_candidates(cfg, logger)
        skipped_recent = len(recent_skipped)

        # Candidate pre-filter: skip cached max_savings policy rows that still apply.
        if getattr(cfg, "max_savings_percent", 0):
            threshold = float(cfg.max_savings_percent)
            filtered_candidates: list[Path] = []
            cached_threshold_hits: dict[Path, float | None] = {}
            for cand in cands:
                cached = get_policy_skip_cache(cfg, logger, src=cand, skip_reason="max_savings")
                if cached is None:
                    filtered_candidates.append(cand)
                    continue
                cached_pct = float(cached.get("savings_percent") or 0.0)
                if threshold <= cached_pct:
                    prefiltered_policy_cache += 1
                    LOGGER.info(
                        "Skipping cached policy decision: file='%s' reason='%s' stored_savings=%.1f%% threshold=%.1f%%",
                        _display_name(cand),
                        "max_savings",
                        cached_pct,
                        threshold,
                    )
                    if cfg.log_skips:
                        logger.log(
                            f"CANDIDATE SKIP(max_savings:cached): {cached_pct:.1f}% > {threshold:.1f}% :: {cand}"
                        )
                    continue
                # Threshold was raised; this cached policy no longer applies.
                delete_policy_skip_cache(cfg, logger, src=cand, skip_reason="max_savings")
                filtered_candidates.append(cand)
                cached_threshold_hits[cand] = cached_pct
            cands = filtered_candidates
        else:
            cached_threshold_hits = {}

        if cands:
            cands, ranking_meta = _rank_candidates_by_score(
                cfg,
                cands,
                cached_max_savings_by_path=cached_threshold_hits,
            )
            logger.log("===== CANDIDATE RANKING (SCORE) =====")
            for src in cands[: min(10, len(cands))]:
                rank_row = ranking_meta.get(src, {})
                rank_score = float(rank_row.get("score", 0.0))
                reasons = tuple(rank_row.get("reasons", ()))
                reasons_text = ", ".join(reasons[:2]) if reasons else "none"
                logger.log(f"RANK score={rank_score:.3f} reasons={reasons_text} :: {src}")
            if len(cands) > 10:
                logger.log(f"...and {len(cands) - 10} more ranked candidates")
            logger.log("=====================================")

        ranked_candidates = list(cands)
        budget_selection_meta: dict[Path, dict[str, object]] = {}
        cands = _apply_estimated_savings_budget_selection(
            cfg,
            cands,
            logger,
            selection_meta=budget_selection_meta,
        )
        cands = _apply_score_cutoff_budget_selection(
            cfg,
            cands,
            ranking_meta,
            logger,
            selection_meta=budget_selection_meta,
        )
        budget_excluded_candidates = [
            src
            for src in ranked_candidates
            if budget_selection_meta.get(src, {}).get("included_by_budget") is False
        ]

        logger.log(f"Found {len(cands)} candidates")
        _progress(candidates_found=len(cands), current_file="", files_evaluated=evaluated, files_processed=processed, success_count=succeeded, files_skipped=0, files_failed=failed, bytes_saved=saved_bytes_run)
        _progress(mode="Preview" if cfg.preview else "Live")

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

        budget_excluded_preview_results: list[dict[str, object]] = []
        if cfg.preview and budget_excluded_candidates:
            for src in budget_excluded_candidates:
                budget_meta = budget_selection_meta.get(src, {})
                rank_row = ranking_meta.get(src, {})
                rank_score = float(rank_row.get("score", 0.0))
                rank_reasons = tuple(rank_row.get("reasons", ()))
                try:
                    original_size = int(src.stat().st_size)
                except Exception:
                    original_size = 0
                estimated_savings_bytes = budget_meta.get("estimated_savings_bytes")
                estimated_size = int(original_size)
                if isinstance(estimated_savings_bytes, int) and estimated_savings_bytes > 0 and original_size > 0:
                    estimated_size = max(0, int(original_size) - int(estimated_savings_bytes))
                decision = "Skip (budget limit)"
                budget_status = str(budget_meta.get("budget_status", "")).strip()
                if budget_status == "excluded_missing_estimate":
                    decision = "Skip (missing estimated savings)"
                elif budget_status == "excluded_score_cutoff":
                    decision = "Skip (score cutoff)"
                budget_excluded_preview_results.append(
                    {
                        "file": str(src),
                        "original_size": int(original_size or 0),
                        "estimated_size": int(estimated_size or 0),
                        "estimated_savings_pct": 0.0,
                        "score": round(rank_score, 3),
                        "score_band": _preview_score_band(rank_score),
                        "confidence_label": "",
                        "confidence_adjustment_points": 0.0,
                        "score_reasons": list(rank_reasons[:3]),
                        "history_influenced": False,
                        "history_influence_reason": None,
                        "included_by_budget": budget_meta.get("included_by_budget"),
                        "budget_status": budget_meta.get("budget_status"),
                        "budget_reason": budget_meta.get("budget_reason"),
                        "estimated_savings_bytes": budget_meta.get("estimated_savings_bytes"),
                        "cumulative_estimated_savings_bytes": budget_meta.get("cumulative_estimated_savings_bytes"),
                        "budget_target_bytes": budget_meta.get("budget_target_bytes"),
                        "decision": decision,
                    }
                )

        for src in cands:
            if _cancel_check("candidate scanning"):
                break
            if done >= effective_max_files:
                break

            # Max GB per run guard (Story 43)
            if getattr(cfg, "max_gb_per_run", 0):
                limit_bytes = float(cfg.max_gb_per_run) * (1024 ** 3)
                if saved_bytes_run >= limit_bytes and limit_bytes > 0:
                    logger.log(
                        f"Max GB per run reached: saved={saved_bytes_run/1024**3:.2f}GB "
                        f"limit={float(cfg.max_gb_per_run):.2f}GB — stopping early."
                    )
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

            evaluated += 1
            logger.log(f"Processing: {src}")
            _progress(
                current_file=str(src),
                files_evaluated=evaluated,
                encode_percent="",
                encode_speed="",
                encode_eta="",
                encode_out_time="",
            )

            try:
                src_stat = src.stat()
                before_bytes = src_stat.st_size
                file_mtime = src_stat.st_mtime
            except Exception:
                before_bytes = 0
                file_mtime = None

            if _cancel_check("evaluation"):
                break

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

                skipped_dry_run += 1
                done += 1
                continue


            # Policy skip cache (reason-aware): only max_savings is currently cached.
            cached_max_savings = get_policy_skip_cache(cfg, logger, src=src, skip_reason="max_savings")
            if cached_max_savings is not None:
                cached_pct = float(cached_max_savings.get("savings_percent") or 0.0)
                threshold = float(getattr(cfg, "max_savings_percent", 0) or 0)
                if threshold and threshold <= cached_pct:
                    skipped_max_savings += 1
                    LOGGER.info(
                        "Skipping cached policy decision: file='%s' reason='%s' stored_savings=%.1f%% threshold=%.1f%%",
                        _display_name(src),
                        "max_savings",
                        cached_pct,
                        threshold,
                    )
                    if cfg.log_skips:
                        logger.log(
                            f"SKIP(max_savings:cached): {cached_pct:.1f}% > {threshold:.1f}% :: {src}"
                        )
                    record_skip(
                        cfg,
                        logger,
                        run_id=run_id,
                        mode=mode.lower(),
                        skip_reason='max_savings',
                        src=src,
                        before_bytes=int(before_bytes or 0),
                        detail=f"cached {cached_pct:.1f}% > {threshold:.1f}%",
                    )
                    done += 1
                    continue
                # Threshold is now more permissive; force fresh evaluation by removing stale cache row.
                delete_policy_skip_cache(cfg, logger, src=src, skip_reason="max_savings")

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

            historical_avg_savings_percent, historical_context = _select_historical_signal(
                history_summaries=history_summaries,
                src=src,
                before_probe=before_probe,
                library_name=str(getattr(cfg, "library", "") or ""),
            )
            _score_inputs = build_candidate_score_inputs(
                cfg=cfg,
                src=src,
                file_size_bytes=int(before_bytes or 0),
                before_probe=before_probe,
                cached_max_savings_percent=(
                    float(cached_max_savings.get("savings_percent")) if cached_max_savings is not None else None
                ),
                historical_avg_savings_percent=historical_avg_savings_percent,
                historical_context=historical_context,
                file_mtime=file_mtime,
            )
            _score_result = calculate_candidate_score(_score_inputs)
            history_fragment = ""
            history_points = float(getattr(_score_result, "historical_adjustment_points", 0.0) or 0.0)
            if history_points:
                history_fragment = " (+history: %+0.1f)" % history_points
            logger.log(
                "SCORE: %.3f%s conf=%s (%+0.1f) history_influenced=%s reasons=%s file=%s"
                % (
                    float(_score_result.score),
                    history_fragment,
                    str(getattr(_score_result, "confidence_label", "medium") or "medium"),
                    float(getattr(_score_result, "confidence_adjustment_points", 0.0) or 0.0),
                    "yes" if bool(getattr(_score_result, "history_influenced", False)) else "no",
                    ", ".join(_score_result.reasons[:3]) if _score_result.reasons else "none",
                    src,
                )
            )

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
                # Stats: record policy/runtime skips (avoid marker/backup prefilters)
                record_skip(
                    cfg,
                    logger,
                    run_id=run_id,
                    mode=mode.lower(),
                    skip_reason=cat,
                    src=src,
                    before_bytes=int(before_bytes or 0),
                    codec_from=(before_probe.get('codec') if before_probe else None),
                    detail=str(reason),
                )
                if cfg.preview:
                    decision = "Skip (unsupported codec)" if cat == "codec" else "Skip (resolution rules)"
                    budget_meta = budget_selection_meta.get(src, {})
                    preview_result = {
                        "file": str(src),
                        "original_size": int(before_bytes or 0),
                        "estimated_size": int(before_bytes or 0),
                        "estimated_savings_pct": 0.0,
                        "score": round(float(_score_result.score), 3),
                        "score_band": _preview_score_band(float(_score_result.score)),
                        "confidence_label": str(getattr(_score_result, "confidence_label", "medium") or "medium"),
                        "confidence_adjustment_points": round(
                            float(getattr(_score_result, "confidence_adjustment_points", 0.0) or 0.0), 3
                        ),
                        "score_reasons": list(_score_result.reasons[:3]),
                        "history_influenced": bool(getattr(_score_result, "history_influenced", False)),
                        "history_influence_reason": getattr(_score_result, "history_influence_reason", None),
                        "included_by_budget": budget_meta.get("included_by_budget"),
                        "budget_status": budget_meta.get("budget_status"),
                        "budget_reason": budget_meta.get("budget_reason"),
                        "estimated_savings_bytes": budget_meta.get("estimated_savings_bytes"),
                        "cumulative_estimated_savings_bytes": budget_meta.get("cumulative_estimated_savings_bytes"),
                        "budget_target_bytes": budget_meta.get("budget_target_bytes"),
                        "decision": decision,
                    }
                    _progress(
                        preview_result=preview_result,
                        preview_result_json=json.dumps(preview_result),
                        files_evaluated=evaluated,
                    )
                    done += 1
                continue

            if cfg.preview:
                estimated_bytes = _estimate_size_bytes(int(before_bytes or 0), cfg, before_probe)
                estimated_savings_pct = 0.0
                if before_bytes > 0 and estimated_bytes > 0:
                    estimated_savings_pct = ((before_bytes - estimated_bytes) / float(before_bytes)) * 100.0
                historical_avg_savings_percent, historical_context = _select_historical_signal(
                    history_summaries=history_summaries,
                    src=src,
                    before_probe=before_probe,
                    library_name=str(getattr(cfg, "library", "") or ""),
                )
                _score_inputs = build_candidate_score_inputs(
                    cfg=cfg,
                    src=src,
                    file_size_bytes=int(before_bytes or 0),
                    before_probe=before_probe,
                    estimated_encoded_size_bytes=int(estimated_bytes or 0),
                    estimated_savings_percent=float(estimated_savings_pct),
                    cached_max_savings_percent=(
                        float(cached_max_savings.get("savings_percent")) if cached_max_savings is not None else None
                    ),
                    historical_avg_savings_percent=historical_avg_savings_percent,
                    historical_context=historical_context,
                    file_mtime=file_mtime,
                )
                _score_result = calculate_candidate_score(_score_inputs)

                if cfg.min_savings_percent and estimated_savings_pct < float(cfg.min_savings_percent):
                    skipped_min_savings += 1
                    decision = "Skip (below savings threshold)"
                elif getattr(cfg, "max_savings_percent", 0) and estimated_savings_pct > float(cfg.max_savings_percent):
                    skipped_max_savings += 1
                    upsert_policy_skip_cache(
                        cfg,
                        logger,
                        src=src,
                        skip_reason="max_savings",
                        savings_percent=float(estimated_savings_pct),
                    )
                    LOGGER.info(
                        "Caching policy skip: file='%s' reason='%s' savings=%.1f%%",
                        _display_name(src),
                        "max_savings",
                        float(estimated_savings_pct),
                    )
                    decision = "Skip (above max savings threshold)"
                else:
                    decision = "Encode"

                budget_meta = budget_selection_meta.get(src, {})
                preview_result = {
                    "file": str(src),
                    "original_size": int(before_bytes or 0),
                    "estimated_size": int(estimated_bytes or 0),
                    "estimated_savings_pct": round(float(estimated_savings_pct), 1),
                    "score": round(float(_score_result.score), 3),
                    "score_band": _preview_score_band(float(_score_result.score)),
                    "confidence_label": str(getattr(_score_result, "confidence_label", "medium") or "medium"),
                    "confidence_adjustment_points": round(
                        float(getattr(_score_result, "confidence_adjustment_points", 0.0) or 0.0), 3
                    ),
                    "score_reasons": list(_score_result.reasons[:3]),
                    "history_influenced": bool(getattr(_score_result, "history_influenced", False)),
                    "history_influence_reason": getattr(_score_result, "history_influence_reason", None),
                    "included_by_budget": budget_meta.get("included_by_budget"),
                    "budget_status": budget_meta.get("budget_status"),
                    "budget_reason": budget_meta.get("budget_reason"),
                    "estimated_savings_bytes": budget_meta.get("estimated_savings_bytes"),
                    "cumulative_estimated_savings_bytes": budget_meta.get("cumulative_estimated_savings_bytes"),
                    "budget_target_bytes": budget_meta.get("budget_target_bytes"),
                    "decision": decision,
                }
                _progress(
                    preview_result=preview_result,
                    preview_result_json=json.dumps(preview_result),
                    files_evaluated=evaluated,
                )
                logger.log(
                    "PREVIEW: %s before=%.2fGB estimated=%.2fGB savings=%.1f%% score=%.3f conf=%s history_influenced=%s decision=%s"
                    % (
                        src,
                        (before_bytes / 1024 ** 3) if before_bytes else 0.0,
                        (estimated_bytes / 1024 ** 3) if estimated_bytes else 0.0,
                        estimated_savings_pct,
                        float(_score_result.score),
                        str(getattr(_score_result, "confidence_label", "medium") or "medium"),
                        "yes" if bool(getattr(_score_result, "history_influenced", False)) else "no",
                        decision,
                    )
                )
                done += 1
                continue

            attempt_errors: list[str] = []
            for attempt in range(cfg.retry_count + 1):
                _progress(retry_attempt=attempt, retry_max=cfg.retry_count)
                if _cancel_check("encoding"):
                    break
                if attempt > 0:
                    logger.log(f"RETRY {attempt}/{cfg.retry_count}: {src}")
                    if cfg.retry_backoff_seconds:
                        time.sleep(cfg.retry_backoff_seconds)

                t0 = time.monotonic()
                try:
                    stage = "encode"
                    try:
                        encode_qsv(
                            src,
                            encoded,
                            cfg,
                            logger,
                            cancel_requested=cancel_requested,
                            on_process_start=_set_active_ffmpeg,
                            progress_callback=_progress,
                        )
                    except TypeError as exc:
                        if "cancel_requested" not in str(exc) and "on_process_start" not in str(exc) and "progress_callback" not in str(exc):
                            raise
                        encode_qsv(src, encoded, cfg, logger)
                    if _cancel_check("encoding"):
                        try:
                            encoded.unlink(missing_ok=True)
                        except Exception:
                            pass
                        break

                    if _cancel_check("evaluation"):
                        try:
                            encoded.unlink(missing_ok=True)
                        except Exception:
                            pass
                        break

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
                            record_skip(
                                cfg,
                                logger,
                                run_id=run_id,
                                mode=mode.lower(),
                                skip_reason='min_savings',
                                src=src,
                                before_bytes=int(before_bytes or 0),
                                codec_from=(before_probe.get('codec') if before_probe else None),
                                detail=f"{pct_tmp:.1f}% < {cfg.min_savings_percent:.1f}%",
                            )
                            done += 1
                            break

                        # Max savings guard (Story 44) - reject overly aggressive reductions
                        if getattr(cfg, 'max_savings_percent', 0) and pct_tmp > float(cfg.max_savings_percent):
                            logger.log(
                                f"SKIP: savings {pct_tmp:.1f}% > MAX_SAVINGS_PERCENT {float(cfg.max_savings_percent):.1f}% "
                                f"(before={before_bytes/1024**3:.2f}GB encoded={encoded_bytes/1024**3:.2f}GB)"
                            )
                            try:
                                encoded.unlink(missing_ok=True)
                            except Exception:
                                pass
                            skipped_max_savings += 1
                            upsert_policy_skip_cache(
                                cfg,
                                logger,
                                src=src,
                                skip_reason="max_savings",
                                savings_percent=float(pct_tmp),
                            )
                            LOGGER.info(
                                "Caching policy skip: file='%s' reason='%s' savings=%.1f%%",
                                _display_name(src),
                                "max_savings",
                                float(pct_tmp),
                            )
                            record_skip(
                                cfg,
                                logger,
                                run_id=run_id,
                                mode=mode.lower(),
                                skip_reason='max_savings',
                                src=src,
                                before_bytes=int(before_bytes or 0),
                                codec_from=(before_probe.get('codec') if before_probe else None),
                                detail=f"{pct_tmp:.1f}% > {float(cfg.max_savings_percent):.1f}%",
                            )
                            done += 1
                            break

                    if _cancel_check("evaluation"):
                        try:
                            encoded.unlink(missing_ok=True)
                        except Exception:
                            pass
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
                        saved_bytes_run += int(saved)
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
                    succeeded += 1
                    processed += 1
                    _progress(files_processed=processed, current_file=str(src))
                    _progress(success_count=succeeded, bytes_saved=saved_bytes_run, current_file=str(src), retry_attempt="", retry_max="")
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
                    _progress(files_failed=failed, current_file=str(src), retry_attempt="", retry_max="")
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

        if cfg.preview and budget_excluded_preview_results:
            for preview_result in budget_excluded_preview_results:
                _progress(
                    preview_result=preview_result,
                    preview_result_json=json.dumps(preview_result),
                    files_evaluated=evaluated,
                )
                logger.log(
                    "PREVIEW: %s decision=%s budget=%s cumulative=%s/%s"
                    % (
                        str(preview_result.get("file", "")),
                        str(preview_result.get("decision", "")),
                        str(preview_result.get("budget_status", "")),
                        int(preview_result.get("cumulative_estimated_savings_bytes") or 0),
                        int(preview_result.get("budget_target_bytes") or 0),
                    )
                )

        # Summary
        if cancelled:
            logger.log("RUN STATUS: cancelled")
            if not cancel_recorded:
                cancel_src = cands[0] if cands else cfg.media_root
                record_skip(
                    cfg,
                    logger,
                    run_id=run_id,
                    mode=mode.lower(),
                    skip_reason="cancelled",
                    src=cancel_src,
                    before_bytes=0,
                    detail="Run cancelled by operator",
                )
                cancel_recorded = True

        logger.log("===== SUMMARY =====")
        ignored_files = sum(ignored_folders.values()) if ignored_folders else 0
        prefiltered = skipped_marker + skipped_backup + skipped_recent + prefiltered_policy_cache
        skipped_policy = skipped_codec + skipped_resolution + skipped_min_savings + skipped_max_savings + skipped_dry_run
        _progress(files_evaluated=evaluated, files_processed=processed, success_count=succeeded, files_skipped=skipped_policy, files_failed=failed, bytes_saved=saved_bytes_run)
        logger.log(f"Candidates found:     {len(cands)}")
        logger.log(f"Pre-filtered:         {prefiltered}")
        logger.log(f"Evaluated:            {evaluated}")
        logger.log(f"Processed (encode):   {processed}")
        logger.log(f"Succeeded:            {succeeded}")
        logger.log(f"Skipped (policy):     {skipped_policy}")
        logger.log(f"Failed:               {failed}")
        logger.log(f"Pre-filtered (marker):     {skipped_marker}")
        logger.log(f"Pre-filtered (backup):     {skipped_backup}")
        logger.log(f"Pre-filtered (recent):     {skipped_recent}")
        logger.log(f"Pre-filtered (policy cache): {prefiltered_policy_cache}")
        logger.log(f"Skipped (codec):      {skipped_codec}")
        logger.log(f"Skipped (resolution): {skipped_resolution}")
        logger.log(f"Skipped (min savings): {skipped_min_savings}")
        logger.log(f"Skipped (max savings): {skipped_max_savings}")
        logger.log(f"Skipped (dry run):    {skipped_dry_run}")
        logger.log(f"Ignored folders:      {len(ignored_folders)}")
        logger.log(f"Ignored files:        {ignored_files}")

        record_run_counters(
            cfg,
            logger,
            run_id=run_id,
            candidates_found=len(cands),
            prefiltered_count=prefiltered,
            evaluated_count=evaluated,
            processed_count=processed,
            prefiltered_marker_count=skipped_marker,
            prefiltered_backup_count=skipped_backup,
            skipped_codec_count=skipped_codec,
            skipped_resolution_count=skipped_resolution,
            skipped_min_savings_count=skipped_min_savings,
            skipped_max_savings_count=skipped_max_savings,
            skipped_dry_run_count=skipped_dry_run,
            ignored_folder_count=len(ignored_folders),
            ignored_file_count=ignored_files,
        )

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
        if cancelled:
            return 0
        return 0 if failed == 0 else 2

    finally:
        release_lock(lock_path, logger)
