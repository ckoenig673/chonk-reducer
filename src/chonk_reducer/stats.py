from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from .config import Config
from .logging_utils import Logger


def _iso_ts() -> str:
    # ISO-8601 without timezone (consistent with existing logs)
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())


def _safe_str(e: Exception) -> str:
    s = str(e).replace("\n", " ").strip()
    # keep it compact for Discord/logging/import
    return (s[:500] + "…") if len(s) > 500 else s


def append_ndjson(path: Path, obj: dict[str, Any], logger: Logger) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
        with path.open("a", encoding="utf-8", newline="\n") as f:
            f.write(line + "\n")
    except Exception as e:
        logger.log(f"WARN: stats append failed: {path} ({e})")


def infer_library(cfg: Config) -> str:
    if getattr(cfg, "library", ""):
        return cfg.library
    s = str(cfg.media_root).lower()
    if "tv" in s:
        return "tv"
    if "movie" in s:
        return "movies"
    return "media"


def build_base(cfg: Config, run_id: str, mode: str) -> dict[str, Any]:
    return {
        "ts": _iso_ts(),
        "run_id": run_id,
        "version": getattr(cfg, "version", "") or "unknown",
        "library": infer_library(cfg),
        "mode": mode,
        "encoder": getattr(cfg, "encoder", "hevc_qsv"),
        "quality": int(cfg.qsv_quality),
        "preset": int(cfg.qsv_preset),
    }


def record_success(
    cfg: Config,
    logger: Logger,
    run_id: str,
    mode: str,
    stage: str,
    src: Path,
    before_bytes: int,
    after_bytes: int,
    codec_from: str | None,
    codec_to: str | None,
    duration_seconds: float,
    bak_path: Path | None = None,
) -> None:
    if not getattr(cfg, "stats_enabled", False):
        return
    saved = int(before_bytes) - int(after_bytes)
    pct = (saved / before_bytes) * 100.0 if before_bytes else 0.0

    obj = build_base(cfg, run_id, mode)
    obj.update(
        {
            "status": "success",
            "stage": stage,
            "path": str(src),
            "filename": src.name,
            "size_before_bytes": int(before_bytes),
            "size_after_bytes": int(after_bytes),
            "saved_bytes": int(saved),
            "saved_pct": round(pct, 3),
            "codec_from": codec_from or "",
            "codec_to": codec_to or "",
            "duration_seconds": round(float(duration_seconds), 3),
        }
    )
    if bak_path is not None:
        obj["bak_path"] = str(bak_path)

    append_ndjson(Path(cfg.stats_path), obj, logger)


def record_failure(
    cfg: Config,
    logger: Logger,
    run_id: str,
    mode: str,
    stage: str,
    src: Path,
    before_bytes: int,
    duration_seconds: float,
    err: Exception,
    encoded_path: Path | None = None,
) -> None:
    if not getattr(cfg, "stats_enabled", False):
        return

    obj = build_base(cfg, run_id, mode)
    obj.update(
        {
            "status":"failed",
            "stage": stage,
            "fail_stage": stage,
            "path": str(src),
            "filename": src.name,
            "size_before_bytes": int(before_bytes),
            "duration_seconds": round(float(duration_seconds), 3),
            "error_type": type(err).__name__,
            "error_msg": _safe_str(err),
        }
    )

    if encoded_path is not None:
        obj["encoded_path"] = str(encoded_path)
        try:
            obj["encoded_bytes"] = int(encoded_path.stat().st_size)
        except Exception:
            pass

    append_ndjson(Path(cfg.stats_path), obj, logger)


def record_skip(
    cfg: Config,
    logger: Logger,
    run_id: str,
    mode: str,
    skip_reason: str,
    src: Path,
    before_bytes: int,
    codec_from: str | None = None,
    detail: str | None = None,
) -> None:
    """Record a skipped file in NDJSON stats (policy/runtime skip, not pre-filter markers).

    Note: We intentionally do not record marker/backup pre-filters to avoid bloating stats.
    """
    if not getattr(cfg, "stats_enabled", False):
        return

    obj = build_base(cfg, run_id, mode)
    obj.update(
        {
            "status": "skipped",
            "stage": "skip",
            "skip_reason": (skip_reason or "unknown").lower(),
            "path": str(src),
            "filename": src.name,
            "size_before_bytes": int(before_bytes),
            "codec_from": codec_from or "",
        }
    )
    if detail:
        obj["skip_detail"] = str(detail)[:500]

    append_ndjson(Path(cfg.stats_path), obj, logger)


def record_dry_run(
    cfg: Config,
    logger: Logger,
    run_id: str,
    src: Path,
    before_bytes: int,
) -> None:
    if not getattr(cfg, "stats_enabled", False):
        return
    obj = build_base(cfg, run_id, "dry_run")
    obj.update(
        {
            "status":"skipped",
            "stage":"dry_run",
            "skip_reason":"dry_run",
            "path": str(src),
            "filename": src.name,
            "size_before_bytes": int(before_bytes),
        }
    )
    append_ndjson(Path(cfg.stats_path), obj, logger)
