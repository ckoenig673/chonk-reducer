from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import time
from typing import Mapping, Any


@dataclass(frozen=True)
class CandidateScoreInputs:
    """Groundwork model for future candidate scoring/ranking stories."""

    library_name: str
    library_priority: int | None
    file_path: Path
    file_size_bytes: int
    estimated_encoded_size_bytes: int | None
    estimated_savings_percent: float | None
    estimated_savings_bytes: int | None
    file_age_minutes: int | None
    codec: str | None
    width: int | None
    height: int | None
    resolution_tag_hints: tuple[str, ...]
    has_cached_max_savings_skip: bool
    cached_max_savings_percent: float | None


def build_candidate_score_inputs(
    *,
    cfg,
    src: Path,
    file_size_bytes: int,
    before_probe: Mapping[str, Any] | None = None,
    estimated_encoded_size_bytes: int | None = None,
    estimated_savings_percent: float | None = None,
    cached_max_savings_percent: float | None = None,
    now_ts: float | None = None,
    file_mtime: float | None = None,
) -> CandidateScoreInputs:
    """Build a scoring-input snapshot using only existing cheap pipeline data."""
    estimated_savings_bytes: int | None = None
    if estimated_encoded_size_bytes is not None:
        estimated_savings_bytes = int(file_size_bytes) - int(estimated_encoded_size_bytes)

    if file_mtime is not None:
        now_value = float(now_ts) if now_ts is not None else time.time()
        file_age_minutes = int(max(0, (now_value - float(file_mtime)) // 60))
    else:
        file_age_minutes = None

    codec: str | None = None
    width: int | None = None
    height: int | None = None
    if before_probe:
        codec_raw = before_probe.get("codec")
        codec = str(codec_raw) if codec_raw else None
        try:
            width = int(before_probe.get("width")) if before_probe.get("width") is not None else None
        except Exception:
            width = None
        try:
            height = int(before_probe.get("height")) if before_probe.get("height") is not None else None
        except Exception:
            height = None

    source_name = src.name.lower()
    resolution_tag_hints = tuple(
        tag for tag in getattr(cfg, "skip_resolution_tags", ()) if tag and str(tag).lower() in source_name
    )

    return CandidateScoreInputs(
        library_name=str(getattr(cfg, "library", "") or ""),
        library_priority=getattr(cfg, "library_priority", None),
        file_path=src,
        file_size_bytes=int(file_size_bytes),
        estimated_encoded_size_bytes=int(estimated_encoded_size_bytes) if estimated_encoded_size_bytes is not None else None,
        estimated_savings_percent=float(estimated_savings_percent) if estimated_savings_percent is not None else None,
        estimated_savings_bytes=estimated_savings_bytes,
        file_age_minutes=file_age_minutes,
        codec=codec,
        width=width,
        height=height,
        resolution_tag_hints=resolution_tag_hints,
        has_cached_max_savings_skip=cached_max_savings_percent is not None,
        cached_max_savings_percent=float(cached_max_savings_percent) if cached_max_savings_percent is not None else None,
    )
