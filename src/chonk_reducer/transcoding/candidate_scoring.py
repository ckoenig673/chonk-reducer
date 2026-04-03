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
    historical_avg_savings_percent: float | None = None
    historical_context: str | None = None


@dataclass(frozen=True)
class CandidateScoreResult:
    """Deterministic score + concise factor labels for explainability."""

    score: float
    reasons: tuple[str, ...]
    savings_bytes_points: float
    savings_percent_points: float
    library_priority_points: float
    file_size_points: float
    cached_max_savings_penalty: float
    historical_adjustment_points: float


def _clamp(value: float, *, low: float, high: float) -> float:
    return max(low, min(value, high))


def _reason_for_savings_bytes(points: float) -> str | None:
    if points >= 24.0:
        return "high projected GB savings"
    if points >= 8.0:
        return "notable projected GB savings"
    return None


def _reason_for_savings_percent(points: float) -> str | None:
    if points >= 24.0:
        return "strong projected % savings"
    if points >= 8.0:
        return "good projected % savings"
    return None


def _reason_for_library_priority(points: float) -> str | None:
    if points >= 10.0:
        return "high library priority"
    if points >= 3.0:
        return "elevated library priority"
    return None


def _reason_for_file_size(points: float) -> str | None:
    if points >= 6.0:
        return "large source file"
    return None


def _reason_for_history_adjustment(*, points: float, context: str | None) -> str | None:
    if points == 0:
        return None
    context_value = str(context or "").strip().lower()
    if context_value.startswith("codec:"):
        return "historically strong savings for this codec" if points > 0 else "historically weak savings for this codec"
    if context_value.startswith("resolution:"):
        return (
            "history suggests better than estimate"
            if points > 0
            else "historically weak savings for this resolution"
        )
    if context_value.startswith("library:"):
        return "history suggests better than estimate" if points > 0 else "history suggests worse than estimate"
    return "history suggests better than estimate" if points > 0 else "history suggests worse than estimate"


def calculate_candidate_score(inputs: CandidateScoreInputs) -> CandidateScoreResult:
    """
    Convert scoring inputs into a simple weighted heuristic score.

    Notes:
    - This only computes score metadata; candidate ordering is unchanged for now.
    - Weights intentionally prioritize estimated byte/percent savings first.
    """
    reasons: list[str] = []

    savings_bytes_points = 0.0
    if inputs.estimated_savings_bytes is not None:
        savings_mib = float(inputs.estimated_savings_bytes) / float(1024 * 1024)
        savings_bytes_points = _clamp(savings_mib / 4.0, low=0.0, high=40.0)
        reason = _reason_for_savings_bytes(savings_bytes_points)
        if reason:
            reasons.append(reason)

    savings_percent_points = 0.0
    if inputs.estimated_savings_percent is not None:
        savings_percent_points = _clamp(float(inputs.estimated_savings_percent) * 0.8, low=0.0, high=40.0)
        reason = _reason_for_savings_percent(savings_percent_points)
        if reason:
            reasons.append(reason)

    library_priority_points = 0.0
    if inputs.library_priority is not None:
        library_priority_points = _clamp(float(inputs.library_priority) / 10.0, low=0.0, high=15.0)
        reason = _reason_for_library_priority(library_priority_points)
        if reason:
            reasons.append(reason)

    file_size_points = 0.0
    if inputs.file_size_bytes > 0:
        size_gib = float(inputs.file_size_bytes) / float(1024 ** 3)
        file_size_points = _clamp(size_gib * 4.0, low=0.0, high=8.0)
        reason = _reason_for_file_size(file_size_points)
        if reason:
            reasons.append(reason)

    cached_max_savings_penalty = 0.0
    if inputs.has_cached_max_savings_skip:
        cached_max_savings_penalty = 30.0
        reasons.append("reduced by prior max-savings skip signal")

    historical_adjustment_points = 0.0
    if (
        inputs.historical_avg_savings_percent is not None
        and inputs.estimated_savings_percent is not None
    ):
        delta = float(inputs.historical_avg_savings_percent) - float(inputs.estimated_savings_percent)
        historical_adjustment_points = _clamp(delta * 0.5, low=-10.0, high=10.0)
        reason = _reason_for_history_adjustment(
            points=historical_adjustment_points,
            context=inputs.historical_context,
        )
        if reason:
            reasons.append(reason)

    score = (
        savings_bytes_points
        + savings_percent_points
        + library_priority_points
        + file_size_points
        - cached_max_savings_penalty
        + historical_adjustment_points
    )
    score = round(max(0.0, score), 3)

    return CandidateScoreResult(
        score=score,
        reasons=tuple(reasons),
        savings_bytes_points=round(savings_bytes_points, 3),
        savings_percent_points=round(savings_percent_points, 3),
        library_priority_points=round(library_priority_points, 3),
        file_size_points=round(file_size_points, 3),
        cached_max_savings_penalty=round(cached_max_savings_penalty, 3),
        historical_adjustment_points=round(historical_adjustment_points, 3),
    )


def build_candidate_score_inputs(
    *,
    cfg,
    src: Path,
    file_size_bytes: int,
    before_probe: Mapping[str, Any] | None = None,
    estimated_encoded_size_bytes: int | None = None,
    estimated_savings_percent: float | None = None,
    cached_max_savings_percent: float | None = None,
    historical_avg_savings_percent: float | None = None,
    historical_context: str | None = None,
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
        historical_avg_savings_percent=(
            float(historical_avg_savings_percent) if historical_avg_savings_percent is not None else None
        ),
        historical_context=(str(historical_context) if historical_context else None),
    )
