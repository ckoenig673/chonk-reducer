from __future__ import annotations

import types
from pathlib import Path

from chonk_reducer.transcoding.candidate_scoring import (
    build_candidate_score_inputs,
    calculate_candidate_score,
)


def _cfg(**overrides):
    base = types.SimpleNamespace(
        library="movies",
        library_priority=None,
        skip_resolution_tags=("2160p", "4k"),
    )
    for k, v in overrides.items():
        setattr(base, k, v)
    return base


def test_build_candidate_score_inputs_populates_available_fields(tmp_path: Path):
    src = tmp_path / "Movie.2160p.mkv"
    inputs = build_candidate_score_inputs(
        cfg=_cfg(library="tv", library_priority=150),
        src=src,
        file_size_bytes=10_000,
        before_probe={"codec": "h264", "width": "3840", "height": 2160},
        estimated_encoded_size_bytes=5_500,
        estimated_savings_percent=45.0,
        cached_max_savings_percent=51.2,
        now_ts=3_600,
        file_mtime=0,
    )

    assert inputs.library_name == "tv"
    assert inputs.library_priority == 150
    assert inputs.file_path == src
    assert inputs.file_size_bytes == 10_000
    assert inputs.estimated_encoded_size_bytes == 5_500
    assert inputs.estimated_savings_percent == 45.0
    assert inputs.estimated_savings_bytes == 4_500
    assert inputs.file_age_minutes == 60
    assert inputs.codec == "h264"
    assert inputs.width == 3840
    assert inputs.height == 2160
    assert inputs.resolution_tag_hints == ("2160p",)
    assert inputs.has_cached_max_savings_skip is True
    assert inputs.cached_max_savings_percent == 51.2
    assert inputs.historical_avg_savings_percent is None
    assert inputs.historical_context is None


def test_build_candidate_score_inputs_handles_missing_optional_values(tmp_path: Path):
    src = tmp_path / "Movie.mkv"
    inputs = build_candidate_score_inputs(
        cfg=_cfg(),
        src=src,
        file_size_bytes=7_500,
        before_probe={"width": "unknown", "height": None},
    )

    assert inputs.library_name == "movies"
    assert inputs.library_priority is None
    assert inputs.estimated_encoded_size_bytes is None
    assert inputs.estimated_savings_percent is None
    assert inputs.estimated_savings_bytes is None
    assert inputs.file_age_minutes is None
    assert inputs.codec is None
    assert inputs.width is None
    assert inputs.height is None
    assert inputs.resolution_tag_hints == ()
    assert inputs.has_cached_max_savings_skip is False
    assert inputs.cached_max_savings_percent is None
    assert inputs.historical_avg_savings_percent is None
    assert inputs.historical_context is None


def test_calculate_candidate_score_prefers_higher_value_candidates(tmp_path: Path):
    strong = build_candidate_score_inputs(
        cfg=_cfg(library_priority=120),
        src=tmp_path / "Strong.mkv",
        file_size_bytes=8 * 1024 ** 3,
        estimated_encoded_size_bytes=4 * 1024 ** 3,
        estimated_savings_percent=50.0,
    )
    weak = build_candidate_score_inputs(
        cfg=_cfg(library_priority=10),
        src=tmp_path / "Weak.mkv",
        file_size_bytes=400 * 1024 ** 2,
        estimated_encoded_size_bytes=380 * 1024 ** 2,
        estimated_savings_percent=5.0,
    )

    strong_result = calculate_candidate_score(strong)
    weak_result = calculate_candidate_score(weak)

    assert strong_result.score > weak_result.score
    assert strong_result.savings_bytes_points > weak_result.savings_bytes_points
    assert strong_result.savings_percent_points > weak_result.savings_percent_points


def test_calculate_candidate_score_cached_max_savings_penalty_reduces_score(tmp_path: Path):
    baseline = build_candidate_score_inputs(
        cfg=_cfg(library_priority=100),
        src=tmp_path / "PenaltyCheck.mkv",
        file_size_bytes=2 * 1024 ** 3,
        estimated_encoded_size_bytes=1 * 1024 ** 3,
        estimated_savings_percent=50.0,
    )
    penalized = build_candidate_score_inputs(
        cfg=_cfg(library_priority=100),
        src=tmp_path / "PenaltyCheck.mkv",
        file_size_bytes=2 * 1024 ** 3,
        estimated_encoded_size_bytes=1 * 1024 ** 3,
        estimated_savings_percent=50.0,
        cached_max_savings_percent=60.0,
    )

    baseline_result = calculate_candidate_score(baseline)
    penalized_result = calculate_candidate_score(penalized)

    assert penalized_result.score < baseline_result.score
    assert penalized_result.cached_max_savings_penalty == 30.0
    assert "reduced by prior max-savings skip signal" in penalized_result.reasons


def test_calculate_candidate_score_handles_missing_optional_values(tmp_path: Path):
    inputs = build_candidate_score_inputs(
        cfg=_cfg(library_priority=None),
        src=tmp_path / "Unknown.mkv",
        file_size_bytes=0,
        estimated_encoded_size_bytes=None,
        estimated_savings_percent=None,
    )

    result = calculate_candidate_score(inputs)

    assert result.score == 0.0
    assert result.savings_bytes_points == 0.0
    assert result.savings_percent_points == 0.0
    assert result.library_priority_points == 0.0
    assert result.file_size_points == 0.0
    assert result.cached_max_savings_penalty == 0.0
    assert result.confidence_adjustment_points == -2.0
    assert result.confidence_label == "low"
    assert result.history_influenced is False
    assert result.history_influence_reason is None
    assert result.reasons == ()


def test_calculate_candidate_score_reasons_are_consistent_order(tmp_path: Path):
    inputs = build_candidate_score_inputs(
        cfg=_cfg(library_priority=100),
        src=tmp_path / "Consistent.mkv",
        file_size_bytes=2 * 1024 ** 3,
        estimated_encoded_size_bytes=1 * 1024 ** 3,
        estimated_savings_percent=50.0,
        cached_max_savings_percent=63.2,
    )

    result = calculate_candidate_score(inputs)

    assert result.reasons == (
        "high projected GB savings",
        "strong projected % savings",
        "high library priority",
        "large source file",
        "reduced by prior max-savings skip signal",
    )


def test_calculate_candidate_score_reasons_are_readable_and_high_signal(tmp_path: Path):
    inputs = build_candidate_score_inputs(
        cfg=_cfg(library_priority=85),
        src=tmp_path / "Readable.mkv",
        file_size_bytes=2 * 1024 ** 3,
        estimated_encoded_size_bytes=int(0.75 * 1024 ** 3),
        estimated_savings_percent=38.0,
    )

    result = calculate_candidate_score(inputs)

    assert result.reasons == (
        "high projected GB savings",
        "strong projected % savings",
        "elevated library priority",
        "large source file",
    )
    assert all(":" not in reason for reason in result.reasons)
    assert all("_" not in reason for reason in result.reasons)


def test_calculate_candidate_score_omits_low_value_reasons_for_weak_candidate(tmp_path: Path):
    weak = build_candidate_score_inputs(
        cfg=_cfg(library_priority=10),
        src=tmp_path / "WeakReasons.mkv",
        file_size_bytes=300 * 1024 ** 2,
        estimated_encoded_size_bytes=280 * 1024 ** 2,
        estimated_savings_percent=6.0,
    )

    result = calculate_candidate_score(weak)

    assert result.score > 0.0
    assert result.reasons == ()


def test_calculate_candidate_score_applies_positive_history_adjustment(tmp_path: Path):
    inputs = build_candidate_score_inputs(
        cfg=_cfg(library_priority=50),
        src=tmp_path / "PositiveHistory.mkv",
        file_size_bytes=2 * 1024 ** 3,
        estimated_encoded_size_bytes=int(1.3 * 1024 ** 3),
        estimated_savings_percent=35.0,
        historical_avg_savings_percent=60.0,
        historical_context="codec:h264",
    )

    result = calculate_candidate_score(inputs)
    baseline = calculate_candidate_score(
        build_candidate_score_inputs(
            cfg=_cfg(library_priority=50),
            src=tmp_path / "PositiveHistory.mkv",
            file_size_bytes=2 * 1024 ** 3,
            estimated_encoded_size_bytes=int(1.3 * 1024 ** 3),
            estimated_savings_percent=35.0,
        )
    )

    assert result.historical_adjustment_points == 10.0
    assert result.confidence_adjustment_points == 3.0
    assert baseline.confidence_adjustment_points == 2.0
    assert result.score == round(baseline.score + 11.0, 3)
    assert result.history_influenced is True
    assert result.history_influence_reason == "history-influenced score"
    assert "historically strong savings for this codec" in result.reasons


def test_calculate_candidate_score_applies_negative_history_adjustment(tmp_path: Path):
    inputs = build_candidate_score_inputs(
        cfg=_cfg(library_priority=50),
        src=tmp_path / "NegativeHistory.mkv",
        file_size_bytes=2 * 1024 ** 3,
        estimated_encoded_size_bytes=int(1.3 * 1024 ** 3),
        estimated_savings_percent=35.0,
        historical_avg_savings_percent=10.0,
        historical_context="resolution:1080p",
    )

    result = calculate_candidate_score(inputs)

    assert result.historical_adjustment_points == -10.0
    assert result.history_influenced is True
    assert "historically weak savings for this resolution" in result.reasons


def test_calculate_candidate_score_history_adjustment_not_applied_without_history(tmp_path: Path):
    inputs = build_candidate_score_inputs(
        cfg=_cfg(library_priority=50),
        src=tmp_path / "NoHistory.mkv",
        file_size_bytes=2 * 1024 ** 3,
        estimated_encoded_size_bytes=int(1.3 * 1024 ** 3),
        estimated_savings_percent=35.0,
    )

    result = calculate_candidate_score(inputs)

    assert result.historical_adjustment_points == 0.0
    assert result.history_influenced is False
    assert result.history_influence_reason is None
    assert "history suggests better than estimate" not in result.reasons
    assert "history suggests worse than estimate" not in result.reasons


def test_calculate_candidate_score_history_adjustment_is_bounded(tmp_path: Path):
    high = calculate_candidate_score(
        build_candidate_score_inputs(
            cfg=_cfg(),
            src=tmp_path / "BoundHigh.mkv",
            file_size_bytes=1_000,
            estimated_encoded_size_bytes=500,
            estimated_savings_percent=5.0,
            historical_avg_savings_percent=95.0,
            historical_context="library:tv",
        )
    )
    low = calculate_candidate_score(
        build_candidate_score_inputs(
            cfg=_cfg(),
            src=tmp_path / "BoundLow.mkv",
            file_size_bytes=1_000,
            estimated_encoded_size_bytes=500,
            estimated_savings_percent=95.0,
            historical_avg_savings_percent=5.0,
            historical_context="library:tv",
        )
    )

    assert high.historical_adjustment_points == 10.0
    assert low.historical_adjustment_points == -10.0


def test_calculate_candidate_score_history_adjustment_is_deterministic(tmp_path: Path):
    inputs = build_candidate_score_inputs(
        cfg=_cfg(),
        src=tmp_path / "DeterministicHistory.mkv",
        file_size_bytes=8_000,
        estimated_encoded_size_bytes=4_000,
        estimated_savings_percent=42.0,
        historical_avg_savings_percent=50.0,
        historical_context="library:movies",
    )

    first = calculate_candidate_score(inputs)
    second = calculate_candidate_score(inputs)

    assert first == second


def test_calculate_candidate_score_confidence_label_is_deterministic(tmp_path: Path):
    high_conf = calculate_candidate_score(
        build_candidate_score_inputs(
            cfg=_cfg(library_priority=80),
            src=tmp_path / "HighConfidence.mkv",
            file_size_bytes=4 * 1024 ** 3,
            estimated_encoded_size_bytes=2 * 1024 ** 3,
            estimated_savings_percent=55.0,
            historical_avg_savings_percent=52.0,
            historical_context="library:movies",
        )
    )
    medium_conf = calculate_candidate_score(
        build_candidate_score_inputs(
            cfg=_cfg(library_priority=0),
            src=tmp_path / "MediumConfidence.mkv",
            file_size_bytes=300 * 1024 ** 2,
            estimated_encoded_size_bytes=260 * 1024 ** 2,
            estimated_savings_percent=12.0,
        )
    )
    low_conf = calculate_candidate_score(
        build_candidate_score_inputs(
            cfg=_cfg(),
            src=tmp_path / "LowConfidence.mkv",
            file_size_bytes=1024,
            estimated_encoded_size_bytes=None,
            estimated_savings_percent=None,
            cached_max_savings_percent=70.0,
        )
    )

    assert high_conf.confidence_label == "high"
    assert medium_conf.confidence_label == "medium"
    assert low_conf.confidence_label == "low"
    assert -3.0 <= high_conf.confidence_adjustment_points <= 3.0
    assert -3.0 <= medium_conf.confidence_adjustment_points <= 3.0
    assert -3.0 <= low_conf.confidence_adjustment_points <= 3.0


def test_calculate_candidate_score_confidence_adjustment_is_small_and_bounded(tmp_path: Path):
    strong = calculate_candidate_score(
        build_candidate_score_inputs(
            cfg=_cfg(library_priority=120),
            src=tmp_path / "StrongSignals.mkv",
            file_size_bytes=8 * 1024 ** 3,
            estimated_encoded_size_bytes=4 * 1024 ** 3,
            estimated_savings_percent=60.0,
            historical_avg_savings_percent=55.0,
            historical_context="codec:h264",
        )
    )
    weak = calculate_candidate_score(
        build_candidate_score_inputs(
            cfg=_cfg(library_priority=120),
            src=tmp_path / "WeakSignals.mkv",
            file_size_bytes=1_000,
            estimated_encoded_size_bytes=None,
            estimated_savings_percent=None,
            cached_max_savings_percent=90.0,
        )
    )

    assert strong.confidence_adjustment_points == 3.0
    assert weak.confidence_adjustment_points == -3.0
    assert strong.score > weak.score
