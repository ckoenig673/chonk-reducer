from __future__ import annotations

import types
from pathlib import Path

from chonk_reducer.transcoding.candidate_scoring import build_candidate_score_inputs


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
