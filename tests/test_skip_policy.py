from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from chonk_reducer.skip_policy import evaluate_skip


def test_skip_when_codec_matches_target():
    cfg = SimpleNamespace(skip_codecs=("hevc",), skip_min_height=0, skip_resolution_tags=())

    result = evaluate_skip(Path("movie.mkv"), {"codec": "hevc"}, cfg)

    assert result == ("codec", "codec=hevc")


def test_skip_when_resolution_tag_present():
    cfg = SimpleNamespace(skip_codecs=(), skip_min_height=0, skip_resolution_tags=("2160p",))

    result = evaluate_skip(Path("Movie.2160p.Remux.mkv"), {"codec": "h264"}, cfg)

    assert result == ("resolution", "tag=2160p")
