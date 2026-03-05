from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from chonk_reducer.stats import record_failure, record_skip, record_success


class StubLogger:
    def log(self, msg: str) -> None:
        pass


def _cfg(tmp_path: Path):
    return SimpleNamespace(
        stats_enabled=True,
        stats_path=tmp_path / "stats.ndjson",
        version="test",
        media_root=tmp_path / "movies",
        library="movies",
        encoder="hevc_qsv",
        qsv_quality=21,
        qsv_preset=7,
    )


def test_stats_record_success_and_saved_bytes(tmp_path):
    cfg = _cfg(tmp_path)
    src = tmp_path / "movie.mkv"
    src.write_bytes(b"x")

    record_success(
        cfg,
        StubLogger(),
        run_id="r1",
        mode="live",
        stage="swap",
        src=src,
        before_bytes=1000,
        after_bytes=600,
        codec_from="h264",
        codec_to="hevc",
        duration_seconds=1.2,
    )

    row = json.loads(cfg.stats_path.read_text().splitlines()[0])
    assert row["status"] == "success"
    assert row["saved_bytes"] == 400


def test_stats_record_skip_and_failure(tmp_path):
    cfg = _cfg(tmp_path)
    src = tmp_path / "movie.mkv"
    src.write_bytes(b"x")

    record_skip(
        cfg,
        StubLogger(),
        run_id="r2",
        mode="live",
        skip_reason="min_savings",
        src=src,
        before_bytes=1000,
        detail="too small",
    )
    record_failure(
        cfg,
        StubLogger(),
        run_id="r2",
        mode="live",
        stage="encode",
        src=src,
        before_bytes=1000,
        duration_seconds=0.3,
        err=RuntimeError("boom"),
    )

    rows = [json.loads(line) for line in cfg.stats_path.read_text().splitlines()]
    assert [r["status"] for r in rows] == ["skipped", "failed"]
