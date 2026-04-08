from __future__ import annotations

import json
import sqlite3
import types
from pathlib import Path

import chonk_reducer.transcoding.runner as runner
import pytest

from chonk_reducer.transcoding.run_budget import RunBudgetType, normalize_run_budget


def _base_cfg(tmp_path: Path, **overrides):
    media_root = tmp_path / "media"
    work_root = tmp_path / "work"
    media_root.mkdir(parents=True, exist_ok=True)
    work_root.mkdir(parents=True, exist_ok=True)

    cfg = types.SimpleNamespace(
        log_prefix="",
        work_root=work_root,
        media_root=media_root,
        preview=False,
        dry_run=False,
        version="test",
        stats_enabled=False,
        stats_path=work_root / "stats.ndjson",
        min_size_gb=0.0,
        max_files=10,
        min_savings_percent=0.0,
        qsv_quality=21,
        qsv_preset=7,
        post_encode_validate=False,
        validate_mode="decode",
        validate_seconds=1,
        out_uid=0,
        out_gid=0,
        out_mode=0o664,
        out_dir_mode=0o775,
        exclude_path_parts=(),
        fail_fast=False,
        log_skips=False,
        top_candidates=0,
        retry_count=0,
        retry_backoff_seconds=0,
        skip_codecs=(),
        skip_min_height=0,
        skip_resolution_tags=(),
        min_file_age_minutes=0,
        lock_stale_hours=12,
        lock_self_heal=True,
        work_cleanup_hours=0,
        log_retention_days=30,
        bak_retention_days=30,
        min_media_free_gb=0.0,
        max_gb_per_run=0.0,
        max_savings_percent=0.0,
        ffprobe_analyzeduration=1,
        ffprobe_probesize=1,
        probe_timeout_secs=1,
    )
    for k, v in overrides.items():
        setattr(cfg, k, v)
    return cfg


def test_run_initializes_logs(tmp_path, monkeypatch):
    cfg = _base_cfg(tmp_path)
    (cfg.media_root / ".chonkpause").write_text("1")

    monkeypatch.setattr(runner, "load_config", lambda: cfg)
    monkeypatch.setattr(runner, "make_run_stamp", lambda: "20240101_000000")
    monkeypatch.setattr(runner.uuid, "uuid4", lambda: types.SimpleNamespace(hex="abcd1234ef"))

    rc = runner.run()

    assert rc == 0
    run_log = cfg.work_root / "logs" / "transcode_20240101_000000.log"
    assert run_log.exists()
    assert "TRANSCODE START" in run_log.read_text()


def test_run_records_raw_log_path_for_run_detail(tmp_path, monkeypatch):
    cfg = _base_cfg(tmp_path)
    (cfg.media_root / ".chonkpause").write_text("1")

    captured = {"run_id": None, "path": None}

    monkeypatch.setattr(runner, "load_config", lambda: cfg)
    monkeypatch.setattr(runner, "make_run_stamp", lambda: "20240101_000000")
    monkeypatch.setattr(runner.uuid, "uuid4", lambda: types.SimpleNamespace(hex="abcd1234ef"))

    def fake_record_run_log_path(cfg_obj, logger, *, run_id, mode, raw_log_path):
        del cfg_obj, logger, mode
        captured["run_id"] = run_id
        captured["path"] = str(raw_log_path)

    monkeypatch.setattr(runner, "record_run_log_path", fake_record_run_log_path)

    rc = runner.run()

    assert rc == 0
    assert captured["run_id"] == "abcd1234"
    assert captured["path"].endswith("transcode_20240101_000000.log")


def test_run_respects_chonkpause_without_lock(tmp_path, monkeypatch):
    cfg = _base_cfg(tmp_path)
    (cfg.media_root / ".chonkpause").write_text("1")

    called = {"lock": False}

    monkeypatch.setattr(runner, "load_config", lambda: cfg)
    monkeypatch.setattr(runner, "acquire_lock", lambda *args, **kwargs: called.__setitem__("lock", True))

    rc = runner.run()

    assert rc == 0
    assert called["lock"] is False


def test_run_dry_run_mode_skips_encode(tmp_path, monkeypatch):
    cfg = _base_cfg(tmp_path, dry_run=True, max_files=1)
    src = cfg.media_root / "movie.mkv"
    src.write_bytes(b"x" * 5000)

    monkeypatch.setattr(runner, "load_config", lambda: cfg)
    monkeypatch.setattr(runner, "acquire_lock", lambda *a, **k: True)
    monkeypatch.setattr(runner, "release_lock", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_work_dir", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_media_temp", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_logs", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_baks", lambda *a, **k: None)
    monkeypatch.setattr(runner, "gather_candidates", lambda *a, **k: ([src], {}, []))

    called = {"encode": 0, "dry": 0}
    monkeypatch.setattr(runner, "encode_qsv", lambda *a, **k: called.__setitem__("encode", called["encode"] + 1))
    monkeypatch.setattr(runner, "record_dry_run", lambda *a, **k: called.__setitem__("dry", called["dry"] + 1))

    rc = runner.run()

    assert rc == 0
    assert called["encode"] == 0
    assert called["dry"] == 1


def test_run_dry_run_mode_processes_all_candidates_until_max_files(tmp_path, monkeypatch):
    cfg = _base_cfg(tmp_path, dry_run=True, max_files=2)
    src1 = cfg.media_root / "movie1.mkv"
    src2 = cfg.media_root / "movie2.mkv"
    src1.write_bytes(b"x" * 5000)
    src2.write_bytes(b"x" * 5000)

    monkeypatch.setattr(runner, "load_config", lambda: cfg)
    monkeypatch.setattr(runner, "acquire_lock", lambda *a, **k: True)
    monkeypatch.setattr(runner, "release_lock", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_work_dir", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_media_temp", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_logs", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_baks", lambda *a, **k: None)
    monkeypatch.setattr(runner, "gather_candidates", lambda *a, **k: ([src1, src2], {}, []))

    called = {"encode": 0, "dry": 0}
    monkeypatch.setattr(runner, "encode_qsv", lambda *a, **k: called.__setitem__("encode", called["encode"] + 1))
    monkeypatch.setattr(runner, "record_dry_run", lambda *a, **k: called.__setitem__("dry", called["dry"] + 1))

    rc = runner.run()

    assert rc == 0
    assert called["encode"] == 0
    assert called["dry"] == 2


def test_run_stops_after_max_files(tmp_path, monkeypatch):
    cfg = _base_cfg(tmp_path, max_files=1)
    src1 = cfg.media_root / "a.mkv"
    src2 = cfg.media_root / "b.mkv"
    src1.write_bytes(b"x" * 5000)
    src2.write_bytes(b"x" * 5000)

    monkeypatch.setattr(runner, "load_config", lambda: cfg)
    monkeypatch.setattr(runner, "acquire_lock", lambda *a, **k: True)
    monkeypatch.setattr(runner, "release_lock", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_work_dir", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_media_temp", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_logs", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_baks", lambda *a, **k: None)
    monkeypatch.setattr(runner, "gather_candidates", lambda *a, **k: ([src1, src2], {}, []))
    monkeypatch.setattr(runner, "evaluate_skip", lambda *a, **k: None)
    monkeypatch.setattr(runner, "validate_post_encode", lambda *a, **k: True)
    monkeypatch.setattr(runner, "probe_video_stream", lambda *a, **k: {"codec": "h264", "height": 1080, "width": 1920, "bit_rate": 1000000})
    monkeypatch.setattr(runner, "swap_in", lambda src, encoded, cfg, logger: (src.with_suffix(".bak"), src.with_suffix(".optimized")))
    monkeypatch.setattr(runner, "record_success", lambda *a, **k: None)

    calls = {"encode": 0}

    def fake_encode(src, encoded, cfg, logger):
        calls["encode"] += 1
        encoded.write_bytes(b"x" * 1000)

    monkeypatch.setattr(runner, "encode_qsv", fake_encode)

    rc = runner.run()

    assert rc == 0
    assert calls["encode"] == 1




def test_run_non_max_files_budget_type_keeps_max_files_limit_for_now(tmp_path, monkeypatch):
    cfg = _base_cfg(tmp_path, max_files=1)
    cfg.run_budget = normalize_run_budget(budget_type_raw=RunBudgetType.SCORE_CUTOFF.value, max_files=cfg.max_files)
    src1 = cfg.media_root / "a.mkv"
    src2 = cfg.media_root / "b.mkv"
    src1.write_bytes(b"x" * 5000)
    src2.write_bytes(b"x" * 5000)

    monkeypatch.setattr(runner, "load_config", lambda: cfg)
    monkeypatch.setattr(runner, "acquire_lock", lambda *a, **k: True)
    monkeypatch.setattr(runner, "release_lock", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_work_dir", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_media_temp", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_logs", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_baks", lambda *a, **k: None)
    monkeypatch.setattr(runner, "gather_candidates", lambda *a, **k: ([src1, src2], {}, []))
    monkeypatch.setattr(runner, "evaluate_skip", lambda *a, **k: None)
    monkeypatch.setattr(runner, "validate_post_encode", lambda *a, **k: True)
    monkeypatch.setattr(runner, "probe_video_stream", lambda *a, **k: {"codec": "h264", "height": 1080, "width": 1920, "bit_rate": 1000000})
    monkeypatch.setattr(runner, "swap_in", lambda src, encoded, cfg, logger: (src.with_suffix(".bak"), src.with_suffix(".optimized")))
    monkeypatch.setattr(runner, "record_success", lambda *a, **k: None)

    calls = {"encode": 0}

    def fake_encode(src, encoded, cfg, logger):
        calls["encode"] += 1
        encoded.write_bytes(b"x" * 1000)

    monkeypatch.setattr(runner, "encode_qsv", fake_encode)

    rc = runner.run()

    assert rc == 0
    assert calls["encode"] == 1


def test_run_estimated_savings_budget_selects_in_rank_order_until_target(tmp_path, monkeypatch):
    cfg = _base_cfg(tmp_path, max_files=10)
    cfg.run_budget = normalize_run_budget(
        budget_type_raw=RunBudgetType.ESTIMATED_SAVINGS_BYTES.value,
        max_files=cfg.max_files,
        budget_value_raw="1200",
    )
    src1 = cfg.media_root / "a.mkv"
    src2 = cfg.media_root / "b.mkv"
    src3 = cfg.media_root / "c.mkv"
    for src in (src1, src2, src3):
        src.write_bytes(b"x" * 5000)

    monkeypatch.setattr(runner, "load_config", lambda: cfg)
    monkeypatch.setattr(runner, "acquire_lock", lambda *a, **k: True)
    monkeypatch.setattr(runner, "release_lock", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_work_dir", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_media_temp", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_logs", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_baks", lambda *a, **k: None)
    monkeypatch.setattr(runner, "gather_candidates", lambda *a, **k: ([src1, src2, src3], {}, []))
    monkeypatch.setattr(runner, "evaluate_skip", lambda *a, **k: None)
    monkeypatch.setattr(runner, "validate_post_encode", lambda *a, **k: True)
    monkeypatch.setattr(runner, "probe_video_stream", lambda *a, **k: {"codec": "h264", "height": 1080, "width": 1920, "bit_rate": 1_000_000})
    monkeypatch.setattr(runner, "swap_in", lambda src, encoded, cfg, logger: (src.with_suffix(".bak"), src.with_suffix(".optimized")))
    monkeypatch.setattr(runner, "record_success", lambda *a, **k: None)

    monkeypatch.setattr(runner, "_rank_candidates_by_score", lambda *a, **k: ([src2, src1, src3], {}))
    savings_map = {src2: 700, src1: 600, src3: 500}
    monkeypatch.setattr(
        runner,
        "_estimate_candidate_savings_bytes_for_budget",
        lambda _cfg, src, _logger: savings_map[src],
    )

    calls = {"encode": []}

    def fake_encode(src, encoded, cfg, logger):
        calls["encode"].append(src)
        encoded.write_bytes(b"x" * 1000)

    monkeypatch.setattr(runner, "encode_qsv", fake_encode)

    rc = runner.run()

    assert rc == 0
    assert calls["encode"] == [src2, src1]


def test_run_estimated_savings_budget_excludes_missing_estimates_deterministically(tmp_path, monkeypatch):
    cfg = _base_cfg(tmp_path, max_files=10)
    cfg.run_budget = normalize_run_budget(
        budget_type_raw=RunBudgetType.ESTIMATED_SAVINGS_BYTES.value,
        max_files=cfg.max_files,
        budget_value_raw="1000",
    )
    src1 = cfg.media_root / "a.mkv"
    src2 = cfg.media_root / "b.mkv"
    src3 = cfg.media_root / "c.mkv"
    for src in (src1, src2, src3):
        src.write_bytes(b"x" * 5000)

    monkeypatch.setattr(runner, "load_config", lambda: cfg)
    monkeypatch.setattr(runner, "acquire_lock", lambda *a, **k: True)
    monkeypatch.setattr(runner, "release_lock", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_work_dir", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_media_temp", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_logs", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_baks", lambda *a, **k: None)
    monkeypatch.setattr(runner, "gather_candidates", lambda *a, **k: ([src1, src2, src3], {}, []))
    monkeypatch.setattr(runner, "evaluate_skip", lambda *a, **k: None)
    monkeypatch.setattr(runner, "validate_post_encode", lambda *a, **k: True)
    monkeypatch.setattr(runner, "probe_video_stream", lambda *a, **k: {"codec": "h264", "height": 1080, "width": 1920, "bit_rate": 1_000_000})
    monkeypatch.setattr(runner, "swap_in", lambda src, encoded, cfg, logger: (src.with_suffix(".bak"), src.with_suffix(".optimized")))
    monkeypatch.setattr(runner, "record_success", lambda *a, **k: None)

    monkeypatch.setattr(runner, "_rank_candidates_by_score", lambda *a, **k: ([src1, src2, src3], {}))
    savings_map = {src1: None, src2: 600, src3: 450}
    monkeypatch.setattr(
        runner,
        "_estimate_candidate_savings_bytes_for_budget",
        lambda _cfg, src, _logger: savings_map[src],
    )

    calls = {"encode": []}

    def fake_encode(src, encoded, cfg, logger):
        calls["encode"].append(src)
        encoded.write_bytes(b"x" * 1000)

    monkeypatch.setattr(runner, "encode_qsv", fake_encode)

    rc = runner.run()

    assert rc == 0
    assert calls["encode"] == [src2, src3]


def test_preview_run_does_not_launch_ffmpeg_and_reports_estimates(tmp_path, monkeypatch):
    cfg = _base_cfg(tmp_path, preview=True, max_files=1, min_savings_percent=10.0)
    src = cfg.media_root / "movie.mkv"
    src.write_bytes(b"x" * 10_000)

    monkeypatch.setattr(runner, "load_config", lambda: cfg)
    monkeypatch.setattr(runner, "acquire_lock", lambda *a, **k: True)
    monkeypatch.setattr(runner, "release_lock", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_work_dir", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_media_temp", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_logs", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_baks", lambda *a, **k: None)
    monkeypatch.setattr(runner, "gather_candidates", lambda *a, **k: ([src], {}, []))
    monkeypatch.setattr(runner, "probe_video_stream", lambda *a, **k: {"codec": "h264", "height": 1080, "width": 1920, "bit_rate": 8_000_000})
    monkeypatch.setattr(runner, "evaluate_skip", lambda *a, **k: None)

    calls = {"encode": 0, "snapshots": []}

    def _encode(*a, **k):
        calls["encode"] += 1

    monkeypatch.setattr(runner, "encode_qsv", _encode)

    rc = runner.run(progress_callback=lambda values: calls["snapshots"].append(values))

    assert rc == 0
    assert calls["encode"] == 0
    preview_rows = [item for item in calls["snapshots"] if "preview_result_json" in item]
    assert len(preview_rows) == 1
    parsed = json.loads(preview_rows[0]["preview_result_json"])
    assert parsed["file"].endswith("movie.mkv")
    assert parsed["estimated_size"] == 5699
    assert parsed["estimated_savings_pct"] == 43.0
    assert parsed["score_band"] in ("High value", "Medium value", "Low confidence")
    assert parsed["confidence_label"] in ("high", "medium", "low")
    assert "confidence_adjustment_points" in parsed
    assert "history_influenced" in parsed
    assert parsed["decision"] == "Encode"


def test_preview_run_marks_skip_decisions_for_codec_and_resolution(tmp_path, monkeypatch):
    cfg = _base_cfg(tmp_path, preview=True, max_files=2)
    src1 = cfg.media_root / "codec.mkv"
    src2 = cfg.media_root / "resolution.mkv"
    src1.write_bytes(b"x" * 5_000)
    src2.write_bytes(b"x" * 5_000)

    monkeypatch.setattr(runner, "load_config", lambda: cfg)
    monkeypatch.setattr(runner, "acquire_lock", lambda *a, **k: True)
    monkeypatch.setattr(runner, "release_lock", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_work_dir", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_media_temp", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_logs", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_baks", lambda *a, **k: None)
    monkeypatch.setattr(runner, "gather_candidates", lambda *a, **k: ([src1, src2], {}, []))
    monkeypatch.setattr(runner, "probe_video_stream", lambda *a, **k: {"codec": "h264", "height": 1080, "width": 1920, "bit_rate": 1_000_000})

    def _skip(src, *_args, **_kwargs):
        if src == src1:
            return ("codec", "codec=h264")
        return ("resolution", "height=1080")

    monkeypatch.setattr(runner, "evaluate_skip", _skip)

    snapshots = []
    rc = runner.run(progress_callback=lambda values: snapshots.append(values))

    assert rc == 0
    rows = [json.loads(item["preview_result_json"]) for item in snapshots if "preview_result_json" in item]
    assert rows[0]["decision"] == "Skip (unsupported codec)"
    assert rows[1]["decision"] == "Skip (resolution rules)"
    assert "score" in rows[0]
    assert "score_band" in rows[0]


def test_preview_estimated_savings_budget_only_emits_selected_candidates(tmp_path, monkeypatch):
    cfg = _base_cfg(tmp_path, preview=True, max_files=10)
    cfg.run_budget = normalize_run_budget(
        budget_type_raw=RunBudgetType.ESTIMATED_SAVINGS_BYTES.value,
        max_files=cfg.max_files,
        budget_value_raw="800",
    )
    src1 = cfg.media_root / "a.mkv"
    src2 = cfg.media_root / "b.mkv"
    src3 = cfg.media_root / "c.mkv"
    for src in (src1, src2, src3):
        src.write_bytes(b"x" * 10_000)

    monkeypatch.setattr(runner, "load_config", lambda: cfg)
    monkeypatch.setattr(runner, "acquire_lock", lambda *a, **k: True)
    monkeypatch.setattr(runner, "release_lock", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_work_dir", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_media_temp", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_logs", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_baks", lambda *a, **k: None)
    monkeypatch.setattr(runner, "gather_candidates", lambda *a, **k: ([src1, src2, src3], {}, []))
    monkeypatch.setattr(runner, "_rank_candidates_by_score", lambda *a, **k: ([src1, src2, src3], {}))
    monkeypatch.setattr(runner, "evaluate_skip", lambda *a, **k: None)
    monkeypatch.setattr(runner, "probe_video_stream", lambda *a, **k: {"codec": "h264", "height": 1080, "width": 1920, "bit_rate": 1_000_000})

    savings_map = {src1: 500, src2: 400, src3: 350}
    monkeypatch.setattr(
        runner,
        "_estimate_candidate_savings_bytes_for_budget",
        lambda _cfg, src, _logger: savings_map[src],
    )

    snapshots = []
    rc = runner.run(progress_callback=lambda values: snapshots.append(values))

    assert rc == 0
    rows = [json.loads(item["preview_result_json"]) for item in snapshots if "preview_result_json" in item]
    assert [Path(row["file"]) for row in rows] == [src1, src2]
    assert "score_reasons" in rows[0]
    assert "confidence_label" in rows[0]
    assert "history_influenced" in rows[0]


def test_run_ranks_candidates_by_score_descending(tmp_path, monkeypatch):
    cfg = _base_cfg(tmp_path, dry_run=True, max_files=2)
    low = cfg.media_root / "low.mkv"
    high = cfg.media_root / "high.mkv"
    low.write_bytes(b"x" * 5_000)
    high.write_bytes(b"x" * 5_000)

    monkeypatch.setattr(runner, "load_config", lambda: cfg)
    monkeypatch.setattr(runner, "acquire_lock", lambda *a, **k: True)
    monkeypatch.setattr(runner, "release_lock", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_work_dir", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_media_temp", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_logs", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_baks", lambda *a, **k: None)
    monkeypatch.setattr(runner, "gather_candidates", lambda *a, **k: ([low, high], {}, []))
    monkeypatch.setattr(runner, "build_candidate_score_inputs", lambda **kwargs: kwargs["src"])
    monkeypatch.setattr(
        runner,
        "calculate_candidate_score",
        lambda src: types.SimpleNamespace(
            score=100.0 if src == high else 5.0,
            reasons=("test",),
        ),
    )

    seen_files: list[str] = []
    rc = runner.run(
        progress_callback=lambda values: seen_files.append(values["current_file"])
        if values.get("current_file")
        else None
    )

    assert rc == 0
    assert seen_files[:2] == [str(high), str(low)]


def test_run_ranking_ties_keep_existing_order(tmp_path, monkeypatch):
    cfg = _base_cfg(tmp_path, dry_run=True, max_files=2)
    first = cfg.media_root / "first.mkv"
    second = cfg.media_root / "second.mkv"
    first.write_bytes(b"x" * 5_000)
    second.write_bytes(b"x" * 5_000)

    monkeypatch.setattr(runner, "load_config", lambda: cfg)
    monkeypatch.setattr(runner, "acquire_lock", lambda *a, **k: True)
    monkeypatch.setattr(runner, "release_lock", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_work_dir", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_media_temp", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_logs", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_baks", lambda *a, **k: None)
    monkeypatch.setattr(runner, "gather_candidates", lambda *a, **k: ([first, second], {}, []))
    monkeypatch.setattr(runner, "build_candidate_score_inputs", lambda **kwargs: kwargs["src"])
    monkeypatch.setattr(
        runner,
        "calculate_candidate_score",
        lambda _src: types.SimpleNamespace(score=25.0, reasons=("tied",)),
    )

    seen_files: list[str] = []
    rc = runner.run(
        progress_callback=lambda values: seen_files.append(values["current_file"])
        if values.get("current_file")
        else None
    )

    assert rc == 0
    assert seen_files[:2] == [str(first), str(second)]


def test_run_skip_eligibility_rules_unchanged_with_ranking(tmp_path, monkeypatch):
    cfg = _base_cfg(tmp_path, max_files=2)
    skipped = cfg.media_root / "skip-me.mkv"
    encoded = cfg.media_root / "encode-me.mkv"
    skipped.write_bytes(b"x" * 8_000)
    encoded.write_bytes(b"x" * 6_000)

    monkeypatch.setattr(runner, "load_config", lambda: cfg)
    monkeypatch.setattr(runner, "acquire_lock", lambda *a, **k: True)
    monkeypatch.setattr(runner, "release_lock", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_work_dir", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_media_temp", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_logs", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_baks", lambda *a, **k: None)
    monkeypatch.setattr(runner, "gather_candidates", lambda *a, **k: ([skipped, encoded], {}, []))
    monkeypatch.setattr(runner, "build_candidate_score_inputs", lambda **kwargs: kwargs["src"])
    monkeypatch.setattr(
        runner,
        "calculate_candidate_score",
        lambda src: types.SimpleNamespace(score=99.0 if src == skipped else 1.0, reasons=("rank",)),
    )
    monkeypatch.setattr(runner, "probe_video_stream", lambda *a, **k: {"codec": "h264", "height": 1080, "width": 1920, "bit_rate": 1_000_000})
    monkeypatch.setattr(runner, "validate_post_encode", lambda *a, **k: True)
    monkeypatch.setattr(runner, "swap_in", lambda src, encoded_path, cfg_obj, logger: (src.with_suffix(".bak"), src.with_suffix(".optimized")))
    monkeypatch.setattr(runner, "record_success", lambda *a, **k: None)
    monkeypatch.setattr(runner, "evaluate_skip", lambda src, *_a, **_k: ("codec", "codec=h264") if src == skipped else None)

    encoded_calls = {"count": 0}

    def _encode(src, encoded_path, cfg_obj, logger):
        del cfg_obj, logger
        encoded_calls["count"] += 1
        encoded_path.write_bytes(b"x" * 1_000)

    monkeypatch.setattr(runner, "encode_qsv", _encode)

    rc = runner.run()

    assert rc == 0
    assert encoded_calls["count"] == 1


def test_select_historical_signal_uses_priority_codec_then_resolution_then_library(tmp_path):
    src = tmp_path / "Movie.1080p.mkv"
    summaries = {
        "by_codec": [{"codec": "h264", "avg_savings_pct": 41.0}],
        "by_resolution_bucket": [{"resolution_bucket": "1080p", "avg_savings_pct": 32.0}],
        "by_library": [{"library": "tv", "avg_savings_pct": 28.0}],
    }

    codec_pick = runner._select_historical_signal(
        history_summaries=summaries,
        src=src,
        before_probe={"codec": "h264", "height": 1080},
        library_name="tv",
    )
    assert codec_pick == (41.0, "codec:h264")

    resolution_pick = runner._select_historical_signal(
        history_summaries=summaries,
        src=src,
        before_probe={"codec": "mpeg2video", "height": 1080},
        library_name="tv",
    )
    assert resolution_pick == (32.0, "resolution:1080p")

    library_pick = runner._select_historical_signal(
        history_summaries=summaries,
        src=tmp_path / "Movie.Unknown.mkv",
        before_probe={"codec": "mpeg2video", "height": 240},
        library_name="tv",
    )
    assert library_pick == (28.0, "library:tv")


def test_preview_payload_marks_history_influence_when_applied(tmp_path, monkeypatch):
    cfg = _base_cfg(tmp_path, preview=True, max_files=1, stats_enabled=True, stats_path=tmp_path / "chonk.db")
    src = cfg.media_root / "history-influence.mkv"
    src.write_bytes(b"x" * 10_000)

    monkeypatch.setattr(runner, "load_config", lambda: cfg)
    monkeypatch.setattr(runner, "acquire_lock", lambda *a, **k: True)
    monkeypatch.setattr(runner, "release_lock", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_work_dir", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_media_temp", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_logs", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_baks", lambda *a, **k: None)
    monkeypatch.setattr(runner, "gather_candidates", lambda *a, **k: ([src], {}, []))
    monkeypatch.setattr(
        runner,
        "probe_video_stream",
        lambda *a, **k: {"codec": "h264", "height": 1080, "width": 1920, "bit_rate": 8_000_000},
    )
    monkeypatch.setattr(runner, "evaluate_skip", lambda *a, **k: None)
    monkeypatch.setattr(
        runner,
        "get_history_summaries",
        lambda *_a, **_k: {"by_codec": [{"codec": "h264", "avg_savings_pct": 80.0}]},
    )

    snapshots = []
    rc = runner.run(progress_callback=lambda values: snapshots.append(values))

    assert rc == 0
    rows = [json.loads(item["preview_result_json"]) for item in snapshots if "preview_result_json" in item]
    assert len(rows) == 1
    assert rows[0]["history_influenced"] is True
    assert rows[0]["history_influence_reason"] == "history-influenced score"


def test_run_exits_when_free_space_too_low(tmp_path, monkeypatch):
    cfg = _base_cfg(tmp_path, min_media_free_gb=10)

    monkeypatch.setattr(runner, "load_config", lambda: cfg)
    monkeypatch.setattr(runner, "acquire_lock", lambda *a, **k: True)
    monkeypatch.setattr(runner, "release_lock", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_work_dir", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_media_temp", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_logs", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_baks", lambda *a, **k: None)
    monkeypatch.setattr(runner.shutil, "disk_usage", lambda *_: types.SimpleNamespace(free=1024))

    rc = runner.run()

    assert rc == 2


def test_run_migrates_legacy_stats_with_zero_candidates(tmp_path, monkeypatch):
    cfg = _base_cfg(tmp_path, stats_enabled=True, stats_path=tmp_path / "chonk.db")
    legacy = cfg.media_root / ".chonkstats.ndjson"
    legacy.parent.mkdir(parents=True, exist_ok=True)
    legacy_row = {
        "ts": "2026-01-01T00:00:01",
        "run_id": "run-zero-cands",
        "version": "test",
        "library": "movies",
        "mode": "live",
        "encoder": "hevc_qsv",
        "quality": 21,
        "preset": 7,
        "status": "success",
        "stage": "swap",
        "path": "/movies/a.mkv",
        "filename": "a.mkv",
        "size_before_bytes": 100,
        "size_after_bytes": 60,
        "saved_bytes": 40,
        "saved_pct": 40.0,
        "duration_seconds": 1.2,
    }
    legacy.write_text(json.dumps(legacy_row) + "\n", encoding="utf-8")

    monkeypatch.setattr(runner, "load_config", lambda: cfg)
    monkeypatch.setattr(runner, "acquire_lock", lambda *a, **k: True)
    monkeypatch.setattr(runner, "release_lock", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_work_dir", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_media_temp", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_logs", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_baks", lambda *a, **k: None)
    monkeypatch.setattr(runner, "gather_candidates", lambda *a, **k: ([], {}, []))

    rc = runner.run()

    assert rc == 0
    migrated = legacy.with_suffix(legacy.suffix + ".migrated")
    assert migrated.exists()
    assert not legacy.exists()

    conn = sqlite3.connect(str(cfg.stats_path))
    count = conn.execute("SELECT COUNT(*) FROM encodes").fetchone()[0]
    conn.close()
    assert count == 1


def test_run_persists_run_summary_counters(tmp_path, monkeypatch):
    cfg = _base_cfg(tmp_path, dry_run=True, max_files=1, stats_enabled=True, stats_path=tmp_path / "chonk.db")
    src = cfg.media_root / "movie.mkv"
    src.write_bytes(b"x" * 5000)

    monkeypatch.setattr(runner, "load_config", lambda: cfg)
    monkeypatch.setattr(runner, "acquire_lock", lambda *a, **k: True)
    monkeypatch.setattr(runner, "release_lock", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_work_dir", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_media_temp", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_logs", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_baks", lambda *a, **k: None)
    monkeypatch.setattr(runner, "gather_candidates", lambda *a, **k: ([src], {}, []))

    rc = runner.run()

    assert rc == 0
    conn = sqlite3.connect(str(cfg.stats_path))
    row = conn.execute(
        """
        SELECT
            candidates_found,
            prefiltered_count,
            evaluated_count,
            processed_count,
            prefiltered_marker_count,
            prefiltered_backup_count,
            skipped_codec_count,
            skipped_resolution_count,
            skipped_min_savings_count,
            skipped_max_savings_count,
            skipped_dry_run_count,
            ignored_folder_count,
            ignored_file_count
        FROM runs
        ORDER BY ts_start DESC
        LIMIT 1
        """
    ).fetchone()
    conn.close()

    assert row == (1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0)

def test_run_reports_processed_only_after_successful_encode(tmp_path, monkeypatch):
    cfg = _base_cfg(tmp_path, max_files=1)
    src = cfg.media_root / "movie.mkv"
    src.write_bytes(b"x" * 5000)

    monkeypatch.setattr(runner, "load_config", lambda: cfg)
    monkeypatch.setattr(runner, "acquire_lock", lambda *a, **k: True)
    monkeypatch.setattr(runner, "release_lock", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_work_dir", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_media_temp", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_logs", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_baks", lambda *a, **k: None)
    monkeypatch.setattr(runner, "gather_candidates", lambda *a, **k: ([src], {}, []))
    monkeypatch.setattr(runner, "evaluate_skip", lambda *a, **k: None)
    monkeypatch.setattr(runner, "validate_post_encode", lambda *a, **k: True)
    monkeypatch.setattr(runner, "probe_video_stream", lambda *a, **k: {"codec": "h264", "height": 1080, "width": 1920, "bit_rate": 1000000})
    monkeypatch.setattr(runner, "swap_in", lambda src, encoded, cfg, logger: (src.with_suffix(".bak"), src.with_suffix(".optimized")))
    monkeypatch.setattr(runner, "record_success", lambda *a, **k: None)

    snapshots = []

    def progress_callback(values):
        snapshots.append(dict(values))

    def fake_encode(src, encoded, cfg, logger):
        del src, cfg, logger
        encoded.write_bytes(b"x" * 1000)
        assert max(int(s.get("files_processed", 0)) for s in snapshots) == 0

    monkeypatch.setattr(runner, "encode_qsv", fake_encode)

    rc = runner.run(progress_callback=progress_callback)

    assert rc == 0
    assert any(s.get("current_file") == str(src) and int(s.get("files_evaluated", 0)) == 1 for s in snapshots)
    assert any(int(s.get("files_processed", 0)) == 1 for s in snapshots)


def test_run_progress_callback_reports_encode_runtime_fields(tmp_path, monkeypatch):
    cfg = _base_cfg(tmp_path, max_files=1)
    src = cfg.media_root / "movie.mkv"
    src.write_bytes(b"x" * 5000)

    monkeypatch.setattr(runner, "load_config", lambda: cfg)
    monkeypatch.setattr(runner, "acquire_lock", lambda *a, **k: True)
    monkeypatch.setattr(runner, "release_lock", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_work_dir", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_media_temp", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_logs", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_baks", lambda *a, **k: None)
    monkeypatch.setattr(runner, "gather_candidates", lambda *a, **k: ([src], {}, []))
    monkeypatch.setattr(runner, "evaluate_skip", lambda *a, **k: None)
    monkeypatch.setattr(runner, "validate_post_encode", lambda *a, **k: True)
    monkeypatch.setattr(runner, "probe_video_stream", lambda *a, **k: {"codec": "h264", "height": 1080, "width": 1920, "bit_rate": 1000000})
    monkeypatch.setattr(runner, "swap_in", lambda src, encoded, cfg, logger: (src.with_suffix(".bak"), src.with_suffix(".optimized")))
    monkeypatch.setattr(runner, "record_success", lambda *a, **k: None)

    def fake_encode(src, encoded, cfg, logger, progress_callback=None, **kwargs):
        del src, cfg, logger, kwargs
        encoded.write_bytes(b"x" * 1000)
        if progress_callback is not None:
            progress_callback(encode_percent="62.5", encode_speed="3.2x", encode_eta="102", encode_out_time="12345678")

    monkeypatch.setattr(runner, "encode_qsv", fake_encode)

    snapshots = []
    rc = runner.run(progress_callback=lambda values: snapshots.append(dict(values)))

    assert rc == 0
    assert any(s.get("encode_percent") == "62.5" for s in snapshots)
    assert any(s.get("encode_speed") == "3.2x" for s in snapshots)
    assert any(s.get("encode_eta") == "102" for s in snapshots)


def test_run_stops_processing_after_cancel_requested(tmp_path, monkeypatch):
    cfg = _base_cfg(tmp_path, max_files=5)
    src1 = cfg.media_root / "a.mkv"
    src2 = cfg.media_root / "b.mkv"
    src1.write_bytes(b"x" * 5000)
    src2.write_bytes(b"x" * 5000)

    monkeypatch.setattr(runner, "load_config", lambda: cfg)
    monkeypatch.setattr(runner, "acquire_lock", lambda *a, **k: True)
    monkeypatch.setattr(runner, "release_lock", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_work_dir", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_media_temp", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_logs", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_baks", lambda *a, **k: None)
    monkeypatch.setattr(runner, "gather_candidates", lambda *a, **k: ([src1, src2], {}, []))
    monkeypatch.setattr(runner, "evaluate_skip", lambda *a, **k: None)
    monkeypatch.setattr(runner, "validate_post_encode", lambda *a, **k: True)
    monkeypatch.setattr(runner, "probe_video_stream", lambda *a, **k: {"codec": "h264", "height": 1080, "width": 1920, "bit_rate": 1000000})
    monkeypatch.setattr(runner, "swap_in", lambda src, encoded, cfg, logger: (src.with_suffix(".bak"), src.with_suffix(".optimized")))
    monkeypatch.setattr(runner, "record_success", lambda *a, **k: None)

    state = {"cancel": False, "calls": 0}

    def fake_encode(src, encoded, cfg, logger, **kwargs):
        del src, cfg, logger, kwargs
        state["calls"] += 1
        encoded.write_bytes(b"x" * 1000)
        state["cancel"] = True

    monkeypatch.setattr(runner, "encode_qsv", fake_encode)

    rc = runner.run(cancel_requested=lambda: state["cancel"])

    assert rc == 0
    assert state["calls"] == 1



def test_run_retries_failed_encode_until_success(tmp_path, monkeypatch):
    cfg = _base_cfg(tmp_path, max_files=1, retry_count=2, retry_backoff_seconds=3)
    src = cfg.media_root / "movie.mkv"
    src.write_bytes(b"x" * 5000)

    monkeypatch.setattr(runner, "load_config", lambda: cfg)
    monkeypatch.setattr(runner, "acquire_lock", lambda *a, **k: True)
    monkeypatch.setattr(runner, "release_lock", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_work_dir", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_media_temp", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_logs", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_baks", lambda *a, **k: None)
    monkeypatch.setattr(runner, "gather_candidates", lambda *a, **k: ([src], {}, []))
    monkeypatch.setattr(runner, "evaluate_skip", lambda *a, **k: None)
    monkeypatch.setattr(runner, "validate_post_encode", lambda *a, **k: True)
    monkeypatch.setattr(runner, "probe_video_stream", lambda *a, **k: {"codec": "h264", "height": 1080, "width": 1920, "bit_rate": 1000000})
    monkeypatch.setattr(runner, "swap_in", lambda src, encoded, cfg, logger: (src.with_suffix(".bak"), src.with_suffix(".optimized")))

    sleep_calls = []
    monkeypatch.setattr(runner.time, "sleep", lambda seconds: sleep_calls.append(seconds))

    attempts = {"count": 0, "success": 0, "failure": 0}
    monkeypatch.setattr(runner, "record_success", lambda *a, **k: attempts.__setitem__("success", attempts["success"] + 1))
    monkeypatch.setattr(runner, "record_failure", lambda *a, **k: attempts.__setitem__("failure", attempts["failure"] + 1))

    def fake_encode(src, encoded, cfg, logger, **kwargs):
        del src, cfg, logger, kwargs
        attempts["count"] += 1
        if attempts["count"] < 2:
            raise RuntimeError("transient ffmpeg failure")
        encoded.write_bytes(b"x" * 1000)

    monkeypatch.setattr(runner, "encode_qsv", fake_encode)

    snapshots = []
    rc = runner.run(progress_callback=lambda values: snapshots.append(dict(values)))

    assert rc == 0
    assert attempts["count"] == 2
    assert attempts["success"] == 1
    assert attempts["failure"] == 0
    assert sleep_calls == [3]
    assert any(s.get("retry_attempt") == 1 and s.get("retry_max") == 2 for s in snapshots)


def test_run_marks_file_failed_after_retry_exhaustion(tmp_path, monkeypatch):
    cfg = _base_cfg(tmp_path, max_files=1, retry_count=2, retry_backoff_seconds=0)
    src = cfg.media_root / "movie.mkv"
    src.write_bytes(b"x" * 5000)

    monkeypatch.setattr(runner, "load_config", lambda: cfg)
    monkeypatch.setattr(runner, "acquire_lock", lambda *a, **k: True)
    monkeypatch.setattr(runner, "release_lock", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_work_dir", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_media_temp", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_logs", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_baks", lambda *a, **k: None)
    monkeypatch.setattr(runner, "gather_candidates", lambda *a, **k: ([src], {}, []))
    monkeypatch.setattr(runner, "evaluate_skip", lambda *a, **k: None)
    monkeypatch.setattr(runner, "validate_post_encode", lambda *a, **k: True)
    monkeypatch.setattr(runner, "probe_video_stream", lambda *a, **k: {"codec": "h264", "height": 1080, "width": 1920, "bit_rate": 1000000})

    failure_calls = {"count": 0}
    monkeypatch.setattr(runner, "record_failure", lambda *a, **k: failure_calls.__setitem__("count", failure_calls["count"] + 1))
    monkeypatch.setattr(runner, "record_success", lambda *a, **k: None)

    attempts = {"count": 0}

    def always_fail(*args, **kwargs):
        attempts["count"] += 1
        raise RuntimeError("hard failure")

    monkeypatch.setattr(runner, "encode_qsv", always_fail)

    rc = runner.run()

    assert rc == 2
    assert attempts["count"] == 3
    assert failure_calls["count"] == 1
    assert src.with_suffix(src.suffix + ".failed").exists()


def test_max_savings_skip_is_cached_and_reused_until_threshold_increases(tmp_path, monkeypatch, caplog):
    db_path = tmp_path / "chonk.db"
    cfg = _base_cfg(
        tmp_path,
        max_files=1,
        stats_enabled=True,
        stats_path=db_path,
        max_savings_percent=65.0,
        min_savings_percent=1.0,
    )
    src = cfg.media_root / "movie.mkv"
    src.write_bytes(b"x" * 10_000)

    monkeypatch.setattr(runner, "load_config", lambda: cfg)
    monkeypatch.setattr(runner, "acquire_lock", lambda *a, **k: True)
    monkeypatch.setattr(runner, "release_lock", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_work_dir", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_media_temp", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_logs", lambda *a, **k: None)
    monkeypatch.setattr(runner, "cleanup_baks", lambda *a, **k: None)
    monkeypatch.setattr(runner, "gather_candidates", lambda *a, **k: ([src], {}, []))
    monkeypatch.setattr(runner, "probe_video_stream", lambda *a, **k: {"codec": "h264", "height": 1080, "width": 1920, "bit_rate": 8_000_000})
    monkeypatch.setattr(runner, "evaluate_skip", lambda *a, **k: None)
    monkeypatch.setattr(runner, "validate_post_encode", lambda *a, **k: True)

    calls = {"encode": 0}

    def fake_encode(_src, encoded, *_args, **_kwargs):
        calls["encode"] += 1
        # 67.6% savings => skipped when max_savings_percent is 65.0
        encoded.write_bytes(b"x" * 3_240)

    monkeypatch.setattr(runner, "encode_qsv", fake_encode)

    caplog.set_level("INFO", logger="chonk_reducer.runner")

    assert runner.run() == 0
    assert calls["encode"] == 1

    conn = sqlite3.connect(str(db_path))
    row = conn.execute(
        "SELECT path, library, skip_reason, savings_percent FROM policy_skip_cache"
    ).fetchone()
    conn.close()
    assert row is not None
    assert row[0] == str(src)
    assert row[2] == "max_savings"
    assert float(row[3]) == pytest.approx(67.6, abs=0.001)

    # Same threshold: skip from cache, no re-evaluation.
    assert runner.run() == 0
    assert calls["encode"] == 1

    reused_messages = [
        rec.getMessage()
        for rec in caplog.records
        if "Skipping cached policy decision:" in rec.getMessage()
    ]
    assert len(reused_messages) == 1

    # Lower threshold: still skip from cache, no re-evaluation.
    cfg.max_savings_percent = 60.0
    assert runner.run() == 0
    assert calls["encode"] == 1

    reused_messages = [
        rec.getMessage()
        for rec in caplog.records
        if "Skipping cached policy decision:" in rec.getMessage()
    ]
    assert len(reused_messages) == 2

    # Raise threshold above cached measurement: allow re-evaluation.
    cfg.max_savings_percent = 70.0
    assert runner.run() == 0
    assert calls["encode"] == 2

    cached_messages = [
        rec.getMessage()
        for rec in caplog.records
        if "Caching policy skip:" in rec.getMessage()
    ]
    assert len(cached_messages) == 1
    assert "file='movie'" in cached_messages[0]
    assert "reason='max_savings'" in cached_messages[0]
    assert "savings=67.6%" in cached_messages[0]

    assert "file='movie'" in reused_messages[0]
    assert "reason='max_savings'" in reused_messages[0]
    assert "stored_savings=67.6%" in reused_messages[0]
    assert "threshold=65.0%" in reused_messages[0]
