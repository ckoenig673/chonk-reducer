from __future__ import annotations

import json
import sqlite3
import types
from pathlib import Path

import chonk_reducer.runner as runner


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
        retry_backoff_secs=0,
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
