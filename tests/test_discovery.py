from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from chonk_reducer.discovery import gather_candidates


class StubLogger:
    def __init__(self):
        self.messages: list[str] = []

    def log(self, msg: str) -> None:
        self.messages.append(msg)


def test_discovery_skips_optimized_and_backup_files(tmp_path):
    media = tmp_path / "media"
    media.mkdir()

    large = media / "movie.mkv"
    large.write_bytes(b"x" * 3000)

    optimized = media / "movie2.encoded.mkv"
    optimized.write_bytes(b"x" * 3000)

    backup_like = media / "movie3.bak.20240101.mkv"
    backup_like.write_bytes(b"x" * 3000)

    cfg = SimpleNamespace(media_root=media, min_size_gb=0, exclude_path_parts=())
    logger = StubLogger()

    candidates, _, recent_skipped = gather_candidates(cfg, logger)

    assert large in candidates
    assert optimized not in candidates
    assert backup_like not in candidates
    assert recent_skipped == []


def test_discovery_respects_ignore_folders_and_size_threshold(tmp_path):
    media = tmp_path / "media"
    ignored = media / "show"
    ignored.mkdir(parents=True)
    (ignored / ".chonkignore").write_text("1")

    ignored_file = ignored / "ep1.mkv"
    ignored_file.write_bytes(b"x" * 6000)

    small = media / "small.mkv"
    small.parent.mkdir(parents=True, exist_ok=True)
    small.write_bytes(b"x" * 100)

    large = media / "big.mkv"
    large.write_bytes(b"x" * 6000)

    cfg = SimpleNamespace(
        media_root=media,
        min_size_gb=0.000001,  # ~1KB
        exclude_path_parts=(),
    )
    logger = StubLogger()

    candidates, ignored_folders, recent_skipped = gather_candidates(cfg, logger)

    assert large in candidates
    assert small not in candidates
    assert ignored_file not in candidates
    assert ignored in ignored_folders
    assert recent_skipped == []


def test_discovery_skips_recently_modified_files(tmp_path):
    media = tmp_path / "media"
    media.mkdir()

    old_file = media / "old.mkv"
    old_file.write_bytes(b"x" * 5000)

    new_file = media / "new.mkv"
    new_file.write_bytes(b"x" * 5000)

    import time
    old_ts = time.time() - (15 * 60)
    new_ts = time.time() - (2 * 60)
    import os
    os.utime(old_file, (old_ts, old_ts))
    os.utime(new_file, (new_ts, new_ts))

    cfg = SimpleNamespace(media_root=media, min_size_gb=0, exclude_path_parts=(), min_file_age_minutes=10)
    logger = StubLogger()

    candidates, _, recent_skipped = gather_candidates(cfg, logger)

    assert old_file in candidates
    assert new_file not in candidates
    assert any(p == new_file for p, _ in recent_skipped)


def test_discovery_min_file_age_zero_disables_recent_skip(tmp_path):
    media = tmp_path / "media"
    media.mkdir()

    new_file = media / "new.mkv"
    new_file.write_bytes(b"x" * 5000)

    cfg = SimpleNamespace(media_root=media, min_size_gb=0, exclude_path_parts=(), min_file_age_minutes=0)
    logger = StubLogger()

    candidates, _, recent_skipped = gather_candidates(cfg, logger)

    assert new_file in candidates
    assert recent_skipped == []
