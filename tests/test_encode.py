from __future__ import annotations

from chonk_reducer.transcoding.encode import _encoding_progress_update, parse_ffmpeg_progress_line


def test_parse_ffmpeg_progress_line():
    assert parse_ffmpeg_progress_line("out_time_ms=123456") == ("out_time_ms", "123456")
    assert parse_ffmpeg_progress_line(" speed = 2.5x ") == ("speed", "2.5x")
    assert parse_ffmpeg_progress_line("not-a-progress-line") is None


def test_encoding_progress_update_percent_and_eta():
    snapshot = _encoding_progress_update({"out_time_ms": "5000", "speed": "2.0x"}, duration_ms=10000)

    assert snapshot["encode_percent"] == "50.0"
    assert snapshot["encode_speed"] == "2.0x"
    assert snapshot["encode_eta"] == "2"
    assert snapshot["encode_out_time"] == "5000"
