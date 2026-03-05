from __future__ import annotations

from chonk_reducer.logging_utils import Logger


def test_logger_writes_expected_message(tmp_path, monkeypatch):
    log_file = tmp_path / "run.log"
    logger = Logger(str(log_file))

    monkeypatch.setattr("chonk_reducer.logging_utils.now_ts", lambda: "2024-01-01 00:00:00")

    logger.log("hello world")

    line = log_file.read_text().strip()
    assert line == "[2024-01-01 00:00:00] hello world"
