from __future__ import annotations

import sqlite3

from chonk_reducer import notifications


def _seed_settings(db_path, values):
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)")
    for key, value in values.items():
        conn.execute(
            "INSERT INTO settings(key, value, updated_at) VALUES (?, ?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, str(value), "2026-01-01T00:00:00"),
        )
    conn.commit()
    conn.close()


def test_send_run_complete_builds_discord_and_generic_payloads(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    _seed_settings(
        db_path,
        {
            "discord_webhook_url": "https://discord.example/hook",
            "generic_webhook_url": "https://generic.example/hook",
            "enable_run_complete_notifications": "1",
        },
    )

    calls = []

    def fake_post(url, payload):
        calls.append((url, payload))

    monkeypatch.setattr(notifications, "_post_json", fake_post)

    notifications.send_run_complete(
        {
            "library": "Movies",
            "run_id": "run-123",
            "files_scanned": 10,
            "files_optimized": 3,
            "total_space_saved": "18.2 GB",
            "duration": "14.0s",
            "host": "nas-01",
        },
        settings_db_path=str(db_path),
    )

    assert len(calls) == 2
    assert calls[0][0] == "https://discord.example/hook"
    assert "Chonk Reducer Run Complete" in calls[0][1]["content"]
    assert "Library: Movies" in calls[0][1]["content"]

    assert calls[1][0] == "https://generic.example/hook"
    assert calls[1][1]["event"] == "run_complete"
    assert calls[1][1]["run_id"] == "run-123"
    assert calls[1][1]["files_scanned"] == 10


def test_notifications_are_skipped_when_disabled(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    _seed_settings(
        db_path,
        {
            "discord_webhook_url": "https://discord.example/hook",
            "generic_webhook_url": "https://generic.example/hook",
            "enable_run_complete_notifications": "0",
            "enable_run_failure_notifications": "0",
        },
    )

    called = []

    def fake_post(url, payload):
        called.append((url, payload))

    monkeypatch.setattr(notifications, "_post_json", fake_post)

    notifications.send_run_complete({"library": "Movies", "run_id": "x"}, settings_db_path=str(db_path))
    notifications.send_run_failure(
        {"library": "Movies", "run_id": "x", "error_message": "boom"},
        settings_db_path=str(db_path),
    )

    assert called == []


def test_send_run_failure_swallows_webhook_errors(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    _seed_settings(
        db_path,
        {
            "discord_webhook_url": "https://discord.example/hook",
            "enable_run_failure_notifications": "1",
        },
    )

    def fake_post(url, payload):
        raise RuntimeError("webhook down")

    monkeypatch.setattr(notifications, "_post_json", fake_post)

    notifications.send_run_failure(
        {
            "library": "Movies",
            "run_id": "run-123",
            "error_message": "Encoding crashed",
            "host": "nas-01",
        },
        settings_db_path=str(db_path),
    )


def test_build_run_complete_summary_uses_run_row_values():
    class RowStub(dict):
        def keys(self):
            return super().keys()

    row = RowStub(candidates_found=21, success_count=7, saved_bytes=1024 * 1024 * 5, duration_seconds=12.4)

    summary = notifications.build_run_complete_summary("Movies", "run-abc", row=row)

    assert summary["library"] == "Movies"
    assert summary["run_id"] == "run-abc"
    assert summary["files_scanned"] == 21
    assert summary["files_optimized"] == 7
    assert summary["total_space_saved"] == "5.0 MB"
    assert summary["duration"] == "12.4s"
