from __future__ import annotations

import sqlite3

from chonk_reducer import notifications
from chonk_reducer import secrets


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
    monkeypatch.setenv(secrets.SECRET_ENV_VAR, "test-secret-key-123")
    db_path = tmp_path / "chonk.db"
    _seed_settings(
        db_path,
        {
            "discord_webhook_url": secrets.encrypt_secret("https://discord.example/hook"),
            "generic_webhook_url": secrets.encrypt_secret("https://generic.example/hook"),
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
    monkeypatch.setenv(secrets.SECRET_ENV_VAR, "test-secret-key-123")
    db_path = tmp_path / "chonk.db"
    _seed_settings(
        db_path,
        {
            "discord_webhook_url": secrets.encrypt_secret("https://discord.example/hook"),
            "generic_webhook_url": secrets.encrypt_secret("https://generic.example/hook"),
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
    monkeypatch.setenv(secrets.SECRET_ENV_VAR, "test-secret-key-123")
    db_path = tmp_path / "chonk.db"
    _seed_settings(
        db_path,
        {
            "discord_webhook_url": secrets.encrypt_secret("https://discord.example/hook"),
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


def test_load_settings_handles_missing_secret_key_for_encrypted_values(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv(secrets.SECRET_ENV_VAR, "test-secret-key-123")
    encrypted = secrets.encrypt_secret("https://discord.example/hook")
    monkeypatch.delenv(secrets.SECRET_ENV_VAR, raising=False)
    _seed_settings(db_path, {"discord_webhook_url": encrypted})

    settings = notifications._load_settings(str(db_path))

    assert settings["discord_webhook_url"] == ""


def test_send_test_notification_returns_success_when_configured(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv(secrets.SECRET_ENV_VAR, "test-secret-key-123")
    _seed_settings(
        db_path,
        {
            "discord_webhook_url": secrets.encrypt_secret("https://discord.example/hook"),
            "generic_webhook_url": secrets.encrypt_secret("https://generic.example/hook"),
        },
    )
    calls = []

    def fake_post(url, payload):
        calls.append((url, payload))

    monkeypatch.setattr(notifications, "_post_json", fake_post)

    result = notifications.send_test_notification(settings_db_path=str(db_path))

    assert result["ok"] is True
    assert len(calls) == 2


def test_send_test_notification_failure_is_non_fatal(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv(secrets.SECRET_ENV_VAR, "test-secret-key-123")
    _seed_settings(
        db_path,
        {
            "discord_webhook_url": secrets.encrypt_secret("https://discord.example/hook"),
        },
    )

    def fake_post(url, payload):
        raise RuntimeError("webhook down")

    monkeypatch.setattr(notifications, "_post_json", fake_post)

    result = notifications.send_test_notification(settings_db_path=str(db_path))

    assert result["ok"] is False
    assert "failed" in result["message"].lower()


def test_run_complete_decrypts_urls_before_http_requests(monkeypatch):
    encrypted_discord = "enc::discord-token"
    encrypted_generic = "enc::generic-token"
    monkeypatch.setattr(
        notifications,
        "_load_settings",
        lambda settings_db_path=None: {
            "discord_webhook_url": encrypted_discord,
            "generic_webhook_url": encrypted_generic,
            "enable_run_complete_notifications": "1",
        },
    )

    seen_decrypt_inputs = []

    def fake_decrypt(value):
        seen_decrypt_inputs.append(value)
        if value == encrypted_discord:
            return "https://discord.example/decrypted"
        if value == encrypted_generic:
            return "https://generic.example/decrypted"
        return value

    sent_urls = []

    def fake_post(url, payload):
        del payload
        sent_urls.append(url)

    monkeypatch.setattr(secrets, "decrypt_secret", fake_decrypt)
    monkeypatch.setattr(notifications, "_post_json", fake_post)

    notifications.send_run_complete({"library": "Movies", "run_id": "run-1"})

    assert seen_decrypt_inputs == [encrypted_discord, encrypted_generic]
    assert sent_urls == ["https://discord.example/decrypted", "https://generic.example/decrypted"]


def test_test_notification_supports_plaintext_and_encrypted_urls(monkeypatch):
    monkeypatch.setattr(
        notifications,
        "_load_settings",
        lambda settings_db_path=None: {
            "discord_webhook_url": "https://discord.example/plaintext",
            "generic_webhook_url": "enc::generic-token",
        },
    )

    def fake_decrypt(value):
        if value == "enc::generic-token":
            return "https://generic.example/decrypted"
        return value

    sent_urls = []

    def fake_post(url, payload):
        del payload
        sent_urls.append(url)

    monkeypatch.setattr(secrets, "decrypt_secret", fake_decrypt)
    monkeypatch.setattr(notifications, "_post_json", fake_post)

    result = notifications.send_test_notification()

    assert result["ok"] is True
    assert sent_urls == ["https://discord.example/plaintext", "https://generic.example/decrypted"]


def test_run_failure_uses_decrypted_generic_url(monkeypatch):
    monkeypatch.setattr(
        notifications,
        "_load_settings",
        lambda settings_db_path=None: {
            "discord_webhook_url": "",
            "generic_webhook_url": "enc::generic-token",
            "enable_run_failure_notifications": "1",
        },
    )
    monkeypatch.setattr(
        secrets,
        "decrypt_secret",
        lambda value: "https://generic.example/decrypted" if value == "enc::generic-token" else value,
    )

    sent_urls = []

    def fake_post(url, payload):
        del payload
        sent_urls.append(url)

    monkeypatch.setattr(notifications, "_post_json", fake_post)

    notifications.send_run_failure({"library": "Movies", "run_id": "run-1", "error_message": "boom"})

    assert sent_urls == ["https://generic.example/decrypted"]
