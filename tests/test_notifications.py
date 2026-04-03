from __future__ import annotations

import sqlite3
import urllib.error

from chonk_reducer.services import notifications
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


def test_discord_webhook_detection_accepts_discord_com_and_discordapp_com():
    assert notifications.is_discord_webhook_url("https://discord.com/api/webhooks/123/abc") is True
    assert notifications.is_discord_webhook_url("https://discordapp.com/api/webhooks/123/abc") is True
    assert notifications.is_discord_webhook_url("https://discord.com/not-a-webhook") is False
    assert notifications.is_discord_webhook_url("https://example.com/api/webhooks/123/abc") is False


def test_discordapp_webhook_urls_are_normalized_to_discord_com():
    normalized = notifications.normalize_discord_webhook_url("https://discordapp.com/api/webhooks/123/abc")
    assert normalized == "https://discord.com/api/webhooks/123/abc"

    same_url = notifications.normalize_discord_webhook_url("https://discord.com/api/webhooks/123/abc")
    assert same_url == "https://discord.com/api/webhooks/123/abc"


def test_send_test_notification_normalizes_decrypted_discordapp_url(monkeypatch):
    monkeypatch.setattr(
        notifications,
        "_load_settings",
        lambda settings_db_path=None: {
            "discord_webhook_url": "enc::discord-token",
            "generic_webhook_url": "",
        },
    )

    monkeypatch.setattr(
        secrets,
        "decrypt_secret",
        lambda value: "https://discordapp.com/api/webhooks/123/abc" if value == "enc::discord-token" else value,
    )

    sent_urls = []

    def fake_post(url, payload):
        del payload
        sent_urls.append(url)

    monkeypatch.setattr(notifications, "_post_json", fake_post)

    result = notifications.send_test_notification()

    assert result["ok"] is True
    assert sent_urls == ["https://discord.com/api/webhooks/123/abc"]


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


def test_post_json_sets_explicit_headers_and_disables_proxies_by_default(monkeypatch):
    captured = {}

    class DummyResponse(object):
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            return False

    class DummyOpener(object):
        def open(self, req, timeout=None):
            captured["request"] = req
            captured["timeout"] = timeout
            return DummyResponse()

    class ProxyHandlerStub(object):
        def __init__(self, proxies):
            captured["proxies"] = proxies

    monkeypatch.delenv("CHONK_WEBHOOK_USE_PROXY", raising=False)
    monkeypatch.setattr(notifications.urllib.request, "ProxyHandler", ProxyHandlerStub)
    monkeypatch.setattr(notifications.urllib.request, "build_opener", lambda *handlers: DummyOpener())

    notifications._post_json("https://discord.com/api/webhooks/123/abc", {"content": "ok"})

    assert captured["proxies"] == {}
    assert captured["timeout"] == notifications._NOTIFICATION_TIMEOUT_SECONDS
    assert captured["request"].headers["Content-type"] == "application/json"
    assert captured["request"].headers["User-agent"] == notifications._NOTIFICATIONS_USER_AGENT


def test_post_json_enables_env_proxy_when_opted_in(monkeypatch):
    captured = {"proxy_handler_calls": 0}

    class DummyResponse(object):
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            del exc_type, exc, tb
            return False

    class DummyOpener(object):
        def open(self, req, timeout=None):
            del req, timeout
            return DummyResponse()

    def fake_proxy_handler(proxies):
        captured["proxy_handler_calls"] += 1
        return proxies

    monkeypatch.setenv("CHONK_WEBHOOK_USE_PROXY", "1")
    monkeypatch.setattr(notifications.urllib.request, "ProxyHandler", fake_proxy_handler)
    monkeypatch.setattr(notifications.urllib.request, "build_opener", lambda *handlers: DummyOpener())

    notifications._post_json("https://generic.example/hook", {"event": "ok"})

    assert captured["proxy_handler_calls"] == 0


def test_post_json_logs_proxy_environment_errors(monkeypatch, caplog):
    class DummyOpener(object):
        def open(self, req, timeout=None):
            del req, timeout
            raise urllib.error.URLError("proxy tunnel connection failed")

    monkeypatch.setenv("CHONK_WEBHOOK_USE_PROXY", "1")
    monkeypatch.setattr(notifications.urllib.request, "build_opener", lambda *handlers: DummyOpener())

    with caplog.at_level("WARNING"):
        try:
            notifications._post_json("https://discord.com/api/webhooks/123/abc", {"content": "x"})
            assert False
        except urllib.error.URLError:
            pass

    assert "proxy/environment issue" in caplog.text


def test_post_json_logs_discord_403(monkeypatch, caplog):
    class DummyOpener(object):
        def open(self, req, timeout=None):
            del timeout
            raise urllib.error.HTTPError(req.full_url, 403, "Forbidden", hdrs=None, fp=None)

    monkeypatch.delenv("CHONK_WEBHOOK_USE_PROXY", raising=False)
    monkeypatch.setattr(notifications.urllib.request, "build_opener", lambda *handlers: DummyOpener())

    with caplog.at_level("WARNING"):
        try:
            notifications._post_json("https://discord.com/api/webhooks/123/abc", {"content": "x"})
            assert False
        except urllib.error.HTTPError:
            pass

    assert "HTTP 403" in caplog.text
