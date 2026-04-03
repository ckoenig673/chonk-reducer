from __future__ import annotations

import json
import logging
import os
import socket
import sqlite3
from pathlib import Path
from typing import Dict, Optional
import urllib.error
import urllib.request
from urllib.parse import urlsplit, urlunsplit

from ..core import secrets

LOGGER = logging.getLogger("chonk_reducer.notifications")

_DISCORD_WEBHOOK_HOSTS = {"discord.com", "discordapp.com"}
_NOTIFICATION_TIMEOUT_SECONDS = 10
_NOTIFICATIONS_USER_AGENT = "ChonkReducer/1.x (+https://github.com/chonk-reducer/chonk-reducer)"
_WEBHOOK_PROXY_ENV_VAR = "CHONK_WEBHOOK_USE_PROXY"


def _settings_db_path(explicit_path: Optional[str] = None) -> Path:
    value = str(explicit_path or os.getenv("STATS_PATH") or "/config/chonk.db").strip()
    return Path(value or "/config/chonk.db")


def _load_settings(settings_db_path: Optional[str] = None) -> Dict[str, str]:
    defaults = {
        "discord_webhook_url": "",
        "generic_webhook_url": "",
        "enable_run_complete_notifications": "0",
        "enable_run_failure_notifications": "0",
    }
    db_path = _settings_db_path(settings_db_path)
    try:
        conn = sqlite3.connect(str(db_path))
        rows = conn.execute(
            "SELECT key, value FROM settings WHERE key IN (?, ?, ?, ?)",
            tuple(defaults.keys()),
        ).fetchall()
        conn.close()
    except Exception:
        return defaults

    values = dict(defaults)
    for key, value in rows:
        values[str(key)] = str(value or "")

    for key in ("discord_webhook_url", "generic_webhook_url"):
        raw_value = values.get(key, "")
        if not raw_value:
            continue
        try:
            values[key] = secrets.decrypt_secret(raw_value)
        except secrets.SecretConfigError as exc:
            LOGGER.warning("Unable to decrypt %s: %s", key, exc)
            values[key] = ""
    return values


def _resolve_secret_url(value: str, setting_key: str) -> str:
    raw_value = str(value or "").strip()
    if not raw_value:
        return ""
    try:
        resolved = str(secrets.decrypt_secret(raw_value)).strip()
    except secrets.SecretConfigError as exc:
        LOGGER.warning("Unable to decrypt %s at runtime: %s", setting_key, exc)
        return ""
    if setting_key == "discord_webhook_url":
        return normalize_discord_webhook_url(resolved)
    return resolved


def is_discord_webhook_url(value: str) -> bool:
    parsed = urlsplit(str(value or "").strip())
    host = str(parsed.hostname or "").lower()
    path = str(parsed.path or "")
    return parsed.scheme in ("http", "https") and host in _DISCORD_WEBHOOK_HOSTS and path.startswith("/api/webhooks/")


def normalize_discord_webhook_url(value: str) -> str:
    raw_value = str(value or "").strip()
    if not is_discord_webhook_url(raw_value):
        return raw_value

    parsed = urlsplit(raw_value)
    host = str(parsed.hostname or "").lower()
    if host != "discordapp.com":
        return raw_value

    netloc = str(parsed.netloc or "")
    replaced_netloc = "discord.com" + netloc[len("discordapp.com") :]
    return urlunsplit((parsed.scheme, replaced_netloc, parsed.path, parsed.query, parsed.fragment))


def _is_enabled(value: str) -> bool:
    return str(value or "").strip().lower() in ("1", "true", "yes", "y", "on")


def _post_json(url: str, payload: Dict[str, object]) -> None:
    raw_url = str(url).strip()
    parsed = urlsplit(raw_url)
    host = str(parsed.hostname or "")
    use_proxy = _is_enabled(os.getenv(_WEBHOOK_PROXY_ENV_VAR, "0"))

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url=raw_url,
        data=data,
        headers={"Content-Type": "application/json", "User-Agent": _NOTIFICATIONS_USER_AGENT},
        method="POST",
    )

    opener = (
        urllib.request.build_opener()
        if use_proxy
        else urllib.request.build_opener(urllib.request.ProxyHandler({}))
    )
    try:
        with opener.open(req, timeout=_NOTIFICATION_TIMEOUT_SECONDS):
            return None
    except urllib.error.HTTPError as exc:
        if exc.code == 403:
            LOGGER.warning(
                "Webhook POST rejected with HTTP 403 (host=%s, proxy=%s): %s",
                host,
                "enabled" if use_proxy else "disabled",
                exc,
            )
        else:
            LOGGER.warning(
                "Webhook POST failed with HTTP %s (host=%s, proxy=%s): %s",
                exc.code,
                host,
                "enabled" if use_proxy else "disabled",
                exc,
            )
        raise
    except urllib.error.URLError as exc:
        reason_text = str(exc.reason)
        lower_reason = reason_text.lower()
        is_proxy_error = "proxy" in lower_reason or "tunnel connection failed" in lower_reason
        if is_proxy_error:
            LOGGER.warning(
                "Webhook POST failed due to proxy/environment issue (host=%s, proxy=%s): %s",
                host,
                "enabled" if use_proxy else "disabled",
                reason_text,
            )
        else:
            LOGGER.warning(
                "Webhook POST failed due to network failure (host=%s, proxy=%s): %s",
                host,
                "enabled" if use_proxy else "disabled",
                reason_text,
            )
        raise


def _format_space_saved(saved_bytes: int) -> str:
    if saved_bytes < 1024:
        return "%d B" % saved_bytes
    units = ["KB", "MB", "GB", "TB"]
    scaled = float(saved_bytes)
    for unit in units:
        scaled = scaled / 1024.0
        if scaled < 1024.0 or unit == units[-1]:
            return "%.1f %s" % (scaled, unit)
    return "%d B" % saved_bytes


def _complete_discord_content(run_summary: Dict[str, object]) -> str:
    return (
        "Chonk Reducer Run Complete\n"
        "Library: %s\n"
        "Run ID: %s\n"
        "Files Scanned: %s\n"
        "Files Optimized: %s\n"
        "Space Saved: %s\n"
        "Duration: %s\n"
        "Host: %s"
    ) % (
        run_summary.get("library", ""),
        run_summary.get("run_id", ""),
        run_summary.get("files_scanned", 0),
        run_summary.get("files_optimized", 0),
        run_summary.get("total_space_saved", "0 B"),
        run_summary.get("duration", "0.0s"),
        run_summary.get("host", socket.gethostname()),
    )


def _failure_discord_content(run_summary: Dict[str, object]) -> str:
    return (
        "Chonk Reducer Run Failed\n"
        "Library: %s\n"
        "Run ID: %s\n"
        "Error: %s\n"
        "Host: %s"
    ) % (
        run_summary.get("library", ""),
        run_summary.get("run_id", ""),
        run_summary.get("error_message", "Unknown error"),
        run_summary.get("host", socket.gethostname()),
    )


def send_run_complete(run_summary: Dict[str, object], settings_db_path: Optional[str] = None) -> None:
    settings = _load_settings(settings_db_path)
    if not _is_enabled(settings.get("enable_run_complete_notifications", "0")):
        return

    discord_url = _resolve_secret_url(settings.get("discord_webhook_url", ""), "discord_webhook_url")
    generic_url = _resolve_secret_url(settings.get("generic_webhook_url", ""), "generic_webhook_url")

    if not discord_url and not generic_url:
        return

    if discord_url:
        try:
            _post_json(discord_url, {"content": _complete_discord_content(run_summary)})
        except Exception as exc:
            LOGGER.warning("Discord run complete notification failed: %s", exc)

    if generic_url:
        payload = {
            "event": "run_complete",
            "library": run_summary.get("library", ""),
            "run_id": run_summary.get("run_id", ""),
            "files_scanned": int(run_summary.get("files_scanned", 0) or 0),
            "files_optimized": int(run_summary.get("files_optimized", 0) or 0),
            "total_space_saved": run_summary.get("total_space_saved", "0 B"),
            "duration": run_summary.get("duration", "0.0s"),
            "host": run_summary.get("host", socket.gethostname()),
        }
        try:
            _post_json(generic_url, payload)
        except Exception as exc:
            LOGGER.warning("Generic run complete notification failed: %s", exc)


def send_run_failure(run_summary: Dict[str, object], settings_db_path: Optional[str] = None) -> None:
    settings = _load_settings(settings_db_path)
    if not _is_enabled(settings.get("enable_run_failure_notifications", "0")):
        return

    discord_url = _resolve_secret_url(settings.get("discord_webhook_url", ""), "discord_webhook_url")
    generic_url = _resolve_secret_url(settings.get("generic_webhook_url", ""), "generic_webhook_url")

    if not discord_url and not generic_url:
        return

    if discord_url:
        try:
            _post_json(discord_url, {"content": _failure_discord_content(run_summary)})
        except Exception as exc:
            LOGGER.warning("Discord run failure notification failed: %s", exc)

    if generic_url:
        payload = {
            "event": "run_failure",
            "library": run_summary.get("library", ""),
            "run_id": run_summary.get("run_id", ""),
            "error_message": run_summary.get("error_message", "Unknown error"),
            "host": run_summary.get("host", socket.gethostname()),
        }
        try:
            _post_json(generic_url, payload)
        except Exception as exc:
            LOGGER.warning("Generic run failure notification failed: %s", exc)


def send_test_notification(settings_db_path: Optional[str] = None) -> Dict[str, object]:
    settings = _load_settings(settings_db_path)
    discord_url = _resolve_secret_url(settings.get("discord_webhook_url", ""), "discord_webhook_url")
    generic_url = _resolve_secret_url(settings.get("generic_webhook_url", ""), "generic_webhook_url")

    if not discord_url and not generic_url:
        return {"ok": False, "message": "No webhook URL configured. Add one and try again."}

    sent = 0
    failures = []
    if discord_url:
        try:
            _post_json(discord_url, {"content": "Chonk Reducer test notification: configuration looks good."})
            sent += 1
        except Exception as exc:
            failures.append("Discord: %s" % exc)
            LOGGER.warning("Discord test notification failed: %s", exc)

    if generic_url:
        try:
            _post_json(generic_url, {"event": "test_notification", "message": "Chonk Reducer test notification"})
            sent += 1
        except Exception as exc:
            failures.append("Generic: %s" % exc)
            LOGGER.warning("Generic test notification failed: %s", exc)

    if sent and not failures:
        return {"ok": True, "message": "Test notification sent successfully."}
    if sent:
        return {"ok": True, "message": "Test notification sent with partial failures: %s" % "; ".join(failures)}
    return {"ok": False, "message": "Test notification failed: %s" % "; ".join(failures)}


def build_run_complete_summary(library: str, run_id: str, row: Optional[sqlite3.Row] = None) -> Dict[str, object]:
    files_scanned = int(row["candidates_found"] if row is not None and "candidates_found" in row.keys() else 0)
    files_optimized = int(row["success_count"] if row is not None and "success_count" in row.keys() else 0)
    saved_bytes = int(row["saved_bytes"] if row is not None and "saved_bytes" in row.keys() else 0)
    duration_seconds = float(row["duration_seconds"] if row is not None and "duration_seconds" in row.keys() else 0.0)

    return {
        "library": library,
        "run_id": run_id,
        "files_scanned": files_scanned,
        "files_optimized": files_optimized,
        "total_space_saved": _format_space_saved(saved_bytes),
        "duration": "%.1fs" % duration_seconds,
        "host": socket.gethostname(),
    }
