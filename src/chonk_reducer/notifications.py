from __future__ import annotations

import json
import logging
import os
import socket
import sqlite3
from pathlib import Path
from typing import Dict, Optional
import urllib.request


LOGGER = logging.getLogger("chonk_reducer.notifications")


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
    return values


def _is_enabled(value: str) -> bool:
    return str(value or "").strip().lower() in ("1", "true", "yes", "y", "on")


def _post_json(url: str, payload: Dict[str, object]) -> None:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url=str(url).strip(),
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10):
        return None


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

    discord_url = str(settings.get("discord_webhook_url", "")).strip()
    generic_url = str(settings.get("generic_webhook_url", "")).strip()

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

    discord_url = str(settings.get("discord_webhook_url", "")).strip()
    generic_url = str(settings.get("generic_webhook_url", "")).strip()

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
