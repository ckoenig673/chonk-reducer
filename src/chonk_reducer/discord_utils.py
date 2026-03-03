from __future__ import annotations

import json
import os
import urllib.request
from typing import Optional


def _env(name: str, default: str = "") -> str:
    return (os.environ.get(name) or default).strip()


def _env_bool(name: str, default: bool = False) -> bool:
    v = _env(name, "true" if default else "false").lower()
    return v in ("1", "true", "yes", "y", "on")


def discord_enabled() -> bool:
    return bool(_env("DISCORD_WEBHOOK_URL", ""))


def send_discord_message(content: str, *, ping_user: bool = False) -> bool:
    """Post a message to Discord via webhook. Returns True if successful."""
    url = _env("DISCORD_WEBHOOK_URL", "")
    if not url:
        return False

    user_id = _env("DISCORD_USER_ID", "")
    if ping_user and user_id:
        content = f"<@{user_id}> {content}"

    payload = {"content": content}
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        url=url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            # Discord returns 204 No Content on success
            return 200 <= resp.status < 300 or resp.status == 204
    except Exception:
        return False


def notify_healthcheck_enabled() -> bool:
    return _env_bool("DISCORD_NOTIFY_HEALTHCHECK", False)


def notify_weekly_enabled() -> bool:
    return _env_bool("DISCORD_NOTIFY_WEEKLY", False)
