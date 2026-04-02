from __future__ import annotations

import os
from datetime import datetime
from typing import Optional

try:
    from zoneinfo import ZoneInfo  # type: ignore
except Exception:  # pragma: no cover - fallback for Python 3.8 runtime
    try:
        from backports.zoneinfo import ZoneInfo  # type: ignore
    except Exception:  # pragma: no cover - best-effort timezone display
        ZoneInfo = None


def display_version(version: str) -> str:
    normalized = str(version or "").strip() or "dev"
    if normalized.lower().startswith("v"):
        return normalized
    return "v%s" % normalized


def analytics_file_display_name(path: str) -> str:
    raw_path = str(path or "").strip()
    if not raw_path:
        return "-"
    filename = os.path.basename(raw_path)
    stem, _ = os.path.splitext(filename)
    return stem or filename


def format_duration_seconds(value) -> str:
    try:
        seconds = int(round(float(value)))
    except Exception:
        return "Unknown"
    if seconds < 0:
        return "Unknown"
    if seconds < 60:
        return "%ss" % seconds
    if seconds < 3600:
        minutes = seconds // 60
        remainder = seconds % 60
        return "%sm %ss" % (minutes, remainder)
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    return "%sh %02dm" % (hours, minutes)


def duration_seconds_from_run(ts_start, ts_end, fallback_seconds) -> Optional[float]:
    start_text = str(ts_start or "").strip()
    end_text = str(ts_end or "").strip()
    if start_text and end_text:
        try:
            start_dt = datetime.fromisoformat(start_text)
            end_dt = datetime.fromisoformat(end_text)
            delta_seconds = (end_dt - start_dt).total_seconds()
            if delta_seconds > 0:
                return float(delta_seconds)
        except Exception:
            pass
    try:
        fallback = float(fallback_seconds)
    except Exception:
        return None
    if fallback < 0:
        return None
    return fallback


def run_saved_mb_gb_label(saved_bytes) -> str:
    try:
        value = int(saved_bytes)
    except Exception:
        return "Unknown"
    if value < 0:
        return "Unknown"
    gib = float(1024 ** 3)
    mib = float(1024 ** 2)
    if value >= 1024 ** 3:
        return "%.1f GB" % (value / gib)
    return "%.1f MB" % (value / mib)


def format_saved_bytes(value) -> str:
    try:
        saved_bytes = int(value)
    except Exception:
        return "Unknown"

    if saved_bytes < 0:
        return "Unknown"
    if saved_bytes < 1024:
        return "%d B" % saved_bytes

    units = ["KB", "MB", "GB", "TB"]
    scaled = float(saved_bytes)
    for unit in units:
        scaled = scaled / 1024.0
        if scaled < 1024.0 or unit == units[-1]:
            return "%.1f %s" % (scaled, unit)
    return "%d B" % saved_bytes


def format_eta_seconds(value) -> str:
    text = str(value or "").strip()
    if not text:
        return "-"
    try:
        seconds = int(text)
    except Exception:
        return "-"
    if seconds < 0:
        return "-"
    if seconds < 60:
        return "%ss" % seconds
    minutes = seconds // 60
    remainder = seconds % 60
    return "%sm %ss" % (minutes, remainder)


def display_trigger(trigger: str) -> str:
    value = str(trigger or "").strip().lower()
    if value == "manual":
        return "Manual"
    if value in ("schedule", "scheduled"):
        return "Scheduled"
    if not value:
        return "-"
    return str(trigger)


def display_run_mode(mode: str) -> str:
    value = str(mode or "").strip().lower()
    if value in ("preview", "dry_run", "dry-run"):
        return "Preview"
    if value in ("live", "normal", "encode"):
        return "Live"
    if not value:
        return "Live"
    return str(mode)


def display_run_trigger(event_type: str) -> str:
    value = str(event_type or "").strip().lower()
    if value == "manual_preview_requested":
        return "Manual Preview"
    if value == "manual_run_requested":
        return "Manual"
    if value == "scheduled_run_requested":
        return "Scheduled"
    return "Unknown"


def format_scheduler_datetime(value, timezone_name: Optional[str] = None) -> str:
    if value is None:
        return "Unknown"
    if isinstance(value, datetime):
        tzinfo = None
        if ZoneInfo is not None:
            tz_name = str(timezone_name or os.getenv("TZ", "UTC") or "UTC").strip() or "UTC"
            try:
                tzinfo = ZoneInfo(tz_name)
            except Exception:
                tzinfo = None
        if tzinfo is not None and value.tzinfo is not None:
            try:
                value = value.astimezone(tzinfo)
            except Exception:
                pass
        return value.strftime("%Y-%m-%d %H:%M")
    return str(value)


def format_readable_timestamp(value: object) -> str:
    text = str(value or "").strip()
    if not text or text == "-":
        return "-"
    if "T" in text:
        text = text.replace("T", " ")
    if text.endswith("Z"):
        text = text[:-1]
    if len(text) >= 16:
        return text[:16]
    return text


def coerce_scheduler_datetime(value) -> Optional[datetime]:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        normalized = text
        if normalized.endswith(" UTC"):
            normalized = normalized[:-4] + "+00:00"
        try:
            return datetime.fromisoformat(normalized)
        except Exception:
            return None
    return None


def format_savings_pct(size_before, size_after) -> str:
    try:
        before = float(size_before)
        after = float(size_after)
    except Exception:
        return "Unknown"

    if before <= 0:
        return "Unknown"

    pct = ((before - after) / before) * 100.0
    return "%.1f%%" % pct
