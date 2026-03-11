from __future__ import annotations

import json
import logging
import os
import socket
import sqlite3
import threading
import uuid
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from time import strftime
from typing import Callable, Deque, Dict, Iterator, List, Optional, Set
from urllib.parse import parse_qs, quote, unquote, urlsplit

try:
    from zoneinfo import ZoneInfo  # type: ignore
except Exception:  # pragma: no cover - fallback for Python 3.8 runtime
    try:
        from backports.zoneinfo import ZoneInfo  # type: ignore
    except Exception:  # pragma: no cover - best-effort timezone display
        ZoneInfo = None

try:
    from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore
    from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED, EVENT_JOB_MISSED  # type: ignore
    from apscheduler.triggers.cron import CronTrigger  # type: ignore
except Exception:  # pragma: no cover - exercised by fallback tests
    BackgroundScheduler = None
    EVENT_JOB_ERROR = None
    EVENT_JOB_EXECUTED = None
    EVENT_JOB_MISSED = None
    CronTrigger = None

try:
    from fastapi import FastAPI, Request, Response  # type: ignore
    from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse  # type: ignore
except Exception:  # pragma: no cover - exercised by fallback tests
    FastAPI = None
    Request = None
    Response = None
    HTMLResponse = None
    JSONResponse = None
    RedirectResponse = None

try:
    import uvicorn  # type: ignore
except Exception:  # pragma: no cover - exercised by fallback tests
    uvicorn = None

from .cleanup import cleanup_logs
from .logging_utils import Logger
from .runner import run
from . import notifications
from . import secrets
from . import __version__


LOGGER = logging.getLogger("chonk_reducer.service")
_ENV_MUTATION_LOCK = threading.RLock()
_ENV_RUNTIME_BASELINES: Dict[str, Optional[str]] = {}
_ENV_RUNTIME_DEPTH = 0

WEEKDAY_CHOICES = [
    ("Su", "sun"),
    ("M", "mon"),
    ("T", "tue"),
    ("W", "wed"),
    ("Th", "thu"),
    ("F", "fri"),
    ("Sa", "sat"),
]

LEGACY_CRON_WEEKDAY_MAP = {
    "0": "sun",
    "1": "mon",
    "2": "tue",
    "3": "wed",
    "4": "thu",
    "5": "fri",
    "6": "sat",
    "7": "sun",
}


EDITABLE_SETTINGS = {
    "min_file_age_minutes": {
        "env": "MIN_FILE_AGE_MINUTES",
        "default": "10",
        "label": "Minimum File Age (Minutes)",
        "description": "Skip very recent files newer than this age. Helps avoid processing files still being copied.",
    },
    "min_savings_percent": {
        "env": "MIN_SAVINGS_PERCENT",
        "default": "15",
        "label": "Minimum Savings Percent",
        "description": "Minimum required savings percent before swap. Higher values are stricter.",
    },
    "max_savings_percent": {
        "env": "MAX_SAVINGS_PERCENT",
        "default": "0",
        "label": "Maximum Savings Percent",
        "description": "Optional upper savings guard. Set to 0 to disable this limit.",
    },
    "min_media_free_gb": {
        "env": "MIN_MEDIA_FREE_GB",
        "default": "0",
        "label": "Minimum Media Free Space (GB)",
        "description": "Minimum free-space safety threshold for the media volume. Set to 0 to disable.",
    },
    "max_gb_per_run": {
        "env": "MAX_GB_PER_RUN",
        "default": "0",
        "label": "Maximum GB Per Run",
        "description": "Optional cap on total GB processed in one run. Set to 0 for no cap.",
    },
    "fail_fast": {
        "env": "FAIL_FAST",
        "default": "0",
        "label": "Fail Fast",
        "description": "Stop early on failure conditions instead of continuing with remaining files.",
    },
    "log_skips": {
        "env": "LOG_SKIPS",
        "default": "0",
        "label": "Log Skips",
        "description": "Emit skip reasons more verbosely in logs and stats output.",
    },
    "top_candidates": {
        "env": "TOP_CANDIDATES",
        "default": "5",
        "label": "Top Candidates",
        "description": "Candidate ranking helper limit used for selection and display.",
    },
    "retry_count": {
        "env": "RETRY_COUNT",
        "default": "1",
        "label": "Retry Count",
        "description": "Number of retries after the initial encode attempt.",
    },
    "retry_backoff_seconds": {
        "env": "RETRY_BACKOFF_SECONDS",
        "default": "5",
        "label": "Retry Backoff Seconds",
        "description": "Delay between retry attempts in seconds.",
    },
    "validate_seconds": {
        "env": "VALIDATE_SECONDS",
        "default": "10",
        "label": "Validate Seconds",
        "description": "Validation sample duration used for post-encode checks.",
    },
    "log_retention_days": {
        "env": "LOG_RETENTION_DAYS",
        "default": "30",
        "label": "Log Retention Days",
        "description": "Retention window for log cleanup.",
    },
    "bak_retention_days": {
        "env": "BAK_RETENTION_DAYS",
        "default": "60",
        "label": "Backup Retention Days",
        "description": "Retention window for backup (.bak) cleanup.",
    },
    "discord_webhook_url": {
        "env": "DISCORD_WEBHOOK_URL",
        "default": "",
        "label": "Discord Webhook URL",
        "description": "Discord notification endpoint. Leave empty if not used.",
    },
    "generic_webhook_url": {
        "env": "GENERIC_WEBHOOK_URL",
        "default": "",
        "label": "Generic Webhook URL",
        "description": "Generic webhook endpoint. Leave empty if not used.",
    },
    "enable_run_complete_notifications": {
        "env": "ENABLE_RUN_COMPLETE_NOTIFICATIONS",
        "default": "0",
        "label": "Enable Run Complete Notifications",
        "description": "Send notifications after successful run completion.",
    },
    "enable_run_failure_notifications": {
        "env": "ENABLE_RUN_FAILURE_NOTIFICATIONS",
        "default": "0",
        "label": "Enable Run Failure Notifications",
        "description": "Send notifications when a run fails.",
    },
}

LIBRARY_SETTINGS_HELP = {
    "name": "Operator label for the library. Must be unique.",
    "path": "Media root path for scanning. Must be unique.",
    "min_size_gb": "Skip files below this library-specific size floor.",
    "max_files": "Maximum files processed per run for this library.",
    "priority": "Queue priority for this library. Higher numbers run first.",
    "qsv_quality": "QSV quality value for this library. Lower values generally mean higher quality and larger output.",
    "qsv_preset": "QSV preset for this library. Tune based on speed and quality needs.",
    "min_savings_percent": "Library-specific minimum savings percent required before swap.",
    "skip_codecs": "Comma-separated codecs to skip, such as hevc,av1.",
    "skip_min_height": "Skip files at or above this vertical resolution.",
    "skip_resolution_tags": "Comma-separated filename tags to skip, such as 2160p,4k,uhd.",
    "schedule": "Cron schedule for automatic runs. Blank schedule means manual runs only.",
    "enabled": "Enable or disable this library for runtime controls and scheduling.",
    "schedule_days": "Select weekdays for simple schedule mode.",
    "schedule_time": "Run time used with selected days in simple schedule mode.",
    "raw_cron": "Raw cron expression for advanced scheduling.",
}

CHECKBOX_SETTINGS = {"enable_run_complete_notifications", "enable_run_failure_notifications", "fail_fast", "log_skips"}
SECRET_SETTINGS = {"discord_webhook_url", "generic_webhook_url"}
SECRET_PLACEHOLDER_VALUES = {"set (hidden)", "configured (hidden)", "********", "******"}

RESTART_REQUIRED_SETTINGS = set()


@dataclass(frozen=True)
class LibraryRecord:
    id: int
    name: str
    path: str
    enabled: bool
    schedule: str
    min_size_gb: float
    max_files: int
    priority: int
    qsv_quality: Optional[int]
    qsv_preset: Optional[int]
    min_savings_percent: Optional[float]
    skip_codecs: str = ""
    skip_min_height: int = 0
    skip_resolution_tags: str = ""


@dataclass(frozen=True)
class RuntimeLibrary:
    id: int
    name: str
    path: str
    schedule: str
    min_size_gb: float
    max_files: int
    priority: int
    qsv_quality: Optional[int]
    qsv_preset: Optional[int]
    min_savings_percent: Optional[float]
    skip_codecs: str = ""
    skip_min_height: int = 0
    skip_resolution_tags: str = ""


@dataclass(frozen=True)
class RuntimeJob:
    library_id: int
    library_name: str
    trigger: str
    priority: int


@dataclass(frozen=True)
class ServiceSettings:
    enabled: bool
    host: str
    port: int
    movie_schedule: str
    tv_schedule: str
    settings_db_path: str = ""

    @classmethod
    def from_env(cls) -> "ServiceSettings":
        return cls(
            enabled=_env_bool("SERVICE_MODE", False),
            host=_env("SERVICE_HOST", "0.0.0.0"),
            port=_env_int("SERVICE_PORT", 8080),
            movie_schedule=_env("MOVIE_SCHEDULE", ""),
            tv_schedule=_env("TV_SCHEDULE", ""),
            settings_db_path=_env("STATS_PATH", "/config/chonk.db"),
        )


def _env(name: str, default: str) -> str:
    with _ENV_MUTATION_LOCK:
        return (os.getenv(name) or default).strip()




def _env_bootstrap(name: str, default: str) -> str:
    with _ENV_MUTATION_LOCK:
        if _ENV_RUNTIME_DEPTH > 0 and name in _ENV_RUNTIME_BASELINES:
            baseline = _ENV_RUNTIME_BASELINES.get(name)
            return (baseline or default).strip()
        return (os.getenv(name) or default).strip()


def _env_bootstrap_compat(primary: str, fallback: str, default: str) -> str:
    primary_value = _env_bootstrap(primary, "")
    if primary_value:
        return primary_value
    return _env_bootstrap(fallback, default)

def _env_int(name: str, default: int) -> int:
    value = _env(name, str(default))
    try:
        return int(value)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    value = _env(name, str(default))
    try:
        return float(value)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    value = _env(name, "1" if default else "0").lower()
    return value in ("1", "true", "yes", "y", "on")


def _env_bool_text(value: str) -> bool:
    return str(value or "").strip().lower() in ("1", "true", "yes", "y", "on")


class _ScheduledJob:
    def __init__(self, job_id: str):
        self.id = job_id
        self.next_run_time = None


class _FallbackScheduler:
    def __init__(self):
        self._jobs: List[_ScheduledJob] = []

    def add_job(
        self,
        func,
        trigger=None,
        id=None,
        args=None,
        coalesce=True,
        max_instances=1,
        replace_existing=True,
        next_run_time=None,
    ):
        del func, trigger, args, coalesce, max_instances, replace_existing, next_run_time
        self._jobs = [job for job in self._jobs if job.id != id]
        self._jobs.append(_ScheduledJob(id))

    def get_jobs(self):
        return list(self._jobs)

    def start(self):
        return None

    def shutdown(self, wait=False):
        del wait
        return None


class _FallbackFastAPI:
    def __init__(self):
        self.routes: Dict[str, Callable] = {}

    def get(self, path: str):
        def decorator(fn):
            self.routes["GET %s" % path] = fn
            return fn

        return decorator

    def post(self, path: str):
        def decorator(fn):
            self.routes["POST %s" % path] = fn
            return fn

        return decorator


class ChonkService:
    def __init__(self, settings: ServiceSettings):
        settings_db_path = (settings.settings_db_path or _env("STATS_PATH", "/config/chonk.db")).strip() or "/config/chonk.db"
        self._settings_db_path = Path(settings_db_path)
        self._editable_settings = self._bootstrap_editable_settings()
        self._bootstrap_libraries()
        self.settings = ServiceSettings(
            enabled=settings.enabled,
            host=settings.host,
            port=settings.port,
            movie_schedule=settings.movie_schedule,
            tv_schedule=settings.tv_schedule,
            settings_db_path=settings_db_path,
        )
        self.scheduler = self._build_scheduler()
        self._attach_scheduler_listeners()
        self.app = self._build_app()
        self._library_locks: Dict[str, threading.Lock] = {}
        self._job_state_lock = threading.Lock()
        for library in self.enabled_runtime_libraries():
            self._library_locks[str(library.id)] = threading.Lock()
            self._library_locks[library.name.strip().lower()] = self._library_locks[str(library.id)]
        self._job_state_lock = threading.Lock()
        self._job_condition = threading.Condition(self._job_state_lock)
        self._job_queue: Deque[RuntimeJob] = deque()
        self._queued_or_running_library_ids: Set[int] = set()
        self._current_job: Optional[RuntimeJob] = None
        self._current_job_started_at = ""
        self._current_job_run_id = ""
        self._worker_shutdown = False
        self._cancel_requested = False
        self._last_run_was_cancelled = False
        self._current_run_snapshot: Dict[str, str] = {}
        self._scheduler_started_at = ""
        self._scheduler_stopped = False
        self._last_preview_results: List[Dict[str, object]] = []
        self._last_preview_snapshots_by_library: Dict[int, Dict[str, object]] = {}
        self._latest_preview_library_id: Optional[int] = None
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()
        self._configure_routes()

    def _build_scheduler(self):
        if BackgroundScheduler is not None:
            return BackgroundScheduler(timezone=os.getenv("TZ", "UTC"))
        return _FallbackScheduler()

    def _build_app(self):
        if FastAPI is not None:
            return FastAPI(title="Chonk Reducer Service")
        return _FallbackFastAPI()

    def _attach_scheduler_listeners(self) -> None:
        add_listener = getattr(self.scheduler, "add_listener", None)
        if not callable(add_listener):
            return
        if EVENT_JOB_EXECUTED is None or EVENT_JOB_ERROR is None or EVENT_JOB_MISSED is None:
            return
        try:
            add_listener(self._on_scheduler_event, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR | EVENT_JOB_MISSED)
        except Exception:
            LOGGER.warning("Unable to attach APScheduler event listeners", exc_info=True)

    def _on_scheduler_event(self, event) -> None:
        job_id = str(getattr(event, "job_id", "") or "")
        exception = getattr(event, "exception", None)
        if getattr(event, "code", None) == EVENT_JOB_MISSED:
            LOGGER.warning("Scheduler event: job_missed job_id=%s", job_id)
            return
        if exception is not None:
            LOGGER.error("Scheduler event: job_error job_id=%s", job_id)
            return
        LOGGER.info("Scheduler event: job_executed job_id=%s", job_id)

    def _configure_routes(self) -> None:
        @self.app.get("/")
        def home():
            return self._html_response(self.home_page_html())

        @self.app.get("/dashboard")
        def dashboard():
            return self._html_response(self.home_page_html())

        @self.app.get("/favicon.ico")
        def favicon():
            return self._no_content_response()

        @self.app.get("/runs")
        def runs_page():
            return self._html_response(self.runs_page_html())

        @self.app.get("/runs/{run_id}")
        def run_detail_page(run_id: str):
            html, status_code = self.run_detail_page_html(run_id)
            return self._html_response(html, status_code=status_code)

        @self.app.get("/activity")
        def activity_page():
            return self._html_response(self.activity_page_html())

        @self.app.get("/history")
        def history_page():
            return self._html_response(self.history_page_html())

        @self.app.get("/system")
        def system_page():
            return self._html_response(self.system_page_html())

        @self.app.get("/settings")
        def settings_page():
            return self._html_response(self.settings_page_html())

        @self.app.post("/settings")
        async def save_settings(request: Request = None):  # type: ignore[assignment]
            values = await self._request_form_values(request)
            normalized = self._normalize_settings_updates(values)
            self.update_editable_settings(normalized)
            return self._html_response(self.settings_page_html(self.settings_saved_message(normalized)))

        @self.app.post("/settings/test-notification")
        def test_notification():
            result = notifications.send_test_notification(settings_db_path=str(self._settings_db_path))
            if result.get("ok"):
                self._record_activity("notification_test", str(result.get("message", "Test notification sent.")))
            else:
                self._record_activity("notification_test_failed", str(result.get("message", "Test notification failed.")))
            return self._html_response(self.settings_page_html(str(result.get("message", ""))))

        @self.app.post("/settings/libraries/create")
        async def create_library(request: Request = None):  # type: ignore[assignment]
            values = await self._request_form_values(request)
            return self._html_response(self.settings_page_html(self.create_library(values)))

        @self.app.post("/settings/libraries/update")
        async def update_library(request: Request = None):  # type: ignore[assignment]
            values = await self._request_form_values(request)
            return self._html_response(self.settings_page_html(self.update_library(values)))

        @self.app.post("/settings/libraries/delete")
        async def delete_library(request: Request = None):  # type: ignore[assignment]
            values = await self._request_form_values(request)
            return self._html_response(self.settings_page_html(self.delete_library(values)))

        @self.app.post("/settings/libraries/toggle")
        async def toggle_library(request: Request = None):  # type: ignore[assignment]
            values = await self._request_form_values(request)
            return self._html_response(self.settings_page_html(self.toggle_library(values)))

        @self.app.get("/health")
        def health() -> dict:
            return self.health_payload()

        @self.app.get("/api/status")
        def api_status() -> dict:
            return self.current_job_status()

        @self.app.post("/api/run/cancel")
        def api_cancel_run():
            payload = self.request_cancel_active_run()
            if JSONResponse is not None:
                return JSONResponse(content=payload, status_code=200)
            return payload

        @self.app.post("/api/preview/clear")
        def api_clear_preview():
            payload = self.clear_preview_results()
            if JSONResponse is not None:
                return JSONResponse(content=payload, status_code=200)
            return payload

        @self.app.post("/libraries/{library_id}/run")
        def run_library(library_id: int):
            payload, status_code = self.manual_run_payload_for_id(int(library_id))
            if JSONResponse is not None:
                return JSONResponse(content=payload, status_code=status_code)
            return payload

        @self.app.post("/libraries/{library_id}/preview")
        def preview_library(library_id: int):
            payload, status_code = self.manual_preview_payload_for_id(int(library_id))
            if JSONResponse is not None:
                return JSONResponse(content=payload, status_code=status_code)
            return payload

        @self.app.post("/dashboard/libraries/{library_id}/run")
        def run_library_from_dashboard(library_id: int):
            payload, _ = self.manual_run_payload_for_id(int(library_id))
            location = "/dashboard"
            if payload.get("status") in ("queued", "busy"):
                location = "/dashboard?manual_run=%s&library_id=%s" % (
                    quote(str(payload.get("status", ""))),
                    quote(str(payload.get("library_id", ""))),
                )
            if RedirectResponse is not None:
                return RedirectResponse(url=location, status_code=303)
            return self._html_response(self.home_page_html())

        @self.app.post("/dashboard/libraries/{library_id}/preview")
        def preview_library_from_dashboard(library_id: int):
            payload, _ = self.manual_preview_payload_for_id(int(library_id))
            location = "/dashboard"
            if payload.get("status") in ("queued", "busy"):
                location = "/dashboard?manual_run=%s&library_id=%s" % (
                    quote(str(payload.get("status", ""))),
                    quote(str(payload.get("library_id", ""))),
                )
            if RedirectResponse is not None:
                return RedirectResponse(url=location, status_code=303)
            return self._html_response(self.home_page_html())

        @self.app.post("/run/movies")
        def run_movies():
            payload, status_code = self.manual_run_payload("movies")
            if JSONResponse is not None:
                return JSONResponse(content=payload, status_code=status_code)
            return payload

        @self.app.post("/run/tv")
        def run_tv():
            payload, status_code = self.manual_run_payload("tv")
            if JSONResponse is not None:
                return JSONResponse(content=payload, status_code=status_code)
            return payload

    def _html_response(self, html: str, status_code: int = 200):
        if HTMLResponse is not None:
            return HTMLResponse(content=html, status_code=status_code)
        return html

    def _no_content_response(self):
        if Response is not None:
            return Response(status_code=204)
        return ""

    async def _request_form_values(self, request: Request = None) -> Dict[str, str]:
        values: Dict[str, str] = {}
        if request is not None and hasattr(request, "form"):
            form = await request.form()
            values = {key: str(value) for key, value in form.items()}
        return values

    def home_page_html(self) -> str:
        libraries = self.list_libraries()
        recent_runs = self._recent_runs(limit=10)
        lifetime_savings = self._lifetime_savings()
        library_totals = self._library_lifetime_totals()
        library_sections = []
        for library in libraries:
            status = self._latest_run_status(library.name)
            runtime_library = RuntimeLibrary(
                id=library.id,
                name=library.name,
                path=library.path,
                schedule=library.schedule,
                min_size_gb=library.min_size_gb,
                max_files=library.max_files,
                priority=library.priority,
                qsv_quality=library.qsv_quality,
                qsv_preset=library.qsv_preset,
                min_savings_percent=library.min_savings_percent,
                skip_codecs=library.skip_codecs,
                skip_min_height=library.skip_min_height,
                skip_resolution_tags=library.skip_resolution_tags,
            )
            runtime_status = "Disabled"
            runtime_summary = ""
            if library.enabled:
                runtime_status = self._library_runtime_status(runtime_library)
                runtime_summary = self._library_runtime_summary(runtime_library)
            last_run_label = "Never"
            processed_label = "0"
            savings_label = "0 B"
            totals = library_totals.get(library.name.strip().lower(), {"files_optimized": 0, "total_saved": 0})
            if status is not None:
                last_run_label = status.get("ts_end") or status.get("ts_start") or "Unknown"
                processed_label = str(status.get("processed_count") or 0)
                savings_label = _format_saved_bytes(status.get("saved_bytes"))
            library_sections.append(
                """
  <section style="border: 1px solid #ddd; padding: 0.75rem; margin-bottom: 0.75rem; background: #fff;">
    <h2 style="margin-top: 0; margin-bottom: 0.5rem;">%s</h2>
    <div><strong>Path:</strong> %s</div>
    <div><strong>Status:</strong> %s</div>
    <div><strong>Priority:</strong> %s</div>
    <div><strong>Last Run:</strong> %s</div>
    <div><strong>Next Run:</strong> %s</div>
    <div><strong>Files Optimized:</strong> %s</div>
    <div><strong>Total Saved:</strong> %s</div>
    <div><strong>Recent Savings:</strong> %s across %s files</div>
    %s
    <form method="post" action="/dashboard/libraries/%d/run" style="margin-top: 0.75rem;">
      <button type="submit">Run Now</button>
      <button type="submit" formaction="/dashboard/libraries/%d/preview" style="margin-left: 0.45rem;">Preview Run</button>
    </form>
  </section>
"""
                % (
                    _escape_html(library.name),
                    _escape_html(library.path),
                    _escape_html(runtime_status),
                    _escape_html(str(library.priority)),
                    _escape_html(last_run_label),
                    _escape_html(self._next_run_label(library)),
                    _escape_html(str(totals["files_optimized"])),
                    _escape_html(_format_saved_bytes(totals["total_saved"])),
                    _escape_html(savings_label),
                    _escape_html(processed_label),
                    runtime_summary,
                    library.id,
                    library.id,
                )
            )

        if not library_sections:
            library_sections.append('<div style="padding: 0.5rem; border: 1px solid #ddd;">No enabled libraries configured.</div>')

        content = """
  <h1>Dashboard</h1>
  <p>Manual run controls for troubleshooting and operational checks.</p>
  <h2 style="margin-top: 1rem; margin-bottom: 0.5rem;">Current Job Status</h2>
  %s
  %s
  <h2 style="margin-top: 1rem; margin-bottom: 0.5rem;">Lifetime Savings</h2>
  %s
  <h2 style="margin-top: 1rem; margin-bottom: 0.5rem;">Recent Runs</h2>
  %s
  <script>
    (function () {
      function textValue(value, placeholder) {
        if (value === null || value === undefined || String(value).trim() === "") {
          return placeholder;
        }
        return String(value);
      }

      function setText(id, value, placeholder) {
        var node = document.getElementById(id);
        if (!node) {
          return;
        }
        node.textContent = textValue(value, placeholder);
      }

      function savedBytesLabel(rawValue) {
        var parsed = parseInt(rawValue, 10);
        if (isNaN(parsed) || parsed <= 0) {
          return "-";
        }
        var units = ["B", "KB", "MB", "GB", "TB"];
        var scaled = parsed;
        var index = 0;
        while (scaled >= 1024 && index < units.length - 1) {
          scaled = scaled / 1024;
          index += 1;
        }
        if (index === 0) {
          return String(parsed) + " B";
        }
        return scaled.toFixed(1) + " " + units[index];
      }

      function schedulerTimestampLabel(rawValue) {
        var text = String(rawValue || "").trim();
        if (!text || text === "-") {
          return "-";
        }
        if (text.indexOf("T") !== -1) {
          text = text.replace("T", " ");
        }
        if (text.endsWith("Z")) {
          text = text.slice(0, -1);
        }
        if (text.length >= 16) {
          return text.slice(0, 16);
        }
        return text;
      }

      function triggerLabel(trigger) {
        var value = String(trigger || "").trim().toLowerCase();
        if (value === "manual") {
          return "Manual";
        }
        if (value === "schedule" || value === "scheduled") {
          return "Scheduled";
        }
        return textValue(trigger, "-");
      }

      function parseCount(value) {
        var parsed = parseInt(value, 10);
        if (isNaN(parsed) || parsed < 0) {
          return 0;
        }
        return parsed;
      }

      function formatEtaSeconds(rawValue) {
        var seconds = parseInt(rawValue, 10);
        if (isNaN(seconds) || seconds < 0) {
          return "-";
        }
        if (seconds < 60) {
          return String(seconds) + "s";
        }
        var minutes = Math.floor(seconds / 60);
        var rem = seconds %% 60;
        return String(minutes) + "m " + String(rem) + "s";
      }

      function progressMarkup(snapshot) {
        if (String(snapshot.status || "") !== "Running") {
          return "";
        }
        var processed = parseCount(snapshot.files_processed);
        var candidates = parseCount(snapshot.candidates_found);
        var ratio = 0;
        var progressLabel = "";
        if (candidates > 0) {
          ratio = Math.min(1, processed / candidates);
          progressLabel = String(processed) + " / " + String(candidates) + " files processed";
        } else {
          ratio = processed > 0 ? 1 : 0;
          progressLabel = String(processed) + " files processed";
        }
        var pctLabel = String(Math.round(ratio * 100)) + "%%";
        var encodePercent = parseFloat(snapshot.encode_percent);
        if (!isNaN(encodePercent)) {
          pctLabel = String(Math.round(Math.max(0, Math.min(100, encodePercent)))) + "%%";
        }
        var currentFile = textValue(snapshot.current_file, "Waiting for first file");
        var encodeSpeed = textValue(snapshot.encode_speed, "-");
        var encodeEta = formatEtaSeconds(snapshot.encode_eta);
        var retryAttempt = parseInt(snapshot.retry_attempt, 10);
        var retryMax = parseInt(snapshot.retry_max, 10);
        var retryMarkup = '';
        if (!isNaN(retryAttempt) && !isNaN(retryMax) && retryAttempt > 0 && retryAttempt <= retryMax) {
          retryMarkup = '<div><strong>Retry Attempt:</strong> ' + String(retryAttempt) + ' / ' + String(retryMax) + '</div>';
        }
        return '' +
          '<div style="margin-top:0.75rem; padding:0.6rem; border:1px solid #d7e2f4; background:#f8fbff;">' +
          '<div style="font-weight:600; margin-bottom:0.35rem;">Run Progress</div>' +
          '<div style="border:1px solid #c8d8f0; background:#eef4ff; width:100%%; height:18px;">' +
          '<div style="background:#2a6fd6; width:' + pctLabel + '; height:100%%;"></div>' +
          '</div>' +
          '<div style="margin-top:0.35rem;">' + progressLabel + ' (' + pctLabel + ')</div>' +
          '<div style="margin-top:0.35rem;"><strong>Percent Complete:</strong> ' + pctLabel + '</div>' +
          '<div><strong>Speed:</strong> ' + encodeSpeed + '</div>' +
          '<div><strong>ETA:</strong> ' + encodeEta + '</div>' +
          retryMarkup +
          '<div style="margin-top:0.55rem;"><strong>Current Library:</strong> ' + textValue(snapshot.current_library, "-") + '</div>' +
          '<div><strong>Current File:</strong> ' + currentFile + '</div>' +
          '<div style="margin-top:0.4rem;"><strong>Files Evaluated:</strong> ' + String(parseCount(snapshot.files_evaluated)) + '</div>' +
          '<div><strong>Files Processed:</strong> ' + String(processed) + '</div>' +
          '<div><strong>Files Skipped:</strong> ' + String(parseCount(snapshot.files_skipped)) + '</div>' +
          '<div><strong>Files Failed:</strong> ' + String(parseCount(snapshot.files_failed)) + '</div>' +
          '<div><strong>Total Saved:</strong> ' + textValue(savedBytesLabel(snapshot.bytes_saved), "0 B") + '</div>' +
          '</div>';
      }

      function updateFromSnapshot(snapshot) {
        setText("runtime-status", snapshot.status, "Idle");
        setText("runtime-mode", snapshot.mode, "-");
        setText("runtime-library", snapshot.current_library, "-");
        setText("runtime-trigger", triggerLabel(snapshot.trigger), "-");
        setText("runtime-scheduler-status", snapshot.scheduler_status, "-");
        setText("runtime-scheduler-started", schedulerTimestampLabel(snapshot.scheduler_started_at), "-");
        setText("runtime-next-scheduled-job", snapshot.next_scheduled_job, "-");
        setText("runtime-next-scheduled-time", snapshot.next_scheduled_time, "-");
        setText("runtime-queue-depth", snapshot.queue_depth, "0");
        setText("runtime-run-id", snapshot.run_id, "-");
        setText("runtime-started-at", snapshot.started_at, "-");
        var currentFilePlaceholder = String(snapshot.status || "") === "Running" ? "Waiting for first file" : "-";
        setText("runtime-current-file", snapshot.current_file, currentFilePlaceholder);
        setText("runtime-candidates-found", snapshot.candidates_found, "-");
        setText("runtime-files-evaluated", snapshot.files_evaluated, "-");
        setText("runtime-files-processed", snapshot.files_processed, "-");
        setText("runtime-files-skipped", snapshot.files_skipped, "-");
        setText("runtime-files-failed", snapshot.files_failed, "-");
        setText("runtime-bytes-saved", savedBytesLabel(snapshot.bytes_saved), "-");
        setText("runtime-preview-library", snapshot.preview_library, "-");
        setText("runtime-preview-generated-at", snapshot.preview_generated_at, "-");
        setPreviewResults(snapshot.preview_results || []);
        var progress = document.getElementById("runtime-progress-section");
        if (progress) {
          progress.innerHTML = progressMarkup(snapshot);
        }
        updateStopButton(snapshot);
      }

      function setPreviewResults(rows) {
        var body = document.getElementById("runtime-preview-results-body");
        if (!body) {
          return;
        }
        if (!rows || !rows.length) {
          body.innerHTML = '<tr><td colspan="5" style="padding: 0.35rem;">No preview results yet.</td></tr>';
          return;
        }
        var html = '';
        for (var i = 0; i < rows.length; i += 1) {
          var row = rows[i] || {};
          html += '<tr>' +
            '<td style="border-top:1px solid #ddd; padding:0.3rem;">' + textValue(row.file, '-') + '</td>' +
            '<td style="border-top:1px solid #ddd; padding:0.3rem;">' + savedBytesLabel(row.original_size) + '</td>' +
            '<td style="border-top:1px solid #ddd; padding:0.3rem;">' + savedBytesLabel(row.estimated_size) + '</td>' +
            '<td style="border-top:1px solid #ddd; padding:0.3rem;">' + textValue(row.estimated_savings_pct, '-') + '%%</td>' +
            '<td style="border-top:1px solid #ddd; padding:0.3rem;">' + textValue(row.decision, '-') + '</td>' +
          '</tr>';
        }
        body.innerHTML = html;
      }

      function requestStopRun() {
        return fetch("/api/run/cancel", { method: "POST" }).catch(function () { return null; });
      }

      function requestClearPreviewResults() {
        return fetch("/api/preview/clear", { method: "POST" })
          .then(function (response) {
            if (!response.ok) {
              throw new Error("preview clear failed");
            }
            return response.json();
          })
          .catch(function () {
            return null;
          });
      }

      function updateStopButton(snapshot) {
        var button = document.getElementById("runtime-stop-button");
        if (!button) {
          return;
        }
        var running = String(snapshot.status || "") === "Running" || String(snapshot.status || "") === "Cancelling";
        button.style.display = running ? "inline-block" : "none";
        button.disabled = String(snapshot.status || "") === "Cancelling";
      }

      function fetchStatus() {
        fetch("/api/status", { cache: "no-store" })
          .then(function (response) {
            if (!response.ok) {
              throw new Error("status request failed");
            }
            return response.json();
          })
          .then(updateFromSnapshot)
          .catch(function () {
            return null;
          });
      }

      var stopButton = document.getElementById("runtime-stop-button");
      if (stopButton) {
        stopButton.addEventListener("click", function () {
          requestStopRun().then(fetchStatus);
        });
      }

      var clearPreviewButton = document.getElementById("runtime-clear-preview-button");
      if (clearPreviewButton) {
        clearPreviewButton.addEventListener("click", function () {
          requestClearPreviewResults().then(fetchStatus);
        });
      }

      fetchStatus();
      window.setInterval(fetchStatus, 3000);
    })();
  </script>
""" % (
            self._runtime_status_html(),
            "".join(library_sections),
            self._lifetime_savings_html(lifetime_savings),
            self._recent_runs_html(recent_runs),
        )
        return self._render_shell_html("Dashboard", content)

    def _library_runtime_status(self, library: RuntimeLibrary) -> str:
        with self._job_condition:
            current_job = self._current_job
            queued_ids = {job.library_id for job in self._job_queue}

        if current_job is not None and current_job.library_id == library.id:
            return "Running"
        if library.id in queued_ids:
            return "Queued"
        return "Idle"

    def _library_runtime_summary(self, library: RuntimeLibrary) -> str:
        snapshot = self._runtime_status_snapshot()
        if snapshot["status"] != "Running":
            return ""
        current_library = str(snapshot.get("current_library") or "").strip().lower()
        if current_library != library.name.strip().lower():
            return ""
        current_file = str(snapshot.get("current_file") or "").strip() or "Waiting for first file"
        files_processed = self._snapshot_int(snapshot, "files_processed")
        candidates_found = self._snapshot_int(snapshot, "candidates_found")
        if candidates_found > 0:
            progress_label = "%s / %s files" % (files_processed, candidates_found)
        else:
            progress_label = "%s files" % files_processed
        return (
            '<div style="margin-top: 0.4rem; padding: 0.4rem; border: 1px solid #e3ebf6; background: #f7faff;">'
            '<strong>Active:</strong> %s<br /><strong>Progress:</strong> %s'
            "</div>"
        ) % (
            _escape_html(current_file),
            _escape_html(progress_label),
        )

    def _label_with_help(self, label: str, help_text: str, token: str) -> str:
        if not help_text:
            return "<strong>%s</strong>" % _escape_html(label)
        tooltip_id = "help-%s" % "".join(ch if ch.isalnum() else "-" for ch in token)
        return (
            '<span class="help-label"><strong>%s</strong>%s</span>'
            % (_escape_html(label), self._help_icon_html(help_text, tooltip_id))
        )

    def _help_icon_html(self, help_text: str, tooltip_id: str) -> str:
        escaped_text = _escape_html(help_text)
        escaped_id = _escape_html(tooltip_id)
        return (
            '<span class="help-tooltip-wrap">'
            '<span class="help-tooltip-trigger" tabindex="0" aria-label="Help: %s" aria-describedby="%s">?</span>'
            '<span class="help-tooltip-bubble" role="tooltip" id="%s">%s</span>'
            "</span>"
            % (escaped_text, escaped_id, escaped_id, escaped_text)
        )

    def settings_page_html(self, message: str = "") -> str:
        libraries = self.list_libraries()
        rows = []
        for key in EDITABLE_SETTINGS:
            meta = EDITABLE_SETTINGS[key]
            value = self._editable_settings.get(key, "")
            label = meta.get("label", key.replace("_", " ").title())
            description = meta.get("description", "")
            label_html = self._label_with_help(label, description, "global-%s" % key)
            restart_badge = ""
            if key in RESTART_REQUIRED_SETTINGS:
                restart_badge = ' <span style="color: #8a4f00; font-size: 0.9rem;">(restart required)</span>'
            if key in CHECKBOX_SETTINGS:
                checked = "checked" if _env_bool_text(value) else ""
                rows.append(
                    """<label for="{key}" style="display:block; margin-top: 0.75rem;">{label_html}{restart_badge}</label>
  <input id="{key}" name="{key}" type="checkbox" value="1" {checked} />""".format(
                        key=key,
                        label_html=label_html,
                        restart_badge=restart_badge,
                        checked=checked,
                    )
                )
            elif key in SECRET_SETTINGS:
                configured = bool(str(value or "").strip())
                status = "Configured (hidden)" if configured else "Not configured"
                rows.append(
                    """<label for="{key}" style="display:block; margin-top: 0.75rem;">{label_html}{restart_badge}</label>
  <input id="{key}" name="{key}" value="" placeholder="Set (hidden)" style="width: 100%; max-width: 420px;" autocomplete="off" />
  <div style="font-size: 0.9rem; color: #555; margin-top: 0.2rem;">{status}</div>
  <label style="display:block; margin-top: 0.3rem; color:#444;"><input type="checkbox" name="clear_{key}" value="1" /> Clear stored secret</label>""".format(
                        key=key,
                        label_html=label_html,
                        restart_badge=restart_badge,
                        status=_escape_html(status),
                    )
                )
            else:
                rows.append(
                    """<label for="{key}" style="display:block; margin-top: 0.75rem;">{label_html}{restart_badge}</label>
  <input id="{key}" name="{key}" value="{value}" style="width: 100%; max-width: 420px;" />""".format(
                        key=key,
                        label_html=label_html,
                        restart_badge=restart_badge,
                        value=_escape_html(value),
                    )
                )

        content = """
<style>
.help-label { display: inline-flex; align-items: center; gap: 0.35rem; }
.help-tooltip-wrap { position: relative; display: inline-flex; align-items: center; }
.help-tooltip-trigger {
  width: 1rem;
  height: 1rem;
  border-radius: 999px;
  border: 1px solid #8da0b4;
  background: #eef4fb;
  color: #1f3f5b;
  font-size: 0.75rem;
  line-height: 1rem;
  text-align: center;
  cursor: help;
}
.help-tooltip-trigger:focus { outline: 2px solid #2d6aa0; outline-offset: 1px; }
.help-tooltip-bubble {
  display: none;
  position: absolute;
  left: 1.3rem;
  top: -0.2rem;
  min-width: 220px;
  max-width: 320px;
  padding: 0.45rem 0.55rem;
  border-radius: 4px;
  background: #1f3f5b;
  color: #fff;
  font-size: 0.82rem;
  line-height: 1.3;
  z-index: 10;
}
.help-tooltip-wrap:hover .help-tooltip-bubble,
.help-tooltip-wrap:focus-within .help-tooltip-bubble { display: block; }
</style>
<h1>Settings</h1>
"""
        if message:
            content += '<div style="padding: 0.5rem; border: 1px solid #cfe9cf; background:#f4fff4;">%s</div>' % _escape_html(
                message
            )
        content += """
<section>
<h2>Global Settings</h2>
<p>Editable service defaults persisted in SQLite.</p>
<form method="post" action="/settings">%s
  <div style="margin-top: 1rem;"><button type="submit">Save</button></div>
</form>
<form method="post" action="/settings/test-notification" style="margin-top: 0.75rem;">
  <button type="submit">Send Test Notification</button>
</form>
<p style="margin-top: 1rem; color: #555;">Settings are saved immediately to SQLite. Some service-level behaviors are applied on startup/restart only.</p>
</section>
<section style="margin-top: 2rem;">
<h2>Libraries</h2>
<p>Configured media library roots for future dynamic scheduling.</p>
%s
%s
</section>""" % (
            "".join(rows),
            self._libraries_table_html(libraries),
            self._library_create_form_html(),
        )
        return self._render_shell_html("Settings", content)

    def settings_saved_message(self, updates: Dict[str, str]) -> str:
        if any(key in RESTART_REQUIRED_SETTINGS for key in updates):
            return "Settings saved. Some changes require a service restart to take effect."
        return "Settings saved."

    def _normalize_settings_updates(self, updates: Dict[str, str]) -> Dict[str, str]:
        normalized = dict(updates)
        for key in CHECKBOX_SETTINGS:
            if key not in normalized:
                normalized[key] = "0"
        for key in SECRET_SETTINGS:
            clear_key = "clear_%s" % key
            if str(normalized.get(clear_key, "")).strip() == "1":
                normalized[key] = ""
                continue
            if key not in normalized:
                continue
            raw_value = _clean_secret_input(normalized.get(key, ""))
            if not raw_value or raw_value.lower() in SECRET_PLACEHOLDER_VALUES:
                normalized.pop(key, None)
                continue
            normalized[key] = raw_value

        for key in ("retry_count", "retry_backoff_seconds", "top_candidates"):
            if key not in normalized:
                continue
            raw_value = str(normalized.get(key, "")).strip()
            try:
                parsed = int(raw_value)
            except Exception:
                parsed = 0
            if parsed < 0:
                parsed = 0
            normalized[key] = str(parsed)
        return normalized

    def activity_page_html(self) -> str:
        rows = self._recent_activity(limit=25)
        content = "<h1>Activity</h1><p>Recent operator-facing service events from SQLite.</p>"
        content += self._recent_activity_html(rows)
        return self._render_shell_html("Activity", content)

    def runs_page_html(self) -> str:
        rows = self._run_history(limit=50)
        content = "<h1>Runs</h1><p>Recent run history from SQLite.</p>"
        content += self._runs_history_html(rows)
        return self._render_shell_html("Runs", content)

    def history_page_html(self) -> str:
        rows = self._recent_encode_history(limit=200)
        content = "<h1>History</h1><p>Recent completed encode entries from SQLite.</p>"
        content += self._history_table_html(rows)
        return self._render_shell_html("History", content)

    def run_detail_page_html(self, run_id: str) -> tuple:
        run = self._run_detail(run_id)
        if run is None:
            content = "<h1>Run Not Found</h1><p>No run exists for run_id: <code>%s</code>.</p>" % _escape_html(run_id)
            return self._render_shell_html("Runs", content), 404

        encodes = self._encodes_for_run(run_id)
        content = "<h1>Run Detail</h1><p>Operator-facing summary for this run from SQLite.</p>"
        content += self._run_summary_html(run)
        content += self._related_run_info_html(run)
        content += '<h2 style="margin-top: 1rem;">File-Level Entries</h2>'
        content += self._run_encodes_html(encodes)
        return self._render_shell_html("Runs", content), 200

    def system_page_html(self) -> str:
        service_mode = "Enabled" if self.settings.enabled else "Disabled"
        scheduler_running = self._scheduler_running_label()

        enabled_libraries = self.enabled_runtime_libraries()
        schedule_rows = []
        for library in enabled_libraries:
            schedule_rows.append(
                '<tr><th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;">%s Schedule</th><td style="border-bottom: 1px solid #ddd; padding: 0.35rem;"><code>%s</code></td></tr>'
                % (_escape_html(library.name), _escape_html(library.schedule.strip() or "Not set"))
            )
            schedule_rows.append(
                '<tr><th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;">Next %s Run</th><td style="border-bottom: 1px solid #ddd; padding: 0.35rem;">%s</td></tr>'
                % (_escape_html(library.name), _escape_html(self._next_run_label(library)))
            )
        if not schedule_rows:
            schedule_rows.append(
                '<tr><th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;">Libraries</th><td style="border-bottom: 1px solid #ddd; padding: 0.35rem;">No enabled libraries</td></tr>'
            )

        work_root = (_env("WORK_ROOT", "") or "").strip() or "Not set"
        now_label = self._current_time_label()

        content = """
  <h1>System</h1>
  <p>Operator-facing service and scheduler status for this instance.</p>

  <h2 style="margin-top: 1rem; margin-bottom: 0.5rem;">Current Job Status</h2>
  %s

  <h2 style="margin-top: 1rem; margin-bottom: 0.5rem;">Service Information</h2>
  <table style="border-collapse: collapse; width: 100%%; border: 1px solid #ddd;">
    <tbody>
      <tr><th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem; width: 250px;">Version</th><td style="border-bottom: 1px solid #ddd; padding: 0.35rem;">%s</td></tr>
      <tr><th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;">Service Mode</th><td style="border-bottom: 1px solid #ddd; padding: 0.35rem;">%s</td></tr>
      <tr><th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;">Service Host</th><td style="border-bottom: 1px solid #ddd; padding: 0.35rem;">%s</td></tr>
      <tr><th style="text-align: left; padding: 0.35rem;">Service Port</th><td style="padding: 0.35rem;">%s</td></tr>
    </tbody>
  </table>

  <h2 style="margin-top: 1rem; margin-bottom: 0.5rem;">Scheduler Information</h2>
  <table style="border-collapse: collapse; width: 100%%; border: 1px solid #ddd;">
    <tbody>
      <tr><th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem; width: 250px;">Scheduler Status</th><td style="border-bottom: 1px solid #ddd; padding: 0.35rem;">%s</td></tr>
      %s
    </tbody>
  </table>

  <h2 style="margin-top: 1rem; margin-bottom: 0.5rem;">Runtime / Storage Information</h2>
  <table style="border-collapse: collapse; width: 100%%; border: 1px solid #ddd;">
    <tbody>
      <tr><th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem; width: 250px;">Stats Database Path</th><td style="border-bottom: 1px solid #ddd; padding: 0.35rem;">%s</td></tr>
      <tr><th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;">Work / Log Path</th><td style="border-bottom: 1px solid #ddd; padding: 0.35rem;">%s</td></tr>
      <tr><th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;">Enabled Library Roots</th><td style="border-bottom: 1px solid #ddd; padding: 0.35rem;">%s</td></tr>
      <tr><th style="text-align: left; padding: 0.35rem;">Current Time</th><td style="padding: 0.35rem;">%s</td></tr>
    </tbody>
  </table>

  <h2 style="margin-top: 1rem; margin-bottom: 0.5rem;">Current Job Status</h2>
  %s

  <h2 style="margin-top: 1rem; margin-bottom: 0.5rem;">Settings Source Information</h2>
  <p>Schedules and paths shown above are loaded from SQLite-backed settings and libraries, with environment/compose values used only for compatibility defaults.</p>
""" % (
            self._runtime_status_html(),
            _escape_html(__version__),
            _escape_html(service_mode),
            _escape_html(self.settings.host),
            _escape_html(str(self.settings.port)),
            _escape_html(scheduler_running),
            "".join(schedule_rows),
            _escape_html(str(self._settings_db_path)),
            _escape_html(work_root),
            _escape_html(self._enabled_library_roots_label(enabled_libraries)),
            _escape_html(now_label),
            self._runtime_job_status_html(),
        )
        return self._render_shell_html("System", content)

    def current_job_status(self) -> Dict[str, str]:
        snapshot = self._runtime_status_snapshot()
        scheduler_snapshot = self._scheduler_status_snapshot()
        return {
            "status": snapshot["status"],
            "mode": snapshot.get("mode", "Live"),
            "current_library": snapshot["current_library"],
            "trigger": snapshot["current_trigger"],
            "queue_depth": snapshot["queue_depth"],
            "run_id": snapshot["run_id"],
            "started_at": snapshot["started_at"],
            "current_file": snapshot["current_file"],
            "candidates_found": snapshot["candidates_found"],
            "files_evaluated": snapshot["files_evaluated"],
            "files_processed": snapshot["files_processed"],
            "files_skipped": snapshot["files_skipped"],
            "files_failed": snapshot["files_failed"],
            "bytes_saved": snapshot["bytes_saved"],
            "encode_percent": snapshot["encode_percent"],
            "encode_speed": snapshot["encode_speed"],
            "encode_eta": snapshot["encode_eta"],
            "encode_out_time": snapshot["encode_out_time"],
            "preview_results": snapshot.get("preview_results", []),
            "preview_library": snapshot.get("preview_library", ""),
            "preview_generated_at": snapshot.get("preview_generated_at", ""),
            "evaluated_count": snapshot["files_evaluated"],
            "processed_count": snapshot["files_processed"],
            "skipped_count": snapshot["files_skipped"],
            "failed_count": snapshot["files_failed"],
            "cancel_requested": snapshot.get("cancel_requested", "0"),
            "scheduler_status": scheduler_snapshot["status"],
            "scheduler_started_at": scheduler_snapshot["started_at"],
            "next_scheduled_job": scheduler_snapshot["next_job"],
            "next_scheduled_time": scheduler_snapshot["next_time"],
        }

    def _runtime_job_status_html(self) -> str:
        status = self.current_job_status()
        return """<table style=\"border-collapse: collapse; width: 100%%; border: 1px solid #ddd;\">
  <tbody>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem; width: 250px;\">Status</th><td style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Current Library</th><td style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Trigger</th><td style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Scheduler Status</th><td style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Scheduler Started</th><td style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Next Scheduled Job</th><td style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Next Scheduled Time</th><td style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Queue Depth</th><td style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Current Run ID</th><td style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; padding: 0.35rem;\">Started At</th><td style=\"padding: 0.35rem;\">%s</td></tr>
  </tbody>
</table>""" % (
            _escape_html(status["status"]),
            _escape_html(status["current_library"] or "-"),
            _escape_html(_display_trigger(status["trigger"] or "-")),
            _escape_html(status["scheduler_status"] or "-"),
            _escape_html(_format_readable_timestamp(status["scheduler_started_at"] or "-")),
            _escape_html(status["next_scheduled_job"] or "-"),
            _escape_html(status["next_scheduled_time"] or "-"),
            _escape_html(status["queue_depth"]),
            _escape_html(status["run_id"] or "-"),
            _escape_html(status["started_at"] or "-"),
        )

    def _scheduler_status_snapshot(self) -> Dict[str, str]:
        next_job, next_time = self._next_global_scheduled_job()
        return {
            "status": self._scheduler_running_label(),
            "started_at": self._scheduler_started_at or "-",
            "next_job": next_job,
            "next_time": next_time,
        }

    def _next_global_scheduled_job(self) -> Tuple[str, str]:
        earliest = None
        for library in self.enabled_runtime_libraries():
            next_run_time = self._library_next_run_datetime(library)
            if next_run_time is None:
                continue
            if earliest is None or next_run_time < earliest[1]:
                earliest = (library.name, next_run_time)

        if earliest is None:
            return "-", "-"

        return earliest[0], _format_scheduler_datetime(earliest[1])

    def _scheduled_job_display_name(self, job_id: str) -> str:
        value = str(job_id or "").strip()
        if not value:
            return "-"
        prefix = "library-"
        suffix = "-schedule"
        if value.startswith(prefix) and value.endswith(suffix):
            library_id = value[len(prefix) : -len(suffix)]
            if library_id.isdigit():
                for library in self.list_libraries():
                    if int(library.id) == int(library_id):
                        return library.name
        return value

    def _scheduler_running_label(self) -> str:
        if self._scheduler_stopped:
            return "Stopped"
        if self._scheduler_started_at:
            return "Running"
        return "Stopped"

    def _library_next_run_datetime(self, library: LibraryRecord) -> Optional[datetime]:
        if not bool(getattr(library, "enabled", True)):
            return None

        schedule = str(getattr(library, "schedule", "")).strip()
        if not schedule:
            return None

        job = None
        get_job = getattr(self.scheduler, "get_job", None)
        if callable(get_job):
            try:
                job = get_job(self._schedule_job_id(library.id))
            except Exception:
                job = None
        if job is None:
            jobs = getattr(self.scheduler, "get_jobs", lambda: [])() or []
            for candidate in jobs:
                if getattr(candidate, "id", "") == self._schedule_job_id(library.id):
                    job = candidate
                    break
        if job is not None:
            next_run_time = getattr(job, "next_run_time", None)
            if next_run_time is not None:
                parsed = _coerce_scheduler_datetime(next_run_time)
                if parsed is not None:
                    return parsed

        return _next_run_from_cron(schedule)

    def _next_run_label(self, library: LibraryRecord, manual_label: str = "Not Scheduled") -> str:
        if not bool(getattr(library, "enabled", True)):
            return "Disabled"

        schedule = str(getattr(library, "schedule", "")).strip()
        if not schedule:
            return manual_label

        computed_next = self._library_next_run_datetime(library)
        if computed_next is None:
            return manual_label
        return _format_scheduler_datetime(computed_next)

    def _current_time_label(self) -> str:
        tz_name = (_env("TZ", "UTC") or "UTC").strip() or "UTC"
        if ZoneInfo is None:
            return "%s (timezone data unavailable)" % datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        try:
            now = datetime.now(ZoneInfo(tz_name))
            return "%s (%s)" % (now.strftime("%Y-%m-%d %H:%M:%S"), tz_name)
        except Exception:
            return "%s (timezone: %s unavailable)" % (datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC"), tz_name)

    def _enabled_library_roots_label(self, libraries: List[RuntimeLibrary]) -> str:
        if not libraries:
            return "Not set"
        return ", ".join(["%s: %s" % (library.name, library.path) for library in libraries])

    def _render_placeholder_page(self, title: str, message: str) -> str:
        content = "<h1>%s</h1><p>%s</p>" % (_escape_html(title), _escape_html(message))
        return self._render_shell_html(title, content)

    def _render_shell_html(self, title: str, content_html: str) -> str:
        nav_items = [
            ("Dashboard", "/dashboard"),
            ("Runs", "/runs"),
            ("History", "/history"),
            ("Activity", "/activity"),
            ("Settings", "/settings"),
            ("System", "/system"),
        ]
        nav = []
        for item_title, href in nav_items:
            active = "font-weight: bold;" if item_title == title else ""
            nav.append(
                '<li style="margin: 0.5rem 0;"><a href="%s" style="text-decoration:none; color:#1f3f5b; %s">%s</a></li>'
                % (href, active, item_title)
            )
        return """<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <title>Chonk Reducer</title>
</head>
<body style=\"font-family: sans-serif; margin: 0; background: #f6f8fb;\">
  <div style=\"display: flex; min-height: 100vh;\">
    <aside style=\"width: 210px; background: #e6edf4; padding: 1rem; border-right: 1px solid #ccd7e3;\">
      <h2 style=\"margin-top: 0;\">Chonk Reducer</h2>
      <ul style=\"list-style: none; padding: 0; margin: 0;\">%s</ul>
    </aside>
    <main style=\"flex: 1; padding: 1.25rem 1.5rem;\">%s</main>
  </div>
</body>
</html>
""" % ("".join(nav), content_html)

    def health_payload(self) -> dict:
        return {"status": "ok"}

    def _bootstrap_editable_settings(self) -> Dict[str, str]:
        conn = _connect_settings_db(self._settings_db_path)
        values: Dict[str, str] = {}
        with conn:
            for key, meta in EDITABLE_SETTINGS.items():
                row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
                if row is None:
                    if key == "retry_backoff_seconds":
                        value = _env_bootstrap_compat(meta["env"], "RETRY_BACKOFF_SECS", meta["default"])
                    else:
                        value = _env_bootstrap(meta["env"], meta["default"])
                    if key in SECRET_SETTINGS and value:
                        value = secrets.encrypt_secret(value)
                    conn.execute(
                        "INSERT INTO settings(key, value, updated_at) VALUES (?, ?, ?)",
                        (key, value, _utc_timestamp()),
                    )
                else:
                    value = str(row["value"])
                    if key in SECRET_SETTINGS and value and not secrets.is_encrypted(value):
                        try:
                            encrypted = secrets.encrypt_secret(value)
                            conn.execute(
                                "UPDATE settings SET value = ?, updated_at = ? WHERE key = ?",
                                (encrypted, _utc_timestamp(), key),
                            )
                            value = encrypted
                        except secrets.SecretConfigError:
                            LOGGER.warning("%s is configured in plaintext and could not be auto-encrypted yet.", key)
                values[key] = value
        conn.close()
        return values

    def update_editable_settings(self, updates: Dict[str, str]) -> None:
        if not updates:
            return
        updates = self._normalize_settings_updates(updates)
        conn = _connect_settings_db(self._settings_db_path)
        with conn:
            for key in EDITABLE_SETTINGS:
                if key not in updates:
                    continue
                value = str(updates[key]).strip()
                if key in SECRET_SETTINGS and value:
                    value = secrets.encrypt_secret(_clean_secret_input(value))
                conn.execute(
                    """
                    INSERT INTO settings(key, value, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(key) DO UPDATE SET
                        value = excluded.value,
                        updated_at = excluded.updated_at
                    """,
                    (key, value, _utc_timestamp()),
                )
                self._editable_settings[key] = value
        conn.close()

    def _bootstrap_libraries(self) -> None:
        conn = _connect_settings_db(self._settings_db_path)
        should_bootstrap = False
        with conn:
            row = conn.execute("SELECT COUNT(*) AS c FROM libraries").fetchone()
            should_bootstrap = int(row["c"] if row is not None else 0) <= 0

            if not should_bootstrap:
                return

            now = _utc_timestamp()
            movies_schedule = self._legacy_schedule_value(conn, "movie_schedule", "MOVIE_SCHEDULE")
            tv_schedule = self._legacy_schedule_value(conn, "tv_schedule", "TV_SCHEDULE")
            defaults = [
                (
                    "Movies",
                    _env("MOVIE_MEDIA_ROOT", _library_values("movies").get("MEDIA_ROOT", "/movies")),
                    1,
                    movies_schedule,
                    0.0,
                    1,
                    100,
                ),
                (
                    "TV",
                    _env("TV_MEDIA_ROOT", _library_values("tv").get("MEDIA_ROOT", "/tv_shows")),
                    1,
                    tv_schedule,
                    0.0,
                    1,
                    100,
                ),
            ]
            skip_codecs_default = _normalize_csv_text(_env_bootstrap("SKIP_CODECS", ""))
            skip_resolution_tags_default = _normalize_csv_text(_env_bootstrap("SKIP_RESOLUTION_TAGS", ""))
            skip_min_height_default = max(0, _env_int("SKIP_MIN_HEIGHT", 0))
            for name, path, enabled, schedule, min_size_gb, max_files, priority in defaults:
                conn.execute(
                    """
                    INSERT INTO libraries(
                        name, path, enabled, schedule, min_size_gb, max_files, priority,
                        qsv_quality, qsv_preset, min_savings_percent,
                        skip_codecs, skip_min_height, skip_resolution_tags,
                        created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        name,
                        path,
                        enabled,
                        schedule,
                        min_size_gb,
                        max_files,
                        priority,
                        _env_int("QSV_QUALITY", 21),
                        _env_int("QSV_PRESET", 7),
                        _env_float("MIN_SAVINGS_PERCENT", 15.0),
                        skip_codecs_default,
                        skip_min_height_default,
                        skip_resolution_tags_default,
                        now,
                        now,
                    ),
                )
        conn.close()

    def _legacy_schedule_value(self, conn: sqlite3.Connection, key: str, env_name: str) -> str:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        if row is not None:
            return str(row["value"] or "").strip()
        return _env(env_name, "")

    def list_libraries(self) -> List[LibraryRecord]:
        conn = _connect_settings_db(self._settings_db_path)
        rows = conn.execute(
            "SELECT id, name, path, enabled, schedule, min_size_gb, max_files, priority, qsv_quality, qsv_preset, min_savings_percent, skip_codecs, skip_min_height, skip_resolution_tags FROM libraries ORDER BY id ASC"
        ).fetchall()
        conn.close()
        return [
            LibraryRecord(
                id=int(row["id"]),
                name=str(row["name"]),
                path=str(row["path"]),
                enabled=bool(int(row["enabled"])),
                schedule=str(row["schedule"] or ""),
                min_size_gb=float(row["min_size_gb"]),
                max_files=int(row["max_files"]),
                priority=int(row["priority"]),
                qsv_quality=int(row["qsv_quality"]) if row["qsv_quality"] is not None else None,
                qsv_preset=int(row["qsv_preset"]) if row["qsv_preset"] is not None else None,
                min_savings_percent=float(row["min_savings_percent"]) if row["min_savings_percent"] is not None else None,
                skip_codecs=str(row["skip_codecs"] or ""),
                skip_min_height=max(0, int(row["skip_min_height"] or 0)),
                skip_resolution_tags=str(row["skip_resolution_tags"] or ""),
            )
            for row in rows
        ]

    def create_library(self, values: Dict[str, str]) -> str:
        normalized, message = self._validate_library_values(values)
        if message:
            return message
        conn = _connect_settings_db(self._settings_db_path)
        try:
            with conn:
                conn.execute(
                    """
                    INSERT INTO libraries(
                        name, path, enabled, schedule, min_size_gb, max_files, priority,
                        qsv_quality, qsv_preset, min_savings_percent,
                        skip_codecs, skip_min_height, skip_resolution_tags,
                        created_at, updated_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized["name"],
                        normalized["path"],
                        int(normalized["enabled"]),
                        normalized["schedule"],
                        float(normalized["min_size_gb"]),
                        int(normalized["max_files"]),
                        int(normalized["priority"]),
                        int(normalized["qsv_quality"]),
                        int(normalized["qsv_preset"]),
                        float(normalized["min_savings_percent"]),
                        str(normalized["skip_codecs"]),
                        int(normalized["skip_min_height"]),
                        str(normalized["skip_resolution_tags"]),
                        _utc_timestamp(),
                        _utc_timestamp(),
                    ),
                )
        except sqlite3.IntegrityError as exc:
            conn.close()
            return self._library_integrity_error_message(exc)
        conn.close()
        return "Library created."

    def update_library(self, values: Dict[str, str]) -> str:
        library_id = str(values.get("library_id", "")).strip()
        if not library_id:
            return "Library update failed: missing library id."
        normalized, message = self._validate_library_values(values)
        if message:
            return message
        conn = _connect_settings_db(self._settings_db_path)
        try:
            with conn:
                cursor = conn.execute(
                    """
                    UPDATE libraries
                    SET name = ?, path = ?, enabled = ?, schedule = ?, min_size_gb = ?, max_files = ?, priority = ?, qsv_quality = ?, qsv_preset = ?, min_savings_percent = ?, skip_codecs = ?, skip_min_height = ?, skip_resolution_tags = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        normalized["name"],
                        normalized["path"],
                        int(normalized["enabled"]),
                        normalized["schedule"],
                        float(normalized["min_size_gb"]),
                        int(normalized["max_files"]),
                        int(normalized["priority"]),
                        int(normalized["qsv_quality"]),
                        int(normalized["qsv_preset"]),
                        float(normalized["min_savings_percent"]),
                        str(normalized["skip_codecs"]),
                        int(normalized["skip_min_height"]),
                        str(normalized["skip_resolution_tags"]),
                        _utc_timestamp(),
                        int(library_id),
                    ),
                )
                if cursor.rowcount <= 0:
                    return "Library update failed: library not found."
        except ValueError:
            conn.close()
            return "Library update failed: invalid library id."
        except sqlite3.IntegrityError as exc:
            conn.close()
            return self._library_integrity_error_message(exc)
        conn.close()
        return "Library updated."

    def delete_library(self, values: Dict[str, str]) -> str:
        library_id = str(values.get("library_id", "")).strip()
        if not library_id:
            return "Library delete failed: missing library id."
        conn = _connect_settings_db(self._settings_db_path)
        try:
            with conn:
                cursor = conn.execute("DELETE FROM libraries WHERE id = ?", (int(library_id),))
                if cursor.rowcount <= 0:
                    return "Library delete failed: library not found."
        except ValueError:
            conn.close()
            return "Library delete failed: invalid library id."
        conn.close()
        return "Library deleted."

    def toggle_library(self, values: Dict[str, str]) -> str:
        library_id = str(values.get("library_id", "")).strip()
        enabled_value = str(values.get("enabled", "")).strip()
        if not library_id:
            return "Library toggle failed: missing library id."
        enabled = 1 if enabled_value == "1" else 0
        conn = _connect_settings_db(self._settings_db_path)
        try:
            with conn:
                cursor = conn.execute(
                    "UPDATE libraries SET enabled = ?, updated_at = ? WHERE id = ?",
                    (enabled, _utc_timestamp(), int(library_id)),
                )
                if cursor.rowcount <= 0:
                    return "Library toggle failed: library not found."
        except ValueError:
            conn.close()
            return "Library toggle failed: invalid library id."
        conn.close()
        return "Library %s." % ("enabled" if enabled else "disabled")

    def _validate_library_values(self, values: Dict[str, str]) -> tuple:
        name = str(values.get("name", "")).strip()
        path = str(values.get("path", "")).strip()
        enabled = 1 if str(values.get("enabled", "1")).strip() == "1" else 0
        schedule_mode = str(values.get("schedule_mode", "simple")).strip().lower() or "simple"
        has_simple_days = any(str(values.get("schedule_day_%s" % day_value, "")).strip() for _, day_value in WEEKDAY_CHOICES)
        has_simple_time = bool(str(values.get("schedule_time", "")).strip())
        has_explicit_mode = "schedule_mode" in values
        if not has_explicit_mode and not has_simple_days and not has_simple_time:
            schedule_mode = "legacy"
        elif schedule_mode == "simple" and str(values.get("schedule", "")).strip() and not has_simple_days and not has_simple_time:
            schedule_mode = "advanced"

        if not name:
            return {}, "Library validation failed: name is required."
        if not path:
            return {}, "Library validation failed: path is required."

        min_size_raw = str(values.get("min_size_gb", "0")).strip() or "0"
        try:
            min_size_gb = float(min_size_raw)
        except ValueError:
            return {}, "Library validation failed: minimum file size must be a number."
        if min_size_gb < 0:
            return {}, "Library validation failed: minimum file size must be >= 0."

        max_files_raw = str(values.get("max_files", "1")).strip() or "1"
        try:
            max_files = int(max_files_raw)
        except ValueError:
            return {}, "Library validation failed: max files per run must be an integer."
        if max_files < 1:
            return {}, "Library validation failed: max files per run must be >= 1."

        priority_raw = str(values.get("priority", "100")).strip() or "100"
        try:
            priority = int(priority_raw)
        except ValueError:
            return {}, "Library validation failed: priority must be an integer."

        qsv_quality_raw = str(values.get("qsv_quality", _env_bootstrap("QSV_QUALITY", "21"))).strip() or "0"
        try:
            qsv_quality = int(qsv_quality_raw)
        except ValueError:
            return {}, "Library validation failed: QSV quality must be an integer."
        if qsv_quality < 0:
            return {}, "Library validation failed: QSV quality must be >= 0."

        qsv_preset_raw = str(values.get("qsv_preset", _env_bootstrap("QSV_PRESET", "7"))).strip() or "0"
        try:
            qsv_preset = int(qsv_preset_raw)
        except ValueError:
            return {}, "Library validation failed: QSV preset must be an integer."
        if qsv_preset < 0:
            return {}, "Library validation failed: QSV preset must be >= 0."

        min_savings_raw = str(values.get("min_savings_percent", _env_bootstrap("MIN_SAVINGS_PERCENT", "15"))).strip() or "0"
        try:
            min_savings_percent = float(min_savings_raw)
        except ValueError:
            return {}, "Library validation failed: minimum savings percent must be a number."
        if min_savings_percent < 0:
            return {}, "Library validation failed: minimum savings percent must be >= 0."

        skip_codecs = _normalize_csv_text(str(values.get("skip_codecs", _env_bootstrap("SKIP_CODECS", ""))))
        skip_resolution_tags = _normalize_csv_text(str(values.get("skip_resolution_tags", _env_bootstrap("SKIP_RESOLUTION_TAGS", ""))))
        skip_min_height_raw = str(values.get("skip_min_height", _env_bootstrap("SKIP_MIN_HEIGHT", "0"))).strip() or "0"
        try:
            skip_min_height = int(skip_min_height_raw)
        except ValueError:
            return {}, "Library validation failed: skip minimum height must be an integer."
        if skip_min_height < 0:
            return {}, "Library validation failed: skip minimum height must be >= 0."

        if schedule_mode == "legacy":
            schedule = str(values.get("schedule", "")).strip()
        elif schedule_mode == "advanced":
            schedule = str(values.get("schedule", "")).strip()
            if not schedule:
                return {}, "Library validation failed: cron schedule is required in advanced mode."
        else:
            selected_days = [day_value for _, day_value in WEEKDAY_CHOICES if str(values.get("schedule_day_%s" % day_value, "")).strip()]
            if not selected_days:
                return {}, "Library validation failed: select at least one weekday in simple mode."
            schedule_time = str(values.get("schedule_time", "")).strip()
            if not schedule_time:
                return {}, "Library validation failed: time is required in simple mode."
            schedule = _build_simple_cron(schedule_time, selected_days)
            if not schedule:
                return {}, "Library validation failed: invalid simple schedule time."
        return {
            "name": name,
            "path": path,
            "schedule": schedule,
            "enabled": enabled,
            "min_size_gb": min_size_gb,
            "max_files": max_files,
            "priority": priority,
            "qsv_quality": qsv_quality,
            "qsv_preset": qsv_preset,
            "min_savings_percent": min_savings_percent,
            "skip_codecs": skip_codecs,
            "skip_min_height": skip_min_height,
            "skip_resolution_tags": skip_resolution_tags,
        }, ""

    def _library_integrity_error_message(self, exc: sqlite3.IntegrityError) -> str:
        msg = str(exc).lower()
        if "libraries.name" in msg:
            return "Library validation failed: duplicate library name."
        if "libraries.path" in msg:
            return "Library validation failed: duplicate library path."
        return "Library validation failed: duplicate value."

    def _libraries_table_html(self, libraries: List[LibraryRecord]) -> str:
        if not libraries:
            return '<div style="padding: 0.5rem; border: 1px solid #ddd;">No libraries configured.</div>'

        row_html = []
        for library in libraries:
            schedule_state = _schedule_form_state(library.schedule)
            enabled_label = "Enabled" if library.enabled else "Disabled"
            toggle_target = "0" if library.enabled else "1"
            toggle_label = "Disable" if library.enabled else "Enable"
            row_html.append(
                """<tr>
  <td style=\"padding: 0.35rem; border-bottom: 1px solid #eee;\">{name}</td>
  <td style=\"padding: 0.35rem; border-bottom: 1px solid #eee;\"><code>{path}</code></td>
  <td style=\"padding: 0.35rem; border-bottom: 1px solid #eee;\">{enabled}</td>
  <td style=\"padding: 0.35rem; border-bottom: 1px solid #eee;\">{priority}</td>
  <td style=\"padding: 0.35rem; border-bottom: 1px solid #eee;\"><code>{schedule}</code></td>
  <td style=\"padding: 0.35rem; border-bottom: 1px solid #eee;\">{actions}</td>
</tr>
<tr>
  <td colspan=\"6\" style=\"padding: 0.35rem 0.35rem 0.75rem 0.35rem; border-bottom: 1px solid #ddd; background: #fafcff;\">
    <details>
      <summary>Edit {name}</summary>
      <form method=\"post\" action=\"/settings/libraries/update\" style=\"margin-top: 0.5rem;\">
        <input type=\"hidden\" name=\"library_id\" value=\"{library_id}\" />
        <label>{name_label}</label><br />
        <input name=\"name\" value=\"{name}\" style=\"width: 100%%; max-width: 420px;\" /><br />
        <label>{path_label}</label><br />
        <input name="path" value="{path}" style="width: 100%; max-width: 420px;" /><br />
        <fieldset style="margin-top: 0.5rem; padding: 0.5rem; border: 1px solid #ddd; max-width: 420px;">
          <legend><strong>Processing Settings</strong></legend>
          <label>{min_size_gb_label}</label><br />
          <input name="min_size_gb" type="number" step="0.1" min="0" value="{min_size_gb}" style="width: 100%;" /><br />
          <label>{max_files_label}</label><br />
          <input name="max_files" type="number" step="1" min="1" value="{max_files}" style="width: 100%;" /><br />
          <label>{priority_label}</label><br />
          <input name="priority" type="number" step="1" value="{priority}" style="width: 100%;" /><br />
          <small>Higher numbers run first when multiple libraries are queued.</small><br />
        </fieldset>
        <fieldset style="margin-top: 0.5rem; padding: 0.5rem; border: 1px solid #ddd; max-width: 420px;">
          <legend><strong>Encoding Settings</strong></legend>
          <label>{qsv_quality_label}</label><br />
          <input name="qsv_quality" type="number" step="1" min="0" value="{qsv_quality}" style="width: 100%;" /><br />
          <label>{qsv_preset_label}</label><br />
          <input name="qsv_preset" type="number" step="1" min="0" value="{qsv_preset}" style="width: 100%;" /><br />
          <label>{min_savings_percent_label}</label><br />
          <input name="min_savings_percent" type="number" step="0.1" min="0" value="{min_savings_percent}" style="width: 100%;" /><br />
        </fieldset>
        <fieldset style="margin-top: 0.5rem; padding: 0.5rem; border: 1px solid #ddd; max-width: 420px;">
          <legend><strong>Skip Settings</strong></legend>
          <label>{skip_codecs_label}</label><br />
          <input name="skip_codecs" value="{skip_codecs}" style="width: 100%;" /><br />
          <small>Comma-separated codecs to skip, such as hevc,av1.</small><br />
          <label>{skip_min_height_label}</label><br />
          <input name="skip_min_height" type="number" step="1" min="0" value="{skip_min_height}" style="width: 100%;" /><br />
          <small>Skip files at or above this vertical resolution.</small><br />
          <label>{skip_resolution_tags_label}</label><br />
          <input name="skip_resolution_tags" value="{skip_resolution_tags}" style="width: 100%;" /><br />
          <small>Comma-separated filename tags to skip, such as 2160p,4k,uhd.</small><br />
        </fieldset>
        {schedule_fields}
        <label>{enabled_label}</label>
        <select name=\"enabled\"><option value=\"1\" {enabled_yes}>Yes</option><option value=\"0\" {enabled_no}>No</option></select>
        <div style=\"margin-top: 0.5rem;\"><button type=\"submit\">Save Library</button></div>
      </form>
    </details>
  </td>
</tr>""".format(
                    name=_escape_html(library.name),
                    path=_escape_html(library.path),
                    enabled=enabled_label,
                    schedule=_escape_html(library.schedule),
                    min_size_gb=_escape_html("%s" % library.min_size_gb),
                    max_files=_escape_html(str(library.max_files)),
                    priority=_escape_html(str(library.priority)),
                    qsv_quality=_escape_html(str(library.qsv_quality if library.qsv_quality is not None else _env_bootstrap("QSV_QUALITY", "21"))),
                    qsv_preset=_escape_html(str(library.qsv_preset if library.qsv_preset is not None else _env_bootstrap("QSV_PRESET", "7"))),
                    min_savings_percent=_escape_html(str(library.min_savings_percent if library.min_savings_percent is not None else _env_bootstrap("MIN_SAVINGS_PERCENT", "15"))),
                    schedule_fields=self._schedule_fields_html(schedule_state, "edit-%d" % library.id),
                    name_label=self._label_with_help("Name", LIBRARY_SETTINGS_HELP["name"], "lib-name-edit-%d" % library.id),
                    path_label=self._label_with_help("Path", LIBRARY_SETTINGS_HELP["path"], "lib-path-edit-%d" % library.id),
                    min_size_gb_label=self._label_with_help("Minimum File Size (GB)", LIBRARY_SETTINGS_HELP["min_size_gb"], "lib-min-size-edit-%d" % library.id),
                    max_files_label=self._label_with_help("Max Files Per Run", LIBRARY_SETTINGS_HELP["max_files"], "lib-max-files-edit-%d" % library.id),
                    priority_label=self._label_with_help("Priority", LIBRARY_SETTINGS_HELP["priority"], "lib-priority-edit-%d" % library.id),
                    qsv_quality_label=self._label_with_help("QSV Quality", LIBRARY_SETTINGS_HELP["qsv_quality"], "lib-qsv-quality-edit-%d" % library.id),
                    qsv_preset_label=self._label_with_help("QSV Preset", LIBRARY_SETTINGS_HELP["qsv_preset"], "lib-qsv-preset-edit-%d" % library.id),
                    min_savings_percent_label=self._label_with_help("Minimum Savings Percent", LIBRARY_SETTINGS_HELP["min_savings_percent"], "lib-min-savings-edit-%d" % library.id),
                    skip_codecs_label=self._label_with_help("Skip Codecs", LIBRARY_SETTINGS_HELP["skip_codecs"], "lib-skip-codecs-edit-%d" % library.id),
                    skip_min_height_label=self._label_with_help("Skip Minimum Height", LIBRARY_SETTINGS_HELP["skip_min_height"], "lib-skip-min-height-edit-%d" % library.id),
                    skip_resolution_tags_label=self._label_with_help("Skip Resolution Tags", LIBRARY_SETTINGS_HELP["skip_resolution_tags"], "lib-skip-resolution-tags-edit-%d" % library.id),
                    skip_codecs=_escape_html(str(library.skip_codecs or "")),
                    skip_min_height=_escape_html(str(max(0, int(library.skip_min_height or 0)))),
                    skip_resolution_tags=_escape_html(str(library.skip_resolution_tags or "")),
                    enabled_label=self._label_with_help("Enabled", LIBRARY_SETTINGS_HELP["enabled"], "lib-enabled-edit-%d" % library.id),
                    library_id=library.id,
                    enabled_yes="selected" if library.enabled else "",
                    enabled_no="selected" if not library.enabled else "",
                    actions="""
<form method=\"post\" action=\"/settings/libraries/toggle\" style=\"display: inline-block; margin-right: 0.4rem;\">
  <input type=\"hidden\" name=\"library_id\" value=\"{library_id}\" />
  <input type=\"hidden\" name=\"enabled\" value=\"{toggle_target}\" />
  <button type=\"submit\">{toggle_label}</button>
</form>
<form method=\"post\" action=\"/settings/libraries/delete\" style=\"display: inline-block;\">
  <input type=\"hidden\" name=\"library_id\" value=\"{library_id}\" />
  <button type=\"submit\">Delete</button>
</form>""".format(
                        library_id=library.id,
                        toggle_target=toggle_target,
                        toggle_label=toggle_label,
                    ),
                )
            )
        return """<table style=\"border-collapse: collapse; width: 100%%; border: 1px solid #ddd;\">
  <thead>
    <tr>
      <th style=\"text-align: left; padding: 0.35rem; border-bottom: 1px solid #ddd;\">Name</th>
      <th style=\"text-align: left; padding: 0.35rem; border-bottom: 1px solid #ddd;\">Path</th>
      <th style=\"text-align: left; padding: 0.35rem; border-bottom: 1px solid #ddd;\">Enabled</th>
      <th style=\"text-align: left; padding: 0.35rem; border-bottom: 1px solid #ddd;\">Priority</th>
      <th style=\"text-align: left; padding: 0.35rem; border-bottom: 1px solid #ddd;\">Schedule</th>
      <th style=\"text-align: left; padding: 0.35rem; border-bottom: 1px solid #ddd;\">Actions</th>
    </tr>
  </thead>
  <tbody>%s</tbody>
</table>""" % "".join(row_html)
    def _library_create_form_html(self) -> str:
        schedule_state = _schedule_form_state("")
        schedule_fields = self._schedule_fields_html(schedule_state, "create")
        return """
<h3 style="margin-top: 1rem;">Create Library</h3>
<form method="post" action="/settings/libraries/create">
  <label>{name_label}</label><br />
  <input name="name" style="width: 100%; max-width: 420px;" /><br />
  <label>{path_label}</label><br />
  <input name="path" style="width: 100%; max-width: 420px;" /><br />
  <fieldset style="margin-top: 0.5rem; padding: 0.5rem; border: 1px solid #ddd; max-width: 420px;">
    <legend><strong>Processing Settings</strong></legend>
    <label>{min_size_gb_label}</label><br />
    <input name="min_size_gb" type="number" step="0.1" min="0" value="0.0" style="width: 100%;" /><br />
    <label>{max_files_label}</label><br />
    <input name="max_files" type="number" step="1" min="1" value="1" style="width: 100%;" /><br />
    <label>{priority_label}</label><br />
    <input name="priority" type="number" step="1" value="100" style="width: 100%;" /><br />
    <small>Higher numbers run first when multiple libraries are queued.</small><br />
  </fieldset>
  <fieldset style="margin-top: 0.5rem; padding: 0.5rem; border: 1px solid #ddd; max-width: 420px;">
    <legend><strong>Encoding Settings</strong></legend>
    <label>{qsv_quality_label}</label><br />
    <input name="qsv_quality" type="number" step="1" min="0" value="{qsv_quality_default}" style="width: 100%;" /><br />
    <label>{qsv_preset_label}</label><br />
    <input name="qsv_preset" type="number" step="1" min="0" value="{qsv_preset_default}" style="width: 100%;" /><br />
    <label>{min_savings_percent_label}</label><br />
    <input name="min_savings_percent" type="number" step="0.1" min="0" value="{min_savings_percent_default}" style="width: 100%;" /><br />
  </fieldset>
  <fieldset style="margin-top: 0.5rem; padding: 0.5rem; border: 1px solid #ddd; max-width: 420px;">
    <legend><strong>Skip Settings</strong></legend>
    <label>{skip_codecs_label}</label><br />
    <input name="skip_codecs" value="{skip_codecs_default}" style="width: 100%;" /><br />
    <small>Comma-separated codecs to skip, such as hevc,av1.</small><br />
    <label>{skip_min_height_label}</label><br />
    <input name="skip_min_height" type="number" step="1" min="0" value="{skip_min_height_default}" style="width: 100%;" /><br />
    <small>Skip files at or above this vertical resolution.</small><br />
    <label>{skip_resolution_tags_label}</label><br />
    <input name="skip_resolution_tags" value="{skip_resolution_tags_default}" style="width: 100%;" /><br />
    <small>Comma-separated filename tags to skip, such as 2160p,4k,uhd.</small><br />
  </fieldset>
  {schedule_fields}
  <label>{enabled_label}</label>
  <select name="enabled"><option value="1" selected>Yes</option><option value="0">No</option></select>
  <div style="margin-top: 0.5rem;"><button type="submit">Create Library</button></div>
</form>
""".format(
    schedule_fields=schedule_fields,
    qsv_quality_default=_escape_html(_env_bootstrap("QSV_QUALITY", "21")),
    qsv_preset_default=_escape_html(_env_bootstrap("QSV_PRESET", "7")),
    min_savings_percent_default=_escape_html(_env_bootstrap("MIN_SAVINGS_PERCENT", "15")),
    skip_codecs_default=_escape_html(_normalize_csv_text(_env_bootstrap("SKIP_CODECS", ""))),
    skip_min_height_default=_escape_html(str(max(0, _env_int("SKIP_MIN_HEIGHT", 0)))),
    skip_resolution_tags_default=_escape_html(_normalize_csv_text(_env_bootstrap("SKIP_RESOLUTION_TAGS", ""))),
    name_label=self._label_with_help("Name", LIBRARY_SETTINGS_HELP["name"], "lib-name-create"),
    path_label=self._label_with_help("Path", LIBRARY_SETTINGS_HELP["path"], "lib-path-create"),
    min_size_gb_label=self._label_with_help("Minimum File Size (GB)", LIBRARY_SETTINGS_HELP["min_size_gb"], "lib-min-size-create"),
    max_files_label=self._label_with_help("Max Files Per Run", LIBRARY_SETTINGS_HELP["max_files"], "lib-max-files-create"),
    priority_label=self._label_with_help("Priority", LIBRARY_SETTINGS_HELP["priority"], "lib-priority-create"),
    qsv_quality_label=self._label_with_help("QSV Quality", LIBRARY_SETTINGS_HELP["qsv_quality"], "lib-qsv-quality-create"),
    qsv_preset_label=self._label_with_help("QSV Preset", LIBRARY_SETTINGS_HELP["qsv_preset"], "lib-qsv-preset-create"),
    min_savings_percent_label=self._label_with_help("Minimum Savings Percent", LIBRARY_SETTINGS_HELP["min_savings_percent"], "lib-min-savings-create"),
    skip_codecs_label=self._label_with_help("Skip Codecs", LIBRARY_SETTINGS_HELP["skip_codecs"], "lib-skip-codecs-create"),
    skip_min_height_label=self._label_with_help("Skip Minimum Height", LIBRARY_SETTINGS_HELP["skip_min_height"], "lib-skip-min-height-create"),
    skip_resolution_tags_label=self._label_with_help("Skip Resolution Tags", LIBRARY_SETTINGS_HELP["skip_resolution_tags"], "lib-skip-resolution-tags-create"),
    enabled_label=self._label_with_help("Enabled", LIBRARY_SETTINGS_HELP["enabled"], "lib-enabled-create"),
)

    def _schedule_fields_html(self, schedule_state: Dict[str, object], form_id: str) -> str:
        form_token = "".join(ch if ch.isalnum() else "_" for ch in str(form_id))
        mode = str(schedule_state.get("mode", "simple"))
        raw_value = _escape_html(str(schedule_state.get("raw", "")))
        simple_time = _escape_html(str(schedule_state.get("time", "00:00")))
        selected_days = set(schedule_state.get("days", []))
        simple_radio_id = "schedule-mode-simple-%s" % form_token
        advanced_radio_id = "schedule-mode-advanced-%s" % form_token
        simple_checked = "checked" if mode == "simple" else ""
        advanced_checked = "checked" if mode == "advanced" else ""

        weekday_options = []
        for label, day_value in WEEKDAY_CHOICES:
            checked = "checked" if day_value in selected_days else ""
            weekday_options.append(
                '<label style="margin-right: 0.5rem;"><input type="checkbox" name="schedule_day_%s" value="1" %s /> %s</label>'
                % (day_value, checked, label)
            )

        time_options = []
        for value in _simple_schedule_time_options():
            selected = "selected" if value == simple_time else ""
            time_options.append('<option value="%s" %s>%s</option>' % (_escape_html(value), selected, _escape_html(value)))

        simple_display = "block" if mode == "simple" else "none"
        advanced_display = "block" if mode == "advanced" else "none"
        preview = _escape_html(str(schedule_state.get("preview", "")))

        return """
  <fieldset style=\"margin-top: 0.5rem; padding: 0.5rem; border: 1px solid #ddd;\">
    <legend>%s</legend>
    <label style=\"margin-right: 1rem;\"><input id=\"%s\" type=\"radio\" name=\"schedule_mode\" value=\"simple\" %s onchange=\"toggleScheduleMode_%s()\" /> Simple</label>
    <label><input id=\"%s\" type=\"radio\" name=\"schedule_mode\" value=\"advanced\" %s onchange=\"toggleScheduleMode_%s()\" /> Advanced cron</label>

    <div id=\"simple-schedule-%s\" style=\"display:%s; margin-top: 0.5rem;\">
      <label>%s</label><br />
      %s
      <br />
      <label>%s</label><br />
      <select name=\"schedule_time\" style=\"width: 100%%; max-width: 180px;\">%s</select>
      <div style=\"margin-top: 0.35rem; color:#555;\">Generated cron: <code>%s</code></div>
    </div>

    <div id=\"advanced-schedule-%s\" style=\"display:%s; margin-top: 0.5rem;\">
      <label>%s</label><br />
      <input name=\"schedule\" value=\"%s\" style=\"width: 100%%; max-width: 420px;\" />
    </div>
  </fieldset>
  <script>
    function toggleScheduleMode_%s() {
      var simpleRadio = document.getElementById('%s');
      var simple = document.getElementById('simple-schedule-%s');
      var advanced = document.getElementById('advanced-schedule-%s');
      if (!simpleRadio || !simple || !advanced) { return; }
      if (simpleRadio.checked) {
        simple.style.display = 'block';
        advanced.style.display = 'none';
      } else {
        simple.style.display = 'none';
        advanced.style.display = 'block';
      }
    }
    toggleScheduleMode_%s();
  </script>
""" % (
            self._label_with_help("Schedule", LIBRARY_SETTINGS_HELP["schedule"], "lib-schedule-%s" % form_token),
            simple_radio_id,
            simple_checked,
            form_token,
            advanced_radio_id,
            advanced_checked,
            form_token,
            form_token,
            simple_display,
            self._label_with_help("Days", LIBRARY_SETTINGS_HELP["schedule_days"], "lib-schedule-days-%s" % form_token),
            "".join(weekday_options),
            self._label_with_help("Time", LIBRARY_SETTINGS_HELP["schedule_time"], "lib-schedule-time-%s" % form_token),
            "".join(time_options),
            preview,
            form_token,
            advanced_display,
            self._label_with_help("Raw cron expression", LIBRARY_SETTINGS_HELP["raw_cron"], "lib-schedule-raw-%s" % form_token),
            raw_value,
            form_token,
            simple_radio_id,
            form_token,
            form_token,
            form_token,
        )

    def register_jobs(self) -> None:
        for library in self.enabled_runtime_libraries():
            self._register_library_job(library)
        self._register_housekeeping_job()

    def _register_housekeeping_job(self) -> None:
        if CronTrigger is not None:
            trigger = _build_scheduler_cron_trigger("0 2 * * *", timezone=_cron_trigger_timezone())
        else:
            trigger = "0 2 * * *"
        add_job_kwargs = {
            "trigger": trigger,
            "id": "housekeeping-daily",
            "coalesce": True,
            "max_instances": 1,
            "replace_existing": True,
        }
        next_run_time = self._compute_initial_next_run_time(trigger)
        if next_run_time is not None:
            add_job_kwargs["next_run_time"] = next_run_time
        self.scheduler.add_job(
            self.run_housekeeping_once,
            **add_job_kwargs,
        )

    def run_housekeeping_once(self) -> None:
        with self._job_condition:
            if self._current_job is not None or len(self._job_queue) > 0:
                LOGGER.info("Skipping housekeeping while jobs are active")
                return
        self._record_activity(event_type="housekeeping_started", message="Daily housekeeping started")
        logger = Logger(str(self._settings_db_path.parent / "housekeeping.log"))
        try:
            work_root = Path(_env("WORK_ROOT", "/work"))
            log_dir = work_root / "logs"
            retention_days = _env_int("LOG_RETENTION_DAYS", 30)
            cleanup_logs(log_dir, retention_days, logger)
        finally:
            self._record_activity(event_type="housekeeping_completed", message="Daily housekeeping completed")

    def _register_library_job(self, library: RuntimeLibrary) -> None:
        raw_schedule = str(library.schedule or "").strip()
        schedule = _normalize_schedule_for_scheduler(raw_schedule)
        if not schedule:
            LOGGER.info("No schedule configured for %s; job disabled", library.name)
            return

        timezone = _cron_trigger_timezone()
        parsed_fields = _parse_scheduler_cron_fields(schedule)
        if CronTrigger is not None:
            try:
                trigger = _build_scheduler_cron_trigger(schedule, timezone=timezone, parsed_fields=parsed_fields)
            except ValueError:
                LOGGER.error("Invalid cron schedule for %s: %r", library.name, schedule)
                return
        else:
            if not _is_valid_crontab(schedule):
                LOGGER.error("Invalid cron schedule for %s: %r", library.name, schedule)
                return
            trigger = schedule

        add_job_kwargs = {
            "trigger": trigger,
            "id": self._schedule_job_id(library.id),
            "args": [library.id],
            "coalesce": True,
            "max_instances": 1,
            "replace_existing": True,
        }
        next_run_time = self._compute_initial_next_run_time(trigger)
        if next_run_time is not None:
            add_job_kwargs["next_run_time"] = next_run_time
        self.scheduler.add_job(
            self._scheduled_library_trigger,
            **add_job_kwargs,
        )
        registered_job = None
        get_job = getattr(self.scheduler, "get_job", None)
        if callable(get_job):
            try:
                registered_job = get_job(self._schedule_job_id(library.id))
            except Exception:
                registered_job = None

        raw_next_run_time = getattr(registered_job, "next_run_time", None)
        if raw_next_run_time is None and CronTrigger is not None and hasattr(trigger, "get_next_fire_time"):
            now = datetime.now(timezone) if hasattr(timezone, "utcoffset") else datetime.utcnow()
            raw_next_run_time = trigger.get_next_fire_time(None, now)
        next_run_time = None if raw_next_run_time is None else str(raw_next_run_time)
        job_id = getattr(registered_job, "id", self._schedule_job_id(library.id))
        LOGGER.info(
            "Registering %s schedule: raw=%r minute=%s hour=%s day=%r month=%r dow=%r timezone=%r trigger=%r job_id=%r next_run=%r",
            library.name,
            raw_schedule,
            parsed_fields["minute"],
            parsed_fields["hour"],
            parsed_fields["day"],
            parsed_fields["month"],
            parsed_fields["day_of_week"],
            str(timezone),
            str(trigger),
            job_id,
            next_run_time,
        )
        self._record_activity(
            event_type="schedule_registered",
            message="Scheduler registered %s schedule" % library.name,
            library=library.name,
        )

    def _compute_initial_next_run_time(self, trigger):
        if CronTrigger is None or not hasattr(trigger, "get_next_fire_time"):
            return None
        timezone = _cron_trigger_timezone()
        now = datetime.now(timezone) if hasattr(timezone, "utcoffset") else datetime.utcnow()
        return trigger.get_next_fire_time(None, now)

    def _scheduled_library_trigger(self, library_id: int) -> bool:
        library = self._library_by_id(library_id)
        library_name = str(getattr(library, "name", library_id))
        LOGGER.info("Scheduled trigger fired for %s", library_name)
        return self.trigger_library_by_id(library_id)

    def _schedule_job_id(self, library_id: int) -> str:
        return "library-%d-schedule" % int(library_id)

    def enabled_runtime_libraries(self) -> List[RuntimeLibrary]:
        return [
            RuntimeLibrary(
                id=library.id,
                name=library.name,
                path=library.path,
                schedule=library.schedule,
                min_size_gb=library.min_size_gb,
                max_files=library.max_files,
                priority=library.priority,
                qsv_quality=library.qsv_quality,
                qsv_preset=library.qsv_preset,
                min_savings_percent=library.min_savings_percent,
                skip_codecs=library.skip_codecs,
                skip_min_height=library.skip_min_height,
                skip_resolution_tags=library.skip_resolution_tags,
            )
            for library in self.list_libraries()
            if library.enabled
        ]

    def _library_by_id(self, library_id: int) -> Optional[RuntimeLibrary]:
        for library in self.enabled_runtime_libraries():
            if library.id == int(library_id):
                return library
        return None

    def _library_by_key(self, key: str) -> Optional[RuntimeLibrary]:
        target = str(key or "").strip()
        if target.isdigit():
            return self._library_by_id(int(target))
        lowered = target.lower()
        for library in self.enabled_runtime_libraries():
            if library.name.strip().lower() == lowered:
                return library
        return None

    def trigger_library_by_id(self, library_id: int) -> bool:
        library = self._library_by_id(library_id)
        if library is None:
            return False
        return self.trigger_library(library.name)

    def trigger_library(self, library: str) -> bool:
        library_record = self._library_by_key(library)
        if library_record is None:
            return False
        library_name = library_record.name
        self._record_activity(
            event_type="scheduled_run_requested",
            message="Scheduled run requested for %s" % library_name,
            library=library_name,
        )
        accepted = self._enqueue_library_job(library_record, trigger="schedule")
        if not accepted:
            LOGGER.info("%s run already queued or in progress; skipping overlapping schedule", library_name)
            self._record_activity(
                event_type="run_rejected_busy",
                message="%s run skipped because library is already queued or running" % library_name,
                library=library_name,
                level="warning",
            )
            return False
        return True

    def _library_lock_for_id(self, library_id: int) -> threading.Lock:
        key = str(int(library_id))
        if key not in self._library_locks:
            self._library_locks[key] = threading.Lock()
        library_record = self._library_by_id(int(library_id))
        if library_record is not None:
            self._library_locks[library_record.name.strip().lower()] = self._library_locks[key]
        return self._library_locks[key]

    def manual_run_payload_for_id(self, library_id: int):
        library_record = self._library_by_id(library_id)
        if library_record is None:
            return {"status": "not_found", "library_id": int(library_id)}, 404
        return self.manual_run_payload(library_record.name)

    def manual_preview_payload_for_id(self, library_id: int):
        library_record = self._library_by_id(library_id)
        if library_record is None:
            return {"status": "not_found", "library_id": int(library_id)}, 404
        return self.manual_run_payload(library_record.name, preview=True)

    def manual_run_payload(self, library: str, preview: bool = False):
        library_record = self._library_by_key(library)
        if library_record is None:
            return {"status": "not_found", "library": library}, 404
        library_name = library_record.name
        run_kind = "preview" if preview else "run"
        LOGGER.info("Manual %s %s request received", library_name, run_kind)
        self._record_activity(
            event_type="manual_preview_requested" if preview else "manual_run_requested",
            message="Manual %s requested for %s" % (run_kind, library_name),
            library=library_name,
        )
        queued = self._enqueue_library_job(library_record, trigger="preview" if preview else "manual")
        payload = {
            "status": "queued" if queued else "busy",
            "library": library,
            "library_id": library_record.id,
        }
        if queued:
            LOGGER.info("Manual %s %s accepted and queued", library_name, run_kind)
            return payload, 202

        LOGGER.info("Manual %s %s rejected; run already queued or in progress", library_name, run_kind)
        self._record_activity(
            event_type="run_rejected_busy",
            message="%s run skipped because library is already queued or running" % library_name,
            library=library_name,
            level="warning",
        )
        return payload, 409

    def request_cancel_active_run(self) -> Dict[str, str]:
        with self._job_condition:
            if self._current_job is None:
                return {"status": "idle"}
            self._cancel_requested = True
            self._current_run_snapshot["cancel_requested"] = "1"
            library_name = self._current_job.library_name
        self._record_activity(
            event_type="run_cancel_requested",
            message="Cancellation requested for %s" % library_name,
            library=library_name,
            run_id=self._current_job_run_id or None,
        )
        return {"status": "cancelling"}

    def _is_cancel_requested(self) -> bool:
        with self._job_condition:
            return bool(self._cancel_requested)

    def _on_run_cancelled(self, stage: str) -> None:
        with self._job_condition:
            self._current_run_snapshot["cancel_requested"] = "1"
            self._current_run_snapshot["cancel_stage"] = str(stage)

    def _enqueue_library_job(self, library: RuntimeLibrary, trigger: str) -> bool:
        job = RuntimeJob(library_id=library.id, library_name=library.name, trigger=trigger, priority=library.priority)
        with self._job_condition:
            if job.library_id in self._queued_or_running_library_ids:
                return False
            self._job_queue.append(job)
            self._queued_or_running_library_ids.add(job.library_id)
            self._job_condition.notify()
            queue_depth = len(self._job_queue)
        self._record_activity(
            event_type="job_queued",
            message="%s run queued (%s trigger)" % (job.library_name, job.trigger),
            library=job.library_name,
        )
        LOGGER.info("Queued %s run via %s trigger (queue depth=%s)", job.library_name, job.trigger, queue_depth)
        return True

    def _worker_loop(self) -> None:
        while True:
            with self._job_condition:
                while not self._job_queue and not self._worker_shutdown:
                    self._job_condition.wait(timeout=0.5)
                if self._worker_shutdown:
                    return
                job = self._pop_next_job_locked()
                self._current_job = job
                self._current_job_started_at = _utc_timestamp()
                self._current_job_run_id = ""
                self._cancel_requested = False
                self._last_run_was_cancelled = False
                self._current_run_snapshot = {}

            lock = self._library_lock_for_id(job.library_id)
            lock_acquired = lock.acquire(blocking=False)
            self._record_activity(
                event_type="job_started",
                message="%s queued job started (%s trigger)" % (job.library_name, job.trigger),
                library=job.library_name,
            )
            try:
                if not lock_acquired:
                    self._record_activity(
                        event_type="run_rejected_busy",
                        message="%s queued job skipped because library is already busy" % job.library_name,
                        library=job.library_name,
                        level="warning",
                    )
                else:
                    self._run_library_once(job.library_name.lower(), trigger=job.trigger)
            except Exception:
                LOGGER.exception("Queued job failed for %s", job.library_name)
            finally:
                if lock_acquired:
                    lock.release()
                with self._job_condition:
                    self._queued_or_running_library_ids.discard(job.library_id)
                    if job.trigger == "preview":
                        preview_results = self._extract_preview_results(self._current_run_snapshot)
                        preview_generated_at = str(self._current_run_snapshot.get("preview_generated_at", "") or "").strip() or _utc_timestamp()
                        snapshot = {
                            "library_id": job.library_id,
                            "library_name": job.library_name,
                            "generated_at": preview_generated_at,
                            "results": list(preview_results),
                        }
                        self._last_preview_snapshots_by_library[job.library_id] = snapshot
                        self._latest_preview_library_id = job.library_id
                        self._last_preview_results = list(preview_results)
                    self._current_job = None
                    self._current_job_started_at = ""
                    self._current_job_run_id = ""
                    self._cancel_requested = False
                    self._current_run_snapshot = {}
                self._record_activity(
                    event_type="job_completed",
                    message="%s queued job completed" % job.library_name,
                    library=job.library_name,
                )

    def _pop_next_job_locked(self) -> RuntimeJob:
        selected_index = 0
        selected_priority = self._job_queue[0].priority
        for index, job in enumerate(self._job_queue):
            if job.priority > selected_priority:
                selected_index = index
                selected_priority = job.priority

        if selected_index == 0:
            return self._job_queue.popleft()

        selected_job = self._job_queue[selected_index]
        del self._job_queue[selected_index]
        return selected_job

    def _runtime_status_snapshot(self) -> Dict[str, str]:
        scheduler_snapshot = self._scheduler_status_snapshot()
        with self._job_condition:
            queue_depth = len(self._job_queue)
            current_job = self._current_job
            started_at = self._current_job_started_at
            run_id = self._current_job_run_id
            run_snapshot = dict(self._current_run_snapshot)
            cancel_requested = bool(self._cancel_requested)
            last_run_was_cancelled = bool(self._last_run_was_cancelled)
        if current_job is not None:
            status = "Cancelling" if cancel_requested else "Running"
        elif queue_depth > 0:
            status = "Queued"
        elif last_run_was_cancelled:
            status = "Cancelled"
        else:
            status = "Idle"
        preview_results = []
        preview_library = ""
        preview_generated_at = ""
        if current_job is not None:
            preview_results = self._extract_preview_results(run_snapshot)
            if current_job.trigger == "preview":
                preview_library = current_job.library_name
                preview_generated_at = str(run_snapshot.get("preview_generated_at", "") or "").strip() or started_at
        else:
            latest_preview = self._latest_preview_snapshot()
            if latest_preview is not None:
                preview_results = list(latest_preview.get("results") or [])
                preview_library = str(latest_preview.get("library_name", "") or "")
                preview_generated_at = str(latest_preview.get("generated_at", "") or "")
            else:
                preview_results = list(self._last_preview_results)

        return {
            "status": status,
            "current_library": current_job.library_name if current_job is not None else "",
            "current_trigger": current_job.trigger if current_job is not None else "",
            "queue_depth": str(queue_depth),
            "run_id": run_id,
            "started_at": started_at,
            "current_file": str(run_snapshot.get("current_file", "")),
            "candidates_found": str(run_snapshot.get("candidates_found", "")),
            "files_evaluated": str(run_snapshot.get("files_evaluated", run_snapshot.get("evaluated_count", ""))),
            "files_processed": str(run_snapshot.get("files_processed", run_snapshot.get("processed_count", ""))),
            "files_skipped": str(run_snapshot.get("files_skipped", run_snapshot.get("skipped_count", ""))),
            "files_failed": str(run_snapshot.get("files_failed", run_snapshot.get("failed_count", ""))),
            "bytes_saved": str(run_snapshot.get("bytes_saved", "")),
            "encode_percent": str(run_snapshot.get("encode_percent", "")),
            "encode_speed": str(run_snapshot.get("encode_speed", "")),
            "encode_eta": str(run_snapshot.get("encode_eta", "")),
            "encode_out_time": str(run_snapshot.get("encode_out_time", "")),
            "retry_attempt": str(run_snapshot.get("retry_attempt", "")),
            "retry_max": str(run_snapshot.get("retry_max", "")),
            "mode": str(run_snapshot.get("mode", "Preview" if (current_job is not None and current_job.trigger == "preview") else "Live") or "Live"),
            "preview_results": preview_results,
            "preview_library": preview_library,
            "preview_generated_at": preview_generated_at,
            "cancel_requested": "1" if cancel_requested else "0",
            "scheduler_status": scheduler_snapshot["status"],
            "scheduler_started_at": scheduler_snapshot["started_at"],
            "next_scheduled_job": scheduler_snapshot["next_job"],
            "next_scheduled_time": scheduler_snapshot["next_time"],
        }

    def _latest_preview_snapshot(self) -> Optional[Dict[str, object]]:
        library_id = self._latest_preview_library_id
        if library_id is None:
            return None
        snapshot = self._last_preview_snapshots_by_library.get(library_id)
        if snapshot is None:
            return None
        return dict(snapshot)

    def _runtime_status_html(self) -> str:
        snapshot = self._runtime_status_snapshot()
        idle_placeholder = "-"
        current_library = snapshot["current_library"] or idle_placeholder
        current_trigger = snapshot["current_trigger"] or idle_placeholder
        run_id = snapshot["run_id"] or idle_placeholder
        started_at = snapshot["started_at"] or idle_placeholder
        current_file = snapshot["current_file"] or ("Waiting for first file" if snapshot["status"] == "Running" else idle_placeholder)
        return """<table style=\"border-collapse: collapse; width: 100%%; border: 1px solid #ddd; background: #fff;\">
  <tbody>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem; width: 250px;\">Status</th><td id=\"runtime-status\" style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Current Library</th><td id=\"runtime-library\" style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Trigger</th><td id=\"runtime-trigger\" style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Scheduler Status</th><td id=\"runtime-scheduler-status\" style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Scheduler Started</th><td id=\"runtime-scheduler-started\" style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Next Scheduled Job</th><td id=\"runtime-next-scheduled-job\" style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Next Scheduled Time</th><td id=\"runtime-next-scheduled-time\" style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Mode</th><td id=\"runtime-mode\" style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Queue Depth</th><td id=\"runtime-queue-depth\" style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Current Run ID</th><td id=\"runtime-run-id\" style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Started At</th><td id=\"runtime-started-at\" style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Current File</th><td id=\"runtime-current-file\" style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Candidates Found</th><td id=\"runtime-candidates-found\" style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Files Evaluated</th><td id=\"runtime-files-evaluated\" style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Files Processed</th><td id=\"runtime-files-processed\" style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Files Skipped</th><td id=\"runtime-files-skipped\" style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Files Failed</th><td id=\"runtime-files-failed\" style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; padding: 0.35rem;\">Bytes Saved So Far</th><td id=\"runtime-bytes-saved\" style=\"padding: 0.35rem;\">%s</td></tr>
  </tbody>
</table><div style=\"margin-top:0.6rem;\"><button id=\"runtime-stop-button\" type=\"button\" style=\"display:none;\">Stop Run</button></div><div id=\"runtime-progress-section\">%s</div>%s""" % (
            _escape_html(snapshot["status"]),
            _escape_html(current_library),
            _escape_html(_display_trigger(current_trigger)),
            _escape_html(snapshot.get("scheduler_status") or "-"),
            _escape_html(_format_readable_timestamp(snapshot.get("scheduler_started_at") or "-")),
            _escape_html(snapshot.get("next_scheduled_job") or "-"),
            _escape_html(snapshot.get("next_scheduled_time") or "-"),
            _escape_html(snapshot.get("mode", "Live") or "Live"),
            _escape_html(snapshot["queue_depth"]),
            _escape_html(run_id),
            _escape_html(started_at),
            _escape_html(current_file),
            _escape_html(snapshot["candidates_found"] or "-"),
            _escape_html(snapshot["files_evaluated"] or "-"),
            _escape_html(snapshot["files_processed"] or "-"),
            _escape_html(snapshot["files_skipped"] or "-"),
            _escape_html(snapshot["files_failed"] or "-"),
            _escape_html(_format_saved_bytes(snapshot["bytes_saved"]) if snapshot["bytes_saved"] else "-"),
            self._runtime_progress_overview_html(snapshot),
            self._preview_results_html(snapshot),
        )

    def _preview_results_html(self, snapshot: Dict[str, str]) -> str:
        rows = snapshot.get("preview_results") or []
        preview_library = str(snapshot.get("preview_library", "") or "").strip()
        preview_generated_at = str(snapshot.get("preview_generated_at", "") or "").strip()
        details = '<div style="margin-bottom:0.35rem;"><strong>Library:</strong> <span id="runtime-preview-library">%s</span></div><div style="margin-bottom:0.35rem;"><strong>Generated At:</strong> <span id="runtime-preview-generated-at">%s</span></div>' % (
            _escape_html(preview_library or "-"),
            _escape_html(preview_generated_at or "-"),
        )
        body_rows = []
        for row in rows[:25]:
            savings_pct = row.get("estimated_savings_pct", "")
            savings_label = "%s%%" % savings_pct if savings_pct != "" else "-"
            body_rows.append(
                '<tr><td style="border-top:1px solid #ddd; padding:0.3rem;">%s</td><td style="border-top:1px solid #ddd; padding:0.3rem;">%s</td><td style="border-top:1px solid #ddd; padding:0.3rem;">%s</td><td style="border-top:1px solid #ddd; padding:0.3rem;">%s</td><td style="border-top:1px solid #ddd; padding:0.3rem;">%s</td></tr>' 
                % (
                    _escape_html(str(row.get("file", "-"))),
                    _escape_html(_format_saved_bytes(row.get("original_size"))),
                    _escape_html(_format_saved_bytes(row.get("estimated_size"))),
                    _escape_html(str(savings_label)),
                    _escape_html(str(row.get("decision", "-"))),
                )
            )
        if not body_rows:
            body_rows.append('<tr><td colspan="5" style="padding: 0.35rem;">No preview results yet.</td></tr>')
        return """<div id="runtime-preview-results" style="margin-top:0.8rem;"><div style="display:flex; justify-content:space-between; align-items:center; gap:0.5rem; margin-bottom:0.35rem;"><div style="font-weight:600;">Preview Results</div><button id="runtime-clear-preview-button" type="button" style="font-size:0.85rem;">Clear Preview Results</button></div><div style="border:1px solid #d7e2f4; background:#f8fbff; padding:0.45rem; margin-bottom:0.5rem;">%s</div><table style="border-collapse: collapse; width: 100%%; border: 1px solid #ddd; background: #fff;"><thead><tr><th style="text-align:left; padding:0.35rem;">File</th><th style="text-align:left; padding:0.35rem;">Original Size</th><th style="text-align:left; padding:0.35rem;">Estimated Size</th><th style="text-align:left; padding:0.35rem;">Savings %%</th><th style="text-align:left; padding:0.35rem;">Decision</th></tr></thead><tbody id="runtime-preview-results-body">%s</tbody></table></div>""" % (details, "".join(body_rows))

    def clear_preview_results(self) -> Dict[str, str]:
        with self._job_state_lock:
            self._last_preview_results = []
            self._last_preview_snapshots_by_library = {}
            self._latest_preview_library_id = None
        return {"status": "cleared"}

    def _runtime_progress_overview_html(self, snapshot: Dict[str, str]) -> str:
        if snapshot.get("status") != "Running":
            return ""

        processed = self._snapshot_int(snapshot, "files_processed")
        candidates = self._snapshot_int(snapshot, "candidates_found")
        if candidates > 0:
            ratio = min(1.0, float(processed) / float(candidates))
            progress_label = "%s / %s files processed" % (processed, candidates)
        else:
            ratio = 1.0 if processed > 0 else 0.0
            progress_label = "%s files processed" % processed
        pct_label = "%.0f%%" % (ratio * 100.0)
        encode_percent_raw = str(snapshot.get("encode_percent", "") or "").strip()
        if encode_percent_raw:
            try:
                pct_label = "%.0f%%" % max(0.0, min(100.0, float(encode_percent_raw)))
            except Exception:
                pass
        encode_speed = str(snapshot.get("encode_speed", "") or "").strip() or "-"
        encode_eta = _format_eta_seconds(snapshot.get("encode_eta", ""))
        retry_attempt = self._snapshot_int(snapshot, "retry_attempt")
        retry_max = self._snapshot_int(snapshot, "retry_max")
        retry_line = ""
        if retry_attempt > 0 and retry_max >= retry_attempt:
            retry_line = '  <div><strong>Retry Attempt:</strong> %s / %s</div>\n' % (_escape_html(str(retry_attempt)), _escape_html(str(retry_max)))

        return """
<div style=\"margin-top:0.75rem; padding:0.6rem; border:1px solid #d7e2f4; background:#f8fbff;\">
  <div style=\"font-weight:600; margin-bottom:0.35rem;\">Run Progress</div>
  <div style=\"border:1px solid #c8d8f0; background:#eef4ff; width:100%%; height:18px;\">
    <div style=\"background:#2a6fd6; width:%s; height:100%%;\"></div>
  </div>
  <div style=\"margin-top:0.35rem;\">%s (%s)</div>
  <div style=\"margin-top:0.35rem;\"><strong>Percent Complete:</strong> %s</div>
  <div><strong>Speed:</strong> %s</div>
  <div><strong>ETA:</strong> %s</div>
%s  <div style=\"margin-top:0.55rem;\"><strong>Current Library:</strong> %s</div>
  <div><strong>Current File:</strong> %s</div>
  <div style=\"margin-top:0.4rem;\"><strong>Files Evaluated:</strong> %s</div>
  <div><strong>Files Processed:</strong> %s</div>
  <div><strong>Files Skipped:</strong> %s</div>
  <div><strong>Files Failed:</strong> %s</div>
  <div><strong>Total Saved:</strong> %s</div>
</div>
""" % (
            _escape_html(pct_label),
            _escape_html(progress_label),
            _escape_html(pct_label),
            _escape_html(pct_label),
            _escape_html(encode_speed),
            _escape_html(encode_eta),
            retry_line,
            _escape_html(snapshot.get("current_library") or "-"),
            _escape_html(snapshot.get("current_file") or "Waiting for first file"),
            _escape_html(str(self._snapshot_int(snapshot, "files_evaluated"))),
            _escape_html(str(processed)),
            _escape_html(str(self._snapshot_int(snapshot, "files_skipped"))),
            _escape_html(str(self._snapshot_int(snapshot, "files_failed"))),
            _escape_html(_format_saved_bytes(snapshot["bytes_saved"]) if snapshot["bytes_saved"] else "0 B"),
        )

    def _snapshot_int(self, snapshot: Dict[str, str], key: str) -> int:
        raw_value = str(snapshot.get(key, "") or "").strip()
        if not raw_value:
            return 0
        try:
            return int(raw_value)
        except Exception:
            return 0

    def _update_runtime_progress(self, values: Dict[str, object]) -> None:
        if not values:
            return
        aliases = {
            "evaluated_count": "files_evaluated",
            "processed_count": "files_processed",
            "skipped_count": "files_skipped",
            "failed_count": "files_failed",
        }
        with self._job_condition:
            if values.get("preview_result_json"):
                results_raw = str(self._current_run_snapshot.get("preview_results_json", "") or "").strip()
                try:
                    results = json.loads(results_raw) if results_raw else []
                except Exception:
                    results = []
                if not isinstance(results, list):
                    results = []
                try:
                    parsed = json.loads(str(values.get("preview_result_json")))
                    if isinstance(parsed, dict):
                        results.append(parsed)
                        if not str(self._current_run_snapshot.get("preview_generated_at", "") or "").strip():
                            self._current_run_snapshot["preview_generated_at"] = _utc_timestamp()
                except Exception:
                    pass
                self._current_run_snapshot["preview_results_json"] = json.dumps(results[:25])
            for key, value in values.items():
                if value is None:
                    continue
                if str(key) in ("preview_result", "preview_result_json"):
                    continue
                key_text = str(key)
                value_text = str(value)
                self._current_run_snapshot[key_text] = value_text
                alias_key = aliases.get(key_text)
                if alias_key:
                    self._current_run_snapshot[alias_key] = value_text

    def _extract_preview_results(self, run_snapshot: Dict[str, object]) -> List[Dict[str, object]]:
        preview_results: List[Dict[str, object]] = []
        preview_results_json = str(run_snapshot.get("preview_results_json", "") or "").strip()
        if not preview_results_json:
            return preview_results
        try:
            parsed = json.loads(preview_results_json)
        except Exception:
            return preview_results
        if not isinstance(parsed, list):
            return preview_results
        for row in parsed[:25]:
            if isinstance(row, dict):
                preview_results.append(row)
        return preview_results

    def _latest_run_status(self, library: str) -> Optional[Dict[str, str]]:
        db_path = Path(_env("STATS_PATH", "/config/chonk.db"))
        if not db_path.exists():
            return None

        try:
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT library, ts_end, ts_start, success_count, failed_count, skipped_count, duration_seconds
                       , processed_count, saved_bytes, run_id
                FROM runs
                WHERE lower(COALESCE(library, '')) = lower(?)
                ORDER BY ts_end DESC
                LIMIT 1
                """,
                (library,),
            ).fetchone()
        except Exception:
            conn.close()
            return None

        if row is None:
            return None

        was_cancelled = False
        try:
            run_id = str(row["run_id"] or "")
            if run_id:
                cancelled_row = conn.execute(
                    "SELECT 1 FROM encodes WHERE run_id = ? AND lower(COALESCE(skip_reason, '')) = 'cancelled' LIMIT 1",
                    (run_id,),
                ).fetchone()
                was_cancelled = cancelled_row is not None
        except Exception:
            was_cancelled = False

        status = _derive_run_status(
            success_count=int(row["success_count"] or 0),
            failed_count=int(row["failed_count"] or 0),
            skipped_count=int(row["skipped_count"] or 0),
            was_cancelled=was_cancelled,
        )
        conn.close()
        return {
            "library": str(row["library"] or library),
            "ts_end": str(row["ts_end"] or ""),
            "ts_start": str(row["ts_start"] or ""),
            "status": status,
            "duration_seconds": _format_duration_seconds(row["duration_seconds"]),
            "processed_count": int(row["processed_count"] or 0),
            "saved_bytes": int(row["saved_bytes"] or 0),
        }

    def _status_block_html(self, status: Optional[Dict[str, str]]) -> str:
        if status is None:
            return '<div style="padding: 0.5rem; border: 1px solid #ddd;">No runs recorded yet.</div>'

        return """<div style=\"padding: 0.5rem; border: 1px solid #ddd;\">
  <div><strong>Status:</strong> %s</div>
  <div><strong>Last run:</strong> %s</div>
  <div><strong>Duration:</strong> %s</div>
</div>""" % (
            status["status"],
            status["ts_end"] or status["ts_start"] or "Unknown",
            status["duration_seconds"],
        )

    def _recent_runs(self, limit: int = 10) -> List[Dict[str, str]]:
        db_path = Path(_env("STATS_PATH", "/config/chonk.db"))
        if not db_path.exists():
            return []

        try:
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT ts_end, ts_start, library, success_count, failed_count, skipped_count, duration_seconds, saved_bytes
                FROM runs
                ORDER BY ts_end DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
            conn.close()
        except Exception:
            return []

        result: List[Dict[str, str]] = []
        for row in rows:
            status = _derive_run_status(
                success_count=int(row["success_count"] or 0),
                failed_count=int(row["failed_count"] or 0),
                skipped_count=int(row["skipped_count"] or 0),
            )
            result.append(
                {
                    "time": str(row["ts_end"] or row["ts_start"] or "Unknown"),
                    "library": str(row["library"] or "Unknown"),
                    "status": status,
                    "duration": _format_duration_seconds(row["duration_seconds"]),
                    "saved": _format_saved_bytes(row["saved_bytes"]),
                }
            )
        return result

    def _run_history(self, limit: int = 50) -> List[Dict[str, str]]:
        db_path = Path(_env("STATS_PATH", "/config/chonk.db"))
        if not db_path.exists():
            return []

        try:
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT run_id, ts_end, ts_start, mode, library, success_count, failed_count, skipped_count,
                       duration_seconds, saved_bytes
                FROM runs
                ORDER BY ts_end DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
            conn.close()
        except Exception:
            return []

        result: List[Dict[str, str]] = []
        for row in rows:
            success_count = int(row["success_count"] or 0)
            failed_count = int(row["failed_count"] or 0)
            skipped_count = int(row["skipped_count"] or 0)
            saved_bytes = self._run_total_saved_bytes(str(row["run_id"] or ""), int(row["saved_bytes"] or 0))
            duration_seconds = _duration_seconds_from_run(row["ts_start"], row["ts_end"], row["duration_seconds"])
            result.append(
                {
                    "time": str(row["ts_end"] or row["ts_start"] or "Unknown"),
                    "library": str(row["library"] or "Unknown"),
                    "mode": _display_run_mode(str(row["mode"] or "")),
                    "result": _derive_run_status(
                        success_count=success_count,
                        failed_count=failed_count,
                        skipped_count=skipped_count,
                    ),
                    "duration": _format_duration_seconds(duration_seconds),
                    "processed": str(success_count + failed_count + skipped_count),
                    "skipped": str(skipped_count),
                    "failed": str(failed_count),
                    "saved": _run_saved_mb_gb_label(saved_bytes),
                    "run_id": str(row["run_id"] or "-"),
                }
            )
        return result

    def _run_detail(self, run_id: str) -> Optional[Dict[str, str]]:
        db_path = Path(_env("STATS_PATH", "/config/chonk.db"))
        if not db_path.exists():
            return None

        requested_columns = [
            "run_id",
            "ts_start",
            "ts_end",
            "mode",
            "library",
            "candidates_found",
            "evaluated_count",
            "processed_count",
            "success_count",
            "skipped_count",
            "failed_count",
            "saved_bytes",
            "duration_seconds",
            "prefiltered_count",
            "prefiltered_marker_count",
            "prefiltered_backup_count",
            "prefiltered_recent_count",
            "skipped_codec_count",
            "skipped_resolution_count",
            "skipped_min_savings_count",
            "skipped_max_savings_count",
            "skipped_dry_run_count",
            "ignored_folder_count",
            "ignored_file_count",
            "raw_log_path",
        ]

        conn = None
        try:
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            existing_columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(runs)").fetchall()}
            columns = [col for col in requested_columns if col in existing_columns]
            if not columns:
                conn.close()
                return None
            row = conn.execute(
                "SELECT %s FROM runs WHERE run_id = ? LIMIT 1" % ", ".join(columns),
                (run_id,),
            ).fetchone()
        except Exception:
            if conn is not None:
                conn.close()
            return None

        if row is None:
            conn.close()
            return None

        was_cancelled = False
        try:
            cancelled_row = conn.execute(
                "SELECT 1 FROM encodes WHERE run_id = ? AND lower(COALESCE(skip_reason, '')) = 'cancelled' LIMIT 1",
                (run_id,),
            ).fetchone()
            was_cancelled = cancelled_row is not None
        except Exception:
            was_cancelled = False

        trigger_type = ""
        try:
            trigger_row = conn.execute(
                """
                SELECT event_type
                FROM activity_events
                WHERE run_id = ? AND event_type IN ('manual_run_requested', 'manual_preview_requested', 'scheduled_run_requested')
                ORDER BY id DESC
                LIMIT 1
                """,
                (run_id,),
            ).fetchone()
            if trigger_row is not None:
                trigger_type = str(trigger_row["event_type"] or "")
        except Exception:
            trigger_type = ""

        result: Dict[str, str] = {
            "result": _derive_run_status(
                success_count=int(row["success_count"] or 0),
                failed_count=int(row["failed_count"] or 0),
                skipped_count=int(row["skipped_count"] or 0),
                was_cancelled=was_cancelled,
            ),
            "was_cancelled": "1" if was_cancelled else "0",
            "trigger_type": trigger_type,
        }
        for key in requested_columns:
            if key in row.keys():
                result[key] = row[key]
        conn.close()
        return result

    def _run_total_saved_bytes(self, run_id: str, fallback_saved_bytes: int = 0) -> int:
        db_path = Path(_env("STATS_PATH", "/config/chonk.db"))
        if not db_path.exists() or not run_id:
            return int(fallback_saved_bytes or 0)
        try:
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT COALESCE(SUM(CASE
                    WHEN size_before_bytes IS NOT NULL AND size_after_bytes IS NOT NULL AND size_before_bytes >= size_after_bytes
                    THEN size_before_bytes - size_after_bytes
                    ELSE COALESCE(saved_bytes, 0)
                END), 0) AS total_saved
                FROM encodes
                WHERE run_id = ?
                """,
                (run_id,),
            ).fetchone()
            conn.close()
            return int(row["total_saved"] or 0) if row is not None else int(fallback_saved_bytes or 0)
        except Exception:
            return int(fallback_saved_bytes or 0)

    def _run_successful_encode_count(self, run_id: str, fallback_success_count: int = 0) -> int:
        db_path = Path(_env("STATS_PATH", "/config/chonk.db"))
        if not db_path.exists() or not run_id:
            return int(fallback_success_count or 0)
        try:
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT COUNT(*) AS success_count FROM encodes WHERE run_id = ? AND lower(COALESCE(status, '')) = 'success'",
                (run_id,),
            ).fetchone()
            conn.close()
            return int(row["success_count"] or 0) if row is not None else int(fallback_success_count or 0)
        except Exception:
            return int(fallback_success_count or 0)

    def _encodes_for_run(self, run_id: str) -> List[Dict[str, str]]:
        db_path = Path(_env("STATS_PATH", "/config/chonk.db"))
        if not db_path.exists():
            return []

        try:
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT ts, path, filename, status, codec_from, codec_to,
                       size_before_bytes, size_after_bytes, saved_bytes, duration_seconds,
                       skip_reason, skip_detail, fail_stage, error_type, error_msg
                FROM encodes
                WHERE run_id = ?
                ORDER BY ts ASC, id ASC
                """,
                (run_id,),
            ).fetchall()
            conn.close()
        except Exception:
            return []

        result: List[Dict[str, str]] = []
        for row in rows:
            reason = ""
            if row["skip_reason"]:
                reason = str(row["skip_reason"])
                if row["skip_detail"]:
                    reason += ": %s" % str(row["skip_detail"])
            elif row["error_type"]:
                reason = str(row["error_type"])
                if row["error_msg"]:
                    reason += ": %s" % str(row["error_msg"])
            elif row["fail_stage"]:
                reason = "fail_stage: %s" % str(row["fail_stage"])

            path = str(row["path"] or row["filename"] or "-")
            codec_info = "-"
            if row["codec_from"] and row["codec_to"]:
                codec_info = "%s -> %s" % (row["codec_from"], row["codec_to"])
            elif row["codec_from"]:
                codec_info = str(row["codec_from"])

            result.append(
                {
                    "ts": str(row["ts"] or ""),
                    "path": path,
                    "status": str(row["status"] or "unknown"),
                    "codec_info": codec_info,
                    "before": _format_saved_bytes(row["size_before_bytes"]),
                    "after": _format_saved_bytes(row["size_after_bytes"]),
                    "saved": _format_saved_bytes(row["saved_bytes"]),
                    "encode_duration": _format_duration_seconds(row["duration_seconds"]) if row["duration_seconds"] is not None else "-",
                    "reason": reason or "-",
                }
            )
        return result

    def _recent_runs_html(self, rows: List[Dict[str, str]]) -> str:
        if not rows:
            return '<div style="padding: 0.5rem; border: 1px solid #ddd;">No recent runs recorded yet.</div>'

        row_html = []
        for row in rows:
            row_html.append(
                "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
                % (row["time"], row["library"], row["status"], row["duration"], row["saved"])
            )

        return """<table style=\"border-collapse: collapse; width: 100%%; border: 1px solid #ddd;\">
  <thead>
    <tr>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Time</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Library</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Status</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Duration</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Saved</th>
    </tr>
  </thead>
  <tbody>
    %s
  </tbody>
</table>""" % "".join(row_html)

    def _runs_history_html(self, rows: List[Dict[str, str]]) -> str:
        if not rows:
            return '<div style="padding: 0.5rem; border: 1px solid #ddd;">No runs recorded yet.</div>'

        row_html = []
        for row in rows:
            run_id = _escape_html(row["run_id"])
            row_html.append(
                "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
                % (
                    _escape_html(row["time"]),
                    _escape_html(row["library"]),
                    _escape_html(row["mode"]),
                    _escape_html(row["result"]),
                    _escape_html(row["processed"]),
                    _escape_html(row["skipped"]),
                    _escape_html(row["failed"]),
                    _escape_html(row["saved"]),
                    _escape_html(row["duration"]),
                    '<a href="/runs/%s">%s</a>' % (run_id, run_id),
                )
            )

        return """<table style=\"border-collapse: collapse; width: 100%%; border: 1px solid #ddd;\">
  <thead>
    <tr>
      <th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;">Time</th>
      <th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;">Library</th>
      <th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;">Mode</th>
      <th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;">Result</th>
      <th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;">Processed</th>
      <th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;">Skipped</th>
      <th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;">Failed</th>
      <th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;">Saved</th>
      <th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;">Duration</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Run ID</th>
    </tr>
  </thead>
  <tbody>
    %s
  </tbody>
</table>""" % "".join(row_html)

    def _run_summary_html(self, run: Dict[str, str]) -> str:
        summary_rows = [
            ("Run ID", _escape_html(str(run.get("run_id") or "-"))),
            ("Library", _escape_html(str(run.get("library") or "Unknown"))),
            ("Trigger Type", _escape_html(_display_run_trigger(str(run.get("trigger_type") or "")))),
            ("Mode", _escape_html(_display_run_mode(str(run.get("mode") or "")))),
            ("Started At", _escape_html(_format_readable_timestamp(run.get("ts_start")))),
            ("Completed At", _escape_html(_format_readable_timestamp(run.get("ts_end")))),
            ("Duration", _escape_html(_format_duration_seconds(_duration_seconds_from_run(run.get("ts_start"), run.get("ts_end"), run.get("duration_seconds"))))),
        ]
        outcome_rows = [
            ("Result", _escape_html(str(run.get("result") or "completed"))),
            ("Cancellation", "Cancelled" if str(run.get("was_cancelled") or "0") == "1" else "Not Cancelled"),
            ("Retry Attempts", "Not recorded"),
        ]
        count_rows = [
            ("Candidates Found", _escape_html(str(run.get("candidates_found") or 0))),
            ("Evaluated", _escape_html(str(run.get("evaluated_count") or 0))),
            ("Processed", _escape_html(str(run.get("processed_count") or 0))),
            ("Success", _escape_html(str(run.get("success_count") or 0))),
            ("Skipped", _escape_html(str(run.get("skipped_count") or 0))),
            ("Failed", _escape_html(str(run.get("failed_count") or 0))),
        ]
        total_saved_bytes = self._run_total_saved_bytes(str(run.get("run_id") or ""), int(run.get("saved_bytes") or 0))
        successful_encodes = self._run_successful_encode_count(str(run.get("run_id") or ""), int(run.get("success_count") or 0))
        average_saved_bytes = int(total_saved_bytes / successful_encodes) if successful_encodes > 0 else 0
        savings_rows = [("Total Saved", _escape_html(_run_saved_mb_gb_label(total_saved_bytes))), ("Avg Saved / File", _escape_html(_run_saved_mb_gb_label(average_saved_bytes) if successful_encodes > 0 else "0.0 MB"))]

        optional_rows = []
        optional_fields = [
            ("Prefiltered", "prefiltered_count"),
            ("Prefiltered Marker", "prefiltered_marker_count"),
            ("Prefiltered Backup", "prefiltered_backup_count"),
            ("Prefiltered Recent", "prefiltered_recent_count"),
            ("Skipped Codec", "skipped_codec_count"),
            ("Skipped Resolution", "skipped_resolution_count"),
            ("Skipped Min Savings", "skipped_min_savings_count"),
            ("Skipped Max Savings", "skipped_max_savings_count"),
            ("Skipped Dry Run", "skipped_dry_run_count"),
            ("Ignored Folder", "ignored_folder_count"),
            ("Ignored File", "ignored_file_count"),
        ]
        for label, key in optional_fields:
            if key in run:
                optional_rows.append((label, _escape_html(str(run.get(key) or 0))))

        return "".join(
            [
                '<h2 style="margin-top: 1rem;">Run Summary</h2>%s' % self._key_value_table_html(summary_rows),
                '<h2 style="margin-top: 1rem;">Outcome</h2>%s' % self._key_value_table_html(outcome_rows),
                '<h2 style="margin-top: 1rem;">Counts</h2>%s' % self._key_value_table_html(count_rows),
                '<h2 style="margin-top: 1rem;">Savings</h2>%s' % self._key_value_table_html(savings_rows),
                '<h2 style="margin-top: 1rem;">Related Information</h2>%s'
                % self._key_value_table_html(optional_rows or [("Details", "No additional summary counters recorded.")]),
            ]
        )

    def _key_value_table_html(self, rows: List[tuple]) -> str:
        row_html = []
        for label, value in rows:
            row_html.append(
                '<tr><th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem; width: 250px;">%s</th><td style="border-bottom: 1px solid #ddd; padding: 0.35rem;">%s</td></tr>'
                % (label, value)
            )
        return '<table style="border-collapse: collapse; width: 100%%; border: 1px solid #ddd;"><tbody>%s</tbody></table>' % "".join(row_html)

    def _related_run_info_html(self, run: Dict[str, str]) -> str:
        raw_log_path = str(run.get("raw_log_path") or "").strip()
        run_log_value = _escape_html(raw_log_path) if raw_log_path else "No raw log path recorded for this run."
        mode_value = _escape_html(_display_run_mode(str(run.get("mode") or "")))
        result_value = _escape_html(str(run.get("result") or "completed"))
        if str(run.get("mode") or "").strip().lower() in ("preview", "dry_run", "dry-run"):
            result_value = "Preview-only (no files encoded)"
        rows = [
            ("Run Log Path", run_log_value),
            ("Preview vs Live", mode_value),
            ("Preview vs Encode Result", result_value),
        ]
        return '<h2 style="margin-top: 1rem;">Run Logs and Distinctions</h2>%s' % self._key_value_table_html(rows)

    def _raw_log_path_html(self, run: Dict[str, str]) -> str:
        return self._related_run_info_html(run)

    def _run_file_summary_html(self, rows: List[Dict[str, str]]) -> str:
        total = len(rows)
        if total == 0:
            return '<div style="padding: 0.5rem; border: 1px solid #ddd;">No file-level entries recorded for this run.</div>'
        skipped_reasons = sorted(
            {
                str(row.get("reason") or "-")
                for row in rows
                if str(row.get("status") or "").lower() == "skipped" and str(row.get("reason") or "-") != "-"
            }
        )
        failure_reasons = sorted(
            {
                str(row.get("reason") or "-")
                for row in rows
                if str(row.get("status") or "").lower() == "failed" and str(row.get("reason") or "-") != "-"
            }
        )
        rows_data = [
            ("Total File Entries", _escape_html(str(total))),
            ("Skip Reasons", _escape_html(", ".join(skipped_reasons) if skipped_reasons else "None recorded")),
            ("Failure Reasons", _escape_html(", ".join(failure_reasons) if failure_reasons else "None recorded")),
        ]
        return '<h2 style="margin-top: 1rem;">File List Summary</h2>%s' % self._key_value_table_html(rows_data)

    def _run_encodes_html(self, rows: List[Dict[str, str]]) -> str:
        if not rows:
            return self._run_file_summary_html(rows)

        body_rows = []
        for row in rows:
            body_rows.append(
                "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
                % (
                    _escape_html(row["path"]),
                    _escape_html(row["status"]),
                    _escape_html(row["codec_info"]),
                    _escape_html(row["before"]),
                    _escape_html(row["after"]),
                    _escape_html(row["saved"]),
                    _escape_html(row["encode_duration"]),
                    _escape_html(row["reason"]),
                )
            )

        return self._run_file_summary_html(rows) + """<table style="border-collapse: collapse; width: 100%%; border: 1px solid #ddd;">
  <thead>
    <tr>
      <th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;">Path</th>
      <th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;">Status</th>
      <th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;">Codec</th>
      <th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;">Before</th>
      <th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;">After</th>
      <th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;">Saved</th>
      <th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;">Encode Time</th>
      <th style="text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;">Reason / Detail</th>
    </tr>
  </thead>
  <tbody>
    %s
  </tbody>
</table>""" % "".join(body_rows)

    def _recent_encode_history(self, limit: int = 200) -> List[Dict[str, str]]:
        db_path = Path(_env("STATS_PATH", "/config/chonk.db"))
        if not db_path.exists():
            return []

        try:
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            encode_columns = {str(row["name"]) for row in conn.execute("PRAGMA table_info(encodes)").fetchall()}
            if not encode_columns:
                conn.close()
                return []

            library_expr = "NULL"
            if "library" in encode_columns:
                library_expr = "e.library"

            filename_expr = "NULL"
            if "filename" in encode_columns:
                filename_expr = "e.filename"

            path_expr = "NULL"
            if "path" in encode_columns:
                path_expr = "e.path"

            size_before_expr = "NULL"
            if "size_before_bytes" in encode_columns:
                size_before_expr = "e.size_before_bytes"

            size_after_expr = "NULL"
            if "size_after_bytes" in encode_columns:
                size_after_expr = "e.size_after_bytes"

            saved_expr = "NULL"
            if "saved_bytes" in encode_columns:
                saved_expr = "e.saved_bytes"

            row_id_expr = "0"
            if "id" in encode_columns:
                row_id_expr = "e.id"

            rows = conn.execute(
                """
                SELECT
                    e.ts,
                    COALESCE(NULLIF(%s, ''), NULLIF(r.library, ''), '-') AS library,
                    COALESCE(NULLIF(%s, ''), NULLIF(%s, ''), '-') AS file_name,
                    %s AS size_before_bytes,
                    %s AS size_after_bytes,
                    %s AS saved_bytes,
                    %s AS row_id
                FROM encodes e
                LEFT JOIN runs r ON r.run_id = e.run_id
                WHERE e.status = 'success'
                ORDER BY e.ts DESC, row_id DESC
                LIMIT ?
                """
                % (
                    library_expr,
                    filename_expr,
                    path_expr,
                    size_before_expr,
                    size_after_expr,
                    saved_expr,
                    row_id_expr,
                ),
                (int(limit),),
            ).fetchall()
            conn.close()
        except Exception:
            return []

        result: List[Dict[str, str]] = []
        for row in rows:
            result.append(
                {
                    "ts": str(row["ts"] or ""),
                    "library": str(row["library"] or "-"),
                    "file_name": str(row["file_name"] or "-"),
                    "original_size": _format_saved_bytes(row["size_before_bytes"]),
                    "new_size": _format_saved_bytes(row["size_after_bytes"]),
                    "savings_pct": _format_savings_pct(row["size_before_bytes"], row["size_after_bytes"]),
                    "savings_amount": _format_saved_bytes(row["saved_bytes"]),
                }
            )
        return result

    def _history_table_html(self, rows: List[Dict[str, str]]) -> str:
        if not rows:
            return '<div style="padding: 0.5rem; border: 1px solid #ddd;">No completed encode history recorded yet.</div>'

        row_html = []
        for row in rows:
            row_html.append(
                "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
                % (
                    _escape_html(row["library"]),
                    _escape_html(row["file_name"]),
                    _escape_html(row["original_size"]),
                    _escape_html(row["new_size"]),
                    _escape_html(row["savings_pct"]),
                    _escape_html(row["savings_amount"]),
                    _escape_html(row["ts"]),
                )
            )

        return """<table style=\"border-collapse: collapse; width: 100%%; border: 1px solid #ddd;\">
  <thead>
    <tr>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Library</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">File Name</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Original Size</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">New Size</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Savings %%</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Savings Amount</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Date / Time</th>
    </tr>
  </thead>
  <tbody>
    %s
  </tbody>
</table>""" % "".join(row_html)

    def _lifetime_savings(self) -> Optional[Dict[str, int]]:
        db_path = Path(_env("STATS_PATH", "/config/chonk.db"))
        if not db_path.exists():
            return None

        try:
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN library = 'movies' THEN COALESCE(saved_bytes, 0) ELSE 0 END), 0) AS movies_saved,
                    COALESCE(SUM(CASE WHEN library = 'tv' THEN COALESCE(saved_bytes, 0) ELSE 0 END), 0) AS tv_saved,
                    COALESCE(SUM(COALESCE(saved_bytes, 0)), 0) AS total_saved,
                    COALESCE(SUM(CASE WHEN COALESCE(success_count, 0) > 0 THEN COALESCE(success_count, 0) ELSE 0 END), 0) AS files_optimized
                FROM runs
                WHERE COALESCE(success_count, 0) > 0
                """
            ).fetchone()
        except Exception:
            conn.close()
            return None

        if row is None:
            return None

        return {
            "movies_saved": int(row["movies_saved"] or 0),
            "tv_saved": int(row["tv_saved"] or 0),
            "total_saved": int(row["total_saved"] or 0),
            "files_optimized": int(row["files_optimized"] or 0),
        }

    def _library_lifetime_totals(self) -> Dict[str, Dict[str, int]]:
        conn = _connect_settings_db(self._settings_db_path)
        try:
            rows = conn.execute(
                """
                SELECT
                    lower(trim(COALESCE(library, ''))) AS library_key,
                    COUNT(*) AS files_optimized,
                    COALESCE(SUM(COALESCE(saved_bytes, 0)), 0) AS total_saved
                FROM encodes
                WHERE lower(COALESCE(status, '')) = 'success'
                GROUP BY lower(trim(COALESCE(library, '')))
                """
            ).fetchall()
        except sqlite3.Error:
            conn.close()
            return {}
        conn.close()

        totals: Dict[str, Dict[str, int]] = {}
        for row in rows:
            key = str(row["library_key"] or "").strip().lower()
            if not key:
                continue
            totals[key] = {
                "files_optimized": int(row["files_optimized"] or 0),
                "total_saved": int(row["total_saved"] or 0),
            }
        return totals

    def _lifetime_savings_html(self, savings: Optional[Dict[str, int]]) -> str:
        if savings is None or savings["files_optimized"] <= 0:
            return '<div style="padding: 0.5rem; border: 1px solid #ddd;">No reclaimed storage recorded yet.</div>'

        return """<div style=\"padding: 0.5rem; border: 1px solid #ddd;\">
  <div><strong>Movies reclaimed:</strong> %s</div>
  <div><strong>TV reclaimed:</strong> %s</div>
  <div><strong>Total reclaimed:</strong> %s</div>
  <div><strong>Files optimized:</strong> %d</div>
</div>""" % (
            _format_saved_bytes(savings["movies_saved"]),
            _format_saved_bytes(savings["tv_saved"]),
            _format_saved_bytes(savings["total_saved"]),
            savings["files_optimized"],
        )

    def _recent_activity(self, limit: int = 25) -> List[Dict[str, str]]:
        conn = _connect_settings_db(self._settings_db_path)
        runs_table_exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='runs'"
        ).fetchone()
        if runs_table_exists:
            rows = conn.execute(
                """
                SELECT
                    a.ts,
                    a.library,
                    a.run_id,
                    a.event_type,
                    a.message,
                    CASE WHEN r.run_id IS NULL THEN 0 ELSE 1 END AS run_exists
                FROM activity_events a
                LEFT JOIN runs r ON r.run_id = a.run_id
                ORDER BY a.id DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT
                    ts,
                    library,
                    run_id,
                    event_type,
                    message,
                    0 AS run_exists
                FROM activity_events
                ORDER BY id DESC
                LIMIT ?
                """,
                (int(limit),),
            ).fetchall()
        conn.close()

        return [
            {
                "ts": str(row["ts"]),
                "library": str(row["library"] or "-"),
                "run_id": str(row["run_id"] or ""),
                "event_type": str(row["event_type"]),
                "message": str(row["message"]),
                "run_exists": str(row["run_exists"] or 0),
            }
            for row in rows
        ]

    def _recent_activity_html(self, rows: List[Dict[str, str]]) -> str:
        if not rows:
            return '<div style="padding: 0.5rem; border: 1px solid #ddd;">No recent activity recorded yet.</div>'

        row_html = []
        for row in rows:
            run_id = row["run_id"]
            run_id_html = "-"
            if run_id:
                escaped_run_id = _escape_html(run_id)
                if str(row.get("run_exists") or "0") == "1":
                    run_id_html = '<a href="/runs/%s">%s</a>' % (escaped_run_id, escaped_run_id)
                else:
                    run_id_html = "%s <span style=\"color:#666;\">(run unavailable)</span>" % escaped_run_id
            row_html.append(
                "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
                % (
                    _escape_html(row["ts"]),
                    _escape_html(row["library"]),
                    _escape_html(row["event_type"]),
                    _escape_html(row["message"]),
                    run_id_html,
                )
            )

        return """<table style=\"border-collapse: collapse; width: 100%%; border: 1px solid #ddd;\">
  <thead>
    <tr>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Timestamp</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Library</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Event Type</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Message</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Run ID</th>
    </tr>
  </thead>
  <tbody>
    %s
  </tbody>
</table>""" % "".join(row_html)

    def _record_activity(
        self,
        event_type: str,
        message: str,
        library: Optional[str] = None,
        run_id: Optional[str] = None,
        level: str = "info",
    ) -> None:
        conn = _connect_settings_db(self._settings_db_path)
        with conn:
            conn.execute(
                """
                INSERT INTO activity_events(ts, level, library, run_id, event_type, message)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    _utc_timestamp(),
                    str(level),
                    str(library) if library is not None else None,
                    str(run_id) if run_id is not None else None,
                    str(event_type),
                    str(message),
                ),
            )
        conn.close()

    def _run_library_once(self, library: str, trigger: str, run_id: Optional[str] = None) -> None:
        library_record = self._library_by_key(library)
        if library_record is None:
            return
        run_id = str(uuid.uuid4())
        with self._job_condition:
            self._current_job_run_id = run_id
            self._current_run_snapshot = {
                "current_library": library_record.name,
                "mode": "Preview" if str(trigger).strip().lower() == "preview" else "Live",
            }
            if str(trigger).strip().lower() == "preview":
                self._last_preview_results = []
        self._record_activity(
            event_type="run_started",
            message="%s run started" % library_record.name,
            library=library_record.name,
            run_id=run_id,
        )

        with editable_settings_environment(self._editable_settings):
            with library_runtime_environment(library_record):
                original_preview = os.environ.get("PREVIEW")
                is_preview = str(trigger).strip().lower() == "preview"
                os.environ["PREVIEW"] = "true" if is_preview else "false"
                original_stats_path = os.environ.get("STATS_PATH")
                run_stats_path = str(self._settings_db_path)
                os.environ["STATS_PATH"] = run_stats_path
                LOGGER.info("Starting %s %s run", trigger, library_record.name)
                try:
                    try:
                        rc = run(
                            progress_callback=self._update_runtime_progress,
                            cancel_requested=self._is_cancel_requested,
                            on_cancelled=self._on_run_cancelled,
                        )
                    except TypeError as exc:
                        msg = str(exc)
                        if "progress_callback" not in msg and "cancel_requested" not in msg and "on_cancelled" not in msg:
                            raise
                        try:
                            rc = run(progress_callback=self._update_runtime_progress)
                        except TypeError:
                            rc = run()
                except Exception as exc:
                    self._notify_run_failure(library_record.name, run_id, str(exc))
                    raise
                finally:
                    if original_preview is None:
                        os.environ.pop("PREVIEW", None)
                    else:
                        os.environ["PREVIEW"] = original_preview
                    if os.environ.get("STATS_PATH") == run_stats_path:
                        if original_stats_path is None:
                            os.environ.pop("STATS_PATH", None)
                        else:
                            os.environ["STATS_PATH"] = original_stats_path
                LOGGER.info("Finished %s %s run with exit code %s", trigger, library_record.name, rc)

        was_cancelled = self._is_cancel_requested()
        if was_cancelled:
            with self._job_condition:
                self._last_run_was_cancelled = True
            self._record_activity(
                event_type="run_cancelled",
                message="%s run cancelled" % library_record.name,
                library=library_record.name,
                run_id=run_id,
                level="warning",
            )
        elif rc == 0:
            self._notify_run_complete(library_record.name, run_id)
        else:
            self._notify_run_failure(library_record.name, run_id, "Run exited with code %s" % rc)

        self._record_activity(
            event_type="run_completed",
            message="%s run %s" % (library_record.name, "cancelled" if was_cancelled else "completed"),
            library=library_record.name,
            run_id=run_id,
        )

    def _latest_run_summary_row(self, library: str) -> Optional[sqlite3.Row]:
        conn = _connect_settings_db(self._settings_db_path)
        try:
            row = conn.execute(
                """
                SELECT candidates_found, success_count, saved_bytes, duration_seconds
                FROM runs
                WHERE lower(COALESCE(library, '')) = ?
                ORDER BY ts_end DESC
                LIMIT 1
                """,
                (str(library).strip().lower(),),
            ).fetchone()
        except sqlite3.OperationalError:
            row = None
        conn.close()
        return row

    def _notify_run_complete(self, library: str, run_id: str) -> None:
        row = self._latest_run_summary_row(library)
        run_summary = notifications.build_run_complete_summary(library=library, run_id=run_id, row=row)
        run_summary["host"] = socket.gethostname()
        try:
            notifications.send_run_complete(run_summary, settings_db_path=str(self._settings_db_path))
        except Exception:
            LOGGER.warning("Run complete notification raised unexpectedly", exc_info=True)

    def _notify_run_failure(self, library: str, run_id: str, error_message: str) -> None:
        run_summary = {
            "library": library,
            "run_id": run_id,
            "error_message": str(error_message),
            "host": socket.gethostname(),
        }
        try:
            notifications.send_run_failure(run_summary, settings_db_path=str(self._settings_db_path))
        except Exception:
            LOGGER.warning("Run failure notification raised unexpectedly", exc_info=True)

    def stop_background_worker(self) -> None:
        worker_thread = self._worker_thread
        if worker_thread.is_alive():
            worker_thread.join(timeout=2)

    def run_forever(self) -> int:
        self._record_activity(event_type="service_start", message="Service startup complete")
        self.register_jobs()
        self._scheduler_stopped = False
        self.scheduler.start()

        for job in self.scheduler.get_jobs():
            next_run_time = getattr(job, "next_run_time", None)
            LOGGER.info(
                "Scheduler job active: job_id=%s next_run=%s",
                job.id,
                next_run_time,
            )

        self._scheduler_started_at = _utc_timestamp()
        LOGGER.info("Service scheduler started")
        self._record_activity(event_type="scheduler_start", message="Scheduler started")

        try:
            if uvicorn is not None and FastAPI is not None and isinstance(self.app, FastAPI):
                uvicorn.run(self.app, host=self.settings.host, port=self.settings.port)
            else:
                _run_simple_http_server(
                    self.settings.host,
                    self.settings.port,
                    self.health_payload,
                    self.home_page_html,
                    self.runs_page_html,
                    self.history_page_html,
                    self.run_detail_page_html,
                    self.settings_page_html,
                    self.activity_page_html,
                    self.system_page_html,
                    self.current_job_status,
                    self.update_editable_settings,
                    self.settings_saved_message,
                    notifications.send_test_notification,
                    self.create_library,
                    self.update_library,
                    self.delete_library,
                    self.toggle_library,
                    self.manual_run_payload,
                    self.request_cancel_active_run,
                    self.clear_preview_results,
                )
        finally:
            with self._job_condition:
                self._worker_shutdown = True
                self._job_condition.notify_all()
            self._scheduler_stopped = True
            self.scheduler.shutdown(wait=False)
            self.stop_background_worker()

        return 0


@contextmanager
def library_runtime_environment(library: RuntimeLibrary) -> Iterator[None]:
    values = {
        "LIBRARY": library.name,
        "LOG_PREFIX": _slugify_library_name(library.name),
        "MEDIA_ROOT": library.path,
        "MIN_SIZE_GB": str(library.min_size_gb),
        "MAX_FILES": str(library.max_files),
        "QSV_QUALITY": str(library.qsv_quality if library.qsv_quality is not None else _env_bootstrap("QSV_QUALITY", "21")),
        "QSV_PRESET": str(library.qsv_preset if library.qsv_preset is not None else _env_bootstrap("QSV_PRESET", "7")),
        "MIN_SAVINGS_PERCENT": str(
            library.min_savings_percent
            if library.min_savings_percent is not None
            else _env_bootstrap("MIN_SAVINGS_PERCENT", "15")
        ),
        "SKIP_CODECS": _normalize_csv_text(library.skip_codecs),
        "SKIP_MIN_HEIGHT": str(max(0, int(library.skip_min_height))),
        "SKIP_RESOLUTION_TAGS": _normalize_csv_text(library.skip_resolution_tags),
    }
    original: Dict[str, Optional[str]] = {key: os.environ.get(key) for key in values}

    with _ENV_MUTATION_LOCK:
        for key, value in values.items():
            os.environ[key] = value

    try:
        yield
    finally:
        with _ENV_MUTATION_LOCK:
            for key, value in original.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


@contextmanager
def library_environment(library: str) -> Iterator[None]:
    values = _library_values(library)
    original: Dict[str, Optional[str]] = {key: os.environ.get(key) for key in values}

    with _ENV_MUTATION_LOCK:
        for key, value in values.items():
            os.environ[key] = value

    try:
        yield
    finally:
        with _ENV_MUTATION_LOCK:
            for key, value in original.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value


@contextmanager
def editable_settings_environment(values: Dict[str, str]) -> Iterator[None]:
    env_map = {
        "min_file_age_minutes": "MIN_FILE_AGE_MINUTES",
        "min_savings_percent": "MIN_SAVINGS_PERCENT",
        "max_savings_percent": "MAX_SAVINGS_PERCENT",
        "min_media_free_gb": "MIN_MEDIA_FREE_GB",
        "max_gb_per_run": "MAX_GB_PER_RUN",
        "fail_fast": "FAIL_FAST",
        "log_skips": "LOG_SKIPS",
        "top_candidates": "TOP_CANDIDATES",
        "retry_count": "RETRY_COUNT",
        "retry_backoff_seconds": "RETRY_BACKOFF_SECONDS",
        "validate_seconds": "VALIDATE_SECONDS",
        "log_retention_days": "LOG_RETENTION_DAYS",
        "bak_retention_days": "BAK_RETENTION_DAYS",
    }
    original: Dict[str, Optional[str]] = {name: os.environ.get(name) for name in env_map.values()}

    with _ENV_MUTATION_LOCK:
        global _ENV_RUNTIME_DEPTH
        _ENV_RUNTIME_DEPTH += 1
        for env_name, baseline in original.items():
            if env_name not in _ENV_RUNTIME_BASELINES:
                _ENV_RUNTIME_BASELINES[env_name] = baseline
        for key, env_name in env_map.items():
            if key in values:
                os.environ[env_name] = str(values[key])

    try:
        yield
    finally:
        with _ENV_MUTATION_LOCK:
            for env_name, value in original.items():
                if value is None:
                    os.environ.pop(env_name, None)
                else:
                    os.environ[env_name] = value
            _ENV_RUNTIME_DEPTH = max(0, _ENV_RUNTIME_DEPTH - 1)
            if _ENV_RUNTIME_DEPTH == 0:
                _ENV_RUNTIME_BASELINES.clear()


def _library_values(library: str) -> Dict[str, str]:
    prefix = library.upper()
    defaults = {
        "movies": {
            "LIBRARY": "movies",
            "LOG_PREFIX": "movie",
            "MEDIA_ROOT": "/movies",
            "MIN_SIZE_GB": "17",
        },
        "tv": {
            "LIBRARY": "tv",
            "LOG_PREFIX": "tv",
            "MEDIA_ROOT": "/tv_shows",
            "MIN_SIZE_GB": "8",
        },
    }
    base = defaults[library]

    values = {
        "LIBRARY": _env("%s_LIBRARY" % prefix, base["LIBRARY"]),
        "LOG_PREFIX": _env("%s_LOG_PREFIX" % prefix, base["LOG_PREFIX"]),
        "MEDIA_ROOT": _env("%s_MEDIA_ROOT" % prefix, base["MEDIA_ROOT"]),
        "MIN_SIZE_GB": _env("%s_MIN_SIZE_GB" % prefix, base["MIN_SIZE_GB"]),
    }
    return values



def _slugify_library_name(name: str) -> str:
    value = "".join(ch.lower() if ch.isalnum() else "_" for ch in str(name))
    value = value.strip("_")
    return value or "library"


def _normalize_csv_text(value: str) -> str:
    parts: List[str] = []
    seen = set()
    for raw in str(value or "").split(","):
        token = raw.strip().lower()
        if not token or token in seen:
            continue
        parts.append(token)
        seen.add(token)
    return ",".join(parts)

def _connect_settings_db(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS libraries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            path TEXT NOT NULL UNIQUE,
            enabled INTEGER NOT NULL DEFAULT 1,
            schedule TEXT NOT NULL DEFAULT '',
            min_size_gb REAL NOT NULL DEFAULT 0.0,
            max_files INTEGER NOT NULL DEFAULT 1,
            priority INTEGER NOT NULL DEFAULT 100,
            qsv_quality INTEGER,
            qsv_preset INTEGER,
            min_savings_percent REAL,
            skip_codecs TEXT NOT NULL DEFAULT '',
            skip_min_height INTEGER NOT NULL DEFAULT 0,
            skip_resolution_tags TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    library_columns = {str(row[1]).strip().lower() for row in conn.execute("PRAGMA table_info(libraries)").fetchall()}
    if "min_size_gb" not in library_columns:
        conn.execute("ALTER TABLE libraries ADD COLUMN min_size_gb REAL NOT NULL DEFAULT 0.0")
    if "max_files" not in library_columns:
        conn.execute("ALTER TABLE libraries ADD COLUMN max_files INTEGER NOT NULL DEFAULT 1")
    if "priority" not in library_columns:
        conn.execute("ALTER TABLE libraries ADD COLUMN priority INTEGER NOT NULL DEFAULT 100")
    if "qsv_quality" not in library_columns:
        conn.execute("ALTER TABLE libraries ADD COLUMN qsv_quality INTEGER")
    if "qsv_preset" not in library_columns:
        conn.execute("ALTER TABLE libraries ADD COLUMN qsv_preset INTEGER")
    if "min_savings_percent" not in library_columns:
        conn.execute("ALTER TABLE libraries ADD COLUMN min_savings_percent REAL")
    if "skip_codecs" not in library_columns:
        conn.execute("ALTER TABLE libraries ADD COLUMN skip_codecs TEXT NOT NULL DEFAULT ''")
    if "skip_min_height" not in library_columns:
        conn.execute("ALTER TABLE libraries ADD COLUMN skip_min_height INTEGER NOT NULL DEFAULT 0")
    if "skip_resolution_tags" not in library_columns:
        conn.execute("ALTER TABLE libraries ADD COLUMN skip_resolution_tags TEXT NOT NULL DEFAULT ''")
    conn.execute("UPDATE libraries SET min_size_gb = COALESCE(min_size_gb, 0.0)")
    conn.execute("UPDATE libraries SET max_files = CASE WHEN max_files IS NULL OR max_files < 1 THEN 1 ELSE max_files END")
    conn.execute("UPDATE libraries SET priority = COALESCE(priority, 100)")
    conn.execute(
        "UPDATE libraries SET qsv_quality = CASE WHEN qsv_quality IS NULL OR qsv_quality < 0 THEN ? ELSE qsv_quality END",
        (_env_int("QSV_QUALITY", 21),),
    )
    conn.execute(
        "UPDATE libraries SET qsv_preset = CASE WHEN qsv_preset IS NULL OR qsv_preset < 0 THEN ? ELSE qsv_preset END",
        (_env_int("QSV_PRESET", 7),),
    )
    conn.execute(
        "UPDATE libraries SET min_savings_percent = CASE WHEN min_savings_percent IS NULL OR min_savings_percent < 0 THEN ? ELSE min_savings_percent END",
        (_env_float("MIN_SAVINGS_PERCENT", 15.0),),
    )
    conn.execute(
        "UPDATE libraries SET skip_codecs = CASE WHEN skip_codecs IS NULL OR trim(skip_codecs) = '' THEN ? ELSE skip_codecs END",
        (_normalize_csv_text(_env_bootstrap("SKIP_CODECS", "")),),
    )
    conn.execute(
        "UPDATE libraries SET skip_resolution_tags = CASE WHEN skip_resolution_tags IS NULL OR trim(skip_resolution_tags) = '' THEN ? ELSE skip_resolution_tags END",
        (_normalize_csv_text(_env_bootstrap("SKIP_RESOLUTION_TAGS", "")),),
    )
    conn.execute(
        "UPDATE libraries SET skip_min_height = CASE WHEN skip_min_height IS NULL OR skip_min_height < 0 THEN ? ELSE skip_min_height END",
        (max(0, _env_int("SKIP_MIN_HEIGHT", 0)),),
    )
    rows = conn.execute("SELECT id, skip_codecs, skip_resolution_tags FROM libraries").fetchall()
    for row in rows:
        conn.execute(
            "UPDATE libraries SET skip_codecs = ?, skip_resolution_tags = ? WHERE id = ?",
            (
                _normalize_csv_text(str(row["skip_codecs"] or "")),
                _normalize_csv_text(str(row["skip_resolution_tags"] or "")),
                int(row["id"]),
            ),
        )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS activity_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            level TEXT NOT NULL,
            library TEXT,
            run_id TEXT,
            event_type TEXT NOT NULL,
            message TEXT NOT NULL
        )
        """
    )
    return conn


def _simple_schedule_time_options() -> List[str]:
    options: List[str] = []
    for hour in range(24):
        for minute in (0, 15, 30, 45):
            options.append("%02d:%02d" % (hour, minute))
    return options


def _build_simple_cron(time_value: str, day_values: List[str]) -> str:
    parts = str(time_value or "").strip().split(":", 1)
    if len(parts) != 2:
        return ""
    try:
        hour = int(parts[0])
        minute = int(parts[1])
    except ValueError:
        return ""
    if hour < 0 or hour > 23 or minute < 0 or minute > 59:
        return ""

    allowed_days = {day for _, day in WEEKDAY_CHOICES}
    normalized_days = [day for day in day_values if day in allowed_days]
    if not normalized_days:
        return ""
    ordered_days = [day for _, day in WEEKDAY_CHOICES if day in normalized_days]
    return "%d %d * * %s" % (minute, hour, ",".join(ordered_days))


def _parse_simple_cron(schedule: str) -> Optional[Dict[str, object]]:
    parts = str(schedule or "").strip().split()
    if len(parts) != 5:
        return None
    minute_text, hour_text, dom, month, dow = parts
    if dom != "*" or month != "*":
        return None
    if not minute_text.isdigit() or not hour_text.isdigit():
        return None

    minute = int(minute_text)
    hour = int(hour_text)
    if minute < 0 or minute > 59 or hour < 0 or hour > 23:
        return None
    if minute not in {0, 15, 30, 45}:
        return None

    raw_days = [item.strip() for item in dow.split(",") if item.strip()]
    if not raw_days:
        return None

    normalized_days = []
    for day in raw_days:
        normalized_day = _normalize_cron_day_of_week(day)
        if normalized_day is None:
            return None
        if normalized_day not in normalized_days:
            normalized_days.append(normalized_day)

    ordered_days = [day for _, day in WEEKDAY_CHOICES if day in normalized_days]
    if not ordered_days:
        return None

    return {
        "days": ordered_days,
        "time": "%02d:%02d" % (hour, minute),
    }


def _normalize_cron_day_of_week(day: str) -> Optional[str]:
    value = str(day or "").strip().lower()
    if value in LEGACY_CRON_WEEKDAY_MAP:
        return LEGACY_CRON_WEEKDAY_MAP[value]
    if value in {"sun", "mon", "tue", "wed", "thu", "fri", "sat"}:
        return value
    return None


def _normalize_schedule_for_scheduler(schedule: str) -> str:
    raw = str(schedule or "").strip()
    parsed = _parse_simple_cron(raw)
    if parsed is None:
        return raw
    return _build_simple_cron(str(parsed["time"]), list(parsed["days"]))


def _schedule_form_state(schedule: str) -> Dict[str, object]:
    raw = str(schedule or "").strip()
    parsed = _parse_simple_cron(raw)
    if parsed is None:
        return {
            "mode": "advanced" if raw else "simple",
            "days": [],
            "time": "00:00",
            "raw": raw,
            "preview": raw,
        }
    preview = _build_simple_cron(str(parsed["time"]), list(parsed["days"]))
    return {
        "mode": "simple",
        "days": list(parsed["days"]),
        "time": str(parsed["time"]),
        "raw": raw,
        "preview": preview,
    }


def _is_valid_crontab(expr: str) -> bool:
    parts = expr.split()
    if len(parts) != 5:
        return False
    return all(bool(part.strip()) for part in parts)


def _next_run_from_cron(schedule: str, now: Optional[datetime] = None) -> Optional[datetime]:
    cron_expr = str(schedule or "").strip()
    if not cron_expr:
        return None

    tzinfo = _resolved_timezone_info()

    reference = now
    if reference is None:
        if tzinfo is not None:
            reference = datetime.now(tzinfo)
        else:
            reference = datetime.utcnow()

    parsed_simple = _parse_simple_cron(cron_expr)
    if parsed_simple is not None:
        hour_text, minute_text = str(parsed_simple["time"]).split(":", 1)
        minute = int(minute_text)
        hour = int(hour_text)
        day_name_to_weekday = {
            "mon": 0,
            "tue": 1,
            "wed": 2,
            "thu": 3,
            "fri": 4,
            "sat": 5,
            "sun": 6,
        }
        days = [day_name_to_weekday[day] for day in list(parsed_simple["days"]) if day in day_name_to_weekday]

        day_offsets = [0, 1, 2, 3, 4, 5, 6, 7]
        reference_base = reference.replace(second=0, microsecond=0)
        for offset in day_offsets:
            candidate_day = reference_base.date().toordinal() + offset
            candidate_date = datetime.fromordinal(candidate_day)
            weekday_value = candidate_date.weekday()
            if weekday_value not in days:
                continue
            candidate = reference_base.replace(
                year=candidate_date.year,
                month=candidate_date.month,
                day=candidate_date.day,
                hour=hour,
                minute=minute,
            )
            if candidate > reference_base:
                return candidate

    if CronTrigger is None:
        return None

    try:
        trigger = _build_scheduler_cron_trigger(cron_expr, timezone=_cron_trigger_timezone())
        return trigger.get_next_fire_time(None, reference)
    except Exception:
        return None


def _parse_scheduler_cron_fields(schedule: str) -> Dict[str, str]:
    parts = str(schedule or "").strip().split()
    if len(parts) != 5:
        raise ValueError("Invalid cron schedule")
    minute, hour, day, month, day_of_week = parts
    return {
        "minute": minute,
        "hour": hour,
        "day": day,
        "month": month,
        "day_of_week": day_of_week,
    }


def _build_scheduler_cron_trigger(schedule: str, timezone, parsed_fields: Optional[Dict[str, str]] = None):
    if CronTrigger is None:
        raise ValueError("Cron scheduler not available")
    fields = parsed_fields or _parse_scheduler_cron_fields(schedule)
    return CronTrigger(
        minute=fields["minute"],
        hour=fields["hour"],
        day=fields["day"],
        month=fields["month"],
        day_of_week=fields["day_of_week"],
        timezone=timezone,
    )


def _configured_timezone_name() -> str:
    return (_env("TZ", "UTC") or "UTC").strip() or "UTC"


def _resolved_timezone_info():
    if ZoneInfo is None:
        return None
    try:
        return ZoneInfo(_configured_timezone_name())
    except Exception:
        return None


def _cron_trigger_timezone():
    tzinfo = _resolved_timezone_info()
    if tzinfo is not None:
        return tzinfo
    return _configured_timezone_name()



def _derive_run_status(success_count: int, failed_count: int, skipped_count: int, was_cancelled: bool = False) -> str:
    """Map run counters to a compact status label for the operator page."""
    if was_cancelled:
        return "cancelled"
    if failed_count > 0:
        return "failed"
    if success_count > 0:
        return "success"
    if skipped_count > 0:
        return "skipped"
    return "completed"


def _format_duration_seconds(value) -> str:
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


def _duration_seconds_from_run(ts_start, ts_end, fallback_seconds) -> Optional[float]:
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


def _run_saved_mb_gb_label(saved_bytes) -> str:
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


def _format_saved_bytes(value) -> str:
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


def _format_eta_seconds(value) -> str:
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




def _display_trigger(trigger: str) -> str:
    value = str(trigger or "").strip().lower()
    if value == "manual":
        return "Manual"
    if value in ("schedule", "scheduled"):
        return "Scheduled"
    if not value:
        return "-"
    return str(trigger)


def _display_run_mode(mode: str) -> str:
    value = str(mode or "").strip().lower()
    if value in ("preview", "dry_run", "dry-run"):
        return "Preview"
    if value in ("live", "normal", "encode"):
        return "Live"
    if not value:
        return "Live"
    return str(mode)


def _display_run_trigger(event_type: str) -> str:
    value = str(event_type or "").strip().lower()
    if value == "manual_preview_requested":
        return "Manual Preview"
    if value == "manual_run_requested":
        return "Manual"
    if value == "scheduled_run_requested":
        return "Scheduled"
    return "Unknown"


def _format_scheduler_datetime(value) -> str:
    if value is None:
        return "Unknown"
    if isinstance(value, datetime):
        tzinfo = None
        if ZoneInfo is not None:
            tz_name = (_env("TZ", "UTC") or "UTC").strip() or "UTC"
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


def _format_readable_timestamp(value: object) -> str:
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


def _coerce_scheduler_datetime(value) -> Optional[datetime]:
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


def _format_savings_pct(size_before, size_after) -> str:
    try:
        before = float(size_before)
        after = float(size_after)
    except Exception:
        return "Unknown"

    if before <= 0:
        return "Unknown"

    pct = ((before - after) / before) * 100.0
    return "%.1f%%" % pct


def _escape_html(value: str) -> str:
    escaped = str(value)
    escaped = escaped.replace("&", "&amp;")
    escaped = escaped.replace("<", "&lt;")
    escaped = escaped.replace(">", "&gt;")
    escaped = escaped.replace('"', "&quot;")
    return escaped


def _clean_secret_input(value: object) -> str:
    cleaned = str(value or "").replace("\r", "").replace("\n", "")
    return cleaned.strip()


def _utc_timestamp() -> str:
    return strftime("%Y-%m-%dT%H:%M:%SZ")


def _run_simple_http_server(
    host: str,
    port: int,
    health_fn: Callable[[], dict],
    home_html_fn: Callable[[], str],
    runs_html_fn: Callable[[], str],
    history_html_fn: Callable[[], str],
    run_detail_html_fn: Callable[[str], tuple],
    settings_html_fn: Callable[[str], str],
    activity_html_fn: Callable[[], str],
    system_html_fn: Callable[[], str],
    status_snapshot_fn: Callable[[], Dict[str, str]],
    update_settings_fn: Callable[[Dict[str, str]], None],
    settings_saved_message_fn: Callable[[Dict[str, str]], str],
    test_notification_fn: Callable[[Optional[str]], Dict[str, object]],
    create_library_fn: Callable[[Dict[str, str]], str],
    update_library_fn: Callable[[Dict[str, str]], str],
    delete_library_fn: Callable[[Dict[str, str]], str],
    toggle_library_fn: Callable[[Dict[str, str]], str],
    manual_run_fn: Callable[[str, bool], tuple],
    cancel_run_fn: Optional[Callable[[], Dict[str, str]]] = None,
    clear_preview_fn: Optional[Callable[[], Dict[str, str]]] = None,
) -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            request_path = urlsplit(self.path).path

            if request_path == "/favicon.ico":
                self.send_response(204)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return

            if request_path in ("/", "/dashboard"):
                payload = home_html_fn().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            if request_path == "/settings":
                payload = settings_html_fn("").encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            if request_path == "/runs":
                payload = runs_html_fn().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            if request_path == "/history":
                payload = history_html_fn().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            if request_path.startswith("/runs/"):
                run_id = unquote(request_path[len("/runs/") :])
                html, status_code = run_detail_html_fn(run_id)
                payload = html.encode("utf-8")
                self.send_response(status_code)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            if request_path == "/activity":
                payload = activity_html_fn().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            if request_path == "/system":
                payload = system_html_fn().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            if request_path == "/api/status":
                payload = json.dumps(status_snapshot_fn()).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            if request_path != "/health":
                self.send_response(404)
                self.end_headers()
                return

            payload = json.dumps(health_fn()).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_POST(self):  # noqa: N802
            if self.path.startswith("/dashboard/libraries/") and (self.path.endswith("/run") or self.path.endswith("/preview")):
                parts = [part for part in self.path.split("/") if part]
                if len(parts) == 4 and parts[0] == "dashboard" and parts[1] == "libraries" and parts[3] in ("run", "preview"):
                    try:
                        library_id = int(parts[2])
                    except ValueError:
                        self.send_response(400)
                        self.end_headers()
                        return
                    payload, _ = manual_run_fn(str(library_id), parts[3] == "preview")
                    location = "/dashboard"
                    if payload.get("status") in ("queued", "busy"):
                        location = "/dashboard?manual_run=%s&library_id=%s" % (
                            quote(str(payload.get("status", ""))),
                            quote(str(payload.get("library_id", ""))),
                        )
                    self.send_response(303)
                    self.send_header("Location", location)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                self.send_response(404)
                self.end_headers()
                return

            if self.path.startswith("/libraries/") and self.path.endswith("/run"):
                parts = [part for part in self.path.split("/") if part]
                if len(parts) == 3 and parts[0] == "libraries" and parts[2] == "run":
                    try:
                        library_id = int(parts[1])
                    except ValueError:
                        self.send_response(400)
                        self.end_headers()
                        return
                    payload, status_code = manual_run_fn(str(library_id), False)
                else:
                    self.send_response(404)
                    self.end_headers()
                    return
            elif self.path == "/run/movies":
                payload, status_code = manual_run_fn("movies", False)
            elif self.path == "/run/tv":
                payload, status_code = manual_run_fn("tv", False)
            elif self.path == "/api/run/cancel":
                payload = cancel_run_fn() if callable(cancel_run_fn) else {"status": "idle"}
                status_code = 200
            elif self.path == "/api/preview/clear":
                payload = clear_preview_fn() if callable(clear_preview_fn) else {"status": "cleared"}
                status_code = 200
            elif self.path == "/settings":
                content_length = int(self.headers.get("Content-Length", "0") or "0")
                body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else ""
                updates = {key: values[-1] for key, values in parse_qs(body, keep_blank_values=True).items() if values}
                for key in CHECKBOX_SETTINGS:
                    if key not in updates:
                        updates[key] = "0"
                update_settings_fn(updates)
                html = settings_html_fn(settings_saved_message_fn(updates))
                encoded = html.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)
                return
            elif self.path == "/settings/test-notification":
                result = test_notification_fn(None)
                html = settings_html_fn(str(result.get("message", "")))
                encoded = html.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)
                return
            elif self.path in (
                "/settings/libraries/create",
                "/settings/libraries/update",
                "/settings/libraries/delete",
                "/settings/libraries/toggle",
            ):
                content_length = int(self.headers.get("Content-Length", "0") or "0")
                body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else ""
                values = {key: items[-1] for key, items in parse_qs(body, keep_blank_values=True).items() if items}
                if self.path == "/settings/libraries/create":
                    message = create_library_fn(values)
                elif self.path == "/settings/libraries/update":
                    message = update_library_fn(values)
                elif self.path == "/settings/libraries/delete":
                    message = delete_library_fn(values)
                else:
                    message = toggle_library_fn(values)
                html = settings_html_fn(message)
                encoded = html.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)
                return
            else:
                self.send_response(404)
                self.end_headers()
                return

            encoded = json.dumps(payload).encode("utf-8")
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def log_message(self, format, *args):  # noqa: A003
            del format, args
            return

    server = ThreadingHTTPServer((host, port), Handler)
    server.serve_forever()


def run_service() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = ServiceSettings.from_env()
    service = ChonkService(settings)
    return service.run_forever()
