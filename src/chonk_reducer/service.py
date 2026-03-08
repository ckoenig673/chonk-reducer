from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import uuid
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from time import strftime
from typing import Callable, Deque, Dict, Iterator, List, Optional, Set
from urllib.parse import parse_qs, unquote

try:
    from zoneinfo import ZoneInfo  # type: ignore
except Exception:  # pragma: no cover - fallback for Python 3.8 runtime
    try:
        from backports.zoneinfo import ZoneInfo  # type: ignore
    except Exception:  # pragma: no cover - best-effort timezone display
        ZoneInfo = None

try:
    from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore
    from apscheduler.triggers.cron import CronTrigger  # type: ignore
except Exception:  # pragma: no cover - exercised by fallback tests
    BackgroundScheduler = None
    CronTrigger = None

try:
    from fastapi import FastAPI, Request  # type: ignore
    from fastapi.responses import HTMLResponse, JSONResponse  # type: ignore
except Exception:  # pragma: no cover - exercised by fallback tests
    FastAPI = None
    Request = None
    HTMLResponse = None
    JSONResponse = None

try:
    import uvicorn  # type: ignore
except Exception:  # pragma: no cover - exercised by fallback tests
    uvicorn = None

from .runner import run
from . import __version__


LOGGER = logging.getLogger("chonk_reducer.service")

WEEKDAY_CHOICES = [
    ("Su", "0"),
    ("M", "1"),
    ("T", "2"),
    ("W", "3"),
    ("Th", "4"),
    ("F", "5"),
    ("Sa", "6"),
]


EDITABLE_SETTINGS = {
    "min_file_age_minutes": {"env": "MIN_FILE_AGE_MINUTES", "default": "10"},
    "max_files": {"env": "MAX_FILES", "default": "1"},
    "min_savings_percent": {"env": "MIN_SAVINGS_PERCENT", "default": "15"},
    "max_savings_percent": {"env": "MAX_SAVINGS_PERCENT", "default": "0"},
    "retry_count": {"env": "RETRY_COUNT", "default": "1"},
    "retry_backoff_secs": {"env": "RETRY_BACKOFF_SECS", "default": "5"},
    "skip_codecs": {"env": "SKIP_CODECS", "default": ""},
    "skip_resolution_tags": {"env": "SKIP_RESOLUTION_TAGS", "default": ""},
    "skip_min_height": {"env": "SKIP_MIN_HEIGHT", "default": "0"},
    "validate_seconds": {"env": "VALIDATE_SECONDS", "default": "10"},
    "log_retention_days": {"env": "LOG_RETENTION_DAYS", "default": "30"},
    "bak_retention_days": {"env": "BAK_RETENTION_DAYS", "default": "60"},
}

RESTART_REQUIRED_SETTINGS = set()


@dataclass(frozen=True)
class LibraryRecord:
    id: int
    name: str
    path: str
    enabled: bool
    schedule: str


@dataclass(frozen=True)
class RuntimeLibrary:
    id: int
    name: str
    path: str
    schedule: str


@dataclass(frozen=True)
class RuntimeJob:
    library_id: int
    library_name: str
    trigger: str


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
    return (os.getenv(name) or default).strip()


def _env_int(name: str, default: int) -> int:
    value = _env(name, str(default))
    try:
        return int(value)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    value = _env(name, "1" if default else "0").lower()
    return value in ("1", "true", "yes", "y", "on")


class _ScheduledJob:
    def __init__(self, job_id: str):
        self.id = job_id


class _FallbackScheduler:
    def __init__(self):
        self._jobs: List[_ScheduledJob] = []

    def add_job(self, func, trigger=None, id=None, args=None, coalesce=True, max_instances=1, replace_existing=True):
        del func, trigger, args, coalesce, max_instances, replace_existing
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

    def _configure_routes(self) -> None:
        @self.app.get("/")
        def home():
            return self._html_response(self.home_page_html())

        @self.app.get("/dashboard")
        def dashboard():
            return self._html_response(self.home_page_html())

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
            self.update_editable_settings(values)
            return self._html_response(self.settings_page_html(self.settings_saved_message(values)))

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

        @self.app.post("/libraries/{library_id}/run")
        def run_library(library_id: int):
            payload, status_code = self.manual_run_payload_for_id(int(library_id))
            if JSONResponse is not None:
                return JSONResponse(content=payload, status_code=status_code)
            return payload

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

    async def _request_form_values(self, request: Request = None) -> Dict[str, str]:
        values: Dict[str, str] = {}
        if request is not None and hasattr(request, "form"):
            form = await request.form()
            values = {key: str(value) for key, value in form.items()}
        return values

    def home_page_html(self) -> str:
        libraries = self.enabled_runtime_libraries()
        recent_runs = self._recent_runs(limit=10)
        lifetime_savings = self._lifetime_savings()
        library_sections = []
        for library in libraries:
            status = self._latest_run_status(library.name)
            last_run_label = "Never"
            processed_label = "0"
            savings_label = "0 B"
            if status is not None:
                last_run_label = status.get("ts_end") or status.get("ts_start") or "Unknown"
                processed_label = str(status.get("processed_count") or 0)
                savings_label = _format_saved_bytes(status.get("saved_bytes"))
            library_sections.append(
                """
  <section style="border: 1px solid #ddd; padding: 0.75rem; margin-bottom: 0.75rem; background: #fff;">
    <h2 style="margin-top: 0; margin-bottom: 0.5rem;">%s</h2>
    <div><strong>Path:</strong> %s</div>
    <div><strong>Last Run:</strong> %s</div>
    <div><strong>Next Run:</strong> %s</div>
    <div><strong>Recent Savings:</strong> %s across %s files</div>
    <form method="post" action="/libraries/%d/run" style="margin-top: 0.75rem;">
      <button type="submit">Run Now</button>
    </form>
  </section>
"""
                % (
                    _escape_html(library.name),
                    _escape_html(library.path),
                    _escape_html(last_run_label),
                    _escape_html(self._next_run_label(library, manual_label="Manual Only")),
                    _escape_html(savings_label),
                    _escape_html(processed_label),
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
""" % (
            self._runtime_status_html(),
            "".join(library_sections),
            self._lifetime_savings_html(lifetime_savings),
            self._recent_runs_html(recent_runs),
        )
        return self._render_shell_html("Dashboard", content)

    def settings_page_html(self, message: str = "") -> str:
        libraries = self.list_libraries()
        rows = []
        for key in EDITABLE_SETTINGS:
            value = self._editable_settings.get(key, "")
            restart_badge = ""
            if key in RESTART_REQUIRED_SETTINGS:
                restart_badge = ' <span style="color: #8a4f00; font-size: 0.9rem;">(restart required)</span>'
            rows.append(
                """<label for=\"{key}\" style=\"display:block; margin-top: 0.75rem;\"><strong>{label}</strong>{restart_badge}</label>
  <input id=\"{key}\" name=\"{key}\" value=\"{value}\" style=\"width: 100%; max-width: 420px;\" />""".format(
                    key=key,
                    label=key.replace("_", " ").title(),
                    restart_badge=restart_badge,
                    value=_escape_html(value),
                )
            )

        content = "<h1>Settings</h1>"
        if message:
            content += '<div style="padding: 0.5rem; border: 1px solid #cfe9cf; background:#f4fff4;">%s</div>' % _escape_html(
                message
            )
        content += """
<section>
<h2>Global Settings</h2>
<p>Editable service defaults persisted in SQLite.</p>
<form method=\"post\" action=\"/settings\">%s
  <div style=\"margin-top: 1rem;\"><button type=\"submit\">Save</button></div>
</form>
<p style=\"margin-top: 1rem; color: #555;\">Settings are saved immediately to SQLite. Some service-level behaviors are applied on startup/restart only.</p>
</section>
<section style=\"margin-top: 2rem;\">
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
        content += self._raw_log_path_html(run)
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
        return {
            "status": snapshot["status"],
            "current_library": snapshot["current_library"],
            "trigger": snapshot["current_trigger"],
            "queue_depth": snapshot["queue_depth"],
            "run_id": snapshot["run_id"],
            "started_at": snapshot["started_at"],
        }

    def _runtime_job_status_html(self) -> str:
        status = self.current_job_status()
        return """<table style=\"border-collapse: collapse; width: 100%%; border: 1px solid #ddd;\">
  <tbody>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem; width: 250px;\">Status</th><td style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Current Library</th><td style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Trigger</th><td style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Queue Depth</th><td style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Current Run ID</th><td style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; padding: 0.35rem;\">Started At</th><td style=\"padding: 0.35rem;\">%s</td></tr>
  </tbody>
</table>""" % (
            _escape_html(status["status"]),
            _escape_html(status["current_library"] or "-"),
            _escape_html(status["trigger"] or "-"),
            _escape_html(status["queue_depth"]),
            _escape_html(status["run_id"] or "-"),
            _escape_html(status["started_at"] or "-"),
        )

    def _scheduler_running_label(self) -> str:
        running = getattr(self.scheduler, "running", None)
        if running is True:
            return "Running"
        if running is False:
            return "Stopped"
        return "Unknown"

    def _next_run_label(self, library: RuntimeLibrary, manual_label: str = "Not scheduled") -> str:
        schedule = library.schedule.strip()
        if not schedule:
            return manual_label

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
        if job is None:
            return "Unknown"

        next_run_time = getattr(job, "next_run_time", None)
        if next_run_time is None:
            return "Unknown"
        return str(next_run_time)

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
                    value = _env(meta["env"], meta["default"])
                    conn.execute(
                        "INSERT INTO settings(key, value, updated_at) VALUES (?, ?, ?)",
                        (key, value, _utc_timestamp()),
                    )
                else:
                    value = str(row["value"])
                values[key] = value
        conn.close()
        return values

    def update_editable_settings(self, updates: Dict[str, str]) -> None:
        if not updates:
            return
        conn = _connect_settings_db(self._settings_db_path)
        with conn:
            for key in EDITABLE_SETTINGS:
                if key not in updates:
                    continue
                value = str(updates[key]).strip()
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
                ),
                (
                    "TV",
                    _env("TV_MEDIA_ROOT", _library_values("tv").get("MEDIA_ROOT", "/tv_shows")),
                    1,
                    tv_schedule,
                ),
            ]
            for name, path, enabled, schedule in defaults:
                conn.execute(
                    """
                    INSERT INTO libraries(name, path, enabled, schedule, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (name, path, enabled, schedule, now, now),
                )
        conn.close()

    def _legacy_schedule_value(self, conn: sqlite3.Connection, key: str, env_name: str) -> str:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        if row is not None:
            return str(row["value"] or "").strip()
        return _env(env_name, "")

    def list_libraries(self) -> List[LibraryRecord]:
        conn = _connect_settings_db(self._settings_db_path)
        rows = conn.execute("SELECT id, name, path, enabled, schedule FROM libraries ORDER BY id ASC").fetchall()
        conn.close()
        return [
            LibraryRecord(
                id=int(row["id"]),
                name=str(row["name"]),
                path=str(row["path"]),
                enabled=bool(int(row["enabled"])),
                schedule=str(row["schedule"] or ""),
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
                    INSERT INTO libraries(name, path, enabled, schedule, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized["name"],
                        normalized["path"],
                        int(normalized["enabled"]),
                        normalized["schedule"],
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
                    SET name = ?, path = ?, enabled = ?, schedule = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        normalized["name"],
                        normalized["path"],
                        int(normalized["enabled"]),
                        normalized["schedule"],
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
  <td style=\"padding: 0.35rem; border-bottom: 1px solid #eee;\"><code>{schedule}</code></td>
  <td style=\"padding: 0.35rem; border-bottom: 1px solid #eee;\">{actions}</td>
</tr>
<tr>
  <td colspan=\"5\" style=\"padding: 0.35rem 0.35rem 0.75rem 0.35rem; border-bottom: 1px solid #ddd; background: #fafcff;\">
    <details>
      <summary>Edit {name}</summary>
      <form method=\"post\" action=\"/settings/libraries/update\" style=\"margin-top: 0.5rem;\">
        <input type=\"hidden\" name=\"library_id\" value=\"{library_id}\" />
        <label><strong>Name</strong></label><br />
        <input name=\"name\" value=\"{name}\" style=\"width: 100%; max-width: 420px;\" /><br />
        <label><strong>Path</strong></label><br />
        <input name=\"path\" value=\"{path}\" style=\"width: 100%; max-width: 420px;\" /><br />
        {schedule_fields}
        <label><strong>Enabled</strong></label>
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
                    schedule_fields=self._schedule_fields_html(schedule_state, "edit-%d" % library.id),
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
  <label><strong>Name</strong></label><br />
  <input name="name" style="width: 100%; max-width: 420px;" /><br />
  <label><strong>Path</strong></label><br />
  <input name="path" style="width: 100%; max-width: 420px;" /><br />
  {schedule_fields}
  <label><strong>Enabled</strong></label>
  <select name="enabled"><option value="1" selected>Yes</option><option value="0">No</option></select>
  <div style="margin-top: 0.5rem;"><button type="submit">Create Library</button></div>
</form>
""".format(schedule_fields=schedule_fields)

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
    <legend><strong>Schedule</strong></legend>
    <label style=\"margin-right: 1rem;\"><input id=\"%s\" type=\"radio\" name=\"schedule_mode\" value=\"simple\" %s onchange=\"toggleScheduleMode_%s()\" /> Simple</label>
    <label><input id=\"%s\" type=\"radio\" name=\"schedule_mode\" value=\"advanced\" %s onchange=\"toggleScheduleMode_%s()\" /> Advanced cron</label>

    <div id=\"simple-schedule-%s\" style=\"display:%s; margin-top: 0.5rem;\">
      <label><strong>Days</strong></label><br />
      %s
      <br />
      <label><strong>Time</strong></label><br />
      <select name=\"schedule_time\" style=\"width: 100%%; max-width: 180px;\">%s</select>
      <div style=\"margin-top: 0.35rem; color:#555;\">Generated cron: <code>%s</code></div>
    </div>

    <div id=\"advanced-schedule-%s\" style=\"display:%s; margin-top: 0.5rem;\">
      <label><strong>Raw cron expression</strong></label><br />
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
            simple_radio_id,
            simple_checked,
            form_token,
            advanced_radio_id,
            advanced_checked,
            form_token,
            form_token,
            simple_display,
            "".join(weekday_options),
            "".join(time_options),
            preview,
            form_token,
            advanced_display,
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

    def _register_library_job(self, library: RuntimeLibrary) -> None:
        schedule = (library.schedule or "").strip()
        if not schedule:
            LOGGER.info("No schedule configured for %s; job disabled", library.name)
            return

        if CronTrigger is not None:
            try:
                trigger = CronTrigger.from_crontab(schedule)
            except ValueError:
                LOGGER.error("Invalid cron schedule for %s: %r", library.name, schedule)
                return
        else:
            if not _is_valid_crontab(schedule):
                LOGGER.error("Invalid cron schedule for %s: %r", library.name, schedule)
                return
            trigger = schedule

        self.scheduler.add_job(
            self.trigger_library_by_id,
            trigger=trigger,
            id=self._schedule_job_id(library.id),
            args=[library.id],
            coalesce=True,
            max_instances=1,
            replace_existing=True,
        )
        LOGGER.info("Registered %s schedule: %s", library.name, schedule)
        self._record_activity(
            event_type="schedule_registered",
            message="Scheduler registered %s schedule" % library.name,
            library=library.name,
        )

    def _schedule_job_id(self, library_id: int) -> str:
        return "library-%d-schedule" % int(library_id)

    def enabled_runtime_libraries(self) -> List[RuntimeLibrary]:
        return [
            RuntimeLibrary(id=library.id, name=library.name, path=library.path, schedule=library.schedule)
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

    def manual_run_payload(self, library: str):
        library_record = self._library_by_key(library)
        if library_record is None:
            return {"status": "not_found", "library": library}, 404
        library_name = library_record.name
        LOGGER.info("Manual %s run request received", library_name)
        self._record_activity(
            event_type="manual_run_requested",
            message="Manual run requested for %s" % library_name,
            library=library_name,
        )
        queued = self._enqueue_library_job(library_record, trigger="manual")
        payload = {
            "status": "queued" if queued else "busy",
            "library": library,
            "library_id": library_record.id,
        }
        if queued:
            LOGGER.info("Manual %s run accepted and queued", library_name)
            return payload, 202

        LOGGER.info("Manual %s run rejected; run already queued or in progress", library_name)
        self._record_activity(
            event_type="run_rejected_busy",
            message="%s run skipped because library is already queued or running" % library_name,
            library=library_name,
            level="warning",
        )
        return payload, 409

    def _enqueue_library_job(self, library: RuntimeLibrary, trigger: str) -> bool:
        job = RuntimeJob(library_id=library.id, library_name=library.name, trigger=trigger)
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
                job = self._job_queue.popleft()
                self._current_job = job
                self._current_job_started_at = _utc_timestamp()
                self._current_job_run_id = ""

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
                    self._current_job = None
                    self._current_job_started_at = ""
                    self._current_job_run_id = ""
                self._record_activity(
                    event_type="job_completed",
                    message="%s queued job completed" % job.library_name,
                    library=job.library_name,
                )

    def _runtime_status_snapshot(self) -> Dict[str, str]:
        with self._job_condition:
            queue_depth = len(self._job_queue)
            current_job = self._current_job
            started_at = self._current_job_started_at
            run_id = self._current_job_run_id
        if current_job is not None:
            status = "Running"
        elif queue_depth > 0:
            status = "Queued"
        else:
            status = "Idle"
        return {
            "status": status,
            "current_library": current_job.library_name if current_job is not None else "",
            "current_trigger": current_job.trigger if current_job is not None else "",
            "queue_depth": str(queue_depth),
            "run_id": run_id,
            "started_at": started_at,
        }

    def _runtime_status_html(self) -> str:
        snapshot = self._runtime_status_snapshot()
        current_library = snapshot["current_library"] or "N/A"
        current_trigger = snapshot["current_trigger"] or "N/A"
        run_id = snapshot["run_id"] or "N/A"
        started_at = snapshot["started_at"] or "N/A"
        return """<table style=\"border-collapse: collapse; width: 100%%; border: 1px solid #ddd; background: #fff;\">
  <tbody>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem; width: 250px;\">Status</th><td style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Current Library</th><td style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Trigger</th><td style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Queue Depth</th><td style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.35rem;\">Current Run ID</th><td style=\"border-bottom: 1px solid #ddd; padding: 0.35rem;\">%s</td></tr>
    <tr><th style=\"text-align: left; padding: 0.35rem;\">Started At</th><td style=\"padding: 0.35rem;\">%s</td></tr>
  </tbody>
</table>""" % (
            _escape_html(snapshot["status"]),
            _escape_html(current_library),
            _escape_html(current_trigger),
            _escape_html(snapshot["queue_depth"]),
            _escape_html(run_id),
            _escape_html(started_at),
        )

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
                       , processed_count, saved_bytes
                FROM runs
                WHERE lower(COALESCE(library, '')) = lower(?)
                ORDER BY ts_end DESC
                LIMIT 1
                """,
                (library,),
            ).fetchone()
            conn.close()
        except Exception:
            return None

        if row is None:
            return None

        status = _derive_run_status(
            success_count=int(row["success_count"] or 0),
            failed_count=int(row["failed_count"] or 0),
            skipped_count=int(row["skipped_count"] or 0),
        )
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
                SELECT run_id, ts_end, ts_start, library, success_count, failed_count, skipped_count,
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
            result.append(
                {
                    "time": str(row["ts_end"] or row["ts_start"] or "Unknown"),
                    "library": str(row["library"] or "Unknown"),
                    "result": _derive_run_status(
                        success_count=success_count,
                        failed_count=failed_count,
                        skipped_count=skipped_count,
                    ),
                    "duration": _format_duration_seconds(row["duration_seconds"]),
                    "processed": str(success_count + failed_count + skipped_count),
                    "success": str(success_count),
                    "skipped": str(skipped_count),
                    "failed": str(failed_count),
                    "saved": _format_saved_bytes(row["saved_bytes"]),
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
            conn.close()
        except Exception:
            return None

        if row is None:
            return None

        result: Dict[str, str] = {
            "result": _derive_run_status(
                success_count=int(row["success_count"] or 0),
                failed_count=int(row["failed_count"] or 0),
                skipped_count=int(row["skipped_count"] or 0),
            )
        }
        for key in requested_columns:
            if key in row.keys():
                result[key] = row[key]
        return result

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
                       size_before_bytes, size_after_bytes, saved_bytes,
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
                    _escape_html(row["result"]),
                    _escape_html(row["duration"]),
                    _escape_html(row["processed"]),
                    _escape_html(row["success"]),
                    _escape_html(row["skipped"]),
                    _escape_html(row["failed"]),
                    _escape_html(row["saved"]),
                    '<a href="/runs/%s">%s</a>' % (run_id, run_id),
                )
            )

        return """<table style=\"border-collapse: collapse; width: 100%%; border: 1px solid #ddd;\">
  <thead>
    <tr>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Time</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Library</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Result</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Duration</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Processed</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Success</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Skipped</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Failed</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Saved</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Run ID</th>
    </tr>
  </thead>
  <tbody>
    %s
  </tbody>
</table>""" % "".join(row_html)

    def _run_summary_html(self, run: Dict[str, str]) -> str:
        items = [
            ("Run ID", _escape_html(str(run.get("run_id") or "-"))),
            ("Timestamp", _escape_html(str(run.get("ts_end") or run.get("ts_start") or "Unknown"))),
            ("Library", _escape_html(str(run.get("library") or "Unknown"))),
            ("Result", _escape_html(str(run.get("result") or "completed"))),
            ("Duration", _escape_html(_format_duration_seconds(run.get("duration_seconds")))),
            ("Candidates Found", _escape_html(str(run.get("candidates_found") or 0))),
            ("Evaluated", _escape_html(str(run.get("evaluated_count") or 0))),
            ("Processed", _escape_html(str(run.get("processed_count") or 0))),
            ("Success", _escape_html(str(run.get("success_count") or 0))),
            ("Skipped", _escape_html(str(run.get("skipped_count") or 0))),
            ("Failed", _escape_html(str(run.get("failed_count") or 0))),
            ("Saved", _escape_html(_format_saved_bytes(run.get("saved_bytes")))),
        ]
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
                items.append((label, _escape_html(str(run.get(key) or 0))))

        lines = ["<li><strong>%s:</strong> %s</li>" % (label, value) for label, value in items]
        return '<h2>Run Summary</h2><ul style="line-height:1.5;">%s</ul>' % "".join(lines)

    def _raw_log_path_html(self, run: Dict[str, str]) -> str:
        raw_log_path = str(run.get("raw_log_path") or "").strip()
        if raw_log_path:
            return '<h2 style="margin-top: 1rem;">Raw Log Path</h2><pre style="padding: 0.5rem; border: 1px solid #ddd; background: #fafafa;">%s</pre>' % _escape_html(raw_log_path)
        return '<h2 style="margin-top: 1rem;">Raw Log Path</h2><div style="padding: 0.5rem; border: 1px solid #ddd;">No raw log path recorded for this run.</div>'

    def _run_encodes_html(self, rows: List[Dict[str, str]]) -> str:
        if not rows:
            return '<div style="padding: 0.5rem; border: 1px solid #ddd;">No file-level entries recorded for this run.</div>'

        body_rows = []
        for row in rows:
            body_rows.append(
                "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
                % (
                    _escape_html(row["path"]),
                    _escape_html(row["status"]),
                    _escape_html(row["codec_info"]),
                    _escape_html(row["before"]),
                    _escape_html(row["after"]),
                    _escape_html(row["saved"]),
                    _escape_html(row["reason"]),
                )
            )

        return """<table style=\"border-collapse: collapse; width: 100%%; border: 1px solid #ddd;\">
  <thead>
    <tr>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Path</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Status</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Codec</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Before</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">After</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Saved</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Reason / Detail</th>
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
            conn.close()
        except Exception:
            return None

        if row is None:
            return None

        return {
            "movies_saved": int(row["movies_saved"] or 0),
            "tv_saved": int(row["tv_saved"] or 0),
            "total_saved": int(row["total_saved"] or 0),
            "files_optimized": int(row["files_optimized"] or 0),
        }

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
        rows = conn.execute(
            """
            SELECT ts, library, run_id, event_type, message
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
                run_id_html = '<a href="/runs/%s">%s</a>' % (escaped_run_id, escaped_run_id)
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
        self._record_activity(
            event_type="run_started",
            message="%s run started" % library_record.name,
            library=library_record.name,
            run_id=run_id,
        )

        with editable_settings_environment(self._editable_settings):
            with library_runtime_environment(library_record):
                LOGGER.info("Starting %s %s run", trigger, library_record.name)
                rc = run()
                LOGGER.info("Finished %s %s run with exit code %s", trigger, library_record.name, rc)
        self._record_activity(
            event_type="run_completed",
            message="%s run completed" % library_record.name,
            library=library_record.name,
            run_id=run_id,
        )

    def stop_background_worker(self) -> None:
        worker_thread = self._worker_thread
        if worker_thread.is_alive():
            worker_thread.join(timeout=2)

    def run_forever(self) -> int:
        self._record_activity(event_type="service_start", message="Service startup complete")
        self.register_jobs()
        self.scheduler.start()
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
                    self.update_editable_settings,
                    self.settings_saved_message,
                    self.create_library,
                    self.update_library,
                    self.delete_library,
                    self.toggle_library,
                    self.manual_run_payload,
                )
        finally:
            with self._job_condition:
                self._worker_shutdown = True
                self._job_condition.notify_all()
            self.scheduler.shutdown(wait=False)
            self.stop_background_worker()

        return 0


@contextmanager
def library_runtime_environment(library: RuntimeLibrary) -> Iterator[None]:
    values = {
        "LIBRARY": library.name,
        "LOG_PREFIX": _slugify_library_name(library.name),
        "MEDIA_ROOT": library.path,
        "MIN_SIZE_GB": _env("MIN_SIZE_GB", "0"),
    }
    original: Dict[str, Optional[str]] = {key: os.environ.get(key) for key in values}

    try:
        for key, value in values.items():
            os.environ[key] = value
        yield
    finally:
        for key, value in original.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


@contextmanager
def library_environment(library: str) -> Iterator[None]:
    values = _library_values(library)
    original: Dict[str, Optional[str]] = {key: os.environ.get(key) for key in values}

    try:
        for key, value in values.items():
            os.environ[key] = value
        yield
    finally:
        for key, value in original.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


@contextmanager
def editable_settings_environment(values: Dict[str, str]) -> Iterator[None]:
    env_map = {
        "min_file_age_minutes": "MIN_FILE_AGE_MINUTES",
        "max_files": "MAX_FILES",
        "min_savings_percent": "MIN_SAVINGS_PERCENT",
        "max_savings_percent": "MAX_SAVINGS_PERCENT",
        "retry_count": "RETRY_COUNT",
        "retry_backoff_secs": "RETRY_BACKOFF_SECS",
        "skip_codecs": "SKIP_CODECS",
        "skip_resolution_tags": "SKIP_RESOLUTION_TAGS",
        "skip_min_height": "SKIP_MIN_HEIGHT",
        "validate_seconds": "VALIDATE_SECONDS",
        "log_retention_days": "LOG_RETENTION_DAYS",
        "bak_retention_days": "BAK_RETENTION_DAYS",
    }
    original: Dict[str, Optional[str]] = {name: os.environ.get(name) for name in env_map.values()}

    try:
        for key, env_name in env_map.items():
            if key in values:
                os.environ[env_name] = str(values[key])
        yield
    finally:
        for env_name, value in original.items():
            if value is None:
                os.environ.pop(env_name, None)
            else:
                os.environ[env_name] = value


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
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
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
        if day == "7":
            day = "0"
        if day not in {"0", "1", "2", "3", "4", "5", "6"}:
            return None
        if day not in normalized_days:
            normalized_days.append(day)

    ordered_days = [day for _, day in WEEKDAY_CHOICES if day in normalized_days]
    if not ordered_days:
        return None

    return {
        "days": ordered_days,
        "time": "%02d:%02d" % (hour, minute),
    }


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


def _derive_run_status(success_count: int, failed_count: int, skipped_count: int) -> str:
    """Map run counters to a compact status label for the operator page."""
    if failed_count > 0:
        return "failed"
    if success_count > 0:
        return "success"
    if skipped_count > 0:
        return "skipped"
    return "completed"


def _format_duration_seconds(value) -> str:
    try:
        seconds = float(value)
    except Exception:
        return "Unknown"
    if seconds < 0:
        return "Unknown"
    return "%.1fs" % seconds


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
    update_settings_fn: Callable[[Dict[str, str]], None],
    settings_saved_message_fn: Callable[[Dict[str, str]], str],
    create_library_fn: Callable[[Dict[str, str]], str],
    update_library_fn: Callable[[Dict[str, str]], str],
    delete_library_fn: Callable[[Dict[str, str]], str],
    toggle_library_fn: Callable[[Dict[str, str]], str],
    manual_run_fn: Callable[[str], tuple],
) -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            if self.path in ("/", "/dashboard"):
                payload = home_html_fn().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            if self.path == "/settings":
                payload = settings_html_fn("").encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            if self.path == "/runs":
                payload = runs_html_fn().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            if self.path == "/history":
                payload = history_html_fn().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            if self.path.startswith("/runs/"):
                run_id = unquote(self.path[len("/runs/") :])
                html, status_code = run_detail_html_fn(run_id)
                payload = html.encode("utf-8")
                self.send_response(status_code)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            if self.path == "/activity":
                payload = activity_html_fn().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            if self.path == "/system":
                payload = system_html_fn().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            if self.path != "/health":
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
            if self.path.startswith("/libraries/") and self.path.endswith("/run"):
                parts = [part for part in self.path.split("/") if part]
                if len(parts) == 3 and parts[0] == "libraries" and parts[2] == "run":
                    try:
                        library_id = int(parts[1])
                    except ValueError:
                        self.send_response(400)
                        self.end_headers()
                        return
                    payload, status_code = manual_run_fn(str(library_id))
                else:
                    self.send_response(404)
                    self.end_headers()
                    return
            elif self.path == "/run/movies":
                payload, status_code = manual_run_fn("movies")
            elif self.path == "/run/tv":
                payload, status_code = manual_run_fn("tv")
            elif self.path == "/settings":
                content_length = int(self.headers.get("Content-Length", "0") or "0")
                body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else ""
                updates = {key: values[-1] for key, values in parse_qs(body, keep_blank_values=True).items() if values}
                update_settings_fn(updates)
                html = settings_html_fn(settings_saved_message_fn(updates))
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

    server = HTTPServer((host, port), Handler)
    server.serve_forever()


def run_service() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = ServiceSettings.from_env()
    service = ChonkService(settings)
    return service.run_forever()
