from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from time import strftime
from typing import Callable, Dict, Iterator, List, Optional
from urllib.parse import parse_qs

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


LOGGER = logging.getLogger("chonk_reducer.service")


EDITABLE_SETTINGS = {
    "movie_schedule": {"env": "MOVIE_SCHEDULE", "default": ""},
    "tv_schedule": {"env": "TV_SCHEDULE", "default": ""},
    "min_file_age_minutes": {"env": "MIN_FILE_AGE_MINUTES", "default": "10"},
    "max_files": {"env": "MAX_FILES", "default": "1"},
    "min_savings_percent": {"env": "MIN_SAVINGS_PERCENT", "default": "15"},
}


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
        self.settings = ServiceSettings(
            enabled=settings.enabled,
            host=settings.host,
            port=settings.port,
            movie_schedule=settings.movie_schedule or self._editable_settings.get("movie_schedule", ""),
            tv_schedule=settings.tv_schedule or self._editable_settings.get("tv_schedule", ""),
            settings_db_path=settings_db_path,
        )
        self.scheduler = self._build_scheduler()
        self.app = self._build_app()
        self._library_locks = {
            "movies": threading.Lock(),
            "tv": threading.Lock(),
        }
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
            return self._html_response(self._render_placeholder_page("Runs", "Runs page coming soon."))

        @self.app.get("/activity")
        def activity_page():
            return self._html_response(self.activity_page_html())

        @self.app.get("/system")
        def system_page():
            return self._html_response(self._render_placeholder_page("System", "System page coming soon."))

        @self.app.get("/settings")
        def settings_page():
            return self._html_response(self.settings_page_html())

        @self.app.post("/settings")
        async def save_settings(request: Request = None):  # type: ignore[assignment]
            values: Dict[str, str] = {}
            if request is not None and hasattr(request, "form"):
                form = await request.form()
                values = {key: str(value) for key, value in form.items()}
            self.update_editable_settings(values)
            return self._html_response(self.settings_page_html("Settings saved."))

        @self.app.get("/health")
        def health() -> dict:
            return self.health_payload()

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

    def _html_response(self, html: str):
        if HTMLResponse is not None:
            return HTMLResponse(content=html)
        return html

    def home_page_html(self) -> str:
        movies_status = self._latest_run_status("movies")
        tv_status = self._latest_run_status("tv")
        recent_runs = self._recent_runs(limit=10)
        lifetime_savings = self._lifetime_savings()
        content = """
  <h1>Dashboard</h1>
  <p>Manual run controls for troubleshooting and operational checks.</p>
  <h2 style=\"margin-bottom: 0.5rem;\">Movies</h2>
  <form method=\"post\" action=\"/run/movies\" style=\"margin-bottom: 0.5rem;\">
    <button type=\"submit\">Run Movies</button>
  </form>
  %s
  <h2 style=\"margin-top: 1rem; margin-bottom: 0.5rem;\">TV</h2>
  <form method=\"post\" action=\"/run/tv\" style=\"margin-bottom: 0.5rem;\">
    <button type=\"submit\">Run TV</button>
  </form>
  %s
  <h2 style=\"margin-top: 1rem; margin-bottom: 0.5rem;\">Lifetime Savings</h2>
  %s
  <h2 style=\"margin-top: 1rem; margin-bottom: 0.5rem;\">Recent Runs</h2>
  %s
""" % (
            self._status_block_html(movies_status),
            self._status_block_html(tv_status),
            self._lifetime_savings_html(lifetime_savings),
            self._recent_runs_html(recent_runs),
        )
        return self._render_shell_html("Dashboard", content)

    def settings_page_html(self, message: str = "") -> str:
        rows = []
        for key in EDITABLE_SETTINGS:
            value = self._editable_settings.get(key, "")
            rows.append(
                """<label for=\"{key}\" style=\"display:block; margin-top: 0.75rem;\"><strong>{label}</strong></label>
  <input id=\"{key}\" name=\"{key}\" value=\"{value}\" style=\"width: 100%; max-width: 420px;\" />""".format(
                    key=key,
                    label=key.replace("_", " ").title(),
                    value=_escape_html(value),
                )
            )

        content = "<h1>Settings</h1><p>Editable service settings persisted in SQLite.</p>"
        if message:
            content += '<div style="padding: 0.5rem; border: 1px solid #cfe9cf; background:#f4fff4;">%s</div>' % _escape_html(
                message
            )
        content += """<form method=\"post\" action=\"/settings\">%s
  <div style=\"margin-top: 1rem;\"><button type=\"submit\">Save</button></div>
</form>
<p style=\"margin-top: 1rem; color: #555;\">Schedule changes are applied on service startup. Restart the service after updating schedules.</p>""" % "".join(
            rows
        )
        return self._render_shell_html("Settings", content)

    def activity_page_html(self) -> str:
        rows = self._recent_activity(limit=25)
        content = "<h1>Activity</h1><p>Recent operator-facing service events from SQLite.</p>"
        content += self._recent_activity_html(rows)
        return self._render_shell_html("Activity", content)

    def _render_placeholder_page(self, title: str, message: str) -> str:
        content = "<h1>%s</h1><p>%s</p>" % (_escape_html(title), _escape_html(message))
        return self._render_shell_html(title, content)

    def _render_shell_html(self, title: str, content_html: str) -> str:
        nav_items = [
            ("Dashboard", "/dashboard"),
            ("Runs", "/runs"),
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

    def register_jobs(self) -> None:
        self._register_library_job("movies", self.settings.movie_schedule)
        self._register_library_job("tv", self.settings.tv_schedule)

    def _register_library_job(self, library: str, schedule: str) -> None:
        schedule = (schedule or "").strip()
        if not schedule:
            LOGGER.info("No %s schedule configured; job disabled", library)
            return

        if CronTrigger is not None:
            try:
                trigger = CronTrigger.from_crontab(schedule)
            except ValueError:
                LOGGER.error("Invalid cron schedule for %s: %r", library, schedule)
                return
        else:
            if not _is_valid_crontab(schedule):
                LOGGER.error("Invalid cron schedule for %s: %r", library, schedule)
                return
            trigger = schedule

        self.scheduler.add_job(
            self.trigger_library,
            trigger=trigger,
            id="%s-schedule" % library,
            args=[library],
            coalesce=True,
            max_instances=1,
            replace_existing=True,
        )
        LOGGER.info("Registered %s schedule: %s", library, schedule)
        self._record_activity(
            event_type="schedule_registered",
            message="Scheduler registered %s schedule" % library.title(),
            library=library,
        )

    def trigger_library(self, library: str) -> bool:
        self._record_activity(
            event_type="scheduled_run_requested",
            message="Scheduled run requested for %s" % library.title(),
            library=library,
        )
        lock = self._library_locks[library]
        if not lock.acquire(blocking=False):
            LOGGER.info("%s run already in progress; skipping overlapping schedule", library)
            self._record_activity(
                event_type="run_rejected_busy",
                message="%s run skipped because library is already busy" % library.title(),
                library=library,
                level="warning",
            )
            return False

        try:
            self._run_library_once(library, trigger="schedule")
        finally:
            lock.release()
        return True

    def manual_run_payload(self, library: str):
        LOGGER.info("Manual %s run request received", library)
        self._record_activity(
            event_type="manual_run_requested",
            message="Manual run requested for %s" % library.title(),
            library=library,
        )
        started = self._start_manual_run(library)
        payload = {
            "status": "started" if started else "busy",
            "library": library,
        }
        if started:
            LOGGER.info("Manual %s run accepted and started", library)
            return payload, 202

        LOGGER.info("Manual %s run rejected; run already in progress", library)
        self._record_activity(
            event_type="run_rejected_busy",
            message="%s run skipped because library is already busy" % library.title(),
            library=library,
            level="warning",
        )
        return payload, 409

    def _start_manual_run(self, library: str) -> bool:
        lock = self._library_locks[library]
        if not lock.acquire(blocking=False):
            return False

        thread = threading.Thread(target=self._run_manual_library_once, args=(library, lock), daemon=True)
        thread.start()
        return True

    def _run_manual_library_once(self, library: str, lock: threading.Lock) -> None:
        try:
            self._run_library_once(library, trigger="manual")
        finally:
            lock.release()

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
                FROM runs
                WHERE library = ?
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
            SELECT ts, library, event_type, message
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
            row_html.append(
                "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
                % (
                    _escape_html(row["ts"]),
                    _escape_html(row["library"]),
                    _escape_html(row["event_type"]),
                    _escape_html(row["message"]),
                )
            )

        return """<table style=\"border-collapse: collapse; width: 100%%; border: 1px solid #ddd;\">
  <thead>
    <tr>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Timestamp</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Library</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Event Type</th>
      <th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Message</th>
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

    def _run_library_once(self, library: str, trigger: str) -> None:
        run_id = str(uuid.uuid4())
        self._record_activity(
            event_type="run_started",
            message="%s run started" % library.title(),
            library=library,
            run_id=run_id,
        )

        with editable_settings_environment(self._editable_settings):
            with library_environment(library):
                LOGGER.info("Starting %s %s run", trigger, library)
                rc = run()
                LOGGER.info("Finished %s %s run with exit code %s", trigger, library, rc)
        self._record_activity(
            event_type="run_completed",
            message="%s run completed" % library.title(),
            library=library,
            run_id=run_id,
        )

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
                    self.settings_page_html,
                    self.activity_page_html,
                    self._render_placeholder_page,
                    self.update_editable_settings,
                    self.manual_run_payload,
                )
        finally:
            self.scheduler.shutdown(wait=False)

        return 0


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
        "movie_schedule": "MOVIE_SCHEDULE",
        "tv_schedule": "TV_SCHEDULE",
        "min_file_age_minutes": "MIN_FILE_AGE_MINUTES",
        "max_files": "MAX_FILES",
        "min_savings_percent": "MIN_SAVINGS_PERCENT",
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
    settings_html_fn: Callable[[str], str],
    activity_html_fn: Callable[[], str],
    placeholder_html_fn: Callable[[str, str], str],
    update_settings_fn: Callable[[Dict[str, str]], None],
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

            if self.path == "/activity":
                payload = activity_html_fn().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                return

            placeholders = {
                "/runs": ("Runs", "Runs page coming soon."),
                "/system": ("System", "System page coming soon."),
            }
            if self.path in placeholders:
                title, message = placeholders[self.path]
                payload = placeholder_html_fn(title, message).encode("utf-8")
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
            if self.path == "/run/movies":
                payload, status_code = manual_run_fn("movies")
            elif self.path == "/run/tv":
                payload, status_code = manual_run_fn("tv")
            elif self.path == "/settings":
                content_length = int(self.headers.get("Content-Length", "0") or "0")
                body = self.rfile.read(content_length).decode("utf-8") if content_length > 0 else ""
                updates = {key: values[-1] for key, values in parse_qs(body, keep_blank_values=True).items() if values}
                update_settings_fn(updates)
                html = settings_html_fn("Settings saved.")
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
