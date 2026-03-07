from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Callable, Dict, Iterator, List, Optional

try:
    from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore
    from apscheduler.triggers.cron import CronTrigger  # type: ignore
except Exception:  # pragma: no cover - exercised by fallback tests
    BackgroundScheduler = None
    CronTrigger = None

try:
    from fastapi import FastAPI  # type: ignore
    from fastapi.responses import HTMLResponse, JSONResponse  # type: ignore
except Exception:  # pragma: no cover - exercised by fallback tests
    FastAPI = None
    HTMLResponse = None
    JSONResponse = None

try:
    import uvicorn  # type: ignore
except Exception:  # pragma: no cover - exercised by fallback tests
    uvicorn = None

from .runner import run


LOGGER = logging.getLogger("chonk_reducer.service")


@dataclass(frozen=True)
class ServiceSettings:
    enabled: bool
    host: str
    port: int
    movie_schedule: str
    tv_schedule: str

    @classmethod
    def from_env(cls) -> "ServiceSettings":
        return cls(
            enabled=_env_bool("SERVICE_MODE", False),
            host=_env("SERVICE_HOST", "0.0.0.0"),
            port=_env_int("SERVICE_PORT", 8080),
            movie_schedule=_env("MOVIE_SCHEDULE", ""),
            tv_schedule=_env("TV_SCHEDULE", ""),
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
        self.settings = settings
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
            if HTMLResponse is not None:
                return HTMLResponse(content=self.home_page_html())
            return self.home_page_html()

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

    def home_page_html(self) -> str:
        movies_status = self._latest_run_status("movies")
        tv_status = self._latest_run_status("tv")
        recent_runs = self._recent_runs(limit=10)
        lifetime_savings = self._lifetime_savings()
        return """<!doctype html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <title>Chonk Reducer Operator Page</title>
</head>
<body style=\"font-family: sans-serif; max-width: 540px; margin: 2rem auto;\">
  <h1>Chonk Reducer</h1>
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
</body>
</html>
""" % (
            self._status_block_html(movies_status),
            self._status_block_html(tv_status),
            self._lifetime_savings_html(lifetime_savings),
            self._recent_runs_html(recent_runs),
        )

    def health_payload(self) -> dict:
        return {"status": "ok"}

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

    def trigger_library(self, library: str) -> bool:
        lock = self._library_locks[library]
        if not lock.acquire(blocking=False):
            LOGGER.info("%s run already in progress; skipping overlapping schedule", library)
            return False

        try:
            self._run_library_once(library, trigger="schedule")
        finally:
            lock.release()
        return True

    def manual_run_payload(self, library: str):
        LOGGER.info("Manual %s run request received", library)
        started = self._start_manual_run(library)
        payload = {
            "status": "started" if started else "busy",
            "library": library,
        }
        if started:
            LOGGER.info("Manual %s run accepted and started", library)
            return payload, 202

        LOGGER.info("Manual %s run rejected; run already in progress", library)
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

    def _run_library_once(self, library: str, trigger: str) -> None:
        with library_environment(library):
            LOGGER.info("Starting %s %s run", trigger, library)
            rc = run()
            LOGGER.info("Finished %s %s run with exit code %s", trigger, library, rc)

    def run_forever(self) -> int:
        self.register_jobs()
        self.scheduler.start()
        LOGGER.info("Service scheduler started")

        try:
            if uvicorn is not None and FastAPI is not None and isinstance(self.app, FastAPI):
                uvicorn.run(self.app, host=self.settings.host, port=self.settings.port)
            else:
                _run_simple_http_server(
                    self.settings.host,
                    self.settings.port,
                    self.health_payload,
                    self.home_page_html,
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


def _run_simple_http_server(
    host: str,
    port: int,
    health_fn: Callable[[], dict],
    home_html_fn: Callable[[], str],
    manual_run_fn: Callable[[str], tuple],
) -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            if self.path == "/":
                payload = home_html_fn().encode("utf-8")
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
