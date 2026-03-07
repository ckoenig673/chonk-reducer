from __future__ import annotations

import json
import logging
import os
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Callable, Dict, Iterator, List, Optional

try:
    from apscheduler.schedulers.background import BackgroundScheduler  # type: ignore
    from apscheduler.triggers.cron import CronTrigger  # type: ignore
except Exception:  # pragma: no cover - exercised by fallback tests
    BackgroundScheduler = None
    CronTrigger = None

try:
    from fastapi import FastAPI  # type: ignore
except Exception:  # pragma: no cover - exercised by fallback tests
    FastAPI = None

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
        self.routes: Dict[str, Callable[[], dict]] = {}

    def get(self, path: str):
        def decorator(fn):
            self.routes[path] = fn
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
        @self.app.get("/health")
        def health() -> dict:
            return self.health_payload()

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

    def trigger_library(self, library: str) -> None:
        lock = self._library_locks[library]
        if not lock.acquire(blocking=False):
            LOGGER.info("%s run already in progress; skipping overlapping schedule", library)
            return

        try:
            self._run_library_once(library)
        finally:
            lock.release()

    def _run_library_once(self, library: str) -> None:
        with library_environment(library):
            LOGGER.info("Starting scheduled %s run", library)
            rc = run()
            LOGGER.info("Finished scheduled %s run with exit code %s", library, rc)

    def run_forever(self) -> int:
        self.register_jobs()
        self.scheduler.start()
        LOGGER.info("Service scheduler started")

        try:
            if uvicorn is not None and FastAPI is not None and isinstance(self.app, FastAPI):
                uvicorn.run(self.app, host=self.settings.host, port=self.settings.port)
            else:
                _run_simple_health_server(self.settings.host, self.settings.port, self.health_payload)
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


def _run_simple_health_server(host: str, port: int, health_fn: Callable[[], dict]) -> None:
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
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
