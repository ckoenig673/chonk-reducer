from __future__ import annotations

import os
import threading

from chonk_reducer import cli
from chonk_reducer import service as service_module
from chonk_reducer.service import ChonkService, ServiceSettings, library_environment


def _call_get(service, path):
    if service_module.FastAPI is not None and isinstance(service.app, service_module.FastAPI):
        from starlette.testclient import TestClient

        with TestClient(service.app) as client:
            response = client.get(path)
        body = response.text
        try:
            payload = response.json()
        except Exception:
            payload = None
        return response.status_code, body, payload

    handler = service.app.routes["GET %s" % path]
    result = handler()
    if path == "/health":
        return 200, None, result
    return 200, result, None


def _call_post(service, path):
    if service_module.FastAPI is not None and isinstance(service.app, service_module.FastAPI):
        from starlette.testclient import TestClient

        with TestClient(service.app) as client:
            response = client.post(path)
        return response.status_code, response.json()

    handler = service.app.routes["POST %s" % path]
    if path == "/run/movies":
        result = handler()
    else:
        result = handler()
    return (202 if result["status"] == "started" else 409), result


def test_service_settings_from_env(monkeypatch):
    monkeypatch.setenv("SERVICE_MODE", "true")
    monkeypatch.setenv("SERVICE_HOST", "127.0.0.1")
    monkeypatch.setenv("SERVICE_PORT", "9090")
    monkeypatch.setenv("MOVIE_SCHEDULE", "0 1 * * *")
    monkeypatch.setenv("TV_SCHEDULE", "15 2 * * *")

    settings = ServiceSettings.from_env()

    assert settings.enabled is True
    assert settings.host == "127.0.0.1"
    assert settings.port == 9090
    assert settings.movie_schedule == "0 1 * * *"
    assert settings.tv_schedule == "15 2 * * *"


def test_scheduler_registers_jobs_from_env():
    settings = ServiceSettings(
        enabled=True,
        host="0.0.0.0",
        port=8080,
        movie_schedule="0 1 * * *",
        tv_schedule="15 2 * * *",
    )
    service = ChonkService(settings)

    service.register_jobs()

    jobs = {job.id for job in service.scheduler.get_jobs()}
    assert "movies-schedule" in jobs
    assert "tv-schedule" in jobs


def test_blank_schedule_disables_job_registration():
    settings = ServiceSettings(
        enabled=True,
        host="0.0.0.0",
        port=8080,
        movie_schedule="",
        tv_schedule="",
    )
    service = ChonkService(settings)

    service.register_jobs()

    assert service.scheduler.get_jobs() == []


def test_home_page_route_returns_minimal_operator_page():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/")

    assert status_code == 200
    assert "Chonk Reducer" in body
    assert "Run Movies" in body
    assert "Run TV" in body


def test_health_endpoint_payload():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    assert service.health_payload() == {"status": "ok"}


def test_post_run_movies_starts_manual_run(monkeypatch):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    done = threading.Event()

    def fake_run_once(library, trigger):
        assert library == "movies"
        assert trigger == "manual"
        done.set()

    monkeypatch.setattr(service, "_run_library_once", fake_run_once)
    status_code, payload = _call_post(service, "/run/movies")

    assert status_code == 202
    assert payload == {"status": "started", "library": "movies"}
    assert done.wait(timeout=1)


def test_post_run_tv_starts_manual_run(monkeypatch):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    done = threading.Event()

    def fake_run_once(library, trigger):
        assert library == "tv"
        assert trigger == "manual"
        done.set()

    monkeypatch.setattr(service, "_run_library_once", fake_run_once)
    status_code, payload = _call_post(service, "/run/tv")

    assert status_code == 202
    assert payload == {"status": "started", "library": "tv"}
    assert done.wait(timeout=1)


def test_post_run_movies_rejects_overlap(monkeypatch):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    gate = threading.Event()

    def blocking_run_once(library, trigger):
        assert library == "movies"
        assert trigger == "manual"
        gate.wait(timeout=1)

    monkeypatch.setattr(service, "_run_library_once", blocking_run_once)
    first_status, first_payload = _call_post(service, "/run/movies")
    second_status, second_payload = _call_post(service, "/run/movies")
    gate.set()

    assert first_status == 202
    assert first_payload == {"status": "started", "library": "movies"}
    assert second_status == 409
    assert second_payload == {"status": "busy", "library": "movies"}


def test_prevents_overlapping_runs(monkeypatch):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    calls = []

    def fake_run_once(library, trigger):
        calls.append((library, trigger))

    monkeypatch.setattr(service, "_run_library_once", fake_run_once)

    lock = service._library_locks["movies"]
    lock.acquire()
    try:
        started = service.trigger_library("movies")
    finally:
        lock.release()

    assert started is False
    assert calls == []


def test_trigger_library_starts_scheduled_run(monkeypatch):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    calls = []

    def fake_run_once(library, trigger):
        calls.append((library, trigger))

    monkeypatch.setattr(service, "_run_library_once", fake_run_once)

    started = service.trigger_library("movies")

    assert started is True
    assert calls == [("movies", "schedule")]


def test_library_environment_sets_expected_values(monkeypatch):
    monkeypatch.delenv("LIBRARY", raising=False)
    monkeypatch.delenv("LOG_PREFIX", raising=False)

    with library_environment("movies"):
        assert os.getenv("LIBRARY") == "movies"
        assert os.getenv("LOG_PREFIX") == "movie"

    assert os.getenv("LIBRARY") is None
    assert os.getenv("LOG_PREFIX") is None


def test_cli_service_mode_enabled_routes_to_service(monkeypatch):
    monkeypatch.setenv("SERVICE_MODE", "true")
    monkeypatch.setattr(cli, "run_service", lambda: 33)
    monkeypatch.setattr(cli, "run", lambda: 99)

    rc = cli.main([])

    assert rc == 33


def test_cli_no_service_mode_defaults_to_one_shot(monkeypatch):
    monkeypatch.delenv("SERVICE_MODE", raising=False)
    monkeypatch.setattr(cli, "run_service", lambda: 33)
    monkeypatch.setattr(cli, "run", lambda: 7)

    rc = cli.main([])

    assert rc == 7
