from __future__ import annotations

import json
import os
import sqlite3
import threading

from chonk_reducer import cli
from chonk_reducer import service as service_module
from chonk_reducer.service import ChonkService, ServiceSettings, library_environment


def _call_get(service, path):
    can_use_test_client = service_module.FastAPI is not None and isinstance(service.app, service_module.FastAPI)
    if can_use_test_client:
        try:
            from starlette.testclient import TestClient

            with TestClient(service.app) as client:
                response = client.get(path)
            body = response.text
            try:
                payload = response.json()
            except Exception:
                payload = None
            return response.status_code, body, payload
        except Exception:
            for route in service.app.routes:
                methods = getattr(route, "methods", set())
                if getattr(route, "path", None) == path and "GET" in methods:
                    result = route.endpoint()
                    if hasattr(result, "body"):
                        return 200, result.body.decode("utf-8"), None
                    return 200, result, None

    handler = service.app.routes["GET %s" % path]
    result = handler()
    if path == "/health":
        return 200, None, result
    return 200, result, None


def _call_post(service, path):
    can_use_test_client = service_module.FastAPI is not None and isinstance(service.app, service_module.FastAPI)
    if can_use_test_client:
        try:
            from starlette.testclient import TestClient

            with TestClient(service.app) as client:
                response = client.post(path)
            return response.status_code, response.json()
        except Exception:
            for route in service.app.routes:
                methods = getattr(route, "methods", set())
                if getattr(route, "path", None) == path and "POST" in methods:
                    result = route.endpoint()
                    if hasattr(result, "status_code") and hasattr(result, "body"):
                        return int(result.status_code), json.loads(result.body.decode("utf-8"))
                    return (202 if result["status"] == "started" else 409), result

    handler = service.app.routes["POST %s" % path]
    result = handler()
    return (202 if result["status"] == "started" else 409), result


def _seed_run(
    db_path,
    library,
    ts_end,
    success_count=0,
    failed_count=0,
    skipped_count=0,
    duration_seconds=0.0,
    saved_bytes=0,
):
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            ts_start TEXT NOT NULL,
            ts_end TEXT NOT NULL,
            mode TEXT,
            library TEXT,
            version TEXT,
            encoder TEXT,
            quality INTEGER,
            preset INTEGER,
            success_count INTEGER NOT NULL DEFAULT 0,
            failed_count INTEGER NOT NULL DEFAULT 0,
            skipped_count INTEGER NOT NULL DEFAULT 0,
            before_bytes INTEGER NOT NULL DEFAULT 0,
            after_bytes INTEGER NOT NULL DEFAULT 0,
            saved_bytes INTEGER NOT NULL DEFAULT 0,
            duration_seconds REAL NOT NULL DEFAULT 0.0
        )
        """
    )
    conn.execute(
        """
        INSERT INTO runs(
            run_id, ts_start, ts_end, mode, library, version, encoder, quality, preset,
            success_count, failed_count, skipped_count, before_bytes, after_bytes, saved_bytes, duration_seconds
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            f"{library}-{ts_end}",
            ts_end,
            ts_end,
            "normal",
            library,
            "test",
            "hevc_qsv",
            21,
            7,
            int(success_count),
            int(failed_count),
            int(skipped_count),
            0,
            0,
            int(saved_bytes),
            float(duration_seconds),
        ),
    )
    conn.commit()
    conn.close()

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
    assert "Lifetime Savings" in body
    assert "Recent Runs" in body


def test_home_page_shows_placeholder_when_no_runs_recorded(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/")

    assert status_code == 200
    assert body.count("No runs recorded yet") == 2
    assert "No reclaimed storage recorded yet" in body
    assert "No recent runs recorded yet" in body


def test_home_page_shows_lifetime_savings_values_for_movies_and_tv(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    _seed_run(
        db_path,
        library="movies",
        ts_end="2026-01-02T08:00:00",
        success_count=2,
        failed_count=0,
        skipped_count=0,
        saved_bytes=3 * 1024 * 1024 * 1024,
    )
    _seed_run(
        db_path,
        library="tv",
        ts_end="2026-01-02T09:00:00",
        success_count=3,
        failed_count=0,
        skipped_count=1,
        saved_bytes=2 * 1024 * 1024 * 1024,
    )
    _seed_run(
        db_path,
        library="movies",
        ts_end="2026-01-02T10:00:00",
        success_count=0,
        failed_count=1,
        skipped_count=0,
        saved_bytes=1 * 1024 * 1024 * 1024,
    )
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/")

    assert status_code == 200
    assert "Lifetime Savings" in body
    assert "Movies reclaimed:</strong> 3.0 GB" in body
    assert "TV reclaimed:</strong> 2.0 GB" in body
    assert "Total reclaimed:</strong> 5.0 GB" in body
    assert "Files optimized:</strong> 5" in body


def test_home_page_shows_lifetime_savings_empty_state_when_no_successful_runs(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    _seed_run(
        db_path,
        library="movies",
        ts_end="2026-01-03T09:00:00",
        success_count=0,
        failed_count=1,
        skipped_count=0,
        saved_bytes=0,
    )
    _seed_run(
        db_path,
        library="tv",
        ts_end="2026-01-03T10:00:00",
        success_count=0,
        failed_count=0,
        skipped_count=2,
        saved_bytes=0,
    )
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/")

    assert status_code == 200
    assert "Lifetime Savings" in body
    assert "No reclaimed storage recorded yet" in body


def test_home_page_shows_latest_movies_run_information(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    _seed_run(
        db_path,
        library="movies",
        ts_end="2026-01-01T09:00:00",
        success_count=0,
        failed_count=0,
        skipped_count=2,
        duration_seconds=5.0,
    )
    _seed_run(
        db_path,
        library="movies",
        ts_end="2026-01-01T10:00:00",
        success_count=3,
        failed_count=0,
        skipped_count=1,
        duration_seconds=12.4,
    )
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/")

    assert status_code == 200
    assert "Movies" in body
    assert "2026-01-01T10:00:00" in body
    assert "Status:</strong> success" in body
    assert "Duration:</strong> 12.4s" in body


def test_home_page_shows_latest_tv_run_information(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    _seed_run(
        db_path,
        library="tv",
        ts_end="2026-01-02T11:00:00",
        success_count=0,
        failed_count=1,
        skipped_count=0,
        duration_seconds=2.0,
    )
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/")

    assert status_code == 200
    assert "TV" in body
    assert "2026-01-02T11:00:00" in body
    assert "Status:</strong> failed" in body
    assert "Duration:</strong> 2.0s" in body


def test_home_page_shows_recent_runs_table_with_latest_rows(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    _seed_run(
        db_path,
        library="movies",
        ts_end="2026-01-02T08:00:00",
        success_count=2,
        failed_count=0,
        skipped_count=0,
        duration_seconds=12.0,
        saved_bytes=1024,
    )
    _seed_run(
        db_path,
        library="tv",
        ts_end="2026-01-02T09:00:00",
        success_count=0,
        failed_count=1,
        skipped_count=0,
        duration_seconds=9.0,
        saved_bytes=0,
    )
    _seed_run(
        db_path,
        library="tv",
        ts_end="2026-01-02T10:00:00",
        success_count=0,
        failed_count=0,
        skipped_count=3,
        duration_seconds=4.0,
        saved_bytes=0,
    )
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/")

    assert status_code == 200
    assert "Recent Runs" in body
    assert "<th style=\"text-align: left; border-bottom: 1px solid #ddd; padding: 0.25rem;\">Time</th>" in body
    assert "2026-01-02T10:00:00" in body
    assert "2026-01-02T09:00:00" in body
    assert "2026-01-02T08:00:00" in body
    recent_runs_start = body.index("Recent Runs")
    recent_runs_html = body[recent_runs_start:]
    assert recent_runs_html.index("2026-01-02T10:00:00") < recent_runs_html.index("2026-01-02T09:00:00")
    assert recent_runs_html.index("2026-01-02T09:00:00") < recent_runs_html.index("2026-01-02T08:00:00")
    assert "<td>skipped</td>" in body
    assert "<td>failed</td>" in body
    assert "<td>success</td>" in body
    assert "<td>1.0 KB</td>" in body


def test_home_page_recent_runs_empty_state_when_runs_table_has_no_rows(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            ts_start TEXT NOT NULL,
            ts_end TEXT NOT NULL,
            mode TEXT,
            library TEXT,
            version TEXT,
            encoder TEXT,
            quality INTEGER,
            preset INTEGER,
            success_count INTEGER NOT NULL DEFAULT 0,
            failed_count INTEGER NOT NULL DEFAULT 0,
            skipped_count INTEGER NOT NULL DEFAULT 0,
            before_bytes INTEGER NOT NULL DEFAULT 0,
            after_bytes INTEGER NOT NULL DEFAULT 0,
            saved_bytes INTEGER NOT NULL DEFAULT 0,
            duration_seconds REAL NOT NULL DEFAULT 0.0
        )
        """
    )
    conn.commit()
    conn.close()
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/")

    assert status_code == 200
    assert "Recent Runs" in body
    assert "No recent runs recorded yet" in body


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
