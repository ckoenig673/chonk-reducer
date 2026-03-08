from __future__ import annotations

import json
import os
import sqlite3
import threading
import time

import pytest

from chonk_reducer import cli
from chonk_reducer import service as service_module
from chonk_reducer.service import ChonkService, ServiceSettings, library_environment


@pytest.fixture(autouse=True)
def _service_settings_db_path(tmp_path, monkeypatch):
    monkeypatch.setenv("STATS_PATH", str(tmp_path / "chonk.db"))



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

    if isinstance(service.app.routes, dict):
        handler = service.app.routes.get("GET %s" % path)
        if handler is None and path.startswith("/runs/"):
            handler = service.app.routes["GET /runs/{run_id}"]
            result = handler(path.split("/runs/", 1)[1])
            if hasattr(result, "status_code") and hasattr(result, "body"):
                return int(result.status_code), result.body.decode("utf-8"), None
            return 200, result, None
        result = handler()
    else:
        result = None
        for route in service.app.routes:
            methods = getattr(route, "methods", set())
            route_path = getattr(route, "path", None)
            if route_path == path and "GET" in methods:
                result = route.endpoint()
                break
            if route_path == "/runs/{run_id}" and path.startswith("/runs/") and "GET" in methods:
                run_id = path.split("/runs/", 1)[1]
                result = route.endpoint(run_id)
                break
        if result is None:
            raise KeyError("No GET route for %s" % path)

    if path == "/health":
        return 200, None, result
    if hasattr(result, "status_code") and hasattr(result, "body"):
        return int(result.status_code), result.body.decode("utf-8"), None
    return 200, result, None


def _call_post(service, path, data=None):
    def _status_code_from_payload(payload):
        status = payload.get("status")
        if status in ("queued", "started"):
            return 202
        if status == "busy":
            return 409
        if status == "not_found":
            return 404
        return 200

    can_use_test_client = service_module.FastAPI is not None and isinstance(service.app, service_module.FastAPI)
    if can_use_test_client:
        try:
            from starlette.testclient import TestClient

            with TestClient(service.app) as client:
                response = client.post(path, data=data or {})
            try:
                payload = response.json()
            except Exception:
                payload = response.text
            return response.status_code, payload
        except Exception:
            if path == "/settings" and data is not None:
                service.update_editable_settings(data)
                return 200, service.settings_page_html(service.settings_saved_message(data))
            if path == "/settings/libraries/create" and data is not None:
                return 200, service.settings_page_html(service.create_library(data))
            if path == "/settings/libraries/update" and data is not None:
                return 200, service.settings_page_html(service.update_library(data))
            if path == "/settings/libraries/delete" and data is not None:
                return 200, service.settings_page_html(service.delete_library(data))
            if path == "/settings/libraries/toggle" and data is not None:
                return 200, service.settings_page_html(service.toggle_library(data))
            for route in service.app.routes:
                methods = getattr(route, "methods", set())
                route_path = getattr(route, "path", None)
                if route_path == path and "POST" in methods:
                    result = route.endpoint()
                    if hasattr(result, "status_code") and hasattr(result, "body"):
                        return int(result.status_code), json.loads(result.body.decode("utf-8"))
                    return _status_code_from_payload(result), result
                if route_path == "/libraries/{library_id}/run" and path.startswith("/libraries/") and path.endswith("/run") and "POST" in methods:
                    library_id = int(path.split("/")[2])
                    result = route.endpoint(library_id)
                    if hasattr(result, "status_code") and hasattr(result, "body"):
                        return int(result.status_code), json.loads(result.body.decode("utf-8"))
                    return _status_code_from_payload(result), result

    if not isinstance(service.app.routes, dict):
        raise TypeError("POST helper fallback expects dict routes")
    handler = service.app.routes.get("POST %s" % path)
    if handler is None and path.startswith("/libraries/") and path.endswith("/run"):
        handler = service.app.routes.get("POST /libraries/{library_id}/run")
        if handler is not None:
            library_id = int(path.split("/")[2])
            result = handler(library_id)
            if hasattr(result, "status_code") and hasattr(result, "body"):
                return int(result.status_code), json.loads(result.body.decode("utf-8"))
            return _status_code_from_payload(result), result
    if handler is None:
        raise KeyError("No POST route for %s" % path)
    if data is None:
        result = handler()
        return _status_code_from_payload(result), result

    if hasattr(handler, "__call__") and getattr(handler, "__name__", "") == "save_settings":
        service.update_editable_settings(data)
        return 200, service.settings_page_html(service.settings_saved_message(data))

    if path == "/settings/libraries/create":
        return 200, service.settings_page_html(service.create_library(data))
    if path == "/settings/libraries/update":
        return 200, service.settings_page_html(service.update_library(data))
    if path == "/settings/libraries/delete":
        return 200, service.settings_page_html(service.delete_library(data))
    if path == "/settings/libraries/toggle":
        return 200, service.settings_page_html(service.toggle_library(data))

    result = handler()
    return _status_code_from_payload(result), result


def _seed_run(
    db_path,
    library,
    ts_end,
    success_count=0,
    failed_count=0,
    skipped_count=0,
    duration_seconds=0.0,
    saved_bytes=0,
    raw_log_path=None,
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
            ,candidates_found INTEGER NOT NULL DEFAULT 0
            ,prefiltered_count INTEGER NOT NULL DEFAULT 0
            ,evaluated_count INTEGER NOT NULL DEFAULT 0
            ,processed_count INTEGER NOT NULL DEFAULT 0
            ,prefiltered_marker_count INTEGER NOT NULL DEFAULT 0
            ,prefiltered_backup_count INTEGER NOT NULL DEFAULT 0
            ,skipped_codec_count INTEGER NOT NULL DEFAULT 0
            ,skipped_resolution_count INTEGER NOT NULL DEFAULT 0
            ,skipped_min_savings_count INTEGER NOT NULL DEFAULT 0
            ,skipped_max_savings_count INTEGER NOT NULL DEFAULT 0
            ,skipped_dry_run_count INTEGER NOT NULL DEFAULT 0
            ,ignored_folder_count INTEGER NOT NULL DEFAULT 0
            ,ignored_file_count INTEGER NOT NULL DEFAULT 0
            ,raw_log_path TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO runs(
            run_id, ts_start, ts_end, mode, library, version, encoder, quality, preset,
            success_count, failed_count, skipped_count, before_bytes, after_bytes, saved_bytes, duration_seconds, raw_log_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            raw_log_path,
        ),
    )
    conn.commit()
    conn.close()


def _read_activity_rows(db_path):
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT event_type, library, run_id, message FROM activity_events ORDER BY id ASC"
    ).fetchall()
    conn.close()
    return rows


def _seed_encode(
    db_path,
    run_id,
    ts,
    status,
    path,
    codec_from="h264",
    codec_to="hevc",
    size_before_bytes=0,
    size_after_bytes=0,
    saved_bytes=0,
    skip_reason=None,
    skip_detail=None,
    fail_stage=None,
    error_type=None,
    error_msg=None,
):
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS encodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            run_id TEXT NOT NULL,
            status TEXT NOT NULL,
            path TEXT,
            filename TEXT,
            codec_from TEXT,
            codec_to TEXT,
            size_before_bytes INTEGER,
            size_after_bytes INTEGER,
            saved_bytes INTEGER,
            skip_reason TEXT,
            skip_detail TEXT,
            fail_stage TEXT,
            error_type TEXT,
            error_msg TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT INTO encodes(
            ts, run_id, status, path, codec_from, codec_to,
            size_before_bytes, size_after_bytes, saved_bytes,
            skip_reason, skip_detail, fail_stage, error_type, error_msg
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ts,
            run_id,
            status,
            path,
            codec_from,
            codec_to,
            int(size_before_bytes),
            int(size_after_bytes),
            int(saved_bytes),
            skip_reason,
            skip_detail,
            fail_stage,
            error_type,
            error_msg,
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


def test_scheduler_registers_jobs_from_env(monkeypatch):
    monkeypatch.setenv("MOVIE_SCHEDULE", "0 1 * * *")
    monkeypatch.setenv("TV_SCHEDULE", "15 2 * * *")
    settings = ServiceSettings(
        enabled=True,
        host="0.0.0.0",
        port=8080,
        movie_schedule="",
        tv_schedule="",
    )
    service = ChonkService(settings)

    service.register_jobs()

    jobs = {job.id for job in service.scheduler.get_jobs()}
    assert "library-1-schedule" in jobs
    assert "library-2-schedule" in jobs


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
    assert "Run Now" in body
    assert "Path:" in body
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
    assert "Last Run:</strong> Never" in body
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
    assert "Recent Savings:</strong> 0 B across 0 files" in body


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
    assert "Recent Savings:</strong> 0 B across 0 files" in body


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
    assert payload == {"status": "queued", "library": "movies", "library_id": 1}
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
    assert payload == {"status": "queued", "library": "tv", "library_id": 2}
    assert done.wait(timeout=1)


def test_post_run_library_id_starts_manual_run(monkeypatch):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    done = threading.Event()

    def fake_run_once(library, trigger):
        assert library == "movies"
        assert trigger == "manual"
        done.set()

    monkeypatch.setattr(service, "_run_library_once", fake_run_once)
    status_code, payload = _call_post(service, "/libraries/1/run")

    assert status_code == 202
    assert payload == {"status": "queued", "library": "Movies", "library_id": 1}
    assert done.wait(timeout=1)


def test_disabled_library_not_scheduled_or_manually_runnable(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    service.toggle_library({"library_id": "2", "enabled": "0"})

    service.register_jobs()

    jobs = {job.id for job in service.scheduler.get_jobs()}
    assert "library-2-schedule" not in jobs
    payload, status_code = service.manual_run_payload_for_id(2)
    assert status_code == 404
    assert payload["status"] == "not_found"


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
    assert first_payload == {"status": "queued", "library": "movies", "library_id": 1}
    assert second_status == 409
    assert second_payload == {"status": "busy", "library": "movies", "library_id": 1}


def test_prevents_overlapping_runs(monkeypatch):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    started_running = threading.Event()
    release = threading.Event()

    def fake_run_once(library, trigger):
        started_running.set()
        release.wait(timeout=1)

    monkeypatch.setattr(service, "_run_library_once", fake_run_once)

    assert service.trigger_library("movies") is True
    assert started_running.wait(timeout=1)
    started = service.trigger_library("movies")
    release.set()

    assert started is False


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
    for _ in range(100):
        if len(calls) >= 1:
            break
        time.sleep(0.01)
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

def test_dashboard_library_cards_render_enabled_libraries_with_recent_run_data(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    _seed_run(
        db_path,
        library="movies",
        ts_end="2026-01-04T08:00:00",
        success_count=3,
        failed_count=1,
        skipped_count=2,
        saved_bytes=3 * 1024,
    )
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/dashboard")

    assert status_code == 200
    assert "Movies" in body
    assert "TV" in body
    assert "Path:</strong> /movies" in body
    assert "Path:</strong> /tv_shows" in body
    assert "Recent Savings:</strong> 3.0 KB across 0 files" in body


def test_dashboard_library_card_shows_manual_only_for_blank_schedule():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    status_code, body, _ = _call_get(service, "/dashboard")

    assert status_code == 200
    assert body.count("Next Run:</strong> Manual Only") == 2
    assert "Current Job Status" in body
    assert "Status</th><td" in body and "Idle" in body


def test_dashboard_route_renders_in_shell():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/dashboard")

    assert status_code == 200
    assert "Dashboard" in body
    assert "href=\"/settings\"" in body
    assert "href=\"/history\"" in body


def test_shell_routes_render_expected_pages():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    for path, heading in (("/activity", "Activity"), ("/system", "System")):
        status_code, body, _ = _call_get(service, path)
        assert status_code == 200
        assert "href=\"/dashboard\"" in body
        assert "<h1>%s</h1>" % heading in body






def test_history_route_renders_in_shell(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/history")

    assert status_code == 200
    assert "<h1>History</h1>" in body
    assert "Recent completed encode entries from SQLite" in body
    assert 'href="/history"' in body


def test_history_page_returns_rows_when_encode_stats_exist(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    run_id = "movies-2026-01-02T08:00:00"
    _seed_run(db_path, library="movies", ts_end="2026-01-02T08:00:00", success_count=1)
    _seed_encode(
        db_path,
        run_id=run_id,
        ts="2026-01-02T08:00:00",
        status="success",
        path="/movies/A.mkv",
        size_before_bytes=1024 * 1024 * 1024,
        size_after_bytes=512 * 1024 * 1024,
        saved_bytes=512 * 1024 * 1024,
    )
    _seed_encode(
        db_path,
        run_id=run_id,
        ts="2026-01-02T09:00:00",
        status="success",
        path="/movies/B.mkv",
        size_before_bytes=2 * 1024 * 1024 * 1024,
        size_after_bytes=1024 * 1024 * 1024,
        saved_bytes=1024 * 1024 * 1024,
    )
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/history")

    assert status_code == 200
    for heading in (
        "Library",
        "File Name",
        "Original Size",
        "New Size",
        "Savings %",
        "Savings Amount",
        "Date / Time",
    ):
        assert ">%s<" % heading in body
    assert "movies" in body
    assert "/movies/B.mkv" in body
    assert "2.0 GB" in body
    assert "1.0 GB" in body
    assert "50.0%" in body
    assert body.index("2026-01-02T09:00:00") < body.index("2026-01-02T08:00:00")


def test_history_page_handles_empty_stats_gracefully(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/history")

    assert status_code == 200
    assert "No completed encode history recorded yet" in body


def test_system_page_displays_service_scheduler_and_paths(monkeypatch, tmp_path):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    monkeypatch.setenv("WORK_ROOT", "/work")
    monkeypatch.setenv("MOVIE_MEDIA_ROOT", "/data/movies")
    monkeypatch.setenv("TV_MEDIA_ROOT", "/data/tv")
    monkeypatch.setenv("TZ", "UTC")

    monkeypatch.setenv("MOVIE_SCHEDULE", "0 2 * * *")
    monkeypatch.setenv("TV_SCHEDULE", "0 4 * * *")

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    class FakeJob:
        def __init__(self, job_id, next_run_time):
            self.id = job_id
            self.next_run_time = next_run_time

    class FakeScheduler:
        running = True

        def __init__(self):
            self._jobs = {
                "library-1-schedule": FakeJob("library-1-schedule", "2026-01-08 02:00:00+00:00"),
                "library-2-schedule": FakeJob("library-2-schedule", "2026-01-08 04:00:00+00:00"),
            }

        def get_job(self, job_id):
            return self._jobs.get(job_id)

        def get_jobs(self):
            return list(self._jobs.values())

    service.scheduler = FakeScheduler()

    status_code, body, _ = _call_get(service, "/system")

    assert status_code == 200
    assert "Service Information" in body
    assert "Scheduler Information" in body
    assert "Runtime / Storage Information" in body
    assert "Current Job Status" in body
    assert "Version</th>" in body
    assert "Service Mode</th><td" in body and "Enabled" in body
    assert "Scheduler Status</th><td" in body and "Running" in body
    assert "Movies Schedule" in body and "<code>0 2 * * *</code>" in body
    assert "TV Schedule" in body and "<code>0 4 * * *</code>" in body
    assert "2026-01-08 02:00:00+00:00" in body
    assert "2026-01-08 04:00:00+00:00" in body
    assert str(db_path) in body
    assert "/work" in body
    assert "Movies: /data/movies" in body
    assert "TV: /data/tv" in body
    assert "Settings Source Information" in body
    assert "environment/compose values" in body
    assert "Queue Depth" in body


def test_system_page_shows_placeholders_for_missing_schedule_data(monkeypatch, tmp_path):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    monkeypatch.delenv("WORK_ROOT", raising=False)

    service = ChonkService(
        ServiceSettings(enabled=True, host="127.0.0.1", port=9090, movie_schedule="", tv_schedule="")
    )

    class EmptyScheduler:
        running = False

        def get_job(self, job_id):
            del job_id
            return None

        def get_jobs(self):
            return []

    service.scheduler = EmptyScheduler()

    status_code, body, _ = _call_get(service, "/system")

    assert status_code == 200
    assert "Service Host</th><td" in body and "127.0.0.1" in body
    assert "Service Port</th><td" in body and "9090" in body
    assert "Scheduler Status</th><td" in body and "Stopped" in body
    assert "<code>Not set</code>" in body
    assert "Movies Schedule" in body
    assert "TV Schedule" in body
    assert "Not scheduled" in body
    assert "Work / Log Path</th><td" in body and "Not set" in body


def test_runs_page_renders_history_table_with_expected_columns(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    _seed_run(
        db_path,
        library="movies",
        ts_end="2026-01-02T08:00:00",
        success_count=2,
        failed_count=0,
        skipped_count=1,
        duration_seconds=12.0,
        saved_bytes=1024,
    )
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/runs")

    assert status_code == 200
    assert "<h1>Runs</h1>" in body
    assert "Recent run history from SQLite" in body
    for heading in (
        "Time",
        "Library",
        "Result",
        "Duration",
        "Processed",
        "Success",
        "Skipped",
        "Failed",
        "Saved",
        "Run ID",
    ):
        assert ">%s<" % heading in body
    assert "<td>movies</td>" in body
    assert "<td>success</td>" in body
    assert "<td>3</td>" in body
    assert "<td>2</td>" in body
    assert "<td>1</td>" in body
    assert "<td>0</td>" in body
    assert "<td>1.0 KB</td>" in body
    assert 'href="/runs/movies-2026-01-02T08:00:00"' in body


def test_run_detail_page_renders_summary_and_file_rows(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    _seed_run(
        db_path,
        library="movies",
        ts_end="2026-01-02T08:00:00",
        success_count=1,
        failed_count=1,
        skipped_count=1,
        duration_seconds=15.0,
        saved_bytes=1024 * 1024,
    )
    run_id = "movies-2026-01-02T08:00:00"
    _seed_encode(
        db_path,
        run_id=run_id,
        ts="2026-01-02T08:00:01",
        status="success",
        path="/movies/A.mkv",
        size_before_bytes=4 * 1024,
        size_after_bytes=3 * 1024,
        saved_bytes=1024,
    )
    _seed_encode(
        db_path,
        run_id=run_id,
        ts="2026-01-02T08:00:02",
        status="skipped",
        path="/movies/B.mkv",
        skip_reason="min_savings",
        skip_detail="below threshold",
    )
    _seed_encode(
        db_path,
        run_id=run_id,
        ts="2026-01-02T08:00:03",
        status="failed",
        path="/movies/C.mkv",
        error_type="encode_error",
        error_msg="ffmpeg failed",
    )
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/runs/%s" % run_id)

    assert status_code == 200
    assert "<h1>Run Detail</h1>" in body
    assert "Run Summary" in body
    assert "<strong>Run ID:</strong> %s" % run_id in body
    assert "<strong>Result:</strong> failed" in body
    assert "<strong>Duration:</strong> 15.0s" in body
    assert "<strong>Saved:</strong> 1.0 MB" in body
    assert "Raw Log Path" in body
    assert "No raw log path recorded for this run" in body
    assert "File-Level Entries" in body
    assert "/movies/A.mkv" in body
    assert "/movies/B.mkv" in body
    assert "/movies/C.mkv" in body
    assert "min_savings: below threshold" in body
    assert "encode_error: ffmpeg failed" in body


def test_run_detail_page_shows_raw_log_path_when_available(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    run_id = "movies-2026-01-02T08:00:00"
    _seed_run(
        db_path,
        library="movies",
        ts_end="2026-01-02T08:00:00",
        raw_log_path="/work/logs/movie_transcode_20260307_154335.log",
    )
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/runs/%s" % run_id)

    assert status_code == 200
    assert "Raw Log Path" in body
    assert "/work/logs/movie_transcode_20260307_154335.log" in body


def test_run_detail_page_shows_no_file_entries_message(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    _seed_run(db_path, library="tv", ts_end="2026-01-02T09:00:00", skipped_count=1)
    run_id = "tv-2026-01-02T09:00:00"
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/runs/%s" % run_id)

    assert status_code == 200
    assert "No file-level entries recorded for this run" in body


def test_run_detail_page_returns_404_for_unknown_run(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    _seed_run(db_path, library="movies", ts_end="2026-01-02T08:00:00")
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/runs/does-not-exist")

    assert status_code == 404
    assert "Run Not Found" in body


def test_runs_page_sorts_newest_first(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    _seed_run(db_path, library="movies", ts_end="2026-01-02T08:00:00")
    _seed_run(db_path, library="tv", ts_end="2026-01-02T09:00:00")
    _seed_run(db_path, library="tv", ts_end="2026-01-02T10:00:00")
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/runs")

    assert status_code == 200
    assert body.index("2026-01-02T10:00:00") < body.index("2026-01-02T09:00:00")
    assert body.index("2026-01-02T09:00:00") < body.index("2026-01-02T08:00:00")


def test_runs_page_shows_empty_state_when_no_rows(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/runs")

    assert status_code == 200
    assert "No runs recorded yet" in body


@pytest.mark.parametrize(
    ("saved_bytes", "expected_saved"),
    [
        (500, "<td>500 B</td>"),
        (1024 * 1024 * 3, "<td>3.0 MB</td>"),
        (1024 * 1024 * 1024 * 2, "<td>2.0 GB</td>"),
    ],
)
def test_runs_page_saved_bytes_formatting(tmp_path, monkeypatch, saved_bytes, expected_saved):
    db_path = tmp_path / "chonk.db"
    _seed_run(
        db_path,
        library="movies",
        ts_end="2026-01-02T10:00:00",
        success_count=1,
        saved_bytes=saved_bytes,
    )
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/runs")

    assert status_code == 200
    assert expected_saved in body


def test_activity_table_created_automatically(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))

    ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    conn = sqlite3.connect(str(db_path))
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='activity_events'"
    ).fetchone()
    conn.close()

    assert row is not None


def test_run_forever_records_service_and_scheduler_start_activity(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(ServiceSettings(enabled=True, host="127.0.0.1", port=8080, movie_schedule="", tv_schedule=""))
    monkeypatch.setattr(service.scheduler, "start", lambda: None)
    monkeypatch.setattr(service.scheduler, "shutdown", lambda wait=False: None)
    monkeypatch.setattr(service_module, "uvicorn", None)
    monkeypatch.setattr(service_module, "_run_simple_http_server", lambda *args, **kwargs: None)

    rc = service.run_forever()

    assert rc == 0
    event_types = [row["event_type"] for row in _read_activity_rows(db_path)]
    assert "service_start" in event_types
    assert "scheduler_start" in event_types


def test_schedule_registration_records_activity_entries(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))

    monkeypatch.setenv("MOVIE_SCHEDULE", "0 1 * * *")
    monkeypatch.setenv("TV_SCHEDULE", "15 2 * * *")

    service = ChonkService(
        ServiceSettings(
            enabled=True,
            host="0.0.0.0",
            port=8080,
            movie_schedule="",
            tv_schedule="",
        )
    )

    service.register_jobs()

    rows = _read_activity_rows(db_path)
    assert [row["event_type"] for row in rows].count("schedule_registered") == 2
    assert any(row["library"] == "Movies" for row in rows)
    assert any(row["library"] == "TV" for row in rows)


def test_manual_run_records_requested_and_busy_activity(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    started = threading.Event()
    release = threading.Event()

    def blocking_run_once(library, trigger, run_id=None):
        assert library == "movies"
        started.set()
        release.wait(timeout=1)

    monkeypatch.setattr(service, "_run_library_once", blocking_run_once)
    first_payload, first_status_code = service.manual_run_payload("movies")
    assert first_status_code == 202
    assert started.wait(timeout=1)

    payload, status_code = service.manual_run_payload("movies")
    release.set()

    assert status_code == 409
    assert payload == {"status": "busy", "library": "movies", "library_id": 1}
    assert first_payload == {"status": "queued", "library": "movies", "library_id": 1}
    event_types = [row["event_type"] for row in _read_activity_rows(db_path)]
    assert "manual_run_requested" in event_types
    assert "run_rejected_busy" in event_types


def test_scheduled_busy_rejection_records_activity(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    started_running = threading.Event()
    release = threading.Event()

    def blocking_run_once(library, trigger, run_id=None):
        started_running.set()
        release.wait(timeout=1)

    monkeypatch.setattr(service, "_run_library_once", blocking_run_once)

    assert service.trigger_library("movies") is True
    assert started_running.wait(timeout=1)
    started = service.trigger_library("movies")
    release.set()

    assert started is False
    event_types = [row["event_type"] for row in _read_activity_rows(db_path)]
    assert "scheduled_run_requested" in event_types
    assert "run_rejected_busy" in event_types


def test_run_start_and_completion_recorded(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    monkeypatch.setattr(service_module, "run", lambda: 0)

    service._run_library_once("movies", "manual")

    event_types = [row["event_type"] for row in _read_activity_rows(db_path)]
    assert "run_started" in event_types
    assert "run_completed" in event_types


def test_current_job_status_reflects_idle_queued_and_running(monkeypatch):
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    started = threading.Event()
    release = threading.Event()

    def blocking_run_once(library, trigger, run_id=None):
        started.set()
        release.wait(timeout=1)

    monkeypatch.setattr(service, "_run_library_once", blocking_run_once)

    assert service.current_job_status()["status"] == "Idle"
    payload, status = service.manual_run_payload("movies")
    assert status == 202
    assert payload["status"] == "queued"

    running_observed = False
    queued_observed = False
    for _ in range(200):
        current = service.current_job_status()
        if current["status"] == "Queued":
            queued_observed = True
        if current["status"] == "Running":
            running_observed = True
            assert current["current_library"] == "Movies"
            assert current["trigger"] == "manual"
            break
        time.sleep(0.01)

    assert queued_observed or started.wait(timeout=1)
    assert running_observed
    release.set()

    for _ in range(200):
        if service.current_job_status()["status"] == "Idle":
            break
        time.sleep(0.01)
    assert service.current_job_status()["status"] == "Idle"


def test_activity_page_shows_recent_entries(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    service._record_activity("manual_run_requested", "Manual run requested for Movies", library="movies")
    service._record_activity("run_started", "Movies run started", library="movies")

    status_code, body, _ = _call_get(service, "/activity")

    assert status_code == 200
    assert "<h1>Activity</h1>" in body
    assert "manual_run_requested" in body
    assert "Movies run started" in body




def test_activity_page_links_run_id_when_present(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    service._record_activity(
        "run_completed",
        "Run completed: movies",
        library="movies",
        run_id="fd2b992c",
    )

    status_code, body, _ = _call_get(service, "/activity")

    assert status_code == 200
    assert 'href="/runs/fd2b992c"' in body
    assert '>fd2b992c</a>' in body


def test_activity_page_run_id_plain_when_missing(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    service._record_activity("manual_run_requested", "Manual run requested for Movies", library="movies")

    status_code, body, _ = _call_get(service, "/activity")

    assert status_code == 200
    assert "<td>-</td>" in body


def test_activity_page_run_id_link_uses_existing_run_detail_route(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    run_ts = "fd2b992c"
    _seed_run(db_path, library="movies", ts_end=run_ts)
    run_id = "movies-%s" % run_ts

    service._record_activity(
        "run_completed",
        "Run completed: movies",
        library="movies",
        run_id=run_id,
    )

    activity_status, activity_body, _ = _call_get(service, "/activity")
    run_status, run_body, _ = _call_get(service, "/runs/%s" % run_id)

    assert activity_status == 200
    assert 'href="/runs/%s"' % run_id in activity_body
    assert run_status == 200
    assert "<h1>Run Detail</h1>" in run_body

def test_activity_page_shows_empty_state(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    status_code, body, _ = _call_get(service, "/activity")

    assert status_code == 200
    assert "No recent activity recorded yet" in body


def test_settings_route_renders_and_shows_editable_fields(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    monkeypatch.setenv("MAX_FILES", "9")

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    status_code, body, _ = _call_get(service, "/settings")

    assert status_code == 200
    assert "<h1>Settings</h1>" in body
    assert "name=\"movie_schedule\"" not in body
    assert "name=\"tv_schedule\"" not in body
    assert "name=\"min_file_age_minutes\"" in body
    assert "name=\"max_files\"" in body
    assert "value=\"9\"" in body
    assert "Global Settings" in body
    assert "Libraries" in body
    assert "<strong>Schedule</strong>" in body
    assert "name=\"schedule\"" in body


def test_libraries_table_created_and_bootstrapped_from_env(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    monkeypatch.setenv("MOVIE_MEDIA_ROOT", "/mnt/media/movies")
    monkeypatch.setenv("TV_MEDIA_ROOT", "/mnt/media/tv")
    monkeypatch.setenv("MOVIE_SCHEDULE", "0 3 * * *")
    monkeypatch.setenv("TV_SCHEDULE", "0 4 * * *")

    ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT name, path, enabled, schedule FROM libraries ORDER BY id ASC").fetchall()
    conn.close()

    assert len(rows) == 2
    assert rows[0]["name"] == "Movies"
    assert rows[0]["path"] == "/mnt/media/movies"
    assert int(rows[0]["enabled"]) == 1
    assert rows[0]["schedule"] == "0 3 * * *"
    assert rows[1]["name"] == "TV"
    assert rows[1]["path"] == "/mnt/media/tv"
    assert int(rows[1]["enabled"]) == 1
    assert rows[1]["schedule"] == "0 4 * * *"


def test_libraries_bootstrap_schedule_from_legacy_settings_table(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))

    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL)")
    conn.execute(
        "INSERT INTO settings(key, value, updated_at) VALUES (?, ?, ?)",
        ("movie_schedule", "5 1 * * *", "2026-01-01T00:00:00Z"),
    )
    conn.execute(
        "INSERT INTO settings(key, value, updated_at) VALUES (?, ?, ?)",
        ("tv_schedule", "10 2 * * *", "2026-01-01T00:00:00Z"),
    )
    conn.commit()
    conn.close()

    ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT name, schedule FROM libraries ORDER BY id ASC").fetchall()
    conn.close()

    assert rows[0]["name"] == "Movies"
    assert rows[0]["schedule"] == "5 1 * * *"
    assert rows[1]["name"] == "TV"
    assert rows[1]["schedule"] == "10 2 * * *"


def test_create_edit_delete_and_toggle_library(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    create_status, create_body = _call_post(
        service,
        "/settings/libraries/create",
        data={"name": "Anime", "path": "/data/anime", "enabled": "1", "schedule": "10 1 * * *"},
    )
    assert create_status == 200
    assert "Library created." in create_body

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    anime = conn.execute("SELECT id, name, path, enabled, schedule FROM libraries WHERE name = 'Anime'").fetchone()
    assert anime is not None
    library_id = int(anime["id"])
    assert anime["path"] == "/data/anime"
    assert int(anime["enabled"]) == 1
    assert anime["schedule"] == "10 1 * * *"
    conn.close()

    update_status, update_body = _call_post(
        service,
        "/settings/libraries/update",
        data={
            "library_id": str(library_id),
            "name": "Anime Updated",
            "path": "/data/anime-updated",
            "enabled": "0",
            "schedule": "20 2 * * *",
        },
    )
    assert update_status == 200
    assert "Library updated." in update_body

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    updated = conn.execute("SELECT name, path, enabled, schedule FROM libraries WHERE id = ?", (library_id,)).fetchone()
    assert updated is not None
    assert updated["name"] == "Anime Updated"
    assert updated["path"] == "/data/anime-updated"
    assert int(updated["enabled"]) == 0
    assert updated["schedule"] == "20 2 * * *"
    conn.close()

    toggle_status, toggle_body = _call_post(
        service,
        "/settings/libraries/toggle",
        data={"library_id": str(library_id), "enabled": "1"},
    )
    assert toggle_status == 200
    assert "Library enabled." in toggle_body

    conn = sqlite3.connect(str(db_path))
    enabled_value = conn.execute("SELECT enabled FROM libraries WHERE id = ?", (library_id,)).fetchone()[0]
    conn.close()
    assert int(enabled_value) == 1

    delete_status, delete_body = _call_post(
        service,
        "/settings/libraries/delete",
        data={"library_id": str(library_id)},
    )
    assert delete_status == 200
    assert "Library deleted." in delete_body

    conn = sqlite3.connect(str(db_path))
    remaining = conn.execute("SELECT COUNT(*) FROM libraries WHERE id = ?", (library_id,)).fetchone()[0]
    conn.close()
    assert int(remaining) == 0




def test_simple_schedule_helper_build_and_parse_round_trip():
    cron = service_module._build_simple_cron("13:45", ["1", "4"])
    assert cron == "45 13 * * 1,4"

    parsed = service_module._parse_simple_cron("45 13 * * 1,4")
    assert parsed is not None
    assert parsed["time"] == "13:45"
    assert parsed["days"] == ["1", "4"]


def test_simple_schedule_ui_populates_for_supported_cron(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    conn = sqlite3.connect(str(db_path))
    conn.execute("UPDATE libraries SET schedule = ? WHERE name = ?", ("30 6 * * 1,3,5", "Movies"))
    conn.commit()
    conn.close()

    status_code, body, _ = _call_get(service, "/settings")

    assert status_code == 200
    assert 'value="06:30" selected' in body
    assert 'name="schedule_day_1" value="1" checked' in body
    assert 'name="schedule_day_3" value="1" checked' in body
    assert 'name="schedule_day_5" value="1" checked' in body


def test_unsupported_cron_falls_back_to_advanced_mode(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    conn = sqlite3.connect(str(db_path))
    conn.execute("UPDATE libraries SET schedule = ? WHERE name = ?", ("*/5 * * * *", "Movies"))
    conn.commit()
    conn.close()

    status_code, body, _ = _call_get(service, "/settings")

    assert status_code == 200
    assert 'value="advanced" checked' in body
    assert 'name="schedule" value="*/5 * * * *"' in body


def test_simple_mode_create_generates_expected_cron(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    status_code, body = _call_post(
        service,
        "/settings/libraries/create",
        data={
            "name": "Kids",
            "path": "/data/kids",
            "enabled": "1",
            "schedule_mode": "simple",
            "schedule_time": "09:15",
            "schedule_day_0": "1",
            "schedule_day_6": "1",
        },
    )
    assert status_code == 200
    assert "Library created." in body

    conn = sqlite3.connect(str(db_path))
    row = conn.execute("SELECT schedule FROM libraries WHERE name = ?", ("Kids",)).fetchone()
    conn.close()
    assert row[0] == "15 9 * * 0,6"


def test_schedule_validation_in_simple_and_advanced_modes(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    status_code, body = _call_post(
        service,
        "/settings/libraries/create",
        data={
            "name": "One",
            "path": "/data/one",
            "enabled": "1",
            "schedule_mode": "simple",
            "schedule_time": "08:00",
        },
    )
    assert status_code == 200
    assert "select at least one weekday" in body

    status_code, body = _call_post(
        service,
        "/settings/libraries/create",
        data={
            "name": "Two",
            "path": "/data/two",
            "enabled": "1",
            "schedule_mode": "simple",
            "schedule_day_1": "1",
        },
    )
    assert status_code == 200
    assert "time is required in simple mode" in body

    status_code, body = _call_post(
        service,
        "/settings/libraries/create",
        data={
            "name": "Three",
            "path": "/data/three",
            "enabled": "1",
            "schedule_mode": "advanced",
            "schedule": "",
        },
    )
    assert status_code == 200
    assert "cron schedule is required in advanced mode" in body


def test_advanced_mode_update_preserves_raw_cron(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    conn = sqlite3.connect(str(db_path))
    row = conn.execute("SELECT id FROM libraries WHERE name = 'Movies'").fetchone()
    conn.close()
    library_id = int(row[0])

    status_code, body = _call_post(
        service,
        "/settings/libraries/update",
        data={
            "library_id": str(library_id),
            "name": "Movies",
            "path": "/movies",
            "enabled": "1",
            "schedule_mode": "advanced",
            "schedule": "5,35 2-4 * * 1-5",
        },
    )
    assert status_code == 200
    assert "Library updated." in body

    conn = sqlite3.connect(str(db_path))
    updated = conn.execute("SELECT schedule FROM libraries WHERE id = ?", (library_id,)).fetchone()
    conn.close()
    assert updated[0] == "5,35 2-4 * * 1-5"

def test_library_validation_rejects_duplicates_and_blanks(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    status_code, body = _call_post(
        service,
        "/settings/libraries/create",
        data={"name": "", "path": "/data/custom", "enabled": "1", "schedule": ""},
    )
    assert status_code == 200
    assert "name is required" in body

    status_code, body = _call_post(
        service,
        "/settings/libraries/create",
        data={"name": "Custom", "path": "", "enabled": "1", "schedule": ""},
    )
    assert status_code == 200
    assert "path is required" in body

    status_code, body = _call_post(
        service,
        "/settings/libraries/create",
        data={"name": "Movies", "path": "/data/another", "enabled": "1", "schedule": ""},
    )
    assert status_code == 200
    assert "duplicate library name" in body

    status_code, body = _call_post(
        service,
        "/settings/libraries/create",
        data={"name": "Another", "path": "/movies", "enabled": "1", "schedule": ""},
    )
    assert status_code == 200
    assert "duplicate library path" in body


def test_settings_table_created_and_bootstrapped_from_env(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    monkeypatch.setenv("MOVIE_SCHEDULE", "0 5 * * *")
    monkeypatch.setenv("TV_SCHEDULE", "30 6 * * *")
    monkeypatch.setenv("MIN_FILE_AGE_MINUTES", "22")
    monkeypatch.setenv("MAX_FILES", "3")
    monkeypatch.setenv("MIN_SAVINGS_PERCENT", "18")
    monkeypatch.setenv("MAX_SAVINGS_PERCENT", "45")
    monkeypatch.setenv("RETRY_COUNT", "4")
    monkeypatch.setenv("RETRY_BACKOFF_SECS", "8")
    monkeypatch.setenv("SKIP_CODECS", "mpeg2")
    monkeypatch.setenv("SKIP_RESOLUTION_TAGS", "2160p")
    monkeypatch.setenv("SKIP_MIN_HEIGHT", "720")
    monkeypatch.setenv("VALIDATE_SECONDS", "12")
    monkeypatch.setenv("LOG_RETENTION_DAYS", "40")
    monkeypatch.setenv("BAK_RETENTION_DAYS", "70")

    ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT key, value FROM settings ORDER BY key").fetchall()
    conn.close()

    assert {row["key"] for row in rows} == {
        "min_file_age_minutes",
        "max_files",
        "min_savings_percent",
        "max_savings_percent",
        "retry_count",
        "retry_backoff_secs",
        "skip_codecs",
        "skip_resolution_tags",
        "skip_min_height",
        "validate_seconds",
        "log_retention_days",
        "bak_retention_days",
    }
    values = {row["key"]: row["value"] for row in rows}
    assert values["min_file_age_minutes"] == "22"
    assert values["max_files"] == "3"
    assert values["min_savings_percent"] == "18"
    assert values["max_savings_percent"] == "45"
    assert values["retry_count"] == "4"
    assert values["retry_backoff_secs"] == "8"
    assert values["skip_codecs"] == "mpeg2"
    assert values["skip_resolution_tags"] == "2160p"
    assert values["skip_min_height"] == "720"
    assert values["validate_seconds"] == "12"
    assert values["log_retention_days"] == "40"
    assert values["bak_retention_days"] == "70"


def test_post_settings_persists_to_sqlite(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    status_code, body = _call_post(
        service,
        "/settings",
        data={
            "min_file_age_minutes": "40",
            "max_files": "6",
            "min_savings_percent": "25",
            "max_savings_percent": "35",
            "retry_count": "3",
            "retry_backoff_secs": "11",
            "skip_codecs": "h264,mpeg2",
            "skip_resolution_tags": "2160p,4k",
            "skip_min_height": "1080",
            "validate_seconds": "14",
            "log_retention_days": "15",
            "bak_retention_days": "45",
        },
    )

    assert status_code == 200
    assert "Settings saved" in body

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    values = {
        row["key"]: row["value"]
        for row in conn.execute("SELECT key, value FROM settings").fetchall()
    }
    conn.close()

    assert values["min_file_age_minutes"] == "40"
    assert values["max_files"] == "6"
    assert values["min_savings_percent"] == "25"
    assert values["max_savings_percent"] == "35"
    assert values["retry_count"] == "3"
    assert values["retry_backoff_secs"] == "11"
    assert values["skip_codecs"] == "h264,mpeg2"
    assert values["skip_resolution_tags"] == "2160p,4k"
    assert values["skip_min_height"] == "1080"
    assert values["validate_seconds"] == "14"
    assert values["log_retention_days"] == "15"
    assert values["bak_retention_days"] == "45"


def test_post_settings_update_shows_standard_saved_message(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    status_code, body = _call_post(
        service,
        "/settings",
        data={
            "min_file_age_minutes": "40",
            "max_files": "6",
            "min_savings_percent": "25",
        },
    )

    assert status_code == 200
    assert "Settings saved." in body


def test_settings_page_hides_schedule_fields_and_shows_operator_note(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    status_code, body, _ = _call_get(service, "/settings")

    assert status_code == 200
    assert "Movie Schedule</strong>" not in body
    assert "Tv Schedule</strong>" not in body
    assert "Settings are saved immediately to SQLite. Some service-level behaviors are applied on startup/restart only." in body


def test_run_uses_db_backed_editable_settings(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    monkeypatch.setenv("MAX_FILES", "2")

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    service.update_editable_settings({"max_files": "11", "min_file_age_minutes": "7", "retry_count": "9"})

    captured = {}

    def fake_run_once():
        captured["max_files"] = os.getenv("MAX_FILES")
        captured["min_file_age_minutes"] = os.getenv("MIN_FILE_AGE_MINUTES")
        captured["retry_count"] = os.getenv("RETRY_COUNT")
        return 0

    monkeypatch.setattr(service_module, "run", fake_run_once)

    service._run_library_once("movies", "manual")

    assert captured["max_files"] == "11"
    assert captured["min_file_age_minutes"] == "7"
    assert captured["retry_count"] == "9"
