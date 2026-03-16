from __future__ import annotations

import json
from datetime import datetime
import os
import socket
import sqlite3
import threading
import time
import urllib.request

import pytest

from chonk_reducer import cli
from chonk_reducer import service as service_module
from chonk_reducer.service import ChonkService, RuntimeJob, ServiceSettings, library_environment


@pytest.fixture(autouse=True)
def _service_settings_db_path(tmp_path, monkeypatch):
    monkeypatch.setenv("STATS_PATH", str(tmp_path / "chonk.db"))



def _call_get(service, path):
    normalized_path = path.split("?", 1)[0]
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
                        return int(getattr(result, "status_code", 200)), result.body.decode("utf-8"), None
                    return 200, result, None

    if isinstance(service.app.routes, dict):
        handler = service.app.routes.get("GET %s" % path)
        if handler is None:
            handler = service.app.routes.get("GET %s" % normalized_path)
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
            if route_path in (path, normalized_path) and "GET" in methods:
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


def _call_post(service, path, data=None, follow_redirects=True):
    def _status_code_from_payload(payload):
        status = payload.get("status")
        if status in ("queued", "started"):
            return 202
        if status in ("cancelling", "idle"):
            return 200
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
                response = client.post(path, data=data or {}, follow_redirects=follow_redirects)
            if not follow_redirects and 300 <= int(response.status_code) < 400:
                return response.status_code, response
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
            if path == "/settings/test-notification":
                result = service_module.notifications.send_test_notification(settings_db_path=str(service._settings_db_path))
                return 200, service.settings_page_html(str(result.get("message", "")))
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
                    return (202 if result.get("status") in ("started", "queued") else 409), result
                if route_path == "/libraries/{library_id}/preview" and path.startswith("/libraries/") and path.endswith("/preview") and "POST" in methods:
                    library_id = int(path.split("/")[2])
                    result = route.endpoint(library_id)
                    if hasattr(result, "status_code") and hasattr(result, "body"):
                        return int(result.status_code), json.loads(result.body.decode("utf-8"))
                    return (202 if result.get("status") in ("started", "queued") else 409), result
                if route_path == "/dashboard/libraries/{library_id}/run" and path.startswith("/dashboard/libraries/") and path.endswith("/run") and "POST" in methods:
                    library_id = int(path.split("/")[3])
                    result = route.endpoint(library_id)
                    if hasattr(result, "status_code") and hasattr(result, "headers"):
                        return int(result.status_code), result
                    return 200, result
                if route_path == "/dashboard/libraries/{library_id}/preview" and path.startswith("/dashboard/libraries/") and path.endswith("/preview") and "POST" in methods:
                    library_id = int(path.split("/")[3])
                    result = route.endpoint(library_id)
                    if hasattr(result, "status_code") and hasattr(result, "headers"):
                        return int(result.status_code), result
                    return 200, result

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
            return (202 if result.get("status") in ("started", "queued") else 409), result
    if handler is None and path.startswith("/libraries/") and path.endswith("/preview"):
        handler = service.app.routes.get("POST /libraries/{library_id}/preview")
        if handler is not None:
            library_id = int(path.split("/")[2])
            result = handler(library_id)
            if hasattr(result, "status_code") and hasattr(result, "body"):
                return int(result.status_code), json.loads(result.body.decode("utf-8"))
            return (202 if result.get("status") in ("started", "queued") else 409), result
    if handler is None and path.startswith("/dashboard/libraries/") and path.endswith("/run"):
        handler = service.app.routes.get("POST /dashboard/libraries/{library_id}/run")
        if handler is not None:
            library_id = int(path.split("/")[3])
            result = handler(library_id)
            if hasattr(result, "status_code") and hasattr(result, "headers"):
                return int(result.status_code), result
            return 200, result
    if handler is None and path.startswith("/dashboard/libraries/") and path.endswith("/preview"):
        handler = service.app.routes.get("POST /dashboard/libraries/{library_id}/preview")
        if handler is not None:
            library_id = int(path.split("/")[3])
            result = handler(library_id)
            if hasattr(result, "status_code") and hasattr(result, "headers"):
                return int(result.status_code), result
            return 200, result
    if handler is None:
        raise KeyError("No POST route for %s" % path)
    if data is None:
        result = handler()
        if isinstance(result, dict):
            return _status_code_from_payload(result), result
        return 200, result

    if hasattr(handler, "__call__") and getattr(handler, "__name__", "") == "save_settings":
        service.update_editable_settings(data)
        return 200, service.settings_page_html(service.settings_saved_message(data))

    if path == "/settings/test-notification":
        result = service_module.notifications.send_test_notification(settings_db_path=str(service._settings_db_path))
        return 200, service.settings_page_html(str(result.get("message", "")))

    if path == "/settings/libraries/create":
        return 200, service.settings_page_html(service.create_library(data))
    if path == "/settings/libraries/update":
        return 200, service.settings_page_html(service.update_library(data))
    if path == "/settings/libraries/delete":
        return 200, service.settings_page_html(service.delete_library(data))
    if path == "/settings/libraries/toggle":
        return 200, service.settings_page_html(service.toggle_library(data))

    result = handler()
    if isinstance(result, dict):
        return _status_code_from_payload(result), result
    return 200, result


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
    mode="normal",
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
            mode,
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
    duration_seconds=None,
    library="",
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
            library TEXT,
            status TEXT NOT NULL,
            path TEXT,
            filename TEXT,
            codec_from TEXT,
            codec_to TEXT,
            size_before_bytes INTEGER,
            size_after_bytes INTEGER,
            saved_bytes INTEGER,
            duration_seconds REAL,
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
            ts, run_id, library, status, path, codec_from, codec_to,
            size_before_bytes, size_after_bytes, saved_bytes, duration_seconds,
            skip_reason, skip_detail, fail_stage, error_type, error_msg
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ts,
            run_id,
            library,
            status,
            path,
            codec_from,
            codec_to,
            int(size_before_bytes),
            int(size_after_bytes),
            int(saved_bytes),
            duration_seconds,
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
    assert "housekeeping-daily" in jobs


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

    jobs = {job.id for job in service.scheduler.get_jobs()}
    assert jobs == {"housekeeping-daily"}


def test_scheduler_registration_normalizes_legacy_numeric_weekday_schedule(monkeypatch):
    captured = {}

    class FakeCronTrigger(object):
        def __init__(self, minute, hour, day, month, day_of_week, timezone=None):
            captured["expr"] = "%s %s %s %s %s" % (minute, hour, day, month, day_of_week)
            captured["timezone"] = timezone

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    monkeypatch.setattr(service_module, "CronTrigger", FakeCronTrigger)

    added = {}

    def fake_add_job(func, trigger, id, args, coalesce, max_instances, replace_existing, **kwargs):
        del kwargs
        added["trigger"] = trigger
        added["id"] = id

    monkeypatch.setattr(service.scheduler, "add_job", fake_add_job)

    service._register_library_job(
        service_module.RuntimeLibrary(
            id=99,
            name="Movies",
            path="/movies",
            schedule="0 2 * * 0",
            min_size_gb=0.0,
            max_files=1,
            priority=1,
            qsv_quality=None,
            qsv_preset=None,
            min_savings_percent=None,
        )
    )

    assert captured["expr"] == "0 2 * * sun"
    assert str(captured["timezone"]) == "UTC"
    assert isinstance(added["trigger"], FakeCronTrigger)
    assert added["id"] == "library-99-schedule"


def test_scheduler_registration_logs_timezone_and_next_run_for_named_weekday_schedule(monkeypatch, caplog):
    monkeypatch.setenv("TZ", "America/Chicago")
    captured = {}

    class FakeCronTrigger(object):
        def __init__(self, minute, hour, day, month, day_of_week, timezone=None):
            captured["expr"] = "%s %s %s %s %s" % (minute, hour, day, month, day_of_week)
            captured["timezone"] = timezone

        def __str__(self):
            return "cron[0 2 * * sun,tue,thu]"

    class FakeJob(object):
        next_run_time = datetime(2026, 3, 10, 2, 0, tzinfo=service_module.ZoneInfo("America/Chicago"))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    monkeypatch.setattr(service_module, "CronTrigger", FakeCronTrigger)
    monkeypatch.setattr(service.scheduler, "add_job", lambda *args, **kwargs: None)
    monkeypatch.setattr(service.scheduler, "get_job", lambda job_id: FakeJob())

    caplog.set_level("INFO")
    service._register_library_job(
        service_module.RuntimeLibrary(
            id=99,
            name="Movies",
            path="/movies",
            schedule="0 2 * * sun,tue,thu",
            min_size_gb=0.0,
            max_files=1,
            priority=1,
            qsv_quality=None,
            qsv_preset=None,
            min_savings_percent=None,
        )
    )

    assert captured["expr"] == "0 2 * * sun,tue,thu"
    assert captured["timezone"] == service_module._cron_trigger_timezone()
    assert "Registering Movies schedule: raw='0 2 * * sun,tue,thu'" in caplog.text
    assert "minute=0 hour=2 day='*' month='*' dow='sun,tue,thu'" in caplog.text
    assert "timezone='America/Chicago'" in caplog.text
    assert "job_id='library-99-schedule'" in caplog.text
    assert "next_run='2026-03-10 02:00:00-05:00'" in caplog.text


def test_home_page_route_returns_minimal_operator_page():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/")

    assert status_code == 200
    assert "Chonk Reducer" in body
    assert "Run Now" in body
    assert "Path:" in body
    assert "System Status" in body
    assert "Libraries" in body
    assert "Preview Results" in body


def test_home_page_shows_placeholder_when_no_runs_recorded(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/")

    assert status_code == 200
    assert "Last Run:</strong> Never" in body
    assert "System Status" in body
    assert "Preview Results" in body


def test_home_page_shows_latest_run_savings_values_for_movies_and_tv(tmp_path, monkeypatch):
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
    assert "Recent Savings:</strong> 1.0 GB across 0 files" in body
    assert "Recent Savings:</strong> 2.0 GB across 0 files" in body


def test_home_page_shows_zero_recent_savings_when_no_successful_runs(tmp_path, monkeypatch):
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
    assert body.count("Recent Savings:</strong> 0 B across 0 files") >= 2


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


def test_home_page_does_not_render_recent_runs_section(tmp_path, monkeypatch):
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
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/")

    assert status_code == 200
    assert "Recent Runs" not in body
    assert "Preview Results" in body


def test_home_page_no_recent_runs_section_even_with_empty_runs_table(tmp_path, monkeypatch):
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
    assert "Recent Runs" not in body
    assert "Preview Results" in body


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


def test_dashboard_run_library_redirects_immediately_after_queueing(monkeypatch):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    entered = threading.Event()
    release = threading.Event()

    def blocking_run_once(library, trigger):
        entered.set()
        release.wait(timeout=1)

    monkeypatch.setattr(service, "_run_library_once", blocking_run_once)

    start = time.monotonic()
    status_code, response = _call_post(service, "/dashboard/libraries/1/run", follow_redirects=False)
    elapsed = time.monotonic() - start

    assert status_code == 303
    assert response.headers["location"] == "/dashboard?manual_run=queued&library_id=1"
    assert elapsed < 0.5
    release.set()


def test_dashboard_run_library_redirects_promptly_when_busy(monkeypatch):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    monkeypatch.setattr(service, "_enqueue_library_job", lambda library, trigger: False)

    start = time.monotonic()
    status_code, response = _call_post(service, "/dashboard/libraries/1/run", follow_redirects=False)
    elapsed = time.monotonic() - start

    assert status_code == 303
    assert response.headers["location"] == "/dashboard?manual_run=busy&library_id=1"
    assert elapsed < 0.2


def test_dashboard_manual_run_redirect_lands_on_successful_dashboard_response(monkeypatch):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    monkeypatch.setattr(service, "_enqueue_library_job", lambda library, trigger: True)

    status_code, response = _call_post(service, "/dashboard/libraries/1/run", follow_redirects=False)
    dashboard_code, body, _ = _call_get(service, response.headers["location"])

    assert status_code == 303
    assert dashboard_code == 200
    assert "Run Now" in body


def test_dashboard_route_accepts_manual_run_status_query_params():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    status_code, body, _ = _call_get(service, "/dashboard?manual_run=queued&library_id=1")

    assert status_code == 200
    assert "Run Now" in body


def test_favicon_route_returns_no_content_promptly():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    started = time.monotonic()
    status_code, body, payload = _call_get(service, "/favicon.ico")
    elapsed = time.monotonic() - started

    assert status_code == 204
    assert body == ""
    assert payload is None
    assert elapsed < 1.0




def test_simple_http_server_handles_another_request_while_dashboard_request_is_blocked(monkeypatch):
    host = "127.0.0.1"
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((host, 0))
        port = int(probe.getsockname()[1])

    dashboard_entered = threading.Event()
    release_dashboard = threading.Event()

    def blocking_home_html():
        dashboard_entered.set()
        release_dashboard.wait(timeout=2)
        return "<html>dashboard</html>"

    server_thread = threading.Thread(
        target=service_module._run_simple_http_server,
        args=(
            host,
            port,
            lambda: {"status": "ok"},
            blocking_home_html,
            lambda: "<html>runs</html>",
            lambda: "<html>history</html>",
            lambda run_id: ("<html>%s</html>" % run_id, 200),
            lambda message="": "<html>settings</html>",
            lambda: "<html>activity</html>",
            lambda: "<html>system</html>",
            lambda: {"status": "Idle"},
            lambda updates: None,
            lambda updates: "saved",
            lambda payload: {"status": "ok", "message": "sent"},
            lambda values: "created",
            lambda values: "updated",
            lambda values: "deleted",
            lambda values: "toggled",
            lambda library, preview=False: ({"status": "queued", "library": library, "library_id": 1}, 202),
        ),
        daemon=True,
    )
    server_thread.start()

    # Wait for the server socket to accept connections.
    for _ in range(50):
        try:
            urllib.request.urlopen("http://%s:%d/health" % (host, port), timeout=0.1)
            break
        except Exception:
            time.sleep(0.02)

    def request_dashboard():
        urllib.request.urlopen("http://%s:%d/dashboard" % (host, port), timeout=2).read()

    dashboard_thread = threading.Thread(target=request_dashboard, daemon=True)
    dashboard_thread.start()

    assert dashboard_entered.wait(timeout=1)

    started = time.monotonic()
    with urllib.request.urlopen("http://%s:%d/favicon.ico" % (host, port), timeout=1) as response:
        body = response.read()
        status_code = response.getcode()
    elapsed = time.monotonic() - started

    release_dashboard.set()
    dashboard_thread.join(timeout=1)

    assert status_code == 204
    assert body == b""
    assert elapsed < 0.5


def test_simple_http_server_uses_threading_http_server(monkeypatch):
    captured = {}

    class FakeThreadingHTTPServer:
        def __init__(self, server_address, handler):
            captured["server_address"] = server_address
            captured["handler"] = handler

        def serve_forever(self):
            captured["served"] = True

    monkeypatch.setattr(service_module, "ThreadingHTTPServer", FakeThreadingHTTPServer)

    service_module._run_simple_http_server(
        "127.0.0.1",
        18080,
        lambda: {"status": "ok"},
        lambda: "<html>home</html>",
        lambda: "<html>runs</html>",
        lambda: "<html>history</html>",
        lambda run_id: ("<html>%s</html>" % run_id, 200),
        lambda message="": "<html>settings</html>",
        lambda: "<html>activity</html>",
        lambda: "<html>system</html>",
        lambda: {"status": "Idle"},
        lambda updates: None,
        lambda updates: "saved",
        lambda payload: {"status": "ok", "message": "sent"},
        lambda values: "created",
        lambda values: "updated",
        lambda values: "deleted",
        lambda values: "toggled",
        lambda library, preview=False: ({"status": "queued", "library": library, "library_id": 1}, 202),
    )

    assert captured["server_address"] == ("127.0.0.1", 18080)
    assert captured.get("served") is True

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
    assert "housekeeping-daily" in jobs
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

    calls = []
    gate = threading.Event()

    def fake_run_once(library, trigger):
        calls.append((library, trigger))
        gate.wait(timeout=1)

    monkeypatch.setattr(service, "_run_library_once", fake_run_once)

    started = service.trigger_library("movies")
    rejected = service.trigger_library("movies")
    for _ in range(50):
        if calls:
            break
        threading.Event().wait(0.01)
    gate.set()

    assert started is True
    assert rejected is False
    assert calls == [("movies", "schedule")]


def test_trigger_library_starts_scheduled_run(monkeypatch):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    calls = []
    done = threading.Event()

    def fake_run_once(library, trigger):
        calls.append((library, trigger))
        done.set()

    monkeypatch.setattr(service, "_run_library_once", fake_run_once)

    started = service.trigger_library("movies")

    assert started is True
    assert done.wait(timeout=1)
    assert calls == [("movies", "schedule")]


def test_runtime_status_reflects_running_and_idle(monkeypatch):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    entered = threading.Event()
    release = threading.Event()

    def fake_run_once(library, trigger):
        assert library == "movies"
        assert trigger == "manual"
        entered.set()
        release.wait(timeout=1)

    monkeypatch.setattr(service, "_run_library_once", fake_run_once)

    payload, status_code = service.manual_run_payload("movies")
    assert status_code == 202
    assert payload["status"] == "queued"
    assert entered.wait(timeout=1)

    running = service._runtime_status_snapshot()
    assert running["status"] == "Running"
    assert running["current_library"] == "Movies"
    assert running["current_trigger"] == "manual"

    release.set()
    for _ in range(20):
        idle = service._runtime_status_snapshot()
        if idle["status"] == "Idle":
            break
        threading.Event().wait(0.01)
    assert idle["status"] == "Idle"


def test_api_status_returns_valid_json_payload():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    status_code, body, payload = _call_get(service, "/api/status")
    effective = payload
    if effective is None and body is not None:
        try:
            effective = json.loads(body)
        except Exception:
            effective = body

    assert status_code == 200
    assert isinstance(effective, dict)
    expected = {
        "status",
        "current_library",
        "trigger",
        "queue_depth",
        "run_id",
        "started_at",
        "current_file",
        "candidates_found",
        "files_evaluated",
        "files_processed",
        "files_skipped",
        "files_failed",
        "bytes_saved",
        "encode_percent",
        "encode_speed",
        "encode_eta",
        "encode_out_time",
    }
    assert expected.issubset(set(effective.keys()))
    assert effective["version"]
    assert isinstance(effective["scheduler"], dict)


def test_api_status_includes_scheduler_health_snapshot():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    class _Job:
        def __init__(self, next_run_time):
            self.next_run_time = next_run_time

    class _Scheduler:
        state = 1

        @staticmethod
        def get_jobs():
            return [_Job("2026-03-12T02:00:00-05:00"), _Job("2026-03-12T03:00:00-05:00")]

    service.scheduler = _Scheduler()

    status_code, body, payload = _call_get(service, "/api/status")
    effective = payload if isinstance(payload, dict) else (body if isinstance(body, dict) else json.loads(body))

    assert status_code == 200
    assert effective["scheduler"]["running"] is True
    assert effective["scheduler"]["paused"] is False
    assert effective["scheduler"]["next_run"] == "2026-03-12T02:00:00-05:00"


def test_api_status_returns_current_runtime_snapshot_data():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    with service._job_condition:
        service._current_job = service_module.RuntimeJob(library_id=1, library_name="TV", trigger="manual", priority=100)
        service._current_job_started_at = "2026-01-05T00:00:00Z"
        service._current_job_run_id = "run-abc"
        service._current_run_snapshot = {
            "current_file": "episode.mkv",
            "candidates_found": "1",
            "files_evaluated": "1",
            "files_processed": "0",
            "files_skipped": "0",
            "files_failed": "0",
            "bytes_saved": "0",
            "encode_percent": "62.5",
            "encode_speed": "3.2x",
            "encode_eta": "102",
            "encode_out_time": "12345678",
        }

    status_code, body, payload = _call_get(service, "/api/status")
    effective = payload if isinstance(payload, dict) else (body if isinstance(body, dict) else json.loads(body))

    assert status_code == 200
    assert effective["status"] == "Running"
    assert effective["current_library"] == "TV"
    assert effective["trigger"] == "manual"
    assert effective["current_file"] == "episode.mkv"
    assert effective["files_evaluated"] == "1"
    assert effective["candidates_found"] == "1"
    assert effective["encode_percent"] == "62.5"
    assert effective["encode_speed"] == "3.2x"
    assert effective["encode_eta"] == "102"
    assert effective["encode_out_time"] == "12345678"


def test_api_status_endpoint_remains_responsive_while_run_active(monkeypatch):
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    started = threading.Event()
    progress_reached = threading.Event()
    release = threading.Event()

    def fake_run_once(library, trigger):
        assert library == "movies"
        assert trigger == "manual"
        started.set()
        service._update_runtime_progress({"current_file": "movie.mkv", "files_processed": 1, "candidates_found": 3})
        progress_reached.set()
        release.wait(timeout=2)

    monkeypatch.setattr(service, "_run_library_once", fake_run_once)

    payload, status_code = service.manual_run_payload("movies")
    assert status_code == 202
    assert payload["status"] == "queued"
    assert started.wait(timeout=1)
    assert progress_reached.wait(timeout=1)

    result = {}

    def call_status():
        result["status_code"], result["body"], result["payload"] = _call_get(service, "/api/status")

    request_thread = threading.Thread(target=call_status)
    request_thread.start()
    request_thread.join(timeout=0.75)

    release.set()
    for _ in range(20):
        if service.current_job_status()["status"] == "Idle":
            break
        threading.Event().wait(0.01)

    assert not request_thread.is_alive()
    assert result["status_code"] == 200
    effective = result["payload"] if isinstance(result["payload"], dict) else (result["body"] if isinstance(result["body"], dict) else json.loads(result["body"]))
    assert effective["status"] == "Running"
    assert effective["current_file"] == "movie.mkv"
    assert effective["files_processed"] == "1"


def test_dashboard_and_system_show_runtime_status():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    dashboard_code, dashboard_body, _ = _call_get(service, "/dashboard")
    system_code, system_body, _ = _call_get(service, "/system")

    assert dashboard_code == 200
    assert "Status</th><td" in dashboard_body
    assert "Idle" in dashboard_body
    assert "Queue Depth" in dashboard_body
    assert "Current File" in dashboard_body
    assert "Candidates Found" in dashboard_body
    assert "id=\"runtime-progress-section\"></div>" in dashboard_body

    assert system_code == 200
    assert "Current Job Status" not in system_body
    assert "Scheduler Status</th><td" in system_body
    assert "Queue Depth" in system_body


def test_dashboard_includes_live_status_polling_script():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    status_code, body, _ = _call_get(service, "/dashboard")

    assert status_code == 200
    assert 'fetch("/api/status"' in body
    assert "window.setInterval(fetchStatus, 3000);" in body


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
    assert "Status:</strong> Idle" in body
    assert "Files Optimized:</strong> 0" in body
    assert "Total Saved:</strong> 0 B" in body




def test_dashboard_library_card_shows_lifetime_totals_from_successful_encodes(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    _seed_encode(
        db_path,
        run_id="r1",
        ts="2026-01-04T08:10:00",
        status="success",
        path="/movies/a.mkv",
        saved_bytes=500 * 1024 * 1024,
        library="movies",
    )
    _seed_encode(
        db_path,
        run_id="r2",
        ts="2026-01-04T08:12:00",
        status="success",
        path="/movies/b.mkv",
        saved_bytes=2 * 1024 * 1024 * 1024,
        library="movies",
    )
    _seed_encode(
        db_path,
        run_id="r3",
        ts="2026-01-04T08:13:00",
        status="failed",
        path="/movies/c.mkv",
        saved_bytes=9 * 1024 * 1024 * 1024,
        library="movies",
    )
    _seed_encode(
        db_path,
        run_id="r4",
        ts="2026-01-04T08:14:00",
        status="success",
        path="/tv_shows/a.mkv",
        saved_bytes=4 * 1024 * 1024 * 1024,
        library="tv",
    )

    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/dashboard")

    assert status_code == 200
    assert "Files Optimized:</strong> 2" in body
    assert "Total Saved:</strong> 2.5 GB" in body
    assert "Files Optimized:</strong> 1" in body
    assert "Total Saved:</strong> 4.0 GB" in body


def test_dashboard_library_card_shows_zero_lifetime_totals_when_no_successful_encodes(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    _seed_encode(
        db_path,
        run_id="r5",
        ts="2026-01-04T09:00:00",
        status="failed",
        path="/movies/d.mkv",
        saved_bytes=1024,
        library="movies",
    )
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/dashboard")

    assert status_code == 200
    assert body.count("Files Optimized:</strong> 0") == 2
    assert body.count("Total Saved:</strong> 0 B") == 2

def test_dashboard_library_card_displays_runtime_statuses(monkeypatch):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    _, idle_body, _ = _call_get(service, "/dashboard")
    assert idle_body.count("Status:</strong> Idle") >= 2

    with service._job_condition:
        service._job_queue = service_module.deque(
            [service_module.RuntimeJob(library_id=1, library_name="Movies", trigger="schedule", priority=100)]
        )

    _, queued_body, _ = _call_get(service, "/dashboard")
    assert "Status:</strong> Queued" in queued_body

    with service._job_condition:
        service._current_job = service_module.RuntimeJob(library_id=2, library_name="TV", trigger="manual", priority=100)
        service._current_job_started_at = "2026-01-05T00:00:00"
        service._current_run_snapshot = {"files_processed": "8", "candidates_found": "20"}
        service._job_queue = service_module.deque()

    _, running_body, _ = _call_get(service, "/dashboard")
    assert "Status:</strong> Running" in running_body
    assert "Progress:</strong> 8 / 20 files" in running_body

def test_display_version_avoids_double_prefix():
    assert service_module._display_version("v1.42.0") == "v1.42.0"
    assert service_module._display_version("1.42.0") == "v1.42.0"


def test_shell_header_version_does_not_double_prefix(monkeypatch):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    monkeypatch.setattr(service_module, "APP_VERSION", "v1.42.0")

    status_code, body, _ = _call_get(service, "/dashboard")

    assert status_code == 200
    assert "Chonk Reducer v1.42.0" in body
    assert "Chonk Reducer vv1.42.0" not in body


def test_dashboard_system_status_shows_library_and_housekeeping_runs(monkeypatch):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    monkeypatch.setattr(service, "_next_global_scheduled_job_label", lambda: "TV — 2:00 AM")
    monkeypatch.setattr(service, "_next_housekeeping_run_label", lambda: "2026-03-14 03:00")

    status_code, body, _ = _call_get(service, "/dashboard")

    assert status_code == 200
    assert "System Status" in body
    assert 'Next Library Run:</strong> <span id="runtime-system-next-library-run">TV — 2:00 AM</span>' in body
    assert 'Next Housekeeping Run:</strong> <span id="runtime-system-next-housekeeping-run">2026-03-14 03:00</span>' in body
    assert "runtime-system-next-run" not in body
    assert body.index("System Status") < body.index("Libraries")
    assert body.index("Libraries") < body.index("Current Job Status")
    assert body.index("Current Job Status") < body.index("Preview Results")


def test_dashboard_library_card_shows_manual_only_for_blank_schedule():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    status_code, body, _ = _call_get(service, "/dashboard")

    assert status_code == 200
    assert body.count("Next Run:</strong> Not Scheduled") == 2
    assert "Current Job Status" in body
    assert "Status</th><td" in body and "Idle" in body


def test_dashboard_library_card_shows_not_scheduled_for_invalid_schedule(monkeypatch):
    monkeypatch.setenv("MOVIE_SCHEDULE", "invalid schedule")
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    class _NoRunJob:
        id = "library-1-schedule"
        next_run_time = None

    monkeypatch.setattr(service.scheduler, "get_job", lambda _job_id: _NoRunJob())

    status_code, body, _ = _call_get(service, "/dashboard")

    assert status_code == 200
    assert "Next Run:</strong> Not Scheduled" in body


def test_dashboard_library_card_shows_scheduler_next_run_time(monkeypatch):
    monkeypatch.setenv("MOVIE_SCHEDULE", "0 1 * * *")
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    class _NextRunJob:
        id = "library-1-schedule"
        next_run_time = "2026-01-08 01:00:00 UTC"

    monkeypatch.setattr(service.scheduler, "get_job", lambda _job_id: _NextRunJob())

    status_code, body, _ = _call_get(service, "/dashboard")

    assert status_code == 200
    assert "Next Run:</strong> 2026-01-08 01:00" in body


def test_dashboard_current_job_status_shows_scheduler_running_and_start_time():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    class _Scheduler:
        running = True

        def get_jobs(self):
            return []

    service.scheduler = _Scheduler()
    service._scheduler_started_at = "2026-03-08 20:27"

    status_code, body, _ = _call_get(service, "/dashboard")

    assert status_code == 200
    assert "Scheduler Status</th><td id=\"runtime-scheduler-status\"" in body
    assert "Running" in body
    assert "Scheduler Started</th><td id=\"runtime-scheduler-started\"" in body
    assert "2026-03-08 20:27" in body


def test_current_job_status_uses_earliest_enabled_library_next_run_for_global_schedule(monkeypatch):
    monkeypatch.setenv("MOVIE_SCHEDULE", "0 2 * * *")
    monkeypatch.setenv("TV_SCHEDULE", "0 4 * * *")
    monkeypatch.setenv("TZ", "UTC")

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    def _fake_next_run(schedule, now=None):
        del now
        mapping = {
            "0 2 * * *": datetime(2026, 3, 9, 2, 0, 0),
            "0 4 * * *": datetime(2026, 3, 9, 4, 0, 0),
        }
        return mapping.get(schedule)

    monkeypatch.setattr(service_module, "_next_run_from_cron", _fake_next_run)

    class _Scheduler:
        def get_jobs(self):
            return []

    service.scheduler = _Scheduler()
    service._scheduler_started_at = "2026-03-09T00:59:00"

    snapshot = service.current_job_status()

    assert snapshot["scheduler_status"] == "Running"
    assert snapshot["next_scheduled_job"] == "Movies"
    assert snapshot["next_scheduled_time"] == "2026-03-09 02:00"


def test_current_job_status_uses_dash_when_no_enabled_libraries_have_schedules(monkeypatch):
    monkeypatch.delenv("MOVIE_SCHEDULE", raising=False)
    monkeypatch.delenv("TV_SCHEDULE", raising=False)
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    movies, tv = service.list_libraries()
    movies_no_schedule = service_module.LibraryRecord(
        id=movies.id,
        name=movies.name,
        path=movies.path,
        enabled=True,
        schedule="",
        min_size_gb=movies.min_size_gb,
        max_files=movies.max_files,
        priority=movies.priority,
        qsv_quality=movies.qsv_quality,
        qsv_preset=movies.qsv_preset,
        min_savings_percent=movies.min_savings_percent,
    )
    disabled_tv = service_module.LibraryRecord(
        id=tv.id,
        name=tv.name,
        path=tv.path,
        enabled=False,
        schedule="0 5 * * *",
        min_size_gb=tv.min_size_gb,
        max_files=tv.max_files,
        priority=tv.priority,
        qsv_quality=tv.qsv_quality,
        qsv_preset=tv.qsv_preset,
        min_savings_percent=tv.min_savings_percent,
    )
    monkeypatch.setattr(service, "list_libraries", lambda: [movies_no_schedule, disabled_tv])

    snapshot = service.current_job_status()

    assert snapshot["next_scheduled_job"] == "-"
    assert snapshot["next_scheduled_time"] == "-"


def test_scheduler_status_reports_stopped_after_explicit_shutdown_state():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    service._scheduler_started_at = "2026-03-08 20:27"
    service._scheduler_stopped = True

    snapshot = service.current_job_status()

    assert snapshot["scheduler_status"] == "Stopped"


def test_dashboard_renders_scheduler_placeholders_when_no_scheduled_jobs():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    class _Scheduler:
        running = False

        def get_jobs(self):
            return []

    service.scheduler = _Scheduler()

    status_code, body, _ = _call_get(service, "/dashboard")

    assert status_code == 200
    assert "Scheduler Status" in body
    assert "Stopped" in body
    assert "Next Scheduled Job" in body
    assert "Next Scheduled Time" in body




def test_dashboard_library_card_shows_computed_next_run_for_valid_schedule(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    monkeypatch.setenv("TZ", "UTC")
    monkeypatch.setenv("MOVIE_SCHEDULE", "0 2 * * 6")
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    monkeypatch.setattr(service.scheduler, "get_job", lambda _job_id: None)

    fixed_now = datetime(2026, 3, 9, 1, 0)
    original_next = service_module._next_run_from_cron

    def _patched_next(schedule: str, now=None):
        return original_next(schedule, now=fixed_now)

    monkeypatch.setattr(service_module, "_next_run_from_cron", _patched_next)

    status_code, body, _ = _call_get(service, "/dashboard")

    assert status_code == 200
    assert "Movies" in body
    assert "Next Run:</strong> 2026-03-14 02:00" in body


def test_simple_schedule_builder_values_reflected_in_next_run_display(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    monkeypatch.setenv("TZ", "UTC")
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    status_code, body = _call_post(
        service,
        "/settings/libraries/update",
        data={
            "library_id": "1",
            "name": "Movies",
            "path": "/movies",
            "enabled": "1",
            "schedule_mode": "simple",
            "schedule_time": "02:00",
            "schedule_day_sat": "1",
            "min_size_gb": "0",
            "max_files": "1",
            "priority": "100",
            "qsv_quality": "21",
            "qsv_preset": "7",
            "min_savings_percent": "15",
        },
    )
    assert status_code == 200
    assert "Library updated." in body

    monkeypatch.setattr(service.scheduler, "get_job", lambda _job_id: None)

    fixed_now = datetime(2026, 3, 9, 1, 0)
    original_next = service_module._next_run_from_cron

    def _patched_next(schedule: str, now=None):
        return original_next(schedule, now=fixed_now)

    monkeypatch.setattr(service_module, "_next_run_from_cron", _patched_next)

    status_code, dashboard_body, _ = _call_get(service, "/dashboard")

    assert status_code == 200
    assert "Next Run:</strong> 2026-03-14 02:00" in dashboard_body


def test_dashboard_runtime_status_shows_current_file_and_live_snapshot():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    with service._job_condition:
        service._current_job = service_module.RuntimeJob(library_id=1, library_name="Movies", trigger="manual", priority=100)
        service._current_job_started_at = "2026-01-05T00:00:00Z"
        service._current_job_run_id = "run-123"
        service._current_run_snapshot = {
            "current_file": "/movies/Example.mkv",
            "candidates_found": "10",
            "evaluated_count": "6",
            "processed_count": "4",
            "success_count": "3",
            "skipped_count": "1",
            "failed_count": "0",
            "bytes_saved": str(5 * 1024 * 1024),
        }

    status_code, body, _ = _call_get(service, "/dashboard")

    assert status_code == 200
    assert "Current Library</th><td" in body and "Movies" in body
    assert "Current File</th><td" in body and "/movies/Example.mkv" in body
    assert "Candidates Found</th><td" in body and ">10<" in body
    assert "Files Evaluated</th><td" in body and ">6<" in body
    assert "Files Processed</th><td" in body and ">4<" in body
    assert "Files Skipped</th><td" in body and ">1<" in body
    assert "Files Failed</th><td" in body and ">0<" in body
    assert "Bytes Saved So Far</th><td" in body and "5.0 MB" in body

def test_dashboard_progress_bar_renders_for_active_run():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    with service._job_condition:
        service._current_job = service_module.RuntimeJob(library_id=1, library_name="Movies", trigger="manual", priority=100)
        service._current_run_snapshot = {
            "current_file": "/movies/Example.mkv",
            "candidates_found": "20",
            "files_evaluated": "12",
            "files_processed": "8",
            "files_skipped": "3",
            "files_failed": "0",
            "bytes_saved": str(4 * 1024 * 1024 * 1024),
            "encode_percent": "62.0",
            "encode_speed": "3.2x",
            "encode_eta": "102",
        }

    status_code, body, _ = _call_get(service, "/dashboard")

    assert status_code == 200
    assert "Run Progress" in body
    assert "8 / 20 files processed" in body
    assert "Current Library:</strong> Movies" in body
    assert "Current File:</strong> /movies/Example.mkv" in body
    assert "Files Processed:</strong> 8" in body
    assert "Files Skipped:</strong> 3" in body
    assert "Files Failed:</strong> 0" in body
    assert "Total Saved:</strong> 4.0 GB" in body
    assert "Percent Complete:</strong> 62%" in body
    assert "Speed:</strong> 3.2x" in body
    assert "ETA:</strong> 1m 42s" in body


def test_dashboard_progress_not_complete_during_active_encode():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    with service._job_condition:
        service._current_job = service_module.RuntimeJob(library_id=1, library_name="Movies", trigger="manual", priority=100)
        service._current_run_snapshot = {
            "current_file": "/movies/Example.mkv",
            "candidates_found": "1",
            "files_evaluated": "1",
            "files_processed": "0",
            "files_skipped": "0",
            "files_failed": "0",
            "bytes_saved": "0",
        }

    status_code, body, _ = _call_get(service, "/dashboard")

    assert status_code == 200
    assert "0 / 1 files processed" in body
    assert "(0%)" in body
    assert "(100%)" not in body


def test_dashboard_progress_renders_without_candidates_total():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    with service._job_condition:
        service._current_job = service_module.RuntimeJob(library_id=1, library_name="Movies", trigger="manual", priority=100)
        service._current_run_snapshot = {
            "files_processed": "3",
        }

    status_code, body, _ = _call_get(service, "/dashboard")

    assert status_code == 200
    assert "3 files processed" in body


def test_runtime_progress_snapshot_resets_after_job_completion(monkeypatch):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    entered = threading.Event()
    release = threading.Event()

    def fake_run_once(library, trigger):
        service._update_runtime_progress({
            "current_file": "/movies/Example.mkv",
            "candidates_found": 10,
            "files_processed": 4,
            "files_skipped": 1,
            "files_failed": 0,
            "bytes_saved": 1024,
        })
        entered.set()
        release.wait(timeout=1)

    monkeypatch.setattr(service, "_run_library_once", fake_run_once)

    payload, status_code = service.manual_run_payload("movies")
    assert status_code == 202
    assert payload["status"] == "queued"
    assert entered.wait(timeout=1)

    during = service.current_job_status()
    assert during["status"] == "Running"
    assert during["current_file"] == "/movies/Example.mkv"
    assert during["files_processed"] == "4"

    release.set()
    for _ in range(20):
        done = service.current_job_status()
        if done["status"] == "Idle":
            break
        threading.Event().wait(0.01)

    assert done["status"] == "Idle"
    assert done["current_file"] == ""
    assert done["files_processed"] == ""


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
    service._scheduler_started_at = "2026-01-08T00:00:00"

    status_code, body, _ = _call_get(service, "/system")

    assert status_code == 200
    assert "Service / Scheduler Summary" in body
    assert "Runtime / Storage Information" in body
    assert "Current Job Status" not in body
    assert "App Version</th>" in body
    assert "Service Mode</th><td" in body and "Enabled" in body
    assert "Scheduler Status</th><td" in body and "Running" in body
    assert "Next Scheduled Job</th><td" in body and "Movies" in body
    assert "Next Scheduled Time</th><td" in body and "2026-01-08 02:00" in body
    assert "Housekeeping Enabled" in body
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
    assert "Next Scheduled Job</th><td" in body and ">-<" in body
    assert "Next Scheduled Time</th><td" in body and ">-<" in body
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
    assert "Completed and recorded runs from SQLite" in body
    for heading in (
        "Time",
        "Library",
        "Mode",
        "Result",
        "Processed",
        "Skipped",
        "Failed",
        "Saved",
        "Duration",
        "Run ID",
    ):
        assert ">%s<" % heading in body
    assert "<td>movies</td>" in body
    assert "<td>Live</td>" in body
    assert "<td>success</td>" in body
    assert "<td>3</td>" in body
    assert "<td>1</td>" in body
    assert "<td>0</td>" in body
    assert "<td>0.0 MB</td>" in body
    assert "<td>12s</td>" in body
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
        duration_seconds=201,
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
    assert "Outcome" in body
    assert "Counts" in body
    assert "Savings" in body
    assert "Related Information" in body
    assert "Run ID</th><td" in body and run_id in body
    assert "Trigger Type</th><td" in body and "Unknown" in body
    assert "Mode</th><td" in body and "Live" in body
    assert "Result</th><td" in body and "failed" in body
    assert "Duration</th><td" in body and "15s" in body
    assert "Total Saved</th><td" in body and "0.0 MB" in body
    assert "Avg Saved / File</th><td" in body and "0.0 MB" in body
    assert "Run Log Path" in body
    assert "No raw log path recorded for this run" in body
    assert "File-Level Entries" in body
    assert "File List Summary" in body
    assert "/movies/A.mkv" in body
    assert "/movies/B.mkv" in body
    assert "/movies/C.mkv" in body
    assert "min_savings: below threshold" in body
    assert "encode_error: ffmpeg failed" in body
    assert "Encode Time" in body and "3m 21s" in body


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
    assert "Run Log Path" in body
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



def test_run_detail_page_labels_preview_mode_and_manual_preview_trigger(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    run_id = "movies-2026-01-02T08:00:00"
    _seed_run(db_path, library="movies", ts_end="2026-01-02T08:00:00", mode="preview")
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    service._record_activity("manual_preview_requested", "Manual preview requested for movies", library="movies", run_id=run_id)

    status_code, body, _ = _call_get(service, "/runs/%s" % run_id)

    assert status_code == 200
    assert "Trigger Type</th><td" in body and "Manual Preview" in body
    assert "Mode</th><td" in body and "Preview" in body
    assert "Preview vs Encode Result" in body and "Preview-only (no files encoded)" in body


def test_run_detail_page_labels_cancelled_runs(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    run_id = "tv-2026-01-02T08:00:00"
    _seed_run(db_path, library="tv", ts_end="2026-01-02T08:00:00", skipped_count=1)
    _seed_encode(
        db_path,
        run_id=run_id,
        ts="2026-01-02T08:00:01",
        status="skipped",
        path="/tv/A.mkv",
        skip_reason="cancelled",
    )
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    status_code, body, _ = _call_get(service, "/runs/%s" % run_id)

    assert status_code == 200
    assert "Result</th><td" in body and "cancelled" in body
    assert "Cancellation</th><td" in body and "Cancelled" in body


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


def test_runs_page_shows_active_run_banner_when_run_is_live(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    _seed_run(db_path, library="movies", ts_end="2026-01-02T08:00:00")
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    with service._job_condition:
        service._current_job = RuntimeJob(library_id=1, library_name="TV", trigger="manual", priority=1)
        service._current_run_snapshot = {"current_file": "/tv/True Detective S02E01.mkv"}

    status_code, body, _ = _call_get(service, "/runs")

    assert status_code == 200
    assert "Active Run:</strong> TV — True Detective S02E01" in body
    assert 'href="/dashboard"' in body
    assert "See Dashboard for live progress." in body


def test_runs_page_hides_active_run_banner_when_no_live_run(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    _seed_run(db_path, library="movies", ts_end="2026-01-02T08:00:00")
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    status_code, body, _ = _call_get(service, "/runs")

    assert status_code == 200
    assert "Active Run:</strong>" not in body


@pytest.mark.parametrize(
    ("saved_bytes", "expected_saved"),
    [
        (500, "<td>0.0 MB</td>"),
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




def test_run_duration_formatter_supports_seconds_minutes_and_hours():
    assert service_module._format_duration_seconds(12) == "12s"
    assert service_module._format_duration_seconds(222) == "3m 42s"
    assert service_module._format_duration_seconds(3900) == "1h 05m"


def test_run_duration_prefers_started_and_completed_timestamps():
    seconds = service_module._duration_seconds_from_run("2026-01-02T08:00:00", "2026-01-02T08:00:12", 99)
    assert seconds == 12.0


def test_run_detail_uses_encode_derived_savings_and_average(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    run_id = "movies-2026-01-02T08:00:00"
    _seed_run(db_path, library="movies", ts_end="2026-01-02T08:00:00", success_count=2, saved_bytes=1)
    _seed_encode(db_path, run_id=run_id, ts="2026-01-02T08:00:01", status="success", path="/movies/A.mkv", size_before_bytes=4 * 1024 * 1024, size_after_bytes=2 * 1024 * 1024)
    _seed_encode(db_path, run_id=run_id, ts="2026-01-02T08:00:02", status="success", path="/movies/B.mkv", size_before_bytes=3 * 1024 * 1024, size_after_bytes=2 * 1024 * 1024)
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    status_code, body, _ = _call_get(service, "/runs/%s" % run_id)

    assert status_code == 200
    assert "Total Saved</th><td" in body and "3.0 MB" in body
    assert "Avg Saved / File</th><td" in body and "1.5 MB" in body


def test_run_detail_zero_encode_average_saved_defaults_to_zero(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    run_id = "movies-2026-01-02T08:00:00"
    _seed_run(db_path, library="movies", ts_end="2026-01-02T08:00:00", success_count=0, saved_bytes=0)
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    status_code, body, _ = _call_get(service, "/runs/%s" % run_id)

    assert status_code == 200
    assert "Avg Saved / File</th><td" in body and "0.0 MB" in body
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


def test_scheduled_run_records_activity_and_run_detail_trigger(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    _seed_run(db_path, library="movies", ts_end="2026-03-09T02:00:00", success_count=1)

    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    run_id = "movies-2026-03-09T02:00:00"
    service._record_activity(
        event_type="scheduled_run_requested",
        message="Scheduled run requested for Movies",
        library="Movies",
        run_id=run_id,
    )

    history = service._run_history(limit=1)
    assert len(history) == 1
    assert history[0]["run_id"] == run_id
    run_detail = service._run_detail(run_id)
    assert run_detail is not None
    assert run_detail["trigger_type"] == "scheduled_run_requested"

    event_types = [row["event_type"] for row in _read_activity_rows(db_path)]
    assert "scheduled_run_requested" in event_types


def test_manual_run_records_requested_and_busy_activity(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    hold = threading.Event()

    def blocking_run_once(library, trigger):
        assert library == "movies"
        assert trigger == "manual"
        hold.wait(timeout=1)

    monkeypatch.setattr(service, "_run_library_once", blocking_run_once)
    first_payload, first_status_code = service.manual_run_payload("movies")
    payload, status_code = service.manual_run_payload("movies")
    hold.set()

    assert first_status_code == 202
    assert first_payload == {"status": "queued", "library": "movies", "library_id": 1}
    assert status_code == 409
    assert payload == {"status": "busy", "library": "movies", "library_id": 1}
    event_types = [row["event_type"] for row in _read_activity_rows(db_path)]
    assert "manual_run_requested" in event_types
    assert "run_rejected_busy" in event_types


def test_scheduled_busy_rejection_records_activity(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    hold = threading.Event()

    def blocking_run_once(library, trigger):
        assert library == "movies"
        assert trigger == "schedule"
        hold.wait(timeout=1)

    monkeypatch.setattr(service, "_run_library_once", blocking_run_once)
    assert service.trigger_library("movies") is True
    started = service.trigger_library("movies")
    hold.set()

    assert started is False
    event_types = [row["event_type"] for row in _read_activity_rows(db_path)]
    assert "scheduled_run_requested" in event_types
    assert "run_rejected_busy" in event_types


def test_manual_run_records_queue_activity(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    monkeypatch.setattr(service, "_run_library_once", lambda library, trigger: None)

    payload, status_code = service.manual_run_payload("movies")

    assert status_code == 202
    assert payload["status"] == "queued"
    event_types = [row["event_type"] for row in _read_activity_rows(db_path)]
    assert "manual_run_requested" in event_types
    assert "job_queued" in event_types


def test_trigger_library_records_queue_activity(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    monkeypatch.setattr(service, "_run_library_once", lambda library, trigger: None)

    started = service.trigger_library("movies")

    assert started is True
    event_types = [row["event_type"] for row in _read_activity_rows(db_path)]
    assert "scheduled_run_requested" in event_types
    assert "job_queued" in event_types


def test_run_start_and_completion_recorded(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    monkeypatch.setattr(service_module, "run", lambda: 0)

    service._run_library_once("movies", "manual")

    event_types = [row["event_type"] for row in _read_activity_rows(db_path)]
    assert "run_started" in event_types
    assert "run_completed" in event_types




def test_service_initializes_worker_thread_without_attribute_error():
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    assert hasattr(service, "_worker_loop")
    assert isinstance(service._worker_thread, threading.Thread)

    with service._job_condition:
        service._worker_shutdown = True
        service._job_condition.notify_all()
    service._worker_thread.join(timeout=1)

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


def test_dashboard_route_remains_responsive_while_run_active(monkeypatch):
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    started = threading.Event()
    release = threading.Event()

    def blocking_run(progress_callback=None):
        del progress_callback
        started.set()
        release.wait(timeout=2)
        return 0

    monkeypatch.setattr(service_module, "run", blocking_run)

    payload, status_code = service.manual_run_payload("movies")
    assert status_code == 202
    assert payload["status"] == "queued"
    assert started.wait(timeout=1)

    result = {}

    def call_dashboard():
        result["status_code"], result["body"], _ = _call_get(service, "/dashboard")

    request_thread = threading.Thread(target=call_dashboard)
    request_thread.start()
    request_thread.join(timeout=0.75)

    release.set()

    assert not request_thread.is_alive()
    assert result["status_code"] == 200
    assert "<h1>Dashboard</h1>" in result["body"]


def test_runs_route_remains_responsive_while_run_active(monkeypatch):
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    started = threading.Event()
    progress_reached = threading.Event()
    release = threading.Event()

    def blocking_run(progress_callback=None):
        started.set()
        if progress_callback is not None:
            progress_callback({"current_file": "movie.mkv", "files_processed": 1, "candidates_found": 3})
            progress_reached.set()
        release.wait(timeout=2)
        return 0

    monkeypatch.setattr(service_module, "run", blocking_run)

    payload, status_code = service.manual_run_payload("movies")
    assert status_code == 202
    assert payload["status"] == "queued"
    assert started.wait(timeout=1)
    assert progress_reached.wait(timeout=1)

    result = {}

    def call_runs():
        result["status_code"], result["body"], _ = _call_get(service, "/runs")

    request_thread = threading.Thread(target=call_runs)
    request_thread.start()
    request_thread.join(timeout=0.75)

    snapshot = service._runtime_status_snapshot()
    release.set()

    assert not request_thread.is_alive()
    assert result["status_code"] == 200
    assert "<h1>Runs</h1>" in result["body"]
    assert snapshot["status"] == "Running"
    assert snapshot["files_processed"] == "1"
    assert snapshot["candidates_found"] == "3"


def test_runtime_snapshot_and_dashboard_match_during_active_encode(monkeypatch):
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    started = threading.Event()
    progress_reached = threading.Event()
    release = threading.Event()

    def blocking_run(progress_callback=None):
        started.set()
        if progress_callback is not None:
            progress_callback({
                "current_file": "movie.mkv",
                "candidates_found": 1,
                "files_evaluated": 1,
                "files_processed": 0,
            })
            progress_reached.set()
        release.wait(timeout=2)
        return 0

    monkeypatch.setattr(service_module, "run", blocking_run)

    payload, status_code = service.manual_run_payload("movies")
    assert status_code == 202
    assert payload["status"] == "queued"
    assert started.wait(timeout=1)
    assert progress_reached.wait(timeout=1)

    snapshot = service._runtime_status_snapshot()
    dashboard_status, dashboard_body, _ = _call_get(service, "/dashboard")
    release.set()

    assert snapshot["status"] == "Running"
    assert snapshot["current_file"] == "movie.mkv"
    assert snapshot["files_processed"] == "0"
    assert snapshot["candidates_found"] == "1"
    assert dashboard_status == 200
    assert "0 / 1 files processed" in dashboard_body
    assert "Current File:</strong> movie.mkv" in dashboard_body


def test_update_runtime_progress_tracks_encode_fields():
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    service._update_runtime_progress({"encode_percent": "55.5", "encode_speed": "2.1x", "encode_eta": "45", "encode_out_time": "1234"})
    snapshot = service._runtime_status_snapshot()

    assert snapshot["encode_percent"] == "55.5"
    assert snapshot["encode_speed"] == "2.1x"
    assert snapshot["encode_eta"] == "45"
    assert snapshot["encode_out_time"] == "1234"


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
    _seed_run(db_path, library="movies", ts_end="fd2b992c")
    service._record_activity(
        "run_completed",
        "Run completed: movies",
        library="movies",
        run_id="movies-fd2b992c",
    )

    status_code, body, _ = _call_get(service, "/activity")

    assert status_code == 200
    assert 'href="/runs/movies-fd2b992c"' in body
    assert '>movies-fd2b992c</a>' in body


def test_activity_page_run_id_plain_when_missing(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    service._record_activity("manual_run_requested", "Manual run requested for Movies", library="movies")

    status_code, body, _ = _call_get(service, "/activity")

    assert status_code == 200
    assert "<td>-</td>" in body


def test_activity_page_run_id_not_linked_when_run_missing(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    service._record_activity(
        "run_completed",
        "Run completed: movies",
        library="movies",
        run_id="legacy-missing-run",
    )

    status_code, body, _ = _call_get(service, "/activity")

    assert status_code == 200
    assert "legacy-missing-run" in body
    assert "run unavailable" in body
    assert 'href="/runs/legacy-missing-run"' not in body


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
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    status_code, body, _ = _call_get(service, "/settings")

    assert status_code == 200
    assert "<h1>Settings</h1>" in body
    assert "name=\"movie_schedule\"" not in body
    assert "name=\"tv_schedule\"" not in body
    assert "name=\"min_file_age_minutes\"" in body
    assert "Global Settings" in body
    assert "Libraries" in body
    assert "<strong>Schedule</strong>" in body
    assert "name=\"schedule\"" in body
    assert "<strong>Encoding Settings</strong>" in body
    assert "name=\"qsv_quality\"" in body
    assert "name=\"qsv_preset\"" in body
    assert "name=\"min_savings_percent\"" in body
    assert "name=\"max_savings_percent\"" in body
    assert "Savings Policy" in body
    assert "If unset, this value inherits the global setting." in body
    assert "help-tooltip-trigger" in body
    assert "Minimum File Age (Minutes)" in body


def test_settings_tooltip_metadata_is_defined_for_global_and_library_fields():
    for key, meta in service_module.EDITABLE_SETTINGS.items():
        assert "label" in meta
        assert str(meta["label"]).strip()
        assert "description" in meta
        assert str(meta["description"]).strip()

    expected_library_fields = {
        "name",
        "path",
        "min_size_gb",
        "max_files",
        "priority",
        "qsv_quality",
        "qsv_preset",
        "min_savings_percent",
        "max_savings_percent",
        "skip_codecs",
        "skip_min_height",
        "skip_resolution_tags",
        "schedule",
        "enabled",
    }
    assert expected_library_fields.issubset(set(service_module.LIBRARY_SETTINGS_HELP.keys()))


def test_libraries_table_created_and_bootstrapped_from_env(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    monkeypatch.setenv("MOVIE_MEDIA_ROOT", "/mnt/media/movies")
    monkeypatch.setenv("TV_MEDIA_ROOT", "/mnt/media/tv")
    monkeypatch.setenv("MOVIE_SCHEDULE", "0 3 * * *")
    monkeypatch.setenv("TV_SCHEDULE", "0 4 * * *")
    monkeypatch.setenv("QSV_QUALITY", "22")
    monkeypatch.setenv("QSV_PRESET", "5")
    monkeypatch.setenv("MIN_SAVINGS_PERCENT", "13")

    ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT name, path, enabled, schedule, qsv_quality, qsv_preset, min_savings_percent, max_savings_percent, skip_codecs, skip_min_height, skip_resolution_tags FROM libraries ORDER BY id ASC"
    ).fetchall()
    conn.close()

    assert len(rows) == 2
    assert rows[0]["name"] == "Movies"
    assert rows[0]["path"] == "/mnt/media/movies"
    assert int(rows[0]["enabled"]) == 1
    assert rows[0]["schedule"] == "0 3 * * *"
    assert int(rows[0]["qsv_quality"]) == 22
    assert int(rows[0]["qsv_preset"]) == 5
    assert float(rows[0]["min_savings_percent"]) == 13.0
    assert rows[0]["max_savings_percent"] is None
    assert rows[0]["skip_codecs"] == ""
    assert int(rows[0]["skip_min_height"]) == 0
    assert rows[0]["skip_resolution_tags"] == ""
    assert rows[1]["name"] == "TV"
    assert rows[1]["path"] == "/mnt/media/tv"
    assert int(rows[1]["enabled"]) == 1
    assert rows[1]["schedule"] == "0 4 * * *"
    assert int(rows[1]["qsv_quality"]) == 22
    assert int(rows[1]["qsv_preset"]) == 5
    assert float(rows[1]["min_savings_percent"]) == 13.0
    assert rows[1]["max_savings_percent"] is None
    assert rows[1]["skip_codecs"] == ""
    assert int(rows[1]["skip_min_height"]) == 0
    assert rows[1]["skip_resolution_tags"] == ""


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
        data={
            "name": "Anime",
            "path": "/data/anime",
            "enabled": "1",
            "schedule": "10 1 * * *",
            "min_size_gb": "0.5",
            "max_files": "3",
            "priority": "250",
            "qsv_quality": "20",
            "qsv_preset": "7",
            "min_savings_percent": "12.5",
            "max_savings_percent": "70",
            "skip_codecs": "HEVC, av1",
            "skip_min_height": "1080",
            "skip_resolution_tags": "2160p, 4K",
        },
    )
    assert create_status == 200
    assert "Library created." in create_body

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    anime = conn.execute(
        "SELECT id, name, path, enabled, schedule, min_size_gb, max_files, priority, qsv_quality, qsv_preset, min_savings_percent, max_savings_percent, skip_codecs, skip_min_height, skip_resolution_tags FROM libraries WHERE name = 'Anime'"
    ).fetchone()
    assert anime is not None
    library_id = int(anime["id"])
    assert anime["path"] == "/data/anime"
    assert int(anime["enabled"]) == 1
    assert anime["schedule"] == "10 1 * * *"
    assert float(anime["min_size_gb"]) == 0.5
    assert int(anime["max_files"]) == 3
    assert int(anime["priority"]) == 250
    assert int(anime["qsv_quality"]) == 20
    assert int(anime["qsv_preset"]) == 7
    assert float(anime["min_savings_percent"]) == 12.5
    assert float(anime["max_savings_percent"]) == 70.0
    assert anime["skip_codecs"] == "hevc,av1"
    assert int(anime["skip_min_height"]) == 1080
    assert anime["skip_resolution_tags"] == "2160p,4k"
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
            "min_size_gb": "1.25",
            "max_files": "2",
            "priority": "5",
            "qsv_quality": "23",
            "qsv_preset": "8",
            "min_savings_percent": "10",
            "max_savings_percent": "",
            "skip_codecs": "mpeg2",
            "skip_min_height": "720",
            "skip_resolution_tags": "uhd",
        },
    )
    assert update_status == 200
    assert "Library updated." in update_body

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    updated = conn.execute(
        "SELECT name, path, enabled, schedule, min_size_gb, max_files, priority, qsv_quality, qsv_preset, min_savings_percent, max_savings_percent, skip_codecs, skip_min_height, skip_resolution_tags FROM libraries WHERE id = ?",
        (library_id,),
    ).fetchone()
    assert updated is not None
    assert updated["name"] == "Anime Updated"
    assert updated["path"] == "/data/anime-updated"
    assert int(updated["enabled"]) == 0
    assert updated["schedule"] == "20 2 * * *"
    assert float(updated["min_size_gb"]) == 1.25
    assert int(updated["max_files"]) == 2
    assert int(updated["priority"]) == 5
    assert int(updated["qsv_quality"]) == 23
    assert int(updated["qsv_preset"]) == 8
    assert float(updated["min_savings_percent"]) == 10.0
    assert updated["max_savings_percent"] is None
    assert updated["skip_codecs"] == "mpeg2"
    assert int(updated["skip_min_height"]) == 720
    assert updated["skip_resolution_tags"] == "uhd"
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
    cron = service_module._build_simple_cron("13:45", ["mon", "thu"])
    assert cron == "45 13 * * mon,thu"

    parsed = service_module._parse_simple_cron("45 13 * * mon,thu")
    assert parsed is not None
    assert parsed["time"] == "13:45"
    assert parsed["days"] == ["mon", "thu"]


def test_simple_schedule_parser_supports_sunday_zero_and_seven():
    parsed_zero = service_module._parse_simple_cron("0 2 * * 0")
    assert parsed_zero is not None
    assert parsed_zero["days"] == ["sun"]

    parsed_seven = service_module._parse_simple_cron("0 2 * * 7")
    assert parsed_seven is not None
    assert parsed_seven["days"] == ["sun"]


def test_simple_schedule_parser_supports_legacy_numeric_saturday_and_monday():
    parsed_saturday = service_module._parse_simple_cron("15 20 * * 6")
    assert parsed_saturday is not None
    assert parsed_saturday["days"] == ["sat"]

    parsed_monday = service_module._parse_simple_cron("0 6 * * 1")
    assert parsed_monday is not None
    assert parsed_monday["days"] == ["mon"]


def test_simple_schedule_ui_populates_for_supported_cron(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    conn = sqlite3.connect(str(db_path))
    conn.execute("UPDATE libraries SET schedule = ? WHERE name = ?", ("30 6 * * 1,wed,5", "Movies"))
    conn.commit()
    conn.close()

    status_code, body, _ = _call_get(service, "/settings")

    assert status_code == 200
    assert 'value="06:30" selected' in body
    assert 'name="schedule_day_mon" value="1" checked' in body
    assert 'name="schedule_day_wed" value="1" checked' in body
    assert 'name="schedule_day_fri" value="1" checked' in body


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
            "schedule_day_sun": "1",
            "schedule_day_sat": "1",
        },
    )
    assert status_code == 200
    assert "Library created." in body

    conn = sqlite3.connect(str(db_path))
    row = conn.execute("SELECT schedule FROM libraries WHERE name = ?", ("Kids",)).fetchone()
    conn.close()
    assert row[0] == "15 9 * * sun,sat"


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
            "schedule_day_mon": "1",
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



def test_library_columns_migrated_with_defaults(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    monkeypatch.setenv("QSV_QUALITY", "24")
    monkeypatch.setenv("QSV_PRESET", "6")
    monkeypatch.setenv("MIN_SAVINGS_PERCENT", "11")

    conn = sqlite3.connect(str(db_path))
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
        "INSERT INTO libraries(name, path, enabled, schedule, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
        ("Legacy", "/legacy", 1, "", "2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"),
    )
    conn.commit()
    conn.close()

    ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    conn = sqlite3.connect(str(db_path))
    row = conn.execute(
        "SELECT min_size_gb, max_files, priority, qsv_quality, qsv_preset, min_savings_percent FROM libraries WHERE name = ?",
        ("Legacy",),
    ).fetchone()
    conn.close()

    assert float(row[0]) == 0.0
    assert int(row[1]) == 1
    assert int(row[2]) == 100
    assert int(row[3]) == 24
    assert int(row[4]) == 6
    assert float(row[5]) == 11.0


def test_library_validation_rejects_invalid_processing_inputs(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    message = service.create_library(
        {"name": "Bad1", "path": "/data/bad1", "enabled": "1", "schedule": "", "min_size_gb": "-1", "max_files": "1"}
    )
    assert "minimum file size must be >= 0" in message

    message = service.create_library(
        {"name": "Bad2", "path": "/data/bad2", "enabled": "1", "schedule": "", "min_size_gb": "0", "max_files": "0"}
    )
    assert "max files per run must be >= 1" in message

    message = service.create_library(
        {
            "name": "Bad3",
            "path": "/data/bad3",
            "enabled": "1",
            "schedule": "",
            "min_size_gb": "0",
            "max_files": "1",
            "priority": "urgent",
        }
    )
    assert "priority must be an integer" in message

    message = service.create_library(
        {"name": "Bad4", "path": "/data/bad4", "enabled": "1", "schedule": "", "qsv_quality": "fast"}
    )
    assert "QSV quality must be an integer" in message

    message = service.create_library(
        {"name": "Bad5", "path": "/data/bad5", "enabled": "1", "schedule": "", "qsv_preset": "-1"}
    )
    assert "QSV preset must be >= 0" in message

    message = service.create_library(
        {
            "name": "Bad6",
            "path": "/data/bad6",
            "enabled": "1",
            "schedule": "",
            "min_savings_percent": "none",
        }
    )
    assert "minimum savings percent must be a number" in message


def test_queue_prefers_higher_priority_and_keeps_fifo_for_ties(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    _call_post(
        service,
        "/settings/libraries/create",
        data={
            "name": "Anime",
            "path": "/data/anime",
            "enabled": "1",
            "schedule": "",
            "min_size_gb": "0",
            "max_files": "1",
            "priority": "25",
        },
    )

    conn = sqlite3.connect(str(db_path))
    conn.execute("UPDATE libraries SET priority = ? WHERE name = ?", (100, "Movies"))
    conn.execute("UPDATE libraries SET priority = ? WHERE name = ?", (50, "TV"))
    conn.commit()
    conn.close()

    calls = []
    start_gate = threading.Event()
    release_gate = threading.Event()

    def blocking_run_once(library, trigger):
        calls.append((library, trigger))
        start_gate.set()
        release_gate.wait(timeout=1)

    monkeypatch.setattr(service, "_run_library_once", blocking_run_once)

    assert service.trigger_library("tv") is True
    assert start_gate.wait(timeout=1)
    assert service.trigger_library("anime") is True
    assert service.trigger_library("movies") is True
    release_gate.set()

    for _ in range(100):
        if len(calls) >= 3:
            break
        time.sleep(0.01)

    assert [item[0] for item in calls] == ["tv", "movies", "anime"]

    calls[:] = []
    start_gate.clear()
    release_gate.clear()

    conn = sqlite3.connect(str(db_path))
    conn.execute("UPDATE libraries SET priority = ? WHERE name IN (?, ?)", (80, "Movies", "TV"))
    conn.commit()
    conn.close()

    assert service.trigger_library("movies") is True
    assert start_gate.wait(timeout=1)
    assert service.trigger_library("tv") is True
    release_gate.set()

    for _ in range(100):
        if len(calls) >= 2:
            break
        time.sleep(0.01)

    assert [item[0] for item in calls] == ["movies", "tv"]


def test_duplicate_library_protection_unchanged_with_priority(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    gate = threading.Event()

    def blocking_run_once(library, trigger):
        gate.wait(timeout=1)

    monkeypatch.setattr(service, "_run_library_once", blocking_run_once)

    first_started = service.trigger_library("movies")
    second_started = service.trigger_library("movies")
    gate.set()

    assert first_started is True
    assert second_started is False

def test_settings_table_created_and_bootstrapped_from_env(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    monkeypatch.setenv("MOVIE_SCHEDULE", "0 5 * * *")
    monkeypatch.setenv("TV_SCHEDULE", "30 6 * * *")
    monkeypatch.setenv("MIN_FILE_AGE_MINUTES", "22")
    monkeypatch.setenv("MIN_SAVINGS_PERCENT", "18")
    monkeypatch.setenv("MAX_SAVINGS_PERCENT", "45")
    monkeypatch.setenv("MIN_MEDIA_FREE_GB", "80")
    monkeypatch.setenv("MAX_GB_PER_RUN", "55")
    monkeypatch.setenv("FAIL_FAST", "true")
    monkeypatch.setenv("LOG_SKIPS", "true")
    monkeypatch.setenv("TOP_CANDIDATES", "9")
    monkeypatch.setenv("RETRY_COUNT", "4")
    monkeypatch.setenv("RETRY_BACKOFF_SECONDS", "8")
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
        "min_savings_percent",
        "max_savings_percent",
        "min_media_free_gb",
        "max_gb_per_run",
        "fail_fast",
        "log_skips",
        "top_candidates",
        "retry_count",
        "retry_backoff_seconds",
        "validate_seconds",
        "log_retention_days",
        "bak_retention_days",
        "discord_webhook_url",
        "generic_webhook_url",
        "enable_run_complete_notifications",
        "enable_run_failure_notifications",
        "housekeeping_enabled",
        "housekeeping_schedule",
    }
    values = {row["key"]: row["value"] for row in rows}
    assert values["min_file_age_minutes"] == "22"
    assert values["min_savings_percent"] == "18"
    assert values["max_savings_percent"] == "45"
    assert values["min_media_free_gb"] == "80"
    assert values["max_gb_per_run"] == "55"
    assert values["fail_fast"] == "true"
    assert values["log_skips"] == "true"
    assert values["top_candidates"] == "9"
    assert values["retry_count"] == "4"
    assert values["retry_backoff_seconds"] == "8"
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
            "min_savings_percent": "25",
            "max_savings_percent": "35",
            "min_media_free_gb": "44",
            "max_gb_per_run": "12",
            "top_candidates": "8",
            "fail_fast": "1",
            "log_skips": "1",
            "retry_count": "3",
            "retry_backoff_seconds": "11",
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
    assert values["min_savings_percent"] == "25"
    assert values["max_savings_percent"] == "35"
    assert values["min_media_free_gb"] == "44"
    assert values["max_gb_per_run"] == "12"
    assert values["top_candidates"] == "8"
    assert values["fail_fast"] == "1"
    assert values["log_skips"] == "1"
    assert values["retry_count"] == "3"
    assert values["retry_backoff_seconds"] == "11"
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


def test_run_uses_db_backed_library_and_editable_settings(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    service.update_editable_settings({"min_file_age_minutes": "7", "retry_count": "9", "fail_fast": "1", "top_candidates": "6"})

    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "UPDATE libraries SET min_size_gb = ?, max_files = ?, qsv_quality = ?, qsv_preset = ?, min_savings_percent = ?, max_savings_percent = ?, skip_codecs = ?, skip_min_height = ?, skip_resolution_tags = ? WHERE name = ?",
        (2.5, 4, 20, 8, 12.0, 70.0, "h264,av1", 1080, "2160p,uhd", "Movies"),
    )
    conn.commit()
    conn.close()

    captured = {}

    def fake_run_once():
        captured["max_files"] = os.getenv("MAX_FILES")
        captured["min_size_gb"] = os.getenv("MIN_SIZE_GB")
        captured["min_file_age_minutes"] = os.getenv("MIN_FILE_AGE_MINUTES")
        captured["retry_count"] = os.getenv("RETRY_COUNT")
        captured["fail_fast"] = os.getenv("FAIL_FAST")
        captured["top_candidates"] = os.getenv("TOP_CANDIDATES")
        captured["qsv_quality"] = os.getenv("QSV_QUALITY")
        captured["qsv_preset"] = os.getenv("QSV_PRESET")
        captured["min_savings_percent"] = os.getenv("MIN_SAVINGS_PERCENT")
        captured["max_savings_percent"] = os.getenv("MAX_SAVINGS_PERCENT")
        captured["skip_codecs"] = os.getenv("SKIP_CODECS")
        captured["skip_min_height"] = os.getenv("SKIP_MIN_HEIGHT")
        captured["skip_resolution_tags"] = os.getenv("SKIP_RESOLUTION_TAGS")
        return 0

    monkeypatch.setattr(service_module, "run", fake_run_once)

    service._run_library_once("movies", "manual")

    assert captured["max_files"] == "4"
    assert captured["min_size_gb"] == "2.5"
    assert captured["min_file_age_minutes"] == "7"
    assert captured["retry_count"] == "9"
    assert captured["fail_fast"] == "1"
    assert captured["top_candidates"] == "6"
    assert captured["qsv_quality"] == "20"
    assert captured["qsv_preset"] == "8"
    assert captured["min_savings_percent"] == "12.0"
    assert captured["max_savings_percent"] == "70.0"
    assert captured["skip_codecs"] == "h264,av1"
    assert captured["skip_min_height"] == "1080"
    assert captured["skip_resolution_tags"] == "2160p,uhd"




def test_db_global_settings_override_environment_after_bootstrap(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    monkeypatch.setenv("TOP_CANDIDATES", "11")

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    monkeypatch.setenv("TOP_CANDIDATES", "2")

    captured = {}

    def fake_run_once():
        captured["top_candidates"] = os.getenv("TOP_CANDIDATES")
        return 0

    monkeypatch.setattr(service_module, "run", fake_run_once)

    service._run_library_once("movies", "manual")
    assert captured["top_candidates"] == "11"

    service.update_editable_settings({"top_candidates": "4"})
    service._run_library_once("movies", "manual")
    assert captured["top_candidates"] == "4"

def test_run_falls_back_to_defaults_when_library_encode_settings_missing(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    monkeypatch.setenv("QSV_QUALITY", "26")
    monkeypatch.setenv("QSV_PRESET", "4")
    monkeypatch.setenv("MIN_SAVINGS_PERCENT", "9")
    monkeypatch.setenv("MAX_SAVINGS_PERCENT", "75")

    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "UPDATE libraries SET qsv_quality = NULL, qsv_preset = NULL, min_savings_percent = NULL, max_savings_percent = NULL WHERE name = ?",
        ("Movies",),
    )
    conn.commit()
    conn.close()

    captured = {}

    def fake_run_once():
        captured["qsv_quality"] = os.getenv("QSV_QUALITY")
        captured["qsv_preset"] = os.getenv("QSV_PRESET")
        captured["min_savings_percent"] = os.getenv("MIN_SAVINGS_PERCENT")
        captured["max_savings_percent"] = os.getenv("MAX_SAVINGS_PERCENT")
        captured["skip_codecs"] = os.getenv("SKIP_CODECS")
        captured["skip_min_height"] = os.getenv("SKIP_MIN_HEIGHT")
        captured["skip_resolution_tags"] = os.getenv("SKIP_RESOLUTION_TAGS")
        return 0

    monkeypatch.setattr(service_module, "run", fake_run_once)

    service._run_library_once("movies", "manual")

    assert captured["qsv_quality"] == "26"
    assert captured["qsv_preset"] == "4"
    assert captured["min_savings_percent"] == "9.0"
    assert captured["max_savings_percent"] == "75"


def test_settings_route_renders_notification_fields(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    status_code, body, _ = _call_get(service, "/settings")

    assert status_code == 200
    assert "name=\"discord_webhook_url\"" in body
    assert "name=\"generic_webhook_url\"" in body
    assert "name=\"enable_run_complete_notifications\"" in body
    assert "name=\"enable_run_failure_notifications\"" in body




def test_settings_notification_secrets_are_masked_and_never_echoed(tmp_path, monkeypatch):
    from chonk_reducer import secrets

    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    monkeypatch.setenv(secrets.SECRET_ENV_VAR, "test-secret-key-123")

    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    service.update_editable_settings({"discord_webhook_url": "https://discord.example/hook"})

    status_code, body, _ = _call_get(service, "/settings")

    assert status_code == 200
    assert "https://discord.example/hook" not in body
    assert "Configured (hidden)" in body
    assert "placeholder=\"Set (hidden)\"" in body


def test_blank_secret_submission_preserves_existing_notification_secret(tmp_path, monkeypatch):
    from chonk_reducer import secrets

    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    monkeypatch.setenv(secrets.SECRET_ENV_VAR, "test-secret-key-123")

    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    service.update_editable_settings({"discord_webhook_url": "https://discord.example/hook"})

    before = service._editable_settings["discord_webhook_url"]
    status_code, _ = _call_post(service, "/settings", data={"discord_webhook_url": "", "min_file_age_minutes": "12"})

    assert status_code == 200
    assert service._editable_settings["discord_webhook_url"] == before


def test_replacing_secret_updates_encrypted_value(tmp_path, monkeypatch):
    from chonk_reducer import secrets

    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    monkeypatch.setenv(secrets.SECRET_ENV_VAR, "test-secret-key-123")

    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    service.update_editable_settings({"discord_webhook_url": "https://discord.example/one"})
    first = service._editable_settings["discord_webhook_url"]

    service.update_editable_settings({"discord_webhook_url": "https://discord.example/two"})
    second = service._editable_settings["discord_webhook_url"]

    assert first != second
    assert secrets.decrypt_secret(second) == "https://discord.example/two"


def test_secret_webhook_round_trip_preserves_exact_value_after_decrypt(tmp_path, monkeypatch):
    from chonk_reducer import secrets

    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    monkeypatch.setenv(secrets.SECRET_ENV_VAR, "test-secret-key-123")

    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    entered = "  https://discord.com/api/webhooks/123/abcDEF-gh?wait=true&name=ops+alerts  \r\n"
    status_code, _ = _call_post(service, "/settings", data={"discord_webhook_url": entered})

    assert status_code == 200
    encrypted = service._editable_settings["discord_webhook_url"]
    decrypted = secrets.decrypt_secret(encrypted)
    assert decrypted == entered.strip().replace("\r", "").replace("\n", "")


def test_secret_webhook_special_characters_survive_post_round_trip(tmp_path, monkeypatch):
    from chonk_reducer import secrets

    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    monkeypatch.setenv(secrets.SECRET_ENV_VAR, "test-secret-key-123")

    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    webhook = "https://example.test/hook?token=a+b%2Fz&meta=%3Ctag%3E&x=1&y=2"
    status_code, _ = _call_post(service, "/settings", data={"generic_webhook_url": webhook})

    assert status_code == 200
    assert secrets.decrypt_secret(service._editable_settings["generic_webhook_url"]) == webhook


def test_masked_secret_placeholder_submission_preserves_existing_notification_secret(tmp_path, monkeypatch):
    from chonk_reducer import secrets

    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    monkeypatch.setenv(secrets.SECRET_ENV_VAR, "test-secret-key-123")

    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    service.update_editable_settings({"discord_webhook_url": "https://discord.example/hook"})

    before = service._editable_settings["discord_webhook_url"]
    status_code, _ = _call_post(service, "/settings", data={"discord_webhook_url": "Configured (hidden)", "min_file_age_minutes": "12"})

    assert status_code == 200
    assert service._editable_settings["discord_webhook_url"] == before


def test_post_settings_persists_notification_fields_encrypted(tmp_path, monkeypatch):
    from chonk_reducer import secrets

    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    monkeypatch.setenv(secrets.SECRET_ENV_VAR, "test-secret-key-123")

    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    status_code, _ = _call_post(
        service,
        "/settings",
        data={
            "discord_webhook_url": "https://discord.example/hook",
            "generic_webhook_url": "https://generic.example/hook",
            "enable_run_complete_notifications": "1",
            "enable_run_failure_notifications": "1",
        },
    )

    assert status_code == 200

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    values = {row["key"]: row["value"] for row in conn.execute("SELECT key, value FROM settings").fetchall()}
    conn.close()

    assert values["discord_webhook_url"].startswith(secrets.SECRET_PREFIX)
    assert values["generic_webhook_url"].startswith(secrets.SECRET_PREFIX)
    assert secrets.decrypt_secret(values["discord_webhook_url"]) == "https://discord.example/hook"
    assert secrets.decrypt_secret(values["generic_webhook_url"]) == "https://generic.example/hook"


def test_settings_test_notification_action_renders_result_message(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    monkeypatch.setattr(
        service_module.notifications,
        "send_test_notification",
        lambda settings_db_path=None: {"ok": True, "message": "Test notification sent successfully."},
    )

    status_code, body = _call_post(service, "/settings/test-notification", data={})

    assert status_code == 200
    assert "Test notification sent successfully." in body


def test_run_completion_triggers_notification(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    called = {}

    def fake_send(summary, settings_db_path=None):
        called["summary"] = summary
        called["settings_db_path"] = settings_db_path

    monkeypatch.setattr(service_module, "run", lambda: 0)
    monkeypatch.setattr(service_module.notifications, "send_run_complete", fake_send)

    service._run_library_once("movies", "manual")

    assert called["summary"]["library"] == "Movies"
    assert called["summary"]["run_id"]
    assert called["settings_db_path"] == str(db_path)


def test_notification_errors_do_not_crash_service(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    monkeypatch.setattr(service_module, "run", lambda: 0)

    def explode(summary, settings_db_path=None):
        raise RuntimeError("webhook unavailable")

    monkeypatch.setattr(service_module.notifications, "send_run_complete", explode)

    service._run_library_once("movies", "manual")


def test_run_failure_triggers_notification(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    called = {}

    def fake_failure(summary, settings_db_path=None):
        called["summary"] = summary
        called["settings_db_path"] = settings_db_path

    monkeypatch.setattr(service_module, "run", lambda: 3)
    monkeypatch.setattr(service_module.notifications, "send_run_failure", fake_failure)

    service._run_library_once("movies", "manual")

    assert called["summary"]["library"] == "Movies"
    assert "Run exited with code 3" in called["summary"]["error_message"]
    assert called["settings_db_path"] == str(db_path)


def test_cancel_endpoint_returns_idle_when_no_active_run():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    status_code, payload = _call_post(service, "/api/run/cancel")

    assert status_code == 200
    assert payload == {"status": "idle"}


def test_cancel_endpoint_sets_cancelling_and_runtime_status(monkeypatch):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    started = threading.Event()
    release = threading.Event()

    def blocking_run(library, trigger):
        del library, trigger
        started.set()
        while not service._is_cancel_requested():
            time.sleep(0.01)
        release.wait(timeout=1)

    monkeypatch.setattr(service, "_run_library_once", blocking_run)

    payload, status_code = service.manual_run_payload("movies")
    assert status_code == 202
    assert payload["status"] == "queued"
    assert started.wait(timeout=1)

    status_code, payload = _call_post(service, "/api/run/cancel")
    assert status_code == 200
    assert payload == {"status": "cancelling"}

    observed = None
    for _ in range(40):
        status_code, body, snapshot = _call_get(service, "/api/status")
        assert status_code == 200
        if snapshot is None:
            if isinstance(body, dict):
                snapshot = body
            else:
                snapshot = json.loads(body or "{}")
        observed = snapshot.get("status")
        if observed == "Cancelling":
            break
        time.sleep(0.01)
    assert observed == "Cancelling"

    release.set()


def test_dashboard_includes_stop_run_button():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    status_code, body, _ = _call_get(service, "/dashboard")

    assert status_code == 200
    assert "runtime-stop-button" in body
    assert "Stop Run" in body


def test_settings_bootstrap_uses_legacy_retry_backoff_env_if_new_absent(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    monkeypatch.delenv("RETRY_BACKOFF_SECONDS", raising=False)
    monkeypatch.setenv("RETRY_BACKOFF_SECS", "13")

    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    assert service._editable_settings["retry_backoff_seconds"] == "13"


def test_runtime_snapshot_and_dashboard_show_retry_attempt_only_while_retrying(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    service._current_job = RuntimeJob(library_id=1, library_name="Movies", trigger="manual", priority=100)
    service._current_run_snapshot = {"retry_attempt": "1", "retry_max": "3", "files_processed": "0", "candidates_found": "1"}

    snapshot = service._runtime_status_snapshot()
    assert snapshot["retry_attempt"] == "1"
    assert snapshot["retry_max"] == "3"

    html = service._runtime_progress_overview_html(snapshot)
    assert "Retry Attempt:</strong> 1 / 3" in html

    service._current_run_snapshot = {"retry_attempt": "", "retry_max": "", "files_processed": "0", "candidates_found": "1"}
    snapshot = service._runtime_status_snapshot()
    html = service._runtime_progress_overview_html(snapshot)
    assert "Retry Attempt:" not in html


def test_dashboard_shows_preview_run_button():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    try:
        status_code, body, _ = _call_get(service, "/dashboard")
    finally:
        service.stop_background_worker()

    assert status_code == 200
    assert "Preview Run" in body
    assert "formaction=\"/dashboard/libraries/1/preview\"" in body
    assert "Clear Preview Results" in body




def test_dashboard_preview_library_redirects_immediately_after_queueing(monkeypatch):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    release = threading.Event()

    def blocking_run_once(library, trigger):
        assert trigger == "preview"
        release.wait(timeout=1)

    monkeypatch.setattr(service, "_run_library_once", blocking_run_once)

    start = time.monotonic()
    status_code, response = _call_post(service, "/dashboard/libraries/1/preview", follow_redirects=False)
    elapsed = time.monotonic() - start

    assert status_code == 303
    assert response.headers["location"] == "/dashboard?manual_run=queued&library_id=1"
    assert elapsed < 0.5
    release.set()
    service.stop_background_worker()


def test_preview_run_sets_runtime_mode_to_preview_while_active(monkeypatch):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    running = threading.Event()
    release = threading.Event()

    def blocking_run_once(library, trigger):
        assert trigger == "preview"
        running.set()
        release.wait(timeout=1)

    monkeypatch.setattr(service, "_run_library_once", blocking_run_once)
    status_code, payload = _call_post(service, "/libraries/1/preview")

    assert status_code == 202
    assert payload["status"] == "queued"
    assert running.wait(timeout=1)

    snapshot = service.current_job_status()
    assert snapshot["status"] == "Running"
    assert snapshot["mode"] == "Preview"
    assert snapshot["current_library"] == "Movies"

    release.set()
    service.stop_background_worker()


def test_preview_results_persist_on_dashboard_after_preview_completes(monkeypatch):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    done = threading.Event()

    def fake_run_once(_library, trigger):
        assert trigger == "preview"
        with service._job_condition:
            service._current_run_snapshot["preview_results_json"] = json.dumps(
                [
                    {
                        "file": "/movies/a.mkv",
                        "original_size": 1000,
                        "estimated_size": 700,
                        "estimated_savings_pct": 30.0,
                        "decision": "Encode",
                    }
                ]
            )
        done.set()

    monkeypatch.setattr(service, "_run_library_once", fake_run_once)
    status_code, payload = _call_post(service, "/libraries/1/preview")

    assert status_code == 202
    assert payload["status"] == "queued"
    assert done.wait(timeout=1)

    for _ in range(20):
        snapshot = service.current_job_status()
        if snapshot["status"] == "Idle":
            break
        time.sleep(0.01)

    snapshot = service.current_job_status()
    assert snapshot["status"] == "Idle"
    assert snapshot["preview_results"]
    assert snapshot["preview_results"][0]["decision"] == "Encode"
    assert snapshot["preview_library"] == "Movies"
    assert snapshot["preview_generated_at"]

    status_code, body, _ = _call_get(service, "/dashboard")
    assert status_code == 200
    assert "Library:</strong>" in body
    assert "Generated At:</strong>" in body
    assert "Movies" in body
    assert "/movies/a.mkv" in body
    assert "Encode" in body

    service.stop_background_worker()

def test_preview_result_replacement_is_scoped_per_library(monkeypatch):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    def fake_run_once(library, trigger):
        assert trigger == "preview"
        row_file = "/movies/new.mkv" if library.lower() == "movies" else "/tv/new.mkv"
        with service._job_condition:
            service._current_run_snapshot["preview_generated_at"] = "2026-01-01T00:00:00Z" if library.lower() == "movies" else "2026-01-02T00:00:00Z"
            service._current_run_snapshot["preview_results_json"] = json.dumps(
                [
                    {
                        "file": row_file,
                        "original_size": 1000,
                        "estimated_size": 700,
                        "estimated_savings_pct": 30.0,
                        "decision": "Encode",
                    }
                ]
            )

    monkeypatch.setattr(service, "_run_library_once", fake_run_once)

    status_code, payload = _call_post(service, "/libraries/1/preview")
    assert status_code == 202
    assert payload["status"] == "queued"

    for _ in range(40):
        if service.current_job_status()["status"] == "Idle":
            break
        time.sleep(0.01)

    movie_snapshot = service.current_job_status()
    assert movie_snapshot["preview_library"] == "Movies"
    assert movie_snapshot["preview_results"][0]["file"] == "/movies/new.mkv"

    status_code, payload = _call_post(service, "/libraries/1/preview")
    assert status_code == 202
    assert payload["status"] == "queued"
    for _ in range(40):
        if service.current_job_status()["status"] == "Idle":
            break
        time.sleep(0.01)

    replaced_snapshot = service.current_job_status()
    assert replaced_snapshot["preview_library"] == "Movies"
    assert replaced_snapshot["preview_results"][0]["file"] == "/movies/new.mkv"

    status_code, payload = _call_post(service, "/libraries/2/preview")
    assert status_code == 202
    assert payload["status"] == "queued"
    for _ in range(40):
        if service.current_job_status()["status"] == "Idle":
            break
        time.sleep(0.01)

    tv_snapshot = service.current_job_status()
    assert tv_snapshot["preview_library"] == "TV"
    assert tv_snapshot["preview_results"][0]["file"] == "/tv/new.mkv"

    status_code, body, _ = _call_get(service, "/dashboard")
    assert status_code == 200
    assert "Library:</strong> <span id=\"runtime-preview-library\">TV" in body
    assert "Generated At:</strong> <span id=\"runtime-preview-generated-at\">2026-01-02T00:00:00Z" in body

    service.stop_background_worker()


def test_preview_endpoint_exists_and_returns_json_payload():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    try:
        status_code, payload = _call_post(service, "/libraries/1/preview")
    finally:
        service.stop_background_worker()

    assert status_code == 202
    assert payload["status"] == "queued"
    assert payload["library_id"] == 1


def test_preview_clear_endpoint_clears_snapshot_and_returns_json_payload():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )

    service._last_preview_results = [
        {
            "file": "/movies/a.mkv",
            "original_size": 1000,
            "estimated_size": 700,
            "estimated_savings_pct": 30.0,
            "decision": "Encode",
        }
    ]
    service._last_preview_snapshots_by_library = {
        1: {
            "library_id": 1,
            "library_name": "Movies",
            "generated_at": "2026-03-08T22:48:17Z",
            "results": list(service._last_preview_results),
        }
    }
    service._latest_preview_library_id = 1

    try:
        status_code, payload = _call_post(service, "/api/preview/clear")
    finally:
        service.stop_background_worker()

    assert status_code == 200
    assert payload == {"status": "cleared"}
    snapshot = service.current_job_status()
    assert snapshot["preview_results"] == []
    assert snapshot["preview_library"] == ""
    assert snapshot["preview_generated_at"] == ""


def test_dashboard_returns_to_empty_preview_state_after_preview_clear():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    service._last_preview_snapshots_by_library = {
        1: {
            "library_id": 1,
            "library_name": "Movies",
            "generated_at": "2026-03-08T22:48:17Z",
            "results": [
                {
                    "file": "/movies/a.mkv",
                    "original_size": 1000,
                    "estimated_size": 700,
                    "estimated_savings_pct": 30.0,
                    "decision": "Encode",
                }
            ],
        }
    }
    service._latest_preview_library_id = 1

    try:
        before_code, before_body, _ = _call_get(service, "/dashboard")
        clear_code, clear_payload = _call_post(service, "/api/preview/clear")
        after_code, after_body, _ = _call_get(service, "/dashboard")
    finally:
        service.stop_background_worker()

    assert before_code == 200
    assert "/movies/a.mkv" in before_body
    assert clear_code == 200
    assert clear_payload == {"status": "cleared"}
    assert after_code == 200
    assert "Library:</strong> <span id=\"runtime-preview-library\">-" in after_body
    assert "Generated At:</strong> <span id=\"runtime-preview-generated-at\">-" in after_body
    assert "No preview results yet." in after_body


def test_dashboard_formats_scheduler_started_timestamp_for_readability():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    service._scheduler_started_at = "2026-03-08T22:48:17Z"

    try:
        status_code, body, _ = _call_get(service, "/dashboard")
    finally:
        service.stop_background_worker()

    assert status_code == 200
    assert "2026-03-08 22:48" in body
    assert "2026-03-08T22:48:17Z" not in body


def test_manual_preview_payload_queues_preview_trigger(monkeypatch):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    captured = {"trigger": ""}

    def _enqueue(_library, trigger):
        captured["trigger"] = trigger
        return True

    monkeypatch.setattr(service, "_enqueue_library_job", _enqueue)
    try:
        payload, status_code = service.manual_preview_payload_for_id(1)
    finally:
        service.stop_background_worker()

    assert status_code == 202
    assert payload["status"] == "queued"
    assert captured["trigger"] == "preview"


def test_preview_endpoint_triggers_preview_job(monkeypatch):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    captured = {"trigger": "", "library_name": ""}

    def _enqueue(library, trigger):
        captured["trigger"] = trigger
        captured["library_name"] = library.name
        return True

    monkeypatch.setattr(service, "_enqueue_library_job", _enqueue)
    try:
        status_code, payload = _call_post(service, "/libraries/1/preview")
    finally:
        service.stop_background_worker()

    assert status_code == 202
    assert payload["status"] == "queued"
    assert captured["trigger"] == "preview"
    assert captured["library_name"].lower() == "movies"


def test_runtime_snapshot_includes_preview_mode_and_results():
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    try:
        with service._job_condition:
            service._current_job = RuntimeJob(library_id=1, library_name="movies", trigger="preview", priority=1)
            service._current_run_snapshot = {
                "mode": "Preview",
                "preview_results_json": json.dumps(
                    [
                        {
                            "file": "/movies/a.mkv",
                            "original_size": 1000,
                            "estimated_size": 600,
                            "estimated_savings_pct": 40.0,
                            "decision": "Encode",
                        }
                    ]
                ),
                "files_processed": "",
                "bytes_saved": "",
            }

        snapshot = service.current_job_status()
        status_code, body, payload = _call_get(service, "/api/status")
    finally:
        service.stop_background_worker()

    if payload is None:
        if isinstance(body, dict):
            payload = body
        else:
            payload = json.loads(body or "{}")

    assert snapshot["mode"] == "Preview"
    assert snapshot["files_processed"] == ""
    assert snapshot["bytes_saved"] == ""
    assert snapshot["preview_results"][0]["decision"] == "Encode"
    assert status_code == 200
    assert payload["mode"] == "Preview"
    assert payload["preview_results"][0]["decision"] == "Encode"


def test_next_run_from_cron_computes_simple_schedule(monkeypatch):
    monkeypatch.setenv("TZ", "UTC")
    now = datetime(2026, 3, 10, 1, 0)

    value = service_module._next_run_from_cron("0 2 * * *", now=now)

    assert value is not None
    assert service_module._format_scheduler_datetime(value) == "2026-03-10 02:00"


def test_next_run_from_cron_computes_advanced_cron(monkeypatch):
    monkeypatch.setenv("TZ", "UTC")
    now = datetime(2026, 3, 10, 1, 5)

    value = service_module._next_run_from_cron("*/15 1-2 * * *", now=now)

    assert value is not None
    assert service_module._format_scheduler_datetime(value) == "2026-03-10 01:15"


def test_next_run_from_cron_computes_saturday_schedule(monkeypatch):
    monkeypatch.setenv("TZ", "UTC")
    now = datetime(2026, 3, 9, 12, 0)

    value = service_module._next_run_from_cron("15 20 * * 6", now=now)

    assert value is not None
    assert service_module._format_scheduler_datetime(value) == "2026-03-14 20:15"


def test_next_run_from_cron_computes_sunday_schedule_with_zero(monkeypatch):
    monkeypatch.setenv("TZ", "UTC")
    now = datetime(2026, 3, 9, 12, 0)

    value = service_module._next_run_from_cron("0 2 * * 0", now=now)

    assert value is not None
    assert service_module._format_scheduler_datetime(value) == "2026-03-15 02:00"


def test_next_run_from_cron_computes_sunday_schedule_with_seven(monkeypatch):
    monkeypatch.setenv("TZ", "UTC")
    now = datetime(2026, 3, 9, 12, 0)

    value = service_module._next_run_from_cron("0 2 * * 7", now=now)

    assert value is not None
    assert service_module._format_scheduler_datetime(value) == "2026-03-15 02:00"


def test_next_run_from_cron_computes_named_sunday_and_saturday_schedule(monkeypatch):
    monkeypatch.setenv("TZ", "UTC")
    now = datetime(2026, 3, 9, 12, 0)

    value = service_module._next_run_from_cron("30 20 * * sun,sat", now=now)

    assert value is not None
    assert service_module._format_scheduler_datetime(value) == "2026-03-14 20:30"


def test_next_run_from_cron_computes_named_monday_schedule(monkeypatch):
    monkeypatch.setenv("TZ", "UTC")
    now = datetime(2026, 3, 9, 1, 0)

    value = service_module._next_run_from_cron("0 2 * * mon", now=now)

    assert value is not None
    assert service_module._format_scheduler_datetime(value) == "2026-03-09 02:00"


def test_next_run_from_cron_computes_sunday_before_scheduled_time_same_day(monkeypatch):
    monkeypatch.setenv("TZ", "UTC")
    now = datetime(2026, 3, 15, 1, 58)

    value = service_module._next_run_from_cron("0 2 * * sun", now=now)

    assert value is not None
    assert service_module._format_scheduler_datetime(value) == "2026-03-15 02:00"


def test_next_run_from_cron_computes_sunday_after_scheduled_time_next_week(monkeypatch):
    monkeypatch.setenv("TZ", "UTC")
    now = datetime(2026, 3, 15, 19, 58)

    value = service_module._next_run_from_cron("0 2 * * sun", now=now)

    assert value is not None
    assert service_module._format_scheduler_datetime(value) == "2026-03-22 02:00"


def test_next_run_from_cron_computes_saturday_after_scheduled_time_next_week(monkeypatch):
    monkeypatch.setenv("TZ", "UTC")
    now = datetime(2026, 3, 14, 21, 0)

    value = service_module._next_run_from_cron("15 20 * * sat", now=now)

    assert value is not None
    assert service_module._format_scheduler_datetime(value) == "2026-03-21 20:15"


def test_next_run_from_cron_computes_weekday_after_scheduled_time_next_week(monkeypatch):
    monkeypatch.setenv("TZ", "UTC")
    now = datetime(2026, 3, 11, 3, 0)

    value = service_module._next_run_from_cron("0 2 * * wed", now=now)

    assert value is not None
    assert service_module._format_scheduler_datetime(value) == "2026-03-18 02:00"


def test_dashboard_next_run_matches_scheduler_for_legacy_weekend_crons(monkeypatch):
    monkeypatch.setenv("TZ", "UTC")
    now = datetime(2026, 3, 9, 12, 0)

    normalized_sun = service_module._normalize_schedule_for_scheduler("0 2 * * 0")
    normalized_sat = service_module._normalize_schedule_for_scheduler("15 20 * * 6")

    sunday_next = service_module._next_run_from_cron("0 2 * * 0", now=now)
    saturday_next = service_module._next_run_from_cron("15 20 * * 6", now=now)

    assert sunday_next is not None
    assert saturday_next is not None
    assert service_module._format_scheduler_datetime(sunday_next) == "2026-03-15 02:00"
    assert service_module._format_scheduler_datetime(saturday_next) == "2026-03-14 20:15"

    timezone = service_module._cron_trigger_timezone()
    expected_sunday = service_module._build_scheduler_cron_trigger(normalized_sun, timezone=timezone).get_next_fire_time(None, now)
    expected_saturday = service_module._build_scheduler_cron_trigger(normalized_sat, timezone=timezone).get_next_fire_time(None, now)

    assert service_module._format_scheduler_datetime(sunday_next) == service_module._format_scheduler_datetime(expected_sunday)
    assert service_module._format_scheduler_datetime(saturday_next) == service_module._format_scheduler_datetime(expected_saturday)


def test_next_run_from_cron_computes_named_sun_tue_thu_schedule(monkeypatch):
    monkeypatch.setenv("TZ", "America/Chicago")
    now = datetime(2026, 3, 10, 1, 30, tzinfo=service_module.ZoneInfo("America/Chicago"))

    value = service_module._next_run_from_cron("0 2 * * sun,tue,thu", now=now)

    assert value is not None
    assert service_module._format_scheduler_datetime(value) == "2026-03-10 02:00"


def test_next_run_from_cron_computes_named_mon_wed_fri_schedule(monkeypatch):
    monkeypatch.setenv("TZ", "America/Chicago")
    now = datetime(2026, 3, 11, 1, 30, tzinfo=service_module.ZoneInfo("America/Chicago"))

    value = service_module._next_run_from_cron("0 2 * * mon,wed,fri", now=now)

    assert value is not None
    assert service_module._format_scheduler_datetime(value) == "2026-03-11 02:00"


def test_scheduler_registration_uses_same_next_fire_time_as_dashboard_helper(monkeypatch):
    monkeypatch.setenv("TZ", "America/Chicago")
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    library = service_module.RuntimeLibrary(
        id=99,
        name="Movies",
        path="/movies",
        schedule="0 2 * * sun,tue,thu",
        min_size_gb=0.0,
        max_files=1,
        priority=1,
        qsv_quality=None,
        qsv_preset=None,
        min_savings_percent=None,
    )
    service._register_library_job(library)

    service.scheduler.start()
    try:
        registered_job = service.scheduler.get_job("library-99-schedule")
        expected_next = service_module._next_run_from_cron("0 2 * * sun,tue,thu")

        assert registered_job is not None
        assert expected_next is not None
        assert service_module._format_scheduler_datetime(registered_job.next_run_time) == service_module._format_scheduler_datetime(expected_next)
    finally:
        service.scheduler.shutdown(wait=False)


def test_build_scheduler_cron_trigger_supports_named_weekday_sets(monkeypatch):
    monkeypatch.setenv("TZ", "America/Chicago")
    timezone = service_module._cron_trigger_timezone()
    now = datetime(2026, 3, 9, 12, 0, tzinfo=service_module.ZoneInfo("America/Chicago"))

    movies_trigger = service_module._build_scheduler_cron_trigger("0 2 * * sun,tue,thu", timezone=timezone)
    tv_trigger = service_module._build_scheduler_cron_trigger("0 2 * * mon,wed,fri", timezone=timezone)

    assert service_module._format_scheduler_datetime(movies_trigger.get_next_fire_time(None, now)) == "2026-03-10 02:00"
    assert service_module._format_scheduler_datetime(tv_trigger.get_next_fire_time(None, now)) == "2026-03-11 02:00"


def test_scheduler_event_listener_records_missed_and_error_events(caplog):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    caplog.set_level("INFO")

    class MissedEvent(object):
        code = service_module.EVENT_JOB_MISSED
        job_id = "library-1-schedule"
        exception = None

    class ErrorEvent(object):
        code = service_module.EVENT_JOB_ERROR
        job_id = "library-2-schedule"
        exception = RuntimeError("boom")

    service._on_scheduler_event(MissedEvent())
    service._on_scheduler_event(ErrorEvent())

    assert "Scheduler event: job_missed job_id=library-1-schedule" in caplog.text
    assert "Scheduler event: job_error job_id=library-2-schedule" in caplog.text


def test_scheduler_event_listener_records_library_schedule_execution(caplog):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    caplog.set_level("INFO")

    class ExecutedEvent(object):
        code = service_module.EVENT_JOB_EXECUTED
        job_id = "library-1-schedule"
        exception = None

    service._on_scheduler_event(ExecutedEvent())

    assert "Scheduler event: job_executed job_id=library-1-schedule" in caplog.text


def test_build_scheduler_prefers_apscheduler_when_available(caplog):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    caplog.set_level("INFO")

    scheduler = service._build_scheduler()
    assert scheduler.__class__.__name__ == "BackgroundScheduler"
    assert "APScheduler import status: available" in caplog.text


def test_build_scheduler_logs_import_error_reason_when_falling_back(monkeypatch, caplog):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    caplog.set_level("INFO")

    monkeypatch.setattr(service_module, "BackgroundScheduler", None)
    monkeypatch.setattr(service_module, "_APSCHEDULER_IMPORT_ERROR", ImportError("No module named 'apscheduler'"))

    scheduler = service._build_scheduler()

    assert isinstance(scheduler, service_module._FallbackScheduler)
    assert "APScheduler import status: unavailable (ImportError: No module named 'apscheduler')" in caplog.text




def test_parse_scheduler_cron_fields_supports_named_weekday_sets():
    movies = service_module._parse_scheduler_cron_fields("0 2 * * sun,tue,thu")
    tv = service_module._parse_scheduler_cron_fields("0 2 * * mon,wed,fri")

    assert movies == {
        "minute": "0",
        "hour": "2",
        "day": "*",
        "month": "*",
        "day_of_week": "sun,tue,thu",
    }
    assert tv["day_of_week"] == "mon,wed,fri"


def test_scheduler_registration_logs_none_next_run(monkeypatch, caplog):
    monkeypatch.setattr(service_module, "CronTrigger", None)
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    class FakeJob(object):
        id = "library-99-schedule"
        next_run_time = None

    monkeypatch.setattr(service.scheduler, "add_job", lambda *args, **kwargs: None)
    monkeypatch.setattr(service.scheduler, "get_job", lambda _job_id: FakeJob())

    caplog.set_level("INFO")
    service._register_library_job(
        service_module.RuntimeLibrary(
            id=99,
            name="Movies",
            path="/movies",
            schedule="0 2 * * sun,tue,thu",
            min_size_gb=0.0,
            max_files=1,
            priority=1,
            qsv_quality=None,
            qsv_preset=None,
            min_savings_percent=None,
        )
    )

    assert "next_run=None" in caplog.text


def test_registered_recurring_jobs_have_non_none_next_run_time(monkeypatch):
    monkeypatch.setenv("MOVIE_SCHEDULE", "0 1 * * *")
    monkeypatch.setenv("TV_SCHEDULE", "15 2 * * *")
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    service.register_jobs()
    service.scheduler.start()
    try:
        jobs = {job.id: getattr(job, "next_run_time", None) for job in service.scheduler.get_jobs()}
    finally:
        service.scheduler.shutdown(wait=False)
        service.stop_background_worker()

    assert jobs["library-1-schedule"] is not None
    assert jobs["library-2-schedule"] is not None
    assert jobs["housekeeping-daily"] is not None


def test_run_forever_starts_library_jobs_in_active_state(monkeypatch, caplog):
    monkeypatch.setenv("MOVIE_SCHEDULE", "0 1 * * *")
    monkeypatch.setenv("TV_SCHEDULE", "15 2 * * *")
    service = ChonkService(ServiceSettings(enabled=True, host="127.0.0.1", port=8080, movie_schedule="", tv_schedule=""))
    monkeypatch.setattr(service_module, "uvicorn", None)
    monkeypatch.setattr(service_module, "_run_simple_http_server", lambda *args, **kwargs: None)

    caplog.set_level("INFO")
    rc = service.run_forever()

    assert rc == 0
    assert "Scheduler job active: job_id=library-1-schedule next_run=None" not in caplog.text
    assert "Scheduler job active: job_id=library-2-schedule next_run=None" not in caplog.text
    assert "Scheduler state after start:" in caplog.text
    assert "paused=False" in caplog.text


def test_run_forever_starts_housekeeping_job_in_active_state(monkeypatch, caplog):
    monkeypatch.setenv("MOVIE_SCHEDULE", "0 1 * * *")
    monkeypatch.setenv("TV_SCHEDULE", "15 2 * * *")
    service = ChonkService(ServiceSettings(enabled=True, host="127.0.0.1", port=8080, movie_schedule="", tv_schedule=""))
    monkeypatch.setattr(service_module, "uvicorn", None)
    monkeypatch.setattr(service_module, "_run_simple_http_server", lambda *args, **kwargs: None)

    caplog.set_level("INFO")
    rc = service.run_forever()

    assert rc == 0
    assert "Scheduler job active: job_id=housekeeping-daily next_run=None" not in caplog.text


def test_run_forever_resumes_scheduler_and_logs_job_diagnostics(monkeypatch, caplog):
    service = ChonkService(ServiceSettings(enabled=True, host="127.0.0.1", port=8080, movie_schedule="", tv_schedule=""))
    monkeypatch.setattr(service_module, "uvicorn", None)
    monkeypatch.setattr(service_module, "_run_simple_http_server", lambda *args, **kwargs: None)

    class FakeTrigger(object):
        def get_next_fire_time(self, previous_fire_time, now):
            del previous_fire_time, now
            return datetime(2026, 1, 1, 1, 0, 0)

    class FakeJob(object):
        def __init__(self):
            self.id = "housekeeping-daily"
            self.next_run_time = None
            self.pending = False
            self.trigger = FakeTrigger()

        def modify(self, next_run_time=None):
            self.next_run_time = next_run_time

    class FakeScheduler(object):
        def __init__(self):
            self.state = 2
            self.running = False
            self._job = FakeJob()

        def add_job(self, func, **kwargs):
            del func, kwargs
            return None

        def start(self):
            return None

        def resume(self):
            self.state = 1
            self.running = True

        def get_jobs(self):
            return [self._job]

        def resume_job(self, _job_id):
            return None

        def shutdown(self, wait=False):
            del wait
            return None

    service.scheduler = FakeScheduler()

    caplog.set_level("INFO")
    rc = service.run_forever()

    assert rc == 0
    assert "Scheduler state after start: state=2" in caplog.text
    assert "Scheduler resumed at startup: state=1" in caplog.text
    assert "Scheduler job active: job_id=housekeeping-daily" in caplog.text
    assert "pending=False" in caplog.text
    assert "trigger=<" in caplog.text
    assert "next_run=2026-01-01 01:00:00" in caplog.text

def test_scheduled_library_trigger_logs_and_enqueues_expected_job(caplog, monkeypatch):
    service = ChonkService(
        ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule="")
    )
    calls = []

    def _enqueue(library, trigger):
        calls.append((library.name, trigger))
        return True

    monkeypatch.setattr(service, "_enqueue_library_job", _enqueue)

    caplog.set_level("INFO")
    assert service._scheduled_library_trigger(1) is True

    assert calls == [("Movies", "schedule")]
    assert "Scheduled trigger fired for Movies" in caplog.text


def test_dashboard_library_card_shows_disabled_for_disabled_library(monkeypatch):
    monkeypatch.setenv("MOVIE_SCHEDULE", "0 1 * * *")
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    movies, tv = service.list_libraries()
    disabled_movies = service_module.LibraryRecord(
        id=movies.id,
        name=movies.name,
        path=movies.path,
        enabled=False,
        schedule=movies.schedule,
        min_size_gb=movies.min_size_gb,
        max_files=movies.max_files,
        priority=movies.priority,
        qsv_quality=movies.qsv_quality,
        qsv_preset=movies.qsv_preset,
        min_savings_percent=movies.min_savings_percent,
    )
    monkeypatch.setattr(service, "list_libraries", lambda: [disabled_movies, tv])

    status_code, body, _ = _call_get(service, "/dashboard")

    assert status_code == 200
    assert "Status:</strong> Disabled" in body
    assert "Next Run:</strong> Disabled" in body


def test_next_run_from_cron_returns_none_for_invalid_expression():
    assert service_module._next_run_from_cron("invalid schedule") is None

def test_library_skip_settings_migrate_from_env_and_are_not_overridden(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    monkeypatch.setenv("SKIP_CODECS", "hevc,av1")
    monkeypatch.setenv("SKIP_MIN_HEIGHT", "1080")
    monkeypatch.setenv("SKIP_RESOLUTION_TAGS", "2160p,4k")

    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    movies = conn.execute("SELECT id, skip_codecs, skip_min_height, skip_resolution_tags FROM libraries WHERE name = 'Movies'").fetchone()
    assert movies is not None
    assert movies["skip_codecs"] == "hevc,av1"
    assert int(movies["skip_min_height"]) == 1080
    assert movies["skip_resolution_tags"] == "2160p,4k"

    conn.execute(
        "UPDATE libraries SET skip_codecs = ?, skip_min_height = ?, skip_resolution_tags = ? WHERE id = ?",
        ("mpeg2", 720, "uhd", int(movies["id"])),
    )
    conn.commit()
    conn.close()

    monkeypatch.setenv("SKIP_CODECS", "vp9")
    monkeypatch.setenv("SKIP_MIN_HEIGHT", "2160")
    monkeypatch.setenv("SKIP_RESOLUTION_TAGS", "8k")

    captured = {}

    def fake_run_once():
        captured["skip_codecs"] = os.getenv("SKIP_CODECS")
        captured["skip_min_height"] = os.getenv("SKIP_MIN_HEIGHT")
        captured["skip_resolution_tags"] = os.getenv("SKIP_RESOLUTION_TAGS")
        return 0

    monkeypatch.setattr(service_module, "run", fake_run_once)
    service._run_library_once("movies", "manual")

    assert captured["skip_codecs"] == "mpeg2"
    assert captured["skip_min_height"] == "720"
    assert captured["skip_resolution_tags"] == "uhd"


def test_housekeeping_runs_cleanup_without_encode_run(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    work_root = tmp_path / "work"
    logs_dir = work_root / "logs"
    logs_dir.mkdir(parents=True)
    stale_log = logs_dir / "transcode_20000101_000000.log"
    stale_log.write_text("old", encoding="utf-8")

    monkeypatch.setenv("STATS_PATH", str(db_path))
    monkeypatch.setenv("WORK_ROOT", str(work_root))
    monkeypatch.setenv("LOG_RETENTION_DAYS", "1")

    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    service.run_housekeeping_once()

    assert not stale_log.exists()

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    events = [
        row["event_type"]
        for row in conn.execute(
            "SELECT event_type FROM activity_events WHERE event_type LIKE 'housekeeping_%' ORDER BY id ASC"
        ).fetchall()
    ]
    conn.close()
    assert events == ["housekeeping_started", "housekeeping_completed"]


def test_housekeeping_skips_when_jobs_active(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    with service._job_condition:
        service._job_queue.append(RuntimeJob(library_id=1, library_name="Movies", trigger="manual", priority=100))

    service.run_housekeeping_once()

    conn = sqlite3.connect(str(db_path))
    count = conn.execute(
        "SELECT COUNT(*) FROM activity_events WHERE event_type LIKE 'housekeeping_%'"
    ).fetchone()[0]
    conn.close()
    assert int(count) == 0


def test_housekeeping_settings_bootstrap_defaults(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))

    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    assert service._editable_settings["housekeeping_enabled"] == "1"
    assert service._editable_settings["housekeeping_schedule"] == "0 2 * * *"


def test_housekeeping_schedule_persists_in_db(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    service.update_editable_settings(
        {
            "housekeeping_form": "1",
            "housekeeping_enabled": "1",
            "housekeeping_schedule": "0 2 * * *",
            "housekeeping_day_mon": "1",
            "housekeeping_day_wed": "1",
            "housekeeping_time": "03:30",
        }
    )

    conn = sqlite3.connect(str(db_path))
    value = conn.execute("SELECT value FROM settings WHERE key = 'housekeeping_schedule'").fetchone()[0]
    conn.close()
    assert value == "30 3 * * mon,wed"


def test_housekeeping_job_absent_when_disabled(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    monkeypatch.setenv("HOUSEKEEPING_ENABLED", "0")

    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    service.register_jobs()

    jobs = {job.id for job in service.scheduler.get_jobs()}
    assert service_module.HOUSEKEEPING_JOB_ID not in jobs


def test_updating_housekeeping_settings_refreshes_live_job(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    service.register_jobs()
    assert service.scheduler.get_job(service_module.HOUSEKEEPING_JOB_ID) is not None

    service.update_editable_settings({"housekeeping_form": "1", "housekeeping_schedule": "0 2 * * *", "housekeeping_enabled": "0"})
    assert service.scheduler.get_job(service_module.HOUSEKEEPING_JOB_ID) is None

    service.update_editable_settings(
        {
            "housekeeping_form": "1",
            "housekeeping_enabled": "1",
            "housekeeping_schedule": "0 2 * * *",
            "housekeeping_day_tue": "1",
            "housekeeping_day_thu": "1",
            "housekeeping_time": "01:15",
        }
    )
    assert service.scheduler.get_job(service_module.HOUSEKEEPING_JOB_ID) is not None
    assert service._editable_settings["housekeeping_schedule"] == "15 1 * * tue,thu"


def test_system_page_renders_housekeeping_next_run_and_scheduler_summary(monkeypatch, tmp_path):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    monkeypatch.setenv("MOVIE_SCHEDULE", "0 2 * * *")
    monkeypatch.setenv("TZ", "UTC")

    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    service.update_editable_settings(
        {
            "housekeeping_form": "1",
            "housekeeping_enabled": "1",
            "housekeeping_schedule": "0 2 * * *",
            "housekeeping_day_mon": "1",
            "housekeeping_day_fri": "1",
            "housekeeping_time": "02:00",
        }
    )
    service.register_jobs()

    status_code, body, _ = _call_get(service, "/system")

    assert status_code == 200
    assert "Service / Scheduler Summary" in body
    assert "App Version" in body
    assert "Next Scheduled Job" in body
    assert "Next Scheduled Time" in body
    assert "Housekeeping Enabled" in body
    assert "Housekeeping Schedule" in body
    assert "Next Housekeeping Run" in body
    assert "Log Retention Days" in body
    assert "Save Housekeeping" not in body
    assert "Generated cron" not in body
    assert "Enable housekeeping scheduler" not in body


def test_analytics_page_and_aggregations_render(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))

    run_id = "run-analytics-1"
    _seed_run(
        db_path,
        library="movies",
        ts_end="2026-01-02T08:00:00",
        success_count=2,
        failed_count=0,
        skipped_count=0,
        saved_bytes=300,
    )
    _seed_encode(
        db_path,
        run_id=run_id,
        ts="2026-01-02T08:00:00",
        status="success",
        path="/movies/a.mkv",
        library="movies",
        size_before_bytes=1000,
        size_after_bytes=700,
        saved_bytes=300,
        duration_seconds=30,
    )
    _seed_encode(
        db_path,
        run_id="run-analytics-2",
        ts="2026-01-03T08:00:00",
        status="success",
        path="/tv/b.mkv",
        library="tv",
        size_before_bytes=2000,
        size_after_bytes=1400,
        saved_bytes=600,
        duration_seconds=22,
    )

    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))

    summary = service._analytics_overall_summary()
    assert summary["files_optimized"] == 2
    assert summary["total_saved"] == 900

    libs = service._analytics_library_breakdown()
    by_name = {row["library"].lower(): row for row in libs}
    assert by_name["movies"]["total_saved"] == 300
    assert by_name["tv"]["total_saved"] == 600

    top_files = service._analytics_top_savings_files(limit=2)
    assert top_files[0]["file"] == "/tv/b.mkv"
    assert top_files[0]["saved_bytes"] == 600

    status_code, body, _ = _call_get(service, "/analytics")
    assert status_code == 200
    assert "Overall Summary" in body
    assert "font-weight:800; font-size:1.1rem; color:#111827;" in body
    assert "Per-Library Breakdown" in body
    assert "Top Savings Files" in body
    assert 'title="/tv/b.mkv"' in body
    assert ">b<" in body
    assert "Best Next Opportunities" in body
    assert "Highest Potential Files" in body
    assert "<li title=\"/tv/b.mkv\">b</li>" in body


def test_top_savings_runs_calculation(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))

    _seed_run(db_path, library="movies", ts_end="2026-02-01T00:00:00", success_count=1, saved_bytes=1200)
    _seed_run(db_path, library="tv", ts_end="2026-02-02T00:00:00", success_count=2, saved_bytes=800)

    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    rows = service._analytics_top_savings_runs(limit=2)

    assert rows[0]["saved_bytes"] == 1200
    assert rows[0]["library"].lower() == "movies"
    assert rows[1]["saved_bytes"] == 800


def test_preview_savings_summary_calculation_and_render():
    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    snapshot = {
        "preview_results": [
            {"file": "a.mkv", "original_size": 1000, "estimated_size": 700, "estimated_savings_pct": "30.0", "decision": "Encode"},
            {"file": "b.mkv", "original_size": 1000, "estimated_size": 950, "estimated_savings_pct": "5.0", "decision": "Skip"},
        ],
        "preview_library": "movies",
        "preview_generated_at": "2026-01-01T00:00:00Z",
    }

    summary = service._preview_summary(snapshot["preview_results"])
    assert summary["files_evaluated"] == 2
    assert summary["candidates_found"] == 1
    assert summary["estimated_total_savings"] == 350

    html = service._preview_results_html(snapshot)
    assert "Preview Summary" in html
    assert "Estimated Total Savings" in html


def test_dashboard_summary_widget_render(tmp_path, monkeypatch):
    db_path = tmp_path / "chonk.db"
    monkeypatch.setenv("STATS_PATH", str(db_path))
    _seed_encode(
        db_path,
        run_id="run-dashboard-1",
        ts=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
        status="success",
        path="/movies/dashboard.mkv",
        library="movies",
        size_before_bytes=1000,
        size_after_bytes=800,
        saved_bytes=200,
    )

    service = ChonkService(ServiceSettings(enabled=True, host="0.0.0.0", port=8080, movie_schedule="", tv_schedule=""))
    status_code, body, _ = _call_get(service, "/dashboard")

    assert status_code == 200
    assert "Compact dashboard summary" in body
    assert "runtime-dashboard-total-saved" in body
    assert "Saved This Week" in body
