from __future__ import annotations

from urllib.parse import quote

try:
    from fastapi import Request  # type: ignore
except Exception:  # pragma: no cover - fallback runtime
    class Request:  # type: ignore[no-redef]
        pass

from ... import notifications


def register_action_routes(service, JSONResponse, RedirectResponse) -> None:
    @service.app.post("/settings")
    async def save_settings(request: Request):
        values = await service._request_form_values(request)
        normalized = service._normalize_settings_updates(values)
        service.update_editable_settings(normalized)
        return service._html_response(service.settings_page_html(service.settings_saved_message(normalized)))

    @service.app.post("/settings/test-notification")
    def test_notification():
        result = notifications.send_test_notification(settings_db_path=str(service._settings_db_path))
        if result.get("ok"):
            service._record_activity("notification_test", str(result.get("message", "Test notification sent.")))
        else:
            service._record_activity("notification_test_failed", str(result.get("message", "Test notification failed.")))
        return service._html_response(service.settings_page_html(str(result.get("message", ""))))

    @service.app.post("/settings/libraries/create")
    async def create_library(request: Request):
        values = await service._request_form_values(request)
        return service._html_response(service.settings_page_html(service.create_library(values)))

    @service.app.post("/settings/libraries/update")
    async def update_library(request: Request):
        values = await service._request_form_values(request)
        return service._html_response(service.settings_page_html(service.update_library(values)))

    @service.app.post("/settings/libraries/delete")
    async def delete_library(request: Request):
        values = await service._request_form_values(request)
        return service._html_response(service.settings_page_html(service.delete_library(values)))

    @service.app.post("/settings/libraries/toggle")
    async def toggle_library(request: Request):
        values = await service._request_form_values(request)
        return service._html_response(service.settings_page_html(service.toggle_library(values)))

    @service.app.post("/settings/libraries/ignored/add")
    async def add_ignored_folder(request: Request):
        values = await service._request_form_values(request)
        return service._html_response(service.settings_page_html(service.add_ignored_folder(values)))

    @service.app.post("/settings/libraries/ignored/remove")
    async def remove_ignored_folder(request: Request):
        values = await service._request_form_values(request)
        return service._html_response(service.settings_page_html(service.remove_ignored_folder(values)))

    @service.app.get("/api/library/{library_id}/folders")
    def api_library_folders(library_id: int, request: Request):
        relative_path = ""
        if request is not None and hasattr(request, "query_params"):
            relative_path = str(request.query_params.get("path", ""))
        payload, status_code = service.library_folders_payload(int(library_id), relative_path)
        if JSONResponse is not None:
            return JSONResponse(content=payload, status_code=status_code)
        return payload

    @service.app.get("/health")
    def health() -> dict:
        return service.health_payload()

    @service.app.get("/api/status")
    def api_status() -> dict:
        return service.current_job_status()

    @service.app.post("/api/run/cancel")
    def api_cancel_run():
        payload = service.request_cancel_active_run()
        if JSONResponse is not None:
            return JSONResponse(content=payload, status_code=200)
        return payload

    @service.app.post("/api/preview/clear")
    def api_clear_preview():
        payload = service.clear_preview_results()
        if JSONResponse is not None:
            return JSONResponse(content=payload, status_code=200)
        return payload

    @service.app.post("/libraries/{library_id}/run")
    def run_library(library_id: int):
        payload, status_code = service.manual_run_payload_for_id(int(library_id))
        if JSONResponse is not None:
            return JSONResponse(content=payload, status_code=status_code)
        return payload

    @service.app.post("/libraries/{library_id}/preview")
    def preview_library(library_id: int):
        payload, status_code = service.manual_preview_payload_for_id(int(library_id))
        if JSONResponse is not None:
            return JSONResponse(content=payload, status_code=status_code)
        return payload

    @service.app.post("/dashboard/libraries/{library_id}/run")
    def run_library_from_dashboard(library_id: int):
        payload, _ = service.manual_run_payload_for_id(int(library_id))
        location = "/dashboard"
        if payload.get("status") in ("queued", "busy"):
            location = "/dashboard?manual_run=%s&library_id=%s" % (
                quote(str(payload.get("status", ""))),
                quote(str(payload.get("library_id", ""))),
            )
        if RedirectResponse is not None:
            return RedirectResponse(url=location, status_code=303)
        return service._html_response(service.home_page_html())

    @service.app.post("/dashboard/libraries/{library_id}/preview")
    def preview_library_from_dashboard(library_id: int):
        payload, _ = service.manual_preview_payload_for_id(int(library_id))
        location = "/dashboard"
        if payload.get("status") in ("queued", "busy"):
            location = "/dashboard?manual_run=%s&library_id=%s" % (
                quote(str(payload.get("status", ""))),
                quote(str(payload.get("library_id", ""))),
            )
        if RedirectResponse is not None:
            return RedirectResponse(url=location, status_code=303)
        return service._html_response(service.home_page_html())

    @service.app.post("/run/movies")
    def run_movies():
        payload, status_code = service.manual_run_payload("movies")
        if JSONResponse is not None:
            return JSONResponse(content=payload, status_code=status_code)
        return payload

    @service.app.post("/run/tv")
    def run_tv():
        payload, status_code = service.manual_run_payload("tv")
        if JSONResponse is not None:
            return JSONResponse(content=payload, status_code=status_code)
        return payload
