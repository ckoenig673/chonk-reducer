from __future__ import annotations


def register_page_routes(service) -> None:
    @service.app.get("/static/css/base.css")
    def static_base_css():
        return service._static_asset_response("css/base.css", media_type="text/css")

    @service.app.get("/static/js/dashboard_runtime.js")
    def static_dashboard_runtime_js():
        return service._static_asset_response("js/dashboard_runtime.js", media_type="application/javascript")

    @service.app.get("/")
    def home():
        return service._html_response(service.home_page_html())

    @service.app.get("/dashboard")
    def dashboard():
        return service._html_response(service.home_page_html())

    @service.app.get("/favicon.ico")
    def favicon():
        return service._no_content_response()

    @service.app.get("/runs")
    def runs_page():
        return service._html_response(service.runs_page_html())

    @service.app.get("/runs/{run_id}")
    def run_detail_page(run_id: str):
        html, status_code = service.run_detail_page_html(run_id)
        return service._html_response(html, status_code=status_code)

    @service.app.get("/activity")
    def activity_page():
        return service._html_response(service.activity_page_html())

    @service.app.get("/history")
    def history_page():
        return service._html_response(service.history_page_html())

    @service.app.get("/analytics")
    def analytics_page():
        return service._html_response(service.analytics_page_html())

    @service.app.get("/system")
    def system_page():
        return service._html_response(service.system_page_html())

    @service.app.get("/settings")
    def settings_page():
        return service._html_response(service.settings_page_html())
