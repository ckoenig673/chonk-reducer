from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Callable, Mapping, Optional, Sequence


@dataclass(frozen=True)
class DashboardRenderDeps:
    load_template: Callable[[str], str]
    escape_html: Callable[[str], str]
    format_saved_bytes: Callable[[object], str]
    run_saved_mb_gb_label: Callable[[object], str]
    format_readable_timestamp: Callable[[object], str]
    format_duration_seconds: Callable[[object], str]
    duration_seconds_from_run: Callable[[object, object, object], object]
    display_run_trigger: Callable[[str], str]
    display_run_mode: Callable[[str], str]
    runtime_status_snapshot: Callable[[], dict[str, object]]
    latest_run_status: Callable[[str], Optional[dict[str, object]]]
    next_run_label: Callable[[object], str]
    library_runtime_status: Callable[[object], str]
    library_runtime_summary: Callable[[object], str]
    scheduler_running_label: Callable[[], str]
    next_global_scheduled_job_label: Callable[[], str]
    next_housekeeping_run_label: Callable[[], str]
    analytics_overall_summary: Callable[[], dict[str, object]]
    runtime_status_html: Callable[..., str]
    preview_results_html: Callable[[dict[str, object]], str]
    render_shell_html: Callable[[str, str], str]
    run_total_saved_bytes: Callable[[str, int], int]
    run_successful_encode_count: Callable[[str, int], int]
    key_value_table_html: Callable[[Sequence[tuple[str, str]]], str]


def render_home_page_html(
    *,
    libraries: Sequence[object],
    lifetime_savings: Optional[Mapping[str, int]],
    library_totals: Mapping[str, Mapping[str, int]],
    deps: DashboardRenderDeps,
) -> str:
    dashboard_library_template = deps.load_template("partials/dashboard_library_card.html")
    dashboard_empty_template = deps.load_template("partials/dashboard_libraries_empty.html")
    dashboard_status_template = deps.load_template("partials/dashboard_system_status.html")
    dashboard_page_template = deps.load_template("dashboard.html")

    library_sections: list[str] = []
    for library in libraries:
        status = deps.latest_run_status(str(getattr(library, "name", "")))
        runtime_status = "Disabled"
        runtime_summary = ""
        if bool(getattr(library, "enabled", False)):
            runtime_status = deps.library_runtime_status(library)
            runtime_summary = deps.library_runtime_summary(library)

        last_run_label = "Never"
        processed_label = "0"
        savings_label = "0 B"
        totals = library_totals.get(str(getattr(library, "name", "")).strip().lower(), {"files_optimized": 0, "total_saved": 0})
        if status is not None:
            last_run_label = str(status.get("ts_end") or status.get("ts_start") or "Unknown")
            processed_label = str(status.get("processed_count") or 0)
            savings_label = deps.format_saved_bytes(status.get("saved_bytes"))

        library_sections.append(
            dashboard_library_template.format(
                library_name=deps.escape_html(str(getattr(library, "name", ""))),
                library_path=deps.escape_html(str(getattr(library, "path", ""))),
                runtime_status=deps.escape_html(runtime_status),
                library_priority=deps.escape_html(str(getattr(library, "priority", ""))),
                last_run_label=deps.escape_html(last_run_label),
                next_run_label=deps.escape_html(deps.next_run_label(library)),
                files_optimized=deps.escape_html(str(totals.get("files_optimized", 0))),
                total_saved=deps.escape_html(deps.format_saved_bytes(totals.get("total_saved", 0))),
                recent_savings=deps.escape_html(savings_label),
                processed_count=deps.escape_html(processed_label),
                runtime_summary=runtime_summary,
                library_id=int(getattr(library, "id", 0)),
            )
        )

    if not library_sections:
        library_sections.append(dashboard_empty_template)

    analytics_summary = deps.analytics_overall_summary()
    system_status_html = dashboard_status_template.format(
        total_saved=deps.escape_html(deps.format_saved_bytes((lifetime_savings or {}).get("total_saved", 0))),
        files_optimized=deps.escape_html(str((lifetime_savings or {}).get("files_optimized", 0))),
        saved_this_week=deps.escape_html(deps.format_saved_bytes(analytics_summary.get("saved_this_week", 0))),
        saved_this_month=deps.escape_html(deps.format_saved_bytes(analytics_summary.get("saved_this_month", 0))),
        scheduler_label=deps.scheduler_running_label(),
        next_library_run=deps.next_global_scheduled_job_label(),
        next_housekeeping_run=deps.next_housekeeping_run_label(),
    )
    content = dashboard_page_template.format(
        system_status_html=system_status_html,
        library_sections_html="".join(library_sections),
        runtime_status_html=deps.runtime_status_html(include_preview=False),
        preview_results_html=deps.preview_results_html(deps.runtime_status_snapshot()),
    )
    return deps.render_shell_html("Dashboard", content)


def render_runs_active_run_banner_html(*, deps: DashboardRenderDeps) -> str:
    snapshot = deps.runtime_status_snapshot()
    if snapshot.get("status") not in {"Running", "Cancelling"}:
        return ""

    library = str(snapshot.get("current_library") or "").strip() or "Unknown Library"
    current_file = str(snapshot.get("current_file") or "").strip()
    if current_file:
        file_name = os.path.basename(current_file)
        if "." in file_name:
            file_name = file_name.rsplit(".", 1)[0]
        active_label = "%s — %s" % (library, file_name)
    else:
        active_label = library

    runs_banner_template = deps.load_template("partials/runs_active_run_banner.html")
    return runs_banner_template.format(active_label=deps.escape_html(active_label))


def render_recent_runs_html(rows: Sequence[Mapping[str, str]]) -> str:
    if not rows:
        return '<div class="common-bordered-message">No recent runs recorded yet.</div>'

    row_html = []
    for row in rows:
        row_html.append(
            "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
            % (row["time"], row["library"], row["status"], row["duration"], row["saved"])
        )

    return """<div class="table-frame"><table class="data-table">
  <thead>
    <tr>
      <th>Time</th>
      <th>Library</th>
      <th>Status</th>
      <th>Duration</th>
      <th>Saved</th>
    </tr>
  </thead>
  <tbody>
    %s
  </tbody>
</table></div>""" % "".join(row_html)


def render_runs_history_html(rows: Sequence[Mapping[str, str]], *, deps: DashboardRenderDeps) -> str:
    if not rows:
        return '<div class="common-bordered-message">No runs recorded yet.</div>'

    row_html = []
    for row in rows:
        run_id = deps.escape_html(row["run_id"])
        row_html.append(
            "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
            % (
                deps.escape_html(row["time"]),
                deps.escape_html(row["library"]),
                deps.escape_html(row["mode"]),
                deps.escape_html(row["result"]),
                deps.escape_html(row["processed"]),
                deps.escape_html(row["skipped"]),
                deps.escape_html(row["failed"]),
                deps.escape_html(row["saved"]),
                deps.escape_html(row["duration"]),
                '<a href="/runs/%s">%s</a>' % (run_id, run_id),
            )
        )

    return """<div class="table-frame"><table class="data-table">
  <thead>
    <tr>
      <th>Time</th>
      <th>Library</th>
      <th>Mode</th>
      <th>Result</th>
      <th>Processed</th>
      <th>Skipped</th>
      <th>Failed</th>
      <th>Saved</th>
      <th>Duration</th>
      <th>Run ID</th>
    </tr>
  </thead>
  <tbody>
    %s
  </tbody>
</table></div>""" % "".join(row_html)


def render_run_summary_html(run: Mapping[str, object], *, deps: DashboardRenderDeps) -> str:
    summary_rows = [
        ("Run ID", deps.escape_html(str(run.get("run_id") or "-"))),
        ("Library", deps.escape_html(str(run.get("library") or "Unknown"))),
        ("Trigger Type", deps.escape_html(deps.display_run_trigger(str(run.get("trigger_type") or "")))),
        ("Mode", deps.escape_html(deps.display_run_mode(str(run.get("mode") or "")))),
        ("Started At", deps.escape_html(deps.format_readable_timestamp(run.get("ts_start")))),
        ("Completed At", deps.escape_html(deps.format_readable_timestamp(run.get("ts_end")))),
        (
            "Duration",
            deps.escape_html(
                deps.format_duration_seconds(
                    deps.duration_seconds_from_run(run.get("ts_start"), run.get("ts_end"), run.get("duration_seconds"))
                )
            ),
        ),
    ]
    outcome_rows = [
        ("Result", deps.escape_html(str(run.get("result") or "completed"))),
        ("Cancellation", "Cancelled" if str(run.get("was_cancelled") or "0") == "1" else "Not Cancelled"),
        ("Retry Attempts", "Not recorded"),
    ]
    count_rows = [
        ("Candidates Found", deps.escape_html(str(run.get("candidates_found") or 0))),
        ("Evaluated", deps.escape_html(str(run.get("evaluated_count") or 0))),
        ("Processed", deps.escape_html(str(run.get("processed_count") or 0))),
        ("Success", deps.escape_html(str(run.get("success_count") or 0))),
        ("Skipped", deps.escape_html(str(run.get("skipped_count") or 0))),
        ("Failed", deps.escape_html(str(run.get("failed_count") or 0))),
    ]
    total_saved_bytes = deps.run_total_saved_bytes(str(run.get("run_id") or ""), int(run.get("saved_bytes") or 0))
    successful_encodes = deps.run_successful_encode_count(str(run.get("run_id") or ""), int(run.get("success_count") or 0))
    average_saved_bytes = int(total_saved_bytes / successful_encodes) if successful_encodes > 0 else 0
    savings_rows = [
        ("Total Saved", deps.escape_html(deps.run_saved_mb_gb_label(total_saved_bytes))),
        (
            "Avg Saved / File",
            deps.escape_html(deps.run_saved_mb_gb_label(average_saved_bytes) if successful_encodes > 0 else "0.0 MB"),
        ),
    ]

    optional_rows: list[tuple[str, str]] = []
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
            optional_rows.append((label, deps.escape_html(str(run.get(key) or 0))))

    return "".join(
        [
            '<h2 style="margin-top: 1rem;">Run Summary</h2>%s' % deps.key_value_table_html(summary_rows),
            '<h2 style="margin-top: 1rem;">Outcome</h2>%s' % deps.key_value_table_html(outcome_rows),
            '<h2 style="margin-top: 1rem;">Counts</h2>%s' % deps.key_value_table_html(count_rows),
            '<h2 style="margin-top: 1rem;">Savings</h2>%s' % deps.key_value_table_html(savings_rows),
            '<h2 style="margin-top: 1rem;">Related Information</h2>%s'
            % deps.key_value_table_html(optional_rows or [("Details", "No additional summary counters recorded.")]),
        ]
    )


def render_related_run_info_html(run: Mapping[str, object], *, deps: DashboardRenderDeps) -> str:
    raw_log_path = str(run.get("raw_log_path") or "").strip()
    run_log_value = deps.escape_html(raw_log_path) if raw_log_path else "No raw log path recorded for this run."
    mode_value = deps.escape_html(deps.display_run_mode(str(run.get("mode") or "")))
    result_value = deps.escape_html(str(run.get("result") or "completed"))
    if str(run.get("mode") or "").strip().lower() in ("preview", "dry_run", "dry-run"):
        result_value = "Preview-only (no files encoded)"
    rows = [
        ("Run Log Path", run_log_value),
        ("Preview vs Live", mode_value),
        ("Preview vs Encode Result", result_value),
    ]
    return '<h2 style="margin-top: 1rem;">Run Logs and Distinctions</h2>%s' % deps.key_value_table_html(rows)


def render_run_file_summary_html(rows: Sequence[Mapping[str, str]], *, deps: DashboardRenderDeps) -> str:
    total = len(rows)
    if total == 0:
        return '<div style="padding: 0.5rem; border: 1px solid #ddd;">No file-level entries recorded for this run.</div>'
    skipped_reasons = sorted(
        {
            str(row.get("reason") or "-")
            for row in rows
            if str(row.get("status") or "").lower() == "skipped" and str(row.get("reason") or "-") != "-"
        }
    )
    failure_reasons = sorted(
        {
            str(row.get("reason") or "-")
            for row in rows
            if str(row.get("status") or "").lower() == "failed" and str(row.get("reason") or "-") != "-"
        }
    )
    rows_data = [
        ("Total File Entries", deps.escape_html(str(total))),
        ("Skip Reasons", deps.escape_html(", ".join(skipped_reasons) if skipped_reasons else "None recorded")),
        ("Failure Reasons", deps.escape_html(", ".join(failure_reasons) if failure_reasons else "None recorded")),
    ]
    return '<h2 style="margin-top: 1rem;">File List Summary</h2>%s' % deps.key_value_table_html(rows_data)


def render_run_encodes_html(rows: Sequence[Mapping[str, str]], *, deps: DashboardRenderDeps) -> str:
    if not rows:
        return render_run_file_summary_html(rows, deps=deps)

    body_rows = []
    for row in rows:
        body_rows.append(
            "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
            % (
                deps.escape_html(row["path"]),
                deps.escape_html(row["status"]),
                deps.escape_html(row["codec_info"]),
                deps.escape_html(row["before"]),
                deps.escape_html(row["after"]),
                deps.escape_html(row["saved"]),
                deps.escape_html(row["encode_duration"]),
                deps.escape_html(row["reason"]),
            )
        )

    return render_run_file_summary_html(rows, deps=deps) + """<div class="table-frame"><table class="data-table">
  <thead>
    <tr>
      <th>Path</th>
      <th>Status</th>
      <th>Codec</th>
      <th>Before</th>
      <th>After</th>
      <th>Saved</th>
      <th>Encode Time</th>
      <th>Reason / Detail</th>
    </tr>
  </thead>
  <tbody>
    %s
  </tbody>
</table></div>""" % "".join(body_rows)


def render_recent_activity_html(rows: Sequence[Mapping[str, str]], *, deps: DashboardRenderDeps) -> str:
    if not rows:
        return '<div class="common-bordered-message">No recent activity recorded yet.</div>'

    row_html = []
    for row in rows:
        run_id = row["run_id"]
        run_id_html = "-"
        if run_id:
            escaped_run_id = deps.escape_html(run_id)
            if str(row.get("run_exists") or "0") == "1":
                run_id_html = '<a href="/runs/%s">%s</a>' % (escaped_run_id, escaped_run_id)
            else:
                run_id_html = "%s <span class=\"table-inline-note\">(run unavailable)</span>" % escaped_run_id
        row_html.append(
            "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>"
            % (
                deps.escape_html(row["ts"]),
                deps.escape_html(row["library"]),
                deps.escape_html(row["event_type"]),
                deps.escape_html(row["message"]),
                run_id_html,
            )
        )

    return """<div class="table-frame"><table class="data-table">
  <thead>
    <tr>
      <th>Timestamp</th>
      <th>Library</th>
      <th>Event Type</th>
      <th>Message</th>
      <th>Run ID</th>
    </tr>
  </thead>
  <tbody>
    %s
  </tbody>
</table></div>""" % "".join(row_html)
