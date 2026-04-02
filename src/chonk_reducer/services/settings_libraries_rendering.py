from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Mapping, Sequence


@dataclass(frozen=True)
class SettingsLibrariesRenderDeps:
    load_template: Callable[[str], str]
    label_with_help: Callable[[str, str, str], str]
    ignored_folders_section_html: Callable[[object], str]
    schedule_form_state: Callable[[str], dict[str, object]]
    simple_schedule_time_options: Callable[[], Sequence[str]]
    env_bootstrap: Callable[[str, str], str]
    env_int: Callable[[str, int], int]
    normalize_csv_text: Callable[[str], str]
    escape_html: Callable[[str], str]
    sanitize_token: Callable[[str], str]
    weekday_choices: Sequence[tuple[str, str]]
    library_settings_help: Mapping[str, str]


_LIBRARY_TABLE_ROW_TEMPLATE = """<tr>
  <td class="libraries-table-cell">{name}</td>
  <td class="libraries-table-cell"><code>{path}</code></td>
  <td class="libraries-table-cell">{enabled}</td>
  <td class="libraries-table-cell">{priority}</td>
  <td class="libraries-table-cell"><code>{schedule}</code></td>
  <td class="libraries-table-cell">{actions}</td>
</tr>
<tr>
  <td colspan="6" class="libraries-table-detail-cell">
    <details>
      <summary>Edit {name}</summary>
      <form method="post" action="/settings/libraries/update" class="library-edit-form">
        <input type="hidden" name="library_id" value="{library_id}" />
        {name_path_fields_html}
        {common_sections_html}
      </form>
      {ignored_folders_html}
    </details>
  </td>
</tr>"""


def render_libraries_table_html(libraries: list[object], deps: SettingsLibrariesRenderDeps) -> str:
    empty_template = deps.load_template("partials/libraries_empty.html")
    table_template = deps.load_template("partials/libraries_table.html")
    common_sections_template = deps.load_template("partials/library_form_common_sections.html")
    table_actions_template = deps.load_template("partials/library_table_row_actions.html")
    if not libraries:
        return empty_template

    row_html: list[str] = []
    for library in libraries:
        schedule_state = deps.schedule_form_state(str(getattr(library, "schedule", "")))
        enabled = bool(getattr(library, "enabled", False))
        enabled_label = "Enabled" if enabled else "Disabled"
        toggle_target = "0" if enabled else "1"
        toggle_label = "Disable" if enabled else "Enable"
        library_id = int(getattr(library, "id", 0))
        row_html.append(
            _LIBRARY_TABLE_ROW_TEMPLATE.format(
                name=deps.escape_html(str(getattr(library, "name", ""))),
                path=deps.escape_html(str(getattr(library, "path", ""))),
                enabled=enabled_label,
                priority=deps.escape_html(str(getattr(library, "priority", ""))),
                schedule=deps.escape_html(str(getattr(library, "schedule", ""))),
                name_path_fields_html=render_library_name_path_fields_html(
                    deps,
                    name_label=deps.label_with_help("Name", deps.library_settings_help["name"], "lib-name-edit-%d" % library_id),
                    path_label=deps.label_with_help("Path", deps.library_settings_help["path"], "lib-path-edit-%d" % library_id),
                    name_value=deps.escape_html(str(getattr(library, "name", ""))),
                    path_value=deps.escape_html(str(getattr(library, "path", ""))),
                ),
                common_sections_html=common_sections_template.format(
                    min_size_gb_label=deps.label_with_help("Minimum File Size (GB)", deps.library_settings_help["min_size_gb"], "lib-min-size-edit-%d" % library_id),
                    max_files_label=deps.label_with_help("Max Files Per Run", deps.library_settings_help["max_files"], "lib-max-files-edit-%d" % library_id),
                    priority_label=deps.label_with_help("Priority", deps.library_settings_help["priority"], "lib-priority-edit-%d" % library_id),
                    qsv_quality_label=deps.label_with_help("QSV Quality", deps.library_settings_help["qsv_quality"], "lib-qsv-quality-edit-%d" % library_id),
                    qsv_preset_label=deps.label_with_help("QSV Preset", deps.library_settings_help["qsv_preset"], "lib-qsv-preset-edit-%d" % library_id),
                    min_savings_percent_label=deps.label_with_help("Minimum Savings Percent", deps.library_settings_help["min_savings_percent"], "lib-min-savings-edit-%d" % library_id),
                    max_savings_percent_label=deps.label_with_help("Maximum Savings Percent", deps.library_settings_help["max_savings_percent"], "lib-max-savings-edit-%d" % library_id),
                    skip_codecs_label=deps.label_with_help("Skip Codecs", deps.library_settings_help["skip_codecs"], "lib-skip-codecs-edit-%d" % library_id),
                    skip_min_height_label=deps.label_with_help("Skip Minimum Height", deps.library_settings_help["skip_min_height"], "lib-skip-min-height-edit-%d" % library_id),
                    skip_resolution_tags_label=deps.label_with_help("Skip Resolution Tags", deps.library_settings_help["skip_resolution_tags"], "lib-skip-resolution-tags-edit-%d" % library_id),
                    min_size_gb=deps.escape_html("%s" % getattr(library, "min_size_gb", "")),
                    max_files=deps.escape_html(str(getattr(library, "max_files", ""))),
                    priority=deps.escape_html(str(getattr(library, "priority", ""))),
                    qsv_quality=deps.escape_html(str(getattr(library, "qsv_quality", None) if getattr(library, "qsv_quality", None) is not None else deps.env_bootstrap("QSV_QUALITY", "21"))),
                    qsv_preset=deps.escape_html(str(getattr(library, "qsv_preset", None) if getattr(library, "qsv_preset", None) is not None else deps.env_bootstrap("QSV_PRESET", "7"))),
                    min_savings_percent=deps.escape_html(str(getattr(library, "min_savings_percent", None) if getattr(library, "min_savings_percent", None) is not None else deps.env_bootstrap("MIN_SAVINGS_PERCENT", "15"))),
                    max_savings_percent=deps.escape_html(str(getattr(library, "max_savings_percent", "")) if getattr(library, "max_savings_percent", None) is not None else ""),
                    skip_codecs=deps.escape_html(str(getattr(library, "skip_codecs", "") or "")),
                    skip_min_height=deps.escape_html(str(max(0, int(getattr(library, "skip_min_height", 0) or 0)))),
                    skip_resolution_tags=deps.escape_html(str(getattr(library, "skip_resolution_tags", "") or "")),
                    schedule_fields=render_schedule_fields_html(deps, schedule_state=schedule_state, form_id="edit-%d" % library_id),
                    enabled_label=deps.label_with_help("Enabled", deps.library_settings_help["enabled"], "lib-enabled-edit-%d" % library_id),
                    enabled_yes="selected" if enabled else "",
                    enabled_no="selected" if not enabled else "",
                    submit_text="Save Library",
                ),
                ignored_folders_html=deps.ignored_folders_section_html(library),
                library_id=library_id,
                actions=table_actions_template.format(
                    library_id=library_id,
                    toggle_target=toggle_target,
                    toggle_label=toggle_label,
                ),
            )
        )
    return table_template.format(library_rows_html="".join(row_html))


def render_library_create_form_html(deps: SettingsLibrariesRenderDeps) -> str:
    create_form_template = deps.load_template("partials/library_create_form.html")
    schedule_state = deps.schedule_form_state("")
    schedule_fields = render_schedule_fields_html(deps, schedule_state, "create")
    common_sections_template = deps.load_template("partials/library_form_common_sections.html")
    name_path_fields_html = render_library_name_path_fields_html(
        deps,
        name_label=deps.label_with_help("Name", deps.library_settings_help["name"], "lib-name-create"),
        path_label=deps.label_with_help("Path", deps.library_settings_help["path"], "lib-path-create"),
        name_value="",
        path_value="",
    )
    return create_form_template.format(
        name_path_fields_html=name_path_fields_html,
        common_sections_html=common_sections_template.format(
            min_size_gb_label=deps.label_with_help("Minimum File Size (GB)", deps.library_settings_help["min_size_gb"], "lib-min-size-create"),
            max_files_label=deps.label_with_help("Max Files Per Run", deps.library_settings_help["max_files"], "lib-max-files-create"),
            priority_label=deps.label_with_help("Priority", deps.library_settings_help["priority"], "lib-priority-create"),
            qsv_quality_label=deps.label_with_help("QSV Quality", deps.library_settings_help["qsv_quality"], "lib-qsv-quality-create"),
            qsv_preset_label=deps.label_with_help("QSV Preset", deps.library_settings_help["qsv_preset"], "lib-qsv-preset-create"),
            min_savings_percent_label=deps.label_with_help("Minimum Savings Percent", deps.library_settings_help["min_savings_percent"], "lib-min-savings-create"),
            max_savings_percent_label=deps.label_with_help("Maximum Savings Percent", deps.library_settings_help["max_savings_percent"], "lib-max-savings-create"),
            skip_codecs_label=deps.label_with_help("Skip Codecs", deps.library_settings_help["skip_codecs"], "lib-skip-codecs-create"),
            skip_min_height_label=deps.label_with_help("Skip Minimum Height", deps.library_settings_help["skip_min_height"], "lib-skip-min-height-create"),
            skip_resolution_tags_label=deps.label_with_help("Skip Resolution Tags", deps.library_settings_help["skip_resolution_tags"], "lib-skip-resolution-tags-create"),
            min_size_gb="0.0",
            max_files="1",
            priority="100",
            qsv_quality=deps.escape_html(deps.env_bootstrap("QSV_QUALITY", "21")),
            qsv_preset=deps.escape_html(deps.env_bootstrap("QSV_PRESET", "7")),
            min_savings_percent=deps.escape_html(deps.env_bootstrap("MIN_SAVINGS_PERCENT", "15")),
            max_savings_percent="",
            skip_codecs=deps.escape_html(deps.normalize_csv_text(deps.env_bootstrap("SKIP_CODECS", ""))),
            skip_min_height=deps.escape_html(str(max(0, deps.env_int("SKIP_MIN_HEIGHT", 0)))),
            skip_resolution_tags=deps.escape_html(deps.normalize_csv_text(deps.env_bootstrap("SKIP_RESOLUTION_TAGS", ""))),
            schedule_fields=schedule_fields,
            enabled_label=deps.label_with_help("Enabled", deps.library_settings_help["enabled"], "lib-enabled-create"),
            enabled_yes="selected",
            enabled_no="",
            submit_text="Create Library",
        ),
    )


def render_library_name_path_fields_html(
    deps: SettingsLibrariesRenderDeps,
    *,
    name_label: str,
    path_label: str,
    name_value: str,
    path_value: str,
) -> str:
    fields_template = deps.load_template("partials/library_form_name_path_fields.html")
    return fields_template.format(
        name_label=name_label,
        path_label=path_label,
        name_value=name_value,
        path_value=path_value,
    )


def render_schedule_fields_html(deps: SettingsLibrariesRenderDeps, schedule_state: dict[str, object], form_id: str) -> str:
    schedule_template = deps.load_template("partials/library_schedule_fields.html")
    form_token = deps.sanitize_token(str(form_id))
    mode = str(schedule_state.get("mode", "simple"))
    raw_value = deps.escape_html(str(schedule_state.get("raw", "")))
    simple_time = deps.escape_html(str(schedule_state.get("time", "00:00")))
    selected_days = set(_safe_str_values(schedule_state.get("days", [])))
    simple_radio_id = "schedule-mode-simple-%s" % form_token
    advanced_radio_id = "schedule-mode-advanced-%s" % form_token
    simple_checked = "checked" if mode == "simple" else ""
    advanced_checked = "checked" if mode == "advanced" else ""

    weekday_options: list[str] = []
    for label, day_value in deps.weekday_choices:
        checked = "checked" if day_value in selected_days else ""
        weekday_options.append(
            '<label class="library-schedule-weekday"><input type="checkbox" name="schedule_day_%s" value="1" %s /> %s</label>'
            % (day_value, checked, label)
        )

    time_options: list[str] = []
    for value in deps.simple_schedule_time_options():
        selected = "selected" if value == simple_time else ""
        time_options.append('<option value="%s" %s>%s</option>' % (deps.escape_html(value), selected, deps.escape_html(value)))

    simple_display = "block" if mode == "simple" else "none"
    advanced_display = "block" if mode == "advanced" else "none"
    preview = deps.escape_html(str(schedule_state.get("preview", "")))

    return schedule_template.format(
        schedule_label=deps.label_with_help("Schedule", deps.library_settings_help["schedule"], "lib-schedule-%s" % form_token),
        simple_radio_id=simple_radio_id,
        simple_checked=simple_checked,
        advanced_radio_id=advanced_radio_id,
        advanced_checked=advanced_checked,
        form_token=form_token,
        simple_display=simple_display,
        days_label=deps.label_with_help("Days", deps.library_settings_help["schedule_days"], "lib-schedule-days-%s" % form_token),
        weekday_options_html="".join(weekday_options),
        time_label=deps.label_with_help("Time", deps.library_settings_help["schedule_time"], "lib-schedule-time-%s" % form_token),
        time_options_html="".join(time_options),
        preview=preview,
        advanced_display=advanced_display,
        raw_cron_label=deps.label_with_help("Raw cron expression", deps.library_settings_help["raw_cron"], "lib-schedule-raw-%s" % form_token),
        raw_value=raw_value,
    )


def _safe_str_values(values: object) -> Iterable[str]:
    if not isinstance(values, (list, tuple, set)):
        return []
    return [str(value) for value in values]
