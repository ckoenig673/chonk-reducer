(function () {
  function textValue(value, placeholder) {
    if (value === null || value === undefined || String(value).trim() === "") {
      return placeholder;
    }
    return String(value);
  }

  function setText(id, value, placeholder) {
    var node = document.getElementById(id);
    if (!node) {
      return;
    }
    node.textContent = textValue(value, placeholder);
  }

  function savedBytesLabel(rawValue) {
    var parsed = parseInt(rawValue, 10);
    if (isNaN(parsed) || parsed <= 0) {
      return "-";
    }
    var units = ["B", "KB", "MB", "GB", "TB"];
    var scaled = parsed;
    var index = 0;
    while (scaled >= 1024 && index < units.length - 1) {
      scaled = scaled / 1024;
      index += 1;
    }
    if (index === 0) {
      return String(parsed) + " B";
    }
    return scaled.toFixed(1) + " " + units[index];
  }

  function schedulerTimestampLabel(rawValue) {
    var text = String(rawValue || "").trim();
    if (!text || text === "-") {
      return "-";
    }
    if (text.indexOf("T") !== -1) {
      text = text.replace("T", " ");
    }
    if (text.endsWith("Z")) {
      text = text.slice(0, -1);
    }
    if (text.length >= 16) {
      return text.slice(0, 16);
    }
    return text;
  }

  function triggerLabel(trigger) {
    var value = String(trigger || "").trim().toLowerCase();
    if (value === "manual") {
      return "Manual";
    }
    if (value === "schedule" || value === "scheduled") {
      return "Scheduled";
    }
    return textValue(trigger, "-");
  }

  function parseCount(value) {
    var parsed = parseInt(value, 10);
    if (isNaN(parsed) || parsed < 0) {
      return 0;
    }
    return parsed;
  }

  function formatEtaSeconds(rawValue) {
    var seconds = parseInt(rawValue, 10);
    if (isNaN(seconds) || seconds < 0) {
      return "-";
    }
    if (seconds < 60) {
      return String(seconds) + "s";
    }
    var minutes = Math.floor(seconds / 60);
    var rem = seconds % 60;
    return String(minutes) + "m " + String(rem) + "s";
  }

  function progressMarkup(snapshot) {
    if (String(snapshot.status || "") !== "Running") {
      return "";
    }
    var processed = parseCount(snapshot.files_processed);
    var candidates = parseCount(snapshot.candidates_found);
    var ratio = 0;
    var progressLabel = "";
    if (candidates > 0) {
      ratio = Math.min(1, processed / candidates);
      progressLabel = String(processed) + " / " + String(candidates) + " files processed";
    } else {
      ratio = processed > 0 ? 1 : 0;
      progressLabel = String(processed) + " files processed";
    }
    var pctLabel = String(Math.round(ratio * 100)) + "%";
    var encodePercent = parseFloat(snapshot.encode_percent);
    if (!isNaN(encodePercent)) {
      pctLabel = String(Math.round(Math.max(0, Math.min(100, encodePercent)))) + "%";
    }
    var currentFile = textValue(snapshot.current_file, "Waiting for first file");
    var encodeSpeed = textValue(snapshot.encode_speed, "-");
    var encodeEta = formatEtaSeconds(snapshot.encode_eta);
    var retryAttempt = parseInt(snapshot.retry_attempt, 10);
    var retryMax = parseInt(snapshot.retry_max, 10);
    var retryMarkup = '';
    if (!isNaN(retryAttempt) && !isNaN(retryMax) && retryAttempt > 0 && retryAttempt <= retryMax) {
      retryMarkup = '<div><strong>Retry Attempt:</strong> ' + String(retryAttempt) + ' / ' + String(retryMax) + '</div>';
    }
    return '' +
      '<div style="margin-top:0.75rem; padding:0.6rem; border:1px solid #d7e2f4; background:#f8fbff;">' +
      '<div style="font-weight:600; margin-bottom:0.35rem;">Run Progress</div>' +
      '<div style="border:1px solid #c8d8f0; background:#eef4ff; width:100%; height:18px;">' +
      '<div style="background:#2a6fd6; width:' + pctLabel + '; height:100%;"></div>' +
      '</div>' +
      '<div style="margin-top:0.35rem;">' + progressLabel + ' (' + pctLabel + ')</div>' +
      '<div style="margin-top:0.35rem;"><strong>Percent Complete:</strong> ' + pctLabel + '</div>' +
      '<div><strong>Speed:</strong> ' + encodeSpeed + '</div>' +
      '<div><strong>ETA:</strong> ' + encodeEta + '</div>' +
      retryMarkup +
      '<div style="margin-top:0.55rem;"><strong>Current Library:</strong> ' + textValue(snapshot.current_library, "-") + '</div>' +
      '<div><strong>Current File:</strong> ' + currentFile + '</div>' +
      '<div style="margin-top:0.4rem;"><strong>Files Evaluated:</strong> ' + String(parseCount(snapshot.files_evaluated)) + '</div>' +
      '<div><strong>Files Processed:</strong> ' + String(processed) + '</div>' +
      '<div><strong>Files Skipped:</strong> ' + String(parseCount(snapshot.files_skipped)) + '</div>' +
      '<div><strong>Files Failed:</strong> ' + String(parseCount(snapshot.files_failed)) + '</div>' +
      '<div><strong>Total Saved:</strong> ' + textValue(savedBytesLabel(snapshot.bytes_saved), "0 B") + '</div>' +
      '</div>';
  }

  function schedulerStateLabel(snapshot) {
    var scheduler = snapshot.scheduler || {};
    if (scheduler.paused) {
      return "Paused";
    }
    return scheduler.running ? "Running" : "Stopped";
  }

  function nextLibraryRunLabel(snapshot) {
    var library = String(snapshot.next_scheduled_job || "").trim();
    var time = String(snapshot.next_scheduled_time || "").trim();
    if (!library || library === "-" || !time || time === "-") {
      return "-";
    }
    return library + " — " + time;
  }

  function updateFromSnapshot(snapshot) {
    setText("runtime-app-version", snapshot.version, "dev");
    setText("runtime-status", snapshot.status, "Idle");
    setText("runtime-mode", snapshot.mode, "-");
    setText("runtime-library", snapshot.current_library, "-");
    setText("runtime-trigger", triggerLabel(snapshot.trigger), "-");
    setText("runtime-scheduler-status", snapshot.scheduler_status, "-");
    setText("runtime-scheduler-started", schedulerTimestampLabel(snapshot.scheduler_started_at), "-");
    setText("runtime-next-scheduled-job", snapshot.next_scheduled_job, "-");
    setText("runtime-next-scheduled-time", snapshot.next_scheduled_time, "-");
    setText("runtime-queue-depth", snapshot.queue_depth, "0");
    setText("runtime-run-id", snapshot.run_id, "-");
    setText("runtime-started-at", snapshot.started_at, "-");
    var currentFilePlaceholder = String(snapshot.status || "") === "Running" ? "Waiting for first file" : "-";
    setText("runtime-current-file", snapshot.current_file, currentFilePlaceholder);
    setText("runtime-candidates-found", snapshot.candidates_found, "-");
    setText("runtime-files-evaluated", snapshot.files_evaluated, "-");
    setText("runtime-files-processed", snapshot.files_processed, "-");
    setText("runtime-files-skipped", snapshot.files_skipped, "-");
    setText("runtime-files-failed", snapshot.files_failed, "-");
    setText("runtime-bytes-saved", savedBytesLabel(snapshot.bytes_saved), "-");
    setText("runtime-system-scheduler", schedulerStateLabel(snapshot), "-");
    setText("runtime-system-next-library-run", nextLibraryRunLabel(snapshot), "-");
    setText("runtime-system-next-housekeeping-run", snapshot.next_housekeeping_run, "-");
    var dashboardSummary = snapshot.dashboard_summary || {};
    setText("runtime-dashboard-total-saved", savedBytesLabel(dashboardSummary.total_saved), "-");
    setText("runtime-dashboard-files-optimized", dashboardSummary.files_optimized, "0");
    setText("runtime-dashboard-saved-week", savedBytesLabel(dashboardSummary.saved_this_week), "-");
    setText("runtime-dashboard-saved-month", savedBytesLabel(dashboardSummary.saved_this_month), "-");
    setText("runtime-preview-library", snapshot.preview_library, "-");
    setText("runtime-preview-generated-at", snapshot.preview_generated_at, "-");
    setPreviewResults(snapshot.preview_results || [], snapshot.preview_summary || {});
    var progress = document.getElementById("runtime-progress-section");
    if (progress) {
      progress.innerHTML = progressMarkup(snapshot);
    }
    updateStopButton(snapshot);
  }

  function setPreviewResults(rows, summary) {
    var body = document.getElementById("runtime-preview-results-body");
    if (!body) {
      return;
    }
    var summaryValue = summary || {};
    setText("runtime-preview-files-evaluated", summaryValue.files_evaluated, "0");
    setText("runtime-preview-candidates-found", summaryValue.candidates_found, "0");
    setText("runtime-preview-estimated-original", savedBytesLabel(summaryValue.estimated_original_total), "-");
    setText("runtime-preview-estimated-encoded", savedBytesLabel(summaryValue.estimated_encoded_total), "-");
    setText("runtime-preview-estimated-saved", savedBytesLabel(summaryValue.estimated_total_savings), "-");
    var pctValue = summaryValue.estimated_savings_percent;
    var pctLabel = "-";
    if (pctValue !== null && pctValue !== undefined && String(pctValue).trim() !== "") {
      pctLabel = Number(pctValue).toFixed(1) + "%";
    }
    setText("runtime-preview-estimated-pct", pctLabel, "-");
    if (!rows || !rows.length) {
      body.innerHTML = '<tr><td colspan="5" style="padding: 0.35rem;">No preview results yet.</td></tr>';
      return;
    }
    var html = '';
    for (var i = 0; i < rows.length; i += 1) {
      var row = rows[i] || {};
      html += '<tr>' +
        '<td style="border-top:1px solid #ddd; padding:0.3rem;">' + textValue(row.file, '-') + '</td>' +
        '<td style="border-top:1px solid #ddd; padding:0.3rem;">' + savedBytesLabel(row.original_size) + '</td>' +
        '<td style="border-top:1px solid #ddd; padding:0.3rem;">' + savedBytesLabel(row.estimated_size) + '</td>' +
        '<td style="border-top:1px solid #ddd; padding:0.3rem;">' + textValue(row.estimated_savings_pct, '-') + '%</td>' +
        '<td style="border-top:1px solid #ddd; padding:0.3rem;">' + textValue(row.decision, '-') + '</td>' +
      '</tr>';
    }
    body.innerHTML = html;
  }

  function requestStopRun() {
    return fetch("/api/run/cancel", { method: "POST" }).catch(function () { return null; });
  }

  function requestClearPreviewResults() {
    return fetch("/api/preview/clear", { method: "POST" })
      .then(function (response) {
        if (!response.ok) {
          throw new Error("preview clear failed");
        }
        return response.json();
      })
      .catch(function () {
        return null;
      });
  }

  function updateStopButton(snapshot) {
    var button = document.getElementById("runtime-stop-button");
    if (!button) {
      return;
    }
    var running = String(snapshot.status || "") === "Running" || String(snapshot.status || "") === "Cancelling";
    button.style.display = running ? "inline-block" : "none";
    button.disabled = String(snapshot.status || "") === "Cancelling";
  }

  function fetchStatus() {
    fetch("/api/status", { cache: "no-store" })
      .then(function (response) {
        if (!response.ok) {
          throw new Error("status request failed");
        }
        return response.json();
      })
      .then(updateFromSnapshot)
      .catch(function () {
        return null;
      });
  }

  var stopButton = document.getElementById("runtime-stop-button");
  if (stopButton) {
    stopButton.addEventListener("click", function () {
      requestStopRun().then(fetchStatus);
    });
  }

  var clearPreviewButton = document.getElementById("runtime-clear-preview-button");
  if (clearPreviewButton) {
    clearPreviewButton.addEventListener("click", function () {
      requestClearPreviewResults().then(fetchStatus);
    });
  }

  fetchStatus();
  window.setInterval(fetchStatus, 3000);
})();
