#!/bin/sh
set -eu

# Ensure scheduled-task PATH includes common locations (DSM can be minimal)
export PATH="$PATH:/usr/local/bin:/usr/bin:/bin:/usr/syno/bin"

PROJ_DIR="/volume1/docker/projects/nas-transcoder"
COMPOSE="${PROJ_DIR}/compose.yaml"
DOCKER="/usr/local/bin/docker"

LOG_ROOT="/volume1/data/transcodework/logs"
STATE_ROOT="/volume1/data/transcodework/state"
ESCALATE_FAILURES="${ESCALATE_FAILURES:-true}"
ESCALATE_FAILURE_THRESHOLD="${ESCALATE_FAILURE_THRESHOLD:-3}"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
WRAPPER_LOG="${LOG_ROOT}/wrapper_${RUN_TS}.log"

# --- Discord notifications ---
# NOTE: Keep secrets out of compose.yaml. Provide these via DSM Task Scheduler env vars
# (or export inline in the DSM "Run command" box).
DISCORD_WEBHOOK_URL="${DISCORD_WEBHOOK_URL:-}"
DISCORD_USER_ID="${DISCORD_USER_ID:-}"

# Behavior toggles (wrapper-level)
DISCORD_ONLY_IF_WORK_DONE="${DISCORD_ONLY_IF_WORK_DONE:-true}"   # send only if "work happened"
DISCORD_PING_ON_SUCCESS="${DISCORD_PING_ON_SUCCESS:-false}"      # ping you on success (usually noisy)
DISCORD_PING_ON_FAILURE="${DISCORD_PING_ON_FAILURE:-true}"       # ping you when failures happen
DISCORD_DEBUG="${DISCORD_DEBUG:-false}"                          # when true, prints Discord API response + curl exit code

# Feature toggles (wrapper-level)
DISCORD_NOTIFY_HEALTHCHECK="${DISCORD_NOTIFY_HEALTHCHECK:-true}"
DISCORD_NOTIFY_WEEKLY="${DISCORD_NOTIFY_WEEKLY:-true}"

# Ensure log directory exists
mkdir -p "$LOG_ROOT"
mkdir -p "$STATE_ROOT"

# Redirect all output to wrapper log
exec >> "$WRAPPER_LOG" 2>&1

# Optional: keep a pointer to the latest wrapper log

echo "===== WRAPPER START $(date) ====="
echo "Webhook: $DISCORD_WEBHOOK_URL"
echo "UserID: $DISCORD_USER_ID"

# Fail fast if no service provided
if [ -z "${1:-}" ]; then
  echo "ERROR: No service name provided"
  echo "Usage: chonkreducer_task.sh <service> [run|healthcheck|weekly-report]"
  exit 1
fi

SERVICE="$1"
CMD="${2:-run}"

echo "Service: $SERVICE"
echo "Command: $CMD"
echo "Project Dir: $PROJ_DIR"
echo "Compose File: $COMPOSE"

cd "$PROJ_DIR"

# Guard against overlap (container already running)
if "$DOCKER" ps --format '{{.Names}}' | grep -q "^${SERVICE}\$"; then
  echo "ERROR: ${SERVICE} appears to already be running. Exiting to avoid overlap."
  echo "===== WRAPPER END $(date) ====="
  exit 1
fi


# --- DRY_RUN visibility: read resolved DRY_RUN from docker compose config (matches container env) ---
if [ "${CMD:-run}" = "run" ]; then
  COMPOSE_DRY_RUN="$("$DOCKER" compose -f "$COMPOSE" config 2>/dev/null | awk '$1=="DRY_RUN:" {print $2; exit}' | tr -d '"')"
  if [ "$COMPOSE_DRY_RUN" = "true" ] || [ "$COMPOSE_DRY_RUN" = "True" ] || [ "$COMPOSE_DRY_RUN" = "1" ]; then
    echo "***** DRY RUN ENABLED (no swaps will occur) *****"
    echo "Wrapper detected: MODE=DRY_RUN (from compose)"
  fi
fi

echo "Starting docker compose for $SERVICE ..."

# Temporarily disable 'exit on error' so we can log exit code cleanly
set +e
"$DOCKER" compose -f "$COMPOSE" run --rm --no-deps "$SERVICE" "$CMD"
EXIT_CODE=$?
set -e

echo "Docker exit code: $EXIT_CODE"

# --- Story 45: Escalated failure handling (consecutive failure counter) ---
FAILKEY="${SERVICE}.${CMD}"
FAILFILE="${STATE_ROOT}/${FAILKEY}.failcount"
FAILCOUNT="0"
if [ -f "$FAILFILE" ]; then FAILCOUNT="$(cat "$FAILFILE" 2>/dev/null || echo 0)"; fi
case "$FAILCOUNT" in (*[!0-9]*|"") FAILCOUNT="0";; esac
ESCALATE_NOW="false"
if [ "$EXIT_CODE" -ne 0 ]; then
  FAILCOUNT=$((FAILCOUNT + 1))
  echo "$FAILCOUNT" > "$FAILFILE" 2>/dev/null || true
  if [ "$ESCALATE_FAILURES" = "true" ] && [ "$FAILCOUNT" -ge "$ESCALATE_FAILURE_THRESHOLD" ]; then
    ESCALATE_NOW="true"
  fi
else
  # reset on success
  echo "0" > "$FAILFILE" 2>/dev/null || true
fi

echo "===== WRAPPER END $(date) ====="

send_discord() {
  [ -n "${DISCORD_WEBHOOK_URL:-}" ] || return 0

  # Feature toggles
  if [ "$CMD" = "healthcheck" ] && [ "${DISCORD_NOTIFY_HEALTHCHECK:-true}" != "true" ]; then
    return 0
  fi
  if [ "$CMD" = "weekly-report" ] && [ "${DISCORD_NOTIFY_WEEKLY:-true}" != "true" ]; then
    return 0
  fi

  # Pull a concise summary from wrapper log (supports run + weekly-report + healthcheck)
  SUMMARY="$(grep -E 'RUN_ID=|MODE=|Processed:|Failed:|TOTAL BEFORE \(run\):|TOTAL AFTER \(run\):|TOTAL SAVED \(run\):|TOTAL SAVED PCT \(run\):|RUN DURATION:|TOTAL TIME:|TOTAL RATE:|Run log:|Weekly report written:|Window:|TV saved:|Movies saved:|Total saved:|Failures:|===== HEALTHCHECK|\[OK\]|\[FAIL\]|HEALTHCHECK OK|HEALTHCHECK FAIL' "$WRAPPER_LOG" 2>/dev/null | tail -n 80)"

  if [ -z "$SUMMARY" ]; then
    SUMMARY="$(tail -n 80 "$WRAPPER_LOG" 2>/dev/null)"
  fi

  PROCESSED="0"
  FAILED="0"

  if [ "$CMD" = "run" ]; then
    PROCESSED="$(echo "$SUMMARY" | awk -F: '/Processed/{gsub(/^[ \t]+|[ \t]+$/,"",$2); print $2; exit}')"
    FAILED="$(echo "$SUMMARY" | awk -F: '/Failed/{gsub(/^[ \t]+|[ \t]+$/,"",$2); print $2; exit}')"
    PROCESSED="${PROCESSED:-0}"
    FAILED="${FAILED:-0}"
  elif [ "$CMD" = "weekly-report" ]; then
    # Weekly-report emits: Total saved / Failures
    FAILED="$(echo "$SUMMARY" | awk -F: '/Failures/{gsub(/^[ \t]+|[ \t]+$/,"",$2); print $2; exit}')"
    FAILED="${FAILED:-0}"

    SAVED_GB="$(echo "$SUMMARY" | awk -F: '/Total saved/{gsub(/^[ \t]+|[ \t]+$/,"",$2); print $2; exit}' | sed 's/GB//g; s/[ \t]//g')"
    SAVED_GB="${SAVED_GB:-0}"
    if [ "$SAVED_GB" != "0.00" ] && [ "$SAVED_GB" != "0" ]; then
      PROCESSED="1"
    fi
  elif [ "$CMD" = "healthcheck" ]; then
    # Gate healthcheck on exit code / [FAIL] lines
    if [ "$EXIT_CODE" != "0" ] || echo "$SUMMARY" | grep -q '\[FAIL\]'; then
      FAILED="1"
    else
      PROCESSED="1"
    fi
  else
    # Unknown command: treat any non-zero exit as failure
    if [ "$EXIT_CODE" != "0" ]; then FAILED="1"; fi
  fi

  # Optional gating: only send if something happened
  if [ "${DISCORD_ONLY_IF_WORK_DONE:-true}" = "true" ]; then
    if [ "$PROCESSED" = "0" ] && [ "$FAILED" = "0" ]; then
      return 0
    fi
  fi

  MENTION=""
  if [ -n "${DISCORD_USER_ID:-}" ]; then
    if [ "$FAILED" != "0" ] && [ "${DISCORD_PING_ON_FAILURE:-true}" = "true" ]; then
      MENTION="<@${DISCORD_USER_ID}> "
    elif [ "${DISCORD_PING_ON_SUCCESS:-false}" = "true" ]; then
      MENTION="<@${DISCORD_USER_ID}> "
    fi
  fi

  # Escalation override
  if [ "${ESCALATE_NOW:-false}" = "true" ] && [ -n "${DISCORD_USER_ID:-}" ]; then
    MENTION="<@${DISCORD_USER_ID}> "
  fi

  ESCALATION_LINE=""
  if [ "${ESCALATE_NOW:-false}" = "true" ]; then
    ESCALATION_LINE="[ESCALATION] consecutive failures: ${FAILCOUNT} (threshold=${ESCALATE_FAILURE_THRESHOLD})\n\n"
  fi

  MSG="${MENTION}${ESCALATION_LINE}Chonk run complete (${SERVICE}). Cmd=${CMD}. Exit=${EXIT_CODE}

${SUMMARY}"

  # JSON escape for Discord
  JSON_MSG="$(printf "%s" "$MSG" \
    | sed 's/\\/\\\\/g; s/"/\\"/g' \
    | awk '{printf "%s\\n", $0}' \
    | sed 's/\\n$//')"

  if [ -n "${DISCORD_USER_ID:-}" ]; then
    if [ "${DISCORD_DEBUG:-false}" = "true" ]; then
      /bin/curl -sS -H "Content-Type: application/json" -X POST \
        -d "{\"content\":\"$JSON_MSG\",\"allowed_mentions\":{\"users\":[\"${DISCORD_USER_ID}\"]}}" \
        "$DISCORD_WEBHOOK_URL"
      echo "discord curl exit=$?"
    else
      /bin/curl -sS -H "Content-Type: application/json" -X POST \
        -d "{\"content\":\"$JSON_MSG\",\"allowed_mentions\":{\"users\":[\"${DISCORD_USER_ID}\"]}}" \
        "$DISCORD_WEBHOOK_URL" >/dev/null 2>&1 || true
    fi
  else
    if [ "${DISCORD_DEBUG:-false}" = "true" ]; then
      /bin/curl -sS -H "Content-Type: application/json" -X POST \
        -d "{\"content\":\"$JSON_MSG\"}" \
        "$DISCORD_WEBHOOK_URL"
      echo "discord curl exit=$?"
    else
      /bin/curl -sS -H "Content-Type: application/json" -X POST \
        -d "{\"content\":\"$JSON_MSG\"}" \
        "$DISCORD_WEBHOOK_URL" >/dev/null 2>&1 || true
    fi
  fi
}

send_discord
exit "$EXIT_CODE"