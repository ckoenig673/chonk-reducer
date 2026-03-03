#!/bin/sh
set -eu

# Ensure scheduled-task PATH includes common locations (DSM can be minimal)
export PATH="$PATH:/usr/local/bin:/usr/bin:/bin:/usr/syno/bin"

PROJ_DIR="/volume1/docker/projects/nas-transcoder"
COMPOSE="${PROJ_DIR}/compose.yaml"
DOCKER="/usr/local/bin/docker"

LOG_ROOT="/volume1/data/transcodework/logs"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
WRAPPER_LOG="${LOG_ROOT}/wrapper_${RUN_TS}.log"
LATEST_LOG="${LOG_ROOT}/wrapper_latest.log"

# --- Discord notifications ---
DISCORD_WEBHOOK_URL="${DISCORD_WEBHOOK_URL:-}"
DISCORD_USER_ID="${DISCORD_USER_ID:-}"

# Behavior toggles
DISCORD_ONLY_IF_WORK_DONE="true"   # send only if Processed>0 or Failed>0
DISCORD_PING_ON_SUCCESS="false"    # ping you on success (usually noisy)
DISCORD_PING_ON_FAILURE="true"     # ping you when failures happen
DISCORD_DEBUG="false"              # when true, prints Discord API response + curl exit code (for troubleshooting)

# Ensure log directory exists
mkdir -p "$LOG_ROOT"

# Redirect all output to wrapper log
exec >> "$WRAPPER_LOG" 2>&1

# Optional: keep a pointer to the latest run log
ln -sf "$WRAPPER_LOG" "$LATEST_LOG"

echo "===== WRAPPER START $(date) ====="
echo "Webhook: $DISCORD_WEBHOOK_URL"
echo "UserID: $DISCORD_USER_ID"

# Fail fast if no service provided
if [ -z "${1:-}" ]; then
  echo "ERROR: No service name provided"
  exit 1
fi

SERVICE="$1"
echo "Service: $SERVICE"
echo "Project Dir: $PROJ_DIR"
echo "Compose File: $COMPOSE"

cd "$PROJ_DIR"

# Guard against overlap (container already running)
if "$DOCKER" ps --format '{{.Names}}' | grep -q "^${SERVICE}\$"; then
  echo "ERROR: ${SERVICE} appears to already be running. Exiting to avoid overlap."
  echo "===== WRAPPER END $(date) ====="
  exit 1
fi

echo "Starting docker compose for $SERVICE ..."

# Temporarily disable 'exit on error' so we can log exit code cleanly
set +e
"$DOCKER" compose -f "$COMPOSE" run --rm --no-deps "$SERVICE"
EXIT_CODE=$?
set -e

echo "Docker exit code: $EXIT_CODE"
echo "===== WRAPPER END $(date) ====="

send_discord() {
  [ -n "${DISCORD_WEBHOOK_URL:-}" ] || return 0

  DISCORD_DEBUG="${DISCORD_DEBUG:-false}"

  # Keep the payload short and safe (Discord message limits + JSON escaping)
  SUMMARY="$(grep -E 'RUN_ID=|MODE=|Processed:|Failed:|TOTAL BEFORE \(run\):|TOTAL AFTER \(run\):|TOTAL SAVED \(run\):|TOTAL SAVED PCT \(run\):|RUN DURATION:|TOTAL TIME:|TOTAL RATE:|Run log:' "$WRAPPER_LOG" 2>/dev/null | tail -n 40)"

  # Fallback: last lines of wrapper log
  if [ -z "$SUMMARY" ]; then
    SUMMARY="$(tail -n 60 "$WRAPPER_LOG" 2>/dev/null)"
  fi

  PROCESSED="$(echo "$SUMMARY" | awk -F: '/Processed/{gsub(/^[ 	]+|[ 	]+$/,"",$2); print $2; exit}')"
  FAILED="$(echo "$SUMMARY" | awk -F: '/Failed/{gsub(/^[ 	]+|[ 	]+$/,"",$2); print $2; exit}')"

  PROCESSED="${PROCESSED:-0}"
  FAILED="${FAILED:-0}"

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

  MSG="${MENTION}Chonk run complete (${SERVICE}). Exit=${EXIT_CODE}

${SUMMARY}"

  JSON_MSG="$(printf "%s" "$MSG"     | sed 's/\\/\\\\/g; s/"/\\\"/g'     | awk '{printf "%s\\n", $0}'     | sed 's/\\n$//')"

  if [ -n "${DISCORD_USER_ID:-}" ]; then
    if [ "$DISCORD_DEBUG" = "true" ]; then
      /bin/curl -sS -H "Content-Type: application/json" -X POST         -d "{\"content\":\"$JSON_MSG\",\"allowed_mentions\":{\"users\":[\"${DISCORD_USER_ID}\"]}}"         "$DISCORD_WEBHOOK_URL"
      echo "discord curl exit=$?"
    else
      /bin/curl -sS -H "Content-Type: application/json" -X POST         -d "{\"content\":\"$JSON_MSG\",\"allowed_mentions\":{\"users\":[\"${DISCORD_USER_ID}\"]}}"         "$DISCORD_WEBHOOK_URL" >/dev/null 2>&1 || true
    fi
  else
    if [ "$DISCORD_DEBUG" = "true" ]; then
      /bin/curl -sS -H "Content-Type: application/json" -X POST         -d "{\"content\":\"$JSON_MSG\"}"         "$DISCORD_WEBHOOK_URL"
      echo "discord curl exit=$?"
    else
      /bin/curl -sS -H "Content-Type: application/json" -X POST         -d "{\"content\":\"$JSON_MSG\"}"         "$DISCORD_WEBHOOK_URL" >/dev/null 2>&1 || true
    fi
  fi
}

send_discord
exit "$EXIT_CODE"
