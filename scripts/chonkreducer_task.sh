#!/bin/sh
# chonkreducer_task.sh
# Usage: chonkreducer_task.sh <service>
# Example: chonkreducer_task.sh movie-transcoder

set -eu

SERVICE="${1:-}"
if [ -z "$SERVICE" ]; then
  echo "Usage: $0 <service>"
  exit 2
fi

# --- CONFIG ---
PROJ_DIR="${PROJ_DIR:-/volume1/docker/projects/nas-transcoder}"
COMPOSE="${COMPOSE:-$PROJ_DIR/compose.yaml}"
DOCKER="${DOCKER:-/usr/local/bin/docker}"

# Discord notifications
DISCORD_WEBHOOK_URL="${DISCORD_WEBHOOK_URL:-}"
DISCORD_USER_ID="${DISCORD_USER_ID:-}"

# Where to write task logs (host)
LOG_DIR="$PROJ_DIR/logs"
mkdir -p "$LOG_DIR"

STAMP="$(date +%Y%m%d-%H%M%S)"
TASK_LOG="$LOG_DIR/${SERVICE}_${STAMP}.task.log"

# Ensure a sane PATH in DSM Task Scheduler
export PATH="$PATH:/usr/local/bin:/usr/bin:/usr/syno/bin"

# --- Helpers ---
notify_discord() {
  msg="$1"
  [ -z "$DISCORD_WEBHOOK_URL" ] && return 0

  # minimal JSON escaping
  esc="$(printf '%s' "$msg" | sed 's/"/\\"/g')"
  payload="{\"content\": \"${DISCORD_USER_ID:+<@${DISCORD_USER_ID}> }${esc}\"}"

  /usr/bin/curl -sS -X POST -H "Content-Type: application/json"     -d "$payload" "$DISCORD_WEBHOOK_URL" >/dev/null 2>&1 || true
}

log() {
  echo "$@" | tee -a "$TASK_LOG"
}

# --- Start ---
log "===== CHONK TASK START ====="
log "SERVICE=$SERVICE"
log "TIME=$(date)"
log "HOST=$(hostname)"
log "PROJ_DIR=$PROJ_DIR"
log "COMPOSE=$COMPOSE"

cd "$PROJ_DIR" || exit 1

# --- OVERLAP GUARD (avoid running same service twice) ---
LOCK_FILE="/tmp/chonk_${SERVICE}.lock"
if [ -f "$LOCK_FILE" ]; then
  log "LOCK_FILE exists ($LOCK_FILE). Another run may already be active. Exiting."
  notify_discord "ChonkReducer: $SERVICE skipped (lock file exists)."
  exit 0
fi
touch "$LOCK_FILE"
trap 'rm -f "$LOCK_FILE"' EXIT INT TERM

# --- SYNC LATEST CODE (git fetch + compare HEAD) ---
REPO_CHANGED="false"
if command -v git >/dev/null 2>&1 && [ -d ".git" ]; then
  log "[git] checking for updates..."
  git fetch --all --prune >>"$TASK_LOG" 2>&1 || true

  LOCAL_HEAD="$(git rev-parse HEAD 2>/dev/null || true)"
  UPSTREAM_REF="$(git rev-parse --abbrev-ref --symbolic-full-name @{u} 2>/dev/null || true)"
  REMOTE_HEAD=""

  if [ -n "$UPSTREAM_REF" ]; then
    REMOTE_HEAD="$(git rev-parse "$UPSTREAM_REF" 2>/dev/null || true)"
  fi

  if [ -z "$REMOTE_HEAD" ]; then
    REMOTE_HEAD="$(git rev-parse origin/HEAD 2>/dev/null || true)"
  fi
  if [ -z "$REMOTE_HEAD" ]; then
    REMOTE_HEAD="$(git rev-parse origin/main 2>/dev/null || true)"
  fi
  if [ -z "$REMOTE_HEAD" ]; then
    REMOTE_HEAD="$(git rev-parse origin/master 2>/dev/null || true)"
  fi

  if [ -n "$LOCAL_HEAD" ] && [ -n "$REMOTE_HEAD" ] && [ "$LOCAL_HEAD" != "$REMOTE_HEAD" ]; then
    REPO_CHANGED="true"
    log "[git] updates detected — pulling latest changes"
    # Prefer fast-forward only to avoid accidental merges on the NAS
    if ! git pull --ff-only >>"$TASK_LOG" 2>&1; then
      # If upstream isn't configured, fall back to origin/main
      git fetch origin >>"$TASK_LOG" 2>&1 || true
      git reset --hard origin/main >>"$TASK_LOG" 2>&1 || true
    fi
    log "[git] now at $(git rev-parse --short HEAD 2>/dev/null || echo "unknown")"
  else
    log "[git] repository up to date — skipping pull"
  fi
else
  log "[git] git not available or repo not initialized; skipping update check"
fi

# --- QUICK SAFETY CHECK (pytest, only after repo changes) ---
RUN_PYTEST="${RUN_PYTEST:-true}"
if [ "$RUN_PYTEST" = "true" ] && [ "$REPO_CHANGED" = "true" ]; then
  if command -v python3 >/dev/null 2>&1; then
    log "[test] running pytest..."
    PYTHONPATH=src python3 -m pytest -q >>"$TASK_LOG" 2>&1 || { log "[test] pytest failed; aborting"; exit 1; }
  else
    log "[test] python3 not found; skipping pytest"
  fi
elif [ "$RUN_PYTEST" = "true" ]; then
  log "[test] repository unchanged — skipping pytest"
fi

# --- REBUILD IMAGE (only when code changed, or image missing) ---
REBUILD_IMAGE="${REBUILD_IMAGE:-true}"
REBUILD_NO_CACHE="${REBUILD_NO_CACHE:-true}"
if [ "$REBUILD_IMAGE" = "true" ]; then
  SHOULD_BUILD="$REPO_CHANGED"
  if [ "$SHOULD_BUILD" != "true" ]; then
    set +e
    EXISTING_IMAGE_ID="$($DOCKER compose -f "$COMPOSE" images -q "$SERVICE" 2>>"$TASK_LOG")"
    IMAGE_LOOKUP_RC=$?
    set -e
    if [ $IMAGE_LOOKUP_RC -ne 0 ]; then
      log "[build] image lookup failed for $SERVICE (continuing with rebuild)"
      EXISTING_IMAGE_ID=""
    fi
    if [ -z "$EXISTING_IMAGE_ID" ]; then
      SHOULD_BUILD="true"
      log "[build] no local image found for $SERVICE — building container"
    fi
  fi

  BUILD_ARGS=""
  if [ "$REBUILD_NO_CACHE" = "true" ]; then
    BUILD_ARGS="--no-cache"
  fi
  if [ "$SHOULD_BUILD" = "true" ]; then
    log "[build] rebuilding image for service: $SERVICE ($BUILD_ARGS)"
    "$DOCKER" compose -f "$COMPOSE" build $BUILD_ARGS "$SERVICE" >>"$TASK_LOG" 2>&1 || { log "[build] docker compose build failed; aborting"; exit 1; }
  else
    log "[build] repository up to date — skipping container rebuild"
  fi
fi

# --- DRY_RUN visibility (optional) ---
DRY_RUN="${DRY_RUN:-false}"
if [ "$DRY_RUN" = "true" ]; then
  log "DRY_RUN=true (will run container, but your app should no-op if it honors DRY_RUN)"
fi

# --- RUN ---
log "Running docker compose service: $SERVICE"
set +e
"$DOCKER" compose -f "$COMPOSE" run --rm "$SERVICE" >>"$TASK_LOG" 2>&1
RC=$?
set -e

if [ $RC -eq 0 ]; then
  log "SUCCESS: $SERVICE exit code 0"
  notify_discord "ChonkReducer: ✅ $SERVICE completed successfully."
else
  log "FAIL: $SERVICE exit code $RC"
  notify_discord "ChonkReducer: ❌ $SERVICE failed (exit $RC). Check logs: $TASK_LOG"
fi

log "===== CHONK TASK END ====="
exit $RC
