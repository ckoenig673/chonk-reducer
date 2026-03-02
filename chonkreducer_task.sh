#!/bin/sh
set -eu

PROJ_DIR="/volume1/docker/projects/nas-transcoder"
COMPOSE="${PROJ_DIR}/compose.yaml"
DOCKER="/usr/local/bin/docker"

LOG_ROOT="/volume1/data/transcodework/logs"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
WRAPPER_LOG="${LOG_ROOT}/wrapper_${RUN_TS}.log"

# Ensure log directory exists
mkdir -p "$LOG_ROOT"

# Redirect all output to wrapper log
exec >> "$WRAPPER_LOG" 2>&1

echo "===== WRAPPER START $(date) ====="

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

echo "Starting docker compose for $SERVICE ..."
"$DOCKER" compose -f "$COMPOSE" run --rm --no-deps "$SERVICE"

EXIT_CODE=$?

echo "Docker exit code: $EXIT_CODE"
echo "===== WRAPPER END $(date) ====="

exit "$EXIT_CODE"