#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 6 ]; then
  echo "Usage: $0 <service> <playlist> <adaptation> <preloading> <swiping>" >&2
  exit 2
fi

LOG_FILE="${EMULATOR_LOG_FILE:-$HOME/emulator.log}"
APP_DIR="${EMULATOR_APP_DIR:-$HOME/streamingapp}"
#LOGS_DIR="${STREAM_LOGS_DIR:-logs}"
LOGS_DIR="${6}"

mkdir -p "$(dirname "$LOG_FILE")"
exec >> "$LOG_FILE" 2>&1

echo "[$(date -Is)] starting emulator traffic generation"
echo "app_dir=$APP_DIR logs_dir=$LOGS_DIR service=$1 playlist=$2 adaptation=$3 preloading=$4 swiping=$5"

cd "$APP_DIR"
source venv/bin/activate

nohup timeout 60 env \
  "stream_logs_dir=$LOGS_DIR" \
  "stream_service=$1" \
  "stream_playlist=$2" \
  "stream_adaptation=$3" \
  "stream_preloading=$4" \
  "stream_swiping=$5" \
  python3 main.py &

pid=$!
echo "started emulator process pid=$pid"

deactivate
