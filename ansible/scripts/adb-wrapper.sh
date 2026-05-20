#!/usr/bin/env bash
set -euo pipefail

APP_NAME="android-test-stack"

RUN_DIR="${RUN_DIR:-/tmp/${APP_NAME}}"
LOG_DIR="${LOG_DIR:-/tmp/${APP_NAME}/logs}"

APPIUM_PID_FILE="${RUN_DIR}/appium.pid"
EMULATOR_PID_FILE="${RUN_DIR}/emulator.pid"

APPIUM_LOG="${LOG_DIR}/appium.log"
EMULATOR_LOG="${LOG_DIR}/emulator.log"

AVD_NAME="${AVD_NAME:-aosp_api33}"

export ANDROID_SDK_ROOT="${ANDROID_SDK_ROOT:-/opt/android-sdk}"
export ANDROID_HOME="${ANDROID_HOME:-$ANDROID_SDK_ROOT}"

export NVM_DIR="${NVM_DIR:-/home/gnb/.config/nvm}"
export NODE_VERSION="${NODE_VERSION:-v20.20.2}"

export PATH="$ANDROID_SDK_ROOT/platform-tools:$ANDROID_SDK_ROOT/emulator:$NVM_DIR/versions/node/$NODE_VERSION/bin:/usr/local/bin:/usr/bin:/bin:$PATH"

ADB="${ADB:-/opt/android-sdk/platform-tools/adb}"
EMULATOR="${EMULATOR:-/opt/android-sdk/emulator/emulator}"
APPIUM="${APPIUM:-/home/gnb/.config/nvm/versions/node/v20.20.2/bin/appium}"
NODE="${NODE:-/home/gnb/.config/nvm/versions/node/v20.20.2/bin/node}"

for bin in "$ADB" "$EMULATOR" "$APPIUM" "$NODE"; do
  if [[ ! -x "$bin" ]]; then
    echo "Required executable not found or not executable: $bin" >&2
    echo "Current PATH: $PATH" >&2
    exit 127
  fi
done

mkdir -p "$RUN_DIR" "$LOG_DIR"

is_running() {
  local pid_file="$1"

  [[ -f "$pid_file" ]] || return 1

  local pid
  pid="$(cat "$pid_file")"

  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

find_emulator_pid() {
  pgrep -f "$EMULATOR.*-avd ${AVD_NAME}" | head -n 1 || true
}

start_emulator() {
  if is_running "$EMULATOR_PID_FILE"; then
    echo "emulator is already running with PID $(cat "$EMULATOR_PID_FILE")"
    return 0
  fi

  local existing_pid
  existing_pid="$(find_emulator_pid)"

  if [[ -n "$existing_pid" ]]; then
    echo "$existing_pid" > "$EMULATOR_PID_FILE"
    echo "emulator is already running with PID $existing_pid"
    return 0
  fi

  echo "Starting emulator..."

  nohup "$EMULATOR" \
    -avd "$AVD_NAME" \
    -no-window \
    -no-audio \
    -no-boot-anim \
    -gpu swiftshader_indirect \
    -skin 1080x2220 \
    >> "$EMULATOR_LOG" 2>&1 &

  echo $! > "$EMULATOR_PID_FILE"

  sleep 5

  local emulator_pid
  emulator_pid="$(find_emulator_pid)"

  if [[ -n "$emulator_pid" ]]; then
    echo "$emulator_pid" > "$EMULATOR_PID_FILE"
    echo "emulator started with PID $emulator_pid"
  else
    echo "emulator process started, waiting for adb instead..."
  fi
}

wait_for_emulator_ready() {
  echo "Waiting for emulator to become ready..."

  "$ADB" wait-for-device

  until [[ "$("$ADB" shell getprop sys.boot_completed 2>/dev/null | tr -d '\r')" == "1" ]]; do
    sleep 2
  done

  echo "Emulator boot completed"
}

start_appium() {
  if is_running "$APPIUM_PID_FILE"; then
    echo "appium is already running with PID $(cat "$APPIUM_PID_FILE")"
    return 0
  fi

  echo "Starting appium..."

  nohup "$APPIUM" >> "$APPIUM_LOG" 2>&1 &
  echo $! > "$APPIUM_PID_FILE"

  sleep 2

  if is_running "$APPIUM_PID_FILE"; then
    echo "appium started with PID $(cat "$APPIUM_PID_FILE")"
  else
    echo "Failed to start appium. Check log: $APPIUM_LOG"
    rm -f "$APPIUM_PID_FILE"
    return 1
  fi
}

stop_process() {
  local name="$1"
  local pid_file="$2"

  if ! is_running "$pid_file"; then
    echo "$name is not running"
    rm -f "$pid_file"
    return 0
  fi

  local pid
  pid="$(cat "$pid_file")"

  echo "Stopping $name with PID $pid..."
  kill "$pid" 2>/dev/null || true

  sleep 2

  if kill -0 "$pid" 2>/dev/null; then
    echo "$name did not stop gracefully; killing..."
    kill -9 "$pid" 2>/dev/null || true
  fi

  rm -f "$pid_file"
  echo "$name stopped"
}

start() {
  start_emulator
  wait_for_emulator_ready
  start_appium
}

stop() {
  stop_process "appium" "$APPIUM_PID_FILE"
  stop_process "emulator" "$EMULATOR_PID_FILE"
}

status() {
  if is_running "$EMULATOR_PID_FILE"; then
    echo "emulator is running with PID $(cat "$EMULATOR_PID_FILE")"
  else
    echo "emulator is not running"
  fi

  if is_running "$APPIUM_PID_FILE"; then
    echo "appium is running with PID $(cat "$APPIUM_PID_FILE")"
  else
    echo "appium is not running"
  fi
}

case "${1:-}" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  restart)
    stop
    start
    ;;
  status)
    status
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|status}"
    exit 1
    ;;
esac