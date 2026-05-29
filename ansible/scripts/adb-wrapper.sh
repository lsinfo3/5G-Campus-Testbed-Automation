#!/usr/bin/env bash
set -euo pipefail

APP_NAME="android-test-stack"

RUN_DIR="${RUN_DIR:-/tmp/${APP_NAME}}"
LOG_DIR="${LOG_DIR:-/tmp/${APP_NAME}/logs}"

APPIUM_PID_FILE="${RUN_DIR}/appium.pid"
EMULATOR_PID_FILE="${RUN_DIR}/emulator.pid"
WRAPPER_PID_FILE="${RUN_DIR}/wrapper.pid"

APPIUM_LOG="${LOG_DIR}/appium.log"
EMULATOR_LOG="${LOG_DIR}/emulator.log"
WRAPPER_LOG="${LOG_DIR}/wrapper.log"
MONITOR_INTERVAL="${MONITOR_INTERVAL:-5}"
DAEMON_START_TIMEOUT="${DAEMON_START_TIMEOUT:-30}"

AVD_NAME="${AVD_NAME:-aosp_api33}"
if [[ -n "${AVD_HOME:-}" && -z "${ANDROID_AVD_HOME:-}" ]]; then
  export ANDROID_AVD_HOME="$AVD_HOME"
fi
if [[ -n "${HOME:-}" && -z "${ANDROID_USER_HOME:-}" ]]; then
  export ANDROID_USER_HOME="$HOME/.android"
fi
if [[ -n "${ANDROID_USER_HOME:-}" && -z "${ANDROID_AVD_HOME:-}" ]]; then
  if [[ -d "${XDG_DATA_HOME:-$HOME/.local/share}/android/avd" ]]; then
    export ANDROID_AVD_HOME="${XDG_DATA_HOME:-$HOME/.local/share}/android/avd"
  else
    export ANDROID_AVD_HOME="$ANDROID_USER_HOME/avd"
  fi
fi

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

print_avd_diagnostics() {
  {
    echo "Configured AVD_NAME: $AVD_NAME"
    echo "HOME: ${HOME:-<unset>}"
    echo "USER: ${USER:-<unset>}"
    echo "LOGNAME: ${LOGNAME:-<unset>}"
    echo "PWD: ${PWD:-<unset>}"
    echo "ANDROID_USER_HOME: ${ANDROID_USER_HOME:-<unset>}"
    echo "ANDROID_AVD_HOME: ${ANDROID_AVD_HOME:-<unset>}"
    echo "ANDROID_SDK_HOME: ${ANDROID_SDK_HOME:-<unset>}"
    echo "ANDROID_SDK_ROOT: ${ANDROID_SDK_ROOT:-<unset>}"
    echo "Available AVDs from $EMULATOR:"
    local avds
    avds="$("$EMULATOR" -list-avds 2>/dev/null || true)"
    if [[ -n "$avds" ]]; then
      echo "$avds"
    else
      echo "  <none>"
    fi

    local avd_dirs=()
    local candidate_dir existing_dir seen_dir
    for candidate_dir in \
      "${ANDROID_AVD_HOME:-}" \
      "${ANDROID_SDK_HOME:+$ANDROID_SDK_HOME/.android/avd}" \
      "${HOME:+$HOME/.android/avd}"; do
      [[ -n "$candidate_dir" ]] || continue
      seen_dir=false
      for existing_dir in "${avd_dirs[@]}"; do
        if [[ "$existing_dir" == "$candidate_dir" ]]; then
          seen_dir=true
          break
        fi
      done
      [[ "$seen_dir" == true ]] || avd_dirs+=("$candidate_dir")
    done

    echo "Expected AVD ini locations:"
    local avd_dir
    for avd_dir in "${avd_dirs[@]}"; do
      echo "  $avd_dir/${AVD_NAME}.ini"
    done

    echo "AVD search directory contents:"
    local ini
    for avd_dir in "${avd_dirs[@]}"; do
      if [[ -d "$avd_dir" ]]; then
        echo "  $avd_dir:"
        shopt -s nullglob
        local ini_files=("$avd_dir"/*.ini)
        shopt -u nullglob
        if (( ${#ini_files[@]} )); then
          for ini in "${ini_files[@]}"; do
            echo "    $(basename "$ini" .ini)"
          done
        else
          echo "    <no .ini files>"
        fi
      else
        echo "  $avd_dir: <missing>"
      fi
    done

    echo "Common AVD locations:"
    local common_avd_dir
    for common_avd_dir in \
      "/home/gnb/.android/avd" \
      "/home/gnb/.local/share/android/avd" \
      "/root/.android/avd" \
      "/root/.local/share/android/avd" \
      "/opt/android-sdk/.android/avd" \
      "/opt/android-sdk/avd"; do
      if [[ -d "$common_avd_dir" ]]; then
        echo "  $common_avd_dir: <exists>"
      else
        echo "  $common_avd_dir: <missing>"
      fi
    done
  } >&2
}

validate_avd() {
  if "$EMULATOR" -list-avds | grep -Fxq "$AVD_NAME"; then
    return 0
  fi

  echo "Unknown AVD name: $AVD_NAME" >&2
  print_avd_diagnostics
  return 1
}

is_running() {
  local pid_file="$1"

  [[ -f "$pid_file" ]] || return 1

  local pid
  pid="$(cat "$pid_file")"

  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

current_pid_matches_file() {
  local pid_file="$1"

  [[ -f "$pid_file" ]] || return 1
  [[ "$(cat "$pid_file")" == "$$" ]]
}

find_emulator_pid() {
  pgrep -f "$EMULATOR.*-avd ${AVD_NAME}" | head -n 1 || true
}

refresh_emulator_pid() {
  local emulator_pid
  emulator_pid="$(find_emulator_pid)"

  if [[ -n "$emulator_pid" ]]; then
    echo "$emulator_pid" > "$EMULATOR_PID_FILE"
    return 0
  fi

  is_running "$EMULATOR_PID_FILE"
}

require_emulator_running() {
  if refresh_emulator_pid; then
    return 0
  fi

  echo "emulator is not running; no live PID found" >&2
  rm -f "$EMULATOR_PID_FILE"
  return 1
}

print_log_tail() {
  local label="$1"
  local log_file="$2"

  if [[ ! -f "$log_file" ]]; then
    echo "No $label log found at $log_file" >&2
    return 0
  fi

  echo "Last lines from $label log ($log_file):" >&2
  tail -n 40 "$log_file" >&2 || true
}

print_startup_logs() {
  print_log_tail "wrapper" "$WRAPPER_LOG"
  print_log_tail "emulator" "$EMULATOR_LOG"
  print_log_tail "appium" "$APPIUM_LOG"
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
  validate_avd

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
  elif is_running "$EMULATOR_PID_FILE"; then
    echo "emulator started with PID $(cat "$EMULATOR_PID_FILE")"
  else
    echo "Failed to start emulator. Check log: $EMULATOR_LOG" >&2
    rm -f "$EMULATOR_PID_FILE"
    return 1
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
  require_emulator_running
  start_appium
}

monitor() {
  echo "Monitoring emulator and appium every ${MONITOR_INTERVAL}s..."

  while true; do
    if ! refresh_emulator_pid; then
      echo "emulator is not running; restarting emulator and appium..." >&2
      rm -f "$EMULATOR_PID_FILE"
      stop_process "appium" "$APPIUM_PID_FILE"

      if start_emulator && wait_for_emulator_ready && require_emulator_running; then
        start_appium
      else
        echo "emulator restart failed; retrying in ${MONITOR_INTERVAL}s" >&2
      fi
    elif ! is_running "$APPIUM_PID_FILE"; then
      echo "appium is not running; restarting appium..." >&2
      rm -f "$APPIUM_PID_FILE"
      start_appium || echo "appium restart failed; retrying in ${MONITOR_INTERVAL}s" >&2
    fi

    sleep "$MONITOR_INTERVAL"
  done
}

run() {
  echo "$$" > "$WRAPPER_PID_FILE"
  trap '' HUP
  trap 'echo "Stopping monitored services..."; stop_services; rm -f "$WRAPPER_PID_FILE"; exit 0' INT TERM

  until start; do
    echo "wrapper startup failed; retrying in ${MONITOR_INTERVAL}s..." >&2
    sleep "$MONITOR_INTERVAL"
  done

  monitor
}

daemon() {
  if is_running "$WRAPPER_PID_FILE"; then
    echo "wrapper is already running with PID $(cat "$WRAPPER_PID_FILE")"
    return 0
  fi

  echo "Starting wrapper monitor..."
  nohup bash "$0" run >> "$WRAPPER_LOG" 2>&1 &
  echo $! > "$WRAPPER_PID_FILE"

  sleep 1

  if is_running "$WRAPPER_PID_FILE"; then
    echo "wrapper monitor started with PID $(cat "$WRAPPER_PID_FILE")"
  else
    echo "Failed to start wrapper monitor. Check log: $WRAPPER_LOG" >&2
    rm -f "$WRAPPER_PID_FILE"
    return 1
  fi

  local deadline
  deadline=$((SECONDS + DAEMON_START_TIMEOUT))

  while (( SECONDS < deadline )); do
    if ! is_running "$WRAPPER_PID_FILE"; then
      echo "wrapper monitor exited during startup. Check log: $WRAPPER_LOG" >&2
      print_startup_logs
      rm -f "$WRAPPER_PID_FILE"
      return 1
    fi

    if refresh_emulator_pid && is_running "$APPIUM_PID_FILE"; then
      echo "wrapper services are ready"
      return 0
    fi

    sleep 1
  done

  echo "Timed out waiting for wrapper services to become ready. Check log: $WRAPPER_LOG" >&2
  print_startup_logs
  return 1
}

stop_wrapper() {
  if ! is_running "$WRAPPER_PID_FILE"; then
    rm -f "$WRAPPER_PID_FILE"
    return 0
  fi

  if current_pid_matches_file "$WRAPPER_PID_FILE"; then
    return 0
  fi

  local pid
  pid="$(cat "$WRAPPER_PID_FILE")"

  echo "Stopping wrapper monitor with PID $pid..."
  kill "$pid" 2>/dev/null || true

  sleep 2

  if kill -0 "$pid" 2>/dev/null; then
    echo "wrapper monitor did not stop gracefully; killing..."
    kill -9 "$pid" 2>/dev/null || true
  fi

  rm -f "$WRAPPER_PID_FILE"
  echo "wrapper monitor stopped"
}

stop_services() {
  stop_process "appium" "$APPIUM_PID_FILE"
  stop_process "emulator" "$EMULATOR_PID_FILE"
}

stop() {
  stop_wrapper
  stop_services
}

status() {
  if is_running "$WRAPPER_PID_FILE"; then
    echo "wrapper is running with PID $(cat "$WRAPPER_PID_FILE")"
  else
    echo "wrapper is not running"
  fi

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
  run)
    run
    ;;
  daemon)
    daemon
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
    echo "Usage: $0 {start|run|daemon|stop|restart|status}"
    exit 1
    ;;
esac
