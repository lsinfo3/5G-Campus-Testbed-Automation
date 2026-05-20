#!/bin/bash
#
# start-emulator-appium.sh
# Launches Android emulator, waits for boot complete, then starts Appium.
#

set -euo pipefail

AVD_NAME="aosp_api33"
EMULATOR_PORT="5554"
BOOT_TIMEOUT="${EMULATOR_BOOT_TIMEOUT:-90}"
WAIT_SETTLE=8

log() { echo "[$(date '+%H:%M:%S')] $*" >&2; }

# ── Step 1: Launch emulator ─────────────────────────────────────
log "Starting Android emulator (avd=$AVD_NAME)..."
emulator -avd aosp_api33 -no-window -no-audio -no-boot-anim -gpu swiftshader_indirect -skin 1080x2220 &

EMULATOR_PID=$!
log "Emulator launched (PID=$EMULATOR_PID, port=$EMULATOR_PORT)"

# ── Wait for emulator to be contactable via adb ─────────────────
log "Waiting up to ${BOOT_TIMEOUT}s for adb device..."
if ! timeout "$BOOT_TIMEOUT" adb wait-for-device; then
  log "WARNING: adb not contactable within ${BOOT_TIMEOUT}s — continuing anyway"
else
  log "adb device detected"
fi

# ── Wait for boot_complete marker via logcat ────────────────────
BOOT_DONE=false
if command -v adb >/dev/null 2>&1; then
  log "Checking boot status via logcat..."
  # Give logcat 5s to catch the marker
  BOOT_OUT=$(timeout 5 adb logcat -d -t 100 2>/dev/null | grep -m1 "boot completed" || true)
  if [[ -n "$BOOT_OUT" ]]; then
    log "Emulator boot confirmed"
    BOOT_DONE=true
  else
    log "Emulator boot not confirmed via logcat — may still be initialising"
  fi
fi

# Stabilisation pause
log "Pausing ${WAIT_SETTLE}s for system to settle..."
sleep "$WAIT_SETTLE"

# ── Step 2: Start Appium ────────────────────────────────────────
log "Starting Appium server (127.0.0.1:4723)..."
pkill -f "node.*appium" 2>/dev/null || true
sleep 1

appium --address 127.0.0.1 --port 4723 &
APPIUM_PID=$!
log "Appium started (PID=$APPIUM_PID)"

# ── Monitor both processes ─────────────────────────────────────
log "Emulator (PID=$EMULATOR_PID) + Appium (PID=$APPIUM_PID) running."
log "Service is UP — waiting for processes to exit..."

# Wait on emulator; if it dies, restart this script entirely
# Appium auto-restarts via systemd, emulator needs the wrapper
wait $EMULATOR_PID 2>/dev/null || true
EXIT_CODE=$?

log "Emulator exited (code=$EXIT_CODE) — restarting..."
sleep 5
exec "$0" "$@"