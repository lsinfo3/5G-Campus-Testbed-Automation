#!/bin/bash
set -xe
export TERM=xterm-256color

# 1. Define and MANUALLY create the socket directory
SOCKET_DIR="/tmp/tmux-1000"
SOCKET="$SOCKET_DIR/default"
SESSION="emulator"

# This is the missing link:
mkdir -p "$SOCKET_DIR"
chown gnb:gnb "$SOCKET_DIR"
chmod 700 "$SOCKET_DIR"

# 2. Start the session using the explicit socket
# We use 'sleep infinity' to ensure the session doesn't close if the emulator fails
if ! tmux -S "$SOCKET" has-session -t "$SESSION" 2>/dev/null; then
    tmux -S "$SOCKET" new-session -d -s "$SESSION" -x 128 -y 32 -n "main" "sleep infinity"
    sleep 1
fi

tmux -S "$SOCKET" send-keys -t "$SESSION" "appium" C-m
sleep 10
tmux -S "$SOCKET" split-window -v -t "$SESSION"
tmux -S "$SOCKET" send-keys -t "$SESSION:main" "emulator -avd aosp_api33 -no-window -no-audio -no-boot-anim -gpu swiftshader_indirect -skin 1080x2220" C-m
tmux -S "$SOCKET" select-layout -t "$SESSION" tiled