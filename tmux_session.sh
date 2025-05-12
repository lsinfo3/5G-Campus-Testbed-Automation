#!/bin/bash

TMUX_SESSION_NAME="5G-Masterarbeit"


tmux set pane-border-status top
# tmux set -g pane-border-format "#{pane_index} #{pane_current_command}"
tmux set pane-border-format "#T"

tmux -u new-window -t "$TMUX_SESSION_NAME" -n '󰌘' 'printf "\033]2;%s\033\\" "core";ssh core; zsh'
tmux set -st "$TMUX_SESSION_NAME" pane-border-status top
tmux set -st "$TMUX_SESSION_NAME" pane-border-format "#T"

tmux -u split-window -h -t "$TMUX_SESSION_NAME" 'printf "\033]2;%s\033\\" "gNB";ssh gnodeb; zsh'
tmux set -st "$TMUX_SESSION_NAME" pane-border-status top
tmux set -st "$TMUX_SESSION_NAME" pane-border-format "#T"


tmux -u new-window -t "$TMUX_SESSION_NAME" -n ''
tmux -u new-window -t "$TMUX_SESSION_NAME" -n '󱃖'
tmux -u new-window -t "$TMUX_SESSION_NAME" -n '󰆼' "cd /home/lks/Documents/datastore/5g-masterarbeit/; zsh"

