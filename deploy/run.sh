#!/data/data/com.termux/files/usr/bin/bash
# Run the bot in the foreground with a wake lock held.
#
# Use inside tmux so that closing the Termux notification or the terminal does not SIGHUP it:
#   tmux new -s tgagent
#   bash deploy/run.sh
#   (detach with ctrl-b d, reattach with: tmux attach -t tgagent)
#
# Logs go to stderr AND are appended to logs/tgagent.log, rotated by size so a phone's storage
# is not slowly filled by an unbounded log.

set -euo pipefail

cd "$(dirname "$0")/.."

LOG_DIR="logs"
LOG_FILE="$LOG_DIR/tgagent.log"
PID_FILE="$LOG_DIR/tgagent.pid"   # shared with deploy/boot-start.sh
MAX_LOG_BYTES=5242880   # 5 MB

mkdir -p "$LOG_DIR"

# --- single instance -------------------------------------------------------------------
# Two pollers on one bot token fight over the same update queue and both lose messages. The
# boot script and this one share a pid file, so starting one while the other runs is refused
# rather than silently doubling up -- and both append to the same log, where a second writer
# would also rotate the file out from under the first.
if [[ -f "$PID_FILE" ]]; then
    existing="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "$existing" ]] && kill -0 "$existing" 2>/dev/null; then
        echo "tgagent is already running (pid $existing); not starting a second instance." >&2
        echo "Stop it first, or attach to it:  tmux attach -t tgagent" >&2
        exit 1
    fi
    echo "removing stale pid file (pid ${existing:-unknown} is not running)"
    rm -f "$PID_FILE"
fi
echo $$ > "$PID_FILE"

WAKELOCK_ACQUIRED=0

release() {
    if [[ "$(cat "$PID_FILE" 2>/dev/null || true)" == "$$" ]]; then
        rm -f "$PID_FILE"
    fi
    if [[ "$WAKELOCK_ACQUIRED" -eq 1 ]]; then
        # The wake lock is global: a boot-started instance may still need the CPU awake, so
        # only release it when no other tgagent process is left. pgrep is not always present.
        if command -v pgrep >/dev/null 2>&1 && pgrep -f "python -m tgagent" >/dev/null 2>&1; then
            :
        else
            termux-wake-unlock 2>/dev/null || true
        fi
    fi
}
trap release EXIT

# Rotate once at startup rather than continuously; good enough for a personal bot and it
# avoids depending on logrotate, which Termux does not ship by default.
if [[ -f "$LOG_FILE" ]] && (( $(stat -c %s "$LOG_FILE" 2>/dev/null || echo 0) > MAX_LOG_BYTES )); then
    mv -f "$LOG_FILE" "$LOG_FILE.1"
    echo "rotated $LOG_FILE"
fi

# Keep the CPU awake. This does NOT bypass battery optimisation - set Termux to
# "Unrestricted" in Android settings as well, or Doze will still stop the process.
if command -v termux-wake-lock >/dev/null 2>&1; then
    termux-wake-lock
    WAKELOCK_ACQUIRED=1
    echo "wake lock acquired"
fi

echo "starting tgagent; logging to $LOG_FILE"
# PYTHONUNBUFFERED matters here: without it, a crash can lose the last seconds of log output,
# which is exactly the output you need to diagnose the crash.
export PYTHONUNBUFFERED=1

# PIPESTATUS, not $?: with a pipe, $? reports tee's status and would hide the bot crashing.
python -m tgagent 2>&1 | tee -a "$LOG_FILE"
exit "${PIPESTATUS[0]}"
