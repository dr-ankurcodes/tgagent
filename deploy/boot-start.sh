#!/data/data/com.termux/files/usr/bin/bash
# Termux:Boot script. Installed to ~/.termux/boot/tgagent by deploy/termux_setup.sh.
#
# Termux:Boot must be installed from F-Droid and opened once after install, otherwise the
# BOOT_COMPLETED receiver is never registered and this script will silently never run.
#
# On boot the bot reconciles from SQLite and re-attaches to any Qoder session that is still
# running, so a reboot mid-turn does not lose the answer.
#
# Deliberately NOT `set -e`: this is a restart loop, and a failing command must be survived
# rather than abort the boot script.

# termux-wake-lock as early as possible: the rest of startup can be slow on a cold boot.
WAKELOCK_ACQUIRED=0
if command -v termux-wake-lock >/dev/null 2>&1; then
    termux-wake-lock 2>/dev/null && WAKELOCK_ACQUIRED=1
fi

# Resolved to the real checkout path at install time by deploy/termux_setup.sh, because the
# installed copy lives in ~/.termux/boot/ and cannot find the project relative to itself.
# Export TGAGENT_DIR to override.
#
# A plain assignment, not a ${TGAGENT_DIR:-<path>} default: bash parses quotes inside a
# parameter-expansion default even when the whole expression is double-quoted, so a path
# containing an apostrophe produced a script that did not parse.
TGAGENT_PROJECT_DIR="__PROJECT_DIR__"
TGAGENT_DIR="${TGAGENT_DIR:-$TGAGENT_PROJECT_DIR}"

# Checked by CONTENT rather than by looking for an unsubstituted placeholder: one test then
# covers the placeholder never having been substituted, the checkout having moved, and
# TGAGENT_DIR pointing somewhere wrong.
#
# Without it, a bad path used to fall through to `cd` and the restart loop below spun forever
# at 10-second intervals, surviving every reboot, with the reason buried in a log nobody reads.
if [[ ! -f "$TGAGENT_DIR/tgagent/__main__.py" ]]; then
    echo "tgagent: '$TGAGENT_DIR' is not the project directory." >&2
    echo "       Re-run deploy/termux_setup.sh to bake in the path, or export TGAGENT_DIR." >&2
    exit 1
fi

cd "$TGAGENT_DIR" || exit 1

LOG_DIR="logs"
LOG_FILE="$LOG_DIR/tgagent.log"
PID_FILE="$LOG_DIR/tgagent.pid"
MAX_LOG_BYTES=5242880   # 5 MB, matching deploy/run.sh

mkdir -p "$LOG_DIR"
export PYTHONUNBUFFERED=1

# --- single instance -------------------------------------------------------------------
# Two pollers on one bot token fight over the same update queue and both lose messages, so a
# second instance must refuse to start rather than run alongside one the user started by hand.
instance_running() {
    [[ -f "$PID_FILE" ]] || return 1
    local pid
    pid="$(cat "$PID_FILE" 2>/dev/null)"
    [[ -n "$pid" ]] || return 1
    kill -0 "$pid" 2>/dev/null
}

if instance_running; then
    echo "tgagent: already running (pid $(cat "$PID_FILE")); not starting a second instance." >&2
    exit 0
fi
echo $$ > "$PID_FILE"

release() {
    # Only clear the pid file if it is still ours, so we never remove a later instance's.
    if [[ "$(cat "$PID_FILE" 2>/dev/null)" == "$$" ]]; then
        rm -f "$PID_FILE"
    fi
    if [[ "$WAKELOCK_ACQUIRED" -eq 1 ]]; then
        # The wake lock is global. An instance the user started by hand may still need the CPU
        # awake, so only drop it when no other tgagent process is left. Our own python has
        # already exited by the time this trap runs. pgrep is not always present on Termux.
        if command -v pgrep >/dev/null 2>&1 && pgrep -f "python -m tgagent" >/dev/null 2>&1; then
            :
        else
            termux-wake-unlock 2>/dev/null || true
        fi
    fi
}
trap release EXIT

# Rotate by size, as deploy/run.sh does. The boot path used to append forever, which on a
# device that reboots regularly filled the phone's storage and defeated run.sh's rotation.
rotate_log() {
    [[ -f "$LOG_FILE" ]] || return 0
    local size
    size="$(stat -c %s "$LOG_FILE" 2>/dev/null || echo 0)"
    if (( size > MAX_LOG_BYTES )); then
        mv -f "$LOG_FILE" "$LOG_FILE.1"
        echo "rotated $LOG_FILE" >> "$LOG_DIR/boot.log"
    fi
}

# Restart in a loop. Android kills processes for reasons outside our control, and the correct
# response is to come back and reconcile, not to stay dead.
while true; do
    rotate_log
    echo "=== boot-start $(date -Is) ===" >> "$LOG_DIR/boot.log"
    python -m tgagent >> "$LOG_FILE" 2>&1
    status=$?
    echo "=== exited $status $(date -Is) ===" >> "$LOG_DIR/boot.log"
    [[ $status -eq 0 ]] && break
    # Someone started it by hand while we were waiting: let that instance own the bot.
    if [[ -f "$PID_FILE" ]] && [[ "$(cat "$PID_FILE" 2>/dev/null)" != "$$" ]]; then
        echo "=== another instance took over; stopping $(date -Is) ===" >> "$LOG_DIR/boot.log"
        break
    fi
    sleep 10
done
