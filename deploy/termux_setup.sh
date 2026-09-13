#!/data/data/com.termux/files/usr/bin/bash
# One-time Termux setup.
#
# Run from the project directory:  bash deploy/termux_setup.sh
#
# Android specifics that matter, in rough order of how often they bite:
#
# 1. Battery optimisation. This is the single biggest cause of a bot that "randomly" stops.
#    Android settings -> Apps -> Termux -> Battery -> Unrestricted.
#
# 2. Android 12+ phantom process killer. It terminates child processes even when Termux holds
#    a wake lock. Disable it once over adb from a computer:
#      adb shell settings put global settings_enable_monitor_phantom_procs false
#      adb shell "/system/bin/device_config set_sync_disabled_for_tests persistent"
#      adb shell "device_config put activity_manager max_phantom_processes 2147483647"
#    Some OEM builds (Xiaomi, Samsung, Oppo) add their own killers on top; those need their
#    own per-vendor settings. Treat occasional death as inevitable either way - the bot is
#    built to reconcile from SQLite on every boot.
#
# 3. Wake lock. deploy/run.sh calls termux-wake-lock, which keeps the CPU alive but does NOT
#    exempt you from battery optimisation. You need both.
#
# 4. Termux:Boot. Install it from F-Droid (not the Play Store build) and place
#    deploy/boot-start.sh at ~/.termux/boot/tgagent, then chmod +x it. Opening Termux:Boot once
#    after install is required to register the receiver.

set -euo pipefail

cd "$(dirname "$0")/.."
PROJECT_DIR="$(pwd)"
echo "Setting up in $PROJECT_DIR"

if [[ ! -d /data/data/com.termux ]]; then
    echo "This script is for Termux on Android. On desktop just use a venv:"
    echo "  python3 -m venv .venv && .venv/bin/pip install -e ."
    exit 1
fi

echo
echo "--> Updating packages"
pkg update -y
pkg install -y python git tmux termux-api

echo
echo "--> Installing Python dependencies"
# python-telegram-bot and httpx are pure Python, so nothing here needs clang or python headers.
# That is exactly why this stack was chosen for Termux.
pip install --upgrade pip
pip install "python-telegram-bot[job-queue]==22.8" "httpx>=0.27,<0.29"

echo
echo "--> Verifying imports"
python -c "import telegram, httpx; print('  python-telegram-bot', telegram.__version__); print('  httpx', httpx.__version__)"

echo
echo "--> Checking configuration"
if [[ ! -f .env ]]; then
    echo "  .env is missing. Copy it and fill in your credentials:"
    echo "    cp .env.example .env && chmod 600 .env"
    exit 1
fi
chmod 600 .env
if ! grep -q '^QODER_PAT=.\+' .env; then
    echo "  QODER_PAT is empty in .env"
    exit 1
fi
if ! grep -q '^TG_BOT_TOKEN=.\+' .env; then
    echo "  TG_BOT_TOKEN is empty in .env"
    exit 1
fi
echo "  .env looks usable"

echo
echo "--> Installing the boot script"
BOOT_DIR="$HOME/.termux/boot"
mkdir -p "$BOOT_DIR"

# Install with the project path baked in, by SPLITTING the template at the placeholder rather
# than substituting into it. bash 5.2 gives '&' in a ${var//pat/repl} replacement the meaning
# "the whole match", so a path containing an ampersand silently corrupted the installed script;
# escaping it depends on the bash version. Passing the path as its own printf argument involves
# no replacement-string semantics, so no character in it can be special. This replaces only the
# FIRST occurrence, which is why the count is asserted.
placeholder='__PROJECT_DIR__'
occurrences="$(grep -o "$placeholder" deploy/boot-start.sh | wc -l)"
if [[ "$occurrences" -ne 1 ]]; then
    echo "  deploy/boot-start.sh must contain $placeholder exactly once (found $occurrences)" >&2
    exit 1
fi
boot_script="$(< deploy/boot-start.sh)"
# The path lands inside a double-quoted assignment in the generated script, so the four
# characters that are special there must be escaped. Without this, a path containing $(...) or
# a backtick would be EVALUATED on every boot — and this script runs unattended from
# Termux:Boot. Backslash is escaped first, or it would double-escape the ones added below.
escaped="${PROJECT_DIR//\\/\\\\}"
escaped="${escaped//\"/\\\"}"
escaped="${escaped//\$/\\\$}"
escaped="${escaped//\`/\\\`}"
printf '%s%s%s\n' \
    "${boot_script%%$placeholder*}" "$escaped" "${boot_script#*$placeholder}" \
    > "$BOOT_DIR/tgagent"
chmod +x "$BOOT_DIR/tgagent"
echo "  installed to $BOOT_DIR/tgagent (project dir: $PROJECT_DIR)"
if grep -q "$placeholder" "$BOOT_DIR/tgagent"; then
    echo "  WARNING: the project path was not substituted; the boot script will refuse to run." >&2
    exit 1
fi
bash -n "$BOOT_DIR/tgagent" || { echo "  the installed boot script does not parse" >&2; exit 1; }

echo
echo "--> Provisioning"
echo "  nothing to do here: the bot provisions its own Qoder environment on first"
echo "  start, and each user's agent and memory store on their first conversation."

cat <<'EOF'

Setup complete. Now do the manual parts:

  1. Android settings -> Apps -> Termux -> Battery -> Unrestricted
  2. Install Termux:Boot from F-Droid and open it once (registers the boot receiver)
  3. Optionally disable the phantom process killer over adb (see the comments at the
     top of this script)

Then start it:

  bash deploy/run.sh            # foreground, logs to stderr
  tmux new -s tgagent             # or inside tmux so closing Termux does not kill it
    bash deploy/run.sh

It restarts on boot automatically via ~/.termux/boot/tgagent.
EOF
