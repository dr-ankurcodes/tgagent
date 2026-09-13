"""Configuration: environment loading plus every magic number in one place.

The constants here are not arbitrary. Each is either a hard platform limit (Telegram's
4096-character message cap, its ~1 message/second per-chat budget) or a value derived from
observed Qoder API behaviour recorded in README.md. Change them deliberately.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger("tgagent.config")

ROOT = Path(__file__).resolve().parent.parent

# --- Telegram hard limits -----------------------------------------------------------
# These apply to the DECODED text, so we measure them against our escaped output.
TG_MESSAGE_LIMIT = 4096
TG_CALLBACK_DATA_LIMIT = 64
TG_UPLOAD_MAX_BYTES = 50 * 1024 * 1024  # bot sending a document
TG_DOWNLOAD_MAX_BYTES = 20 * 1024 * 1024  # bot retrieving a user's file via getFile
TG_SEND_INTERVAL_S = 1.05  # ~1 message/s per chat; a hair over, to stay clear of flood waits

# --- Qoder hard limits --------------------------------------------------------------
# Documented multipart limit for POST /files. Distinct from the JSON request-body cap:
# an upload is streamed as multipart, so a 5 MB file is accepted where a 5 MB inline
# base64 blob would not be.
QODER_UPLOAD_MAX_BYTES = 5 * 1024 * 1024
QODER_EVENTS_PAGE_MAX = 100

# --- Rendering ---------------------------------------------------------------------
# Edits share Telegram's ~1 msg/s per-chat budget, and rapid repeated edits on one message
# escalate flood waits. Coalesce on a timer, never on token count.
THROTTLE_MS = 1200
STATUS_THROTTLE_MS = 3000
TYPING_REPEAT_S = 4  # sendChatAction expires after 5s
TOOL_LINES_KEPT = 6  # older tool calls fold into "+N more"
TOOL_SUMMARY_CHARS = 160

# Quiet period after the last inbound message before a turn is dispatched. Typing three
# quick lines then costs one billable round-trip instead of three, and the agent sees one
# coherent thought rather than answering each fragment in isolation. Measured from the most
# recent enqueue, so a burst settles shortly after its last line, while a message that has
# already been sitting in the queue (e.g. typed during a long turn) dispatches immediately.
#
# Resolved through Settings, NOT read from os.environ here: this module is imported long
# before load_settings() copies .env into the environment, so an override read at import time
# would only ever see a real exported variable and silently ignore .env.
DEFAULT_BURST_DEBOUNCE_S = 6.0

# Telegram's HTML spoiler: click-to-reveal. Used to collapse a tool call's argument detail so
# the tool NAME stays visible at a glance without the chat being dominated by long command
# lines. This is Telegram's exact required markup — an invented tag such as <tg-spoiler> is
# rejected with "Can't parse entities", which degrades the whole conversation to plain text.
SPOILER_OPEN = '<span class="tg-spoiler">'
SPOILER_CLOSE = "</span>"

# Tool arguments longer than this are collapsed into a spoiler; shorter ones stay inline,
# where an extra tap to reveal "/data/x.png" would be pure friction.
TOOL_SPOILER_MIN_CHARS = 40

# --- Handlers -----------------------------------------------------------------------
ID_REMINDER_S = 3600  # /start echoes the caller's own user id at most this often


# --- Concurrency -------------------------------------------------------------------
# Each live conversation holds one open SSE connection, and a conversation beyond the cap
# degrades to polling rather than failing — so this is a socket-and-battery budget on a phone,
# not a correctness limit. Four covers the realistic shape of this deployment (two people, each
# with a DM and a group) without anyone silently landing in the slower poll fallback.
DEFAULT_MAX_LIVE_STREAMS = 4
POLL_FALLBACK_INTERVAL_S = 5  # used when the live-stream cap is reached
PUMP_RETRY_S = 5  # backoff after the inbound pump fails; the queue is durable so retrying is safe
SSE_STALL_TIMEOUT_S = 90  # no frame at all for this long -> reconnect

# How many rendered event ids a conversation remembers. A rejected SSE cursor forces a walk
# of the whole history, and without this every past answer would be posted to the chat again
# and every past credit span counted twice. Bounded so the table cannot grow without limit.
RENDER_DEDUPE_KEEP = 2000

# Pruning counts every dedupe row for the conversation, so doing it on every acknowledgement
# made a long conversation quadratic. Batch it instead; the ceiling above is then overshot by
# at most this many rows before the next prune brings it back.
RENDER_DEDUPE_PRUNE_EVERY = 64

# --- Local transcript ------------------------------------------------------------------
# Every event is also appended to conversation_history so a conversation can be resumed on a
# fresh session after its cloud session becomes unreachable (a rotated PAT makes every
# session the bot knew about return 404). Oldest events are dropped past this ceiling: a
# transcript that grows forever would eventually exceed the model's context window, and the
# oldest turns carry the least of the working state.
HISTORY_MAX_EVENTS = 4000

# Character budget for the transcript sent as context on a resume. Deliberately well under
# even the smallest 180k-token window, because it shares that window with the system prompt,
# the tool definitions and the new message the user actually sent.
HISTORY_CONTEXT_BUDGET = 24000

# Retained copies of files that crossed a conversation, so they can be re-uploaded to a new
# account when a conversation is resumed. Deliberately NOT under tmp_root(): the OS clears
# /tmp and a phone reboot wipes it, whereas a resume may happen weeks later.
FILE_CACHE_DIRNAME = "filecache"

# Ceiling on the whole cache. Past this, the oldest files are evicted until it fits — a phone
# has little storage, and an unbounded cache of every file ever sent would eventually fill it.
FILE_CACHE_MAX_BYTES = 200 * 1024 * 1024

# --- Sessions ----------------------------------------------------------------------
WORKSPACE_RECLAIM_HOURS = 24  # documented ephemeral-storage reclaim window

# Placeholder title for a conversation opened with /new or the "New chat" button, before the
# user has said anything to name it after. on_message replaces it on the first real message so
# /sessions never shows a meaningless "new conversation".
DEFAULT_CONVO_TITLE = "new conversation"

# How many sessions to fetch at once during boot reconciliation. Sequential fetches made
# startup cost grow linearly with the number of conversations ever created.
RECONCILE_CONCURRENCY = 4

# Ceiling on silent conversation rebuilds per chat within one process life. A rotated PAT
# makes every session the bot knew about return 404, and rebuilding from the local transcript
# is the recovery — but a session that vanishes again straight after being created means
# something is wrong that retrying cannot fix, so this stops the rebuild loop and says so.
AUTO_RESUME_MAX_ATTEMPTS = 3

# inbound_queue state for rows carried across to a replacement conversation. Distinct from
# 'sent' so a migrated message is never mistaken for one the dead session already accepted.
RESUME_QUEUE_STATE = "moved"


# --- Money -------------------------------------------------------------------------
DEFAULT_CREDIT_BUDGET = 1000.0
BUDGET_WARN_FRACTION = 0.80

# --- Models ------------------------------------------------------------------------
# qmodel_38max = Qwen3.8-Max: vision-capable, price_factor 0.2, max_input_tokens 180000. It
# advertises efforts (low / medium / xhigh), so /effort can bring it down from its `xhigh`
# default — which matters more than the price factor does, since effort multiplies the real
# cost of every request.
#
# The cheaper alternative is qmodel (Qwen3.7-Plus, 0.04, 1M-token window), but it advertises no
# efforts at all, so it cannot be tuned down. See README sections 10 and 14.
DEFAULT_MODEL = "qmodel_38max"
DEFAULT_CONTEXT_WINDOW = 200000

# Canonical reasoning-effort levels, in ascending order, for validation and display. A model
# only accepts the subset it advertises in its own `efforts` array.
EFFORT_LEVELS = ("none", "low", "medium", "high", "xhigh", "max")

# Effort seeded into a new user row. Left unset so each model runs at its own catalog
# `default_effort` — which for ten of the eighteen current models is `max`, `xhigh` or `high`,
# the expensive end of the range. Override with TGAGENT_EFFORT, or per user with /effort.
#
# The API hard-rejects a level the target model does not advertise (400 "Field 'model.effort'
# is not supported by model '<id>'"), so a global default here cannot be applied blindly:
# model_ref drops any effort the model does not list. Six models advertise no efforts at all.
DEFAULT_EFFORT: str | None = None

# --- Files -------------------------------------------------------------------------
UPLOAD_MOUNT_DIR = "/data/workspace/uploads"

AGENT_TOOLSET = "agent_toolset_20260401"
DEFAULT_TOOLS = [
    "Bash",
    "Read",
    "Write",
    "Edit",
    "Glob",
    "Grep",
    "WebFetch",
    "WebSearch",
    "ImageSearch",
    "ImageGen",
    "DeliverArtifacts",
]

ENV_NAME = "tgagent-env"
ENV_PACKAGES = {"pip": ["python-pptx", "pillow", "pypdf", "python-docx"]}
ENV_SETUP_SCRIPT = "set -euo pipefail\nmkdir -p /data/workspace/uploads\n"

# One agent PER USER, not one shared agent. An agent carries the model, the system prompt
# and the tool permissions, and a session inherits all three from it — so with a single
# shared agent there is no way to honour /model for one user without changing it for
# everyone. The environment stays shared: it is only a template (packages plus a setup
# script) and holds no per-user data, so it is not an isolation boundary either way.
AGENT_NAME_PREFIX = "tgagent-user-"


def agent_name(tg_user_id: int) -> str:
    return f"{AGENT_NAME_PREFIX}{tg_user_id}"


# Memory stores are named the same way as the agents they belong to, so the two can be
# paired up at a glance in the console. Single-sourced here because qmemory used to repeat
# the literal, and renaming the prefix would then have silently desynchronised the two.
MEMSTORE_NAME_PREFIX = AGENT_NAME_PREFIX


def memstore_name(tg_user_id: int) -> str:
    return f"{MEMSTORE_NAME_PREFIX}{tg_user_id}"


AGENT_SYSTEM = (
    "You are a capable general assistant reached through Telegram. You have a full "
    "Linux sandbox with Bash, file tools, web search and fetch, image generation, and image "
    "search.\n\n"
    "How to work well here:\n"
    "- Your replies are rendered into Telegram chat messages. Prefer clear prose and markdown "
    "that survives being split across messages. Avoid very wide tables.\n"
    "- When you create a file the user should receive (a .pptx, .pdf, image, dataset, script), "
    "write it under /data/ and then call DeliverArtifacts on it. Files you do not deliver are "
    "invisible to the user.\n"
    "- When the user attaches a file, it is mounted under /data/workspace/uploads/ and the "
    "message tells you the exact path. Use Read on it; Read decodes images natively, so you "
    "can genuinely see pictures.\n"
    "- For research, actually use WebSearch and WebFetch rather than answering from memory, "
    "and cite the URLs you used.\n"
    "- Your sandbox filesystem is ephemeral and may be reclaimed after 24 hours of inactivity. "
    "Anything the user needs to keep must be delivered as an artifact.\n"
    "- Be concise. Do not narrate your plan before acting; act, then summarise what you did."
)


@dataclass(frozen=True)
class Settings:
    qoder_pat: str
    qoder_api_base: str
    tg_bot_token: str
    allowed_ids: frozenset[int]
    db_path: Path | None = None
    max_live_streams: int = DEFAULT_MAX_LIVE_STREAMS
    credit_budget: float = DEFAULT_CREDIT_BUDGET
    default_model: str = DEFAULT_MODEL
    default_effort: str | None = DEFAULT_EFFORT
    burst_debounce_s: float = DEFAULT_BURST_DEBOUNCE_S
    log_level: str = "INFO"

    @property
    def allowlist_open(self) -> bool:
        """An empty allowlist means discovery mode: admit nobody, but log who knocks."""
        return not self.allowed_ids


def _parse_ids(raw: str) -> frozenset[int]:
    """Parse TG_ALLOWED_IDS as a comma or semicolon separated list of Telegram user ids.

    Ids are integers on purpose: they are immutable, whereas a Telegram @username can be
    renamed and then claimed by someone else, and is optional so some accounts have none.
    """
    ids: set[int] = set()
    for part in raw.replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.add(int(part))
        except ValueError:
            raise ValueError(f"TG_ALLOWED_IDS entry {part!r} is not an integer") from None
    return frozenset(ids)


def _parse_env_value(raw: str) -> str:
    """Strip one layer of matching quotes, or a trailing inline comment.

    Without this, ``QODER_PAT="abc"`` is used with the quotes still attached and every
    request 401s with no hint why. Quoting is also how you keep a literal ``#``.
    """
    value = raw.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
        return value[1:-1]
    # Require the space before '#' so a value containing '#' unquoted still survives.
    return value.split(" #", 1)[0].strip()


def _parse_effort(raw: str) -> str | None:
    """Validate TGAGENT_EFFORT against the canonical levels.

    An unrecognised value is a configuration error, not something to ignore: ``model_ref`` drops
    any effort the target model does not advertise, so a typo would silently never apply and the
    operator would be left wondering why the cost did not move.
    """
    value = raw.strip().lower()
    if not value:
        return DEFAULT_EFFORT
    if value not in EFFORT_LEVELS:
        raise ValueError(
            f"TGAGENT_EFFORT {raw!r} is not a reasoning effort level "
            f"(expected one of: {', '.join(EFFORT_LEVELS)})"
        )
    return value


def load_settings(env_path: Path | None = None) -> Settings:
    """Read .env then real environment variables. Real env wins, so Termux can override."""
    path = env_path or ROOT / ".env"
    if path.exists():
        for raw in path.read_text().splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), _parse_env_value(value))

    pat = os.environ.get("QODER_PAT", "").strip()
    token = os.environ.get("TG_BOT_TOKEN", "").strip()
    if not pat:
        raise RuntimeError("QODER_PAT is not set (see .env.example)")
    if not token:
        raise RuntimeError("TG_BOT_TOKEN is not set (see .env.example)")

    return Settings(
        qoder_pat=pat,
        qoder_api_base=os.environ.get("QODER_API_BASE", "https://api.qoder.com").strip().rstrip("/"),
        tg_bot_token=token,
        allowed_ids=_parse_ids(os.environ.get("TG_ALLOWED_IDS", "")),
        db_path=Path(os.environ.get("TGAGENT_DB", str(ROOT / "tgagent.db"))),
        max_live_streams=int(os.environ.get("MAX_LIVE_STREAMS", DEFAULT_MAX_LIVE_STREAMS)),
        credit_budget=float(os.environ.get("CREDIT_BUDGET", DEFAULT_CREDIT_BUDGET)),
        default_model=os.environ.get("TGAGENT_MODEL", DEFAULT_MODEL).strip(),
        default_effort=_parse_effort(os.environ.get("TGAGENT_EFFORT", "")),
        burst_debounce_s=float(os.environ.get("BURST_DEBOUNCE_S", DEFAULT_BURST_DEBOUNCE_S)),
        log_level=os.environ.get("TGAGENT_LOG", "INFO").strip().upper(),
    )


def restrict(path: Path, mode: int) -> None:
    """Set permissions on a path we own, ignoring a filesystem that refuses.

    Everything below holds conversation content: the database is every transcript this bot has
    seen, the file cache is the bytes users uploaded and the agent produced. SQLite and
    ``Path.write_bytes`` both create with the process umask — 022 on most systems — which
    leaves all of it world-readable to any other local account.

    Best-effort on purpose: a filesystem that does not support chmod must not stop the bot
    from starting.
    """
    try:
        path.chmod(mode)
    except OSError as exc:
        log.debug("could not set %o on %s: %s", mode, path, exc)


def tmp_root() -> Path:
    """Scratch space for downloaded artifacts. Honours TMPDIR, which Termux sets.

    Private because /tmp is world-writable: an artifact left here between download and
    delivery is otherwise readable by every other local account.
    """
    base = Path(os.environ.get("TMPDIR", "/tmp")) / "tgagent"
    base.mkdir(parents=True, exist_ok=True)
    restrict(base, 0o700)
    return base


def file_cache_root() -> Path:
    """Durable storage for retained conversation files.

    Lives beside the database rather than in tmp_root(): these copies exist so a conversation
    can be resumed weeks later under a different PAT, and scratch space does not survive a
    reboot. Override with TGAGENT_FILECACHE.
    """
    override = os.environ.get("TGAGENT_FILECACHE")
    base = Path(override) if override else ROOT / FILE_CACHE_DIRNAME
    base.mkdir(parents=True, exist_ok=True)
    # Re-applied on every call, so a cache directory an older build created world-readable is
    # tightened the next time anything is retained.
    restrict(base, 0o700)
    return base
