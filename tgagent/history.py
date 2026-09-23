"""Local conversation transcript, so a conversation survives losing its cloud session.

A Qoder session belongs to whichever PAT created it. Rotate the token and every session the
bot knew about returns 404, which used to mean the conversation was simply gone: reconcile
soft-deleted it and the user started over with no memory of what they had been working on.

So every event is appended here as it renders. That is a local record, not a cloud backup —
it lets a conversation be *resumed* on a fresh session under a new PAT by replaying the
transcript as context, which is the only thing possible given that sessions are not portable
between accounts.

What is recorded is text and tool activity only. Tool calls are summarised, never re-executed:
replaying a past ``Bash`` call would run it again, in a different sandbox, at a different time.

Storage is bounded per conversation, oldest-first. A transcript that grows without limit would
eventually exceed the model's context window, and the oldest turns are the least useful part of
it — recent turns carry the working state.
"""

from __future__ import annotations

import json
import logging
import sqlite3
import time
from pathlib import Path

from . import config, tg_html
from .db import Database, utcnow
from .qstream import Frame

log = logging.getLogger("tgagent.history")

# Tool input/output blobs are truncated before storage. They are context for a summary, not a
# faithful archive, and an unbounded Read result would otherwise dominate the transcript.
TOOL_BLOB_MAX_CHARS = 2000

# Grace period before an unreferenced cache file is treated as an orphan and swept. A copy a
# concurrent retain_file has just written but not yet recorded must not be raced away.
ORPHAN_GRACE_S = 3600

# Reserved headroom for build_transcript's header, so the body never fills the whole budget and
# is then pushed over it by the header prepended afterwards.
_HEADER_RESERVE = 512

# Pruning counts every row in the transcript, so doing it on every append made a long
# conversation quadratic. seq is monotonic per conversation, which gives a stateless trigger:
# check every Nth insert and the ceiling is overshot by at most N rows.
PRUNE_EVERY = 64


def record_frame(db: Database, convo_id: int, frame: Frame) -> None:
    """Append one rendered event to the local transcript. No-ops for frames worth no storage.

    Only the events that carry conversation meaning are stored. Status frames, credit spans and
    artifact markers are all reconstructible from elsewhere or are noise in a transcript, and
    ``agent.thinking`` is skipped because the API delivers it with no content at all — storing
    it would append an empty row per turn, forever.
    """
    if frame.kind != "event":
        return  # deltas are fragments; the buffered event is the record

    ftype = frame.type
    payload = frame.payload

    if ftype == "user.message":
        text = _blocks_to_text(payload.get("content"))
        if text.strip():
            _append(db, convo_id, "user", "text", text=text)
        return

    if ftype == "agent.message":
        text = frame.text()
        if text.strip():
            _append(db, convo_id, "agent", "text", text=text)
        return

    if ftype == "agent.tool_use":
        _append(
            db, convo_id, "agent", "tool_use",
            tool_name=str(payload.get("name") or "tool"),
            tool_input=_clip(json.dumps(payload.get("input") or {}, default=str)),
        )
        return

    if ftype == "agent.tool_result":
        _append(
            db, convo_id, "agent", "tool_result",
            tool_name=None,
            tool_input=None,
            tool_output=_clip(_blocks_to_text(payload.get("content"))),
            is_error=bool(payload.get("is_error")),
        )
        return

    # agent.thinking (no content), span.*, session.status_*, agent.artifact_delivered:
    # nothing worth persisting.


def _append(
    db: Database,
    convo_id: int,
    direction: str,
    content_type: str,
    *,
    text: str | None = None,
    tool_name: str | None = None,
    tool_input: str | None = None,
    tool_output: str | None = None,
    is_error: bool = False,
) -> None:
    """One INSERT with the next sequence number, then a prune.

    The SELECT-then-INSERT is not atomic, but every caller runs on the conversation's single
    frame-handling path, so two inserts for one conversation cannot interleave. A UNIQUE
    violation means that assumption broke; log and drop the row rather than kill rendering.
    """
    try:
        row = db.query_one(
            "SELECT COALESCE(MAX(seq), 0) + 1 AS next FROM conversation_history WHERE convo_id = ?",
            (convo_id,),
        )
        seq = int(row["next"]) if row else 1
        db.execute(
            """INSERT INTO conversation_history(
                   convo_id, seq, direction, content_type, text, tool_name,
                   tool_input, tool_output, is_error, created_at)
               VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                convo_id, seq, direction, content_type,
                text, tool_name, tool_input, tool_output, int(is_error), utcnow(),
            ),
        )
    except sqlite3.IntegrityError as exc:
        log.warning("could not record history for convo %s: %s", convo_id, exc)
        return
    if seq % PRUNE_EVERY == 0:
        _prune(db, convo_id)


def _prune(db: Database, convo_id: int) -> None:
    count = db.query_one(
        "SELECT COUNT(*) AS n FROM conversation_history WHERE convo_id = ?", (convo_id,)
    )
    if not count or count["n"] <= config.HISTORY_MAX_EVENTS:
        return
    db.execute(
        """DELETE FROM conversation_history WHERE convo_id = ? AND seq NOT IN (
               SELECT seq FROM conversation_history WHERE convo_id = ?
               ORDER BY seq DESC LIMIT ?
           )""",
        (convo_id, convo_id, config.HISTORY_MAX_EVENTS),
    )


def _blocks_to_text(content) -> str:
    """Flatten a content-block array to plain text, ignoring non-text blocks."""
    if not content:
        return ""
    if isinstance(content, str):
        return content
    parts = []
    for block in content:
        if isinstance(block, dict) and block.get("type") == "text":
            parts.append(block.get("text") or "")
        elif isinstance(block, str):
            parts.append(block)
    return "".join(parts)


def _clip(text: str | None) -> str | None:
    if text is None:
        return None
    if len(text) <= TOOL_BLOB_MAX_CHARS:
        return text
    return text[:TOOL_BLOB_MAX_CHARS] + "…[truncated]"


# --- reading ---------------------------------------------------------------------------


def event_count(db: Database, convo_id: int) -> int:
    row = db.query_one(
        "SELECT COUNT(*) AS n FROM conversation_history WHERE convo_id = ?", (convo_id,)
    )
    return int(row["n"]) if row else 0


def load_events(db: Database, convo_id: int) -> list[sqlite3.Row]:
    """Every stored event for a conversation, oldest first."""
    return db.query(
        """SELECT seq, direction, content_type, text, tool_name, tool_input, tool_output,
                  is_error, created_at
           FROM conversation_history WHERE convo_id = ? ORDER BY seq""",
        (convo_id,),
    )


def build_transcript(
    db: Database,
    convo_id: int,
    *,
    max_chars: int = config.HISTORY_CONTEXT_BUDGET,
    title: str | None = None,
) -> str:
    """Render the stored history as context for a fresh session.

    Keeps the NEWEST turns that fit the budget and says how many older ones were dropped, so
    the agent knows its view is partial rather than silently assuming it has everything.
    Tool activity is compressed to one line per call: the point is to convey what was done
    and concluded, not to re-execute it.
    """
    events = load_events(db, convo_id)
    if not events:
        return ""

    lines: list[str] = []
    for event in events:
        rendered = _render_event(event)
        if rendered:
            lines.append(rendered)

    # Reserve room for the header before filling the body. The header is part of what the model
    # receives, so filling the body right up to max_chars and then prepending it pushed the total
    # over the budget the header itself claimed to respect.
    body_budget = max(0, max_chars - _HEADER_RESERVE)

    # Walk backwards keeping a CONTIGUOUS newest suffix. The old loop `continue`d past a line that
    # did not fit and kept older, smaller ones, so the result could have a hole in the middle
    # while the header called it "the most recent part of the conversation" — the agent would read
    # a gap as continuous context. Stopping at the first line that does not fit keeps that claim
    # true; everything older is counted as dropped.
    kept: list[str] = []
    used = 0
    idx = len(lines)
    while idx > 0:
        line = lines[idx - 1]
        cost = len(line) + 1
        if used + cost > body_budget:
            break
        kept.append(line)
        used += cost
        idx -= 1
    kept.reverse()
    dropped = idx

    header = ["[Previous conversation transcript, restored after the cloud session was lost.]"]
    if title:
        header.append(f"Topic: {tg_html.truncate(title, 120)}")
    if dropped:
        header.append(
            f"{dropped} earlier exchange(s) were dropped to fit the context window; "
            "what follows is the most recent part of the conversation."
        )
    header.append("Continue from here. Do not repeat work already shown as completed.")
    header.append("")

    return "\n".join(header + kept)


def _render_event(event: sqlite3.Row) -> str:
    ctype = event["content_type"]
    if ctype == "text":
        who = "USER" if event["direction"] == "user" else "ASSISTANT"
        body = (event["text"] or "").strip()
        if not body:
            return ""
        return f"[{who}] {body}"

    if ctype == "tool_use":
        name = event["tool_name"] or "tool"
        detail = _summarise_blob(event["tool_input"])
        return f"[TOOL CALL] {name} {detail}".rstrip()

    if ctype == "tool_result":
        marker = "error" if event["is_error"] else "ok"
        detail = _summarise_blob(event["tool_output"])
        return f"[TOOL RESULT {marker}] {detail}".rstrip()

    return ""


def _summarise_blob(blob: str | None) -> str:
    """One short line out of a stored tool input/output blob."""
    if not blob:
        return ""
    text = blob.strip()
    if text.startswith("{") or text.startswith("["):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, dict):
            for key in config.TOOL_SUMMARY_KEYS:
                value = parsed.get(key)
                if isinstance(value, str) and value.strip():
                    return tg_html.truncate(value.strip().splitlines()[0], 160)
    return tg_html.truncate(" ".join(text.split()), 160)


# --- resume context ------------------------------------------------------------------------

_RESUME_CTX_KEY = "resume_context:{convo_id}"


def set_resume_context(db: Database, convo_id: int, transcript: str) -> None:
    """Hold a rebuilt conversation's transcript until its next turn.

    Durable rather than a field on the Conversation, because it has to survive until the user
    actually says something — and on a phone the process can be killed in between, so an
    in-memory field would silently drop the one thing a resume exists to provide.

    This replaces enqueueing the transcript as an ordinary message. The dispatcher records what
    it sends into THIS conversation's transcript, so sending the old transcript as a message
    nested it inside the new one; a second resume then nested the previous resume inside that,
    and the context grew every single time the session was lost.
    """
    if transcript.strip():
        db.kv_set(_RESUME_CTX_KEY.format(convo_id=convo_id), transcript)


def peek_resume_context(db: Database, convo_id: int) -> str | None:
    """The held transcript for a rebuilt conversation, without consuming it.

    Peek and clear are separate steps on purpose. Reading-and-deleting in one call was wrong:
    ``Conversation.__init__`` ran BEFORE the caller stored the transcript, so it consumed
    nothing, and the value then sat in kv unread forever because nothing re-loaded it. Reading
    at dispatch time instead means there is no in-memory copy to go stale, and deleting only
    after the POST succeeds means a 409 or a process kill leaves the context intact for the
    retry — losing it would silently rebuild the session with no memory of the conversation.
    """
    return db.kv_get(_RESUME_CTX_KEY.format(convo_id=convo_id))


def clear_resume_context(db: Database, convo_id: int) -> None:
    """Drop the held transcript once it has been delivered to the model."""
    db.kv_delete(_RESUME_CTX_KEY.format(convo_id=convo_id))


# --- local file copies -----------------------------------------------------------------


def remember_local_file(
    db: Database,
    *,
    convo_id: int,
    file_id: str,
    path: Path,
    size_bytes: int,
    owner_type: str,
    filename: str | None = None,
) -> None:
    """Record a local copy of a file, so it can be re-uploaded after the account changes."""
    try:
        db.execute(
            """INSERT INTO local_files(convo_id, file_id, path, size_bytes, owner_type,
                                       filename, created_at)
               VALUES(?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(file_id, path) DO UPDATE SET
                 convo_id = excluded.convo_id,
                 size_bytes = excluded.size_bytes,
                 filename = excluded.filename""",
            (convo_id, file_id, str(path), size_bytes, owner_type, filename, utcnow()),
        )
    except sqlite3.IntegrityError as exc:
        log.warning("could not record local file %s: %s", file_id, exc)


def retain_file(
    db: Database,
    *,
    convo_id: int,
    file_id: str,
    contents: bytes,
    filename: str,
    owner_type: str,
) -> Path | None:
    """Copy a file into the durable cache and record it. Returns the cached path, or None.

    Best-effort by design: a full disk or an unwritable cache must never cost the user their
    message or their artifact, so every failure is logged and returns None.
    """
    try:
        cache = config.file_cache_root()
        # Sanitised: the filename comes from a Telegram upload or an agent-chosen artifact name
        # and the file id from the API, and either could contain a path separator that would
        # escape the cache directory.
        safe_id = config.safe_path_component(file_id, "unknown")
        safe_name = config.safe_path_component(filename, f"{safe_id}.bin")
        dest = cache / f"{convo_id}_{safe_id}_{safe_name}"
        dest.write_bytes(contents)
    except OSError as exc:
        log.warning("could not retain a local copy of %s: %s", filename, exc)
        return None

    try:
        remember_local_file(
            db,
            convo_id=convo_id,
            file_id=file_id,
            path=dest,
            size_bytes=len(contents),
            owner_type=owner_type,
            filename=safe_name,
        )
        evict_overflow(db)
    except sqlite3.Error as exc:
        # The bytes are on disk but the ledger row could not be written (a full or corrupt DB).
        # This stays best-effort: an unrecorded copy is an orphan the sweep in evict_overflow
        # collects later, whereas letting this propagate would fail the user's message or the
        # artifact AFTER the API had already accepted it — exactly what the docstring promises
        # cannot happen. remember_local_file swallows IntegrityError; this covers the rest
        # (OperationalError on a full disk, and anything evict_overflow's writes can raise).
        log.warning("retained %s on disk but could not record it: %s", filename, exc)
        return dest
    return dest


def evict_overflow(db: Database) -> int:
    """Drop the oldest cached files until the cache fits its ceiling. Returns bytes freed.

    The database row goes with the file: a recorded path that no longer exists would make a
    resume promise a file it cannot deliver.
    """
    rows = db.query(
        "SELECT id, path, size_bytes FROM local_files ORDER BY created_at DESC"
    )
    # Count only bytes that are actually on disk. The old total summed every row including ones
    # whose file had already vanished (the OS cleared scratch, a crash left a partial write), so
    # live files were evicted to compensate for phantom bytes — and the dead rows were never
    # removed, so the phantom total persisted across every call.
    live: list[sqlite3.Row] = []
    total = 0
    for row in rows:
        if Path(row["path"]).exists():
            live.append(row)
            total += int(row["size_bytes"] or 0)
        else:
            db.execute("DELETE FROM local_files WHERE id = ?", (row["id"],))

    if total > config.FILE_CACHE_MAX_BYTES:
        freed = 0
        # Oldest last in that ordering, so walk from the end and evict until it fits.
        for row in reversed(live):
            if total <= config.FILE_CACHE_MAX_BYTES:
                break
            size = int(row["size_bytes"] or 0)
            try:
                Path(row["path"]).unlink(missing_ok=True)
            except OSError as exc:
                log.debug("could not evict %s: %s", row["path"], exc)
            db.execute("DELETE FROM local_files WHERE id = ?", (row["id"],))
            total -= size
            freed += size
        if freed:
            log.info(
                "evicted %d bytes of cached conversation files to stay under the ceiling", freed
            )
    else:
        freed = 0

    _sweep_orphans(db)
    return freed


def _sweep_orphans(db: Database) -> None:
    """Remove cache files that have no ledger row.

    A crash between ``write_bytes`` and ``remember_local_file`` leaves bytes on disk that nothing
    references. They are invisible to the size total — which is summed from rows — so eviction
    never reclaims them and they accumulate until the phone fills. Only files older than
    ``ORPHAN_GRACE_S`` are touched, so a copy a concurrent ``retain_file`` has just written but
    not yet recorded is never raced away.
    """
    try:
        cache = config.file_cache_root()
        entries = list(cache.iterdir())
    except OSError as exc:
        log.debug("orphan sweep could not read the cache directory: %s", exc)
        return

    known = {row["path"] for row in db.query("SELECT path FROM local_files")}
    cutoff = time.time() - ORPHAN_GRACE_S
    for entry in entries:
        if not entry.is_file() or str(entry) in known:
            continue
        try:
            if entry.stat().st_mtime > cutoff:
                continue
            entry.unlink()
            log.info("removed an orphaned cache file %s", entry.name)
        except OSError as exc:
            log.debug("could not sweep orphan %s: %s", entry, exc)


def load_local_files(db: Database, convo_id: int) -> list[sqlite3.Row]:
    return db.query(
        """SELECT file_id, path, size_bytes, owner_type, filename FROM local_files
           WHERE convo_id = ? ORDER BY id""",
        (convo_id,),
    )


def available_local_files(db: Database, convo_id: int) -> list[sqlite3.Row]:
    """Cached files for a conversation whose copy is actually still on disk.

    Rows whose file has vanished are dropped as a side effect, so a resume never promises an
    upload it cannot perform.
    """
    rows = load_local_files(db, convo_id)
    present = []
    for row in rows:
        if Path(row["path"]).exists():
            present.append(row)
        else:
            db.execute(
                "DELETE FROM local_files WHERE file_id = ? AND path = ?",
                (row["file_id"], row["path"]),
            )
    return present


# --- inbound queue handover ----------------------------------------------------------------


def take_queued_texts(db: Database, convo_id: int) -> list[str]:
    """Claim a dead conversation's undelivered messages, oldest first.

    Each row is marked :data:`config.RESUME_QUEUE_STATE` so nothing replays it against the
    session that is already gone, and the text of the ``text`` rows is returned so the
    replacement conversation can re-queue it.

    Re-queueing rather than rewriting ``convo_id`` in place is deliberate. ``_dispatch_burst``
    orders by row id, and migrated rows keep the low ids they were inserted with, so they
    would be posted AHEAD of the transcript that gives them context — the agent would see the
    user's question before the conversation it belongs to. Fresh inserts land after it.

    An ``interrupt`` row carries no text; it asked for a cancellation that can no longer mean
    anything to a session that does not exist, so it is simply retired.
    """
    rows = db.query(
        """SELECT id, kind, payload_json FROM inbound_queue
           WHERE convo_id = ? AND state = 'queued' ORDER BY id""",
        (convo_id,),
    )
    texts: list[str] = []
    for row in rows:
        if row["kind"] == "text":
            try:
                payload = json.loads(row["payload_json"])
            except json.JSONDecodeError:
                log.warning("dropping an unparseable queued message %s for convo %s",
                            row["id"], convo_id)
                payload = {}
            text = (payload.get("text") or "").strip()
            if text:
                texts.append(text)
        db.execute(
            "UPDATE inbound_queue SET state = ?, sent_at = ? WHERE id = ?",
            (config.RESUME_QUEUE_STATE, utcnow(), row["id"]),
        )
    return texts


def requeue_texts(db: Database, convo_id: int, texts: list[str]) -> None:
    """Put claimed texts back into a conversation's durable queue as fresh ``queued`` rows.

    The counterpart of :func:`take_queued_texts` for every path where a rebuild does NOT
    complete: the claimed texts existed only in a local variable, and without this a failed
    auto-resume (billing, transient API error, teardown, crash) silently dropped messages the
    user had already sent. Fresh inserts keep the original relative order, and the next resume
    claims them again through :func:`take_queued_texts`.
    """
    for text in texts:
        db.execute(
            """INSERT INTO inbound_queue(convo_id, kind, payload_json, state, created_at)
               VALUES(?, 'text', ?, 'queued', ?)""",
            (convo_id, json.dumps({"text": text}), utcnow()),
        )
