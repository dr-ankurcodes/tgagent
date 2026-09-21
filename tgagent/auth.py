"""Authorization and ownership.

This module is the ONLY place that joins a Telegram user id to Qoder resources. Every
function that returns conversation, session or file data takes ``tg_user_id`` and filters on
it, so there is no code path by which one user can reach another user's data.

That matters because the isolation boundary is thin. Both users share one Qoder PAT, and
within a PAT everything is account-global: ``GET /files`` lists every file in the account,
and environments are shared templates. The hard boundaries we do get are separate sessions
(isolated sandboxes) and separate memory stores. Everything else is enforced here.

Handlers must never query the conversations table directly.
"""

from __future__ import annotations

import sqlite3
from typing import Sequence

from .config import Settings
from .db import Database, utcnow

# kv key under which the runtime allowlist is persisted. Once the administrator changes access
# from Telegram, THIS — not the TG_ALLOWED_IDS environment seed — is authoritative across boots,
# so a user removed with /disallow stays removed rather than being re-added from env on restart.
KV_ALLOWED_IDS = "allowed_ids"


class AccessControl:
    """Runtime authority on who may use the bot, and who administers it.

    Two halves with deliberately different lifetimes:

    * ``admin_id`` is fixed from the environment (TG_ADMIN_ID) and is NEVER writable at runtime.
      It is the root of trust — whoever holds it can change the allowlist and rotate the API
      token — so letting it be changed from a chat would mean a compromised session could hand
      an attacker the keys. Telegram's Bot API offers no way to discover who owns a bot, so the
      operator names their own id at deploy time.
    * ``allowed_ids`` is mutable from Telegram via /allow and /disallow. It is seeded from
      TG_ALLOWED_IDS on first run, then persisted to the ``kv`` table; once persisted, the stored
      set wins over env so a removal survives a restart.

    The administrator is always allowed, listed or not, so a deployment that sets only TG_ADMIN_ID
    is immediately usable by its owner, who can then populate the allowlist from the chat.

    Mutating methods contain no ``await``, so under asyncio's single thread each runs to
    completion without interleaving — no lock is needed for the read-modify-persist sequence.
    """

    def __init__(self, db: Database, *, admin_id: int | None, seed_allowed):
        self.db = db
        self.admin_id = admin_id
        stored = db.kv_get(KV_ALLOWED_IDS)
        if stored is not None:
            self.allowed_ids: set[int] = _parse_stored_ids(stored)
        else:
            self.allowed_ids = set(seed_allowed)

    @property
    def discovery_mode(self) -> bool:
        """An empty allowlist: admit nobody but the admin, and tell knockers their own id."""
        return not self.allowed_ids

    def is_admin(self, tg_user_id: int | None) -> bool:
        return tg_user_id is not None and self.admin_id is not None and tg_user_id == self.admin_id

    def is_allowed(self, tg_user_id: int | None) -> bool:
        """Gate every inbound update. The admin always passes; an empty allowlist admits nobody else."""
        if tg_user_id is None:
            return False
        if self.is_admin(tg_user_id):
            return True
        return tg_user_id in self.allowed_ids

    def add(self, tg_user_id: int) -> None:
        self.allowed_ids.add(tg_user_id)
        self._persist()

    def remove(self, tg_user_id: int) -> None:
        self.allowed_ids.discard(tg_user_id)
        self._persist()

    def snapshot(self) -> list[int]:
        """The allowlist, sorted, excluding the admin (who is allowed implicitly)."""
        return sorted(self.allowed_ids)

    def _persist(self) -> None:
        self.db.kv_set(KV_ALLOWED_IDS, ",".join(str(i) for i in sorted(self.allowed_ids)))


def _parse_stored_ids(raw: str) -> set[int]:
    """Parse a value this module wrote with ``_persist``. Tolerates blanks; never raises."""
    ids: set[int] = set()
    for part in raw.replace(";", ",").split(","):
        part = part.strip()
        if part:
            try:
                ids.add(int(part))
            except ValueError:
                continue
    return ids


def ensure_user(db: Database, settings: Settings, tg_user_id: int, handle: str | None) -> sqlite3.Row:
    """Create the user row on first contact and return it.

    Uses an upsert rather than check-then-insert. With ``concurrent_updates(10)`` a message and
    a command from the same first-contact user can arrive together; two plain INSERTs would race
    and the loser would raise an unhandled ``IntegrityError`` on the primary key, crashing that
    user's very first interaction. ``ON CONFLICT DO NOTHING`` makes the insert idempotent, so
    the row is guaranteed to exist for the re-read regardless of who won.
    """
    db.execute(
        """INSERT INTO users(tg_user_id, handle, credit_budget, model_id, effort, created_at)
           VALUES(?, ?, ?, ?, ?, ?)
           ON CONFLICT(tg_user_id) DO NOTHING""",
        (
            tg_user_id, handle, settings.credit_budget, settings.default_model,
            settings.default_effort, utcnow(),
        ),
    )
    if handle:
        # Refresh a changed handle. The NULL-safe comparison means a row inserted moments ago by
        # the racing caller — possibly with a stale or missing handle — still converges on one
        # row carrying the latest value, without a second read-modify-write cycle.
        db.execute(
            "UPDATE users SET handle = ? WHERE tg_user_id = ? AND (handle IS NULL OR handle != ?)",
            (handle, tg_user_id, handle),
        )
    return db.query_one("SELECT * FROM users WHERE tg_user_id = ?", (tg_user_id,))


def get_user(db: Database, tg_user_id: int) -> sqlite3.Row | None:
    return db.query_one("SELECT * FROM users WHERE tg_user_id = ?", (tg_user_id,))


def create_conversation(
    db: Database,
    *,
    tg_user_id: int,
    chat_id: int,
    message_thread_id: int | None = None,
    title: str | None = None,
    session_id: str | None = None,
    agent_id: str | None = None,
    env_id: str | None = None,
    model_id: str | None = None,
    activate: bool = True,
) -> int:
    """Insert a conversation and return its local convo_id.

    convo_id is a small local integer, which is what makes it safe to embed in Telegram
    callback_data: it fits the 64-byte cap and, unlike a Qoder session id, it cannot be
    guessed or replayed by another user because every lookup re-checks ownership.
    """
    with db.transaction():
        if activate:
            db.execute(
                """UPDATE conversations SET active = 0, updated_at = ?
                   WHERE chat_id = ? AND tg_user_id = ? AND deleted_at IS NULL AND active = 1""",
                (utcnow(), chat_id, tg_user_id),
            )
        cur = db.execute(
            """INSERT INTO conversations(
                   tg_user_id, chat_id, message_thread_id, title, session_id, agent_id,
                   env_id, model_id, active, created_at, updated_at)
               VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                tg_user_id,
                chat_id,
                message_thread_id,
                title,
                session_id,
                agent_id,
                env_id,
                model_id,
                1 if activate else 0,
                utcnow(),
                utcnow(),
            ),
        )
        convo_id = int(cur.lastrowid)
    return convo_id


def active_conversation(
    db: Database, tg_user_id: int, chat_id: int, message_thread_id: int | None = None
) -> sqlite3.Row | None:
    """The conversation new messages should go to, or None if the user has none.

    Excludes archived rows as well as inactive ones. Filtering on ``active`` alone is not
    enough: an archived conversation that still held the flag would keep receiving messages,
    which is what ``/archive`` did before it cleared it.
    """
    if message_thread_id is None:
        return db.query_one(
            """SELECT * FROM conversations
               WHERE tg_user_id = ? AND chat_id = ? AND message_thread_id IS NULL
                 AND active = 1 AND deleted_at IS NULL AND archived_at IS NULL
               ORDER BY convo_id DESC LIMIT 1""",
            (tg_user_id, chat_id),
        )
    return db.query_one(
        """SELECT * FROM conversations
           WHERE tg_user_id = ? AND chat_id = ? AND message_thread_id = ?
             AND active = 1 AND deleted_at IS NULL AND archived_at IS NULL
           ORDER BY convo_id DESC LIMIT 1""",
        (tg_user_id, chat_id, message_thread_id),
    )


def get_conversation(db: Database, convo_id: int, tg_user_id: int) -> sqlite3.Row | None:
    """Ownership-checked fetch. This is what callback_data handlers must use."""
    return db.query_one(
        "SELECT * FROM conversations WHERE convo_id = ? AND tg_user_id = ? AND deleted_at IS NULL",
        (convo_id, tg_user_id),
    )


def list_conversations(db: Database, tg_user_id: int, *, include_archived: bool = False) -> list[sqlite3.Row]:
    sql = """SELECT * FROM conversations
             WHERE tg_user_id = ? AND deleted_at IS NULL"""
    if not include_archived:
        sql += " AND archived_at IS NULL"
    sql += " ORDER BY active DESC, updated_at DESC LIMIT 30"
    return db.query(sql, (tg_user_id,))


def switch_active_conversation(db: Database, convo_id: int, tg_user_id: int) -> bool:
    """Make convo_id the active one for its chat. Returns False if not owned or archived.

    An archived row is rejected explicitly. :func:`get_conversation` filters on ``deleted_at``
    but not ``archived_at``, so without this check an archived conversation could be set
    ``active = 1`` alongside a non-NULL ``archived_at`` — a state :func:`active_conversation`
    never returns. The user would believe they switched, while their next message silently
    opened a *new* conversation, and the row would occupy the one-active-per-chat slot doing it.
    """
    row = get_conversation(db, convo_id, tg_user_id)
    if row is None or row["archived_at"] is not None:
        return False
    with db.transaction():
        db.execute(
            """UPDATE conversations SET active = 0, updated_at = ?
               WHERE chat_id = ? AND tg_user_id = ? AND deleted_at IS NULL AND active = 1""",
            (utcnow(), row["chat_id"], tg_user_id),
        )
        db.execute(
            "UPDATE conversations SET active = 1, updated_at = ? WHERE convo_id = ? AND tg_user_id = ?",
            (utcnow(), convo_id, tg_user_id),
        )
    return True


def migrate_chat_id(db: Database, old_chat_id: int, new_chat_id: int) -> int:
    """Repoint every conversation from a superseded chat id to its replacement.

    Telegram assigns a new chat id when it upgrades a basic group to a supergroup. Nothing else
    in the bot can notice that on its own: conversations are keyed on ``chat_id``, so every row
    for the old id simply stops matching — yet :func:`list_conversations` filters on the user,
    not the chat, so /sessions keeps listing them and switching to one posts into a chat that no
    longer exists.

    Idempotent, and returns how many rows moved. That matters because Telegram posts the service
    message into BOTH the old chat and the new one, so this can legitimately run twice for one
    upgrade.

    Archived and soft-deleted rows move too. :func:`resumable_conversation` filters on
    ``chat_id``, so leaving a lost conversation behind under the old id would make it
    unrestorable from the chat that replaced it — which is the whole point of keeping it.
    """
    with db.transaction():
        # The partial unique index is (chat_id, tg_user_id, active) over undeleted rows. If a
        # message reached the new chat before this handler ran, it already created an active
        # row there, and moving the old one in beside it would violate the index from inside a
        # service-message handler. Demote the newcomer instead: the row being migrated carries
        # the transcript and the session, and a row created seconds ago carries neither. It
        # stays in /sessions, so demoting it loses nothing.
        db.execute(
            """UPDATE conversations SET active = 0
               WHERE chat_id = ? AND active = 1 AND deleted_at IS NULL
                 AND tg_user_id IN (
                     SELECT tg_user_id FROM conversations
                     WHERE chat_id = ? AND active = 1 AND deleted_at IS NULL
                 )""",
            (new_chat_id, old_chat_id),
        )
        # updated_at is deliberately left alone. A chat id change is not conversation activity,
        # and bumping it would suppress the "your sandbox may have been reclaimed" warning,
        # which measures idle time from updated_at — the files really would still be gone.
        cur = db.execute(
            "UPDATE conversations SET chat_id = ? WHERE chat_id = ?",
            (new_chat_id, old_chat_id),
        )
        return cur.rowcount


def update_conversation(db: Database, convo_id: int, tg_user_id: int, **fields) -> bool:
    """Ownership-checked partial update.

    Returns True if a matching owned row was updated, False if no row matched. An empty
    ``fields`` is a programming error rather than a silent no-op: raising here keeps the False
    return unambiguous ("no matching owned row") for the callers that branch on it, instead of
    overloading it to also mean "you asked me to write nothing".
    """
    if not fields:
        raise ValueError("update_conversation requires at least one field to write")
    allowed = {
        "title",
        "session_id",
        "agent_id",
        "env_id",
        "model_id",
        "run_status",
        "last_event_id",
        "last_stop_reason",
        "active",
        "archived_at",
        "deleted_at",
        "lost_session",
        "workspace_warned",
        "credits_spent",
    }
    unknown = set(fields) - allowed
    if unknown:
        raise ValueError(f"refusing to write unrecognised conversation fields: {sorted(unknown)}")

    assignments = ", ".join(f"{name} = ?" for name in fields)
    params = [*fields.values(), utcnow(), convo_id, tg_user_id]
    cur = db.execute(
        f"UPDATE conversations SET {assignments}, updated_at = ? WHERE convo_id = ? AND tg_user_id = ?",
        params,
    )
    return cur.rowcount > 0


def conversations_needing_reconciliation(
    db: Database, live_statuses: Sequence[str] = ("running", "rescheduling", "canceling")
) -> list[sqlite3.Row]:
    """Conversations worth fetching at boot, for the startup reconciliation pass.

    Only two kinds qualify:

    * the one the user is currently addressing (``active = 1``), so their next message has a
      live conversation waiting;
    * one that was mid-turn when the process died, so an answer in flight is not abandoned.

    Everything else is deliberately left alone and materialised lazily when the user switches
    to it — see the "no separate reaper" note in convo.py. Fetching every conversation ever
    created instead made boot cost grow without bound, and archiving the idle ones would be
    worse still: only the sandbox FILESYSTEM is reclaimed after 24 hours, so a week-old session
    still holds its whole conversation history server-side.

    Deliberately not user-scoped: this runs at boot before any user is involved, and its
    results are only ever used to talk to Qoder, never to send a message to anyone.
    """
    placeholders = ",".join("?" * len(live_statuses))
    return db.query(
        f"""SELECT * FROM conversations
            WHERE deleted_at IS NULL AND archived_at IS NULL AND session_id IS NOT NULL
              AND (active = 1 OR run_status IN ({placeholders}))
            ORDER BY updated_at DESC""",
        tuple(live_statuses),
    )


def mark_conversation_lost(db: Database, convo_id: int, tg_user_id: int) -> bool:
    """Record that a conversation became unreachable, and retire it.

    Sets ``deleted_at`` so every ordinary read stops seeing it, clears ``active`` so the
    one-active-per-chat index stays satisfied, and raises ``lost_session`` — the flag that
    makes this resumable from the local transcript.

    What separates a lost conversation from a closed one is ``archived_at``, not this flag:
    ``/archive`` sets that, and ``resumable_conversation`` excludes it, because the user closed
    that conversation deliberately and quietly reopening it would undo an explicit choice. A
    lost one was taken away by something outside their control — a rotated PAT makes every
    session the old token created return 404, and losing the right to post in a chat does the
    same to the conversation around it — so offering it back is the friendly thing to do.
    """
    return update_conversation(
        db, convo_id, tg_user_id, deleted_at=utcnow(), active=0, lost_session=1
    )


def resumable_conversation(
    db: Database,
    tg_user_id: int,
    chat_id: int,
    message_thread_id: int | None = None,
    *,
    convo_id: int | None = None,
) -> sqlite3.Row | None:
    """A conversation whose session was lost, if any.

    Without ``convo_id`` given, only conversations in this chat are returned, because that is
    the path a SILENT resume follows — the user sent a fresh message and nothing should change
    unless there IS something to resume in this chat. With ``convo_id``, that specific one is
    returned regardless of which chat the request came from. /sessions lists conversations from
    every chat (the bot belongs to many), so pressing RESTORE from a DM for a group conversation
    must not fail just because the originating chat doesn't match the stored one; the convo_id
    owns the recovery, not the button's chat. Ownership, the lost flag and archived_at ARE still
    checked. An archived one is excluded: the user closed it deliberately, and quietly reopening
    it would undo an explicit choice. A lost one was taken away by something outside their control
    — a rotated PAT makes every session the old token created return 404, and losing the right to
    post in a chat does the same to the conversation around it — so offering it back is the
    friendly thing to do.
    """
    if convo_id is not None:
        # Targeted restore from /sessions: the convo_id is what matters, not where the button
        # appeared. Ownership, lost_session and archived_at IS NULL are all still checked.
        return db.query_one(
            """SELECT * FROM conversations
               WHERE convo_id = ? AND tg_user_id = ? AND lost_session = 1 AND archived_at IS NULL""",
            (convo_id, tg_user_id),
        )
    # Un-targeted path (silent resume, /health): restrict to this chat/thread.
    if message_thread_id is None:
        return db.query_one(
            """SELECT * FROM conversations
               WHERE tg_user_id = ? AND chat_id = ? AND message_thread_id IS NULL
                 AND lost_session = 1 AND archived_at IS NULL
               ORDER BY convo_id DESC LIMIT 1""",
            (tg_user_id, chat_id),
        )
    return db.query_one(
        """SELECT * FROM conversations
           WHERE tg_user_id = ? AND chat_id = ? AND message_thread_id = ?
             AND lost_session = 1 AND archived_at IS NULL
           ORDER BY convo_id DESC LIMIT 1""",
        (tg_user_id, chat_id, message_thread_id),
    )


def list_recoverable_conversations(db: Database, tg_user_id: int, *, limit: int = 30) -> list[sqlite3.Row]:
    """Conversations whose cloud session became unreachable but whose transcript survives.

    :func:`list_conversations` excludes these, because it filters on ``deleted_at IS NULL`` and
    retiring a lost conversation sets that. Listing them separately is what lets /sessions offer
    recovery instead of silently dropping the conversation from view.

    Uses the local ``lost_session`` flag only — no API calls, so it costs nothing to render.
    """
    return db.query(
        """SELECT * FROM conversations
           WHERE tg_user_id = ? AND lost_session = 1 AND archived_at IS NULL
           ORDER BY convo_id DESC LIMIT ?""",
        (tg_user_id, limit),
    )


def record_tg_file(
    db: Database,
    *,
    file_id: str,
    owner_tg_user_id: int,
    convo_id: int | None,
    filename: str | None,
    mime_type: str | None,
    size_bytes: int | None,
    mount_path: str | None,
) -> None:
    db.execute(
        """INSERT INTO tg_files(file_id, owner_tg_user_id, convo_id, filename, mime_type,
                                size_bytes, mount_path, created_at)
           VALUES(?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(file_id, owner_tg_user_id) DO UPDATE SET
             mount_path = excluded.mount_path, convo_id = excluded.convo_id""",
        (file_id, owner_tg_user_id, convo_id, filename, mime_type, size_bytes, mount_path, utcnow()),
    )
