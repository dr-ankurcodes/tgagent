"""SQLite persistence.

Everything the bot needs to survive being killed by Android lives here. Process death is
treated as inevitable rather than exceptional, so durable state is committed eagerly: the
SSE cursor on every buffered event, the inbound queue before a turn is posted, and artifact
delivery as soon as the Telegram send returns.

Renderer state is deliberately NOT persisted. A live message id belongs to one process life,
so after a restart the renderer sends a fresh message rather than editing one it no longer
owns — the buffer, not the message, is the source of truth.

WAL mode is used because the SSE consumer, the renderer and the Telegram handlers all touch
the database concurrently from different asyncio tasks.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Sequence

from .config import restrict

SCHEMA_VERSION = 5


def utcnow() -> str:
    """ISO-8601 UTC timestamp, matching the format the Qoder API returns."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


DDL = """
CREATE TABLE IF NOT EXISTS users (
    tg_user_id      INTEGER PRIMARY KEY,
    handle          TEXT,
    memstore_id     TEXT,
    credit_budget   REAL    NOT NULL DEFAULT 1000,
    -- Vestigial: /think was removed because the API delivers no reasoning text. The column
    -- stays because dropping it would mean rebuilding the table, which is not worth it for
    -- one unused integer. Nothing reads or writes it.
    show_thinking   INTEGER NOT NULL DEFAULT 0,
    show_tools      INTEGER NOT NULL DEFAULT 1,
    model_id        TEXT,
    -- Reasoning effort, sent as model.effort alongside model_id. NULL means "let the model use
    -- its own catalog default_effort". Only ever applied when the target model advertises the
    -- level, so a preference set for one model cannot break a later switch to another.
    effort          TEXT,
    warned_at_pct   INTEGER NOT NULL DEFAULT 0,
    last_id_request INTEGER,
    created_at      TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS environments (
    env_id          TEXT PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    packages_json   TEXT,
    setup_script    TEXT,
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS agents (
    agent_id        TEXT PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    tg_user_id      INTEGER REFERENCES users(tg_user_id) ON DELETE CASCADE,
    version         INTEGER NOT NULL DEFAULT 1,
    model_json      TEXT,
    tools_json      TEXT,
    system          TEXT,
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS conversations (
    convo_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    tg_user_id      INTEGER NOT NULL REFERENCES users(tg_user_id) ON DELETE CASCADE,
    chat_id         INTEGER NOT NULL,
    message_thread_id INTEGER,
    title           TEXT,
    session_id      TEXT UNIQUE,
    agent_id        TEXT,
    env_id          TEXT,
    model_id        TEXT,
    run_status      TEXT    NOT NULL DEFAULT 'idle',
    last_event_id   TEXT,
    last_stop_reason TEXT,
    active          INTEGER NOT NULL DEFAULT 1,
    archived_at     TEXT,
    deleted_at      TEXT,
    -- Set when the cloud session became unreachable (a rotated PAT makes every session the bot
    -- knew about return 404). This is what separates a conversation that was LOST from one the
    -- user deliberately archived: only a lost one is silently resumed from the local transcript.
    lost_session    INTEGER NOT NULL DEFAULT 0,
    workspace_warned INTEGER NOT NULL DEFAULT 0,
    credits_spent   REAL    NOT NULL DEFAULT 0,
    created_at      TEXT    NOT NULL,
    updated_at      TEXT    NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_one_active_per_chat
    ON conversations(chat_id, tg_user_id, active) WHERE deleted_at IS NULL AND active = 1;
CREATE INDEX IF NOT EXISTS ix_convo_session ON conversations(session_id);
CREATE INDEX IF NOT EXISTS ix_convo_user    ON conversations(tg_user_id, deleted_at);

-- Local transcript of every conversation event worth keeping: user messages, agent replies,
-- and a summarised record of tool calls and their results. This is NOT a cloud backup and it
-- cannot replay a session — tool calls are recorded as text, never re-executed. Its purpose is
-- to let a conversation be resumed on a fresh session under a different PAT, after the cloud
-- session that held it became unreachable and returned 404.
CREATE TABLE IF NOT EXISTS conversation_history (
    convo_id        INTEGER NOT NULL REFERENCES conversations(convo_id) ON DELETE CASCADE,
    seq             INTEGER NOT NULL,
    direction       TEXT    NOT NULL CHECK(direction IN ('user', 'agent')),
    content_type    TEXT    NOT NULL CHECK(content_type IN ('text', 'tool_use', 'tool_result')),
    text            TEXT,               -- main content, for text events
    tool_name       TEXT,               -- for tool_use events
    tool_input      TEXT,               -- JSON blob of the tool input, clipped
    tool_output     TEXT,               -- tool_result content, clipped
    is_error        INTEGER NOT NULL DEFAULT 0,  -- tool_result only: did the call fail?
    created_at      TEXT    NOT NULL,
    PRIMARY KEY(convo_id, seq)
);

CREATE INDEX IF NOT EXISTS ix_hist_convo_seq ON conversation_history(convo_id, seq);

-- Paths to local copies of files that crossed the conversation: ones the user uploaded, and
-- artifacts the agent produced. No bytes live in this table, only a path into scratch space,
-- and that space can be reclaimed by the OS or wiped by a reboot, so every path is
-- re-verified before it is relied on during a resume.
CREATE TABLE IF NOT EXISTS local_files (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    convo_id        INTEGER NOT NULL REFERENCES conversations(convo_id) ON DELETE CASCADE,
    file_id         TEXT NOT NULL,      -- the Qoder file id this copy came from
    path            TEXT NOT NULL,      -- local filesystem path of the copy
    size_bytes      INTEGER NOT NULL,
    owner_type      TEXT NOT NULL CHECK(owner_type IN ('user', 'agent')),
    filename        TEXT,
    created_at      TEXT NOT NULL,
    UNIQUE(file_id, path)
);

CREATE INDEX IF NOT EXISTS ix_local_convo ON local_files(convo_id);

CREATE TABLE IF NOT EXISTS inbound_queue (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    convo_id        INTEGER NOT NULL REFERENCES conversations(convo_id) ON DELETE CASCADE,
    kind            TEXT    NOT NULL,
    payload_json    TEXT    NOT NULL,
    state           TEXT    NOT NULL DEFAULT 'queued',
    attempts        INTEGER NOT NULL DEFAULT 0,
    created_at      TEXT    NOT NULL,
    sent_at         TEXT
);

CREATE INDEX IF NOT EXISTS ix_inbound_pending ON inbound_queue(convo_id, state, id);

CREATE TABLE IF NOT EXISTS tg_files (
    file_id         TEXT PRIMARY KEY,
    owner_tg_user_id INTEGER NOT NULL REFERENCES users(tg_user_id) ON DELETE CASCADE,
    convo_id        INTEGER REFERENCES conversations(convo_id) ON DELETE SET NULL,
    filename        TEXT,
    mime_type       TEXT,
    size_bytes      INTEGER,
    mount_path      TEXT,
    created_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_tgf_owner ON tg_files(owner_tg_user_id);

CREATE TABLE IF NOT EXISTS artifacts (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    convo_id        INTEGER NOT NULL REFERENCES conversations(convo_id) ON DELETE CASCADE,
    file_id         TEXT    NOT NULL,
    event_id        TEXT,
    filename        TEXT,
    size_bytes      INTEGER,
    mime_type       TEXT,
    delivered       INTEGER NOT NULL DEFAULT 0,
    tg_message_id   INTEGER,
    skipped_reason  TEXT,
    discovered_at   TEXT    NOT NULL,
    UNIQUE(convo_id, file_id)
);

CREATE TABLE IF NOT EXISTS spend (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    convo_id        INTEGER REFERENCES conversations(convo_id) ON DELETE SET NULL,
    tg_user_id      INTEGER NOT NULL,
    credits         REAL    NOT NULL,
    model           TEXT,
    is_error        INTEGER NOT NULL DEFAULT 0,
    evt_id          TEXT,
    at              TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_spend_user_at ON spend(tg_user_id, at);

CREATE TABLE IF NOT EXISTS rendered_events (
    convo_id        INTEGER NOT NULL REFERENCES conversations(convo_id) ON DELETE CASCADE,
    event_id        TEXT    NOT NULL,
    seq             INTEGER PRIMARY KEY AUTOINCREMENT,
    UNIQUE(convo_id, event_id)
);

CREATE INDEX IF NOT EXISTS ix_rendered_convo ON rendered_events(convo_id, seq);

CREATE TABLE IF NOT EXISTS kv (
    k               TEXT PRIMARY KEY,
    v               TEXT NOT NULL
);
"""

# Indexes over columns that a migration adds rather than the original DDL. These MUST run
# after the ALTERs: against a database written by an older build, agents has no tg_user_id
# yet, and creating the index during executescript(DDL) fails with "no such column".
DDL_POST_MIGRATION = """
CREATE UNIQUE INDEX IF NOT EXISTS ux_one_agent_per_user ON agents(tg_user_id);

-- One ledger row per credit span. rendered_events normally stops a replayed
-- span.model_request_end from being counted twice, but that table is bounded at
-- RENDER_DEDUPE_KEEP ids, so a full history re-walk on an old conversation can present a
-- span this ledger has already seen. NULLs are excluded: a row recorded without an event id
-- has nothing to dedupe on and must still be allowed in.
CREATE UNIQUE INDEX IF NOT EXISTS ux_spend_evt ON spend(evt_id) WHERE evt_id IS NOT NULL;
"""


class Database:
    """Thin synchronous sqlite3 wrapper.

    Calls are cheap and the bot is single-process, so these are invoked directly from async
    code rather than offloaded to a thread. Every method commits unless inside a transaction.
    """

    def __init__(self, path: Path):
        self.path = path
        self._depth = 0
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self._restrict()
        self._migrate()

    def _restrict(self) -> None:
        """Owner-only permissions on the database and its WAL sidecars.

        This file is every transcript, credit ledger row and queued message the bot has ever
        held. ``sqlite3.connect`` creates it with the process umask — 022 on most systems — and
        the WAL pragma above adds two more files created the same way, so all three come out
        world-readable to any other local account.

        Runs after the pragmas so the sidecars exist, and on every start so a database an older
        build left at 0644 is tightened rather than kept.
        """
        for suffix in ("", "-wal", "-shm"):
            target = Path(f"{self.path}{suffix}")
            if target.exists():
                restrict(target, 0o600)

    def _migrate(self) -> None:
        # executescript() implicitly commits any open transaction, so DDL must run outside
        # one. That is safe here: every statement is CREATE ... IF NOT EXISTS, so the
        # migration is idempotent and needs no rollback path.
        self.conn.executescript(DDL)

        current = self.kv_get("schema_version")
        if current and int(current) > SCHEMA_VERSION:
            raise RuntimeError(
                f"database schema_version {current} is newer than this build ({SCHEMA_VERSION})"
            )

        # Alterations that DDL cannot express, keyed on the version that introduced them.
        # A fresh database is already at SCHEMA_VERSION after the DDL above, so these only
        # run against a database written by an older build.
        version = int(current) if current else 0
        if version < 2:
            self._add_column_if_missing("users", "last_id_request", "INTEGER")
            self._add_column_if_missing("agents", "tg_user_id", "INTEGER")
        if version < 3:
            self._add_column_if_missing(
                "conversations", "lost_session", "INTEGER NOT NULL DEFAULT 0"
            )
        if version < 5:
            self._add_column_if_missing("users", "effort", "TEXT")

        self.conn.executescript(DDL_POST_MIGRATION)
        self.kv_set("schema_version", str(SCHEMA_VERSION))

    def _add_column_if_missing(self, table: str, column: str, decl: str) -> None:
        """SQLite has no ADD COLUMN IF NOT EXISTS, so the duplicate error is the check."""
        try:
            self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")
        except sqlite3.OperationalError as exc:
            if "duplicate column name" not in str(exc):
                raise

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Reentrant transaction: only the outermost block issues BEGIN and COMMIT.

        Ownership-checked helpers in auth.py call one another, so nesting is easy to hit by
        accident. Without this guard the inner COMMIT raises "no transaction is active".

        ``_depth`` is per-connection, not per-task, so two concurrent tasks would mistake each
        other's transaction for a nested one and the outer COMMIT would publish both. That is
        safe today only because NO transaction block awaits: every statement here is
        synchronous and so cannot yield the loop mid-transaction. Keep it that way — if a
        transaction ever needs to await, this must become per-task state first.
        """
        if self._depth > 0:
            self._depth += 1
            try:
                yield
            finally:
                self._depth -= 1
            return

        self.conn.execute("BEGIN IMMEDIATE")
        self._depth = 1
        try:
            yield
        except BaseException:
            self._depth = 0
            self.conn.execute("ROLLBACK")
            raise
        self._depth = 0
        self.conn.execute("COMMIT")

    def execute(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Cursor:
        return self.conn.execute(sql, params)

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[sqlite3.Row]:
        return list(self.conn.execute(sql, params).fetchall())

    def query_one(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Row | None:
        return self.conn.execute(sql, params).fetchone()

    def kv_get(self, key: str) -> str | None:
        row = self.query_one("SELECT v FROM kv WHERE k = ?", (key,))
        return row["v"] if row else None

    def kv_set(self, key: str, value: str) -> None:
        self.execute(
            "INSERT INTO kv(k, v) VALUES(?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v",
            (key, value),
        )

    def kv_delete(self, key: str) -> None:
        """Drop a key. For one-shot values that must not be read twice."""
        self.execute("DELETE FROM kv WHERE k = ?", (key,))

    def close(self) -> None:
        self.conn.close()
