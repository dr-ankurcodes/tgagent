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

import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, Sequence

from .config import restrict

log = logging.getLogger("tgagent.db")

SCHEMA_VERSION = 7


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

-- Telegram dedupes uploads per bot, so the SAME file_id can legitimately arrive from two
-- different users. Keying on file_id alone made the second upload's ON CONFLICT overwrite the
-- first user's owner/convo/mount_path, which then hid the file from its real owner and let a
-- cascade delete on user A wipe user B's live record. The primary key is therefore the pair:
-- one row per (file, owner), each carrying its own mount_path and convo_id.
CREATE TABLE IF NOT EXISTS tg_files (
    file_id         TEXT NOT NULL,
    owner_tg_user_id INTEGER NOT NULL REFERENCES users(tg_user_id) ON DELETE CASCADE,
    convo_id        INTEGER REFERENCES conversations(convo_id) ON DELETE SET NULL,
    filename        TEXT,
    mime_type       TEXT,
    size_bytes      INTEGER,
    mount_path      TEXT,
    created_at      TEXT NOT NULL,
    PRIMARY KEY(file_id, owner_tg_user_id)
);

CREATE INDEX IF NOT EXISTS ix_tgf_owner ON tg_files(owner_tg_user_id);
CREATE INDEX IF NOT EXISTS ix_tgf_convo  ON tg_files(convo_id);

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
    -- Cross-boot retry counter. A transient Telegram failure deliberately leaves the row
    -- pending so the next boot tries again, but without a cap a file Telegram repeatedly
    -- refuses was re-downloaded (up to 50 MB into phone RAM) on every start forever.
    attempts        INTEGER NOT NULL DEFAULT 0,
    discovered_at   TEXT    NOT NULL,
    UNIQUE(convo_id, file_id)
);

CREATE TABLE IF NOT EXISTS spend (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    convo_id        INTEGER REFERENCES conversations(convo_id) ON DELETE SET NULL,
    -- FK added for consistency with every other user-owned table. No code path deletes a user
    -- row (forget_chat operates on conversations), so ON DELETE CASCADE is belt-and-braces;
    -- databases created before this line lack the constraint and cannot gain it without a
    -- table rebuild, which is not worth the risk for a deletion that never happens.
    tg_user_id      INTEGER NOT NULL REFERENCES users(tg_user_id) ON DELETE CASCADE,
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
        self._nested_failed = False
        path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(path), isolation_level=None, check_same_thread=False)
        try:
            self.conn.row_factory = sqlite3.Row
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA foreign_keys=ON")
            # FULL, not NORMAL. NORMAL+WAL survives a process crash — which is the common case
            # on Android — but loses the last committed transactions on an OS crash or battery
            # pull, and the module docstring promises durable state is committed eagerly because
            # process death is inevitable. A phone losing power mid-checkpoint is realistic
            # enough that the fsync cost is the price of keeping that promise.
            self.conn.execute("PRAGMA synchronous=FULL")
            self.conn.execute("PRAGMA busy_timeout=5000")
            self._restrict()
            self._migrate()
        except BaseException:
            # _migrate raises RuntimeError on a newer schema; without this the connection (and
            # its WAL sidecars) leak for the life of the process.
            self.conn.close()
            raise

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
        # No `version < 4` branch: schema v4 was DDL-only (it added rendered_events and kv via
        # CREATE TABLE IF NOT EXISTS above, with no ALTER). Recorded here so the jump from 3 to 5
        # is not mistaken for a skipped migration — a column added in a future v4-style step must
        # get its own branch or it will silently never migrate on an existing database.
        if version < 5:
            self._add_column_if_missing("users", "effort", "TEXT")
        if version < 6:
            self._rebuild_tg_files()
        if version < 7:
            self._add_column_if_missing("artifacts", "attempts", "INTEGER NOT NULL DEFAULT 0")

        self.conn.executescript(DDL_POST_MIGRATION)
        self.kv_set("schema_version", str(SCHEMA_VERSION))

    def _rebuild_tg_files(self) -> None:
        """Migrate tg_files from a file_id primary key to (file_id, owner_tg_user_id).

        SQLite cannot alter a primary key in place, so the table is rebuilt: create the new
        shape, copy every row, drop the old table, rename. ``PRAGMA foreign_keys`` is a no-op
        inside a transaction, so it is toggled around an explicit BEGIN/COMMIT rather than
        through :meth:`transaction` — and the copy is atomic, so a crash mid-rebuild leaves the
        old table intact instead of a half-populated new one.

        No other table references tg_files, so the drop cascades nowhere. The old file_id PK
        guaranteed uniqueness on file_id alone, which the composite key relaxes by design; every
        existing row already has a distinct (file_id, owner) pair, so the copy cannot conflict.
        """
        self.conn.execute("PRAGMA foreign_keys=OFF")
        try:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                self.conn.execute(
                    """CREATE TABLE tg_files_new (
                           file_id         TEXT NOT NULL,
                           owner_tg_user_id INTEGER NOT NULL
                               REFERENCES users(tg_user_id) ON DELETE CASCADE,
                           convo_id        INTEGER REFERENCES conversations(convo_id)
                               ON DELETE SET NULL,
                           filename        TEXT,
                           mime_type       TEXT,
                           size_bytes      INTEGER,
                           mount_path      TEXT,
                           created_at      TEXT NOT NULL,
                           PRIMARY KEY(file_id, owner_tg_user_id)
                       )"""
                )
                self.conn.execute(
                    """INSERT INTO tg_files_new(file_id, owner_tg_user_id, convo_id, filename,
                                                mime_type, size_bytes, mount_path, created_at)
                       SELECT file_id, owner_tg_user_id, convo_id, filename,
                              mime_type, size_bytes, mount_path, created_at
                       FROM tg_files"""
                )
                self.conn.execute("DROP TABLE tg_files")
                self.conn.execute("ALTER TABLE tg_files_new RENAME TO tg_files")
                self.conn.execute(
                    "CREATE INDEX IF NOT EXISTS ix_tgf_owner ON tg_files(owner_tg_user_id)"
                )
                self.conn.execute(
                    "CREATE INDEX IF NOT EXISTS ix_tgf_convo ON tg_files(convo_id)"
                )
                self.conn.execute("COMMIT")
            except BaseException:
                self.conn.execute("ROLLBACK")
                raise
        finally:
            self.conn.execute("PRAGMA foreign_keys=ON")

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

        Two failure modes are handled explicitly:

        * A nested block that raises and is CAUGHT by the outer block. Without tracking, the
          outer would COMMIT the partial work the inner had already abandoned. ``_nested_failed``
          propagates the failure up so the outermost block rolls back instead.
        * A COMMIT that fails (SQLITE_BUSY past the timeout, disk I/O error). ``_depth`` would
          already be 0 but the transaction could remain open on the connection, so every
          subsequent "autocommit" statement would silently join the zombie and a later stray
          COMMIT would publish them. A best-effort ROLLBACK after the failure leaves the
          connection clean; if even that fails the connection is unusable and is closed.
        """
        if self._depth > 0:
            self._depth += 1
            try:
                yield
            except BaseException:
                self._nested_failed = True
                raise
            finally:
                self._depth -= 1
            return

        self.conn.execute("BEGIN IMMEDIATE")
        self._depth = 1
        self._nested_failed = False
        try:
            yield
        except BaseException:
            self._depth = 0
            self._nested_failed = False
            self.conn.execute("ROLLBACK")
            raise

        self._depth = 0
        if self._nested_failed:
            self._nested_failed = False
            self.conn.execute("ROLLBACK")
            raise RuntimeError("transaction rolled back: a nested block failed")

        self._nested_failed = False
        try:
            self.conn.execute("COMMIT")
        except sqlite3.Error:
            try:
                self.conn.execute("ROLLBACK")
            except sqlite3.Error:
                log.exception("could not roll back after a failed COMMIT; closing connection")
                try:
                    self.conn.close()
                except sqlite3.Error:
                    pass
                raise
            raise

    def execute(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Cursor:
        return self.conn.execute(sql, params)

    def query(self, sql: str, params: Sequence[Any] = ()) -> list[sqlite3.Row]:
        cur = self.conn.execute(sql, params)
        try:
            return list(cur.fetchall())
        finally:
            cur.close()

    def query_one(self, sql: str, params: Sequence[Any] = ()) -> sqlite3.Row | None:
        cur = self.conn.execute(sql, params)
        try:
            return cur.fetchone()
        finally:
            cur.close()

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
