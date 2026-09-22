"""Conversation registry, provisioning, and crash reconciliation.

The manager decides which conversations get live tasks. That matters on a phone: an idle
conversation in the background does not need an open SSE connection, so we only keep live
tasks for conversations that are actively running a turn or that the user is currently
addressing. Everything else is materialised on demand.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from pathlib import Path

from telegram import Bot

from . import auth, config, history, tg_html, uploads
from .convo import Conversation
from .db import Database, utcnow
from .qclient import BillingError, QoderError, Unauthorized
from .qmemory import STORE_INSTRUCTIONS, ensure_memory_store
from .qsessions import QoderAPI, conversation_title, memory_resource, provision
from .renderer import BudgetRegistry
from .tgsink import TelegramSink

log = logging.getLogger("tgagent.manager")

LIVE_STATUSES = ("running", "rescheduling", "canceling")


class Manager:
    def __init__(self, *, db: Database, api: QoderAPI, settings, bot: Bot):
        self.db = db
        self.api = api
        self.settings = settings
        self.bot = bot
        self.budgets = BudgetRegistry()
        self.stream_slots = asyncio.Semaphore(settings.max_live_streams)
        self.env_id: str | None = None
        self._convos: dict[int, Conversation] = {}
        # Store ids whose existence has already been confirmed in this process life, so
        # creating a second conversation does not pay for the same validating GET again.
        self._verified_stores: set[str] = set()
        # Silent rebuilds already attempted per chat. A rebuilt session that vanishes again
        # means retrying cannot help, so this is what stops the loop.
        self._resume_attempts: dict[tuple[int, int | None], int] = {}
        # One lock per chat, so concurrent updates cannot both provision a session for it.
        self._provision_locks: dict[tuple[int, int | None], asyncio.Lock] = {}
        # One lock per conversation id, so retire() and _adopt() for the same conversation cannot
        # interleave. retire() pops the conversation and then awaits a shutdown that takes seconds
        # (cancelling tasks, waiting for hooks, force-flushing); without this a concurrent
        # active_for/switch_to saw the empty slot mid-retirement and adopted a SECOND live
        # conversation for the same row — two renderers writing one chat at once.
        self._lifecycle_locks: dict[int, asyncio.Lock] = {}
        # Set during shutdown so a detached retirement task cannot mint a replacement session
        # (auto_resume) while the process is tearing down and the HTTP client is about to close.
        self._shutting_down = False

    def _lock_for(self, chat_id: int, message_thread_id: int | None) -> asyncio.Lock:
        """The provisioning lock for one chat (and topic, in a forum supergroup).

        PTB runs handlers concurrently, so two messages arriving together for a user with no
        live conversation both reached ``create`` and both minted a paid session; the second
        insert then failed the ``ux_one_active_per_chat`` index and surfaced as an unhandled
        error. One lock per chat makes provisioning a critical section.
        """
        key = (chat_id, message_thread_id)
        lock = self._provision_locks.get(key)
        if lock is None:
            # Bound the registry: keys are chosen by whoever messages the bot, so without this it
            # grew with every chat ever seen. Only UNLOCKED entries are dropped — discarding a held
            # lock would let a second caller into the critical section it exists to own.
            if len(self._provision_locks) > 256:
                for stale in [k for k, v in self._provision_locks.items() if not v.locked()]:
                    self._provision_locks.pop(stale, None)
            lock = asyncio.Lock()
            self._provision_locks[key] = lock
        return lock

    def _lifecycle_lock(self, convo_id: int) -> asyncio.Lock:
        """The retire/adopt lock for one conversation, so the two cannot interleave."""
        lock = self._lifecycle_locks.get(convo_id)
        if lock is None:
            lock = asyncio.Lock()
            self._lifecycle_locks[convo_id] = lock
        return lock

    async def _notify_chat(self, convo: Conversation, text: str, *, is_error: bool = True) -> None:
        """Send to a chat whose conversation has already been retired.

        The retired conversation's renderer has been force-flushed and stopped, so a notice
        queued on it would never be drained. Sending directly is safe here precisely because
        that renderer is gone: the single-writer invariant is about two live tasks editing one
        message, and there is no live writer left for this chat's old conversation.
        """
        try:
            await self.bot.send_message(
                chat_id=convo.chat_id,
                text=("⚠ " if is_error else "") + text,
                message_thread_id=convo.thread_id,
            )
        except Exception:  # noqa: BLE001 - a failed notice must never break retirement
            log.warning("could not notify chat %s", convo.chat_id)

    async def startup(self) -> None:
        """Provision the shared environment, then reconcile surviving state."""
        try:
            self.env_id = await provision(self.api)
            log.info("provisioned env=%s", self.env_id)
        except QoderError as exc:
            # Without this nothing works, but the bot should still start so /health can
            # explain what is wrong rather than the process silently exiting.
            log.error("provisioning failed: %s", exc)
            raise

        # Renaming ENV_NAME in config orphans the old resource in the account and leaves a
        # local row that looks live. Forget those rows so /health reports the truth.
        self.api.prune_stale_environments()
        self.api.prune_stale_agents()

        models = await self.api.list_models()
        log.info("model catalog: %d models available", len(models))

        await self.reconcile()

    async def agent_for(self, tg_user_id: int, model_id: str) -> str:
        """This user's agent, created on first use. Agents are per user so that /model can
        change one person's model without changing everyone else's.

        Requires the users row to exist first: agents.tg_user_id is a foreign key, so an agent
        cannot be recorded for a user the bot has never seen. ``create()`` satisfies this by
        calling ``auth.ensure_user`` above; any other caller must do the same.
        """
        return await self.api.ensure_user_agent(tg_user_id, model_id)

    async def reconcile(self) -> None:
        """Re-adopt conversations after a restart.

        Android kills this process without warning, so this runs on every boot. It must never
        assume the in-memory state from a previous life exists.

        Only conversations worth fetching are fetched — the active one, and any that were
        mid-turn when the process died. The rest are materialised lazily when the user switches
        to them, so boot cost does not grow with the number of conversations ever created.
        """
        rows = auth.conversations_needing_reconciliation(self.db, LIVE_STATUSES)
        if not rows:
            return

        # Bounded concurrency: a sequential GET per conversation meant boot time grew with
        # every conversation, which on a cold boot over mobile data is the difference between
        # a bot that answers in two seconds and one that takes a minute.
        slot = asyncio.Semaphore(config.RECONCILE_CONCURRENCY)
        results = await asyncio.gather(
            *(self._reconcile_one(row, slot) for row in rows), return_exceptions=True
        )

        adopted = 0
        for row, result in zip(rows, results):
            if isinstance(result, BaseException):
                log.warning("reconciliation of conversation %s failed: %s",
                            row["convo_id"], result)
                continue
            if result:
                adopted += 1

        log.info("reconciled %d conversations, %d live", len(rows), adopted)

        # Anything queued while the process was dead gets another chance.
        stuck = self.db.query_one(
            "SELECT COUNT(*) AS n FROM inbound_queue WHERE state = 'queued'"
        )
        if stuck and stuck["n"]:
            log.info("%d queued messages awaiting dispatch", stuck["n"])

    async def _reconcile_one(self, row: sqlite3.Row, slot: asyncio.Semaphore) -> bool:
        """Reconcile a single conversation. Returns whether it was adopted as live."""
        async with slot:
            session_id = row["session_id"]
            try:
                session = await self.api.get_session(session_id)
            except QoderError as exc:
                log.warning("could not fetch %s during reconciliation: %s", session_id, exc)
                return False

            if session is None:
                log.info("session %s is gone; retiring conversation %s as resumable",
                         session_id, row["convo_id"])
                # Lost, not deleted: the local transcript can rebuild this on a fresh session.
                auth.mark_conversation_lost(self.db, row["convo_id"], row["tg_user_id"])
                return False

            status = session.get("status") or "idle"
            if session.get("archived_at") or status == "terminated":
                auth.update_conversation(
                    self.db, row["convo_id"], row["tg_user_id"], run_status="terminated"
                )
                return False

            auth.update_conversation(self.db, row["convo_id"], row["tg_user_id"], run_status=status)

            # Only keep live tasks where they earn their keep.
            if status in LIVE_STATUSES or row["active"]:
                await self._adopt(row)
                return True
            return False

    async def _adopt(self, row: sqlite3.Row) -> Conversation | None:
        """Build and start a conversation, protecting against retire-then-replace."""
        convo_id = row["convo_id"]
        lock = self._lifecycle_lock(convo_id)
        async with lock:
            if convo_id in self._convos:
                return self._convos[convo_id]
            convo = self._build(row)
            self._convos[convo_id] = convo
            try:
                await convo.start()
                convo.warn_if_workspace_reclaimed()
            except Exception:  # noqa: BLE001 - the lock already held prevents a race
                log.exception("could not start conversation %s", convo_id)
                self._convos.pop(convo_id, None)
                raise
            return convo

    def _build(self, row: sqlite3.Row) -> Conversation:
        sink = TelegramSink(self.bot, message_thread_id=row["message_thread_id"])
        return Conversation(
            db=self.db,
            api=self.api,
            settings=self.settings,
            row=row,
            sink=sink,
            chat_budget=self.budgets.for_chat(row["chat_id"]),
            stream_slots=self.stream_slots,
            on_fatal=self._on_fatal,
            on_gone=self._on_gone,
            on_ended=self._on_ended,
        )

    async def _on_fatal(self, convo: Conversation) -> None:
        """The chat is unreachable: the user blocked the bot, or we lost the right to post.

        Retired as LOST rather than deleted. ``ChatGone`` covers both "blocked by the user" and
        the transient or administrative cases — "bot can't initiate conversations", "not enough
        rights to send text messages" — and marking those deleted made the conversation
        unrecoverable on a guess. Lost retires it just as firmly but leaves it resumable, so if
        the block is lifted or the rights restored, the next message rebuilds it from the
        on-device transcript instead of starting cold.
        """
        log.warning("chat %s unreachable; retiring conversation %s", convo.chat_id, convo.convo_id)
        auth.mark_conversation_lost(self.db, convo.convo_id, convo.tg_user_id)
        await self.retire(convo.convo_id)
        self.budgets.drop(convo.chat_id)

    async def _on_gone(self, convo: Conversation) -> None:
        """The cloud session vanished, but the chat is still reachable.

        A rotated PAT is the usual cause: the new token cannot see any session the old one
        created, so every one of them 404s. Retiring the conversation is not enough on its own,
        because the message that revealed the problem is already sitting in its durable queue
        and would now never be dispatched — the user would have to notice the failure and type
        it again. So claim those messages, retire, and rebuild.

        The per-chat budget is deliberately kept: the replacement conversation needs it.
        """
        log.info("session for conversation %s is gone; releasing its tasks", convo.convo_id)
        queued = history.take_queued_texts(self.db, convo.convo_id)
        await self.retire(convo.convo_id)
        if self._shutting_down:
            # Do not mint a replacement session during teardown. The queued texts are durable and
            # the conversation is already marked lost, so the next boot reconciles it and /sessions
            # still offers ⟳ RESTORE. Rebuilding now would race the HTTP client that post_shutdown
            # closes the moment Manager.shutdown returns.
            return
        await self._auto_resume(convo, queued)

    async def _on_ended(self, convo: Conversation) -> None:
        """The cloud session ended or was archived; nothing is resumable.

        Retire it so it stops holding three tasks and a stream slot. Unlike :meth:`_on_gone` there
        is no rebuild — an ended session cannot accept messages again — and the conversation's
        ``active`` flag was already cleared on the conversation side, so the user's next message
        opens a fresh conversation instead of enqueueing into a pump that has already exited.
        """
        log.info("session for conversation %s ended; retiring it", convo.convo_id)
        await self.retire(convo.convo_id)

    async def _auto_resume(self, convo: Conversation, queued: list[str]) -> None:
        """Rebuild a lost conversation without being asked, carrying over what was pending.

        Bounded by ``AUTO_RESUME_MAX_ATTEMPTS`` CONSECUTIVE failures per chat. A session that
        vanishes again immediately after being created is not something another rebuild will
        fix, and an unbounded rebuild loop would mint a paid session every few seconds.

        The counter is cleared on success, so it measures a run of failures rather than the
        chat's whole life: without that, three losses spread across a month — each recovered
        cleanly — would disable automatic recovery until the process happened to restart, and
        the fourth loss would leave the user with a dead chat and no explanation.

        Crucially, the counter is NOT incremented on transient failures: a message arriving while
        rebuilding, or the user tapping ⟳ RESTORE mid-flight, are both normal and should not
        poison the retry logic for a genuinely broken session. Only a hard error that actually
        prevents a rebuild counts towards the ceiling.
        """
        key = (convo.chat_id, convo.thread_id)
        attempts = self._resume_attempts.get(key, 0)
        if attempts >= config.AUTO_RESUME_MAX_ATTEMPTS:
            log.error(
                "conversation %s lost its session and rebuilding has already failed %d "
                "time(s) in a row in this chat; not trying again until the bot restarts. The "
                "transcript is safe on disk and /sessions still offers ⟳ RESTORE.",
                convo.convo_id, attempts,
            )
            await self._notify_chat(convo, "That conversation could not be rebuilt automatically. Use /sessions and tap ⟳ RESTORE to try again, or /new to start fresh.")
            return

        # Do NOT increment before attempting: a None result means nothing resuming exists
        # (the user already tapped ⟳ RESTORE), and returning early without popping would leave
        # the counter incremented forever. We also don't count transient failures (QoderError):
        # a message arriving mid-rebuild can cause this error, and counting it would poison
        # the counter on the next normal message. Only a real blockage that prevents resume
        # (None or BillingError) should contribute to the limit.
        try:
            resumed = await self.resume_lost_conversation(
                tg_user_id=convo.tg_user_id,
                chat_id=convo.chat_id,
                message_thread_id=convo.thread_id,
            )
        except BillingError as exc:
            # This is a real blockage: no credits to rebuild with. Count it and bail.
            self._resume_attempts[key] = attempts + 1
            if self._resume_attempts[key] > config.AUTO_RESUME_MAX_ATTEMPTS:
                await self._notify_chat(convo, "Could not rebuild that conversation: no available credit to restore it. Please renew your plan or add credits, then use /sessions and tap ⟳ RESTORE, or /new to start fresh.")
            else:
                await self._notify_chat(convo, f"Could not rebuild that conversation: {exc.message}. Please renew your plan or add credits, then use /sessions and tap ⟳ RESTORE to try again.")
            return
        except QoderError as exc:
            # Not fatal: the user's next message reaches the same path through the handler,
            # where the failure can be reported to them directly. But tell them now so there's
            # no dead silence between one error and the next attempt.
            log.warning("could not rebuild conversation %s: %s", convo.convo_id, exc.message)
            await self._notify_chat(convo, f"Could not rebuild that conversation: {exc.message}. Send your message again and I will retry; or use /sessions and tap ⟳ RESTORE to try again.")
            return

        if resumed is None:
            # Nothing resuming existed — likely the user already tapped ⟳ RESTOIRE somewhere else.
            # Do not count this against the limit; the user explicitly intervened.
            return

        # This rebuild worked, so the next loss starts counting from zero again.
        self._resume_attempts.pop(key, None)

        new_convo, transcript = resumed
        # Hold the transcript durably instead of enqueueing it: it goes out as part of the turn
        # but must NOT be recorded in this new conversation's history, or a second resume would
        # nest the previous resume's history inside this one and then that one inside the next,
        # growing exponentially every time the session was lost.
        history.set_resume_context(self.db, new_convo.convo_id, transcript)

        # Queue the carried-over messages only; _dispatch_burst will prepend pending_transcript
        # once before sending them. If there were none, the transcript sits there waiting for
        # the user's next message — no billable round-trip just to say "Welcome back!".
        for text in queued:
            new_convo.send_text(text)

        if queued:
            new_convo.notice(
                "Your previous conversation has been restored. The cloud session was lost "
                "(likely from API token rotation) and rebuilt on a fresh session, with your "
                f"history carried over from this device.\n\nThe {len(queued)} message(s) you "
                "sent while that was happening have been re-queued and will be processed now."
            )
        else:
            new_convo.notice(
                "Your previous conversation has been restored. The cloud session was lost "
                "(likely from API token rotation) and rebuilt on a fresh session, with your "
                "history carried over from this device. Send a message to continue."
            )
        log.info(
            "auto-resumed conversation %s as %s with %d queued message(s)",
            convo.convo_id, new_convo.convo_id, len(queued),
        )

    def set_show_tools(self, tg_user_id: int, value: bool) -> int:
        """Apply a /tools change to every live conversation this user has.

        Updating only the active one left the others rendering tool activity until they
        happened to be rebuilt, so the toggle appeared not to stick after switching
        conversations. Returns how many were updated.
        """
        updated = 0
        for convo in self._convos.values():
            if convo.tg_user_id == tg_user_id and not convo.stop.is_set():
                convo.renderer.show_tools = value
                updated += 1
        return updated

    # --- conversation access ------------------------------------------------------

    async def active_for(
        self, tg_user_id: int, chat_id: int, message_thread_id: int | None = None
    ) -> Conversation | None:
        """The conversation new messages go to, creating nothing."""
        row = auth.active_conversation(self.db, tg_user_id, chat_id, message_thread_id)
        if row is None:
            return None
        existing = self._convos.get(row["convo_id"])
        if existing and not existing.stop.is_set():
            return existing
        return await self._adopt(row)

    async def _provision_session(
        self, tg_user_id: int, model_id: str, *, title: str
    ) -> tuple[dict, str]:
        """Create a session with this user's agent AND memory store attached.

        Returns ``(session, agent_id)``. The caller must have run ``auth.ensure_user`` first:
        ``agents.tg_user_id`` is a foreign key, and the memory store is read off the user row.

        Shared by :meth:`_create` and :meth:`resume_lost_conversation` because the two must
        agree, and they did not — the resume path built its session with no ``resources`` at
        all, so every conversation rebuilt after a PAT rotation silently lost the user's
        long-term memory. A rotation is precisely what puts every conversation onto that path,
        so this was not an edge case.
        """
        if not self.env_id:
            self.env_id = await provision(self.api)

        # The session takes its model, system prompt and tool permissions from its agent, and
        # the API offers no per-session override. So the model choice only becomes real here.
        agent_id = await self.agent_for(tg_user_id, model_id)

        resources: list[dict] = []
        user = auth.get_user(self.db, tg_user_id)
        cached_store = user["memstore_id"] if user else None
        memstore_id = await ensure_memory_store(
            self.api,
            self.db,
            tg_user_id,
            assume_valid=bool(cached_store) and cached_store in self._verified_stores,
        )
        if memstore_id:
            self._verified_stores.add(memstore_id)
            resources.append(memory_resource(memstore_id, STORE_INSTRUCTIONS))

        session = await self.api.create_session(
            agent_id, self.env_id, title=title, resources=resources or None
        )
        return session, agent_id

    async def create(
        self,
        *,
        tg_user_id: int,
        chat_id: int,
        message_thread_id: int | None = None,
        first_text: str | None = None,
        handle: str | None = None,
        model_id: str | None = None,
        reuse_if_active: bool = False,
    ) -> Conversation:
        """Create a fresh Qoder session and the conversation around it.

        ``reuse_if_active`` is for the IMPLICIT path — an ordinary message from a user who has
        no live conversation. It adopts whatever a concurrent update just created rather than
        minting a second paid session. ``/new`` and the "New chat" button leave it False,
        because starting fresh is the whole point of those.
        """
        async with self._lock_for(chat_id, message_thread_id):
            if reuse_if_active:
                existing = await self.active_for(tg_user_id, chat_id, message_thread_id)
                if existing is not None:
                    return existing
            return await self._create(
                tg_user_id=tg_user_id,
                chat_id=chat_id,
                message_thread_id=message_thread_id,
                first_text=first_text,
                handle=handle,
                model_id=model_id,
            )

    async def _create(
        self,
        *,
        tg_user_id: int,
        chat_id: int,
        message_thread_id: int | None,
        first_text: str | None,
        handle: str | None,
        model_id: str | None,
    ) -> Conversation:
        user = auth.ensure_user(self.db, self.settings, tg_user_id, handle)
        model_id = model_id or user["model_id"] or self.settings.default_model
        title = conversation_title(first_text) if first_text else config.DEFAULT_CONVO_TITLE

        try:
            session, agent_id = await self._provision_session(tg_user_id, model_id, title=title)
        except BillingError as exc:
            log.error("session creation failed due to billing error: %s", exc)
            raise
        except QoderError as exc:
            log.error("session creation failed: %s", exc)
            raise

        convo_id = auth.create_conversation(
            self.db,
            tg_user_id=tg_user_id,
            chat_id=chat_id,
            message_thread_id=message_thread_id,
            title=title,
            session_id=session["id"],
            agent_id=agent_id,
            env_id=self.env_id,
            model_id=model_id,
        )
        # H3: live conversations are never retired on switch/new — unbounded task & socket growth.
        # Before minting a new session, retire any other conversation in this chat that's still
        # running its tasks but won't receive messages anymore — the user is starting fresh here.
        await self._retire_superseded(tg_user_id, chat_id, message_thread_id)

        row = auth.get_conversation(self.db, convo_id, tg_user_id)
        convo = self._build(row)
        self._convos[convo_id] = convo
        await convo.start()
        log.info("created conversation %s (session %s, agent %s, model %s) for user %s",
                 convo_id, session["id"], agent_id, model_id, tg_user_id)
        return convo

    async def _retire_superseded(
        self,
        tg_user_id: int,
        chat_id: int,
        message_thread_id: int | None,
    ) -> int:
        """Retire every active conversation in this chat except the one we're about to adopt.

        ``_lock_for`` owns the critical section for provisioning; entering it serialises with
        other concurrent creates/switches. Without this step, the existing conversation stayed
        in `_convos` with all three tasks running, draining stream slots even though no message
        would ever go to it again — the exact battery/socket drain that keeps conversations
        alive while the user moves to a new one. Returns how many were retired.
        """
        retired = 0
        for cid, convo in list(self._convos.items()):
            if (
                convo.tg_user_id == tg_user_id
                and convo.chat_id == chat_id
                and convo.thread_id == message_thread_id
                and not convo.stop.is_set()
            ):
                await self.retire(cid)
                retired += 1
        return retired

    async def resume_lost_conversation(
        self,
        *,
        tg_user_id: int,
        chat_id: int,
        message_thread_id: int | None = None,
        handle: str | None = None,
        convo_id: int | None = None,
    ) -> tuple[Conversation, str] | None:
        """Rebuild a conversation whose cloud session became unreachable.

        Returns the live conversation and the transcript to send as its first turn, or None if
        there is nothing resumable in this chat.

        ``convo_id`` targets one specific lost conversation; without it the most recent one in
        the chat is chosen. The targeted form is what /sessions' ⟳ RESTORE buttons need, since
        that list shows one button per recoverable conversation.

        This is the recovery path for a rotated PAT: the new token cannot see any session the
        old one created, so every one of them 404s and reconcile retires them. Without this the
        user would silently lose the conversation. With it they get a fresh session that starts
        knowing what was already discussed.

        Deliberately limited to conversations flagged ``lost_session``. An archived one is
        excluded: the user closed that on purpose, and quietly reopening it would undo an
        explicit choice.
        """
        async with self._lock_for(chat_id, message_thread_id):
            row = auth.resumable_conversation(
                self.db, tg_user_id, chat_id, message_thread_id, convo_id=convo_id
            )
            if row is None:
                return None

            old_convo_id = int(row["convo_id"])
            transcript = history.build_transcript(self.db, old_convo_id, title=row["title"])
            retained = history.available_local_files(self.db, old_convo_id)

            user = auth.ensure_user(self.db, self.settings, tg_user_id, handle)
            user_model_id = user["model_id"] if user else None
            model_id = row["model_id"] or user_model_id or self.settings.default_model

            # Remember the store the user had BEFORE provisioning. A rotation onto a different
            # account leaves the cached id pointing at a store that 404s, so ensure_memory_store
            # forgets it and creates a fresh, empty one; comparing the ids afterwards is how we
            # learn the user's long-term memory did not survive, and can tell the agent so.
            old_store = user["memstore_id"] if user else None

            # Through the shared helper, so a rebuilt session carries the user's memory store
            # exactly as a freshly created one does.
            try:
                session, agent_id = await self._provision_session(
                    tg_user_id, model_id, title=row["title"] or "resumed conversation"
                )
            except BillingError as exc:
                log.error("session resume failed due to billing error: %s", exc)
                raise

            after = auth.get_user(self.db, tg_user_id)
            new_store = after["memstore_id"] if after else None
            memory_reset = bool(old_store) and old_store != new_store

            # The old conversation is retired so the new one can hold the (chat, user, active) slot.
            auth.update_conversation(
                self.db, old_convo_id, tg_user_id, lost_session=0, run_status="superseded"
            )

            convo_id = auth.create_conversation(
                self.db,
                tg_user_id=tg_user_id,
                chat_id=chat_id,
                message_thread_id=message_thread_id,
                title=row["title"],
                session_id=session["id"],
                agent_id=agent_id,
                env_id=self.env_id,
                model_id=model_id,
            )

            mounted = await self._restore_files(convo_id, session["id"], retained)

            new_row = auth.get_conversation(self.db, convo_id, tg_user_id)
            convo = self._build(new_row)
            self._convos[convo_id] = convo
            await convo.start()

            log.info(
                "resumed conversation %s as %s (session %s) with %d history event(s) and %d file(s)",
                old_convo_id, convo_id, session["id"],
                history.event_count(self.db, old_convo_id), len(mounted),
            )
            return convo, _compose_resume_message(transcript, mounted, memory_reset=memory_reset)

    async def _restore_files(
        self, convo_id: int, session_id: str, retained: list
    ) -> list[tuple[str, str]]:
        """Re-upload cached files into the new session. Returns (filename, mount_path) pairs.

        A file that fails to upload is skipped rather than aborting the resume: losing one
        attachment is much better than losing the whole conversation.
        """
        mounted: list[tuple[str, str]] = []
        for row in retained:
            path = Path(row["path"])
            try:
                contents = path.read_bytes()
            except OSError as exc:
                log.warning("cached file %s vanished before restore: %s", path, exc)
                continue
            filename = row["filename"] or path.name
            try:
                uploaded = await self.api.upload(
                    contents, filename,
                    metadata={"convo_id": convo_id, "source": "resume",
                              "original_file_id": row["file_id"]},
                )
                new_file_id = uploaded.get("id")
                if not new_file_id:
                    log.warning("re-upload of %s returned no file id", filename)
                    continue
                mount_path = uploads.unique_mount_path(filename)
                await self.api.attach_file(session_id, new_file_id, mount_path)
            except QoderError as exc:
                log.warning("could not restore %s into session %s: %s",
                            filename, session_id, exc.message)
                continue
            mounted.append((filename, mount_path))
            history.retain_file(
                self.db, convo_id=convo_id, file_id=new_file_id, contents=contents,
                filename=filename, owner_type=row["owner_type"],
            )
        return mounted

    async def switch_to(self, convo_id: int, tg_user_id: int) -> Conversation | None:
        """Activate an existing conversation. Ownership is checked inside auth."""
        if not auth.switch_active_conversation(self.db, convo_id, tg_user_id):
            return None
        row = auth.get_conversation(self.db, convo_id, tg_user_id)
        if row is None:
            return None
        existing = self._convos.get(convo_id)
        if existing and not existing.stop.is_set():
            existing.warn_if_workspace_reclaimed()
            return existing
        convo = await self._adopt(row)
        if convo:
            convo.warn_if_workspace_reclaimed()
        return convo

    def get(self, convo_id: int) -> Conversation | None:
        return self._convos.get(convo_id)

    def live_count(self, tg_user_id: int | None = None) -> int:
        """Conversations with live tasks right now, optionally filtered to one user.

        Public so /health need not poke at _convos. The filter matters: every allowlisted user
        shares this process, and a global count told whoever asked how busy everyone else was.
        """
        return sum(
            1
            for convo in self._convos.values()
            if not convo.stop.is_set()
            and (tg_user_id is None or convo.tg_user_id == tg_user_id)
        )

    async def retire(self, convo_id: int) -> None:
        """Retire a conversation: shut down its tasks and remove it from the active set."""
        # Lifecycle lock: retire() pops from _convos then awaits shutdown() (seconds-long work);
        # without this guard a concurrent active_for or switch_to saw the empty slot mid-retirement
        # and adopted a SECOND live Conversation for the same row — two renderers writing one chat
        # at once, breaking the single-writer invariant every other path obeys.
        lock = self._lifecycle_lock(convo_id)
        async with lock:
            convo = self._convos.pop(convo_id, None)
            if convo:
                await convo.shutdown()
                log.info("retired conversation %s", convo_id)

        # Prune the per-chat provision locks to keep the registry bounded. Only unlocked entries
        # are dropped — discarding a held lock would let a second caller into the critical section
        # it exists to own. A 256-entry ceiling is high enough for any realistic deployment and
        # low enough that the worst-case memory leak is negligible even when the process runs for
        # weeks on an always-on phone.
        stale = [k for k, v in self._provision_locks.items() if not v.locked()]
        if len(stale) + len(self._provision_locks) > 256:
            for key in stale[:len(stale)//2]:
                self._provision_locks.pop(key, None)

        # Prune the lifecycle locks we just released; they can grow unbounded if conversations
        # are archived/forgotten in quick succession and the garbage collector doesn't notice.
        self._lifecycle_locks.pop(convo_id, None)

    async def forget_chat(self, chat_id: int) -> int:
        """Retire every live conversation bound to a chat id. Returns how many were retired.

        Called before a chat id is rewritten, when Telegram upgrades a basic group to a
        supergroup. The live objects cache the old id in several places this class owns — the
        row they were built from, their ``TelegramSink``, the per-chat send budget, the
        resume-attempt counters and the provisioning locks — and a sink pointed at a superseded
        chat id posts into nothing. Retiring them means the next message rebuilds them from the
        rewritten rows.
        """
        stale = [cid for cid, convo in self._convos.items() if convo.chat_id == chat_id]
        for convo_id in stale:
            await self.retire(convo_id)
        self.budgets.drop(chat_id)
        for key in [k for k in self._resume_attempts if k[0] == chat_id]:
            self._resume_attempts.pop(key, None)
        # Only drop locks nobody is holding: discarding a held one would let a second caller
        # into the critical section it exists to own.
        for key, lock in list(self._provision_locks.items()):
            if key[0] == chat_id and not lock.locked():
                self._provision_locks.pop(key, None)
        if stale:
            log.info("released %d live conversation(s) bound to superseded chat %s",
                     len(stale), chat_id)
        return len(stale)

    async def archive(self, convo_id: int, tg_user_id: int) -> bool:
        """Close a conversation remotely and locally.

        The session id comes from the row, not from the in-memory conversation: /archive is
        reachable for a conversation this process never adopted, and reading it from _convos
        meant the remote call was silently skipped and the sandbox left open in the account.

        The local update is authoritative: the user asked to archive, and a mobile-link blip
        on the remote call must not leave the conversation sitting at active = 1.
        """
        row = auth.get_conversation(self.db, convo_id, tg_user_id)
        if row is None:
            return False
        session_id = row["session_id"]
        if session_id:
            try:
                await self.api.archive_session(session_id)
            except QoderError as exc:
                log.warning("remote archive of %s failed (%s); archiving locally anyway",
                            session_id, exc.message)
        auth.update_conversation(
            self.db, convo_id, tg_user_id,
            archived_at=utcnow(), run_status="archived", active=0,
        )
        await self.retire(convo_id)
        return True

    # --- shutdown -----------------------------------------------------------------

    async def shutdown(self) -> None:
        log.info("shutting down %d conversations", len(self._convos))
        await asyncio.gather(
            *(convo.shutdown() for convo in list(self._convos.values())),
            return_exceptions=True,
        )
        self._convos.clear()


def _compose_resume_message(
    transcript: str,
    mounted_files: list[tuple[str, str]],
    *,
    memory_reset: bool = False,
) -> str:
    """Message to send as the resumed conversation's first turn.

    Empty when there is nothing to carry over. The "History follows" scaffolding used to be
    emitted unconditionally, so a conversation with no stored history handed the model a
    contentless preamble between two rules — and now that handlers._active resumes on an
    ordinary message, that preamble rides along with what the user actually said.
    """
    transcript = transcript.strip()
    if not transcript and not mounted_files and not memory_reset:
        return ""
    lines = ["Resuming conversation from when your session was lost."]
    if mounted_files:
        # Spell out each file's NEW mount path. The transcript below still references the paths
        # these files had in the old sandbox, and a restored file is re-mounted at a fresh random
        # path — so listing names alone (as this used to) left the agent Reading stale paths that
        # no longer resolve. This is the same "mounted at exactly" contract an inbound upload's
        # pointer_text gives it.
        lines.append(
            "These files were carried over into your NEW sandbox. Any path for them that appears "
            "in the transcript below is STALE — they are now mounted at exactly these paths, so "
            "Read them from here:"
        )
        for name, path in mounted_files:
            lines.append(f"- {name} → {path}")
    if memory_reset:
        # The session was rebuilt under a different account, so the per-user memory store could
        # not be carried over and a fresh, empty one was provisioned in its place. Without this
        # the agent follows its standing instruction to read its awareness files at the start of
        # a task and fails on a MEMORY.md that no longer exists.
        lines.append(
            "Note: this session was rebuilt under a different account, so your long-term memory "
            "store is EMPTY — awareness files such as /data/.qoder/awareness/MEMORY.md from "
            "before are not available. Do not try to read them; rely on the transcript below, and "
            "re-create any memory notes you still need as you go."
        )
    if transcript:
        lines.append("History follows:\n---\n")
        lines.append(transcript)
        lines.append("---")
    return "\n".join(lines)



def model_choices_text(models: list[dict], current: str) -> str:
    """Compact model list for /model, cheapest first."""
    ordered = sorted(models, key=lambda m: m.get("price_factor") or 0)
    lines = ["<b>Models</b> (price factor · default effort, ✱ = current)"]
    for model in ordered:
        marker = " ✱" if model.get("id") == current else ""
        vision = "vision" if model.get("is_vl") else "no vision"
        # The default effort belongs next to the price: ten of the eighteen current models run
        # at max, xhigh or high unless told otherwise, which multiplies the real cost by far
        # more than the price factor alone suggests. "fixed" means the model advertises no
        # efforts at all, so /effort cannot change it.
        effort = model.get("default_effort") if model.get("efforts") else None
        effort_label = effort or "fixed"
        # Escaped: these fields come from the API, and a stray '<' would make Telegram reject
        # the whole message rather than just that line.
        lines.append(
            f"<code>{tg_html.escape(str(model.get('id', '')))}</code> "
            f"— {tg_html.escape(str(model.get('display_name', '')))} "
            f"· {model.get('price_factor', 0)}× · {tg_html.escape(str(effort_label))} "
            f"· {vision}{marker}"
        )
    lines.append("")
    lines.append(
        "Usage: <code>/model &lt;id&gt;</code>, then <code>/effort &lt;level&gt;</code>. "
        "Both take effect from your next <code>/new</code>."
    )
    return "\n".join(lines)
