"""One conversation = one Qoder session = three asyncio tasks.

The tasks are created and cancelled together, because a half-alive conversation is worse than
a dead one:

* ``render_task`` — the only writer to Telegram for this conversation (single-writer invariant).
* ``consume_task`` — reads the SSE stream, or polls history when no stream slot is free.
* ``pump_task`` — drains the durable inbound queue, but ONLY while the session is idle. The
  API has no queue and returns 409 if we post during a turn, so this task is the serializer.

There is no separate reaper. A session that has gone away is noticed lazily, by ``_idle_now``
asking the API before each dispatch and by the stream consumer seeing the session end.

The inbound queue lives in SQLite, not in memory. Android kills this process without warning,
and a message the user already sent must not vanish because of that.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone

from . import auth, budget, config, history, tg_html
from .artifacts import ArtifactDeliverer
from .db import Database, utcnow
from .qclient import BillingError, Conflict, NotFound, QoderError, Unauthorized
from .qsessions import QoderAPI
from .qstream import AckTracker, Frame, StreamConsumer
from .renderer import BILLING_MARKERS, ChatBudget, RenderHooks, Renderer
from .tgsink import TelegramSink

log = logging.getLogger("tgagent.convo")


def _parse_ts(value: str | None) -> datetime | None:
    """Parse an ISO-8601 timestamp into an aware UTC datetime, or None if unparseable.

    A missing offset is treated as UTC rather than left naive: the caller subtracts this from
    ``datetime.now(timezone.utc)``, and mixing naive with aware raises TypeError. That call
    happens inside reconcile() at startup, so one naive timestamp in the database would be
    enough to abort boot.
    """
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


class Conversation:
    def __init__(
        self,
        *,
        db: Database,
        api: QoderAPI,
        settings: config.Settings,
        row: sqlite3.Row,
        sink: TelegramSink,
        chat_budget: ChatBudget,
        stream_slots: asyncio.Semaphore,
        on_fatal: Callable[[Conversation], Awaitable[None]] | None = None,
        on_gone: Callable[[Conversation], Awaitable[None]] | None = None,
        on_ended: Callable[[Conversation], Awaitable[None]] | None = None,
    ):
        self.db = db
        self.api = api
        self.settings = settings
        self.sink = sink
        self.chat_budget = chat_budget
        self.stream_slots = stream_slots
        self.on_fatal = on_fatal
        self.on_gone = on_gone
        self.on_ended = on_ended

        self.convo_id: int = row["convo_id"]
        self.tg_user_id: int = row["tg_user_id"]
        self.chat_id: int = row["chat_id"]
        self.thread_id: int | None = row["message_thread_id"]
        self.session_id: str | None = row["session_id"]
        self.title: str | None = row["title"]
        # Recorded against every credit this conversation spends. The span.model_request_end
        # payload carries only the credit figure, so the model has to come from here.
        self.model_id: str | None = row["model_id"]

        self.stop = asyncio.Event()
        self.wake = asyncio.Event()
        self.terminated = False
        self.credentials_rejected = False
        self._tasks: list[asyncio.Task] = []
        # The live SSE consumer, when there is one. Renderer acknowledgements are routed here
        # so the durable cursor can advance; None means we are in the poll fallback.
        self._consumer: StreamConsumer | None = None
        # The poll fallback has no consumer to own the cursor, so it uses the same tracker.
        self._poll_acks = AckTracker(commit=self._set_cursor)
        # rendered_events.seq position of the last cursor committed, for the monotonic guard in
        # _set_cursor. None until the first commit that has a rendered position to compare.
        self._cursor_seq: int | None = None
        # Strong references to tasks that retire this conversation. They are created on the
        # loop rather than awaited, because awaiting a shutdown from inside one of _tasks makes
        # that task cancel and await itself.
        self._detached: set[asyncio.Task] = set()

        # Burst debouncing state: the quiet period ends after no new message has been enqueued
        # for settings.burst_debounce_s seconds. Measured from the most recent enqueue time so
        # bursts settle soon after their last line, while a message that has already been
        # sitting in the queue (e.g. typed during a long turn) dispatches immediately.
        self.last_enqueue_mono = 0.0

        # Pruning rendered_events counts every row for the conversation, so doing it on every
        # acknowledgement made a long conversation quadratic. Count instead and prune in
        # batches; the dedupe ceiling is overshot by at most one batch.
        self._renders_since_prune = 0

        user = auth.get_user(db, self.tg_user_id)
        self.renderer = Renderer(
            chat_id=self.chat_id,
            sink=sink,
            budget=chat_budget,
            show_tools=bool(user["show_tools"]) if user else True,
            hooks=RenderHooks(
                on_idle=self._on_idle,
                on_artifact=self._on_artifact,
                on_credits=self._on_credits,
                on_error=self._on_error,
                on_terminated=self._on_terminated,
                on_run_started=self._on_run_started,
                on_rendered=self._record_rendered,
            ),
        )
        self.artifacts = ArtifactDeliverer(
            api=api,
            db=db,
            sink=sink,
            budget=chat_budget,
            convo_id=self.convo_id,
            chat_id=self.chat_id,
            # Route artifact notices through the renderer so there is still exactly one writer to
            # this chat. Sending them straight through the sink let an artifact notice interleave
            # with a status edit the renderer was mid-way through.
            notifier=self.renderer.post_notice,
        )

    # --- lifecycle ----------------------------------------------------------------

    async def start(self) -> None:
        self._tasks = [
            asyncio.create_task(self._render_loop(), name=f"render-{self.convo_id}"),
            asyncio.create_task(self._consume_loop(), name=f"consume-{self.convo_id}"),
            asyncio.create_task(self._pump_loop(), name=f"pump-{self.convo_id}"),
        ]
        # Anything left undelivered by a crash gets another attempt. A failure here must NOT
        # abort start(): the three tasks are already running, and letting it propagate left the
        # conversation registered and live while the caller reported "could not start" — tasks
        # nobody would ever shut down. Artifact retry is best-effort at startup.
        try:
            pending = await self.artifacts.retry_pending()
            if pending:
                log.info("convo %s retrying %d undelivered artifacts", self.convo_id, pending)
        except Exception:  # noqa: BLE001 - startup artifact retry must not kill the conversation
            log.exception("convo %s could not retry pending artifacts", self.convo_id)
        self.wake.set()

    async def shutdown(self) -> None:
        self.stop.set()
        self.wake.set()
        # Cancel existing tasks and await them. Detached tasks are separate from the main three;
        # they're created by _schedule_detached to avoid a task awaiting its own shutdown from
        # inside the event loop. We must still await them here or the HTTP client closes while
        # one is in-flight — except the current task: retire() → shutdown() called from inside
        # a detached _on_gone task would otherwise gather itself and raise RuntimeError.
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.renderer.wait_for_hooks(timeout=5.0)
        await self.renderer.force_flush()
        # Await all detached tasks so _auto_resume cannot mint a replacement session after the
        # process tears down and the HTTP client has already been closed.
        if self._detached:
            current = asyncio.current_task()
            await asyncio.gather(
                *(t for t in self._detached if t is not current), return_exceptions=True
            )

    # --- durable cursor -----------------------------------------------------------

    def _get_cursor(self) -> str | None:
        row = self.db.query_one(
            "SELECT last_event_id FROM conversations WHERE convo_id = ?", (self.convo_id,)
        )
        return row["last_event_id"] if row else None

    def _set_cursor(self, event_id: str | None) -> None:
        """Advance the durable cursor, never backwards.

        Two AckTrackers commit through here — the live consumer's and the poll fallback's — and
        ``_record_rendered`` acknowledges BOTH on every write, because an event offered while
        polling can be confirmed after a stream consumer has taken over. Across that handover the
        laggard tracker can present an older event id after the leader already committed a newer
        one. A backward jump is not data loss (the durable dedupe suppresses the re-delivery) but
        it forces a full history re-walk on the next reconnect, which on a phone is real battery
        and quota.

        Ordering is taken from ``rendered_events.seq`` — the local monotonic order in which events
        were actually rendered — rather than from the opaque ids, which are not guaranteed
        comparable. A rewind is refused ONLY when both the committed cursor and the new id have a
        rendered position and the new one is earlier; an id with no row (an ephemeral event acked
        on sight, or a cold start) has nothing to compare and is always written, because refusing
        it could freeze the cursor — strictly worse than a redundant re-walk.
        """
        if event_id is None:
            self.db.execute(
                "UPDATE conversations SET last_event_id = NULL WHERE convo_id = ?",
                (self.convo_id,),
            )
            self._cursor_seq = None
            return
        new_seq = self._rendered_seq(event_id)
        if new_seq is not None and self._cursor_seq is not None and new_seq < self._cursor_seq:
            log.debug(
                "convo %s ignoring a backward cursor commit %s (seq %d < %d)",
                self.convo_id, event_id, new_seq, self._cursor_seq,
            )
            return
        self.db.execute(
            "UPDATE conversations SET last_event_id = ? WHERE convo_id = ?",
            (event_id, self.convo_id),
        )
        if new_seq is not None:
            self._cursor_seq = new_seq

    def _rendered_seq(self, event_id: str) -> int | None:
        row = self.db.query_one(
            "SELECT seq FROM rendered_events WHERE convo_id = ? AND event_id = ?",
            (self.convo_id, event_id),
        )
        return int(row["seq"]) if row else None

    def _set_status(self, status: str, stop_reason: dict | None = None) -> None:
        auth.update_conversation(
            self.db,
            self.convo_id,
            self.tg_user_id,
            run_status=status,
            last_stop_reason=json.dumps(stop_reason) if stop_reason else None,
        )

    def run_status(self) -> str:
        """A one-line description of this conversation's state, for /health.

        The in-memory conclusions are checked first because they override whatever the row
        says. The row is then read rather than guessed at: it holds what the last reconcile or
        status event recorded, and the two diverge exactly when something has gone wrong — a
        process that restarted mid-turn has flags that mean nothing and a row that means
        everything.
        """
        if self.credentials_rejected:
            return "credentials rejected"
        if self.terminated:
            return "terminated"
        row = self.db.query_one(
            "SELECT run_status, lost_session FROM conversations WHERE convo_id = ?",
            (self.convo_id,),
        )
        if row is None:
            return "unknown"
        if row["lost_session"]:
            return "session lost"
        return row["run_status"] or "idle"

    # --- inbound ------------------------------------------------------------------

    def enqueue(self, kind: str, payload: dict) -> None:
        """Durable enqueue. Survives a process kill between here and the API call."""
        self.last_enqueue_mono = time.monotonic()
        self.db.execute(
            """INSERT INTO inbound_queue(convo_id, kind, payload_json, state, created_at)
               VALUES(?, ?, ?, 'queued', ?)""",
            (self.convo_id, kind, json.dumps(payload), utcnow()),
        )
        self.wake.set()

    def send_text(self, text: str) -> None:
        self.enqueue("text", {"text": text})

    def notice(self, text: str, *, is_error: bool = False) -> None:
        """A bot-composed message. Routed through the renderer so there is still one writer."""
        self.renderer.post_notice(text, is_error=is_error)

    async def request_stop(self) -> bool:
        """Cancel the running turn AND drop anything still waiting to be sent.

        Cancelling the session alone was not enough: a message sitting in the debounce window or
        queued behind the turn dispatched the moment the session went idle, so /stop appeared to
        work and then the very message the user was trying to stop arrived anyway. Pending rows
        are marked cancelled so the pump skips them.

        The session is only told to cancel — and the status only set to ``canceling`` — when a turn
        is actually running. Against an idle session the cancel is a no-op, and claiming otherwise
        left the conversation stuck in ``canceling`` with nothing to cancel while the user was told
        "could not cancel that turn" about a message that then sent regardless.

        One mechanism, not two: ``POST /sessions/{id}/cancel`` is the documented verb. The pump
        still knows how to retire a legacy ``interrupt`` row, because one may sit in a database
        written before this change.
        """
        dropped = self._cancel_queued()
        if not self.session_id:
            if dropped:
                self.notice(f"Dropped {dropped} message(s) that had not been sent yet.")
            return dropped > 0

        running = self.renderer.running
        try:
            await self.api.cancel_session(self.session_id)
        except QoderError as exc:
            log.warning("cancel failed for %s: %s", self.session_id, exc)
            if not dropped:
                self.notice(f"Could not cancel that turn: {exc.message}", is_error=True)
            return dropped > 0

        if running:
            self._set_status("canceling")
            self.notice("Cancelling the current turn…")
        if dropped:
            self.notice(f"Dropped {dropped} message(s) that had not been sent yet.")
        return True

    def _cancel_queued(self) -> int:
        """Mark every not-yet-sent inbound row cancelled, so the pump will not dispatch it.

        Returns how many were dropped. ``cancelled`` is a terminal state distinct from ``sent``
        and from :data:`config.RESUME_QUEUE_STATE`: both :meth:`_next_queued` and
        ``history.take_queued_texts`` select on ``state = 'queued'``, so a cancelled row is
        neither dispatched nor carried over to a rebuilt conversation.
        """
        cur = self.db.execute(
            """UPDATE inbound_queue SET state = 'cancelled', sent_at = ?
               WHERE convo_id = ? AND state = 'queued'""",
            (utcnow(), self.convo_id),
        )
        return cur.rowcount

    # --- pump: the 409 serializer -------------------------------------------------

    async def _wait_for_quiet(self) -> bool:
        """Wait until the inbound queue has been quiet for ``settings.burst_debounce_s``.

        If a new message arrives while we're waiting, the quiet timer resets from its time
        point, giving bursts time to coalesce into a single turn. Returns False if stopped,
        True when the burst has settled.
        """
        debounce = self.settings.burst_debounce_s
        if self.stop.is_set():
            return False
        deadline = self.last_enqueue_mono + debounce
        while not self.stop.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return True
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=min(remaining, 0.5))
            except asyncio.TimeoutError:
                continue
            # A signal arrived — recompute deadline from the latest enqueue.
            self.wake.clear()
            deadline = self.last_enqueue_mono + debounce
        return False

    async def _dispatch_burst(self) -> bool:
        """Coalesce every queued text row into ONE user.message and send it."""
        rows = self.db.query(
            """SELECT * FROM inbound_queue
               WHERE convo_id = ? AND state='queued' AND kind='text' ORDER BY id""",
            (self.convo_id,),
        )
        if not rows:
            return True
        parts = []
        for row in rows:
            payload = json.loads(row["payload_json"])
            # Strip BEFORE testing: appending an empty string would leave `parts` non-empty,
            # skip the guard below, and post a contentless message the API rejects outright.
            text = (payload.get("text") or "").strip()
            if text:
                parts.append(text)
        if not parts:
            # No usable content — still retire the rows, or the pump would spin on them.
            self._mark_sent_many([int(row["id"]) for row in rows])
            return True

        ids = [int(row["id"]) for row in rows]
        combined_text = "\n\n".join(parts)
        # Prepend any held resume transcript. This is context for the model, not something the
        # user said, so it goes out as part of the turn but must NOT be recorded in this
        # conversation's history — or a second resume would nest the previous resume's transcript
        # inside the next one, growing exponentially every time the session was lost.
        outgoing = combined_text
        transcript = history.peek_resume_context(self.db, self.convo_id)
        if transcript:
            outgoing = f"{transcript}\n\n{combined_text}"

        try:
            await self.api.send_message(self.session_id, outgoing)
        except Conflict:
            # A turn is already running. The rows stay 'queued', so the pump picks them up
            # again when session.status_idle wakes it.
            log.info("convo %s hit a 409; leaving burst queued", self.convo_id)
            return False
        except NotFound:
            # The session is gone — a rotated PAT makes every session the old token created
            # return 404. Leaving the rows 'queued' is what lets the rebuild carry them across:
            # history.take_queued_texts claims only that state, and the message that revealed
            # the dead session is the one already sitting there.
            log.info("convo %s posted to a session that no longer exists", self.convo_id)
            if await self._session_is_gone():
                self._on_session_gone()
            return False

        # Marked sent only now that the API has accepted the turn. Marking BEFORE the POST —
        # which is what this used to do — lost the message on every failure path above, plus
        # any transport error: qclient deliberately does not retry a POST after a ReadTimeout,
        # because the server may have acted on it, so one blip on a mobile link surfaced here
        # as a QoderError and the pump then retried a queue that was already empty.
        #
        # The residual risk is the mirror image and the smaller one: a crash between a
        # successful POST and this write re-sends one message. This module already prefers the
        # duplicate over the loss — see AckTracker, "losing text is unrecoverable, re-delivering
        # a duplicate is merely untidy" — and _drain_queue asks the API whether the session is
        # idle before every dispatch, so a turn the POST did start is not posted over.
        self._mark_sent_many(ids)

        # Record what the user actually said, not the injected context. If we died between
        # sending and this write, the echo will replay the full text including the
        # transcript; that's why we skip recording user.message frames here in the first
        # place — see _on_frame.
        if transcript:
            history.clear_resume_context(self.db, self.convo_id)
        history.record_frame(
            self.db, self.convo_id,
            Frame(kind="event", type="user.message",
                  payload={"type": "user.message",
                           "content": [{"type": "text", "text": combined_text}]}),
        )
        log.debug("coalesced %d queued messages into one", len(rows))
        return True

    async def _pump_loop(self) -> None:
        # The terminated check matters as much as the stop check. Without it a conversation
        # whose session had ended or gone kept cycling here for the rest of the process's
        # life: _next_queued still returned the undispatched row, _idle_now kept reporting the
        # session unavailable, and the loop retried every PUMP_RETRY_S.
        while not self.stop.is_set() and not self.terminated:
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass
            self.wake.clear()
            if self.stop.is_set():
                return

            try:
                await self._drain_queue()
            except asyncio.CancelledError:
                raise
            except Unauthorized as exc:
                # Retrying a rejected credential cannot succeed, and the generic path below
                # logs a full traceback every PUMP_RETRY_S, which would churn the rotating log
                # for as long as the process lives. Give up loudly instead — the queue is
                # durable, so a restart with a working PAT resumes where this left off. The
                # stream consumer gives up on 401/403 for the same reason.
                log.error(
                    "pump for convo %s stopped: Qoder rejected our credentials (%s)",
                    self.convo_id, exc.message,
                )
                self._give_up_on_credentials(message_is_queued=True)
                return
            except BillingError as exc:
                # Out of credits — do not retry forever and fill the log. The message stays
                # queued, so give up and mark it stopped. A restart will not help; credits must
                # be added.
                self._stop_for_billing(exc, "pump")
                return
            except Exception:  # noqa: BLE001 - the pump must never die silently
                # This task is the only thing that can post a turn, so letting an exception
                # escape wedges the conversation for the rest of the process's life. The queue
                # is durable, which makes backing off and retrying always safe.
                log.exception("pump failed for convo %s; retrying", self.convo_id)
                await _sleep_or_stop(self.stop, config.PUMP_RETRY_S)
                # Signal ourselves, otherwise the outer wait adds up to another 5s of latency
                # before the retry is attempted.
                self.wake.set()

    async def _drain_queue(self) -> None:
        """Dispatch queued rows until the queue is empty, a turn is in flight, or we are done.

        Re-checks periodically even without a signal: a missed status_idle (for example one
        that arrived while the process was dead) would otherwise wedge the queue. Stops on
        ``terminated`` as well as ``stop``, so a session that has ended or gone does not leave
        this spinning over a queue it can never dispatch.
        """
        while not self.stop.is_set() and not self.terminated:
            row = self._next_queued()
            if row is None:
                break
            if row["kind"] == "interrupt":
                # Marked sent only AFTER the API call, matching the rule _dispatch_burst follows
                # for text rows. Marking first meant a send_interrupt that raised left the row
                # retired and the interrupt silently lost — the one place that violated this
                # module's own "mark after the API accepts" invariant.
                if self.session_id:
                    await self.api.send_interrupt(self.session_id)
                self._mark_sent(int(row["id"]))
                continue
            if not await self._idle_now():
                break  # a turn is in flight; wait for status_idle
            # Burst debouncing: wait until the queue has been quiet for DEBOUNCE_S seconds
            # so rapid successive messages coalesce into one turn instead of several.
            if not await self._wait_for_quiet():
                continue
            # Now dispatch everything currently queued as one turn.
            if not await self._dispatch_burst():
                break

    def _next_queued(self) -> sqlite3.Row | None:
        return self.db.query_one(
            """SELECT * FROM inbound_queue
               WHERE convo_id = ? AND state = 'queued' ORDER BY id LIMIT 1""",
            (self.convo_id,),
        )

    def _mark_sent(self, row_id: int) -> None:
        self.db.execute(
            "UPDATE inbound_queue SET state = 'sent', sent_at = ? WHERE id = ?",
            (utcnow(), row_id),
        )

    def _mark_sent_many(self, row_ids: list[int]) -> None:
        """Retire a coalesced burst in one statement, so the rows cannot be split apart."""
        if not row_ids:
            return
        placeholders = ",".join("?" * len(row_ids))
        self.db.execute(
            f"UPDATE inbound_queue SET state = 'sent', sent_at = ? WHERE id IN ({placeholders})",
            [utcnow(), *row_ids],
        )

    async def _session_is_gone(self) -> bool:
        """Whether the cloud session has definitively ceased to exist.

        Only a 404 counts. A transport error or a 5xx is not proof, and retiring a healthy
        conversation because of one blip on a mobile link would cost the user their session —
        the same rule the environment, agent and memory-store lookups already follow.
        """
        if not self.session_id:
            return True
        try:
            return await self.api.get_session(self.session_id) is None
        except QoderError as exc:
            log.debug("could not check whether session %s exists: %s", self.session_id, exc)
            return False

    async def _idle_now(self) -> bool:
        """Ask the API rather than trusting our cached status.

        The cache can be stale after a restart or a missed event, and posting during a turn
        costs a 409 and a confusing retry loop.
        """
        if not self.session_id:
            return False
        session = await self.api.get_session(self.session_id)
        if session is None:
            # Gone, not merely busy. Handing this to _on_session_gone rather than just saying
            # so matters: this path leaves the queued row in place, the pump retries every few
            # seconds, and nothing else retires the conversation — so a notice here was
            # re-posted forever, one message every five seconds. _on_session_gone marks the
            # conversation lost, tells the user once, and triggers the rebuild.
            self._on_session_gone()
            return False
        status = session.get("status")
        if session.get("archived_at") or status == "terminated":
            self.terminated = True
            self._set_status("terminated")
            self.notice("That conversation has ended and can no longer accept messages. "
                        "Use /new to start another.", is_error=True)
            self._wind_down(clear_active=True)
            return False
        self._note_actual_model(session)
        self._set_status(status or "idle")
        return status in (None, "idle")

    def _note_actual_model(self, session: dict) -> None:
        """Correct the recorded model to the one the session is actually running.

        ``conversations.model_id`` is what the bot asked for when it created the session, and
        every ``spend`` row copies it — so the ledger labels each credit with an assumption
        rather than a fact. The session payload carries the truth at ``agent.model.id``, and
        this method's caller has already fetched it, so checking costs nothing extra.

        The two diverge whenever the agent is changed underneath a live session: someone edits
        it in the console, another client PUTs it, or the user runs /model — a session keeps the
        model it started with, so only /new moves it. Without this, /usage confidently reported
        the cheap model while the account was being billed for the expensive one.
        """
        actual = ((session.get("agent") or {}).get("model") or {}).get("id")
        if not actual or actual == self.model_id:
            return
        log.warning(
            "convo %s is running on %r, not the %r recorded for it; correcting the ledger",
            self.convo_id, actual, self.model_id,
        )
        self.model_id = actual
        auth.update_conversation(
            self.db, self.convo_id, self.tg_user_id, model_id=actual
        )

    # --- stream consumption -------------------------------------------------------

    async def _consume_loop(self) -> None:
        while not self.stop.is_set() and not self.terminated:
            if not self.session_id:
                await _sleep_or_stop(self.stop, 2.0)
                continue

            # Take a slot only if one is free right now. Wrapping acquire() in wait_for — which
            # is what this used to do — can cancel an acquire that had already been granted the
            # slot, leaking it permanently; three leaks and every conversation is stuck in the
            # poll fallback for the life of the process. A semaphore with a positive value and
            # no waiters completes acquire() without yielding to the loop, so this cannot leak.
            got_slot = not self.stream_slots.locked()
            if got_slot:
                await self.stream_slots.acquire()

            consumer: StreamConsumer | None = None
            try:
                if got_slot:
                    consumer = StreamConsumer(
                        client=self.api.client,
                        session_id=self.session_id,
                        get_cursor=self._get_cursor,
                        set_cursor=self._set_cursor,
                        on_frame=self._on_frame,
                    )
                    # Published so the renderer's acknowledgements can advance the cursor.
                    self._consumer = consumer
                    await consumer.run(self.stop)
                    if consumer.gave_up:
                        self._give_up_on_credentials()
                        return
                    if consumer.session_gone:
                        self._on_session_gone()
                        return
                else:
                    # No stream slot free. Polling is a first-class degradation, not an error:
                    # the same frames reach the renderer, just less promptly.
                    await self._poll_history()
                    if self.terminated:
                        return
                    await _sleep_or_stop(self.stop, config.POLL_FALLBACK_INTERVAL_S)
            except asyncio.CancelledError:
                raise
            except Unauthorized as exc:
                log.error(
                    "consumer for convo %s stopped: Qoder rejected our credentials (%s)",
                    self.convo_id, exc.message,
                )
                self._give_up_on_credentials()
                return
            except BillingError as exc:
                # Out of credits — the session cannot generate anything more until credits are
                # added, so there is nothing left to consume. A restart will not help.
                self._stop_for_billing(exc, "consumer")
                return
            except Exception:  # noqa: BLE001 - a consumer must never die silently
                log.exception("consumer failed for convo %s", self.convo_id)
                await _sleep_or_stop(self.stop, 5.0)
            finally:
                self._consumer = None
                if got_slot:
                    self.stream_slots.release()

    def _give_up_on_credentials(self, *, message_is_queued: bool = False) -> None:
        """Stop working and say why, once.

        Retrying a rejected PAT cannot succeed, and reconnecting in a loop would issue one
        doomed request after another for the rest of the process's life. Both the pump and the
        consume loop can detect this, so the guard here is what keeps the user from being told
        twice about the same dead token.

        Nothing is lost by stopping: the cursor, the rendered-event log and the inbound queue
        are all durable, so a restart with working credentials resumes exactly here.
        """
        if self.credentials_rejected:
            return
        self.credentials_rejected = True
        detail = (
            "Your message is still queued and will go once they are."
            if message_is_queued
            else "Anything already delivered to you is safe."
        )
        self.notice(
            "The Qoder credentials were rejected, so this conversation has stopped. "
            f"Fix the token in .env and restart the bot to resume. {detail}",
            is_error=True,
        )
        # Recoverable, so active stays set: a restart re-adopts this same conversation. Retiring
        # it here means a message before that restart re-adopts and re-reports the dead token
        # instead of enqueueing into a pump that has already given up.
        self._wind_down(clear_active=False)

    def _stop_for_billing(self, exc: BillingError, source: str) -> None:
        """Retire the conversation and say why, once, for any path that hits a 402.

        The pump, the stream consumer and the poll fallback can each be the first to see an
        exhausted credit balance, and all three must end in the same place: terminated locally,
        with one notice. ``source`` only labels the log line.

        Idempotent on ``terminated`` for the same reason :meth:`_give_up_on_credentials` guards
        on ``credentials_rejected`` — two of the three paths can notice the same dead balance
        within one tick, and the user should not be told twice.

        Nothing is lost by stopping: the inbound queue is durable, so the message that could not
        be paid for is still there after credits are added and the bot restarts.
        """
        log.error("%s for convo %s stopped: billing error (%s)", source, self.convo_id, exc.message)
        if self.terminated:
            return
        self.terminated = True
        self._set_status("terminated")
        self.notice(
            budget.exhausted_notice(
                exc.message,
                blocked="The conversation has stopped because you have no available credits.",
                hint="You can start a new conversation anytime using /new.",
            ),
            is_error=True,
        )
        # Recoverable once credits are added, so active stays set and a restart re-adopts this
        # same conversation. Retiring it now means a message before that re-adopts, re-hits the
        # 402 and re-reports it, rather than enqueueing into a pump that has already stopped.
        self._wind_down(clear_active=False)

    def _on_session_gone(self) -> None:
        """The cloud session no longer exists. Retire it as RESUMABLE and say so.

        The notice is only queued when the renderer is running, so it survives into the
        force-flush on shutdown. When it is not — tasks never started, or already stopped — the
        rebuild tells the user instead, and a second message would be noise.

        Distinct from :meth:`_give_up_on_credentials`: a rejected PAT is not recoverable from
        inside the bot, whereas a vanished session is — the local transcript can rebuild the
        conversation on a fresh session, which is what marking it lost (rather than merely
        deleted) arranges for.
        """
        if self.terminated:
            return
        self.terminated = True
        self._set_status("terminated")
        auth.mark_conversation_lost(self.db, self.convo_id, self.tg_user_id)

        if self.renderer.running:
            self.notice(
                "That conversation's cloud session is gone, so it has been closed. Its history is "
                "still on this device — you can resume it any time using /sessions and tapping "
                "⟳ RESTORE.",
                is_error=True,
            )

        if self.on_gone:
            self._schedule_detached(self.on_gone(self))

    async def _on_frame(self, frame: Frame) -> None:
        # A user.message coming back. Our own POST created it, and _dispatch_burst already
        # recorded it in the transcript the instant that POST succeeded — which is what matters
        # for resumes when the session later 404s. Recording the echo again put every message in
        # twice, so a rebuilt session saw "what" twice and duly reported the user's own words
        # back to them duplicated. It also re-recorded anything the cursor had not yet moved
        # past when the process died.
        if frame.type == "user.message":
            return
        if self._already_rendered(frame):
            return
        # Append to the local transcript BEFORE rendering. If recording failed after the send
        # we would have a conversation the user saw but could not resume; recording first means
        # the worst case is a transcript entry for a frame that never reached Telegram, which
        # is harmless context.
        try:
            history.record_frame(self.db, self.convo_id, frame)
        except Exception:  # noqa: BLE001 - a transcript must never cost the user their answer
            log.exception("could not record history for convo %s", self.convo_id)
        self.renderer.queue.put_nowait(frame)

    def _already_rendered(self, frame: Frame) -> bool:
        """Whether this event's text has already reached Telegram.

        READ-ONLY, and deliberately so. This used to INSERT the dedupe row right here, on
        receipt. Combined with the stream consumer committing its cursor at the same moment, a
        process kill anywhere before the renderer's next flush lost the answer twice over: the
        cursor sat past the event, and this row then suppressed every replay of it. The row is
        now written by :meth:`_record_rendered`, once delivery is confirmed.

        Delta frames and bot-composed notices carry no durable id and are never deduped, and
        must not be: a notice has no id at all, and a delta is a fragment of an event whose
        completion is what gets recorded.
        """
        event_id = frame.event_id
        if not event_id:
            return False
        return self.db.query_one(
            "SELECT 1 FROM rendered_events WHERE convo_id = ? AND event_id = ?",
            (self.convo_id, event_id),
        ) is not None

    def _record_rendered(self, event_id: str) -> None:
        """Record that an event's text reached Telegram, then release the durable cursor.

        Called by the renderer after a successful write. Both ack trackers are told, because an
        event offered while polling can be acknowledged after a stream consumer has taken over;
        each ignores ids it never saw.
        """
        try:
            self.db.execute(
                "INSERT INTO rendered_events(convo_id, event_id) VALUES(?, ?)",
                (self.convo_id, event_id),
            )
            self._renders_since_prune += 1
            if self._renders_since_prune >= config.RENDER_DEDUPE_PRUNE_EVERY:
                self._renders_since_prune = 0
                self._prune_rendered()
        except sqlite3.IntegrityError:
            pass  # already recorded in an earlier life of this process
        if self._consumer is not None:
            self._consumer.ack(event_id)
        self._poll_acks.ack(event_id)

    def _prune_rendered(self) -> None:
        """Keep the dedupe table bounded. Recent ids are the ones a replay can present."""
        count = self.db.query_one(
            "SELECT COUNT(*) AS n FROM rendered_events WHERE convo_id = ?", (self.convo_id,)
        )
        if not count or count["n"] <= config.RENDER_DEDUPE_KEEP:
            return
        self.db.execute(
            """DELETE FROM rendered_events WHERE convo_id = ? AND seq NOT IN (
                   SELECT seq FROM rendered_events WHERE convo_id = ?
                   ORDER BY seq DESC LIMIT ?
               )""",
            (self.convo_id, self.convo_id, config.RENDER_DEDUPE_KEEP),
        )

    async def _poll_history(self) -> None:
        if not self.session_id:
            return
        cursor = self._get_cursor()
        try:
            async for event in self.api.iter_events(self.session_id, after_id=cursor):
                frame = Frame(
                    kind="event",
                    type=event.get("type", "unknown"),
                    payload=event,
                    sse_id=event.get("id"),
                )
                await self._on_frame(frame)
                if event.get("id"):
                    # Through the same tracker the SSE path uses, so a kill mid-render cannot
                    # strand an answer behind a committed cursor here either.
                    self._poll_acks.offer(event["id"], frame.type)
        except NotFound:
            # A 404 here is ambiguous: this endpoint returns it both for a cursor it no longer
            # recognises and for a session that does not exist. Assuming the former meant a
            # dead session was never detected on the poll path at all — the cursor was cleared,
            # re-fetched, 404ed and cleared again, forever, while the pump told the user the
            # session was missing every few seconds. The stream path resolves this inside
            # StreamConsumer; polling has no such handler, so probe.
            if await self._session_is_gone():
                self._on_session_gone()
                return
            log.info("convo %s cursor rejected by history; resetting", self.convo_id)
            self._set_cursor(None)
        except Unauthorized:
            raise  # the consume loop gives up on this; retrying every 5s cannot help
        except BillingError as exc:
            self._stop_for_billing(exc, "poll")
            return
        except QoderError as exc:
            log.warning("poll failed for convo %s: %s", self.convo_id, exc)
            return

    # --- render loop --------------------------------------------------------------

    async def _render_loop(self) -> None:
        try:
            await self.renderer.run(self.stop)
        except asyncio.CancelledError:
            await self.renderer.force_flush()
            raise
        if self.renderer.fatal and self.on_fatal:
            self._schedule_detached(self.on_fatal(self))

    def _schedule_detached(self, coro) -> None:
        """Hand a retirement callback to the loop rather than awaiting it here.

        The manager's handler calls :meth:`shutdown`, which cancels every task in ``_tasks``
        and then awaits them. Awaiting that from inside one of those tasks makes the task
        cancel and await itself, and asyncio turns the cycle into unbounded recursion inside
        ``Task.cancel`` — which wedges the whole event loop, not just this conversation, so a
        user simply blocking the bot froze the entire process. Every retirement trigger fires
        from within a task, so none of them may await its own shutdown.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            log.warning("no running loop; retirement of convo %s dropped", self.convo_id)
            coro.close()
            return
        task = loop.create_task(coro)
        self._detached.add(task)
        task.add_done_callback(self._detached.discard)

    def _wind_down(self, *, clear_active: bool) -> None:
        """Stop routing to this conversation and hand retirement to the manager.

        The pump and consume loops exit on ``terminated``/``stop``, but the row stayed
        ``active = 1`` and the object stayed in the manager's registry unless something cleared
        them — so the next message still resolved to this corpse, enqueued into a pump that had
        already exited, and was never drained or acknowledged. That silent black hole left a chat
        wedged until the user happened to guess /new.

        ``clear_active`` is set for a session that can never accept messages again (ended or
        archived): the next message then opens a fresh conversation. It is left unset for billing
        and credential failures, which are recoverable — keeping ``active = 1`` is what lets
        reconcile re-adopt the SAME conversation on the next boot once credits or the token are
        fixed, and retiring it here means a message in the meantime re-adopts and re-reports the
        problem rather than vanishing into a dead queue.

        Sets ``stop`` immediately so ``active_for`` and ``live_count`` skip this conversation at
        once, then schedules the manager's retirement as a detached task: awaiting it from inside
        one of our own tasks would be a task cancelling and awaiting itself.
        """
        if clear_active:
            auth.update_conversation(self.db, self.convo_id, self.tg_user_id, active=0)
        self.stop.set()
        self.wake.set()
        if self.on_ended:
            self._schedule_detached(self.on_ended(self))

    # --- hooks --------------------------------------------------------------------

    async def _on_run_started(self) -> None:
        self._set_status("running")

    async def _on_idle(self, stop_reason: dict | None) -> None:
        self._set_status("idle", stop_reason)
        self.renderer.running = False
        # Wake the pump: the queue may hold messages that arrived during the turn.
        self.wake.set()
        if (stop_reason or {}).get("type") == "requires_action":
            self.notice(
                "The agent is waiting for a tool approval. Approvals are disabled for this "
                "bot, so this usually means a tool was denied by policy."
            )

    async def _on_artifact(self, payload: dict) -> None:
        await self.artifacts.deliver(payload)

    async def _on_credits(self, credits: float, is_error: bool, evt_id: str | None = None) -> None:
        recorded = budget.record(
            self.db,
            tg_user_id=self.tg_user_id,
            convo_id=self.convo_id,
            credits=credits,
            model=self.model_id,
            is_error=is_error,
            evt_id=evt_id,
        )
        if not recorded:
            # A replayed span the ledger already holds. Skipping the check as well as the
            # insert matters: re-evaluating the threshold against spend that has not changed
            # could warn the user a second time about the same money.
            return
        user = auth.get_user(self.db, self.tg_user_id)
        if user:
            warning = budget.check_budget(self.db, user)
            if warning:
                self.notice(warning)

    async def _on_error(self, error: dict) -> None:
        self._set_status("error")
        self.wake.set()
        # The renderer only puts an in-stream error on the ephemeral status line, which the
        # following session.status_idle frame overwrites with a generic "stopped (error)" inside
        # the collapsible tool block — so the user never learns WHY the turn stopped until their
        # next message re-hits the same failure. Post the explicit reason as a durable notice the
        # moment it arrives. Skipped when a path that already posted the reason has run: the HTTP
        # 402 (_stop_for_billing) and the rejected-PAT (_give_up_on_credentials) both terminate the
        # conversation and say so themselves, and a second notice would only repeat them.
        if self.terminated or self.credentials_rejected:
            return
        etype = str(error.get("type") or "error")
        message = str(error.get("message") or "unknown error")
        if any(marker in f"{etype} {message}".lower() for marker in BILLING_MARKERS):
            self.notice(
                budget.exhausted_notice(
                    message,
                    blocked="The turn stopped because you have no available credits.",
                    hint="Add credits, then send your message again to continue.",
                ),
                is_error=True,
            )
        else:
            self.notice(
                f"The agent stopped with an error: {etype}: {message}",
                is_error=True,
            )

    async def _on_terminated(self, payload: dict) -> None:
        # Guarded: a session can emit more than one terminated-flavoured event, and without this
        # each would post the notice again and schedule another retirement.
        if self.terminated:
            return
        self.terminated = True
        self._set_status("terminated")
        # This renderer-hook path used to set the flag and say nothing — half of the black hole.
        # The pump exits on ``terminated``, but the user got no word and their next message still
        # routed here. Tell them, and stop routing to a session that can never accept messages.
        self.notice("That conversation has ended and can no longer accept messages. "
                    "Use /new to start another.", is_error=True)
        self._wind_down(clear_active=True)

    # --- workspace staleness ------------------------------------------------------

    def warn_if_workspace_reclaimed(self) -> None:
        """The sandbox filesystem is reclaimed after 24h of inactivity.

        Files the agent made earlier may simply be gone, so say so up front rather than let
        the user watch the agent fail to find its own output.
        """
        row = self.db.query_one(
            "SELECT updated_at, workspace_warned FROM conversations WHERE convo_id = ?",
            (self.convo_id,),
        )
        if not row or row["workspace_warned"]:
            return
        updated = _parse_ts(row["updated_at"])
        if updated is None:
            return
        idle_for = datetime.now(timezone.utc) - updated
        if idle_for < timedelta(hours=config.WORKSPACE_RECLAIM_HOURS):
            return
        self.db.execute(
            "UPDATE conversations SET workspace_warned = 1 WHERE convo_id = ?", (self.convo_id,)
        )
        hours = int(idle_for.total_seconds() // 3600)
        self.notice(
            f"This conversation has been idle for about {hours} hours. Its sandbox filesystem "
            "may have been reclaimed, so files created earlier might be gone. Anything already "
            "sent to you here is safe; ask me to regenerate anything you need again."
        )


async def _sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


# IST has no daylight saving, so a fixed offset is exact — and unlike zoneinfo("Asia/Kolkata") it
# needs no tz database, which a Termux install may not ship.
IST = timezone(timedelta(hours=5, minutes=30))

# Title budget once the marker, the stamp and the longest prefix the switcher adds
# ("▸ ACTIVE — ", 11 characters) are reserved from Telegram's 64-character button cap:
# 11 + 2 + 12 + 3 + 32 = 60, so the stamp always survives the truncation.
TITLE_CHARS = 32


def _ist_stamp(value: str | None) -> str:
    """Last-activity stamp in IST: the time alone today, the date and time on any other day.

    Empty rather than a placeholder when the timestamp is missing or unparseable. The label is
    still useful without it, and one malformed row must not take down /sessions.
    """
    moment = _parse_ts(value)
    if moment is None:
        return ""
    local = moment.astimezone(IST)
    if local.date() == datetime.now(IST).date():
        return local.strftime("%H:%M")
    return local.strftime("%d %b %H:%M")


def describe_row(row: sqlite3.Row) -> str:
    """Short label for the conversation switcher.

    The stamp is LAST ACTIVITY (``updated_at``), not creation. Two conversations with the same
    title — the common case, since titles come from the first message — are told apart by which
    one was used most recently, and ``created_at`` never changes after the fact so it cannot do
    that job. It sits before the title because handlers truncates the whole label to the button
    cap: losing the tail of a long title is a smaller loss than losing the stamp that distinguishes
    it from its twin.
    """
    title = row["title"] or f"conversation {row['convo_id']}"
    marker = "●" if row["active"] else "○"
    stamp = _ist_stamp(row["updated_at"])
    if not stamp:
        return f"{marker} {tg_html.truncate(title, TITLE_CHARS)}"
    return f"{marker} {stamp} · {tg_html.truncate(title, TITLE_CHARS)}"
