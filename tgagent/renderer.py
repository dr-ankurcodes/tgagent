"""The renderer: SSE frames in, Telegram messages out.

This is the module that makes the bot feel like an interactive agent rather than a batch job.

Two invariants hold it together.

**Single writer.** Exactly one task per conversation may send or edit Telegram messages for
that conversation. Commands and notices go through the frame queue; artifact delivery draws
from the same per-chat budget. Without this, two tasks race to edit one message and Telegram
returns "message to edit not found".

**Never lose text.** Every edit sends the *entire* current buffer, not an increment. So a
flood wait, a parse rejection or a dropped connection costs latency, never content. The
buffer is the source of truth; the Telegram message is only a projection of it.

Telegram specifics that shape the code:
* 4096-character cap, measured on the DECODED text, i.e. after our HTML escaping.
* ~1 message/second per chat, and edits share that budget. Rapid repeated edits on one
  message escalate flood waits, so we coalesce on a 1200 ms timer, never on token count.
* HTML parse mode. MarkdownV2 escaping is hostile to half-finished LLM output.
* ``sendChatAction`` expires after 5 seconds, so it must be re-sent while a turn runs.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Protocol

from . import config, tg_html
from .qstream import Frame

log = logging.getLogger("tgagent.render")

# Cap on how long the loop blocks with nothing to do, so `stop` is noticed promptly.
IDLE_POLL_S = 1.0

# Pause after recovering from an unexpected exception, so a sink that fails on every attempt
# cannot spin the loop into a log-flooding busy wait. Each consecutive failure multiplies it.
RECOVERY_PAUSE_S = 1.0
RECOVERY_PAUSE_MAX_S = 30.0


# --- Sink abstraction ---------------------------------------------------------------
# The renderer never imports python-telegram-bot. That keeps the frame machine testable
# offline and means a PTB upgrade cannot silently change rendering behaviour.


class FloodWait(Exception):
    def __init__(self, retry_after: float):
        self.retry_after = retry_after
        super().__init__(f"flood wait {retry_after}s")


class ParseRejected(Exception):
    """Telegram could not parse our entities (HTTP 400 "Can't parse entities")."""


class MessageGone(Exception):
    """The message we were editing was deleted, or is too old to edit."""


class ChatGone(Exception):
    """The chat is unreachable: the user blocked the bot or deleted the chat. Fatal."""


class Sink(Protocol):
    async def send_text(self, chat_id: int, text: str, *, parse_mode: str | None) -> int: ...

    async def edit_text(self, chat_id: int, message_id: int, text: str, *, parse_mode: str | None) -> None: ...

    async def send_typing(self, chat_id: int) -> None: ...

    async def delete_message(self, chat_id: int, message_id: int) -> None: ...


class ChatBudget:
    """One token per second per chat, shared by every task that sends to that chat.

    Telegram's limit is per chat, not per message, so the streaming answer and the
    tool-status message must draw from the same budget or together they will exceed it.
    """

    def __init__(self, interval: float | None = None):
        self.interval = config.TG_SEND_INTERVAL_S if interval is None else interval
        self._next_ok = 0.0
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            wait = self._next_ok - now
            if wait > 0:
                await asyncio.sleep(wait)
                now = time.monotonic()
            self._next_ok = max(now, self._next_ok) + self.interval


class BudgetRegistry:
    def __init__(self) -> None:
        self._budgets: dict[int, ChatBudget] = {}

    def for_chat(self, chat_id: int) -> ChatBudget:
        budget = self._budgets.get(chat_id)
        if budget is None:
            budget = ChatBudget()
            self._budgets[chat_id] = budget
        return budget

    def drop(self, chat_id: int) -> None:
        self._budgets.pop(chat_id, None)


@dataclass
class RenderHooks:
    """Side effects that belong to the conversation, not to rendering.

    The renderer stays the only Telegram writer, but it cannot know what to do when a turn
    ends or an artifact lands.
    """

    on_idle: Callable[[dict | None], Awaitable[None]] | None = None
    on_artifact: Callable[[dict], Awaitable[None]] | None = None
    on_credits: Callable[[float, bool, str | None], Awaitable[None]] | None = None
    on_error: Callable[[dict], Awaitable[None]] | None = None
    on_terminated: Callable[[dict], Awaitable[None]] | None = None
    on_run_started: Callable[[], Awaitable[None]] | None = None
    # Called with a durable event id once that event's text has actually reached Telegram.
    # This is what lets the stream consumer advance its cursor without ever moving past an
    # answer that has not arrived yet. Synchronous, unlike the hooks above: it commits a
    # SQLite write that must not be reorderable against the renders that follow it, and it
    # must not be silently dropped when no loop is running.
    on_rendered: Callable[[str], None] | None = None


LOCAL_NOTICE = "local.notice"
LOCAL_ERROR = "local.error"

# Banner for an agent.thread_context_compacted event. Bot-composed and constant, so it needs no
# escaping. Kept short: it shares a 4096-character budget with the tool list.
COMPACT_LINE = "⟲ context compacted — earlier detail was summarised"

# The only two status lines that say nothing a user needs: they are the default for a turn that is
# merely in progress, or merely finished normally. EVERY other status line — an error, a platform
# retry, an abnormal stop, a pending approval, a terminated session — is the only channel that
# information has, because convo's hooks for those events post no notice of their own. They are
# listed as constants rather than repeated at their use sites because flush_status now branches on
# them, and a typo in one place would silently hide errors again.
STATUS_WORKING = "working…"
STATUS_DONE = "done"
QUIET_STATUS_LINES = frozenset({STATUS_WORKING, STATUS_DONE})


@dataclass
class _ToolLine:
    name: str
    summary: str
    tool_use_id: str | None = None
    state: str = "running"  # running | ok | error


@dataclass
class Renderer:
    chat_id: int
    sink: Sink
    budget: ChatBudget
    hooks: RenderHooks = field(default_factory=RenderHooks)
    show_tools: bool = True
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)

    # --- the answer being streamed into the live message ---
    text_buf: str = ""
    active_event_id: str | None = None
    stream_msg_id: int | None = None
    # Raw text of the current segment already frozen into its own completed message. An
    # answer longer than one Telegram message is split as it streams: each full chunk is
    # written once and never edited again, and text_buf then holds only what is left. This
    # records what has gone, so that when the authoritative buffered agent.message replaces
    # text_buf with the WHOLE answer, retiring the segment queues only the part that has not
    # been delivered yet. Without it the entire answer was sent a second time.
    committed_text: str = ""
    # Completed segments awaiting one authoritative final write, as (message_id, text,
    # event_id). message_id is the message the segment was streamed into, so the final write
    # EDITS it rather than sending a duplicate; it is None for a segment we only ever saw
    # complete (e.g. replayed from history after a reconnect), which must be sent fresh.
    # event_id is the durable id to acknowledge once the write lands, and is None for text
    # that never came from a buffered event.
    #
    # This replaces a `start_new_message` flag, which caused two real bugs: setting it when
    # the buffered agent.message arrived sent the already-streamed answer a second time, and
    # clearing active_event_id there made the NEXT turn's event_start skip its guard and edit
    # the previous answer in place, destroying it.
    pending_final: list[tuple[int | None, str, str | None]] = field(default_factory=list)

    # --- tool activity ---
    status_msg_id: int | None = None
    status_line: str = ""
    tool_lines: list[_ToolLine] = field(default_factory=list)
    folded_tools: int = 0
    tools_hidden: int = 0  # count of tool_use events seen while show_tools was False

    # --- transient turn markers ---
    # True while an agent.thinking phase is open. The API never exposes reasoning content —
    # the event is a marker only — but the marker is still worth rendering, because a turn
    # that reasons for a minute without calling a tool otherwise shows NOTHING: flush_status
    # retracts a status message that has no tool lines. Kept as its own flag rather than a
    # status_line value so that clearing it retracts the message instead of leaving "working…"
    # behind, and so a tool call cannot overwrite it mid-phase.
    thinking: bool = False
    # Set when the platform compacts this session's context. The event's payload is
    # undocumented, so nothing is read from it; the type alone is the signal. Compaction is
    # lossy by design — earlier detail is replaced by a summary — so it is kept on screen for
    # the rest of the turn rather than folded into status_line, which the next tool call or the
    # idle transition would immediately overwrite.
    compact_line: str = ""

    # --- bot-composed messages, kept separate so they never clobber a streamed answer ---
    notices: list[tuple[str, bool]] = field(default_factory=list)

    # --- loop state ---
    running: bool = False
    dirty_text: bool = False
    dirty_status: bool = False
    last_text_ms: float = 0.0
    last_status_ms: float = 0.0
    last_typing_ms: float = 0.0

    # Set when Telegram rejects our entities; from then on this turn sends plain text rather
    # than failing the same way on every edit. Cleared when the next turn starts.
    degraded_html: bool = False
    fatal: bool = False

    _hook_tasks: set = field(default_factory=set)

    # --- inbound ------------------------------------------------------------------

    def post_notice(self, text: str, *, is_error: bool = False) -> None:
        """Queue a bot-composed message. Commands use this instead of sending directly."""
        self.queue.put_nowait(
            Frame(kind="local", type=LOCAL_ERROR if is_error else LOCAL_NOTICE, payload={"text": text})
        )

    def apply(self, frame: Frame) -> None:
        """Pure buffer mutation. Performs no I/O, so the whole frame machine is testable."""
        ftype = frame.type

        if frame.kind == "delta":
            self._apply_delta(frame)
            return

        if ftype in (LOCAL_NOTICE, LOCAL_ERROR):
            text = frame.payload.get("text", "")
            if text:
                self.notices.append((text, ftype == LOCAL_ERROR))
            return

        if ftype == "agent.message":
            # Also cleared here, not only on the event_start delta: a history replay after a
            # reconnect delivers the buffered message with no deltas at all, and an indicator
            # left open by a pre-disconnect thinking phase would otherwise never close.
            self._end_thinking()
            # The buffered event is authoritative: deltas are advisory and may be dropped or
            # duplicated across a reconnect, so replace the buffer rather than append.
            event_id = frame.payload.get("id")
            text = frame.text()
            if event_id == self.active_event_id:
                self.text_buf = text
                # The segment is complete. Retiring it writes the final text into the message
                # it was streamed into, then resets so the next segment opens a new one.
                self._retire_segment(event_id)
            else:
                # A complete message for a segment we were not streaming — a history replay
                # after a reconnect, or a turn whose deltas we never saw.
                self._retire_segment()
                if text.strip():
                    self.pending_final.append((None, text, event_id))
                    self.dirty_text = True
                else:
                    self._ack(event_id)
            return

        if ftype == "agent.tool_use":
            self._on_tool_use(frame)
            return

        if ftype == "agent.tool_result":
            self._on_tool_result(frame)
            return

        if ftype == "agent.artifact_delivered":
            self._spawn_hook(self.hooks.on_artifact, frame.payload)
            return

        if ftype == "span.model_request_end":
            usage = frame.payload.get("model_usage") or {}
            credits = float(usage.get("credits") or 0)
            is_error = bool(frame.payload.get("is_error"))
            # Every span is reported, not only the ones that cost something. Qoder does not
            # bill a failed model call, and `lite` is free, so filtering on a nonzero figure
            # dropped both — which broke budget.py's promise to record failed calls so the
            # ledger reconciles with what the user saw happen, and made /usage under-count
            # model requests.
            self._spawn_hook(self.hooks.on_credits, credits, is_error, frame.event_id)
            return

        if ftype == "session.status_running":
            self.running = True
            # Give HTML another chance each turn. A parse rejection is caused by the markup of
            # one particular answer, so without this reset a single rejection stripped
            # formatting from every later message in the conversation, permanently.
            self.degraded_html = False
            # Each turn gets its OWN status message. Reusing one forever makes it sit above
            # every answer and mutate to show the latest turn's tools, which reads as
            # nonsense when scrolling back. Resetting here leaves a clean history of
            # (activity, answer) pairs, each stamped with what that turn actually cost.
            self.status_msg_id = None
            self.tool_lines = []
            self.folded_tools = 0
            self.tools_hidden = 0
            self.thinking = False
            self.compact_line = ""
            # STATUS_WORKING on its own still never reaches the chat: flush_status retracts a
            # status message whose only content is a filler line, rather than leave it hanging
            # above the answer. The message is no longer strictly tool-gated, though — a thinking
            # phase, a compaction banner, or any informative status line (an error, a retry, an
            # abnormal stop) each justify one of their own. Deliberately no dirty_status here:
            # status_msg_id was just cleared, so it would schedule a flush whose only possible
            # outcome is retracting a message that does not exist.
            self.status_line = STATUS_WORKING
            self._spawn_hook(self.hooks.on_run_started)
            return

        if ftype == "session.status_rescheduled":
            # The platform is auto-retrying a failed model request. Keep the stream open and
            # do NOT resend: resending would run the turn twice.
            # thinking is cleared because _status_text lets it supersede status_line, and a
            # retry is far more worth showing than the reasoning phase it interrupted.
            self.thinking = False
            self.status_line = "platform retrying the model request…"
            self.dirty_status = True
            return

        if ftype == "session.status_idle":
            stop_reason = frame.payload.get("stop_reason")
            self._retire_segment()
            self.running = False
            self.thinking = False
            self.status_line = self._idle_status(stop_reason)
            self.dirty_status = True
            self._spawn_hook(self.hooks.on_idle, stop_reason)
            return

        if ftype in ("session.status_terminated", "session.deleted"):
            self._retire_segment()
            self.running = False
            self.thinking = False
            self.status_line = "session ended"
            self.dirty_status = True
            self._spawn_hook(self.hooks.on_terminated, frame.payload)
            return

        if ftype == "session.error":
            error = frame.payload.get("error") or {}
            self.running = False
            self.thinking = False
            self._retire_segment()
            message = error.get("message") or "unknown error"
            self.status_line = f"⚠ {error.get('type', 'error')}: {tg_html.truncate(message, 180)}"
            self.dirty_status = True
            self._spawn_hook(self.hooks.on_error, error)
            return

        if ftype == "agent.thread_context_compacted":
            # The platform compacted this session's context to stay inside the model's window:
            # earlier detail was replaced by a denser summary. Not an error and not a
            # truncation — the turn continues — but it is lossy, so the user is told.
            #
            # Nothing is read from the payload. The event is listed among the public event
            # types and documented nowhere else: no field schema, no threshold, no semantics.
            # The type alone is the signal, which is also why this is logged — before this
            # branch the event fell through to the catch-all below, so a compacted session was
            # invisible in the chat AND in the logs, and "it forgot what I said earlier" was
            # undiagnosable.
            log.info("chat %s: the platform compacted this session's context", self.chat_id)
            self.compact_line = COMPACT_LINE
            self.dirty_status = True
            return

        # Everything else (span.model_request_start, thread statuses, system messages) is
        # ignored on purpose: it carries nothing the user needs to see.

    def _apply_delta(self, frame: Frame) -> None:
        if frame.type == "event_start":
            started = frame.started_event
            stype, sid = started.get("type"), started.get("id")
            if stype == "agent.message":
                # The answer has begun, so the thinking indicator has done its job. Retracted
                # here rather than left to the idle transition, which can be a minute away.
                self._end_thinking()
                if self.active_event_id != sid:
                    # A new segment is starting. Retire the previous one so its text is
                    # written into its own message and cannot be overwritten by this
                    # segment's deltas.
                    self._retire_segment()
                self.active_event_id = sid
                self.dirty_text = True
            elif stype == "agent.thinking":
                # Start-only: the API sends no event_delta frames for thinking, and the
                # buffered agent.thinking that follows carries no content either. This frame
                # is the only live signal that reasoning is happening.
                #
                # Always enabled — unlike the thinking marker before it was gated on show_tools
                # because it was activity feedback of the same kind as the tool list. Now it's
                # kept visible even when tools are hidden, so users get feedback during any turn.
                self.thinking = True
                self.dirty_status = True
            return

        if frame.type == "event_delta":
            text = frame.delta_text
            if not text:
                return
            event_id = frame.delta_event_id
            if event_id == self.active_event_id:
                self.text_buf += text
                self.dirty_text = True
            # Any other event id is stale: it belongs to a segment we already closed, or
            # arrived after a reconnect. Dropping it is correct, because the buffered event
            # replaces the whole buffer anyway.

    def _on_tool_use(self, frame: Frame) -> None:
        payload = frame.payload
        if self.show_tools:
            self.tool_lines.append(
                _ToolLine(
                    name=payload.get("name") or "tool",
                    summary=summarise_tool_input(payload.get("name") or "", payload.get("input") or {}),
                    tool_use_id=payload.get("id"),
                )
            )
            # Fold older lines so a 200-tool turn costs a handful of edits, not 200.
            if len(self.tool_lines) > config.TOOL_LINES_KEPT:
                overflow = len(self.tool_lines) - config.TOOL_LINES_KEPT
                self.tool_lines = self.tool_lines[overflow:]
                self.folded_tools += overflow
        elif hasattr(payload, "get"):
            # Tools ran this turn but the user hid them with /tools off. Track the count so we can
            # render an italic remark in place of the tool list, keeping the status message alive.
            self.tools_hidden += 1
        # Even with tools hidden, the user should see we're working. The thinking marker
        # (if active) or "working…" keeps them engaged while the answer generates.
        self.status_line = STATUS_WORKING
        self.thinking = False
        self.dirty_status = True

    def _on_tool_result(self, frame: Frame) -> None:
        if not self.show_tools:
            return
        payload = frame.payload
        tool_use_id = payload.get("tool_use_id")
        for line in reversed(self.tool_lines):
            if line.tool_use_id == tool_use_id and line.state == "running":
                is_error = bool(payload.get("is_error"))
                # Billing errors mid-turn come through as tool results with error status. Don't
                # render them as obscure tool failures; let the pump/consumer handle them via the
                # normal billing notice path. Detect by looking for "credit" or HTTP 402 in the
                # line summary (which gets truncated but still matches).
                summary_lower = str(line.summary or "").lower()
                if is_error and ("credit" in summary_lower or "402" in summary_lower or "billing" in summary_lower):
                    # Leave the line state as-is (error ✓ will show); we'll get a proper notice
                    # from the pump/consumer which is clearer. This avoids the "⚠ billing_error"
                    # tool-line noise the user complained about.
                    self.dirty_status = True
                    return
                line.state = "error" if is_error else "ok"
                self.dirty_status = True
                return
        # A result for a line we already folded away: nothing to update.

    def _end_thinking(self) -> None:
        """Close the thinking phase and ask for a re-render.

        The re-render is the point: with no tool lines and no compaction banner, flush_status
        now finds nothing worth a message of its own and deletes the indicator. Doing it
        through the flag rather than by writing a status_line is what makes the message go away
        instead of sitting above the answer as a stale "thinking…".
        """
        if not self.thinking:
            return
        self.thinking = False
        self.dirty_status = True

    def _retire_segment(self, event_id: str | None = None) -> None:
        """Close out the live segment.

        Its undelivered text is queued for one final write into the message it was streaming
        into, and the live-message id is cleared so the next segment cannot overwrite it. This
        is what keeps successive answers in separate, intact messages.

        ``event_id`` is the durable id to acknowledge once that write lands. It defaults to the
        segment being streamed, which is what every caller other than a completed
        ``agent.message`` wants.
        """
        ack_id = event_id if event_id is not None else self.active_event_id
        remainder = self._uncommitted(self.text_buf)
        if remainder.strip():
            self.pending_final.append((self.stream_msg_id, remainder, ack_id))
        else:
            # Nothing worth a message, but the event still has to be acknowledged: an unacked
            # id stalls the durable cursor behind it forever, and every reconnect would then
            # re-walk the whole session history.
            self._ack(ack_id)
        self.text_buf = ""
        self.committed_text = ""
        self.stream_msg_id = None
        self.active_event_id = None
        self.dirty_text = bool(self.pending_final)

    def _ack(self, event_id: str | None) -> None:
        """Report that an event's content has reached Telegram.

        This is the only signal that lets the stream consumer commit its cursor, so it is sent
        after the write succeeds and never before.
        """
        if event_id and self.hooks.on_rendered:
            try:
                self.hooks.on_rendered(event_id)
            except Exception:  # noqa: BLE001 - a failed ack must not break rendering
                log.exception("on_rendered failed for event %s", event_id)

    def _uncommitted(self, text: str) -> str:
        """The part of ``text`` that has not already been written to a completed message.

        ``text`` is normally the whole authoritative answer, while ``committed_text`` is the
        prefix already frozen into its own messages during streaming. Slicing by length is
        correct even when the deltas diverged from the buffered event: what has already been
        sent cannot be recalled, so the best available answer is to send everything past that
        point and no more.
        """
        if not self.committed_text:
            return text
        return text[len(self.committed_text):]

    def _idle_status(self, stop_reason: dict | None) -> str:
        kind = (stop_reason or {}).get("type")
        if kind == "requires_action":
            return "waiting for approval"
        if kind == "end_turn":
            return STATUS_DONE
        if kind:
            # An abnormal stop — retries_exhausted, interrupted, max_turns. Unlike "done" this is
            # worth a message of its own; see status_worth_a_message.
            return f"stopped ({kind})"
        return STATUS_DONE

    @property
    def status_worth_a_message(self) -> bool:
        """Whether ``status_line`` alone justifies sending, with no tool list behind it.

        Inverted on purpose: the QUIET set names the two lines that are pure filler, so anything
        new defaults to being shown. The opposite default is what hid errors for so long — an
        error on a turn that called no tools was retracted as "nothing to show", and since
        ``convo._on_error`` posts no notice, the user never learned their message had failed.
        """
        return bool(self.status_line) and self.status_line not in QUIET_STATUS_LINES

    def _spawn_hook(self, hook: Callable | None, *args) -> None:
        """Run a hook without blocking the render loop.

        References are kept so the task is not garbage collected mid-flight, and failures are
        logged rather than swallowed — a silently dropped on_idle would stall the conversation.
        """
        if hook is None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            log.warning("no running loop; hook %s dropped", getattr(hook, "__name__", hook))
            return
        task = loop.create_task(_run_hook(hook, args))
        self._hook_tasks.add(task)
        task.add_done_callback(self._hook_tasks.discard)

    # --- outbound -----------------------------------------------------------------

    def _status_text(self) -> str:
        # In degraded mode parse_mode is None, so every tag would reach the user as literal
        # text. _should_spoiler already suppresses the spoiler for this reason; the emphasis
        # tags need the same treatment.
        def emph(text: str) -> str:
            return text if self.degraded_html else f"<b>{text}</b>"

        def soft(text: str) -> str:
            return text if self.degraded_html else f"<i>{text}</i>"

        parts: list[str] = []
        if self.compact_line:
            # First, so it reads as a banner about the conversation rather than getting lost
            # underneath the tool list for this turn.
            parts.append(soft(self.compact_line))
        if self.tools_hidden:
            # Tools ran this turn but the user hid them with /tools off. Say so instead of
            # rendering nothing: a silent turn looks like a hung bot, and the remark is also
            # what tells them how to get the detail back. This remark justifies the status message
            # when show_tools is False, so thinking…/working… still reach the chat.
            parts.append(soft("Tool activity hidden — /tools to enable"))
        if self.folded_tools:
            parts.append(soft(f"+{self.folded_tools} earlier steps"))
        if self.tool_lines:
            parts.append(emph("🔧 Tools"))
        for line in self.tool_lines:
            mark = {"running": "⚙", "ok": "✓", "error": "✗"}[line.state]
            # Bot-composed from untrusted tool input: scrub internal ids, escape, then wrap.
            # Escaping must happen BEFORE wrapping, or the spoiler's own angle brackets get
            # escaped and Telegram shows the tag as literal text.
            detail = tg_html.escape(tg_html.scrub_ids(line.summary))
            if self._should_spoiler(line.summary):
                detail = f"{config.SPOILER_OPEN}{detail}{config.SPOILER_CLOSE}"
            parts.append(
                f"{mark} {emph(tg_html.escape(tg_html.truncate(line.name, 40)))} {detail}".rstrip()
            )
        if self.thinking:
            # Supersedes status_line rather than joining it: "working…" says nothing, and this
            # is the only feedback a tool-free reasoning turn gets until the answer lands.
            parts.append(soft("thinking…"))
        elif self.status_line:
            parts.append(tg_html.escape(self.status_line))
        return "\n".join(p for p in parts if p.strip())

    def _should_spoiler(self, summary: str) -> bool:
        """Collapse a tool's argument detail when it is long enough to be worth a tap.

        Short arguments stay inline: hiding "/data/x.png" behind a spoiler would mean tapping
        to read something that cost one line. And in degraded plain-text mode the markup would
        render literally, so it is skipped entirely.
        """
        if self.degraded_html or not summary:
            return False
        return len(summary) >= config.TOOL_SPOILER_MIN_CHARS

    @property
    def parse_mode(self) -> str | None:
        return None if self.degraded_html else "HTML"

    @property
    def body_limit(self) -> int:
        """Characters available for answer text: Telegram's full 4096 cap."""
        return config.TG_MESSAGE_LIMIT

    async def flush_notices(self) -> None:
        """Send bot-composed messages. These always start a new message.

        A notice is popped only AFTER its send succeeds. Popping first meant that any
        exception outside the four this module models — tgsink deliberately re-raises
        NetworkError and TimedOut — unwound the frame with the notice held only in a local,
        losing it for good. flush_text guards pending_final the same way.
        """
        while self.notices:
            text, is_error = self.notices[0]
            raw = f"{'⚠ ' if is_error else ''}{text}"
            # A notice can carry an arbitrary API error string. Telegram rejects an over-cap
            # message whole rather than truncating it, so it would be lost rather than cut.
            if tg_html.display_length(raw) > self.body_limit:
                head, _ = tg_html.split_message(raw, max(1, self.body_limit - 1))
                raw = f"{head}…"
            body = tg_html.render(raw)
            await self.budget.acquire()
            try:
                await self.sink.send_text(self.chat_id, body, parse_mode=self.parse_mode)
            except FloodWait as exc:
                await asyncio.sleep(exc.retry_after + 0.1)
                return
            except ParseRejected:
                self.degraded_html = True
                return
            except MessageGone:
                self.notices.pop(0)
                continue
            except ChatGone:
                self.fatal = True
                return
            self.notices.pop(0)

    async def flush_status(self) -> None:
        text = self._status_text()
        # Backstop for the cap. Every field feeding this string is individually bounded, but
        # those bounds live in config and could be raised independently, and an over-cap
        # status is rejected whole — losing the entire tool display rather than part of it.
        #
        # decoded_length, not utf16_length: this string is mostly markup that Telegram
        # discards (<b>, <i>, and a 30-character spoiler span per line), so charging for the
        # tags folded tool lines away long before the real limit was anywhere near.
        #
        # The loop folds down to ZERO lines, not one. Stopping at a single line left an
        # over-cap status in place whenever that one line could not fit, which is exactly the
        # rejection this backstop exists to prevent; with no lines left the retraction below
        # fires instead, and a missing tool display beats a rejected one.
        while tg_html.decoded_length(text) > self.body_limit and self.tool_lines:
            self.tool_lines.pop(0)
            self.folded_tools += 1
            text = self._status_text()
        # Nothing worth a message of its own. A filler status_line ("working…", "done") with no
        # tools behind it is retracted rather than left hanging above the answer — but a thinking
        # phase, a compaction banner, a tools-hidden remark or an INFORMATIVE status line is the
        # content, so each of those keeps the message alive on its own. This gate is why "working…"
        # never reached the chat, and it is also what used to swallow errors on any turn that called
        # no tools. The tools-hidden remark now justifies the message when show_tools is False.
        if not text or (
            not self.tool_lines
            and not self.thinking
            and not self.compact_line
            and not self.tools_hidden
            and not self.status_worth_a_message
        ):
            if self.status_msg_id is not None:
                await self._safe_delete(self.status_msg_id)
                self.status_msg_id = None
            self.dirty_status = False
            return

        await self.budget.acquire()
        try:
            if self.status_msg_id is None:
                self.status_msg_id = await self.sink.send_text(self.chat_id, text, parse_mode=self.parse_mode)
            else:
                await self.sink.edit_text(self.chat_id, self.status_msg_id, text, parse_mode=self.parse_mode)
            self.dirty_status = False
            self.last_status_ms = time.monotonic()
        except FloodWait as exc:
            # Leave dirty_status set; the loop retries on a later tick.
            await asyncio.sleep(exc.retry_after + 0.1)
        except ParseRejected:
            self.degraded_html = True
            self.dirty_status = True
        except MessageGone:
            self.status_msg_id = None
            self.dirty_status = True
        except ChatGone:
            self.fatal = True

    async def flush_text(self) -> None:
        """Drain completed segments, then update the live streaming preview.

        Completed segments go first so message order stays chronological: an answer that
        finished must appear above the one still being generated.
        """
        while self.pending_final:
            msg_id, text, ack_id = self.pending_final[0]
            if not await self._write_segment(text, msg_id):
                # Leave the queue intact; a later tick retries. Nothing is dropped.
                self.last_text_ms = time.monotonic()
                return
            self.pending_final.pop(0)
            # Acknowledged only now that the text is on Telegram. Acking earlier is what let a
            # process kill lose an answer for good: the cursor moved past the event, and the
            # durable dedupe then suppressed every replay of it.
            self._ack(ack_id)
            self.last_text_ms = time.monotonic()

        raw = self.text_buf
        if not raw.strip():
            self.dirty_text = False
            return

        if tg_html.display_length(raw) > self.body_limit:
            head, tail = tg_html.split_message(raw, self.body_limit)
            if not head and tail:
                # Pathological: a single character escapes past the cap. Force one through
                # rather than re-splitting to the same empty head on every tick forever.
                head, tail = tail[:1], tail[1:]
            if head.strip():
                if await self._write_live(tg_html.render(head)):
                    # That message is now full and complete: it will never be edited again, so
                    # remember it as delivered and let the tail open a fresh one.
                    self.committed_text += head
                    self.stream_msg_id = None
                    self.text_buf = tail
                    self.dirty_text = bool(tail.strip())
                # On failure the buffer is untouched, so a retry re-splits identically.
            elif head:
                # A whitespace-only head is not worth a message, but it still has to be
                # consumed: leaving the buffer untouched would spin on the same split. It is
                # added to committed_text so that committed_text + text_buf still reconstructs
                # the whole answer when the authoritative buffered message arrives.
                self.committed_text += head
                self.text_buf = tail
                self.dirty_text = bool(tail.strip())
            self.last_text_ms = time.monotonic()
            return

        self.dirty_text = not await self._write_live(tg_html.render(raw))
        self.last_text_ms = time.monotonic()

    async def _write_segment(self, text: str, msg_id: int | None) -> bool:
        """Write a COMPLETED segment. Spills into extra messages if it exceeds the cap."""
        chunks = tg_html.split_all(text, self.body_limit) or [""]
        target = msg_id
        for chunk in chunks:
            ok, _ = await self._write_raw(tg_html.render(chunk), target)
            if not ok:
                return False
            # Only the first chunk belongs in the message this segment streamed into;
            # any overflow has to be new messages.
            target = None
        return True

    async def _write_live(self, text: str) -> bool:
        """Write the in-progress segment, editing its live message in place."""
        ok, message_id = await self._write_raw(text, self.stream_msg_id)
        if ok:
            self.stream_msg_id = message_id
        return ok

    async def _write_raw(self, text: str, msg_id: int | None) -> tuple[bool, int | None]:
        """One Telegram write. Returns (succeeded, message id now holding this text).

        Callers must check the boolean: False means the text never landed and must be retried,
        otherwise it is silently dropped.
        """
        await self.budget.acquire()
        try:
            if msg_id is None:
                return True, await self.sink.send_text(self.chat_id, text, parse_mode=self.parse_mode)
            await self.sink.edit_text(self.chat_id, msg_id, text, parse_mode=self.parse_mode)
            return True, msg_id
        except FloodWait as exc:
            # Sleep it out. Every write carries the full text, so waiting costs latency only.
            log.debug("flood wait %.1fs on chat %s", exc.retry_after, self.chat_id)
            await asyncio.sleep(exc.retry_after + 0.1)
            return False, msg_id
        except ParseRejected:
            self.degraded_html = True
            return False, msg_id
        except MessageGone:
            if msg_id is None:
                return False, None
            # The user deleted the message we were editing. Send it fresh rather than lose it.
            log.debug("message %s is gone; resending as a new message", msg_id)
            await self.budget.acquire()
            try:
                return True, await self.sink.send_text(self.chat_id, text, parse_mode=self.parse_mode)
            except ChatGone:
                self.fatal = True
                return False, None
            except (FloodWait, ParseRejected, MessageGone) as exc:
                log.debug("resend also failed: %s", exc)
                return False, None
        except ChatGone:
            self.fatal = True
            return False, msg_id

    async def _safe_delete(self, message_id: int) -> None:
        try:
            await self.budget.acquire()
            await self.sink.delete_message(self.chat_id, message_id)
        except (MessageGone, ChatGone, ParseRejected, FloodWait) as exc:
            log.debug("could not delete message %s: %s", message_id, exc)

    async def maybe_typing(self) -> None:
        if not self.running:
            return
        now = time.monotonic()
        if now - self.last_typing_ms < config.TYPING_REPEAT_S:
            return
        self.last_typing_ms = now
        try:
            await self.sink.send_typing(self.chat_id)
        except ChatGone:
            self.fatal = True
        except Exception as exc:  # noqa: BLE001 - a typing indicator is never worth crashing for
            log.debug("typing indicator failed: %s", exc)

    # --- main loop ----------------------------------------------------------------

    async def run(self, stop: asyncio.Event) -> None:
        """Consume frames and flush on a timer until stopped."""
        failures = 0
        while not stop.is_set() and not self.fatal:
            try:
                await self._tick()
                await asyncio.sleep(0)
                timeout = self._next_timeout()
                try:
                    frame = await asyncio.wait_for(self.queue.get(), timeout=timeout)
                except asyncio.TimeoutError:
                    frame = None
                if frame is not None:
                    self.apply(frame)
                failures = 0
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - don't silently kill rendering
                failures += 1
                log.exception(
                    "unexpected error in render loop; recovering (%d consecutive)", failures
                )
                # NOTHING buffered is discarded here. pending_final, notices and text_buf all
                # hold text that has not reached Telegram yet, and this module's central
                # invariant is that a failure costs latency, never content. Clearing them is
                # what silently ate the tail of an answer whenever a transient error escaped
                # the sink's four mapped exceptions — tgsink deliberately re-raises
                # NetworkError and TimedOut, so a mobile-data blip mid-answer landed here.
                #
                # Markup is NOT degraded here either. ParseRejected is the real signal that
                # Telegram cannot parse our entities and is handled where it is raised; an
                # unexpected exception is far more often a network blip, and degrading for one
                # would strip formatting for the rest of the conversation's life.
                #
                # The pause escalates so a sink failing on every attempt cannot spin this loop
                # into a log flood.
                await _sleep_or_stop(
                    stop, min(RECOVERY_PAUSE_S * failures, RECOVERY_PAUSE_MAX_S)
                )

        await self.force_flush()

    def _next_timeout(self) -> float:
        """How long we can sleep before something is due.

        Bounded by IDLE_POLL_S so a stop request is noticed promptly even when idle.
        """
        now = time.monotonic()
        due: list[float] = []
        if self.dirty_text:
            due.append(max(0.0, (self.last_text_ms + config.THROTTLE_MS / 1000) - now))
        if self.dirty_status:
            due.append(max(0.0, (self.last_status_ms + config.STATUS_THROTTLE_MS / 1000) - now))
        if self.notices:
            due.append(0.0)
        if self.running:
            due.append(max(0.0, (self.last_typing_ms + config.TYPING_REPEAT_S) - now))
        return min(due) if due else IDLE_POLL_S

    async def _tick(self) -> None:
        """Flush whatever is due. Ordering matters: notices, then status, then the answer."""
        now = time.monotonic()
        if self.notices:
            await self.flush_notices()
        if self.dirty_status and (now - self.last_status_ms) * 1000 >= config.STATUS_THROTTLE_MS:
            await self.flush_status()
        if self.dirty_text and (now - self.last_text_ms) * 1000 >= config.THROTTLE_MS:
            await self.flush_text()
        await self.maybe_typing()

    async def force_flush(self) -> None:
        """Write out whatever is still unwritten, ignoring throttles.

        Guarded by the dirty flags: they track precisely what has not reached Telegram yet, so
        this never re-edits a message with text it already has.
        """
        if self.fatal:
            return
        try:
            if self.notices:
                await self.flush_notices()
            # A thinking indicator must not outlive teardown: the phase it described ends when
            # the stream closes, and unlike the tool list it is not a record of anything that
            # happened. Clearing it through the helper is what makes the flush below retract
            # the message rather than freeze "thinking…" above an answer that never came.
            self._end_thinking()
            if self.dirty_status and (
                self.status_line or self.tool_lines or self.compact_line or self.tools_hidden or self.status_msg_id
            ):
                await self.flush_status()

            # flush_text already handles completed segments, the live preview and over-cap
            # splitting, so reuse it rather than duplicating that logic here. Loop because a
            # single call emits at most one message per over-cap chunk. Termination comes from
            # the no-progress check, not from a fixed iteration count: a capped loop silently
            # dropped the tail of any answer long enough to need more chunks than the cap.
            while self.pending_final or self.text_buf.strip():
                before = (len(self.pending_final), len(self.text_buf))
                await self.flush_text()
                if (len(self.pending_final), len(self.text_buf)) == before:
                    break  # no progress: a write is failing, so stop rather than spin
            self.dirty_text = bool(self.pending_final or self.text_buf.strip())
        except Exception:  # noqa: BLE001 - shutdown must not raise into the caller
            log.exception("force_flush failed for chat %s", self.chat_id)

    async def wait_for_hooks(self, timeout: float = 10.0) -> None:
        """Let in-flight hooks finish. Called on shutdown so no side effect is lost."""
        if not self._hook_tasks:
            return
        await asyncio.wait(set(self._hook_tasks), timeout=timeout)


async def _sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    """Sleep, but return as soon as ``stop`` is set so shutdown is never delayed."""
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass


async def _run_hook(hook: Callable, args: tuple) -> None:
    name = getattr(hook, "__name__", repr(hook))
    try:
        await hook(*args)
    except Exception:  # noqa: BLE001 - one bad hook must not kill the render loop
        log.exception("render hook %s failed", name)


def summarise_tool_input(name: str, raw: dict) -> str:
    """One short line describing a tool call, for the status message."""
    if not isinstance(raw, dict):
        return tg_html.truncate(str(raw), config.TOOL_SUMMARY_CHARS)

    for key in ("command", "file_path", "pattern", "path", "query", "url", "prompt", "description"):
        value = raw.get(key)
        if isinstance(value, str) and value.strip():
            return tg_html.truncate(value.strip().splitlines()[0], config.TOOL_SUMMARY_CHARS)

    if name == "DeliverArtifacts":
        names = [
            f.get("name") or f.get("path")
            for f in (raw.get("files") or [])
            if isinstance(f, dict)
        ]
        names = [str(n) for n in names if n]
        if names:
            return tg_html.truncate(", ".join(names), config.TOOL_SUMMARY_CHARS)

    if not raw:
        return ""
    return tg_html.truncate(", ".join(sorted(raw.keys())), config.TOOL_SUMMARY_CHARS)
