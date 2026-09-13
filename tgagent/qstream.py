"""SSE parsing and resilient stream consumption.

The stream is the only source of live output, and on a phone it will break constantly:
mobile data drops, Android dozes, the process is killed. So reconnection is not an error
path, it is the normal operating mode.

Two rules from Phase 0 shape this module:

* ``Last-Event-ID`` resume returns 200 and replays everything after that id, but an unknown
  or malformed id returns **404** (not 400). So a 404 means "your cursor is stale" and the
  recovery is to rebuild from ``GET /events`` history.
* Delta frames never appear in history. Only buffered events have a durable id, and even for
  those the cursor advances only once the renderer confirms the text reached Telegram — see
  :class:`AckTracker`. Committing it on receipt is what made a process kill lose an answer for
  good: the cursor sat past the event and the durable dedupe suppressed every replay of it.
"""

from __future__ import annotations

import asyncio
import collections
import json
import logging
import time
from dataclasses import dataclass, field
from typing import AsyncIterator, Awaitable, Callable

import httpx

from .qclient import NotFound, QoderClient, QoderError, iter_session_events

log = logging.getLogger("tgagent.stream")

DELTA_TYPES = frozenset({"event_start", "event_delta"})
RECONNECT_BACKOFF = (1.0, 2.0, 5.0, 10.0, 15.0)

# How long a connection must stay up before it counts as a success and resets the backoff. The
# server heartbeats roughly every 15s, so this means at least one heartbeat was seen. Resetting
# on connect alone let a proxy that accepts and immediately drops the stream pin the reconnect
# delay at its first step forever.
STABLE_STREAM_S = 30.0

# Events whose content the user would notice losing, so the durable cursor must not move past
# them until the renderer confirms it reached Telegram. Everything else feeds the ephemeral
# status message or a durable side table of its own, and is acked on sight.
DEFERRED_ACK_TYPES = frozenset({"agent.message"})

# How many offered-but-unacknowledged events to tolerate before force-advancing the cursor.
# Generous enough that a slow chat or a long flood wait never trips it, small enough that a
# renderer which has stopped confirming writes cannot grow the list without limit.
PENDING_ACK_MAX = 500

# Acknowledgements are remembered so a late offer can match them, but one that matches no
# pending offer can never advance the cursor, so there is nothing to lose by dropping them.
ACKED_SET_MAX = 2000


@dataclass
class Frame:
    """One parsed SSE frame.

    ``kind`` is ``"delta"`` for the incremental protocol (event_start / event_delta) and
    ``"event"`` for a complete buffered event. Only buffered events carry a durable ``id``.
    """

    kind: str
    type: str
    payload: dict = field(default_factory=dict)
    sse_id: str | None = None

    @property
    def event_id(self) -> str | None:
        """Durable cursor value, or None for delta frames."""
        return self.payload.get("id") if self.kind == "event" else None

    @property
    def delta_event_id(self) -> str | None:
        """For event_delta frames, the id of the event being built."""
        return self.payload.get("event_id")

    @property
    def delta_text(self) -> str:
        delta = self.payload.get("delta") or {}
        if delta.get("type") != "content_delta":
            return ""
        content = delta.get("content") or {}
        return content.get("text") or ""

    @property
    def started_event(self) -> dict:
        """For event_start frames: {"id": ..., "type": "agent.message"}."""
        return self.payload.get("event") or {}

    def text(self) -> str:
        """Concatenated text content of a buffered message-like event."""
        parts = []
        for block in self.payload.get("content") or []:
            if isinstance(block, dict) and block.get("type") == "text":
                parts.append(block.get("text") or "")
        return "".join(parts)


async def iter_sse(response: httpx.Response) -> AsyncIterator[Frame]:
    """Parse an SSE byte stream into Frames.

    Handles multi-line data fields, comment heartbeats, and both ``id:`` and ``event:``
    fields. A blank line dispatches the accumulated event.
    """
    sse_id: str | None = None
    sse_event: str | None = None
    data_lines: list[str] = []

    async for raw in response.aiter_lines():
        line = raw.rstrip("\r")

        if line == "":
            if data_lines or sse_event or sse_id:
                frame = _build_frame(sse_id, sse_event, "\n".join(data_lines))
                if frame is not None:
                    yield frame
            sse_id, sse_event, data_lines = None, None, []
            continue

        if line.startswith(":"):
            # Heartbeat comment, roughly every 15s. Reading it is what keeps the socket alive.
            continue

        field_name, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]

        if field_name == "data":
            data_lines.append(value)
        elif field_name == "id":
            sse_id = value
        elif field_name == "event":
            sse_event = value
        # Any other field (retry:, etc.) is ignored per the SSE spec.

    # Stream closed without a trailing blank line: flush whatever is pending.
    if data_lines:
        frame = _build_frame(sse_id, sse_event, "\n".join(data_lines))
        if frame is not None:
            yield frame


def _build_frame(sse_id: str | None, sse_event: str | None, data: str) -> Frame | None:
    if not data:
        return None
    try:
        payload = json.loads(data)
    except json.JSONDecodeError:
        log.debug("skipping unparseable SSE data: %.200s", data)
        return None
    if not isinstance(payload, dict):
        return None

    ptype = payload.get("type") or sse_event or "unknown"
    kind = "delta" if ptype in DELTA_TYPES else "event"
    return Frame(kind=kind, type=ptype, payload=payload, sse_id=sse_id or payload.get("id"))


FrameHandler = Callable[[Frame], Awaitable[None]]
CursorGetter = Callable[[], str | None]
CursorSetter = Callable[[str], None]


@dataclass
class AckTracker:
    """How far the durable cursor may advance, given what has actually reached Telegram.

    The cursor moves only over a CONTIGUOUS prefix of acknowledged events. Committing it past
    an event whose text is still buffered in the renderer is what made a process kill lose an
    answer permanently: the cursor then sat past the event, and the durable dedupe row written
    alongside it suppressed every replay. Losing text is unrecoverable; re-delivering a
    duplicate is merely untidy.

    Shared by the SSE consumer and the poll fallback so both obey the same rule.

    Both containers are bounded, because an unacknowledged head is not merely a memory leak:
    it freezes the cursor, and a frozen cursor makes every reconnect re-walk the whole session
    history, re-offering every event and growing the pending list further. See
    :meth:`_force_advance` for what happens when that state persists.
    """

    commit: Callable[[str], None]
    deferred_types: frozenset = DEFERRED_ACK_TYPES
    _pending: list = field(default_factory=list)
    _acked: set = field(default_factory=set)
    _forced_total: int = 0

    def offer(self, event_id: str, event_type: str) -> None:
        """Record an event handed to the renderer. Types nobody would notice losing are acked
        at once; an ``agent.message`` waits for the renderer to confirm delivery.

        An id already in ``_acked`` counts as delivered immediately: the poll fallback and the
        stream consumer share a conversation but not a tracker, so an acknowledgement can
        legitimately arrive before the matching offer. Ignoring that ordering left the id at
        the head of ``_pending`` with nothing left to re-trigger the prefix walk.
        """
        self._pending.append(event_id)
        if len(self._pending) > PENDING_ACK_MAX:
            self._force_advance()
        if event_type not in self.deferred_types or event_id in self._acked:
            self.ack(event_id)

    def ack(self, event_id: str | None) -> None:
        """Report that an event's content reached Telegram. Idempotent, and safe to call with
        an id never offered — which happens when a conversation is adopted mid-stream."""
        if not event_id:
            return
        self._acked.add(event_id)
        advanced: str | None = None
        while self._pending and self._pending[0] in self._acked:
            advanced = self._pending.pop(0)
            self._acked.discard(advanced)
        if advanced is not None:
            self.commit(advanced)
        if len(self._acked) > ACKED_SET_MAX:
            self._forget_unmatched_acks()

    def _force_advance(self) -> None:
        """Give up on the oldest unacknowledged events so the cursor can move again.

        Reaching this cap means the renderer has stopped confirming writes altogether — the
        conversation is already broken, and the choice is between an unbounded leak plus a
        stream that can never catch up, and a possible duplicate delivery. This module already
        prefers the duplicate: losing text is unrecoverable, re-delivering it is untidy.

        Only the FIRST occurrence is logged at ERROR. Once the pending list sits at the cap
        every subsequent offer lands here, so repeating the error would churn the rotating log
        for the rest of the process's life — the same failure mode the credential branches in
        this package are written to avoid.
        """
        overflow = len(self._pending) - PENDING_ACK_MAX
        stale = self._pending[:overflow]
        del self._pending[:overflow]
        for event_id in stale:
            self._acked.discard(event_id)
        self._forced_total += len(stale)
        log.log(
            logging.ERROR if self._forced_total == len(stale) else logging.DEBUG,
            "advancing the durable cursor past %d event(s) that were never confirmed as "
            "delivered (%d total). The renderer is not acknowledging writes, so a replay may "
            "deliver these twice.",
            len(stale), self._forced_total,
        )
        self.commit(stale[-1])

    def _forget_unmatched_acks(self) -> None:
        """Drop acknowledgements for ids this tracker never offered.

        They are remembered only so a late offer can match them, and they can never advance
        the cursor on their own, so there is no reason to keep them once there are too many.
        """
        self._acked.intersection_update(self._pending)


@dataclass
class StreamConsumer:
    """Owns the read side of one session's event stream, including reconnection.

    The cursor lives outside this class (in SQLite) because it must survive the process. We
    read it fresh on every reconnect so a value committed elsewhere is honoured.
    """

    client: QoderClient
    session_id: str
    get_cursor: CursorGetter
    set_cursor: CursorSetter
    on_frame: FrameHandler
    deltas: bool = True
    # Set when run() stopped for a reason that retrying cannot fix. The caller must check
    # this: run() returns normally both here and on a clean stop, and treating an auth
    # failure as "reconnect" spins one doomed request per iteration forever.
    gave_up: bool = False
    # Set when the session itself is gone, as opposed to the credentials being rejected. The
    # caller owes the user a different sentence for each, and a different local state change:
    # a gone session is resumable from the local transcript, a dead PAT is not.
    session_gone: bool = False
    _seen: collections.deque = field(default_factory=lambda: collections.deque(maxlen=4000))
    _seen_set: set = field(default_factory=set)
    _acks: AckTracker | None = None

    def __post_init__(self) -> None:
        self._acks = AckTracker(commit=self.set_cursor)

    def _already_seen(self, event_id: str) -> bool:
        if event_id in self._seen_set:
            return True
        if len(self._seen) == self._seen.maxlen:
            evicted = self._seen[0]
            self._seen_set.discard(evicted)
        self._seen.append(event_id)
        self._seen_set.add(event_id)
        return False

    async def _emit(self, frame: Frame, stop: asyncio.Event) -> bool:
        """Hand a frame to the renderer. Returns False if it was a duplicate or we are stopping."""
        if stop.is_set():
            return False
        event_id = frame.event_id
        if event_id and self._already_seen(event_id):
            return False
        if event_id:
            self._acks.offer(event_id, frame.type)
        await self.on_frame(frame)
        return True

    def ack(self, event_id: str | None) -> None:
        """Report that an event's content has reached Telegram, and advance the cursor."""
        self._acks.ack(event_id)

    async def _rebuild_from_history(self, stop: asyncio.Event) -> int:
        """Replay buffered events after the cursor. Returns how many were NEWLY delivered.

        Events ``_emit`` discards as duplicates are not counted. The caller uses this to tell
        "history is reachable" from "we are re-walking the same ground", and counting
        duplicates would make a second rebuild look productive and reconnect without backoff.
        """
        replayed = 0
        async for event in iter_session_events(self.client, self.session_id, self.get_cursor()):
            if stop.is_set():
                break
            frame = Frame(kind="event", type=event.get("type", "unknown"), payload=event,
                          sse_id=event.get("id"))
            if await self._emit(frame, stop):
                replayed += 1
        return replayed

    async def run(self, stop: asyncio.Event) -> None:
        """Consume until ``stop`` is set. Never raises for expected transport problems.

        Returns normally in two very different situations, which the caller must distinguish
        via ``gave_up``: it was asked to stop, or it gave up because retrying cannot help.
        """
        attempt = 0
        while not stop.is_set():
            cursor = self.get_cursor()
            connected_at: float | None = None
            try:
                async with self.client.stream_events(
                    self.session_id, deltas=self.deltas, last_event_id=cursor
                ) as response:
                    connected_at = time.monotonic()
                    async for frame in iter_sse(response):
                        if stop.is_set():
                            return
                        await self._emit(frame, stop)
                # Server closed the stream cleanly. Reconnect to keep watching.
                log.debug("stream %s closed by server; reconnecting", self.session_id)

            except NotFound as exc:
                # Stale or unknown cursor. Rebuild from history, which also repairs the
                # cursor, then retry. If that still 404s, drop the cursor and rely on
                # dedupe so we can never loop forever on a poisoned value.
                attempt += 1
                log.info("stream cursor rejected for %s (%s); rebuilding from history",
                         self.session_id, exc.message)
                emitted = 0
                try:
                    emitted = await self._rebuild_from_history(stop)
                    log.info("rebuilt %d events for %s", emitted, self.session_id)
                except NotFound as gone:
                    # The history endpoint 404s for the SESSION itself, not only for a bad
                    # cursor. This was swallowed by the generic handler below, which left the
                    # loop clearing the cursor, reconnecting, 404ing and rebuilding forever.
                    log.error("session %s no longer exists (%s); stopping the stream",
                              self.session_id, gone.message)
                    self.session_gone = True
                    return
                except QoderError as rebuild_exc:
                    log.warning("history rebuild failed for %s: %s", self.session_id, rebuild_exc)
                if emitted or self.get_cursor() != cursor:
                    # History is reachable, so reconnect straight away rather than paying the
                    # backoff. `emitted` counts only events that got past dedupe, which is what
                    # keeps this from tight-looping: a second rebuild yields nothing new, so we
                    # fall through to the normal backoff below. The cursor is not a reliable
                    # signal on its own any more — it legitimately lags behind an answer the
                    # renderer has not finished writing.
                    continue
                log.warning("cursor %s still rejected and history added nothing; clearing it", cursor)
                self.set_cursor(None)

            except asyncio.CancelledError:
                raise

            except (httpx.ReadTimeout, httpx.HTTPError) as exc:
                attempt += 1
                log.info("stream %s dropped (%s: %s)", self.session_id, type(exc).__name__, exc)

            except QoderError as exc:
                if exc.status in (401, 403):
                    # Credential problem: retrying cannot help and would spin forever.
                    log.error("stream %s auth failure (%s); giving up", self.session_id, exc.message)
                    self.gave_up = True
                    return
                attempt += 1
                log.warning("stream %s api error %s: %s", self.session_id, exc.status, exc.message)

            except Exception:  # noqa: BLE001 - a consumer must never die silently
                attempt += 1
                log.exception("unexpected stream failure for %s", self.session_id)

            if stop.is_set():
                return
            # A connection that STAYED up counts as success even if it delivered nothing: the
            # server heartbeats roughly every 15s, so an idle-but-healthy stream must still
            # reset the backoff. What must not reset it is a connection that was accepted and
            # dropped at once — resetting on connect alone pinned the delay at the first step
            # forever, draining battery and quota until the process was killed.
            if connected_at is not None and time.monotonic() - connected_at >= STABLE_STREAM_S:
                attempt = 0
            # A single backoff point for every reconnect reason. A clean server-side close
            # leaves attempt at 0 and so pauses only briefly; repeated failures escalate.
            delay = 1.0 if attempt == 0 else RECONNECT_BACKOFF[min(attempt - 1, len(RECONNECT_BACKOFF) - 1)]
            await _sleep_or_stop(stop, delay)


async def _sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        pass
