"""Async HTTP client for the Qoder Cloud Agents API.

Two separate httpx clients on purpose. The signed download URLs returned by
``GET /files/{id}/content`` point at third-party object storage
(``qoder-cloud-agents-storage-sg.oss-ap-southeast-1.aliyuncs.com``). Sending our bearer
token there would hand the credential to a third party, so signed URLs are fetched with a
client that carries no Authorization header at all.
"""

from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any, AsyncIterator
from urllib.parse import urlsplit

import httpx

from . import __version__
from .config import QODER_EVENTS_PAGE_MAX, SSE_STALL_TIMEOUT_S

log = logging.getLogger("tgagent.qoder")

# Observed error envelope:
# {"type":"error","request_id":"...","error":{"type":"...","message":"...","param":"..."}}
RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
BACKOFF_SCHEDULE = (1.0, 2.0, 4.0)

# Replaying these is harmless: they read, or they write a value rather than perform an
# action. POST is the odd one out — POST /sessions/{id}/events RUNS A TURN and bills for
# it, so a retry after the server already accepted the request pays twice and renders the
# same answer into the chat twice.
IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "PUT", "PATCH", "DELETE"})

# A 429 is safe to replay even for POST: the server rejected the request before processing
# it. Every other 4xx/5xx leaves it unknown, so a non-idempotent method must not retry.
POST_RETRY_STATUSES = frozenset({429})

# Transport failures that prove the request never reached the server, so even a POST is safe
# to replay. A ReadTimeout or RemoteProtocolError does NOT prove that — the server may well
# have received and acted on it before the connection dropped.
PRE_SEND_ERRORS = (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout)

# Ceiling on how many pages one collection lookup will walk. At 100 items a page this is 5000
# resources, far past anything this bot creates, and it bounds a lookup whose cursor the server
# keeps accepting without ever reporting the end.
MAX_PAGES = 50


class QoderError(Exception):
    """An API error, with the envelope fields unpacked."""

    def __init__(
        self,
        status: int,
        message: str,
        *,
        error_type: str = "api_error",
        param: str | None = None,
        request_id: str | None = None,
        body: str = "",
    ):
        self.status = status
        self.message = message
        self.error_type = error_type
        self.param = param
        self.request_id = request_id
        self.body = body
        super().__init__(f"HTTP {status} {error_type}: {message}")

    @classmethod
    def from_response(cls, resp: httpx.Response) -> "QoderError":
        body = resp.text
        message, error_type, param, request_id = body[:400], "api_error", None, None
        try:
            payload = json.loads(body)
            inner = payload.get("error") or {}
            message = inner.get("message") or message
            error_type = inner.get("type") or payload.get("type") or error_type
            param = inner.get("param")
            request_id = payload.get("request_id")
        except (json.JSONDecodeError, AttributeError):
            pass
        return cls(
            resp.status_code,
            message,
            error_type=error_type,
            param=param,
            request_id=request_id,
            body=body,
        )


class NotFound(QoderError):
    """404. During reconciliation this means the session or event cursor is gone."""


class Conflict(QoderError):
    """409. Most importantly: posting a turn while one is already running."""


class Unauthorized(QoderError):
    """401/403. The PAT is missing, expired, or lacks scope."""


class RateLimited(QoderError):
    def __init__(self, *args, retry_after: float | None = None, **kwargs):
        self.retry_after = retry_after
        super().__init__(*args, **kwargs)


class BillingError(QoderError):
    """402. The user has run out of credits."""


def _retry_after_seconds(raw: str | None) -> float | None:
    """Parse a ``Retry-After`` header, which is either delta-seconds or an HTTP-date.

    ``float(raw)`` alone silently discarded the date form: the parse failed, ``retry_after``
    became None, and the caller fell back to a 1–4s schedule that re-429'd immediately because
    the server had asked for a specific future instant.
    """
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return max(0.0, (when - datetime.now(timezone.utc)).total_seconds())


def _classify(resp: httpx.Response) -> QoderError:
    if resp.status_code in (401, 403):
        return Unauthorized.from_response(resp)
    if resp.status_code == 402:
        return BillingError.from_response(resp)
    if resp.status_code == 404:
        return NotFound.from_response(resp)
    if resp.status_code == 409:
        return Conflict.from_response(resp)
    if resp.status_code == 429:
        error = RateLimited.from_response(resp)
        error.retry_after = _retry_after_seconds(resp.headers.get("Retry-After"))
        return error
    return QoderError.from_response(resp)


class QoderClient:
    def __init__(
        self,
        base_url: str,
        pat: str,
        *,
        timeout: float = 60.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        """``transport`` is an injection seam: it replaces httpx's real transport, so a caller
        can drive this client — including its retry and SSE handling — without a network.
        Production leaves it None, which is httpx's own default.
        """
        self.base_url = base_url.rstrip("/")
        self._auth = httpx.AsyncClient(
            base_url=f"{self.base_url}/api/v1/cloud",
            headers={
                "Authorization": f"Bearer {pat}",
                "Accept": "application/json",
                "User-Agent": f"tgagent/{__version__}",
            },
            timeout=timeout,
            transport=transport,
        )
        # No Authorization header, no base_url: for pre-signed third-party download URLs.
        self._plain = httpx.AsyncClient(
            timeout=httpx.Timeout(120.0), follow_redirects=True, transport=transport
        )

    def _path(self, path: str) -> str:
        return path if path.startswith("/") else f"/{path}"

    async def request(
        self,
        method: str,
        path: str,
        *,
        payload: Any | None = None,
        params: dict | None = None,
        files: Any | None = None,
        data: dict | None = None,
        extra_headers: dict | None = None,
        attempts: int = 3,
    ) -> dict:
        """One API call with bounded retry.

        What may be retried depends on the method. A 4xx other than 429 is never retried for
        any method: it means our request was wrong, and repeating it would just burn time.

        For a POST, a retry is limited to a 429 and to transport failures that happened
        before the request left this process. Anything else leaves it unknown whether the
        server acted, and replaying POST /sessions/{id}/events would run — and bill for —
        the same turn twice. Losing one turn to a flaky mobile link is recoverable: the
        caller's durable queue still holds it. Running it twice is not.

        ``files`` and ``data`` are replayed verbatim on a retry. That is safe for the bytes
        ``upload_file`` passes, but a caller that passed an OPEN FILE OBJECT would get a
        silently empty or partial body on the second attempt, because the file position is
        already at EOF. Pass bytes, or seek(0) is the caller's problem — this method does not
        and cannot rewind a stream it does not own.
        """
        idempotent = method.upper() in IDEMPOTENT_METHODS
        retry_statuses = RETRY_STATUSES if idempotent else POST_RETRY_STATUSES

        kwargs: dict[str, Any] = {"headers": extra_headers or {}}
        if params:
            kwargs["params"] = params
        if files is not None or data is not None:
            if files is not None:
                kwargs["files"] = files
            if data is not None:
                kwargs["data"] = data
        elif payload is not None:
            kwargs["json"] = payload

        last_error: QoderError | None = None
        for attempt in range(attempts):
            try:
                resp = await self._auth.request(method, self._path(path), **kwargs)
            except httpx.HTTPError as exc:
                # Transport-level failure (DNS, connection reset, timeout).
                retriable = idempotent or isinstance(exc, PRE_SEND_ERRORS)
                last_error = QoderError(0, f"{type(exc).__name__}: {exc}", error_type="transport_error")
                if not retriable:
                    log.warning(
                        "qoder %s %s failed after the request may have been received (%s); "
                        "not retrying a non-idempotent method",
                        method, path, type(exc).__name__,
                    )
                    raise last_error from exc
                if attempt < attempts - 1:
                    await asyncio.sleep(BACKOFF_SCHEDULE[min(attempt, len(BACKOFF_SCHEDULE) - 1)])
                    continue
                raise last_error from exc

            if resp.status_code < 400:
                text = resp.text
                if not text:
                    return {}
                try:
                    return json.loads(text)
                except json.JSONDecodeError as exc:
                    # A captive portal or proxy can return 200 with an HTML body, which is
                    # common on mobile data. Report it in the caller's exception vocabulary
                    # rather than leaking a JSONDecodeError nobody catches. Not retried:
                    # repeating the request will not make a proxy start returning JSON.
                    raise QoderError(
                        resp.status_code,
                        f"expected JSON, got {resp.headers.get('content-type') or 'unknown content type'}",
                        error_type="unexpected_shape",
                        body=text[:400],
                    ) from exc

            error = _classify(resp)
            if resp.status_code not in retry_statuses or attempt >= attempts - 1:
                raise error

            last_error = error
            delay = BACKOFF_SCHEDULE[min(attempt, len(BACKOFF_SCHEDULE) - 1)]
            if isinstance(error, RateLimited) and error.retry_after:
                delay = max(delay, error.retry_after)
            log.warning(
                "qoder %s %s -> %s, retrying in %.1fs (attempt %d/%d)",
                method, path, resp.status_code, delay, attempt + 1, attempts,
            )
            await asyncio.sleep(delay)

        raise last_error or QoderError(0, "exhausted retries")

    async def get(self, path: str, **params) -> dict:
        return await self.request("GET", path, params={k: v for k, v in params.items() if v is not None})

    async def post(self, path: str, payload: dict | None = None, **kwargs) -> dict:
        return await self.request("POST", path, payload=payload if payload is not None else {}, **kwargs)

    async def put(self, path: str, payload: dict, **kwargs) -> dict:
        """Whole-object replace. The agents resource advertises GET/POST/PUT/DELETE; PATCH
        answers 405, so an update has to send the complete definition."""
        return await self.request("PUT", path, payload=payload, **kwargs)

    async def delete(self, path: str, **kwargs) -> dict:
        return await self.request("DELETE", path, **kwargs)

    async def upload_file(self, contents: bytes, name: str, metadata: dict | None = None) -> dict:
        """POST /files as multipart. Accepts binary as well as text (verified in Phase 0)."""
        data = {"name": name}
        if metadata is not None:
            # metadata is a JSON string field, capped at 8 KB by the API.
            data["metadata"] = json.dumps(metadata)[:8000]
        return await self.request("POST", "/files", files={"file": (name, contents)}, data=data)

    async def get_signed_download_url(self, file_id: str) -> tuple[str, str | None]:
        """GET /files/{id}/content returns {expires_at, url}, NOT bytes.

        Verified in Phase 0: the url is a pre-signed object-storage link valid ~1 hour.
        Raises QoderError(403) when the file is not downloadable.
        """
        envelope = await self.get(f"/files/{file_id}/content")
        url = envelope.get("url")
        if not url:
            raise QoderError(
                502,
                f"content envelope for {file_id} had no url",
                error_type="unexpected_shape",
                body=json.dumps(envelope)[:400],
            )
        return url, envelope.get("expires_at")

    async def download_signed(self, url: str, *, max_bytes: int | None = None) -> bytes:
        """Fetch a pre-signed URL WITHOUT sending our bearer token.

        Streamed rather than read whole, so ``max_bytes`` can abort a download that is larger
        than we are willing to hold in memory. The artifact event's declared ``size`` is not
        always present or accurate, so the real bound has to be enforced on the bytes.

        HTTPS is enforced on the envelope's URL. No credential rides along, so the impact of a
        non-HTTPS URL is limited to probing internal hosts from the phone — but the envelope is
        attacker-influenceable if the API response is ever tampered with, and ``file://`` or
        ``http://`` there would turn this method into an open redirect/probe. A host allowlist
        is deliberately NOT used: the storage provider (currently aliyuncs.com) can change, and
        pinning it would break downloads the day Qoder moves buckets.
        """
        scheme = urlsplit(url).scheme.lower()
        if scheme != "https":
            raise QoderError(
                0,
                f"refusing to fetch a signed url with scheme {scheme!r}; only https is allowed",
                error_type="unsafe_url",
            )
        async with self._plain.stream("GET", url) as resp:
            if resp.status_code >= 400:
                await resp.aread()
                raise QoderError(resp.status_code, f"signed download failed: {resp.text[:200]}")
            chunks: list[bytes] = []
            total = 0
            async for chunk in resp.aiter_bytes():
                total += len(chunk)
                if max_bytes is not None and total > max_bytes:
                    raise QoderError(
                        502,
                        f"download exceeded the {max_bytes} byte cap; aborted",
                        error_type="too_large",
                    )
                chunks.append(chunk)
            return b"".join(chunks)

    async def download_file(self, file_id: str, *, max_bytes: int | None = None) -> bytes:
        """Full three-step artifact download: envelope -> signed url -> bytes."""
        url, _ = await self.get_signed_download_url(file_id)
        return await self.download_signed(url, max_bytes=max_bytes)

    @asynccontextmanager
    async def stream_events(
        self,
        session_id: str,
        *,
        deltas: bool = True,
        last_event_id: str | None = None,
        read_timeout: float | None = SSE_STALL_TIMEOUT_S,
    ) -> AsyncIterator[httpx.Response]:
        """Open the SSE stream for a session.

        Deltas are OPT-IN and only agent.message / agent.thinking are accepted; any other
        value returns 400. Without them the renderer would only ever see complete buffered
        events and there would be no live-typing feel.

        ``read_timeout`` is finite on purpose. The server heartbeats roughly every 15s, so a
        read timeout is how we notice a silently dead socket — a phone losing mobile data
        gives no FIN, and an infinite read timeout would hang the consumer forever. A
        ReadTimeout is expected, recoverable, and the caller reconnects from its cursor.

        No retry here: a stream is long-lived and only the caller knows whether it is safe to
        resume from the stored cursor.
        """
        params: list[tuple[str, str]] = []
        if deltas:
            params.append(("event_deltas[]", "agent.message"))
            params.append(("event_deltas[]", "agent.thinking"))
        headers = {"Accept": "text/event-stream"}
        if last_event_id:
            headers["Last-Event-ID"] = last_event_id

        # Pool and write are finite too. The previous ``Timeout(None, ...)`` left them
        # unlimited, so a consumer waiting for a pool slot at shutdown could hang forever
        # instead of failing into the reconnect loop. Read stays the stall detector.
        timeout = httpx.Timeout(pool=30.0, connect=30.0, read=read_timeout, write=30.0)
        async with self._auth.stream(
            "GET",
            f"/sessions/{session_id}/events/stream",
            params=params,
            headers=headers,
            timeout=timeout,
        ) as resp:
            if resp.status_code >= 400:
                await resp.aread()
                raise _classify(resp)
            yield resp

    async def aclose(self) -> None:
        # try/finally: if closing the authenticated client raises (e.g. during cancellation),
        # the plain client's connection pool would otherwise leak for the life of the process.
        try:
            await self._auth.aclose()
        finally:
            await self._plain.aclose()


async def iter_pages(
    client: QoderClient, path: str, *, limit: int = QODER_EVENTS_PAGE_MAX, **params
) -> AsyncIterator[dict]:
    """Yield every item of a paginated collection endpoint, following ``after_id``.

    Reading only the first page — which is what the environment, agent and memory-store
    lookups used to do — makes an existing resource invisible once the account holds more
    than one page of them. The caller then concludes it does not exist and creates a
    duplicate, silently orphaning the original.

    Stops on ``has_more`` being false, on an empty page, and on a page that makes no cursor
    progress, so an endpoint that ignores ``after_id`` cannot loop forever. ``MAX_PAGES`` is a
    final ceiling on how much of the account one lookup will walk.
    """
    after_id = None
    for _ in range(MAX_PAGES):
        page = await client.get(path, limit=limit, after_id=after_id, **params)
        data = page.get("data") or []
        for item in data:
            yield item
        if not page.get("has_more") or not data:
            return
        next_id = data[-1].get("id")
        if not next_id or next_id == after_id:
            log.warning("pagination of %s made no progress at %s; stopping", path, after_id)
            return
        after_id = next_id
    # Reaching here means the loop exhausted MAX_PAGES without the server reporting the end.
    # iter_session_events logs this; iter_pages used to stop silently, so a collection lookup
    # that hit the ceiling looked identical to one that finished normally.
    log.warning("pagination of %s hit the %d page ceiling", path, MAX_PAGES)


async def iter_session_events(
    client: QoderClient, session_id: str, after_id: str | None = None
) -> AsyncIterator[dict]:
    """Yield every buffered event after a cursor, following pagination.

    The single implementation of history paging, shared by ``QoderAPI.iter_events`` (poll
    mode) and ``StreamConsumer._rebuild_from_history`` (recovering from a rejected cursor).
    The two must agree exactly: the rebuild path is what repairs a stale cursor, so
    divergent pagination would silently drop events.

    ``after_id`` and ``page`` are mutually exclusive on this endpoint; we only use
    ``after_id``.

    Bounded by ``MAX_PAGES`` and by a no-progress check, for the same reason ``iter_pages``
    is: this runs on the poll fallback every few seconds, so a cursor the server never
    advances would spin one doomed request per tick for the life of the process.
    """
    cursor = after_id
    for _ in range(MAX_PAGES):
        page = await client.get(
            f"/sessions/{session_id}/events",
            after_id=cursor,
            limit=QODER_EVENTS_PAGE_MAX,
            order="asc",
        )
        events = page.get("data", [])
        before = cursor
        for event in events:
            yield event
            cursor = event.get("id") or cursor
        if not page.get("has_more") or not events:
            return
        if cursor == before:
            log.warning(
                "event pagination for session %s made no progress at %s; stopping",
                session_id, cursor,
            )
            return
    log.warning("event pagination for session %s hit the %d page ceiling", session_id, MAX_PAGES)
