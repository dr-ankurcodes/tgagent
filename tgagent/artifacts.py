"""Delivering agent-produced files to Telegram.

The path is three steps, verified in Phase 0 and unlike what the docs describe:

1. ``agent.artifact_delivered`` gives us ``file_id``, ``original_filename``, ``size``.
2. ``GET /files/{id}/content`` returns a JSON envelope ``{expires_at, url}``, NOT bytes.
3. The ``url`` is a pre-signed object-storage link valid about an hour; fetch it for bytes.

Because the link expires, we fetch it immediately rather than storing it. And because the
signed URL is third-party, the client fetches it without our bearer token.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from pathlib import Path
from typing import Callable

from telegram.error import BadRequest, NetworkError, TelegramError

from . import config, history
from .db import Database, utcnow
from .qclient import QoderError
from .qsessions import QoderAPI
from .renderer import ChatBudget, ChatGone, FloodWait
from .tgsink import TelegramSink

log = logging.getLogger("tgagent.artifacts")

# How many times one artifact is offered to Telegram before it is left for the next boot. Two:
# the first attempt, plus one more after riding out a flood wait or a transient network failure.
SEND_ATTEMPTS = 2

# Ceiling on cross-boot retries. A transient Telegram failure leaves the row pending so the next
# boot tries again, but a file Telegram repeatedly refuses is not transient — and without a cap it
# was re-downloaded (up to 50 MB into phone RAM) on every single start, forever.
MAX_BOOT_RETRIES = 5


def _is_transient(exc: TelegramError) -> bool:
    """Whether a Telegram failure is worth another attempt rather than a permanent verdict.

    ``BadRequest`` SUBCLASSES ``NetworkError`` in PTB, so the hierarchy alone cannot separate a
    mobile-data blip from "file is too big" — and treating the latter as transient would
    re-download and re-fail the same artifact on every boot, forever.
    """
    return isinstance(exc, NetworkError) and not isinstance(exc, BadRequest)


class ArtifactDeliverer:
    def __init__(
        self,
        *,
        api: QoderAPI,
        db: Database,
        sink: TelegramSink,
        budget: ChatBudget,
        convo_id: int,
        chat_id: int,
        notifier: Callable[[str], None] | None = None,
    ):
        self.api = api
        self.db = db
        self.sink = sink
        self.budget = budget
        self.convo_id = convo_id
        self.chat_id = chat_id
        # The conversation renderer's post_notice, so artifact notices go through the same single
        # writer as everything else. None only in tests; production always wires it (see convo.py).
        self.notifier = notifier

    def _record(self, file_id: str, payload: dict, **extra) -> bool:
        """Insert an artifact row. Returns False if it was already handled."""
        try:
            self.db.execute(
                """INSERT INTO artifacts(convo_id, file_id, event_id, filename, size_bytes,
                                         mime_type, discovered_at, delivered, skipped_reason)
                   VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    self.convo_id,
                    file_id,
                    payload.get("id"),
                    payload.get("original_filename"),
                    payload.get("size"),
                    payload.get("content_type"),
                    utcnow(),
                    int(extra.get("delivered", 0)),
                    extra.get("skipped_reason"),
                ),
            )
            return True
        except sqlite3.IntegrityError as exc:
            # UNIQUE(convo_id, file_id): already delivered or skipped in an earlier life.
            log.debug("artifact %s already recorded (%s)", file_id, exc)
            return False

    def _mark_delivered(self, file_id: str, message_id: int) -> None:
        """Recorded in the same step as the send, so a restart never double-sends."""
        self.db.execute(
            "UPDATE artifacts SET delivered = 1, tg_message_id = ? WHERE convo_id = ? AND file_id = ?",
            (message_id, self.convo_id, file_id),
        )

    def _mark_skipped(self, file_id: str, reason: str) -> None:
        self.db.execute(
            "UPDATE artifacts SET skipped_reason = ? WHERE convo_id = ? AND file_id = ?",
            (reason, self.convo_id, file_id),
        )

    async def deliver(self, payload: dict) -> None:
        """Handle one agent.artifact_delivered event."""
        file_id = payload.get("file_id")
        if not file_id:
            log.warning("artifact event with no file_id: %s", payload)
            return

        if not self._record(file_id, payload):
            return  # already delivered or already skipped in an earlier life of this process

        filename = payload.get("original_filename") or f"{file_id}.bin"
        try:
            size = int(payload.get("size") or 0)
        except (TypeError, ValueError):
            # A non-numeric size used to raise straight out of deliver(), after the row had
            # already been recorded — so it stayed pending and retry_pending re-attempted it on
            # every boot. Treat it as unknown; the byte-level cap in _fetch_and_send is the real
            # bound anyway, since the declared size can be missing or wrong.
            log.warning(
                "artifact %s had a non-numeric size %r; treating it as unknown",
                file_id, payload.get("size"),
            )
            size = 0

        if size > config.TG_UPLOAD_MAX_BYTES:
            reason = f"too large for telegram ({size} bytes)"
            self._mark_skipped(file_id, reason)
            await self._notify(
                f"The agent produced {filename} but it is "
                f"{size // (1024 * 1024)} MB, over Telegram's 50 MB bot limit. "
                "Ask it to split or compress the file.",
                is_error=True,
            )
            return

        await self._fetch_and_send(file_id, filename)

    async def retry_pending(self) -> int:
        """Re-attempt artifacts left undelivered by a crash. Called during reconciliation."""
        rows = self.db.query(
            """SELECT file_id, filename, size_bytes, mime_type, attempts FROM artifacts
               WHERE convo_id = ? AND delivered = 0 AND skipped_reason IS NULL""",
            (self.convo_id,),
        )
        count = 0
        for row in rows:
            attempts = int(row["attempts"] or 0)
            if attempts >= MAX_BOOT_RETRIES:
                # Surviving this many boots means the failure is not transient. Mark it skipped so
                # it leaves the pending set instead of being re-downloaded on every start, and
                # record why rather than leaving a mystery row behind.
                self._mark_skipped(row["file_id"], f"gave up after {attempts} attempts")
                log.warning(
                    "artifact %s failed %d times across boots; not retrying again",
                    row["file_id"], attempts,
                )
                continue
            self.db.execute(
                "UPDATE artifacts SET attempts = ? WHERE convo_id = ? AND file_id = ?",
                (attempts + 1, self.convo_id, row["file_id"]),
            )
            payload = {
                "file_id": row["file_id"],
                "original_filename": row["filename"],
                "size": row["size_bytes"],
                "content_type": row["mime_type"],
            }
            # _record would reject a duplicate, so drive the delivery steps directly.
            await self._redeliver(payload)
            count += 1
        return count

    async def _redeliver(self, payload: dict) -> None:
        file_id = payload["file_id"]
        filename = payload.get("original_filename") or f"{file_id}.bin"
        await self._fetch_and_send(file_id, filename, notify=False)

    async def _fetch_and_send(self, file_id: str, filename: str, *, notify: bool = True) -> bool:
        """Download via the signed URL, then send it. Records the outcome either way.

        Every PERMANENT failure sets ``skipped_reason``, because an artifact left with neither
        ``delivered`` nor a reason is picked up by ``retry_pending`` again on every boot — so one
        undeliverable file would be re-downloaded and re-failed forever. The single exception is
        a transient Telegram failure, which deliberately leaves the row pending: the bytes are
        still downloadable, so the next boot should try again rather than give up on them.
        """
        try:
            # Capped on the real bytes too: the declared size can be missing or wrong, and an
            # uncapped read into RAM is not survivable on a phone.
            contents = await self.api.download(file_id, max_bytes=config.TG_UPLOAD_MAX_BYTES)
        except QoderError as exc:
            if exc.status == 403:
                self._mark_skipped(file_id, "not downloadable")
                if notify:
                    await self._notify(
                        f"The agent referenced {filename} but it is marked internal and cannot "
                        "be downloaded.",
                        is_error=True,
                    )
                return False
            if exc.error_type == "too_large":
                # The declared size passed the pre-check in deliver() but the real bytes exceeded
                # the cap. Saying "download failed: 502" and echoing the client's own internal
                # message told the user nothing actionable; this is the one case where the cause
                # is known precisely, so name it.
                self._mark_skipped(file_id, "download exceeded the size cap")
                log.warning("artifact %s download exceeded the byte cap", file_id)
                if notify:
                    await self._notify(
                        f"The agent produced {filename} but it is larger than the "
                        f"{config.TG_UPLOAD_MAX_BYTES // (1024 * 1024)} MB delivery limit. "
                        "Ask it to split or compress the file.",
                        is_error=True,
                    )
                return False
            self._mark_skipped(file_id, f"download failed: {exc.status}")
            log.warning("artifact download failed for %s: %s", file_id, exc)
            if notify:
                await self._notify(
                    f"Could not download {filename} ({exc.message}).",
                    is_error=True,
                )
            return False

        return await self._send(file_id, filename, contents, notify=notify)

    async def _send(
        self, file_id: str, filename: str, contents: bytes, *, notify: bool
    ) -> bool:
        """Stage the bytes locally and hand them to Telegram."""
        # Both components sanitised: the filename is agent-chosen and the file id comes from the
        # API, and either could carry a separator that escapes the scratch directory.
        safe_id = config.safe_path_component(file_id, "artifact")
        safe_name = config.safe_path_component(filename, f"{safe_id}.bin")
        dest = config.tmp_root() / f"{safe_id}_{safe_name}"
        try:
            dest.write_bytes(contents)
            # Retain a copy in the durable cache before the scratch file is cleaned up below.
            # Without this the bytes exist nowhere after delivery, and a resumed conversation
            # could mention a file it had no way to provide again.
            history.retain_file(
                self.db,
                convo_id=self.convo_id,
                file_id=file_id,
                contents=contents,
                filename=safe_name,
                owner_type="agent",
            )
            message_id = await self._send_document_with_retry(dest, safe_name)
        except ChatGone:
            self._mark_skipped(file_id, "chat unreachable")
            return False
        except OSError as exc:
            self._mark_skipped(file_id, f"could not stage it locally: {type(exc).__name__}")
            log.warning("could not stage artifact %s locally: %s", file_id, exc)
            return False
        except Exception as exc:  # noqa: BLE001 - keep the conversation alive
            self._mark_skipped(file_id, f"telegram rejected it: {type(exc).__name__}")
            log.exception("failed to send artifact %s", file_id)
            return False
        finally:
            _cleanup(dest)

        if message_id is None:
            # Transient, so deliberately NOT marked skipped — retry_pending picks it up again.
            log.info("artifact %s could not be sent yet; it stays pending", file_id)
            if notify:
                await self._notify(
                    f"Could not send {safe_name} yet — Telegram failed twice. It is still "
                    "queued and I will try again on the next restart.",
                    is_error=True,
                )
            return False

        self._mark_delivered(file_id, message_id)
        log.info("delivered %s to chat %s", safe_name, self.chat_id)
        return True

    async def _send_document_with_retry(self, dest: Path, filename: str) -> int | None:
        """Send the file, riding out one flood wait or one transient Telegram failure.

        Returns the Telegram message id, or None when every attempt failed transiently and the
        artifact should stay pending. A PERMANENT rejection is raised instead, so the caller
        records why rather than leaving the row to be retried on every boot.
        """
        for _ in range(SEND_ATTEMPTS):
            try:
                return await self._send_document(dest, filename)
            except FloodWait as exc:
                await asyncio.sleep(exc.retry_after + 0.1)
            except TelegramError as exc:
                if not _is_transient(exc):
                    raise
                log.debug("sending %s hit a transient telegram error (%s); retrying", filename, exc)
                await asyncio.sleep(2.0)
        return None

    async def _send_document(self, dest: Path, filename: str) -> int:
        await self.budget.acquire()
        return await self.sink.send_document(self.chat_id, dest, filename=filename)

    async def _notify(self, text: str, *, is_error: bool = False) -> None:
        """Route an explanatory message through the conversation's single writer.

        This used to send straight through the sink, which bypassed the renderer's notice queue:
        an artifact notice could then interleave with a status edit the renderer was mid-way
        through, breaking the single-writer invariant every other outbound path obeys. The text is
        PLAIN — the renderer escapes notices wholesale, so markup here would reach the user as a
        literal ``<b>``. Never raises: a failed notice must not undo a delivery decision that has
        already been recorded.
        """
        if self.notifier is not None:
            try:
                self.notifier(text, is_error=is_error)
            except Exception:  # noqa: BLE001 - the notice is the least important part
                log.debug("could not enqueue a notice for chat %s", self.chat_id)
            return
        try:
            await self.budget.acquire()
            await self.sink.send_text(self.chat_id, text, parse_mode=None)
        except Exception as exc:  # noqa: BLE001 - the notice is the least important part
            log.debug("could not notify chat %s: %s", self.chat_id, exc)


def _cleanup(path: Path) -> None:
    """Remove the local copy. Phones have little storage, so do not accumulate artifacts."""
    try:
        path.unlink(missing_ok=True)
    except OSError as exc:
        log.debug("could not remove %s: %s", path, exc)
