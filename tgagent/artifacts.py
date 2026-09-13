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

from . import config, history, tg_html
from .db import Database, utcnow
from .qclient import QoderError
from .qsessions import QoderAPI
from .renderer import ChatBudget, ChatGone, FloodWait
from .tgsink import TelegramSink

log = logging.getLogger("tgagent.artifacts")


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
    ):
        self.api = api
        self.db = db
        self.sink = sink
        self.budget = budget
        self.convo_id = convo_id
        self.chat_id = chat_id

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
        size = int(payload.get("size") or 0)

        if size > config.TG_UPLOAD_MAX_BYTES:
            reason = f"too large for telegram ({size} bytes)"
            self._mark_skipped(file_id, reason)
            await self._notify(
                f"The agent produced <b>{tg_html.escape(filename)}</b> but it is "
                f"{size // (1024 * 1024)} MB, over Telegram's 50 MB bot limit. "
                "Ask it to split or compress the file."
            )
            return

        if await self._fetch_and_send(file_id, filename):
            log.info("delivered %s to chat %s", filename, self.chat_id)

    async def retry_pending(self) -> int:
        """Re-attempt artifacts left undelivered by a crash. Called during reconciliation."""
        rows = self.db.query(
            """SELECT file_id, filename, size_bytes, mime_type FROM artifacts
               WHERE convo_id = ? AND delivered = 0 AND skipped_reason IS NULL""",
            (self.convo_id,),
        )
        count = 0
        for row in rows:
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
        """Download via the signed URL and send it. Records the outcome either way.

        Every failure path sets ``skipped_reason``, because an artifact left with neither
        ``delivered`` nor a reason is picked up by ``retry_pending`` again on every boot —
        so one permanently undeliverable file would be re-downloaded and re-failed forever.

        Transient network errors (Telegram timeouts, connection resets) are retraced ONCE
        before any permanent decision. They should not retire files that might be recoverable
        on the next boot.
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
                        f"The agent referenced <b>{tg_html.escape(filename)}</b> but it is marked "
                        "internal and cannot be downloaded."
                    )
                return False
            self._mark_skipped(file_id, f"download failed: {exc.status}")
            log.warning("artifact download failed for %s: %s", file_id, exc)
            if notify:
                await self._notify(
                    f"Could not download <b>{tg_html.escape(filename)}</b> "
                    f"({tg_html.escape(exc.message)})."
                )
            return False

        dest = config.tmp_root() / f"{file_id}_{Path(filename).name}"
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
                filename=Path(filename).name,
                owner_type="agent",
            )
            message_id = await self._send_document(dest, filename)
        except FloodWait as exc:
            await asyncio.sleep(exc.retry_after + 0.1)
            try:
                message_id = await self._send_document(dest, filename)
            except Exception as retry_exc:  # noqa: BLE001 - a retry must not escape either
                self._mark_skipped(file_id, f"telegram rejected it: {retry_exc}")
                log.warning("artifact retry failed for %s: %s", file_id, retry_exc)
                return False
        except TelegramError as exc:
            # Treat Telegram time-outs / transport errors as transient: retry once, then leave
            # skipped_reason=NULL so retry_pending will try again on the next boot. Notify the
            # user so they know something went wrong rather than silently losing the file.
            is_timeout = "timedout" in type(exc).__name__.lower() or "timed_out" in type(exc).__name__.lower()
            if is_timeout:
                log.debug("artifact %s timed out, retrying once...", file_id)
                await asyncio.sleep(2.0)
                try:
                    message_id = await self._send_document(dest, filename)
                    self._mark_delivered(file_id, message_id)
                    return True
                except Exception as retry_exc:  # noqa: BLE001
                    log.warning("artifact retry also failed for %s: %s", file_id, retry_exc)
                    # Leave skipped_reason=NULL so retry_pending will try again on next boot
                    if notify:
                        await self._notify(f"Could not send <b>{tg_html.escape(filename)}</b> yet "
                                          f"(network timeout). Retrying on your next restart.")
                    return False
            else:
                # Non-timeout Telegram error: permanent rejection.
                self._mark_skipped(file_id, f"telegram rejected it: {type(exc).__name__}")
                log.warning("artifact telegram error for %s: %s", file_id, exc)
                return False
        except ChatGone:
            self._mark_skipped(file_id, "chat unreachable")
            return False
        except Exception as exc:  # noqa: BLE001 - keep the conversation alive
            self._mark_skipped(file_id, f"send failed: {type(exc).__name__}")
            log.exception("failed to send artifact %s", file_id)
            return False
        finally:
            _cleanup(dest)

        self._mark_delivered(file_id, message_id)
        return True

    async def _send_document(self, dest: Path, filename: str) -> int:
        await self.budget.acquire()
        return await self.sink.send_document(self.chat_id, dest, filename=Path(filename).name)

    async def _notify(self, html: str) -> None:
        try:
            await self.budget.acquire()
            await self.sink.send_text(self.chat_id, html, parse_mode="HTML")
        except (FloodWait, ChatGone) as exc:
            log.debug("could not notify chat %s: %s", self.chat_id, exc)


def _cleanup(path: Path) -> None:
    """Remove the local copy. Phones have little storage, so do not accumulate artifacts."""
    try:
        path.unlink(missing_ok=True)
    except OSError as exc:
        log.debug("could not remove %s: %s", path, exc)
