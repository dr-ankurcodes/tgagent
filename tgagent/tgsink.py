"""Adapter from python-telegram-bot to the renderer's Sink protocol.

The renderer deliberately knows nothing about PTB. This module is the only place that
translates PTB's exception hierarchy into the four outcomes the renderer reasons about:
FloodWait, ParseRejected, MessageGone, ChatGone.

One translation deserves a note: Telegram rejects an edit whose text is identical to the
current message with BadRequest "message is not modified". That is not a failure — it means
the projection already matches the buffer — so it is reported as success. Treating it as an
error would send the renderer into a pointless retry loop.
"""

from __future__ import annotations

import logging
from pathlib import Path

from telegram import Bot
from telegram.constants import ChatAction
from telegram.error import BadRequest, Forbidden, RetryAfter, TelegramError

from .renderer import ChatGone, FloodWait, MessageGone, MessageTooLong, ParseRejected

log = logging.getLogger("tgagent.telegram")

NOT_MODIFIED = "message is not modified"
PARSE_MARKERS = (
    "can't parse entities",
    "parse entities",
    "unsupported parse_mode",
    "can't find end of the tag",
)
# Telegram rejects an over-cap message WHOLE rather than truncating it. Left unmapped, the raw
# BadRequest escaped the renderer's four handled outcomes into its generic recovery path, which
# retains buffered content by design — so the identical over-cap send was retried forever and the
# conversation wedged. Mapping it lets the renderer fall back to a truncated plain-text send.
TOO_LONG_MARKERS = (
    "message is too long",
    "text is too long",
    "caption is too long",
)
GONE_MARKERS = (
    "message to edit not found",
    "message can't be edited",
    "message with identifier not found",
    "message to delete not found",
    "replied message not found",
)
CHAT_GONE_MARKERS = (
    "bot was blocked by the user",
    "user is deactivated",
    "chat not found",
    "bot can't initiate conversations",
    "have no rights to send a message",
    "not enough rights to send text messages",
)


def translate(exc: TelegramError) -> Exception:
    """Map a PTB error onto the renderer's failure vocabulary."""
    if isinstance(exc, RetryAfter):
        return FloodWait(float(exc.retry_after))

    if isinstance(exc, Forbidden):
        return ChatGone(str(exc))

    if isinstance(exc, BadRequest):
        message = str(exc).lower()
        if NOT_MODIFIED in message:
            return _NotModified()
        if any(marker in message for marker in PARSE_MARKERS):
            return ParseRejected(str(exc))
        if any(marker in message for marker in TOO_LONG_MARKERS):
            return MessageTooLong(str(exc))
        if any(marker in message for marker in CHAT_GONE_MARKERS):
            return ChatGone(str(exc))
        if any(marker in message for marker in GONE_MARKERS):
            return MessageGone(str(exc))
        # An unrecognised BadRequest is classified by English substrings above, so a wording
        # change on Telegram's side lands here. Log the exact text before passing it through:
        # without this the only trace was the renderer's generic "unexpected error in render
        # loop", which named neither the cause nor the message, making a new marker invisible
        # until a chat wedged. It is still returned raw rather than mapped to a content-dropping
        # outcome — an unknown rejection must not silently eat buffered text.
        log.warning("unclassified BadRequest passed through to the renderer: %s", exc)
        return exc

    # NetworkError, TimedOut and anything unrecognised are transient: let them propagate so
    # the caller's own retry and backoff logic handles them.
    return exc


class _NotModified(Exception):
    """Internal signal: the message already has this exact text."""


class TelegramSink:
    """Implements renderer.Sink, plus the file sending that artifacts need."""

    def __init__(self, bot: Bot, *, message_thread_id: int | None = None):
        self.bot = bot
        self.message_thread_id = message_thread_id

    async def _call(self, coro_factory):
        try:
            return await coro_factory()
        except TelegramError as exc:
            mapped = translate(exc)
            if isinstance(mapped, _NotModified):
                return None
            raise mapped from exc

    async def send_text(self, chat_id: int, text: str, *, parse_mode: str | None) -> int:
        async def do():
            message = await self.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=parse_mode or None,
                message_thread_id=self.message_thread_id,
                disable_web_page_preview=True,
            )
            return message.message_id

        return await self._call(do)

    async def edit_text(self, chat_id: int, message_id: int, text: str, *, parse_mode: str | None) -> None:
        async def do():
            return await self.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                parse_mode=parse_mode or None,
                disable_web_page_preview=True,
            )

        await self._call(do)

    async def send_typing(self, chat_id: int) -> None:
        async def do():
            return await self.bot.send_chat_action(
                chat_id=chat_id,
                action=ChatAction.TYPING,
                message_thread_id=self.message_thread_id,
            )

        await self._call(do)

    async def delete_message(self, chat_id: int, message_id: int) -> None:
        async def do():
            return await self.bot.delete_message(chat_id=chat_id, message_id=message_id)

        await self._call(do)

    async def send_document(
        self,
        chat_id: int,
        path: Path,
        *,
        filename: str | None = None,
        caption: str | None = None,
    ) -> int:
        """Send a file as a document.

        send_document rather than send_photo even for images: send_photo recompresses and caps
        at 10 MB, and a generated chart or slide export should reach the user byte-exact.

        ``caption`` is PLAIN TEXT and is sent with no parse mode. It used to be sent as HTML
        without being escaped, so a '<' or '&' in an agent-chosen artifact name made Telegram
        reject the entire send and the file was never delivered. Captions here are filenames;
        a caller that genuinely needs markup must escape it and pass a parse mode of its own.
        """

        async def do():
            with path.open("rb") as handle:
                message = await self.bot.send_document(
                    chat_id=chat_id,
                    document=handle,
                    filename=filename or path.name,
                    caption=caption,
                    message_thread_id=self.message_thread_id,
                )
            return message.message_id

        return await self._call(do)
