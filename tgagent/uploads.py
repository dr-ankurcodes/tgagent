"""Inbound files: Telegram -> Qoder sandbox.

Phase 0 settled the approach. Binary uploads are accepted despite the API reference still
claiming text-only, and the sandbox's Read tool decodes images natively — it correctly
reported a mounted 64x32 solid-blue PNG as "64x32 pixels, solid blue".

So the flow is: download from Telegram, upload to Qoder, mount at a known path, then tell the
agent that path in the same user.message. That is strictly better than the inline base64
image block, which inflates the payload by 33% against the 4 MB request-body cap and charges
image tokens on every subsequent turn.

Two hard caps to respect: Telegram lets a bot download at most 20 MB via getFile, and the
Qoder multipart upload accepts about 5 MB of file content.
"""

from __future__ import annotations

import logging
import re
import secrets
from dataclasses import dataclass
from pathlib import PurePosixPath

from telegram import Message

from . import auth, config, history
from .db import Database
from .qclient import QoderError
from .qsessions import QoderAPI

log = logging.getLogger("tgagent.uploads")

_SLUG_RE = re.compile(r"[^a-z0-9._-]+")


class IngestError(Exception):
    """A user-facing reason the file could not be ingested."""

    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


@dataclass
class Ingested:
    file_id: str
    mount_path: str
    filename: str
    mime_type: str | None
    size_bytes: int
    pointer_text: str


def slugify(name: str, fallback: str = "file") -> str:
    stem = PurePosixPath(name).name.lower()
    stem = _SLUG_RE.sub("-", stem).strip("-.")
    return stem[:60] or fallback


def unique_mount_path(filename: str) -> str:
    """A collision-free path under the uploads dir.

    The random suffix matters: two users could both send "report.pdf", and a shared sandbox
    path would silently overwrite one with the other.
    """
    slug = slugify(filename)
    return f"{config.UPLOAD_MOUNT_DIR}/{secrets.token_hex(3)}-{slug}"


def _pick_file(message: Message):
    """Return (telegram_file, filename, mime_type, size) for whatever the user sent."""
    if message.photo:
        # photo is an array of progressively larger sizes; the last is the biggest.
        photo = message.photo[-1]
        return photo, f"photo_{photo.file_unique_id}.jpg", "image/jpeg", photo.file_size
    if message.document:
        doc = message.document
        return doc, doc.file_name or f"document_{doc.file_unique_id}", doc.mime_type, doc.file_size
    if message.video:
        vid = message.video
        return vid, vid.file_name or f"video_{vid.file_unique_id}", "video/mp4", vid.file_size
    if message.audio:
        aud = message.audio
        return aud, aud.file_name or f"audio_{aud.file_unique_id}", aud.mime_type, aud.file_size
    if message.voice:
        voice = message.voice
        return voice, f"voice_{voice.file_unique_id}.ogg", "audio/ogg", voice.file_size
    if message.animation:
        anim = message.animation
        return anim, anim.file_name or f"gif_{anim.file_unique_id}", "image/gif", anim.file_size
    return None


def has_attachment(message: Message) -> bool:
    return _pick_file(message) is not None


async def ingest(
    *,
    message: Message,
    api: QoderAPI,
    db: Database,
    tg_user_id: int,
    convo_id: int,
    session_id: str | None,
    caption: str | None = None,
) -> Ingested:
    """Download from Telegram, upload to Qoder, mount it, and build the pointer message."""
    picked = _pick_file(message)
    if picked is None:
        raise IngestError("that message had no attachment I can read")
    if not session_id:
        # Without a session there is no sandbox to mount into. Say so, rather than let the
        # request below fail against /sessions/None/resources and report a mount error.
        raise IngestError(
            "that conversation has no session to attach a file to. Use /new to start another."
        )

    tg_file, filename, mime_type, size = picked
    size = int(size or 0)

    if size > config.TG_DOWNLOAD_MAX_BYTES:
        raise IngestError(
            f"that file is {size // (1024 * 1024)} MB. Telegram only lets a bot download "
            "up to 20 MB."
        )
    if size > config.QODER_UPLOAD_MAX_BYTES:
        raise IngestError(
            f"that file is {size / (1024 * 1024):.1f} MB. The Qoder upload endpoint accepts "
            "about 5 MB. Try splitting it, or send the important part as text."
        )

    try:
        remote = await tg_file.get_file()
        contents = await remote.download_as_bytearray()
    except Exception as exc:  # noqa: BLE001 - Telegram raises many types here
        log.warning("telegram download failed for %s: %s", filename, exc)
        raise IngestError(f"Telegram would not give me that file ({type(exc).__name__}).") from exc

    # The two checks above trust Telegram's reported file_size, which can be missing (None -> 0)
    # or simply wrong, and download_as_bytearray itself is uncapped. The real bytes are the only
    # trustworthy bound, so re-check them: a missing size sailed past both pre-checks entirely,
    # and an oversized file would otherwise reach the Qoder upload and fail there with a less
    # useful error after the whole download had already been paid for in memory.
    actual = len(contents)
    if actual > config.TG_DOWNLOAD_MAX_BYTES:
        raise IngestError(
            f"that file turned out to be {actual // (1024 * 1024)} MB. Telegram only lets a bot "
            "download up to 20 MB."
        )
    if actual > config.QODER_UPLOAD_MAX_BYTES:
        raise IngestError(
            f"that file is {actual / (1024 * 1024):.1f} MB. The Qoder upload endpoint accepts "
            "about 5 MB. Try splitting it, or send the important part as text."
        )

    metadata = {"convo_id": convo_id, "tg_user_id": tg_user_id, "source": "telegram"}
    try:
        uploaded = await api.upload(bytes(contents), filename, metadata=metadata)
    except QoderError as exc:
        log.warning("qoder upload rejected %s (%s): %s", filename, exc.status, exc.message)
        raise IngestError(
            f"Qoder rejected that file ({exc.status}: {exc.message}). "
            "Only files up to about 5 MB are accepted."
        ) from exc

    file_id = uploaded.get("id")
    if not file_id:
        raise IngestError("Qoder accepted the upload but returned no file id.")

    mount_path = unique_mount_path(filename)
    try:
        await api.attach_file(session_id, file_id, mount_path)
    except QoderError as exc:
        log.warning("mount failed for %s: %s", file_id, exc.message)
        raise IngestError(f"the file uploaded but could not be mounted ({exc.message}).") from exc

    auth.record_tg_file(
        db,
        file_id=file_id,
        owner_tg_user_id=tg_user_id,
        convo_id=convo_id,
        filename=filename,
        mime_type=mime_type,
        size_bytes=size,
        mount_path=mount_path,
    )

    # Keep a local copy. The uploaded file belongs to whichever account the PAT pointed at, so
    # after a rotation it is unreachable; this copy is what lets a resumed conversation
    # re-mount the same file under the new account. Best-effort: a full disk must not lose the
    # user's message, which has already been accepted by the API at this point.
    history.retain_file(
        db,
        convo_id=convo_id,
        file_id=file_id,
        contents=bytes(contents),
        filename=filename,
        owner_type="user",
    )

    return Ingested(
        file_id=file_id,
        mount_path=mount_path,
        filename=filename,
        mime_type=mime_type,
        size_bytes=size,
        pointer_text=build_pointer_text(filename, mime_type, size, mount_path, caption),
    )


def build_pointer_text(
    filename: str,
    mime_type: str | None,
    size: int,
    mount_path: str,
    caption: str | None = None,
) -> str:
    """What the agent actually receives.

    The agent cannot see the Telegram message, so the path has to be spelled out. Mentioning
    that Read decodes images is what makes "what is in this photo?" work without a nudge.
    """
    size_label = f"{size / 1024:.0f} KB" if size < 1024 * 1024 else f"{size / (1024 * 1024):.1f} MB"
    lines = [
        f"The user attached a file: {filename}"
        + (f" ({mime_type}, {size_label})" if mime_type else f" ({size_label})"),
        f"It is mounted in your sandbox at exactly: {mount_path}",
        "Use the Read tool on that path to inspect it. Read decodes images natively, so you "
        "can see pictures directly; for other formats use Bash as needed.",
    ]
    if caption and caption.strip():
        lines.append("")
        lines.append("The user's message was:")
        lines.append(caption.strip())
    else:
        lines.append("")
        lines.append("The user sent no text with it. Inspect the file and tell them what it "
                     "contains, then ask what they would like done with it.")
    return "\n".join(lines)
