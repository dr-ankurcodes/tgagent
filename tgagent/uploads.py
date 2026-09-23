"""Inbound files: Telegram -> Qoder sandbox.

Phase 0 settled the approach. Binary uploads are accepted despite the API reference still
claiming text-only, and the sandbox's Read tool decodes images natively — it correctly
reported a mounted 64x32 solid-blue PNG as "64x32 pixels, solid blue".

So the flow is: download from Telegram, upload to Qoder, mount at a known path, then tell the
agent that path in the same user.message. That is strictly better than the inline base64
image block, which inflates the payload by 33% against the 4 MB request-body cap and charges
image tokens on every subsequent turn.

Two hard caps to respect: Telegram lets a bot download at most 20 MB via getFile, and the
Qoder multipart upload accepts about 5 MB of file content per request. The gap between them
is bridged by chunking: a larger file is uploaded in <5 MB pieces, mounted side by side as
numbered ``.partNNN`` files, and the pointer message tells the agent to reassemble it with a
single ``cat`` — see :func:`upload_and_mount`.
"""

from __future__ import annotations

import logging
import re
import secrets
from dataclasses import dataclass, field
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


@dataclass
class Mounted:
    """One logical file uploaded to Qoder and mounted into a session.

    ``parts`` has a single entry for an ordinary upload; for a chunked one it lists every
    piece in order, and ``mount_path`` is the reassembly target rather than a file that
    already exists in the sandbox.
    """

    file_id: str
    mount_path: str
    parts: list[tuple[str, str]] = field(default_factory=list)
    size_bytes: int = 0

    @property
    def chunked(self) -> bool:
        return len(self.parts) > 1


async def upload_and_mount(
    api: QoderAPI,
    session_id: str,
    contents: bytes,
    filename: str,
    *,
    metadata: dict | None = None,
) -> Mounted:
    """Upload a file to Qoder and mount it into the session, chunking it if it is too big.

    Qoder's multipart endpoint takes about 5 MB per request, but Telegram users can send up
    to 20 MB, so a larger file is uploaded in ``QODER_UPLOAD_CHUNK_BYTES`` pieces and the
    pieces are mounted side by side as ``<base>.part000``, ``<base>.part001``, ... The agent
    reassembles them with one ``cat``; the exact command rides along in the pointer text
    (inbound) or the resume message (restore), never in the agent's head.

    Raises ``QoderError`` on the first piece that fails. Pieces uploaded before that stay in
    the account as orphans — the same failure mode a single-piece upload already had, and
    the account's file list is not something a conversation depends on.
    """
    if len(contents) <= config.QODER_UPLOAD_MAX_BYTES:
        file_id = _uploaded_id(await api.upload(contents, filename, metadata=metadata), filename)
        mount_path = unique_mount_path(filename)
        await api.attach_file(session_id, file_id, mount_path)
        return Mounted(
            file_id=file_id,
            mount_path=mount_path,
            parts=[(file_id, mount_path)],
            size_bytes=len(contents),
        )

    base = unique_mount_path(filename)
    chunk_size = config.QODER_UPLOAD_CHUNK_BYTES
    total = -(-len(contents) // chunk_size)  # ceildiv, so partNNN naming needs no lookahead
    parts: list[tuple[str, str]] = []
    for index in range(total):
        chunk = contents[index * chunk_size:(index + 1) * chunk_size]
        part_meta = dict(metadata or {})
        part_meta.update({"part": index, "parts": total, "whole_filename": filename})
        part_name = f"{filename}.part{index:03d}"
        file_id = _uploaded_id(await api.upload(chunk, part_name, metadata=part_meta), part_name)
        part_path = f"{base}.part{index:03d}"
        await api.attach_file(session_id, file_id, part_path)
        parts.append((file_id, part_path))
    return Mounted(file_id=parts[0][0], mount_path=base, parts=parts, size_bytes=len(contents))


def _uploaded_id(uploaded: dict, name: str) -> str:
    file_id = uploaded.get("id")
    if not file_id:
        # Shaped as a QoderError so both callers (ingest, and the manager's resume restore,
        # which must never let one file abort the rebuild) handle it on a path they already have.
        raise QoderError(
            502, f"upload of {name} returned no file id", error_type="unexpected_shape"
        )
    return file_id


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

    try:
        remote = await tg_file.get_file()
        contents = await remote.download_as_bytearray()
    except Exception as exc:  # noqa: BLE001 - Telegram raises many types here
        log.warning("telegram download failed for %s: %s", filename, exc)
        raise IngestError(f"Telegram would not give me that file ({type(exc).__name__}).") from exc

    # The check above trusts Telegram's reported file_size, which can be missing (None -> 0)
    # or simply wrong, and download_as_bytearray itself is uncapped. The real bytes are the only
    # trustworthy bound, so re-check them: a missing size sailed past the pre-check entirely.
    # The Qoder-side 5 MB cap is no longer a rejection reason — upload_and_mount chunks
    # anything between it and Telegram's 20 MB download cap.
    actual = len(contents)
    if actual > config.TG_DOWNLOAD_MAX_BYTES:
        raise IngestError(
            f"that file turned out to be {actual // (1024 * 1024)} MB. Telegram only lets a bot "
            "download up to 20 MB."
        )

    metadata = {"convo_id": convo_id, "tg_user_id": tg_user_id, "source": "telegram"}
    try:
        mounted = await upload_and_mount(
            api, session_id, bytes(contents), filename, metadata=metadata
        )
    except QoderError as exc:
        log.warning("qoder upload/mount failed for %s (%s): %s", filename, exc.status, exc.message)
        raise IngestError(
            f"Qoder would not take that file ({exc.status}: {exc.message})."
        ) from exc

    auth.record_tg_file(
        db,
        file_id=mounted.file_id,
        owner_tg_user_id=tg_user_id,
        convo_id=convo_id,
        filename=filename,
        mime_type=mime_type,
        size_bytes=actual,
        mount_path=mounted.mount_path,
    )

    # Keep a local copy. The uploaded file belongs to whichever account the PAT pointed at, so
    # after a rotation it is unreachable; this copy is what lets a resumed conversation
    # re-mount the same file under the new account (re-chunking it on the way, if needed).
    # Best-effort: a full disk must not lose the user's message, which has already been
    # accepted by the API at this point.
    history.retain_file(
        db,
        convo_id=convo_id,
        file_id=mounted.file_id,
        contents=bytes(contents),
        filename=filename,
        owner_type="user",
    )

    return Ingested(
        file_id=mounted.file_id,
        mount_path=mounted.mount_path,
        filename=filename,
        mime_type=mime_type,
        size_bytes=actual,
        pointer_text=build_pointer_text(filename, mime_type, mounted, caption),
    )


def build_pointer_text(
    filename: str,
    mime_type: str | None,
    mounted: Mounted,
    caption: str | None = None,
) -> str:
    """What the agent actually receives.

    The agent cannot see the Telegram message, so the path has to be spelled out. Mentioning
    that Read decodes images is what makes "what is in this photo?" work without a nudge.
    For a chunked upload the reassembly command is spelled out the same way: an agent left
    to guess from ``.partNNN`` filenames alone sometimes Reads a part and reasons from a
    truncated file.
    """
    size = mounted.size_bytes
    size_label = f"{size / 1024:.0f} KB" if size < 1024 * 1024 else f"{size / (1024 * 1024):.1f} MB"
    lines = [
        f"The user attached a file: {filename}"
        + (f" ({mime_type}, {size_label})" if mime_type else f" ({size_label})"),
    ]
    if mounted.chunked:
        lines.append(
            f"It is larger than the {config.QODER_UPLOAD_MAX_BYTES // (1024 * 1024)} MB "
            f"per-file upload limit, so it was split into {len(mounted.parts)} parts, mounted "
            "in your sandbox at exactly:"
        )
        lines.extend(f"- {path}" for _, path in mounted.parts)
        lines.append("Reassemble it with a single command BEFORE using it:")
        lines.append(f"cat {mounted.mount_path}.part* > {mounted.mount_path}")
        lines.append(
            f"The result must be exactly {size} bytes; verify with `wc -c < {mounted.mount_path}` "
            "and treat any mismatch as a corrupt file."
        )
        lines.append(
            f"Then work with {mounted.mount_path} as the original file; ignore the parts."
        )
    else:
        lines.append(f"It is mounted in your sandbox at exactly: {mounted.mount_path}")
        lines.append(
            "Use the Read tool on that path to inspect it. Read decodes images natively, so you "
            "can see pictures directly; for other formats use Bash as needed."
        )
    if caption and caption.strip():
        lines.append("")
        lines.append("The user's message was:")
        lines.append(caption.strip())
    else:
        lines.append("")
        lines.append("The user sent no text with it. Inspect the file and tell them what it "
                     "contains, then ask what they would like done with it.")
    return "\n".join(lines)
