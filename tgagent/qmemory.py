"""Per-user memory stores.

Memory is what makes a months-long conversation coherent without paying to resend its entire
history every turn — which is the real cost driver, since a long session's context is billed on
every single request.

Memories are not a tool the agent calls. The store mounts at
``/data/.qoder/awareness/<path>`` and the agent reads and edits those files like any other;
each change automatically creates an immutable version.

Isolation: a memory store is a genuine content boundary, but only if we attach exactly one
user's store to that user's sessions. Stores must be attached at session CREATION — the
add-resource endpoint accepts only type "file".
"""

from __future__ import annotations

import logging

from . import auth, config
from .db import Database
from .qclient import NotFound, QoderError, Unauthorized, iter_pages
from .qsessions import QoderAPI

log = logging.getLogger("tgagent.memory")

STORE_INSTRUCTIONS = (
    "Long-lived context about this user: their preferences, ongoing projects, people and "
    "tools they mention, and facts they have asked you to remember. These files are mounted "
    "at /data/.qoder/awareness/. Read them at the start of a task, and update them when you "
    "learn something durable. Keep them short and factual; they are notes, not a transcript. "
    "Never write secrets into them."
)


async def ensure_memory_store(
    api: QoderAPI, db: Database, tg_user_id: int, *, assume_valid: bool = False
) -> str | None:
    """Return this user's memory store id, creating it once on first use.

    ``assume_valid`` skips the existence check for a store the caller has already confirmed in
    this process life, so creating a second conversation does not pay for the same GET again.

    Returns None rather than raising: memory is an enhancement, and a conversation must still
    work if the store cannot be created.
    """
    user = auth.get_user(db, tg_user_id)
    if user is None:
        return None

    cached = user["memstore_id"]
    if cached and assume_valid:
        return cached
    if cached:
        if await _store_exists(api, cached):
            return cached
        # A rotated PAT or a deleted store leaves an id here that session creation rejects.
        # Returning it anyway would make the user unable to start ANY conversation, with no
        # way to recover from inside the bot, so drop it and fall through to provisioning.
        log.warning("cached memory store %s no longer exists; provisioning a fresh one", cached)
        _forget(db, tg_user_id)

    name = config.memstore_name(tg_user_id)
    async with api.provision_lock(tg_user_id):
        # Double-check the cache INSIDE the lock. The read above happened before we acquired it,
        # so a concurrent caller for the same user (a DM and a group topic opening at once) may
        # have provisioned the store while we waited. Without this re-read both callers would
        # fall through to list-and-create and mint duplicate stores, orphaning one remotely.
        refreshed = auth.get_user(db, tg_user_id)
        if refreshed and refreshed["memstore_id"]:
            return refreshed["memstore_id"]

        try:
            async for store in iter_pages(api.client, "/memory_stores"):
                if store.get("name") == name and not store.get("archived_at"):
                    _remember(db, tg_user_id, store["id"])
                    log.info("adopted existing memory store %s for user %s", store["id"], tg_user_id)
                    return store["id"]
        except Unauthorized as exc:
            # An auth failure is not "the list didn't work, try creating instead": the create
            # would fail the same way, and logging it as a fallthrough masked a dead or
            # scope-less PAT behind the creation error that followed.
            log.warning("could not list memory stores: credentials rejected (%s)", exc.message)
            return None
        except QoderError as exc:
            log.warning("could not list memory stores (%s); creating a new one", exc.message)

        try:
            created = await api.client.post(
                "/memory_stores",
                {"name": name, "description": f"Long-term memory for Telegram user {tg_user_id}"},
            )
        except QoderError as exc:
            log.warning("memory store creation failed for user %s: %s", tg_user_id, exc.message)
            return None

        store_id = created.get("id")
        if not store_id:
            log.warning("memory store response had no id: %s", created)
            return None

        _remember(db, tg_user_id, store_id)
        log.info("created memory store %s for user %s", store_id, tg_user_id)
        return store_id


def _remember(db: Database, tg_user_id: int, store_id: str) -> None:
    db.execute(
        "UPDATE users SET memstore_id = ? WHERE tg_user_id = ?",
        (store_id, tg_user_id),
    )


def _forget(db: Database, tg_user_id: int) -> None:
    db.execute("UPDATE users SET memstore_id = NULL WHERE tg_user_id = ?", (tg_user_id,))


async def _store_exists(api: QoderAPI, store_id: str) -> bool:
    """Whether the cached store is still there.

    Only a definitive 404 counts as gone. A transport error or a 5xx is not proof, and
    discarding the id on a network blip would silently orphan everything the user had asked
    the bot to remember — losing memory is worse than one failed session creation.
    """
    try:
        await api.client.get(f"/memory_stores/{store_id}")
        return True
    except NotFound:
        return False
    except QoderError as exc:
        log.warning("could not verify memory store %s (%s); keeping it", store_id, exc.message)
        return True
