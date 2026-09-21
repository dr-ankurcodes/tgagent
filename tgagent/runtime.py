"""Qoder backend lifecycle: building it at boot and hot-swapping the PAT at runtime.

``post_init`` and the administrator's ``/setpat`` command both need to construct the same object
graph — ``QoderClient`` -> ``QoderAPI`` -> ``Manager`` — and the swap has to tear the old one down
and rebuild it without ever leaving two live managers touching the same conversation rows. That
logic lives here rather than in handlers or __main__ so neither imports the other: __main__ wires
the app, handlers answer updates, and both call into this module.

The PAT set from Telegram is persisted to the ``qoder_pat`` kv key. On the next boot __main__
reads that key and lets it override the TG_ALLOWED_IDS-style environment seed, exactly as the
runtime allowlist overrides TG_ALLOWED_IDS — so a rotation survives a restart without the bot
ever rewriting its own .env.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging

from telegram.ext import Application

from .config import Settings
from .db import Database
from .manager import Manager
from .qclient import QoderClient, QoderError
from .qsessions import QoderAPI

log = logging.getLogger("tgagent.runtime")

# kv key holding the runtime PAT. Its presence overrides the environment value at boot.
KV_QODER_PAT = "qoder_pat"


def resolve_pat(db: Database, settings: Settings) -> Settings:
    """Return settings with the PAT overridden by the stored one, if the administrator set it.

    Called once at boot before any client is built. The environment value is the bootstrap: it
    gets the bot online the first time and after a database is wiped, but a token rotated from
    Telegram takes precedence from then on.
    """
    stored = db.kv_get(KV_QODER_PAT)
    if stored and stored != settings.qoder_pat:
        log.info("using the PAT stored at runtime; it overrides the environment value")
        return dataclasses.replace(settings, qoder_pat=stored)
    return settings


async def build_backend(app: Application, settings: Settings, db: Database) -> tuple[QoderAPI, Manager]:
    """Construct the QoderAPI and Manager for a credential. Pure construction; starts nothing."""
    client = QoderClient(settings.qoder_api_base, settings.qoder_pat)
    api = QoderAPI(client, db)
    manager = Manager(db=db, api=api, settings=settings, bot=app.bot)
    return api, manager


async def validate_pat(settings: Settings, pat: str) -> None:
    """Prove a candidate PAT authenticates before anything is torn down for it.

    A cheap authenticated read (``GET /models``). Raises ``Unauthorized`` for a bad token and
    ``QoderError`` for any other rejection, so the caller can refuse the swap and keep the
    current credential. The probe client is closed on every path — a rejected token must not
    leak a connection pool.
    """
    probe = QoderClient(settings.qoder_api_base, pat)
    try:
        await probe.get("/models")
    finally:
        await probe.aclose()


async def hot_swap_pat(app: Application, new_pat: str) -> None:
    """Validate ``new_pat``, then move the whole backend onto it without a restart.

    Ordering is the safety property here:

    1. Validate with a throwaway client. A bad token changes nothing.
    2. Take the backend offline (bot_data manager/api -> None). Concurrent updates now get the
       honest "still starting up, try again shortly" reply rather than grabbing a half-swapped
       manager. There is never a moment where two live managers adopt the same conversation.
    3. Shut the old manager down and close its client.
    4. Persist the token, replace settings, build the new backend, and run its startup — which
       provisions the environment under the new account and reconciles. Every session the OLD
       token created now 404s, so reconcile retires those conversations as ``lost_session``;
       they rebuild from the on-device transcript on the next message, which is the documented
       PAT-rotation recovery.

    The swap lock serialises two concurrent /setpat calls so the second cannot start tearing down
    what the first just built.
    """
    settings: Settings = app.bot_data["settings"]
    db: Database = app.bot_data["db"]

    # 1. Validate before touching live state.
    await validate_pat(settings, new_pat)

    lock: asyncio.Lock = app.bot_data.setdefault("swap_lock", asyncio.Lock())
    async with lock:
        old_manager: Manager | None = app.bot_data.get("manager")
        old_api: QoderAPI | None = app.bot_data.get("api")

        # 2. Offline first.
        app.bot_data["manager"] = None
        app.bot_data["api"] = None

        # 3. Tear down the old backend.
        try:
            if old_manager is not None:
                await old_manager.shutdown()
        finally:
            if old_api is not None:
                await old_api.client.aclose()

        # 4. Persist, swap settings, rebuild, re-provision.
        db.kv_set(KV_QODER_PAT, new_pat)
        new_settings = dataclasses.replace(settings, qoder_pat=new_pat)
        app.bot_data["settings"] = new_settings

        api, manager = await build_backend(app, new_settings, db)
        app.bot_data.update(api=api, manager=manager, startup_error=None)
        try:
            await manager.startup()
        except QoderError as exc:
            # Mirror post_init: keep running so /health can explain it rather than leaving the
            # bot with no backend and no diagnostic. The token is already persisted and valid,
            # so a retry on the next /new (or a restart) will re-provision.
            app.bot_data["startup_error"] = exc.message
            log.error("re-provisioning after PAT swap failed: %s", exc)
        log.info("swapped to a new Qoder PAT and rebuilt the backend")
