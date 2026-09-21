"""Bot entrypoint.

Runs Telegram long polling. Webhooks are not an option here: they need a public HTTPS
endpoint, and this bot runs on a phone behind carrier NAT.

Startup deliberately does not abort if Qoder provisioning fails. A dead process on a phone
gives no diagnostics at all, whereas a running bot can explain the problem through /health
and retry provisioning on the next /new.
"""

from __future__ import annotations

import asyncio
import logging
import sys

from telegram import Update
from telegram.ext import Application, ApplicationBuilder

from tgagent import handlers, runtime
from tgagent.auth import AccessControl
from tgagent.config import load_settings
from tgagent.db import Database
from tgagent.manager import Manager
from tgagent.qclient import QoderError
from tgagent.qsessions import QoderAPI

log = logging.getLogger("tgagent")


def configure_logging(level: str) -> None:
    logging.basicConfig(
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=getattr(logging, level, logging.INFO),
        stream=sys.stderr,
    )
    # httpx logs every request at INFO, which buries anything useful under SSE traffic.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("telegram.ext.Application").setLevel(logging.INFO)


async def post_init(app: Application) -> None:
    settings = app.bot_data["settings"]
    db = Database(settings.db_path)

    # A PAT rotated from Telegram (persisted to kv) overrides the environment bootstrap value.
    settings = runtime.resolve_pat(db, settings)
    app.bot_data["settings"] = settings

    # Runtime access control: the admin id is fixed from env, the allowlist is DB-backed and
    # mutable from Telegram. Seeded from settings.allowed_ids on first run.
    access = AccessControl(db, admin_id=settings.admin_id, seed_allowed=settings.allowed_ids)

    api, manager = await runtime.build_backend(app, settings, db)
    app.bot_data.update(
        db=db, access=access, api=api, manager=manager,
        swap_lock=asyncio.Lock(), startup_error=None,
    )

    me = await app.bot.get_me()
    log.info("connected to telegram as @%s (id %s)", me.username, me.id)

    if access.admin_id is not None:
        log.info("administrator: user id %s", access.admin_id)
    if access.discovery_mode:
        log.warning(
            "allowlist is empty: only the administrator can use the bot. Send it a message to "
            "discover a user id, then add it with /allow (or TG_ALLOWED_IDS in .env)."
        )
    else:
        log.info("allowlist: %d user(s)", len(access.snapshot()))

    try:
        await manager.startup()
    except QoderError as exc:
        # Keep running so /health can report this instead of the process just vanishing.
        app.bot_data["startup_error"] = exc.message
        log.error("Qoder provisioning failed: %s", exc)
    except Exception:  # noqa: BLE001
        app.bot_data["startup_error"] = "unexpected failure during startup"
        log.exception("startup failed")

    # Discoverability: / autocomplete + menu button next to input box.
    # Newbies don't have to read /help first.
    try:
        from telegram import BotCommand, MenuButtonCommands

        await app.bot.set_my_commands([
            BotCommand("start", "your user id, plus what this bot does"),
            BotCommand("help", "commands and capabilities"),
            BotCommand("new", "start a fresh conversation"),
            BotCommand("sessions", "switch between conversations"),
            BotCommand("stop", "cancel the current turn"),
            BotCommand("model", "list or change models"),
            BotCommand("effort", "reasoning effort for your model"),
            BotCommand("tools", "show tool activity"),
            BotCommand("files", "this conversation's files"),
            BotCommand("usage", "credit spend"),
            BotCommand("archive", "close this conversation"),
            BotCommand("health", "is everything connected?"),
        ])
        await app.bot.set_chat_menu_button(
            chat_id=None,
            menu_button=MenuButtonCommands(),
        )
    except Exception:  # noqa: BLE001 - setup shouldn't kill the bot
        log.warning("could not register menu buttons")


async def post_shutdown(app: Application) -> None:
    manager: Manager | None = app.bot_data.get("manager")
    if manager is not None:
        await manager.shutdown()

    api: QoderAPI | None = app.bot_data.get("api")
    if api is not None:
        await api.client.aclose()

    db: Database | None = app.bot_data.get("db")
    if db is not None:
        db.close()
    log.info("shutdown complete")


def main() -> int:
    try:
        settings = load_settings()
    except (RuntimeError, ValueError) as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2

    configure_logging(settings.log_level)
    log.info(
        "starting tgagent: model=%s max_streams=%d budget=%.0f db=%s",
        settings.default_model,
        settings.max_live_streams,
        settings.credit_budget,
        settings.db_path,
    )

    app = (
        ApplicationBuilder()
        .token(settings.tg_bot_token)
        .concurrent_updates(10)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )
    app.bot_data["settings"] = settings
    handlers.register(app)

    # drop_pending_updates=False is important: messages sent while the process was dead
    # (Android killed it) must still be processed when it comes back.
    app.run_polling(
        timeout=30,
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=False,
        close_loop=False,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
