"""Telegram update handlers and commands.

Every handler goes through auth.is_allowed first, and every conversation lookup goes through
auth.py so ownership is always checked. Handlers never touch the database's conversation
table directly.

Two commands — /new and /stop — post through the conversation's renderer, because they have a
conversation to post to and the renderer is the single writer for it. The rest reply directly
with a NEW message, which cannot race the renderer: the single-writer invariant is about two
tasks editing one message, and a command reply never edits anything the renderer owns.
"""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import time

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Message, Update
from telegram.constants import ParseMode
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

from . import auth, budget, config, history, runtime, tg_html, uploads
from .auth import AccessControl
from .convo import describe_row
from .db import Database
from .manager import Manager, model_choices_text
from .qclient import BillingError, QoderError, Unauthorized
from .qsessions import conversation_title
from .uploads import IngestError

log = logging.getLogger("tgagent.handlers")

HELP_TEXT = """An AI agent you drive from Telegram.

Just send a message and it works: writing code, researching the web, building PowerPoint
decks, generating images, reading files you send.

<b>Commands</b>
<code>/start</code> — get your user ID and help text
<code>/new</code> — start a fresh conversation
<code>/sessions</code> — switch between your conversations
<code>/stop</code> — cancel the turn in progress
<code>/model</code> — list models; <code>/model qmodel</code> to switch
<code>/effort</code> — reasoning effort; <code>/effort low</code> costs far less than <code>max</code>
<code>/tools</code> — show or hide tool activity
<code>/files</code> — files sent in this conversation
<code>/usage</code> — credit spend
<code>/archive</code> — close this conversation
<code>/health</code> — is everything connected?

A session keeps the model and effort it was created with, so /model and /effort take effect
from your next <code>/new</code>.

Send a document or photo with an optional caption and the agent can read it. Files it
produces are delivered back to you here.

Conversations are private to you. Long idle conversations may lose their sandbox files after
24 hours, but anything already sent to you here is safe.

If the cloud session behind a conversation is ever lost — which is what rotating the API
token does to every session the old one created — the bot rebuilds that conversation from the
transcript kept on its own device and carries over anything you had queued. You will see a
notice saying so; nothing you were told is lost, and you do not have to retype anything."""


def _safe_cb_data(*parts: str | int) -> str:
    """Encode data into callback_data without exceeding Telegram's 64-byte cap.

    The pattern is ``<action>:<local_int>``, e.g. ``sw:12``. The local int is our internal
    convo_id, not the long Qoder session id, so it always fits. on_callback splits on the
    first colon and requires the second field to be numeric, so the action must come first.
    """
    return ":".join(str(p) for p in parts)[: config.TG_CALLBACK_DATA_LIMIT]


def _thread_id(message) -> int | None:
    """The forum topic a message belongs to, or None outside a real topic.

    Gated on ``is_topic_message``, not on ``message_thread_id`` merely being set. In a
    NON-forum group, replying to a message ALSO populates ``message_thread_id`` — with the id
    of the message being replied to — and that value is not a topic. Treating it as one had two
    effects: it keyed a brand-new conversation off every reply-to-message, silently fragmenting
    one conversation into several, and the bogus id was then handed back to ``send_message`` as
    a thread that does not exist.

    ``getattr`` throughout: this is called with an ``InaccessibleMessage`` behind an expired
    inline button, and from the error handler, where the update may not be an ``Update`` at all.
    """
    if message is None:
        return None
    if not getattr(message, "is_topic_message", False):
        return None
    return getattr(message, "message_thread_id", None)


def _cb_origin(query) -> tuple[int, int | None] | None:
    """The chat a callback button's message lives in, plus its topic if any.

    Reads ``message.chat.id`` rather than ``message.chat_id`` on purpose. Telegram replaces an
    old message behind a button with an ``InaccessibleMessage``, which in PTB 22.8 has neither
    a ``chat_id`` nor a ``message_thread_id`` attribute — accessing them raises AttributeError
    and takes the whole handler down. ``chat`` is always present on both types.
    """
    message = query.message
    chat = getattr(message, "chat", None)
    if chat is None:
        return None
    return chat.id, _thread_id(message)


def _manager(context: ContextTypes.DEFAULT_TYPE) -> Manager | None:
    return context.application.bot_data.get("manager")


def _access(context: ContextTypes.DEFAULT_TYPE) -> AccessControl:
    """The runtime access authority. Always present once post_init has run."""
    return context.application.bot_data["access"]


class _RejectThrottle:
    """How often a non-allowed user is answered — and therefore how often it is logged.

    One answer per user per ``config.REJECT_REPLY_INTERVAL_S``. Two channels use a throttle of
    this shape, each with its own instance: the message a stranger is sent, and the alert popped
    up when they tap a button. Without one, anyone who finds the bot can make it issue one
    Telegram API call per message they send, which burns the bot's global send budget and gets
    it flood-banned: a stranger could take the bot offline for the people it exists to serve.

    Keyed on the USER, not the chat, because the user is what an attacker controls — one key per
    chat would let a single account spam every group the bot belongs to. The cost is that someone
    rejected in one chat is not answered again in another until the interval passes, which is the
    right trade: they already have the only thing the reply tells them.

    In memory, not SQLite. The only state worth keeping is "did we answer this person a moment
    ago", and a restart clearing it is harmless — Android kills this process often and a spammer
    cannot make it restart. Staying out of the database also means a flood of rejected messages
    costs no writes at all.

    A refused call does NOT refresh the clock, so a sustained flood cannot keep pushing its own
    next answer further away and then collect a fresh one the moment it pauses.
    """

    def __init__(self) -> None:
        self._answered: dict[int, float] = {}

    def allow(self, user_id: int) -> bool:
        """Whether this user may be answered now. Records it when the answer is allowed."""
        now = time.monotonic()
        last = self._answered.get(user_id)
        if last is not None and now - last < config.REJECT_REPLY_INTERVAL_S:
            return False
        self._answered[user_id] = now
        if len(self._answered) > config.REJECT_TRACKED_MAX:
            self._prune(now)
        return True

    def _prune(self, now: float) -> None:
        """Drop keys whose interval has expired anyway, then the oldest until the ceiling holds.

        Evicting a key that is still live only costs that user one extra reply, so oldest-first
        is a safe fallback and the loop cannot spin.
        """
        for stale in [
            key for key, at in self._answered.items()
            if now - at >= config.REJECT_REPLY_INTERVAL_S
        ]:
            del self._answered[stale]
        while len(self._answered) > config.REJECT_TRACKED_MAX:
            del self._answered[next(iter(self._answered))]


# One instance per channel, not one shared. They answer different things — _rejects bounds the
# message SENT to a stranger, _denials bounds the ALERT shown when they tap a button — and a
# shared key space would let a message rejection consume a user's alert, so they would tap a
# dead button and never be told why. Each is bounded independently by REJECT_TRACKED_MAX.
_rejects = _RejectThrottle()
_denials = _RejectThrottle()


def _denied_text(user_id: int, *, discovery: bool) -> str:
    """What a non-allowed user is told. Both variants include their own id.

    The id is not a secret — it is theirs, and any bot on Telegram will tell it to them — and
    without it an administrator has to go digging through the logs to allowlist someone who has
    just asked for access. ``discovery`` is an empty allowlist, where nobody has been configured
    yet and the id is the whole point of the reply.
    """
    id_block = f"Your Telegram user id is:\n\n<code>{user_id}</code>\n\n"
    if discovery:
        return (
            "This bot is not configured yet, so it is not accepting messages.\n\n"
            f"{id_block}"
            "Add it to TG_ALLOWED_IDS in .env and restart."
        )
    return (
        "Sorry, access to this bot is restricted to authorized users only.\n\n"
        f"{id_block}"
        "If you believe you should have access, send that id to the administrator."
    )


async def _deny_callback(query, user) -> None:
    """Answer a button press from a non-allowed user, showing the alert at most once a minute.

    ``query.answer()`` itself is NOT optional and is never suppressed: without it Telegram leaves
    the button showing a spinning clock, so the user has no way to tell the tap was refused from
    the bot having hung. Only the ``show_alert`` modal is throttled, because that is the part that
    costs an outbound API call per tap.

    Separate from :func:`_reject` because the two channels answer different things and must not
    spend each other's budget — see the note on the throttle instances.

    A stranger can only reach a button on a message the bot already sent, so in practice this is
    someone who was allowlisted, received a /sessions menu, and was later removed. They get told
    why once, and after that their dead buttons simply stop spinning.
    """
    if user is not None and _denials.allow(user.id):
        log.warning("rejected a callback from unauthorized user id=%s", user.id)
        await query.answer("Not authorized", show_alert=True)
    else:
        await query.answer()


async def _reject(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Return True if this update should be ignored.

    The log line is tied to the reply, so one throttle bounds both: a flood cannot fill the
    rotating log either. The FIRST rejection still logs the id at warning level, which is the
    one an administrator needs, and repeats drop to debug.
    """
    user = update.effective_user
    access = _access(context)

    if access.is_allowed(user.id if user else None):
        return False

    if user is None or update.effective_chat is None:
        return True

    if _rejects.allow(user.id):
        log.warning("rejected update from unauthorized user id=%s", user.id)
        await _reply(
            update, _denied_text(user.id, discovery=access.discovery_mode)
        )
    else:
        log.debug("already answered user id=%s; ignoring a repeat", user.id)
    return True


# --- commands -----------------------------------------------------------------------


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await _reject(update, context):
        return
    user = update.effective_user
    db = context.application.bot_data["db"]
    settings = context.application.bot_data["settings"]

    # ensure_user must run before the UPDATE below: against a row that does not exist yet the
    # UPDATE affects nothing, which is what left last_id_request NULL and crashed the next /start.
    auth.ensure_user(db, settings, user.id, user.username)

    if _should_show_id(db, user.id):
        db.execute(
            "UPDATE users SET last_id_request = ? WHERE tg_user_id = ?",
            (int(time.time()), user.id),
        )
        await _reply(update, f"Your Telegram user id: <code>{user.id}</code>")

    await _reply(update, HELP_TEXT)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await _reject(update, context):
        return
    await _reply(update, HELP_TEXT)


async def cmd_new(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await _reject(update, context):
        return
    manager = _manager(context)
    if manager is None:
        await _reply(update, "The bot is still starting up. Try again in a few seconds.", error=True)
        return

    user = update.effective_user
    chat = update.effective_chat
    title_arg = " ".join(context.args) if context.args else None
    try:
        convo = await manager.create(
            tg_user_id=user.id,
            chat_id=chat.id,
            message_thread_id=_thread_id(update.effective_message),
            first_text=title_arg or config.DEFAULT_CONVO_TITLE,
            handle=user.username,
        )
    except BillingError as exc:
        await _reply(
            update,
            tg_html.escape(budget.exhausted_notice(
                exc.message,
                blocked="You have no available credit to start a new conversation.",
            )),
            error=True,
        )
        return
    except QoderError as exc:
        await _reply(
            update,
            f"Could not start a new conversation: {tg_html.escape(exc.message)}",
            error=True,
        )
        return

    db = context.application.bot_data["db"]
    lines = [f"Started a new conversation: {convo.title or 'untitled'}"]

    # Name the model and effort this conversation will actually run on. Resolved from the stored
    # preference plus the catalog rather than from what was requested, because model_ref drops an
    # effort the model does not advertise — the user should see the outcome, not the intention.
    detail, warning = await _model_and_effort_line(manager, db, user.id, convo.model_id)
    if detail:
        lines.append(detail)
    if warning:
        lines.append(f"⚠ {warning}")

    # /new deliberately starts fresh rather than auto-resuming, so someone who runs it just
    # after a PAT rotation lands in a blank conversation with no hint that the old one is still
    # on the device. Point at it — this is the moment they are most likely to want it.
    recoverable = auth.list_recoverable_conversations(db, user.id)
    if recoverable:
        lines.append(
            f"You also have {len(recoverable)} earlier conversation(s) whose cloud session was "
            "lost. Their history is still on this device — use /sessions and tap ⟳ RESTORE to "
            "pick one back up."
        )
    convo.notice("\n".join(lines))


async def cmd_stop(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await _reject(update, context):
        return
    convo, _ = await _active(update, context)
    if convo is None:
        return
    await convo.request_stop()


async def cmd_sessions(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await _reject(update, context):
        return
    manager = _manager(context)
    db = context.application.bot_data["db"]
    user = update.effective_user

    if manager is None:
        await _reply(update, "The bot is still starting up. Try again shortly.", error=True)
        return

    rows = auth.list_conversations(db, user.id)
    # Conversations whose cloud session became unreachable (a rotated PAT). These are excluded
    # from list_conversations because retiring them sets deleted_at, so they need their own
    # query — otherwise they vanish from view and the user has no way to get them back.
    recoverable = auth.list_recoverable_conversations(db, user.id)

    if not rows and not recoverable:
        await _reply(update, "No conversations yet. Send a message to start one.")
        return

    keyboard: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton("➕ New chat", callback_data=_safe_cb_data("new", 0))]
    ]

    # Add existing conversations up to 30 (button count limit). The active one gets an
    # explicit label as well as describe_row's ● marker: a dot alone is easy to miss, and
    # button labels are plain text so markup like *bold* would show up literally.
    for row in rows[:29]:
        label = describe_row(row)
        if row["active"]:
            label = f"▸ ACTIVE — {label}"
        keyboard.append([
            InlineKeyboardButton(label[:64], callback_data=_safe_cb_data("sw", row["convo_id"]))
        ])

    for row in recoverable[: max(0, 29 - len(rows))]:
        label = f"RESTORE — {describe_row(row)}"
        keyboard.append([
            InlineKeyboardButton(label[:64], callback_data=_safe_cb_data("rc", row["convo_id"]))
        ])

    header = (
        "<b>Your conversations</b>\nTap one to switch.\n"
        "<i>Times are IST — when each was last used.</i>"
    )
    if recoverable:
        header += (
            f"\n\n<i>{len(recoverable)} conversation(s) lost their cloud session and can be "
            "restored from local history — tap RESTORE.</i>"
        )

    await _reply(update, header, reply_markup=InlineKeyboardMarkup(keyboard))


async def cmd_model(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await _reject(update, context):
        return
    manager = _manager(context)
    if manager is None:
        await _reply(update, "The bot is still starting up. Try again in a few seconds.", error=True)
        return
    db = context.application.bot_data["db"]
    settings = context.application.bot_data["settings"]
    user = update.effective_user

    try:
        models = await manager.api.list_models(use_cache=not context.args)
    except QoderError as exc:
        await _reply(update, f"Could not list models: {tg_html.escape(exc.message)}", error=True)
        return

    # ensure_user, not get_user: without a row the UPDATE below would affect nothing and the
    # user's choice would be silently lost.
    user_row = auth.ensure_user(db, settings, user.id, user.username)
    current = user_row["model_id"] or config.DEFAULT_MODEL

    if not context.args:
        await _reply(update, model_choices_text(models, current))
        return

    wanted = context.args[0].strip()
    chosen = next((m for m in models if m.get("id") == wanted), None)
    if chosen is None:
        await _reply(update, f"There is no model called <code>{tg_html.escape(wanted)}</code>.", error=True)
        return
    if not chosen.get("is_enabled", True):
        # pick_model would silently substitute the default for a disabled id, so we would tell
        # the user their choice took effect while the agent runs on something else — and
        # users.model_id would record a model no session ever uses.
        await _reply(
            update,
            f"<code>{tg_html.escape(wanted)}</code> is not available on this account right now.",
            error=True,
        )
        return

    db.execute("UPDATE users SET model_id = ? WHERE tg_user_id = ?", (wanted, user.id))

    # A session takes its model from its agent, and the API offers no per-session override, so
    # this update is what makes the choice real. It reaches only THIS user's agent. The verb is
    # PUT, not PATCH — agents advertise GET/POST/PUT/DELETE and PATCH answers 405 (README
    # section 12) — and PUT replaces the whole object, so set_agent_model reads the agent back
    # and re-sends it with only the model changed.
    try:
        applied = await manager.api.apply_agent_model(user.id, wanted)
    except QoderError as exc:
        # A concurrent change (e.g. running /effort at the same time) returns 409 "Version conflict".
        # Don't show this raw exception as an error — it's not a user problem, just two updates
        # clashing. The model is saved locally; next time they run /new it will take effect from
        # a clean state.
        if exc.status == 409:
            log.debug("model update hit a version conflict (%s); saving anyway", exc.message)
        else:
            await _reply(
                update,
                f"Saved <code>{tg_html.escape(wanted)}</code>, but could not apply it to your agent "
                f"yet: {tg_html.escape(exc.message)}. It will be applied when you next use /new.",
                error=True,
            )
            return

    # What effort this model will actually run at. A stored preference is never cleared by a
    # switch, and model_ref drops any level the new model does not advertise — so without this
    # line the user can move from `low` onto a model whose default is `max` and be told nothing.
    description, warning = _effort_outcome(chosen, user_row["effort"])
    body = (
        f"Model set to <code>{tg_html.escape(wanted)}</code> · "
        f"{tg_html.escape(description)}.\n"
    )
    if warning:
        body += f"\n⚠ {tg_html.escape(warning)}\n"
    body += (
        "<i>This conversation keeps the model it started with, since a session's model is "
        "fixed when it is created. Use /new to run on the new one.</i>"
        if applied
        else "<i>It will be used from your next conversation.</i>"
    )
    await _reply(update, body)


async def cmd_effort(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show or set this user's reasoning effort.

    Effort is a field of the agent's ``model`` object, so like /model it only takes effect from
    the next /new — a session keeps the configuration it was created with.

    Validated against the CURRENT model's own ``efforts`` array rather than against the global
    level list, because the API rejects a level a model does not advertise with
    ``400 Field 'model.effort' is not supported by model '<id>'``. Six of the eighteen current
    models advertise none at all, and each model offers a different subset — ``gmodel`` takes
    low/high/max but not medium, while ``qmodel_38max`` takes low/medium/xhigh but not high.
    """
    if await _reject(update, context):
        return
    manager = _manager(context)
    if manager is None:
        await _reply(update, "The bot is still starting up. Try again in a few seconds.", error=True)
        return
    db = context.application.bot_data["db"]
    settings = context.application.bot_data["settings"]
    user = update.effective_user

    # ensure_user, not get_user: this may be a newly allowlisted user's first command.
    user_row = auth.ensure_user(db, settings, user.id, user.username)
    model_id = user_row["model_id"] or config.DEFAULT_MODEL
    stored_effort = user_row["effort"]

    try:
        models = await manager.api.list_models(use_cache=not context.args)
    except QoderError as exc:
        await _reply(update, f"Could not list models: {tg_html.escape(exc.message)}", error=True)
        return

    model = next((m for m in models if m.get("id") == model_id), None)
    if model is None:
        # The recorded model is no longer offered, so there is nothing to validate against.
        await _reply(
            update,
            f"Your selected model <code>{tg_html.escape(model_id)}</code> is no longer in the "
            "catalog, so its effort levels cannot be checked. Choose another with /model first.",
            error=True,
        )
        return

    allowed = model.get("efforts") or []
    code_model = f"<code>{tg_html.escape(model_id)}</code>"

    if not context.args:
        name = tg_html.escape(str(model.get("display_name") or model_id))
        lines = [f"<b>Reasoning effort</b> — {code_model} ({name})"]
        if allowed:
            lines.append("\nLevels it offers: " + ", ".join(f"<code>{lv}</code>" for lv in allowed))
            lines.append(
                "Platform default: <code>"
                f"{tg_html.escape(str(model.get('default_effort') or 'unspecified'))}</code>"
            )
        else:
            lines.append("\nThis model has no adjustable effort — the API rejects every level for it.")
        if stored_effort:
            applies = "in effect" if stored_effort in allowed else "ignored by this model"
            lines.append(f"Your setting: <code>{tg_html.escape(stored_effort)}</code> ({applies})")
        else:
            lines.append("Your setting: none — the platform default above is used.")
        if allowed:
            lines.append(
                "\nUsage: <code>/effort &lt;level&gt;</code>. Takes effect from your next /new."
            )
        await _reply(update, "\n".join(lines))
        return

    wanted = context.args[0].strip().lower()
    if not allowed:
        await _reply(
            update,
            f"{code_model} does not support reasoning effort, so any level would be rejected. "
            "Switch to a model that offers some — /model lists them.",
            error=True,
        )
        return
    if wanted not in allowed:
        # Distinguish a real level this model lacks from a word that is not a level at all.
        detail = (
            "That is a real level, just not one this model supports."
            if wanted in config.EFFORT_LEVELS
            else f"Valid levels are: {', '.join(config.EFFORT_LEVELS)}."
        )
        await _reply(
            update,
            f"{code_model} does not offer <code>{tg_html.escape(wanted)}</code>. {detail}\n"
            f"Available for it: {', '.join(f'<code>{lv}</code>' for lv in allowed)}",
            error=True,
        )
        return

    db.execute("UPDATE users SET effort = ? WHERE tg_user_id = ?", (wanted, user.id))
    try:
        applied = await manager.api.apply_agent_model(user.id, model_id)
    except QoderError as exc:
        # A concurrent change (e.g. running /model at the same time) returns 409 "Version conflict".
        # Don't show this raw exception as an error — it's not a user problem, just two updates
        # clashing. The effort is saved locally; next time they run /new it will take effect from
        # a clean state.
        if exc.status == 409:
            log.debug("effort update hit a version conflict (%s); saving anyway", exc.message)
        else:
            await _reply(
                update,
                f"Saved <code>{tg_html.escape(wanted)}</code>, but could not apply it to your agent "
                f"yet: {tg_html.escape(exc.message)}. It will be applied when you next use /new.",
                error=True,
            )
            return

    if not applied:
        await _reply(
            update,
            f"Effort set to <code>{tg_html.escape(wanted)}</code> for your next conversation.",
        )
        return

    # Read back what the agent actually stores. Reporting the write as success without checking
    # is how a model choice can be recorded locally while the agent runs on something else.
    ref = _agent_model_ref(db, user.id)
    actual = ref.get("effort") if isinstance(ref, dict) else None
    if actual == wanted:
        await _reply(update, (
            f"Effort set to <code>{tg_html.escape(wanted)}</code> on {code_model}.\n"
            "<i>This conversation keeps the setting it started with. Use /new to run at the "
            "new one.</i>"
        ))
    else:
        await _reply(
            update,
            f"Saved <code>{tg_html.escape(wanted)}</code>, but your agent reports "
            f"<code>{tg_html.escape(str(actual or 'no effort'))}</code>. "
            "Use /new, then /health to check.",
            error=True,
        )


async def cmd_tools(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Show or hide the tool-activity message.

    This also governs the renderer's ``thinking…`` marker, which is activity feedback of the same
    kind and lives in the same status message. It deliberately does NOT govern the context
    compaction banner: hiding tool activity is not a request to be kept ignorant of a lossy change
    to conversation state.

    There is still no /think counterpart. The Qoder API emits an ``agent.thinking`` event that
    carries no ``content`` and no deltas — verified live across qmodel_38max, dfmodel, gmodel and
    ultimate with explicit effort settings — so there is no reasoning text to display. A toggle
    that could only ever say "this does nothing" was removed rather than left in the command menu.
    See README sections 13 and 15.
    """
    if await _reject(update, context):
        return
    db = context.application.bot_data["db"]
    settings = context.application.bot_data["settings"]
    user = update.effective_user
    # ensure_user, not get_user: this may be the first command a newly allowlisted user
    # sends, and subscripting a missing row crashed the handler.
    row = auth.ensure_user(db, settings, user.id, user.username)
    new_value = 0 if row["show_tools"] else 1
    db.execute("UPDATE users SET show_tools = ? WHERE tg_user_id = ?", (new_value, user.id))

    # Every live conversation this user has, not just the active one. Updating only the active
    # one left the others rendering tool activity until they happened to be rebuilt, so the
    # toggle appeared not to stick after switching with /sessions.
    manager = _manager(context)
    if manager is not None:
        manager.set_show_tools(user.id, bool(new_value))

    await _reply(update, f"Tool activity display is now <b>{'on' if new_value else 'off'}</b>.")


async def cmd_files(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await _reject(update, context):
        return
    db = context.application.bot_data["db"]
    convo, _ = await _active(update, context)
    if convo is None:
        return

    sent = db.query(
        "SELECT filename, mime_type, size_bytes FROM tg_files WHERE convo_id = ? ORDER BY created_at DESC LIMIT 20",
        (convo.convo_id,),
    )
    made = db.query(
        """SELECT filename, size_bytes, delivered, skipped_reason FROM artifacts
           WHERE convo_id = ? ORDER BY discovered_at DESC LIMIT 20""",
        (convo.convo_id,),
    )

    if not sent and not made:
        await _reply(update, "No files in this conversation yet.")
        return

    lines = ["<b>Files in this conversation</b>"]
    if sent:
        lines.append("\n<i>You sent:</i>")
        for row in sent:
            lines.append(f"  • {tg_html.escape(row['filename'] or '?')} ({_size(row['size_bytes'])})")
    if made:
        lines.append("\n<i>The agent produced:</i>")
        for row in made:
            state = "sent" if row["delivered"] else (row["skipped_reason"] or "pending")
            lines.append(
                f"  • {tg_html.escape(row['filename'] or '?')} ({_size(row['size_bytes'])}) — {tg_html.escape(str(state))}"
            )
    await _reply(update, "\n".join(lines))


async def cmd_usage(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await _reject(update, context):
        return
    db = context.application.bot_data["db"]
    user = update.effective_user
    await _reply(update, budget.usage_report(db, user.id))


async def cmd_archive(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await _reject(update, context):
        return
    manager = _manager(context)
    convo, _ = await _active(update, context)
    if convo is None or manager is None:
        return
    if not await manager.archive(convo.convo_id, convo.tg_user_id):
        await _reply(update, "That conversation is no longer available.", error=True)
        return
    await _reply(update, (
        f"Conversation <b>{tg_html.escape(convo.title or 'untitled')}</b> archived.\n\n"
        "<i>To start fresh:</i>\n• Send <code>/new</code> for a new chat\n"
        "• Or just type your first question!"
    ))


async def cmd_health(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await _reject(update, context):
        return
    manager = _manager(context)
    db = context.application.bot_data["db"]
    settings = context.application.bot_data["settings"]
    user = update.effective_user
    lines = ["<b>Health</b>"]

    # post_init stores a provisioning failure here precisely so this command can explain it —
    # a bot that keeps running with no environment is otherwise indistinguishable from one
    # that is merely slow. It was never read, so the failure went unreported.
    startup_error = context.application.bot_data.get("startup_error")
    if startup_error:
        lines.append(
            f"startup: <b>FAILED</b> — {tg_html.escape(str(startup_error))}\n"
            "<i>Provisioning will be retried on your next /new.</i>"
        )

    if manager is None:
        lines.append("manager: <b>not started</b>")
        await _reply(update, "\n".join(lines))
        return

    # A fingerprint, not the token or a substring of it. This deployment rotates its PAT
    # often, and after a rotation every session the old token created returns 404 — being able
    # to see which credential is loaded turns that from a guessing game into a glance.
    lines.append(f"credentials: <code>{_pat_fingerprint(settings.qoder_pat)}</code>")
    lines.append(f"environment: <code>{'ok' if manager.env_id else 'MISSING'}</code>")

    agent_id = manager.api.agent_for_user(user.id)
    lines.append(
        f"your agent: <code>{tg_html.escape(agent_id) if agent_id else 'not created yet'}</code>"
    )
    agent_model = _agent_model(db, user.id)
    if agent_model != "?":
        lines.append(f"agent model: <code>{tg_html.escape(agent_model)}</code>")

    # Fetched here, above the effort line, because resolving an effort needs the model's own
    # ``efforts`` array and only the catalog carries it. A failure costs the effort detail and
    # is reported on its own line rather than discarding the whole report.
    try:
        models = await manager.api.list_models(use_cache=False)
    except QoderError as exc:
        models = []
        api_line = f"qoder api: <b>FAILING</b> — {tg_html.escape(exc.message)}"
    else:
        api_line = f"qoder api: <code>ok ({len(models)} models)</code>"

    # Per-user toggles that have no visible effect otherwise. effort shows the level in effect
    # for the current model, or "no adjustable effort on this model" when it advertises none.
    #
    # ensure_user, not get_user: /health may be the first command a newly allowlisted user
    # sends, and there is no row to read yet. Subscripting update.effective_user instead — a
    # telegram.User, which has none of these columns — raised on every single /health.
    user_row = auth.ensure_user(db, settings, user.id, user.username)
    lines.append(
        f"effort: <code>{tg_html.escape(_effort_description(db, models, user.id))}</code>"
        f" · budget: <code>{budget.budget_label(user_row['credit_budget'])}</code>"
        f" · tools: <code>{'on' if user_row['show_tools'] else 'off'}</code>"
    )

    lines.append(api_line)

    # Read-only. _active() would ADOPT the conversation — three tasks and an SSE stream —
    # which is not a side effect a diagnostic command should have.
    chat = update.effective_chat
    message = update.effective_message
    thread_id = _thread_id(message)
    crow = auth.active_conversation(db, user.id, chat.id, thread_id)
    if crow is not None:
        live = manager.get(crow["convo_id"])
        status = live.run_status() if live else (crow["run_status"] or "idle")
        lines.append(
            f"this conversation: <code>{tg_html.escape(status)}</code>"
            f" · session <code>{tg_html.escape(crow['session_id'] or 'none')}</code>"
            + ("" if live else " <i>(not live in this process)</i>")
        )
        # The model the session is ACTUALLY running, read from the session rather than from our
        # own row. A session keeps the model it was created with — /model only takes effect on
        # the next /new — so this is the figure being billed, and the one to compare against the
        # console when the two disagree.
        #
        # Guarded: this is the last network call /health makes, after most of the report is
        # already assembled, and a blip here used to discard all of it.
        session_model = None
        if crow["session_id"]:
            try:
                session = await manager.api.get_session(crow["session_id"])
            except QoderError as exc:
                log.debug("could not read session %s for /health: %s",
                          crow["session_id"], exc.message)
            else:
                agent_model_obj = ((session or {}).get("agent") or {}).get("model") or {}
                session_model = agent_model_obj.get("id")
        if session_model:
            lines.append(f"session model: <code>{tg_html.escape(str(session_model))}</code>")

    recoverable = auth.list_recoverable_conversations(db, user.id, limit=100)
    if recoverable:
        lines.append(
            f"awaiting restore: <code>{len(recoverable)}</code> "
            "<i>(use /sessions, then ⟳ RESTORE)</i>"
        )

    # Scoped to the caller. Both of these used to count the whole database, and since every
    # allowlisted user shares one process, that disclosed other people's activity to whoever
    # happened to ask.
    queued = db.query_one(
        """SELECT COUNT(*) AS n FROM inbound_queue q
           JOIN conversations c ON c.convo_id = q.convo_id
           WHERE q.state = 'queued' AND c.tg_user_id = ?""",
        (user.id,),
    )
    lines.append(f"your live conversations: <code>{manager.live_count(user.id)}</code>")
    lines.append(f"your messages awaiting dispatch: <code>{queued['n'] if queued else 0}</code>")
    lines.append(f"max concurrent streams: <code>{settings.max_live_streams}</code>")
    await _reply(update, "\n".join(lines))


# --- administrator commands ---------------------------------------------------------
# Gated on _require_admin. The admin id is fixed from TG_ADMIN_ID and is not writable at
# runtime, so these are the only path to mutate the allowlist or rotate the API token from
# Telegram. They are deliberately NOT advertised in HELP_TEXT or the command menu, which every
# user sees; the admin discovers them through /admin.


async def _require_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """True if the caller is the administrator; otherwise refuse and return False.

    Every caller runs ``_reject`` first, so a stranger never reaches here — only an allowlisted
    non-admin can, and they are told plainly. The refusal is not throttled on purpose: only
    trusted, allowlisted users can trigger it, so it cannot be used to burn the send budget the
    way a stranger's flood could.
    """
    access = _access(context)
    user = update.effective_user
    if access.is_admin(user.id if user else None):
        return True
    await _reply(update, "That command is restricted to the administrator.", error=True)
    return False


def _parse_id_arg(args) -> int | None:
    """A positive Telegram user id from the first command argument, or None."""
    if not args:
        return None
    try:
        uid = int(args[0].strip())
    except ValueError:
        return None
    return uid if uid > 0 else None


async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await _reject(update, context):
        return
    if not await _require_admin(update, context):
        return
    await _reply(update, (
        "<b>Administrator commands</b>\n"
        "<code>/allow &lt;id&gt;</code> — let a user id use the bot\n"
        "<code>/disallow &lt;id&gt;</code> — revoke a user id\n"
        "<code>/allowed</code> — show the admin id and the allowlist\n"
        "<code>/setpat &lt;token&gt;</code> — validate and hot-swap the Qoder API token\n\n"
        "Changes take effect immediately and survive a restart. The admin id itself is fixed by "
        "TG_ADMIN_ID in .env and cannot be changed from here."
    ))


async def cmd_allow(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await _reject(update, context):
        return
    if not await _require_admin(update, context):
        return
    uid = _parse_id_arg(context.args)
    if uid is None:
        await _reply(
            update,
            "Usage: <code>/allow &lt;user_id&gt;</code>. The id is the number, not the @username "
            "— the person can get it from /start.",
            error=True,
        )
        return
    _access(context).add(uid)
    log.info("admin allowlisted user id=%s", uid)
    await _reply(update, f"Allowed <code>{uid}</code>. They can use the bot now — no restart needed.")


async def cmd_disallow(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await _reject(update, context):
        return
    if not await _require_admin(update, context):
        return
    access = _access(context)
    uid = _parse_id_arg(context.args)
    if uid is None:
        await _reply(update, "Usage: <code>/disallow &lt;user_id&gt;</code>.", error=True)
        return
    if access.is_admin(uid):
        await _reply(
            update,
            "That is the administrator id, which is always allowed and cannot be removed.",
            error=True,
        )
        return
    access.remove(uid)
    log.info("admin revoked user id=%s", uid)
    await _reply(
        update,
        f"Removed <code>{uid}</code> from the allowlist. Their next message will be refused.",
    )


async def cmd_allowed(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await _reject(update, context):
        return
    if not await _require_admin(update, context):
        return
    access = _access(context)
    lines = ["<b>Access</b>"]
    if access.admin_id is not None:
        lines.append(f"administrator: <code>{access.admin_id}</code>")
    else:
        lines.append("administrator: <i>none set (TG_ADMIN_ID is empty)</i>")
    ids = access.snapshot()
    if ids:
        lines.append("allowed users:\n" + "\n".join(f"  • <code>{i}</code>" for i in ids))
    else:
        lines.append("allowed users: <i>none — only the administrator</i>")
    lines.append(
        "\n<code>/allow &lt;id&gt;</code> · <code>/disallow &lt;id&gt;</code> · "
        "<code>/setpat &lt;token&gt;</code>"
    )
    await _reply(update, "\n".join(lines))


async def cmd_setpat(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Validate a new Qoder PAT and hot-swap the backend onto it, without a restart.

    The token is a secret that arrives as plaintext in a Telegram message, so two rules govern
    this handler: it is NEVER logged or echoed (only its non-reversible fingerprint is shown),
    and the invoking message is deleted best-effort so the token does not linger in the chat.
    Deletion cannot be relied on — a bot cannot delete a user's message in a DM and may lack
    rights in a group — so the usage text also tells the admin to prefer a DM.
    """
    if await _reject(update, context):
        return
    if not await _require_admin(update, context):
        return

    token = context.args[0].strip() if context.args else ""
    if not token:
        await _reply(update, (
            "Usage: <code>/setpat &lt;token&gt;</code>\n\n"
            "The token is validated first, then swapped in live. Active conversations lose their "
            "cloud session — as any token rotation does — and rebuild from the on-device "
            "transcript on your next message.\n\n"
            "⚠ A command sent here stays in the chat history. Prefer a DM; the bot deletes the "
            "message best-effort after reading it."
        ), error=True)
        return

    message = update.effective_message
    if message is not None:
        try:
            await message.delete()
        except Exception:  # noqa: BLE001 - deletion is best-effort; rights are often absent
            pass

    app = context.application
    try:
        await runtime.hot_swap_pat(app, token)
    except Unauthorized as exc:
        log.warning("PAT swap rejected by the API: %s", exc.message)
        await _reply(update, (
            f"That token was rejected ({tg_html.escape(exc.message)}). "
            "The current one is still in use."
        ), error=True)
        return
    except QoderError as exc:
        log.warning("PAT swap failed: %s", exc.message)
        await _reply(update, (
            f"Could not switch to that token: {tg_html.escape(exc.message)}. "
            "The current one is still in use."
        ), error=True)
        return
    except Exception:  # noqa: BLE001 - never leak the token in a traceback to the chat
        log.exception("PAT swap failed unexpectedly")
        await _reply(update, (
            "Could not switch tokens; the current one is still in use. Check the log for detail."
        ), error=True)
        return

    settings = context.application.bot_data["settings"]
    body = f"Switched to the new token <code>{_pat_fingerprint(settings.qoder_pat)}</code>."
    startup_error = context.application.bot_data.get("startup_error")
    if startup_error:
        body += (
            f"\n\n⚠ Re-provisioning reported: {tg_html.escape(str(startup_error))}\n"
            "It will be retried on your next /new."
        )
    else:
        body += "\n\nActive conversations will rebuild from their on-device history on the next message."
    log.info("PAT swapped from Telegram by the administrator")
    await _reply(update, body)


class _MigrationFilter(filters.MessageFilter):
    """Matches the service message Telegram posts when a basic group becomes a supergroup.

    Matched on the two payload fields it carries rather than through PTB's status-message
    taxonomy. This is the only service message the bot cares about, so naming the fields is both
    narrower and more obvious than a category — and a filter attribute resolved at registration
    time is a way to stop the bot from starting at all, on a phone nobody is watching.
    """

    def filter(self, message: Message) -> bool:
        return bool(
            getattr(message, "migrate_to_chat_id", None)
            or getattr(message, "migrate_from_chat_id", None)
        )


async def on_migrate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Follow a group through its upgrade from basic group to supergroup.

    The upgrade changes the chat id, and conversations are keyed on it, so without this every
    row for the old id stops matching: the bot quietly starts a fresh conversation in the new
    chat while /sessions keeps listing the old ones, and switching to one posts into a chat that
    no longer exists.

    Deliberately NOT gated on the allowlist. Telegram generates this message when an admin
    changes a group setting — no member can forge one — and its sender is not an allowlisted
    human, so gating it would mean the migration never ran. All it does is repoint our own rows
    at the chat Telegram says replaced the old one.
    """
    message = update.effective_message
    chat = getattr(message, "chat", None)
    if message is None or chat is None:
        return

    to_id = getattr(message, "migrate_to_chat_id", None)
    from_id = getattr(message, "migrate_from_chat_id", None)
    if to_id:
        # Arrived in the OLD chat, naming its replacement.
        old_id, new_id = chat.id, to_id
    elif from_id:
        # Arrived in the NEW chat, naming its predecessor.
        old_id, new_id = from_id, chat.id
    else:
        return

    db = context.application.bot_data["db"]
    manager = _manager(context)
    if manager is not None:
        # Retire BEFORE rewriting. A live conversation's sink still targets the old id and would
        # keep posting into a chat that no longer exists until it was rebuilt from the new rows.
        await manager.forget_chat(old_id)
    moved = auth.migrate_chat_id(db, old_id, new_id)
    log.info("chat %s became %s; repointed %d conversation(s)", old_id, new_id, moved)

    if moved and new_id == chat.id:
        # Announce it only in the chat that still exists. The old one is a stub by now, so a
        # reply there would fail — harmlessly, but pointlessly.
        await _reply(
            update,
            f"This group was upgraded to a supergroup, so I moved {moved} conversation(s) and "
            "their history across. Nothing was lost.",
        )


# --- ordinary messages --------------------------------------------------------------


async def on_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if await _reject(update, context):
        return
    message = update.effective_message
    if message is None:
        return

    convo, transcript = await _active(
        update,
        context,
        create_from=message,
    )
    if convo is None:
        # No conversation could be resolved (nothing active and nothing to create/resume), or
        # creation failed and _active already told the user why. transcript is None on the
        # normal create/reuse paths — only a lost-conversation resume sets it — so it must NOT
        # be part of this guard, or every ordinary message is dropped before it is enqueued.
        return

    text = message.text or message.caption or ""
    attachment_note = None

    if uploads.has_attachment(message):
        try:
            ingested = await uploads.ingest(
                message=message,
                api=convo.api,
                db=convo.db,
                tg_user_id=convo.tg_user_id,
                convo_id=convo.convo_id,
                session_id=convo.session_id,
                caption=message.caption,
            )
            attachment_note = ingested.pointer_text
        except IngestError as exc:
            convo.notice(exc.reason, is_error=True)
            if not text.strip():
                return
        except QoderError as exc:
            convo.notice(f"Could not attach that file: {exc.message}", is_error=True)
            if not text.strip():
                return

    outgoing = attachment_note or text.strip()
    if not outgoing:
        convo.notice("I could not read anything in that message.", is_error=True)
        return

    if transcript:
        # A resume just happened: the rebuilt session knows nothing yet, so the transcript
        # rides along with this message rather than spending a billable turn of its own.
        # set_resume_context holds it durably and _dispatch_burst prepends it once — see the
        # note there on why it must not be recorded into this conversation's own history.
        history.set_resume_context(convo.db, convo.convo_id, transcript)
        convo.notice(
            "That conversation lost its cloud session, so I rebuilt it from the history kept on "
            "this device and carried your message across. Nothing you were told is gone."
        )
    elif text.strip() and (convo.title or "") == config.DEFAULT_CONVO_TITLE:
        # A conversation opened with /new or the "New chat" button keeps its placeholder title
        # until the user actually says something. Rename it from this first real message so
        # /sessions shows something meaningful instead of a row of "new conversation".
        #
        # No announcement here: this branch can only be reached for a conversation that /new or
        # the button just created, and both of those already told the user, so a second message
        # saying the same thing was pure noise.
        convo.title = conversation_title(text)
        auth.update_conversation(
            convo.db, convo.convo_id, convo.tg_user_id, title=convo.title
        )

    convo.send_text(outgoing)


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if query is None:
        return
    user = query.from_user
    access = _access(context)
    if not access.is_allowed(user.id if user else None):
        await _deny_callback(query, user)
        return

    # Always answer, or Telegram shows a spinning clock to the user.
    await query.answer()

    manager = _manager(context)
    if manager is None:
        await query.edit_message_text("The bot is still starting up. Try again shortly.")
        return

    action, _, arg = (query.data or "").partition(":")

    # Restore button pressed — rebuild the conversation from local transcript.
    if action == "rc" and arg.isdigit():
        origin = _cb_origin(query)
        if origin is None:
            await query.edit_message_text("That button has expired. Use /sessions to try again.")
            return
        chat_id, thread_id = origin
        try:
            res = await manager.resume_lost_conversation(
                tg_user_id=user.id,
                chat_id=chat_id,
                message_thread_id=thread_id,
                handle=user.username,
                # The button names the conversation it belongs to. Dropping this — which is
                # what used to happen — made every ⟳ RESTORE in the list resolve to the newest
                # lost conversation, so the older ones had a button that did nothing useful.
                convo_id=int(arg),
            )
        except BillingError as exc:
            await query.edit_message_text(budget.exhausted_notice(
                exc.message,
                blocked="You have no available credit to restore that conversation.",
            ))
            return
        except QoderError as exc:
            await query.edit_message_text(f"Could not restore that conversation: {exc.message}")
            return
        if res is None:
            await query.edit_message_text(
                "That conversation is no longer recoverable — it may already have been "
                "restored. Use /sessions to see what is left, or /new to start fresh."
            )
            return
        convo, transcript = res
        # There is no user message to merge this with, so it goes as the first turn on its
        # own. Dropping it would leave the fresh session knowing nothing about the
        # conversation the user just asked to get back.
        if transcript:
            history.set_resume_context(convo.db, convo.convo_id, transcript)
        await query.edit_message_text(
            f"Restored <b>{tg_html.escape(convo.title or 'conversation')}</b> from local history. "
            "Its files were carried over where they were still on disk.",
            parse_mode=ParseMode.HTML,
        )
        return

    if not arg.isdigit():
        return
    convo_id = int(arg)

    # Active conversation switch.
    if action == "sw":
        convo = await manager.switch_to(convo_id, user.id)
        if convo is None:
            await query.edit_message_text("That conversation is not available.")
            return
        await query.edit_message_text(f"Switched to <b>{tg_html.escape(convo.title or 'untitled')}</b>.",
                                      parse_mode=ParseMode.HTML)
    elif action == "new":
        origin = _cb_origin(query)
        if origin is None:
            await query.edit_message_text("That button has expired. Use /new to start again.")
            return
        chat_id, thread_id = origin
        try:
            convo = await manager.create(
                tg_user_id=user.id,
                chat_id=chat_id,
                message_thread_id=thread_id,
                first_text=config.DEFAULT_CONVO_TITLE,
                handle=user.username,
            )
        except QoderError as exc:
            await query.edit_message_text(f"Could not start a new conversation: {exc.message}")
            return
        await query.edit_message_text(
            f"Started <b>{tg_html.escape(convo.title or 'a new conversation')}</b>.",
            parse_mode=ParseMode.HTML,
        )


# --- helpers ------------------------------------------------------------------------


async def _active(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    create_from=None,
):
    """The conversation this update belongs to, creating or resuming one if asked and needed.

    Returns ``(conversation, transcript_or_none)``. The transcript is non-empty only when a lost
    conversation was just rebuilt. It is context for the AGENT, not for the user: the caller
    stores it with ``history.set_resume_context`` so it rides along with the message the user
    just sent, rather than spending a billable turn of its own.
    """
    manager = _manager(context)
    if manager is None:
        await _reply(update, "The bot is still starting up. Try again shortly.", error=True)
        return None, None

    user = update.effective_user
    chat = update.effective_chat
    message = update.effective_message
    thread_id = _thread_id(message)

    # Normal path: reuse the conversation already live for this chat. Must be tried first,
    # otherwise every message would mint a fresh session and lose continuity.
    convo = await manager.active_for(user.id, chat.id, thread_id)
    if convo is not None:
        return convo, None

    if create_from is None:
        await _reply(update, "No active conversation. Send a message or use /new to start one.")
        return None, None

    # Nothing live. Before starting cold, look for a conversation in this chat that LOST its
    # cloud session: a rotated PAT makes every session the old token created return 404, and
    # reconcile retires those as resumable rather than deleted. Rebuilding one from the
    # on-device transcript is the difference between the agent knowing what the user was
    # working on and silently starting them over — which is the recovery the README promises
    # for exactly this path.
    #
    # Gated on create_from so only a real message triggers it. A command such as /stop must
    # not mint a paid session just to discover there is nothing to cancel.
    #
    # resume_lost_conversation clears lost_session on the row it adopts, so this fires at most
    # once per lost conversation: it cannot resurrect the same one on every message.
    try:
        resumed = await manager.resume_lost_conversation(
            tg_user_id=user.id,
            chat_id=chat.id,
            message_thread_id=thread_id,
            handle=user.username,
        )
    except BillingError as exc:
        # Starting cold would fail the same way, so there is nothing to fall through to.
        await _reply(
            update,
            tg_html.escape(budget.exhausted_notice(
                exc.message,
                blocked="You have no available credit to restore that conversation.",
            )),
            error=True,
        )
        return None, None
    except QoderError as exc:
        # Told, not silently worked around. Falling through to create() would answer this
        # message in a blank session with no memory of the conversation, which reads as the
        # bot ignoring the user; and the transcript is still on disk, so ⟳ RESTORE in
        # /sessions keeps working. This mirrors manager._auto_resume's handling of the same
        # failure.
        log.warning("could not rebuild a lost conversation in chat %s: %s", chat.id, exc.message)
        await _reply(
            update,
            f"That conversation lost its cloud session and I could not rebuild it just now: "
            f"{tg_html.escape(exc.message)}\n\nIts history is still on this device — use "
            "/sessions and tap ⟳ RESTORE to try again, or /new to start fresh.",
            error=True,
        )
        return None, None
    if resumed is not None:
        return resumed

    # Nothing resumable either. Create a fresh conversation for this chat/thread.
    first_text = create_from.text or create_from.caption or config.DEFAULT_CONVO_TITLE
    try:
        convo = await manager.create(
            tg_user_id=user.id,
            chat_id=chat.id,
            message_thread_id=thread_id,
            first_text=first_text,
            handle=user.username,
            # Two messages arriving at once for a user with no live conversation used to mint
            # two paid sessions, the second of which then failed the one-active-per-chat index.
            # The loser of that race should simply adopt what the winner created.
            reuse_if_active=True,
        )
    except BillingError as exc:
        # The user sent a message with no credits left. Say so plainly and point at renewal.
        await _reply(
            update,
            tg_html.escape(budget.exhausted_notice(
                exc.message,
                blocked="You have no available credit to continue this conversation.",
            )),
            error=True,
        )
        return None, None
    except QoderError as exc:
        await _reply(
            update,
            f"Could not start a conversation: {tg_html.escape(exc.message)}",
            error=True,
        )
        return None, None
    return convo, None


async def _reply(
    update: Update,
    text: str,
    *,
    error: bool = False,
    reply_markup=None,
    parse_mode: str | None = ParseMode.HTML,
) -> None:
    """Send a direct reply, into the topic the update came from.

    The single path for every command reply, so the thread id, the parse mode and the
    escape-and-retry fallback cannot drift between handlers. ``message_thread_id`` is what
    makes forum topics usable: without it every reply went to the General topic, while the
    renderer — which does forward the thread through ``TelegramSink`` — put the agent's answers
    in the right one. Asking a question in topic 3 got the answer in topic 3, then ``/usage``
    answered from somewhere else entirely.
    """
    chat = update.effective_chat
    if chat is None:
        return
    prefix = "⚠ " if error else ""
    thread_id = _thread_id(update.effective_message)
    try:
        await chat.send_message(
            prefix + text,
            parse_mode=parse_mode,
            message_thread_id=thread_id,
            reply_markup=reply_markup,
        )
    except Exception as exc:  # noqa: BLE001 - a failed reply must never break the handler
        log.exception("could not reply in chat %s", chat.id)
        # Fallback on the original exception, not a second attempt at retrying: sending plain
        # text is what survives any markup failure, and we don't need two tries to prove that.
        try:
            await chat.send_message(
                tg_html.escape(prefix + text),
                parse_mode=None,
                message_thread_id=thread_id,
                reply_markup=reply_markup,
            )
        except Exception:  # noqa: BLE001
            pass


def _should_show_id(db, tg_user_id: int) -> bool:
    """Whether /start should echo the caller's own user id, at most once an hour.

    NULL means never shown. Atomic on purpose: without this, concurrent /starts could both read
    "show" and both update, echoing the id twice. Using a single UPDATE with a condition prevents
    that race while avoiding the SELECT-then-UPDATE pattern altogether.

    Returns True if the id was just echoed (the row was modified).
    """
    try:
        cur = db.execute(
            """UPDATE users SET last_id_request = ?
               WHERE tg_user_id = ? AND (last_id_request IS NULL OR datetime(last_id_request, 'unixepoch') < datetime('now', '-' || ? || ' seconds'))""",
            (int(time.time()), tg_user_id, config.ID_REMINDER_S),
        )
        return bool(cur.rowcount)
    except sqlite3.DatabaseError:  # if SQLite misbehaves, don't let it kill the handler
        log.exception("could not update last_id_request")
        return False


def _size(size: int | None) -> str:
    if not size:
        return "?"
    value = float(size)
    for unit in ("B", "KB", "MB"):
        if value < 1024:
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} GB"


def _pat_fingerprint(pat: str) -> str:
    """A short, non-reversible label for the loaded credential.

    Enough to tell two tokens apart after a rotation — which is when every session the previous
    token created starts returning 404, and knowing which one is loaded is the whole diagnosis.
    A hash rather than a substring on purpose: a prefix or suffix of a secret is still part of
    a secret, and this string is posted into a chat that may have other members in it.
    """
    return hashlib.sha256(pat.encode("utf-8")).hexdigest()[:12]


def _effort_outcome(model: dict | None, stored_effort: str | None) -> tuple[str, str | None]:
    """PLAIN-text ``(description, warning)`` for the effort a model will actually run at.

    ``model`` is the catalog entry, or None when the id is no longer listed and the effective
    effort cannot be known.

    The warning is the point of this function. A stored effort preference is never cleared by a
    model switch, and ``model_ref`` silently drops any level the new model does not advertise —
    which quietly lands the user on that model's own ``default_effort``. For the two models that
    refuse ``low`` (``gfmodel``, ``dmodel``) that default is ``max``, so switching model can jump
    the cheapest setting to the most expensive one with nothing said. Both callers that can
    change the model surface it at the moment it happens.

    Plain text, not HTML: the result is used both in a ``_reply`` (where the caller escapes the
    dynamic part) and in a ``convo.notice`` (which the renderer escapes wholesale), and markup
    here would reach the user literally in the second case.
    """
    if model is None:
        return "effort unknown (that model is no longer in the catalog)", None

    model_id = str(model.get("id") or "?")
    allowed = model.get("efforts") or []
    effective = str(model.get("default_effort") or "unspecified")

    if not allowed:
        warning = None
        if stored_effort:
            warning = (
                f"Your effort setting '{stored_effort}' is ignored by {model_id}, which has no "
                "adjustable effort."
            )
        return "no adjustable effort on this model", warning

    if stored_effort and stored_effort in allowed:
        return f"effort {stored_effort} (your setting)", None

    description = f"effort {effective} (this model's default)"
    if not stored_effort:
        return description, None
    return description, (
        f"Your effort setting '{stored_effort}' does not apply to {model_id} — it offers "
        f"{', '.join(allowed)}. It will run at '{effective}' instead; use /effort to change that."
    )


async def _model_and_effort_line(
    manager: Manager, db, tg_user_id: int, model_id: str | None
) -> tuple[str | None, str | None]:
    """``(description, warning)`` naming the model a conversation will run on and its effort.

    A catalog lookup failure costs just the effort detail: naming the model is still useful, and
    /new must not fail because a diagnostic line could not be built.
    """
    if not model_id:
        return None, None
    user = auth.get_user(db, tg_user_id)
    stored = user["effort"] if user else None
    try:
        models = await manager.api.list_models()
    except QoderError as exc:
        log.debug("could not list models for the /new summary: %s", exc.message)
        return f"model {model_id}", None
    model = next((m for m in models if m.get("id") == model_id), None)
    description, warning = _effort_outcome(model, stored)
    return f"model {model_id} · {description}", warning


def _agent_model_ref(db, tg_user_id: int) -> dict | str | None:
    """The stored ``agents.model_json``, parsed. An object, a bare string, or None."""
    row = db.query_one("SELECT model_json FROM agents WHERE tg_user_id = ?", (tg_user_id,))
    if not row or not row["model_json"]:
        return None
    try:
        return json.loads(row["model_json"])
    except json.JSONDecodeError:
        return None


def _agent_model(db, tg_user_id: int) -> str:
    """One-line description of an agent's stored model, effort included.

    Effort is part of the model object, and it is the half that silently drives cost — a model
    left at its own ``default_effort`` runs at ``max`` or ``xhigh`` for ten of the eighteen
    current models, which is not visible from the id alone.
    """
    parsed = _agent_model_ref(db, tg_user_id)
    if parsed is None:
        return "?"
    if not isinstance(parsed, dict):
        return str(parsed) if parsed else "?"
    model_id = str(parsed.get("id") or "?")
    effort = parsed.get("effort")
    return f"{model_id} · effort {effort}" if effort else model_id


def _effort_description(db: Database, models: list[dict], tg_user_id: int) -> str:
    """Human-readable effort display for /health.

    Resolved against the CATALOG, not against the agent's stored model object. ``model_ref``
    builds only ``id``, ``effort`` and ``context_window`` — ``efforts`` and ``default_effort``
    are catalog fields that never reach ``agents.model_json`` — so reading them off the agent
    found nothing and reported "no adjustable effort" for every user on every model.
    """
    user = auth.get_user(db, tg_user_id)
    stored_effort = user["effort"] if user else None

    ref = _agent_model_ref(db, tg_user_id)
    if ref is None:
        return str(stored_effort) if stored_effort else "(unknown)"
    model_id = str(ref.get("id")) if isinstance(ref, dict) else str(ref)

    model = next((m for m in models if m.get("id") == model_id), None)
    return _effort_outcome(model, stored_effort)[0]


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Last-resort handler for anything that escaped a command or message handler.

    Without a registered error handler PTB logs "No error handlers are registered" and
    swallows the exception, so a crash presents to the user as the bot simply not replying.
    Saying something is always better than silence.

    This is NOT throttled: a real bug should be reported immediately and once per update,
    never suppressed by the reject throttle. The admin can see every one in the rotating log.
    """
    # Check allowlist first so we don't waste an API call on someone who shouldn't interact
    # with the bot at all. But even then, no throttle — this is a diagnostic path.
    user = getattr(update, "effective_user", None)
    if user:
        access = context.application.bot_data.get("access")
        if access and not access.is_allowed(user.id):
            return

    chat = getattr(update, "effective_chat", None)
    if chat is None:
        return

    # Log the full traceback to the server console / logs. The chat message is just the
    # visible part of what happened.
    log.exception("unhandled error while processing an update", exc_info=context.error)

    try:
        # Plain text: this message must not itself fail on markup. The thread id still has
        # to be carried, or a failure inside a topic gets reported in General instead.
        await chat.send_message(
            "⚠ Something went wrong handling that. Please try again — /health shows what "
            "is still connected.",
            parse_mode=None,
            message_thread_id=_thread_id(getattr(update, "effective_message", None)),
        )
    except Exception:  # noqa: BLE001 - an error handler must never raise
        log.debug("could not report the failure to chat %s", getattr(chat, "id", "?"))


def register(app: Application) -> None:
    app.add_error_handler(on_error)

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("new", cmd_new))
    app.add_handler(CommandHandler("stop", cmd_stop))
    app.add_handler(CommandHandler(["sessions", "switch"], cmd_sessions))
    app.add_handler(CommandHandler("model", cmd_model))
    app.add_handler(CommandHandler("effort", cmd_effort))
    app.add_handler(CommandHandler("tools", cmd_tools))
    app.add_handler(CommandHandler("files", cmd_files))
    app.add_handler(CommandHandler("usage", cmd_usage))
    app.add_handler(CommandHandler("archive", cmd_archive))
    app.add_handler(CommandHandler("health", cmd_health))

    # Administrator-only. Gated inside each handler by _require_admin; not listed in the menu.
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CommandHandler("allow", cmd_allow))
    app.add_handler(CommandHandler("disallow", cmd_disallow))
    app.add_handler(CommandHandler(["allowed", "allowlist"], cmd_allowed))
    app.add_handler(CommandHandler("setpat", cmd_setpat))

    app.add_handler(CallbackQueryHandler(on_callback))

    # A group upgrading to a supergroup changes its chat id, which silently orphans every
    # conversation keyed on the old one. on_message could not catch this even if it were
    # registered first: its filter requires text or an attachment, and a service message has
    # neither.
    app.add_handler(MessageHandler(_MigrationFilter(), on_migrate))

    # Text and attachments, but never commands (those are handled above).
    app.add_handler(
        MessageHandler(
            (filters.TEXT & ~filters.COMMAND) | filters.Document.ALL | filters.PHOTO
            | filters.VIDEO | filters.AUDIO | filters.VOICE | filters.ANIMATION,
            on_message,
        )
    )
