"""Credit tracking.

Personal Qoder spaces are prepaid and have NO spending cap, so the bot keeps its own ledger.
Per the product decision this only ever warns: it never refuses a turn, because running out
mid-task with no explanation is worse than overspending slightly.

Credits arrive from ``span.model_request_end.model_usage.credits``. A single turn emits
several of these — one PPT turn cost 6.66 + 0.96 + 0.59 — so per-turn cost is always a sum.
Failed model calls are not billed by Qoder, but we still record them (flagged) so the ledger
reconciles with what the user saw happen.
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta, timezone

from . import config, tg_html
from .db import Database, utcnow

log = logging.getLogger("tgagent.budget")

RENEWAL_LINE = "Please renew your plan or purchase a resource package to continue."


def exhausted_notice(message: str, *, blocked: str, hint: str | None = None) -> str:
    """PLAIN-text notice for an exhausted credit balance (HTTP 402).

    One builder for every path that can hit a 402 — the pump, the stream consumer, the poll
    fallback, /new, the restore button and both message paths — because each of those used to
    hold its own near-copy and they had already drifted. ``blocked`` names what the user was
    trying to do; ``hint`` is an optional closing line.

    Plain, not HTML. convo hands the result to the renderer, which escapes wholesale, and
    handlers escapes it at the call sites that send markup. Escaping it here would double up.
    """
    parts = [f"Credits exhausted: {message}", "", blocked, RENEWAL_LINE]
    if hint:
        parts.append(hint)
    return "\n".join(parts)


def budget_label(stored: float | None) -> str:
    """How a stored credit budget reads to the user, bare: ``"1000"`` or ``"no cap"``."""
    value = effective_budget(stored)
    return "no cap" if value <= 0 else f"{value:.0f}"


def effective_budget(stored: float | None) -> float:
    """The monthly budget in force for a stored ``users.credit_budget``.

    Only NULL falls back to the default. A stored 0 means "no cap": reading 0 as falsy
    substituted the default and made the no-cap guard in :func:`check_budget` unreachable for
    exactly the value it was written to handle.
    """
    return config.DEFAULT_CREDIT_BUDGET if stored is None else float(stored)


def record(
    db: Database,
    *,
    tg_user_id: int,
    convo_id: int | None,
    credits: float,
    model: str | None = None,
    is_error: bool = False,
    evt_id: str | None = None,
) -> bool:
    """Add one credit span to the ledger. Returns False if it was already recorded.

    ``evt_id`` is what makes this idempotent. ``rendered_events`` normally stops a replayed
    ``span.model_request_end`` from being counted twice, but that table is bounded at
    ``RENDER_DEDUPE_KEEP`` ids, so a full history re-walk on an old conversation can present a
    span this ledger has already seen. The unique index on ``spend(evt_id)`` is the backstop;
    without it the duplicate was silently added to the user's monthly total, and to
    ``conversations.credits_spent`` as well.
    """
    with db.transaction():
        cur = db.execute(
            """INSERT OR IGNORE INTO spend(convo_id, tg_user_id, credits, model, is_error, evt_id, at)
               VALUES(?, ?, ?, ?, ?, ?, ?)""",
            (convo_id, tg_user_id, credits, model, int(is_error), evt_id, utcnow()),
        )
        if cur.rowcount == 0:
            log.debug("credit span %s was already in the ledger; not counting it again", evt_id)
            return False
        if convo_id is not None:
            db.execute(
                "UPDATE conversations SET credits_spent = credits_spent + ? WHERE convo_id = ?",
                (credits, convo_id),
            )
    return True


def _sum_since(db: Database, tg_user_id: int, since: datetime) -> float:
    """Sum billed credits since ``since``, EXCLUDING error rows.

    Failed model calls are not billed by Qoder (see the module docstring), but ``record`` does
    not enforce ``credits == 0`` for them — so an error span carrying a nonzero amount would
    otherwise inflate the monthly total that drives :func:`check_budget` warnings and /usage.
    Filtering on the flag the ledger already stores is cheaper and more reliable than trusting
    every caller to zero it out.
    """
    row = db.query_one(
        """SELECT COALESCE(SUM(credits), 0) AS total FROM spend
           WHERE tg_user_id = ? AND at >= ? AND is_error = 0""",
        (tg_user_id, since.isoformat(timespec="seconds").replace("+00:00", "Z")),
    )
    return float(row["total"]) if row else 0.0


def totals(db: Database, tg_user_id: int) -> dict[str, float]:
    # "today" is UTC midnight, not the user's local day. The bot has no timezone for a user —
    # Telegram does not expose one — so UTC is the only consistent anchor. A user far from UTC
    # sees their daily figure roll over at an odd local hour; the weekly and monthly figures,
    # which are what the budget warning actually keys on, are unaffected.
    now = datetime.now(timezone.utc)
    day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return {
        "today": _sum_since(db, tg_user_id, day_start),
        "week": _sum_since(db, tg_user_id, now - timedelta(days=7)),
        "month": _sum_since(db, tg_user_id, now - timedelta(days=30)),
    }


def check_budget(db: Database, user: sqlite3.Row) -> str | None:
    """Return a warning message if the user just crossed a 20% threshold, else None.

    ``warned_at_pct`` makes each threshold fire once instead of on every single turn, which
    would otherwise nag continuously once the user is over 80%.

    A stored budget of 0 means "no cap". Only NULL falls back to the default — see
    :func:`effective_budget`.

    The read of ``warned_at_pct`` and the write that claims the threshold run inside one
    transaction: two concurrent turns for the same user could otherwise both see the old value
    and both emit the warning. The id is derived from ``user`` rather than passed alongside it,
    so a mismatched (row, id) pair cannot silently warn the wrong account.
    """
    tg_user_id = int(user["tg_user_id"])
    budget = effective_budget(user["credit_budget"])
    if budget <= 0:
        return None

    spent = totals(db, tg_user_id)["month"]
    pct = int((spent / budget) * 100)
    crossed = (pct // 20) * 20

    with db.transaction():
        warned = int(user["warned_at_pct"] or 0)
        if crossed < config.BUDGET_WARN_FRACTION * 100:
            if warned > crossed:
                db.execute(
                    "UPDATE users SET warned_at_pct = 0 WHERE tg_user_id = ?", (tg_user_id,)
                )
            return None
        if warned >= crossed:
            return None
        db.execute(
            "UPDATE users SET warned_at_pct = ? WHERE tg_user_id = ?", (crossed, tg_user_id)
        )

    return (
        f"Credit notice: you have used {spent:.2f} of your {budget:.0f} monthly budget "
        f"({pct}%). This is a warning only — nothing will be blocked. "
        "Long conversations are the main cost, since the whole history is resent each turn; "
        "/new starts fresh, and /usage shows the breakdown."
    )


def usage_report(db: Database, tg_user_id: int) -> str:
    """HTML summary for /usage."""
    figures = totals(db, tg_user_id)
    user = db.query_one("SELECT credit_budget FROM users WHERE tg_user_id = ?", (tg_user_id,))
    budget = effective_budget(user["credit_budget"] if user else None)
    cap = "no cap" if budget <= 0 else f"budget {budget:.0f}"

    lines = [
        "<b>Credit usage</b>",
        f"today: {figures['today']:.2f} cr",
        f"7 days: {figures['week']:.2f} cr",
        f"30 days: {figures['month']:.2f} cr  ({cap})",
    ]

    # No deleted_at filter: the totals above are summed straight from the spend ledger, which
    # keeps rows for conversations that were later archived, soft-deleted or lost. Filtering
    # them out of this list made the five entries visibly add up to less than the monthly total
    # printed two lines above. They are included and annotated instead, so the list reconciles.
    rows = db.query(
        """SELECT c.title, c.convo_id, c.credits_spent, c.model_id, c.deleted_at, c.archived_at
           FROM conversations c
           WHERE c.tg_user_id = ? AND c.credits_spent > 0
           ORDER BY c.credits_spent DESC LIMIT 5""",
        (tg_user_id,),
    )
    if rows:
        lines.append("")
        lines.append("<b>Most expensive conversations</b>")
        for row in rows:
            title = tg_html.escape(row["title"] or f"#{row['convo_id']}")
            model = tg_html.escape(row["model_id"] or "?")
            state = ""
            if row["deleted_at"]:
                state = " <i>(deleted)</i>"
            elif row["archived_at"]:
                state = " <i>(archived)</i>"
            lines.append(f"  {row['credits_spent']:.2f} cr — {title} <i>({model})</i>{state}")

    turns = db.query_one(
        "SELECT COUNT(*) AS n FROM spend WHERE tg_user_id = ?", (tg_user_id,)
    )
    if turns and turns["n"]:
        lines.append("")
        lines.append(f"<i>{turns['n']} model requests logged</i>")

    return "\n".join(lines)
