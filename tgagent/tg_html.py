"""Telegram HTML rendering primitives.

Two rules drive this module.

First, HTML parse mode, never MarkdownV2. MarkdownV2 requires escaping
``_ * [ ] ( ) ~ ` > # + - = | { } . !`` throughout, which is hostile to half-finished LLM
output arriving mid-token. HTML needs only ``& < >`` escaped.

Second, Telegram's 4096-character cap applies to the DECODED text: what remains after
Telegram parses our HTML, discards the tags and collapses each entity back to a single
character. Every length decision here measures the ENCODED form instead, which is a strict
upper bound on that — escaping can only expand a string (``&`` becomes ``&amp;``) — so
measuring it can under-fill a message but can never overflow one. :func:`decoded_length` is
the exception, for the one place where the markup is ours rather than the agent's and the
overhead is worth reclaiming.

Code fences need care: a message split inside a ``` block must close the fence in the head
and reopen it in the tail, or both halves render as garbage.
"""

from __future__ import annotations

import re

from .config import TG_MESSAGE_LIMIT

_FENCE = "```"

# A code fence: up to three leading spaces, then a run of three or more backticks.
_FENCE_RE = re.compile(r"^ {0,3}(`{3,})(.*)$")


def _fence_match(line: str) -> tuple[str, str] | None:
    """``(marker, info)`` for a fence line, or None if the line is not one.

    CommonMark forbids a backtick in an opening fence's info string, which is what keeps a
    line of prose that merely mentions ``` from toggling block state.
    """
    match = _FENCE_RE.match(line)
    if match is None:
        return None
    marker, info = match.group(1), match.group(2)
    if "`" in info:
        return None
    return marker, info.strip()


def fence_state(text: str) -> tuple[bool, str]:
    """Scan for unbalanced fences.

    Returns ``(is_open, opener)`` where ``opener`` is the line that opened the currently
    unclosed fence (e.g. ``"```python"``), so a split tail can reopen with the same language.

    The opener's LENGTH is tracked, not just its presence: CommonMark requires a closing fence
    to be at least as long as the one that opened the block, so a ``` line inside a ```` block
    is content rather than a close. Toggling on any run of three backticks mis-balanced
    exactly the nested blocks an agent is most likely to emit when writing about markdown.
    """
    open_marker: str | None = None
    opener = _FENCE
    for line in text.split("\n"):
        match = _fence_match(line)
        if match is None:
            continue
        marker, info = match
        if open_marker is None:
            open_marker = marker
            opener = line.strip()
        elif not info and len(marker) >= len(open_marker):
            open_marker = None
    return open_marker is not None, opener


def _closing_marker(opener: str) -> str:
    """A fence long enough to close the block ``opener`` started."""
    match = _fence_match(opener)
    return match[0] if match else _FENCE


def balance_fences(text: str) -> str:
    """Close a dangling code fence so a partial stream still renders as code."""
    is_open, opener = fence_state(text)
    if not is_open:
        return text
    # Closing with exactly three backticks leaves a longer block still open, and the raw code
    # then swallows the rest of the message.
    return f"{text}\n{_closing_marker(opener)}"

# Qoder internal identifiers. See scrub_ids for when this is and is not applied.
# Real ids look like file_00orbd2ff2hhc55ljymc or sesr_7fb06a01312c8442c7938455: a long
# alphanumeric suffix that always contains digits. Requiring both a 12-character minimum and
# a digit keeps ordinary words such as "file_system" or "agent_based" out of the match.
_ID_PATTERN = re.compile(
    r"\b(?:file|sess|session|evt|env|agent|memstore|memory|sthr|sesr|vault|dream)_"
    r"(?=[A-Za-z0-9]*\d)[A-Za-z0-9]{12,}\b"
)


def escape(text: str) -> str:
    """Escape for HTML parse mode. Only &, < and > are special to Telegram here."""
    if not text:
        return ""
    # & first, or we would double-escape the entities we just produced.
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def utf16_length(text: str) -> int:
    """Length Telegram actually counts: UTF-16 code units, not Python code points.

    Every astral character — which is nearly all emoji — is a surrogate pair, so it costs 2
    against the 4096 cap while ``len()`` charges 1. Measuring with ``len()`` let an
    emoji-dense answer through at up to twice the real limit, and Telegram then rejected the
    whole message as too long rather than truncating it.

    ``surrogatepass`` is deliberate: a lone surrogate in agent output measures as the single
    unit Telegram would see, instead of raising UnicodeEncodeError inside a length check.
    """
    return len(text.encode("utf-16-le", "surrogatepass")) // 2


def display_length(text: str) -> int:
    """Length Telegram will count: escaped, with any dangling fence closed."""
    return utf16_length(escape(balance_fences(text)))


_TAG_RE = re.compile(r"<[^>]*>")


def _unescape(text: str) -> str:
    """Collapse the three entities :func:`escape` produces back to one character each.

    ``&amp;`` is replaced LAST on purpose. Doing it first would turn the literal sequence
    ``&amp;lt;`` — an agent that wrote ``&lt;`` — into ``&lt;`` and then into ``<``, charging
    one character for something Telegram also charges one for but mis-decoding it on the way.
    """
    return text.replace("&lt;", "<").replace("&gt;", ">").replace("&amp;", "&")


def decoded_length(html: str) -> int:
    """Length Telegram actually counts for a string we send with ``parse_mode=HTML``.

    Use this where the markup is OURS rather than the agent's. The renderer's tool-status
    message is mostly tags — ``<b>``, ``<i>``, and a 30-character spoiler wrapper per line —
    and Telegram counts none of them, so measuring that string with :func:`display_length`
    would charge for markup that is about to disappear and fold tool lines away far earlier
    than the real limit requires.
    """
    return utf16_length(_unescape(_TAG_RE.sub("", html)))


def _fence_reserve(text: str) -> int:
    """Room to leave for the closing fence ``balance_fences`` may append to a cut of ``text``.

    Sized from the block actually open, not from a fixed three: a four-backtick block needs a
    four-backtick closer, and under-reserving would let the cut overflow the cap.
    """
    is_open, opener = fence_state(text)
    marker = _closing_marker(opener) if is_open else _FENCE
    return utf16_length(escape(f"\n{marker}"))


def _max_hard_cut(text: str, limit: int) -> int:
    """Largest n such that escaping text[:n] plus a closing fence still fits."""
    budget = max(0, limit - _fence_reserve(text))
    lo, hi = 0, len(text)
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if utf16_length(escape(text[:mid])) <= budget:
            lo = mid
        else:
            hi = mid - 1
    return lo


def split_message(text: str, limit: int = TG_MESSAGE_LIMIT) -> tuple[str, str]:
    """Split into ``(head, tail)`` where head fits Telegram's cap.

    Prefers a paragraph break, then any line break, then a hard cut. Returns the whole text
    as head with an empty tail when it already fits. The head is returned RAW; call
    :func:`render` to produce what actually goes to Telegram.
    """
    if display_length(text) <= limit:
        return text, ""

    cut = _max_hard_cut(text, limit)
    if cut <= 0:
        # Pathological: a single character escapes past the limit. Should be impossible,
        # but never loop forever or emit an oversized message.
        return "", text

    window = text[:cut]

    # Reject boundaries that would leave a stub; a near-empty first message reads as a bug.
    floor = max(1, cut // 4)

    para = window.rfind("\n\n")
    if para >= floor:
        head = window[:para]
        tail = text[para:].lstrip("\n")
        return _rejoin_fence(head, tail)

    line = window.rfind("\n")
    if line >= floor:
        head = window[:line]
        tail = text[line + 1 :]
        return _rejoin_fence(head, tail)

    return _rejoin_fence(window, text[cut:])


def _rejoin_fence(head: str, tail: str) -> tuple[str, str]:
    """If the split landed inside a code block, reopen it in the tail."""
    is_open, opener = fence_state(head)
    if is_open and tail:
        tail = f"{opener}\n{tail}"
    return head, tail


def render(text: str) -> str:
    """Produce the exact string to send: escaped, fences balanced."""
    return escape(balance_fences(text))


def split_all(text: str, limit: int = TG_MESSAGE_LIMIT) -> list[str]:
    """Split into as many chunks as needed. Each chunk is raw; render before sending."""
    chunks: list[str] = []
    remaining = text
    while remaining:
        head, tail = split_message(remaining, limit)
        if not head and not tail:
            break
        if not head:
            # Nothing fits; force progress rather than spinning.
            head, tail = tail[:1], tail[1:]
        chunks.append(head)
        remaining = tail
    return chunks


def scrub_ids(text: str) -> str:
    """Redact Qoder internal identifiers.

    Apply this to text WE compose — tool-activity lines that echo a tool's input, and bot
    notices — because those can contain mount paths like ``/mnt/session/uploads/file_...``.

    Do NOT apply it to agent message text. A coding agent legitimately writes strings that
    match this pattern, and redacting them would corrupt its output. Internal ids are also
    not exploitable on their own: every Qoder call needs the PAT, which never leaves the bot.
    """
    return _ID_PATTERN.sub("[id]", text)


def truncate(text: str, limit: int) -> str:
    """One-line summary for status displays."""
    flat = " ".join(text.split())
    if len(flat) <= limit:
        return flat
    return flat[: max(0, limit - 1)].rstrip() + "…"
