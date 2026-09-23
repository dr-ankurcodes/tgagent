# tgagent — internals & API reference

This is the deep reference for **tgagent**, split out of the top-level
[README](../README.md) so the README can stay a usable front door. Everything here was
verified empirically against the live Qoder Cloud Agents API and is kept because it is
hard-won and not in the published docs — **where this contradicts the official documentation,
trust this.**

Read the [README](../README.md) first for what the bot does, how to install it, and how to
drive it from Telegram. This document is for maintainers and the curious: the exact API
shapes, the per-event cost of each command, the Qoder resource lifecycle, and what happens
when the credential is rotated.

Contents:

- **Phase 0 findings — verified API truth** (sections 1–15): artifact delivery, file
  downloads, uploads, event shapes, streaming, resume, credits, the model catalog, request
  shapes, agent updates, reasoning text, `model.effort`, and context compaction.
- **What lives in the Qoder account — cost & lifecycle**: what each command creates remotely.
- **Rotating the PAT, or moving to another account**: the full recovery mechanics.

---

## Phase 0 findings — verified API truth

Everything below was observed empirically against the live API on 2026-09-07 with throwaway
probe scripts that are no longer part of the tree. **Where this contradicts the published
docs, trust this.**

### 1. `agent.artifact_delivered` is a real, undocumented event

The docs list no artifact event and never explain how `DeliverArtifacts` output reaches you.
It arrives as a first-class event — use this, not poll-diffing `/files`:

```json
{
  "type": "agent.artifact_delivered",
  "id": "evt_...",
  "file_id": "file_...",
  "original_filename": "deck.pptx",
  "size": 30097,
  "content_type": "application/octet-stream",
  "processed_at": "2026-09-07T18:59:46.679111Z"
}
```

Note `content_type` is often a useless `application/octet-stream`; trust `original_filename`
for the extension instead.

### 2. `GET /files/{id}/content` returns a JSON envelope, NOT bytes

This is the single most surprising finding. It returns a short-lived pre-signed URL:

```json
{"expires_at": "2026-09-07T19:59:51Z", "url": "https://qoder-cloud-agents-storage-sg.oss-ap-southeast-1.aliyuncs.com/files%2F..."}
```

Downloading an artifact is therefore **three** steps:

1. read `file_id` from `agent.artifact_delivered`
2. `GET /api/v1/cloud/files/{file_id}/content` → parse `{expires_at, url}`
3. `GET url` (follow redirects) → the real bytes

The URL expires roughly **one hour** after issue, so fetch it immediately; never cache it.
`403` means `downloadable:false` (agent-internal file).

### 3. Binary uploads work — the API reference page is stale

`POST /files` accepts png, zip, pptx and text alike. The reference page still says "Only
text-based files are accepted. Binary document, image, audio, video, and archive files are
rejected"; the July 2026 release notes are correct. Uploaded inputs come back with
`downloadable: false` (they are inputs, not outputs).

### 4. `POST /sessions/{id}/resources` takes a BARE object

Not a `{"resources": [...]}` wrapper — that returns
`400 unknown field "resources"`. Correct body:

```json
{"type": "file", "file_id": "file_...", "mount_path": "/data/workspace/uploads/x.png"}
```

Response includes `id` (`sesr_...`) and the resolved `mount_path`. Omitting `mount_path`
defaults to `/mnt/session/uploads/<file_id>`. This endpoint accepts **only** `type:"file"`;
`memory_store`, `github_repository` and `git_repository` must be attached in the `resources`
array at session **creation** time.

### 5. `user.message` content blocks: only `text` and `image`

The API says so explicitly: `content[1].type "file" is not supported. Allowed: text, image.`
An `image` block does **not** accept `file_id` (`unknown field "file_id"`). The working shape
is inline base64:

```json
{"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "<b64>"}}
```

**But prefer mount + `Read` over base64.** The `Read` tool decodes images natively:

```
Read image: /data/workspace/uploads/vision_target.png
Original: 64x32, 108 B, .png
Returned: 64x32, 108 B, image/png, kept original
```

A model asked about a mounted 64×32 solid-blue PNG answered "64×32 pixels and is solid blue"
— real vision, for 0.03 credits. Mounting avoids base64's ~33% inflation against the 4 MB
request-body cap and avoids paying image tokens on every subsequent turn. Use inline base64
only when a file must appear in the message itself.

### 6. Events are FLAT, not `content[]`-wrapped

Observed top-level keys per type:

| type | top-level keys |
|---|---|
| `agent.tool_use` | `id, type, name, input, evaluated_permission, processed_at` |
| `agent.tool_result` | `id, type, tool_use_id, is_error, content[], processed_at` |
| `agent.message` | `id, type, content[], processed_at` |
| `agent.thinking` | `id, type, processed_at` (content may be absent) |
| `agent.artifact_delivered` | `id, type, file_id, original_filename, size, content_type, processed_at` |
| `span.model_request_end` | `id, type, is_error, model_request_start_id, model_usage, processed_at` |
| `session.status_idle` | `id, type, stop_reason, processed_at` |
| `session.thread_status_*` | `id, type, agent_name, session_thread_id, processed_at` |

So `tool_use.name` and `tool_use.input` are **top-level**, while `message`/`tool_result`
carry `content: [{"type":"text","text":...}]`.

`session.thread_status_running` / `_idle` appear even for a single-agent session (there is
always a default coordinator thread, `sthr_...`). Ignore them.

### 7. Incremental streaming shapes

Opt in with `?event_deltas[]=agent.message&event_deltas[]=agent.thinking`. The SSE `event:`
field is literally `event_start` / `event_delta`, and `id:` is the event id:

```json
{"type":"event_start","event":{"id":"evt_...","type":"agent.message"}}
{"type":"event_delta","event_id":"evt_...","delta":{"type":"content_delta","index":0,"content":{"type":"text","text":"1\n2\n3"}}}
```

Deltas arrive in **chunks, not per token** (6 delta frames for a 40-line reply). The final
buffered `agent.message` is authoritative — replace the accumulated text with it rather than
trusting the deltas to be complete.

### 8. `Last-Event-ID` resume

- Valid id → `HTTP 200`, replays every event **after** that id, in order.
- Unknown id → `HTTP 404` `not_found_error` "Event '...' was not found."
- Malformed id → also `404`, not `400`.

So reconciliation must treat `404` as "cursor is stale, rebuild from `GET /events`".

### 9. Credits are per model request — SUM them per turn

One turn emits several `span.model_request_end` events. The PPT turn cost
`6.66 + 0.96 + 0.59 = 8.21` credits. Never read a single span as the turn cost.

### 10. Model catalog (18 models, live)

Re-read on **2026-09-13**: 18 models. The table below omits `available_context_windows` and
`default_context_window`; section 12 explains why those matter and cannot be guessed.

| id | name | price | vision | max input |
|---|---|---|---|---|
| `lite` | Lite | **0** | no | 200 000 |
| `qmodel` | Qwen3.7-Plus | **0.04** | yes | 1 000 000 |
| `qfmodel` | Qwen3.8-Flash | 0.04 | yes | 180 000 |
| `gfmodel` | GLM-5.3-Flash | 0.1 | yes | 1 000 000 |
| `qmodel_latest` | Qwen3.7-Max | 0.1 | yes | 1 000 000 |
| `qmodel_38max` | Qwen3.8-Max | 0.2 | yes | 180 000 |
| `mmodel` | MiniMax-M3 | 0.2 | yes | 1 000 000 |
| `dfmodel` | DeepSeek-Flash | 0.2 | yes | 1 000 000 |
| `efficient` | Efficient | 0.3 | yes | 200 000 |
| `kmodel` | Kimi-K2.8-Preview | 0.3 | yes | — |
| `gmodel` | GLM-5.3 | 0.6 | yes | 180 000 |
| `dmodel` | DeepSeek-V4-Pro | 0.8 | yes | 1 000 000 |
| `kmodel_latest` | Kimi-K3 | 0.8 | yes | 180 000 |
| `auto` | Auto | 1.0 | yes | 200 000 |
| `performance` | Performance | 1.1 | yes | 1 000 000 |
| `ultimate` | Ultimate | 1.6 | yes | 1 000 000 |
| `cmodel` | Cantus | 3.2 | yes | 180 000 |
| `smodel` | Sonus | 3.2 | yes | 180 000 |

> **Drift since the 2026-09-10 capture**, which is the reason `pick_model` resolves against the
> live catalog rather than a table like this one: `smodel` (Sonus) was missing from the table
> entirely; `gfmodel` repriced 0.05 → **0.1**; `dfmodel` was renamed DeepSeek-V4-Flash →
> **DeepSeek-Flash** and repriced 0.3 → **0.2**; `kmodel` was renamed Kimi-K2.7-Code →
> **Kimi-K2.8-Preview** and now reports **no `max_input_tokens` at all**. Vision is the `is_vl`
> flag. Treat every figure here as a snapshot, not a contract.

**Default is `qmodel_38max`** (Qwen3.8-Max) — vision-capable, price factor 0.2, and unlike
`qmodel` it advertises effort levels (`low` / `medium` / `xhigh`), so `/effort` can bring it down
from its `xhigh` default. Given that effort multiplies the real cost of every request (section
14), being tunable is worth more than the cheaper factor: `qmodel` (Qwen3.7-Plus, 0.04, 1M-token
window) costs less per token but advertises no `efforts` at all and so cannot be tuned.
Measured: the same style of turn cost **8.21 credits on `ultimate`** (effort `low`) versus
**0.03 credits on `qmodel`**.

> **Correction, 2026-09-13.** That 0.03 is an outlier, not a baseline, and `price_factor` cannot
> be used to predict what a turn costs. The billing docs put a simple question at **1–3 credits**,
> routine document generation at 10–15, and a complex multi-turn tool task at 20–50; credits
> scale with model, task complexity, context length and tool use, with `price_factor` only a
> "relative multiplier". Measured on `qmodel`: `"hello how are you?"` cost **2.99** credits in
> one session and **0.31** for a byte-identical exchange in another. See section 14 for the
> larger lever, `model.effort`.

`/model` switches **per user**, not per conversation. A session takes its model from its
agent and the API offers no per-session override, so the bot provisions one agent per
Telegram user (`tgagent-user-<id>`) and `/model <id>` updates only that user's agent, with
`PUT` and a `context_window` taken from that model's catalog entry (see section 12). It takes
effect from their next `/new`; a session already created keeps the model it started with.
This is also why a shared agent would not do: one user switching to `ultimate` would silently
change what everyone else's sessions run on.

`lite` is free but has no vision, so it cannot see uploaded images.

### 11. Confirmed working request shapes

Agent with explicit permissions (`evaluated_permission: "allow"` was observed on every tool
call, so this shape is accepted and effective):

```json
{
  "name": "kebab-case-name",
  "model": {"id": "qmodel", "context_window": 200000},
  "system": "...",
  "tools": [{
    "type": "agent_toolset_20260401",
    "enabled_tools": ["Bash", "Read", "Write", "Edit", "Glob", "Grep",
                      "WebFetch", "WebSearch", "ImageSearch", "ImageGen", "DeliverArtifacts"],
    "configs": [{"name": "Bash", "permission_policy": {"type": "always_allow"}}]
  }]
}
```

`model` also accepts a bare string, and an optional `effort` where the model lists `efforts`.
Environment with dependencies:

```json
{"name": "...", "config": {"type": "cloud", "packages": {"pip": ["python-pptx"]},
  "setup_script": "set -euo pipefail\nmkdir -p /data/workspace\n"}}
```

Session: `{"agent": "<agent_id>", "environment_id": "<env_id>"}` → `status: "idle"`.
Turn: `POST /sessions/{id}/events` with `{"events": [{"type":"user.message","content":[{"type":"text","text":"..."}]}]}`.
Cancel: `POST /sessions/{id}/cancel` → `{"status": "canceling"}`.
History: `GET /sessions/{id}/events?order=asc&limit=100&after_id=<evt>` → `{data, has_more}`.

---

### 12. Agents are updated with PUT, and `context_window` is not a free number

Verified against the live API on 2026-09-10. Neither fact is in the docs, and both are needed
before `/model` can work at all.

`OPTIONS /agents/{id}` returns `Allow: GET, POST, PUT, DELETE`. **There is no PATCH** — it
answers `405`. `PUT` replaces the whole object, so an update has to read the agent back and
re-send the complete definition with only the model changed; sending `{"model": ...}` alone
would drop the system prompt and every tool permission. Agents use optimistic concurrency: a
stale `version` in the body returns `409 Version conflict. Expected version 1, got 2.`, and a
successful PUT increments it.

`model.context_window` must be one of that model's own `available_context_windows` from
`GET /models`. Anything else is rejected:

```
400 Field 'model.context_window' is not supported by model 'gmodel'.
```

The catalog splits three ways, so the field cannot be hardcoded:

| models | catalog fields | what to send |
|---|---|---|
| 16 of 18 — `qmodel`, `gmodel`, `ultimate`, `qmodel_38max`, `kmodel`, … | `default_context_window: 200000`, `available_context_windows: [200000, 400000, 1000000]` | `200000`, which is also the platform default |
| `performance` | `default_context_window: 272000`, `available_context_windows: [272000, 400000, 1000000]` | **272000** — 200000 is not offered, so `model_ref` falls through to the model's own default |
| `lite`, `auto`, `efficient` | no window fields at all | **omit `context_window`** |

> **Correction, 2026-09-13.** This table used to single out `kmodel` as offering
> `available_context_windows: [256000]` only, with 200000 refused by a 400. That was true when it
> was observed — the catalog has since moved, and `kmodel` now advertises the standard
> `[200000, 400000, 1000000]`. The lesson survives the specific row: the tiers are per-model and
> do change underneath us, which is exactly why `model_ref` derives the value from the catalog
> entry and gates it against `available_context_windows` instead of sending a constant.

`DEFAULT_CONTEXT_WINDOW = 200000` in config is therefore a **cost ceiling, not a guess at the
default**. On today's catalog it coincides with every model's `default_context_window`, so
preferring it changes nothing — but if a model ever shipped offering
`[200000, 400000, 1000000]` with `default_context_window: 400000`, preferring the constant holds
the session at the cheap tier where preferring the default would silently double the window. A
larger window is not free: compaction (section 15) is what keeps each request's input small, and a
bigger window means the runtime accumulates more history before compacting.

`tgagent.qsessions.model_ref` derives it from the catalog entry. `DELETE /agents/{id}` works
and returns `{"deleted": true}`; nothing in the bot calls it.

---

### 13. No reasoning text — `/think` removed

Verified against the live Qoder Cloud Agents platform on 2026-09-10: despite emitting an
`agent.thinking` event marker, **no reasoning text arrives** at all. Across qmodel_38max, dfmodel,
gmodel and ultimate (with explicit `effort` setting), every probe shows:

- **1** `event_start(agent.thinking)` frame per turn
- **0** `event_delta` frames carrying thinking content (zero chars accumulated)
- **1** buffered `agent.thinking` event with keys `['id','processed_at','type']` — **no `content` field**

The framework used to build a “reasoning display” toggle (`/think`) was therefore removed entirely.
A command that could only ever say "nothing to show" was worse than hiding it: leaving it in the
command menu would invite users to tap it again and again. See Section 10 for the model list
(`qmodel_38max` is the default: vision-capable, 0.2 against `ultimate`'s 1.6, and tunable with
`/effort` — see Section 14) and Section 12 for how models are updated via `PUT /agents/{id}`
rather than the non-existent `PATCH`.

**The marker is still worth rendering, though** (added 2026-09-13). `event_start(agent.thinking)`
is the only *live* signal that reasoning is happening — the buffered event lands after the phase
has ended, which is useless as an indicator. `Renderer` now uses it to show a `thinking…` status
line and retracts it the moment the answer's own `event_start(agent.message)` arrives, plus on a
tool call, a platform retry, and every turn-ending status.

That fills a real hole rather than adding decoration. `flush_status` retracts a status message
whose only content is filler, which is why `"working…"` never reaches the chat — so a turn that
reasons for a minute without calling a single tool used to render as **nothing at all** until the
answer landed. It is **not** gated on the `/tools` preference: it originally was, as activity
feedback of the same kind as the tool list, but it is now kept visible even when tools are hidden
(`renderer.py`, `agent.thinking` branch of `_apply_delta`) — hiding tool activity is not a request
for total silence during a long reasoning phase. (The compaction banner in section 15 is likewise
not gated — that reports conversation state, not something the agent did.)

**Fixing that exposed a worse bug behind the same gate.** The retraction used to be
`if not self.tool_lines or not text` — *no tool lines, retract* — which meant every informative
status line was swallowed on any turn that called no tools. That includes `session.error`'s
`⚠ <type>: <message>`, `session.status_rescheduled`'s retry notice, `_idle_status`'s
`stopped (retries_exhausted)`, `waiting for approval`, and `session ended`. `convo._on_error` and
`_on_terminated` post no notice of their own — they only set internal state and wake the pump — so
the status message was the **only** channel that information had. Net effect: on a plain
question-and-answer turn with no tools, an API error was completely invisible. The user saw their
message go in and nothing come back.

The rule is now inverted. `QUIET_STATUS_LINES` names the only two lines that are pure filler
(`STATUS_WORKING`, `STATUS_DONE`) and everything else justifies a message of its own via
`Renderer.status_worth_a_message`. Inverted on purpose: a new status line now defaults to being
shown, where the old default silently hid it. Both constants exist because `flush_status` branches
on the values, and a literal retyped at one of the four assignment sites would quietly reintroduce
the bug. This also means `/tools` off no longer hides failures — hiding tool activity is not a
request to be told nothing when a turn dies.

`qclient` had been requesting `event_deltas[]=agent.thinking` since Phase 0 and discarding the
frame in `_apply_delta`; the parameter is now load-bearing. There is still no reasoning *text*, so
`/think` stays removed and `users.show_thinking` stays vestigial.

---

### 14. `model.effort` — the cost lever the catalog hides

Verified against the live API on 2026-09-13. `effort` is an optional field of the Agent `model`
object, but what actually decides the cost when you omit it is each model's `default_effort` —
and that is not the cheap end:

| `default_effort` | models |
|---|---|
| `max` | `gmodel`, `gfmodel`, `dfmodel`, `kmodel`, `kmodel_latest`, `dmodel` |
| `xhigh` | `qfmodel`, `qmodel_38max` |
| `high` | `smodel`, `cmodel`, `ultimate` |
| `medium` | `performance` |
| *none — not adjustable* | `lite`, `qmodel`, `qmodel_latest`, `mmodel`, `efficient`, `auto` |

Ten of eighteen therefore run at `max`, `xhigh` or `high` unless the agent says otherwise, which
moves real cost far more than `price_factor` does: `gmodel` is only 0.6× but defaults to `max`.

The API validates effort two different ways, both observed:

```
{"id":"gmodel","effort":"low"}     -> 200, stored as {"context_window":200000,"effort":"low","id":"gmodel"}
{"id":"gmodel","effort":"medium"}  -> 400 Field 'model.effort' is not supported by model 'gmodel'.
{"id":"gmodel","effort":"turbo"}   -> 400 Field 'model.effort' must be one of: none, low, medium, high, xhigh, max.
{"id":"qmodel","effort":"low"}     -> 400 Field 'model.effort' is not supported by model 'qmodel'.
```

The middle two are the important pair. `medium` is a *valid level* that `gmodel` simply does not
offer — its `efforts` are `low`/`high`/`max` — while `qmodel` advertises no `efforts` at all and
rejects every level. So the only authoritative list is each model's own `efforts` array; the
global level list is not sufficient to validate against.

That makes the gate in `tgagent.qsessions.model_ref` load-bearing rather than cosmetic. Effort
is a stored per-user preference that OUTLIVES a model switch, so replaying it blindly would send
`effort:"low"` to `qmodel` at the next provisioning and 400 — failing conversation creation
outright, not just the model change. `model_ref` drops any level the target model does not
advertise, quietly returning it to its own default.

`/effort` shows and sets it, validated against the current model's `efforts`; `/model` lists each
model's `default_effort` beside its price. Both take effect from the next `/new`, because a
session keeps the configuration it was created with — `GET /sessions/{id}` embeds the agent
including `model.effort` and a read-only `effective_context_window`, which is what `/health`
reads to report the model actually running.

---

### 15. Context overflow is auto-compacted server-side, and the event is barely documented

Researched 2026-09-13. When a session's history outgrows its `context_window` (section 12), the
platform **compacts it itself**: earlier detail is replaced by a denser summary and the turn
continues. It is not an error, not a truncation, and not a `session.error`.

The client has **no control over it**. `POST /sessions/{id}/events` accepts exactly seven client
event types — `user.message`, `user.interrupt`, `user.tool_confirmation`, `user.tool_result`,
`user.custom_tool_result`, `user.define_outcome`, `system.message` — so there is no `/compact`
equivalent, no threshold to set, and no way to opt out. `effective_context_window` on a session is
response-only and the docs call it "informational".

The evidence that compaction happens at all is one entry in one list. `agent.thread_context_compacted`
appears among the public event types in the Session schemas page, and **nowhere else**: the SSE
Event Stream page does not mention it, the multiagent thread-event table does not, and the webhook
catalog does not include it. So its payload fields, its trigger threshold and its exact semantics
are all undocumented. The mechanism is described only on the IDE-facing *Context compaction* page,
which covers the same runtime: "Qoder can also compact automatically when the runtime needs more
room", "Compaction does not delete the task history; it prepares a denser summary for later
requests", and "**Compaction is lossy by design.**"

What the bot does with it, in `Renderer.apply`: logs at INFO and raises a `⟲ context compacted —
earlier detail was summarised` banner in that turn's status message. Three deliberate choices:

- **Nothing is read from the payload.** With no schema, guessing at fields would be inventing
  behaviour; the event type alone is the signal.
- **A banner, not a `status_line`.** `status_line` is overwritten by the next tool call and again
  by the idle transition, so a compaction landing early in a turn would have vanished before the
  answer arrived. Its own field keeps it on screen for the rest of the turn, and the next
  `session.status_running` clears it — each turn still gets its own status message.
- **Not gated on `/tools`**, like the `thinking…` marker in section 13 (both are always visible).
  Hiding tool activity is not a request to be kept ignorant of a lossy change to conversation
  state.

Before this the event fell through `apply`'s catch-all *and* `history.record_frame`'s, so a
compacted session was invisible in the chat **and** in the logs — "it forgot what I said earlier"
would have been undiagnosable. It is still not recorded in the local transcript: it carries no
content, and the transcript is what gets replayed into a resumed session.

Acknowledgement is safe without any new code. `qstream.DEFERRED_ACK_TYPES` is exactly
`{"agent.message"}`, so every other event type — this one included — is acked at `offer` and cannot
stall the durable cursor behind it.

**Not verified:** this has never been observed firing against a live session, so the banner's
actual appearance and cadence are untested in production. The harness covers the rendering; the
platform side is inference from the event's name and the IDE docs.

Do not confuse this with `HISTORY_MAX_EVENTS` / `HISTORY_CONTEXT_BUDGET`, which bound the *local*
transcript injected when a lost session is rebuilt. They have no effect on a cloud session's
context.

---

## What lives in the Qoder account — cost & lifecycle

The five remote resources are summarised in the README under *How it works*. What follows
is the exact cost of each interaction, and the deletion policy.

Per-event cost:

* `/start`, `/help`, `/model`, `/effort`, `/tools`, `/files`, `/usage`, `/health`, `/sessions` —
  **no** new remote resources. `/model` and `/effort` `PUT` the user's existing agent (there is
  no `PATCH` — see section 12); if they have none yet the choice is stored locally and applied
  when their first agent is created. `/sessions` reads only the local database, which is why it can also offer ⟳ RESTORE
  for a conversation whose cloud session is gone.
* `/archive` — one `POST /sessions/{id}/archive`, which closes the sandbox. Nothing is deleted.
* A stranger messages the bot — nothing is created, remotely or locally. They are rejected by
  the allowlist and told their own user id, so they can pass it to the administrator instead of
  the administrator having to find it in the logs. That reply is throttled to one per user per
  minute (`config.REJECT_REPLY_INTERVAL_S`), because an unthrottled rejection let anyone who
  found the bot make it issue one Telegram sendMessage per message they sent. The same throttle
  bounds the "Not authorized" alert shown when a non-allowed user taps a stale inline button —
  though `query.answer()` itself is always sent, since suppressing it would leave the button
  spinning with no way to tell a refusal from a hang. With an empty allowlist (first-run
  discovery) the same throttled reply is the owner's way of learning their own id, and still
  nothing is created.
* `/new` — one new **session**. On a user's very first conversation, also one **agent** and one
  **memory store**.
* An ordinary message in an existing conversation — no new resources; it posts an event to the
  session that already exists.
* A boot after Android killed the process — `GET` for the environment and the model catalog,
  then one `GET /sessions/{id}` per conversation worth reconciling, four at a time. "Worth
  reconciling" means the conversation the user is currently addressing, plus any that were
  mid-turn when the process died. Everything else is picked up lazily when the user switches
  back to it, so boot cost does not grow with the number of conversations ever created.

Nothing is ever deleted remotely, and nothing is auto-archived for being idle. Only the
sandbox *filesystem* is reclaimed after 24 hours of inactivity; the session itself keeps its
whole conversation history server-side, so an old conversation is still worth returning to.
Archiving closes a sandbox; environments, agents and memory stores persist until removed from
the console. Renaming `ENV_NAME` or the agent prefix therefore orphans the old resource in the
account — the bot forgets the stale local row and logs the orphaned id, but leaves the remote
resource alone, because deleting it is irreversible and affects an account other code may use.

---

## Rotating the PAT, or moving to another account

A session belongs to the credential that created it. Point the bot at a new PAT — a rotated
token, or a different Qoder account entirely — and every session the old one created returns
`404`. Nothing else about the account is reusable either: the environment, the per-user agents
and the memory stores all have to exist again under the new credential.

This is a supported transition, not a failure. What the bot does:

* **Nothing is treated as deleted.** `reconcile` retires an unreachable conversation with
  `lost_session = 1` rather than `deleted_at` alone, which is the flag that separates "the
  credential changed under me" from "the user pressed /archive". Only a lost conversation is
  offered back.
* **The account is re-provisioned lazily.** The environment on the next boot, and each user's
  agent and memory store on their next conversation. A cached id that 404s is forgotten and
  recreated; an id that merely failed to validate on a flaky link is kept, because minting a
  duplicate on every unstable boot is how an account fills up with orphans.
* **The conversation is rebuilt from the on-device transcript**, on a fresh session, with the
  memory store attached exactly as a brand-new conversation would have it.

Three things can trigger that rebuild, and all three behave the same way:

| trigger | where |
|---|---|
| boot reconcile, then the user's next message | `Manager.reconcile` → `handlers._active` |
| the stream or the poll fallback sees the session 404 | `StreamConsumer.run` / `Conversation._poll_history` |
| a dispatch to the dead session returns 404 | `Conversation._dispatch_burst` |

Whichever fires, anything still sitting in that conversation's durable inbound queue is carried
across and re-queued behind the transcript. That matters most for the third case: the message
that *revealed* the dead session is the one already in the queue, and dropping it would mean
the user watches an error and then has to retype what they said.

What survives, and what does not:

* **Survives** — the newest `HISTORY_CONTEXT_BUDGET` characters of the transcript, and any file
  whose copy is still in `filecache/`, re-uploaded and re-mounted into the new sandbox.
* **Does not survive** — the old sandbox filesystem (a new session is a new sandbox), and the
  old account's server-side session history, which the new credential cannot see at all.

The rebuild is capped at `AUTO_RESUME_MAX_ATTEMPTS` **consecutive** failures per chat. The
counter clears on a successful rebuild, so three losses spread across a month — each recovered
cleanly — do not add up to silently disabling automatic recovery until the process happens to
restart. A session that vanishes again immediately after being created is not something another
rebuild will fix, and an uncapped loop would mint a paid session every few seconds; when the cap
does trip, the transcript is still on disk and ⟳ RESTORE in `/sessions` still works.

`/health` shows a non-reversible fingerprint of the loaded credential — so you can tell at a
glance which token the bot is holding — and how many conversations are awaiting a ⟳ RESTORE.
Resources left behind in the old account are never deleted remotely; they are forgotten locally
and logged, because deleting them is irreversible and affects an account other code may use.
