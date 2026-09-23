"""Qoder resource operations: models, environments, agents, sessions, events.

Provisioning is idempotent and cached in SQLite, because the bot restarts often (Android
kills it) and must not create a fresh environment and agent on every boot.

All request shapes here are the ones Phase 0 verified against the live API. Where they differ
from the published docs, README.md records the discrepancy.
"""

from __future__ import annotations

import asyncio
import json
import logging

from . import auth, config
from .db import Database, utcnow
from .qclient import BillingError, Conflict, NotFound, QoderClient, QoderError, iter_pages, iter_session_events

log = logging.getLogger("tgagent.qoder")


def build_tools(tools: list[str] | None = None) -> list[dict]:
    """Toolset with every tool explicitly allowed.

    The default permission policy for built-in tools is undocumented, so we set
    ``always_allow`` on each one rather than rely on it. Observed effect: every
    agent.tool_use event comes back with ``evaluated_permission: "allow"``.

    Client-side ``{"type": "custom"}`` tools are deliberately never used: they ignore
    permission_policy and always pause the session waiting for us.
    """
    names = tools or config.DEFAULT_TOOLS
    return [
        {
            "type": config.AGENT_TOOLSET,
            "enabled_tools": list(names),
            "configs": [{"name": n, "permission_policy": {"type": "always_allow"}} for n in names],
        }
    ]


def text_block(text: str) -> dict:
    return {"type": "text", "text": text}


def model_ref(
    model: dict, preferred_window: int | None = None, effort: str | None = None
) -> dict:
    """Build the ``model`` object the agents API will accept for a catalog entry.

    ``context_window`` is not a free number: it must be one of the model's own
    ``available_context_windows``, and sending anything else returns
    ``400 Field 'model.context_window' is not supported by model '<id>'``. Several models
    (``auto``, ``lite``, ``efficient``) list no windows at all and reject the field outright,
    so it is omitted for them.

    Hardcoding a window therefore breaks switching to those models. The catalog is the only
    source of truth and it drifts: README §10 records ``kmodel`` reporting no
    ``max_input_tokens`` at all as of 2026-09-13, where an earlier capture had it at 256000.
    A specific number quoted here would outlive the change it describes, so none is.

    ``effort`` is gated the same way, and the gate is load-bearing rather than cosmetic. The
    API rejects a level the model does not advertise — ``400 Field 'model.effort' is not
    supported by model 'qmodel'`` for one of the six models that list no ``efforts`` at all, and
    ``400 Field 'model.effort' must be one of: none, low, medium, high, xhigh, max`` for an
    unknown word. But the effort is a stored user preference that OUTLIVES a model switch, so a
    level chosen for ``gmodel`` (low/max/high) would otherwise be replayed against ``qmodel`` on
    the next provisioning and 400 — failing conversation creation outright. An unadvertised
    level is dropped here instead, quietly returning that model to its own ``default_effort``.
    """
    ref: dict = {"id": model["id"]}

    if effort and effort in (model.get("efforts") or []):
        ref["effort"] = effort

    windows = model.get("available_context_windows") or []
    for candidate in (
        preferred_window,
        model.get("default_context_window"),
        windows[0] if windows else None,
    ):
        if candidate in windows:
            ref["context_window"] = candidate
            break
    return ref


# The three tunable fields of an Agent model object. Everything else the API returns —
# effective_context_window, for instance — is read-only and must not take part in a comparison.
MODEL_KEYS = ("id", "effort", "context_window")


def model_matches(current, wanted: dict) -> bool:
    """Whether an agent's stored model already equals the reference we would send.

    Compared across every tunable field, not by id alone: /effort changes the model object
    WITHOUT changing its id, so an id-only comparison reported "already correct", skipped the
    PUT, and the user's effort never reached the agent. A bare-string ``model`` — the shape
    ``_create_agent``'s last-resort retry sends — is normalised first.
    """
    if not isinstance(current, dict):
        current = {"id": current} if current else {}
    return all(current.get(key) == wanted.get(key) for key in MODEL_KEYS)


class QoderAPI:
    def __init__(self, client: QoderClient, db: Database):
        self.client = client
        self.db = db
        # Per-user provisioning locks, shared by ensure_user_agent and ensure_memory_store.
        # Both do check-cache -> list-remote -> create, and the Manager's own lock is keyed on
        # (chat_id, message_thread_id) — so the SAME user opening two conversations concurrently
        # (a DM and a group topic, which concurrent_updates(10) permits) raced both and could
        # mint duplicate agents or memory stores, orphaning one remotely where nothing ever
        # deletes it. Keying on tg_user_id closes that without serialising different users.
        # Bounded in practice by the allowlist; no eviction is worth the complexity here.
        self._provision_locks: dict[int, asyncio.Lock] = {}

    def provision_lock(self, tg_user_id: int) -> asyncio.Lock:
        """The provisioning lock for one user. Created lazily; safe to call from async code."""
        lock = self._provision_locks.get(tg_user_id)
        if lock is None:
            lock = asyncio.Lock()
            self._provision_locks[tg_user_id] = lock
        return lock

    # --- models -------------------------------------------------------------------

    async def list_models(self, *, use_cache: bool = True) -> list[dict]:
        """GET /models. Note: the docs page is titled "List models" but the path is /models."""
        if use_cache:
            raw = self.db.kv_get("models_catalog")
            if raw:
                try:
                    return json.loads(raw)
                except json.JSONDecodeError:
                    pass
        payload = await self.client.get("/models")
        models = payload.get("data", [])
        self.db.kv_set("models_catalog", json.dumps(models))
        return models

    async def pick_model(self, preferred: str, *, strict: bool = False) -> dict:
        """Resolve a model id against the catalog.

        Lenient by default: falling back to something usable is right when provisioning a
        session, where failing the whole conversation because a model id went stale is worse
        than running on the default.

        ``strict`` is for an explicit user choice, where substituting is a lie — /model would
        report "Model set to X" while the agent runs on the default. Strict also reads the LIVE
        catalog rather than the cache, because a stale cache is precisely what hides a model
        that the account does offer (ids, names and prices all drift: ``dfmodel`` and ``kmodel``
        have both been renamed since this project's Phase 0 capture).
        """
        models = await self.list_models(use_cache=not strict)
        for model in models:
            if model.get("id") == preferred and model.get("is_enabled", True):
                return model
        if strict:
            raise QoderError(
                400,
                f"model {preferred!r} is not available on this account",
                error_type="invalid_request_error",
                param="model",
            )
        # Lenient miss against the cache. Before substituting the default, re-read the live
        # catalog once: ids, names and prices drift (README §10 records two renames since the
        # Phase 0 capture), and a stale cache is precisely what hides a model the account now
        # offers. Without this the lenient path silently fell back to DEFAULT_MODEL forever
        # after a rename, with no path to refresh short of an explicit strict /model.
        live = await self.list_models(use_cache=False)
        for model in live:
            if model.get("id") == preferred and model.get("is_enabled", True):
                log.info("model %r resolved from the live catalog after a cache miss", preferred)
                return model
        models = live
        for fallback in (config.DEFAULT_MODEL, "auto", "lite"):
            for model in models:
                if model.get("id") == fallback and model.get("is_enabled", True):
                    log.warning("model %r unavailable, falling back to %r", preferred, fallback)
                    return model
        if models:
            return models[0]
        raise QoderError(502, "model catalog is empty", error_type="unexpected_shape")

    # --- environments -------------------------------------------------------------

    async def ensure_environment(
        self,
        name: str = config.ENV_NAME,
        packages: dict | None = None,
        setup_script: str | None = None,
    ) -> str:
        """Find or create the shared environment. Environments are templates, not sandboxes.

        They are NOT an isolation boundary between users — the container is provisioned when a
        session starts. So never put per-user secrets in setup_script.

        A nonzero exit from setup_script aborts session startup, which is why the default
        script only creates a directory; the heavy lifting is done by `packages`.
        """
        packages = packages if packages is not None else config.ENV_PACKAGES
        setup_script = setup_script if setup_script is not None else config.ENV_SETUP_SCRIPT

        cached = self.db.query_one("SELECT env_id FROM environments WHERE name = ?", (name,))
        if cached:
            # Only a definitive 404 proves the cached id is dead. A blip on a mobile link is
            # not proof, and treating it as such mints a duplicate environment on every
            # unstable boot — which is how the account accumulates orphans nobody cleans up.
            try:
                await self.client.get(f"/environments/{cached['env_id']}")
                log.debug("validated existing environment %s", cached["env_id"])
                return cached["env_id"]
            except NotFound:
                log.warning("cached environment %s no longer exists; creating a fresh one",
                            cached["env_id"])
                self._forget_environment(cached["env_id"])
            except QoderError as exc:
                log.warning("could not validate environment %s (%s); using it anyway",
                            cached["env_id"], exc.message)
                return cached["env_id"]

        try:
            async for env in iter_pages(self.client, "/environments"):
                if env.get("name") == name and not env.get("archived_at"):
                    self._remember_environment(env, packages, setup_script)
                    return env["id"]
        except QoderError as exc:
            log.warning("could not list environments (%s); creating a new one", exc)

        created = await self.client.post(
            "/environments",
            {
                "name": name,
                "description": "Telegram agent sandbox",
                "config": {
                    "type": "cloud",
                    "packages": packages,
                    "setup_script": setup_script,
                },
            },
        )
        self._remember_environment(created, packages, setup_script)
        log.info("created environment %s", created["id"])
        return created["id"]

    def _forget_environment(self, env_id: str) -> None:
        self.db.execute("DELETE FROM environments WHERE env_id = ?", (env_id,))

    def prune_stale_environments(self, name: str = config.ENV_NAME) -> list[str]:
        """Drop local rows left behind by renaming ENV_NAME. Returns the orphaned remote ids.

        Renaming the environment does not delete the old one in the Qoder account, and the old
        row here would otherwise sit forever looking like a live resource. Deleting remotely is
        deliberately NOT done: it is irreversible and affects an account other code may use.
        """
        stale = self.db.query("SELECT env_id, name FROM environments WHERE name != ?", (name,))
        for row in stale:
            log.warning(
                "forgetting local environment %s (%r); it is orphaned in the Qoder account "
                "and can be deleted from the console",
                row["env_id"], row["name"],
            )
            self._forget_environment(row["env_id"])
        return [row["env_id"] for row in stale]

    def _remember_environment(self, env: dict, packages: dict, setup_script: str) -> None:
        self.db.execute(
            """INSERT INTO environments(env_id, name, packages_json, setup_script, created_at)
               VALUES(?, ?, ?, ?, ?)
               ON CONFLICT(env_id) DO UPDATE SET
                 packages_json = excluded.packages_json,
                 setup_script  = excluded.setup_script""",
            (
                env["id"],
                env.get("name"),
                json.dumps(packages),
                setup_script,
                utcnow(),
            ),
        )

    # --- agents -------------------------------------------------------------------

    async def ensure_user_agent(self, tg_user_id: int, model_id: str) -> str:
        """Find or create THIS user's agent, running on ``model_id``. Returns the agent id.

        One agent per user rather than one shared agent, because a session inherits its model
        from the agent and the API offers no per-session override. With a shared agent there is
        no way to change the model for one user without changing it for everyone, which is why
        /model used to be written to the database and then quietly ignored.
        """
        name = config.agent_name(tg_user_id)
        async with self.provision_lock(tg_user_id):
            cached = self.db.query_one("SELECT agent_id FROM agents WHERE tg_user_id = ?", (tg_user_id,))
            if cached:
                # Only a definitive 404 discards the cache. A blip on a mobile link is not proof
                # the agent is gone, and treating it as such mints a duplicate on every bad boot.
                try:
                    remote = await self.client.get(f"/agents/{cached['agent_id']}")
                except NotFound:
                    log.warning("cached agent %s no longer exists; creating a fresh one",
                                cached["agent_id"])
                    self._forget_agent(cached["agent_id"])
                except QoderError as exc:
                    log.warning("could not validate agent %s (%s); using it anyway",
                                cached["agent_id"], exc.message)
                    return cached["agent_id"]
                else:
                    log.debug("validated agent %s for user %s", cached["agent_id"], tg_user_id)
                    self._remember_agent(remote, tg_user_id)
                    await self._sync_model(remote, model_id, tg_user_id)
                    return remote["id"]

            agent = await self._adopt_agent_by_name(name, tg_user_id)
            if agent is None:
                agent = await self._create_agent(name, tg_user_id, model_id)
                log.info("created agent %s for user %s", agent["id"], tg_user_id)
            else:
                await self._sync_model(agent, model_id, tg_user_id)
            return agent["id"]

    def agent_for_user(self, tg_user_id: int) -> str | None:
        """This user's cached agent id, or None if they have never started a conversation."""
        row = self.db.query_one("SELECT agent_id FROM agents WHERE tg_user_id = ?", (tg_user_id,))
        return row["agent_id"] if row else None

    async def apply_agent_model(self, tg_user_id: int, model_id: str) -> bool:
        """Re-apply this user's model AND effort to their agent.

        Returns whether the change reached an agent. False means the user has no agent yet, in
        which case ``users.model_id`` and ``users.effort`` carry the choice and the agent is
        created with both on their next conversation — so both callers report it as "takes
        effect from your next /new" rather than as a failure.

        A bool, not the agent id: every caller only ever asked "did this land?", and returning
        an id left them branching on the truthiness of a string.

        One PUT for both fields because they belong to the same object and PUT replaces it
        whole. /model and /effort each change one but must send the other unchanged, so they
        share this path rather than each assembling a partial update.

        Strict: an explicit user choice must surface as an error rather than be quietly replaced
        by the default while the command reports success.
        """
        agent_id = self.agent_for_user(tg_user_id)
        if agent_id is None:
            return False
        await self.set_agent_model(
            agent_id, await self._model_ref(model_id, tg_user_id, strict=True)
        )
        log.info("user %s agent %s set to %s", tg_user_id, agent_id, model_id)
        return True

    def _effort_for(self, tg_user_id: int) -> str | None:
        """This user's stored effort preference, or None to let the model use its own default."""
        user = auth.get_user(self.db, tg_user_id)
        return user["effort"] if user and user["effort"] else None

    async def _model_ref(
        self, model_id: str, tg_user_id: int, *, strict: bool = False
    ) -> dict:
        """Resolve a model id against the catalog and build a reference the API will accept.

        Carries this user's effort, so a single PUT applies both halves of their preference.
        ``model_ref`` drops the effort if the resolved model does not advertise it.
        """
        return model_ref(
            await self.pick_model(model_id, strict=strict),
            config.DEFAULT_CONTEXT_WINDOW,
            self._effort_for(tg_user_id),
        )

    async def _sync_model(self, agent: dict, model_id: str, tg_user_id: int) -> None:
        """Bring a just-adopted agent onto this user's model and effort, if it is not already.

        Compared with :func:`model_matches` rather than by id: an /effort change alters the
        model object without altering its id, so an id-only check reported the agent as already
        correct and the new effort was never sent.

        Also pushes ``AGENT_SYSTEM`` when the remote prompt has drifted from the constant, so
        a prompt change reaches EXISTING users' agents on their next conversation instead of
        only agents created from then on. Drift is only acted on when the API actually returned
        a system prompt: an absent field is not proof of a stale one, and treating it as such
        would PUT (and bump the version of) every agent on every provisioning.
        """
        wanted = await self._model_ref(model_id, tg_user_id)
        current = agent.get("model")
        remote_system = agent.get("system")
        system_stale = (
            isinstance(remote_system, str)
            and bool(remote_system)
            and remote_system != config.AGENT_SYSTEM
        )
        if model_matches(current, wanted) and not system_stale:
            return
        current_id = current.get("id") if isinstance(current, dict) else current
        log.info(
            "agent for user %s is on %r (system prompt %s); updating to %r",
            tg_user_id, current_id, "stale" if system_stale else "current", wanted,
        )
        await self.set_agent_model(
            agent["id"], wanted, system=config.AGENT_SYSTEM if system_stale else None
        )

    async def _adopt_agent_by_name(self, name: str, tg_user_id: int) -> dict | None:
        """Pick up an agent that already exists remotely but is not in our database.

        Agents are one per user, so this is the listing most likely to outgrow a single page.
        Reading only page one made an existing agent invisible, and the caller then created a
        duplicate.
        """
        try:
            async for agent in iter_pages(self.client, "/agents"):
                if agent.get("name") == name and not agent.get("archived_at"):
                    self._remember_agent(agent, tg_user_id)
                    log.info("adopted existing agent %s for user %s", agent["id"], tg_user_id)
                    return agent
        except QoderError as exc:
            log.warning("could not list agents (%s); creating a new one", exc)
            return None
        return None

    async def _create_agent(self, name: str, tg_user_id: int, model_id: str) -> dict:
        resolved = await self.pick_model(model_id)
        ref = model_ref(
            resolved, config.DEFAULT_CONTEXT_WINDOW, self._effort_for(tg_user_id)
        )
        log.debug(
            "resolved model_id=%r for user %s -> %s (price_factor=%s)",
            model_id, tg_user_id, ref, resolved.get("price_factor"),
        )
        payload = {
            "name": name,
            "description": f"Telegram assistant for user {tg_user_id}",
            "model": ref,
            "system": config.AGENT_SYSTEM,
            "tools": build_tools(),
        }
        try:
            created = await self.client.post("/agents", payload)
        except QoderError as exc:
            if exc.status != 400:
                raise
            # Last resort for a stale catalog cache: a bare id is always accepted, and the
            # server applies the model's own default context window.
            log.warning("agent create rejected %r (%s); retrying with a bare model id",
                        payload["model"], exc.message)
            payload["model"] = resolved["id"]
            created = await self.client.post("/agents", payload)
        self._remember_agent(created, tg_user_id)
        return created

    def _remember_agent(self, agent: dict, tg_user_id: int | None = None) -> None:
        """Store an agent definition, tolerating a partial payload.

        A PATCH response is not guaranteed to carry every field. Blindly writing what it omits
        either violates agents.name NOT NULL or, worse, sets tg_user_id to NULL and silently
        detaches the agent from its owner — after which the next lookup mints a duplicate.
        So an omitted field keeps whatever is already stored.
        """
        agent_id = agent.get("id")
        if not agent_id:
            log.warning("agent response had no id; nothing remembered")
            return

        name = agent.get("name")
        if not name:
            # Recover what we can: the owner implies the name, and an existing row has one.
            name = config.agent_name(tg_user_id) if tg_user_id else None
        if not name:
            row = self.db.query_one("SELECT name FROM agents WHERE agent_id = ?", (agent_id,))
            name = row["name"] if row else None
        if not name:
            log.warning("agent %s has no name in the response or on record; not remembered",
                        agent_id)
            return

        self.db.execute(
            """INSERT INTO agents(agent_id, name, tg_user_id, version, model_json, tools_json,
                                 system, created_at)
               VALUES(?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(agent_id) DO UPDATE SET
                 name       = COALESCE(excluded.name, agents.name),
                 tg_user_id = COALESCE(excluded.tg_user_id, agents.tg_user_id),
                 version    = excluded.version,
                 model_json = excluded.model_json,
                 tools_json = excluded.tools_json,
                 system     = excluded.system""",
            (
                agent_id,
                name,
                tg_user_id,
                int(agent.get("version") or 1),
                json.dumps(agent.get("model")),
                json.dumps(agent.get("tools")),
                agent.get("system") or "",
                utcnow(),
            ),
        )

    def _forget_agent(self, agent_id: str) -> None:
        self.db.execute("DELETE FROM agents WHERE agent_id = ?", (agent_id,))

    def prune_stale_agents(self) -> list[str]:
        """Drop local agent rows that belong to no user. Returns the orphaned remote ids.

        This clears out the single shared agent left behind by the move to per-user agents,
        and any row whose owner was removed. Deleting remotely is deliberately NOT done: it is
        irreversible and affects an account other code may use.
        """
        stale = self.db.query(
            "SELECT agent_id, name FROM agents WHERE tg_user_id IS NULL"
        )
        for row in stale:
            log.warning(
                "forgetting local agent %s (%r); it is orphaned in the Qoder account "
                "and can be deleted from the console",
                row["agent_id"], row["name"],
            )
            self._forget_agent(row["agent_id"])
        return [row["agent_id"] for row in stale]

    async def set_agent_model(
        self, agent_id: str, model: str | dict, *, system: str | None = None
    ) -> dict:
        """Change an agent's model (and optionally its system prompt). Returns the updated agent.

        Two things the live API insists on, both discovered the hard way:

        * The verb is **PUT**, not PATCH — ``OPTIONS /agents/{id}`` advertises
          ``GET, POST, PUT, DELETE`` and PATCH answers 405. This method was written against
          PATCH, which is part of why it was never called and /model never worked.
        * PUT replaces the whole object. Sending only ``{"model": ...}`` would drop the system
          prompt and every tool permission, so the current definition is read back and
          re-sent with just the model changed. ``system`` overrides that read-back for the
          one caller that needs to push a changed AGENT_SYSTEM (see :meth:`_sync_model`).

        Agents use optimistic concurrency: a stale ``version`` is a 409, so the version is read
        fresh immediately before each attempt and a conflict retries once.
        """
        for attempt in (0, 1):
            current = await self.client.get(f"/agents/{agent_id}")
            payload = {
                "name": current["name"],
                "description": current.get("description") or "",
                "system": system if system is not None
                          else (current.get("system") or config.AGENT_SYSTEM),
                "model": model,
                "tools": current.get("tools") or build_tools(),
                "version": int(current.get("version") or 1),
            }
            try:
                updated = await self.client.put(f"/agents/{agent_id}", payload)
                self._remember_agent(updated, self._owner_of(agent_id))
                return updated
            except Conflict:
                if attempt:
                    raise
                log.info("agent %s changed underneath us; retrying against a fresh read",
                         agent_id)
        # Unreachable: attempt 0 either returns or continues to attempt 1, and attempt 1 either
        # returns or re-raises the Conflict above. The trailing QoderError(409) that used to sit
        # here was dead code that implied a third path the loop cannot take.

    def _owner_of(self, agent_id: str) -> int | None:
        row = self.db.query_one("SELECT tg_user_id FROM agents WHERE agent_id = ?", (agent_id,))
        return row["tg_user_id"] if row else None

    # --- sessions -----------------------------------------------------------------

    async def create_session(
        self,
        agent_id: str,
        env_id: str,
        *,
        title: str | None = None,
        resources: list[dict] | None = None,
        environment_variables: dict[str, str] | None = None,
    ) -> dict:
        """Create a session. This is where memory stores and repos must be attached —
        the later add-resource endpoint accepts only type "file"."""
        payload: dict = {"agent": agent_id, "environment_id": env_id}
        if title:
            payload["title"] = title[:200]
        if resources:
            payload["resources"] = resources
        if environment_variables:
            payload["environment_variables"] = environment_variables
        return await self.client.post("/sessions", payload)

    async def get_session(self, session_id: str) -> dict | None:
        try:
            return await self.client.get(f"/sessions/{session_id}")
        except NotFound:
            return None

    async def cancel_session(self, session_id: str) -> dict | None:
        """Stop the running turn. Returns the session to idle; it stays reusable."""
        try:
            return await self.client.post(f"/sessions/{session_id}/cancel")
        except Conflict:
            # Already idle or already cancelling: nothing to do, not an error.
            return None

    async def archive_session(self, session_id: str) -> dict | None:
        try:
            return await self.client.post(f"/sessions/{session_id}/archive")
        except (NotFound, Conflict) as exc:
            log.info("archive %s skipped: %s", session_id, exc)
            return None

    async def attach_file(self, session_id: str, file_id: str, mount_path: str | None = None) -> dict:
        """Mount an uploaded file into the sandbox.

        The body is a BARE object. Wrapping it as {"resources": [...]} returns
        400 unknown field "resources" — verified in Phase 0. Omitting mount_path defaults to
        /mnt/session/uploads/<file_id>.
        """
        payload: dict = {"type": "file", "file_id": file_id}
        if mount_path:
            payload["mount_path"] = mount_path
        return await self.client.post(f"/sessions/{session_id}/resources", payload)

    # --- events -------------------------------------------------------------------

    async def send_message(self, session_id: str, text: str) -> dict:
        """Post a user.message. Raises Conflict (409) if a turn is already running.

        There is no server-side queue: the caller must wait for session.status_idle. That is
        what the per-conversation inbound_queue and pump task exist for.

        Attachments are not sent here. They are uploaded, mounted into the sandbox, and named
        in the text — see uploads.build_pointer_text. Inline base64 image blocks work but cost
        33% more body and re-bill image tokens on every later turn.
        """
        if not text:
            raise ValueError("refusing to send an empty user.message")
        return await self.client.post(
            f"/sessions/{session_id}/events",
            {"events": [{"type": "user.message", "content": [text_block(text)]}]},
        )

    async def send_interrupt(self, session_id: str) -> dict | None:
        try:
            return await self.client.post(
                f"/sessions/{session_id}/events", {"events": [{"type": "user.interrupt"}]}
            )
        except (Conflict, NotFound) as exc:
            log.info("interrupt %s skipped: %s", session_id, exc)
            return None

    async def iter_events(self, session_id: str, after_id: str | None = None):
        """Yield every buffered event after a cursor, following pagination.

        Thin wrapper over :func:`tgagent.qclient.iter_session_events`, which is shared with
        the stream consumer's post-404 rebuild so both page identically.
        """
        async for event in iter_session_events(self.client, session_id, after_id):
            yield event

    # --- files --------------------------------------------------------------------

    async def upload(self, contents: bytes, name: str, metadata: dict | None = None) -> dict:
        """Upload a file. Binary is accepted (png, zip, pptx) despite the stale docs page."""
        return await self.client.upload_file(contents, name, metadata)

    async def download(self, file_id: str, *, max_bytes: int | None = None) -> bytes:
        return await self.client.download_file(file_id, max_bytes=max_bytes)


async def provision(api: QoderAPI) -> str:
    """Ensure the shared environment exists and return its id.

    Agents are NOT provisioned here: they are per user, and creating one for every allowlisted
    user at boot would mint resources for people who may never send a message. Each user's
    agent is created on their first conversation, by ``ensure_user_agent``.
    """
    return await api.ensure_environment()


def memory_resource(memstore_id: str, instructions: str | None = None) -> dict:
    """Resource entry attaching a per-user memory store at session creation.

    Memories mount at /data/.qoder/awareness/<path> and the agent edits them as ordinary
    files; each change auto-creates an immutable version. No tool call is involved.
    """
    resource: dict = {
        "type": "memory_store",
        "memory_store_id": memstore_id,
        "access": "read_write",
    }
    if instructions:
        resource["instructions"] = instructions[:4096]
    return resource


def conversation_title(text: str, limit: int = 40) -> str:
    """Derive a short conversation label from its first message."""
    flat = " ".join(text.split())
    return flat[: limit - 1].rstrip() + "…" if len(flat) > limit else flat
