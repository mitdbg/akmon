"""An in-process Open WebUI v0.11.0, faithful to the MEASURED semantics.

Not a mock that returns whatever the test wants. Every behavior below was
measured against a live v0.11.0 container and recorded in
``pact/changes/openwebui-chat-frontend/evidence/``; the point of modelling
them here is that the projection has to survive them:

* ``WEBUI_NAME`` gets ``' (Open WebUI)'`` appended whenever it differs from
  the vendor default, so a naive equality check reports drift forever.
* ``/api/v1/models/sync`` accepts a payload, throws internally, swallows the
  exception, and returns HTTP 200 with ``[]`` while the row is UNCHANGED. A
  projection built on it looks like it works and does nothing.
* ``/api/v1/configs/import`` upserts arbitrary keys and ``/export`` echoes
  them back, so write-then-read proves nothing on its own.
* ``POST /api/v1/functions/id/<id>/valves/update`` validates against the
  function's own Valves model and SILENTLY DROPS unknown fields.
* Model access is a GRANT SET, not a list, and posting ``access_grants``
  REPLACES the whole set — including with an empty list.

Fault injection is deliberately narrow: ``omit_export_keys`` reproduces a
response that leaves a governed toggle out, ``corrupt_export`` reproduces an
uninterpretable value, and ``fail_paths`` reproduces a rejected write. Those
three are the failure shapes the requirements name.

:class:`FakeContainerRuntime` models the OTHER half of the instance — the
container filesystem the static directory lives on — because the census
re-verified that no inbound route writes into it: ``main.py:2823`` mounts it
as ``StaticFiles``, which serves GET and HEAD and exposes no write verb. It
also models the measured GOTCHA that makes that surface easy to get wrong:
the served directory is :data:`STATIC_DIR`, and a write to
:data:`BUILD_STATIC_DIR` changes nothing at all.
"""

from __future__ import annotations

import base64
import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Sequence


SITE_NAME_SUFFIX = " (Open WebUI)"
VENDOR_DEFAULT_NAME = "Open WebUI"

#: MEASURED: the directory the application serves ``/static/<asset>`` from.
STATIC_DIR = "/app/backend/open_webui/static"

#: MEASURED: the frontend build's output. A 165-byte ``custom.css`` written
#: here still served nothing, while the same file written to
#: :data:`STATIC_DIR` served immediately. Modelled so a delivery that reached
#: for the wrong directory fails here rather than passing and failing live.
BUILD_STATIC_DIR = "/app/build/static"

#: The operators a real ``okg mcp-serve`` advertises, and what a successful
#: ``tool_servers/verify`` returns as ``specs`` — measured, seven of them.
MCP_SPECS = (
    "aggregate", "expand", "filter", "inspect", "map", "query", "search",
)


def _default_permissions() -> dict[str, Any]:
    """The v0.11.0 ``UserPermissions`` shape, with its real defaults."""
    return {
        "workspace": {"models": False, "knowledge": False, "prompts": False},
        "sharing": {"public_models": False},
        "access_grants": {},
        # `call` (voice mode) defaults True at v0.11.4 (routers/users.py:253).
        "chat": {"controls": True, "call": True},
        "features": {
            "web_search": True,
            "image_generation": True,
            "code_interpreter": True,
            "memories": True,
            "channels": True,
            "direct_tool_servers": False,
        },
        "settings": {"interface": True},
    }


def _default_config() -> dict[str, Any]:
    """Flat dotted keys, exactly as the vendor's config table stores them."""
    return {
        "web.search.enable": True,
        "image_generation.enable": True,
        "code_interpreter.enable": True,
        "code_execution.enable": True,
        "evaluation.arena.enable": True,
        "channels.enable": True,
        "chat.context_compaction.enable": False,
        "memories.system_context.enable": True,
        "ui.default_models": "",
        "ui.model_order_list": [],
        # NOT `ui.prompt_suggestions`: at v0.11.4 the export returns stored
        # rows only (models/config.py:186-196), and a fresh instance has
        # never stored one -- the browser then shows its own localized set.
        "ui.banners": [],
        # The cut-list keys at their vendor defaults, as an instance that an
        # admin has saved the settings page on would hold them.
        "task.follow_up.enable": True,
        "task.tags.enable": True,
        "task.query.retrieval.enable": True,
        "task.autocomplete.enable": False,
        "ui.enable_community_sharing": True,
        "users.enable_status": True,
        "calendar.enable": True,
        "automations.enable": True,
        "notes.enable": True,
    }


class FakeInstance:
    def __init__(self, *, version: str = "0.11.4", webui_name: str = VENDOR_DEFAULT_NAME) -> None:
        self.version = version
        self.webui_name = webui_name
        self.config: dict[str, Any] = _default_config()
        self.permissions: dict[str, Any] = _default_permissions()
        self.connections: dict[str, Any] = {"ENABLE_DIRECT_CONNECTIONS": True}
        self.models: dict[str, dict[str, Any]] = {}
        self.grants: dict[str, list[dict[str, str]]] = {}
        self.prompts: dict[str, dict[str, Any]] = {}
        self.tool_servers: list[dict[str, Any]] = []
        # -- the MCP endpoint this instance would dial -----------------------
        # `None` models an UNAUTHENTICATED `mcp-serve`; a string models one
        # started with `--auth-token-env`, which is the configuration
        # multi-user SSO refuses to start without.
        self.mcp_auth_token: str | None = None
        self.mcp_specs: list[str] = list(MCP_SPECS)
        # -- a SECOND endpoint (#1183 S4) -----------------------------------
        # The live-tool server: a different URL, its own credential and its
        # own advertised names. Keyed by URL so `verify` answers per
        # connection -- which is the whole point of the second registration
        # and cannot be modelled by one shared token.
        self.live_endpoints: dict[str, dict[str, Any]] = {}
        # MEASURED at v0.11.0: `key` comes back IN CLEAR. Set this to model a
        # version that masks it instead, which the caller's comparison
        # contract has to survive without refusing a healthy site.
        self.mask_tool_server_key = False
        # A version WITHOUT `POST /api/v1/configs/tool_servers/verify`. The
        # route 404s; the run must record an unavailable probe rather than
        # blaming the credential.
        self.verify_route_present = True
        self.groups: list[dict[str, str]] = []
        self.functions: dict[str, dict[str, Any]] = {}
        self.static: dict[str, bytes] = {}
        self.requests: list[tuple[str, str]] = []
        #: Method, path AND body. `requests` carries no payload, so a test
        #: asserting a credential reached only the routes that must carry it
        #: has nothing to read; this does.
        self.request_log: list[tuple[str, str, Any]] = []
        # -- fault injection ------------------------------------------------
        self.omit_export_keys: set[str] = set()
        self.empty_export = False
        self.model_list_override: Any = None
        self.duplicate_create_401 = False
        self.omit_permission_keys: set[str] = set()
        self.corrupt_export: dict[str, Any] = {}
        self.fail_paths: set[str] = set()
        #: Model a version whose /configs/models carries no model defaults.
        self.omit_model_defaults = False
        # -- accounts and the provider (issue #1792, `okg chat setup`) -------
        # MEASURED at v0.11.0: `/api/config` carries `onboarding: true` ONLY
        # while there are zero users; the first signup is `admin`; later
        # signups answer 403; a wrong password answers 400; `GET
        # /api/v1/auths/` answers the caller's own row and 401 to a bad
        # bearer. The provider list reads back with keys IN CLEAR.
        self.users: list[dict[str, Any]] = []
        self.passwords: dict[str, str] = {}
        self.sessions: dict[str, str] = {}
        #: Operator-minted credentials (API keys) the instance accepts, by
        #: user id. Set by a test to model `OKG_CHAT_ADMIN_TOKEN`.
        self.api_keys: dict[str, str] = {}
        #: The sign-in providers `/api/config` offers, e.g. {"oidc": "SSO"}.
        #: MEASURED at v0.11.0: a live instance started with OIDC settings
        #: reports them under `oauth.providers`.
        self.oauth_providers: dict[str, str] = {}
        self.provider_config: dict[str, Any] = {
            "ENABLE_OPENAI_API": True,
            "OPENAI_API_BASE_URLS": ["https://api.openai.com/v1"],
            "OPENAI_API_KEYS": [""],
            "OPENAI_API_CONFIGS": {},
        }
        #: What `POST /openai/verify` answers per (url, key): a list of model
        #: ids, or an int HTTP status to answer instead.
        self.provider_endpoints: dict[tuple[str, str], Any] = {}
        #: `GET /api/models` — the served, de-duplicated listing.
        self.served_models: list[str] = []
        #: `POST /api/chat/completions` — the reply, and the graph tool calls
        #: the model makes: `{"tool": <operator>, "arguments": {...},
        #: "result": <text the tool returns>}`. MEASURED at 0.11.0 (#2907):
        #: function calling defaults to `native`, so a NON-streaming request
        #: gets the calls back unexecuted (`finish_reason: tool_calls`, no
        #: `sources`); only a streamed turn of a stored chat (`chat_id` + `id`)
        #: runs them, answers `null`, and stores them on the assistant message
        #: as `function_call` / `function_call_output` output items.
        self.completion_reply: str | None = "The graph holds records."
        self.completion_tool_runs: list[dict[str, Any]] = []
        self.completion_status: int = 200
        #: Chats by id (`POST /api/v1/chats/new`, `GET /api/v1/chats/<id>`).
        self.chats: dict[str, dict[str, Any]] = {}
        #: A stored turn slower than the caller's timeout: the completion
        #: request raises `TimeoutError` while the site keeps running the turn,
        #: whose assistant message reads `done: false` until the chat has been
        #: read this many times (`None`: it never finishes).
        self.completion_times_out = False
        self.turn_finishes_after_reads: int | None = 1
        self._pending_turn: dict[str, Any] | None = None
        #: Extra top-level keys the PROVIDER put on its response; the vendor
        #: passes them through unchanged (measured on 0.11.0, 2026-09-23).
        self.completion_extra: dict[str, Any] = {}
        #: When true, `POST /openai/verify` reflects the submitted key in its
        #: error body -- the scrubbing test arm.
        self.verify_echoes_key = False
        #: Environment-owned `features.*` served by `GET /api/config` to a
        #: signed-in caller (v0.11.4 main.py:2347-2350). Both OFF, as a
        #: container started by `okg chat-instance up` has them
        #: (instance.CONTAINER_FIXED_ENV); set True to model a container
        #: started some other way, where the vendor default is on.
        self.env_features: dict[str, bool] = {
            "enable_version_update_check": False,
            "enable_easter_eggs": False,
        }
        #: Each user's OWN stored `settings.ui`, by user id. The key the
        #: fake uses when a call carries no known user is "".
        self.user_ui_settings: dict[str, dict[str, Any]] = {}
        self._bearer: str | None = None

    # -- helpers -----------------------------------------------------------

    def install_function(
        self, function_id: str, *, valve_fields: tuple[str, ...] = (),
    ) -> None:
        self.functions[function_id] = {
            "id": function_id,
            "is_active": True,
            "valves": {},
            "spec": {"properties": {name: {"type": "string"} for name in valve_fields}},
        }

    @staticmethod
    def _tool_name(payload: Any, run: dict[str, Any]) -> str:
        """``<tool server id>_<tool>`` (vendor ``utils/middleware.py:2770``)."""
        if run.get("name"):  # a vendor built-in tool, named as-is
            return str(run["name"])
        tool_ids = [str(t) for t in (payload.get("tool_ids") or [])]
        server = next((t for t in tool_ids if t.startswith("server:mcp:")), "")
        return f"{server.removeprefix('server:mcp:')}_{run['tool']}"

    def _stored_turn(self, payload: Any) -> tuple[int, Any]:
        """A completion that names a stored chat and an assistant message id.

        With no ``session_id`` the vendor runs the turn inline and answers
        ``null`` (``main.py:1783``); a provider error is stored on the message
        rather than answered (``main.py:1586``). Tools run only when the turn
        streams (``utils/middleware.py:3783``); the stored record is the
        native loop's ``output`` items.
        """
        row = self.chats.get(str(payload.get("chat_id")))
        if row is None:
            return 400, {"detail": "chat not found"}
        assistant_id = str(payload["id"])
        message: dict[str, Any] = {
            "id": assistant_id, "role": "assistant", "content": "", "done": True,
            "model": payload.get("model"), "parentId": None, "childrenIds": [],
        }
        if self.completion_status != 200:
            message["error"] = {"content": "upstream error"}
        else:
            output: list[dict[str, Any]] = []
            attached = any(
                str(t).startswith("server:mcp:") for t in payload.get("tool_ids") or []
            )
            if payload.get("stream") is True and attached:
                for index, run in enumerate(self.completion_tool_runs):
                    call_id = f"call_{index}"
                    output.append({
                        "type": "function_call", "id": call_id, "call_id": call_id,
                        "name": self._tool_name(payload, run),
                        "arguments": json.dumps(run.get("arguments") or {}),
                        "status": "completed",
                    })
                    output.append({
                        "type": "function_call_output", "id": f"fco_{index}",
                        "call_id": call_id,
                        "output": [{"type": "input_text", "text": str(run.get("result") or "")}],
                        "status": "completed",
                    })
            if self.completion_reply:
                output.append({
                    "type": "message", "id": "msg_1", "status": "completed",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": self.completion_reply}],
                })
            message["output"] = output
            message["content"] = self.completion_reply or ""
        row["chat"]["history"]["messages"][assistant_id] = message
        return 200, None

    def add_group(self, name: str, group_id: str) -> None:
        self.groups.append({"id": group_id, "name": name})

    def _create_user(self, payload: Any, *, role: str) -> dict[str, Any]:
        user_id = f"u-{len(self.users) + 1}"
        user = {
            "id": user_id,
            "name": str(payload.get("name")),
            "email": str(payload.get("email")),
            "role": role,
        }
        self.users.append(user)
        self.passwords[user_id] = str(payload.get("password"))
        token = f"jwt-{len(self.sessions) + 1}-{user_id}"
        self.sessions[token] = user_id
        return {**user, "token": token, "token_type": "Bearer"}

    def add_admin(self, email: str, password: str, *, api_key: str | None = None) -> None:
        """An existing admin account, for the sign-in and env-token arms."""
        row = self._create_user({"name": "existing", "email": email, "password": password},
                                role="admin")
        if api_key is not None:
            self.api_keys[row["id"]] = api_key

    def _bearer_user(self) -> dict[str, Any] | None:
        token = self._bearer
        if not token:
            return None
        user_id = self.sessions.get(token)
        if user_id is None:
            user_id = next((uid for uid, key in self.api_keys.items() if key == token), None)
        if user_id is None:
            return None
        return next((u for u in self.users if u["id"] == user_id), None)

    def public_name(self) -> str:
        if self.webui_name == VENDOR_DEFAULT_NAME:
            return self.webui_name
        return self.webui_name + SITE_NAME_SUFFIX

    def export(self) -> dict[str, Any]:
        out = {
            key: value
            for key, value in self.config.items()
            if key not in self.omit_export_keys
        }
        out.update(self.corrupt_export)
        return out

    def model_row(self, model_id: str) -> dict[str, Any]:
        row = dict(self.models[model_id])
        row["access_grants"] = [dict(g) for g in self.grants.get(model_id, [])]
        return row

    def _set_grants(self, model_id: str, posted: Any) -> None:
        # `set_access_grants` deletes every existing grant and re-inserts the
        # posted set — including when the posted set is empty. `None` skips
        # the replacement entirely, which is why the projection always sends a
        # list.
        if posted is None:
            return
        self.grants[model_id] = [
            {
                "id": f"grant-{model_id}-{index}",
                "principal_type": str(g.get("principal_type")),
                "principal_id": str(g.get("principal_id")),
                "permission": str(g.get("permission")),
                "created_at": 0,
            }
            for index, g in enumerate(posted)
            if isinstance(g, dict)
        ]

    def _verify_tool_server(self, payload: Any) -> tuple[int, Any]:
        """The vendor's verify route, with the MEASURED credential contract.

        Vendor ``routers/configs.py:546`` derives the Authorization header
        from ``auth_type`` + ``key`` and then merges ``headers`` on top;
        ``utils/tools.py:144`` does the same at real tool-call time, so what
        this route accepts is what a chat turn can use.

        Measured arms (#1738, v0.11.0, against an authenticating endpoint):
        ``none`` → 400 (server 401), ``bearer`` + matching key → 200 with
        seven specs, explicit ``Authorization`` header → 200, ``session``
        → 400 (it forwards Open WebUI's own JWT, which a static verifier
        rejects), ``system_oauth`` from a bearer-authenticated admin → 400
        (the vendor takes the token from the admin's `oauth_session_id`
        cookie, which a command-line run never has, and dials with nothing).
        The 400 body is the vendor's literal message.
        """
        connection = payload if isinstance(payload, dict) else {}
        endpoint = self.live_endpoints.get(str(connection.get("url") or ""))
        auth_type = connection.get("auth_type")
        presented: str | None = None
        if auth_type == "bearer":
            presented = str(connection.get("key") or "") or None
        headers = connection.get("headers")
        if isinstance(headers, dict):
            for name, value in headers.items():
                if str(name).lower() == "authorization":
                    presented = str(value or "").removeprefix("Bearer ") or None
        expected_token = self.mcp_auth_token
        specs = list(self.mcp_specs)
        if endpoint is not None:
            expected_token = endpoint.get("token")
            specs = list(endpoint.get("specs") or ())
        if expected_token is not None and presented != expected_token:
            return 400, {"detail": "Failed to create MCP client"}
        return 200, {"status": True, "specs": specs}

    def add_live_endpoint(
        self, url: str, *, token: str | None, specs: Sequence[str],
    ) -> None:
        """Register a second MCP server the instance can dial."""
        self.live_endpoints[url] = {"token": token, "specs": list(specs)}

    def _listed_tool_servers(self) -> list[dict[str, Any]]:
        """What `GET /api/v1/configs/tool_servers` renders.

        Identity at v0.11.0. Under `mask_tool_server_key` the stored value is
        replaced by the vendor-shaped mask, so the caller's own comparison
        contract is exercised against a masking version rather than assumed
        to work on one.
        """
        if not self.mask_tool_server_key:
            return self.tool_servers
        listed: list[dict[str, Any]] = []
        for entry in self.tool_servers:
            shown = dict(entry)
            if shown.get("key"):
                shown["key"] = "***"
            listed.append(shown)
        return listed

    # -- the opener --------------------------------------------------------

    def opener(self, request: urllib.request.Request, timeout: float) -> tuple[int, str]:
        method = request.get_method()
        parsed = urllib.parse.urlparse(request.full_url)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)
        self.requests.append((method, path))
        auth = request.get_header("Authorization") or ""
        self._bearer = auth[len("Bearer "):] if auth.startswith("Bearer ") else None
        if path in self.fail_paths:
            raise urllib.error.HTTPError(
                request.full_url, 422, "Unprocessable Entity", {},  # type: ignore[arg-type]
                None,
            )
        payload = json.loads(request.data.decode("utf-8")) if request.data else None
        self.request_log.append((method, path, payload))
        # Static assets are served as RAW BYTES, not JSON. Encoding them
        # through json.dumps would quote them, and the projection's sha256
        # comparison could then never match — the file-mechanism branch would
        # be untestable and would look broken for the wrong reason.
        if path.startswith("/static/"):
            asset = self.static.get(path)
            if asset is None:
                return 404, ""
            return 200, asset.decode("utf-8")
        status, body = self._route(method, path, query, payload)
        return status, json.dumps(body) if body is not None else ""

    def _route(
        self, method: str, path: str, query: dict[str, list[str]], payload: Any,
    ) -> tuple[int, Any]:
        if path == "/api/version":
            return 200, {"version": self.version, "deployment_id": ""}
        if path == "/api/config":
            return 200, {
                "status": True, "name": self.public_name(), "version": self.version,
                "oauth": {"providers": dict(self.oauth_providers)},
                **({"onboarding": True} if not self.users else {}),
                # The authenticated half of `features` (main.py:2343-2381).
                **({"features": dict(self.env_features)} if self._bearer else {}),
            }
        if path == "/api/v1/configs/suggestions" and method == "POST":
            # routers/configs.py:817-840: one route, two keys.
            self.config["ui.prompt_suggestions"] = payload.get("suggestions")
            self.config["ui.prompt_suggestions_i18n"] = payload.get("i18n") or {}
            return 200, {
                "suggestions": self.config["ui.prompt_suggestions"],
                "i18n": self.config["ui.prompt_suggestions_i18n"],
            }
        if path == "/api/v1/users/user/settings" and method == "GET":
            # routers/users.py:457-475: the caller's own ui settings merged
            # OVER the site's interface defaults (shallow is enough here).
            user = self._bearer_user()
            own = dict(self.user_ui_settings.get(user["id"] if user else "", {}))
            defaults = self.config.get("ui.default_interface_settings")
            merged = {**(defaults if isinstance(defaults, dict) else {}), **own}
            return 200, {"ui": merged}
        if path == "/api/v1/users/user/settings/update" and method == "POST":
            # A field-level patch of settings.ui (models/users.py:832-842).
            user = self._bearer_user()
            own = self.user_ui_settings.setdefault(user["id"] if user else "", {})
            for key, value in ((payload or {}).get("ui") or {}).items():
                if value is None:
                    own.pop(key, None)
                else:
                    own[key] = value
            return 200, {"ui": dict(own)}
        if path == "/api/v1/auths/signup" and method == "POST":
            if self.users:
                return 403, {"detail": "You do not have permission to access this resource."}
            return 200, self._create_user(payload, role="admin")
        if path == "/api/v1/auths/signin" and method == "POST":
            for user in self.users:
                if (
                    user["email"] == payload.get("email")
                    and self.passwords.get(user["id"]) == payload.get("password")
                ):
                    token = f"jwt-{len(self.sessions) + 1}-{user['id']}"
                    self.sessions[token] = user["id"]
                    return 200, {**user, "token": token, "token_type": "Bearer"}
            return 400, {"detail": "The email or password provided is incorrect."}
        if path == "/api/v1/auths/" and method == "GET":
            user = self._bearer_user()
            if user is None:
                return 401, {"detail": "Not authenticated"}
            return 200, dict(user)
        if path == "/openai/config" and method == "GET":
            return 200, json.loads(json.dumps(self.provider_config))
        if path == "/openai/config/update" and method == "POST":
            self.provider_config = json.loads(json.dumps(payload))
            return 200, json.loads(json.dumps(self.provider_config))
        if path == "/openai/verify" and method == "POST":
            answer = self.provider_endpoints.get(
                (str(payload.get("url")), str(payload.get("key")))
            )
            if answer is None:
                return 500, {"detail": "Open WebUI: Server Connection Error"}
            if isinstance(answer, int):
                # A body that ECHOES the submitted key, like the vendor's
                # OpenAI passthrough error does, so the scrubbing is tested
                # against a real reflection rather than a fixed string.
                if self.verify_echoes_key:
                    return answer, {"error": {"message": (
                        f"Incorrect API key provided: {payload.get('key')}"
                    )}}
                return answer, {"error": {"message": "Incorrect API key provided"}}
            return 200, {"object": "list", "data": [{"id": m} for m in answer]}
        if path == "/api/models" and method == "GET":
            return 200, {"data": [{"id": m} for m in self.served_models]}
        if path == "/api/chat/completions" and method == "POST":
            if payload.get("chat_id") and payload.get("id") and self.completion_times_out:
                row = self.chats[str(payload["chat_id"])]
                row["chat"]["history"]["messages"][str(payload["id"])] = {
                    "id": payload["id"], "role": "assistant", "content": "",
                    "done": False, "output": [],
                }
                self._pending_turn = {"payload": payload,
                                      "reads_left": self.turn_finishes_after_reads}
                raise TimeoutError("timed out")
            if payload.get("chat_id") and payload.get("id"):
                return self._stored_turn(payload)
            if self.completion_status != 200:
                return self.completion_status, {"detail": "upstream error"}
            message: dict[str, Any] = {"role": "assistant", "content": self.completion_reply}
            if self.completion_tool_runs:
                message["tool_calls"] = [
                    {
                        "id": f"call_{index}", "index": index, "type": "function",
                        "function": {
                            "name": self._tool_name(payload, run),
                            "arguments": json.dumps(run.get("arguments") or {}),
                        },
                    }
                    for index, run in enumerate(self.completion_tool_runs)
                ]
            body: dict[str, Any] = {
                "id": "c-1", "model": payload.get("model"), "object": "chat.completion",
                "choices": [{
                    "index": 0,
                    "finish_reason": "tool_calls" if self.completion_tool_runs else "stop",
                    "message": message,
                }],
            }
            body.update(json.loads(json.dumps(self.completion_extra)))
            return 200, body
        if path == "/api/v1/chats/new" and method == "POST":
            chat_id = f"chat-{len(self.chats) + 1}"
            chat = dict((payload or {}).get("chat") or {})
            chat.setdefault("history", {"messages": {}, "currentId": None})
            self.chats[chat_id] = {"id": chat_id, "title": chat.get("title", "New Chat"),
                                   "chat": chat}
            return 200, json.loads(json.dumps(self.chats[chat_id]))
        if path.startswith("/api/v1/chats/") and method == "GET":
            pending = self._pending_turn
            if pending is not None and pending["reads_left"] is not None:
                pending["reads_left"] -= 1
                if pending["reads_left"] <= 0:
                    self._pending_turn = None
                    self._stored_turn(pending["payload"])
            row = self.chats.get(urllib.parse.unquote(path.rsplit("/", 1)[-1]))
            if row is None:
                return 401, {"detail": "We could not find what you're looking for :/"}
            return 200, json.loads(json.dumps(row))
        if path == "/api/v1/configs/export":
            return 200, self.export()
        if path == "/api/v1/configs/import":
            self.config.update(payload.get("config") or {})
            return 200, {"status": True}
        if path == "/api/v1/users/default/permissions":
            if method == "POST":
                self.permissions = json.loads(json.dumps(payload))
                return 200, self.permissions
            visible = json.loads(json.dumps(self.permissions))
            for key in self.omit_permission_keys:
                section, _, name = key.partition(".")
                (visible.get(section) or {}).pop(name, None)
            return 200, visible
        if path == "/api/v1/configs/connections":
            if method == "POST":
                self.connections = json.loads(json.dumps(payload))
            return 200, self.connections
        if path == "/api/v1/configs/banners":
            if method == "POST":
                self.config["ui.banners"] = payload.get("banners") or []
            return 200, self.config["ui.banners"]
        if path == "/api/v1/configs/models":
            if method == "POST":
                self.config["ui.default_models"] = payload.get("DEFAULT_MODELS", "")
                self.config["ui.model_order_list"] = payload.get("MODEL_ORDER_LIST", [])
            return 200, {
                "DEFAULT_MODELS": self.config["ui.default_models"],
                "MODEL_ORDER_LIST": self.config["ui.model_order_list"],
                "DEFAULT_PINNED_MODELS": [],
                # `models.default_metadata`: null until an admin sets model
                # defaults (routers/configs.py:66 at v0.11.4).
                **(
                    {}
                    if self.omit_model_defaults
                    else {"DEFAULT_MODEL_METADATA": self.config.get("models.default_metadata")}
                ),
            }
        if path == "/api/v1/configs/tool_servers/verify":
            if not self.verify_route_present:
                return 404, {"detail": "Not Found"}
            return self._verify_tool_server(payload)
        if path == "/api/v1/configs/tool_servers":
            if method == "POST":
                self.tool_servers = list(payload.get("TOOL_SERVER_CONNECTIONS") or [])
            # MEASURED (#1738 spike, v0.11.0): `key` and any custom `headers`
            # come back IN CLEAR — no masking, no `***`, no omission. Echoing
            # what was stored is therefore the faithful behavior, and it is
            # what makes the caller's redaction testable instead of assumed.
            # `mask_tool_server_key` models the OTHER version, on demand.
            return 200, {"TOOL_SERVER_CONNECTIONS": self._listed_tool_servers()}
        # MEASURED at v0.11.0 (live okg-workspace bring-up, 2026-08-18): both
        # listing surfaces filter `Model.base_model_id != None` (vendor
        # models/models.py:346 and :249), so null-base rows — the exact shape
        # the models surface writes — are invisible to them. Only the per-id
        # route reads the table unfiltered. This fake mirrors that, or every
        # fixture proof of the models surface is a proof about a vendor that
        # does not exist.
        if path == "/api/v1/models/export":
            if self.empty_export:
                return 200, []
            return 200, [
                self.model_row(model_id)
                for model_id in sorted(self.models)
                if self.models[model_id].get("base_model_id") is not None
            ]
        if path == "/api/v1/models/list":
            if self.model_list_override is not None:
                return 200, self.model_list_override
            page = int((query.get("page") or ["1"])[0])
            ordered = [
                model_id
                for model_id in sorted(self.models)
                if self.models[model_id].get("base_model_id") is not None
            ]
            start = max(0, page - 1) * 30
            items = [self.model_row(model_id) for model_id in ordered[start:start + 30]]
            return 200, {"items": items, "total": len(ordered)}
        if path == "/api/v1/models/model" and method == "GET":
            model_id = (query.get("id") or [None])[0]
            row = self.model_row(model_id) if model_id in self.models else None
            return 200, row
        if path == "/api/v1/models/create":
            model_id = payload["id"]
            if self.duplicate_create_401 and model_id in self.models:
                return 401, {
                    "detail": (
                        "Uh-oh! This model id is already registered. "
                        "Please choose another model id string"
                    ),
                }
            self.models[model_id] = {
                key: value for key, value in payload.items() if key != "access_grants"
            }
            # Server-owned fields the caller must NOT send and the server
            # always adds. The projection compares a derived subset precisely
            # because these can never round-trip.
            self.models[model_id].update({
                "user_id": "fake-admin", "created_at": 1, "updated_at": 1,
            })
            self._set_grants(model_id, payload.get("access_grants"))
            return 200, self.model_row(model_id)
        if path == "/api/v1/models/model/update":
            # The BODY's id, as the vendor route does (`routers/models.py:701`
            # reads form_data.id and declares no query parameter). Reading the
            # query string here would let a projection that dropped the body
            # id keep passing against this fake and fail against the real
            # instance.
            model_id = payload.get("id")
            if model_id not in self.models:
                return 401, {"detail": "Not found"}
            self.models[model_id].update({
                key: value for key, value in payload.items() if key != "access_grants"
            })
            self.models[model_id]["updated_at"] = 2
            self._set_grants(model_id, payload.get("access_grants"))
            return 200, self.model_row(model_id)
        if path == "/api/v1/models/sync":
            # MEASURED: the update arm raises, the exception is swallowed, and
            # the router returns HTTP 200 with [] while the row is unchanged.
            # Modelled so a projection that reached for it would fail its own
            # readback rather than pass a test it should not.
            return 200, []
        if path == "/api/v1/groups/":
            return 200, self.groups
        if path == "/api/v1/prompts/":
            return 200, list(self.prompts.values())
        if path == "/api/v1/prompts/create":
            prompt_id = f"prompt-{len(self.prompts)}"
            self.prompts[prompt_id] = {"id": prompt_id, **payload}
            return 200, self.prompts[prompt_id]
        if path.startswith("/api/v1/prompts/id/") and path.endswith("/update"):
            prompt_id = path[len("/api/v1/prompts/id/"):-len("/update")]
            self.prompts[prompt_id].update(payload)
            return 200, self.prompts[prompt_id]
        if path == "/api/v1/functions/":
            return 200, [
                {"id": f["id"], "is_active": f["is_active"]}
                for f in self.functions.values()
            ]
        if path.startswith("/api/v1/functions/id/"):
            rest = path[len("/api/v1/functions/id/"):]
            function_id, _, tail = rest.partition("/")
            function = self.functions.get(function_id)
            if function is None:
                return 404, None
            if tail == "valves/spec":
                return 200, function["spec"]
            if tail == "valves":
                return 200, function["valves"]
            if tail == "valves/update":
                declared = set(function["spec"]["properties"])
                # SILENT DROP of unknown fields, exactly as
                # `Valves(**form_data)` does.
                function["valves"].update({
                    key: value for key, value in payload.items() if key in declared
                })
                return 200, function["valves"]
        return 404, {"detail": f"no route for {method} {path}"}


class FakeContainerRuntime:
    """A :class:`okg.substrate.chat.instance.ContainerRuntime` over a fake.

    The instance's filesystem, not a second HTTP surface: the census
    re-verified that the static directory has no inbound write route at all,
    so the only way bytes reach it is a command run INSIDE the container.
    This models that command surface and the one measured consequence of
    getting the directory wrong.

    Three modelling choices, each closing a way a test could pass vacuously:

    * an argv this fake does not recognize answers **127**, not 0. A runtime
      double that exits zero for anything makes every ``test -d`` precondition
      and every removal proof pass without measuring a thing;
    * a write under :data:`STATIC_DIR` becomes served at ``/static/<name>``;
      a write under :data:`BUILD_STATIC_DIR` lands on the filesystem and is
      served by nothing, exactly as measured;
    * ``printenv`` follows the real exit convention — 0 when the variable
      exists, 1 when it does not — so the caller's "I could not find out"
      branch is reachable rather than hypothetical.
    """

    binary = "fake-container-runtime"

    def __init__(
        self,
        instance: FakeInstance,
        *,
        container: str = "okg-chat-fixture",
        env: dict[str, str] | None = None,
        present: bool = True,
    ) -> None:
        self.instance = instance
        self.container = container
        self.env: dict[str, str] = dict(env or {})
        self.present = present
        self.files: dict[str, bytes] = {}
        self.dirs: set[str] = {STATIC_DIR, BUILD_STATIC_DIR, "/app/backend/data"}
        self.calls: list[tuple[str, ...]] = []
        #: Set to refuse every write, modelling a read-only filesystem.
        self.write_fails = False
        #: Set to make `rm` report success while leaving the file in place.
        self.removal_lies = False

    # -- ContainerRuntime --------------------------------------------------

    def inspect(self, container: str) -> dict[str, Any] | None:
        if not self.present or container != self.container:
            return None
        return {
            "id": container,
            "labels": {},
            "image": "ghcr.io/open-webui/open-webui:v0.11.0",
            "running": True,
            "exit_code": None,
        }

    def run(self, spec: Any) -> str:  # pragma: no cover - not exercised here
        raise AssertionError("this double does not launch containers")

    def start(self, container: str) -> None:  # pragma: no cover
        raise AssertionError("this double does not start containers")

    def stop(self, container: str) -> None:  # pragma: no cover
        raise AssertionError("this double does not stop containers")

    def remove(self, container: str) -> None:  # pragma: no cover
        raise AssertionError("this double does not remove containers")

    def execute(
        self, container: str, argv: Sequence[str],
    ) -> tuple[int, str, str]:
        self.calls.append(tuple(argv))
        if self.inspect(container) is None:
            return 1, "", f"Error: No such container: {container}"
        argv = list(argv)
        if argv[:1] == ["printenv"] and len(argv) == 2:
            name = argv[1]
            if name in self.env:
                return 0, self.env[name] + "\n", ""
            return 1, "", ""
        if argv[:1] == ["test"] and len(argv) == 3:
            flag, path = argv[1], argv[2]
            if flag == "-d":
                return (0 if path in self.dirs else 1), "", ""
            if flag in {"-e", "-f"}:
                return (0 if path in self.files else 1), "", ""
            return 127, "", f"test: unsupported flag {flag}"
        if argv[:2] == ["rm", "-f"] and len(argv) == 3:
            if self.write_fails:
                return 1, "", "rm: read-only file system"
            if not self.removal_lies:
                self._unlink(argv[2])
            return 0, "", ""
        if argv[:2] == ["sh", "-c"] and len(argv) == 6:
            # The delivery: `printf %s "$1" | base64 -d > "$2"` with the
            # payload and destination as POSITIONAL arguments. Asserted in
            # shape rather than interpreted, because the point of the argv
            # form is that nothing in the payload is ever parsed as script.
            script, _argv0, encoded, dest = argv[2], argv[3], argv[4], argv[5]
            if "base64 -d" not in script:
                return 127, "", f"sh: unsupported script {script!r}"
            if self.write_fails:
                return 1, "", "sh: read-only file system"
            try:
                payload = base64.b64decode(encoded, validate=True)
            except Exception as exc:  # noqa: BLE001 - modelled as a shell failure
                return 1, "", f"base64: invalid input ({exc})"
            self._write(dest, payload)
            return 0, "", ""
        # UNRECOGNIZED. Never zero: a permissive default would let every
        # precondition and every removal proof in the caller pass vacuously.
        return 127, "", f"sh: command not found: {' '.join(argv)}"

    # -- filesystem model --------------------------------------------------

    def _write(self, path: str, payload: bytes) -> None:
        parent, _, name = path.rpartition("/")
        if parent not in self.dirs:
            # A shell redirect into a directory that does not exist fails.
            raise AssertionError(
                f"the delivery wrote to {path}, whose directory {parent!r} "
                "does not exist on this instance"
            )
        self.files[path] = payload
        if parent == STATIC_DIR:
            self.instance.static[f"/static/{name}"] = payload

    def _unlink(self, path: str) -> None:
        parent, _, name = path.rpartition("/")
        self.files.pop(path, None)
        if parent == STATIC_DIR:
            self.instance.static.pop(f"/static/{name}", None)

    def tamper(self, path: str, payload: bytes) -> None:
        """Edit a delivered file by hand, the way an operator would."""
        self._write(path, payload)
