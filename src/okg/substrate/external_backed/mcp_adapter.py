"""The built-in live-relation adapter for adapter kind `mcp`.

PACT live-relation-mcp-adapter. One binding names ONE tool on ONE
streamable-HTTP MCP server. Each call opens a session, initializes, calls
that tool with the operator's `arguments` (the caller's validated values
substituted into typed `{"$arg": name}` slots), reads a bounded reply and
closes the session, all inside the relation's deadline. The rows come from
the tool result's `structuredContent`, or else from its text blocks: one
JSON block read at `rows_path`, or one JSON object per block (decision
`dbsrows A`; see `_rows_from_text_blocks`). Under a named `rows_path`, a
list is the rows and one object is one row (see `_object_rows` for the
shapes still refused).

It is okg code, registered beside `http` in `BUILTIN_LIVE_ADAPTERS`, and it
is NOT a way to call any tool: a deployment binds a relation to a fixed tool
and can never name code (see `live_dispatch.BUILTIN_LIVE_ADAPTERS`).

The halves mirror `http_adapter`:

* :func:`validate_mcp_binding_configs` runs at server BUILD and makes every
  refusal this module can make about a binding, naming relation and field.
* the runner closure runs per call on the dispatcher's worker thread.

**The byte cap is enforced on the wire, not after parsing.** The MCP SDK is
handed an `httpx.AsyncClient` whose transport wraps every response stream in
a counter. The counter sees each chunk before the SDK does and raises once
the session has received more than `max_response_bytes`, so the SDK never
holds more than the cap plus one network chunk (64 KiB) of the reply, and a
60 MB result is refused without ever being assembled, let alone parsed. The
counter covers the WHOLE session (the `initialize` reply is a few hundred
bytes of it). The same transport refuses every redirect -- the SDK itself
follows a same-origin 307/308, which the egress rules do not allow -- and
any compressed response, because a compressed byte count says nothing about
the decoded size.

**Abandonment.** The adapter holds its own work to a budget slightly
shorter than the relation's deadline, so the session's closing request still
fits inside it, and a hard backstop at the full deadline cancels even that.
In the rare case the dispatcher stops waiting first (its clock started a few
milliseconds earlier), the worker thread finishes the close within that same
backstop and its result is discarded by the dispatcher; it is never sampled
or returned.
"""
from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit

from okg.substrate.errors import SubstrateError
from okg.substrate.external_backed._http_guard import (
    DEFAULT_PORTS,
    HostPort,
    HttpGuardRefusal,
    assert_host_allowed,
    parse_json_strict,
    redact_url,
    redact_userinfo,
    split_base_url,
)
from okg.substrate.external_backed.live_relations import UPSTREAM_CREDENTIALS
from okg.substrate.library.sources._url_safety import (
    UnsafeFetchURL,
    assert_public_http_url,
)
from okg.substrate.external_backed.http_adapter import (
    DEFAULT_MAX_RESPONSE_BYTES,
    MAX_MAX_RESPONSE_BYTES,
    MAX_MAX_ROWS,
    MIN_MAX_RESPONSE_BYTES,
    ROWS_PATH_ROOT,
    SLOT_KEY,
    LiveAdapterArgUnconstrained,
    LiveAdapterConfigInvalid,
    LiveAdapterHostRefused,
    _FORBIDDEN_HEADERS,
    _HEADER_NAME,
    _constraint_satisfied,
    _constraint_wanted,
    _parse_allowed_hosts,
    _require_bool,
    _require_int,
    _resolve_secret,
    _substitute,
    _walk_slots,
)

log = logging.getLogger("okg.substrate.external_backed.mcp_adapter")

__all__ = [
    "MCP_CONFIG_FIELDS",
    "PERSONAL_CREDENTIAL_LIFECYCLES",
    "UPSTREAM_CREDENTIALS",
    "LiveAdapterPersonalCredentialRefused",
    "McpBindingConfig",
    "assert_mcp_credentials_admitted",
    "parse_mcp_binding",
    "run_mcp_relation_for",
    "validate_mcp_binding_configs",
]


# ----------------------------------------------------------------- errors

class LiveAdapterPersonalCredentialRefused(SubstrateError):
    """A `personal` upstream credential on a deployment that may not use one.

    okg cannot inspect the credential an MCP server holds in its own
    container. The binding DECLARES whose it is, and this refusal enforces
    the declaration: a personal credential (one person's grid proxy, say) is
    admitted only on a `prototype` or `demo` deployment, so it cannot serve a
    production or staging deployment even by mistake.
    """

    code = "live_adapter_personal_credential_refused"


# ------------------------------------------------------------ the grammar

#: Every key `config` may carry. An unknown key is REFUSED, not ignored.
MCP_CONFIG_FIELDS = frozenset({
    "allow_non_global",
    "allowed_hosts",
    "arguments",
    "auth_header",
    "max_response_bytes",
    "max_rows",
    "row_fields",
    "rows_path",
    "server_url",
    "tool",
})

#: The only `contract.lifecycle` values under which a `personal` upstream
#: credential is admitted.
PERSONAL_CREDENTIAL_LIFECYCLES = frozenset({"prototype", "demo"})

#: Headers the MCP SDK sets itself; a binding may not name one as its
#: `auth_header`, on top of the transport headers `http` already forbids.
_SDK_HEADERS = frozenset({
    "accept", "accept-encoding", "mcp-session-id", "mcp-protocol-version",
    "last-event-id",
})

#: A tool name as MCP servers spell them. No slot can reach it.
_TOOL_NAME = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.\-/]{0,127}$")

#: Path tokens. Wider than `http`'s lower-snake grammar because a tool's
#: structured result is whatever its server's language spells (`rseName`).
_TOKEN = r"[A-Za-z_][A-Za-z0-9_]*"
_ROWS_PATH = re.compile(rf"^{_TOKEN}(\.{_TOKEN})*$")
_ROW_FIELD = re.compile(rf"^{_TOKEN}$")
_URL_PATH = re.compile(r"^/[A-Za-z0-9._~/-]*$")

#: Longest upstream text an error carries before the dispatcher fences it.
ERROR_EXCERPT_CHARS = 256

#: The share of the deadline kept back so the session's closing request
#: fits inside it, bounded on both sides.
_CLOSE_MARGIN_FRACTION = 0.2
_CLOSE_MARGIN_MIN_SECONDS = 0.1
_CLOSE_MARGIN_MAX_SECONDS = 1.0

#: Text by which a tool that reports "nothing matched" AS AN ERROR says so.
#: Rucio's `list_rses` answers an expression that selects no RSE with
#: `isError` and "RSE Expression resulted in an empty set": the server is
#: up and answered, so `upstream_unavailable` would send the caller looking
#: for an outage. Matched only inside an `isError` result, case-folded.
_EMPTY_MATCH_MARKERS = ("empty set",)

#: Shortest credential fragment the reply scan looks for. Shorter strings
#: occur by chance; `deployment_tools` uses the same floor.
_MIN_SECRET_SCAN_CHARS = 8

#: Named by title, not by repository path: product code must not point at
#: files outside the package (tests/ci/test_lane_scope_db_closure.py).
_DOCS = "the live relations guide (Operate > Live relations)"


def _refuse(relation: str, field: str, reason: str) -> None:
    raise LiveAdapterConfigInvalid(
        f"live binding {relation!r}: `live_bindings.{relation}.config."
        f"{field}` {reason}",
        data={"relation": relation, "field": field, "reason": reason},
        fix_hint=(
            f"correct `live_bindings.{relation}.config.{field}` in the "
            f"deployment manifest; see {_DOCS}, section on `kind: mcp`"
        ),
    )


# ------------------------------------------------------------- the config

@dataclass(frozen=True)
class McpBindingConfig:
    """One parsed, checked `kind: mcp` binding. **Carries no secret.**"""

    relation: str
    scheme: str
    address: Any
    port: int
    path: str
    allowed: tuple[HostPort, ...]
    allow_non_global: bool
    tool: str
    arguments: Mapping[str, Any]
    rows_path: tuple[str, ...]
    max_rows: int | None
    max_response_bytes: int
    auth_header: str | None
    row_fields: tuple[str, ...] | None
    #: The relation's declared argument names: what a caller narrows with.
    narrowing_args: tuple[str, ...] = ()

    @property
    def url(self) -> str:
        host = str(self.address)
        shown = f"[{host}]" if ":" in host else host
        return f"{self.scheme}://{shown}:{self.port}{self.path}"


def _contains_slot(node: Any) -> bool:
    """True when `$arg` appears anywhere in `node`, as key, value or text."""
    if isinstance(node, Mapping):
        return any(
            (isinstance(k, str) and SLOT_KEY in k) or _contains_slot(v)
            for k, v in node.items()
        )
    if isinstance(node, (list, tuple)):
        return any(_contains_slot(item) for item in node)
    return isinstance(node, str) and SLOT_KEY in node


def _parse_server_url(relation: str, raw: Any) -> tuple[str, Any, int, str]:
    """`(scheme, address, port, path)` for `server_url`, or refuse."""
    if not isinstance(raw, str) or not raw.strip():
        _refuse(relation, "server_url", "is required and must be a string")
    text = raw.strip()
    try:
        parts = urlsplit(text)
    except ValueError:
        parts = None
    if parts is None:
        _refuse(relation, "server_url", "could not be parsed as a URL")
    if parts.query or parts.fragment:
        _refuse(
            relation, "server_url",
            "must not carry a query or fragment; it names the MCP endpoint "
            "and nothing else",
        )
    path = parts.path or "/"
    if not _URL_PATH.fullmatch(path):
        _refuse(
            relation, "server_url",
            "has a path with characters outside `A-Za-z0-9._~/-`",
        )
    base = f"{parts.scheme}://{parts.netloc}"
    try:
        scheme, address, port = split_base_url(base)
    except HttpGuardRefusal as exc:
        raise LiveAdapterHostRefused(
            f"live binding {relation!r}: `live_bindings.{relation}.config."
            f"server_url` is refused ({exc.reason}): {exc.detail}",
            data={"relation": relation, "host": redact_url(text),
                  "reason": exc.reason},
            fix_hint=(
                "`server_url` is `http` or `https`, an IP literal, an "
                "optional port and the endpoint path -- no credentials, "
                "query or fragment"
            ),
        ) from None
    return scheme, address, port, path


def _parse_auth_header(relation: str, binding: Any, config: Mapping) -> str | None:
    auth_header = config.get("auth_header")
    secret_refs = tuple(getattr(binding, "secret_refs", ()) or ())
    if auth_header is not None:
        if not isinstance(auth_header, str) or not _HEADER_NAME.fullmatch(auth_header):
            _refuse(
                relation, "auth_header",
                f"must be a bare header NAME, never a value; got "
                f"{type(auth_header).__name__}",
            )
        lowered = auth_header.lower()
        if lowered in _FORBIDDEN_HEADERS or lowered in _SDK_HEADERS:
            _refuse(
                relation, "auth_header",
                f"may not set {auth_header!r}: the MCP client or the "
                f"transport owns that header",
            )
        if not secret_refs:
            _refuse(
                relation, "auth_header",
                "names a header but the binding declares no `secret_refs`, "
                "so there is no value to send",
            )
    elif secret_refs:
        _refuse(
            relation, "auth_header",
            "is absent while `secret_refs` is present. A resolved secret "
            "with nowhere to go is a credential in the process for no "
            "reason",
        )
    return auth_header


def parse_mcp_binding(relation: str, binding: Any, declared: Any) -> McpBindingConfig:
    """Check one `kind: mcp` binding against its relation, or refuse."""
    config = binding.config
    if not isinstance(config, Mapping):
        _refuse(relation, "", f"must be a mapping; got {type(config).__name__}")
    unknown = sorted(set(config) - MCP_CONFIG_FIELDS)
    if unknown:
        _refuse(
            relation, unknown[0],
            f"is not a recognised field. Unknown key(s) {unknown} are "
            f"REFUSED rather than ignored. Known fields are "
            f"{sorted(MCP_CONFIG_FIELDS)}",
        )
    # A slot is legal in `arguments` and NOWHERE else: not in the URL, not
    # in the tool name, not in the rows path. Checked first, by field, so
    # the refusal names where the slot is rather than some later symptom.
    for field_name in sorted(set(config) - {"arguments"}):
        if _contains_slot(config[field_name]):
            _refuse(
                relation, field_name,
                f"contains a `{SLOT_KEY}` slot. A caller's value may only "
                f"reach `arguments`, never the server, the tool or the "
                f"shape of the result",
            )

    scheme, address, port, path = _parse_server_url(relation, config.get("server_url"))
    if config.get("allowed_hosts") is not None:
        allowed = _parse_allowed_hosts(
            relation, config.get("allowed_hosts"),
            default_port=DEFAULT_PORTS[scheme],
        )
    else:
        # Optional here, unlike `http`: the server URL is one fixed endpoint,
        # so its own address is the allowlist unless the operator narrows it
        # further by naming one.
        allowed = ((address, port),)
    allow_non_global = _require_bool(
        relation, "allow_non_global", config.get("allow_non_global"), False,
    )
    try:
        assert_host_allowed(address, port, allowed, allow_non_global=allow_non_global)
    except HttpGuardRefusal as exc:
        raise LiveAdapterHostRefused(
            f"live binding {relation!r}: `live_bindings.{relation}.config."
            f"server_url` may not be dialled ({exc.reason}): {exc.detail}",
            data={"relation": relation,
                  "host": redact_url(str(config.get("server_url"))),
                  "reason": exc.reason},
            fix_hint=(
                "set `allow_non_global: true` for a loopback, private or "
                "link-local address, and make sure the server's `ip:port` "
                "is in `allowed_hosts` when that is set"
            ),
        ) from None
    # The central egress validator too, not only this adapter's own guard:
    # one rule decides what an okg process may dial. A declared
    # `allow_non_global` goes through its logged operator exception.
    host = str(address)
    netloc = f"[{host}]" if ":" in host else host
    try:
        # The validator's operator exception cannot name an IPv6 host (its
        # matcher strips the last `:` group as a port), so a declared
        # non-global IPv6 address rests on this adapter's own guard above.
        if not (allow_non_global and ":" in host):
            assert_public_http_url(
                f"{scheme}://{netloc}:{port}",
                allow_hosts=(host,) if allow_non_global else (),
            )
    except UnsafeFetchURL as exc:
        raise LiveAdapterHostRefused(
            f"live binding {relation!r}: `live_bindings.{relation}.config."
            f"server_url` may not be dialled (egress_validator): {exc}",
            data={"relation": relation,
                  "host": redact_url(str(config.get("server_url"))),
                  "reason": "egress_validator"},
            fix_hint=(
                "use a public address, or set `allow_non_global: true` for "
                "a loopback, private or link-local address"
            ),
        ) from None

    tool = config.get("tool")
    if not isinstance(tool, str) or not _TOOL_NAME.fullmatch(tool):
        _refuse(
            relation, "tool",
            "is required and must be one tool name (letters, digits, `_`, "
            "`.`, `-`, `/`; at most 128 characters)",
        )

    auth_header = _parse_auth_header(relation, binding, config)

    rows_path = config.get("rows_path")
    if rows_path == ROWS_PATH_ROOT:
        parsed_rows_path: tuple[str, ...] = ()
    elif not isinstance(rows_path, str) or not _ROWS_PATH.fullmatch(rows_path):
        _refuse(
            relation, "rows_path",
            f"is required and must be a dotted path of identifier tokens "
            f"(`result.datasets`), or `{ROWS_PATH_ROOT}` when the tool's "
            f"JSON text is itself the list of rows; got {rows_path!r}",
        )
    else:
        parsed_rows_path = tuple(rows_path.split("."))

    max_rows = None
    if config.get("max_rows") is not None:
        max_rows = _require_int(
            relation, "max_rows", config.get("max_rows"),
            default=None, low=1, high=MAX_MAX_ROWS,
        )
    max_response_bytes = _require_int(
        relation, "max_response_bytes", config.get("max_response_bytes"),
        default=DEFAULT_MAX_RESPONSE_BYTES,
        low=MIN_MAX_RESPONSE_BYTES, high=MAX_MAX_RESPONSE_BYTES,
    )

    row_fields_raw = config.get("row_fields")
    row_fields: tuple[str, ...] | None = None
    if row_fields_raw is not None:
        if (
            not isinstance(row_fields_raw, list)
            or not row_fields_raw
            or not all(
                isinstance(f, str) and _ROW_FIELD.fullmatch(f)
                for f in row_fields_raw
            )
            or len(set(row_fields_raw)) != len(row_fields_raw)
        ):
            _refuse(
                relation, "row_fields",
                f"must be a non-empty list of distinct identifier keys; got "
                f"{row_fields_raw!r}",
            )
        row_fields = tuple(row_fields_raw)

    arguments = config.get("arguments")
    if not isinstance(arguments, Mapping):
        _refuse(
            relation, "arguments",
            "is required and must be a JSON object: the tool's arguments, "
            "with each caller value as a `{\"$arg\": \"name\"}` slot",
        )
    slots = _walk_slots(arguments, relation, "arguments", _refuse)

    declared_args = {arg.name: arg for arg in getattr(declared, "args", ())}
    unknown_slots = sorted(slots - set(declared_args))
    if unknown_slots:
        raise LiveAdapterConfigInvalid(
            f"live binding {relation!r}: `arguments` slots argument(s) "
            f"{unknown_slots}, which the relation does not declare. Declared "
            f"arguments are {sorted(declared_args)}.",
            data={"relation": relation, "field": "arguments",
                  "reason": f"undeclared argument(s) {unknown_slots}"},
            fix_hint=(
                "fix the spelling, or declare the argument on the subtype's "
                "`okg_live_relations` annotation"
            ),
        )
    unslotted = sorted(set(declared_args) - slots)
    if unslotted:
        raise LiveAdapterConfigInvalid(
            f"live binding {relation!r}: the relation declares argument(s) "
            f"{unslotted} that no slot in `arguments` uses. `inspect` would "
            f"advertise a parameter that changes nothing.",
            data={"relation": relation, "field": "arguments",
                  "reason": f"unused declared argument(s) {unslotted}"},
            fix_hint=(
                "slot each declared argument into `arguments`, or remove it "
                "from the declaration"
            ),
        )
    for name in sorted(slots):
        arg = declared_args[name]
        if not _constraint_satisfied(arg):
            raise LiveAdapterArgUnconstrained(
                f"live binding {relation!r}: argument {name!r} of type "
                f"{arg.type_name!r} is slotted into the tool's arguments but "
                f"carries no constraint. This adapter requires "
                f"{_constraint_wanted(arg.type_name)}, exactly as `http` "
                f"does. The rule catches an ABSENT constraint, not a weak "
                f"one; how tight it must be is a fact about the tool.",
                data={"relation": relation, "argument": name,
                      "type": arg.type_name},
                fix_hint=(
                    f"add {_constraint_wanted(arg.type_name)} to argument "
                    f"{name!r} on the subtype's `okg_live_relations` "
                    f"annotation"
                ),
            )

    # A deep copy that also proves the arguments are JSON: YAML turns an
    # unquoted `2026-10-06` into a date, which no tool call can carry. The
    # refusal is raised OUTSIDE the `except`, so it chains to nothing.
    unjsonable: str | None = None
    try:
        frozen_arguments = json.loads(json.dumps(arguments))
    except (TypeError, ValueError) as exc:
        unjsonable = type(exc).__name__
    if unjsonable is not None:
        _refuse(
            relation, "arguments",
            f"holds a value JSON cannot carry ({unjsonable}); quote dates "
            f"and other YAML-typed scalars as strings",
        )
    return McpBindingConfig(
        relation=relation,
        scheme=scheme,
        address=address,
        port=port,
        path=path,
        allowed=allowed,
        allow_non_global=allow_non_global,
        tool=tool,
        arguments=frozen_arguments,
        rows_path=parsed_rows_path,
        max_rows=max_rows,
        max_response_bytes=max_response_bytes,
        auth_header=auth_header,
        row_fields=row_fields,
        narrowing_args=tuple(sorted(declared_args)),
    )


# ------------------------------------------------------------ credentials

def assert_mcp_credentials_admitted(
    bindings: Mapping[str, Any], *, lifecycle: str | None,
) -> None:
    """Refuse a `personal` upstream credential outside prototype and demo.

    Runs at server build for EVERY `kind: mcp` binding, including one whose
    runner a test supplies: it is a rule about whose credential serves the
    deployment, not about the request's shape. `lifecycle` is the
    deployment's `contract.lifecycle`, or None when it declares none --
    which refuses, because "unknown" is not one of the two admitted values.
    """
    for name in sorted(bindings):
        binding = bindings[name]
        if getattr(binding, "kind", None) != "mcp":
            continue
        credential = getattr(binding, "upstream_credential", None)
        if credential not in UPSTREAM_CREDENTIALS:
            # `parse_live_bindings` refuses this at manifest load; reaching
            # here means a binding was built some other way.
            raise LiveAdapterConfigInvalid(
                f"live binding {name!r}: `live_bindings.{name}."
                f"upstream_credential` must be one of "
                f"{list(UPSTREAM_CREDENTIALS)}; got {credential!r}",
                data={"relation": name, "field": "upstream_credential",
                      "reason": "missing or unknown value"},
                fix_hint=(
                    "declare whose credential the MCP server uses: `service` "
                    "or `personal`"
                ),
            )
        if credential == "personal" and lifecycle not in PERSONAL_CREDENTIAL_LIFECYCLES:
            seen = lifecycle if lifecycle is not None else "(none declared)"
            raise LiveAdapterPersonalCredentialRefused(
                f"live binding {name!r} declares `upstream_credential: "
                f"personal`, and this deployment's `contract.lifecycle` is "
                f"{seen}. A personal credential is admitted only under "
                f"{sorted(PERSONAL_CREDENTIAL_LIFECYCLES)}. okg cannot "
                f"inspect the credential the MCP server holds; it enforces "
                f"what the binding declares.",
                data={"relation": name, "lifecycle": lifecycle,
                      "admitted": sorted(PERSONAL_CREDENTIAL_LIFECYCLES)},
                fix_hint=(
                    "serve this relation from an MCP server that holds a "
                    "service credential and declare `upstream_credential: "
                    "service`, or set `contract.lifecycle` to `prototype` or "
                    "`demo` if this deployment really is one"
                ),
            )


# ------------------------------------------------------- log redaction

#: The loggers the MCP client path writes to. The SDK logs every message it
#: receives at DEBUG, so a reply that echoes the credential would reach a
#: log line before the reply scan refuses it.
_CLIENT_LOGGERS = (
    "mcp.client.streamable_http",
    "client",  # `mcp.client.session` logs under this bare name
    "httpx",
    "httpcore.connection",
    "httpcore.connection_pool",
    "httpcore.http11",
)


class _RedactActiveSecrets(logging.Filter):
    """Replace any in-flight call's credential in a client log record.

    Attached to the loggers that CREATE the records (a filter on a parent
    logger does not see a child's records). Credentials are registered for
    the length of one call and counted, so concurrent calls with the same
    secret do not unregister each other's.
    """

    def __init__(self) -> None:
        super().__init__()
        import threading

        self._lock = threading.Lock()
        self._active: dict[str, int] = {}

    def activate(self, fragments: tuple[str, ...]) -> None:
        with self._lock:
            for fragment in fragments:
                self._active[fragment] = self._active.get(fragment, 0) + 1

    def deactivate(self, fragments: tuple[str, ...]) -> None:
        with self._lock:
            for fragment in fragments:
                left = self._active.get(fragment, 0) - 1
                if left > 0:
                    self._active[fragment] = left
                else:
                    self._active.pop(fragment, None)

    def filter(self, record: logging.LogRecord) -> bool:
        if not self._active:
            return True
        with self._lock:
            fragments = tuple(self._active)
        try:
            text = record.getMessage()
        except Exception:  # noqa: BLE001 -- a malformed record is not ours to fix.
            return True
        if any(fragment in text for fragment in fragments):
            record.msg = _mask_token_fragments(text, fragments)
            record.args = None
        # The traceback is text too. The SDK logs `logger.exception(...)` on
        # a reply it cannot parse, and a pydantic error quotes the input --
        # which can be the credential the server echoed. Formatted here, once,
        # and the exception object dropped, so no handler formats it again.
        if record.exc_info:
            formatted = logging.Formatter().formatException(record.exc_info)
            record.exc_info = None
            record.exc_text = _mask_token_fragments(formatted, fragments)
        elif record.exc_text:
            record.exc_text = _mask_token_fragments(record.exc_text, fragments)
        if record.stack_info:
            record.stack_info = _mask_token_fragments(record.stack_info, fragments)
        return True


def _mask_token_fragments(text: str, fragments: tuple[str, ...]) -> str:
    """`text` with each fragment of the active bearer token replaced.

    Masks token fragments, not connection strings; DSNs go through
    `okg.substrate.db.dsn`.
    """
    for fragment in fragments:
        text = text.replace(fragment, "<redacted>")
    return text


_REDACTOR = _RedactActiveSecrets()
for _name in _CLIENT_LOGGERS:
    logging.getLogger(_name).addFilter(_REDACTOR)


# ------------------------------------------------------------- the wire

def _adapter_error(failure: str, **kwargs: Any) -> Exception:
    from okg.substrate.external_backed.live_dispatch import LiveAdapterError

    return LiveAdapterError(failure, **kwargs)


class _GuardTripped(Exception):
    """Raised inside the MCP client when the guard refuses; carries nothing."""


class _WireGuard:
    """What the transport saw, for the runner to classify after the call.

    The first refusal wins and sets `event`, which the runner watches so a
    refusal raised deep inside the SDK -- where the SDK may log and swallow
    it -- still ends the call at once rather than at the deadline.
    """

    def __init__(self, cap: int) -> None:
        import anyio

        self.cap = cap
        self.total = 0
        self.failure: str | None = None
        self.status: int | None = None
        self.excerpt: str | None = None
        self.event = anyio.Event()

    def refuse(
        self, failure: str, *, status: int | None = None,
        excerpt: str | None = None,
    ) -> None:
        if self.failure is None:
            self.failure = failure
            self.status = status
            self.excerpt = excerpt
            self.event.set()

    def charge(self, size: int) -> None:
        self.total += size
        if self.total > self.cap:
            self.refuse(
                "response_over_cap",
                excerpt=(
                    f"the reply exceeded max_response_bytes ({self.cap} "
                    f"bytes); reading stopped at {self.total} bytes and the "
                    f"reply was not parsed"
                ),
            )
            raise _GuardTripped("response_over_cap")


def _wire_classes() -> tuple[type, type]:
    """Build the httpx stream and transport wrappers on first use.

    Deferred so importing this module, which every server build does, does
    not import httpx and the MCP client for a deployment that binds no
    `mcp` relation.
    """
    import httpx

    class _CountingStream(httpx.AsyncByteStream):
        def __init__(self, inner: Any, guard: _WireGuard) -> None:
            self._inner = inner
            self._guard = guard

        async def __aiter__(self):  # type: ignore[override]
            async for chunk in self._inner:
                # Charged BEFORE the chunk is handed on, so the SDK never
                # receives a byte past the cap plus this one chunk.
                self._guard.charge(len(chunk))
                yield chunk

        async def aclose(self) -> None:
            await self._inner.aclose()

    class _GuardedTransport(httpx.AsyncBaseTransport):
        def __init__(self, guard: _WireGuard) -> None:
            self._guard = guard
            self._inner = httpx.AsyncHTTPTransport(retries=0)

        async def handle_async_request(self, request: Any) -> Any:
            guard = self._guard
            try:
                response = await self._inner.handle_async_request(request)
            except httpx.TimeoutException:
                guard.refuse("timeout")
                raise
            except httpx.TransportError as exc:
                guard.refuse(
                    "upstream_unavailable",
                    excerpt=(
                        f"the MCP server could not be reached "
                        f"({type(exc).__name__})"
                    ),
                )
                raise
            status = response.status_code
            # Every method, the session's closing DELETE included, is held to
            # the redirect and content-encoding refusals: the SDK reads the
            # DELETE reply whole (and would decompress it without a bound) and
            # follows a same-origin 307/308 with the bearer header. Only the
            # STATUS checks are waived for DELETE -- the call is over, and a
            # server that answers 405 to the close is legal.
            refusal = self._refusal_for(request.method, status, response)
            if refusal is not None:
                await response.aclose()
                guard.refuse(refusal[0], status=status, excerpt=refusal[1])
                raise _GuardTripped(refusal[0])
            return httpx.Response(
                status_code=status,
                headers=response.headers,
                stream=_CountingStream(response.stream, guard),
                extensions=response.extensions,
                request=request,
            )

        @staticmethod
        def _refusal_for(
            method: str, status: int, response: Any,
        ) -> tuple[str, str | None] | None:
            if 300 <= status < 400:
                # The `Location` target is never requested or echoed.
                return "redirect_refused", None
            encoding = response.headers.get("content-encoding", "").strip().lower()
            if encoding not in ("", "identity"):
                return "response_unusable", (
                    "the MCP server compressed its reply; a compressed byte "
                    "count does not bound the decoded size, so it is refused"
                )
            if method == "DELETE":
                return None
            if status in (401, 403):
                return "upstream_unavailable", (
                    "the MCP server's gate refused the request"
                )
            if method == "POST" and status >= 400:
                return "upstream_unavailable", None
            if method == "POST" and status == 200:
                media = response.headers.get("content-type", "").lower()
                if not media.startswith(("application/json", "text/event-stream")):
                    # Not an MCP endpoint. The SDK would log it and leave the
                    # call waiting for an answer until the deadline, which
                    # would read as a slow server rather than a wrong one.
                    return "response_unusable", (
                        "the server answered with a content type that is not "
                        "an MCP reply; check `server_url`"
                    )
            return None

        async def aclose(self) -> None:
            await self._inner.aclose()

    return _CountingStream, _GuardedTransport


@dataclass
class _CallOutcome:
    result: Any = None
    guard: _WireGuard | None = None
    timed_out: bool = False
    raised: BaseException | None = None
    #: Which step the session was waiting on, for a timeout to name:
    #: `open` (no MCP request sent yet), `initialize`, `tools/call` or
    #: `close`. A server that runs its tools one at a time answers nothing
    #: -- not even `initialize` -- while another client's call runs, and a
    #: bare "timeout" hid that.
    phase: str = "open"
    phase_started: float = 0.0
    initialize_seconds: float | None = None

    def enter(self, phase: str) -> None:
        self.phase = phase
        self.phase_started = time.monotonic()


def _phase_excerpt(outcome: _CallOutcome) -> str:
    """What a timed-out session was waiting for, in one sentence."""
    waited = max(0.0, time.monotonic() - outcome.phase_started)
    if outcome.phase == "initialize":
        return (
            f"the MCP server did not answer `initialize` within {waited:.1f} s, "
            f"so no tool call was sent; a server that runs tool calls one at "
            f"a time answers nothing while another call is running"
        )
    if outcome.phase == "tools/call":
        init = outcome.initialize_seconds or 0.0
        return (
            f"the MCP server answered `initialize` in {init:.1f} s, then sent "
            f"no tool result within {waited:.1f} s"
        )
    if outcome.phase == "close":
        return (
            "the tool result arrived, but closing the MCP session did not "
            "finish within the deadline"
        )
    return f"the MCP session was not opened within {waited:.1f} s"


def _close_margin(deadline_seconds: float) -> float:
    return min(
        _CLOSE_MARGIN_MAX_SECONDS,
        max(_CLOSE_MARGIN_MIN_SECONDS, deadline_seconds * _CLOSE_MARGIN_FRACTION),
    )


async def _call_tool_once(
    config: McpBindingConfig,
    arguments: Mapping[str, Any],
    secret: str | None,
    *,
    call_deadline: float,
    hard_deadline: float,
) -> _CallOutcome:
    """One session: initialize, ONE tools/call, close. Never retried."""
    import anyio
    import httpx
    from mcp.client.streamable_http import streamable_http_client
    from mcp.types import (
        CallToolRequest,
        CallToolRequestParams,
        CallToolResult,
        ClientRequest,
    )

    _, transport_class = _wire_classes()
    session_class = _refusing_session_class()
    outcome = _CallOutcome(guard=_WireGuard(config.max_response_bytes))
    outcome.enter("open")
    guard = outcome.guard
    assert guard is not None
    headers = {"Accept-Encoding": "identity"}
    if config.auth_header and secret:
        # The ONE header a binding may set, to the ONE value resolved at
        # load. No argument, and no part of one, reaches a header.
        headers[config.auth_header] = secret
    budget = max(0.001, hard_deadline - time.monotonic())
    client = httpx.AsyncClient(
        transport=transport_class(guard),
        headers=headers,
        timeout=httpx.Timeout(budget),
        follow_redirects=False,
        # `HTTP_PROXY` must not route a gated call through a host no
        # allowlist saw, as for `http`.
        trust_env=False,
    )
    request = ClientRequest(CallToolRequest(
        params=CallToolRequestParams(name=config.tool, arguments=dict(arguments)),
    ))

    with anyio.move_on_after(max(0.0, hard_deadline - time.monotonic())) as hard:
        async with client:
            try:
                async with streamable_http_client(
                    config.url, http_client=client, terminate_on_close=True,
                ) as (read, write, _session_id):
                    async with session_class(read, write, guard=guard) as session:
                        with anyio.CancelScope() as scope:
                            scope.deadline = anyio.current_time() + max(
                                0.0, call_deadline - time.monotonic(),
                            )
                            async with anyio.create_task_group() as watcher:
                                async def watch() -> None:
                                    await guard.event.wait()
                                    scope.cancel()

                                watcher.start_soon(watch)
                                outcome.enter("initialize")
                                await session.initialize()
                                outcome.initialize_seconds = (
                                    time.monotonic() - outcome.phase_started
                                )
                                outcome.enter("tools/call")
                                # `send_request`, NOT `call_tool`: the SDK's
                                # `call_tool` follows a successful result with
                                # a `tools/list` to validate it, which is a
                                # second request and another reply to read.
                                outcome.result = await session.send_request(
                                    request, CallToolResult,
                                )
                                outcome.enter("close")
                                watcher.cancel_scope.cancel()
                        if scope.cancelled_caught and guard.failure is None:
                            outcome.timed_out = True
            except Exception as exc:  # noqa: BLE001 -- classified by the caller.
                outcome.raised = exc
            except BaseExceptionGroup as group:
                # A task group can end with a cancellation BESIDE a real
                # error -- the SDK's reader failing while the watcher cancels
                # it. That group is not an `Exception`, so without this arm it
                # escaped every handler and left the adapter as an exception
                # type the dispatcher does not declare. The errors in it are
                # classified like any other. A group of cancellations alone
                # is re-raised untouched, for the scope that owns it.
                errors, _cancellations = group.split(Exception)
                if errors is None:
                    raise
                outcome.raised = errors
    if hard.cancelled_caught and guard.failure is None:
        outcome.timed_out = True
    return outcome


def _refusing_session_class() -> type:
    """A `ClientSession` that answers no request the SERVER starts.

    Built on first use for the same reason as the transport. The SDK's
    defaults reply with an error and the session then waits for a tool
    result that a server busy asking the client may never send, so the call
    would sit until its deadline and read as a slow server. Here a sampling,
    elicitation or roots request trips the guard, which ends the call at once
    with `response_unusable`. A ping is still answered: it asks nothing.
    """
    global _SESSION_CLASS
    if _SESSION_CLASS is not None:
        return _SESSION_CLASS
    from mcp import ClientSession
    from mcp import types as mcp_types

    class _RefusingClientSession(ClientSession):
        def __init__(self, *args: Any, guard: _WireGuard, **kwargs: Any) -> None:
            super().__init__(*args, **kwargs)
            self._okg_guard = guard

        async def _received_request(self, responder: Any) -> None:
            root = getattr(responder.request, "root", None)
            if isinstance(root, mcp_types.PingRequest):
                await super()._received_request(responder)
                return
            method = getattr(root, "method", type(root).__name__)
            self._okg_guard.refuse(
                "response_unusable",
                excerpt=(
                    f"the MCP server sent the client a `{method}` request; "
                    f"this adapter answers no server-initiated request"
                ),
            )

    _SESSION_CLASS = _RefusingClientSession
    return _SESSION_CLASS


_SESSION_CLASS: type | None = None


def _secret_fragments(secret: str | None) -> tuple[str, ...]:
    """The strings whose presence in a reply means the credential leaked.

    The whole header value, and each whitespace-separated part of it long
    enough not to occur by chance -- so `Bearer abc...` is caught whether the
    server echoes the header or only the token.
    """
    if not secret:
        return ()
    parts = {secret, *secret.split()}
    return tuple(p for p in parts if len(p) >= _MIN_SECRET_SCAN_CHARS)


def _without_credential(error: Any, fragments: tuple[str, ...]) -> Any:
    """`error`, or a fixed refusal when any of its text holds the credential."""
    if not fragments:
        return error
    texts = (
        str(getattr(error, "excerpt", "") or ""),
        str(error),
        repr(getattr(error, "data", None)),
    )
    if any(fragment in text for text in texts for fragment in fragments):
        return _adapter_error(
            "response_unusable",
            status=getattr(error, "status", None),
            excerpt=(
                "the MCP server's reply contains this binding's credential, "
                "so it is refused and not repeated"
            ),
        )
    return error


def _excerpt(text: str) -> str:
    return redact_userinfo(text[:ERROR_EXCERPT_CHARS])


def _classify_raised(exc: BaseException) -> Exception:
    """The failure for an exception the MCP client raised, never its text."""
    from mcp.shared.exceptions import McpError
    from pydantic import ValidationError

    leaves: list[BaseException] = [exc]
    flat: list[BaseException] = []
    while leaves:
        node = leaves.pop()
        if isinstance(node, BaseExceptionGroup):
            leaves.extend(node.exceptions)
        else:
            flat.append(node)
    for leaf in flat:
        if isinstance(leaf, McpError):
            # A JSON-RPC error from the server: an unknown tool, bad params,
            # a server fault. Its message is upstream text, so it travels
            # only as a bounded excerpt the dispatcher fences.
            return _adapter_error(
                "upstream_unavailable",
                excerpt=_excerpt(str(getattr(leaf.error, "message", "") or "")),
            )
    for leaf in flat:
        if isinstance(leaf, ValidationError):
            return _adapter_error(
                "response_unusable",
                excerpt="the MCP server's reply is not a valid tool result",
            )
    names = sorted({type(leaf).__name__ for leaf in flat})
    log.warning(
        "live mcp: the MCP client raised %s; reported as upstream_unavailable",
        ", ".join(names),
    )
    return _adapter_error(
        "upstream_unavailable",
        excerpt=(
            f"the MCP client failed ({', '.join(names)}); no message text is "
            f"passed on"
        ),
    )


def _follow_rows_path(node: Any, config: McpBindingConfig) -> list[Any]:
    shown = ".".join(config.rows_path) if config.rows_path else ROWS_PATH_ROOT
    for token in config.rows_path:
        if not isinstance(node, Mapping) or token not in node:
            raise _adapter_error(
                "result_unusable",
                excerpt=(
                    f"the tool result has no `{shown}`; its shape does not "
                    f"match this binding"
                ),
            )
        node = node[token]
    if isinstance(node, Mapping):
        return _object_rows(node, config, shown)
    if not isinstance(node, list):
        raise _adapter_error(
            "result_unusable",
            excerpt=(
                f"`{shown}` is a {type(node).__name__}, not a list of rows "
                f"or one object"
            ),
        )
    return node


def _object_rows(
    node: Mapping, config: McpBindingConfig, shown: str,
) -> list[Any]:
    """The rows of an object where `rows_path` lands, or a refusal.

    Many MCP tools answer with one object (`{"status": "ok", "data": {...}}`).
    Under a NAMED `rows_path` that object is one row, and an empty object is
    no rows. Two shapes are refused, because reading them as one row would
    put a whole list in a single row, where `max_rows` and the response
    budget no longer limit it:

    * an object at the root of `structuredContent`. MCP structured content
      is always an object, and FastMCP wraps a typed list as
      `{"result": [...]}`, so the root is almost never the row. (One text
      block holding one object at the root stays one row; that case is
      handled in `_rows_from_text_blocks`, before this is reached.)
    * a wrapper: an object with exactly one key, whose value is a list.
      The binding stopped one level short of the rows.
    """
    if not config.rows_path:
        raise _adapter_error(
            "result_unusable",
            excerpt=(
                f"the tool's structuredContent is an object (keys "
                f"{_shown_keys(node)}), not a list of rows; set `rows_path` "
                f"to the key holding the rows, or holding the one object "
                f"that is the row"
            ),
        )
    if not node:
        return []
    if len(node) == 1:
        (key, value), = node.items()
        if isinstance(value, list):
            raise _adapter_error(
                "result_unusable",
                excerpt=(
                    f"`{shown}` is an object whose only key "
                    f"`{_shown_key(key)}` holds a list, so it wraps the rows "
                    f"rather than being one; set "
                    f"`rows_path: {shown}.{_shown_key(key)}`"
                ),
            )
    return [node]


def _shown_key(key: Any) -> str:
    """One key of the reply, short enough to name in an excerpt."""
    return _excerpt(str(key)[:40])


def _shown_keys(node: Mapping, limit: int = 5) -> str:
    if not node:
        return "none"
    keys = [f"`{_shown_key(k)}`" for k in list(node)[:limit]]
    more = len(node) - limit
    return ", ".join(keys) + (f" and {more} more" if more > 0 else "")


def _project_rows(rows: list[Any], config: McpBindingConfig) -> list[Any]:
    if config.row_fields is None:
        return rows
    projected: list[Any] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise _adapter_error(
                "result_unusable",
                excerpt=(
                    f"row {index} is a {type(row).__name__}, not an object, "
                    f"so `row_fields` cannot select from it"
                ),
            )
        missing = [f for f in config.row_fields if f not in row]
        if missing:
            raise _adapter_error(
                "result_unusable",
                excerpt=(
                    f"row {index} has no "
                    f"{', '.join(f'`{m}`' for m in missing)}; the tool's shape "
                    f"does not match this binding"
                ),
            )
        projected.append({f: row[f] for f in config.row_fields})
    return projected


def _parse_text_block(text: str) -> Any:
    """One text block as JSON, or the named failure."""
    try:
        return parse_json_strict(text.encode("utf-8"))
    except HttpGuardRefusal as exc:
        if exc.reason == "body_too_deep":
            raise _adapter_error(
                "response_over_cap", excerpt=exc.detail,
            ) from None
        raise _adapter_error(
            "result_unusable",
            excerpt="the tool's text content is not JSON",
        ) from None


def _rows_from_text_blocks(
    result: Any, texts: list[str], config: McpBindingConfig,
) -> list[Any]:
    """Rows from a result with no `structuredContent` (decision `dbsrows A`).

    A FastMCP tool whose return type is not structured (`-> Any`) sends a
    list as ONE TEXT BLOCK PER ITEM, and a one-item list as one block
    holding a bare object -- Archi's DBS server does exactly this. So:

    * one text block is parsed and read at `rows_path` as before; at the
      root, a list is the rows and a single object is one row; under a
      named path, an object is read as in `_object_rows`;
    * several text blocks are one row each, in order, and each must hold
      exactly one JSON object. `rows_path` must then be the root: a path
      names a place inside ONE document, and there is no one document;
    * no block at all is no rows, at the root (an empty list sends none).

    Any non-text block is refused: an image is not a row.
    """
    if len(texts) != len(result.content):
        raise _adapter_error(
            "result_unusable",
            excerpt=(
                f"the tool returned {len(result.content) - len(texts)} "
                f"non-text content block(s) and no structuredContent"
            ),
        )
    if len(texts) == 1:
        node = _parse_text_block(texts[0])
        if not config.rows_path and isinstance(node, Mapping):
            return [node]
        return _follow_rows_path(node, config)
    if config.rows_path:
        raise _adapter_error(
            "result_unusable",
            excerpt=(
                f"the tool returned {len(texts)} text blocks and no "
                f"structuredContent, so there is no one document to read "
                f"`{'.'.join(config.rows_path)}` in; a binding for a "
                f"one-block-per-row tool sets `rows_path: {ROWS_PATH_ROOT}`"
            ),
        )
    rows: list[Any] = []
    # `max_rows` cuts by count after the rows are read; with one row per
    # block, the blocks past it are never parsed at all.
    if config.max_rows is not None:
        texts = texts[: config.max_rows]
    for index, text in enumerate(texts):
        node = _parse_text_block(text)
        if not isinstance(node, Mapping):
            raise _adapter_error(
                "result_unusable",
                excerpt=(
                    f"text block {index} of {len(texts)} holds a "
                    f"{type(node).__name__}, not one JSON object; with more "
                    f"than one block, each block must be one row"
                ),
            )
        rows.append(node)
    return rows


def _rows_from_result(
    result: Any, config: McpBindingConfig, secret: str | None,
) -> list[Any]:
    """Turn one `CallToolResult` into rows, or raise a named failure."""
    from mcp.types import TextContent

    # Counted FIRST, before the credential scan serialises the result and
    # before any block is parsed: 8 MiB of two-byte JSON objects is hundreds
    # of thousands of blocks, and each would become a dict here. No answer
    # this adapter serves has more rows than `MAX_MAX_ROWS`, so a reply with
    # more content blocks than that is refused unread.
    if len(result.content) > MAX_MAX_ROWS:
        raise _adapter_error(
            "result_unusable",
            excerpt=(
                f"the tool returned {len(result.content)} content blocks; at "
                f"most {MAX_MAX_ROWS} are read, so the reply was refused "
                f"before any block was parsed"
            ),
        )
    fragments = _secret_fragments(secret)
    if fragments:
        dumped = result.model_dump_json()
        if any(fragment in dumped for fragment in fragments):
            # Refused, and the reply is not echoed: the excerpt would carry
            # the very value this check exists to keep out of the envelope.
            raise _adapter_error(
                "response_unusable",
                excerpt=(
                    "the MCP server's reply contains this binding's "
                    "credential, so it is refused and not repeated"
                ),
            )
    texts = [
        block.text for block in result.content if isinstance(block, TextContent)
    ]
    if result.isError:
        message = "\n".join(texts)
        folded = message.casefold()
        if any(marker in folded for marker in _EMPTY_MATCH_MARKERS):
            # The server answered: this query matched nothing. Its own
            # message travels as the excerpt, so the caller reads what the
            # system said rather than a guess at it.
            raise _adapter_error("upstream_empty_result", excerpt=_excerpt(message))
        raise _adapter_error(
            "upstream_unavailable",
            excerpt=_excerpt(message) or "the tool reported an error",
        )
    if result.structuredContent is not None:
        rows = _follow_rows_path(result.structuredContent, config)
    else:
        rows = _rows_from_text_blocks(result, texts, config)
    if config.max_rows is not None:
        rows = rows[: config.max_rows]
    selected = _project_rows(list(rows), config)

    from okg.substrate.external_backed.live_dispatch import (
        _assert_result_is_shapeable,
    )

    # Raises `result_unusable` itself for rows too deep or too wide.
    _assert_result_is_shapeable(selected)
    return selected


def run_mcp_relation_for(
    config: McpBindingConfig, secret: str | None,
) -> Callable[..., list[Any]]:
    """Build the per-relation runner, closing over the resolved secret."""

    def run_mcp_relation(
        *,
        args: Mapping[str, Any],
        node_id: str,
        deadline_seconds: float,
        **_ignored: Any,
    ) -> list[Any]:
        started = time.monotonic()
        hard_deadline = started + float(deadline_seconds)
        call_deadline = hard_deadline - _close_margin(float(deadline_seconds))
        try:
            assert_host_allowed(
                config.address, config.port, config.allowed,
                allow_non_global=config.allow_non_global,
            )
        except HttpGuardRefusal:
            # Unreachable after a successful load; asserted, not assumed.
            raise _adapter_error("upstream_unavailable") from None

        rendered = _substitute(config.arguments, args)
        fragments = _secret_fragments(secret)
        try:
            return _run_once(rendered, fragments, call_deadline, hard_deadline)
        except LiveAdapterError as error:
            # EVERY failure leaving the adapter is scanned, whatever built it:
            # a JSON-RPC error message, an `isError` text and a transport
            # excerpt are all upstream text, and the server may echo the
            # header it was sent.
            raise _without_credential(error, fragments) from None

    def _run_once(
        rendered: Any, fragments: tuple[str, ...],
        call_deadline: float, hard_deadline: float,
    ) -> list[Any]:
        import anyio

        _REDACTOR.activate(fragments)
        try:
            outcome = anyio.run(
                lambda: _call_tool_once(
                    config, rendered, secret,
                    call_deadline=call_deadline, hard_deadline=hard_deadline,
                ),
            )
        except BaseException as exc:  # noqa: BLE001 -- the backstop of the backstop.
            if not isinstance(exc, (Exception, BaseExceptionGroup)):
                raise
            failure = _classify_raised(exc)
            raise failure from None
        finally:
            _REDACTOR.deactivate(fragments)

        guard = outcome.guard
        if guard is not None and guard.failure is not None:
            excerpt = guard.excerpt
            if guard.failure == "timeout" and excerpt is None:
                excerpt = _phase_excerpt(outcome)
            raise _adapter_error(
                guard.failure, status=guard.status, excerpt=excerpt,
            )
        if outcome.timed_out:
            # Same deadline as before; only the report changed: it names the
            # step the server never answered.
            raise _adapter_error("timeout", excerpt=_phase_excerpt(outcome))
        if outcome.raised is not None:
            failure = _classify_raised(outcome.raised)
            raise failure
        if outcome.result is None:
            raise _adapter_error(
                "upstream_unavailable",
                excerpt="the MCP session ended without a tool result",
            )
        return _rows_from_result(outcome.result, config, secret)

    from okg.substrate.external_backed.live_dispatch import LiveAdapterError

    def _entry(*, config: Mapping[str, Any], **kwargs: Any) -> list[Any]:
        # The dispatcher passes the binding's raw `config=`; the closure's
        # own parsed config is the one trusted.
        return run_mcp_relation(**kwargs)

    _entry.__name__ = f"run_mcp_relation[{config.relation}]"
    return _entry


# -------------------------------------------------------- the load hook

def validate_mcp_binding_configs(
    bindings: Mapping[str, Any],
    relations: Mapping[str, Any],
    *,
    runners: Mapping[str, Any],
) -> dict[str, Callable[..., Any]]:
    """Check every `kind: mcp` binding at LOAD and return its bound runner.

    A caller-supplied `mcp` runner (the `live_adapters` test seam) skips the
    config parser, exactly as for `http`.
    """
    bound: dict[str, Callable[..., Any]] = {}
    for name in sorted(bindings):
        binding = bindings[name]
        if getattr(binding, "kind", None) != "mcp":
            continue
        if runners.get("mcp") is not None:
            continue
        declared = relations.get(name)
        if declared is None:
            raise LiveAdapterConfigInvalid(
                f"live binding {name!r} has no compiled declaration to check "
                f"its config against.",
                data={"relation": name, "field": "", "reason": "no declaration"},
                fix_hint="recompose the catalog and rebuild the server",
            )
        config = parse_mcp_binding(name, binding, declared)
        secret = _resolve_secret(name, binding)
        bound[name] = run_mcp_relation_for(config, secret)
    return bound


def _unbound_mcp_runner(**_kwargs: Any) -> list[Any]:
    """The kind-level `mcp` entry, which must never actually run.

    As for `http`: the runner a call needs is keyed by RELATION and
    installed by `_build_server`. Reaching this means that wiring was
    skipped, which is a defect in this process, not an upstream fault.
    """
    raise _adapter_error(
        "upstream_unavailable",
        excerpt=(
            "per-relation runner not installed: the adapter kind is "
            "registered but this relation's bound runner is missing, so no "
            "session was opened. This is a wiring defect in the server "
            "build, not a fault of the MCP server"
        ),
    )


def register() -> None:
    from okg.substrate.external_backed.live_dispatch import (
        BUILTIN_LIVE_ADAPTERS,
    )

    BUILTIN_LIVE_ADAPTERS["mcp"] = _unbound_mcp_runner


register()
