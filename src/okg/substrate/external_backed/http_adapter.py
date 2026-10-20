"""The built-in live-relation adapter for adapter kind `http`.

Issue #2100. This is the layer #2083 assumes and does not provide: a runner
that turns an agent's validated arguments into one outbound HTTP request and
its response into rows. It is the most security-sensitive component of the
sprint, because it is the only place in the substrate where a caller's input
becomes network traffic.

**It is not OpenSearch-specific, and must not become so.** It copies an
operator-authored JSON tree and fills typed holes in it. It never learns what
`query_string` or `date_histogram` mean; the operator does, which is correct,
because the operator is the one who chose the index and the field names.
#2014 binds OpenSearch with this adapter and adds nothing to it.

Two halves, and the split is the point:

* :func:`validate_live_binding_configs` runs at server BUILD. Every refusal
  this module can make happens there, naming the relation and the field, so a
  deployment whose binding could never produce a legal request does not
  start. It returns bound runners -- closures over the load-resolved secret.
* :func:`_make_runner`'s closure runs per call, on the dispatcher's worker
  thread, and does only what could not be decided at load: substitute the
  validated arguments, dial the checked address, read a bounded body.

The secret travels in the CLOSURE and nowhere else. `LiveBinding.config` is
copied by the dispatcher and can reach a traceback's `repr()`; a closure cell
cannot.
"""
from __future__ import annotations

import contextlib
import json
import logging
import os
import re
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Mapping

from okg.substrate.errors import SubstrateError
from okg.substrate.external_backed._http_guard import (
    DEFAULT_PORTS,
    HostPort,
    HttpGuardRefusal,
    assert_host_allowed,
    parse_host_port,
    parse_json_strict,
    read_bounded_body,
    redact_url,
    redact_userinfo,
    remaining_budget,
    split_base_url,
)

log = logging.getLogger("okg.substrate.external_backed.http_adapter")

__all__ = [
    "HTTP_CONFIG_FIELDS",
    "HttpBindingConfig",
    "LiveAdapterArgUnconstrained",
    "LiveAdapterConfigInvalid",
    "LiveAdapterHostRefused",
    "LiveAdapterSecretMissing",
    "parse_http_binding",
    "run_http_relation_for",
    "validate_live_binding_configs",
]


# ----------------------------------------------------------------- errors

class LiveAdapterConfigInvalid(SubstrateError):
    """A live binding's `config` block cannot produce a legal request.

    LOAD-time, always. The alternative is discovering it on an agent's first
    call, by which time the deployment has been advertising a relation
    through `inspect` that it was never able to serve.
    """

    code = "live_adapter_config_invalid"


class LiveAdapterSecretMissing(SubstrateError):
    """A binding's `env:NAME` secret reference resolves to nothing.

    The data carries the REFERENCE name and never the value -- there is no
    value, which is the problem, but the rule holds on every path so there is
    no path where it might not.
    """

    code = "live_adapter_secret_missing"


class LiveAdapterArgUnconstrained(SubstrateError):
    """A slotted argument carries no constraint this adapter can bound.

    The mechanism keeps `enum`, `pattern`, `minimum`, `maximum` and
    `maxLength` optional, which is right for a mechanism. It is wrong here:
    an unbounded string is an unbounded string in someone's query DSL. An
    adapter that cannot make an argument safe declines to serve it.
    """

    code = "live_adapter_arg_unconstrained"


class LiveAdapterHostRefused(SubstrateError):
    """A binding's `base_url` names a host the egress rules will not dial."""

    code = "live_adapter_host_refused"


# ------------------------------------------------------------ the grammar

#: Every key `config` may carry. An unknown key is REFUSED, not ignored: a
#: silently dropped key is how an operator comes to believe a limit is set.
HTTP_CONFIG_FIELDS = frozenset({
    "allow_non_global",
    "allowed_hosts",
    "auth_header",
    "base_url",
    "body",
    "content_type",
    "max_response_bytes",
    "max_rows",
    "method",
    "path",
    "row_fields",
    "rows_path",
})

#: The only methods. `GET` for a relation with no arguments, `POST` with a
#: body for everything else.
ALLOWED_METHODS = ("GET", "POST")

#: The slot key. One key, one grammar, no escape and no second parsing mode.
SLOT_KEY = "$arg"

DEFAULT_MAX_RESPONSE_BYTES = 8 * 1024 * 1024
MIN_MAX_RESPONSE_BYTES = 1024
MAX_MAX_RESPONSE_BYTES = 32 * 1024 * 1024
MAX_MAX_ROWS = 10_000

#: A chunk big enough that the between-chunk deadline check is cheap and
#: small enough that a drip cannot hide a long silence inside one read.
CHUNK_BYTES = 65536

#: Longest upstream error body echoed back, before fencing.
ERROR_EXCERPT_BYTES = 256

_HEADER_NAME = re.compile(r"^[A-Za-z0-9!#$%&'*+.^_`|~-]+$")
_ROWS_PATH = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*$")
_ROW_FIELD = re.compile(r"^[a-z][a-z0-9_]*$")

#: The ONE spelling that means "the parsed body itself is the list of rows".
#: A real API returns a top-level JSON array over GET -- Glances 4.5.6 does it
#: for per-CPU and process-list endpoints -- and before this there was no way
#: to bind one: every dotted path requires an object at each step, so such a
#: response was refused at load AND at runtime.
#:
#: A single character, chosen and documented rather than inferred. An empty
#: string would have been the obvious alternative and is deliberately NOT
#: accepted: "" is what an absent or mistyped key looks like, and a
#: configuration language should not spell "the whole document" the same way
#: it spells "I forgot to fill this in".
ROWS_PATH_ROOT = "."

#: Header names a binding may never set, because the adapter or the transport
#: owns them and an override would either break framing or move the request.
_FORBIDDEN_HEADERS = frozenset({
    "host", "content-length", "content-type", "connection",
    "transfer-encoding", "keep-alive", "proxy-authorization",
    "proxy-authenticate", "te", "trailer", "upgrade", "expect",
})

#: JSON media types the adapter will read. Anything else is a refusal: a
#: text or HTML fallback is how an error page becomes a "result".
#: Media types the adapter will read, matched EXACTLY.
_JSON_MEDIA = ("application/json", "text/json")

#: The one structured-suffix form, matched as a suffix. Kept separate from
#: the exact list on purpose: testing every entry with `endswith` accepted
#: `evil/application/json` and `x-text/json`, which are types this adapter
#: knows nothing about and an upstream can set at will.
_JSON_SUFFIX = "+json"


def _secret_refs_error(relation: str, reason: str) -> LiveAdapterConfigInvalid:
    """Build the refusal, so a caller inside an `except` can suppress context.

    Separate from :func:`_refuse_secret_refs` because `raise X from None` has
    to be written AT the raise, and a helper that raises for you cannot do
    it on your behalf.
    """
    return LiveAdapterConfigInvalid(
        f"live binding {relation!r}: `live_bindings.{relation}.secret_refs` "
        f"{reason}",
        data={"relation": relation, "field": "secret_refs", "reason": reason},
        fix_hint=(
            f"declare exactly one `env:NAME` reference on "
            f"`live_bindings.{relation}.secret_refs`; the value comes from "
            f"the server process's environment and nowhere else"
        ),
    )


def _refuse_secret_refs(relation: str, reason: str) -> None:
    """Refuse a binding's `secret_refs`, naming the field it really is.

    NOT `_refuse`, for two reasons that both matter on this path.

    `_refuse` writes the path `live_bindings.<relation>.config.<field>`, and
    `secret_refs` is a SIBLING of `config`, not a key inside it. An operator
    sent to `config.secret_refs` goes looking at a block that does not have
    the key.

    And the reason string reaches `data["reason"]`, so anything interpolated
    into it is published in the error envelope. Every caller here passes a
    reason that quotes nothing from the binding.

    A caller inside an `except` block must use :func:`_secret_refs_error`
    and `raise ... from None` instead, so the wrapped exception does not
    survive as `__context__`.
    """
    raise _secret_refs_error(relation, reason)


def _refuse(relation: str, field: str, reason: str) -> None:
    raise LiveAdapterConfigInvalid(
        f"live binding {relation!r}: `live_bindings.{relation}.config."
        f"{field}` {reason}",
        data={"relation": relation, "field": field, "reason": reason},
        fix_hint=(
            f"correct `live_bindings.{relation}.config.{field}` in the "
            f"deployment manifest; see docs/reference/live-http-bindings.md"
        ),
    )


# ------------------------------------------------------------- the config

@dataclass(frozen=True)
class HttpBindingConfig:
    """One parsed, checked binding. **Carries no secret.**

    Everything here was decided at load: the address is canonical, the port
    is exact, the body's slots are known to name declared arguments and the
    arguments are known to be constrained. The runner re-checks the address
    anyway (§3: unreachable after a successful load, and tested as such),
    because "unreachable" is a belief.
    """

    relation: str
    scheme: str
    address: Any
    port: int
    allowed: tuple[HostPort, ...]
    allow_non_global: bool
    method: str
    path: str
    content_type: str
    auth_header: str | None
    body: Any
    rows_path: tuple[str, ...]
    max_rows: int
    max_response_bytes: int
    #: The top-level keys each row keeps, or None to keep whole rows.
    row_fields: tuple[str, ...] | None = None

    @property
    def url(self) -> str:
        host = str(self.address)
        shown = f"[{host}]" if ":" in host else host
        return f"{self.scheme}://{shown}:{self.port}{self.path}"


# ------------------------------------------------------- slot walking

def _walk_slots(
    node: Any, relation: str, trail: str = "body",
    refuse: Callable[[str, str, str], None] | None = None,
) -> set[str]:
    """Collect every slot name, refusing every malformed or misplaced slot.

    A slot is legal ONLY in value position. This walk therefore inspects
    keys separately from values and refuses a slot object found as a key,
    which `json` would have accepted as a string key and silently flattened.

    `refuse` lets the `mcp` adapter reuse this ONE grammar while naming its
    own docs page in the refusal; it defaults to this module's `_refuse`.
    """
    say_no = refuse or _refuse
    names: set[str] = set()
    if isinstance(node, Mapping):
        if SLOT_KEY in node:
            extra = sorted(set(node) - {SLOT_KEY})
            if extra:
                # Sibling keys are how a SECOND reading of the same object
                # creeps in -- a default, a type coercion, a format. Refused
                # rather than ignored, so the grammar stays one grammar.
                say_no(
                    relation, trail,
                    f"is a `{SLOT_KEY}` slot with sibling key(s) {extra}. A "
                    f"slot is an object with exactly one key",
                )
            value = node[SLOT_KEY]
            if not isinstance(value, str) or not value.strip():
                say_no(
                    relation, trail,
                    f"has a `{SLOT_KEY}` value that is not an argument name "
                    f"({type(value).__name__})",
                )
            return {value.strip()}
        for key, value in node.items():
            if not isinstance(key, str):
                say_no(
                    relation, trail,
                    f"uses a non-string object key {key!r}; the body must be "
                    f"JSON",
                )
            if SLOT_KEY in key:
                say_no(
                    relation, f"{trail}.{key}",
                    f"puts `{SLOT_KEY}` in KEY position. An argument may "
                    f"only ever be a value, never a key, so that it cannot "
                    f"change the SHAPE of the request",
                )
            names |= _walk_slots(value, relation, f"{trail}.{key}", refuse)
        return names
    if isinstance(node, (list, tuple)):
        for index, item in enumerate(node):
            names |= _walk_slots(item, relation, f"{trail}[{index}]", refuse)
        return names
    if isinstance(node, str) and SLOT_KEY in node:
        # A slot spelled inside a string is a TEMPLATE, and there is no
        # templating here. Refused by name rather than copied through as a
        # literal, because an operator who wrote it meant it to substitute.
        say_no(
            relation, trail,
            f"contains the text `{SLOT_KEY}` inside a string. There is no "
            f"string templating: a slot is the object {{\"{SLOT_KEY}\": "
            f"\"name\"}} standing alone in value position",
        )
    return names


def _substitute(node: Any, args: Mapping[str, Any]) -> Any:
    """Replace every slot NODE with its validated value, in a SINGLE PASS.

    Structural, never textual. The returned tree is freshly built, so the
    operator's parsed body is never mutated and a second call sees the same
    template. A substituted value is NEVER rescanned, so an argument whose
    value happens to be the string `{"$arg": "x"}` stays that string.
    """
    if isinstance(node, Mapping):
        if SLOT_KEY in node:
            # Terminal. Whatever the argument's value is, it is placed here
            # and the walk does not descend into it.
            return args[node[SLOT_KEY]]
        return {key: _substitute(value, args) for key, value in node.items()}
    if isinstance(node, (list, tuple)):
        return [_substitute(item, args) for item in node]
    return node


#: What each argument type needs before this adapter will slot it. Absent, not
#: weak -- see :class:`LiveAdapterArgUnconstrained` and the docs page, which
#: say so in the same words because an operator who thinks the check is
#: stronger than it is writes a weaker pattern than they otherwise would.
def _constraint_satisfied(arg: Any) -> bool:
    if arg.type_name == "boolean":
        return True
    if arg.enum is not None:
        return True
    if arg.type_name == "string":
        return arg.pattern is not None and arg.max_length is not None
    return arg.minimum is not None and arg.maximum is not None


def _constraint_wanted(type_name: str) -> str:
    if type_name == "boolean":
        return "nothing"
    if type_name == "string":
        return "`enum`, or `pattern` together with `maxLength`"
    return "`enum`, or both `minimum` and `maximum`"


# ---------------------------------------------------------------- parsing

def _require_bool(relation: str, field: str, value: Any, default: bool) -> bool:
    if value is None:
        return default
    if not isinstance(value, bool):
        _refuse(relation, field, f"must be true or false; got {type(value).__name__}")
    return bool(value)


def _require_int(
    relation: str, field: str, value: Any, *,
    default: int | None, low: int, high: int,
) -> int:
    if value is None:
        if default is None:
            _refuse(relation, field, "is required")
        return int(default)  # type: ignore[arg-type]
    # `bool` is an `int` in Python, and `max_rows: true` is not a row count.
    if isinstance(value, bool) or not isinstance(value, int):
        _refuse(relation, field, f"must be an integer; got {type(value).__name__}")
    if not low <= int(value) <= high:
        _refuse(relation, field, f"must be between {low} and {high}; got {value}")
    return int(value)


def _parse_allowed_hosts(
    relation: str, raw: Any, *, default_port: int,
) -> tuple[HostPort, ...]:
    if not isinstance(raw, (list, tuple)) or not raw:
        _refuse(
            relation, "allowed_hosts",
            "must be a non-empty list of `ip` or `ip:port` entries. An empty "
            "allowlist would authorise nothing, and a missing one would "
            "authorise everything",
        )
    entries: list[HostPort] = []
    for item in raw:
        if not isinstance(item, str):
            _refuse(
                relation, "allowed_hosts",
                f"entry must be a string; got {type(item).__name__}",
            )
        try:
            entries.append(parse_host_port(item, default_port=default_port))
        except HttpGuardRefusal as exc:
            raise LiveAdapterHostRefused(
                f"live binding {relation!r}: `live_bindings.{relation}."
                f"config.allowed_hosts` entry {item!r} is refused "
                f"({exc.reason}): {exc.detail}",
                data={"relation": relation, "host": item, "reason": exc.reason},
                fix_hint=(
                    "write an IP literal, optionally with a port, bracketing "
                    "IPv6 -- `127.0.0.1`, `127.0.0.1:9200`, `[::1]:9200`. "
                    "Hostnames are not supported in this version because a "
                    "name checked at load can resolve differently at connect"
                ),
            ) from None
    return tuple(entries)


def parse_http_binding(
    relation: str, binding: Any, declared: Any,
) -> HttpBindingConfig:
    """Check one `kind: http` binding against its relation, or refuse.

    `declared` is the compiled :class:`LiveRelation`, needed because half the
    checks are about the AGREEMENT between the binding and the declaration:
    every slot must name a declared argument, every declared argument must be
    slotted, and every slotted argument must be constrained. A parser that
    saw only the config could check none of the three.
    """
    config = binding.config
    if not isinstance(config, Mapping):
        _refuse(relation, "", f"must be a mapping; got {type(config).__name__}")
    unknown = sorted(set(config) - HTTP_CONFIG_FIELDS)
    if unknown:
        _refuse(
            relation, unknown[0],
            f"is not a recognised field. Unknown key(s) {unknown} are "
            f"REFUSED rather than ignored, because a silently dropped key is "
            f"how an operator comes to believe a limit is set. Known fields "
            f"are {sorted(HTTP_CONFIG_FIELDS)}",
        )

    method = config.get("method") or "GET"
    if not isinstance(method, str) or method.upper() not in ALLOWED_METHODS:
        _refuse(
            relation, "method",
            f"must be one of {list(ALLOWED_METHODS)}; got {method!r}",
        )
    method = method.upper()

    base_url = config.get("base_url")
    if not isinstance(base_url, str) or not base_url.strip():
        _refuse(relation, "base_url", "is required and must be a string")
    try:
        scheme, address, port = split_base_url(base_url.strip())
    except HttpGuardRefusal as exc:
        raise LiveAdapterHostRefused(
            f"live binding {relation!r}: `live_bindings.{relation}.config."
            f"base_url` is refused ({exc.reason}): {exc.detail}",
            # The REDACTED url: a base_url carrying userinfo is refused, and
            # the refusal must not be the thing that prints it.
            data={"relation": relation, "host": redact_url(base_url),
                  "reason": exc.reason},
            fix_hint=(
                "`base_url` carries scheme, IP literal and port and nothing "
                "else -- no path, query, fragment or credentials. The path "
                "goes in `path`"
            ),
        ) from None

    allowed = _parse_allowed_hosts(
        relation, config.get("allowed_hosts"),
        default_port=DEFAULT_PORTS[scheme],
    )
    allow_non_global = _require_bool(
        relation, "allow_non_global", config.get("allow_non_global"), False,
    )
    # The SAME check the runner makes, at load. A binding whose own base_url
    # is outside its own allowlist can never dispatch, and finding that out
    # on an agent's first call is finding it out too late.
    try:
        assert_host_allowed(
            address, port, allowed, allow_non_global=allow_non_global,
        )
    except HttpGuardRefusal as exc:
        raise LiveAdapterHostRefused(
            f"live binding {relation!r}: `live_bindings.{relation}.config."
            f"base_url` may not be dialled ({exc.reason}): {exc.detail}",
            data={"relation": relation, "host": redact_url(base_url),
                  "reason": exc.reason},
            fix_hint=(
                "add the exact `ip:port` to `allowed_hosts`, and set "
                "`allow_non_global: true` if it is a loopback, private or "
                "link-local address -- deliberately one visible line, "
                "because that is the fact a reviewer should see"
            ),
        ) from None

    path = config.get("path")
    if not isinstance(path, str) or not path.startswith("/"):
        _refuse(relation, "path", "is required and must start with `/`")
    if "?" in path or "#" in path:
        _refuse(
            relation, "path",
            "must not contain `?` or `#`. Query strings are not supported in "
            "this version: there is no place to put an argument that has an "
            "adapter-applied encoding, and a half-specified one is worse "
            "than none",
        )
    if SLOT_KEY in path:
        _refuse(
            relation, "path",
            f"contains `{SLOT_KEY}`. An argument may never reach the URL: it "
            f"would choose the resource, not the question",
        )

    content_type = config.get("content_type") or "application/json"
    if not isinstance(content_type, str) or not _is_json_media(content_type):
        _refuse(
            relation, "content_type",
            f"must be a JSON media type; got {content_type!r}",
        )

    auth_header = config.get("auth_header")
    secret_refs = tuple(getattr(binding, "secret_refs", ()) or ())
    if auth_header is not None:
        if not isinstance(auth_header, str) or not _HEADER_NAME.fullmatch(auth_header):
            _refuse(
                relation, "auth_header",
                f"must be a bare header NAME, never a value; got "
                f"{type(auth_header).__name__}. A literal credential here "
                f"would be a second way to configure a secret, and therefore "
                f"a second way to leak one",
            )
        if auth_header.lower() in _FORBIDDEN_HEADERS:
            _refuse(
                relation, "auth_header",
                f"may not set {auth_header!r}: the adapter or the transport "
                f"owns that header",
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

    rows_path = config.get("rows_path")
    if rows_path == ROWS_PATH_ROOT:
        # The root: the body itself must be the list. Everything else still
        # has to be a dotted path, so `..`, `$`, `.a` and `a.` stay refused.
        parsed_rows_path: tuple[str, ...] = ()
    elif not isinstance(rows_path, str) or not _ROWS_PATH.fullmatch(rows_path):
        _refuse(
            relation, "rows_path",
            f"is required and must be a dotted path of lower-snake tokens "
            f"(`hits.hits`), or `{ROWS_PATH_ROOT}` when the response body is "
            f"itself the list of rows; got {rows_path!r}",
        )
    else:
        parsed_rows_path = tuple(rows_path.split("."))

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
        # An upstream row can carry fields nobody asked for and whose size
        # this deployment does not control (a process's full command line).
        # Naming the kept keys bounds each row by the binding, not by
        # whatever the upstream happened to put in it.
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
                "must be a non-empty list of distinct lower-snake keys "
                f"(`[pid, name]`); got {row_fields_raw!r}",
            )
        row_fields = tuple(row_fields_raw)

    body = config.get("body")
    declared_args = {arg.name: arg for arg in getattr(declared, "args", ())}
    if method == "GET":
        if body is not None:
            _refuse(
                relation, "body",
                "is set on a `GET`. A GET carries no body; the binding "
                "either means POST or means to send nothing",
            )
        if declared_args:
            # A GET has no body and no query parameters in this version, so
            # it has NOWHERE to put an argument. Accepting it would publish a
            # relation through `inspect` whose parameters do nothing -- the
            # agent would vary them and get the same answer, which is worse
            # than a refusal.
            _refuse(
                relation, "body",
                f"is absent on a `GET`, but the relation declares "
                f"argument(s) {sorted(declared_args)} and a GET has nowhere "
                f"to put them. `inspect` would advertise parameters that do "
                f"nothing. Use `method: POST` with a body",
            )
        slots: set[str] = set()
    else:
        if not isinstance(body, Mapping):
            _refuse(
                relation, "body",
                "is required on a `POST` and must be a JSON object",
            )
        slots = _walk_slots(body, relation)

    unknown_slots = sorted(slots - set(declared_args))
    if unknown_slots:
        raise LiveAdapterConfigInvalid(
            f"live binding {relation!r}: the body slots argument(s) "
            f"{unknown_slots}, which the relation does not declare. Declared "
            f"arguments are {sorted(declared_args)}.",
            data={"relation": relation, "field": "body",
                  "reason": f"undeclared argument(s) {unknown_slots}"},
            fix_hint=(
                "fix the spelling, or declare the argument on the subtype's "
                "`okg_live_relations` annotation"
            ),
        )
    unslotted = sorted(set(declared_args) - slots)
    if unslotted:
        # The other direction, and it is not symmetry for its own sake: an
        # argument `inspect` advertises and the body never uses is a knob
        # that does nothing, which an agent discovers by getting the same
        # answer for every value.
        raise LiveAdapterConfigInvalid(
            f"live binding {relation!r}: the relation declares argument(s) "
            f"{unslotted} that no slot in the body uses. `inspect` would "
            f"advertise a parameter that changes nothing.",
            data={"relation": relation, "field": "body",
                  "reason": f"unused declared argument(s) {unslotted}"},
            fix_hint=(
                "slot each declared argument into the body, or remove it "
                "from the declaration"
            ),
        )
    for name in sorted(slots):
        arg = declared_args[name]
        if not _constraint_satisfied(arg):
            raise LiveAdapterArgUnconstrained(
                f"live binding {relation!r}: argument {name!r} of type "
                f"{arg.type_name!r} is slotted into the request body but "
                f"carries no constraint. This adapter requires "
                f"{_constraint_wanted(arg.type_name)}. The rule catches an "
                f"ABSENT constraint, not a weak one: `pattern: \"^.*$\"` with "
                f"a `maxLength` satisfies it and still admits anything the "
                f"upstream query language accepts. Whether a constraint is "
                f"TIGHT enough is a fact about that language and your index, "
                f"so you own its quality -- this rule only guarantees there "
                f"is one to review.",
                data={"relation": relation, "argument": name,
                      "type": arg.type_name},
                fix_hint=(
                    f"add {_constraint_wanted(arg.type_name)} to argument "
                    f"{name!r} on the subtype's `okg_live_relations` "
                    f"annotation. Prefer `enum` wherever the set is knowable "
                    f"-- it is published by `inspect`, so the agent is told "
                    f"the choices instead of guessing and being refused"
                ),
            )

    return HttpBindingConfig(
        relation=relation,
        scheme=scheme,
        address=address,
        port=port,
        allowed=allowed,
        allow_non_global=allow_non_global,
        method=method,
        path=path,
        content_type=content_type,
        auth_header=auth_header,
        body=json.loads(json.dumps(body)) if body is not None else None,
        rows_path=parsed_rows_path,
        max_rows=max_rows,
        max_response_bytes=max_response_bytes,
        row_fields=row_fields,
    )


def _is_json_media(value: str) -> bool:
    """True for a JSON media type, and only for one.

    The type is compared EXACTLY against the known names; only the
    structured-suffix form `+json` is matched as a suffix, and only after a
    `/` has been seen, so `application/vnd.api+json` is JSON while
    `evil/application/json` and `x-text/json` are not.
    """
    head = value.split(";", 1)[0].strip().lower()
    if head in _JSON_MEDIA:
        return True
    subtype = head.partition("/")[2]
    return bool(subtype) and subtype.endswith(_JSON_SUFFIX)


# ---------------------------------------------------------------- secrets

def _resolve_secret(relation: str, binding: Any) -> str | None:
    """Resolve the binding's one `env:NAME` reference, at LOAD.

    `handle._validate_secret_refs` is reused for the inline-credential
    heuristic and WRAPPED rather than called directly: raised on its own it
    names `access.secret_refs`, a field a live binding does not have, and an
    operator reading that message goes looking in the wrong block. The check
    is kept; the field name is corrected.
    """
    from okg.substrate.external_backed.handle import (
        ExternalHandleError,
        _validate_secret_refs,
    )

    raw = tuple(getattr(binding, "secret_refs", ()) or ())
    if not raw:
        return None
    try:
        refs = _validate_secret_refs(f"live_bindings.{relation}", raw)
    except ExternalHandleError:
        # NEITHER the wrapped message NOR the offending entry is passed on.
        # The message names `access.secret_refs`, a field a live binding does
        # not have, which is the reason for the wrapper; and the entry it
        # quotes is, on the inline-credential path, THE CREDENTIAL ITSELF.
        # Repeating either would undo one of the two things this wrapper is
        # for.
        #
        # `from None` IS PART OF THAT, and scrubbing the message alone was
        # not enough. Without it the wrapped `ExternalHandleError` stays on
        # `__context__`, and Python prints a chained exception in full --
        # "During handling of the above exception..." -- so the credential
        # reappears in any formatted traceback, which is exactly what a
        # failed build writes to the log.
        raise _secret_refs_error(
            relation,
            "is not a usable secret reference. It must be a single "
            "`env:NAME` entry naming an environment variable -- never an "
            "inline `KEY=VALUE` credential and never the value itself",
        ) from None
    if len(refs) != 1:
        _refuse_secret_refs(
            relation,
            f"must declare exactly one reference, because the binding sends "
            f"exactly one header; got {len(refs)}",
        )
    ref = refs[0]
    if not ref.startswith("env:") or not ref[4:].strip():
        # The ENTRY IS NOT ECHOED, and this is the whole point of the branch.
        # `handle._validate_secret_refs` only rejects an entry containing
        # `=`, so a bare token -- `sk-live-...`, `ghp_...` -- sails through it
        # and arrives here. An operator who pasted the VALUE where the
        # reference goes has made exactly the mistake that puts a live
        # credential in this string, and quoting it back would write it into
        # the message, into `data["reason"]`, and from there into every log
        # and error envelope that carries a failed build.
        _refuse_secret_refs(
            relation,
            "is not an `env:NAME` reference. `env:` is the only scheme: a "
            "second scheme would be a second place a credential can live. "
            "The offending entry is deliberately not repeated here, because "
            "the usual cause of this refusal is pasting the secret VALUE "
            "where its reference name belongs",
        )
    name = ref[4:].strip()
    value = os.environ.get(name)
    if not value:
        raise LiveAdapterSecretMissing(
            f"live binding {relation!r}: secret reference {ref!r} is not set "
            f"in this process's environment, so the binding cannot "
            f"authenticate and is refused at load rather than on an agent's "
            f"first call.",
            # The REFERENCE, never the value. There is no value here, which
            # is the problem -- but the rule holds on every path, so there is
            # no path where it might not.
            data={"relation": relation, "secret_ref": ref},
            fix_hint=(
                f"set {name} in the server process's environment, or remove "
                f"`auth_header` and `secret_refs` from the binding"
            ),
        )
    return value


# ----------------------------------------------------------------- errors

def _adapter_error(failure: str, **kwargs: Any) -> Exception:
    from okg.substrate.external_backed.live_dispatch import LiveAdapterError

    return LiveAdapterError(failure, **kwargs)


#: Every `HttpGuardRefusal.reason` this adapter can see, mapped to the
#: failure class the dispatcher knows. EXHAUSTIVE on purpose: the dispatcher
#: has a backstop that catches everything and reports `adapter_raised` with
#: no message, and that backstop is correct -- but relying on it for a
#: FORESEEABLE condition is how a real failure gets one generic label.
_GUARD_FAILURES: Mapping[str, str] = {
    "deadline_exceeded": "timeout",
    "response_over_cap": "response_over_cap",
    # A nesting bomb small enough to pass the byte cap (#2170). Depth is a
    # size, so it gets the size refusal, as the operator docs already say.
    "body_too_deep": "response_over_cap",
    "streaming_unavailable": "response_unusable",
    "body_not_json": "response_unusable",
    "body_not_utf8": "response_unusable",
    # The address refusals are UNREACHABLE after a successful load, because
    # the same checks ran there against the same values. Mapped anyway, and a
    # test asserts the load really did make them unreachable.
    "scheme": "upstream_unavailable",
    "userinfo": "upstream_unavailable",
    "url_has_path": "upstream_unavailable",
    "host_not_literal": "upstream_unavailable",
    "ipv6_unbracketed": "upstream_unavailable",
    "bad_port": "upstream_unavailable",
    "host_not_allowed": "upstream_unavailable",
    "non_global": "upstream_unavailable",
}


def _requests_failure(exc: BaseException) -> str | None:
    """The declared failure class for a `requests` exception, or None.

    None means "not one of ours", and the caller lets it reach the
    dispatcher's backstop rather than inventing a class for it.
    """
    import requests

    if isinstance(exc, requests.TooManyRedirects):
        # Should be unreachable with `allow_redirects=False`. Mapped anyway,
        # because "should be unreachable" is a belief, not a guard.
        return "redirect_refused"
    if isinstance(exc, requests.exceptions.Timeout):
        return "timeout"
    if isinstance(exc, requests.exceptions.ContentDecodingError):
        return "response_unusable"
    if isinstance(exc, requests.exceptions.ChunkedEncodingError):
        # A truncated body is not a smaller answer.
        return "response_unusable"
    if isinstance(exc, requests.exceptions.SSLError):
        return "upstream_unavailable"
    if isinstance(exc, (
        requests.exceptions.InvalidURL,
        requests.exceptions.InvalidSchema,
        requests.exceptions.MissingSchema,
    )):
        return "upstream_unavailable"
    if isinstance(exc, requests.exceptions.ConnectionError):
        return "upstream_unavailable"
    if isinstance(exc, requests.exceptions.RequestException):
        return "upstream_unavailable"
    return None


# ----------------------------------------------------------------- runner

def _extract_rows(parsed: Any, config: HttpBindingConfig) -> list[Any]:
    """Follow `rows_path` into the parsed body, or refuse.

    Absent, or present and not a list, is an ERROR and never an empty result.
    "The shape changed" must not read as "no rows": an agent that is told
    zero rows stops looking, and an operator never hears that the upstream
    response stopped matching the binding.

    An EMPTY path is the root (`rows_path: "."`): the body itself must be the
    list. The loop below simply does not run, and the same "not a list"
    refusal then applies to the document -- so an endpoint that switches from
    an array to an object is refused rather than read as empty.
    """
    shown = ".".join(config.rows_path) if config.rows_path else ROWS_PATH_ROOT
    node: Any = parsed
    for token in config.rows_path:
        if not isinstance(node, Mapping) or token not in node:
            raise _adapter_error(
                "response_unusable",
                excerpt=(
                    f"the response has no `{shown}`; the upstream shape does "
                    f"not match this binding"
                ),
            )
        node = node[token]
    if not isinstance(node, list):
        raise _adapter_error(
            "response_unusable",
            excerpt=(
                f"`{shown}` is a {type(node).__name__}, not a list of rows"
            ),
        )
    return node


def _project_rows(rows: list[Any], config: HttpBindingConfig) -> list[Any]:
    """Keep only `row_fields` in each row, or refuse.

    A row that is not an object, or lacks a named key, is `response_unusable`
    rather than a row with a hole: a renamed upstream field must be heard
    about, not read as a process with no name.
    """
    if config.row_fields is None:
        return rows
    projected: list[Any] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            raise _adapter_error(
                "response_unusable",
                excerpt=(
                    f"row {index} is a {type(row).__name__}, not an object, "
                    f"so `row_fields` cannot select from it"
                ),
            )
        missing = [f for f in config.row_fields if f not in row]
        if missing:
            raise _adapter_error(
                "response_unusable",
                excerpt=(
                    f"row {index} has no {', '.join(f'`{m}`' for m in missing)}; "
                    f"the upstream shape does not match this binding"
                ),
            )
        projected.append({f: row[f] for f in config.row_fields})
    return projected


def _close_quietly(response: Any) -> None:
    """Break the connection from the watchdog thread, and never raise.

    `response.close()` ALONE IS NOT ENOUGH, and this is the whole reason the
    watchdog exists. It returns the connection to the pool and marks the
    response consumed, but it does not interrupt a read already blocked in
    `recv`, so a drip held the thread for 15.1s against a 2s deadline with
    only `close()` here.

    `socket.shutdown()` does interrupt it: the blocked `recv` returns at
    once, `iter_content` raises, and the thread unwinds -- which is what
    gives the dispatcher its concurrency slot back. Measured at 2.0s for the
    same drip.

    The reading thread may be closing the same object at the same moment and
    the socket may already be gone, so every step is guarded. An exception
    raised here would surface on a timer thread where nothing can act on it.
    """
    import socket as _socket

    try:
        connection = getattr(response.raw, "_connection", None)
        sock = getattr(connection, "sock", None) if connection else None
        if sock is not None:
            sock.shutdown(_socket.SHUT_RDWR)
    except Exception:
        # Best effort by design: the socket may already be closed by the
        # reading thread, and this runs on a timer thread where a raise
        # would reach no caller. `response.close()` below still runs.
        log.debug("live http: socket shutdown failed", exc_info=True)
    try:
        response.close()
    except Exception:
        # Same reason: teardown of a response another thread may already
        # have closed. Nothing downstream depends on this call succeeding.
        log.debug("live http: response close failed", exc_info=True)


def _read_excerpt(response: Any, deadline: float) -> bytes:
    """Read at most one excerpt's worth of an error body, then stop.

    Deliberately NOT `read_bounded_body`: that one refuses when the body
    exceeds its cap, which is right for a result and wrong for an excerpt.
    Here the body is already being thrown away except for its first bytes,
    so going over the length is the expected case, not a failure.
    """
    iter_content = getattr(response, "iter_content", None)
    if not callable(iter_content):
        return b""
    chunks: list[bytes] = []
    total = 0
    try:
        for chunk in iter_content(ERROR_EXCERPT_BYTES):
            if time.monotonic() >= deadline:
                break
            if not chunk:
                continue
            chunks.append(chunk)
            total += len(chunk)
            if total >= ERROR_EXCERPT_BYTES:
                break
    except Exception:
        # An error body that itself fails to arrive changes nothing: the
        # status already said what went wrong.
        pass
    return b"".join(chunks)[:ERROR_EXCERPT_BYTES]


def _excerpt_of(raw: bytes) -> str:
    """Decode, bound and redact an upstream error body.

    `redact_userinfo`, NOT `redact_url`. `redact_url` parses its argument AS
    a URL and answers `<unparsable url>` for anything that is not one -- so
    an excerpt that merely CONTAINS a URL was replaced wholesale by that
    marker, and the upstream's actual reason, the only reason the excerpt
    exists, was thrown away. `redact_userinfo` is the documented tool for a
    URL embedded in a larger string: it strips `user:password@` from every
    URL in the text and leaves the rest of the sentence intact.
    """
    text = raw[:ERROR_EXCERPT_BYTES].decode("utf-8", errors="replace")
    return redact_userinfo(text)


@dataclass(frozen=True)
class _UpstreamResponse:
    """Everything the runner may know about an upstream response. Nothing more.

    NOT the `requests.Response`. The runner is handed this view by
    :func:`_guarded_response` and has no other way to reach the body: the
    two readers are closures created while the watchdog is armed, and the
    raw response object is not an attribute of anything the runner holds.
    A future read path therefore cannot be written outside the watchdog's
    window by accident -- there is nothing outside the window to read from.
    `test_http_adapter_structure.py` pins that no other code in this module
    touches a response.
    """

    status: int
    content_type: str
    #: The result body, under the byte cap AND the deadline. A read the
    #: watchdog cut short raises `deadline_exceeded` rather than returning a
    #: truncated body that would parse as a smaller answer.
    read_body: Callable[[int], bytes]
    #: At most `ERROR_EXCERPT_BYTES` of an error body; never raises.
    read_excerpt: Callable[[], bytes]


@contextlib.contextmanager
def _guarded_response(
    session: Any,
    config: HttpBindingConfig,
    *,
    payload: bytes | None,
    headers: Mapping[str, str],
    deadline: float,
) -> Iterator[_UpstreamResponse]:
    """Send the request and yield the ONLY view of its response (#2154).

    The watchdog is armed before anything below this function can read a
    byte, and cancelled, with the response closed, on every exit. Three
    defects were each found by measurement because this used to be spread
    across the runner: the success-path body read outside the window (15.1s
    against a 2s deadline), then the non-2xx excerpt read (26.4s). Both
    reads now exist only as closures built in here, so neither can move
    back outside the window.

    The header phase -- inside `session.request` -- is bounded by the
    per-read socket timeout derived from the budget, not by the watchdog,
    which needs a response to close. That is unchanged by this function.
    """
    budget = remaining_budget(deadline)
    response = session.request(
        config.method,
        config.url,
        data=payload,
        headers=dict(headers),
        # Connect AND read timeouts derived from the REMAINING BUDGET, never
        # a fixed constant. A fixed read timeout leaves a silent-after-headers
        # server unbounded for a long deadline; the between-chunk check
        # cannot help there, because it only fires when a chunk arrives.
        timeout=(budget, budget),
        # A redirect is how a credentialed request gets re-sent somewhere no
        # allowlist covers.
        allow_redirects=False,
        stream=True,
    )
    watchdog: threading.Timer | None = None
    try:
        # ARMED FIRST, before the status or headers are even looked at,
        # because everything after this line can block on a socket the
        # upstream controls. `remaining_budget` refuses a spent budget here,
        # and the `finally` still closes the response.
        #
        # `iter_content` fills its buffer before it yields, so a drip of
        # 4 KiB a second into a 64 KiB chunk holds the thread inside ONE
        # read, and the per-read socket timeout never fires because every
        # write resets it. Only closing the socket from another thread
        # interrupts that read, which is what `_close_quietly` does.
        watchdog = threading.Timer(
            remaining_budget(deadline), _close_quietly, args=(response,),
        )
        watchdog.daemon = True
        watchdog.start()

        def read_body(max_bytes: int) -> bytes:
            raw = read_bounded_body(
                response, max_bytes=max_bytes, deadline=deadline,
                chunk_size=CHUNK_BYTES,
            )
            # The watchdog closes the socket mid-body, so the read can come
            # back SHORT rather than raising. That is the timeout it is,
            # never a truncated body parsed as a smaller answer.
            if time.monotonic() >= deadline:
                raise HttpGuardRefusal(
                    "deadline_exceeded",
                    "the body did not arrive within the time budget",
                )
            return raw

        def read_excerpt() -> bytes:
            return _read_excerpt(response, deadline)

        yield _UpstreamResponse(
            status=int(response.status_code),
            content_type=str(response.headers.get("Content-Type", "")),
            read_body=read_body,
            read_excerpt=read_excerpt,
        )
    finally:
        # On EVERY exit -- success, refusal, or an exception from the
        # socket. A timer left armed would fire into a closed response
        # later; `_close_quietly` tolerates that, but a pending timer per
        # in-flight call is a leak worth not having.
        if watchdog is not None:
            watchdog.cancel()
        response.close()


def run_http_relation_for(
    config: HttpBindingConfig, secret: str | None,
) -> Callable[..., list[Any]]:
    """Build the per-relation runner, closing over the resolved secret.

    A CLOSURE, not a module function taking the secret as an argument. The
    dispatcher calls the runner with `config=dict(binding.config)` and that
    mapping is copied, logged on some paths and reachable from a traceback's
    `repr()`. A closure cell is none of those things, so the value cannot
    travel with the binding.

    The returned callable takes exactly the four keywords
    `_run_adapter_under_deadline` passes and nothing else. `node_id` is
    accepted and UNUSED: the relation is a row query with declared
    arguments, the node is already the gate's resource and is recorded on the
    sample, and folding it into the request would be a fifth untyped input
    channel -- the thing the slot grammar exists to prevent.
    """

    def run_http_relation(
        *,
        config_: Mapping[str, Any] | None = None,
        args: Mapping[str, Any],
        node_id: str,
        deadline_seconds: float,
        **_ignored: Any,
    ) -> list[Any]:
        import requests

        # The budget this call is actually held to, tracked in THIS thread.
        # The dispatcher enforces the same number by abandoning the future,
        # but an abandoned thread keeps its concurrency slot until its I/O
        # returns, so the bound that matters is the one enforced here.
        deadline = time.monotonic() + float(deadline_seconds)

        # Re-check the address. Unreachable after a successful load, because
        # the same guard ran there over the same values -- asserted rather
        # than assumed, because the cost is one comparison and the failure
        # mode is an unchecked egress.
        try:
            assert_host_allowed(
                config.address, config.port, config.allowed,
                allow_non_global=config.allow_non_global,
            )
        except HttpGuardRefusal as exc:
            raise _adapter_error(
                _GUARD_FAILURES.get(exc.reason, "upstream_unavailable"),
            ) from None

        headers = {"Accept": "application/json"}
        payload: bytes | None = None
        if config.method == "POST":
            rendered = _substitute(config.body, args)
            payload = json.dumps(rendered, separators=(",", ":")).encode("utf-8")
            headers["Content-Type"] = config.content_type
        if config.auth_header and secret:
            # The ONE header a binding may set, to the ONE value it resolved
            # at load. No argument, and no part of an argument, reaches here.
            headers[config.auth_header] = secret

        session = requests.Session()
        # Proxies, and REQUESTS_CA_BUNDLE, are ignored. `HTTP_PROXY` must not
        # silently redirect a gated live call through a host no allowlist
        # ever saw. The CA bundle sharing this flag is the surprise, and the
        # docs page says so: a private CA goes in the system trust store.
        session.trust_env = False
        try:
            # The body is reachable ONLY through `upstream`, and only while
            # `_guarded_response`'s watchdog is armed (#2154).
            with _guarded_response(
                session, config,
                payload=payload, headers=headers, deadline=deadline,
            ) as upstream:
                status = upstream.status
                if 300 <= status < 400:
                    # The `Location` target is never requested or echoed.
                    raise _adapter_error("redirect_refused", status=status)
                if not 200 <= status < 300:
                    # The excerpt is a COURTESY, and must never change what
                    # the failure is called. Reading it under the ordinary
                    # cap made a long error page report `response_over_cap`,
                    # which says the answer was too big when what happened
                    # is that the upstream said no. So an over-long error
                    # body is TRUNCATED -- the one place truncation is
                    # right, because an excerpt is already a fragment -- and
                    # the class stays `upstream_unavailable`.
                    raise _adapter_error(
                        "upstream_unavailable",
                        status=status,
                        excerpt=_excerpt_of(upstream.read_excerpt()),
                    )
                media = upstream.content_type
                if not _is_json_media(media):
                    raise _adapter_error(
                        "response_unusable",
                        status=status,
                        excerpt=(
                            f"the response content type is {media!r}, which "
                            f"is not JSON. There is no text, XML or HTML "
                            f"fallback"
                        ),
                    )
                raw = upstream.read_body(config.max_response_bytes)
            # Parsed after the connection is closed: parsing is CPU, not I/O,
            # and holds no socket.
            parsed = parse_json_strict(raw)
        except HttpGuardRefusal as exc:
            raise _adapter_error(
                _GUARD_FAILURES.get(exc.reason, "response_unusable"),
                excerpt=exc.detail,
            ) from None
        except Exception as exc:
            # DEADLINE FIRST, and the order is the point. When the watchdog
            # breaks the socket the read raises whatever the transport makes
            # of a connection that vanished -- a ChunkedEncodingError for a
            # drip, a ConnectionError for a server that went quiet. Mapping
            # those by type would report `response_unusable` or
            # `upstream_unavailable` for a call that simply ran out of time,
            # which tells an operator to go and look at a healthy upstream.
            # What happened is the budget expired, so that is what it is
            # called.
            if time.monotonic() >= deadline:
                raise _adapter_error("timeout") from None
            failure = _requests_failure(exc)
            if failure is None:
                # Not a foreseeable condition. Let the dispatcher's backstop
                # have it: it logs the TYPE NAME only and returns
                # `adapter_raised` with no message text, which is the right
                # treatment for an exception nobody predicted.
                raise
            # `str(exc)` is never passed on: a requests exception's message
            # routinely carries the URL it was dialling, userinfo and all.
            raise _adapter_error(failure) from None
        finally:
            # Per call, closed here, so no connection survives into a pool to
            # be inherited by a later call under a different principal.
            session.close()

        from okg.substrate.external_backed.live_dispatch import (
            _assert_result_is_shapeable,
        )

        rows = _extract_rows(parsed, config)
        # By COUNT, before the envelope's 16 KiB inline cap, so a
        # 10,000-row answer is cut by rows rather than degraded to a digest.
        selected = _project_rows(list(rows[: config.max_rows]), config)
        # THE NODE CAP IS CHARGED ON WHAT THIS RETURNS, not on the document
        # it came from. Charging the whole parsed body made `max_rows`
        # useless against a wide endpoint: a Glances `/api/4/all` response
        # measured 64,612 nodes, over the 50,000 cap, so even `max_rows: 12`
        # failed -- and whether it failed depended on how busy the machine
        # was that second, which is the worst kind of intermittent.
        #
        # Nothing is lost by moving it. The cap exists to bound what gets
        # FENCED and serialised, and that is exactly this slice: the
        # dispatcher charges the same cap on the adapter's return value
        # before it fences it. A depth bomb sitting outside the extracted
        # rows is never walked by anything.
        #
        # The decoded-byte cap bounds MEMORY, and only memory. Parse DEPTH
        # is bounded separately, before parsing, by `parse_json_strict`'s
        # `MAX_JSON_DEPTH`: roughly 20,000 nested brackets is about 39 KB,
        # well inside any byte cap, and used to escape `json.loads` as a
        # RecursionError labelled `adapter_raised` (#2170). It is now
        # `body_too_deep`, reported as `response_over_cap`.
        try:
            _assert_result_is_shapeable(selected)
        except Exception:
            # Rows under the byte cap can still be a depth bomb: 20,000
            # nested lists is a few kilobytes.
            raise _adapter_error("response_over_cap") from None
        return selected

    # The dispatcher passes `config=`; the closure's own parsed config is the
    # one it trusts. Accepting and discarding the keyword keeps the call
    # signature exactly what `_run_adapter_under_deadline` submits.
    def _entry(*, config: Mapping[str, Any], **kwargs: Any) -> list[Any]:
        return run_http_relation(config_=config, **kwargs)

    _entry.__name__ = f"run_http_relation[{config.relation}]"
    return _entry


# -------------------------------------------------------- the load hook

def validate_live_binding_configs(
    bindings: Mapping[str, Any],
    relations: Mapping[str, Any],
    *,
    runners: Mapping[str, Any],
) -> dict[str, Callable[..., Any]]:
    """Check every binding's config at LOAD and return its bound runner.

    Returns relation -> runner. A relation whose kind has a caller-supplied
    runner is left alone: the `live_adapters` parameter is a test seam and
    overrides the built-in, so a test with its own fake `http` runner is not
    made to satisfy this parser.

    Every refusal in this module fires HERE, naming the relation and the
    field. A deployment whose binding can never produce a legal request does
    not start.
    """
    bound: dict[str, Callable[..., Any]] = {}
    for name in sorted(bindings):
        binding = bindings[name]
        if getattr(binding, "kind", None) != "http":
            continue
        if runners.get("http") is not None:
            continue
        declared = relations.get(name)
        if declared is None:
            # `assert_live_bindings_are_declared` runs first and refuses this
            # by name. Reaching here means the caller skipped it, and
            # guessing an empty declaration would accept a body whose slots
            # were never checked against anything.
            raise LiveAdapterConfigInvalid(
                f"live binding {name!r} has no compiled declaration to check "
                f"its config against.",
                data={"relation": name, "field": "",
                      "reason": "no declaration"},
                fix_hint="recompose the catalog and rebuild the server",
            )
        config = parse_http_binding(name, binding, declared)
        secret = _resolve_secret(name, binding)
        bound[name] = run_http_relation_for(config, secret)
    return bound


def _unbound_http_runner(**_kwargs: Any) -> list[Any]:
    """The kind-level `http` entry, which must never actually run.

    The registry is keyed by KIND, because that is what
    `assert_live_bindings_are_declared` checks and what the load refusal
    reports. The runner a call needs is keyed by RELATION, because its secret
    and its parsed config are the relation's, and is installed separately by
    `_build_server`.

    Reaching this function means the per-relation wiring was skipped while
    the kind check passed. That is a defect, and it fails LOUDLY here rather
    than dialling anything: a fallback that "worked" by re-parsing the config
    per call would drop the load-time checks and lose the closure the secret
    lives in.

    It carries its OWN excerpt rather than reading as a plain
    `upstream_unavailable`, because nothing was dialled and there is no
    upstream to be unavailable. An operator told the external system was
    unreachable would go and inspect a system that is perfectly healthy; the
    fault is in this process.
    """
    raise _adapter_error(
        "upstream_unavailable",
        excerpt=(
            "per-relation runner not installed: the adapter kind is "
            "registered but this relation's bound runner is missing, so no "
            "request was attempted. This is a wiring defect in the server "
            "build, not a fault of the external system"
        ),
    )


#: Registered at import. `_build_server` imports this module explicitly, so
#: the registration is an ordinary import and not import-order magic.
def register() -> None:
    from okg.substrate.external_backed.live_dispatch import (
        BUILTIN_LIVE_ADAPTERS,
    )

    BUILTIN_LIVE_ADAPTERS["http"] = _unbound_http_runner


register()
