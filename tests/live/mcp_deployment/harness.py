"""Prove live relations and multi-user access on a real deployment (#3413).

PACT ``live-proof-3413``. This harness drives a deployment's running
``okg mcp-serve --transport streamable-http`` endpoint over the network with
the MCP SDK's own client, the way an agent or a chat front end reaches it,
and writes one JSON evidence file per run. It never touches the database,
the deployment directory, or the server process: what it proves is what a
caller can observe.

What one run checks
-------------------
``transport``
    A wrong bearer token and a missing one are refused with HTTP 401; the
    configured token is accepted (HTTP 200 on ``initialize``). In the
    ``after-rotation`` phase the OLD token must now get 401 too.
``live``
    Each live-relation call the expectations file lists, made through
    ``expand(live={relation, args})``: its state, row count, row keys, the
    freshness fields (``observed_at`` age, ``pinned_generation_id``), and the
    ``inspect`` declaration of the relation on the node. Then the listed
    refusals (a bad argument pattern, an unknown argument key, an enum value
    out of range, a relation the subtype does not declare), each with its
    expected code and no rows. At least one call also runs as principal B
    (``b_expect_state``) when the multi-user section is configured.
``multi_user`` (delegated-header mode: ``--principal-header`` with
``--principal-a`` and ``--principal-b``)
    The same calls as principal A and principal B: the answers differ as the
    policy says, and B is shown at least one node A also gets; B is refused,
    or told the node does not exist, where A is served, with an explicit
    expected code (``refusal.b_expect_code``; a node B's policy denies
    usually reads as ``node_not_found``, so a denied node does not reveal
    that it exists); B sees a masked value A sees in clear; a call with no
    principal header (and one naming an unbound principal) is refused
    ``nomos_principal_unidentified``; and no value visible only to A appears
    anywhere in any of B's responses (the whole serialised JSON is scanned,
    not chosen fields).
``concurrency``
    A and B calls interleaved in parallel (default 2 x 20); every single
    response is checked: each A answer is A's, and no B answer carries an
    A-only value.

Restart and token rotation are operator steps. Run the harness once per
phase with ``--phase baseline``, ``after-restart`` and ``after-rotation``;
``--baseline-evidence`` makes a later phase also check that the differential
answer is the same as the baseline's. The harness cannot see a restart, so
``--restart-declared-at`` (required for ``after-restart``, optional for
``after-rotation``) records when the operator says it happened; the run
checks that time falls after the baseline run and before this one.

What the evidence never holds
-----------------------------
No token value, no principal name, no masked value and no A-only value is
written in clear: each appears only as a truncated sha256 (``_digest``).
Principal names are hashed with a random salt drawn per run and never
written, so a short or guessable name cannot be recovered by hashing
candidates; their labels therefore differ between runs. The answer digests
(``differential_digest``) are unsalted so phases stay comparable. Denied node
ids are hashed too. Row CONTENTS from live relations are not written either;
their count, keys and a digest are. The evidence names the harness that
produced it: the sha256 of ``harness.py``, the git head, and whether the
harness files differ from that head.

Running it
----------
``--help`` lists every flag. Every flag has an ``OKG_LIVE_MCP_*`` environment
default, so the pytest wrapper in ``test_deployment_proof.py`` and the command
line take the same inputs::

    OKG_MCP_TOKEN=... .venv/bin/python -m tests.live.mcp_deployment.harness \\
        --url http://127.0.0.1:8765/mcp --token-env OKG_MCP_TOKEN \\
        --principal-header X-OKG-Principal \\
        --principal-a alice@example.edu --principal-b bob@example.edu \\
        --expectations expectations.json --phase baseline \\
        --out evidence-baseline.json

Exit status: 0 every check passed, 1 a check failed, 2 the inputs were
unusable (no check ran).
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as _dt
import hashlib
import json
import os
import secrets
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import urlsplit

#: Mirrors ``okg.substrate.mcp.result_text.RESULT_FORMAT_META_KEY``. Imported
#: when okg is importable so a rename fails loudly here, not as text results.
try:  # pragma: no cover - exercised by whichever environment runs it
    from okg.substrate.mcp.result_text import RESULT_FORMAT_META_KEY
except ImportError:  # pragma: no cover - harness run outside the repo venv
    RESULT_FORMAT_META_KEY = "okg/result_format"

#: Mirrors ``okg.substrate.nomos.read_masking.MASK_TOKEN`` / ``REDACTION_MARKER``.
try:  # pragma: no cover
    from okg.substrate.nomos.read_masking import MASK_TOKEN, REDACTION_MARKER
except ImportError:  # pragma: no cover
    MASK_TOKEN = "[nomos:masked]"
    REDACTION_MARKER = "nomos_redacted"

EVIDENCE_SCHEMA = "okg.live_mcp_proof.evidence.v1"
EXPECTATIONS_SCHEMA = "okg.live_mcp_proof.expectations.v1"
PHASES = ("baseline", "after-restart", "after-rotation")
UNIDENTIFIED_CODE = "nomos_principal_unidentified"

#: A-only strings shorter than this are not scanned for: a three-letter value
#: like "gpu" appears in legitimate B answers by coincidence, and a scan that
#: fires on coincidences gets switched off. The floor is recorded in evidence.
MIN_LEAK_TOKEN_LENGTH = 6

_JSON_META = {RESULT_FORMAT_META_KEY: "json"}
_MISSING = object()


class HarnessInputError(Exception):
    """The inputs cannot drive a run; no check was attempted."""


def _digest(value: Any) -> str:
    """A short, one-way label for a value that must not appear in clear."""
    text = value if isinstance(value, str) else json.dumps(value, sort_keys=True, default=str)
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _salted_digest(value: str, salt: bytes) -> str:
    """Like :func:`_digest`, keyed by a per-run salt that is never written."""
    return "salted-sha256:" + hashlib.sha256(salt + value.encode("utf-8")).hexdigest()[:16]


def _now() -> _dt.datetime:
    return _dt.datetime.now(_dt.timezone.utc)


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Target:
    """Where and as whom to call. ``repr`` never shows a token."""

    url: str
    token: str = field(repr=False)
    token_env: str
    principal_header: str | None = None
    principal_a: str | None = field(default=None, repr=False)
    principal_b: str | None = field(default=None, repr=False)
    old_token: str | None = field(default=None, repr=False)
    old_token_env: str | None = None

    @property
    def multi_user(self) -> bool:
        return bool(self.principal_header and self.principal_a and self.principal_b)


@dataclass
class CallResult:
    """One tool call's observable outcome."""

    payload: dict[str, Any] | None
    is_error: bool = False
    transport_error: str | None = None
    server_info: dict[str, Any] | None = None
    #: The strings the caller itself sent. A refusal may echo the node id it
    #: was asked about; that is the caller's own input, not an A-only value.
    sent: frozenset[str] = frozenset()

    @property
    def serialised(self) -> str:
        return json.dumps(self.payload, sort_keys=True, ensure_ascii=False, default=str)


@dataclass
class Check:
    section: str
    name: str
    passed: bool
    detail: dict[str, Any] = field(default_factory=dict)

    def as_json(self) -> dict[str, Any]:
        return {
            "section": self.section,
            "name": self.name,
            "passed": self.passed,
            "detail": self.detail,
        }


class Recorder:
    def __init__(self) -> None:
        self.checks: list[Check] = []

    def add(self, section: str, name: str, passed: bool, **detail: Any) -> bool:
        self.checks.append(Check(section, name, bool(passed), detail))
        return bool(passed)

    @property
    def ok(self) -> bool:
        return bool(self.checks) and all(check.passed for check in self.checks)


def load_expectations(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HarnessInputError(f"expectations file {path} is unreadable: {exc}") from exc
    if not isinstance(data, dict) or data.get("schema") != EXPECTATIONS_SCHEMA:
        raise HarnessInputError(
            f"expectations file {path} must be a JSON object with "
            f"schema={EXPECTATIONS_SCHEMA!r}"
        )
    validate_expectations(data)
    return data


def validate_expectations(data: Mapping[str, Any]) -> None:
    """Refuse expectations under which a check could pass vacuously."""
    live = data.get("live")
    if not isinstance(live, dict) or not live.get("calls"):
        raise HarnessInputError("expectations.live.calls must list at least one call")
    if not live.get("refusals"):
        raise HarnessInputError(
            "expectations.live.refusals must list at least one refusal: a run "
            "that records no refusal proves nothing about the bounds"
        )
    spec = data.get("multi_user")
    if not spec:
        return
    for call in spec.get("calls") or []:
        name = call.get("name") or call.get("tool")
        if not (call.get("b") or {}).get("node_ids_include"):
            raise HarnessInputError(
                f"expectations.multi_user.calls[{name}].b.node_ids_include must name at "
                "least one node B is served: a B answer that is simply empty would "
                "otherwise pass as 'differentiated'"
            )
    refusal = spec.get("refusal")
    if refusal is not None:
        code = refusal.get("b_expect_code")
        if not isinstance(code, str) or not code:
            raise HarnessInputError(
                "expectations.multi_user.refusal.b_expect_code must be an explicit "
                "code (a node B is denied usually reads as 'node_not_found'); a null "
                "code would accept any refusal, including an unrelated failure"
            )
    if not any("b_expect_state" in call for call in live["calls"]):
        raise HarnessInputError(
            "expectations.live.calls: at least one call must set b_expect_state, so "
            "principal B also calls a live relation"
        )


# ---------------------------------------------------------------------------
# The wire
# ---------------------------------------------------------------------------


def _headers(target: Target, principal: str | None, token: str | None) -> dict[str, str]:
    headers: dict[str, str] = {}
    if token is not None:
        headers["Authorization"] = f"Bearer {token}"
    if principal is not None and target.principal_header:
        headers[target.principal_header] = principal
    return headers


def _decode(result: Any) -> dict[str, Any]:
    """The JSON payload of an okg tool result (one text block, issue #3059)."""
    content = getattr(result, "content", None) or []
    if not content or not hasattr(content[0], "text"):
        raise ValueError("result carried no text content block")
    decoded = json.loads(content[0].text)
    if not isinstance(decoded, dict):
        raise ValueError("result text is JSON but not an object")
    return decoded


def _exception_text(exc: BaseException) -> str:
    """Flatten an ExceptionGroup to its leaves; never includes request headers."""
    leaves: list[BaseException] = []

    def walk(item: BaseException) -> None:
        inner = getattr(item, "exceptions", None)
        if inner:
            for child in inner:
                walk(child)
        else:
            leaves.append(item)

    walk(exc)
    return "; ".join(f"{type(leaf).__name__}: {str(leaf)[:200]}" for leaf in leaves)


async def call_tool(
    target: Target,
    principal: str | None,
    tool: str,
    arguments: Mapping[str, Any],
    *,
    token: str | None = _MISSING,  # type: ignore[assignment]
    timeout: float = 60.0,
) -> CallResult:
    """One tool call in its own MCP session, as one user's client makes it."""
    from mcp import ClientSession
    from mcp.client.streamable_http import streamablehttp_client

    use_token = target.token if token is _MISSING else token
    sent = frozenset(string_leaves(dict(arguments)))
    try:
        async with streamablehttp_client(
            target.url, headers=_headers(target, principal, use_token), timeout=timeout
        ) as (read, write, _):
            async with ClientSession(read, write) as session:
                init = await session.initialize()
                result = await session.call_tool(tool, dict(arguments), meta=_JSON_META)
    except Exception as exc:  # noqa: BLE001 - recorded as the call's outcome
        return CallResult(payload=None, transport_error=_exception_text(exc), sent=sent)
    info = getattr(init, "serverInfo", None)
    server_info = (
        {"name": getattr(info, "name", None), "version": getattr(info, "version", None)}
        if info is not None
        else None
    )
    try:
        payload = _decode(result)
    except (ValueError, json.JSONDecodeError) as exc:
        return CallResult(
            payload=None,
            is_error=bool(getattr(result, "isError", False)),
            transport_error=f"undecodable result: {exc}",
            server_info=server_info,
            sent=sent,
        )
    return CallResult(
        payload=payload,
        is_error=bool(getattr(result, "isError", False)),
        server_info=server_info,
        sent=sent,
    )


async def raw_initialize_status(
    target: Target, token: str | None, principal: str | None
) -> int:
    """HTTP status of a bare ``initialize`` POST, before any session exists."""
    import httpx

    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "okg-live-proof", "version": "1"},
        },
    }
    headers = {
        "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json",
        **_headers(target, principal, token),
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.post(target.url, json=body, headers=headers)
        return int(response.status_code)


# ---------------------------------------------------------------------------
# Reading payloads
# ---------------------------------------------------------------------------


def _walk(value: Any) -> Iterable[Any]:
    yield value
    if isinstance(value, dict):
        for item in value.values():
            yield from _walk(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk(item)


def node_ids(payload: Any) -> set[str]:
    """Every ``node_id`` anywhere in a payload."""
    return {
        str(item["node_id"])
        for item in _walk(payload)
        if isinstance(item, dict) and isinstance(item.get("node_id"), (str, int))
    }


def string_leaves(value: Any) -> set[str]:
    return {item for item in _walk(value) if isinstance(item, str)}


def records_for(payload: Any, node_id: str) -> list[dict[str, Any]]:
    return [
        item
        for item in _walk(payload)
        if isinstance(item, dict) and str(item.get("node_id")) == node_id
    ]


def refusal_code(payload: Mapping[str, Any] | None) -> str | None:
    """The tool-level refusal code, or None when the call was served."""
    if not isinstance(payload, Mapping):
        return None
    error = payload.get("error")
    if isinstance(error, Mapping) and error.get("code"):
        return str(error["code"])
    if payload.get("status") == "refused":
        return "refused"
    return None


def attribute_value(record: Mapping[str, Any], attribute: str) -> Any:
    attrs = record.get("attrs")
    if isinstance(attrs, Mapping) and attribute in attrs:
        return attrs[attribute]
    return record.get(attribute, _MISSING)


def is_masked(record: Mapping[str, Any], attribute: str, *, a_has_attribute: bool) -> bool:
    """The attribute is withheld: the mask token, or dropped under the marker.

    A dropped attribute counts as masked only when the redaction marker names
    it (a marker holding attribute names), or -- the okg marker is a bare
    ``True`` -- when A's copy of the same record carries the attribute, so the
    absence is a withholding rather than an attribute the record never had.
    """
    value = attribute_value(record, attribute)
    if value == MASK_TOKEN:
        return True
    if value is not _MISSING:
        return False
    attrs = record.get("attrs") if isinstance(record.get("attrs"), Mapping) else record
    marker = attrs.get(REDACTION_MARKER)
    if isinstance(marker, str):
        return marker == attribute
    if isinstance(marker, (list, tuple, set, frozenset, Mapping)):
        return attribute in {str(name) for name in marker}
    return marker is True and a_has_attribute


def _parse_time(text: Any) -> _dt.datetime | None:
    if not isinstance(text, str):
        return None
    try:
        parsed = _dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=_dt.timezone.utc)


# ---------------------------------------------------------------------------
# Sections
# ---------------------------------------------------------------------------


async def _status_or_error(
    target: Target, token: str | None, principal: str | None
) -> tuple[int | None, str | None]:
    """The HTTP status, or the reason no status came back (server unreachable)."""
    try:
        return await raw_initialize_status(target, token, principal), None
    except Exception as exc:  # noqa: BLE001 - recorded as a failed check
        return None, _exception_text(exc)


async def check_transport(target: Target, phase: str, rec: Recorder) -> None:
    section = "transport"
    principal = target.principal_a if target.multi_user else None
    wrong = secrets.token_hex(32)  # never written anywhere
    status, error = await _status_or_error(target, wrong, principal)
    rec.add(section, "wrong_token_401", status == 401, http_status=status, transport_error=error)
    status, error = await _status_or_error(target, None, principal)
    rec.add(section, "missing_token_401", status == 401, http_status=status, transport_error=error)
    status, error = await _status_or_error(target, target.token, principal)
    rec.add(
        section, "configured_token_200", status == 200, http_status=status, transport_error=error
    )
    if phase == "after-rotation":
        if not target.old_token:
            rec.add(section, "old_token_401", False, reason="no old token supplied")
            return
        status, error = await _status_or_error(target, target.old_token, principal)
        rec.add(
            section,
            "old_token_401",
            status == 401,
            http_status=status,
            transport_error=error,
            old_token_fingerprint=_digest(target.old_token),
            new_token_fingerprint=_digest(target.token),
            tokens_differ=target.old_token != target.token,
        )


def _expand_args(call: Mapping[str, Any], extra: Mapping[str, Any]) -> dict[str, Any]:
    arguments = {
        "input": {"node_id": call["node_id"]},
        "live": {"relation": call["relation"], "args": dict(call.get("args") or {})},
        **dict(extra),
    }
    arguments.update(dict(call.get("expand") or {}))
    return arguments


async def check_live(
    target: Target, expectations: Mapping[str, Any], rec: Recorder, info: dict[str, Any]
) -> None:
    section = "live"
    live = expectations["live"]
    extra = live.get("expand") or {"direction": "out", "limit": 1}
    max_age = live.get("max_observed_age_seconds", 300)
    principal = target.principal_a if target.multi_user else None

    declared: dict[str, set[str]] = {}
    for node in sorted({str(call["node_id"]) for call in live["calls"]}):
        result = await call_tool(
            target, principal, "inspect", {"target": {"kind": "node", "node_id": node}}
        )
        if result.server_info and "server_info" not in info:
            info["server_info"] = result.server_info
        relations = (result.payload or {}).get("live_relations")
        declared[node] = set(relations) if isinstance(relations, dict) else set()
        rec.add(
            section,
            f"inspect_declares_live_relations[{_digest(node)}]",
            bool(declared[node]),
            declared=sorted(declared[node]),
            transport_error=result.transport_error,
        )

    exercised: set[str] = set()
    for call in live["calls"]:
        name = str(call.get("name") or call["relation"])
        result = await call_tool(target, principal, "expand", _expand_args(call, extra))
        envelope = (result.payload or {}).get("external_live")
        envelope = envelope if isinstance(envelope, dict) else {}
        rows = envelope.get("rows") if isinstance(envelope.get("rows"), list) else []
        observed_at = _parse_time(envelope.get("observed_at"))
        age = (_now() - observed_at).total_seconds() if observed_at else None
        keys = sorted({str(k) for row in rows if isinstance(row, dict) for k in row})
        required = [str(k) for k in call.get("required_row_keys") or []]
        missing_keys = sorted(
            {k for row in rows if isinstance(row, dict) for k in required if k not in row}
        ) if rows else required
        expect_state = call.get("expect_state", "ok")
        problems: list[str] = []
        if result.transport_error:
            problems.append("transport_error")
        if envelope.get("schema") != "okg.external_live.v1":
            problems.append("no_external_live_envelope")
        if envelope.get("relation") != call["relation"]:
            problems.append("relation_mismatch")
        if envelope.get("state") != expect_state:
            problems.append("state")
        if not int(call.get("min_rows", 1)) <= len(rows) <= int(call.get("max_rows", 10_000)):
            problems.append("row_count")
        if missing_keys:
            problems.append("row_keys")
        if age is None or age > float(max_age) or age < -60:
            problems.append("observed_at")
        if not envelope.get("pinned_generation_id"):
            problems.append("pinned_generation_id")
        if call["relation"] not in declared.get(str(call["node_id"]), set()):
            problems.append("not_declared_by_inspect")
        if not problems:
            exercised.add(str(call["relation"]))
        rec.add(
            section,
            f"call[{name}]",
            not problems,
            problems=problems,
            relation=call["relation"],
            state=envelope.get("state"),
            expected_state=expect_state,
            rows=len(rows),
            row_keys=keys,
            rows_digest=_digest(rows),
            truncated=bool(envelope.get("truncated")),
            policy=envelope.get("policy"),
            observed_at=envelope.get("observed_at"),
            observed_age_seconds=None if age is None else round(age, 3),
            max_observed_age_seconds=max_age,
            pinned_generation_id=envelope.get("pinned_generation_id"),
            graph_freshness=(result.payload or {}).get("freshness"),
            transport_error=result.transport_error,
        )
        if target.multi_user and "b_expect_state" in call:
            b_result = await call_tool(
                target, target.principal_b, "expand", _expand_args(call, extra)
            )
            b_env = (b_result.payload or {}).get("external_live") or {}
            b_state = b_env.get("state") if isinstance(b_env, dict) else None
            b_code = refusal_code(b_result.payload)
            observed = b_state or b_code
            rec.add(
                section,
                f"call_as_b[{name}]",
                observed == call["b_expect_state"],
                observed=observed,
                expected=call["b_expect_state"],
            )

    all_declared = set().union(*declared.values()) if declared else set()
    rec.add(
        section,
        "every_declared_relation_exercised",
        bool(all_declared) and all_declared <= exercised,
        declared=sorted(all_declared),
        exercised_ok=sorted(exercised),
    )

    for refusal in live["refusals"]:
        name = str(refusal.get("name") or refusal["relation"])
        result = await call_tool(target, principal, "expand", _expand_args(refusal, extra))
        payload = result.payload or {}
        envelope = payload.get("external_live") if isinstance(payload.get("external_live"), dict) else {}
        error = envelope.get("error") if isinstance(envelope.get("error"), dict) else {}
        observed = {
            "tool_code": refusal_code(payload),
            "live_state": envelope.get("state"),
            "live_reason": error.get("reason"),
        }
        rows = envelope.get("rows") or []
        expected = refusal["expect_code"]
        rec.add(
            section,
            f"refusal[{name}]",
            expected in observed.values() and not rows and not result.transport_error,
            expected=expected,
            observed=observed,
            rows=len(rows),
            transport_error=result.transport_error,
        )


@dataclass
class Differential:
    """What A and B were each served, for the leak scan and the concurrency arm."""

    a_responses: list[CallResult] = field(default_factory=list)
    b_responses: list[CallResult] = field(default_factory=list)
    a_only: set[str] = field(default_factory=set)
    denied: set[str] = field(default_factory=set)
    #: A's clear value of the masked attribute.
    clear_values: set[str] = field(default_factory=set)
    digest: dict[str, str] = field(default_factory=dict)


def _a_only_values(
    a_payloads: Sequence[dict[str, Any] | None],
    denied: set[str],
    extra: Iterable[str],
    shared: Iterable[str],
) -> set[str]:
    """Strings A was served that B must never see.

    The node ids B is denied; every string inside A's records for those
    nodes; the clear value of the masked attribute. Minus every string inside
    A's records for nodes B may see (those are legitimately shared, e.g. the
    subtype name), the operator's ``shared_values``, and short strings.
    """
    pinned = set(denied) | {value for value in extra if isinstance(value, str)}
    hidden: set[str] = set(pinned)
    visible: set[str] = set(shared)
    for payload in a_payloads:
        for item in _walk(payload):
            if not isinstance(item, dict) or "node_id" not in item:
                continue
            leaves = string_leaves(item)
            if str(item["node_id"]) in denied:
                hidden |= leaves
            else:
                visible |= leaves
    # A visible record can hold a pinned value: an edge naming the denied id,
    # or -- in A's copy -- the masked attribute's clear value, which sits in a
    # record B may read. Those stay hidden whatever else shares them.
    hidden -= visible - pinned
    return {value for value in hidden if len(value) >= MIN_LEAK_TOKEN_LENGTH}


def _json_forms(value: str) -> set[str]:
    """A string as it can appear in the serialised JSON: raw and escaped."""
    return {value, json.dumps(value, ensure_ascii=False)[1:-1]}


def _spans(text: str, needle: str) -> Iterable[tuple[int, int]]:
    start = text.find(needle)
    while start != -1:
        yield start, start + len(needle)
        start = text.find(needle, start + 1)


def leaked_values(response: CallResult, a_only: set[str]) -> set[str]:
    """A-only values found anywhere in one response's serialised JSON.

    Substring, not leaf equality: a value folded into a snippet, a message or
    a continuation token is as much a leak as one returned whole. The strings
    the caller itself sent are taken out of the text first (a refusal may
    echo the requested node id, and a part of that id is the caller's own
    input), and the rest is scanned: an occurrence counts unless it lies
    wholly inside an echo of a sent string. So an echoed id excuses only the
    echo, never the same value returned again in another field. Spans are
    used rather than deleting the sent text, so a short sent string such as
    ``"partition"`` cannot cut a longer A-only value out of the scan.
    """
    text = response.serialised
    sent_spans = [
        span
        for sent in response.sent
        if len(sent) >= MIN_LEAK_TOKEN_LENGTH
        for form in _json_forms(sent)
        for span in _spans(text, form)
    ]
    leaks: set[str] = set()
    for value in a_only:
        for form in _json_forms(value):
            for start, end in _spans(text, form):
                if not any(s <= start and end <= e for s, e in sent_spans):
                    leaks.add(value)
                    break
            if value in leaks:
                break
    return leaks


async def check_multi_user(
    target: Target, expectations: Mapping[str, Any], rec: Recorder
) -> Differential:
    section = "multi_user"
    spec = expectations.get("multi_user") or {}
    diff = Differential()
    if not spec:
        rec.add(section, "configured", False, reason="expectations.multi_user is missing")
        return diff
    if not target.multi_user:
        rec.add(
            section,
            "configured",
            False,
            reason="principal header, principal A and principal B are all required",
        )
        return diff
    diff.denied = {str(item) for item in spec.get("denied_node_ids") or []}
    rec.add(
        section,
        "denied_node_ids_declared",
        bool(diff.denied),
        count=len(diff.denied),
        digests=sorted(_digest(item) for item in diff.denied),
    )

    a_ids_all: list[set[str]] = []
    b_ids_all: list[set[str]] = []
    for call in spec.get("calls") or []:
        name = str(call.get("name") or call["tool"])
        a = await call_tool(target, target.principal_a, call["tool"], call["arguments"])
        b = await call_tool(target, target.principal_b, call["tool"], call["arguments"])
        diff.a_responses.append(a)
        diff.b_responses.append(b)
        a_ids, b_ids = node_ids(a.payload), node_ids(b.payload)
        a_ids_all.append(a_ids)
        b_ids_all.append(b_ids)
        a_include = {str(x) for x in (call.get("a") or {}).get("node_ids_include") or []}
        b_include = {str(x) for x in (call.get("b") or {}).get("node_ids_include") or []}
        problems = []
        if a.transport_error or b.transport_error:
            problems.append("transport_error")
        if refusal_code(a.payload) or refusal_code(b.payload):
            problems.append("refused")
        if not a_include <= a_ids or not diff.denied <= a_ids:
            problems.append("a_missing_expected_nodes")
        if not b_include <= b_ids:
            problems.append("b_missing_expected_nodes")
        if (diff.denied & b_ids) - b.sent:
            problems.append("b_sees_denied_node")
        if a_ids == b_ids:
            problems.append("not_differentiating")
        rec.add(
            section,
            f"differential[{name}]",
            not problems,
            problems=problems,
            a_node_count=len(a_ids),
            b_node_count=len(b_ids),
            a_nodes_digest=_digest(sorted(a_ids)),
            b_nodes_digest=_digest(sorted(b_ids)),
        )
    if not spec.get("calls"):
        rec.add(section, "differential", False, reason="multi_user.calls is empty")
    diff.digest = {
        "a": _digest([sorted(ids) for ids in a_ids_all]),
        "b": _digest([sorted(ids) for ids in b_ids_all]),
    }

    refusal = spec.get("refusal")
    if refusal:
        a = await call_tool(target, target.principal_a, refusal["tool"], refusal["arguments"])
        b = await call_tool(target, target.principal_b, refusal["tool"], refusal["arguments"])
        diff.a_responses.append(a)
        diff.b_responses.append(b)
        b_code, a_code = refusal_code(b.payload), refusal_code(a.payload)
        expected = refusal["b_expect_code"]
        rec.add(
            section,
            "b_refused_where_a_is_served",
            b_code == expected
            and a_code is None
            and not a.transport_error
            and not b.transport_error,
            b_code=b_code,
            b_expected=expected,
            a_code=a_code,
        )
    else:
        rec.add(section, "b_refused_where_a_is_served", False, reason="multi_user.refusal missing")

    mask = spec.get("mask")
    clear_values: list[str] = []
    if mask:
        a = await call_tool(target, target.principal_a, mask["tool"], mask["arguments"])
        b = await call_tool(target, target.principal_b, mask["tool"], mask["arguments"])
        diff.a_responses.append(a)
        diff.b_responses.append(b)
        node, attribute = str(mask["node_id"]), str(mask["attribute"])
        a_records = records_for(a.payload, node)
        b_records = records_for(b.payload, node)
        a_values = [attribute_value(r, attribute) for r in a_records]
        a_clear = [v for v in a_values if v is not _MISSING and v != MASK_TOKEN]
        clear_values = [v for v in a_clear if isinstance(v, str)]
        diff.clear_values = set(clear_values)
        b_masked = [is_masked(r, attribute, a_has_attribute=bool(a_clear)) for r in b_records]
        rec.add(
            section,
            "b_masked_where_a_is_clear",
            bool(a_clear)
            and not any(is_masked(r, attribute, a_has_attribute=True) for r in a_records)
            and bool(b_records)
            and all(b_masked),
            node=_digest(node),
            attribute=attribute,
            a_records=len(a_records),
            b_records=len(b_records),
            a_value_digests=sorted({_digest(v) for v in a_clear}),
            b_masked=b_masked,
        )
    else:
        rec.add(section, "b_masked_where_a_is_clear", False, reason="multi_user.mask missing")

    probe = (spec.get("calls") or [None])[0]
    if probe:
        expected = spec.get("unidentified_expect_code", UNIDENTIFIED_CODE)
        for label, principal in (
            ("no_principal_header", None),
            ("unbound_principal", f"okg-live-proof-{secrets.token_hex(6)}@invalid.example"),
        ):
            result = await call_tool(target, principal, probe["tool"], probe["arguments"])
            diff.b_responses.append(result)  # scanned for A-only values too
            code = refusal_code(result.payload)
            rec.add(
                section,
                f"unidentified[{label}]",
                code == expected and not node_ids(result.payload),
                code=code,
                expected=expected,
                transport_error=result.transport_error,
            )

    diff.a_only = _a_only_values(
        [r.payload for r in diff.a_responses],
        diff.denied,
        clear_values,
        [str(v) for v in spec.get("shared_values") or []],
    )
    leaks = sorted(
        _digest(value)
        for response in diff.b_responses
        for value in leaked_values(response, diff.a_only)
    )
    rec.add(
        section,
        "no_a_only_value_in_any_b_response",
        bool(diff.a_only) and not leaks,
        a_only_values_scanned=len(diff.a_only),
        b_responses_scanned=len(diff.b_responses),
        min_value_length=MIN_LEAK_TOKEN_LENGTH,
        leaked_value_digests=sorted(set(leaks)),
    )
    return diff


async def check_concurrency(
    target: Target,
    expectations: Mapping[str, Any],
    diff: Differential,
    rec: Recorder,
    *,
    per_principal: int,
    parallelism: int,
) -> None:
    section = "concurrency"
    spec = expectations.get("concurrency") or {}
    call = spec.get("call") or ((expectations.get("multi_user") or {}).get("calls") or [None])[0]
    if not target.multi_user or not call or not diff.a_only:
        rec.add(
            section,
            "interleaved",
            False,
            reason="needs multi-user target, a differential call and a non-empty A-only set",
        )
        return
    count = int(spec.get("calls_per_principal", per_principal))
    gate = asyncio.Semaphore(int(spec.get("parallelism", parallelism)))

    async def one(principal: str) -> tuple[str, CallResult]:
        async with gate:
            return principal, await call_tool(target, principal, call["tool"], call["arguments"])

    order = [p for _ in range(count) for p in (target.principal_a, target.principal_b)]
    started = time.monotonic()
    results = await asyncio.gather(*(one(p) for p in order))  # type: ignore[arg-type]
    elapsed = time.monotonic() - started
    counts = {"a_ok": 0, "b_ok": 0, "errors": 0, "a_wrong_answer": 0, "b_leaks": 0}
    for principal, result in results:
        if result.transport_error or result.payload is None or refusal_code(result.payload):
            counts["errors"] += 1
            continue
        ids = node_ids(result.payload)
        if principal == target.principal_a:
            if diff.denied <= ids:
                counts["a_ok"] += 1
            else:
                counts["a_wrong_answer"] += 1
        else:
            if leaked_values(result, diff.a_only) or (diff.denied & ids) - result.sent:
                counts["b_leaks"] += 1
            else:
                counts["b_ok"] += 1
    rec.add(
        section,
        "interleaved",
        counts["a_ok"] == count and counts["b_ok"] == count,
        calls=len(results),
        calls_per_principal=count,
        elapsed_seconds=round(elapsed, 3),
        **counts,
    )


def check_against_baseline(
    baseline_path: Path | None, diff: Differential, rec: Recorder
) -> None:
    if baseline_path is None:
        return
    try:
        baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
        before = baseline["differential_digest"]
    except (OSError, ValueError, KeyError) as exc:
        rec.add("phase", "same_answer_as_baseline", False, reason=f"baseline unreadable: {exc}")
        return
    rec.add(
        "phase",
        "same_answer_as_baseline",
        before == diff.digest and bool(diff.digest),
        baseline_digest=before,
        this_digest=diff.digest,
    )


def check_restart_declaration(
    phase: str,
    declared_at: str | None,
    baseline_path: Path | None,
    started: _dt.datetime,
    rec: Recorder,
) -> None:
    """The operator's restart time lies after the baseline run and before this one."""
    if declared_at is None:
        return
    declared = _parse_time(declared_at)
    baseline_finished = None
    if baseline_path is not None:
        try:
            baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
            baseline_finished = _parse_time(baseline.get("finished_at"))
        except (OSError, ValueError, AttributeError):
            baseline_finished = None
    problems = []
    if declared is None:
        problems.append("unparseable")
    else:
        if declared > started:
            problems.append("after_this_run_started")
        if baseline_path is not None and baseline_finished is None:
            problems.append("baseline_finished_at_unreadable")
        elif baseline_finished is not None and declared < baseline_finished:
            problems.append("before_the_baseline_finished")
    rec.add(
        "phase",
        "restart_declared_between_runs",
        not problems,
        problems=problems,
        phase=phase,
        declared_at=declared_at,
        baseline_finished_at=None if baseline_finished is None else baseline_finished.isoformat(),
    )


def harness_provenance() -> dict[str, Any]:
    """Which harness produced the evidence: file hash, git head, local edits."""
    here = Path(__file__).resolve()
    provenance: dict[str, Any] = {
        "harness_sha256": hashlib.sha256(here.read_bytes()).hexdigest(),
        "harness_git_head": None,
        "harness_tree_dirty": None,
    }
    try:
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=here.parent, capture_output=True, text=True, timeout=10, check=True,
        ).stdout.strip()
        status = subprocess.run(
            ["git", "status", "--porcelain", "--", "."],
            cwd=here.parent, capture_output=True, text=True, timeout=10, check=True,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        return provenance  # not a git checkout, or git is absent: recorded as null
    provenance["harness_git_head"] = head or None
    provenance["harness_tree_dirty"] = bool(status.strip())
    return provenance


# ---------------------------------------------------------------------------
# One run
# ---------------------------------------------------------------------------


@dataclass
class RunResult:
    evidence: dict[str, Any]
    #: Values that must never reach the evidence in clear; checked on write.
    sensitive: set[str] = field(default_factory=set)


async def run(
    target: Target,
    expectations: Mapping[str, Any],
    *,
    phase: str,
    expectations_sha256: str = "",
    baseline_evidence: Path | None = None,
    concurrency_per_principal: int = 20,
    concurrency_parallelism: int = 10,
    okg_commit: str | None = None,
    restart_declared_at: str | None = None,
) -> RunResult:
    if phase not in PHASES:
        raise HarnessInputError(f"--phase must be one of {', '.join(PHASES)}")
    if phase == "after-restart" and not restart_declared_at:
        raise HarnessInputError(
            "--phase after-restart needs --restart-declared-at: the time the operator "
            "restarted the server, as ISO 8601"
        )
    if restart_declared_at and _parse_time(restart_declared_at) is None:
        raise HarnessInputError("--restart-declared-at must be an ISO 8601 time")
    validate_expectations(expectations)
    salt = secrets.token_bytes(16)  # principal labels; never written
    rec = Recorder()
    info: dict[str, Any] = {}
    started = _now()
    await check_transport(target, phase, rec)
    await check_live(target, expectations, rec, info)
    diff = Differential()
    if target.multi_user or expectations.get("multi_user"):
        diff = await check_multi_user(target, expectations, rec)
        await check_concurrency(
            target,
            expectations,
            diff,
            rec,
            per_principal=concurrency_per_principal,
            parallelism=concurrency_parallelism,
        )
    check_against_baseline(baseline_evidence, diff, rec)
    check_restart_declaration(phase, restart_declared_at, baseline_evidence, started, rec)
    finished = _now()
    split = urlsplit(target.url)
    failed = [c for c in rec.checks if not c.passed]
    evidence = {
        "schema": EVIDENCE_SCHEMA,
        "phase": phase,
        "deployment": expectations.get("deployment"),
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "target": {
            "url_host": split.netloc,
            "url_path": split.path,
            "token_env": target.token_env,
            "token_fingerprint": _digest(target.token),
            "old_token_env": target.old_token_env,
            "principal_header": target.principal_header,
            "principal_a": (
                _salted_digest(target.principal_a, salt) if target.principal_a else None
            ),
            "principal_b": (
                _salted_digest(target.principal_b, salt) if target.principal_b else None
            ),
            "principal_label_salt": "random_per_run_not_written",
            "multi_user": target.multi_user,
        },
        "server_info": info.get("server_info"),
        "okg_commit": (
            {"value": okg_commit, "source": "operator_declared"}
            if okg_commit
            else {"value": None, "source": "not_served_by_the_endpoint"}
        ),
        "expectations_sha256": expectations_sha256,
        **harness_provenance(),
        "restart_declared_at": (
            {"value": restart_declared_at, "source": "operator_declared"}
            if restart_declared_at
            else None
        ),
        "differential_digest": diff.digest,
        "checks": [check.as_json() for check in rec.checks],
        "summary": {
            "ok": rec.ok,
            "total": len(rec.checks),
            "passed": len(rec.checks) - len(failed),
            "failed": [f"{c.section}.{c.name}" for c in failed],
        },
    }
    # Not every A-only string: those include ordinary words a server may also
    # say elsewhere. The denied ids and the masked clear value are the ones
    # the evidence must never carry.
    return RunResult(evidence=evidence, sensitive=diff.denied | diff.clear_values)


def assert_evidence_holds_no_secret(evidence_text: str, secrets_in_clear: Iterable[str]) -> None:
    """Refuse to write evidence that carries a token, principal or A-only value."""
    for value in secrets_in_clear:
        if value and len(value) >= 4 and value in evidence_text:
            raise RuntimeError(
                "refusing to write evidence: it contains a value that must only "
                f"appear hashed ({_digest(value)})"
            )


def write_evidence(
    evidence: Mapping[str, Any], out: Path, target: Target, extra_secret_values: Iterable[str] = ()
) -> None:
    text = json.dumps(evidence, indent=2, sort_keys=True, default=str) + "\n"
    assert_evidence_holds_no_secret(
        text,
        [
            target.token,
            target.old_token or "",
            target.principal_a or "",
            target.principal_b or "",
            *extra_secret_values,
        ],
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text, encoding="utf-8")


# ---------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------


def _env(name: str) -> str | None:
    value = os.environ.get(name)
    return value if value else None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tests.live.mcp_deployment.harness",
        description="Prove live relations and multi-user MCP access on a running deployment.",
    )
    parser.add_argument("--url", default=_env("OKG_LIVE_MCP_URL"))
    parser.add_argument(
        "--token-env",
        default=_env("OKG_LIVE_MCP_TOKEN_ENV"),
        help="NAME of the environment variable holding the bearer token",
    )
    parser.add_argument("--principal-header", default=_env("OKG_LIVE_MCP_PRINCIPAL_HEADER"))
    parser.add_argument("--principal-a", default=_env("OKG_LIVE_MCP_PRINCIPAL_A"))
    parser.add_argument("--principal-b", default=_env("OKG_LIVE_MCP_PRINCIPAL_B"))
    parser.add_argument("--expectations", default=_env("OKG_LIVE_MCP_EXPECTATIONS"))
    parser.add_argument("--phase", default=_env("OKG_LIVE_MCP_PHASE") or "baseline", choices=PHASES)
    parser.add_argument(
        "--old-token-env",
        default=_env("OKG_LIVE_MCP_OLD_TOKEN_ENV"),
        help="after-rotation: NAME of the variable holding the retired token",
    )
    parser.add_argument("--baseline-evidence", default=_env("OKG_LIVE_MCP_BASELINE_EVIDENCE"))
    parser.add_argument("--out", default=_env("OKG_LIVE_MCP_EVIDENCE_OUT"))
    parser.add_argument(
        "--okg-commit",
        default=_env("OKG_LIVE_MCP_OKG_COMMIT"),
        help="the okg commit the server runs, as the operator knows it (the endpoint does not serve it)",
    )
    parser.add_argument(
        "--restart-declared-at",
        default=_env("OKG_LIVE_MCP_RESTART_DECLARED_AT"),
        help=(
            "ISO 8601 time the operator restarted the server (required for "
            "--phase after-restart; optional for after-rotation)"
        ),
    )
    parser.add_argument("--concurrency", type=int, default=20, help="calls per principal")
    parser.add_argument("--parallelism", type=int, default=10)
    return parser


def target_from_args(args: argparse.Namespace) -> Target:
    missing = [
        flag
        for flag, value in (
            ("--url", args.url),
            ("--token-env", args.token_env),
            ("--expectations", args.expectations),
            ("--out", args.out),
        )
        if not value
    ]
    if missing:
        raise HarnessInputError(f"missing required inputs: {', '.join(missing)}")
    token = os.environ.get(args.token_env)
    if not token:
        raise HarnessInputError(f"environment variable {args.token_env} is empty or unset")
    old_token = None
    if args.phase == "after-rotation":
        if not args.old_token_env or not os.environ.get(args.old_token_env):
            raise HarnessInputError("--phase after-rotation needs --old-token-env naming a set variable")
        old_token = os.environ[args.old_token_env]
    principals = [args.principal_header, args.principal_a, args.principal_b]
    if any(principals) and not all(principals):
        raise HarnessInputError(
            "give all of --principal-header, --principal-a and --principal-b, or none"
        )
    if args.principal_a and args.principal_a == args.principal_b:
        raise HarnessInputError("principal A and principal B must differ")
    return Target(
        url=args.url,
        token=token,
        token_env=args.token_env,
        principal_header=args.principal_header,
        principal_a=args.principal_a,
        principal_b=args.principal_b,
        old_token=old_token,
        old_token_env=args.old_token_env,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        target = target_from_args(args)
        path = Path(args.expectations)
        expectations = load_expectations(path)
        result = asyncio.run(
            run(
                target,
                expectations,
                phase=args.phase,
                expectations_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                baseline_evidence=Path(args.baseline_evidence) if args.baseline_evidence else None,
                concurrency_per_principal=args.concurrency,
                concurrency_parallelism=args.parallelism,
                okg_commit=args.okg_commit,
                restart_declared_at=args.restart_declared_at,
            )
        )
    except HarnessInputError as exc:
        print(f"live-proof: {exc}", file=sys.stderr)
        return 2
    evidence = result.evidence
    write_evidence(evidence, Path(args.out), target, result.sensitive)
    summary = evidence["summary"]
    print(
        f"live-proof {evidence['phase']}: {summary['passed']}/{summary['total']} checks passed"
        + (f"; failed: {', '.join(summary['failed'])}" if summary["failed"] else "")
        + f"; evidence: {args.out}"
    )
    return 0 if summary["ok"] else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
