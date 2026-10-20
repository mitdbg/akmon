"""The one envelope every `okg pact` verb returns (`pact-honest-envelope`).

`ok` used to mean "the command did not crash", and each verb bolted its real
verdict onto a different sibling field (`actionable`, `marked`,
`intent_registration.status`, `decision_gate.ok`, `coordination`). Agents built
summarizers on `ok` and shipped false "gates green" reports.

Under this contract there is exactly one verdict vocabulary:

``recorded``
    A mutating verb persisted, a gate subject holds, a read is actionable.
``deferred``
    The local half happened but the durable half did not: the decision-ledger
    write is queued as a closure intent. Never ``ok: true``.
``refused``
    Nothing was persisted, the gate subject does not hold, or the read cannot
    answer the question. Every error path lands here.

``ok`` is derived — ``ok == (verdict == "recorded")`` — so it cannot disagree
with the verdict, and ``blocked_by`` is the ONLY location for refusal and
deferral causes. Exit codes follow the verdict (recorded 0, deferred 3,
refused 1, crash 2); deferred is nonzero so a `set -e` pipeline stops treating
queued-not-recorded as success.

Every dict carrying an `ok` key in this package is built here — asserted by an
AST check in `tests/pact/test_envelope_contract.py`, because 107 hand-built
`ok` literals is how the per-verb dialects arose in the first place, and a
string grep is satisfiable by `dict(ok=...)`.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

RECORDED = "recorded"
DEFERRED = "deferred"
REFUSED = "refused"

VERDICTS: tuple[str, ...] = (RECORDED, DEFERRED, REFUSED)

# Exit codes follow the verdict. `crash` is not a verdict — it is the absence
# of one (an unhandled exception), and the CLI maps it separately.
EXIT_RECORDED = 0
EXIT_REFUSED = 1
EXIT_CRASH = 2
EXIT_DEFERRED = 3

EXIT_CODES: dict[str, int] = {
    RECORDED: EXIT_RECORDED,
    DEFERRED: EXIT_DEFERRED,
    REFUSED: EXIT_REFUSED,
}

# The keys the contract guarantees on every payload, in emission order.
ENVELOPE_KEYS: tuple[str, ...] = (
    "verdict",
    "ok",
    "blocked_by",
    "next_commands",
    "text",
)

# Keys that survive any size trim: a byte budget may never cost a reader the
# verdict or its cause (`size never lies`).
PROTECTED_KEYS: frozenset[str] = frozenset(
    {*ENVELOPE_KEYS, "message", "why", "change", "format"}
)

ENVELOPE_SCHEMA: dict[str, Any] = {
    "schema": "okg.pact.envelope.v1",
    "required": list(ENVELOPE_KEYS),
    "properties": {
        "verdict": {"type": "string", "enum": list(VERDICTS)},
        "ok": {"type": "boolean", "derived": 'verdict == "recorded"'},
        "blocked_by": {
            "type": "array",
            "items": {"type": "string"},
            "description": "the only location for refusal/deferral causes",
        },
        "next_commands": {"type": "array", "items": {"type": "string"}},
        "text": {"type": "string", "description": "one-line human summary"},
        "message": {
            "type": "string",
            "required_when": 'verdict != "recorded"',
        },
        "why": {"type": "string", "optional": True},
    },
    "exit_codes": dict(EXIT_CODES) | {"crash": EXIT_CRASH},
}


def _tokens(values: Iterable[Any] | None) -> list[str]:
    """Normalize a cause source into ordered, de-duplicated glossary tokens."""
    if not values:
        return []
    seen: dict[str, None] = {}
    for value in values:
        if value is None:
            continue
        token = value if isinstance(value, str) else str(value)
        token = token.strip()
        if token:
            seen.setdefault(token, None)
    return list(seen)


def tokens(*sources: Iterable[Any] | None) -> list[str]:
    """Union several cause locations into one `blocked_by` list.

    Causes used to rotate across `blockers`, `missing_proof`,
    `closure_intent.reason`, and `workflow_authority.blockers`; a reader that
    checked one saw an empty diagnostic beside `ok: false`.
    """
    merged: list[Any] = []
    for source in sources:
        if source is None:
            continue
        merged.extend(source)
    return _tokens(merged)


def envelope(
    verdict: str,
    *,
    text: str | None = None,
    blocked_by: Iterable[Any] | None = None,
    next_commands: Iterable[Any] | None = None,
    message: str | None = None,
    why: str | None = None,
    **detail: Any,
) -> dict[str, Any]:
    """Build the one envelope. `ok` is derived, never passed in."""
    if verdict not in VERDICTS:
        raise ValueError(f"unknown pact verdict: {verdict!r}")
    causes = _tokens(blocked_by)
    commands = [str(command) for command in (next_commands or []) if command]
    resolved_text = text if isinstance(text, str) and text else _default_text(
        verdict, detail.get("change"), causes
    )
    payload: dict[str, Any] = {
        "verdict": verdict,
        "ok": verdict == RECORDED,
        "blocked_by": causes,
        "next_commands": commands,
        "text": resolved_text,
    }
    if verdict != RECORDED:
        # A refusal or deferral without a message is the shape agents read
        # past; `text` is the fallback so the key is never absent.
        payload["message"] = message or resolved_text
    elif message:
        payload["message"] = message
    if why:
        payload["why"] = why
    for key, value in detail.items():
        if key in ENVELOPE_KEYS:
            raise ValueError(f"{key!r} is an envelope key, not detail")
        payload[key] = value
    return payload


def _default_text(verdict: str, subject: Any, causes: list[str]) -> str:
    head = f"{verdict} {subject}" if subject else verdict
    return f"{head}: {', '.join(causes)}" if causes else head


def recorded(**kwargs: Any) -> dict[str, Any]:
    return envelope(RECORDED, **kwargs)


def deferred(**kwargs: Any) -> dict[str, Any]:
    return envelope(DEFERRED, **kwargs)


def refused(**kwargs: Any) -> dict[str, Any]:
    return envelope(REFUSED, **kwargs)


def from_ok(ok: Any, **kwargs: Any) -> dict[str, Any]:
    """`recorded` when the subject holds, `refused` otherwise.

    For verbs whose only two outcomes are "it holds" / "it does not"; a verb
    that can also queue a durable write must choose `deferred` explicitly.
    """
    return envelope(RECORDED if ok else REFUSED, **kwargs)


def gate_verdict(*, holds: bool, deferred_write: bool) -> str:
    """The gating-class row of the verdict-contract table.

    A queued ledger write is `deferred` even though the subject holds: the
    closure was not recorded anywhere durable, and `ok: true` there is the
    audited lie (12 commit messages claiming green gates that registered
    nothing).
    """
    if not holds:
        return REFUSED
    return DEFERRED if deferred_write else RECORDED


def restate(
    payload: Mapping[str, Any],
    verdict: str,
    *,
    text: str | None = None,
    blocked_by: Iterable[Any] | None = None,
    next_commands: Iterable[Any] | None = None,
    message: str | None = None,
    why: str | None = None,
) -> dict[str, Any]:
    """Re-stamp an envelope's verdict, keeping every detail field.

    For the arms where the CLI learns something the operation could not know —
    `gate task-done` discovers the refusal is ledger-only and queues a closure
    intent, so the same payload is a DEFERRAL. Re-stamping through the
    constructor is what keeps `ok` from being hand-patched out of agreement
    with `verdict`.
    """
    detail = {
        key: value
        for key, value in payload.items()
        if key not in ENVELOPE_KEYS and key not in ("message", "why")
    }
    return envelope(
        verdict,
        text=text if text is not None else payload.get("text"),
        blocked_by=blocked_by if blocked_by is not None else payload.get("blocked_by"),
        next_commands=(
            next_commands
            if next_commands is not None
            else payload.get("next_commands")
        ),
        message=message if message is not None else payload.get("message"),
        why=why if why is not None else payload.get("why"),
        **detail,
    )


def component(*, ok: bool | None, **detail: Any) -> dict[str, Any]:
    """A NESTED sub-report's `ok`, never a top-level envelope.

    Sub-reports (`review_view`, `decision_gate`, `graph_projection`,
    `scope_drift`, per-row list entries) carry their own boolean so the
    aggregating envelope can explain itself. They are built here too, so the
    AST check has exactly one origin to point at, and so a nested `ok` can
    never be mistaken for the verdict: only a payload with a `verdict` key is
    an envelope.
    """
    payload: dict[str, Any] = {"ok": ok}
    payload.update(detail)
    return payload


GENERIC_ERROR_TOKEN = "substrate_error"

# `SubstrateError.code` defaults to the base-class spelling, so a bare
# `SubstrateError(...)` used to name `substrate_error` as the cause of every
# error path — a `blocked_by` token that distinguishes nothing. These data keys
# are where the raise sites actually put the discriminating value.
_ERROR_DATA_TOKEN_KEYS: tuple[str, ...] = ("cause", "reason", "code", "status")


def error_token(err: Any) -> str:
    """The most specific cause token an error offers.

    Subclass wire code first (`extension_missing`, `unknown_change`, ...), then
    a `data` discriminator (`no_dsn_configured`, `writer_grant_not_applied`),
    and only then the generic spelling.
    """
    code = getattr(err, "code", None)
    if isinstance(code, str) and code.strip() and code != GENERIC_ERROR_TOKEN:
        return code.strip()
    data = getattr(err, "data", None)
    if isinstance(data, Mapping):
        for key in _ERROR_DATA_TOKEN_KEYS:
            value = data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return GENERIC_ERROR_TOKEN


def from_error(err: Any) -> dict[str, Any]:
    """A `SubstrateError` as a refused envelope.

    The raise path used to print `{"error": {...}}` with no `ok` key at all
    (operations.py:3076 / cli.py:703-714), so `payload.get("ok")` yielded
    `None` and agents read straight past the refusal. The `error` block stays
    as detail — every existing reader of `error.code` keeps working — but the
    envelope now leads.
    """
    from okg.substrate.pact import repair

    code = getattr(err, "code", GENERIC_ERROR_TOKEN)
    message = getattr(err, "message", None) or str(err)
    fix_hint = getattr(err, "fix_hint", None)
    data = getattr(err, "data", None)
    data = data if isinstance(data, Mapping) else {}
    detail = err.to_json() if hasattr(err, "to_json") else {"error": {"code": code}}
    # Every error-path recommendation goes through the one command funnel:
    # a hint that is prose ("inspect the intent file; delete it to unwedge
    # sync") is still shown — as `why` — but it is never published as a
    # command an agent is told to run
    # (pact-self-repairing-refusals req.refusal.executable).
    candidates = [*(data.get("next_commands") or []), fix_hint]
    commands = repair.executable_commands(candidates)
    why = fix_hint if fix_hint and fix_hint not in commands else None
    return refused(
        text=f"refused [{code}]: {message}",
        message=message,
        blocked_by=[error_token(err)],
        next_commands=commands,
        why=why,
        **detail,
    )


def is_envelope(payload: Any) -> bool:
    return isinstance(payload, Mapping) and "verdict" in payload and "ok" in payload


def verdict_of(payload: Mapping[str, Any]) -> str:
    """The verdict of a payload, tolerating a pre-envelope shape."""
    value = payload.get("verdict")
    if isinstance(value, str) and value in VERDICTS:
        return value
    return RECORDED if payload.get("ok") else REFUSED


def exit_code(payload: Mapping[str, Any]) -> int:
    return EXIT_CODES.get(verdict_of(payload), EXIT_REFUSED)


def schema_violations(payload: Any) -> list[str]:
    """Why `payload` is not a legal envelope; empty means it is."""
    problems: list[str] = []
    if not isinstance(payload, Mapping):
        return ["payload is not a mapping"]
    for key in ENVELOPE_KEYS:
        if key not in payload:
            problems.append(f"missing key: {key}")
    verdict = payload.get("verdict")
    if verdict not in VERDICTS:
        problems.append(f"verdict not in {VERDICTS}: {verdict!r}")
    ok = payload.get("ok")
    if not isinstance(ok, bool):
        problems.append(f"ok is not a boolean: {ok!r}")
    elif verdict in VERDICTS and ok is not (verdict == RECORDED):
        problems.append(f"ok={ok} contradicts verdict={verdict}")
    causes = payload.get("blocked_by")
    if not isinstance(causes, list) or not all(isinstance(t, str) for t in causes):
        problems.append("blocked_by is not a list of strings")
    elif verdict in (DEFERRED, REFUSED) and not causes:
        problems.append(f"{verdict} envelope names no cause in blocked_by")
    commands = payload.get("next_commands")
    if not isinstance(commands, list) or not all(isinstance(c, str) for c in commands):
        problems.append("next_commands is not a list of strings")
    text = payload.get("text")
    if not isinstance(text, str) or not text:
        problems.append("text is not a non-empty string")
    if verdict in (DEFERRED, REFUSED):
        message = payload.get("message")
        if not isinstance(message, str) or not message:
            problems.append(f"{verdict} envelope carries no message")
    return problems


def fit_budget(
    payload: dict[str, Any],
    budget_bytes: int,
    *,
    trim: Iterable[tuple[str, int]] = (),
) -> dict[str, Any]:
    """Shrink verbose detail to fit a byte budget WITHOUT touching the verdict.

    Overflow used to set `ok = False` (cli.py:1420-1424), turning a size
    problem into a fake gate failure. Size never lies: `verdict`, `ok`,
    `blocked_by`, `message`, `next_commands`, and `text` always survive; only
    verbose artifacts shrink, and the trim is reported so the caller knows the
    full report is one `--full` away.
    """
    import json

    trim = tuple(trim)

    def _encoded() -> int:
        return len(json.dumps(payload, default=str).encode("utf-8"))

    before = _encoded()
    if before <= budget_bytes:
        return payload
    for key, keep in trim:
        value = payload.get(key)
        if isinstance(value, list):
            payload[key] = value[:keep]
        if _encoded() <= budget_bytes:
            break
    payload["budget_trimmed"] = {
        "bytes": before,
        "budget": budget_bytes,
        # Names WHAT was trimmed, not "rerun with --full": the trimmed keys
        # are verbose artifacts, and every message this payload needs to be
        # understood survives the trim by construction (PROTECTED_KEYS).
        "note": (
            "verbose detail trimmed to fit the byte budget; verdict, message, "
            "blocked_by and next_commands are intact"
        ),
        "trimmed_keys": [key for key, _keep in trim],
    }
    return payload
