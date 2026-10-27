"""Hint H3 (#3666, lane ri): name the `each` form built from the agent's own calls.

Agents asked one live relation once per value, call after call (30
single calls to one summary relation), while `run` with
``live.each`` makes them all in one call. On the third call in a session
to one relation on one record whose arguments differ from two earlier
calls in exactly ONE argument (the others equal), the reply names that
run: the same relation, the shared arguments as this call sent them, and
``each`` over the values the calls used, in call order. The hint is built
only from the agent's own calls, so the call it names is one this server
serves; it is not printed when `run` is not served to the session.

The same memory finds the chain of kept answers such calls left (PACT
read-multi-handle-input): :func:`note_live_call` also returns the chain,
which the server names as one chain handle, when the varying argument is an
exact identifier (a declared string with no enum, every value without a
wildcard) and at least three of its answers are still kept.

Memory is per reader and logical MCP session (the caller passes that key;
no key, no hint: a hint never shows one caller another's values), bounded:
the last :data:`CALLS_KEPT` direct live calls of at most
:data:`SESSIONS_KEPT` sessions, in process. The `run` line reads only the
last :data:`RUN_CALLS` of them, as before.
"""

from __future__ import annotations

import collections
import itertools
import json
import threading
from collections.abc import Callable, Collection
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional

__all__ = [
    "CALLS_KEPT", "MAX_CHAIN_SOURCES", "RUN_CALLS", "SESSIONS_KEPT", "note_call",
    "note_live_call",
]

#: Direct live calls remembered per session, and sessions remembered.
CALLS_KEPT = 64
#: How far back the `run` line looks: unchanged since #3666.
RUN_CALLS = 16
SESSIONS_KEPT = 512
#: The fewest calls (this one included) that differ in one argument.
HINT_CALLS = 3
#: Kept answers one chain handle holds at most (the newest).
MAX_CHAIN_SOURCES = 60
#: A value holding one of these is a pattern; its answers may overlap.
WILDCARD_CHARS = ("*", "?", "%")


@dataclass
class _Call:
    node_id: str
    relation: str
    validated: dict[str, Any]
    sent: dict[str, Any]
    #: The kept answer's id; None when the call failed or was not kept.
    handle: Optional[str] = None
    ok: bool = False
    seq: int = 0


@dataclass
class Chain:
    """The kept answers of one hand chain, as the hint memory finds them."""

    argument: str
    #: Kept answers' ids and their values, in call order (at most 60).
    source_ids: list[str]
    values: list[Any]
    #: Values whose calls failed, and values whose answers are gone.
    failed: list[Any] = field(default_factory=list)
    missing: list[Any] = field(default_factory=list)
    #: Values of kept answers older than the newest 60, left out of the handle.
    dropped: list[Any] = field(default_factory=list)
    #: The validated arguments every call shares, and as this call sent them.
    shared: dict[str, Any] = field(default_factory=dict)
    shared_sent: dict[str, Any] = field(default_factory=dict)
    #: This call's number: only a higher one replaces a lower chain handle.
    seq: int = 0


_LOCK = threading.Lock()
_CALLS: "collections.OrderedDict[str, collections.deque[_Call]]" = collections.OrderedDict()
_SEQ = itertools.count(1)


def _same(a: Any, b: Any) -> bool:
    return json.dumps(a, sort_keys=True, default=str) == json.dumps(b, sort_keys=True, default=str)


def _text(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _run_hint(
    earlier: list[_Call], node_id: str, relation: str,
    validated: Mapping[str, Any], sent: Mapping[str, Any],
) -> Optional[dict[str, Any]]:
    """The `run` line: unchanged since #3666, over the last RUN_CALLS calls."""

    mine = [c.validated for c in earlier if c.node_id == node_id and c.relation == relation]
    best: Optional[tuple[str, list[Any]]] = None
    for key in sorted(validated):
        values: list[Any] = []
        for args in mine:
            if set(args) != set(validated) or _same(args.get(key), validated[key]):
                continue
            if all(_same(args[k], validated[k]) for k in validated if k != key):
                if not any(_same(args[key], seen) for seen in values):
                    values.append(args[key])
        values.append(validated[key])
        if len(values) >= HINT_CALLS and (best is None or len(values) > len(best[1])):
            best = (key, values)
    if best is None:
        return None
    key, values = best
    shared = {k: v for k, v in sent.items() if k != key}
    live: dict[str, Any] = {"relation": relation}
    if shared:
        live["args"] = shared
    live["each"] = {key: values}
    return {
        "argument": key,
        "values": len(values),
        "call": {"tool": "run", "arguments": {"steps": [
            {"expand": {"input": {"node_id": node_id}, "live": live}},
        ]}},
    }


def _chain(
    calls: list[_Call], current: _Call, exact: Collection[str],
    held: Callable[[str], Optional[str]],
) -> Optional[Chain]:
    """The chain of kept answers this call extends, or None.

    Only an exact varying argument (a declared string with no enum, no value
    with a wildcard) forms one. A value asked twice keeps its latest kept
    call; failed calls are named, never joined; an earlier answer counts as
    kept when the store still holds it, committed or as a draft of a call
    still being served (`held`); the current call's own answer is a draft
    and counts when it was kept."""

    if not current.ok or current.handle is None:
        return None
    best: Optional[Chain] = None
    for key in sorted(current.validated):
        if key not in exact:
            continue
        members = [
            c for c in calls
            if c.node_id == current.node_id and c.relation == current.relation
            and set(c.validated) == set(current.validated)
            and all(
                _same(c.validated[k], current.validated[k])
                for k in current.validated if k != key
            )
        ]
        if any(
            not isinstance(c.validated[key], str)
            or any(ch in c.validated[key] for ch in WILDCARD_CHARS)
            for c in members
        ):
            continue
        latest: "collections.OrderedDict[str, _Call]" = collections.OrderedDict()
        failed: "collections.OrderedDict[str, Any]" = collections.OrderedDict()
        for c in members:
            text = _text(c.validated[key])
            if c.ok and c.handle is not None:
                latest[text] = c
                failed.pop(text, None)
            elif text not in latest:
                failed[text] = c.validated[key]
        kept: list[_Call] = []
        missing: list[Any] = []
        for c in sorted(latest.values(), key=lambda call: call.seq):
            if c is current or held(str(c.handle)) in ("committed", "pending"):
                kept.append(c)
            else:
                missing.append(c.validated[key])
        if len(kept) < HINT_CALLS:
            continue
        dropped = [c.validated[key] for c in kept[:-MAX_CHAIN_SOURCES]]
        kept = kept[-MAX_CHAIN_SOURCES:]
        found = Chain(
            argument=key,
            source_ids=[str(c.handle) for c in kept],
            values=[c.validated[key] for c in kept],
            failed=list(failed.values()),
            missing=missing,
            dropped=dropped,
            shared={k: v for k, v in current.validated.items() if k != key},
            shared_sent={k: v for k, v in current.sent.items() if k != key},
            seq=current.seq,
        )
        if best is None or len(found.source_ids) > len(best.source_ids):
            best = found
    return best


def note_live_call(
    session_id: str, node_id: str, relation: str,
    validated: Mapping[str, Any], sent: Mapping[str, Any],
    *,
    handle: Optional[str] = None,
    ok: bool = True,
    exact: Collection[str] = (),
    held: Optional[Callable[[str], Optional[str]]] = None,
) -> tuple[Optional[dict[str, Any]], Optional[Chain]]:
    """Remember one direct live call: `(run hint, chain)`, each None when
    it does not qualify.

    `handle` is this call's kept answer (a draft until its reply is served),
    `ok` whether it answered, `exact` the arguments that may form a chain,
    and `held(id)` the store's answer for an earlier call's id."""

    current = _Call(
        node_id=node_id, relation=relation, validated=dict(validated), sent=dict(sent),
        handle=handle, ok=bool(ok),
    )
    with _LOCK:
        calls = _CALLS.get(session_id)
        if calls is None:
            calls = collections.deque(maxlen=CALLS_KEPT)
            _CALLS[session_id] = calls
            while len(_CALLS) > SESSIONS_KEPT:
                _CALLS.popitem(last=False)
        else:
            _CALLS.move_to_end(session_id)
        earlier = list(calls)
        # Drawn under the lock, just before the call joins memory (review M1):
        # a higher number is always a later position, so its chain holds
        # every answer a lower-numbered chain of the same calls holds.
        current.seq = next(_SEQ)
        calls.append(current)
    run = _run_hint(earlier[-RUN_CALLS:], node_id, relation, validated, sent)
    chain = _chain([*earlier, current], current, exact, held or (lambda _id: None))
    return run, chain


def note_call(
    session_id: str, node_id: str, relation: str,
    validated: Mapping[str, Any], sent: Mapping[str, Any],
) -> Optional[dict[str, Any]]:
    """Remember one direct live call; the `run` hint when it qualifies, else None.

    `validated` (defaults filled in) is what calls are compared on; `sent`
    is what the hint repeats, so it reads as the agent wrote it."""

    return note_live_call(session_id, node_id, relation, validated, sent)[0]
