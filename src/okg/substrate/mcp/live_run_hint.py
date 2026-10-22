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

Memory is per reader and logical MCP session (the caller passes that key;
no key, no hint: a hint never shows one caller another's values), bounded:
the last :data:`CALLS_KEPT` direct live calls of at most
:data:`SESSIONS_KEPT` sessions, in process.
"""

from __future__ import annotations

import collections
import json
import threading
from typing import Any, Mapping, Optional

__all__ = ["CALLS_KEPT", "SESSIONS_KEPT", "note_call"]

#: Direct live calls remembered per session, and sessions remembered.
CALLS_KEPT = 16
SESSIONS_KEPT = 512
#: The fewest calls (this one included) that differ in one argument.
HINT_CALLS = 3

_LOCK = threading.Lock()
_CALLS: "collections.OrderedDict[str, collections.deque[tuple[str, str, dict[str, Any], dict[str, Any]]]]" = (
    collections.OrderedDict()
)


def _same(a: Any, b: Any) -> bool:
    return json.dumps(a, sort_keys=True, default=str) == json.dumps(b, sort_keys=True, default=str)


def note_call(
    session_id: str, node_id: str, relation: str,
    validated: Mapping[str, Any], sent: Mapping[str, Any],
) -> Optional[dict[str, Any]]:
    """Remember one direct live call; the hint when it qualifies, else None.

    `validated` (defaults filled in) is what calls are compared on; `sent`
    is what the hint repeats, so it reads as the agent wrote it."""
    with _LOCK:
        calls = _CALLS.get(session_id)
        if calls is None:
            calls = collections.deque(maxlen=CALLS_KEPT)
            _CALLS[session_id] = calls
            while len(_CALLS) > SESSIONS_KEPT:
                _CALLS.popitem(last=False)
        else:
            _CALLS.move_to_end(session_id)
        earlier = [
            (args, given) for anchor, name, args, given in calls
            if anchor == node_id and name == relation
        ]
        calls.append((node_id, relation, dict(validated), dict(sent)))
    best: Optional[tuple[str, list[Any]]] = None
    for key in sorted(validated):
        values: list[Any] = []
        for args, _given in earlier:
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
