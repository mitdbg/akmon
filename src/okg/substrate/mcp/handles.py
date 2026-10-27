"""Live handles: the parts every live set read shares (PACT read-live-handles).

A handle is the name of a set the server holds for one reader: every live set
read returns one, and the next call reads it with ``input="<handle>"``. The
store is :mod:`okg.substrate.mcp.live_result_sets` (``l1.`` ids); this module
holds what sits around it:

- :func:`quota_key`: what the caps count by, the reader plus okg's logical
  session, or the reader alone on a stateless transport (M1);
- :class:`DecodeBudget`: the bound on rows decoded from handles, per MCP call
  and per process, refused at once by name (4.1.5);
- the producing calls a handle is remembered with, and the retry hint a dead
  handle names (3.6);
- the ``handle`` block of a reply (3.7).
"""

from __future__ import annotations

import threading
from collections.abc import Mapping, Sequence
from typing import Any, Optional

from okg.substrate.errors import SubstrateError

# -- quota key ---------------------------------------------------------------


def quota_key(reader_key: str, logical_session: Optional[str], stateless: bool) -> str:
    """The reader plus its logical session; the reader alone on stateless
    HTTP or when the transport gives no session."""

    if stateless or not logical_session:
        return str(reader_key)
    return f"{reader_key}\x00{logical_session}"


# -- the decode budget ---------------------------------------------------------

#: Compact JSON bytes of handles one MCP call may decode together: half the
#: process bound, so one call never takes the whole budget (review F6). At
#: the measured ratios (4.04 typed rows, 6.57 raw rows) 16 MiB compact is
#: about 65-105 MiB of Python objects.
PER_CALL_BYTES = 16 * 1024 * 1024
#: Compact JSON bytes of handles decoded by calls in flight, per process.
PER_PROCESS_BYTES = 32 * 1024 * 1024


class LiveDecodeCapacity(SubstrateError):
    """A read would decode more handle rows than the call or the process holds."""

    code = "live_decode_capacity"


class DecodeBudget:
    """Bytes of decoded handle rows, reserved per call and per process.

    ``reserve`` refuses at once past either bound: the caller runs on a call
    worker of a bounded pool, and a wait would hold that worker. A
    reservation is given back with ``release`` when the call that made it
    ends.
    """

    def __init__(
        self, *, per_call_bytes: int = PER_CALL_BYTES,
        per_process_bytes: int = PER_PROCESS_BYTES,
    ) -> None:
        self.per_call_bytes = per_call_bytes
        self.per_process_bytes = per_process_bytes
        self._lock = threading.Lock()
        self._held: dict[int, int] = {}
        self._next = 0

    def in_flight(self) -> int:
        with self._lock:
            return sum(self._held.values())

    def reserve(self, nbytes: int, *, call_bytes: int = 0) -> int:
        """A token for `nbytes` more, beside the `call_bytes` this call holds."""

        nbytes = max(0, int(nbytes))
        if call_bytes + nbytes > self.per_call_bytes:
            raise _capacity("call", nbytes, call_bytes, self.per_call_bytes)
        with self._lock:
            held = sum(self._held.values())
            if held + nbytes > self.per_process_bytes:
                raise _capacity("server", nbytes, None, self.per_process_bytes)
            self._next += 1
            self._held[self._next] = nbytes
            return self._next

    def release(self, token: int) -> None:
        with self._lock:
            self._held.pop(token, None)


def _capacity(scope: str, nbytes: int, call_bytes: Optional[int], limit: int) -> LiveDecodeCapacity:
    where = "this call's handles" if scope == "call" else "the handles calls in flight read"
    data: dict[str, Any] = {"scope": scope, "bytes": nbytes, "limit": limit}
    if call_bytes is not None:
        data["call_bytes"] = call_bytes
    return LiveDecodeCapacity(
        f"reading this handle would decode {nbytes:,} bytes of rows; {where} "
        f"hold at most {limit:,}, so the read is refused before any row is served",
        data=data,
        fix_hint="narrow the set, or retry",
    )


decode_budget = DecodeBudget()


# -- the calls a handle is remembered with (3.6) -------------------------------

#: Arguments a remembered call never carries: the caller's own authority,
#: and what only this server put on the call.
_DROPPED = frozenset({"authority", "max_bytes", "next"})


def producing_call(
    tool: str, arguments: Mapping[str, Any], *, upstream_calls: Optional[int] = None,
) -> dict[str, Any]:
    """`{"tool", "arguments"}` of the call that made a set, `authority`
    removed, with the upstream calls it made when known (a live retry makes
    them again)."""

    call: dict[str, Any] = {
        "tool": str(tool),
        "arguments": {
            k: v for k, v in dict(arguments).items()
            if k not in _DROPPED and v is not None
        },
    }
    if upstream_calls is not None:
        call["upstream_calls"] = int(upstream_calls)
    return call


def remember(
    set_id: str, reader_key: str, call: Mapping[str, Any], *, parent: Optional[str] = None,
) -> None:
    """Remember the calls that made `set_id`: its parent's, then `call`."""

    from okg.substrate.mcp import live_result_sets as _lrs

    if parent is None:
        _lrs.remember_recipe(set_id, reader_key, [call])
        return
    known, recipe = _lrs.remembered_recipe(parent, reader_key)
    _lrs.remember_recipe(
        set_id, reader_key, [*recipe, call] if known and recipe is not None else None,
    )


def retry_hint(set_id: str, reader_key: str) -> tuple[str, Optional[dict[str, Any]]]:
    """`(fix_hint, retry_call)` for this reader's expired or evicted handle.

    One producing call: that call, `authority` removed, and how many upstream
    calls it made when it says. A chain: "run the read that made it again",
    with the call that started it (3a-i; the one-`run` form comes with run
    handles). Nothing remembered: said so.
    """

    from okg.substrate.mcp import live_result_sets as _lrs
    from okg.substrate.mcp.result_text import call_text

    known, recipe = _lrs.remembered_recipe(set_id, reader_key)
    if not known:
        return (
            "run the read that made the handle again; the call that made it "
            "is no longer remembered",
            None,
        )
    if not recipe:
        return ("run the read that made the handle again", None)
    first = recipe[0]
    written = call_text(first)
    if written is None:
        return ("run the read that made the handle again", None)
    calls = first.get("upstream_calls")
    cost = (
        f"; it asks the live system again ({int(calls)} upstream "
        f"call{'s' if int(calls) != 1 else ''} last time)"
        if isinstance(calls, int) and not isinstance(calls, bool) else
        "; it asks the live system again"
    )
    retry = {"tool": first["tool"], "arguments": dict(first["arguments"])}
    if len(recipe) == 1:
        return (f"retry: {written}{cost}", retry)
    return (
        f"run the read that made the handle again: it began with {written}, "
        f"then {len(recipe) - 1} more call{'s' if len(recipe) != 2 else ''} "
        f"on its handle{cost}",
        retry,
    )


def replaced_retry(data: Mapping[str, Any]) -> tuple[str, Optional[dict[str, Any]]]:
    """`(fix_hint, retry_call)` for a chain handle a newer one replaced
    (PACT read-multi-handle-input, N2): read the newer id, which holds these
    answers, with no upstream call."""

    newer = data.get("replaced_by")
    if not isinstance(newer, str) or not newer:
        return ("run the read that made the handle again", None)
    return (
        f'read input="{newer}" instead: it holds these answers; no new upstream calls',
        {"tool": "filter", "arguments": {"input": newer}},
    )


# -- the reply's handle block (3.7) --------------------------------------------

#: Columns a handle block lists at most.
MAX_COLUMNS_SHOWN = 12


def handle_block(
    *,
    set_id: str,
    rows: int,
    columns: Sequence[str],
    order: str,
    expires_at: float,
    read_window: Optional[str] = None,
    from_id: Optional[str] = None,
    stateless: bool = False,
) -> dict[str, Any]:
    """`handle` of a reply that made or names a live handle."""

    from okg.substrate.mcp import result_references as _rr

    names = list(dict.fromkeys(str(c) for c in columns))
    block: dict[str, Any] = {
        "id": set_id,
        "rows": int(rows),
        "columns": names[:MAX_COLUMNS_SHOWN],
        "columns_more": max(0, len(names) - MAX_COLUMNS_SHOWN),
        "order": order,
        "pins": {"read": read_window} if read_window else {},
        "expires_at": _rr.expires_at_text(expires_at),
    }
    if from_id is not None:
        block["from"] = from_id
    if stateless:
        block["scope"] = "reader (stateless transport)"
    return block


def columns_of(payload: Any, rows: Sequence[Any]) -> list[str]:
    """A set's columns as a reply names them: `node_id` and `subtype` on
    typed rows, then each upstream key as `attrs.<key>`, neutralised as the
    rows serve it; a groups handle's own columns."""

    from okg.substrate.mcp import live_reader as _reader
    from okg.substrate.mcp import live_result_sets as _lrs

    if isinstance(payload, _lrs.LiveGroupsSet):
        return list(payload.columns)
    keys: dict[str, None] = {}
    typed = bool(rows) and all(_reader.is_typed(row) for row in rows)
    for row in rows:
        if isinstance(row, Mapping):
            for name in _reader.attrs_of(row):
                keys[str(name)] = None
    names = [f"attrs.{k}" for k in _reader.served_key_names(keys)]
    return (["node_id", "subtype"] if typed else []) + names


def combined_block(block: Mapping[str, Any]) -> dict[str, Any]:
    """`combined` of a chain read's reply (PACT read-multi-handle-input,
    6.3): the chain id, the answers read and of how many, rows, rows in more
    than one answer, the varying argument, failed calls, the read window,
    and each answer no longer kept with the call that asks it again."""

    out = {
        key: block[key] for key in (
            "chain", "answers", "of", "rows", "rows_in_several_answers", "varying",
            "failed", "read", "upstream_calls_cut",
        ) if key in block
    }
    if block.get("not_kept"):
        out["not_kept"] = [dict(item) for item in block["not_kept"]]
    return out


def no_handle(reason: str) -> dict[str, Any]:
    """The reply fields of a set read that made no handle."""

    return {"handle": None, "handle_reason": reason}


def expires_of(set_id: str) -> float:
    """A handle id's own expiry (the third dotted part)."""

    return float(int(str(set_id).split(".")[2]))


__all__ = [
    "DecodeBudget",
    "LiveDecodeCapacity",
    "PER_CALL_BYTES",
    "PER_PROCESS_BYTES",
    "combined_block",
    "decode_budget",
    "expires_of",
    "handle_block",
    "no_handle",
    "producing_call",
    "quota_key",
    "remember",
    "replaced_retry",
    "retry_hint",
]
