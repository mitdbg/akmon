"""A hand chain of kept live answers, read as one set (PACT read-multi-handle-input).

Agents ask one live relation once per value, call after call, and every
answer is kept (PACT read-live-handles), but no call reads the answers
together: the agent adds 30 numbers by hand. From the third kept answer of
such a chain the reply names ONE chain handle (:class:`LiveChainSet`), which
holds the answers' ids, never their rows. Reading it combines the answers
into the set one ``expand(live={..., "each": {arg: [values]}})`` would have
kept, with every source re-checked as a single handle read is and zero
upstream calls. Nothing combined is stored: each read (and each page)
combines again.

Only one call shape combines (:func:`check_sources`): raw answers of one
relation on one anchor whose validated arguments differ in one argument,
each value once. Rows are never dropped or deduplicated; rows found in more
than one answer are counted (:func:`combine`).
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp import live_result_sets as _lrs

#: Sources one chain holds at most: the per-session count cap (64) minus the
#: chain handle, a replaced chain handle, a groups handle and a derived one.
MAX_SOURCES = 60
#: Values one combined set may list: the fan-out cap it must equal.
MAX_VALUES = 1_500
#: A value holding one of these is a pattern: its answers may overlap.
WILDCARD_CHARS = ("*", "?", "%")
#: Why a chain read is refused (`data.reason`).
REASONS = (
    "typed_set", "groups_handle", "derived_handle", "relation_differs",
    "anchor_differs", "args_differ", "value_twice", "same_call",
    "too_many_values", "too_large", "no_source_kept",
)
#: Why a source counts as gone: the read answers over the rest.
GONE_REASONS = frozenset({"expired", "evicted", "unknown"})

_MESSAGES = {
    "typed_set": "a chain source is a live type's set, which reads many records in one call",
    "groups_handle": "a chain source is an aggregate's groups",
    "derived_handle": "a chain source is a handle derived from another",
    "relation_differs": "the chain's answers come from different relations",
    "anchor_differs": "the chain's answers come from different records",
    "args_differ": "the chain's answers differ in more than one argument",
    "value_twice": "two answers of the chain were asked with the same value",
    "same_call": "two answers of the chain are the same call",
    "too_many_values": f"the chain lists more than {MAX_VALUES:,} values",
    "too_large": (
        f"the combined answers hold more than {_lrs.MAX_ROWS:,} rows or "
        f"{_lrs.MAX_SET_BYTES // (1024 * 1024)} MiB"
    ),
    "no_source_kept": "no answer of the chain is kept any more",
}


class LiveHandlesNotCombinable(SubstrateError):
    """A chain handle whose answers cannot be read as one set."""

    code = "live_handles_not_combinable"


def refuse(reason: str, *, index: int, handles: Sequence[str]) -> LiveHandlesNotCombinable:
    """The refusal for `reason`; `index` is the 1-based source, 0 for the
    whole chain."""

    return LiveHandlesNotCombinable(
        _MESSAGES[reason],
        data={"reason": reason, "index": int(index), "handles": list(handles)},
        fix_hint="read the chain again, or call the missing values again",
    )


@dataclass(frozen=True)
class LiveChainSet:
    """What a chain handle names: its sources' ids, in call order, and the
    call shape they share. No rows."""

    chain_key: str
    seq: int
    source_ids: tuple[str, ...]
    relation: str
    node_id: str
    varying: str
    values: tuple[Any, ...]
    #: The validated arguments every source shares (the varying one removed).
    shared: Mapping[str, Any] = field(default_factory=dict)
    #: Values whose calls failed, and values no longer kept at mint.
    failed: tuple[Any, ...] = ()
    missing: tuple[Any, ...] = ()
    #: Values of kept answers older than the newest 60, not in this chain.
    dropped: tuple[Any, ...] = ()

    @property
    def relation_name(self) -> str:
        return str(self.relation)

    def size(self) -> int:
        """What the store counts: the payload's compact JSON."""

        return len(json.dumps({
            "ids": list(self.source_ids), "values": list(self.values),
            "shared": dict(self.shared), "key": self.chain_key,
        }, default=str, separators=(",", ":")))

    def retry_call(self, value: Any) -> dict[str, Any]:
        """The call that asks the live system for one source again."""

        return _lrs.redo_call(self.node_id, self.relation, {**dict(self.shared), self.varying: value})

    def fan_out_call(self) -> dict[str, Any]:
        """The one call that rebuilds the whole chain, with its upstream calls."""

        call = _lrs.redo_call(
            self.node_id, self.relation, {**dict(self.shared), self.varying: list(self.values)},
            self.varying,
        )
        call["upstream_calls"] = len(self.values)
        return call


def chain_key(
    quota_key: str, node_id: str, relation: str, shared: Mapping[str, Any], varying: str,
) -> str:
    """One chain per (quota key, anchor, relation, shared arguments, varying
    argument): a newer chain handle of the same key replaces the older."""

    text = json.dumps(
        [quota_key, node_id, relation, dict(shared), varying],
        sort_keys=True, default=str, separators=(",", ":"),
    )
    return "chain:" + hashlib.sha256(text.encode("utf-8")).hexdigest()


def exact_value(value: Any) -> bool:
    """A value an exact identifier can take: text with no wildcard."""

    return isinstance(value, str) and not any(ch in value for ch in WILDCARD_CHARS)


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))


def _kind(payload: Any) -> Optional[str]:
    if isinstance(payload, _lrs.LiveGroupsSet):
        return "groups_handle"
    if isinstance(payload, LiveChainSet):
        return "derived_handle"
    if not isinstance(payload, _lrs.RetainedSet):
        return "typed_set"
    if payload.derived_from is not None:
        return "derived_handle"
    return None


def source_values(payload: Any, varying: str) -> list[Any]:
    """The varying argument's values one source was asked with."""

    query = dict(getattr(payload, "query", None) or {})
    if getattr(payload, "each", None) == varying and isinstance(query.get(varying), list):
        return list(query[varying])
    return [query.get(varying)]


def check_sources(
    chain: LiveChainSet, opened: Sequence[tuple[int, str, Any]], *, chain_id: str = "",
) -> None:
    """Refuse, by name and position, sources that do not form one fan-out.

    `opened` is `(index, id, payload)` per kept source, 1-based, in chain
    order. Checked again at read time, as a guard: the chain was built at
    mint from the calls themselves. `handles` in a refusal names the ids it
    is about: the source (and the earlier one it repeats), or the chain."""

    whole = [chain_id] if chain_id else []
    for index, source_id, payload in opened:
        kind = _kind(payload)
        if kind is not None:
            raise refuse(kind, index=index, handles=[source_id])
    shared_text: Optional[str] = None
    calls: dict[str, str] = {}
    values: dict[str, str] = {}
    listed = 0
    for index, source_id, payload in opened:
        if payload.relation_name != chain.relation:
            raise refuse("relation_differs", index=index, handles=[source_id])
        if str(payload.node_id) != str(chain.node_id):
            raise refuse("anchor_differs", index=index, handles=[source_id])
        query = dict(payload.query or {})
        if chain.varying not in query:
            raise refuse("args_differ", index=index, handles=[source_id])
        shared = _canonical({k: v for k, v in query.items() if k != chain.varying})
        if shared_text is None:
            shared_text = shared
        elif shared != shared_text:
            raise refuse("args_differ", index=index, handles=[source_id])
        call = _canonical(query)
        if call in calls:
            raise refuse("same_call", index=index, handles=[calls[call], source_id])
        calls[call] = source_id
        for value in source_values(payload, chain.varying):
            text = _canonical(value)
            if text in values:
                raise refuse("value_twice", index=index, handles=[values[text], source_id])
            values[text] = source_id
            listed += 1
    if listed > MAX_VALUES:
        raise refuse("too_many_values", index=0, handles=whole)


@dataclass
class Combined:
    """One read of a chain: the combined set, its rows and its reply block."""

    retained: _lrs.RetainedSet
    rows: list[dict[str, Any]]
    block: dict[str, Any]


def _clock_text(stamp: str) -> str:
    try:
        moment = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
    except ValueError:
        return str(stamp)
    return moment.strftime("%H:%M:%S")


def read_window(observed: Sequence[str]) -> Optional[str]:
    """`first-last` read times of the sources, as the reply prints them."""

    stamps = sorted(str(s) for s in observed if s)
    if not stamps:
        return None
    first, last = stamps[0], stamps[-1]
    if first[:10] == last[:10]:
        return f"{_clock_text(first)}-{_clock_text(last)}Z"
    return f"{first}-{last}"


def combine(
    chain: LiveChainSet,
    chain_id: str,
    opened: Sequence[tuple[int, str, Any, Sequence[Any]]],
    gone: Sequence[tuple[int, Any, str]],
    *,
    failed: Sequence[Any] = (),
) -> Combined:
    """The combined set of the kept sources.

    `opened` is `(index, id, payload, rows)` per kept source; `gone` is
    `(index, value, reason)` per source no longer kept, which the read
    answers over and names. Rows come in chain order, upstream order within
    a source; a row lacking the varying argument gets it as its first field,
    as a fan-out's rows do. Rows whose upstream JSON (the varying key
    removed) appears in two or more answers are counted, never dropped.
    """

    whole = [chain_id]
    if not opened:
        raise refuse("no_source_kept", index=0, handles=whole)
    check_sources(chain, [(i, s, p) for i, s, p, _rows in opened], chain_id=chain_id)
    key = chain.varying
    rows: list[dict[str, Any]] = []
    seen: dict[str, set[int]] = {}
    per_value: list[dict[str, Any]] = []
    for index, _source_id, payload, source_rows in opened:
        query = dict(payload.query or {})
        fanned = payload.each == key and isinstance(query.get(key), list)
        if fanned:
            per_value += [{**query, key: value} for value in query[key]]
        else:
            per_value.append(query)
        value = query.get(key)
        for row in source_rows:
            if isinstance(row, Mapping):
                upstream = {k: v for k, v in row.items() if k != key}
                out = dict(row) if key in row else {key: value, **row}
            else:
                upstream, out = row, row
            digest = hashlib.sha1(_canonical(upstream).encode("utf-8")).hexdigest()
            seen.setdefault(digest, set()).add(index)
            rows.append(out)
    if len(rows) > _lrs.MAX_ROWS:
        raise refuse("too_large", index=0, handles=whole)
    blob = _lrs.encode_rows(rows)
    if len(blob) > _lrs.MAX_SET_BYTES:
        raise refuse("too_large", index=0, handles=whole)
    from okg.substrate.external_backed.live_dispatch import (
        canonical_args_fingerprint,
        each_query,
    )

    first = opened[0][2]
    observed = [str(p.observed_at or "") for _i, _s, p, _r in opened]
    query = each_query(key, per_value)
    # The external system's cut, summed over the sources in upstream calls
    # (PACT live-kept-answer-cut): a direct call is 1 of 1, a fan-out its
    # own counts; an uncut source adds its calls and no cut one.
    calls_cut = calls = 0
    for _index, _source_id, payload, _rows in opened:
        cut = getattr(payload, "upstream_cut", None)
        if cut:
            calls_cut += int(cut["calls_cut"])
            calls += int(cut["calls"])
        else:
            calls += len(source_values(payload, key))
    upstream_cut = (
        {"rows": len(rows), "calls_cut": calls_cut, "calls": calls} if calls_cut else None
    )
    retained = _lrs.RetainedSet(
        rows_json=blob,
        row_count=len(rows),
        relation=first.relation,
        node_id=str(first.node_id),
        subject=first.subject,
        args_fingerprint=canonical_args_fingerprint(query),
        observed_at=min(s for s in observed if s) if any(observed) else "",
        source=first.source,
        deployment_name=first.deployment_name,
        session_id=first.session_id,
        digest="sha256:" + hashlib.sha256(blob).hexdigest(),
        rows_returned=len(rows),
        query=query,
        each=key,
        read_window=read_window(observed),
        upstream_cut=upstream_cut,
    )
    in_several = sum(1 for indexes in seen.values() if len(indexes) > 1)
    total = len(opened) + len(gone)
    block: dict[str, Any] = {
        "chain": chain_id,
        "answers": len(opened),
        "of": total,
        "rows": len(rows),
        "rows_in_several_answers": in_several,
        "varying": key,
        "failed": list(failed),
        "read": retained.read_window,
        "upstream_calls_cut": calls_cut,
    }
    # Answers the chain never held (review M4, L1) are counted and named too:
    # gone before the chain was minted, or kept but older than its 60.
    before = [(0, value, "gone_before_chain") for value in chain.missing]
    older = [(0, value, "not_in_chain") for value in chain.dropped]
    unread = [*gone, *before, *older]
    if unread:
        block["not_kept"] = [
            {"index": index, "value": value, "reason": reason,
             "retry": chain.retry_call(value)}
            for index, value, reason in unread
        ]
        block["of"] = len(opened) + len(unread)
    return Combined(retained=retained, rows=rows, block=block)


#: The metric field the hint leaves to the agent: rows of one relation
#: often carry five or more numeric columns (open pick 2).
FIELD_PLACEHOLDER = "attrs.<field>"


def chain_hint(chain_id: str, chain: LiveChainSet) -> dict[str, Any]:
    """`external_live.chain_hint`: the chain handle and the two calls that
    read it, a total and a ranking (8.4)."""

    total = {"tool": "aggregate", "arguments": {
        "input": chain_id, "by": [], "metrics": [f"sum({FIELD_PLACEHOLDER})"],
    }}
    rank = {"tool": "aggregate", "arguments": {
        "input": chain_id, "by": [f"attrs.{chain.varying}"],
        "metrics": [f"max({FIELD_PLACEHOLDER})"],
        "order_by": f"max({FIELD_PLACEHOLDER}) desc", "limit": 5,
    }}
    return {
        "chain": chain_id,
        "answers": len(chain.source_ids),
        "argument": chain.varying,
        "failed": list(chain.failed),
        "missing": list(chain.missing),
        "dropped": list(chain.dropped),
        "calls": [total, rank],
    }


def value_text(key: str, values: Sequence[Any], *, shown: int = 3) -> str:
    """`q=i03` or `q=i03, i07 and 2 more`, upstream values wrapped as text."""

    names = [v if isinstance(v, str) else json.dumps(v, default=str) for v in values]
    head = ", ".join(names[:shown])
    more = len(names) - shown
    return f"{key}={head}" + (f" and {more} more" if more > 0 else "")


__all__ = [
    "Combined",
    "GONE_REASONS",
    "LiveChainSet",
    "LiveHandlesNotCombinable",
    "MAX_SOURCES",
    "MAX_VALUES",
    "REASONS",
    "WILDCARD_CHARS",
    "chain_hint",
    "chain_key",
    "check_sources",
    "combine",
    "exact_value",
    "read_window",
    "refuse",
    "source_values",
    "value_text",
]
