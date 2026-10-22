"""Retained live answers: one large live result, read again without the adapter.

``expand(live={..., "retain": true})`` keeps the full row set of one live
call whose state is ``ok`` for 300 seconds, under a signed random id, for
the reader (and, on a multi-user server, the principal) that made the call.
``filter``, ``map`` and ``aggregate`` read it with ``input={"ref": id}``.
A live answer CUT to fit its reply is kept the same way without being
asked, and its reply names one ``expand(next="<token>")`` call that pages
the kept rows (#3666, Live build 3; see ``next_token``).

It is not a cache. A set is found only by its id, never by relation, node or
arguments: two identical retained calls are two adapter calls and two ids.
Every read re-checks the anchor node's visibility, the binding (authority,
Nomos policy pin, catalog version) and the live gate, and any refusal drops
the set. Every read carries ``served_from: "retained_answer"`` with the
original ``observed_at`` and ``digest``.

Storage is a sibling of the graph result-reference store
(:mod:`okg.substrate.mcp.result_references`) with its own id version
(``l1``), its own MAC label and its own limits: 300 s, 50,000 rows and
8 MiB per set, 4 sets per reader and per principal, 16 sets and 64 MiB per
server. A set is stored once, as compact JSON of the UNWRAPPED rows in
upstream row and key order; the caps count those bytes. Key order is kept
because the untrusted-text wrapping renames colliding keys in order, and
each served page is wrapped again with that same wrapping.

Server capacity is reserved when the set is drafted and released if the
served response does not carry its id. The original call's sample row
records ``retained: true`` before the response is built; in the one path
where the response is then replaced by a refusal after the live step (a
measured size that disagrees with itself), that row over-reports a set that
was never stored. This is a known, accepted residual.
"""

from __future__ import annotations

import collections
import json
import math
import re
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Optional

from okg.substrate.errors import SubstrateError
from okg.substrate.external_backed.call_sample import RETAINED_ANSWER_READ
from okg.substrate.mcp import result_references as _rr

#: Every live id starts with this; a graph reference id starts with ``r1.``.
ID_VERSION = "l1"
ID_PREFIX = ID_VERSION + "."
LIFETIME_SECONDS = 300
MAX_ROWS = 50_000
MAX_SET_BYTES = 8 * 1024 * 1024
MAX_PER_READER = 4
MAX_PER_PRINCIPAL = 4
MAX_PER_SERVER = 16
MAX_TOTAL_BYTES = 64 * 1024 * 1024
#: Groups one aggregate page serves at most.
MAX_GROUPS = 50
MAX_BY_FIELDS = 4
FIX_HINT = "call expand(live=..., retain=true) again"
#: One owner: the sample module, whose call counts exclude these reads.
SERVED_FROM = RETAINED_ANSWER_READ

LIMITS = _rr.StoreLimits(
    id_version=ID_VERSION,
    mac_label=b"okg/live-result/v1",
    lifetime_seconds=LIFETIME_SECONDS,
    max_members=MAX_ROWS,
    max_reference_bytes=MAX_SET_BYTES,
    max_per_reader=MAX_PER_READER,
    max_per_principal=MAX_PER_PRINCIPAL,
    max_per_server=MAX_PER_SERVER,
    max_total_bytes=MAX_TOTAL_BYTES,
    copy_on_lookup=False,
    reserve_on_mint=True,
    fix_hint=FIX_HINT,
)

#: Why a requested set was not kept, as `retained: false` reports it.
NOT_KEPT_REASONS = (
    "too_large", "state_not_ok", "rows_not_a_list", "rows_not_objects", "capacity",
)

#: Predicate operators a read accepts: the graph `filter` grammar. `contains`
#: is text inside one field only, never JSON containment on the whole attrs
#: object (`_check_text_needle`).
_LEAF_OPS = frozenset({
    "eq", "in", "exists", "lt", "gt", "between", "prefix", "suffix", "contains",
})
#: The record columns of a live typed row (`{node_id, subtype, attrs, live}`),
#: next to `attrs.<field>`. Internally a group or metric key for one of them
#: starts with a NUL, so it can never be an upstream field's name.
RECORD_COLUMNS = ("node_id", "subtype")
_RECORD_KEY = "\x00"


class LiveResultDenied(SubstrateError):
    """The live gate did not allow a read of a retained live answer."""

    code = "live_result_denied"


class LiveResultPredicateUnsupported(SubstrateError):
    """A read of a retained live answer used an argument it does not accept."""

    code = "live_result_predicate_unsupported"


def new_store(**kwargs: Any) -> _rr.ResultReferenceStore:
    """A live result-set store (a test seam takes ``clock``)."""

    return _rr.ResultReferenceStore(limits=LIMITS, **kwargs)


_STORE_LOCK = threading.Lock()
_STORE = new_store()


def live_store() -> _rr.ResultReferenceStore:
    """This process's live result-set store."""

    with _STORE_LOCK:
        return _STORE


def install_live_store(replacement: _rr.ResultReferenceStore) -> _rr.ResultReferenceStore:
    """Replace the process's live store and return the previous one (a test seam)."""

    global _STORE
    with _STORE_LOCK:
        previous, _STORE = _STORE, replacement
    return previous


def is_live_id(value: Any) -> bool:
    """Whether a `{"ref": ...}` value names a live set (by its version prefix)."""

    return isinstance(value, str) and value.startswith(ID_PREFIX)


# -- capture ------------------------------------------------------------------


class NotRetained(Exception):
    """A requested set that is not kept; `reason` is one of NOT_KEPT_REASONS."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def encode_rows(rows: Sequence[Any]) -> bytes:
    """Compact JSON of the unwrapped rows in upstream row and key order."""

    return json.dumps(
        list(rows), ensure_ascii=False, separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")


def decode_rows(blob: bytes) -> list[dict[str, Any]]:
    return json.loads(blob)


def capture(raw: Any) -> tuple[bytes, int]:
    """The stored form of an adapter's answer, or why it is not kept.

    Only a list of JSON objects is kept, and only one that round-trips
    through JSON unchanged (no NaN, no non-JSON value), so the stored rows
    wrap to the digest the caller was given.
    """

    if not isinstance(raw, list):
        raise NotRetained("rows_not_a_list")
    if not all(isinstance(row, dict) for row in raw):
        raise NotRetained("rows_not_objects")
    if len(raw) > MAX_ROWS:
        raise NotRetained("too_large")
    try:
        blob = encode_rows(raw)
    except (TypeError, ValueError):
        raise NotRetained("rows_not_objects") from None
    if json.loads(blob) != raw:
        # A key that is not a string, or a tuple: it would not wrap back to
        # the same rows, so the digest could not be honoured.
        raise NotRetained("rows_not_objects")
    if len(blob) > MAX_SET_BYTES:
        raise NotRetained("too_large")
    return blob, len(raw)


def fence_rows(rows: Any, relation: str) -> Any:
    """Wrap rows as untrusted text exactly as the original live envelope did."""

    from okg.substrate.external_backed.resolver import _fence_payload

    return _fence_payload(rows, source=f"external_live:{relation}", fence_keys=True)


def served_node_id(node_id: Any, relation: str, *, checked: bool) -> Any:
    """A live node id as served: plain when its name passed the type's
    anchored ``name_pattern`` (a checked identifier), wrapped as untrusted
    text otherwise, since it is then an upstream name as the system sent
    it."""

    return node_id if checked else fence_rows(node_id, relation)


def _neutral_keys(keys: Any, taken: Mapping[str, str]) -> dict[str, str]:
    """raw key -> served key for upstream keys, neutralised as
    :func:`fence_rows` neutralises keys; keys already in `taken` keep their
    served name, so a field's state and its value share one key."""

    from okg.substrate.external_backed.resolver import _neutralise_key

    out: dict[str, str] = {}
    used = set(taken.values())
    for key in keys:
        raw = str(key)
        if raw in taken:
            out[raw] = taken[raw]
            continue
        safe = _neutralise_key(raw)
        if safe in used:
            suffix = 2
            while f"{safe}~{suffix}" in used:
                suffix += 1
            safe = f"{safe}~{suffix}"
        used.add(safe)
        out[raw] = safe
    return out


def served_live(
    live: Mapping[str, Any], served_keys: Optional[Mapping[str, str]] = None,
) -> dict[str, Any]:
    """A live typed row's ``live`` block as served: every upstream key in
    ``fields`` (and in ``field_observed_at``) neutralised as the row's
    ``attrs`` keys are. `served_keys` is the row's own raw -> served map for
    its attrs keys, so a key keeps one served name in both places."""

    taken = dict(served_keys or {})
    out = dict(live)
    for name in ("fields", "field_observed_at"):
        block = live.get(name)
        if isinstance(block, Mapping):
            keys = _neutral_keys(block, taken)
            taken.update(keys)
            out[name] = {keys[str(k)]: v for k, v in block.items()}
    return out


# -- the retained set ---------------------------------------------------------


@dataclass
class RetainedSet:
    """What a live id names. Filled once, before the draft can be committed."""

    rows_json: bytes
    row_count: int
    #: The `LiveRelation` the expand gated, asked again on every read.
    relation: Any
    node_id: str
    #: The resolved subject the expand's gate was asked about ("" if none).
    subject: str
    args_fingerprint: str
    observed_at: str
    source: str
    deployment_name: str
    session_id: str
    digest: str = ""
    rows_returned: int = 0
    refetch: Any = None
    #: The validated arguments of the call that made the set (its redo call).
    query: Optional[dict[str, Any]] = None
    #: The argument a fanned-out call listed values of, and, when the values
    #: came from a kept listing, that listing's relation and arguments.
    each: Optional[str] = None
    each_from: Optional[dict[str, Any]] = None

    @property
    def relation_name(self) -> str:
        return str(self.relation.name)


def reference_block(
    *, set_id: str, expires_at: float, rows: int, digest: str, observed_at: str,
) -> dict[str, Any]:
    """`external_live.result_reference` for a kept set."""

    return {
        "retained": True,
        "id": set_id,
        "expires_at": _rr.expires_at_text(expires_at),
        "rows": rows,
        "digest": digest,
        "observed_at": observed_at,
        "read_with": {"tools": ["filter", "map", "aggregate"], "input": {"ref": set_id}},
    }


def not_kept_block(reason: str) -> dict[str, Any]:
    return {"retained": False, "reason": reason}


#: Fixed-width stand-ins: the reserve and the fit measure a block before
#: the real digest is known; a real one has the same length.
PLACEHOLDER_DIGEST = "sha256:" + "0" * 64


def largest_block(observed_at: str) -> dict[str, Any]:
    """The largest block a call can carry: sizes the response reserve."""

    set_id = ".".join([
        ID_VERSION, "e" * 8, "9" * 12, "n" * 16, "m" * 22,
    ])
    return reference_block(
        set_id=set_id, expires_at=float(10**12 - 1), rows=10**9,
        digest=PLACEHOLDER_DIGEST, observed_at=observed_at,
    )


# -- the next token of a cut live answer (#3666, Live build 3) ---------------
#
# A live answer cut to fit its reply is kept here, as a retained set, and the
# reply names ONE call that goes on: `expand(next="<token>")`. The token is
# the set id, the first row the next page serves (0-based) and a MAC over
# both. The set id is in it verbatim, so the served reply carries the id
# (that is what commits the kept set; `result_references.ServeScope`), and
# the store's own checks (expiry, reader, eviction) answer for the token.

#: The MAC label of a next token's offset; distinct from the set id's own.
NEXT_MAC_LABEL = b"okg/live-next/v1"
_NEXT_MAC_CHARS = 22
#: Kept answers whose redo call is remembered after they are gone, so an
#: expired or evicted token's refusal can name the call that asks again.
MAX_REMEMBERED_CALLS = 1024


def next_token(set_id: str, offset: int) -> str:
    """The `next` token that serves the kept set `set_id` from row `offset`."""

    prefix = f"{set_id}.{int(offset)}"
    return f"{prefix}.{_rr._mac(prefix, NEXT_MAC_LABEL)}"


def next_token_placeholder(set_id: str, total_rows: int) -> str:
    """A token at least as long as any `next_token(set_id, n)` for n <= total.

    Fitting measures a reply before the next row is known; the real token is
    never longer than this one."""

    return f"{set_id}.{'9' * len(str(max(int(total_rows), 0)))}.{'m' * _NEXT_MAC_CHARS}"


def largest_next_token() -> str:
    """The longest next token any call can carry: sizes the response reserve."""

    return next_token_placeholder(str(largest_block("")["id"]), MAX_ROWS)


def is_next_token(value: Any) -> bool:
    """Whether a `next` value is a live next token (by its form only)."""

    return (
        isinstance(value, str) and value.startswith(ID_PREFIX)
        and value.count(".") == 6 and len(value) <= 160
    )


def parse_next_token(token: str) -> tuple[str, int]:
    """`(set_id, offset)` of a next token whose MAC holds, or a refusal."""

    from okg.substrate.mcp.continuation import ContinuationTokenInvalid

    parts = token.split(".") if isinstance(token, str) else []
    if len(parts) != 7 or not parts[5].isdigit():
        raise ContinuationTokenInvalid(
            "the next token is malformed", data={"reason": "malformed"},
            fix_hint="pass the next token exactly as the reply printed it",
        )
    set_id, offset, mac = ".".join(parts[:5]), parts[5], parts[6]
    import hmac as _hmac

    if not _hmac.compare_digest(mac, _rr._mac(f"{set_id}.{offset}", NEXT_MAC_LABEL)):
        raise ContinuationTokenInvalid(
            "the next token signature is invalid", data={"reason": "signature_invalid"},
            fix_hint="pass the next token exactly as the reply printed it",
        )
    return set_id, int(offset)


def next_set_id(token: Any) -> Optional[str]:
    """The set id a next token names, unverified (for the served-id check)."""

    if not is_next_token(token):
        return None
    return ".".join(str(token).split(".")[:5])


_CALLS_LOCK = threading.Lock()
#: set id -> (reader key, the call that asks the live system again).
_REDO_CALLS: "collections.OrderedDict[str, tuple[str, dict[str, Any]]]" = (
    collections.OrderedDict()
)


def redo_call(
    node_id: str, relation: str, query: Mapping[str, Any], each: Optional[str] = None,
) -> dict[str, Any]:
    """The published call that asks the live system for this answer again.
    `each` names the argument a fanned-out call listed values of."""

    live: dict[str, Any] = {"relation": relation}
    args = dict(query or {})
    if each is not None and each in args:
        live["each"] = {each: args.pop(each)}
    if args:
        live["args"] = args
    return {"tool": "expand", "arguments": {"input": node_id, "live": live}}


def remember_call(set_id: str, reader_key: str, call: Mapping[str, Any]) -> None:
    """Remember a kept answer's redo call past its set's life (bounded)."""

    with _CALLS_LOCK:
        _REDO_CALLS[set_id] = (reader_key, dict(call))
        _REDO_CALLS.move_to_end(set_id)
        while len(_REDO_CALLS) > MAX_REMEMBERED_CALLS:
            _REDO_CALLS.popitem(last=False)


def remembered_call(set_id: str, reader_key: str) -> Optional[dict[str, Any]]:
    """The redo call remembered for `set_id`, for the reader that made it."""

    import hmac as _hmac

    with _CALLS_LOCK:
        found = _REDO_CALLS.get(set_id)
    if found is None or not _hmac.compare_digest(found[0], reader_key):
        return None
    return dict(found[1])


#: Why a next token's kept answer cannot be read, in the reader's words.
NEXT_GONE_MESSAGES = {
    "expired": (
        f"the kept live answer this next token pages has expired (an answer "
        f"is kept {LIFETIME_SECONDS} seconds)"
    ),
    "evicted": (
        f"the kept live answer this next token pages was dropped for newer "
        f"ones (a reader keeps {MAX_PER_READER})"
    ),
    "store_lost": (
        "the kept live answer this next token pages is gone: the server "
        "restarted, or another server process answered"
    ),
    "unknown": "this server holds no kept live answer for this next token",
    "signature_invalid": "this server did not issue this next token",
    "malformed": "the next token is malformed",
}


def page_envelope(
    retained: RetainedSet,
    rows: Sequence[Any],
    *,
    offset: int,
    fits: Callable[[dict[str, Any]], bool],
    set_id: str,
) -> dict[str, Any]:
    """One page of a kept live answer, from row `offset`, every field whole.

    The longest run of rows from `offset` that fits, each with all its
    fields: a page never cuts fields to hold more rows, so following `next`
    reaches every row with every field. Only a single row too wide for the
    reply on its own is printed with its values cut (`text_shortened`), and
    the next page starts after it. No adapter is called: the rows are the
    kept answer's.
    """

    from okg.substrate.mcp.result_text import (
        LIVE_TEXT_SHORTENED_KEY,
        LIVE_VALUE_CHARS,
        LIVE_VALUE_FLOOR,
    )

    total = len(rows)
    offset = max(0, min(int(offset), total))
    base: dict[str, Any] = {
        "schema": "okg.external_live.v1",
        "relation": retained.relation_name,
        "trust": "untrusted",
        "source": retained.source,
        "observed_at": retained.observed_at,
        "query": dict(retained.query or {}),
        "args_fingerprint": retained.args_fingerprint,
        "state": "ok",
        "served_from": SERVED_FROM,
        "policy": "inline",
        "digest": retained.digest,
        "offset": offset,
        "summary": {"row_count": total},
    }
    placeholder = next_token_placeholder(set_id, total)

    def envelope(count: int, spec: Optional[dict[str, int]], token: Optional[str]) -> dict[str, Any]:
        served = fence_rows(list(rows[offset: offset + count]), retained.relation_name)
        out = {**base, "rows": served, "rows_returned": count,
               "truncated": offset + count < total}
        if token is not None:
            out["next"] = token
            out["next_from_row"] = offset + count + 1
        if spec is not None:
            out[LIVE_TEXT_SHORTENED_KEY] = spec
        return out

    def page(count: int, spec: Optional[dict[str, int]] = None) -> dict[str, Any]:
        more = offset + count < total
        return envelope(count, spec, placeholder if more else None)

    remaining = total - offset
    if remaining == 0:
        return envelope(0, None, None)
    best = 0
    low, high = 1, remaining
    while low <= high:
        middle = (low + high) // 2
        if fits(page(middle)):
            best, low = middle, middle + 1
        else:
            high = middle - 1
    spec: Optional[dict[str, int]] = None
    if best == 0:
        # One row is wider than the reply: print it cut, as the first page
        # would have, and go on after it.
        for candidate in (
            *({"value_chars": n} for n in (LIVE_VALUE_CHARS // 2, 48, 24, LIVE_VALUE_FLOOR)),
            {"value_chars": LIVE_VALUE_FLOOR, "fields": 0},
        ):
            if fits(page(1, candidate)):
                best, spec = 1, candidate
                break
    if best == 0:
        return envelope(0, None, None)
    more = offset + best < total
    return envelope(best, spec, next_token(set_id, offset + best) if more else None)


@dataclass
class RetainHandle:
    """One expand's request to keep its answer, as the dispatcher sees it."""

    block: dict[str, Any]
    draft: Optional[_rr.Draft] = None
    retained: Optional[RetainedSet] = None
    store: Optional[_rr.ResultReferenceStore] = None
    sample_attrs: dict[str, Any] = field(default_factory=dict)

    @property
    def kept(self) -> bool:
        return self.draft is not None

    def finalize(self, *, digest: str, rows_returned: int, refetch: Any) -> None:
        """Fill the digest the caller is given; called once the rows are fitted."""

        if self.retained is None:
            return
        self.retained.digest = digest
        self.retained.rows_returned = rows_returned
        self.retained.refetch = refetch
        self.block["digest"] = digest

    def abandon(self) -> None:
        """The response will not carry the id: give the reservation back now."""

        if self.draft is not None and self.store is not None:
            self.store.release(self.draft.id)
        self.draft = None
        self.retained = None
        self.sample_attrs = {"retained": False, "retained_reason": "response_bound_failed"}


def not_kept(reason: str) -> RetainHandle:
    return RetainHandle(
        block=not_kept_block(reason),
        sample_attrs={"retained": False, "retained_reason": reason},
    )


def begin_retain(
    raw: Any,
    *,
    base: Mapping[str, Any],
    store: _rr.ResultReferenceStore,
    reader_key: str,
    header_claim: Optional[Mapping[str, Any]],
    multi_user: bool,
    binding: Mapping[str, Any],
    relation: Any,
    node_id: str,
    subject: str,
    deployment_name: str,
    session_id: str,
    each_from: Optional[Mapping[str, Any]] = None,
) -> RetainHandle:
    """Draft a set for an `ok` call's answer, or say why it is not kept.

    The draft reserves server capacity at once, so a full server is reported
    here as `capacity` while the rows are still served. The id is committed
    only when the final response carries it (the read call's draft scope).
    """

    try:
        blob, count = capture(raw)
    except NotRetained as exc:
        return not_kept(exc.reason)
    retained = RetainedSet(
        rows_json=blob,
        row_count=count,
        relation=relation,
        node_id=node_id,
        subject=subject,
        args_fingerprint=str(base.get("args_fingerprint") or ""),
        observed_at=str(base.get("observed_at") or ""),
        source=str(base.get("source") or ""),
        deployment_name=deployment_name,
        session_id=session_id,
        query=dict(base.get("query") or {}),
        each=str(base["each"]) if base.get("each") else None,
        each_from=dict(each_from) if each_from else None,
    )
    try:
        draft = store.mint(
            reader_key=reader_key, binding=binding, members=(),
            header_claim=header_claim, multi_user=multi_user,
            payload=retained, payload_bytes=len(blob), payload_rows=count,
        )
    except _rr.ResultReferenceCapacity as exc:
        reason = (exc.data or {}).get("reason")
        return not_kept(
            "too_large" if reason in {"reference_members", "reference_bytes"} else "capacity"
        )
    _rr.adopt_draft(store, draft)
    return RetainHandle(
        block=reference_block(
            set_id=draft.id, expires_at=draft.expires_at, rows=count,
            digest=PLACEHOLDER_DIGEST, observed_at=retained.observed_at,
        ),
        draft=draft,
        retained=retained,
        store=store,
        sample_attrs={"retained": True, "retained_rows": count},
    )


# -- reads --------------------------------------------------------------------


def _unsupported(argument: str, message: str, fix_hint: str) -> LiveResultPredicateUnsupported:
    return LiveResultPredicateUnsupported(
        message, data={"argument": argument}, fix_hint=fix_hint,
    )


def _attrs_key(value: Any, *, argument: str) -> str:
    """The upstream field an `attrs.<field>` name reads, or a refusal by name."""

    if not isinstance(value, str) or not value.startswith("attrs."):
        raise _unsupported(
            argument,
            f"a retained live answer is read by attrs.<field> only; {value!r} is not one",
            "name the upstream field as attrs.<field>",
        )
    key = value.split(".", 1)[1]
    if not key or "." in key:
        raise _unsupported(
            argument,
            f"a retained live answer reads one top-level upstream field; {value!r} "
            "is a nested path",
            "name a top-level upstream field as attrs.<field>",
        )
    return key


def _condition_key(value: Any, *, argument: str) -> str:
    """The key a condition, `by` or metric field reads: an upstream field's
    name, or a record column's name behind NUL (:data:`RECORD_COLUMNS`)."""

    if isinstance(value, str) and value in RECORD_COLUMNS:
        return _RECORD_KEY + value
    return _attrs_key(value, argument=argument)


def column_label(key: str) -> str:
    """A group or metric key as the agent wrote it."""

    return key[len(_RECORD_KEY):] if key.startswith(_RECORD_KEY) else f"attrs.{key}"


def is_record_key(key: str) -> bool:
    return key.startswith(_RECORD_KEY)


def check_predicate(predicate: Any) -> None:
    """Refuse, by name, a predicate the in-memory evaluator must not answer."""

    if predicate in (None, True) or predicate == {}:
        return
    pending = [predicate]
    while pending:
        node = pending.pop()
        if not isinstance(node, Mapping):
            raise _unsupported("predicate", "a predicate must be an object", "pass a predicate object")
        op = node.get("op")
        if op in ("and", "or"):
            clauses = node.get("clauses")
            if not isinstance(clauses, list) or not clauses:
                raise _unsupported("predicate", f"{op} needs a non-empty clauses list",
                                   "pass clauses=[...]")
            pending.extend(clauses)
        elif op == "not":
            pending.append(node.get("clause"))
        elif op in _LEAF_OPS:
            _condition_key(node.get("field"), argument="predicate")
            if op in ("contains", "prefix", "suffix"):
                _check_text_needle(node)
            elif op == "in":
                values = node.get("values")
                if not isinstance(values, list) or not values:
                    raise _unsupported(
                        "predicate", "in needs a non-empty values list",
                        'write {"op": "in", "field": ..., "values": [...]}',
                    )
            elif op == "between":
                bounds = node.get("values")
                if not isinstance(bounds, list) or len(bounds) != 2:
                    raise _unsupported(
                        "predicate", "between needs values [low, high]",
                        'write {"op": "between", "field": ..., "values": [low, high]}',
                    )
        elif op in ("like", "like_exact"):
            # loose-match-3666: the fold is the graph read's; an outside
            # system's rows are compared only as the operators above do.
            raise _unsupported(
                "predicate",
                f"a retained live answer does not support {op}: its rows are an "
                "outside system's, compared exactly",
                "use eq, in, prefix or suffix on the live set's own column",
            )
        else:
            raise _unsupported(
                "predicate",
                f"a retained live answer does not support the predicate op {op!r}",
                "use and, or, not, eq, in, exists, lt, gt, between, contains, "
                "prefix or suffix",
            )


def _check_text_needle(node: Mapping[str, Any]) -> None:
    """`contains`, `prefix` and `suffix` over a live set read text inside one
    field (never JSON containment on the whole attrs object): the value
    must be non-empty text."""

    from okg.substrate.mcp.operator_sql import text_operand_problem

    op = str(node.get("op"))
    value = node.get("value")
    if not isinstance(value, str):
        raise _unsupported(
            "predicate",
            f"{op} on a retained live answer finds text inside one field; "
            "its value must be text",
            f'write {{"op": "{op}", "field": "attrs.<field>", "value": "<text>"}}',
        )
    problem = text_operand_problem(value)
    if problem is not None:
        raise _unsupported("predicate", problem, "pass a non-empty text value")


def check_projection(projection: Any, *, argument: str) -> None:
    """A projection names `attrs.<field>`, or a record column of a live typed
    row (`node_id`, `subtype`), as a list or {alias: field}."""

    if projection is None:
        return
    if isinstance(projection, list):
        fields = projection
    elif isinstance(projection, Mapping):
        fields = list(projection.values())
    else:
        raise _unsupported(argument, f"{argument} must be a list or mapping",
                           "pass a list of attrs.<field> names")
    for item in fields:
        _condition_key(item, argument=argument)


def check_by(by: Any) -> list[str]:
    """`by` of an aggregate over a retained answer. Omitted (or empty), the
    whole set is one group: a total."""

    fields = list(by or [])
    if len(fields) > MAX_BY_FIELDS:
        raise _unsupported("by", f"by names 0-{MAX_BY_FIELDS} fields",
                           "pass by=['attrs.<field>'], or omit by for one total")
    for item in fields:
        _condition_key(item, argument="by")
    return [str(item) for item in fields]


#: What an aggregate over a retained answer computes per group: `count`, and
#: sum, min or max of one upstream column (a sum over 30 live
#: rows, which the agent otherwise added by hand).
METRIC_FUNCTIONS = ("sum", "min", "max")
MAX_METRICS = 4
_METRIC_FORM = re.compile(r"^(sum|min|max)\((attrs\.[^()]*)\)$")
METRICS_HINT = (
    "pass metrics=['count'], or e.g. ['sum(attrs.<field>)'] (also min, max)"
)


def check_metrics(metrics: Any) -> list[tuple[str, Optional[str]]]:
    """Parsed metrics: ("count", None) or (function, upstream field)."""

    items = list(metrics or ["count"])
    if not 1 <= len(items) <= MAX_METRICS or len(set(map(str, items))) != len(items):
        raise _unsupported(
            "metrics", f"metrics names 1-{MAX_METRICS} different metrics", METRICS_HINT,
        )
    parsed: list[tuple[str, Optional[str]]] = []
    for item in items:
        if item == "count":
            parsed.append(("count", None))
            continue
        form = _METRIC_FORM.match(item) if isinstance(item, str) else None
        if form is None:
            raise _unsupported(
                "metrics", f"{item!r} is not a metric of a retained live answer",
                METRICS_HINT,
            )
        parsed.append((form.group(1), _condition_key(form.group(2), argument="metrics")))
    return parsed


def _args_text(query: Mapping[str, Any], listed: Optional[str] = None) -> str:
    """Arguments as `k=v, ...`; a listed argument as its value count."""

    parts = []
    for key, value in query.items():
        if key == listed and isinstance(value, list):
            parts.append(f"{key}=<{len(value)} values>")
        elif isinstance(value, str):
            parts.append(f"{key}={value}")
        else:
            parts.append(f"{key}={json.dumps(value, default=str)}")
    return ", ".join(parts)


def totals_cover(retained: "RetainedSet", rows_counted: int) -> str:
    """What a total over a kept answer covers, in one line (a listing
    of 142 items matched by a pattern was reported as all 223). It names the
    call and, for a fan-out over a kept listing, that listing's pattern."""

    query = retained.query or {}
    text = (
        f"{rows_counted} rows of {retained.relation_name}"
        f"({_args_text(query, retained.each)})"
    )
    if retained.each:
        values = query.get(retained.each)
        count = len(values) if isinstance(values, list) else 1
        text += f", one call per {retained.each} ({count} values"
        source = retained.each_from or {}
        if source.get("relation"):
            text += (
                f" from {source['relation']}({_args_text(source.get('query') or {})})"
            )
        text += ")"
    return text + "; nothing outside these calls is counted"


def metric_label(metric: tuple[str, Optional[str]]) -> str:
    function, key = metric
    return "count" if key is None else f"{function}({column_label(key)})"


def float_part_label(label: str) -> str:
    """The column of a sum's non-integer values, summed apart from its
    exact integer part (`label`)."""
    return f"{label} floats"


def _number(value: Any) -> Optional[Any]:
    """An int or a finite float; a numeric string is read as one. Anything
    else (a bool, null, text, a list) is not a number."""

    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, str):
        text = value.strip()
        try:
            return int(text)
        except ValueError:
            pass
        try:
            number = float(text)
        except ValueError:
            return None
        return number if math.isfinite(number) else None
    return None


#: A group key a row does not carry at all (#3666, lane ri): its own group,
#: printed `(absent)`, apart from a stored null. In one listing of ~260 members,
#: most had no such attribute; grouped as null they read as an
#: attribute set to null.
ABSENT = "(absent)"
#: The group text of an absent key: no JSON text starts with a NUL.
_ABSENT_TEXT = "\x00absent"


class _Absent:
    """The value of a group key a row does not carry."""

    def __repr__(self) -> str:
        return ABSENT


ABSENT_VALUE = _Absent()


@dataclass
class GroupReport:
    """What :func:`group_report` computes: the groups, and what a reader
    of the totals must be told."""

    #: (key values, metrics) per group, largest count first. A key a row
    #: does not carry is :data:`ABSENT_VALUE`.
    groups: list[tuple[tuple[Any, ...], dict[str, Any]]]
    #: Per computed column, rows left out of it (`missing`, `not_numeric`).
    skipped: dict[str, dict[str, int]]
    #: Per `by` field, the rows that do not carry it (only fields with any).
    absent: dict[str, int]
    #: The sum metrics that met a non-integer value: the metric is still the
    #: exact integer sum; those values are in `<metric> floats`.
    inexact: list[str]


def group_report(
    rows: Sequence[Mapping[str, Any]],
    by: Sequence[str],
    metrics: Sequence[tuple[str, Optional[str]]],
) -> GroupReport:
    """Groups by `by` with every metric, largest count first.

    Per computed column, rows with no number there (`missing`: absent or
    null; `not_numeric`: any other value) are left out of that metric,
    never read as 0. Integers (and whole-number floats) add exactly, at any
    size, into the metric; other floats are added apart with ``math.fsum``
    (one rounding, in any row order) into `<metric> floats`, and the metric
    is named in `inexact`. A row that does not carry a `by`
    field is in the `(absent)` group, apart from a stored null. A group
    whose column holds no number gets null for that metric."""

    keys = [
        _RECORD_KEY + field if field in RECORD_COLUMNS else field.split(".", 1)[1]
        for field in by
    ]
    counts: collections.Counter[tuple[str, ...]] = collections.Counter()
    values: dict[tuple[str, ...], tuple[Any, ...]] = {}
    found: dict[tuple[str, ...], dict[str, list[Any]]] = {}
    skipped: dict[str, dict[str, int]] = {}
    absent: collections.Counter[str] = collections.Counter()
    columns = sorted({key for _function, key in metrics if key is not None})
    for column in columns:
        skipped[column_label(column)] = {"missing": 0, "not_numeric": 0}
    for row in rows:
        raw = tuple(row[key] if key in row else ABSENT_VALUE for key in keys)
        text = tuple(
            _ABSENT_TEXT if value is ABSENT_VALUE else _group_key(value) for value in raw
        )
        for name, value in zip(by, raw):
            if value is ABSENT_VALUE:
                absent[name] += 1
        counts[text] += 1
        values.setdefault(text, raw)
        bucket = found.setdefault(text, {})
        for column in columns:
            value = row.get(column)
            number = _number(value)
            if number is None:
                reason = "missing" if value is None else "not_numeric"
                skipped[column_label(column)][reason] += 1
                continue
            bucket.setdefault(column, []).append(number)
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    out: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    inexact: set[str] = set()
    for text, count in ordered:
        computed: dict[str, Any] = {}
        for metric in metrics:
            function, key = metric
            if key is None:
                computed["count"] = count
                continue
            numbers = found[text].get(key) or []
            label = metric_label(metric)
            if not numbers:
                computed[label] = None
            elif function == "sum":
                # A whole-number float (3.0) is added as the integer it is.
                # The rest are summed apart: the integer sum stays exact
                # and the float part is its own column (review M4: one
                # float made a 17-digit byte total a rounded float).
                integers = sum(
                    n if isinstance(n, int) else int(n)
                    for n in numbers if isinstance(n, int) or n.is_integer()
                )
                floats = [n for n in numbers if isinstance(n, float) and not n.is_integer()]
                computed[label] = integers
                if floats:
                    inexact.add(label)
                    computed[float_part_label(label)] = math.fsum(floats)
            else:
                computed[label] = min(numbers) if function == "min" else max(numbers)
        out.append((values[text], computed))
    return GroupReport(
        groups=out, skipped=skipped, absent=dict(absent),
        inexact=[metric_label(m) for m in metrics if metric_label(m) in inexact],
    )


def group_metrics(
    rows: Sequence[Mapping[str, Any]],
    by: Sequence[str],
    metrics: Sequence[tuple[str, Optional[str]]],
) -> tuple[list[tuple[tuple[Any, ...], dict[str, Any]]], dict[str, dict[str, int]]]:
    """:func:`group_report`'s groups and skipped counts, with an absent
    key served as the text ``(absent)``."""

    report = group_report(rows, by, metrics)
    return served_groups(report.groups), report.skipped


def served_groups(
    groups: Sequence[tuple[tuple[Any, ...], dict[str, Any]]],
) -> list[tuple[tuple[Any, ...], dict[str, Any]]]:
    """Groups with :data:`ABSENT_VALUE` written as ``(absent)``."""

    return [
        (tuple(ABSENT if value is ABSENT_VALUE else value for value in key), computed)
        for key, computed in groups
    ]


def fenced_groups(
    groups: Sequence[tuple[tuple[Any, ...], dict[str, Any]]],
    fields: Sequence[str],
    relation: str,
    *,
    source: Optional[str] = None,
) -> list[dict[str, Any]]:
    """Groups as served rows, every key value wrapped as untrusted text of
    `relation` (or of the fence `source` itself, when given); an absent key
    is the plain ``(absent)``, the server's word for "this row has no such
    field", never wrapped."""

    plain: list[dict[str, Any]] = []
    absent: list[list[str]] = []
    for key, computed in groups:
        plain.append({
            **{
                field: (None if key[i] is ABSENT_VALUE else key[i])
                for i, field in enumerate(fields)
            },
            **computed,
        })
        absent.append([field for i, field in enumerate(fields) if key[i] is ABSENT_VALUE])
    if source is None:
        fenced = fence_rows(plain, relation)
    else:
        from okg.substrate.external_backed.resolver import _fence_payload

        fenced = _fence_payload(plain, source=source, fence_keys=True)
    for row, names in zip(fenced, absent):
        for name in names:
            row[name] = ABSENT
    return fenced


#: Said when a group key is absent from some rows (#3666, lane ri).
def absent_sentence(absent: Mapping[str, int]) -> str:
    return "; ".join(
        f"{count:,} {'row has' if count == 1 else 'rows have'} no "
        f"{field.split('.', 1)[-1]}: {'it is' if count == 1 else 'they are'} "
        f"the group {ABSENT}"
        for field, count in absent.items()
    )


#: Said when a sum added a non-integer value.
def inexact_sentence(labels: Sequence[str]) -> str:
    return (
        ", ".join(labels) + " includes non-integer values: the metric is the "
        "exact sum of the whole numbers; the others are one correctly rounded "
        "float sum, in " + ", ".join(float_part_label(label) for label in labels)
    )


_LIVE_MESSAGES = {
    "binding_mismatch": (
        "the retained live answer was made under a different binding than this read"
    ),
    "generation_conflict": (
        "the generation_id named differs from the generation this read checked "
        "the retained live answer's anchor node at"
    ),
}


def invalid(reason: str, **data: Any) -> _rr.ResultReferenceInvalid:
    """A read-side `result_reference_invalid` for a live set, in live words."""

    return _rr.ResultReferenceInvalid(
        _LIVE_MESSAGES[reason], data={"reason": reason, **data}, fix_hint=FIX_HINT,
    )


def page_cursor(offset: int, page: int, truncated: bool) -> dict[str, Any]:
    """`next_offset` for a live read page, never past the published offset cap.

    `offset` is published with a maximum (`MAX_OFFSET`), so a cursor beyond
    it could not be followed. There the page ends the cursor and says how to
    reach the rest instead.
    """

    from okg.substrate.mcp.operator_models import MAX_OFFSET

    if not truncated:
        return {"next_offset": None}
    following = offset + page
    if following <= MAX_OFFSET:
        return {"next_offset": following}
    return {
        "next_offset": None,
        # The shared pagination hint must not rebuild the cursor from
        # offset + limit; this is the flag it already honours.
        "pagination_capped": True,
        "paging_limit": {
            "reason": "offset_cap",
            "max_offset": MAX_OFFSET,
            "remaining_from": following,
            "remedy": (
                "narrow the set with a predicate on attrs.<field> and page the "
                "narrower set"
            ),
        },
    }


def project(
    raw_rows: Sequence[Mapping[str, Any]],
    served_rows: Sequence[Mapping[str, Any]],
    pairs: Sequence[tuple[str, str]],
    records: Optional[Sequence[Mapping[str, Any]]] = None,
) -> list[dict[str, Any]]:
    """Project `attrs.<field>` names over served (wrapped) rows.

    A name is the upstream field as the external system returned it; a key
    the wrapping rewrote (a control character, a fence marker, a collision)
    is found through that row's own raw-to-served key map. A name that is
    not an upstream key is read as a served key, so a key copied from a
    served row still projects.
    """

    out: list[dict[str, Any]] = []
    for index, (raw, served) in enumerate(zip(raw_rows, served_rows)):
        served_key = dict(zip((str(key) for key in raw), served))
        item: dict[str, Any] = {}
        record = records[index] if records is not None and index < len(records) else {}
        for alias, field_name in pairs:
            if field_name in RECORD_COLUMNS:
                item[alias] = record.get(field_name)
                continue
            name = field_name.split(".", 1)[1]
            item[alias] = served.get(served_key.get(name, name))
        out.append(item)
    return out


def refuse_argument(argument: str, operator: str) -> LiveResultPredicateUnsupported:
    hints = {
        "order_by": "drop order_by; rows are read in upstream order",
        "retain": "drop retain; a retained live answer has no child references",
        "metrics": METRICS_HINT,
    }
    return _unsupported(
        argument,
        f"{operator}: {argument} does not apply to a retained live answer",
        hints.get(argument, f"drop {argument}"),
    )


def _group_key(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def group_counts(
    rows: Sequence[Mapping[str, Any]], by: Sequence[str],
) -> list[tuple[tuple[Any, ...], int]]:
    """Counts grouped by `by` (`attrs.<field>` names), largest first.

    Ties are ordered by the groups' JSON text, so pages are stable. A field
    a row does not carry groups as ``(absent)``, apart from a stored null
    (#3666, lane ri; :func:`group_report`).
    """

    report = group_report(rows, by, [("count", None)])
    return [(key, computed["count"]) for key, computed in served_groups(report.groups)]


def served_rows_sha256(rows: Any) -> str:
    """`sha256:` of what one read served, by the call sample's own encoding."""

    from okg.substrate.external_backed.call_sample import digest_of

    return "sha256:" + digest_of(rows)[0]


Evaluator = Callable[[dict[str, Any], Any], bool]


__all__ = [
    "FIX_HINT",
    "ID_PREFIX",
    "LIMITS",
    "LiveResultDenied",
    "LiveResultPredicateUnsupported",
    "NOT_KEPT_REASONS",
    "NotRetained",
    "RetainHandle",
    "RetainedSet",
    "SERVED_FROM",
    "begin_retain",
    "capture",
    "check_by",
    "check_metrics",
    "check_predicate",
    "RECORD_COLUMNS",
    "check_projection",
    "column_label",
    "decode_rows",
    "encode_rows",
    "fence_rows",
    "float_part_label",
    "group_counts",
    "group_metrics",
    "invalid",
    "is_next_token",
    "largest_next_token",
    "metric_label",
    "totals_cover",
    "next_set_id",
    "next_token",
    "next_token_placeholder",
    "page_cursor",
    "page_envelope",
    "parse_next_token",
    "project",
    "redo_call",
    "remember_call",
    "remembered_call",
    "install_live_store",
    "is_live_id",
    "is_record_key",
    "live_store",
    "new_store",
    "not_kept",
    "refuse_argument",
    "served_rows_sha256",
]
