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
8 MiB per set, 2 sets per reader and per principal, 8 sets and 64 MiB per
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
MAX_PER_READER = 2
MAX_PER_PRINCIPAL = 2
MAX_PER_SERVER = 8
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

#: Predicate operators a read accepts: the graph `filter` grammar, without
#: `contains` (which is JSON containment on the whole attrs object).
_LEAF_OPS = frozenset({"eq", "in", "exists", "lt", "gt", "between", "prefix", "suffix"})


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


def redo_call(node_id: str, relation: str, query: Mapping[str, Any]) -> dict[str, Any]:
    """The published call that asks the live system for this answer again."""

    live: dict[str, Any] = {"relation": relation}
    if query:
        live["args"] = dict(query)
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
            _attrs_key(node.get("field"), argument="predicate")
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
                "use and, or, not, eq, in, exists, lt, gt, between, prefix or suffix",
            )


def check_projection(projection: Any, *, argument: str) -> None:
    """A projection names `attrs.<field>` only, as a list or {alias: field}."""

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
        _attrs_key(item, argument=argument)


def check_by(by: Any) -> list[str]:
    fields = list(by or [])
    if not 1 <= len(fields) <= MAX_BY_FIELDS:
        raise _unsupported("by", f"by names 1-{MAX_BY_FIELDS} fields",
                           "pass by=['attrs.<field>']")
    for item in fields:
        _attrs_key(item, argument="by")
    return [str(item) for item in fields]


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
) -> list[dict[str, Any]]:
    """Project `attrs.<field>` names over served (wrapped) rows.

    A name is the upstream field as the external system returned it; a key
    the wrapping rewrote (a control character, a fence marker, a collision)
    is found through that row's own raw-to-served key map. A name that is
    not an upstream key is read as a served key, so a key copied from a
    served row still projects.
    """

    out: list[dict[str, Any]] = []
    for raw, served in zip(raw_rows, served_rows):
        served_key = dict(zip((str(key) for key in raw), served))
        item: dict[str, Any] = {}
        for alias, field_name in pairs:
            name = field_name.split(".", 1)[1]
            item[alias] = served.get(served_key.get(name, name))
        out.append(item)
    return out


def refuse_argument(argument: str, operator: str) -> LiveResultPredicateUnsupported:
    hints = {
        "order_by": "drop order_by; rows are read in upstream order",
        "retain": "drop retain; a retained live answer has no child references",
        "metrics": "use metrics=['count']",
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

    Ties are ordered by the groups' JSON text, so pages are stable. A missing
    field groups as null.
    """

    keys = [field.split(".", 1)[1] for field in by]
    counts: collections.Counter[tuple[str, ...]] = collections.Counter()
    values: dict[tuple[str, ...], tuple[Any, ...]] = {}
    for row in rows:
        raw = tuple(row.get(key) for key in keys)
        text = tuple(_group_key(value) for value in raw)
        counts[text] += 1
        values.setdefault(text, raw)
    ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
    return [(values[text], count) for text, count in ordered]


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
    "check_predicate",
    "check_projection",
    "decode_rows",
    "encode_rows",
    "fence_rows",
    "group_counts",
    "invalid",
    "is_next_token",
    "largest_next_token",
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
    "live_store",
    "new_store",
    "not_kept",
    "refuse_argument",
    "served_rows_sha256",
]
