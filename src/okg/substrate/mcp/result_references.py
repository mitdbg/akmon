"""Short-lived, reader-bound result references (issue #3222).

A result reference names the ordered members of one read result -- node ids,
ranks and hit locations, never text or attributes -- under a compact signed
id, so a later call can work on that set without the caller copying it.

The store lives in this server process only. Its limits are fixed (design
D8): a 600 s lifetime; 200 members and 256 KiB canonical bytes per
reference; 128 references per reader, 1,024 per server and 32 MiB of
canonical bytes in all. "Canonical bytes" is the UTF-8 length of the
canonical JSON of the stored binding and members.

At its own cap a reader's oldest reference is evicted (with its children),
and the evicted id then refuses ``evicted`` to that reader. The server and
byte caps refuse ``result_reference_capacity`` instead: one reader never
evicts another reader's reference. Refusals report the caller's own usage
and the limits, never the server-wide counts. (The live handles store sets
``fair_eviction``: it counts its caps per quota key, the reader plus its
logical session, and a full server evicts the oldest entry of the quota key
holding the most; PACT read-live-handles.)

On a multi-user server one authenticated principal (its header identity
claim) holds at most 128 live references across all its reader keys, so
varying the ``authority`` argument cannot multiply its share. At that cap
the principal's oldest reference is evicted the same way, and refuses
``evicted`` to the reader that owned it. A single-user server has no
principal cap.

A reference is produced in two steps. Inside one read call a producer
*drafts* it (:func:`draft`); the byte-budget wrapper commits the draft only
when the final response it returns carries the draft's id
(:class:`DraftScope`). Drafts never consume capacity, so byte-fitting reruns,
refusals and raised errors leave the store unchanged.

Ids look like ``r1.<epoch>.<exp>.<nonce>.<mac>``. ``epoch`` is random per
store, so an id from another process or an earlier store refuses
``store_lost``. The MAC key is derived from the continuation signing key
under its own label, so a reference id and a continuation token never share
a key.
"""

from __future__ import annotations

import base64
import contextlib
import copy
import functools
import hashlib
import hmac
import json
import math
import re
import secrets
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Any, Optional, TypeVar

from okg.substrate.errors import SubstrateError

LIFETIME_SECONDS = 600
MAX_MEMBERS = 200
MAX_REFERENCE_BYTES = 256 * 1024
MAX_PER_READER = 128
#: Live references one authenticated principal holds across all its reader
#: keys, on a multi-user server only (rrcap A).
MAX_PER_PRINCIPAL = 128
MAX_PER_SERVER = 1024
MAX_TOTAL_BYTES = 32 * 1024 * 1024
#: Evicted ids remembered so their reader hears ``evicted``, not ``unknown``.
MAX_EVICTED_RECORDS = 1024

_ID_VERSION = "r1"
_MAC_LABEL = b"okg/result-reference/v1"
_MAC_BYTES = 16
_MEMBER_KEYS = frozenset({"node_id", "rank", "location"})
_FIX_HINT = "run the read that produced the reference again"


def _id_pattern(version: str) -> re.Pattern[str]:
    return re.compile(
        re.escape(version)
        + r"\.([A-Za-z0-9_-]{8})\.([0-9]{1,12})\.([A-Za-z0-9_-]{16})\.([A-Za-z0-9_-]{22})"
    )


_ID_PATTERN = _id_pattern(_ID_VERSION)


@dataclass(frozen=True)
class StoreLimits:
    """What makes one store differ from another: its ids and its limits.

    The graph store uses the defaults. A sibling store (the live result
    sets of ``live_result_sets``) passes its own id version and MAC label,
    so an id from one store never validates in the other, and its own caps.
    ``copy_on_lookup`` False hands back the stored entry itself (its payload
    is immutable bytes); ``reserve_on_mint`` counts a draft against the
    server and byte caps from the moment it is minted until it is committed
    or released, so a full store is known before the response is built.
    """

    id_version: str = _ID_VERSION
    mac_label: bytes = _MAC_LABEL
    lifetime_seconds: int = LIFETIME_SECONDS
    max_members: int = MAX_MEMBERS
    max_reference_bytes: int = MAX_REFERENCE_BYTES
    max_per_reader: int = MAX_PER_READER
    max_per_principal: int = MAX_PER_PRINCIPAL
    max_per_server: int = MAX_PER_SERVER
    max_total_bytes: int = MAX_TOTAL_BYTES
    copy_on_lookup: bool = True
    reserve_on_mint: bool = False
    fix_hint: str = _FIX_HINT
    #: Bytes one quota key holds at most (None: no such cap). Its own oldest
    #: entry is evicted to make room, as at its count cap.
    max_quota_bytes: Optional[int] = None
    #: When the server count or byte cap would be passed, evict the oldest
    #: entry of the quota key holding the most entries (ties: the most
    #: bytes) until the draft fits, instead of refusing (PACT
    #: read-live-handles, M1). The caps count by quota key (reader plus
    #: logical session) in a store that sets it; the graph store keeps
    #: today's rule until stored handles (3d).
    fair_eviction: bool = False
    #: What a refusal calls one entry ("handle" for live handles).
    noun: str = "result reference"

    def advertised(self) -> dict[str, int]:
        out = {
            "lifetime_seconds": self.lifetime_seconds,
            "members_per_reference": self.max_members,
            "bytes_per_reference": self.max_reference_bytes,
            "references_per_reader": self.max_per_reader,
            "references_per_principal": self.max_per_principal,
            "references_per_server": self.max_per_server,
            "total_bytes": self.max_total_bytes,
        }
        if self.max_quota_bytes is not None:
            out["bytes_per_reader"] = self.max_quota_bytes
        return out


GRAPH_LIMITS = StoreLimits()


def limits() -> dict[str, int]:
    """The fixed store limits, as a surface advertises them."""

    return {
        "lifetime_seconds": LIFETIME_SECONDS,
        "members_per_reference": MAX_MEMBERS,
        "bytes_per_reference": MAX_REFERENCE_BYTES,
        "references_per_reader": MAX_PER_READER,
        "references_per_principal": MAX_PER_PRINCIPAL,
        "references_per_server": MAX_PER_SERVER,
        "total_bytes": MAX_TOTAL_BYTES,
    }


#: Delivery order of every reference in this slice: the producer's rank.
ORDER = "rank"
#: Producer methods that can produce a reference (design D1: record units).
_PRODUCER_METHODS = frozenset({"lexical"})
_PRODUCER_RANK_BY = frozenset({None, "records", "passages"})
#: Binding fields compared at use, in refusal order, and the name each
#: refusal gives (validation step 7). ``attributes`` is step 8, apart.
_BINDING_FIELDS: tuple[tuple[str, str], ...] = (
    ("authority", "authority"),
    ("policy", "authority"),
    ("catalog_version", "catalog"),
    ("branch", "branch"),
    ("database", "database"),
    ("deployment", "deployment"),
)


def producer_refusal(
    *,
    method: Any,
    rank_by: Any,
    view: Any,
    offset: Any,
    select: Any,
    max_chars: Any,
    limit: Any,
) -> Optional[SubstrateError]:
    """Why a search cannot produce a reference, from its arguments alone.

    Pure, so a refused form runs no SQL (design D1, D5). ``None`` when the
    form can produce one. ``view="metadata"`` delivers no rows, so a
    ``select`` or ``max_chars`` with it refuses by name (slice 3). A
    ``limit`` over the member cap refuses ``result_reference_capacity``, as
    a draft of that size would.
    """

    if method not in _PRODUCER_METHODS or view == "windows":
        return _unsupported(
            "method", argument="method",
            message=f"search: method={method!r} cannot produce a result reference; "
            "only lexical record searches can",
            fix_hint="use method='lexical' with rank_by 'records' or 'passages', or drop retain",
        )
    if rank_by not in _PRODUCER_RANK_BY:
        return _unsupported(
            "rank_by", argument="rank_by",
            message=f"search: rank_by={rank_by!r} cannot produce a result reference",
            fix_hint="use rank_by 'records' or 'passages', or drop retain",
        )
    if view == "metadata":
        for name, value in (("select", select), ("max_chars", max_chars)):
            if value is not None:
                return _unsupported(
                    "argument", argument=name,
                    message=f"search: view='metadata' delivers no rows, so {name} "
                    "does not apply",
                    fix_hint=f"drop {name}, or use retain=true with the cards or list view",
                )
    if isinstance(offset, int) and offset > 0:
        return _unsupported(
            "argument", argument="offset",
            message="search: retain makes a reference from the first page only",
            fix_hint="drop offset; page the reference with map(input={'ref': id}, offset=n)",
        )
    if isinstance(limit, int) and limit > MAX_MEMBERS:
        return ResultReferenceCapacity(
            f"a result reference holds at most {MAX_MEMBERS} members",
            data={"reason": "reference_members", "members": limit, "limits": limits()},
            fix_hint="narrow the read or lower its limit",
        )
    return None


#: `summary` (design D3): 1-4 `by` fields, count only, at most 50 groups.
SUMMARY_MAX_FIELDS = 4
SUMMARY_MAX_GROUPS = 50
#: Fields a summary may not group by: each names a record's identity (its id,
#: handle or title), so its groups would deliver the rows a metadata page
#: withholds (lead decision on review MED 3).
SUMMARY_IDENTITY_FIELDS = frozenset({"node_id", "handle", "attrs.title"})
#: The group key every summary group carries its count under.
SUMMARY_COUNT_KEY = "count"


def summary_refusal(*, view: Any, summary: Any) -> Optional[SubstrateError]:
    """Why a search cannot serve ``summary``, from its arguments alone.

    Pure, so a refused form runs no SQL. ``summary`` is served only with
    ``view="metadata"`` and names 1-4 fields as strings (the published
    schema says the same; a direct Python caller is checked here). An
    identity field (:data:`SUMMARY_IDENTITY_FIELDS`), a repeated field, or
    a field named ``count`` is refused: the first would deliver row
    identities, the others would collapse the group keys.
    """

    if summary is None:
        return None
    if view != "metadata":
        return _unsupported(
            "argument", argument="summary",
            message="search: summary is served only with view='metadata'",
            fix_hint="add view='metadata', or drop summary",
        )
    if (
        not isinstance(summary, (list, tuple))
        or not 1 <= len(summary) <= SUMMARY_MAX_FIELDS
        or not all(isinstance(field, str) and field for field in summary)
    ):
        return _unsupported(
            "argument", argument="summary",
            message=f"search: summary names 1-{SUMMARY_MAX_FIELDS} fields",
            fix_hint="pass summary=['subtype'] or up to four field names",
        )
    identity = sorted(SUMMARY_IDENTITY_FIELDS & set(summary))
    if identity:
        return _unsupported(
            "argument", argument="summary",
            message=f"search: summary cannot group by {identity[0]!r}: its groups "
            "would name the records a metadata search does not deliver",
            fix_hint="group by subtype or a non-identifying attrs.<key>, or read the "
            "records with map(input={'ref': id})",
        )
    if len(set(summary)) != len(summary) or SUMMARY_COUNT_KEY in summary:
        return _unsupported(
            "argument", argument="summary",
            message="search: summary fields must be distinct and none may be "
            f"named {SUMMARY_COUNT_KEY!r} (each group carries its count there)",
            fix_hint="name each field once, and not 'count'",
        )
    return None


def aggregate_call(reference_id: str, by: Sequence[str], *, offset: int) -> dict[str, Any]:
    """The ``aggregate`` call that reads a summary's remaining groups."""

    arguments: dict[str, Any] = {
        "input": {"ref": reference_id}, "by": list(by), "metrics": ["count"],
    }
    if offset:
        arguments["offset"] = offset
    return {"tool": "aggregate", "arguments": arguments}


def summary_block(
    ordered: Sequence[tuple[Sequence[Any], int]],
    *,
    by: Sequence[str],
    reference_id: str,
) -> dict[str, Any]:
    """A metadata search's count summary (design D3).

    ``ordered`` is the reference-aggregate groups, largest first, exactly as
    ``aggregate(input={"ref": id})`` orders them. At most
    :data:`SUMMARY_MAX_GROUPS` are kept; ``groups_total`` is exact, and when
    more exist the block names the ``summary_groups`` bound and the
    ``aggregate`` call that reads the rest.
    """

    fields = list(by)
    groups = [
        {**{fields[i]: key[i] for i in range(len(fields))}, "count": count}
        for key, count in list(ordered)[:SUMMARY_MAX_GROUPS]
    ]
    block: dict[str, Any] = {
        "by": fields, "groups": groups, "groups_total": len(ordered),
    }
    if len(ordered) > len(groups):
        block["bounds"] = ["summary_groups"]
        block["next"] = aggregate_call(reference_id, fields, offset=len(groups))
    return block


def fit_summary(
    page: Mapping[str, Any],
    *,
    budget: int,
    measure: Callable[[dict[str, Any]], int],
    reference_id: str,
) -> dict[str, Any]:
    """``page`` with its summary cut to the most groups that fit ``budget``.

    Byte fitting cannot shrink a metadata page by rerunning at a smaller
    limit (that would change the reference), so its summary groups are cut
    here instead, from the smallest, and the cut is named: the block's
    ``bounds`` gain ``output_bytes`` and its ``next`` reads the rest. When
    not even an empty summary fits, the page goes back with no groups and
    the byte-budget wrapper refuses it as it refuses any page that cannot
    fit. ``measure`` is the served size of a page.
    """

    out = dict(page)
    block = out.get("summary")
    if not isinstance(block, Mapping) or measure(out) <= budget:
        return out
    groups = list(block.get("groups") or [])
    if not groups:
        return out
    by = list(block.get("by") or [])

    def cut(count: int) -> dict[str, Any]:
        bounds = [b for b in block.get("bounds") or [] if b != "output_bytes"]
        trimmed = {
            **block,
            "groups": groups[:count],
            "bounds": [*bounds, "output_bytes"],
            "next": aggregate_call(reference_id, by, offset=count),
        }
        return {**out, "summary": trimmed}

    low, high, best = 0, len(groups) - 1, 0
    while low <= high:
        middle = (low + high) // 2
        if measure(cut(middle)) <= budget:
            best, low = middle, middle + 1
        else:
            high = middle - 1
    return cut(best)


def _unsupported(
    reason: str, *, message: str, fix_hint: str, argument: Optional[str] = None,
) -> ResultReferenceUnsupported:
    data: dict[str, Any] = {"reason": reason}
    if argument is not None:
        data["argument"] = argument
    return ResultReferenceUnsupported(message, data=data, fix_hint=fix_hint)


def unsupported(
    reason: str, *, message: str, fix_hint: str, argument: Optional[str] = None,
) -> ResultReferenceUnsupported:
    """A ``result_reference_unsupported`` refusal with its named reason."""

    return _unsupported(reason, message=message, fix_hint=fix_hint, argument=argument)


def binding_mismatch(
    stored: Mapping[str, Any], current: Mapping[str, Any],
) -> Optional[str]:
    """The first binding field that differs, by the name a refusal gives."""

    for key, named in _BINDING_FIELDS:
        if stored.get(key) != current.get(key):
            return named
    return None


def selection_fingerprint(fields: Mapping[str, Any]) -> str:
    """A digest of the arguments that decide a reference's members (L4, D13)."""

    return hashlib.sha256(_canonical(dict(fields))).hexdigest()


def expires_at_text(expires_at: float) -> str:
    """A reference's expiry as an ISO-8601 UTC timestamp."""

    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(expires_at)))


class ResultReferenceInvalid(SubstrateError, ValueError):
    """A result reference id cannot be used by this reader on this server."""

    code = "result_reference_invalid"


class ResultReferenceCapacity(SubstrateError):
    """A result reference does not fit the store's fixed limits."""

    code = "result_reference_capacity"


class ResultReferenceInternal(SubstrateError):
    """A producer broke the result-reference contract (a server bug)."""

    code = "result_reference_internal"


class ResultReferenceUnsupported(SubstrateError):
    """A read that cannot produce or consume a result reference (slices 2, 3).

    ``reason`` is ``method``, ``rank_by`` or ``argument`` (named in
    ``argument``: ``offset``, ``subtype_filter``, ``order_by``, ``retain``,
    ``select``, ``max_chars`` or ``summary``) for a form refused before any
    SQL. A pin the consumers could not read (no overlay marker) is refused
    ``overlay_generation_unmaterialized`` instead (PACT
    lane-s-storage-cutover).
    """

    code = "result_reference_unsupported"


_MESSAGES = {
    "malformed": "the result reference id is malformed",
    "store_lost": "the result reference was issued by a store this server process no longer holds (the server restarted, or another process served it)",
    "signature_invalid": "the result reference id signature is invalid",
    "expired": "the result reference has expired",
    "evicted": "the result reference was evicted to make room for this reader's newer references",
    "unknown": "the result reference is unknown to this reader",
    "generation_conflict": "the generation_id named differs from the result reference's pinned generation",
    "generation_unavailable": "the result reference's pinned generation can no longer be read",
    "binding_mismatch": "the result reference was made under a different binding than this read",
    "member_missing": "members of the result reference are no longer readable at its pin",
}


#: The same refusals for a live handle (PACT read-live-handles, 3.6): the
#: message says "handle", and an eviction can be any session's doing.
_HANDLE_MESSAGES = {
    **{
        reason: text.replace("the result reference", "the handle")
        for reason, text in _MESSAGES.items()
    },
    "evicted": "the handle was evicted to make room for newer handles",
    "unknown": "the handle is unknown to this reader",
}


def _invalid(
    reason: str, fix_hint: str = _FIX_HINT, noun: str = "result reference",
) -> ResultReferenceInvalid:
    # Only the reason: never the id, a count or anything that differs between
    # another reader's id and an id that was never issued.
    messages = _HANDLE_MESSAGES if noun == "handle" else _MESSAGES
    return ResultReferenceInvalid(
        messages[reason], data={"reason": reason}, fix_hint=fix_hint,
    )


#: A handle id of either store: `r1.` (graph) or `l1.` (live).
_ANY_ID_PATTERN = re.compile(
    r"(?:r1|l1)\.[A-Za-z0-9_-]{8}\.[0-9]{1,12}\.[A-Za-z0-9_-]{16}\.[A-Za-z0-9_-]{22}"
)


def is_handle_id(value: Any) -> bool:
    """Whether `value` has the form of a handle id of either store.

    The form only: a node id contains `type:` and never matches.
    """

    return isinstance(value, str) and _ANY_ID_PATTERN.fullmatch(value.strip()) is not None


def string_input_shape(value: Any) -> tuple[str, Any]:
    """The one rule for what a string `input` is (PACT read-live-handles, N7).

    ``("handle", id)`` for a bare handle id, a JSON string literal of one
    (``'"l1..."'``), or a JSON object string that is exactly
    ``{"ref": "<id>"}``; ``("json", text)`` for any other text that starts
    with ``{`` or ``[``; ``("text", value)`` otherwise (a node id, or
    anything that is not a string). The grammar's handle rewrite and the
    JSON decoder of #3989 both ask this, so they never disagree.
    """

    if not isinstance(value, str):
        return "text", value
    stripped = value.strip()
    if is_handle_id(stripped):
        return "handle", stripped
    head = stripped[:1]
    if head == '"' and len(stripped) <= 160:
        try:
            decoded = json.loads(stripped)
        except ValueError:
            decoded = None
        if is_handle_id(decoded):
            return "handle", decoded.strip()
        return "text", value
    if head in ("{", "["):
        if head == "{" and len(stripped) <= 200:
            try:
                decoded = json.loads(stripped)
            except (ValueError, RecursionError):
                decoded = None
            if (
                isinstance(decoded, dict) and set(decoded) == {"ref"}
                and is_handle_id(decoded["ref"])
            ):
                return "handle", decoded["ref"].strip()
        return "json", value
    return "text", value


def invalid(reason: str, **data: Any) -> ResultReferenceInvalid:
    """A consumer-side ``result_reference_invalid`` (validation steps 5-9).

    ``data`` is what the reason may say: ``field`` for ``binding_mismatch``,
    ``count`` for ``member_missing``, ``cause`` for
    ``generation_unavailable``. Never a member id or another reader's fact.
    """

    return ResultReferenceInvalid(
        _MESSAGES[reason], data={"reason": reason, **data}, fix_hint=_FIX_HINT,
    )


def _replaced_invalid(
    newer: str, added: int, holds_all: bool,
) -> ResultReferenceInvalid:
    """`replaced`: a newer chain handle stands for this one's chain (PACT
    read-multi-handle-input, N2). Said to the owner only."""

    holds = (
        f"which holds these answers and {added} more" if holds_all and added
        else "which holds these answers" if holds_all
        else "which holds the chain's newest answers"
    )
    return ResultReferenceInvalid(
        f"this handle was replaced by {newer}, {holds}; read {newer} with no "
        "new upstream calls",
        data={"reason": "replaced", "replaced_by": newer},
        fix_hint=f'read input="{newer}" instead',
    )


def _internal(message: str) -> ResultReferenceInternal:
    return ResultReferenceInternal(
        message, data={"reason": "contract"}, fix_hint="report this server bug",
    )


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")


def canonical_bytes(binding: Mapping[str, Any], members: Sequence[Mapping[str, Any]]) -> int:
    """The measured and enforced size of one reference."""

    return len(_canonical({"binding": dict(binding), "members": [dict(m) for m in members]}))


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _mac_key(label: bytes = _MAC_LABEL) -> bytes:
    from okg.substrate.mcp.continuation import _signing_key

    return hmac.new(_signing_key(), label, hashlib.sha256).digest()


def _mac(prefix: str, label: bytes = _MAC_LABEL) -> str:
    digest = hmac.new(_mac_key(label), prefix.encode("ascii"), hashlib.sha256).digest()
    return _b64(digest[:_MAC_BYTES])


@dataclass(frozen=True)
class Draft:
    """A minted reference that is not in the store yet."""

    id: str
    reader_key: str
    binding: Mapping[str, Any]
    members: tuple[Mapping[str, Any], ...]
    parent: Optional[str]
    expires_at: float
    canonical_bytes: int
    #: The authenticated principal on a multi-user server, else None.
    principal_key: Optional[str] = None
    #: A sibling store's opaque payload (members are then empty).
    payload: Any = None
    #: What the caps count by: the reader plus its logical session (a live
    #: handle), or None for the reader alone.
    quota_key: Optional[str] = None
    #: A chain handle (PACT read-multi-handle-input): the chain it stands
    #: for, its sequence number (only a higher one replaces a lower one at
    #: commit), and the ids it pins against its owner's own eviction.
    replaces_key: Optional[str] = None
    seq: int = 0
    pins: tuple[str, ...] = ()


@dataclass(frozen=True)
class ResultReference:
    """A committed reference, as a lookup returns it (a private copy)."""

    id: str
    reader_key: str
    binding: Mapping[str, Any]
    members: tuple[Mapping[str, Any], ...]
    parent: Optional[str]
    expires_at: float
    canonical_bytes: int
    #: The authenticated principal on a multi-user server, else None.
    principal_key: Optional[str] = None
    #: A sibling store's opaque payload (members are then empty).
    payload: Any = None
    #: What the caps count by: the reader plus its logical session (a live
    #: handle), or None for the reader alone.
    quota_key: Optional[str] = None
    #: A chain handle's chain, sequence number and pinned ids (see `Draft`).
    replaces_key: Optional[str] = None
    seq: int = 0
    pins: tuple[str, ...] = ()


def _normalize_members(members: Any) -> list[dict[str, Any]]:
    if not isinstance(members, Sequence) or isinstance(members, (str, bytes)):
        raise _internal("result reference members must be a list")
    normalized: list[dict[str, Any]] = []
    for member in members:
        if not isinstance(member, Mapping) or not set(member) <= _MEMBER_KEYS:
            raise _internal(
                "a result reference member holds only node_id, rank and location"
            )
        node_id = member.get("node_id")
        rank = member.get("rank")
        if not isinstance(node_id, str) or not node_id:
            raise _internal("a result reference member needs a node_id")
        if not isinstance(rank, int) or isinstance(rank, bool):
            raise _internal("a result reference member needs an integer rank")
        normalized.append(
            {"node_id": node_id, "rank": rank, "location": member.get("location")}
        )
    return normalized


class ResultReferenceStore:
    """One process's references. Every mutation holds one lock."""

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.time,
        limits: StoreLimits = GRAPH_LIMITS,
    ) -> None:
        self._clock = clock
        self.limits = limits
        self._pattern = _id_pattern(limits.id_version)
        self.epoch = _b64(secrets.token_bytes(6))
        self._lock = threading.Lock()
        self._entries: dict[str, ResultReference] = {}
        #: Drafts holding a reservation (``reserve_on_mint``), by id.
        self._pending: dict[str, Draft] = {}
        #: reader key -> its committed ids, oldest first.
        self._by_reader: dict[str, OrderedDict[str, None]] = {}
        #: principal key -> its committed ids across reader keys, oldest first.
        self._by_principal: dict[str, OrderedDict[str, None]] = {}
        self._children: dict[str, set[str]] = {}
        #: evicted id -> (reader key, expires_at, quota key), oldest first.
        self._evicted: OrderedDict[str, tuple[str, float, Optional[str]]] = OrderedDict()
        #: quota key -> its committed ids, oldest first (an entry with no
        #: quota key of its own counts under its reader key).
        self._by_quota: dict[str, OrderedDict[str, None]] = {}
        #: expired id -> (reader key, quota key, forget_at): who owned an
        #: entry the purge dropped, so another session of its reader hears
        #: `unknown` for it, not `expired` (lead decision, read-live-handles
        #: Q1). Bounded like the evicted records.
        self._expired_owners: OrderedDict[str, tuple[str, Optional[str], float]] = (
            OrderedDict()
        )
        #: replaced id -> (reader key, quota key, expires_at, newer id, ids
        #: the newer adds, whether it holds every id of the replaced one):
        #: its owner hears `replaced` naming the newer id (PACT
        #: read-multi-handle-input, N2). Bounded like the evicted records.
        self._replaced: OrderedDict[
            str, tuple[str, Optional[str], float, str, int, bool]
        ] = OrderedDict()
        #: chain key -> the committed chain handle that stands for it.
        self._chains: dict[str, str] = {}
        self._bytes = 0

    # -- introspection ------------------------------------------------------

    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    def total_bytes(self) -> int:
        with self._lock:
            return self._bytes

    def evicted_records(self) -> int:
        with self._lock:
            return len(self._evicted)

    def principal_count(self, principal_key: Optional[str]) -> int:
        """Live references one principal holds across its reader keys."""

        with self._lock:
            return len(self._by_principal.get(principal_key or "", ()))

    def usage(self, reader_key: str) -> dict[str, int]:
        """One reader's own usage: what a refusal may show that reader."""

        with self._lock:
            return self._usage_locked(reader_key)

    def _usage_locked(
        self, reader_key: str, quota_key: Optional[str] = None,
    ) -> dict[str, int]:
        ids = self._by_quota.get(quota_key or reader_key, OrderedDict())
        return {
            "references": len(ids),
            "bytes": sum(self._entries[i].canonical_bytes for i in ids),
        }

    def quota_count(self, quota_key: str) -> int:
        """Live entries one quota key holds (a test and evidence seam)."""

        with self._lock:
            return len(self._by_quota.get(quota_key, ()))

    # -- ids ----------------------------------------------------------------

    def _invalid(self, reason: str) -> ResultReferenceInvalid:
        return _invalid(reason, self.limits.fix_hint, self.limits.noun)

    def _check_id(self, reference_id: Any) -> float:
        """Steps 1-3 of validation, before the lock: form, store, MAC, expiry."""

        if not isinstance(reference_id, str) or len(reference_id) > 128:
            raise self._invalid("malformed")
        match = self._pattern.fullmatch(reference_id)
        if match is None:
            raise self._invalid("malformed")
        epoch, expires, _nonce, supplied_mac = match.groups()
        if not hmac.compare_digest(epoch, self.epoch):
            raise self._invalid("store_lost")
        prefix = reference_id.rsplit(".", 1)[0]
        if not hmac.compare_digest(supplied_mac, _mac(prefix, self.limits.mac_label)):
            raise self._invalid("signature_invalid")
        expires_at = float(int(expires))
        if self._clock() >= expires_at:
            raise self._invalid("expired")
        return expires_at

    @staticmethod
    def _other_session(
        owner_reader: str, owner_quota: Optional[str], reader_key: str,
        quota_key: Optional[str],
    ) -> bool:
        """The same reader in another logical session (or transport scope)."""

        if quota_key is None or not hmac.compare_digest(owner_reader, reader_key):
            return False
        owner = owner_quota if owner_quota is not None else owner_reader
        return not hmac.compare_digest(owner, quota_key)

    def _resolve_locked(
        self, reference_id: str, reader_key: str, quota_key: Optional[str] = None,
    ) -> ResultReference:
        entry = self._entries.get(reference_id)
        if entry is not None and hmac.compare_digest(entry.reader_key, reader_key):
            if self._other_session(entry.reader_key, entry.quota_key, reader_key, quota_key):
                # Another session of this reader: exactly as never issued,
                # and the owner's entry stays (lead decision Q1).
                raise self._invalid("unknown")
            return entry
        evicted = self._evicted.get(reference_id)
        if evicted is not None and hmac.compare_digest(evicted[0], reader_key):
            if self._other_session(evicted[0], evicted[2], reader_key, quota_key):
                raise self._invalid("unknown")
            raise self._invalid("evicted")
        replaced = self._replaced.get(reference_id)
        if replaced is not None and hmac.compare_digest(replaced[0], reader_key):
            if self._other_session(replaced[0], replaced[1], reader_key, quota_key):
                raise self._invalid("unknown")
            raise _replaced_invalid(replaced[3], replaced[4], replaced[5])
        raise self._invalid("unknown")

    def held_for(
        self, reference_id: Any, *, reader_key: str, quota_key: Optional[str] = None,
    ) -> Optional[str]:
        """``"committed"``, ``"pending"`` (a draft not yet served) or None:
        whether this reader and quota key still hold `reference_id`. Under
        the lock, with no decode and no gate (PACT read-multi-handle-input,
        8.3): a hint counts a sibling call's draft as kept."""

        if not isinstance(reference_id, str):
            return None
        now = self._clock()
        quota = quota_key or reader_key
        with self._lock:
            entry = self._entries.get(reference_id)
            if entry is not None and now < entry.expires_at and hmac.compare_digest(
                entry.reader_key, reader_key,
            ) and hmac.compare_digest(entry.quota_key or entry.reader_key, quota):
                return "committed"
            draft_ = self._pending.get(reference_id)
            if draft_ is not None and now < draft_.expires_at and hmac.compare_digest(
                draft_.reader_key, reader_key,
            ) and hmac.compare_digest(draft_.quota_key or draft_.reader_key, quota):
                return "pending"
        return None

    def _expired_for(
        self, reference_id: str, reader_key: str, quota_key: Optional[str],
    ) -> ResultReferenceInvalid:
        """`expired`, or `unknown` to another session of the owning reader."""

        with self._lock:
            entry = self._entries.get(reference_id)
            owner: Optional[tuple[str, Optional[str]]] = (
                (entry.reader_key, entry.quota_key) if entry is not None else None
            )
            if owner is None and reference_id in self._evicted:
                record = self._evicted[reference_id]
                owner = (record[0], record[2])
            if owner is None and reference_id in self._expired_owners:
                record_ = self._expired_owners[reference_id]
                owner = (record_[0], record_[1])
        if owner is not None and self._other_session(owner[0], owner[1], reader_key, quota_key):
            return self._invalid("unknown")
        return self._invalid("expired")

    def lookup(
        self, reference_id: Any, *, reader_key: str, quota_key: Optional[str] = None,
    ) -> ResultReference:
        """The reader's live reference, or a typed ``result_reference_invalid``.

        Order: malformed, store_lost, signature_invalid, expired, then under
        the lock evicted (this reader's only) or unknown. Another reader's id
        answers exactly as an id that was never issued. With ``quota_key``
        (a live handle read), another logical session of the same reader
        answers ``unknown`` too, for a live, an evicted or an expired id,
        and the owner's entry is left as it was.
        """

        try:
            self._check_id(reference_id)
        except ResultReferenceInvalid as exc:
            if (exc.data or {}).get("reason") == "expired" and quota_key is not None:
                raise self._expired_for(str(reference_id), reader_key, quota_key) from None
            raise
        with self._lock:
            entry = self._resolve_locked(reference_id, reader_key, quota_key)
        # A sibling store whose payload is immutable hands back the entry
        # itself: copying an 8 MiB set on every read is the cost it avoids.
        return copy.deepcopy(entry) if self.limits.copy_on_lookup else entry

    def discard(self, reference_id: str) -> bool:
        """Drop one committed entry (a consumer refused it); True if it was there.

        The id then answers ``unknown``, never ``evicted``: it was dropped,
        not displaced by the reader's newer entry.
        """

        with self._lock:
            return self._remove_locked(reference_id) is not None

    def release(self, reference_id: str) -> None:
        """Give back a draft's reservation; a no-op for any other id."""

        with self._lock:
            self._pending.pop(reference_id, None)

    def pending_count(self) -> int:
        with self._lock:
            return len(self._pending)

    # -- produce ------------------------------------------------------------

    def mint(
        self,
        *,
        reader_key: str,
        binding: Mapping[str, Any],
        members: Sequence[Mapping[str, Any]],
        parent: Optional[str] = None,
        header_claim: Optional[Mapping[str, Any]],
        multi_user: bool,
        payload: Any = None,
        payload_bytes: int = 0,
        payload_rows: int = 0,
        quota_key: Optional[str] = None,
        replaces_key: Optional[str] = None,
        seq: int = 0,
        pins: Sequence[str] = (),
    ) -> Draft:
        """A draft id and its contents; nothing is stored or counted yet.

        A sibling store passes an opaque ``payload`` with its measured
        ``payload_bytes`` and ``payload_rows`` instead of members; the
        per-reference caps then count those. With ``reserve_on_mint`` the
        draft also reserves its place against the server and byte caps
        here, and a full store refuses here rather than at commit.

        The per-reference caps refuse here. A child takes its parent's
        expiry, and the parent must be this reader's live reference.
        ``header_claim`` and ``multi_user`` name the principal the
        per-principal cap counts against (none on a single-user server).
        Both are required, so no producer skips the cap by omission.
        """
        from okg.substrate.mcp.continuation import principal_key


        if not isinstance(reader_key, str) or not reader_key:
            raise _internal("a result reference needs a reader key")
        if not isinstance(binding, Mapping):
            raise _internal("a result reference binding must be a mapping")
        lim = self.limits
        normalized = _normalize_members(members)
        if payload is not None and normalized:
            raise _internal("a result reference holds members or a payload, not both")
        count = len(normalized) if payload is None else int(payload_rows)
        if count > lim.max_members:
            raise ResultReferenceCapacity(
                f"a result reference holds at most {lim.max_members} members",
                data={"reason": "reference_members", "members": count,
                      "limits": lim.advertised()},
                fix_hint="narrow the read or lower its limit",
            )
        try:
            size = (
                canonical_bytes(binding, normalized) if payload is None
                else int(payload_bytes)
            )
            stored_binding = json.loads(_canonical(dict(binding)))
        except (TypeError, ValueError) as exc:
            raise _internal("a result reference must be JSON-serializable") from exc
        if size > lim.max_reference_bytes:
            raise ResultReferenceCapacity(
                f"a result reference holds at most {lim.max_reference_bytes} canonical bytes",
                data={"reason": "reference_bytes", "bytes": size, "limits": lim.advertised()},
                fix_hint="narrow the read or lower its limit",
            )
        if parent is not None:
            expires_at = self.lookup(parent, reader_key=reader_key).expires_at
        else:
            expires_at = float(math.ceil(self._clock()) + lim.lifetime_seconds)
        prefix = (
            f"{lim.id_version}.{self.epoch}.{int(expires_at)}."
            f"{_b64(secrets.token_bytes(12))}"
        )
        minted = Draft(
            id=f"{prefix}.{_mac(prefix, lim.mac_label)}",
            reader_key=reader_key,
            binding=stored_binding,
            members=tuple(normalized),
            parent=parent,
            expires_at=expires_at,
            canonical_bytes=size,
            principal_key=principal_key(header_claim, multi_user),
            payload=payload,
            quota_key=quota_key,
            replaces_key=replaces_key,
            seq=int(seq),
            pins=tuple(str(i) for i in pins),
        )
        if lim.reserve_on_mint:
            with self._lock:
                self._purge_expired_locked(self._clock())
                self._plan_locked(minted)
                self._pending[minted.id] = minted
        return minted

    def _purge_expired_locked(self, now: float) -> None:
        for reference_id in [i for i, e in self._entries.items() if now >= e.expires_at]:
            entry = self._remove_locked(reference_id)
            if entry is not None and entry.quota_key is not None:
                # Who owned it, for one more lifetime: another session of
                # its reader then hears `unknown`, never `expired`.
                self._expired_owners[reference_id] = (
                    entry.reader_key, entry.quota_key,
                    entry.expires_at + self.limits.lifetime_seconds,
                )
                while len(self._expired_owners) > MAX_EVICTED_RECORDS:
                    self._expired_owners.popitem(last=False)
        for reference_id in [i for i, record in self._evicted.items() if now >= record[1]]:
            del self._evicted[reference_id]
        for reference_id in [i for i, record in self._replaced.items() if now >= record[2]]:
            del self._replaced[reference_id]
        for reference_id in [
            i for i, record in self._expired_owners.items() if now >= record[2]
        ]:
            del self._expired_owners[reference_id]
        for reference_id in [i for i, d in self._pending.items() if now >= d.expires_at]:
            del self._pending[reference_id]

    def _remove_locked(self, reference_id: str) -> Optional[ResultReference]:
        entry = self._entries.pop(reference_id, None)
        if entry is None:
            return None
        self._bytes -= entry.canonical_bytes
        reader_ids = self._by_reader[entry.reader_key]
        del reader_ids[reference_id]
        if not reader_ids:
            del self._by_reader[entry.reader_key]
        quota = entry.quota_key or entry.reader_key
        quota_ids = self._by_quota.get(quota)
        if quota_ids is not None:
            quota_ids.pop(reference_id, None)
            if not quota_ids:
                del self._by_quota[quota]
        if entry.principal_key is not None:
            principal_ids = self._by_principal[entry.principal_key]
            del principal_ids[reference_id]
            if not principal_ids:
                del self._by_principal[entry.principal_key]
        self._children.pop(reference_id, None)
        if entry.parent is not None and entry.parent in self._children:
            self._children[entry.parent].discard(reference_id)
        if entry.replaces_key is not None and self._chains.get(entry.replaces_key) == reference_id:
            del self._chains[entry.replaces_key]
        return entry

    def _pinned_locked(self, draft: Draft) -> set[str]:
        """Ids the live chain handles pin, read at eviction time (a source
        pending at chain mint is protected once it commits), with the
        draft's own and every reserved draft's."""

        pinned: set[str] = set(draft.pins)
        for reference_id, entry in self._entries.items():
            pinned.update(entry.pins)
            if entry.pins:
                # A chain handle holds its own place as its sources do: an
                # unpinned entry goes first, then the oldest pinned one.
                pinned.add(reference_id)
        for pending in self._pending.values():
            pinned.update(pending.pins)
        return pinned

    def _record_replaced_locked(self, old: Any, newer: Draft | ResultReference) -> None:
        """`old` (an entry or a draft) answers `replaced` naming `newer`."""

        added = len(set(newer.pins) - set(old.pins))
        self._replaced[old.id] = (
            old.reader_key, old.quota_key, old.expires_at, newer.id, added,
            set(old.pins) <= set(newer.pins),
        )
        self._replaced.move_to_end(old.id)
        while len(self._replaced) > MAX_EVICTED_RECORDS:
            self._replaced.popitem(last=False)

    def _with_descendants_locked(self, reference_id: str) -> list[str]:
        found: list[str] = []
        pending = [reference_id]
        while pending:
            current = pending.pop()
            if current in self._entries and current not in found:
                found.append(current)
                pending.extend(self._children.get(current, ()))
        return found

    def _ancestors_locked(self, parent: Optional[str]) -> set[str]:
        ancestors: set[str] = set()
        while parent is not None and parent not in ancestors:
            ancestors.add(parent)
            entry = self._entries.get(parent)
            parent = entry.parent if entry is not None else None
        return ancestors

    def commit(self, draft: Draft) -> ResultReference:
        """Store ``draft``; the only step that consumes capacity.

        Under one lock: drop expired entries; at the reader's own cap pick
        its oldest reference (never an ancestor of the draft) to evict with
        its children; then, on a multi-user server, do the same for the
        principal's cap across its reader keys; refuse if the server count
        or the total bytes would still exceed their caps; then evict and
        insert. Nothing changes before the last check passes.
        """

        if not isinstance(draft, Draft):
            raise _internal("only a minted draft can be committed")
        with self._lock:
            now = self._clock()
            self._purge_expired_locked(now)
            if now >= draft.expires_at:
                raise self._invalid("expired")
            if draft.id in self._entries:
                raise _internal("a result reference draft was committed twice")
            if draft.parent is not None:
                self._resolve_locked(draft.parent, draft.reader_key)

            # A chain handle (PACT read-multi-handle-input, N1/N2): only a
            # higher sequence number replaces the live one of its chain; a
            # lower one that commits later is committed as already replaced.
            replacing: Optional[str] = None
            if draft.replaces_key is not None:
                live_id = self._chains.get(draft.replaces_key)
                live = self._entries.get(live_id) if live_id is not None else None
                if live is not None and live.seq > draft.seq:
                    self._record_replaced_locked(draft, live)
                    self._pending.pop(draft.id, None)
                    return copy.deepcopy(live) if self.limits.copy_on_lookup else live
                if live is not None:
                    replacing = live.id

            evict = self._plan_locked(draft, replacing=replacing)

            self._after_capacity_check()
            if replacing is not None:
                old = self._remove_locked(replacing)
                if old is not None:
                    self._record_replaced_locked(old, draft)
            for reference_id in (i for i in evict if i != replacing):
                entry = self._remove_locked(reference_id)
                if entry is None:
                    raise _internal("an evicted result reference vanished under the lock")
                self._evicted[reference_id] = (
                    entry.reader_key, entry.expires_at, entry.quota_key,
                )
                while len(self._evicted) > MAX_EVICTED_RECORDS:
                    self._evicted.popitem(last=False)

            entry = ResultReference(
                id=draft.id,
                reader_key=draft.reader_key,
                binding=draft.binding,
                members=draft.members,
                parent=draft.parent,
                expires_at=draft.expires_at,
                canonical_bytes=draft.canonical_bytes,
                principal_key=draft.principal_key,
                payload=draft.payload,
                quota_key=draft.quota_key,
                replaces_key=draft.replaces_key,
                seq=draft.seq,
                pins=draft.pins,
            )
            self._entries[draft.id] = entry
            if draft.replaces_key is not None:
                self._chains[draft.replaces_key] = draft.id
            self._by_reader.setdefault(draft.reader_key, OrderedDict())[draft.id] = None
            self._by_quota.setdefault(
                draft.quota_key or draft.reader_key, OrderedDict(),
            )[draft.id] = None
            if draft.principal_key is not None:
                self._by_principal.setdefault(draft.principal_key, OrderedDict())[draft.id] = None
            if draft.parent is not None:
                self._children.setdefault(draft.parent, set()).add(draft.id)
            self._bytes += draft.canonical_bytes
            self._pending.pop(draft.id, None)
        return copy.deepcopy(entry) if self.limits.copy_on_lookup else entry

    def _plan_locked(self, draft: Draft, *, replacing: Optional[str] = None) -> list[str]:
        """The ids committing ``draft`` evicts, or a capacity refusal.

        Every eviction is worked out first; the server and byte caps are
        checked against the state after them, counting every other draft's
        reservation; nothing changes here, so a refused commit evicts
        nothing. `replacing` is the chain handle the draft replaces: it
        leaves at commit, so it is counted gone (and is first in the list).

        The owner's own count, byte and per-principal eviction skips the ids
        live chain handles pin (PACT read-multi-handle-input, N3). A pin
        never causes a refusal (P1): when only pinned ids are left, the
        oldest pinned one goes, and the chain's next read names it.
        """

        lim = self.limits
        quota = draft.quota_key or draft.reader_key
        usage = self._usage_locked(draft.reader_key, quota)
        if draft.principal_key is not None:
            usage["principal_references"] = len(
                self._by_principal.get(draft.principal_key, ())
            )
        ancestors = self._ancestors_locked(draft.parent)
        own = self._by_quota.get(quota, OrderedDict())
        evict: list[str] = [replacing] if replacing is not None and replacing in self._entries else []
        pinned = self._pinned_locked(draft)

        def oldest_of(ids: Any) -> Optional[str]:
            """The oldest evictable id: unpinned first, then pinned (P1)."""
            free = [i for i in ids if i not in ancestors and i not in evict]
            return next((i for i in free if i not in pinned), None) or next(iter(free), None)

        if sum(1 for i in own if i not in evict) >= lim.max_per_reader:
            oldest = oldest_of(own)
            if oldest is None:
                raise self._capacity("reader_references", usage)
            evict += [i for i in self._with_descendants_locked(oldest) if i not in evict]
        if lim.max_quota_bytes is not None:
            # The quota key's own bytes: its oldest go first, as at its
            # count cap (never an ancestor of the draft; pinned ids last).
            held = usage["bytes"] - sum(
                self._entries[i].canonical_bytes for i in evict if i in own
            )
            while held + draft.canonical_bytes > lim.max_quota_bytes:
                candidate = oldest_of(own)
                if candidate is None:
                    break
                for gone in self._with_descendants_locked(candidate):
                    if gone not in evict:
                        evict.append(gone)
                        if gone in own:
                            held -= self._entries[gone].canonical_bytes
            if held + draft.canonical_bytes > lim.max_quota_bytes:
                raise self._capacity("reader_bytes", usage)
        if draft.principal_key is not None:
            # One principal varying its authority argument gets many
            # reader keys; this cap bounds them together (rrcap A).
            principal_ids = self._by_principal.get(draft.principal_key, OrderedDict())
            if sum(1 for i in principal_ids if i not in evict) >= lim.max_per_principal:
                oldest = oldest_of(principal_ids)
                if oldest is None:
                    raise self._capacity("principal_references", usage)
                evict += [
                    i for i in self._with_descendants_locked(oldest) if i not in evict
                ]
        reserved = [d for i, d in self._pending.items() if i != draft.id]
        reserved_bytes = sum(d.canonical_bytes for d in reserved)

        def over() -> Optional[str]:
            freed = sum(self._entries[i].canonical_bytes for i in evict)
            if len(self._entries) - len(evict) + len(reserved) + 1 > lim.max_per_server:
                return "server_references"
            if (
                self._bytes - freed + reserved_bytes + draft.canonical_bytes
                > lim.max_total_bytes
            ):
                return "total_bytes"
            return None

        reason = over()
        while reason is not None and lim.fair_eviction:
            # M1: the oldest entry of the quota key that holds the most
            # (ties: the most bytes), never an ancestor of the draft, one at
            # a time until the draft fits. A cut reply is then never refused
            # because other sessions filled the server.
            # Under the byte cap the holder of the most bytes goes first (review
            # F3): evicting a session's small handles frees almost nothing.
            victim = self._largest_holders_oldest_locked(
                ancestors, evict, by_bytes=reason == "total_bytes",
            )
            if victim is None:
                break
            evict += [i for i in self._with_descendants_locked(victim) if i not in evict]
            reason = over()
        if reason is not None:
            raise self._capacity(reason, usage)
        return evict

    def _largest_holders_oldest_locked(
        self, ancestors: set[str], evict: Sequence[str], *, by_bytes: bool = False,
    ) -> Optional[str]:
        """The oldest evictable entry of the quota key holding the most:
        entries, then bytes; with `by_bytes`, bytes, then entries."""

        gone = set(evict)
        best: Optional[tuple[int, int, str]] = None
        choice: Optional[str] = None
        for quota, ids in self._by_quota.items():
            left = [i for i in ids if i not in gone]
            candidates = [i for i in left if i not in ancestors]
            if not candidates:
                continue
            size = sum(self._entries[i].canonical_bytes for i in left)
            rank = (size, len(left), quota) if by_bytes else (len(left), size, quota)
            if best is None or rank[:2] > best[:2] or (rank[:2] == best[:2] and quota < best[2]):
                best, choice = rank, candidates[0]
        return choice

    def _after_capacity_check(self) -> None:
        """A seam between the capacity check and the insert, under the lock.

        Tests widen the race window here to prove the lock is what makes the
        check and the insert one step.
        """

    def _capacity(self, reason: str, usage: Mapping[str, int]) -> ResultReferenceCapacity:
        data: dict[str, Any] = {
            "reason": reason,
            "reader_references": usage["references"],
            "reader_bytes": usage["bytes"],
        }
        if "principal_references" in usage:
            data["principal_references"] = usage["principal_references"]
        data["limits"] = self.limits.advertised()
        return ResultReferenceCapacity(
            "the result reference store is full; this reference was not kept",
            data=data,
            fix_hint="retry after older references expire, or read without a reference",
        )


_STORE_LOCK = threading.Lock()
_STORE = ResultReferenceStore()


def store() -> ResultReferenceStore:
    """This process's store."""

    with _STORE_LOCK:
        return _STORE


def install_store(replacement: ResultReferenceStore) -> ResultReferenceStore:
    """Replace the process store and return the previous one (a test seam)."""

    global _STORE
    with _STORE_LOCK:
        previous, _STORE = _STORE, replacement
    return previous


# -- the request-local draft ----------------------------------------------------


class DraftScope:
    """The drafts of one read call, one per attempt, until the call ends.

    The byte-budget wrapper runs a read several times while it fits the
    response; each run is an attempt. A producer drafts at most one
    reference per attempt. Only the draft whose id the final returned
    payload carries (``payload["result_reference"]["id"]``), when that
    payload is not a refusal, goes on; every other draft is released.
    Drafts from every attempt are kept to the end because the served page
    need not come from the last rerun.

    Inside an MCP call (a :class:`ServeScope` is open) that draft is handed
    up and committed only once the served result is built (design F11).
    A direct Python caller of the wrapped function has no serve scope, so
    the draft is committed when the wrapper returns.

    Each wrapped call opens its own scope, so a wrapped read that called
    another wrapped read would commit the inner one independently. No
    production tool nests: ``tool_registration`` wraps each tool once.
    """

    def __init__(self, store_: ResultReferenceStore) -> None:
        self.store = store_
        #: id -> (the store that minted it, the draft).
        self._drafts: dict[str, tuple[ResultReferenceStore, Draft]] = {}
        #: Run with the final payload before any draft is committed; a
        #: raise fails the call and releases every draft.
        self._on_finish: list[Callable[[Any], None]] = []
        self._attempt_drafted = False
        #: Run once when the read call ends, however it ends (the decode
        #: budget's reservations, PACT read-live-handles 4.1.5).
        self._on_close: list[Callable[[], None]] = []
        #: Shared by every attempt of this call: a producer freezes its
        #: members on the first (full-limit) attempt, so a byte-fitting
        #: rerun at a smaller limit shrinks delivered rows, never the
        #: reference.
        self.memo: dict[str, Any] = {}

    def begin_attempt(self) -> None:
        self._attempt_drafted = False

    def draft(
        self,
        *,
        reader_key: str,
        binding: Mapping[str, Any],
        members: Sequence[Mapping[str, Any]],
        parent: Optional[str] = None,
        header_claim: Optional[Mapping[str, Any]],
        multi_user: bool,
    ) -> str:
        if self._attempt_drafted:
            raise _internal("a read attempt drafted a second result reference")
        minted = self.store.mint(
            reader_key=reader_key, binding=binding, members=members, parent=parent,
            header_claim=header_claim, multi_user=multi_user,
        )
        self._attempt_drafted = True
        self._drafts[minted.id] = (self.store, minted)
        return minted.id

    def adopt(self, store_: ResultReferenceStore, draft_: Draft) -> None:
        """Hold a draft another store minted (a live result set) to this call's end."""

        self._drafts[draft_.id] = (store_, draft_)

    def on_finish(self, callback: Callable[[Any], None]) -> None:
        self._on_finish.append(callback)

    def finish(self, payload: Any) -> Optional[ResultReference]:
        """Run the finish callbacks, commit the served drafts, release the rest."""

        callbacks, self._on_finish = self._on_finish, []
        for callback in callbacks:
            callback(payload)
        drafts, self._drafts = self._drafts, {}
        served = _served_reference_ids(payload)
        serve = _SERVE.get()
        committed: Optional[ResultReference] = None
        adopted: set[str] = set()
        try:
            for draft_id, (store_, draft_) in drafts.items():
                if draft_id in served and serve is not None:
                    serve.adopt(store_, draft_)
                    adopted.add(draft_id)
                elif draft_id in served:
                    committed = store_.commit(draft_)
        finally:
            # Every draft not handed up gives its reservation back, a raised
            # commit included, so one failure never strands the others. A
            # committed draft holds no reservation, so this is a no-op there.
            for draft_id, (store_, _draft) in drafts.items():
                if draft_id not in adopted:
                    store_.release(draft_id)
        return committed

    def release(self) -> None:
        drafts, self._drafts = self._drafts, {}
        self._on_finish = []
        for draft_id, (store_, _draft) in drafts.items():
            store_.release(draft_id)

    def close(self) -> None:
        """Run the close callbacks once, in the order they were registered."""

        callbacks, self._on_close = self._on_close, []
        for callback in callbacks:
            callback()


def _served_reference_ids(payload: Any) -> set[str]:
    """The draft ids a final payload serves: the read's own reference, and a
    live result set's under ``external_live.result_reference``."""

    if not isinstance(payload, Mapping) or payload.get("status") == "refused":
        return set()
    found: set[str] = set()
    live = payload.get("external_live")
    for holder in (payload, live if isinstance(live, Mapping) else {}):
        reference = holder.get("result_reference")
        if isinstance(reference, Mapping) and isinstance(reference.get("id"), str):
            found.add(reference["id"])
    handle = payload.get("handle")
    if isinstance(handle, Mapping):
        # A set read's handle, and the set an aggregate grouped (PACT
        # read-live-handles, 3.7): both are drafts of this call.
        for key in ("id", "from"):
            if isinstance(handle.get(key), str):
                found.add(handle[key])
    if isinstance(live, Mapping) and isinstance(live.get("next"), str):
        # A cut live answer's `next` token holds its kept set's id (#3666).
        from okg.substrate.mcp.live_result_sets import next_set_id

        set_id = next_set_id(live["next"])
        if set_id is not None:
            found.add(set_id)
    combined = live.get("chain_hint") if isinstance(live, Mapping) else None
    if isinstance(combined, Mapping) and isinstance(combined.get("chain"), str):
        # A hand chain's chain handle (PACT read-multi-handle-input): a
        # draft of this call, kept only when the reply prints its hint.
        found.add(combined["chain"])
    return found


class ServeScope:
    """The drafts one MCP call hands up, committed at the served point.

    ``ByteBoundedFastMCP`` opens it around a whole call and calls
    :func:`commit_served` with the built ``CallToolResult``. A draft is
    committed only when that result is not an error and its id appears in
    the served content (the text block, or ``structuredContent``). Anything
    else -- a refusal, a raised error, a result that does not carry the id
    -- releases it when the scope closes. A reference committed before a
    failed transport write lives until it expires.
    """

    def __init__(self) -> None:
        self._drafts: list[tuple[ResultReferenceStore, Draft]] = []

    def adopt(self, store_: ResultReferenceStore, draft_: Draft) -> None:
        self._drafts.append((store_, draft_))

    def commit_served(self, result: Any) -> list[ResultReference]:
        drafts, self._drafts = self._drafts, []
        if not drafts:
            return []
        if getattr(result, "isError", True) is not False:
            for store_, draft_ in drafts:
                store_.release(draft_.id)
            return []
        served = _served_content(result)
        committed: list[ResultReference] = []
        try:
            for store_, draft_ in drafts:
                if draft_.id in served:
                    committed.append(store_.commit(draft_))
        finally:
            # Every draft gives its reservation back, a raised commit
            # included; a committed draft holds none, so this is a no-op.
            for store_, draft_ in drafts:
                store_.release(draft_.id)
        return committed

    def release(self) -> None:
        drafts, self._drafts = self._drafts, []
        for store_, draft_ in drafts:
            store_.release(draft_.id)


def _served_content(result: Any) -> str:
    parts = [
        text
        for block in getattr(result, "content", None) or ()
        if isinstance(text := getattr(block, "text", None), str)
    ]
    structured = getattr(result, "structuredContent", None)
    if structured is not None:
        parts.append(json.dumps(structured, ensure_ascii=False, default=str))
    return "\n".join(parts)


_SCOPE: ContextVar[Optional[DraftScope]] = ContextVar(
    "okg_mcp_result_reference_drafts", default=None,
)
_SERVE: ContextVar[Optional[ServeScope]] = ContextVar(
    "okg_mcp_result_reference_serve", default=None,
)


@contextlib.contextmanager
def serve_scope() -> Iterator[ServeScope]:
    """One serve scope for one MCP call; releases whatever it did not commit."""

    scope = ServeScope()
    token = _SERVE.set(scope)
    try:
        yield scope
    finally:
        scope.release()
        _SERVE.reset(token)


def commit_served(result: Any) -> list[ResultReference]:
    """Commit the open serve scope's drafts that ``result`` serves."""

    scope = _SERVE.get()
    return [] if scope is None else scope.commit_served(result)


@contextlib.contextmanager
def draft_scope() -> Iterator[DraftScope]:
    """One draft scope for one read call; released on any exit not committed."""

    scope = DraftScope(store())
    token = _SCOPE.set(scope)
    try:
        yield scope
    finally:
        try:
            scope.release()
        finally:
            scope.close()
            _SCOPE.reset(token)


T = TypeVar("T")


def attempt(fn: Callable[..., T]) -> Callable[..., T]:
    """``fn`` with each run marked as a new attempt of the open scope."""

    @functools.wraps(fn)
    def run(*args: Any, **kwargs: Any) -> T:
        scope = _SCOPE.get()
        if scope is not None:
            scope.begin_attempt()
        return fn(*args, **kwargs)

    return run


def adopt_draft(store_: ResultReferenceStore, draft_: Draft) -> None:
    """Hold ``draft_`` in the open read call until its final payload.

    Committed only when that payload serves its id; released otherwise.
    Outside a read call it is a server bug and refuses.
    """

    scope = _SCOPE.get()
    if scope is None:
        store_.release(draft_.id)
        raise _internal("a result reference was drafted outside a read call")
    scope.adopt(store_, draft_)


def on_finish(callback: Callable[[Any], None]) -> None:
    """Run ``callback(final_payload)`` once when the open read call returns.

    It runs before any draft is committed; a raise fails the call. Outside a
    read call it is a server bug and refuses.
    """

    scope = _SCOPE.get()
    if scope is None:
        raise _internal("a finish callback was registered outside a read call")
    scope.on_finish(callback)


def on_close(callback: Callable[[], None]) -> None:
    """Run ``callback()`` once when the open read call ends, however it ends.

    Outside a read call it is a server bug and refuses.
    """

    scope = _SCOPE.get()
    if scope is None:
        raise _internal("a close callback was registered outside a read call")
    scope._on_close.append(callback)  # noqa: SLF001 — the scope's own list.


def call_memo() -> Optional[dict[str, Any]]:
    """The open read call's memo, shared by its attempts; None outside one."""

    scope = _SCOPE.get()
    return None if scope is None else scope.memo


def draft(
    *,
    reader_key: str,
    binding: Mapping[str, Any],
    members: Sequence[Mapping[str, Any]],
    parent: Optional[str] = None,
    header_claim: Optional[Mapping[str, Any]],
    multi_user: bool,
) -> str:
    """Draft a reference in the current read call and return its id.

    ``header_claim`` and ``multi_user`` are the same pre-session inputs the
    caller passed to ``continuation.reader_key``; on a multi-user server they
    name the principal whose 128-reference cap the commit applies.

    The id is usable only after the call returns a payload carrying it. A
    draft outside a read call is a server bug and refuses. That includes a
    draft from a worker thread the producer started: a new thread does not
    inherit the call's context, so it sees no scope and refuses
    ``result_reference_internal`` rather than drafting into nothing.
    """

    scope = _SCOPE.get()
    if scope is None:
        raise _internal("a result reference was drafted outside a read call")
    return scope.draft(
        reader_key=reader_key, binding=binding, members=members, parent=parent,
        header_claim=header_claim, multi_user=multi_user,
    )
