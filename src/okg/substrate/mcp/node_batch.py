"""Several records in one inspect call, within one byte budget (#3657).

`inspect(target={"kind": "nodes", ...})` reads up to `MAX_NODE_BATCH` records
by id, or a graph result reference's members in rank order, through the same
single-record read as `inspect(node)`. The served page never exceeds the
call's budget: records that fit a fair share are served whole, the rest share
what is left and are cut, marked with the same `attrs_cut` and `read_whole`
a cut single node read carries (#3647), and records that cannot be
served even cut are listed in `omitted` with a signed `next` that continues
them. No record is dropped silently.

The batch keeps no server-side state: the token signs the request (ids or
reference, select, budget) and carries only the offset of the first record
not yet served and the page's stamp reserve.
"""
from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any, Optional

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.continuation import (
    MAX_DECODED_TOKEN_BYTES,
    MAX_ENCODED_TOKEN_CHARS,
    ContinuationTokenInvalid,
    consume_continuation,
    continuation_binding,
    continuation_body_bytes,
    issue_continuation,
    open_continuation,
)
from okg.substrate.mcp.operator_models import MAX_NODE_BATCH
from okg.substrate.mcp.text_batch import _page_reserve

#: Per-record fields the page already carries once.
_PAGE_FIELDS = ("generation_id", "pinned_generation_id")
#: Single-record refusals that mean "this record is not readable here"; any
#: other error (authority, pin, deployment) refuses the whole batch.
_NOT_VISIBLE_CODES = frozenset({"node_not_found", "node_not_visible"})
#: Times a page whose parts measured within budget but whose whole did not
#: is retried with smaller shares before one record fewer is tried.
_SHRINK_TRIES = 3
#: Bytes added to a refusal's needed budget, for the few bytes a page of
#: another size renders differently.
_REQUIRED_SPARE = 16


class NodeBatchBudgetTooSmall(SubstrateError):
    code = "node_batch_budget_too_small"


class NodeBatchStateTooLarge(SubstrateError):
    code = "node_batch_state_too_large"


def shape_refusal(target: Any) -> Optional[tuple[str, str, list[str]]]:
    """Pure. The exact fix for a `nodes` target of the wrong shape, or a
    `node` target asked for several records: `(message, fix_hint, fields)`,
    None when the shape is not one of these. Only counts are echoed, never
    the caller's ids."""
    if not isinstance(target, Mapping):
        return None
    kind, ids = target.get("kind"), target.get("node_ids")
    form = 'target={"kind":"nodes","node_ids":[...]}'
    if kind == "node":
        if "node_ids" in target or isinstance(target.get("node_id"), (list, tuple)):
            field = "node_ids" if "node_ids" in target else "node_id"
            return (
                "inspect kind 'node' reads one record by `node_id` (a string)",
                f"to read several records in one call pass {form}",
                [f"target.node.{field}"],
            )
        return None
    if kind != "nodes":
        return None
    if "node_id" in target:
        return (
            "inspect kind 'nodes' takes `node_ids` (a list of ids), not `node_id`",
            f"pass {form}, or kind 'node' with node_id for one record",
            ["target.nodes.node_id"],
        )
    if "hydrate" in target:
        return (
            "inspect kind 'nodes' does not take `hydrate`",
            "remove hydrate; to overlay live external state read that one "
            'record with target={"kind":"node","node_id":"<id>","hydrate":true}',
            ["target.nodes.hydrate"],
        )
    has_ids, has_ref = ids is not None, target.get("ref") is not None
    if has_ids == has_ref:
        return (
            "inspect kind 'nodes' takes exactly one of `node_ids` or `ref`, "
            + ("got both" if has_ids else "got neither"),
            f"pass {form} for ids you already have, or "
            'target={"kind":"nodes","ref":"<result reference id>"} for a '
            "retained search, filter or map result; not both",
            ["target.nodes.node_ids", "target.nodes.ref"],
        )
    if has_ids and not isinstance(ids, (list, tuple)):
        return (
            "inspect kind 'nodes' takes `node_ids` as a list of ids",
            f"pass {form}, a list even for one id",
            ["target.nodes.node_ids"],
        )
    if isinstance(ids, (list, tuple)):
        if any(not isinstance(i, str) or not i for i in ids):
            return (
                "inspect kind 'nodes' needs every entry of `node_ids` to be a "
                "nonempty string id",
                "pass each id as the `node_id` string a search or expand row gave",
                ["target.nodes.node_ids"],
            )
        if not ids:
            return (
                "inspect kind 'nodes' needs at least one id in `node_ids`",
                f"pass {form} with 1 to {MAX_NODE_BATCH} ids",
                ["target.nodes.node_ids"],
            )
        if len(ids) > MAX_NODE_BATCH:
            return (
                f"inspect kind 'nodes' reads at most {MAX_NODE_BATCH} `node_ids` "
                f"per call, got {len(ids)}",
                f"pass the first {MAX_NODE_BATCH} ids now and the other "
                f"{len(ids) - MAX_NODE_BATCH} in another call",
                ["target.nodes.node_ids"],
            )
        if len(set(ids)) != len(ids):
            return (
                "inspect kind 'nodes' needs unique `node_ids`: "
                f"{len(ids) - len(set(ids))} of them repeat an earlier id",
                "pass each id once",
                ["target.nodes.node_ids"],
            )
    return None


def _next_token(binding: dict[str, Any], offset: int, reserve: int) -> str:
    cursor = {"offset": offset, "reserve": reserve}
    if continuation_body_bytes(binding, cursor) >= MAX_DECODED_TOKEN_BYTES:
        raise NodeBatchStateTooLarge(
            "the batch request is too large for a usable continuation",
            data={"max_decoded_token_bytes": MAX_DECODED_TOKEN_BYTES - 1},
            fix_hint="pass fewer or shorter node_ids per call",
        )
    token = issue_continuation(binding, cursor=cursor)
    if len(token) > MAX_ENCODED_TOKEN_CHARS:
        raise NodeBatchStateTooLarge(
            "the batch request is too large for a usable continuation",
            data={"max_encoded_token_chars": MAX_ENCODED_TOKEN_CHARS},
            fix_hint="pass fewer or shorter node_ids per call",
        )
    open_continuation(token)
    return token


def _cursor(cursor: dict[str, Any], count: int) -> tuple[int, int]:
    offset, reserve = cursor.get("offset"), cursor.get("reserve")
    valid = (
        isinstance(offset, int) and not isinstance(offset, bool) and 0 < offset < count
        and isinstance(reserve, int) and not isinstance(reserve, bool) and reserve >= 0
    )
    if not valid:
        raise ContinuationTokenInvalid(
            "the batch cursor has no valid offset", data={"reason": "cursor_invalid"},
        )
    return offset, reserve


def _cuttable(attrs: Mapping[str, Any]) -> tuple[list[str], list[str]]:
    """The attribute keys the cutter may shorten (text with characters) and
    the ones it may leave out whole (lists and objects)."""
    from okg.substrate.mcp.response_bounds import _text_length

    texts = [k for k, v in attrs.items() if isinstance(v, str) and _text_length(v) > 0]
    whole = [k for k, v in attrs.items()
             if v is not None and not isinstance(v, (str, int, float, bool))]
    return texts, whole


def _size(value: Any) -> int:
    # The single-node cutter's measure of a value's full size.
    return len(json.dumps(value, default=str, ensure_ascii=False).encode("utf-8"))


def _cut_form(record: dict[str, Any], keep: int, dropped: frozenset[str]) -> dict[str, Any]:
    """`record` with every text attribute cut to its first `keep` characters
    (an untrusted fence kept whole) and the `dropped` lists and objects left
    out, each named in `attrs_cut` exactly as a cut `inspect(node)` names it,
    with the same `read_whole` call. Status `cut`."""
    from okg.substrate.mcp.response_bounds import _cut_value, _text_length, attr_read_call

    attrs = dict(record.get("attrs") or {})
    cut: dict[str, Any] = {}
    # A left-out list or object is in the record's select=["attrs"] read,
    # not its text: about the JSON size of its whole attributes plus room
    # for the envelope.
    read = (
        attr_read_call(record.get("node_id"), _size(attrs) + _size(record.get("provenance")) + 2048)
        if dropped else None
    )
    texts, _ = _cuttable(attrs)
    for key in texts:
        value = attrs[key]
        length = _text_length(value)
        if length > keep:
            attrs[key] = _cut_value(value, keep)
            cut[key] = {"bytes": _size(value), "chars": length, "served_chars": keep}
    for key in dropped:
        cut[key] = {"bytes": _size(attrs[key]), "served_chars": 0}
        if read is not None:
            cut[key]["read"] = read
        attrs[key] = None
    out = {**record, "status": "cut", "attrs": attrs, "attrs_cut": cut}
    if isinstance(record.get("node_id"), str):
        out["read_whole"] = {"tool": "inspect", "target": {"kind": "text", "node_id": record["node_id"]}}
    return out


def _floor(record: dict[str, Any]) -> dict[str, Any]:
    """`record` as small as it can be cut: every text attribute emptied,
    every list or object left out. A share below this cannot serve it."""
    _, whole = _cuttable(record.get("attrs") or {})
    return _cut_form(record, 0, frozenset(whole))


def _cut_to(
    record: dict[str, Any], share: int, cost: Callable[[dict[str, Any]], int],
) -> Optional[dict[str, Any]]:
    """`record` cut to cost at most `share`, or None when even its floor
    costs more. Every long text keeps the same number of leading characters
    (so long fields share the room and each keeps its start); lists and
    objects are left out, largest first, only when emptying the text is not
    enough. A bounded number of measures: one binary search over the kept
    length per set of dropped values."""
    from okg.substrate.mcp.response_bounds import _MIN_KEPT_CHARS, _text_length

    attrs = record.get("attrs") or {}
    texts, whole = _cuttable(attrs)
    longest = max((_text_length(attrs[k]) for k in texts), default=0)
    by_size = sorted(whole, key=lambda k: (-_size(attrs[k]), k))
    for count in range(len(by_size) + 1):
        dropped = frozenset(by_size[:count])
        if cost(_cut_form(record, 0, dropped)) > share:
            continue
        low, high = 0, longest
        while low < high:
            middle = (low + high + 1) // 2
            if cost(_cut_form(record, middle, dropped)) <= share:
                low = middle
            else:
                high = middle - 1
        if low < _MIN_KEPT_CHARS:
            # A few dozen characters say less than an empty value does.
            low = 0
        return _cut_form(record, low, dropped)
    return None


def _shares(costs: list[int], available: int) -> list[Optional[int]]:
    """Pure. Fair shares of `available` bytes: a record whose whole cost fits
    an equal share of what is left is served whole (None); the others split
    the rest equally. Deterministic in request order."""
    shares: list[Optional[int]] = [None] * len(costs)
    left, open_ = available, list(range(len(costs)))
    changed = True
    while changed and open_:
        changed = False
        share = left // len(open_)
        for i in list(open_):
            if costs[i] <= share:
                left -= costs[i]
                open_.remove(i)
                changed = True
    if open_:
        share = left // len(open_)
        for i in open_:
            shares[i] = share
    return shares


def read_node_batch(
    conn: Any,
    session: Any,
    *,
    node_ids: Optional[list[str]],
    ref: Optional[str],
    resolve_ref: Callable[[str], list[str]],
    read_node: Callable[[str], Optional[dict[str, Any]]],
    select: Optional[list[str]],
    max_bytes: int,
    measure: Callable[[dict[str, Any]], int],
    continuation_token: Optional[str] = None,
    reserve_bytes: int = 0,
) -> dict[str, Any]:
    """One page of a batch record read.

    `read_node(id)` is the single-record read (`inspect(node)`'s payload, or
    None when the record is not visible). `measure` returns the page's served
    bytes without the stamps added after it; `reserve_bytes` is those stamps'
    measured cost, which the token carries so a retried token cuts the same
    page.
    """
    binding = continuation_binding(
        conn, session, kind="inspect_node_batch", sort="request-order-v1",
        projection={"node_ids": node_ids, "ref": ref, "select": select, "max_bytes": max_bytes},
    )
    offset, token_reserve = 0, None
    cursor = None
    if continuation_token is not None:
        # The whole read context is compared before any record is read.
        cursor = consume_continuation(continuation_token, expected_binding=binding)
    ids = list(node_ids) if node_ids is not None else list(resolve_ref(str(ref)))
    count = len(ids)
    if cursor is not None:
        offset, token_reserve = _cursor(cursor, count)
    reserve = _page_reserve(reserve_bytes, token_reserve, max_bytes)
    budget = max_bytes - reserve
    window = ids[offset:offset + MAX_NODE_BATCH]

    tokens: dict[int, str] = {}

    def token_for(rest: int) -> str:
        # Signed once per offset: a page is measured many times while it is
        # fitted, and the token for one offset is always the same.
        if rest not in tokens:
            tokens[rest] = _next_token(binding, rest, reserve)
        return tokens[rest]

    def page_for(records: list[dict[str, Any]], served: int, *, stated: int = max_bytes) -> dict[str, Any]:
        rest = offset + served
        page: dict[str, Any] = {
            "status": "ok", "result_kind": "node_batch",
            "generation_id": session.generation_id,
            "pinned_generation_id": session.generation_id,
            "max_bytes": stated,
            "request_count": count,
            "offset": offset,
            "records": records,
            "complete": rest >= count,
            "truncated": rest < count,
        }
        if ref is not None:
            # A reference holds the rows its search retained, which can be
            # fewer than the search matched: the page says how many it reads.
            page["reference_members"] = count
        cut_count = sum(1 for record in records if record.get("status") == "cut")
        if cut_count:
            # A page whose records were shortened never reads as whole.
            page["cut_count"] = cut_count
        omitted = window[served:]
        if omitted:
            page["omitted"] = omitted
        if rest < count:
            page["remaining_count"] = count - rest
            page["continuation_token"] = token_for(rest)
        return page

    def cost_of(record: dict[str, Any], skeleton_bytes: int, served: int) -> int:
        return measure(page_for([record], served)) - skeleton_bytes

    if not window:
        return page_for([], 0)
    # Each record's whole cost and its floor (every attribute value cut) are
    # measured once, against the full page's skeleton: a record's cost does
    # not depend on how many others share the page.
    full_skeleton = measure(page_for([], len(window)))
    entries: list[dict[str, Any]] = []
    costs: list[int] = []
    floors: list[int] = []
    floor_sum = 0
    for node_id in window:
        # Records are read in request order and only while one more could
        # still be served: a page serves a prefix of the window, and a
        # prefix whose floors alone pass the budget cannot fit. A small
        # budget over a long list therefore reads a few records, not fifty.
        if entries and floor_sum > budget:
            break
        try:
            payload = read_node(node_id)
        except SubstrateError as error:
            if error.code not in _NOT_VISIBLE_CODES:
                raise
            payload = None
        if payload is None:
            entry: dict[str, Any] = {"node_id": node_id, "status": "not_visible"}
        else:
            entry = {"status": "ok", **{k: v for k, v in payload.items() if k not in _PAGE_FIELDS}}
        cost = cost_of(entry, full_skeleton, len(window))
        floor = cost_of(_floor(entry), full_skeleton, len(window)) if entry["status"] == "ok" else cost
        entries.append(entry)
        costs.append(cost)
        floors.append(min(floor, cost))
        floor_sum += floors[-1]

    def feasible(served: int) -> Optional[tuple[int, list[Optional[int]]]]:
        skeleton_bytes = measure(page_for([], served))
        available = budget - skeleton_bytes
        if available <= 0:
            return None
        shares = _shares(costs[:served], available)
        if any(share is not None and share < floor for share, floor in zip(shares, floors)):
            return None
        return skeleton_bytes, shares

    for served in range(len(entries), 0, -1):
        plan = feasible(served)
        if plan is None:
            continue
        skeleton_bytes, shares = plan
        part = entries[:served]
        slack = 0
        for _ in range(_SHRINK_TRIES):
            records: list[dict[str, Any]] = []
            for record, share in zip(part, shares):
                if share is None or record["status"] != "ok":
                    records.append(record)
                    continue
                shortened = _cut_to(
                    record, max(share - slack, 0),
                    lambda value: cost_of(value, skeleton_bytes, served),
                )
                if shortened is None:
                    break
                records.append(shortened)
            if len(records) < served:
                break
            page = page_for(records, served)
            over = measure(page) - budget
            if over <= 0:
                return page
            # The parts were measured alone; the whole page renders a little
            # differently. Take the difference out of every cut share.
            cut_shares = sum(1 for share in shares if share is not None)
            if not cut_shares:
                break
            slack += over // cut_shares + 1
    # The least that serves the first record: its floor, alone on a page.
    # The page states its own max_bytes, so the figure is measured with the
    # figure in it (a longer number is a longer page).
    first = entries[0]
    smallest = _floor(first) if first["status"] == "ok" else first
    required = max_bytes
    for _ in range(3):
        required = measure(page_for([smallest], 1, stated=required)) + reserve + _REQUIRED_SPARE
    raise NodeBatchBudgetTooSmall(
        "not even one record of the batch fits the response budget",
        data={"max_bytes": max_bytes, "required_budget_bytes": required},
        fix_hint="increase max_bytes within the deployment ceiling or narrow select",
    )
