"""Fair source-text delivery with one real wire budget (issue3221).

The batch owns no server-retained text state. A signed token holds only the
original request, unfinished positions and the page's stamp reserve. Source
preparation is reused for allocation; the existing row cap does not bound text
bytes or transient memory.
"""
from __future__ import annotations

import copy
from dataclasses import replace
from collections.abc import Callable
from typing import Any

from pydantic import ValidationError

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
from okg.substrate.mcp.operator_models import TTexts, as_mapping
from okg.substrate.mcp.provenance import classify_sources
from okg.substrate.mcp.record_text import (
    MAX_RECORD_OCCURRENCES,
    TEXT_READ_RESERVE_MARGIN,
    PreparedRecordText,
    LineSpan,
    line_spans,
    lines_past_end,
    parse_line_target,
    prepare_record_text,
    render_piece,
    span_lines,
    spans_from,
    whole_lines,
)

#: Extra room a newly chosen stamp reserve carries, so that a retry whose
#: stamps grew a little still keeps the token's reserve and cuts the same
#: page. It covers digit drift: the publish age (0.1 s steps) gains at most
#: eight digits between a fresh pin and a year-old one, and the session's
#: pin-scope label differs by four bytes between calls. A freshness failure
#: needs no room here: the server measures the stamps at their unavailable
#: shape too and passes the larger. What it does not cover is a source
#: crossing the stale threshold between calls, whose notice names sources;
#: that chooses a new reserve, as the windows search does. Kept small: at a
#: 4096-byte page every byte reserved is a byte of source text not sent.
TEXT_BATCH_DRIFT_BYTES = 32


def _page_reserve(stamp_bytes: int, token_reserve: int | None, max_bytes: int) -> int:
    """Pure. The bytes a page keeps free below ``max_bytes`` for the stamps.

    The windows search's rule (`record_window_search.page_reserve`) with the
    single text read's spare: a token's reserve is kept while this call's
    measured stamps fit it with the full `TEXT_READ_RESERVE_MARGIN` to spare,
    so a retried token cuts the same page. Otherwise (a first page, or stamps
    that outgrew the token's reserve) the reserve is the measured stamps plus
    that spare plus `TEXT_BATCH_DRIFT_BYTES`. A new reserve only moves the cut.
    """
    need = max(stamp_bytes, 0)
    if token_reserve is not None and need + TEXT_READ_RESERVE_MARGIN <= token_reserve < max_bytes:
        return token_reserve
    return need + TEXT_READ_RESERVE_MARGIN + TEXT_BATCH_DRIFT_BYTES


class BatchStateTooLarge(SubstrateError):
    code = "text_batch_state_too_large"


class BatchBudgetTooSmall(SubstrateError):
    code = "text_batch_budget_too_small"


def _next_token(
    binding: dict[str, Any], positions: dict[int, tuple[int, int] | None], rotation: int, *, reserve: int,
) -> str | None:
    if not positions:
        return None
    cursor = {
        "positions": [
            {"i": i, **({"start": True} if position is None else {"segment": position[0], "char": position[1]})}
            for i, position in sorted(positions.items())
        ],
        "rotation": rotation,
        # The page's stamp reserve travels with the cursor, so a retried
        # token cuts its page at the same characters (`_page_reserve`).
        "reserve": reserve,
    }
    if continuation_body_bytes(binding, cursor) >= MAX_DECODED_TOKEN_BYTES:
        raise BatchStateTooLarge(
            "the batch request is too large for a usable continuation",
            data={"max_decoded_token_bytes": MAX_DECODED_TOKEN_BYTES - 1},
            fix_hint="split the request into smaller batches",
        )
    token = issue_continuation(binding, cursor=cursor)
    if len(token) > MAX_ENCODED_TOKEN_CHARS:
        raise BatchStateTooLarge(
            "the batch request is too large for a usable continuation",
            data={"max_encoded_token_chars": MAX_ENCODED_TOKEN_CHARS},
            fix_hint="split the request into smaller batches",
        )
    # Use the actual shared parser, so no apparently valid page emits an
    # unusable next token at either compression or serialized boundary.
    open_continuation(token)
    return token


def _positions(cursor: dict[str, Any], count: int) -> tuple[dict[int, tuple[int, int] | None], int, int]:
    rows, rotation, reserve = cursor.get("positions"), cursor.get("rotation"), cursor.get("reserve")
    if not isinstance(rows, list) or not rows or not isinstance(rotation, int) or isinstance(rotation, bool) or not 0 <= rotation < count:
        raise ContinuationTokenInvalid("the batch cursor has no valid item positions", data={"reason": "cursor_invalid"})
    if not isinstance(reserve, int) or isinstance(reserve, bool) or reserve < 0:
        raise ContinuationTokenInvalid("the batch cursor has no valid stamp reserve", data={"reason": "cursor_invalid"})
    positions: dict[int, tuple[int, int] | None] = {}
    for row in rows:
        if not isinstance(row, dict):
            raise ContinuationTokenInvalid("the batch cursor has no valid item positions", data={"reason": "cursor_invalid"})
        i = row.get("i")
        if not isinstance(i, int) or isinstance(i, bool) or not 0 <= i < count or i in positions:
            raise ContinuationTokenInvalid("the batch cursor has no valid item positions", data={"reason": "cursor_invalid"})
        if row.get("start") is True and set(row) == {"i", "start"}:
            positions[i] = None
        else:
            segment, char = row.get("segment"), row.get("char")
            if set(row) != {"i", "segment", "char"} or any(not isinstance(v, int) or isinstance(v, bool) or v < 0 for v in (segment, char)):
                raise ContinuationTokenInvalid("the batch cursor has no valid item positions", data={"reason": "cursor_invalid"})
            positions[i] = (segment, char)
    return positions, rotation, reserve


def _refused(error: SubstrateError) -> dict[str, Any]:
    # Item ids are already attributed once. Do not duplicate arbitrary huge
    # caller ids or source blobs inside a refusal's metadata.
    data = getattr(error, "data", None)
    reason = data.get("reason") if isinstance(data, dict) else None
    out: dict[str, Any] = {"code": error.code, "message": "requested source text is unavailable"}
    if isinstance(reason, str) and len(reason) <= 128:
        out["reason"] = reason
    if error.code == "record_text_too_large":
        # Reading it alone refuses the same way; only a narrower read helps.
        out["fix_hint"] = (
            "this source has too many chunk occurrences to read whole; search "
            "within it for a location, or page its chunks with expand"
        )
    else:
        out["fix_hint"] = "use a fresh search location or read the source separately"
    return out


def read_text_batch(
    conn: Any,
    session: Any,
    items: list[dict[str, Any]],
    *,
    max_bytes: int,
    measure: Callable[[dict[str, Any]], int],
    continuation_token: str | None = None,
    reserve_bytes: int = 0,
) -> dict[str, Any]:
    """Return one deterministic page of the original keyed requests.

    ``measure`` returns the page's served bytes WITHOUT the stamps the server
    adds after it (freshness ages and blocks vary per call), so the cut
    depends only on the page, the token and ``max_bytes``. ``reserve_bytes``
    is this call's measured stamp cost, which `_page_reserve` turns into the
    bytes kept free below ``max_bytes``; the token carries that reserve.
    """
    try:
        target = TTexts.model_validate({"kind": "texts", "items": items})
    except ValidationError as error:
        raise SubstrateError(
            "batch text needs one to eight uniquely keyed node/location targets",
            data={"reason": "invalid_batch_items"},
            fix_hint="use unique nonempty keys of at most 128 characters and node_id or at",
        ) from error
    items = as_mapping(target)["items"]
    # Every item's `lines` is checked before any source is read, so a bad
    # one refuses the call by name and no item is half served.
    for index, item in enumerate(items):
        parse_line_target(item.get("lines"), item.get("at"), field=f"target.items[{index}]")
    count = len(items)
    binding = continuation_binding(
        conn, session, kind="inspect_text_batch", sort="original-item-rotation-v1",
        projection={"items": items, "max_bytes": max_bytes},
    )
    positions: dict[int, tuple[int, int] | None] = {i: None for i in range(count)}
    rotation = 0
    token_reserve: int | None = None
    if continuation_token is not None:
        # This full context comparison precedes every source preparation.
        cursor = consume_continuation(continuation_token, expected_binding=binding)
        positions, rotation, token_reserve = _positions(cursor, count)
    reserve = _page_reserve(reserve_bytes, token_reserve, max_bytes)
    order = sorted(positions, key=lambda i: (i - rotation) % count)
    next_rotation = (order[0] + 1) % count
    page: dict[str, Any] = {
        "status": "ok", "result_kind": "text_batch", "items": [],
        "generation_id": session.generation_id, "pinned_generation_id": session.generation_id,
        "max_bytes": max_bytes, "request_item_count": count,
        "total_count_unit": "item", "total_count_set": "current_page", "total_count_basis": "exact",
        "work": {"prepared_items": 0, "prepared_visible_segments": 0, "prepared_visible_text_bytes": 0,
                 "occurrence_limit_per_edge_family": MAX_RECORD_OCCURRENCES},
    }
    entries = {
        i: {"key": items[i]["key"], "node_id": items[i].get("node_id") or items[i]["at"]["chunk"],
            "state": "deferred", "segments": []}
        for i in order
    }

    def refresh() -> None:
        page["items"] = [entries[i] for i in order]
        page["total_count"] = len(entries)
        page["complete"] = not positions
        page["truncated"] = bool(positions)
        token = _next_token(binding, positions, next_rotation, reserve=reserve)
        if token is not None:
            page["continuation_token"] = token
        else:
            page.pop("continuation_token", None)
        page["coverage"] = {
            "retrieval": {"status": "unknown", "basis": "requested_source_text", "bounds": []},
            "selection": {"unit": "item", "basis": "input_set", "size": count, "limit": 8},
            "delivery": {"complete": not positions, "returned": len(entries), "next_available": bool(positions)},
        }

    def budget_refusal(required: int) -> None:
        raise BatchBudgetTooSmall(
            "the batch metadata and source progress do not fit the response budget",
            data={"max_bytes": max_bytes, "required_budget_bytes": required},
            fix_hint="increase max_bytes within the deployment ceiling or split the batch",
        )

    refresh()
    budget = max_bytes - reserve
    if measure(page) > budget:
        budget_refusal(measure(page) + reserve)

    prepared: dict[int, PreparedRecordText] = {}
    #: A line item's whole plan: the parts of its source that hold its lines.
    plans: dict[int, list[LineSpan]] = {}
    terminal_progress = False
    for i in order:
        try:
            source = prepare_record_text(
                conn, session, str(items[i].get("node_id") or ""),
                at=items[i].get("at"), lines=items[i].get("lines"),
                max_bytes=max_bytes,
                bind_context=False, classify_trust=False,
            )
        except SubstrateError as error:
            # Authority/pin/deployment errors are batch-global and must not
            # masquerade as ordinary per-source refusal.
            if error.code not in {"node_not_found", "text_location_stale", "record_text_too_large"}:
                raise
            entries[i].update(state="refused", error=_refused(error))
            positions.pop(i)
            terminal_progress = True
            continue
        prepared[i] = source
        page["work"]["prepared_items"] += 1
        page["work"]["prepared_visible_segments"] += len(source.segments)
        page["work"]["prepared_visible_text_bytes"] += source.prepared_text_bytes
        entries[i].update(
            node_id=source.node_id, subtype=source.payload.get("subtype"),
            source_coverage=source.payload.get("coverage"), order_basis=source.payload.get("order_basis"),
            requested_extent={"kind": "lines" if source.lines is not None
                              else "chunk" if source.payload.get("order_basis") == "single_chunk"
                              else "anchor_to_record_end" if items[i].get("at") else "record_start_to_end",
                              "leading_context_limit_chars": 200 if items[i].get("at") else 0},
        )
        if source.lines is not None and "lines" in source.payload:
            # What this item's line numbers are lines of, the last line, and
            # the ranges asked; each piece names the lines it holds.
            entries[i]["lines"] = dict(source.payload["lines"])
            if lines_past_end(source.lines, entries[i]["lines"]["total"]):
                entries[i]["lines"]["past_end"] = True
        # Like a single text read, say where a located extent began (the
        # text before it was not sent) and when an unhashed location was
        # checked by offset alone.
        for name in ("read_from", "location_check"):
            if name in source.payload:
                entries[i][name] = source.payload[name]
        position = positions[i] if positions[i] is not None else source.cursor
        if not source.segments:
            entries[i].update(state="delivered", outcome=source.payload.get("outcome"))
            positions.pop(i)
            terminal_progress = True
        else:
            segment, char = position
            if segment >= len(source.segments) or char >= max(1, len(source.segments[segment].text)):
                raise ContinuationTokenInvalid("the batch cursor no longer matches its source", data={"reason": "cursor_invalid"})
            if source.lines is not None:
                plans[i] = line_spans(source.segments, source.lines)
                rest = spans_from(plans[i], position)
                if not rest:
                    # Every asked line lies past the last one: nothing to send.
                    entries[i]["state"] = "delivered"
                    positions.pop(i)
                    terminal_progress = True
                    continue
                position = (rest[0].segment, rest[0].start)
            positions[i] = position
    if prepared:
        trust = classify_sources(
            conn, deployment_name=session.deployment_name,
            sources={segment.source for source in prepared.values() for segment in source.segments},
        )
        prepared = {i: replace(source, trust_by_source=trust) for i, source in prepared.items()}
    active = [i for i in order if i in positions]
    if active:
        next_rotation = (active[0] + 1) % count
    refresh()
    if measure(page) > budget:
        budget_refusal(measure(page) + reserve)
    source_progress = False

    def take(i: int, ceiling: int) -> bool:
        nonlocal source_progress
        source = prepared[i]
        moved = False
        while i in positions:
            segment_index, start = positions[i]  # type: ignore[misc]
            segment = source.segments[segment_index]
            original = copy.deepcopy(entries[i])
            original_position = positions[i]
            # Where this piece may run to, and where the item goes on after
            # it: to the segment's end and the next segment, or, for a line
            # item, to the end of the part that holds its lines and the next
            # such part.
            span: LineSpan | None = None
            if source.lines is None:
                stop = len(segment.text)
                after = (segment_index + 1, 0) if segment_index + 1 < len(source.segments) else None
            else:
                rest = spans_from(plans[i], (segment_index, start))
                span, stop = rest[0], rest[0].end
                after = (rest[1].segment, rest[1].start) if len(rest) > 1 else None

            def trial(end: int) -> bool:
                entries[i] = copy.deepcopy(original)
                entries[i]["segments"].append(render_piece(
                    segment, start=start, end=end,
                    trust=source.trust_by_source.get(segment.source, "untrusted"),
                    lines=span_lines(source.segments, span, end) if span is not None else None,
                ))
                if end == stop:
                    if after is None:
                        positions.pop(i, None)
                        entries[i]["state"] = "delivered"
                    else:
                        positions[i] = after
                        entries[i]["state"] = "partial"
                else:
                    positions[i] = (segment_index, end)
                    entries[i]["state"] = "partial"
                refresh()
                return measure(page) <= ceiling

            if trial(stop):
                moved = source_progress = True
                continue
            low, high, fit = start + 1, stop - 1, 0
            while low <= high:
                middle = (low + high) // 2
                if trial(middle):
                    fit, low = middle, middle + 1
                else:
                    high = middle - 1
            if fit and span is not None:
                # A line item stops after a whole line when one fits. The
                # shorter cut is measured too: a token's length is not
                # monotonic in its cursor, so it keeps the longer cut,
                # which did fit, when the shorter one does not.
                whole = start + whole_lines(segment.text, start, fit - start)
                if whole != fit and trial(whole):
                    fit = whole
            if fit:
                # Re-apply the largest fitting cut; trial() sets the entry
                # and position as its side effect, so it must run (not an
                # assert, which `python -O` strips) and must still fit.
                if not trial(fit):
                    raise RuntimeError(
                        "batch text allocation is not deterministic: a cut that "
                        "fit the budget no longer fits on re-application"
                    )
                moved = source_progress = True
            else:
                entries[i] = original
                positions[i] = original_position
                refresh()
            break
        return moved

    available = max(0, budget - measure(page))
    share = available // max(1, len(active))
    for i in active:
        take(i, min(budget, measure(page) + share))
    # One redistribution walk in the same immutable rotation order. It
    # reuses cached prepared text and cannot query the source again.
    for i in active:
        if i in positions:
            take(i, budget)
    refresh()
    if not source_progress and not terminal_progress and positions:
        i = active[0]
        segment_index, start = positions[i]  # type: ignore[misc]
        segment = prepared[i].segments[segment_index]
        trial_page = copy.deepcopy(page)
        # The probe is the piece a retry would serve: a line item's piece
        # names its lines, and leaving that out understated the size, so a
        # retry at the stated size was refused again with the same number.
        probe_lines = None
        if prepared[i].lines is not None:
            probe_span = spans_from(plans[i], (segment_index, start))[0]
            probe_lines = span_lines(prepared[i].segments, probe_span, start + 1)
        for entry in trial_page["items"]:
            if entry["key"] == items[i]["key"]:
                entry["segments"] = [render_piece(segment, start=start, end=start + 1,
                    trust=prepared[i].trust_by_source.get(segment.source, "untrusted"),
                    lines=probe_lines)]
                entry["state"] = "partial"
        # The retry signs its new size and position into the token, whose
        # length can move by a few bytes: the stated size leaves that room.
        budget_refusal(measure(trial_page) + reserve + TEXT_BATCH_DRIFT_BYTES)
    if measure(page) > budget:
        budget_refusal(measure(page) + reserve)
    return page
