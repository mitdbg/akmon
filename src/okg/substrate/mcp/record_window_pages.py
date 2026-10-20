"""Position-only paging of a deterministically replayed record-window plan.

Acquisition, membership and count proof belong to the scanner adapter. This
module fits actual served pages supplied by its envelope measurement callback.
Logical matches and attribution locations are finite parts, so even a window
with many matches or overlap aliases can be reached with a small byte budget.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.continuation import (
    ContinuationTokenInvalid,
    consume_continuation,
    issue_continuation,
)


class WindowBudgetTooSmall(SubstrateError):
    code = "window_budget_too_small"


class ContinuationTooLarge(Exception):
    """Raised by an ``issue_token`` hook: this trial page cannot carry its token.

    Internal to paging: the trial is treated as not fitting, so the pager
    tries a smaller page; only a page with no part at all is refused.
    """


@dataclass(frozen=True)
class WindowPart:
    """One match/location object or one original literal source fragment."""

    kind: str
    value: Mapping[str, Any]
    text: str | None = None


@dataclass(frozen=True)
class PlannedWindow:
    identity: str
    header: Mapping[str, Any]
    parts: tuple[WindowPart, ...]


@dataclass(frozen=True)
class PlannedRecord:
    identity: str
    header: Mapping[str, Any]
    windows: tuple[PlannedWindow, ...]


def _position(value: Any) -> tuple[int, int, int, int]:
    if not isinstance(value, list) or len(value) != 4 or not all(
        isinstance(item, int) and not isinstance(item, bool) and item >= 0
        for item in value
    ):
        raise ContinuationTokenInvalid("invalid record-window cursor position")
    return tuple(value)  # type: ignore[return-value]


def page_windows(
    records: Sequence[PlannedRecord],
    *,
    envelope: Mapping[str, Any],
    binding: Mapping[str, Any],
    limit: int,
    max_bytes: int,
    measure: Callable[[dict[str, Any]], int],
    render_fragment: Callable[[WindowPart, int, int], dict[str, Any]],
    continuation_token: str | None = None,
    reserve_bytes: int = 0,
    issue_token: Callable[[dict[str, Any], Callable[[str], int]], str] | None = None,
    consume_token: Callable[[str], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Return a nonempty progressing page, or refuse instead of losing parts.

    ``measure`` must include the final server envelope and SDK serialization.
    ``render_fragment`` preserves original coordinates and applies existing
    provenance rules to the exact served slice. Neither callback mutates plans.
    Limit counts distinct records appearing on this page, including a resumed
    record. Complete original budget and selection are already in binding.

    ``issue_token(cursor, measure_token)`` and ``consume_token(token)`` let an
    adapter use the shared scanner helpers (``read_scanner``); unset, the
    plain signed continuation under ``binding`` is used. ``reserve_bytes``
    is kept free below ``max_bytes`` for stamps that vary after measurement.
    """
    if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
        raise ValueError("window record limit must be a positive integer")
    if not isinstance(max_bytes, int) or isinstance(max_bytes, bool) or max_bytes < 4096:
        raise ValueError("window byte budget must be an integer of at least 4096")
    if not isinstance(reserve_bytes, int) or isinstance(reserve_bytes, bool) or not (
        0 <= reserve_bytes < max_bytes
    ):
        raise ValueError("window reserve must be a nonnegative integer below max_bytes")
    fit_bytes = max_bytes - reserve_bytes

    def issue(cursor: dict[str, Any], measure_token: Callable[[str], int]) -> str:
        if issue_token is not None:
            return issue_token(cursor, measure_token)
        return issue_continuation(binding, cursor=cursor)

    # A record with no windows (an undecided one) still lists its header:
    # it pages as one header-only part, so the cursor can stop on it.
    records = [
        record if record.windows else PlannedRecord(record.identity, record.header, (
            PlannedWindow("", {}, (WindowPart("header", {}),)),
        ))
        for record in records
    ]
    start = (0, 0, 0, 0)
    if continuation_token is not None:
        state = (
            consume_token(continuation_token) if consume_token is not None
            else consume_continuation(continuation_token, expected_binding=binding)
        )
        if set(state) != {"position"}:
            raise ContinuationTokenInvalid("invalid record-window cursor fields")
        start = _position(state["position"])

    def normalize(pos: tuple[int, int, int, int]) -> tuple[int, int, int, int]:
        r, w, p, char = pos
        if r > len(records):
            raise ContinuationTokenInvalid("record-window cursor exceeds record set")
        while r < len(records):
            if w > len(records[r].windows):
                raise ContinuationTokenInvalid("record-window cursor exceeds windows")
            if w == len(records[r].windows):
                if p or char:
                    raise ContinuationTokenInvalid("invalid terminal window position")
                r, w = r + 1, 0
                continue
            parts = records[r].windows[w].parts
            if p > len(parts):
                raise ContinuationTokenInvalid("record-window cursor exceeds parts")
            if p == len(parts):
                if char:
                    raise ContinuationTokenInvalid("invalid terminal part position")
                w, p = w + 1, 0
                continue
            part = parts[p]
            if char and (part.text is None or char >= len(part.text)):
                raise ContinuationTokenInvalid("record-window cursor exceeds fragment")
            return r, w, p, char
        if w or p or char:
            raise ContinuationTokenInvalid("invalid terminal record position")
        return r, 0, 0, 0

    pos = normalize(start)
    first_record = pos[0]
    page: dict[str, Any] = {**deepcopy(dict(envelope)), "results": []}
    listed: dict[int, dict[str, Any]] = {}
    byte_stop = False
    # Trial pages that cannot carry their token, held by reference: an `id()`
    # set would outlive a discarded trial and misjudge a later one that
    # reuses its address, so a retried token could cut a different page.
    unfit: list[dict[str, Any]] = []

    def stamp(
        target: dict[str, Any], at: tuple[int, int, int, int], *, by_bytes: bool = False,
    ) -> None:
        more = at[0] < len(records)
        # No `truncated` key: the server's generic offset hint would read a
        # truncated page with `offset` and `limit` as offset-pageable. The
        # token is the only continuation.
        # Delivered rows are the `results` list and a usable continuation is
        # the token: `coverage` alone qualifies them (PACT
        # operator-page-counts-3190), so no returned/complete copies here.
        fields: dict[str, Any] = {
            "continuation_token": None,
            "limit": limit,
            # Records wholly delivered before this page; a record split
            # across pages is listed again on the next one.
            "offset": first_record,
            "delivery_order": "matches_locations_then_literal_fragments",
        }
        # `response_byte_budget` reads as a byte cut wherever it appears, so
        # it is present only when this page stopped at the byte budget.
        target.pop("truncation_reason", None)
        target.pop("response_byte_budget", None)
        target.pop("delivery_last_page", None)
        if not more:
            # The producer's last-page marker (PACT operator-page-counts-3190):
            # every planned part is on this or an earlier page, so the
            # envelope proves `complete` even when the count is a lower
            # bound (an undecided record). The envelope consumes the key.
            fields["delivery_last_page"] = True
        if more and by_bytes:
            fields.update(truncation_reason="response_byte_budget", response_byte_budget=max_bytes)
        target.update(fields)
        if more:
            try:
                token = issue(
                    {"position": list(at)},
                    lambda candidate: measure({**target, "continuation_token": candidate}),
                )
            except ContinuationTooLarge:
                unfit.append(target)
                return
            if len(token.encode("ascii")) > 16384:
                raise WindowBudgetTooSmall("usable window continuation exceeds token limit")
            target["continuation_token"] = token

    def size(target: dict[str, Any]) -> int:
        return fit_bytes + 1 if any(item is target for item in unfit) else measure(target)

    def append(target: dict[str, Any], r: int, w: int, kind: str, value: Any) -> None:
        row = next((row for row in target["results"] if row["record_id"] == records[r].identity), None)
        if row is None:
            row = {**deepcopy(dict(records[r].header)), "record_id": records[r].identity, "windows": []}
            target["results"].append(row)
        if kind == "header":
            return
        window = next((item for item in row["windows"] if item["window_id"] == records[r].windows[w].identity), None)
        if window is None:
            planned = records[r].windows[w]
            window = {**deepcopy(dict(planned.header)), "window_id": planned.identity,
                      "matches": [], "locations": [], "fragments": []}
            row["windows"].append(window)
        window[kind].append(value)

    while pos[0] < len(records):
        r, w, p, char = pos
        if r not in listed and len(listed) >= limit:
            break
        part = records[r].windows[w].parts[p]
        full_end = len(part.text) if part.text is not None else 0

        def candidate(end: int) -> tuple[dict[str, Any], tuple[int, int, int, int]]:
            if part.kind == "header":
                value = None
                kind = "header"
                following = normalize((r, w, p + 1, 0))
            elif part.text is None:
                value = deepcopy(dict(part.value))
                kind = "matches" if part.kind == "match" else "locations"
                following = normalize((r, w, p + 1, 0))
            else:
                value = render_fragment(part, char, end)
                kind = "fragments"
                following = normalize((r, w, p + 1, 0)) if end == full_end else (r, w, p, end)
            trial = deepcopy(page)
            append(trial, r, w, kind, value)
            stamp(trial, following, by_bytes=True)
            return trial, following

        trial, following = candidate(full_end)
        if size(trial) > fit_bytes:
            byte_stop = True
            if part.text is not None and char < full_end:
                lo, hi = char + 1, full_end - 1
                best = None
                while lo <= hi:
                    end = (lo + hi) // 2
                    sized, next_pos = candidate(end)
                    if size(sized) <= fit_bytes:
                        best = (sized, next_pos)
                        lo = end + 1
                    else:
                        hi = end - 1
                if best is not None:
                    page, pos = best
                    listed[r] = page["results"][-1]
                    break
            if not page["results"]:
                raise WindowBudgetTooSmall(
                    "one window part and a usable continuation do not fit the byte budget",
                    data={"max_bytes": max_bytes},
                    fix_hint="increase max_bytes",
                )
            break
        page, pos = trial, following
        listed[r] = page["results"][-1]
    stamp(page, pos, by_bytes=byte_stop)
    if size(page) > fit_bytes:
        raise WindowBudgetTooSmall(
            "record-window envelope exceeds the byte budget", data={"max_bytes": max_bytes},
            fix_hint="increase max_bytes",
        )
    return page
