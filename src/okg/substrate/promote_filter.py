"""Promote-scoping filter (worktree-fork-merge-back: promote-scoping-and-envelope).

Deny-by-default: of a fork's merge diff, ONLY rows the fork explicitly marked durable
(`attrs.promote_intent == "durable"`, server-stamped in P2 -- see admission_workflows
`_emit_review_payload_facts` / server `_normalize_promote_intent`) are eligible to land
on default. An unmarked (scratch) row never reaches default. The orchestrator stages
`PromoteScope.promote` onto the staging candidate and routes `PromoteScope.excluded` to
the envelope/quarantine side.

Pure policy: operates on the in-memory merge diff (a list of RowDiff-shaped objects with
`.change`/`.after`/`.key`/`.table`); no DB, no chronos. The marker is read from the
post-merge state (`row.after["attrs"]["promote_intent"]`); a row whose after-state lacks
the marker -- including every deletion (after is None) -- is excluded by construction.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Sequence

from okg.substrate.merge_guard import MergeRefused

# The server-stamped durable marker (P2). Caller attrs cannot set it -- it is injected
# server-side only for an explicit durable fork write -- so its presence on a row's
# after-state is authoritative.
PROMOTE_MARKER_KEY = "promote_intent"
PROMOTE_MARKER_VALUE = "durable"


def is_promote_marked(diff: Any) -> bool:
    """True iff the row's post-merge state carries the durable promote marker.

    A deletion (after is None), a row with no attrs, or attrs lacking/!= the marker are
    all NOT promote-marked. Case/whitespace tolerant on the marker value; never raises."""
    after = getattr(diff, "after", None)
    if not isinstance(after, dict):
        return False
    attrs = after.get("attrs")
    if not isinstance(attrs, dict):
        return False
    return str(attrs.get(PROMOTE_MARKER_KEY) or "").strip().lower() == PROMOTE_MARKER_VALUE


@dataclass(frozen=True)
class PromoteScope:
    """A merge diff partitioned by the durable promote marker. `promote` is what may be
    staged onto default; `excluded` is everything else (scratch rows + deletions)."""

    promote: tuple[Any, ...]
    excluded: tuple[Any, ...]


def scope_to_promote_marked(changes: Iterable[Any]) -> PromoteScope:
    """Partition a merge diff into promote-marked vs excluded (deny-by-default)."""
    promote: list[Any] = []
    excluded: list[Any] = []
    for diff in changes:
        (promote if is_promote_marked(diff) else excluded).append(diff)
    return PromoteScope(promote=tuple(promote), excluded=tuple(excluded))


def _diff_key(diff: Any) -> dict[str, Any]:
    key = getattr(diff, "key", None)
    return key if isinstance(key, dict) else {}


def assert_only_promote_marked(
    changes: Sequence[Any], *, source: str, target: str
) -> tuple[Any, ...]:
    """Strict variant: refuse the whole merge if ANY row lacks the durable marker.

    Returns the (all promote-marked) rows on success. Use where the caller requires the
    diff to be entirely promote-scoped already, rather than silently dropping rows."""
    scope = scope_to_promote_marked(changes)
    if scope.excluded:
        raise MergeRefused(
            f"merge {source!r} -> {target!r} carries "
            f"{len(scope.excluded)} row(s) not marked for promotion",
            fix_hint=(
                "only durable (promote-marked) fork rows may merge; mark them at write "
                "time, or stage only the promote-scoped subset"
            ),
            data={
                "source": source,
                "target": target,
                "reason": "not_promote_marked",
                "excluded_keys": [_diff_key(d) for d in scope.excluded],
            },
        )
    return scope.promote


__all__ = [
    "PROMOTE_MARKER_KEY",
    "PROMOTE_MARKER_VALUE",
    "PromoteScope",
    "assert_only_promote_marked",
    "is_promote_marked",
    "scope_to_promote_marked",
]
