"""Merge provenance re-stamp (worktree-fork-merge-back: trust-restamp).

When a fork's rows land on default, they must NOT keep the fork's original source --
read-side trust is derived purely from the source string (mcp/provenance._classify_one),
so a merged row inherits whatever trust its fork source implied. Two rules:

  1. Refuse-unmergeable-source: a fork row whose source is null/empty, or carries a
     reserved system/derived prefix, is REFUSED. The null source and the SYSTEM prefixes
     (provenance._SYSTEM_SOURCE_PREFIXES) classify TRUSTED read-side, so allowing them
     would let a fork LAUNDER a trusted label onto default; the additional
     derived_/extracted_ producer prefixes read UNtrusted but are substrate-derived and
     must be re-derived on default, not carried over.

  2. Re-stamp: EVERY provenance-bearing field of a merged row is normalized to the merge
     producer, not only the top-level `source`. `source` is rewritten to a synthetic merge
     producer `merge:<worktree>@<fork_tip_gen>` (NOT a system prefix, no producer profile,
     so it reads back UNTRUSTED by default via the no-profile fail-safe -- merged facts are
     quoted/fenced, never trusted-and-unfenced, with no trust column written). The
     `source_record_id` and `source_revision` provenance blobs (real columns the read-side
     dedup indexes key on, alongside `source`) are OVERWRITTEN with a merge-scoped descriptor
     so no fork-controlled identity survives onto default (a carried-over fork
     source_record_id would launder the fork's identity / collide under the merge source).
     The reserved server-only `promote_intent` attr (the P2 durable marker) is STRIPPED from
     the landed row -- it is meaningless on default and must not leak.

Pure policy: transforms in-memory merge-diff rows (RowDiff-shaped objects); no DB, no
chronos. The orchestrator applies the re-stamped rows when it builds the staged candidate
(re-stamp cannot be injected into the raw merge_apply, which re-applies source rows
wholesale -- it runs in the staging step).
"""
from __future__ import annotations

import dataclasses
from typing import Any, Iterable

from okg.substrate.mcp.provenance import _SYSTEM_SOURCE_PREFIXES
from okg.substrate.merge_guard import MergeRefused
from okg.substrate.promote_filter import PROMOTE_MARKER_KEY

# Source prefixes a fork must not merge. The SYSTEM prefixes are DERIVED from the read-side
# trust authority (_SYSTEM_SOURCE_PREFIXES) by construction so the two cannot drift -- every
# source the classifier trusts-by-prefix is refused here, else a fork could launder a
# trusted label onto default. The extra derived_/extracted_ producer prefixes read
# untrusted but are substrate-derived and must be re-derived on default, not carried.
RESERVED_SOURCE_PREFIXES = tuple(_SYSTEM_SOURCE_PREFIXES) + ("derived_", "extracted_")


def assert_source_mergeable(source: str | None) -> None:
    """Raise MergeRefused unless `source` is a mergeable (non-laundering) fork source.

    Null/empty -> refused (reads back trusted). A reserved system/derived prefix ->
    refused (reads back trusted, or is substrate-derived and must be re-derived on
    default, not carried)."""
    if source is None or not str(source).strip():
        raise MergeRefused(
            "refusing to merge a row with a null/empty source (would read back trusted)",
            data={"reason": "null_source", "source": source},
            fix_hint="merged rows must carry a concrete fork source; null sources are refused",
        )
    normalized = str(source).strip().lower()
    if normalized.startswith(RESERVED_SOURCE_PREFIXES):
        raise MergeRefused(
            f"refusing to merge a row whose source {source!r} carries a reserved "
            "system/derived prefix (would launder a trusted/derived label onto default)",
            data={"reason": "reserved_system_source", "source": source},
            fix_hint=(
                "system/enricher/lens/derived rows are not merged; default re-derives "
                "them after the merge"
            ),
        )


def merge_producer_id(worktree: str, fork_tip_gen: str) -> str:
    """The synthetic merge producer id stamped onto every merged row. Reads back untrusted
    (not a system prefix, no producer profile)."""
    return f"merge:{worktree}@{fork_tip_gen}"


def restamp_row(after: dict[str, Any], *, worktree: str, fork_tip_gen: str) -> dict[str, Any]:
    """Refuse an unmergeable source, then return a NEW row dict (input not mutated) with ALL
    provenance-bearing fields normalized to the merge producer and the reserved promote
    marker stripped: `source` -> merge id; `source_record_id`/`source_revision` -> a
    merge-scoped descriptor (so no fork-controlled identity survives); `attrs.promote_intent`
    removed. Idempotent: re-stamping an already `merge:`-stamped row just updates the
    generation and re-applies the same normalization."""
    assert_source_mergeable(after.get("source"))
    restamped = dict(after)
    restamped["source"] = merge_producer_id(worktree, fork_tip_gen)
    # Overwrite the fork's provenance identity unconditionally -- the read-side dedup indexes
    # key on (source, source_record_id, ...), so a carried-over fork value would launder the
    # fork's identity or collide under the new merge source.
    restamped["source_record_id"] = {"merged_from": worktree, "fork_tip_gen": fork_tip_gen}
    restamped["source_revision"] = {}
    # Strip the server-only durable marker (P2) so it does not leak onto the landed row.
    attrs = restamped.get("attrs")
    if isinstance(attrs, dict) and PROMOTE_MARKER_KEY in attrs:
        restamped["attrs"] = {k: v for k, v in attrs.items() if k != PROMOTE_MARKER_KEY}
    return restamped


def restamp_diff(changes: Iterable[Any], *, worktree: str, fork_tip_gen: str) -> list[Any]:
    """Re-stamp the `after`-state provenance (source + source_record_id + source_revision)
    and strip the promote marker of every add/modify row in a merge diff (deletes carry no
    after-state and are left for the deletion-authority guard). Refuses the whole batch if
    any row carries an unmergeable source (deny-by-default, before any rewrite)."""
    out: list[Any] = []
    for diff in changes:
        after = getattr(diff, "after", None)
        if not isinstance(after, dict):
            out.append(diff)
            continue
        new_after = restamp_row(after, worktree=worktree, fork_tip_gen=fork_tip_gen)
        out.append(dataclasses.replace(diff, after=new_after))
    return out


__all__ = [
    "RESERVED_SOURCE_PREFIXES",
    "assert_source_mergeable",
    "merge_producer_id",
    "restamp_diff",
    "restamp_row",
]
