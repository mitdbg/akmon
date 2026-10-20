"""Merge admission partition (worktree-fork-merge-back: admission-partition-quarantine).

Split a fork's merge preview into what may land on default vs what must be set aside in
quarantine:<wt>:
  - QUARANTINED = every conflict + every non-conflicting-but-illegal row.
  - ACCEPTED    = the non-conflicting, legal rows.

"Legal" here is the node-subtype-conformance verdict (the same check the publish gate
runs): a node carrying a subtype absent from the active catalog is illegal. Edge / full
narrowing soundness is the publish gate's job over the staged candidate, not this
partition -- the partition consumes a legality verdict, it does not own gate soundness.

The partition GUARANTEES no silent loss: ACCEPTED and QUARANTINED are disjoint and
together cover every preview row (the explicit fix for chronos's `conflict_choices`
"skip", which silently drops a source fact rather than routing it to a real branch).

Pure policy: operates on the in-memory preview (objects with `.changes`/`.conflicts`,
each a RowDiff-shaped object); no DB, no chronos. The orchestrator applies ACCEPTED onto
the staging candidate and writes QUARANTINED onto quarantine:<wt> in one transaction (the
atomic write rides the blocked stage->gate->fast-forward orchestrator).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from okg.substrate.merge_guard import MergeRefused

# The chronos node table; a row on this table is subtype-gated. Anything else (edges)
# is out of scope for the node-subtype legality verdict and treated as legal here.
NODE_TABLE = "okg.graph_nodes"

LegalityVerdict = Callable[[Any], bool]


@dataclass(frozen=True)
class MergePartition:
    """A merge preview split into rows that may land vs rows set aside in quarantine."""

    accepted: tuple[Any, ...]
    quarantined: tuple[Any, ...]


def subtype_legality_verdict(legal_subtypes: frozenset[str]) -> LegalityVerdict:
    """Build a node-subtype legality verdict over the catalog's legal subtype set. A node
    add/modify is legal iff its subtype is in the set; a deletion (no subtype) and any
    non-node row are legal; an add/modify node with no subtype is unclassifiable and
    refused (deny-by-default -- never silently accepted)."""

    def is_legal(diff: Any) -> bool:
        if getattr(diff, "table", None) != NODE_TABLE:
            return True  # edges: soundness rides the publish gate, out of scope here
        if getattr(diff, "change", None) == "deleted":
            return True  # no subtype to check on a deletion
        after = getattr(diff, "after", None)
        if not isinstance(after, dict) or "subtype" not in after:
            raise MergeRefused(
                "cannot classify a node merge row with no subtype; refusing",
                data={
                    "reason": "unclassifiable_merge_row",
                    "key": getattr(diff, "key", None),
                    "table": getattr(diff, "table", None),
                },
            )
        return after["subtype"] in legal_subtypes

    return is_legal


def partition_merge(preview: Any, *, is_legal: LegalityVerdict) -> MergePartition:
    """Partition a merge preview into accepted vs quarantined (conflicts + illegal rows).

    Guarantees: accepted and quarantined are disjoint and together cover every row in
    `preview.changes ∪ preview.conflicts` (no silent loss)."""
    changes = list(getattr(preview, "changes", None) or [])
    conflicts = list(getattr(preview, "conflicts", None) or [])

    quarantined: list[Any] = list(conflicts)  # every conflict is quarantined
    accepted: list[Any] = []
    for diff in changes:
        (accepted if is_legal(diff) else quarantined).append(diff)

    # No-silent-loss invariant: total coverage of changes ∪ conflicts.
    if len(accepted) + len(quarantined) != len(changes) + len(conflicts):
        raise MergeRefused(
            "merge partition lost rows (accepted+quarantined != changes+conflicts)",
            data={"reason": "partition_coverage_violation"},
        )
    return MergePartition(accepted=tuple(accepted), quarantined=tuple(quarantined))


__all__ = [
    "NODE_TABLE",
    "LegalityVerdict",
    "MergePartition",
    "partition_merge",
    "subtype_legality_verdict",
]
