"""The console's "what is in this graph" read: record and relationship types.

Home's contents panel names the record types and relationship types the pinned
generation holds, with a count each. The authority is the maintained console
count rollup (``okg.console_count_rollup``), read as of the pinned generation:
its ``subtype`` dimension is the record types and its ``edge_type`` dimension
the relationship types.

Not to be confused with ``console_views/ontology_rollup_read.py``. That module
reads ``okg.ontology_rollup``, whose rows are ``(src_subtype, edge_type,
dst_subtype, producer)`` edge buckets. It carries NO node counts, so it cannot
answer "which record types exist and how many of each" at all; the count rollup
is the only maintained source that can, and using it keeps this read O(1) in
the size of the graph.

The route is named ``console/ontology-rollup`` because that is the route the
console already calls. The name predates the split between the two rollups.

Fail loud: when the pinned generation has no maintained rollup the reader
raises ``ConsoleCountRollupUnavailable`` and this composer lets it out. A
generation that IS maintained and holds nothing returns empty lists -- a real
empty graph, not a missing read.
"""

from __future__ import annotations

from typing import Any

from okg.substrate.console_views.console_count_rollup_read import (
    read_console_count_rollup,
)
from okg.substrate.derived_artifacts.maintained_derivation_constants import (
    COUNT_KIND_EDGE_TYPE,
    COUNT_KIND_SUBTYPE,
)

#: What the counts mean, as the ENUM every console read uses for this field
#: (console_views/records.py emits the same one). A value, not a sentence: the
#: console maps the known values to words and refuses an unknown one by name,
#: which a free-text basis would make impossible. It means the rollup was
#: summed at publish time over everything the generation published, before any
#: read-time policy filtering -- so a reader whose policy hides rows can see a
#: total larger than what they can open, and the page has to say so.
COUNT_BASIS = "published_total_before_access_rules"


def _rows(counts: dict[tuple[str, str], int], kind: str) -> list[dict[str, Any]]:
    """The rollup's rows for one dimension, largest first then by name."""
    selected = [
        {"type": value, "count": int(count)}
        for (row_kind, value), count in counts.items()
        if row_kind == kind
    ]
    selected.sort(key=lambda row: (-row["count"], row["type"]))
    return selected


def compose_ontology_rollup(
    conn: Any,
    *,
    deployment_name: str,
    generation_id: int | str,
) -> dict[str, Any]:
    """Record types and relationship types at the pinned generation."""
    counts = read_console_count_rollup(
        conn,
        generation_id=generation_id,
        kinds=(COUNT_KIND_SUBTYPE, COUNT_KIND_EDGE_TYPE),
    )
    node_types = _rows(counts, COUNT_KIND_SUBTYPE)
    relationship_types = _rows(counts, COUNT_KIND_EDGE_TYPE)
    return {
        "deployment": deployment_name,
        "generation_id": generation_id,
        "node_types": node_types,
        "relationship_types": relationship_types,
        # How many distinct TYPES there are, which is what Home's count strip
        # shows; the per-type counts above carry the totals.
        "node_type_total": len(node_types),
        "relationship_type_total": len(relationship_types),
        "count_basis": COUNT_BASIS,
    }
