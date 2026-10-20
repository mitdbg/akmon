"""Concept-scope CTE injection for the invariants runner.

Per ``add-concept-scoped-invariants`` (capability E of the af_pubs
supervisor demo). When an invariant declares ``concept_scope:``, the
runner injects a ``scope`` CTE before the user's row-source SQL that
filters nodes to the named Concept's neighborhood:

  - Hop 1 (default): nodes that have a ``mentions`` edge to the
    Concept whose attached similarity ≥ ``min_confidence``. The
    SemanticConceptLinker stamps similarity into
    ``graph_edges.attrs->'evidence'->>'similarity'`` (per
    ``add-substrate-semantic-concept-linking``); we read it back from
    there.
  - Hops > 1: extend the neighborhood by walking edges of the
    declared types. Reserved for future deployments; the af_pubs
    demo only uses hops=1.

The user's ``candidates_sql`` (for kind='llm_judge') or ``sql_body``
(for kind='sql') typically references the injected CTE explicitly,
e.g.:

    JOIN scope sa ON sa.node_id = a.node_id
    JOIN scope sb ON sb.node_id = b.node_id

It is the YAML author's job to write a SQL that consumes ``scope``;
the runner does NOT auto-filter the user's row source. The injected
CTE is just available in the resulting query namespace.

Loud failure modes (project P0 rule):

  - ``concept_name`` does not resolve to any Concept node at the
    given generation → returns ``ScopeUnresolved``; the caller
    records a ``scope_unresolved`` warn and skips the invariant.
    The runner does NOT silently treat the empty scope as
    "all rows".
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import psycopg


__all__ = [
    "ScopeInjection",
    "ScopeUnresolved",
    "concept_name_to_node_id",
    "compute_scope_injection",
]


def concept_name_to_node_id(concept_name: str) -> str:
    """Mirror of ``okg.substrate.catalog.concept_seed.parse_seed`` /
    ``deployments/af_pubs/views/concept_corpus._concept_node_id``.

    The slug is a deterministic function of the operator-authored
    concept name, so the runner can resolve it without reading the
    Concept node first.
    """
    slug = concept_name.strip().lower()
    out: list[str] = []
    for ch in slug:
        # ASCII-alphanumeric only: `'¹'.isalnum()` is True in
        # Python (Unicode superscript), but the node_id format is
        # documented as `concept:[a-z0-9_]*` — the substrate's
        # downstream identifier-pattern matchers and YAML config
        # don't tolerate non-ASCII. Per the property test in
        # `tests/substrate/property/test_concept_name_slug_property.py`.
        if ch.isascii() and ch.isalnum():
            out.append(ch)
        elif ch in (" ", "-", "_"):
            out.append("_")
        # other punctuation / non-ASCII alnum drops
    cleaned = "".join(out).strip("_")
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return f"concept:{cleaned}"


class ScopeUnresolved(RuntimeError):
    """Raised when ``concept_name`` does not resolve to any
    Concept node at the published generation.

    The runner catches this to record a ``scope_unresolved`` warn
    on the invariant rather than silently treating the empty
    neighborhood as "all rows".
    """


@dataclass(frozen=True)
class ScopeInjection:
    """One concept-scope CTE prepended to the user's row source.

    Attributes
    ----------
    cte_sql :
        Postgres CTE definition of shape
        ``"scope AS (SELECT node_id FROM ...)"`` — without the
        leading ``WITH`` keyword. Caller composes it into a
        ``WITH scope AS (...), <user_ctes...> SELECT ...`` shape.
    params :
        Named parameters that ``cte_sql`` references (currently
        ``concept_id`` and ``min_confidence``). Caller passes
        these to ``cur.execute(...)`` alongside its own params.
    concept_id :
        The resolved Concept node id; carried out so the caller
        can stamp it on fired warns for downstream grouping.
    """

    cte_sql: str
    params: dict[str, Any]
    concept_id: str


def _resolve_concept_id(
    conn: psycopg.Connection,
    concept_id: str,
    *,
    generation_id: int,
) -> bool:
    """Return True iff a Concept node with the given id is live at
    the given generation.

    The Concept subtype lives in the ``concepts`` substrate module;
    visibility at the given generation is injected by the Chronos
    checkout session.
    """
    from okg.substrate.graph_store import ChronosGraphStore

    graph = ChronosGraphStore.from_connection(
        conn, ensure_schema=False
    ).checkout_generation(generation_id)
    rows = graph.query(
        """
        SELECT 1 AS present
          FROM okg.graph_nodes
         WHERE node_id = :node_id
           AND subtype = 'concept'
         LIMIT 1
        """,
        {"node_id": concept_id},
    )
    return bool(rows)


def compute_scope_injection(
    conn: psycopg.Connection,
    concept_scope: dict[str, Any],
    *,
    generation_id: int,
) -> ScopeInjection:
    """Build the ``scope`` CTE definition for a concept_scope spec.

    Parameters
    ----------
    conn :
        Open ``psycopg.Connection`` against the deployment DB.
    concept_scope :
        The spec dict carried on ``InvariantSpec.concept_scope``;
        either the validated YAML form (with tuple ``hop_edges``)
        or the JSONB-deserialized form (with list ``hop_edges``).
        Both are accepted.
    generation_id :
        The published generation to resolve against.

    Raises ``ScopeUnresolved`` when the concept name doesn't
    resolve to a Concept node at the published generation.
    """
    concept_name = concept_scope["concept_name"]
    min_confidence = float(concept_scope["min_confidence"])
    hops = int(concept_scope.get("hops", 1))
    hop_edges = list(concept_scope.get("hop_edges", ("mentions",)))

    concept_id = concept_name_to_node_id(concept_name)
    if not _resolve_concept_id(
        conn, concept_id, generation_id=generation_id,
    ):
        raise ScopeUnresolved(
            f"concept_name {concept_name!r} resolves to "
            f"{concept_id!r}, but no live Concept node with that "
            f"id exists at generation {generation_id}; the runner "
            f"refuses to silently treat this as 'all rows' "
            f"(project P0 rule against silent fallbacks)"
        )

    if hops != 1 or hop_edges != ["mentions"]:
        # Future-work guard: the af_pubs demo only exercises
        # hops=1 with the shipped 'mentions' edge. Reject
        # anything else loudly so the YAML is honest about
        # what's actually supported.
        raise NotImplementedError(
            f"concept_scope: hops={hops}, hop_edges={hop_edges} "
            f"is not yet supported. Only hops=1 with "
            f"hop_edges=['mentions'] is wired today; the multi-hop "
            f"path is reserved for a follow-up change. Either "
            f"drop those keys (defaults are fine) or implement "
            f"the multi-hop traversal here before scoping the "
            f"invariant."
        )

    # Hop-1 scope: nodes that mention the concept above threshold.
    # Read similarity back from the SemanticConceptLinker's
    # `attrs->'evidence'->>'similarity'`. Reads the Chronos graph
    # (`okg.graph_edges`); the runner injects generation visibility.
    cte_sql = """
        scope AS (
          SELECT e.src AS node_id,
                 (e.attrs->'evidence'->>'similarity')::float
                     AS similarity
            FROM okg.graph_edges e
           WHERE e.edge_type = 'mentions'
             AND e.dst = %(scope_concept_id)s
             AND (e.attrs->'evidence'->>'similarity')::float
                     >= %(scope_min_confidence)s
        )
    """.strip()
    return ScopeInjection(
        cte_sql=cte_sql,
        params={
            "scope_concept_id": concept_id,
            "scope_min_confidence": min_confidence,
        },
        concept_id=concept_id,
    )
