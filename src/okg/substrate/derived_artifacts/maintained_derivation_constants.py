"""Single source of the maintained-derivation sentinel + the ontology-rollup producer
constants.

This module is deliberately **import-light at module level** -- it pulls nothing from
``okg.substrate.graph_store`` or any heavy substrate module -- so both the write path
(``publisher/ontology_rollup.py``) and the read path
(``console_views/ontology_rollup_read.py``) can import these tokens from ONE place without
dragging ``graph_store`` into a read-side module-level import.

It is the de-dup target for the maintained-derivation primitive
(``extract-maintained-derivation-primitive`` / GitHub #444): before this module, the read
side hand-duplicated the sentinel + producer constants and a test asserted the two copies
stayed in sync. Now there is exactly one definition, and read/write agree by construction.

The string values are byte-identical to the prior definitions in
``publisher/ontology_rollup.py`` and ``console_views/ontology_rollup_read.py``, so the
extraction is behavior-preserving.
"""
from __future__ import annotations

from collections.abc import Sequence

# A reserved token. A row whose every primary-key column equals this token is the
# maintained-derivation *sentinel* -- written at every maintained generation, proving the
# maintain hook ran, so a read can fail loud when it is absent rather than silently fall
# back to an O(graph) scan. Schema-agnostic: callers supply their own PK columns and this
# module never assumes a fixed PK arity.
SENTINEL_TOKEN: str = "__okg_ontology_rollup_sentinel__"


def sentinel_pk_values(pk_columns: Sequence[str]) -> dict[str, str]:
    """Map every primary-key column to ``SENTINEL_TOKEN``.

    Works for any derivation's PK shape -- the ontology rollup's 4-column PK, the console
    count rollup's 2-column PK, etc. -- by iterating the columns rather than indexing a
    fixed arity. Used by both the sentinel writer and the sentinel probe so the two agree
    on the sentinel's identity by construction.
    """
    return {column: SENTINEL_TOKEN for column in pk_columns}


# --- ontology-rollup CONSUMER constants ------------------------------------------------
# Co-located here (rather than in the generic kit) ONLY to dedup the read/write copy: they
# are ontology-rollup-specific producer data, kept in the import-light module so the read
# path can share them without a module-level ``graph_store`` import. Values are
# byte-identical to the originals.

#: Producer bucket for a publisher-derived edge whose linker could not be recovered. SHOWN,
#: never silently dropped or defaulted (it would be a P0 if folded into a fake enricher).
UNATTRIBUTED_DERIVED_PRODUCER: str = "_publisher_derived:unattributed"

#: ``graph_edges.source`` value stamped on every publisher-derived (enricher) edge.
PUBLISHER_DERIVED_SOURCE: str = "_publisher_derived"

#: Prefix marking an enricher-attributed producer (``_enricher:<linker>``).
ENRICHER_PREFIX: str = "_enricher:"


# --- console-count-rollup CONSUMER constants -------------------------------------------
# The dimension-kind discriminators stamped into ``okg.console_count_rollup``'s
# ``dimension_kind`` column (a closed set). Co-located here so the publish-time maintainer
# (``publisher/console_count_rollup.py``) and the read path
# (``console_views/console_count_rollup_read.py``) share ONE definition -- the read's
# per-kind filter matches the write's stamping by construction, and the read stays
# import-light (no module-level ``graph_store``).

#: nodes grouped by ``graph_nodes.subtype``.
COUNT_KIND_SUBTYPE: str = "subtype"

#: edges grouped by ``graph_edges.edge_type``.
COUNT_KIND_EDGE_TYPE: str = "edge_type"

#: nodes grouped by ``graph_nodes.source`` (``source IS NOT NULL``).
COUNT_KIND_SOURCE: str = "source"

#: edges grouped by ``graph_edges.source`` (``source IS NOT NULL``). Lets the
#: per-source console (sources.py) read live EDGES-per-source O(1) instead of the
#: unconditional full-graph GROUP BY it kept after slice 2.
COUNT_KIND_EDGE_SOURCE: str = "edge_source"

#: NODES grouped by their ENRICHER producer -- the source when it is ``_enricher:%``,
#: else the linker recovered from ``source_record_id`` for ``_publisher_derived`` rows
#: (see ``ENRICHER_PRODUCER_SQL_EXPR``). Lets enricher-diagnostics read live
#: enricher node contribution O(1) instead of a full-graph CASE GROUP BY.
COUNT_KIND_ENRICHER_NODE: str = "enricher_node"

#: EDGES grouped by their ENRICHER producer (same attribution as ``enricher_node``).
COUNT_KIND_ENRICHER_EDGE: str = "enricher_edge"

#: NODES grouped by ``(producer, subtype)`` — the per-node-type emission detail the
#: operator Graph view's producer layer draws as ``emits`` edges (issue #446). The
#: ``dimension_value`` encodes both coordinates as ``<producer>\x1f<subtype>``
#: (:data:`SOURCE_SUBTYPE_SEP`, subtype LAST so a ``rsplit`` parse is injective for
#: an arbitrary producer string). Domain: every node with ``source IS NOT NULL`` —
#: exactly the ``source`` dimension's domain, unattributed bucket included, so
#: "``source`` rows present iff ``source_subtype`` rows present" holds by
#: construction (the shape-upgrade backfill predicate relies on this equivalence).
COUNT_KIND_SOURCE_SUBTYPE: str = "source_subtype"

#: Separator between producer and subtype inside a ``source_subtype``
#: ``dimension_value``. The 0x1f unit separator sits outside any plausible subtype
#: charset; the maintain path still guards LOUDLY against a subtype containing it
#: (a mis-parse rendered as truth would be a silent-fallback P0).
SOURCE_SUBTYPE_SEP: str = "\x1f"

#: The NODE-producer SQL expression for this dimension
#: (``NODE_PRODUCER_SQL_EXPR``) lives with its sole consumer,
#: ``publisher/console_count_rollup.py`` — it is interpolated into the
#: recompute-oracle and node-dims SQL there, and the cross-artifact key parity
#: with ``publisher/ontology_rollup._PRODUCER_EXPR`` is enforced by test, not
#: by co-location.


def encode_source_subtype(producer: str, subtype: str) -> str:
    """Encode a ``source_subtype`` ``dimension_value``. Raises loudly on a subtype
    containing the separator — never a silent mis-parse rendered as truth."""
    if SOURCE_SUBTYPE_SEP in subtype:
        raise ValueError(
            f"subtype {subtype!r} contains the source_subtype separator (0x1f); "
            "refusing to encode an ambiguous dimension_value"
        )
    return f"{producer}{SOURCE_SUBTYPE_SEP}{subtype}"


def parse_source_subtype(dimension_value: str) -> tuple[str, str]:
    """Parse a ``source_subtype`` ``dimension_value`` into ``(producer, subtype)``.

    Requires EXACTLY one separator: no legitimate producer key or subtype
    contains 0x1f (both are code/catalog-defined identifiers; the write legs
    guard the subtype and ``NODE_PRODUCER_SQL_EXPR`` shapes the producer), so
    any other count is a malformed or ambiguous row and raises loudly — never
    a silent skip or a mis-bucketing rendered as truth.
    """
    if dimension_value.count(SOURCE_SUBTYPE_SEP) != 1:
        # Zero separators = malformed; more than one = an ambiguous encoding
        # (no legitimate producer key contains 0x1f — see
        # NODE_PRODUCER_SQL_EXPR's output shapes). Either way: raise, never a
        # silent skip or mis-bucketing rendered as truth.
        raise ValueError(
            f"malformed source_subtype dimension_value {dimension_value!r}: "
            f"expected exactly one 0x1f separator"
        )
    producer, _sep, subtype = dimension_value.rpartition(SOURCE_SUBTYPE_SEP)
    return producer, subtype

#: SQL expression computing a row's ENRICHER producer key from ``(source,
#: source_record_id)``, or NULL when the row is not enricher-attributed. ``_enricher:%``
#: sources are their own key; ``_publisher_derived`` rows recover the enricher from
#: ``source_record_id->>'linker'`` (+ optional ``rule_id``). Shared by the rollup
#: maintainer (publisher/console_count_rollup.py) AND the read (console_views/
#: enricher_diagnostics.py) so the dimension and the flag-off scan agree by construction.
#: Valid against okg.graph_nodes and okg.graph_edges (both carry source + source_record_id).
ENRICHER_PRODUCER_SQL_EXPR: str = (
    "CASE "
    "WHEN source LIKE '_enricher:%' THEN source "
    "WHEN source = '_publisher_derived' AND source_record_id->>'linker' IS NOT NULL "
    "THEN '_enricher:' || (source_record_id->>'linker') || "
    "CASE WHEN source_record_id->>'rule_id' IS NULL THEN '' "
    "ELSE ':' || (source_record_id->>'rule_id') END "
    "END"
)

#: The matching WHERE filter (only enricher-attributed rows).
ENRICHER_PRODUCER_FILTER_SQL: str = (
    "source LIKE '_enricher:%' "
    "OR (source = '_publisher_derived' AND source_record_id->>'linker' IS NOT NULL)"
)
