"""Consistency checks authored as transform programs (task.gtir.consistency-program).

The substrate's consistency checker, expressed in the graph transform IR. Each
check is a read-only program of the canonical shape

    FROM(edges_live) [JOIN(nodes_live ...)] -> WHERE(violation) -> SELECT(offending)

whose relational part lowers to SQL over the generation-pinned live views
(okg.v_edges / okg.v_nodes). The offending relation feeds an ASSERT, turning it
into an AssertionResult -- the checker's only output. It never mutates, so the
guard-dominance rule does not arise (req.gtir.consistency-first-surface).

Pure module: builds programs only. Execution (lower -> SET pin -> run -> assert)
lives at the call site, which owns the DB connection.
"""

from __future__ import annotations

from .expr import and_, col, eq, is_null, or_
from .ir import Binding, Block, Op, Terminator, TransformProgram
from .registry import Effect, OpKind, TerminatorKind


def _entry(*bindings: Binding, returns: tuple[str, ...]) -> Block:
    return Block(
        block_id="entry",
        bindings=tuple(bindings),
        terminator=Terminator(TerminatorKind.RETURN, {"values": list(returns)}),
    )


def no_dangling_edges_program() -> TransformProgram:
    """Edges whose source or destination is absent from the live nodes.

    Lowers to: SELECT e.edge_id FROM okg.v_edges e
               LEFT JOIN okg.v_nodes ns ON ns.node_id = e.src
               LEFT JOIN okg.v_nodes nd ON nd.node_id = e.dst
               WHERE ns.node_id IS NULL OR nd.node_id IS NULL
    """
    return TransformProgram(
        program_id="consistency.no_dangling_edges",
        effect=Effect.READ_ONLY,
        outputs=("offending",),
        entry_block="entry",
        attrs={"contract": "no_dangling_edges", "offending_field": "edge_id"},
        blocks=(
            _entry(
                Binding(
                    Op(OpKind.FROM, {"surface": "edges_live", "alias": "e"}), output="e"
                ),
                Binding(
                    Op(OpKind.FROM, {"surface": "nodes_live", "alias": "ns"}),
                    output="ns",
                ),
                Binding(
                    Op(
                        OpKind.JOIN,
                        {"how": "left", "on": eq(col("ns.node_id"), col("e.src"))},
                    ),
                    output="src_lj",
                    inputs=("e", "ns"),
                ),
                Binding(
                    Op(OpKind.FROM, {"surface": "nodes_live", "alias": "nd"}),
                    output="nd",
                ),
                Binding(
                    Op(
                        OpKind.JOIN,
                        {"how": "left", "on": eq(col("nd.node_id"), col("e.dst"))},
                    ),
                    output="both_lj",
                    inputs=("src_lj", "nd"),
                ),
                Binding(
                    Op(
                        OpKind.WHERE,
                        {
                            "expr": or_(
                                is_null(col("ns.node_id")),
                                is_null(col("nd.node_id")),
                            )
                        },
                    ),
                    output="dangling",
                    inputs=("both_lj",),
                ),
                Binding(
                    Op(OpKind.SELECT, {"project": {"edge_id": col("e.edge_id")}}),
                    output="offending",
                    inputs=("dangling",),
                ),
                Binding(
                    Op(OpKind.EMIT, {"target": "assertion_input"}),
                    inputs=("offending",),
                ),
                returns=("offending",),
            ),
        ),
    )


def narrowing_conformance_program(catalog_version_id: int) -> TransformProgram:
    """Edges whose (src_subtype, edge_type, dst_subtype) triple is not admitted
    by the active narrowing catalog -- an antijoin against the allowed-triple
    closure (narrowings JOIN subtype_ancestors, exposed as a registered surface).

    Lowers to a multi-join + LEFT JOIN + WHERE-null over okg.v_edges, okg.v_nodes
    (twice), and the `narrowing_allowed` surface bound to the closure subquery.
    """
    return TransformProgram(
        program_id="consistency.narrowing_conformance",
        effect=Effect.READ_ONLY,
        params=("catalog_version_id",),
        outputs=("offending",),
        entry_block="entry",
        attrs={"contract": "narrowing_conformance", "offending_field": "edge_id",
               "catalog_version_id": catalog_version_id},
        blocks=(
            _entry(
                Binding(Op(OpKind.FROM, {"surface": "edges_live", "alias": "e"}), output="e"),
                Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "ns"}), output="ns"),
                Binding(
                    Op(OpKind.JOIN, {"how": "inner", "on": eq(col("ns.node_id"), col("e.src"))}),
                    output="j1", inputs=("e", "ns"),
                ),
                Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "nd"}), output="nd"),
                Binding(
                    Op(OpKind.JOIN, {"how": "inner", "on": eq(col("nd.node_id"), col("e.dst"))}),
                    output="j2", inputs=("j1", "nd"),
                ),
                Binding(Op(OpKind.FROM, {"surface": "narrowing_allowed", "alias": "al"}), output="al"),
                Binding(
                    Op(OpKind.JOIN, {"how": "left", "on": and_(
                        eq(col("al.edge_type"), col("e.edge_type")),
                        eq(col("al.src_subtype"), col("ns.subtype")),
                        eq(col("al.dst_subtype"), col("nd.subtype")),
                    )}),
                    output="j3", inputs=("j2", "al"),
                ),
                Binding(
                    Op(OpKind.WHERE, {"expr": is_null(col("al.edge_type"))}),
                    output="violating", inputs=("j3",),
                ),
                Binding(
                    Op(OpKind.SELECT, {"project": {"edge_id": col("e.edge_id")}}),
                    output="offending", inputs=("violating",),
                ),
                Binding(Op(OpKind.EMIT, {"target": "assertion_input"}), inputs=("offending",)),
                returns=("offending",),
            ),
        ),
    )


def live_rows_witnessed_program() -> TransformProgram:
    """Commit-witness gate (PACT route-mutation-through-transform-ir): live node
    rows whose source_program_checksum does NOT resolve in the IR ledger -- an
    antijoin of nodes_live against ir_programs. The enforcement of the
    commit-witness invariant, itself a transform program.

    Lowers to: SELECT n.node_id FROM okg.v_nodes n
               LEFT JOIN okg.ir_programs p ON p.program_checksum = n.source_program_checksum
               WHERE p.program_id IS NULL
    """
    return TransformProgram(
        program_id="consistency.live_rows_witnessed",
        effect=Effect.READ_ONLY,
        outputs=("offending",),
        entry_block="entry",
        attrs={"contract": "commit_witness", "offending_field": "node_id"},
        blocks=(
            _entry(
                Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "n"}), output="n"),
                Binding(Op(OpKind.FROM, {"surface": "ir_programs", "alias": "p"}), output="p"),
                Binding(
                    Op(OpKind.JOIN, {"how": "left", "on": eq(
                        col("p.program_checksum"), col("n.source_program_checksum"))}),
                    output="lj", inputs=("n", "p"),
                ),
                Binding(
                    Op(OpKind.WHERE, {"expr": is_null(col("p.program_id"))}),
                    output="unwitnessed", inputs=("lj",),
                ),
                Binding(
                    Op(OpKind.SELECT, {"project": {"node_id": col("n.node_id")}}),
                    output="offending", inputs=("unwitnessed",),
                ),
                Binding(Op(OpKind.EMIT, {"target": "assertion_input"}), inputs=("offending",)),
                returns=("offending",),
            ),
        ),
    )


def node_subtype_conformance_program(catalog_version_id: int) -> TransformProgram:
    """Live nodes whose subtype is not a known subtype of the active catalog -- an
    antijoin of nodes_live against the catalog's subtype set (`catalog_subtypes`, a
    registered surface bound to okg.subtypes for the active catalog version).

    Catches a node carrying a retired or unknown subtype -- e.g. a fork node whose
    subtype was retired on default before merge-back. Edges are narrowing-gated but
    NODES are not, so without this a retired-subtype node could publish.

    Lowers to (over the `nodes_live` surface -- bound to okg.graph_nodes under the
    chronos gate registry the consistency gate uses, NOT the pinned okg.v_nodes view):
               SELECT n.node_id FROM <nodes_live> n
               LEFT JOIN <catalog_subtypes for cv> cs ON cs.subtype_name = n.subtype
               WHERE cs.subtype_name IS NULL
    """
    return TransformProgram(
        program_id="consistency.node_subtype_conformance",
        effect=Effect.READ_ONLY,
        params=("catalog_version_id",),
        outputs=("offending",),
        entry_block="entry",
        attrs={"contract": "node_subtype_conformance", "offending_field": "node_id",
               "catalog_version_id": catalog_version_id},
        blocks=(
            _entry(
                Binding(Op(OpKind.FROM, {"surface": "nodes_live", "alias": "n"}), output="n"),
                Binding(Op(OpKind.FROM, {"surface": "catalog_subtypes", "alias": "cs"}), output="cs"),
                Binding(
                    Op(OpKind.JOIN, {"how": "left", "on": eq(
                        col("cs.subtype_name"), col("n.subtype"))}),
                    output="lj", inputs=("n", "cs"),
                ),
                Binding(
                    Op(OpKind.WHERE, {"expr": is_null(col("cs.subtype_name"))}),
                    output="unknown", inputs=("lj",),
                ),
                Binding(
                    Op(OpKind.SELECT, {"project": {"node_id": col("n.node_id")}}),
                    output="offending", inputs=("unknown",),
                ),
                Binding(Op(OpKind.EMIT, {"target": "assertion_input"}), inputs=("offending",)),
                returns=("offending",),
            ),
        ),
    )


# The subquery bound to the `catalog_subtypes` surface: the set of subtype names in
# the active catalog version. A node whose subtype is absent here is non-conformant.
def catalog_subtypes_binding(catalog_version_id: int) -> str:
    return (
        "(SELECT name AS subtype_name FROM okg.subtypes "
        f"WHERE catalog_version_id = {int(catalog_version_id)})"
    )


# The subquery bound to the `narrowing_allowed` surface. Parameterized by
# catalog version; the deployment registers this surface against its active
# catalog. It MUST match the writer's allowed-set (catalog/narrowings.py) and the
# DB trigger (db/schema.sql) exactly: the descendant-closure JOIN arm UNION the
# raw declared narrowing rows. The raw arm admits the declared
# (parent_subtype, edge_type, child_subtype) triple even when subtype_ancestors
# lacks the reflexive self-row (e.g. incomplete closure population); without it
# the gate's allowed-set is a strict subset of the writer's and the gate would
# spuriously block a triple the writer and trigger accept.
def narrowing_allowed_binding(catalog_version_id: int) -> str:
    return (
        "(SELECT n.edge_type, sa_src.subtype_name AS src_subtype, "
        "sa_dst.subtype_name AS dst_subtype "
        "FROM okg.narrowings n "
        "JOIN okg.subtype_ancestors sa_src "
        "  ON sa_src.catalog_version_id = n.catalog_version_id "
        " AND sa_src.ancestor_name = n.parent_subtype "
        "JOIN okg.subtype_ancestors sa_dst "
        "  ON sa_dst.catalog_version_id = n.catalog_version_id "
        " AND sa_dst.ancestor_name = n.child_subtype "
        f"WHERE n.catalog_version_id = {int(catalog_version_id)} "
        "UNION "
        "SELECT n.edge_type, n.parent_subtype AS src_subtype, "
        "n.child_subtype AS dst_subtype "
        "FROM okg.narrowings n "
        f"WHERE n.catalog_version_id = {int(catalog_version_id)})"
    )


# The subquery bound to the `cardinality_constrained` surface
# (fix-projection-alias-identity #710, ruling 3): the set of (src_subtype,
# edge_type) pairs under an `at_most_one_target` narrowing, expanded through the
# subtype-refinement closure on the annotated parent_subtype. A live edge whose
# (src_subtype, edge_type) is here participates in the at-most-one-target
# invariant. Enforcement is TARGET-SUBTYPE-BLIND: the annotation lives at
# (parent_subtype, edge_type) and constrains every src in the parent's ancestor
# closure regardless of the target's subtype (the #710 twins are wrong whether
# or not both targets share a subtype). The closure arm mirrors
# `narrowing_allowed_binding`; the raw arm admits the declared parent even when
# subtype_ancestors lacks the reflexive self-row.
def cardinality_constrained_binding(catalog_version_id: int) -> str:
    return (
        "(SELECT DISTINCT sa.subtype_name AS src_subtype, n.edge_type "
        "FROM okg.narrowings n "
        "JOIN okg.subtype_ancestors sa "
        "  ON sa.catalog_version_id = n.catalog_version_id "
        " AND sa.ancestor_name = n.parent_subtype "
        f"WHERE n.catalog_version_id = {int(catalog_version_id)} "
        "  AND n.attr_rules->>'cardinality' = 'at_most_one_target' "
        "UNION "
        "SELECT n.parent_subtype AS src_subtype, n.edge_type "
        "FROM okg.narrowings n "
        f"WHERE n.catalog_version_id = {int(catalog_version_id)} "
        "  AND n.attr_rules->>'cardinality' = 'at_most_one_target')"
    )


__all__ = [
    "no_dangling_edges_program",
    "narrowing_conformance_program",
    "narrowing_allowed_binding",
    "node_subtype_conformance_program",
    "catalog_subtypes_binding",
    "cardinality_constrained_binding",
    "live_rows_witnessed_program",
]
