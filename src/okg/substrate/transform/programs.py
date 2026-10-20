"""Canonical substrate transform programs -- the IR artifacts that production
write paths are witnessed by (PACT route-all-writers-through-transform-ir).

These are content-addressed: a single shared program per write role, so every row
a routed writer stamps resolves to one ledgered program (the commit-witness
invariant). Concrete bitemporal application stays in the writer; the program is the
logical witness, persisted once via persist_transform_program.
"""

from __future__ import annotations

from .ir import Binding, Block, Op, Terminator, TransformProgram
from .registry import Effect, OpKind, TerminatorKind


def _entry(*bindings: Binding, returns: tuple[str, ...]) -> Block:
    return Block(
        block_id="entry",
        bindings=tuple(bindings),
        terminator=Terminator(TerminatorKind.RETURN, {"values": list(returns)}),
    )


def projection_program() -> TransformProgram:
    """The substrate projection: facts -> DIFF -> live DeltaSet, guarded by a
    no-dangling ASSERT. The logical witness for projector-written live rows."""
    return TransformProgram(
        program_id="substrate.projection",
        effect=Effect.DELTA,
        outputs=("live_delta",),
        entry_block="entry",
        attrs={"role": "projection"},
        blocks=(
            _entry(
                Binding(Op(OpKind.FROM, {"surface": "node_facts"}), output="prev"),
                Binding(Op(OpKind.FROM, {"surface": "node_facts"}), output="cur"),
                Binding(Op(OpKind.DIFF, {"key": "graph_key"}), output="changes", inputs=("prev", "cur")),
                Binding(
                    Op(OpKind.ASSERT, {"predicate": "no_dangling", "contract": "no_dangling_edges"}),
                    output="g", inputs=("changes",),
                ),
                Binding(
                    Op(OpKind.DELTA, {"op": "upsert", "target_surface": "nodes_live"}),
                    output="live_delta", inputs=("changes", "g"),
                ),
                Binding(Op(OpKind.EMIT, {"target": "live_delta"}), inputs=("live_delta",)),
                returns=("live_delta",),
            ),
        ),
    )


def ingestion_program() -> TransformProgram:
    """Source ingestion: parse source records into a fact DeltaSet, guarded by a
    narrowing ASSERT. The logical witness for source-runner-written facts."""
    return TransformProgram(
        program_id="substrate.ingestion",
        effect=Effect.DELTA,
        outputs=("fact_delta",),
        entry_block="entry",
        attrs={"role": "ingestion"},
        blocks=(
            _entry(
                Binding(Op(OpKind.FROM, {"surface": "node_facts"}), output="src"),
                Binding(Op(OpKind.CALL, {"function_id": "parse_records"}), output="parsed", inputs=("src",)),
                Binding(Op(OpKind.DIFF, {"key": "source_key"}), output="changes", inputs=("src", "parsed")),
                Binding(
                    Op(OpKind.ASSERT, {"predicate": "narrowings", "contract": "narrowings_hold"}),
                    output="g", inputs=("changes",),
                ),
                Binding(
                    Op(OpKind.DELTA, {"op": "upsert", "target_surface": "node_facts"}),
                    output="fact_delta", inputs=("changes", "g"),
                ),
                Binding(Op(OpKind.EMIT, {"target": "fact_delta"}), inputs=("fact_delta",)),
                returns=("fact_delta",),
            ),
        ),
    )


def enrichment_program() -> TransformProgram:
    """Enrichment: read live graph, derive new edges, guarded by a narrowing
    ASSERT. The logical witness for enricher-/linker-written derived facts."""
    return TransformProgram(
        program_id="substrate.enrichment",
        effect=Effect.DELTA,
        outputs=("derived_delta",),
        entry_block="entry",
        attrs={"role": "enrichment"},
        blocks=(
            _entry(
                Binding(Op(OpKind.FROM, {"surface": "edges_live"}), output="live"),
                Binding(Op(OpKind.CALL, {"function_id": "derive_edges"}), output="derived", inputs=("live",)),
                Binding(Op(OpKind.DIFF, {"key": "graph_key"}), output="changes", inputs=("live", "derived")),
                Binding(
                    Op(OpKind.ASSERT, {"predicate": "narrowings", "contract": "narrowings_hold"}),
                    output="g", inputs=("changes",),
                ),
                Binding(
                    Op(OpKind.DELTA, {"op": "upsert", "target_surface": "edge_facts"}),
                    output="derived_delta", inputs=("changes", "g"),
                ),
                Binding(Op(OpKind.EMIT, {"target": "derived_delta"}), inputs=("derived_delta",)),
                returns=("derived_delta",),
            ),
        ),
    )


def admission_program() -> TransformProgram:
    """Admission: agent-proposed payload, guarded by a producer-policy + narrowing
    ASSERT. The logical witness for admission-path-written facts."""
    return TransformProgram(
        program_id="substrate.admission",
        effect=Effect.DELTA,
        outputs=("admit_delta",),
        entry_block="entry",
        attrs={"role": "admission"},
        blocks=(
            _entry(
                Binding(Op(OpKind.FROM, {"surface": "node_facts"}), output="payload"),
                Binding(Op(OpKind.CALL, {"function_id": "admit_payload"}), output="admitted", inputs=("payload",)),
                Binding(Op(OpKind.DIFF, {"key": "source_key"}), output="changes", inputs=("payload", "admitted")),
                Binding(
                    Op(OpKind.ASSERT, {"predicate": "narrowings", "contract": "narrowings_hold"}),
                    output="g", inputs=("changes",),
                ),
                Binding(
                    Op(OpKind.DELTA, {"op": "upsert", "target_surface": "node_facts"}),
                    output="admit_delta", inputs=("changes", "g"),
                ),
                Binding(Op(OpKind.EMIT, {"target": "admit_delta"}), inputs=("admit_delta",)),
                returns=("admit_delta",),
            ),
        ),
    )


__all__ = [
    "projection_program",
    "ingestion_program",
    "enrichment_program",
    "admission_program",
]
