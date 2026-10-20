"""Coverage-and-jointness probe (task.gtir.coverage-probe).

The falsifiable claim behind req.gtir.language: the fixed, closed operator set
expresses the substrate's program species without leaking to opaque CALL on the
common case. This module lowers a representative program per species into the
operators and reports, per species, the operator-frequency histogram and the
CALL-escape rate (fraction of ops that are opaque CALLs).

Expectation: the read / consistency species have a CALL-escape of ZERO (they are
pure relational/retrieval). Ingestion and enrichment legitimately use CALL
(parsers, LLM derivation) but bounded. TPC-H/TPC-DS breadth and a live AKMON
trace are SECONDARY checks; they are reported as deferred, never silently dropped.
"""

from __future__ import annotations

from .consistency import narrowing_conformance_program, no_dangling_edges_program
from .expr import col, eq
from .ir import Binding, Block, Op, Terminator, TransformProgram
from .registry import Effect, OpKind, TerminatorKind


def _block(*bindings: Binding, returns: tuple[str, ...]) -> Block:
    return Block("entry", tuple(bindings), Terminator(TerminatorKind.RETURN, {"values": list(returns)}))


def akmon_read_program() -> TransformProgram:
    return TransformProgram(
        "species.akmon_read", effect=Effect.READ_ONLY, params=("q",), outputs=("out",),
        entry_block="entry", blocks=(_block(
            Binding(Op(OpKind.SEARCH, {"surface": "nodes_live", "method": "lexical", "query": "$q", "scorer": "s"}), output="c"),
            Binding(Op(OpKind.WHERE, {"expr": eq(col("subtype"), col("subtype"))}), output="f", inputs=("c",)),
            Binding(Op(OpKind.ORDER, {"key": "score", "desc": True}), output="r", inputs=("f",)),
            Binding(Op(OpKind.LIMIT, {"n": 10}), output="out", inputs=("r",)),
            Binding(Op(OpKind.EMIT, {"target": "out"}), inputs=("out",)),
            returns=("out",)),))


def projection_program() -> TransformProgram:
    return TransformProgram(
        "species.projection", effect=Effect.DELTA, outputs=("live_delta",),
        entry_block="entry", blocks=(_block(
            Binding(Op(OpKind.FROM, {"surface": "node_facts"}), output="prev"),
            Binding(Op(OpKind.FROM, {"surface": "node_facts"}), output="cur"),
            Binding(Op(OpKind.DIFF, {"key": "k"}), output="ch", inputs=("prev", "cur")),
            Binding(Op(OpKind.ASSERT, {"predicate": "no_dangling", "contract": "no_dangling_edges"}), output="g", inputs=("ch",)),
            Binding(Op(OpKind.DELTA, {"op": "upsert", "target_surface": "nodes_live"}), output="live_delta", inputs=("ch", "g")),
            Binding(Op(OpKind.EMIT, {"target": "live_delta"}), inputs=("live_delta",)),
            returns=("live_delta",)),))


def view_refresh_program() -> TransformProgram:
    return TransformProgram(
        "species.view_refresh", effect=Effect.DELTA, outputs=("view_delta",),
        entry_block="entry", blocks=(_block(
            Binding(Op(OpKind.FROM, {"surface": "edges_live"}), output="e"),
            Binding(Op(OpKind.GROUP, {"key": "author", "agg": "count"}), output="agg", inputs=("e",)),
            Binding(Op(OpKind.FROM, {"surface": "edges_live"}), output="old"),
            Binding(Op(OpKind.DIFF, {"key": "author"}), output="ch", inputs=("old", "agg")),
            Binding(Op(OpKind.ASSERT, {"predicate": "always", "contract": "always"}), output="g", inputs=("ch",)),
            Binding(Op(OpKind.DELTA, {"op": "replace", "target_surface": "edges_live"}), output="view_delta", inputs=("ch", "g")),
            Binding(Op(OpKind.EMIT, {"target": "view_delta"}), inputs=("view_delta",)),
            returns=("view_delta",)),))


def ingestion_program() -> TransformProgram:
    # Deterministic parse may share the program with DELTA; one CALL (the parser).
    return TransformProgram(
        "species.ingestion", effect=Effect.DELTA, outputs=("fact_delta",),
        entry_block="entry", blocks=(_block(
            Binding(Op(OpKind.FROM, {"surface": "node_facts"}), output="src"),
            Binding(Op(OpKind.CALL, {"function_id": "parse_records"}), output="parsed", inputs=("src",)),
            Binding(Op(OpKind.DIFF, {"key": "k"}), output="ch", inputs=("src", "parsed")),
            Binding(Op(OpKind.ASSERT, {"predicate": "narrowings", "contract": "narrowings_hold"}), output="g", inputs=("ch",)),
            Binding(Op(OpKind.DELTA, {"op": "upsert", "target_surface": "node_facts"}), output="fact_delta", inputs=("ch", "g")),
            Binding(Op(OpKind.EMIT, {"target": "fact_delta"}), inputs=("fact_delta",)),
            returns=("fact_delta",)),))


def enrichment_compute_program() -> TransformProgram:
    # Pure-compute half of the fork-2 split: one external CALL, output Evidence.
    return TransformProgram(
        "species.enrichment_compute", effect=Effect.EXTERNAL, outputs=("evidence",),
        entry_block="entry", blocks=(_block(
            Binding(Op(OpKind.FROM, {"surface": "nodes_live"}), output="n"),
            Binding(Op(OpKind.CALL, {"function_id": "derive_links"}), output="evidence", inputs=("n",)),
            Binding(Op(OpKind.EMIT, {"target": "evidence"}), inputs=("evidence",)),
            returns=("evidence",)),))


SPECIES = {
    "akmon_read": akmon_read_program,
    "consistency.no_dangling_edges": no_dangling_edges_program,
    "consistency.narrowing_conformance": lambda: narrowing_conformance_program(1),
    "projection": projection_program,
    "view_refresh": view_refresh_program,
    "ingestion": ingestion_program,
    "enrichment_compute": enrichment_compute_program,
}

# Read / consistency species: the common case, expected to need ZERO CALLs.
PURE_CORE_SPECIES = {"akmon_read", "consistency.no_dangling_edges", "consistency.narrowing_conformance"}

DEFERRED = [
    "TPC-H / TPC-DS (121 queries) breadth check -- secondary, not run in this probe",
    "live AKMON read trace lowering -- requires a real deployment trace, deferred",
]


def op_histogram(prog: TransformProgram) -> dict[str, int]:
    hist: dict[str, int] = {}
    for _block, binding in prog.iter_ops():
        hist[binding.op.kind.value] = hist.get(binding.op.kind.value, 0) + 1
    return hist


def call_escape_rate(prog: TransformProgram) -> float:
    hist = op_histogram(prog)
    total = sum(hist.values())
    return hist.get("call", 0) / total if total else 0.0


def coverage_report() -> dict:
    species = {}
    for name, builder in SPECIES.items():
        prog = builder()
        species[name] = {
            "histogram": op_histogram(prog),
            "call_escape_rate": call_escape_rate(prog),
        }
    return {"species": species, "deferred": list(DEFERRED)}


__all__ = ["SPECIES", "PURE_CORE_SPECIES", "DEFERRED", "op_histogram", "call_escape_rate", "coverage_report"]
