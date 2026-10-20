"""task.gtir.models -- typed IR construction, validation, content addressing.

Covers the expanded req.gtir.language / req.gtir.guarded-mutation /
req.gtir.mutation-as-data after the design review: two operator categories,
bag carrier + DIFF re-typing, outer JOIN, ITERATE bound + nesting, taint
reachability (CALL ~> DELTA), guard-dominance, and DeltaSet coherence.
"""

from __future__ import annotations

import pytest

from okg.substrate.transform import (
    AssertionResult,
    Binding,
    Block,
    DeltaOp,
    DeltaRow,
    DeltaSet,
    Determinism,
    Effect,
    FunctionRegistry,
    FunctionSpec,
    IRValidationError,
    Op,
    OpKind,
    Registries,
    Surface,
    SurfaceKind,
    SurfaceRegistry,
    Terminator,
    TerminatorKind,
    TransformProgram,
)


def _registries() -> Registries:
    surfaces = SurfaceRegistry(
        [
            Surface("nodes_live", SurfaceKind.LIVE, "okg.v_nodes", pinned=True),
            Surface("edges_live", SurfaceKind.LIVE, "okg.v_edges", pinned=True),
            Surface("node_facts", SurfaceKind.FACT_LOG, "okg.node_facts", pinned=True),
            Surface("search_index", SurfaceKind.INDEX, "okg.search_index", pinned=True),
        ]
    )
    functions = FunctionRegistry(
        [
            FunctionSpec("parse_records", Effect.READ_ONLY, Determinism.DETERMINISTIC),
            FunctionSpec("agent_score", Effect.EXTERNAL, Determinism.OBSERVED_REPLAY),
        ]
    )
    return Registries(surfaces=surfaces, functions=functions)


def _block(*bindings: Binding, returns: tuple[str, ...] = ("out",)) -> Block:
    return Block(
        block_id="entry",
        bindings=tuple(bindings),
        terminator=Terminator(TerminatorKind.RETURN, {"values": list(returns)}),
    )


def _codes(prog: TransformProgram) -> set[str]:
    return {e.code for e in prog.validate(_registries())}


# --------------------------------------------------------------------------- #
# Two categories: 15 dataflow operators + 3 control terminators
# --------------------------------------------------------------------------- #


def test_two_categories_sixteen_ops_three_terminators():
    # Sixteen since RANK (operator decision D1, PACT consistency-claim-model-core
    # designs/graph-derivation-model.md §3 — per-group ranked top-k with basis
    # provenance for the claim-model ≻-resolution). Grew from fifteen 2026-07-06.
    assert len(list(OpKind)) == 16
    assert "rank" in {k.value for k in OpKind}
    assert len(list(TerminatorKind)) == 3
    assert {t.value for t in TerminatorKind} == {"return", "if", "iterate"}  # no WHILE


def test_value_nouns_constructible():
    DeltaSet("node_facts", (DeltaRow("node_facts", DeltaOp.UPSERT, key=1, after={"x": 2}),))
    AssertionResult("no_dangling_edges", passed=True)


# --------------------------------------------------------------------------- #
# Valid programs of each shape
# --------------------------------------------------------------------------- #


def test_read_only_search_program_valid():
    prog = TransformProgram(
        program_id="akmon.search",
        effect=Effect.READ_ONLY,
        params=("q",),
        outputs=("out",),
        entry_block="entry",
        blocks=(
            _block(
                Binding(Op(OpKind.SEARCH, {"surface": "nodes_live", "method": "lexical", "query": "$q", "scorer": "s"}), output="cands"),
                Binding(Op(OpKind.ORDER, {"key": "score"}), output="ranked", inputs=("cands",)),
                Binding(Op(OpKind.LIMIT, {"n": 10}), output="out", inputs=("ranked",)),
                Binding(Op(OpKind.EMIT, {"target": "out"}), inputs=("out",)),
            ),
        ),
    )
    assert prog.validate(_registries()) == []


def _guarded_delta_program() -> TransformProgram:
    # FROM -> CALL(deterministic) -> DIFF(before, after) -> ASSERT(g) -> DELTA(changes, g)
    return TransformProgram(
        program_id="ingest.stark",
        effect=Effect.DELTA,
        outputs=("fact_delta",),
        entry_block="entry",
        blocks=(
            _block(
                Binding(Op(OpKind.FROM, {"surface": "node_facts"}), output="src"),
                Binding(Op(OpKind.CALL, {"function_id": "parse_records"}), output="parsed", inputs=("src",)),
                Binding(Op(OpKind.DIFF, {"key": "k"}), output="changes", inputs=("src", "parsed")),
                Binding(Op(OpKind.ASSERT, {"predicate": "p", "contract": "narrowings_hold"}), output="g", inputs=("changes",)),
                Binding(Op(OpKind.DELTA, {"op": "upsert", "target_surface": "node_facts"}), output="fact_delta", inputs=("changes", "g")),
                Binding(Op(OpKind.EMIT, {"target": "fact_delta"}), inputs=("fact_delta",)),
                returns=("fact_delta",),
            ),
        ),
    )


def test_guarded_delta_program_with_deterministic_call_valid():
    assert _guarded_delta_program().validate(_registries()) == []


def test_external_call_without_delta_ok():
    prog = TransformProgram(
        program_id="ok.external_compute",
        effect=Effect.EXTERNAL,
        outputs=("evidence",),
        entry_block="entry",
        blocks=(
            _block(
                Binding(Op(OpKind.FROM, {"surface": "nodes_live"}), output="n"),
                Binding(Op(OpKind.CALL, {"function_id": "agent_score"}), output="evidence", inputs=("n",)),
                Binding(Op(OpKind.EMIT, {"target": "evidence"}), inputs=("evidence",)),
                returns=("evidence",),
            ),
        ),
    )
    assert prog.validate(_registries()) == []


# --------------------------------------------------------------------------- #
# Registry rejections
# --------------------------------------------------------------------------- #


def test_unregistered_call_function_rejected():
    prog = TransformProgram(
        program_id="bad.call",
        effect=Effect.READ_ONLY,
        entry_block="entry",
        blocks=(
            _block(
                Binding(Op(OpKind.FROM, {"surface": "nodes_live"}), output="n"),
                Binding(Op(OpKind.CALL, {"function_id": "ghost"}), output="out", inputs=("n",)),
                Binding(Op(OpKind.EMIT, {"target": "out"}), inputs=("out",)),
            ),
        ),
    )
    assert "unregistered_call_function" in _codes(prog)


def test_unregistered_surface_rejected_in_from_and_delta():
    from_prog = TransformProgram(
        program_id="bad.from",
        effect=Effect.READ_ONLY,
        entry_block="entry",
        blocks=(_block(Binding(Op(OpKind.FROM, {"surface": "ghost"}), output="out")),),
    )
    delta_prog = TransformProgram(
        program_id="bad.delta",
        effect=Effect.DELTA,
        entry_block="entry",
        blocks=(
            _block(
                Binding(Op(OpKind.FROM, {"surface": "node_facts"}), output="src"),
                Binding(Op(OpKind.ASSERT, {"predicate": "p", "contract": "c"}), output="g", inputs=("src",)),
                Binding(Op(OpKind.DELTA, {"op": "upsert", "target_surface": "ghost"}), output="out", inputs=("src", "g")),
            ),
        ),
    )
    assert "unregistered_surface" in _codes(from_prog)
    assert "unregistered_surface" in _codes(delta_prog)


# --------------------------------------------------------------------------- #
# P1-a: DIFF is R x R -> R_changes; DELTA is the sole DeltaSet constructor
# --------------------------------------------------------------------------- #


def test_diff_is_change_relation_and_delta_is_sole_deltaset_constructor():
    # DIFF requires two relation inputs (before, after).
    bad_arity = TransformProgram(
        program_id="bad.diff",
        effect=Effect.READ_ONLY,
        entry_block="entry",
        blocks=(
            _block(
                Binding(Op(OpKind.FROM, {"surface": "node_facts"}), output="a"),
                Binding(Op(OpKind.DIFF, {"key": "k"}), output="d", inputs=("a",)),
                Binding(Op(OpKind.EMIT, {"target": "d"}), inputs=("d",)),
            ),
        ),
    )
    assert "diff_arity" in _codes(bad_arity)
    # The guarded ingestion program (DIFF -> ASSERT -> DELTA) is the canonical
    # shape and validates; DELTA is the only op that yields a DeltaSet.
    assert _guarded_delta_program().validate(_registries()) == []
    assert OpKind.DELTA.value == "delta"


# --------------------------------------------------------------------------- #
# P1-b: outer JOIN variants (antijoin needs LEFT JOIN)
# --------------------------------------------------------------------------- #


def test_join_variants_and_arity():
    def join_prog(how: str, n_inputs: int) -> TransformProgram:
        inputs = ("a", "b")[:n_inputs]
        return TransformProgram(
            program_id="j",
            effect=Effect.READ_ONLY,
            entry_block="entry",
            blocks=(
                _block(
                    Binding(Op(OpKind.FROM, {"surface": "edges_live"}), output="a"),
                    Binding(Op(OpKind.FROM, {"surface": "nodes_live"}), output="b"),
                    Binding(Op(OpKind.JOIN, {"how": how, "condition": "c"}), output="out", inputs=inputs),
                    Binding(Op(OpKind.EMIT, {"target": "out"}), inputs=("out",)),
                ),
            ),
        )

    assert join_prog("left", 2).validate(_registries()) == []   # antijoin base
    assert "invalid_join_how" in _codes(join_prog("weird", 2))
    assert "join_arity" in _codes(join_prog("inner", 1))


# --------------------------------------------------------------------------- #
# P0-b: guard-dominance  (req.gtir.guarded-mutation)
# --------------------------------------------------------------------------- #


def test_delta_without_dominating_guard_rejected():
    prog = TransformProgram(
        program_id="bad.unguarded",
        effect=Effect.DELTA,
        entry_block="entry",
        blocks=(
            _block(
                Binding(Op(OpKind.FROM, {"surface": "node_facts"}), output="src"),
                Binding(Op(OpKind.DELTA, {"op": "upsert", "target_surface": "node_facts"}), output="d", inputs=("src",)),
                Binding(Op(OpKind.EMIT, {"target": "d"}), inputs=("d",)),
            ),
        ),
    )
    assert "unguarded_delta" in _codes(prog)
    # Wiring the ASSERT result into the DELTA removes the error.
    assert "unguarded_delta" not in _codes(_guarded_delta_program())


# --------------------------------------------------------------------------- #
# P1-c: taint reachability  (req.gtir.mutation-as-data)
# --------------------------------------------------------------------------- #


def test_external_call_taint_reaching_delta_rejected():
    # agent_score (external) feeds the DELTA input -> tainted_delta_input.
    prog = TransformProgram(
        program_id="bad.tainted",
        effect=Effect.DELTA,
        entry_block="entry",
        blocks=(
            _block(
                Binding(Op(OpKind.FROM, {"surface": "nodes_live"}), output="n"),
                Binding(Op(OpKind.CALL, {"function_id": "agent_score"}), output="scored", inputs=("n",)),
                Binding(Op(OpKind.ASSERT, {"predicate": "p", "contract": "c"}), output="g", inputs=("scored",)),
                Binding(Op(OpKind.DELTA, {"op": "upsert", "target_surface": "node_facts"}), output="d", inputs=("scored", "g")),
            ),
        ),
    )
    assert "tainted_delta_input" in _codes(prog)
    # The deterministic-CALL ingestion program is NOT tainted.
    assert "tainted_delta_input" not in _codes(_guarded_delta_program())


def test_read_only_program_with_delta_rejected():
    prog = TransformProgram(
        program_id="bad.read_only_delta",
        effect=Effect.READ_ONLY,
        entry_block="entry",
        blocks=(
            _block(
                Binding(Op(OpKind.FROM, {"surface": "node_facts"}), output="src"),
                Binding(Op(OpKind.ASSERT, {"predicate": "p", "contract": "c"}), output="g", inputs=("src",)),
                Binding(Op(OpKind.DELTA, {"op": "upsert", "target_surface": "node_facts"}), output="d", inputs=("src", "g")),
            ),
        ),
    )
    assert "read_only_program_has_delta" in _codes(prog)


# --------------------------------------------------------------------------- #
# P2-a: ITERATE bound + nesting
# --------------------------------------------------------------------------- #


def test_iterate_requires_positive_max_steps():
    def iter_prog(args: dict) -> TransformProgram:
        return TransformProgram(
            program_id="loop",
            effect=Effect.READ_ONLY,
            entry_block="entry",
            blocks=(
                Block(
                    block_id="entry",
                    bindings=(Binding(Op(OpKind.FROM, {"surface": "nodes_live"}), output="frontier"),),
                    terminator=Terminator(TerminatorKind.ITERATE, args),
                ),
            ),
        )

    assert "unbounded_iteration" in {e.code for e in iter_prog({}).validate(_registries())}
    assert "unbounded_iteration" in {e.code for e in iter_prog({"max_steps": 0}).validate(_registries())}
    assert "unbounded_iteration" not in {e.code for e in iter_prog({"max_steps": 8}).validate(_registries())}


def test_iterate_nesting_bound():
    # Four nested ITERATE blocks exceeds MAX_ITERATE_NESTING (3).
    blocks = []
    for i in range(5):
        nxt = f"b{i + 1}"
        blocks.append(
            Block(
                block_id=f"b{i}",
                bindings=(Binding(Op(OpKind.FROM, {"surface": "nodes_live"}), output=f"s{i}"),),
                terminator=Terminator(TerminatorKind.ITERATE, {"max_steps": 2, "step_block": nxt}),
            )
        )
    blocks.append(Block(block_id="b5", bindings=(), terminator=Terminator(TerminatorKind.RETURN, {"values": []})))
    prog = TransformProgram(program_id="deep", effect=Effect.READ_ONLY, entry_block="b0", blocks=tuple(blocks))
    assert "iterate_nesting_exceeded" in _codes(prog)


# --------------------------------------------------------------------------- #
# P2-b: DeltaSet coherence
# --------------------------------------------------------------------------- #


def test_incoherent_deltaset_rejected():
    retract_with_after = DeltaSet(
        "node_facts", (DeltaRow("node_facts", DeltaOp.RETRACT, key=1, after={"x": 1}),)
    )
    upsert_without_after = DeltaSet(
        "node_facts", (DeltaRow("node_facts", DeltaOp.UPSERT, key=1),)
    )
    conflicting_keys = DeltaSet(
        "node_facts",
        (
            DeltaRow("node_facts", DeltaOp.UPSERT, key=1, after={"x": 1}),
            DeltaRow("node_facts", DeltaOp.UPSERT, key=1, after={"x": 2}),
        ),
    )
    assert {e.code for e in retract_with_after.coherence_errors()} == {"incoherent_delta"}
    assert {e.code for e in upsert_without_after.coherence_errors()} == {"incoherent_delta"}
    assert "conflicting_delta_key" in {e.code for e in conflicting_keys.coherence_errors()}
    # A coherent set is clean.
    good = DeltaSet("node_facts", (DeltaRow("node_facts", DeltaOp.UPSERT, key=1, after={"x": 1}),))
    assert good.coherence_errors() == []


def test_delta_set_target_consistency_enforced():
    good = DeltaRow("node_facts", DeltaOp.UPSERT, key=1, after={})
    bad = DeltaRow("edges_live", DeltaOp.UPSERT, key=2, after={})
    with pytest.raises(ValueError):
        DeltaSet("node_facts", (good, bad))


# --------------------------------------------------------------------------- #
# Structural + content addressing
# --------------------------------------------------------------------------- #


def test_undefined_symbol_rejected():
    prog = TransformProgram(
        program_id="bad.ssa",
        effect=Effect.READ_ONLY,
        entry_block="entry",
        blocks=(_block(Binding(Op(OpKind.WHERE, {"predicate": "p"}), output="out", inputs=("nope",))),),
    )
    assert "undefined_symbol" in _codes(prog)


def test_validate_or_raise():
    prog = TransformProgram(
        program_id="bad",
        effect=Effect.READ_ONLY,
        entry_block="missing",
        blocks=(_block(Binding(Op(OpKind.FROM, {"surface": "nodes_live"}), output="out")),),
    )
    with pytest.raises(IRValidationError):
        prog.validate_or_raise(_registries())


def _sample_program() -> TransformProgram:
    return TransformProgram(
        program_id="akmon.search",
        effect=Effect.READ_ONLY,
        params=("q",),
        outputs=("out",),
        entry_block="entry",
        blocks=(
            _block(
                Binding(Op(OpKind.SEARCH, {"surface": "nodes_live", "method": "lexical", "query": "$q", "scorer": "s"}), output="cands"),
                Binding(Op(OpKind.LIMIT, {"n": 10}), output="out", inputs=("cands",)),
                Binding(Op(OpKind.EMIT, {"target": "out"}), inputs=("out",)),
            ),
        ),
    )


def test_checksum_stable_across_json_round_trip():
    prog = _sample_program()
    restored = TransformProgram.from_dict(prog.to_dict())
    assert restored.to_dict() == prog.to_dict()
    assert restored.checksum() == prog.checksum()
    assert prog.checksum().startswith("sha256:")


def test_checksum_changes_when_an_op_changes():
    prog = _sample_program()
    mutated = TransformProgram.from_dict(prog.to_dict())
    mutated.blocks[0].bindings[1].op.args["n"] = 5
    assert mutated.checksum() != prog.checksum()
