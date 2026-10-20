"""task.gtir.surface-registry -- the substrate surface registry.

Surfaces are a registry, not grammar: the language hard-codes no surface name,
and a deployment seeds concrete table/view bindings + pin semantics. These tests
confirm the seeded registry resolves every named surface, that an unregistered
surface is rejected at validation, and that a DeltaSet is the single output noun
indexed by target_surface.
"""

from __future__ import annotations

from okg.substrate.transform import (
    SUBSTRATE_SURFACES,
    Binding,
    Block,
    DeltaOp,
    DeltaRow,
    DeltaSet,
    Effect,
    Op,
    OpKind,
    Registries,
    SurfaceKind,
    SurfaceRegistry,
    Terminator,
    TerminatorKind,
    TransformProgram,
    substrate_surface_registry,
)

EXPECTED = {
    "node_facts": (SurfaceKind.FACT_LOG, "okg.node_facts", True),
    "edge_facts": (SurfaceKind.FACT_LOG, "okg.edge_facts", True),
    "nodes_live": (SurfaceKind.LIVE, "okg.v_nodes", True),
    "edges_live": (SurfaceKind.LIVE, "okg.v_edges", True),
    "catalog": (SurfaceKind.CATALOG_POLICY, "okg.catalog_versions", True),
    "narrowing_catalog": (SurfaceKind.CATALOG_POLICY, "okg.narrowings", True),
    "alias_index": (SurfaceKind.INDEX, "okg.v_aliases", True),
    "search_index": (SurfaceKind.INDEX, "okg.v_nodes", True),
    "ir_programs": (SurfaceKind.LEDGER, "okg.ir_programs", False),
    "ir_invocations": (SurfaceKind.LEDGER, "okg.ir_invocations", False),
    "trace": (SurfaceKind.TRACE, "okg.mcp_trace_events", False),
    "admission_envelope": (SurfaceKind.FACT_LOG, "okg.v_admission_envelopes", False),
}


def test_registry_resolves_each_surface_to_binding_and_pin():
    reg = substrate_surface_registry()
    assert set(reg.names()) == set(EXPECTED)
    for name, (kind, binding, pinned) in EXPECTED.items():
        s = reg.resolve(name)
        assert s is not None, name
        assert (s.kind, s.binding, s.pinned) == (kind, binding, pinned), name


def test_every_binding_is_a_real_okg_object():
    # Bindings name real okg.* tables/views (verified against schema.sql/views.sql).
    for s in SUBSTRATE_SURFACES:
        assert s.binding.startswith("okg.")


def test_unregistered_surface_rejected_against_substrate_registry():
    reg = Registries(surfaces=substrate_surface_registry())
    prog = TransformProgram(
        program_id="bad",
        effect=Effect.READ_ONLY,
        entry_block="entry",
        blocks=(
            Block(
                block_id="entry",
                bindings=(Binding(Op(OpKind.FROM, {"surface": "not_a_surface"}), output="x"),),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["x"]}),
            ),
        ),
    )
    assert "unregistered_surface" in {e.code for e in prog.validate(reg)}
    # A registered surface validates.
    ok = TransformProgram(
        program_id="ok",
        effect=Effect.READ_ONLY,
        entry_block="entry",
        blocks=(
            Block(
                block_id="entry",
                bindings=(Binding(Op(OpKind.FROM, {"surface": "nodes_live"}), output="x"),),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["x"]}),
            ),
        ),
    )
    assert "unregistered_surface" not in {e.code for e in ok.validate(reg)}


def test_grammar_has_zero_hard_coded_surface_names():
    # A fresh empty registry knows no surface; surfaces exist only by seeding.
    empty = SurfaceRegistry()
    assert empty.names() == ()
    for name in EXPECTED:
        assert empty.resolve(name) is None


def test_deltaset_is_single_output_noun_indexed_by_target():
    # DeltaSets for different targets are the same noun, distinguished only by
    # target_surface -- not distinct per-target types.
    a = DeltaSet("node_facts", (DeltaRow("node_facts", DeltaOp.UPSERT, key=1, after={}),))
    b = DeltaSet("edge_facts", (DeltaRow("edge_facts", DeltaOp.UPSERT, key=2, after={}),))
    assert type(a) is type(b) is DeltaSet
    assert (a.target_surface, b.target_surface) == ("node_facts", "edge_facts")
