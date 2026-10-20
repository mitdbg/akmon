"""task.gtir.normalize-and-lower (normalization half) -- sugar -> core.

Sugar normalizes into core bindings before costing/verification/ledgering, the
normal form is byte-stable, and a one-hop EXPAND equals a hand-written edge join.
There is no primitive traversal: WALK lowers to a bounded ITERATE.
"""

from __future__ import annotations

from okg.substrate.transform import (
    Binding,
    Block,
    Effect,
    FunctionRegistry,
    Op,
    OpKind,
    Registries,
    Sugar,
    SugarKind,
    Surface,
    SurfaceKind,
    SurfaceRegistry,
    Terminator,
    TerminatorKind,
    TransformProgram,
    evaluate,
    expand,
    normalize_block,
    walk_blocks,
)

CORE_KINDS = set(OpKind)


def _expand_registry() -> Registries:
    return Registries(
        surfaces=SurfaceRegistry(
            [
                Surface("seed", SurfaceKind.LIVE, "okg.v_nodes", pinned=True),
                Surface("edges", SurfaceKind.LIVE, "okg.v_edges", pinned=True),
                Surface("nodes", SurfaceKind.LIVE, "okg.v_nodes", pinned=True),
            ]
        ),
        functions=FunctionRegistry(),
    )


# --------------------------------------------------------------------------- #
# Sugar expands to ONLY core operators, deterministically
# --------------------------------------------------------------------------- #


def test_each_sugar_expands_to_only_core_ops():
    samples = [
        Sugar(SugarKind.TOPK, "out", {"key": "k", "n": 5}, inputs=("rel",)),
        Sugar(SugarKind.UPSERT, "out", {"target_surface": "node_facts"}, inputs=("ch", "g")),
        Sugar(SugarKind.DELETE, "out", {"target_surface": "node_facts"}, inputs=("ch", "g")),
        Sugar(SugarKind.PATCH, "out", {"target_surface": "node_facts"}, inputs=("ch", "g")),
        Sugar(SugarKind.EXPAND, "out", {"edge_surface": "edges", "node_surface": "nodes", "frontier_edge_cond": "fe", "edge_node_cond": "en", "projection": "p"}, inputs=("frontier",)),
        Sugar(SugarKind.MATCH, "out", {"src_surface": "nodes", "edge_surface": "edges", "dst_surface": "nodes", "src_edge_cond": "se", "edge_dst_cond": "ed", "projection": "p"}),
    ]
    for s in samples:
        kinds = {b.op.kind for b in expand(s)}
        assert kinds <= CORE_KINDS, (s.kind, kinds)


def test_normalization_is_byte_stable():
    s = Sugar(SugarKind.EXPAND, "out", {"edge_surface": "edges", "node_surface": "nodes", "frontier_edge_cond": "fe", "edge_node_cond": "en", "projection": "p"}, inputs=("frontier",))
    a = [b.to_dict() for b in expand(s)]
    b = [b.to_dict() for b in expand(s)]
    assert a == b


def test_topk_is_order_then_limit():
    kinds = [b.op.kind for b in expand(Sugar(SugarKind.TOPK, "out", {"key": "score", "n": 3}, inputs=("rel",)))]
    assert kinds == [OpKind.ORDER, OpKind.LIMIT]


def test_upsert_delete_patch_map_to_delta_ops():
    assert expand(Sugar(SugarKind.UPSERT, "o", {"target_surface": "node_facts"}, inputs=("c", "g")))[0].op.args["op"] == "upsert"
    assert expand(Sugar(SugarKind.DELETE, "o", {"target_surface": "node_facts"}, inputs=("c", "g")))[0].op.args["op"] == "retract"
    assert expand(Sugar(SugarKind.PATCH, "o", {"target_surface": "node_facts"}, inputs=("c", "g")))[0].op.args["op"] == "patch"


# --------------------------------------------------------------------------- #
# One-hop EXPAND equals a hand-written edge join (interpreter equivalence)
# --------------------------------------------------------------------------- #

_SURFACES = {
    "seed": [{"fid": 1}],
    "edges": [{"src": 1, "dst": 2}, {"src": 1, "dst": 3}, {"src": 9, "dst": 4}],
    "nodes": [{"node_id": 1, "name": "A"}, {"node_id": 2, "name": "B"}, {"node_id": 3, "name": "C"}],
}
_KERNELS = {
    "fe": lambda fr, e: fr["fid"] == e["src"],
    "en": lambda je, n: je["dst"] == n["node_id"],
    "proj": lambda r: {"neighbor": r["node_id"], "name": r["name"]},
}


def _run(block: Block) -> list:
    prog = TransformProgram(program_id="t", effect=Effect.READ_ONLY, entry_block="entry", blocks=(block,))
    return evaluate(prog, _SURFACES, _KERNELS).values["neighbors"]


def test_one_hop_expand_equals_hand_written_join():
    sugar_block = normalize_block(
        "entry",
        [
            Binding(Op(OpKind.FROM, {"surface": "seed"}), output="frontier"),
            Sugar(SugarKind.EXPAND, "neighbors", {"edge_surface": "edges", "node_surface": "nodes", "frontier_edge_cond": "fe", "edge_node_cond": "en", "projection": "proj"}, inputs=("frontier",)),
        ],
        returns=("neighbors",),
    )
    hand = Block(
        block_id="entry",
        bindings=(
            Binding(Op(OpKind.FROM, {"surface": "seed"}), output="frontier"),
            Binding(Op(OpKind.FROM, {"surface": "edges"}), output="e"),
            Binding(Op(OpKind.JOIN, {"how": "inner", "condition": "fe"}), output="je", inputs=("frontier", "e")),
            Binding(Op(OpKind.FROM, {"surface": "nodes"}), output="n"),
            Binding(Op(OpKind.JOIN, {"how": "inner", "condition": "en"}), output="jn", inputs=("je", "n")),
            Binding(Op(OpKind.SELECT, {"projection": "proj"}), output="neighbors", inputs=("jn",)),
        ),
        terminator=Terminator(TerminatorKind.RETURN, {"values": ["neighbors"]}),
    )
    expected = [{"neighbor": 2, "name": "B"}, {"neighbor": 3, "name": "C"}]
    got_sugar = sorted(_run(sugar_block), key=lambda r: r["neighbor"])
    got_hand = sorted(_run(hand), key=lambda r: r["neighbor"])
    assert got_sugar == expected
    assert got_sugar == got_hand


# --------------------------------------------------------------------------- #
# WALK lowers to a bounded ITERATE (no primitive traversal)
# --------------------------------------------------------------------------- #


def test_walk_lowers_to_bounded_iterate():
    walk = Sugar(
        SugarKind.WALK,
        "final_frontier",
        {
            "seed_surface": "seed",
            "edge_surface": "edges",
            "node_surface": "nodes",
            "max_steps": 4,
            "frontier_edge_cond": "fe",
            "edge_node_cond": "en",
            "projection": "proj",
        },
    )
    entry, step = walk_blocks(walk)
    prog = TransformProgram(
        program_id="walk", effect=Effect.READ_ONLY, entry_block="entry", blocks=(entry, step)
    )
    # Bounded ITERATE terminator, and the step body is all core ops.
    assert entry.terminator.kind is TerminatorKind.ITERATE
    assert entry.terminator.args["max_steps"] == 4
    assert {b.op.kind for b in step.bindings} <= CORE_KINDS
    assert prog.validate(_expand_registry()) == []
