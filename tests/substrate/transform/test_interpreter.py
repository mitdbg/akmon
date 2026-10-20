"""task.gtir.normalize-and-lower (interpreter half) -- the executable denotation.

The reference interpreter realizes [[op]] over the bag carrier. These tests pin
the denotation of each operator, the per-operator order discipline, the runtime
guard (failed ASSERT -> empty DeltaSet), the antijoin via LEFT JOIN + WHERE null,
and DIFF's value-level change classification.
"""

from __future__ import annotations

from okg.substrate.transform import (
    AssertionResult,
    Binding,
    Block,
    DeltaSet,
    Effect,
    Op,
    OpKind,
    Terminator,
    TerminatorKind,
    TransformProgram,
    evaluate,
)

KERNELS = {
    "is_paper": lambda r: r.get("subtype") == "paper",
    "proj_id": lambda r: {"id": r["id"]},
    "endpoint_absent": lambda r: r.get("node_id") is None,
    "dst_eq_node": lambda lr, rr: lr.get("dst") == rr.get("node_id"),
    "by_kind": lambda r: r["kind"],
    "count_kind": lambda rows: {"kind": rows[0]["kind"], "n": len(rows)},
    "score_desc": lambda r: r["score"],
    "scorer": lambda q, r: r.get("score", 0),
    "id_key": lambda r: r["id"],
    "parse": lambda before: [{"id": 1, "v": "x"}, {"id": 2, "v": "Y"}],
    "always_pass": lambda bag: True,
    "always_fail": lambda bag: False,
    "no_violations": lambda bag: len(bag) == 0,
}


def _entry(*bindings: Binding, term: Terminator | None = None) -> TransformProgram:
    return TransformProgram(
        program_id="t",
        effect=Effect.READ_ONLY,
        entry_block="entry",
        blocks=(
            Block(
                block_id="entry",
                bindings=tuple(bindings),
                terminator=term or Terminator(TerminatorKind.RETURN, {"values": ["out"]}),
            ),
        ),
    )


# --------------------------------------------------------------------------- #
# Denotation of the relational + retrieval + boundary read operators
# --------------------------------------------------------------------------- #


def test_reference_interpreter_denotes_each_operator():
    surfaces = {
        "nodes_live": [
            {"id": 1, "subtype": "paper", "kind": "p", "score": 0.9},
            {"id": 2, "subtype": "author", "kind": "a", "score": 0.5},
            {"id": 3, "subtype": "paper", "kind": "p", "score": 0.7},
        ],
        "search_index": [
            {"id": 1, "score": 0.2},
            {"id": 2, "score": 0.95},
        ],
    }
    prog = _entry(
        Binding(Op(OpKind.FROM, {"surface": "nodes_live"}), output="n"),
        Binding(Op(OpKind.WHERE, {"predicate": "is_paper"}), output="papers", inputs=("n",)),
        Binding(Op(OpKind.SELECT, {"projection": "proj_id"}), output="ids", inputs=("papers",)),
        Binding(Op(OpKind.GROUP, {"key": "by_kind", "agg": "count_kind"}), output="by_kind", inputs=("n",)),
        Binding(Op(OpKind.UNION, {}), output="doubled", inputs=("ids", "ids")),
        Binding(Op(OpKind.SEARCH, {"surface": "search_index", "method": "semantic", "query": "q", "scorer": "scorer"}), output="ranked"),
        Binding(Op(OpKind.LIMIT, {"n": 1}), output="out", inputs=("ranked",)),
        Binding(Op(OpKind.ASSERT, {"predicate": "always_pass", "contract": "c"}), output="a", inputs=("n",)),
        Binding(Op(OpKind.EMIT, {"target": "ranked_out"}), inputs=("ranked",)),
    )
    res = evaluate(prog, surfaces, KERNELS)
    v = res.values
    assert [r["id"] for r in v["papers"]] == [1, 3]              # WHERE preserves order
    assert v["ids"] == [{"id": 1}, {"id": 3}]                    # SELECT projects
    assert {b["kind"]: b["n"] for b in v["by_kind"]} == {"p": 2, "a": 1}  # GROUP
    assert len(v["doubled"]) == 4                                # UNION concatenates bags
    assert [r["id"] for r in v["ranked"]] == [2, 1]             # SEARCH ranks desc by score
    assert v["ranked"][0]["rank"] == 0
    assert [r["id"] for r in v["out"]] == [2]                    # LIMIT after rank
    assert isinstance(v["a"], AssertionResult) and v["a"].passed
    assert res.outputs["ranked_out"][0]["id"] == 2              # EMIT names an output


# --------------------------------------------------------------------------- #
# Order discipline (design 01)
# --------------------------------------------------------------------------- #


def test_order_discipline_join_is_bag_search_is_ordered():
    surfaces = {
        "edges_live": [{"dst": 2}, {"dst": 1}],
        "nodes_live": [{"node_id": 1}, {"node_id": 2}],
        "search_index": [{"id": "a", "score": 0.1}, {"id": "b", "score": 0.9}],
    }
    prog = _entry(
        Binding(Op(OpKind.FROM, {"surface": "edges_live"}), output="e"),
        Binding(Op(OpKind.FROM, {"surface": "nodes_live"}), output="nd"),
        Binding(Op(OpKind.JOIN, {"how": "inner", "condition": "dst_eq_node"}), output="j", inputs=("e", "nd")),
        Binding(Op(OpKind.SEARCH, {"surface": "search_index", "method": "lexical", "query": "q", "scorer": "scorer"}), output="s"),
        Binding(Op(OpKind.EMIT, {"target": "out"}), inputs=("j",)),
    )
    res = evaluate(prog, surfaces, KERNELS)
    # JOIN is a bag op: result is order-agnostic, compared as a multiset.
    assert sorted(r["dst"] for r in res.values["j"]) == [1, 2]
    # SEARCH introduces order: strictly descending by score.
    scores = [r["score"] for r in res.values["s"]]
    assert scores == sorted(scores, reverse=True)


# --------------------------------------------------------------------------- #
# Antijoin: no_dangling_edges via LEFT JOIN + WHERE endpoint IS NULL (P1-b)
# --------------------------------------------------------------------------- #


def test_antijoin_left_join_where_null_finds_dangling_edges():
    surfaces = {
        "edges_live": [{"edge_id": 10, "dst": 1}, {"edge_id": 11, "dst": 99}],
        "nodes_live": [{"node_id": 1}],
    }
    prog = _entry(
        Binding(Op(OpKind.FROM, {"surface": "edges_live"}), output="e"),
        Binding(Op(OpKind.FROM, {"surface": "nodes_live"}), output="nd"),
        Binding(Op(OpKind.JOIN, {"how": "left", "condition": "dst_eq_node"}), output="lj", inputs=("e", "nd")),
        Binding(Op(OpKind.WHERE, {"predicate": "endpoint_absent"}), output="dangling", inputs=("lj",)),
        Binding(Op(OpKind.ASSERT, {"predicate": "no_violations", "contract": "no_dangling_edges"}), output="out", inputs=("dangling",)),
        Binding(Op(OpKind.EMIT, {"target": "result"}), inputs=("out",)),
    )
    res = evaluate(prog, surfaces, KERNELS)
    dangling = res.values["dangling"]
    assert [r["edge_id"] for r in dangling] == [11]            # the absent-endpoint edge
    assert res.values["out"].passed is False                   # consistency fails as expected


# --------------------------------------------------------------------------- #
# DIFF classifies upsert / replace / retract by value (P1-a)
# --------------------------------------------------------------------------- #


def test_diff_classifies_changes():
    surfaces = {
        "before": [{"id": 1, "v": "x"}, {"id": 2, "v": "y"}],
        "after": [{"id": 2, "v": "Y"}, {"id": 3, "v": "z"}],
    }
    prog = _entry(
        Binding(Op(OpKind.FROM, {"surface": "before"}), output="b"),
        Binding(Op(OpKind.FROM, {"surface": "after"}), output="a"),
        Binding(Op(OpKind.DIFF, {"key": "id_key"}), output="ch", inputs=("b", "a")),
        Binding(Op(OpKind.EMIT, {"target": "out"}), inputs=("ch",)),
    )
    res = evaluate(prog, surfaces, KERNELS)
    by_key = {c["key"]: c["_change_op"] for c in res.values["ch"]}
    assert by_key == {3: "upsert", 2: "replace", 1: "retract"}


# --------------------------------------------------------------------------- #
# Runtime guard: failed ASSERT -> empty DeltaSet (req.gtir.guarded-mutation)
# --------------------------------------------------------------------------- #


def _ingest_program() -> TransformProgram:
    return TransformProgram(
        program_id="ingest",
        effect=Effect.DELTA,
        entry_block="entry",
        blocks=(
            Block(
                block_id="entry",
                bindings=(
                    Binding(Op(OpKind.FROM, {"surface": "node_facts"}), output="src"),
                    Binding(Op(OpKind.CALL, {"function_id": "parse"}), output="parsed", inputs=("src",)),
                    Binding(Op(OpKind.DIFF, {"key": "id_key"}), output="ch", inputs=("src", "parsed")),
                    Binding(Op(OpKind.ASSERT, {"predicate": "$guard", "contract": "narrowings"}), output="g", inputs=("ch",)),
                    Binding(Op(OpKind.DELTA, {"op": "upsert", "target_surface": "node_facts"}), output="fd", inputs=("ch", "g")),
                    Binding(Op(OpKind.EMIT, {"target": "fact_delta"}), inputs=("fd",)),
                ),
                terminator=Terminator(TerminatorKind.RETURN, {"values": ["fd"]}),
            ),
        ),
    )


def test_failed_guard_yields_empty_deltaset():
    surfaces = {"node_facts": [{"id": 1, "v": "old"}]}
    prog = _ingest_program()

    passed = evaluate(prog, surfaces, {**KERNELS, "$guard": KERNELS["always_pass"]})
    ds = passed.values["fd"]
    assert isinstance(ds, DeltaSet) and len(ds.rows) > 0
    assert all(r.source_program_checksum == prog.checksum() for r in ds.rows)

    failed = evaluate(prog, surfaces, {**KERNELS, "$guard": KERNELS["always_fail"]})
    assert failed.values["fd"] == DeltaSet("node_facts", ())   # guard failed -> empty
