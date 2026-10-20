"""RANK — the sixteenth operator (decision D1, graph-derivation design §3).

Per-group ranking under an ordered component list, emitting the 1-based rank
and, on each partition's leader row, the comparison provenance (the first
component separating leader from runner-up; ``sole`` / ``tie`` otherwise).

Proofs here, per task.core.window-op:
- interpreter == lowered SQL on a claims-shaped fixture including NULL
  components (NULLS LAST pinned in both directions) and full ties;
- the post-RANK ops (WHERE on the rank, SELECT, ORDER, LIMIT) lower through
  the nested L3 shape and agree with the interpreter;
- RANK reproduces ``claim_model.resolve_bucket`` (ranked order + basis) on
  seeded random signal buckets — the differential oracle;
- shapes the lowering cannot compile faithfully are REJECTED
  (GROUP with RANK, ORDER before RANK, two RANKs, non-identifier component
  names) and malformed args fail validation;
- existing program checksums are UNAFFECTED by the language extension
  (pinned golden digest), and a RANK program's checksum is deterministic;
- the locality analyzer forces RANK to a full sweep by name, and
  default-denies unrecognized ops.
"""

from __future__ import annotations

import os
import random

import pytest

from okg.substrate.consistency.claim_model import (
    OUTCOME_CANONICAL,
    ClaimSignals,
    resolve_bucket,
)
from okg.substrate.consistency.ir_locality import derive_frontier_plan
from okg.substrate.transform import (
    Binding,
    Block,
    Effect,
    Op,
    OpKind,
    Registries,
    Surface,
    SurfaceKind,
    SurfaceRegistry,
    Terminator,
    TerminatorKind,
    TransformProgram,
    col,
    evaluate,
    lower_query,
)
from okg.substrate.transform.expr import le, lit
from okg.substrate.transform.lower import LoweringError

psycopg = pytest.importorskip("psycopg")

DSN = os.environ.get("OKG_TEST_DSN")

# The seven ≻ signal components, ranked descending, claim_id as the tiebreak —
# the claim-model resolution shape this op exists for.
SIGNAL_COMPONENTS = [
    {"name": "operator_precedent", "expr": col("precedent_rank"), "desc": True},
    {"name": "explicit_supersession", "expr": col("not_superseded"), "desc": True},
    {"name": "claim_local_effective_date", "expr": col("effective_date"), "desc": True},
    {"name": "declared_source_authority", "expr": col("source_authority"), "desc": True},
    {"name": "dependency_adjusted_support", "expr": col("independent_support"), "desc": True},
    {"name": "document_recency", "expr": col("document_recency"), "desc": True},
    {"name": "maintenance_heuristic", "expr": col("maintenance_rank"), "desc": True},
]

CLAIMS = [
    # bucket a: authority separates 1st from 2nd -> basis declared_source_authority
    {"bucket": "a", "claim_id": "a1", "precedent_rank": 0, "not_superseded": 1,
     "effective_date": "2024-01-01", "source_authority": 5, "independent_support": 1,
     "document_recency": "2020-01-01", "maintenance_rank": 0},
    {"bucket": "a", "claim_id": "a2", "precedent_rank": 0, "not_superseded": 1,
     "effective_date": "2024-01-01", "source_authority": 1, "independent_support": 1,
     "document_recency": "2026-01-01", "maintenance_rank": 0},
    # bucket b: full tie -> basis "tie" (tiebreak orders but never separates)
    {"bucket": "b", "claim_id": "b1", "precedent_rank": 0, "not_superseded": 1,
     "effective_date": "", "source_authority": 2, "independent_support": 0,
     "document_recency": "", "maintenance_rank": 0},
    {"bucket": "b", "claim_id": "b2", "precedent_rank": 0, "not_superseded": 1,
     "effective_date": "", "source_authority": 2, "independent_support": 0,
     "document_recency": "", "maintenance_rank": 0},
    # bucket c: sole claim
    {"bucket": "c", "claim_id": "c1", "precedent_rank": 1, "not_superseded": 1,
     "effective_date": "2020-01-01", "source_authority": 0, "independent_support": 0,
     "document_recency": "", "maintenance_rank": 0},
    # bucket d: NULL effective dates on both sides (same under IS DISTINCT FROM),
    # NULL recency vs value (differs) — but support separates FIRST
    {"bucket": "d", "claim_id": "d1", "precedent_rank": 0, "not_superseded": 1,
     "effective_date": None, "source_authority": 2, "independent_support": 3,
     "document_recency": None, "maintenance_rank": 1},
    {"bucket": "d", "claim_id": "d2", "precedent_rank": 0, "not_superseded": 1,
     "effective_date": None, "source_authority": 2, "independent_support": 1,
     "document_recency": "2026-01-01", "maintenance_rank": 1},
    # bucket e: decisives all tied, recency differs (ranking-only basis) and the
    # NULL recency sorts LAST despite DESC (NULLS LAST pinned)
    {"bucket": "e", "claim_id": "e1", "precedent_rank": 0, "not_superseded": 1,
     "effective_date": "", "source_authority": 1, "independent_support": 0,
     "document_recency": None, "maintenance_rank": 0},
    {"bucket": "e", "claim_id": "e2", "precedent_rank": 0, "not_superseded": 1,
     "effective_date": "", "source_authority": 1, "independent_support": 0,
     "document_recency": "2021-01-01", "maintenance_rank": 0},
]


def _registry() -> Registries:
    return Registries(
        surfaces=SurfaceRegistry(
            [Surface("claims", SurfaceKind.LIVE, "tmp_claims", pinned=True)]
        )
    )


def _rank_bindings(extra=()):
    return (
        Binding(Op(OpKind.FROM, {"surface": "claims"}), output="c"),
        Binding(
            Op(
                OpKind.RANK,
                {
                    "partition": [col("bucket")],
                    "components": SIGNAL_COMPONENTS,
                    "tiebreak": {"expr": col("claim_id"), "desc": True},
                    "rank_as": "rnk",
                    "basis_as": "basis",
                },
            ),
            output="ranked", inputs=("c",),
        ),
        *extra,
        Binding(Op(OpKind.EMIT, {"target": "out"}), inputs=(extra[-1].output if extra else "ranked",)),
    )


def _program(bindings, outputs=("ranked",)) -> TransformProgram:
    return TransformProgram(
        program_id="consistency.rank_test",
        effect=Effect.READ_ONLY,
        outputs=outputs,
        entry_block="entry",
        blocks=(
            Block(
                block_id="entry",
                bindings=bindings,
                terminator=Terminator(TerminatorKind.RETURN, {"values": list(outputs)}),
            ),
        ),
    )


def _seed(cur, claims) -> None:
    cur.execute(
        "CREATE TEMP TABLE tmp_claims (bucket text, claim_id text, "
        "precedent_rank int, not_superseded int, effective_date text, "
        "source_authority int, independent_support int, "
        "document_recency text, maintenance_rank int)"
    )
    cols = [
        "bucket", "claim_id", "precedent_rank", "not_superseded", "effective_date",
        "source_authority", "independent_support", "document_recency",
        "maintenance_rank",
    ]
    cur.executemany(
        "INSERT INTO tmp_claims VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        [tuple(c[k] for k in cols) for c in claims],
    )


EXPECTED = {
    # claim_id -> (rank, basis-on-leader)
    "a1": (1, "declared_source_authority"), "a2": (2, None),
    "b2": (1, "tie"), "b1": (2, None),  # tiebreak claim_id DESC orders b2 first
    "c1": (1, "sole"),
    "d1": (1, "dependency_adjusted_support"), "d2": (2, None),
    "e2": (1, "document_recency"), "e1": (2, None),  # NULL recency LAST under DESC
}


@pytest.mark.postgres
@pytest.mark.skipif(not DSN, reason="OKG_TEST_DSN not set")
def test_rank_lowered_sql_equals_interpreter():
    prog = _program(_rank_bindings())
    reg = _registry()
    assert prog.validate(reg) == []

    interp = evaluate(prog, {"claims": CLAIMS}, kernels={})
    interp_rows = sorted(
        (r["claim_id"], r["rnk"], r["basis"]) for r in interp.values["ranked"]
    )
    assert dict((cid, (rk, b)) for cid, rk, b in interp_rows) == EXPECTED

    sql, params = lower_query(prog, reg)
    assert "ROW_NUMBER() OVER" in sql and "IS DISTINCT FROM" in sql
    assert "NULLS LAST" in sql

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        _seed(cur, CLAIMS)
        cur.execute(
            f"SELECT claim_id, rnk, basis FROM (\n{sql}\n) q", params
        )
        sql_rows = sorted((r[0], r[1], r[2]) for r in cur.fetchall())
        cur.execute("DROP TABLE tmp_claims")
    assert sql_rows == interp_rows


@pytest.mark.postgres
@pytest.mark.skipif(not DSN, reason="OKG_TEST_DSN not set")
def test_rank_post_ops_lower_through_nested_shape():
    # rank -> WHERE rnk <= 2 -> SELECT -> ORDER -> LIMIT: the L3 level.
    extra = (
        Binding(
            Op(OpKind.WHERE, {"expr": le(col("rnk"), lit(1))}),
            output="leaders", inputs=("ranked",),
        ),
        Binding(
            Op(OpKind.SELECT, {"project": {
                "bucket": col("bucket"), "claim_id": col("claim_id"),
                "basis": col("basis"),
            }}),
            output="out", inputs=("leaders",),
        ),
        Binding(
            Op(OpKind.ORDER, {"expr": col("bucket")}),
            output="ordered", inputs=("out",),
        ),
        Binding(Op(OpKind.LIMIT, {"n": 4}), output="top", inputs=("ordered",)),
    )
    bindings = (
        Binding(Op(OpKind.FROM, {"surface": "claims"}), output="c"),
        Binding(
            Op(OpKind.RANK, {
                "partition": [col("bucket")],
                "components": SIGNAL_COMPONENTS,
                "tiebreak": {"expr": col("claim_id"), "desc": True},
                "rank_as": "rnk", "basis_as": "basis",
            }),
            output="ranked", inputs=("c",),
        ),
        *extra,
        Binding(Op(OpKind.EMIT, {"target": "out"}), inputs=("top",)),
    )
    prog = _program(bindings, outputs=("top",))
    reg = _registry()
    assert prog.validate(reg) == []

    interp = evaluate(prog, {"claims": CLAIMS}, kernels={})
    interp_rows = [(r["bucket"], r["claim_id"], r["basis"]) for r in interp.values["top"]]

    sql, params = lower_query(prog, reg)
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        _seed(cur, CLAIMS)
        cur.execute(sql, params)
        sql_rows = [(r[0], r[1], r[2]) for r in cur.fetchall()]
        cur.execute("DROP TABLE tmp_claims")

    assert interp_rows == sql_rows
    assert interp_rows == [
        ("a", "a1", "declared_source_authority"),
        ("b", "b2", "tie"),
        ("c", "c1", "sole"),
        ("d", "d1", "dependency_adjusted_support"),
    ]


def test_rank_reproduces_resolve_bucket():
    """Differential oracle: RANK's order + basis == claim_model.resolve_bucket
    on seeded random signal buckets (precedent applicability pre-resolved to a
    precedent_rank column, as the tier-3 dataflow would)."""
    rng = random.Random(0x9A4C)
    dates = ["", "2020-01-01", "2024-06-01"]
    for _ in range(120):
        n = rng.randint(1, 5)
        signals = [
            ClaimSignals(
                claim_id=f"claim:{i}",
                is_superseded=rng.random() < 0.3,
                effective_date=rng.choice(dates),
                source_authority=rng.randint(0, 2),
                independent_support=rng.randint(0, 2),
                document_recency=rng.choice(dates),
                maintenance_rank=rng.randint(0, 2),
            )
            for i in range(n)
        ]
        oracle = resolve_bucket(signals)  # no precedents: precedent_rank 0
        rows = [
            {
                "bucket": "x", "claim_id": s.claim_id, "precedent_rank": 0,
                "not_superseded": 0 if s.is_superseded else 1,
                "effective_date": s.effective_date,
                "source_authority": s.source_authority,
                "independent_support": s.independent_support,
                "document_recency": s.document_recency,
                "maintenance_rank": s.maintenance_rank,
            }
            for s in signals
        ]
        rng.shuffle(rows)
        prog = _program(_rank_bindings())
        out = evaluate(prog, {"claims": rows}, kernels={}).values["ranked"]
        by_rank = sorted(out, key=lambda r: r["rnk"])
        assert tuple(r["claim_id"] for r in by_rank) == oracle.ranked_claim_ids
        leader = by_rank[0]
        if oracle.basis == "sole_claim":
            assert leader["basis"] == "sole"
        elif oracle.basis == "tie":
            assert leader["basis"] == "tie"
        else:
            assert leader["basis"] == oracle.basis
        if oracle.outcome == OUTCOME_CANONICAL and oracle.basis != "sole_claim":
            assert leader["claim_id"] == oracle.canonical_claim_id


def test_rank_shape_rejections_and_validation():
    reg = _registry()
    rank_op = Op(OpKind.RANK, {
        "partition": [col("bucket")], "components": SIGNAL_COMPONENTS,
        "rank_as": "rnk", "basis_as": "basis",
    })

    # GROUP does not compose with RANK
    bindings = (
        Binding(Op(OpKind.FROM, {"surface": "claims"}), output="c"),
        Binding(rank_op, output="ranked", inputs=("c",)),
        Binding(
            Op(OpKind.GROUP, {"key": {"bucket": col("bucket")}, "agg": {"n": {"fn": "count"}}}),
            output="g", inputs=("ranked",),
        ),
        Binding(Op(OpKind.EMIT, {"target": "out"}), inputs=("g",)),
    )
    with pytest.raises(LoweringError):
        lower_query(_program(bindings, outputs=("g",)), reg)

    # ORDER before RANK violates the canonical shape
    bindings = (
        Binding(Op(OpKind.FROM, {"surface": "claims"}), output="c"),
        Binding(Op(OpKind.ORDER, {"expr": col("claim_id")}), output="o", inputs=("c",)),
        Binding(rank_op, output="ranked", inputs=("o",)),
        Binding(Op(OpKind.EMIT, {"target": "out"}), inputs=("ranked",)),
    )
    with pytest.raises(LoweringError):
        lower_query(_program(bindings), reg)

    # two RANKs
    bindings = (
        Binding(Op(OpKind.FROM, {"surface": "claims"}), output="c"),
        Binding(rank_op, output="ranked", inputs=("c",)),
        Binding(
            Op(OpKind.RANK, {"components": [{"name": "n", "expr": col("rnk")}],
                             "rank_as": "rnk2"}),
            output="r2", inputs=("ranked",),
        ),
        Binding(Op(OpKind.EMIT, {"target": "out"}), inputs=("r2",)),
    )
    with pytest.raises(LoweringError):
        lower_query(_program(bindings, outputs=("r2",)), reg)

    # non-identifier component name is rejected at lowering (inline literal)
    bindings = (
        Binding(Op(OpKind.FROM, {"surface": "claims"}), output="c"),
        Binding(
            Op(OpKind.RANK, {"components": [{"name": "bad'name", "expr": col("claim_id")}],
                             "basis_as": "basis"}),
            output="ranked", inputs=("c",),
        ),
        Binding(Op(OpKind.EMIT, {"target": "out"}), inputs=("ranked",)),
    )
    with pytest.raises(LoweringError):
        lower_query(_program(bindings), reg)

    # validation: empty components / duplicate names / output-name collision
    def _validate_with(rank_args):
        bindings = (
            Binding(Op(OpKind.FROM, {"surface": "claims"}), output="c"),
            Binding(Op(OpKind.RANK, rank_args), output="ranked", inputs=("c",)),
            Binding(Op(OpKind.EMIT, {"target": "out"}), inputs=("ranked",)),
        )
        return [e.code for e in _program(bindings).validate(reg)]

    # review F1: post-RANK tail permutations are REJECTED, never mis-compiled
    def _tail(extra):
        return (
            Binding(Op(OpKind.FROM, {"surface": "claims"}), output="c"),
            Binding(rank_op, output="ranked", inputs=("c",)),
            *extra,
            Binding(Op(OpKind.EMIT, {"target": "out"}), inputs=(extra[-1].output,)),
        )

    sel = Binding(
        Op(OpKind.SELECT, {"project": {"b": col("bucket"), "r": col("rnk")}}),
        output="s", inputs=("ranked",),
    )
    whr = Binding(Op(OpKind.WHERE, {"expr": le(col("rnk"), lit(1))}), output="w", inputs=("s",))
    with pytest.raises(LoweringError):  # WHERE after SELECT (CX3)
        lower_query(_program(_tail((sel, whr)), outputs=("w",)), reg)
    ordr = Binding(Op(OpKind.ORDER, {"expr": col("claim_id")}), output="o", inputs=("ranked",))
    lim = Binding(Op(OpKind.LIMIT, {"n": 1}), output="l", inputs=("o",))
    whr2 = Binding(Op(OpKind.WHERE, {"expr": le(col("rnk"), lit(1))}), output="w2", inputs=("l",))
    with pytest.raises(LoweringError):  # WHERE after ORDER/LIMIT (CX4)
        lower_query(_program(_tail((ordr, lim, whr2)), outputs=("w2",)), reg)
    sel2 = Binding(
        Op(OpKind.SELECT, {"project": {"z": col("b")}}), output="s2", inputs=("s",)
    )
    with pytest.raises(LoweringError):  # two post-RANK SELECTs (CX6)
        lower_query(_program(_tail((sel, sel2)), outputs=("s2",)), reg)

    # review F2/F3: rank_as==basis_as and malformed tiebreak fail validation
    assert "invalid_rank_components" in _validate_with(
        {"components": [{"name": "a", "expr": col("x")}],
         "rank_as": "x2", "basis_as": "x2"}
    )
    assert "invalid_rank_tiebreak" in _validate_with(
        {"components": [{"name": "a", "expr": col("x")}],
         "tiebreak": {"desc": True}}
    )

    assert "invalid_rank_components" in _validate_with({"components": []})
    assert "invalid_rank_components" in _validate_with(
        {"components": [{"name": "x", "expr": col("a")}, {"name": "x", "expr": col("b")}]}
    )
    assert "invalid_rank_components" in _validate_with(
        {"components": [{"name": "rnk", "expr": col("a")}], "rank_as": "rnk"}
    )


def test_program_checksum_unaffected_by_language_extension():
    """The RANK addition changes no existing program's identity: checksum()
    hashes only the program's own serialized dict (ir_version rides outside
    the hash). Golden digest pinned pre-extension — if serialization ever
    changes, this fails and the migration story must be written."""
    prog = _program((
        Binding(Op(OpKind.FROM, {"surface": "claims"}), output="c"),
        Binding(Op(OpKind.WHERE, {"expr": le(col("rnk"), lit(1))}), output="w", inputs=("c",)),
        Binding(Op(OpKind.EMIT, {"target": "out"}), inputs=("w",)),
    ), outputs=("w",))
    assert prog.checksum() == (
        "sha256:bf46446964dca966418a4453a0b67b59dd88fb67bd36bf8a1ae93efd96c83359"
    )
    # And a RANK-bearing program checksums deterministically.
    rank_prog = _program(_rank_bindings())
    assert rank_prog.checksum() == rank_prog.checksum()


@pytest.mark.postgres
@pytest.mark.skipif(not DSN, reason="OKG_TEST_DSN not set")
def test_rank_sql_survives_chronos_session_rewriter():
    """The chronos session executor round-trips every query through sqlglot
    (_rewrite_tables) — the layer that historically mangled `attrs -> :param`.
    The RANK lowering (window functions + nested subqueries + inline basis
    literals) must survive that round-trip byte-equivalently in RESULTS."""
    from chronos_core.branching._common import _rewrite_tables

    prog = _program(_rank_bindings())
    reg = _registry()
    sql, params = lower_query(prog, reg)
    rewritten = _rewrite_tables(sql, {"tmp_claims": "tmp_claims"}, "postgres")
    assert "ROW_NUMBER" in rewritten.upper()

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        _seed(cur, CLAIMS)
        cur.execute(f"SELECT claim_id, rnk, basis FROM (\n{sql}\n) q", params)
        direct = sorted(cur.fetchall())
        cur.execute(f"SELECT claim_id, rnk, basis FROM (\n{rewritten}\n) q", params)
        roundtripped = sorted(cur.fetchall())
        cur.execute("DROP TABLE tmp_claims")
    assert roundtripped == direct


def test_rank_forces_full_scope_and_unknown_ops_default_deny():
    prog = _program(_rank_bindings())
    plan = derive_frontier_plan(prog, check_attrs={})
    assert plan.mode == "full"
    assert plan.reason == "rank_over_other_rows"

    # review F5: the default-deny branch itself, witnessed with a UNION op
    # (locality-unanalyzed) — an untaught op must force full, never scope.
    union_prog = _program((
        Binding(Op(OpKind.FROM, {"surface": "claims"}), output="a"),
        Binding(Op(OpKind.FROM, {"surface": "claims"}), output="b"),
        Binding(Op(OpKind.UNION, {}), output="u", inputs=("a", "b")),
        Binding(Op(OpKind.EMIT, {"target": "out"}), inputs=("u",)),
    ), outputs=("u",))
    plan2 = derive_frontier_plan(union_prog, check_attrs={})
    assert plan2.mode == "full"
    assert plan2.reason == "unrecognized_op:union"
