"""Bucket-level corroboration: the engine computes ``independent_support``.

Corroboration is the one ``≻`` signal no producer can supply — it is a
property of the BUCKET, which is formed downstream of extraction over
``C_eff``, so any producer-side count is stale by construction. Until this
change the derivation read ``COALESCE(attrs->>'independent_support', 0)``,
which is 0 in every production bucket, so the corroboration tier could never
settle anything.

These witnesses pin three things:

1. the support computation itself (distinct sources per (bucket, value));
2. the SQL/interpreter differential — the interpreter-only fuzz cannot catch a
   count-distinct mis-compile, because it never lowers;
3. **reachability**, which is where the interesting result lives. Computing
   the signal was necessary but NOT sufficient. ``≻`` ranked CLAIMS, and a
   value with support *s* is carried by *s* claims that tie with each other
   on the corroboration component; those siblings occupied both top rank
   slots, so the rank-1/rank-2 comparison tied before the rival value was
   ever reached. Measured on a real 9090-claim corpus: 398 contested buckets,
   20 carrying a strict majority, and 0 settled.

   The basis now compares the leader against the best claim asserting a
   DIFFERENT value — the RANK op's ``peer`` group, mirrored by
   ``ClaimSignals.value`` in the library. ``test_bare_majority_settles_on_
   corroboration`` pins the shape that now settles,
   ``test_a_one_to_one_split_still_stays_contested`` pins the counter-case so
   the fix cannot be mistaken for "always settle", and
   ``test_unvalued_buckets_keep_the_legacy_comparison`` pins that callers
   supplying no values are unaffected.
"""

from __future__ import annotations

import json
import os
from typing import Any

import pytest

from okg.substrate.derived_artifacts.claim_effective_derivation import (
    CLAIM_EFFECTIVE_TABLE,
)
from okg.substrate.derived_artifacts.claim_projection_derivation import (
    DECLARED_ORIGIN_ATTR,
    GRAPH_NODES_TABLE,
    ROW_KIND_RESOLUTION,
    _support_binding,
    compute_rows_interpreted,
    computed_support,
    library_truth_rows,
    rows_as_truth,
    support_surface_rows,
)
from okg.substrate.errors import SubstrateError

psycopg = pytest.importorskip("psycopg")

DSN = os.environ.get("OKG_TEST_DSN")


def _eff(cid: str, value: str, source: str, *, pred: str = "p_func",
         key: str = "k1") -> dict[str, Any]:
    return {"claim_id": cid, "predicate": pred, "normalized_key": key,
            "value": value, "scope": "", "scope_state": "known",
            "source_id": source}


def _policy() -> dict[str, Any]:
    return {"node_id": "pol:f1", "attrs": {
        "policy_id": "c_func", "policy_kind": "functional",
        "predicate": "p_func", "enabled": "true"}}


def _support(effective: list[dict[str, Any]],
             claims: list[dict[str, Any]] | None = None) -> dict[str, int]:
    return {r["sp_value"]: r["sp_support"] for r in support_surface_rows(
        effective, claims or [])}


def _origin_claims(origin_by_claim: dict[str, str]) -> list[dict[str, Any]]:
    """Claim nodes declaring an origin (``provenance_group``)."""
    return [{"node_id": cid, "attrs": {DECLARED_ORIGIN_ATTR: origin}}
            for cid, origin in origin_by_claim.items()]


def _resolution(effective: list[dict[str, Any]],
                claims: list[dict[str, Any]]) -> dict[str, Any]:
    rows = compute_rows_interpreted(effective, claims, [_policy()], [])
    res = [v for k, v in rows.items() if k[0] == ROW_KIND_RESOLUTION]
    assert len(res) == 1, res
    # The GATING parity oracle must agree — a lockstep miss here blocks every
    # publish, so the same corpus is checked against the library restatement.
    assert rows_as_truth(rows) == library_truth_rows(
        effective, claims, [_policy()], [])
    return res[0]


def _bare(effective: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"node_id": r["claim_id"], "attrs": {}} for r in effective]


# --------------------------------------------------------------------------- #
# 1. the count itself                                                          #
# --------------------------------------------------------------------------- #
def test_support_counts_distinct_sources_within_a_bucket() -> None:
    """Three distinct sources score 3; a value asserted once scores 1."""
    effective = [
        _eff("claim:a1", "v1", "page:1"),
        _eff("claim:a2", "v1", "page:2"),
        _eff("claim:a3", "v1", "page:3"),
        _eff("claim:b1", "v2", "page:4"),
    ]
    assert _support(effective) == {"v1": 3, "v2": 1}


def test_same_document_restatements_score_one() -> None:
    """Restating a value twice in ONE document is not corroboration — an
    internal-locality conflict must stay contested rather than be settled by
    a source repeating itself."""
    effective = [
        _eff("claim:a1", "v1", "page:1"),
        _eff("claim:a2", "v1", "page:1"),
        _eff("claim:b1", "v2", "page:2"),
    ]
    assert _support(effective) == {"v1": 1, "v2": 1}
    assert _resolution(effective, _bare(effective))["outcome"] == "contested"


def test_values_compare_by_raw_string_equality() -> None:
    """Decision D3: canonicalization is an extraction-time responsibility;
    there is no bucket-level value normalizer, so 'v1' and 'V1' are distinct
    values, not corroborating restatements."""
    effective = [
        _eff("claim:a1", "v1", "page:1"),
        _eff("claim:a2", "V1", "page:2"),
    ]
    assert _support(effective) == {"v1": 1, "V1": 1}


def test_support_is_scoped_to_the_bucket() -> None:
    """The same value under a DIFFERENT key does not corroborate."""
    effective = [
        _eff("claim:a1", "v1", "page:1"),
        _eff("claim:a2", "v1", "page:2", key="k2"),
    ]
    assert {(r["sp_key"], r["sp_value"]): r["sp_support"]
            for r in support_surface_rows(effective)} == {
        ("k1", "v1"): 1, ("k2", "v1"): 1}


def test_unsourced_claim_in_a_conflicted_bucket_raises() -> None:
    """Negative control for design decision 8. ``source_id`` is the
    distinctness key, so every unsourced claim folds to the same empty key: a
    value asserted by three unsourced documents would score 1 and corroboration
    would be silently disabled exactly where it was meant to decide. Capping at
    1 quietly is a default-value-as-fallback, so it must be loud."""
    effective = [
        _eff("claim:a1", "v1", "page:1"),
        _eff("claim:a2", "v1", ""),
        _eff("claim:b1", "v2", "page:2"),
    ]
    with pytest.raises(SubstrateError, match="no source_id"):
        compute_rows_interpreted(effective, _bare(effective), [_policy()], [])


def test_unsourced_claim_outside_a_conflict_still_publishes() -> None:
    """The check is scoped to conflicted buckets: corroboration decides nothing
    in a bucket with one value, so deployments that never populate source_id
    keep publishing rather than being blocked by a signal they do not use."""
    effective = [
        _eff("claim:a1", "v1", ""),
        _eff("claim:a2", "v1", ""),
    ]
    compute_rows_interpreted(effective, _bare(effective), [_policy()], [])


def test_null_coordinate_raises_before_any_join_can_miss() -> None:
    """The support surface joins on (predicate, normalized_key, value) WITHOUT
    coalescing, so a NULL coordinate would miss the join in postgres only
    (``NULL = NULL`` is NULL) while the interpreter's ``==`` matched — and
    ``COALESCE(sp_support, 0)`` would then silently report support 0.

    That divergence is unreachable because the coordinates are validated first,
    on BOTH entry points. This pins the invariant the join relies on, so it
    cannot be weakened without a failure here. (Coalescing the binding instead
    is not free: it feeds every finding program, so folding NULL to '' would
    change conflict detection, not just the support join.)
    """
    effective = [
        _eff("claim:a1", "v1", "page:1"),
        {"claim_id": "claim:a2", "predicate": "p_func",
         "normalized_key": "k1", "value": None, "scope": "",
         "scope_state": "known", "source_id": "page:2"},
    ]
    with pytest.raises(SubstrateError, match="missing required coordinate"):
        compute_rows_interpreted(effective, _bare(effective), [_policy()], [])


def test_support_is_deterministic_across_runs() -> None:
    effective = [
        _eff("claim:a1", "v1", "page:1"),
        _eff("claim:a2", "v1", "page:2"),
        _eff("claim:b1", "v2", "page:3"),
    ]
    assert support_surface_rows(effective) == support_surface_rows(effective)


# --------------------------------------------------------------------------- #
# 1b. DECLARED ORIGIN — support counts origins, not copies                     #
#                                                                              #
# PACT consistency-independent-support. Distinct source_id counts distinct     #
# DOCUMENT PATHS, so a value echoed into three documents from one origin       #
# scored 3, identical to three genuinely independent sources. A claim MAY      #
# declare its origin; a claim that does not IS its own origin (its document    #
# identity) — a declared identity rule, which is what keeps every              #
# declare-nothing corpus at exactly its previous count.                        #
# --------------------------------------------------------------------------- #
def test_three_documents_one_declared_origin_score_one() -> None:
    """req.support.origin ACCEPTANCE: three documents echoing ONE origin are
    evidence of copying, not of truth."""
    effective = [
        _eff("claim:a1", "v1", "page:1"),
        _eff("claim:a2", "v1", "page:2"),
        _eff("claim:a3", "v1", "page:3"),
        _eff("claim:b1", "v2", "page:4"),
    ]
    claims = _origin_claims({
        "claim:a1": "wire:reuters-1",
        "claim:a2": "wire:reuters-1",
        "claim:a3": "wire:reuters-1",
    })
    assert _support(effective, claims) == {"v1": 1, "v2": 1}


def test_three_distinct_declared_origins_score_three() -> None:
    """req.support.origin ACCEPTANCE, the counter-case: three genuinely
    independent origins are still worth 3. Byte-identical corpus to the test
    above except for the declared origins, so the two together pin that the
    DECLARATION is what moves the count — not the document count, which is 3
    in both."""
    effective = [
        _eff("claim:a1", "v1", "page:1"),
        _eff("claim:a2", "v1", "page:2"),
        _eff("claim:a3", "v1", "page:3"),
        _eff("claim:b1", "v2", "page:4"),
    ]
    claims = _origin_claims({
        "claim:a1": "wire:reuters-1",
        "claim:a2": "lab:mit-2",
        "claim:a3": "registry:fda-3",
    })
    assert _support(effective, claims) == {"v1": 3, "v2": 1}


def test_declaring_nothing_scores_the_document_count_unchanged() -> None:
    """The declared identity rule, stated as the property that makes this
    change behaviour-preserving: with NO declarations anywhere, support is
    COUNT(DISTINCT source_id) — restated here literally, not by calling the
    implementation — over a corpus with duplicate sources, empty sources, and
    several buckets."""
    import random

    rng = random.Random(20260724)
    effective = [
        _eff(f"claim:{i}", f"v{rng.randint(1, 3)}", f"page:{rng.randint(1, 4)}",
             key=f"k{rng.randint(1, 3)}")
        for i in range(120)
    ]
    pre_change: dict[tuple[Any, Any, Any], set[str]] = {}
    for r in effective:
        pre_change.setdefault(
            (r["predicate"], r["normalized_key"], r["value"]), set()
        ).add(r["source_id"] or "")

    got = {(r["sp_predicate"], r["sp_key"], r["sp_value"]): r["sp_support"]
           for r in support_surface_rows(effective)}
    assert got == {k: len(v) for k, v in pre_change.items()}
    # non-vacuity: some bucket really does have more claims than sources, so a
    # plain row COUNT would differ from the count this pins.
    assert any(len(v) > 1 for v in pre_change.values())
    assert len(effective) > len(got)


def test_an_empty_declaration_declares_nothing() -> None:
    """``provenance_group = ''`` is not an origin. Folding it to a shared
    empty key would collapse a whole bucket to support 1 — corroboration
    silently disabled, the default-value-as-fallback this repo treats as P0."""
    effective = [
        _eff("claim:a1", "v1", "page:1"),
        _eff("claim:a2", "v1", "page:2"),
    ]
    claims = _origin_claims({"claim:a1": "", "claim:a2": ""})
    assert _support(effective, claims) == {"v1": 2}


def test_a_claim_with_no_node_keeps_its_document_identity() -> None:
    """Operator-ASSERTED effective rows have no claim node at all (module
    docstring). The LEFT join must leave them counting as their own origin
    rather than dropping them or folding them together."""
    effective = [
        _eff("claim:a1", "v1", "page:1"),
        _eff("claim:a2", "v1", "page:2"),
    ]
    assert _support(effective, _origin_claims({"claim:a1": "wire:x"})) == {"v1": 2}


def test_a_copy_majority_no_longer_settles_the_bucket() -> None:
    """The result that matters, at the engine level: the shape the benchmark
    measured. Two corrupted renderings echoing ONE origin used to beat the
    single truthful counterpart on dependency_adjusted_support; now they are
    one origin against one origin and the bucket stays contested."""
    effective = [
        _eff("claim:c1", "v1", "page:1"),
        _eff("claim:c2", "v1", "page:2"),
        _eff("claim:c3", "v2", "page:3"),
    ]
    claims = _origin_claims({"claim:c1": "pg:1", "claim:c2": "pg:1"})
    assert _support(effective, claims) == {"v1": 1, "v2": 1}

    row = _resolution(effective, claims)
    assert row["outcome"] == "contested"
    assert row["basis"] == "tie"


def test_an_independent_majority_still_settles() -> None:
    """The counter-case the PACT requires to survive: a majority of GENUINELY
    independent sources is still better evidence and must still settle. Same
    corpus as the copy case, distinct declared origins."""
    effective = [
        _eff("claim:c1", "v1", "page:1"),
        _eff("claim:c2", "v1", "page:2"),
        _eff("claim:c3", "v2", "page:3"),
    ]
    claims = _origin_claims({"claim:c1": "pg:1:a", "claim:c2": "pg:1:b"})
    assert _support(effective, claims) == {"v1": 2, "v2": 1}

    row = _resolution(effective, claims)
    assert row["outcome"] == "canonical"
    assert row["basis"] == "dependency_adjusted_support"
    assert row["winner_claim_id"] in {"claim:c1", "claim:c2"}


# --------------------------------------------------------------------------- #
# 2. the readers move in lockstep, and the oracle restates independently       #
# --------------------------------------------------------------------------- #
def test_oracle_restatement_agrees_with_the_surface_mirror() -> None:
    """``computed_support`` (oracle) and ``support_surface_rows``
    (interpreter surface) are deliberately independent statements of one rule.
    They must agree — and the next test proves the agreement is not vacuous."""
    effective = [
        _eff("claim:a1", "v1", "page:1"),
        _eff("claim:a2", "v1", "page:2"),
        _eff("claim:b1", "v2", "page:3"),
    ]
    from okg.substrate.derived_artifacts.claim_projection_derivation import (
        _view_of,
    )
    oracle = computed_support([_view_of(r) for r in effective])
    surface = _support(effective)
    assert oracle == {"claim:a1": 2, "claim:a2": 2, "claim:b1": 1}
    assert {c: surface[r["value"]]
            for c, r in zip(oracle, effective)} == oracle


def test_oracle_detects_a_derivation_side_error_rather_than_mirroring_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Negative control for the discipline in §4 of the design: if the oracle
    reached for the derivation's helper it would agree with the derivation by
    construction, going vacuous exactly where the new logic lives. Corrupt the
    interpreter's surface and the oracle must DIVERGE.

    The corruption must be LOAD-BEARING, and the fixture must be one where
    support actually separates. Inflating every count by a constant preserves
    the ordering, so no reader could detect it and the control would prove
    nothing; and in a bucket whose siblings tie, support changes no outcome at
    all. So: the demoted-sibling shape, with the counts inverted so the rival
    value would win.
    """
    effective = [
        _eff("claim:c1", "v1", "page:1"),
        _eff("claim:c2", "v1", "page:2"),
        _eff("claim:c3", "v2", "page:3"),
    ]
    dates = {"claim:c1": "2026-01-01",
             "claim:c3": "2026-01-01",
             "claim:c2": "2020-01-01"}
    claims = [{"node_id": r["claim_id"],
               "attrs": {"effective_date": dates[r["claim_id"]]}}
              for r in effective]

    def _wrong(rows, nodes=()):  # invert the ordering the ranking depends on
        return [dict(r, sp_support=-r["sp_support"])
                for r in support_surface_rows(rows, nodes)]

    monkeypatch.setattr(
        "okg.substrate.derived_artifacts.claim_projection_derivation.support_surface_rows",
        _wrong,
    )
    derived = rows_as_truth(
        compute_rows_interpreted(effective, claims, [_policy()], []))
    truth = library_truth_rows(effective, claims, [_policy()], [])
    assert derived != truth, (
        "the oracle mirrored a deliberate derivation-side error instead of "
        "detecting it — the parity check is vacuous"
    )


def test_oracle_detects_a_divergent_ORIGIN_restatement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The same negative control aimed at the NEW logic (req.support.origin
    ACCEPTANCE: "a deliberately divergent restatement fails it").

    The control above corrupts the support VALUE, which any earlier version of
    this rule would have caught; it says nothing about whether the two
    restatements of the DECLARED-ORIGIN rule are still independent. Here the
    interpreter mirror is made to ignore the declaration and fall back to the
    old document count, which is exactly the divergence a shared helper would
    have hidden. The oracle must DIVERGE.

    The fixture is the copy-majority shape, where ignoring the declaration
    changes the OUTCOME (contested -> canonical), so the divergence is
    load-bearing rather than a cosmetic count difference.
    """
    effective = [
        _eff("claim:c1", "v1", "page:1"),
        _eff("claim:c2", "v1", "page:2"),
        _eff("claim:c3", "v2", "page:3"),
    ]
    claims = _origin_claims({"claim:c1": "pg:1", "claim:c2": "pg:1"})

    def _ignores_the_declaration(rows, nodes=()):
        return support_surface_rows(rows)  # declarations dropped on the floor

    # non-vacuity of the control itself: the two restatements DO agree first.
    assert rows_as_truth(
        compute_rows_interpreted(effective, claims, [_policy()], [])
    ) == library_truth_rows(effective, claims, [_policy()], [])

    monkeypatch.setattr(
        "okg.substrate.derived_artifacts.claim_projection_derivation.support_surface_rows",
        _ignores_the_declaration,
    )
    derived = rows_as_truth(
        compute_rows_interpreted(effective, claims, [_policy()], []))
    truth = library_truth_rows(effective, claims, [_policy()], [])
    assert derived != truth, (
        "the oracle mirrored an interpreter restatement that ignores the "
        "declared origin — the two are no longer independent statements of "
        "the origin rule, so the parity check is vacuous where the new logic "
        "lives"
    )


# --------------------------------------------------------------------------- #
# 2b. SQL / interpreter differential on the support surface                    #
#                                                                              #
# The 200-case projection fuzz never lowers, so it cannot catch the SQL        #
# surface and the interpreter mirror disagreeing — exactly where a            #
# count-distinct mis-compile would live. This is the only witness that runs    #
# the surface's real SQL text.                                                 #
# --------------------------------------------------------------------------- #
# (claim_id, predicate, normalized_key, value, source_id, declared origin).
# ``None`` origin = the claim node exists but declares nothing; a claim id
# absent from _SQL_ORIGIN_NODES has no node at all.
_SQL_FIXTURE = [
    # duplicate (bucket, value, source): separates count from count_distinct
    ("c1", "p", "k1", "v1", "s1", None),
    ("c2", "p", "k1", "v1", "s1", None),
    ("c3", "p", "k1", "v1", "s2", None),
    # rival value in the same bucket
    ("c4", "p", "k1", "v2", "s3", None),
    # case variant: raw string equality, NOT a corroborating restatement
    ("c5", "p", "k1", "V1", "s4", None),
    # NULL source_id: COALESCE(source_id,'') vs Python `or ""`. Legal HERE
    # because k2 is unconflicted (one value); inside a conflicted bucket the
    # validator raises — see test_unsourced_claim_in_a_conflicted_bucket_raises.
    ("c6", "p", "k2", "v9", None, None),
    ("c7", "p", "k2", "v9", "s5", None),
    # DECLARED ORIGIN: three documents, one origin -> 1 (SQL NULLIF/COALESCE
    # chain vs the mirror's `declared or source_id`)
    ("c8", "p", "k3", "vc", "s6", "org:one"),
    ("c9", "p", "k3", "vc", "s7", "org:one"),
    ("c10", "p", "k3", "vc", "s8", "org:one"),
    # three documents, three origins -> 3
    ("c11", "p", "k4", "vi", "s6", "org:a"),
    ("c12", "p", "k4", "vi", "s7", "org:b"),
    ("c13", "p", "k4", "vi", "s8", "org:c"),
    # an EMPTY declaration declares nothing: NULLIF('','') is NULL on the SQL
    # side, `if origin` is False on the mirror's — both fall back to source_id
    ("c14", "p", "k5", "ve", "s6", ""),
    ("c15", "p", "k5", "ve", "s7", ""),
    # a declared origin that EQUALS another claim's document identity merges
    # with it: origins and documents share one namespace, deliberately
    ("c16", "p", "k6", "vn", "s9", None),
    ("c17", "p", "k6", "vn", "s10", "s9"),
    # a claim with NO node at all (operator-asserted row) keeps its document
    # identity through the LEFT join rather than vanishing
    ("c18", "p", "k7", "vx", "s11", None),
]
# every fixture claim EXCEPT c18 gets a node; a None origin means the node
# carries other attrs but declares no origin (the production shape).
_SQL_ORIGIN_NODES = [
    (cid, json.dumps({"source_class": "x"} if origin is None
                     else {"source_class": "x",
                           DECLARED_ORIGIN_ATTR: origin}))
    for cid, _p, _k, _v, _s, origin in _SQL_FIXTURE
    if cid != "c18"
]


def _sql_fixture_rows() -> list[dict[str, Any]]:
    return [{"claim_id": c, "predicate": p, "normalized_key": k, "value": v,
             "source_id": s, "row_kind": "effective_claim"}
            for c, p, k, v, s, _o in _SQL_FIXTURE]


def _sql_fixture_claims() -> list[dict[str, Any]]:
    return [{"node_id": cid, "attrs": json.loads(attrs)}
            for cid, attrs in _SQL_ORIGIN_NODES]


def _sql_fixture_support() -> dict[tuple[str, str], int]:
    return {(r["sp_key"], r["sp_value"]): r["sp_support"]
            for r in support_surface_rows(
                _sql_fixture_rows(), _sql_fixture_claims())}


def test_sql_fixture_discriminates_count_from_count_distinct() -> None:
    """Non-vacuity: without the duplicate source row the differential would
    pass against a plain COUNT, proving nothing — and without the declared
    origins it would pass against the pre-change document count."""
    got = _sql_fixture_support()
    assert got[("k1", "v1")] == 2, "v1 has 3 rows but only 2 distinct sources"
    assert got[("k1", "V1")] == 1, "case variants must not merge"
    assert got[("k2", "v9")] == 2, "NULL source_id folds to '' on both sides"
    assert got[("k3", "vc")] == 1, "3 documents, 1 declared origin"
    assert got[("k4", "vi")] == 3, "3 documents, 3 declared origins"
    assert got[("k5", "ve")] == 2, "an empty declaration declares nothing"
    assert got[("k6", "vn")] == 1, "origins and documents share one namespace"
    assert got[("k7", "vx")] == 1, "a claim with no node keeps its document"


@pytest.mark.postgres
@pytest.mark.skipif(not DSN, reason="OKG_TEST_DSN not set")
def test_support_surface_sql_equals_interpreter_mirror() -> None:
    sql = _support_binding()
    assert CLAIM_EFFECTIVE_TABLE in sql, (
        "the binding no longer names the effective-claims table; this test's "
        "substitution below would silently target nothing"
    )
    assert GRAPH_NODES_TABLE in sql, (
        "the binding no longer names the graph-node table, so it cannot be "
        "reading the DECLARED origin; this test's substitution below would "
        "silently target nothing and the differential would go vacuous"
    )
    sql = sql.replace(CLAIM_EFFECTIVE_TABLE, "tmp_effective")
    sql = sql.replace(GRAPH_NODES_TABLE, "tmp_nodes")

    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "CREATE TEMP TABLE tmp_effective ("
            "row_key text, predicate text, normalized_key text, value text, "
            "source_id text, row_kind text)"
        )
        cur.execute(
            "CREATE TEMP TABLE tmp_nodes ("
            "node_id text, subtype text, attrs jsonb)"
        )
        cur.executemany(
            "INSERT INTO tmp_effective VALUES (%s, %s, %s, %s, %s, "
            "'effective_claim')",
            [(c, p, k, v, s) for c, p, k, v, s, _o in _SQL_FIXTURE],
        )
        cur.executemany(
            "INSERT INTO tmp_nodes VALUES (%s, 'consistency_claim', %s)",
            _SQL_ORIGIN_NODES,
        )
        cur.execute(
            f"SELECT sp_predicate, sp_key, sp_value, sp_support FROM {sql} q"
        )
        from_sql = {(r[0], r[1], r[2]): r[3] for r in cur.fetchall()}
        cur.execute("DROP TABLE tmp_effective")
        cur.execute("DROP TABLE tmp_nodes")

    from_interp = {
        (r["sp_predicate"], r["sp_key"], r["sp_value"]): r["sp_support"]
        for r in support_surface_rows(
            _sql_fixture_rows(), _sql_fixture_claims())
    }
    assert from_sql == from_interp, (
        f"support surface SQL disagrees with the interpreter mirror: "
        f"{from_sql} vs {from_interp}"
    )


@pytest.mark.postgres
@pytest.mark.skipif(not DSN, reason="OKG_TEST_DSN not set")
def test_the_sql_differential_still_detects_a_divergent_binding() -> None:
    """Non-vacuity of the differential itself: retarget it at a binding whose
    origin expression is deliberately wrong (the pre-change document count) and
    it must FAIL. Without this, a binding that silently stopped reading the
    declaration would pass the test above the day the mirror stopped too."""
    sql = (
        "(SELECT e.predicate AS sp_predicate, e.normalized_key AS sp_key, "
        "e.value AS sp_value, "
        "COUNT(DISTINCT COALESCE(e.source_id, '')) AS sp_support "
        "FROM tmp_effective e GROUP BY e.predicate, e.normalized_key, e.value)"
    )
    with psycopg.connect(DSN, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "CREATE TEMP TABLE tmp_effective ("
            "row_key text, predicate text, normalized_key text, value text, "
            "source_id text, row_kind text)"
        )
        cur.executemany(
            "INSERT INTO tmp_effective VALUES (%s, %s, %s, %s, %s, "
            "'effective_claim')",
            [(c, p, k, v, s) for c, p, k, v, s, _o in _SQL_FIXTURE],
        )
        cur.execute(
            f"SELECT sp_predicate, sp_key, sp_value, sp_support FROM {sql} q"
        )
        from_sql = {(r[1], r[2]): r[3] for r in cur.fetchall()}
        cur.execute("DROP TABLE tmp_effective")

    assert from_sql != _sql_fixture_support(), (
        "the pre-change document count agrees with the interpreter mirror on "
        "this fixture, so the differential cannot see the origin rule at all"
    )


# --------------------------------------------------------------------------- #
# 3. reachability — the result that matters                                    #
# --------------------------------------------------------------------------- #
def test_corroboration_separates_when_a_sibling_is_demoted() -> None:
    """The shape in which corroboration DOES settle a bucket.

    ``≻`` compares rank-1 against rank-2. For corroboration to be the
    separating component those two claims must assert different values and
    tie on every earlier signal. Here v1's second claim (``c2``) is demoted
    below the rival by an earlier signal (effective_date), freeing rank-2 for
    the v2 claim — so the top pair ties on date and differs on support.
    """
    effective = [
        _eff("claim:c1", "v1", "page:1"),
        _eff("claim:c2", "v1", "page:2"),
        _eff("claim:c3", "v2", "page:3"),
    ]
    dates = {"claim:c1": "2026-01-01",
             "claim:c3": "2026-01-01",
             "claim:c2": "2020-01-01"}
    claims = [{"node_id": r["claim_id"],
               "attrs": {"effective_date": dates[r["claim_id"]]}}
              for r in effective]

    row = _resolution(effective, claims)
    assert row["outcome"] == "canonical"
    assert row["basis"] == "dependency_adjusted_support"
    assert row["winner_claim_id"] == "claim:c1"


def test_bare_majority_settles_on_corroboration() -> None:
    """A 2-vs-1 majority with every other signal absent — the shape real
    extracted corpora take, since producers populate no precedence attrs.

    This case is the whole point of the change, and it did NOT work when
    corroboration was merely computed. Support ordered the claims correctly,
    but v1's two claims tied with each other on every component, so they
    occupied rank-1 and rank-2 and the basis comparison tied before the rival
    value was ever reached: 398 contested buckets on a real 9090-claim corpus,
    20 of them carrying a strict majority, and 0 settled.

    The basis now compares the leader against the best claim asserting a
    DIFFERENT value (the RANK op's ``peer``), so the majority wins.
    """
    effective = [
        _eff("claim:c1", "v1", "page:1"),
        _eff("claim:c2", "v1", "page:2"),
        _eff("claim:c3", "v2", "page:3"),
    ]
    assert _support(effective) == {"v1": 2, "v2": 1}

    row = _resolution(effective, _bare(effective))
    assert row["outcome"] == "canonical"
    assert row["basis"] == "dependency_adjusted_support"
    # the winner asserts the corroborated value, and the minority sorts last
    assert row["winner_claim_id"] in {"claim:c1", "claim:c2"}
    assert row["ranked_claim_ids"].endswith('"claim:c3"]')


def test_corroborators_are_not_superseded_by_the_value_they_support() -> None:
    """A bucket settled BY corroboration must not declare its own corroborators
    superseded.

    Found by adversarial review. The basis comparison moved to value level but
    the loser set stayed at "everything below rank 1", so the sibling that
    PRODUCED the majority was recorded as superseded and queued for retraction.
    Three harms: the persisted row says c1 was superseded by a claim it agrees
    with; the repair deletes the evidence that settled the bucket; and support
    for the winning value then drops 2 -> 1, so the next cycle re-opens the
    conflict the repair just closed.
    """
    effective = [
        _eff("claim:c1", "v1", "page:1"),
        _eff("claim:c2", "v1", "page:2"),
        _eff("claim:c3", "v2", "page:3"),
    ]
    row = _resolution(effective, _bare(effective))
    assert row["outcome"] == "canonical"
    winner = row["winner_claim_id"]
    superseded = json.loads(row["superseded_claim_ids"])

    # only the rival VALUE was beaten
    assert superseded == ["claim:c3"], superseded
    assert winner not in superseded
    # the corroborating sibling is neither the winner nor superseded
    sibling = {"claim:c1", "claim:c2"} - {winner}
    assert not (sibling & set(superseded)), (
        f"{sibling} asserts the winning value and corroborates it; recording "
        "it as superseded would retract the evidence that settled the bucket"
    )


def test_repair_does_not_retract_the_winning_value_s_corroborators() -> None:
    """The same defect one layer down, where it becomes a persisted correction:
    ``select_repairs`` must not propose retracting a claim that asserts the
    winning value."""
    from okg.substrate.consistency.claim_model.selection import select_repairs
    from okg.substrate.derived_artifacts.claim_projection_derivation import (
        build_conflict_buckets,
    )

    effective = [
        _eff("claim:c1", "v1", "page:1"),
        _eff("claim:c2", "v1", "page:2"),
        _eff("claim:c3", "v2", "page:3"),
    ]
    value_of = {r["claim_id"]: r["value"] for r in effective}
    buckets, precedents = build_conflict_buckets(
        effective, _bare(effective), [_policy()], [])
    selected = select_repairs(buckets, precedents)

    retracted = {
        c.claim_id
        for s in selected.selected for c in s.corrections
        if c.action == "retract"
    }
    assert retracted, "expected the settled bucket to propose a repair"
    kept_values = {value_of[cid] for cid in retracted}
    assert kept_values == {"v2"}, (
        f"repair retracts {sorted(retracted)} = values {sorted(kept_values)}; "
        "only claims asserting a value OTHER than the winner's may be retracted"
    )
    distrusted = set(selected.distrusted_claim_ids)
    assert {value_of[cid] for cid in distrusted} == {"v2"}, (
        f"distrusted={sorted(distrusted)} includes a claim asserting the "
        "winning value v1")


def test_a_one_to_one_split_still_stays_contested() -> None:
    """The counter-case, so the fix cannot be mistaken for "always settle".
    With one source per value there is no majority to find, and 378 of the
    real corpus's 398 contested buckets are exactly this shape."""
    effective = [
        _eff("claim:c1", "v1", "page:1"),
        _eff("claim:c2", "v2", "page:2"),
    ]
    assert _support(effective) == {"v1": 1, "v2": 1}

    row = _resolution(effective, _bare(effective))
    assert row["outcome"] == "contested"
    assert row["basis"] == "tie"


def test_unanimous_bucket_is_canonical_not_tied() -> None:
    """Claims that all assert one value restate each other, so there is no
    rival to compare against. Comparing the leader to a restatement would
    report a tie — the exact confusion this change removes.

    Exercised at the LIBRARY level: a functional policy only fires on two
    DIFFERENT values, so the derivation never builds a unanimous bucket. The
    branch is defensive there but reachable for direct callers, and
    ``resolve_bucket`` is a total function on any bucket it is handed.
    """
    from okg.substrate.consistency.claim_model.precedence import (
        ClaimSignals,
        resolve_bucket,
    )

    got = resolve_bucket([
        ClaimSignals(claim_id="claim:c1", value="v1"),
        ClaimSignals(claim_id="claim:c2", value="v1"),
    ])
    assert got.outcome == "canonical"
    assert got.basis == "sole_claim"
    assert got.canonical_claim_id == "claim:c2"  # ranked by the claim_id tiebreak


def test_partially_valued_bucket_is_rejected_not_guessed() -> None:
    """Mixing valued and unvalued claims has no well-defined peer grouping.
    Defaulting the unvalued ones to a shared empty value would silently merge
    distinct claims into one peer group and suppress a real conflict, so it
    must raise rather than guess."""
    from okg.substrate.consistency.claim_model.precedence import (
        ClaimSignals,
        resolve_bucket,
    )

    with pytest.raises(ValueError, match="some claims but not all"):
        resolve_bucket([
            ClaimSignals(claim_id="claim:c1", value="v1"),
            ClaimSignals(claim_id="claim:c2"),
        ])


def test_unvalued_buckets_keep_the_legacy_comparison() -> None:
    """Callers that supply no values at all — including the vendored adoption
    contract — keep the historical claim-adjacent comparison unchanged."""
    from okg.substrate.consistency.claim_model.precedence import (
        ClaimSignals,
        resolve_bucket,
    )

    got = resolve_bucket([
        ClaimSignals(claim_id="claim:c1", source_authority=2),
        ClaimSignals(claim_id="claim:c2", source_authority=1),
    ])
    assert got.outcome == "canonical"
    assert got.basis == "declared_source_authority"
    assert got.canonical_claim_id == "claim:c1"
