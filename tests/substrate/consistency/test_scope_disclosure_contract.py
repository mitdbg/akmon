"""The scope disclosure a scoped run persists must be USABLE, not decorative.

Three defects this pins, each of which shipped a field that reads as an answer
while being wrong or uncheckable:

1. The complement recipe ("unevaluated = universe MINUS these ids") was emitted
   unconditionally. Above ``SCOPE_ID_LIMIT`` the id list is CAPPED, so a reader
   following the recipe computes an "unevaluated" set containing thousands of
   subjects the gate DID evaluate. The SQL gate additionally emitted it on
   UNSCOPED runs, where the subtraction says the entire universe went
   unevaluated on a run that swept all of it -- the disclosure inverted.

2. The scope digest was computed two different ways (Python codepoint sort vs a
   collation-dependent SQL ``ORDER BY``) and the algorithm was recorded nowhere,
   so nobody could recompute it and the two paths could disagree on the same set.

3. ``verdict_universe_valid_by_induction`` was a hardcoded ``True`` next to prose
   asserting the previous generation swept the full graph. Nothing measured that.
"""
from __future__ import annotations

import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.consistency import TransformConsistencyGate  # noqa: E402
from okg.substrate.consistency import observations  # noqa: E402
from okg.substrate.consistency.observations import (  # noqa: E402
    SCOPE_DIGEST_ALGORITHM,
    SCOPE_ID_LIMIT,
    scope_complement_recipe,
    scope_digest,
    scope_digest_sql,
    scope_identity,
)
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402

pytestmark = pytest.mark.postgres

# The same synthetic token the publisher passes (int(time.time_ns() // 1000)):
# never a real Chronos checkpoint, so the gate falls back to the branch head.
_SYN_GEN = 9_000_000_000_002

# Ids chosen to make every failure mode of a text sort observable: mixed case
# (where `en_US.UTF-8` sorts case-insensitively but `C` does not), punctuation
# and underscores (which `en_US.UTF-8` can ignore entirely at the primary
# level), and a digit-vs-letter boundary.
_COLLATION_HOSTILE_IDS = (
    "Zebra",
    "apple",
    "Apple",
    "_leading",
    "a-dash",
    "a_underscore",
    "A.dot",
    "10",
    "2",
    "zebra:1",
    "ZEBRA:1",
)

# Non-C collations to stand in for a deployment whose database default is one,
# in preference order. `en_US.utf8` is the libc name on the dev/CI image;
# `*-x-icu` are the ICU-provider names PostgreSQL ships when built with ICU.
_NON_C_COLLATIONS = ("en_US.utf8", "en-US-x-icu", "und-x-icu")


def _setup_one_edge_delta(conn) -> tuple[int, list[str], list[int]]:
    """One node + one edge written to `default`, with no publish and therefore no
    prior gate row. Returns (catalog_version_id, node_ids, edge_ids)."""

    sfx = uuid.uuid4().hex[:8]
    src, dst = f"t:{sfx}:src", f"p:{sfx}:dst"
    cv = seed_catalog(
        conn,
        subtypes=["ticket", "person"],
        narrowings=[("ticket", "mentions", "person")],
    )
    store = ChronosGraphStore.from_connection(conn)
    store.apply_facts(
        [
            NodeFact(dst, "person", {}, {"id": dst}, {"v": 1}),
            NodeFact(src, "ticket", {}, {"id": src}, {"v": 1}),
            EdgeFact(
                src, dst, "mentions",
                source_record_id={"id": "e1"}, source_revision={"v": 1},
            ),
        ],
        source="default",
        strict_endpoints=True,
    )
    rows = list(
        store.checkout(branch="default").query(
            "SELECT edge_id FROM okg.graph_edges", {}
        )
    )
    return cv, [src, dst], [int(r["edge_id"]) for r in rows]


def _scoped_evaluation(
    conn,
    cv: int,
    node_ids: list[str],
    edge_ids: list[int],
    *,
    seed_published_basis: bool = True,
):
    gate = TransformConsistencyGate()
    if seed_published_basis:
        full = gate.evaluate(
            conn,
            target_generation_id=_SYN_GEN - 1,
            catalog_version_id=cv,
            deployment_name="default",
            branch_name="default",
        )
        ChronosGraphStore.from_connection(conn).checkpoint_generation(
            deployment_name="default",
            catalog_version_id=cv,
            status="published",
            metadata={
                "invariant_summary": {
                    "consistency_gates": {
                        "results": [
                            {
                                "check_id": full.check_id,
                                "gate_name": full.gate_name,
                                "result": full.result,
                                "blocks_publish": full.blocks_publish,
                                "coverage_complete": full.coverage_complete,
                                "details": dict(full.details),
                            }
                        ],
                    },
                },
            },
        )
    dirty_scope = (
        {"ticket": [node_ids[0]], "person": [node_ids[1]]},
        {"mentions": list(edge_ids)},
        True,
        {},
    )
    delta_counts = {
        "nodes_added": len(node_ids), "nodes_retracted": 0, "nodes_updated": 0,
        "edges_added": len(edge_ids), "edges_retracted": 0, "edges_updated": 0,
    }
    evaluation = gate.evaluate_incremental(
        conn,
        target_generation_id=_SYN_GEN,
        catalog_version_id=cv,
        deployment_name="default",
        previous_catalog_version_id=cv,
        delta_counts=delta_counts,
        dirty_scope=dirty_scope,
        branch_name="default",
    )
    if seed_published_basis:
        assert (
            evaluation.details.get("scope_mode") == "dirty_edges+incident_dirty_nodes"
        ), evaluation.details
    return evaluation


# --- (1) the complement recipe in the TRUNCATING regime ---------------------


def test_truncated_scope_refuses_the_complement_recipe() -> None:
    """A capped id list must NOT be handed a subtraction recipe.

    Real truncating regime: SCOPE_ID_LIMIT + 1 ids, no patched constant. The
    pre-change recipe was the unconditional string, so following it would have
    reported the SCOPE_ID_LIMIT-th onward ids -- all of which WERE evaluated --
    as unevaluated.
    """

    ids = [f"subject:{i:07d}" for i in range(SCOPE_ID_LIMIT + 1)]
    identity = scope_identity("scope_subject", ids)

    assert identity["scope_subject_ids_truncated"] is True, identity
    assert identity["scope_subject_count"] == SCOPE_ID_LIMIT + 1
    assert len(identity["scope_subject_ids"]) == SCOPE_ID_LIMIT

    recipe = scope_complement_recipe(
        "scope_subject",
        identity,
        universe="this check's subject universe at this generation",
    )
    # The recipe must not offer a subtraction it cannot support...
    assert "MINUS" not in recipe, recipe
    # ...and must say WHY, quantitatively, plus what to use instead.
    assert "NOT COMPUTABLE" in recipe, recipe
    assert str(SCOPE_ID_LIMIT + 1) in recipe, recipe
    assert "scope_subject_digest_sha256" in recipe, recipe

    # The un-truncated case still gets the usable recipe (no over-correction).
    small = scope_identity("scope_subject", ids[:3])
    assert "MINUS" in scope_complement_recipe(
        "scope_subject", small, universe="U"
    )


def test_transform_gate_recipe_refuses_when_the_cap_binds(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The refusal reaches the PERSISTED gate row, not just the helper.

    The cap is lowered to 0 rather than seeding >10k edges: what is under test is
    that the gate's payload asks the helper per-set and carries the answer, and
    the boundary condition is identical at any limit. Pre-change this row said
    "unevaluated edges = live edge_ids at this generation MINUS
    details.scope_identity.scoped_edge_ids" while scoped_edge_ids was empty.
    """

    monkeypatch.setattr(observations, "SCOPE_ID_LIMIT", 0)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv, node_ids, edge_ids = _setup_one_edge_delta(conn)
        evaluation = _scoped_evaluation(conn, cv, node_ids, edge_ids)

    identity = evaluation.details["scope_identity"]
    assert identity["scoped_edge_ids"] == [], identity
    assert identity["scoped_edge_ids_truncated"] is True, identity
    recipe = identity["scope_complement_recipe"]
    for family in ("edge_families", "node_subtype"):
        assert "MINUS" not in recipe[family], recipe
        assert "NOT COMPUTABLE" in recipe[family], recipe


def test_unscoped_sql_gate_frontier_emits_no_complement_recipe(
    clean_runtime: str,
) -> None:
    """An unscoped run must not disclose a complement recipe at all.

    ``scope_active=False`` with ``subject_ids=[]`` plus "unevaluated = universe
    MINUS subject_ids" reads as "the entire universe went unevaluated" for a run
    that swept every subject -- the disclosure inverted. The transform gate and
    the IR executor both gate this inside their scoped branch; pre-change the SQL
    gate returned it unconditionally.
    """

    from okg.substrate.consistency.sql_gate import (
        DeterministicSqlConsistencyGate,
        SqlConsistencyGateSpec,
    )

    spec = SqlConsistencyGateSpec(
        name="unscoped_gate",
        check_id=f"test.sql.unscoped.{uuid.uuid4().hex}",
        predicate_family="unscoped",
        confidence_type="deterministic_boolean",
        candidate_sql="SELECT 1 WHERE false",
        coverage_sql="SELECT 1 WHERE false",
    )
    gate = DeterministicSqlConsistencyGate(spec)
    # No scope_frontier configured => scope_active is False for every run.
    assert gate.spec.scope_frontier is None

    with psycopg.connect(clean_runtime) as conn:
        frontier = gate._stage_scope_frontier(conn, scope_subject_ids=None)

    assert frontier["scope_active"] is False, frontier
    assert frontier["subject_ids"] == [], frontier
    assert "scope_complement_recipe" not in frontier, frontier


# --- (2) the digest: one algorithm, recorded, collation-independent ---------


def test_sql_and_python_scope_digests_agree(clean_runtime: str) -> None:
    """The two implementations must produce the SAME digest for the same set.

    Pre-change the SQL path sorted with the DATABASE COLLATION
    (``ORDER BY subject_id``) while Python sorted by codepoint, so a set with
    mixed-case or punctuated ids digested differently depending on where the
    check ran -- and neither value recorded which function produced it.
    """

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT datcollate FROM pg_database "
                "WHERE datname = current_database()"
            )
            lc_collate = cur.fetchone()[0]
            cur.execute(
                "CREATE TEMP TABLE digest_probe (subject_id TEXT PRIMARY KEY)"
            )
            cur.executemany(
                "INSERT INTO digest_probe (subject_id) VALUES (%s)",
                [(value,) for value in _COLLATION_HOSTILE_IDS],
            )
            cur.execute(
                f"SELECT {scope_digest_sql('subject_id')} FROM digest_probe"
            )
            sql_digest = cur.fetchone()[0]

            # The CONTRAST, and it must not depend on this database's default
            # collation (the dev/CI Postgres is `C`, which would make the whole
            # point inert). An explicit non-C collation stands in for a
            # deployment whose database default is one: the pre-change bare
            # `ORDER BY subject_id` follows it and disagrees; the fixed
            # expression's `COLLATE "C"` overrides it and agrees.
            cur.execute(
                """
                SELECT collname FROM pg_collation
                 WHERE collname = ANY(%s)
                 ORDER BY array_position(%s, collname)
                 LIMIT 1
                """,
                (list(_NON_C_COLLATIONS), list(_NON_C_COLLATIONS)),
            )
            row = cur.fetchone()
            assert row is not None, (
                "no non-C collation available, so the collation dependence "
                f"cannot be witnessed; looked for {_NON_C_COLLATIONS}"
            )
            non_c_collation = row[0]
            cur.execute(
                f"""
                SELECT encode(sha256(convert_to(COALESCE(
                         string_agg(
                           subject_id, E'\n'
                           ORDER BY subject_id COLLATE "{non_c_collation}"
                         ), ''
                       ), 'UTF8')), 'hex'),
                       encode(sha256(convert_to(COALESCE(
                         string_agg(
                           subject_id, E'\n'
                           ORDER BY subject_id COLLATE "C"
                         ), ''
                       ), 'UTF8')), 'hex')
                  FROM digest_probe
                """
            )
            collation_dependent_digest, c_digest = cur.fetchone()

    python_digest = scope_digest(_COLLATION_HOSTILE_IDS)
    # The fix: SQL and Python agree, whatever the database collation is.
    assert sql_digest == python_digest, {
        "sql": sql_digest,
        "python": python_digest,
        "datcollate": lc_collate,
    }
    # The defect being fixed is REAL, not hypothetical: the same subject set
    # digests to two different values across collations, so a bare ORDER BY
    # cannot be identity.
    assert collation_dependent_digest != c_digest, {
        "collation": non_c_collation,
        "digest": collation_dependent_digest,
        "datcollate": lc_collate,
    }
    assert collation_dependent_digest != python_digest, {
        "collation": non_c_collation,
        "python": python_digest,
    }

    # The algorithm travels with the digest, so a reader knows what to run.
    identity = scope_identity("scope_subject", _COLLATION_HOSTILE_IDS)
    assert identity["scope_subject_digest_sha256"] == python_digest
    assert identity["scope_subject_digest_algorithm"] == SCOPE_DIGEST_ALGORITHM

    # And the persisted ids are the SAME strings the digest covers, so a reader
    # can recompute it from the row when the cap did not bind.
    assert scope_digest(identity["scope_subject_ids"]) == python_digest


def test_sql_gate_frontier_digest_is_recomputable(clean_runtime: str) -> None:
    """The digest the REAL gate persists must be reproducible in Python, and must
    name the algorithm that produced it.

    The two preceding tests exercise the shared helpers; this one exercises the
    gate that writes the row an operator actually reads. Pre-change the frontier
    persisted ``subject_digest_sha256`` with no algorithm beside it, so nobody
    could know which function to run against ``subject_ids``.
    """

    from okg.substrate.consistency.sql_gate import (
        DeterministicSqlConsistencyGate,
        SqlConsistencyGateSpec,
        SqlGateScopeFrontier,
    )

    spec = SqlConsistencyGateSpec(
        name="frontier_digest_gate",
        check_id=f"test.sql.frontier-digest.{uuid.uuid4().hex}",
        predicate_family="scoped_frontier",
        confidence_type="deterministic_boolean",
        scope_frontier=SqlGateScopeFrontier(
            source=f"test.frontier.{uuid.uuid4().hex}"
        ),
        candidate_sql="SELECT 1 WHERE false",
        coverage_sql="SELECT 1 WHERE false",
    )
    gate = DeterministicSqlConsistencyGate(spec)

    with psycopg.connect(clean_runtime) as conn:
        frontier = gate._stage_scope_frontier(
            conn,
            scope_subject_ids=list(_COLLATION_HOSTILE_IDS),
        )

    assert frontier["scope_active"] is True, frontier
    assert frontier["subject_count"] == len(_COLLATION_HOSTILE_IDS), frontier
    # The algorithm rides beside the digest...
    assert frontier["subject_digest_algorithm"] == SCOPE_DIGEST_ALGORITHM
    # ...and running that algorithm in Python over the persisted ids reproduces
    # the value the gate computed in SQL. This is the property that makes the
    # digest identity rather than decoration.
    assert frontier["subject_digest_sha256"] == scope_digest(
        frontier["subject_ids"]
    ), frontier
    assert frontier["subject_digest_sha256"] == scope_digest(
        _COLLATION_HOSTILE_IDS
    ), frontier
    # A scoped, untruncated run still gets the usable recipe.
    assert "MINUS" in frontier["scope_complement_recipe"], frontier


def test_empty_scope_digests_agree(clean_runtime: str) -> None:
    """An empty scope must digest identically on both paths (not NULL vs sha256)."""

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE empty_probe (subject_id TEXT)")
            cur.execute(
                f"SELECT {scope_digest_sql('subject_id')} FROM empty_probe"
            )
            sql_digest = cur.fetchone()[0]

    assert sql_digest == scope_digest([])


# --- (3) the induction premise is MEASURED ---------------------------------


def test_scoped_run_falls_back_when_no_published_full_sweep_precedes_it(
    clean_runtime: str,
) -> None:
    """A run with no published full v3 basis must not enter scoped evaluation.

    Pre-change ``verdict_universe_valid_by_induction`` was the literal ``True``
    with ``induction_basis`` = "the previous published generation passed this
    gate over the full graph". Here no generation ever passed this gate at all,
    so that basis is false -- and the field said True anyway.
    """

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv, node_ids, edge_ids = _setup_one_edge_delta(conn)
        evaluation = _scoped_evaluation(
            conn, cv, node_ids, edge_ids, seed_published_basis=False
        )

    details = evaluation.details
    assert details["scope_mode"] == "full", details
    assert details["scope_fallback_reason"] == "no_full_induction_basis", details
    assert details["published_induction_basis"]["verified"] is False, details


def test_induction_claim_is_true_and_names_the_measured_full_sweep(
    clean_runtime: str,
) -> None:
    """With a real earlier full sweep, the claim holds and CITES the evidence."""

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv, node_ids, edge_ids = _setup_one_edge_delta(conn)
        evaluation = _scoped_evaluation(conn, cv, node_ids, edge_ids)

    details = evaluation.details
    assert details["verdict_universe_valid_by_induction"] is True, details
    assert details["induction_unverified_reason"] is None, details
    # The claim now points at the row that backs it -- checkable, not prose.
    assert details["induction_basis_generation_id"] == _SYN_GEN - 1, details
    assert details["induction_chain_unverified_generation_ids"] == [], details
    assert "MEASURED" in details["induction_basis"], details


def test_scoped_gate_row_discloses_scoped_exact(clean_runtime: str) -> None:
    """The gate row itself says scoped-EXACT, for readers that never join to
    consistency_candidate_coverage.

    ``coverage_complete=False`` alone cannot distinguish "exact over a delta"
    from "gave up partway", which is why the differential-replay proof read every
    scoped publish as unproven coverage.
    """

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv, node_ids, edge_ids = _setup_one_edge_delta(conn)
        evaluation = _scoped_evaluation(conn, cv, node_ids, edge_ids)

    assert evaluation.coverage_complete is False
    assert evaluation.details["scoped"] is True, evaluation.details
    assert evaluation.details["scoped_exact"] is True, evaluation.details
    assert evaluation.details["scope_coverage_basis"] == "exact_postgres"
    # Present on the full path too, stated rather than absent.
    full = TransformConsistencyGate()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv2, _, _ = _setup_one_edge_delta(conn)
        ev_full = full.evaluate(
            conn,
            target_generation_id=_SYN_GEN + 1,
            catalog_version_id=cv2,
            deployment_name="default",
            branch_name="default",
        )
    assert ev_full.details["scoped"] is False, ev_full.details
    assert ev_full.details["scoped_exact"] is False, ev_full.details
