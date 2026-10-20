"""Differential repair selection — the §9.8 witness named by the consistency
model: bucket-scoped re-selection carries every untouched bucket forward and the
incremental and from-scratch paths select IDENTICALLY.

PACT ``consistency-repair-objective``, task.repair.differential
(req.repair.differential-selection). docs/develop/architecture/consistency.md §9.8
NAMES ``test_dirty_bucket_only_reselection`` here. A REAL publish cycle on the
in-tree ``consistency-fixture`` deployment (``build_deployment_publisher_stack``
+ ``Publisher.cycle`` — the idiom of test_claim_plane_wire.py), then a genuine
bucket-scoped incremental update:

* ``test_dirty_bucket_only_reselection`` — N buckets, touch ONE claim; the pure
  selection layer is invoked for the dirty bucket ALONE (selection-run count),
  the generation's ``okg.repair_selections`` set is COMPLETE (all buckets), the
  dirty bucket's row is freshly selected (``carried_from_generation`` NULL), the
  untouched rows byte-match the prior generation with their carry provenance, and
  the whole set is byte-identical to a from-scratch full recompute (the parity
  arbiter — extract both, compare canonically);
* ``test_incremental_update_equals_complete_reference_including_selection`` — the
  equivalence tie-in: a genuine incremental update diffs EQUAL against
  ``run_complete_reference`` INCLUDING the SELECTION family (the seam the
  live-series wave designed around, now closed);
* ``test_precedent_edit_records_cause_and_lands_complete_selection`` /
  ``test_policy_edit_records_cause_and_lands_complete_selection`` — a precedent /
  policy edit records its conservative full-recompute cause
  (``precedent_scope_unprovable`` / ``policy_change``) and still lands a COMPLETE
  selection set.

Scheduler-side certificate invalidation (the policy/precedence fingerprint class
forcing the ``claim_projection`` step to run) is witnessed in
test_reuse_scheduler.py; the pure bucket-locality is in test_repair_materialize.py.
Seeding + scratch idioms follow test_claim_plane_wire.py and
test_equivalence_harness.py.
"""
from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, Callable, Iterator
from urllib.parse import urlsplit, urlunsplit

import pytest

psycopg = pytest.importorskip("psycopg")

import okg.substrate.derived_artifacts.repair_materialization as rm  # noqa: E402
from okg.substrate.consistency.claim_model.selection import (  # noqa: E402
    SELECTION_SCHEMA_VERSION,
    SELECTION_SCHEMA_VERSION_LEGACY_V1,
)
from okg.substrate.consistency.equivalence_differ import (  # noqa: E402
    VERDICT_EQUAL,
    ResolvedStateSnapshot,
    diff_resolved_states,
)
from okg.substrate.derived_artifacts.claim_projection_derivation import (  # noqa: E402
    bucket_row_key,
)
from okg.substrate.derived_artifacts.maintained_derivation import (  # noqa: E402
    FULL_RECOMPUTE_CAUSES,
    FULL_REASON_SELECTION_SCHEMA_UPGRADE,
)
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from okg.substrate.publisher.deployment_stack import (  # noqa: E402
    build_deployment_publisher_stack,
)
from okg.substrate.qa.complete_reference import (  # noqa: E402
    OutcomeReplaySource,
    run_complete_reference,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

_DEPLOYMENT = "consistency-fixture"
_PREDICATE = "preferred_target"
_CONSTRAINT = "unique_preferred"
_SCOPE = "HCAL/Run3"
_DEP_DIR = Path(__file__).resolve().parents[3] / "deployments" / "consistency-fixture"

_SCRATCH_EXTENSIONS = (
    "btree_gist", "timescaledb", "vector", "pg_textsearch", "pg_trgm",
    "fuzzystrmatch",
)

# The selection columns that carry the SELECTION CONTENT (the parity comparison
# excludes generation_id, recorded_at, and carried_from_generation — the last is
# differential-maintenance provenance, not selection content, so a from-scratch
# recompute NULLs it while a carry stamps the prior generation).
_SEMANTIC_COLUMNS = (
    "outcome", "winner_claim_id", "generator_id", "generator_version", "basis",
    "distrusted_claim_ids", "asserted_claim_ids", "override_claim_ids",
    "unresolved_reason",
)


# --------------------------------------------------------------------------- #
# Scratch-DB factory (test_equivalence_harness idiom).                          #
# --------------------------------------------------------------------------- #
def _swap_dbname(base_dsn: str, dbname: str) -> str:
    parts = urlsplit(base_dsn)
    return urlunsplit(parts._replace(path="/" + dbname))


def _create_scratch_db(base_dsn: str, dbname: str) -> str:
    if not dbname.startswith("okg_test_"):
        raise AssertionError(f"unsafe scratch db name {dbname!r}")
    maint = _swap_dbname(base_dsn, "postgres")
    with psycopg.connect(maint, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            " WHERE datname = %s AND pid <> pg_backend_pid()",
            (dbname,),
        )
        cur.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        cur.execute(f'CREATE DATABASE "{dbname}"')
    scratch = _swap_dbname(base_dsn, dbname)
    with psycopg.connect(scratch, autocommit=True) as conn, conn.cursor() as cur:
        for ext in _SCRATCH_EXTENSIONS:
            cur.execute(f"CREATE EXTENSION IF NOT EXISTS {ext}")
    return scratch


def _drop_scratch_db(base_dsn: str, dbname: str) -> None:
    from okg.substrate.db.pool import close_all

    close_all()
    maint = _swap_dbname(base_dsn, "postgres")
    with psycopg.connect(maint, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            " WHERE datname = %s AND pid <> pg_backend_pid()",
            (dbname,),
        )
        cur.execute(f'DROP DATABASE IF EXISTS "{dbname}"')


@pytest.fixture
def scratch_db(okg_migrated: str) -> Iterator[Callable[[], str]]:
    base_dsn = okg_migrated
    created: list[str] = []

    def _make() -> str:
        dbname = f"okg_test_repairsel_{uuid.uuid4().hex[:10]}"
        dsn = _create_scratch_db(base_dsn, dbname)
        created.append(dbname)
        return dsn

    try:
        yield _make
    finally:
        for dbname in created:
            try:
                _drop_scratch_db(base_dsn, dbname)
            except Exception:  # noqa: BLE001 - best-effort teardown
                pass


# --------------------------------------------------------------------------- #
# Fixture wiring + fact builders.                                              #
# --------------------------------------------------------------------------- #
def _install_catalog(dsn: str) -> None:
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog

    composed = compose_catalog(_DEP_DIR)
    with psycopg.connect(dsn, autocommit=True) as conn:
        apply_composed_catalog(conn, composed)


def _apply(dsn: str, facts: list[NodeFact]) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        store.apply_facts(facts, source=_DEPLOYMENT, strict_endpoints=False)


def _claim(node_id: str, value: str, *, key: str, source: str, source_class: str,
           **signals: Any) -> NodeFact:
    attrs = {
        "predicate": _PREDICATE, "normalized_key": key, "value": value,
        "scope": _SCOPE, "scope_state": "known",
        "claim_provenance": "derived", "source_id": source,
        "source_class": source_class,
    }
    attrs.update({k: str(v) for k, v in signals.items()})
    return NodeFact(node_id, "consistency_claim", attrs, {"id": node_id}, {"v": 1})


def _policy(node_id: str, *, rev: int = 1, **extra: Any) -> NodeFact:
    attrs = {"enabled": "true", "policy_id": _CONSTRAINT,
             "policy_kind": "functional", "predicate": _PREDICATE}
    attrs.update({k: str(v) for k, v in extra.items()})
    return NodeFact(node_id, "predicate_policy", attrs, {"id": node_id}, {"v": rev})


def _precedent(node_id: str, *, prefer: str, sequence: int) -> NodeFact:
    return NodeFact(
        node_id, "claim_precedent",
        {"prefer_source": prefer, "scope": "", "sequence": str(sequence)},
        {"id": node_id}, {"v": 1},
    )


def _publish(dsn: str) -> dict[str, Any]:
    stack = build_deployment_publisher_stack(
        dsn=dsn, deployment_name=_DEPLOYMENT, deployment_dir=_DEP_DIR,
    )
    out = Publisher(dsn, _DEPLOYMENT, **stack.publisher_kwargs(), catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    assert out["status"] == "published", out
    return out


def _proj_summary(out: dict[str, Any]) -> dict[str, Any]:
    return dict(out["maintained_derivations"]["claim_projection"])


def _selection_content(dsn: str, generation_id: str) -> dict[str, dict[str, Any]]:
    """The selection ROWS for a generation, keyed by bucket, with the semantic
    columns + carry provenance + digest — the parity comparison surface."""
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT bucket_key, outcome, winner_claim_id, generator_id, "
            "generator_version, basis, distrusted_claim_ids, asserted_claim_ids, "
            "override_claim_ids, unresolved_reason, carried_from_generation, "
            "selection_digest "
            "FROM okg.repair_selections "
            "WHERE deployment_name = %s AND generation_id = %s ORDER BY bucket_key",
            (_DEPLOYMENT, str(generation_id)),
        )
        out: dict[str, dict[str, Any]] = {}
        for row in cur.fetchall():
            out[str(row[0])] = {
                "bucket_key": str(row[0]),
                "outcome": row[1], "winner_claim_id": row[2],
                "generator_id": row[3], "generator_version": row[4],
                "basis": row[5], "distrusted_claim_ids": list(row[6] or []),
                "asserted_claim_ids": list(row[7] or []),
                "override_claim_ids": list(row[8] or []),
                "unresolved_reason": row[9],
                "carried_from_generation": row[10], "selection_digest": row[11],
            }
    return out


def _semantic(content: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        bk: {c: row[c] for c in _SEMANTIC_COLUMNS}
        for bk, row in content.items()
    }


def _seed_three_buckets(sfx: str) -> tuple[list[NodeFact], dict[str, str]]:
    """k1 decisive (a>b), k2 contested (c,d tie), k3 decisive (e>f) — plus the
    functional policy. Returns (facts, {bucket-label: bucket_row_key})."""
    key1, key2, key3 = f"k|1:{sfx}", f"k|2:{sfx}", f"k|3:{sfx}"
    facts = [
        _claim(f"a:{sfx}", "vA", key=key1, source="p1",
               source_class="release_notes", source_authority=10),
        _claim(f"b:{sfx}", "vB", key=key1, source="p2",
               source_class="wiki", source_authority=1),
        _claim(f"c:{sfx}", "vC", key=key2, source="p3", source_class="wiki"),
        _claim(f"d:{sfx}", "vD", key=key2, source="p4", source_class="wiki"),
        _claim(f"e:{sfx}", "vE", key=key3, source="p5",
               source_class="release_notes", source_authority=7),
        _claim(f"f:{sfx}", "vF", key=key3, source="p6",
               source_class="wiki", source_authority=2),
        _policy(f"pol:{sfx}"),
    ]
    rks = {
        "k1": bucket_row_key(_PREDICATE, key1),
        "k2": bucket_row_key(_PREDICATE, key2),
        "k3": bucket_row_key(_PREDICATE, key3),
    }
    return facts, rks


# =========================================================================== #
# The §9.8-named witness.                                                       #
# =========================================================================== #
@pytest.mark.postgres
def test_dirty_bucket_only_reselection(
    clean_runtime: str, scratch_db: Callable[[], str], monkeypatch: Any
) -> None:
    """Touch one bucket → the pure layer runs for the dirty bucket ALONE, the
    generation carries a COMPLETE selection set (dirty freshly selected, untouched
    carried forward with provenance), and the whole set is byte-identical to a
    from-scratch full recompute of the same state (the parity arbiter)."""
    _install_catalog(clean_runtime)
    sfx = uuid.uuid4().hex[:8]
    key1 = f"k|1:{sfx}"
    base_facts, rks = _seed_three_buckets(sfx)

    # ---- G0: first publish (full) records all three buckets -----------------
    _apply(clean_runtime, base_facts)
    out0 = _publish(clean_runtime)
    g0 = str(out0["compat_generation_id"])
    c0 = _selection_content(clean_runtime, g0)
    print(f"\n[dirty-reselect] G0 gen={g0} buckets={sorted(c0)}")
    assert set(c0) == set(rks.values()), sorted(c0)
    assert c0[rks["k1"]]["outcome"] == "selected"
    assert c0[rks["k2"]]["outcome"] == "unresolved"
    assert c0[rks["k3"]]["outcome"] == "selected"
    # every G0 row was freshly selected (a full recompute carries nothing).
    assert all(r["carried_from_generation"] is None for r in c0.values())

    # ---- spy: the pure selection layer must run for the DIRTY bucket alone ---
    seen: list[list[str]] = []
    _orig = rm.select_repairs

    def _spy(buckets: Any, precedents: Any = ()) -> Any:
        materialized = list(buckets)
        seen.append(sorted(b.bucket_key for b in materialized))
        return _orig(materialized, precedents)

    monkeypatch.setattr(rm, "select_repairs", _spy)

    # ---- touch ONE bucket: add a higher-authority claim to k1 (genuine
    #      bucket-scoped incremental — k1 flips to g, distrust a+b) ------------
    g_claim = _claim(f"g:{sfx}", "vG", key=key1, source="p7",
                     source_class="release_notes", source_authority=20)
    _apply(clean_runtime, [g_claim])
    out1 = _publish(clean_runtime)
    g1 = str(out1["compat_generation_id"])
    proj = _proj_summary(out1)
    print(f"[dirty-reselect] G1 gen={g1} proj_mode={proj['mode']} "
          f"full_reason={proj['full_reason']} dirty_buckets={proj['dirty_bucket_count']} "
          f"select_repairs_calls={seen}")

    # genuine bucket-scoped incremental: no full-recompute cause, one dirty bucket.
    assert proj["mode"] == "incremental"
    assert proj["full_reason"] is None
    assert proj["dirty_bucket_count"] == 1
    # THE selection-run count: select_repairs saw the dirty bucket and nothing else.
    assert seen == [[rks["k1"]]], seen

    # ---- the generation carries a COMPLETE selection set --------------------
    c1 = _selection_content(clean_runtime, g1)
    assert set(c1) == set(rks.values()), sorted(c1)

    # the dirty bucket (k1) is freshly selected: winner g, distrust a+b, no carry.
    k1 = c1[rks["k1"]]
    assert k1["outcome"] == "selected"
    assert k1["winner_claim_id"] == f"g:{sfx}"
    assert sorted(k1["distrusted_claim_ids"]) == sorted([f"a:{sfx}", f"b:{sfx}"])
    assert k1["carried_from_generation"] is None

    # the untouched buckets (k2, k3) are CARRIED forward from G0: their semantic
    # content byte-matches the prior generation and they record its id as their
    # carry provenance.
    for label in ("k2", "k3"):
        rk = rks[label]
        assert _semantic({rk: c1[rk]})[rk] == _semantic({rk: c0[rk]})[rk], label
        assert c1[rk]["carried_from_generation"] == g0, label

    # every row of the generation shares ONE whole-generation digest.
    digests = {r["selection_digest"] for r in c1.values()}
    assert len(digests) == 1 and next(iter(digests))

    # ---- the parity arbiter: G1's selection content == a from-scratch full
    #      recompute of the SAME post-update state (extract both, compare) -----
    post_facts = [*base_facts, g_claim]
    scratch = scratch_db()
    ref = run_complete_reference(
        post_facts, deployment_dir=_DEP_DIR, scratch_dsn=scratch,
        outcome_store_source=OutcomeReplaySource(deployment=_DEPLOYMENT, rows=()),
    )
    ref_content = _selection_content(scratch, str(ref.compat_generation_id))
    print(f"[dirty-reselect] parity: incremental buckets={sorted(c1)} "
          f"from_scratch buckets={sorted(ref_content)}")
    # the from-scratch recompute carries nothing (all freshly selected)...
    assert all(r["carried_from_generation"] is None for r in ref_content.values())
    # ...yet the SELECTION CONTENT is byte-identical to the incremental cycle's.
    assert _semantic(c1) == _semantic(ref_content)
    # and the whole-generation digest is identical (the decisive parity check).
    assert {r["selection_digest"] for r in c1.values()} == {
        r["selection_digest"] for r in ref_content.values()
    }
    print("[dirty-reselect] parity arbiter EQUAL (content + whole-generation digest)")


def _make_prior_generation_legacy(dsn: str, generation_id: str) -> int:
    """Rewrite a generation's selection rows into the PRE-OVERRIDE shape.

    The upgrade defect this witnesses cannot be reproduced on a fresh database:
    every test database is created by the CURRENT schema, so both sides of the
    comparison are post-change and the carry looks correct. The legacy state has
    to be CONSTRUCTED — rows whose ``override_claim_ids`` is the DDL default
    ``'[]'`` (absence, not a measured refusal) and whose
    ``selection_schema_version`` is the version those rows were written under.
    Returns the number of rows rewritten (0 would make the test vacuous)."""
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE okg.repair_selections "
            "   SET override_claim_ids = '[]'::jsonb, "
            "       selection_schema_version = %s "
            " WHERE deployment_name = %s AND generation_id = %s",
            (SELECTION_SCHEMA_VERSION_LEGACY_V1, _DEPLOYMENT, str(generation_id)),
        )
        return cur.rowcount


@pytest.mark.postgres
def test_upgraded_deployment_recomputes_instead_of_carrying_legacy_rows(
    clean_runtime: str, scratch_db: Callable[[], str]
) -> None:
    """An UPGRADED deployment's first incremental cycle must not carry its
    pre-override selection rows forward.

    Those rows carry ``override_claim_ids = '[]'`` because the upgrade's DDL
    default put it there, NOT because the override generator refused — and the
    two are indistinguishable in the column (a contested, value-less, or
    surrogate-valued bucket legitimately records an empty list). Carrying them
    would leave every untouched bucket with no second repair recorded — the
    measurement surface this change exists to create, EMPTY for exactly the
    stable buckets — and would stamp a whole-generation ``selection_digest``
    assembled from a candidate set missing those override candidates, so the
    incremental and from-scratch paths would no longer select identically and
    ``equivalence_differ`` would report a release-blocking mismatch. Nothing
    else detects it: a carried row keeps the SELECTED generator's id and
    version, both unchanged by this PACT.

    The fence is the per-row selection schema version: a prior row written under
    a different version is not carryable, so the cycle falls back to a full
    recompute with the recorded cause ``selection_schema_upgrade``."""
    _install_catalog(clean_runtime)
    sfx = uuid.uuid4().hex[:8]
    key1 = f"k|1:{sfx}"
    base_facts, rks = _seed_three_buckets(sfx)

    # ---- G0: a normal publish, whose rows DO record the second repair -------
    _apply(clean_runtime, base_facts)
    out0 = _publish(clean_runtime)
    g0 = str(out0["compat_generation_id"])
    c0 = _selection_content(clean_runtime, g0)
    # k3 is an untouched-in-G1 DECISIVE bucket: its fresh computation populates
    # override_claim_ids, so a legacy-shaped carry of it is provably lossy.
    assert c0[rks["k3"]]["override_claim_ids"] == [f"f:{sfx}"], c0[rks["k3"]]
    g0_digest = {r["selection_digest"] for r in c0.values()}

    # ---- make G0 look like it was written before the override column --------
    rewritten = _make_prior_generation_legacy(clean_runtime, g0)
    print(f"\n[upgrade-fence] rewrote {rewritten} G0 rows to the legacy shape")
    assert rewritten == len(c0) and rewritten > 0, rewritten
    legacy = _selection_content(clean_runtime, g0)
    assert all(r["override_claim_ids"] == [] for r in legacy.values()), legacy

    # ---- G1: touch ONE bucket — the cycle a differential carry would serve ---
    g_claim = _claim(f"g:{sfx}", "vG", key=key1, source="p7",
                     source_class="release_notes", source_authority=20)
    _apply(clean_runtime, [g_claim])
    out1 = _publish(clean_runtime)
    g1 = str(out1["compat_generation_id"])
    proj = _proj_summary(out1)
    print(f"[upgrade-fence] G1 gen={g1} mode={proj['mode']} "
          f"full_reason={proj['full_reason']}")

    # the fence fired: a recorded full recompute, not a silent carry.
    assert proj["full_reason"] == FULL_REASON_SELECTION_SCHEMA_UPGRADE, proj
    assert proj["full_reason"] in FULL_RECOMPUTE_CAUSES

    c1 = _selection_content(clean_runtime, g1)
    assert set(c1) == set(rks.values()), sorted(c1)
    # NOTHING was carried, and the untouched decisive bucket's second repair is
    # present again (a carry would have left it [] forever).
    assert all(r["carried_from_generation"] is None for r in c1.values()), c1
    assert c1[rks["k3"]]["override_claim_ids"] == [f"f:{sfx}"], c1[rks["k3"]]
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT selection_schema_version FROM okg.repair_selections "
            " WHERE deployment_name = %s AND generation_id = %s",
            (_DEPLOYMENT, g1),
        )
        versions = sorted(r[0] for r in cur.fetchall())
    print(f"[upgrade-fence] G1 selection_schema_version={versions}")
    assert versions == [SELECTION_SCHEMA_VERSION], versions

    # ---- the decisive check: the digest equals a FROM-SCRATCH recompute's ---
    post_facts = [*base_facts, g_claim]
    scratch = scratch_db()
    ref = run_complete_reference(
        post_facts, deployment_dir=_DEP_DIR, scratch_dsn=scratch,
        outcome_store_source=OutcomeReplaySource(deployment=_DEPLOYMENT, rows=()),
    )
    ref_content = _selection_content(scratch, str(ref.compat_generation_id))
    g1_digest = {r["selection_digest"] for r in c1.values()}
    ref_digest = {r["selection_digest"] for r in ref_content.values()}
    print(f"[upgrade-fence] g1_digest={sorted(g1_digest)} "
          f"ref_digest={sorted(ref_digest)} legacy_g0_digest={sorted(g0_digest)}")
    assert _semantic(c1) == _semantic(ref_content)
    assert len(g1_digest) == 1 and g1_digest == ref_digest
    print("[upgrade-fence] upgraded cycle EQUALS from-scratch (content + digest)")


@pytest.mark.postgres
def test_incremental_update_equals_complete_reference_including_selection(
    clean_runtime: str, scratch_db: Callable[[], str]
) -> None:
    """The equivalence tie-in (the seam the live-series wave designed around): a
    genuine bucket-scoped INCREMENTAL update on the fixture now diffs EQUAL
    against ``run_complete_reference`` INCLUDING the SELECTION family — the
    differential path no longer defers, so selections are present and equal."""
    _install_catalog(clean_runtime)
    sfx = uuid.uuid4().hex[:8]
    key1 = f"k|1:{sfx}"
    base_facts, rks = _seed_three_buckets(sfx)

    _apply(clean_runtime, base_facts)
    _publish(clean_runtime)  # G0 full

    g_claim = _claim(f"g:{sfx}", "vG", key=key1, source="p7",
                     source_class="release_notes", source_authority=20)
    _apply(clean_runtime, [g_claim])
    out1 = _publish(clean_runtime)  # G1 genuine incremental
    proj = _proj_summary(out1)
    assert proj["mode"] == "incremental" and proj["full_reason"] is None, proj
    g1, g1_chronos = str(out1["compat_generation_id"]), str(out1["generation_id"])

    scratch = scratch_db()
    ref = run_complete_reference(
        [*base_facts, g_claim], deployment_dir=_DEP_DIR, scratch_dsn=scratch,
        outcome_store_source=OutcomeReplaySource(deployment=_DEPLOYMENT, rows=()),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        candidate = ResolvedStateSnapshot.extract(
            conn, deployment=_DEPLOYMENT, generation_id=g1,
            chronos_generation_id=g1_chronos,
        )
    with psycopg.connect(scratch, autocommit=True) as conn:
        reference = ResolvedStateSnapshot.extract(
            conn, deployment=_DEPLOYMENT,
            generation_id=str(ref.compat_generation_id),
            chronos_generation_id=str(ref.generation_id),
        )
    verdict = diff_resolved_states(reference, candidate)
    print(f"\n[incr-equals-complete] verdict={verdict.verdict} "
          f"incr_gen={g1} ref_gen={ref.compat_generation_id} "
          f"selection_buckets={sorted(candidate.selections)}")
    assert verdict.verdict == VERDICT_EQUAL, verdict.to_json_dict()
    # the SELECTION family is genuinely populated and equal (seam closed).
    assert candidate.selections and reference.selections
    assert candidate.selections == reference.selections
    assert candidate.selection_digest == reference.selection_digest
    # sanity: the incremental cycle actually carried untouched buckets forward.
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.repair_selections WHERE deployment_name = %s "
            "AND generation_id = %s AND carried_from_generation IS NOT NULL",
            (_DEPLOYMENT, g1),
        )
        carried = int(cur.fetchone()[0])
    print(f"[incr-equals-complete] carried-forward rows at G1={carried}")
    assert carried >= 1


@pytest.mark.postgres
def test_published_generation_records_two_repairs_per_decisive_conflict(
    clean_runtime: str
) -> None:
    """PACT ``concord-second-repair-generator``, both requirements, on a REAL
    published generation rather than a unit fixture.

    On the fixture deployment's three buckets:

    * each DECISIVE bucket's candidate set has THREE members (do nothing,
      retract the loser, rewrite the loser to the winner's value) — read back
      from the published rows, not recomputed from the seed;
    * applying the two non-empty repairs to the generation's own effective
      claims leaves MEASURABLY different residual state;
    * the CONTESTED bucket records no override at all and still retains its
      violation — the second generator did not settle what precedence refused;
    * the selection record still names which generator produced the chosen
      repair.
    """
    from okg.substrate.consistency.claim_model import (
        ACTION_OVERRIDE,
        ACTION_RETRACT,
        EMPTY_REPAIR_GENERATOR_ID,
        OVERRIDE_SURVIVING_VALUE_GENERATOR_ID,
        PRECEDENCE_RETRACTION_GENERATOR_ID,
        Correction,
        merge_effective_claims,
    )
    from okg.substrate.consistency.claim_model.views import ClaimView
    from okg.substrate.derived_artifacts.claim_effective_derivation import (
        ROW_KIND_EFFECTIVE,
    )

    _install_catalog(clean_runtime)
    sfx = uuid.uuid4().hex[:8]
    base_facts, rks = _seed_three_buckets(sfx)
    _apply(clean_runtime, base_facts)
    out = _publish(clean_runtime)
    gen, chronos_gen = str(out["compat_generation_id"]), str(out["generation_id"])
    content = _selection_content(clean_runtime, gen)
    assert set(content) == set(rks.values()), sorted(content)

    # ---- the decisive buckets each record a SECOND available repair ---------
    k1, k3 = content[rks["k1"]], content[rks["k3"]]
    k2 = content[rks["k2"]]
    print(f"\n[two-repairs] gen={gen}")
    for label, row in (("k1", k1), ("k2", k2), ("k3", k3)):
        print(f"[two-repairs] {label} outcome={row['outcome']} "
              f"generator={row['generator_id']} winner={row['winner_claim_id']} "
              f"distrusted={row['distrusted_claim_ids']} "
              f"override={row['override_claim_ids']}")
    assert k1["outcome"] == "selected"
    assert k1["distrusted_claim_ids"] == [f"b:{sfx}"]
    assert k1["override_claim_ids"] == [f"b:{sfx}"]
    assert k3["override_claim_ids"] == [f"f:{sfx}"]
    # the selection record names WHICH generator produced the chosen repair.
    assert k1["generator_id"] == PRECEDENCE_RETRACTION_GENERATOR_ID
    assert k2["generator_id"] == EMPTY_REPAIR_GENERATOR_ID

    # ---- the contested bucket: no override, violation retained --------------
    assert k2["outcome"] == "unresolved"
    # the PACT acceptance is that the bucket RETAINS its violation, which is
    # the reason token 'contested' — the outcome column ('unresolved') is the
    # recording vocabulary, the reason is WHY. Strict equality: a disjunction
    # here would pass on any truthy reason and assert nothing.
    from okg.substrate.consistency.claim_model.precedence import OUTCOME_CONTESTED

    assert k2["unresolved_reason"] == OUTCOME_CONTESTED, k2
    assert k2["override_claim_ids"] == [], k2
    assert k2["distrusted_claim_ids"] == []
    assert k2["winner_claim_id"] is None

    # ---- the per-conflict candidate set has MORE THAN TWO members -----------
    # rebuilt from the published rows through the same inverse the differential
    # digest uses, so this is what a reader of the generation can see.
    rebuilt = rm._selected_repair_json_from_rows(list(content.values()))
    per_bucket: dict[str, list[dict[str, Any]]] = {}
    for cand in rebuilt["candidate_set"]["candidates"]:
        per_bucket.setdefault(cand["bucket_key"], []).append(cand)
    empty = per_bucket[""]
    for label in ("k1", "k3"):
        offered = per_bucket[rks[label]]
        generators = sorted(c["generator_id"] for c in offered)
        print(f"[two-repairs] {label} candidate generators="
              f"{sorted([*generators, EMPTY_REPAIR_GENERATOR_ID])}")
        assert generators == [OVERRIDE_SURVIVING_VALUE_GENERATOR_ID,
                              PRECEDENCE_RETRACTION_GENERATOR_ID], generators
        # empty + two non-empty repairs = three members for this conflict.
        assert len(offered) + len(empty) == 3
    assert rks["k2"] not in per_bucket  # contested: nothing offered at all

    # ---- residual state: apply each repair to the GENERATION's own claims ---
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkout = store.checkout_generation(chronos_gen)
        # okg.claim_effective pk is (row_kind, row_key); for an 'effective' row
        # the row_key IS the claim's node_id.
        eff = checkout.query(
            "SELECT row_key, value FROM okg.claim_effective "
            "WHERE row_kind = :k", {"k": ROW_KIND_EFFECTIVE},
        )
    values = {str(r["row_key"]): r["value"] for r in eff}
    print(f"[two-repairs] effective claim values={values}")
    members = [f"a:{sfx}", f"b:{sfx}"]
    derived = [
        ClaimView(node_id=cid, source_id=f"p:{cid}", predicate=_PREDICATE,
                  normalized_key=f"k|1:{sfx}", value=values[cid],
                  scope=_SCOPE, scope_state="known")
        for cid in members
    ]
    winner, loser = f"a:{sfx}", f"b:{sfx}"
    after_retract = {
        v.node_id: v.value for v in merge_effective_claims(
            derived, [Correction(claim_id=loser, action=ACTION_RETRACT)]
        ).effective
    }
    after_override = {
        v.node_id: v.value for v in merge_effective_claims(
            derived,
            # the value is COPIED off the surviving winner named by the
            # candidate's source_claim_id — never authored by the repair.
            [Correction(claim_id=loser, action=ACTION_OVERRIDE,
                        value=values[winner])],
        ).effective
    }
    print(f"[two-repairs] k1 derived={ {v.node_id: v.value for v in derived} }")
    print(f"[two-repairs] k1 after retraction={after_retract}")
    print(f"[two-repairs] k1 after override  ={after_override}")
    assert after_retract == {winner: values[winner]}
    assert after_override == {winner: values[winner], loser: values[winner]}
    assert after_retract != after_override
    assert set(after_retract) != set(after_override)
    # the override candidate's source is the winner the row records.
    override_cand = next(
        c for c in per_bucket[rks["k1"]]
        if c["generator_id"] == OVERRIDE_SURVIVING_VALUE_GENERATOR_ID
    )
    assert {c["source_claim_id"] for c in override_cand["corrections"]} == {winner}
    assert k1["winner_claim_id"] == winner


@pytest.mark.postgres
def test_precedent_edit_records_cause_and_lands_complete_selection(
    clean_runtime: str
) -> None:
    """A precedent edit falls back to a FULL recompute with the honest
    conservative cause ``precedent_scope_unprovable`` (v1 does not prove the
    governed bucket set, design D4) AND still lands a COMPLETE selection set —
    the recorded-cause full path of req.repair.differential-selection."""
    _install_catalog(clean_runtime)
    sfx = uuid.uuid4().hex[:8]
    base_facts, rks = _seed_three_buckets(sfx)

    _apply(clean_runtime, base_facts)
    _publish(clean_runtime)  # G0 full

    # A precedent write's blast radius is every bucket its scope touches; v1 does
    # not prove that set, so a changed claim_precedent forces a full recompute.
    _apply(clean_runtime, [_precedent(f"prec:{sfx}", prefer="registry", sequence=1)])
    out1 = _publish(clean_runtime)
    proj = _proj_summary(out1)
    g1 = str(out1["compat_generation_id"])
    print(f"\n[precedent-edit] G1 gen={g1} full_reason={proj['full_reason']}")
    assert proj["full_reason"] == "precedent_scope_unprovable", proj

    content = _selection_content(clean_runtime, g1)
    print(f"[precedent-edit] selection buckets={sorted(content)} "
          f"(complete set, all freshly selected)")
    assert set(content) == set(rks.values()), sorted(content)
    # a full recompute carries nothing forward — every row is freshly selected.
    assert all(r["carried_from_generation"] is None for r in content.values())


@pytest.mark.postgres
def test_policy_edit_records_cause_and_lands_complete_selection(
    clean_runtime: str
) -> None:
    """A policy edit falls back to a FULL recompute with cause ``policy_change``
    (a Σ edit re-classifies every bucket) AND still lands a COMPLETE selection
    set — the second recorded-cause full path req.repair.differential-selection
    names."""
    _install_catalog(clean_runtime)
    sfx = uuid.uuid4().hex[:8]
    base_facts, rks = _seed_three_buckets(sfx)

    _apply(clean_runtime, base_facts)
    _publish(clean_runtime)  # G0 full

    # Re-affirm the policy (a changed predicate_policy node) -> policy_change.
    _apply(clean_runtime, [_policy(f"pol:{sfx}", rev=2, review_note="re-affirmed")])
    out1 = _publish(clean_runtime)
    proj = _proj_summary(out1)
    g1 = str(out1["compat_generation_id"])
    print(f"\n[policy-edit] G1 gen={g1} full_reason={proj['full_reason']}")
    assert proj["full_reason"] == "policy_change", proj

    content = _selection_content(clean_runtime, g1)
    print(f"[policy-edit] selection buckets={sorted(content)} "
          f"(complete set, all freshly selected)")
    assert set(content) == set(rks.values()), sorted(content)
    assert all(r["carried_from_generation"] is None for r in content.values())
