"""ivm-effective-attribute-view / task.generation-ordinal-primitive.

A sound, branch-aware, backfill-safe generation ordinal is the PREREQUISITE for
the planned validity-interval representation of the effective-attribute view
(``req.incremental-storage-representation`` needs a ``from_gen <= G < to_gen``
predicate). These tests pin the four properties the representation depends on,
NONE of which the substrate's existing wall-clock order (``created_at`` /
``_chronos_compat_generation_id`` = ``time.time_ns()//1000``) provides:

  * **monotonic + dense** within a branch,
  * **backfill-safe**: re-assigning a PAST generation keeps its ordinal -- it
    never gets a fresh now-value that would order it AFTER newer generations
    (the exact silent-stale bug the design notes flag),
  * **branch-aware**: a fork continues from its base generation's ordinal and
    does not collide with the parent branch's own later ordinals,
  * **fail-loud**: a missing ordinal or an un-backfilled fork base RAISES --
    never a wall-clock fallback (a default-as-fallback P0).

The primitive is exercised directly here (it is intentionally not yet wired into
the live publish path; see the module docstring).
"""
from __future__ import annotations

import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.errors import SubstrateError  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.graph_store_internal.generation_ordinal import (  # noqa: E402
    assign_generation_ordinal,
    backfill_generation_ordinals,
    resolve_generation_ordinal,
)

pytestmark = pytest.mark.postgres


def _cp(branch: str, n: int, suffix: str) -> str:
    """A checkpoint id shaped like a real okg generation id."""
    return f"okg:default:branch:{branch}:gen:{suffix}{n:04d}:{suffix}"


def _ctx(conn):
    store = ChronosGraphStore.from_connection(conn)
    return store, store.context, store.context.db


def test_ordinal_monotonic_dense_and_backfill_safe(clean_runtime: str) -> None:
    suffix = uuid.uuid4().hex[:8]
    branch = f"root_{suffix}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _store, ctx, db = _ctx(conn)
        cps = [_cp(branch, i, suffix) for i in range(3)]
        ords = [
            assign_generation_ordinal(db, ctx, checkpoint_id=cp, branch=branch)
            for cp in cps
        ]
        # Dense, monotonic, starting at 0 on a fresh root branch.
        assert ords == [0, 1, 2], ords

        # BACKFILL-SAFETY: re-assigning the FIRST (now past) generation after
        # newer ones exist returns its ORIGINAL ordinal -- not a fresh max+1.
        # This is the property a wall-clock token cannot provide.
        again = assign_generation_ordinal(db, ctx, checkpoint_id=cps[0], branch=branch)
        assert again == 0, again

        assert resolve_generation_ordinal(db, cps[1]) == 1


def test_ordinal_is_branch_aware_across_fork(clean_runtime: str) -> None:
    suffix = uuid.uuid4().hex[:8]
    root = f"root_{suffix}"
    fork = f"fork_{suffix}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store, ctx, db = _ctx(conn)
        root_cps = [_cp(root, i, suffix) for i in range(3)]
        for cp in root_cps:
            assign_generation_ordinal(db, ctx, checkpoint_id=cp, branch=root)

        # Fork from the root generation at ordinal 1.
        store.create_branch(
            fork,
            from_branch="default",
            metadata={
                "kind": "okg_branch",
                "base_generation_id": root_cps[1],
                "base_branch": root,
            },
        )
        f0 = assign_generation_ordinal(
            db, ctx, checkpoint_id=_cp(fork, 0, suffix), branch=fork
        )
        f1 = assign_generation_ordinal(
            db, ctx, checkpoint_id=_cp(fork, 1, suffix), branch=fork
        )
        # The fork CONTINUES from its base (ordinal 1) + 1; it does not restart
        # at 0 and does not inherit the parent's later ordinals.
        assert (f0, f1) == (2, 3), (f0, f1)

        # Branch scoping: the parent's OWN ordinal 2 and the fork's ordinal 2
        # coexist (the uniqueness is per branch_id), and each resolves to itself.
        assert resolve_generation_ordinal(db, root_cps[2]) == 2
        assert resolve_generation_ordinal(db, _cp(fork, 0, suffix)) == 2


def test_fork_with_unbackfilled_base_fails_loud(clean_runtime: str) -> None:
    suffix = uuid.uuid4().hex[:8]
    fork = f"orphan_{suffix}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store, ctx, db = _ctx(conn)
        # The base generation was NEVER assigned an ordinal: positioning the
        # fork would require inventing one -> must fail loud, never guess.
        store.create_branch(
            fork,
            from_branch="default",
            metadata={
                "kind": "okg_branch",
                "base_generation_id": _cp("missing", 9, suffix),
                "base_branch": "missing",
            },
        )
        with pytest.raises(SubstrateError):
            assign_generation_ordinal(
                db, ctx, checkpoint_id=_cp(fork, 0, suffix), branch=fork
            )


def test_resolve_missing_ordinal_fails_loud(clean_runtime: str) -> None:
    suffix = uuid.uuid4().hex[:8]
    branch = f"root_{suffix}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _store, ctx, db = _ctx(conn)
        # Create the registry (assign one), then resolve a DIFFERENT, absent
        # generation: a missing ordinal raises rather than falling back to
        # wall-clock order.
        assign_generation_ordinal(db, ctx, checkpoint_id=_cp(branch, 0, suffix), branch=branch)
        with pytest.raises(SubstrateError):
            resolve_generation_ordinal(db, _cp(branch, 99, suffix))


def test_backfill_seeds_then_live_mint_continues(clean_runtime: str) -> None:
    suffix = uuid.uuid4().hex[:8]
    branch = f"root_{suffix}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _store, ctx, db = _ctx(conn)
        # Pretend these are pre-existing, un-ordinaled generations in causal order.
        existing = [_cp(branch, i, suffix) for i in range(4)]
        res = backfill_generation_ordinals(
            db, branch=branch, ordered_checkpoint_ids=existing
        )
        assert res["assigned"] == 4, res
        assert [resolve_generation_ordinal(db, cp) for cp in existing] == [0, 1, 2, 3]

        # Idempotent: a second backfill assigns nothing and renumbers nothing.
        res2 = backfill_generation_ordinals(
            db, branch=branch, ordered_checkpoint_ids=existing
        )
        assert res2["assigned"] == 0, res2
        assert [resolve_generation_ordinal(db, cp) for cp in existing] == [0, 1, 2, 3]

        # A NEW generation minted live continues from the seeded max + 1.
        new_cp = _cp(branch, 4, suffix)
        assert assign_generation_ordinal(db, ctx, checkpoint_id=new_cp, branch=branch) == 4


def test_monotonicity_guard_rejects_duplicate_slot(clean_runtime: str) -> None:
    suffix = uuid.uuid4().hex[:8]
    branch = f"root_{suffix}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _store, ctx, db = _ctx(conn)
        assign_generation_ordinal(db, ctx, checkpoint_id=_cp(branch, 0, suffix), branch=branch)
        # The unique (branch_id, ordinal) index forbids a second checkpoint from
        # occupying the same slot -- the structural monotonicity guarantee.
        with conn.cursor() as cur, pytest.raises(psycopg.errors.UniqueViolation):
            cur.execute(
                "INSERT INTO okg.generation_ordinals "
                "(checkpoint_id, branch_id, ordinal) VALUES (%s, %s, %s)",
                (_cp(branch, 1, suffix), branch, 0),
            )


def test_structural_fork_without_base_key_fails_loud(clean_runtime: str) -> None:
    """A real Chronos fork off `default` that records NO base_generation_id (the
    ingest / merge_back quarantine shape) must FAIL LOUD, not silently get ordinal
    0. The structural parent_branch_id is the fork signal -- base-key presence is
    not. This is the silent fork-as-root mis-ordering the primitive must prevent."""
    suffix = uuid.uuid4().hex[:8]
    fork = f"qfork_{suffix}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store, ctx, db = _ctx(conn)
        store.create_branch(fork, from_branch="default", metadata={"kind": "okg_branch"})
        with pytest.raises(SubstrateError):
            assign_generation_ordinal(
                db, ctx, checkpoint_id=_cp(fork, 0, suffix), branch=fork
            )


def test_backfill_rejects_inconsistent_order(clean_runtime: str) -> None:
    """Backfill is position-authoritative: a re-run whose order contradicts the
    prior seed (a generation spliced between already-seeded ones) must FAIL LOUD
    before mutating, never silently mis-number the tail."""
    suffix = uuid.uuid4().hex[:8]
    branch = f"root_{suffix}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _store, ctx, db = _ctx(conn)
        seed = [_cp(branch, i, suffix) for i in range(3)]  # A,B,C -> 0,1,2
        backfill_generation_ordinals(db, branch=branch, ordered_checkpoint_ids=seed)
        # Splice a new generation between B and C: C now sits at ordinal 2 but the
        # new order expects it at 3 -> inconsistent with the seed -> must raise.
        spliced = [seed[0], seed[1], _cp(branch, 99, suffix), seed[2]]
        with pytest.raises(SubstrateError):
            backfill_generation_ordinals(
                db, branch=branch, ordered_checkpoint_ids=spliced
            )
