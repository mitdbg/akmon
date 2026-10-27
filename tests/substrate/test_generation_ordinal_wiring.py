"""ivm-effective-attribute-view / wiring the generation ordinal into the publish path.

Proves the live-path behaviour of `mint_generation_ordinal` (called from
`checkpoint_generation`):
  * real publishes mint strictly-increasing ordinals,
  * the FIRST mint on a branch with pre-existing un-ordinaled generations
    one-time backfills them in causal order, then continues O(1),
  * every publish, intermediate ones too, assigns the ordinal and syncs the
    overlay ranges in one publication transaction that a failure rolls back.

These run real `Publisher.cycle()` publishes, so they want the isolated throwaway
Postgres (not the contended shared instance).
"""
from __future__ import annotations

import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.graph_store_internal.generation_ordinal import (  # noqa: E402
    mint_generation_ordinal,
    resolve_generation_ordinal,
)
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres

_DEPLOYMENT = "default"


def _add_fact(dsn: str, node_id: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    node_id=node_id,
                    subtype="doc",
                    attrs={"k": node_id},
                    source_record_id={"id": node_id},
                    source_revision={"v": 1},
                )
            ],
            source="default",
            strict_endpoints=True,
        )


def test_publishes_mint_strictly_increasing_ordinals(clean_runtime: str) -> None:
    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    gens = []
    for i in range(3):
        _add_fact(clean_runtime, f"doc:{i}:{suffix}")
        gens.append(str(Publisher(clean_runtime, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(clean_runtime, _DEPLOYMENT)).cycle()["generation_id"]))

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        db = ChronosGraphStore.from_connection(conn).context.db
        ords = [resolve_generation_ordinal(db, g) for g in gens]
    # Each published generation got an ordinal, strictly increasing in publish
    # order (no wall-clock dependence, no collisions).
    assert ords[0] < ords[1] < ords[2], ords
    assert len(set(ords)) == 3, ords


def test_first_mint_lazy_backfills_then_assigns_o1(clean_runtime: str) -> None:
    suffix = uuid.uuid4().hex[:8]
    branch = f"root_{suffix}"
    pre = [f"okg:default:branch:{branch}:gen:{suffix}{i:04d}:{suffix}" for i in range(3)]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        db, ctx = store.context.db, store.context
        # Branch unseeded: the first mint one-time backfills the 3 pre-existing
        # generations (incl. the one being minted) in causal order.
        out = mint_generation_ordinal(
            db, ctx, checkpoint_id=pre[2], branch=branch,
            deployment_name=_DEPLOYMENT, list_generations=lambda: list(pre),
        )
        assert out == 2, out
        assert [resolve_generation_ordinal(db, c) for c in pre] == [0, 1, 2]
        # Branch now seeded: the next mint is an O(1) assign that continues at 3.
        new = f"okg:default:branch:{branch}:gen:{suffix}9999:{suffix}"
        assert (
            mint_generation_ordinal(
                db, ctx, checkpoint_id=new, branch=branch,
                deployment_name=_DEPLOYMENT, list_generations=lambda: [*pre, new],
            )
            == 3
        )


def test_checkpoint_on_unresolvable_fork_is_nonfatal(clean_runtime: str) -> None:
    """A checkpoint on a structural fork with no recorded base (the quarantine
    shape) must NOT break the publish: the mint raises internally, is swallowed
    (non-fatal while unconsumed), and the fork records no ordinal -- and it is
    skipped CHEAPLY (the cheap fork check precedes any full list_generations scan)."""
    suffix = uuid.uuid4().hex[:8]
    fork = f"qfork_{suffix}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.create_branch(fork, from_branch="default", metadata={"kind": "okg_branch"})
        cp = store.checkpoint_generation(
            f"okg:default:branch:{fork}:gen:{suffix}0001:{suffix}",
            branch=fork,
            deployment_name=_DEPLOYMENT,
            status="published",
        )
        assert cp  # checkpoint_generation returned normally; the mint did not propagate
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.generation_ordinals WHERE branch_id = %s",
                (fork,),
            )
            assert cur.fetchone()[0] == 0, "unresolvable fork must record no ordinal"


def _publish(dsn: str) -> dict:
    return Publisher(
        dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)
    ).cycle()


def _branch_ordinals(dsn: str, branch: str = "default") -> dict[str, int]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('okg.generation_ordinals')")
        if cur.fetchone()[0] is None:
            return {}
        cur.execute(
            "SELECT checkpoint_id, ordinal FROM okg.generation_ordinals "
            "WHERE branch_id = %s ORDER BY ordinal",
            (branch,),
        )
        return {str(row[0]): int(row[1]) for row in cur.fetchall()}


def _synced_marker_ordinal(dsn: str, generation_id: str) -> int | None:
    """The ordinal the overlay marker of ``generation_id`` was synced at, or
    None when its overlays were not folded into the ranges."""
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT ordinal FROM okg.attribute_overlay_generations "
            "WHERE generation_id = %s AND namespace = 'default' AND ranges_synced",
            (str(generation_id),),
        )
        row = cur.fetchone()
    return None if row is None else int(row[0])


def _ranges_front(dsn: str, branch: str = "default") -> tuple[int, int, int]:
    """(range rows, highest synced marker ordinal, closed ranges) of the branch.

    A failed cycle that rolled back cleanly leaves this unchanged: it must not
    open or close a range, or mark a generation synced, at an ordinal no
    generation owns.
    """
    from okg.substrate.attributes import OVERLAY_RANGE_OPEN_ORD

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*), count(*) FILTER (WHERE to_ord < %s) "
            "FROM okg.attribute_overlay_ranges WHERE chronos_branch_id = %s",
            (OVERLAY_RANGE_OPEN_ORD, branch),
        )
        rows, closed = cur.fetchone()
        cur.execute(
            "SELECT COALESCE(max(ordinal), -1) FROM okg.attribute_overlay_generations "
            "WHERE chronos_branch_id = %s AND ranges_synced",
            (branch,),
        )
        synced = cur.fetchone()[0]
    return int(rows), int(synced), int(closed)


def _published_checkpoints(dsn: str) -> list[str]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        return [
            str(g.checkpoint_id)
            for g in store.list_generations(
                deployment_name=_DEPLOYMENT, branch="default", status="published"
            )
        ]


def _inject_checkpoint_failure(monkeypatch: pytest.MonkeyPatch, calls: list) -> None:
    def _boom(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        calls.append("checkpoint_generation")
        raise RuntimeError("injected checkpoint_generation failure")

    monkeypatch.setattr(ChronosGraphStore, "checkpoint_generation", _boom)


def _inject_backend_checkpoint_failure(
    monkeypatch: pytest.MonkeyPatch, calls: list
) -> None:
    # One level down: the Chronos backend fails while writing the checkpoint
    # row. The runtime's own failure handling runs here, inside the
    # publisher's transaction.
    from chronos_core.branching._interval_backend import _IntervalBackend

    def _boom(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        calls.append("chronos_create_checkpoint")
        raise RuntimeError("injected chronos_create_checkpoint failure")

    monkeypatch.setattr(_IntervalBackend, "create_checkpoint", _boom)


def _inject_ranges_failure(monkeypatch: pytest.MonkeyPatch, calls: list) -> None:
    # The overlay-range sync, which runs after the ordinal in the same
    # publication transaction.
    import okg.substrate.attributes as attributes

    def _boom(*args, **kwargs):  # type: ignore[no-untyped-def]
        calls.append("ranges")
        raise RuntimeError("injected ranges failure")

    monkeypatch.setattr(attributes, "sync_overlay_ranges_for_generation", _boom)


def test_every_publish_assigns_the_ordinal_and_syncs_the_ranges(
    clean_runtime: str,
) -> None:
    """PACT lane-s-storage-drop-copies: the ordinal and the overlay-range sync
    used to run only inside the interval-sync block, i.e. only on a final
    publish. Now every published root-branch generation -- an intermediate
    (non-final) one too -- gets the next ordinal and its marker is synced at
    it, and no copy table exists to write."""
    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
        for table in (
            "okg.attribute_effective_view",
            "okg.attribute_effective_view_intervals",
            "okg.attribute_versions",
        ):
            assert conn.execute(
                "SELECT to_regclass(%s) IS NULL", (table,)
            ).fetchone()[0], table

    _add_fact(clean_runtime, f"doc:0:{suffix}")
    first = _publish(clean_runtime)
    _add_fact(clean_runtime, f"doc:1:{suffix}")
    intermediate = Publisher(
        clean_runtime,
        _DEPLOYMENT,
        catalog_identity=bind_test_catalog_writer(clean_runtime, _DEPLOYMENT),
    ).cycle(native_view_refresh_final=False)
    _add_fact(clean_runtime, f"doc:2:{suffix}")
    third = _publish(clean_runtime)
    for result in (first, intermediate, third):
        assert result["status"] == "published", result
    ordinals = _branch_ordinals(clean_runtime)
    assert ordinals == {
        str(first["generation_id"]): 0,
        str(intermediate["generation_id"]): 1,
        str(third["generation_id"]): 2,
    }, ordinals
    for result, ordinal in ((first, 0), (intermediate, 1), (third, 2)):
        assert _synced_marker_ordinal(clean_runtime, result["generation_id"]) == ordinal

    from okg.substrate.attributes import overlay_range_parity

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert overlay_range_parity(conn, branch_id="default") == []


@pytest.mark.parametrize(
    ("failure_point", "inject"),
    [
        ("checkpoint_generation", _inject_checkpoint_failure),
        ("chronos_create_checkpoint", _inject_backend_checkpoint_failure),
        ("ranges", _inject_ranges_failure),
    ],
)
def test_failed_publish_leaves_no_stranded_ordinal(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
    inject,  # type: ignore[no-untyped-def]
) -> None:
    """Issue #1587: a failure between the ordinal and the checkpoint strands nothing.

    The ordinal, the overlay-range sync and the checkpoint commit in one
    publication transaction, so a failure at each point rolls the ordinal AND
    the ranges back, and the next successful publish takes the very next
    ordinal.
    """
    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])

    _add_fact(clean_runtime, f"doc:0:{suffix}")
    first = _publish(clean_runtime)
    assert first["status"] == "published", first
    # Discriminator: the publication transaction (ordinal + ranges) ran.
    assert _synced_marker_ordinal(clean_runtime, first["generation_id"]) == 0
    after_first = _branch_ordinals(clean_runtime)
    assert after_first == {str(first["generation_id"]): 0}, after_first
    front_before = _ranges_front(clean_runtime)
    published_before = _published_checkpoints(clean_runtime)
    assert published_before == [str(first["generation_id"])], published_before

    calls: list[str] = []
    with monkeypatch.context() as failing:
        inject(failing, calls)
        _add_fact(clean_runtime, f"doc:1:{suffix}")
        with pytest.raises(RuntimeError, match=f"injected {failure_point} failure"):
            _publish(clean_runtime)
    # The failure fired at the named point, not somewhere earlier.
    assert calls == [failure_point], calls

    # Nothing of the failed cycle survived: no ordinal, no range or synced
    # marker at its ordinal, no generation.
    assert _branch_ordinals(clean_runtime) == after_first, _branch_ordinals(
        clean_runtime
    )
    assert _ranges_front(clean_runtime) == front_before
    assert _published_checkpoints(clean_runtime) == published_before

    # The next successful publish takes ordinal 1 -- no gap -- and includes the
    # fact the failed cycle could not publish.
    second = _publish(clean_runtime)
    assert second["status"] == "published", second
    assert _synced_marker_ordinal(clean_runtime, second["generation_id"]) == 1
    after_second = _branch_ordinals(clean_runtime)
    assert after_second == {
        str(first["generation_id"]): 0,
        str(second["generation_id"]): 1,
    }, after_second

    # And the one after that continues densely: every ordinal names a real
    # published generation, each exactly once.
    _add_fact(clean_runtime, f"doc:2:{suffix}")
    third = _publish(clean_runtime)
    assert third["status"] == "published", third
    final = _branch_ordinals(clean_runtime)
    assert sorted(final.values()) == [0, 1, 2], final
    assert final[str(third["generation_id"])] == 2, final
    assert sorted(final) == sorted(_published_checkpoints(clean_runtime)), final


def _inject_verifier_failure(monkeypatch: pytest.MonkeyPatch, calls: list) -> None:
    # Handled by the caller-owned test itself: the precommit verifier raises
    # after the ordinal, the range sync and the checkpoint all ran.
    return None


@pytest.mark.parametrize(
    ("failure_point", "inject"),
    [
        ("checkpoint_generation", _inject_checkpoint_failure),
        ("chronos_create_checkpoint", _inject_backend_checkpoint_failure),
        ("ranges", _inject_ranges_failure),
        ("verifier", _inject_verifier_failure),
    ],
)
def test_caller_owned_failed_publish_leaves_no_stranded_ordinal(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
    inject,  # type: ignore[no-untyped-def]
) -> None:
    """Issue #1587 under a caller-verified publish (#2268).

    The publication transaction joins the caller-owned transaction instead of
    opening its own. A failure after the ordinal is assigned -- including the
    caller's own precommit verifier, which runs after the checkpoint -- must
    reach the caller's rollback with the ordinal, the ranges and the
    checkpoint, run the Chronos finalize hook exactly once as a rollback, and
    leave the next publish at the very next ordinal.
    """
    from chronos_core.branching._runtime import ChronosBranchContext

    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])

    _add_fact(clean_runtime, f"doc:0:{suffix}")
    first = _publish(clean_runtime)
    assert first["status"] == "published", first
    assert _synced_marker_ordinal(clean_runtime, first["generation_id"]) == 0
    after_first = _branch_ordinals(clean_runtime)
    assert after_first == {str(first["generation_id"]): 0}, after_first
    front_before = _ranges_front(clean_runtime)
    published_before = _published_checkpoints(clean_runtime)

    calls: list[str] = []
    finalized: list[bool] = []
    seen_inside: dict[str, int] = {}
    original_finalize = ChronosBranchContext.finalize_external_transaction

    def record_finalize(self, *, committed: bool) -> None:  # type: ignore[no-untyped-def]
        finalized.append(committed)
        original_finalize(self, committed=committed)

    def verifier(conn, generation_id: str) -> None:  # type: ignore[no-untyped-def]
        if failure_point != "verifier":
            return
        # Inside the caller's transaction the ordinal IS assigned.
        with conn.cursor() as cur:
            cur.execute(
                "SELECT ordinal FROM okg.generation_ordinals "
                "WHERE branch_id = 'default' AND checkpoint_id::text = %s",
                (generation_id,),
            )
            row = cur.fetchone()
        assert row is not None
        seen_inside["ordinal"] = int(row[0])
        calls.append("verifier")
        raise RuntimeError("injected verifier failure")

    _add_fact(clean_runtime, f"doc:1:{suffix}")
    with monkeypatch.context() as failing:
        inject(failing, calls)
        failing.setattr(
            ChronosBranchContext, "finalize_external_transaction", record_finalize
        )
        with psycopg.connect(clean_runtime, autocommit=False) as conn:
            with pytest.raises(
                RuntimeError, match=f"injected {failure_point} failure"
            ):
                Publisher(
                    clean_runtime,
                    _DEPLOYMENT,
                    catalog_identity=bind_test_catalog_writer(
                        clean_runtime, _DEPLOYMENT
                    ),
                ).cycle(conn=conn, atomic_precommit_verifier=verifier)
    assert calls == [failure_point], calls
    if failure_point == "verifier":
        assert seen_inside == {"ordinal": 1}, seen_inside
    # The caller's rollback ran the Chronos hook once, as a rollback.
    assert finalized == [False], finalized

    # Nothing of the failed cycle is visible after the caller's rollback.
    assert _branch_ordinals(clean_runtime) == after_first
    assert _ranges_front(clean_runtime) == front_before
    assert _published_checkpoints(clean_runtime) == published_before

    second = _publish(clean_runtime)
    assert second["status"] == "published", second
    assert _branch_ordinals(clean_runtime) == {
        str(first["generation_id"]): 0,
        str(second["generation_id"]): 1,
    }


def test_full_cycle_reasons_match_the_publisher_predicate() -> None:
    """Issue 3955: the publisher decides a full-count cycle as bool() of
    _full_cycle_reasons. Over every combination of the inputs, that equals the
    four-condition predicate (the recovery and integrity reasons are retired
    with the attribute copies)."""
    import itertools

    from okg.substrate.publisher.publisher import _full_cycle_reasons

    every = 4
    for has_previous, blocked, stamped, position in itertools.product(
        (False, True), (False, True), (False, True), (1, 2, 3, 4),
    ):
        original = (
            not has_previous or blocked or not stamped or position % every == 0
        )
        reasons = _full_cycle_reasons(
            has_previous=has_previous,
            block_intervened=blocked,
            has_cadence_stamp=stamped,
            cadence_position=position,
            self_heal_every=every,
        )
        assert bool(reasons) == original, (
            has_previous, blocked, stamped, position, reasons,
        )
    assert _full_cycle_reasons(
        has_previous=True,
        block_intervened=True,
        has_cadence_stamp=True,
        cadence_position=40,
        self_heal_every=20,
    ) == ["blocked_generation_between", "cadence_every_20"]
    assert _full_cycle_reasons(
        has_previous=False,
        block_intervened=True,
        has_cadence_stamp=False,
        cadence_position=1,
        self_heal_every=20,
    ) == ["first_publish"]
