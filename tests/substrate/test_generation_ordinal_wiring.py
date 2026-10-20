"""ivm-effective-attribute-view / wiring the generation ordinal into the publish path.

Proves the live-path behaviour of `mint_generation_ordinal` (called from
`checkpoint_generation`):
  * real publishes mint strictly-increasing ordinals,
  * the FIRST mint on a branch with pre-existing un-ordinaled generations
    one-time backfills them in causal order, then continues O(1),
  * the OKG_IVM_GENERATION_ORDINAL kill-switch disables minting.

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


def test_kill_switch_disables_minting(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OKG_IVM_GENERATION_ORDINAL", "0")
    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _add_fact(clean_runtime, f"doc:0:{suffix}")
    Publisher(clean_runtime, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(clean_runtime, _DEPLOYMENT)).cycle()
    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass('okg.generation_ordinals')")
        if cur.fetchone()[0] is None:
            count = 0
        else:
            cur.execute("SELECT count(*) FROM okg.generation_ordinals")
            count = cur.fetchone()[0]
    assert count == 0, count


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


def _snapshot_node_ids(dsn: str, generation_id: str) -> set[str]:
    """Node ids in the per-generation effective-attribute SNAPSHOT."""
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT target_id FROM okg.attribute_effective_view "
            "WHERE generation_id = %s AND target_kind = 'node'",
            (str(generation_id),),
        )
        return {str(row[0]) for row in cur.fetchall()}


def _checkpoint_metadata(dsn: str, generation_id: str) -> dict:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        return dict(store.get_generation(str(generation_id)).metadata or {})


def test_kill_switch_disables_minting_on_the_interval_path(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Issue #1514: OKG_IVM_GENERATION_ORDINAL=0 must also stop the INTERVAL arm.

    The kill switch gated only `ChronosGraphStore._maybe_mint_generation_ordinal`.
    The interval-tracked publish path calls `assign_generation_ordinal` directly
    inside the `effective_interval_view_sync` block, so it minted regardless and
    the switch was inert on every interval-tracked branch.

    The first publish here runs with the switch OFF (default: minting enabled)
    purely as a DISCRIMINATOR: only the interval-sync block writes
    `effective_interval_view_prune` into the checkpoint metadata, so its presence
    proves this deployment really takes the arm under test. Without that, the
    second publish minting nothing could just mean the interval arm never ran and
    the test would pass vacuously on unfixed code.
    """
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    monkeypatch.setenv("OKG_IVM_GENERATION_ORDINAL", "1")
    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])

    # TWO interval publishes before the flip, not one. After the second, the
    # prior snapshot is the interval arm's CHANGED-ONLY form, which is the shape
    # a real deployment is in when an operator reaches for the switch -- and it
    # drives a different carry-forward path in the materialize than a full prior
    # snapshot does.
    _add_fact(clean_runtime, f"doc:0:{suffix}")
    _publish(clean_runtime)
    _add_fact(clean_runtime, f"doc:1:{suffix}")
    first = _publish(clean_runtime)
    assert first["status"] == "published", first
    first_metadata = _checkpoint_metadata(clean_runtime, first["generation_id"])
    assert "effective_interval_view_prune" in first_metadata, sorted(first_metadata)
    after_first = _branch_ordinals(clean_runtime)
    assert sorted(after_first.values()) == [0, 1], after_first
    assert after_first[str(first["generation_id"])] == 1, after_first

    # Same branch, same deployment -- only the kill switch changes.
    monkeypatch.setenv("OKG_IVM_GENERATION_ORDINAL", "0")
    _add_fact(clean_runtime, f"doc:2:{suffix}")
    second = _publish(clean_runtime)
    assert second["status"] == "published", second

    # No new ordinal: the interval arm no longer mints behind the switch.
    assert _branch_ordinals(clean_runtime) == after_first, _branch_ordinals(
        clean_runtime
    )
    # And the interval arm itself stood down rather than syncing an
    # ordinal-keyed front with no ordinal to key it on.
    second_metadata = _checkpoint_metadata(clean_runtime, second["generation_id"])
    assert "effective_interval_view_prune" not in second_metadata, sorted(
        second_metadata
    )
    # Standing the sync down is only half the contract. The effective-attribute
    # materializer drops the snapshot carry-forward when it believes the
    # interval front carries the unchanged majority. It has to reach the SAME
    # conclusion as the publisher, or this generation's snapshot holds only the
    # one changed node while no interval rows describe the rest -- the
    # unchanged majority living in neither place. Assert the FULL snapshot.
    assert _snapshot_node_ids(clean_runtime, second["generation_id"]) == {
        f"doc:0:{suffix}",
        f"doc:1:{suffix}",
        f"doc:2:{suffix}",
    }, _snapshot_node_ids(clean_runtime, second["generation_id"])


def _interval_front(dsn: str, branch: str = "default") -> tuple[int, int, int]:
    """(rows, highest from_ord, closed rows) of the branch's interval table.

    A failed cycle that rolled back cleanly leaves this unchanged: it must not
    open or close any interval at an ordinal no generation owns.
    """
    from okg.substrate.attributes import (
        ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE,
        EFFECTIVE_INTERVAL_OPEN_ORD,
    )

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*), COALESCE(max(from_ord), -1), "
            f"count(*) FILTER (WHERE to_ord < %s) "
            f"FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} WHERE branch_id = %s",
            (EFFECTIVE_INTERVAL_OPEN_ORD, branch),
        )
        rows, max_from, closed = cur.fetchone()
    return int(rows), int(max_from), int(closed)


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


def _inject_prune_failure(monkeypatch: pytest.MonkeyPatch, calls: list) -> None:
    import okg.substrate.attributes as attributes

    def _boom(*args, **kwargs):  # type: ignore[no-untyped-def]
        calls.append("prune")
        raise RuntimeError("injected prune failure")

    monkeypatch.setattr(attributes, "prune_effective_interval_view_closed", _boom)


def _inject_integrity_failure(monkeypatch: pytest.MonkeyPatch, calls: list) -> None:
    # Both arms of the pre-checkpoint integrity write: a full anchor when the
    # cycle has no changeset, an incremental receipt when it has one.
    import okg.substrate.attribute_integrity as attribute_integrity

    def _boom(*args, **kwargs):  # type: ignore[no-untyped-def]
        calls.append("integrity")
        raise RuntimeError("injected integrity failure")

    monkeypatch.setattr(
        attribute_integrity, "finalize_full_attribute_integrity_anchor", _boom
    )
    monkeypatch.setattr(
        attribute_integrity, "finalize_incremental_attribute_integrity_receipt", _boom
    )


@pytest.mark.parametrize(
    ("failure_point", "inject"),
    [
        ("checkpoint_generation", _inject_checkpoint_failure),
        ("chronos_create_checkpoint", _inject_backend_checkpoint_failure),
        ("prune", _inject_prune_failure),
        ("integrity", _inject_integrity_failure),
    ],
)
def test_failed_interval_publish_leaves_no_stranded_ordinal(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
    inject,  # type: ignore[no-untyped-def]
) -> None:
    """Issue #1587: a failure between the ordinal and the checkpoint strands nothing.

    The interval arm used to COMMIT the ordinal (with the interval sync) before
    the retention prune, the integrity preflight and `checkpoint_generation`
    ran, each in its own transaction. A failure in any of them left an ordinal
    row with no generation behind it, and the next successful publish skipped
    that position. All of them now commit in one publication transaction, so a
    failure at each point rolls the ordinal AND the interval front back, and the
    next successful publish takes the very next ordinal.
    """
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])

    _add_fact(clean_runtime, f"doc:0:{suffix}")
    first = _publish(clean_runtime)
    assert first["status"] == "published", first
    first_metadata = _checkpoint_metadata(clean_runtime, first["generation_id"])
    # Discriminator: prove the interval arm (the one that mints early) ran.
    assert "effective_interval_view_prune" in first_metadata, sorted(first_metadata)
    after_first = _branch_ordinals(clean_runtime)
    assert after_first == {str(first["generation_id"]): 0}, after_first
    front_before = _interval_front(clean_runtime)
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

    # Nothing of the failed cycle survived: no ordinal, no interval rows at its
    # ordinal, no generation.
    assert _branch_ordinals(clean_runtime) == after_first, _branch_ordinals(
        clean_runtime
    )
    assert _interval_front(clean_runtime) == front_before
    assert _published_checkpoints(clean_runtime) == published_before

    # The next successful publish takes ordinal 1 -- no gap -- and includes the
    # fact the failed cycle could not publish.
    second = _publish(clean_runtime)
    assert second["status"] == "published", second
    assert "effective_interval_view_prune" in _checkpoint_metadata(
        clean_runtime, second["generation_id"]
    )
    after_second = _branch_ordinals(clean_runtime)
    assert after_second == {
        str(first["generation_id"]): 0,
        str(second["generation_id"]): 1,
    }, after_second
    assert _interval_front(clean_runtime)[1] == 1, _interval_front(clean_runtime)

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
    # after the ordinal, interval sync, prune, preflight and checkpoint all ran.
    return None


@pytest.mark.parametrize(
    ("failure_point", "inject"),
    [
        ("checkpoint_generation", _inject_checkpoint_failure),
        ("chronos_create_checkpoint", _inject_backend_checkpoint_failure),
        ("prune", _inject_prune_failure),
        ("integrity", _inject_integrity_failure),
        ("verifier", _inject_verifier_failure),
    ],
)
def test_caller_owned_failed_interval_publish_leaves_no_stranded_ordinal(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    failure_point: str,
    inject,  # type: ignore[no-untyped-def]
) -> None:
    """Issue #1587 under a caller-verified publish (#2268).

    The publication transaction joins the caller-owned transaction instead of
    opening its own. A failure after the ordinal is assigned -- including the
    caller's own precommit verifier, which runs after the checkpoint -- must
    reach the caller's rollback with the ordinal, the interval rows and the
    checkpoint, run the Chronos finalize hook exactly once as a rollback, and
    leave the next publish at the very next ordinal.
    """
    from chronos_core.branching._runtime import ChronosBranchContext

    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])

    _add_fact(clean_runtime, f"doc:0:{suffix}")
    first = _publish(clean_runtime)
    assert first["status"] == "published", first
    assert "effective_interval_view_prune" in _checkpoint_metadata(
        clean_runtime, first["generation_id"]
    )
    after_first = _branch_ordinals(clean_runtime)
    assert after_first == {str(first["generation_id"]): 0}, after_first
    front_before = _interval_front(clean_runtime)
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
    assert _interval_front(clean_runtime) == front_before
    assert _published_checkpoints(clean_runtime) == published_before

    second = _publish(clean_runtime)
    assert second["status"] == "published", second
    assert _branch_ordinals(clean_runtime) == {
        str(first["generation_id"]): 0,
        str(second["generation_id"]): 1,
    }
