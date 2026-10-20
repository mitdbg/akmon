"""ivm-effective-attribute-view / task.oracle-harness:

The byte-identical equivalence ORACLE that every later IVM task is checked
against. For each of four change shapes -- pure INSERT, attr UPDATE, RETRACT,
and a NO-OP -- the incrementally-maintained snapshot (carry-forward over the
prior generation, the path a real publish takes) must be BYTE-IDENTICAL,
row-for-row, to a FULL re-materialize of the same graph (``previous=None``,
every row merged from scratch). It also records the O(changed) merge work so a
later task (plumb-dirty-scope) can prove the residual staged/carried I/O drops
from O(total) to O(changed).

This is the correctness foundation: the representation change to shared
validity-interval rows (req.incremental-storage-representation) and explicit
retract/update (req.explicit-retract-and-update) must keep this oracle green.

SCOPE. The CARRY-FORWARD assertions here -- ``carried_rows`` and the
snapshot-vs-full byte-identity -- hold only on the SNAPSHOT arm, so the three
tests that make them pin ``OKG_IVM_INTERVAL_VIEW=0`` individually. That arm is
still live in production for a STRUCTURAL FORK (a branch with no recorded
ordinal keeps the full snapshot and reads fall back to it,
task.branch-fork-correctness) and whenever the flag is off. Under the shipped
default the interval cutover deliberately makes the per-generation snapshot
CHANGED-ONLY and serves reads from the interval front, so comparing the raw
snapshot table against a full re-materialize is not the contract there.

The pin is deliberately per-test, NOT module-wide: the ``merged``/``staged``
O(changed) assertions in ``test_oracle_records_o_total_staged_baseline`` and
``test_dirty_path_uses_keyed_interval_lookup`` are arm-independent and must keep
running at the shipped default, because they are the only place the cutover's
O(changed) staging claim is asserted against the default configuration.

Interval-arm counterparts for what the pinned tests give up
(test_effective_attribute_view_intervals.py):

  * insert/update/retract byte-identity   -> ::test_cutover_drops_carry_forward_delta_sync_oracle
                                             assertion (b), stab vs full re-materialize
  * changed-only snapshot (no carry)      -> same test, assertion (a)
  * overlay-only change not served stale,
    and not OVER-merged                   -> ::test_overlay_only_change_not_served_stale

``::test_interval_stab_equals_snapshot_with_time_travel`` is NOT in that list: it
pins the flag off itself and drives the sync in full mode, so it is a
snapshot-arm test despite the name.
"""

from __future__ import annotations

import uuid

import pytest

psycopg = pytest.importorskip("psycopg")
import okg.substrate.attributes as attributes_module  # noqa: E402

from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

_MATERIALIZE_INSTRUMENTATION = attributes_module._MATERIALIZE_INSTRUMENTATION
_reset_materialize_instrumentation = attributes_module._reset_materialize_instrumentation
ATTRIBUTE_EFFECTIVE_VIEW_TABLE = attributes_module.ATTRIBUTE_EFFECTIVE_VIEW_TABLE
DirtyTargets = attributes_module.DirtyTargets
delete_effective_attribute_view_for_generation = (
    attributes_module.delete_effective_attribute_view_for_generation
)
materialize_effective_attribute_view_for_generation = (
    attributes_module.materialize_effective_attribute_view_for_generation
)
materialize_effective_attributes_for_generation = (
    attributes_module.materialize_effective_attributes_for_generation
)
record_attribute_observation = attributes_module.record_attribute_observation


pytestmark = pytest.mark.postgres

_N = 40
_DEPLOYMENT = "default"


def _pin_snapshot_arm(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pin the SNAPSHOT arm for one test (see the module docstring's SCOPE note).

    Call this from a test that asserts ``carried_rows`` or compares the raw
    ``attribute_effective_view`` against a full re-materialize. With the interval
    arm on -- the shipped default -- a dirty-driven materialize deliberately
    drops the O(total) carry-forward and leaves a CHANGED-ONLY snapshot, so those
    reads would compare a delta against a full rebuild. Same pin, same reason, as
    test_effective_attribute_view_incremental.py and test_publisher_native_views.py.

    Do NOT hoist this into an autouse fixture: the tests that assert only
    ``merged``/``staged`` are arm-independent and are the sole remaining coverage
    of the O(changed) staging claim at the shipped default.
    """
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "0")


def _snapshot_rows(conn, gen: str) -> dict[tuple[str, str], tuple]:
    """Generation-agnostic snapshot: keyed by (target_kind, target_id) -> the
    materialized tuple (merged attrs, raw, overlay_count, ...), so two
    materializations of the same graph under different generation_ids compare
    directly."""
    # schema-interval-narrow-split: attrs/raw_attrs materialize through the
    # version-store join; the compared tuples are unchanged by the split.
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT v.target_kind, v.target_id, av.attrs, av.raw_attrs, "
            f"v.overlay_count, "
            f"v.raw_text_hash, v.subtype, v.src, v.dst, v.edge_type "
            f"FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} v "
            "JOIN okg.attribute_versions av "
            "  ON av.target_kind = v.target_kind "
            " AND av.target_id = v.target_id "
            " AND av.attrs_hash = v.attrs_hash "
            "WHERE v.generation_id = %s AND v.namespace = 'default'",
            (gen,),
        )
        return {
            (r[0], r[1]): (r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9])
            for r in cur.fetchall()
        }


def _seed_gen1(clean_runtime: str) -> tuple[str, str]:
    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
        store = ChronosGraphStore.from_connection(conn)
        facts = [
            NodeFact(
                node_id=f"doc:{i}:{suffix}",
                subtype="doc",
                attrs={"k": f"v{i % 7}"},
                source_record_id={"id": f"doc:{i}:{suffix}"},
                source_revision={"v": 1},
            )
            for i in range(_N)
        ]
        store.apply_facts(facts, source="default", strict_endpoints=True)
    g1 = str(Publisher(clean_runtime, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(clean_runtime, _DEPLOYMENT)).cycle()["generation_id"])
    return g1, suffix


def _apply_mutation(clean_runtime: str, suffix: str, scenario: str) -> dict:
    """Mutate the live graph for the next publish; return the oracle expectations
    for that scenario (merged-row count, and the target that must vanish)."""
    facts: list[NodeFact] = []
    expect = {"merged": 0, "absent": None}
    if scenario == "insert":
        facts = [
            NodeFact(
                node_id=f"doc:new{i}:{suffix}",
                subtype="doc",
                attrs={"k": "new"},
                source_record_id={"id": f"doc:new{i}:{suffix}"},
                source_revision={"v": 1},
            )
            for i in range(5)
        ]
        expect["merged"] = 5  # only the 5 new targets pay the merge
    elif scenario == "update":
        facts = [
            NodeFact(
                node_id=f"doc:0:{suffix}",
                subtype="doc",
                attrs={"k": "CHANGED"},
                source_record_id={"id": f"doc:0:{suffix}"},
                source_revision={"v": 2},
            )
        ]
        expect["merged"] = 1  # only the one changed target is re-merged
    elif scenario == "retract":
        facts = [
            NodeFact(
                node_id=f"doc:0:{suffix}",
                subtype="doc",
                attrs={},
                source_record_id={"id": f"doc:0:{suffix}"},
                source_revision={"v": 2},
                op="R",
            )
        ]
        expect["merged"] = 0  # nothing new to merge; the target just vanishes
        expect["absent"] = ("node", f"doc:0:{suffix}")
    elif scenario == "noop":
        pass  # no facts; handled via the no-publish pseudo-generation path
    else:  # pragma: no cover - guard
        raise AssertionError(scenario)
    if facts:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            store = ChronosGraphStore.from_connection(conn)
            store.apply_facts(facts, source="default", strict_endpoints=True)
    return expect


def _assert_byte_identical(incr: dict, full: dict) -> None:
    assert set(incr) == set(full), set(incr) ^ set(full)
    for key in full:
        assert incr[key] == full[key], (key, incr[key], full[key])


@pytest.mark.parametrize("scenario", ["insert", "update", "retract", "noop"])
def test_incremental_materialize_equals_full_oracle(
    clean_runtime: str, scenario: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    if scenario != "noop":
        # The three mutating scenarios publish through the dirty-driven path,
        # where byte-identity against a full rebuild is a snapshot-arm contract.
        # The noop scenario passes `dirty_targets=None`, so it never takes the
        # interval arm's changed-only branch and stays arm-independent -- leave it
        # running at the shipped default.
        _pin_snapshot_arm(monkeypatch)
    g1, suffix = _seed_gen1(clean_runtime)
    expect = _apply_mutation(clean_runtime, suffix, scenario)

    if scenario == "noop":
        # No graph change -> a real publish would noop-reuse gen1's snapshot, so
        # exercise the materializer directly: incremental (carry-forward over g1)
        # vs full (from scratch) on the SAME unchanged graph.
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            full_res = materialize_effective_attribute_view_for_generation(
                conn, generation_id=f"{g1}:oracle_full", previous_generation_id=None
            )
            assert full_res["materialized"] is True, full_res
            full = _snapshot_rows(conn, f"{g1}:oracle_full")
            _reset_materialize_instrumentation()
            incr_res = materialize_effective_attribute_view_for_generation(
                conn, generation_id=f"{g1}:oracle_incr", previous_generation_id=g1
            )
            assert incr_res["materialized"] is True, incr_res
            merged = _MATERIALIZE_INSTRUMENTATION["merged_rows"]
            carried = _MATERIALIZE_INSTRUMENTATION["carried_rows"]
            incr = _snapshot_rows(conn, f"{g1}:oracle_incr")
        assert merged == 0, (merged, carried)
        assert carried >= _N, (carried, merged)
    else:
        # Real publish path: gen2's snapshot is built incrementally (carry-forward
        # over gen1). Compare it to a FULL re-materialize of the now-current graph.
        _reset_materialize_instrumentation()
        g2 = str(Publisher(clean_runtime, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(clean_runtime, _DEPLOYMENT)).cycle()["generation_id"])
        merged = _MATERIALIZE_INSTRUMENTATION["merged_rows"]
        staged = _MATERIALIZE_INSTRUMENTATION["staged_rows"]
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            incr = _snapshot_rows(conn, g2)  # captured BEFORE the full re-mat prunes
            full_res = materialize_effective_attribute_view_for_generation(
                conn, generation_id=f"{g2}:oracle_full", previous_generation_id=None
            )
            assert full_res["materialized"] is True, full_res
            full = _snapshot_rows(conn, f"{g2}:oracle_full")
        # O(changed): only the changed/new targets paid the merge AND were staged
        # (the dirty path stages O(changed), not the full chronos-visible set of N).
        assert merged == expect["merged"], (scenario, merged, expect["merged"])
        assert staged == expect["merged"], (scenario, staged, expect["merged"], _N)

    # The oracle: incremental carry-forward is byte-identical to a full rebuild.
    _assert_byte_identical(incr, full)

    if expect["absent"] is not None:
        assert expect["absent"] not in incr, (expect["absent"], "present in incr")
        assert expect["absent"] not in full, (expect["absent"], "present in full")


def test_oracle_records_o_total_staged_baseline(clean_runtime: str) -> None:
    """Baseline the residual O(total) work the snapshot model pays even when
    nothing changes: a carry-forward over an unchanged graph still INSERTs one
    row per live target (carried_rows == N), merging zero. task.plumb-dirty-scope
    must drive this carried/staged count from O(total) toward O(changed); this
    test records today's value so that improvement is measurable, not asserted
    away."""
    g1, _ = _seed_gen1(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_materialize_instrumentation()
        materialize_effective_attribute_view_for_generation(
            conn, generation_id=f"{g1}:baseline", previous_generation_id=g1
        )
        merged = _MATERIALIZE_INSTRUMENTATION["merged_rows"]
        carried = _MATERIALIZE_INSTRUMENTATION["carried_rows"]
    # Today: zero merge CPU but O(total) carried INSERTs -- the floor IVM removes.
    assert merged == 0, merged
    assert carried >= _N, (carried, _N)


def test_dirty_path_noop_stages_zero(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """task.plumb-dirty-scope: a dirty-driven materialize with an EMPTY changeset
    stages ZERO rows (no full chronos-visible scan), carries the whole prior
    snapshot forward, and is byte-identical to a full re-materialize. This is the
    O(changed) staging win the full-scan path could not deliver."""
    _pin_snapshot_arm(monkeypatch)  # asserts carried_rows + byte-identity
    g1, _ = _seed_gen1(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        full_res = materialize_effective_attribute_view_for_generation(
            conn, generation_id=f"{g1}:full", previous_generation_id=None
        )
        assert full_res["materialized"] is True, full_res
        full = _snapshot_rows(conn, f"{g1}:full")
        _reset_materialize_instrumentation()
        materialize_effective_attribute_view_for_generation(
            conn,
            generation_id=f"{g1}:dirty",
            previous_generation_id=g1,
            dirty_targets=DirtyTargets(node_ids=frozenset(), edge_ids=frozenset()),
        )
        staged = _MATERIALIZE_INSTRUMENTATION["staged_rows"]
        carried = _MATERIALIZE_INSTRUMENTATION["carried_rows"]
        merged = _MATERIALIZE_INSTRUMENTATION["merged_rows"]
        dirty = _snapshot_rows(conn, f"{g1}:dirty")
    assert staged == 0, staged          # O(changed): nothing staged for a no-op
    assert merged == 0, merged
    assert carried >= _N, (carried, _N)  # whole prior snapshot carried forward
    _assert_byte_identical(dirty, full)


def test_dirty_path_catches_overlay_only_change(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """task.plumb-dirty-scope CRITICAL GUARD: an OVERLAY-only change (an enricher
    attribute observation, raw graph untouched) is INVISIBLE to the chronos dirty
    scope. The dirty-driven materialize must still re-merge that target via the
    per-generation overlay gen-diff union -- never carry its stale prior row. Drive
    the dirty path with an EMPTY raw changeset against a generation whose overlay
    snapshot changed one node, and require byte-identity with a full re-materialize
    of the SAME overlay; if the overlay union were missing, the node would be
    carried stale and this would fail (a silent-correctness P0).

    Interval-arm counterpart: ::test_overlay_only_change_not_served_stale."""
    _pin_snapshot_arm(monkeypatch)  # asserts byte-identity against a full rebuild
    g1, suffix = _seed_gen1(clean_runtime)
    enriched = f"doc:1:{suffix}"
    g2 = f"{g1}:ov"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Build g2's overlay snapshot with a NEW overlay attr on one node; raw
        # graph is untouched, so this node is NOT in any chronos dirty scope.
        record_attribute_observation(
            conn, target_kind="node", target_id=enriched, generation_id=g2,
            attr_key="enriched", value="yes", producer="test",
        )
        materialize_effective_attributes_for_generation(conn, generation_id=g2)

        # DIRTY path: empty raw changeset, prev=g1. Correct only if the overlay
        # gen-diff union pulls `enriched` into the merge set.
        _reset_materialize_instrumentation()
        materialize_effective_attribute_view_for_generation(
            conn, generation_id=g2, previous_generation_id=g1,
            dirty_targets=DirtyTargets(node_ids=frozenset(), edge_ids=frozenset()),
        )
        merged = _MATERIALIZE_INSTRUMENTATION["merged_rows"]
        dirty = _snapshot_rows(conn, g2)

        # FULL path over the SAME g2 overlay (from scratch).
        delete_effective_attribute_view_for_generation(conn, g2)
        full_res = materialize_effective_attribute_view_for_generation(
            conn, generation_id=g2, previous_generation_id=None
        )
        assert full_res["materialized"] is True, full_res
        full = _snapshot_rows(conn, g2)

    # Exactly the overlay-changed node was re-merged (raw-dirty was empty).
    assert merged == 1, merged
    _assert_byte_identical(dirty, full)
    # The overlay was applied, not carried stale.
    assert dirty[("node", enriched)][0].get("enriched") == "yes", dirty[("node", enriched)]


def test_dirty_path_uses_keyed_interval_lookup(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A dirty id must not force the full Chronos visible-read subquery.

    Poison the old fallback SQL while preserving the real branch point. The
    dirty path should stage by physical interval key lookup instead and still
    materialize only the changed row.
    """

    g1, suffix = _seed_gen1(clean_runtime)
    _apply_mutation(clean_runtime, suffix, "update")
    target = f"doc:0:{suffix}"
    original_visible = attributes_module._chronos_visible_read_sql

    def poisoned_visible_sql(session, table):
        visible = original_visible(session, table)
        if visible is None:
            return None
        if table != "okg.graph_nodes":
            return visible
        return "SELECT * FROM missing_visible_sql_for_keyed_staging", visible[1]

    monkeypatch.setattr(
        attributes_module,
        "_chronos_visible_read_sql",
        poisoned_visible_sql,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_materialize_instrumentation()
        res = materialize_effective_attribute_view_for_generation(
            conn,
            generation_id=f"{g1}:keyed",
            previous_generation_id=g1,
            dirty_targets=DirtyTargets(
                node_ids=frozenset([target]),
                edge_ids=frozenset(),
            ),
        )
        rows = _snapshot_rows(conn, f"{g1}:keyed")
        staged = _MATERIALIZE_INSTRUMENTATION["staged_rows"]
        merged = _MATERIALIZE_INSTRUMENTATION["merged_rows"]

    assert res["materialized"] is True, res
    assert staged == 1, staged
    assert merged == 1, merged
    assert rows[("node", target)][0]["k"] == "CHANGED"
