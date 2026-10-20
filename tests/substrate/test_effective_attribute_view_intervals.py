"""ivm-effective-attribute-view / task.interval-row-representation.

The shared validity-interval table (rows carrying [from_ord, to_ord) over
branch-scoped generation ordinals) is the O(changed)-storage replacement for the
per-generation snapshot. This is its ORACLE: a STAB at ordinal(G) must be
byte-identical to the full snapshot at generation G (the same equivalence the
snapshot oracle proves, now for shared rows), AND it must

  * preserve TIME TRAVEL -- an older generation's stab is unchanged after later
    generations close/reopen intervals,
  * write O(changed) rows per generation (a no-op writes ZERO), not O(live),
  * be idempotent (a re-sync of the same generation writes nothing).

These run real Publisher.cycle() publishes (which write the snapshot and mint the
ordinal), then fold each generation into the interval table and stab it.
"""
from __future__ import annotations

import importlib
import json
import uuid

import pytest

psycopg = pytest.importorskip("psycopg")
from okg.substrate.db.connection import connect as db_connect  # noqa: E402
from psycopg.rows import dict_row  # noqa: E402
from psycopg.types.json import Jsonb  # noqa: E402

from okg.substrate.attributes import (  # noqa: E402
    ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE,
    ATTRIBUTE_EFFECTIVE_VIEW_TABLE,
    EFFECTIVE_INTERVAL_OPEN_ORD,
    EFFECTIVE_VIEW_RETENTION_GENERATIONS,
    DirtyTargets,
    _INTERVAL_READ_COLS,
    _close_dirty_effective_intervals,
    backfill_effective_interval_raw_attrs_dedup,
    backfill_effective_interval_view_for_generation,
    classify_effective_attribute_recovery,
    count_prunable_closed_intervals,
    effective_interval_stab_rows,
    effective_snapshot_read_rows,
    ensure_attribute_schema,
    materialize_effective_attribute_view_for_generation,
    materialize_effective_attributes_for_generation,
    prune_effective_attribute_view_to_recent,
    prune_effective_interval_view_backlog,
    prune_effective_interval_view_closed,
    reanchor_effective_interval_view_for_generation,
    repair_effective_attribute_interval_view,
    require_effective_interval_backfill_precondition,
    record_attribute_observation,
    sync_effective_interval_view_for_generation,
)
from okg.substrate.attribute_integrity import (  # noqa: E402
    ATTRIBUTE_INTEGRITY_RECEIPT_TABLE,
    ATTRIBUTE_INTEGRITY_STATE_TABLE,
    ATTRIBUTE_INTEGRITY_TRANSITION_TABLE,
    clear_attribute_integrity_transition,
    prepare_attribute_integrity_transition,
)
from okg.substrate.attributes import (  # noqa: E402
    _MATERIALIZE_INSTRUMENTATION,
    _reset_materialize_instrumentation,
)
from okg.substrate.graph_store_internal.generation_ordinal import (  # noqa: E402
    assign_generation_ordinal,
    is_interval_tracked_branch,
    resolve_generation_ordinal_row,
)
from okg.substrate.errors import EffectiveIntervalBackfillRequired  # noqa: E402
from okg.substrate.db.migrate import migrate  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

pytestmark = pytest.mark.postgres

_DEPLOYMENT = "default"

def _db(conn: psycopg.Connection):
    return ChronosGraphStore.from_connection(conn, ensure_schema=False).context.db


def _snapshot_read(conn: psycopg.Connection, gen: str) -> dict[tuple[str, str], tuple]:
    """The per-generation snapshot rows, in _INTERVAL_READ_COLS order, keyed by
    (target_kind, target_id) -- the ground truth the interval stab must match.
    schema-interval-narrow-split: payloads are materialized through the
    version-store join (effective_snapshot_read_rows)."""
    out: dict[tuple[str, str], tuple] = {}
    for tk in ("node", "edge"):
        for row in effective_snapshot_read_rows(
            conn, generation_id=gen, target_kind=tk
        ):
            out[(tk, row[0])] = tuple(row[1:])
    return out


def _stab(conn: psycopg.Connection, db, gen: str) -> dict[tuple[str, str], tuple]:
    out: dict[tuple[str, str], tuple] = {}
    for tk in ("node", "edge"):
        for row in effective_interval_stab_rows(
            conn, db, generation_id=gen, target_kind=tk
        ):
            out[(tk, row[0])] = tuple(row[1:])
    return out


def _assert_stab_equals_snapshot(conn, db, gen: str) -> dict[tuple[str, str], tuple]:
    stab = _stab(conn, db, gen)
    snap = _snapshot_read(conn, gen)
    assert set(stab) == set(snap), set(stab) ^ set(snap)
    for key in snap:
        assert stab[key] == snap[key], (key, stab[key], snap[key])
    return stab


def _facts(suffix, ids, attrs, *, op=None, rev=1):
    out = []
    for i in ids:
        kw = {"op": op} if op else {}
        out.append(
            NodeFact(
                node_id=f"doc:{i}:{suffix}",
                subtype="doc",
                attrs=attrs,
                source_record_id={"id": f"doc:{i}:{suffix}"},
                source_revision={"v": rev},
                **kw,
            )
        )
    return out


def _edge_facts(suffix, pairs, attrs, *, op=None, rev=1):
    out = []
    for src_i, dst_i in pairs:
        kw = {"op": op} if op else {}
        src = f"doc:{src_i}:{suffix}"
        dst = f"doc:{dst_i}:{suffix}"
        out.append(
            EdgeFact(
                src=src,
                dst=dst,
                edge_type="links",
                attrs=attrs,
                source_record_id={"id": f"{src}->{dst}:links"},
                source_revision={"v": rev},
                **kw,
            )
        )
    return out


def _apply(dsn: str, facts) -> None:
    if not facts:
        return
    with db_connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            facts, source="default", strict_endpoints=True
        )


def _insert_version(
    cur,
    target_kind: str,
    target_id: str,
    attrs: dict,
    raw_attrs: dict | None = None,
) -> bytes:
    """Insert a version-store row for a fixture target, hashing through THE one
    in-database hash site (schema-interval-narrow-split), and return the hash."""
    cur.execute(
        "SELECT okg.attribute_content_hash(%s::jsonb, %s::jsonb)",
        (Jsonb(attrs), Jsonb(raw_attrs) if raw_attrs is not None else None),
    )
    attrs_hash = bytes(cur.fetchone()[0])
    cur.execute(
        "INSERT INTO okg.attribute_versions "
        "(target_kind, target_id, attrs_hash, attrs, raw_attrs) "
        "VALUES (%s, %s, %s, %s::jsonb, %s::jsonb) "
        "ON CONFLICT (target_kind, target_id, attrs_hash) DO NOTHING",
        (
            target_kind,
            target_id,
            attrs_hash,
            Jsonb(attrs),
            Jsonb(raw_attrs) if raw_attrs is not None else None,
        ),
    )
    return attrs_hash


def test_interval_stab_equals_snapshot_with_time_travel(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "0")
    dsn = clean_runtime
    suffix = uuid.uuid4().hex[:8]

    # g1: insert 8 docs -> 8 intervals opened.
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, range(8), {"k": "v"}))
    g1 = str(Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()["generation_id"])
    with db_connect(dsn, autocommit=True) as conn:
        db = _db(conn)
        r1 = sync_effective_interval_view_for_generation(conn, db, generation_id=g1)
        assert r1["rows_written"] == 8, r1
        snap1 = _assert_stab_equals_snapshot(conn, db, g1)

    # g2: update doc:0 + insert doc:100,101 -> close 1, open 1, open 2 = 4 (NOT 10).
    _apply(dsn, _facts(suffix, [0], {"k": "CHANGED"}, rev=2))
    _apply(dsn, _facts(suffix, [100, 101], {"k": "new"}))
    g2 = str(Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()["generation_id"])
    with db_connect(dsn, autocommit=True) as conn:
        db = _db(conn)
        r2 = sync_effective_interval_view_for_generation(conn, db, generation_id=g2)
        assert r2["rows_written"] == 4, r2  # O(changed), not O(live)
        _assert_stab_equals_snapshot(conn, db, g2)
        # TIME TRAVEL: g1's stab is unchanged by g2's close/reopen.
        assert _stab(conn, db, g1) == snap1

    # g3: retract doc:1 -> close 1, open 0.
    _apply(dsn, _facts(suffix, [1], {}, op="R", rev=2))
    g3 = str(Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()["generation_id"])
    with db_connect(dsn, autocommit=True) as conn:
        db = _db(conn)
        r3 = sync_effective_interval_view_for_generation(conn, db, generation_id=g3)
        assert r3["rows_written"] == 1, r3
        s3 = _assert_stab_equals_snapshot(conn, db, g3)
        assert ("node", f"doc:1:{suffix}") not in s3  # retracted: absent at g3
        # ...but still present at g2 (time travel preserved across a retract).
        assert ("node", f"doc:1:{suffix}") in _stab(conn, db, g2)

        # Idempotent + no-op floor: re-syncing g3 writes ZERO rows.
        r3b = sync_effective_interval_view_for_generation(conn, db, generation_id=g3)
        assert r3b["rows_written"] == 0, r3b
        _assert_stab_equals_snapshot(conn, db, g3)


def test_publish_with_flag_auto_syncs_intervals(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """task.interval-row-representation (publish wiring): with OKG_IVM_INTERVAL_VIEW
    on, a real Publisher.cycle() folds the snapshot into the interval table ITSELF
    (no manual sync) -- proving the pre-checkpoint publish-path hook runs."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    with db_connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(clean_runtime, _facts(suffix, range(6), {"k": "v"}))
    g1 = str(Publisher(clean_runtime, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(clean_runtime, _DEPLOYMENT)).cycle()["generation_id"])

    with db_connect(clean_runtime, autocommit=True) as conn:
        db = _db(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.attribute_effective_view_intervals"
            )
            assert cur.fetchone()[0] == 6, "publish did not auto-populate intervals"
        # The auto-synced intervals stab byte-identical to the published snapshot.
        _assert_stab_equals_snapshot(conn, db, g1)


def test_interval_sync_failure_never_exposes_published_generation(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Interval rows and ordinals are prepared atomically before checkpoint visibility."""
    import okg.substrate.attributes as attributes_module

    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    with db_connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        published_before = {
            generation.checkpoint_id
            for generation in store.list_generations(
                deployment_name=_DEPLOYMENT,
                branch="default",
                status="published",
            )
        }
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.generation_ordinals")
            ordinals_before = int(cur.fetchone()[0])
            cur.execute(f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE}")
            intervals_before = int(cur.fetchone()[0])

    _apply(clean_runtime, _facts(suffix, range(3), {"k": "v"}))
    real_sync = attributes_module.reanchor_effective_interval_view_for_generation

    def sync_then_fail(*args, **kwargs):
        real_sync(*args, **kwargs)
        raise RuntimeError("injected interval sync failure")

    monkeypatch.setattr(
        attributes_module,
        "reanchor_effective_interval_view_for_generation",
        sync_then_fail,
    )
    with pytest.raises(RuntimeError, match="injected interval sync failure"):
        Publisher(clean_runtime, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(clean_runtime, _DEPLOYMENT)).cycle()

    with db_connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        published_after = {
            generation.checkpoint_id
            for generation in store.list_generations(
                deployment_name=_DEPLOYMENT,
                branch="default",
                status="published",
            )
        }
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.generation_ordinals")
            assert int(cur.fetchone()[0]) == ordinals_before
            cur.execute(f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE}")
            assert int(cur.fetchone()[0]) == intervals_before
    assert published_after == published_before


def test_publish_defaults_to_interval_view(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The dogfood-safe default is the interval materializer; deployments must
    opt out explicitly with OKG_IVM_INTERVAL_VIEW=0."""
    monkeypatch.delenv("OKG_IVM_INTERVAL_VIEW", raising=False)
    suffix = uuid.uuid4().hex[:8]
    with db_connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(clean_runtime, _facts(suffix, range(2), {"k": "v"}))
    result = Publisher(clean_runtime, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(clean_runtime, _DEPLOYMENT)).cycle()
    g1 = str(result["generation_id"])
    # Issue #2941: the integrity receipt pass under the lock is timed.
    assert "attribute_integrity_receipt_preflight" in result["timings_ms"]

    with db_connect(clean_runtime, autocommit=True) as conn:
        db = _db(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.attribute_effective_view_intervals"
            )
            assert cur.fetchone()[0] == 2
        _assert_stab_equals_snapshot(conn, db, g1)


def test_publish_cardinality_source_agrees(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Cutover cardinality keystone: with the flag on, the published count source is
    the live Chronos graph count, which equals the count of OPEN intervals AND (while
    carry-forward is still on) the snapshot row count. Proves the cardinality source
    is correct before the snapshot write is dropped."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    n = 7
    with db_connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(clean_runtime, _facts(suffix, range(n), {"k": "v"}))
    res = Publisher(clean_runtime, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(clean_runtime, _DEPLOYMENT)).cycle()
    g = str(res["generation_id"])

    with db_connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.attribute_effective_view_intervals "
            "WHERE target_kind = 'node' AND to_ord = %s",
            (EFFECTIVE_INTERVAL_OPEN_ORD,),
        )
        interval_open_n = cur.fetchone()[0]
        cur.execute(
            f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
            "WHERE generation_id = %s AND target_kind = 'node' AND namespace = 'default'",
            (g,),
        )
        snap_n = cur.fetchone()[0]
    # The publisher stamped node_count from the live Chronos graph count (flag on).
    # It must equal the OPEN-interval count and (while carry-forward is still on) the
    # snapshot row count -- the cardinality sources agree.
    assert int(res["node_count"]) == n, res.get("node_count")
    assert interval_open_n == n, interval_open_n
    assert snap_n == n, snap_n


def test_cutover_drops_carry_forward_delta_sync_oracle(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The cutover WIN + its correctness, end to end. With the flag on a real publish
    drops the O(total) carry-forward (the snapshot goes CHANGED-ONLY) and sync runs
    in delta mode. Across insert/update/RETRACT the interval stab must STILL equal a
    full re-materialize, the retracted target must be GONE (not silently kept open),
    and the published cardinality must stay the live count."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])

    # g1: 8 new docs.
    _apply(dsn, _facts(suffix, range(8), {"k": "v"}))
    g1 = str(Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()["generation_id"])
    # g2: update doc:0, insert doc:100/101, RETRACT doc:1 -> 9 live (8 - 1 + 2).
    _apply(dsn, _facts(suffix, [0], {"k": "CHANGED"}, rev=2))
    _apply(dsn, _facts(suffix, [100, 101], {"k": "new"}))
    _apply(dsn, _facts(suffix, [1], {}, op="R", rev=2))
    res2 = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    g2 = str(res2["generation_id"])
    doc1 = ("node", f"doc:1:{suffix}")

    with db_connect(dsn, autocommit=True) as conn:
        db = _db(conn)
        # (a) THE WIN: g2's snapshot is CHANGED-ONLY -- 3 changed/new rows (update +
        # 2 inserts; the retract writes none), NOT the 9 live targets a carry-forward
        # snapshot would hold.
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
                "WHERE generation_id = %s AND target_kind = 'node' "
                "AND namespace = 'default'",
                (g2,),
            )
            assert cur.fetchone()[0] == 3, "carry-forward not dropped"

        # (b) ORACLE: the interval stab at g2 == a FULL re-materialize of g2.
        assert materialize_effective_attribute_view_for_generation(
            conn, generation_id=f"{g2}:full", previous_generation_id=None
        )["materialized"]
        full_rows = _snapshot_read(conn, f"{g2}:full")
        stab2 = _stab(conn, db, g2)
        assert set(stab2) == set(full_rows), set(stab2) ^ set(full_rows)
        for key in full_rows:
            assert stab2[key] == full_rows[key], (key, stab2[key], full_rows[key])

        # (c) RETRACT did not silently leave doc:1 open: gone at g2, kept at g1.
        assert doc1 not in stab2, "retracted target still live at g2 (silent stale!)"
        assert doc1 in _stab(conn, db, g1), "time travel broken: doc:1 lost at g1"

        # (d) CARDINALITY stays the live count (9), from Chronos == open intervals.
        assert int(res2["node_count"]) == 9, res2.get("node_count")
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.attribute_effective_view_intervals "
                "WHERE target_kind = 'node' AND to_ord = %s",
                (EFFECTIVE_INTERVAL_OPEN_ORD,),
            )
            assert cur.fetchone()[0] == 9, "open-interval count != live count"


def test_flag_off_after_changed_only_snapshot_rebuilds_full_snapshot(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A process running with interval mode off must not carry forward a prior
    changed-only snapshot. That state can happen after a local flag flip or a
    worker restart with stale env; snapshot fallback needs a full snapshot again.
    """

    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])

    _apply(dsn, _facts(suffix, range(8), {"k": "v"}))
    Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    _apply(dsn, _facts(suffix, [0], {"k": "changed"}, rev=2))
    g2 = str(Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()["generation_id"])

    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
            "WHERE generation_id = %s AND target_kind = 'node' "
            "AND namespace = 'default'",
            (g2,),
        )
        assert cur.fetchone()[0] == 1, "setup did not create a changed-only snapshot"

    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "0")
    _apply(dsn, _facts(suffix, [1], {"k": "changed2"}, rev=2))
    g3 = str(Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()["generation_id"])

    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
            "WHERE generation_id = %s AND target_kind = 'node' "
            "AND namespace = 'default'",
            (g3,),
        )
        assert cur.fetchone()[0] == 8, "flag-off snapshot fallback stayed partial"


def test_interval_mode_dirty_drives_kind_after_empty_previous_snapshot(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression for live cutover: once interval mode drops carry-forward, a
    previous changed-only generation can have no rows for one target kind. The
    next dirty publish for that kind must still stay changed-only; the interval
    table, not the previous snapshot, carries unchanged targets."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=["doc"],
            narrowings=[("doc", "links", "doc")],
        )

    _apply(
        dsn,
        [
            *_facts(suffix, range(4), {"k": "v"}),
            *_edge_facts(suffix, [(0, 1), (1, 2), (2, 3)], {"w": 1}),
        ],
    )
    g1 = str(Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()["generation_id"])

    _apply(dsn, _facts(suffix, [0], {"k": "node-only"}, rev=2))
    g2 = str(Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()["generation_id"])

    _apply(dsn, _edge_facts(suffix, [(0, 1)], {"w": 2}, rev=2))
    res3 = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    g3 = str(res3["generation_id"])

    with db_connect(dsn, autocommit=True) as conn:
        db = _db(conn)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT target_kind, count(*) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
                "WHERE generation_id = %s AND namespace = 'default' "
                "GROUP BY target_kind ORDER BY target_kind",
                (g2,),
            )
            assert dict(cur.fetchall()) == {"node": 1}
            cur.execute(
                f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
                "WHERE generation_id = %s AND namespace = 'default' "
                "AND target_kind = 'edge'",
                (g3,),
            )
            assert cur.fetchone()[0] == 1
            cur.execute(
                "SELECT count(*) FROM okg.attribute_effective_view_intervals "
                "WHERE target_kind = 'edge' AND to_ord = %s",
                (EFFECTIVE_INTERVAL_OPEN_ORD,),
            )
            assert cur.fetchone()[0] == 3
        stab3 = _stab(conn, db, g3)
        src_idx = _INTERVAL_READ_COLS.index("src") - 1
        dst_idx = _INTERVAL_READ_COLS.index("dst") - 1
        attrs_idx = _INTERVAL_READ_COLS.index("attrs") - 1
        assert any(
            key[0] == "edge"
            and row[src_idx] == f"doc:0:{suffix}"
            and row[dst_idx] == f"doc:1:{suffix}"
            and row[attrs_idx].get("w") == 2
            for key, row in stab3.items()
        )
    assert int(res3["edge_count"]) == 3
    assert g1 != g2 != g3


def test_interval_publish_prunes_changed_only_legacy_snapshot(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Interval-owned publishes maintain changed-only snapshots plus interval rows,
    but still bound the retained legacy snapshot table."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}")

    _apply(dsn, _facts(suffix, range(3), {"k": "v"}))
    r1 = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()["attribute_effective_view_refresh"]
    _apply(dsn, _facts(suffix, [0], {"k": "v2"}, rev=2))
    r2 = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()["attribute_effective_view_refresh"]
    _apply(dsn, _facts(suffix, [1], {"k": "v3"}, rev=2))
    r3 = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()["attribute_effective_view_refresh"]

    assert r1["pruned_old_generation_rows"] == 0, r1
    assert r2["pruned_old_generation_rows"] == 0, r2
    assert r3["pruned_old_generation_rows"] > 0, r3

    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT count(DISTINCT generation_id) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}"
        )
        assert cur.fetchone()[0] <= EFFECTIVE_VIEW_RETENTION_GENERATIONS


def test_overlay_only_change_not_served_stale(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """P0 regression (adversarial review): an OVERLAY-only change (raw graph
    untouched, so NOT in the raw chronos dirty scope) must still close+reopen the
    interval. The materializer merges overlay-only changes into the snapshot, so the
    delta-mode close unions the snapshot's target ids; closing on raw-dirty ALONE
    would leave the OLD interval open and serve stale attrs."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    enriched = f"doc:0:{suffix}"
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, range(3), {"k": "v"}))
    g1 = str(Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()["generation_id"])

    with db_connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        db, ctx = store.context.db, store.context
        # Craft g2 with an OVERLAY-only change on doc:0 (raw untouched -> the raw
        # dirty scope for doc:0 is EMPTY); the materializer merges it into g2's
        # (changed-only) snapshot.
        g2 = f"{g1}:ov"
        record_attribute_observation(
            conn, target_kind="node", target_id=enriched, generation_id=g2,
            attr_key="enriched", value="yes", producer="test",
        )
        materialize_effective_attributes_for_generation(conn, generation_id=g2)
        _reset_materialize_instrumentation()
        materialize_effective_attribute_view_for_generation(
            conn, generation_id=g2, previous_generation_id=g1,
            dirty_targets=DirtyTargets(node_ids=frozenset(), edge_ids=frozenset()),
        )
        merged = _MATERIALIZE_INSTRUMENTATION["merged_rows"]
        assign_generation_ordinal(db, ctx, checkpoint_id=g2, branch="default")
        # Delta-sync with an EMPTY raw dirty set (overlay-only). The fix must still
        # close doc:0's old interval and open the new one (via the snapshot union).
        sync_effective_interval_view_for_generation(
            conn, db, generation_id=g2,
            dirty_targets=DirtyTargets(node_ids=frozenset(), edge_ids=frozenset()),
        )
        stab2 = _stab(conn, db, g2)
        attrs_idx = _INTERVAL_READ_COLS.index("attrs") - 1  # stab tuple drops target_id
    assert stab2[("node", enriched)][attrs_idx].get("enriched") == "yes", (
        "overlay-only change served stale", stab2[("node", enriched)]
    )
    # ...and the overlay gen-diff union pulled in EXACTLY the changed target, not
    # the whole live set. This is the O(changed) half of the same guard; the
    # snapshot-arm oracle asserts it too, but only at OKG_IVM_INTERVAL_VIEW=0
    # (test_effective_attribute_view_oracle.py::test_dirty_path_catches_overlay_only_change),
    # so without this line no test checks it at the shipped default.
    assert merged == 1, merged


def _numeric_edge_id() -> str:
    """A fresh edge id in the numeric form real edge ids take."""
    return str(uuid.uuid4().int >> 65)


def test_dirty_interval_close_handles_edge_update_retract_and_unchanged(
    clean_runtime: str,
) -> None:
    suffix = uuid.uuid4().hex[:8]
    gen = f"okg:test:gen:{suffix}"
    # Edge ids are numeric (the edge page index orders them as bigint).
    changed, retracted, unchanged = (_numeric_edge_id() for _ in range(3))
    old_ord = 10
    new_ord = 11

    with db_connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            # schema-interval-narrow-split: fixture rows carry attrs_hash into
            # the narrow tables; the payload lives once in the version store.
            hashes = {
                (tid, key): _insert_version(cur, "edge", tid, {"k": key})
                for tid, key in (
                    (changed, "old"),
                    (changed, "new"),
                    (retracted, "old"),
                    (unchanged, "same"),
                )
            }

            def interval_row(target_id: str, key: str) -> tuple:
                return (
                    "edge",
                    target_id,
                    "default",
                    "default",
                    old_ord,
                    EFFECTIVE_INTERVAL_OPEN_ORD,
                    f"src:{target_id}",
                    f"dst:{target_id}",
                    "test-edge",
                    0,
                    None,
                    hashes[(target_id, key)],
                )

            def snapshot_row(target_id: str, key: str) -> tuple:
                return (
                    "edge",
                    target_id,
                    gen,
                    "default",
                    None,
                    f"src:{target_id}",
                    f"dst:{target_id}",
                    "test-edge",
                    0,
                    None,
                    hashes[(target_id, key)],
                )

            cur.executemany(
                f"""
                INSERT INTO {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE}
                  (target_kind, target_id, namespace, branch_id, from_ord, to_ord,
                   src, dst, edge_type, overlay_count, raw_text_hash, attrs_hash)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    interval_row(changed, "old"),
                    interval_row(retracted, "old"),
                    interval_row(unchanged, "same"),
                ],
            )
            cur.executemany(
                f"""
                INSERT INTO {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}
                  (target_kind, target_id, generation_id, namespace, subtype, src, dst,
                   edge_type, overlay_count, raw_text_hash, attrs_hash)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                [
                    snapshot_row(changed, "new"),
                    snapshot_row(unchanged, "same"),
                ],
            )
            closed = _close_dirty_effective_intervals(
                cur,
                iv=ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE,
                snap=ATTRIBUTE_EFFECTIVE_VIEW_TABLE,
                target_kind="edge",
                params={
                    "new": new_ord,
                    "br": "default",
                    "ns": "default",
                    "tk": "edge",
                    "open": EFFECTIVE_INTERVAL_OPEN_ORD,
                    "gen": gen,
                },
                close_ids={changed, retracted, unchanged},
            )
            assert closed == 2
            cur.execute(
                f"""
                SELECT target_id, to_ord
                  FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE}
                 WHERE target_id = ANY(%s)
                 ORDER BY target_id
                """,
                ([changed, retracted, unchanged],),
            )
            assert dict(cur.fetchall()) == {
                changed: new_ord,
                retracted: new_ord,
                unchanged: EFFECTIVE_INTERVAL_OPEN_ORD,
            }


def _reanchor_branch(cur, branch: str, ordinals: int) -> list[str]:
    """Register ``ordinals`` generations on a private branch; return their ids."""
    generations = [f"generation:{branch}:g{ordv}" for ordv in range(ordinals)]
    for ordv, generation in enumerate(generations):
        cur.execute(
            "INSERT INTO okg.generation_ordinals "
            "(checkpoint_id, branch_id, ordinal) VALUES (%s, %s, %s)",
            (generation, branch, ordv),
        )
    return generations


def _write_reanchor_snapshot(
    cur, generation: str, payload_by_target: dict[str, str]
) -> dict[str, bytes]:
    """Write a full node snapshot for ``generation``; return target -> attrs_hash."""
    hashes: dict[str, bytes] = {}
    for target_id, value in payload_by_target.items():
        attrs_hash = _insert_version(cur, "node", target_id, {"k": value})
        hashes[target_id] = attrs_hash
        cur.execute(
            f"""
            INSERT INTO {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}
              (target_kind, target_id, generation_id, namespace, subtype, src,
               dst, edge_type, overlay_count, raw_text_hash, attrs_hash)
            VALUES ('node', %s, %s, 'default', 'doc', NULL, NULL, NULL, 0,
                    NULL, %s)
            """,
            (target_id, generation, attrs_hash),
        )
    return hashes


def _reanchor_rows(cur, branch: str) -> dict[str, list[tuple[int, int, bytes]]]:
    """Every interval row on ``branch``: target -> [(from_ord, to_ord, hash)]."""
    cur.execute(
        f"SELECT target_id, from_ord, to_ord, attrs_hash "
        f"FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
        "WHERE branch_id = %s ORDER BY target_id, from_ord",
        (branch,),
    )
    rows: dict[str, list[tuple[int, int, bytes]]] = {}
    for target_id, from_ord, to_ord, attrs_hash in cur.fetchall():
        rows.setdefault(target_id, []).append(
            (int(from_ord), int(to_ord), bytes(attrs_hash))
        )
    return rows


def _reanchor(conn, generation: str) -> dict:
    return reanchor_effective_interval_view_for_generation(
        conn, _db(conn), generation_id=generation
    )


def test_reanchor_keeps_unchanged_open_intervals(clean_runtime: str) -> None:
    """okg#2314: repeated reanchors over an unchanged view add no rows."""
    suffix = uuid.uuid4().hex[:8]
    branch = f"reanchor_same_{suffix}"
    targets = [f"doc:{name}:{suffix}" for name in ("a", "b", "c")]
    with db_connect(clean_runtime, autocommit=True) as conn:
        ensure_attribute_schema(conn)
        with conn.cursor() as cur:
            generations = _reanchor_branch(cur, branch, 3)
            for generation in generations:
                hashes = _write_reanchor_snapshot(
                    cur, generation, {target: "v" for target in targets}
                )
        first = _reanchor(conn, generations[0])
        assert first["counts"]["node"]["opened"] == 3, first
        for generation in generations[1:]:
            again = _reanchor(conn, generation)
            assert again["rows_written"] == 0, again
        with conn.cursor() as cur:
            rows = _reanchor_rows(cur, branch)
    assert rows == {
        target: [(0, EFFECTIVE_INTERVAL_OPEN_ORD, hashes[target])]
        for target in targets
    }


def test_reanchor_closes_and_opens_only_changed_targets(
    clean_runtime: str,
) -> None:
    """A reanchor touches exactly the changed, the vanished and the new target."""
    suffix = uuid.uuid4().hex[:8]
    branch = f"reanchor_delta_{suffix}"
    keep, changed, gone, new = (
        f"doc:{name}:{suffix}" for name in ("keep", "changed", "gone", "new")
    )
    with db_connect(clean_runtime, autocommit=True) as conn:
        ensure_attribute_schema(conn)
        with conn.cursor() as cur:
            g0, g1 = _reanchor_branch(cur, branch, 2)
            before = _write_reanchor_snapshot(
                cur, g0, {keep: "v", changed: "old", gone: "v"}
            )
            after = _write_reanchor_snapshot(
                cur, g1, {keep: "v", changed: "new", new: "v"}
            )
        _reanchor(conn, g0)
        result = _reanchor(conn, g1)
        with conn.cursor() as cur:
            rows = _reanchor_rows(cur, branch)
    assert result["counts"]["node"] == {
        "deleted_current": 0,
        "closed": 2,
        "opened": 2,
    }, result
    assert before[changed] != after[changed]
    assert rows == {
        keep: [(0, EFFECTIVE_INTERVAL_OPEN_ORD, before[keep])],
        changed: [
            (0, 1, before[changed]),
            (1, EFFECTIVE_INTERVAL_OPEN_ORD, after[changed]),
        ],
        gone: [(0, 1, before[gone])],
        new: [(1, EFFECTIVE_INTERVAL_OPEN_ORD, after[new])],
    }


_REANCHOR_BASE_PAYLOAD = {
    "node": {
        "subtype": "doc",
        "src": None,
        "dst": None,
        "edge_type": None,
        "overlay_count": 0,
        "raw_text_hash": "rawtext:base",
    },
    "edge": {
        "subtype": None,
        "src": "doc:src:base",
        "dst": "doc:dst:base",
        "edge_type": "cites",
        "overlay_count": 0,
        "raw_text_hash": None,
    },
}


def _write_reanchor_payload_row(
    cur, generation: str, target_kind: str, target_id: str, payload: dict
) -> None:
    """Write one snapshot row with an explicit narrow payload."""
    cur.execute(
        f"""
        INSERT INTO {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}
          (target_kind, target_id, generation_id, namespace, subtype, src,
           dst, edge_type, overlay_count, raw_text_hash, attrs_hash)
        VALUES (%(tk)s, %(tid)s, %(gen)s, 'default', %(subtype)s, %(src)s,
                %(dst)s, %(edge_type)s, %(overlay_count)s, %(raw_text_hash)s,
                %(attrs_hash)s)
        """,
        {"tk": target_kind, "tid": target_id, "gen": generation, **payload},
    )


@pytest.mark.parametrize(
    ("target_kind", "column", "new_value"),
    [
        ("node", "subtype", "paper"),
        ("node", "overlay_count", 1),
        ("node", "raw_text_hash", "rawtext:changed"),
        ("node", "attrs_hash", None),  # filled with a second version's hash
        ("edge", "src", "doc:src:changed"),
        ("edge", "dst", "doc:dst:changed"),
        ("edge", "edge_type", "supersedes"),
    ],
)
def test_reanchor_closes_when_only_one_payload_column_differs(
    clean_runtime: str, target_kind: str, column: str, new_value
) -> None:
    """okg#2314 review: every payload column, alone, decides "changed".

    The target keeps its attrs_hash (except in the attrs_hash case) and differs
    in exactly ``column``; the reanchor must close its open interval and open a
    new one carrying the new value, while an identical sibling stays open.
    """
    suffix = uuid.uuid4().hex[:8]
    branch = f"reanchor_col_{column}_{suffix}"
    if target_kind == "edge":
        changed, steady = _numeric_edge_id(), _numeric_edge_id()
    else:
        changed = f"{target_kind}:changed:{suffix}"
        steady = f"{target_kind}:steady:{suffix}"
    with db_connect(clean_runtime, autocommit=True) as conn:
        ensure_attribute_schema(conn)
        with conn.cursor() as cur:
            g0, g1 = _reanchor_branch(cur, branch, 2)
            hashes = {
                target: _insert_version(cur, target_kind, target, {"k": "v"})
                for target in (changed, steady)
            }
            base = {
                target: {
                    **_REANCHOR_BASE_PAYLOAD[target_kind],
                    "attrs_hash": hashes[target],
                }
                for target in (changed, steady)
            }
            if column == "attrs_hash":
                new_value = _insert_version(
                    cur, target_kind, changed, {"k": "other"}
                )
            assert base[changed][column] != new_value
            after = {**base[changed], column: new_value}
            for target in (changed, steady):
                _write_reanchor_payload_row(
                    cur, g0, target_kind, target, base[target]
                )
            _write_reanchor_payload_row(cur, g1, target_kind, changed, after)
            _write_reanchor_payload_row(cur, g1, target_kind, steady, base[steady])
        _reanchor(conn, g0)
        result = _reanchor(conn, g1)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT target_id, from_ord, to_ord, {column} "
                f"FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
                "WHERE branch_id = %s ORDER BY target_id, from_ord",
                (branch,),
            )
            rows: dict[str, list[tuple]] = {}
            for target_id, from_ord, to_ord, value in cur.fetchall():
                if isinstance(value, memoryview):
                    value = bytes(value)
                rows.setdefault(target_id, []).append(
                    (int(from_ord), int(to_ord), value)
                )
    assert result["counts"][target_kind] == {
        "deleted_current": 0,
        "closed": 1,
        "opened": 1,
    }, result
    assert rows == {
        changed: [
            (0, 1, base[changed][column]),
            (1, EFFECTIVE_INTERVAL_OPEN_ORD, new_value),
        ],
        steady: [(0, EFFECTIVE_INTERVAL_OPEN_ORD, base[steady][column])],
    }


def test_reanchor_repairs_drifted_and_duplicate_open_intervals(
    clean_runtime: str,
) -> None:
    """The repair survives: a wrong or duplicated open interval is replaced."""
    suffix = uuid.uuid4().hex[:8]
    branch = f"reanchor_repair_{suffix}"
    drifted, duplicated, healthy = (
        f"doc:{name}:{suffix}" for name in ("drifted", "duplicated", "healthy")
    )
    with db_connect(clean_runtime, autocommit=True) as conn:
        ensure_attribute_schema(conn)
        with conn.cursor() as cur:
            g0, _g1, g2 = _reanchor_branch(cur, branch, 3)
            payload = {drifted: "v", duplicated: "v", healthy: "v"}
            truth = _write_reanchor_snapshot(cur, g0, payload)
            _write_reanchor_snapshot(cur, g2, payload)
        _reanchor(conn, g0)
        with conn.cursor() as cur:
            wrong = _insert_version(cur, "node", drifted, {"k": "corrupt"})
            cur.execute(
                f"UPDATE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} SET attrs_hash = %s "
                "WHERE branch_id = %s AND target_id = %s AND to_ord = %s",
                (wrong, branch, drifted, EFFECTIVE_INTERVAL_OPEN_ORD),
            )
            # A second open interval for one target: both carry the right
            # payload, but a stab would return the target twice.
            cur.execute(
                f"""
                INSERT INTO {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE}
                  (target_kind, target_id, namespace, branch_id, from_ord, to_ord,
                   subtype, src, dst, edge_type, overlay_count, raw_text_hash,
                   attrs_hash)
                VALUES ('node', %s, 'default', %s, 1, %s, 'doc', NULL, NULL,
                        NULL, 0, NULL, %s)
                """,
                (duplicated, branch, EFFECTIVE_INTERVAL_OPEN_ORD, truth[duplicated]),
            )
        result = _reanchor(conn, g2)
        with conn.cursor() as cur:
            rows = _reanchor_rows(cur, branch)
    assert result["counts"]["node"]["closed"] == 3, result
    assert result["counts"]["node"]["opened"] == 2, result
    assert rows == {
        drifted: [(0, 2, wrong), (2, EFFECTIVE_INTERVAL_OPEN_ORD, truth[drifted])],
        duplicated: [
            (0, 2, truth[duplicated]),
            (1, 2, truth[duplicated]),
            (2, EFFECTIVE_INTERVAL_OPEN_ORD, truth[duplicated]),
        ],
        healthy: [(0, EFFECTIVE_INTERVAL_OPEN_ORD, truth[healthy])],
    }


def test_reconcile_guard_reanchors_detected_interval_drift(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A protected interval mutation makes the next publish a full repair boundary."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    monkeypatch.setenv("OKG_IVM_INTERVAL_RECONCILE_EVERY", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, range(5), {"k": "v"}))
    Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()  # g1: 5 nodes, intervals synced + reconciled

    # Simulate a missed/partial sync: drop one OPEN interval for an UNCHANGED target.
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM okg.attribute_effective_view_intervals "
            "WHERE target_kind = 'node' AND to_ord = %s AND target_id = %s",
            (EFFECTIVE_INTERVAL_OPEN_ORD, f"doc:4:{suffix}"),
        )
    # The dirty receipt prevents an incremental retry. The next publish changes an
    # unrelated node, observes that dirty proof, and reanchors the whole front:
    # the missing row is reopened, the changed node is split, the rest stay open.
    _apply(dsn, _facts(suffix, [0], {"k": "CHANGED"}, rev=2))
    recovered = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    receipt = recovered["attribute_effective_view_refresh"]["integrity_receipt"]
    assert receipt["receipt_type"] == "anchor"
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.attribute_effective_view_intervals "
            "WHERE target_kind = 'node' AND to_ord = %s",
            (EFFECTIVE_INTERVAL_OPEN_ORD,),
        )
        assert cur.fetchone()[0] == 5
        classification = classify_effective_attribute_recovery(
            conn, deployment_name=_DEPLOYMENT
        )
    assert classification["classification"] == "no_repair"


def _full_recount(conn, gen: str) -> int:
    """Ground-truth node count: a from-scratch full re-materialize of the live graph
    at the current head, independent of the running counter and the interval table."""
    res = materialize_effective_attribute_view_for_generation(
        conn, generation_id=f"{gen}:recount", previous_generation_id=None
    )
    assert res["materialized"], res
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
            "WHERE generation_id = %s AND target_kind = 'node' AND namespace = 'default'",
            (f"{gen}:recount",),
        )
        return int(cur.fetchone()[0])


def test_cardinality_running_counter_matches_full_count(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """task.cardinality-counter: the published node_count is a RUNNING COUNTER
    (prev + per-cycle delta, O(1)), and it must equal a ground-truth full re-count
    across insert/update/retract/noop over several generations -- i.e. the delta is
    accurate, so the counter never drifts (the F1 P0). Reconcile forced every publish."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    monkeypatch.setenv("OKG_IVM_INTERVAL_RECONCILE_EVERY", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])

    # (gen, mutation, expected live node count)
    _apply(dsn, _facts(suffix, range(6), {"k": "v"}))           # insert 6
    steps = [(6, None)]
    g = str(Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()["node_count"])
    assert g == "6", g

    _apply(dsn, _facts(suffix, [0], {"k": "CHANGED"}, rev=2))   # update -> still 6
    steps.append((6, str(Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()["node_count"])))
    _apply(dsn, _facts(suffix, [1], {}, op="R", rev=2))         # retract -> 5
    steps.append((5, str(Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()["node_count"])))
    _apply(dsn, _facts(suffix, [100, 101], {"k": "n"}))         # insert 2 -> 7
    last = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    steps.append((7, str(last["node_count"])))

    for expected, got in steps[1:]:
        assert got == str(expected), (expected, got)
    # The running counter agrees with an INDEPENDENT full re-count at the final head.
    with db_connect(dsn, autocommit=True) as conn:
        assert _full_recount(conn, str(last["generation_id"])) == 7
    assert int(last["node_count"]) == 7


def test_cardinality_counter_drift_fails_loud(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A forced counter-vs-full divergence raises a LOUD typed error (never a silent
    wrong total): stub the delta-counter to return a wrong value and require the
    periodic reconcile to fail the publish."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    monkeypatch.setenv("OKG_IVM_INTERVAL_RECONCILE_EVERY", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, range(4), {"k": "v"}))
    Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()  # g1 ok
    # Force the running counter wrong for the next publish.
    monkeypatch.setattr(
        Publisher,
        "_chronos_counts_after_delta",
        staticmethod(lambda *a, **k: (9999, 9999)),
    )
    _apply(dsn, _facts(suffix, [0], {"k": "CHANGED"}, rev=2))
    with pytest.raises(Exception, match="running counter drifted"):
        Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()


def test_structural_fork_keeps_full_snapshot_not_intervals(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """task.branch-fork-correctness: a STRUCTURAL fork (forked from the non-root
    'default') is NOT interval-tracked, so with the flag ON it keeps the FULL
    per-generation snapshot (carry-forward NOT dropped) -- its snapshot-fallback
    reads stay byte-identical to a re-materialize on the fork (interval acceleration
    deferred for forks). The default branch stays interval-tracked."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    fork = f"qfork_{suffix}"
    with db_connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
        store = ChronosGraphStore.from_connection(conn)
        db = store.context.db
        # Classification: default is interval-tracked; a structural fork is not.
        assert is_interval_tracked_branch(db, "default") is True
        store.create_branch(
            fork, from_branch="default", metadata={"kind": "okg_branch"}
        )
        assert is_interval_tracked_branch(db, fork) is False

        store.apply_facts(
            [
                NodeFact(
                    f"d:{i}:{suffix}", "doc", {"k": "v"},
                    {"id": f"d:{i}:{suffix}"}, {"v": 1},
                )
                for i in range(4)
            ],
            source="default",
            branch=fork,
            strict_endpoints=True,
        )
        f1 = f"okg:default:branch:{fork}:gen:{suffix}:1"
        materialize_effective_attribute_view_for_generation(
            conn, generation_id=f1, branch=fork, previous_generation_id=None
        )
        store.apply_facts(
            [
                NodeFact(
                    f"d:0:{suffix}", "doc", {"k": "CHG"},
                    {"id": f"d:0:{suffix}"}, {"v": 2},
                )
            ],
            source="default",
            branch=fork,
            strict_endpoints=True,
        )
        f2 = f"okg:default:branch:{fork}:gen:{suffix}:2"
        _reset_materialize_instrumentation()
        materialize_effective_attribute_view_for_generation(
            conn,
            generation_id=f2,
            branch=fork,
            previous_generation_id=f1,
            dirty_targets=DirtyTargets(
                node_ids=frozenset([f"d:0:{suffix}"]), edge_ids=frozenset()
            ),
        )
        carried = _MATERIALIZE_INSTRUMENTATION["carried_rows"]
    # Fork is NOT tracked -> flag-on it KEEPS the carry-forward (the 3 unchanged
    # docs), unlike the default branch which drops it to a changed-only snapshot.
    assert carried >= 3, carried


def test_interval_backfill_enables_flag_flip(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The one-time backfill makes OKG_IVM_INTERVAL_VIEW safely flippable on an
    EXISTING (flag-off) deployment: it opens an interval per live target from the
    full snapshot, so the first flag-on publish's reconcile passes instead of
    failing (open-count < live)."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "0")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    # Three publishes with the flag OFF: full snapshots + minted ordinals, NO intervals.
    _apply(dsn, _facts(suffix, range(5), {"k": "v"}))
    Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    _apply(dsn, _facts(suffix, [0], {"k": "CHANGED"}, rev=2))
    Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    _apply(dsn, _facts(suffix, [100], {"k": "new"}))
    g3 = str(Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()["generation_id"])  # 6 live nodes

    with db_connect(dsn, autocommit=True) as conn:
        db = _db(conn)
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.attribute_effective_view_intervals")
            assert cur.fetchone()[0] == 0, "intervals exist before backfill"
        backfill = backfill_effective_interval_view_for_generation(
            conn, db, generation_id=g3,
        )
        assert backfill["mode"] == "empty_initial_load"
        assert backfill["rows_written"] == 6
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.attribute_effective_view_intervals "
                "WHERE target_kind = 'node' AND to_ord = %s",
                (EFFECTIVE_INTERVAL_OPEN_ORD,),
            )
            assert cur.fetchone()[0] == 6, "backfill did not open all live intervals"
        rerun = backfill_effective_interval_view_for_generation(conn, db, generation_id=g3)
        assert rerun["mode"] == "already_backfilled"
        assert rerun["rows_written"] == 0
        _assert_stab_equals_snapshot(conn, db, g3)
    with db_connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        db = _db(conn)
        dict_row_rerun = backfill_effective_interval_view_for_generation(
            conn, db, generation_id=g3,
        )
        assert dict_row_rerun["mode"] == "already_backfilled"
        assert dict_row_rerun["rows_written"] == 0

    # Flip the flag on (reconcile every publish): the next publish must NOT be
    # blocked by the reconcile -- the backfill made the interval table complete.
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    monkeypatch.setenv("OKG_IVM_INTERVAL_RECONCILE_EVERY", "1")
    _apply(dsn, _facts(suffix, [1], {"k": "CHANGED2"}, rev=2))
    res = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    assert res["status"] == "published", res


def test_unbackfilled_existing_deployment_refuses_before_interval_fast_path(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Issue #774: default-on cannot silently repair an existing deployment."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "0")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, range(5), {"k": "v"}))
    first = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    first_generation = str(first["generation_id"])

    def derived_relation_counts() -> tuple[int, int]:
        with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}")
            snapshot = int(cur.fetchone()[0])
            cur.execute(f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE}")
            intervals = int(cur.fetchone()[0])
        return snapshot, intervals

    _apply(dsn, _facts(suffix, [100], {"k": "pending"}))
    before_relations = derived_relation_counts()
    assert before_relations[1] == 0, "flag-off predecessor unexpectedly has intervals"
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"INSERT INTO {ATTRIBUTE_INTEGRITY_STATE_TABLE} "
            "(deployment_name, branch_id, namespace, latest_receipt_digest) "
            "VALUES (%s, 'default', 'default', 'sha256:dangling') "
            "ON CONFLICT (deployment_name, branch_id, namespace) DO UPDATE "
            "SET latest_receipt_digest = EXCLUDED.latest_receipt_digest",
            (_DEPLOYMENT,),
        )
    _reset_materialize_instrumentation()
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")

    with pytest.raises(EffectiveIntervalBackfillRequired) as excinfo:
        Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    refusal = excinfo.value
    assert refusal.code == "effective_interval_backfill_required"
    assert refusal.data == {
        "reason": "predecessor_interval_count_mismatch",
        "deployment_name": _DEPLOYMENT,
        "branch": "default",
        "predecessor_generation_id": first_generation,
        "predecessor_ordinal": 0,
        "expected_counts": {
            "node": int(first["node_count"]),
            "edge": int(first["edge_count"]),
        },
        "observed_counts": {"node": 0, "edge": 0},
        "receipt_reason": "receipt_dirty",
    }
    assert refusal.fix_hint == (
        "run `uv run okg db repair-effective-attribute-intervals "
        "--deployment default --apply` before enabling OKG_IVM_INTERVAL_VIEW"
    )
    assert derived_relation_counts() == before_relations
    assert all(value == 0 for value in _MATERIALIZE_INSTRUMENTATION.values())
    with db_connect(dsn, autocommit=True) as conn:
        latest = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).latest_generation(deployment_name=_DEPLOYMENT, status="published")
        assert latest is not None
        assert str(latest.checkpoint_id) == first_generation
        repaired = repair_effective_attribute_interval_view(
            conn, deployment_name=_DEPLOYMENT
        )
        assert repaired["ok"] is True
        assert repaired["generation_id"] == first_generation

    published = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    assert published["status"] == "published", published
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT target_kind, count(*) FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
            "WHERE branch_id = 'default' AND to_ord = %s GROUP BY target_kind",
            (EFFECTIVE_INTERVAL_OPEN_ORD,),
        )
        rows = {str(kind): int(count) for kind, count in cur.fetchall()}
        open_counts = {"node": rows.get("node", 0), "edge": rows.get("edge", 0)}
    assert open_counts == {
        "node": int(published["node_count"]),
        "edge": int(published["edge_count"]),
    }


def test_interval_backfill_precondition_preserves_post_sync_crash_retry(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A committed candidate front never invalidates its published predecessor."""
    attributes_module = importlib.import_module("okg.substrate.attributes")

    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, range(3), {"k": "v"}))
    first = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    first_generation = str(first["generation_id"])
    assert first["attribute_effective_view_refresh"]["integrity_receipt"][
        "receipt_type"
    ] == "anchor"

    exact_count_calls: list[tuple[str, int]] = []
    real_exact_count = attributes_module._effective_interval_stab_counts_for_generation

    def track_exact_count(conn, *, branch_id, ordinal, namespace="default"):
        exact_count_calls.append((str(branch_id), int(ordinal)))
        return real_exact_count(
            conn,
            branch_id=branch_id,
            ordinal=ordinal,
            namespace=namespace,
        )

    monkeypatch.setattr(
        attributes_module,
        "_effective_interval_stab_counts_for_generation",
        track_exact_count,
    )
    _apply(dsn, _facts(suffix, [100], {"k": "candidate"}))

    # Since okg#1587 the candidate's ordinal and interval sync commit in the
    # same transaction as its checkpoint, so this crash rolls them back. A
    # deployment that crashed here under the old code still carries the
    # committed candidate front, and the precondition must keep handling it.
    # Capture the candidate's rows from inside the doomed transaction, then
    # restore them after the rollback to recreate that state exactly.
    captured: dict[str, list[str]] = {}

    def crash_after_interval_sync(self, generation_id, *args, **kwargs):
        with self.context.conn.cursor() as cur:
            cur.execute(
                "SELECT row_to_json(o)::text FROM okg.generation_ordinals o "
                "WHERE checkpoint_id = %s",
                (str(generation_id),),
            )
            captured["ordinals"] = [row[0] for row in cur.fetchall()]
            cur.execute(
                f"SELECT row_to_json(i)::text "
                f"FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} i "
                "WHERE (i.branch_id, i.from_ord) IN ("
                "  SELECT branch_id, ordinal FROM okg.generation_ordinals"
                "   WHERE checkpoint_id = %s)",
                (str(generation_id),),
            )
            captured["intervals"] = [row[0] for row in cur.fetchall()]
        raise RuntimeError("injected checkpoint failure after interval sync")

    with monkeypatch.context() as crash_patch:
        crash_patch.setattr(
            ChronosGraphStore,
            "checkpoint_generation",
            crash_after_interval_sync,
        )
        with pytest.raises(RuntimeError, match="injected checkpoint failure"):
            Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    assert exact_count_calls == [], "clean receipt path performed an interval count"
    assert len(captured["ordinals"]) == 1, captured
    assert len(captured["intervals"]) == 1, captured

    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        # The crash left nothing behind: no candidate ordinal, no candidate row.
        cur.execute(
            "SELECT count(*) FROM okg.generation_ordinals WHERE checkpoint_id = %s",
            (json.loads(captured["ordinals"][0])["checkpoint_id"],),
        )
        assert cur.fetchone()[0] == 0
        cur.execute(
            f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
            "WHERE target_id = %s",
            (f"doc:100:{suffix}",),
        )
        assert cur.fetchone()[0] == 0
        # Recreate the pre-#1587 post-crash state.
        cur.execute(
            "INSERT INTO okg.generation_ordinals "
            "SELECT * FROM json_populate_record(NULL::okg.generation_ordinals, %s::json)",
            (captured["ordinals"][0],),
        )
        cur.execute(
            f"INSERT INTO {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
            f"SELECT * FROM json_populate_record("
            f"NULL::{ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE}, %s::json)",
            (captured["intervals"][0],),
        )

    with db_connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        db = store.context.db
        latest = store.latest_generation(
            deployment_name=_DEPLOYMENT, status="published"
        )
        assert latest is not None
        assert str(latest.checkpoint_id) == first_generation
        branch_id, predecessor_ordinal = resolve_generation_ordinal_row(
            db, first_generation
        )
        with conn.cursor() as cur:
            cur.execute(
                "SELECT max(ordinal) FROM okg.generation_ordinals "
                "WHERE branch_id = %s",
                (branch_id,),
            )
            candidate_ordinal = int(cur.fetchone()[0])
            cur.execute(
                f"SELECT target_kind, count(*) "
                f"FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
                "WHERE branch_id = %s AND to_ord = %s GROUP BY target_kind",
                (branch_id, EFFECTIVE_INTERVAL_OPEN_ORD),
            )
            open_counts = {
                str(kind): int(count) for kind, count in cur.fetchall()
            }
            cur.execute(
                f"SELECT target_kind, count(*) "
                f"FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
                "WHERE branch_id = %s AND from_ord <= %s AND %s < to_ord "
                "GROUP BY target_kind",
                (branch_id, predecessor_ordinal, predecessor_ordinal),
            )
            predecessor_rows = {
                str(kind): int(count) for kind, count in cur.fetchall()
            }
            predecessor_counts = {
                "node": predecessor_rows.get("node", 0),
                "edge": predecessor_rows.get("edge", 0),
            }
    expected_predecessor = {
        "node": int(first["node_count"]),
        "edge": int(first["edge_count"]),
    }
    assert candidate_ordinal > predecessor_ordinal
    assert open_counts.get("node", 0) == expected_predecessor["node"] + 1
    assert predecessor_counts == expected_predecessor

    reanchor_modes: list[str] = []
    real_reanchor = attributes_module.reanchor_effective_interval_view_for_generation

    def track_reanchor(*args, **kwargs):
        result = real_reanchor(*args, **kwargs)
        reanchor_modes.append(str(result["mode"]))
        return result

    monkeypatch.setattr(
        attributes_module,
        "reanchor_effective_interval_view_for_generation",
        track_reanchor,
    )
    retried = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    assert retried["status"] == "published", retried
    assert exact_count_calls == [(str(branch_id), predecessor_ordinal)]
    assert reanchor_modes == ["full_reanchor"]
    assert retried["attribute_effective_view_refresh"]["integrity_receipt"][
        "receipt_type"
    ] == "anchor"


def test_interval_backfill_precondition_names_unverifiable_state(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "0")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, [1], {"k": "v"}))
    first = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    generation = str(first["generation_id"])
    expected = {
        "node": int(first["node_count"]),
        "edge": int(first["edge_count"]),
    }

    def assert_refusal(
        excinfo,
        *,
        reason: str,
        expected_counts: dict[str, int] | None,
        observed_counts: dict[str, int] | None = None,
    ) -> None:
        refusal = excinfo.value
        assert refusal.code == "effective_interval_backfill_required"
        assert refusal.data["reason"] == reason
        assert refusal.data["expected_counts"] == expected_counts
        assert refusal.data["observed_counts"] == observed_counts

    with db_connect(dsn) as conn:
        db = _db(conn)
        with pytest.raises(EffectiveIntervalBackfillRequired) as missing_counts:
            require_effective_interval_backfill_precondition(
                conn,
                db,
                deployment_name=_DEPLOYMENT,
                branch="default",
                generation_id=generation,
                checkpoint_counts=None,
            )
        assert_refusal(
            missing_counts,
            reason="predecessor_checkpoint_counts_unavailable",
            expected_counts=None,
        )

        with pytest.raises(EffectiveIntervalBackfillRequired) as missing_ordinal:
            require_effective_interval_backfill_precondition(
                conn,
                db,
                deployment_name=_DEPLOYMENT,
                branch="default",
                generation_id="missing-generation",
                checkpoint_counts=expected,
            )
        assert_refusal(
            missing_ordinal,
            reason="predecessor_ordinal_unavailable",
            expected_counts=expected,
        )

        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE}")
        with pytest.raises(EffectiveIntervalBackfillRequired) as missing_table:
            require_effective_interval_backfill_precondition(
                conn,
                db,
                deployment_name=_DEPLOYMENT,
                branch="default",
                generation_id=generation,
                checkpoint_counts=expected,
            )
        assert_refusal(
            missing_table,
            reason="interval_table_missing",
            expected_counts=expected,
        )
        conn.rollback()

        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE {ATTRIBUTE_INTEGRITY_STATE_TABLE}")
            cur.execute(f"DROP TABLE {ATTRIBUTE_INTEGRITY_RECEIPT_TABLE}")
        with pytest.raises(EffectiveIntervalBackfillRequired) as missing_authority:
            require_effective_interval_backfill_precondition(
                conn,
                db,
                deployment_name=_DEPLOYMENT,
                branch="default",
                generation_id=generation,
                checkpoint_counts=expected,
            )
        assert_refusal(
            missing_authority,
            reason="predecessor_interval_count_mismatch",
            expected_counts=expected,
            observed_counts={"node": 0, "edge": 0},
        )
        assert (
            missing_authority.value.data["receipt_reason"]
            == "receipt_authority_unavailable"
        )
        conn.rollback()


def test_lifecycle_repair_rebuilds_changed_only_snapshot_and_partial_intervals(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A live upgrade defect is repaired from the published checkpoint, not
    from two partial caches that happen to agree with one another.

    This is the database behavior behind ``db
    repair-effective-attribute-intervals``.
    """
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, range(4), {"k": "v"}))
    _apply(dsn, _edge_facts(suffix, [(0, 1), (1, 2)], {"kind": "test"}))
    Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    _apply(dsn, _facts(suffix, [0], {"k": "changed"}, rev=2))
    latest = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    generation = str(latest["generation_id"])

    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT target_kind, count(*) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
            "WHERE generation_id = %s GROUP BY target_kind",
            (generation,),
        )
        assert dict(cur.fetchall()).get("node", 0) < int(latest["node_count"])
        cur.execute(
            f"DELETE FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
            "WHERE target_id IN (%s, %s)",
            (f"doc:3:{suffix}", f"doc:0:{suffix}->doc:1:{suffix}:links"),
        )

    with db_connect(dsn, autocommit=True) as conn:
        repaired = repair_effective_attribute_interval_view(
            conn, deployment_name=_DEPLOYMENT
        )
        assert repaired["ok"] is True
        assert repaired["generation_id"] == generation
        assert repaired["snapshot_rebuilt"] is True
        assert repaired["checkpoint_counts"] == repaired["snapshot_counts"]
        assert repaired["checkpoint_counts"] == repaired["interval_counts"]
        assert repaired["payload_match"] is True

        rerun = repair_effective_attribute_interval_view(
            conn, deployment_name=_DEPLOYMENT
        )
        assert rerun["ok"] is True
        assert rerun["snapshot_rebuilt"] is False
        assert rerun["interval_reconcile"]["rows_written"] == 0


def test_attribute_integrity_receipt_fast_path_never_scans_payload_rows(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, range(4), {"k": "v"}))
    published = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    assert (
        published["attribute_effective_view_refresh"]["integrity_receipt"][
            "receipt_type"
        ]
        == "anchor"
    )

    with db_connect(dsn, autocommit=True) as conn:
        anchored = repair_effective_attribute_interval_view(
            conn, deployment_name=_DEPLOYMENT
        )
        assert anchored["reason"] == "already_repaired"

        def reject_payload_scan(*args, **kwargs):
            raise AssertionError("healthy receipt path scanned protected payload rows")

        monkeypatch.setattr(
            "okg.substrate.attribute_integrity._payload_signature",
            reject_payload_scan,
        )
        rerun = repair_effective_attribute_interval_view(
            conn, deployment_name=_DEPLOYMENT
        )
    assert rerun["reason"] == "already_repaired"
    assert rerun["integrity_receipt"]["reason"] == "already_repaired"


def test_publisher_advances_attribute_integrity_receipt_from_exact_dirty_scope(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, range(3), {"k": "v"}))
    first = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    with db_connect(dsn, autocommit=True) as conn:
        anchor = repair_effective_attribute_interval_view(
            conn, deployment_name=_DEPLOYMENT
        )["integrity_receipt"]
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT dirty, dirty_reason FROM {ATTRIBUTE_INTEGRITY_STATE_TABLE} "
                "WHERE deployment_name = %s AND branch_id = 'default'",
                (_DEPLOYMENT,),
            )
            assert cur.fetchone() == (False, "clean")

    _apply(dsn, _facts(suffix, [1], {"k": "changed"}, rev=2))
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT dirty, dirty_reason FROM {ATTRIBUTE_INTEGRITY_STATE_TABLE} "
            "WHERE deployment_name = %s AND branch_id = 'default'",
            (_DEPLOYMENT,),
        )
        assert cur.fetchone() == (False, "clean")
    second = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    with db_connect(dsn, autocommit=True) as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"SELECT * FROM {ATTRIBUTE_INTEGRITY_STATE_TABLE} "
                "WHERE deployment_name = %s AND branch_id = 'default'",
                (_DEPLOYMENT,),
            )
            state_before_fast = dict(cur.fetchone())
            cur.execute(
                f"SELECT * FROM {ATTRIBUTE_INTEGRITY_RECEIPT_TABLE} "
                "WHERE receipt_digest = %s",
                (state_before_fast["latest_receipt_digest"],),
            )
            receipt_before_fast = dict(cur.fetchone())
        assert receipt_before_fast["receipt_type"] == "incremental", (
            state_before_fast["dirty"],
            state_before_fast["dirty_reason"],
            state_before_fast["mutation_epoch"],
            state_before_fast["clean_mutation_epoch"],
            receipt_before_fast["generation_id"],
            receipt_before_fast["ordinal"],
            str(second["generation_id"]),
        )
        assert "transition_token" not in state_before_fast
        assert "transition_token" not in json.dumps(second, sort_keys=True)
        fast = repair_effective_attribute_interval_view(
            conn, deployment_name=_DEPLOYMENT
        )
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                f"SELECT r.* FROM {ATTRIBUTE_INTEGRITY_STATE_TABLE} s "
                f"JOIN {ATTRIBUTE_INTEGRITY_RECEIPT_TABLE} r "
                "ON r.receipt_digest = s.latest_receipt_digest "
                "WHERE s.deployment_name = %s AND s.branch_id = 'default'",
                (_DEPLOYMENT,),
            )
            receipt = dict(cur.fetchone())
    assert first["status"] == "published"
    assert second["status"] == "published"
    assert fast["reason"] == "already_repaired"
    assert receipt["receipt_type"] == "incremental"
    assert receipt["prior_receipt_digest"] == anchor["receipt_digest"]
    assert receipt["generation_id"] == str(second["generation_id"])
    assert receipt["logical_payload_seed0"] == receipt["interval_payload_seed0"]
    assert receipt["logical_payload_seed1"] == receipt["interval_payload_seed1"]
    with db_connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
                "WHERE generation_id = %s AND namespace = 'default'",
                (str(second["generation_id"]),),
            )
            assert cur.fetchone()[0] == 1
        classification = classify_effective_attribute_recovery(
            conn, deployment_name=_DEPLOYMENT
        )
    assert classification["classification"] == "no_repair"
    assert classification["integrity_receipt"]["ok"] is True
    assert classification["payload_match"] is False

    # views.sql first revokes every okg_mcp table grant, then rebuilds its
    # allow-list. That migration-time reconciliation must preserve the same
    # integrity proof posture as ensure_attribute_integrity_schema; otherwise
    # every lifecycle migrate invalidates an otherwise healthy receipt.
    migrate(dsn)
    with db_connect(dsn, autocommit=True) as conn:
        migrated = classify_effective_attribute_recovery(
            conn, deployment_name=_DEPLOYMENT
        )
    assert migrated["integrity_receipt"]["ok"] is True, migrated[
        "integrity_receipt"
    ]
    assert migrated["classification"] == "no_repair", migrated


def test_dirty_integrity_state_forces_next_publish_to_reanchor(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A dirty proof recovers on the immediate retry, not at cadence +20."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "1000")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, range(3), {"k": "v"}))
    first = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    assert (
        first["attribute_effective_view_refresh"]["integrity_receipt"][
            "receipt_type"
        ]
        == "anchor"
    )

    _apply(dsn, _facts(suffix, [0], {"k": "changed"}, rev=2))
    delta = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    assert (
        delta["attribute_effective_view_refresh"]["integrity_receipt"][
            "receipt_type"
        ]
        == "incremental"
    )

    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        # Model the durable aftermath of a refused publish: protected state
        # changed, but no successor checkpoint was made visible.
        cur.execute(
            f"UPDATE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
            "SET materialized_at = materialized_at "
            "WHERE branch_id = 'default' AND namespace = 'default' "
            "AND to_ord = %s",
            (EFFECTIVE_INTERVAL_OPEN_ORD,),
        )
        cur.execute(
            f"SELECT dirty FROM {ATTRIBUTE_INTEGRITY_STATE_TABLE} "
            "WHERE deployment_name = %s AND branch_id = 'default'",
            (_DEPLOYMENT,),
        )
        assert cur.fetchone()[0] is True

    _apply(dsn, _facts(suffix, [100], {"k": "new"}))
    recovered = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    receipt = recovered["attribute_effective_view_refresh"]["integrity_receipt"]
    assert recovered["status"] == "published"
    assert receipt["advanced"] is True
    assert receipt["receipt_type"] == "anchor"

    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT metadata::jsonb->>'count_stamp_cadence' "
            "FROM _chronos_branch_interval_checkpoints "
            "WHERE checkpoint_id = %s",
            (recovered["generation_id"],),
        )
        assert cur.fetchone()[0] == "0"
        classification = classify_effective_attribute_recovery(
            conn, deployment_name=_DEPLOYMENT
        )
    assert classification["classification"] == "no_repair"
    assert classification["integrity_receipt"]["ok"] is True


def test_direct_interval_write_dirties_receipt_and_forces_full_fallback(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, range(2), {"k": "v"}))
    Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        repair_effective_attribute_interval_view(conn, deployment_name=_DEPLOYMENT)
        # schema-interval-narrow-split: the payload is FK-referenced through
        # attrs_hash, so the direct-write corruption vector is repointing the
        # hash at a divergent version row — still a protected-table write that
        # must dirty the receipt in the same transaction.
        corrupt_hash = _insert_version(
            cur, "node", f"doc:0:{suffix}", {"k": "v", "corrupt": True}
        )
        cur.execute(
            f"UPDATE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
            "SET attrs_hash = %s "
            "WHERE target_id = %s AND to_ord = %s",
            (corrupt_hash, f"doc:0:{suffix}", EFFECTIVE_INTERVAL_OPEN_ORD),
        )
        cur.execute(
            f"SELECT dirty FROM {ATTRIBUTE_INTEGRITY_STATE_TABLE} "
            "WHERE deployment_name = %s AND branch_id = 'default'",
            (_DEPLOYMENT,),
        )
        assert cur.fetchone()[0] is True
        repaired = repair_effective_attribute_interval_view(
            conn, deployment_name=_DEPLOYMENT
        )
    assert repaired.get("reason") != "already_repaired"
    assert repaired["interval_reconcile"]["rows_written"] > 0
    assert repaired["payload_match"] is True


def test_external_write_invalidates_active_attribute_integrity_transition(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, range(2), {"k": "v"}))
    Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()

    with db_connect(dsn, autocommit=True) as publisher_conn:
        repair_effective_attribute_interval_view(
            publisher_conn, deployment_name=_DEPLOYMENT
        )
        transition = prepare_attribute_integrity_transition(
            publisher_conn,
            deployment_name=_DEPLOYMENT,
            branch="default",
            namespace="default",
        )
        assert transition["eligible"] is True

        with db_connect(dsn, autocommit=True) as external, external.cursor() as cur:
            # The shared substrate writer role cannot inspect transition state.
            # A separate privileged backend still cannot impersonate the
            # Publisher session when it invokes the protected-table write path.
            cur.execute("SET ROLE okg_substrate")
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                cur.execute(f"SELECT * FROM {ATTRIBUTE_INTEGRITY_TRANSITION_TABLE}")
            cur.execute("RESET ROLE")
            # A row-touching write from another backend; a zero-row
            # statement would (correctly, under scoped invalidation)
            # leave the in-flight proof intact.
            cur.execute(
                f"UPDATE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
                "SET materialized_at = materialized_at "
                "WHERE namespace = 'default' AND branch_id = 'default'"
            )

        with publisher_conn.cursor() as cur:
            cur.execute(
                f"SELECT dirty FROM {ATTRIBUTE_INTEGRITY_STATE_TABLE} "
                "WHERE deployment_name = %s AND branch_id = 'default'",
                (_DEPLOYMENT,),
            )
            dirty = cur.fetchone()[0]
            cur.execute(
                "SELECT count(*) FROM "
                "okg.attribute_effective_integrity_transitions "
                "WHERE deployment_name = %s AND branch_id = 'default'",
                (_DEPLOYMENT,),
            )
            active_transitions = cur.fetchone()[0]
        assert dirty is True
        assert active_transitions == 0
        clear_attribute_integrity_transition(
            publisher_conn,
            deployment_name=_DEPLOYMENT,
            branch="default",
            namespace="default",
            transition_active=True,
        )


def test_publish_exception_clears_backend_bound_integrity_transition(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okg.substrate.publisher.publisher as publisher_module

    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, range(2), {"k": "v"}))
    Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    with db_connect(dsn, autocommit=True) as conn:
        repair_effective_attribute_interval_view(
            conn, deployment_name=_DEPLOYMENT
        )
    _apply(dsn, _facts(suffix, [0], {"k": "changed"}, rev=2))

    original_emit = publisher_module._emit_phase_event_with_timing

    def fail_after_transition(timings, name, payload):
        if name == "reserve_complete":
            raise RuntimeError("injected after transition prepare")
        return original_emit(timings, name, payload)

    monkeypatch.setattr(
        publisher_module,
        "_emit_phase_event_with_timing",
        fail_after_transition,
    )
    with pytest.raises(RuntimeError, match="after transition prepare"):
        Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()

    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM {ATTRIBUTE_INTEGRITY_TRANSITION_TABLE} "
            "WHERE deployment_name = %s AND branch_id = 'default'",
            (_DEPLOYMENT,),
        )
        assert cur.fetchone()[0] == 0


@pytest.mark.parametrize(
    ("table", "operation", "statement"),
    [
        (
            ATTRIBUTE_EFFECTIVE_VIEW_TABLE,
            "insert",
            # schema-interval-narrow-split: the probe row must satisfy the
            # composite FK, so mint its version row in the same statement.
            "WITH v AS ("
            "  INSERT INTO okg.attribute_versions"
            "    (target_kind, target_id, attrs_hash, attrs, raw_attrs)"
            "  VALUES ('node', 'receipt-probe',"
            "          okg.attribute_content_hash('{}'::jsonb, NULL),"
            "          '{}'::jsonb, NULL)"
            "  ON CONFLICT (target_kind, target_id, attrs_hash) DO NOTHING"
            "  RETURNING attrs_hash"
            ") "
            f"INSERT INTO {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
            "(target_kind, target_id, generation_id, namespace, attrs_hash) "
            "SELECT 'node', 'receipt-probe', 'receipt-probe', 'default', "
            "attrs_hash FROM v",
        ),
        (
            ATTRIBUTE_EFFECTIVE_VIEW_TABLE,
            "update",
            f"UPDATE {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
            "SET materialized_at = materialized_at "
            "WHERE namespace = 'default'",
        ),
        (
            ATTRIBUTE_EFFECTIVE_VIEW_TABLE,
            "delete",
            f"DELETE FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
            "WHERE namespace = 'default'",
        ),
        (
            ATTRIBUTE_EFFECTIVE_VIEW_TABLE,
            "truncate",
            f"TRUNCATE {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}",
        ),
        (
            ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE,
            "insert",
            "WITH v AS ("
            "  INSERT INTO okg.attribute_versions"
            "    (target_kind, target_id, attrs_hash, attrs, raw_attrs)"
            "  VALUES ('node', 'receipt-probe',"
            "          okg.attribute_content_hash('{}'::jsonb, NULL),"
            "          '{}'::jsonb, NULL)"
            "  ON CONFLICT (target_kind, target_id, attrs_hash) DO NOTHING"
            "  RETURNING attrs_hash"
            ") "
            f"INSERT INTO {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
            "(target_kind, target_id, namespace, branch_id, from_ord, to_ord, "
            "attrs_hash) "
            "SELECT 'node', 'receipt-probe', 'default', 'default', 0, 1, "
            "attrs_hash FROM v",
        ),
        (
            ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE,
            "update",
            f"UPDATE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
            "SET materialized_at = materialized_at "
            "WHERE namespace = 'default' AND branch_id = 'default'",
        ),
        (
            ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE,
            "delete",
            f"DELETE FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
            "WHERE namespace = 'default' AND branch_id = 'default'",
        ),
        (
            ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE,
            "truncate",
            f"TRUNCATE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE}",
        ),
    ],
    ids=lambda value: str(value).split(".")[-1],
)
def test_every_protected_attribute_write_marks_receipt_dirty_in_same_transaction(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    table: str,
    operation: str,
    statement: str,
) -> None:
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, [0], {"k": "v"}))
    Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    with db_connect(dsn, autocommit=True) as conn:
        repair_effective_attribute_interval_view(conn, deployment_name=_DEPLOYMENT)

    with db_connect(dsn, autocommit=False) as conn, conn.cursor() as cur:
        cur.execute(statement)
        cur.execute(
            f"SELECT dirty, dirty_reason FROM {ATTRIBUTE_INTEGRITY_STATE_TABLE} "
            "WHERE deployment_name = %s AND branch_id = 'default'",
            (_DEPLOYMENT,),
        )
        dirty, reason = cur.fetchone()
        assert dirty is True
        assert table.split(".")[-1] in reason
        assert operation.upper() in reason
        conn.rollback()


def test_missing_generation_link_forces_new_full_anchor(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, [0], {"k": "v"}))
    Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    with db_connect(dsn, autocommit=True) as conn:
        repair_effective_attribute_interval_view(conn, deployment_name=_DEPLOYMENT)
    _apply(dsn, _facts(suffix, [0], {"k": "changed"}, rev=2))
    latest = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()

    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE okg.generation_ordinals SET previous_checkpoint_id = %s "
            "WHERE checkpoint_id = %s",
            ("missing-generation-link", str(latest["generation_id"])),
        )
        repaired = repair_effective_attribute_interval_view(
            conn, deployment_name=_DEPLOYMENT
        )
    assert repaired.get("reason") != "already_repaired"
    assert repaired["integrity_receipt"]["receipt_type"] == "anchor"


def test_protected_ddl_mismatch_is_observed_before_repair_reinstalls_trigger(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, [0], {"k": "v"}))
    Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        repair_effective_attribute_interval_view(conn, deployment_name=_DEPLOYMENT)
        cur.execute(
            """
            SELECT t.tgname
              FROM pg_trigger t
             WHERE t.tgrelid = %s::regclass
               AND t.tgname LIKE 'attribute_integrity_dirty_%%'
             ORDER BY t.tgname
             LIMIT 1
            """,
            (ATTRIBUTE_EFFECTIVE_VIEW_TABLE,),
        )
        trigger_name = cur.fetchone()[0]
        cur.execute(
            f"DROP TRIGGER {trigger_name} ON {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}"
        )
        repaired = repair_effective_attribute_interval_view(
            conn, deployment_name=_DEPLOYMENT
        )
        cur.execute(
            "SELECT count(*) FROM pg_trigger WHERE tgrelid = %s::regclass "
            "AND tgname = %s",
            (ATTRIBUTE_EFFECTIVE_VIEW_TABLE, trigger_name),
        )
        trigger_restored = int(cur.fetchone()[0]) == 1
    assert repaired.get("reason") != "already_repaired"
    assert repaired["integrity_receipt"]["receipt_type"] == "anchor"
    assert trigger_restored is True


@pytest.mark.parametrize("role", ["okg_substrate", "app_rw"])
def test_runtime_role_cannot_mark_attribute_integrity_receipt_clean(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch, role: str
) -> None:
    """Direct state/receipt DML is revoked; only the finalizer API remains."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, [0], {"k": "v"}))
    Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        repair_effective_attribute_interval_view(conn, deployment_name=_DEPLOYMENT)
        # Non-vacuous even where the role lacks schema USAGE (role_split is
        # a runtime-bootstrap script): the table-level authority itself must
        # be absent, not merely unreachable.
        cur.execute(
            "SELECT has_table_privilege(%s, %s::regclass, 'UPDATE'), "
            "has_table_privilege(%s, %s::regclass, 'INSERT')",
            (
                role,
                ATTRIBUTE_INTEGRITY_STATE_TABLE,
                role,
                ATTRIBUTE_INTEGRITY_RECEIPT_TABLE,
            ),
        )
        assert cur.fetchone() == (False, False)
        cur.execute(f"SET ROLE {role}")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            cur.execute(
                f"UPDATE {ATTRIBUTE_INTEGRITY_STATE_TABLE} "
                "SET dirty = FALSE WHERE deployment_name = %s",
                (_DEPLOYMENT,),
            )
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            cur.execute(
                f"INSERT INTO {ATTRIBUTE_INTEGRITY_RECEIPT_TABLE} "
                "(receipt_digest, receipt_type, deployment_name, branch_id, "
                "namespace, generation_id, ordinal, checkpoint_node_count, "
                "checkpoint_edge_count, logical_node_count, logical_edge_count, "
                "schema_version, schema_digest, migration_digest, "
                "logical_payload_seed0, logical_payload_seed1, "
                "interval_payload_seed0, interval_payload_seed1, "
                "dirty_scope_digest, mutation_epoch) "
                "VALUES ('sha256:forged', 'anchor', %s, 'default', 'default', "
                "'g', 0, 0, 0, 0, 0, 2, 'd', 'm', 0, 0, 0, 0, 'scope', 0)",
                (_DEPLOYMENT,),
            )
        cur.execute("RESET ROLE")


def test_only_validated_api_finalizes_matching_transition_and_epoch(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The SECURITY DEFINER finalizer demands a live backend token + epoch CAS."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, [0], {"k": "v"}))
    Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    with db_connect(dsn, autocommit=True) as conn:
        repair_effective_attribute_interval_view(conn, deployment_name=_DEPLOYMENT)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT mutation_epoch FROM {ATTRIBUTE_INTEGRITY_STATE_TABLE} "
                "WHERE deployment_name = %s AND branch_id = 'default'",
                (_DEPLOYMENT,),
            )
            epoch = int(cur.fetchone()[0])

    def finalize(cur, digest: str, mutation_epoch: int):
        cur.execute(
            "SELECT okg.finalize_attribute_integrity_receipt("
            "%s, 'anchor', %s, 'default', 'default', 'g', 0, NULL, "
            "0, 0, 0, 0, 2, 'd', 'm', 0, 0, 0, 0, 'scope', %s)",
            (f"{digest}-{suffix}", _DEPLOYMENT, mutation_epoch),
        )
        return cur.fetchone()

    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        # okg_substrate is the sole finalizer principal. The migrated test
        # DB lacks role_split's schema USAGE grant, so mirror it here; the
        # role still has no direct state/receipt DML, so this exercises the
        # exact "API-only" authority surface end to end.
        cur.execute("GRANT USAGE ON SCHEMA okg TO okg_substrate")
        try:
            cur.execute("SET ROLE okg_substrate")
            # Without a live backend-bound transition the finalizer refuses.
            with pytest.raises(
                psycopg.errors.RaiseException,
                match="transition changed during finalization",
            ):
                finalize(cur, "sha256:no-transition", epoch)
            # A live transition with a stale mutation epoch also refuses; the
            # aborted statement rolls back atomically, appending no receipt.
            cur.execute(
                "SELECT okg.begin_attribute_integrity_transition(%s, %s, %s)",
                (_DEPLOYMENT, "default", "default"),
            )
            with pytest.raises(
                psycopg.errors.RaiseException,
                match="transition changed during finalization",
            ):
                finalize(cur, "sha256:stale-epoch", epoch + 999)
            cur.execute(
                f"SELECT count(*) FROM {ATTRIBUTE_INTEGRITY_RECEIPT_TABLE} "
                "WHERE receipt_digest IN (%s, %s)",
                (
                    f"sha256:no-transition-{suffix}",
                    f"sha256:stale-epoch-{suffix}",
                ),
            )
            assert cur.fetchone()[0] == 0
            # Matching token and epoch is the one accepted path.
            cur.execute(
                "SELECT okg.begin_attribute_integrity_transition(%s, %s, %s)",
                (_DEPLOYMENT, "default", "default"),
            )
            assert finalize(cur, "sha256:api-finalized", epoch)[0] is True
        finally:
            cur.execute("RESET ROLE")
            cur.execute("REVOKE USAGE ON SCHEMA okg FROM okg_substrate")
        cur.execute(
            f"SELECT dirty, clean_mutation_epoch, latest_receipt_digest "
            f"FROM {ATTRIBUTE_INTEGRITY_STATE_TABLE} "
            "WHERE deployment_name = %s AND branch_id = 'default'",
            (_DEPLOYMENT,),
        )
        assert cur.fetchone() == (
            False,
            epoch,
            f"sha256:api-finalized-{suffix}",
        )


_FINALIZE_SIGNATURE = (
    "okg.finalize_attribute_integrity_receipt"
    "(TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, BIGINT, TEXT, BIGINT, BIGINT, "
    "BIGINT, BIGINT, INTEGER, TEXT, TEXT, NUMERIC, NUMERIC, NUMERIC, "
    "NUMERIC, TEXT, BIGINT)"
)


@pytest.mark.parametrize(
    ("drift_sql", "undo_sql"),
    [
        (
            "ALTER FUNCTION okg.finish_attribute_integrity_transition"
            "(TEXT, TEXT, TEXT) SET work_mem = '64MB'",
            "ALTER FUNCTION okg.finish_attribute_integrity_transition"
            "(TEXT, TEXT, TEXT) RESET work_mem",
        ),
        # app_rw holds no finalizer authority after 0015; granting it is
        # exactly the drift the identity must catch.
        (
            f"GRANT EXECUTE ON FUNCTION {_FINALIZE_SIGNATURE} TO app_rw",
            f"REVOKE EXECUTE ON FUNCTION {_FINALIZE_SIGNATURE} FROM app_rw",
        ),
    ],
    ids=["function-config-drift", "privilege-drift"],
)
def test_transition_authority_drift_forces_full_fallback(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    drift_sql: str,
    undo_sql: str,
) -> None:
    """Authority-surface drift must invalidate the fast path, not be blessed."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, [0], {"k": "v"}))
    Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    with db_connect(dsn, autocommit=True) as conn:
        repair_effective_attribute_interval_view(conn, deployment_name=_DEPLOYMENT)
        healthy = repair_effective_attribute_interval_view(
            conn, deployment_name=_DEPLOYMENT
        )
        assert healthy["reason"] == "already_repaired"
        try:
            with conn.cursor() as cur:
                cur.execute(drift_sql)
            drifted = repair_effective_attribute_interval_view(
                conn, deployment_name=_DEPLOYMENT
            )
            assert drifted.get("reason") != "already_repaired"
            # The full fallback re-anchored under the new authority
            # surface, so the fast path is healthy again against the
            # drifted identity.
            settled = repair_effective_attribute_interval_view(
                conn, deployment_name=_DEPLOYMENT
            )
            assert settled["reason"] == "already_repaired"
        finally:
            # The session-scoped test DB persists across tests; put the
            # authority surface back.
            with conn.cursor() as cur:
                cur.execute(undo_sql)


def test_transition_authority_privileges_survive_full_runtime_bootstrap(
    clean_runtime: str,
) -> None:
    """Migrate + views + role split keeps the backend-bound table hidden."""
    signature = "okg.begin_attribute_integrity_transition(text,text,text)"
    with db_connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT has_table_privilege('okg_substrate', %s, 'SELECT'), "
            "has_table_privilege('okg_substrate', %s, 'INSERT'), "
            "has_table_privilege('okg_mcp', %s, 'SELECT'), "
            "has_function_privilege('okg_substrate', %s, 'EXECUTE'), "
            "has_function_privilege('okg_mcp', %s, 'EXECUTE')",
            (
                ATTRIBUTE_INTEGRITY_TRANSITION_TABLE,
                ATTRIBUTE_INTEGRITY_TRANSITION_TABLE,
                ATTRIBUTE_INTEGRITY_TRANSITION_TABLE,
                signature,
                signature,
            ),
        )
        assert cur.fetchone() == (False, False, False, True, False)


def test_disabled_dirtying_trigger_drifts_identity_and_forces_fallback(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """DISABLE TRIGGER changes only tgenabled; the digest must still move."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, [0], {"k": "v"}))
    Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    with db_connect(dsn, autocommit=True) as conn:
        repair_effective_attribute_interval_view(conn, deployment_name=_DEPLOYMENT)
        healthy = repair_effective_attribute_interval_view(
            conn, deployment_name=_DEPLOYMENT
        )
        assert healthy["reason"] == "already_repaired"
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT t.tgname FROM pg_trigger t
                 WHERE t.tgrelid = %s::regclass
                   AND t.tgname LIKE 'attribute_integrity_scoped_update_%%'
                """,
                (ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE,),
            )
            trigger_name = cur.fetchone()[0]
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"ALTER TABLE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
                    f"DISABLE TRIGGER {trigger_name}"
                )
            drifted = repair_effective_attribute_interval_view(
                conn, deployment_name=_DEPLOYMENT
            )
            assert drifted.get("reason") != "already_repaired"
        finally:
            with conn.cursor() as cur:
                cur.execute(
                    f"ALTER TABLE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
                    f"ENABLE TRIGGER {trigger_name}"
                )


def test_self_heal_refuses_restricted_runtime_roles() -> None:
    """A runtime role must never own or install the authority surface.

    With the schema fully installed the call is a DDL-free no-op
    (migrations own convergence); with it missing it refuses instead of
    minting runtime-owned SECURITY DEFINER objects.
    """
    from okg.substrate.attribute_integrity import (
        ensure_attribute_integrity_schema,
    )
    from okg.substrate.errors import SubstrateError

    class _RoleCursor:
        def __init__(self, installed: bool, statements: list[str]) -> None:
            self.installed = installed
            self.statements = statements
            self.last = ""

        def __enter__(self):
            return self

        def __exit__(self, *args) -> None:
            return None

        def execute(self, query, params=None) -> None:
            self.last = str(query)
            self.statements.append(self.last)

        def fetchone(self):
            if "current_user" in self.last:
                return ("okg_substrate",)
            return (self.installed,)

    class _RoleConnection:
        def __init__(self, installed: bool) -> None:
            self.installed = installed
            self.statements: list[str] = []

        def cursor(self) -> _RoleCursor:
            return _RoleCursor(self.installed, self.statements)

    missing = _RoleConnection(installed=False)
    with pytest.raises(SubstrateError, match="restricted runtime role"):
        ensure_attribute_integrity_schema(missing)

    healthy = _RoleConnection(installed=True)
    ensure_attribute_integrity_schema(healthy)
    assert not any(
        "CREATE" in statement or "GRANT" in statement or "REVOKE" in statement
        for statement in healthy.statements
    )


def test_restricted_role_ensure_is_noop_or_refuses_on_real_catalog(
    clean_runtime: str,
) -> None:
    """The restricted-role probe must read the real catalog correctly.

    Healthy schema: a DDL-free no-op. A dropped or disabled dirtying
    trigger: loud refusal — never a blessed no-op over a surface whose
    writes would escape invalidation.
    """
    from okg.substrate.attribute_integrity import (
        ensure_attribute_integrity_schema,
    )
    from okg.substrate.errors import SubstrateError

    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SET ROLE app_rw")
        ensure_attribute_integrity_schema(conn)
        cur.execute("RESET ROLE")
        cur.execute(
            """
            SELECT t.tgname FROM pg_trigger t
             WHERE t.tgrelid = %s::regclass
               AND t.tgname LIKE 'attribute_integrity_scoped_insert_%%'
            """,
            (ATTRIBUTE_EFFECTIVE_VIEW_TABLE,),
        )
        trigger_name = cur.fetchone()[0]
        try:
            cur.execute(
                f"DROP TRIGGER {trigger_name} "
                f"ON {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}"
            )
            cur.execute("SET ROLE app_rw")
            with pytest.raises(SubstrateError, match="restricted runtime role"):
                ensure_attribute_integrity_schema(conn)
        finally:
            cur.execute("RESET ROLE")
            # The owner path restores the missing trigger.
            ensure_attribute_integrity_schema(conn)
        cur.execute(
            "SELECT count(*) FROM pg_trigger "
            "WHERE tgrelid = %s::regclass AND tgname = %s",
            (ATTRIBUTE_EFFECTIVE_VIEW_TABLE, trigger_name),
        )
        assert int(cur.fetchone()[0]) == 1
        try:
            cur.execute(
                f"ALTER TABLE {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
                f"DISABLE TRIGGER {trigger_name}"
            )
            cur.execute("SET ROLE app_rw")
            with pytest.raises(SubstrateError, match="restricted runtime role"):
                ensure_attribute_integrity_schema(conn)
        finally:
            cur.execute("RESET ROLE")
            cur.execute(
                f"ALTER TABLE {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
                f"ENABLE TRIGGER {trigger_name}"
            )
        cur.execute("SET ROLE app_rw")
        ensure_attribute_integrity_schema(conn)
        cur.execute("RESET ROLE")


def test_app_rw_holds_no_transition_or_finalizer_authority(
    clean_runtime: str,
) -> None:
    """Migration 0015 narrows every integrity API to okg_substrate."""
    signatures = [
        "okg.begin_attribute_integrity_transition(text,text,text)",
        "okg.attribute_integrity_transition_matches(text,text,text)",
        "okg.finish_attribute_integrity_transition(text,text,text)",
        "okg.abort_attribute_integrity_transition(text,text,text)",
        "okg.register_attribute_integrity_scope(text,text,text)",
        "okg.mark_attribute_integrity_scope_dirty(text,text,text,text)",
        "okg.finalize_attribute_integrity_receipt("
        + "text,text,text,text,text,text,bigint,text,bigint,bigint,bigint,"
        + "bigint,integer,text,text,numeric,numeric,numeric,numeric,text,"
        + "bigint)",
    ]
    with db_connect(clean_runtime, autocommit=True) as conn, (
        conn.cursor()
    ) as cur:
        for signature in signatures:
            cur.execute(
                "SELECT has_function_privilege('app_rw', %s, 'EXECUTE'), "
                "has_function_privilege('okg_substrate', %s, 'EXECUTE')",
                (signature, signature),
            )
            app_rw_can, substrate_can = cur.fetchone()
            assert app_rw_can is False, signature
            assert substrate_can is True, signature


def test_protected_write_dirties_only_the_touched_scope(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Scoped invalidation: untouched scopes keep epoch, dirty bit, and O(1)."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, [0], {"k": "v"}))
    Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        repair_effective_attribute_interval_view(conn, deployment_name=_DEPLOYMENT)
        cur.execute(
            "SELECT okg.register_attribute_integrity_scope(%s, %s, %s)",
            (_DEPLOYMENT, "default", "other-namespace"),
        )
        cur.execute(
            "SELECT okg.register_attribute_integrity_scope(%s, %s, %s)",
            (_DEPLOYMENT, "other-branch", "default"),
        )
        cur.execute(
            "SELECT okg.register_attribute_integrity_scope(%s, %s, %s)",
            (_DEPLOYMENT, "default", "moved-namespace"),
        )

        def scope_state(branch: str, namespace: str):
            cur.execute(
                f"SELECT mutation_epoch, dirty "
                f"FROM {ATTRIBUTE_INTEGRITY_STATE_TABLE} "
                "WHERE deployment_name = %s AND branch_id = %s "
                "AND namespace = %s",
                (_DEPLOYMENT, branch, namespace),
            )
            return cur.fetchone()

        default_before = scope_state("default", "default")
        other_ns_before = scope_state("default", "other-namespace")
        other_branch_before = scope_state("other-branch", "default")
        moved_before = scope_state("default", "moved-namespace")
        # Zero-row statements change no payload and must dirty nothing.
        cur.execute(
            f"UPDATE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
            "SET materialized_at = materialized_at WHERE FALSE"
        )
        assert scope_state("default", "default") == default_before
        assert scope_state("default", "other-namespace") == other_ns_before
        # A row-touching write in the default scope dirties it alone:
        # neither the sibling namespace nor the sibling branch moves.
        cur.execute(
            f"UPDATE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
            "SET materialized_at = materialized_at "
            "WHERE namespace = 'default' AND branch_id = 'default'"
        )
        default_after = scope_state("default", "default")
        assert default_after[0] == default_before[0] + 1
        assert default_after[1] is True
        assert scope_state("default", "other-namespace") == other_ns_before
        assert scope_state("other-branch", "default") == other_branch_before
        # An UPDATE that moves a row across namespaces dirties BOTH the
        # old and the new coordinates (old_rows/new_rows union).
        cur.execute(
            f"SELECT target_id FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
            "WHERE namespace = 'default' AND branch_id = 'default' LIMIT 1"
        )
        moved_target = cur.fetchone()[0]
        default_mid = scope_state("default", "default")
        cur.execute(
            f"UPDATE {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
            "SET namespace = 'moved-namespace' "
            "WHERE target_id = %s AND branch_id = 'default'",
            (moved_target,),
        )
        assert scope_state("default", "default")[0] == default_mid[0] + 1
        moved_after = scope_state("default", "moved-namespace")
        assert moved_after[0] == moved_before[0] + 1
        assert moved_after[1] is True
        assert scope_state("default", "other-namespace") == other_ns_before
        # TRUNCATE carries no transition tables: invalidation is global,
        # across namespaces and branches alike.
        cur.execute(f"TRUNCATE {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}")
        assert scope_state("default", "other-namespace")[0] == (
            other_ns_before[0] + 1
        )
        assert scope_state("default", "other-namespace")[1] is True
        assert scope_state("other-branch", "default")[0] == (
            other_branch_before[0] + 1
        )


def test_finalizer_refuses_chain_dangling_prior_receipt(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An incremental receipt must extend the scope's current chain head."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, [0], {"k": "v"}))
    Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        anchor = repair_effective_attribute_interval_view(
            conn, deployment_name=_DEPLOYMENT
        )["integrity_receipt"]
        cur.execute(
            f"SELECT mutation_epoch, latest_receipt_digest "
            f"FROM {ATTRIBUTE_INTEGRITY_STATE_TABLE} "
            "WHERE deployment_name = %s AND branch_id = 'default' "
            "AND namespace = 'default'",
            (_DEPLOYMENT,),
        )
        epoch, latest = cur.fetchone()
        assert latest == anchor["receipt_digest"]
        # Append a synthetic second receipt through the API so the chain
        # head moves past the anchor.
        cur.execute(
            "SELECT okg.begin_attribute_integrity_transition(%s, %s, %s)",
            (_DEPLOYMENT, "default", "default"),
        )
        head_digest = f"sha256:head-{suffix}"
        cur.execute(
            "SELECT okg.finalize_attribute_integrity_receipt("
            "%s, 'incremental', %s, 'default', 'default', 'g', 1, %s, "
            "0, 0, 0, 0, 2, 'd', 'm', 0, 0, 0, 0, 'scope', %s)",
            (head_digest, _DEPLOYMENT, latest, epoch),
        )
        # A finalize whose prior is the (now superseded) anchor digest is
        # chain-dangling and must refuse without appending.
        cur.execute(
            "SELECT okg.begin_attribute_integrity_transition(%s, %s, %s)",
            (_DEPLOYMENT, "default", "default"),
        )
        dangling_digest = f"sha256:dangling-{suffix}"
        with pytest.raises(
            psycopg.errors.RaiseException,
            match="receipt chain head moved during finalization",
        ):
            cur.execute(
                "SELECT okg.finalize_attribute_integrity_receipt("
                "%s, 'incremental', %s, 'default', 'default', 'g', 2, %s, "
                "0, 0, 0, 0, 2, 'd', 'm', 0, 0, 0, 0, 'scope', %s)",
                (dangling_digest, _DEPLOYMENT, latest, epoch),
            )
        cur.execute(
            f"SELECT count(*) FROM {ATTRIBUTE_INTEGRITY_RECEIPT_TABLE} "
            "WHERE receipt_digest = %s",
            (dangling_digest,),
        )
        assert cur.fetchone()[0] == 0
        # An anchor (NULL prior) legitimately resets the chain.
        cur.execute(
            "SELECT okg.begin_attribute_integrity_transition(%s, %s, %s)",
            (_DEPLOYMENT, "default", "default"),
        )
        cur.execute(
            "SELECT okg.finalize_attribute_integrity_receipt("
            "%s, 'anchor', %s, 'default', 'default', 'g', 0, NULL, "
            "0, 0, 0, 0, 2, 'd', 'm', 0, 0, 0, 0, 'scope', %s)",
            (f"sha256:reset-{suffix}", _DEPLOYMENT, epoch),
        )
        assert cur.fetchone()[0] is True


def test_lifecycle_repair_leaves_safe_full_snapshot_on_interval_failure(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, range(3), {"k": "v"}))
    Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    _apply(dsn, _facts(suffix, [0], {"k": "changed"}, rev=2))
    latest = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    generation = str(latest["generation_id"])

    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
            "WHERE generation_id = %s",
            (generation,),
        )
        before = int(cur.fetchone()[0])
        cur.execute(
            f"DELETE FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
            "WHERE target_id = %s AND to_ord = %s",
            (f"doc:2:{suffix}", EFFECTIVE_INTERVAL_OPEN_ORD),
        )

    def fail_after_close():
        raise RuntimeError("injected interval failure")

    monkeypatch.setattr(
        "okg.substrate.attributes._EFFECTIVE_INTERVAL_REPAIR_FAILPOINT",
        fail_after_close,
    )
    with db_connect(dsn, autocommit=True) as conn:
        with pytest.raises(RuntimeError, match="injected interval failure"):
            repair_effective_attribute_interval_view(
                conn, deployment_name=_DEPLOYMENT
            )
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
                "WHERE generation_id = %s",
                (generation,),
            )
            after = int(cur.fetchone()[0])
            assert after == int(latest["node_count"]) + int(latest["edge_count"])
            assert after > before
            cur.execute(
                f"SELECT dirty FROM {ATTRIBUTE_INTEGRITY_STATE_TABLE} "
                "WHERE deployment_name = %s AND branch_id = 'default'",
                (_DEPLOYMENT,),
            )
            assert cur.fetchone()[0] is True


def test_lifecycle_repair_refuses_publisher_lock_contention_without_mutation(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from okg.substrate.errors import SubstrateError
    from okg.substrate.runtime.source_sync_priority import (
        publisher_publish_advisory_keys,
    )

    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, range(2), {"k": "v"}))
    latest = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    generation = str(latest["generation_id"])

    key = publisher_publish_advisory_keys(_DEPLOYMENT)
    with db_connect(dsn, autocommit=True) as holder, db_connect(
        dsn, autocommit=True
    ) as contender:
        with holder.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s, %s)", key)
        with contender.cursor() as cur:
            cur.execute(
                f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
                "WHERE generation_id = %s",
                (generation,),
            )
            before = int(cur.fetchone()[0])
        with pytest.raises(SubstrateError, match="publisher lock is held"):
            repair_effective_attribute_interval_view(
                contender,
                deployment_name=_DEPLOYMENT,
                lock_wait_seconds=0,
            )
        with contender.cursor() as cur:
            cur.execute(
                f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
                "WHERE generation_id = %s",
                (generation,),
            )
            assert int(cur.fetchone()[0]) == before


def test_interval_publish_bounds_legacy_snapshot(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Interval-owned publishes MUST bound the legacy per-generation snapshot table
    (PACT bound-okg-workspace-disk-growth, task 4 -- a regression of okg#400). The
    snapshot is written every publish (changed-only in interval mode), so the prune
    must still run or it grows unbounded (33 GB / 27 generations on okg-workspace).
    Reads use the interval table and carry-forward needs only the previous snapshot,
    so it is pruned to EFFECTIVE_VIEW_RETENTION_GENERATIONS regardless of mode."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    suffix = uuid.uuid4().hex[:8]
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
        # clean_runtime truncates the _intervals table but NOT the snapshot, so
        # start from a clean snapshot to make the whole-table count deterministic.
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}")

    # Publish more than keep generations, each changing a different target.
    _apply(dsn, _facts(suffix, range(3), {"k": "v"}))
    Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    for i in range(3):
        _apply(dsn, _facts(suffix, [i], {"k": f"v{i + 2}"}, rev=2))
        Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()

    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT count(DISTINCT generation_id) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}"
        )
        retained = cur.fetchone()[0]
    assert retained <= EFFECTIVE_VIEW_RETENTION_GENERATIONS, retained


def test_prune_keeps_recent_generations_per_namespace(clean_runtime: str) -> None:
    """The prune keeps keep_generations PER NAMESPACE, not globally (review of PACT
    bound-okg-workspace-disk-growth task 4): a busy namespace's newer generations
    must NOT evict another namespace's current generation -> incomplete reads. A
    global rank would delete both of ns_a's generations because ns_b's is newest."""
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        ensure_attribute_schema(conn)
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}")
            for ns, gen, ago in [
                ("ns_a", "a1", "3 hours"),   # ns_a older  -> should be pruned
                ("ns_a", "a2", "2 hours"),   # ns_a newest -> must survive
                ("ns_b", "b1", "1 hour"),    # ns_b newest (globally newest) -> survive
            ]:
                attrs_hash = _insert_version(cur, "node", f"n:{gen}", {"g": gen})
                cur.execute(
                    f"""INSERT INTO {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}
                        (target_kind, target_id, generation_id, namespace,
                         materialized_at, attrs_hash)
                        VALUES ('node', %s, %s, %s, now() - %s::interval, %s)""",
                    (f"n:{gen}", gen, ns, ago, attrs_hash),
                )
        deleted = prune_effective_attribute_view_to_recent(conn, keep_generations=1)
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT namespace, generation_id FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}"
            )
            kept = {(r[0], r[1]) for r in cur.fetchall()}

    assert deleted == 1, deleted              # only ns_a's a1
    assert ("ns_a", "a2") in kept             # ns_a current generation survived
    assert ("ns_b", "b1") in kept             # ns_b current generation survived
    assert ("ns_a", "a1") not in kept         # ns_a's older generation pruned


def test_fresh_db_fast_path_interval_parity(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Issue #647 / fix-ivm-fresh-db-publish: the empty-overlay fast path
    and the interval arm are mutually exclusive. Before the fix, a fresh
    database publishing with the manifest fast-path flag AND
    OKG_IVM_INTERVAL_VIEW=1 (the shipped default since #624) wrote ZERO
    snapshot rows, so the interval sync opened ZERO intervals and the
    first-publish reconcile (ordinal 0 always reconciles) failed the
    publish against the full graph count."""
    monkeypatch.delenv("OKG_IVM_INTERVAL_VIEW", raising=False)  # shipped default
    suffix = uuid.uuid4().hex[:8]
    with db_connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(clean_runtime, _facts(suffix, range(6), {"k": "v"}))

    # The exact #647 shape: fast path enabled on a fresh DB, interval arm
    # active. The first publish must succeed with intervals fully open.
    res = Publisher(
        clean_runtime,
        _DEPLOYMENT,
        attribute_effective_view_empty_overlay_fast_path=True,
        catalog_identity=bind_test_catalog_writer(clean_runtime, _DEPLOYMENT),
    ).cycle()
    g1 = str(res["generation_id"])

    with db_connect(clean_runtime, autocommit=True) as conn:
        db = _db(conn)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.attribute_effective_view_intervals"
            )
            open_intervals = cur.fetchone()[0]
            cur.execute(
                f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
                "WHERE generation_id = %s AND target_kind = 'node'",
                (g1,),
            )
            snapshot_nodes = cur.fetchone()[0]
        # Ground-truth parity: intervals and snapshot both carry the full
        # published graph (6 nodes) — the fast path did NOT starve them.
        assert open_intervals == 6, (
            f"interval sync starved by the fast path: {open_intervals} open "
            "intervals for 6 published nodes (issue #647 regressed)"
        )
        assert snapshot_nodes == 6
        _assert_stab_equals_snapshot(conn, db, g1)


def test_materialize_mirror_refuses_fast_path_under_interval_arm(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Defense in depth for direct materialize callers: with the interval
    arm active, an explicit empty_overlay_fast_path=True request still
    writes a real snapshot (no empty_overlay_identity early return)."""
    monkeypatch.delenv("OKG_IVM_INTERVAL_VIEW", raising=False)  # shipped default
    suffix = uuid.uuid4().hex[:8]
    with db_connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(clean_runtime, _facts(suffix, range(3), {"k": "v"}))
    # Publish WITHOUT the fast path to mint the generation + ordinal.
    g1 = str(Publisher(clean_runtime, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(clean_runtime, _DEPLOYMENT)).cycle()["generation_id"])

    with db_connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}")
        out = materialize_effective_attribute_view_for_generation(
            conn,
            generation_id=g1,
            branch="default",
            previous_generation_id=None,
            empty_overlay_fast_path=True,
            from_checkpoint=True,
        )
        # Ground truth, not just the return payload: real snapshot rows
        # landed for this generation despite the fast-path request.
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
                "WHERE generation_id = %s AND target_kind = 'node'",
                (g1,),
            )
            assert cur.fetchone()[0] == 3
    assert out.get("reason") != "empty_overlay_identity", out
    assert out.get("node_rows") == 3, out


# --------------------------------------------------------------------------
# PACT bound-okg-workspace-disk-growth, req.bound-disk.intervals-sidecar:
# closed-interval retention pruner + raw_attrs presence-means-overlay witnesses.
# --------------------------------------------------------------------------


def _open_interval_count(conn, branch: str | None = None) -> int:
    where = "" if branch is None else "AND branch_id = %s"
    args = () if branch is None else (branch,)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
            f"WHERE to_ord = {EFFECTIVE_INTERVAL_OPEN_ORD} {where}",
            args,
        )
        return int(cur.fetchone()[0])


def test_closed_interval_prune_wired_into_publish_bounds_window_reads_recompute(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """THE NON-NEGOTIABLE: after the retention prune runs, effective-attribute
    reads on a RETAINED pin still equal a from-scratch recompute. Also proves the
    prune is WIRED into the publish path (the per-cycle bounded prune already
    bounded the window by g3) and that an OPEN interval is never touched.

    Ordinals g1<g2<g3 are the three newest; keep=2 (the shipped snapshot window),
    so keep_from_ord = ordinal(g2). doc:0's closed [ord(g1), ord(g2)) has
    to_ord == keep_from_ord -> OUTSIDE the window -> pruned; every OPEN interval
    (the live front) survives, so a read pinned to g2/g3 is unaffected."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")  # publisher auto-syncs+prunes
    dsn = clean_runtime
    suffix = uuid.uuid4().hex[:8]
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])

    _apply(dsn, _facts(suffix, range(8), {"k": "v"}))
    g1 = str(Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()["generation_id"])
    _apply(dsn, _facts(suffix, [0], {"k": "CHANGED"}, rev=2))
    _apply(dsn, _facts(suffix, [100, 101], {"k": "new"}))
    g2 = str(Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()["generation_id"])
    _apply(dsn, _facts(suffix, [1], {}, op="R", rev=2))
    Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()  # g3: pushes g1 out of the keep=2 window

    with db_connect(dsn, autocommit=True) as conn:
        db = _db(conn)
        open_before = _open_interval_count(conn, branch="default")
        # WIRING: the per-cycle prune (keep=2) already bounded the window by g3 --
        # nothing on the default branch is left outside it.
        assert (
            count_prunable_closed_intervals(conn, keep_generations=2, branch="default")
            == 0
        )
        # It never deleted an OPEN interval: the live front (8 - 1 retracted + 2 new
        # = 9 nodes) is intact and a re-run of the pruner deletes nothing.
        assert open_before == 9, open_before
        assert (
            prune_effective_interval_view_backlog(
                conn, keep_generations=2, batch_size=100, branch="default"
            )["deleted"]
            == 0
        )
        assert _open_interval_count(conn, branch="default") == open_before

        # THE NON-NEGOTIABLE: reads on the RETAINED pin g2 equal a from-scratch
        # recompute. Rebuild g2's FULL snapshot from its published checkpoint (a
        # genuine recompute of raw||overlay), then the interval stab at g2 -- served
        # entirely by intervals that survived the prune -- must match it row-for-row.
        with conn.cursor() as cur:
            cur.execute(
                f"DELETE FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} WHERE generation_id = %s",
                (g2,),
            )
        materialize_effective_attribute_view_for_generation(
            conn,
            generation_id=g2,
            from_checkpoint=True,
            previous_generation_id=None,
            empty_overlay_fast_path=False,
        )
        recompute_g2 = _snapshot_read(conn, g2)
        assert recompute_g2, "recompute produced no rows"
        assert _stab(conn, db, g2) == recompute_g2

        # The out-of-window pin g1 correctly LOST doc:0's closed history (pruned):
        # the attribute plane no longer serves that pin for doc:0 -- recompute does.
        assert ("node", f"doc:0:{suffix}") not in _stab(conn, db, g1)


def test_declared_full_retention_disables_legacy_publish_prune(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An explicit policy makes confirmed compaction the deletion authority."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    dsn = clean_runtime
    suffix = uuid.uuid4().hex[:8]
    policy = {
        "default": "full",
        "audit_tail_generations": 2,
        "nodes": {"doc": "full"},
        "edges": {},
    }
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])

    _apply(dsn, _facts(suffix, [0], {"revision": 1}, rev=1))
    g1 = str(
        Publisher(dsn, _DEPLOYMENT, history_retention_policy=policy, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()[
            "generation_id"
        ]
    )
    _apply(dsn, _facts(suffix, [0], {"revision": 2}, rev=2))
    Publisher(dsn, _DEPLOYMENT, history_retention_policy=policy, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    _apply(dsn, _facts(suffix, [0], {"revision": 3}, rev=3))
    latest = Publisher(
        dsn, _DEPLOYMENT, history_retention_policy=policy,
        catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT),
    ).cycle()

    with db_connect(dsn, autocommit=True) as conn:
        db = _db(conn)
        assert ("node", f"doc:0:{suffix}") in _stab(conn, db, g1)
        checkpoint = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).get_generation(str(latest["generation_id"]))
        assert checkpoint.metadata["effective_interval_view_prune"] == {
            "deleted": 0,
            "capped": False,
            "branch": "default",
            "skipped": "explicit_history_retention_policy",
        }


def test_closed_interval_prune_window_cap_open_and_idempotence(
    clean_runtime: str,
) -> None:
    """Precise, hermetic control of the window: build a known interval front over
    manual ordinals and prove the pruner (1) deletes exactly the CLOSED intervals
    with to_ord <= keep_from_ord, (2) is CAPPED per cycle (max_delete), (3) drains
    in bounded batches, (4) NEVER deletes an OPEN interval, (5) is idempotent, and
    (6) leaves a branch with fewer than keep_generations ordinals fully intact."""
    dsn = clean_runtime
    suffix = uuid.uuid4().hex[:8]
    branch = f"pb_{suffix}"      # main test branch: 5 ordinals 0..4
    tiny = f"tiny_{suffix}"      # a branch with a single ordinal -> no cutoff
    with db_connect(dsn, autocommit=True) as conn:
        ensure_attribute_schema(conn)
        with conn.cursor() as cur:
            for ordv in range(5):
                cur.execute(
                    "INSERT INTO okg.generation_ordinals "
                    "(checkpoint_id, branch_id, ordinal) VALUES (%s, %s, %s)",
                    (f"generation:{branch}:g{ordv}", branch, ordv),
                )
            cur.execute(
                "INSERT INTO okg.generation_ordinals "
                "(checkpoint_id, branch_id, ordinal) VALUES (%s, %s, %s)",
                (f"generation:{tiny}:g0", tiny, 0),
            )

            def iv(br: str, tid: str, from_ord: int, to_ord: int) -> tuple:
                # schema-interval-narrow-split: the row references its payload
                # in the version store via attrs_hash (FK-enforced).
                attrs_hash = _insert_version(cur, "node", tid, {"k": "v"})
                return (
                    "node", tid, "default", br, from_ord, to_ord,
                    "doc", None, None, None, 0, None, attrs_hash,
                )

            rows = [
                # OPEN live front (never pruned).
                iv(branch, f"open_a:{suffix}", 4, EFFECTIVE_INTERVAL_OPEN_ORD),
                iv(branch, f"open_b:{suffix}", 4, EFFECTIVE_INTERVAL_OPEN_ORD),
                iv(branch, f"open_c:{suffix}", 4, EFFECTIVE_INTERVAL_OPEN_ORD),
                # CLOSED outside window (keep=2 -> keep_from_ord=3; to_ord<=3).
                iv(branch, f"closed_a:{suffix}", 0, 1),
                iv(branch, f"closed_b:{suffix}", 1, 2),
                iv(branch, f"closed_c:{suffix}", 2, 3),   # to_ord == keep_from_ord
                # CLOSED inside window (to_ord=4 > 3 -> kept).
                iv(branch, f"keep_d:{suffix}", 2, 4),
                iv(branch, f"keep_e:{suffix}", 3, 4),
                # A one-ordinal branch: fewer than keep -> no cutoff, never pruned.
                iv(tiny, f"tiny_closed:{suffix}", 0, 1),
            ]
            cur.executemany(
                f"""
                INSERT INTO {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE}
                  (target_kind, target_id, namespace, branch_id, from_ord, to_ord,
                   subtype, src, dst, edge_type, overlay_count, raw_text_hash,
                   attrs_hash)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                rows,
            )

        # Window predicate: exactly the 3 closed-outside-window rows on `branch`.
        assert (
            count_prunable_closed_intervals(conn, keep_generations=2, branch=branch)
            == 3
        )
        # A branch with fewer than keep_generations ordinals has no cutoff.
        assert (
            count_prunable_closed_intervals(conn, keep_generations=2, branch=tiny)
            == 0
        )

        # (2) per-cycle CAP: a single bounded batch deletes at most max_delete.
        capped = prune_effective_interval_view_closed(
            conn, keep_generations=2, max_delete=2, branch=branch
        )
        assert capped["deleted"] == 2 and capped["capped"] is True, capped

        # (3) bounded-batch drain finishes the remaining 1.
        drain = prune_effective_interval_view_backlog(
            conn, keep_generations=2, batch_size=1, branch=branch
        )
        assert drain["deleted"] == 1 and drain["drained"] is True, drain

        # (1)+(4)+(5): only the out-of-window closed rows are gone; the OPEN front
        # and the in-window closed rows remain; a re-drain deletes nothing.
        assert (
            count_prunable_closed_intervals(conn, keep_generations=2, branch=branch)
            == 0
        )
        assert _open_interval_count(conn, branch=branch) == 3
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT target_id, to_ord FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
                "WHERE branch_id = %s ORDER BY target_id",
                (branch,),
            )
            remaining = dict(cur.fetchall())
        assert f"closed_a:{suffix}" not in remaining
        assert f"closed_b:{suffix}" not in remaining
        assert f"closed_c:{suffix}" not in remaining
        assert remaining[f"keep_d:{suffix}"] == 4
        assert remaining[f"keep_e:{suffix}"] == 4
        assert (
            prune_effective_interval_view_backlog(
                conn, keep_generations=2, batch_size=10, branch=branch
            )["deleted"]
            == 0
        )
        # (6) the tiny branch was never touched.
        assert _open_interval_count(conn, branch=tiny) == 0
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
                "WHERE branch_id = %s",
                (tiny,),
            )
            assert cur.fetchone()[0] == 1


def test_raw_attrs_presence_means_overlay_round_trip(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """presence-means-overlay: the writer stores raw_attrs ONLY when it byte-differs
    from attrs. A no-overlay row stores NULL in BOTH the snapshot and the interval,
    and the reader recovers the raw via COALESCE(raw_attrs, attrs). An overlay row
    stores the DIVERGENT raw and the stab reads it back."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    dsn = clean_runtime
    suffix = uuid.uuid4().hex[:8]
    doc0 = f"doc:0:{suffix}"
    with db_connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, range(3), {"k": "v"}))
    g1 = str(Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()["generation_id"])

    with db_connect(dsn, autocommit=True) as conn:
        db = _db(conn)
        ra_idx = _INTERVAL_READ_COLS.index("raw_attrs") - 1
        attrs_idx = _INTERVAL_READ_COLS.index("attrs") - 1
        # WRITER OMITS when equal: every no-overlay row stores raw_attrs NULL in
        # the VERSION STORE (schema-interval-narrow-split: the snapshot and
        # interval rows reference the payload through attrs_hash), yet the
        # reader fallback yields the true raw (== the attrs payload, whatever
        # source-lineage keys it carries).
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} v "
                "JOIN okg.attribute_versions av "
                "  ON av.target_kind = v.target_kind "
                " AND av.target_id = v.target_id "
                " AND av.attrs_hash = v.attrs_hash "
                "WHERE v.generation_id = %s AND av.raw_attrs IS NOT NULL",
                (g1,),
            )
            assert cur.fetchone()[0] == 0            # snapshot refs: all NULL raw
            cur.execute(
                f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} i "
                "JOIN okg.attribute_versions av "
                "  ON av.target_kind = i.target_kind "
                " AND av.target_id = i.target_id "
                " AND av.attrs_hash = i.attrs_hash "
                "WHERE av.raw_attrs IS NOT NULL"
            )
            assert cur.fetchone()[0] == 0            # interval refs: all NULL raw
            # READER FALLS BACK: COALESCE(raw_attrs, attrs) recovers the payload.
            cur.execute(
                f"SELECT av.raw_attrs, av.attrs, "
                "COALESCE(av.raw_attrs, av.attrs) "
                f"FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} v "
                "JOIN okg.attribute_versions av "
                "  ON av.target_kind = v.target_kind "
                " AND av.target_id = v.target_id "
                " AND av.attrs_hash = v.attrs_hash "
                "WHERE v.generation_id = %s AND v.target_id = %s",
                (g1, doc0),
            )
            raw1, attrs1, fallback1 = cur.fetchone()
        raw_base = attrs1                            # no overlay -> raw base == attrs
        assert raw1 is None                          # writer omitted the duplicate raw
        assert fallback1 == raw_base                 # reader fallback == the raw base
        assert raw_base["k"] == "v"
        # The stab (interval reader) carries raw_attrs NULL for the no-overlay row.
        stab1 = _stab(conn, db, g1)
        assert stab1[("node", doc0)][ra_idx] is None
        assert stab1[("node", doc0)][attrs_idx] == raw_base

        # OVERLAY CASE: an overlay diverges attrs from raw, so raw_attrs is STORED.
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        db, ctx = store.context.db, store.context
        g2 = f"{g1}:ov"
        record_attribute_observation(
            conn, target_kind="node", target_id=doc0, generation_id=g2,
            attr_key="ov", value="x", producer="test",
        )
        materialize_effective_attributes_for_generation(conn, generation_id=g2)
        materialize_effective_attribute_view_for_generation(
            conn, generation_id=g2, previous_generation_id=g1,
            dirty_targets=DirtyTargets(node_ids=frozenset(), edge_ids=frozenset()),
        )
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT av.raw_attrs, av.attrs "
                f"FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} v "
                "JOIN okg.attribute_versions av "
                "  ON av.target_kind = v.target_kind "
                " AND av.target_id = v.target_id "
                " AND av.attrs_hash = v.attrs_hash "
                "WHERE v.generation_id = %s AND v.target_id = %s",
                (g2, doc0),
            )
            raw_attrs, attrs = cur.fetchone()
        assert raw_attrs == raw_base                 # divergent raw STORED (== raw base)
        assert attrs == {**raw_base, "ov": "x"}      # merged = raw || overlay
        assert "ov" not in raw_attrs                 # raw base carries no overlay key
        # The interval carries the same divergent raw after a sync.
        assign_generation_ordinal(db, ctx, checkpoint_id=g2, branch="default")
        sync_effective_interval_view_for_generation(
            conn, db, generation_id=g2,
            dirty_targets=DirtyTargets(node_ids=frozenset(), edge_ids=frozenset()),
        )
        stab2 = _stab(conn, db, g2)
        assert stab2[("node", doc0)][ra_idx] == raw_base
        assert stab2[("node", doc0)][attrs_idx] == {**raw_base, "ov": "x"}


def test_backfill_raw_attrs_dedup_is_structural_noop_post_split(
    clean_runtime: str,
) -> None:
    """schema-interval-narrow-split: the interval/snapshot tables no longer carry
    raw_attrs/attrs columns (payloads live in okg.attribute_versions, where the
    writer's NULLIF enforces presence-means-overlay and the content hash seals
    it — a post-hoc NULLing would make the stored hash dishonest). The legacy
    dedup backfill must therefore report a structural no-op with an explicit
    reason instead of erroring on the missing column."""
    dsn = clean_runtime
    with db_connect(dsn, autocommit=True) as conn:
        ensure_attribute_schema(conn)
        report = backfill_effective_interval_raw_attrs_dedup(
            conn, batch_size=1, include_snapshot=True
        )
        assert report["narrow_split_active"] is True, report
        assert report["intervals_nulled"] == 0, report
        assert report["snapshot_nulled"] == 0, report
        assert report["drained"] is True, report
        # Idempotent: a re-run is the same structural no-op.
        again = backfill_effective_interval_raw_attrs_dedup(
            conn, batch_size=10, include_snapshot=True
        )
        assert again["intervals_nulled"] == 0 and again["snapshot_nulled"] == 0, again
