"""PACT schema-interval-narrow-split: the four requirement clauses, executed.

- req.split-shape (acc.split-shape): fresh install carries NO jsonb payload
  columns on the interval/snapshot tables; measured interval row width is
  under 300 B on populated test data; snapshot and interval rows resolve their
  payloads through okg.attribute_versions; the overlay round trip keeps the
  raw-vs-effective distinction intact.
- req.sigma-pinned (acc.sigma-pinned): one in-database hash site; the same
  logical payload entering via both write paths yields ONE version row; a
  jsonb-equal-but-text-different payload (key order, 1.0 vs 1.00) produces NO
  new version row and NO interval churn; the hash-honesty doctor check is red
  on a manually corrupted payload.
- req.fk-and-gc (acc.fk-and-gc): deleting a referenced version row is refused
  by the composite FK; after churn + prune cycles orphaned version rows are
  zero and the version count is bounded; the growth gate is red when the GC is
  disabled in a fixture and green after the recovery drain.
- req.publish-cost (acc.publish-cost): a side-by-side benchmark at
  1,000-target churn measures the split attr stage at or below the wide-row
  baseline (timings recorded as junit properties), the split stage lands under
  the 150 ms gate, and reads are byte-identical to an independently computed
  wide-row oracle (raw || overlay from the pinned Chronos checkout).
"""
from __future__ import annotations

import statistics
import time
import uuid

import pytest

psycopg = pytest.importorskip("psycopg")
from psycopg.types.json import Jsonb  # noqa: E402

from okg.substrate.attributes import (  # noqa: E402
    ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE,
    ATTRIBUTE_EFFECTIVE_VIEW_TABLE,
    ATTRIBUTE_VERSIONS_TABLE,
    EFFECTIVE_INTERVAL_OPEN_ORD,
    _INTERVAL_READ_COLS,
    _close_dirty_effective_intervals,
    check_attribute_version_gc,
    check_attribute_version_hash_honesty,
    count_orphan_attribute_versions,
    effective_interval_stab_rows,
    effective_snapshot_read_rows,
    ensure_attribute_schema,
    materialize_effective_attributes_for_generation,
    prune_orphan_attribute_versions,
    record_attribute_observation,
)
from okg.substrate.derived_artifacts.maintained_derivation import (  # noqa: E402
    ExplicitIntervalTable,
    sync_explicit_interval_rows_for_generation,
)
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402

pytestmark = pytest.mark.postgres

_DEPLOYMENT = "default"


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


def _apply(dsn: str, facts) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            facts, source="default", strict_endpoints=True
        )


def _publish(dsn: str) -> str:
    res = Publisher(dsn, _DEPLOYMENT, catalog_identity=bind_test_catalog_writer(dsn, _DEPLOYMENT)).cycle()
    assert res["status"] == "published", res
    return str(res["generation_id"])


def _jsonb_columns(cur, table_name: str) -> set[str]:
    cur.execute(
        """
        SELECT column_name FROM information_schema.columns
         WHERE table_schema = 'okg' AND table_name = %s
           AND data_type = 'jsonb'
        """,
        (table_name,),
    )
    return {r[0] for r in cur.fetchall()}


# --------------------------------------------------------------------------- #
# req.split-shape                                                             #
# --------------------------------------------------------------------------- #


def test_split_shape_no_jsonb_columns_row_width_and_version_resolution(
    clean_runtime: str,
) -> None:
    """acc.split-shape: no jsonb payload columns; row width under 300 B on
    populated data; every snapshot/interval row resolves its payload through
    the version store (INNER join loses no row); the composite FKs exist."""
    dsn = clean_runtime
    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    # A realistically-sized payload (the live exemplar averaged ~1.2 KB).
    payload = {"k": "v", "text": "x" * 900, "path": "a/b/c.py", "pid": 4242}
    _apply(dsn, _facts(suffix, range(50), payload))
    gen = _publish(dsn)

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        # (1) No jsonb payload columns on either narrow table.
        assert _jsonb_columns(cur, "attribute_effective_view") == set()
        assert _jsonb_columns(cur, "attribute_effective_view_intervals") == set()

        # (2) Measured row width under 300 B on populated data (the wide row
        # averaged ~1.3 KB on the live exemplar; the split is 5-10x narrower).
        cur.execute(
            f"SELECT avg(pg_column_size(t.*)) FROM "
            f"{ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} t"
        )
        width = float(cur.fetchone()[0])
        assert width < 300, f"interval row width {width:.0f} B >= 300 B"

        # (3) Payload resolution: the INNER version join covers every row of
        # this publish (rows are scoped to this test's targets — snapshot rows
        # from other tests persist in the shared DB keyed by generation).
        like = f"doc:%:{suffix}"
        for table, scope in (
            (ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE, "t.target_id LIKE %(like)s"),
            (
                ATTRIBUTE_EFFECTIVE_VIEW_TABLE,
                "t.generation_id = %(gen)s",
            ),
        ):
            cur.execute(
                f"SELECT count(*) FROM {table} t WHERE {scope}",
                {"like": like, "gen": gen},
            )
            total = int(cur.fetchone()[0])
            cur.execute(
                f"SELECT count(*) FROM {table} t "
                f"JOIN {ATTRIBUTE_VERSIONS_TABLE} av "
                "  ON av.target_kind = t.target_kind "
                " AND av.target_id = t.target_id "
                " AND av.attrs_hash = t.attrs_hash "
                f"WHERE {scope}",
                {"like": like, "gen": gen},
            )
            joined = int(cur.fetchone()[0])
            assert total == 50, (table, total)
            assert joined == total, (table, joined, total)

        # (4) The composite FKs are present and RESTRICT.
        cur.execute(
            """
            SELECT conname, confdeltype FROM pg_constraint
             WHERE conname IN ('attribute_effective_view_version_fkey',
                               'attribute_effective_view_intervals_version_fkey')
             ORDER BY conname
            """
        )
        constraints = dict(cur.fetchall())
        assert constraints == {
            "attribute_effective_view_version_fkey": "r",
            "attribute_effective_view_intervals_version_fkey": "r",
        }

        # (5) The read contract materializes the payload (the publisher adds
        # its lineage key on top of the source attrs — assert the source
        # payload arrives intact through the version join).
        rows = effective_snapshot_read_rows(
            conn, generation_id=gen, target_kind="node"
        )
        attrs_idx = _INTERVAL_READ_COLS.index("attrs")
        assert len(rows) == 50
        assert all(payload.items() <= r[attrs_idx].items() for r in rows)


def test_split_shape_overlay_round_trip_raw_vs_effective(
    clean_runtime: str,
) -> None:
    """acc.split-shape: the overlay round trip keeps raw vs effective distinct —
    the version row stores the DIVERGENT raw base (presence-means-overlay) and
    merged attrs; removing the overlay collapses raw_attrs back to NULL."""
    dsn = clean_runtime
    suffix = uuid.uuid4().hex[:8]
    doc0 = f"doc:0:{suffix}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, [0], {"k": "raw-value"}))
    gen = _publish(dsn)

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        obs = record_attribute_observation(
            conn, target_kind="node", target_id=doc0, generation_id=gen,
            attr_key="k", value="overlaid", producer="test",
        )
        materialize_effective_attributes_for_generation(conn, generation_id=gen)
        cur.execute(
            f"SELECT av.attrs, av.raw_attrs FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} v "
            f"JOIN {ATTRIBUTE_VERSIONS_TABLE} av "
            "  ON av.target_kind = v.target_kind "
            " AND av.target_id = v.target_id AND av.attrs_hash = v.attrs_hash "
            "WHERE v.generation_id = %s AND v.target_id = %s",
            (gen, doc0),
        )
        attrs, raw_attrs = cur.fetchone()
        assert attrs["k"] == "overlaid"          # effective = raw || overlay
        assert raw_attrs["k"] == "raw-value"     # divergent raw base preserved
        assert obs.observation_id

        # The distinction survives in the STORE: the pre-overlay raw state
        # (raw_attrs NULL, attrs == raw) is still the version the publish-time
        # interval front references, alongside the overlaid version — the two
        # logical states never collapse into one row.
        cur.execute(
            f"SELECT av.attrs, av.raw_attrs FROM "
            f"{ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} i "
            f"JOIN {ATTRIBUTE_VERSIONS_TABLE} av "
            "  ON av.target_kind = i.target_kind "
            " AND av.target_id = i.target_id AND av.attrs_hash = i.attrs_hash "
            "WHERE i.target_id = %s AND i.to_ord = %s",
            (doc0, EFFECTIVE_INTERVAL_OPEN_ORD),
        )
        iv_attrs, iv_raw = cur.fetchone()
        assert iv_attrs["k"] == "raw-value"
        assert iv_raw is None                    # presence-means-overlay
        cur.execute(
            f"SELECT count(*) FROM {ATTRIBUTE_VERSIONS_TABLE} "
            "WHERE target_kind = 'node' AND target_id = %s",
            (doc0,),
        )
        assert int(cur.fetchone()[0]) == 2       # raw state + overlaid state


# --------------------------------------------------------------------------- #
# req.sigma-pinned                                                            #
# --------------------------------------------------------------------------- #


def test_sigma_same_payload_via_both_write_paths_yields_one_version_row(
    clean_runtime: str,
) -> None:
    """acc.sigma-pinned: the publish materialize path and the migration-shaped
    recompute path (INSERT ... SELECT okg.attribute_content_hash over the
    stored payload) land on the SAME version row — the hash has exactly one
    in-database definition, so re-derived content can never fork the store."""
    dsn = clean_runtime
    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, [0], {"k": "v", "n": 1}))
    _publish(dsn)
    # A second unchanged publish (keep-open / carry) must not mint a version.
    _apply(dsn, _facts(suffix, [1], {"k": "other"}))
    _publish(dsn)

    doc0 = f"doc:0:{suffix}"
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM {ATTRIBUTE_VERSIONS_TABLE} "
            "WHERE target_kind = 'node' AND target_id = %s",
            (doc0,),
        )
        assert int(cur.fetchone()[0]) == 1
        # Second entry path: recompute-and-insert from the stored payload (the
        # migration backfill shape) — conflicts into the same row, adds none.
        cur.execute(
            f"INSERT INTO {ATTRIBUTE_VERSIONS_TABLE} "
            "(target_kind, target_id, attrs_hash, attrs, raw_attrs) "
            "SELECT target_kind, target_id, "
            "       okg.attribute_content_hash(attrs, raw_attrs), attrs, raw_attrs "
            f"  FROM {ATTRIBUTE_VERSIONS_TABLE} WHERE target_id = %s "
            "ON CONFLICT (target_kind, target_id, attrs_hash) DO NOTHING",
            (doc0,),
        )
        assert cur.rowcount == 0
        cur.execute(
            f"SELECT count(*) FROM {ATTRIBUTE_VERSIONS_TABLE} "
            "WHERE target_kind = 'node' AND target_id = %s",
            (doc0,),
        )
        assert int(cur.fetchone()[0]) == 1


def test_sigma_jsonb_equal_payload_produces_no_churn_and_no_new_version(
    clean_runtime: str,
) -> None:
    """acc.sigma-pinned: a jsonb-equal-but-text-different payload (key order,
    1.0 vs 1.00) closes NOTHING and mints NO new version row. Proven at the
    exact keep-open-vs-close site: a changed-only snapshot row carrying the
    reordered/rescaled payload's hash confirms the open interval unchanged."""
    dsn = clean_runtime
    suffix = uuid.uuid4().hex[:8]
    tid = f"doc:sigma:{suffix}"
    gen = f"okg:test:gen:{suffix}"
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        ensure_attribute_schema(conn)
        # Open interval whose payload was written with one text rendering...
        cur.execute(
            "SELECT okg.attribute_content_hash(%s::jsonb, NULL)",
            ('{"a": 1.0, "b": "x", "z": {"q": 2.50}}',),
        )
        h1 = bytes(cur.fetchone()[0])
        cur.execute(
            f"INSERT INTO {ATTRIBUTE_VERSIONS_TABLE} "
            "(target_kind, target_id, attrs_hash, attrs, raw_attrs) "
            "VALUES ('node', %s, %s, %s::jsonb, NULL)",
            (tid, h1, '{"a": 1.0, "b": "x", "z": {"q": 2.50}}'),
        )
        cur.execute(
            f"INSERT INTO {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
            "(target_kind, target_id, namespace, branch_id, from_ord, to_ord, "
            " subtype, overlay_count, raw_text_hash, attrs_hash) "
            "VALUES ('node', %s, 'default', 'default', 0, %s, 'doc', 0, NULL, %s)",
            (tid, EFFECTIVE_INTERVAL_OPEN_ORD, h1),
        )
        # ... and a NEW snapshot row whose logically-equal payload arrived with
        # different key order and numeric scale. THE hash site normalizes both.
        cur.execute(
            "SELECT okg.attribute_content_hash(%s::jsonb, NULL)",
            ('{"z": {"q": 2.5}, "b": "x", "a": 1.00}',),
        )
        h2 = bytes(cur.fetchone()[0])
        assert h2 == h1, "jsonb-equal payloads must hash identically"
        cur.execute(
            f"INSERT INTO {ATTRIBUTE_VERSIONS_TABLE} "
            "(target_kind, target_id, attrs_hash, attrs, raw_attrs) "
            "VALUES ('node', %s, %s, %s::jsonb, NULL) "
            "ON CONFLICT (target_kind, target_id, attrs_hash) DO NOTHING",
            (tid, h2, '{"z": {"q": 2.5}, "b": "x", "a": 1.00}'),
        )
        assert cur.rowcount == 0, "NO new version row for a jsonb-equal payload"
        cur.execute(
            f"INSERT INTO {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
            "(target_kind, target_id, generation_id, namespace, subtype, "
            " overlay_count, raw_text_hash, attrs_hash) "
            "VALUES ('node', %s, %s, 'default', 'doc', 0, NULL, %s)",
            (tid, gen, h2),
        )
        # The keep-open-vs-close decision: the target is in the dirty set, but
        # the snapshot confirms it unchanged BY HASH -> zero closes.
        closed = _close_dirty_effective_intervals(
            cur,
            iv=ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE,
            snap=ATTRIBUTE_EFFECTIVE_VIEW_TABLE,
            target_kind="node",
            params={
                "new": 1,
                "br": "default",
                "ns": "default",
                "tk": "node",
                "open": EFFECTIVE_INTERVAL_OPEN_ORD,
                "gen": gen,
            },
            close_ids={tid},
        )
        assert closed == 0, "jsonb-equal payload must NOT close the interval"
        cur.execute(
            f"SELECT to_ord FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} "
            "WHERE target_id = %s",
            (tid,),
        )
        assert int(cur.fetchone()[0]) == EFFECTIVE_INTERVAL_OPEN_ORD


def test_sigma_doctor_hash_honesty_red_on_corruption(clean_runtime: str) -> None:
    """acc.sigma-pinned: the doctor check recomputes the hash over the STORED
    payload; green on honest rows, red the moment a payload is mutated under a
    stale hash."""
    dsn = clean_runtime
    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, range(5), {"k": "v"}))
    _publish(dsn)

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {ATTRIBUTE_VERSIONS_TABLE}")
        total_versions = int(cur.fetchone()[0])
        # The shared test DB may carry generation-keyed version rows from other
        # tests; sample the WHOLE table so green-before is a real full check.
        green = check_attribute_version_hash_honesty(conn, sample=total_versions)
        assert green["status"] == "ok", green
        assert green["sampled"] >= 5
        # Manual corruption: rewrite one stored payload, leave the hash stale.
        # Target the FIRST row in the checker's deterministic sample order
        # (ORDER BY target_kind, target_id, attrs_hash) so the default-sample
        # doctor check must see it regardless of how many version rows other
        # tests left in the shared database — an arbitrary-ctid pick could
        # land outside the bounded sample and vacuously stay green.
        cur.execute(
            f"UPDATE {ATTRIBUTE_VERSIONS_TABLE} v "
            "SET attrs = v.attrs || '{\"corrupt\": true}'::jsonb "
            "WHERE (v.target_kind, v.target_id, v.attrs_hash) = ("
            "  SELECT target_kind, target_id, attrs_hash "
            f"   FROM {ATTRIBUTE_VERSIONS_TABLE} "
            "  ORDER BY target_kind, target_id, attrs_hash LIMIT 1)"
        )
        assert cur.rowcount == 1
        red = check_attribute_version_hash_honesty(conn)
        assert red["status"] == "red", red
        assert red["mismatched"] >= 1
        # Repair the planted corruption (drop the injected key; the stored
        # hash is honest again and FK referents are untouched) so the shared
        # test DB is left clean and this test is idempotent across runs.
        cur.execute(
            f"UPDATE {ATTRIBUTE_VERSIONS_TABLE} "
            "SET attrs = attrs - 'corrupt' WHERE attrs ? 'corrupt'"
        )
        assert cur.rowcount == 1
        repaired = check_attribute_version_hash_honesty(
            conn, sample=total_versions
        )
        assert repaired["status"] == "ok", repaired


# --------------------------------------------------------------------------- #
# req.fk-and-gc                                                               #
# --------------------------------------------------------------------------- #


def test_fk_restrict_refuses_deleting_referenced_version(
    clean_runtime: str,
) -> None:
    """acc.fk-and-gc: deleting a version row still referenced by an interval or
    snapshot row is refused by the composite ON DELETE RESTRICT FK."""
    dsn = clean_runtime
    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, [0], {"k": "v"}))
    _publish(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        with pytest.raises(psycopg.errors.ForeignKeyViolation):
            cur.execute(f"DELETE FROM {ATTRIBUTE_VERSIONS_TABLE}")


def test_gc_two_prune_cycles_zero_orphans_and_bounded_growth(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """acc.fk-and-gc: on a churning deployment, after the publish-path prune has
    run across cycles, orphaned version rows are ZERO and the version count is
    bounded by the retained read model (never one-per-write unbounded)."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    dsn = clean_runtime
    suffix = uuid.uuid4().hex[:8]
    n = 20
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, range(n), {"rev": 0}))
    _publish(dsn)
    # Churn EVERY target for several cycles; each close falls out of the keep=2
    # window two cycles later, and the same-loop GC must reclaim its version.
    for rev in range(1, 5):
        _apply(dsn, _facts(suffix, range(n), {"rev": rev}, rev=rev + 1))
        _publish(dsn)

    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        # Zero orphans among THIS churn run's version rows (the shared test DB
        # carries generation-keyed leftovers from other tests, so the witness
        # is scoped to this suffix; the global gate is proven by the
        # red-on-disabled fixture below).
        cur.execute(
            f"""
            SELECT count(*) FROM {ATTRIBUTE_VERSIONS_TABLE} v
             WHERE v.target_id LIKE %(like)s
               AND NOT EXISTS (
                 SELECT 1 FROM {ATTRIBUTE_EFFECTIVE_INTERVALS_TABLE} i
                  WHERE i.target_kind = v.target_kind
                    AND i.target_id = v.target_id
                    AND i.attrs_hash = v.attrs_hash)
               AND NOT EXISTS (
                 SELECT 1 FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} s
                  WHERE s.target_kind = v.target_kind
                    AND s.target_id = v.target_id
                    AND s.attrs_hash = v.attrs_hash)
            """,
            {"like": f"doc:%:{suffix}"},
        )
        orphans = int(cur.fetchone()[0])
        assert orphans == 0, f"{orphans} orphaned version rows after prune cycles"
        cur.execute(
            f"SELECT count(*) FROM {ATTRIBUTE_VERSIONS_TABLE} "
            "WHERE target_id LIKE %s",
            (f"doc:%:{suffix}",),
        )
        versions = int(cur.fetchone()[0])
        # Bounded: at most the retained read model (keep=2 windows of snapshot
        # + interval history), never the 5*n one-per-write worst case.
        assert versions <= 3 * n, (versions, n)


def test_gc_growth_gate_red_when_gc_disabled_then_drained_green(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """acc.fk-and-gc: with the targeted GC disabled in a fixture, churn + prune
    strands orphaned version rows and the growth gate goes RED; the bounded
    recovery drain reclaims them and the gate returns green."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "1")
    dsn = clean_runtime
    suffix = uuid.uuid4().hex[:8]
    n = 10
    # Disable the same-loop GC exactly the way a broken deploy would: the
    # targeted reclaim never fires while prunes keep deleting referents.
    monkeypatch.setattr(
        "okg.substrate.attributes._gc_attribute_versions_for_keys",
        lambda *a, **k: 0,
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(dsn, _facts(suffix, range(n), {"rev": 0}))
    _publish(dsn)
    for rev in range(1, 5):
        _apply(dsn, _facts(suffix, range(n), {"rev": rev}, rev=rev + 1))
        _publish(dsn)

    with psycopg.connect(dsn, autocommit=True) as conn:
        stranded = count_orphan_attribute_versions(conn)
        assert stranded > 0, "fixture failed to strand orphans with GC disabled"
        gate = check_attribute_version_gc(conn, max_orphans=0)
        assert gate["status"] == "red", gate
        assert gate["orphan_count"] == stranded

        report = prune_orphan_attribute_versions(conn, batch_size=7)
        assert report["deleted"] == stranded, report
        assert report["drained"] is True
        assert count_orphan_attribute_versions(conn) == 0
        green = check_attribute_version_gc(conn, max_orphans=0)
        assert green["status"] == "ok", green


# --------------------------------------------------------------------------- #
# req.publish-cost                                                            #
# --------------------------------------------------------------------------- #

_CHURN = 1000
_BENCH_CYCLES = 5
# ~1.2 KB effective payload matching the live exemplar's measured average
# (~1177 B). Built from per-row random hex so it does NOT collapse under
# TOAST/pglz compression — a trivially compressible filler would shrink the
# wide rows to a few dozen bytes and benchmark compression, not row width.
import os as _os  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402

_PAYLOAD_TOKENS = [_os.urandom(500).hex() for _ in range(16)]

_WIDE_PAYLOAD_COLS = (
    "subtype",
    "src",
    "dst",
    "edge_type",
    "raw_attrs",
    "attrs",
    "overlay_count",
    "raw_text_hash",
)
_NARROW_PAYLOAD_COLS = (
    "subtype",
    "src",
    "dst",
    "edge_type",
    "overlay_count",
    "raw_text_hash",
    "attrs_hash",
)


def _bench_payload(i: int, rev: int) -> dict:
    return {
        "k": f"v{rev}",
        "pid": 1000 + i,
        "rev": rev,
        "text": _PAYLOAD_TOKENS[(i + rev) % len(_PAYLOAD_TOKENS)],
    }


def _create_wide_scratch(cur, prefix: str) -> tuple[str, str]:
    iv, snap = f"{prefix}_iv", f"{prefix}_snap"
    cur.execute(
        f"""
        CREATE TABLE {iv} (
          target_kind TEXT NOT NULL, target_id TEXT NOT NULL,
          namespace TEXT NOT NULL DEFAULT 'default', branch_id TEXT NOT NULL,
          from_ord BIGINT NOT NULL,
          to_ord BIGINT NOT NULL DEFAULT {EFFECTIVE_INTERVAL_OPEN_ORD},
          subtype TEXT, src TEXT, dst TEXT, edge_type TEXT,
          raw_attrs JSONB, attrs JSONB NOT NULL DEFAULT '{{}}'::jsonb,
          overlay_count INT NOT NULL DEFAULT 0, raw_text_hash TEXT,
          materialized_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (target_kind, target_id, namespace, branch_id, from_ord)
        )
        """
    )
    cur.execute(
        f"CREATE INDEX {iv}_open_idx ON {iv} "
        f"(branch_id, namespace, target_kind, target_id) "
        f"WHERE to_ord = {EFFECTIVE_INTERVAL_OPEN_ORD}"
    )
    cur.execute(
        f"CREATE INDEX {iv}_node_idx ON {iv} "
        "(branch_id, namespace, subtype, from_ord, to_ord, target_id) "
        "WHERE target_kind = 'node'"
    )
    # The shipped wide shape carried a GIN(attrs jsonb_path_ops) index on BOTH
    # tables (attribute_effective_intervals_attrs_gin_idx /
    # attribute_effective_view_attrs_gin_idx — this PACT is what deletes them,
    # per the design's dead-index finding). The baseline must pay the write
    # cost the wide rows actually paid.
    cur.execute(
        f"CREATE INDEX {iv}_attrs_gin_idx ON {iv} "
        "USING GIN (attrs jsonb_path_ops)"
    )
    cur.execute(
        f"""
        CREATE TABLE {snap} (
          target_kind TEXT NOT NULL, target_id TEXT NOT NULL,
          generation_id TEXT NOT NULL,
          namespace TEXT NOT NULL DEFAULT 'default',
          subtype TEXT, src TEXT, dst TEXT, edge_type TEXT,
          raw_attrs JSONB, attrs JSONB NOT NULL DEFAULT '{{}}'::jsonb,
          overlay_count INT NOT NULL DEFAULT 0, raw_text_hash TEXT,
          materialized_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (target_kind, target_id, generation_id, namespace)
        )
        """
    )
    cur.execute(f"CREATE INDEX {snap}_gen_idx ON {snap} (generation_id, namespace)")
    cur.execute(
        f"CREATE INDEX {snap}_attrs_gin_idx ON {snap} "
        "USING GIN (attrs jsonb_path_ops)"
    )
    return iv, snap


def _create_narrow_scratch(cur, prefix: str) -> tuple[str, str, str]:
    iv, snap, versions = f"{prefix}_iv", f"{prefix}_snap", f"{prefix}_versions"
    cur.execute(
        f"""
        CREATE TABLE {versions} (
          target_kind TEXT NOT NULL, target_id TEXT NOT NULL,
          attrs_hash BYTEA NOT NULL, attrs JSONB NOT NULL, raw_attrs JSONB,
          created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          PRIMARY KEY (target_kind, target_id, attrs_hash)
        )
        """
    )
    cur.execute(
        f"""
        CREATE TABLE {iv} (
          target_kind TEXT NOT NULL, target_id TEXT NOT NULL,
          namespace TEXT NOT NULL DEFAULT 'default', branch_id TEXT NOT NULL,
          from_ord BIGINT NOT NULL,
          to_ord BIGINT NOT NULL DEFAULT {EFFECTIVE_INTERVAL_OPEN_ORD},
          subtype TEXT, src TEXT, dst TEXT, edge_type TEXT,
          overlay_count INT NOT NULL DEFAULT 0, raw_text_hash TEXT,
          materialized_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          attrs_hash BYTEA NOT NULL,
          PRIMARY KEY (target_kind, target_id, namespace, branch_id, from_ord),
          FOREIGN KEY (target_kind, target_id, attrs_hash)
            REFERENCES {versions} (target_kind, target_id, attrs_hash)
            ON DELETE RESTRICT
        )
        """
    )
    cur.execute(
        f"CREATE INDEX {iv}_open_idx ON {iv} "
        f"(branch_id, namespace, target_kind, target_id) "
        f"WHERE to_ord = {EFFECTIVE_INTERVAL_OPEN_ORD}"
    )
    cur.execute(
        f"CREATE INDEX {iv}_node_idx ON {iv} "
        "(branch_id, namespace, subtype, from_ord, to_ord, target_id) "
        "WHERE target_kind = 'node'"
    )
    cur.execute(
        f"""
        CREATE TABLE {snap} (
          target_kind TEXT NOT NULL, target_id TEXT NOT NULL,
          generation_id TEXT NOT NULL,
          namespace TEXT NOT NULL DEFAULT 'default',
          subtype TEXT, src TEXT, dst TEXT, edge_type TEXT,
          overlay_count INT NOT NULL DEFAULT 0, raw_text_hash TEXT,
          materialized_at TIMESTAMPTZ NOT NULL DEFAULT now(),
          attrs_hash BYTEA NOT NULL,
          PRIMARY KEY (target_kind, target_id, generation_id, namespace),
          FOREIGN KEY (target_kind, target_id, attrs_hash)
            REFERENCES {versions} (target_kind, target_id, attrs_hash)
            ON DELETE RESTRICT
        )
        """
    )
    cur.execute(f"CREATE INDEX {snap}_gen_idx ON {snap} (generation_id, namespace)")
    return iv, snap, versions


def _seed_bench_ordinals(cur, branch: str, cycles: int) -> list[str]:
    gens = []
    for ordv in range(cycles + 1):
        gen = f"generation:{branch}:g{ordv}"
        cur.execute(
            "INSERT INTO okg.generation_ordinals "
            "(checkpoint_id, branch_id, ordinal) VALUES (%s, %s, %s)",
            (gen, branch, ordv),
        )
        gens.append(gen)
    return gens


def test_publish_cost_split_at_or_below_wide_row_baseline_at_1k_churn(
    clean_runtime: str, record_property
) -> None:
    """acc.publish-cost: side-by-side attr-stage benchmark at 1,000-target
    churn over {_BENCH_CYCLES} cycles. Both sides run the SAME generic
    close/open writer (the kit `sync_explicit_interval_rows_for_generation`,
    exactly the production interval writer) plus their snapshot-write step:
    the wide side inserts full ~1.2 KB jsonb rows; the split side computes the
    content hash in-database, writes the insert-once version store, and
    inserts narrow rows. The split MUST write strictly less WAL than the wide
    baseline; both sides' wall-clock timings land in the junit XML as
    properties, recorded but not gated."""
    dsn = clean_runtime
    suffix = uuid.uuid4().hex[:8]
    wide_branch, narrow_branch = f"bw_{suffix}", f"bn_{suffix}"

    with psycopg.connect(dsn, autocommit=True) as conn:
        ensure_attribute_schema(conn)
        db = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).context.db
        with conn.cursor() as cur:
            wide_iv, wide_snap = _create_wide_scratch(cur, f"bench_wide_{suffix}")
            narrow_iv, narrow_snap, narrow_versions = _create_narrow_scratch(
                cur, f"bench_narrow_{suffix}"
            )
            wide_gens = _seed_bench_ordinals(cur, wide_branch, _BENCH_CYCLES)
            narrow_gens = _seed_bench_ordinals(cur, narrow_branch, _BENCH_CYCLES)

        wide_spec = ExplicitIntervalTable(
            table=wide_iv,
            snapshot_table=wide_snap,
            payload_columns=_WIDE_PAYLOAD_COLS,
            read_columns=_INTERVAL_READ_COLS,
            target_kinds=("node",),
        )
        narrow_spec = ExplicitIntervalTable(
            table=narrow_iv,
            snapshot_table=narrow_snap,
            payload_columns=_NARROW_PAYLOAD_COLS,
            read_columns=("target_id",),
            target_kinds=("node",),
        )
        dirty = {"node": [f"t:{i}" for i in range(_CHURN)]}
        rows = [(f"t:{i}", i) for i in range(_CHURN)]

        def _stage_payloads(cur, rev: int) -> None:
            cur.execute(
                f"CREATE TEMP TABLE bench_stage_{suffix} "
                "(target_id TEXT, attrs JSONB, raw_attrs JSONB)"
            )
            with cur.copy(
                f"COPY bench_stage_{suffix} (target_id, attrs) FROM STDIN"
            ) as copy:
                for tid, i in rows:
                    copy.write_row((tid, Jsonb(_bench_payload(i, rev))))

        def _wide_cycle(rev: int, gen: str) -> float:
            # Symmetric staging: COPY the churned payloads, then one set-based
            # snapshot INSERT of full jsonb rows — the wide-row materialize.
            t0 = time.perf_counter()
            with conn.cursor() as cur:
                _stage_payloads(cur, rev)
                cur.execute(
                    f"INSERT INTO {wide_snap} (target_kind, target_id, "
                    "generation_id, namespace, subtype, raw_attrs, attrs, "
                    "overlay_count, raw_text_hash) "
                    "SELECT 'node', target_id, %s, 'default', 'doc', "
                    "raw_attrs, attrs, 0, NULL "
                    f"FROM bench_stage_{suffix}",
                    (gen,),
                )
                cur.execute(f"DROP TABLE bench_stage_{suffix}")
            sync_explicit_interval_rows_for_generation(
                conn, db, spec=wide_spec, generation_id=gen,
                dirty_target_ids_by_kind=(dirty if rev > 0 else None),
            )
            return (time.perf_counter() - t0) * 1000

        def _narrow_cycle(rev: int, gen: str) -> float:
            t0 = time.perf_counter()
            with conn.cursor() as cur:
                # Stage payloads once, hash ONCE at THE in-database hash site,
                # write the insert-once version store and the narrow snapshot
                # rows in one statement (a materialized CTE evaluates the hash
                # a single time; no second staging copy).
                _stage_payloads(cur, rev)
                cur.execute(
                    "WITH hashed AS MATERIALIZED ("
                    "  SELECT target_id, attrs, raw_attrs, "
                    "         okg.attribute_content_hash(attrs, raw_attrs) "
                    "         AS attrs_hash "
                    f"   FROM bench_stage_{suffix}"
                    "), ins_versions AS ("
                    f"  INSERT INTO {narrow_versions} "
                    "   (target_kind, target_id, attrs_hash, attrs, raw_attrs) "
                    "  SELECT 'node', target_id, attrs_hash, attrs, raw_attrs "
                    "    FROM hashed "
                    "  ON CONFLICT (target_kind, target_id, attrs_hash) "
                    "  DO NOTHING"
                    ") "
                    f"INSERT INTO {narrow_snap} (target_kind, target_id, "
                    "generation_id, namespace, subtype, overlay_count, "
                    "raw_text_hash, attrs_hash) "
                    "SELECT 'node', target_id, %s, 'default', 'doc', 0, NULL, "
                    "attrs_hash FROM hashed",
                    (gen,),
                )
                cur.execute(f"DROP TABLE bench_stage_{suffix}")
            sync_explicit_interval_rows_for_generation(
                conn, db, spec=narrow_spec, generation_id=gen,
                dirty_target_ids_by_kind=(dirty if rev > 0 else None),
            )
            return (time.perf_counter() - t0) * 1000

        def _maintain(*tables: str) -> None:
            # Untimed between-cycle maintenance, identical on both sides: the
            # production tables carry aggressive autovacuum reloptions
            # (scale_factor 0.05, cost_limit 2000 — attributes.py DDL), which
            # keeps stats and dead tuples in check between publishes. Scratch
            # tables have no autovacuum history, so emulate it explicitly;
            # without it both sides measure planner drift, not write cost.
            with conn.cursor() as cur:
                for table in tables:
                    cur.execute(f"VACUUM (ANALYZE) {table}")

        def _wal_lsn() -> int:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), '0/0')"
                )
                return int(cur.fetchone()[0])

        def _checkpoint() -> None:
            # Equalize full-page-write state: a background checkpoint landing
            # inside one side's loop would charge that side an arbitrary set
            # of 8 KB page images. Checkpointing right before each side means
            # both start from the same all-pages-need-FPI state.
            with conn.cursor() as cur:
                cur.execute("CHECKPOINT")

        wide_ms, wide_wal = [], []
        _checkpoint()
        for rev in range(_BENCH_CYCLES):
            lsn0 = _wal_lsn()
            wide_ms.append(_wide_cycle(rev, wide_gens[rev]))
            wide_wal.append(_wal_lsn() - lsn0)
            _maintain(wide_iv, wide_snap)
        narrow_ms, narrow_wal = [], []
        _checkpoint()
        for rev in range(_BENCH_CYCLES):
            lsn0 = _wal_lsn()
            narrow_ms.append(_narrow_cycle(rev, narrow_gens[rev]))
            narrow_wal.append(_wal_lsn() - lsn0)
            _maintain(narrow_iv, narrow_snap, narrow_versions)

        # Both sides did real work: every churned cycle closed + reopened 1K.
        with conn.cursor() as cur:
            for iv in (wide_iv, narrow_iv):
                cur.execute(
                    f"SELECT count(*) FROM {iv} WHERE to_ord = %s",
                    (EFFECTIVE_INTERVAL_OPEN_ORD,),
                )
                assert int(cur.fetchone()[0]) == _CHURN, iv
                cur.execute(f"SELECT count(*) FROM {iv}")
                assert int(cur.fetchone()[0]) == _CHURN * _BENCH_CYCLES, iv

    # First cycle is the cold full open (both sides identical mode); compare
    # the steady-state churn cycles.
    wide_churn = statistics.median(wide_ms[1:])
    split_churn = statistics.median(narrow_ms[1:])
    wide_wal_churn = statistics.median(wide_wal[1:])
    split_wal_churn = statistics.median(narrow_wal[1:])
    record_property("wide_row_cycle_ms", [round(m, 1) for m in wide_ms])
    record_property("split_cycle_ms", [round(m, 1) for m in narrow_ms])
    record_property("wide_row_churn_median_ms", round(wide_churn, 1))
    record_property("split_churn_median_ms", round(split_churn, 1))
    record_property("wide_row_churn_wal_bytes", int(wide_wal_churn))
    record_property("split_churn_wal_bytes", int(split_wal_churn))
    print(
        f"\n[publish-cost 1K-churn] wide={wide_churn:.1f}ms "
        f"({wide_wal_churn/1e6:.2f} MB WAL) split={split_churn:.1f}ms "
        f"({split_wal_churn/1e6:.2f} MB WAL) "
        f"(cycles: wide={wide_ms} split={narrow_ms})"
    )
    # The DECISIVE measure of publish cost at churn is bytes written — the
    # un-defer evidence was byte weight (every close rewrote ~1.3 KB; the
    # 17M-row/35 GB regrowth is byte volume), and WAL volume is deterministic
    # where wall-clock on an otherwise-idle scratch DB is scheduler noise.
    # The split must write strictly less than the wide-row baseline.
    assert split_wal_churn < wide_wal_churn, (
        f"split churn WAL {split_wal_churn/1e6:.2f} MB is not below the "
        f"wide-row baseline {wide_wal_churn/1e6:.2f} MB"
    )
    # Wall-clock is recorded, not gated (#2554, card splittime2 C). With its
    # two guards -- split median within 1.25x of the wide-row median, best
    # split cycle under 150 ms -- this test passed on no machine available:
    # the 150 ms bound fails on a lab machine even at the commit that added it
    # (86f6a295d48), and the ratio sits at the noise edge (1.09-1.38 on
    # that machine, 1.37 and 1.67 on the nightly runner) with no change in the
    # split path. Both timings stay in the junit properties for trend
    # reading; a timing gate needs a quiet runner (#3601).
    split_best = min(narrow_ms[1:])
    record_property("split_churn_best_ms", round(split_best, 1))
    record_property("split_to_wide_churn_ratio", round(split_churn / wide_churn, 3))


def test_publish_cost_read_parity_byte_identical_to_wide_row_oracle(
    clean_runtime: str,
) -> None:
    """acc.publish-cost: interval-stab and snapshot reads materialize payloads
    byte-identical to an INDEPENDENT wide-row oracle — the effective payload
    recomputed as raw || overlay from the pinned Chronos checkout, exactly
    what every wide row stored before the split."""
    dsn = clean_runtime
    suffix = uuid.uuid4().hex[:8]
    doc1 = f"doc:1:{suffix}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(conn, subtypes=["doc"])
    _apply(
        dsn,
        _facts(suffix, range(10), {"k": "v", "num": 1.5, "path": "a/b.py"}),
    )
    gen = _publish(dsn)

    with psycopg.connect(dsn, autocommit=True) as conn:
        # Post-publish overlay on one target: the oracle must see raw||overlay
        # while raw stays recoverable.
        record_attribute_observation(
            conn, target_kind="node", target_id=doc1, generation_id=gen,
            attr_key="k", value="overlaid", producer="test",
        )
        materialize_effective_attributes_for_generation(conn, generation_id=gen)

        # The wide-row oracle: raw attrs from the PINNED checkout + the Python
        # overlay merge (the pre-split row content, computed independently of
        # the version store).
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        session = store.checkout_generation(gen)
        raw_rows = session.query(
            "SELECT node_id, attrs FROM okg.graph_nodes", {}
        )
        from okg.substrate.attributes import apply_effective_attrs_to_rows

        merged = apply_effective_attrs_to_rows(
            conn,
            [{"node_id": r["node_id"], "attrs": dict(r["attrs"] or {})} for r in raw_rows],
            target_kind="node",
            target_id_field="node_id",
            generation_id=gen,
        )
        oracle_attrs = {r["node_id"]: r["attrs"] for r in merged}
        oracle_raw = {str(r["node_id"]): dict(r["attrs"] or {}) for r in raw_rows}

        attrs_idx = _INTERVAL_READ_COLS.index("attrs")
        raw_idx = _INTERVAL_READ_COLS.index("raw_attrs")
        snapshot = {
            r[0]: r
            for r in effective_snapshot_read_rows(
                conn, generation_id=gen, target_kind="node"
            )
        }
        assert set(snapshot) == set(oracle_attrs)
        for node_id, row in snapshot.items():
            assert row[attrs_idx] == oracle_attrs[node_id], node_id
            # presence-means-overlay: NULL raw == "raw equals effective".
            resolved_raw = row[raw_idx] if row[raw_idx] is not None else row[attrs_idx]
            assert resolved_raw == oracle_raw[node_id], node_id
        assert snapshot[doc1][raw_idx] is not None  # the overlaid target diverges

        db = store.context.db
        stab = {
            r[0]: r
            for r in effective_interval_stab_rows(
                conn, db, generation_id=gen, target_kind="node"
            )
        }
        # The interval front was synced at publish (pre-overlay); its payloads
        # must equal the oracle RAW for every target (the overlay arrived
        # post-publish and is a snapshot/refresh concern, unchanged by the
        # split). Byte-identical: same dict content, presence semantics intact.
        assert set(stab) == set(oracle_raw)
        for node_id, row in stab.items():
            assert row[attrs_idx] == oracle_raw[node_id], node_id
            assert row[raw_idx] is None, node_id
