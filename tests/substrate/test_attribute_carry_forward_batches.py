"""Issue 3864 (PACT attribute-carry-forward-bounded-3864): the full-scan
carry-forward runs one statement per batch of staged pages instead of one
statement over the whole graph. The batches must carry exactly the rows the
single statement carried, and must really split the work.

The fixtures and the pre-fix oracle statement are shared with
test_effective_attribute_view_incremental.py and imported from it.
"""

from __future__ import annotations

import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.attributes import (  # noqa: E402
    _MATERIALIZE_INSTRUMENTATION,
    _reset_materialize_instrumentation,
    ATTRIBUTE_EFFECTIVE_TABLE,
    ATTRIBUTE_EFFECTIVE_VIEW_TABLE,
    materialize_effective_attribute_view_for_generation,
)
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from psycopg.types.json import Jsonb  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402
from tests.substrate.test_effective_attribute_view_incremental import (  # noqa: E402
    _CF_EXPECTED_CARRIED,
    _CF_RESERVED,
    _N,
    _PRE_FIX_CARRY_FORWARD_SQL,
    _carried_rows,
    _seed_carry_forward_fixture,
    _seed_two_generations,
    _snapshot_rows,
)

pytestmark = pytest.mark.postgres


_BULK_PAD = "x" * 400


def _seed_bulk_carry_targets(cur, *, kind: str, pgen: str, gen: str, stage: str, n: int) -> set[str]:
    """Add ``n`` targets with wide raw rows, so the stage spans many pages.

    Every 7th target's raw changed, every 11th left the graph and every 13th
    gained an overlay; the rest are expected to be carried. Returns the
    names (target id after the kind prefix) expected to be carried.
    """
    versions, snapshot, overlays, staged = [], [], [], []
    expected: set[str] = set()
    for i in range(n):
        name = f"bulk_{i:05d}"
        tid = f"{kind}:{name}"
        raw = {"a": i, "pad": _BULK_PAD}
        attrs_hash = f"{tid}:{pgen}".encode()
        versions.append((kind, tid, attrs_hash, Jsonb(raw)))
        snapshot.append((
            kind, tid, pgen,
            "doc" if kind == "node" else None,
            None if kind == "node" else "n:src",
            None if kind == "node" else "n:dst",
            None if kind == "node" else "links",
            attrs_hash,
        ))
        if i % 13 == 0:
            overlays.append((kind, tid, gen, "k", f"obs:{tid}:{gen}:k", Jsonb("h1"), "h1"))
        if i % 11 == 0:
            continue
        staged.append((tid, Jsonb({"a": -i, "pad": _BULK_PAD} if i % 7 == 0 else raw)))
        if i % 7 != 0 and i % 13 != 0:
            expected.add(name)
    cur.executemany(
        "INSERT INTO okg.attribute_versions (target_kind, target_id, attrs_hash, attrs) "
        "VALUES (%s, %s, %s, %s)",
        versions,
    )
    cur.executemany(
        f"INSERT INTO {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} (target_kind, target_id, "
        "generation_id, namespace, subtype, src, dst, edge_type, overlay_count, "
        "raw_text_hash, attrs_hash) VALUES (%s, %s, %s, 'default', %s, %s, %s, %s, 0, NULL, %s)",
        snapshot,
    )
    cur.executemany(
        f"INSERT INTO {ATTRIBUTE_EFFECTIVE_TABLE} (target_kind, target_id, "
        "generation_id, namespace, attr_key, observation_id, value, value_hash, "
        "producer) VALUES (%s, %s, %s, 'default', %s, %s, %s, %s, 'test')",
        overlays,
    )
    cur.executemany(f"INSERT INTO {stage} VALUES (%s, %s)", staged)
    return expected


@pytest.mark.parametrize("kind", ["node", "edge"])
def test_full_scan_carry_forward_batches_match_one_statement(
    clean_runtime: str, kind: str
) -> None:
    """A carry split into many page batches carries exactly the rows the
    pre-fix single statement carried, and the number of statements is the
    staged page count divided by the pages per batch (not one)."""
    import math

    from okg.substrate.attributes import _carry_forward_unchanged_rows

    suffix = uuid.uuid4().hex[:8]
    pgen, gen = f"gen-cfb-prev-{suffix}", f"gen-cfb-cur-{suffix}"
    stage = f"_cfb_stage_{kind}_{suffix}"
    id_col = "node_id" if kind == "node" else "edge_id"
    params = {"gen": gen, "pgen": pgen, "ns": "default", "tk": kind,
              "reserved": [_CF_RESERVED]}
    batch_rows = 20
    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            _seed_carry_forward_fixture(cur, kind=kind, pgen=pgen, gen=gen, stage=stage)
            expected_bulk = _seed_bulk_carry_targets(
                cur, kind=kind, pgen=pgen, gen=gen, stage=stage, n=300
            )
            cur.execute(
                f"SELECT pg_relation_size('{stage}'::regclass) "
                "/ current_setting('block_size')::bigint, "
                f"(SELECT count(*) FROM {stage})"
            )
            pages, staged = (int(v) for v in cur.fetchone())
            cur.execute(_PRE_FIX_CARRY_FORWARD_SQL.format(stage=stage, id_col=id_col), params)
            old_count = cur.rowcount
            old_rows = _carried_rows(cur, kind=kind, gen=gen)
            cur.execute(
                f"DELETE FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} WHERE generation_id = %s",
                (gen,),
            )
        _reset_materialize_instrumentation()
        new_count = _carry_forward_unchanged_rows(
            conn,
            generation=gen,
            previous_generation=pgen,
            namespace="default",
            target_kind=kind,
            reserved=[_CF_RESERVED],
            new_raw_temp=stage,
            id_col=id_col,
            batch_rows=batch_rows,
        )
        batches = _MATERIALIZE_INSTRUMENTATION["carry_batches"]
        with conn.cursor() as cur:
            new_rows = _carried_rows(cur, kind=kind, gen=gen)
            # A batch statement must never become a server-side prepared
            # statement: after five runs PostgreSQL may give it a generic
            # plan, where the overlay id test is no longer hashed.
            cur.execute(
                "SELECT count(*) FROM pg_prepared_statements "
                "WHERE statement LIKE '%%CROSS JOIN LATERAL%%'"
            )
            prepared = int(cur.fetchone()[0])
        conn.rollback()

    assert set(old_rows) == _CF_EXPECTED_CARRIED | expected_bulk, (
        set(old_rows) ^ (_CF_EXPECTED_CARRIED | expected_bulk)
    )
    assert new_rows == old_rows
    assert new_count == old_count == len(old_rows)
    pages_per_batch = max(1, pages * batch_rows // staged)
    assert pages > 5, pages  # the stage really spans many pages
    assert batches == math.ceil(pages / pages_per_batch) > 5, (batches, pages, staged)
    assert prepared == 0, prepared


def test_full_scan_carry_forward_refuses_nonpositive_batch(clean_runtime: str) -> None:
    """A batch size below one is refused before any row is written, not
    silently treated as one statement."""
    from okg.substrate.attributes import _carry_forward_unchanged_rows

    with psycopg.connect(clean_runtime) as conn:
        with pytest.raises(ValueError, match="batch_rows must be positive"):
            _carry_forward_unchanged_rows(
                conn,
                generation="gen-cfb-none",
                previous_generation="gen-cfb-none-prev",
                namespace="default",
                target_kind="node",
                reserved=[_CF_RESERVED],
                new_raw_temp="_cfb_missing_stage",
                id_col="node_id",
                batch_rows=0,
            )
        conn.rollback()


def test_batched_full_scan_materialize_is_byte_identical(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End to end through materialize_effective_attribute_view_for_generation:
    with a one-row batch size the full-scan path (no dirty scope) runs several
    carry statements, carries every unchanged row, and the snapshot equals a
    full re-materialize row for row."""
    import okg.substrate.attributes as attributes_mod

    fx = _seed_two_generations(clean_runtime)
    g1 = fx["gen1"]
    monkeypatch.setattr(attributes_mod, "CARRY_FORWARD_BATCH_ROWS", 1)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        gen_full = g1 + ":full-b"
        res_full = materialize_effective_attribute_view_for_generation(
            conn, generation_id=gen_full, previous_generation_id=None
        )
        assert res_full["materialized"] is True, res_full
        full_rows = _snapshot_rows(conn, gen_full)

        _reset_materialize_instrumentation()
        gen_incr = g1 + ":incr-b"
        res_incr = materialize_effective_attribute_view_for_generation(
            conn, generation_id=gen_incr, previous_generation_id=g1
        )
        assert res_incr["materialized"] is True, res_incr
        carried = _MATERIALIZE_INSTRUMENTATION["carried_rows"]
        merged = _MATERIALIZE_INSTRUMENTATION["merged_rows"]
        batches = _MATERIALIZE_INSTRUMENTATION["carry_batches"]
        incr_rows = _snapshot_rows(conn, gen_incr)

    assert batches >= 2, batches
    assert carried >= _N and merged == 0, (carried, merged)
    assert res_incr["node_rows"] == res_full["node_rows"], (res_incr, res_full)
    assert full_rows == incr_rows, set(full_rows) ^ set(incr_rows)


def test_publish_through_batched_full_scan_carry(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A real publish cycle on the full-scan path (dirty scope off, as on a
    count self-heal or recovery cycle) with a one-row carry batch publishes a
    generation whose snapshot holds every node once, with the changed node
    re-merged and the rest carried over several statements."""
    import okg.substrate.attributes as attributes_mod

    fx = _seed_two_generations(clean_runtime)
    suffix = fx["suffix"]
    changed_id = f"doc:0:{suffix}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(changed_id, "doc", {"k": "CHANGED"}, {"id": changed_id}, {"v": 2})],
            source="default",
            strict_endpoints=True,
        )
    monkeypatch.setenv("OKG_IVM_DIRTY_MATERIALIZE", "0")
    monkeypatch.setattr(attributes_mod, "CARRY_FORWARD_BATCH_ROWS", 1)
    _reset_materialize_instrumentation()
    result = Publisher(
        clean_runtime,
        "default",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()
    g2 = str(result["generation_id"])
    batches = _MATERIALIZE_INSTRUMENTATION["carry_batches"]
    carried = _MATERIALIZE_INSTRUMENTATION["carried_rows"]
    assert g2 != fx["gen1"], result
    assert batches >= 2, batches
    assert carried >= _N - 1, carried
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = _snapshot_rows(conn, g2)
    node_rows = {k: v for k, v in rows.items() if k[0] == "node"}
    assert len(node_rows) >= _N, len(node_rows)
    assert node_rows[("node", changed_id)][0]["k"] == "CHANGED"
    assert node_rows[("node", f"doc:1:{suffix}")][0]["k"] == "v1"
