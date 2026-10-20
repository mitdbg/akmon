"""scale-operator-attr-reads / req.effective-attrs-view (Layer 2):
incremental carry-forward, backfill, and doctor-integrity proofs.

  * INCREMENTAL: a re-materialize with NO attr changes carries forward ~all
    snapshot rows (O(changed) merge work, instrumented) and is BYTE-IDENTICAL to
    a full re-materialize.
  * BACKFILL: idempotent -- running it twice on a generation yields the same
    snapshot, and it materializes an already-published generation that had none.
  * DOCTOR: the integrity check reports ok when the snapshot is complete,
    degraded when a row is dropped (the silent-partial signal).
"""

from __future__ import annotations

import json
import uuid

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.attributes import (  # noqa: E402
    _MATERIALIZE_INSTRUMENTATION,
    _reset_materialize_instrumentation,
    ATTRIBUTE_EFFECTIVE_TABLE,
    ATTRIBUTE_EFFECTIVE_VIEW_TABLE,
    ATTRIBUTE_OBSERVATION_TABLE,
    backfill_effective_attribute_view,
    check_effective_attribute_view_integrity,
    delete_effective_attribute_view_for_generation,
    EFFECTIVE_VIEW_RETENTION_GENERATIONS,
    ensure_attribute_schema,
    materialize_effective_attributes_for_generation,
    materialize_effective_attribute_view_for_generation,
    prune_effective_attribute_view_to_recent,
    record_attribute_observation,
)
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from tests.pytest.catalog_fixture_builders import seed_catalog  # noqa: E402

from psycopg.types.json import Jsonb  # noqa: E402
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _seed_view_row(cur, *, target_id: str, generation: str, materialized_at: str) -> None:
    """Insert one effective-attrs view row for a synthetic generation.

    schema-interval-narrow-split moved payloads out of this table: it now holds
    `attrs_hash BYTEA NOT NULL` under a foreign key into okg.attribute_versions
    rather than an inline `attrs` column. A retention test that only cares which
    generations survive still has to satisfy both, so the version row is written
    first and hashed through the one in-database hash site.
    """
    attrs = {"probe": target_id}
    cur.execute("SELECT okg.attribute_content_hash(%s::jsonb, NULL)", (Jsonb(attrs),))
    attrs_hash = bytes(cur.fetchone()[0])
    cur.execute(
        "INSERT INTO okg.attribute_versions "
        "(target_kind, target_id, attrs_hash, attrs) "
        "VALUES ('node', %s, %s, %s::jsonb) "
        "ON CONFLICT (target_kind, target_id, attrs_hash) DO NOTHING",
        (target_id, attrs_hash, Jsonb(attrs)),
    )
    cur.execute(
        f"""
        INSERT INTO {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}
            (target_kind, target_id, generation_id, namespace,
             materialized_at, attrs_hash)
        VALUES ('node', %s, %s, 'default', %s, %s)
        """,
        (target_id, generation, materialized_at, attrs_hash),
    )

_N = 200


def _seed_two_generations(clean_runtime: str) -> dict[str, str]:
    """Publish gen1 with N `doc` nodes, then publish gen2 with NO changes (a
    no-op cycle still produces a generation). Both snapshots are built at publish
    (gen2 incrementally over gen1)."""
    suffix = uuid.uuid4().hex[:8]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {ATTRIBUTE_OBSERVATION_TABLE}")
            cur.execute(f"DELETE FROM {ATTRIBUTE_EFFECTIVE_TABLE}")
        seed_catalog(conn, subtypes=["doc"])
        store = ChronosGraphStore.from_connection(conn)
        facts = [
            NodeFact(
                f"doc:{i}:{suffix}",
                "doc",
                {"k": f"v{i % 7}"},
                {"id": f"doc:{i}:{suffix}"},
                {"v": 1},
            )
            for i in range(_N)
        ]
        store.apply_facts(facts, source="default", strict_endpoints=True)
    g1 = str(Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()["generation_id"])
    return {"gen1": g1, "suffix": suffix}


def _snapshot_rows(conn, gen: str) -> dict[tuple[str, str], tuple]:
    # schema-interval-narrow-split: attrs/raw_attrs materialize through the
    # version-store join; row identity semantics are unchanged.
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
        out = {}
        for r in cur.fetchall():
            out[(r[0], r[1])] = (r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9])
        return out


def test_empty_overlay_fast_path_skips_duplicate_snapshot_rows(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "OKG_ATTRIBUTE_EFFECTIVE_VIEW_EMPTY_OVERLAY_FAST_PATH",
        "1",
    )
    # The fast path is now mutually exclusive with the interval arm
    # (issue #647, fix-ivm-fresh-db-publish): on an interval-tracked
    # branch at the shipped default it is REFUSED so the interval sync
    # always has a real snapshot to fold. This test pins the flag-OFF
    # contract, where the fast path legitimately skips snapshot rows.
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "0")
    generation_id = f"gen-empty-overlay-{uuid.uuid4().hex}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {ATTRIBUTE_OBSERVATION_TABLE}")
            cur.execute(f"DELETE FROM {ATTRIBUTE_EFFECTIVE_TABLE}")
        seed_catalog(conn, subtypes=["doc"])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "doc:empty-overlay",
                    "doc",
                    {"k": "v"},
                    {"id": "doc:empty-overlay"},
                    {"v": 1},
                )
            ],
            source="default",
            strict_endpoints=True,
        )
        res = materialize_effective_attribute_view_for_generation(
            conn,
            generation_id=generation_id,
        )
        assert res == {
            "generation_id": generation_id,
            "previous_generation_id": None,
            "namespace": "default",
            "materialized": True,
            "reason": "empty_overlay_identity",
            "node_rows": 0,
            "edge_rows": 0,
            # No pre-lock hash cache was passed, so nothing was hashed before
            # the lock (PACT lane-h-2941-hash-before-lock added the field).
            "prelock_hash": None,
        }
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
                "WHERE generation_id = %s",
                (generation_id,),
            )
            assert cur.fetchone()[0] == 0


def test_empty_overlay_fast_path_does_not_skip_when_overlay_exists(
    clean_runtime: str,
) -> None:
    generation_id = f"gen-overlay-present-{uuid.uuid4().hex}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {ATTRIBUTE_OBSERVATION_TABLE}")
            cur.execute(f"DELETE FROM {ATTRIBUTE_EFFECTIVE_TABLE}")
        seed_catalog(conn, subtypes=["doc"])
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    "doc:overlay-present",
                    "doc",
                    {"k": "raw"},
                    {"id": "doc:overlay-present"},
                    {"v": 1},
                )
            ],
            source="default",
            strict_endpoints=True,
        )
        record_attribute_observation(
            conn,
            target_kind="node",
            target_id="doc:overlay-present",
            generation_id=generation_id,
            attr_key="k",
            value="overlay",
            producer="test",
        )
        materialize_effective_attributes_for_generation(
            conn,
            generation_id=generation_id,
        )

        res = materialize_effective_attribute_view_for_generation(
            conn,
            generation_id=generation_id,
            empty_overlay_fast_path=True,
        )

        assert res["materialized"] is True
        assert res["node_rows"] == 1
        rows = _snapshot_rows(conn, generation_id)
        assert rows[("node", "doc:overlay-present")][0]["k"] == "overlay"


def test_incremental_carry_forward_is_byte_identical_and_o_changed(
    clean_runtime: str,
) -> None:
    """Re-materialize gen1's snapshot into a NEW pseudo-generation incrementally
    (previous = gen1, no attr changes) and compare to a FULL re-materialize of the
    same pseudo-generation. The two snapshots must be BYTE-IDENTICAL row-for-row,
    and the incremental path must do ~0 merge work (everything carried forward)."""
    fx = _seed_two_generations(clean_runtime)
    g1 = fx["gen1"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # FULL re-materialize: a fresh pseudo-generation, no `previous`, so every
        # row is merged from scratch.
        gen_full = g1 + ":full"
        # The materializer reads raw from the live branch (from_checkpoint=False);
        # gen1 IS the live head here (only one publish), so the raw base is gen1's.
        res_full = materialize_effective_attribute_view_for_generation(
            conn, generation_id=gen_full, previous_generation_id=None
        )
        assert res_full["materialized"] is True, res_full
        full_rows = _snapshot_rows(conn, gen_full)

        # INCREMENTAL re-materialize: previous = gen1 (which has a snapshot), no
        # attr changes -> every unchanged row carried forward, ~0 merged.
        _reset_materialize_instrumentation()
        gen_incr = g1 + ":incr"
        res_incr = materialize_effective_attribute_view_for_generation(
            conn, generation_id=gen_incr, previous_generation_id=g1
        )
        assert res_incr["materialized"] is True, res_incr
        merged = _MATERIALIZE_INSTRUMENTATION["merged_rows"]
        carried = _MATERIALIZE_INSTRUMENTATION["carried_rows"]
        incr_rows = _snapshot_rows(conn, gen_incr)

    # O(changed): with NO changes, ~all rows are carried forward, ~0 merged.
    assert carried >= _N, (carried, merged)
    assert merged == 0, (merged, carried)
    # Byte-identical to a full re-materialize (same id set, same tuple per id).
    assert set(full_rows) == set(incr_rows), (
        set(full_rows) ^ set(incr_rows)
    )
    for key in full_rows:
        assert full_rows[key] == incr_rows[key], (key, full_rows[key], incr_rows[key])


def test_incremental_remerges_only_changed_targets(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ONE node's raw changes between generations, the incremental path
    re-merges exactly that one (changed) target and carries forward the rest --
    and the result still equals a full re-materialize of the changed graph.

    Pinned to OKG_IVM_INTERVAL_VIEW=0: the carry-forward these assertions
    read (the unchanged node present in gen2's snapshot) is the FULL-snapshot
    path's contract. Under the interval arm the snapshot is changed-only by
    design and the interval table carries the unchanged rows (covered by
    tests/substrate/test_effective_attribute_view_intervals.py). This test
    was red at the shipped default since #624 flipped the env default."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "0")
    fx = _seed_two_generations(clean_runtime)
    suffix = fx["suffix"]
    changed_id = f"doc:0:{suffix}"
    # Change one node's raw and publish gen2.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [
                NodeFact(
                    changed_id,
                    "doc",
                    {"k": "CHANGED"},
                    {"id": changed_id},
                    {"v": 2},
                )
            ],
            source="default",
            strict_endpoints=True,
        )
    _reset_materialize_instrumentation()
    g2 = str(Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()["generation_id"])
    merged = _MATERIALIZE_INSTRUMENTATION["merged_rows"]
    # Exactly the one changed node is re-merged; the rest are carried forward.
    assert merged == 1, merged
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = _snapshot_rows(conn, g2)
        # The changed node's effective attrs reflect the new raw.
        assert rows[("node", changed_id)][0]["k"] == "CHANGED", rows[("node", changed_id)]
        # An unchanged node retains its value.
        other = f"doc:1:{suffix}"
        assert rows[("node", other)][0]["k"] == "v1", rows[("node", other)]


def test_backfill_is_idempotent(clean_runtime: str) -> None:
    """MUST-FIX #3: backfill materializes the latest published generation's
    snapshot, and a SECOND backfill is a no-op yielding the SAME snapshot."""
    fx = _seed_two_generations(clean_runtime)
    g1 = fx["gen1"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Simulate an already-published-but-unmaterialized generation by deleting
        # the snapshot the publish built.
        delete_effective_attribute_view_for_generation(conn, g1)
        first = backfill_effective_attribute_view(conn, deployment_name="default")
        assert first["materialized"] is True, first
        assert int(first["node_rows"]) == _N, first
        rows_after_first = _snapshot_rows(conn, g1)

        # Second backfill: idempotent no-op.
        second = backfill_effective_attribute_view(conn, deployment_name="default")
        assert second["materialized"] is False, second
        assert second["reason"] == "already_materialized", second
        rows_after_second = _snapshot_rows(conn, g1)

    assert set(rows_after_first) == set(rows_after_second)
    for key in rows_after_first:
        assert rows_after_first[key] == rows_after_second[key]


def test_doctor_integrity_ok_and_degraded(clean_runtime: str) -> None:
    """SHOULD-FIX doctor check: a COMPLETE snapshot reports ok; dropping one row
    makes the row-count check report degraded (the silent-partial signal)."""
    fx = _seed_two_generations(clean_runtime)
    g1 = fx["gen1"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ok = check_effective_attribute_view_integrity(conn, deployment_name="default")
        assert ok["status"] == "ok", ok
        assert ok["generation_id"] == g1
        row_check = next(
            c for c in ok["checks"] if c["name"] == "row_count_matches_live"
        )
        assert row_check["ok"] is True, row_check

        # Drop one snapshot row -> partial -> degraded (caught loudly, not silent).
        conn.execute(
            f"DELETE FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
            "WHERE generation_id = %s AND target_kind = 'node' "
            "AND ctid = (SELECT ctid FROM "
            f"{ATTRIBUTE_EFFECTIVE_VIEW_TABLE} WHERE generation_id = %s "
            "AND target_kind = 'node' LIMIT 1)",
            (g1, g1),
        )
        degraded = check_effective_attribute_view_integrity(
            conn, deployment_name="default"
        )
        assert degraded["status"] == "degraded", degraded


def test_doctor_integrity_absent_when_no_snapshot(clean_runtime: str) -> None:
    """When the latest published generation has NO snapshot, the doctor reports
    `absent` (fast path off) -- NOT degraded and NOT a silent partial."""
    fx = _seed_two_generations(clean_runtime)
    g1 = fx["gen1"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        delete_effective_attribute_view_for_generation(conn, g1)
        res = check_effective_attribute_view_integrity(conn, deployment_name="default")
    assert res["status"] == "absent", res


def test_publish_prunes_view_to_recent_generations(clean_runtime: str) -> None:
    """okg#400 retention: each materialize prunes the view to the most recently
    materialized candidate plus the last published generation, so failed or
    synthetic candidates cannot evict the last readable published snapshot."""
    assert EFFECTIVE_VIEW_RETENTION_GENERATIONS == 2  # this test assumes the default
    fx = _seed_two_generations(clean_runtime)
    g1 = fx["gen1"]  # already has a snapshot from the publish
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Materialize three more pseudo-generations in sequence. They have no
        # checkpoint, so each is treated like a pre-gate candidate and the
        # published g1 snapshot remains protected alongside the newest candidate.
        for tag in ("a", "b", "c"):
            res = materialize_effective_attribute_view_for_generation(
                conn, generation_id=f"{g1}:{tag}", previous_generation_id=None
            )
            assert res["materialized"] is True, res
            assert "pruned_old_generation_rows" in res, res
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT generation_id FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
                "GROUP BY generation_id ORDER BY MAX(materialized_at)"
            )
            gens = [r[0] for r in cur.fetchall()]
    # Ordered, not a set: the query sorts by MAX(materialized_at), so which
    # survivor is older is itself part of the retention claim -- g1 is the
    # published snapshot kept from before the loop, g1:c the newest candidate.
    assert gens == [g1, f"{g1}:c"], gens

    # Direct prune to a single generation keeps only the most recent.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        deleted = prune_effective_attribute_view_to_recent(conn, keep_generations=1)
        assert deleted > 0
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT DISTINCT generation_id FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}"
            )
            remaining = [r[0] for r in cur.fetchall()]
    assert remaining == [f"{g1}:c"], remaining


def test_candidate_prune_keeps_last_published_snapshot(
    clean_runtime: str,
) -> None:
    """Blocked candidates must never evict the last readable published view."""
    published = "gen:published"
    older_blocked = "gen:blocked:older"
    newer_blocked = "gen:blocked:newer"
    candidate = "gen:candidate"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ensure_attribute_schema(conn)
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}")
            cur.execute(
                "DELETE FROM _chronos_branch_interval_checkpoints "
                "WHERE checkpoint_id = ANY(%s)",
                ([published, older_blocked, newer_blocked],),
            )
            for generation, status, created_at in [
                (published, "published", "2026-07-01T00:00:00Z"),
                (older_blocked, "blocked", "2026-07-02T00:00:00Z"),
                (newer_blocked, "blocked", "2026-07-03T00:00:00Z"),
            ]:
                cur.execute(
                    """
                    INSERT INTO _chronos_branch_interval_checkpoints
                        (checkpoint_id, branch_id, segment_id, created_at,
                         metadata)
                    VALUES (%s, 'default', 0, %s, %s)
                    """,
                    (
                        generation,
                        created_at,
                        json.dumps({"status": status}),
                    ),
                )
            for generation, materialized_at in [
                (published, "2026-07-01T00:00:00Z"),
                (older_blocked, "2026-07-02T00:00:00Z"),
                (newer_blocked, "2026-07-03T00:00:00Z"),
                (candidate, "2026-07-04T00:00:00Z"),
            ]:
                _seed_view_row(
                    cur,
                    target_id=f"node:{generation}",
                    generation=generation,
                    materialized_at=materialized_at,
                )

        deleted = prune_effective_attribute_view_to_recent(
            conn,
            keep_generations=2,
            protected_generation_id=candidate,
        )
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT DISTINCT generation_id FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}"
            )
            kept = {str(row[0]) for row in cur.fetchall()}

    assert deleted == 2
    assert kept == {published, candidate}


def test_candidate_prune_ranks_published_snapshots_by_checkpoint_order(
    clean_runtime: str,
) -> None:
    """Rebuilding an old snapshot must not evict the newer publication."""
    older_published = "gen:published:older"
    newer_published = "gen:published:newer"
    candidate = "gen:candidate"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ensure_attribute_schema(conn)
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}")
            cur.execute(
                "DELETE FROM _chronos_branch_interval_checkpoints "
                "WHERE checkpoint_id = ANY(%s)",
                ([older_published, newer_published],),
            )
            for generation, created_at in [
                (older_published, "2026-07-01T00:00:00Z"),
                (newer_published, "2026-07-02T00:00:00Z"),
            ]:
                cur.execute(
                    """
                    INSERT INTO _chronos_branch_interval_checkpoints
                        (checkpoint_id, branch_id, segment_id, created_at,
                         metadata)
                    VALUES (%s, 'default', 0, %s, %s)
                    """,
                    (generation, created_at, json.dumps({"status": "published"})),
                )
            for generation, materialized_at in [
                # The older publication was rebuilt last. Retention must still
                # prefer the newer checkpoint.
                (older_published, "2026-07-04T00:00:00Z"),
                (newer_published, "2026-07-03T00:00:00Z"),
                (candidate, "2026-07-05T00:00:00Z"),
            ]:
                _seed_view_row(
                    cur,
                    target_id=f"node:{generation}",
                    generation=generation,
                    materialized_at=materialized_at,
                )

        deleted = prune_effective_attribute_view_to_recent(
            conn,
            keep_generations=2,
            protected_generation_id=candidate,
        )
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT DISTINCT generation_id FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}"
            )
            kept = {str(row[0]) for row in cur.fetchall()}

    assert deleted == 1
    assert kept == {newer_published, candidate}


def test_materialize_self_heals_dropped_view(clean_runtime: str) -> None:
    """okg#400 self-heal: the view is a DERIVED cache. If it is dropped (e.g. to
    reclaim disk), the next publish-time materialize recreates the table and
    rebuilds the snapshot rather than silently no-op'ing and leaving reads on the
    slow fallback path until a re-migrate."""
    fx = _seed_two_generations(clean_runtime)
    g1 = fx["gen1"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE {ATTRIBUTE_EFFECTIVE_VIEW_TABLE}")
            cur.execute(f"SELECT to_regclass('{ATTRIBUTE_EFFECTIVE_VIEW_TABLE}')")
            assert cur.fetchone()[0] is None  # the cache is gone
        res = materialize_effective_attribute_view_for_generation(
            conn, generation_id=f"{g1}:reheal", previous_generation_id=None
        )
        assert res["materialized"] is True, res
        assert res["node_rows"] >= _N, res
        with conn.cursor() as cur:
            cur.execute(f"SELECT to_regclass('{ATTRIBUTE_EFFECTIVE_VIEW_TABLE}')")
            assert cur.fetchone()[0] is not None  # recreated
            cur.execute(
                f"SELECT count(*) FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
                "WHERE generation_id = %s",
                (f"{g1}:reheal",),
            )
            assert cur.fetchone()[0] >= _N  # rebuilt


# Lane SG carry-forward stall (PACT lane-sg-carry-forward-stall): the pre-fix
# full-scan carry-forward statement, kept verbatim as the equality oracle. It
# LEFT JOINed two per-target overlay fingerprint aggregates; on a large prior
# snapshot the planner re-ran them once per row and the publish timed out. The
# fix must carry exactly the rows this statement carries.
_PRE_FIX_CARRY_FORWARD_SQL = """
  WITH cur_fp AS (SELECT target_id, md5(string_agg(attr_key || '=' || value_hash, ','
                  ORDER BY attr_key)) AS fp FROM okg.attribute_effective_attrs
                  WHERE target_kind = %(tk)s AND namespace = %(ns)s
                  AND generation_id = %(gen)s AND attr_key <> ALL(%(reserved)s::text[])
                  GROUP BY target_id),
       prev_fp AS (SELECT target_id, md5(string_agg(attr_key || '=' || value_hash, ','
                  ORDER BY attr_key)) AS fp FROM okg.attribute_effective_attrs
                  WHERE target_kind = %(tk)s AND namespace = %(ns)s
                  AND generation_id = %(pgen)s AND attr_key <> ALL(%(reserved)s::text[])
                  GROUP BY target_id)
  INSERT INTO okg.attribute_effective_view
    (target_kind, target_id, generation_id, namespace, subtype, src, dst,
     edge_type, overlay_count, raw_text_hash, attrs_hash)
  SELECT p.target_kind, p.target_id, %(gen)s, p.namespace, p.subtype,
         p.src, p.dst, p.edge_type, p.overlay_count,
         p.raw_text_hash, p.attrs_hash
    FROM okg.attribute_effective_view p
    JOIN okg.attribute_versions av
      ON av.target_kind = p.target_kind
     AND av.target_id = p.target_id
     AND av.attrs_hash = p.attrs_hash
    JOIN {stage} nr
      ON nr.{id_col}::text = p.target_id
     AND nr.attrs = COALESCE(av.raw_attrs, av.attrs)
    LEFT JOIN cur_fp c ON c.target_id = p.target_id
    LEFT JOIN prev_fp pv ON pv.target_id = p.target_id
   WHERE p.generation_id = %(pgen)s
     AND p.namespace = %(ns)s
     AND p.target_kind = %(tk)s
     AND c.fp IS NOT DISTINCT FROM pv.fp
  ON CONFLICT (target_kind, target_id, generation_id, namespace) DO NOTHING
"""

# target -> (prior raw attrs, prior overlay {key: value_hash},
#            current overlay {key: value_hash}, staged raw attrs or None)
_CF_RESERVED = "nomos_source_lineage"
_CF_CASES: dict[str, tuple[dict, dict, dict, dict | None]] = {
    "plain": ({"a": 1}, {}, {}, {"a": 1}),
    "overlay_same": ({"a": 1}, {"k": "h1"}, {"k": "h1"}, {"a": 1}),
    "overlay_two_keys_same": ({"a": 1}, {"k": "h1", "j": "h2"}, {"j": "h2", "k": "h1"}, {"a": 1}),
    "reserved_only_changed": ({"a": 1}, {_CF_RESERVED: "x1"}, {_CF_RESERVED: "x2"}, {"a": 1}),
    "overlay_changed": ({"a": 1}, {"k": "h1"}, {"k": "h9"}, {"a": 1}),
    "overlay_added": ({"a": 1}, {}, {"k": "h1"}, {"a": 1}),
    "overlay_removed": ({"a": 1}, {"k": "h1"}, {}, {"a": 1}),
    "overlay_key_added": ({"a": 1}, {"k": "h1"}, {"k": "h1", "z": "h3"}, {"a": 1}),
    "raw_changed": ({"a": 1}, {}, {}, {"a": 2}),
    "raw_changed_overlay_same": ({"a": 1}, {"k": "h1"}, {"k": "h1"}, {"a": 2}),
    "gone_from_graph": ({"a": 1}, {}, {}, None),
}
_CF_EXPECTED_CARRIED = {
    "plain", "overlay_same", "overlay_two_keys_same", "reserved_only_changed",
}


def _seed_carry_forward_fixture(cur, *, kind: str, pgen: str, gen: str, stage: str) -> None:
    id_col = "node_id" if kind == "node" else "edge_id"
    cur.execute(f"CREATE TEMP TABLE {stage} ({id_col} TEXT, attrs JSONB) ON COMMIT DROP")
    for name, (raw, prev_ov, cur_ov, staged) in _CF_CASES.items():
        tid = f"{kind}:{name}"
        merged = {**raw, **{k: v for k, v in prev_ov.items() if k != _CF_RESERVED}}
        raw_attrs = raw if merged != raw else None
        attrs_hash = f"{tid}:{pgen}".encode()
        cur.execute(
            "INSERT INTO okg.attribute_versions (target_kind, target_id, attrs_hash, attrs, raw_attrs) "
            "VALUES (%s, %s, %s, %s, %s)",
            (kind, tid, attrs_hash, Jsonb(merged), Jsonb(raw_attrs) if raw_attrs else None),
        )
        cur.execute(
            f"INSERT INTO {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} (target_kind, target_id, "
            "generation_id, namespace, subtype, src, dst, edge_type, overlay_count, "
            "raw_text_hash, attrs_hash) VALUES (%s, %s, %s, 'default', %s, %s, %s, %s, %s, %s, %s)",
            (
                kind, tid, pgen,
                "doc" if kind == "node" else None,
                None if kind == "node" else "n:src",
                None if kind == "node" else "n:dst",
                None if kind == "node" else "links",
                len(prev_ov), f"th:{name}" if kind == "node" else None, attrs_hash,
            ),
        )
        for g, ov in ((pgen, prev_ov), (gen, cur_ov)):
            for key, vh in ov.items():
                cur.execute(
                    f"INSERT INTO {ATTRIBUTE_EFFECTIVE_TABLE} (target_kind, target_id, "
                    "generation_id, namespace, attr_key, observation_id, value, value_hash, "
                    "producer) VALUES (%s, %s, %s, 'default', %s, %s, %s, %s, 'test')",
                    (kind, tid, g, key, f"obs:{tid}:{g}:{key}", Jsonb(vh), vh),
                )
        if staged is not None:
            cur.execute(f"INSERT INTO {stage} VALUES (%s, %s)", (tid, Jsonb(staged)))
    # a target staged for the first time (no prior snapshot row) is never carried
    cur.execute(f"INSERT INTO {stage} VALUES (%s, %s)", (f"{kind}:new_target", Jsonb({"a": 1})))


def _carried_rows(cur, *, kind: str, gen: str) -> dict[str, tuple]:
    cur.execute(
        f"SELECT target_id, subtype, src, dst, edge_type, overlay_count, raw_text_hash, "
        f"attrs_hash FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} "
        "WHERE generation_id = %s AND namespace = 'default' AND target_kind = %s",
        (gen, kind),
    )
    return {str(r[0]).split(":", 1)[1]: tuple(r[1:]) for r in cur.fetchall()}


@pytest.mark.parametrize("kind", ["node", "edge"])
def test_full_scan_carry_forward_matches_pre_fix_statement(
    clean_runtime: str, kind: str
) -> None:
    """The full-scan carry-forward carries exactly the rows the pre-fix
    statement carried: unchanged raw AND unchanged overlay (ignoring reserved
    keys), across overlays that changed, appeared, disappeared or gained a key,
    targets with no overlay, a changed raw, a vanished and a new target."""
    from okg.substrate.attributes import _carry_forward_unchanged_rows

    suffix = uuid.uuid4().hex[:8]
    pgen, gen = f"gen-cf-prev-{suffix}", f"gen-cf-cur-{suffix}"
    stage = f"_cf_stage_{kind}_{suffix}"
    id_col = "node_id" if kind == "node" else "edge_id"
    params = {"gen": gen, "pgen": pgen, "ns": "default", "tk": kind,
              "reserved": [_CF_RESERVED]}
    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            _seed_carry_forward_fixture(cur, kind=kind, pgen=pgen, gen=gen, stage=stage)
            cur.execute(_PRE_FIX_CARRY_FORWARD_SQL.format(stage=stage, id_col=id_col), params)
            old_count = cur.rowcount
            old_rows = _carried_rows(cur, kind=kind, gen=gen)
            cur.execute(
                f"DELETE FROM {ATTRIBUTE_EFFECTIVE_VIEW_TABLE} WHERE generation_id = %s",
                (gen,),
            )
        new_count = _carry_forward_unchanged_rows(
            conn,
            generation=gen,
            previous_generation=pgen,
            namespace="default",
            target_kind=kind,
            reserved=[_CF_RESERVED],
            new_raw_temp=stage,
            id_col=id_col,
        )
        with conn.cursor() as cur:
            new_rows = _carried_rows(cur, kind=kind, gen=gen)
        conn.rollback()

    assert set(old_rows) == _CF_EXPECTED_CARRIED, old_rows
    assert new_rows == old_rows
    assert new_count == old_count == len(_CF_EXPECTED_CARRIED)
