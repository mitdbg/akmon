"""Issue 2941 fix 2: attribute content hashes computed before the publish lock.

A full materialize hashes every row. The publisher can hash the branch head
into a session TEMP table before it takes the publish lock; under the lock the
materialize reuses a cached hash only for a row whose raw attrs text has the
digest the cache recorded and which has no overlay row. These tests pin:

* parity: with a cache built BEFORE the branch changed (a changed row, a
  scale-only rewrite, a new row, a new overlay), the materialize writes exactly
  the hashes, version rows and snapshot rows it writes without a cache, and
  reuses a hash only where the inputs match;
* the cache is refused, with a stated reason, when its table is gone or the
  hash functions changed, and the rows are still identical;
* a role without CREATE on schema okg builds the cache serially;
* a real first publish uses the cache, leaves no cache table behind, and
  equals a cacheless rebuild; a session holding an advisory lock skips it;
* fix 5: an incremental cycle, and a full cycle over a full previous
  snapshot, hash the rows written since the last checkpoint before the lock
  when there are enough of them; a row changed after that pass is re-hashed
  under the lock; the published rows equal a cacheless rebuild; the plan and
  its stable reason code are logged as one structured record.
"""

from __future__ import annotations

import json
import uuid

import pytest

psycopg = pytest.importorskip("psycopg")
import okg.substrate.attributes as attributes_module  # noqa: E402
import okg.substrate.publisher.publisher as publisher_module  # noqa: E402

from okg.substrate.errors import SubstrateError  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import NodeFact  # noqa: E402
from tests.substrate.test_attribute_parallel_hash import (  # noqa: E402
    _grant_table_rights,
    _payload,
    _seed,
    no_create_role,  # noqa: F401 - pytest fixture
)

pytestmark = pytest.mark.postgres

_INSTR = attributes_module._MATERIALIZE_INSTRUMENTATION


def _prelock_tables(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT relname FROM pg_class WHERE relkind = 'r' "
            "AND (relname LIKE '\\_eff\\_prelock\\_hash\\_%' "
            "     OR relname LIKE '\\_eff\\_stage\\_prelock\\_%')"
        )
        return sorted(r[0] for r in cur.fetchall())


def _physical_node_table(conn) -> str:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT physical_table FROM _chronos_branch_tables "
            "WHERE table_name = 'okg.graph_nodes' AND backend = 'interval'"
        )
        return str(cur.fetchone()[0])


def _change_branch_after_cache(dsn: str, nodes: list[str], generation: str) -> str:
    """Writes that land between the pre-lock pass and the lock."""
    new_node = f"doc:new:{uuid.uuid4().hex[:8]}"
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(nodes[0], "doc", {**_payload(0), "rev": 2}, {"id": nodes[0]}, {"v": 2}),
                NodeFact(new_node, "doc", _payload(77), {"id": new_node}, {"v": 1}),
            ],
            source="default",
            strict_endpoints=True,
        )
        # A same-value rewrite: 1.0 -> 1.000 is jsonb-equal but not the same
        # text, so the check (exact text) must miss and re-hash it.
        physical = _physical_node_table(conn)
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {physical} SET attrs = jsonb_set(attrs, '{{n_scale_a}}', "
                "'1.000'::jsonb) WHERE node_id = %s AND deleted = FALSE",
                (nodes[1],),
            )
            assert cur.rowcount >= 1
        attributes_module.record_attribute_observation(
            conn,
            target_kind="node",
            target_id=nodes[2],
            generation_id=generation,
            attr_key="enriched",
            value={"why": "late overlay", "score": 0.5},
            producer="test",
        )
        attributes_module.materialize_effective_attributes_for_generation(
            conn, generation_id=generation
        )
    return new_node


def _capture_text(conn, generation: str) -> dict:
    """Like _capture, but attrs and raw_attrs as jsonb TEXT: byte parity, so a
    scale-only difference (1.0 vs 1.000) cannot hide behind float equality."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT v.target_kind, v.target_id, v.attrs_hash, v.overlay_count, "
            "v.raw_text_hash, v.subtype, v.src, v.dst, v.edge_type, "
            "av.attrs::text, av.raw_attrs::text "
            f"FROM {attributes_module.ATTRIBUTE_EFFECTIVE_VIEW_TABLE} v "
            "JOIN okg.attribute_versions av "
            "  ON av.target_kind = v.target_kind AND av.target_id = v.target_id "
            " AND av.attrs_hash = v.attrs_hash "
            "WHERE v.generation_id = %s AND v.namespace = 'default'",
            (generation,),
        )
        return {(r[0], r[1]): tuple(r[2:]) for r in cur.fetchall()}


def _materialize_rows(dsn: str, generation: str, cache=None, conn=None) -> tuple[dict, dict]:
    attributes_module._reset_materialize_instrumentation()
    own = conn is None
    conn = conn or psycopg.connect(dsn)
    try:
        result = attributes_module.materialize_effective_attribute_view_for_generation(
            conn,
            generation_id=generation,
            previous_generation_id=None,
            empty_overlay_fast_path=False,
            prelock_hash_cache=cache,
        )
        rows = _capture_text(conn, generation)
        result["instr"] = dict(_INSTR)
        conn.rollback()
    finally:
        if own:
            conn.close()
    assert result["materialized"] is True, result
    return rows, result


@pytest.mark.parametrize("parallel", [True, False], ids=["parallel", "serial"])
def test_prelock_cache_writes_identical_rows_after_branch_changes(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch, parallel: bool
) -> None:
    seeded = _seed(clean_runtime)
    nodes = seeded["nodes"]
    generation = f"gen-prelock-{seeded['suffix']}"
    if parallel:
        monkeypatch.setattr(attributes_module, "_PARALLEL_HASH_MIN_ROWS", 0)
    else:
        monkeypatch.setattr(
            attributes_module,
            "_parallel_hash_workers",
            lambda conn: (0, "forced serial for the test"),
        )

    with psycopg.connect(clean_runtime, autocommit=True) as cache_conn:
        cache = attributes_module.prepare_prelock_attribute_hash_cache(cache_conn)
        assert cache.report["node"] == len(nodes), cache.report
        assert cache.report["edge"] == len(nodes), cache.report
        assert cache.report["parallel"].startswith(
            "up to" if parallel else "unavailable"
        ), cache.report
        # Only the TEMP caches survive the pass; the unlogged stage is gone.
        assert _prelock_tables(cache_conn) == [
            "_eff_prelock_hash_edge",
            "_eff_prelock_hash_node",
        ]
        new_node = _change_branch_after_cache(clean_runtime, nodes, generation)
        cache_conn.autocommit = False
        cached_rows, cached = _materialize_rows(
            clean_runtime, generation, cache, conn=cache_conn
        )
        cache_conn.autocommit = True
        attributes_module.drop_prelock_attribute_hash_cache(cache_conn, cache)
        assert _prelock_tables(cache_conn) == []
    plain_rows, plain = _materialize_rows(clean_runtime, generation)

    # Row parity first: it must catch a wrong reuse on its own.
    assert set(cached_rows) == set(plain_rows), set(cached_rows) ^ set(plain_rows)
    for key, row in plain_rows.items():
        assert cached_rows[key] == row, (key, cached_rows[key], row)

    assert plain["prelock_hash"] is None
    # Changed row (key added), scale-only rewrite, new row and overlaid row
    # are hashed under the lock; every other row reuses the cache.
    n = len(nodes) + 1
    assert cached["prelock_hash"] == {
        "node": f"used: {n - 4} of {n} merged rows reused a pre-lock hash",
        "edge": f"used: {len(nodes)} of {len(nodes)} merged rows reused a pre-lock hash",
    }, cached["prelock_hash"]
    assert cached["instr"]["prelock_hash_reused"] == n - 4 + len(nodes)
    assert json.loads(cached_rows[("node", nodes[0])][-2])["rev"] == 2
    assert '"n_scale_a": 1.000' in cached_rows[("node", nodes[1])][-2]
    assert ("node", new_node) in cached_rows
    assert cached_rows[("node", nodes[2])][-1] is not None  # overlay: raw_attrs kept


@pytest.mark.parametrize("breakage", ["table_dropped", "function_changed"])
def test_unusable_cache_is_refused_and_says_why(
    clean_runtime: str, caplog: pytest.LogCaptureFixture, breakage: str
) -> None:
    seeded = _seed(clean_runtime)
    generation = f"gen-prelock-bad-{seeded['suffix']}"
    plain_rows, _ = _materialize_rows(clean_runtime, generation)
    caplog.set_level("WARNING", logger=attributes_module.__name__)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cache = attributes_module.prepare_prelock_attribute_hash_cache(conn)
        if breakage == "table_dropped":
            conn.execute("DISCARD TEMP")
            reason = "node cache table pg_temp._eff_prelock_hash_node no longer exists"
        else:
            cache.function_fingerprint = "not-the-installed-function"
            reason = "the attribute content hash functions changed after the pre-lock pass"
        conn.autocommit = False
        rows, result = _materialize_rows(clean_runtime, generation, cache, conn=conn)
        conn.autocommit = True
        attributes_module.drop_prelock_attribute_hash_cache(conn, cache)
    assert result["prelock_hash"] == {
        "node": f"unavailable: {reason}",
        "edge": f"unavailable: {reason}",
    }, result["prelock_hash"]
    assert result["instr"]["prelock_hash_reused"] == 0
    assert any("pre-lock hash cache not used" in r.getMessage() for r in caplog.records)
    assert rows == plain_rows


def test_role_without_create_on_okg_builds_cache_serially(
    clean_runtime: str, no_create_role: str  # noqa: F811
) -> None:
    seeded = _seed(clean_runtime)
    _grant_table_rights(clean_runtime, no_create_role)
    generation = f"gen-prelock-role-{seeded['suffix']}"
    plain_rows, _ = _materialize_rows(clean_runtime, generation)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(f"SET ROLE {no_create_role}")
        cache = attributes_module.prepare_prelock_attribute_hash_cache(conn)
        assert cache.report["parallel"] == (
            "unavailable: no CREATE on schema okg for the publishing role"
        )
        conn.autocommit = False
        attributes_module._reset_materialize_instrumentation()
        result = attributes_module.materialize_effective_attribute_view_for_generation(
            conn,
            generation_id=generation,
            previous_generation_id=None,
            empty_overlay_fast_path=False,
            prelock_hash_cache=cache,
        )
        conn.execute("RESET ROLE")
        rows = _capture_text(conn, generation)
        conn.rollback()
        conn.autocommit = True
        attributes_module.drop_prelock_attribute_hash_cache(conn, cache)
    n = len(seeded["nodes"])
    assert result["prelock_hash"] == {
        "node": f"used: {n} of {n} merged rows reused a pre-lock hash",
        "edge": f"used: {n} of {n} merged rows reused a pre-lock hash",
    }, result["prelock_hash"]
    assert rows == plain_rows


def _publisher(dsn: str):
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer

    return Publisher(
        dsn,
        "default",
        catalog_identity=bind_test_catalog_writer(dsn, "default"),
    )


def test_first_publish_hashes_before_the_lock_and_matches_a_rebuild(
    clean_runtime: str,
) -> None:
    seeded = _seed(clean_runtime)
    n = len(seeded["nodes"])
    result = _publisher(clean_runtime).cycle()
    assert result["status"] == "published", result
    refresh = result["attribute_effective_view_refresh"]
    assert refresh["prelock_hash_pass"]["planned"] is True, refresh
    assert refresh["prelock_hash_pass"]["reason"] == "first publish of the branch"
    assert refresh["prelock_hash_pass"]["rows"] == {"node": n, "edge": n}, refresh
    assert refresh["prelock_hash"] == {
        "node": f"used: {n} of {n} merged rows reused a pre-lock hash",
        "edge": f"used: {n} of {n} merged rows reused a pre-lock hash",
    }, refresh
    assert "attribute_hash_prelock" in result["timing_details"]["top_level_ms"], (
        result["timing_details"]["top_level_ms"]
    )
    generation = str(result["generation_id"])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # The publisher dropped its cache before returning the connection.
        assert _prelock_tables(conn) == []
        published = _capture_text(conn, generation)
    rebuilt, _ = _materialize_rows(clean_runtime, f"{generation}:rebuild")
    assert len(published) == 2 * n
    assert published == rebuilt


def test_prelock_pass_is_skipped_under_a_held_advisory_lock_or_when_disabled(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    _seed(clean_runtime)
    publisher = _publisher(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert publisher._prelock_attribute_hash_plan(conn)[:4] == (
            True, "first publish of the branch", "first_publish", "full",
        )
        conn.execute("SELECT pg_advisory_lock(42, 42)")
        plan = publisher._prelock_attribute_hash_plan(conn)
        conn.execute("SELECT pg_advisory_unlock(42, 42)")
        assert plan.run is False and "advisory lock" in plan.reason, plan
        assert plan.code == "advisory_lock_held", plan
    with psycopg.connect(clean_runtime) as conn:
        # A non-autocommit caller connection is left exactly as it was.
        assert publisher._prelock_attribute_hash_plan(conn)[:3] == (
            False, "connection is not in autocommit mode", "not_autocommit",
        )
        assert conn.info.transaction_status == psycopg.pq.TransactionStatus.IDLE
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        monkeypatch.setenv("OKG_ATTR_HASH_PRELOCK", "0")
        assert publisher._prelock_attribute_hash_plan(conn)[:3] == (
            False, "disabled by OKG_ATTR_HASH_PRELOCK=0", "disabled",
        )


def test_incremental_cycle_skips_the_pass_and_a_failed_pass_is_announced(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    seeded = _seed(clean_runtime)
    first = _publisher(clean_runtime).cycle()
    assert first["status"] == "published", first
    changed = seeded["nodes"][0]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(changed, "doc", {**_payload(5), "rev": 3}, {"id": changed}, {"v": 3})],
            source="default",
            strict_endpoints=True,
        )
    caplog.set_level("INFO", logger=publisher_module.__name__)
    second = _publisher(clean_runtime).cycle()
    assert second["status"] == "published", second
    prelock = second["attribute_effective_view_refresh"]["prelock_hash_pass"]
    counted = prelock["changed_rows_counted"]
    # One changed node writes a few row versions (the new version, and the
    # chronos bookkeeping of the old one); far below the threshold.
    assert 1 <= counted < publisher_module._PRELOCK_CHANGED_MIN_ROWS, prelock
    assert prelock == {
        "planned": False,
        "reason": (
            f"an incremental cycle is expected, and only {counted} row "
            "version(s) were written since the last checkpoint (below "
            f"{publisher_module._PRELOCK_CHANGED_MIN_ROWS}); hashing them "
            "under the lock costs less than a pass"
        ),
        "reason_code": "changed_rows_below_threshold",
        "changed_rows_counted": counted,
        "elapsed_ms": prelock["elapsed_ms"],
    }
    # The skip reason is one structured record, not only prose.
    events = [
        r.okg_event for r in caplog.records
        if getattr(r, "okg_event", {}).get("event")
        == "publisher.prelock_attribute_hash_plan"
    ]
    assert events[-1]["reason_code"] == "changed_rows_below_threshold", events
    assert events[-1]["planned"] is False and events[-1]["deployment"] == "default"

    # A full cadence cycle whose pass fails still publishes, hashing under
    # the lock, and says why.
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "1")

    def _boom(*_a, **_k):
        raise SubstrateError("injected pre-lock failure")

    monkeypatch.setattr(attributes_module, "prepare_prelock_attribute_hash_cache", _boom)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(changed, "doc", {**_payload(6), "rev": 4}, {"id": changed}, {"v": 4})],
            source="default",
            strict_endpoints=True,
        )
    third = _publisher(clean_runtime).cycle()
    assert third["status"] == "published", third
    prelock = third["attribute_effective_view_refresh"]["prelock_hash_pass"]
    assert prelock["planned"] is True, prelock
    assert prelock["failed"] == "SubstrateError: injected pre-lock failure", prelock
    assert third["attribute_effective_view_refresh"].get("prelock_hash") is None


def test_unexpected_pass_error_propagates(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only database, substrate and Chronos branch errors are announced and
    absorbed; anything else is a bug and must surface."""
    _seed(clean_runtime)

    def _bug(*_a, **_k):
        raise RuntimeError("a bug in the pass")

    monkeypatch.setattr(attributes_module, "prepare_prelock_attribute_hash_cache", _bug)
    publisher = _publisher(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(RuntimeError, match="a bug in the pass"):
            publisher._prelock_attribute_hash(conn, {}, {})


def _write_node(dsn: str, node: str, rev: int) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact(node, "doc", {**_payload(rev), "rev": rev}, {"id": node}, {"v": rev})],
            source="default",
            strict_endpoints=True,
        )


def test_cadence_cycle_reuses_the_pass_and_matches_a_rebuild(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The count-stamp cadence cycle (a full cycle on an existing graph) runs
    the pass, reuses it for every merged row, reports used/merged counts, and
    publishes the same rows as a cacheless rebuild. After it, the next full
    cycle skips the pass: the cadence cycle's snapshot is full, so
    carry-forward copies every unchanged row, and only one row changed since
    (fix 5 decides on the changed rows, not on the full snapshot alone)."""
    seeded = _seed(clean_runtime)
    nodes = seeded["nodes"]
    n = len(nodes)
    assert _publisher(clean_runtime).cycle()["status"] == "published"
    _write_node(clean_runtime, nodes[0], 2)
    second = _publisher(clean_runtime).cycle()
    assert second["attribute_effective_view_refresh"]["prelock_hash_pass"]["planned"] is False

    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "1")
    _write_node(clean_runtime, nodes[1], 3)
    attributes_module._reset_materialize_instrumentation()
    third = _publisher(clean_runtime).cycle()
    assert third["status"] == "published", third
    refresh = third["attribute_effective_view_refresh"]
    assert refresh["prelock_hash_pass"]["planned"] is True, refresh
    assert refresh["prelock_hash_pass"]["reason"] == "a full count-stamp cycle is expected"
    merged = _INSTR["merged_rows"]
    carried = _INSTR["carried_rows"]
    assert merged + carried == 2 * n, dict(_INSTR)
    assert merged >= 2 * n - 2, dict(_INSTR)  # the previous snapshot was changed-only
    assert _INSTR["prelock_hash_reused"] == merged, dict(_INSTR)
    for kind in ("node", "edge"):
        used = refresh["prelock_hash"][kind]
        assert used.startswith("used: ") and used.endswith(" merged rows reused a pre-lock hash")
        hit, total = used[len("used: "):].split(" merged")[0].split(" of ")
        assert hit == total, used
    generation = str(third["generation_id"])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        published = _capture_text(conn, generation)
        assert _prelock_tables(conn) == []
    rebuilt, _ = _materialize_rows(clean_runtime, f"{generation}:rebuild")
    assert len(published) == 2 * n
    assert published == rebuilt

    _write_node(clean_runtime, nodes[2], 4)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        plan = _publisher(clean_runtime)._prelock_attribute_hash_plan(conn)
    # Fix 5: the decision now rests on the rows written since then, which
    # here are few, not on the full snapshot alone.
    assert plan.run is False, plan
    assert plan.code == "changed_rows_below_threshold", plan
    assert plan.reason.startswith(
        f"a full count-stamp cycle over a full previous snapshot ({n} node "
        f"rows, {n} edge rows) is expected, and only {plan.changed_rows} row "
        "version(s) were written since the last checkpoint"
    ), plan
    # Each kind's count is an index scan filtered by its kind, not a scan of
    # the whole snapshot table.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert attributes_module.prior_snapshot_coverage(conn, generation) == {
            "node": (n, n), "edge": (n, n),
        }
        conn.execute("SET enable_seqscan = off")
        partial = {
            "node": "attribute_effective_view_node_subtype_idx",
            "edge": "attribute_effective_view_edge_type_idx",
        }
        for kind in ("node", "edge"):
            plan = "\n".join(
                row[0] for row in conn.execute(
                    "EXPLAIN " + attributes_module._SNAPSHOT_KIND_COUNT_SQL[kind],
                    (generation, "default", n),
                ).fetchall()
            )
            assert "Seq Scan" not in plan and "Index" in plan, plan
            # Either the kind's partial index, or another index plus a
            # filter on the kind; never the whole table.
            assert partial[kind] in plan or f"target_kind = '{kind}'" in plan, plan


def test_prelock_pass_is_skipped_while_another_session_holds_the_publish_lock(
    clean_runtime: str,
) -> None:
    from okg.substrate.publisher.publisher import _advisory_keys

    _seed(clean_runtime)
    publisher = _publisher(clean_runtime)
    classid, objid = _advisory_keys(publisher.deployment_name)
    with psycopg.connect(clean_runtime, autocommit=True) as holder, \
            psycopg.connect(clean_runtime, autocommit=True) as waiter:
        holder.execute("SELECT pg_advisory_lock(%s, %s)", (classid, objid))
        try:
            assert publisher._prelock_attribute_hash_plan(waiter)[:3] == (
                False,
                "another session holds the publish lock; hashing now would "
                "compete with its publish",
                "publish_lock_busy",
            )
        finally:
            holder.execute("SELECT pg_advisory_unlock(%s, %s)", (classid, objid))
        assert publisher._prelock_attribute_hash_plan(waiter)[:2] == (
            True, "first publish of the branch"
        )


def test_the_same_publish_lock_held_in_another_database_does_not_skip_the_pass(
    clean_runtime: str,
) -> None:
    """Advisory locks are per database, but pg_locks lists every database on
    the server. A session holding the same deployment's publish lock in
    another database (parallel test workers, or two deployments with one name
    on a shared server) holds an unrelated lock: the pass still plans."""
    from okg.substrate.publisher.publisher import _advisory_keys
    from okg.substrate.dsn_form import dsn_with_dbname

    _seed(clean_runtime)
    publisher = _publisher(clean_runtime)
    classid, objid = _advisory_keys(publisher.deployment_name)
    other_database = dsn_with_dbname(clean_runtime, "postgres")
    with psycopg.connect(other_database, autocommit=True) as holder, \
            psycopg.connect(clean_runtime, autocommit=True) as waiter:
        holder.execute("SELECT pg_advisory_lock(%s, %s)", (classid, objid))
        try:
            held_elsewhere = waiter.execute(
                "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' AND granted "
                "AND classid = %s::bigint::oid AND objid = %s::bigint::oid AND objsubid = 2",
                (classid & 0xFFFFFFFF, objid & 0xFFFFFFFF),
            ).fetchone()[0]
            # The waiter sees the other database's lock (>= 1: another
            # worker's publish of the same name may hold one too).
            assert held_elsewhere >= 1
            assert publisher._prelock_attribute_hash_plan(waiter)[:3] == (
                True, "first publish of the branch", "first_publish",
            )
        finally:
            holder.execute("SELECT pg_advisory_unlock(%s, %s)", (classid, objid))


def test_materialize_that_writes_nothing_says_why_the_cache_was_unused(
    clean_runtime: str,
) -> None:
    seeded = _seed(clean_runtime)
    generation = f"gen-prelock-done-{seeded['suffix']}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        attributes_module.materialize_effective_attribute_view_for_generation(
            conn, generation_id=generation, previous_generation_id=None,
            empty_overlay_fast_path=False,
        )
        cache = attributes_module.prepare_prelock_attribute_hash_cache(conn)
        result = attributes_module.materialize_effective_attribute_view_for_generation(
            conn, generation_id=generation, previous_generation_id=None,
            empty_overlay_fast_path=False, prelock_hash_cache=cache,
        )
        attributes_module.drop_prelock_attribute_hash_cache(conn, cache)
    assert result["reason"] == "already_materialized", result
    assert result["prelock_hash"] == {
        "node": "unused: generation already materialized",
        "edge": "unused: generation already materialized",
    }, result


# --------------------------------------------------------------------------- #
# Issue 2941 fix 5: hash the rows written since the last checkpoint.
#
# On the real okg-workspace sequence (a small git-files publish, then a
# code-structure ingest adding ~1.8M rows) the second publish is an
# incremental cycle (count_stamp_cadence 1 of 20). Before fix 5 the plan
# returned "an incremental cycle is expected" for every such cycle, so all of
# those rows were hashed under the lock.

_NEW_ROWS = 30


def _land_new_source(dsn: str, count: int = _NEW_ROWS) -> list[str]:
    """A new source adds ``count`` nodes and ``count`` edges among them."""
    suffix = uuid.uuid4().hex[:8]
    new_nodes = [f"doc:src2:{i}:{suffix}" for i in range(count)]
    facts: list = [
        NodeFact(node, "doc", _payload(100 + i), {"id": node}, {"v": 1})
        for i, node in enumerate(new_nodes)
    ]
    facts += [
        EdgeFact(
            new_nodes[i],
            new_nodes[(i + 1) % count],
            "cites",
            attrs={"weight": i / 8, **_payload(200 + i)},
            source_record_id={"id": f"edge:src2:{i}:{suffix}"},
            source_revision={"v": 1},
        )
        for i in range(count)
    ]
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            facts, source="default", strict_endpoints=True
        )
    return new_nodes


def _assert_all_reused(refresh: dict, nodes: int, edges: int) -> None:
    assert refresh["prelock_hash"] == {
        "node": f"used: {nodes} of {nodes} merged rows reused a pre-lock hash",
        "edge": f"used: {edges} of {edges} merged rows reused a pre-lock hash",
    }, refresh


def test_incremental_cycle_with_many_new_rows_hashes_them_before_the_lock(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The real-workload shape at test scale: a first publish, then a new
    source whose rows exceed the threshold. The pass runs over the changed
    rows only, every merged row reuses its hash, and the publish equals a
    cacheless rebuild."""
    seeded = _seed(clean_runtime)
    n = len(seeded["nodes"])
    first = _publisher(clean_runtime).cycle()
    assert first["status"] == "published", first
    monkeypatch.setattr(publisher_module, "_PRELOCK_CHANGED_MIN_ROWS", _NEW_ROWS)
    _land_new_source(clean_runtime)

    attributes_module._reset_materialize_instrumentation()
    second = _publisher(clean_runtime).cycle()
    assert second["status"] == "published", second
    refresh = second["attribute_effective_view_refresh"]
    prelock = refresh["prelock_hash_pass"]
    assert prelock["planned"] is True, prelock
    assert prelock["reason_code"] == "changed_rows", prelock
    assert prelock["scope"] == "changed", prelock
    assert prelock["reason"].startswith("an incremental cycle is expected"), prelock
    assert prelock["changed_rows_counted"] == _NEW_ROWS, prelock
    # Only the rows written since the checkpoint were hashed before the lock.
    assert prelock["rows"] == {"node": _NEW_ROWS, "edge": _NEW_ROWS}, prelock
    # A dirty-driven merge of exactly the new rows, all from the cache.
    _assert_all_reused(refresh, _NEW_ROWS, _NEW_ROWS)
    assert _INSTR["prelock_hash_reused"] == 2 * _NEW_ROWS, dict(_INSTR)

    generation = str(second["generation_id"])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _prelock_tables(conn) == []
    rebuilt, _ = _materialize_rows(clean_runtime, f"{generation}:rebuild")
    assert len(rebuilt) == 2 * (n + _NEW_ROWS)
    # The changed-only snapshot rows equal the same targets of a full,
    # cacheless rebuild of the published branch head, byte for byte.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        published = _capture_text(conn, generation)
    assert len(published) == 2 * _NEW_ROWS, len(published)
    for key, row in published.items():
        assert rebuilt[key] == row, (key, row, rebuilt[key])


def test_full_cycle_over_a_full_previous_snapshot_now_hashes_changed_rows(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Before fix 5 a full cycle whose previous snapshot was full always
    skipped the pass. It now runs it over the changed rows when there are
    enough of them, and the carry-forward full scan reuses those hashes."""
    seeded = _seed(clean_runtime)
    n = len(seeded["nodes"])
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "1")
    first = _publisher(clean_runtime).cycle()
    assert first["status"] == "published", first
    _write_node(clean_runtime, seeded["nodes"][0], 2)
    # A full cycle: its snapshot is full, the state the old plan refused on.
    full = _publisher(clean_runtime).cycle()
    assert full["status"] == "published", full
    full_generation = str(full["generation_id"])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert attributes_module.prior_snapshot_coverage(conn, full_generation) == {
            "node": (n, n), "edge": (n, n),
        }

    monkeypatch.setattr(publisher_module, "_PRELOCK_CHANGED_MIN_ROWS", _NEW_ROWS)
    _land_new_source(clean_runtime)
    attributes_module._reset_materialize_instrumentation()
    third = _publisher(clean_runtime).cycle()
    assert third["status"] == "published", third
    refresh = third["attribute_effective_view_refresh"]
    prelock = refresh["prelock_hash_pass"]
    assert prelock["planned"] is True, prelock
    assert prelock["reason_code"] == "changed_rows", prelock
    assert prelock["reason"].startswith(
        f"a full count-stamp cycle over a full previous snapshot ({n} node "
        f"rows, {n} edge rows) is expected"
    ), prelock
    assert prelock["rows"] == {"node": _NEW_ROWS, "edge": _NEW_ROWS}, prelock
    # Full scan: carry-forward copied the unchanged rows, the new ones
    # merged, and every merged row reused its pre-lock hash.
    assert _INSTR["carried_rows"] == 2 * n, dict(_INSTR)
    _assert_all_reused(refresh, _NEW_ROWS, _NEW_ROWS)
    generation = str(third["generation_id"])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        published = _capture_text(conn, generation)
    rebuilt, _ = _materialize_rows(clean_runtime, f"{generation}:rebuild")
    assert len(published) == 2 * (n + _NEW_ROWS)
    assert published == rebuilt


def test_row_changed_between_the_pass_and_the_lock_is_rehashed(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Writes that land after the changed-rows pass and before the lock: a
    new row rewritten, an old row changed, and a scale-only rewrite (same
    jsonb value, other text). Each misses the cache and is hashed under the
    lock; the publish equals a cacheless rebuild."""
    seeded = _seed(clean_runtime)
    nodes = seeded["nodes"]
    n = len(nodes)
    assert _publisher(clean_runtime).cycle()["status"] == "published"
    monkeypatch.setattr(publisher_module, "_PRELOCK_CHANGED_MIN_ROWS", _NEW_ROWS)
    new_nodes = _land_new_source(clean_runtime)

    real_prepare = attributes_module.prepare_prelock_attribute_hash_cache

    def _prepare_then_write(conn, **kwargs):
        cache = real_prepare(conn, **kwargs)
        assert kwargs.get("writer_segment_ids"), kwargs
        _write_node(clean_runtime, new_nodes[0], 9)  # in the cache, rewritten
        _write_node(clean_runtime, nodes[0], 9)  # not in the cache
        with psycopg.connect(clean_runtime, autocommit=True) as other:
            physical = _physical_node_table(other)
            with other.cursor() as cur:
                cur.execute(
                    f"UPDATE {physical} SET attrs = jsonb_set(attrs, "
                    "'{n_scale_a}', '1.000'::jsonb) "
                    "WHERE node_id = %s AND deleted = FALSE",
                    (new_nodes[1],),
                )
                assert cur.rowcount >= 1
        return cache

    monkeypatch.setattr(
        attributes_module, "prepare_prelock_attribute_hash_cache",
        _prepare_then_write,
    )
    second = _publisher(clean_runtime).cycle()
    assert second["status"] == "published", second
    refresh = second["attribute_effective_view_refresh"]
    assert refresh["prelock_hash_pass"]["scope"] == "changed", refresh
    # 30 new nodes plus the changed old node merged; the rewritten new node
    # and the scale-only rewrite missed the cache, as did the old node.
    merged_nodes = _NEW_ROWS + 1
    assert refresh["prelock_hash"] == {
        "node": (
            f"used: {merged_nodes - 3} of {merged_nodes} merged rows reused "
            "a pre-lock hash"
        ),
        "edge": f"used: {_NEW_ROWS} of {_NEW_ROWS} merged rows reused a pre-lock hash",
    }, refresh
    generation = str(second["generation_id"])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        published = _capture_text(conn, generation)
    rebuilt, _ = _materialize_rows(clean_runtime, f"{generation}:rebuild")
    assert len(rebuilt) == 2 * (n + _NEW_ROWS)
    assert len(published) == merged_nodes + _NEW_ROWS
    for key, row in published.items():
        assert rebuilt[key] == row, (key, row, rebuilt[key])
    assert json.loads(published[("node", new_nodes[0])][-2])["rev"] == 9
    assert json.loads(published[("node", nodes[0])][-2])["rev"] == 9
    assert '"n_scale_a": 1.000' in published[("node", new_nodes[1])][-2]


@pytest.mark.parametrize("parallel", [True, False], ids=["parallel", "serial"])
def test_changed_scope_cache_in_a_dirty_merge_writes_identical_rows(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch, parallel: bool
) -> None:
    """Materialize-level parity for the dirty-driven merge: the same
    generation materialized with and without a changed-scope cache, after the
    branch changed under the cache (changed row, scale-only rewrite, new row,
    late overlay), writes identical hashes, versions and snapshot rows."""
    seeded = _seed(clean_runtime)
    first = _publisher(clean_runtime).cycle()
    assert first["status"] == "published", first
    previous = str(first["generation_id"])
    if parallel:
        monkeypatch.setattr(attributes_module, "_PARALLEL_HASH_MIN_ROWS", 0)
    else:
        monkeypatch.setattr(
            attributes_module,
            "_parallel_hash_workers",
            lambda conn: (0, "forced serial for the test"),
        )
    new_nodes = _land_new_source(clean_runtime)
    generation = f"gen-prelock-dirty-{seeded['suffix']}"

    with psycopg.connect(clean_runtime, autocommit=True) as cache_conn:
        segments, _ = publisher_module.Publisher._chronos_rows_written_since_checkpoint(
            cache_conn, branch_name="default", limit=10**9,
        )
        cache = attributes_module.prepare_prelock_attribute_hash_cache(
            cache_conn, writer_segment_ids=segments,
        )
        assert cache.report["scope"] == "changed", cache.report
        assert cache.report["node"] == _NEW_ROWS, cache.report
        assert cache.report["edge"] == _NEW_ROWS, cache.report
        added = _change_branch_after_cache(clean_runtime, new_nodes, generation)
        dirty = _written_since(cache_conn, segments)
        assert added in dirty.node_ids and new_nodes[0] in dirty.node_ids
        cache_conn.autocommit = False
        cached_rows, cached = _materialize_dirty(
            cache_conn, generation, previous, dirty, cache
        )
        cache_conn.autocommit = True
        attributes_module.drop_prelock_attribute_hash_cache(cache_conn, cache)
        assert _prelock_tables(cache_conn) == []
    with psycopg.connect(clean_runtime) as conn:
        plain_rows, plain = _materialize_dirty(conn, generation, previous, dirty)

    assert set(cached_rows) == set(plain_rows), set(cached_rows) ^ set(plain_rows)
    for key, row in plain_rows.items():
        assert cached_rows[key] == row, (key, cached_rows[key], row)
    assert plain["prelock_hash"] is None
    # Merged: the 30 new nodes, the node added after the pass; the changed
    # row, the scale-only rewrite, the added row and the overlaid row miss.
    merged = _NEW_ROWS + 1
    assert cached["prelock_hash"] == {
        "node": f"used: {merged - 4} of {merged} merged rows reused a pre-lock hash",
        "edge": f"used: {_NEW_ROWS} of {_NEW_ROWS} merged rows reused a pre-lock hash",
    }, cached["prelock_hash"]
    assert '"n_scale_a": 1.000' in cached_rows[("node", new_nodes[1])][-2]
    assert cached_rows[("node", new_nodes[2])][-1] is not None  # overlay kept


def _written_since(conn, segments) -> "attributes_module.DirtyTargets":
    """Every node and edge id written in ``segments``: the raw dirty set."""
    ids = {}
    for table, col in (("okg.graph_nodes", "node_id"), ("okg.graph_edges", "edge_id")):
        with conn.cursor() as cur:
            cur.execute(
                "SELECT physical_table FROM _chronos_branch_tables "
                "WHERE table_name = %s AND backend = 'interval'",
                (table,),
            )
            physical = str(cur.fetchone()[0])
            cur.execute(
                f"SELECT DISTINCT {col}::text FROM {physical} "
                "WHERE writer_segment_id = ANY(%s)",
                (list(segments),),
            )
            ids[col] = frozenset(r[0] for r in cur.fetchall())
    return attributes_module.DirtyTargets(
        node_ids=ids["node_id"], edge_ids=ids["edge_id"]
    )


def _materialize_dirty(conn, generation, previous, dirty, cache=None):
    attributes_module._reset_materialize_instrumentation()
    result = attributes_module.materialize_effective_attribute_view_for_generation(
        conn,
        generation_id=generation,
        previous_generation_id=previous,
        empty_overlay_fast_path=False,
        dirty_targets=dirty,
        prelock_hash_cache=cache,
    )
    rows = _capture_text(conn, generation)
    conn.rollback()
    assert result["materialized"] is True, result
    return rows, result


def _plan_events(caplog: pytest.LogCaptureFixture) -> list[dict]:
    return [
        r.okg_event for r in caplog.records
        if getattr(r, "okg_event", {}).get("event")
        == "publisher.prelock_attribute_hash_plan"
    ]


def test_plan_that_raises_still_reports_a_reason_code(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A plan that fails (here a refusal; a database error takes the same
    path) leaves no plan reason, so the report says plan_failed, the error
    and the elapsed time, and the structured record says so too."""
    _seed(clean_runtime)
    publisher = _publisher(clean_runtime)

    def _raise(_conn):
        raise SubstrateError("injected plan failure")

    monkeypatch.setattr(publisher, "_prelock_attribute_hash_plan", _raise)
    caplog.set_level("INFO", logger=publisher_module.__name__)
    report: dict = {}
    timings: dict = {}
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert publisher._prelock_attribute_hash(conn, timings, report) is None
    assert report == {
        "planned": False,
        "reason": "the pre-lock plan failed",
        "reason_code": "plan_failed",
        "failed": "SubstrateError: injected plan failure",
        "elapsed_ms": report["elapsed_ms"],
    }, report
    assert timings["attribute_hash_prelock"] == report["elapsed_ms"]
    event = _plan_events(caplog)[-1]
    assert event["outcome"] == "failed", event
    assert event["reason_code"] == "plan_failed", event
    assert event["failed"] == "SubstrateError: injected plan failure", event
    assert "elapsed_ms" in event, event


def test_plan_record_is_written_after_the_pass_with_rows_and_time(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The one structured record per cycle is written once the pass is over:
    it carries the rows hashed, the elapsed time and the outcome; a bug that
    propagates is recorded as error."""
    _seed(clean_runtime)
    assert _publisher(clean_runtime).cycle()["status"] == "published"
    monkeypatch.setattr(publisher_module, "_PRELOCK_CHANGED_MIN_ROWS", _NEW_ROWS)
    _land_new_source(clean_runtime)
    caplog.set_level("INFO", logger=publisher_module.__name__)
    publisher = _publisher(clean_runtime)
    report: dict = {}
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cache = publisher._prelock_attribute_hash(conn, {}, report)
        assert cache is not None
        attributes_module.drop_prelock_attribute_hash_cache(conn, cache)
    events = _plan_events(caplog)
    assert len(events) == 1, events
    event = events[0]
    assert event["outcome"] == "completed", event
    assert event["reason_code"] == "changed_rows", event
    assert event["rows"] == {"node": _NEW_ROWS, "edge": _NEW_ROWS}, event
    assert event["elapsed_ms"] == report["elapsed_ms"] > 0, event

    def _bug(*_a, **_k):
        raise RuntimeError("a bug in the pass")

    monkeypatch.setattr(attributes_module, "prepare_prelock_attribute_hash_cache", _bug)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with pytest.raises(RuntimeError, match="a bug in the pass"):
            publisher._prelock_attribute_hash(conn, {}, {})
    assert _plan_events(caplog)[-1]["outcome"] == "error"
