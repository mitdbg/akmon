"""okg#3112: index reclaim, ``okg db reclaim-index-space``.

PACT lane-h-3112-storage. Native PostgreSQL.

A rolled-back bulk write keeps the btree pages its rows allocated; VACUUM then
truncates the heap and leaves the index allocation behind. Until PACT
lane-s-storage-drop-copies the table this happened to was the interval front,
written inside the publication transaction; the copies are gone (migration
0046), so these tests produce the state directly: a rolled-back bulk insert
into ``okg.attribute_overlay_ranges``.

What the tests prove (req.reclaim / req.preserve / req.after): the dry run
changes nothing; refusals under a held publisher lock, a source run in
progress and an invalid index; a REINDEX that waits on another session's
snapshot fails cleanly; a forced fingerprint difference fails the verb with
both values; the CLI dry run and apply.

Scale is small so the file fits a CI lane. The okg#1324 detector's 50 MiB
floor would flag nothing at this scale, so the tests lower it (``_FLOOR``);
the detector's query is otherwise unchanged.
"""

from __future__ import annotations

import json
import time
import uuid
from typing import Any

import pytest

psycopg = pytest.importorskip("psycopg")


from okg.substrate.cli.main import main as okg_main  # noqa: E402
from okg.substrate.db import index_reclaim  # noqa: E402
from okg.substrate.errors import SubstrateError  # noqa: E402
from okg.substrate.graph_store import ChronosGraphStore  # noqa: E402
from okg.substrate.library.sources.base import EdgeFact, NodeFact  # noqa: E402
from okg.substrate.publisher import Publisher  # noqa: E402
from okg.substrate.runtime.source_sync_priority import (  # noqa: E402
    publisher_publish_advisory_keys,
)
from tests.pytest.catalog_fixture_builders import (  # noqa: E402
    SubtypeSpec,
    bind_test_catalog_writer,
    seed_catalog,
)

pytestmark = pytest.mark.postgres

_DEPLOYMENT = "default"
#: The table the cheap refusal tests bloat on their own (a rolled-back bulk
#: insert): no foreign key to satisfy; a primary key and two indexes.
_CHEAP = "okg.attribute_overlay_ranges"
#: The detector's index-size floor for this file (the shipped floor is 50 MiB).
_FLOOR = 256 * 1024
# --------------------------------------------------------------------------- #
# Seeding                                                                      #
# --------------------------------------------------------------------------- #


def _node_attrs(i: int, rev: int) -> dict[str, Any]:
    return {
        "path": f"dir{i % 97:02d}/file{i:07d}.csv",
        "owner": ["alice", "bob", "carol", "dave"][i % 4],
        "rank": (i * 7 + rev) % 1000,
        "active": (i + rev) % 2 == 0,
        "seen_at": f"2026-0{(i % 9) + 1}-{(i % 27) + 1:02d}T00:00:00Z",
        "note": f"row {i} revision {rev}",
    }


def _nid(i: int, suffix: str) -> str:
    return f"doc:{i:07d}:{suffix}"


def _apply(dsn: str, facts: list[Any]) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        for start in range(0, len(facts), 5000):
            store.apply_facts(
                facts[start:start + 5000], source="default", strict_endpoints=True
            )


def _node_facts(suffix: str, ids: range, rev: int) -> list[Any]:
    return [
        NodeFact(
            _nid(i, suffix),
            "doc",
            _node_attrs(i, rev),
            {"id": _nid(i, suffix)},
            {"v": rev},
        )
        for i in ids
    ]


def _edge_facts(suffix: str, n_nodes: int, per_node: int) -> list[Any]:
    return [
        EdgeFact(
            src=_nid(i, suffix),
            dst=_nid((i + k * 101) % n_nodes, suffix),
            edge_type="links",
            attrs={
                "label": f"l{(i + k) % 13}",
                "rank": (i + k) % 50,
                "seen_at": f"2026-09-{(k % 27) + 1:02d}T12:00:00+00:00",
            },
            source_record_id={"id": f"link:{i}:{k}:{suffix}"},
            source_revision={"v": 1},
        )
        for i in range(n_nodes)
        for k in range(1, per_node + 1)
    ]


def _seed(dsn: str, *, n_nodes: int, edges_per_node: int) -> tuple[Any, str]:
    suffix = uuid.uuid4().hex[:6]
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=[
                SubtypeSpec(
                    name="doc",
                    metadata={
                        "filterable_attrs": {
                            "path": "text",
                            "owner": "text",
                            "rank": "num",
                            "active": "bool",
                            "seen_at": "ts",
                            "label": "text",
                        }
                    },
                )
            ],
            narrowings=[("doc", "links", "doc")],
        )
    identity = bind_test_catalog_writer(dsn, _DEPLOYMENT)
    _apply(dsn, _node_facts(suffix, range(n_nodes), rev=1))
    if edges_per_node:
        _apply(dsn, _edge_facts(suffix, n_nodes, edges_per_node))
    return identity, suffix


# --------------------------------------------------------------------------- #
# Publishing, failing, cancelling                                              #
# --------------------------------------------------------------------------- #


def _publish(dsn: str, identity: Any) -> dict[str, Any]:
    return Publisher(dsn, _DEPLOYMENT, catalog_identity=identity).cycle()


# --------------------------------------------------------------------------- #
# Refusals and the forced difference (cheap state: a rolled-back bulk insert)  #
# --------------------------------------------------------------------------- #


def _rolled_back_allocation(dsn: str, rows: int = 40_000) -> None:
    """The failure mechanism on its own: insert, roll back, VACUUM."""
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {_CHEAP} (chronos_branch_id, target_kind, target_id,
                                  namespace, attr_key, from_ord, to_ord,
                                  observation_id, value, value_hash)
            SELECT 'okg-3112', 'node', 'doc:' || g, 'default', 'path', 0, 1,
                   'obs:' || g, jsonb_build_object('path', 'v' || g), 'v' || g
              FROM generate_series(1, %s) g
            """,
            (rows,),
        )
        conn.rollback()
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(f"VACUUM {_CHEAP}")


def _index_oids(dsn: str) -> list[int]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT indexrelid::bigint FROM pg_index "
            "WHERE indrelid = %s::regclass ORDER BY 1",
            (_CHEAP,),
        )
        return [int(r[0]) for r in cur.fetchall()]


def test_reclaim_refuses_while_the_publisher_lock_is_held(clean_runtime: str) -> None:
    dsn = clean_runtime
    _rolled_back_allocation(dsn)
    oids = _index_oids(dsn)
    classid, objid = publisher_publish_advisory_keys(_DEPLOYMENT)
    with psycopg.connect(dsn, autocommit=True) as holder, holder.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(%s, %s)", (classid, objid))
        with pytest.raises(SubstrateError) as refused:
            index_reclaim.reclaim_index_space(
                dsn,
                deployment=_DEPLOYMENT,
                apply=True,
                min_index_bytes=_FLOOR,
                publish_lock_wait_seconds=0.5,
            )
        assert refused.value.data["reason"] == "publisher_lock_held"
        assert "okg up" not in (refused.value.fix_hint or "")
        assert "reclaim-index-space" in (refused.value.fix_hint or "")
        # The dry run reports the same blocker, and fails.
        with pytest.raises(SubstrateError) as blocked:
            index_reclaim.reclaim_index_space(
                dsn, deployment=_DEPLOYMENT, min_index_bytes=_FLOOR
            )
        assert blocked.value.data["reason"] == "dry_run_blocked"
        assert blocked.value.data["blocked_by"] == ["publisher_lock_held"]
        assert _CHEAP in {t["qualified"] for t in blocked.value.data["tables"]}
    assert _index_oids(dsn) == oids, "an index was rebuilt under a held publisher lock"
    # Released: the same call succeeds.
    applied = index_reclaim.reclaim_index_space(
        dsn, deployment=_DEPLOYMENT, apply=True, min_index_bytes=_FLOOR
    )
    assert applied["applied"] is True and _index_oids(dsn) != oids


def test_reclaim_refuses_while_a_source_run_is_in_progress(clean_runtime: str) -> None:
    """A live run base (a run still writing) refuses before any read; once the
    run's process lets go, the run reads as ended and the verb proceeds."""
    from okg.substrate.ingest.run_base import (
        hold_run_liveness,
        run_base_checkpoint_id,
        take_run_base,
    )

    dsn = clean_runtime
    identity, _suffix = _seed(dsn, n_nodes=20, edges_per_node=0)
    assert _publish(dsn, identity)["status"] == "published"
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute("SELECT DISTINCT branch_id FROM okg.generation_ordinals")
        (branch,) = [str(r[0]) for r in cur.fetchall()]
    _rolled_back_allocation(dsn)
    oids = _index_oids(dsn)
    checkpoint_id = run_base_checkpoint_id(branch, "okg-3112-run")
    liveness = hold_run_liveness(dsn, checkpoint_id)
    try:
        store = ChronosGraphStore.connect(dsn)
        try:
            assert take_run_base(
                store, deployment=_DEPLOYMENT, branch=branch, source="default",
                run_id="okg-3112-run",
            ) == checkpoint_id
        finally:
            store.close()
        with pytest.raises(SubstrateError) as refused:
            index_reclaim.reclaim_index_space(
                dsn, deployment=_DEPLOYMENT, apply=True, min_index_bytes=_FLOOR
            )
        data = refused.value.data
        assert data["reason"] == "source_run_in_progress", data
        assert [run["run_id"] for run in data["in_progress"]] == ["okg-3112-run"]
        assert "okg-3112-run" in (refused.value.fix_hint or "")
        with pytest.raises(SubstrateError) as blocked:
            index_reclaim.reclaim_index_space(
                dsn, deployment=_DEPLOYMENT, min_index_bytes=_FLOOR
            )
        assert blocked.value.data["blocked_by"] == ["source_run_in_progress:okg-3112-run"]
        assert _index_oids(dsn) == oids
    finally:
        liveness.close()
    applied = index_reclaim.reclaim_index_space(
        dsn, deployment=_DEPLOYMENT, apply=True, min_index_bytes=_FLOOR
    )
    assert applied["applied"] is True and _index_oids(dsn) != oids


def test_reindex_artifact_names_match_only_this_runs_copies() -> None:
    """Only <existing>_ccnew[N] / _ccold[N] (truncated when the name would not
    fit 63 bytes) are this run's; a third party's index is never matched."""
    existing = {
        "okg.attribute_overlay_ranges_pkey",
        "okg.idx_attribute_overlay_ranges_key",
        "okg." + "x" * 60,
    }
    match = index_reclaim.is_reindex_artifact
    assert match("okg.attribute_overlay_ranges_pkey_ccnew", existing)
    assert match("okg.attribute_overlay_ranges_pkey_ccnew1", existing)
    assert match("okg.idx_attribute_overlay_ranges_key_ccold", existing)
    assert match("okg." + "x" * 57 + "_ccnew", existing)  # truncated base
    assert not match("okg.attribute_overlay_ranges_3112_ccnew", existing)  # someone else's
    assert not match("okg.attribute_overlay_ranges_new_idx", existing)
    assert not match("public.attribute_overlay_ranges_pkey_ccnew", existing)  # other schema
    assert not match("okg.attribute_overlay_ranges_pkey", existing)  # the original itself
    assert not match("okg.attribute_overlay_ranges_p_ccnew", existing)  # short, not truncated


def test_writer_starting_mid_run_fails_the_reclaim_cleanly(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A writer that starts while the verb runs, in a transaction holding a
    snapshot: the REINDEX's wait for that snapshot hits the bounded
    lock_timeout; the verb fails with reindex_failed, drops the invalid
    *_ccnew index its REINDEX left, and leaves no invalid index behind."""
    dsn = clean_runtime
    _rolled_back_allocation(dsn)
    real = index_reclaim._reindex_table
    seen: dict[str, Any] = {}

    def rebuild_starts_then_reindex(cur: Any, schema: str, relation: str) -> None:
        writer = psycopg.connect(dsn)
        try:
            writer.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            writer.execute(f"SELECT count(*) FROM {_CHEAP}").fetchone()
            seen["writer_started"] = True
            started = time.monotonic()
            try:
                real(cur, schema, relation)
            finally:
                seen["reindex_waited_s"] = round(time.monotonic() - started, 2)
                # The writer finishes after the REINDEX gave up.
                writer.rollback()
        finally:
            writer.close()

    monkeypatch.setattr(index_reclaim, "_reindex_table", rebuild_starts_then_reindex)
    with pytest.raises(SubstrateError) as failed:
        index_reclaim.reclaim_index_space(
            dsn,
            deployment=_DEPLOYMENT,
            apply=True,
            min_index_bytes=_FLOOR,
            lock_timeout_seconds=1.0,
        )
    data = failed.value.data
    assert seen["writer_started"] is True
    assert data["reason"] == "reindex_failed", data
    assert "deadlock" not in data["error"].lower(), data["error"]
    assert "lock timeout" in data["error"].lower(), data["error"]
    assert data["invalid_indexes_dropped"], data
    assert all("ccnew" in name for name in data["invalid_indexes_dropped"]), data
    assert data["invalid_indexes_left"] == [], data
    assert index_reclaim_invalid(dsn) == []
    # REINDEX TABLE also rebuilds the TOAST index; its leftover copy is
    # reported with its drop command and never dropped by the verb.
    toast = data["toast_invalid_indexes_left"]
    assert all(
        bad["index"].startswith("pg_toast.") and bad["index"].endswith("_ccnew")
        for bad in toast
    ), toast
    if toast:
        # It blocks the next run until the operator drops it.
        with pytest.raises(SubstrateError) as refused:
            index_reclaim.reclaim_index_space(
                dsn, deployment=_DEPLOYMENT, apply=True, min_index_bytes=_FLOOR
            )
        assert refused.value.data["reason"] == "invalid_index_present"
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            for bad in toast:
                cur.execute(bad["drop_command"])
    # And the verb runs cleanly afterwards.
    monkeypatch.setattr(index_reclaim, "_reindex_table", real)
    applied = index_reclaim.reclaim_index_space(
        dsn, deployment=_DEPLOYMENT, apply=True, min_index_bytes=_FLOOR
    )
    assert applied["applied"] is True


def index_reclaim_invalid(dsn: str) -> list[str]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT indexrelid::regclass::text FROM pg_index "
            "WHERE indrelid = %s::regclass "
            "AND NOT (indisvalid AND indisready AND indislive)",
            (_CHEAP,),
        )
        return [str(r[0]) for r in cur.fetchall()]


def test_reclaim_refuses_when_an_invalid_index_exists(clean_runtime: str) -> None:
    dsn = clean_runtime
    _rolled_back_allocation(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        # Two committed rows with one value, then a unique concurrent build
        # that fails on them: PostgreSQL leaves the index behind, invalid --
        # what an interrupted REINDEX CONCURRENTLY leaves as *_ccnew.
        for target in ("doc:a", "doc:b"):
            cur.execute(
                f"INSERT INTO {_CHEAP} (chronos_branch_id, target_kind, "
                "target_id, namespace, attr_key, from_ord, to_ord, "
                "observation_id, value, value_hash) "
                "VALUES ('okg-3112', 'node', %s, 'default', 'path', 0, 1, "
                "'obs:same', '\"same\"'::jsonb, 'same')",
                (target,),
            )
        with pytest.raises(psycopg.errors.UniqueViolation):
            cur.execute(
                "CREATE UNIQUE INDEX CONCURRENTLY attribute_overlay_ranges_3112_ccnew "
                f"ON {_CHEAP} (value_hash)"
            )
    oids = _index_oids(dsn)
    with pytest.raises(SubstrateError) as blocked:
        index_reclaim.reclaim_index_space(dsn, deployment=_DEPLOYMENT, min_index_bytes=_FLOOR)
    assert blocked.value.data["reason"] == "dry_run_blocked"
    assert blocked.value.data["blocked_by"] == [
        "invalid_index:okg.attribute_overlay_ranges_3112_ccnew"
    ], blocked.value.data
    with pytest.raises(SubstrateError) as refused:
        index_reclaim.reclaim_index_space(
            dsn, deployment=_DEPLOYMENT, apply=True, min_index_bytes=_FLOOR
        )
    assert refused.value.data["reason"] == "invalid_index_present"
    (bad,) = refused.value.data["invalid_indexes"]
    assert bad["drop_command"] == (
        'DROP INDEX CONCURRENTLY "okg"."attribute_overlay_ranges_3112_ccnew";'
    )
    assert _index_oids(dsn) == oids


def test_reclaim_fails_loudly_when_the_fingerprint_changes(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    dsn = clean_runtime
    _rolled_back_allocation(dsn)
    real = index_reclaim._reindex_table

    def reindex_and_tamper(cur: Any, schema: str, relation: str) -> None:
        real(cur, schema, relation)
        cur.execute(f'COMMENT ON TABLE "{schema}"."{relation}" IS \'changed mid-run\'')

    monkeypatch.setattr(index_reclaim, "_reindex_table", reindex_and_tamper)
    with pytest.raises(SubstrateError) as failed:
        index_reclaim.reclaim_index_space(
            dsn, deployment=_DEPLOYMENT, apply=True, min_index_bytes=_FLOOR
        )
    data = failed.value.data
    assert data["reason"] == "fingerprint_changed"
    assert {
        "path": f"tables.{_CHEAP}.table.comment",
        "before": None,
        "after": "changed mid-run",
    } in data["differences"], data["differences"]
    assert data["fingerprint_before"] != data["fingerprint_after"]
    assert _CHEAP in data["rebuilt"]


def test_reclaim_cli_dry_run_and_apply(
    clean_runtime: str, capsys: pytest.CaptureFixture[str]
) -> None:
    dsn = clean_runtime
    _rolled_back_allocation(dsn)
    base = [
        "db", "reclaim-index-space", "--deployment", _DEPLOYMENT, "--dsn", dsn,
        "--min-index-bytes", str(_FLOOR), "--json",
    ]
    oids = _index_oids(dsn)
    assert okg_main(base) == 0
    dry = json.loads(capsys.readouterr().out)
    payload = dry.get("data", dry)
    assert payload["dry_run"] is True
    assert _CHEAP in {t["qualified"] for t in payload["tables"]}
    assert _index_oids(dsn) == oids
    # A dry run that finds a blocker fails like the other refusing db verbs:
    # exit 1 and the JSON error envelope on stderr.
    classid, objid = publisher_publish_advisory_keys(_DEPLOYMENT)
    with psycopg.connect(dsn, autocommit=True) as holder, holder.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(%s, %s)", (classid, objid))
        assert okg_main(base) == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    envelope = json.loads(captured.err)
    assert "dry_run_blocked" in json.dumps(envelope), envelope
    assert "publisher_lock_held" in json.dumps(envelope), envelope
    assert okg_main(base + ["--apply"]) == 0
    applied = json.loads(capsys.readouterr().out)
    payload = applied.get("data", applied)
    assert payload["applied"] is True and payload["reclaimed_bytes"] > 0
    assert _index_oids(dsn) != oids
