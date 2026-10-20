"""okg#3112: a failed or cancelled publish, the retry plan, and index reclaim.

PACT lane-h-3112-storage. Native PostgreSQL, the real ``Publisher.cycle()``.

A full publish writes ``okg.attribute_effective_view_intervals`` inside the
one publication transaction. A failure or a cancel rolls the rows back and
keeps the btree pages; VACUUM then truncates the heap to zero pages and leaves
the index allocation behind. (Until okg#3112 step 4b the same transaction also
wrote ``okg.attr_index``, whose derivation needed a nested-loop guard after
such a failure; step 4b removed that table, its derivation and the guard's
tests, and migration 0043 drops it.)

What the tests prove:

* req.regression -- a publish failing after the interval sync and one
  cancelled inside the interval INSERT leave zero live rows and index
  allocation above a stated floor after VACUUM
  (``test_failed_and_cancelled_publish_*``).
* req.reclaim / req.preserve / req.after -- ``okg db reclaim-index-space``:
  the dry run changes nothing, ``--apply`` returns the allocation, the
  fingerprint is equal (and an independent one the test reads itself), the
  disk check stops flagging the table, and publishing resumes; refusals under
  a held publisher lock, a source run in progress and an invalid index; a
  REINDEX that waits on another session's snapshot fails cleanly; a forced
  fingerprint difference fails the verb with both values; and a
  committed-history case.

Scale is small so the file fits a CI lane. The okg#1324 detector's 50 MiB
floor would flag nothing at this scale, so the tests lower it (``_FLOOR``) for
both the verb and ``okg doctor --check disk``; the detector's query is
otherwise unchanged.
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from typing import Any

import pytest

psycopg = pytest.importorskip("psycopg")

from psycopg import sql  # noqa: E402

from okg.substrate import attributes as attributes_mod  # noqa: E402
from okg.substrate.cli.main import main as okg_main  # noqa: E402
from okg.substrate.db import index_reclaim  # noqa: E402
from okg.substrate.db import management  # noqa: E402
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
_TARGETS = ("okg.attribute_effective_view_intervals",)
#: The table the cheap refusal tests bloat on their own (a rolled-back bulk
#: insert): no foreign key to satisfy, one primary key index.
_CHEAP = "okg.attribute_versions"
#: The detector's index-size floor for this file (the shipped floor is 50 MiB).
_FLOOR = 256 * 1024
#: Index allocation that must survive a failed build plus VACUUM at this
#: scale, per table. A fresh empty table's indexes are one page each (8 KiB):
#: 4 x 8 KiB for the intervals table. Lane H's 800,000-target run measured
#: about 310 B per target on the intervals table after one failure; the floor
#: below is well under that and well over the empty size.
_N_NODES = 1500
_EDGES_PER_NODE = 2
_ALLOC_FLOOR = {
    "okg.attribute_effective_view_intervals": 300_000,
}


class _InjectedFailure(RuntimeError):
    pass


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
        with conn.cursor() as cur:
            # Authority markers the rebuild must preserve: a grant, table and
            # index comments. NOLOGIN: the role only owns an ACL entry.
            cur.execute(
                "DO $$ BEGIN CREATE ROLE okg_3112_reader NOLOGIN; "
                "EXCEPTION WHEN duplicate_object OR unique_violation THEN NULL; END $$"
            )
            for rel in _TARGETS:
                cur.execute(f"GRANT SELECT ON {rel} TO okg_3112_reader")
                cur.execute(f"COMMENT ON TABLE {rel} IS 'okg#3112 marker'")
            cur.execute(
                "COMMENT ON INDEX okg.attribute_effective_intervals_edge_idx "
                "IS 'okg#3112 index marker'"
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


def _publish_failing_after_interval_sync(
    dsn: str, identity: Any, monkeypatch: pytest.MonkeyPatch
) -> dict[str, Any]:
    """Fail the publish inside the publication transaction, right after the
    interval front is written (the full-proof reanchor or the delta sync)."""
    seen: dict[str, Any] = {}
    names = (
        "reanchor_effective_interval_view_for_generation",
        "sync_effective_interval_view_for_generation",
    )
    real = {name: getattr(attributes_mod, name) for name in names}

    def failing(name: str) -> Any:
        def run_then_fail(*args: Any, **kwargs: Any) -> Any:
            seen["sync"] = {"stage": name, **dict(real[name](*args, **kwargs) or {})}
            raise _InjectedFailure("injected failure after the interval sync")

        return run_then_fail

    for name in names:
        monkeypatch.setattr(attributes_mod, name, failing(name))
    try:
        with pytest.raises(_InjectedFailure):
            _publish(dsn, identity)
    finally:
        for name in names:
            monkeypatch.setattr(attributes_mod, name, real[name])
    assert seen.get("sync"), "the injected failure never reached the interval sync"
    return dict(seen["sync"])


def _publish_cancelled_in_interval_insert(dsn: str, identity: Any, last_target: str) -> dict[str, Any]:
    """Cancel the publish while it is inside
    ``INSERT INTO okg.attribute_effective_view_intervals``.

    At this scale the INSERT takes milliseconds, so a row trigger parks it on
    one target for up to 30 s; a watcher session cancels the backend once that
    statement has run for 0.3 s. Every earlier row of the INSERT is already
    written by then, and the trigger is dropped before anything is measured.
    """
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            CREATE FUNCTION public.okg_3112_park() RETURNS trigger
            LANGUAGE plpgsql AS $$
            BEGIN
              IF NEW.target_id = TG_ARGV[0] THEN PERFORM pg_sleep(30); END IF;
              RETURN NEW;
            END $$
            """
        )
        cur.execute(
            sql.SQL(
                "CREATE TRIGGER okg_3112_park BEFORE INSERT "
                "ON okg.attribute_effective_view_intervals "
                "FOR EACH ROW EXECUTE FUNCTION public.okg_3112_park({})"
            ).format(sql.Literal(last_target))
        )
    state: dict[str, Any] = {"cancelled": None}
    stop = threading.Event()

    def watcher() -> None:
        with psycopg.connect(dsn, autocommit=True) as wconn, wconn.cursor() as wcur:
            while not stop.is_set():
                wcur.execute(
                    """
                    SELECT pid FROM pg_stat_activity
                     WHERE state = 'active' AND pid <> pg_backend_pid()
                       AND datname = current_database()
                       AND query ILIKE '%%INSERT INTO okg.attribute_effective_view_intervals%%'
                       AND now() - query_start > interval '300 milliseconds'
                     LIMIT 1
                    """
                )
                row = wcur.fetchone()
                if row:
                    wcur.execute("SELECT pg_cancel_backend(%s)", (row[0],))
                    state["cancelled"] = int(row[0])
                    return
                time.sleep(0.05)

    thread = threading.Thread(target=watcher, daemon=True)
    thread.start()
    error: BaseException | None = None
    try:
        _publish(dsn, identity)
    except BaseException as exc:  # noqa: BLE001 -- the cancel is the point
        error = exc
    finally:
        stop.set()
        thread.join(timeout=10)
        with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                "DROP TRIGGER okg_3112_park ON okg.attribute_effective_view_intervals"
            )
            cur.execute("DROP FUNCTION public.okg_3112_park()")
    assert state["cancelled"] is not None, "the watcher never saw the interval INSERT"
    assert error is not None, "the cancelled publish did not raise"
    assert "cancel" in f"{type(error).__name__} {error}".lower(), repr(error)
    return {"cancelled_pid": state["cancelled"], "error": f"{type(error).__name__}"}


def _vacuum(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        for rel in _TARGETS:
            cur.execute(f"VACUUM {rel}")


def _state(dsn: str) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        for rel in _TARGETS:
            cur.execute(
                f"SELECT (SELECT count(*) FROM {rel}), "
                f"pg_relation_size('{rel}'::regclass, 'main'), "
                f"pg_indexes_size('{rel}'::regclass), "
                f"(SELECT reltuples FROM pg_class WHERE oid = '{rel}'::regclass)"
            )
            rows, heap, index, reltuples = cur.fetchone()
            out[rel] = {
                "rows": int(rows),
                "heap_bytes": int(heap),
                "index_bytes": int(index),
                "reltuples": int(reltuples),
            }
    return out


# --------------------------------------------------------------------------- #
# Fingerprint the test reads itself (independent of the verb's)                #
# --------------------------------------------------------------------------- #


def _own_fingerprint(dsn: str) -> dict[str, Any]:
    fp: dict[str, Any] = {}
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        for rel in _TARGETS:
            cur.execute(
                "SELECT ic.relname, pg_get_indexdef(i.indexrelid), i.indisvalid, "
                "obj_description(i.indexrelid, 'pg_class') "
                "FROM pg_index i JOIN pg_class ic ON ic.oid = i.indexrelid "
                "WHERE i.indrelid = %s::regclass ORDER BY 1",
                (rel,),
            )
            indexes = [list(map(str, r)) for r in cur.fetchall()]
            cur.execute(
                "SELECT relacl::text, obj_description(oid, 'pg_class'), "
                "reloptions::text FROM pg_class WHERE oid = %s::regclass",
                (rel,),
            )
            meta = list(map(str, cur.fetchone()))
            cur.execute(f"SELECT t::text FROM {rel} t ORDER BY 1")
            rows = hashlib.sha256(
                "\n".join(r[0] for r in cur.fetchall()).encode()
            ).hexdigest()
            fp[rel] = {"indexes": indexes, "meta": meta, "rows": rows}
        for rel in ("okg.attribute_versions", "okg.generation_ordinals"):
            cur.execute(f"SELECT t::text FROM {rel} t ORDER BY 1")
            fp[rel] = hashlib.sha256(
                "\n".join(r[0] for r in cur.fetchall()).encode()
            ).hexdigest()
    return fp


def _flagged_by_doctor(dsn: str, monkeypatch: pytest.MonkeyPatch) -> set[str]:
    monkeypatch.setattr(management, "_DISK_INDEX_BLOAT_MIN_INDEX_BYTES", _FLOOR)
    report = management.disk_usage_report(dsn, deployment_name=_DEPLOYMENT, record_sample=False)
    return {
        f["relation"] for f in report["findings"]
        if f.get("code") == "index_larger_than_table"
    }


# --------------------------------------------------------------------------- #
# req.regression + req.reclaim + req.preserve + req.after                      #
# --------------------------------------------------------------------------- #


def test_failed_and_cancelled_publish_leave_allocation_and_reclaim_returns_it(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch, record_property: Any
) -> None:
    dsn = clean_runtime
    fresh = _state(dsn)
    identity, suffix = _seed(dsn, n_nodes=_N_NODES, edges_per_node=_EDGES_PER_NODE)

    # a1: the first publish fails after the interval sync, inside the
    # publication transaction.
    synced = _publish_failing_after_interval_sync(dsn, identity, monkeypatch)
    assert synced["rows_written"] > 0, synced
    _vacuum(dsn)
    after_failure = _state(dsn)
    record_property("after_failure_vacuum", json.dumps(after_failure))
    for rel in _TARGETS:
        assert after_failure[rel]["rows"] == 0, after_failure
        assert after_failure[rel]["heap_bytes"] == 0, after_failure
        assert after_failure[rel]["index_bytes"] >= _ALLOC_FLOOR[rel], after_failure

    # a2: the next first publish is cancelled inside the interval INSERT.
    cancelled = _publish_cancelled_in_interval_insert(
        dsn, identity, last_target=_nid(_N_NODES - 1, suffix)
    )
    record_property("cancelled", json.dumps(cancelled))
    _vacuum(dsn)
    after_cancel = _state(dsn)
    record_property("after_cancel_vacuum", json.dumps(after_cancel))
    for rel in _TARGETS:
        assert after_cancel[rel]["rows"] == 0, after_cancel
        assert after_cancel[rel]["heap_bytes"] == 0, after_cancel
        assert after_cancel[rel]["index_bytes"] >= after_failure[rel]["index_bytes"], (
            after_failure, after_cancel,
        )

    # The disk check flags the table before the reclaim.
    assert set(_TARGETS) <= _flagged_by_doctor(dsn, monkeypatch)

    # Dry run: lists the tables, their bytes and the statements; writes nothing.
    dry = index_reclaim.reclaim_index_space(
        dsn, deployment=_DEPLOYMENT, apply=False, min_index_bytes=_FLOOR
    )
    assert dry["dry_run"] is True and dry["applied"] is False
    listed = {t["qualified"]: t for t in dry["tables"]}
    assert set(_TARGETS) <= set(listed), dry
    for rel in _TARGETS:
        assert listed[rel]["index_bytes"] == after_cancel[rel]["index_bytes"]
        assert listed[rel]["command"].startswith("REINDEX TABLE CONCURRENTLY ")
    assert dry["blocked_by"] == []
    assert _state(dsn) == after_cancel, "the dry run changed the database"

    own_before = _own_fingerprint(dsn)
    applied = index_reclaim.reclaim_index_space(
        dsn, deployment=_DEPLOYMENT, apply=True, min_index_bytes=_FLOOR
    )
    record_property("reclaim", json.dumps({k: applied[k] for k in (
        "index_bytes_before", "index_bytes_after", "reclaimed_bytes", "tables",
    )}))
    assert applied["applied"] is True and applied["fingerprint"]["equal"] is True
    assert set(_TARGETS) <= {t["relation"] for t in applied["tables"]}
    assert applied["reclaimed_bytes"] > 0
    assert applied["still_flagged"] == []
    assert _own_fingerprint(dsn) == own_before
    reclaimed = _state(dsn)
    for rel in _TARGETS:
        # Back to a fresh build's size: one page per index on an empty table.
        assert reclaimed[rel]["index_bytes"] <= fresh[rel]["index_bytes"] * 1.1, (
            fresh, reclaimed,
        )
    assert not (set(_TARGETS) & _flagged_by_doctor(dsn, monkeypatch))

    # The next publish succeeds and writes the interval front.
    result = _publish(dsn, identity)
    assert result["status"] == "published", result
    assert _state(dsn)["okg.attribute_effective_view_intervals"]["rows"] > 0

    # A delta publish succeeds too.
    _apply(dsn, _node_facts(suffix, range(0, _N_NODES, 50), rev=2))
    again = _publish(dsn, identity)
    assert again["status"] == "published", again


def test_reclaim_keeps_committed_history_fingerprint(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch, record_property: Any
) -> None:
    """Committed interval rows: a failed self-heal cycle adds allocation; the
    verb rebuilds what the detector flags and every row survives.

    The reanchor rewrites only targets whose payload changed or that are new
    (okg#2314), so the failing cycle brings many new targets on top of a small
    committed history: their rolled-back intervals leave the index allocation
    the detector flags (index over 3x the table), while the committed rows
    stay. Re-revising a few committed targets alone writes too little to flag.
    """
    dsn = clean_runtime
    committed = _N_NODES // 10
    identity, suffix = _seed(dsn, n_nodes=committed, edges_per_node=_EDGES_PER_NODE)
    assert _publish(dsn, identity)["status"] == "published"
    rel = "okg.attribute_effective_view_intervals"
    published = _state(dsn)
    assert published[rel]["rows"] > 0
    _apply(dsn, _node_facts(suffix, range(0, committed, 10), rev=2))
    _apply(dsn, _node_facts(suffix, range(committed, 3 * _N_NODES), rev=1))
    monkeypatch.setenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY", "1")
    synced = _publish_failing_after_interval_sync(dsn, identity, monkeypatch)
    monkeypatch.delenv("OKG_COUNT_STAMP_SELF_HEAL_EVERY")
    assert synced["stage"] == "reanchor_effective_interval_view_for_generation", synced
    _vacuum(dsn)
    before = _state(dsn)
    # The failure rolled back every row it wrote and kept their allocation.
    assert before[rel]["rows"] == published[rel]["rows"], (published, before)
    assert before[rel]["index_bytes"] > published[rel]["index_bytes"] + _FLOOR, (
        published, before,
    )

    own_before = _own_fingerprint(dsn)
    applied = index_reclaim.reclaim_index_space(
        dsn, deployment=_DEPLOYMENT, apply=True, min_index_bytes=_FLOOR
    )
    record_property("reclaim", json.dumps(applied["tables"]))
    assert rel in {t["relation"] for t in applied["tables"]}, applied
    assert applied["fingerprint"]["equal"] is True
    assert _own_fingerprint(dsn) == own_before
    after = _state(dsn)
    assert after[rel]["rows"] == before[rel]["rows"]
    assert after[rel]["index_bytes"] < before[rel]["index_bytes"]

    _apply(dsn, _node_facts(suffix, range(1, committed, 10), rev=3))
    assert _publish(dsn, identity)["status"] == "published"
    assert _state(dsn)[rel]["rows"] > before[rel]["rows"]


# --------------------------------------------------------------------------- #
# Refusals and the forced difference (cheap state: a rolled-back bulk insert)  #
# --------------------------------------------------------------------------- #


def _rolled_back_allocation(dsn: str, rows: int = 40_000) -> None:
    """The failure mechanism on its own: insert, roll back, VACUUM."""
    with psycopg.connect(dsn) as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO {_CHEAP} (target_kind, target_id, attrs_hash, attrs)
            SELECT 'node', 'doc:' || g, sha256(convert_to('v' || g, 'UTF8')),
                   jsonb_build_object('path', 'v' || g)
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
        "okg.attribute_versions_pkey",
        "okg.attribute_effective_intervals_edge_idx",
        "okg." + "x" * 60,
    }
    match = index_reclaim.is_reindex_artifact
    assert match("okg.attribute_versions_pkey_ccnew", existing)
    assert match("okg.attribute_versions_pkey_ccnew1", existing)
    assert match("okg.attribute_effective_intervals_edge_idx_ccold", existing)
    assert match("okg." + "x" * 57 + "_ccnew", existing)  # truncated base
    assert not match("okg.attribute_versions_3112_ccnew", existing)  # someone else's
    assert not match("okg.attribute_versions_new_idx", existing)
    assert not match("public.attribute_versions_pkey_ccnew", existing)  # other schema
    assert not match("okg.attribute_versions_pkey", existing)  # the original itself
    assert not match("okg.attribute_versions_p_ccnew", existing)  # short, not truncated


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
                f"INSERT INTO {_CHEAP} (target_kind, target_id, attrs_hash, attrs) "
                "VALUES ('node', %s, sha256(convert_to('same', 'UTF8')), "
                "'{\"path\": \"same\"}'::jsonb)",
                (target,),
            )
        with pytest.raises(psycopg.errors.UniqueViolation):
            cur.execute(
                "CREATE UNIQUE INDEX CONCURRENTLY attribute_versions_3112_ccnew "
                f"ON {_CHEAP} (attrs_hash)"
            )
    oids = _index_oids(dsn)
    with pytest.raises(SubstrateError) as blocked:
        index_reclaim.reclaim_index_space(dsn, deployment=_DEPLOYMENT, min_index_bytes=_FLOOR)
    assert blocked.value.data["reason"] == "dry_run_blocked"
    assert blocked.value.data["blocked_by"] == [
        "invalid_index:okg.attribute_versions_3112_ccnew"
    ], blocked.value.data
    with pytest.raises(SubstrateError) as refused:
        index_reclaim.reclaim_index_space(
            dsn, deployment=_DEPLOYMENT, apply=True, min_index_bytes=_FLOOR
        )
    assert refused.value.data["reason"] == "invalid_index_present"
    (bad,) = refused.value.data["invalid_indexes"]
    assert bad["drop_command"] == (
        'DROP INDEX CONCURRENTLY "okg"."attribute_versions_3112_ccnew";'
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
