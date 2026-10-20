"""``okg db prune-blobs --kind image``: delete image rows nothing references.

PACT first-class-images, task.first-class-images.cleanup (issue #3046).

The fixtures publish real generations through ``ChronosGraphStore`` (the
ingest render step is not used here; the values are written completed, as
ingest would leave them) and store blob rows through ``blob_store``. They
check:

* a dry run deletes nothing;
* ``--apply`` inside the grace window deletes nothing;
* past grace, only rows no retained generation references are deleted;
* a row referenced only by the older retained generation is kept;
* a row referenced only through an overlay in ``attribute_effective_attrs``
  is kept, and so is one only a substitute declarer's row holds;
* a ``touch`` between the scan and the delete keeps the row;
* after compaction retires the old generation, its exclusive row goes;
* the SQL reference paths agree with ``iter_image_values``;
* a grace window under one hour is refused (library and CLI), so a row an
  in-flight ingest just touched cannot be deleted;
* with no registered graph storage, ``--apply`` refuses and deletes nothing.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

import psycopg
import pytest
from psycopg.types.json import Jsonb

from okg.substrate.cli.main import main as cli_main
from okg.substrate.db import blob_store
from okg.substrate.db.blob_prune import (
    MIN_GRACE_SECONDS,
    BlobPruneGraceTooShort,
    BlobPruneNoGraph,
    apply_blob_prune,
    blob_kind_references,
    plan_blob_prune,
    plan_report,
    prune_blobs,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.history_compaction import (
    apply_history_compaction_plan,
    create_history_compaction_plan,
)
from okg.substrate.images import classify, iter_image_values
from okg.substrate.images.prune import ATTRS_PNG_ID_PATHS, VALUE_PNG_ID_PATHS
from okg.substrate.library.sources.base import NodeFact
from tests.pytest.catalog_fixture_builders import seed_catalog

PNG = b"\x89PNG\r\n\x1a\nnot-really-a-png"
DEPLOYMENT = "prune-blobs-test"


def _id(name: str) -> str:
    return hashlib.sha256(name.encode()).hexdigest()


def _png(path: str, name: str) -> dict[str, Any]:
    blob_id = _id(name)
    return {
        "okg_image": "file",
        "path": path,
        "png": {
            "id": blob_id,
            "sha256": blob_id,
            "mime_type": "image/png",
            "width": 1,
            "height": 1,
            "byte_count": len(PNG),
        },
    }


def _store(conn: psycopg.Connection, *names: str) -> None:
    for name in names:
        blob_store.put(
            conn, kind="image", id=_id(name), data=PNG,
            mime_type="image/png", meta={"width": 1, "height": 1},
        )


def _age(conn: psycopg.Connection, *, days: int = 2) -> None:
    conn.execute(
        "UPDATE okg.blobs SET last_used_at = now() - make_interval(days => %s)",
        (days,),
    )


def _remaining(conn: psycopg.Connection) -> set[str]:
    return {
        row[0] for row in conn.execute("SELECT id FROM okg.blobs").fetchall()
    }


def _publish(dsn: str, generations: list[list[NodeFact]]) -> list[str]:
    """Publish each fact list as one generation; return the checkpoint ids."""
    with psycopg.connect(dsn) as conn:
        catalog = seed_catalog(conn, subtypes=["figure"])
    store = ChronosGraphStore.connect(dsn)
    checkpoints = []
    try:
        for index, facts in enumerate(generations):
            store.apply_facts(facts)
            checkpoints.append(
                store.checkpoint_generation(
                    "100" if index == 0 else f"prune-{index}",
                    deployment_name=DEPLOYMENT,
                    catalog_version_id=catalog,
                    metadata={"target_generation_id": 100 + index},
                )
            )
    finally:
        store.close()
    return checkpoints


def _figure(node_id: str, attrs: dict[str, Any]) -> NodeFact:
    return NodeFact(node_id, "figure", attrs, {"id": node_id}, {"rev": 1})


def _overlay(conn: psycopg.Connection, value: Any, *, name: str) -> None:
    conn.execute(
        """
        INSERT INTO okg.attribute_effective_attrs
          (target_kind, target_id, generation_id, namespace, attr_key,
           observation_id, value, value_hash, producer)
        VALUES ('node', 'n:overlay', 'okg:test:gen:1', 'default', %s,
                %s, %s, 'h', 'test')
        """,
        (name, f"obs-{name}", Jsonb(value)),
    )


def _scanned(report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Scan reports by relation: the Chronos storage for the graph tables
    (their plain logical tables are empty once Chronos owns them), the table
    itself for the overlays."""
    return {
        s["relation"]: s
        for s in report["scanned"]
        if s["storage"] == "chronos_interval"
        or s["relation"].startswith("okg.attribute_")
    }


@pytest.fixture
def two_generations(clean_runtime: str) -> str:
    """Generation 1 references old+kept+listed; generation 2 drops old.

    Blob rows: ``old`` (only generation 1), ``kept`` (both), ``listed`` (a
    list element, both), ``overlay`` (only an overlay value), ``orphan`` (no
    reference), ``deep`` (only nested too deep to be an image), and
    ``failed`` (a failure row nothing references).
    """
    _publish(
        clean_runtime,
        [
            [
                _figure("n:a", {
                    "figure": _png("a.png", "kept"),
                    "panels": ["text", _png("p.png", "listed")],
                }),
                _figure("n:b", {"figure": _png("b.png", "old")}),
            ],
            [
                _figure("n:b", {
                    "figure": {"okg_image": "file", "path": "b2.png",
                               "error": "image_decode_failed"},
                    "nested": {"inner": _png("d.png", "deep")},
                }),
            ],
        ],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _store(conn, "old", "kept", "listed", "overlay", "orphan", "deep")
        blob_store.put_error(
            conn, kind="image", id=_id("failed"),
            error="image_decode_failed", meta={},
        )
        _overlay(conn, [_png("o.png", "overlay")], name="gallery")
    return clean_runtime


pytestmark = pytest.mark.postgres


def test_registry_covers_every_blob_kind() -> None:
    assert set(blob_kind_references()) == set(blob_store.BLOB_KINDS) == {"image"}


def test_dry_run_and_grace_delete_nothing_then_apply_deletes_only_unreferenced(
    two_generations: str,
) -> None:
    dsn = two_generations
    everything = {
        _id(n) for n in
        ("old", "kept", "listed", "overlay", "orphan", "deep", "failed")
    }

    dry = prune_blobs(dsn, kind="image")
    assert dry["dry_run"] is True
    assert dry["graph_registered"] is True
    assert dry["candidate_count"] == 0
    assert dry["kept"] == {"referenced": 4, "within_grace": 3}
    assert dry["referenced_ids"] == 4

    inside_grace = prune_blobs(dsn, kind="image", confirm=True)
    assert inside_grace["deleted_count"] == 0
    with psycopg.connect(dsn) as conn:
        assert _remaining(conn) == everything

    with psycopg.connect(dsn, autocommit=True) as conn:
        _age(conn)
    dry_past = prune_blobs(dsn, kind="image")
    unreferenced = {_id("orphan"), _id("deep"), _id("failed")}
    assert dry_past["candidate_count"] == 3
    assert set(dry_past["would_delete_sample"]) == unreferenced
    assert dry_past["candidate_bytes"] == 2 * len(PNG)
    with psycopg.connect(dsn) as conn:
        assert _remaining(conn) == everything

    applied = prune_blobs(dsn, kind="image", confirm=True)
    assert applied["dry_run"] is False
    assert applied["deleted_count"] == 3
    assert applied["deleted_bytes"] == 2 * len(PNG)
    assert set(applied["deleted_sample"]) == unreferenced
    assert applied["kept"] == {
        "referenced": 4, "within_grace": 0, "refreshed_since_scan": 0,
    }
    with psycopg.connect(dsn) as conn:
        # `old` is referenced only by the older retained generation;
        # `overlay` only through attribute_effective_attrs.
        assert _remaining(conn) == everything - unreferenced

    by_relation = _scanned(applied)
    assert by_relation["okg.graph_nodes"]["rows_scanned"] == 3
    assert by_relation["okg.attribute_effective_attrs"]["rows_scanned"] == 1
    assert applied["rows_scanned"] >= 4


def test_scan_batches_by_key_and_finds_the_same_references(
    two_generations: str,
) -> None:
    with psycopg.connect(two_generations) as conn:
        one = plan_blob_prune(conn, kind="image", scan_batch_rows=10_000)
        tiny = plan_blob_prune(conn, kind="image", scan_batch_rows=1)
    assert tiny.scan.referenced == one.scan.referenced == {
        _id("old"), _id("kept"), _id("listed"), _id("overlay"),
    }
    nodes = _scanned({"scanned": tiny.scan.sources})["okg.graph_nodes"]
    # Three node rows, one per statement; the empty fourth read ends the scan.
    assert nodes["rows_scanned"] == 3
    assert nodes["batches"] == 3


def test_a_touch_after_the_scan_keeps_the_row(two_generations: str) -> None:
    dsn = two_generations
    with psycopg.connect(dsn, autocommit=True) as conn:
        _age(conn)
    with psycopg.connect(dsn) as conn:
        plan = plan_blob_prune(conn, kind="image")
        assert set(plan.candidates) == {_id("orphan"), _id("deep"), _id("failed")}
        # An ingest reuses `orphan` between the scan and the delete: its cache
        # hit touches the row before it writes the value referencing it.
        assert blob_store.touch(conn, kind="image", id=_id("orphan")) is not None
        conn.commit()
        applied = apply_blob_prune(conn, plan, delete_batch_size=1)
        assert set(applied["deleted"]) == {_id("deep"), _id("failed")}
        assert applied["refreshed_since_scan"] == [_id("orphan")]
        assert _id("orphan") in _remaining(conn)
    report = plan_report(plan, dry_run=False, applied=applied)
    assert report["kept"]["refreshed_since_scan"] == 1
    assert report["deleted_count"] == 2


def test_a_locked_row_stops_the_delete_with_a_named_error(
    two_generations: str,
) -> None:
    dsn = two_generations
    with psycopg.connect(dsn, autocommit=True) as conn:
        _age(conn)
    with psycopg.connect(dsn) as holder, psycopg.connect(dsn) as conn:
        plan = plan_blob_prune(conn, kind="image")
        # An ingest's touch of `orphan` is still open.
        holder.execute(
            "SELECT 1 FROM okg.blobs WHERE id = %s FOR UPDATE", (_id("orphan"),)
        )
        with pytest.raises(SubstrateError) as raised:
            apply_blob_prune(conn, plan, lock_timeout="200ms")
        assert raised.value.data["reason"] == "blob_prune_lock_timeout"
        assert raised.value.data["deleted_count"] == 0
        holder.rollback()
        assert _remaining(conn) >= set(plan.candidates)


def test_a_row_only_a_substitute_declarer_holds_is_kept(
    clean_runtime: str,
) -> None:
    """A declarer's row becomes the graph row when the owner retracts, with
    no ingest run, so its image must survive while the declarer row does."""
    with psycopg.connect(clean_runtime) as conn:
        catalog = seed_catalog(conn, subtypes=["figure"])
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        for source, name in (("src-owner", "owner"), ("src-other", "declared")):
            store.apply_facts(
                [NodeFact(
                    "author:x", "figure", {"photo": _png(f"{name}.png", name)},
                    {"id": "x"}, {"rev": 1}, ownership_mode="substitute",
                )],
                source=source,
            )
        store.checkpoint_generation(
            "100", deployment_name=DEPLOYMENT, catalog_version_id=catalog,
            metadata={"target_generation_id": 100},
        )
    finally:
        store.close()
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _store(conn, "owner", "declared")
        _age(conn)
    report = prune_blobs(clean_runtime, kind="image", confirm=True)
    by_relation = _scanned(report)
    # The graph row holds the owner's image; only the declarer table also
    # holds the other source's.
    assert by_relation["okg.graph_nodes"]["ids_found"] == 1
    assert by_relation["okg.graph_node_declarers"]["ids_found"] == 2
    assert report["deleted_count"] == 0
    assert report["kept"]["referenced"] == 2
    with psycopg.connect(clean_runtime) as conn:
        assert _remaining(conn) == {_id("owner"), _id("declared")}


def test_compacting_the_old_generation_frees_its_exclusive_row(
    clean_runtime: str,
) -> None:
    checkpoints = _publish(
        clean_runtime,
        [
            [_figure("n:a", {"figure": _png("a.png", "retired")})],
            [_figure("n:a", {"figure": _png("a2.png", "current")})],
            [_figure("n:b", {"revision": 2})],
            [_figure("n:b", {"revision": 3})],
            [_figure("n:b", {"revision": 4})],
        ],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _store(conn, "retired", "current")
        _age(conn)
    before = prune_blobs(clean_runtime, kind="image")
    assert before["candidate_count"] == 0
    assert before["kept"]["referenced"] == 2

    policy = {
        "default": "latest-only",
        "audit_tail_generations": 2,
        "nodes": {"figure": "latest-only"},
        "edges": {},
    }
    with psycopg.connect(clean_runtime) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        plan = create_history_compaction_plan(
            conn, store=store, deployment_name=DEPLOYMENT, policy_raw=policy,
        )
        assert plan["retired_checkpoints"] == checkpoints[:-2]
        apply_history_compaction_plan(
            conn, store=store, plan_id=plan["plan_id"],
            confirm=plan["plan_id"], apply=True, policy_raw=policy,
        )

    after = prune_blobs(clean_runtime, kind="image", confirm=True)
    assert after["deleted_count"] == 1
    assert after["deleted_sample"] == [_id("retired")]
    with psycopg.connect(clean_runtime) as conn:
        assert _remaining(conn) == {_id("current")}


def test_without_registered_graph_storage_apply_refuses_and_deletes_nothing(
    clean_runtime: str,
) -> None:
    """A fresh environment clone purges the Chronos tables but keeps
    ``okg.blobs``: the scan sees no graph, so nothing reads as referenced.
    A dry run reports that; ``--apply`` refuses instead of deleting every
    preserved row."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        registered = conn.execute(
            "SELECT to_regclass('_chronos_branch_tables') IS NOT NULL "
            "AND EXISTS (SELECT 1 FROM _chronos_branch_tables "
            "            WHERE table_name = 'okg.graph_nodes')"
        ).fetchone()[0]
        if registered:
            conn.execute(
                "DELETE FROM _chronos_branch_tables "
                "WHERE table_name IN ('okg.graph_nodes', 'okg.graph_node_declarers')"
            )
        _store(conn, "young", "aged")
        conn.execute(
            "UPDATE okg.blobs SET last_used_at = now() - interval '2 days' "
            "WHERE id = %s",
            (_id("aged"),),
        )
    dry = prune_blobs(clean_runtime, kind="image")
    assert dry["graph_registered"] is False
    assert "no graph node storage" in dry["note"]
    assert dry["apply_refused"] == "blob_prune_no_graph"
    assert dry["would_delete_sample"] == [_id("aged")]
    assert dry["kept"]["within_grace"] == 1

    with pytest.raises(BlobPruneNoGraph) as raised:
        prune_blobs(clean_runtime, kind="image", confirm=True)
    assert raised.value.code == "blob_prune_no_graph"
    assert raised.value.data["candidate_count"] == 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert _remaining(conn) == {_id("young"), _id("aged")}
        plan = plan_blob_prune(conn, kind="image")
        # The library entry point refuses too, not only prune_blobs.
        with pytest.raises(BlobPruneNoGraph):
            apply_blob_prune(conn, plan)
        assert _remaining(conn) == {_id("young"), _id("aged")}


@pytest.mark.parametrize(
    "attrs",
    [
        {"a": _png("a.png", "top")},
        {"a": [1, "s", _png("l.png", "element"), {"okg_image": "file", "path": "e.png",
                                                  "error": "image_decode_failed"}]},
        {"a": [[_png("x.png", "list-in-list")]]},
        {"a": {"b": _png("x.png", "object-in-object")}},
        {"a": {**_png("x.png", "png-array"), "png": [_png("x.png", "png-array")["png"]]}},
        {"a": {"okg_image": "url", "url": "https://example.org/x.png",
               "png": _png("x.png", "url-kind")["png"]}},
        {"a": {"okg_image": "file", "path": "x.png"}, "b": 3, "c": None, "d": "okg_image"},
        {},
    ],
)
def test_sql_paths_match_iter_image_values(
    clean_runtime: str, attrs: dict[str, Any]
) -> None:
    """The scan's SQL/JSON paths find exactly the png ids the value module
    would read: an attr value or a list element, never deeper."""
    expected = {
        site.value["png"]["id"]
        for site in iter_image_values(attrs)
        if classify(site.value).state == "png"
    }
    with psycopg.connect(clean_runtime) as conn:
        found = {
            row[0]
            for path in ATTRS_PNG_ID_PATHS
            for row in conn.execute(
                "SELECT jsonb_path_query(%s::jsonb, %s::jsonpath) #>> '{}'",
                (json.dumps(attrs), path),
            ).fetchall()
        }
        for name, value in attrs.items():
            per_value = {
                row[0]
                for path in VALUE_PNG_ID_PATHS
                for row in conn.execute(
                    "SELECT jsonb_path_query(%s::jsonb, %s::jsonpath) #>> '{}'",
                    (json.dumps(value), path),
                ).fetchall()
            }
            assert per_value == {
                site.value["png"]["id"]
                for site in iter_image_values({name: value})
                if classify(site.value).state == "png"
            }
    assert found == expected


def test_refusals(clean_runtime: str) -> None:
    with pytest.raises(SubstrateError, match="unknown blob kind"):
        prune_blobs(clean_runtime, kind="video")
    for grace in (-1, 0, MIN_GRACE_SECONDS - 1):
        with pytest.raises(BlobPruneGraceTooShort) as raised:
            prune_blobs(clean_runtime, kind="image", grace_seconds=grace)
        assert raised.value.code == "blob_prune_grace_too_short"
        assert raised.value.data["argument"] == "grace_seconds"
        assert raised.value.data["minimum_seconds"] == 3600
        with psycopg.connect(clean_runtime) as conn, pytest.raises(
            BlobPruneGraceTooShort
        ):
            plan_blob_prune(conn, kind="image", grace_seconds=grace)
    assert MIN_GRACE_SECONDS == 3600
    assert prune_blobs(
        clean_runtime, kind="image", grace_seconds=MIN_GRACE_SECONDS
    )["grace_seconds"] == MIN_GRACE_SECONDS


def test_a_row_an_ingest_just_touched_survives_the_shortest_grace(
    two_generations: str,
) -> None:
    """The reviewer's case: an ingest touches a row it is about to
    reference, and cleanup runs before that run publishes. A zero grace
    window used to delete it; it is now refused, and the shortest accepted
    window keeps the row."""
    dsn = two_generations
    with psycopg.connect(dsn, autocommit=True) as conn:
        _age(conn)
        assert blob_store.touch(conn, kind="image", id=_id("orphan")) is not None
    with pytest.raises(BlobPruneGraceTooShort):
        prune_blobs(dsn, kind="image", grace_seconds=0, confirm=True)
    with psycopg.connect(dsn) as conn:
        assert _id("orphan") in _remaining(conn)
    applied = prune_blobs(
        dsn, kind="image", grace_seconds=MIN_GRACE_SECONDS, confirm=True,
    )
    assert set(applied["deleted_sample"]) == {_id("deep"), _id("failed")}
    assert applied["kept"]["within_grace"] == 1
    with psycopg.connect(dsn) as conn:
        assert _id("orphan") in _remaining(conn)


def _cli(argv: list[str], capsys: pytest.CaptureFixture[str]) -> tuple[int, Any]:
    code = cli_main(argv)
    out = capsys.readouterr().out
    return code, json.loads(out) if out.strip() else None


def test_cli_dry_runs_by_default_and_apply_needs_confirm(
    two_generations: str, capsys: pytest.CaptureFixture[str],
) -> None:
    dsn = two_generations
    with psycopg.connect(dsn, autocommit=True) as conn:
        _age(conn)
    code, payload = _cli(
        ["--json", "db", "prune-blobs", "--kind", "image", "--dsn", dsn], capsys,
    )
    assert code == 0
    data = payload.get("data", payload)
    assert data["dry_run"] is True and data["candidate_count"] == 3

    code, _ = _cli(
        ["--json", "db", "prune-blobs", "--kind", "image", "--dsn", dsn, "--apply"],
        capsys,
    )
    assert code != 0
    for grace in ("0", "3599"):
        code = cli_main(
            ["--json", "db", "prune-blobs", "--kind", "image", "--dsn", dsn,
             "--grace-seconds", grace, "--confirm", "--apply"],
        )
        captured = capsys.readouterr()
        assert code != 0
        error = json.loads(captured.err)["error"]
        assert error["code"] == "blob_prune_grace_too_short", error
        assert error["data"]["minimum_seconds"] == 3600
    with psycopg.connect(dsn) as conn:
        assert len(_remaining(conn)) == 7
    code, payload = _cli(
        ["--json", "db", "prune-blobs", "--kind", "image", "--dsn", dsn,
         "--grace-seconds", "3600", "--confirm", "--apply"],
        capsys,
    )
    assert code == 0
    data = payload.get("data", payload)
    assert data["deleted_count"] == 3
    with psycopg.connect(dsn) as conn:
        assert len(_remaining(conn)) == 4
