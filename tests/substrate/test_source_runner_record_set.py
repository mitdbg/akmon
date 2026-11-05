from __future__ import annotations

import hashlib
import json
import re
import shutil
from datetime import datetime, timedelta, timezone
from typing import Any
import uuid

import psycopg
import pytest

import okg.substrate.ingest.runner as runner_module
from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunContext, SourceRunner
from okg.substrate.ingest.change_identity import (
    build_effective_token,
    identity_fingerprint,
)
from okg.substrate.ingest.sanitize import value_contains_nul
from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceHealth,
    SourceRun,
)
from okg.substrate.library.sources.change_probe import (
    CURSOR_TOKEN_KEY,
    ProbeVerdict,
    compose_token,
)
from tests.substrate._db_helpers import seed_catalog_version


pytestmark = pytest.mark.postgres


def _allow_full_retraction(source: str):
    """`okg ingest --allow-full-retraction <source>` (issue #2818 backstop).

    A completed run whose record set is empty while the source still has
    live rows is refused without it; these tests pin the deletion itself.
    """
    from okg.substrate.ingest.full_retraction_guard import (
        FullRetractionOverride,
    )

    return FullRetractionOverride(
        source=source,
        requested_by="record-set-test",
        requested_at=datetime.now(timezone.utc),
    )



def _node(node_id: str, subtype: str = "concept") -> NodeFact:
    return NodeFact(
        node_id=node_id,
        subtype=subtype,
        attrs={"text": node_id},
        source_record_id={"id": node_id},
        source_revision={"rev": "stable"},
    )


def _edge(src: str, edge_type: str, dst: str) -> EdgeFact:
    return EdgeFact(
        src=src,
        dst=dst,
        edge_type=edge_type,
        source_record_id={"src": src, "edge_type": edge_type, "dst": dst},
        source_revision={"rev": "stable"},
    )


def _record(
    key: str,
    *,
    primary: str = "record:doc",
    descendants: tuple[str, ...] = (),
    edges: tuple[tuple[str, str, str], ...] = (),
) -> RecordSet:
    return RecordSet(records={
        key: RecordEmission(
            primary=RecordNode(primary, "concept"),
            descendants=[
                RecordNode(node_id, "concept")
                for node_id in descendants
            ],
            edges=[
                EdgeKey(src=src, edge_type=edge_type, dst=dst)
                for src, edge_type, dst in edges
            ],
        )
    })


class _RecordSetAdapter:
    name = "record-set"
    profile = "discovery_crawl"
    probe_short_circuit_safe = True

    def __init__(
        self,
        runs: list[tuple[list[NodeFact | EdgeFact], RecordSet | None]],
        *,
        next_cursor: dict[str, Any] | None = None,
        completed_scope: bool = True,
    ) -> None:
        self._runs = list(runs)
        self._next_cursor = next_cursor or {}
        self._completed_scope = completed_scope
        self.seen_cursors: list[dict[str, Any] | None] = []

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: dict[str, Any] | None = None,
        since_progress: dict[str, str] | None = None,
    ) -> SourceRun:
        del run_id, since_progress
        self.seen_cursors.append(cursor)
        facts, record_set = self._runs.pop(0)
        return SourceRun(
            facts=facts,
            completed_scope=self._completed_scope,
            next_cursor=dict(self._next_cursor),
            run_mode=mode,
            record_set=record_set,
        )


class _StreamFootprintAdapter:
    name = "stream-footprint"
    profile = "discovery_crawl"
    probe_short_circuit_safe = True

    def __init__(self, runs: list[dict[str, Any]]) -> None:
        self._runs = list(runs)
        self.run_calls = 0

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: dict[str, Any] | None = None,
        since_progress: dict[str, str] | None = None,
        sync_scope: dict[str, Any] | None = None,
    ) -> SourceRun:
        del run_id, cursor, since_progress, sync_scope
        self.run_calls += 1
        spec = self._runs.pop(0)
        return SourceRun(
            facts=spec.get("facts", []),
            completed_scope=spec.get("completed_scope", True),
            next_cursor=dict(spec.get("next_cursor", {})),
            applied_raw_token=spec.get("applied_raw_token"),
            applied_token_scope=spec.get(
                "applied_token_scope",
                "unavailable",
            ),
            applied_token_authority=spec.get(
                "applied_token_authority",
                "unavailable",
            ),
            run_mode=spec.get("run_mode", mode),
            record_set=spec.get("record_set"),
            health=spec.get("health"),
            asserts_empty_scope=spec.get("asserts_empty_scope", False),
        )


class _ScopedRecordSetAdapter:
    """A narrowed run that still carries exact record-set authority.

    Issue #2145 class guard. The runner used to discard the record set of
    every run the caller narrowed with a sync_scope, so no scoped run could
    retract anything. Both shapes a real adapter returns are covered:
    a bounded replacement slice (`record_set_replacement_keys` set,
    `completed_scope` False, as GitFilesSource returns) and a merged
    complete set (`completed_scope` True with no replacement keys, as
    CodeStructureSource returns once it folds the durable prior in).
    """

    name = "scoped-record-set"
    profile = "discovery_crawl"

    def __init__(self, runs: list[dict[str, Any]]) -> None:
        self._runs = list(runs)
        self.seen_scopes: list[dict[str, Any] | None] = []

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: dict[str, Any] | None = None,
        since_progress: dict[str, str] | None = None,
        sync_scope: dict[str, Any] | None = None,
    ) -> SourceRun:
        del run_id, cursor, since_progress
        self.seen_scopes.append(sync_scope)
        spec = self._runs.pop(0)
        return SourceRun(
            facts=spec.get("facts", []),
            completed_scope=spec.get("completed_scope", True),
            next_cursor={},
            run_mode=spec.get("run_mode", mode),
            record_set=spec.get("record_set"),
            record_set_replacement_keys=spec.get("replacement_keys"),
        )


def _two_record_prior() -> RecordSet:
    return RecordSet(records={
        **_record("a.md", primary="doc:a").records,
        **_record("b.md", primary="doc:b").records,
    })


def _three_record_prior() -> RecordSet:
    return RecordSet(records={
        **_record("a.md", primary="doc:a").records,
        **_record("b.md", primary="doc:b").records,
        **_record("c.md", primary="doc:c").records,
    })


def _seed_three_records(
    clean_runtime: str,
    source: str,
    second: dict[str, Any],
    *,
    executable_profile: str | None = None,
) -> tuple[SourceRunner, _ScopedRecordSetAdapter]:
    """Publish a/b/c, then hand back a runner primed with `second`."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    adapter = _ScopedRecordSetAdapter([
        {
            "facts": [_node("doc:a"), _node("doc:b"), _node("doc:c")],
            "completed_scope": True,
            "record_set": _three_record_prior(),
        },
        second,
    ])
    runner = (
        SourceRunner(
            clean_runtime,
            source_name=source,
            executable_profile=executable_profile,
            registry_name=source,
            ownership_id=source,
            registry_binding_fingerprint="sha256:scoped-record-set-guard",
        )
        if executable_profile is not None
        else SourceRunner(clean_runtime, source_name=source)
    )
    first = runner.run(adapter, mode="scope_complete")
    assert first.retractions_node == 0
    assert _live_source_nodes(clean_runtime, source) == [
        "doc:a", "doc:b", "doc:c",
    ]
    return runner, adapter


def _live_source_nodes(dsn: str, source: str) -> list[str]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(conn).checkout().query(
            """
            SELECT node_id
              FROM okg.graph_nodes
             WHERE source = :source
             ORDER BY node_id
            """,
            {"source": source},
        )
    return [str(row["node_id"]) for row in rows]


class _ReplacementSliceRecordSetAdapter:
    name = "replacement-slice-record-set"
    profile = "discovery_crawl"
    retain_existing_graph_on_record_set_bootstrap = False

    def __init__(
        self,
        runs: list[
            tuple[
                list[NodeFact | EdgeFact],
                RecordSet | None,
                set[str] | None,
            ]
        ],
    ) -> None:
        self._runs = list(runs)

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: dict[str, Any] | None = None,
        since_progress: dict[str, str] | None = None,
    ) -> SourceRun:
        del run_id, cursor, since_progress
        facts, record_set, replacement_keys = self._runs.pop(0)
        return SourceRun(
            facts=facts,
            completed_scope=True,
            next_cursor={},
            run_mode=mode,
            record_set=record_set,
            record_set_replacement_keys=replacement_keys,
        )


class _CursorAwareRecordSetAdapter:
    name = "cursor-aware-record-set"
    profile = "discovery_crawl"

    def __init__(
        self,
        *,
        facts_when_reset: list[NodeFact | EdgeFact],
        record_set: RecordSet,
    ) -> None:
        self.facts_when_reset = list(facts_when_reset)
        self.record_set = record_set
        self.seen_cursors: list[dict[str, Any] | None] = []

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: dict[str, Any] | None = None,
        since_progress: dict[str, str] | None = None,
    ) -> SourceRun:
        del run_id, since_progress
        self.seen_cursors.append(cursor)
        facts = [] if cursor is not None else list(self.facts_when_reset)
        return SourceRun(
            facts=facts,
            completed_scope=True,
            next_cursor={"cursor_seen": cursor is not None},
            run_mode=mode,
            record_set=self.record_set,
        )


class _ProgressAwareAdapter:
    name = "progress-aware"
    profile = "discovery_crawl"

    def __init__(self) -> None:
        self.seen_progress: dict[str, str] | None = None

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        since_progress: dict[str, str] | None = None,
    ) -> SourceRun:
        del run_id
        self.seen_progress = dict(since_progress or {})
        return SourceRun(
            facts=[],
            completed_scope=True,
            next_cursor={"mode": mode},
            run_mode=mode,
            record_set=None,
        )


class _UnchangedProbe:
    def __init__(self, token: dict[str, Any]) -> None:
        self._token = token

    def probe(
        self,
        *,
        cursor: dict[str, Any] | None,
        sync_scope: dict[str, Any] | None,
    ) -> ProbeVerdict:
        del cursor, sync_scope
        return ProbeVerdict(changed=False, token=self._token)


def test_record_set_retracts_disappeared_record(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    first_record = _record(
        "docs/a.md",
        primary="doc:a",
        descendants=("chunk:a:1", "chunk:a:2"),
        edges=(
            ("doc:a", "contains", "chunk:a:1"),
            ("doc:a", "contains", "chunk:a:2"),
        ),
    )
    first_facts = [
        _node("doc:a"),
        _node("chunk:a:1"),
        _node("chunk:a:2"),
        _edge("doc:a", "contains", "chunk:a:1"),
        _edge("doc:a", "contains", "chunk:a:2"),
    ]
    adapter = _RecordSetAdapter([
        (first_facts, first_record),
        ([], RecordSet()),
    ])

    runner = SourceRunner(
        clean_runtime,
        source_name="test.record-set",
        write_fact_log=True,
        full_retraction_override=_allow_full_retraction("test.record-set"),
    )
    first = runner.run(adapter, mode="scope_complete")
    assert first.retractions_node == 0
    assert first.retractions_edge == 0

    second = runner.run(adapter, mode="scope_complete")
    assert second.retractions_node == 3
    assert second.retractions_edge == 2
    assert second.next_cursor is not None
    assert second.next_cursor["record_set"]["records"] == {}

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT node_id
              FROM okg.node_facts
             WHERE source = 'test.record-set' AND op = 'R'
             ORDER BY node_id
            """
        )
        assert [row[0] for row in cur.fetchall()] == [
            "chunk:a:1",
            "chunk:a:2",
            "doc:a",
        ]
        cur.execute(
            """
            SELECT src, edge_type, dst
              FROM okg.edge_facts
             WHERE source = 'test.record-set' AND op = 'R'
             ORDER BY src, edge_type, dst
            """
        )
        assert cur.fetchall() == [
            ("doc:a", "contains", "chunk:a:1"),
            ("doc:a", "contains", "chunk:a:2"),
        ]


def test_large_record_set_authority_is_externalized_and_reloaded(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_SOURCE_RUNNER_INLINE_RECORD_SET_MAX_BYTES", "1")
    source = "test.record-set-external"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    record = _record(
        "docs/a.md",
        primary="doc:a",
        descendants=("chunk:a",),
        edges=(("doc:a", "contains", "chunk:a"),),
    )
    adapter = _RecordSetAdapter([
        ([
            _node("doc:a"),
            _node("chunk:a"),
            _edge("doc:a", "contains", "chunk:a"),
        ], record),
        ([], RecordSet()),
    ])
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
        full_retraction_override=_allow_full_retraction(source),
    )

    first = runner.run(adapter, mode="scope_complete")
    assert first.next_cursor is not None
    assert "record_set" not in first.next_cursor
    assert first.next_cursor["record_set_ref"]["source"] == source
    assert first.next_cursor["record_set_ref"]["storage"] == (
        "okg.source_record_set_blobs"
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT record_set->'records' ? 'docs/a.md'
              FROM okg.source_record_set_blobs
             WHERE source = %s
               AND sha256 = %s
            """,
            (source, first.next_cursor["record_set_ref"]["sha256"]),
        )
        assert cur.fetchone() == (True,)

    second = runner.run(adapter, mode="scope_complete")
    assert second.retractions_node == 2
    assert second.retractions_edge == 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_applied_rev ? 'record_set',
                   last_applied_rev ? 'record_set_ref'
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            (source,),
        )
        assert cur.fetchone() == (False, True)


def test_oversized_external_record_set_uses_pieces(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_SOURCE_RUNNER_INLINE_RECORD_SET_MAX_BYTES", "1")
    monkeypatch.setenv("OKG_SOURCE_RUNNER_RECORD_SET_JSONB_MAX_BYTES", "1")
    source = "test.record-set-text-blob"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    record = _record(
        "docs/a.md",
        primary="doc:a",
        descendants=("chunk:a",),
        edges=(("doc:a", "contains", "chunk:a"),),
    )
    adapter = _RecordSetAdapter([
        ([
            _node("doc:a"),
            _node("chunk:a"),
            _edge("doc:a", "contains", "chunk:a"),
        ], record),
        ([], RecordSet()),
    ])
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
        full_retraction_override=_allow_full_retraction(source),
    )

    first = runner.run(adapter, mode="scope_complete")
    assert first.next_cursor is not None
    record_set_ref = first.next_cursor["record_set_ref"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT b.record_set, b.record_set_text,
                   string_agg(p.piece, '' ORDER BY p.piece_no)
              FROM okg.source_record_set_blobs b
              JOIN okg.source_record_set_blob_pieces p
                ON p.write_id = b.piece_write_id
             WHERE b.source = %s
               AND b.sha256 = %s
             GROUP BY b.record_set, b.record_set_text
            """,
            (source, record_set_ref["sha256"]),
        )
        stored_record_set, stored_text, pieced_text = cur.fetchone()
    # Issue #3008: above the JSONB threshold the text lives in pieces, not in
    # one record_set_text value, and the header carries a marker older
    # readers refuse.
    assert stored_record_set == {
        "pieced_record_set": "okg.source_runner.record_set_pieces.v1",
    }
    assert stored_text is None
    assert json.loads(pieced_text)["records"]["docs/a.md"]["primary"]["node_id"] == "doc:a"

    second = runner.run(adapter, mode="scope_complete")
    assert second.retractions_node == 2
    assert second.retractions_edge == 1


def test_empty_record_set_replacement_keys_preserve_external_authority(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_SOURCE_RUNNER_INLINE_RECORD_SET_MAX_BYTES", "1")
    source = "test.record-set-empty-replacement"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    record = _record(
        "docs/a.md",
        primary="doc:a",
        descendants=("chunk:a",),
        edges=(("doc:a", "contains", "chunk:a"),),
    )
    adapter = _ReplacementSliceRecordSetAdapter([
        ([
            _node("doc:a"),
            _node("chunk:a"),
            _edge("doc:a", "contains", "chunk:a"),
        ], record, None),
        ([], RecordSet(), set()),
    ])
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
    )

    first = runner.run(adapter, mode="scope_complete")
    assert first.next_cursor is not None
    assert "record_set_ref" in first.next_cursor

    second = runner.run(adapter, mode="scope_complete")

    assert second.retractions_node == 0
    assert second.retractions_edge == 0
    assert second.next_cursor is not None
    assert "record_set" not in second.next_cursor
    assert "record_set_ref" in second.next_cursor

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT record_set->'records' ? 'docs/a.md',
                   record_set->'records'->'docs/a.md'->'primary'->>'node_id'
              FROM okg.source_record_set_blobs
             WHERE source = %s
               AND sha256 = %s
            """,
            (source, second.next_cursor["record_set_ref"]["sha256"]),
        )
        assert cur.fetchone() == (True, "doc:a")


def test_replacement_slice_can_adopt_retained_chronos_authority(
    clean_runtime: str,
) -> None:
    source = "test.record-set-retained-bootstrap"
    current = _record("current", primary="doc:current")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                _node("doc:old"),
                _node("chunk:old"),
                _edge("doc:old", "contains", "chunk:old"),
            ],
            source=source,
        )
        partial_prior = _record("partial", primary="doc:old")
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.source_watermarks
                  (source, last_applied_rev)
                VALUES (%s, %s::jsonb)
                """,
                (
                    source,
                    json.dumps({
                        "record_set": partial_prior.to_cursor(),
                    }),
                ),
            )

    adapter = _ReplacementSliceRecordSetAdapter([
        ([_node("doc:current")], current, {"current"}),
    ])
    adapter.retain_existing_graph_on_record_set_bootstrap = True
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
    )

    result = runner.run(adapter, mode="scope_complete")

    assert result.retractions_node == 0
    assert result.retractions_edge == 0
    assert result.next_cursor is not None
    retained = RecordSet.from_cursor(result.next_cursor["record_set"])
    nodes, edges = runner._record_set_identities(retained)
    assert nodes == {"doc:old", "chunk:old", "doc:current"}
    assert edges == {("doc:old", "contains", "chunk:old")}


# Issue #2838: a narrowed run may not retract records adopted from the
# graph. The retained-graph bootstrap keys the live-graph rows it adopts
# ``__chronos_retained__:node:<fingerprint>``. That spelling used to count
# as scope-independent, so a narrowed run could list such a key as a
# replacement (or drop it from a merged-prior claim) and retract the
# adopted record, although it lies under no scope the run was handed.
_RETAINED_KEY_SCOPE = {"scope_keys": ["b.md"]}


def _retained_node_key(node_id: str) -> str:
    return "__chronos_retained__:node:" + identity_fingerprint({
        "node_id": node_id,
        "subtype": "concept",
    })


def _watermark(dsn: str, source: str) -> Any:
    with psycopg.connect(dsn, autocommit=True) as conn:
        row = conn.execute(
            "SELECT last_applied_rev FROM okg.source_watermarks "
            "WHERE source = %s",
            (source,),
        ).fetchone()
    return None if row is None else row[0]


def _seed_unverified_legacy_source(
    dsn: str,
    source: str,
    runs: list[dict[str, Any]],
) -> tuple[SourceRunner, _ScopedRecordSetAdapter]:
    """Publish doc:b and doc:legacy with an UNVERIFIED record-set prior.

    The prior names only b.md, and carries no Chronos proof, which is the
    state the retained-graph bootstrap exists to migrate.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)
        ChronosGraphStore.from_connection(conn).apply_facts(
            [_node("doc:b"), _node("doc:legacy")],
            source=source,
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.source_watermarks (source, last_applied_rev)
                VALUES (%s, %s::jsonb)
                """,
                (
                    source,
                    json.dumps({
                        "record_set": _record(
                            "b.md", primary="doc:b",
                        ).to_cursor(),
                    }),
                ),
            )
    adapter = _ScopedRecordSetAdapter(runs)
    adapter.retain_existing_graph_on_record_set_bootstrap = True
    runner = SourceRunner(dsn, source_name=source, write_fact_log=True)
    assert _live_source_nodes(dsn, source) == ["doc:b", "doc:legacy"]
    return runner, adapter


def test_scoped_slice_listing_a_retained_key_is_refused(
    clean_runtime: str,
) -> None:
    """The issue's failure: must refuse on this branch, retracts on dev."""
    source = "test.record-set-scoped-retained-key"
    runner, adapter = _seed_unverified_legacy_source(clean_runtime, source, [
        {
            "facts": [],
            "completed_scope": False,
            "record_set": RecordSet(),
            "replacement_keys": {
                "b.md",
                _retained_node_key("doc:legacy"),
            },
        },
    ])
    cursor_before = _watermark(clean_runtime, source)

    with pytest.raises(SubstrateError) as excinfo:
        runner.run(adapter, mode="scope_complete", sync_scope=_RETAINED_KEY_SCOPE)

    data = excinfo.value.data
    assert data["reason_code"] == "scoped_replacement_retained_key_forbidden"
    assert data["reserved_replacement_keys"] == [
        _retained_node_key("doc:legacy"),
    ]
    assert _live_source_nodes(clean_runtime, source) == [
        "doc:b", "doc:legacy",
    ]
    assert _watermark(clean_runtime, source) == cursor_before
    assert runner.tail_failure_diagnostic["phase"] == (
        "record_set.scoped_replacement_reserved"
    )
    assert runner.tail_failure_diagnostic["status"] == "failed"


def test_scoped_slice_keeps_runner_generated_retained_keys(
    clean_runtime: str,
) -> None:
    """The runner's own bootstrap and a real synthetic key still work.

    The same narrowed run declaring only its own keys -- b.md plus the
    GitFilesSource-style ``__branch__:main`` -- succeeds, the runner adopts
    doc:legacy under a retained key it generated in this run, and that
    record survives into the next cursor.
    """
    source = "test.record-set-scoped-retained-legit"
    runner, adapter = _seed_unverified_legacy_source(clean_runtime, source, [
        {
            "facts": [],
            "completed_scope": False,
            "record_set": RecordSet(),
            "replacement_keys": {"b.md", "__branch__:main"},
        },
    ])

    result = runner.run(adapter, mode="scope_complete", sync_scope=_RETAINED_KEY_SCOPE)

    assert "doc:legacy" in _live_source_nodes(clean_runtime, source)
    assert result.next_cursor is not None
    carried = RecordSet.from_cursor(result.next_cursor["record_set"])
    assert _retained_node_key("doc:legacy") in carried.records


def test_scoped_merged_claim_dropping_a_retained_record_is_refused(
    clean_runtime: str,
) -> None:
    """The sibling door: a merged-prior claim may not drop a retained key.

    Once the bootstrap has written retained records into the prior, a later
    narrowed run claiming a merged set that omits them used to pass the
    coverage check (the key read as scope-independent) and retract them.
    """
    source = "test.record-set-merged-drops-retained"
    runner, adapter = _seed_unverified_legacy_source(clean_runtime, source, [
        {
            "facts": [_node("doc:b")],
            "completed_scope": False,
            "record_set": _record("b.md", primary="doc:b"),
            "replacement_keys": {"b.md"},
        },
        {
            "facts": [_node("doc:b")],
            "completed_scope": True,
            "record_set": _record("b.md", primary="doc:b"),
        },
    ])
    first = runner.run(adapter, mode="scope_complete", sync_scope=_RETAINED_KEY_SCOPE)
    assert first.next_cursor is not None
    assert _retained_node_key("doc:legacy") in RecordSet.from_cursor(
        first.next_cursor["record_set"],
    ).records
    cursor_before = _watermark(clean_runtime, source)

    with pytest.raises(SubstrateError) as excinfo:
        runner.run(adapter, mode="scope_complete", sync_scope=_RETAINED_KEY_SCOPE)

    data = excinfo.value.data
    assert data["reason_code"] == "scoped_merged_record_set_incomplete"
    assert _retained_node_key("doc:legacy") in (
        data["uncovered_prior_record_keys"]
    )
    assert _live_source_nodes(clean_runtime, source) == [
        "doc:b", "doc:legacy",
    ]
    assert _watermark(clean_runtime, source) == cursor_before


def test_scoped_slice_with_one_shot_replacement_keys_is_still_bounded(
    clean_runtime: str,
) -> None:
    """Issue #2838 review: replacement keys are read exactly once.

    `record_set_replacement_keys` is typed Iterable. The reserved-key check
    became its first reader, so a generator was used up there and the scope
    bound saw no keys at all: a run narrowed to b.md that also declared a.md
    passed. The runner now freezes the keys before any check.
    """
    source = "test.record-set-slice-one-shot-keys"
    runner, adapter = _seed_three_records(clean_runtime, source, {
        "facts": [],
        "completed_scope": False,
        "record_set": RecordSet(),
        "replacement_keys": (key for key in ("b.md", "a.md")),
    })

    with pytest.raises(SubstrateError) as excinfo:
        runner.run(
            adapter,
            mode="scope_complete",
            sync_scope={"scope_keys": ["b.md"]},
        )

    assert excinfo.value.data["reason_code"] == (
        "scoped_replacement_keys_outside_scope"
    )
    assert excinfo.value.data["out_of_scope_replacement_keys"] == ["a.md"]
    assert _live_source_nodes(clean_runtime, source) == [
        "doc:a", "doc:b", "doc:c",
    ]


def test_large_cursor_fields_are_externalized_and_reloaded(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_SOURCE_RUNNER_INLINE_CURSOR_FIELD_MAX_BYTES", "100")
    source = "test.cursor-field-external"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    large_field = {
        "doc:a": {"fingerprint": "a" * 200},
        "doc:b": {"fingerprint": "b" * 200},
    }
    second_large_field = {"session": {"fingerprint": "c" * 200}}
    third_large_field = {"cursor": {"fingerprint": "d" * 200}}
    adapter = _RecordSetAdapter(
        [
            ([], None),
            ([], None),
        ],
        next_cursor={
            "fact_hashes": large_field,
            "session_state": second_large_field,
            "cursor_state": third_large_field,
            "small": "ok",
        },
    )
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
    )

    first = runner.run(adapter, mode="scope_complete")

    assert first.next_cursor is not None
    assert first.next_cursor["small"] == "ok"
    assert first.next_cursor["fact_hashes"]["schema"] == (
        "okg.source_runner.cursor_blob_ref.v1"
    )
    assert first.next_cursor["fact_hashes"]["storage"] == (
        "okg.source_cursor_blobs"
    )
    assert first.next_cursor["cursor_blob_summary"]["field_count"] == 3

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_applied_rev->'fact_hashes'->>'storage',
                   last_applied_rev ? 'cursor_blob_summary',
                   pg_column_size(last_applied_rev)
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            (source,),
        )
        storage, has_summary, watermark_bytes = cur.fetchone()
        cur.execute(
            """
            SELECT value, updated_at
              FROM okg.source_cursor_blobs
             WHERE source = %s
               AND cursor_key = 'fact_hashes'
               AND sha256 = %s
            """,
            (source, first.next_cursor["fact_hashes"]["sha256"]),
        )
        blob_value, blob_updated_at = cur.fetchone()

    assert storage == "okg.source_cursor_blobs"
    assert has_summary is True
    assert watermark_bytes < 3_000
    assert blob_value == large_field

    selective = runner._hydrate_cursor_blobs(
        first.next_cursor,
        keys={"fact_hashes"},
    )
    assert selective["fact_hashes"] == large_field
    assert selective["session_state"]["storage"] == "okg.source_cursor_blobs"
    assert selective["cursor_state"]["storage"] == "okg.source_cursor_blobs"

    dangling = dict(first.next_cursor)
    dangling["session_state"] = {
        **dangling["session_state"],
        "sha256": "missing",
    }
    with pytest.raises(SubstrateError, match="cursor blob ref is missing"):
        runner._hydrate_cursor_blobs(dangling, keys={"fact_hashes"})

    second = runner.run(adapter, mode="scope_complete")

    assert adapter.seen_cursors[1]["fact_hashes"] == large_field
    assert adapter.seen_cursors[1]["session_state"] == second_large_field
    assert adapter.seen_cursors[1]["cursor_state"] == third_large_field
    assert adapter.seen_cursors[1]["small"] == "ok"
    assert "cursor_blob_summary" not in adapter.seen_cursors[1]
    assert second.next_cursor["fact_hashes"]["storage"] == (
        "okg.source_cursor_blobs"
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*), max(updated_at)
              FROM okg.source_cursor_blobs
             WHERE source = %s
               AND cursor_key = 'fact_hashes'
               AND sha256 = %s
            """,
            (source, first.next_cursor["fact_hashes"]["sha256"]),
        )
        blob_count, last_blob_updated_at = cur.fetchone()
    assert blob_count == 1
    # PACT record-set-blob-autoprune: reusing the blob refreshes updated_at
    # (it was left unchanged before), so the automatic prune's grace window
    # counts from the latest run that wrote it.
    assert last_blob_updated_at > blob_updated_at
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.source_cursor_blobs WHERE source = %s",
            (source,),
        )
        assert cur.fetchone()[0] == 3


def test_record_set_retracts_removed_descendants_and_edges(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    first_record = _record(
        "docs/a.md",
        primary="doc:a",
        descendants=("chunk:a:1", "chunk:a:2"),
        edges=(
            ("doc:a", "contains", "chunk:a:1"),
            ("doc:a", "contains", "chunk:a:2"),
        ),
    )
    second_record = _record(
        "docs/a.md",
        primary="doc:a",
        descendants=("chunk:a:2", "chunk:a:3"),
        edges=(
            ("doc:a", "contains", "chunk:a:2"),
            ("doc:a", "contains", "chunk:a:3"),
        ),
    )
    adapter = _RecordSetAdapter([
        ([
            _node("doc:a"),
            _node("chunk:a:1"),
            _node("chunk:a:2"),
            _edge("doc:a", "contains", "chunk:a:1"),
            _edge("doc:a", "contains", "chunk:a:2"),
        ], first_record),
        ([
            _node("doc:a"),
            _node("chunk:a:2"),
            _node("chunk:a:3"),
            _edge("doc:a", "contains", "chunk:a:2"),
            _edge("doc:a", "contains", "chunk:a:3"),
        ], second_record),
    ])

    runner = SourceRunner(
        clean_runtime,
        source_name="test.record-set-diff",
        write_fact_log=True,
    )
    runner.run(adapter, mode="scope_complete")
    second = runner.run(adapter, mode="scope_complete")

    assert second.retractions_node == 1
    assert second.retractions_edge == 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT node_id
              FROM okg.node_facts
             WHERE source = 'test.record-set-diff' AND op = 'R'
            """
        )
        assert cur.fetchall() == [("chunk:a:1",)]
        cur.execute(
            """
            SELECT src, edge_type, dst
              FROM okg.edge_facts
             WHERE source = 'test.record-set-diff' AND op = 'R'
            """
        )
        assert cur.fetchall() == [
            ("doc:a", "contains", "chunk:a:1"),
        ]


def test_record_set_keeps_nodes_still_owned_by_another_record(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    first = RecordSet(records={
        "docs/a.md": RecordEmission(
            primary=RecordNode("doc:a", "concept"),
            descendants=[RecordNode("chunk:shared", "concept")],
            edges=[
                EdgeKey(
                    src="doc:a",
                    edge_type="contains",
                    dst="chunk:shared",
                ),
            ],
        ),
        "docs/b.md": RecordEmission(
            primary=RecordNode("doc:b", "concept"),
            descendants=[RecordNode("chunk:shared", "concept")],
            edges=[
                EdgeKey(
                    src="doc:b",
                    edge_type="contains",
                    dst="chunk:shared",
                ),
            ],
        ),
    })
    second = RecordSet(records={
        "docs/b.md": RecordEmission(
            primary=RecordNode("doc:b", "concept"),
            descendants=[RecordNode("chunk:shared", "concept")],
            edges=[
                EdgeKey(
                    src="doc:b",
                    edge_type="contains",
                    dst="chunk:shared",
                ),
            ],
        )
    })
    adapter = _RecordSetAdapter([
        ([
            _node("doc:a"),
            _node("doc:b"),
            _node("chunk:shared"),
            _edge("doc:a", "contains", "chunk:shared"),
            _edge("doc:b", "contains", "chunk:shared"),
        ], first),
        ([
            _node("doc:b"),
            _node("chunk:shared"),
            _edge("doc:b", "contains", "chunk:shared"),
        ], second),
    ])

    runner = SourceRunner(
        clean_runtime,
        source_name="test.record-set-shared",
        write_fact_log=True,
    )
    runner.run(adapter, mode="scope_complete")
    result = runner.run(adapter, mode="scope_complete")

    assert result.retractions_node == 1
    assert result.retractions_edge == 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT node_id
              FROM okg.node_facts
             WHERE source = 'test.record-set-shared' AND op = 'R'
            """
        )
        assert cur.fetchall() == [("doc:a",)]
        cur.execute(
            """
            SELECT src, edge_type, dst
              FROM okg.edge_facts
             WHERE source = 'test.record-set-shared' AND op = 'R'
            """
        )
        assert cur.fetchall() == [
            ("doc:a", "contains", "chunk:shared"),
        ]


def test_record_set_cursor_upgrade_and_opt_out(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.source_watermarks
                  (source, last_applied_rev)
                VALUES ('test.record-set-upgrade', '{"legacy": true}'::jsonb)
                """
            )

    record = _record("docs/a.md", primary="doc:a")
    adapter = _RecordSetAdapter(
        [([_node("doc:a")], record)],
        next_cursor={"legacy": True},
    )
    result = SourceRunner(
        clean_runtime,
        source_name="test.record-set-upgrade",
        write_fact_log=True,
    ).run(adapter, mode="scope_complete")
    assert adapter.seen_cursors == [{"legacy": True}]
    assert result.retractions_node == 0
    assert result.next_cursor is not None
    assert result.next_cursor["legacy"] is True
    assert "record_set" in result.next_cursor

    opt_out = _RecordSetAdapter(
        [([_node("plain:a")], None)],
        next_cursor={"plain": True},
    )
    opt_out_result = SourceRunner(
        clean_runtime,
        source_name="test.record-set-opt-out",
        write_fact_log=True,
    ).run(opt_out, mode="scope_complete")
    assert opt_out_result.next_cursor == {"plain": True}


def test_source_runner_writes_last_run_progress(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    adapter = _RecordSetAdapter([
        ([
            _node("doc:a"),
            _edge("doc:a", "contains", "chunk:a"),
        ], _record(
            "docs/a.md",
            primary="doc:a",
            edges=(("doc:a", "contains", "chunk:a"),),
        )),
    ])
    result = SourceRunner(
        clean_runtime,
        source_name="test.record-set-progress",
        deployment_name="test-source-runner",
        write_fact_log=True,
    ).run(adapter, mode="scope_complete")

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            SELECT key, value
              FROM okg.source_progress
             WHERE deployment = 'test-source-runner'
               AND source_name = 'test.record-set-progress'
               AND key IN ('sync.last_run', %s)
            """,
            (f"sync.run.{result.run_id}",),
        )
        progress = {key: json.loads(value) for key, value in cur.fetchall()}
    payload = progress["sync.last_run"]
    run_payload = progress[f"sync.run.{result.run_id}"]
    assert payload["schema"] == "okg.source_runner.last_run.v1"
    assert payload["run_id"] == result.run_id
    assert run_payload == payload
    assert payload["source_name"] == "test.record-set-progress"
    assert payload["mode"] == "scope_complete"
    assert payload["nodes_appended"] == 1
    assert payload["edges_appended"] == 1
    assert payload["nodes_retracted"] == 0
    assert payload["edges_retracted"] == 0
    assert payload["completed_scope"] is True
    assert payload["source_runner_ms"] >= 0


def test_source_runner_hides_reserved_sync_progress_from_adapters(
    clean_runtime: str,
) -> None:
    source = "test.progress-aware"
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        seed_catalog_version(conn)
        cur.executemany(
            """
            INSERT INTO okg.source_progress
              (deployment, source_name, key, value)
            VALUES ('test-source-runner', %s, %s, %s)
            """,
            [
                (source, "record:doc:a", "fingerprint-a"),
                (source, "sync.last_run", "{}"),
                (source, f"sync.run.{uuid.uuid4()}", "{}"),
                (source, f"sync.stage_timings.run.{uuid.uuid4()}", "{}"),
            ],
        )

    adapter = _ProgressAwareAdapter()
    SourceRunner(
        clean_runtime,
        source_name=source,
        deployment_name="test-source-runner",
        write_fact_log=True,
    ).run(adapter, mode="scope_complete")

    assert adapter.seen_progress == {"record:doc:a": "fingerprint-a"}


def test_last_run_source_runner_ms_uses_runner_wall_time(
    clean_runtime: str,
) -> None:
    observed_at = datetime.now(timezone.utc) - timedelta(days=1)
    run_id = str(uuid.uuid4())
    adapter = _RecordSetAdapter([
        ([_node("doc:a")], _record("doc:a", primary="doc:a")),
    ])
    result = SourceRunner(
        clean_runtime,
        source_name="test.record-set-timing",
        deployment_name="test-source-runner",
        write_fact_log=True,
    ).run(
        adapter,
        mode="scope_complete",
        run_context=SourceRunContext(
            run_id=run_id,
            observed_at=observed_at,
        ),
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            SELECT key, value
              FROM okg.source_progress
             WHERE deployment = 'test-source-runner'
               AND source_name = 'test.record-set-timing'
               AND key IN ('sync.last_run', %s)
            """,
            (f"sync.run.{run_id}",),
        )
        progress = {key: json.loads(value) for key, value in cur.fetchall()}
    payload = progress["sync.last_run"]

    assert payload["run_id"] == run_id
    assert progress[f"sync.run.{run_id}"] == payload
    assert payload["observed_at"] == observed_at.isoformat()
    assert payload["admission_observed_at"] == observed_at.isoformat()
    assert payload["started_at"] != observed_at.isoformat()
    assert payload["source_runner_ms"] < 60_000
    assert result.started_at > observed_at
    result_payload = result.as_dict()
    assert result_payload["timings_ms"]["load_run_prelude"] >= 0
    assert result_payload["timings_ms"]["adapter_run"] >= 0
    assert result_payload["timings_ms"]["total"] >= 0


def test_record_set_first_run_retracts_stray_legacy_facts(
    clean_runtime: str,
) -> None:
    source = "test.record-set-stray"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                _node("doc:old"),
                _node("chunk:old"),
                _edge("doc:old", "contains", "chunk:old"),
            ],
            source=source,
        )

    adapter = _RecordSetAdapter([
        ([_node("doc:a")], _record("docs/a.md", primary="doc:a")),
    ])
    result = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
    ).run(
        adapter,
        mode="scope_complete",
    )

    assert result.retractions_node == 2
    assert result.retractions_edge == 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT node_id
              FROM okg.node_facts
             WHERE source = %s AND op = 'R'
            """,
            (source,),
        )
        assert cur.fetchall() == [("chunk:old",), ("doc:old",)]
        cur.execute(
            """
            SELECT src, edge_type, dst
              FROM okg.edge_facts
             WHERE source = %s AND op = 'R'
            """,
            (source,),
        )
        assert cur.fetchall() == [("doc:old", "contains", "chunk:old")]


def test_record_set_verified_cursor_skips_full_repair_sweep(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = "test.record-set-verified-skip"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    record = _record("docs/a.md", primary="doc:a")
    adapter = _RecordSetAdapter([
        ([_node("doc:a")], record),
        ([], record),
    ])
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
    )

    first = runner.run(adapter, mode="scope_complete")
    assert first.next_cursor is not None
    assert first.next_cursor["record_set_chronos_proof"]["ok"] is True
    assert first.next_cursor["record_set_summary"] == {
        "schema": "okg.source_runner.record_set_summary.v1",
        "admission_sanitization": (
            "okg.source_runner.record_set_sanitization.v1"
        ),
        "source_name": source,
        "graph_branch": "default",
        "record_count": 1,
        "node_count": 1,
        "edge_count": 0,
        "chronos_graph_verified": True,
    }

    def _fail_full_repair(*args, **kwargs):
        raise AssertionError("verified record_set run used full graph sweep")

    monkeypatch.setattr(
        runner,
        "_record_set_stray_retractions",
        _fail_full_repair,
    )
    monkeypatch.setattr(
        runner,
        "_verify_record_set_chronos_coverage",
        _fail_full_repair,
    )
    second = runner.run(adapter, mode="scope_complete")

    assert second.retractions_node == 0
    assert second.retractions_edge == 0
    assert second.next_cursor is not None
    assert second.next_cursor["record_set_chronos_proof"]["ok"] is True
    assert second.next_cursor["record_set_summary"]["node_count"] == 1
    assert second.next_cursor["record_set_summary"]["edge_count"] == 0


def test_incomplete_run_preserves_prior_record_set_summary(
    clean_runtime: str,
) -> None:
    source = "test.record-set-incomplete-preserves-summary"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    record = _record("docs/a.md", primary="doc:a")
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
    )
    first = runner.run(
        _RecordSetAdapter([([_node("doc:a")], record)]),
        mode="scope_complete",
    )
    assert first.next_cursor is not None
    summary = first.next_cursor["record_set_summary"]

    second = runner.run(
        _RecordSetAdapter(
            [([], None)],
            next_cursor={"page": "2"},
            completed_scope=False,
        ),
        mode="scope_incomplete",
    )

    assert second.completed_scope is False
    assert second.next_cursor is not None
    assert second.next_cursor["page"] == "2"
    assert second.next_cursor["record_set_summary"] == summary
    assert second.next_cursor["record_set"] == first.next_cursor["record_set"]

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT last_applied_rev
              FROM okg.source_watermarks
             WHERE source = %s
            """,
            (source,),
        )
        stored = cur.fetchone()[0]

    assert stored["page"] == "2"
    assert stored["record_set_summary"] == summary
    assert stored["record_set"] == first.next_cursor["record_set"]


@pytest.mark.parametrize("reset_cursor", [False, True])
def test_completed_scope_deletion_stream_retracts_missing_record(
    clean_runtime: str,
    reset_cursor: bool,
) -> None:
    source = f"test.completed-scope-deletion-stream-{reset_cursor}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    adapter = _StreamFootprintAdapter([
        {
            "facts": [
                _node("doc:a"),
                _node("doc:b"),
                _edge("doc:a", "links_to", "doc:b"),
            ],
        },
        {"facts": [_node("doc:a")]},
    ])
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
        deletion_semantics="missing_from_completed_scope",
    )

    first = runner.run(adapter, mode="scope_complete")
    second = runner.run(
        adapter,
        mode="reconcile",
        reset_cursor=reset_cursor,
    )

    assert first.next_cursor is not None
    assert first.next_cursor["record_set_summary"]["node_count"] == 2
    assert first.next_cursor["record_set_summary"]["edge_count"] == 1
    assert second.retractions_node == 1
    assert second.retractions_edge == 1
    assert second.next_cursor is not None
    assert second.next_cursor["record_set_summary"]["node_count"] == 1
    assert second.next_cursor["record_set_summary"]["edge_count"] == 0
    assert _live_source_nodes(clean_runtime, source) == ["doc:a"]


@pytest.mark.parametrize(
    (
        "second_mode",
        "reported_mode",
        "completed_scope",
        "health",
        "sync_scope",
    ),
    [
        ("scope_incomplete", "scope_incomplete", False, None, None),
        ("scope_complete", "scope_incomplete", True, None, None),
        (
            "scope_complete",
            "scope_complete",
            True,
            SourceHealth(status="endpoint_failed", reason="fixture degraded"),
            None,
        ),
        (
            "scope_complete",
            "scope_complete",
            True,
            None,
            {"scope_keys": ["doc:a"]},
        ),
    ],
)
def test_completed_scope_deletion_unsafe_stream_preserves_prior_authority(
    clean_runtime: str,
    second_mode: str,
    reported_mode: str,
    completed_scope: bool,
    health: SourceHealth | None,
    sync_scope: dict[str, Any] | None,
) -> None:
    source = "test.completed-scope-deletion-unsafe-" + second_mode + str(
        bool(health or sync_scope)
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    adapter = _StreamFootprintAdapter([
        {"facts": [_node("doc:a"), _node("doc:b")]},
        {
            "facts": [_node("doc:a")],
            "completed_scope": completed_scope,
            "run_mode": reported_mode,
            "health": health,
        },
    ])
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
        deletion_semantics="missing_from_completed_scope",
    )
    first = runner.run(adapter, mode="scope_complete")
    second = runner.run(
        adapter,
        mode=second_mode,
        sync_scope=sync_scope,
    )

    assert first.next_cursor is not None
    assert second.retractions_node == 0
    assert second.next_cursor is not None
    assert second.next_cursor["record_set"] == first.next_cursor["record_set"]
    assert _live_source_nodes(clean_runtime, source) == ["doc:a", "doc:b"]


def test_completed_scope_deletion_acquisition_health_blocks_tail_authority(
    clean_runtime: str,
) -> None:
    source = "test.completed-scope-deletion-acquisition-health"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    class _AcquisitionHealthAdapter(_StreamFootprintAdapter):
        def __init__(self) -> None:
            super().__init__([
                {"facts": [_node("doc:a"), _node("doc:b")]},
                {"facts": [_node("doc:a")]},
            ])
            self._health = [
                None,
                SourceHealth(
                    status="acquire_stale",
                    reason="fixture checkout is stale",
                ),
            ]

        def ensure_checkout(self) -> SourceHealth | None:
            return self._health.pop(0)

    adapter = _AcquisitionHealthAdapter()
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
        deletion_semantics="missing_from_completed_scope",
    )

    first = runner.run(adapter, mode="scope_complete")
    second = runner.run(adapter, mode="scope_complete")

    assert first.next_cursor is not None
    assert second.retractions_node == 0
    assert second.next_cursor is not None
    assert second.next_cursor["record_set"] == first.next_cursor["record_set"]
    assert second.source_health is not None
    assert second.source_health.status == "acquire_stale"
    assert _live_source_nodes(clean_runtime, source) == ["doc:a", "doc:b"]


def test_completed_scope_deletion_no_change_preserves_prior_authority(
    clean_runtime: str,
) -> None:
    source = "test.completed-scope-deletion-no-change"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    token = compose_token(
        content={
            "kind": "content_hash",
            "value": hashlib.sha256(b"stable").hexdigest(),
        },
        config=hashlib.sha256(b"config").hexdigest(),
        emit_version=hashlib.sha256(b"emit").hexdigest(),
    )
    adapter = _StreamFootprintAdapter([{
        "facts": [_node("doc:a"), _node("doc:b")],
        "applied_raw_token": token,
        "applied_token_scope": "global",
        "applied_token_authority": "exact",
    }])
    adapter.change_probe = _UnchangedProbe(token)
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
        deletion_semantics="missing_from_completed_scope",
    )

    first = runner.run(adapter, mode="scope_complete")
    second = runner.run(adapter, mode="scope_complete")

    assert adapter.run_calls == 1
    assert first.next_cursor is not None
    assert second.mode == "no_change"
    assert second.retractions_node == 0
    assert second.next_cursor is not None
    assert second.next_cursor["record_set"] == first.next_cursor["record_set"]
    assert _live_source_nodes(clean_runtime, source) == ["doc:a", "doc:b"]


def test_completed_scope_deletion_reconcile_allows_completed_explicit_retraction(
    clean_runtime: str,
) -> None:
    source = "test.completed-scope-deletion-reconcile-explicit"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    retracted_b = NodeFact(
        node_id="doc:b",
        subtype="concept",
        attrs={},
        source_record_id={"id": "doc:b"},
        source_revision={"rev": "removed"},
        op="R",
    )
    adapter = _StreamFootprintAdapter([
        {"facts": [_node("doc:a"), _node("doc:b")]},
        {"facts": [_node("doc:a"), retracted_b]},
    ])
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
        deletion_semantics="missing_from_completed_scope",
    )

    runner.run(adapter, mode="scope_complete")
    second = runner.run(adapter, mode="reconcile")

    assert second.retractions_node == 1
    assert _live_source_nodes(clean_runtime, source) == ["doc:a"]


@pytest.mark.parametrize(
    ("reported_mode", "sync_scope", "health"),
    [
        ("scope_incomplete", None, None),
        ("reconcile", {"scope_keys": ["doc:b"]}, None),
        (
            "reconcile",
            None,
            SourceHealth(
                status="endpoint_failed",
                reason="fixture degraded",
            ),
        ),
    ],
)
def test_completed_scope_deletion_reconcile_refuses_unsafe_explicit_retraction(
    clean_runtime: str,
    reported_mode: str,
    sync_scope: dict[str, Any] | None,
    health: SourceHealth | None,
) -> None:
    source = "test.completed-scope-deletion-reconcile-refused-" + str(
        bool(sync_scope or health),
    ) + reported_mode
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    retracted_b = NodeFact(
        node_id="doc:b",
        subtype="concept",
        attrs={},
        source_record_id={"id": "doc:b"},
        source_revision={"rev": "removed"},
        op="R",
    )
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
        deletion_semantics="missing_from_completed_scope",
    )
    runner.run(
        _StreamFootprintAdapter([{
            "facts": [_node("doc:a"), _node("doc:b")],
        }]),
        mode="scope_complete",
    )

    with pytest.raises(
        SubstrateError,
        match="cannot evaluate retraction safety",
    ):
        runner.run(
            _StreamFootprintAdapter([{
                "facts": [_node("doc:a"), retracted_b],
                "run_mode": reported_mode,
                "health": health,
            }]),
            mode="reconcile",
            sync_scope=sync_scope,
        )

    assert _live_source_nodes(clean_runtime, source) == ["doc:a", "doc:b"]


def test_completed_scope_deletion_legacy_append_policy_refuses_tail_retraction(
    clean_runtime: str,
) -> None:
    source = "test.completed-scope-deletion-append-policy"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ChronosGraphStore.from_connection(conn).apply_facts(
            [_node("doc:a"), _node("doc:b")],
            source=source,
        )

    class _LegacyAppendStreamAdapter(_StreamFootprintAdapter):
        profile = "append_feed"

    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
        deletion_semantics="missing_from_completed_scope",
    )

    with pytest.raises(
        SubstrateError,
        match="cannot evaluate retraction safety",
    ):
        runner.run(
            _LegacyAppendStreamAdapter([{"facts": [_node("doc:a")]}]),
            mode="scope_complete",
        )

    assert _live_source_nodes(clean_runtime, source) == ["doc:a", "doc:b"]


def test_completed_scope_deletion_rejected_identity_preserves_prior_authority(
    clean_runtime: str,
) -> None:
    source = "test.completed-scope-deletion-rejected"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    adapter = _StreamFootprintAdapter([
        {"facts": [_node("doc:a"), _node("doc:b")]},
        {"facts": [_node("doc:bad\x00id")]},
    ])
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
        deletion_semantics="missing_from_completed_scope",
    )
    first = runner.run(adapter, mode="scope_complete")
    second = runner.run(adapter, mode="scope_complete")

    assert first.next_cursor is not None
    assert second.rejected_fact_count == 1
    assert second.retractions_node == 0
    assert second.next_cursor is not None
    assert second.next_cursor["record_set"] == first.next_cursor["record_set"]
    assert _live_source_nodes(clean_runtime, source) == ["doc:a", "doc:b"]


def test_completed_scope_deletion_unsafe_explicit_record_set_preserves_prior(
    clean_runtime: str,
) -> None:
    source = "test.completed-scope-deletion-explicit-unsafe"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    both = RecordSet(records={
        "a": RecordEmission(primary=RecordNode("doc:a", "concept")),
        "b": RecordEmission(primary=RecordNode("doc:b", "concept")),
    })
    only_a = RecordSet(records={
        "a": RecordEmission(primary=RecordNode("doc:a", "concept")),
    })
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
    )
    first = runner.run(
        _StreamFootprintAdapter([{
            "facts": [_node("doc:a"), _node("doc:b")],
            "record_set": both,
        }]),
        mode="scope_complete",
    )
    second = runner.run(
        _StreamFootprintAdapter([{
            "facts": [_node("doc:a")],
            "record_set": only_a,
            "health": SourceHealth(
                status="endpoint_failed",
                reason="fixture degraded",
            ),
        }]),
        mode="scope_complete",
    )

    assert first.next_cursor is not None
    assert second.retractions_node == 0
    assert second.next_cursor is not None
    assert second.next_cursor["record_set"] == first.next_cursor["record_set"]
    assert _live_source_nodes(clean_runtime, source) == ["doc:a", "doc:b"]


def test_completed_scope_deletion_aborted_stream_preserves_prior_authority(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = "test.completed-scope-deletion-aborted"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    monkeypatch.setattr(runner_module, "COPY_CHUNK_ROWS", 1)

    def _aborted_facts():
        yield _node("doc:a")
        raise RuntimeError("fixture stream aborted")

    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
        deletion_semantics="missing_from_completed_scope",
    )
    first = runner.run(
        _StreamFootprintAdapter([{
            "facts": [_node("doc:a"), _node("doc:b")],
        }]),
        mode="scope_complete",
    )
    with pytest.raises(RuntimeError, match="fixture stream aborted"):
        runner.run(
            _StreamFootprintAdapter([{"facts": _aborted_facts()}]),
            mode="scope_complete",
        )

    assert first.next_cursor is not None
    assert _live_source_nodes(clean_runtime, source) == ["doc:a", "doc:b"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            "SELECT last_applied_rev FROM okg.source_watermarks "
            "WHERE source = %s",
            (source,),
        )
        stored = cur.fetchone()[0]
    assert stored["record_set"] == first.next_cursor["record_set"]


def test_completed_scope_deletion_substitute_retraction_preserves_owner(
    clean_runtime: str,
) -> None:
    source = "test.completed-scope-deletion-substitute"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ChronosGraphStore.from_connection(conn).apply_facts(
            [_node("shared:node")],
            source="test.authoritative-owner",
        )

    substitute = NodeFact(
        node_id="shared:node",
        subtype="concept",
        attrs={"text": "substitute"},
        source_record_id={"id": "shared:node"},
        source_revision={"rev": "substitute"},
        ownership_mode="substitute",
    )
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
        deletion_semantics="missing_from_completed_scope",
    )
    runner.run(
        _StreamFootprintAdapter([{"facts": [substitute]}]),
        mode="scope_complete",
    )
    # #1859 rule (b): an empty completed stream now keeps the prior state, so
    # the emptied scope is asserted explicitly to reach the substitute
    # retraction this test guards.
    second = runner.run(
        _StreamFootprintAdapter([{"facts": [], "asserts_empty_scope": True}]),
        mode="scope_complete",
    )

    assert second.retractions_node == 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(conn).checkout().query(
            """
            SELECT node_id, source
              FROM okg.graph_nodes
             WHERE node_id = 'shared:node'
            """,
        )
    assert [(row["node_id"], row["source"]) for row in rows] == [
        ("shared:node", "test.authoritative-owner"),
    ]


def test_completed_scope_deletion_conflicting_ownership_fails_before_tail(
    clean_runtime: str,
) -> None:
    source = "test.completed-scope-deletion-ownership-conflict"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    authoritative = _node("shared:node")
    substitute = NodeFact(
        node_id="shared:node",
        subtype="concept",
        attrs={},
        source_record_id={"id": "shared:node"},
        source_revision={"rev": "substitute"},
        ownership_mode="substitute",
    )
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
        deletion_semantics="missing_from_completed_scope",
    )
    with pytest.raises(SubstrateError, match="conflicting node ownership"):
        runner.run(
            _StreamFootprintAdapter([{
                "facts": [authoritative, substitute],
            }]),
            mode="scope_complete",
        )

    assert _live_source_nodes(clean_runtime, source) == []


def test_strict_discovery_crawl_requires_source_record_set_authority(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    runner = SourceRunner(
        clean_runtime,
        source_name="test.strict-discovery-crawl",
        write_fact_log=True,
        executable_profile="discovery_crawl",
        registry_name="strict-discovery-crawl",
        ownership_id="test.strict-discovery-crawl",
        registry_binding_fingerprint="binding-v1",
    )

    with pytest.raises(
        SubstrateError,
        match="lacks complete deletion authority",
    ):
        runner.run(
            _RecordSetAdapter([([], None)]),
            mode="scope_complete",
        )


def _seed_legacy_external_record_set(
    dsn: str,
    *,
    source: str,
    record_set: RecordSet,
    chronos_graph_verified: bool = False,
    cursor_values: dict[str, Any] | None = None,
) -> None:
    payload = json.dumps(
        record_set.to_cursor(),
        sort_keys=True,
        separators=(",", ":"),
    )
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    ref = {
        "schema": "okg.source_runner.record_set_ref.v1",
        "storage": "okg.source_record_set_blobs",
        "source": source,
        "sha256": digest,
    }
    legacy_summary = {
        "schema": "okg.source_runner.record_set_summary.v1",
        "source_name": source,
        "graph_branch": "default",
        "record_count": len(record_set.records),
        "node_count": 1,
        "edge_count": 0,
        "chronos_graph_verified": chronos_graph_verified,
    }
    runner = SourceRunner(dsn, source_name=source, write_fact_log=True)
    try:
        runner._ensure_source_record_sets_table()
        graph_incarnation = runner._chronos_branch_incarnation()
    finally:
        runner._release_conn()
    # A None incarnation would seed a proof that fails the runner's
    # isinstance(str) guard, silently routing the legacy tests through the
    # unverified coverage path instead of the delta path they exist to cover.
    assert graph_incarnation, "fixture needs a live graph incarnation"
    proof = (
        {
            "ok": True,
            "source_name": source,
            "graph_branch": "default",
            "schema": "okg.source_runner.record_set_chronos_proof.v1",
            "graph_incarnation": graph_incarnation,
        }
        if chronos_graph_verified
        else None
    )
    cursor = {
        "record_set_ref": ref,
        "record_set_summary": legacy_summary,
        **(cursor_values or {}),
    }
    if proof is not None:
        cursor["record_set_chronos_proof"] = proof
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.source_record_set_blobs
              (source, sha256, record_set, record_set_summary,
               record_set_text)
            VALUES (%s, %s, '{}'::jsonb, %s::jsonb, %s)
            ON CONFLICT (source, sha256) DO UPDATE SET
              record_set = EXCLUDED.record_set,
              record_set_summary = EXCLUDED.record_set_summary,
              record_set_text = EXCLUDED.record_set_text
            """,
            (source, digest, json.dumps(legacy_summary), payload),
        )
        cur.execute(
            """
            INSERT INTO okg.source_watermarks (source, last_applied_rev)
            VALUES (%s, %s::jsonb)
            ON CONFLICT (source) DO UPDATE SET
              last_applied_rev = EXCLUDED.last_applied_rev
            """,
            (
                source,
                json.dumps(cursor),
            ),
        )


def test_partial_replacement_sanitizes_legacy_external_prior(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The prior-load sanitizer holds on a state the product cannot produce.

    THIS FIXTURE MANUFACTURES AN UNREACHABLE STATE, deliberately. It seeds a
    prior that is simultaneously chronos-verified and NUL-bearing, which
    linear history cannot produce: write-path sanitization landed 2026-07-19
    (ac42f2f9a6) and incarnation binding -- what makes a stored proof
    verifiable at all -- landed 2026-08-01 (cd014cb751), so every verified
    prior was sanitized when it was written. An unverified legacy prior never
    reaches this code either; ``_prior_cursor_for_adapter`` strips it empty
    first.

    So this test does not prove a bug is fixed in the field. It pins the
    defense-in-depth sanitization at the prior-load sites in
    ``runner.py`` -- without it the retired-identity query inside
    ``_verify_record_set_chronos_delta`` raises
    ``psycopg.DataError: PostgreSQL text fields cannot contain NUL`` and the
    run aborts. Read a failure here as "the guard was removed", not as "a
    reachable path regressed".
    """
    monkeypatch.setenv("OKG_SOURCE_RUNNER_INLINE_RECORD_SET_MAX_BYTES", "1")
    monkeypatch.setenv("OKG_SOURCE_RUNNER_RECORD_SET_JSONB_MAX_BYTES", "1")
    source = "test.record-set-legacy-external-partial"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    _seed_legacy_external_record_set(
        clean_runtime,
        source=source,
        record_set=_record("docs/dirty.md", primary="doc:dirty\x00identity"),
        chronos_graph_verified=True,
    )

    clean = _record("docs/clean.md", primary="doc:clean")
    result = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
    ).run(
        _ReplacementSliceRecordSetAdapter([
            ([_node("doc:clean")], clean, {"docs/clean.md"}),
        ]),
        mode="scope_complete",
    )

    assert result.next_cursor is not None
    assert not value_contains_nul(result.next_cursor)
    assert result.next_cursor["record_set_summary"][
        "admission_sanitization"
    ] == "okg.source_runner.record_set_sanitization.v1"
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            SELECT b.record_set,
                   coalesce(b.record_set_text, (
                     SELECT string_agg(p.piece, '' ORDER BY p.piece_no)
                       FROM okg.source_record_set_blob_pieces p
                      WHERE p.write_id = b.piece_write_id))
              FROM okg.source_record_set_blobs b
             WHERE b.source = %s AND b.sha256 = %s
            """,
            (source, result.next_cursor["record_set_ref"]["sha256"]),
        )
        stored_json, stored_text = cur.fetchone()
        stored = json.loads(stored_text) if stored_text else stored_json
        assert set(stored["records"]) == {
            "docs/clean.md",
            "docs/dirty.md",
        }
        assert stored["records"]["docs/dirty.md"] == {
            "primary": None,
            "descendants": [],
            "edges": [],
        }
        assert not value_contains_nul(stored)
        cur.execute(
            """
            SELECT COUNT(*)
              FROM okg.node_facts
             WHERE source = %s AND node_id LIKE 'doc:dirty%%'
            """,
            (source,),
        )
        assert cur.fetchone()[0] == 0


def test_stream_footprint_sanitizes_legacy_external_prior(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The tail stream-footprint path must clean its prior authority too.

    Without sanitization at the stream-footprint prior load, the legacy
    NUL-bearing identity reaches ``_record_set_retractions`` and the retired
    identity query inside ``_verify_record_set_chronos_delta``, which
    PostgreSQL refuses because text cannot hold U+0000.
    """
    monkeypatch.setenv("OKG_SOURCE_RUNNER_INLINE_RECORD_SET_MAX_BYTES", "1")
    monkeypatch.setenv("OKG_SOURCE_RUNNER_RECORD_SET_JSONB_MAX_BYTES", "1")
    source = "test.record-set-legacy-external-stream-footprint"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    _seed_legacy_external_record_set(
        clean_runtime,
        source=source,
        record_set=_record("docs/dirty.md", primary="doc:dirty\x00identity"),
        chronos_graph_verified=True,
    )

    result = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
        deletion_semantics="missing_from_completed_scope",
    ).run(
        _StreamFootprintAdapter([{"facts": [_node("doc:clean")]}]),
        mode="scope_complete",
    )

    assert result.next_cursor is not None
    assert not value_contains_nul(result.next_cursor)
    assert _live_source_nodes(clean_runtime, source) == ["doc:clean"]


def test_incomplete_run_sanitizes_legacy_external_authority(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_SOURCE_RUNNER_INLINE_RECORD_SET_MAX_BYTES", "1")
    monkeypatch.setenv("OKG_SOURCE_RUNNER_RECORD_SET_JSONB_MAX_BYTES", "1")
    source = "test.record-set-legacy-external-incomplete"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    _seed_legacy_external_record_set(
        clean_runtime,
        source=source,
        record_set=_record("docs/dirty.md", primary="doc:dirty\x00identity"),
    )

    result = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
    ).run(
        _RecordSetAdapter(
            [([], None)],
            next_cursor={"page": "2"},
            completed_scope=False,
        ),
        mode="scope_incomplete",
    )

    assert result.next_cursor is not None
    assert result.next_cursor["page"] == "2"
    assert not value_contains_nul(result.next_cursor)
    assert result.next_cursor["record_set_summary"][
        "admission_sanitization"
    ] == "okg.source_runner.record_set_sanitization.v1"
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            SELECT b.record_set,
                   coalesce(b.record_set_text, (
                     SELECT string_agg(p.piece, '' ORDER BY p.piece_no)
                       FROM okg.source_record_set_blob_pieces p
                      WHERE p.write_id = b.piece_write_id))
              FROM okg.source_record_set_blobs b
             WHERE b.source = %s AND b.sha256 = %s
            """,
            (source, result.next_cursor["record_set_ref"]["sha256"]),
        )
        stored_json, stored_text = cur.fetchone()
    stored = json.loads(stored_text) if stored_text else stored_json
    assert stored["records"] == {
        "docs/dirty.md": {
            "primary": None,
            "descendants": [],
            "edges": [],
        },
    }
    assert not value_contains_nul(stored)


def test_probe_skip_sanitizes_carried_legacy_external_authority(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_SOURCE_RUNNER_INLINE_RECORD_SET_MAX_BYTES", "1")
    monkeypatch.setenv("OKG_SOURCE_RUNNER_RECORD_SET_JSONB_MAX_BYTES", "1")
    source = "test.record-set-legacy-external-probe-skip"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    content_digest = hashlib.sha256(b"v1").hexdigest()
    config_digest = hashlib.sha256(b"config").hexdigest()
    emit_digest = hashlib.sha256(b"emit").hexdigest()
    token = compose_token(
        content={"kind": "content_hash", "value": content_digest},
        config=config_digest,
        emit_version=emit_digest,
    )
    adapter = _RecordSetAdapter([([], None)])
    adapter.change_probe = _UnchangedProbe(token)
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
    )
    try:
        effective_token = build_effective_token(
            token,
            runner._resolve_pipeline_identity_snapshot(adapter),
        )
    finally:
        runner._release_conn()
    _seed_legacy_external_record_set(
        clean_runtime,
        source=source,
        record_set=_record("docs/dirty.md", primary="doc:dirty\x00identity"),
        chronos_graph_verified=True,
        cursor_values={CURSOR_TOKEN_KEY: effective_token},
    )

    result = runner.run(adapter, mode="scope_complete")

    assert adapter.seen_cursors == [], "unchanged probe must skip the reader"
    assert result.completed_scope is True
    assert result.next_cursor is not None
    assert not value_contains_nul(result.next_cursor)
    assert result.next_cursor["record_set_summary"][
        "admission_sanitization"
    ] == "okg.source_runner.record_set_sanitization.v1"
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            SELECT b.record_set,
                   coalesce(b.record_set_text, (
                     SELECT string_agg(p.piece, '' ORDER BY p.piece_no)
                       FROM okg.source_record_set_blob_pieces p
                      WHERE p.write_id = b.piece_write_id))
              FROM okg.source_record_set_blobs b
             WHERE b.source = %s AND b.sha256 = %s
            """,
            (source, result.next_cursor["record_set_ref"]["sha256"]),
        )
        stored_json, stored_text = cur.fetchone()
    stored = json.loads(stored_text) if stored_text else stored_json
    assert stored["records"] == {
        "docs/dirty.md": {
            "primary": None,
            "descendants": [],
            "edges": [],
        },
    }
    assert not value_contains_nul(stored)


def test_record_set_verified_cursor_checks_added_delta(
    clean_runtime: str,
) -> None:
    source = "test.record-set-verified-delta"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    first_record = _record("docs/a.md", primary="doc:a")
    second_record = RecordSet(records={
        **first_record.records,
        "docs/b.md": RecordEmission(primary=RecordNode("doc:b", "concept")),
    })
    adapter = _RecordSetAdapter([
        ([_node("doc:a")], first_record),
        ([], second_record),
    ])
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
    )

    runner.run(adapter, mode="scope_complete")
    with pytest.raises(SubstrateError, match="delta coverage"):
        runner.run(adapter, mode="scope_complete")


def test_record_set_verified_cursor_rejects_emitted_stray(
    clean_runtime: str,
) -> None:
    source = "test.record-set-verified-stray"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    record = _record("docs/a.md", primary="doc:a")
    adapter = _RecordSetAdapter([
        ([_node("doc:a")], record),
        ([_node("doc:orphan")], record),
    ])
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
    )

    runner.run(adapter, mode="scope_complete")
    with pytest.raises(SubstrateError, match="record_set omitted a node"):
        runner.run(adapter, mode="scope_complete")

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(conn).checkout().query(
            """
            SELECT node_id
              FROM okg.graph_nodes
             WHERE source = :source
               AND node_id = 'doc:orphan'
            """,
            {"source": source},
        )
    assert rows == []


def test_record_set_coverage_blocks_cursor_when_graph_missing(
    clean_runtime: str,
) -> None:
    source = "test.record-set-coverage"
    record_set = _record(
        "docs/a.md",
        primary="doc:a",
        descendants=("chunk:a",),
        edges=(("doc:a", "contains", "chunk:a"),),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.source_watermarks
                  (source, last_applied_rev)
                VALUES (%s, %s::jsonb)
                """,
                (
                    source,
                    json.dumps({
                        "record_set": record_set.to_cursor(),
                        "files": {"docs/a.md": {"sha": "old"}},
                    }),
                ),
            )

    adapter = _CursorAwareRecordSetAdapter(
        facts_when_reset=[
            _node("doc:a"),
            _node("chunk:a"),
            _edge("doc:a", "contains", "chunk:a"),
        ],
        record_set=record_set,
    )
    with pytest.raises(SubstrateError, match="record_set coverage"):
        SourceRunner(clean_runtime, source_name=source).run(
            adapter,
            mode="scope_complete",
        )

    # Unverified record authority is withheld; adapter resume state survives.
    assert adapter.seen_cursors == [
        {
            "files": {"docs/a.md": {"sha": "old"}},
        }
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_applied_rev FROM okg.source_watermarks "
                "WHERE source = %s",
                (source,),
            )
            assert cur.fetchone()[0]["files"]["docs/a.md"]["sha"] == "old"


def test_record_set_coverage_accepts_semantic_edge_from_other_source(
    clean_runtime: str,
) -> None:
    source = "test.record-set-semantic-edge"
    record_set = _record(
        "docs/a.md",
        primary="doc:a",
        descendants=("chunk:a",),
        edges=(("doc:a", "contains", "chunk:a"),),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [_node("doc:a"), _node("chunk:a")],
            source=source,
        )
        store.apply_facts(
            [_edge("doc:a", "contains", "chunk:a")],
            source="_publisher_derived",
        )

    SourceRunner(
        clean_runtime,
        source_name=source,
    )._verify_record_set_chronos_coverage(record_set)


def test_record_set_substitute_node_accepts_authoritative_owner(
    clean_runtime: str,
) -> None:
    source = "test.record-set-substitute"
    node_id = "shared:node"
    substitute = RecordSet(
        records={
            node_id: RecordEmission(
                primary=RecordNode(
                    node_id,
                    "concept",
                    ownership_mode="substitute",
                )
            )
        }
    )
    authoritative = RecordSet(
        records={
            node_id: RecordEmission(
                primary=RecordNode(node_id, "concept")
            )
        }
    )
    restored = RecordSet.from_cursor(substitute.to_cursor())
    assert restored == substitute
    restored_primary = restored.records[node_id].primary
    assert restored_primary is not None
    assert restored_primary.ownership_mode == "substitute"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        ChronosGraphStore.from_connection(conn).apply_facts(
            [_node(node_id)],
            source="test.authoritative-owner",
        )

    runner = SourceRunner(clean_runtime, source_name=source)
    retraction = runner._node_retraction_fact(node_id, restored_primary)
    assert retraction.ownership_mode == "substitute"
    runner._verify_record_set_chronos_coverage(substitute)
    with pytest.raises(SubstrateError, match="record_set coverage"):
        runner._verify_record_set_chronos_coverage(authoritative)


_SHARED_AUTHOR = "author:shared@example.test"


def _shared_author_fact(repo: str) -> NodeFact:
    return NodeFact(
        node_id=_SHARED_AUTHOR,
        subtype="concept",
        attrs={"text": _SHARED_AUTHOR, "seen_in": repo},
        source_record_id={"author": _SHARED_AUTHOR, "repo": repo},
        source_revision={"rev": repo},
        ownership_mode="substitute",
    )


def _repo_record(repo: str, *, with_author: bool) -> RecordSet:
    return RecordSet(records={
        f"repo:{repo}": RecordEmission(
            primary=RecordNode(f"repo:{repo}", "concept"),
            descendants=(
                [
                    RecordNode(
                        _SHARED_AUTHOR,
                        "concept",
                        ownership_mode="substitute",
                    )
                ]
                if with_author else []
            ),
        )
    })


def _shared_author_owner(dsn: str) -> str | None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(conn).checkout().query(
            "SELECT source FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": _SHARED_AUTHOR},
        )
    return str(rows[0]["source"]) if rows else None


def test_shared_node_passes_to_sibling_source_and_its_coverage_holds(
    clean_runtime: str,
) -> None:
    """Issue #1901 acceptance: alpha owns a node beta also declares; alpha's
    scope-complete run drops it; the row stays, beta owns it, and beta's next
    coverage proof passes (it refused with missing_nodes before)."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    alpha = SourceRunner(clean_runtime, source_name="test.repo-alpha")
    beta = SourceRunner(clean_runtime, source_name="test.repo-beta")
    alpha_adapter = _RecordSetAdapter([
        (
            [_node("repo:alpha"), _shared_author_fact("alpha")],
            _repo_record("alpha", with_author=True),
        ),
        ([_node("repo:alpha")], _repo_record("alpha", with_author=False)),
    ])
    beta_record = _repo_record("beta", with_author=True)
    beta_adapter = _RecordSetAdapter([
        ([_node("repo:beta"), _shared_author_fact("beta")], beta_record),
    ])

    alpha.run(alpha_adapter, mode="scope_complete")
    beta.run(beta_adapter, mode="scope_complete")
    assert _shared_author_owner(clean_runtime) == "test.repo-alpha"

    second = alpha.run(alpha_adapter, mode="scope_complete")
    assert second.retractions_node == 1

    assert _shared_author_owner(clean_runtime) == "test.repo-beta"
    beta._verify_record_set_chronos_coverage(beta_record)
    assert _live_source_nodes(clean_runtime, "test.repo-alpha") == [
        "repo:alpha",
    ]


def test_shared_node_stays_while_another_record_of_the_same_source_declares_it(
    clean_runtime: str,
) -> None:
    """Several repositories inside ONE source: the runner retracts a node only
    when no record of that source still declares it, so dropping the author
    from one repository's record leaves it in place."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    both = RecordSet(records={
        **_repo_record("one", with_author=True).records,
        **_repo_record("two", with_author=True).records,
    })
    only_two = RecordSet(records={
        **_repo_record("one", with_author=False).records,
        **_repo_record("two", with_author=True).records,
    })
    adapter = _RecordSetAdapter([
        (
            [
                _node("repo:one"),
                _node("repo:two"),
                _shared_author_fact("one"),
            ],
            both,
        ),
        (
            [
                _node("repo:one"),
                _node("repo:two"),
                _shared_author_fact("two"),
            ],
            only_two,
        ),
    ])
    runner = SourceRunner(clean_runtime, source_name="test.multi-repo")
    runner.run(adapter, mode="scope_complete")
    second = runner.run(adapter, mode="scope_complete")

    assert second.retractions_node == 0
    assert _shared_author_owner(clean_runtime) == "test.multi-repo"
    runner._verify_record_set_chronos_coverage(only_two)


def _git_repo_with_author(path, email: str, filename: str):
    import subprocess

    def git(*args: str) -> None:
        subprocess.run(
            ["git", "-C", str(path), *args],
            check=True,
            capture_output=True,
            text=True,
        )

    path.mkdir()
    git("init", "-b", "main")
    git("config", "user.email", email)
    git("config", "user.name", "Shared Author")
    (path / filename).write_text("x = 1\n", encoding="utf-8")
    git("add", filename)
    git("commit", "-m", "first")
    return git


def test_reset_cursor_run_records_existing_shared_author_declarations(
    clean_runtime: str,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The operator step for #1901's no-backfill limit, on real git history.

    A deployment upgraded without a backfill has no declarations. A cursor
    run walks only new commits, so it does not re-emit an existing author;
    an unchanged repository's scope-complete run is skipped by the change
    probe. A scope-complete run with --reset-cursor walks every commit and
    records the declaration.
    """
    from okg.substrate.library.sources.git_graph_history import GitHistorySource

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    email = "shared@example.test"
    author_id = f"author:{email}"
    _git_repo_with_author(tmp_path / "alpha", email, "a.py")
    beta_git = _git_repo_with_author(tmp_path / "beta", email, "b.py")

    alpha_source = GitHistorySource(repo="alpha", repo_root=tmp_path / "alpha")
    beta_source = GitHistorySource(repo="beta", repo_root=tmp_path / "beta")
    alpha = SourceRunner(
        clean_runtime, source_name="demo.alpha-history", deployment_name="demo",
    )
    beta = SourceRunner(
        clean_runtime, source_name="demo.beta-history", deployment_name="demo",
    )
    # Runs made by code from before the declarer table existed record
    # nothing: the state of a deployment upgraded without a backfill.
    with monkeypatch.context() as before_upgrade:
        before_upgrade.setattr(
            ChronosGraphStore, "_tracks_node_declarers", lambda self: False,
        )
        alpha.run(alpha_source, mode="scope_complete")
        beta.run(beta_source, mode="scope_complete")

    def declarers() -> list[str]:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            store = ChronosGraphStore.from_connection(conn)
            return sorted(
                str(row["source"])
                for row in store.checkout().query(
                    "SELECT source FROM okg.graph_node_declarers "
                    "WHERE node_id = :node_id",
                    {"node_id": author_id},
                )
            )

    assert declarers() == []

    # An unchanged repository: the change probe skips the scope-complete run.
    skipped = beta.run(beta_source, mode="scope_complete")
    assert skipped.mode == "no_change", skipped.mode
    assert declarers() == []

    # A cursor run after a new commit by someone else walks only that commit.
    beta_git("config", "user.email", "other@example.test")
    (tmp_path / "beta" / "c.py").write_text("y = 2\n", encoding="utf-8")
    beta_git("add", "c.py")
    beta_git("commit", "-m", "second")
    beta.run(beta_source, mode="cursor")
    assert declarers() == []

    beta.run(beta_source, mode="scope_complete", reset_cursor=True)
    assert declarers() == ["demo.beta-history"]


def test_record_set_rejects_conflicting_node_ownership_modes(
    clean_runtime: str,
) -> None:
    node_id = "shared:conflict"
    record_set = RecordSet(
        records={
            "one": RecordEmission(
                primary=RecordNode(node_id, "concept")
            ),
            "two": RecordEmission(
                primary=RecordNode(
                    node_id,
                    "concept",
                    ownership_mode="substitute",
                )
            ),
        }
    )

    with pytest.raises(SubstrateError, match="conflicting node ownership"):
        SourceRunner(
            clean_runtime,
            source_name="test.record-set-conflict",
        )._record_set_node_ownership_modes(record_set)


def test_record_set_rejects_conflicting_node_ownership_modes_within_emission(
    clean_runtime: str,
) -> None:
    node_id = "shared:conflict-within-emission"
    record_set = RecordSet(
        records={
            "one": RecordEmission(
                primary=RecordNode(node_id, "concept"),
                descendants=[
                    RecordNode(
                        node_id,
                        "concept",
                        ownership_mode="substitute",
                    )
                ],
            )
        }
    )

    with pytest.raises(SubstrateError, match="conflicting node ownership"):
        SourceRunner(
            clean_runtime,
            source_name="test.record-set-conflict-within-emission",
        )._record_set_node_ownership_modes(record_set)


def test_record_set_reset_cursor_repairs_missing_graph(
    clean_runtime: str,
) -> None:
    source = "test.record-set-reset"
    record_set = _record(
        "docs/a.md",
        primary="doc:a",
        descendants=("chunk:a",),
        edges=(("doc:a", "contains", "chunk:a"),),
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.source_watermarks
                  (source, last_applied_rev)
                VALUES (%s, %s::jsonb)
                """,
                (
                    source,
                    json.dumps({
                        "record_set": record_set.to_cursor(),
                        "files": {"docs/a.md": {"sha": "old"}},
                    }),
                ),
            )

    adapter = _CursorAwareRecordSetAdapter(
        facts_when_reset=[
            _node("doc:a"),
            _node("chunk:a"),
            _edge("doc:a", "contains", "chunk:a"),
        ],
        record_set=record_set,
    )
    result = SourceRunner(clean_runtime, source_name=source).run(
        adapter,
        mode="scope_complete",
        reset_cursor=True,
    )

    assert adapter.seen_cursors == [None]
    assert result.nodes_appended == 2
    assert result.edges_appended == 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(conn).checkout().query(
            """
            SELECT node_id
              FROM okg.graph_nodes
             WHERE source = :source
             ORDER BY node_id
            """,
            {"source": source},
        )
        assert [row["node_id"] for row in rows] == ["chunk:a", "doc:a"]
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_applied_rev FROM okg.source_watermarks "
                "WHERE source = %s",
                (source,),
            )
            cursor = cur.fetchone()[0]
    assert cursor["cursor_seen"] is False
    assert cursor["record_set"] == record_set.to_cursor()


def test_source_runner_requires_at_least_one_write_sink(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    adapter = _RecordSetAdapter([
        ([_node("doc:a")], _record("docs/a.md", primary="doc:a")),
    ])
    with pytest.raises(SubstrateError, match="no enabled write sink"):
        SourceRunner(
            clean_runtime,
            source_name="test.no-sink",
            write_chronos_graph=False,
            write_fact_log=False,
        ).run(adapter, mode="scope_complete")


def _insert_legacy_node_fact(
    conn: psycopg.Connection,
    source: str,
    node_id: str,
    subtype: str,
) -> None:
    run_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.node_facts
              (node_id, subtype, attrs, source, source_record_id,
               source_revision, observed_at, dedupe_key, run_id, op)
            VALUES (%s, %s, '{}'::jsonb, %s, %s::jsonb, %s::jsonb,
                    now(), %s, %s, 'I')
            """,
            (
                node_id,
                subtype,
                source,
                json.dumps({"id": node_id}),
                json.dumps({"v": "legacy"}),
                f"{source}:{node_id}:legacy:{uuid.uuid4().hex}",
                run_id,
            ),
        )


def _insert_legacy_edge_fact(
    conn: psycopg.Connection,
    source: str,
    src: str,
    edge_type: str,
    dst: str,
) -> None:
    run_id = str(uuid.uuid4())
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.edge_facts
              (edge_id, src, dst, edge_type, attrs, provenance,
               confidence, source, source_record_id, source_revision,
               observed_at, dedupe_key, run_id, op)
            VALUES (nextval('okg.edge_facts_edge_id_seq'),
                    %s, %s, %s, '{}'::jsonb, 'explicit', 1.0,
                    %s, %s::jsonb, %s::jsonb, now(), %s, %s, 'I')
            """,
            (
                src,
                dst,
                edge_type,
                source,
                json.dumps({
                    "src": src,
                    "edge_type": edge_type,
                    "dst": dst,
                }),
                json.dumps({"v": "legacy"}),
                f"{source}:{src}:{edge_type}:{dst}:legacy:{uuid.uuid4().hex}",
                run_id,
            ),
        )


def test_scoped_replacement_slice_retracts_only_its_own_keys(
    clean_runtime: str,
) -> None:
    """Issue #2145: a narrowed delete run retracts its key and nothing else.

    Runs under an executable profile, which is what the real ingest service
    passes, so this also covers the discovery-crawl forced-full refusal and
    the retraction-policy guard -- three separate places dropped the scoped
    authority, and any one of them re-refusing brings this back to red.
    """
    source = "test.record-set-scoped-slice"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    adapter = _ScopedRecordSetAdapter([
        {
            "facts": [_node("doc:a"), _node("doc:b")],
            "completed_scope": True,
            "record_set": _two_record_prior(),
        },
        {
            "facts": [],
            "completed_scope": False,
            "record_set": RecordSet(),
            "replacement_keys": {"b.md"},
        },
    ])
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        executable_profile="discovery_crawl",
        registry_name=source,
        ownership_id=source,
        registry_binding_fingerprint="sha256:scoped-record-set-guard",
    )

    first = runner.run(adapter, mode="scope_complete")
    assert first.retractions_node == 0
    assert _live_source_nodes(clean_runtime, source) == ["doc:a", "doc:b"]

    second = runner.run(
        adapter,
        mode="scope_complete",
        sync_scope={"scope_keys": ["b.md"]},
    )

    assert second.retractions_node == 1
    assert _live_source_nodes(clean_runtime, source) == ["doc:a"]
    assert adapter.seen_scopes[1] == {"scope_keys": ["b.md"]}


def test_scoped_merged_record_set_retracts_within_scope(
    clean_runtime: str,
) -> None:
    """Issue #2145: the merged-authority shape retracts too.

    An adapter that folds the durable prior into its slice returns
    `completed_scope=True` and no replacement keys. The run is still narrowed,
    and the `not sync_scope` term used to throw the whole set away.
    """
    source = "test.record-set-scoped-merged"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    adapter = _ScopedRecordSetAdapter([
        {
            "facts": [_node("doc:a"), _node("doc:b")],
            "completed_scope": True,
            "record_set": _two_record_prior(),
        },
        {
            "facts": [],
            "completed_scope": True,
            "record_set": _record("a.md", primary="doc:a"),
        },
    ])
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        executable_profile="discovery_crawl",
        registry_name=source,
        ownership_id=source,
        registry_binding_fingerprint="sha256:scoped-record-set-guard",
    )

    runner.run(adapter, mode="scope_complete")
    assert _live_source_nodes(clean_runtime, source) == ["doc:a", "doc:b"]

    second = runner.run(
        adapter,
        mode="scope_complete",
        sync_scope={"scope_keys": ["b.md"]},
    )

    assert second.retractions_node == 1
    assert _live_source_nodes(clean_runtime, source) == ["doc:a"]


def test_scoped_run_without_record_set_authority_still_retracts_nothing(
    clean_runtime: str,
) -> None:
    """The #2145 fix must not hand deletion authority to a bare scoped run.

    A narrowed run that returns no RecordSet argues only from absence, and a
    partial walk proves no absence. It must keep retracting nothing.
    """
    source = "test.record-set-scoped-no-authority"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    adapter = _ScopedRecordSetAdapter([
        {
            "facts": [_node("doc:a"), _node("doc:b")],
            "completed_scope": True,
            "record_set": _two_record_prior(),
        },
        {
            "facts": [],
            "completed_scope": False,
            "record_set": None,
        },
    ])
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        deletion_semantics="missing_from_completed_scope",
    )

    runner.run(adapter, mode="scope_complete")
    second = runner.run(
        adapter,
        mode="scope_complete",
        sync_scope={"scope_keys": ["b.md"]},
    )

    assert second.retractions_node == 0
    assert _live_source_nodes(clean_runtime, source) == ["doc:a", "doc:b"]


def test_scoped_merged_claim_covering_only_its_scope_is_refused(
    clean_runtime: str,
) -> None:
    """Issue #2145 review B1: completed_scope alone is not whole-source authority.

    A narrowed run that declares `completed_scope=True` but returns a set
    covering only the file it was asked about has NOT merged the durable
    prior. Trusting the claim retracted a.md and c.md - records the run
    never walked, which is the issue #2122 failure mode reached through the
    scoped door. The runner now compares the declared set against the
    durable prior and refuses.
    """
    source = "test.record-set-scoped-merged-claim-narrow"
    runner, adapter = _seed_three_records(clean_runtime, source, {
        "facts": [],
        "completed_scope": True,
        "record_set": _record("b.md", primary="doc:b"),
    })

    with pytest.raises(SubstrateError) as excinfo:
        runner.run(
            adapter,
            mode="scope_complete",
            sync_scope={"scope_keys": ["b.md"]},
        )

    assert excinfo.value.data["reason_code"] == (
        "scoped_merged_record_set_incomplete"
    )
    assert excinfo.value.data["uncovered_prior_record_keys"] == [
        "a.md", "c.md",
    ]
    assert runner.tail_failure_diagnostic["phase"] == (
        "record_set.scoped_merged_prior"
    )
    assert _live_source_nodes(clean_runtime, source) == [
        "doc:a", "doc:b", "doc:c",
    ]


def test_scoped_merged_claim_with_empty_record_set_is_refused(
    clean_runtime: str,
) -> None:
    """Issue #2145 review B1: an empty set must not wipe the source.

    `completed_scope=True` with an EMPTY RecordSet on a narrowed run
    retracted all three records - the whole source erased by a run asked
    about one file.
    """
    source = "test.record-set-scoped-merged-claim-empty"
    runner, adapter = _seed_three_records(clean_runtime, source, {
        "facts": [],
        "completed_scope": True,
        "record_set": RecordSet(),
    })

    with pytest.raises(SubstrateError) as excinfo:
        runner.run(
            adapter,
            mode="scope_complete",
            sync_scope={"scope_keys": ["b.md"]},
        )

    assert excinfo.value.data["reason_code"] == (
        "scoped_merged_record_set_incomplete"
    )
    assert _live_source_nodes(clean_runtime, source) == [
        "doc:a", "doc:b", "doc:c",
    ]


def test_unscoped_replacement_keys_carry_no_deletion_authority(
    clean_runtime: str,
) -> None:
    """Issue #2145 review S1: the scoped exception needs a scope.

    The replacement-slice authority is an exception granted to a NARROWED
    run. An unscoped run that is not in a completed-scope run mode gets no
    deletion authority from replacement keys alone, or this change would
    widen a deletion path it never examined.
    """
    source = "test.record-set-unscoped-replacement-keys"
    runner, adapter = _seed_three_records(clean_runtime, source, {
        "facts": [],
        "completed_scope": False,
        "run_mode": "cursor",
        "record_set": RecordSet(),
        "replacement_keys": {"b.md"},
    })

    second = runner.run(adapter, mode="scope_complete")

    assert second.retractions_node == 0
    assert _live_source_nodes(clean_runtime, source) == [
        "doc:a", "doc:b", "doc:c",
    ]


def test_replacement_slice_carrying_an_undeclared_record_is_refused(
    clean_runtime: str,
) -> None:
    """Issue #2145 review S2: the slice bound covers content, not only retraction.

    A slice declaring {b.md} that also carries a record for a.md does not
    retract a.md - it REWRITES it, which the retraction diff cannot see.
    The merge refuses the undeclared key.
    """
    source = "test.record-set-slice-undeclared-key"
    runner, adapter = _seed_three_records(clean_runtime, source, {
        "facts": [_node("doc:a-hijacked")],
        "completed_scope": False,
        "record_set": _record("a.md", primary="doc:a-hijacked"),
        "replacement_keys": {"b.md"},
    })

    with pytest.raises(SubstrateError) as excinfo:
        runner.run(
            adapter,
            mode="scope_complete",
            sync_scope={"scope_keys": ["b.md"]},
        )

    assert excinfo.value.data["reason_code"] == (
        "record_set_replacement_slice_undeclared_key"
    )
    assert excinfo.value.data["undeclared_record_keys"] == ["a.md"]
    assert _live_source_nodes(clean_runtime, source) == [
        "doc:a", "doc:b", "doc:c",
    ]
    assert runner.tail_failure_diagnostic["phase"] == (
        "record_set.replacement_merge"
    )
    assert runner.tail_failure_diagnostic["status"] == "failed"


def test_scoped_record_set_without_completed_scope_retracts_nothing(
    clean_runtime: str,
) -> None:
    """Issue #2145 review S3: the guard the mutation sweep was missing.

    A narrowed run that carries a RecordSet but neither declares
    replacement keys nor claims a completed scope has stated no bound at
    all. It must retract nothing. Without this test the whole tail of
    `_scoped_record_set_deletion_authority` could be replaced by an
    unconditional grant and every other test stayed green.
    """
    source = "test.record-set-scoped-no-bound"
    runner, adapter = _seed_three_records(clean_runtime, source, {
        "facts": [],
        "completed_scope": False,
        "record_set": _record("b.md", primary="doc:b"),
    })

    second = runner.run(
        adapter,
        mode="scope_complete",
        sync_scope={"scope_keys": ["b.md"]},
    )

    assert second.retractions_node == 0
    assert _live_source_nodes(clean_runtime, source) == [
        "doc:a", "doc:b", "doc:c",
    ]


def test_scoped_slice_never_strays_outside_a_prior_without_authority(
    clean_runtime: str,
) -> None:
    """Issue #2145 review B1: the stray sweep is whole-source authority.

    Stray retraction reads the whole live graph for the source and retires
    everything the record set does not cover. Running it for a narrowed
    slice let that slice wipe records published before the source carried
    any record-set authority: there is no durable prior to check the slice
    against, so the coverage comparison passes vacuously. A narrowed run
    must leave those records standing.
    """
    source = "test.record-set-scoped-stray"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    adapter = _ScopedRecordSetAdapter([
        {
            "facts": [_node("doc:a"), _node("doc:b"), _node("doc:c")],
            "completed_scope": True,
            "record_set": None,
        },
        {
            "facts": [],
            "completed_scope": False,
            "record_set": RecordSet(),
            "replacement_keys": {"b.md"},
        },
    ])
    runner = SourceRunner(clean_runtime, source_name=source)
    runner.run(adapter, mode="scope_complete")
    assert _live_source_nodes(clean_runtime, source) == [
        "doc:a", "doc:b", "doc:c",
    ]

    with pytest.raises(SubstrateError):
        runner.run(
            adapter,
            mode="scope_complete",
            sync_scope={"scope_keys": ["b.md"]},
        )

    assert _live_source_nodes(clean_runtime, source) == [
        "doc:a", "doc:b", "doc:c",
    ]


def test_scoped_slice_declaring_the_whole_source_is_refused(
    clean_runtime: str,
) -> None:
    """Issue #2145 review B1: the slice bound must come from the SCOPE.

    `record_set_replacement_keys` is the exact list of prior keys the diff
    may retract, and the adapter supplies it. A run narrowed to b.md that
    declares a.md/b.md/c.md and returns an empty slice therefore emptied
    the whole source. The runner holds the sync_scope it handed out and now
    refuses any declared key outside it.
    """
    source = "test.record-set-slice-declares-whole-source"
    runner, adapter = _seed_three_records(clean_runtime, source, {
        "facts": [],
        "completed_scope": False,
        "record_set": RecordSet(),
        "replacement_keys": {"a.md", "b.md", "c.md"},
    })

    with pytest.raises(SubstrateError) as excinfo:
        runner.run(
            adapter,
            mode="scope_complete",
            sync_scope={"scope_keys": ["b.md"]},
        )

    assert excinfo.value.data["reason_code"] == (
        "scoped_replacement_keys_outside_scope"
    )
    assert excinfo.value.data["out_of_scope_replacement_keys"] == [
        "a.md", "c.md",
    ]
    assert _live_source_nodes(clean_runtime, source) == [
        "doc:a", "doc:b", "doc:c",
    ]
    # Issue #2145 review S2: a record-set refusal leaves an
    # okg.source_tail.v1 breadcrumb, like its neighbours.
    assert runner.tail_failure_diagnostic["phase"] == (
        "record_set.scoped_replacement_scope"
    )
    assert runner.tail_failure_diagnostic["status"] == "failed"


def test_scoped_slice_declaring_only_another_key_is_refused(
    clean_runtime: str,
) -> None:
    """Issue #2145 review B1: one out-of-scope key is already too many.

    A run narrowed to b.md that declares {a.md} and returns an empty slice
    retracted a.md - a record wholly outside the scope - while leaving the
    record the run was actually asked about untouched.
    """
    source = "test.record-set-slice-declares-other-key"
    runner, adapter = _seed_three_records(clean_runtime, source, {
        "facts": [],
        "completed_scope": False,
        "record_set": RecordSet(),
        "replacement_keys": {"a.md"},
    })

    with pytest.raises(SubstrateError) as excinfo:
        runner.run(
            adapter,
            mode="scope_complete",
            sync_scope={"scope_keys": ["b.md"]},
        )

    assert excinfo.value.data["reason_code"] == (
        "scoped_replacement_keys_outside_scope"
    )
    assert excinfo.value.data["out_of_scope_replacement_keys"] == ["a.md"]
    assert _live_source_nodes(clean_runtime, source) == [
        "doc:a", "doc:b", "doc:c",
    ]


def test_scoped_slice_declaring_the_whole_source_is_refused_under_profile(
    clean_runtime: str,
) -> None:
    """The same refusal under the profile the real ingest service passes.

    Issue #2145 review B1 case O3. `executable_profile="discovery_crawl"`
    runs the forced-full and retraction-policy branches this change also
    touched, so the bound has to hold on that path too.
    """
    source = "test.record-set-slice-whole-source-profile"
    runner, adapter = _seed_three_records(
        clean_runtime,
        source,
        {
            "facts": [],
            "completed_scope": False,
            "record_set": RecordSet(),
            "replacement_keys": {"a.md", "b.md", "c.md"},
        },
        executable_profile="discovery_crawl",
    )

    with pytest.raises(SubstrateError) as excinfo:
        runner.run(
            adapter,
            mode="scope_complete",
            sync_scope={"scope_keys": ["b.md"]},
        )

    assert excinfo.value.data["reason_code"] == (
        "scoped_replacement_keys_outside_scope"
    )
    assert _live_source_nodes(clean_runtime, source) == [
        "doc:a", "doc:b", "doc:c",
    ]


def test_scoped_slice_may_declare_a_scope_independent_synthetic_key(
    clean_runtime: str,
) -> None:
    """The one allowance the scope bound grants, pinned here.

    Issue #2145 review B1. Some adapters own synthetic records that belong
    to no path -- GitFilesSource keys its branch record
    `__branch__:<branch>` -- so those keys lie under no scope key by
    construction. The runner recognises them by spelling, never on the
    adapter's say-so, and the run proceeds and retracts its scoped key.
    Drop the allowance and every scoped GitFilesSource delete refuses.
    """
    source = "test.record-set-slice-synthetic-key"
    runner, adapter = _seed_three_records(clean_runtime, source, {
        "facts": [],
        "completed_scope": False,
        "record_set": RecordSet(),
        "replacement_keys": {"b.md", "__branch__:main"},
    })

    second = runner.run(
        adapter,
        mode="scope_complete",
        sync_scope={"scope_keys": ["b.md"]},
    )

    assert second.retractions_node == 1
    assert _live_source_nodes(clean_runtime, source) == ["doc:a", "doc:c"]


def test_scoped_merged_claim_rewriting_an_out_of_scope_record_is_refused(
    clean_runtime: str,
) -> None:
    """Issue #2145 review B1: presence is not preservation.

    A merged claim that CARRIES every prior key passes a presence-only
    coverage check, so a set narrowed to b.md that returns a.md with a
    different primary node retracted the real doc:a and published
    doc:a-hijacked in its place - an out-of-scope rewrite the retraction
    diff cannot see. The declared record for an out-of-scope key must now
    match the durable prior exactly.
    """
    source = "test.record-set-merged-claim-rewrites-prior"
    hijacked = RecordSet(records={
        **_record("a.md", primary="doc:a-hijacked").records,
        **_record("b.md", primary="doc:b").records,
        **_record("c.md", primary="doc:c").records,
    })
    runner, adapter = _seed_three_records(clean_runtime, source, {
        "facts": [_node("doc:a-hijacked")],
        "completed_scope": True,
        "record_set": hijacked,
    })

    with pytest.raises(SubstrateError) as excinfo:
        runner.run(
            adapter,
            mode="scope_complete",
            sync_scope={"scope_keys": ["b.md"]},
        )

    assert excinfo.value.data["reason_code"] == (
        "scoped_merged_record_set_altered_outside_scope"
    )
    assert excinfo.value.data["altered_prior_record_keys"] == ["a.md"]
    assert _live_source_nodes(clean_runtime, source) == [
        "doc:a", "doc:b", "doc:c",
    ]
    assert runner.tail_failure_diagnostic["phase"] == (
        "record_set.scoped_merged_prior"
    )
    assert runner.tail_failure_diagnostic["status"] == "failed"


# Issue #1539. An LSP-like source: pkg/utils.py owns a symbol whose id
# carries its line; pkg/queries.py imports it and owns only a reference
# whose edges point at that symbol and at one in pkg/storages.py.
_LSP_OLD_TARGET = "symbol:t:pkg/utils.py:pkg.utils.f:10"
_LSP_NEW_TARGET = "symbol:t:pkg/utils.py:pkg.utils.f:14"
_LSP_OTHER_TARGET = "symbol:t:pkg/storages.py:pkg.storages.S:5"
_LSP_REF = "reference:queries-1"
_LSP_OTHER_REF = "reference:storages-1"


def _lsp_emission(
    primary: str,
    *,
    edges: tuple[tuple[str, str, str], ...] = (),
    descendants: tuple[str, ...] = (),
) -> RecordEmission:
    return _record(
        "k",
        primary=primary,
        descendants=descendants,
        edges=edges,
    ).records["k"]


def _lsp_queries(target: str, *, keep_other: bool = True) -> RecordEmission:
    edges = ((_LSP_REF, "references", target),)
    if keep_other:
        edges += ((_LSP_REF, "references", _LSP_OTHER_TARGET),)
    return _lsp_emission(_LSP_REF, edges=edges)


def _seed_lsp_like_source(
    clean_runtime: str,
    source: str,
    second: dict[str, Any],
) -> tuple[SourceRunner, _ScopedRecordSetAdapter]:
    """Publish utils/queries/storages, then prime `second` under a profile."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    adapter = _ScopedRecordSetAdapter([
        {
            "facts": [
                _node(_LSP_OLD_TARGET),
                _node(_LSP_OTHER_TARGET),
                _node(_LSP_REF),
                _node(_LSP_OTHER_REF),
                _edge(_LSP_REF, "references", _LSP_OLD_TARGET),
                _edge(_LSP_REF, "references", _LSP_OTHER_TARGET),
                _edge(_LSP_OTHER_REF, "references", _LSP_OTHER_TARGET),
            ],
            "completed_scope": True,
            "record_set": RecordSet(records={
                "pkg/utils.py": _lsp_emission(_LSP_OLD_TARGET),
                "pkg/queries.py": _lsp_queries(_LSP_OLD_TARGET),
                "pkg/storages.py": _lsp_emission(
                    _LSP_OTHER_TARGET,
                    descendants=(_LSP_OTHER_REF,),
                    edges=((_LSP_OTHER_REF, "references", _LSP_OTHER_TARGET),),
                ),
            }),
        },
        second,
    ])
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        executable_profile="discovery_crawl",
        registry_name=source,
        ownership_id=source,
        registry_binding_fingerprint="sha256:scoped-dependent-guard",
    )
    first = runner.run(adapter, mode="scope_complete")
    assert first.retractions_node == 0
    return runner, adapter


def _stored_source_cursor(dsn: str, source: str) -> Any:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT last_applied_rev FROM okg.source_watermarks "
            "WHERE source = %s",
            (source,),
        )
        row = cur.fetchone()
    return None if row is None else row[0]


def _live_source_edges(dsn: str, source: str) -> list[tuple[str, str, str]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(conn).checkout().query(
            """
            SELECT src, edge_type, dst
              FROM okg.graph_edges
             WHERE source = :source
             ORDER BY src, edge_type, dst
            """,
            {"source": source},
        )
    return [
        (str(row["src"]), str(row["edge_type"]), str(row["dst"]))
        for row in rows
    ]


_LSP_SEEDED_NODES = sorted([
    _LSP_OLD_TARGET, _LSP_OTHER_TARGET, _LSP_REF, _LSP_OTHER_REF,
])


def _lsp_repoint_slice(**overrides: Any) -> dict[str, Any]:
    records = {
        "pkg/utils.py": _lsp_emission(_LSP_NEW_TARGET),
        "pkg/queries.py": _lsp_queries(_LSP_NEW_TARGET),
    }
    records.update(overrides.pop("records", {}))
    spec = {
        "facts": [
            _node(_LSP_NEW_TARGET),
            _node(_LSP_REF),
            _edge(_LSP_REF, "references", _LSP_NEW_TARGET),
            _edge(_LSP_REF, "references", _LSP_OTHER_TARGET),
        ],
        "completed_scope": False,
        "record_set": RecordSet(records=records),
        "replacement_keys": {"pkg/utils.py", "pkg/queries.py"},
    }
    spec.update(overrides)
    return spec


def test_scoped_run_may_repoint_a_dependents_edges_into_its_scope(
    clean_runtime: str,
) -> None:
    """Issue #1539: a dependent of a changed file re-points its references.

    pkg/utils.py changed, so its symbol moved from line 10 to line 14, and
    pkg/queries.py, which imports it and lies outside the scope, must swap
    its edge to the new id. Before this change the runner refused the run
    with scoped_replacement_keys_outside_scope and the generation blocked.
    Only the stale edge and the stale symbol are retired.
    """
    source = "test.record-set-scoped-dependent"
    runner, adapter = _seed_lsp_like_source(
        clean_runtime, source, _lsp_repoint_slice(),
    )

    second = runner.run(
        adapter,
        mode="scope_complete",
        sync_scope={"scope_keys": ["pkg/utils.py"]},
    )

    assert second.retractions_node == 1
    assert second.retractions_edge == 1
    assert _live_source_nodes(clean_runtime, source) == sorted([
        _LSP_NEW_TARGET, _LSP_OTHER_TARGET, _LSP_REF, _LSP_OTHER_REF,
    ])
    assert _live_source_edges(clean_runtime, source) == sorted([
        (_LSP_REF, "references", _LSP_NEW_TARGET),
        (_LSP_REF, "references", _LSP_OTHER_TARGET),
        (_LSP_OTHER_REF, "references", _LSP_OTHER_TARGET),
    ])


def test_scoped_dependent_run_claiming_an_unrelated_file_is_refused(
    clean_runtime: str,
) -> None:
    """Issue #1539: the allowance never reaches an unrelated record.

    The same run also declares pkg/storages.py, which has no edge into the
    scope, and drops it. The whole run is refused with the 09-22 reason
    code before anything is retracted.
    """
    source = "test.record-set-scoped-dependent-unrelated"
    runner, adapter = _seed_lsp_like_source(
        clean_runtime,
        source,
        _lsp_repoint_slice(replacement_keys={
            "pkg/utils.py", "pkg/queries.py", "pkg/storages.py",
        }),
    )
    edges_before = _live_source_edges(clean_runtime, source)
    cursor_before = _stored_source_cursor(clean_runtime, source)
    assert cursor_before is not None

    with pytest.raises(SubstrateError) as excinfo:
        runner.run(
            adapter,
            mode="scope_complete",
            sync_scope={"scope_keys": ["pkg/utils.py"]},
        )

    assert excinfo.value.data["reason_code"] == (
        "scoped_replacement_keys_outside_scope"
    )
    assert excinfo.value.data["out_of_scope_replacement_keys"] == [
        "pkg/storages.py",
    ]
    assert _live_source_nodes(clean_runtime, source) == _LSP_SEEDED_NODES
    assert _live_source_edges(clean_runtime, source) == edges_before
    assert _stored_source_cursor(clean_runtime, source) == cursor_before
    assert runner.tail_failure_diagnostic["phase"] == (
        "record_set.scoped_replacement_scope"
    )


def test_scoped_dependent_dropping_an_out_of_scope_edge_is_refused(
    clean_runtime: str,
) -> None:
    """Issue #1539: a real dependent may not drop an edge outside the scope.

    pkg/queries.py re-points its edge into pkg/utils.py but also drops its
    edge into pkg/storages.py, which the scope does not name.
    """
    source = "test.record-set-scoped-dependent-overreach"
    runner, adapter = _seed_lsp_like_source(
        clean_runtime,
        source,
        _lsp_repoint_slice(records={
            "pkg/queries.py": _lsp_queries(_LSP_NEW_TARGET, keep_other=False),
        }),
    )
    cursor_before = _stored_source_cursor(clean_runtime, source)
    assert cursor_before is not None

    with pytest.raises(SubstrateError) as excinfo:
        runner.run(
            adapter,
            mode="scope_complete",
            sync_scope={"scope_keys": ["pkg/utils.py"]},
        )

    assert excinfo.value.data["reason_code"] == (
        "scoped_dependent_replacement_unverified"
    )
    assert excinfo.value.data["unverified_dependent_keys"] == [
        "pkg/queries.py",
    ]
    assert _live_source_nodes(clean_runtime, source) == _LSP_SEEDED_NODES
    assert (
        _LSP_REF, "references", _LSP_OTHER_TARGET,
    ) in _live_source_edges(clean_runtime, source)
    assert _stored_source_cursor(clean_runtime, source) == cursor_before


# Issue #2390. The github-actions reader lists only the newest `run_limit`
# workflow runs. The fake below stands in for `gh api` so the real
# `GitHubActionsSource._fetch_live` decides whether its read was complete.
def _fake_actions_api(
    monkeypatch: pytest.MonkeyPatch,
    run_ids: list[int],
) -> None:
    from urllib.parse import parse_qs, urlsplit

    import okg.substrate.library.sources.github_actions as actions_module

    def _gh_api_json(path: str) -> dict[str, Any]:
        parts = urlsplit(path)
        assert parts.path == "/repos/owner/repo/actions/runs", path
        per_page = int(parse_qs(parts.query)["per_page"][0])
        newest_first = sorted(run_ids, reverse=True)
        return {
            "total_count": len(newest_first),
            "workflow_runs": [
                {
                    "id": rid,
                    "run_attempt": 1,
                    "name": "Tests",
                    "status": "completed",
                    "conclusion": "success",
                    "head_sha": f"sha{rid}",
                    "created_at": f"2026-09-{rid:02d}T00:00:00Z",
                    "updated_at": f"2026-09-{rid:02d}T00:10:00Z",
                }
                for rid in newest_first[:per_page]
            ],
        }

    monkeypatch.setattr(actions_module, "_gh_api_json", _gh_api_json)


def _actions_source(run_limit: int) -> Any:
    from okg.substrate.library.sources.github_actions import (
        GitHubActionsSource,
    )

    return GitHubActionsSource(
        repo="owner/repo",
        run_limit=run_limit,
        include_jobs=False,
        include_artifacts=False,
        include_check_runs=False,
    )


def _live_workflow_run_ids(dsn: str, source: str) -> list[int]:
    prefix = "ci-workflow:github:owner/repo:run:"
    return sorted(
        int(node_id[len(prefix):].split(":", 1)[0])
        for node_id in _live_source_nodes(dsn, source)
        if node_id.startswith(prefix)
    )


def test_github_actions_truncated_reconcile_keeps_runs_older_than_window(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = "test.github-actions-truncated-reconcile"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
        deletion_semantics="missing_from_completed_scope",
    )

    _fake_actions_api(monkeypatch, [1, 2, 3])
    first = runner.run(_actions_source(run_limit=10), mode="reconcile")
    assert first.completed_scope is True
    assert _live_workflow_run_ids(clean_runtime, source) == [1, 2, 3]

    # Run 4 arrives. A reconcile with run_limit=2 reads only runs 4 and 3.
    _fake_actions_api(monkeypatch, [1, 2, 3, 4])
    second = runner.run(_actions_source(run_limit=2), mode="reconcile")

    assert second.retractions_node == 0
    assert second.retractions_edge == 0
    assert _live_workflow_run_ids(clean_runtime, source) == [1, 2, 3, 4]
    assert second.completed_scope is False
    assert first.next_cursor is not None
    assert second.next_cursor is not None
    # The prior whole-source footprint is carried forward rather than
    # replaced by the two-run window, so a later complete read diffs
    # against every run the source published.
    assert second.next_cursor["record_set"] == first.next_cursor["record_set"]


def test_github_actions_complete_reconcile_still_retracts_deleted_runs(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = "test.github-actions-complete-reconcile"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
        deletion_semantics="missing_from_completed_scope",
    )

    _fake_actions_api(monkeypatch, [1, 2, 3])
    runner.run(_actions_source(run_limit=10), mode="reconcile")

    # Runs 1 and 2 were deleted upstream. The listing now ends inside the
    # window, so the read is complete and their absence is real.
    _fake_actions_api(monkeypatch, [3, 4])
    second = runner.run(_actions_source(run_limit=10), mode="reconcile")

    assert second.completed_scope is True
    assert second.retractions_node == 2
    assert _live_workflow_run_ids(clean_runtime, source) == [3, 4]


# --- Issue #3301: a file deleted before an incomplete git-files run ---------


def _git_cmd(repo, *args: str) -> str:  # noqa: ANN001
    import subprocess

    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _owed_repo(tmp_path):  # noqa: ANN001, ANN202
    repo = tmp_path / "repo"
    repo.mkdir()
    _git_cmd(repo, "init", "-q", "-b", "main")
    _git_cmd(repo, "config", "user.email", "test@example.com")
    _git_cmd(repo, "config", "user.name", "OKG Test")
    (repo / "keep.cc").write_text("keep\n", encoding="utf-8")
    (repo / "other.cc").write_text("other\n", encoding="utf-8")
    _git_cmd(repo, "add", "-A")
    _git_cmd(repo, "commit", "-q", "-m", "A")
    return repo


def _owed_source(repo, *, managed: bool, include_untracked: bool = False):  # noqa: ANN001, ANN202
    from okg.substrate.library.sources.git_graph_files import GitFilesSource

    class _UnmanagedGitFilesSource(GitFilesSource):
        # The runner then manages no change identity and never skips a run
        # on the change probe.
        probe_short_circuit_safe = False

    cls = GitFilesSource if managed else _UnmanagedGitFilesSource
    return cls(
        repo="demo",
        repo_root=repo,
        include_untracked=include_untracked,
    )


def _owed_runner(dsn: str, source: str) -> SourceRunner:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)
    return SourceRunner(dsn, source_name=source)


def _source_files(dsn: str, source: str) -> set[str]:
    return {
        node_id
        for node_id in _live_source_nodes(dsn, source)
        if node_id.startswith("file:demo:")
    }


@pytest.mark.parametrize(
    ("incomplete_mode", "complete_mode", "managed"),
    [
        # Each value of each axis appears with each value of the others at
        # least once (pairwise): incomplete mode, complete mode, and whether
        # the runner manages the change identity.
        ("cursor", "reconcile", True),
        ("cursor", "scope_complete", False),
        ("scope_incomplete", "scope_complete", True),
        ("scope_incomplete", "reconcile", False),
    ],
)
def test_git_files_deleted_before_incomplete_run_is_retracted_by_complete_run(
    clean_runtime: str,
    tmp_path,
    incomplete_mode: str,
    complete_mode: str,
    managed: bool,
) -> None:
    source = f"test.git-files-owed-{incomplete_mode}-{complete_mode}"
    runner = _owed_runner(clean_runtime, source)
    repo = _owed_repo(tmp_path)
    first = runner.run(_owed_source(repo, managed=managed), mode="scope_complete")
    assert first.completed_scope is True
    assert _source_files(clean_runtime, source) == {
        "file:demo:keep.cc",
        "file:demo:other.cc",
    }

    _git_cmd(repo, "rm", "-q", "keep.cc")
    _git_cmd(repo, "commit", "-q", "-m", "B deletes keep.cc")
    incomplete = runner.run(
        _owed_source(repo, managed=managed), mode=incomplete_mode,
    )
    assert incomplete.completed_scope is False
    assert incomplete.retractions_node == 0
    assert "file:demo:keep.cc" in _source_files(clean_runtime, source)

    complete = runner.run(
        _owed_source(repo, managed=managed), mode=complete_mode,
    )
    assert complete.completed_scope is True
    assert complete.retractions_node == 1
    assert _source_files(clean_runtime, source) == {"file:demo:other.cc"}
    assert "owed_retraction_paths" not in (complete.next_cursor or {})

    # The next complete run has nothing left to retract.
    again = runner.run(_owed_source(repo, managed=managed), mode=complete_mode)
    assert again.retractions_node == 0
    assert _source_files(clean_runtime, source) == {"file:demo:other.cc"}


def test_git_files_file_readded_before_complete_run_is_read_not_retracted(
    clean_runtime: str,
    tmp_path,
) -> None:
    source = "test.git-files-owed-readded"
    runner = _owed_runner(clean_runtime, source)
    repo = _owed_repo(tmp_path)
    runner.run(_owed_source(repo, managed=True), mode="scope_complete")
    _git_cmd(repo, "rm", "-q", "keep.cc")
    _git_cmd(repo, "commit", "-q", "-m", "B deletes keep.cc")
    runner.run(_owed_source(repo, managed=True), mode="cursor")

    (repo / "keep.cc").write_text("keep v2\n", encoding="utf-8")
    _git_cmd(repo, "add", "-A")
    _git_cmd(repo, "commit", "-q", "-m", "C adds keep.cc back")
    complete = runner.run(_owed_source(repo, managed=True), mode="reconcile")

    assert complete.completed_scope is True
    assert complete.retractions_node == 0
    assert _source_files(clean_runtime, source) == {
        "file:demo:keep.cc",
        "file:demo:other.cc",
    }
    blob = _git_cmd(repo, "rev-parse", "HEAD:keep.cc")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(conn).checkout().query(
            """
            SELECT attrs->>'git_blob_sha' AS blob
              FROM okg.graph_nodes
             WHERE node_id = :node_id
            """,
            {"node_id": "file:demo:keep.cc"},
        )
    assert [row["blob"] for row in rows] == [blob]


def test_git_files_untracked_file_removed_before_incomplete_run_is_retracted(
    clean_runtime: str,
    tmp_path,
) -> None:
    source = "test.git-files-owed-untracked"
    runner = _owed_runner(clean_runtime, source)
    repo = _owed_repo(tmp_path)
    (repo / "scratch.cc").write_text("scratch\n", encoding="utf-8")
    runner.run(
        _owed_source(repo, managed=True, include_untracked=True),
        mode="scope_complete",
    )
    assert "file:demo:scratch.cc" in _source_files(clean_runtime, source)

    (repo / "scratch.cc").unlink()
    runner.run(
        _owed_source(repo, managed=True, include_untracked=True),
        mode="cursor",
    )
    assert "file:demo:scratch.cc" in _source_files(clean_runtime, source)

    complete = runner.run(
        _owed_source(repo, managed=True, include_untracked=True),
        mode="scope_complete",
    )
    assert complete.retractions_node == 1
    assert _source_files(clean_runtime, source) == {
        "file:demo:keep.cc",
        "file:demo:other.cc",
    }


def test_git_files_delete_then_complete_run_retracts_control(
    clean_runtime: str,
    tmp_path,
) -> None:
    source = "test.git-files-owed-control"
    runner = _owed_runner(clean_runtime, source)
    repo = _owed_repo(tmp_path)
    runner.run(_owed_source(repo, managed=True), mode="scope_complete")
    _git_cmd(repo, "rm", "-q", "keep.cc")
    _git_cmd(repo, "commit", "-q", "-m", "B deletes keep.cc")

    complete = runner.run(_owed_source(repo, managed=True), mode="scope_complete")
    assert complete.retractions_node == 1
    assert _source_files(clean_runtime, source) == {"file:demo:other.cc"}


def test_git_files_owed_path_that_became_a_directory_is_retracted(
    clean_runtime: str,
    tmp_path,
) -> None:
    source = "test.git-files-owed-directory"
    runner = _owed_runner(clean_runtime, source)
    repo = _owed_repo(tmp_path)
    runner.run(_owed_source(repo, managed=True), mode="scope_complete")
    _git_cmd(repo, "rm", "-q", "keep.cc")
    (repo / "keep.cc").mkdir()
    (repo / "keep.cc" / "a.cc").write_text("a\n", encoding="utf-8")
    _git_cmd(repo, "add", "-A")
    _git_cmd(repo, "commit", "-q", "-m", "B replaces keep.cc with a directory")

    from okg.substrate.library.sources import _git_helpers

    # Each run starts with fresh repository handles, as a new process would:
    # this process's cached handle keeps a stale index and would miss the
    # new keep.cc/a.cc (issue #3291, fixed separately by PR #3300).
    _git_helpers._repo_tls.cache = {}
    runner.run(_owed_source(repo, managed=True), mode="cursor")
    _git_helpers._repo_tls.cache = {}
    runner.run(_owed_source(repo, managed=True), mode="cursor")
    _git_helpers._repo_tls.cache = {}
    complete = runner.run(_owed_source(repo, managed=True), mode="reconcile")

    assert complete.completed_scope is True
    assert complete.retractions_node == 1
    assert _source_files(clean_runtime, source) == {
        "file:demo:keep.cc/a.cc",
        "file:demo:other.cc",
    }


@pytest.mark.parametrize("managed", [True, False])
def test_git_files_owed_list_past_limit_still_retracts_incomplete_run_file(
    clean_runtime: str,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    managed: bool,
) -> None:
    """A file first read by an incomplete run is in no record set the runner
    keeps, so only an owed retraction can remove it. Past the owed limit the
    incomplete run must hold the cursor rather than drop that retraction."""
    from okg.substrate.library.sources import _git_graph_files_emit, _git_helpers

    monkeypatch.setattr(
        _git_graph_files_emit, "OWED_RETRACTION_PATHS_LIMIT", 1,
    )
    source = f"test.git-files-owed-limit-{managed}"
    runner = _owed_runner(clean_runtime, source)
    repo = _owed_repo(tmp_path)

    def run(mode: str):  # noqa: ANN202
        # Fresh repository handles per run, as a new process would have:
        # this process's cached handle keeps a stale index (issue #3291).
        _git_helpers._repo_tls.cache = {}
        return runner.run(_owed_source(repo, managed=managed), mode=mode)

    run("scope_complete")
    (repo / "new.cc").write_text("new\n", encoding="utf-8")
    _git_cmd(repo, "add", "-A")
    _git_cmd(repo, "commit", "-q", "-m", "B adds new.cc")
    run("cursor")
    assert "file:demo:new.cc" in _source_files(clean_runtime, source)

    _git_cmd(repo, "rm", "-q", "keep.cc", "new.cc")
    _git_cmd(repo, "commit", "-q", "-m", "C deletes keep.cc and new.cc")
    if managed:
        # The held run binds no applied snapshot, so the runner refuses to
        # acknowledge it and keeps its durable replay latch: the next run is
        # an authoritative replay, whose stray sweep retracts every live row
        # the complete footprint no longer covers.
        with pytest.raises(
            SubstrateError, match="cannot acknowledge change identity",
        ):
            run("cursor")
    else:
        held = run("cursor")
        assert held.completed_scope is False
        assert held.retractions_node == 0
        assert held.source_health is not None
        assert held.source_health.status == "partial"
    assert "file:demo:new.cc" in _source_files(clean_runtime, source)

    if managed:
        # An ordinary cursor run is enough: the latch turns it into an
        # authoritative replay (run as the profile's replay mode).
        complete = run("cursor")
        assert complete.mode == "scope_complete"
        assert complete.clear_replay_required is True
    else:
        complete = run("reconcile")
    assert complete.completed_scope is True
    assert complete.retractions_node == 2
    assert _source_files(clean_runtime, source) == {"file:demo:other.cc"}
    if managed:
        after = run("cursor")
        assert after.retractions_node == 0
        assert _source_files(clean_runtime, source) == {"file:demo:other.cc"}


# ---------------------------------------------------------------------------
# PACT record-set-by-reference: the default inline cap is 64 KiB, so a large
# record set is stored once in okg.source_record_set_blobs and every copy of
# the cursor (watermark, accepted verdict, admission events, ingest report)
# carries only record_set_ref. A missing blob is reported, not silent.
# ---------------------------------------------------------------------------

_CAP_ENV = "OKG_SOURCE_RUNNER_INLINE_RECORD_SET_MAX_BYTES"
_OLD_CAP = "1000000"


def _json_keys(value: Any) -> list[str]:
    """Every mapping key anywhere inside ``value``."""
    keys: list[str] = []
    stack = [value]
    while stack:
        item = stack.pop()
        if isinstance(item, dict):
            keys.extend(str(k) for k in item)
            stack.extend(item.values())
        elif isinstance(item, list):
            stack.extend(item)
    return keys


def _assert_by_reference(surface: str, value: Any, sha256: str) -> None:
    keys = _json_keys(value)
    assert "record_set" not in keys, f"{surface} still holds the record set inline"
    assert "record_set_ref" in keys, f"{surface} names no record_set_ref"
    assert sha256 in json.dumps(value), f"{surface} names another blob"


def _assert_inline(surface: str, value: Any) -> None:
    keys = _json_keys(value)
    assert "record_set" in keys, f"{surface} lost the inline record set"
    assert "record_set_ref" not in keys, f"{surface} moved a small set to a blob"


def _catalog_labels(count: int, *, tag: str = "a", width: int = 100) -> tuple[str, ...]:
    # Whitespace-free labels: the catalog reader splits on whitespace.
    return tuple(f"REL_{tag}_{i:04d}_" + "x" * width for i in range(count))


def _ingest_deployment(tmp_path, dsn: str, labels: tuple[str, ...], monkeypatch):
    from tests.pytest.source_jobs_inline import use_inline_source_jobs
    from tests.substrate.runtime import test_connector_completed_scope_modes as modes
    from tests.substrate.test_connector_completed_scope_modes_e2e import _deployment

    use_inline_source_jobs(monkeypatch)
    dep, catalog = _deployment(tmp_path, dsn, "ExplicitRecordSetCatalogSource")
    modes.write_catalog(tmp_path / "releases.txt", labels)
    return dep, catalog


def _ingest_once(dsn: str, dep) -> dict:
    from tests.substrate.test_connector_completed_scope_modes_e2e import _ingest

    record, report = _ingest(dsn, dep, publish=True)
    assert record["any_failed"] is False, json.dumps({k: v for k, v in report.items() if k != "results"}, default=str)[:4000]
    assert record["admission_status"] in {"accepted", "published"}, record
    return record


def _admission_rows(dsn: str, admission_id: str) -> tuple[dict, list[dict]]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        (verdict,) = conn.execute(
            "SELECT verdict FROM okg.admission_envelopes"
            " WHERE admission_id = %s::uuid",
            (admission_id,),
        ).fetchone()
        payloads = [
            row[0]
            for row in conn.execute(
                "SELECT payload FROM okg.admission_events"
                " WHERE admission_id = %s::uuid ORDER BY event_id",
                (admission_id,),
            ).fetchall()
        ]
    return verdict, payloads


def _svc_watermark(dsn: str) -> dict:
    with psycopg.connect(dsn, autocommit=True) as conn:
        row = conn.execute(
            "SELECT last_applied_rev FROM okg.source_watermarks"
            " WHERE source = 'svc.first'"
        ).fetchone()
    assert row is not None, "the sync wrote no watermark"
    return row[0]


def _blob_bytes(dsn: str, source: str, sha256: str) -> int:
    with psycopg.connect(dsn, autocommit=True) as conn:
        row = conn.execute(
            "SELECT octet_length(record_set::text)"
            " FROM okg.source_record_set_blobs"
            " WHERE source = %s AND sha256 = %s",
            (source, sha256),
        ).fetchone()
    assert row is not None, f"no blob row for {source} {sha256}"
    return int(row[0])


def test_default_cap_moves_a_100kb_record_set_to_a_blob_everywhere(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No override: a ~100 KB set is referenced in the watermark, the
    accepted verdict's next_cursor, every admission event and the ingest
    report. Fails on a 1 MB default cap."""
    monkeypatch.delenv(_CAP_ENV, raising=False)
    dep, _ = _ingest_deployment(
        tmp_path, clean_runtime, _catalog_labels(200), monkeypatch,
    )
    record = _ingest_once(clean_runtime, dep)

    watermark = _svc_watermark(clean_runtime)
    assert watermark.get("record_set_ref") is not None, sorted(watermark)
    sha256 = watermark["record_set_ref"]["sha256"]
    _assert_by_reference("watermark", watermark, sha256)
    size = _blob_bytes(clean_runtime, "svc.first", sha256)
    assert 65_536 < size < 1_000_000, size

    verdict, payloads = _admission_rows(clean_runtime, record["admission_id"])
    _assert_by_reference(
        "verdict.source_terminal_state.next_cursor",
        verdict["source_terminal_state"]["next_cursor"],
        sha256,
    )
    _assert_by_reference("verdict", verdict, sha256)
    assert payloads, "no admission events"
    assert any("record_set_ref" in _json_keys(p) for p in payloads), payloads
    for payload in payloads:
        assert "record_set" not in _json_keys(payload), payload.keys()
    assert "record_set" not in _json_keys(record), "first ingest report"

    # The report carries the verdict for a zero-output run: rerun unchanged.
    rerun = _ingest_once(clean_runtime, dep)
    assert rerun["nodes_appended"] == 0, rerun
    assert rerun["admission_verdict"], rerun
    rerun_sha = _svc_watermark(clean_runtime)["record_set_ref"]["sha256"]
    _assert_by_reference("rerun report admission_verdict", rerun["admission_verdict"], rerun_sha)
    assert "record_set" not in _json_keys(rerun), "rerun ingest report"
    verdict, payloads = _admission_rows(clean_runtime, rerun["admission_id"])
    _assert_by_reference("rerun verdict", verdict, rerun_sha)
    for payload in payloads:
        assert "record_set" not in _json_keys(payload), payload.keys()


def test_default_cap_keeps_a_10kb_record_set_inline(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(_CAP_ENV, raising=False)
    dep, _ = _ingest_deployment(
        tmp_path, clean_runtime, _catalog_labels(18), monkeypatch,
    )
    record = _ingest_once(clean_runtime, dep)

    watermark = _svc_watermark(clean_runtime)
    _assert_inline("watermark", watermark)
    inline_bytes = len(json.dumps(watermark["record_set"]).encode("utf-8"))
    assert 5_000 < inline_bytes <= 65_536, inline_bytes
    verdict, _payloads = _admission_rows(clean_runtime, record["admission_id"])
    _assert_inline(
        "verdict.source_terminal_state.next_cursor",
        verdict["source_terminal_state"]["next_cursor"],
    )
    rerun = _ingest_once(clean_runtime, dep)
    assert rerun["nodes_appended"] == 0, rerun
    _assert_inline("rerun report admission_verdict", rerun["admission_verdict"])




_VOLATILE_ATTRS = frozenset({"parsed_at"})


def _live_rows(dsn: str, source: str) -> dict[str, Any]:
    """Live node and edge ids of ``source`` and a content hash of each row."""
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        nodes = store.query(
            "SELECT node_id, subtype, attrs FROM okg.graph_nodes"
            " WHERE source = :source ORDER BY node_id",
            {"source": source},
        )
        edges = store.query(
            "SELECT edge_id, src, edge_type, dst, attrs FROM okg.graph_edges"
            " WHERE source = :source ORDER BY edge_id",
            {"source": source},
        )
    finally:
        store.close()

    def digest(row: Any) -> str:
        content = dict(row)
        attrs = content.get("attrs")
        if isinstance(attrs, dict):
            # The wall-clock parse time differs between any two syncs.
            content["attrs"] = {
                key: value for key, value in attrs.items()
                if key not in _VOLATILE_ATTRS
            }
        return hashlib.sha256(
            json.dumps(content, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()

    return {
        "nodes": {str(row["node_id"]): digest(row) for row in nodes},
        "edges": {str(row["edge_id"]): digest(row) for row in edges},
    }


def _source_progress(dsn: str, source: str) -> dict[str, str]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        return dict(
            conn.execute(
                "SELECT key, value FROM okg.source_progress"
                " WHERE source_name = %s AND key NOT LIKE 'sync.%%'",
                (source,),
            ).fetchall()
        )


def _source_watermark(dsn: str, source: str) -> dict:
    with psycopg.connect(dsn, autocommit=True) as conn:
        row = conn.execute(
            "SELECT last_applied_rev FROM okg.source_watermarks"
            " WHERE source = %s",
            (source,),
        ).fetchone()
    assert row is not None, f"no watermark for {source}"
    return row[0]


def _sync_outcome(
    dsn: str, source: str, before: dict[str, Any], run: dict[str, Any],
) -> dict[str, Any]:
    """What one sync decided: rows, their content, what it retracted."""
    after = _live_rows(dsn, source)
    return {
        "run": run,
        "live": after,
        "retracted_nodes": sorted(set(before["nodes"]) - set(after["nodes"])),
        "retracted_edges": sorted(set(before["edges"]) - set(after["edges"])),
        "added_nodes": sorted(set(after["nodes"]) - set(before["nodes"])),
        "changed_nodes": sorted(
            key for key in set(before["nodes"]) & set(after["nodes"])
            if before["nodes"][key] != after["nodes"][key]
        ),
        "source_progress": _source_progress(dsn, source),
        "stored_record_keys": _stored_record_keys(dsn, source),
    }


def _stored_record_keys(dsn: str, source: str) -> list[str]:
    """The record keys of the record set the watermark holds, either form."""
    runner = SourceRunner(dsn, source_name=source)
    try:
        return sorted(
            runner._prior_record_set(_source_watermark(dsn, source)).records
        )
    finally:
        runner._release_conn()


def _assert_prior_form(dsn: str, source: str, arm: str) -> None:
    """The prior cursor each arm's second sync starts from."""
    watermark = _source_watermark(dsn, source)
    if arm == "old_cap":
        _assert_inline(f"{arm} prior watermark", watermark)
        return
    ref = watermark.get("record_set_ref")
    assert ref is not None, sorted(watermark)
    _assert_by_reference(f"{arm} prior watermark", watermark, ref["sha256"])
    size = _blob_bytes(dsn, source, ref["sha256"])
    assert 65_536 < size <= 1_000_000, size


def _two_arms(
    okg_migrated: str,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    scenario,
    *,
    switch_after_first: bool,
) -> dict[str, dict[str, Any]]:
    """Run ``scenario`` in a fresh database twice: kept at the old 1 MB cap,
    and at the new default (from the start, or only for the second sync)."""
    from okg.substrate.db.pool import close_all
    from tests.substrate.conftest import recreate_okg_test_database

    arms: dict[str, dict[str, Any]] = {}
    for arm in ("old_cap", "new_default"):
        recreate_okg_test_database(okg_migrated)
        # One path for both arms: sources record their input paths in rows.
        root = tmp_path / "arm"
        shutil.rmtree(root, ignore_errors=True)
        root.mkdir()
        try:
            with monkeypatch.context() as patch:
                patch.setenv(_CAP_ENV, _OLD_CAP)
                if arm == "new_default" and not switch_after_first:
                    patch.delenv(_CAP_ENV)

                def to_second_sync(patch=patch, arm=arm) -> None:
                    if arm == "new_default" and switch_after_first:
                        patch.delenv(_CAP_ENV)

                arms[arm] = scenario(okg_migrated, root, patch, arm, to_second_sync)
        finally:
            close_all()
    return arms


def _assert_same_outcome(arms: dict[str, dict[str, Any]]) -> None:
    old, new = arms["old_cap"], arms["new_default"]
    assert set(old) == set(new)
    for key in old:
        assert new[key] == old[key], key


def _catalog_switch_scenario(dsn, root, patch, arm, to_second_sync):
    from tests.substrate.runtime import test_connector_completed_scope_modes as modes

    first_labels = _catalog_labels(200)
    dep, _ = _ingest_deployment(root, dsn, first_labels, patch)
    first = _ingest_once(dsn, dep)
    _assert_inline("watermark after the old-cap sync", _svc_watermark(dsn))
    before = _live_rows(dsn, "svc.first")

    to_second_sync()
    # 20 records leave, 10 arrive.
    modes.write_catalog(root / "releases.txt", first_labels[20:] + _catalog_labels(10, tag="b"))
    second = _ingest_once(dsn, dep)
    watermark = _svc_watermark(dsn)
    if arm == "new_default":
        ref = watermark.get("record_set_ref")
        assert ref is not None, sorted(watermark)
        _assert_by_reference("watermark after the first new-default sync", watermark, ref["sha256"])
    else:
        _assert_inline("old-cap watermark", watermark)
    outcome = _sync_outcome(dsn, "svc.first", before, {
        key: second[key]
        for key in ("nodes_appended", "edges_appended", "retract_n", "retract_e")
    })
    outcome["first_nodes_appended"] = first["nodes_appended"]
    outcome["live_after_first"] = before
    return outcome


def test_switching_to_the_default_cap_keeps_graph_progress_and_retractions(
    okg_migrated: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A source whose prior cursor holds an inline set reads it, writes the
    reference form, and decides exactly what an old-cap run decides."""
    arms = _two_arms(
        okg_migrated, tmp_path, monkeypatch, _catalog_switch_scenario,
        switch_after_first=True,
    )
    old = arms["old_cap"]
    assert len(old["retracted_nodes"]) == 20, old["retracted_nodes"]
    assert len(old["added_nodes"]) == 10, old["added_nodes"]
    _assert_same_outcome(arms)


_DOC_CORPUS_SOURCE = "test.doc_corpus.by-reference"
_DOC_CORPUS_PRIOR_STATS = (
    "scope_status",
    "files_changed",
    "files_processed_unchanged",
    "files_reused_prior_record_set",
    "record_set_authority",
    "record_set_replacement_keys",
)


def _doc_corpus_scoped_scenario(dsn, root, patch, arm, to_second_sync):
    """A full doc_corpus sync, then a scoped scope_complete run that edits
    one document and deletes another. doc_corpus builds a scoped run's
    record set from the prior set only when that set is inline."""
    from tests.substrate.library.sources import test_doc_corpus_source_file_link as dc

    paths = tuple(f"doc_{i:03d}_{'p' * 40}.md" for i in range(160))
    dc._seed(dsn, predecessor=False)
    corpus = dc._corpus(root, git=False, paths=paths)
    source = dc._source(
        dsn, corpus, dc._extractors(root), predecessor=False, git_frontier=False,
    )
    runner = SourceRunner(dsn, source_name=_DOC_CORPUS_SOURCE)
    runner.run(source, mode="reconcile")
    _assert_prior_form(dsn, _DOC_CORPUS_SOURCE, arm)
    before = _live_rows(dsn, _DOC_CORPUS_SOURCE)

    to_second_sync()
    (corpus / paths[0]).write_text(f"# {paths[0]}\n\nA new body.\n")
    (corpus / paths[1]).unlink()
    scoped = runner.run(
        source,
        mode="scope_complete",
        sync_scope={"scope_keys": [paths[0], paths[1]]},
    )
    return _sync_outcome(dsn, _DOC_CORPUS_SOURCE, before, {
        "completed_scope": scoped.completed_scope,
        "retractions_node": scoped.retractions_node,
        "retractions_edge": scoped.retractions_edge,
        # How the source treated its prior set: reused, rebuilt, or
        # replaced by key (doc_corpus reads the prior set only inline).
        "source_stats": {
            key: source.last_run_stats.get(key)
            for key in _DOC_CORPUS_PRIOR_STATS
        },
    })


def test_doc_corpus_scoped_run_over_a_referenced_prior_matches_the_old_cap(
    okg_migrated: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    arms = _two_arms(
        okg_migrated, tmp_path, monkeypatch, _doc_corpus_scoped_scenario,
        switch_after_first=False,
    )
    old = arms["old_cap"]
    assert old["run"]["completed_scope"] is True, old["run"]
    assert any(":doc_001_" in node for node in old["retracted_nodes"]), old
    _assert_same_outcome(arms)


_AGENT_SESSIONS_SOURCE = "test.agent-sessions.by-reference"


def _agent_sessions_scoped_scenario(dsn, root, patch, arm, to_second_sync):
    """A full agent_sessions sync with one long transcript, then a scoped
    scope_complete run over that unchanged transcript. agent_sessions
    completes a scoped run only from an inline prior set. (A changed
    in-scope transcript is refused at either cap: its record key, the
    session id, is not its scope key, the transcript path.)"""
    from okg.substrate.library.sources.agent_sessions import AgentSessionsSource
    from tests.substrate.library.sources import test_session_source_sync_db as ss
    from okg_self_micro import materialize  # type: ignore

    fixture = materialize(root)
    projects_root = fixture.sessions_root.parent
    alpha = fixture.sessions_root / "alpha-001.jsonl"
    original = alpha.read_text(encoding="utf-8").rstrip("\n")
    added = [
        json.dumps({
            "type": "user",
            "timestamp": f"2026-05-05T{12 + i // 3600:02d}:{(i // 60) % 60:02d}:{i % 60:02d}Z",
            "message": {"content": f"message {i:04d} " + "m" * 60},
        })
        for i in range(600)
    ]
    alpha.write_text("\n".join([original, *added]) + "\n", encoding="utf-8")

    def source() -> AgentSessionsSource:
        return AgentSessionsSource(
            repo="okg",
            claude_projects_root=projects_root,
            since="all",
            cost_estimation=False,
        )

    ss._seed_catalog_and_external_nodes(dsn, source().run("preview").facts)
    runner = SourceRunner(dsn, source_name=_AGENT_SESSIONS_SOURCE)
    baseline = runner.run(source(), mode="scope_complete")
    assert baseline.completed_scope is True
    _assert_prior_form(dsn, _AGENT_SESSIONS_SOURCE, arm)
    before = _live_rows(dsn, _AGENT_SESSIONS_SOURCE)

    to_second_sync()
    scoped = runner.run(
        source(),
        mode="scope_complete",
        sync_scope={
            "scope_keys": [alpha.relative_to(projects_root).as_posix()],
        },
    )
    return _sync_outcome(dsn, _AGENT_SESSIONS_SOURCE, before, {
        "completed_scope": scoped.completed_scope,
        "retractions_node": scoped.retractions_node,
        "retractions_edge": scoped.retractions_edge,
    })


def test_agent_sessions_scoped_run_over_a_referenced_prior_matches_the_old_cap(
    okg_migrated: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    arms = _two_arms(
        okg_migrated, tmp_path, monkeypatch, _agent_sessions_scoped_scenario,
        switch_after_first=False,
    )
    old = arms["old_cap"]
    assert old["run"]["completed_scope"] is True, old["run"]
    _assert_same_outcome(arms)


def test_a_carried_forward_inline_prior_is_stored_by_reference_again(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The adapter sees the referenced prior set inline and carries it
    forward; the stored cursor names the reference again, not the set."""
    from okg.substrate.library.sources.base import (
        preserve_cursor_record_set_authority,
    )

    monkeypatch.delenv(_CAP_ENV, raising=False)
    source = "test.record-set-carry-forward"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    facts, record_set = _large_record_set(200)
    seen: list[Any] = []

    class _CarryForward:
        name = "carry-forward"
        profile = "discovery_crawl"

        def __init__(self) -> None:
            self.first = True

        def run(self, run_id, *, mode="cursor", cursor=None, since_progress=None):
            del run_id, since_progress
            seen.append(cursor)
            if self.first:
                self.first = False
                return SourceRun(
                    facts=facts, completed_scope=True, next_cursor={},
                    run_mode=mode, record_set=record_set,
                )
            return SourceRun(
                facts=[],
                completed_scope=False,
                next_cursor=preserve_cursor_record_set_authority({}, cursor),
                run_mode=mode,
            )

    adapter = _CarryForward()
    runner = SourceRunner(clean_runtime, source_name=source)
    first = runner.run(adapter, mode="scope_complete")
    ref = first.next_cursor["record_set_ref"]
    assert 65_536 < _blob_bytes(clean_runtime, source, ref["sha256"]) <= 1_000_000

    second = runner.run(adapter, mode="cursor")
    # The adapter saw the set inline, as at the old cap, and no reference.
    assert seen[1]["record_set"] == record_set.to_cursor()
    assert "record_set_ref" not in seen[1]
    # What was stored is the reference form.
    watermark = _source_watermark(clean_runtime, source)
    _assert_by_reference("carried-forward watermark", watermark, ref["sha256"])
    assert "record_set" not in _json_keys(second.next_cursor or {})


def test_a_referenced_prior_over_the_adapter_limit_reaches_the_adapter_by_reference(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(_CAP_ENV, raising=False)
    monkeypatch.setattr(runner_module, "_ADAPTER_INLINE_PRIOR_RECORD_SET_MAX_BYTES", 65_536)
    source = "test.record-set-over-adapter-limit"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    facts, record_set = _large_record_set(200)
    adapter = _RecordSetAdapter([(facts, record_set), (facts, record_set)])
    runner = SourceRunner(clean_runtime, source_name=source)
    runner.run(adapter, mode="scope_complete")
    runner.run(adapter, mode="scope_complete")
    assert adapter.seen_cursors[1].get("record_set") is None
    assert adapter.seen_cursors[1]["record_set_ref"]["storage"] == (
        "okg.source_record_set_blobs"
    )


def test_a_non_integer_cap_override_warns_and_uses_the_default(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setenv(_CAP_ENV, "64k")
    with caplog.at_level("WARNING", logger=runner_module.__name__):
        assert SourceRunner._inline_record_set_max_bytes() == 65_536
    assert any("64k" in record.getMessage() for record in caplog.records)


def _large_record_set(count: int) -> tuple[list[NodeFact], RecordSet]:
    records: dict[str, RecordEmission] = {}
    facts: list[NodeFact] = []
    for i in range(count):
        node_id = f"doc:{i:04d}:" + "y" * 100
        facts.append(_node(node_id))
        records[f"docs/{i:04d}-" + "z" * 100 + ".md"] = RecordEmission(
            primary=RecordNode(node_id, "concept"),
        )
    return facts, RecordSet(records=records)


def test_missing_prior_blob_is_reported_by_the_run_and_by_doctor(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A deleted record-set blob: doctor names the source and sha256, and the
    next run still proceeds but reports prior_record_set_missing."""
    from okg.substrate.cli import doctor as doctor_cli
    from okg.substrate.ingest.runner import prior_record_set_report_fields

    monkeypatch.delenv(_CAP_ENV, raising=False)
    source = "test.record-set-missing-blob"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    facts, record_set = _large_record_set(200)
    adapter = _RecordSetAdapter([(facts, record_set), (facts, record_set)])
    runner = SourceRunner(clean_runtime, source_name=source, write_fact_log=True)

    first = runner.run(adapter, mode="scope_complete")
    assert first.prior_record_set_missing is None
    assert "prior_record_set_missing" not in first.as_dict()
    sha256 = first.next_cursor["record_set_ref"]["sha256"]

    report: dict = {"ok": True, "errors": []}
    clean = doctor_cli._default_doctor_record_set_blobs(clean_runtime, report)
    assert clean["ok"] is True and clean["missing"] == [], clean
    assert report.get("warnings", []) == []

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        deleted = conn.execute(
            "DELETE FROM okg.source_record_set_blobs"
            " WHERE source = %s AND sha256 = %s",
            (source, sha256),
        ).rowcount
    assert deleted == 1

    report = {"ok": True, "errors": []}
    found = doctor_cli._default_doctor_record_set_blobs(clean_runtime, report)
    assert found["ok"] is False
    assert [(m["source"], m["sha256"]) for m in found["missing"]] == [(source, sha256)]
    (warning,) = [
        w for w in report["warnings"]
        if w["code"] == "source_record_set_blob_missing"
    ]
    assert source in warning["message"] and sha256 in warning["message"]
    assert "last_applied_rev" in warning["rev_columns"]
    assert "next run of this source reads no prior record set" in warning["message"]

    second = runner.run(adapter, mode="scope_complete")
    expected = {
        "source": source,
        "sha256": sha256,
        "reason": "okg_source_record_set_blobs_row_missing",
    }
    assert second.prior_record_set_missing == expected
    assert second.as_dict()["prior_record_set_missing"] == expected
    assert prior_record_set_report_fields(second) == {
        "prior_record_set_missing": expected,
    }
    # The run proceeded (as before) and wrote a fresh, reachable blob.
    assert second.next_cursor["record_set_ref"]["sha256"] == sha256
    report = {"ok": True, "errors": []}
    after = doctor_cli._default_doctor_record_set_blobs(clean_runtime, report)
    assert after["missing"] == [], after


def test_doctor_names_a_blob_only_a_published_rev_still_needs(
    clean_runtime: str,
) -> None:
    """A missing blob named only by last_published_rev is reported without
    claiming the next run reads no prior set."""
    from okg.substrate.cli import doctor as doctor_cli

    ref = {
        "schema": "okg.source_runner.record_set_ref.v1",
        "storage": "okg.source_record_set_blobs",
        "source": "test.published-only",
        "sha256": "f" * 64,
    }
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO okg.source_watermarks (source, last_published_rev)"
            " VALUES (%s, %s::jsonb)",
            ("test.published-only", json.dumps({"record_set_ref": ref})),
        )
    report: dict = {"ok": True, "errors": []}
    found = doctor_cli._default_doctor_record_set_blobs(clean_runtime, report)
    assert found["missing"] == [{
        "source": "test.published-only",
        "sha256": "f" * 64,
        "rev_columns": ["last_published_rev"],
    }]
    (warning,) = report["warnings"]
    assert "next run" not in warning["message"]
    assert "restore" in warning["message"]


def test_missing_blob_reaches_the_ingest_report_and_plain_doctor_json(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Through the ingest service and the `okg doctor --json` command: the
    per-source report row carries prior_record_set_missing, and doctor's
    record_set_blobs check names the source and sha256."""
    from okg.substrate.cli import main as cli

    monkeypatch.setenv(_CAP_ENV, "65536")
    dep, _ = _ingest_deployment(
        tmp_path, clean_runtime, _catalog_labels(200), monkeypatch,
    )
    first = _ingest_once(clean_runtime, dep)
    assert "prior_record_set_missing" not in first
    sha256 = _svc_watermark(clean_runtime)["record_set_ref"]["sha256"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        deleted = conn.execute(
            "DELETE FROM okg.source_record_set_blobs"
            " WHERE source = 'svc.first' AND sha256 = %s",
            (sha256,),
        ).rowcount
    assert deleted == 1

    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(dep.parent))
    monkeypatch.setenv("OKG_DSN", clean_runtime)
    capsys.readouterr()
    cli.main(["doctor", "--deployment", str(dep), "--dsn", clean_runtime, "--json"])
    doctor = json.loads(capsys.readouterr().out)
    check = doctor["record_set_blobs"]
    assert [(m["source"], m["sha256"]) for m in check["missing"]] == [
        ("svc.first", sha256),
    ], check
    assert any(
        w["code"] == "source_record_set_blob_missing"
        and w["source"] == "svc.first" and w["sha256"] == sha256
        for w in doctor.get("warnings", [])
    )

    from tests.substrate.test_connector_completed_scope_modes_e2e import _ingest

    record, _report = _ingest(clean_runtime, dep, publish=True)
    assert record["prior_record_set_missing"] == {
        "source": "svc.first",
        "sha256": sha256,
        "reason": "okg_source_record_set_blobs_row_missing",
    }, {key: record.get(key) for key in ("status", "admission_status", "error")}


def _ingest_then_lose_the_blob(dsn: str, tmp_path, monkeypatch) -> tuple[Any, str, str]:
    """One accepted (unpublished) sync of a ~100 KB set, then its blob row
    deleted. Returns the deployment, the catalog path and the sha256."""
    from tests.substrate.test_connector_completed_scope_modes_e2e import _ingest

    monkeypatch.setenv(_CAP_ENV, "65536")
    dep, catalog = _ingest_deployment(tmp_path, dsn, _catalog_labels(200), monkeypatch)
    first, _report = _ingest(dsn, dep, publish=False)
    assert first["any_failed"] is False, first
    sha256 = _svc_watermark(dsn)["record_set_ref"]["sha256"]
    with psycopg.connect(dsn, autocommit=True) as conn:
        deleted = conn.execute(
            "DELETE FROM okg.source_record_set_blobs"
            " WHERE source = 'svc.first' AND sha256 = %s",
            (sha256,),
        ).rowcount
    assert deleted == 1
    return dep, catalog, sha256


def _expected_missing(sha256: str) -> dict[str, str]:
    return {
        "source": "svc.first",
        "sha256": sha256,
        "reason": "okg_source_record_set_blobs_row_missing",
    }


def test_missing_blob_reaches_the_fail_row_of_a_sync_that_raises(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The runner notes the unreachable blob before the adapter reads; the
    adapter then raises (its input is gone), so no RunResult exists. The
    exception FAIL row still carries prior_record_set_missing."""
    from pathlib import Path

    from tests.substrate.test_connector_completed_scope_modes_e2e import _ingest

    dep, catalog, sha256 = _ingest_then_lose_the_blob(clean_runtime, tmp_path, monkeypatch)
    Path(catalog).unlink()

    record, _report = _ingest(clean_runtime, dep, publish=False)
    assert record["status"] == "FAIL", record
    assert "admission_reject_reason" not in record, record
    assert record["prior_record_set_missing"] == _expected_missing(sha256), {
        key: record.get(key) for key in ("stage", "error")
    }


def test_missing_blob_reaches_the_fail_row_of_an_admission_rejection(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The next sync reads an empty input and claims no complete scope; its
    only earlier run was never published, so admission rejects it
    (`prior_emission_unpublished`). That FAIL row carries prior_record_set_missing too."""
    from pathlib import Path

    from tests.substrate.runtime import test_connector_completed_scope_modes as modes
    from tests.substrate.test_connector_completed_scope_modes_e2e import _ingest

    dep, catalog, sha256 = _ingest_then_lose_the_blob(clean_runtime, tmp_path, monkeypatch)
    modes.write_catalog(Path(catalog), ())

    record, _report = _ingest(clean_runtime, dep, publish=False)
    assert record["status"] == "FAIL", record
    assert record["admission_reject_reason"] == "prior_emission_unpublished", record
    assert record["prior_record_set_missing"] == _expected_missing(sha256)


# ---------------------------------------------------------------------------
# PACT record-set-blob-autoprune: after each accepted run okg deletes the
# source's record-set and cursor blobs that no watermark revision and no held
# run names, once they are older than the grace window
# (deployment.yaml source_blob_prune_grace_seconds; 0 turns it off).
# ---------------------------------------------------------------------------

_SHA_RE = re.compile(r"[0-9a-f]{64}")


def _set_grace(dsn: str, dep, value: int | None) -> None:
    """Write (or drop) the grace setting and re-claim the fixture catalog."""
    import yaml

    from tests.substrate.test_ingest_service import _claim_layered_fixture_catalog

    path = dep / "deployment.yaml"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if value is None:
        data.pop("source_blob_prune_grace_seconds", None)
    else:
        data["source_blob_prune_grace_seconds"] = value
    path.write_text(yaml.safe_dump(data), encoding="utf-8")
    _claim_layered_fixture_catalog(dsn, dep)


def _blob_rows(dsn: str, source: str = "svc.first") -> dict[str, set]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        record_sets = {
            row[0] for row in conn.execute(
                "SELECT sha256 FROM okg.source_record_set_blobs WHERE source = %s",
                (source,),
            )
        }
        piece_writes = {
            str(row[0]) for row in conn.execute(
                "SELECT DISTINCT write_id FROM okg.source_record_set_blob_pieces"
                " WHERE source = %s",
                (source,),
            )
        }
        header_writes = {
            str(row[0]) for row in conn.execute(
                "SELECT piece_write_id FROM okg.source_record_set_blobs"
                " WHERE source = %s AND piece_write_id IS NOT NULL",
                (source,),
            )
        }
        present = conn.execute(
            "SELECT to_regclass('okg.source_cursor_blobs') IS NOT NULL"
        ).fetchone()[0]
        cursors = {
            (row[0], row[1]) for row in conn.execute(
                "SELECT cursor_key, sha256 FROM okg.source_cursor_blobs"
                " WHERE source = %s",
                (source,),
            )
        } if present else set()
    return {
        "record_sets": record_sets,
        "piece_writes": piece_writes,
        "header_writes": header_writes,
        "cursors": cursors,
    }


def _watermark_shas(dsn: str, source: str = "svc.first") -> set[str]:
    with psycopg.connect(dsn, autocommit=True) as conn:
        row = conn.execute(
            "SELECT concat_ws(' ', last_applied_rev::text,"
            " cutover_watermark::text, last_published_rev::text)"
            " FROM okg.source_watermarks WHERE source = %s",
            (source,),
        ).fetchone()
        named = set(_SHA_RE.findall(row[0] if row else ""))
        # A large cursor field (record_set_ref included) is itself stored as
        # a cursor blob; what it names is named too.
        nested = conn.execute(
            "SELECT string_agg(value::text, ' ') FROM okg.source_cursor_blobs"
            " WHERE source = %s AND sha256 = ANY(%s::text[])",
            (source, sorted(named)),
        ).fetchone()[0]
    return named | set(_SHA_RE.findall(nested or ""))


def _age_blobs(dsn: str, source: str = "svc.first", interval: str = "1 day") -> None:
    """Make every blob row of ``source`` look ``interval`` old."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        for table in ("source_record_set_blobs", "source_cursor_blobs"):
            if conn.execute(
                "SELECT to_regclass(%s) IS NOT NULL", (f"okg.{table}",),
            ).fetchone()[0]:
                conn.execute(
                    f"UPDATE okg.{table}"
                    " SET updated_at = now() - %s::interval WHERE source = %s",
                    (interval, source),
                )


class _WorkerChildLock:
    """The two job hooks the service calls, as a worker child has them: the
    run lock is a real session lock held on its own connection."""

    def __init__(self, dsn: str, deployment: str) -> None:
        self.dsn, self.deployment = dsn, deployment
        self.conn: Any = None
        self.lock_key: str | None = None

    def acquire_run_lock(self, source: str) -> None:
        from okg.substrate.ingest.source_jobs import run_lock_key

        self.conn = psycopg.connect(self.dsn, autocommit=True)
        key = run_lock_key(self.deployment, source)
        (got,) = self.conn.execute(
            "SELECT pg_try_advisory_lock(hashtextextended(%s, 0))", (key,),
        ).fetchone()
        assert got, "the run lock is already held"
        self.lock_key = key

    def started(self, **_payload: Any) -> None:
        return None

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()


def _run_as_worker_children(dsn: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the inline source runtime run each source with worker-child hooks."""
    from okg.substrate.ingest import service as ingest_service

    real = ingest_service._run_source

    def run_as_child(options, entry, name, **kwargs):
        hooks = _WorkerChildLock(dsn, options.deployment)
        try:
            return real(options, entry, name, job_hooks=hooks, **kwargs)
        finally:
            hooks.close()

    monkeypatch.setattr(ingest_service, "_run_source", run_as_child)


_SYNC_COUNTER = iter(range(1, 1_000_000))


def _changed_sync(dsn: str, dep, catalog) -> dict:
    """One accepted sync after one catalog line changed, so the record set
    (and its blob sha256) is new, as on a live source."""
    from pathlib import Path

    from tests.substrate.runtime import test_connector_completed_scope_modes as modes

    changed = f"REL_v{next(_SYNC_COUNTER):06d}_" + "x" * 100
    modes.write_catalog(Path(catalog), _catalog_labels(199) + (changed,))
    return _ingest_once(dsn, dep)


def _seed_orphan_blob(dsn: str, source: str, sha256: str, *, age: str = "1 day") -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO okg.source_record_set_blobs"
            " (source, sha256, record_set, updated_at)"
            " VALUES (%s, %s, %s::jsonb, now() - %s::interval)",
            (source, sha256, json.dumps({"records": {"x": "y" * 500}}), age),
        )


def test_autoprune_leaves_exactly_the_watermark_blobs_after_several_syncs(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Several accepted syncs of a ~100 KB set (stored in pieces, with
    externalized cursor fields), run as worker children holding the run
    lock: each sync's prune deletes every blob that is older than the window
    and not named by the watermark; the record-set, piece and cursor tables
    end holding exactly the watermark's blobs."""
    monkeypatch.delenv(_CAP_ENV, raising=False)
    monkeypatch.setenv("OKG_SOURCE_RUNNER_RECORD_SET_JSONB_MAX_BYTES", "1")
    monkeypatch.setenv("OKG_SOURCE_RUNNER_INLINE_CURSOR_FIELD_MAX_BYTES", "100")
    dep, catalog = _ingest_deployment(
        tmp_path, clean_runtime, _catalog_labels(200), monkeypatch,
    )
    _run_as_worker_children(clean_runtime, monkeypatch)
    _set_grace(clean_runtime, dep, 3600)
    reports = []
    written: set[str] = set()
    for _ in range(4):
        reports.append(_changed_sync(clean_runtime, dep, catalog))
        written |= _blob_rows(clean_runtime)["record_sets"]
        # Two hours pass before the next sync.
        _age_blobs(clean_runtime, interval="2 hours")
    reports.append(_changed_sync(clean_runtime, dep, catalog))
    written |= _blob_rows(clean_runtime)["record_sets"]

    assert len(written) == 5, written
    prunes = [r["record_set_blob_prune"] for r in reports]
    assert all(p["status"] == "ok" for p in prunes), prunes
    assert sum(p["deleted"]["source_record_set_blobs"]["rows"] for p in prunes) >= 2, prunes
    assert sum(p["deleted"]["source_record_set_blobs"]["pieces"] for p in prunes) >= 2, prunes
    assert all(p["deleted_bytes"] >= 0 for p in prunes)

    rows = _blob_rows(clean_runtime)
    named = _watermark_shas(clean_runtime)
    # Besides the watermark's own blobs, only the last run's run-start blob
    # remains: the run pinned it, so it is younger than the window.
    assert named & written <= rows["record_sets"] <= written, (rows, named)
    assert len(rows["record_sets"] - named) <= 1, (rows, named)
    assert rows["piece_writes"] == rows["header_writes"] != set()
    # Here record_set_ref itself is a cursor blob (the 100-byte field cap).
    assert _svc_watermark(clean_runtime)["record_set_ref"]["storage"] == (
        "okg.source_cursor_blobs"
    )
    assert rows["cursors"], "the run wrote no cursor blob"
    # With no window at all exactly the watermark's blobs stay, the record-set
    # blob named only inside the record_set_ref cursor blob included, and the
    # next sync reads that prior set.
    from okg.substrate.ingest import source_blob_prune

    _age_blobs(clean_runtime)
    zero = source_blob_prune.prune_source_blobs_after_accept(
        clean_runtime, deployment="svc", source="svc.first", grace_seconds=0,
    )
    assert zero["status"] == "ok", zero
    exact = _blob_rows(clean_runtime)
    assert exact["record_sets"] == written & named, (exact, named)
    assert exact["piece_writes"] == exact["header_writes"] != set()
    assert {sha for _key, sha in exact["cursors"]} <= named, exact["cursors"]
    assert exact["cursors"]
    nxt = _changed_sync(clean_runtime, dep, catalog)
    assert "prior_record_set_missing" not in nxt, nxt


def test_autoprune_grace_zero_deletes_every_unnamed_blob_and_caps_rows_and_bytes(
    clean_runtime: str,
) -> None:
    """The prune function itself: grace 0 deletes every unnamed row; the row
    cap and the byte cap bound one prune; named rows always stay."""
    from okg.substrate.ingest import source_blob_prune

    source = "test.autoprune-caps"
    named = "a" * 64
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO okg.source_watermarks (source, last_applied_rev)"
            " VALUES (%s, %s::jsonb)",
            (source, json.dumps({"record_set_ref": {"sha256": named}})),
        )
    for index, sha in enumerate([named] + [f"{i:064x}" for i in range(1, 7)]):
        _seed_orphan_blob(clean_runtime, source, sha, age=f"{10 - index} hours")
    # A just-written unnamed blob, newer than any window but zero.
    _seed_orphan_blob(clean_runtime, source, "b" * 64, age="0 seconds")

    def prune(**kwargs):
        return source_blob_prune.prune_source_blobs_after_accept(
            clean_runtime, deployment="svc", source=source, **kwargs,
        )

    capped = prune(grace_seconds=3600, max_rows=2)
    assert capped["deleted"]["source_record_set_blobs"]["rows"] == 2
    assert capped["capped"] is True
    one = prune(grace_seconds=3600, max_bytes=1)
    assert one["deleted_rows"] == 1 and one["capped"] is True, one
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        sizes = [
            int(row[0]) for row in conn.execute(
                "SELECT pg_column_size(b.record_set)"
                " + pg_column_size(b.record_set_summary)"
                " FROM okg.source_record_set_blobs b"
                " WHERE source = %s AND sha256 NOT IN (%s, %s)"
                " ORDER BY updated_at, sha256",
                (source, named, "b" * 64),
            )
        ]
    assert len(sizes) == 3, sizes
    two = prune(grace_seconds=3600, max_bytes=sizes[0] + sizes[1])
    assert two["deleted_rows"] == 2, two
    assert two["deleted_bytes"] == sizes[0] + sizes[1], (two, sizes)
    windowed = prune(grace_seconds=3600)
    assert windowed["deleted_rows"] == 1 and windowed["capped"] is False, windowed
    assert _blob_rows(clean_runtime, source)["record_sets"] == {named, "b" * 64}
    zero = prune(grace_seconds=0)
    assert zero["deleted_rows"] == 1, zero
    assert _blob_rows(clean_runtime, source)["record_sets"] == {named}


def test_autoprune_skips_while_another_session_holds_the_run_lock(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Another holder of the source's run lock: the prune reports
    skipped_locked, in the function and in the ingest report, and deletes
    nothing; a caller that holds the lock itself (a worker child) prunes."""
    from okg.substrate.ingest import source_blob_prune
    from okg.substrate.ingest.source_jobs import run_lock_key

    monkeypatch.delenv(_CAP_ENV, raising=False)
    dep, _ = _ingest_deployment(
        tmp_path, clean_runtime, _catalog_labels(200), monkeypatch,
    )
    _set_grace(clean_runtime, dep, 3600)
    _ingest_once(clean_runtime, dep)
    _seed_orphan_blob(clean_runtime, "svc.first", "c" * 64)
    with psycopg.connect(clean_runtime, autocommit=True) as holder:
        holder.execute(
            "SELECT pg_advisory_lock(hashtextextended(%s, 0))",
            (run_lock_key("svc", "svc.first"),),
        )
        record = _ingest_once(clean_runtime, dep)
        assert record["record_set_blob_prune"] == {
            "status": "skipped_locked", "grace_seconds": 3600, "lock": "try_lock",
        }
        direct = source_blob_prune.prune_source_blobs_after_accept(
            clean_runtime, deployment="svc", source="svc.first", grace_seconds=0,
        )
        assert direct["status"] == "skipped_locked"
        assert "c" * 64 in _blob_rows(clean_runtime)["record_sets"]
        held = source_blob_prune.prune_source_blobs_after_accept(
            clean_runtime, deployment="svc", source="svc.first",
            grace_seconds=3600, lock_held=True,
        )
    assert held["status"] == "ok" and held["lock"] == "held_by_run", held
    assert "c" * 64 not in _blob_rows(clean_runtime)["record_sets"]


def test_autoprune_default_window_keeps_recent_blobs_and_zero_turns_it_off(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No setting: a 6-hour window, so recent superseded blobs stay and blobs
    older than 6 hours go. Setting 0: no prune at all."""
    monkeypatch.delenv(_CAP_ENV, raising=False)
    dep, catalog = _ingest_deployment(
        tmp_path, clean_runtime, _catalog_labels(200), monkeypatch,
    )
    first = _changed_sync(clean_runtime, dep, catalog)
    _changed_sync(clean_runtime, dep, catalog)
    third = _changed_sync(clean_runtime, dep, catalog)
    assert first["record_set_blob_prune"]["grace_seconds"] == 21600
    assert third["record_set_blob_prune"]["deleted_rows"] == 0, third
    recent = _blob_rows(clean_runtime)["record_sets"]
    assert len(recent) >= 2, recent

    _seed_orphan_blob(clean_runtime, "svc.first", "d" * 64, age="7 hours")
    _seed_orphan_blob(clean_runtime, "svc.first", "e" * 64, age="5 hours")
    fourth = _changed_sync(clean_runtime, dep, catalog)
    assert fourth["record_set_blob_prune"]["deleted_rows"] == 1, fourth
    rows = _blob_rows(clean_runtime)["record_sets"]
    assert "d" * 64 not in rows and "e" * 64 in rows
    assert recent <= rows

    _set_grace(clean_runtime, dep, 0)
    _age_blobs(clean_runtime)
    before = _blob_rows(clean_runtime)["record_sets"]
    off = _changed_sync(clean_runtime, dep, catalog)
    assert off["record_set_blob_prune"] == {"status": "disabled", "grace_seconds": 0}
    assert before <= _blob_rows(clean_runtime)["record_sets"]


def test_cursor_blob_reuse_refreshes_updated_at(clean_runtime: str) -> None:
    source = "test.cursor-blob-reuse"
    runner = SourceRunner(clean_runtime, source_name=source, write_fact_log=True)
    value = json.dumps({"k": "v" * 300}).encode("utf-8")
    digest = hashlib.sha256(value).hexdigest()
    runner._store_external_cursor_fields([("fact_hashes", value, digest, len(value))])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.source_cursor_blobs"
            " SET updated_at = now() - interval '1 day' WHERE source = %s",
            (source,),
        )
    runner._store_external_cursor_fields([("fact_hashes", value, digest, len(value))])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        (age,) = conn.execute(
            "SELECT now() - updated_at < interval '1 hour'"
            " FROM okg.source_cursor_blobs WHERE source = %s",
            (source,),
        ).fetchone()
    assert age is True


def test_autoprune_keeps_the_run_start_blob_an_overlapping_run_restores(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An inline run snapshots the watermark (its run-start blob is old),
    another run of the source is accepted and prunes, then the inline run is
    rejected and writes its snapshot back: the restored watermark's blob is
    still there and the next sync reads it (no prior_record_set_missing)."""
    from okg.substrate.ingest import source_blob_prune
    from okg.substrate.ingest.compensation import (
        restore_source_state,
        snapshot_source_state,
    )

    monkeypatch.delenv(_CAP_ENV, raising=False)
    dep, catalog = _ingest_deployment(
        tmp_path, clean_runtime, _catalog_labels(200), monkeypatch,
    )
    _set_grace(clean_runtime, dep, 3600)
    _changed_sync(clean_runtime, dep, catalog)
    start_sha = _svc_watermark(clean_runtime)["record_set_ref"]["sha256"]
    _age_blobs(clean_runtime)

    # The inline run starts: the service snapshots, then pins.
    snapshot = snapshot_source_state(clean_runtime, deployment="svc", source="svc.first")
    source_blob_prune.pin_snapshot_blobs(clean_runtime, snapshot)
    # A worker run is accepted meanwhile and prunes under its run lock.
    _run_as_worker_children(clean_runtime, monkeypatch)
    other = _changed_sync(clean_runtime, dep, catalog)
    assert other["record_set_blob_prune"]["status"] == "ok", other
    assert other["record_set_blob_prune"]["lock"] == "held_by_run", other
    assert _svc_watermark(clean_runtime)["record_set_ref"]["sha256"] != start_sha
    # The inline run is rejected: its run-start state is written back.
    restore_source_state(clean_runtime, snapshot)
    assert _svc_watermark(clean_runtime)["record_set_ref"]["sha256"] == start_sha
    assert start_sha in _blob_rows(clean_runtime)["record_sets"]
    after = _changed_sync(clean_runtime, dep, catalog)
    assert "prior_record_set_missing" not in after, after


def test_autoprune_keeps_a_blob_written_by_a_run_not_yet_accepted(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A second run of the source writes its blob before the first run's
    acceptance and prune: the window keeps it, so it is there when that run
    is accepted."""
    monkeypatch.delenv(_CAP_ENV, raising=False)
    dep, catalog = _ingest_deployment(
        tmp_path, clean_runtime, _catalog_labels(200), monkeypatch,
    )
    _set_grace(clean_runtime, dep, 3600)
    _changed_sync(clean_runtime, dep, catalog)
    _age_blobs(clean_runtime)
    # An old orphan (deleted) and the other run's just-written blob (kept).
    _seed_orphan_blob(clean_runtime, "svc.first", "0" * 64, age="1 day")
    _seed_orphan_blob(clean_runtime, "svc.first", "f" * 64, age="1 second")
    record = _changed_sync(clean_runtime, dep, catalog)
    assert record["record_set_blob_prune"]["deleted_rows"] == 1, record
    rows = _blob_rows(clean_runtime)["record_sets"]
    assert "0" * 64 not in rows and "f" * 64 in rows


def _hold_for_review(dsn: str, dep, monkeypatch: pytest.MonkeyPatch) -> None:
    import yaml

    from okg.substrate.runtime import admission_workflows
    from tests.substrate.test_ingest_service import _claim_layered_fixture_catalog

    registry_path = dep / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    registry["sources"]["first"]["admission_policy"]["admission_mode"] = "review"
    registry_path.write_text(yaml.safe_dump(registry), encoding="utf-8")
    _claim_layered_fixture_catalog(dsn, dep)
    monkeypatch.setattr(admission_workflows, "_resolve_dsn", lambda _name: dsn)
    # The approval path resolves the deployment by name for its grace window.
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(dep.parent))


def _held_envelope(dsn: str) -> dict:
    with psycopg.connect(dsn, autocommit=True) as conn:
        admission_id, status, verdict = conn.execute(
            "SELECT admission_id::text, status, verdict FROM okg.admission_envelopes"
            " WHERE source = 'svc.first' AND intent = 'source_sync'"
            " ORDER BY created_at DESC, admission_id DESC LIMIT 1"
        ).fetchone()
    return {"admission_id": admission_id, "status": status, "verdict": verdict or {}}


def _approve(admission_id: str) -> dict:
    from okg.substrate.runtime import admission_workflows

    return admission_workflows._run_decide(
        deployment="svc", admission_id=admission_id, decision="accept",
        actor="operator", reason="record-set-blob-autoprune", verdict=None,
    )


def test_autoprune_runs_on_held_run_approval_and_keeps_a_held_runs_blob(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A held (pending) run's blob is kept by every prune until it is
    decided; approving it prunes the source the same way the direct path
    does, and the approved watermark's blob is there to read."""
    from okg.substrate.ingest import source_blob_prune
    from tests.substrate.test_connector_completed_scope_modes_e2e import _ingest

    monkeypatch.delenv(_CAP_ENV, raising=False)
    dep, _ = _ingest_deployment(
        tmp_path, clean_runtime, _catalog_labels(200), monkeypatch,
    )
    _set_grace(clean_runtime, dep, 3600)
    _ingest_once(clean_runtime, dep)
    _hold_for_review(clean_runtime, dep, monkeypatch)
    held, _report = _ingest(clean_runtime, dep, publish=False)
    assert held["admission_status"] == "pending", held
    assert "record_set_blob_prune" not in held, held
    envelope = _held_envelope(clean_runtime)
    held_shas = set(_SHA_RE.findall(json.dumps(envelope["verdict"]))) & (
        _blob_rows(clean_runtime)["record_sets"]
    )
    assert held_shas, "the held run names no record-set blob"
    _age_blobs(clean_runtime)

    # Any prune while the run is held keeps its blob.
    direct = source_blob_prune.prune_source_blobs_after_accept(
        clean_runtime, deployment="svc", source="svc.first", grace_seconds=0,
    )
    assert direct["status"] == "ok", direct
    assert held_shas <= _blob_rows(clean_runtime)["record_sets"]

    _seed_orphan_blob(clean_runtime, "svc.first", "9" * 64)
    decided = _approve(envelope["admission_id"])
    assert decided["status"] == "accepted", decided
    prune = decided["record_set_blob_prune"]
    assert prune["status"] == "ok" and prune["deleted_rows"] >= 1, prune
    rows = _blob_rows(clean_runtime)["record_sets"]
    assert "9" * 64 not in rows
    applied = _svc_watermark(clean_runtime)["record_set_ref"]["sha256"]
    assert applied in held_shas and applied in rows


def test_autoprune_keeps_a_held_runs_blob_through_a_newer_accepted_run(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A run is held, a newer run of the source is accepted and prunes, then
    the held run is approved: its blob survived the newer run's prune, the
    approval happens, and the approved watermark's blob is there to read."""
    import yaml
    from pathlib import Path

    from tests.substrate.runtime import test_connector_completed_scope_modes as modes
    from tests.substrate.test_connector_completed_scope_modes_e2e import _ingest
    from tests.substrate.test_ingest_service import _claim_layered_fixture_catalog

    monkeypatch.delenv(_CAP_ENV, raising=False)
    dep, catalog = _ingest_deployment(
        tmp_path, clean_runtime, _catalog_labels(200), monkeypatch,
    )
    _set_grace(clean_runtime, dep, 3600)
    first = _ingest_once(clean_runtime, dep)
    first_sha = _svc_watermark(clean_runtime)["record_set_ref"]["sha256"]
    _hold_for_review(clean_runtime, dep, monkeypatch)
    modes.write_catalog(Path(catalog), _catalog_labels(199) + ("REL_held_" + "x" * 100,))
    held, _report = _ingest(clean_runtime, dep, publish=False)
    assert held["admission_status"] == "pending", held
    envelope = _held_envelope(clean_runtime)
    held_shas = set(_SHA_RE.findall(json.dumps(envelope["verdict"]))) & (
        _blob_rows(clean_runtime)["record_sets"]
    )
    assert held_shas and first_sha not in held_shas, (held_shas, first)
    _age_blobs(clean_runtime)

    registry_path = dep / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    registry["sources"]["first"]["admission_policy"]["admission_mode"] = "fast_track"
    registry_path.write_text(yaml.safe_dump(registry), encoding="utf-8")
    _claim_layered_fixture_catalog(clean_runtime, dep)
    modes.write_catalog(Path(catalog), _catalog_labels(199) + ("REL_newer_" + "x" * 100,))
    newer, _report = _ingest(clean_runtime, dep, publish=False)
    assert newer["admission_status"] in {"accepted", "published"}, newer
    assert newer["record_set_blob_prune"]["status"] == "ok", newer
    # Old and unnamed by the watermark, but the pending envelope names it.
    assert held_shas <= _blob_rows(clean_runtime)["record_sets"]

    decided = _approve(envelope["admission_id"])
    assert decided["status"] == "accepted", decided
    assert "emitted" in decided and not decided.get("already_decided"), decided
    assert decided["record_set_blob_prune"]["status"] == "ok", decided
    applied = _svc_watermark(clean_runtime)["record_set_ref"]["sha256"]
    assert applied in held_shas, (applied, held_shas)
    assert applied in _blob_rows(clean_runtime)["record_sets"]


def test_failed_autoprune_keeps_the_acceptance_and_is_reported_everywhere(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A prune that hits its lock timeout: the run stays accepted, the ingest
    report and okg.source_progress carry the error, `okg doctor --json` names
    the source; the next successful prune clears the record."""
    from okg.substrate.cli import main as cli
    from okg.substrate.ingest import source_blob_prune

    monkeypatch.delenv(_CAP_ENV, raising=False)
    monkeypatch.setattr(source_blob_prune, "LOCK_TIMEOUT", "300ms")
    dep, _ = _ingest_deployment(
        tmp_path, clean_runtime, _catalog_labels(200), monkeypatch,
    )
    _set_grace(clean_runtime, dep, 3600)
    _ingest_once(clean_runtime, dep)
    # An old, unnamed cursor blob, row-locked by another session. (A lock on
    # a record-set blob row would also block the runner's own ALTER TABLE.)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO okg.source_cursor_blobs"
            " (source, cursor_key, sha256, value, size_bytes, updated_at)"
            " VALUES ('svc.first', 'stale', %s, '{}'::jsonb, 2,"
            " now() - interval '1 day')",
            ("7" * 64,),
        )
    with psycopg.connect(clean_runtime) as blocker:
        blocker.execute(
            "SELECT 1 FROM okg.source_cursor_blobs"
            " WHERE source = 'svc.first' AND sha256 = %s FOR UPDATE",
            ("7" * 64,),
        )
        record = _ingest_once(clean_runtime, dep)
        blocker.rollback()
    prune = record["record_set_blob_prune"]
    assert record["status"] == "OK" and prune["status"] == "error", record
    assert "lock" in prune["error"].lower(), prune
    applied = _svc_watermark(clean_runtime)["record_set_ref"]["sha256"]
    assert applied in _blob_rows(clean_runtime)["record_sets"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        (value,) = conn.execute(
            "SELECT value FROM okg.source_progress WHERE deployment = 'svc'"
            " AND source_name = 'svc.first' AND key = %s",
            (source_blob_prune.ERROR_KEY,),
        ).fetchone()
    recorded = json.loads(value)
    assert recorded["source"] == "svc.first" and recorded["at"]
    assert recorded["error"] == prune["error"]

    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(dep.parent))
    monkeypatch.setenv("OKG_DSN", clean_runtime)
    capsys.readouterr()
    cli.main(["doctor", "--deployment", str(dep), "--dsn", clean_runtime, "--json"])
    doctor = json.loads(capsys.readouterr().out)
    check = doctor["record_set_blobs"]
    assert [e["source"] for e in check["prune_errors"]] == ["svc.first"], check
    assert check["ok"] is False
    assert any(
        w["code"] == "source_record_set_blob_prune_failed"
        and w["source"] == "svc.first"
        for w in doctor.get("warnings", [])
    )

    again = _ingest_once(clean_runtime, dep)
    assert again["record_set_blob_prune"]["status"] == "ok", again
    assert ("stale", "7" * 64) not in _blob_rows(clean_runtime)["cursors"]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        assert conn.execute(
            "SELECT count(*) FROM okg.source_progress WHERE key = %s",
            (source_blob_prune.ERROR_KEY,),
        ).fetchone()[0] == 0
    capsys.readouterr()
    cli.main(["doctor", "--deployment", str(dep), "--dsn", clean_runtime, "--json"])
    assert json.loads(capsys.readouterr().out)["record_set_blobs"]["prune_errors"] == []


def test_autoprune_grace_setting_is_validated() -> None:
    from okg.substrate.ingest import source_blob_prune

    assert source_blob_prune.grace_seconds_from_manifest({}) == 21600
    assert source_blob_prune.grace_seconds_from_manifest(
        {"source_blob_prune_grace_seconds": None},
    ) == 21600
    assert source_blob_prune.grace_seconds_from_manifest(
        {"source_blob_prune_grace_seconds": 0},
    ) == 0
    for bad in (-1, "6h", True, 1.5):
        with pytest.raises(ValueError, match="source_blob_prune_grace_seconds"):
            source_blob_prune.grace_seconds_from_manifest(
                {"source_blob_prune_grace_seconds": bad},
            )


def test_autoprune_error_key_is_runner_owned_progress(clean_runtime: str) -> None:
    """The prune's error record is a sync.* key: a run-start snapshot does
    not carry it, so a restore neither erases nor revives it; a run undo
    keeps it; and a source whose only progress row is that record still
    looks empty to its adapter."""
    from okg.substrate.ingest import source_blob_prune
    from okg.substrate.ingest.compensation import (
        restore_source_state,
        snapshot_source_state,
    )
    from okg.substrate.ingest.run_base import forget_progress_after_undo

    key = source_blob_prune.ERROR_KEY
    assert key == "sync.record_set_blob_prune.last_error"
    deployment, source = "test-source-runner", "test.autoprune-error-key"

    def keys() -> set[str]:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            return {
                row[0] for row in conn.execute(
                    "SELECT key FROM okg.source_progress"
                    " WHERE deployment = %s AND source_name = %s",
                    (deployment, source),
                )
            }

    def put(name: str) -> None:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            conn.execute(
                "INSERT INTO okg.source_progress (deployment, source_name, key, value)"
                " VALUES (%s, %s, %s, '{}')",
                (deployment, source, name),
            )

    def drop(name: str) -> None:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            conn.execute(
                "DELETE FROM okg.source_progress"
                " WHERE deployment = %s AND source_name = %s AND key = %s",
                (deployment, source, name),
            )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    # Adapter view: only the error record -> the same empty progress as none.
    put(key)
    adapter = _ProgressAwareAdapter()
    SourceRunner(
        clean_runtime, source_name=source, deployment_name=deployment,
        write_fact_log=True,
    ).run(adapter, mode="scope_complete")
    assert adapter.seen_progress == {}
    drop(key)
    keys_after_run = keys()

    # Snapshot and restore: never captured, so never erased or revived.
    put(key)
    snapshot = snapshot_source_state(clean_runtime, deployment=deployment, source=source)
    assert key not in {row["key"] for row in snapshot.progress_rows}
    restore_source_state(clean_runtime, snapshot)
    assert key in keys()  # not erased
    drop(key)
    restore_source_state(clean_runtime, snapshot)
    assert key not in keys()  # not revived
    assert keys() == keys_after_run

    # Run undo forgets the source's own progress but keeps sync.* keys.
    put(key)
    put("record:doc:a")
    checkpoint_id = f"autoprune-undo-{uuid.uuid4()}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # A run base cut with no recorded cut time: the undo forgets all of
        # the source's progress except sync.* keys.
        conn.execute(
            "INSERT INTO public._chronos_branch_interval_checkpoints"
            " (checkpoint_id, branch_id, segment_id, created_at, metadata)"
            " VALUES (%s, 'default', 0, now()::text, %s)",
            (checkpoint_id, json.dumps({"deployment_name": deployment, "source": source})),
        )
        forgotten = forget_progress_after_undo(conn, checkpoint_id=checkpoint_id)
        conn.execute(
            "DELETE FROM public._chronos_branch_interval_checkpoints"
            " WHERE checkpoint_id = %s",
            (checkpoint_id,),
        )
    assert forgotten >= 1
    assert key in keys()
    assert "record:doc:a" not in keys()


def test_autoprune_keeps_a_dead_worker_childs_run_start_blob_for_the_parent_restore(
    clean_runtime: str, tmp_path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A worker child pins its run-start blobs too. Here the child's run is
    treated as one that died: after its lock is gone a newer worker run is
    accepted and prunes, and the parent then writes the child's run-start
    state back. That state's blob, old before the child started, is still
    there and the next sync reads it."""
    from okg.substrate.ingest import source_blob_prune
    from okg.substrate.ingest.compensation import restore_source_state

    monkeypatch.delenv(_CAP_ENV, raising=False)
    dep, catalog = _ingest_deployment(
        tmp_path, clean_runtime, _catalog_labels(200), monkeypatch,
    )
    _set_grace(clean_runtime, dep, 3600)
    _changed_sync(clean_runtime, dep, catalog)
    start_sha = _svc_watermark(clean_runtime)["record_set_ref"]["sha256"]
    _age_blobs(clean_runtime)

    snapshots: list[Any] = []
    real_pin = source_blob_prune.pin_snapshot_blobs

    def capture(dsn, snapshot, *args, **kwargs):
        snapshots.append(snapshot)
        return real_pin(dsn, snapshot, *args, **kwargs)

    monkeypatch.setattr(source_blob_prune, "pin_snapshot_blobs", capture)
    _run_as_worker_children(clean_runtime, monkeypatch)
    child = _changed_sync(clean_runtime, dep, catalog)  # the run that "dies"
    (child_snapshot,) = snapshots
    assert start_sha in json.dumps(child_snapshot.watermark, default=str)
    assert child["record_set_blob_prune"]["lock"] == "held_by_run", child
    newer = _changed_sync(clean_runtime, dep, catalog)
    assert newer["record_set_blob_prune"]["status"] == "ok", newer
    assert start_sha not in _watermark_shas(clean_runtime)

    restore_source_state(clean_runtime, child_snapshot)  # the parent's restore
    assert _svc_watermark(clean_runtime)["record_set_ref"]["sha256"] == start_sha
    assert start_sha in _blob_rows(clean_runtime)["record_sets"]
    after = _changed_sync(clean_runtime, dep, catalog)
    assert "prior_record_set_missing" not in after, after
