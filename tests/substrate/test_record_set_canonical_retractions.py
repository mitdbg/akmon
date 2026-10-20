"""Record-set retractions under id canonicalization (wave 5,
strengthen-formal-models).

The 2026-07-01 audit flagged `_record_set_retractions` as a candidate bug:
it diffs RAW record-set ids while the graph holds canonical ids, so a
disappeared aliased record looked like it would produce a no-op tombstone
on the Chronos-verified fast path (where the canonicalizing stray sweep is
skipped). The code-level decision (recorded in PACT
formal-strengthen-source-sync): NOT a bug — every synthesized retraction
flows through the same per-chunk flush as source facts, and the flush
canonicalizes ALL fact ids (`_canonicalize_facts`, op='R' included) before
COPY; alias churn within a surviving record is handled by the R-before-I
yield ordering (retractions are emitted ahead of the run's source facts, so
the insert lands as the latest fact). These tests pin both mechanisms so a
pipeline reorder or a canonicalize-stage filter regression turns them red.
"""
from __future__ import annotations

import psycopg
import pytest

import okg.substrate.ingest.runner as runner_module
from okg.substrate.canonicalize import RepoNamespaceCanonicalizer
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.change_probe import (
    ProbeVerdict,
    compose_token,
)

from tests.substrate._db_helpers import seed_catalog_version
from tests.substrate.test_source_runner_record_set import (
    _RecordSetAdapter,
    _StreamFootprintAdapter,
    _node,
    _record,
)
from okg.substrate.library.sources.base import NodeFact, RecordSet, SourceRun

pytestmark = pytest.mark.postgres


class _AliasCanonicalizer:
    """alias:<x>[@rev] -> doc:<x> — one canonical node behind rotating raw ids."""

    id_family = "alias"

    def canonicalize(self, raw_id: str) -> str:
        rest = raw_id.split(":", 1)[1]
        return "doc:" + rest.split("@", 1)[0]


def _runner(dsn: str) -> SourceRunner:
    return SourceRunner(
        dsn,
        source_name="test.canonical-record-set",
        write_fact_log=True,
        canonicalizers=[_AliasCanonicalizer()],
    )


def _stream_runner(dsn: str) -> SourceRunner:
    return SourceRunner(
        dsn,
        source_name="test.canonical-record-set",
        write_fact_log=True,
        canonicalizers=[_AliasCanonicalizer()],
        deletion_semantics="missing_from_completed_scope",
    )


def _retraction_node_ids(dsn: str) -> list[str]:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT node_id FROM okg.node_facts
             WHERE source = 'test.canonical-record-set' AND op = 'R'
             ORDER BY node_id
            """
        )
        return [row[0] for row in cur.fetchall()]


def test_disappeared_aliased_record_retracts_the_canonical_node(
    clean_runtime: str,
) -> None:
    """The raw-id diff synthesizes a retraction carrying the RAW prior id;
    the flush canonicalizes it, so the tombstone targets the canonical node
    the graph actually holds — never a nonexistent raw id."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    record = _record("docs/a.md", primary="alias:a")
    adapter = _RecordSetAdapter([
        ([_node("alias:a")], record),
        ([], RecordSet()),
    ])
    runner = _runner(clean_runtime)
    # Issue #2818 backstop: the only record disappearing empties the whole
    # source, which is refused without `okg ingest --allow-full-retraction`.
    from datetime import datetime, timezone

    from okg.substrate.ingest.full_retraction_guard import (
        FullRetractionOverride,
    )

    runner.full_retraction_override = FullRetractionOverride(
        source="test.canonical-record-set",
        requested_by="canonical-retraction-test",
        requested_at=datetime.now(timezone.utc),
    )

    first = runner.run(adapter, mode="scope_complete")
    assert first.retractions_node == 0

    second = runner.run(adapter, mode="scope_complete")
    assert second.retractions_node == 1

    retracted = _retraction_node_ids(clean_runtime)
    assert retracted == ["doc:a"], (
        "the retraction must land under the CANONICAL id; a raw-id "
        f"tombstone would be a silent no-op — got {retracted}"
    )


def test_alias_churn_within_surviving_record_keeps_node_live(
    clean_runtime: str,
) -> None:
    """Two raw ids for the same canonical node across runs: the raw diff
    retracts the old alias (canonicalized to doc:a), but retractions are
    yielded BEFORE the run's source facts, so the re-emitted insert is the
    latest fact and the node stays live."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    adapter = _RecordSetAdapter([
        ([_node("alias:a@r1")], _record("docs/a.md", primary="alias:a@r1")),
        ([_node("alias:a@r2")], _record("docs/a.md", primary="alias:a@r2")),
    ])
    runner = _runner(clean_runtime)

    runner.run(adapter, mode="scope_complete")
    runner.run(adapter, mode="scope_complete")

    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            SELECT op FROM okg.node_facts
             WHERE source = 'test.canonical-record-set'
               AND node_id = 'doc:a'
             ORDER BY fact_id DESC
             LIMIT 1
            """
        )
        latest_op = cur.fetchone()[0]
    assert latest_op == "I", (
        "alias churn within a surviving record must leave the canonical "
        "node LIVE (R-before-I ordering); a trailing retraction would "
        f"silently drop it — latest op was {latest_op!r}"
    )


def test_completed_scope_deletion_stream_alias_churn_keeps_node_live(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    adapter = _StreamFootprintAdapter([
        {"facts": [_node("alias:a@r1")]},
        {"facts": [_node("alias:a@r2")]},
    ])
    runner = _stream_runner(clean_runtime)

    runner.run(adapter, mode="scope_complete")
    second = runner.run(adapter, mode="scope_complete")

    assert second.retractions_node == 0
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(conn).checkout().query(
            """
            SELECT node_id
              FROM okg.graph_nodes
             WHERE source = :source
             ORDER BY node_id
            """,
            {"source": "test.canonical-record-set"},
        )
    assert [row["node_id"] for row in rows] == ["doc:a"]


def test_completed_scope_deletion_stream_tail_uses_final_chunk_order(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)
    monkeypatch.setattr(runner_module, "COPY_CHUNK_ROWS", 1)

    adapter = _StreamFootprintAdapter([
        {"facts": [_node("alias:a@r1")]},
        {
            "facts": [
                NodeFact(
                    node_id="alias:a@r1",
                    subtype="concept",
                    attrs={},
                    source_record_id={"id": "a"},
                    source_revision={"rev": "removed"},
                    op="R",
                ),
                _node("alias:a@r2"),
            ],
        },
    ])
    runner = _stream_runner(clean_runtime)

    runner.run(adapter, mode="scope_complete")
    second = runner.run(adapter, mode="scope_complete")

    assert second.retractions_node == 1
    with psycopg.connect(clean_runtime, autocommit=True) as conn, \
            conn.cursor() as cur:
        cur.execute(
            """
            SELECT op
              FROM okg.node_facts
             WHERE source = 'test.canonical-record-set'
               AND node_id = 'doc:a'
             ORDER BY fact_id DESC
             LIMIT 1
            """
        )
        latest_op = cur.fetchone()[0]
    assert latest_op == "I"


def test_canonicalizer_transition_retracts_prior_effective_graph_topology(
    clean_runtime: str,
) -> None:
    """A pipeline replay must retire ids produced by the old canonicalizer.

    Diffing only the raw record set is insufficient here: the raw id remains
    ``file:foo:x.py`` while its effective graph id changes from ``old`` to
    ``new``.  The replay therefore has to compare against the graph-authoritative
    footprint, not canonicalize the old raw id with the new rules.
    """
    source = "test.canonicalizer-transition"
    token = compose_token(
        content={"kind": "content_hash", "value": "a" * 64},
        config="b" * 64,
        emit_version="c" * 64,
    )

    class _Probe:
        def __init__(self) -> None:
            self.changed = True

        def probe(self, *, cursor, sync_scope):
            del cursor, sync_scope
            return ProbeVerdict(changed=self.changed, token=token)

    class _Adapter:
        name = "canonicalizer-transition"
        profile = "discovery_crawl"
        probe_short_circuit_safe = True

        def __init__(self) -> None:
            self.change_probe = _Probe()
            self.run_calls = 0

        def run(self, run_id, *, mode="cursor", cursor=None):
            del run_id, cursor
            self.run_calls += 1
            raw_id = "file:foo:x.py"
            return SourceRun(
                facts=[_node(raw_id, subtype="source_file")],
                completed_scope=True,
                next_cursor={},
                run_mode=mode,
                record_set=_record("x.py", primary=raw_id),
                applied_raw_token=token,
                applied_token_scope="global",
                applied_token_authority="exact",
            )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    adapter = _Adapter()
    runner = SourceRunner(
        clean_runtime,
        source_name=source,
        write_fact_log=True,
        canonicalizers=RepoNamespaceCanonicalizer.chain_from_aliases(
            {"foo": "old"},
            families=("file",),
        ),
    )
    runner.run(adapter, mode="scope_complete")

    adapter.change_probe.changed = False
    runner.canonicalizers = RepoNamespaceCanonicalizer.chain_from_aliases(
        {"foo": "new"},
        families=("file",),
    )
    replay = runner.run(adapter, mode="scope_complete")

    assert replay.mode == "scope_complete"
    assert adapter.run_calls == 2
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        rows = store.query(
            """
            SELECT node_id
              FROM okg.graph_nodes
             WHERE source = :source
             ORDER BY node_id
            """,
            {"source": source},
        )
        latest_old_op = conn.execute(
            """
            SELECT op
              FROM okg.node_facts
             WHERE source = %s
               AND node_id = 'file:old:x.py'
             ORDER BY fact_id DESC
             LIMIT 1
            """,
            (source,),
        ).fetchone()[0]

    assert [row["node_id"] for row in rows] == ["file:new:x.py"]
    assert latest_old_op == "R"
