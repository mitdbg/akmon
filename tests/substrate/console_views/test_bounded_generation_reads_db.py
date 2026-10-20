"""The console's version-listing reads must stay bounded and projected.

Three console reads -- the published versions list, the attempts list and the
attempts-per-day chart -- used to call `ChronosGraphStore.list_generations`,
which has no LIMIT and no projection: it fetches every checkpoint's FULL
metadata document. On okg-workspace that is 88 rows and 131 MB per call,
1.65 s for a twelve-row panel (issue #2202).

These tests seed MORE generation checkpoints than the reads' window and then
make the unbounded path unusable: `list_generations` is monkeypatched to
raise. A composer that still falls back to it fails here with that exception
rather than quietly costing a full fetch in production, where the only symptom
is a slow page.

They also pin the two properties the bound depends on: the projected read is
ONE statement per call, and it returns only the declared metadata keys.
"""
from __future__ import annotations

import json
from types import SimpleNamespace
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from okg.substrate.console_views.attempts_by_day import compose_attempts_by_day
from okg.substrate.console_views.records import compose_published_versions
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.graph_store_internal import generations as gen_mod

pytestmark = pytest.mark.postgres

_DEP = "default"
_BRANCH = "default"
# More than any window under test, so a bounded read provably stops early.
_SEEDED = 40


class _ListGenerationsRefused(AssertionError):
    """Raised in place of the unbounded full-metadata fetch."""


@pytest.fixture
def _no_full_fetch(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the unbounded read fail loud, so a fallback cannot hide."""
    def _refuse(*args, **kwargs):
        raise _ListGenerationsRefused(
            "list_generations fetches every checkpoint's full metadata; a "
            "bounded console read must not call it"
        )

    monkeypatch.setattr(ChronosGraphStore, "list_generations", _refuse)
    monkeypatch.setattr(
        gen_mod.ChronosGraphGenerationLifecycleMixin, "list_generations", _refuse
    )


def _seed_checkpoints(dsn: str, count: int = _SEEDED) -> list[str]:
    """`count` generation checkpoints, newest last, each with fat metadata.

    The padding is what the projection is for: a full fetch moves it, a
    projected read leaves it in the database.
    """
    base = datetime(2026, 9, 1, tzinfo=timezone.utc)
    ids: list[str] = []
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS _chronos_branch_interval_checkpoints (
                    checkpoint_id text PRIMARY KEY,
                    branch_id text,
                    segment_id integer,
                    created_at text,
                    metadata text
                )
                """
            )
            cur.execute(
                "DELETE FROM _chronos_branch_interval_checkpoints WHERE branch_id = %s",
                (_BRANCH,),
            )
            for index in range(count):
                stamp = (base + timedelta(hours=index)).isoformat()
                checkpoint_id = (
                    f"okg:{_DEP}:branch:{_BRANCH}:gen:"
                    f"2026090{index % 9}T{index:02d}0000000000Z:{index:012d}"
                )
                metadata = {
                    "kind": "okg_generation",
                    "deployment_name": _DEP,
                    "branch": _BRANCH,
                    # Every other attempt blocked, so a window of published
                    # versions has to scan past rows it does not serve.
                    "status": "published" if index % 2 == 0 else "blocked",
                    "catalog_version_id": 3,
                    "target_generation_id": index + 1,
                    "node_count": index,
                    "edge_count": index,
                    # The bytes the projection exists to leave behind.
                    "blocked_reason": {"detail": "x" * 200_000},
                    "projection_deltas": {"pad": "y" * 200_000},
                    # Enricher runs for the producer-metrics read, recorded
                    # both ways a publisher has written them.
                    **(
                        {"enricher_runs": [{
                            "producer_id": "_enricher:first", "status": "ok",
                            "n_edges_emitted": index,
                            "timings_ms": {"total": 10 + index},
                        }]}
                        if index % 4 == 0 else
                        {"timing_details": {"enrichers": [{
                            "producer_id": "_enricher:first", "status": "ok",
                            "n_edges_emitted": index,
                            "timings_ms": {"total": 10 + index},
                        }]}}
                    ),
                }
                cur.execute(
                    """
                    INSERT INTO _chronos_branch_interval_checkpoints
                        (checkpoint_id, branch_id, segment_id, created_at, metadata)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (checkpoint_id, _BRANCH, index, stamp, json.dumps(metadata)),
                )
                ids.append(checkpoint_id)
        assert store is not None
    return ids


def _statement_count(conn, fn):
    """How many statements `fn` issues through the store's db adapter."""
    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    calls: list[str] = []
    original = store.context.db.execute

    def _counting(sql, params=None, *args, **kwargs):
        calls.append(sql)
        return original(sql, params, *args, **kwargs) if params is not None \
            else original(sql, *args, **kwargs)

    store.context.db.execute = _counting  # type: ignore[method-assign]
    try:
        result = fn(store)
    finally:
        store.context.db.execute = original  # type: ignore[method-assign]
    return result, calls


def test_recent_generations_is_one_statement_and_stops_at_its_window(
    clean_runtime: str,
) -> None:
    """One statement, at most `scan_window` rows scanned, bound served."""
    _seed_checkpoints(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        recent, calls = _statement_count(
            conn,
            lambda store: store.recent_generations(
                deployment_name=_DEP, branch=_BRANCH, scan_window=8,
                keys=("status",),
            ),
        )
    assert len(calls) == 1, f"expected ONE statement, got {len(calls)}"
    assert recent.scanned == 8
    assert recent.scan_window == 8
    assert recent.window_exhausted is True, (
        "40 checkpoints seeded and 8 scanned: the read must say it stopped at "
        "its window, or a caller will print a bounded history as a complete one"
    )
    assert len(recent.rows) == 8
    for row in recent.rows:
        assert set(row.metadata) == {"kind", "deployment_name", "status"}, (
            "the projection must return the declared keys and nothing else; "
            f"got {sorted(row.metadata)}"
        )
    # Newest first.
    stamps = [row.created_at for row in recent.rows]
    assert stamps == sorted(stamps, reverse=True)


def test_published_versions_never_falls_back_to_the_full_fetch(
    clean_runtime: str, _no_full_fetch: None
) -> None:
    """Pre-fix this read the `okg.v_generations` view, which parses every row."""
    _seed_checkpoints(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        payload = compose_published_versions(
            conn, deployment_name=_DEP, branch_name=_BRANCH, limit=5,
        )
    assert len(payload["versions"]) == 5
    assert payload["latest"] == payload["versions"][0]["generation_id"]
    assert payload["truncated"] is True
    assert payload["scanned"] <= payload["scan_window"]
    for version in payload["versions"]:
        assert version["catalog_version_id"] == 3
        assert version["published_at"], "a version must carry its stamp"


def test_attempts_by_day_never_falls_back_to_the_full_fetch(
    clean_runtime: str, _no_full_fetch: None
) -> None:
    """The chart reads a SPAN, bounded by created_at, not the whole history."""
    _seed_checkpoints(clean_runtime)
    now = datetime(2026, 9, 2, 12, tzinfo=timezone.utc)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        from okg.substrate.mcp import open_session

        session = open_session(conn, deployment_name=_DEP)
        payload = compose_attempts_by_day(
            conn,
            deployment_name=_DEP,
            session=session,
            requested_range="1d",
            now=now,
        )
    assert payload["range"] == "1d"
    assert payload["scan_from"], "the scan bound must be served"
    # The seeded checkpoints run 2026-09-01T00:00 onward, one an hour, so a
    # one-day window ending 2026-09-02 holds a real, non-empty count.
    assert sum(payload["totals"].values()) > 0
    assert payload["undated_count"] == 0
    assert payload["scanned"] <= _SEEDED


# ---------------------------------------------------------------------------
# Health, the knowledge overview and producer metrics (issue #2113).
#
# Each read is checked twice: once against the full fetch it replaced, so
# every served value is the one `list_generations` produced, and once with the
# full fetch refused, so the bounded path is the one that ran.
# ---------------------------------------------------------------------------


def _insert_raw_checkpoint(dsn: str, checkpoint_id: str, created_at: str,
                           metadata: dict) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO _chronos_branch_interval_checkpoints
                (checkpoint_id, branch_id, segment_id, created_at, metadata)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (checkpoint_id, _BRANCH, 0, created_at, json.dumps(metadata)),
        )


def test_count_generations_is_exact_across_checkpoint_id_shapes(
    clean_runtime: str,
) -> None:
    """The count equals `len(list_generations(status=None))`, whatever the id.

    A generation minted with an explicit integer id is stored as
    `generation:<n>`, which the id-prefix count cannot see; another
    deployment's generation and a non-generation checkpoint on the same
    branch must not be counted.
    """
    _seed_checkpoints(clean_runtime)
    stamp = "2026-09-03T00:00:00+00:00"
    _insert_raw_checkpoint(clean_runtime, "generation:7", stamp, {
        "kind": "okg_generation", "deployment_name": _DEP, "status": "published",
    })
    _insert_raw_checkpoint(clean_runtime, "generation:8", stamp, {
        "kind": "okg_generation", "deployment_name": "another", "status": "published",
    })
    _insert_raw_checkpoint(clean_runtime, "manual:snapshot", stamp, {
        "kind": "okg_branch", "deployment_name": _DEP,
    })
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        expected = len(store.list_generations(
            deployment_name=_DEP, branch=_BRANCH, status=None,
        ))
        counted, calls = _statement_count(
            conn,
            lambda s: s.count_generations(deployment_name=_DEP, branch=_BRANCH),
        )
    assert expected == _SEEDED + 1
    assert counted == expected
    assert len(calls) == 1, f"expected ONE statement, got {len(calls)}"


def test_health_knowledge_and_metrics_serve_what_the_full_fetch_served(
    clean_runtime: str,
) -> None:
    """Field by field against `list_generations` on the same checkpoints."""
    from okg.substrate.console_views import health, knowledge, producer_metrics

    ids = _seed_checkpoints(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        everything = store.list_generations(
            deployment_name=_DEP, branch=_BRANCH, status=None,
        )
        published = store.list_generations(
            deployment_name=_DEP, branch=_BRANCH, status="published",
        )

        attempts = health._recent_attempts(
            store, deployment_name=_DEP, branch=_BRANCH,
        )
        assert [row.checkpoint_id for row in attempts] == [
            row.checkpoint_id for row in reversed(everything[-10:])
        ]
        for bounded, full in zip(attempts, reversed(everything[-10:])):
            assert bounded.created_at == full.created_at
            for key in health.HEALTH_HISTORY_METADATA_KEYS:
                assert bounded.metadata.get(key) == full.metadata.get(key), key
            assert health._publication_attempt(bounded, _DEP, _BRANCH) == \
                health._publication_attempt(full, _DEP, _BRANCH)

        # The newest published generation, and one deep inside the window.
        for pinned_index in (len(published) - 1, len(published) - 6):
            pinned = published[pinned_index].checkpoint_id
            previous = health._previous_published(
                store, deployment_name=_DEP, branch=_BRANCH, generation_id=pinned,
            )
            expected = published[pinned_index - 1]
            assert previous is not None
            assert previous.checkpoint_id == expected.checkpoint_id
            for key in health.HEALTH_PREVIOUS_METADATA_KEYS:
                assert previous.metadata.get(key) == expected.metadata.get(key), key

        assert knowledge._generation_total(
            store, deployment_name=_DEP, branch=_BRANCH,
        ) == len(everything)
        assert health._history_total(
            store, deployment_name=_DEP, branch=_BRANCH,
        ) == len(everything)

    start = datetime(2026, 9, 1, 6, tzinfo=timezone.utc)
    end = datetime(2026, 9, 2, 6, tzinfo=timezone.utc)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        runs = producer_metrics._selected_enricher_runs(
            conn, deployment_name=_DEP,
            session=SimpleNamespace(branch_name=_BRANCH),
            names=["first"], start=start, end=end, limit=50,
        )
    # Seeded one an hour from 2026-09-01T00:00, so hours 6..30 inclusive.
    assert [row["n_edges_emitted"] for row in runs["first"]] == list(range(6, 31))
    assert {row["generation_status"] for row in runs["first"]} == {
        "published", "blocked",
    }
    assert ids


def test_health_knowledge_and_metrics_never_fall_back_to_the_full_fetch(
    clean_runtime: str, _no_full_fetch: None
) -> None:
    from okg.substrate.console_views import health, knowledge, producer_metrics

    _seed_checkpoints(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        assert len(health._recent_attempts(
            store, deployment_name=_DEP, branch=_BRANCH,
        )) == 10
        newest_published = store.recent_generations(
            deployment_name=_DEP, branch=_BRANCH, status="published",
            keys=("status",), scan_window=4,
        ).rows[0].checkpoint_id
        assert health._previous_published(
            store, deployment_name=_DEP, branch=_BRANCH,
            generation_id=newest_published,
        ) is not None
        assert knowledge._generation_total(
            store, deployment_name=_DEP, branch=_BRANCH,
        ) == _SEEDED
        runs = producer_metrics._selected_enricher_runs(
            conn, deployment_name=_DEP,
            session=SimpleNamespace(branch_name=_BRANCH),
            names=["first"],
            start=datetime(2026, 9, 1, tzinfo=timezone.utc),
            end=datetime(2026, 9, 3, tzinfo=timezone.utc),
            limit=5,
        )
    assert len(runs["first"]) == 5



# ---------------------------------------------------------------------------
# One checkpoint's metadata never crosses the wire whole (issue #2546).
#
# okg-workspace's checkpoints carry up to 62 MB of metadata, most of it
# `invariant_summary.frontier_node_ids`. The generations panel, the Sources
# readiness and opening the pinned graph each used to fetch that document
# (`get_generation` / `get_checkpoint` run `SELECT *`) to read a few small
# fields. This seeds one real generation whose frontier list is megabytes,
# makes both full reads raise, and measures what every statement returns.
# ---------------------------------------------------------------------------

#: The bound on what any one statement may return for a checkpoint. The fat
#: checkpoint below is ~4 MB, so one full fetch exceeds it sixty-fold.
_BYTES_PER_CHECKPOINT = 64 * 1024


class _FullCheckpointReadRefused(AssertionError):
    """Raised in place of a read that moves a checkpoint's whole document."""


def _publish_fat_generation(dsn: str) -> str:
    """One real published generation whose metadata is several megabytes."""
    from okg.substrate.graph_store import DEFAULT_BRANCH
    from okg.substrate.history_compaction import ensure_history_compaction_schema
    from okg.substrate.publisher.console_count_rollup import (
        maintain_console_count_rollup,
    )

    frontier = [f"node:frontier:{index:08d}" for index in range(180_000)]
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        ensure_history_compaction_schema(conn)
        graph = store.checkout(DEFAULT_BRANCH)
        with graph.transaction():
            graph.upsert_rows("okg.graph_nodes", [
                {"node_id": "n:a", "subtype": "alpha", "source": "src"},
                {"node_id": "n:b", "subtype": "alpha", "source": "src"},
            ])
            graph.upsert_rows("okg.graph_edges", [
                {"edge_id": 1, "src": "n:a", "dst": "n:b", "edge_type": "rel"},
            ])
        maintain_console_count_rollup(
            conn,
            generation_id=1,
            changed_node_ids=[],
            changed_edge_ids=[],
            prior_generation_checkpoint=None,
            force_full=True,
        )
        checkpoint_id = store.checkpoint_generation(
            branch=DEFAULT_BRANCH,
            deployment_name=_DEP,
            status="published",
            catalog_version_id=1,
            metadata={
                "node_count": 2,
                "edge_count": 1,
                "target_generation_id": 1,
                "publisher_configuration": {"mode": "test"},
                # `edges_retracted` left out on purpose: an absent key must
                # stay absent through the projection.
                "projection_deltas": {
                    "nodes_added": 7,
                    "nodes_retracted": 2,
                    "edges_added": 3,
                },
                "invariant_summary": {
                    "frontier_node_ids": frontier,
                    "cross_source": {"total": 5, "failed": 1, "timed_out": 0},
                    "total_duration_ms": 1234,
                    "total_budget_exceeded": False,
                },
            },
        )
        size = conn.execute(
            "SELECT octet_length(metadata) FROM _chronos_branch_interval_checkpoints"
            " WHERE checkpoint_id = %s",
            (checkpoint_id,),
        ).fetchone()[0]
    assert size > 60 * _BYTES_PER_CHECKPOINT, (
        f"the seeded checkpoint must dwarf the bound; it is {size} bytes"
    )
    return checkpoint_id


@pytest.fixture
def _bytes_per_statement(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, int]]:
    """Record the bytes the server returned for every statement, by statement.

    Measured on the result itself (`pgresult`), so a row counts whether or
    not the caller fetched it.
    """
    seen: list[tuple[str, int]] = []
    original = psycopg.Cursor.execute

    def _measuring(self, query, params=None, *args, **kwargs):
        result = original(self, query, params, *args, **kwargs)
        pgresult = self.pgresult
        total = 0
        if pgresult is not None:
            for row in range(pgresult.ntuples):
                for column in range(pgresult.nfields):
                    total += len(pgresult.get_value(row, column) or b"")
        seen.append((" ".join(str(query).split())[:160], total))
        return result

    monkeypatch.setattr(psycopg.Cursor, "execute", _measuring)
    return seen


@pytest.fixture
def _no_full_checkpoint_read(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every whole-document checkpoint read fails loud instead of running."""
    from chronos_core.branching._interval_backend import _IntervalBackend
    from chronos_core.branching._runtime import ChronosBranchContext

    def _refuse(*args, **kwargs):
        raise _FullCheckpointReadRefused(
            "this read moves the checkpoint's whole metadata document"
        )

    monkeypatch.setattr(ChronosGraphStore, "get_generation", _refuse)
    monkeypatch.setattr(
        gen_mod.ChronosGraphGenerationLifecycleMixin, "get_generation", _refuse
    )
    monkeypatch.setattr(ChronosBranchContext, "get_checkpoint", _refuse)
    monkeypatch.setattr(_IntervalBackend, "get_checkpoint", _refuse)


def test_console_reads_never_move_a_whole_checkpoint_document(
    clean_runtime: str,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    _no_full_checkpoint_read: None,
    _bytes_per_statement: list[tuple[str, int]],
) -> None:
    """Generations, Sources readiness and the pinned graph stay under 64 KiB.

    Pre-fix, the generations panel fetched each row's whole document to read
    its counts, the readiness re-found the latest generation by reading whole
    documents and then counted every node, and opening the pinned graph
    fetched the pinned document twice.
    """
    from okg.substrate.console_views.console_readiness import (
        compose_console_readiness,
    )
    from okg.substrate.console_views.generations import compose_generations
    from okg.substrate.console_views.health import compose_health
    from okg.substrate.console_views.knowledge import compose_knowledge
    from okg.substrate.console_views.status import compose_status
    from okg.substrate.mcp import open_session

    monkeypatch.setenv("OKG_IVM_CONSOLE_COUNT_ROLLUP", "1")
    checkpoint_id = _publish_fat_generation(clean_runtime)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=_DEP)
    assert session.generation_id == checkpoint_id
    _bytes_per_statement.clear()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        generations = compose_generations(
            conn, deployment_name=_DEP, session=session, limit=5,
        )
        readiness = compose_console_readiness(
            deployment_name=_DEP,
            deployment_dir=tmp_path,
            dsn=clean_runtime,
            session=session,
        )
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        pinned = store.checkout_generation(
            session.generation_id, branch=session.branch_name,
        )
        node_rows = pinned.query(
            "SELECT node_id FROM okg.graph_nodes ORDER BY node_id"
        )
        once = store.query_generation_once(
            session.generation_id, "SELECT count(*) AS n FROM okg.graph_edges",
        )
        # Home's reads: the topbar status, the knowledge overview and the
        # health dashboard. `compose_health` used to call `get_generation`
        # for the pinned document (8.7 MB on okg-workspace).
        status = compose_status(
            conn, session=session, deployment_name=_DEP,
            operator_email=None, operator_display_name=None,
        )
        knowledge = compose_knowledge(
            conn, session=session, generated_at="2026-09-28T00:00:00+00:00",
            deployment_name=_DEP,
        )
        health = compose_health(conn, deployment_name=_DEP, session=session)

    # Every read served its answer, from the projected fields.
    [row] = [
        item for item in generations["generations"]
        if item["full_generation_id"] == checkpoint_id
    ]
    assert row["delta_available"] is True, row["delta_unavailable_reason"]
    assert (row["nodes_added"], row["edges_added"]) == (2, 1)
    assert readiness["read_authority"]["generation"] == {"status": "available"}
    assert readiness["read_authority"]["mcp"] == {"status": "available"}
    generation = readiness["details"]["readiness"]["generation"]
    assert generation["generation_id"] == checkpoint_id
    assert generation["status"] == "published"
    assert (generation["node_count"], generation["edge_count"]) == (2, 1)
    assert generation["publisher_configuration"] == {"mode": "test"}
    assert generation["catalog_version_id"] == 1
    assert "count_integrity" not in generation, (
        "the page says generation.count_integrity is not run; the payload "
        "must not carry a count it did not make"
    )
    assert "generation.count_integrity" in readiness["checks_not_run"]
    assert [item["node_id"] for item in node_rows] == ["n:a", "n:b"]
    assert int(once[0]["n"]) == 1
    assert status["generation_id"] == checkpoint_id
    assert (status["nodes"], status["edges"]) == (2, 1)
    assert status["generation_age"], "the status read must serve the stamp"
    assert knowledge["generation_id"]
    assert health["generation_id"] == checkpoint_id
    assert health["generation_status"] == "published"
    assert (health["vitals"]["nodes_live"], health["vitals"]["edges_live"]) == (2, 1)
    # The invariant summary is projected key by key, never whole: the
    # frontier list stays in the database and the duration still arrives.
    invariants = health["vitals"]["invariants"]
    assert invariants["duration_ms"] == 1234
    assert (invariants["cross_source_total"], invariants["cross_source_failed"]) == (5, 1)
    assert invariants["budget_exceeded"] is False

    assert _bytes_per_statement, "no statement was measured"
    too_big = [
        (statement, size) for statement, size in _bytes_per_statement
        if size > _BYTES_PER_CHECKPOINT
    ]
    assert not too_big, (
        f"statements returned more than {_BYTES_PER_CHECKPOINT} bytes for one "
        f"checkpoint: {too_big}"
    )

    # The deltas health serves are the ones the whole-document read gave.
    # Read the full document last, outside the measured reads above.
    from okg.substrate.console_views.health import _projection_delta

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        full = json.loads(conn.execute(
            "SELECT metadata FROM _chronos_branch_interval_checkpoints"
            " WHERE checkpoint_id = %s",
            (checkpoint_id,),
        ).fetchone()[0])
    expected = (_projection_delta(full, "nodes"), _projection_delta(full, "edges"))
    assert expected == (5, 3)
    assert (
        health["vitals"]["nodes_live_delta"], health["vitals"]["edges_live_delta"],
    ) == expected


def test_generation_counts_keep_their_reasons_on_the_projected_read(
    clean_runtime: str, _no_full_checkpoint_read: None,
) -> None:
    """Missing, not-published and non-integer counts each keep their reason."""
    import dataclasses

    from okg.substrate.console_views._chronos import (
        GenerationCountsUnavailable,
        generation_counts,
    )
    from okg.substrate.mcp import open_session

    _seed_checkpoints(clean_runtime, count=2)
    stamp = "2026-09-04T00:00:00+00:00"
    prefix = f"okg:{_DEP}:branch:{_BRANCH}:gen:"
    cases = {
        prefix + "zz-missing": (
            {"status": "published"},
            "pinned generation checkpoint has no stamped node/edge counts",
        ),
        prefix + "zz-blocked": (
            {"status": "blocked", "node_count": 1, "edge_count": 1},
            "pinned generation is not published",
        ),
        prefix + "zz-text": (
            {"status": "published", "node_count": "many", "edge_count": 1},
            "stamped node/edge counts are not integers",
        ),
    }
    for checkpoint_id, (extra, _reason) in cases.items():
        _insert_raw_checkpoint(clean_runtime, checkpoint_id, stamp, {
            "kind": "okg_generation", "deployment_name": _DEP,
            "catalog_version_id": 3, **extra,
        })
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=_DEP)
        for checkpoint_id, (_extra, reason) in cases.items():
            with pytest.raises(GenerationCountsUnavailable) as caught:
                generation_counts(
                    conn, dataclasses.replace(session, generation_id=checkpoint_id),
                )
            assert caught.value.message == reason
        with pytest.raises(GenerationCountsUnavailable) as absent:
            generation_counts(
                conn, dataclasses.replace(session, generation_id=prefix + "none"),
            )
        assert absent.value.message == (
            "pinned generation checkpoint could not be resolved"
        )


def _tombstone(dsn: str, checkpoint_id: str, *, delete_row: bool) -> None:
    """Retire a checkpoint as compaction does: tombstone, then (optionally)
    delete the checkpoint row."""
    import uuid

    from okg.substrate.history_compaction import ensure_history_compaction_schema

    plan_id = str(uuid.uuid4())
    with psycopg.connect(dsn, autocommit=True) as conn:
        ensure_history_compaction_schema(conn)
        with conn.transaction():
            conn.execute(
                "INSERT INTO okg.history_compaction_plans "
                "(plan_id, deployment_name, chronos_branch_id, status, plan) "
                "VALUES (%s, %s, %s, 'applying', '{}'::jsonb)",
                (plan_id, _DEP, _BRANCH),
            )
            conn.execute(
                "INSERT INTO okg.compacted_generations "
                "(checkpoint_id, deployment_name, branch_id, plan_id) "
                "VALUES (%s, %s, %s, %s)",
                (checkpoint_id, _DEP, _BRANCH, plan_id),
            )
            if delete_row:
                conn.execute(
                    "DELETE FROM _chronos_branch_interval_checkpoints"
                    " WHERE checkpoint_id = %s",
                    (checkpoint_id,),
                )


def test_generation_published_at_fails_loud_for_missing_and_compacted(
    clean_runtime: str, _no_full_checkpoint_read: None,
) -> None:
    """The projected timestamp read keeps `get_generation`'s two refusals."""
    import dataclasses

    from chronos_core.branching import BranchNotFoundError
    from okg.substrate.console_views._chronos import generation_published_at
    from okg.substrate.errors import GenerationCompacted
    from okg.substrate.mcp import open_session

    ids = _seed_checkpoints(clean_runtime, count=4)
    retired_kept_row, retired_deleted_row = ids[0], ids[1]
    _tombstone(clean_runtime, retired_kept_row, delete_row=False)
    _tombstone(clean_runtime, retired_deleted_row, delete_row=True)
    prefix = f"okg:{_DEP}:branch:{_BRANCH}:gen:"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=_DEP)
        assert generation_published_at(conn, session) is not None
        with pytest.raises(BranchNotFoundError):
            generation_published_at(
                conn, dataclasses.replace(session, generation_id=prefix + "none"),
            )
        for retired in (retired_kept_row, retired_deleted_row):
            with pytest.raises(GenerationCompacted):
                generation_published_at(
                    conn, dataclasses.replace(session, generation_id=retired),
                )


def test_metadata_postgres_cannot_parse_is_named_not_missing(
    clean_runtime: str, _no_full_checkpoint_read: None,
) -> None:
    """A NaN token (which `json.dumps` writes) fails the `::json` cast.

    That must read as unreadable metadata, not as a missing checkpoint.
    """
    import dataclasses

    from okg.substrate.console_views._chronos import (
        CheckpointMetadataUnreadable,
        GenerationCountsUnavailable,
        generation_counts,
        generation_published_at,
        read_generation_fields,
    )
    from okg.substrate.mcp import open_session

    _seed_checkpoints(clean_runtime, count=2)
    # Pinned before the bad checkpoint exists, as a console session would be.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        pinned = open_session(conn, deployment_name=_DEP)
    nan_id = f"okg:{_DEP}:branch:{_BRANCH}:gen:zz-nan"
    _insert_raw_checkpoint(clean_runtime, nan_id, "2026-09-04T00:00:00+00:00", {
        "kind": "okg_generation", "deployment_name": _DEP,
        "status": "published", "node_count": 1, "edge_count": 1,
        "timing": float("nan"),
    })
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        stored = conn.execute(
            "SELECT metadata FROM _chronos_branch_interval_checkpoints"
            " WHERE checkpoint_id = %s",
            (nan_id,),
        ).fetchone()[0]
        assert "NaN" in stored, "the fixture must store a bare NaN token"
        session = dataclasses.replace(pinned, generation_id=nan_id)
        with pytest.raises(GenerationCountsUnavailable) as counts:
            generation_counts(conn, session)
        assert counts.value.message == (
            "pinned generation checkpoint metadata is not readable JSON"
        )
        with pytest.raises(CheckpointMetadataUnreadable):
            read_generation_fields(conn, nan_id, ("status",))
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        with pytest.raises(CheckpointMetadataUnreadable):
            store.checkout_generation(nan_id, branch=_BRANCH)
        with pytest.raises(CheckpointMetadataUnreadable):
            store.query_generation_once(nan_id, "SELECT 1 AS one", branch=_BRANCH)
        # The timestamp is a column, not metadata: it still reads.
        assert generation_published_at(conn, session) is not None
