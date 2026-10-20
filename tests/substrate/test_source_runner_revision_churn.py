"""Real runs of one source through SourceRunner (Postgres).

The lineage envelope embeds each fact's source_revision, so a revision that
moves while the content does not used to rewrite every record: on a normal run
unless the source declared ``content_hash`` revisions (issue #2076), and on
every authoritative replay regardless, because replay skipped the persisted
comparison entirely (#2076, #1731). The rule now: on any run, including
authoritative replay, a fact whose content matches the current live row on the
branch is not written, and a row that is missing on the branch, or differs, is
still written, so replay keeps repairing the branch.
"""
from __future__ import annotations

import hashlib
import json
from uuid import uuid4

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest.runner import SourceRunner
from okg.substrate.library.sources.base import EdgeFact, NodeFact, SourceRun
from okg.substrate.nomos.source_lineage import LINEAGE_ATTR
from tests.substrate._db_helpers import ensure_published_generation, seed_catalog_version

pytestmark = pytest.mark.postgres


class _RunIdRevisionAdapter:
    """Constant content; the content_hash revision also carries the run id."""

    name = "revision-churn"
    profile = "reference_catalog"

    def run(self, run_id: str, *, mode: str = "cursor") -> SourceRun:
        revision = {"run_id": run_id, "content_hash": "sha256:constant", "n_records": 2}
        facts = [
            NodeFact(node_id="rel:1", subtype="concept", attrs={"text": "one", "title": "R1"},
                     source_record_id={"id": "rel:1"}, source_revision=revision, op="I"),
            NodeFact(node_id="rel:2", subtype="concept", attrs={"text": "two", "title": "R2"},
                     source_record_id={"id": "rel:2"}, source_revision=revision, op="I"),
            EdgeFact(src="rel:2", dst="rel:1", edge_type="supersedes", attrs={"rank": 1},
                     source_record_id={"id": "rel:2->rel:1"}, source_revision=revision, op="I"),
        ]
        return SourceRun(facts=facts, completed_scope=True, next_cursor=None, run_mode=mode)


class _RecordsAdapter:
    """Three records and one edge; the revision is chosen per test.

    ``revision="run"`` stamps a run-scoped ``updated_at``-style revision on
    every fact (the Jira shape in #2076). ``revision="batch"`` stamps one hash
    of the whole batch on every fact (the openspec / pact / git-files shape in
    #1731), so editing one record moves every record's revision.
    """

    name = "revision-churn"
    profile = "reference_catalog"

    def __init__(self, titles: dict[str, str], *, revision: str) -> None:
        self.titles = dict(titles)
        self.revision = revision

    def run(self, run_id: str, *, mode: str = "cursor") -> SourceRun:
        if self.revision == "run":
            revision = {"run_id": run_id, "updated_at": "2026-09-01T00:00:00Z"}
        elif self.revision == "repo":
            # git_graph_files' shape (#2308): one repo-wide {tree_sha,
            # head_sha} on every record, moving on every run.
            sha = hashlib.sha1(run_id.encode()).hexdigest()
            revision = {"tree_sha": sha, "head_sha": sha}
        else:
            digest = hashlib.sha256(json.dumps(self.titles, sort_keys=True).encode()).hexdigest()
            revision = {"source_state_sha256": digest}
        facts: list = [
            NodeFact(node_id=node_id, subtype="concept", attrs={"title": title},
                     source_record_id={"id": node_id}, source_revision=revision, op="I")
            for node_id, title in sorted(self.titles.items())
        ]
        facts.append(EdgeFact(src="rel:2", dst="rel:1", edge_type="supersedes", attrs={"rank": 1},
                              source_record_id={"id": "rel:2->rel:1"}, source_revision=revision, op="I"))
        return SourceRun(facts=facts, completed_scope=True, next_cursor=None, run_mode=mode)


def _rows(dsn: str, source: str) -> list[dict]:
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        return store.query(
            "SELECT node_id, attrs, source_revision FROM okg.graph_nodes "
            "WHERE source = :source ORDER BY node_id",
            {"source": source},
        )
    finally:
        store.close()


def _physical_versions(dsn: str, source: str) -> dict[str, list[str]]:
    """Per node and per edge: the row-version stamps of every physical row.

    ``xmin`` is Postgres' row-version stamp. A rewrite closes the open
    interval row (a new ``xmin``) and inserts a new one, so an unchanged list
    means the row was not rewritten.
    """
    out: dict[str, list[str]] = {}
    with psycopg.connect(dsn, autocommit=True) as conn:
        for table, key in (("okg.graph_nodes", "node_id"), ("okg.graph_edges", "edge_id::text")):
            (physical,) = conn.execute(
                "SELECT physical_table FROM _chronos_branch_tables "
                "WHERE backend = 'interval' AND table_name = %s",
                (table,),
            ).fetchone()
            for ident, xmin in conn.execute(
                f'SELECT {key}, xmin::text FROM "{physical}" WHERE source = %s ORDER BY 1, 2',
                (source,),
            ).fetchall():
                out.setdefault(f"{table}:{ident}", []).append(xmin)
    return out


def _seed(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)


@pytest.mark.parametrize("kind", ["content_hash", "release_tag"])
def test_unchanged_source_with_run_id_revision(clean_runtime: str, kind):
    """A normal run over unchanged content appends nothing for any declared
    revision kind; before #2076 a non-content_hash kind rewrote every row."""
    _seed(clean_runtime)
    source = "test.revision-churn"
    first = SourceRunner(clean_runtime, source_name=source, source_revision_kind=kind).run(
        _RunIdRevisionAdapter(), mode="release_new")
    assert (first.nodes_appended, first.edges_appended) == (2, 1), first
    rows_after_first = _rows(clean_runtime, source)
    assert [row["node_id"] for row in rows_after_first] == ["rel:1", "rel:2"]
    assert all(LINEAGE_ATTR in row["attrs"] for row in rows_after_first)

    second = SourceRunner(clean_runtime, source_name=source, source_revision_kind=kind).run(
        _RunIdRevisionAdapter(), mode="release_new")
    assert (second.nodes_appended, second.edges_appended) == (0, 0), second.as_dict()
    assert (second.dedupe_revision_only_node, second.dedupe_revision_only_edge) == (2, 1)
    assert second.dedupe_kept_by_key == {}
    assert second.dedupe_reference == "live_branch"
    # The stored rows keep the first run's lineage: nothing was rewritten.
    assert _rows(clean_runtime, source) == rows_after_first


def test_normal_runs_with_a_moving_repo_revision_keep_one_version_per_record(clean_runtime: str):
    """#2308: consecutive NORMAL runs of a source whose repo-wide revision
    moves every run while no record's content changes. Each node and edge
    must keep exactly one row in the Chronos interval history, not one per
    run (okg-workspace grew ~30 versions per record over 39 runs)."""
    _seed(clean_runtime)
    source = "test.revision-churn"
    titles = {"rel:1": "R1", "rel:2": "R2", "rel:3": "R3"}
    results = []
    for generation_id in range(1, 5):
        results.append(SourceRunner(clean_runtime, source_name=source).run(
            _RecordsAdapter(titles, revision="repo"), mode="release_new"))
        # A published checkpoint after each run pins the interval history,
        # as on a real deployment; without one Chronos overwrites the
        # unpinned interval in place and a rewrite leaves no extra row.
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            ensure_published_generation(conn, generation_id)
    versions = _physical_versions(clean_runtime, source)
    assert len(versions) == 4, versions
    assert {key: len(rows) for key, rows in versions.items()} == dict.fromkeys(versions, 1)
    appended = [(r.nodes_appended, r.edges_appended) for r in results]
    assert appended == [(3, 1), (0, 0), (0, 0), (0, 0)]
    assert [r.dedupe_reference for r in results] == ["live_branch"] * 4


def test_authoritative_replay_over_unchanged_updated_at_source_writes_nothing(clean_runtime: str):
    """#2076: an ``updated_at`` source whose revision carries the run id,
    replayed with ``reset_cursor=True``, appends no node or edge and leaves
    every physical row untouched."""
    _seed(clean_runtime)
    source = "test.revision-churn"
    titles = {"rel:1": "R1", "rel:2": "R2"}
    first = SourceRunner(clean_runtime, source_name=source, source_revision_kind="updated_at").run(
        _RecordsAdapter(titles, revision="run"), mode="release_new")
    assert (first.nodes_appended, first.edges_appended) == (2, 1), first.as_dict()
    rows_before = _rows(clean_runtime, source)
    versions_before = _physical_versions(clean_runtime, source)

    replay = SourceRunner(clean_runtime, source_name=source, source_revision_kind="updated_at").run(
        _RecordsAdapter(titles, revision="run"), mode="release_new", reset_cursor=True)
    assert (replay.nodes_appended, replay.edges_appended) == (0, 0), replay.as_dict()
    assert replay.dedupe_reference == "live_branch"
    assert (replay.dedupe_revision_only_node, replay.dedupe_revision_only_edge) == (2, 1)
    assert _rows(clean_runtime, source) == rows_before
    assert _physical_versions(clean_runtime, source) == versions_before


def test_replay_after_one_edit_under_batch_revision_writes_one_node(clean_runtime: str):
    """#1731: one record edited under a batch-wide revision moves every
    record's revision, but only the edited record's content changed, so a
    replay writes exactly that one node."""
    _seed(clean_runtime)
    source = "test.revision-churn"
    titles = {"rel:1": "R1", "rel:2": "R2", "rel:3": "R3"}
    first = SourceRunner(clean_runtime, source_name=source).run(
        _RecordsAdapter(titles, revision="batch"), mode="release_new")
    assert (first.nodes_appended, first.edges_appended) == (3, 1), first.as_dict()
    versions_before = _physical_versions(clean_runtime, source)

    edited = {**titles, "rel:3": "R3 edited"}
    replay = SourceRunner(clean_runtime, source_name=source).run(
        _RecordsAdapter(edited, revision="batch"), mode="release_new", reset_cursor=True)
    assert (replay.nodes_appended, replay.edges_appended) == (1, 0), replay.as_dict()
    assert replay.dedupe_kept_by_key == {
        "title": 1, LINEAGE_ATTR + ".source_revision": 1,
    }
    versions_after = _physical_versions(clean_runtime, source)
    unchanged = {key for key in versions_before if key != "okg.graph_nodes:rel:3"}
    assert {key: versions_after[key] for key in unchanged} == {
        key: versions_before[key] for key in unchanged
    }
    rows = {row["node_id"]: row["attrs"]["title"] for row in _rows(clean_runtime, source)}
    assert rows == {"rel:1": "R1", "rel:2": "R2", "rel:3": "R3 edited"}


def test_replay_repairs_a_missing_row_and_a_drifted_row(clean_runtime: str):
    """Replay is still graph repair: a row deleted from the branch and a row
    whose attributes drifted on the branch are both written back, while a
    third row that already matches is not rewritten."""
    _seed(clean_runtime)
    source = "test.revision-churn"
    titles = {"rel:1": "R1", "rel:2": "R2", "rel:3": "R3"}
    first = SourceRunner(clean_runtime, source_name=source, source_revision_kind="updated_at").run(
        _RecordsAdapter(titles, revision="run"), mode="release_new")
    assert (first.nodes_appended, first.edges_appended) == (3, 1), first.as_dict()

    # Damage the branch behind the runner's back: rel:3 disappears and rel:2
    # carries a title the source never emitted.
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        store.apply_facts(
            [
                NodeFact(node_id="rel:3", subtype="concept", attrs={},
                         source_record_id={"id": "rel:3"}, source_revision={}, op="R"),
                NodeFact(node_id="rel:2", subtype="concept", attrs={"title": "drifted"},
                         source_record_id={"id": "rel:2"}, source_revision={"v": "damage"}, op="I"),
            ],
            branch="default",
            source=source,
        )
    finally:
        store.close()
    damaged = {row["node_id"]: row["attrs"]["title"] for row in _rows(clean_runtime, source)}
    assert damaged == {"rel:1": "R1", "rel:2": "drifted"}
    versions_before = _physical_versions(clean_runtime, source)

    replay = SourceRunner(clean_runtime, source_name=source, source_revision_kind="updated_at").run(
        _RecordsAdapter(titles, revision="run"), mode="release_new", reset_cursor=True)
    assert (replay.nodes_appended, replay.edges_appended) == (2, 0), replay.as_dict()
    assert replay.dedupe_reference == "live_branch"
    repaired = {row["node_id"]: row["attrs"]["title"] for row in _rows(clean_runtime, source)}
    assert repaired == titles
    versions_after = _physical_versions(clean_runtime, source)
    assert versions_after["okg.graph_nodes:rel:1"] == versions_before["okg.graph_nodes:rel:1"]
    edge_keys = [key for key in versions_before if key.startswith("okg.graph_edges:")]
    assert len(edge_keys) == 1
    assert versions_after[edge_keys[0]] == versions_before[edge_keys[0]]


def test_a_writer_outside_the_runner_does_not_rewrite_a_revision_only_change(
    clean_runtime: str,
) -> None:
    """#2335: the graph writer's own no-op check used the whole lineage and
    the ``source_revision`` column, so a writer outside the runner (the
    publisher's derived re-application, admission, compensation, MCP) stored a
    new version of unchanged content whenever an input's revision moved. It
    now decides with the runner's rule; a real change is still written."""
    _seed(clean_runtime)
    source = "_publisher_derived"

    def facts(revision: str, title: str = "R1") -> list:
        rev = {"run_id": revision}
        return [
            NodeFact(node_id="rel:1", subtype="concept", attrs={"title": title},
                     source_record_id={"id": "rel:1"}, source_revision=rev, op="I"),
            NodeFact(node_id="rel:2", subtype="concept", attrs={"title": "R2"},
                     source_record_id={"id": "rel:2"}, source_revision=rev, op="I"),
            EdgeFact(src="rel:2", dst="rel:1", edge_type="supersedes", attrs={"rank": 1},
                     source_record_id={"id": "rel:2->rel:1"}, source_revision=rev,
                     confidence=0.7, op="I"),
        ]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(facts("run-1"), source=source, run_id=str(uuid4()))
        before = _physical_versions(clean_runtime, source)
        moved = store.apply_facts(facts("run-2"), source=source, run_id=str(uuid4()))
        assert (moved.nodes_upserted, moved.edges_upserted) == (0, 0)
        assert _physical_versions(clean_runtime, source) == before

        changed = store.apply_facts(facts("run-3", title="R1 edited"), source=source, run_id=str(uuid4()))
        assert (changed.nodes_upserted, changed.edges_upserted) == (1, 0)
    rows = {row["node_id"]: row for row in _rows(clean_runtime, source)}
    assert rows["rel:1"]["attrs"]["title"] == "R1 edited"
    assert rows["rel:2"]["attrs"][LINEAGE_ATTR]
