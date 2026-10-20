"""Database test for a git-files branch switch with an unchanged tree
(PACT git-files-branch-switch-3780, issue #3780).

A checkout that moves to another branch at the same tree must move every
file's member_of link to the new branch record and retract the old branch
record, for an inline record set and for one stored by reference. Its own
file so the substrate-db lane can own it.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest


def _rows(dsn: str, sql: str, params: dict | None = None) -> list[dict]:
    import psycopg

    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        return list(
            ChronosGraphStore.from_connection(conn, ensure_schema=False).query(
                sql, params or {}
            )
        )


def _branch_ids(dsn: str) -> set[str]:
    return {
        str(r["node_id"])
        for r in _rows(
            dsn,
            "SELECT node_id FROM okg.graph_nodes WHERE subtype = :subtype",
            {"subtype": "git_branch"},
        )
    }


def _member_links(dsn: str) -> set[tuple[str, str]]:
    return {
        (str(r["src"]), str(r["dst"]))
        for r in _rows(
            dsn,
            "SELECT src, dst FROM okg.graph_edges WHERE edge_type = :edge_type",
            {"edge_type": "member_of"},
        )
    }


@pytest.mark.postgres
@pytest.mark.parametrize("stored", ["inline", "by-reference"])
def test_branch_switch_with_unchanged_tree_moves_branch_and_file_links(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stored: str,
) -> None:
    """Before the fix the run after the switch took the unchanged path: the
    old branch record and every link to it stayed, and the new branch record
    had no file linked to it."""
    import psycopg

    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from okg.substrate.library.sources.git_graph_files import GitFilesSource
    from tests.pytest.catalog_fixture_builders import seed_catalog

    monkeypatch.setenv("OKG_DISABLE_CHANGE_PROBE_GATE", "1")
    if stored == "by-reference":
        monkeypatch.setenv("OKG_SOURCE_RUNNER_INLINE_RECORD_SET_MAX_BYTES", "1")
    repo = tmp_path / "repo"
    repo.mkdir()
    git = ["git", "-C", str(repo)]
    subprocess.run([*git, "init", "-q", "-b", "main"], check=True)
    subprocess.run([*git, "config", "user.email", "test@example.com"], check=True)
    subprocess.run([*git, "config", "user.name", "OKG Test"], check=True)
    for i in range(3):
        (repo / f"f{i}.py").write_text(f"X{i} = {i}\n", encoding="utf-8")
    subprocess.run([*git, "add", "-A"], check=True)
    subprocess.run([*git, "commit", "-q", "-m", "one"], check=True)
    file_ids = {f"file:demo:f{i}.py" for i in range(3)}

    def files() -> GitFilesSource:
        return GitFilesSource(
            repo="demo", repo_root=repo, source_name="demo.git-files"
        )

    preview = list(files().run("p", mode="scope_complete").facts)
    subtypes = {f.node_id: f.subtype for f in preview if isinstance(f, NodeFact)}
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=sorted(set(subtypes.values())),
            narrowings=sorted(
                {
                    (subtypes[f.src], f.edge_type, subtypes[f.dst])
                    for f in preview
                    if isinstance(f, EdgeFact)
                }
            ),
        )
    runner = SourceRunner(
        clean_runtime,
        source_name="demo.git-files",
        deployment_name="demo",
        deletion_semantics="missing_from_completed_scope",
    )

    first = runner.run(files(), mode="scope_complete")
    assert first.next_cursor is not None
    stored_key = "record_set_ref" if stored == "by-reference" else "record_set"
    assert stored_key in first.next_cursor
    assert _branch_ids(clean_runtime) == {"branch:demo:main"}
    assert _member_links(clean_runtime) == {
        (f, "branch:demo:main") for f in file_ids
    }

    # A new branch cut from the current one: same tree, same HEAD.
    subprocess.run([*git, "checkout", "-q", "-b", "feature"], check=True)
    second = runner.run(files(), mode="scope_complete")

    assert _branch_ids(clean_runtime) == {"branch:demo:feature"}
    assert _member_links(clean_runtime) == {
        (f, "branch:demo:feature") for f in file_ids
    }
    # The old branch record and its three links.
    assert (second.retractions_node, second.retractions_edge) == (1, 3)
    file_owners = {
        str(r["node_id"]): r["source"]
        for r in _rows(
            clean_runtime,
            "SELECT node_id, source FROM okg.graph_nodes WHERE subtype = :s",
            {"s": "source_file"},
        )
    }
    assert {file_owners.get(f) for f in file_ids} == {"demo.git-files"}

    # Nothing changed since: the next run retracts nothing and the graph
    # stays as it is.
    third = runner.run(files(), mode="scope_complete")
    assert (third.retractions_node, third.retractions_edge) == (0, 0)
    assert (third.nodes_appended, third.edges_appended) == (0, 0)
    assert _branch_ids(clean_runtime) == {"branch:demo:feature"}
    assert _member_links(clean_runtime) == {
        (f, "branch:demo:feature") for f in file_ids
    }
