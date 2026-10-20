"""Database test for an unchanged git-files run over an external record set
(PACT bug-gitfiles-external-noop-3666, issue #3776).

Its own file so the substrate-db lane can own it without taking in
test_git_graph_files.py, whose other tests name paths outside that lane.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest


# --- Issue #3666: an unchanged run over an external record set ----------------


def _graph_rows(dsn: str, subtype: str) -> dict[str, dict]:
    import psycopg

    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(conn, ensure_schema=False).query(
            "SELECT node_id, source, attrs FROM okg.graph_nodes WHERE subtype = :subtype",
            {"subtype": subtype},
        )
    return {str(r["node_id"]): r for r in rows}


@pytest.mark.postgres
@pytest.mark.parametrize(
    "with_history", [False, True], ids=["files-only", "history-first"]
)
def test_unchanged_files_run_with_external_record_set_retracts_nothing(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    with_history: bool,
) -> None:
    """A record set over the inline limit is stored by reference. Before the
    fix an unchanged scope-complete run built no record set, the runner read
    its branch record alone as the whole footprint and retracted every file,
    or, with git history declaring the files, handed them to git history."""
    import psycopg

    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from okg.substrate.library.sources.git_graph_files import GitFilesSource
    from okg.substrate.library.sources.git_graph_history import GitHistorySource
    from tests.pytest.catalog_fixture_builders import seed_catalog

    monkeypatch.setenv("OKG_DISABLE_CHANGE_PROBE_GATE", "1")
    monkeypatch.setenv("OKG_SOURCE_RUNNER_INLINE_RECORD_SET_MAX_BYTES", "1")
    repo = tmp_path / "repo"
    repo.mkdir()
    git = ["git", "-C", str(repo)]
    subprocess.run([*git, "init", "-q", "-b", "main"], check=True)
    subprocess.run([*git, "config", "user.email", "test@example.com"], check=True)
    subprocess.run([*git, "config", "user.name", "OKG Test"], check=True)
    for i in range(4):
        (repo / f"f{i}.py").write_text(f"X{i} = {i}\n", encoding="utf-8")
    subprocess.run([*git, "add", "-A"], check=True)
    subprocess.run([*git, "commit", "-q", "-m", "one"], check=True)
    files_ids = {f"file:demo:f{i}.py" for i in range(4)}

    def files() -> GitFilesSource:
        # A registry-built source knows its name, which lets it verify the
        # external record-set proof and take the no-change path.
        return GitFilesSource(
            repo="demo", repo_root=repo, source_name="demo.git-files"
        )

    def history() -> GitHistorySource:
        return GitHistorySource(repo="demo", repo_root=repo)

    preview = [
        *files().run("p1", mode="scope_complete").facts,
        *history().run("p2", mode="scope_complete").facts,
    ]
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
    file_runner = SourceRunner(
        clean_runtime,
        source_name="demo.git-files",
        deployment_name="demo",
        deletion_semantics="missing_from_completed_scope",
    )
    history_runner = SourceRunner(
        clean_runtime, source_name="demo.git-history", deployment_name="demo"
    )

    if with_history:
        history_runner.run(history(), mode="scope_complete")
    first = file_runner.run(files(), mode="scope_complete")
    assert first.next_cursor is not None
    assert "record_set_ref" in first.next_cursor
    if with_history:
        history_runner.run(history(), mode="scope_complete", reset_cursor=True)

    # Same tree, new HEAD: the branch record must still move.
    subprocess.run([*git, "commit", "-q", "--allow-empty", "-m", "empty"], check=True)
    new_head = subprocess.check_output([*git, "rev-parse", "HEAD"], text=True).strip()
    second_source = files()
    second = file_runner.run(second_source, mode="scope_complete")

    assert second_source.last_run_stats["scope_status"] == "full_unchanged_noop"
    assert (second.retractions_node, second.retractions_edge) == (0, 0)
    owners = {k: r["source"] for k, r in _graph_rows(clean_runtime, "source_file").items()}
    assert {owners.get(i) for i in files_ids} == {"demo.git-files"}
    branch = _graph_rows(clean_runtime, "git_branch")["branch:demo:main"]
    assert branch["attrs"]["head_commit_sha"] == new_head
    assert second.next_cursor is not None
    assert "record_set_ref" in second.next_cursor or "record_set" in second.next_cursor

    # The carried authority is intact: a later deletion retracts exactly it.
    subprocess.run([*git, "rm", "-q", "f3.py"], check=True)
    subprocess.run([*git, "commit", "-q", "-m", "drop f3"], check=True)
    third = file_runner.run(files(), mode="scope_complete")
    assert third.retractions_node == 1
    owners = {k: r["source"] for k, r in _graph_rows(clean_runtime, "source_file").items()}
    assert {owners.get(i) for i in files_ids - {"file:demo:f3.py"}} == {"demo.git-files"}
