from __future__ import annotations

import sys
from pathlib import Path

import pytest

from okg.substrate.library.extraction.code.lsp import _close_persistent_clients
from okg.substrate.library.sources.code_graph import CodeLspResolutionSource
from tests.substrate.library.extraction.code.test_lsp_semantic_resolution import (
    _hermetic_lsp_server,
)


@pytest.fixture(autouse=True)
def _reset_persistent_lsp_clients():
    """Persistent LSP clients live at process scope; close them between tests
    so one test's reused server never leaks into another."""
    _close_persistent_clients()
    yield
    _close_persistent_clients()


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _source(
    repo: Path,
    server: Path,
    **kwargs,
) -> CodeLspResolutionSource:
    target_uri = (repo / "src" / "b.py").resolve().as_uri()
    return CodeLspResolutionSource(
        repo="demo",
        repo_root=repo,
        server_command=[sys.executable, str(server)],
        server_name="hermetic-python-lsp",
        server_version="0.1",
        initialization_options={
            "definitions": {
                "src/a.py:4:11": {
                    "uri": target_uri,
                    "range": {
                        "start": {"line": 0, "character": 0},
                        "end": {"line": 1, "character": 12},
                    },
                },
                "src/a.py:5:11": {
                    "uri": target_uri,
                    "range": {
                        "start": {"line": 0, "character": 0},
                        "end": {"line": 1, "character": 12},
                    },
                },
            },
        },
        **kwargs,
    )


def _fixture(tmp_path: Path) -> tuple[Path, Path]:
    repo = tmp_path / "repo"
    _write(
        repo / "src" / "a.py",
        "from .b import helper\n\n"
        "def caller():\n"
        "    return helper()\n",
    )
    _write(repo / "src" / "b.py", "def helper():\n    return 1\n")
    server = tmp_path / "hermetic_lsp_server.py"
    _hermetic_lsp_server(server)
    return repo, server


def test_no_change_run_skips_file_open_and_lsp_requests(
    tmp_path: Path,
) -> None:
    repo, server = _fixture(tmp_path)
    source = _source(repo, server)

    first = source.run("run-1", mode="scope_complete")
    first_facts = list(first.facts)
    assert len(first_facts) == 2

    cursor = dict(first.next_cursor or {})
    assert first.record_set is not None
    cursor["record_set"] = first.record_set.to_cursor()
    second = source.run("run-2", mode="scope_complete", cursor=cursor)

    assert list(second.facts) == []
    assert source.last_run_stats["changed_file_count"] == 0
    assert source.last_run_stats["files_opened"] == 0
    assert source.last_run_stats["lsp_requests"] == 0
    assert source.last_run_stats["status"] == "no_change"


def test_no_record_set_cursor_still_skips_unchanged_files(
    tmp_path: Path,
) -> None:
    repo, server = _fixture(tmp_path)
    source = _source(repo, server)

    first = source.run("run-1", mode="scope_complete")
    cursor = dict(first.next_cursor or {})
    cursor.pop("record_set", None)
    cursor.pop("record_set_ref", None)

    second = source.run("run-2", mode="scope_complete", cursor=cursor)

    assert list(second.facts) == []
    assert source.last_run_stats["scope_status"] == "full"
    assert source.last_run_stats["files_seen"] == 2
    assert source.last_run_stats["changed_file_count"] == 0
    assert source.last_run_stats["files_opened"] == 0
    assert source.last_run_stats["lsp_requests"] == 0
    assert source.last_run_stats["status"] == "no_change"


def test_single_delta_run_is_frontier_bounded(tmp_path: Path) -> None:
    repo, server = _fixture(tmp_path)
    source = _source(repo, server)

    first = source.run("run-1", mode="scope_complete")
    cursor = dict(first.next_cursor or {})
    assert first.record_set is not None
    cursor["record_set"] = first.record_set.to_cursor()

    _write(
        repo / "src" / "a.py",
        "from .b import helper\n\n"
        "# moved caller down one line\n"
        "def caller():\n"
        "    return helper()\n",
    )
    second = source.run(
        "run-2",
        mode="scope_complete",
        cursor=cursor,
        sync_scope={"scope_keys": ["src/a.py"]},
    )
    facts = list(second.facts)

    assert len(facts) == 2
    assert source.last_run_stats["scope_status"] == "scoped"
    assert source.last_run_stats["changed_file_count"] == 1
    assert source.last_run_stats["files_seen"] == 1
    assert source.last_run_stats["files_opened"] == 2
    assert source.last_run_stats["dependency_fanout"] == 1
    assert source.last_run_stats["record_set_authority"] == "replacement_slice"
    assert source.last_run_stats["lsp_requests"] == 1
    assert source.last_run_stats["references_resolved"] == 1
    assert second.record_set_replacement_keys == {"src/a.py"}
    assert second.next_cursor is not None
    assert "src/b.py" in second.next_cursor["files"]
    assert "record_set" not in second.next_cursor
    assert {
        fact.attrs["resolution_kind"] for fact in facts
    } == {"cross_file_lsp"}


def test_callee_delta_refreshes_reverse_importing_callers(tmp_path: Path) -> None:
    repo, server = _fixture(tmp_path)
    source = _source(repo, server)

    first = source.run("run-1", mode="scope_complete")
    cursor = dict(first.next_cursor or {})
    assert first.record_set is not None
    cursor["record_set"] = first.record_set.to_cursor()

    _write(repo / "src" / "b.py", "def helper():\n    return 2\n")
    second = source.run(
        "run-2",
        mode="scope_complete",
        cursor=cursor,
        sync_scope={"scope_keys": ["src/b.py"]},
    )
    facts = list(second.facts)

    assert facts == []
    assert source.last_run_stats["changed_file_count"] == 1
    assert source.last_run_stats["dependency_fanout"] == 0
    assert source.last_run_stats["reverse_dependency_fanout"] == 1
    assert source.last_run_stats["files_opened"] == 0
    assert source.last_run_stats["lsp_requests"] == 0
    assert source.last_run_stats["references_reused"] == 1
    assert source.last_run_stats["cache_hit_files"] == 1
    assert source.last_run_stats["cache_remapped_files"] == 0
    assert second.record_set_replacement_keys == {"src/b.py"}


def test_callee_line_move_remaps_cached_targets_by_semantic_key(
    tmp_path: Path,
) -> None:
    repo, server = _fixture(tmp_path)
    source = _source(repo, server)

    first = source.run("run-1", mode="scope_complete")
    cursor = dict(first.next_cursor or {})
    assert first.record_set is not None
    cursor["record_set"] = first.record_set.to_cursor()

    _write(
        repo / "src" / "b.py",
        "# moved helper down\n"
        "def helper():\n"
        "    return 2\n",
    )
    second = source.run(
        "run-2",
        mode="scope_complete",
        cursor=cursor,
        sync_scope={"scope_keys": ["src/b.py"]},
    )
    facts = list(second.facts)
    targets = {
        fact.dst
        for fact in facts
        if fact.edge_type == "references"
    }

    assert source.last_run_stats["lsp_requests"] == 0
    assert source.last_run_stats["references_reused"] == 1
    assert source.last_run_stats["cache_remapped_files"] == 1
    assert second.record_set_replacement_keys == {"src/a.py", "src/b.py"}
    assert targets == {"symbol:demo:src/b.py:helper:2"}


def test_delete_only_scope_retracts_without_lsp_boot(tmp_path: Path) -> None:
    repo, server = _fixture(tmp_path)
    source = _source(repo, server)

    first = source.run("run-1", mode="scope_complete")
    cursor = dict(first.next_cursor or {})
    assert first.record_set is not None
    cursor["record_set"] = first.record_set.to_cursor()

    (repo / "src" / "a.py").unlink()
    second = source.run(
        "run-2",
        mode="scope_complete",
        cursor=cursor,
        sync_scope={"scope_keys": ["src/a.py"]},
    )

    assert list(second.facts) == []
    assert source.last_run_stats["status"] == "deleted_scope"
    assert source.last_run_stats["files_opened"] == 0
    assert source.last_run_stats["lsp_requests"] == 0
    assert second.record_set_replacement_keys == {"src/a.py"}
    assert second.next_cursor is not None
    assert "src/a.py" not in second.next_cursor["files"]


def test_over_budget_hot_path_defers_and_reconcile_completes(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    _write(
        repo / "src" / "a.py",
        "from .b import helper\n\n"
        "def caller_a():\n"
        "    return helper()\n",
    )
    _write(
        repo / "src" / "c.py",
        "from .b import helper\n\n"
        "def caller_c():\n"
        "    return helper()\n",
    )
    _write(repo / "src" / "b.py", "def helper():\n    return 1\n")
    server = tmp_path / "hermetic_lsp_server.py"
    _hermetic_lsp_server(server)
    target_uri = (repo / "src" / "b.py").resolve().as_uri()
    source = CodeLspResolutionSource(
        repo="demo",
        repo_root=repo,
        server_command=[sys.executable, str(server)],
        server_name="hermetic-python-lsp",
        server_version="0.1",
        hot_path_max_lsp_requests=1,
        initialization_options={
            "definitions": {
                "src/a.py:4:11": {
                    "uri": target_uri,
                    "range": {
                        "start": {"line": 0, "character": 0},
                        "end": {"line": 1, "character": 12},
                    },
                },
                "src/c.py:4:11": {
                    "uri": target_uri,
                    "range": {
                        "start": {"line": 0, "character": 0},
                        "end": {"line": 1, "character": 12},
                    },
                },
            },
        },
    )

    hot = source.run("run-1", mode="scope_complete")
    hot_facts = list(hot.facts)
    assert hot.completed_scope is False
    assert source.last_run_stats["status"] == "deferred"
    assert source.last_run_stats["lsp_requests"] == 1
    assert source.last_run_stats["deferred_files"] == 1
    assert source.last_run_stats["deferred_lsp_requests"] == 1
    assert hot.next_cursor is not None
    assert hot.next_cursor["lsp_deferred"]["paths"]
    assert len([f for f in hot_facts if f.edge_type == "references"]) == 1

    reconcile = source.run(
        "run-2",
        mode="reconcile",
        cursor=hot.next_cursor,
    )
    reconcile_facts = list(reconcile.facts)

    assert reconcile.completed_scope is True
    assert source.last_run_stats["status"] == "resolved"
    assert source.last_run_stats["lsp_requests"] == 1
    assert source.last_run_stats["deferred_files"] == 0
    assert reconcile.next_cursor is not None
    assert "lsp_deferred" not in reconcile.next_cursor
    assert len([f for f in reconcile_facts if f.edge_type == "references"]) == 2


def test_scoped_run_reads_only_scope_keys_inside_the_walk(
    tmp_path: Path,
) -> None:
    """Issue #3259: a scope key narrows the walk, never widens it."""
    from okg.substrate.library.sources._code_graph_common import DEFAULT_EXCLUDES

    repo, server = _fixture(tmp_path)
    _write(repo / "gen" / "c.py", "def generated():\n    return 3\n")
    _write(repo / "README.md", "# demo\n")
    source = _source(
        repo,
        server,
        exclude_globs=(*DEFAULT_EXCLUDES, "**/gen/**"),
    )

    first = source.run("run-1", mode="scope_complete")
    list(first.facts)
    cursor = dict(first.next_cursor or {})
    assert first.record_set is not None
    cursor["record_set"] = first.record_set.to_cursor()
    assert "gen/c.py" not in set(cursor.get("files") or {})

    _write(repo / "gen" / "c.py", "def generated():\n    return 4\n")
    _write(
        repo / "src" / "a.py",
        "from .b import helper\n\n"
        "# moved caller down one line\n"
        "def caller():\n"
        "    return helper()\n",
    )
    second = source.run(
        "run-2",
        mode="scope_complete",
        cursor=cursor,
        sync_scope={"scope_keys": ["src/a.py", "gen/c.py", "README.md"]},
    )
    facts = list(second.facts)

    assert facts
    assert all(
        fact.source_record_id.get("path") != "gen/c.py" for fact in facts
    )
    assert source.last_run_stats["files_seen"] == 1
    assert source.last_run_stats["scope_keys"] == 3
    assert source.last_run_stats["scope_keys_outside_source_paths"] == 2
    assert second.record_set_replacement_keys == {"src/a.py"}
    assert "gen/c.py" not in set((second.next_cursor or {}).get("files") or {})
