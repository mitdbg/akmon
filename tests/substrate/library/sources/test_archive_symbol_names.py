from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.code_graph import CodeStructureSource
from okg.substrate.library.sources._code_graph_common import _archive_symbol_prefix


_CODE = "class A:\n    def run(self): pass\nclass B:\n    def run(self): pass\n"


def _symbols(facts):
    return [f for f in facts if isinstance(f, NodeFact) and f.subtype == "code_symbol"]


def test_archived_qualified_names_preserve_reference_and_handler_lookup(tmp_path: Path) -> None:
    from okg.substrate.library.linkers.code_reference import _Node, _SymbolIndex
    from okg.substrate.library.linkers.interface_handler import (
        _CodeSymbol, _SymbolIndex as HandlerIndex,
    )

    path = "pact/archive/old/handler.py"
    target = tmp_path / path
    target.parent.mkdir(parents=True)
    target.write_text(_CODE)
    source = CodeStructureSource(repo="demo", repo_root=tmp_path)
    symbols = _symbols(source.run("first").facts)
    methods = [s for s in symbols if s.attrs["name"] == "run"]
    assert len(methods) == 2
    prefix = _archive_symbol_prefix("demo", path)
    assert {s.attrs["qualified_name"] for s in methods} == {prefix + "A.run", prefix + "B.run"}
    references = _SymbolIndex([_Node(s.node_id, s.attrs) for s in symbols])
    handlers = HandlerIndex([
        _CodeSymbol(s.node_id, path, s.attrs["name"], s.attrs["qualified_name"],
                    s.attrs["start_line"], s.attrs["end_line"], s.attrs["kind"], s.attrs)
        for s in symbols
    ])
    for method in methods:
        lexical = method.attrs["qualified_name"].removeprefix(prefix)
        resolved, _ = references.resolve_reference({
            "repo": "demo", "path": path, "name": "run", "qualified_name": lexical,
        })
        assert resolved == method.node_id
        assert handlers.by_path_name(path, lexical).node_id == method.node_id
        assert handlers.by_path_name(path, method.attrs["qualified_name"]).node_id == method.node_id


def test_old_cursor_refreshes_only_archives_and_partial_run_cannot_certify_all(tmp_path: Path) -> None:
    paths = ["src/live.py", "archive/old.py", "pact/archive/proposal/mock.py"]
    for path in paths:
        target = tmp_path / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(_CODE)
    source = CodeStructureSource(repo="demo", repo_root=tmp_path)
    initial = source.run("initial")
    before = {s.node_id for s in _symbols(initial.facts)}
    old_cursor = dict(initial.next_cursor)
    old_cursor.pop("archive_qualified_names_version")
    partial = source.run("partial", mode="scope_complete", cursor=old_cursor,
                         sync_scope={"scope_keys": [paths[1]]})
    assert {s.attrs["path"] for s in _symbols(partial.facts)} == {paths[1]}
    assert "archive_qualified_names_version" not in partial.next_cursor
    refreshed = source.run("refresh", cursor=partial.next_cursor)
    refreshed_symbols = _symbols(refreshed.facts)
    assert {s.attrs["path"] for s in refreshed_symbols} == set(paths[1:])
    assert {s.node_id for s in refreshed_symbols} <= before
    unchanged = source.run("unchanged", cursor=refreshed.next_cursor)
    assert list(unchanged.facts) == []
    assert source.last_run_stats["files_scanned"] == 0


@pytest.mark.parametrize("path", ["src/archive/api.py", "archived/api.py", "pact/changes/api.py"])
def test_live_name_scope_is_unchanged(path: str) -> None:
    assert _archive_symbol_prefix("demo", path) == ""


def _write(root: Path, rel: str, text: str) -> None:
    target = root / rel
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text)


def test_same_module_path_files_get_distinct_scoped_names(tmp_path: Path) -> None:
    """Issue #1747: `pkg/util.ts` and `pkg/util.js` share every bare name."""
    _write(tmp_path, "pkg/util.ts", "export function helper() { return 1; }\n")
    _write(tmp_path, "pkg/util.js", "function helper() { return 2; }\n")
    symbols = _symbols(CodeStructureSource(repo="demo", repo_root=tmp_path).run("r").facts)
    helpers = [s for s in symbols if s.attrs["name"] == "helper"]
    assert len(helpers) == 2
    # The bare name still repeats: it is unchanged, and it is not the alias.
    assert {s.attrs["qualified_name"] for s in helpers} == {"helper"}
    assert {s.attrs["scoped_qualified_name"] for s in helpers} == {
        "demo:pkg/util.ts::helper", "demo:pkg/util.js::helper",
    }
    # Every symbol, module rows included, has a scoped name unique to it.
    scoped = [s.attrs["scoped_qualified_name"] for s in symbols]
    assert len(scoped) == len(set(scoped)) == len(symbols)
    assert not any(name.startswith("/") or str(tmp_path) in name for name in scoped)


def test_same_path_in_two_repositories_gets_distinct_scoped_names(tmp_path: Path) -> None:
    names = {}
    for repo in ("alpha", "beta"):
        _write(tmp_path / repo, "pkg/util.py", "def helper(): pass\n")
        symbols = _symbols(CodeStructureSource(repo=repo, repo_root=tmp_path / repo).run("r").facts)
        names[repo] = {s.attrs["scoped_qualified_name"] for s in symbols}
    assert "alpha:pkg/util.py::helper" in names["alpha"]
    assert "beta:pkg/util.py::helper" in names["beta"]
    assert not names["alpha"] & names["beta"]


def test_scoped_name_does_not_change_node_ids(tmp_path: Path) -> None:
    """Node ids stay `symbol:<repo>:<path>:<raw qualified name>:<line>`."""
    paths = ["pkg/util.py", "archive/old/util.py"]
    for path in paths:
        _write(tmp_path, path, _CODE)
    symbols = _symbols(CodeStructureSource(repo="demo", repo_root=tmp_path).run("r").facts)
    assert len(symbols) == 10
    for sym in symbols:
        path = sym.attrs["path"]
        raw = sym.attrs["qualified_name"].removeprefix(_archive_symbol_prefix("demo", path))
        # The id as the code computed it before issue #1747, spelled out.
        assert sym.node_id == f"symbol:demo:{path}:{raw}:{sym.attrs['start_line']}"
        # Archived names already carried the scope, so the two keys agree there.
        assert sym.attrs["scoped_qualified_name"] == f"demo:{path}::{raw}"


def test_scoped_name_escapes_identity_delimiters() -> None:
    from okg.substrate.library.sources._code_graph_common import _scoped_symbol_name

    assert _scoped_symbol_name("repo:x", "a.py", "f") != _scoped_symbol_name("repo", "x:a.py", "f")
    assert _scoped_symbol_name("demo", "a:b.py", "f") != _scoped_symbol_name("demo", "a%3Ab.py", "f")
    assert _scoped_symbol_name("demo", "./pkg\\a.py", "f") == "demo:pkg/a.py::f"


def test_cursor_without_scoped_names_reemits_every_file_once(tmp_path: Path) -> None:
    paths = ["src/live.py", "archive/old.py"]
    for path in paths:
        _write(tmp_path, path, _CODE)
    source = CodeStructureSource(repo="demo", repo_root=tmp_path)
    initial = source.run("initial")
    before = {s.node_id for s in _symbols(initial.facts)}
    assert initial.next_cursor["scoped_qualified_names_version"] == 1
    old_cursor = dict(initial.next_cursor)
    old_cursor.pop("scoped_qualified_names_version")
    partial = source.run("partial", mode="scope_complete", cursor=old_cursor,
                         sync_scope={"scope_keys": [paths[0]]})
    assert {s.attrs["path"] for s in _symbols(partial.facts)} == {paths[0]}
    assert "scoped_qualified_names_version" not in partial.next_cursor
    refreshed = source.run("refresh", cursor=partial.next_cursor)
    refreshed_symbols = _symbols(refreshed.facts)
    assert {s.attrs["path"] for s in refreshed_symbols} == set(paths)
    assert {s.node_id for s in refreshed_symbols} == before
    assert all(s.attrs["scoped_qualified_name"] for s in refreshed_symbols)
    unchanged = source.run("unchanged", cursor=refreshed.next_cursor)
    assert list(unchanged.facts) == []
    assert source.last_run_stats["files_scanned"] == 0


def test_archive_prefix_escapes_identity_delimiters() -> None:
    assert _archive_symbol_prefix("repo:x", "archive/a.py") != _archive_symbol_prefix("repo", "archive/x:a.py")
    assert _archive_symbol_prefix("demo", "archive/a:b.py") != _archive_symbol_prefix("demo", "archive/a%3Ab.py")
    assert _archive_symbol_prefix("demo", "archive/a.py") != _archive_symbol_prefix("other", "archive/a.py")
