"""InterfaceSource must not re-do per-file work it has already done.

Issue #1325 item 3. `interfaces` cost 81.7 s of a 1,469 s okg-workspace sync
pass to emit 430 rows, and the cost was per FILE, not per row: every Python
file in the repo was compiled to an AST twice, and every file paid a
whole-module `ast.walk` looking for argparse subparsers that almost no file
builds.

These assertions count work, not wall time, so they hold on any host.
"""
from __future__ import annotations

import ast
import subprocess
import sys
from collections import Counter
from pathlib import Path

import pytest

from okg.substrate.library.sources import code_graph_interface
from okg.substrate.library.sources._code_graph_common import _analyze_python
from okg.substrate.library.sources.code_graph_interface import (
    InterfaceSource,
    _argparse_parser_paths,
)


FILE_COUNT = 12


def _git_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.email", "test@okg.example"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.name", "OKG Test"],
        check=True,
    )
    # One file that really does build argparse subparsers, and a majority that
    # cannot: that is the production shape the walk was paying for.
    (repo / "cli.py").write_text(
        "import argparse\n"
        "parser = argparse.ArgumentParser()\n"
        "sub = parser.add_subparsers()\n"
        "run_cmd = sub.add_parser('run')\n"
        "def main():\n"
        "    return parser\n"
    )
    (repo / "app.py").write_text(
        "from fastapi import FastAPI\n"
        "app = FastAPI()\n"
        "@app.get('/things')\n"
        "def things():\n"
        "    return {'ok': True}\n"
    )
    for i in range(FILE_COUNT - 2):
        (repo / f"mod{i}.py").write_text(
            f"VALUE = {i}\n"
            f"def helper{i}(x):\n"
            f"    return x + VALUE\n"
        )
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-q", "-m", "fixture"],
        check=True,
    )
    return repo


def test_interface_run_compiles_each_file_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One `ast.parse` per in-scope file, not two.

    Pre-fix the run called `_analyze_python` (which parses) and then
    `ast.parse` on the same text again, so this counted 2 * FILE_COUNT.
    """
    repo = _git_repo(tmp_path)
    fixture_texts = {
        path.read_text(): path.name for path in sorted(repo.glob("*.py"))
    }
    assert len(fixture_texts) == FILE_COUNT
    real_parse = ast.parse
    parses: Counter[str] = Counter()

    def _counting_parse(source, *args, **kwargs):
        name = fixture_texts.get(source if isinstance(source, str) else "")
        if name is not None:
            parses[name] += 1
        return real_parse(source, *args, **kwargs)

    # `ast` is one module object, so patching it here covers the parse inside
    # `_analyze_python` too. Only the fixture files are counted, so unrelated
    # `ast.parse` calls elsewhere in the process cannot mask a regression.
    monkeypatch.setattr(ast, "parse", _counting_parse, raising=True)

    source = InterfaceSource(repo="demo", repo_root=repo)
    facts = list(source.run("run-parse-cost", mode="scope_complete").facts)

    assert dict(parses) == {name: 1 for name in fixture_texts.values()}, (
        f"expected one parse per file, got {dict(parses)}"
    )
    # The endpoint the run exists to find is still there.
    assert any(fact.attrs.get("route") == "/things" for fact in facts)


def test_interface_run_skips_the_argparse_walk_for_files_without_one(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`_argparse_parser_paths` walks only the files that can have a parser.

    The counter wraps `ast.walk` itself, so it records the work that was
    actually done. Dropping the guard from `_argparse_parser_paths` makes this
    count 12 walks instead of 1.
    """
    repo = _git_repo(tmp_path)
    real_walk = ast.walk
    argparse_walks: list[int] = []

    def _counting_walk(node):
        # `_argparse_parser_paths` is the only frame that walks a tree
        # looking for Assign nodes; `_interface_facts` walks the same tree
        # once for its own reasons and must keep doing so.
        frame = sys._getframe(1)
        if frame.f_code is code_graph_interface._argparse_parser_paths.__code__:
            argparse_walks.append(1)
        return real_walk(node)

    monkeypatch.setattr(ast, "walk", _counting_walk, raising=True)

    source = InterfaceSource(repo="demo", repo_root=repo)
    list(source.run("run-argparse-cost", mode="scope_complete").facts)

    # Exactly the one file that builds subparsers, out of FILE_COUNT files.
    assert argparse_walks == [1], (
        f"expected 1 argparse walk, got {len(argparse_walks)} "
        f"for {FILE_COUNT} files"
    )


def test_argparse_guard_agrees_with_the_full_walk() -> None:
    """The guard skips dead work without changing the answer.

    The equivalence is exact for source Python: a call reaches the matcher
    through an `ast.Attribute` whose `attr` is copied from the token, so a
    file whose text lacks the identifier cannot produce a parser path. The
    known exceptions are pathological -- an identifier the tokenizer
    NFKC-normalizes to `add_parser`, or one split across an explicit line
    join -- and both fail closed, dropping a CLI endpoint rather than
    inventing one.
    """
    with_parser = (
        "import argparse\n"
        "parser = argparse.ArgumentParser()\n"
        "sub = parser.add_subparsers()\n"
        "run_cmd = sub.add_parser('run')\n"
        "deep = run_cmd.add_subparsers()\n"
        "leaf = deep.add_parser('once')\n"
    )
    without_parser = "VALUE = 1\ndef helper(x):\n    return x + VALUE\n"

    for text in (with_parser, without_parser):
        tree = ast.parse(text)
        assert _argparse_parser_paths(tree, text=text) == (
            _argparse_parser_paths(tree)
        )

    tree = ast.parse(with_parser)
    assert _argparse_parser_paths(tree, text=with_parser) == {
        "sub": "",
        "run_cmd": "run",
        "deep": "run",
        "leaf": "run once",
    }


def test_analyze_python_publishes_the_tree_it_visited() -> None:
    """The reused tree is the SAME object the symbol pass walked."""
    text = "class A:\n    def b(self):\n        return 1\n"
    analyzer = _analyze_python("demo", "a.py", text)

    assert isinstance(analyzer.tree, ast.Module)
    assert [s.qname for s in analyzer.symbols if s.kind == "class"] == ["A"]
    assert any(
        isinstance(node, ast.ClassDef) and node.name == "A"
        for node in ast.walk(analyzer.tree)
    )
