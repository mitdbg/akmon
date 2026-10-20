from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _init_git(repo: Path) -> None:
    subprocess.run(["git", "-C", str(repo), "init", "-q"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.name", "Test"],
        check=True,
    )
    subprocess.run(["git", "-C", str(repo), "add", "-A"], check=True)
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-q", "-m", "initial"],
        check=True,
    )


def test_code_readers_without_snapshot_binding_fail_closed(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.code_graph import (
        CodeDataFlowSource,
        CodeQLDataFlowSource,
        CodeStructureSource,
        SemgrepDataFlowSource,
    )

    assert (
        CodeStructureSource(
            repo="demo",
            repo_root=tmp_path,
        ).probe_short_circuit_safe
        is False
    )
    assert (
        CodeDataFlowSource(
            repo="demo",
            repo_root=tmp_path,
        ).probe_short_circuit_safe
        is False
    )
    assert (
        SemgrepDataFlowSource(
            repo="demo",
            repo_root=tmp_path,
        ).probe_short_circuit_safe
        is False
    )
    assert (
        CodeQLDataFlowSource(
            repo="demo",
            repo_root=tmp_path,
        ).probe_short_circuit_safe
        is False
    )


def test_code_structure_source_emits_symbols_imports_references(tmp_path: Path) -> None:
    from okg.substrate.library.sources.code_graph import CodeStructureSource

    _write(
        tmp_path / "src" / "app.py",
        """
import os

def helper():
    return os.getenv("HOME")

def main():
    return helper()
""".strip(),
    )

    source = CodeStructureSource(repo="demo", repo_root=tmp_path)
    facts = list(source.run("run-1", mode="cursor").facts)

    node_subtypes = {f.subtype for f in facts if isinstance(f, NodeFact)}
    edge_keys = {(f.src, f.edge_type, f.dst) for f in facts if isinstance(f, EdgeFact)}

    assert {"code_symbol", "code_import", "code_reference"} <= node_subtypes
    assert any(e[1] == "contains" and e[0] == "file:demo:src/app.py" for e in edge_keys)
    assert any(e[1] == "calls" for e in edge_keys)


def test_code_structure_source_normalizes_javascript_usage_evidence(
    tmp_path: Path,
) -> None:
    pytest.importorskip("tree_sitter_language_pack")

    from okg.substrate.library.sources.code_graph import CodeStructureSource

    _write(
        tmp_path / "src" / "app.js",
        """
import { readFile as read } from "node:fs";
function closeReader() {}
function init(state, button) {
  state.ops?.setVisibleOps?.(["startup"]);
  button.addEventListener("click", closeReader);
  return { closeReader };
}
export { closeReader as close };
""".strip(),
    )

    source = CodeStructureSource(
        repo="demo",
        repo_root=tmp_path,
        include_exts=[".js"],
    )
    facts = list(source.run("run-1", mode="cursor").facts)
    refs = [
        f.attrs
        for f in facts
        if isinstance(f, NodeFact) and f.subtype == "code_reference"
    ]
    imports = [
        f.attrs
        for f in facts
        if isinstance(f, NodeFact) and f.subtype == "code_import"
    ]

    optional = next(
        ref for ref in refs
        if ref["qualified_name"] == "state.ops.setVisibleOps"
    )
    assert optional["name"] == "setVisibleOps"
    assert optional["syntax_optional_chain"] is True
    assert optional["reference_kind"] == "call"
    assert {
        ref["reference_kind"]
        for ref in refs
        if ref["name"] == "closeReader"
    } >= {"callback_argument", "returned_object", "public_export"}
    assert any(
        imp["module"] == "node:fs"
        and imp["name"] == "readFile"
        and imp["alias"] == "read"
        for imp in imports
    )


def test_code_structure_source_emits_tree_sitter_languages(tmp_path: Path) -> None:
    pytest.importorskip("tree_sitter_language_pack")

    from okg.substrate.library.sources.code_graph import CodeStructureSource

    _write(
        tmp_path / "src" / "app.js",
        "import fs from 'fs';\nfunction jsMain() { return helper(); }\nfunction helper() { return fs.readFileSync('x'); }\n",
    )
    _write(
        tmp_path / "src" / "app.ts",
        "import path from 'path';\nfunction tsMain(): string { return path.join('a', 'b'); }\n",
    )
    _write(
        tmp_path / "src" / "app.go",
        "package main\nimport \"fmt\"\nfunc goMain() { fmt.Println(\"hi\") }\n",
    )
    _write(
        tmp_path / "src" / "app.rs",
        "use std::fs;\nfn rust_main() { fs::read_to_string(\"x\"); }\n",
    )
    _write(
        tmp_path / "src" / "app.c",
        "#include <stdio.h>\nvoid c_main() { puts(\"hi\"); }\n",
    )
    _write(
        tmp_path / "src" / "app.cpp",
        "#include <iostream>\nclass Runner { void run() { helper(); } };\nvoid helper() {}\n",
    )

    source = CodeStructureSource(
        repo="demo",
        repo_root=tmp_path,
        include_exts=[".js", ".ts", ".go", ".rs", ".c", ".cpp"],
    )
    facts = list(source.run("run-1", mode="cursor").facts)

    symbols = [
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "code_symbol"
    ]
    imports = [
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "code_import"
    ]
    references = [
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "code_reference"
    ]
    languages = {f.attrs["language"] for f in symbols}

    assert {"javascript", "typescript", "go", "rust", "c", "cpp"} <= languages
    assert len(imports) >= 6
    assert len(references) >= 6


def test_code_structure_source_fails_loudly_for_missing_parser(tmp_path: Path) -> None:
    from okg.substrate.library.extraction.code.dispatch import (
        CodeExtractorNotFound,
    )
    from okg.substrate.library.sources.code_graph import CodeStructureSource

    _write(tmp_path / "src" / "app.kt", "fun main() = println(\"hi\")\n")
    source = CodeStructureSource(
        repo="demo",
        repo_root=tmp_path,
        include_exts=[".kt"],
    )

    with pytest.raises(CodeExtractorNotFound):
        list(source.run("run-1", mode="cursor").facts)


def test_code_structure_probe_manifest_covers_concrete_extractor_registry() -> None:
    from okg.substrate.library.extraction.code.dispatch import available_languages
    from okg.substrate.library.sources.code_graph_structure import (
        _code_extractor_emit_targets,
    )

    targets = dict(_code_extractor_emit_targets())

    assert set(targets) == set(available_languages())
    assert targets["python"].__name__ == "PythonAstSymbolExtractor"
    assert all(isinstance(target, type) for target in targets.values())


def test_code_structure_run_binds_global_applied_probe_token(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.change_probe import tokens_equal
    from okg.substrate.library.sources.code_graph import CodeStructureSource

    _write(tmp_path / "src" / "app.py", "def main():\n    return 1\n")
    subprocess.run(["git", "-C", str(tmp_path), "init", "-q"], check=True)
    subprocess.run(
        ["git", "-C", str(tmp_path), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(tmp_path), "config", "user.name", "Test"],
        check=True,
    )
    subprocess.run(["git", "-C", str(tmp_path), "add", "-A"], check=True)
    subprocess.run(
        ["git", "-C", str(tmp_path), "commit", "-q", "-m", "initial"],
        check=True,
    )

    source = CodeStructureSource(repo="demo", repo_root=tmp_path)
    run = source.run("code-applied-token", mode="reconcile")
    list(run.facts)

    assert run.applied_token_scope == "global"
    assert run.applied_token_authority == "exact"
    assert run.applied_raw_token is not None
    assert tokens_equal(run.applied_raw_token, source.change_probe.build_token())


def test_code_structure_snapshot_reads_same_mtime_content_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources.change_probe import tokens_equal
    from okg.substrate.library.sources.code_graph import CodeStructureSource

    path = _write(tmp_path / "src" / "app.py", "def main():\n    return 1\n")
    subprocess.run(["git", "-C", str(tmp_path), "init", "-q"], check=True)
    subprocess.run(
        ["git", "-C", str(tmp_path), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(tmp_path), "config", "user.name", "Test"],
        check=True,
    )
    subprocess.run(["git", "-C", str(tmp_path), "add", "-A"], check=True)
    subprocess.run(
        ["git", "-C", str(tmp_path), "commit", "-q", "-m", "initial"],
        check=True,
    )

    source = CodeStructureSource(repo="demo", repo_root=tmp_path)
    first = source.run("run-1", mode="scope_complete")
    list(first.facts)
    assert first.record_set is not None
    cursor = dict(first.next_cursor or {})
    cursor["record_set"] = first.record_set.to_cursor()

    original_stat = path.stat()
    path.write_text("def main():\n    return 2\n", encoding="utf-8")
    os.utime(
        path,
        ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
    )
    parsed: list[str] = []
    original = source._facts_for_file

    def _record_parse(
        full_path: Path,
        rel_path: str,
        text: str,
        text_hash: str,
    ) -> list[NodeFact | EdgeFact]:
        parsed.append(text)
        return original(full_path, rel_path, text, text_hash)

    monkeypatch.setattr(source, "_facts_for_file", _record_parse)
    second = source.run("run-2", mode="scope_complete", cursor=cursor)
    list(second.facts)

    assert parsed == ["def main():\n    return 2\n"]
    assert second.applied_token_authority == "exact"
    assert second.applied_token_scope == "global"
    assert second.applied_raw_token is not None
    assert not tokens_equal(first.applied_raw_token, second.applied_raw_token)
    assert tokens_equal(
        second.applied_raw_token,
        source.change_probe.build_token(),
    )


def test_code_structure_snapshot_token_binds_ignored_source_file(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.change_probe import tokens_equal
    from okg.substrate.library.sources.code_graph import CodeStructureSource

    _write(tmp_path / ".gitignore", "generated.py\n")
    ignored = _write(tmp_path / "generated.py", "def generated():\n    return 1\n")
    subprocess.run(["git", "-C", str(tmp_path), "init", "-q"], check=True)
    subprocess.run(
        ["git", "-C", str(tmp_path), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(tmp_path), "config", "user.name", "Test"],
        check=True,
    )
    subprocess.run(["git", "-C", str(tmp_path), "add", ".gitignore"], check=True)
    subprocess.run(
        ["git", "-C", str(tmp_path), "commit", "-q", "-m", "initial"],
        check=True,
    )

    source = CodeStructureSource(repo="demo", repo_root=tmp_path)
    before = source.change_probe.build_token()
    ignored.write_text("def generated():\n    return 2\n", encoding="utf-8")
    after = source.change_probe.build_token()
    run = source.run("ignored-source", mode="reconcile")
    facts = list(run.facts)

    assert not tokens_equal(before, after)
    assert any(
        fact.source_record_id.get("path") == "generated.py"
        for fact in facts
    )
    assert tokens_equal(run.applied_raw_token, source.change_probe.build_token())


def test_code_structure_applied_token_is_snapshot_not_post_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources.change_probe import tokens_equal
    from okg.substrate.library.sources.code_graph import CodeStructureSource

    path = _write(tmp_path / "app.py", "def main():\n    return 1\n")
    subprocess.run(["git", "-C", str(tmp_path), "init", "-q"], check=True)
    subprocess.run(
        ["git", "-C", str(tmp_path), "config", "user.email", "test@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(tmp_path), "config", "user.name", "Test"],
        check=True,
    )
    subprocess.run(["git", "-C", str(tmp_path), "add", "-A"], check=True)
    subprocess.run(
        ["git", "-C", str(tmp_path), "commit", "-q", "-m", "initial"],
        check=True,
    )

    source = CodeStructureSource(repo="demo", repo_root=tmp_path)
    original = source._facts_for_file

    def _mutate_after_capture(
        full_path: Path,
        rel_path: str,
        text: str,
        text_hash: str,
    ) -> list[NodeFact | EdgeFact]:
        path.write_text("def main():\n    return 2\n", encoding="utf-8")
        return original(full_path, rel_path, text, text_hash)

    monkeypatch.setattr(source, "_facts_for_file", _mutate_after_capture)
    run = source.run("mutated-after-capture", mode="reconcile")
    list(run.facts)

    assert run.applied_token_authority == "exact"
    assert run.applied_token_scope == "global"
    assert run.applied_raw_token is not None
    assert not tokens_equal(
        run.applied_raw_token,
        source.change_probe.build_token(),
    )


def test_code_data_flow_snapshot_contract_mutation_matrix(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.ingest.runner import SourceRunner
    from okg.substrate.library.sources.change_probe import (
        CURSOR_TOKEN_KEY,
        run_probe,
        tokens_equal,
    )
    from okg.substrate.library.sources.code_graph import CodeDataFlowSource

    path = _write(
        tmp_path / "src" / "app.py",
        "import os\nVALUE = os.getenv('FIRST_KEY')\n",
    )
    _init_git(tmp_path)
    source = CodeDataFlowSource(
        repo="demo",
        repo_root=tmp_path,
        include_exts=[".py"],
    )
    assert source.probe_short_circuit_safe is True
    assert SourceRunner._adapter_probe_short_circuit_safe(source) is True

    baseline = source.change_probe.build_token()
    original_stat = path.stat()
    os.utime(
        path,
        ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns + 1),
    )
    assert tokens_equal(
        baseline,
        source.change_probe.build_token(),
    ), "mtime-only churn must not change source identity"

    first = source.run("data-flow-first", mode="scope_complete")
    first_facts = list(first.facts)
    assert any(
        fact.node_id == "config:demo:env:FIRST_KEY"
        for fact in first_facts
        if isinstance(fact, NodeFact)
    )
    assert first.applied_token_scope == "global"
    assert first.applied_token_authority == "exact"
    assert tokens_equal(first.applied_raw_token, baseline)
    assert first.record_set is not None
    cursor = dict(first.next_cursor or {})
    cursor["record_set"] = first.record_set.to_cursor()
    cursor[CURSOR_TOKEN_KEY] = first.applied_raw_token

    parsed: list[str] = []
    original_reader = source._facts_for_file

    def _record_reader(
        rel_path: str,
        text: str,
        text_hash: str,
    ) -> list[NodeFact | EdgeFact]:
        parsed.append(rel_path)
        return original_reader(rel_path, text, text_hash)

    monkeypatch.setattr(source, "_facts_for_file", _record_reader)
    unchanged = run_probe(
        source.change_probe,
        cursor=cursor,
        sync_scope=None,
    )
    assert unchanged.changed is False
    second = source.run(
        "data-flow-unchanged",
        mode="scope_complete",
        cursor=cursor,
    )
    assert list(second.facts) == []
    assert parsed == []

    path.write_text(
        "import os\nVALUE = os.getenv('SECOND_KEY')\n",
        encoding="utf-8",
    )
    os.utime(
        path,
        ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
    )
    edited_token = source.change_probe.build_token()
    assert not tokens_equal(baseline, edited_token)
    edited = source.run(
        "data-flow-edited",
        mode="scope_complete",
        cursor=cursor,
    )
    edited_facts = list(edited.facts)
    assert parsed == ["src/app.py"]
    assert any(
        fact.node_id == "config:demo:env:SECOND_KEY"
        for fact in edited_facts
        if isinstance(fact, NodeFact)
    )
    assert tokens_equal(edited.applied_raw_token, edited_token)

    added_path = _write(
        tmp_path / "src" / "added.py",
        "import os\nVALUE = os.getenv('ADDED_KEY')\n",
    )
    added_token = source.change_probe.build_token()
    assert not tokens_equal(edited_token, added_token)
    edited_cursor = dict(edited.next_cursor or {})
    assert edited.record_set is not None
    edited_cursor["record_set"] = edited.record_set.to_cursor()
    added = source.run(
        "data-flow-added",
        mode="scope_complete",
        cursor=edited_cursor,
    )
    list(added.facts)
    assert added.record_set is not None
    assert set(added.record_set.records) == {"src/app.py", "src/added.py"}
    added_cursor = dict(added.next_cursor or {})
    added_cursor["record_set"] = added.record_set.to_cursor()
    added_path.unlink()
    deleted_token = source.change_probe.build_token()
    assert not tokens_equal(added_token, deleted_token)
    assert tokens_equal(edited_token, deleted_token)
    deleted = source.run(
        "data-flow-deleted",
        mode="scope_complete",
        cursor=added_cursor,
    )
    list(deleted.facts)
    assert deleted.record_set is not None
    assert set(deleted.record_set.records) == {"src/app.py"}

    changed_config = CodeDataFlowSource(
        repo="demo",
        repo_root=tmp_path,
        include_exts=[".py", ".js"],
    )
    assert not tokens_equal(
        source.change_probe.build_token(),
        changed_config.change_probe.build_token(),
    )

    def _probe_error():
        raise OSError("forced snapshot failure")

    monkeypatch.setattr(source.change_probe, "capture_snapshot", _probe_error)
    failed = run_probe(
        source.change_probe,
        cursor=cursor,
        sync_scope=None,
    )
    assert failed.changed is True
    assert failed.token == {}
    assert failed.reason.startswith("probe_error:OSError")


def test_code_data_flow_applied_token_is_captured_not_post_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources.change_probe import tokens_equal
    from okg.substrate.library.sources.code_graph import CodeDataFlowSource

    path = _write(
        tmp_path / "app.py",
        "import os\nVALUE = os.getenv('BEFORE')\n",
    )
    _init_git(tmp_path)
    source = CodeDataFlowSource(repo="demo", repo_root=tmp_path)
    original_reader = source._facts_for_file

    def _mutate_after_capture(
        rel_path: str,
        text: str,
        text_hash: str,
    ) -> list[NodeFact | EdgeFact]:
        path.write_text(
            "import os\nVALUE = os.getenv('AFTER')\n",
            encoding="utf-8",
        )
        return original_reader(rel_path, text, text_hash)

    monkeypatch.setattr(source, "_facts_for_file", _mutate_after_capture)
    run = source.run("data-flow-aba-fence", mode="reconcile")
    facts = list(run.facts)

    assert any(
        fact.node_id == "config:demo:env:BEFORE"
        for fact in facts
        if isinstance(fact, NodeFact)
    )
    assert not any(
        fact.node_id == "config:demo:env:AFTER"
        for fact in facts
        if isinstance(fact, NodeFact)
    )
    assert run.applied_token_authority == "exact"
    assert run.applied_token_scope == "global"
    assert not tokens_equal(
        run.applied_raw_token,
        source.change_probe.build_token(),
    )


def test_code_data_flow_git_visible_scope_is_exact_for_ignored_and_symlinked_files(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.change_probe import tokens_equal
    from okg.substrate.library.sources.code_graph import CodeDataFlowSource

    _write(tmp_path / ".gitignore", "ignored.py\n")
    _write(
        tmp_path / "app.py",
        "import os\nVALUE = os.getenv('APP_KEY')\n",
    )
    ignored = _write(
        tmp_path / "ignored.py",
        "import os\nVALUE = os.getenv('IGNORED_KEY')\n",
    )
    payload = _write(
        tmp_path / "payload.txt",
        "import os\nVALUE = os.getenv('LINKED_KEY')\n",
    )
    (tmp_path / "linked.py").symlink_to("payload.txt")
    _init_git(tmp_path)

    source = CodeDataFlowSource(
        repo="demo",
        repo_root=tmp_path,
        include_exts=[".py"],
    )
    baseline = source.change_probe.build_token()
    facts = list(source.run("git-visible", mode="reconcile").facts)
    node_ids = {
        fact.node_id for fact in facts if isinstance(fact, NodeFact)
    }
    assert "config:demo:env:APP_KEY" in node_ids
    assert "config:demo:env:IGNORED_KEY" not in node_ids
    assert "config:demo:env:LINKED_KEY" not in node_ids

    ignored.write_text(
        "import os\nVALUE = os.getenv('IGNORED_CHANGED')\n",
        encoding="utf-8",
    )
    payload.write_text(
        "import os\nVALUE = os.getenv('LINKED_CHANGED')\n",
        encoding="utf-8",
    )
    assert tokens_equal(
        baseline,
        source.change_probe.build_token(),
    ), "inputs outside the Git-visible reader scope must not affect identity"

    (tmp_path / "dynamic.py").symlink_to("payload.txt")
    dynamic_token = source.change_probe.build_token()
    assert not tokens_equal(baseline, dynamic_token)
    dynamic_facts = list(
        source.run("git-visible-untracked-link", mode="reconcile").facts
    )
    assert any(
        fact.node_id == "config:demo:env:LINKED_CHANGED"
        for fact in dynamic_facts
        if isinstance(fact, NodeFact)
    )
    payload.write_text(
        "import os\nVALUE = os.getenv('DYNAMIC_CHANGED')\n",
        encoding="utf-8",
    )
    assert not tokens_equal(
        dynamic_token,
        source.change_probe.build_token(),
    ), "a reader-visible untracked symlink binds the followed target bytes"


def test_code_graph_walk_prunes_excluded_directories(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources._code_graph_common import _walk_files

    _write(tmp_path / "src" / "app.py", "def app():\n    return 1\n")
    _write(
        tmp_path / ".claude" / "worktrees" / "wf" / "src" / "ignored.py",
        "def ignored():\n    return 2\n",
    )

    excluded_root = (tmp_path / ".claude").resolve()
    real_scandir = os.scandir

    def guarded_scandir(path: str | os.PathLike[str]):
        path_obj = Path(path).resolve()
        if path_obj == excluded_root or excluded_root in path_obj.parents:
            raise AssertionError(f"walk descended into excluded directory: {path_obj}")
        return real_scandir(path)

    monkeypatch.setattr(os, "scandir", guarded_scandir)

    walked = _walk_files(
        tmp_path,
        include_exts=[".py"],
        exclude_globs=[".claude/**", "**/.claude/worktrees/**"],
        max_files=10,
        max_bytes_per_file=1_000_000,
    )

    assert [p.relative_to(tmp_path).as_posix() for p in walked] == ["src/app.py"]


@pytest.mark.postgres
def test_code_structure_source_publishes_tree_sitter_languages(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    pytest.importorskip("tree_sitter_language_pack")

    import psycopg
    import yaml

    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.code_graph import CodeStructureSource
    from okg.substrate.library.sources.git_graph_files import GitFilesSource
    from okg.substrate.publisher import Publisher

    repo = tmp_path / "repo"
    _write(repo / "src" / "app.py", "def py_main():\n    return helper()\ndef helper():\n    return 1\n")
    _write(repo / "src" / "app.js", "function jsMain() { return helper(); }\nfunction helper() { return 1; }\n")
    _write(repo / "src" / "app.ts", "function tsMain(): number { return helper(); }\nfunction helper(): number { return 1; }\n")
    _write(repo / "src" / "app.go", "package main\nfunc goMain() { helper() }\nfunc helper() {}\n")
    _write(repo / "src" / "app.rs", "fn rust_main() { helper(); }\nfn helper() {}\n")
    _write(repo / "src" / "app.c", "void helper() {}\nvoid c_main() { helper(); }\n")
    _write(repo / "src" / "app.cpp", "void helper() {}\nvoid cpp_main() { helper(); }\n")

    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-code-structure",
        "description": "multi-language code structure publish fixture",
        "modules": [
            "git_graph",
            "openspec",
            "agent_sessions",
            "compute_env",
            "ci",
            "code_graph",
        ],
    }))
    (dep / "schemas").mkdir()

    composed = compose_catalog(dep)
    with psycopg.connect(clean_runtime) as conn:
        apply_composed_catalog(conn, composed)

    SourceRunner(clean_runtime, source_name="fixture.git-files").run(
        GitFilesSource(repo="demo", repo_root=repo),
        mode="scope_complete",
    )
    SourceRunner(clean_runtime, source_name="fixture.code-structure").run(
        CodeStructureSource(
            repo="demo",
            repo_root=repo,
            include_exts=[".py", ".js", ".ts", ".go", ".rs", ".c", ".cpp"],
        ),
        mode="scope_complete",
    )

    out = Publisher(clean_runtime, "test-code-structure", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-code-structure")).cycle()
    assert out["status"] == "published", out

    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        graph = store.checkout_generation(out["generation_id"])
        counts = {
            str(row["language"]): int(row["count"])
            for row in graph.query(
                """
                SELECT attrs->>'language' AS language, count(*) AS count
                  FROM okg.graph_nodes
                 WHERE subtype = 'code_symbol'
                 GROUP BY 1
                """
            )
        }
        call_edges = int(graph.query(
            """
            SELECT count(*) AS count
              FROM okg.graph_edges
             WHERE edge_type = 'calls'
            """
        )[0]["count"])
    finally:
        store.close()

    assert {
        "python",
        "javascript",
        "typescript",
        "go",
        "rust",
        "c",
        "cpp",
    } <= {language for language, count in counts.items() if count > 0}
    assert call_edges > 0


@pytest.mark.postgres
def test_code_structure_scope_complete_retracts_stale_symbol_ids(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    pytest.importorskip("tree_sitter_language_pack")


    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.code_graph import CodeStructureSource

    repo = tmp_path / "repo"
    _write(
        repo / "src" / "app.js",
        "function helper() { return 1; }\n",
    )
    source = CodeStructureSource(
        repo="demo",
        repo_root=repo,
        include_exts=[".js"],
    )
    runner = SourceRunner(
        clean_runtime,
        source_name="fixture.code-structure",
    )

    first = runner.run(source, mode="scope_complete")
    assert first.retractions_node == 0

    _write(
        repo / "src" / "app.js",
        "// inserted header\nfunction helper() { return 1; }\n",
    )
    second = runner.run(source, mode="scope_complete")

    old_symbol = "symbol:demo:src/app.js:helper:1"
    new_symbol = "symbol:demo:src/app.js:helper:2"
    assert second.retractions_node > 0
    assert _graph_node_exists(clean_runtime, old_symbol) is False
    assert _graph_node_exists(clean_runtime, new_symbol) is True


def test_code_structure_scoped_scope_complete_carries_prior_record_set(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("tree_sitter_language_pack")

    from okg.substrate.library.sources.code_graph import CodeStructureSource

    monkeypatch.setenv("OKG_FILE_WALK_USE_MTIME", "0")
    repo = tmp_path / "repo"
    _write(repo / "src" / "app.js", "function app() { return 1; }\n")
    _write(repo / "src" / "util.js", "function util() { return 2; }\n")
    source = CodeStructureSource(
        repo="demo",
        repo_root=repo,
        include_exts=[".js"],
    )

    first = source.run("run-1", mode="scope_complete")
    assert first.record_set is not None
    cursor = dict(first.next_cursor or {})
    cursor["record_set"] = first.record_set.to_cursor()

    _write(
        repo / "src" / "app.js",
        "// inserted header\nfunction app() { return 1; }\n",
    )
    second = source.run(
        "run-2",
        mode="scope_complete",
        cursor=cursor,
        sync_scope={"scope_keys": ["src/app.js"]},
    )

    assert second.completed_scope is True
    assert second.record_set is not None
    assert set(second.record_set.records) == {"src/app.js", "src/util.js"}
    app_nodes = second.record_set.records["src/app.js"].nodes_by_id()
    util_nodes = second.record_set.records["src/util.js"].nodes_by_id()
    assert "symbol:demo:src/app.js:app:1" not in app_nodes
    assert "symbol:demo:src/app.js:app:2" in app_nodes
    assert "symbol:demo:src/util.js:util:1" in util_nodes


def test_code_structure_scoped_scope_complete_removes_deleted_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("tree_sitter_language_pack")

    from okg.substrate.library.sources.code_graph import CodeStructureSource

    monkeypatch.setenv("OKG_FILE_WALK_USE_MTIME", "0")
    repo = tmp_path / "repo"
    _write(repo / "src" / "app.js", "function app() { return 1; }\n")
    _write(repo / "src" / "util.js", "function util() { return 2; }\n")
    source = CodeStructureSource(
        repo="demo",
        repo_root=repo,
        include_exts=[".js"],
    )

    first = source.run("run-1", mode="scope_complete")
    assert first.record_set is not None
    cursor = dict(first.next_cursor or {})
    cursor["record_set"] = first.record_set.to_cursor()

    (repo / "src" / "app.js").unlink()
    second = source.run(
        "run-2",
        mode="scope_complete",
        cursor=cursor,
        sync_scope={"scope_keys": ["src/app.js"]},
    )

    assert second.completed_scope is True
    assert second.record_set is not None
    assert set(second.record_set.records) == {"src/util.js"}
    assert set((second.next_cursor or {})["files"]) == {"src/util.js"}


def test_code_structure_scope_complete_carries_prior_record_set_on_no_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("tree_sitter_language_pack")

    from okg.substrate.library.sources.code_graph import CodeStructureSource

    monkeypatch.setenv("OKG_FILE_WALK_USE_MTIME", "0")
    repo = tmp_path / "repo"
    _write(repo / "src" / "app.js", "function app() { return 1; }\n")
    _write(repo / "src" / "util.js", "function util() { return 2; }\n")
    source = CodeStructureSource(
        repo="demo",
        repo_root=repo,
        include_exts=[".js"],
    )

    first = source.run("run-1", mode="scope_complete")
    assert first.record_set is not None
    cursor = dict(first.next_cursor or {})
    cursor["record_set"] = first.record_set.to_cursor()

    calls: list[str] = []

    def _spy_facts_for_file(
        path: Path,
        rel_path: str,
        text: str,
        text_hash: str,
    ) -> list[NodeFact | EdgeFact]:
        calls.append(rel_path)
        raise AssertionError("unchanged complete run should not parse files")

    monkeypatch.setattr(source, "_facts_for_file", _spy_facts_for_file)
    second = source.run("run-2", mode="scope_complete", cursor=cursor)

    assert list(second.facts) == []
    assert calls == []
    assert second.completed_scope is True
    assert second.record_set is not None
    assert set(second.record_set.records) == {"src/app.js", "src/util.js"}
    assert set((second.next_cursor or {})["files"]) == {
        "src/app.js",
        "src/util.js",
    }


def test_code_structure_external_record_set_ref_no_change_skips_parsing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("tree_sitter_language_pack")

    from okg.substrate.library.sources.code_graph import CodeStructureSource

    monkeypatch.setenv("OKG_FILE_WALK_USE_MTIME", "0")
    repo = tmp_path / "repo"
    _write(repo / "src" / "app.js", "function app() { return 1; }\n")
    source = CodeStructureSource(
        repo="demo",
        repo_root=repo,
        include_exts=[".js"],
    )
    first = source.run("run-1", mode="cursor")
    list(first.facts)
    assert first.next_cursor is not None

    def _spy_facts_for_file(
        path: Path,
        rel_path: str,
        text: str,
        text_hash: str,
    ) -> list[NodeFact | EdgeFact]:
        raise AssertionError(f"unchanged file parsed: {rel_path}")

    monkeypatch.setattr(source, "_facts_for_file", _spy_facts_for_file)
    cursor = {
        **first.next_cursor,
        "record_set_ref": {
            "storage": "okg.source_record_set_blobs",
            "sha256": "abc",
        },
        "record_set_summary": {"record_count": 1},
    }
    second = source.run("run-2", mode="scope_complete", cursor=cursor)

    assert list(second.facts) == []
    assert second.completed_scope is True
    assert second.record_set is None
    assert second.next_cursor is not None
    assert second.next_cursor["record_set_ref"] == cursor["record_set_ref"]


def test_code_structure_git_frontier_no_change_skips_parsing_after_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("tree_sitter_language_pack")

    from okg.substrate.library.sources._git_helpers import run_git
    from okg.substrate.library.sources.code_graph import CodeStructureSource

    repo = tmp_path / "repo"
    _write(repo / "src" / "app.js", "function app() { return 1; }\n")
    run_git(repo, ["init"])
    run_git(repo, ["config", "user.email", "fixture@example.test"])
    run_git(repo, ["config", "user.name", "OKG Fixture"])
    run_git(repo, ["add", "src/app.js"])
    run_git(repo, ["commit", "-m", "initial"])

    source = CodeStructureSource(
        repo="demo",
        repo_root=repo,
        include_exts=[".js"],
        git_frontier_skip=True,
    )
    first = source.run("run-1", mode="scope_complete")
    list(first.facts)
    assert first.record_set is not None
    assert first.next_cursor is not None
    assert first.next_cursor.get("git_frontier") is not None

    cursor = dict(first.next_cursor)
    cursor["record_set"] = first.record_set.to_cursor()

    def _unexpected_parse(*_args, **_kwargs):
        raise AssertionError("unchanged git frontier should skip parsing")

    monkeypatch.setattr(source, "_facts_for_file", _unexpected_parse)

    second = source.run("run-2", mode="scope_complete", cursor=cursor)

    assert list(second.facts) == []
    assert second.completed_scope is True
    assert second.record_set is None
    assert second.next_cursor is not None
    assert second.next_cursor["files"] == first.next_cursor["files"]
    assert second.next_cursor["git_frontier"] == first.next_cursor["git_frontier"]
    assert second.next_cursor["record_set"] == cursor["record_set"]
    assert source.last_run_stats["scope_status"] == "git_frontier_no_change"
    assert source.last_run_stats["files_seen"] == 0


def test_code_structure_external_record_set_ref_replaces_changed_file_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("tree_sitter_language_pack")

    from okg.substrate.library.sources.code_graph import CodeStructureSource

    monkeypatch.setenv("OKG_FILE_WALK_USE_MTIME", "0")
    repo = tmp_path / "repo"
    _write(repo / "src" / "app.js", "function app() { return 1; }\n")
    _write(repo / "src" / "util.js", "function util() { return 2; }\n")
    source = CodeStructureSource(
        repo="demo",
        repo_root=repo,
        include_exts=[".js"],
    )
    first = source.run("run-1", mode="cursor")
    list(first.facts)
    assert first.next_cursor is not None

    original = source._facts_for_file
    calls: list[str] = []

    def _spy_facts_for_file(
        path: Path,
        rel_path: str,
        text: str,
        text_hash: str,
    ) -> list[NodeFact | EdgeFact]:
        calls.append(rel_path)
        return original(path, rel_path, text, text_hash)

    monkeypatch.setattr(source, "_facts_for_file", _spy_facts_for_file)
    _write(
        repo / "src" / "app.js",
        "// inserted header\nfunction app() { return 1; }\n",
    )
    cursor = {
        **first.next_cursor,
        "record_set_ref": {
            "storage": "okg.source_record_set_blobs",
            "sha256": "abc",
        },
        "record_set_summary": {"record_count": 2},
    }
    second = source.run("run-2", mode="scope_complete", cursor=cursor)
    facts = list(second.facts)

    assert calls == ["src/app.js"]
    assert {
        fact.source_record_id.get("path")
        for fact in facts
        if isinstance(fact, (NodeFact, EdgeFact))
    } == {"src/app.js"}
    assert second.completed_scope is True
    assert second.record_set is not None
    assert set(second.record_set.records) == {"src/app.js"}
    assert set(second.record_set_replacement_keys or ()) == {"src/app.js"}
    assert second.next_cursor is not None
    assert "record_set_ref" not in second.next_cursor


def test_code_structure_scoped_external_record_set_ref_returns_replacement_slice(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("tree_sitter_language_pack")

    from okg.substrate.library.sources.code_graph import CodeStructureSource

    monkeypatch.setenv("OKG_FILE_WALK_USE_MTIME", "0")
    repo = tmp_path / "repo"
    _write(repo / "src" / "app.js", "function app() { return 1; }\n")
    _write(repo / "src" / "util.js", "function util() { return 2; }\n")
    source = CodeStructureSource(
        repo="demo",
        repo_root=repo,
        include_exts=[".js"],
    )
    first = source.run("run-1", mode="scope_complete")
    assert first.record_set is not None
    cursor = dict(first.next_cursor or {})
    cursor["record_set_ref"] = {
        "storage": "okg.source_record_set_blobs",
        "sha256": "abc",
    }
    cursor["record_set_summary"] = {"record_count": 2}

    _write(
        repo / "src" / "app.js",
        "// inserted header\nfunction app() { return 1; }\n",
    )
    second = source.run(
        "run-2",
        mode="scope_complete",
        cursor=cursor,
        sync_scope={"scope_keys": ["src/app.js"]},
    )
    facts = list(second.facts)

    assert second.completed_scope is True
    assert second.record_set is not None
    assert set(second.record_set.records) == {"src/app.js"}
    assert set(second.record_set_replacement_keys or ()) == {"src/app.js"}
    assert {
        fact.source_record_id.get("path")
        for fact in facts
        if isinstance(fact, (NodeFact, EdgeFact))
    } == {"src/app.js"}


def test_code_structure_scope_complete_replaces_changed_file_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("tree_sitter_language_pack")

    from okg.substrate.library.sources.code_graph import CodeStructureSource

    monkeypatch.setenv("OKG_FILE_WALK_USE_MTIME", "0")
    repo = tmp_path / "repo"
    _write(repo / "src" / "app.js", "function app() { return 1; }\n")
    _write(repo / "src" / "util.js", "function util() { return 2; }\n")
    source = CodeStructureSource(
        repo="demo",
        repo_root=repo,
        include_exts=[".js"],
    )

    first = source.run("run-1", mode="scope_complete")
    assert first.record_set is not None
    cursor = dict(first.next_cursor or {})
    cursor["record_set"] = first.record_set.to_cursor()
    original = source._facts_for_file
    calls: list[str] = []

    def _spy_facts_for_file(
        path: Path,
        rel_path: str,
        text: str,
        text_hash: str,
    ) -> list[NodeFact | EdgeFact]:
        calls.append(rel_path)
        return original(path, rel_path, text, text_hash)

    monkeypatch.setattr(source, "_facts_for_file", _spy_facts_for_file)
    _write(
        repo / "src" / "app.js",
        "// inserted header\nfunction app() { return 1; }\n",
    )
    second = source.run("run-2", mode="scope_complete", cursor=cursor)
    facts = list(second.facts)

    assert calls == ["src/app.js"]
    assert {
        fact.source_record_id.get("path")
        for fact in facts
        if isinstance(fact, (NodeFact, EdgeFact))
    } == {"src/app.js"}
    assert second.completed_scope is True
    assert second.record_set is not None
    assert set(second.record_set.records) == {"src/app.js", "src/util.js"}
    app_nodes = second.record_set.records["src/app.js"].nodes_by_id()
    util_nodes = second.record_set.records["src/util.js"].nodes_by_id()
    assert "symbol:demo:src/app.js:app:1" not in app_nodes
    assert "symbol:demo:src/app.js:app:2" in app_nodes
    assert "symbol:demo:src/util.js:util:1" in util_nodes


def test_code_structure_scope_complete_removes_record_for_unparseable_changed_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources.code_graph import CodeStructureSource

    monkeypatch.setenv("OKG_FILE_WALK_USE_MTIME", "0")
    repo = tmp_path / "repo"
    _write(repo / "src" / "legacy.py", "def old():\n    return 1\n")
    _write(repo / "src" / "util.py", "def util():\n    return 2\n")
    source = CodeStructureSource(repo="demo", repo_root=repo)

    first = source.run("run-1", mode="scope_complete")
    assert first.record_set is not None
    assert set(first.record_set.records) == {"src/legacy.py", "src/util.py"}
    cursor = dict(first.next_cursor or {})
    cursor["record_set"] = first.record_set.to_cursor()

    _write(repo / "src" / "legacy.py", 'print "python-2 syntax"\n')
    second = source.run("run-2", mode="scope_complete", cursor=cursor)

    assert list(second.facts) == []
    assert second.completed_scope is True
    assert second.record_set is not None
    assert set(second.record_set.records) == {"src/util.py"}
    assert set((second.next_cursor or {})["files"]) == {
        "src/legacy.py",
        "src/util.py",
    }


def test_code_structure_scope_complete_carry_forward_removes_deleted_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("tree_sitter_language_pack")

    from okg.substrate.library.sources.code_graph import CodeStructureSource

    monkeypatch.setenv("OKG_FILE_WALK_USE_MTIME", "0")
    repo = tmp_path / "repo"
    _write(repo / "src" / "app.js", "function app() { return 1; }\n")
    _write(repo / "src" / "util.js", "function util() { return 2; }\n")
    source = CodeStructureSource(
        repo="demo",
        repo_root=repo,
        include_exts=[".js"],
    )

    first = source.run("run-1", mode="scope_complete")
    assert first.record_set is not None
    cursor = dict(first.next_cursor or {})
    cursor["record_set"] = first.record_set.to_cursor()

    (repo / "src" / "app.js").unlink()
    second = source.run("run-2", mode="scope_complete", cursor=cursor)

    assert list(second.facts) == []
    assert second.completed_scope is True
    assert second.record_set is not None
    assert set(second.record_set.records) == {"src/util.js"}
    assert set((second.next_cursor or {})["files"]) == {"src/util.js"}


@pytest.mark.postgres
def test_code_structure_scoped_scope_complete_retracts_changed_file_only(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("tree_sitter_language_pack")

    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.code_graph import CodeStructureSource

    monkeypatch.setenv("OKG_FILE_WALK_USE_MTIME", "0")
    repo = tmp_path / "repo"
    _write(repo / "src" / "app.js", "function app() { return 1; }\n")
    _write(repo / "src" / "util.js", "function util() { return 2; }\n")
    source = CodeStructureSource(
        repo="demo",
        repo_root=repo,
        include_exts=[".js"],
    )
    runner = SourceRunner(
        clean_runtime,
        source_name="fixture.code-structure",
    )

    first = runner.run(source, mode="scope_complete")
    assert first.retractions_node == 0

    _write(
        repo / "src" / "app.js",
        "// inserted header\nfunction app() { return 1; }\n",
    )
    second = runner.run(
        source,
        mode="scope_complete",
        sync_scope={"scope_keys": ["src/app.js"]},
    )

    assert second.retractions_node > 0
    assert _graph_node_exists(
        clean_runtime,
        "symbol:demo:src/app.js:app:1",
    ) is False
    assert _graph_node_exists(
        clean_runtime,
        "symbol:demo:src/app.js:app:2",
    ) is True
    assert _graph_node_exists(
        clean_runtime,
        "symbol:demo:src/util.js:util:1",
    ) is True


def test_package_manifest_source_emits_dependencies(tmp_path: Path) -> None:
    from okg.substrate.library.sources.code_graph import PackageManifestSource

    _write(
        tmp_path / "pyproject.toml",
        """
[project]
name = "demo"
dependencies = ["pydantic>=2", "mcp"]
""".strip(),
    )

    source = PackageManifestSource(repo="demo", repo_root=tmp_path)
    run = source.run("run-1", mode="scope_complete")
    facts = list(run.facts)

    assert any(isinstance(f, NodeFact) and f.subtype == "package_manifest" for f in facts)
    deps = [
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "dependency_package"
    ]
    assert {d.attrs["name"] for d in deps} == {"pydantic", "mcp"}
    assert any(isinstance(f, EdgeFact) and f.edge_type == "contains" for f in facts)
    assert run.record_set is not None
    emission = run.record_set.records["pyproject.toml"]
    assert {
        node.node_id for node in emission.descendants
    } == {"dep:python:pydantic", "dep:python:mcp"}


def test_package_manifest_source_accepts_stub_glob_config(tmp_path: Path) -> None:
    from okg.substrate.library.sources.code_graph import PackageManifestSource

    _write(
        tmp_path / "pyproject.toml",
        """
[project]
name = "demo"
dependencies = ["pydantic"]
""".strip(),
    )

    source = PackageManifestSource(
        repo="demo",
        repo_root=tmp_path,
        source_file_stub_globs=[".tmp/**"],
    )
    facts = list(source.run("run-1", mode="scope_complete").facts)

    assert any(
        isinstance(fact, NodeFact)
        and fact.subtype == "dependency_package"
        and fact.attrs["name"] == "pydantic"
        for fact in facts
    )


@pytest.mark.postgres
def test_package_manifest_scope_complete_retracts_removed_dependencies(
    clean_runtime: str,
    tmp_path: Path,
) -> None:

    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.code_graph import PackageManifestSource

    _write(
        tmp_path / "pyproject.toml",
        """
[project]
name = "demo"
dependencies = ["pydantic>=2", "mcp"]
""".strip(),
    )
    source = PackageManifestSource(repo="demo", repo_root=tmp_path)
    runner = SourceRunner(
        clean_runtime,
        source_name="fixture.package-manifests",
    )

    first = runner.run(source, mode="scope_complete")
    assert first.retractions_node == 0

    _write(
        tmp_path / "pyproject.toml",
        """
[project]
name = "demo"
dependencies = ["pydantic>=2"]
""".strip(),
    )
    second = runner.run(source, mode="scope_complete")

    assert second.retractions_node == 1
    assert _graph_node_exists(clean_runtime, "dep:python:pydantic") is True
    assert _graph_node_exists(clean_runtime, "dep:python:mcp") is False


def test_interface_source_emits_cli_and_mcp_endpoints(tmp_path: Path) -> None:
    from okg.substrate.library.sources.code_graph import InterfaceSource

    _write(
        tmp_path / "cli.py",
        """
def configure(parser):
    parser.add_parser("doctor")
    sub = parser.add_subparsers()
    p_alias = sub.add_parser("alias")
    p_alias_sub = p_alias.add_subparsers()
    p_alias_sub.add_parser("list")

class Server:
    def tool(self, name):
        return lambda fn: fn

server = Server()

@server.tool("describe_graph")
def describe_graph():
    return {}

@server.tool()
def list_neighbors():
    return {}
""".strip(),
    )

    source = InterfaceSource(repo="demo", repo_root=tmp_path)
    facts = list(source.run("run-1").facts)
    legacy_facts = list(
        source.run("run-2", cursor={"run_id": "old"}).facts,
    )
    endpoints = [
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "interface_endpoint"
    ]

    assert {e.attrs["kind"] for e in endpoints} >= {"cli_command", "mcp_tool"}
    assert {
        e.attrs["name"] for e in endpoints
        if e.attrs["kind"] == "cli_command"
    } >= {"doctor", "alias", "alias list"}
    assert {
        e.attrs["name"] for e in endpoints
        if e.attrs["kind"] == "mcp_tool"
    } >= {"describe_graph", "list_neighbors"}
    assert not any(
        isinstance(f, NodeFact)
        and f.subtype == "interface_endpoint"
        and f.op == "R"
        for f in facts
    )
    assert any(
        isinstance(f, NodeFact)
        and f.subtype == "interface_endpoint"
        and f.op == "R"
        and f.attrs["name"] == "tool"
        for f in legacy_facts
    )
    assert any(isinstance(f, EdgeFact) and f.edge_type == "routes_to" for f in facts)


def test_interface_scope_complete_carries_prior_record_set_on_no_change(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources.code_graph import InterfaceSource

    monkeypatch.setenv("OKG_FILE_WALK_USE_MTIME", "0")
    _write(
        tmp_path / "app.py",
        """
class Server:
    def tool(self, name):
        return lambda fn: fn

server = Server()

@server.tool("describe_graph")
def describe_graph():
    return {}
""".strip(),
    )
    _write(
        tmp_path / "cli.py",
        """
def configure(parser):
    parser.add_parser("doctor")
""".strip(),
    )
    source = InterfaceSource(repo="demo", repo_root=tmp_path)

    first = source.run("run-1", mode="scope_complete")
    assert first.record_set is not None
    cursor = dict(first.next_cursor or {})
    cursor["record_set"] = first.record_set.to_cursor()

    second = source.run("run-2", mode="scope_complete", cursor=cursor)

    assert list(second.facts) == []
    assert second.completed_scope is True
    assert second.record_set is not None
    assert set(second.record_set.records) == {"app.py", "cli.py"}


def test_interface_scoped_scope_complete_replaces_changed_file_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources.code_graph import InterfaceSource

    monkeypatch.setenv("OKG_FILE_WALK_USE_MTIME", "0")
    _write(
        tmp_path / "app.py",
        """
class Server:
    def tool(self, name):
        return lambda fn: fn

server = Server()

@server.tool("describe_graph")
def describe_graph():
    return {}
""".strip(),
    )
    _write(
        tmp_path / "cli.py",
        """
def configure(parser):
    parser.add_parser("doctor")
""".strip(),
    )
    source = InterfaceSource(repo="demo", repo_root=tmp_path)

    first = source.run("run-1", mode="scope_complete")
    assert first.record_set is not None
    cursor = dict(first.next_cursor or {})
    cursor["record_set"] = first.record_set.to_cursor()
    _write(
        tmp_path / "app.py",
        """
class Server:
    def tool(self, name):
        return lambda fn: fn

server = Server()

@server.tool("inspect_graph")
def inspect_graph():
    return {}
""".strip(),
    )
    second = source.run(
        "run-2",
        mode="scope_complete",
        cursor=cursor,
        sync_scope={"scope_keys": ["app.py"]},
    )
    facts = list(second.facts)

    assert {
        fact.source_record_id.get("path")
        for fact in facts
        if isinstance(fact, (NodeFact, EdgeFact))
    } == {"app.py"}
    assert second.completed_scope is True
    assert second.record_set is not None
    assert set(second.record_set.records) == {"app.py", "cli.py"}
    endpoint_names = {
        fact.attrs["name"]
        for fact in facts
        if isinstance(fact, NodeFact)
        and fact.subtype == "interface_endpoint"
    }
    assert "inspect_graph" in endpoint_names
    assert (
        second.record_set.records["cli.py"].nodes_by_id()
        == first.record_set.records["cli.py"].nodes_by_id()
    )


def test_code_data_flow_source_emits_config_and_assets(tmp_path: Path) -> None:
    pytest.importorskip("tree_sitter_language_pack")

    from okg.substrate.library.sources.code_graph import CodeDataFlowSource

    _write(
        tmp_path / "src" / "storage.py",
        """
import os
import requests

config = {"batch_size": 500}

def load(settings, queue, scheduler):
    dsn = os.getenv("OKG_DSN")
    token = os.environ["API_TOKEN"]
    batch_size = config["batch_size"]
    region = settings.get("region")
    sql = "select * from okg.nodes_live join okg.edges_live on true"
    write_sql = "insert into audit.events (id) values (1)"
    path = "deployments/okg-workspace/source_registry.yaml"
    with open("data/out.json", "w") as fh:
        fh.write(str(batch_size))
    requests.get("https://api.example.com/v1/users")
    queue.publish("ingest-events")
    scheduler.schedule(job_kind="nightly-index")
    return dsn, token, region, sql, write_sql, path
""".strip(),
    )
    _write(
        tmp_path / "src" / "client.ts",
        """
export function refresh(config, queue, scheduler) {
  const token = process.env.API_TOKEN;
  const region = config.get("region");
  const sql = "select * from okg.jobs";
  fetch("https://api.example.com/v1/jobs");
  queue.publish("frontend-events");
  scheduler.schedule("nightly-ui-refresh");
  return {token, region, sql};
}
""".strip(),
    )
    _write(
        tmp_path / "src" / "config.ts",
        """
export const token = process.env.PUBLIC_TOKEN;
export const endpoint = "https://config.example.com/v1";
""".strip(),
    )

    source = CodeDataFlowSource(repo="demo", repo_root=tmp_path)
    facts = list(source.run("run-1").facts)

    config_keys = {
        (f.attrs["source_kind"], f.attrs["key"])
        for f in facts
        if isinstance(f, NodeFact) and f.subtype == "config_key"
    }
    assets = {
        (f.attrs["kind"], f.attrs["name"])
        for f in facts
        if isinstance(f, NodeFact) and f.subtype == "data_asset"
    }
    asset_edges = [
        f for f in facts
        if isinstance(f, EdgeFact)
        and f.dst.startswith("asset:")
    ]
    config_edges = [
        f for f in facts
        if isinstance(f, EdgeFact)
        and f.dst.startswith("config:")
    ]

    assert {
        ("env", "OKG_DSN"),
        ("env", "API_TOKEN"),
        ("env", "PUBLIC_TOKEN"),
        ("settings", "batch_size"),
        ("settings", "region"),
    } <= config_keys
    assert {
        ("sql_relation", "okg.nodes_live"),
        ("sql_relation", "okg.edges_live"),
        ("sql_relation", "audit.events"),
        ("file_path", "deployments/okg-workspace/source_registry.yaml"),
        ("file_path", "data/out.json"),
        ("external_api_host", "api.example.com"),
        ("external_api_host", "config.example.com"),
        ("queue_kind", "ingest-events"),
        ("queue_kind", "frontend-events"),
        ("job_kind", "nightly-index"),
        ("job_kind", "nightly-ui-refresh"),
    } <= assets
    assert ("sql_relation", "okg.jobs") in assets
    assert any(e.edge_type == "reads" and e.attrs["access_kind"] == "read" for e in asset_edges)
    assert any(e.edge_type == "writes" and e.attrs["access_kind"] == "write" for e in asset_edges)
    assert any(e.edge_type == "writes" and e.attrs["access_kind"] == "publish" for e in asset_edges)
    assert any(
        e.edge_type == "writes"
        and e.attrs["source"] == "src/client.ts"
        and e.attrs["access_kind"] == "publish"
        for e in asset_edges
    )
    assert any(
        e.attrs["source"] == "src/config.ts"
        and e.src == "symbol:demo:src/config.ts:src.config:1"
        for e in [*asset_edges, *config_edges]
    )
    assert any(e.edge_type == "references" for e in config_edges)
    data_flow_facts = [
        f for f in facts
        if (
            isinstance(f, NodeFact)
            and f.subtype in {"config_key", "data_asset"}
        )
        or (
            isinstance(f, EdgeFact)
            and (
                f.dst.startswith("config:")
                or f.dst.startswith("asset:")
            )
        )
    ]
    assert data_flow_facts
    assert all("path" in f.source_record_id for f in data_flow_facts)


def test_code_data_flow_non_python_uses_lightweight_text_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources import code_graph_dataflow as dataflow

    def fail_parser(*args, **kwargs):
        raise AssertionError("data-flow source should not invoke symbol parser")

    monkeypatch.setattr(
        dataflow,
        "_ensure_tree_sitter_registered",
        fail_parser,
        raising=False,
    )
    monkeypatch.setattr(
        dataflow,
        "code_extractor_for",
        fail_parser,
        raising=False,
    )
    _write(
        tmp_path / "src" / "client.ts",
        """
export function refresh(queue) {
  const token = process.env.API_TOKEN;
  queue.publish("frontend-events");
  return token;
}
""".strip(),
    )

    source = dataflow.CodeDataFlowSource(
        repo="demo",
        repo_root=tmp_path,
        include_exts=[".ts"],
    )
    facts = list(source.run("run-1").facts)

    config_keys = [
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "config_key"
    ]
    asset_edges = [
        f for f in facts
        if isinstance(f, EdgeFact) and f.dst.startswith("asset:")
    ]
    config_edges = [
        f for f in facts
        if isinstance(f, EdgeFact) and f.dst.startswith("config:")
    ]

    assert any(f.attrs["key"] == "API_TOKEN" for f in config_keys)
    assert any(
        f.edge_type == "writes"
        and f.attrs["access_kind"] == "publish"
        for f in asset_edges
    )
    assert any(
        f.src == "symbol:demo:src/client.ts:src.client:1"
        for f in [*asset_edges, *config_edges]
    )


def test_code_data_flow_skips_unchanged_scoped_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources import code_graph_dataflow as dataflow

    _write(
        tmp_path / "src" / "worker.py",
        """
import os

def run():
    token = os.getenv("API_TOKEN")
    return token
""".strip(),
    )

    source = dataflow.CodeDataFlowSource(repo="demo", repo_root=tmp_path)
    first = source.run(
        "run-1",
        mode="scope_incomplete",
        sync_scope={"scope_keys": ["src/worker.py"]},
    )
    assert list(first.facts)
    assert source.last_run_stats["files_scanned"] == 1

    def fail_extract(*args, **kwargs):
        raise AssertionError("unchanged data-flow source should not parse")

    monkeypatch.setattr(dataflow, "_data_flow_facts", fail_extract)
    monkeypatch.setattr(dataflow, "_data_flow_facts_from_text", fail_extract)

    second = source.run(
        "run-2",
        mode="scope_incomplete",
        cursor=first.next_cursor,
        sync_scope={"scope_keys": ["src/worker.py"]},
    )

    assert list(second.facts) == []
    assert second.completed_scope is False
    assert second.next_cursor is not None
    assert set(second.next_cursor["files"]) == {"src/worker.py"}
    assert source.last_run_stats == {
        "scope_status": "scoped_unchanged_noop",
        "scope_keys": 1,
        "scope_keys_outside_source_paths": 0,
        "files_seen": 1,
        "files_scanned": 0,
        "files_skipped_unchanged": 1,
        "changed_scope_keys": 0,
        "emitted_nodes": 0,
        "emitted_edges": 0,
    }


def test_code_data_flow_scope_complete_carries_record_set_on_scoped_noop(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources import code_graph_dataflow as dataflow

    _write(
        tmp_path / "src" / "worker.py",
        """
import os

def run():
    token = os.getenv("API_TOKEN")
    return token
""".strip(),
    )

    source = dataflow.CodeDataFlowSource(repo="demo", repo_root=tmp_path)
    first = source.run(
        "run-1",
        mode="scope_complete",
        sync_scope={"scope_keys": ["src/worker.py"]},
    )
    assert list(first.facts)
    assert first.completed_scope is True
    assert first.record_set is not None
    assert set(first.record_set.records) == {"src/worker.py"}

    def fail_extract(*args, **kwargs):
        raise AssertionError("unchanged data-flow source should not parse")

    monkeypatch.setattr(dataflow, "_data_flow_facts", fail_extract)
    monkeypatch.setattr(dataflow, "_data_flow_facts_from_text", fail_extract)

    cursor = dict(first.next_cursor or {})
    cursor["record_set"] = first.record_set.to_cursor()
    second = source.run(
        "run-2",
        mode="scope_complete",
        cursor=cursor,
        sync_scope={"scope_keys": ["src/worker.py"]},
    )

    assert list(second.facts) == []
    assert second.completed_scope is True
    assert second.record_set is not None
    assert set(second.record_set.records) == {"src/worker.py"}
    assert source.last_run_stats["scope_status"] == "scoped_unchanged_noop"
    assert source.last_run_stats["files_scanned"] == 0
    assert source.last_run_stats["emitted_nodes"] == 0
    assert source.last_run_stats["emitted_edges"] == 0


def test_code_data_flow_skips_unchanged_unscoped_cursor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources import code_graph_dataflow as dataflow

    _write(
        tmp_path / "src" / "worker.py",
        """
import os

def run():
    token = os.getenv("API_TOKEN")
    return token
""".strip(),
    )
    _write(
        tmp_path / "src" / "client.ts",
        """
export function run() {
  return process.env.API_TOKEN;
}
""".strip(),
    )

    source = dataflow.CodeDataFlowSource(repo="demo", repo_root=tmp_path)
    first = source.run("run-1", mode="scope_incomplete")
    assert list(first.facts)
    assert source.last_run_stats["files_scanned"] == 2

    def fail_extract(*args, **kwargs):
        raise AssertionError("unchanged data-flow source should not parse")

    monkeypatch.setattr(dataflow, "_data_flow_facts", fail_extract)
    monkeypatch.setattr(dataflow, "_data_flow_facts_from_text", fail_extract)

    second = source.run(
        "run-2",
        mode="scope_incomplete",
        cursor=first.next_cursor,
    )

    assert list(second.facts) == []
    assert set((second.next_cursor or {})["files"]) == {
        "src/client.ts",
        "src/worker.py",
    }
    assert source.last_run_stats["scope_status"] == "full_unchanged_noop"
    assert source.last_run_stats["files_seen"] == 2
    assert source.last_run_stats["files_scanned"] == 0
    assert source.last_run_stats["emitted_nodes"] == 0
    assert source.last_run_stats["emitted_edges"] == 0


def test_code_data_flow_sql_relation_scan_is_bounded() -> None:
    from okg.substrate.library.sources import code_graph_dataflow as dataflow

    assert not hasattr(dataflow, "_sqlglot_relations")

    relations = dataflow._sql_relations(
        "SELECT * FROM okg.nodes_live JOIN okg.edges_live ON true; "
        "DROP TABLE ignored"
    )

    assert ("okg.nodes_live", "read") in relations
    assert ("okg.edges_live", "read") in relations
    assert ("ignored", "write") not in relations


def test_semgrep_data_flow_source_imports_observations_and_steps(tmp_path: Path) -> None:
    from okg.substrate.library.sources.code_graph import SemgrepDataFlowSource

    artifact = _write(
        tmp_path / "reports" / "semgrep.json",
        json.dumps({
            "version": "1.99.0",
            "results": [
                {
                    "check_id": "okg.sql.write",
                    "path": "src/db.py",
                    "start": {"line": 10, "col": 1},
                    "end": {"line": 14, "col": 5},
                    "extra": {
                        "message": "SQL write reaches sink",
                        "metadata": {
                            "confidence": "high",
                            "rule_version": "2026-05-02",
                            "access_kind": "write",
                        },
                        "dataflow_trace": {
                            "taint_source": [
                                {
                                    "path": "src/db.py",
                                    "start": {"line": 10, "col": 1},
                                    "end": {"line": 10, "col": 8},
                                    "message": "source",
                                },
                            ],
                            "taint_sink": [
                                {
                                    "path": "src/db.py",
                                    "start": {"line": 14, "col": 1},
                                    "end": {"line": 14, "col": 5},
                                    "message": "sink",
                                },
                            ],
                        },
                    },
                },
            ],
        }),
    )

    source = SemgrepDataFlowSource(
        repo="demo",
        repo_root=tmp_path,
        artifact_paths=[artifact],
    )
    run = source.run("run-1", mode="scope_complete")
    facts = list(run.facts)

    observations = [
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "data_flow_observation"
    ]
    steps = [
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "data_flow_step"
    ]
    edges = [f for f in facts if isinstance(f, EdgeFact)]

    assert len(observations) == 1
    assert observations[0].attrs["tool"] == "semgrep"
    assert observations[0].attrs["tool_version"] == "1.99.0"
    assert observations[0].attrs["rule_id"] == "okg.sql.write"
    assert observations[0].attrs["rule_version"] == "2026-05-02"
    assert observations[0].attrs["confidence"] == "high"
    assert len(steps) == 2
    assert {step.attrs["kind"] for step in steps} == {"taint_source", "taint_sink"}
    assert all(edge.edge_type == "contains" for edge in edges)
    assert all(not edge.dst.startswith(("symbol:", "asset:", "config:")) for edge in edges)
    assert run.completed_scope is True
    assert run.record_set is not None
    assert observations[0].node_id in run.record_set.records


def test_codeql_data_flow_source_imports_sarif_paths(tmp_path: Path) -> None:
    from okg.substrate.library.sources.code_graph import CodeQLDataFlowSource

    artifact = _write(
        tmp_path / "reports" / "codeql.sarif",
        json.dumps({
            "version": "2.1.0",
            "$schema": "https://json.schemastore.org/sarif-2.1.0.json",
            "runs": [
                {
                    "tool": {
                        "driver": {
                            "name": "CodeQL",
                            "semanticVersion": "2.20.0",
                            "rules": [
                                {
                                    "id": "py/sql-injection",
                                    "version": "1.0",
                                    "shortDescription": {"text": "SQL injection"},
                                },
                            ],
                        },
                    },
                    "results": [
                        {
                            "ruleId": "py/sql-injection",
                            "level": "warning",
                            "message": {"text": "query built from user input"},
                            "locations": [
                                {
                                    "physicalLocation": {
                                        "artifactLocation": {"uri": "src/db.py"},
                                        "region": {
                                            "startLine": 20,
                                            "endLine": 21,
                                        },
                                    },
                                },
                            ],
                            "codeFlows": [
                                {
                                    "threadFlows": [
                                        {
                                            "locations": [
                                                {
                                                    "location": {
                                                        "physicalLocation": {
                                                            "artifactLocation": {"uri": "src/db.py"},
                                                            "region": {"startLine": 12},
                                                        },
                                                        "message": {"text": "source"},
                                                    },
                                                },
                                                {
                                                    "location": {
                                                        "physicalLocation": {
                                                            "artifactLocation": {"uri": "src/db.py"},
                                                            "region": {"startLine": 20},
                                                        },
                                                        "message": {"text": "sink"},
                                                    },
                                                },
                                            ],
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                },
            ],
        }),
    )

    source = CodeQLDataFlowSource(
        repo="demo",
        repo_root=tmp_path,
        artifact_paths=[artifact],
    )
    run = source.run("run-1", mode="scope_complete")
    facts = list(run.facts)

    observations = [
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "data_flow_observation"
    ]
    steps = [
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "data_flow_step"
    ]
    edges = [f for f in facts if isinstance(f, EdgeFact)]

    assert len(observations) == 1
    assert observations[0].attrs["tool"] == "codeql"
    assert observations[0].attrs["tool_version"] == "2.20.0"
    assert observations[0].attrs["rule_id"] == "py/sql-injection"
    assert observations[0].attrs["rule_version"] == "1.0"
    assert observations[0].attrs["path"] == "src/db.py"
    assert len(steps) == 2
    assert all(edge.edge_type == "contains" for edge in edges)
    assert all(not edge.dst.startswith(("symbol:", "asset:", "config:")) for edge in edges)
    assert run.completed_scope is True
    assert run.record_set is not None
    assert observations[0].node_id in run.record_set.records


@pytest.mark.postgres
def test_code_data_flow_source_publishes_assets(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    pytest.importorskip("tree_sitter_language_pack")
    import psycopg
    import yaml

    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.code_graph import (
        CodeDataFlowSource,
        CodeStructureSource,
    )
    from okg.substrate.library.sources.git_graph_files import GitFilesSource
    from okg.substrate.publisher import Publisher

    repo = tmp_path / "repo"
    _write(
        repo / "src" / "storage.py",
        """
import os

config = {"batch_size": 500}

def load(settings, queue):
    dsn = os.getenv("OKG_DSN")
    batch_size = config["batch_size"]
    region = settings.get("region")
    sql = "select * from okg.nodes_live join okg.edges_live on true"
    write_sql = "insert into audit.events (id) values (1)"
    queue.publish("ingest-events")
    with open("data/out.json", "w") as fh:
        fh.write(str(batch_size))
    return dsn, region, sql, write_sql
""".strip(),
    )
    _write(
        repo / "src" / "client.ts",
        """
export function refresh(config, queue, scheduler) {
  const token = process.env.API_TOKEN;
  const region = config.get("region");
  const sql = "select * from okg.jobs";
  fetch("https://api.example.com/v1/jobs");
  queue.publish("frontend-events");
  scheduler.schedule("nightly-ui-refresh");
  return {token, region, sql};
}
""".strip(),
    )

    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-code-data-flow",
        "description": "code data-flow publish fixture",
        "modules": [
            "git_graph",
            "openspec",
            "agent_sessions",
            "compute_env",
            "ci",
            "code_graph",
        ],
    }))
    (dep / "schemas").mkdir()

    composed = compose_catalog(dep)
    with psycopg.connect(clean_runtime) as conn:
        apply_composed_catalog(conn, composed)

    SourceRunner(clean_runtime, source_name="fixture.git-files").run(
        GitFilesSource(repo="demo", repo_root=repo),
        mode="scope_complete",
    )
    SourceRunner(clean_runtime, source_name="fixture.code-structure").run(
        CodeStructureSource(repo="demo", repo_root=repo, include_exts=[".py", ".ts"]),
        mode="scope_complete",
    )
    SourceRunner(clean_runtime, source_name="fixture.code-data-flow").run(
        CodeDataFlowSource(repo="demo", repo_root=repo),
        mode="scope_complete",
    )

    out = Publisher(clean_runtime, "test-code-data-flow", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-code-data-flow")).cycle()
    assert out["status"] == "published", out

    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        graph = store.checkout_generation(out["generation_id"])
        counts = {
            str(row["subtype"]): int(row["count"])
            for row in graph.query(
                """
                SELECT subtype, count(*) AS count
                  FROM okg.graph_nodes
                 WHERE subtype IN ('data_asset', 'config_key')
                 GROUP BY 1
                """
            )
        }
        edge_counts = {
            (str(row["edge_type"]), str(row["dst_subtype"])): int(row["count"])
            for row in graph.query(
                """
                SELECT e.edge_type, d.subtype AS dst_subtype, count(*) AS count
                  FROM okg.graph_edges e
                  JOIN okg.graph_nodes s ON s.node_id = e.src
                  JOIN okg.graph_nodes d ON d.node_id = e.dst
                 WHERE s.subtype = 'code_symbol'
                   AND d.subtype IN ('data_asset', 'config_key')
                 GROUP BY 1, 2
                """
            )
        }
        ts_edge_counts = {
            str(row["edge_type"]): int(row["count"])
            for row in graph.query(
                """
                SELECT e.edge_type, count(*) AS count
                  FROM okg.graph_edges e
                  JOIN okg.graph_nodes s ON s.node_id = e.src
                  JOIN okg.graph_nodes d ON d.node_id = e.dst
                 WHERE s.subtype = 'code_symbol'
                   AND s.attrs->>'language' = 'typescript'
                   AND d.subtype IN ('data_asset', 'config_key')
                 GROUP BY 1
                """
            )
        }
    finally:
        store.close()

    assert counts["data_asset"] >= 5
    assert counts["config_key"] >= 3
    assert edge_counts[("reads", "data_asset")] > 0
    assert edge_counts[("writes", "data_asset")] > 0
    assert edge_counts[("references", "config_key")] > 0
    assert ts_edge_counts["writes"] > 0
    assert ts_edge_counts["references"] > 0


@pytest.mark.postgres
def test_third_party_data_flow_sources_publish_observations(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    import psycopg
    import yaml

    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.code_graph import (
        CodeQLDataFlowSource,
        SemgrepDataFlowSource,
    )
    from okg.substrate.publisher import Publisher

    semgrep_artifact = _write(
        tmp_path / "reports" / "semgrep.json",
        json.dumps({
            "version": "1.99.0",
            "results": [
                {
                    "check_id": "okg.sql.write",
                    "path": "src/db.py",
                    "start": {"line": 10},
                    "end": {"line": 14},
                    "extra": {
                        "message": "SQL write reaches sink",
                        "metadata": {"confidence": "high"},
                        "dataflow_trace": {
                            "taint_source": [{"path": "src/db.py", "start": {"line": 10}}],
                            "taint_sink": [{"path": "src/db.py", "start": {"line": 14}}],
                        },
                    },
                },
            ],
        }),
    )
    codeql_artifact = _write(
        tmp_path / "reports" / "codeql.sarif",
        json.dumps({
            "version": "2.1.0",
            "runs": [
                {
                    "tool": {"driver": {"name": "CodeQL", "semanticVersion": "2.20.0"}},
                    "results": [
                        {
                            "ruleId": "py/sql-injection",
                            "message": {"text": "query built from user input"},
                            "locations": [
                                {
                                    "physicalLocation": {
                                        "artifactLocation": {"uri": "src/db.py"},
                                        "region": {"startLine": 20, "endLine": 21},
                                    },
                                },
                            ],
                            "codeFlows": [
                                {
                                    "threadFlows": [
                                        {
                                            "locations": [
                                                {
                                                    "location": {
                                                        "physicalLocation": {
                                                            "artifactLocation": {"uri": "src/db.py"},
                                                            "region": {"startLine": 20},
                                                        },
                                                    },
                                                },
                                            ],
                                        },
                                    ],
                                },
                            ],
                        },
                    ],
                },
            ],
        }),
    )

    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test-third-party-data-flow",
        "description": "third-party data-flow publish fixture",
        "modules": [
            "git_graph",
            "openspec",
            "agent_sessions",
            "compute_env",
            "ci",
            "code_graph",
        ],
    }))
    (dep / "schemas").mkdir()

    composed = compose_catalog(dep)
    with psycopg.connect(clean_runtime) as conn:
        apply_composed_catalog(conn, composed)

    SourceRunner(clean_runtime, source_name="fixture.semgrep-data-flow").run(
        SemgrepDataFlowSource(
            repo="demo",
            repo_root=tmp_path,
            artifact_paths=[semgrep_artifact],
        ),
        mode="scope_complete",
    )
    SourceRunner(clean_runtime, source_name="fixture.codeql-data-flow").run(
        CodeQLDataFlowSource(
            repo="demo",
            repo_root=tmp_path,
            artifact_paths=[codeql_artifact],
        ),
        mode="scope_complete",
    )

    out = Publisher(clean_runtime, "test-third-party-data-flow", catalog_identity=bind_test_catalog_writer(clean_runtime, "test-third-party-data-flow")).cycle()
    assert out["status"] == "published", out

    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        graph = store.checkout_generation(out["generation_id"])
        counts = {
            str(row["subtype"]): int(row["count"])
            for row in graph.query(
                """
                SELECT subtype, count(*) AS count
                  FROM okg.graph_nodes
                 WHERE subtype IN ('data_flow_observation', 'data_flow_step')
                 GROUP BY 1
                """
            )
        }
        resolved_edges = int(graph.query(
            """
            SELECT count(*) AS count
              FROM okg.graph_edges e
              JOIN okg.graph_nodes d ON d.node_id = e.dst
             WHERE d.subtype IN ('code_symbol', 'data_asset', 'config_key')
            """
        )[0]["count"])
    finally:
        store.close()

    assert counts["data_flow_observation"] == 2
    assert counts["data_flow_step"] >= 3
    assert resolved_edges == 0


def test_codex_sessions_source_emits_session_tool_and_file_edges(tmp_path: Path) -> None:
    from okg.substrate.library.sources.codex_sessions import CodexSessionsSource

    repo = tmp_path / "repo"
    _write(repo / "src" / "app.py", "def app():\n    return 1\n")
    _write(repo / ".gitignore", ".tmp/\n.okg/\n")
    _write(repo / ".tmp" / "scratch.xml", "<scratch />\n")
    _write(repo / ".okg" / "local.env", "OKG_DSN=postgres://redacted\n")
    from okg.substrate.library.sources._git_helpers import run_git

    run_git(repo, ["init"])
    run_git(repo, ["add", ".gitignore", "src/app.py"])
    sessions_root = tmp_path / "codex" / "sessions" / "2026" / "05" / "02"
    transcript = sessions_root / "rollout.jsonl"
    transcript.parent.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "timestamp": "2026-05-02T12:00:00Z",
            "type": "session_meta",
            "payload": {
                "id": "abc-123",
                "timestamp": "2026-05-02T12:00:00Z",
                "cwd": str(repo),
                "originator": "Codex Desktop",
                "cli_version": "0.126.0",
                "model_provider": "openai",
                "git": {"branch": "main", "commit_hash": "deadbeef"},
            },
        },
        {
            "timestamp": "2026-05-02T12:00:01Z",
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "name": "exec_command",
                "arguments": json.dumps({
                    "cmd": (
                        "cat src/app.py && "
                        "cat openspec/changes/deleted/proposal.md && "
                        "cat .tmp/scratch.xml && "
                        "cat .okg/local.env"
                    ),
                    "workdir": str(repo),
                }),
            },
        },
    ]
    transcript.write_text(
        # Newline-terminated, like every real transcript: an unterminated
        # final line means a writer mid-append, which the reader now defers.
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )

    source = CodexSessionsSource(
        repo="demo",
        codex_sessions_root=sessions_root.parents[3],
        archived_sessions_root=None,
        cwd_filter=[repo],
        repo_root=repo,
        repo_file_exclude_globs=[".tmp/**", ".okg/**"],
        capture_tool_use_events=["exec_command"],
        since="all",
    )
    first = source.run("run-1", mode="cursor")
    facts = list(first.facts)

    assert any(isinstance(f, NodeFact) and f.node_id == "session:codex:abc-123" for f in facts)
    tool_events = [
        f for f in facts
        if isinstance(f, NodeFact) and f.subtype == "tool_use_event"
    ]
    assert tool_events
    assert tool_events[0].attrs["file_path"] == "src/app.py"
    assert any(
        isinstance(f, EdgeFact)
        and f.edge_type == "affects"
        and f.dst == "file:demo:src/app.py"
        for f in facts
    )
    assert not any(
        isinstance(f, EdgeFact)
        and f.edge_type == "affects"
        and f.dst == "file:demo:openspec/changes/deleted/proposal.md"
        for f in facts
    )
    assert not any(
        isinstance(f, EdgeFact)
        and f.edge_type == "affects"
        and f.dst == "file:demo:.tmp/scratch.xml"
        for f in facts
    )
    assert not any(
        isinstance(f, EdgeFact)
        and f.edge_type == "affects"
        and f.dst == "file:demo:.okg/local.env"
        for f in facts
    )

    second = source.run("run-2", mode="cursor", cursor=first.next_cursor)
    assert list(second.facts) == []


def test_codex_sessions_source_scopes_to_requested_transcript(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.codex_sessions import CodexSessionsSource

    repo = tmp_path / "repo"
    _write(repo / "src" / "app.py", "def app():\n    return 1\n")
    root = tmp_path / "codex" / "sessions"
    first = root / "2026" / "05" / "02" / "alpha.jsonl"
    second = root / "2026" / "05" / "02" / "beta.jsonl"
    _write_codex_transcript(first, session_id="alpha", repo=repo)
    _write_codex_transcript(second, session_id="beta", repo=repo)

    source = CodexSessionsSource(
        repo="demo",
        codex_sessions_root=root,
        archived_sessions_root=None,
        cwd_filter=[repo],
        since="all",
    )
    run = source.run(
        "run-scoped",
        mode="scope_incomplete",
        sync_scope={
            "scope_keys": [
                second.relative_to(root).as_posix(),
                "../escape.jsonl",
            ],
        },
    )
    session_ids = {
        f.node_id
        for f in run.facts
        if isinstance(f, NodeFact) and f.subtype == "agent_session"
    }

    assert run.completed_scope is False
    assert session_ids == {"session:codex:beta"}


def test_codex_sessions_cwd_globs_capture_worktree_family(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.codex_sessions import CodexSessionsSource

    repo = tmp_path / "codex" / "worktrees" / "alpha" / "okg"
    _write(repo / "src" / "app.py", "def app():\n    return 1\n")
    other_repo = tmp_path / "other" / "repo"
    _write(other_repo / "src" / "app.py", "def app():\n    return 2\n")
    root = tmp_path / "codex" / "sessions"
    alpha = root / "2026" / "05" / "02" / "alpha.jsonl"
    beta = root / "2026" / "05" / "02" / "beta.jsonl"
    _write_codex_transcript(alpha, session_id="alpha", repo=repo)
    _write_codex_transcript(beta, session_id="beta", repo=other_repo)

    source = CodexSessionsSource(
        repo="demo",
        codex_sessions_root=root,
        archived_sessions_root=None,
        cwd_globs=[str(tmp_path / "codex" / "worktrees" / "*" / "okg")],
        since="all",
    )
    session_ids = {
        f.node_id
        for f in source.run("run-1", mode="cursor").facts
        if isinstance(f, NodeFact) and f.subtype == "agent_session"
    }

    assert session_ids == {"session:codex:alpha"}


def test_codex_sessions_sanitize_nul_in_message_excerpt(tmp_path: Path) -> None:
    from okg.substrate.library.sources.codex_sessions import CodexSessionsSource

    repo = tmp_path / "repo"
    _write(repo / "src" / "app.py", "def app():\n    return 1\n")
    root = tmp_path / "codex" / "sessions"
    transcript = root / "2026" / "05" / "02" / "nul.jsonl"
    rows = [
        {
            "timestamp": "2026-05-02T12:00:00Z",
            "type": "session_meta",
            "payload": {
                "id": "nul",
                "timestamp": "2026-05-02T12:00:00Z",
                "cwd": str(repo),
                "model_provider": "openai",
            },
        },
        {
            "timestamp": "2026-05-02T12:00:01Z",
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "assistant",
                "content": [
                    {"type": "output_text", "text": "hello\u0000world"},
                ],
            },
        },
    ]
    # Trailing newline matters now: an unterminated final line is what a
    # writer mid-append leaves behind, so the reader defers it rather than
    # consuming it (consuming it put the stop offset mid-line, which made a
    # live session re-parse from zero on every append). All 1713 transcripts
    # on this host are newline-terminated, so a fixture without one does not
    # describe anything the writer produces.
    _write(transcript, "\n".join(json.dumps(row) for row in rows) + "\n")

    source = CodexSessionsSource(
        repo="demo",
        codex_sessions_root=root,
        archived_sessions_root=None,
        cwd_filter=[repo],
        since="all",
    )
    message = next(
        fact
        for fact in source.run("run-1", mode="cursor").facts
        if isinstance(fact, NodeFact) and fact.subtype == "agent_message"
    )

    assert message.attrs["content_excerpt"] == "helloworld"
    assert "\x00" not in message.attrs["content_excerpt"]


def test_codex_sessions_scoped_cursor_preserves_untouched_transcripts(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.codex_sessions import CodexSessionsSource

    repo = tmp_path / "repo"
    _write(repo / "src" / "app.py", "def app():\n    return 1\n")
    root = tmp_path / "codex" / "sessions"
    first = root / "2026" / "05" / "02" / "alpha.jsonl"
    second = root / "2026" / "05" / "02" / "beta.jsonl"
    _write_codex_transcript(first, session_id="alpha", repo=repo)
    _write_codex_transcript(second, session_id="beta", repo=repo)

    source = CodexSessionsSource(
        repo="demo",
        codex_sessions_root=root,
        archived_sessions_root=None,
        cwd_filter=[repo],
        since="all",
    )
    baseline = source.run("run-1", mode="cursor")
    list(baseline.facts)
    first_key = str(first.resolve())
    second_key = str(second.resolve())

    _write_codex_transcript(
        second,
        session_id="beta",
        repo=repo,
        extra_command="sed -n '1,2p' src/app.py",
    )
    scoped = source.run(
        "run-2",
        mode="scope_incomplete",
        cursor=baseline.next_cursor,
        sync_scope={"scope_keys": [second.relative_to(root).as_posix()]},
    )
    facts = list(scoped.facts)
    session_ids = {
        f.node_id
        for f in facts
        if isinstance(f, NodeFact) and f.subtype == "agent_session"
    }

    assert session_ids == {"session:codex:beta"}
    assert scoped.next_cursor is not None
    transcripts = scoped.next_cursor["transcripts"]
    assert transcripts[first_key] == baseline.next_cursor["transcripts"][first_key]
    assert transcripts[second_key] != baseline.next_cursor["transcripts"][second_key]


def test_codex_sessions_scope_complete_declares_transcript_record_set(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.codex_sessions import CodexSessionsSource

    repo = tmp_path / "repo"
    _write(repo / "src" / "app.py", "def app():\n    return 1\n")
    root = tmp_path / "codex" / "sessions"
    first = root / "2026" / "05" / "02" / "alpha.jsonl"
    second = root / "2026" / "05" / "02" / "beta.jsonl"
    _write_codex_transcript(first, session_id="alpha", repo=repo)
    _write_codex_transcript(second, session_id="beta", repo=repo)

    run = CodexSessionsSource(
        repo="demo",
        codex_sessions_root=root,
        archived_sessions_root=None,
        cwd_filter=[repo],
        since="all",
    ).run("run-1", mode="scope_complete")
    list(run.facts)

    assert run.completed_scope is True
    assert run.record_set is not None
    assert set(run.record_set.records) == {
        str(first.resolve()),
        str(second.resolve()),
    }
    beta_record = run.record_set.records[str(second.resolve())]
    assert beta_record.primary is not None
    assert beta_record.primary.node_id == "session:codex:beta"


def test_codex_sessions_bounded_scope_complete_uses_append_cursor(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.codex_sessions import CodexSessionsSource

    repo = tmp_path / "repo"
    _write(repo / "src" / "app.py", "def app():\n    return 1\n")
    root = tmp_path / "codex" / "sessions"
    first = root / "2026" / "05" / "02" / "alpha.jsonl"
    second = root / "2026" / "05" / "02" / "beta.jsonl"
    _write_codex_transcript(first, session_id="alpha", repo=repo)
    _write_codex_transcript(second, session_id="beta", repo=repo)
    # (The helper now terminates the last line itself; appending another "\n"
    # here would leave a blank line, which is not valid JSON and trips the
    # fail-closed enumeration check.)

    bounded = CodexSessionsSource(
        repo="demo",
        codex_sessions_root=root,
        archived_sessions_root=None,
        cwd_filter=[repo],
        since="7d",
    )
    baseline = bounded.run("run-1", mode="cursor")
    list(baseline.facts)
    first_key = str(first.resolve())
    assert (
        baseline.next_cursor["transcripts"][first_key]["append_cursor"]["append_safe"]
        is True
    )

    _append_codex_tool_call(first, repo=repo, cmd="sed -n '1,2p' src/app.py")
    changed = bounded.run(
        "run-2",
        mode="scope_complete",
        cursor=baseline.next_cursor,
    )
    facts = list(changed.facts)
    session_ids = {
        f.node_id
        for f in facts
        if isinstance(f, NodeFact) and f.subtype == "agent_session"
    }

    assert changed.completed_scope is True
    assert changed.record_set is None
    assert session_ids == {"session:codex:alpha"}
    assert bounded.last_run_stats["scope_status"] == "bounded_full"
    assert bounded.last_run_stats["files_seen"] == 2
    assert bounded.last_run_stats["files_scanned"] == 1
    assert bounded.last_run_stats["files_skipped_unchanged"] == 1
    assert bounded.last_run_stats["append_delta_files"] == 1
    assert bounded.last_run_stats["full_parse_files"] == 0
    assert bounded.last_run_stats["bytes_delta"] < first.stat().st_size
    assert bounded.last_run_stats["emitted_nodes"] > 0

    authoritative = CodexSessionsSource(
        repo="demo",
        codex_sessions_root=root,
        archived_sessions_root=None,
        cwd_filter=[repo],
        since="all",
    )
    full_baseline = authoritative.run("run-3", mode="scope_complete")
    list(full_baseline.facts)
    assert full_baseline.record_set is not None
    _append_codex_tool_call(first, repo=repo, cmd="sed -n '1,3p' src/app.py")
    full_changed = authoritative.run(
        "run-4",
        mode="scope_complete",
        cursor={
            **full_baseline.next_cursor,
            "record_set": full_baseline.record_set.to_cursor(),
        },
    )
    list(full_changed.facts)

    assert full_changed.record_set is not None
    assert authoritative.last_run_stats["scope_status"] == "full"
    assert authoritative.last_run_stats["files_scanned"] == 1
    # A record-set-authoritative reconcile USED TO forbid resume: the full
    # re-parse was what made the emitted footprint complete. Chunked ingest
    # replaces that mechanism, because under a per-run budget the re-read of an
    # already-ingested transcript gets cut short and the file never finishes.
    # The footprint is now accumulated across runs instead, so resume is safe
    # here -- and the property that used to be implied by the full re-parse is
    # asserted directly below.
    assert authoritative.last_run_stats["append_delta_files"] == 1
    assert authoritative.last_run_stats["full_parse_files"] == 0
    # The invariant that actually matters: resuming must not shrink the
    # record's footprint. Everything the full parse owned is still owned, or
    # missing_from_completed_scope retracts it from the graph.
    baseline_nodes = set(
        full_baseline.record_set.records[str(first)].nodes_by_id()
    )
    resumed_nodes = set(
        full_changed.record_set.records[str(first)].nodes_by_id()
    )
    assert baseline_nodes <= resumed_nodes


def test_codex_sessions_scoped_complete_removes_deleted_transcript_record(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources.codex_sessions import CodexSessionsSource

    repo = tmp_path / "repo"
    _write(repo / "src" / "app.py", "def app():\n    return 1\n")
    root = tmp_path / "codex" / "sessions"
    first = root / "2026" / "05" / "02" / "alpha.jsonl"
    second = root / "2026" / "05" / "02" / "beta.jsonl"
    _write_codex_transcript(first, session_id="alpha", repo=repo)
    _write_codex_transcript(second, session_id="beta", repo=repo)
    source = CodexSessionsSource(
        repo="demo",
        codex_sessions_root=root,
        archived_sessions_root=None,
        cwd_filter=[repo],
        since="all",
    )
    baseline = source.run("run-1", mode="scope_complete")
    list(baseline.facts)
    assert baseline.record_set is not None

    second.unlink()
    scoped = source.run(
        "run-2",
        mode="scope_complete",
        cursor={
            **baseline.next_cursor,
            "record_set": baseline.record_set.to_cursor(),
        },
        sync_scope={"scope_keys": [second.relative_to(root).as_posix()]},
    )
    facts = list(scoped.facts)

    assert facts == []
    assert scoped.completed_scope is True
    assert scoped.record_set is not None
    assert set(scoped.record_set.records) == {str(first.resolve())}
    assert str(second.resolve()) not in scoped.next_cursor["transcripts"]


def test_codex_sessions_escaped_scope_emits_no_facts(tmp_path: Path) -> None:
    from okg.substrate.library.sources.codex_sessions import CodexSessionsSource

    root = tmp_path / "codex" / "sessions"
    root.mkdir(parents=True)
    source = CodexSessionsSource(
        repo="demo",
        codex_sessions_root=root,
        archived_sessions_root=None,
        since="all",
    )

    run = source.run(
        "run-escaped",
        mode="scope_incomplete",
        sync_scope={"scope_keys": ["../escape.jsonl"]},
    )

    assert list(run.facts) == []
    assert run.completed_scope is False


def _write_codex_transcript(
    path: Path,
    *,
    session_id: str,
    repo: Path,
    extra_command: str | None = None,
) -> None:
    tool_timestamp = (
        "2026-05-02T12:00:02Z"
        if extra_command is not None
        else "2026-05-02T12:00:01Z"
    )
    rows = [
        {
            "timestamp": "2026-05-02T12:00:00Z",
            "type": "session_meta",
            "payload": {
                "id": session_id,
                "timestamp": "2026-05-02T12:00:00Z",
                "cwd": str(repo),
                "originator": "Codex Desktop",
                "cli_version": "0.126.0",
                "model_provider": "openai",
                "git": {"branch": "main", "commit_hash": "deadbeef"},
            },
        },
        {
            "timestamp": tool_timestamp,
            "type": "response_item",
            "payload": {
                "type": "function_call",
                "name": "exec_command",
                "arguments": json.dumps({
                    "cmd": extra_command or "sed -n '1p' src/app.py",
                    "workdir": str(repo),
                }),
            },
        },
    ]
    # Newline-terminated, as every one of the 1713 real transcripts on this
    # host is. An unterminated final line means a writer mid-append, and the
    # reader now defers it rather than consuming it -- consuming it put the
    # stop offset mid-line, which made a live session re-parse from zero on
    # every append.
    _write(path, "\n".join(json.dumps(row) for row in rows) + "\n")


def _append_codex_tool_call(
    path: Path,
    *,
    repo: Path,
    cmd: str,
) -> None:
    row = {
        "timestamp": "2026-05-02T12:00:03Z",
        "type": "response_item",
        "payload": {
            "type": "function_call",
            "name": "exec_command",
            "arguments": json.dumps({
                "cmd": cmd,
                "workdir": str(repo),
            }),
        },
    }
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row) + "\n")


# ---------------------------------------------------------------
# Regression: code-data-flow must not crash on a bracketed
# template-literal URL. Python 3.12+ urlsplit validates bracketed
# hosts and raises ValueError when the content isn't a real IPv6
# literal — e.g. a JS `https://[${l.value}]/...` template URL. Before
# the guard, that ValueError aborted the whole source run and skipped
# its 5 downstream sources.
# ---------------------------------------------------------------


def test_url_hosts_skips_malformed_bracketed_template_url() -> None:
    # _url_hosts moved to code_graph_dataflow.py in dev's Wave D-27 split
    # (`extract-code-graph-dataflow`); the code_graph shim re-exports the
    # classes but not module-private helpers.
    from okg.substrate.library.sources.code_graph_dataflow import _url_hosts

    # The exact shape that crashed the okg-workspace code-data-flow run:
    # a bracketed host whose content ('${l.value}') is not a valid IP.
    assert _url_hosts("https://[${l.value}]/api") == []
    # A normal URL still yields its host (guard didn't over-skip).
    assert _url_hosts("see https://api.github.com/repos for docs") == [
        "api.github.com"
    ]
    # A well-formed bracketed IPv6 URL is still parsed.
    assert _url_hosts("https://[::1]:8080/health") == ["[::1]:8080"]


def test_code_data_flow_source_survives_template_literal_url(
    tmp_path: Path,
) -> None:
    pytest.importorskip("tree_sitter_language_pack")
    from okg.substrate.library.sources.code_graph import CodeDataFlowSource

    _write(
        tmp_path / "src" / "client.js",
        'const base = "https://[${l.value}]/api";\n'
        'fetch(base + "/items");\n',
    )
    source = CodeDataFlowSource(repo="demo", repo_root=tmp_path)
    # Must not raise (pre-fix: ValueError from urlsplit's bracketed-host
    # check propagated out of the source run).
    facts = list(source.run("run-1", mode="scope_complete").facts)
    assert isinstance(facts, list)


def _graph_node_exists(dsn: str, node_id: str) -> bool:
    import psycopg

    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).query(
            """
            SELECT 1 AS present
              FROM okg.graph_nodes
             WHERE node_id = :node_id
             LIMIT 1
            """,
            {"node_id": node_id},
        )
    return bool(rows)


def test_code_structure_source_skips_unparseable_file(tmp_path: Path) -> None:
    """A single unparseable file (e.g. Python-2 syntax in a legacy repo)
    must be skipped, not abort indexing of the whole repository."""
    from okg.substrate.library.sources.code_graph import CodeStructureSource

    # Valid file: should still be indexed.
    _write(
        tmp_path / "src" / "good.py",
        "def good():\n    return 1\n",
    )
    # Unparseable file: a Python-2 print statement raises SyntaxError ->
    # CodeParseFailed inside the extractor.
    _write(
        tmp_path / "src" / "legacy.py",
        'print "python-2 syntax"\n',
    )

    source = CodeStructureSource(repo="demo", repo_root=tmp_path)
    # Must not raise despite the unparseable file.
    facts = list(source.run("run-1", mode="cursor").facts)

    contains = {
        e.src
        for e in facts
        if isinstance(e, EdgeFact) and e.edge_type == "contains"
    }
    # The valid file still produced symbols...
    assert "file:demo:src/good.py" in contains
    # ...and the unparseable file emitted nothing (was skipped, not fatal).
    assert "file:demo:src/legacy.py" not in contains


# --- issue #3259: a scope key narrows the walk, never widens it -------------


def _scope_fence_repo(repo: Path) -> None:
    """A git repo with one file inside the walk, one outside the extension
    allowlist, and one under a directory a `**/gen/**` exclude prunes."""
    _write(
        repo / "src" / "app.py",
        "import os\n\n\ndef app():\n    return os.getenv('APP_TOKEN')\n",
    )
    _write(repo / "docs" / "notes.md", "# notes\n")
    _write(
        repo / "gen" / "skip.py",
        "import os\n\n\ndef skip():\n    return os.getenv('GEN_TOKEN')\n",
    )
    _init_git(repo)


def _scope_fence_kwargs() -> dict:
    from okg.substrate.library.sources._code_graph_common import (
        DEFAULT_EXCLUDES,
    )

    return {
        "include_exts": [".py"],
        "exclude_globs": (*DEFAULT_EXCLUDES, "**/gen/**"),
    }


def _touch_scope_fence(repo: Path) -> None:
    _write(
        repo / "src" / "app.py",
        "import os\n\n# edited\ndef app():\n    return os.getenv('APP_TOKEN')\n",
    )
    _write(repo / "docs" / "notes.md", "# notes v2\n")
    _write(
        repo / "gen" / "skip.py",
        "import os\n\n# edited\ndef skip():\n    return os.getenv('GEN_TOKEN')\n",
    )


_SCOPE_FENCE_KEYS = ["src/app.py", "docs/notes.md", "gen/skip.py"]


def _assert_scope_fence(
    source,  # noqa: ANN001
    *,
    tmp_path: Path,
    full_run_reads_excluded: bool = False,
) -> None:
    """A scoped run reads only keys its walk selects and leaves the rest.

    `full_run_reads_excluded` marks a reader whose unscoped git-snapshot read
    ignores `exclude_globs` today (a separate defect reported with #3259):
    its full run emits `gen/skip.py`, and the scoped run must then neither
    re-read nor retract that record.
    """
    repo = tmp_path / "repo"
    first = source.run("run-1", mode="scope_complete")
    first_facts = list(first.facts)
    assert first.record_set is not None
    full_keys = set(first.record_set.records)
    first_files = dict((first.next_cursor or {}).get("files") or {})
    assert "src/app.py" in full_keys
    assert "docs/notes.md" not in full_keys
    if full_run_reads_excluded:
        assert "gen/skip.py" in full_keys
    else:
        # The full walk never selects the excluded file.
        assert "gen/skip.py" not in full_keys
        assert "gen/skip.py" not in json.dumps(
            [getattr(f, "source_record_id", {}) for f in first_facts],
        )
    cursor = dict(first.next_cursor or {})
    cursor["record_set"] = first.record_set.to_cursor()

    _touch_scope_fence(repo)
    scoped = source.run(
        "run-2",
        mode="scope_complete",
        cursor=cursor,
        sync_scope={"scope_keys": list(_SCOPE_FENCE_KEYS)},
    )
    facts = list(scoped.facts)

    assert facts, "the in-scope edit must be read"
    assert "gen/skip.py" not in json.dumps(
        [
            [
                getattr(f, "node_id", ""),
                getattr(f, "src", ""),
                getattr(f, "dst", ""),
                getattr(f, "source_record_id", {}),
            ]
            for f in facts
        ],
        default=str,
    )
    next_files = dict((scoped.next_cursor or {}).get("files") or {})
    assert set(next_files) == set(first_files)
    # The excluded file was not re-read: its cursor entry is untouched.
    assert next_files.get("gen/skip.py") == first_files.get("gen/skip.py")
    assert next_files["src/app.py"] != first_files["src/app.py"]
    if scoped.record_set is not None:
        # Outside keys neither enter (resurrect) nor leave (retract) the
        # footprint: the scoped run's record set equals the full one's keys.
        assert set(scoped.record_set.records) == full_keys
    if scoped.record_set_replacement_keys is not None:
        assert scoped.record_set_replacement_keys <= {"src/app.py"}
    assert source.last_run_stats["scope_keys"] == 3
    assert source.last_run_stats["scope_keys_outside_source_paths"] == 2


def test_code_structure_scoped_run_stays_inside_walk(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("tree_sitter_language_pack")
    from okg.substrate.library.sources.code_graph import CodeStructureSource

    monkeypatch.setenv("OKG_FILE_WALK_USE_MTIME", "0")
    repo = tmp_path / "repo"
    _scope_fence_repo(repo)
    source = CodeStructureSource(
        repo="demo", repo_root=repo, **_scope_fence_kwargs(),
    )

    _assert_scope_fence(source, tmp_path=tmp_path)
    assert source.last_run_stats["files_seen"] == 1


def test_interface_scoped_run_stays_inside_walk(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources.code_graph import InterfaceSource

    monkeypatch.setenv("OKG_FILE_WALK_USE_MTIME", "0")
    repo = tmp_path / "repo"
    _scope_fence_repo(repo)
    _write(
        repo / "src" / "app.py",
        "import argparse\n\n\ndef main():\n"
        "    parser = argparse.ArgumentParser(prog='app')\n"
        "    return parser\n",
    )
    _write(
        repo / "gen" / "skip.py",
        "import argparse\n\n\ndef main():\n"
        "    parser = argparse.ArgumentParser(prog='skip')\n"
        "    return parser\n",
    )
    source = InterfaceSource(
        repo="demo", repo_root=repo, **_scope_fence_kwargs(),
    )

    first = source.run("run-1", mode="scope_complete")
    list(first.facts)
    assert first.record_set is not None
    cursor = dict(first.next_cursor or {})
    cursor["record_set"] = first.record_set.to_cursor()
    first_files = dict(cursor.get("files") or {})
    # The unscoped git-snapshot read ignores exclude_globs today (separate
    # defect reported with #3259), so the full run did read gen/skip.py.
    assert "gen/skip.py" in first_files

    _write(
        repo / "gen" / "skip.py",
        "import argparse\n\n# edited\ndef main():\n"
        "    parser = argparse.ArgumentParser(prog='skip')\n"
        "    return parser\n",
    )
    _write(
        repo / "src" / "app.py",
        "import argparse\n\n# edited\ndef main():\n"
        "    parser = argparse.ArgumentParser(prog='app')\n"
        "    return parser\n",
    )
    scoped = source.run(
        "run-2",
        mode="scope_complete",
        cursor=cursor,
        sync_scope={"scope_keys": list(_SCOPE_FENCE_KEYS)},
    )
    facts = list(scoped.facts)

    assert "gen/skip.py" not in json.dumps(
        [getattr(f, "source_record_id", {}) for f in facts], default=str,
    )
    next_files = dict((scoped.next_cursor or {}).get("files") or {})
    # Not re-read (entry untouched) and not retracted (entry kept).
    assert next_files["gen/skip.py"] == first_files["gen/skip.py"]
    assert next_files["src/app.py"] != first_files["src/app.py"]
    assert scoped.record_set is not None
    assert set(scoped.record_set.records) == set(first.record_set.records)
    assert source.last_run_stats["scope_keys"] == 3
    assert source.last_run_stats["scope_keys_outside_source_paths"] == 2


def test_code_data_flow_scoped_run_stays_inside_walk(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.library.sources.code_graph import CodeDataFlowSource

    monkeypatch.setenv("OKG_FILE_WALK_USE_MTIME", "0")
    repo = tmp_path / "repo"
    _scope_fence_repo(repo)
    source = CodeDataFlowSource(
        repo="demo", repo_root=repo, **_scope_fence_kwargs(),
    )

    _assert_scope_fence(
        source, tmp_path=tmp_path, full_run_reads_excluded=True,
    )
    assert source.last_run_stats["files_seen"] == 1


def test_scope_key_in_walk_matches_full_walk_filters(tmp_path: Path) -> None:
    from okg.substrate.library.sources._code_graph_common import (
        DEFAULT_EXCLUDES,
        _scope_key_in_walk,
        _walk_files,
    )

    repo = tmp_path / "repo"
    paths = [
        "src/app.py",
        "gen/skip.py",
        "pkg/gen/deep.py",
        "vendor/lib.py",
        "node_modules/x/index.py",
        "docs/notes.md",
    ]
    for rel in paths:
        _write(repo / rel, "x = 1\n")
    excludes = (*DEFAULT_EXCLUDES, "**/gen/**", "vendor")
    walked = {
        p.relative_to(repo).as_posix()
        for p in _walk_files(
            repo,
            include_exts=[".py"],
            exclude_globs=excludes,
            max_files=100,
            max_bytes_per_file=1_000_000,
        )
    }
    admitted = {
        rel
        for rel in paths
        if _scope_key_in_walk(rel, include_exts=[".py"], exclude_globs=excludes)
    }
    assert walked == {"src/app.py"}
    assert admitted == walked


def test_scope_keys_govern_symlinks_like_the_full_walk(tmp_path: Path) -> None:
    """A symlinked file is governed under its own name, as `os.walk` lists
    it; a path through a symlinked directory is not walked at all."""
    from okg.substrate.library.sources._code_graph_common import (
        DEFAULT_EXCLUDES,
        _scope_record_keys_in_walk,
        _walk_files,
    )

    repo = tmp_path / "repo"
    _write(repo / "src" / "app.py", "x = 1\n")
    (repo / "link.py").symlink_to(Path("src") / "app.py")
    (repo / "linkdir").symlink_to("src", target_is_directory=True)
    walked = {
        p.relative_to(repo).as_posix()
        for p in _walk_files(
            repo,
            include_exts=[".py"],
            exclude_globs=DEFAULT_EXCLUDES,
            max_files=100,
            max_bytes_per_file=1_000_000,
        )
    }
    keys = ["link.py", "linkdir/app.py", "src/app.py", "./src/app.py"]

    governed = _scope_record_keys_in_walk(
        repo, keys, include_exts=[".py"], exclude_globs=DEFAULT_EXCLUDES,
    )

    assert walked == {"src/app.py", "link.py"}
    assert governed == walked


def test_code_structure_scoped_symlink_key_reads_the_link_record(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("tree_sitter_language_pack")
    from okg.substrate.library.sources.code_graph import CodeStructureSource

    monkeypatch.setenv("OKG_FILE_WALK_USE_MTIME", "0")
    repo = (tmp_path / "repo").resolve()
    _write(repo / "src" / "app.py", "def app():\n    return 1\n")
    (repo / "link.py").symlink_to(Path("src") / "app.py")
    source = CodeStructureSource(repo="demo", repo_root=repo, include_exts=[".py"])

    run = source.run(
        "run-scoped",
        mode="scope_incomplete",
        sync_scope={"scope_keys": ["link.py"]},
    )
    list(run.facts)

    assert set((run.next_cursor or {}).get("files") or {}) == {"link.py"}


def _symlink_fixture(root: Path) -> None:
    _write(root / ".gitignore", "ignored.py\n")
    _write(
        root / "src" / "app.py",
        "import os\n\n\ndef app():\n    return os.getenv('APP_TOKEN')\n",
    )
    _write(
        root / "ignored.py",
        "import os\n\n\ndef ignored():\n    return os.getenv('IGNORED_TOKEN')\n",
    )
    (root / "link.py").symlink_to(Path("src") / "app.py")


def _git_snapshot_reader(name: str, repo: Path):  # noqa: ANN202
    from okg.substrate.library.sources.code_graph import (
        CodeDataFlowSource,
        InterfaceSource,
    )

    cls = {"interfaces": InterfaceSource, "data-flow": CodeDataFlowSource}[name]
    return cls(repo="demo", repo_root=repo, include_exts=[".py"])


@pytest.mark.parametrize("reader", ["interfaces", "data-flow"])
@pytest.mark.parametrize("in_git", [True, False], ids=["git-snapshot", "os-walk"])
def test_snapshot_readers_scoped_symlink_and_ignored_keys_match_full_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    in_git: bool,
    reader: str,
) -> None:
    """On a git repository the full run of interfaces and code-data-flow
    reads a git-visible snapshot, which skips a clean tracked symlink and a
    gitignored file; outside git it walks the tree with os.walk, which reads
    both. A scoped run reads the same keys the full run reads, so records
    never flip between them."""
    monkeypatch.setenv("OKG_FILE_WALK_USE_MTIME", "0")
    repo = (tmp_path / "repo").resolve()
    _symlink_fixture(repo)
    if in_git:
        _init_git(repo)
    source = _git_snapshot_reader(reader, repo)
    assert source.probe_short_circuit_safe is in_git

    full = source.run("run-1", mode="scope_complete")
    list(full.facts)
    full_files = set((full.next_cursor or {}).get("files") or {})
    expected = (
        {"src/app.py"} if in_git else {"src/app.py", "link.py", "ignored.py"}
    )
    assert full_files == expected

    scoped = source.run(
        "run-2",
        mode="scope_incomplete",
        sync_scope={"scope_keys": ["link.py", "ignored.py", "src/app.py"]},
    )
    list(scoped.facts)

    assert set((scoped.next_cursor or {}).get("files") or {}) == expected


def test_code_structure_absolute_scope_keys_inside_the_root_are_read(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("tree_sitter_language_pack")
    from okg.substrate.library.sources.code_graph import CodeStructureSource

    monkeypatch.setenv("OKG_FILE_WALK_USE_MTIME", "0")
    repo = tmp_path / "repo"
    _write(repo / "src" / "app.py", "def app():\n    return 1\n")
    _write(tmp_path / "elsewhere" / "x.py", "def x():\n    return 2\n")
    source = CodeStructureSource(repo="demo", repo_root=repo, include_exts=[".py"])

    run = source.run(
        "run-scoped",
        mode="scope_incomplete",
        sync_scope={
            "scope_keys": [
                str(repo / "src" / "app.py"),
                str(tmp_path / "elsewhere" / "x.py"),
            ],
        },
    )
    list(run.facts)

    assert set((run.next_cursor or {}).get("files") or {}) == {"src/app.py"}
    assert source.last_run_stats["scope_keys_outside_source_paths"] == 1


def _git_cmd(repo: Path, *args: str) -> None:
    subprocess.run(
        [
            "git", "-C", str(repo),
            "-c", "user.email=test@example.com",
            "-c", "user.name=Test",
            "-c", "protocol.file.allow=always",
            *args,
        ],
        check=True,
        capture_output=True,
    )


def _repo_with_submodule_and_nested_repo(tmp_path: Path) -> Path:
    """A git repo with a checked-out submodule `sub/` (one tracked file and
    one new untracked file inside it) and an untracked nested repository
    `nested/` with its own commit."""
    inner = tmp_path / "inner"
    _write(inner / "a.py", "import os\nA = os.getenv('SUB_TRACKED')\n")
    _init_git(inner)
    repo = tmp_path / "repo"
    _write(repo / "app.py", "import os\nAPP = os.getenv('APP_TOKEN')\n")
    _init_git(repo)
    _git_cmd(repo, "submodule", "add", "-q", str(inner), "sub")
    _git_cmd(repo, "commit", "-q", "-m", "add submodule")
    _write(repo / "sub" / "new.py", "import os\nN = os.getenv('SUB_NEW')\n")
    _write(repo / "nested" / "n.py", "import os\nN = os.getenv('NESTED')\n")
    _init_git(repo / "nested")
    return repo


@pytest.mark.parametrize(
    "keys",
    [["sub/a.py", "sub/new.py", "app.py"], ["nested/n.py", "app.py"]],
    ids=["submodule", "nested-repo"],
)
@pytest.mark.parametrize("reader", ["interfaces", "data-flow"])
def test_snapshot_readers_skip_submodule_and_nested_repo_keys(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reader: str,
    keys: list[str],
) -> None:
    """The git-visible snapshot never reads inside a submodule or a nested
    repository. A scoped key there must not crash the run (`git
    check-ignore` exits 128 on a submodule path) nor be read."""
    monkeypatch.setenv("OKG_FILE_WALK_USE_MTIME", "0")
    repo = _repo_with_submodule_and_nested_repo(tmp_path)
    source = _git_snapshot_reader(reader, repo)

    full = source.run("run-1", mode="scope_complete")
    list(full.facts)
    full_files = set((full.next_cursor or {}).get("files") or {})
    assert full_files == {"app.py"}

    scoped = source.run(
        "run-2",
        mode="scope_incomplete",
        sync_scope={"scope_keys": list(keys)},
    )
    list(scoped.facts)

    assert set((scoped.next_cursor or {}).get("files") or {}) == full_files


def test_scope_key_relative_resolves_an_aliased_root_but_not_the_file(
    tmp_path: Path,
) -> None:
    from okg.substrate.library.sources._code_graph_common import (
        _scope_key_relative,
    )

    real = tmp_path / "real"
    repo = real / "repo"
    _write(repo / "src" / "app.py", "x = 1\n")
    (repo / "link.py").symlink_to(Path("src") / "app.py")
    (repo / "linkdir").symlink_to("src", target_is_directory=True)
    alias = tmp_path / "alias"
    alias.symlink_to(real, target_is_directory=True)
    aliased = alias / "repo"

    assert _scope_key_relative(repo, str(aliased / "src" / "app.py")) == "src/app.py"
    assert _scope_key_relative(aliased, str(repo / "src" / "app.py")) == "src/app.py"
    # The file name is never resolved, and an in-repo symlinked directory
    # keeps its spelling so the walk rule can drop it.
    assert _scope_key_relative(repo, str(aliased / "link.py")) == "link.py"
    assert _scope_key_relative(repo, str(repo / "linkdir" / "app.py")) == (
        "linkdir/app.py"
    )
    assert _scope_key_relative(repo, str(tmp_path / "elsewhere.py")) is None


@pytest.mark.parametrize("reader", ["interfaces", "data-flow"])
def test_snapshot_readers_read_untracked_files_in_a_nested_repo_under_a_tracked_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reader: str,
) -> None:
    """Git lists untracked files inside a nested repository when the outer
    index already tracks paths below that directory, so the git-visible full
    run reads them; the scoped run must read them too."""
    monkeypatch.setenv("OKG_FILE_WALK_USE_MTIME", "0")
    repo = tmp_path / "repo"
    _write(repo / "app.py", "import os\nAPP = os.getenv('APP_TOKEN')\n")
    _write(repo / "v" / "t.py", "import os\nT = os.getenv('V_TRACKED')\n")
    _init_git(repo)
    _init_git(repo / "v")
    _write(repo / "v" / "n.py", "import os\nN = os.getenv('V_NEW')\n")
    others = subprocess.run(
        ["git", "-C", str(repo), "ls-files", "--others"],
        check=True, capture_output=True, text=True,
    ).stdout.split()
    assert others == ["v/n.py"]
    source = _git_snapshot_reader(reader, repo)

    full = source.run("run-1", mode="scope_complete")
    list(full.facts)
    full_files = set((full.next_cursor or {}).get("files") or {})
    assert full_files == {"app.py", "v/t.py", "v/n.py"}

    scoped = source.run(
        "run-2",
        mode="scope_incomplete",
        sync_scope={"scope_keys": ["v/n.py", "v/t.py"]},
    )
    list(scoped.facts)

    assert set((scoped.next_cursor or {}).get("files") or {}) == {"v/n.py", "v/t.py"}


@pytest.mark.parametrize("dirname", [":x", ":(y"], ids=["colon", "colon-paren"])
@pytest.mark.parametrize("reader", ["interfaces", "data-flow"])
def test_snapshot_readers_take_magic_looking_directory_names_literally(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    reader: str,
    dirname: str,
) -> None:
    """A directory named `:x` or `:(y` is a path, not pathspec magic. The
    nested-repository check must still see the outer index's entries below
    it (no dropped key, no flip) and must not exit 128."""
    monkeypatch.setenv("OKG_FILE_WALK_USE_MTIME", "0")
    repo = tmp_path / "repo"
    _write(repo / "app.py", "import os\nAPP = os.getenv('APP_TOKEN')\n")
    _write(repo / dirname / "t.py", "import os\nT = os.getenv('MAGIC_TRACKED')\n")
    _init_git(repo)
    _init_git(repo / dirname)
    _write(repo / dirname / "n.py", "import os\nN = os.getenv('MAGIC_NEW')\n")
    source = _git_snapshot_reader(reader, repo)

    full = source.run("run-1", mode="scope_complete")
    list(full.facts)
    full_files = set((full.next_cursor or {}).get("files") or {})
    assert full_files == {"app.py", f"{dirname}/t.py", f"{dirname}/n.py"}

    scoped = source.run(
        "run-2",
        mode="scope_incomplete",
        sync_scope={"scope_keys": [f"{dirname}/n.py", f"{dirname}/t.py"]},
    )
    list(scoped.facts)

    assert set((scoped.next_cursor or {}).get("files") or {}) == {
        f"{dirname}/n.py",
        f"{dirname}/t.py",
    }
