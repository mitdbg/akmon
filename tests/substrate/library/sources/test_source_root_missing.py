"""Issue #3254: a path-rooted reader refuses a configured root that is missing.

Before the fix an unmanaged code-structure source walked a missing root as an
empty tree and returned a complete-scope run with no records (which retracts
every code fact), and doc-corpus failed with a bare ``FileNotFoundError`` that
named neither the source nor the setting. Both now raise ``SourceRootMissing``
naming the source, the setting, and the path; a root that is a file is refused
the same way. A managed checkout that never materialised still degrades to
``acquire_failed``. A junit_xml source, which wraps the test-report reader,
names itself in the error. No database is needed.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.errors import SourceRootMissing


def _assert_names_root(error: SourceRootMissing, source: str, path: Path) -> None:
    assert error.code == "source_root_missing"
    assert error.data == {
        "source": source,
        "setting": "repo_root",
        "path": str(path),
    }
    message = str(error)
    assert source in message
    assert "repo_root" in message
    assert str(path) in message
    assert error.fix_hint == SourceRootMissing.FIX_HINT


@pytest.mark.parametrize("mode", ["cursor", "scope_complete"])
def test_code_structure_missing_root_fails_the_run(tmp_path: Path, mode: str) -> None:
    from okg.substrate.library.sources.code_graph_structure import (
        CodeStructureSource,
    )

    missing = (tmp_path / "absent-root").resolve()
    source = CodeStructureSource(repo="demo", repo_root=missing)

    with pytest.raises(SourceRootMissing) as raised:
        source.run("run-missing-root", mode=mode)

    _assert_names_root(raised.value, "code_structure", missing)


def test_code_structure_root_that_is_a_file_fails_the_run(tmp_path: Path) -> None:
    from okg.substrate.library.sources.code_graph_structure import (
        CodeStructureSource,
    )

    not_a_dir = (tmp_path / "plain-file-root").resolve()
    not_a_dir.write_text("x", encoding="utf-8")
    source = CodeStructureSource(repo="demo", repo_root=not_a_dir)

    with pytest.raises(SourceRootMissing) as raised:
        source.run("run-file-root", mode="scope_complete")

    _assert_names_root(raised.value, "code_structure", not_a_dir)


def test_code_structure_managed_missing_checkout_still_degrades(
    tmp_path: Path,
) -> None:
    """A managed checkout that never materialised keeps its health path.

    The runner reports acquire_failed with zero facts and an incomplete
    scope; it does not raise SourceRootMissing.
    """
    from okg.substrate.library.sources.code_graph_structure import (
        CodeStructureSource,
    )

    missing = (tmp_path / "absent-checkout").resolve()
    source = CodeStructureSource(
        repo="demo",
        repo_root=missing,
        url="https://example.invalid/demo.git",
        manage_checkout=True,
    )

    run = source.run("run-managed-missing", mode="scope_complete")

    assert list(run.facts) == []
    assert run.completed_scope is False
    assert run.record_set is None
    assert run.health is not None
    assert run.health.status == "acquire_failed"


def test_code_structure_existing_plain_root_still_runs(tmp_path: Path) -> None:
    from okg.substrate.library.sources.code_graph_structure import (
        CodeStructureSource,
    )

    (tmp_path / "mod.py").write_text("def f():\n    return 1\n", encoding="utf-8")
    run = CodeStructureSource(repo="demo", repo_root=tmp_path).run("run-plain")

    assert list(run.facts)


def _extractors(tmp_path: Path) -> Path:
    ex = tmp_path / "extractors"
    ex.mkdir(parents=True)
    (ex / "extractors.yaml").write_text("parser: []\n")
    return ex


@pytest.mark.parametrize("non_git_walk", [False, True])
def test_doc_corpus_missing_root_fails_the_run(
    tmp_path: Path, non_git_walk: bool
) -> None:
    from okg.substrate.library.sources.doc_corpus import DocCorpusSource

    missing = (tmp_path / "absent-root").resolve()
    source = DocCorpusSource(
        repo="demo",
        repo_root=missing,
        extractors_dir=_extractors(tmp_path),
        include_globs=("**/*",),
        non_git_walk=non_git_walk,
        name="demo_docs",
    )

    with pytest.raises(SourceRootMissing) as raised:
        source.run("run-missing-root", mode="scope_complete")

    _assert_names_root(raised.value, "demo_docs", missing)


def test_doc_corpus_root_that_is_a_file_fails_the_run(tmp_path: Path) -> None:
    from okg.substrate.library.sources.doc_corpus import DocCorpusSource

    not_a_dir = (tmp_path / "plain-file-root").resolve()
    not_a_dir.write_text("x", encoding="utf-8")
    source = DocCorpusSource(
        repo="demo",
        repo_root=not_a_dir,
        extractors_dir=_extractors(tmp_path),
        include_globs=("**/*",),
        non_git_walk=True,
        name="demo_docs",
    )

    with pytest.raises(SourceRootMissing) as raised:
        source.run("run-file-root", mode="scope_complete")

    _assert_names_root(raised.value, "demo_docs", not_a_dir)


def test_junit_xml_missing_repo_root_names_the_junit_source(tmp_path: Path) -> None:
    """The junit_xml wrapper must name the operator's source, not its reader."""
    pytest.importorskip("defusedxml")
    from okg.substrate.library.sources.junit_xml import JUnitXmlSource

    drop = tmp_path / "junit-drop"
    drop.mkdir()
    missing = (tmp_path / "absent-root").resolve()
    source = JUnitXmlSource(
        name="ci_junit",
        drop_dir=drop,
        repo_root=missing,
        sha="0" * 40,
    )

    with pytest.raises(SourceRootMissing) as raised:
        source.run("run-missing-root", mode="scope_complete")

    _assert_names_root(raised.value, "ci_junit", missing)
