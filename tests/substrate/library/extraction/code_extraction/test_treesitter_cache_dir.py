"""An unusable tree-sitter grammar cache names its path and variable (#2672).

tree-sitter-language-pack downloads each grammar on first use into a
per-user cache. On a host whose home directory is not writable every
code-structure source failed with an error that pointed at language support
instead of the cache. These tests run the real language pack in a child
process so an already-loaded grammar in this process cannot mask the cache.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

pytest.importorskip("tree_sitter_language_pack")

from okg.substrate.library.extraction.code import treesitter
from okg.substrate.library.extraction.code.treesitter import (
    TreeSitterNotInstalled,
    TreeSitterSymbolExtractor,
)


ROOT = Path(__file__).resolve().parents[5]
VARIABLE = "TREE_SITTER_LANGUAGE_PACK_CACHE_DIR"

_CHILD = r"""
import json
from okg.substrate.library.extraction.code.treesitter import (
    TreeSitterNotInstalled,
    TreeSitterSymbolExtractor,
)
try:
    TreeSitterSymbolExtractor(language="go")
except TreeSitterNotInstalled as exc:
    print(json.dumps({
        "code": exc.code,
        "message": str(exc),
        "fix_hint": exc.fix_hint,
        "data": exc.data,
        "cause_type": type(exc.__cause__).__name__,
        "cause": str(exc.__cause__),
    }))
else:
    print(json.dumps({"loaded": True}))
"""


def _run_child(cache_dir: Path) -> dict:
    env = dict(os.environ)
    env[VARIABLE] = str(cache_dir)
    result = subprocess.run(
        [sys.executable, "-c", _CHILD],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout.strip().splitlines()[-1])


def test_unwritable_grammar_cache_names_path_and_variable(tmp_path) -> None:
    # A regular file where the cache's parent should be: directory creation
    # fails even for root (CI containers), and nothing is downloaded.
    blocker = tmp_path / "not-a-directory"
    blocker.write_text("x", encoding="utf-8")
    cache_dir = blocker / "cache"

    report = _run_child(cache_dir)

    assert report.get("code") == "tree_sitter_not_installed", report
    # The library's own error survives, chained and quoted.
    assert report["cause_type"] == "DownloadError"
    assert "Failed to create cache directory" in report["cause"]
    assert report["cause"] in report["message"]
    # The operator sees the effective path and the variable that moves it.
    assert report["data"]["grammar_cache_dir"].startswith(str(cache_dir))
    assert report["data"]["cache_dir_variable"] == VARIABLE
    assert report["data"]["grammar_cache_dir"] in report["message"]
    assert VARIABLE in report["message"]
    # The variable was already set, so the message says so instead of
    # telling the operator to set it.
    assert f"{VARIABLE} is set to" in report["message"]
    assert VARIABLE in report["fix_hint"]
    assert "XDG_CACHE_HOME" in report["fix_hint"]
    assert "verify the language is supported" not in report["fix_hint"]


def test_variable_moves_the_grammar_cache(tmp_path) -> None:
    env = dict(os.environ)
    env[VARIABLE] = str(tmp_path / "grammars")
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import tree_sitter_language_pack as t; print(t.cache_dir())",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=True,
    )
    assert result.stdout.strip().startswith(str(tmp_path / "grammars"))


def test_other_grammar_failures_keep_the_language_hint(monkeypatch) -> None:
    import tree_sitter_language_pack as pack

    def _missing(name):
        raise pack.LanguageNotFoundError(f"Language not found: {name}")

    monkeypatch.setattr(pack, "get_language", _missing)
    with pytest.raises(TreeSitterNotInstalled) as excinfo:
        TreeSitterSymbolExtractor(language="go")
    error = excinfo.value
    assert "Language not found: go" in str(error)
    assert VARIABLE not in str(error)
    assert "grammar_cache_dir" not in error.data
    assert "verify the language is supported" in (error.fix_hint or "")


# Library texts for an unusable cache. The first two were reproduced with
# the real 1.17.0 pack (a cache that cannot be created; an existing cache
# made read-only, then a new grammar loaded); the others are quoted from the
# native module's strings.
_CACHE_TEXTS = (
    (
        "Download error: Failed to create cache directory /x/tree-sitter-language-pack/"
        "v1.17.0 with owner-only (0700) permissions: Permission denied (os error 13)"
    ),
    (
        "Download error: Failed to extract libtree_sitter_rust.dylib: IO error: "
        "Permission denied (os error 13)"
    ),
    (
        "Download error: Refusing to use cache directory /x because it is group- or "
        "other-writable"
    ),
    "Download cache lock error: could not lock",
    "Could not determine a cache directory: the platform reported none",
)


@pytest.mark.parametrize("text", _CACHE_TEXTS)
def test_every_unusable_cache_text_is_classified(monkeypatch, text: str) -> None:
    import tree_sitter_language_pack as pack

    def _fail(name):
        raise pack.DownloadError(text)

    monkeypatch.setattr(pack, "get_language", _fail)
    with pytest.raises(TreeSitterNotInstalled) as excinfo:
        TreeSitterSymbolExtractor(language="go")
    error = excinfo.value
    assert error.data["cache_dir_variable"] == VARIABLE
    assert error.data["grammar_cache_dir"] in str(error)
    assert text in str(error)
    assert VARIABLE in (error.fix_hint or "")
    assert isinstance(error.__cause__, pack.DownloadError)


def test_network_permission_refusal_is_not_a_cache_failure(monkeypatch) -> None:
    import tree_sitter_language_pack as pack

    def _refused(name):
        raise pack.DownloadError(
            "Download error: HTTP request failed: Permission denied (os error 13)"
        )

    monkeypatch.setattr(pack, "get_language", _refused)
    with pytest.raises(TreeSitterNotInstalled) as excinfo:
        TreeSitterSymbolExtractor(language="go")
    assert "grammar_cache_dir" not in excinfo.value.data
    assert VARIABLE not in str(excinfo.value)


def test_uv_tool_install_keeps_the_cache_hint_first(monkeypatch) -> None:
    import tree_sitter_language_pack as pack
    from okg.substrate.support.env_provenance import UV_TOOL_ENVIRONMENT

    def _fail(name):
        raise pack.DownloadError(_CACHE_TEXTS[0])

    monkeypatch.setattr(pack, "get_language", _fail)
    monkeypatch.setattr(
        treesitter,
        "classify_interpreter",
        lambda: ("/home/u/.local/share/uv/tools/okg/bin/python", UV_TOOL_ENVIRONMENT),
    )
    with pytest.raises(TreeSitterNotInstalled) as excinfo:
        TreeSitterSymbolExtractor(language="go")
    error = excinfo.value
    assert (error.fix_hint or "").startswith(f"set {VARIABLE}")
    assert "extras" not in (error.fix_hint or "")
    assert error.data["environment_kind"] == UV_TOOL_ENVIRONMENT
    assert "uv/tools/okg/bin/python" in str(error)


def test_unknown_cache_location_is_reported_not_guessed(monkeypatch) -> None:
    import tree_sitter_language_pack as pack

    def _no_cache():
        raise pack.ConfigError("Could not determine a cache directory")

    monkeypatch.setattr(pack, "cache_dir", _no_cache)
    data = treesitter._grammar_cache_failure(
        pack.DownloadError("Could not determine a cache directory"),
    )
    assert data is not None
    assert data["grammar_cache_dir"].startswith("<unknown: cache_dir() raised")
    # Only the pack's own errors are classified.
    assert treesitter._grammar_cache_failure(ValueError("cache directory")) is None


_SOURCE_CHILD = r"""
import json, sys
from okg.substrate.library.extraction.code.treesitter import TreeSitterNotInstalled
from okg.substrate.library.sources.code_graph_structure import CodeStructureSource
try:
    CodeStructureSource(repo="probe", repo_root=sys.argv[1])
except TreeSitterNotInstalled as exc:
    print(json.dumps({"message": str(exc), "data": exc.data}))
else:
    print(json.dumps({"constructed": True}))
"""


def test_code_structure_source_reports_the_cache(tmp_path) -> None:
    # Every code-structure source constructs all tree-sitter extractors, so
    # the cache failure surfaces when the source is built.
    blocker = tmp_path / "not-a-directory"
    blocker.write_text("x", encoding="utf-8")
    repo = tmp_path / "repo"
    repo.mkdir()
    env = dict(os.environ)
    env[VARIABLE] = str(blocker / "cache")
    result = subprocess.run(
        [sys.executable, "-c", _SOURCE_CHILD, str(repo)],
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    report = json.loads(result.stdout.strip().splitlines()[-1])
    assert "constructed" not in report, report
    assert VARIABLE in report["message"]
    assert report["data"]["grammar_cache_dir"].startswith(str(blocker / "cache"))


@pytest.mark.parametrize(
    "page",
    [
        "docs/start/evaluate-from-source.md",
        "docs/runbooks/workspace-graph/setup.md",
        "docs/runbooks/troubleshooting/first-run.md",
    ],
)
def test_install_and_triage_docs_name_the_variable(page: str) -> None:
    text = (ROOT / page).read_text(encoding="utf-8")
    assert VARIABLE in text, page
