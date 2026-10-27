"""Every read-only ``git status`` probe passes ``--no-optional-locks`` (#4086).

Plain ``git status`` refreshes stat data and rewrites ``.git/index`` under
``.git/index.lock`` in the inspected repository, which can make a concurrent
commit there fail. A probe that only reads must pass the global option before
the subcommand. These tests pin that so a later edit cannot drop it silently.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

import pytest

from tests.mirror_scope import shipped

REPO = Path(__file__).resolve().parents[2]
FLAG = "--no-optional-locks"

# Call sites that run git status without the flag in their own argv, with the
# reason. Keyed by (repo-relative path, enclosing function); the value is how
# many such argvs that function may hold, so a new unflagged probe, even in
# the same function, fails the guard.
ALLOWED_WITHOUT_FLAG = {
    # The shared helper prepends the flag to every command it runs.
    ("src/okg/substrate/runtime/deployment_drift.py", "_dirty_paths"): 1,
    # The probe environment sets GIT_OPTIONAL_LOCKS=0 for every command.
    ("src/okg/substrate/library/agent_runtimes/coordination_ssh.py", "_trusted_repo_root"): 1,
    # Inspect a detached worktree this module created for the release.
    ("src/okg/substrate/sealed_release.py", "prepare_sealed_release"): 2,
}

_STATUS_COMPANIONS = ("--porcelain", "--short", "--untracked-files")


def _scanned_files() -> list[Path]:
    files = sorted((REPO / "src" / "okg").rglob("*.py"))
    files.append(REPO / "scripts" / "okg_lanes.py")
    # scripts/okg_lanes.py is an internal script the public copy leaves out;
    # there shipped() drops it. Here it is the identity.
    return shipped(files, root=REPO)


def _status_argvs(path: Path) -> list[tuple[int, str, list[str]]]:
    """Constant-string argument sequences that look like a git status argv."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for function in ast.walk(tree):
        if isinstance(function, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for child in ast.walk(function):
                child.function = function.name  # innermost wins (walk is outer-first)
    found: list[tuple[int, str, list[str]]] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.List, ast.Tuple)):
            items = node.elts
        elif isinstance(node, ast.Call):
            items = node.args
        else:
            continue
        words = [
            item.value
            for item in items
            if isinstance(item, ast.Constant) and isinstance(item.value, str)
        ]
        if "status" in words and any(
            word.startswith(_STATUS_COMPANIONS) for word in words
        ):
            found.append((node.lineno, getattr(node, "function", ""), words))
    return found


def test_every_status_probe_passes_the_flag_before_the_subcommand() -> None:
    missing: list[str] = []
    compliant = 0
    unflagged: dict[tuple[str, str], int] = {}
    for path in _scanned_files():
        relative = path.relative_to(REPO).as_posix()
        for lineno, function, words in _status_argvs(path):
            if FLAG in words and words.index(FLAG) < words.index("status"):
                compliant += 1
                continue
            key = (relative, function)
            unflagged[key] = unflagged.get(key, 0) + 1
            if unflagged[key] > ALLOWED_WITHOUT_FLAG.get(key, 0):
                missing.append(f"{relative}:{lineno} in {function}() {words}")
    assert missing == []
    # Each exception is used exactly as declared, so none goes stale.
    assert unflagged == ALLOWED_WITHOUT_FLAG
    # The scan is not vacuous: it saw the probes this change flagged.
    assert compliant >= 10


def test_allowed_exceptions_still_exist() -> None:
    for relative, _function in ALLOWED_WITHOUT_FLAG:
        assert (REPO / relative).is_file(), relative


def _assert_flag_precedes_status(argvs: list[list[str]]) -> None:
    status = [argv for argv in argvs if "status" in argv]
    assert status, f"no status argv recorded in {argvs}"
    for argv in status:
        assert argv.index(FLAG) < argv.index("status"), argv


def test_source_urls_dirty_paths_argv(monkeypatch: pytest.MonkeyPatch) -> None:
    from okg.substrate.library.sources import _git_helpers, _source_urls

    argvs: list[list[str]] = []

    def record(repo_root: Path, args: list[str], **_: Any) -> str:
        argvs.append(list(args))
        return ""

    monkeypatch.setattr(_git_helpers, "run_git", record)
    assert _source_urls.dirty_paths(Path("/unused")) == frozenset()
    _assert_flag_precedes_status(argvs)


def test_guided_remove_registry_uncommitted_argv(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import subprocess

    from okg.substrate.sources import guided_remove

    argvs: list[list[str]] = []

    def record(dep: Path, *args: str, **_: Any) -> Any:
        argvs.append(list(args))
        return subprocess.CompletedProcess(args, 0, stdout="", stderr="")

    monkeypatch.setattr(guided_remove, "_git", record)
    guided_remove._registry_uncommitted(Path("/unused"), Path("registry.yaml"))
    _assert_flag_precedes_status(argvs)


def test_okg_repo_binding_status_argv() -> None:
    from okg.substrate.consistency.batch_measurements import okg_repo_binding

    argvs: list[list[str]] = []

    def record(argv: Any) -> str:
        argvs.append(list(argv))
        return "abc123" if "rev-parse" in argv else ""

    okg_repo_binding(
        repo_root="/unused",
        corpus_manifest_digest="d",
        resource_config_version=1,
        resource_config_digest="d",
        run_git=record,
    )
    _assert_flag_precedes_status(argvs)
