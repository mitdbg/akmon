"""Hints name a command and a document the reader can use (issue #3480).

`okg doctor` told wheel users to run `uv run okg ...` and to read
`docs/runbooks/...`, a path that exists only in a source checkout. These
tests pin both forms: the repository forms inside a checkout, plain `okg`
and a public link everywhere else.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.cli.command_reference import okg_command
from okg.substrate.support import user_hints


@pytest.fixture
def installed(monkeypatch):
    """Behave as an installed wheel: no checkout around the package."""
    monkeypatch.setattr(user_hints, "checkout_root", lambda: None)


def test_a_checkout_is_detected_from_this_repository() -> None:
    root = user_hints.checkout_root()
    assert root is not None
    assert (root / "pyproject.toml").is_file()
    assert (root / "docs" / "runbooks" / "troubleshooting" / "storage.md").is_file()


def test_a_checkout_keeps_the_repository_command() -> None:
    command = okg_command("doctor", deployment="x")
    assert user_hints.hint_command(command) == "uv run okg doctor --deployment x"


def test_an_installed_package_gets_plain_okg(installed) -> None:
    command = okg_command("deployment apply", "x", json=True)
    assert user_hints.hint_command(command) == "okg deployment apply x --json"


def test_an_installed_package_gets_a_public_link(installed) -> None:
    ref = user_hints.doc_reference("docs/runbooks/troubleshooting/storage.md")
    assert ref == (
        "https://github.com/mitdbg/akmon/blob/main/"
        "docs/runbooks/troubleshooting/storage.md"
    )


def test_a_checkout_reference_resolves_from_any_folder(tmp_path, monkeypatch) -> None:
    root = user_hints.checkout_root()
    assert root is not None
    path = "docs/runbooks/troubleshooting/storage.md"
    monkeypatch.chdir(root)
    assert user_hints.doc_reference(path) == path
    monkeypatch.chdir(tmp_path)
    resolved = user_hints.doc_reference(path)
    assert Path(resolved).is_absolute() and Path(resolved).is_file()


def test_the_env_doctor_hints_follow_the_install_kind(installed) -> None:
    """The deployment env doctor's next steps are among the hints a wheel
    user sees first."""
    from okg.substrate import deployment_env_doctor

    hints = deployment_env_doctor._recommended_commands(
        "demo", unresolved=["OKG_DSN"], resolved=False,
        suggested_dsn="postgresql://u@h/db",
    )
    assert hints and all(h.startswith("okg deployment ") for h in hints)
    assert not any("uv run" in h for h in hints)


def test_a_tree_without_docs_reads_as_installed(tmp_path, monkeypatch) -> None:
    """The checkout probe only checks that folders exist; with no `docs/`
    beside the package (a wheel) it picks the installed form and opens
    nothing."""
    module = tmp_path / "src" / "okg" / "substrate" / "support" / "user_hints.py"
    module.parent.mkdir(parents=True)
    module.write_text("")
    (tmp_path / "pyproject.toml").write_text("")
    monkeypatch.setattr(user_hints, "__file__", str(module))
    user_hints.checkout_root.cache_clear()
    try:
        assert user_hints.checkout_root() is None
        assert user_hints.okg_prefix() == "okg"
        user_hints.checkout_root.cache_clear()
        (tmp_path / "docs").mkdir()
        assert user_hints.checkout_root() == tmp_path.resolve()
    finally:
        user_hints.checkout_root.cache_clear()


# --- One place picks the command prefix (issue #2276) -----------------------

_SRC = Path(__file__).resolve().parents[2] / "src" / "okg"
_CHECKOUT_COMMAND = "uv run okg"

#: Runtime strings that must keep the literal checkout command: (path under
#: src/okg, the string's exact value). Exact, so an entry never covers a new
#: string beside it. Every entry says why.
_LITERAL_CHECKOUT_COMMAND_ALLOWED = {
    # The one place: the checkout form okg_prefix() returns.
    ("substrate/support/user_hints.py", "uv run okg"),
    # The stale-entrypoint refusal tells a person to cd into THIS checkout
    # and run its code; the checkout form is the point of the message.
    ("substrate/cli/main.py", " && uv run okg"),
    # Checkout provenance: the command a source checkout is expected to run,
    # and two messages that name "run from a checkout" by its command.
    ("substrate/support/env_provenance.py", "uv run okg"),
    ("substrate/support/env_provenance.py",
     " Fix either way: install the missing dependency into that tool "
     "environment, or run from a checkout (`uv run okg ...`)."),
    ("substrate/support/env_provenance.py",
     "`), or run from a checkout (`uv run okg ...`) with "),
    # Executed, not a hint: `okg dogfood service render --okg-bin` becomes the
    # command in the rendered compose (repo-mounted image), systemd and
    # launchd services, which run the checkout's uv project.
    ("substrate/cli/dogfood.py", "uv run okg"),
    # The worker service authority record: it pairs the command with
    # launcher_executable=uv and the checkout's .venv interpreter.
    ("substrate/deployment_apply.py", "uv run okg runtime worker"),
    # The source-sync optimization ledger must stay stdlib-only with no
    # effectful imports (tests/architecture/
    # test_source_sync_optimization_ledger_boundaries.py), so it cannot import
    # user_hints; its commands target okg's own workspace deployment, which
    # runs from a checkout.
    ("substrate/source_sync_optimization_ledger.py",
     "env -u OKG_DSN uv run okg sync microbench inventory --deployment "
     "okg-workspace --json"),
    ("substrate/source_sync_optimization_ledger.py",
     "env -u OKG_DSN uv run okg sync microbench ledger --deployment "
     "okg-workspace --json"),
    ("substrate/source_sync_optimization_ledger.py",
     "env -u OKG_DSN uv run okg view inspect --deployment okg-workspace "
     "--view okg_workspace.source_optimization_ledger --json"),
}

#: PACT names its own command from the repository's `pact/project.yaml`
#: (`pact.repair.repo_command`); its hints, git hooks, CI workflow and agent
#: instruction blocks are that authority's business, not okg_prefix()'s.
_OWN_COMMAND_AUTHORITY_DIRS = ("substrate/pact/",)


def _literal_checkout_commands(src: Path = _SRC) -> list[tuple[str, int, str]]:
    """(path, line, value) of every runtime string under src/okg that spells
    `uv run okg`. Docstrings are documentation, not output, and are skipped.
    """
    import ast

    found = []
    for path in sorted(src.rglob("*.py")):
        rel = path.relative_to(src).as_posix()
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source)
        docstrings = set()
        for node in ast.walk(tree):
            if isinstance(
                node,
                (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef),
            ) and node.body:
                first = node.body[0]
                if isinstance(first, ast.Expr) and isinstance(first.value, ast.Constant):
                    docstrings.add(id(first.value))
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Constant)
                and isinstance(node.value, str)
                and _CHECKOUT_COMMAND in node.value
                and id(node) not in docstrings
            ):
                found.append((rel, node.lineno, node.value))
    return found


def _allowed(rel: str, value: str) -> bool:
    return (
        (rel, value) in _LITERAL_CHECKOUT_COMMAND_ALLOWED
        or rel.startswith(_OWN_COMMAND_AUTHORITY_DIRS)
    )


def test_hints_take_the_command_prefix_from_one_place() -> None:
    """A hint that spells `uv run okg` fails for a wheel user, who has no uv
    project. Build it with okg_prefix() (or hint_command) instead."""
    offenders = [
        f"src/okg/{rel}:{line}: {value.strip()}"
        for rel, line, value in _literal_checkout_commands()
        if not _allowed(rel, value)
    ]
    assert not offenders, (
        "runtime strings spell the checkout command; use "
        "okg.substrate.support.user_hints.okg_prefix():\n" + "\n".join(offenders)
    )


def test_every_allowlist_entry_still_matches_a_string() -> None:
    """A stale exception would let a new literal hide behind it."""
    found = _literal_checkout_commands()
    stale = [
        entry
        for entry in _LITERAL_CHECKOUT_COMMAND_ALLOWED
        if not any((rel, value) == entry for rel, _, value in found)
    ]
    assert not stale


def test_the_scan_catches_a_literal_and_skips_a_docstring(tmp_path) -> None:
    """The guard fails on a new literal and stays quiet on documentation."""
    module = tmp_path / "substrate" / "demo.py"
    module.parent.mkdir(parents=True)
    module.write_text(
        '"""Run `uv run okg doctor` to check."""\n'
        'HINT = "run uv run okg doctor"\n',
        encoding="utf-8",
    )
    found = _literal_checkout_commands(tmp_path)
    assert [(rel, line) for rel, line, _ in found] == [("substrate/demo.py", 2)]
    assert not _allowed("substrate/demo.py", found[0][2])


def test_an_allowlisted_file_does_not_cover_a_new_string() -> None:
    """Review finding: a substring marker let a new hint in the same file
    pass. Entries match one exact value."""
    assert _allowed("substrate/cli/main.py", " && uv run okg")
    assert not _allowed(
        "substrate/cli/main.py", "run `uv run okg doctor` && uv run okg status",
    )
    assert not _allowed("substrate/deployment_apply.py", "run uv run okg runtime worker --once")


def test_migrated_hints_use_plain_okg_when_installed(installed, monkeypatch) -> None:
    """Hints built at call time follow the install kind."""
    from okg.substrate import deployment_apply, publish_recovery
    from okg.substrate.cli import deployment_readiness
    from okg.substrate.errors import SubstrateError

    assert publish_recovery._ingest_command("d") == (
        "okg ingest --deployment d --json"
    )

    monkeypatch.setattr(
        deployment_apply,
        "_environment_authority_for_apply",
        lambda **_: {
            "environment_id": "env-1",
            "adapter": "launchd",
            "service_id": "svc",
        },
    )
    with pytest.raises(SubstrateError) as refused:
        deployment_apply._require_tracking_apply_authority(
            deployment="d", deployment_dir="deployments/d", dsn="postgresql://x/y",
        )
    assert refused.value.fix_hint.startswith("okg environment service restart ")

    project = deployment_readiness._ready_rollout_project_command(
        deployment="d", repo_root=Path("/r"), mcp_port=8765,
    )
    assert project.startswith("okg install ")
    for hint in (refused.value.fix_hint, project):
        assert "uv run" not in hint


def test_migrated_hints_keep_uv_run_in_a_checkout() -> None:
    from okg.substrate import publish_recovery

    assert publish_recovery._ingest_command("d") == (
        "uv run okg ingest --deployment d --json"
    )
