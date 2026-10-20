"""Unit tests for the install-journey extraction (no database, no network).

PACT ``journey-install-tests``. The executed journeys in
``tests/e2e/test_first_run_golden_path.py`` only run in the
first-run-golden-path lane. These tests hold the coupling itself on every
ordinary run: the commands lifted from the install pages, the refusal when a
page changes a command, and the open-issue signatures the strict xfails
accept.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from tests.e2e.install_journey import (
    ACADEMIC_PAGE,
    INSTALL_DOC_DSN,
    INSTALL_PAGE,
    PACKAGED_PAGE,
    InstallDocExtractionError,
    dependency_check_argv,
    extract_install_journey,
    load_install_journey,
    matches_2331,
    matches_2469,
    matches_2470,
    offline_dependency_sync_argv,
    offline_wheel_install_argv,
    substitute,
)


def _pages() -> tuple[str, str, str]:
    return (
        INSTALL_PAGE.read_text(encoding="utf-8"),
        PACKAGED_PAGE.read_text(encoding="utf-8"),
        ACADEMIC_PAGE.read_text(encoding="utf-8"),
    )


def test_extracts_the_commands_the_pages_give() -> None:
    cmds = load_install_journey()
    assert cmds.build_wheel == "uv build --wheel"
    assert cmds.make_venv == "python3.12 -m venv ~/okg-venv"
    assert cmds.install_wheel == (
        "~/okg-venv/bin/pip install /path/to/okg-<version>-py3-none-any.whl"
    )
    assert cmds.check_version == "~/okg-venv/bin/okg --version"
    assert " ".join(cmds.workspace_install.replace("\\\n", " ").split()) == (
        "okg install --profile workspace-graph --deployment-name <name> "
        "--repo-root <repo> "
        f"--postgres-dsn {INSTALL_DOC_DSN} --json"
    )
    assert cmds.codex_recovery == "okg run --once --deployment <name> --apply"
    assert cmds.status == "okg status --deployment <name>"
    assert cmds.search == (
        "okg search --deployment <name> --query '<a function name>'"
    )
    assert cmds.profile_dry_run == (
        "okg install --profile <profile> --deployment-name <name> "
        "--dry-run --json"
    )
    assert cmds.academic_install == (
        "okg install --profile academic-researcher --name <name> --json"
    )
    assert cmds.academic_dsn_install == (
        "okg install --profile academic-researcher --postgres-dsn "
        "postgresql://user:pass@host:5432/okg"
    )
    assert cmds.academic_doctor == "okg doctor --deployment <name>"


@pytest.mark.parametrize(
    ("page", "old", "new"),
    [
        # install.md step 3 renames a flag.
        ("install", "--repo-root <repo>", "--repo <repo>"),
        # install.md drops its Codex recovery note.
        ("install", "**If you use Codex**", "**Codex users**"),
        # install.md step 4 changes the status command.
        ("install", "okg status --deployment <name>", "okg status <name>"),
        # install-packaged.md switches the wheel install to another tool.
        ("packaged", "~/okg-venv/bin/pip install", "pipx install"),
        # install-packaged.md stops handing over to install.md step 2.
        ("packaged", "from step 2", "from step 1"),
        # academic-researcher.md renames the default install's name flag.
        ("academic", "--name <name> --json", "--deployment <name> --json"),
        # academic-researcher.md changes its post-install doctor command.
        (
            "academic",
            "okg doctor --deployment <name>\n",
            "okg doctor <name>\n",
        ),
        # academic-researcher.md stops saying doctor must pass.
        (
            "academic",
            "`doctor` should complete without an error",
            "`doctor` reports the deployment",
        ),
    ],
)
def test_a_changed_command_on_a_page_fails_extraction(
    page: str, old: str, new: str,
) -> None:
    install, packaged, academic = _pages()
    texts = {"install": install, "packaged": packaged, "academic": academic}
    assert old in texts[page], f"test anchor {old!r} is not on the {page} page"
    texts[page] = texts[page].replace(old, new)
    with pytest.raises(InstallDocExtractionError):
        extract_install_journey(
            texts["install"], texts["packaged"], texts["academic"],
        )


def test_substitute_refuses_a_placeholder_it_does_not_fill() -> None:
    assert substitute("okg status --deployment <name>", {"<name>": "x"}) == (
        "okg status --deployment x"
    )
    with pytest.raises(InstallDocExtractionError, match="<repo>"):
        substitute("okg install --repo-root <repo>", {"<name>": "x"})


def test_substitute_replaces_the_dsn_literal_whole() -> None:
    out = substitute(
        f"okg install --postgres-dsn {INSTALL_DOC_DSN}",
        {INSTALL_DOC_DSN: "postgresql://postgres:okg@127.0.0.1:5548/okg_golden_wheel"},
    )
    assert out.endswith("127.0.0.1:5548/okg_golden_wheel")


def test_offline_install_never_resolves_against_an_index() -> None:
    # Dependencies come from the lockfile's own URLs, never a resolver.
    sync = offline_dependency_sync_argv(uv="uv")
    assert sync[:2] == ["uv", "sync"]
    for flag in ("--locked", "--offline", "--no-install-project", "--no-dev"):
        assert flag in sync
    # The wheel goes in without resolving its dependencies ...
    argv = offline_wheel_install_argv(
        uv="uv",
        venv_python=Path("/h/okg-venv/bin/python"),
        wheel=Path("/d/okg-0.1.0-py3-none-any.whl"),
    )
    assert argv[:3] == ["uv", "pip", "install"]
    assert "--offline" in argv and "--no-deps" in argv
    assert argv[argv.index("--python") + 1] == "/h/okg-venv/bin/python"
    assert argv[-1] == "/d/okg-0.1.0-py3-none-any.whl"
    # ... and a check then holds it to every requirement it declares.
    check = dependency_check_argv(
        uv="uv", venv_python=Path("/h/okg-venv/bin/python"),
    )
    assert check == ["uv", "pip", "check", "--python", "/h/okg-venv/bin/python"]


def test_open_issue_signatures_match_only_their_own_failure() -> None:
    issue_2469 = (
        "error: source run cannot acknowledge change identity "
        '"source_name": "demo.codex_sessions"'
    )
    issue_2470 = (
        'File "install.py", line 730, in _cmd_install\n'
        "    _load_source_defaults(repo_root)\n"
        "TypeError: chdir: path should be string, bytes, os.PathLike or "
        "integer, not NoneType"
    )
    issue_2331 = (
        "error [substrate_error]: no runnable sources selected for sync "
        "enqueue"
    )
    assert matches_2469(issue_2469)
    assert matches_2470(issue_2470)
    assert matches_2331(issue_2331)
    for text in (issue_2470, issue_2331, "ingest failed: timeout"):
        assert not matches_2469(text)
    for text in (issue_2469, issue_2331, "TypeError: something else"):
        assert not matches_2470(text)
    for text in (issue_2469, issue_2470, "no sources configured"):
        assert not matches_2331(text)
    # The source is named: a change-identity refusal of another source is
    # not #2469.
    assert not matches_2469(
        "error: source run cannot acknowledge change identity "
        '"source_name": "demo.git_history"'
    )
