"""The academic-researcher installer re-enters ITS OWN okg for the scaffold child.

Issue #2211. `_provision_postgres_and_scaffold` spawned the scaffold step as
``okg install --scaffold-only ...`` -- a bare name, resolved by the shell
against PATH. A wheel user who runs ``./venv/bin/okg install`` without that
venv first on PATH therefore handed the step to whatever other ``okg``
happened to be installed (``~/.local/bin/okg``, an older release), which
exited rc=2 with ``unrecognized arguments: --scaffold-only``. The failure
named neither the second okg nor the fix.

Two guards, one per half of the defect:

* the argv the installer hands its child starts with the running
  interpreter, so no PATH entry can select the program; and
* a non-zero child exit reports the executable that ran, so a collision that
  survives this change is still one line of text to diagnose.
"""
from __future__ import annotations

import argparse
import importlib.metadata
import json
import shlex
import subprocess
import sys
from pathlib import Path

import pytest

import okg

from okg.substrate.cli.child_argv import running_okg_argv
from okg.substrate.cli.profile_cli_loader import get_profile_cli

pi_cli = get_profile_cli("academic-researcher").install

_DSN = "postgresql://okg:sekrit@127.0.0.1:59999/unreachable"


def _bootstrap_namespace(root: Path) -> argparse.Namespace:
    """The namespace `_provision_postgres_and_scaffold` consumes.

    ``use_local_postgres`` skips every Docker step, which leaves the scaffold
    child as the only subprocess the function spawns.
    """
    return argparse.Namespace(
        deployment_name="interpreter",
        orcid=None,
        postgres_dsn=_DSN,
        postgres_port=15435,
        use_local_postgres=True,
        mcp_port=15431,
        papers_dir=None,
        cv_path=None,
        group_webpage=None,
        pi_name=None,
        dblp_pid=None,
        git_repos=None,
        reset=False,
        yes=True,
        force=False,
        deployments_root_resolved=root,
        _invoked_from_install=True,
    )


def _scaffold_argv(recorded: list[list[str]]) -> list[str]:
    scaffold = [c for c in recorded if "--scaffold-only" in c]
    assert len(scaffold) == 1, recorded
    return scaffold[0]


def test_the_scaffold_child_runs_the_interpreter_not_a_path_lookup(
    tmp_path: Path, monkeypatch,
) -> None:
    """argv[0] is this interpreter, so PATH cannot choose the program."""
    recorded: list[list[str]] = []

    class _Done:
        returncode = 0
        stdout = ""
        stderr = ""

    def _record(cmd, **_kw):  # noqa: ANN001
        recorded.append(list(cmd))
        return _Done()

    monkeypatch.setattr(pi_cli.subprocess, "run", _record)
    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: tmp_path)

    rc = pi_cli._provision_postgres_and_scaffold(
        _bootstrap_namespace((tmp_path / "owned").resolve()), as_json=True,
    )

    assert rc == 0
    argv = _scaffold_argv(recorded)
    assert argv[0] == sys.executable, argv
    assert Path(argv[0]).name != "okg", argv
    # `-P` keeps the child's cwd off sys.path, so an `okg/` directory in
    # the repo root cannot shadow the installed package either.
    assert argv[1:4] == ["-P", "-m", "okg.substrate.cli.main"], argv
    assert argv[4] == "install", argv
    # Only the executable slot can reintroduce a PATH lookup. Later elements
    # are arguments, where the string `okg` is legitimate (a deployment name,
    # a path element), so the check targets index 0 alone.
    assert argv[0] != "okg", argv


def test_a_failing_scaffold_child_names_the_executable_that_ran(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    """rc=2 is what a PATH collision looks like; the report must identify it."""

    def _refuse(cmd, **_kw):  # noqa: ANN001
        # A stale okg is an argparse that does not know these flags, and
        # argparse echoes the WHOLE unrecognized tail back -- DSN included.
        # Anything less than the real echo would certify a redaction the
        # code does not perform.
        argv = list(cmd)
        tail = argv[argv.index("install") + 1:]
        raise subprocess.CalledProcessError(
            2, argv, output="",
            stderr=(
                "usage: okg [-h] ...\n"
                "okg: error: unrecognized arguments: " + " ".join(tail)
            ),
        )

    monkeypatch.setattr(pi_cli.subprocess, "run", _refuse)
    monkeypatch.setattr(pi_cli, "_find_repo_root", lambda: tmp_path)

    rc = pi_cli._provision_postgres_and_scaffold(
        _bootstrap_namespace((tmp_path / "owned").resolve()), as_json=True,
    )

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    message = payload["error"]
    assert "rc=2" in message
    assert sys.executable in message, message
    assert "okg.substrate.cli.main" in message, message
    assert "argv:" in message, message
    assert "--scaffold-only" in message, message
    # Neither the argv echo nor the child's own output tail may leak the
    # DSN password they carry.
    assert "sekrit" not in message, message
    assert "<redacted-dsn>" in message, message
    # The child's echo of the refused tail is in the message, redacted.
    assert "unrecognized arguments:" in message, message
    assert "--postgres-dsn <redacted-dsn>" in message, message
    # The identity half of the report is load-bearing too.
    assert f"okg package: {okg.__file__}" in message, message
    assert f"version: {importlib.metadata.version('okg')}" in message, message


def test_the_child_interpreter_report_names_the_package_and_its_version() -> None:
    """The report identifies WHICH okg, not merely that one ran."""
    report = pi_cli._child_interpreter_report()

    assert f"okg package: {okg.__file__}" in report, report
    assert f"version: {importlib.metadata.version('okg')}" in report, report
    # The `ran:` clause is derived from the argv actually used, not typed
    # out a second time.
    assert shlex.join(running_okg_argv()) in report, report


def test_an_unresolvable_version_is_reported_as_unavailable(monkeypatch) -> None:
    """A missing distribution is named with its reason, never omitted."""

    def _missing(_name: str) -> str:
        raise importlib.metadata.PackageNotFoundError("okg")

    monkeypatch.setattr(importlib.metadata, "version", _missing)

    report = pi_cli._child_interpreter_report()
    assert "version: unavailable (okg distribution not installed)" in report, report


def test_a_metadata_backend_failure_is_reported_with_its_reason(monkeypatch) -> None:
    """Any other metadata failure reports its own cause, not a blank."""

    def _explode(_name: str) -> str:
        raise RuntimeError("metadata backend offline")

    monkeypatch.setattr(importlib.metadata, "version", _explode)

    report = pi_cli._child_interpreter_report()
    assert "version: unavailable (metadata backend offline)" in report, report


def test_both_dsn_flag_spellings_and_bare_urls_are_redacted() -> None:
    """`--flag=value` is as much a password carrier as `--flag value`."""
    argv = pi_cli._redacted_argv(["--postgres-dsn=" + _DSN, "--dsn", _DSN, _DSN])
    joined = " ".join(argv)
    assert "sekrit" not in joined, argv
    assert "--postgres-dsn=<redacted-dsn>" in joined, argv
    assert "--dsn <redacted-dsn>" in joined, argv

    equals_tail = f"okg: error: unrecognized arguments: --postgres-dsn={_DSN}"
    assert "sekrit" not in pi_cli._redacted_text(equals_tail), equals_tail
    assert "sekrit" not in pi_cli._redacted_text(f"could not connect to {_DSN}")


@pytest.mark.parametrize("tail", [
    # Empty user name, password in the password slot (#2812 review).
    "psql: error: postgresql://:fake-sentinel-pw@db:5432/okg refused",
    "postgresql://u@db:5432/okg?password=fake-sentinel-pw",
    "host=db dbname=okg password=fake-sentinel-pw",
])
def test_a_dsn_password_in_a_child_tail_is_redacted_in_every_shape(tail: str) -> None:
    shown = pi_cli._redacted_text(tail)
    assert "fake-sentinel-pw" not in shown, shown
    assert "db" in shown, shown
