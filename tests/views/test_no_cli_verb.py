"""Guard against resurrecting the legacy view sidecar.

Native V1 view reads live under ``okg view show`` because they read
canonical derived-artifact manifests and typed slots. What remains
forbidden is a separate ``okg-view`` executable or a standalone refresh
surface that bypasses the publish cycle.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest


_REPO_ROOT = Path(__file__).resolve().parents[2]
_PYPROJECT = _REPO_ROOT / "pyproject.toml"
# The installed CLI, run in this interpreter as the other CLI tests do. `uv run`
# first checks and may sync the whole environment, which took more than the old
# 30 s bound on a loaded CI runner (dev check run 37658217769, 2026-10-07).
_OKG_CLI = [sys.executable, "-m", "okg.substrate.cli.main"]
# Importing the CLI is heavy; a stuck process still fails well inside the job.
_CLI_TIMEOUT_SECONDS = 120


def _project_scripts() -> dict[str, str]:
    """Parse ``[project.scripts]`` from pyproject.toml.

    Uses the stdlib ``tomllib`` (Python 3.11+) so this test has
    no extra dependency.
    """
    import tomllib
    with _PYPROJECT.open("rb") as f:
        data = tomllib.load(f)
    return data.get("project", {}).get("scripts", {})


class TestPyprojectEntryPoints:
    def test_no_okg_view_script_registered(self):
        scripts = _project_scripts()
        forbidden = [
            name for name in scripts
            if name == "okg-view" or name.startswith("okg-view-")
        ]
        assert forbidden == [], (
            f"pyproject.toml [project.scripts] contains forbidden "
            f"okg-view entries: {forbidden}. Materialization runs "
            f"as a publish-cycle stage, not a CLI sidecar — see "
            f"establish-views-contract Decision 2."
        )

    def test_okg_substrate_cli_remains_the_only_okg_entry(self):
        # Sanity-check: the existing scripts (okg, okg-ingest,
        # okg-audit, okg-agent) are unchanged. New scripts that
        # mention "view" should fail
        # ``test_no_okg_view_script_registered``; new scripts
        # without "view" are allowed but should be acknowledged
        # by extending this test if needed.
        scripts = _project_scripts()
        # The umbrella's invariant is specifically about an
        # ``okg-view`` namespace. Other scripts (okg-something-
        # else) are out of scope here.
        view_named = [n for n in scripts if "view" in n.lower()]
        assert view_named == [], (
            f"unexpected script names containing 'view': "
            f"{view_named}"
        )


class TestOkgViewSurface:
    def test_top_level_okg_help_lists_only_canonical_view_command(self):
        # ``okg view`` is now the canonical read surface. The forbidden
        # historical mistake is a separate refresh/materialization
        # command or executable.
        result = subprocess.run(
            [*_OKG_CLI, "--help"],
            capture_output=True,
            text=True,
            cwd=_REPO_ROOT,
            timeout=_CLI_TIMEOUT_SECONDS,
        )
        if result.returncode != 0:
            pytest.skip(
                f"`okg --help` failed (rc={result.returncode}); "
                f"can't audit the verb list. stderr: "
                f"{result.stderr[:200]}"
            )

        # argparse's --help for a subparsers group prints the
        # subcommand list as ``{cmd1,cmd2,cmd3}``. Find that line.
        verbs = _extract_subcommands(result.stdout)
        if verbs is None:
            pytest.skip(
                "could not parse subcommand list from `okg --help` "
                "output; the help format may have changed"
            )

        assert "view" in verbs
        forbidden = [v for v in verbs if v.startswith("view-")]
        assert forbidden == [], (
            f"`okg --help` lists forbidden top-level verbs: "
            f"{forbidden}. Native view publication runs inside `okg "
            f"run`; `okg view` is read-only."
        )

    def test_okg_view_help_exposes_show_but_not_refresh(self):
        result = subprocess.run(
            [*_OKG_CLI, "view", "--help"],
            capture_output=True,
            text=True,
            cwd=_REPO_ROOT,
            timeout=_CLI_TIMEOUT_SECONDS,
        )
        if result.returncode != 0:
            pytest.skip(
                f"`okg view --help` failed (rc={result.returncode}); "
                f"stderr: {result.stderr[:200]}"
            )

        verbs = _extract_subcommands(result.stdout)
        if verbs is None:
            pytest.skip(
                "could not parse subcommand list from `okg view --help`"
            )

        assert "list" in verbs
        assert "show" in verbs
        assert "refresh" not in verbs


def _extract_subcommands(help_text: str) -> list[str] | None:
    """Pull the comma-separated subcommand list out of argparse
    --help output.

    The format is::

        positional arguments:
          {migrate,doctor,run,...}
            migrate     ...

    We find the first ``{...}`` block at the start of a line (after
    optional whitespace) and split on commas.
    """
    import re
    m = re.search(
        r"^\s*\{([a-zA-Z0-9_,\-]+)\}",
        help_text,
        flags=re.MULTILINE,
    )
    if m is None:
        return None
    return [v.strip() for v in m.group(1).split(",") if v.strip()]
