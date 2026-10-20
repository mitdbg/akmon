"""Tests for the dead-simple-install additions to academic-researcher's
install.py: smart defaults derivation, handoff line composition,
and `_parse_compose_port`.

Strategy: pure-function tests where possible. The full install
flow remains exercised by test_install_command.py + a live
verify in Phase 7.
"""
from __future__ import annotations

from pathlib import Path
from unittest import mock

from okg.substrate.cli.profile_cli_loader import get_profile_cli
from okg.substrate.library.agent_runtimes import (
    RegisterReport, RegisterResult,
)

_pi_cli_pkg = get_profile_cli("academic-researcher")
pi_cli = _pi_cli_pkg.install


# ---------- _parse_compose_port ----------


def test_parse_compose_port_handles_ipv4_only() -> None:
    out = "0.0.0.0:32773\n"
    assert pi_cli._parse_compose_port(out) == 32773


def test_parse_compose_port_handles_ipv4_and_ipv6() -> None:
    out = "0.0.0.0:32773\n[::]:32773\n"
    assert pi_cli._parse_compose_port(out) == 32773


def test_parse_compose_port_returns_none_on_unparseable() -> None:
    assert pi_cli._parse_compose_port("") is None
    assert pi_cli._parse_compose_port("garbage\n") is None


# ---------- _derive_name_from_random ----------


def test_derive_name_from_random_picks_from_roster() -> None:
    """Every call SHALL return a name from `SCIENTIST_ROSTER`.
    Pick a few times to surface any out-of-band returns."""
    roster = set(pi_cli.SCIENTIST_ROSTER)
    for _ in range(20):
        name = pi_cli._derive_name_from_random()
        assert name in roster, (
            f"_derive_name_from_random returned {name!r} not in "
            f"SCIENTIST_ROSTER"
        )


def test_scientist_roster_is_well_formed() -> None:
    """Every entry SHALL be lowercase, ASCII, slug-safe (only
    `[a-z0-9_-]`), and unique. These are constraints from
    Docker project naming + MCP server key naming."""
    seen = set()
    for name in pi_cli.SCIENTIST_ROSTER:
        assert name == name.lower(), f"{name!r} not lowercase"
        assert name.isascii(), f"{name!r} not ASCII"
        assert all(
            c.isalnum() or c in "-_" for c in name
        ), f"{name!r} contains non-slug character"
        assert name not in seen, f"{name!r} duplicated in roster"
        seen.add(name)
    # Sanity: roster is meaningfully diverse.
    assert len(pi_cli.SCIENTIST_ROSTER) >= 40


def test_derive_name_from_random_falls_back_to_hostname_if_roster_empty() -> None:
    """Defensive: if the roster is somehow empty,
    `_derive_name_from_random` SHALL fall back to the hostname
    derivation rather than raise."""
    with mock.patch.object(pi_cli, "SCIENTIST_ROSTER", ()):
        with mock.patch(
            "socket.gethostname", return_value="dogfood",
        ):
            assert pi_cli._derive_name_from_random() == "dogfood"


# ---------- _derive_name_from_hostname ----------


def test_derive_name_from_hostname_sanitizes_dots() -> None:
    with mock.patch(
        "socket.gethostname",
        return_value="Rileys-MacBook-Pro.local",
    ):
        name = pi_cli._derive_name_from_hostname()
    assert name == "rileys-macbook-pro"


def test_derive_name_from_hostname_falls_back_to_okg() -> None:
    """If hostname is empty or all-non-alphanum, default to
    a safe `okg` slug rather than emitting an empty string."""
    with mock.patch("socket.gethostname", return_value=""):
        assert pi_cli._derive_name_from_hostname() == "okg"


def test_derive_name_from_hostname_strips_mdns_suffixes() -> None:
    """`.local`, `.lan`, `.home` are mDNS-style suffixes from
    Bonjour/avahi; they shouldn't leak into the slug."""
    with mock.patch(
        "socket.gethostname", return_value="dogfood.lan",
    ):
        assert pi_cli._derive_name_from_hostname() == "dogfood"


# ---------- _derive_pi_name_from_git ----------


def test_derive_pi_name_from_git_returns_value_when_set() -> None:
    fake_proc = mock.MagicMock()
    fake_proc.stdout = b"Marie Curie\n"
    with (
        mock.patch.object(
            pi_cli.shutil, "which", return_value="/usr/bin/git",
        ),
        mock.patch.object(
            pi_cli.subprocess, "run", return_value=fake_proc,
        ),
    ):
        assert pi_cli._derive_pi_name_from_git() == "Marie Curie"


def test_derive_pi_name_from_git_rejects_one_token_name() -> None:
    fake_proc = mock.MagicMock()
    fake_proc.stdout = b"Riley\n"
    with (
        mock.patch.object(
            pi_cli.shutil, "which", return_value="/usr/bin/git",
        ),
        mock.patch.object(
            pi_cli.subprocess, "run", return_value=fake_proc,
        ),
    ):
        assert pi_cli._derive_pi_name_from_git() is None


def test_derive_pi_name_from_git_returns_none_when_git_missing() -> None:
    with mock.patch.object(
        pi_cli.shutil, "which", return_value=None,
    ):
        assert pi_cli._derive_pi_name_from_git() is None


def test_derive_pi_name_from_git_returns_none_when_unset() -> None:
    """If `git config` exits non-zero (key not set), the helper
    SHALL return None — NOT empty string."""
    import subprocess as _sp

    def fake_run(*a, **kw):
        raise _sp.CalledProcessError(
            returncode=1, cmd="git", output=b"", stderr=b"",
        )

    with (
        mock.patch.object(
            pi_cli.shutil, "which", return_value="/usr/bin/git",
        ),
        mock.patch.object(
            pi_cli.subprocess, "run", side_effect=fake_run,
        ),
    ):
        assert pi_cli._derive_pi_name_from_git() is None


# ---------- _build_handoff_line ----------


def test_handoff_line_names_single_primary_agent() -> None:
    report = RegisterReport(results=[
        RegisterResult(
            runtime="claude-desktop", status="registered",
            config_path=Path("/x"), config_mtime=1.0,
        ),
    ])
    line = pi_cli._build_handoff_line(
        deployment_name="dogfood5", register_report=report,
    )
    assert "claude-desktop" in line
    assert "dogfood5" in line
    assert "set up my OKG" in line
    assert "(or " not in line


def test_handoff_line_names_primary_and_one_secondary() -> None:
    """When two runtimes registered, name primary + `(or
    <secondary>)`. Primary is most-recently-modified."""
    report = RegisterReport(results=[
        RegisterResult(
            runtime="claude-desktop", status="registered",
            config_path=Path("/x"), config_mtime=1.0,
        ),
        RegisterResult(
            runtime="codex", status="registered",
            config_path=Path("/y"), config_mtime=999.0,
        ),
    ])
    line = pi_cli._build_handoff_line(
        deployment_name="dogfood5", register_report=report,
    )
    assert "codex" in line  # primary (more recent)
    assert "claude-desktop" in line  # secondary
    assert "(or claude-desktop)" in line


def test_handoff_line_when_no_runtimes_registered() -> None:
    """When no runtimes are detected, the handoff SHALL tell
    the operator to install one + re-run."""
    report = RegisterReport(results=[
        RegisterResult(
            runtime="claude-desktop", status="not_detected",
            config_path=Path("/x"),
        ),
    ])
    line = pi_cli._build_handoff_line(
        deployment_name="dogfood5", register_report=report,
    )
    assert "No agent runtimes" in line
    assert "claude-config-only" in line


def test_handoff_line_when_report_is_none() -> None:
    """`--no-claude-config` produces register_report=None. The
    handoff SHALL still give the operator something actionable."""
    line = pi_cli._build_handoff_line(
        deployment_name="dogfood5", register_report=None,
    )
    assert "dogfood5" in line
    assert "set up my OKG" in line
