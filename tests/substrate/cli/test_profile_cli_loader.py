"""Discovery hook for profile-shipped CLIs.

Per `retire-pi-prefix-commands` Phase 1: substrate's `cli/main.py`
walks `library/profiles/*/cli/__init__.py` and merges each
profile's `register_subparsers` + `DISPATCH`. This test exercises
the loader directly.
"""
from __future__ import annotations

import argparse
import textwrap
from pathlib import Path

import pytest

from okg.substrate.cli.profile_cli_loader import (
    ProfileCliError,
    discover_profile_clis,
    get_profile_cli,
    register_all_profile_clis,
)


# --- discovery surface ---


def test_discover_finds_academic_researcher() -> None:
    """The shipped academic-researcher profile's CLI is found via discovery."""
    clis = discover_profile_clis()
    assert "academic-researcher" in clis
    pi_cli = clis["academic-researcher"]
    assert hasattr(pi_cli, "register_subparsers")
    assert hasattr(pi_cli, "DISPATCH")
    # The build_parsers alias is also exposed for back-compat.
    assert hasattr(pi_cli, "build_parsers")


def test_get_profile_cli_academic_researcher() -> None:
    """`get_profile_cli("academic-researcher")` returns the loaded module.

    `install` is deliberately NOT in DISPATCH: per PACT
    `define-newcomer-entry-path` the substrate owns that verb and reaches
    the profile through `INSTALL_EXTENSION`, so the profile no longer
    registers it.
    """
    pi_cli = get_profile_cli("academic-researcher")
    assert "add" in pi_cli.DISPATCH
    assert "install" not in pi_cli.DISPATCH
    assert hasattr(pi_cli, "INSTALL_EXTENSION")


def test_get_profile_cli_nonexistent_raises() -> None:
    """Asking for a profile that doesn't exist raises a structured
    error.
    """
    with pytest.raises(ProfileCliError) as exc_info:
        get_profile_cli("nonexistent-profile-xyz")
    assert "profile not found" in str(exc_info.value)


# --- register_all_profile_clis: collision detection ---


def test_register_all_refuses_collision_with_substrate_verb(
    tmp_path: Path, monkeypatch,
) -> None:
    """If a profile's DISPATCH contains a verb already shipped by
    the substrate, `register_all_profile_clis` SHALL raise a
    structured error naming the verb.
    """
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd")
    # `add` rather than `install`: the substrate now owns `install`, so the
    # profile no longer registers it and it can no longer collide. `add` is
    # still profile-owned, which is what makes it the right probe for the
    # guard itself.
    with pytest.raises(ProfileCliError) as exc_info:
        register_all_profile_clis(
            sub, substrate_verbs={"add"},
        )
    msg = str(exc_info.value)
    assert "add" in msg
    assert "academic-researcher" in msg


def test_register_all_refuses_collision_between_profiles(
    tmp_path: Path, monkeypatch,
) -> None:
    """Two profiles registering the same verb SHALL raise a
    structured error naming both profiles.

    We synthesize two fake profile dirs in tmp_path and re-point
    the loader at them via monkeypatch.
    """
    fake_root = tmp_path / "library" / "profiles"
    for name in ("alpha", "beta"):
        cli_dir = fake_root / name / "cli"
        cli_dir.mkdir(parents=True)
        (cli_dir / "__init__.py").write_text(textwrap.dedent("""
            import argparse

            def register_subparsers(sub):
                sub.add_parser("collide-verb")

            DISPATCH = {"collide-verb": lambda args, *, as_json: 0}
        """))

    monkeypatch.setattr(
        "okg.substrate.cli.profile_cli_loader._PROFILE_ROOT",
        fake_root,
    )
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd")
    with pytest.raises(ProfileCliError) as exc_info:
        register_all_profile_clis(sub)
    msg = str(exc_info.value)
    assert "collide-verb" in msg
    assert "alpha" in msg
    assert "beta" in msg


def test_register_all_silently_skips_profile_without_cli(
    tmp_path: Path, monkeypatch,
) -> None:
    """A profile dir that has no `cli/__init__.py` is silently
    skipped — not every profile ships CLI.
    """
    fake_root = tmp_path / "library" / "profiles"
    (fake_root / "no-cli-profile").mkdir(parents=True)
    # No cli/__init__.py — just an otherwise-bare profile dir.

    monkeypatch.setattr(
        "okg.substrate.cli.profile_cli_loader._PROFILE_ROOT",
        fake_root,
    )
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd")
    # No raise; empty dispatch.
    dispatch = register_all_profile_clis(sub)
    assert dispatch == {}


def test_register_all_refuses_profile_missing_dispatch(
    tmp_path: Path, monkeypatch,
) -> None:
    """A profile's `cli/__init__.py` without a DISPATCH export
    SHALL be refused with a structured error.
    """
    fake_root = tmp_path / "library" / "profiles"
    cli_dir = fake_root / "broken-profile" / "cli"
    cli_dir.mkdir(parents=True)
    (cli_dir / "__init__.py").write_text(textwrap.dedent("""
        def register_subparsers(sub):
            pass
        # DISPATCH intentionally missing
    """))

    monkeypatch.setattr(
        "okg.substrate.cli.profile_cli_loader._PROFILE_ROOT",
        fake_root,
    )
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd")
    with pytest.raises(ProfileCliError) as exc_info:
        register_all_profile_clis(sub)
    assert "DISPATCH" in str(exc_info.value)
    assert "broken-profile" in str(exc_info.value)
