"""Smoke tests for the academic-researcher CLI subcommands.

The CLIs live in `library/profiles/academic-researcher/cli/` (per
`retire-pi-prefix-commands`) and are discovered by the substrate
via `cli/profile_cli_loader.py`. These tests verify:

1. Each subcommand registers with the main parser.
2. The Claude Desktop config writer merges into an existing
   config without clobbering pre-existing entries.
3. The platform-aware config-path detector returns a Path on
   the host platform (we don't test cross-platform paths).
4. The `pi-confirm` CLI in default mode reports
   "no candidates pending" when no extracted_model rows exist
   (i.e., when no deployment data is available — exercise
   the empty-state path).

End-to-end smoke (run okg pi-bootstrap against a real machine +
ORCID) lives in `tests/integration/` and is OUT of this file's
scope — those tests need Docker + network and live on the
integration suite.
"""
from __future__ import annotations

from pathlib import Path


from okg.substrate.cli.main import _build_parser
from okg.substrate.cli.profile_cli_loader import get_profile_cli

pi_cli = get_profile_cli("academic-researcher").install


def test_parser_registers_operator_facing_subcommands() -> None:
    """The four operator-facing verbs SHALL appear in the
    academic-researcher profile CLI's DISPATCH. Per
    `cleanup-academic-pi-cli-surface` + `retire-pi-prefix-commands`:
    the deprecated `pi-*` verbs are gone; `install` / `add` /
    `remove` / `reset` are the canonical surface.
    """
    _build_parser()
    for cmd in ("add", "remove", "reset"):
        assert cmd in pi_cli.DISPATCH, (
            f"command {cmd!r} missing from academic-researcher DISPATCH"
        )
    # `install` is the exception: per PACT `define-newcomer-entry-path` it
    # is the one documented way in, so the SUBSTRATE owns the verb and
    # routes back here via INSTALL_EXTENSION. It stays operator-facing —
    # it just is not profile-registered.
    assert "install" not in pi_cli.DISPATCH
    assert hasattr(pi_cli, "INSTALL_EXTENSION")
    # Deprecated verbs SHALL be gone.
    for retired in (
        "pi-bootstrap", "pi-doctor", "pi-confirm",
        "pi-write-claude-config",
    ):
        assert retired not in pi_cli.DISPATCH, (
            f"retired verb {retired!r} still in DISPATCH"
        )


# Legacy `_write_claude_desktop_config` /
# `_build_claude_desktop_entry` / `_claude_desktop_config_path` /
# `_resolve_okg_binary` were deleted by `dead-simple-install`. Their
# behavior is covered by the agent-runtime registry's
# ClaudeDesktopRuntime + its tests at
# `tests/substrate/library/agent_runtimes/test_agent_runtimes.py`.


def test_compose_yaml_ships_in_profile() -> None:
    """The shipped docker compose file lives at the expected path
    (referenced from `okg pi-bootstrap`).
    """
    # `install.py` now lives at
    # `library/profiles/academic-researcher/cli/install.py`; compose.yaml is
    # a sibling of `cli/`.
    here = Path(pi_cli.__file__).resolve()
    compose_path = here.parent.parent / "compose.yaml"
    assert compose_path.is_file(), (
        f"compose.yaml missing at {compose_path}"
    )
    # Compose file references the expected env vars.
    text = compose_path.read_text(encoding="utf-8")
    assert "${DEPLOYMENT_NAME}" in text
    assert "${POSTGRES_PORT" in text
    assert "timescale" in text.lower()

