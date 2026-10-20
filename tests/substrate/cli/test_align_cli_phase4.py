"""Phase 4 — pi-* deprecation warnings + `okg install --claude-config-only`.

Per `align-substrate-cli-docs-and-runbooks` Phase 4:
- `okg pi-bootstrap`, `okg pi-doctor`, `okg pi-write-claude-config`
  emit a deprecation warning naming the replacement.
- `okg install --claude-config-only --deployment <name>` re-writes
  just the Claude Desktop config (no Postgres provisioning).
- When `_cmd_install` calls the deprecated handlers internally,
  the warnings are suppressed via the `_invoked_from_install`
  marker.
"""
from __future__ import annotations

import argparse
from pathlib import Path
from unittest import mock


from okg.substrate.cli.profile_cli_loader import get_profile_cli

pi_cli = get_profile_cli("academic-researcher").install


def test_install_parser_accepts_claude_config_only() -> None:
    """The install subparser SHALL accept `--claude-config-only`.

    Built through the profile's standalone install-parser builder:
    `build_parsers` no longer registers `install`, because the substrate
    owns that verb (PACT `define-newcomer-entry-path`).
    """
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd")
    pi_cli._build_install_parser(sub)
    args = parser.parse_args([
        "install", "--orcid", "0000-0002-1234-5678",
        "--name", "test_pi", "--claude-config-only",
    ])
    assert args.claude_config_only is True


def test_install_claude_config_only_short_circuits(
    tmp_path: Path,
) -> None:
    """`--claude-config-only` SHALL skip bootstrap + publish and
    re-register with every detected agent runtime.

    Per `fix-okg-install-claude-config`: reads `init_answers.
    postgres_dsn` from `deployment.yaml` for the DSN. Per
    `dead-simple-install`: re-registration covers Claude
    Desktop, codex, Cursor — not just Claude Desktop.
    """
    import yaml
    repo_root = tmp_path
    dep_dir = repo_root / "deployments/test_pi"
    dep_dir.mkdir(parents=True)
    (dep_dir / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "test_pi",
        "profile": {"name": "academic-researcher", "init_answers": {
            "postgres_dsn": "postgres://postgres:okg@localhost:5440/okg_test_pi",
        }},
    }))

    args = argparse.Namespace(
        orcid="0000-0002-1234-5678",
        name="test_pi",
        postgres_port=None,
        mcp_port=5431,
        no_claude_config=False,
        claude_config_only=True,
    )
    from okg.substrate.library.agent_runtimes import RegisterReport
    with (
        mock.patch.object(
            pi_cli, "_find_repo_root",
            return_value=repo_root,
        ),
        mock.patch(
            "okg.substrate.library.agent_runtimes.registry."
            "register_with_all_detected",
            return_value=RegisterReport(),
        ) as fake_register,
        mock.patch.object(
            pi_cli, "_provision_postgres_and_scaffold",
        ) as fake_bootstrap,
    ):
        rc = pi_cli._cmd_install(args, as_json=True)
    assert rc == 0
    # Bootstrap NOT called when --claude-config-only is set.
    fake_bootstrap.assert_not_called()
    # Multi-runtime registration IS called with the DSN read
    # from deployment.yaml.
    fake_register.assert_called_once()
    _args, kwargs = fake_register.call_args
    assert kwargs["dsn"] == (
        "postgres://postgres:okg@localhost:5440/okg_test_pi"
    )
    assert kwargs["deployment"] == "test_pi"


def test_install_claude_config_only_fails_when_deployment_missing(
    tmp_path: Path,
) -> None:
    """If the deployment dir doesn't exist, the short-circuit
    SHALL fail with a fix-hint pointing at the full install verb.
    """
    args = argparse.Namespace(
        orcid="0000-0002-1234-5678",
        name="nonexistent_pi",
        postgres_port=None,
        mcp_port=5431,
        no_claude_config=False,
        claude_config_only=True,
    )
    with mock.patch.object(
        pi_cli, "_find_repo_root",
        return_value=tmp_path,
    ):
        rc = pi_cli._cmd_install(args, as_json=True)
    assert rc == 1


def test_deprecated_pi_verbs_removed() -> None:
    """Per `cleanup-academic-pi-cli-surface` +
    `retire-pi-prefix-commands`: the deprecated `pi-*` verbs are
    GONE from the academic-researcher DISPATCH.

    Per `dead-simple-install`: the inline Claude Desktop writer
    (`_write_claude_desktop_config`) is now also gone — its
    behavior moved into `agent_runtimes/claude_desktop.py`'s
    `ClaudeDesktopRuntime`. Only `_provision_postgres_and_scaffold`
    survives as a private helper.
    """
    for retired in (
        "pi-bootstrap", "pi-doctor", "pi-confirm",
        "pi-write-claude-config",
    ):
        assert retired not in pi_cli.DISPATCH
    # Private helpers remain (renamed).
    assert hasattr(pi_cli, "_provision_postgres_and_scaffold")
    # _write_claude_desktop_config + friends were deleted in
    # `dead-simple-install`.
    assert not hasattr(pi_cli, "_write_claude_desktop_config")
    assert not hasattr(pi_cli, "_build_claude_desktop_entry")
    assert not hasattr(pi_cli, "_claude_desktop_config_path")
    # _cmd_pi_doctor and _cmd_pi_confirm are gone.
    assert not hasattr(pi_cli, "_cmd_pi_doctor")
    assert not hasattr(pi_cli, "_cmd_pi_confirm")
