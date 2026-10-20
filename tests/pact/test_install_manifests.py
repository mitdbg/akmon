"""ship-harness-integration-pack: per-harness manifests register
the OKG MCP server + the single skills bundle — pointers, never
copied skill bodies in the manifests."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.substrate.pact.agent_install import (
    McpServerSpec,
    bundled_skills,
    install_agent_setup,
)

_SPEC = McpServerSpec(
    name="okg-workspace",
    command="python",
    args=["deployments/okg-workspace/server.py"],
    env={"OKG_DSN": "${OKG_DSN}"},
)


def test_generated_manifests_validate(tmp_path: Path) -> None:
    """All three harness targets produce parseable manifests whose
    server entry round-trips command/args/env."""
    claude = install_agent_setup("claude-code", tmp_path, mcp=_SPEC)
    assert claude.mcp_manifest == ".mcp.json"
    mcp_json = json.loads((tmp_path / ".mcp.json").read_text())
    entry = mcp_json["mcpServers"]["okg-workspace"]
    assert entry["command"] == "python"
    assert entry["args"] == ["deployments/okg-workspace/server.py"]
    assert entry["env"]["OKG_DSN"] == "${OKG_DSN}"

    codex = install_agent_setup("codex", tmp_path, mcp=_SPEC)
    toml_path = tmp_path / codex.mcp_manifest
    toml_text = toml_path.read_text()
    import tomllib
    parsed = tomllib.loads(
        "\n".join(
            line for line in toml_text.splitlines()
            if not line.startswith("#")
        ),
    )
    assert parsed["mcp_servers"]["okg-workspace"]["command"] == "python"

    gemini = install_agent_setup("gemini", tmp_path, mcp=_SPEC)
    ext = json.loads((tmp_path / gemini.mcp_manifest).read_text())
    assert ext["name"] == "okg"
    assert ext["mcpServers"]["okg-workspace"]["command"] == "python"
    assert ext["contextFileName"] == "GEMINI.md"
    assert (tmp_path / ".gemini" / "extensions" / "okg" / "GEMINI.md").is_file()
    # Gemini reads the cross-tool skills dir; instruction file at root.
    assert (tmp_path / "GEMINI.md").is_file()


def test_claude_mcp_json_merge_preserves_other_servers(tmp_path: Path) -> None:
    (tmp_path / ".mcp.json").write_text(json.dumps({
        "mcpServers": {"other": {"command": "x", "args": []}},
    }))
    install_agent_setup("claude-code", tmp_path, mcp=_SPEC)
    merged = json.loads((tmp_path / ".mcp.json").read_text())
    assert set(merged["mcpServers"]) == {"other", "okg-workspace"}


def test_manifests_reference_not_copy_skills(tmp_path: Path) -> None:
    """The one-source-of-truth contract: manifests carry pointers
    (commands, paths), never skill bodies; the skill FILES live in
    exactly the per-harness skills dirs."""
    for agent in ("claude-code", "codex", "gemini"):
        install_agent_setup(agent, tmp_path, mcp=_SPEC)
    skill_bodies = [
        (s / "SKILL.md").read_text(encoding="utf-8").splitlines()[5]
        for s in bundled_skills()
    ]
    for manifest in (
        tmp_path / ".mcp.json",
        tmp_path / ".agents" / "okg-mcp.config.toml",
        tmp_path / ".gemini" / "extensions" / "okg" / "gemini-extension.json",
    ):
        text = manifest.read_text(encoding="utf-8")
        for body_line in skill_bodies:
            if body_line.strip():
                assert body_line not in text, (
                    f"{manifest} embeds skill content"
                )
    # Skills land once per harness-visible dir, identical content.
    claude_skill = tmp_path / ".claude" / "skills" / "okg-query" / "SKILL.md"
    shared_skill = tmp_path / ".agents" / "skills" / "okg-query" / "SKILL.md"
    assert claude_skill.read_text() == shared_skill.read_text()


def test_install_without_mcp_unchanged(tmp_path: Path) -> None:
    """The MCP manifest is opt-in: a plain install writes no
    manifest and reports mcp_manifest=None (back-compat)."""
    result = install_agent_setup("claude-code", tmp_path)
    assert result.mcp_manifest is None
    assert not (tmp_path / ".mcp.json").exists()


def test_unknown_agent_still_raises(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unknown --agent"):
        install_agent_setup("openclaw", tmp_path)


@pytest.mark.parametrize(
    "spec, message",
    [
        (
            dict(command="python", env={"OKG_DSN": "postgresql://user:pass@db/okg"}),
            "environment references",
        ),
        (
            dict(command="ssh", args=["-i", "/Users/alice/.ssh/id_ed25519"]),
            "absolute paths",
        ),
        (
            dict(command="ssh", args=["agent@192.0.2.10"]),
            "SSH or private-key commands",
        ),
        (
            dict(command="python", args=["--key", "/opt/keys/service.pem"]),
            "absolute paths",
        ),
        (
            dict(command="ssh", args=["agent@builder.example.org"]),
            "SSH or private-key commands",
        ),
        (
            dict(command="python", args=["--dsn=postgresql://user:pass@db/okg"]),
            "credential values in commands",
        ),
        (
            dict(command="python", args=["--host", "builder.example.org"]),
            "environment references",
        ),
        (
            dict(command="python", args=["--key", ".ssh/id_ed25519"]),
            "private-key paths",
        ),
        (
            dict(command="python", args=["builder.example.org"]),
            "fixed hostname",
        ),
        (
            dict(command="python", args=["--password", "literal"]),
            "environment references",
        ),
    ],
)
def test_mcp_spec_rejects_tracked_machine_authority(
    spec: dict[str, object], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        McpServerSpec(name="unsafe", **spec)


def test_mcp_spec_allows_referenced_secrets_and_relative_scripts() -> None:
    spec = McpServerSpec(
        name="portable",
        command="python",
        args=["server.py", "--password", "${MCP_PASSWORD}"],
    )

    assert spec.args == ["server.py", "--password", "${MCP_PASSWORD}"]
