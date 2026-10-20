from __future__ import annotations

from pathlib import Path

from tests.mirror_scope import private_path

# The shipped quickstart (docs/runbooks/workspace-graph/setup.md) is checked on
# its own; the design reference (deployments/okg-workspace/canonical-dev-okg.md)
# is this repository's own deployment and is checked in separate tests, which
# the public copy skips with a reason.


def _design_text() -> str:
    return private_path("deployments/okg-workspace/canonical-dev-okg.md").read_text(encoding="utf-8")


def _setup_text() -> str:
    repo = Path(__file__).resolve().parents[2]
    return (repo / "docs" / "runbooks" / "workspace-graph" / "setup.md").read_text(
        encoding="utf-8"
    )


def test_workspace_graph_design_defines_scope_and_names() -> None:
    setup = _setup_text()
    normalized = " ".join(setup.lower().split())

    assert "# Workspace Graph Quickstart" in setup
    assert "one git-backed software workspace per deployment" in setup
    assert "code, docs, tests, change records, git history, agent sessions" in setup
    assert "create one deployment per git-backed workspace" in setup
    assert "every arbitrary folder" in normalized
    assert "does not index your whole machine" in normalized
    for placeholder in ("<deployment>", "<repo-root>", "<db-name>", "<mcp-port>"):
        assert placeholder in setup


def test_workspace_graph_design_reference_defines_the_deployment() -> None:
    reference = _design_text()

    assert "# Canonical dev OKG" in reference
    assert "The `okg-workspace` deployment projects this repository" in reference
    assert "okg-workspace" in reference


def _assert_no_migration_narration(text: str) -> None:
    assert "## Migration Strategy" not in text
    assert "## PACT Dogfood Observation" not in text
    assert "next PACT improvement question" not in text


def _assert_canonical_names_only(text: str) -> None:
    assert "okg-workspace-qa" not in text
    assert "`okg-self`" not in text
    assert "Prefer the `okg-workspace` deployment alias" not in text
    assert "compatibility alias during the staged migration" not in text


def test_workspace_graph_reference_omits_completed_migration_narration() -> None:
    _assert_no_migration_narration(_setup_text())


def test_workspace_graph_design_reference_omits_completed_migration_narration() -> None:
    _assert_no_migration_narration(_design_text())


def test_workspace_graph_current_guidance_prefers_canonical_names() -> None:
    setup = _setup_text()

    assert "Workspace Graph" in setup
    assert "okg install" in setup
    assert "--profile workspace-graph" in setup
    _assert_canonical_names_only(setup)


def test_workspace_graph_design_reference_prefers_canonical_names() -> None:
    _assert_canonical_names_only(_design_text())
