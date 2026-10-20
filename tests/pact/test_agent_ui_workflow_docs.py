from __future__ import annotations

from pathlib import Path

from okg.substrate.pact.agent_install import install_agent_setup


ROOT = Path(__file__).resolve().parents[2]
RUNBOOK = ROOT / "docs" / "runbooks" / "agents" / "ui.md"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_agent_ui_workflow_is_the_canonical_entry_point() -> None:
    runbook = _read(RUNBOOK)
    assert "Use this runbook for any agent-driven UI work" in runbook
    assert "Product Design, Build Web Apps, Impeccable" in runbook

    expected_links = {
        "AGENTS.md": "docs/runbooks/agents/ui.md",
        "CLAUDE.md": "docs/runbooks/agents/ui.md",
        "pact/ui-change-guide.md": "docs/runbooks/agents/ui.md",
        "pact/templates/ui-change/README.md": "docs/runbooks/agents/ui.md",
        "pact/templates/ui-change/pact.yaml": "docs/runbooks/agents/ui.md",
    }
    for rel, expected_link in expected_links.items():
        assert expected_link in _read(ROOT / rel), rel


def test_installed_agent_guidance_routes_ui_work_to_runbook(tmp_path: Path) -> None:
    for target, filename in [
        ("codex", "AGENTS.md"),
        ("claude-code", "CLAUDE.md"),
    ]:
        install_agent_setup(target, tmp_path / target)
        text = _read(tmp_path / target / filename)
        assert "docs/runbooks/agents/ui.md" in text
        assert "Product Design, Build Web Apps, Impeccable" not in text


def test_agent_ui_workflow_preserves_visual_gates() -> None:
    runbook = _read(RUNBOOK)
    for phrase in [
        "pin reference",
        "Build Static First",
        "Get Static Visual Approval",
        "Refinement Loop",
        "keep/change/remove",
        "next-pass-focus",
        "The agent cannot self-approve",
        "Wire Live Data",
        "Browser Proof",
        "Automated UI Gate",
        "single-app gate",
        "Live Sign-Off",
        "okg pact check",
    ]:
        assert phrase in runbook


def test_agent_ui_workflow_routes_tools_under_okg_design_authority() -> None:
    runbook = _read(RUNBOOK)
    for phrase in [
        "docs/develop/ui/operator-contract.md",
        "git show 3253beb069ba:archive/ui/legacy-viz-2026-06-17/INDEX.md",
        "do not code against archived routes or bundles",
        "Browser first in Codex",
        "Product Design or Build Web Apps only for green-field",
        "Repo design docs outrank generic frontend skills",
        "A passing gate is mechanical proof only",
    ]:
        assert phrase in runbook


def test_agent_ui_workflow_defines_taste_and_viz_contracts() -> None:
    runbook = _read(RUNBOOK)
    for phrase in [
        "Taste Guardrails",
        "no generic SaaS dashboard styling",
        "no decorative gradients",
        "loud failures for missing graph data",
        "Viz Semantic Contract",
        "generation, source, subtype, and provenance",
        "omitted, filtered, sampled, clustered, or deferred",
    ]:
        assert phrase in runbook


def test_ui_template_requires_quality_gate_and_feedback_artifacts() -> None:
    guide = _read(ROOT / "pact" / "ui-change-guide.md")
    template_readme = _read(ROOT / "pact" / "templates" / "ui-change" / "README.md")
    template_yaml = _read(ROOT / "pact" / "templates" / "ui-change" / "pact.yaml")

    for text in [guide, template_readme, template_yaml]:
        assert "UI quality gate" in text or "ui_quality_gate" in text

    assert "ui_quality_gate" in guide
    assert "ui_quality_gate" in template_yaml
    assert "ui/feedback/" in template_readme
    assert "keep/change/remove" in template_readme


def test_active_ui_guidance_uses_current_evidence_command_names() -> None:
    active_docs = [
        RUNBOOK,
        ROOT / "pact" / "ui-change-guide.md",
        ROOT / "pact" / "templates" / "ui-change" / "README.md",
        ROOT / "pact" / "templates" / "ui-change" / "pact.yaml",
        ROOT / "docs" / "develop" / "ui" / "operator-contract.md",
    ]
    for path in active_docs:
        text = _read(path)
        assert "okg pact attach-evidence" not in text, str(path)
        assert "okg pact evidence approve" not in text, str(path)
        assert "gate pre-implementation" not in text, str(path)
