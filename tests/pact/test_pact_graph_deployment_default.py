"""PACT's graph surfaces and installed text name nothing a public copy lacks.

``okg pact view --format=bootstrap`` and the graph-projection status used to
fall back to the development repository's own workspace deployment. They now
read the repository's configured PACT graph deployment
(``OKG_PACT_GRAPH_DEPLOYMENT``, else ``graph_projection.deployment``) and say
plainly when none is configured. The installed agent instructions and the
shipped skills point only at files that ship (#2823).
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

from okg.substrate.cli.main import main
from okg.substrate.pact import cli as pact_cli
from okg.substrate.pact.agent_install import (
    bundled_skills,
    instruction_section,
)
from okg.substrate.pact.store import save_manifest
from tests.pact.test_bootstrap_dsn_authority import _manifest
from tests.pact.test_cli import _seed_root

BUILT_IN = "okg-" + "workspace"


@pytest.fixture
def unconfigured(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.delenv("OKG_PACT_GRAPH_DEPLOYMENT", raising=False)
    monkeypatch.delenv("OKG_DEPLOYMENT", raising=False)
    root = _seed_root(tmp_path)
    save_manifest(root, _manifest("demo"))
    monkeypatch.chdir(root)
    return root


def _bootstrap(root: Path, capsys: pytest.CaptureFixture[str]) -> dict:
    rc = main([
        "pact", "--root", str(root), "view", "demo",
        "--format=bootstrap", "--full", "--json",
    ])
    out = capsys.readouterr().out
    assert rc in (0, 1), out
    return json.loads(out)


def test_bootstrap_without_a_configured_deployment_says_so(
    unconfigured: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def no_probe(*_args, **_kwargs):
        raise AssertionError("nothing may be probed without a deployment")

    monkeypatch.setattr(pact_cli, "_bootstrap_readiness", no_probe)
    monkeypatch.setattr(pact_cli, "_bootstrap_catalog_ownership", no_probe)

    payload = _bootstrap(unconfigured, capsys)

    text = json.dumps(payload)
    assert BUILT_IN not in text
    assert "no PACT graph deployment is configured" in text
    assert "configure_pact_graph_deployment" in text
    assert "graph_projection.deployment" in text


def test_bootstrap_uses_the_repository_configured_deployment(
    unconfigured: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = unconfigured / ".okg" / "coordination.yaml"
    config.parent.mkdir()
    config.write_text("graph_projection:\n  deployment: acme-graph\n", encoding="utf-8")
    probed: list[str] = []

    def readiness(_root, *, deployment):
        probed.append(deployment)
        return pact_cli._bootstrap_unconfigured_readiness("probe stubbed")

    def ownership(_root, *, deployment_path):
        probed.append(deployment_path)
        return {"ok": None, "recognized": False, "status": "unknown"}

    monkeypatch.setattr(pact_cli, "_bootstrap_readiness", readiness)
    monkeypatch.setattr(pact_cli, "_bootstrap_catalog_ownership", ownership)

    payload = _bootstrap(unconfigured, capsys)

    assert probed == ["acme-graph", "acme-graph"]
    text = json.dumps(payload)
    assert "--deployment acme-graph" in text
    assert BUILT_IN not in text


def test_graph_projection_status_reports_an_unconfigured_deployment(
    unconfigured: Path,
) -> None:
    manifest = pact_cli._load_manifest_for_gate(unconfigured, "demo")

    status = pact_cli._graph_projection_status(manifest, unconfigured)

    assert status["status"] == "unavailable"
    assert status["deployment"] is None
    assert status["next_action"] == "configure_pact_graph_deployment"
    assert "graph_projection.deployment" in status["reason"]
    assert pact_cli._graph_projection_refresh_commands("demo") == [
        "uv run okg pact gate graph-freshness demo --refresh --json",
        "uv run okg pact view demo --format=status --json",
    ]


def test_graph_refresh_refuses_without_a_deployment(unconfigured: Path) -> None:
    from okg.substrate.errors import SubstrateError

    with pytest.raises(SubstrateError) as raised:
        pact_cli._sync_pact_projection(
            unconfigured, "demo", deployment=None, dry_run=True
        )
    assert raised.value.data["cause"] == "deployment_not_configured"
    assert "--deployment" in (raised.value.fix_hint or "")


#: Development-repository files the public copy does not ship.
_PRIVATE_REFERENCE = re.compile(
    r"docs/runbooks/agents|scripts/ci/|scripts/check_pr_body\.py|"
    r"deployments/okg-workspace|history/openspec|\.github/|pact/project\.md|"
    + re.escape(BUILT_IN)
)


def test_install_text_names_only_files_the_destination_has(tmp_path: Path) -> None:
    from okg.substrate.pact.agent_install import OKG_REPOSITORY_MARKER

    # A bare destination asked for the okg block explicitly keeps it whole.
    assert instruction_section(dest=tmp_path) == instruction_section()
    # An okg checkout without the development files (the public copy) does not.
    (tmp_path / OKG_REPOSITORY_MARKER).mkdir(parents=True)
    text = instruction_section(dest=tmp_path)

    assert not _PRIVATE_REFERENCE.search(text), text
    assert "pact/AGENTS.md" not in text
    # A checkout that has them gets the full block, byte for byte.
    for relative in (
        "history/openspec/x", "pact/AGENTS.md", "pact/project.md",
        "docs/runbooks/agents/reporting.md", "docs/runbooks/agents/coordination.md",
        "docs/runbooks/agents/okg-usage.md", "docs/runbooks/agents/ui.md",
        "scripts/ci/agent_preflight.py",
    ):
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")
    assert instruction_section(dest=tmp_path) == instruction_section()


@pytest.mark.parametrize(
    "skill", [path.name for path in bundled_skills()], ids=lambda name: name
)
def test_shipped_skill_names_no_development_repository_file(skill: str) -> None:
    text = next(p for p in bundled_skills() if p.name == skill).joinpath(
        "SKILL.md"
    ).read_text(encoding="utf-8")

    assert not _PRIVATE_REFERENCE.search(text)
    # `pact/AGENTS.md` appears only as the operator's own optional file.
    for line in text.splitlines():
        if "pact/AGENTS.md" in line:
            assert line.startswith("If the repository has `pact/AGENTS.md`"), line


def test_doctor_recovery_command_reproduces_the_check_readiness_ran(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A deployment that declares a doctor check under its own name gets
    ``--check <name>``, exactly as readiness runs it; others get plain doctor."""
    from types import SimpleNamespace

    declared = {"acme": [SimpleNamespace(name="acme"), SimpleNamespace(name="other")]}
    monkeypatch.setattr(
        "okg.substrate.doctor.load_deployment_doctor_checks",
        lambda deployment: declared.get(deployment, []),
    )

    assert pact_cli._doctor_recovery_command("acme") == (
        "uv run okg doctor --check acme --deployment acme --json"
    )
    assert pact_cli._doctor_recovery_command("plain") == (
        "uv run okg doctor --deployment plain --json"
    )
    unconfigured = pact_cli._doctor_cutover_check("fail", None)
    assert "graph_projection.deployment" in unconfigured["command"]
