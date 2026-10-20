from __future__ import annotations

import json
from pathlib import Path

import yaml

from okg.substrate.cli.main import main
from okg.substrate.pact import cli as pact_cli
from okg.substrate.pact.models import Change, PactManifest
from okg.substrate.pact.store import save_manifest
from tests.pact.test_cli import _seed_root


def _manifest(change_id: str) -> PactManifest:
    """A minimal one-requirement manifest.

    Kept here rather than imported from tests/pact/test_backlog_status.py:
    that file checks this repository's own records and does not ship in the
    public mirror, so importing from it broke collection there.
    """
    return PactManifest(
        change=Change(id=change_id, title=change_id),
        requirements=[
            {
                "id": f"req.{change_id}",
                "title": "Requirement",
                "statement": "Requirement must be visible.",
                "evidence_policy": {"required": ["planned_test"]},
                "scenarios": [
                    {
                        "id": f"scn.{change_id}",
                        "title": "visible",
                        "when": ["PACT list runs"],
                        "then": ["the change is included"],
                    }
                ],
                "acceptance": [
                    {
                        "id": f"acc.{change_id}",
                        "description": "backlog row is emitted",
                        "evidence": ["planned_test"],
                    }
                ],
            }
        ],
        tasks=[
            {
                "id": f"task.{change_id}",
                "title": "Task",
                "verification": "uv run --extra dev pytest tests/pact/test_bootstrap_dsn_authority.py -q",
                "requirements": [f"req.{change_id}"],
            }
        ],
    )


def test_bootstrap_graph_projection_uses_shared_authority(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    root = _seed_root(tmp_path)
    save_manifest(root, _manifest("demo"))
    deployment_dir = tmp_path / "deployments" / "okg-workspace"
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump({
            "name": "okg-workspace",
            "postgres": {
                "dsn": "${OKG_DSN}",
                "database": "okg_workspace",
            },
        }),
        encoding="utf-8",
    )
    monkeypatch.delenv("OKG_DSN", raising=False)
    monkeypatch.setenv("OKG_LOCAL_ENV", str(tmp_path / "missing.env"))
    monkeypatch.setenv("OKG_ENABLE_LOCAL_ENV_FOR_TESTS", "1")
    monkeypatch.setenv("OKG_PACT_GRAPH_DEPLOYMENT", "okg-workspace")
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda deployment: deployment_dir if deployment == "okg-workspace" else None,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_authority.worktree_main_local_env_dsn",
        lambda: "postgres://postgres:okg@127.0.0.1:55434/okg_workspace",
    )

    def missing_projection_row(dsn: str, change_id: str):
        assert dsn == "postgres://postgres:okg@127.0.0.1:55434/okg_workspace"
        assert change_id == "demo"
        return None

    monkeypatch.setattr(
        pact_cli,
        "_load_graph_projection_row",
        missing_projection_row,
    )

    rc = main([
        "pact",
        "--root",
        str(root),
        "view",
        "demo",
        "--format=bootstrap",
        "--full",
        "--json",
    ])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    graph = payload["pact"]["graph_projection"]
    assert graph["status"] == "missing"
    assert graph["authority"]["mode"] == "worktree_main_local_env"
    assert graph["target"]["identity"] == "ok"
