from __future__ import annotations

from pathlib import Path

import yaml

from okg.substrate.deployment_contract import audit_deployment_portfolio


def _write_operator_macro_deployment(
    root: Path,
    *,
    operator: str,
    agent_tool: dict | None = None,
) -> None:
    deployment_dir = root / "akmon-demo"
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "schemas").mkdir()
    (deployment_dir / "source_registry.yaml").write_text(
        "sources: {}\n",
        encoding="utf-8",
    )
    manifest = {
        "name": "akmon-demo",
        "description": "AKMON contract fixture",
        "postgres": {"dsn": "postgres://u:p@localhost:5432/demo"},
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "modules": [],
        "enrichers": [],
        "contract": {
            "version": 1,
            "lifecycle": "prototype",
            "owner": "okg-operators",
            "intended_usage": "AKMON operator macro validation",
            "promotion_path": "replace retired/write operators before promotion",
            "evidence": {
                "readiness": {
                    "status": "not_required",
                    "collected_at": None,
                    "ttl_seconds": None,
                    "provenance": "unit-test",
                },
            },
        },
        "agent_tools": {
            "demo_tool": agent_tool or {
                "kind": "operator_macro",
                "operator": operator,
                "boundary": "okg_graph",
                "description": "fixture",
            },
        },
    }
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump(manifest, sort_keys=False),
        encoding="utf-8",
    )


def _codes(report: dict) -> set[str]:
    return {
        finding["code"]
        for deployment in report["deployments"]
        for finding in deployment["findings"]
    }


def test_operator_macro_accepts_canonical_read_operator(tmp_path: Path) -> None:
    _write_operator_macro_deployment(tmp_path, operator="search")

    report = audit_deployment_portfolio(tmp_path, contract_level="current")

    assert report["ok"] is True
    assert "deployment_contract.akmon.operator_macro_invalid" not in _codes(report)


def test_operator_macro_rejects_write_operator_at_next_level(
    tmp_path: Path,
) -> None:
    _write_operator_macro_deployment(tmp_path, operator="propose")

    current = audit_deployment_portfolio(tmp_path, contract_level="current")
    next_report = audit_deployment_portfolio(tmp_path, contract_level="next")

    assert current["ok"] is True
    assert next_report["ok"] is False
    assert "deployment_contract.akmon.operator_macro_invalid" in _codes(next_report)


def test_credentialed_live_tool_requires_external_live_boundary(
    tmp_path: Path,
) -> None:
    _write_operator_macro_deployment(
        tmp_path,
        operator="search",
        agent_tool={
            "module": "demo.live",
            "function": "fetch",
            "requires_env": ["DEMO_TOKEN"],
            "boundary": "okg_graph",
            "description": "fixture",
        },
    )

    report = audit_deployment_portfolio(tmp_path, contract_level="next")

    assert report["ok"] is False
    assert (
        "deployment_contract.akmon.external_live_boundary_missing"
        in _codes(report)
    )
