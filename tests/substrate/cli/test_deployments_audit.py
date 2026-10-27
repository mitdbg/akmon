from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import yaml

from okg.substrate.deployment_contract import audit_deployment_portfolio


def _contract(lifecycle: str = "prototype") -> dict:
    return {
        "version": 1,
        "lifecycle": lifecycle,
        "owner": "okg-operators",
        "intended_usage": "unit-test deployment",
        "promotion_path": "collect live readiness evidence before promotion",
        "evidence": {
            "readiness": {
                "status": "not_required",
                "collected_at": None,
                "ttl_seconds": None,
                "provenance": "unit-test",
            },
        },
    }


def _clean_live_slots() -> dict[str, dict]:
    observed_at = datetime.now(timezone.utc).isoformat()
    return {
        name: {
            "status": "clean",
            "observed_at": observed_at,
            "ttl_seconds": 3600,
            "provenance": {"command": "unit-test"},
        }
        for name in (
            "environment",
            "migrations",
            "latest_published_generation",
            "source_freshness",
            "queues_runtime",
            "dlq",
            "doctor",
            "mcp_read_alignment",
        )
    }


def _live_contract(lifecycle: str = "production") -> dict:
    return {
        "version": 1,
        "lifecycle": lifecycle,
        "owner": "okg-operators",
        "intended_usage": "unit-test live deployment",
        "evidence": {
            "readiness": {
                "status": "clean",
                "collected_at": datetime.now(timezone.utc).isoformat(),
                "ttl_seconds": 3600,
                "provenance": "unit-test",
                "slots": _clean_live_slots(),
            },
        },
    }


def _write_deployment(
    root: Path,
    *,
    name: str = "demo",
    contract: dict | None = None,
    postgres_dsn: str | None = "postgres://u:p@localhost:5432/demo",
    source_probe: bool = True,
    poll_probe: str | None = None,
    mcp_instructions: str | None = None,
) -> Path:
    deployment_dir = root / name
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "schemas").mkdir()
    (deployment_dir / "schemas" / "schema.yaml").write_text(
        "classes: {}\n",
        encoding="utf-8",
    )
    probe_line = (
        "    change_probe_kind = 'content_hash'\n"
        "    def __init__(self):\n"
        "        self.change_probe = object()\n"
        if source_probe
        else ""
    )
    (deployment_dir / "fixture_sources.py").write_text(
        "class DemoSource:\n" + probe_line + "    pass\n",
        encoding="utf-8",
    )
    source_entry = {
        "module": "fixture_sources",
        "class": "DemoSource",
        "ownership_id": "demo.demo-source",
        "source_class": "discovery_crawl",
        "record_identity_kind": "scoped_locator",
        "record_identity_fields": ["path"],
        "source_revision_kind": "content_hash",
        "deletion_semantics": "missing_from_completed_scope",
        "publication_mode": "published_generation",
    }
    if poll_probe is not None:
        source_entry["sync"] = {
            "triggers": ["poll"],
            "poll": {
                "probe": poll_probe,
                "interval_seconds": 300,
            },
        }
    (deployment_dir / "source_registry.yaml").write_text(
        yaml.safe_dump({"sources": {"demo-source": source_entry}}),
        encoding="utf-8",
    )
    if mcp_instructions is not None:
        (deployment_dir / "mcp_instructions.md").write_text(
            mcp_instructions,
            encoding="utf-8",
        )
    manifest = {
        "name": name,
        "description": "Deployment audit fixture",
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "modules": [],
        "enrichers": [],
        # Declared so the audit counts stay about what each test sets (#3958).
        "readiness": {"profile": "standard"},
    }
    if postgres_dsn is not None:
        manifest["postgres"] = {"dsn": postgres_dsn}
    if contract is not None:
        manifest["contract"] = contract
    if mcp_instructions is not None:
        manifest["mcp_instructions"] = "mcp_instructions.md"
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump(manifest, sort_keys=False),
        encoding="utf-8",
    )
    return deployment_dir


def _codes(report: dict) -> set[str]:
    return {
        finding["code"]
        for deployment in report["deployments"]
        for finding in deployment["findings"]
    }


def test_deployments_audit_current_accepts_non_production_contract(
    tmp_path: Path,
) -> None:
    _write_deployment(tmp_path, contract=_contract("prototype"))

    report = audit_deployment_portfolio(tmp_path, contract_level="current")

    assert report["schema"] == "okg.deployment_contract.audit.v1"
    assert report["ok"] is True
    assert report["deployment_count"] == 1
    deployment = report["deployments"][0]
    assert deployment["lifecycle"] == "prototype"
    assert deployment["readiness_evidence"]["status"] == "not_required"
    assert deployment["summary"]["blockers"] == 0
    assert report["readiness_summary"]["readiness_statuses"]["not_required"] == 1


def test_deployments_audit_blocks_missing_contract(tmp_path: Path) -> None:
    _write_deployment(tmp_path, contract=None)

    report = audit_deployment_portfolio(tmp_path, contract_level="current")

    assert report["ok"] is False
    assert "deployment_contract.block.missing" in _codes(report)


def test_deployments_audit_readiness_summary_fields(tmp_path: Path) -> None:
    prototype_contract = _contract("prototype")
    prototype_contract["evidence"]["readiness"]["status"] = "historical"
    blocked_contract = {
        "version": 1,
        "lifecycle": "blocked",
        "owner": "okg-operators",
        "intended_usage": "unit-test blocked deployment",
        "blocker": {
            "reason": "waiting on live source credentials",
            "review_by": "2999-01-01",
        },
        "evidence": {
            "readiness": {
                "status": "blocked",
                "blocked_slot": "source_freshness",
                "collected_at": None,
                "ttl_seconds": None,
                "provenance": "unit-test",
            },
        },
    }
    _write_deployment(
        tmp_path,
        name="prototype-demo",
        contract=prototype_contract,
        postgres_dsn="${PROTO_DSN}",
    )
    _write_deployment(
        tmp_path,
        name="blocked-demo",
        contract=blocked_contract,
        postgres_dsn="${BLOCKED_DSN}",
    )

    report = audit_deployment_portfolio(tmp_path, env={})

    assert report["ok"] is True
    readiness_summary = report["readiness_summary"]
    assert readiness_summary["env_var_warning_count"] == 2
    assert readiness_summary["missing_governance_count"] == 2
    assert readiness_summary["blocked_readiness_scope_count"] == 1
    assert readiness_summary["stale_or_historical_evidence_count"] == 1
    assert readiness_summary["readiness_statuses"] == {
        "blocked": 1,
        "historical": 1,
    }
    assert (
        readiness_summary["warning_families"][
            "deployment_contract.lint.deployment.env.required_missing"
        ]
        == 2
    )
    assert (
        readiness_summary["by_lifecycle"]["prototype"]["readiness_statuses"][
            "historical"
        ]
        == 1
    )
    assert (
        readiness_summary["by_lifecycle"]["blocked"]["readiness_statuses"][
            "blocked"
        ]
        == 1
    )


def test_live_lifecycle_unset_dsn_ref_is_contract_blocker(
    tmp_path: Path,
) -> None:
    _write_deployment(
        tmp_path,
        contract=_live_contract("production"),
        postgres_dsn="${LIVE_DSN}",
    )

    report = audit_deployment_portfolio(tmp_path, env={})

    assert report["ok"] is False
    findings = {
        finding["code"]: finding
        for deployment in report["deployments"]
        for finding in deployment["findings"]
    }
    env_finding = findings[
        "deployment_contract.lint.deployment.env.required_missing"
    ]
    assert env_finding["severity"] == "blocker"
    assert env_finding["details"]["severity_policy"] == (
        "live_lifecycle_requires_resolved_env"
    )
    assert report["readiness_summary"]["env_var_warning_count"] == 0


def test_non_live_lifecycle_unset_dsn_ref_stays_warning(
    tmp_path: Path,
) -> None:
    _write_deployment(
        tmp_path,
        contract=_contract("prototype"),
        postgres_dsn="${PROTO_DSN}",
    )

    report = audit_deployment_portfolio(tmp_path, env={})

    assert report["ok"] is True
    findings = {
        finding["code"]: finding
        for deployment in report["deployments"]
        for finding in deployment["findings"]
    }
    env_finding = findings[
        "deployment_contract.lint.deployment.env.required_missing"
    ]
    assert env_finding["severity"] == "warning"
    assert env_finding["details"]["severity_policy"] == (
        "declared_env_ref_visible_without_secret"
    )
    assert report["readiness_summary"]["env_var_warning_count"] == 1


def test_next_prototype_unset_dsn_ref_without_authority_is_contract_blocker(
    tmp_path: Path,
) -> None:
    _write_deployment(
        tmp_path,
        contract=_contract("prototype"),
        postgres_dsn="${PROTO_DSN}",
    )

    report = audit_deployment_portfolio(
        tmp_path,
        contract_level="next",
        env={},
    )

    assert report["ok"] is False
    findings = {
        finding["code"]: finding
        for deployment in report["deployments"]
        for finding in deployment["findings"]
    }
    env_finding = findings[
        "deployment_contract.lint.deployment.env.required_missing"
    ]
    assert env_finding["severity"] == "blocker"
    assert env_finding["details"]["severity_policy"] == (
        "prototype_requires_declared_env_authority"
    )


def test_declared_env_authority_removes_env_warning_debt(
    tmp_path: Path,
) -> None:
    contract = _contract("prototype")
    contract["environment"] = {
        "required": {
            "PROTO_DSN": {
                "purpose": "postgres.dsn",
                "source": "operator_secret",
                "owner": "okg-operators",
                "review_by": "2999-01-01",
            },
        },
    }
    _write_deployment(
        tmp_path,
        contract=contract,
        postgres_dsn="${PROTO_DSN}",
    )

    report = audit_deployment_portfolio(tmp_path, env={})

    assert report["ok"] is True
    findings = {
        finding["code"]: finding
        for deployment in report["deployments"]
        for finding in deployment["findings"]
    }
    assert (
        "deployment_contract.lint.deployment.env.required_missing"
        not in findings
    )
    env_finding = findings[
        "deployment_contract.lint.deployment.env.required_declared_unset"
    ]
    assert env_finding["severity"] == "info"
    assert env_finding["details"]["severity_policy"] == (
        "declared_env_ref_visible_without_secret"
    )
    assert report["summary"]["warnings"] == 1  # missing Nomos posture only.
    # Two infos: the declared-env finding under test, plus
    # producer.envelope_missing for the fixture's policy-less demo source
    # (consistency-output-envelopes advisory; the fixture deliberately
    # declares no admission_policy).
    assert report["summary"]["infos"] == 2
    assert any(code.endswith("producer.envelope_missing") for code in findings)
    assert report["deployments"][0]["lint"]["warnings"] == 1
    assert report["deployments"][0]["lint"]["infos"] == 2
    assert report["readiness_summary"]["env_var_warning_count"] == 0


def test_next_prototype_without_dsn_declaration_is_contract_blocker(
    tmp_path: Path,
) -> None:
    _write_deployment(
        tmp_path,
        contract=_contract("prototype"),
        postgres_dsn=None,
    )

    report = audit_deployment_portfolio(
        tmp_path,
        contract_level="next",
        env={},
    )

    assert report["ok"] is False
    findings = {
        finding["code"]: finding
        for deployment in report["deployments"]
        for finding in deployment["findings"]
    }
    env_finding = findings[
        "deployment_contract.lint.deployment.env.dsn_not_declared"
    ]
    assert env_finding["severity"] == "blocker"
    assert env_finding["details"]["severity_policy"] == (
        "prototype_requires_declared_env_authority"
    )


def test_deployments_audit_current_fails_closed_for_source_probe_gaps(
    tmp_path: Path,
) -> None:
    _write_deployment(
        tmp_path,
        contract=_contract("prototype"),
        source_probe=False,
        mcp_instructions="Use get_node, list_neighbors, and run_sql.",
    )

    current = audit_deployment_portfolio(tmp_path, contract_level="current")
    next_report = audit_deployment_portfolio(tmp_path, contract_level="next")

    assert current["ok"] is False
    assert next_report["ok"] is False
    current_codes = _codes(current)
    next_codes = _codes(next_report)
    assert (
        "deployment_contract.lint.deployment.source_registry.probe_missing"
        in current_codes
    )
    assert (
        "deployment_contract.lint.deployment.source_registry.probe_missing"
        in next_codes
    )
    assert (
        "deployment_contract.lint.deployment.source_registry.probe_runtime_missing"
        not in next_codes
    )
    assert "deployment_contract.akmon.retired_tool_reference" in next_codes


def test_next_contract_rejects_legacy_poll_probe(tmp_path: Path) -> None:
    _write_deployment(
        tmp_path,
        contract=_contract("prototype"),
        poll_probe="content_hash",
    )

    current = audit_deployment_portfolio(tmp_path, contract_level="current")
    next_report = audit_deployment_portfolio(tmp_path, contract_level="next")

    code = (
        "deployment_contract.lint."
        "deployment.source_registry.poll_probe_unsound"
    )
    assert current["ok"] is False
    assert code in _codes(current)
    assert next_report["ok"] is False
    assert code in _codes(next_report)


def test_current_contract_probe_enforcement_is_not_env_configurable(
    tmp_path: Path,
) -> None:
    _write_deployment(
        tmp_path,
        contract=_contract("prototype"),
        source_probe=False,
    )

    report = audit_deployment_portfolio(
        tmp_path,
        contract_level="current",
        env={"OKG_ENFORCE_SOURCE_PROBE": "0"},
    )

    assert report["ok"] is False
    codes = _codes(report)
    assert (
        "deployment_contract.lint.deployment.source_registry.probe_missing"
        in codes
    )


def test_archived_deployment_skips_active_akmon_instruction_checks(
    tmp_path: Path,
) -> None:
    contract = {
        "version": 1,
        "lifecycle": "archived",
        "owner": "okg-operators",
        "intended_usage": "archived unit-test deployment",
        "archive_reason": "superseded fixture",
    }
    _write_deployment(
        tmp_path,
        contract=contract,
        mcp_instructions="Historical note: this archive once used get_node.",
    )

    report = audit_deployment_portfolio(
        tmp_path,
        contract_level="next",
        include_archived=True,
    )

    assert report["ok"] is True
    assert "deployment_contract.akmon.retired_tool_reference" not in _codes(report)


def test_deployments_audit_cli_json(tmp_path: Path, capsys) -> None:
    from okg.substrate.cli.main import main

    _write_deployment(tmp_path, contract=_contract("demo"))

    rc = main([
        "deployments",
        "audit",
        "--root",
        str(tmp_path),
        "--json",
    ])

    captured = capsys.readouterr()
    assert rc == 0
    payload = json.loads(captured.out)
    assert payload["ok"] is True
    assert payload["deployments"][0]["name"] == "demo"
