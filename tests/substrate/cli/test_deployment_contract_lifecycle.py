from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta, timezone

import pytest
import yaml

from okg.substrate.deployment_contract import audit_deployment_portfolio


def _write_contract_fixture(root: Path, contract: dict) -> None:
    deployment_dir = root / "lifecycle-demo"
    deployment_dir.mkdir(parents=True)
    (deployment_dir / "schemas").mkdir()
    (deployment_dir / "source_registry.yaml").write_text(
        "sources: {}\n",
        encoding="utf-8",
    )
    manifest = {
        "name": "lifecycle-demo",
        "description": "Lifecycle contract fixture",
        "postgres": {"dsn": "postgres://u:p@localhost:5432/demo"},
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "modules": [],
        "enrichers": [],
        "contract": {
            "version": 1,
            "owner": "okg-operators",
            "intended_usage": "lifecycle contract fixture",
            **contract,
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


@pytest.mark.parametrize("lifecycle", ["production", "staging"])
def test_live_lifecycle_requires_clean_fresh_readiness(
    tmp_path: Path,
    lifecycle: str,
) -> None:
    _write_contract_fixture(tmp_path, {"lifecycle": lifecycle})

    report = audit_deployment_portfolio(tmp_path)

    assert report["ok"] is False
    assert "deployment_contract.readiness.missing" in _codes(report)


def test_blocked_lifecycle_requires_blocker_reason_and_review_date(
    tmp_path: Path,
) -> None:
    _write_contract_fixture(tmp_path, {"lifecycle": "blocked"})

    report = audit_deployment_portfolio(tmp_path)

    assert report["ok"] is False
    assert "deployment_contract.blocker.missing" in _codes(report)


def test_blocked_lifecycle_requires_blocked_readiness_evidence(
    tmp_path: Path,
) -> None:
    _write_contract_fixture(
        tmp_path,
        {
            "lifecycle": "blocked",
            "blocker": {
                "reason": "fixture blocker",
                "review_by": "2999-01-01",
            },
        },
    )

    report = audit_deployment_portfolio(tmp_path)

    assert report["ok"] is False
    assert "deployment_contract.blocked_readiness.missing" in _codes(report)


def test_blocked_lifecycle_rejects_not_required_readiness(
    tmp_path: Path,
) -> None:
    _write_contract_fixture(
        tmp_path,
        {
            "lifecycle": "blocked",
            "blocker": {
                "reason": "fixture blocker",
                "review_by": "2999-01-01",
            },
            "evidence": {
                "readiness": {
                    "status": "not_required",
                    "collected_at": None,
                    "ttl_seconds": None,
                    "provenance": "unit-test",
                },
            },
        },
    )

    report = audit_deployment_portfolio(tmp_path)

    assert report["ok"] is False
    assert (
        "deployment_contract.blocked_readiness.status_invalid"
        in _codes(report)
    )


def test_blocked_lifecycle_requires_scoped_blocked_evidence(
    tmp_path: Path,
) -> None:
    _write_contract_fixture(
        tmp_path,
        {
            "lifecycle": "blocked",
            "blocker": {
                "reason": "fixture blocker",
                "review_by": "2999-01-01",
            },
            "evidence": {
                "readiness": {
                    "status": "blocked",
                    "collected_at": None,
                    "ttl_seconds": None,
                    "provenance": "unit-test",
                },
            },
        },
    )

    report = audit_deployment_portfolio(tmp_path)

    assert report["ok"] is False
    assert (
        "deployment_contract.blocked_readiness.scope_missing"
        in _codes(report)
    )


def test_blocked_lifecycle_accepts_named_blocked_slot(
    tmp_path: Path,
) -> None:
    _write_contract_fixture(
        tmp_path,
        {
            "lifecycle": "blocked",
            "blocker": {
                "reason": "fixture blocker",
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
        },
    )

    report = audit_deployment_portfolio(tmp_path)

    assert report["ok"] is True
    assert report["deployments"][0]["readiness_evidence"]["blocked_scope"] == {
        "source": "blocked_slot",
        "value": "source_freshness",
    }


def test_archived_lifecycle_requires_archive_reason(tmp_path: Path) -> None:
    _write_contract_fixture(tmp_path, {"lifecycle": "archived"})

    report = audit_deployment_portfolio(tmp_path, include_archived=True)

    assert report["ok"] is False
    assert "deployment_contract.archive_reason.missing" in _codes(report)


def test_archived_findings_are_visible_in_default_audit(tmp_path: Path) -> None:
    _write_contract_fixture(tmp_path, {"lifecycle": "archived"})

    report = audit_deployment_portfolio(tmp_path)

    assert report["ok"] is False
    assert report["deployment_count"] == 1
    assert "deployment_contract.archive_reason.missing" in _codes(report)


def test_waivers_require_owner_reason_and_expiry(tmp_path: Path) -> None:
    _write_contract_fixture(
        tmp_path,
        {
            "lifecycle": "prototype",
            "promotion_path": "fixture",
            "evidence": {
                "readiness": {
                    "status": "not_required",
                    "collected_at": None,
                    "ttl_seconds": None,
                    "provenance": "unit-test",
                },
            },
            "waivers": [{"reason": "temporary exception"}],
        },
    )

    report = audit_deployment_portfolio(tmp_path)

    assert report["ok"] is False
    codes = _codes(report)
    assert "deployment_contract.waiver.owner_missing" in codes
    assert "deployment_contract.waiver.expiry_missing" in codes


def test_next_blocked_invalid_review_by_still_checks_waivers(
    tmp_path: Path,
) -> None:
    _write_contract_fixture(
        tmp_path,
        {
            "lifecycle": "blocked",
            "blocker": {
                "reason": "fixture blocker",
                "review_by": "not-a-date",
            },
            "evidence": {
                "readiness": {
                    "status": "blocked",
                    "collected_at": None,
                    "ttl_seconds": None,
                    "provenance": "unit-test",
                },
            },
            "waivers": [{"reason": "temporary exception"}],
        },
    )

    report = audit_deployment_portfolio(tmp_path, contract_level="next")

    assert report["ok"] is False
    codes = _codes(report)
    assert "deployment_contract.blocker.review_by_invalid" in codes
    assert "deployment_contract.waiver.owner_missing" in codes
    assert "deployment_contract.waiver.expiry_missing" in codes


def test_live_lifecycle_requires_all_readiness_slots_clean(
    tmp_path: Path,
) -> None:
    _write_contract_fixture(
        tmp_path,
        {
            "lifecycle": "production",
            "evidence": {
                "readiness": {
                    "status": "clean",
                    "collected_at": datetime.now(timezone.utc).isoformat(),
                    "ttl_seconds": 3600,
                    "provenance": "unit-test",
                    "slots": {
                        **_clean_live_slots(),
                        "source_freshness": {
                            **_clean_live_slots()["source_freshness"],
                            "status": "stale",
                        },
                    },
                },
            },
        },
    )

    report = audit_deployment_portfolio(tmp_path)

    assert report["ok"] is False
    codes = _codes(report)
    assert "deployment_contract.readiness.slot_not_clean" in codes


def test_live_lifecycle_rejects_status_only_slot_stubs(
    tmp_path: Path,
) -> None:
    _write_contract_fixture(
        tmp_path,
        {
            "lifecycle": "production",
            "evidence": {
                "readiness": {
                    "status": "clean",
                    "collected_at": datetime.now(timezone.utc).isoformat(),
                    "ttl_seconds": 3600,
                    "provenance": "unit-test",
                    "slots": {
                        name: {"status": "clean"}
                        for name in _clean_live_slots()
                    },
                },
            },
        },
    )

    report = audit_deployment_portfolio(tmp_path)

    assert report["ok"] is False
    codes = _codes(report)
    assert "deployment_contract.readiness.slot_observed_at_missing" in codes
    assert "deployment_contract.readiness.slot_provenance_missing" in codes


def test_live_lifecycle_accepts_full_slot_metadata(
    tmp_path: Path,
) -> None:
    _write_contract_fixture(
        tmp_path,
        {
            "lifecycle": "production",
            "evidence": {
                "readiness": {
                    "status": "clean",
                    "collected_at": datetime.now(timezone.utc).isoformat(),
                    "ttl_seconds": 3600,
                    "provenance": "unit-test",
                    "slots": _clean_live_slots(),
                },
            },
        },
    )

    report = audit_deployment_portfolio(tmp_path)

    assert report["ok"] is True


def test_live_lifecycle_rejects_expired_slot_ttl(
    tmp_path: Path,
) -> None:
    expired_slots = _clean_live_slots()
    expired_slots["doctor"] = {
        **expired_slots["doctor"],
        "observed_at": (
            datetime.now(timezone.utc) - timedelta(days=1)
        ).isoformat(),
        "ttl_seconds": 60,
    }
    _write_contract_fixture(
        tmp_path,
        {
            "lifecycle": "production",
            "evidence": {
                "readiness": {
                    "status": "clean",
                    "collected_at": datetime.now(timezone.utc).isoformat(),
                    "ttl_seconds": 3600,
                    "provenance": "unit-test",
                    "slots": expired_slots,
                },
            },
        },
    )

    report = audit_deployment_portfolio(tmp_path)

    assert report["ok"] is False
    assert "deployment_contract.readiness.slot_expired" in _codes(report)


def test_live_lifecycle_rejects_future_readiness_timestamp(
    tmp_path: Path,
) -> None:
    _write_contract_fixture(
        tmp_path,
        {
            "lifecycle": "production",
            "evidence": {
                "readiness": {
                    "status": "clean",
                    "collected_at": "2999-01-01T00:00:00+00:00",
                    "ttl_seconds": 3600,
                    "provenance": "unit-test",
                    "slots": _clean_live_slots(),
                },
            },
        },
    )

    report = audit_deployment_portfolio(tmp_path)

    assert report["ok"] is False
    assert "deployment_contract.readiness.future_collected_at" in _codes(report)
