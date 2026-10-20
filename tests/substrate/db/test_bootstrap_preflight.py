"""canonical-dev-okg: bootstrap preflight + extension provisioning.

The pure readiness logic is unit-tested without a database; the live probe +
extension provisioning are postgres-gated.
"""
from __future__ import annotations

import pytest

from okg.substrate.db.management import _bootstrap_readiness, bootstrap_preflight
from okg.substrate.db.migrate import (
    REQUIRED_EXTENSIONS,
    ensure_required_extensions,
)


# --- pure readiness logic (no DB) ---


def test_readiness_names_all_missing_prerequisites_in_one_report() -> None:
    """A fresh DB (no extensions, no schema, no catalog) yields ONE report
    naming every missing prerequisite — not one-per-blocked-publish."""
    report = _bootstrap_readiness(
        installed=set(), schema_applied=False, catalog_version=None,
    )
    assert report["ok"] is False
    assert set(report["extensions"]["missing"]) == set(REQUIRED_EXTENSIONS)
    assert report["schema"]["applied"] is False
    assert report["catalog"]["applied"] is False
    codes = {f["code"] for f in report["findings"]}
    assert codes == {
        "missing_required_extensions",
        "schema_not_applied",
        "catalog_not_applied",
    }
    assert report["summary"]["blocker_count"] == 3
    # every blocker carries an actionable fix hint
    assert all(f.get("fix_hint") for f in report["findings"])


def test_readiness_ok_when_everything_present() -> None:
    report = _bootstrap_readiness(
        installed=set(REQUIRED_EXTENSIONS), schema_applied=True, catalog_version=2,
    )
    assert report["ok"] is True
    assert report["findings"] == []
    assert report["catalog"]["version_id"] == 2


def test_readiness_partial_after_extensions_and_migrate() -> None:
    """Extensions + schema present but no catalog -> only the catalog blocker
    remains (incremental repair is legible)."""
    report = _bootstrap_readiness(
        installed=set(REQUIRED_EXTENSIONS), schema_applied=True, catalog_version=None,
    )
    assert report["ok"] is False
    assert {f["code"] for f in report["findings"]} == {"catalog_not_applied"}


# --- live database integration (postgres-gated) ---


@pytest.mark.postgres
def test_preflight_against_migrated_db(clean_runtime: str) -> None:
    """On a migrated runtime, the preflight reports extensions present + schema
    applied, with the report shape intact."""
    report = bootstrap_preflight(clean_runtime)
    for key in (
        "ok", "status", "check", "extensions", "schema", "catalog",
        "findings", "summary",
    ):
        assert key in report
    assert report["check"] == "preflight"
    assert report["extensions"]["missing"] == []
    assert report["schema"]["applied"] is True
    # internal consistency: ok iff no findings
    assert report["ok"] == (report["findings"] == [])


@pytest.mark.postgres
def test_ensure_required_extensions_idempotent_on_migrated_db(
    clean_runtime: str,
) -> None:
    result = ensure_required_extensions(clean_runtime)
    assert set(result["already_present"]) == set(REQUIRED_EXTENSIONS)
    assert result["created"] == []
