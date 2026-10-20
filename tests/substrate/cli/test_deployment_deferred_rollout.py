"""req.gov.deferred-bounded — a deferred governance rollout is bounded and
cannot share an enforced DB."""
from __future__ import annotations

import datetime

from okg.substrate.cli.deployment import (
    _enforce_mode_dsn_keys,
    _nomos_rollout_entry,
    _rollout_deployment_dsn_key,
    audit_deferred_rollout,
)


TODAY = datetime.date(2026, 6, 19)


def _complete_deferred(**overrides):
    rollout = {
        "status": "deferred",
        "owner": {"name": "okg-bench", "contact": "owner@okg.example"},
        "review_by": "2027-01-01",
    }
    rollout.update(overrides)
    return rollout


def test_deferred_without_owner_and_expiry_fails_audit():
    findings = audit_deferred_rollout(
        {"status": "deferred", "reason": "later"},
        deployment_name="demo",
        deployment_dsn_key="dsn|db=demo",
        enforce_dsn_keys={},
        today=TODAY,
    )
    codes = {f["code"] for f in findings}
    assert "nomos.rollout.deferred_owner_missing" in codes
    assert "nomos.rollout.deferred_expiry_missing" in codes


def test_deferred_unstructured_owner_string_is_rejected():
    # A bare string owner has no contact handle -> not structured.
    findings = audit_deferred_rollout(
        _complete_deferred(owner="someone"),
        deployment_name="demo",
        deployment_dsn_key="dsn|db=demo",
        enforce_dsn_keys={},
        today=TODAY,
    )
    assert any(
        f["code"] == "nomos.rollout.deferred_owner_missing" for f in findings
    )


def test_complete_unexpired_deferred_passes_audit():
    findings = audit_deferred_rollout(
        _complete_deferred(),
        deployment_name="demo",
        deployment_dsn_key="dsn|db=demo",
        enforce_dsn_keys={"other": "dsn|db=other"},
        today=TODAY,
    )
    assert findings == []


def test_expired_deferred_is_reflagged_failed():
    findings = audit_deferred_rollout(
        _complete_deferred(review_by=None, expiry="2026-01-01"),
        deployment_name="demo",
        deployment_dsn_key="dsn|db=demo",
        enforce_dsn_keys={},
        today=TODAY,
    )
    codes = {f["code"] for f in findings}
    assert "nomos.rollout.deferred_expired" in codes


def test_deferred_sharing_enforce_dsn_is_rejected():
    findings = audit_deferred_rollout(
        _complete_deferred(),
        deployment_name="demo",
        deployment_dsn_key="${OKG_DSN}|db=okg_workspace",
        enforce_dsn_keys={"okg-workspace": "${OKG_DSN}|db=okg_workspace"},
        today=TODAY,
    )
    codes = {f["code"] for f in findings}
    assert "nomos.rollout.deferred_shared_enforce_dsn" in codes
    shared = next(
        f for f in findings
        if f["code"] == "nomos.rollout.deferred_shared_enforce_dsn"
    )
    assert shared["shared_with"] == ["okg-workspace"]


def test_distinct_databases_do_not_share_dsn_key():
    workspace = {"postgres": {"dsn": "${OKG_DSN}", "database": "okg_workspace"}}
    synth = {"postgres": {"dsn": "${OKG_DSN}", "database": "okg_synth_bench"}}
    assert _rollout_deployment_dsn_key(workspace) != _rollout_deployment_dsn_key(synth)
    # Same DSN env + same database collide.
    synth_same_db = {"postgres": {"dsn": "${OKG_DSN}", "database": "okg_workspace"}}
    assert _rollout_deployment_dsn_key(workspace) == _rollout_deployment_dsn_key(
        synth_same_db,
    )


def test_rollout_entry_reflags_deferred_without_owner_expiry(tmp_path):
    # The full entry path: a deferred manifest lacking owner/expiry is reported
    # as status=failed, not deferred.
    manifest = {
        "name": "demo",
        "postgres": {"dsn": "${OKG_DSN}", "database": "okg_demo"},
        "nomos": {"rollout": {"status": "deferred", "reason": "later"}},
    }
    entry = _nomos_rollout_entry(
        name="demo",
        kind="deployment",
        origin="repo",
        path=tmp_path / "deployment.yaml",
        manifest=manifest,
        source_registry=None,
        source_registry_path=None,
        enforce_dsn_keys={},
        today=TODAY,
    )
    assert entry["status"] == "failed"
    assert entry["findings"]


def test_rollout_entry_keeps_bounded_deferred(tmp_path):
    manifest = {
        "name": "demo",
        "postgres": {"dsn": "${OKG_DSN}", "database": "okg_demo"},
        "nomos": {
            "rollout": _complete_deferred(),
        },
    }
    entry = _nomos_rollout_entry(
        name="demo",
        kind="deployment",
        origin="repo",
        path=tmp_path / "deployment.yaml",
        manifest=manifest,
        source_registry=None,
        source_registry_path=None,
        enforce_dsn_keys={"okg-ci": "${OKG_CI_DSN}|db=okg_ci"},
        today=TODAY,
    )
    assert entry["status"] == "deferred"
    assert entry["findings"] == []


def test_enforce_mode_dsn_keys_maps_only_enforce_deployments(tmp_path, monkeypatch):
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)
    root = tmp_path / "deployments"
    enforced = root / "enf"
    audited = root / "aud"
    enforced.mkdir(parents=True)
    audited.mkdir(parents=True)
    (enforced / "deployment.yaml").write_text(
        "name: enf\n"
        "postgres:\n  dsn: ${OKG_DSN}\n  database: okg_enf\n"
        "nomos:\n  runtime_enforcement: enforce\n",
        encoding="utf-8",
    )
    (audited / "deployment.yaml").write_text(
        "name: aud\n"
        "postgres:\n  dsn: ${OKG_DSN}\n  database: okg_aud\n"
        "nomos:\n  runtime_enforcement: audit\n",
        encoding="utf-8",
    )
    keys = _enforce_mode_dsn_keys(root)
    assert "enf" in keys
    assert "aud" not in keys
