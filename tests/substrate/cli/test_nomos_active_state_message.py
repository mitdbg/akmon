"""The `nomos.active_state` blocker must name the finding it actually found.

Issue #1053: the summary was a binary present/missing string keyed on `ok`,
so a present-but-stale policy activation, a non-blocking enforcement mode, and
an unreachable database all read as "Nomos runtime active policy/config
missing". Both arms live in one module so neither can pass by accident.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from okg.substrate.cli.deployment import (
    _READY_NOMOS_ACTIVE_STATE_PRESENT_MESSAGE,
    _ready_nomos_active_state_message,
    _ready_nomos_active_state_status,
)
from okg.substrate.cli.main import main


def _failed(*findings: dict) -> dict:
    return {
        "schema": "okg.nomos.runtime_active_state.v1",
        "ok": False,
        "status": "failed",
        "deployment": "okg-workspace",
        "findings": list(findings),
    }


MISMATCH_FINDING = {
    "code": "nomos.runtime.active_policy_mismatch",
    "reason_code": "nomos.runtime.active_policy_mismatch",
    "severity": "error",
    "message": (
        "active Nomos policy does not match the deployment rollout baseline "
        "derived from this manifest"
    ),
    "expected": "sha256:expected",
    "actual": "sha256:actual",
}
ABSENT_POLICY_FINDING = {
    "code": "nomos.runtime.no_active_policy_bundle",
    "reason_code": "nomos.runtime.no_active_policy_bundle",
    "severity": "error",
    "message": "enforce-mode deployment has no active Nomos policy bundle",
}
ABSENT_CONFIG_FINDING = {
    "code": "nomos.runtime.no_active_config_set",
    "reason_code": "nomos.runtime.no_active_config_set",
    "severity": "error",
    "message": "enforce-mode deployment has no active Nomos config set",
}


# --------------------------------------------------------------------------
# The derived summary, one arm per finding code the status payload can emit.
# --------------------------------------------------------------------------


def test_pass_path_keeps_the_present_wording_verbatim() -> None:
    message = _ready_nomos_active_state_message({
        "ok": True,
        "status": "pass",
        "findings": [],
    })

    assert message == "Nomos runtime active policy/config present"
    assert message == _READY_NOMOS_ACTIVE_STATE_PRESENT_MESSAGE


def test_stale_policy_fingerprint_is_not_reported_as_missing() -> None:
    message = _ready_nomos_active_state_message(_failed(MISMATCH_FINDING))

    assert "missing" not in message
    assert "does not match the deployment rollout baseline" in message


def test_absent_policy_bundle_is_still_reported_as_absent() -> None:
    message = _ready_nomos_active_state_message(
        _failed(ABSENT_POLICY_FINDING),
    )

    assert "no active Nomos policy bundle" in message


def test_absence_and_staleness_together_are_both_named() -> None:
    # `_ready_nomos_active_state_status` always appends absence findings
    # before mismatch findings, so summarizing only findings[0] would drop the
    # mismatch every time this pair occurs.
    message = _ready_nomos_active_state_message(
        _failed(ABSENT_CONFIG_FINDING, MISMATCH_FINDING),
    )

    assert "no active Nomos config set" in message
    assert "does not match the deployment rollout baseline" in message


def test_more_than_two_findings_report_a_remainder_count() -> None:
    message = _ready_nomos_active_state_message(
        _failed(
            ABSENT_POLICY_FINDING,
            ABSENT_CONFIG_FINDING,
            MISMATCH_FINDING,
        ),
    )

    assert "no active Nomos policy bundle" in message
    assert "no active Nomos config set" in message
    assert "(+1 more finding)" in message


@pytest.mark.parametrize(
    ("finding", "expected_fragment"),
    [
        (
            {
                "code": "nomos.runtime.mode_not_blocking",
                "message": (
                    "production readiness requires Nomos enforce mode; "
                    "effective mode is off"
                ),
            },
            "requires Nomos enforce mode",
        ),
        (
            {
                "code": "nomos.runtime.dsn_unresolved",
                "message": (
                    "Nomos runtime active-state check needs a resolved DSN"
                ),
            },
            "needs a resolved DSN",
        ),
        (
            {
                "code": "nomos.runtime.active_state_unavailable",
                "message": (
                    "could not read Nomos active state: connection refused"
                ),
            },
            "could not read Nomos active state",
        ),
        (
            {
                "code": "nomos.runtime.active_config_mismatch",
                "message": (
                    "active Nomos config does not match the deployment "
                    "rollout baseline derived from this manifest"
                ),
            },
            "active Nomos config does not match",
        ),
    ],
)
def test_no_other_failure_code_claims_state_is_missing(
    finding: dict,
    expected_fragment: str,
) -> None:
    message = _ready_nomos_active_state_message(_failed(finding))

    assert expected_fragment in message
    assert "missing" not in message


def test_a_finding_without_a_message_names_its_code() -> None:
    message = _ready_nomos_active_state_message(
        _failed({"code": "nomos.runtime.active_policy_mismatch"}),
    )

    assert "nomos.runtime.active_policy_mismatch" in message
    assert "missing" not in message


def test_a_failure_with_no_findings_says_so_rather_than_guessing() -> None:
    message = _ready_nomos_active_state_message(_failed())

    assert message == (
        "Nomos runtime active state check failed without reporting a finding"
    )


def test_a_long_finding_message_is_collapsed_and_truncated() -> None:
    message = _ready_nomos_active_state_message(
        _failed({
            "code": "nomos.runtime.active_state_unavailable",
            "message": "could not connect\n" + ("x" * 500),
        }),
    )

    assert "\n" not in message
    assert message.endswith("…")
    assert len(message) < 320


# --------------------------------------------------------------------------
# The probe-failure finding must not carry a DSN password.
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("dsn", "redacted_fragment"),
    [
        ("postgres://okg:hunter2@db.invalid:5432/okg_workspace", "okg:***@"),
        # libpq accepts an empty username; this spelling used to slip past the
        # shared redactor with the password intact.
        ("postgres://:hunter2@db.invalid:5432/okg_workspace", "://:***@"),
    ],
)
def test_unreachable_database_redacts_the_dsn_and_names_the_probe(
    dsn: str,
    redacted_fragment: str,
    tmp_path: Path,
    monkeypatch,
) -> None:
    def _boom(*args, **kwargs):
        raise RuntimeError(f'connection to "{dsn}" failed: refused')

    monkeypatch.setattr("psycopg.connect", _boom)

    status = _ready_nomos_active_state_status(
        deployment="okg-workspace",
        dsn=dsn,
        manifest={
            "name": "okg-workspace",
            "nomos": {
                "runtime_enforcement": "enforce",
                "deployment_posture": {"readiness_profiles": ["dogfood"]},
            },
        },
        deployment_dir=tmp_path,
        readiness_profile="general",
    )

    assert status["ok"] is False
    finding = status["findings"][0]
    assert finding["code"] == "nomos.runtime.active_state_unavailable"
    assert "hunter2" not in json.dumps(status)
    assert redacted_fragment in finding["message"]

    message = _ready_nomos_active_state_message(status)
    assert "hunter2" not in message
    assert "could not read Nomos active state" in message
    assert "missing" not in message


# --------------------------------------------------------------------------
# Surface 1: `okg deployment ready` check rows.
# --------------------------------------------------------------------------


def _write_deployment(tmp_path: Path) -> Path:
    deployment_dir = tmp_path / "okg-ci"
    deployment_dir.mkdir()
    (deployment_dir / "deployment.yaml").write_text(
        yaml.safe_dump({
            "name": "okg-ci",
            "profile": {"name": "okg-ci"},
            "schema_dir": "schemas",
            "source_registry": "source_registry.yaml",
            "postgres": {"dsn": "${OKG_CI_DSN}"},
            "nomos": {
                "runtime_enforcement": "enforce",
                "deployment_posture": {
                    "deployment_class": "ci_authority",
                    "readiness_profiles": ["ci"],
                },
            },
        }),
        encoding="utf-8",
    )
    (deployment_dir / "source_registry.yaml").write_text(
        "sources: {}\n",
        encoding="utf-8",
    )
    return deployment_dir


def _deployment_ready_nomos_check(
    tmp_path: Path,
    monkeypatch,
    capsys,
    *,
    nomos_runtime: dict,
) -> dict:
    monkeypatch.setenv("OKG_CI_DSN", "postgres://process:pw@localhost/okg_ci")
    deployment_dir = _write_deployment(tmp_path)
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda name: deployment_dir if name == "okg-ci" else None,
    )
    monkeypatch.setattr(
        "okg.substrate.cli.deployment._deployment_ready_live_checks",
        lambda **kwargs: {
            "checks": [],
            "blockers": [],
            "warnings": [],
            "generation": {
                "generation_id": "okg:okg-ci:branch:default:gen:test",
                "status": "published",
                "count_integrity": {"ok": True, "status": "clean"},
            },
            "source_freshness": {"ok": True, "missing": [], "stale": []},
            "source_sync_status": {"ok": True, "status": "healthy"},
            "mcp": {"ok": True},
            "database": {"dsn": kwargs["dsn"]},
        },
    )
    monkeypatch.setattr(
        "okg.substrate.cli.deployment._ready_nomos_active_state_status",
        lambda **kwargs: nomos_runtime,
    )
    monkeypatch.setattr(
        "okg.substrate.deployment_apply.deployment_apply_health",
        lambda **kwargs: {
            "ok": True,
            "status": "ready",
            "managed": True,
            "deployment": kwargs["deployment"],
        },
    )

    main(["deployment", "ready", "okg-ci", "--checks", "ci", "--json"])
    payload = json.loads(capsys.readouterr().out)
    checks = [
        row
        for row in payload.get("checks") or []
        if row.get("id") == "nomos.active_state"
    ]
    assert len(checks) == 1, payload.get("checks")
    return checks[0]


def test_deployment_ready_names_the_mismatch_instead_of_missing(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    check = _deployment_ready_nomos_check(
        tmp_path,
        monkeypatch,
        capsys,
        nomos_runtime=_failed(MISMATCH_FINDING),
    )

    assert check["status"] == "blocker"
    assert "missing" not in check["message"]
    assert "does not match the deployment rollout baseline" in check["message"]


def test_deployment_ready_still_says_missing_when_state_is_absent(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    check = _deployment_ready_nomos_check(
        tmp_path,
        monkeypatch,
        capsys,
        nomos_runtime=_failed(ABSENT_POLICY_FINDING),
    )

    assert check["status"] == "blocker"
    assert "no active Nomos policy bundle" in check["message"]


def test_deployment_ready_pass_message_is_unchanged(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    check = _deployment_ready_nomos_check(
        tmp_path,
        monkeypatch,
        capsys,
        nomos_runtime={
            "schema": "okg.nomos.runtime_active_state.v1",
            "ok": True,
            "status": "pass",
            "deployment": "okg-ci",
            "findings": [],
        },
    )

    assert check["status"] == "pass"
    assert check["message"] == "Nomos runtime active policy/config present"
