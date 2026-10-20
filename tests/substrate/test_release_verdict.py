"""One release verdict read by the release check, `okg mcp-serve` and doctor.

PACT unknown-s7-release-verdict (umbrella unknown-as-a-value, issue #2573,
prepares #2035). Before this change, a database one additive migration
behind a deployment that declares a `release` block got three answers:

* `okg mcp-serve` printed "this database is behind ...; continuing" and
  then refused with `deployment release/version skew detected`;
* `okg doctor --check database` reported the database healthy, because it
  has no finding for a pending additive migration at all;
* `okg deployment release` refused.

The promise test below drives all three from one stubbed database state and
requires one answer and one reason. The rest pin the verdict rules: a
pending destructive cleanup still serves (it is a cleanup `okg up` defers),
`read_only` needs every pending additive migration to declare
`reads_safe_behind`, and the declaration is refused on SQL that backfills,
redefines a view, or drops something.
"""
from __future__ import annotations

import argparse
import dataclasses
import json
from types import SimpleNamespace

import pytest

from okg.substrate import deployment_release
from okg.substrate.deployment_release import (
    DatabaseReleaseState,
    ReleaseArtifact,
    ReleaseContract,
    evaluate_release_contract,
)
from okg.substrate.errors import SubstrateError

_UNDECLARED = {
    "migration_id": "0099_add_widget_table",
    "destructive": False,
}
_DECLARED = {
    "migration_id": "0098_add_gadget_table",
    "destructive": False,
    "reads_safe_behind": True,
}
_DESTRUCTIVE = {
    "migration_id": "retire_legacy_runtime",
    "destructive": True,
}


def _artifact() -> ReleaseArtifact:
    return ReleaseArtifact(
        code_revision="code-new",
        schema_checksum="schema-new",
        catalog_fingerprint="catalog-new",
        skill_bundle_hash="skills-new",
    )


def _db_state(*pending: dict) -> DatabaseReleaseState:
    return DatabaseReleaseState(
        ledger_exists=True,
        schema_checksum="schema-new",
        schema_ok=not any(not row.get("destructive") for row in pending),
        pending_migrations=tuple(pending),
        blocking_pending_migrations=tuple(
            row for row in pending if not row.get("destructive")
        ),
        checksum_mismatches=(),
        checksum_compatibilities=(),
        latest_catalog_version_id=42,
        latest_catalog_fingerprint="catalog-new",
    )


def _report(*pending: dict, channel: str = "dev") -> dict:
    return evaluate_release_contract(
        deployment="dep",
        contract=ReleaseContract(channel=channel, pin=None),
        current=_artifact(),
        database=_db_state(*pending),
        process_role="cli",
    )


def _declare_release(monkeypatch, state: DatabaseReleaseState) -> None:
    """A deployment that declares a `release` block, over ``state``."""
    monkeypatch.setattr(
        deployment_release,
        "resolve_deployment",
        lambda name: SimpleNamespace(release={"channel": "dev"}),
    )
    monkeypatch.setattr(
        deployment_release,
        "load_release_contract",
        lambda manifest: ReleaseContract(channel="dev", pin=None),
    )
    monkeypatch.setattr(
        deployment_release,
        "current_release_artifact",
        lambda manifest: _artifact(),
    )
    monkeypatch.setattr(
        deployment_release,
        "load_database_release_state",
        lambda dsn: state,
    )


def _run_mcp_serve(monkeypatch, capsys, pending_ids: tuple[str, ...]):
    import okg.substrate.cli.dsn_authority as dsn_authority
    import okg.substrate.db.readiness as readiness_mod
    import okg.substrate.mcp.server as server_mod
    from okg.substrate.cli import main as cli_main

    started: list[dict] = []
    monkeypatch.setattr(
        dsn_authority,
        "resolve_read_dsn",
        lambda args, *, surface: SimpleNamespace(
            dsn="postgres://test", authority={"mode": "test"}, target="test"
        ),
    )
    monkeypatch.setattr(
        readiness_mod,
        "require_database_ready",
        lambda dsn, **kwargs: readiness_mod.DatabaseReadiness(
            schema_present=True,
            schema_applied=True,
            ledger_present=True,
            pending_migrations=pending_ids,
            pending_migration_count=len(pending_ids),
        ),
    )
    monkeypatch.setattr(
        readiness_mod, "require_mcp_read_dsn_usable", lambda **kwargs: None
    )
    # The pre-start role-credential check (issue #3273) is another real
    # collaborator the fake DSN cannot satisfy; its refusals are tested in
    # test_local_database_roles.py.
    import okg.substrate.local_database_roles as roles_mod

    monkeypatch.setattr(
        roles_mod, "require_role_dsn_usable", lambda dsn, **kwargs: None,
    )
    monkeypatch.setattr(
        server_mod, "run_server", lambda **kwargs: started.append(kwargs)
    )
    args = argparse.Namespace(
        deployment="dep",
        dsn=None,
        embedder=None,
        branch="default",
        allow_writes=False,
        transport="stdio",
        host="127.0.0.1",
        port=8100,
        allowed_host=[],
        auth_token_env=None,
        auth_token=None,
    )
    code = cli_main._cmd_mcp_serve(args, as_json=True)
    captured = capsys.readouterr()
    return code, captured.err, started


def _run_doctor_database(monkeypatch, capsys, *, as_json: bool = True):
    import okg.substrate.db.management as management
    from okg.substrate.cli import doctor

    doctor._load_main_helpers()
    monkeypatch.setattr(
        doctor, "_resolve_deployment_dsn", lambda args: "postgres://test"
    )
    monkeypatch.setattr(
        management,
        "database_health_report",
        lambda dsn, *, deployment_name=None: {
            "ok": True,
            "status": "OK",
            "database": "test",
            "deployment": deployment_name,
            "findings": [],
            "summary": {
                "finding_count": 0,
                "error_count": 0,
                "warning_count": 0,
            },
        },
    )
    code = doctor._doctor_database(
        argparse.Namespace(deployment="dep", dsn=None), as_json=as_json
    )
    out = capsys.readouterr().out
    return code, (json.loads(out) if as_json else out)


def _last_json_line(text: str) -> dict:
    return json.loads(text.strip().splitlines()[-1])


# --- the promise test -------------------------------------------------------


def test_one_undeclared_additive_migration_gets_one_refusal_everywhere(
    monkeypatch, capsys
):
    """Release check, mcp-serve and doctor refuse with the same reason.

    On the parent commit mcp-serve printed a "continuing" warning before it
    refused, and doctor called the same database healthy.
    """
    state = _db_state(_UNDECLARED)
    _declare_release(monkeypatch, state)

    # Gather all three answers first, then compare, so a failure shows every
    # disagreement at once rather than the first one only.
    code, stderr, started = _run_mcp_serve(
        monkeypatch, capsys, ("0099_add_widget_table",)
    )
    error = _last_json_line(stderr)["error"]
    doctor_code, payload = _run_doctor_database(monkeypatch, capsys)
    release = [
        f for f in payload["findings"] if f.get("subsystem") == "release"
    ]
    report = deployment_release.build_release_report(
        deployment="dep", dsn="postgres://test", process_role="cli"
    )

    observed = {
        "mcp_serve_exit": code,
        "mcp_serve_started": bool(started),
        "mcp_serve_said_continuing": "continuing" in stderr,
        "mcp_serve_verdict_reason": error.get("data", {}).get(
            "verdict_reason"
        ),
        "doctor_exit": doctor_code,
        "doctor_release_findings": [
            (f.get("severity"), f.get("verdict"), f.get("code"))
            for f in release
        ],
        "release_verdict": report.get("verdict"),
        "release_verdict_reason": report.get("verdict_reason"),
    }
    reason = "schema_migrations_pending"
    assert observed == {
        "mcp_serve_exit": 1,
        "mcp_serve_started": False,
        "mcp_serve_said_continuing": False,
        "mcp_serve_verdict_reason": reason,
        "doctor_exit": 1,
        "doctor_release_findings": [("error", "refuse", reason)],
        "release_verdict": "refuse",
        "release_verdict_reason": reason,
    }, f"stderr={stderr!r}"

    # One sentence, word for word, on every surface.
    message = report["refusal_message"]
    assert "0099_add_widget_table" in message
    assert "reads_safe_behind" in message
    assert error["message"] == message
    assert release[0]["observed"] == message
    assert payload["ok"] is False


# --- the verdict rules ------------------------------------------------------


def test_no_problems_serves() -> None:
    report = _report()
    assert report["verdict"] == "serve"
    assert report["verdict_message"] is None
    assert report["schema_behind_by"] == 0


def test_a_pending_destructive_cleanup_still_serves_and_is_named() -> None:
    report = _report(_DESTRUCTIVE)
    assert report["ok"] is True
    assert report["verdict"] == "serve"
    assert "retire_legacy_runtime" in report["verdict_message"]
    assert report["schema_behind_by"] == 0


def test_every_additive_migration_declared_gives_read_only() -> None:
    report = _report(_DECLARED, _DESTRUCTIVE)
    # Classification is unchanged: the pending additive migration is still a
    # problem, so `ok` stays False and assert_release_compatible still raises.
    assert report["ok"] is False
    assert [p["kind"] for p in report["problems"]] == [
        "schema_migrations_pending"
    ]
    assert report["verdict"] == "read_only"
    assert report["verdict_reason"] == "schema_behind_reads_safe"
    assert report["schema_behind_by"] == 1
    assert report["behind_migrations"] == ["0098_add_gadget_table"]
    assert "0098_add_gadget_table" in report["verdict_message"]


def test_one_undeclared_among_declared_refuses_and_names_it() -> None:
    report = _report(_DECLARED, _UNDECLARED)
    assert report["verdict"] == "refuse"
    assert report["schema_behind_by"] == 2
    assert report["reads_unsafe_migrations"] == ["0099_add_widget_table"]
    assert "0099_add_widget_table" in report["verdict_message"]


def test_a_declaration_does_not_excuse_any_other_problem() -> None:
    report = evaluate_release_contract(
        deployment="dep",
        contract=ReleaseContract(channel="prod", pin=None),
        current=_artifact(),
        database=_db_state(_DECLARED),
        process_role="cli",
    )
    assert report["verdict"] == "refuse"
    assert report["verdict_reason"] == "schema_migrations_pending"
    assert "release_pin_missing" in {p["kind"] for p in report["problems"]}
    assert "pin" in report["verdict_message"]


def test_a_missing_ledger_refuses_with_an_unknown_distance() -> None:
    report = evaluate_release_contract(
        deployment="dep",
        contract=ReleaseContract(channel="dev", pin=None),
        current=_artifact(),
        database=DatabaseReleaseState(
            ledger_exists=False,
            schema_checksum=None,
            schema_ok=False,
            pending_migrations=(_UNDECLARED,),
            blocking_pending_migrations=(_UNDECLARED,),
            checksum_mismatches=(),
            checksum_compatibilities=(),
        ),
        process_role="cli",
    )
    assert report["verdict"] == "refuse"
    assert report["verdict_reason"] == "schema_ledger_missing"
    # With no ledger every known migration reads as pending; that count is
    # not a distance, so it is reported as unknown rather than a number.
    assert report["schema_behind_by"] is None


def test_read_only_starts_mcp_serve_read_only_and_says_why(monkeypatch, capsys):
    """Slice 8 (PACT unknown-s8-mcp-readonly): mcp-serve serves reads alone."""
    _declare_release(monkeypatch, _db_state(_DECLARED))
    code, stderr, started = _run_mcp_serve(
        monkeypatch, capsys, ("0098_add_gadget_table",)
    )
    assert code == 0, stderr
    (launch,) = started
    assert "continuing" not in stderr, stderr
    assert "read-only" in stderr and "0098_add_gadget_table" in stderr
    assert "schema_behind_by: 1" in stderr
    release = launch["read_only_release"]
    assert release["verdict"] == "read_only"
    assert release["schema_behind_by"] == 1
    assert release["behind_migrations"] == ["0098_add_gadget_table"]


def test_a_serving_release_launches_without_read_only(monkeypatch, capsys):
    _declare_release(monkeypatch, _db_state())
    code, _stderr, started = _run_mcp_serve(monkeypatch, capsys, ())
    assert code == 0
    assert started[0]["read_only_release"] is None


def test_runtime_worker_still_refuses_read_only(monkeypatch, capsys):
    """The worker writes, so a read-only verdict refuses it, by name."""
    _declare_release(monkeypatch, _db_state(_DECLARED))
    code, stderr, started = _run_runtime_worker(
        monkeypatch, capsys, ("0098_add_gadget_table",)
    )
    error = _last_json_line(stderr)["error"]
    assert (code, started) == (1, [])
    assert error["data"]["verdict"] == "read_only"
    assert "only `okg mcp-serve` serves this database" in error["message"]


def test_read_only_release_from_report_refuses_a_malformed_report() -> None:
    from okg.substrate.mcp.server import read_only_release_from_report

    good = _report(_DECLARED)
    normalized = read_only_release_from_report(good)
    assert read_only_release_from_report(normalized) == normalized
    for bad in (
        _report(_UNDECLARED),  # verdict refuse
        {**good, "schema_behind_by": None},
        {**good, "schema_behind_by": 0},
        {**good, "schema_behind_by": True},
        {**good, "behind_migrations": []},
        {**good, "verdict_message": None},
    ):
        with pytest.raises(ValueError):
            read_only_release_from_report(bad)


def test_a_served_release_replaces_the_readiness_line(monkeypatch, capsys):
    """Declared release, destructive cleanup pending: serve, one note line."""
    _declare_release(monkeypatch, _db_state(_DESTRUCTIVE))
    code, stderr, started = _run_mcp_serve(
        monkeypatch, capsys, ("retire_legacy_runtime",)
    )
    assert code == 0
    assert len(started) == 1
    assert "continuing" not in stderr, stderr
    assert "retire_legacy_runtime" in stderr


def test_no_declared_release_keeps_the_2128_report_and_continue(
    monkeypatch, capsys
):
    """The #2128 decision stands where no release contract is declared."""
    monkeypatch.setattr(
        deployment_release,
        "resolve_deployment",
        lambda name: SimpleNamespace(release=None),
    )
    code, stderr, started = _run_mcp_serve(
        monkeypatch, capsys, ("0099_add_widget_table",)
    )
    assert code == 0
    assert len(started) == 1
    assert "continuing" in stderr
    assert "0099_add_widget_table" in stderr


def test_doctor_without_a_declared_release_adds_no_release_finding(
    monkeypatch, capsys
):
    monkeypatch.setattr(
        deployment_release,
        "resolve_deployment",
        lambda name: SimpleNamespace(release=None),
    )
    code, payload = _run_doctor_database(monkeypatch, capsys)
    assert code == 0
    assert payload["findings"] == []


def test_read_only_is_one_answer_across_surfaces(monkeypatch, capsys):
    """Under read_only, doctor, mcp-serve and the release report agree.

    PACT unknown-s8-mcp-readonly: mcp-serve serves reads, so doctor reports
    read_only as a WARNING (exit 0) with the release report's sentence, and
    `okg deployment release` still exits 1 because `ok` is unchanged.
    """
    state = _db_state(_DECLARED)
    _declare_release(monkeypatch, state)
    report = deployment_release.build_release_report(
        deployment="dep", dsn="postgres://test", process_role="cli"
    )
    code, stderr, started = _run_mcp_serve(
        monkeypatch, capsys, ("0098_add_gadget_table",)
    )
    doctor_code, payload = _run_doctor_database(monkeypatch, capsys)
    (finding,) = payload["findings"]

    assert report["ok"] is False  # `okg deployment release` exits 1
    assert (code, len(started)) == (0, 1), stderr
    assert doctor_code == 0
    assert finding["severity"] == "warning"
    assert finding["verdict"] == "read_only"
    assert finding["schema_behind_by"] == 1
    assert "not yet available" not in report["refusal_message"]
    assert "only `okg mcp-serve` serves" in report["refusal_message"]
    assert finding["observed"] == report["refusal_message"]
    assert report["verdict_message"] in stderr


_INGEST_LINE = "ingest refuses until migrated (run `okg up dep`)"


@pytest.mark.parametrize(
    ("pending", "expect_line"),
    [
        ((_DECLARED,), True),  # read_only
        ((_UNDECLARED,), False),  # refuse
        ((_DESTRUCTIVE,), False),  # serve, with a named cleanup
    ],
    ids=["read_only", "refuse", "serve"],
)
def test_doctor_says_ingest_refuses_only_under_read_only(
    monkeypatch, capsys, pending, expect_line,
):
    """Option 1 of the 2026-09-28 decision: the read_only finding stays a warning
    and names what does not work, in text and JSON alike."""
    _declare_release(monkeypatch, _db_state(*pending))
    _code, payload = _run_doctor_database(monkeypatch, capsys)
    _code, text = _run_doctor_database(monkeypatch, capsys, as_json=False)
    (finding,) = [
        f for f in payload["findings"] if f.get("subsystem") == "release"
    ]
    assert (finding.get("ingest") == _INGEST_LINE) is expect_line, finding
    assert (_INGEST_LINE in text) is expect_line, text
    if expect_line:
        assert finding["severity"] == "warning"


def test_doctor_reports_an_unreadable_release_and_keeps_the_health_report(
    monkeypatch, capsys
):
    def _unreadable(dsn):
        raise SubstrateError(
            "release state unreadable: permission denied",
            fix_hint="grant the doctor role read access",
        )

    _declare_release(monkeypatch, _db_state())
    monkeypatch.setattr(
        deployment_release, "load_database_release_state", _unreadable
    )
    code, payload = _run_doctor_database(monkeypatch, capsys)
    assert code == 1
    assert payload["database"] == "test"  # the health payload still printed
    (finding,) = payload["findings"]
    assert finding["severity"] == "error"
    assert finding["code"] == "release_unverifiable"
    assert "permission denied" in finding["observed"]
    assert finding["fix_hint"] == "grant the doctor role read access"


def _run_runtime_worker(monkeypatch, capsys, pending_ids: tuple[str, ...]):
    import okg.substrate.db.readiness as readiness_mod
    import okg.substrate.deployments as deployments_mod
    import okg.substrate.deployments.admission as admission_mod
    import okg.substrate.runtime.worker as worker_mod
    from okg.substrate.cli import runtime as runtime_cli

    started: list[dict] = []
    monkeypatch.setattr(
        admission_mod, "require_active_deployment", lambda name: None
    )
    monkeypatch.setattr(
        deployments_mod,
        "resolve_deployment",
        lambda name: SimpleNamespace(
            runtime=SimpleNamespace(enabled=True), release={"channel": "dev"}
        ),
    )
    monkeypatch.setattr(
        readiness_mod,
        "require_database_ready",
        lambda dsn, **kwargs: readiness_mod.DatabaseReadiness(
            schema_present=True,
            schema_applied=True,
            ledger_present=True,
            pending_migrations=pending_ids,
            pending_migration_count=len(pending_ids),
        ),
    )
    monkeypatch.setattr(
        worker_mod, "run_worker", lambda **kwargs: started.append(kwargs) or 0
    )
    monkeypatch.delenv("OKG_DSN", raising=False)
    args = argparse.Namespace(
        rt_subcmd="worker",
        deployment="dep",
        dsn="postgres://test",
        operator=False,
        max_wait_seconds=1.0,
    )
    code = runtime_cli._cmd_runtime(args, as_json=True)
    captured = capsys.readouterr()
    return code, captured.err, started


def test_runtime_worker_refuses_once_without_continuing(monkeypatch, capsys):
    """`okg runtime worker` had the same warn-then-refuse order as mcp-serve."""
    state = _db_state(_UNDECLARED)
    _declare_release(monkeypatch, state)
    code, stderr, started = _run_runtime_worker(
        monkeypatch, capsys, ("0099_add_widget_table",)
    )
    report = deployment_release.build_release_report(
        deployment="dep", dsn="postgres://test", process_role="cli"
    )
    error = _last_json_line(stderr)["error"]
    observed = {
        "exit": code,
        "started": bool(started),
        "said_continuing": "continuing" in stderr,
        "verdict_reason": error.get("data", {}).get("verdict_reason"),
    }
    assert observed == {
        "exit": 1,
        "started": False,
        "said_continuing": False,
        "verdict_reason": "schema_migrations_pending",
    }, f"stderr={stderr!r}"
    assert error["message"] == report["refusal_message"]


def test_enforce_still_refuses_read_only() -> None:
    """`ok` is unchanged, so the generic enforcement still refuses."""
    with pytest.raises(SubstrateError) as caught:
        deployment_release.assert_release_compatible(
            deployment="dep",
            contract=ReleaseContract(channel="dev", pin=None),
            current=_artifact(),
            database=_db_state(_DECLARED),
            process_role="cli",
        )
    assert caught.value.data["verdict"] == "read_only"


# The whole-schema snapshot row, as `migration_status` reports its drift.
_SNAPSHOT_ROW_DRIFT = {
    "migration_id": "0000_schema_snapshot",
    "recorded_checksum": "schema-old",
    "expected_checksum": "schema-new",
    "destructive": False,
}
_EDITED_MIGRATION_DRIFT = {
    "migration_id": "0007_add_thing",
    "recorded_checksum": "edit-old",
    "expected_checksum": "edit-new",
    "destructive": False,
}


def _drifted_report(
    *pending: dict,
    mismatches: tuple = (_SNAPSHOT_ROW_DRIFT,),
    recorded_snapshot: str = "schema-old",
) -> dict:
    """A database whose recorded schema fingerprint differs from the code's."""
    state = dataclasses.replace(
        _db_state(*pending),
        schema_checksum=recorded_snapshot,
        checksum_mismatches=mismatches,
    )
    return evaluate_release_contract(
        deployment="dep",
        contract=ReleaseContract(channel="dev", pin=None),
        current=_artifact(),
        database=state,
        process_role="cli",
    )


@pytest.mark.parametrize(
    ("mismatches", "recorded_snapshot"),
    [
        ((_SNAPSHOT_ROW_DRIFT,), "schema-old"),  # both comparisons differ
        ((_SNAPSHOT_ROW_DRIFT,), "schema-new"),  # the ledger row alone
        ((), "schema-old"),  # the snapshot comparison alone
    ],
    ids=["both", "ledger-row-only", "snapshot-only"],
)
def test_declared_behind_with_any_fingerprint_drift_refuses(
    mismatches: tuple, recorded_snapshot: str,
) -> None:
    """Fail closed (decision 2026-09-28). schema.sql and views.sql also carry
    views, functions, grants and policies the ledger does not describe, so a
    declared reads-safe migration never excuses a fingerprint difference.
    Per-migration fingerprints are follow-up #2797."""
    report = _drifted_report(
        _DECLARED, mismatches=mismatches, recorded_snapshot=recorded_snapshot,
    )
    assert report["verdict"] == "refuse", report["verdict_message"]
    assert "schema_checksum_mismatch" in {p["kind"] for p in report["problems"]}
    assert report["behind_migrations"] == ["0098_add_gadget_table"]


def test_read_only_needs_an_exact_fingerprint_match_and_names_okg_up() -> None:
    report = _report(_DECLARED)
    assert report["verdict"] == "read_only"
    # One command across mcp-serve, doctor and the report.
    assert report["fix_hint"] == "run `okg up dep`"


def test_pending_migrations_hint_does_not_say_create_the_ledger() -> None:
    report = _report(_UNDECLARED)
    assert report["verdict"] == "refuse"
    assert "apply the pending schema migrations" in report["fix_hint"]
    assert "create" not in report["fix_hint"]
    mixed = _drifted_report(_UNDECLARED)["fix_hint"]
    assert "no applied schema ledger" not in mixed


def test_snapshot_drift_with_nothing_pending_still_refuses() -> None:
    report = _drifted_report()
    assert report["verdict"] == "refuse"
    assert report["verdict_reason"] == "schema_checksum_mismatch"


def test_an_edited_applied_migration_still_refuses_behind_declared_ones() -> None:
    report = _drifted_report(
        _DECLARED, mismatches=(_SNAPSHOT_ROW_DRIFT, _EDITED_MIGRATION_DRIFT),
    )
    assert report["verdict"] == "refuse"
    assert "schema_checksum_mismatch" in {p["kind"] for p in report["problems"]}


def test_an_undeclared_migration_with_snapshot_drift_still_refuses() -> None:
    report = _drifted_report(_DECLARED, _UNDECLARED)
    assert report["verdict"] == "refuse"
    assert report["reads_unsafe_migrations"] == ["0099_add_widget_table"]
