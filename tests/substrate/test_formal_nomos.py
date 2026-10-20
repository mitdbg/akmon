"""REPORT-SCHEMA tests for the Nomos formal checker wrapper.

These tests install a fake ``java`` shell stub that echoes canned TLC output;
they verify the checker's report/exit-code plumbing and config naming, NOT any
property of the Nomos model. Real model proof comes from the gating aggregate
runner (scripts/run_formal_suite.py) executing real TLC in CI.
"""
from __future__ import annotations

import stat
from pathlib import Path

import yaml

FORMAL_REPORT_SCHEMA_TEST = True


EXPECTED_COUNTEREXAMPLES = {
    "NomosMissingPolicyCounterexample.cfg": "BrokenMissingPolicyCanAllow",
    "NomosWeakDeploymentPostureCounterexample.cfg": (
        "BrokenWeakDeploymentPostureCanAllow"
    ),
    "NomosUnscopedIdentityCounterexample.cfg": (
        "BrokenUnscopedIdentityClaimCanGrant"
    ),
    "NomosUnknownActionCounterexample.cfg": "BrokenUnknownActionCanAllow",
    "NomosPrivateExportCounterexample.cfg": (
        "BrokenPrivateExportWithoutScopedClaimCanPublish"
    ),
    "NomosSourceDeploymentMismatchCounterexample.cfg": (
        "BrokenSourceDeploymentMismatchCanAllow"
    ),
}


def _fake_tlc(
    tmp_path: Path,
    monkeypatch,
    *,
    stdout: str,
    returncode: int,
) -> None:
    fake_jar = tmp_path / "tla2tools.jar"
    fake_jar.write_text("fake jar", encoding="utf-8")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    java = bin_dir / "java"
    java.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\n' {stdout!r}\n"
        f"exit {returncode}\n",
        encoding="utf-8",
    )
    java.chmod(java.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("TLA2TOOLS_JAR", str(fake_jar))
    monkeypatch.setenv("PATH", str(bin_dir))


def _fake_counterexample_tlc(
    tmp_path: Path,
    monkeypatch,
    *,
    wrong_invariant: bool = False,
    runtime_error: bool = False,
) -> None:
    fake_jar = tmp_path / "tla2tools.jar"
    fake_jar.write_text("fake jar", encoding="utf-8")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    java = bin_dir / "java"
    cases = "\n".join(
        f"  *{config}*) echo 'Error: Invariant "
        f"{'SomeOtherInvariant' if wrong_invariant else invariant} "
        "is violated.' ;;"
        for config, invariant in EXPECTED_COUNTEREXAMPLES.items()
    )
    runtime_error_line = (
        "echo 'Error: TLC threw an unexpected exception.' >&2\n"
        if runtime_error
        else ""
    )
    java.write_text(
        (
            "#!/bin/sh\n"
            "case \"$*\" in\n"
            f"{cases}\n"
            "  *) echo 'Error: Invariant UnexpectedConfig is violated.' ;;\n"
            "esac\n"
            f"{runtime_error_line}"
            "exit 1\n"
        ),
        encoding="utf-8",
    )
    java.chmod(java.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("TLA2TOOLS_JAR", str(fake_jar))
    monkeypatch.setenv("PATH", str(bin_dir))


def _rows(report_path: Path) -> list[dict]:
    report = yaml.safe_load(report_path.read_text(encoding="utf-8"))
    return report["results"]


def test_nomos_formal_checker_emits_strict_contract_report(
    tmp_path,
    monkeypatch,
) -> None:
    from scripts import check_formal_nomos

    _fake_tlc(
        tmp_path,
        monkeypatch,
        stdout="Model checking completed. No error has been found.\n"
        "200000000 states generated, 99000000 distinct states found, 0 states left on queue.",
        returncode=0,
    )

    out = tmp_path / "out"
    rc = check_formal_nomos.main(["--out", str(out)])

    assert rc == 0
    rows = _rows(out / "formal-report.yaml")
    assert {row["property_id"] for row in rows} == {"NomosControlPlaneContract"}
    row = rows[0]
    assert row["kind"] == "formal_check"
    assert row["status"] == "pass"
    assert row["fresh"] is True
    # Repo paths are stamped RELATIVE so the recorded command stays portable.
    assert row["checker_command"] == (
        "uv run python scripts/check_formal_nomos.py "
        "--profile full --config Nomos.cfg "
        f"--out {out} --property-id NomosControlPlaneContract "
        "--timeout-seconds 900"
    )
    assert row["execution_profile"] == "full"
    assert row["profile_timeout_seconds"] == 900
    assert row["proof_status"] == "freshly_proven"
    assert set(row["refinement_tests"]) >= {
        "tests/substrate/test_formal_nomos.py",
        "tests/substrate/nomos/test_nomos_core.py",
        "tests/substrate/nomos/test_nomos_institutional_iam.py",
        "tests/substrate/nomos/test_nomos_identity_store.py",
        "tests/substrate/nomos/test_nomos_institutional_iam_postgres.py",
    }
    assert Path(row["artifact"]).is_file()
    assert row["module_digest"].startswith("sha256:")
    assert row["config_digest"].startswith("sha256:")


def test_nomos_formal_checker_can_emit_source_lineage_report(
    tmp_path,
    monkeypatch,
) -> None:
    from scripts import check_formal_nomos

    _fake_tlc(
        tmp_path,
        monkeypatch,
        stdout="Model checking completed. No error has been found.\n"
        "200000000 states generated, 99000000 distinct states found, 0 states left on queue.",
        returncode=0,
    )

    out = tmp_path / "out"
    rc = check_formal_nomos.main(["--profile", "smoke", "--out", str(out)])

    assert rc == 0
    rows = _rows(out / "formal-report.yaml")
    assert {row["property_id"] for row in rows} == {
        "formal.nomos-source-lineage.visibility"
    }
    row = rows[0]
    assert row["checker_command"] == (
        "uv run python scripts/check_formal_nomos.py "
        "--profile smoke --config NomosSourceLineage.cfg "
        f"--out {out} --property-id formal.nomos-source-lineage.visibility "
        "--timeout-seconds 30"
    )
    assert row["execution_profile"] == "smoke"
    assert row["profile_timeout_seconds"] == 30
    assert set(row["refinement_tests"]) >= {
        "tests/substrate/test_formal_nomos.py",
        "tests/substrate/nomos/test_nomos_institutional_iam.py",
        "tests/substrate/nomos/test_nomos_institutional_iam_postgres.py",
        "tests/substrate/nomos/test_nomos_source_lineage_derived_closure.py",
        "tests/substrate/nomos/test_nomos_source_lineage_read_filter.py",
    }
    assert Path(row["artifact"]).is_file()
    assert row["module_digest"].startswith("sha256:")
    assert row["config_digest"].startswith("sha256:")


def test_nomos_formal_checker_can_emit_fine_grained_access_report(
    tmp_path,
    monkeypatch,
) -> None:
    from scripts import check_formal_nomos

    _fake_tlc(
        tmp_path,
        monkeypatch,
        stdout="Model checking completed. No error has been found.\n"
        "200000000 states generated, 99000000 distinct states found, 0 states left on queue.",
        returncode=0,
    )

    out = tmp_path / "out"
    rc = check_formal_nomos.main(
        [
            "--profile",
            "smoke",
            "--out",
            str(out),
            "--property-id",
            "formal.nomos-fgac.fine-grained-lineage",
        ]
    )

    assert rc == 0
    rows = _rows(out / "formal-report.yaml")
    assert {row["property_id"] for row in rows} == {
        "formal.nomos-fgac.fine-grained-lineage"
    }
    row = rows[0]
    assert row["checker_command"] == (
        "uv run python scripts/check_formal_nomos.py "
        "--profile smoke --config NomosFineGrainedAccess.cfg "
        f"--out {out} --property-id formal.nomos-fgac.fine-grained-lineage "
        "--timeout-seconds 30"
    )
    assert set(row["refinement_tests"]) >= {
        "tests/substrate/nomos/test_nomos_fine_grained_access_control.py",
        "tests/substrate/nomos/test_nomos_source_lineage_read_filter.py",
    }
    assert Path(row["artifact"]).is_file()
    assert row["module_digest"].startswith("sha256:")
    assert row["config_digest"].startswith("sha256:")


def test_source_lineage_formal_config_names_rollout_denial_cases() -> None:
    config = Path("formal/nomos/NomosSourceLineage.cfg").read_text(
        encoding="utf-8"
    )

    for request_name in {
        "sourceLineagePartialRead",
        "sourceLineageMalformedRead",
        "sourceLineageMissingRead",
        "sourceLineageUnknownRead",
        "sourceLineageStalePolicyRead",
    }:
        assert request_name in config
    assert "UnknownSourceLineageRequests = {sourceLineageUnknownRead}" in config
    assert (
        "StaleSourcePolicyRequests = {sourceLineageStalePolicyRead}" in config
    )


def test_fine_grained_formal_config_names_access_denial_cases() -> None:
    config = Path("formal/nomos/NomosFineGrainedAccess.cfg").read_text(
        encoding="utf-8"
    )

    for request_name in {
        "fineGrainedAllowedRead",
        "fineGrainedUnaddressableRead",
        "fineGrainedFutureHorizonRead",
        "fineGrainedGraphDeniedRead",
        "fineGrainedDefaultAllowRead",
    }:
        assert request_name in config
    assert "FineGrainedDefaultAllowRequests = {fineGrainedDefaultAllowRead}" in config
    assert "NomosFineGrainedVisibility" in config


def test_nomos_formal_config_names_iam_binding_and_grant_relations() -> None:
    config = Path("formal/nomos/Nomos.cfg").read_text(encoding="utf-8")
    model = Path("formal/nomos/Nomos.tla").read_text(encoding="utf-8")
    inventory = Path("formal/gap-inventory.toml").read_text(encoding="utf-8")

    for expected in {
        "bindingCernReader",
        "bindingCernOperator",
        "ScopedClaims = {validCernClaim}",
        "ActiveSubjectBindings = {",
        "TrustedProviderBindings = {",
        "ReaderSubjectBindings = {bindingCernReader}",
        "OperatorSubjectBindings = {bindingCernOperator}",
        "DefaultScopeBindings = {",
        "ActivePermissionGrants = {",
        "ReaderSubjectGrants = {grantPostgresRead}",
        "OperatorSubjectGrants = {",
        "DefaultScopeGrants = {",
        "GraphReadMcpGrants = {grantPostgresRead}",
        "McpRecordGrants = {grantPostgresRecord}",
        "PorosSubmitGrants = {grantPostgresPoros}",
        "PublicDataGrants = {grantPostgresRead}",
        "InternalDataGrants = {",
        "AllowGrants = {",
    }:
        assert expected in config
    public_requests_block = config.split("  PublicRequests = {", 1)[1].split(
        "  RestrictedRequests",
        1,
    )[0]
    assert "postgresRead" in public_requests_block
    assert "postgresRecord" not in public_requests_block
    assert "postgresPoros" not in public_requests_block
    assert "SubjectBoundFor(req)" in model
    assert "GrantMatches(req, decisions[req].permissionGrant)" in model
    assert "ActiveSubjectBindings" in model
    assert "ActivePermissionGrants" in model
    assert "GrantActionMatches(req, grant)" in model
    assert "GrantClassificationMatches(req, grant)" in model
    # The Nomos model now carries first-class IAM provider/claim/binding/grant
    # /scope relations (asserted above), but deployment-specific identity-provider
    # and subject-binding semantics remain a named *runtime boundary precondition*:
    # `nomos.iam-scope-too-abstract` stays in the formal gap ledger, cross-
    # referenced by the `nomos.iam_scope` `needs_formal_model` boundary in
    # formal/boundary-inventory.toml, until refined or proven by deployment-
    # specific tests. The authoritative design-contract gate
    # (tests/docs/test_formal_design_contract.py) requires this gap entry to be
    # present, so the model coverage and the open boundary precondition coexist.
    assert "nomos.iam-scope-too-abstract" in inventory


def test_nomos_formal_checker_writes_expected_counterexample(
    tmp_path,
    monkeypatch,
) -> None:
    from scripts import check_formal_nomos

    _fake_counterexample_tlc(tmp_path, monkeypatch)

    out = tmp_path / "out"
    rc = check_formal_nomos.main([
        "--model",
        "NomosCounterexample.tla",
        "--config",
        "NomosCounterexample.cfg",
        "--out",
        str(out),
        "--expect-counterexample",
    ])

    assert rc == 0
    rows = _rows(out / "formal-report.yaml")
    assert {row["property_id"] for row in rows} == {
        "NomosNonVacuityMissingPolicy",
        "NomosNonVacuityWeakDeploymentPosture",
        "NomosNonVacuityUnscopedIdentityClaim",
        "NomosNonVacuityUnknownAction",
        "NomosNonVacuityPrivateExportWithoutScopedClaim",
        "NomosNonVacuitySourceDeploymentMismatch",
    }
    assert {row["status"] for row in rows} == {"pass"}
    artifact = out / "counterexample.txt"
    assert artifact.is_file()
    text = artifact.read_text(encoding="utf-8")
    for invariant in EXPECTED_COUNTEREXAMPLES.values():
        assert invariant in text


def test_nomos_formal_checker_rejects_wrong_counterexample_invariant(
    tmp_path,
    monkeypatch,
) -> None:
    from scripts import check_formal_nomos

    _fake_counterexample_tlc(tmp_path, monkeypatch, wrong_invariant=True)

    out = tmp_path / "out"
    rc = check_formal_nomos.main([
        "--model",
        "NomosCounterexample.tla",
        "--config",
        "NomosCounterexample.cfg",
        "--out",
        str(out),
        "--expect-counterexample",
    ])

    assert rc == 1
    rows = _rows(out / "formal-report.yaml")
    assert {row["property_id"] for row in rows} == {
        "NomosNonVacuityMissingPolicy",
        "NomosNonVacuityWeakDeploymentPosture",
        "NomosNonVacuityUnscopedIdentityClaim",
        "NomosNonVacuityUnknownAction",
        "NomosNonVacuityPrivateExportWithoutScopedClaim",
        "NomosNonVacuitySourceDeploymentMismatch",
    }
    assert {row["status"] for row in rows} == {"fail"}
    artifact = out / "counterexample.txt"
    assert artifact.is_file()
    text = artifact.read_text(encoding="utf-8")
    assert "SomeOtherInvariant" in text
    for invariant in EXPECTED_COUNTEREXAMPLES.values():
        assert invariant in text


def test_nomos_formal_checker_rejects_counterexample_with_tlc_runtime_error(
    tmp_path,
    monkeypatch,
) -> None:
    from scripts import check_formal_nomos

    _fake_counterexample_tlc(tmp_path, monkeypatch, runtime_error=True)

    out = tmp_path / "out"
    rc = check_formal_nomos.main([
        "--model",
        "NomosCounterexample.tla",
        "--config",
        "NomosCounterexample.cfg",
        "--out",
        str(out),
        "--expect-counterexample",
    ])

    assert rc == 1
    rows = _rows(out / "formal-report.yaml")
    assert {row["property_id"] for row in rows} == {
        "NomosNonVacuityMissingPolicy",
        "NomosNonVacuityWeakDeploymentPosture",
        "NomosNonVacuityUnscopedIdentityClaim",
        "NomosNonVacuityUnknownAction",
        "NomosNonVacuityPrivateExportWithoutScopedClaim",
        "NomosNonVacuitySourceDeploymentMismatch",
    }
    assert {row["status"] for row in rows} == {"fail"}
    assert {
        "Unexpected TLC error" in row["summary"]
        for row in rows
    } == {True}


def test_nomos_formal_checker_expect_counterexample_fails_when_tlc_passes(
    tmp_path,
    monkeypatch,
) -> None:
    from scripts import check_formal_nomos

    _fake_tlc(
        tmp_path,
        monkeypatch,
        stdout="Model checking completed. No error has been found.\n"
        "200000000 states generated, 99000000 distinct states found, 0 states left on queue.",
        returncode=0,
    )

    rc = check_formal_nomos.main([
        "--model",
        "NomosCounterexample.tla",
        "--config",
        "NomosCounterexample.cfg",
        "--out",
        str(tmp_path / "out"),
        "--expect-counterexample",
    ])

    assert rc == 1


def test_nomos_formal_model_covers_canonical_vocabulary() -> None:
    from okg.substrate.nomos import vocabulary

    model = Path("formal/nomos/Nomos.tla").read_text(encoding="utf-8")
    for term_set in (
        vocabulary.ACTIONS,
        vocabulary.DATA_CLASSIFICATIONS,
        vocabulary.DEPLOYMENT_CLASSES,
        vocabulary.EFFECTS,
        vocabulary.OBLIGATION_TYPES,
        vocabulary.PRINCIPAL_KINDS,
        vocabulary.RESOURCE_KINDS,
        vocabulary.SOURCE_SENSITIVITIES,
    ):
        for term in term_set:
            assert f'"{term}"' in model


def test_nomos_counterexample_names_required_control_plane_witnesses() -> None:
    model = Path("formal/nomos/NomosCounterexample.tla").read_text(
        encoding="utf-8"
    )
    config = Path("formal/nomos/NomosCounterexample.cfg").read_text(
        encoding="utf-8"
    )

    for invariant in {
        "BrokenMissingPolicyCanAllow",
        "BrokenWeakDeploymentPostureCanAllow",
        "BrokenUnscopedIdentityClaimCanGrant",
        "BrokenUnknownActionCanAllow",
        "BrokenPrivateExportWithoutScopedClaimCanPublish",
        "BrokenSourceDeploymentMismatchCanAllow",
        "NomosControlPlaneNonVacuity",
    }:
        assert invariant in model
    assert "NomosControlPlaneNonVacuity" in config
    for config_name in EXPECTED_COUNTEREXAMPLES:
        assert Path("formal/nomos", config_name).is_file()
