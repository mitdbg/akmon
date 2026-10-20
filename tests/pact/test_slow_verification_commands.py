"""PACT commands and evidence cannot prove slow tests they never executed."""

from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.pact.models import (
    AcceptanceCriterion,
    Change,
    Evidence,
    PactManifest,
    Requirement,
    Task,
    TestPlanEntry as PactTestPlanEntry,
)
from okg.substrate.pact.operations import attach_evidence
from okg.substrate.pact.store import load_manifest, save_manifest
from okg.substrate.pact.validation import (
    _slow_pytest_command_issues,
    derive_alignment,
    validate_manifest,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def _root(tmp_path: Path) -> Path:
    (tmp_path / "tests").mkdir()
    (tmp_path / "pyproject.toml").write_text(
        "[tool.pytest.ini_options]\naddopts = '-m \"not slow\"'\n",
        encoding="utf-8",
    )
    (tmp_path / "tests" / "test_sample.py").write_text(
        """import pytest

@pytest.mark.slow
def test_slow():
    pass

def test_fast():
    pass
""",
        encoding="utf-8",
    )
    return tmp_path


def _manifest(
    command: str,
    *,
    test_id: str = "tests/test_sample.py::test_slow",
    evidence: list[Evidence] | None = None,
    verification: str = "Run the planned command.",
) -> PactManifest:
    requirement_id = "req.slow-proof"
    acceptance_id = "acc.slow-proof"
    return PactManifest(
        pact_version=4,
        target_pact_version=4,
        change=Change(id="slow-proof", title="Slow proof fixture"),
        requirements=[
            Requirement(
                id=requirement_id,
                title="Named slow proof",
                statement="The named test must execute.",
                evidence_policy={"required": ["executed_test"]},
                acceptance=[
                    AcceptanceCriterion(
                        id=acceptance_id,
                        description="The named assertion passed.",
                        evidence=["executed_test"],
                    )
                ],
            )
        ],
        tasks=[
            Task(
                id="task.slow-proof",
                title="Run the proof",
                verification=verification,
                requirements=[requirement_id],
            )
        ],
        test_plan=[
            PactTestPlanEntry(
                id="test.slow-proof",
                requirement=requirement_id,
                test_id=test_id,
                command=command,
                evidence=["executed_test"],
            )
        ],
        evidence=list(evidence or []),
    )


def _slow_codes(manifest: PactManifest, root: Path) -> list[str]:
    return [
        issue.code
        for issue in validate_manifest(manifest, repo_root=root).issues
        if issue.code == "pytest_command_deselects_named_slow_test"
    ]


def test_slow_file_without_marker_override_is_refused(tmp_path: Path) -> None:
    root = _root(tmp_path)
    manifest = _manifest("pytest tests/test_sample.py -q")

    assert _slow_codes(manifest, root) == ["pytest_command_deselects_named_slow_test"]


def test_directly_slow_node_without_marker_override_is_refused(tmp_path: Path) -> None:
    root = _root(tmp_path)
    manifest = _manifest("pytest tests/test_sample.py::test_slow -q")

    assert _slow_codes(manifest, root) == ["pytest_command_deselects_named_slow_test"]


def test_class_qualified_slow_node_without_override_is_refused(tmp_path: Path) -> None:
    root = _root(tmp_path)
    (root / "tests" / "test_class.py").write_text(
        """import pytest

@pytest.mark.slow
class TestProof:
    def test_named(self):
        pass
""",
        encoding="utf-8",
    )
    node = "tests/test_class.py::TestProof::test_named"
    manifest = _manifest(f"pytest {node} -q", test_id=node)

    assert _slow_codes(manifest, root) == ["pytest_command_deselects_named_slow_test"]


def test_multiline_module_slow_marker_without_override_is_refused(
    tmp_path: Path,
) -> None:
    root = _root(tmp_path)
    (root / "tests" / "test_module_slow.py").write_text(
        """import pytest

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.slow,
]

def test_named():
    pass
""",
        encoding="utf-8",
    )
    manifest = _manifest(
        "pytest tests/test_module_slow.py -q",
        test_id="tests/test_module_slow.py::test_named",
    )

    assert _slow_codes(manifest, root) == ["pytest_command_deselects_named_slow_test"]


def test_explicit_marker_expression_selecting_slow_test_overrides_default(
    tmp_path: Path,
) -> None:
    root = _root(tmp_path)
    manifest = _manifest(
        'pytest tests/test_sample.py::test_slow -m "postgres and serial_db" -q'
    )

    assert _slow_codes(manifest, root) == []


def test_python_module_flag_is_not_mistaken_for_pytest_marker(tmp_path: Path) -> None:
    root = _root(tmp_path)
    manifest = _manifest("python -m pytest tests/test_sample.py::test_slow -q")

    assert _slow_codes(manifest, root) == ["pytest_command_deselects_named_slow_test"]


def test_explicit_slow_marker_override_is_accepted(tmp_path: Path) -> None:
    root = _root(tmp_path)
    manifest = _manifest(
        'pytest tests/test_sample.py::test_slow -m "slow or not slow" -q'
    )

    assert _slow_codes(manifest, root) == []


def test_explicit_unrelated_marker_expression_replaces_default(tmp_path: Path) -> None:
    root = _root(tmp_path)
    manifest = _manifest('pytest tests/test_sample.py::test_slow -m "not postgres" -q')

    assert _slow_codes(manifest, root) == []


def test_parenthesized_slow_exclusion_is_refused(tmp_path: Path) -> None:
    root = _root(tmp_path)
    manifest = _manifest('pytest tests/test_sample.py::test_slow -m "not (slow)" -q')

    assert _slow_codes(manifest, root) == ["pytest_command_deselects_named_slow_test"]


def test_short_override_ini_can_clear_default_addopts(tmp_path: Path) -> None:
    root = _root(tmp_path)
    manifest = _manifest("pytest tests/test_sample.py::test_slow -o addopts= -q")

    assert _slow_codes(manifest, root) == []


def test_non_slow_node_in_same_file_is_accepted(tmp_path: Path) -> None:
    root = _root(tmp_path)
    manifest = _manifest(
        "pytest tests/test_sample.py::test_fast -q",
        test_id="tests/test_sample.py::test_fast",
    )

    assert _slow_codes(manifest, root) == []


def test_task_verification_scans_only_an_executable_pytest_command(
    tmp_path: Path,
) -> None:
    root = _root(tmp_path)
    prose = (
        "The negative fixture discusses tests/test_sample.py::test_slow. "
        "For example, Run pytest tests/test_sample.py::test_slow -q."
    )
    manifest = _manifest(
        'pytest tests/test_sample.py::test_slow -m "slow or not slow" -q',
        verification=prose,
    )

    assert _slow_codes(manifest, root) == []


@pytest.mark.parametrize("prefix", ["echo", "printf"])
def test_shell_output_that_mentions_pytest_is_not_treated_as_execution(
    tmp_path: Path,
    prefix: str,
) -> None:
    root = _root(tmp_path)
    manifest = _manifest(
        'pytest tests/test_sample.py::test_slow -m "slow or not slow" -q',
        verification=f'{prefix} "pytest tests/test_sample.py::test_slow -q"',
    )

    assert _slow_codes(manifest, root) == []


def test_manual_passing_attach_cannot_attest_a_named_test(tmp_path: Path) -> None:
    root = _root(tmp_path)
    save_manifest(
        root,
        _manifest('pytest tests/test_sample.py::test_slow -m "slow or not slow" -q'),
    )

    with pytest.raises(SubstrateError, match="pytest-report"):
        attach_evidence(
            root,
            "slow-proof",
            "req.slow-proof",
            kind="pytest",
            ref="junit.xml",
            status="pass",
        )


def test_manual_task_attach_cannot_bypass_acceptance_scoped_named_test(
    tmp_path: Path,
) -> None:
    root = _root(tmp_path)
    manifest = _manifest(
        'pytest tests/test_sample.py::test_slow -m "slow or not slow" -q'
    )
    manifest.test_plan[0].requirement = "acc.slow-proof"
    save_manifest(root, manifest)

    with pytest.raises(SubstrateError, match="pytest-report"):
        attach_evidence(
            root,
            "slow-proof",
            "task.slow-proof",
            kind="pytest",
            ref="junit.xml",
            status="pass",
        )


def test_project_alias_cannot_bypass_manual_named_test_refusal(tmp_path: Path) -> None:
    root = _root(tmp_path)
    (root / "pact").mkdir(exist_ok=True)
    (root / "pact" / "project.yaml").write_text(
        "evidence_kinds:\n  aliases:\n    proof_run: executed_test\n",
        encoding="utf-8",
    )
    save_manifest(
        root,
        _manifest('pytest tests/test_sample.py::test_slow -m "slow or not slow" -q'),
    )

    with pytest.raises(SubstrateError, match="pytest-report"):
        attach_evidence(
            root,
            "slow-proof",
            "task.slow-proof",
            kind="proof_run",
            ref="junit.xml",
            status="pass",
        )


def test_unattested_historical_row_is_visible_but_does_not_satisfy() -> None:
    row = Evidence(
        id="ev.pytest.historical",
        kind="pytest",
        status="pass",
        refs=["req.slow-proof", "acc.slow-proof"],
        summary="Historical green command with no executed-test identity.",
    )
    manifest = _manifest("pytest tests/test_sample.py -q", evidence=[row])

    alignment = derive_alignment(manifest)

    assert manifest.evidence[0].status == "pass"
    assert (
        alignment["requirements"]["req.slow-proof"]["satisfaction_status"]
        == "unsatisfied"
    )
    assert (
        alignment["acceptance"]["acc.slow-proof"]["satisfaction_status"]
        == "unsatisfied"
    )
    assert (
        alignment["test_plan"]["test.slow-proof"]["satisfaction_status"]
        == "unsatisfied"
    )


def test_task_only_historical_row_cannot_bypass_named_test_attestation() -> None:
    row = Evidence(
        id="ev.pytest.task-only",
        kind="pytest",
        status="pass",
        refs=["task.slow-proof"],
        summary="Task-scoped historical row with no JUnit identity.",
    )
    manifest = _manifest("pytest tests/test_sample.py -q", evidence=[row])

    alignment = derive_alignment(manifest)

    assert alignment["tasks"]["task.slow-proof"]["satisfaction_status"] == "unsatisfied"


def test_requirement_rollup_task_ref_does_not_broaden_to_sibling_requirement() -> None:
    row = Evidence(
        id="ev.pytest.requirement-rollup",
        kind="pytest",
        status="pass",
        refs=["req.slow-proof", "task.slow-proof"],
        data={"junit_test_ids": ["tests.test_sample::test_slow"]},
    )
    manifest = _manifest("pytest tests/test_sample.py -q", evidence=[row])
    manifest.requirements.append(
        Requirement(
            id="req.sibling",
            title="Sibling",
            statement="A sibling requirement has a different named test.",
            evidence_policy={"required": ["executed_test"]},
            acceptance=[],
        )
    )
    manifest.tasks[0].requirements.append("req.sibling")
    manifest.test_plan.append(
        PactTestPlanEntry(
            id="test.sibling",
            requirement="req.sibling",
            test_id="tests/test_sample.py::test_fast",
            command="pytest tests/test_sample.py::test_fast -q",
            evidence=["executed_test"],
        )
    )

    alignment = derive_alignment(manifest)

    assert (
        alignment["requirements"]["req.slow-proof"]["satisfaction_status"]
        == "satisfied"
    )


def test_exact_junit_identity_satisfies_the_named_test() -> None:
    row = Evidence(
        id="ev.pytest.report-backed",
        kind="pytest",
        status="pass",
        refs=["req.slow-proof", "acc.slow-proof"],
        summary="The named test passed.",
        data={"junit_test_ids": ["tests.test_sample::test_slow"]},
    )
    manifest = _manifest(
        'pytest tests/test_sample.py -m "slow or not slow" -q',
        evidence=[row],
    )

    alignment = derive_alignment(manifest)

    assert (
        alignment["requirements"]["req.slow-proof"]["satisfaction_status"]
        == "satisfied"
    )
    assert (
        alignment["acceptance"]["acc.slow-proof"]["satisfaction_status"] == "satisfied"
    )
    assert (
        alignment["test_plan"]["test.slow-proof"]["satisfaction_status"] == "satisfied"
    )


def test_legacy_singular_junit_identity_still_satisfies_named_test() -> None:
    row = Evidence(
        id="ev.pytest.legacy-report-backed",
        kind="pytest",
        status="pass",
        refs=["req.slow-proof", "acc.slow-proof"],
        data={"junit_test_id": "tests.test_sample::test_slow"},
    )
    manifest = _manifest(
        'pytest tests/test_sample.py -m "slow or not slow" -q', evidence=[row]
    )

    alignment = derive_alignment(manifest)

    assert (
        alignment["requirements"]["req.slow-proof"]["satisfaction_status"]
        == "satisfied"
    )


def test_class_qualified_junit_identity_satisfies_named_test() -> None:
    row = Evidence(
        id="ev.pytest.class-report-backed",
        kind="pytest",
        status="pass",
        refs=["req.slow-proof", "acc.slow-proof"],
        data={"junit_test_ids": ["tests.test_class.TestProof::test_named"]},
    )
    manifest = _manifest(
        'pytest tests/test_class.py::TestProof::test_named -m "slow or not slow" -q',
        test_id="tests/test_class.py::TestProof::test_named",
        evidence=[row],
    )

    alignment = derive_alignment(manifest)

    assert (
        alignment["requirements"]["req.slow-proof"]["satisfaction_status"]
        == "satisfied"
    )


def test_skipped_named_identity_remains_unproven() -> None:
    row = Evidence(
        id="ev.pytest.skipped",
        kind="pytest",
        status="pass",
        refs=["req.slow-proof", "acc.slow-proof"],
        summary="A mixed report must not hide the skipped named test.",
        data={
            "junit_test_ids": ["tests.test_sample::test_slow"],
            "skipped_test_ids": ["tests.test_sample::test_slow"],
        },
    )
    manifest = _manifest("pytest tests/test_sample.py -q", evidence=[row])

    alignment = derive_alignment(manifest)

    assert (
        alignment["requirements"]["req.slow-proof"]["satisfaction_status"]
        == "unsatisfied"
    )


def test_canonical_active_pacts_have_no_hazardous_live_command() -> None:
    findings: list[tuple[str, str]] = []
    for path in sorted((REPO_ROOT / "pact" / "changes").glob("*/pact.yaml")):
        manifest = load_manifest(REPO_ROOT, path.parent.name)
        findings.extend(
            (path.parent.name, issue.path)
            for issue in _slow_pytest_command_issues(
                manifest,
                repo_root_path=REPO_ROOT,
            )
            if issue.code == "pytest_command_deselects_named_slow_test"
        )

    assert findings == []
