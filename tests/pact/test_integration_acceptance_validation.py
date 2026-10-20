"""A program's cross-cutting invariants are checked.

`pact-acceptance-prose-is-enforceable`
req.pact-acceptance-prose-is-enforceable.integration-acceptance-validated-or-deleted.

`hierarchy.integration_acceptance` was declared in the model, echoed into a
view payload, and listed in the schema -- and had no validation rule anywhere
in the package. A program-level invariant such as "no silent fallback
anywhere in the program" was prose no gate could fail.

The CHECKABLE route was taken rather than deletion; the reasoning is recorded
in this change's reports/integration-acceptance-route.md. The two test-side
mentions that exist today (tests/docs/test_akmon_design_audit.py:177, which
asserts the list is non-empty for one program, and
tests/pact/test_nomos_architecture_checkpoint.py:247-250, which asserts a
substring appears in one program's clauses) are change-specific assertions,
not a validation rule, so neither is existing coverage for this requirement.
"""

from __future__ import annotations

from pathlib import Path

import yaml

from okg.substrate.pact.evaluation_context import PactEvaluationContext
from okg.substrate.pact.evidence_kinds import DEFAULT_REGISTRY
from okg.substrate.pact.models import (
    AcceptanceCriterion,
    Change,
    Evidence,
    PactHierarchy,
    PactManifest,
    Requirement,
    Waiver,
)
from okg.substrate.pact.store import save_manifest
from okg.substrate.pact.validation import (
    _child_requirement_rollup,
    _clause_requirement_tokens,
    validate_manifest,
)

REPO_ROOT = Path(__file__).resolve().parents[2]


def _child(
    change_id: str = "child",
    req_id: str = "req.child.invariant",
    *,
    satisfied: bool = False,
) -> PactManifest:
    return PactManifest(
        pact_version=4,
        target_pact_version=4,
        change=Change(
            id=change_id,
            title="Child",
            tier=2,
            labels=["tier-2"],
        ),
        hierarchy=PactHierarchy(role="child", parent="program"),
        requirements=[
            Requirement(
                id=req_id,
                title="Child invariant",
                statement="The child owns the invariant the program names.",
                evidence_policy={"required": ["manual"]},
                acceptance=[
                    AcceptanceCriterion(
                        id="acc.child",
                        description="Proof exists.",
                        evidence=["manual"],
                    )
                ],
            )
        ],
        evidence=(
            [
                Evidence(
                    id="ev.manual.child-invariant",
                    kind="manual",
                    status="pass",
                    refs=[req_id, "acc.child"],
                    summary="The child invariant was demonstrated.",
                )
            ]
            if satisfied
            else []
        ),
    )


def _program(clauses: list[str], *, children: list[str] | None = None) -> PactManifest:
    return PactManifest(
        pact_version=4,
        target_pact_version=4,
        change=Change(id="program", title="Program", tier=3, labels=["tier-3"]),
        hierarchy=PactHierarchy(
            role="program",
            children=list(children if children is not None else ["child"]),
            integration_acceptance=clauses,
        ),
        waivers=[
            Waiver(
                id="waiver.test.program-implementation",
                scope="lifecycle.implementation",
                reason="The fixture isolates integration-acceptance roll-up.",
                owner="test:integration-acceptance",
            )
        ],
    )


def _seed(tmp_path: Path, *manifests: PactManifest) -> Path:
    (tmp_path / "pact" / "schemas").mkdir(parents=True, exist_ok=True)
    (tmp_path / "pact" / "schemas" / "pact.schema.yaml").write_text(
        "pact_schema_version: 0\n", encoding="utf-8"
    )
    for manifest in manifests:
        save_manifest(tmp_path, manifest)
    return tmp_path


def _codes(report, prefix: str = "integration_acceptance") -> list[str]:
    return [issue.code for issue in report.issues if issue.code.startswith(prefix)]


def test_clause_naming_no_requirement_is_flagged(tmp_path: Path) -> None:
    """scn.pact-acceptance-prose-is-enforceable.unowned-clause-flagged."""
    root = _seed(
        tmp_path,
        _child(),
        _program(["No silent fallback anywhere in the program."]),
    )

    report = validate_manifest(
        _program(["No silent fallback anywhere in the program."]),
        repo_root=root,
    )

    assert _codes(report) == ["integration_acceptance_clause_unowned"]


def test_clause_naming_a_nonexistent_requirement_is_flagged(tmp_path: Path) -> None:
    """Existence is checked, not merely presence: naming an id is not the
    same as naming a real one."""
    root = _seed(tmp_path, _child(), _program([]))

    report = validate_manifest(
        _program(["Owned by req.child.does-not-exist."]),
        repo_root=root,
    )

    assert _codes(report) == ["integration_acceptance_clause_unknown_requirement"]


def test_every_named_requirement_must_resolve(tmp_path: Path) -> None:
    """One valid token cannot launder a dangling owner token."""
    root = _seed(tmp_path, _child(), _program([]))

    owned = validate_manifest(
        _program(["Owned by req.child.invariant."]),
        repo_root=root,
    )
    dangling = validate_manifest(
        _program(["Owned by req.child.invariant and req.child.ghost."]),
        repo_root=root,
    )

    assert _codes(owned) == []
    assert _codes(dangling) == ["integration_acceptance_clause_unknown_requirement"]


def test_unsatisfied_owner_blocks_program_completion(tmp_path: Path) -> None:
    root = _seed(tmp_path, _child(satisfied=False), _program([]))

    report = validate_manifest(
        _program(["Owned by req.child.invariant."]),
        repo_root=root,
        lifecycle_gate="verification",
    )

    assert "integration_acceptance_owner_unproven" in _codes(report)
    assert report.ok is False


def test_satisfied_child_owner_passes(tmp_path: Path) -> None:
    """scn.pact-acceptance-prose-is-enforceable.owned-clause-passes."""
    child = _child(satisfied=True)
    child.waivers = [
        Waiver(
            id="waiver.test.child-implementation",
            scope="lifecycle.implementation",
            reason="The fixture isolates integration-acceptance roll-up.",
            owner="test:integration-acceptance",
        )
    ]
    root = _seed(tmp_path, child, _program([]))

    report = validate_manifest(
        _program(["Owned by req.child.invariant."]),
        repo_root=root,
        lifecycle_gate="verification",
    )

    assert "integration_acceptance_owner_unproven" not in _codes(report)
    assert "integration_acceptance_child_unverified" not in _codes(report)
    assert report.ok is True


def test_clause_spanning_multiple_child_owners_is_flagged(tmp_path: Path) -> None:
    root = _seed(
        tmp_path,
        _child(),
        _child(change_id="other", req_id="req.other.invariant"),
        _program([], children=["child", "other"]),
    )

    report = validate_manifest(
        _program(
            ["Owned by req.child.invariant and req.other.invariant."],
            children=["child", "other"],
        ),
        repo_root=root,
    )

    assert "integration_acceptance_ambiguous_owner" in _codes(report)


def test_wrong_parent_child_contract_blocks_program(tmp_path: Path) -> None:
    child = _child(satisfied=True)
    child.hierarchy = PactHierarchy(role="child", parent="wrong-program")
    child.waivers = [
        Waiver(
            id="waiver.test.child-implementation",
            scope="lifecycle.implementation",
            reason="The fixture isolates child contract identity.",
            owner="test:integration-acceptance",
        )
    ]
    root = _seed(tmp_path, child, _program([]))

    report = validate_manifest(
        _program(["Owned by req.child.invariant."]),
        repo_root=root,
        lifecycle_gate="verification",
    )

    assert "integration_acceptance_child_contract_invalid" in _codes(report)
    assert report.ok is False


def test_satisfied_but_unverified_child_blocks_program(tmp_path: Path) -> None:
    child = _child(satisfied=True)
    root = _seed(tmp_path, child, _program([]))

    report = validate_manifest(
        _program(["Owned by req.child.invariant."]),
        repo_root=root,
        lifecycle_gate="verification",
    )

    assert "integration_acceptance_owner_unproven" not in _codes(report)
    assert "integration_acceptance_child_unverified" in _codes(report)


def test_warning_validation_verifies_each_child_lifecycle_once(
    tmp_path: Path, monkeypatch
) -> None:
    root = _seed(tmp_path, _child(satisfied=True), _program([]))
    child_verifications: list[str | None] = []
    original_validate_manifest = validate_manifest

    def track_child_validation(*args, **kwargs):
        child_verifications.append(kwargs.get("lifecycle_gate"))
        return original_validate_manifest(*args, **kwargs)

    monkeypatch.setattr(
        "okg.substrate.pact.validation.validate_manifest",
        track_child_validation,
    )

    report = validate_manifest(
        _program(["Owned by req.child.invariant."]),
        repo_root=root,
    )

    assert _codes(report) == []
    assert child_verifications == ["verification"]


def test_unreadable_child_is_reported_rather_than_passed(tmp_path: Path) -> None:
    """An unreadable child is not agreement. Without this the rule would
    silently accept every clause on a program whose children are missing."""
    root = _seed(tmp_path, _program([]))

    report = validate_manifest(
        _program(["Owned by req.child.invariant."], children=["absent-child"]),
        repo_root=root,
    )

    assert _codes(report) == ["integration_acceptance_owner_unresolved"]


def test_malformed_captured_child_refuses_without_crashing(tmp_path: Path) -> None:
    context = PactEvaluationContext(
        files={"pact/changes/child/pact.yaml": b"change: [unterminated"},
        evidence_registry=DEFAULT_REGISTRY,
    )

    report = validate_manifest(
        _program(["Owned by req.child.invariant."]),
        repo_root=tmp_path,
        context=context,
    )

    assert _codes(report) == ["integration_acceptance_owner_unresolved"]
    assert (
        _child_requirement_rollup(
            "child",
            repo_root_path=tmp_path,
            context=context,
            verify_child=False,
        )
        is None
    )


def test_unreadable_child_cannot_hide_other_named_owner_checks(tmp_path: Path) -> None:
    root = _seed(tmp_path, _child(), _program([]))
    program = _program(
        ["Owned by req.child.invariant and req.child.ghost."],
        children=["child", "absent-child"],
    )

    report = validate_manifest(program, repo_root=root, lifecycle_gate="verification")

    codes = _codes(report)
    assert "integration_acceptance_owner_unresolved" in codes
    assert "integration_acceptance_owner_unproven" in codes


def test_issue_is_blocking_at_the_verification_gate_for_programs(
    tmp_path: Path,
) -> None:
    root = _seed(tmp_path, _child(), _program([]))
    program = _program(["No silent fallback anywhere in the program."])

    plain = validate_manifest(program, repo_root=root)
    gated = validate_manifest(program, repo_root=root, lifecycle_gate="verification")

    plain_issues = [
        issue
        for issue in plain.issues
        if issue.code.startswith("integration_acceptance")
    ]
    gated_issues = [
        issue
        for issue in gated.issues
        if issue.code.startswith("integration_acceptance")
    ]
    assert plain_issues and all(issue.severity == "warning" for issue in plain_issues)
    assert any(issue.severity == "error" for issue in gated_issues)
    assert gated.ok is False


def test_clause_tokens_are_extracted_structurally_not_by_reading_prose() -> None:
    assert _clause_requirement_tokens("Owned by req.a.b.") == ["req.a.b"]
    assert _clause_requirement_tokens("req.a.b and req.c.d, jointly") == [
        "req.a.b",
        "req.c.d",
    ]
    assert _clause_requirement_tokens("The program requires no fallbacks") == []


def test_the_real_tree_has_no_integration_acceptance_clause_left() -> None:
    """The corpus arm of this rule, after the v5 cutover.

    `hierarchy.integration_acceptance` is a v4 field. Every manifest is now
    v5, which has no `hierarchy` at all, so no clause -- owned or unowned --
    survives in `pact/changes/`. The clause-token rule is exercised against
    fixtures above; this records why its corpus arm is empty rather than
    leaving an unsatisfiable non-empty expectation behind.
    """
    with_hierarchy: list[str] = []
    for path in sorted((REPO_ROOT / "pact" / "changes").glob("*/pact.yaml")):
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert raw.get("pact_version") == 5, path
        if raw.get("hierarchy"):
            with_hierarchy.append(path.parent.name)

    assert with_hierarchy == []
