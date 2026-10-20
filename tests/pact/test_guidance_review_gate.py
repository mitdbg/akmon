from __future__ import annotations

from pathlib import Path

from okg.substrate.pact.cli import _simple_gate, _status
from okg.substrate.pact.markdown_view import write_markdown_view
from okg.substrate.pact.models import (
    Change,
    Evidence,
    EvidencePolicy,
    PactManifest,
    Requirement,
    Task,
    TestPlanEntry as PactTestPlanEntry,
    Waiver,
)
from okg.substrate.pact.store import save_manifest


def _manifest(
    *,
    affected_code: list[str],
    target_pact_version: int = 2,
    evidence: list[Evidence] | None = None,
    waivers: list[Waiver] | None = None,
) -> PactManifest:
    return PactManifest(
        pact_version=target_pact_version if target_pact_version >= 4 else 0,
        target_pact_version=target_pact_version,
        change=Change(
            id="guidance-demo",
            title="Guidance demo",
            status="verification",
            tier=1 if target_pact_version >= 4 else None,
            affected_areas=["operator-cli"],
            affected_code=affected_code,
        ),
        requirements=[
            Requirement(
                id="req.demo",
                statement="The operator-facing CLI behavior must be proven.",
                evidence_policy=EvidencePolicy(required=["pytest"]),
            )
        ],
        tasks=[
            Task(
                id="task.demo",
                title="Prove CLI behavior",
                status="done",
                verification="pytest tests/pact/test_guidance_review_gate.py -q",
                requirements=["req.demo"],
            )
        ],
        test_plan=[
            PactTestPlanEntry(
                id="test.demo",
                requirement="req.demo",
                test_id="tests/pact/test_guidance_review_gate.py",
                evidence=["pytest"],
            )
        ],
        evidence=[
            Evidence(
                id="ev.pytest.req.demo",
                kind="pytest",
                status="pass",
                refs=["req.demo"],
            ),
            *(evidence or []),
        ],
        waivers=waivers or [],
    )


def _pre_merge_gate(root: Path, manifest: PactManifest) -> dict:
    save_manifest(root, manifest)
    write_markdown_view(root, manifest.change.id)
    return _simple_gate(
        root,
        manifest.change.id,
        "pre-merge",
        lifecycle_gate="verification",
    )


def _pre_commit_gate(root: Path, manifest: PactManifest) -> dict:
    save_manifest(root, manifest)
    return _simple_gate(root, manifest.change.id, "pre-commit")


def _error_codes(gate: dict) -> set[str]:
    return {
        error["code"]
        for error in gate["validation"]["errors"]
    }


def test_pre_merge_refuses_operator_surface_without_guidance_review(
    tmp_path: Path,
) -> None:
    gate = _pre_merge_gate(
        tmp_path,
        _manifest(affected_code=["src/okg/substrate/cli/main.py"]),
    )

    assert gate["ok"] is False
    assert "verification_gate_missing_guidance_review" in _error_codes(gate)
    assert gate["review_view"]["ok"] is True


def test_pre_merge_accepts_operator_surface_with_docs_update(
    tmp_path: Path,
) -> None:
    gate = _pre_merge_gate(
        tmp_path,
        _manifest(
            affected_code=[
                "src/okg/substrate/cli/main.py",
                "docs/reference/cli.md",
            ],
        ),
    )

    assert gate["ok"] is True
    assert "verification_gate_missing_guidance_review" not in _error_codes(gate)


def test_v4_pre_commit_gate_uses_on_demand_review_view(tmp_path: Path) -> None:
    manifest = _manifest(
        target_pact_version=4,
        affected_code=[
            "src/okg/substrate/cli/main.py",
            "docs/reference/cli.md",
        ],
    )
    gate = _pre_commit_gate(
        tmp_path,
        manifest,
    )
    status = _status(tmp_path, manifest.change.id, include_graph=False)

    assert gate["ok"] is True
    assert gate["review_view"] == {"ok": True, "status": "on-demand"}
    assert status["review_view"] == {"ok": True, "status": "on-demand"}
    assert status["agent_guidance"]["state"] != "refresh_review"


def test_pre_merge_accepts_operator_surface_with_guidance_review_evidence(
    tmp_path: Path,
) -> None:
    gate = _pre_merge_gate(
        tmp_path,
        _manifest(
            affected_code=["src/okg/substrate/cli/main.py"],
            evidence=[
                Evidence(
                    id="ev.guidance-review.req.demo",
                    kind="operator_report",
                    status="pass",
                    refs=["req.demo"],
                    summary="Guidance review: CLI docs already describe this behavior.",
                )
            ],
        ),
    )

    assert gate["ok"] is True
    assert "verification_gate_missing_guidance_review" not in _error_codes(gate)


def test_pre_merge_accepts_operator_surface_with_guidance_review_waiver(
    tmp_path: Path,
) -> None:
    gate = _pre_merge_gate(
        tmp_path,
        _manifest(
            affected_code=["src/okg/substrate/cli/main.py"],
            waivers=[
                Waiver(
                    id="waiver.guidance-review",
                    scope="guidance-review",
                    reason=(
                        "No guidance update is needed because the CLI output "
                        "shape did not change."
                    ),
                    owner="riley",
                )
            ],
        ),
    )

    assert gate["ok"] is True
    assert "verification_gate_missing_guidance_review" not in _error_codes(gate)
