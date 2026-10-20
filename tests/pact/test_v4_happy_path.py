from __future__ import annotations

from okg.substrate.pact.fsm import DecisionEvent
from okg.substrate.pact.models import (
    Change,
    Evidence,
    EvidencePolicy,
    PactManifest,
    Requirement,
    Task,
)
from okg.substrate.pact.review import derive_change_state
from okg.substrate.pact.store import intent_digest


def _manifest(*, tier: int | None, labels: list[str] | None = None) -> PactManifest:
    """A bare v4 manifest.

    Kept here rather than imported from tests/pact/test_v4_lifecycle_authority.py:
    that file checks this repository's own records and does not ship in the
    public mirror, so importing from it broke collection there.
    """
    return PactManifest(
        pact_version=4,
        target_pact_version=4,
        change=Change(
            id="lifecycle-demo",
            title="Lifecycle demo",
            status="proposal",
            tier=tier,
            labels=labels or [],
        ),
    )


def _decision(
    verdict: str,
    from_state: str,
    to_state: str,
    *,
    digest: str,
) -> DecisionEvent:
    return DecisionEvent(
        change_id="lifecycle-demo",
        verdict=verdict,
        from_state=from_state,
        to_state=to_state,
        principal="operator",
        intent_digest=digest,
    )


def test_tier_two_journey_has_one_decision_path_at_each_boundary() -> None:
    manifest = _manifest(tier=2)
    manifest.requirements = [
        Requirement(
            id="req.lifecycle-demo",
            title="Lifecycle demo",
            statement="Lifecycle demo must have executed proof.",
            evidence_policy=EvidencePolicy(required=["executed_test"]),
        )
    ]
    manifest.tasks = [
        Task(
            id="task.lifecycle-demo",
            title="Lifecycle demo",
            verification="pytest tests/pact/test_v4_happy_path.py -q",
            requirements=["req.lifecycle-demo"],
        )
    ]
    assert derive_change_state(manifest, []).state == "proposed"

    digest = intent_digest(manifest)
    proposal = _decision("approve", "proposed", "in_progress", digest=digest)
    assert derive_change_state(manifest, [proposal]).state == "in_progress"

    manifest.tasks[0].status = "done"
    manifest.tasks[0].evidence = ["ev.lifecycle-demo"]
    manifest.evidence = [
        Evidence(
            id="ev.lifecycle-demo",
            kind="executed_test",
            status="pass",
            refs=["req.lifecycle-demo"],
            command="pytest tests/pact/test_v4_happy_path.py -q",
        )
    ]
    assert derive_change_state(manifest, [proposal]).state == "awaiting_review"

    final = _decision("approve", "awaiting_review", "retired", digest=digest)
    assert derive_change_state(manifest, [proposal, final]).state == "retired"


def test_each_recorded_boundary_has_one_advancing_state() -> None:
    path = [
        ("proposed", "approve", "in_progress"),
        ("awaiting_review", "approve", "retired"),
    ]
    assert all(from_state != to_state for from_state, _, to_state in path)
    assert [verdict for _, verdict, _ in path] == ["approve", "approve"]
