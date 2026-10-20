from __future__ import annotations

from copy import deepcopy

from okg.substrate.pact.models import (
    Change,
    Evidence,
    PactCodeProofReceipt,
    PactManifest,
)
from okg.substrate.pact.home_repository import home_repository
from okg.substrate.pact.store import intent_digest, state_digest


def _manifest() -> PactManifest:
    return PactManifest(
        pact_version=0,
        target_pact_version=1,
        change=Change(id="demo", title="Demo", status="proposal"),
        requirements=[
            {
                "id": "req.demo",
                "statement": "Demo intent.",
                "evidence_policy": {"required": ["planned_test"]},
                "scenarios": [
                    {
                        "id": "scn.demo",
                        "title": "intent",
                        "when": ["digest is computed"],
                        "then": ["intent fields are covered"],
                    }
                ],
                "acceptance": [
                    {
                        "id": "acc.demo",
                        "description": "digest contract",
                        "evidence": ["planned_test"],
                    }
                ],
            }
        ],
        tasks=[
            {
                "id": "task.demo",
                "title": "Do demo",
                "verification": "pytest tests/pact/test_v1_intent_digest_contract.py -q",
                "requirements": ["req.demo"],
            }
        ],
        test_plan=[
            {
                "id": "test.demo",
                "requirement": "req.demo",
                "test_id": "tests/pact/test_v1_intent_digest_contract.py::test_intent_digest_changes_only_for_intent",
            }
        ],
    )


def test_intent_digest_changes_only_for_intent() -> None:
    manifest = _manifest()
    original = intent_digest(manifest)

    progress = deepcopy(manifest)
    progress.change.status = "implementation"
    progress.tasks[0].status = "done"
    progress.tasks[0].evidence.append("ev.pytest.req.demo")
    progress.evidence.append(
        Evidence(
            id="ev.pytest.req.demo",
            kind="pytest",
            status="pass",
            refs=["req.demo"],
        )
    )
    assert intent_digest(progress) == original

    changed_intent = deepcopy(manifest)
    changed_intent.requirements[0].statement = "Changed intent."
    assert intent_digest(changed_intent) != original

    changed_task_contract = deepcopy(manifest)
    changed_task_contract.tasks[0].verification = "pytest tests/pact -q"
    assert intent_digest(changed_task_contract) != original


def test_code_proof_receipt_is_state_but_not_intent() -> None:
    manifest = _manifest()
    original_intent = intent_digest(manifest)
    original_state = state_digest(manifest)
    receipt = PactCodeProofReceipt(
        schema="okg.pact_code_proof_receipt/v1",
        repository=home_repository(),
        base_sha="a" * 40,
        base_tree="b" * 40,
        proven_code_sha="c" * 40,
        proven_code_tree="d" * 40,
        pact_intent_digest=original_intent,
        closure_allowlist=["pact/changes/demo/pact.yaml"],
        code_tuples=[
            {"path": "src/demo.py", "mode": "100644", "object_id": "e" * 40}
        ],
        code_digest="sha256:" + "f" * 64,
        receipt_digest="sha256:" + "0" * 64,
    )
    assert receipt.model_dump(mode="json", by_alias=True)["schema"] == (
        "okg.pact_code_proof_receipt/v1"
    )
    assert "schema_" not in receipt.model_dump(mode="json", by_alias=True)
    manifest.proof_receipts.code.append(receipt)

    assert intent_digest(manifest) == original_intent
    assert state_digest(manifest) != original_state
