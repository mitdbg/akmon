from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

import pytest

from okg.substrate.deployment_lifecycle_contract import (
    CANONICAL_CONTRACT,
    CONTRACT_REPO_PATH,
    LEGACY_FULL_PHASES,
    PHASES,
    PLANNER_REPO_PATH,
    contract_digest,
    validate_contract_payload,
    validate_lifecycle_plan_receipt,
)
from okg.substrate.deployment_upgrade import PHASES as EXECUTOR_PHASES
from okg.substrate.deployments import find_deployments_root
from okg.substrate.errors import SubstrateError


ROOT = Path(__file__).resolve().parents[2]
RUNBOOK = ROOT / "deployments/okg-ci/RUNBOOK.md"


def _planner():
    path = find_deployments_root() / "okg-ci/autoupdate_gate.py"
    spec = importlib.util.spec_from_file_location("okgci_contract_planner", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def _planner_digest(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _synthetic_planner(tmp_path: Path) -> Path:
    """Planner bytes for the receipt checks, which only hash the file.

    The real planner (PLANNER_REPO_PATH, under deployments/okg-ci/) does not
    ship in the public copy; #2823 tracks the product path that names it.
    """
    path = tmp_path / "autoupdate_gate.py"
    path.write_text("# a planner the receipt binds by digest\n", encoding="utf-8")
    return path


def test_full_lifecycle_consumers_share_exact_phase_contract() -> None:
    planner = _planner()
    documented = " -> ".join(PHASES)

    assert EXECUTOR_PHASES == PHASES
    assert planner.FULL_PHASES == PHASES
    assert documented in RUNBOOK.read_text(encoding="utf-8").replace("\n  ", " ")
    assert PHASES.index("generation_recovery") < PHASES.index("attribute_read_model")
    assert PHASES.index("restart_services") < PHASES.index("source_convergence")
    assert PHASES.index("source_convergence") < PHASES.index("smoke_publish")


def test_contract_rejects_unknown_duplicate_missing_and_extra_fields() -> None:
    base = {
        "schema": CANONICAL_CONTRACT.schema,
        "contract_version": CANONICAL_CONTRACT.contract_version,
        "phases": list(PHASES),
    }
    mutations = []
    mutations.append({**base, "extra": True})
    mutations.append({**base, "phases": [*PHASES, PHASES[-1]]})
    mutations.append({**base, "phases": [*PHASES[:-1], "unknown"]})
    mutations.append({**base, "phases": list(PHASES[:-1])})

    for mutation in mutations:
        with pytest.raises(SubstrateError):
            validate_contract_payload(mutation)


def test_candidate_executor_admits_digest_bound_v2_plan(tmp_path: Path) -> None:
    planner_path = _synthetic_planner(tmp_path)
    contract = {
        "schema": CANONICAL_CONTRACT.schema,
        "contract_version": CANONICAL_CONTRACT.contract_version,
        "phases": list(PHASES),
    }
    receipt = {
        "schema": "okg.okg_ci_lifecycle_plan.v2",
        "action": "full_lifecycle",
        "candidate_revision": "candidate-a",
        "candidate_planner_digest": _planner_digest(planner_path),
        "selected_phases": list(PHASES),
        "candidate_contract": contract,
        "candidate_contract_digest": contract_digest(contract),
    }
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(receipt), encoding="utf-8")

    admission = validate_lifecycle_plan_receipt(
        path,
        candidate_revision="candidate-a",
        planner_path=planner_path,
    )

    assert admission["admission"] == "contract_v2"
    assert admission["contract"]["digest"] == CANONICAL_CONTRACT.digest


def test_a_checkout_without_the_planner_refuses_the_plan_by_name(tmp_path: Path) -> None:
    """The planner is okg-ci deployment code, absent from the public copy.

    A lifecycle plan binds that planner's bytes, so a checkout without it
    must refuse the plan and say which file it needs -- never admit it.
    """
    receipt = {
        "schema": "okg.okg_ci_lifecycle_plan.v2",
        "action": "full_lifecycle",
        "candidate_revision": "candidate-a",
        "candidate_planner_digest": "sha256:" + "0" * 64,
    }
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(receipt), encoding="utf-8")
    missing = tmp_path / "no-deployment" / "autoupdate_gate.py"

    with pytest.raises(SubstrateError) as raised:
        validate_lifecycle_plan_receipt(
            path, candidate_revision="candidate-a", planner_path=missing
        )

    assert raised.value.data == {"path": str(missing)}
    assert PLANNER_REPO_PATH in (raised.value.fix_hint or "")
    assert "without --lifecycle-plan" in (raised.value.fix_hint or "")


def test_precheckout_admits_candidate_planner_bytes_without_executing_them(
    tmp_path: Path,
) -> None:
    installed_planner = tmp_path / "installed_planner.py"
    installed_planner.write_bytes(b"installed planner")
    candidate_planner_bytes = b"raise RuntimeError('must not execute candidate')"
    contract = {
        "schema": CANONICAL_CONTRACT.schema,
        "contract_version": CANONICAL_CONTRACT.contract_version,
        "phases": list(PHASES),
    }
    receipt = {
        "schema": "okg.okg_ci_lifecycle_plan.v2",
        "action": "full_lifecycle",
        "candidate_revision": "candidate-a",
        "candidate_planner_digest": (
            "sha256:" + hashlib.sha256(candidate_planner_bytes).hexdigest()
        ),
        "selected_phases": list(PHASES),
        "candidate_contract": contract,
        "candidate_contract_digest": contract_digest(contract),
    }
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(receipt), encoding="utf-8")

    admission = validate_lifecycle_plan_receipt(
        path,
        candidate_revision="candidate-a",
        planner_path=installed_planner,
        planner_bytes=candidate_planner_bytes,
    )

    assert admission["admission"] == "contract_v2"


def test_candidate_executor_refuses_plan_contract_or_target_drift(tmp_path: Path) -> None:
    planner_path = _synthetic_planner(tmp_path)
    contract_path = ROOT / CONTRACT_REPO_PATH
    contract = json.loads(contract_path.read_text(encoding="utf-8"))
    receipt = {
        "schema": "okg.okg_ci_lifecycle_plan.v2",
        "action": "full_lifecycle",
        "candidate_revision": "candidate-a",
        "candidate_planner_digest": _planner_digest(planner_path),
        "selected_phases": list(PHASES),
        "candidate_contract": contract,
        "candidate_contract_digest": contract_digest(contract),
    }
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(receipt), encoding="utf-8")

    with pytest.raises(SubstrateError, match="candidate"):
        validate_lifecycle_plan_receipt(
            path,
            candidate_revision="candidate-b",
            planner_path=planner_path,
        )
    receipt["candidate_contract_digest"] = "sha256:" + "0" * 64
    path.write_text(json.dumps(receipt), encoding="utf-8")
    with pytest.raises(SubstrateError, match="contract digest"):
        validate_lifecycle_plan_receipt(
            path,
            candidate_revision="candidate-a",
            planner_path=planner_path,
        )


def test_first_release_bridge_is_exact_and_cannot_expand_legacy_authority(
    tmp_path: Path,
) -> None:
    planner_path = _synthetic_planner(tmp_path)
    receipt = {
        "schema": "okg.okg_ci_lifecycle_plan.v1",
        "action": "full_lifecycle",
        "candidate_revision": "candidate-a",
        "candidate_planner_digest": _planner_digest(planner_path),
        "selected_phases": list(LEGACY_FULL_PHASES),
        "baseline_revision": "baseline-a",
        "baseline_artifact": "sha256:" + "a" * 64,
    }
    path = tmp_path / "legacy-plan.json"
    path.write_text(json.dumps(receipt), encoding="utf-8")

    admission = validate_lifecycle_plan_receipt(
        path,
        candidate_revision="candidate-a",
        planner_path=planner_path,
        authorized_baseline_revision="baseline-a",
        bridge_state_file=tmp_path / "state-a.json",
    )
    assert admission["admission"] == "legacy_v1_candidate_planner_bridge"

    # The bridge is a one-transition compatibility admission, not a reusable
    # permission for another deployment state machine.
    with pytest.raises(SubstrateError, match="already consumed"):
        validate_lifecycle_plan_receipt(
            path,
            candidate_revision="candidate-a",
            planner_path=planner_path,
            authorized_baseline_revision="baseline-a",
            bridge_state_file=tmp_path / "state-b.json",
        )

    receipt["selected_phases"].append("source_convergence")
    path.write_text(json.dumps(receipt), encoding="utf-8")
    with pytest.raises(SubstrateError, match="legacy lifecycle plan"):
        validate_lifecycle_plan_receipt(
            path,
            candidate_revision="candidate-a",
            planner_path=planner_path,
            authorized_baseline_revision="baseline-a",
            bridge_state_file=tmp_path / "state-a.json",
        )
