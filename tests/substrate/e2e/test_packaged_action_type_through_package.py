"""#1185 exit item 3: the packaged action type driven through the package.

`test_external_distribution_hermetic_arm.py` proves the action type is
*packaged*: its bytes verify and carry the declared shape, and nothing runs
them. That is packaging coverage, not use. This module takes the next step
and drives the same packaged declaration through a full review lifecycle:
propose -> approve **with an edit** -> claim -> execute, against a
deterministic in-process lab bench that stands in for the external provider.

What the package supplies here, and nothing else does:

* the action type's identity (`action_type_id`),
* its approval gate (`requires_approval`) and its side-effecting flag,
* the closed set of payload fields (`payload_fields`), which becomes both
  the payload JSON schema and the contract's editable diff-field codes,
* the action schema digest, which is the digest of the packaged bytes.

What the package does **not** supply, and what this arm therefore invents:
the provider binding digests, the target constraint, and the review and
execution limits. The fixture distribution declares none of them, so they
are deployment-side constants here. Naming them is the point: a reader must
not mistake them for packaged authority. Real provider bindings belong with
the deployment product work, not with this proof.

The worker is also not packaged. `blocked.external-automation-and-action-
proposals` records that a package-supplied worker waits on #1181's packaged-
extension loader, so the executor below is written in this module. What is
proven is the *governance path* around the packaged action type, not a
packaged executor.

No database is involved. The review service's reference store is in memory
by design (`InMemoryExternalActionStore`); the durable PostgreSQL boundary
has its own proof in `tests/substrate/external_actions/test_store_postgres.py`.
Nothing here reads or writes the graph, so there is no publish to check.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.distributions import verify_distribution_package
from okg.substrate.external_actions import (
    ActionContract,
    Actor,
    ExternalActionError,
    ExternalActionService,
    InMemoryExternalActionStore,
)

# The hermetic arm owns the fixture distribution. Importing its builder is
# deliberate: if the packaged action-type declaration there changes, this
# module changes with it instead of drifting away from a private copy.
from tests.substrate.e2e.test_external_distribution_hermetic_arm import (
    _ASSET_BYTES,
    _OWNER,
    build_archi_test_package,
)

_ACTION_ASSET_ID = "lab-bench-recalibrate"
_QUALIFIED_ACTION_ASSET = f"{_OWNER}:{_ACTION_ASSET_ID}"

# Deployment-side, NOT packaged. See the module docstring.
_DEPLOYMENT = "lab-hermetic-prod"
_TARGET_REF = "lab_bench:0002"
_NOW = datetime(2026, 9, 18, 10, 0, tzinfo=timezone.utc)
_REVIEW_EXPIRES = "2026-09-18T11:00:00.000000Z"
_EXECUTE_BEFORE = "2026-09-18T10:30:00.000000Z"
_POLICY_DIGEST = "sha256:" + "3" * 64
_EXECUTION_EPOCH = 41

_PROPOSER = Actor(
    "oidc:automation.lab-hermetic:service:lab-sweep",
    "subject:automation:lab-sweep",
    "auth:propose",
    "nomos:propose",
)
_REVIEWER = Actor(
    "oidc:login.lab-hermetic:user:operator-3",
    "subject:human:operator-3",
    "auth:review",
    "nomos:review",
)
_WORKER = Actor(
    "mtls:workers.lab-hermetic:bench-worker-1",
    "subject:worker:bench-service",
    "auth:worker",
    "nomos:worker",
)


def _digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


# --------------------------------------------------------------------------
# Reading the action type back out of the package
# --------------------------------------------------------------------------


def _read_packaged_action_type(archive: Path) -> tuple[bytes, dict[str, Any]]:
    """Read the action-type asset through the package's verifying reader.

    This is the only route the declaration takes into this module. Nothing
    below reads `_ASSET_BYTES` for the declaration itself; the constant is
    used once, to check that what the package returned is what was built.
    """

    package = verify_distribution_package(archive)
    try:
        payload = package.read_asset(_QUALIFIED_ACTION_ASSET)
    finally:
        package.close_runtime_authority()
    return payload, json.loads(payload)


class PackagedActionTypeRefused(ValueError):
    """The packaged declaration cannot drive a reviewed external action."""


def _contract_from_packaged_declaration(
    payload: bytes, declaration: dict[str, Any]
) -> ActionContract:
    """Turn the packaged declaration into a registry contract, fail-closed.

    Every refusal below is a case where the package would otherwise let an
    unreviewed or open-ended side effect through. None of them defaults to a
    permissive value.
    """

    if declaration.get("schema") != "archi-test.action_type/v1":
        raise PackagedActionTypeRefused("unknown packaged action-type schema")
    if declaration.get("side_effecting") is not True:
        raise PackagedActionTypeRefused(
            "a non side-effecting declaration is not an external action"
        )
    if declaration.get("requires_approval") is not True:
        raise PackagedActionTypeRefused(
            "the packaged action type does not require approval"
        )
    fields = declaration.get("payload_fields")
    if not isinstance(fields, list) or not fields or len(set(fields)) != len(fields):
        raise PackagedActionTypeRefused("payload_fields must be a nonempty unique list")
    action_type_id = declaration.get("action_type_id")
    if not isinstance(action_type_id, str) or not action_type_id:
        raise PackagedActionTypeRefused("packaged action type has no identity")

    payload_schema = {
        "type": "object",
        "additionalProperties": False,
        "required": sorted(fields),
        "properties": {name: {"type": ["string", "number"]} for name in fields},
    }
    return ActionContract.from_mapping(
        {
            "action_type": action_type_id,
            # The packaged bytes are the action schema: edit the declaration
            # and every proposal bound to the old digest stops matching.
            "action_schema_digest": _digest(payload),
            "executor_contract_digest": "sha256:" + "1" * 64,
            "payload_renderer_digest": "sha256:" + "2" * 64,
            "governed_tool_binding_ref": f"{_OWNER}:lab-bench-status",
            "governed_tool_binding_digest": "sha256:" + "4" * 64,
            "provider_connection_ref": f"{_OWNER}:lab-bench-connection",
            "provider_connection_contract_digest": "sha256:" + "5" * 64,
            "provider_capability_digest": "sha256:" + "6" * 64,
            "provider_operation_id": "recalibrate",
            "provider_operation_descriptor_digest": "sha256:" + "7" * 64,
            "provider_replay_mode": "idempotent_retry",
            "target_constraint": {
                "kind": "ascii_prefix_decimal_v1",
                "prefix": "lab_bench:",
                "decimal_digits_minimum": 1,
                "decimal_digits_maximum": 6,
                "leading_zeroes": "allowed",
                "full_match": True,
                "normalization": "none",
                "input_encoding": "UTF-8",
                "ascii_only": True,
            },
            # `side_effecting: true` in the package is what makes this high.
            "minimum_risk_tier": "high",
            "review_and_execution_limits": {
                "maximum_execution_window_seconds": 3600,
                "maximum_review_lifetime_seconds": 86400,
                "reporting_attention_seconds": 86400,
                "reconcile_not_before_seconds": 86400,
            },
            # Only a packaged payload field may be edited under review.
            "diff_field_codes": sorted(fields),
            "allowed_summary_codes": ["recalibrate_drifted_bench"],
            "transition_reason_codes": [
                "offset_corrected_by_operator",
                "out_of_tolerance",
            ],
            "outcome_reason_rules": {
                "provider_receipt_observed": {
                    "allowed_outcomes": ["succeeded"],
                    "provider_receipt": "required",
                }
            },
        },
        payload_schema=payload_schema,
    )


# --------------------------------------------------------------------------
# The deterministic fake external-action service
# --------------------------------------------------------------------------


class FakeLabBench:
    """A stand-in provider that records exactly what it was asked to do.

    It accepts canonical payload *bytes* only, never a Python object, so a
    caller cannot hand it a payload the review service never sealed. Every
    accepted call is appended to `executed`; the tests assert on that list
    rather than on a return value, because the question is what reached the
    provider, not what it answered.

    On the refusal paths the empty `executed` list proves little on its own,
    because this module is also the caller: nothing would have called the
    bench there in any case. It is the refusal code that shows the action
    was blocked. The list carries real weight only where an execution does
    happen, and the question is which payload arrived and how many times.
    """

    def __init__(self) -> None:
        self.executed: list[dict[str, Any]] = []
        self.offsets: dict[str, Any] = {}

    def recalibrate(self, canonical_payload: bytes, *, execution_id: str) -> str:
        if not isinstance(canonical_payload, bytes):
            raise TypeError("the provider takes sealed canonical bytes")
        document = json.loads(canonical_payload)
        self.executed.append({"execution_id": execution_id, "payload": document})
        self.offsets[document["lab_unit_id"]] = document["target_offset"]
        return _digest(canonical_payload)


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def packaged_action(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    """Build the fixture distribution and read its action type back out."""

    root = tmp_path_factory.mktemp("lab-action-through-package")
    archive = build_archi_test_package(root)
    payload, declaration = _read_packaged_action_type(archive)
    return {
        "archive": archive,
        "payload": payload,
        "declaration": declaration,
        "contract": _contract_from_packaged_declaration(payload, declaration),
    }


@pytest.fixture()
def service() -> ExternalActionService:
    store = InMemoryExternalActionStore(
        now=_NOW, execution_epoch=_EXECUTION_EPOCH, policy_digest=_POLICY_DIGEST
    )
    return ExternalActionService(store)


@pytest.fixture()
def bench() -> FakeLabBench:
    return FakeLabBench()


def _original_payload(declaration: dict[str, Any]) -> dict[str, Any]:
    assert declaration["payload_fields"] == ["lab_unit_id", "target_offset"]
    return {"lab_unit_id": "lab_unit:beta", "target_offset": 12}


def _edited_payload(declaration: dict[str, Any]) -> dict[str, Any]:
    edited = _original_payload(declaration)
    edited["target_offset"] = 3
    return edited


def _submit(
    service: ExternalActionService,
    packaged: dict[str, Any],
    *,
    payload: Any,
    key: str = "lab-sweep-propose-1",
) -> Any:
    contract = packaged["contract"]
    return service.submit(
        deployment=_DEPLOYMENT,
        actor=_PROPOSER,
        idempotency_key=key,
        automation_id="lab-bench-sweep",
        automation_version="1.0.0",
        run_id="run-7",
        correlation_id="drift-19",
        action_type=contract.action_type,
        target_ref=_TARGET_REF,
        risk_tier="high",
        action_schema_digest=contract.action_schema_digest,
        action_contract=contract,
        requested_review_expires_at=_REVIEW_EXPIRES,
        summary_code="recalibrate_drifted_bench",
        payload=payload,
    )


def _claim_kwargs(
    approved: Any, contract: ActionContract, *, key: str
) -> dict[str, Any]:
    return {
        "actor": _WORKER,
        "idempotency_key": key,
        "decision_id": approved.current_decision_id,
        "approved_lifecycle_version": approved.lifecycle_version,
        "approved_payload_version": approved.payload_version,
        "approved_payload_hash": approved.current_payload_hash,
        "if_match": approved.value.etag,
        "supported_action_schema_digest": contract.action_schema_digest,
        "supported_action_contract_digest": contract.action_contract_digest,
        "supported_executor_contract_digest": contract.executor_contract_digest,
        "supported_payload_renderer_digest": contract.payload_renderer_digest,
        "supported_governed_tool_binding_digest": contract.governed_tool_binding_digest,
        "supported_provider_connection_contract_digest": (
            contract.provider_connection_contract_digest
        ),
        "supported_provider_capability_digest": contract.provider_capability_digest,
        "supported_provider_operation_id": contract.provider_operation_id,
        "supported_provider_operation_descriptor_digest": (
            contract.provider_operation_descriptor_digest
        ),
    }


# --------------------------------------------------------------------------
# (a) the contract comes out of the package
# --------------------------------------------------------------------------


def test_the_contract_is_built_from_the_packaged_declaration(
    packaged_action: dict[str, Any],
) -> None:
    """Identity, gate, schema digest and editable fields are all packaged."""

    declaration = packaged_action["declaration"]
    contract = packaged_action["contract"]
    # What the verifying reader returned is what the package was built from.
    assert packaged_action["payload"] == _ASSET_BYTES[_ACTION_ASSET_ID][1]
    assert contract.action_type == declaration["action_type_id"] == _ACTION_ASSET_ID
    assert contract.action_schema_digest == _digest(packaged_action["payload"])
    assert list(contract.diff_field_codes) == sorted(declaration["payload_fields"])
    assert contract.payload_schema is not None
    assert contract.payload_schema["required"] == sorted(declaration["payload_fields"])
    assert contract.payload_schema["additionalProperties"] is False


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ({"requires_approval": False}, "does not require approval"),
        ({"side_effecting": False}, "not an external action"),
        ({"payload_fields": []}, "nonempty unique list"),
        ({"schema": "archi-test.action_type/v2"}, "unknown packaged action-type"),
    ],
)
def test_a_weakened_packaged_declaration_is_refused(
    packaged_action: dict[str, Any], mutation: dict[str, Any], message: str
) -> None:
    """The gate is the package's, so weakening the package must refuse.

    This is the failure case for the claim above. If any of these mutations
    still produced a contract, the approval gate would be this module's
    invention rather than the packaged declaration's.
    """

    declaration = dict(packaged_action["declaration"], **mutation)
    payload = json.dumps(declaration, sort_keys=True).encode()
    with pytest.raises(PackagedActionTypeRefused) as refusal:
        _contract_from_packaged_declaration(payload, declaration)
    assert message in str(refusal.value)


def test_a_payload_field_the_package_never_declared_is_refused(
    service: ExternalActionService, packaged_action: dict[str, Any], bench: FakeLabBench
) -> None:
    """`payload_fields` is closed, so an extra field never becomes a proposal."""

    payload = _original_payload(packaged_action["declaration"])
    payload["bench_override"] = "force"
    with pytest.raises(ExternalActionError) as refusal:
        _submit(service, packaged_action, payload=payload)
    assert refusal.value.code == "invalid_proposal"
    assert bench.executed == []


# --------------------------------------------------------------------------
# (b) propose -> approve with an edit -> execute
# --------------------------------------------------------------------------


def test_only_the_edited_payload_reaches_the_provider(
    service: ExternalActionService, packaged_action: dict[str, Any], bench: FakeLabBench
) -> None:
    """The whole exit item, in one path.

    The proposer asks for offset 12. The reviewer approves with an edit to
    3. The worker claims and executes. The bench must see 3 exactly once,
    and must never have seen 12 -- an executor that replayed the submitted
    payload, or executed both versions, fails here.
    """

    declaration = packaged_action["declaration"]
    contract = packaged_action["contract"]
    original = _original_payload(declaration)
    edited = _edited_payload(declaration)

    proposal = _submit(service, packaged_action, payload=original)
    assert proposal.review_state == "pending"
    assert proposal.execution_state == "unavailable"
    # Nothing has run: the packaged action type requires approval.
    assert bench.executed == []

    approved = service.approve_with_edit(
        proposal.proposal_id,
        actor=_REVIEWER,
        idempotency_key="operator-3-approve-with-edit-1",
        action_contract=contract,
        replacement_payload=edited,
        execute_before=_EXECUTE_BEFORE,
        reason_code="offset_corrected_by_operator",
        if_match=proposal.value.etag,
    )
    assert approved.review_state == "approved"
    assert approved.payload_version == 2
    assert bench.executed == []

    claim = service.claim(
        approved.proposal_id,
        **_claim_kwargs(approved, contract, key="bench-worker-1-claim-1"),
    )
    # The service seals the approved payload; the worker never chooses it.
    assert json.loads(claim.canonical_payload) == edited

    receipt = bench.recalibrate(
        claim.canonical_payload, execution_id=claim.execution_id
    )
    outcome = service.report_outcome(
        claim.execution_id,
        actor=_WORKER,
        idempotency_key="bench-worker-1-outcome-1",
        outcome="succeeded",
        outcome_reason_code="provider_receipt_observed",
        provider_receipt_digest=receipt,
        recovery_journal_digest="sha256:" + "d" * 64,
    )
    assert outcome.outcome == "succeeded"

    assert [row["payload"] for row in bench.executed] == [edited]
    assert original not in [row["payload"] for row in bench.executed]
    assert bench.offsets == {"lab_unit:beta": 3}


def test_a_claim_replay_does_not_execute_the_action_twice(
    service: ExternalActionService, packaged_action: dict[str, Any], bench: FakeLabBench
) -> None:
    """A retried claim is the same execution, so the bench moves once."""

    declaration = packaged_action["declaration"]
    contract = packaged_action["contract"]
    proposal = _submit(service, packaged_action, payload=_original_payload(declaration))
    approved = service.approve_with_edit(
        proposal.proposal_id,
        actor=_REVIEWER,
        idempotency_key="operator-3-approve-with-edit-2",
        action_contract=contract,
        replacement_payload=_edited_payload(declaration),
        execute_before=_EXECUTE_BEFORE,
        reason_code="offset_corrected_by_operator",
        if_match=proposal.value.etag,
    )
    kwargs = _claim_kwargs(approved, contract, key="bench-worker-1-claim-2")
    first = service.claim(approved.proposal_id, **kwargs)
    replay = service.claim(approved.proposal_id, **kwargs)
    assert replay.replay is True
    assert replay.execution_id == first.execution_id

    # A worker that treats the replay as new work is the failure this guards.
    seen: set[str] = set()
    for result in (first, replay):
        if result.execution_id not in seen:
            seen.add(result.execution_id)
            bench.recalibrate(
                result.canonical_payload, execution_id=result.execution_id
            )
    assert len(bench.executed) == 1


# --------------------------------------------------------------------------
# (c) what must never execute
# --------------------------------------------------------------------------


def test_a_rejected_proposal_never_reaches_the_provider(
    service: ExternalActionService, packaged_action: dict[str, Any], bench: FakeLabBench
) -> None:
    """A refused review cannot be claimed, so no execution is authorized.

    The `bench.executed == []` line below is a consistency check, not a
    guard: this module is its own executor, so the bench was never going to
    be called on this path. What carries the weight is the exact refusal
    code from `claim`.
    """

    declaration = packaged_action["declaration"]
    contract = packaged_action["contract"]
    proposal = _submit(service, packaged_action, payload=_original_payload(declaration))
    rejected = service.reject(
        proposal.proposal_id,
        actor=_REVIEWER,
        idempotency_key="operator-3-reject-1",
        action_contract=contract,
        reason_code="out_of_tolerance",
        if_match=proposal.value.etag,
    )
    assert rejected.review_state == "rejected"
    assert rejected.execution_state == "unavailable"
    with pytest.raises(ExternalActionError) as refusal:
        service.claim(
            proposal.proposal_id,
            **_claim_kwargs(rejected, contract, key="bench-worker-1-claim-3"),
        )
    assert refusal.value.code == "illegal_proposal_transition"
    assert bench.executed == []
    assert bench.offsets == {}


def test_an_unapproved_proposal_cannot_be_claimed(
    service: ExternalActionService, packaged_action: dict[str, Any], bench: FakeLabBench
) -> None:
    """No decision at all is also not permission to act.

    As above, the refusal code is the assertion that matters; the empty
    bench log only confirms the test did not call the executor itself.
    """

    declaration = packaged_action["declaration"]
    contract = packaged_action["contract"]
    proposal = _submit(service, packaged_action, payload=_original_payload(declaration))
    assert proposal.current_decision_id is None
    with pytest.raises(ExternalActionError) as refusal:
        service.claim(
            proposal.proposal_id,
            **_claim_kwargs(proposal, contract, key="bench-worker-1-claim-4"),
        )
    assert refusal.value.code == "illegal_proposal_transition"
    assert bench.executed == []


def test_the_proposer_cannot_approve_its_own_action(
    service: ExternalActionService, packaged_action: dict[str, Any], bench: FakeLabBench
) -> None:
    """Separation of duties holds for the packaged action type too.

    The refusal code is the assertion that matters here too.
    """

    declaration = packaged_action["declaration"]
    contract = packaged_action["contract"]
    proposal = _submit(service, packaged_action, payload=_original_payload(declaration))
    with pytest.raises(ExternalActionError) as refusal:
        service.approve_with_edit(
            proposal.proposal_id,
            actor=_PROPOSER,
            idempotency_key="lab-sweep-self-approve-1",
            action_contract=contract,
            replacement_payload=_edited_payload(declaration),
            execute_before=_EXECUTE_BEFORE,
            reason_code="offset_corrected_by_operator",
            if_match=proposal.value.etag,
        )
    assert refusal.value.code == "proposer_approver_separation_required"
    assert bench.executed == []


def test_an_expired_approval_window_refuses_the_claim(
    service: ExternalActionService, packaged_action: dict[str, Any], bench: FakeLabBench
) -> None:
    """The approval is time-bound, so a late worker executes nothing.

    The clock moves past the 10:30Z execution deadline but stops short of
    the 11:00Z review deadline, so this is the execution window expiring
    and not the review. `claim` materializes that expiry before it checks
    `If-Match`, so a worker still holding the approval ETag is told its
    lifecycle is stale; a worker that retries without the ETag gets the
    deadline refusal itself. Both are checked, because either one alone
    would let a reordering of those two checks pass unnoticed.
    """

    declaration = packaged_action["declaration"]
    contract = packaged_action["contract"]
    proposal = _submit(service, packaged_action, payload=_original_payload(declaration))
    approved = service.approve_with_edit(
        proposal.proposal_id,
        actor=_REVIEWER,
        idempotency_key="operator-3-approve-with-edit-3",
        action_contract=contract,
        replacement_payload=_edited_payload(declaration),
        execute_before=_EXECUTE_BEFORE,
        reason_code="offset_corrected_by_operator",
        if_match=proposal.value.etag,
    )
    assert approved.execution_state == "ready"

    service.store.set_now(datetime(2026, 9, 18, 10, 45, tzinfo=timezone.utc))
    with pytest.raises(ExternalActionError) as stale:
        service.claim(
            approved.proposal_id,
            **_claim_kwargs(approved, contract, key="bench-worker-1-claim-5"),
        )
    assert stale.value.code == "stale_proposal_lifecycle"

    # Retry with the ETag the expiry transition just produced: the worker
    # is now current, and the refusal is the expiry itself.
    current = service.store.get(approved.proposal_id)
    retry = _claim_kwargs(approved, contract, key="bench-worker-1-claim-6")
    retry["if_match"] = current.etag
    with pytest.raises(ExternalActionError) as expired:
        service.claim(approved.proposal_id, **retry)
    assert expired.value.code == "proposal_expired"

    # The execution window closed, not the review window: the clock is
    # still short of the 11:00Z review deadline. If this ever stops being
    # true the test is measuring the wrong deadline.
    final = service.store.get(approved.proposal_id)
    assert final.review_state == "expired"
    assert service.store.now() < final.effective_review_expires_at
    assert bench.executed == []
