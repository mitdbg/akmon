"""Stable plan/activate/resume/rollback/retire lifecycle seam."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from functools import wraps
from typing import Any

from .effects import CompensationRunner, EffectAdapter, EffectRunner, RetirementRunner
from .ledger import ControllerLedger, LedgerError
from .protocol import (
    CONTROLLER_VERSION,
    RETIREMENT_EFFECTS,
    UPGRADE_EFFECT_KIND,
    AuthoritySigner,
    ControllerInstallation,
    EffectSpec,
    MutationCapability,
    ObservationProof,
    ProbeAttestation,
    ProbeResult,
    ProtocolError,
    ReleaseDescriptor,
    TargetPublicationPort,
    TransitionPlan,
    TransitionState,
    UpgradeAttestation,
    canonical_json,
    digest_json,
    require_digest,
)


class ControllerError(RuntimeError):
    pass


def _serialized_execution(method: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(method)
    def locked(self: LocalLifecycleController, *args: Any, **kwargs: Any) -> Any:
        with self.ledger.execution_lock():
            return method(self, *args, **kwargs)

    return locked


@dataclass(frozen=True)
class TransitionResult:
    state: str
    transition_id: str | None
    activation_receipt_digest: str | None = None
    capability: MutationCapability | None = None
    no_op: bool = False


FaultHook = Callable[[str], None]


class LocalLifecycleController:
    """External authority that invokes only caller-supplied protocol ports."""

    def __init__(
        self,
        *,
        ledger: ControllerLedger,
        signer: AuthoritySigner,
        fault_hook: FaultHook | None = None,
    ) -> None:
        self.ledger = ledger
        self.signer = signer
        self.installation: ControllerInstallation = ledger.installation
        self.installation.verify(signer)
        if signer.key_id != self.installation.controller_trust_root_id:
            raise ControllerError("controller signer does not own this installation")
        self.probe_signer = signer.signer_for("probe")
        self.upgrade_signer = signer.signer_for("upgrade")
        self.observation_signer = signer.signer_for("observation")
        self.controller_version = CONTROLLER_VERSION
        self.fault_hook = fault_hook or (lambda _point: None)

    def _fault(self, point: str) -> None:
        self.fault_hook(point)

    def _verify_observation_proof(
        self,
        result: Mapping[str, Any],
        *,
        subject_kind: str,
        subject_digest: str,
        required_fields: set[str],
    ) -> dict[str, Any]:
        if set(result) != required_fields | {"proof"}:
            raise ControllerError(f"{subject_kind} observation fields are not exact")
        raw_proof = result["proof"]
        if not isinstance(raw_proof, Mapping):
            raise ControllerError(f"{subject_kind} observation proof is absent")
        evidence = {key: result[key] for key in required_fields}
        try:
            proof = ObservationProof.from_mapping(raw_proof)
            proof.verify(
                signer=self.observation_signer,
                subject_kind=subject_kind,
                subject_digest=subject_digest,
                evidence=evidence,
            )
        except ProtocolError as exc:
            raise ControllerError(
                f"{subject_kind} observation proof is invalid: {exc}"
            ) from exc
        return evidence

    @staticmethod
    def _publication_subject(receipt: Any, phase: str) -> str:
        return digest_json(
            {
                "schema": "okg.local-controller.publication-subject/v1",
                "phase": phase,
                "receipt": asdict(receipt),
            }
        )

    def _verify_publication_observation(
        self,
        receipt: Any,
        result: Mapping[str, Any],
        *,
        phase: str,
    ) -> dict[str, Any]:
        fields = {
            "schema",
            "activation_receipt_digest",
            "generation_digest",
            "lifecycle_activation_epoch",
            "database_writer_epoch",
            "port_manifest_digest",
            "service_plan_digest",
            "status",
            "observed_at",
            "expires_at",
            "provenance",
            "visibility_switches",
            "publication_transaction_digest",
        }
        evidence = self._verify_observation_proof(
            result,
            subject_kind="publication",
            subject_digest=self._publication_subject(receipt, phase),
            required_fields=fields,
        )
        expected_status = "published" if phase == "publish" else "ready"
        expected = {
            "activation_receipt_digest": receipt.digest,
            "generation_digest": receipt.staged_generation_digest,
            "lifecycle_activation_epoch": receipt.lifecycle_activation_epoch,
            "database_writer_epoch": receipt.database_writer_epoch,
            "port_manifest_digest": receipt.port_manifest_digest,
            "service_plan_digest": receipt.service_plan_digest,
            "status": expected_status,
        }
        mismatches = {
            key: {"expected": value, "observed": evidence.get(key)}
            for key, value in expected.items()
            if evidence.get(key) != value
        }
        if mismatches:
            raise ControllerError(
                "publication observation disagrees with authorization: "
                + canonical_json(mismatches)
            )
        now = int(time.time())
        if (
            not isinstance(evidence["observed_at"], int)
            or not isinstance(evidence["expires_at"], int)
            or evidence["observed_at"] > now
            or evidence["expires_at"] <= now
        ):
            raise ControllerError("publication observation freshness is invalid")
        if not isinstance(evidence["provenance"], str) or not evidence["provenance"]:
            raise ControllerError("publication observation provenance is absent")
        return evidence

    def _verify_noop_observation(
        self, descriptor: ReleaseDescriptor, result: Mapping[str, Any]
    ) -> dict[str, Any]:
        fields = {
            "schema",
            "candidate_digest",
            "activation_receipt_digest",
            "generation_digest",
            "database_writer_epoch",
            "service_plan_digest",
            "status",
            "health",
            "publication_visible",
            "observed_at",
            "expires_at",
            "provenance",
        }
        subject = digest_json(
            {
                "schema": "okg.local-controller.no-op-subject/v1",
                "descriptor": descriptor.payload(),
            }
        )
        evidence = self._verify_observation_proof(
            result,
            subject_kind="no_op",
            subject_digest=subject,
            required_fields=fields,
        )
        expected = {
            "candidate_digest": descriptor.candidate_digest,
            "activation_receipt_digest": descriptor.expected_activation_receipt_digest,
            "generation_digest": descriptor.expected_predecessor,
            "database_writer_epoch": descriptor.expected_database_writer_epoch,
            "service_plan_digest": descriptor.service_plan_digest,
            "status": "ready",
            "health": "healthy",
            "publication_visible": True,
        }
        if any(evidence.get(key) != value for key, value in expected.items()):
            raise ControllerError(
                "no-op target state is not exactly ready and published"
            )
        now = int(time.time())
        if evidence["observed_at"] > now or evidence["expires_at"] <= now:
            raise ControllerError("no-op target observation is stale")
        return evidence

    def _verify_predecessor_observation(
        self, descriptor: ReleaseDescriptor, result: Mapping[str, Any]
    ) -> dict[str, Any]:
        fields = {
            "schema",
            "candidate_digest",
            "activation_receipt_digest",
            "generation_digest",
            "database_writer_epoch",
            "service_plan_digest",
            "status",
            "health",
            "publication_visible",
            "observed_at",
            "expires_at",
            "provenance",
        }
        subject = digest_json(
            {
                "schema": "okg.local-controller.no-op-subject/v1",
                "descriptor": descriptor.payload(),
            }
        )
        evidence = self._verify_observation_proof(
            result,
            subject_kind="no_op",
            subject_digest=subject,
            required_fields=fields,
        )
        expected = {
            "activation_receipt_digest": (
                descriptor.expected_activation_receipt_digest
            ),
            "generation_digest": descriptor.expected_predecessor,
            "database_writer_epoch": descriptor.expected_database_writer_epoch,
            "status": "ready",
            "health": "healthy",
            "publication_visible": True,
        }
        mismatches = {
            key: {"expected": value, "observed": evidence.get(key)}
            for key, value in expected.items()
            if evidence.get(key) != value
        }
        if mismatches:
            raise ControllerError(
                "live predecessor authority does not match the transition plan: "
                + canonical_json(mismatches)
            )
        now = int(time.time())
        if evidence["observed_at"] > now or evidence["expires_at"] <= now:
            raise ControllerError("predecessor target observation is stale")
        return evidence

    def _verify_probe(
        self, probe_result: ProbeResult
    ) -> tuple[ReleaseDescriptor, str, ProbeAttestation]:
        descriptor, staged_generation_digest = descriptor_from_candidate_output(
            probe_result.output
        )
        attestation = probe_result.attestation
        if descriptor.controller_min_version > self.controller_version:
            raise ControllerError(
                "candidate requires a newer compatible controller package"
            )
        if attestation.exit_status != 0:
            raise ControllerError("candidate probe did not exit successfully")
        if attestation.candidate_digest != descriptor.candidate_digest:
            raise ControllerError("probe attestation belongs to another candidate")
        if descriptor.deployment != self.installation.deployment:
            raise ControllerError("probe descriptor targets another installation")
        expected_input = {
            "operation": "plan",
            "deployment": descriptor.deployment,
            "candidate_digest": descriptor.candidate_digest,
        }
        if attestation.input_payload_digest != digest_json(expected_input):
            raise ControllerError("probe input is not the controller-owned plan input")
        if attestation.output_payload_digest != digest_json(probe_result.output):
            raise ControllerError(
                "probe output is not bound by the launcher attestation"
            )
        if attestation.sandbox_profile_digest != self.installation.probe_profile_digest:
            raise ControllerError("probe sandbox profile is not installation-approved")
        if attestation.launcher_digest != self.installation.probe_launcher_digest:
            raise ControllerError(
                "probe launcher identity is not installation-approved"
            )
        if attestation.limits_digest != self.installation.probe_limits_digest:
            raise ControllerError("probe resource limits are not installation-approved")
        now = int(time.time())
        if now < attestation.issued_at or now >= attestation.expires_at:
            raise ControllerError("probe launcher attestation is stale")
        if (
            attestation.expires_at - attestation.issued_at
            > self.installation.probe_max_ttl_seconds
        ):
            raise ControllerError("probe launcher attestation lifetime is too broad")
        if attestation.launcher_key_id != self.probe_signer.key_id:
            raise ControllerError("probe launcher key identity is not trusted")
        if attestation.mutation_canary_before != attestation.mutation_canary_after:
            raise ControllerError("candidate probe changed a mutation canary")
        if not self.probe_signer.verify(
            attestation.unsigned_payload(), attestation.signature
        ):
            raise ControllerError("probe launcher attestation is not trusted")
        return descriptor, staged_generation_digest, attestation

    def plan(
        self,
        *,
        probe_result: ProbeResult,
        target: TargetPublicationPort,
        port_manifest_digest: str,
        capacity_reservation_digest: str,
        backup_digest: str,
        current_lifecycle_epoch: int,
        staged_capability_expires_at: int,
    ) -> tuple[TransitionPlan, MutationCapability]:
        # Every compatibility and attestation check precedes the first durable
        # transition intent, let alone quiesce or product mutation.
        descriptor, staged_generation_digest, probe_attestation = self._verify_probe(
            probe_result
        )
        if descriptor.candidate_digest == descriptor.predecessor_digest:
            raise ControllerError(
                "candidate equality is not a verified no-op; "
                "use activate with a live target observation"
            )
        predecessor_observation = target.observe_current_state(descriptor)
        self._verify_predecessor_observation(descriptor, predecessor_observation)
        effect_set_digest = digest_json(
            [effect.payload() for effect in descriptor.effects]
        )
        irreversible = [
            index
            for index, effect in enumerate(descriptor.effects, start=1)
            if effect.irreversible
        ]
        identity = {
            "descriptor_digest": descriptor.digest,
            "ledger_uuid": self.installation.ledger_uuid,
            "controller_trust_root_id": self.installation.controller_trust_root_id,
            "probe_attestation_id": probe_attestation.attestation_id,
            "port_manifest_digest": port_manifest_digest,
            "staged_generation_digest": staged_generation_digest,
            "capacity_reservation_digest": capacity_reservation_digest,
            "backup_digest": backup_digest,
            "lifecycle_activation_epoch": current_lifecycle_epoch + 1,
            "expected_database_writer_epoch": (
                descriptor.expected_database_writer_epoch
            ),
            "database_writer_epoch": descriptor.database_writer_epoch,
            "effect_set_digest": effect_set_digest,
        }
        transition_id = "controller-transition-" + digest_json(identity).removeprefix(
            "sha256:"
        )
        plan = TransitionPlan(
            transition_id=transition_id,
            ledger_uuid=self.installation.ledger_uuid,
            controller_trust_root_id=self.installation.controller_trust_root_id,
            descriptor_digest=descriptor.digest,
            probe_attestation_id=probe_attestation.attestation_id,
            port_manifest_digest=port_manifest_digest,
            deployment=descriptor.deployment,
            candidate_digest=descriptor.candidate_digest,
            predecessor_digest=descriptor.predecessor_digest,
            expected_predecessor=descriptor.expected_predecessor,
            expected_activation_receipt_digest=descriptor.expected_activation_receipt_digest,
            environment_identity=descriptor.environment_identity,
            database_instance_identity=descriptor.database_instance_identity,
            service_plan_digest=descriptor.service_plan_digest,
            queue_scope_digest=descriptor.queue_scope_digest,
            promotion_approval_digest=descriptor.promotion_approval_digest,
            evidence_digest=descriptor.evidence_digest,
            staged_generation_digest=staged_generation_digest,
            capacity_reservation_digest=capacity_reservation_digest,
            backup_digest=backup_digest,
            lifecycle_activation_epoch=current_lifecycle_epoch + 1,
            expected_database_writer_epoch=(
                descriptor.expected_database_writer_epoch
            ),
            database_writer_epoch=descriptor.database_writer_epoch,
            dbos_application_version=descriptor.dbos_application_version,
            effect_set_digest=effect_set_digest,
            irreversible_boundary=min(irreversible) if irreversible else None,
            staged_capability_expires_at=staged_capability_expires_at,
        )
        self.ledger.create_transition(plan, descriptor.effects)
        self._fault("after_plan_intent")
        staged_capability = self._staged_capability(plan.payload())
        return plan, staged_capability

    def _staged_capability(
        self, plan: Mapping[str, Any], *, expires_at: int | None = None
    ) -> MutationCapability:
        return self.signer.capability(
            transition_id=plan["transition_id"],
            ledger_uuid=plan["ledger_uuid"],
            controller_trust_root_id=plan["controller_trust_root_id"],
            receipt_digest=None,
            provisional_plan_digest=digest_json(dict(plan)),
            lifecycle_epoch=plan["lifecycle_activation_epoch"],
            database_writer_epoch=plan["database_writer_epoch"],
            effect_set_digest=plan["effect_set_digest"],
            mode="staged",
            expires_at=(
                plan["staged_capability_expires_at"]
                if expires_at is None
                else expires_at
            ),
        )

    def _bound_probe_retry(
        self,
        *,
        probe_result: ProbeResult,
        port_manifest_digest: str,
        capacity_reservation_digest: str,
        backup_digest: str,
        current_lifecycle_epoch: int,
    ) -> dict[str, Any] | None:
        existing = self.ledger.transition_for_probe(
            probe_result.attestation.attestation_id
        )
        if existing is None:
            return None
        descriptor, staged_generation_digest = descriptor_from_candidate_output(
            probe_result.output
        )
        attestation = probe_result.attestation
        if (
            attestation.candidate_digest != descriptor.candidate_digest
            or attestation.output_payload_digest != digest_json(probe_result.output)
            or attestation.launcher_key_id != self.probe_signer.key_id
            or not self.probe_signer.verify(
                attestation.unsigned_payload(), attestation.signature
            )
        ):
            raise ControllerError(
                "retry probe does not match the trusted consumed attestation"
            )
        plan = existing["plan"]
        expected = {
            "descriptor_digest": descriptor.digest,
            "probe_attestation_id": attestation.attestation_id,
            "port_manifest_digest": port_manifest_digest,
            "staged_generation_digest": staged_generation_digest,
            "capacity_reservation_digest": capacity_reservation_digest,
            "backup_digest": backup_digest,
            "lifecycle_activation_epoch": current_lifecycle_epoch + 1,
        }
        mismatches = {
            key: {"expected": value, "durable": plan.get(key)}
            for key, value in expected.items()
            if plan.get(key) != value
        }
        if mismatches:
            raise ControllerError(
                "activate retry disagrees with the durable transition: "
                + canonical_json(mismatches)
            )
        return existing

    def activate(
        self,
        *,
        probe_result: ProbeResult,
        port_manifest_digest: str,
        capacity_reservation_digest: str,
        backup_digest: str,
        current_lifecycle_epoch: int,
        staged_capability_expires_at: int,
        adapters: Mapping[str, EffectAdapter],
        target: TargetPublicationPort,
    ) -> TransitionResult:
        existing = self._bound_probe_retry(
            probe_result=probe_result,
            port_manifest_digest=port_manifest_digest,
            capacity_reservation_digest=capacity_reservation_digest,
            backup_digest=backup_digest,
            current_lifecycle_epoch=current_lifecycle_epoch,
        )
        if existing is not None:
            plan = existing["plan"]
            if TransitionState(existing["state"]) in {
                TransitionState.PLANNED,
                TransitionState.APPLYING,
            }:
                stage = getattr(target, "stage_generation", None)
                if stage is None:
                    raise ControllerError(
                        "target adapter has no staged-generation boundary"
                    )
                capability_expires_at = plan["staged_capability_expires_at"]
                if capability_expires_at <= int(time.time()):
                    capability_expires_at = (
                        int(time.time()) + self.installation.probe_max_ttl_seconds
                    )
                    self.ledger.record_staged_capability_renewal(
                        existing["transition_id"],
                        expires_at=capability_expires_at,
                    )
                stage(
                    staged_digest=plan["staged_generation_digest"],
                    candidate_digest=plan["candidate_digest"],
                    capability=self._staged_capability(
                        plan, expires_at=capability_expires_at
                    ),
                )
            return self.resume(
                transition_id=existing["transition_id"],
                adapters=adapters,
                target=target,
                port_manifest_digest=port_manifest_digest,
            )

        descriptor, _staged_generation_digest, attestation = self._verify_probe(
            probe_result
        )
        if descriptor.candidate_digest == descriptor.predecessor_digest:
            observation = target.observe_current_state(descriptor)
            self._verify_noop_observation(descriptor, observation)
            self.ledger.consume_noop_probe(attestation.attestation_id)
            return TransitionResult(state="no_op", transition_id=None, no_op=True)
        plan, staged_capability = self.plan(
            probe_result=probe_result,
            target=target,
            port_manifest_digest=port_manifest_digest,
            capacity_reservation_digest=capacity_reservation_digest,
            backup_digest=backup_digest,
            current_lifecycle_epoch=current_lifecycle_epoch,
            staged_capability_expires_at=staged_capability_expires_at,
        )
        stage = getattr(target, "stage_generation", None)
        if stage is None:
            raise ControllerError("target adapter has no staged-generation boundary")
        stage(
            staged_digest=plan.staged_generation_digest,
            candidate_digest=plan.candidate_digest,
            capability=staged_capability,
        )
        self._fault("after_staged_generation")
        return self.resume(
            transition_id=plan.transition_id,
            adapters=adapters,
            target=target,
            port_manifest_digest=port_manifest_digest,
        )

    @_serialized_execution
    def resume(
        self,
        *,
        transition_id: str,
        adapters: Mapping[str, EffectAdapter],
        target: TargetPublicationPort,
        port_manifest_digest: str,
    ) -> TransitionResult:
        transition = self.ledger.transition(transition_id)
        plan = transition["plan"]
        if plan["port_manifest_digest"] != port_manifest_digest:
            raise ControllerError(
                "resume adapter set differs from the original transition"
            )
        required_adapter_kinds = {
            effect["kind"] for effect in self.ledger.effects(transition_id)
        }
        if not required_adapter_kinds <= set(adapters):
            raise ControllerError("resume is missing a pinned effect adapter")
        state = TransitionState(transition["state"])
        if state in {TransitionState.READY, TransitionState.ROLLED_BACK}:
            if not self.ledger.is_current_attempt(
                transition_id,
                lifecycle_activation_epoch=(
                    transition["active_capability"][
                        "lifecycle_activation_epoch"
                    ]
                    if transition.get("active_capability") is not None
                    else None
                ),
            ):
                raise ControllerError(
                    "transition is not the deployment's current attempt"
                )
            receipt_kind = (
                "rollback" if state == TransitionState.ROLLED_BACK else "candidate"
            )
            receipt = self.ledger.receipt(transition_id, receipt_kind)
            observation = target.observe_authorized_generation(receipt)
            self._verify_publication_observation(receipt, observation, phase="ready")
            capability = transition.get("active_capability")
            return TransitionResult(
                state=state,
                transition_id=transition_id,
                activation_receipt_digest=transition.get("published_receipt_digest"),
                capability=(
                    MutationCapability(**capability) if capability is not None else None
                ),
            )
        if state == TransitionState.BLOCKED:
            raise ControllerError(
                "transition crossed an irreversible boundary and requires rollback"
            )
        if state in {TransitionState.PLANNED, TransitionState.APPLYING}:
            runner = EffectRunner(
                ledger=self.ledger,
                adapters=adapters,
                observation_signer=self.observation_signer,
                fault_hook=self.fault_hook,
            )
            runner.run_prepublication(transition_id)
            transition = self.ledger.transition(transition_id)
            state = TransitionState(transition["state"])
        if state in {TransitionState.APPLYING, TransitionState.PLANNED}:
            try:
                self._consume_upgrade_attestations(transition_id, plan)
            except Exception:
                if self.ledger.transition(transition_id)["irreversible_crossed"]:
                    self.ledger.block_transition(
                        transition_id,
                        reason="irreversible upgrade attestation verification failed",
                    )
                raise
            receipt = self.ledger.authorize_activation(
                transition_id=transition_id,
                staged_generation_digest=plan["staged_generation_digest"],
                signer=self.signer,
            )
            self._fault("after_authorize_activation")
            state = TransitionState.AUTHORIZED
        else:
            receipt = self.ledger.receipt(transition_id)
        if state == TransitionState.AUTHORIZED:
            outcome = target.publish_authorized_generation(receipt)
            verified_outcome = self._verify_publication_observation(
                receipt, outcome, phase="publish"
            )
            self.ledger.record_publication(transition_id, receipt, verified_outcome)
            self._fault("after_publish_authorized_generation")
            state = TransitionState.PUBLISHED
        if state == TransitionState.PUBLISHED:
            observation = target.observe_authorized_generation(receipt)
            self._verify_publication_observation(
                receipt, observation, phase="ready"
            )
            self._fault("after_observe_publication")
            runner = EffectRunner(
                ledger=self.ledger,
                adapters=adapters,
                observation_signer=self.observation_signer,
                fault_hook=self.fault_hook,
            )
            runner.run_effect_key(transition_id, "unfreeze")
            verified_observation = self._verify_publication_observation(
                receipt,
                target.observe_authorized_generation(receipt),
                phase="ready",
            )
            capability = self.signer.capability(
                transition_id=transition_id,
                ledger_uuid=receipt.ledger_uuid,
                controller_trust_root_id=receipt.controller_trust_root_id,
                receipt_digest=receipt.digest,
                provisional_plan_digest=None,
                lifecycle_epoch=receipt.lifecycle_activation_epoch,
                database_writer_epoch=receipt.database_writer_epoch,
                effect_set_digest=receipt.effect_set_digest,
                mode="active",
                expires_at=None,
            )
            self.ledger.finalize_observation(
                transition_id,
                receipt,
                verified_observation,
                asdict(capability),
            )
            self._fault("after_finalize_ready")
            return TransitionResult(
                state=TransitionState.READY,
                transition_id=transition_id,
                activation_receipt_digest=receipt.digest,
                capability=capability,
            )
        raise ControllerError(f"transition cannot resume from {state}")

    def _consume_upgrade_attestations(
        self, transition_id: str, plan: Mapping[str, Any]
    ) -> None:
        for effect in self.ledger.effects(transition_id):
            if effect["kind"] != UPGRADE_EFFECT_KIND:
                continue
            observation = effect.get("observation")
            if not isinstance(observation, Mapping) or set(observation) != {
                "satisfied",
                "upgrade_attestation"
            }:
                raise ControllerError(
                    "upgrade observation must contain one exact attestation"
                )
            if observation["satisfied"] is not True:
                raise ControllerError("upgrade observation is not satisfied")
            raw = observation["upgrade_attestation"]
            if not isinstance(raw, Mapping):
                raise ControllerError("upgrade attestation must be an object")
            try:
                attestation = UpgradeAttestation.from_mapping(raw)
            except ProtocolError as exc:
                raise ControllerError(f"upgrade attestation is invalid: {exc}") from exc
            request = effect["request"]
            expected = {
                "issuer": request["issuer"],
                "issuer_key_id": request["attestation_key_id"],
                "environment_identity": request["environment_identity"],
                "database_instance_identity": request["database_instance_identity"],
                "artifact_digest": plan["candidate_digest"],
                "reconcile_plan_digest": plan["descriptor_digest"],
                "saga_transition_id": transition_id,
                "database_writer_epoch": plan["database_writer_epoch"],
                "backup_digest": plan["backup_digest"],
                "previous_artifact_digest": plan["predecessor_digest"],
                "candidate_artifact_digest": plan["candidate_digest"],
                "schema_before_digest": request["expected_schema_before_digest"],
                "migration_manifest_digest": request["migration_manifest_digest"],
                "rollback_compatibility_class": request["rollback_compatibility_class"],
                "restore_strategy": request["restore_strategy"],
            }
            mismatches = {
                field: {"expected": value, "observed": getattr(attestation, field)}
                for field, value in expected.items()
                if getattr(attestation, field) != value
            }
            if mismatches:
                raise ControllerError(
                    "upgrade attestation disagrees with the exact transition: "
                    + canonical_json(mismatches)
                )
            if attestation.issuer_key_id != self.upgrade_signer.key_id:
                raise ControllerError("upgrade attestation key identity is not trusted")
            if not self.upgrade_signer.verify(
                attestation.unsigned_payload(), attestation.signature
            ):
                raise ControllerError("upgrade attestation signature is invalid")
            ttl = attestation.expires_at - attestation.issued_at
            if ttl > request["max_attestation_ttl_seconds"]:
                raise ControllerError(
                    "upgrade attestation exceeds its bounded lifetime"
                )
            now = int(time.time())
            if attestation.issued_at > now or attestation.expires_at <= now:
                raise ControllerError("upgrade attestation is not current")
            if attestation.result == "no_op" and (
                attestation.schema_after_digest != attestation.schema_before_digest
                or attestation.applied_migrations
            ):
                raise ControllerError("no-op attestation contains an upgrade delta")
            existing = self.ledger.upgrade_attestation_consumption(
                transition_id, effect["effect_key"]
            )
            if existing is not None and (
                existing["attestation_id"] != attestation.attestation_id
                or existing["content_digest"] != attestation.content_digest
            ):
                raise ControllerError(
                    "upgrade effect already consumed another attestation"
                )
            self.ledger.consume_upgrade_attestation(
                transition_id=transition_id,
                effect_key=effect["effect_key"],
                attestation=attestation,
            )

    @_serialized_execution
    def rollback(
        self,
        *,
        transition_id: str,
        lifecycle_activation_epoch: int,
        database_writer_epoch: int,
        adapters: Mapping[str, EffectAdapter],
        target: TargetPublicationPort,
        port_manifest_digest: str,
    ) -> TransitionResult:
        transition = self.ledger.transition(transition_id)
        plan = transition["plan"]
        if plan["port_manifest_digest"] != port_manifest_digest:
            raise ControllerError(
                "rollback adapter set differs from the original transition"
            )
        receipt = self.ledger.authorize_rollback(
            transition_id=transition_id,
            lifecycle_activation_epoch=lifecycle_activation_epoch,
            database_writer_epoch=database_writer_epoch,
            signer=self.signer,
        )
        self._fault("after_authorize_rollback")
        required_adapter_kinds = {
            effect["kind"] for effect in self.ledger.compensations(transition_id)
        }
        if not required_adapter_kinds <= set(adapters):
            raise ControllerError("rollback is missing a pinned compensation adapter")
        CompensationRunner(
            ledger=self.ledger,
            adapters=adapters,
            observation_signer=self.observation_signer,
            fault_hook=self.fault_hook,
        ).run_all(transition_id)
        stage = getattr(target, "stage_generation", None)
        if stage is None:
            raise ControllerError("target adapter has no staged-generation boundary")
        capability = self.signer.capability(
            transition_id=transition_id,
            ledger_uuid=receipt.ledger_uuid,
            controller_trust_root_id=receipt.controller_trust_root_id,
            receipt_digest=None,
            provisional_plan_digest=receipt.digest,
            lifecycle_epoch=receipt.lifecycle_activation_epoch,
            database_writer_epoch=receipt.database_writer_epoch,
            effect_set_digest=receipt.effect_set_digest,
            mode="staged",
            expires_at=int(time.time()) + 300,
        )
        stage(
            staged_digest=receipt.staged_generation_digest,
            candidate_digest=receipt.candidate_digest,
            capability=capability,
        )
        outcome = target.publish_authorized_generation(receipt)
        verified_outcome = self._verify_publication_observation(
            receipt, outcome, phase="publish"
        )
        self.ledger.record_publication(
            transition_id, receipt, verified_outcome, kind="rollback"
        )
        self._fault("after_publish_rollback")
        observation = target.observe_authorized_generation(receipt)
        verified_observation = self._verify_publication_observation(
            receipt, observation, phase="ready"
        )
        active = self.signer.capability(
            transition_id=transition_id,
            ledger_uuid=receipt.ledger_uuid,
            controller_trust_root_id=receipt.controller_trust_root_id,
            receipt_digest=receipt.digest,
            provisional_plan_digest=None,
            lifecycle_epoch=receipt.lifecycle_activation_epoch,
            database_writer_epoch=receipt.database_writer_epoch,
            effect_set_digest=receipt.effect_set_digest,
            mode="active",
            expires_at=None,
        )
        self.ledger.finalize_observation(
            transition_id,
            receipt,
            verified_observation,
            asdict(active),
            kind="rollback",
        )
        self._fault("after_finalize_rollback")
        return TransitionResult(
            state=TransitionState.ROLLED_BACK,
            transition_id=transition_id,
            activation_receipt_digest=receipt.digest,
            capability=active,
        )

    @_serialized_execution
    def retire(
        self,
        *,
        transition_id: str,
        adapters: Mapping[str, EffectAdapter],
        target: TargetPublicationPort,
        port_manifest_digest: str,
    ) -> TransitionResult:
        transition = self.ledger.transition(transition_id)
        plan = transition["plan"]
        if plan["port_manifest_digest"] != port_manifest_digest:
            raise ControllerError(
                "retirement adapter set differs from the original transition"
            )
        receipt_kind = (
            "rollback"
            if transition.get("published_receipt_digest")
            == self._optional_receipt_digest(transition_id, "rollback")
            else "candidate"
        )
        receipt = self.ledger.receipt(transition_id, receipt_kind)
        previous: tuple[str, ...] = ()
        effects: list[EffectSpec] = []
        for key, kind in RETIREMENT_EFFECTS:
            request = {
                "schema": "okg.local-controller.retirement-effect-request/v1",
                "transition_id": transition_id,
                "activation_receipt_digest": receipt.digest,
                "ledger_uuid": receipt.ledger_uuid,
                "controller_trust_root_id": receipt.controller_trust_root_id,
                "port_manifest_digest": receipt.port_manifest_digest,
                "service_plan_digest": receipt.service_plan_digest,
                "operation_digest": digest_json(
                    {"retirement_effect": key, "receipt": receipt.digest}
                ),
            }
            effects.append(
                EffectSpec(
                    key=key,
                    kind=kind,
                    request=request,
                    predecessors=previous,
                )
            )
            previous = (key,)
        retirement = self.ledger.create_retirement(
            transition_id=transition_id,
            receipt=receipt,
            port_manifest_digest=port_manifest_digest,
            effects=effects,
        )
        RetirementRunner(
            ledger=self.ledger,
            adapters=adapters,
            observation_signer=self.observation_signer,
            fault_hook=self.fault_hook,
        ).run_all(retirement["retirement_id"])
        observation = target.observe_retirement(receipt)
        verified = self._verify_retirement_observation(receipt, observation)
        self.ledger.mark_retired(transition_id, observation=verified)
        return TransitionResult(
            state=TransitionState.RETIRED, transition_id=transition_id
        )

    def _optional_receipt_digest(self, transition_id: str, kind: str) -> str | None:
        try:
            return self.ledger.receipt(transition_id, kind).digest
        except LedgerError:
            return None

    @staticmethod
    def _retirement_subject(receipt: Any) -> str:
        return digest_json(
            {
                "schema": "okg.local-controller.retirement-subject/v1",
                "receipt": asdict(receipt),
            }
        )

    def _verify_retirement_observation(
        self, receipt: Any, result: Mapping[str, Any]
    ) -> dict[str, Any]:
        fields = {
            "schema",
            "activation_receipt_digest",
            "generation_digest",
            "services_stopped",
            "queue_drained",
            "publication_visible",
            "capability_withdrawn",
            "observed_at",
            "expires_at",
            "provenance",
        }
        evidence = self._verify_observation_proof(
            result,
            subject_kind="retirement",
            subject_digest=self._retirement_subject(receipt),
            required_fields=fields,
        )
        expected = {
            "activation_receipt_digest": receipt.digest,
            "generation_digest": receipt.staged_generation_digest,
            "services_stopped": True,
            "queue_drained": True,
            "publication_visible": False,
            "capability_withdrawn": True,
        }
        if any(evidence.get(key) != value for key, value in expected.items()):
            raise ControllerError("retirement is not independently complete")
        now = int(time.time())
        if evidence["observed_at"] > now or evidence["expires_at"] <= now:
            raise ControllerError("retirement observation is stale")
        return evidence

    def status(
        self,
        transition_id: str,
        *,
        target: TargetPublicationPort | None = None,
        port_manifest_digest: str | None = None,
    ) -> dict[str, Any]:
        transition = self.ledger.transition(transition_id)
        state = transition["state"]
        if (
            port_manifest_digest is not None
            and transition["plan"]["port_manifest_digest"]
            != port_manifest_digest
        ):
            raise ControllerError("status adapter set differs from activation")
        if state == TransitionState.AUTHORIZED:
            transition["operator_state"] = "read_only_stale"
            transition["broad_writes_allowed"] = False
        elif state == TransitionState.PUBLISHED:
            transition["operator_state"] = "published_not_ready"
            transition["broad_writes_allowed"] = False
        elif state == TransitionState.RETIRING:
            transition["operator_state"] = "retiring_write_blocked"
            transition["broad_writes_allowed"] = False
        else:
            transition["operator_state"] = state
            transition["broad_writes_allowed"] = False
        transition["live_observation"] = None
        if state not in {
            TransitionState.READY,
            TransitionState.ROLLED_BACK,
            TransitionState.RETIRED,
        }:
            return transition
        if not self.ledger.is_current_attempt(transition_id):
            transition["operator_state"] = "superseded_by_newer_attempt"
            transition["broad_writes_allowed"] = False
            return transition
        if target is None or port_manifest_digest is None:
            transition["operator_state"] = "live_state_unavailable"
            transition["live_error"] = "signed live target observation is required"
            return transition
        if transition["plan"]["port_manifest_digest"] != port_manifest_digest:
            transition["operator_state"] = "live_state_unavailable"
            transition["live_error"] = "adapter set differs from activation"
            return transition
        try:
            receipt_kind = (
                "rollback" if state == TransitionState.ROLLED_BACK else "candidate"
            )
            if state == TransitionState.RETIRED:
                receipt_kind = (
                    "rollback"
                    if transition.get("published_receipt_digest")
                    == self._optional_receipt_digest(transition_id, "rollback")
                    else "candidate"
                )
            receipt = self.ledger.receipt(transition_id, receipt_kind)
            if state == TransitionState.RETIRED:
                observation = self._verify_retirement_observation(
                    receipt, target.observe_retirement(receipt)
                )
                transition["operator_state"] = TransitionState.RETIRED
            else:
                observation = self._verify_publication_observation(
                    receipt,
                    target.observe_authorized_generation(receipt),
                    phase="ready",
                )
                transition["operator_state"] = state
                transition["broad_writes_allowed"] = True
            transition["live_observation"] = observation
        except Exception as exc:
            transition["operator_state"] = "live_state_unavailable"
            transition["broad_writes_allowed"] = False
            transition["live_error"] = str(exc)
        return transition

    def verify_capability(
        self,
        capability: MutationCapability,
        *,
        target: TargetPublicationPort | None = None,
        port_manifest_digest: str | None = None,
        require_active: bool = True,
    ) -> None:
        if not self.signer.verify(capability.unsigned_payload(), capability.token):
            raise ControllerError("mutation capability signature is invalid")
        if require_active and capability.mode != "active":
            raise ControllerError("broad mutation requires an active capability")
        transition = self.ledger.transition(capability.transition_id)
        if (
            capability.ledger_uuid != self.installation.ledger_uuid
            or capability.controller_trust_root_id
            != self.installation.controller_trust_root_id
        ):
            raise ControllerError("mutation capability belongs to another installation")
        if transition["state"] not in {
            TransitionState.READY,
            TransitionState.ROLLED_BACK,
        }:
            raise ControllerError("mutation capability transition is not ready")
        if not self.ledger.is_current_attempt(
            capability.transition_id,
            lifecycle_activation_epoch=capability.lifecycle_activation_epoch,
        ):
            raise ControllerError(
                "mutation capability is not current; "
                "it is fenced by a newer deployment attempt"
            )
        active = transition.get("active_capability")
        if active is None or active.get("token") != capability.token:
            raise ControllerError("mutation capability is not current")
        if (
            active["lifecycle_activation_epoch"]
            != capability.lifecycle_activation_epoch
            or active["database_writer_epoch"] != capability.database_writer_epoch
            or active["activation_receipt_digest"]
            != capability.activation_receipt_digest
        ):
            raise ControllerError("mutation capability epochs or receipt do not match")
        if (
            transition.get("published_receipt_digest")
            != capability.activation_receipt_digest
        ):
            raise ControllerError(
                "mutation capability receipt is not published as current"
            )
        receipt = self.ledger.receipt_status(capability.activation_receipt_digest)
        if (
            receipt["invalidated"]
            or not receipt["published"]
            or not receipt["observed"]
        ):
            raise ControllerError("mutation capability receipt is not active")
        if require_active:
            if target is None or port_manifest_digest is None:
                raise ControllerError(
                    "broad mutation requires a fresh signed target observation"
                )
            if transition["plan"]["port_manifest_digest"] != port_manifest_digest:
                raise ControllerError("capability adapter set is not current")
            receipt_kind = (
                "rollback"
                if transition["state"] == TransitionState.ROLLED_BACK
                else "candidate"
            )
            active_receipt = self.ledger.receipt(capability.transition_id, receipt_kind)
            self._verify_publication_observation(
                active_receipt,
                target.observe_authorized_generation(active_receipt),
                phase="ready",
            )


def descriptor_from_candidate_output(
    payload: Mapping[str, Any],
) -> tuple[ReleaseDescriptor, str]:
    """Turn inert candidate JSON into trusted wire types after launcher attestation."""
    try:
        if set(payload) != {"schema", "descriptor", "staged_generation_digest"}:
            raise ProtocolError("candidate output fields are not exact")
        if payload["schema"] != "okg.local-controller.candidate-output/v1":
            raise ProtocolError("candidate output schema is unsupported")
        descriptor = payload["descriptor"]
        if not isinstance(descriptor, Mapping):
            raise ProtocolError("candidate descriptor must be an object")
        release = ReleaseDescriptor.from_mapping(descriptor)
        staged = payload["staged_generation_digest"]
        require_digest(staged, "staged_generation_digest")
        return release, staged
    except (KeyError, TypeError, ProtocolError) as exc:
        raise ControllerError(f"candidate output is malformed: {exc}") from exc
