"""Crash-recoverable intent/apply/observe/commit effect protocol."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from .ledger import ControllerLedger, LedgerError
from .protocol import (
    AuthoritySigner,
    EffectPhase,
    ObservationProof,
    ProtocolError,
    digest_json,
)


class EffectError(RuntimeError):
    pass


class InjectedControllerCrash(RuntimeError):
    """Test-only interruption after a named durable boundary."""


@dataclass(frozen=True)
class EffectContext:
    transition_id: str
    effect_key: str
    kind: str
    request: Mapping[str, Any]
    request_digest: str
    irreversible: bool
    descriptor_digest: str
    candidate_digest: str
    predecessor_digest: str
    backup_digest: str
    lifecycle_activation_epoch: int
    database_writer_epoch: int
    ledger_uuid: str
    controller_trust_root_id: str
    port_manifest_digest: str

    def unsigned_payload(self) -> dict[str, Any]:
        return {
            "transition_id": self.transition_id,
            "effect_key": self.effect_key,
            "kind": self.kind,
            "request": dict(self.request),
            "request_digest": self.request_digest,
            "irreversible": self.irreversible,
            "descriptor_digest": self.descriptor_digest,
            "candidate_digest": self.candidate_digest,
            "predecessor_digest": self.predecessor_digest,
            "backup_digest": self.backup_digest,
            "lifecycle_activation_epoch": self.lifecycle_activation_epoch,
            "database_writer_epoch": self.database_writer_epoch,
            "ledger_uuid": self.ledger_uuid,
            "controller_trust_root_id": self.controller_trust_root_id,
            "port_manifest_digest": self.port_manifest_digest,
        }

    def payload(self) -> dict[str, Any]:
        return self.unsigned_payload()

    @property
    def observation_subject_digest(self) -> str:
        return digest_json(
            {
                "schema": "okg.local-controller.effect-observation-subject/v1",
                **self.payload(),
            }
        )


@dataclass(frozen=True)
class CompensationContext:
    transition_id: str
    compensation_key: str
    forward_effect_key: str
    kind: str
    request: Mapping[str, Any]
    request_digest: str
    rollback_receipt_digest: str
    lifecycle_activation_epoch: int
    expected_database_writer_epoch: int
    database_writer_epoch: int
    ledger_uuid: str
    controller_trust_root_id: str
    port_manifest_digest: str

    def unsigned_payload(self) -> dict[str, Any]:
        return {
            "transition_id": self.transition_id,
            "compensation_key": self.compensation_key,
            "forward_effect_key": self.forward_effect_key,
            "kind": self.kind,
            "request": dict(self.request),
            "request_digest": self.request_digest,
            "rollback_receipt_digest": self.rollback_receipt_digest,
            "lifecycle_activation_epoch": self.lifecycle_activation_epoch,
            "expected_database_writer_epoch": self.expected_database_writer_epoch,
            "database_writer_epoch": self.database_writer_epoch,
            "ledger_uuid": self.ledger_uuid,
            "controller_trust_root_id": self.controller_trust_root_id,
            "port_manifest_digest": self.port_manifest_digest,
        }

    def payload(self) -> dict[str, Any]:
        return self.unsigned_payload()

    @property
    def observation_subject_digest(self) -> str:
        return digest_json(
            {
                "schema": "okg.local-controller.compensation-observation-subject/v1",
                **self.payload(),
            }
        )


@dataclass(frozen=True)
class EffectObservation:
    evidence: Mapping[str, Any]
    proof: ObservationProof

    def __post_init__(self) -> None:
        if not isinstance(self.evidence, Mapping):
            raise ValueError("effect observation evidence must be an object")
        if not isinstance(self.evidence.get("satisfied"), bool):
            raise ValueError(
                "effect observation evidence must bind a boolean satisfaction verdict"
            )

    @property
    def satisfied(self) -> bool:
        return self.evidence["satisfied"]


class EffectAdapter(Protocol):
    def apply(self, effect: EffectContext) -> Mapping[str, Any]:
        raise NotImplementedError

    def observe(self, effect: EffectContext) -> EffectObservation:
        raise NotImplementedError

    def compensate(self, effect: CompensationContext) -> Mapping[str, Any]:
        raise NotImplementedError

    def observe_compensation(
        self, effect: CompensationContext
    ) -> EffectObservation:
        raise NotImplementedError


FaultHook = Callable[[str], None]


class EffectRunner:
    def __init__(
        self,
        *,
        ledger: ControllerLedger,
        adapters: Mapping[str, EffectAdapter],
        observation_signer: AuthoritySigner,
        fault_hook: FaultHook | None = None,
    ) -> None:
        self.ledger = ledger
        self.adapters = dict(adapters)
        self.observation_signer = observation_signer
        self.fault_hook = fault_hook or (lambda _point: None)

    def _fault(self, point: str) -> None:
        self.fault_hook(point)

    def _context(
        self, row: Mapping[str, Any], *, plan: Mapping[str, Any]
    ) -> EffectContext:
        context = EffectContext(
            transition_id=row["transition_id"],
            effect_key=row["effect_key"],
            kind=row["kind"],
            request=row["request"],
            request_digest=row["request_digest"],
            irreversible=bool(row["irreversible"]),
            descriptor_digest=plan["descriptor_digest"],
            candidate_digest=plan["candidate_digest"],
            predecessor_digest=plan["predecessor_digest"],
            backup_digest=plan["backup_digest"],
            lifecycle_activation_epoch=plan["lifecycle_activation_epoch"],
            database_writer_epoch=plan["database_writer_epoch"],
            ledger_uuid=plan["ledger_uuid"],
            controller_trust_root_id=plan["controller_trust_root_id"],
            port_manifest_digest=plan["port_manifest_digest"],
        )
        return context

    def _verify_observation(
        self, context: EffectContext, observation: EffectObservation
    ) -> bool:
        if type(observation) is not EffectObservation:
            raise EffectError(
                f"effect {context.effect_key!r} observation envelope is not exact"
            )
        try:
            observation.proof.verify(
                signer=self.observation_signer,
                subject_kind="effect",
                subject_digest=context.observation_subject_digest,
                evidence=observation.evidence,
            )
        except ProtocolError as exc:
            raise EffectError(
                f"effect {context.effect_key!r} observation proof is invalid: {exc}"
            ) from exc
        return observation.satisfied

    def _adapter(self, kind: str) -> EffectAdapter:
        adapter = self.adapters.get(kind)
        if adapter is None:
            raise EffectError(
                f"no external adapter registered for effect kind {kind!r}"
            )
        return adapter

    def run_effect(self, row: Mapping[str, Any]) -> None:
        transition_id = row["transition_id"]
        effect_key = row["effect_key"]
        phase = EffectPhase(row["phase"])
        if phase == EffectPhase.COMMITTED:
            return
        plan = self.ledger.transition(transition_id)["plan"]
        context = self._context(row, plan=plan)
        adapter = self._adapter(context.kind)

        try:
            if phase == EffectPhase.PLANNED:
                self.ledger.intend_effect(transition_id, effect_key)
                self._fault(f"after_intent:{effect_key}")
                phase = EffectPhase.INTENDED

            if phase == EffectPhase.INTENDED:
                if context.irreversible:
                    self.ledger.mark_irreversible_started(transition_id, effect_key)
                    self._fault(f"after_irreversible_boundary:{effect_key}")
                # An interruption may have happened after the external effect but
                # before its applied receipt. Observe first; never replay blindly.
                prior = adapter.observe(context)
                prior_satisfied = self._verify_observation(context, prior)
                if prior_satisfied:
                    self.ledger.observe_effect(
                        transition_id, effect_key, dict(prior.evidence)
                    )
                    self._fault(f"after_observe:{effect_key}")
                    self.ledger.commit_effect(transition_id, effect_key)
                    self._fault(f"after_commit:{effect_key}")
                    return
                applied = adapter.apply(context)
                self.ledger.mark_effect_applied(
                    transition_id, effect_key, digest_json(applied)
                )
                self._fault(f"after_apply:{effect_key}")
                phase = EffectPhase.APPLIED

            if phase == EffectPhase.APPLIED:
                observation = adapter.observe(context)
                observed_satisfied = self._verify_observation(context, observation)
                if not observed_satisfied:
                    raise EffectError(
                        f"effect {effect_key!r} was applied but is not "
                        "independently observed"
                    )
                self.ledger.observe_effect(
                    transition_id, effect_key, dict(observation.evidence)
                )
                self._fault(f"after_observe:{effect_key}")
                phase = EffectPhase.OBSERVED

            if phase == EffectPhase.OBSERVED:
                self.ledger.commit_effect(transition_id, effect_key)
                self._fault(f"after_commit:{effect_key}")
                return
            raise LedgerError(f"effect {effect_key!r} is blocked in phase {phase}")
        except InjectedControllerCrash:
            raise
        except Exception as exc:
            if self.ledger.transition(transition_id)["irreversible_crossed"]:
                self.ledger.block_transition(
                    transition_id,
                    reason=f"irreversible effect {effect_key} failed: {exc}",
                )
            raise

    def run_all(self, transition_id: str) -> None:
        self._run_matching(transition_id, excluded=frozenset())

    def run_prepublication(self, transition_id: str) -> None:
        """Commit every staged effect while leaving broad producer release planned."""

        self._run_matching(transition_id, excluded=frozenset({"unfreeze"}))

    def run_effect_key(self, transition_id: str, effect_key: str) -> None:
        rows = self.ledger.effects(transition_id)
        row = next((item for item in rows if item["effect_key"] == effect_key), None)
        if row is None:
            raise EffectError(f"unknown effect {effect_key!r}")
        committed = {
            item["effect_key"]
            for item in rows
            if item["phase"] == EffectPhase.COMMITTED
        }
        if not set(row["predecessors"]) <= committed:
            raise EffectError(f"effect {effect_key!r} predecessors are incomplete")
        self.run_effect(row)

    def _run_matching(
        self, transition_id: str, *, excluded: frozenset[str]
    ) -> None:
        while True:
            rows = self.ledger.effects(transition_id)
            selected = [row for row in rows if row["effect_key"] not in excluded]
            if all(row["phase"] == EffectPhase.COMMITTED for row in selected):
                return
            committed = {
                row["effect_key"]
                for row in rows
                if row["phase"] == EffectPhase.COMMITTED
            }
            runnable = [
                row
                for row in selected
                if row["phase"] != EffectPhase.COMMITTED
                and set(row["predecessors"]) <= committed
            ]
            if not runnable:
                raise EffectError("no effect is runnable; plan is cyclic or blocked")
            self.run_effect(runnable[0])


class RetirementRunner:
    """Crash-resumable runner over the separately persisted retirement saga."""

    def __init__(
        self,
        *,
        ledger: ControllerLedger,
        adapters: Mapping[str, EffectAdapter],
        observation_signer: AuthoritySigner,
        fault_hook: FaultHook | None = None,
    ) -> None:
        self.ledger = ledger
        self.adapters = dict(adapters)
        self.observation_signer = observation_signer
        self.fault_hook = fault_hook or (lambda _point: None)

    def _context(self, row: Mapping[str, Any]) -> EffectContext:
        transition = self.ledger.transition(row["transition_id"])
        plan = transition["plan"]
        context = EffectContext(
            transition_id=row["transition_id"],
            effect_key=row["effect_key"],
            kind=row["kind"],
            request=row["request"],
            request_digest=row["request_digest"],
            irreversible=False,
            descriptor_digest=plan["descriptor_digest"],
            candidate_digest=plan["candidate_digest"],
            predecessor_digest=plan["predecessor_digest"],
            backup_digest=plan["backup_digest"],
            lifecycle_activation_epoch=plan["lifecycle_activation_epoch"],
            database_writer_epoch=plan["database_writer_epoch"],
            ledger_uuid=plan["ledger_uuid"],
            controller_trust_root_id=plan["controller_trust_root_id"],
            port_manifest_digest=plan["port_manifest_digest"],
        )
        return context

    def _verify(
        self, context: EffectContext, observation: EffectObservation
    ) -> bool:
        if type(observation) is not EffectObservation:
            raise EffectError(
                f"retirement effect {context.effect_key!r} "
                "observation envelope is not exact"
            )
        try:
            observation.proof.verify(
                signer=self.observation_signer,
                subject_kind="effect",
                subject_digest=context.observation_subject_digest,
                evidence=observation.evidence,
            )
        except ProtocolError as exc:
            raise EffectError(
                f"retirement effect {context.effect_key!r} proof is invalid: {exc}"
            ) from exc
        return observation.satisfied

    def run_all(self, retirement_id: str) -> None:
        while True:
            rows = self.ledger.retirement_effects(retirement_id)
            if all(row["phase"] == EffectPhase.COMMITTED for row in rows):
                return
            committed = {
                row["effect_key"]
                for row in rows
                if row["phase"] == EffectPhase.COMMITTED
            }
            row = next(
                (
                    item
                    for item in rows
                    if item["phase"] != EffectPhase.COMMITTED
                    and set(item["predecessors"]) <= committed
                ),
                None,
            )
            if row is None:
                raise EffectError("no retirement effect is runnable")
            context = self._context(row)
            adapter = self.adapters.get(context.kind)
            if adapter is None:
                raise EffectError(
                    f"no retirement adapter registered for {context.kind!r}"
                )
            phase = EffectPhase(row["phase"])
            if phase == EffectPhase.PLANNED:
                self.ledger.intend_retirement_effect(retirement_id, context.effect_key)
                self.fault_hook(f"after_retirement_intent:{context.effect_key}")
                phase = EffectPhase.INTENDED
            if phase == EffectPhase.INTENDED:
                prior = adapter.observe(context)
                prior_satisfied = self._verify(context, prior)
                if prior_satisfied:
                    self.ledger.observe_retirement_effect(
                        retirement_id, context.effect_key, prior.evidence
                    )
                    self.ledger.commit_retirement_effect(
                        retirement_id, context.effect_key
                    )
                    continue
                applied = adapter.apply(context)
                self.ledger.mark_retirement_effect_applied(
                    retirement_id, context.effect_key, digest_json(applied)
                )
                self.fault_hook(f"after_retirement_apply:{context.effect_key}")
                phase = EffectPhase.APPLIED
            if phase == EffectPhase.APPLIED:
                observed = adapter.observe(context)
                observed_satisfied = self._verify(context, observed)
                if not observed_satisfied:
                    raise EffectError(
                        f"retirement effect {context.effect_key!r} is not observed"
                    )
                self.ledger.observe_retirement_effect(
                    retirement_id, context.effect_key, observed.evidence
                )
                phase = EffectPhase.OBSERVED
            if phase == EffectPhase.OBSERVED:
                self.ledger.commit_retirement_effect(retirement_id, context.effect_key)
                self.fault_hook(f"after_retirement_commit:{context.effect_key}")


class CompensationRunner:
    """Resume the exact reverse-order compensation journal before rollback publish."""

    def __init__(
        self,
        *,
        ledger: ControllerLedger,
        adapters: Mapping[str, EffectAdapter],
        observation_signer: AuthoritySigner,
        fault_hook: FaultHook | None = None,
    ) -> None:
        self.ledger = ledger
        self.adapters = dict(adapters)
        self.observation_signer = observation_signer
        self.fault_hook = fault_hook or (lambda _point: None)

    def _context(self, row: Mapping[str, Any]) -> CompensationContext:
        transition = self.ledger.transition(row["transition_id"])
        receipt = self.ledger.receipt(row["transition_id"], "rollback")
        plan = transition["plan"]
        context = CompensationContext(
            transition_id=row["transition_id"],
            compensation_key=row["compensation_key"],
            forward_effect_key=row["forward_effect_key"],
            kind=row["kind"],
            request=row["request"],
            request_digest=row["request_digest"],
            rollback_receipt_digest=receipt.digest,
            lifecycle_activation_epoch=receipt.lifecycle_activation_epoch,
            expected_database_writer_epoch=receipt.expected_database_writer_epoch,
            database_writer_epoch=receipt.database_writer_epoch,
            ledger_uuid=plan["ledger_uuid"],
            controller_trust_root_id=plan["controller_trust_root_id"],
            port_manifest_digest=plan["port_manifest_digest"],
        )
        return context

    def _verify(
        self, context: CompensationContext, observation: EffectObservation
    ) -> bool:
        if type(observation) is not EffectObservation:
            raise EffectError(
                f"compensation {context.compensation_key!r} "
                "observation envelope is not exact"
            )
        try:
            observation.proof.verify(
                signer=self.observation_signer,
                subject_kind="compensation",
                subject_digest=context.observation_subject_digest,
                evidence=observation.evidence,
            )
        except ProtocolError as exc:
            raise EffectError(
                f"compensation {context.compensation_key!r} proof is invalid: {exc}"
            ) from exc
        return observation.satisfied

    def run_all(self, transition_id: str) -> None:
        while True:
            rows = self.ledger.compensations(transition_id)
            if all(row["phase"] == EffectPhase.COMMITTED for row in rows):
                return
            committed = {
                row["compensation_key"]
                for row in rows
                if row["phase"] == EffectPhase.COMMITTED
            }
            row = next(
                (
                    item
                    for item in rows
                    if item["phase"] != EffectPhase.COMMITTED
                    and set(item["predecessors"]) <= committed
                ),
                None,
            )
            if row is None:
                raise EffectError("no compensation effect is runnable")
            context = self._context(row)
            adapter = self.adapters.get(context.kind)
            if adapter is None:
                raise EffectError(
                    f"no compensation adapter registered for {context.kind!r}"
                )
            phase = EffectPhase(row["phase"])
            try:
                if phase == EffectPhase.PLANNED:
                    self.ledger.intend_compensation(
                        transition_id, context.compensation_key
                    )
                    self.fault_hook(
                        f"after_compensation_intent:{context.compensation_key}"
                    )
                    phase = EffectPhase.INTENDED
                if phase == EffectPhase.INTENDED:
                    prior = adapter.observe_compensation(context)
                    if self._verify(context, prior):
                        self.ledger.observe_compensation(
                            transition_id,
                            context.compensation_key,
                            prior.evidence,
                        )
                        self.ledger.commit_compensation(
                            transition_id, context.compensation_key
                        )
                        self.fault_hook(
                            f"after_compensation_commit:{context.compensation_key}"
                        )
                        continue
                    applied = adapter.compensate(context)
                    self.ledger.mark_compensation_applied(
                        transition_id,
                        context.compensation_key,
                        digest_json(applied),
                    )
                    self.fault_hook(
                        f"after_compensation_apply:{context.compensation_key}"
                    )
                    phase = EffectPhase.APPLIED
                if phase == EffectPhase.APPLIED:
                    observed = adapter.observe_compensation(context)
                    if not self._verify(context, observed):
                        raise EffectError(
                            f"compensation {context.compensation_key!r} "
                            "is not independently observed"
                        )
                    self.ledger.observe_compensation(
                        transition_id,
                        context.compensation_key,
                        observed.evidence,
                    )
                    self.fault_hook(
                        f"after_compensation_observe:{context.compensation_key}"
                    )
                    phase = EffectPhase.OBSERVED
                if phase == EffectPhase.OBSERVED:
                    self.ledger.commit_compensation(
                        transition_id, context.compensation_key
                    )
                    self.fault_hook(
                        f"after_compensation_commit:{context.compensation_key}"
                    )
                    continue
            except InjectedControllerCrash:
                raise
            except Exception as exc:
                self.ledger.block_transition(
                    transition_id,
                    reason=(
                        f"compensation {context.compensation_key} failed: {exc}"
                    ),
                )
                raise
