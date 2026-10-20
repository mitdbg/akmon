from __future__ import annotations

import ast
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import multiprocessing
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import time
import tomllib
from threading import Event, Lock
from types import SimpleNamespace
from typing import Any

import pytest


ROOT = Path(__file__).resolve().parents[2]
CONTROLLER_ROOT = ROOT / "controller"
sys.path.insert(0, str(CONTROLLER_ROOT))

from okg_local_controller.controller import (  # noqa: E402
    ControllerError,
    LocalLifecycleController,
    TransitionResult,
)
from okg_local_controller.effects import (  # noqa: E402
    CompensationContext,
    EffectContext,
    EffectError,
    EffectObservation,
    InjectedControllerCrash,
)
from okg_local_controller.ledger import ControllerLedger, LedgerError  # noqa: E402
from okg_local_controller.probe import (  # noqa: E402
    BubblewrapSandboxBackend,
    MacSandboxBackend,
    ProbeError,
    SandboxLimits,
    TrustedProbeLauncher,
)
from okg_local_controller.protocol import (  # noqa: E402
    AuthoritySigner,
    CANONICAL_SAGA_EFFECTS,
    RETIREMENT_EFFECTS,
    ControllerInstallation,
    EffectPhase,
    EffectSpec,
    ObservationProof,
    ProbeAttestation,
    ProbeResult,
    ProtocolError,
    ReleaseDescriptor,
    SMOKE_OUTCOME_SCHEMA,
    TransitionState,
    UPGRADE_EFFECT_KIND,
    UPGRADE_EFFECT_SCHEMA,
    UpgradeAttestation,
    digest_json,
    rollback_generation_digest,
)
from okg_local_controller.reference_target import (  # noqa: E402
    ReferenceTargetError,
    SQLiteReferenceTarget,
)


probe_module = sys.modules[TrustedProbeLauncher.__module__]


DIGESTS = tuple(f"sha256:{character * 64}" for character in "abcdef0123456789")
KEY = b"controller-test-authority-key-32-bytes-minimum"


def test_probe_cleanup_tolerates_exit_between_poll_and_kill(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        probe_module.os,
        "killpg",
        lambda *_args: (_ for _ in ()).throw(PermissionError()),
    )
    assert probe_module._process_group_exists(12345)
    with pytest.raises(ProbeError, match="could not be terminated"):
        probe_module._kill_process_group(12345)


def test_probe_cleanup_treats_zombies_as_non_executable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ReapedLeader:
        def poll(self) -> int:
            return 0

        def wait(self, *, timeout: float) -> int:
            assert timeout == 1.0
            return 0

    monkeypatch.setattr(probe_module, "_kill_process_group", lambda _group: None)
    monkeypatch.setattr(probe_module, "_process_group_exists", lambda _group: True)
    monkeypatch.setattr(
        probe_module,
        "_process_group_has_executable_members",
        lambda _group: False,
    )

    probe_module._drain_process_group(12345, ReapedLeader())  # type: ignore[arg-type]


@pytest.mark.parametrize(("stdout", "expected"), [("Z+\n", False), ("S+\n", True)])
def test_probe_macos_process_state_distinguishes_live_from_zombie(
    monkeypatch: pytest.MonkeyPatch, stdout: str, expected: bool
) -> None:
    monkeypatch.setattr(probe_module.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(
        probe_module.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=0, stdout=stdout),
    )

    assert probe_module._process_group_has_executable_members(12345) is expected


@pytest.mark.parametrize("stdout", ["", "?\n"])
def test_probe_macos_process_state_refuses_unobservable_or_malformed_group(
    monkeypatch: pytest.MonkeyPatch, stdout: str
) -> None:
    monkeypatch.setattr(probe_module.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(probe_module, "_process_group_exists", lambda _group: True)
    monkeypatch.setattr(
        probe_module.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(returncode=0, stdout=stdout),
    )

    with pytest.raises(ProbeError, match="could not be observed|malformed"):
        probe_module._process_group_has_executable_members(12345)


@pytest.mark.parametrize(("state", "expected"), [("Z", False), ("S", True)])
def test_probe_linux_process_state_distinguishes_live_from_zombie(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    state: str,
    expected: bool,
) -> None:
    proc = tmp_path / "proc"
    process = proc / "123"
    process.mkdir(parents=True)
    (process / "stat").write_text(
        f"123 (candidate) {state} 1 12345 0 0 0\n", encoding="utf-8"
    )
    real_path = Path
    monkeypatch.setattr(probe_module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(
        probe_module,
        "Path",
        lambda value: proc if value == "/proc" else real_path(value),
    )

    assert probe_module._process_group_has_executable_members(12345) is expected


def test_probe_linux_process_state_refuses_malformed_or_unreadable_member(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    proc = tmp_path / "proc"
    process = proc / "123"
    process.mkdir(parents=True)
    stat_path = process / "stat"
    stat_path.write_text("malformed\n", encoding="utf-8")
    real_path = Path
    monkeypatch.setattr(probe_module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(
        probe_module,
        "Path",
        lambda value: proc if value == "/proc" else real_path(value),
    )

    with pytest.raises(ProbeError, match="malformed"):
        probe_module._process_group_has_executable_members(12345)

    original_read_text = Path.read_text

    def refuse_stat_read(path: Path, *args: Any, **kwargs: Any) -> str:
        if path == stat_path:
            raise PermissionError
        return original_read_text(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", refuse_stat_read)
    with pytest.raises(ProbeError, match="could not be observed"):
        probe_module._process_group_has_executable_members(12345)


def _record_process_execution_lock(
    ledger_path: str,
    installation_payload: dict[str, Any],
    trace_path: str,
    barrier_path: str,
) -> None:
    installation = ControllerInstallation.from_mapping(
        installation_payload, signer=_signer()
    )
    ledger = ControllerLedger(ledger_path, installation=installation)
    barrier = Path(barrier_path)
    deadline = time.monotonic() + 10
    while not barrier.exists():
        if time.monotonic() >= deadline:
            raise RuntimeError("process lock test barrier timed out")
        time.sleep(0.01)
    with ledger.execution_lock():
        with Path(trace_path).open("a", encoding="utf-8") as trace:
            trace.write(json.dumps({"pid": os.getpid(), "event": "enter"}) + "\n")
            trace.flush()
            os.fsync(trace.fileno())
        time.sleep(0.2)
        with Path(trace_path).open("a", encoding="utf-8") as trace:
            trace.write(json.dumps({"pid": os.getpid(), "event": "exit"}) + "\n")
            trace.flush()
            os.fsync(trace.fileno())


def _signer() -> AuthoritySigner:
    return AuthoritySigner(KEY)


def _trusted_probe_launcher(**kwargs: Any) -> TrustedProbeLauncher:
    try:
        return TrustedProbeLauncher(**kwargs)
    except ProbeError as exc:
        if "unavailable; candidate probe refused" not in str(exc):
            raise
        return pytest.skip(f"platform probe sandbox is unavailable: {exc}")


def _probe_attestation(
    signer: AuthoritySigner,
    candidate_digest: str,
    *,
    output: dict[str, Any],
    nonce: str | None = None,
) -> ProbeAttestation:
    now = int(time.time())
    nonce = nonce or f"probe-{time.time_ns()}"
    probe_signer = signer.signer_for("probe")
    input_payload = {
        "operation": "plan",
        "deployment": "local-test",
        "candidate_digest": candidate_digest,
    }
    unsigned = {
        "schema": "okg.local-controller.probe-attestation/v1",
        "attestation_id": digest_json(
            {
                "nonce": nonce,
                "launcher_key_id": probe_signer.key_id,
                "candidate_digest": candidate_digest,
                "output_payload_digest": digest_json(output),
            }
        ),
        "nonce": nonce,
        "launcher_key_id": probe_signer.key_id,
        "issued_at": now,
        "expires_at": now + 60,
        "input_digest": digest_json(input_payload),
        "input_payload_digest": digest_json(input_payload),
        "candidate_digest": candidate_digest,
        "sandbox_profile_digest": DIGESTS[5],
        "output_digest": digest_json(output),
        "output_payload_digest": digest_json(output),
        "launcher_digest": DIGESTS[7],
        "limits_digest": DIGESTS[6],
        "exit_status": 0,
        "elapsed_millis": 1,
        "cpu_limit_seconds": 1,
        "memory_limit_bytes": 64 * 1024 * 1024,
        "output_limit_bytes": 4096,
        "mutation_canary_before": DIGESTS[8],
        "mutation_canary_after": DIGESTS[8],
    }
    return ProbeAttestation(**unsigned, signature=probe_signer.sign(unsigned))


def _probe_result(
    signer: AuthoritySigner,
    descriptor: ReleaseDescriptor,
    *,
    staged: str = DIGESTS[3],
    nonce: str | None = None,
) -> ProbeResult:
    output = {
        "schema": "okg.local-controller.candidate-output/v1",
        "descriptor": descriptor.payload(),
        "staged_generation_digest": staged,
    }
    return ProbeResult(
        output=output,
        attestation=_probe_attestation(
            signer, descriptor.candidate_digest, output=output, nonce=nonce
        ),
    )


def _descriptor(
    *,
    candidate: str = DIGESTS[0],
    predecessor: str = DIGESTS[1],
    expected_predecessor: str = DIGESTS[2],
    expected_activation_receipt_digest: str = DIGESTS[4],
    effects: tuple[EffectSpec, ...] | None = None,
    expected_writer_epoch: int = 1,
    writer_epoch: int = 2,
    controller_min_version: int = 1,
) -> ReleaseDescriptor:
    identities = {
        "environment_identity": DIGESTS[11],
        "database_instance_identity": DIGESTS[12],
        "service_plan_digest": DIGESTS[13],
        "queue_scope_digest": DIGESTS[14],
        "promotion_approval_digest": DIGESTS[15],
        "evidence_digest": DIGESTS[4],
    }
    if effects is None:
        ordered: list[EffectSpec] = []
        predecessor_keys: tuple[str, ...] = ()
        for key, kind, irreversible in CANONICAL_SAGA_EFFECTS:
            if kind == UPGRADE_EFFECT_KIND:
                request = {
                    **identities,
                    **_upgrade_effect_request(),
                    "operation_digest": digest_json({"operation": key}),
                    "attestation_key_id": _signer().signer_for("upgrade").key_id,
                }
            else:
                request = {
                    "schema": "okg.local-controller.effect-request/v1",
                    **identities,
                    "operation_digest": digest_json({"operation": key}),
                }
            ordered.append(
                EffectSpec(
                    key,
                    kind,
                    request,
                    predecessors=predecessor_keys,
                    irreversible=irreversible,
                )
            )
            predecessor_keys = (key,)
        effects = tuple(ordered)
    replacement_writer_epoch = (
        expected_writer_epoch if candidate == predecessor else writer_epoch
    )
    return ReleaseDescriptor(
        deployment="local-test",
        candidate_digest=candidate,
        predecessor_digest=predecessor,
        expected_predecessor=expected_predecessor,
        expected_activation_receipt_digest=expected_activation_receipt_digest,
        **identities,
        expected_database_writer_epoch=expected_writer_epoch,
        database_writer_epoch=replacement_writer_epoch,
        dbos_application_version="controller-test-v1",
        controller_min_version=controller_min_version,
        effects=effects,
    )


class MemoryEffectAdapter:
    def __init__(self, signer: AuthoritySigner) -> None:
        self.signer = signer
        self.applied: set[str] = set()
        self.apply_counts: dict[str, int] = {}
        self.attestations: dict[str, UpgradeAttestation] = {}
        self.compensation_calls: list[str] = []

    def apply(self, effect: EffectContext) -> dict[str, Any]:
        self.apply_counts[effect.effect_key] = (
            self.apply_counts.get(effect.effect_key, 0) + 1
        )
        self.applied.add(effect.effect_key)
        if effect.kind == UPGRADE_EFFECT_KIND:
            self.attestations[effect.effect_key] = _upgrade_attestation(
                self.signer, effect
            )
        return {"effect_key": effect.effect_key, "applied": True}

    def observe(self, effect: EffectContext) -> EffectObservation:
        satisfied = effect.effect_key in self.applied
        evidence = (
            {
                "satisfied": satisfied,
                "upgrade_attestation": self.attestations[effect.effect_key].payload(),
            }
            if satisfied and effect.kind == UPGRADE_EFFECT_KIND
            else {"effect_key": effect.effect_key, "satisfied": satisfied}
        )
        proof = ObservationProof.issue(
            signer=self.signer.signer_for("observation"),
            subject_kind="effect",
            subject_digest=effect.observation_subject_digest,
            evidence=evidence,
            nonce=f"observe-{effect.transition_id}-{effect.effect_key}-{satisfied}",
        )
        return EffectObservation(
            evidence=evidence,
            proof=proof,
        )

    def compensate(self, effect: CompensationContext) -> dict[str, Any]:
        self.compensation_calls.append(effect.forward_effect_key)
        self.applied.discard(effect.forward_effect_key)
        self.attestations.pop(effect.forward_effect_key, None)
        return {"compensation_key": effect.compensation_key, "applied": True}

    def observe_compensation(
        self, effect: CompensationContext
    ) -> EffectObservation:
        satisfied = effect.forward_effect_key not in self.applied
        evidence = {
            "compensation_key": effect.compensation_key,
            "forward_effect_key": effect.forward_effect_key,
            "satisfied": satisfied,
        }
        proof = ObservationProof.issue(
            signer=self.signer.signer_for("observation"),
            subject_kind="compensation",
            subject_digest=effect.observation_subject_digest,
            evidence=evidence,
            nonce=f"compensate-{effect.transition_id}-{effect.compensation_key}",
        )
        return EffectObservation(evidence=evidence, proof=proof)


def _upgrade_effect_request() -> dict[str, Any]:
    return {
        "schema": UPGRADE_EFFECT_SCHEMA,
        "issuer": "local-controller-upgrade-adapter",
        "expected_schema_before_digest": DIGESTS[13],
        "migration_manifest_digest": DIGESTS[14],
        "rollback_compatibility_class": "physical_restore_required",
        "restore_strategy": "verified_backup",
        "max_attestation_ttl_seconds": 120,
    }


def _attested_descriptor() -> ReleaseDescriptor:
    return _descriptor()


def _upgrade_attestation(
    signer: AuthoritySigner,
    effect: EffectContext,
    *,
    result: str = "success",
    overrides: dict[str, Any] | None = None,
) -> UpgradeAttestation:
    request = effect.request
    now = int(time.time())
    no_op = result == "no_op"
    fields: dict[str, Any] = {
        "nonce": "nonce-" + effect.transition_id,
        "issuer": request["issuer"],
        "issued_at": now,
        "expires_at": now + request["max_attestation_ttl_seconds"],
        "environment_identity": request["environment_identity"],
        "database_instance_identity": request["database_instance_identity"],
        "artifact_digest": effect.candidate_digest,
        "reconcile_plan_digest": effect.descriptor_digest,
        "saga_transition_id": effect.transition_id,
        "database_writer_epoch": effect.database_writer_epoch,
        "backup_digest": effect.backup_digest,
        "database_state_digest": DIGESTS[15],
        "previous_artifact_digest": effect.predecessor_digest,
        "candidate_artifact_digest": effect.candidate_digest,
        "schema_before_digest": request["expected_schema_before_digest"],
        "schema_after_digest": request["expected_schema_before_digest"]
        if no_op
        else DIGESTS[10],
        "migration_manifest_digest": request["migration_manifest_digest"],
        "applied_migrations": () if no_op else ("0002_upgrade.sql",),
        "skipped_migrations": ("0001_baseline.sql",),
        "rollback_compatibility_class": request["rollback_compatibility_class"],
        "restore_strategy": request["restore_strategy"],
        "smoke_outcome": {
            "schema": SMOKE_OUTCOME_SCHEMA,
            "status": "valid_no_work" if no_op else "published",
            "generation_digest": None if no_op else DIGESTS[3],
            "no_work_reason": "already current" if no_op else None,
        },
        "result": result,
        "no_op_reason": "already current" if no_op else None,
    }
    fields.update(overrides or {})
    return UpgradeAttestation.issue(signer=signer.signer_for("upgrade"), **fields)


class AttestingMemoryEffect:
    def __init__(
        self,
        *,
        result: str = "success",
        overrides: dict[str, Any] | None = None,
        fixed_attestation: UpgradeAttestation | None = None,
        signer: AuthoritySigner | None = None,
    ) -> None:
        self.result = result
        self.overrides = dict(overrides or {})
        self.fixed_attestation = fixed_attestation
        self.signer = signer or _signer()
        self.attestation: UpgradeAttestation | None = None

    def apply(self, effect: EffectContext) -> dict[str, Any]:
        self.attestation = self.fixed_attestation or _upgrade_attestation(
            self.signer,
            effect,
            result=self.result,
            overrides=self.overrides,
        )
        return {"attestation_id": self.attestation.attestation_id}

    def observe(self, effect: EffectContext) -> EffectObservation:
        evidence = {
            "satisfied": self.attestation is not None,
            "upgrade_attestation": (
                self.attestation.payload() if self.attestation is not None else None
            )
        }
        proof = ObservationProof.issue(
            signer=self.signer.signer_for("observation"),
            subject_kind="effect",
            subject_digest=effect.observation_subject_digest,
            evidence=evidence,
            nonce=f"upgrade-observe-{effect.transition_id}",
        )
        return EffectObservation(
            evidence=evidence,
            proof=proof,
        )

    def compensate(self, effect: CompensationContext) -> dict[str, Any]:
        self.attestation = None
        return {"compensation_key": effect.compensation_key, "applied": True}

    def observe_compensation(
        self, effect: CompensationContext
    ) -> EffectObservation:
        evidence = {
            "compensation_key": effect.compensation_key,
            "forward_effect_key": effect.forward_effect_key,
            "satisfied": self.attestation is None,
        }
        proof = ObservationProof.issue(
            signer=self.signer.signer_for("observation"),
            subject_kind="compensation",
            subject_digest=effect.observation_subject_digest,
            evidence=evidence,
            nonce=f"upgrade-compensate-{effect.transition_id}",
        )
        return EffectObservation(evidence=evidence, proof=proof)


def _runtime(tmp_path: Path, *, fault_hook=None):
    signer = _signer()
    ledger = _ledger(tmp_path, signer=signer)
    controller = LocalLifecycleController(
        ledger=ledger, signer=signer, fault_hook=fault_hook
    )
    target = SQLiteReferenceTarget(
        path=tmp_path / "target.sqlite3",
        signer=signer,
        deployment="local-test",
        predecessor=DIGESTS[2],
        database_writer_epoch=1,
        activation_receipt_digest=DIGESTS[4],
        service_plan_digest=DIGESTS[13],
    )
    adapter = MemoryEffectAdapter(signer)
    return signer, ledger, controller, target, adapter


def _ledger(
    tmp_path: Path,
    *,
    signer: AuthoritySigner,
    name: str = "controller-ledger.sqlite3",
) -> ControllerLedger:
    ledger_path = tmp_path / name
    installation = ControllerInstallation.issue(
        signer=signer,
        deployment="local-test",
        ledger_uuid=f"ledger-{time.time_ns()}",
        ledger_path=str(ledger_path.resolve()),
        probe_profile_digest=DIGESTS[5],
        probe_launcher_digest=DIGESTS[7],
        probe_limits_digest=DIGESTS[6],
        probe_max_ttl_seconds=60,
    )
    return ControllerLedger(ledger_path, installation=installation)


def _adapters(adapter: Any) -> dict[str, Any]:
    return {kind: adapter for _key, kind, _irreversible in CANONICAL_SAGA_EFFECTS}


PORT_MANIFEST_DIGEST = DIGESTS[9]


def _activate(
    tmp_path: Path,
    *,
    fault_hook=None,
    descriptor: ReleaseDescriptor | None = None,
):
    signer, ledger, controller, target, adapter = _runtime(
        tmp_path, fault_hook=fault_hook
    )
    descriptor = descriptor or _descriptor()
    result = controller.activate(
        probe_result=_probe_result(signer, descriptor),
        port_manifest_digest=PORT_MANIFEST_DIGEST,
        capacity_reservation_digest=DIGESTS[9],
        backup_digest=DIGESTS[10],
        current_lifecycle_epoch=0,
        staged_capability_expires_at=int(time.time()) + 300,
        adapters=_adapters(adapter),
        target=target,
    )
    return signer, ledger, controller, target, adapter, result


def test_boundary_controller_distribution_has_no_product_dependency() -> None:
    with (CONTROLLER_ROOT / "pyproject.toml").open("rb") as handle:
        pyproject = tomllib.load(handle)
    assert pyproject["project"]["dependencies"] == []
    forbidden: list[str] = []
    for path in (CONTROLLER_ROOT / "okg_local_controller").glob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            names: list[str] = []
            if isinstance(node, ast.Import):
                names.extend(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                names.append(node.module)
            forbidden.extend(
                f"{path.name}:{name}"
                for name in names
                if name == "okg" or name.startswith("okg.")
            )
    assert forbidden == []

    trap = """
import importlib.abc, sys
class Trap(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname == 'okg' or fullname.startswith('okg.'):
            raise AssertionError(fullname)
        return None
sys.meta_path.insert(0, Trap())
import okg_local_controller
assert okg_local_controller.ControllerLedger
"""
    completed = subprocess.run(
        [sys.executable, "-c", trap],
        env={"PYTHONPATH": str(CONTROLLER_ROOT)},
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr


def test_boundary_incompatible_envelope_refuses_before_ledger_mutation(
    tmp_path: Path,
) -> None:
    with pytest.raises(ProtocolError, match="compatible controller package"):
        ReleaseDescriptor.from_mapping(
            {**_descriptor().payload(), "schema": "candidate-owned/v99"}
        )
    assert list(tmp_path.iterdir()) == []


def test_boundary_controller_compatibility_cannot_be_overridden_by_caller(
    tmp_path: Path,
) -> None:
    signer, ledger, controller, target, adapter = _runtime(tmp_path)
    invalid_kwargs: dict[str, Any] = {
        "ledger": ledger,
        "signer": signer,
        "controller_version": 999,
    }
    with pytest.raises(TypeError, match="controller_version"):
        LocalLifecycleController(**invalid_kwargs)
    with pytest.raises(ControllerError, match="newer compatible controller"):
        controller.activate(
            probe_result=_probe_result(
                signer, _descriptor(controller_min_version=2)
            ),
            port_manifest_digest=PORT_MANIFEST_DIGEST,
            capacity_reservation_digest=DIGESTS[9],
            backup_digest=DIGESTS[10],
            current_lifecycle_epoch=0,
            staged_capability_expires_at=int(time.time()) + 300,
            adapters=_adapters(adapter),
            target=target,
        )
    with sqlite3.connect(ledger.path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM transitions").fetchone()[0] == 0


def test_boundary_probe_tamper_refuses_before_transition_intent(tmp_path: Path) -> None:
    signer, ledger, controller, _target, _adapter = _runtime(tmp_path)
    descriptor = _descriptor()
    result = _probe_result(signer, descriptor)
    attestation = replace(result.attestation, output_digest=DIGESTS[11])
    with pytest.raises(ControllerError, match="not trusted"):
        controller.plan(
            probe_result=ProbeResult(output=result.output, attestation=attestation),
            target=_target,
            port_manifest_digest=PORT_MANIFEST_DIGEST,
            capacity_reservation_digest=DIGESTS[9],
            backup_digest=DIGESTS[10],
            current_lifecycle_epoch=0,
            staged_capability_expires_at=int(time.time()) + 300,
        )
    with sqlite3.connect(ledger.path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM transitions").fetchone()[0] == 0


def test_boundary_plan_requires_fresh_predecessor_before_attempt_fence(
    tmp_path: Path,
) -> None:
    signer, ledger, controller, target, _adapter = _runtime(tmp_path)
    descriptor = _descriptor()

    class StalePredecessor:
        def observe_current_state(self, observed_descriptor):
            current = target.observe_current_state(observed_descriptor)
            evidence = {
                key: value for key, value in current.items() if key != "proof"
            }
            evidence.update({"observed_at": 1, "expires_at": 2})
            subject = digest_json(
                {
                    "schema": "okg.local-controller.no-op-subject/v1",
                    "descriptor": observed_descriptor.payload(),
                }
            )
            proof = ObservationProof.issue(
                signer=signer.signer_for("observation"),
                subject_kind="no_op",
                subject_digest=subject,
                evidence=evidence,
                nonce="stale-plan-predecessor",
            )
            return {**evidence, "proof": vars(proof)}

    with pytest.raises(ControllerError, match="stale"):
        controller.plan(
            probe_result=_probe_result(signer, descriptor),
            target=StalePredecessor(),
            port_manifest_digest=PORT_MANIFEST_DIGEST,
            capacity_reservation_digest=DIGESTS[9],
            backup_digest=DIGESTS[10],
            current_lifecycle_epoch=0,
            staged_capability_expires_at=int(time.time()) + 300,
        )
    with sqlite3.connect(ledger.path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM transitions").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM deployment_authority").fetchone()[0] == 0


def test_boundary_broken_candidate_cannot_remove_controller_status(
    tmp_path: Path,
) -> None:
    _signer_value, _ledger, controller, _target, _adapter, result = _activate(tmp_path)
    broken_candidate = tmp_path / "broken-candidate.py"
    broken_candidate.write_text("raise SyntaxError('broken')\n", encoding="utf-8")
    status = controller.status(result.transition_id)
    assert status["state"] == "ready"
    assert status["operator_state"] == "live_state_unavailable"
    assert status["broad_writes_allowed"] is False


def test_activate_retry_after_plan_intent_renews_expired_bound_capability(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    crashed = False

    def fault(point: str) -> None:
        nonlocal crashed
        if not crashed and point == "after_plan_intent":
            crashed = True
            raise InjectedControllerCrash(point)

    signer, ledger, controller, target, adapter = _runtime(
        tmp_path, fault_hook=fault
    )
    probe_result = _probe_result(signer, _descriptor())
    original_capability_expiry = probe_result.attestation.expires_at + 5
    with pytest.raises(InjectedControllerCrash, match="after_plan_intent"):
        controller.activate(
            probe_result=probe_result,
            port_manifest_digest=PORT_MANIFEST_DIGEST,
            capacity_reservation_digest=DIGESTS[9],
            backup_digest=DIGESTS[10],
            current_lifecycle_epoch=0,
            staged_capability_expires_at=original_capability_expiry,
            adapters=_adapters(adapter),
            target=target,
        )

    resumed_at = original_capability_expiry + 1
    monkeypatch.setattr(time, "time", lambda: resumed_at)
    ready = LocalLifecycleController(ledger=ledger, signer=signer).activate(
        probe_result=probe_result,
        port_manifest_digest=PORT_MANIFEST_DIGEST,
        capacity_reservation_digest=DIGESTS[9],
        backup_digest=DIGESTS[10],
        current_lifecycle_epoch=0,
        staged_capability_expires_at=resumed_at + 999,
        adapters=_adapters(adapter),
        target=target,
    )

    assert ready.state == "ready"
    assert ready.transition_id is not None
    receipt = ledger.receipt(ready.transition_id)
    assert receipt.expected_database_writer_epoch == 1
    assert receipt.database_writer_epoch == 2
    assert target.authority()["database_writer_epoch"] == 2
    assert target.authority()["visibility_switches"] == 1
    assert "staged-capability.renewed" in {
        event["event_type"] for event in ledger.events(ready.transition_id)
    }


def test_activate_retry_refreshes_outcome_after_target_committed_before_ledger(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    signer, ledger, controller, target, adapter = _runtime(tmp_path)
    probe_result = _probe_result(signer, _descriptor())
    capability_expiry = probe_result.attestation.expires_at + 240

    class CrashAfterTargetCommit:
        def __init__(self) -> None:
            self.crashed = False

        def __getattr__(self, name: str) -> Any:
            return getattr(target, name)

        def publish_authorized_generation(self, receipt):
            outcome = target.publish_authorized_generation(receipt)
            if not self.crashed:
                self.crashed = True
                raise InjectedControllerCrash("after-target-commit")
            return outcome

    crashing_target = CrashAfterTargetCommit()
    with pytest.raises(InjectedControllerCrash, match="after-target-commit"):
        controller.activate(
            probe_result=probe_result,
            port_manifest_digest=PORT_MANIFEST_DIGEST,
            capacity_reservation_digest=DIGESTS[9],
            backup_digest=DIGESTS[10],
            current_lifecycle_epoch=0,
            staged_capability_expires_at=capability_expiry,
            adapters=_adapters(adapter),
            target=crashing_target,
        )

    assert target.authority()["visibility_switches"] == 1
    resume_now = probe_result.attestation.issued_at + 121
    monkeypatch.setattr(time, "time", lambda: resume_now)
    ready = LocalLifecycleController(ledger=ledger, signer=signer).activate(
        probe_result=probe_result,
        port_manifest_digest=PORT_MANIFEST_DIGEST,
        capacity_reservation_digest=DIGESTS[9],
        backup_digest=DIGESTS[10],
        current_lifecycle_epoch=0,
        staged_capability_expires_at=resume_now + 999,
        adapters=_adapters(adapter),
        target=crashing_target,
    )

    assert ready.state == "ready"
    assert ready.transition_id is not None
    assert ledger.transition(ready.transition_id)["state"] == TransitionState.READY
    assert target.authority()["visibility_switches"] == 1


@pytest.mark.parametrize(
    "crash_point",
    [
        "after_intent:approval",
        "after_apply:approval",
        "after_observe:approval",
        "after_commit:approval",
        "after_intent:upgrade",
        "after_apply:upgrade",
        "after_observe:upgrade",
        "after_commit:upgrade",
        "after_authorize_activation",
        "after_publish_authorized_generation",
        "after_observe_publication",
    ],
)
def test_boundary_crash_matrix_resumes_exact_transition(
    tmp_path: Path, crash_point: str
) -> None:
    crashed = False

    def fault(point: str) -> None:
        nonlocal crashed
        if not crashed and point == crash_point:
            crashed = True
            raise InjectedControllerCrash(point)

    signer, ledger, controller, target, adapter = _runtime(tmp_path, fault_hook=fault)
    descriptor = _descriptor()
    with pytest.raises(InjectedControllerCrash, match=crash_point):
        controller.activate(
            probe_result=_probe_result(signer, descriptor),
            port_manifest_digest=PORT_MANIFEST_DIGEST,
            capacity_reservation_digest=DIGESTS[9],
            backup_digest=DIGESTS[10],
            current_lifecycle_epoch=0,
            staged_capability_expires_at=int(time.time()) + 300,
            adapters=_adapters(adapter),
            target=target,
        )
    with ledger._connect() as conn:
        actual_transition_id = conn.execute(
            "SELECT transition_id FROM transitions"
        ).fetchone()[0]
    result = controller.resume(
        transition_id=actual_transition_id,
        adapters=_adapters(adapter),
        target=target,
        port_manifest_digest=PORT_MANIFEST_DIGEST,
    )
    assert result.state == "ready"
    assert target.authority()["visibility_switches"] == 1
    assert all(count == 1 for count in adapter.apply_counts.values())
    assert ledger.all_effects_committed(actual_transition_id)


def test_boundary_authorization_and_publication_gaps_are_write_blocked(
    tmp_path: Path,
) -> None:
    for point, expected in (
        ("after_authorize_activation", "read_only_stale"),
        ("after_publish_authorized_generation", "published_not_ready"),
    ):
        case = tmp_path / point
        fired = False

        def fault(observed: str) -> None:
            nonlocal fired
            if not fired and observed == point:
                fired = True
                raise InjectedControllerCrash(point)

        signer, ledger, controller, target, adapter = _runtime(case, fault_hook=fault)
        descriptor = _descriptor()
        with pytest.raises(InjectedControllerCrash):
            controller.activate(
                probe_result=_probe_result(signer, descriptor),
                port_manifest_digest=PORT_MANIFEST_DIGEST,
                capacity_reservation_digest=DIGESTS[9],
                backup_digest=DIGESTS[10],
                current_lifecycle_epoch=0,
                staged_capability_expires_at=int(time.time()) + 300,
                adapters=_adapters(adapter),
                target=target,
            )
        with ledger._connect() as conn:
            transition_id = conn.execute(
                "SELECT transition_id FROM transitions"
            ).fetchone()[0]
        status = controller.status(
            transition_id,
            target=target,
            port_manifest_digest=PORT_MANIFEST_DIGEST,
        )
        assert status["operator_state"] == expected
        assert status["broad_writes_allowed"] is False
        with pytest.raises(ControllerError, match="status adapter set differs"):
            controller.status(
                transition_id,
                target=target,
                port_manifest_digest=DIGESTS[11],
            )
        phase_by_key = {
            row["effect_key"]: row["phase"]
            for row in ledger.effects(transition_id)
        }
        assert phase_by_key["unfreeze"] == EffectPhase.PLANNED
        assert target.authority()["database_writer_epoch"] == (
            1 if point == "after_authorize_activation" else 2
        )


def test_boundary_noop_upgrade_has_no_ledger_or_effect_delta(tmp_path: Path) -> None:
    descriptor = _descriptor(candidate=DIGESTS[1], predecessor=DIGESTS[1])
    _signer_value, ledger, _controller, target, adapter, result = _activate(
        tmp_path, descriptor=descriptor
    )
    assert result.no_op is True
    assert adapter.apply_counts == {}
    assert target.authority()["visibility_switches"] == 0
    with sqlite3.connect(ledger.path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM transitions").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM effects").fetchone()[0] == 0


def test_boundary_predecessor_receipt_mismatch_refuses_before_mutation(
    tmp_path: Path,
) -> None:
    signer, ledger, controller, _target, adapter = _runtime(tmp_path)
    mismatched = SQLiteReferenceTarget(
        path=tmp_path / "mismatched-target.sqlite3",
        signer=signer,
        deployment="local-test",
        predecessor=DIGESTS[2],
        database_writer_epoch=1,
        activation_receipt_digest=DIGESTS[5],
        service_plan_digest=DIGESTS[13],
    )
    with pytest.raises(ControllerError, match="predecessor authority"):
        controller.activate(
            probe_result=_probe_result(signer, _descriptor()),
            port_manifest_digest=PORT_MANIFEST_DIGEST,
            capacity_reservation_digest=DIGESTS[9],
            backup_digest=DIGESTS[10],
            current_lifecycle_epoch=0,
            staged_capability_expires_at=int(time.time()) + 300,
            adapters=_adapters(adapter),
            target=mismatched,
        )
    assert adapter.apply_counts == {}
    assert mismatched.authority()["visibility_switches"] == 0
    with sqlite3.connect(ledger.path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM transitions").fetchone()[0] == 0


def test_boundary_publish_cas_uses_plan_bound_predecessor_receipt(
    tmp_path: Path,
) -> None:
    signer, ledger, controller, target, adapter = _runtime(tmp_path)

    class ReceiptRaceTarget:
        def observe_current_state(self, descriptor):
            return target.observe_current_state(descriptor)

        def stage_generation(self, **kwargs):
            target.stage_generation(**kwargs)
            with sqlite3.connect(target.path) as conn:
                conn.execute(
                    "UPDATE target_authority SET activation_receipt_digest=?",
                    (DIGESTS[5],),
                )

        def publish_authorized_generation(self, receipt):
            return target.publish_authorized_generation(receipt)

    with pytest.raises(
        ReferenceTargetError, match="expected predecessor activation receipt"
    ):
        controller.activate(
            probe_result=_probe_result(signer, _descriptor()),
            port_manifest_digest=PORT_MANIFEST_DIGEST,
            capacity_reservation_digest=DIGESTS[9],
            backup_digest=DIGESTS[10],
            current_lifecycle_epoch=0,
            staged_capability_expires_at=int(time.time()) + 300,
            adapters=_adapters(adapter),
            target=ReceiptRaceTarget(),
        )
    assert target.authority()["visibility_switches"] == 0
    with sqlite3.connect(ledger.path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM activation_receipts WHERE published=1"
        ).fetchone()[0] == 0


def test_boundary_unsigned_satisfaction_cannot_skip_or_retire_effects(
    tmp_path: Path,
) -> None:
    signer, ledger, controller, target, base_adapter = _runtime(tmp_path / "saga")

    class ContradictoryObservation:
        def __init__(self, effect: EffectContext) -> None:
            self.satisfied = True
            self.evidence = {
                "effect_key": effect.effect_key,
                "satisfied": False,
            }
            self.proof = ObservationProof.issue(
                signer=signer.signer_for("observation"),
                subject_kind="effect",
                subject_digest=effect.observation_subject_digest,
                evidence=self.evidence,
                nonce="contradictory-" + effect.effect_key,
            )

    class ContradictoryAdapter(MemoryEffectAdapter):
        def observe(self, effect: EffectContext):
            return ContradictoryObservation(effect)

    contradiction = ContradictoryAdapter(signer)
    adapters = _adapters(base_adapter)
    adapters[CANONICAL_SAGA_EFFECTS[0][1]] = contradiction
    with pytest.raises(EffectError, match="envelope is not exact"):
        controller.activate(
            probe_result=_probe_result(signer, _descriptor()),
            port_manifest_digest=PORT_MANIFEST_DIGEST,
            capacity_reservation_digest=DIGESTS[9],
            backup_digest=DIGESTS[10],
            current_lifecycle_epoch=0,
            staged_capability_expires_at=int(time.time()) + 300,
            adapters=adapters,
            target=target,
        )
    assert contradiction.apply_counts == {}
    with sqlite3.connect(ledger.path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM effects WHERE phase='committed'"
        ).fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM activation_receipts").fetchone()[0] == 0

    signer, ledger, controller, target, _adapter, ready = _activate(
        tmp_path / "retirement"
    )
    retirement_adapter = ContradictoryAdapter(signer)
    with pytest.raises(EffectError, match="envelope is not exact"):
        controller.retire(
            transition_id=ready.transition_id,
            adapters={kind: retirement_adapter for _key, kind in RETIREMENT_EFFECTS},
            target=target,
            port_manifest_digest=PORT_MANIFEST_DIGEST,
        )
    assert controller.status(ready.transition_id)["state"] == "retiring"
    assert bool(target.authority()["retired"]) is False
    with sqlite3.connect(ledger.path) as conn:
        assert conn.execute(
            "SELECT COUNT(*) FROM retirement_effects WHERE phase='committed'"
        ).fetchone()[0] == 0


@pytest.mark.parametrize("result", ["success", "no_op"])
def test_boundary_upgrade_attestation_is_exact_and_single_use(
    tmp_path: Path, result: str
) -> None:
    signer = _signer()
    ledger = _ledger(tmp_path, signer=signer)
    target = SQLiteReferenceTarget(
        path=tmp_path / "target.sqlite3",
        signer=signer,
        deployment="local-test",
        predecessor=DIGESTS[2],
        database_writer_epoch=1,
        activation_receipt_digest=DIGESTS[4],
        service_plan_digest=DIGESTS[13],
    )
    descriptor = _attested_descriptor()
    adapter = AttestingMemoryEffect(result=result, signer=signer)
    adapters = _adapters(MemoryEffectAdapter(signer))
    adapters[UPGRADE_EFFECT_KIND] = adapter
    ready = LocalLifecycleController(ledger=ledger, signer=signer).activate(
        probe_result=_probe_result(signer, descriptor),
        port_manifest_digest=PORT_MANIFEST_DIGEST,
        capacity_reservation_digest=DIGESTS[9],
        backup_digest=DIGESTS[10],
        current_lifecycle_epoch=0,
        staged_capability_expires_at=int(time.time()) + 300,
        adapters=adapters,
        target=target,
    )
    assert ready.state == "ready"
    assert adapter.attestation is not None
    consumed = ledger.upgrade_attestation_consumption(ready.transition_id, "upgrade")
    assert consumed is not None
    assert consumed["attestation_id"] == adapter.attestation.attestation_id
    assert consumed["content_digest"] == adapter.attestation.content_digest
    assert [event["event_type"] for event in ledger.events(ready.transition_id)].count(
        "upgrade_attestation.consumed"
    ) == 1
    resumed = LocalLifecycleController(ledger=ledger, signer=signer).resume(
        transition_id=ready.transition_id,
        adapters=adapters,
        target=target,
        port_manifest_digest=PORT_MANIFEST_DIGEST,
    )
    assert resumed.activation_receipt_digest == ready.activation_receipt_digest
    assert [event["event_type"] for event in ledger.events(ready.transition_id)].count(
        "upgrade_attestation.consumed"
    ) == 1


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        ({"issued_at": 1, "expires_at": 2}, "not current"),
        (
            {
                "issued_at": int(time.time()) + 300,
                "expires_at": int(time.time()) + 360,
            },
            "not current",
        ),
        ({"database_instance_identity": DIGESTS[9]}, "exact transition"),
        ({"reconcile_plan_digest": DIGESTS[9]}, "exact transition"),
        ({"database_writer_epoch": 99}, "exact transition"),
        ({"backup_digest": DIGESTS[9]}, "exact transition"),
        ({"migration_manifest_digest": DIGESTS[9]}, "exact transition"),
    ],
)
def test_boundary_upgrade_attestation_rejects_stale_foreign_or_changed_proof(
    tmp_path: Path, overrides: dict[str, Any], error: str
) -> None:
    signer = _signer()
    ledger = _ledger(tmp_path, signer=signer)
    target = SQLiteReferenceTarget(
        path=tmp_path / "target.sqlite3",
        signer=signer,
        deployment="local-test",
        predecessor=DIGESTS[2],
        database_writer_epoch=1,
        activation_receipt_digest=DIGESTS[4],
        service_plan_digest=DIGESTS[13],
    )
    descriptor = _attested_descriptor()
    adapter = AttestingMemoryEffect(overrides=overrides, signer=signer)
    adapters = _adapters(MemoryEffectAdapter(signer))
    adapters[UPGRADE_EFFECT_KIND] = adapter
    with pytest.raises((ControllerError, LedgerError, ProtocolError), match=error):
        LocalLifecycleController(ledger=ledger, signer=signer).activate(
            probe_result=_probe_result(signer, descriptor),
            port_manifest_digest=PORT_MANIFEST_DIGEST,
            capacity_reservation_digest=DIGESTS[9],
            backup_digest=DIGESTS[10],
            current_lifecycle_epoch=0,
            staged_capability_expires_at=int(time.time()) + 300,
            adapters=adapters,
            target=target,
        )
    with sqlite3.connect(ledger.path) as conn:
        assert (
            conn.execute("SELECT COUNT(*) FROM activation_receipts").fetchone()[0] == 0
        )
    assert target.authority()["visibility_switches"] == 0


def test_boundary_upgrade_attestation_replay_across_transition_is_refused(
    tmp_path: Path,
) -> None:
    signer = _signer()
    first_ledger = _ledger(tmp_path, signer=signer, name="first.sqlite3")
    first_target = SQLiteReferenceTarget(
        path=tmp_path / "first-target.sqlite3",
        signer=signer,
        deployment="local-test",
        predecessor=DIGESTS[2],
        database_writer_epoch=1,
        activation_receipt_digest=DIGESTS[4],
        service_plan_digest=DIGESTS[13],
    )
    descriptor = _attested_descriptor()
    first_adapter = AttestingMemoryEffect(signer=signer)
    first_adapters = _adapters(MemoryEffectAdapter(signer))
    first_adapters[UPGRADE_EFFECT_KIND] = first_adapter
    LocalLifecycleController(ledger=first_ledger, signer=signer).activate(
        probe_result=_probe_result(signer, descriptor),
        port_manifest_digest=PORT_MANIFEST_DIGEST,
        capacity_reservation_digest=DIGESTS[9],
        backup_digest=DIGESTS[10],
        current_lifecycle_epoch=0,
        staged_capability_expires_at=int(time.time()) + 300,
        adapters=first_adapters,
        target=first_target,
    )
    assert first_adapter.attestation is not None

    second_ledger = _ledger(tmp_path, signer=signer, name="second.sqlite3")
    second_target = SQLiteReferenceTarget(
        path=tmp_path / "second-target.sqlite3",
        signer=signer,
        deployment="local-test",
        predecessor=DIGESTS[2],
        database_writer_epoch=1,
        activation_receipt_digest=DIGESTS[4],
        service_plan_digest=DIGESTS[13],
    )
    replay = AttestingMemoryEffect(
        fixed_attestation=first_adapter.attestation, signer=signer
    )
    replay_adapters = _adapters(MemoryEffectAdapter(signer))
    replay_adapters[UPGRADE_EFFECT_KIND] = replay
    with pytest.raises(ControllerError, match="exact transition"):
        LocalLifecycleController(ledger=second_ledger, signer=signer).activate(
            probe_result=_probe_result(signer, descriptor, staged=DIGESTS[4]),
            port_manifest_digest=PORT_MANIFEST_DIGEST,
            capacity_reservation_digest=DIGESTS[9],
            backup_digest=DIGESTS[10],
            current_lifecycle_epoch=0,
            staged_capability_expires_at=int(time.time()) + 300,
            adapters=replay_adapters,
            target=second_target,
        )


def test_boundary_strict_newer_rollback_rotates_receipt_and_capability(
    tmp_path: Path,
) -> None:
    signer, ledger, controller, target, adapter, ready = _activate(tmp_path)
    assert ready.capability is not None
    with pytest.raises(LedgerError, match="lifecycle epoch"):
        controller.rollback(
            transition_id=ready.transition_id,
            lifecycle_activation_epoch=1,
            database_writer_epoch=3,
            adapters=_adapters(adapter),
            target=target,
            port_manifest_digest=PORT_MANIFEST_DIGEST,
        )
    rolled_back = controller.rollback(
        transition_id=ready.transition_id,
        lifecycle_activation_epoch=2,
        database_writer_epoch=3,
        adapters=_adapters(adapter),
        target=target,
        port_manifest_digest=PORT_MANIFEST_DIGEST,
    )
    assert rolled_back.state == "rolled_back"
    assert rolled_back.activation_receipt_digest != ready.activation_receipt_digest
    assert rolled_back.capability is not None
    assert rolled_back.capability.lifecycle_activation_epoch == 2
    assert rolled_back.capability.database_writer_epoch == 3
    with pytest.raises(ControllerError, match="not current"):
        controller.verify_capability(
            ready.capability,
            target=target,
            port_manifest_digest=PORT_MANIFEST_DIGEST,
        )
    controller.verify_capability(
        rolled_back.capability,
        target=target,
        port_manifest_digest=PORT_MANIFEST_DIGEST,
    )
    assert target.authority()["current_generation"] == rollback_generation_digest(
        predecessor_digest=DIGESTS[1], expected_predecessor=DIGESTS[2]
    )
    assert target.authority()["database_writer_epoch"] == 3
    assert target.authority()["visibility_switches"] == 2
    compensations = ledger.compensations(ready.transition_id)
    assert [row["forward_effect_key"] for row in compensations] == [
        key for key, _kind, _flag in reversed(CANONICAL_SAGA_EFFECTS)
    ]
    assert all(row["phase"] == "committed" for row in compensations)
    assert adapter.applied == set()
    events = [item["event_type"] for item in ledger.events(ready.transition_id)]
    assert (
        events.index("rollback.authorized")
        < events.index("activation.rollback.published")
        < events.index("activation.rollback.observed")
    )


def test_boundary_newer_attempt_immediately_fences_prior_ready_capability(
    tmp_path: Path,
) -> None:
    signer, ledger, controller, target, _first_adapter, ready = _activate(tmp_path)
    assert ready.capability is not None
    second_descriptor = _descriptor(
        candidate=DIGESTS[5],
        predecessor=DIGESTS[0],
        expected_predecessor=DIGESTS[3],
        expected_activation_receipt_digest=ready.activation_receipt_digest,
        expected_writer_epoch=2,
        writer_epoch=3,
    )
    second_probe = _probe_result(signer, second_descriptor, staged=DIGESTS[6])
    plan, staged_capability = controller.plan(
        probe_result=second_probe,
        target=target,
        port_manifest_digest=PORT_MANIFEST_DIGEST,
        capacity_reservation_digest=DIGESTS[9],
        backup_digest=DIGESTS[10],
        current_lifecycle_epoch=1,
        staged_capability_expires_at=int(time.time()) + 300,
    )
    with pytest.raises(ControllerError, match="fenced by a newer"):
        controller.verify_capability(
            ready.capability,
            target=target,
            port_manifest_digest=PORT_MANIFEST_DIGEST,
        )
    old_status = controller.status(
        ready.transition_id,
        target=target,
        port_manifest_digest=PORT_MANIFEST_DIGEST,
    )
    assert old_status["operator_state"] == "superseded_by_newer_attempt"
    assert old_status["broad_writes_allowed"] is False

    target.stage_generation(
        staged_digest=plan.staged_generation_digest,
        candidate_digest=plan.candidate_digest,
        capability=staged_capability,
    )
    second_adapter = MemoryEffectAdapter(signer)
    second_ready = controller.resume(
        transition_id=plan.transition_id,
        adapters=_adapters(second_adapter),
        target=target,
        port_manifest_digest=PORT_MANIFEST_DIGEST,
    )
    assert second_ready.capability is not None
    controller.verify_capability(
        second_ready.capability,
        target=target,
        port_manifest_digest=PORT_MANIFEST_DIGEST,
    )

    third_descriptor = _descriptor(
        candidate=DIGESTS[7],
        predecessor=DIGESTS[5],
        expected_predecessor=DIGESTS[6],
        expected_activation_receipt_digest=second_ready.activation_receipt_digest,
        expected_writer_epoch=3,
        writer_epoch=4,
    )
    third, _third_capability = controller.plan(
        probe_result=_probe_result(signer, third_descriptor, staged=DIGESTS[8]),
        target=target,
        port_manifest_digest=PORT_MANIFEST_DIGEST,
        capacity_reservation_digest=DIGESTS[9],
        backup_digest=DIGESTS[10],
        current_lifecycle_epoch=2,
        staged_capability_expires_at=int(time.time()) + 300,
    )
    ledger.block_transition(third.transition_id, reason="deliberate newer failure")
    with pytest.raises(ControllerError, match="fenced by a newer"):
        controller.verify_capability(
            second_ready.capability,
            target=target,
            port_manifest_digest=PORT_MANIFEST_DIGEST,
        )


def test_boundary_rollback_authorization_immediately_invalidates_old_capability(
    tmp_path: Path,
) -> None:
    fired = False

    def fault(point: str) -> None:
        nonlocal fired
        if not fired and point == "after_authorize_rollback":
            fired = True
            raise InjectedControllerCrash(point)

    signer, ledger, controller, target, adapter = _runtime(tmp_path)
    descriptor = _descriptor()
    ready = controller.activate(
        probe_result=_probe_result(signer, descriptor),
        port_manifest_digest=PORT_MANIFEST_DIGEST,
        capacity_reservation_digest=DIGESTS[9],
        backup_digest=DIGESTS[10],
        current_lifecycle_epoch=0,
        staged_capability_expires_at=int(time.time()) + 300,
        adapters=_adapters(adapter),
        target=target,
    )
    assert ready.capability is not None
    crashing = LocalLifecycleController(ledger=ledger, signer=signer, fault_hook=fault)
    with pytest.raises(InjectedControllerCrash, match="after_authorize_rollback"):
        crashing.rollback(
            transition_id=ready.transition_id,
            lifecycle_activation_epoch=2,
            database_writer_epoch=3,
            adapters=_adapters(adapter),
            target=target,
            port_manifest_digest=PORT_MANIFEST_DIGEST,
        )
    assert crashing.status(ready.transition_id)["broad_writes_allowed"] is False
    with pytest.raises(ControllerError, match="not ready"):
        crashing.verify_capability(ready.capability)


def test_boundary_failed_compensation_cannot_report_rolled_back(
    tmp_path: Path,
) -> None:
    signer, ledger, controller, target, _adapter = _runtime(tmp_path)

    class FailedCompensation(MemoryEffectAdapter):
        def compensate(self, effect: CompensationContext) -> dict[str, Any]:
            raise RuntimeError("compensation unavailable")

    adapter = FailedCompensation(signer)
    ready = controller.activate(
        probe_result=_probe_result(signer, _descriptor()),
        port_manifest_digest=PORT_MANIFEST_DIGEST,
        capacity_reservation_digest=DIGESTS[9],
        backup_digest=DIGESTS[10],
        current_lifecycle_epoch=0,
        staged_capability_expires_at=int(time.time()) + 300,
        adapters=_adapters(adapter),
        target=target,
    )
    with pytest.raises(RuntimeError, match="compensation unavailable"):
        controller.rollback(
            transition_id=ready.transition_id,
            lifecycle_activation_epoch=2,
            database_writer_epoch=3,
            adapters=_adapters(adapter),
            target=target,
            port_manifest_digest=PORT_MANIFEST_DIGEST,
        )
    assert controller.status(ready.transition_id)["state"] == "blocked"
    assert target.authority()["current_generation"] == DIGESTS[3]
    assert target.authority()["visibility_switches"] == 1
    assert any(
        row["phase"] != "committed"
        for row in ledger.compensations(ready.transition_id)
    )


@pytest.mark.parametrize(
    "crash_point",
    [
        "after_authorize_rollback",
        "after_compensation_intent:compensate:unfreeze",
        "after_compensation_apply:compensate:unfreeze",
        "after_compensation_observe:compensate:unfreeze",
        "after_compensation_commit:compensate:unfreeze",
        "after_publish_rollback",
        "after_finalize_rollback",
    ],
)
def test_boundary_rollback_crash_resumes_exact_newer_receipt(
    tmp_path: Path, crash_point: str
) -> None:
    signer, ledger, controller, target, adapter = _runtime(tmp_path)
    descriptor = _descriptor()
    ready = controller.activate(
        probe_result=_probe_result(signer, descriptor),
        port_manifest_digest=PORT_MANIFEST_DIGEST,
        capacity_reservation_digest=DIGESTS[9],
        backup_digest=DIGESTS[10],
        current_lifecycle_epoch=0,
        staged_capability_expires_at=int(time.time()) + 300,
        adapters=_adapters(adapter),
        target=target,
    )
    fired = False

    def fault(point: str) -> None:
        nonlocal fired
        if not fired and point == crash_point:
            fired = True
            raise InjectedControllerCrash(point)

    crashing = LocalLifecycleController(ledger=ledger, signer=signer, fault_hook=fault)
    rollback = {
        "transition_id": ready.transition_id,
        "lifecycle_activation_epoch": 2,
        "database_writer_epoch": 3,
        "adapters": _adapters(adapter),
        "target": target,
        "port_manifest_digest": PORT_MANIFEST_DIGEST,
    }
    with pytest.raises(InjectedControllerCrash, match=crash_point):
        crashing.rollback(**rollback)

    recovered = LocalLifecycleController(ledger=ledger, signer=signer).rollback(
        **rollback
    )
    assert recovered.state == "rolled_back"
    assert recovered.capability is not None
    assert target.authority()["visibility_switches"] == 2
    LocalLifecycleController(ledger=ledger, signer=signer).verify_capability(
        recovered.capability,
        target=target,
        port_manifest_digest=PORT_MANIFEST_DIGEST,
    )
    with sqlite3.connect(ledger.path) as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM activation_receipts WHERE receipt_kind='rollback'"
            ).fetchone()[0]
            == 1
        )
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM compensations WHERE phase<>'committed'"
            ).fetchone()[0]
            == 0
        )


def test_boundary_probe_result_is_atomic_policy_bound_and_single_use(
    tmp_path: Path,
) -> None:
    signer, ledger, controller, _target, _adapter = _runtime(tmp_path)
    descriptor = _descriptor()
    result = _probe_result(signer, descriptor, nonce="one-use")
    swapped_output = {
        **result.output,
        "staged_generation_digest": DIGESTS[12],
    }
    with pytest.raises(ControllerError, match="output"):
        controller.plan(
            probe_result=ProbeResult(
                output=swapped_output, attestation=result.attestation
            ),
            target=_target,
            port_manifest_digest=PORT_MANIFEST_DIGEST,
            capacity_reservation_digest=DIGESTS[9],
            backup_digest=DIGESTS[10],
            current_lifecycle_epoch=0,
            staged_capability_expires_at=int(time.time()) + 300,
        )
    wrong_profile = replace(result.attestation, sandbox_profile_digest=DIGESTS[11])
    with pytest.raises(ControllerError, match="profile"):
        controller.plan(
            probe_result=ProbeResult(output=result.output, attestation=wrong_profile),
            target=_target,
            port_manifest_digest=PORT_MANIFEST_DIGEST,
            capacity_reservation_digest=DIGESTS[9],
            backup_digest=DIGESTS[10],
            current_lifecycle_epoch=0,
            staged_capability_expires_at=int(time.time()) + 300,
        )
    first_plan, _first_capability = controller.plan(
        probe_result=result,
        target=_target,
        port_manifest_digest=PORT_MANIFEST_DIGEST,
        capacity_reservation_digest=DIGESTS[9],
        backup_digest=DIGESTS[10],
        current_lifecycle_epoch=0,
        staged_capability_expires_at=int(time.time()) + 300,
    )
    retried_plan, _retried_capability = controller.plan(
        probe_result=result,
        target=_target,
        port_manifest_digest=PORT_MANIFEST_DIGEST,
        capacity_reservation_digest=DIGESTS[9],
        backup_digest=DIGESTS[10],
        current_lifecycle_epoch=0,
        staged_capability_expires_at=first_plan.staged_capability_expires_at,
    )
    assert retried_plan == first_plan
    with sqlite3.connect(ledger.path) as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM probe_attestation_consumptions"
            ).fetchone()[0]
            == 1
        )


@pytest.mark.parametrize("mutation", ["empty", "partial", "reordered", "identity"])
def test_boundary_descriptor_refuses_noncanonical_saga(mutation: str) -> None:
    payload = _descriptor().payload()
    if mutation == "empty":
        payload["effects"] = []
    elif mutation == "partial":
        payload["effects"] = payload["effects"][:-1]
    elif mutation == "reordered":
        payload["effects"][0], payload["effects"][1] = (
            payload["effects"][1],
            payload["effects"][0],
        )
    else:
        payload["effects"][0]["request"]["environment_identity"] = DIGESTS[10]
        payload["effects"][0]["request_digest"] = digest_json(
            payload["effects"][0]["request"]
        )
    with pytest.raises(ProtocolError, match="canonical|identities"):
        ReleaseDescriptor.from_mapping(payload)


def test_boundary_valid_content_digest_with_forged_upgrade_signature_refuses(
    tmp_path: Path,
) -> None:
    signer, ledger, controller, target, _adapter = _runtime(tmp_path)
    descriptor = _descriptor()
    good = MemoryEffectAdapter(signer)

    class ForgingUpgrade(MemoryEffectAdapter):
        def apply(self, effect: EffectContext) -> dict[str, Any]:
            result = super().apply(effect)
            self.attestations[effect.effect_key] = replace(
                self.attestations[effect.effect_key],
                signature="hmac-sha256:" + "0" * 64,
            )
            return result

    adapters = _adapters(good)
    adapters[UPGRADE_EFFECT_KIND] = ForgingUpgrade(signer)
    with pytest.raises(ControllerError, match="signature"):
        controller.activate(
            probe_result=_probe_result(signer, descriptor),
            port_manifest_digest=PORT_MANIFEST_DIGEST,
            capacity_reservation_digest=DIGESTS[9],
            backup_digest=DIGESTS[10],
            current_lifecycle_epoch=0,
            staged_capability_expires_at=int(time.time()) + 300,
            adapters=adapters,
            target=target,
        )
    with sqlite3.connect(ledger.path) as conn:
        transition_id = conn.execute(
            "SELECT transition_id FROM transitions"
        ).fetchone()[0]
        assert (
            conn.execute("SELECT COUNT(*) FROM activation_receipts").fetchone()[0] == 0
        )
    assert controller.status(transition_id)["state"] == "blocked"


def test_boundary_installation_manifest_and_concurrency_are_single_authority(
    tmp_path: Path,
) -> None:
    signer, ledger, controller, _target, _adapter = _runtime(tmp_path)
    with pytest.raises(LedgerError, match="owned by this installation"):
        ControllerLedger(
            tmp_path / "alternate.sqlite3", installation=ledger.installation
        )
    first = _descriptor()
    controller.plan(
        probe_result=_probe_result(signer, first),
        target=_target,
        port_manifest_digest=PORT_MANIFEST_DIGEST,
        capacity_reservation_digest=DIGESTS[9],
        backup_digest=DIGESTS[10],
        current_lifecycle_epoch=0,
        staged_capability_expires_at=int(time.time()) + 300,
    )
    second = _descriptor(candidate=DIGESTS[10])
    with pytest.raises(LedgerError, match="nonterminal"):
        controller.plan(
            probe_result=_probe_result(signer, second),
            target=_target,
            port_manifest_digest=PORT_MANIFEST_DIGEST,
            capacity_reservation_digest=DIGESTS[9],
            backup_digest=DIGESTS[10],
            current_lifecycle_epoch=1,
            staged_capability_expires_at=int(time.time()) + 300,
        )


def test_boundary_parallel_resumes_serialize_external_effect_invocation(
    tmp_path: Path,
) -> None:
    signer, ledger, controller, target, _adapter = _runtime(tmp_path)
    descriptor = _descriptor()
    plan, capability = controller.plan(
        probe_result=_probe_result(signer, descriptor),
        target=target,
        port_manifest_digest=PORT_MANIFEST_DIGEST,
        capacity_reservation_digest=DIGESTS[9],
        backup_digest=DIGESTS[10],
        current_lifecycle_epoch=0,
        staged_capability_expires_at=int(time.time()) + 300,
    )
    assert plan is not None and capability is not None
    target.stage_generation(
        staged_digest=plan.staged_generation_digest,
        candidate_digest=plan.candidate_digest,
        capability=capability,
    )

    class RacingAdapter(MemoryEffectAdapter):
        def __init__(self, authority: AuthoritySigner) -> None:
            super().__init__(authority)
            self._observation_lock = Lock()
            self._second_observer = Event()
            self._approval_observers = 0

        def observe(self, effect: EffectContext) -> EffectObservation:
            observation = super().observe(effect)
            if effect.effect_key == "approval" and not observation.satisfied:
                with self._observation_lock:
                    self._approval_observers += 1
                    ordinal = self._approval_observers
                if ordinal == 1:
                    self._second_observer.wait(0.25)
                else:
                    self._second_observer.set()
            return observation

    racing = RacingAdapter(signer)

    def resume() -> TransitionResult:
        isolated = LocalLifecycleController(
            ledger=ControllerLedger(ledger.path, installation=ledger.installation),
            signer=signer,
        )
        return isolated.resume(
            transition_id=plan.transition_id,
            adapters=_adapters(racing),
            target=target,
            port_manifest_digest=PORT_MANIFEST_DIGEST,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = [future.result() for future in (pool.submit(resume), pool.submit(resume))]
    assert [result.state for result in results] == ["ready", "ready"]
    assert all(count == 1 for count in racing.apply_counts.values())


def test_boundary_execution_lock_serializes_independent_processes(
    tmp_path: Path,
) -> None:
    _signer_root, ledger, _controller, _target, _adapter = _runtime(tmp_path)
    trace = tmp_path / "process-lock-trace.jsonl"
    barrier = tmp_path / "process-lock-start"
    context = multiprocessing.get_context("spawn")
    processes = [
        context.Process(
            target=_record_process_execution_lock,
            args=(
                str(ledger.path),
                ledger.installation.payload(),
                str(trace),
                str(barrier),
            ),
        )
        for _ in range(2)
    ]
    for process in processes:
        process.start()
    barrier.touch()
    for process in processes:
        process.join(timeout=15)
        if process.is_alive():
            process.kill()
            process.join(timeout=5)
        assert process.exitcode == 0

    events = [
        json.loads(line) for line in trace.read_text(encoding="utf-8").splitlines()
    ]
    assert [event["event"] for event in events] == ["enter", "exit", "enter", "exit"]
    assert len({event["pid"] for event in events}) == 2


def test_boundary_resume_refuses_changed_adapter_set_digest(
    tmp_path: Path,
) -> None:
    signer_root, _ledger, controller, target, _adapter = _runtime(tmp_path)
    plan, _capability = controller.plan(
        probe_result=_probe_result(signer_root, _descriptor()),
        target=target,
        port_manifest_digest=PORT_MANIFEST_DIGEST,
        capacity_reservation_digest=DIGESTS[9],
        backup_digest=DIGESTS[10],
        current_lifecycle_epoch=0,
        staged_capability_expires_at=int(time.time()) + 300,
    )
    assert plan is not None
    with pytest.raises(ControllerError, match="adapter set differs"):
        controller.resume(
            transition_id=plan.transition_id,
            adapters={},
            target=object(),
            port_manifest_digest=DIGESTS[11],
        )


def test_boundary_live_status_and_noop_fail_closed_on_stale_observation(
    tmp_path: Path,
) -> None:
    signer, _ledger_value, controller, target, _adapter, ready = _activate(tmp_path)
    receipt = controller.ledger.receipt(ready.transition_id)

    class StaleTarget:
        def observe_authorized_generation(self, observed_receipt):
            result = target.observe_authorized_generation(observed_receipt)
            evidence = {key: value for key, value in result.items() if key != "proof"}
            evidence["expires_at"] = 1
            proof = ObservationProof.issue(
                signer=signer.signer_for("observation"),
                subject_kind="publication",
                subject_digest=controller._publication_subject(receipt, "ready"),
                evidence=evidence,
                nonce="stale-live",
            )
            return {**evidence, "proof": proof.__dict__}

    status = controller.status(
        ready.transition_id,
        target=StaleTarget(),
        port_manifest_digest=PORT_MANIFEST_DIGEST,
    )
    assert status["operator_state"] == "live_state_unavailable"
    assert status["broad_writes_allowed"] is False

    no_op_descriptor = _descriptor(candidate=DIGESTS[1], predecessor=DIGESTS[1])
    no_op_result = _probe_result(signer, no_op_descriptor)
    no_op_target = SQLiteReferenceTarget(
        path=tmp_path / "noop-target.sqlite3",
        signer=signer,
        deployment="local-test",
        predecessor=DIGESTS[2],
        database_writer_epoch=1,
        activation_receipt_digest=DIGESTS[4],
        service_plan_digest=DIGESTS[13],
    )

    class StaleNoOp:
        def observe_current_state(self, descriptor):
            result = no_op_target.observe_current_state(descriptor)
            evidence = {key: value for key, value in result.items() if key != "proof"}
            evidence["expires_at"] = 1
            subject = digest_json(
                {
                    "schema": "okg.local-controller.no-op-subject/v1",
                    "descriptor": descriptor.payload(),
                }
            )
            proof = ObservationProof.issue(
                signer=signer.signer_for("observation"),
                subject_kind="no_op",
                subject_digest=subject,
                evidence=evidence,
                nonce="stale-noop",
            )
            return {**evidence, "proof": proof.__dict__}

    second_ledger = _ledger(tmp_path, signer=signer, name="noop-ledger.sqlite3")
    with pytest.raises(ControllerError, match="stale"):
        LocalLifecycleController(ledger=second_ledger, signer=signer).activate(
            probe_result=no_op_result,
            port_manifest_digest=PORT_MANIFEST_DIGEST,
            capacity_reservation_digest=DIGESTS[9],
            backup_digest=DIGESTS[10],
            current_lifecycle_epoch=0,
            staged_capability_expires_at=int(time.time()) + 300,
            adapters={},
            target=StaleNoOp(),
        )
    retried = LocalLifecycleController(ledger=second_ledger, signer=signer).activate(
        probe_result=no_op_result,
        port_manifest_digest=PORT_MANIFEST_DIGEST,
        capacity_reservation_digest=DIGESTS[9],
        backup_digest=DIGESTS[10],
        current_lifecycle_epoch=0,
        staged_capability_expires_at=int(time.time()) + 300,
        adapters={},
        target=no_op_target,
    )
    assert retried.state == "no_op"
    assert retried.no_op is True
    with pytest.raises(LedgerError, match="replay"):
        LocalLifecycleController(ledger=second_ledger, signer=signer).activate(
            probe_result=no_op_result,
            port_manifest_digest=PORT_MANIFEST_DIGEST,
            capacity_reservation_digest=DIGESTS[9],
            backup_digest=DIGESTS[10],
            current_lifecycle_epoch=0,
            staged_capability_expires_at=int(time.time()) + 300,
            adapters={},
            target=no_op_target,
        )


def _blocked_preauthorization_transition(
    tmp_path: Path,
) -> tuple[
    AuthoritySigner,
    ControllerLedger,
    LocalLifecycleController,
    SQLiteReferenceTarget,
    MemoryEffectAdapter,
    str,
]:
    signer, ledger, controller, target, adapter = _runtime(tmp_path)

    class PermanentFailure(MemoryEffectAdapter):
        def apply(self, effect: EffectContext) -> dict[str, Any]:
            raise RuntimeError("permanent upgrade failure")

    adapters = _adapters(adapter)
    adapters[UPGRADE_EFFECT_KIND] = PermanentFailure(signer)
    descriptor = _descriptor()
    with pytest.raises(RuntimeError, match="permanent"):
        controller.activate(
            probe_result=_probe_result(signer, descriptor),
            port_manifest_digest=PORT_MANIFEST_DIGEST,
            capacity_reservation_digest=DIGESTS[9],
            backup_digest=DIGESTS[10],
            current_lifecycle_epoch=0,
            staged_capability_expires_at=int(time.time()) + 300,
            adapters=adapters,
            target=target,
        )
    with sqlite3.connect(ledger.path) as conn:
        transition_id = conn.execute(
            "SELECT transition_id FROM transitions"
        ).fetchone()[0]
    return signer, ledger, controller, target, adapter, transition_id


def test_boundary_irreversible_failure_recovers_with_prepublication_rollback(
    tmp_path: Path,
) -> None:
    signer, ledger, controller, target, adapter, transition_id = (
        _blocked_preauthorization_transition(tmp_path)
    )
    committed_keys = [
        key
        for key, _kind, _irreversible in CANONICAL_SAGA_EFFECTS
        if key != "upgrade"
    ][:6]
    assert controller.status(transition_id)["state"] == "blocked"
    assert [
        row["effect_key"]
        for row in ledger.effects(transition_id)
        if row["phase"] == EffectPhase.COMMITTED
    ] == committed_keys
    assert adapter.applied == set(committed_keys)
    assert ledger.compensations(transition_id) == []
    rolled_back = controller.rollback(
        transition_id=transition_id,
        lifecycle_activation_epoch=2,
        database_writer_epoch=3,
        adapters=_adapters(adapter),
        target=target,
        port_manifest_digest=PORT_MANIFEST_DIGEST,
    )
    assert rolled_back.state == "rolled_back"
    expected_reverse = [
        key for key, _kind, _irreversible in reversed(CANONICAL_SAGA_EFFECTS[:7])
    ]
    compensations = ledger.compensations(transition_id)
    assert [row["forward_effect_key"] for row in compensations] == expected_reverse
    assert [row["ordinal"] for row in compensations] == list(
        range(1, len(expected_reverse) + 1)
    )
    assert all(row["phase"] == EffectPhase.COMMITTED for row in compensations)
    assert all(
        row["request"]["rollback_receipt_digest"]
        == rolled_back.activation_receipt_digest
        for row in compensations
    )
    assert adapter.compensation_calls == list(reversed(committed_keys))
    assert adapter.applied == set()
    events = [row["event_type"] for row in ledger.events(transition_id)]
    assert (
        events.index("rollback.compensation_plan_materialized")
        < events.index("rollback.authorized")
        < events.index("activation.rollback.published")
        < events.index("activation.rollback.observed")
    )


@pytest.mark.parametrize(
    "crash_point",
    [
        "after_authorize_rollback",
        "after_compensation_intent:compensate:upgrade",
        "after_compensation_commit:compensate:upgrade",
        "after_compensation_apply:compensate:reserve_writer_epoch",
        "after_compensation_observe:compensate:reserve_writer_epoch",
    ],
)
def test_boundary_preauthorization_compensation_crash_retry_is_exact(
    tmp_path: Path,
    crash_point: str,
) -> None:
    signer, ledger, _controller, target, adapter, transition_id = (
        _blocked_preauthorization_transition(tmp_path)
    )
    fired = False

    def fault(point: str) -> None:
        nonlocal fired
        if not fired and point == crash_point:
            fired = True
            raise InjectedControllerCrash(point)

    rollback = {
        "transition_id": transition_id,
        "lifecycle_activation_epoch": 2,
        "database_writer_epoch": 3,
        "adapters": _adapters(adapter),
        "target": target,
        "port_manifest_digest": PORT_MANIFEST_DIGEST,
    }
    crashing = LocalLifecycleController(ledger=ledger, signer=signer, fault_hook=fault)
    with pytest.raises(InjectedControllerCrash, match=crash_point):
        crashing.rollback(**rollback)

    receipt = ledger.receipt(transition_id, "rollback")
    compensations = ledger.compensations(transition_id)
    expected_reverse = [
        key for key, _kind, _irreversible in reversed(CANONICAL_SAGA_EFFECTS[:7])
    ]
    assert [row["forward_effect_key"] for row in compensations] == expected_reverse
    assert all(
        row["request"]["rollback_receipt_digest"] == receipt.digest
        for row in compensations
    )
    assert any(row["phase"] != EffectPhase.COMMITTED for row in compensations)
    assert crashing.status(transition_id)["state"] != TransitionState.ROLLED_BACK
    assert target.authority()["visibility_switches"] == 0
    with pytest.raises(LedgerError, match="every compensation committed"):
        ledger.record_publication(
            transition_id,
            receipt,
            {"attempt": "premature rollback publication"},
            kind="rollback",
        )

    recovered = LocalLifecycleController(ledger=ledger, signer=signer).rollback(
        **rollback
    )
    assert recovered.state == TransitionState.ROLLED_BACK
    assert all(
        row["phase"] == EffectPhase.COMMITTED
        for row in ledger.compensations(transition_id)
    )
    assert adapter.compensation_calls == [
        key for key, _kind, _irreversible in reversed(CANONICAL_SAGA_EFFECTS[:6])
    ]
    assert adapter.applied == set()
    assert target.authority()["visibility_switches"] == 1
    with sqlite3.connect(ledger.path) as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM activation_receipts "
                "WHERE transition_id=? AND receipt_kind='rollback'",
                (transition_id,),
            ).fetchone()[0]
            == 1
        )


@pytest.mark.parametrize("corruption", ["partial", "conflicting"])
def test_boundary_preauthorization_compensation_plan_refuses_corruption(
    tmp_path: Path,
    corruption: str,
) -> None:
    signer, ledger, _controller, target, adapter, transition_id = (
        _blocked_preauthorization_transition(tmp_path)
    )

    def crash_after_authorization(point: str) -> None:
        if point == "after_authorize_rollback":
            raise InjectedControllerCrash(point)

    rollback = {
        "transition_id": transition_id,
        "lifecycle_activation_epoch": 2,
        "database_writer_epoch": 3,
        "adapters": _adapters(adapter),
        "target": target,
        "port_manifest_digest": PORT_MANIFEST_DIGEST,
    }
    with pytest.raises(InjectedControllerCrash, match="after_authorize_rollback"):
        LocalLifecycleController(
            ledger=ledger,
            signer=signer,
            fault_hook=crash_after_authorization,
        ).rollback(**rollback)

    with sqlite3.connect(ledger.path) as conn:
        if corruption == "partial":
            conn.execute(
                "DELETE FROM compensations WHERE transition_id=? AND ordinal=6",
                (transition_id,),
            )
        else:
            conn.execute(
                "UPDATE compensations SET request_digest=? "
                "WHERE transition_id=? AND ordinal=1",
                (DIGESTS[15], transition_id),
            )
        conn.commit()

    with pytest.raises(LedgerError, match="partial|conflicts"):
        LocalLifecycleController(ledger=ledger, signer=signer).rollback(**rollback)
    assert target.authority()["visibility_switches"] == 0
    assert LocalLifecycleController(ledger=ledger, signer=signer).status(
        transition_id
    )["state"] != TransitionState.ROLLED_BACK


@pytest.mark.parametrize(
    ("stopping_phase", "forward_fault"),
    [
        (EffectPhase.INTENDED, "apply response lost"),
        (EffectPhase.APPLIED, "independently observed"),
        (EffectPhase.OBSERVED, "after_observe:upgrade"),
        (EffectPhase.COMMITTED, "after_commit:upgrade"),
    ],
)
def test_boundary_rollback_compensates_every_possibly_applied_phase(
    tmp_path: Path,
    stopping_phase: EffectPhase,
    forward_fault: str,
) -> None:
    signer, ledger, _controller, target, _adapter = _runtime(tmp_path)
    compensation_order: list[str] = []

    class LoggedMemoryEffect(MemoryEffectAdapter):
        def compensate(self, effect: CompensationContext) -> dict[str, Any]:
            compensation_order.append(effect.forward_effect_key)
            return super().compensate(effect)

    class PhaseStoppingUpgrade(LoggedMemoryEffect):
        def apply(self, effect: EffectContext) -> dict[str, Any]:
            result = super().apply(effect)
            if stopping_phase == EffectPhase.INTENDED:
                raise RuntimeError("upgrade apply response lost")
            return result

        def observe(self, effect: EffectContext) -> EffectObservation:
            observation = super().observe(effect)
            if (
                stopping_phase == EffectPhase.APPLIED
                and effect.effect_key in self.applied
            ):
                evidence = {
                    "satisfied": False,
                    "upgrade_attestation": self.attestations[
                        effect.effect_key
                    ].payload(),
                }
                proof = ObservationProof.issue(
                    signer=self.signer.signer_for("observation"),
                    subject_kind="effect",
                    subject_digest=effect.observation_subject_digest,
                    evidence=evidence,
                    nonce=f"unsatisfied-{effect.transition_id}-{effect.effect_key}",
                )
                return EffectObservation(evidence=evidence, proof=proof)
            return observation

    def forward_crash(point: str) -> None:
        if stopping_phase == EffectPhase.OBSERVED and point == "after_observe:upgrade":
            raise InjectedControllerCrash(point)
        if stopping_phase == EffectPhase.COMMITTED and point == "after_commit:upgrade":
            raise InjectedControllerCrash(point)

    forward_adapter = LoggedMemoryEffect(signer)
    upgrade_adapter = PhaseStoppingUpgrade(signer)
    adapters = _adapters(forward_adapter)
    adapters[UPGRADE_EFFECT_KIND] = upgrade_adapter
    controller = LocalLifecycleController(
        ledger=ledger,
        signer=signer,
        fault_hook=forward_crash,
    )
    with pytest.raises(
        (RuntimeError, EffectError, InjectedControllerCrash),
        match=forward_fault,
    ):
        controller.activate(
            probe_result=_probe_result(signer, _descriptor()),
            port_manifest_digest=PORT_MANIFEST_DIGEST,
            capacity_reservation_digest=DIGESTS[9],
            backup_digest=DIGESTS[10],
            current_lifecycle_epoch=0,
            staged_capability_expires_at=int(time.time()) + 300,
            adapters=adapters,
            target=target,
        )

    with sqlite3.connect(ledger.path) as conn:
        transition_id = conn.execute(
            "SELECT transition_id FROM transitions"
        ).fetchone()[0]
    phase_by_key = {
        row["effect_key"]: row["phase"] for row in ledger.effects(transition_id)
    }
    assert all(
        phase_by_key[key] == EffectPhase.COMMITTED
        for key, _kind, _irreversible in CANONICAL_SAGA_EFFECTS[:6]
    )
    assert phase_by_key["upgrade"] == stopping_phase
    assert all(
        phase_by_key[key] == EffectPhase.PLANNED
        for key, _kind, _irreversible in CANONICAL_SAGA_EFFECTS[7:]
    )
    assert forward_adapter.applied == {
        key for key, _kind, _irreversible in CANONICAL_SAGA_EFFECTS[:6]
    }
    assert upgrade_adapter.applied == {"upgrade"}
    assert ledger.compensations(transition_id) == []

    rolled_back = LocalLifecycleController(ledger=ledger, signer=signer).rollback(
        transition_id=transition_id,
        lifecycle_activation_epoch=2,
        database_writer_epoch=3,
        adapters=adapters,
        target=target,
        port_manifest_digest=PORT_MANIFEST_DIGEST,
    )
    expected_reverse = [
        key for key, _kind, _irreversible in reversed(CANONICAL_SAGA_EFFECTS[:7])
    ]
    compensations = ledger.compensations(transition_id)
    assert [row["forward_effect_key"] for row in compensations] == expected_reverse
    assert not {
        key for key, _kind, _irreversible in CANONICAL_SAGA_EFFECTS[7:]
    } & {row["forward_effect_key"] for row in compensations}
    assert all(row["phase"] == EffectPhase.COMMITTED for row in compensations)
    assert all(
        row["request"]["rollback_receipt_digest"]
        == rolled_back.activation_receipt_digest
        for row in compensations
    )
    assert compensation_order == expected_reverse
    assert forward_adapter.applied == set()
    assert upgrade_adapter.applied == set()
    assert rolled_back.state == TransitionState.ROLLED_BACK
    assert target.authority()["visibility_switches"] == 1


def test_boundary_rollback_retry_and_retirement_are_exact_and_resumable(
    tmp_path: Path,
) -> None:
    signer, ledger, controller, target, adapter, ready = _activate(tmp_path)
    fired = False

    def rollback_fault(point: str) -> None:
        nonlocal fired
        if not fired and point == "after_authorize_rollback":
            fired = True
            raise InjectedControllerCrash(point)

    crashing = LocalLifecycleController(
        ledger=ledger, signer=signer, fault_hook=rollback_fault
    )
    with pytest.raises(InjectedControllerCrash):
        crashing.rollback(
            transition_id=ready.transition_id,
            lifecycle_activation_epoch=2,
            database_writer_epoch=3,
            adapters=_adapters(adapter),
            target=target,
            port_manifest_digest=PORT_MANIFEST_DIGEST,
        )
    with pytest.raises(LedgerError, match="retry disagrees"):
        controller.rollback(
            transition_id=ready.transition_id,
            lifecycle_activation_epoch=3,
            database_writer_epoch=3,
            adapters=_adapters(adapter),
            target=target,
            port_manifest_digest=PORT_MANIFEST_DIGEST,
        )
    rolled_back = controller.rollback(
        transition_id=ready.transition_id,
        lifecycle_activation_epoch=2,
        database_writer_epoch=3,
        adapters=_adapters(adapter),
        target=target,
        port_manifest_digest=PORT_MANIFEST_DIGEST,
    )
    assert rolled_back.capability is not None
    receipt = ledger.receipt(ready.transition_id, "rollback")

    class RetirementAdapter(MemoryEffectAdapter):
        def apply(self, effect: EffectContext) -> dict[str, Any]:
            result = super().apply(effect)
            if effect.effect_key == "retire_withdraw":
                target.mark_retired(receipt)
            return result

    retirement_adapter = RetirementAdapter(signer)
    retirement_adapters = {
        kind: retirement_adapter for _key, kind in RETIREMENT_EFFECTS
    }
    retirement_fired = False

    def retirement_fault(point: str) -> None:
        nonlocal retirement_fired
        if not retirement_fired and point == "after_retirement_apply:retire_stop":
            retirement_fired = True
            raise InjectedControllerCrash(point)

    with pytest.raises(InjectedControllerCrash, match="retire_stop"):
        LocalLifecycleController(
            ledger=ledger, signer=signer, fault_hook=retirement_fault
        ).retire(
            transition_id=ready.transition_id,
            adapters=retirement_adapters,
            target=target,
            port_manifest_digest=PORT_MANIFEST_DIGEST,
        )
    assert controller.status(ready.transition_id)["state"] == "retiring"
    retired = controller.retire(
        transition_id=ready.transition_id,
        adapters=retirement_adapters,
        target=target,
        port_manifest_digest=PORT_MANIFEST_DIGEST,
    )
    assert retired.state == "retired"
    with pytest.raises(ControllerError, match="not ready"):
        controller.verify_capability(
            rolled_back.capability,
            target=target,
            port_manifest_digest=PORT_MANIFEST_DIGEST,
        )
    assert (
        controller.status(
            ready.transition_id,
            target=target,
            port_manifest_digest=PORT_MANIFEST_DIGEST,
        )["operator_state"]
        == "retired"
    )


def test_probe_sandbox_denies_network_secret_and_protected_mutation(
    tmp_path: Path,
) -> None:
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    protected = tmp_path / "deployment-authority"
    protected.write_text("unchanged", encoding="utf-8")
    script = candidate / "probe.py"
    script.write_text(
        """
import json, os, socket
network_denied = False
mutation_denied = False
input_mutation_denied = False
try:
    socket.socket().connect(('127.0.0.1', 9))
except Exception:
    network_denied = True
try:
    with open(os.environ['PROTECTED'], 'w') as handle:
        handle.write('changed')
except Exception:
    mutation_denied = True
try:
    with open(os.environ['OKG_PROBE_INPUT'], 'w') as handle:
        handle.write('changed')
except Exception:
    input_mutation_denied = True
out = os.environ['OKG_PROBE_OUTPUT'] + '/result.json'
with open(out, 'w') as handle:
    json.dump({
        'network_denied': network_denied,
        'mutation_denied': mutation_denied,
        'input_mutation_denied': input_mutation_denied,
        'lang': os.environ.get('LANG'),
        'environment_names': sorted(os.environ),
        'dsn_absent': 'OKG_DSN' not in os.environ,
        'secret_absent': 'SECRET_TOKEN' not in os.environ,
        'ledger_absent': 'OKG_CONTROLLER_LEDGER' not in os.environ,
        'signing_key_absent': 'OKG_CONTROLLER_SIGNING_KEY' not in os.environ,
    }, handle)
""",
        encoding="utf-8",
    )
    # PROTECTED is deliberately not admitted by the launcher allowlist, so put
    # the literal path in the script without exposing it as ambient authority.
    script.write_text(
        script.read_text(encoding="utf-8").replace(
            "os.environ['PROTECTED']", repr(str(protected))
        ),
        encoding="utf-8",
    )
    launcher = _trusted_probe_launcher(
        signer=_signer(),
        limits=SandboxLimits(
            timeout_seconds=5,
            cpu_seconds=2,
            memory_bytes=128 * 1024 * 1024,
            output_bytes=64 * 1024,
            process_count=4,
        ),
    )
    intermediate_python = candidate / "python"
    intermediate_python.symlink_to(sys.executable)
    requested_python = candidate / "python3"
    requested_python.symlink_to("python")
    output, attestation = launcher.launch(
        candidate_artifact=candidate,
        command=(str(requested_python), str(script)),
        input_payload={"operation": "plan"},
        protected_paths=(protected,),
        harmless_environment={"LANG": "C"},
    )
    assert output == {
        "network_denied": True,
        "mutation_denied": True,
        "input_mutation_denied": True,
        "lang": "C",
        "environment_names": [
            "LANG",
            "OKG_PROBE_INPUT",
            "OKG_PROBE_OUTPUT",
            "PYTHONCOERCECLOCALE",
            "PYTHONDONTWRITEBYTECODE",
        ],
        "dsn_absent": True,
        "secret_absent": True,
        "ledger_absent": True,
        "signing_key_absent": True,
    }
    assert attestation.cpu_limit_seconds == 2
    assert attestation.memory_limit_bytes == 128 * 1024 * 1024
    assert attestation.output_limit_bytes == 64 * 1024
    assert attestation.mutation_canary_before == attestation.mutation_canary_after
    assert (
        _signer()
        .signer_for("probe")
        .verify(attestation.unsigned_payload(), attestation.signature)
    )
    assert protected.read_text(encoding="utf-8") == "unchanged"


def test_probe_macos_profile_grants_no_ambient_mach_lookup(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate"
    output = tmp_path / "output"
    candidate.mkdir()
    output.mkdir()
    script = candidate / "probe.py"
    script.write_text("raise SystemExit(0)\n", encoding="utf-8")
    input_file = tmp_path / "input.json"
    input_file.write_text("{}", encoding="utf-8")
    backend = object.__new__(MacSandboxBackend)
    backend.executable = "/usr/bin/sandbox-exec"
    _command, metadata = backend.wrap(
        (sys.executable, str(script)),
        candidate_root=candidate,
        input_file=input_file,
        output_root=output,
        environment={},
        limits=SandboxLimits(),
    )
    assert "(allow mach-lookup)" not in metadata["profile"]
    assert "(allow process-fork)" not in metadata["profile"]
    assert "(deny process-fork)" in metadata["profile"]
    assert metadata["process_isolation"] == "fork-denied"


def test_probe_linux_profile_uses_disposable_pid_namespace(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    input_file = tmp_path / "input.json"
    input_file.write_text("{}", encoding="utf-8")
    output = tmp_path / "output"
    output.mkdir()
    backend = BubblewrapSandboxBackend(executable="/usr/bin/bwrap")

    environment = {
        "LANG": "C",
        "OKG_PROBE_INPUT": str(input_file),
        "OKG_PROBE_OUTPUT": str(output),
        "PYTHONCOERCECLOCALE": "0",
        "PYTHONDONTWRITEBYTECODE": "1",
    }
    command, metadata = backend.wrap(
        (sys.executable, "-c", "pass"),
        candidate_root=candidate,
        input_file=input_file,
        output_root=output,
        environment=environment,
        limits=SandboxLimits(process_count=4),
    )

    assert command.index("--die-with-parent") < command.index("--unshare-all")
    assert "--new-session" not in command
    assert command.index("--tmpfs") < command.index("--ro-bind")
    assert command[command.index("--tmpfs") + 1] == "/"
    assert command.index("--bind") < command.index("--remount-ro")
    assert command[command.index("--remount-ro") + 1] == "/"
    assert "--clearenv" in command
    separator = command.index("--")
    for name, value in environment.items():
        setting = command.index("--setenv", command.index("--clearenv") + 1)
        while command[setting + 1] != name:
            setting = command.index("--setenv", setting + 1)
        assert command[setting + 2] == value
        assert setting < separator
    assert "--proc" in command
    assert metadata["process_isolation"] == (
        "launcher-session-plus-pid-namespace-supervisor"
    )
    assert metadata["process_limit_enforcement"] == (
        "candidate-namespace-rlimit-nproc"
    )
    assert metadata["reserved_exit_statuses"] == {"125": "descendant-processes"}
    assert command[separator + 1 : separator + 6] == [
        "/bin/sh",
        "-c",
        (
            'ulimit -u "$1"; shift; /usr/bin/env -i "$@"; status=$?; '
            "for process in /proc/[0-9]*; do "
            'pid=${process##*/}; if [ "$pid" != 1 ] && [ "$pid" != "$$" ]; '
            'then exit 125; fi; done; exit "$status"'
        ),
        "okg-probe-process-limit",
        "4",
    ]
    environment_args = [f"{name}={value}" for name, value in sorted(environment.items())]
    assert command[separator + 6 : separator + 6 + len(environment_args)] == (
        environment_args
    )
    assert command[separator + 6 + len(environment_args) :] == [
        sys.executable,
        "-c",
        "pass",
    ]


def test_probe_linux_profile_binds_virtualenv_interpreter_link_target(
    tmp_path: Path,
) -> None:
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    input_file = tmp_path / "input.json"
    input_file.write_text("{}", encoding="utf-8")
    output = tmp_path / "output"
    output.mkdir()
    versioned_runtime = tmp_path / "runtime-versioned"
    versioned_bin = versioned_runtime / "bin"
    versioned_bin.mkdir(parents=True)
    versioned_python = versioned_bin / "python3.12"
    versioned_python.write_bytes(b"\x7fELF")
    runtime_alias = tmp_path / "runtime-current"
    runtime_alias.symlink_to(versioned_runtime, target_is_directory=True)
    virtualenv = tmp_path / "venv"
    virtualenv_bin = virtualenv / "bin"
    virtualenv_bin.mkdir(parents=True)
    intermediate_python = virtualenv_bin / "python"
    intermediate_python.symlink_to(runtime_alias / "bin" / "python3.12")
    requested_python = virtualenv_bin / "python3"
    requested_python.symlink_to("python")
    backend = BubblewrapSandboxBackend(executable="/usr/bin/bwrap")

    _command, metadata = backend.wrap(
        (str(requested_python), "-c", "pass"),
        candidate_root=candidate,
        input_file=input_file,
        output_root=output,
        environment={},
        limits=SandboxLimits(),
    )

    assert metadata["runtime_read_roots"] == [
        str(virtualenv),
        str(runtime_alias),
        str(versioned_runtime),
    ]


def test_probe_launcher_refuses_when_linux_sandbox_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(probe_module.platform, "system", lambda: "Linux")
    monkeypatch.setattr(probe_module.shutil, "which", lambda _name: None)

    with pytest.raises(ProbeError, match="bubblewrap is unavailable"):
        TrustedProbeLauncher(signer=_signer())


def test_probe_sandbox_malformed_self_attestation_and_timeout_refuse(
    tmp_path: Path,
) -> None:
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    script = candidate / "probe.py"
    launcher = _trusted_probe_launcher(
        signer=_signer(),
        limits=SandboxLimits(
            timeout_seconds=1,
            cpu_seconds=2,
            memory_bytes=128 * 1024 * 1024,
            output_bytes=4096,
            process_count=4,
        ),
    )
    script.write_text(
        "import os; open(os.environ['OKG_PROBE_OUTPUT'] + '/result.json', 'w').write('{')",
        encoding="utf-8",
    )
    with pytest.raises(ProbeError, match="malformed"):
        launcher.launch(
            candidate_artifact=candidate,
            command=(sys.executable, str(script)),
            input_payload={},
        )
    script.write_text(
        "import json, os; json.dump({'signature':'fake'}, open(os.environ['OKG_PROBE_OUTPUT'] + '/result.json','w'))",
        encoding="utf-8",
    )
    with pytest.raises(ProbeError, match="cannot attest"):
        launcher.launch(
            candidate_artifact=candidate,
            command=(sys.executable, str(script)),
            input_payload={},
        )
    # Linux reserves 125 for the in-namespace descendant supervisor.
    script.write_text("raise SystemExit(125)\n", encoding="utf-8")
    exit_125_error = (
        "descendant processes"
        if launcher.backend.descendant_exit_status == 125
        else "exit status 125"
    )
    with pytest.raises(ProbeError, match=exit_125_error):
        launcher.launch(
            candidate_artifact=candidate,
            command=(sys.executable, str(script)),
            input_payload={},
        )
    script.write_text("while True: pass\n", encoding="utf-8")
    with pytest.raises(ProbeError, match="time limit"):
        launcher.launch(
            candidate_artifact=candidate,
            command=(sys.executable, str(script)),
            input_payload={},
        )


@pytest.mark.parametrize("escape_session", [False, True])
def test_probe_sandbox_refuses_descendant_after_leader_exits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    escape_session: bool,
) -> None:
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    script = candidate / "probe.py"
    script.write_text(
        """
import json, os, time
root = os.environ['OKG_PROBE_OUTPUT']
child = os.fork()
if child == 0:
    if ESCAPE_SESSION:
        os.setsid()
    time.sleep(0.2)
    with open(root + '/late-write', 'w') as handle:
        handle.write('escaped')
    while True:
        time.sleep(1)
with open(root + '/result.json', 'w') as handle:
    json.dump({'ok': True}, handle)
""",
        encoding="utf-8",
    )
    script.write_text(
        script.read_text(encoding="utf-8").replace(
            "ESCAPE_SESSION", repr(escape_session)
        ),
        encoding="utf-8",
    )
    launcher = _trusted_probe_launcher(
        signer=_signer(),
        limits=SandboxLimits(
            timeout_seconds=2,
            cpu_seconds=3,
            memory_bytes=128 * 1024 * 1024,
            output_bytes=4096,
            process_count=4096,
        ),
    )
    real_popen = probe_module.subprocess.Popen
    process_groups: list[int] = []

    def recording_popen(*args: Any, **kwargs: Any) -> subprocess.Popen[Any]:
        process = real_popen(*args, **kwargs)
        process_groups.append(process.pid)
        return process

    monkeypatch.setattr(probe_module.subprocess, "Popen", recording_popen)

    started = time.monotonic()
    with pytest.raises(ProbeError, match="refused|descendant processes"):
        launcher.launch(
            candidate_artifact=candidate,
            command=(sys.executable, str(script)),
            input_payload={},
        )
    assert time.monotonic() - started < 2
    assert process_groups and not probe_module._process_group_has_executable_members(
        process_groups[0]
    )


def test_probe_sandbox_process_cap_refuses_fork_storm(tmp_path: Path) -> None:
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    script = candidate / "probe.py"
    script.write_text(
        """
import os, time
while True:
    child = os.fork()
    if child == 0:
        time.sleep(10)
        raise SystemExit(0)
""",
        encoding="utf-8",
    )
    launcher = _trusted_probe_launcher(
        signer=_signer(),
        limits=SandboxLimits(
            timeout_seconds=2,
            cpu_seconds=3,
            memory_bytes=128 * 1024 * 1024,
            output_bytes=4096,
            process_count=4,
        ),
    )

    started = time.monotonic()
    with pytest.raises(ProbeError, match="refused|descendant processes"):
        launcher.launch(
            candidate_artifact=candidate,
            command=(sys.executable, str(script)),
            input_payload={},
        )
    assert time.monotonic() - started < 2


def test_probe_sandbox_memory_output_environment_and_platform_limits_refuse(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    candidate = tmp_path / "candidate"
    candidate.mkdir()
    script = candidate / "probe.py"

    memory_launcher = _trusted_probe_launcher(
        signer=_signer(),
        limits=SandboxLimits(
            timeout_seconds=5,
            cpu_seconds=4,
            memory_bytes=32 * 1024 * 1024,
            output_bytes=4096,
            process_count=4,
        ),
    )
    script.write_text(
        "blocks = []\nwhile True:\n    blocks.append(bytearray(8 * 1024 * 1024))\n",
        encoding="utf-8",
    )
    with pytest.raises(ProbeError, match="memory limit"):
        memory_launcher.launch(
            candidate_artifact=candidate,
            command=(sys.executable, str(script)),
            input_payload={},
        )

    output_launcher = _trusted_probe_launcher(
        signer=_signer(),
        limits=SandboxLimits(
            timeout_seconds=5,
            cpu_seconds=2,
            memory_bytes=128 * 1024 * 1024,
            output_bytes=1024,
            process_count=4,
        ),
    )
    script.write_text(
        "import os; open(os.environ['OKG_PROBE_OUTPUT'] + '/result.json', 'w').write('x' * 4096)",
        encoding="utf-8",
    )
    with pytest.raises(ProbeError, match="refused|output.*limit"):
        output_launcher.launch(
            candidate_artifact=candidate,
            command=(sys.executable, str(script)),
            input_payload={},
        )

    script.write_text(
        """
import json, os
root = os.environ['OKG_PROBE_OUTPUT']
with open(root + '/result.json', 'w') as handle:
    json.dump({'ok': True}, handle)
for index in range(16):
    with open(root + f'/extra-{index}.bin', 'wb') as handle:
        handle.write(b'x' * 128)
""",
        encoding="utf-8",
    )
    with pytest.raises(ProbeError, match="aggregate output limit"):
        output_launcher.launch(
            candidate_artifact=candidate,
            command=(sys.executable, str(script)),
            input_payload={},
        )

    with pytest.raises(ProbeError, match="forbidden names"):
        output_launcher.launch(
            candidate_artifact=candidate,
            command=(sys.executable, str(script)),
            input_payload={},
            harmless_environment={"OKG_DSN": "must-not-cross"},
        )

    monkeypatch.setattr("okg_local_controller.probe.platform.system", lambda: "Plan9")
    with pytest.raises(ProbeError, match="unsupported sandbox platform"):
        TrustedProbeLauncher(signer=_signer())
