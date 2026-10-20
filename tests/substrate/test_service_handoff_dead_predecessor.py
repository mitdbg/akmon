"""Adversarial checks for predecessor process authority.

A verified receipt whose exact process incarnation is dead is debris rather
than live authority. Missing, malformed, foreign, partial, or self-declared
inventories prove nothing and keep the handoff closed.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from okg.substrate import environment_services as svc
from okg.substrate.deployments import DeploymentConfigError


LIVE_PID = 1
DEAD_PID = 2_147_400_000


def _receipt(tmp_path: Path, name: str, pid: int | None) -> str:
    path = tmp_path / f"{name}.json"
    payload: dict[str, Any] = {"isolated": True}
    if pid is not None:
        payload["pid"] = pid
    path.write_text(json.dumps(payload), encoding="utf-8")
    return str(path)


def _authority(worker: dict[str, Any], services: dict[str, Any]) -> dict[str, Any]:
    environment_id = "fixture"
    worker = {
        "schema": "okg.environment_worker_authority_status/v1",
        "environment_id": environment_id,
        **worker,
    }
    services = {
        service_id: {
            "schema": "okg.environment_service_authority_status/v1",
            "environment_id": environment_id,
            "service_id": service_id,
            **row,
        }
        for service_id, row in services.items()
    }
    return {
        "schema": "okg.environment_process_authority_status/v1",
        "environment_id": environment_id,
        "declared_services": list(services),
        "ok": False,
        "status": "invalid",
        "worker": worker,
        "services": services,
    }


def _no_process_is_alive(
    authority: dict[str, Any],
    *,
    expected_services: tuple[str, ...] | None = None,
) -> bool:
    return svc._no_process_is_alive(
        authority,
        environment_id="fixture",
        expected_services=(
            tuple(authority.get("declared_services", ()))
            if expected_services is None
            else expected_services
        ),
    )


def _verified_dead_authority() -> dict[str, Any]:
    return _authority(
        {
            "ok": False,
            "status": "invalid",
            "error": "SealedReleaseInvalid",
            "reason_code": "worker_process_not_alive",
            "message": "worker authority process is not alive",
        },
        {
            "svc": {
                "ok": False,
                "status": "invalid",
                "error": "DeploymentConfigError",
                "reason_code": "service_process_not_alive",
                "message": "service authority process is not alive",
            }
        },
    )


def test_verified_dead_complete_inventory_opens_handoff_exception() -> None:
    assert _no_process_is_alive(_verified_dead_authority()) is True


def test_message_only_rows_never_prove_process_death() -> None:
    authority = _verified_dead_authority()
    authority["worker"].pop("reason_code")

    assert _no_process_is_alive(authority) is False


@pytest.mark.parametrize(
    ("row_name", "field", "value"),
    [
        ("worker", "schema", "wrong"),
        ("worker", "environment_id", "wrong"),
        ("worker", "error", "DeploymentConfigError"),
        ("worker", "reason_code", "service_process_not_alive"),
        ("service", "schema", "wrong"),
        ("service", "environment_id", "wrong"),
        ("service", "service_id", "wrong"),
        ("service", "error", "SealedReleaseInvalid"),
        ("service", "reason_code", "worker_process_not_alive"),
    ],
)
def test_fabricated_dead_provenance_refuses(
    row_name: str,
    field: str,
    value: str,
) -> None:
    authority = copy.deepcopy(_verified_dead_authority())
    row = authority["worker"] if row_name == "worker" else authority["services"]["svc"]
    row[field] = value

    assert _no_process_is_alive(authority) is False


def test_omitted_declared_service_refuses() -> None:
    authority = _verified_dead_authority()
    authority["services"] = {}

    assert _no_process_is_alive(authority) is False


def test_omitted_service_and_self_declared_empty_inventory_refuses() -> None:
    authority = _verified_dead_authority()
    authority["declared_services"] = []
    authority["services"] = {}

    assert _no_process_is_alive(authority, expected_services=("svc",)) is False


@pytest.mark.parametrize(
    ("ok", "status"),
    [(True, "healthy"), (False, "healthy"), (True, "invalid")],
)
def test_contradictory_aggregate_status_refuses(ok: bool, status: str) -> None:
    authority = _verified_dead_authority()
    authority["ok"] = ok
    authority["status"] = status

    assert _no_process_is_alive(authority) is False


def test_a_running_process_still_blocks_the_handoff(tmp_path: Path) -> None:
    authority = _authority(
        {
            "ok": False,
            "status": "invalid",
            "message": "worker authority mismatch for definition_digest",
            "path": _receipt(tmp_path, "worker", LIVE_PID),
        },
        {},
    )

    assert _no_process_is_alive(authority) is False


def test_one_live_among_dead_still_blocks(tmp_path: Path) -> None:
    authority = _authority(
        {
            "ok": False,
            "status": "invalid",
            "error": "SealedReleaseInvalid",
            "reason_code": "worker_process_not_alive",
            "message": "worker authority process is not alive",
            "path": _receipt(tmp_path, "worker", DEAD_PID),
        },
        {
            "svc": {
                "ok": False,
                "status": "invalid",
                "path": _receipt(tmp_path, "svc", LIVE_PID),
            }
        },
    )

    assert _no_process_is_alive(authority) is False


def test_a_healthy_row_blocks(tmp_path: Path) -> None:
    authority = _authority(
        {"ok": True, "status": "healthy", "path": _receipt(tmp_path, "w", LIVE_PID)},
        {},
    )

    assert _no_process_is_alive(authority) is False


@pytest.mark.parametrize(
    "row",
    [
        {"ok": False, "status": "missing"},
        {
            "ok": False,
            "status": "invalid",
            "error": "OSError",
            "message": "authority receipt is unreadable",
        },
        {
            "ok": False,
            "status": "invalid",
            "error": "JSONDecodeError",
            "message": "authority receipt is malformed",
        },
        {
            "ok": False,
            "status": "invalid",
            "error": "DeploymentConfigError",
            "message": "service authority pid is invalid",
        },
        {
            "ok": False,
            "status": "invalid",
            "error": "SealedReleaseInvalid",
            "message": "worker authority pid is invalid",
        },
    ],
    ids=("missing", "unreadable", "malformed", "service-invalid-pid", "worker-invalid-pid"),
)
def test_unknown_receipt_classes_do_not_prove_process_death(
    row: dict[str, Any],
) -> None:
    assert _no_process_is_alive(_authority(row, {})) is False


def test_verified_dead_and_unknown_receipt_still_refuses(tmp_path: Path) -> None:
    authority = _authority(
        {
            "ok": False,
            "status": "invalid",
            "message": "worker authority process is not alive",
            "path": _receipt(tmp_path, "worker", DEAD_PID),
        },
        {"svc": {"ok": False, "status": "missing"}},
    )

    assert _no_process_is_alive(authority) is False


def test_no_rows_at_all_is_not_treated_as_dead() -> None:
    assert (
        svc._no_process_is_alive(
            {"ok": False, "worker": None, "services": {}},
            environment_id="fixture",
            expected_services=(),
        )
        is False
    )


def test_the_gate_checks_live_predecessor_against_previous_definition(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    previous_digest = "sha256:" + "1" * 64
    observed: dict[str, Any] = {}

    def process_inventory(
        _environment: object,
        *,
        plan: dict[str, Any],
        current_release: dict[str, Any],
    ) -> dict[str, Any]:
        del plan
        observed.update(current_release)
        return {"ok": True}

    monkeypatch.setattr(svc, "_restart_process_inventory", process_inventory)

    svc._require_verified_previous_process_authority(
        object(),
        plan={
            "release_status": {
                "previous_artifact_id": "sha256:" + "2" * 64,
                "previous_definition_digest": previous_digest,
                "previous_pointer_digest": "sha256:" + "3" * 64,
            }
        },
        current_release={
            "previous_artifact_id": "sha256:" + "2" * 64,
            "previous_definition_digest": previous_digest,
            "previous_pointer_digest": "sha256:" + "3" * 64,
        },
    )

    assert observed["previous_definition_digest"] == previous_digest


def test_the_gate_still_refuses_a_live_predecessor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        svc,
        "_restart_process_inventory",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            DeploymentConfigError("live predecessor authority does not match")
        ),
    )

    with pytest.raises(DeploymentConfigError, match="live predecessor"):
        svc._require_verified_previous_process_authority(
            object(),
            plan={"release_status": {"previous_artifact_id": "x"}},
            current_release={"previous_artifact_id": "x"},
        )


def test_the_gate_refuses_an_entirely_missing_receipt_inventory(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        svc,
        "_restart_process_inventory",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            DeploymentConfigError("unknown or foreign process inventory")
        ),
    )

    with pytest.raises(DeploymentConfigError, match="unknown or foreign"):
        svc._require_verified_previous_process_authority(
            object(),
            plan={"release_status": {"previous_artifact_id": "x"}},
            current_release={"previous_artifact_id": "x"},
        )


def test_inventory_itself_refuses_missing_predecessor_receipts(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    current = SimpleNamespace(
        id="fixture",
        runtime=SimpleNamespace(
            worker_service="worker-current",
            mirror_enabled=False,
            state_root=tmp_path / "current-state",
        ),
        ui=SimpleNamespace(enabled=False),
    )
    previous = SimpleNamespace(
        id="fixture",
        runtime=SimpleNamespace(
            worker_service="worker-previous",
            mirror_enabled=False,
            state_root=tmp_path / "previous-state",
        ),
        ui=SimpleNamespace(enabled=False),
    )
    current_release = {
        "artifact_id": "sha256:" + "a" * 64,
        "previous_artifact_id": "sha256:" + "b" * 64,
    }
    monkeypatch.setattr(
        svc,
        "_previous_release_environment",
        lambda *_args, **_kwargs: (
            {"artifact_id": current_release["previous_artifact_id"]},
            previous,
        ),
    )
    monkeypatch.setattr(
        svc,
        "_service_plan_handoff_ids",
        lambda *_args, **_kwargs: ("worker-current", "worker-previous"),
    )
    monkeypatch.setattr(
        svc,
        "_predecessor_stop_status",
        lambda *_args, **_kwargs: {"ok": False, "status": "missing"},
    )
    monkeypatch.setattr(
        svc,
        "environment_service_authority_status",
        lambda *_args, **_kwargs: {"ok": False, "status": "missing"},
    )
    monkeypatch.setattr(
        svc,
        "_supervisor_process_status",
        lambda *_args, **_kwargs: {
            "ok": True,
            "status": "stopped",
            "pid": None,
        },
    )

    with pytest.raises(DeploymentConfigError, match="unknown or foreign"):
        svc._restart_process_inventory(
            current,
            plan={"plan_digest": "sha256:" + "c" * 64},
            current_release=current_release,
        )


def test_inventory_admits_only_exact_current_release_predecessor_topology_relaunch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    current = SimpleNamespace(
        id="fixture",
        graph_deployment_name="current-deployment",
        runtime=SimpleNamespace(
            worker_service="worker-current",
            mirror_enabled=False,
            state_root=tmp_path / "current-state",
        ),
        ui=SimpleNamespace(enabled=False),
    )
    previous = SimpleNamespace(
        id="fixture",
        graph_deployment_name="previous-deployment",
        runtime=SimpleNamespace(
            worker_service="worker-previous",
            mirror_enabled=False,
            state_root=tmp_path / "previous-state",
        ),
        ui=SimpleNamespace(enabled=False),
    )
    current_release = {
        "artifact_id": "sha256:" + "a" * 64,
        "previous_artifact_id": "sha256:" + "b" * 64,
    }
    previous_release = {"artifact_id": current_release["previous_artifact_id"]}
    identity = {"pid": 424_242, "process_start_identity": "boot:straddled"}
    authority = {**identity, "schema": "okg.service_release_authority/v1"}
    exact = {"value": True}
    monkeypatch.setattr(
        svc,
        "_previous_release_environment",
        lambda *_args, **_kwargs: (previous_release, previous),
    )
    monkeypatch.setattr(
        svc,
        "_service_plan_handoff_ids",
        lambda *_args, **_kwargs: ("worker-current", "worker-previous"),
    )
    monkeypatch.setattr(
        svc,
        "_predecessor_stop_status",
        lambda *_args, **_kwargs: {"ok": False, "status": "missing"},
    )

    def service_status(candidate, *, service_id, release_status, **_kwargs):
        if service_id == "worker-current":
            return {"ok": False, "status": "missing"}
        if candidate is previous and release_status is current_release and exact["value"]:
            return {
                "ok": True,
                "status": "healthy",
                "authority": authority,
                "supervisor": {"ok": True, "status": "running", "pid": identity["pid"]},
            }
        return {"ok": False, "status": "invalid", "message": "mixed authority"}

    monkeypatch.setattr(svc, "environment_service_authority_status", service_status)
    monkeypatch.setattr(
        svc,
        "_supervisor_process_status",
        lambda *_args, **_kwargs: {"ok": True, "status": "stopped", "pid": None},
    )
    monkeypatch.setattr(
        svc,
        "environment_worker_authority_status",
        lambda candidate, *, release_status, expected_pid=None, **_kwargs: (
            {
                "ok": True,
                "status": "healthy",
                "authority": {
                    **identity,
                    "schema": "okg.worker_release_authority/v1",
                },
            }
            if candidate is previous
            and release_status is current_release
            and expected_pid == identity["pid"]
            and exact["value"]
            else {"ok": False, "status": "missing"}
        ),
    )

    inventory = svc._restart_process_inventory(
        current,
        plan={"plan_digest": "sha256:" + "c" * 64},
        current_release=current_release,
    )
    assert inventory["services"]["worker-previous"]["generation"] == "straddled"
    assert inventory["worker"]["generation"] == "straddled"

    exact["value"] = False
    with pytest.raises(DeploymentConfigError, match="unknown or foreign"):
        svc._restart_process_inventory(
            current,
            plan={"plan_digest": "sha256:" + "c" * 64},
            current_release=current_release,
        )
