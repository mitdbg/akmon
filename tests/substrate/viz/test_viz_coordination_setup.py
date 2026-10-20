from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
from fastapi.testclient import TestClient  # noqa: E402

from okg.viz.app import build_app  # noqa: E402
from okg.viz.auth import OPERATOR_TOKEN_HEADER  # noqa: E402
from okg.viz import coordination_services  # noqa: E402
from okg.viz.coordination_services import SetupPlanRequest  # noqa: E402
from okg.viz.operator_control import OperatorControlRequest  # noqa: E402

from tests.substrate.viz.neutral_routes import (  # noqa: E402
    REMOTE_ROUTES,
    ROUTE_NAMES,
    use_neutral_route_policy,
)


TOKEN = "setup-test-token"
ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture(autouse=True)
def _neutral_route_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    use_neutral_route_policy(monkeypatch, ROOT)


class _Probes:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.ledger_empty = True
        self.capacity_state: dict[str, Any] = {"ok": True}

    def _read(self, name: str, value: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(name)
        return value

    def git(self) -> dict[str, Any]:
        return self._read(
            "git",
            {
                "git_ready": True,
                "git_binding": {
                    "branch": "coord/chief",
                    "base_commit": "a" * 40,
                    "head_commit": "b" * 40,
                    "worktree": "/redacted/worktree",
                },
            },
        )

    def pact(self) -> dict[str, Any]:
        return self._read(
            "pact",
            {
                "pact_binding": {
                    "change_id": "agent-coordination-operations",
                    "task_id": "task.coord-operations.setup-start",
                    "intent_digest": "sha256:" + "1" * 64,
                },
            },
        )

    def deployment(self) -> dict[str, Any]:
        return self._read(
            "deployment",
            {"deployment_exists": True, "deployment_ready": True},
        )

    def graph(self) -> dict[str, Any]:
        return self._read(
            "graph",
            {
                "okg_ready": True,
                "graph_binding": {
                    "generation": "generation:1",
                    "branch": "coord/chief",
                },
            },
        )

    def native_control(self) -> dict[str, Any]:
        return self._read(
            "native_control",
            {
                "ledger_empty": self.ledger_empty,
                "operator_control_available": True,
                "identity_ready": True,
            },
        )

    def capacity(self) -> dict[str, Any]:
        return self._read("capacity", {"capacity": dict(self.capacity_state)})

    def runtime(self) -> dict[str, Any]:
        return self._read(
            "runtime",
            {
                "configuration_collisions": [],
                "credential_references": {
                    shape: "${OKG_AGENT_SSH_GRANT}"
                    for shape in ROUTE_NAMES
                },
                "runtime_binding": {
                    "route_ref": "opaque:local-codex",
                    "config_digest": "sha256:" + "2" * 64,
                },
            },
        )

    def route(self) -> dict[str, Any]:
        return self._read(
            "route",
            {
                "local_only": True,
                "host_reachability": {name: True for name in ROUTE_NAMES},
            },
        )


class _Connection:
    def __enter__(self) -> "_Connection":
        return self

    def __exit__(self, *_args: object) -> bool:
        return False


class _HttpResponse:
    def __init__(self, payload: object, status: int = 200) -> None:
        self.payload = payload
        self.status = status

    def getcode(self) -> int:
        return self.status

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")


class _Lifecycle:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.fail_stage = False
        self.fail_activate = False

    def stage(self, request: Any) -> dict[str, Any]:
        if self.fail_stage:
            self.fail_stage = False
            raise RuntimeError("simulated stage interruption")
        self.events.append("stage")
        return {
            "assignment_id": request.assignment_id,
            "assignment_generation": 1,
            "lifecycle_status": "staged",
            "worktree_path": "/redacted/worktree",
            "git": {"branch": "coord/chief", "worktree_path": "/redacted/worktree"},
            "graph": {"branch": "coord/chief", "deployment": "demo"},
        }

    def activate(self, receipt: Any) -> dict[str, Any]:
        if self.fail_activate:
            self.fail_activate = False
            raise RuntimeError("simulated activation interruption")
        self.events.append("activate")
        return {**receipt, "lifecycle_status": "active"}

    def retire(self, receipt: Any, *, disposition: str) -> dict[str, Any]:
        self.events.append(f"retire:{disposition}")
        return {**receipt, "lifecycle_status": "retired"}

    def cleanup(self, receipt: Any) -> SimpleNamespace:
        self.events.append("cleanup")
        return SimpleNamespace(complete=True)


class _Native:
    OPERATOR_CONTROL_ACTOR = "operator:viz-token"
    OPERATOR_CONTROL_SESSION = "operator-control"
    state_reads = 0
    prepare_calls: list[dict[str, Any]] = []
    pair_proof_calls: list[dict[str, Any]] = []
    complete_calls: list[dict[str, Any]] = []
    control: dict[str, Any] = {}
    complete_ok = True
    complete_error: Exception | None = None

    @classmethod
    def reset(cls) -> None:
        cls.state_reads = 0
        cls.prepare_calls = []
        cls.pair_proof_calls = []
        cls.complete_calls = []
        cls.control = {"deployment_name": "demo", "control_mode": "uninitialized"}
        cls.complete_ok = True
        cls.complete_error = None

    @staticmethod
    def ensure_native_coordination_schema(_conn: Any) -> None:
        return None

    @classmethod
    def read_control_state(cls, _conn: Any, *, deployment_name: str) -> dict[str, Any]:
        cls.state_reads += 1
        return {"deployment_name": deployment_name, **cls.control}

    @classmethod
    def prepare_bootstrap_control(cls, _conn: Any, **kwargs: Any) -> dict[str, Any]:
        cls.prepare_calls.append(kwargs)
        if cls.control.get("control_mode") == "uninitialized":
            cls.control = {
                "deployment_name": kwargs["deployment_name"],
                "control_mode": "preparing",
                "control_generation": 0,
                "approved_plan_digest": kwargs["plan_digest"],
                "policy_digest": kwargs["policy_digest"],
                "project_digest": kwargs["project_digest"],
                "last_idempotency_key": kwargs["idempotency_key"],
                "bootstrap_pair_proof": None,
                "last_receipt": {
                    "ok": True,
                    "action": "bootstrap",
                    "phase": "prepare",
                    "complete": False,
                    "deployment_name": kwargs["deployment_name"],
                    "control_mode": "preparing",
                    "control_generation": 0,
                    "plan_digest": kwargs["plan_digest"],
                    "policy_digest": kwargs["policy_digest"],
                    "project_digest": kwargs["project_digest"],
                },
            }
        return dict(cls.control["last_receipt"])

    @classmethod
    def record_bootstrap_pair_proof(cls, _conn: Any, **kwargs: Any) -> dict[str, Any]:
        cls.pair_proof_calls.append(kwargs)
        proof = dict(kwargs["pair_proof"])
        existing = cls.control.get("bootstrap_pair_proof")
        if existing is not None and existing != proof:
            raise RuntimeError("pair proof conflict")
        cls.control["bootstrap_pair_proof"] = proof
        receipt = {
            "ok": True,
            "action": "bootstrap",
            "phase": "pair_proof",
            "complete": False,
            "deployment_name": kwargs["deployment_name"],
            "control_mode": "preparing",
            "control_generation": 0,
            "plan_digest": kwargs["plan_digest"],
            "policy_digest": kwargs["policy_digest"],
            "project_digest": kwargs["project_digest"],
            "assignment_id": proof["assignment_id"],
            "assignment_generation": proof["assignment_generation"],
            "pair_proof": proof,
        }
        cls.control["last_receipt"] = receipt
        return receipt

    @classmethod
    def complete_bootstrap_control(cls, _conn: Any, **kwargs: Any) -> dict[str, Any]:
        cls.complete_calls.append(kwargs)
        if cls.complete_error is not None:
            error = cls.complete_error
            cls.complete_error = None
            raise error
        proof = dict(kwargs["pair_proof"])
        result = {
            "ok": cls.complete_ok,
            "action": "bootstrap",
            "deployment_name": kwargs["deployment_name"],
            "control_mode": "open" if cls.complete_ok else "preparing",
            "control_generation": 1 if cls.complete_ok else 0,
            "assignment_id": proof["assignment_id"],
            "assignment_generation": proof["assignment_generation"],
            "plan_digest": kwargs["plan_digest"],
            "policy_digest": kwargs["policy_digest"],
            "project_digest": kwargs["project_digest"],
            "pair_proof": proof,
            "audit_event_id": "event:1",
            "complete": cls.complete_ok,
        }
        cls.control["last_receipt"] = result
        if cls.complete_ok:
            cls.control.update(
                {
                    "control_mode": "open",
                    "control_generation": 1,
                }
            )
        return result

    # Kept so tests of the legacy loader can still use this fake module.  The
    # setup service must never call this path.
    @classmethod
    def bootstrap_control(cls, _conn: Any, **kwargs: Any) -> dict[str, Any]:
        return cls.complete_bootstrap_control(_conn, **kwargs)

    @staticmethod
    def fallback_control(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {"ok": True, "action": "fallback"}

    @staticmethod
    def reactivate_control(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {"ok": True, "action": "reactivation"}


class _Approvals:
    @staticmethod
    def decide_native_approval(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {"ok": True}

    @staticmethod
    def revoke_native_approval(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {"ok": True}


def _chief_resolver(**_: Any) -> Any:
    return SimpleNamespace(
        assignment_id="assignment:chief",
        assignment_generation=1,
        authenticated_host="localhost",
        owner_principal="chief:server",
        owner_session_id="session:server",
        git_worktree_id="/redacted/worktree",
        okg_branch_id="coord/chief",
    )


def _pair_request_factory(*, chief_request: Any, **_: Any) -> Any:
    return SimpleNamespace(
        assignment_id=chief_request.assignment_id,
        assignment_generation=chief_request.assignment_generation,
        authenticated_host=chief_request.authenticated_host,
    )


def _new_services(probes: _Probes, lifecycle: _Lifecycle):
    return coordination_services.build_native_coordination_services(
        dsn="postgresql://not-returned",
        deployment_name="demo",
        repo_root=ROOT,
        setup_probes=probes,
        chief_resolver=_chief_resolver,
        pair_request_factory=_pair_request_factory,
        lifecycle_factory=lambda _request: lifecycle,
    )


@pytest.fixture
def setup_components(monkeypatch: pytest.MonkeyPatch):
    _Native.reset()
    monkeypatch.setattr(
        coordination_services,
        "_load_native_modules",
        lambda _deployment: (_Native, _Approvals),
    )
    monkeypatch.setattr(
        coordination_services,
        "db_connect",
        lambda *_args, **_kwargs: _Connection(),
    )
    probes = _Probes()
    events: list[str] = []
    lifecycle = _Lifecycle(events)
    services = _new_services(probes, lifecycle)
    return services, probes, events, lifecycle


def test_plan_route_is_authenticated_zero_write_and_authority_probed(setup_components):
    services, probes, _events, _lifecycle = setup_components
    app = build_app(
        lambda: SimpleNamespace(deployment_name="demo"),
        is_ready=lambda: True,
        operator_token=TOKEN,
        setup_plan_service=services.setup_plan_service,
    )
    client = TestClient(app)
    path = "/viz/api/coordination/setup/plan"
    assert client.post(path, json={}, headers={OPERATOR_TOKEN_HEADER: "wrong"}).status_code == 401
    response = client.post(path, json={}, headers={OPERATOR_TOKEN_HEADER: TOKEN})
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["schema"] == "okg.coordination.setup-plan.v1"
    assert payload["plan_digest"].startswith("sha256:")
    assert payload["human_policy_digest"].startswith("sha256:")
    assert "plan" not in payload
    assert probes.calls == [
        "git", "pact", "deployment", "graph", "native_control",
        "capacity", "runtime", "route",
    ]
    assert path not in app.openapi()["paths"]
    # The empty body is the only valid body; identity cannot be smuggled in.
    response = client.post(
        path,
        json={"owner_principal": "attacker"},
        headers={OPERATOR_TOKEN_HEADER: TOKEN},
    )
    assert response.status_code == 422


def test_capacity_probe_is_bound_to_the_canonical_plan_digest(setup_components):
    services, probes, _events, _lifecycle = setup_components
    first_payload = services.setup_plan_service(
        principal="operator:viz-token",
        request=SetupPlanRequest(deployment="demo"),
    )

    def changed_capacity() -> dict[str, Any]:
        return {"capacity": {"ok": True, "shared_storage": 99}}

    probes.capacity = changed_capacity  # type: ignore[method-assign]
    second_payload = services.setup_plan_service(
        principal="operator:viz-token",
        request=SetupPlanRequest(deployment="demo"),
    )
    assert first_payload["capacity_digest"] != second_payload["capacity_digest"]
    assert first_payload["plan_digest"] != second_payload["plan_digest"]


@pytest.mark.parametrize(
    ("blocker", "expected_blocker"),
    [
        ("host", "host_unavailable"),
        ("configuration", "configuration_collision"),
        ("capacity", "capacity_insufficient"),
    ],
)
def test_apply_rejects_blocked_exact_plan_before_native_prepare(
    setup_components,
    blocker: str,
    expected_blocker: str,
):
    services, probes, _events, _lifecycle = setup_components
    if blocker == "host":
        original_route = probes.route

        def blocked_route() -> dict[str, Any]:
            value = original_route()
            value["host_reachability"][REMOTE_ROUTES[0]] = False
            return value

        probes.route = blocked_route  # type: ignore[method-assign]
    elif blocker == "configuration":
        original_runtime = probes.runtime

        def blocked_runtime() -> dict[str, Any]:
            value = original_runtime()
            value["configuration_collisions"] = ["target collision"]
            return value

        probes.runtime = blocked_runtime  # type: ignore[method-assign]
    else:
        original_capacity = probes.capacity

        def blocked_capacity() -> dict[str, Any]:
            value = original_capacity()
            value["capacity"]["ok"] = False
            return value

        probes.capacity = blocked_capacity  # type: ignore[method-assign]

    plan_receipt = services.setup_plan_service(
        principal="operator:viz-token", request=SetupPlanRequest(deployment="demo")
    )
    from okg.substrate.pact.coordination_operations import SetupPlan

    plan = SetupPlan.from_json(plan_receipt, repo_root=ROOT)
    assert plan.ready is False
    assert expected_blocker in plan.blocked_by

    with pytest.raises(
        coordination_services.SetupAuthorityUnavailable,
        match="current setup plan",
    ):
        services.operator_control_service(
            principal="operator:viz-token",
            request=_bootstrap_request(plan, key=f"setup:blocked-{blocker}"),
        )
    assert _Native.prepare_calls == []


def test_apply_reprobes_then_stages_activates_and_bootstraps(setup_components):
    services, probes, events, _lifecycle = setup_components
    plan_receipt = services.setup_plan_service(
        principal="operator:viz-token", request=SetupPlanRequest(deployment="demo")
    )
    from okg.substrate.pact.coordination_operations import SetupPlan

    plan = SetupPlan.from_json(plan_receipt, repo_root=ROOT)
    request = OperatorControlRequest(
        operation="bootstrap",
        structured_plan=plan._unsigned_payload(),
        claimed_plan_digest=plan.plan_digest,
        expected_policy_digest=plan.policy_digest,
        expected_project_digest=plan.project_digest,
        idempotency_key="setup:1",
    )
    result = services.operator_control_service(
        principal="operator:viz-token", request=request
    )
    assert result["complete"] is True
    assert result["pair"]["lifecycle_status"] == "active"
    assert result["pair"]["git_worktree_id"].startswith("opaque:worktree:")
    assert result["pair"]["git_worktree_id"] != "/redacted/worktree"
    assert result["pair"]["okg_branch_id"] == "coord/chief"
    assert "authenticated_host" not in result["pair"]
    assert "/redacted/worktree" not in json.dumps(result)
    assert events == ["stage", "activate"]
    assert [call["chief_request"].owner_principal for call in _Native.complete_calls] == [
        "chief:server"
    ]
    assert len(_Native.prepare_calls) == 1
    assert len(_Native.pair_proof_calls) == 1
    assert (
        _Native.pair_proof_calls[0]["pair_proof"]["git_worktree_id"]
        == "/redacted/worktree"
    )
    assert (
        _Native.complete_calls[0]["pair_proof"]["git_worktree_id"]
        == "/redacted/worktree"
    )
    assert probes.calls.count("git") == 2

    duplicate = services.operator_control_service(
        principal="operator:viz-token", request=request
    )
    assert duplicate == result
    assert events == ["stage", "activate"]
    # The first apply reads before and after prepare. The exact open retry
    # needs one durable-state read and no process-local cache or reprobe.
    assert _Native.state_reads == 3


def test_apply_closes_an_owned_production_lifecycle(setup_components):
    _services, probes, _events, _lifecycle = setup_components

    class _ClosingLifecycle(_Lifecycle):
        def __init__(self) -> None:
            super().__init__([])
            self.close_calls = 0

        def close(self) -> None:
            self.close_calls += 1

    lifecycle = _ClosingLifecycle()
    services = _new_services(probes, lifecycle)
    plan_receipt = services.setup_plan_service(
        principal="operator:viz-token", request=SetupPlanRequest(deployment="demo")
    )
    from okg.substrate.pact.coordination_operations import SetupPlan

    plan = SetupPlan.from_json(plan_receipt, repo_root=ROOT)
    result = services.operator_control_service(
        principal="operator:viz-token",
        request=_bootstrap_request(plan, key="setup:owned-lifecycle-close"),
    )

    assert result["complete"] is True
    assert lifecycle.close_calls == 1


def _bootstrap_request(plan: Any, *, key: str) -> OperatorControlRequest:
    return OperatorControlRequest(
        operation="bootstrap",
        structured_plan=plan._unsigned_payload(),
        claimed_plan_digest=plan.plan_digest,
        expected_policy_digest=plan.policy_digest,
        expected_project_digest=plan.project_digest,
        idempotency_key=key,
    )


def test_restart_after_prepare_stages_once_and_completes(setup_components):
    services, probes, events, lifecycle = setup_components
    plan_receipt = services.setup_plan_service(
        principal="operator:viz-token", request=SetupPlanRequest(deployment="demo")
    )
    from okg.substrate.pact.coordination_operations import SetupPlan

    plan = SetupPlan.from_json(plan_receipt, repo_root=ROOT)
    request = _bootstrap_request(plan, key="setup:restart-after-prepare")
    lifecycle.fail_stage = True
    with pytest.raises(Exception, match="local setup pair staging failed"):
        services.operator_control_service(
            principal="operator:viz-token", request=request
        )
    assert _Native.control["control_mode"] == "preparing"
    assert _Native.control["bootstrap_pair_proof"] is None
    assert events == []

    # A new service closure has no in-memory handoff.  Durable preparation is
    # enough to safely retry the deterministic stage/activate operation.
    restarted_events: list[str] = []
    restarted = _new_services(probes, _Lifecycle(restarted_events))
    result = restarted.operator_control_service(
        principal="operator:viz-token", request=request
    )
    assert result["complete"] is True
    assert restarted_events == ["stage", "activate"]
    assert len(_Native.prepare_calls) == 2
    assert len(_Native.pair_proof_calls) == 1


def test_prepared_retry_adopts_only_its_new_pair_collisions(
    setup_components,
    monkeypatch: pytest.MonkeyPatch,
):
    _services, probes, events, _lifecycle = setup_components

    class _RecoverableLifecycle(_Lifecycle):
        def __init__(self, event_log: list[str]) -> None:
            super().__init__(event_log)
            self.current: dict[str, Any] | None = None

        def stage(self, request: Any) -> dict[str, Any]:
            self.current = super().stage(request)
            return self.current

        def stage_or_adopt(self, request: Any) -> dict[str, Any]:
            if self.current is None:
                return self.stage(request)
            assert request.assignment_id == self.current["assignment_id"]
            self.events.append("adopt")
            return self.current

        def activate(self, receipt: Any) -> dict[str, Any]:
            if receipt.get("lifecycle_status") == "active":
                return receipt
            self.current = super().activate(receipt)
            return self.current

    lifecycle = _RecoverableLifecycle(events)
    services = _new_services(probes, lifecycle)
    plan_receipt = services.setup_plan_service(
        principal="operator:viz-token", request=SetupPlanRequest(deployment="demo")
    )
    from okg.substrate.pact.coordination_operations import SetupPlan

    plan = SetupPlan.from_json(plan_receipt, repo_root=ROOT)
    request = _bootstrap_request(plan, key="setup:adopt-prepared-pair")
    original_record = _Native.record_bootstrap_pair_proof

    def disconnect_before_proof(cls, _connection: Any, **_kwargs: Any) -> dict[str, Any]:
        del cls
        raise OSError("simulated disconnect before durable pair proof")

    monkeypatch.setattr(
        _Native,
        "record_bootstrap_pair_proof",
        classmethod(disconnect_before_proof),
    )
    with pytest.raises(
        coordination_services.SetupAuthorityUnavailable,
        match="local setup apply is unavailable",
    ):
        services.operator_control_service(
            principal="operator:viz-token", request=request
        )
    assert _Native.control["control_mode"] == "preparing"
    assert _Native.control["bootstrap_pair_proof"] is None
    assert events == ["stage", "activate"]

    original_git = probes.git

    def owned_pair_now_exists() -> dict[str, Any]:
        fragment = original_git()
        fragment["configuration_collisions"] = [
            "git_target_branch_exists",
            "git_target_worktree_exists",
            "graph_target_branch_exists",
        ]
        return fragment

    probes.git = owned_pair_now_exists  # type: ignore[method-assign]
    monkeypatch.setattr(_Native, "record_bootstrap_pair_proof", original_record)
    result = services.operator_control_service(
        principal="operator:viz-token", request=request
    )

    assert result["complete"] is True
    assert events == ["stage", "activate", "adopt"]
    assert len(_Native.pair_proof_calls) == 1


def test_restart_after_pair_proof_does_not_restage(setup_components):
    services, probes, _events, lifecycle = setup_components
    plan_receipt = services.setup_plan_service(
        principal="operator:viz-token", request=SetupPlanRequest(deployment="demo")
    )
    from okg.substrate.pact.coordination_operations import SetupPlan

    plan = SetupPlan.from_json(plan_receipt, repo_root=ROOT)
    request = _bootstrap_request(plan, key="setup:restart-after-proof")
    _Native.complete_error = RuntimeError("simulated process crash after proof")
    with pytest.raises(Exception, match="local setup apply is unavailable"):
        services.operator_control_service(
            principal="operator:viz-token", request=request
        )
    assert _Native.control["control_mode"] == "preparing"
    assert _Native.control["bootstrap_pair_proof"] is not None
    assert lifecycle.events == ["stage", "activate"]

    restarted_events: list[str] = []
    restarted = _new_services(probes, _Lifecycle(restarted_events))
    result = restarted.operator_control_service(
        principal="operator:viz-token", request=request
    )
    assert result["complete"] is True
    assert restarted_events == []
    assert len(_Native.pair_proof_calls) == 1
    assert len(_Native.complete_calls) == 2


def test_open_retry_returns_durable_receipt_without_staging(setup_components):
    services, probes, events, _lifecycle = setup_components
    plan_receipt = services.setup_plan_service(
        principal="operator:viz-token", request=SetupPlanRequest(deployment="demo")
    )
    from okg.substrate.pact.coordination_operations import SetupPlan

    plan = SetupPlan.from_json(plan_receipt, repo_root=ROOT)
    request = _bootstrap_request(plan, key="setup:open-retry")
    result = services.operator_control_service(
        principal="operator:viz-token", request=request
    )
    # Successful bootstrap necessarily creates a chief and consumes capacity.
    # Those expected effects make a fresh initial-readiness plan differ, but
    # an exact same-key request must still replay the durable receipt without
    # probing or mutating again.
    probes.ledger_empty = False
    probes.capacity_state = {"ok": True, "lane_usage": {"critical": 1}}
    probe_calls = list(probes.calls)
    restarted_events: list[str] = []
    restarted = _new_services(probes, _Lifecycle(restarted_events))
    retry = restarted.operator_control_service(
        principal="operator:viz-token", request=request
    )
    assert retry == result
    assert restarted_events == []
    assert events == ["stage", "activate"]
    assert probes.calls == probe_calls

    tampered = OperatorControlRequest(
        operation="bootstrap",
        structured_plan={**request.structured_plan, "next_action": "tampered"},
        claimed_plan_digest=request.claimed_plan_digest,
        expected_policy_digest=request.expected_policy_digest,
        expected_project_digest=request.expected_project_digest,
        idempotency_key=request.idempotency_key,
    )
    with pytest.raises(Exception, match="digest is stale"):
        restarted.operator_control_service(
            principal="operator:viz-token", request=tampered
        )


def test_retry_conflict_is_rejected_before_any_pair_stage(setup_components):
    services, probes, events, _lifecycle = setup_components
    plan_receipt = services.setup_plan_service(
        principal="operator:viz-token", request=SetupPlanRequest(deployment="demo")
    )
    from okg.substrate.pact.coordination_operations import SetupPlan

    plan = SetupPlan.from_json(plan_receipt, repo_root=ROOT)
    request = _bootstrap_request(plan, key="setup:conflict")
    services.operator_control_service(principal="operator:viz-token", request=request)
    conflicting = _bootstrap_request(plan, key="setup:other-idempotency")
    with pytest.raises(Exception, match="idempotency"):
        services.operator_control_service(
            principal="operator:viz-token", request=conflicting
        )
    assert events == ["stage", "activate"]


def test_client_planner_and_bootstrap_share_one_unsigned_digest(setup_components):
    """The real transport round-trip must feed SetupPlan.from_json exactly."""

    services, _probes, _events, _lifecycle = setup_components
    app = build_app(
        lambda: SimpleNamespace(deployment_name="demo"),
        is_ready=lambda: True,
        operator_token=TOKEN,
        operator_control_service=services.operator_control_service,
        setup_plan_service=services.setup_plan_service,
    )
    api = TestClient(app)

    class _FastApiOpener:
        def open(self, request: Any, *, timeout: float) -> Any:
            del timeout
            response = api.post(
                request.full_url,
                content=request.data,
                headers={
                    "x-okg-operator-token": request.get_header("X-okg-operator-token") or "",
                    "Content-Type": request.get_header("Content-Type") or "application/json",
                },
            )
            return _HttpResponse(response.json(), response.status_code)

    from okg.substrate.pact.coordination_operations import (
        SetupPlan,
        VizCoordinationClient,
    )

    client = VizCoordinationClient(
        environment={
            "OKG_VIZ_URL": "https://viz.example.invalid",
            "OKG_VIZ_OPERATOR_TOKEN": TOKEN,
        },
        opener=_FastApiOpener(),
    )
    wire = client.setup_plan("demo")
    plan = SetupPlan.from_json(wire, repo_root=ROOT)
    assert wire == plan.to_json()

    receipt = client.apply_setup(plan)
    assert receipt["plan_digest"] == plan.plan_digest
    assert receipt["complete"] is True
    submitted = _Native.complete_calls[-1]
    assert submitted["plan"] == plan._unsigned_payload()
    assert "plan_digest" not in submitted["plan"]
    encoded = json.dumps(
        submitted["plan"], sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    assert "sha256:" + hashlib.sha256(encoded).hexdigest() == plan.plan_digest
    assert "postgresql://" not in json.dumps(submitted["plan"])


def test_native_bootstrap_failure_cleans_only_owned_pair(setup_components):
    services, _probes, events, _lifecycle = setup_components
    _Native.complete_ok = False
    plan_receipt = services.setup_plan_service(
        principal="operator:viz-token", request=SetupPlanRequest(deployment="demo")
    )
    from okg.substrate.pact.coordination_operations import SetupPlan

    plan = SetupPlan.from_json(plan_receipt, repo_root=ROOT)
    from okg.viz.operator_control import OperatorControlRequest

    request = OperatorControlRequest(
        operation="bootstrap",
        structured_plan=plan._unsigned_payload(),
        claimed_plan_digest=plan.plan_digest,
        expected_policy_digest=plan.policy_digest,
        expected_project_digest=plan.project_digest,
        idempotency_key="setup:failure",
    )
    with pytest.raises(Exception):
        services.operator_control_service(
            principal="operator:viz-token", request=request
        )
    # The proof is durable before completion.  Keep the active pair for a
    # restart rather than deleting the object behind the native ledger.
    assert events == ["stage", "activate"]
    assert _Native.control["bootstrap_pair_proof"]["assignment_id"] == "assignment:chief"


def test_ambiguous_pair_proof_commit_preserves_pair_for_retry(
    setup_components,
    monkeypatch: pytest.MonkeyPatch,
):
    services, _probes, events, _lifecycle = setup_components
    committed_record = _Native.record_bootstrap_pair_proof

    def commit_then_disconnect(cls, connection: Any, **kwargs: Any) -> dict[str, Any]:
        del cls
        committed_record(connection, **kwargs)
        raise OSError("simulated disconnect after durable proof")

    monkeypatch.setattr(
        _Native,
        "record_bootstrap_pair_proof",
        classmethod(commit_then_disconnect),
    )
    plan_receipt = services.setup_plan_service(
        principal="operator:viz-token", request=SetupPlanRequest(deployment="demo")
    )
    from okg.substrate.pact.coordination_operations import SetupPlan

    plan = SetupPlan.from_json(plan_receipt, repo_root=ROOT)
    request = OperatorControlRequest(
        operation="bootstrap",
        structured_plan=plan._unsigned_payload(),
        claimed_plan_digest=plan.plan_digest,
        expected_policy_digest=plan.policy_digest,
        expected_project_digest=plan.project_digest,
        idempotency_key="setup:ambiguous-proof",
    )
    with pytest.raises(Exception):
        services.operator_control_service(
            principal="operator:viz-token", request=request
        )
    assert events == ["stage", "activate"]
    assert _Native.control["bootstrap_pair_proof"]["assignment_id"] == "assignment:chief"


def test_partial_production_setup_injection_fails_closed(setup_components):
    _services, probes, _events, _lifecycle = setup_components
    services = coordination_services.build_native_coordination_services(
        dsn="postgresql://not-returned",
        deployment_name="demo",
        setup_probes=probes,
    )
    assert services.setup_plan_service is not None
    from okg.viz.operator_control import OperatorControlRequest

    request = OperatorControlRequest(
        operation="bootstrap",
        structured_plan={"legacy": "not-a-canonical-setup-plan"},
        claimed_plan_digest="sha256:does-not-matter",
        expected_policy_digest="sha256:policy",
        expected_project_digest="sha256:project",
        idempotency_key="setup:partial",
    )
    with pytest.raises(Exception, match="server-owned chief"):
        services.operator_control_service(
            principal="operator:viz-token", request=request
        )


def test_bootstrap_without_setup_adapters_never_uses_native_only_path(
    setup_components,
):
    del setup_components
    _Native.reset()
    services = coordination_services.build_native_coordination_services(
        dsn="postgresql://not-returned",
        deployment_name="demo",
    )
    from okg.viz.operator_control import OperatorControlRequest

    request = OperatorControlRequest(
        operation="bootstrap",
        structured_plan={"metadata": "not-a-setup-plan"},
        claimed_plan_digest="sha256:" + "0" * 64,
        expected_policy_digest="sha256:" + "1" * 64,
        expected_project_digest="sha256:" + "2" * 64,
        idempotency_key="setup:no-adapters",
    )
    with pytest.raises(Exception, match="server-owned chief"):
        services.operator_control_service(
            principal="operator:viz-token", request=request
        )
    assert _Native.complete_calls == []
