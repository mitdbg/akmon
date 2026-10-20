"""Dedicated authentication boundary for coordination approvals."""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
from fastapi.routing import APIRoute  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from okg.viz.app import build_app  # noqa: E402
from okg.viz.auth import (  # noqa: E402
    OPERATOR_APPROVAL_PATHS,
    OPERATOR_PRINCIPAL,
    OPERATOR_SESSION_ID,
    OPERATOR_TOKEN_HEADER,
    is_mutating_route,
    is_operator_approval_route,
)
from okg.viz.approval_control import (  # noqa: E402
    ApprovalControlRequest,
    ApprovalDecisionRequest,
    ApprovalRevokeRequest,
)
from okg.viz import coordination_services  # noqa: E402
from okg.viz.operator_control import OperatorControlRequest  # noqa: E402


TOKEN = "approval-route-token"
DSN = "postgresql://must-not-appear.example/secret"


class _Ctx:
    deployment_name = "demo"


class _Service:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, ApprovalControlRequest]] = []

    def __call__(
        self,
        *,
        principal: str,
        session_id: str,
        request: ApprovalControlRequest,
    ) -> dict[str, Any]:
        self.calls.append((principal, session_id, request))
        return {
            "ok": True,
            "status": "granted" if getattr(request, "decision", None) else "revoked",
            "request_digest": request.request_digest,
        }


def _app(*, service: _Service | None = None, token: str | None = TOKEN):
    return build_app(
        ctx_provider=_Ctx,
        is_ready=lambda: True,
        operator_token=token,
        approval_control_service=service,
    )


def _path(operation: str) -> str:
    return f"/viz/api/coordination/approval/{operation}"


def _decision_body() -> dict[str, str]:
    return {"request_digest": "sha256:request", "decision": "approve"}


def test_approval_surface_is_exactly_two_routes_and_not_generic_or_openapi() -> None:
    app = _app()
    routes = {
        route.path
        for route in app.routes
        if isinstance(route, APIRoute)
        and route.methods
        and "POST" in route.methods
        and "/coordination/approval/" in route.path
    }
    assert routes == set(OPERATOR_APPROVAL_PATHS)
    assert routes == {_path("decide"), _path("revoke")}
    assert all(is_mutating_route("POST", path) for path in routes)
    assert all(is_operator_approval_route("POST", path) for path in routes)
    assert not is_operator_approval_route("POST", _path("other"))
    assert all(path not in app.openapi()["paths"] for path in routes)


def test_missing_token_is_rejected_before_body_validation_or_service() -> None:
    service = _Service()
    response = TestClient(_app(service=service)).post(
        _path("decide"),
        json={"actor": "attacker", "dsn": DSN},
    )
    assert response.status_code == 401
    assert service.calls == []
    assert TOKEN not in response.text
    assert DSN not in response.text


def test_body_identity_and_credentials_are_rejected_without_side_effect() -> None:
    service = _Service()
    client = TestClient(_app(service=service))
    body = _decision_body()
    body.update(
        {
            "principal": "operator:attacker",
            "session": "laundered-session",
            "action": "merge",
            "resource": DSN,
            "scope": "all",
            "assignment": "assignment:attacker",
            "token": TOKEN,
        }
    )
    response = client.post(
        _path("decide"),
        headers={OPERATOR_TOKEN_HEADER: TOKEN},
        json=body,
    )
    assert response.status_code == 422
    assert service.calls == []
    assert DSN not in response.text
    assert TOKEN not in response.text


def test_server_stamps_stable_operator_subject_and_session() -> None:
    service = _Service()
    client = TestClient(_app(service=service))
    response = client.post(
        _path("decide"),
        headers={OPERATOR_TOKEN_HEADER: TOKEN},
        json=_decision_body(),
    )
    assert response.status_code == 200, response.text
    assert service.calls[0][0:2] == (OPERATOR_PRINCIPAL, OPERATOR_SESSION_ID)
    assert isinstance(service.calls[0][2], ApprovalDecisionRequest)
    assert not hasattr(service.calls[0][2], "principal")
    assert not hasattr(service.calls[0][2], "action")

    response = client.post(
        _path("revoke"),
        headers={OPERATOR_TOKEN_HEADER: TOKEN},
        json={"request_digest": "sha256:request"},
    )
    assert response.status_code == 200, response.text
    assert isinstance(service.calls[1][2], ApprovalRevokeRequest)


def test_revoke_body_cannot_smuggle_a_decision_or_operation() -> None:
    service = _Service()
    response = TestClient(_app(service=service)).post(
        _path("revoke"),
        headers={OPERATOR_TOKEN_HEADER: TOKEN},
        json={"request_digest": "sha256:request", "decision": "approve", "operation": "decide"},
    )
    assert response.status_code == 422
    assert service.calls == []


def test_missing_service_is_redacted_503() -> None:
    response = TestClient(_app(service=None)).post(
        _path("revoke"),
        headers={OPERATOR_TOKEN_HEADER: TOKEN},
        json={"request_digest": "sha256:request"},
    )
    assert response.status_code == 503
    assert response.json()["error"] == "operator approval service unavailable"
    assert TOKEN not in response.text
    assert DSN not in response.text


def test_service_failure_is_redacted_503() -> None:
    class FailingService:
        def __call__(self, **_: Any) -> dict[str, Any]:
            raise RuntimeError(f"failed for {DSN} with {TOKEN}")

    response = TestClient(_app(service=FailingService())).post(
        _path("decide"),
        headers={OPERATOR_TOKEN_HEADER: TOKEN},
        json=_decision_body(),
    )
    assert response.status_code == 503
    assert response.json()["error"] == "operator approval service unavailable"
    assert TOKEN not in response.text
    assert DSN not in response.text


def test_base_request_remains_body_identity_free() -> None:
    model = ApprovalControlRequest(request_digest="sha256:request")
    assert model.request_digest == "sha256:request"
    with pytest.raises(ValueError):
        ApprovalControlRequest.model_validate(
            {"request_digest": "sha256:request", "owner": "attacker"}
        )


def test_native_service_factory_is_unavailable_without_a_loadable_deployment() -> None:
    services = coordination_services.build_native_coordination_services(
        dsn="postgresql://unused",
        deployment_name="missing-deployment",
    )
    assert services.operator_control_service is None
    assert services.approval_control_service is None


def test_native_approval_adapter_passes_server_identity_and_digest(monkeypatch) -> None:
    class Native:
        OPERATOR_CONTROL_ACTOR = OPERATOR_PRINCIPAL
        OPERATOR_CONTROL_SESSION = OPERATOR_SESSION_ID

        @staticmethod
        def ensure_native_coordination_schema(_conn) -> None:
            return None

        @staticmethod
        def bootstrap_control(conn, **kwargs):
            return {
                "ok": True,
                "action": "bootstrap",
                "deployment_name": kwargs["deployment_name"],
                "dsn": DSN,
                "unexpected": "must be dropped",
            }

        @staticmethod
        def fallback_control(conn, **kwargs):
            return {
                "ok": True,
                "action": "fallback",
                "deployment_name": kwargs["deployment_name"],
                "dsn": DSN,
                "unexpected": "must be dropped",
            }

        @staticmethod
        def reactivate_control(conn, **kwargs):
            return {"ok": True, "operation": "reactivation", "kwargs": kwargs}

    class Approvals:
        calls: list[dict[str, Any]] = []

        @classmethod
        def decide_native_approval(cls, conn, **kwargs):
            cls.calls.append({"conn": conn, **kwargs})
            return {
                "ok": True,
                "request_digest": kwargs["request_digest"],
                "token": TOKEN,
                "unexpected": "must be dropped",
            }

        @classmethod
        def revoke_native_approval(cls, conn, **kwargs):
            cls.calls.append({"conn": conn, **kwargs})
            return {
                "ok": True,
                "request_digest": kwargs["request_digest"],
                "dsn": DSN,
                "unexpected": "must be dropped",
            }

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(
        coordination_services,
        "_load_native_modules",
        lambda _deployment: (Native, Approvals),
    )
    monkeypatch.setattr(
        coordination_services,
        "db_connect",
        lambda *_args, **_kwargs: Connection(),
    )
    services = coordination_services.build_native_coordination_services(
        dsn="postgresql://unused",
        deployment_name="demo",
    )
    assert services.approval_control_service is not None
    assert services.operator_control_service is not None
    operator_result = services.operator_control_service(
        principal=OPERATOR_PRINCIPAL,
        request=OperatorControlRequest(
            operation="fallback",
            structured_plan={"reason": "operator recovery"},
            claimed_plan_digest="sha256:plan",
            expected_policy_digest="sha256:policy",
            expected_project_digest="sha256:project",
            idempotency_key="fallback-key",
        ),
    )
    assert operator_result == {
        "ok": True,
        "action": "fallback",
        "deployment_name": "demo",
    }
    assert DSN not in str(operator_result)
    result = services.approval_control_service(
        principal=OPERATOR_PRINCIPAL,
        session_id=OPERATOR_SESSION_ID,
        request=ApprovalDecisionRequest(
            request_digest="sha256:request",
            decision="approve",
        ),
    )
    assert result == {"ok": True, "request_digest": "sha256:request"}
    assert len(Approvals.calls) == 1
    assert {
        key: value
        for key, value in Approvals.calls[0].items()
        if key != "conn"
    } == {
        "request_digest": "sha256:request",
        "decision": "approve",
        "deployment_name": "demo",
        "operator_principal": OPERATOR_PRINCIPAL,
        "operator_session_id": OPERATOR_SESSION_ID,
    }

    revoked = services.approval_control_service(
        principal=OPERATOR_PRINCIPAL,
        session_id=OPERATOR_SESSION_ID,
        request=ApprovalRevokeRequest(request_digest="sha256:request"),
    )
    assert revoked == {"ok": True, "request_digest": "sha256:request"}
    assert len(Approvals.calls) == 2
    assert {
        key: value
        for key, value in Approvals.calls[1].items()
        if key != "conn"
    } == {
        "request_digest": "sha256:request",
        "deployment_name": "demo",
        "operator_principal": OPERATOR_PRINCIPAL,
        "operator_session_id": OPERATOR_SESSION_ID,
    }
    assert TOKEN not in str(revoked)
    assert DSN not in str(revoked)

    with pytest.raises(PermissionError):
        services.approval_control_service(
            principal="operator:attacker",
            session_id=OPERATOR_SESSION_ID,
            request=ApprovalRevokeRequest(request_digest="sha256:request"),
        )
