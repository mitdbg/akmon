"""Strict transport seam for the three Viz coordination controls."""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")
from fastapi.routing import APIRoute  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from okg.viz.app import build_app  # noqa: E402
from okg.viz.auth import (  # noqa: E402
    OPERATOR_TOKEN_HEADER,
    is_mutating_route,
    is_operator_control_route,
)
from okg.viz.operator_control import OperatorControlRequest  # noqa: E402


TOKEN = "viz-control-test-token"
DSN = "postgresql://must-never-appear.example/db"


class _Ctx:
    deployment_name = "demo"


class _Service:
    def __init__(self) -> None:
        self.calls: list[tuple[str, OperatorControlRequest]] = []

    def __call__(self, principal: str, request: OperatorControlRequest) -> dict[str, Any]:
        self.calls.append((principal, request))
        return {
            "ok": True,
            "receipt": "redacted-receipt",
            "operation": request.operation,
        }


def _payload(operation: str = "bootstrap") -> dict[str, Any]:
    return {
        "operation": operation,
        "structured_plan": {"allowed_mutations": ["chief"]},
        "claimed_plan_digest": "sha256:claimed",
        "expected_policy_digest": "sha256:policy",
        "expected_project_digest": "sha256:project",
        "idempotency_key": "viz-control-test-1",
    }


def _app(*, service: _Service | None = None, token: str | None = TOKEN):
    return build_app(
        ctx_provider=_Ctx,
        is_ready=lambda: True,
        operator_token=token,
        operator_control_service=service,
    )


def _path(operation: str) -> str:
    return f"/viz/api/coordination/control/{operation}"


def test_control_is_exactly_three_direct_routes_and_not_openapi_or_tools() -> None:
    app = _app()
    routes = {
        route.path
        for route in app.routes
        if isinstance(route, APIRoute)
        and route.methods
        and "POST" in route.methods
        and "/coordination/control/" in route.path
    }
    assert routes == {_path(op) for op in ("bootstrap", "fallback", "reactivation")}
    # They remain in the global POST mutation inventory for its completeness
    # check, while operator_auth gives them a stricter dedicated branch.
    assert all(is_mutating_route("POST", path) for path in routes)
    assert all(is_operator_control_route("POST", path) for path in routes)
    assert not is_operator_control_route("POST", "/viz/api/coordination/control/other")
    assert not is_operator_control_route("GET", _path("bootstrap"))
    assert all(path not in app.openapi()["paths"] for path in routes)


@pytest.mark.parametrize("operation", ["bootstrap", "fallback", "reactivation"])
def test_configured_token_required_for_loopback_and_in_process(
    operation: str,
) -> None:
    service = _Service()
    app = _app(service=service, token=None)

    # TestClient is an in-process ASGI caller and uses a loopback-style client
    # peer.  Neither may use the existing no-token loopback trust policy here.
    response = TestClient(app, client=("127.0.0.1", 41234)).post(
        _path(operation),
        json=_payload(operation),
    )
    assert response.status_code == 401
    assert service.calls == []


def test_wrong_token_is_rejected_before_body_validation_or_side_effect() -> None:
    service = _Service()
    client = TestClient(_app(service=service), client=("127.0.0.1", 41234))
    response = client.post(
        _path("bootstrap"),
        headers={OPERATOR_TOKEN_HEADER: "wrong"},
        json={"actor": "attacker", "dsn": DSN},
    )
    assert response.status_code == 401
    assert service.calls == []
    assert TOKEN not in response.text
    assert DSN not in response.text


def test_server_stamps_principal_and_body_identity_is_rejected() -> None:
    service = _Service()
    client = TestClient(_app(service=service))
    body = _payload()
    body["actor"] = "attacker"
    body["principal"] = "operator:attacker"
    body["dsn"] = DSN
    response = client.post(
        _path("bootstrap"),
        headers={OPERATOR_TOKEN_HEADER: TOKEN},
        json=body,
    )
    assert response.status_code == 422
    assert service.calls == []
    assert DSN not in response.text

    response = client.post(
        _path("bootstrap"),
        headers={OPERATOR_TOKEN_HEADER: TOKEN},
        json=_payload(),
    )
    assert response.status_code == 200, response.text
    assert service.calls[0][0] == "operator:viz-token"
    assert not hasattr(service.calls[0][1], "actor")


def test_operation_is_bound_to_exact_path() -> None:
    service = _Service()
    client = TestClient(_app(service=service))
    response = client.post(
        _path("fallback"),
        headers={OPERATOR_TOKEN_HEADER: TOKEN},
        json=_payload("bootstrap"),
    )
    assert response.status_code == 422
    assert service.calls == []


def test_generic_tools_path_cannot_reach_control_service() -> None:
    service = _Service()
    client = TestClient(_app(service=service))
    response = client.post(
        "/viz/api/tools/coordination_control_bootstrap",
        headers={OPERATOR_TOKEN_HEADER: TOKEN},
        json=_payload(),
    )
    assert response.status_code == 404
    assert service.calls == []


def test_missing_production_service_is_explicit_503_without_secrets() -> None:
    client = TestClient(_app(service=None))
    response = client.post(
        _path("fallback"),
        headers={OPERATOR_TOKEN_HEADER: TOKEN},
        json=_payload("fallback"),
    )
    assert response.status_code == 503
    assert response.json()["error"] == "operator coordination control unavailable"
    assert TOKEN not in response.text
    assert DSN not in response.text


def test_native_control_failure_is_redacted_503() -> None:
    def failing_service(
        principal: str,
        request: OperatorControlRequest,
    ) -> dict[str, Any]:
        del principal, request
        raise RuntimeError(f"native failure at {DSN}")

    response = TestClient(
        _app(service=failing_service),
        raise_server_exceptions=False,
    ).post(
        _path("fallback"),
        headers={OPERATOR_TOKEN_HEADER: TOKEN},
        json=_payload("fallback"),
    )
    assert response.status_code == 503
    assert response.json()["error"] == "operator coordination control unavailable"
    assert TOKEN not in response.text
    assert DSN not in response.text


def test_existing_viz_read_auth_compatibility_is_preserved() -> None:
    # Existing read routes do not inherit this new dedicated token gate.
    response = TestClient(_app(token=None), raise_server_exceptions=False).get(
        "/viz/api/health"
    )
    assert response.status_code != 401
    assert "operator authentication required" not in response.text
