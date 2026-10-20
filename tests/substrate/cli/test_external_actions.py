"""The external-action CLI is an authenticated HTTP client, never an executor."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from okg.substrate.cli.external_actions import (
    _request_for_args,
    _route_for_args,
    _validated_api_url,
)
from okg.substrate.cli.main import _build_parser


EXTERNAL_ACTION_SUBCOMMANDS = {
    "serve",
    "list",
    "show",
    "timeline",
    "recovery",
    "submit",
    "edit",
    "decide",
    "cancel",
    "claim",
    "attest-owner-unavailable",
    "outcome",
    "restore-create",
    "restore-batch",
    "restore-finalize",
}


def _args(**overrides: object) -> SimpleNamespace:
    values: dict[str, object] = {
        "external_action_subcmd": "show",
        "deployment": "support-prod",
        "proposal_id": "proposal-1",
        "execution_id": None,
        "restore_id": None,
        "effect_instance_key": None,
        "state": None,
        "limit": 100,
        "api_url": "https://review.example.test",
        "authorization_env": "TEST_EXTERNAL_ACTION_AUTH",
        "dpop_env": "TEST_EXTERNAL_ACTION_DPOP",
        "client_cert": None,
        "client_key": None,
        "ca_bundle": None,
        "timeout_seconds": 7.0,
        "idempotency_key": None,
        "if_match": None,
        "body_file": None,
        "batch_index": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_client_import_has_no_local_store_or_service_path() -> None:
    source = Path(
        __import__(
            "okg.substrate.cli.external_actions",
            fromlist=["__file__"],
        ).__file__
    ).read_text(encoding="utf-8")
    assert "external_actions.store" not in source
    assert "external_actions.service" not in source


def test_api_url_requires_https_except_loopback() -> None:
    assert _validated_api_url("https://review.example.test/") == (
        "https://review.example.test"
    )
    assert _validated_api_url("http://127.0.0.1:8788") == (
        "http://127.0.0.1:8788"
    )
    with pytest.raises(ValueError, match="require HTTPS"):
        _validated_api_url("http://review.example.test")
    with pytest.raises(ValueError, match="credentials"):
        _validated_api_url("https://token@review.example.test")


def test_routes_percent_encode_identifiers_and_bound_list_limit() -> None:
    assert _route_for_args(_args(proposal_id="../secret")) == (
        "GET",
        "/v1/deployments/support-prod/external-actions/..%2Fsecret",
    )
    assert _route_for_args(
        _args(external_action_subcmd="list", state="pending", limit=10000)
    ) == (
        "GET",
        "/v1/deployments/support-prod/external-actions?state=pending&limit=500",
    )


def test_mutation_uses_env_credentials_and_refuses_redirects(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    body = tmp_path / "decision.json"
    body.write_text(json.dumps({"verdict": "approve"}), encoding="utf-8")
    monkeypatch.setenv("TEST_EXTERNAL_ACTION_AUTH", "DPoP opaque-token")
    monkeypatch.setenv("TEST_EXTERNAL_ACTION_DPOP", "opaque-proof")
    observed: dict[str, object] = {}

    class Response:
        is_success = True

    def request(method: str, url: str, **kwargs: object) -> Response:
        observed.update({"method": method, "url": url, **kwargs})
        return Response()

    monkeypatch.setattr("requests.request", request)
    response = _request_for_args(
        _args(
            external_action_subcmd="decide",
            body_file=str(body),
            idempotency_key="reviewer-key-1",
            if_match='"lifecycle:1"',
        )
    )
    assert response.is_success
    assert observed["method"] == "POST"
    assert observed["url"] == (
        "https://review.example.test/v1/deployments/support-prod/"
        "external-actions/proposal-1/decisions"
    )
    headers = observed["headers"]
    assert isinstance(headers, dict)
    assert headers["Authorization"] == "DPoP opaque-token"
    assert headers["DPoP"] == "opaque-proof"
    assert headers["Idempotency-Key"] == "reviewer-key-1"
    assert headers["If-Match"] == '"lifecycle:1"'
    assert observed["json"] == {"verdict": "approve"}
    assert observed["allow_redirects"] is False


def test_certificate_only_client_omits_token_headers(monkeypatch) -> None:
    observed: dict[str, object] = {}

    class Response:
        is_success = True

    def request(method: str, url: str, **kwargs: object) -> Response:
        observed.update({"method": method, "url": url, **kwargs})
        return Response()

    monkeypatch.delenv("TEST_EXTERNAL_ACTION_AUTH", raising=False)
    monkeypatch.delenv("TEST_EXTERNAL_ACTION_DPOP", raising=False)
    monkeypatch.setattr("requests.request", request)
    response = _request_for_args(
        _args(
            external_action_subcmd="restore-finalize",
            restore_id="restore-1",
            client_cert="worker.crt",
            client_key="worker.key",
        )
    )
    assert response.is_success
    headers = observed["headers"]
    assert isinstance(headers, dict)
    assert "Authorization" not in headers
    assert "DPoP" not in headers
    assert observed["cert"] == ("worker.crt", "worker.key")


def test_parser_requires_state_fence_for_decisions() -> None:
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([
            "external-action",
            "decide",
            "--deployment",
            "support-prod",
            "proposal-1",
            "--idempotency-key",
            "decision-1",
            "--body-file",
            "decision.json",
        ])
    args = parser.parse_args([
        "external-action",
        "decide",
        "--deployment",
        "support-prod",
        "proposal-1",
        "--idempotency-key",
        "decision-1",
        "--if-match",
        '"lifecycle:1"',
        "--body-file",
        "decision.json",
    ])
    assert args.external_action_subcmd == "decide"


def test_parser_exposes_the_complete_bounded_surface() -> None:
    parser = _build_parser()
    top = next(
        action
        for action in parser._actions
        if hasattr(action, "choices") and action.choices
    )
    external = top.choices["external-action"]
    commands = next(
        action
        for action in external._actions
        if hasattr(action, "choices") and action.choices
    )
    assert set(commands.choices) == EXTERNAL_ACTION_SUBCOMMANDS
