"""Supervised UI read token (issue #3278 follow-up).

Before: the supervised ``okg.viz`` argv had no way to carry ``--auth-token-env``,
and the readiness probe called ``/viz/api/health`` with no credential, so
turning the read token on for a supervised UI would have made the probe fail.

After: ``ui.auth_token_env`` in the environment descriptor names the token
variable. It must also be declared under ``runtime.credential_refs`` (that is
how the value reaches the service), the viz argv carries only the NAME, and the
readiness probe sends the value as a Bearer header and never reports it.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml

import okg.substrate.environment_readiness as environment_readiness
import okg.substrate.environment_services as environment_services
from okg.substrate.deployments import DeploymentConfigError
from okg.substrate.environment_services import render_environment_services
from tests.substrate.test_environment_services import (
    _artifact,
    _environment_fixture,
    _load,
)

_UI_TOKEN_ENV = "OKG_TEST_UI_READ_TOKEN"
_UI_TOKEN_VALUE = "ui-read-token-0123456789abcdef"


def _declare_ui_token(
    root: Path,
    environment_id: str,
    *,
    credential_ref: bool = True,
) -> None:
    descriptor = root / environment_id / "environment.yaml"
    raw = yaml.safe_load(descriptor.read_text(encoding="utf-8"))
    raw["ui"]["auth_token_env"] = _UI_TOKEN_ENV
    if credential_ref:
        raw["runtime"]["credential_refs"] = {
            raw["ui"]["service"]: [f"env:{_UI_TOKEN_ENV}"],
        }
    descriptor.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")


def _ui_unit(environment: Any, values: dict[str, str]) -> str:
    plan = render_environment_services(
        environment, adapter="systemd", systemd_scope="user", environ=values
    )
    return _artifact(plan, "ui_service")["content"]


def test_supervised_ui_passes_the_token_variable_name_to_the_viz(
    tmp_path: Path,
) -> None:
    root, values = _environment_fixture(tmp_path)
    plain = _load(root, "fixture-staging", values)
    assert "--auth-token-env" not in _ui_unit(plain, values)
    assert "--auth-token-env" not in environment_services._expected_service_arguments(
        plain, plain.ui.service
    )

    _declare_ui_token(root, "fixture-staging")
    environment = _load(root, "fixture-staging", values)
    assert environment.ui.auth_token_env == _UI_TOKEN_ENV
    unit = _ui_unit(environment, values)
    assert f"--auth-token-env {_UI_TOKEN_ENV}" in unit
    # The value never enters a unit; only the variable name does.
    assert _UI_TOKEN_VALUE not in unit
    # The argv the supervisor verifies matches the installed command.
    expected = environment_services._expected_service_arguments(
        environment, environment.ui.service
    )
    assert expected[-2:] == ("--auth-token-env", _UI_TOKEN_ENV)


def test_ui_token_variable_must_be_a_declared_credential_ref(
    tmp_path: Path,
) -> None:
    root, values = _environment_fixture(tmp_path)
    _declare_ui_token(root, "fixture-staging", credential_ref=False)
    with pytest.raises(DeploymentConfigError, match="credential_refs"):
        _load(root, "fixture-staging", values)


def test_ui_token_field_must_be_a_variable_name_not_a_value(
    tmp_path: Path,
) -> None:
    root, values = _environment_fixture(tmp_path)
    descriptor = root / "fixture-staging" / "environment.yaml"
    raw = yaml.safe_load(descriptor.read_text(encoding="utf-8"))
    raw["ui"]["auth_token_env"] = "tok en/with spaces"
    descriptor.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    with pytest.raises(DeploymentConfigError, match="variable NAME"):
        _load(root, "fixture-staging", values)


def test_readiness_ui_probe_sends_the_bearer_token_and_never_reports_it(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    _declare_ui_token(root, "fixture-staging")
    environment = _load(root, "fixture-staging", values)
    monkeypatch.setenv(_UI_TOKEN_ENV, _UI_TOKEN_VALUE)
    seen: dict[str, Any] = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args) -> None:
            return None

        def read(self) -> bytes:
            return json.dumps(
                {"ok": True, "ready": True, "deployment_name": "other"}
            ).encode("utf-8")

    def open_ui(request, **kwargs):
        seen["authorization"] = request.get_header("Authorization")
        seen["url"] = request.full_url
        return Response()

    monkeypatch.setattr(environment_readiness, "open_validated_url", open_ui)
    # The supervised process owns the port, so the credential may be sent.
    monkeypatch.setattr(
        environment_readiness, "_pid_owns_tcp_listener", lambda *_a, **_k: True
    )
    monkeypatch.setattr(
        environment_readiness, "process_start_identity", lambda _pid: "start-1"
    )
    processes = {
        "services": {
            environment.ui.service: {
                "ok": True,
                "authority": {"pid": 4242, "process_start_identity": "start-1"},
            }
        }
    }

    status = environment_readiness._probe_ui_http(
        environment, processes=processes, timeout_seconds=1
    )

    assert seen["authorization"] == f"Bearer {_UI_TOKEN_VALUE}"
    assert seen["url"].endswith("/viz/api/health")
    assert _UI_TOKEN_VALUE not in seen["url"]
    assert _UI_TOKEN_VALUE not in repr(status)


def test_readiness_ui_probe_blocks_by_name_when_the_token_is_not_in_env(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    root, values = _environment_fixture(tmp_path)
    _declare_ui_token(root, "fixture-staging")
    environment = _load(root, "fixture-staging", values)
    monkeypatch.delenv(_UI_TOKEN_ENV, raising=False)

    def never(*_args, **_kwargs):
        raise AssertionError("an unauthenticated probe must not be sent")

    monkeypatch.setattr(environment_readiness, "open_validated_url", never)

    status = environment_readiness._probe_ui_http(environment, timeout_seconds=1)

    assert status["ok"] is False
    assert status["error"] == "UiAuthTokenUnavailable"
    assert _UI_TOKEN_ENV in status["message"]
