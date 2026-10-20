"""Diagnostics describe their evidence and withhold identity registry details."""

from contextlib import nullcontext
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any
import json

import pytest
from mcp.server.fastmcp import FastMCP
from mcp.server.lowlevel.server import request_ctx
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.routing import Route

from okg.substrate.mcp import principal_readiness as readiness
from okg.substrate.mcp.http_auth import StaticTokenVerifier, build_auth_settings
from okg.substrate.mcp.principal_mapping import PrincipalMapping

CONN = SimpleNamespace(transaction=nullcontext)

MAPPING = PrincipalMapping(
    "provider", "issuer", "audience", "sub", "x-user", "mcp_client"
)
PROVIDER = SimpleNamespace(
    provider_id="provider",
    issuer="issuer",
    audiences=("audience",),
    subject_claim="sub",
    metadata={"trusted_verifiers": ["app"]},
)


def _server():
    server = FastMCP(
        "readiness",
        token_verifier=StaticTokenVerifier("fixture-token"),
        auth=build_auth_settings(),
    )
    server._okg_multi_user_principals = True
    server._okg_principal_mapping = MAPPING
    return server


def _runtime(server, providers=lambda: [PROVIDER]):
    return readiness.runtime_principal_readiness(
        server, deployment="fixture", load_providers=providers
    )


def test_constructed_unserved_verifier_is_not_http_proof():
    server = _server()
    server.streamable_http_app()  # Merely constructing the right app proves nothing.
    report = _runtime(server)
    assert report == {
        "runtime_mode": "multi_user",
        "credential_verifier": "configured",
        "mapping": {"status": "configured", "source": "startup_parsed"},
        "provider_validation": {"status": "validated"},
        "http_protection": "unmeasured",
        "http_credential_probe": "unmeasured",
    }


@pytest.mark.parametrize(
    "shape", ["exact", "unrelated", "duplicate", "wrong_path", "unwrapped"]
)
def test_only_exact_request_app_route_can_report_observed_wrapper(shape):
    from mcp.server.auth.middleware.bearer_auth import RequireAuthMiddleware

    async def endpoint(scope, receive, send):
        pass

    server = _server()
    wrapped = RequireAuthMiddleware(endpoint, required_scopes=[])
    routes = [Route("/elsewhere" if shape == "unrelated" else "/mcp", endpoint=wrapped)]
    if shape == "duplicate":
        routes.append(Route("/mcp", endpoint=endpoint))
    if shape == "unwrapped":
        routes = [Route("/mcp", endpoint=endpoint)]
    app = Starlette(routes=routes)
    request = Request(
        {
            "type": "http",
            "path": "/elsewhere" if shape == "wrong_path" else "/mcp",
            "app": app,
        }
    )
    token = request_ctx.set(SimpleNamespace(request=request))
    try:
        report = _runtime(server)
    finally:
        request_ctx.reset(token)
    assert report["http_protection"] == (
        "auth_wrapper_observed" if shape == "exact" else "unmeasured"
    )
    assert report["http_credential_probe"] == "unmeasured"


@pytest.mark.parametrize("failure", ["load", "validate"])
def test_provider_failures_are_fixed_safe_codes(monkeypatch, failure):
    secret = "private-issuer/private-user/provider-inventory"

    def fail(*args, **kwargs):
        raise RuntimeError(secret)

    loader = fail if failure == "load" else lambda: [PROVIDER]
    if failure == "validate":
        monkeypatch.setattr(readiness, "validate_mapping_against_providers", fail)
    report = _runtime(_server(), loader)
    assert secret not in json.dumps(report)
    assert report["provider_validation"] == (
        {"status": "unmeasured", "reason": "provider_probe_unavailable"}
        if failure == "load"
        else {"status": "blocked", "reason": "provider_validation_failed"}
    )


def _manifest(mapping=MAPPING):
    return SimpleNamespace(
        nomos={
            "runtime_enforcement": "enforce",
            "deployment_posture": {"read_principals": ["mcp_client"]},
            "principal_mapping": mapping.to_dict() if mapping else None,
        }
    )


def _offline_fixture(monkeypatch, manifest):
    import okg.substrate.deployments as deployments
    import okg.substrate.nomos.identity_store as store

    monkeypatch.setattr(deployments, "resolve_deployment", lambda _: manifest)
    monkeypatch.setattr(store, "load_identity_providers", lambda *a, **k: [PROVIDER])
    monkeypatch.delenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", raising=False)


def test_role_failure_does_not_skip_bad_mapping(monkeypatch):
    from okg.substrate.cli import deployment_readiness as cli

    manifest = _manifest()
    manifest.nomos["principal_mapping"] = {"private-input": "do not expose"}
    _offline_fixture(monkeypatch, manifest)
    monkeypatch.setattr(
        cli,
        "_ready_mcp_principal_login_status",
        lambda _: {"ok": False, "reason": "role_unusable"},
    )
    report = cli._ready_mcp_status(CONN, "fixture")
    assert report["reason"] == "role_unusable" and report["ok"] is False
    assert report["principal_readiness"]["mapping"] == {
        "status": "blocked",
        "source": "current_manifest",
        "reason": "mapping_invalid",
    }
    assert report["probe_scope"] == "database_graph_sample_only"
    assert "do not expose" not in json.dumps(report)


def test_runtime_and_current_manifest_drift_are_distinct(monkeypatch, tmp_path):
    import yaml
    import okg.substrate.deployments as deployments
    from okg.substrate.mcp.principal_mapping import principal_mapping_from_manifest
    import okg.substrate.nomos.identity_store as store

    path = tmp_path / "deployment.yaml"
    (tmp_path / "schemas").mkdir()
    (tmp_path / "source_registry.yaml").write_text("sources: {}\n")
    raw = {
        "name": "fixture",
        "description": "Readiness drift proof",
        "postgres": {"dsn": "postgresql://localhost/unused"},
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "modules": [],
        "enrichers": [],
        "nomos": _manifest().nomos,
    }
    path.write_text(yaml.safe_dump(raw))
    monkeypatch.delenv("OKG_ENVIRONMENT_DESCRIPTOR", raising=False)
    monkeypatch.setattr(store, "load_identity_providers", lambda *a, **k: [PROVIDER])
    server = _server()
    startup_mapping = principal_mapping_from_manifest(deployments.resolve_deployment(str(path)))
    server._okg_principal_mapping = startup_mapping
    assert (
        readiness.offline_principal_readiness(CONN, str(path))["provider_validation"][
            "status"
        ]
        == "validated"
    )
    raw["nomos"] = _manifest(replace(MAPPING, issuer="changed-current-manifest")).nomos
    path.write_text(yaml.safe_dump(raw))
    live = _runtime(server)
    offline = readiness.offline_principal_readiness(CONN, str(path))
    assert live["provider_validation"]["status"] == "validated"
    assert offline["provider_validation"] == {
        "status": "blocked",
        "reason": "provider_validation_failed",
    }
    assert live["mapping"]["source"] == "startup_parsed"
    assert offline["mapping"]["source"] == "current_manifest"
    assert offline["runtime_mode"] == offline["http_protection"] == "unmeasured"
    assert server._okg_principal_mapping is startup_mapping


@pytest.mark.parametrize(
    "condition", ["absent", "enforcement_off", "posture", "provider_error"]
)
def test_offline_preconditions_are_separate_and_safe(monkeypatch, condition):
    import okg.substrate.nomos.identity_store as store

    manifest = _manifest(None if condition == "absent" else MAPPING)
    if condition == "enforcement_off":
        manifest.nomos["runtime_enforcement"] = "off"
    if condition == "posture":
        manifest.nomos["deployment_posture"]["read_principals"] = ["human_operator"]
    _offline_fixture(monkeypatch, manifest)
    if condition == "provider_error":
        monkeypatch.setattr(
            store,
            "load_identity_providers",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("sensitive-provider")),
        )
    report = readiness.offline_principal_readiness(CONN, "fixture")
    assert "sensitive-provider" not in json.dumps(report)
    assert report["runtime_mode"] == report["http_credential_probe"] == "unmeasured"
    if condition == "absent":
        assert report["mapping"]["status"] == "absent"
    elif condition == "enforcement_off":
        assert report["runtime_enforcement"] == "disabled"
        assert report["provider_validation"]["status"] == "validated"
    elif condition == "posture":
        assert report["mapping"]["status"] == "blocked"
    else:
        assert report["provider_validation"]["reason"] == "provider_probe_unavailable"


def test_single_operator_mapping_absence_remains_ordinary():
    report = _runtime(FastMCP("single"))
    assert report["runtime_mode"] == "single_operator"
    assert report["mapping"]["status"] == "absent"
    assert report["credential_verifier"] == "absent"


@pytest.mark.parametrize(
    "code",
    [
        "nomos_principal_unidentified",
        "nomos_identity_channel_conflict",
        "mcp_multi_user_requires_auth",
        "mcp_multi_user_stdio_incompatible",
    ],
)
def test_instance_refusal_codes_match_documented_raise_sites(code):
    import yaml
    from okg.substrate.errors import SubstrateError
    from okg.substrate.mcp import principal_mapping, server

    if code == "nomos_principal_unidentified":
        error = principal_mapping.unidentified_principal_error(
            "missing", channel="absent_header"
        )
    elif code == "nomos_identity_channel_conflict":
        error = principal_mapping.conflicting_identity_channel_error()
    elif code == "mcp_multi_user_requires_auth":
        error = principal_mapping.multi_user_requires_auth_error(
            transport="streamable-http"
        )
    else:
        with pytest.raises(SubstrateError) as caught:
            server.run_server(
                dsn="unused",
                deployment_name="fixture",
                multi_user_principals=True,
                transport="stdio",
            )
        error = caught.value
    assert error.code == code
    path = Path(__file__).resolve().parents[3] / "docs/reference/error_codes.yaml"
    entries = {
        entry["code"]: entry for entry in yaml.safe_load(path.read_text())["codes"]
    }
    entry = entries[code]
    assert entry["python_exception"] is None
    assert set(entry["data_shape"]) == set(error.data)
    assert "docs/reference/mcp.md" in entry["referenced_in"]


@pytest.mark.parametrize(
    "case",
    [
        "invalid_mapping",
        "role_and_mapping",
        "invalid_provider",
        "enforcement_off",
        "absent",
        "valid",
    ],
)
@pytest.mark.parametrize("as_json", [True, False])
def test_composed_readiness_keeps_graph_and_configuration_verdicts_separate(
    monkeypatch, capsys, case, as_json
):
    from okg.substrate.cli import deployment_readiness as cli
    from okg.substrate.cli._output import emit_result

    manifest = _manifest(None if case == "absent" else MAPPING)
    if case in {"invalid_mapping", "role_and_mapping"}:
        manifest.nomos["principal_mapping"] = {"secret": "private-configuration"}
    _offline_fixture(monkeypatch, manifest)
    if case == "invalid_provider":
        manifest.nomos["principal_mapping"]["issuer"] = "inconsistent-provider"
    if case == "enforcement_off":
        manifest.nomos["runtime_enforcement"] = "off"
    graph_ok = case != "role_and_mapping"
    graph_calls: list[Any] = []

    def _graph_status(conn: Any, deployment: str, *, session: Any = None) -> dict:
        graph_calls.append(session)
        return {"ok": graph_ok}

    monkeypatch.setattr(cli, "_ready_mcp_graph_status", _graph_status)
    # The console hands its pinned session through; the graph probe must get it.
    pinned = object()
    mcp = cli._ready_mcp_status(CONN, "fixture", session=pinned)
    assert graph_calls == [pinned]
    payload = {"mcp": mcp, "checks": [], "blockers": [], "warnings": []}
    cli._ready_add_check(
        payload, "mcp.graph_read", "pass" if graph_ok else "blocker", "Graph sample"
    )
    cli._ready_add_mcp_principal_check(payload, mcp)
    cli._ready_finalize_payload_state(payload)
    checks = {row["id"]: row for row in payload["checks"]}
    assert checks["mcp.graph_read"]["ok"] is graph_ok
    expected_ok = case in {"absent", "valid"}
    assert payload["ok"] is expected_ok
    assert checks["mcp.principal_configuration"]["ok"] is expected_ok
    assert mcp["principal_readiness"]["http_credential_probe"] == "unmeasured"
    emit_result(payload, as_json=as_json)
    rendered = capsys.readouterr().out
    assert "mcp.principal_configuration" in rendered
    assert "live HTTP authentication unmeasured" in rendered
    assert "private-configuration" not in rendered
    if as_json:
        assert json.loads(rendered)["ok"] is expected_ok
