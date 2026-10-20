from __future__ import annotations

import asyncio
import logging
import ssl
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from okg.substrate.contracts.deployment_product import make_action_plan
from okg.substrate.external_actions.config import (
    ConfigError,
    DeploymentProductAuthority,
    load_config,
    load_deployment_product_authority,
)
from okg.substrate.external_actions.server import (
    ServerStartupError,
    create_app,
    mtls_uvicorn_protocol_class,
    run_server,
)
from okg.substrate.external_actions.signing import generate_keypair
from okg.substrate.external_actions.store import ExternalActionStoreError

import okg.substrate.external_actions.server as server_module

from .test_action_contract_authority import _registry


def _files(tmp_path: Path, *, mutate_registry=None):
    registry, definition, lock = _registry()
    plan = make_action_plan(
        lock,
        expires_at=datetime.now(UTC) + timedelta(hours=1),
        artifact_digests={},
    )
    if mutate_registry is not None:
        mutate_registry(registry)
    definition_path = tmp_path / "definition.yaml"
    plan_path = tmp_path / "product-plan.yaml"
    registry_path = tmp_path / "external-actions.yaml"
    definition_path.write_text(
        yaml.safe_dump(definition.model_dump(mode="json", by_alias=True), sort_keys=False),
        encoding="utf-8",
    )
    plan_path.write_text(yaml.safe_dump(plan.payload(), sort_keys=False), encoding="utf-8")
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")
    authority = DeploymentProductAuthority(
        authored_definition_path=str(definition_path),
        product_plan_path=str(plan_path),
        expected_product_plan_digest=plan.plan_digest,
    )
    return authority, registry_path, plan_path


def _config(
    authority: DeploymentProductAuthority,
    registry_path: Path,
    *,
    claim_key: bool = True,
    dsn: str | None = None,
    verifier_capability_env: str | None = None,
):
    _, epoch_public = generate_keypair()
    keys = [
        {
            "kid": "d/epoch/1",
            "purpose": "execution_epoch_lease",
            "algorithm": "EdDSA",
            "public_key_b64": epoch_public,
            "state": "current",
        }
    ]
    if claim_key:
        _, claim_public = generate_keypair()
        keys.append(
            {
                "kid": "d/claim/1",
                "purpose": "claim_and_restore_completion",
                "algorithm": "EdDSA",
                "public_key_b64": claim_public,
                "state": "current",
            }
        )
    raw = {
        "schema": "okg.external_action_review.config/v1",
        "deployment": "d",
        "authentication": {
            "audience": "deployment_scoped_review_api",
            "allowed_algorithms": ["EdDSA"],
        },
        "signing_keys": keys,
        "action_registry_path": str(registry_path),
        "deployment_product_authority": {
            "authored_definition_path": authority.authored_definition_path,
            "product_plan_path": authority.product_plan_path,
            "expected_product_plan_digest": authority.expected_product_plan_digest,
        }
    }
    if dsn is not None:
        raw["database_dsn"] = dsn
    if verifier_capability_env is not None:
        raw["verifier_capability_env"] = verifier_capability_env
    return load_config(raw)


class _DurableStore:
    is_durable = True
    deployment = "d"
    connection = object()
    nomos_storage_adapter = object()
    policy_digest = "sha256:" + "a" * 64

    def __init__(self):
        self.startup_calls = 0
        self.runtime_contract_calls = 0

    def require_runtime_contract(self):
        self.runtime_contract_calls += 1

    def insert_nonce(self, **_kwargs):
        return True

    def record_auth_audit(self, *_args, **_kwargs):
        return "audit"

    def startup_login_binding(self):
        self.startup_calls += 1
        return {
            "database_system_identifier": "1",
            "session_user": "okg_external_action_d",
            "deployment": "d",
            "binding_version": 1,
            "verifier_capability_generation": 1,
            "bound_at": "2026-08-26T00:00:00.000000Z",
        }

    def retained_leases(self, **_kwargs):
        return ()

    def restore_fence(self, **_kwargs):
        return None

    def insert_lease(self, **_kwargs):
        return None

    def set_restore_fence(self, **_kwargs):
        return None

    @contextmanager
    def transaction(self):
        yield self


class _RunServerService:
    """Minimal service boundary for exercising production store construction."""

    def __init__(self) -> None:
        self.store = None
        self.authorize = lambda *_args, **_kwargs: True


def _patch_run_server_dependencies(monkeypatch, *, expected_capability: str | None = None):
    calls: dict[str, list[object]] = {"stores": [], "apps": [], "listeners": []}

    class RecordingStore:
        is_durable = True
        deployment = "d"
        connection = object()

        def __init__(self, dsn, *, deployment, verifier_capability):
            calls["stores"].append((dsn, deployment, verifier_capability))
            if expected_capability is not None and verifier_capability != expected_capability:
                raise ExternalActionStoreError(
                    "external-action verifier capability is unavailable"
                )

        def require_runtime_contract(self):
            return None

        def insert_nonce(self, **_kwargs):
            return True

    monkeypatch.setattr(
        server_module,
        "_configured_product_authority",
        lambda *_args, **_kwargs: (None, None, None),
    )
    monkeypatch.setattr(
        server_module,
        "_compiled_action_registry",
        lambda *_args, **_kwargs: {"test": object()},
    )
    monkeypatch.setattr(
        server_module,
        "_handoff_signer",
        lambda *_args, **_kwargs: (object(), lambda _payload: {}),
    )
    monkeypatch.setattr(server_module, "PostgresExternalActionStore", RecordingStore)
    def fake_create_app(**kwargs):
        calls["apps"].append(kwargs)
        return SimpleNamespace(listener_bound=False)

    monkeypatch.setattr(server_module, "create_app", fake_create_app)

    import uvicorn

    monkeypatch.setattr(
        uvicorn,
        "run",
        lambda *_args, **_kwargs: calls["listeners"].append(True),
    )
    return calls


def test_product_plan_drift_and_unknown_reference_are_rejected(tmp_path: Path) -> None:
    authority, _, plan_path = _files(tmp_path)
    changed = yaml.safe_load(plan_path.read_text(encoding="utf-8"))
    changed["definition_digest"] = "sha256:" + "0" * 64
    plan_path.write_text(yaml.safe_dump(changed, sort_keys=False), encoding="utf-8")
    with pytest.raises(ConfigError):
        load_deployment_product_authority(authority)


@pytest.mark.parametrize(
    ("verifier_capability_env", "set_environment"),
    [(None, False), ("OKG_EXTERNAL_ACTION_VERIFIER_CAPABILITY", True)],
)
def test_run_server_fails_closed_when_verifier_capability_reference_or_value_is_missing(
    tmp_path: Path,
    monkeypatch,
    verifier_capability_env: str | None,
    set_environment: bool,
) -> None:
    authority, registry_path, _ = _files(tmp_path)
    env_name = "OKG_EXTERNAL_ACTION_VERIFIER_CAPABILITY"
    if set_environment:
        monkeypatch.delenv(env_name, raising=False)
    calls = _patch_run_server_dependencies(monkeypatch, expected_capability="0" * 64)
    config = _config(
        authority,
        registry_path,
        dsn="postgresql://reviewer@localhost/okg",
        verifier_capability_env=verifier_capability_env,
    )

    with pytest.raises(
        ServerStartupError,
        match="verifier capability environment reference is unavailable",
    ):
        run_server(
            config=config,
            epoch_lease_chain=b"unused-test-chain",
            service=_RunServerService(),
        )

    assert calls["stores"] == []
    assert calls["listeners"] == []


@pytest.mark.parametrize("value", ["a" * 63, "A" * 64, "g" * 64, "0" * 63 + " "])
def test_run_server_rejects_invalid_verifier_capability_value(
    tmp_path: Path,
    monkeypatch,
    value: str,
) -> None:
    authority, registry_path, _ = _files(tmp_path)
    env_name = "OKG_EXTERNAL_ACTION_VERIFIER_CAPABILITY"
    monkeypatch.setenv(env_name, value)
    calls = _patch_run_server_dependencies(monkeypatch, expected_capability="0" * 64)
    config = _config(
        authority,
        registry_path,
        dsn="postgresql://reviewer@localhost/okg",
        verifier_capability_env=env_name,
    )

    with pytest.raises(ServerStartupError, match="verifier capability is unavailable"):
        run_server(
            config=config,
            epoch_lease_chain=b"unused-test-chain",
            service=_RunServerService(),
        )

    assert calls["listeners"] == []


def test_run_server_passes_only_env_resolved_verifier_capability_to_store(
    tmp_path: Path,
    monkeypatch,
    caplog,
) -> None:
    authority, registry_path, _ = _files(tmp_path)
    env_name = "OKG_EXTERNAL_ACTION_VERIFIER_CAPABILITY"
    capability = "0123456789abcdef" * 4
    monkeypatch.setenv(env_name, capability)
    calls = _patch_run_server_dependencies(monkeypatch, expected_capability=capability)
    config = _config(
        authority,
        registry_path,
        dsn="postgresql://reviewer@localhost/okg",
        verifier_capability_env=env_name,
    )
    caplog.set_level(logging.DEBUG)

    result = run_server(
        config=config,
        epoch_lease_chain=b"unused-test-chain",
        service=_RunServerService(),
    )

    assert result == 0
    assert calls["stores"] == [
        ("postgresql://reviewer@localhost/okg", "d", capability)
    ]
    assert len(calls["apps"]) == 1
    assert calls["listeners"] == [True]
    assert all(capability not in message for message in caplog.messages)


def test_uvicorn_mtls_protocol_hands_verified_peer_certificate_to_asgi() -> None:
    """Stock Uvicorn has no TLS scope field; the protocol adapter supplies it."""

    import uvicorn
    from uvicorn.server import ServerState

    observed: dict[str, object] = {}

    async def application(scope, _receive, _send):
        observed.update(scope)

    config = uvicorn.Config(application, lifespan="off", log_config=None)
    loop = asyncio.new_event_loop()
    try:
        protocol = mtls_uvicorn_protocol_class()(config, ServerState(), {}, loop)

        class PeerSSL:
            verify_mode = ssl.CERT_REQUIRED

            @staticmethod
            def getpeercert(*, binary_form: bool = False):
                assert binary_form is True
                return b"der-peer-certificate"

        class Transport:
            @staticmethod
            def get_extra_info(name: str):
                return PeerSSL() if name == "ssl_object" else None

        protocol.transport = Transport()
        loop.run_until_complete(protocol.app({"type": "http", "extensions": {}}, lambda: None, lambda _: None))
    finally:
        loop.close()
    assert observed["extensions"] == {
        "tls": {"verified": True, "client_cert": b"der-peer-certificate"}
    }


def test_source_registry_reference_drift_fails_before_startup_or_listener(tmp_path: Path, monkeypatch) -> None:
    def drift(registry):
        registry["actions"][0]["governed_tool_binding_ref"] = "foreign:missing:tool"

    authority, registry_path, _ = _files(tmp_path, mutate_registry=drift)
    config = _config(authority, registry_path)
    store = _DurableStore()
    listener_calls = []

    def forbidden_provider(*_args, **_kwargs):
        raise AssertionError("provider execution is outside startup authority")

    monkeypatch.setattr(
        "okg.substrate.contracts.deployment_product.connections.execute_connection_operation",
        forbidden_provider,
    )
    monkeypatch.setattr("uvicorn.run", lambda *_args, **_kwargs: listener_calls.append(True))
    with pytest.raises(ServerStartupError, match="authority|registry"):
        create_app(
            config=config,
            epoch_lease_chain={},
            store=store,
            action_contracts=str(registry_path),
        )
    assert not listener_calls
    assert store.startup_calls == 0


def test_precompiled_registry_cannot_replace_deployment_product_authority(
    tmp_path: Path,
) -> None:
    authority, registry_path, _ = _files(tmp_path)
    config = _config(authority, registry_path)
    store = _DurableStore()

    with pytest.raises(
        ServerStartupError,
        match="compiled action contracts cannot replace deployment product authority",
    ):
        create_app(
            config=config,
            epoch_lease_chain={},
            store=store,
            action_contracts={"ticket.update": object()},
        )

    assert store.runtime_contract_calls == 0
    assert store.startup_calls == 0


def test_marker_only_durable_store_cannot_bypass_runtime_verifier(
    tmp_path: Path,
) -> None:
    authority, registry_path, _ = _files(tmp_path)
    config = _config(authority, registry_path)
    store = _DurableStore()
    store.require_runtime_contract = None

    with pytest.raises(
        ServerStartupError,
        match="runtime login contract is unavailable",
    ):
        create_app(
            config=config,
            epoch_lease_chain={},
            store=store,
            action_contracts=yaml.safe_load(
                registry_path.read_text(encoding="utf-8")
            ),
        )

    assert store.startup_calls == 0


def test_injected_nonce_store_cannot_replace_capability_bound_store(
    tmp_path: Path,
) -> None:
    authority, registry_path, _ = _files(tmp_path)
    config = _config(authority, registry_path)
    store = _DurableStore()
    injected = SimpleNamespace(insert_nonce=lambda **_kwargs: True)

    with pytest.raises(
        ServerStartupError,
        match="DPoP nonce authority cannot be injected",
    ):
        create_app(
            config=config,
            epoch_lease_chain={},
            store=store,
            nonce_store=injected,
            action_contracts=yaml.safe_load(
                registry_path.read_text(encoding="utf-8")
            ),
        )

    assert store.startup_calls == 0


def test_run_server_rejects_marker_only_store_before_signer_or_listener(
    tmp_path: Path,
    monkeypatch,
) -> None:
    authority, registry_path, _ = _files(tmp_path)
    config = _config(authority, registry_path)
    store = _DurableStore()
    store.require_runtime_contract = None
    service = SimpleNamespace(
        store=store,
        authorize=lambda *_args, **_kwargs: True,
    )
    listener_calls: list[object] = []
    monkeypatch.setattr(
        "uvicorn.run", lambda *_args, **_kwargs: listener_calls.append(True)
    )

    with pytest.raises(
        ServerStartupError,
        match="runtime login contract is unavailable",
    ):
        run_server(
            config=config,
            epoch_lease_chain={},
            service=service,
            store=store,
        )

    assert listener_calls == []


def test_missing_handoff_signer_fails_before_login_binding(tmp_path: Path) -> None:
    authority, registry_path, _ = _files(tmp_path)
    config = _config(authority, registry_path, claim_key=False)
    store = _DurableStore()
    with pytest.raises(ServerStartupError, match="claim.*signing key|handoff signer"):
        create_app(
            config=config,
            epoch_lease_chain={},
            store=store,
            action_contracts=yaml.safe_load(registry_path.read_text(encoding="utf-8")),
        )
    assert store.startup_calls == 0


def test_missing_fixed_login_binding_fails_closed(tmp_path: Path) -> None:
    authority, registry_path, _ = _files(tmp_path)
    config = _config(authority, registry_path)
    store = _DurableStore()
    store.startup_login_binding = None
    with pytest.raises(ServerStartupError, match="login-binding"):
        create_app(
            config=config,
            epoch_lease_chain={},
            store=store,
            action_contracts=yaml.safe_load(registry_path.read_text(encoding="utf-8")),
            handoff_signer=lambda _payload: {
                "protected_b64": "a",
                "payload_b64": "b",
                "signature_b64": "c",
                "envelope_digest": "sha256:" + "a" * 64,
            },
        )


def test_missing_verifier_capability_generation_fails_closed_before_fence(
    tmp_path: Path,
) -> None:
    authority, registry_path, _ = _files(tmp_path)
    config = _config(authority, registry_path)
    store = _DurableStore()
    original_binding = store.startup_login_binding

    def missing_generation():
        binding = dict(original_binding())
        binding.pop("verifier_capability_generation")
        return binding

    store.startup_login_binding = missing_generation
    with pytest.raises(ServerStartupError, match="closed projection"):
        create_app(
            config=config,
            epoch_lease_chain={},
            store=store,
            action_contracts=yaml.safe_load(registry_path.read_text(encoding="utf-8")),
            handoff_signer=lambda _payload: {
                "protected_b64": "a",
                "payload_b64": "b",
                "signature_b64": "c",
                "envelope_digest": "sha256:" + "a" * 64,
            },
        )
    assert store.startup_calls == 1


@pytest.mark.parametrize("generation", [0, True, "1"])
def test_invalid_verifier_capability_generation_fails_closed_before_fence(
    tmp_path: Path,
    generation: object,
) -> None:
    authority, registry_path, _ = _files(tmp_path)
    config = _config(authority, registry_path)
    store = _DurableStore()
    original_binding = store.startup_login_binding

    def invalid_generation():
        binding = dict(original_binding())
        binding["verifier_capability_generation"] = generation
        return binding

    store.startup_login_binding = invalid_generation
    with pytest.raises(ServerStartupError, match="invalid verifier capability generation"):
        create_app(
            config=config,
            epoch_lease_chain={},
            store=store,
            action_contracts=yaml.safe_load(registry_path.read_text(encoding="utf-8")),
            handoff_signer=lambda _payload: {
                "protected_b64": "a",
                "payload_b64": "b",
                "signature_b64": "c",
                "envelope_digest": "sha256:" + "a" * 64,
            },
        )
    assert store.startup_calls == 1
