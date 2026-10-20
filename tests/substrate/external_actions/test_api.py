from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import yaml

from starlette.testclient import TestClient
import pytest
import anyio

from okg.substrate.external_actions.api import ExternalActionAPI, parse_if_match, validate_idempotency_key
from okg.substrate.external_actions.auth import AuthenticationError, VerifiedPrincipal
from okg.substrate.external_actions.config import load_config
from okg.substrate.external_actions.server import ServerStartupError, create_app, run_server
from okg.substrate.external_actions.signing import generate_keypair
from okg.substrate.external_actions.store import InMemoryExternalActionStore
from okg.substrate.external_actions.action_contracts import ActionContract, transition_request_hash


PRINCIPAL = VerifiedPrincipal("principal", "subject:principal", "provider", "binding", auth_method="test", sender_key_thumbprint="thumb")
ROOT = Path(__file__).resolve().parents[3]
VECTORS = ROOT / "tests/fixtures/external_actions/hash-vectors.yaml"


def _startup_config():
    _, public = generate_keypair()
    _, claim_public = generate_keypair()
    return load_config({
        "schema": "okg.external_action_review.config/v1",
        "deployment": "d",
        "authentication": {"audience": "deployment_scoped_review_api", "allowed_algorithms": ["EdDSA"]},
        "signing_keys": [{
            "kid": "d/epoch/1", "purpose": "execution_epoch_lease", "algorithm": "EdDSA",
            "public_key_b64": public, "state": "current",
        }, {
            "kid": "d/claim/1", "purpose": "claim_and_restore_completion", "algorithm": "EdDSA",
            "public_key_b64": claim_public, "state": "current",
        }],
    })


def _startup_contract():
    document = yaml.safe_load(VECTORS.read_text())
    row = next(item for item in document["vectors"] if item["id"] == "action_contract")
    return ActionContract.from_mapping(row["value"])


class _DurableFenceStore(InMemoryExternalActionStore):
    is_durable = True
    deployment = "d"
    connection = object()
    nomos_storage_adapter = object()

    def require_runtime_contract(self):
        return None

    def startup_login_binding(self):
        return {
            "database_system_identifier": "1",
            "session_user": "okg_external_action_d",
            "deployment": "d",
            "binding_version": 1,
            "verifier_capability_generation": 1,
            "bound_at": "2026-08-26T00:00:00.000000Z",
        }

    def insert_nonce(self, **kwargs):
        return True

    def record_auth_audit(self, audit, **metadata):
        return "01J00000000000000000000002"


class Service:
    def submit(self, request):
        assert request.principal == PRINCIPAL
        return 201, {"location": "/v1/deployments/d/external-actions/01J00000000000000000000000"}, {"proposal_id": "01J00000000000000000000000", "review_state": "pending"}


def test_api_is_standalone_and_never_executes_a_provider() -> None:
    app = ExternalActionAPI(Service(), authenticator=lambda **_: PRINCIPAL)
    client = TestClient(app)
    response = client.post("/v1/deployments/d/external-actions", json={"payload": {"safe": True}})
    assert response.status_code == 201
    assert response.json()["review_state"] == "pending"


def test_api_preserves_typed_restore_conflict_without_error_wrapper() -> None:
    class RestoreConflictService:
        def submit(self, request):
            error = RuntimeError("safe typed refusal")
            error.code = "restore_reconciliation_conflict"
            error.status = 409
            error.detail = {"classification": "database_restore_terminal_conflict"}
            raise error

    response = TestClient(
        ExternalActionAPI(RestoreConflictService(), authenticator=lambda **_: PRINCIPAL)
    ).post("/v1/deployments/d/external-actions", json={"payload": {"safe": True}})

    assert response.status_code == 409
    assert response.json()["code"] == "restore_reconciliation_conflict"
    assert response.json()["classification"] == "database_restore_terminal_conflict"
    assert "error" not in response.json()


def test_api_rejects_oversized_stream_before_authentication() -> None:
    called = []
    app = ExternalActionAPI(authenticator=lambda **_: called.append(True))
    client = TestClient(app)
    response = client.post("/v1/deployments/d/external-actions", content=b"x" * 262145)
    assert response.status_code == 413
    assert not called


def test_closed_etag_and_idempotency_grammars() -> None:
    assert parse_if_match('"lifecycle:2;payload-version:1;payload:sha256:' + 'a' * 64 + '"') == (2, 1, "sha256:" + "a" * 64)
    assert len(validate_idempotency_key("propose-run-123456")) >= 16


class _ReadService:
    def __init__(self) -> None:
        self.operations = []

    def authorize(self, operation, actor):
        self.operations.append((operation, actor.principal, actor.separation_subject))
        return True

    def read(self, proposal_id, *, include_payload=False):
        return 200, {"etag": '"read"'}, {"proposal_id": proposal_id, "include_payload": include_payload}

    def timeline(self, proposal_id):
        return ({"proposal_id": proposal_id, "sequence": 1},)

    def list(self, *, deployment=None, state=None):
        return ({"deployment": deployment, "state": state},)


def test_exact_read_routes_authorize_payload_separately_and_emit_headers() -> None:
    service = _ReadService()
    app = ExternalActionAPI(service, authenticator=lambda **_: PRINCIPAL)
    client = TestClient(app)
    proposal_id = "01J00000000000000000000000"

    read = client.get(f"/v1/deployments/d/external-actions/{proposal_id}")
    assert read.status_code == 200 and read.headers["etag"] == '"read"'
    payload = client.get(f"/v1/deployments/d/external-actions/{proposal_id}/payload")
    assert payload.status_code == 200
    timeline = client.get(f"/v1/deployments/d/external-actions/{proposal_id}/timeline")
    assert timeline.status_code == 200
    listed = client.get("/v1/deployments/d/external-actions?state=pending&limit=1")
    assert listed.status_code == 200
    assert [item[0] for item in service.operations] == [
        "external_action.read", "external_action.read_payload", "external_action.read", "external_action.read",
    ]


def test_reads_use_the_service_authorization_boundary_when_available() -> None:
    class BoundaryReadService(_ReadService):
        def __init__(self) -> None:
            super().__init__()
            self.boundary_operations = []

        def _authorize(self, operation, actor, *, context=None):
            self.boundary_operations.append((operation, actor, context))
            return actor

    service = BoundaryReadService()
    app = ExternalActionAPI(service, authenticator=lambda **_: PRINCIPAL)
    response = TestClient(app).get(
        "/v1/deployments/d/external-actions/01J00000000000000000000000"
    )
    assert response.status_code == 200
    operation, actor, context = service.boundary_operations[0]
    assert operation == "external_action.read"
    assert actor.nomos_provider_id == PRINCIPAL.provider_id
    assert actor.nomos_subject_binding_id == PRINCIPAL.subject_binding_id
    assert context["request_id"]


def test_denied_read_is_the_same_not_found_shape_as_unknown_id() -> None:
    from okg.substrate.external_actions.service import ExternalActionError

    proposal = "01J00000000000000000000000"

    class DeniedReadService(_ReadService):
        def _authorize(self, operation, actor, *, context=None):
            raise ExternalActionError("unauthorized", "hidden", status=403)

    denied = TestClient(
        ExternalActionAPI(DeniedReadService(), authenticator=lambda **_: PRINCIPAL)
    ).get(f"/v1/deployments/d/external-actions/{proposal}")
    assert denied.status_code == 404
    assert denied.json()["code"] == "proposal_not_found"

    class UnknownReadService(_ReadService):
        def read(self, proposal_id, *, include_payload=False):
            raise ExternalActionError("proposal_not_found", "missing", status=404)

    unknown = TestClient(
        ExternalActionAPI(UnknownReadService(), authenticator=lambda **_: PRINCIPAL)
    ).get(f"/v1/deployments/d/external-actions/{proposal}")
    assert unknown.status_code == 404
    assert unknown.json()["code"] == "proposal_not_found"


def test_read_authorization_and_projection_share_one_store_lock() -> None:
    class TrackingLock:
        depth = 0

        def __enter__(self):
            self.depth += 1
            return self

        def __exit__(self, *_args):
            self.depth -= 1

    class LockedReadService(_ReadService):
        def __init__(self) -> None:
            super().__init__()
            self.store = SimpleNamespace(lock=TrackingLock(), is_durable=True)

        def _authorize(self, operation, actor, *, context=None):
            assert self.store.lock.depth == 1
            return actor

        def read(self, proposal_id, *, include_payload=False):
            assert self.store.lock.depth == 1
            return super().read(proposal_id, include_payload=include_payload)

    service = LockedReadService()
    response = TestClient(
        ExternalActionAPI(service, authenticator=lambda **_: PRINCIPAL)
    ).get("/v1/deployments/d/external-actions/01J00000000000000000000000")

    assert response.status_code == 200
    assert service.store.lock.depth == 0


def test_full_payload_uses_the_service_canonical_response_and_auth_audit_mapping() -> None:
    class PayloadService(_ReadService):
        def read(self, proposal_id, *, include_payload=False):
            if include_payload:
                return SimpleNamespace(
                    status=200,
                    replay=False,
                    value=SimpleNamespace(),
                    response_body=b'{"canonical_payload":"{\\"ticket\\":\\"T-1\\"}","etag":"\\"read\\""}',
                )
            return super().read(proposal_id, include_payload=include_payload)

    service = PayloadService()
    app = ExternalActionAPI(
        service,
        authenticator=lambda **_: PRINCIPAL,
        auth_audit_writer=lambda **_: {"authentication_audit_id": "auth:audit:1"},
    )
    client = TestClient(app)
    response = client.get("/v1/deployments/d/external-actions/01J00000000000000000000000/payload")
    assert response.status_code == 200
    assert response.json()["canonical_payload"] == '{"ticket":"T-1"}'
    assert response.headers["etag"] == '"read"'


def test_durable_authorized_read_forwards_canonical_digest_without_snapshot() -> None:
    captured = []

    class DurableReadService:
        store = SimpleNamespace(is_durable=True)

        def _authorize(self, operation, actor, *, context=None):
            captured.append(("authorize", operation, context))
            return actor

        def authorized_read(self, **kwargs):
            self._authorize(
                "external_action.read_payload" if kwargs["operation"] == "read_payload" else "external_action.read",
                kwargs["actor"],
                context={"request_hash": kwargs["request_hash"]},
            )
            captured.append(("read", kwargs))
            return 200, {}, {
                "proposal": {"proposal_id": kwargs["proposal_id"], "etag": '"read"'},
                "canonical_payload": '{"safe":true}',
            }

    proposal_id = "01J00000000000000000000000"
    response = TestClient(
        ExternalActionAPI(DurableReadService(), authenticator=lambda **_: PRINCIPAL)
    ).get(f"/v1/deployments/d/external-actions/{proposal_id}/payload")

    assert response.status_code == 200
    assert response.json()["canonical_payload"] == '{"safe":true}'
    assert captured[0][0] == "authorize"
    assert captured[1][0] == "read"
    kwargs = captured[1][1]
    expected_hash = transition_request_hash(
        {"operation": "read_payload", "resource": proposal_id, "include_payload": True}
    )
    assert captured[0][2]["request_hash"] == expected_hash
    assert kwargs["request_hash"] == expected_hash


def test_mutation_replay_uses_the_stored_canonical_response_not_current_state() -> None:
    historical = b'{"etag":"\\"lifecycle:1;payload-version:1;payload:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\\"","review_state":"pending"}'
    current = SimpleNamespace(
        to_projection=lambda: {
            "etag": '"lifecycle:3;payload-version:2;payload:sha256:' + "b" * 64 + '"',
            "review_state": "approved",
        }
    )
    result = SimpleNamespace(
        status=200,
        replay=True,
        response_body=historical,
        value=current,
    )

    status, headers, projection = ExternalActionAPI._normalize_service_result(result)

    assert status == 200
    assert headers == {
        "etag": '"lifecycle:1;payload-version:1;payload:sha256:' + "a" * 64 + '"',
        "idempotent-replay": "true",
    }
    assert projection["review_state"] == "pending"


def test_claim_dispatch_passes_if_match_and_all_worker_support_bindings() -> None:
    captured = {}

    class ClaimService:
        def claim(self, **kwargs):
            captured.update(kwargs)
            return 201, {}, {"proposal_id": kwargs["proposal_id"], "execution_state": "claimed"}

    app = ExternalActionAPI(ClaimService(), authenticator=lambda **_: PRINCIPAL)
    client = TestClient(app)
    proposal_id = "01J00000000000000000000000"
    digest = "sha256:" + "a" * 64
    body = {
        "decision_id": proposal_id,
        "approved_lifecycle_version": 3,
        "approved_payload_version": 2,
        "approved_payload_hash": digest,
        "supported_action_schema_digest": digest,
        "supported_action_contract_digest": digest,
        "supported_executor_contract_digest": digest,
        "supported_payload_renderer_digest": digest,
        "supported_governed_tool_binding_digest": digest,
        "supported_provider_connection_contract_digest": digest,
        "supported_provider_capability_digest": digest,
        "supported_provider_operation_id": "ticket.update",
        "supported_provider_operation_descriptor_digest": digest,
    }
    etag = '"lifecycle:3;payload-version:2;payload:' + digest + '"'
    response = client.post(
        f"/v1/deployments/d/external-actions/{proposal_id}/execution-claims",
        json=body,
        headers={"Idempotency-Key": "worker-claim-dispatch-417", "If-Match": etag},
    )
    assert response.status_code == 201
    assert captured["if_match"] == etag
    assert captured["supported_provider_operation_id"] == "ticket.update"
    assert len([key for key in captured if key.startswith("supported_")]) == 9


def test_report_dispatch_derives_reconciler_from_retained_claim_owner() -> None:
    captured = {}
    execution_id = "01J00000000000000000000001"
    proposal_id = "01J00000000000000000000000"

    class Store:
        def get_execution(self, value):
            assert value == proposal_id
            return SimpleNamespace(execution_id=execution_id, worker_separation_subject="subject:claim-owner")

    class ReportService:
        store = Store()

        def report_outcome(self, **kwargs):
            captured.update(kwargs)
            return 201, {}, {"execution_id": kwargs["execution_id"], "outcome": "indeterminate"}

    app = ExternalActionAPI(ReportService(), authenticator=lambda **_: PRINCIPAL)
    client = TestClient(app)
    response = client.post(
        f"/v1/deployments/d/external-actions/{proposal_id}/execution-claims/{execution_id}/outcome",
        json={
            "outcome": "indeterminate",
            "outcome_reason_code": "claim_owner_unavailable",
            "recovery_journal_digest": "sha256:" + "b" * 64,
        },
        headers={
            "Idempotency-Key": "reconciler-report-dispatch-417",
            "If-Match": '"lifecycle:4;payload-version:2;payload:sha256:' + "a" * 64 + '"',
        },
    )
    assert response.status_code == 201
    assert captured["if_match"].startswith('"lifecycle:4;')
    assert captured["reconciler"] is True


def test_static_bearer_is_explicit_loopback_only_and_out_of_band() -> None:
    config = SimpleNamespace(
        deployment="d",
        authentication=SimpleNamespace(
            static_bearer=True,
            development_loopback_only=True,
            jwks={},
            issuer=None,
            audience="deployment_scoped_review_api",
            allowed_algorithms=("EdDSA",),
            max_token_lifetime_seconds=300,
            clock_skew_seconds=30,
            trusted_proxy_names=(),
        ),
    )
    app = ExternalActionAPI(
        config=config,
        static_bearer_secret="development-secret-417",
        static_bearer_principal=PRINCIPAL,
    )
    headers = (("authorization", "Bearer development-secret-417"),)
    scope = {"client": ("127.0.0.1", 1), "scheme": "https", "headers": [(b"host", b"localhost")]}
    principal = anyio.run(app._authenticate, headers, "GET", scope, "/health")
    assert principal.principal == PRINCIPAL.principal

    with pytest.raises(AuthenticationError):
        anyio.run(app._authenticate, headers, "GET", {**scope, "client": ("192.0.2.1", 1)}, "/health")
    with pytest.raises(ValueError):
        ExternalActionAPI(config=config, static_bearer_secret="secret")


def test_read_body_is_not_an_implicit_request_envelope() -> None:
    app = ExternalActionAPI(_ReadService(), authenticator=lambda **_: PRINCIPAL)
    client = TestClient(app)
    response = client.request(
        "GET",
        "/v1/deployments/d/external-actions/01J00000000000000000000000",
        content=b"{}",
        headers={"content-type": "application/json"},
    )
    assert response.status_code == 400


def test_route_deployment_and_path_grammar_are_closed() -> None:
    app = ExternalActionAPI(_ReadService(), config=SimpleNamespace(deployment="d"), authenticator=lambda **_: PRINCIPAL)
    client = TestClient(app)
    assert client.get("/v1/deployments/other/external-actions").status_code == 404
    assert client.get("/v1/deployments/d/external-actions/not-a-ulid").status_code == 400
    assert client.get("/v1/deployments/d/external-actions/01J00000000000000000000000/").status_code == 404
    assert client.get("/v1/deployments/d/external-actions/01J00000000000000000000000?state=pending").status_code == 400


def test_server_refuses_to_start_without_the_prebind_inputs() -> None:
    with pytest.raises(ServerStartupError):
        run_server()


def test_route_table_covers_the_full_review_and_restore_surface() -> None:
    app = ExternalActionAPI(_ReadService(), config=SimpleNamespace(deployment="d"))
    proposal = "01J00000000000000000000000"
    execution = "01J00000000000000000000001"
    routes = {
        ("POST", "/v1/deployments/d/external-actions"): "submit",
        ("GET", "/v1/deployments/d/external-actions"): "list",
        ("GET", f"/v1/deployments/d/external-actions/{proposal}"): "read",
        ("GET", f"/v1/deployments/d/external-actions/{proposal}/payload"): "read_payload",
        ("GET", f"/v1/deployments/d/external-actions/{proposal}/timeline"): "timeline",
        ("POST", f"/v1/deployments/d/external-actions/{proposal}/edits"): "edit",
        ("POST", f"/v1/deployments/d/external-actions/{proposal}/decisions"): "decide",
        ("POST", f"/v1/deployments/d/external-actions/{proposal}/cancel"): "cancel",
        ("POST", f"/v1/deployments/d/external-actions/{proposal}/execution-claims"): "claim",
        ("POST", f"/v1/deployments/d/external-actions/{proposal}/execution-claims/{execution}/owner-unavailability-attestations"): "attest_owner_unavailable",
        ("POST", f"/v1/deployments/d/external-actions/{proposal}/execution-claims/{execution}/outcome"): "report",
        ("GET", "/v1/deployments/d/external-actions/recovery-records/sha256:" + "a" * 64): "recovery_record",
        ("POST", "/v1/deployments/d/external-actions/restore-reconciliations"): "restore_create",
        ("PUT", "/v1/deployments/d/external-actions/restore-reconciliations/r1/checkpoint-batches/0"): "restore_batch",
        ("POST", "/v1/deployments/d/external-actions/restore-reconciliations/r1/finalize"): "restore_finalize",
    }
    for (method, path), operation in routes.items():
        assert app._route(method, path).operation == operation


def test_owner_unavailability_attestation_uses_idempotency_only() -> None:
    captured = {}

    class OwnerAttestationService:
        def create_owner_unavailability_attestation(self, **kwargs):
            captured.update(kwargs)
            return 201, {}, {"execution_id": kwargs["execution_id"], "status": "recorded"}

    proposal = "01J00000000000000000000000"
    execution = "01J00000000000000000000001"
    app = ExternalActionAPI(OwnerAttestationService(), authenticator=lambda **_: PRINCIPAL)
    response = TestClient(app).post(
        f"/v1/deployments/d/external-actions/{proposal}/execution-claims/{execution}/owner-unavailability-attestations",
        json={},
        headers={"Idempotency-Key": "owner-attestation-dispatch-417"},
    )
    assert response.status_code == 201
    assert captured["idempotency_key"] == "owner-attestation-dispatch-417"


def test_restore_only_reaches_an_exact_mutation_replay_but_redacts_claim_payload() -> None:
    """A quarantine may converge a committed retry, never create a mutation."""

    from okg.substrate.external_actions.signing import domain_digest

    class DurableStore(InMemoryExternalActionStore):
        is_durable = True
        deployment = "d"

    key = "restore-only-replay-key-417"
    key_hash = domain_digest("okg-external-action-idempotency-key-v1", key)
    store = DurableStore()
    store.submit_idempotency[("d", PRINCIPAL.separation_subject, key_hash)] = (
        "01J00000000000000000000000",
        "sha256:" + "a" * 64,
    )
    calls = []

    class ReplayService:
        def __init__(self) -> None:
            self.store = store

        def submit(self, request):
            calls.append(request)
            return SimpleNamespace(
                status=200,
                replay=True,
                value={},
                response_body=b'{"proposal_id":"01J00000000000000000000000","canonical_payload":"{\\"safe\\":true}"}',
            )

    app = ExternalActionAPI(
        ReplayService(),
        mode_provider=lambda: "restore_only",
        authenticator=lambda **_: PRINCIPAL,
    )
    response = TestClient(app).post(
        "/v1/deployments/d/external-actions",
        json={"payload": {"safe": True}},
        headers={"Idempotency-Key": key},
    )
    assert response.status_code == 200
    assert response.headers["idempotent-replay"] == "true"
    assert response.json() == {"proposal_id": "01J00000000000000000000000"}
    assert len(calls) == 1

    blocked = TestClient(app).post(
        "/v1/deployments/d/external-actions",
        json={"payload": {"safe": True}},
        headers={"Idempotency-Key": "restore-only-new-key-417"},
    )
    assert blocked.status_code == 409
    assert blocked.json()["code"] == "external_action_restore_quarantined"
    assert len(calls) == 1


def test_authentication_audit_is_deferred_until_after_nomos() -> None:
    audit_calls = []
    requests = []

    class DeferredService:
        def submit(self, request):
            requests.append(request)
            return 201, {}, {"proposal_id": "01J00000000000000000000000", "review_state": "pending"}

    app = ExternalActionAPI(
        DeferredService(),
        authenticator=lambda **_: PRINCIPAL,
        auth_audit_writer=lambda *args, **kwargs: audit_calls.append((args, kwargs)),
        auth_config_digest="sha256:" + "a" * 64,
    )
    response = TestClient(app).post(
        "/v1/deployments/d/external-actions",
        json={"payload": {"safe": True}},
    )
    assert response.status_code == 201
    assert audit_calls == []
    request = requests[0]
    assert request.auth_audit["request_id"] == request.request_id
    actor = app._actor(request, None)
    assert actor.authentication_audit_ref is None
    assert actor.authentication_audit["auth_config_digest"] == "sha256:" + "a" * 64
    assert actor.nomos_provider_id == PRINCIPAL.provider_id
    assert actor.nomos_subject_binding_id == PRINCIPAL.subject_binding_id


def test_create_app_auto_installs_durable_nomos_authorizer(monkeypatch) -> None:
    from okg.substrate.external_actions import server as server_module

    monkeypatch.setattr(
        server_module,
        "pre_bind_restore_fence",
        lambda *args, **kwargs: SimpleNamespace(service_mode="normal"),
    )
    contract = _startup_contract()
    monkeypatch.setattr(
        server_module,
        "_configured_product_authority",
        lambda *_args, **_kwargs: (object(), object(), object()),
    )
    monkeypatch.setattr(
        server_module,
        "_compiled_action_registry",
        lambda *_args, **_kwargs: {contract.action_type: contract},
    )
    monkeypatch.setattr(
        server_module,
        "_handoff_signer",
        lambda loaded, **_kwargs: (
            next(
                key
                for key in loaded.signing_keys
                if key.purpose == "claim_and_restore_completion"
            ),
            lambda _payload: {},
        ),
    )
    store = _DurableFenceStore()
    app = create_app(
        config=_startup_config(),
        epoch_lease_chain={},
        store=store,
        action_contracts={"actions": []},
    )
    assert app.service.store is store
    assert app.service.authorize.__class__.__name__ == "DurableNomosAuthorizer"
    assert app.listener_bound is False


def test_create_app_refuses_a_durable_service_without_strict_authorizer(monkeypatch) -> None:
    from okg.substrate.external_actions import server as server_module

    monkeypatch.setattr(
        server_module,
        "pre_bind_restore_fence",
        lambda *args, **kwargs: SimpleNamespace(service_mode="normal"),
    )
    contract = _startup_contract()
    monkeypatch.setattr(
        server_module,
        "_configured_product_authority",
        lambda *_args, **_kwargs: (object(), object(), object()),
    )
    monkeypatch.setattr(
        server_module,
        "_compiled_action_registry",
        lambda *_args, **_kwargs: {contract.action_type: contract},
    )
    monkeypatch.setattr(
        server_module,
        "_handoff_signer",
        lambda loaded, **_kwargs: (
            next(
                key
                for key in loaded.signing_keys
                if key.purpose == "claim_and_restore_completion"
            ),
            lambda _payload: {},
        ),
    )
    store = _DurableFenceStore()
    service = SimpleNamespace(store=store, authorize=None)
    with pytest.raises(ServerStartupError, match="authorizer"):
        create_app(
            config=_startup_config(),
            epoch_lease_chain={},
            service=service,
            store=store,
            action_contracts={"actions": []},
        )


def test_create_app_replaces_supplied_service_registry_with_verified_registry(
    monkeypatch,
) -> None:
    from okg.substrate.external_actions import server as server_module

    monkeypatch.setattr(
        server_module,
        "pre_bind_restore_fence",
        lambda *args, **kwargs: SimpleNamespace(service_mode="normal"),
    )
    contract = _startup_contract()
    monkeypatch.setattr(
        server_module,
        "_configured_product_authority",
        lambda *_args, **_kwargs: (object(), object(), object()),
    )
    monkeypatch.setattr(
        server_module,
        "_compiled_action_registry",
        lambda *_args, **_kwargs: {contract.action_type: contract},
    )
    monkeypatch.setattr(
        server_module,
        "_handoff_signer",
        lambda loaded, **_kwargs: (
            next(
                key
                for key in loaded.signing_keys
                if key.purpose == "claim_and_restore_completion"
            ),
            lambda _payload: {},
        ),
    )
    store = _DurableFenceStore()
    service = SimpleNamespace(
        store=store,
        authorize=lambda *_args, **_kwargs: True,
        action_contracts={"caller.selected": object()},
    )

    app = create_app(
        config=_startup_config(),
        epoch_lease_chain={},
        service=service,
        store=store,
        action_contracts={"actions": []},
    )

    assert app.service.action_contracts == {contract.action_type: contract}
