from __future__ import annotations

import hashlib
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography import x509
from cryptography.x509.oid import NameOID
from starlette.testclient import TestClient

from okg.substrate.external_actions.api import ExternalActionAPI
from okg.substrate.external_actions.auth import (
    AuthenticationError,
    MemoryReplayNonceStore,
    authenticate_request,
    build_auth_audit,
    normalized_request_uri,
)
from okg.substrate.external_actions.signing import SigningError, b64url_decode, b64url_encode, canonical_json_bytes


def _dpop_request(uri: str = "https://api.example/v1") -> tuple[list[tuple[str, str]], dict[str, str]]:
    key = Ed25519PrivateKey.generate()
    public = b64url_encode(key.public_key().public_bytes_raw())
    now = int(time.time())
    jkt = b64url_encode(hashlib.sha256(canonical_json_bytes({"crv": "Ed25519", "kty": "OKP", "x": public})).digest())
    head = b64url_encode(canonical_json_bytes({"alg": "EdDSA", "kid": "issuer-key", "typ": "JWT"}))
    body = b64url_encode(canonical_json_bytes({"iss": "https://issuer", "aud": "deployment_scoped_review_api", "sub": "raw-subject", "iat": now, "exp": now + 120, "cnf": {"jkt": jkt}}))
    token = head + "." + body + "." + b64url_encode(key.sign((head + "." + body).encode("ascii")))
    phead = b64url_encode(canonical_json_bytes({"alg": "EdDSA", "typ": "dpop+jwt", "jwk": {"kty": "OKP", "crv": "Ed25519", "x": public}}))
    pbody = b64url_encode(canonical_json_bytes({"htm": "POST", "htu": uri, "iat": now, "jti": "jti-unique-1", "ath": b64url_encode(hashlib.sha256(token.encode()).digest())}))
    proof = phead + "." + pbody + "." + b64url_encode(key.sign((phead + "." + pbody).encode("ascii")))
    return [("authorization", "DPoP " + token), ("dpop", proof)], {"issuer-key": public}


def test_dpop_authentication_maps_to_stable_server_identity_and_fences_nonce() -> None:
    headers, jwks = _dpop_request()
    store = MemoryReplayNonceStore()
    kwargs = dict(headers=headers, method="POST", uri="https://api.example/v1", deployment="support-prod", jwks=jwks, issuer="https://issuer", principal_mapping={"https://issuer\x00raw-subject": {"principal": "oidc:service", "separation_subject": "subject:service", "provider_id": "provider", "subject_binding_id": "binding"}}, nonce_store=store)
    principal = authenticate_request(**kwargs)
    assert principal.principal == "oidc:service"
    audit = build_auth_audit(principal).as_dict()
    assert "access_token" not in audit and "dpop_proof" not in audit
    with pytest.raises(AuthenticationError) as exc:
        authenticate_request(**kwargs)
    assert exc.value.code == "sender_constraint_required"


def test_bearer_without_sender_constraint_is_refused() -> None:
    with pytest.raises(AuthenticationError) as exc:
        authenticate_request(headers={"authorization": "Bearer token"}, method="POST", uri="https://api.example/v1", deployment="d")
    assert exc.value.code == "sender_constraint_required"


def test_dpop_authentication_crosses_the_asgi_boundary_and_replays_fail_closed() -> None:
    uri = "https://api.example/v1/deployments/support-prod/external-actions"
    headers, jwks = _dpop_request(uri)
    principal_mapping = {
        "https://issuer\x00raw-subject": {
            "principal": "oidc:service",
            "separation_subject": "subject:service",
            "provider_id": "provider",
            "subject_binding_id": "binding",
        }
    }

    class Service:
        def submit(self, request):
            assert request.principal.principal == "oidc:service"
            return 201, {}, {"accepted": True}

    config = SimpleNamespace(
        deployment="support-prod",
        service_mode="normal",
        authentication=SimpleNamespace(
            jwks=jwks,
            issuer="https://issuer",
            audience="deployment_scoped_review_api",
            allowed_algorithms=("EdDSA",),
            max_token_lifetime_seconds=300,
            clock_skew_seconds=30,
        ),
        limits=SimpleNamespace(request_body_bytes_maximum=262144),
    )
    app = ExternalActionAPI(Service(), config=config, principal_mapping=principal_mapping, nonce_store=MemoryReplayNonceStore())
    client = TestClient(app, base_url="https://api.example")
    response = client.post("/v1/deployments/support-prod/external-actions", headers=dict(headers), json={})
    assert response.status_code == 201
    replay = client.post("/v1/deployments/support-prod/external-actions", headers=dict(headers), json={})
    assert replay.status_code == 401


def test_noncanonical_base64url_is_rejected() -> None:
    with pytest.raises(SigningError):
        b64url_decode("Zh")


def test_request_uri_normalization_handles_ipv6_and_rejects_userinfo() -> None:
    assert normalized_request_uri("HTTPS://[::1]:443/v1") == "https://[::1]/v1"
    assert normalized_request_uri("https://[::1]:8443/v1") == "https://[::1]:8443/v1"
    assert normalized_request_uri("https://api.example/%41%7e%2f%C3%A9") == "https://api.example/A~%2F%C3%A9"
    with pytest.raises(AuthenticationError):
        normalized_request_uri("https://user@example/v1")


def test_current_provider_binding_barriers_reject_old_jwt_and_mtls_credentials() -> None:
    headers, jwks = _dpop_request()
    now = datetime.now(UTC).replace(microsecond=0)
    mapping = {
        "https://issuer\x00raw-subject": {
            "principal": "oidc:service",
            "separation_subject": "subject:service",
            "provider_id": "provider",
            "subject_binding_id": "binding",
            "provider_status": "active",
            "binding_status": "active",
            "provider_updated_at": (now - timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
            "binding_updated_at": (now + timedelta(seconds=2)).isoformat().replace("+00:00", "Z"),
        }
    }
    # The vector token is issued at ``int(time.time())``.  Move the barrier one
    # second ahead so an otherwise valid token is rejected by durable iat.
    with pytest.raises(AuthenticationError) as exc:
        authenticate_request(
            headers=headers,
            method="POST",
            uri="https://api.example/v1",
            deployment="support-prod",
            jwks=jwks,
            issuer="https://issuer",
            principal_mapping=mapping,
            nonce_store=MemoryReplayNonceStore(),
            require_identity_barrier=True,
            now=time.time(),
        )
    assert exc.value.code == "authentication_required"

    old = {
        "principal": "mtls:worker",
        "separation_subject": "subject:worker",
        "provider_id": "provider",
        "subject_binding_id": "binding",
        "provider_status": "active",
        "binding_status": "active",
        "provider_updated_at": "2026-08-27T00:00:00.000000Z",
        "binding_updated_at": "2026-08-27T00:00:00.000000Z",
        "certificate_not_before": "2026-08-26T23:59:00.000000Z",
        "provider_credential_generation_digest": "generation-new",
        "binding_credential_generation_digest": "generation-new",
        "presented_credential_generation_digest": "generation-old",
        "sender_key_thumbprint": "cert-thumb",
    }
    with pytest.raises(AuthenticationError) as exc:
        authenticate_request(
            headers={},
            method="POST",
            uri="https://api.example/v1",
            deployment="support-prod",
            mtls_principal=old,
            require_identity_barrier=True,
            now=datetime.fromisoformat("2026-08-27T00:01:00+00:00").timestamp(),
        )
    assert exc.value.code == "sender_constraint_required"

    renewed = {
        **old,
        "certificate_not_before": "2026-08-27T00:02:00.000000Z",
        "presented_credential_generation_digest": "generation-new",
    }
    principal = authenticate_request(
        headers={},
        method="POST",
        uri="https://api.example/v1",
        deployment="support-prod",
        mtls_principal=renewed,
        require_identity_barrier=True,
        now=datetime.fromisoformat("2026-08-27T00:03:00+00:00").timestamp(),
    )
    assert principal.auth_method == "mtls"


def test_complete_live_barrier_projection_allows_jwt_and_mtls_authentication() -> None:
    """A resolver projection must retain every field both transports consume.

    The JWT and mTLS paths intentionally use the same provider/binding
    barrier projection.  This catches a resolver that returns only stable
    principal names: authentication would otherwise fail closed after the
    database has successfully mapped the credential.
    """

    digest = "sha256:" + "a" * 64
    headers, jwks = _dpop_request()
    now = int(time.time())
    barrier = datetime.fromtimestamp(now - 10, tz=UTC)
    barrier_value = barrier.isoformat(timespec="microseconds").replace("+00:00", "Z")
    mapping = {
        "https://issuer\x00raw-subject": {
            "principal": "oidc:service",
            "separation_subject": "subject:service",
            "provider_id": "provider",
            "subject_binding_id": "binding",
            "provider_status": "active",
            "binding_status": "active",
            "provider_updated_at": barrier_value,
            "binding_updated_at": barrier_value,
            "provider_credential_generation_digest": digest,
            "binding_credential_generation_digest": digest,
            "credential_generation_digest": digest,
            "presented_credential_generation_digest": digest,
            "provider_certificate_thumbprint": "cert-thumb",
            "binding_certificate_thumbprint": "cert-thumb",
            "certificate_thumbprint": "cert-thumb",
        }
    }

    jwt_principal = authenticate_request(
        headers=headers,
        method="POST",
        uri="https://api.example/v1",
        deployment="support-prod",
        jwks=jwks,
        issuer="https://issuer",
        principal_mapping=mapping,
        nonce_store=MemoryReplayNonceStore(),
        require_identity_barrier=True,
        now=float(now),
    )
    assert jwt_principal.provider_credential_generation_digest == digest
    assert jwt_principal.binding_credential_generation_digest == digest
    assert jwt_principal.provider_updated_at == barrier
    assert jwt_principal.binding_updated_at == barrier

    mtls_principal = authenticate_request(
        headers={},
        method="POST",
        uri="https://api.example/v1",
        deployment="support-prod",
        mtls_principal={
            **mapping["https://issuer\x00raw-subject"],
            "sender_key_thumbprint": "cert-thumb",
            "certificate_not_before": barrier_value,
            "certificate_not_after": datetime.fromtimestamp(now + 120, tz=UTC)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z"),
        },
        require_identity_barrier=True,
        now=float(now),
    )
    assert mtls_principal.auth_method == "mtls"
    assert mtls_principal.presented_credential_generation_digest == digest
    assert mtls_principal.certificate_thumbprint == "cert-thumb"


def test_asgi_mtls_adapter_maps_one_verified_san_without_http_credentials() -> None:
    from okg.substrate.external_actions.auth import ASGIMTLSPrincipalAdapter

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    now = datetime.now(UTC)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "worker")])
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(minutes=5))
        .add_extension(
            x509.SubjectAlternativeName([x509.UniformResourceIdentifier("mtls:worker")]),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    adapter = ASGIMTLSPrincipalAdapter(
        principal_mapping={
            "mtls:worker": {
                "principal": "mtls:worker",
                "separation_subject": "subject:worker",
                "provider_id": "provider",
                "subject_binding_id": "binding",
            }
        },
        require_identity_barrier=False,
    )
    principal = adapter(
        {
            "extensions": {
                "tls": {
                    "verified": True,
                    "client_cert_chain": [certificate.public_bytes(serialization.Encoding.DER)],
                }
            }
        }
    )
    assert principal.auth_method == "mtls"
    assert principal.separation_subject == "subject:worker"
