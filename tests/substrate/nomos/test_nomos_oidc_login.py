from __future__ import annotations

import json
import socket
import time
from concurrent.futures import ThreadPoolExecutor

import jwt
import pytest
import requests
from cryptography.hazmat.primitives.asymmetric import rsa

from okg.substrate.nomos.oidc_login import (
    OIDCLoginError,
    build_nomos_claim_from_verified_claims,
    capture_loopback_callback,
    discover_oidc_metadata,
    generate_pkce_pair,
    requests as oidc_requests,
    sanitize_oidc_claims,
    verify_oidc_id_token,
)


def test_generate_pkce_pair_uses_s256_shape() -> None:
    verifier, challenge = generate_pkce_pair()

    assert 43 <= len(verifier) <= 128
    assert len(challenge) == 43
    assert "=" not in challenge
    assert verifier != challenge


def test_build_claim_from_verified_claims_redacts_tokens() -> None:
    result = build_nomos_claim_from_verified_claims(
        {
            "iss": "https://issuer.example",
            "sub": "scientist-1",
            "aud": ["akmon-lab"],
            "exp": 1_800_000_000,
            "iat": 1_799_999_000,
            "nonce": "expected-nonce",
            "acr": "mfa",
            "cern_roles": ["cms-operator"],
            "id_token": "raw-id-token",
            "access_token": "raw-access-token",
            "refresh_token": "raw-refresh-token",
        },
        provider_id="cern-sso",
        provider_type="oidc",
        verified_by="verifier:cern-sso-gateway",
        verification_method="oidc-jwks",
        expected_nonce="expected-nonce",
    )

    encoded = json.dumps(result.claim, sort_keys=True)
    assert result.external_subject == "scientist-1"
    assert result.audiences == ("akmon-lab",)
    assert result.claim["verified_by"] == "verifier:cern-sso-gateway"
    assert result.claim["claim_fingerprint"].startswith("sha256:")
    assert "cern_roles" in result.claim["claims"]
    assert "nonce" not in result.claim["claims"]
    assert "id_token" not in encoded
    assert "access_token" not in encoded
    assert "refresh_token" not in encoded
    assert "raw-" not in encoded


def test_sanitize_oidc_claims_keeps_only_bounded_keys() -> None:
    sanitized = sanitize_oidc_claims(
        {
            "sub": "user-1",
            "aud": "client-1",
            "custom_scope": "needed",
            "ignored": "not-needed",
            "nested": {"access_token": "raw-token"},
        },
        include_claims=("custom_scope", "nested"),
    )

    assert sanitized == {
        "aud": "client-1",
        "custom_scope": "needed",
        "nested": {},
        "sub": "user-1",
    }


def test_verify_oidc_id_token_rejects_nonce_mismatch() -> None:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()
    now = int(time.time())
    token = jwt.encode(
        {
            "iss": "https://issuer.example",
            "sub": "scientist-1",
            "aud": "akmon-lab",
            "exp": now + 600,
            "iat": now,
            "nonce": "actual-nonce",
        },
        private_key,
        algorithm="RS256",
        headers={"kid": "test-key"},
    )

    class FakeSigningKey:
        key = public_key

    class FakeJWKClient:
        def __init__(self, jwks_uri: str) -> None:
            self.jwks_uri = jwks_uri

        def get_signing_key_from_jwt(self, token: str) -> FakeSigningKey:
            return FakeSigningKey()

    claims = verify_oidc_id_token(
        token,
        metadata={
            "issuer": "https://issuer.example",
            "jwks_uri": "https://issuer.example/jwks",
            "id_token_signing_alg_values_supported": ["RS256"],
        },
        client_id="akmon-lab",
        nonce="actual-nonce",
        jwk_client_factory=FakeJWKClient,
    )
    assert claims["sub"] == "scientist-1"

    with pytest.raises(OIDCLoginError, match="nonce mismatch"):
        verify_oidc_id_token(
            token,
            metadata={
                "issuer": "https://issuer.example",
                "jwks_uri": "https://issuer.example/jwks",
                "id_token_signing_alg_values_supported": ["RS256"],
            },
            client_id="akmon-lab",
            nonce="wrong-nonce",
            jwk_client_factory=FakeJWKClient,
        )


def test_capture_loopback_callback_rejects_state_mismatch() -> None:
    port = _free_port()
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(
            capture_loopback_callback,
            host="127.0.0.1",
            port=port,
            path="/nomos/oidc/callback",
            expected_state="expected-state",
            timeout_seconds=2,
        )
        response = _get_with_retry(
            f"http://127.0.0.1:{port}/nomos/oidc/callback",
            params={"code": "code-1", "state": "wrong-state"},
        )

    assert response.status_code == 400
    with pytest.raises(OIDCLoginError, match="state_mismatch"):
        future.result()


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _get_with_retry(
    url: str,
    *,
    params: dict[str, str],
) -> requests.Response:
    deadline = time.time() + 2
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            return requests.get(url, params=params, timeout=2)
        except requests.RequestException as exc:
            last_error = exc
            time.sleep(0.02)
    raise AssertionError("server did not accept callback") from last_error


def test_discover_oidc_metadata_refuses_redirect(monkeypatch):
    # A 3xx from the discovery endpoint must NOT be followed and its body must
    # NOT be parsed (allow_redirects=False + treat 3xx as failure). Adversarial-
    # review remediation: a redirect body was being fed to response.json().
    from types import SimpleNamespace

    monkeypatch.setattr(
        "okg.substrate.library.sources._url_safety.socket.getaddrinfo",
        lambda *a, **k: [(2, 1, 6, "", ("93.184.216.34", 443))],
    )
    captured: dict = {}

    def fake_get(url, **kw):
        captured["allow_redirects"] = kw.get("allow_redirects")

        def _boom():
            raise AssertionError("redirect body must not be parsed as JSON")

        return SimpleNamespace(
            status_code=302,
            headers={"Location": "http://169.254.169.254/"},
            json=_boom,
            raise_for_status=lambda: None,
        )

    monkeypatch.setattr(oidc_requests, "get", fake_get)
    with pytest.raises(OIDCLoginError):
        discover_oidc_metadata("https://issuer.example")
    assert captured["allow_redirects"] is False
