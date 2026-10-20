"""Interactive OIDC login verifier for Nomos institutional IAM tests.

This module is intentionally a verifier harness, not an OAuth token
store. It performs one browser-based Authorization Code + PKCE login,
verifies the ID token, and emits a bounded Nomos claim envelope. Raw
tokens stay in process memory and are never returned to callers.
"""
from __future__ import annotations

import base64
import hashlib
import os
import secrets
import urllib.parse
import webbrowser
from dataclasses import dataclass
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Callable, Mapping, Protocol, Sequence

import jwt
import requests
from jwt import PyJWKClient

from okg.substrate.library.sources._url_safety import assert_public_http_url
from okg.substrate.nomos.identity import SENSITIVE_CLAIM_KEYS, VALID_CLAIM_STATUS


DEFAULT_OIDC_CLAIM_ALLOWLIST = frozenset({
    "acr",
    "affiliations",
    "amr",
    "aud",
    "auth_time",
    "cern_identity_id",
    "cern_mail_upn",
    "cern_person_id",
    "cern_roles",
    "cern_roles_missing_loa",
    "cern_roles_missing_mfa",
    "cern_uid",
    "cern_upn",
    "email",
    "email_verified",
    "entitlements",
    "exp",
    "family_name",
    "given_name",
    "groups",
    "iat",
    "iss",
    "kerberos_username",
    "name",
    "nbf",
    "preferred_username",
    "realm",
    "resource_access",
    "roles",
    "scope",
    "sub",
})

RESERVED_ID_TOKEN_CLAIMS = frozenset({
    "at_hash",
    "azp",
    "c_hash",
    "nonce",
    "s_hash",
    "sid",
})

ASYMMETRIC_ID_TOKEN_ALGORITHMS = frozenset({
    "RS256",
    "RS384",
    "RS512",
    "ES256",
    "ES384",
    "ES512",
    "PS256",
    "PS384",
    "PS512",
})


class OIDCLoginError(ValueError):
    """Raised when the local OIDC login verifier fails closed."""


class JWKClientLike(Protocol):
    def get_signing_key_from_jwt(self, token: str) -> Any:
        """Return an object with a `key` attribute accepted by PyJWT."""


@dataclass(frozen=True)
class OIDCLoginConfig:
    issuer: str
    client_id: str
    provider_id: str
    provider_type: str
    verified_by: str
    scopes: tuple[str, ...] = ("openid", "profile", "email")
    client_secret_env: str | None = None
    client_auth_method: str = "auto"
    redirect_host: str = "127.0.0.1"
    redirect_port: int = 8765
    redirect_path: str = "/nomos/oidc/callback"
    timeout_seconds: int = 300
    fetch_userinfo: bool = False
    open_browser: bool = True
    include_claims: tuple[str, ...] = ()
    leeway_seconds: int = 60

    @property
    def redirect_uri(self) -> str:
        path = self.redirect_path if self.redirect_path.startswith("/") else (
            "/" + self.redirect_path
        )
        return f"http://{self.redirect_host}:{self.redirect_port}{path}"


@dataclass(frozen=True)
class OIDCLoginResult:
    claim: dict[str, Any]
    issuer: str
    provider_id: str
    provider_type: str
    external_subject: str
    audiences: tuple[str, ...]
    included_claim_keys: tuple[str, ...]
    redirect_uri: str

    def summary(self, *, claim_path: str | None = None) -> dict[str, Any]:
        payload = {
            "status": "ok",
            "provider_id": self.provider_id,
            "provider_type": self.provider_type,
            "issuer": self.issuer,
            "external_subject": self.external_subject,
            "audiences": list(self.audiences),
            "included_claim_keys": list(self.included_claim_keys),
            "redirect_uri": self.redirect_uri,
        }
        if claim_path:
            payload["claim"] = claim_path
        return payload


def run_oidc_loopback_login(
    config: OIDCLoginConfig,
    *,
    status_callback: Callable[[str], None] | None = None,
) -> OIDCLoginResult:
    """Run one interactive OIDC login and return a bounded Nomos claim."""
    _validate_config(config)
    metadata = discover_oidc_metadata(config.issuer)
    verifier, challenge = generate_pkce_pair()
    state = secrets.token_urlsafe(24)
    nonce = secrets.token_urlsafe(24)
    auth_url = build_authorization_url(
        metadata,
        client_id=config.client_id,
        redirect_uri=config.redirect_uri,
        scopes=config.scopes,
        state=state,
        nonce=nonce,
        code_challenge=challenge,
    )
    _status(
        status_callback,
        f"Waiting for OIDC callback on {config.redirect_uri}",
    )
    if config.open_browser:
        opened = webbrowser.open(auth_url)
        if not opened:
            _status(status_callback, f"Open this URL in your browser: {auth_url}")
    else:
        _status(status_callback, f"Open this URL in your browser: {auth_url}")

    callback = capture_loopback_callback(
        host=config.redirect_host,
        port=config.redirect_port,
        path=config.redirect_path,
        expected_state=state,
        timeout_seconds=config.timeout_seconds,
    )
    tokens = exchange_authorization_code(
        metadata,
        code=callback["code"],
        client_id=config.client_id,
        client_secret=_client_secret(config.client_secret_env),
        client_auth_method=config.client_auth_method,
        code_verifier=verifier,
        redirect_uri=config.redirect_uri,
        timeout_seconds=config.timeout_seconds,
    )
    id_token = tokens.get("id_token")
    if not isinstance(id_token, str) or not id_token:
        raise OIDCLoginError("OIDC token response did not include an ID token")
    verified_claims = verify_oidc_id_token(
        id_token,
        metadata=metadata,
        client_id=config.client_id,
        nonce=nonce,
        leeway_seconds=config.leeway_seconds,
    )
    if config.fetch_userinfo:
        verified_claims = merge_userinfo_claims(
            verified_claims,
            metadata=metadata,
            access_token=tokens.get("access_token"),
            timeout_seconds=config.timeout_seconds,
        )
    return build_nomos_claim_from_verified_claims(
        verified_claims,
        provider_id=config.provider_id,
        provider_type=config.provider_type,
        verified_by=config.verified_by,
        verification_method="oidc-jwks",
        include_claims=config.include_claims,
        expected_nonce=nonce,
        issuer=str(metadata["issuer"]),
        redirect_uri=config.redirect_uri,
    )


def generate_pkce_pair() -> tuple[str, str]:
    verifier = secrets.token_urlsafe(64)
    if len(verifier) > 128:
        verifier = verifier[:128]
    if len(verifier) < 43:
        verifier = verifier + secrets.token_urlsafe(16)
    challenge = _base64url(hashlib.sha256(verifier.encode("ascii")).digest())
    return verifier, challenge


def discover_oidc_metadata(
    issuer: str,
    *,
    timeout_seconds: int = 30,
) -> dict[str, Any]:
    normalized_issuer = issuer.rstrip("/")
    if not normalized_issuer.startswith("https://"):
        raise OIDCLoginError("OIDC issuer must be an https URL")
    url = f"{normalized_issuer}/.well-known/openid-configuration"
    try:
        assert_public_http_url(url)  # SSRF guard on the operator issuer
        response = requests.get(url, timeout=timeout_seconds, allow_redirects=False)
        if 300 <= response.status_code < 400:
            raise OIDCLoginError(
                "OIDC discovery endpoint returned a redirect; not followed"
            )
        response.raise_for_status()
        metadata = response.json()
    except requests.RequestException as exc:
        raise OIDCLoginError("OIDC discovery metadata fetch failed") from exc
    except ValueError as exc:
        raise OIDCLoginError("OIDC discovery metadata was not valid JSON") from exc
    if not isinstance(metadata, dict):
        raise OIDCLoginError("OIDC discovery metadata must be a JSON object")
    required = (
        "issuer",
        "authorization_endpoint",
        "token_endpoint",
        "jwks_uri",
    )
    missing = [field for field in required if not metadata.get(field)]
    if missing:
        raise OIDCLoginError(
            "OIDC discovery metadata is missing required fields: "
            + ", ".join(sorted(missing))
        )
    discovered_issuer = str(metadata["issuer"]).rstrip("/")
    if discovered_issuer != normalized_issuer:
        raise OIDCLoginError("OIDC discovery issuer did not match requested issuer")
    return dict(metadata)


def build_authorization_url(
    metadata: Mapping[str, Any],
    *,
    client_id: str,
    redirect_uri: str,
    scopes: Sequence[str],
    state: str,
    nonce: str,
    code_challenge: str,
) -> str:
    endpoint = _required_metadata_url(metadata, "authorization_endpoint")
    if "openid" not in set(scopes):
        raise OIDCLoginError("OIDC scopes must include 'openid'")
    params = {
        "client_id": client_id,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
        "nonce": nonce,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": " ".join(scopes),
        "state": state,
    }
    return f"{endpoint}?{urllib.parse.urlencode(params)}"


def capture_loopback_callback(
    *,
    host: str,
    port: int,
    path: str,
    expected_state: str,
    timeout_seconds: int,
) -> dict[str, str]:
    captured: dict[str, str] = {}
    callback_path = path if path.startswith("/") else "/" + path

    class CallbackHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urllib.parse.urlparse(self.path)
            params = {
                key: values[0]
                for key, values in urllib.parse.parse_qs(parsed.query).items()
                if values
            }
            if parsed.path != callback_path:
                captured["error"] = "unexpected_callback_path"
                self._write_response(404, "Nomos OIDC callback path rejected.")
                return
            error = params.get("error")
            if error:
                captured["error"] = error
                captured["error_description"] = params.get("error_description", "")
                self._write_response(400, "Nomos OIDC login failed.")
                return
            state = params.get("state")
            if state != expected_state:
                captured["error"] = "state_mismatch"
                self._write_response(400, "Nomos OIDC state mismatch.")
                return
            code = params.get("code")
            if not code:
                captured["error"] = "missing_authorization_code"
                self._write_response(400, "Nomos OIDC callback missing code.")
                return
            captured["code"] = code
            captured["state"] = state
            self._write_response(
                200,
                "Nomos OIDC login complete. You can close this tab.",
            )

        def log_message(self, *_args: Any) -> None:  # noqa: N802
            return

        def _write_response(self, status: int, body: str) -> None:
            html = (
                "<!doctype html><html><head><meta charset='utf-8'>"
                "<title>Nomos OIDC Login</title></head><body>"
                f"<p>{body}</p></body></html>"
            ).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(html)))
            self.end_headers()
            self.wfile.write(html)

    server = HTTPServer((host, int(port)), CallbackHandler)
    server.timeout = timeout_seconds
    try:
        server.handle_request()
    finally:
        server.server_close()
    if "error" in captured:
        error = captured["error"]
        description = captured.get("error_description")
        detail = f": {description}" if description else ""
        raise OIDCLoginError(f"OIDC callback rejected: {error}{detail}")
    if "code" not in captured:
        raise OIDCLoginError("OIDC login timed out before callback arrived")
    return captured


def exchange_authorization_code(
    metadata: Mapping[str, Any],
    *,
    code: str,
    client_id: str,
    client_secret: str | None,
    client_auth_method: str,
    code_verifier: str,
    redirect_uri: str,
    timeout_seconds: int,
) -> dict[str, Any]:
    token_endpoint = _required_metadata_url(metadata, "token_endpoint")
    auth_method = _resolve_client_auth_method(client_auth_method, client_secret)
    data = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": client_id,
        "code_verifier": code_verifier,
        "redirect_uri": redirect_uri,
    }
    auth = None
    if auth_method == "client_secret_basic":
        auth = (client_id, client_secret or "")
    elif auth_method == "client_secret_post":
        data["client_secret"] = client_secret or ""
    elif auth_method != "none":
        raise OIDCLoginError(f"unsupported OIDC client auth method: {auth_method}")
    try:
        # SSRF guard + no redirect-follow: the client_secret/code must not be
        # replayed to a redirect-chosen origin.
        assert_public_http_url(token_endpoint)
        response = requests.post(
            token_endpoint,
            data=data,
            auth=auth,
            timeout=timeout_seconds,
            headers={"Accept": "application/json"},
            allow_redirects=False,
        )
    except requests.RequestException as exc:
        raise OIDCLoginError("OIDC token exchange failed") from exc
    if response.status_code >= 300:
        raise OIDCLoginError(
            f"OIDC token exchange failed with HTTP {response.status_code} "
            "(a 3xx redirect is not followed)"
        )
    try:
        tokens = response.json()
    except ValueError as exc:
        raise OIDCLoginError("OIDC token response was not valid JSON") from exc
    if not isinstance(tokens, dict):
        raise OIDCLoginError("OIDC token response must be a JSON object")
    return tokens


def verify_oidc_id_token(
    id_token: str,
    *,
    metadata: Mapping[str, Any],
    client_id: str,
    nonce: str,
    leeway_seconds: int = 60,
    jwk_client_factory: Callable[[str], JWKClientLike] | None = None,
) -> dict[str, Any]:
    jwks_uri = _required_metadata_url(metadata, "jwks_uri")
    issuer = str(metadata.get("issuer") or "")
    if not issuer:
        raise OIDCLoginError("OIDC metadata missing issuer")
    algorithms = _allowed_algorithms(metadata)
    jwk_client = (jwk_client_factory or PyJWKClient)(jwks_uri)
    try:
        signing_key = jwk_client.get_signing_key_from_jwt(id_token)
        claims = jwt.decode(
            id_token,
            signing_key.key,
            algorithms=list(algorithms),
            audience=client_id,
            issuer=issuer,
            options={"require": ["iss", "sub", "aud", "exp", "iat"]},
            leeway=leeway_seconds,
        )
    except Exception as exc:  # PyJWT raises a family of verification errors.
        raise OIDCLoginError("OIDC ID token verification failed") from exc
    if claims.get("nonce") != nonce:
        raise OIDCLoginError("OIDC ID token nonce mismatch")
    return dict(claims)


def merge_userinfo_claims(
    verified_claims: Mapping[str, Any],
    *,
    metadata: Mapping[str, Any],
    access_token: Any,
    timeout_seconds: int,
) -> dict[str, Any]:
    if not isinstance(access_token, str) or not access_token:
        raise OIDCLoginError("OIDC UserInfo fetch requires an access token")
    endpoint = metadata.get("userinfo_endpoint")
    if not isinstance(endpoint, str) or not endpoint.startswith("https://"):
        raise OIDCLoginError("OIDC issuer metadata does not expose UserInfo")
    try:
        # SSRF guard + no redirect-follow: the access-token Bearer must not be
        # replayed to a redirect-chosen origin.
        assert_public_http_url(endpoint)
        response = requests.get(
            endpoint,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {access_token}",
            },
            timeout=timeout_seconds,
            allow_redirects=False,
        )
    except requests.RequestException as exc:
        raise OIDCLoginError("OIDC UserInfo fetch failed") from exc
    if response.status_code >= 300:
        raise OIDCLoginError(
            f"OIDC UserInfo fetch failed with HTTP {response.status_code} "
            "(a 3xx redirect is not followed)"
        )
    try:
        userinfo = response.json()
    except ValueError as exc:
        raise OIDCLoginError("OIDC UserInfo response was not valid JSON") from exc
    if not isinstance(userinfo, dict):
        raise OIDCLoginError("OIDC UserInfo response must be a JSON object")
    if userinfo.get("sub") and userinfo.get("sub") != verified_claims.get("sub"):
        raise OIDCLoginError("OIDC UserInfo subject mismatch")
    merged = dict(verified_claims)
    for key, value in userinfo.items():
        if key in RESERVED_ID_TOKEN_CLAIMS:
            continue
        if key not in merged:
            merged[key] = value
    return merged


def build_nomos_claim_from_verified_claims(
    verified_claims: Mapping[str, Any],
    *,
    provider_id: str,
    provider_type: str,
    verified_by: str,
    verification_method: str,
    include_claims: Sequence[str] = (),
    expected_nonce: str | None = None,
    issuer: str | None = None,
    redirect_uri: str | None = None,
) -> OIDCLoginResult:
    if expected_nonce is not None and verified_claims.get("nonce") != expected_nonce:
        raise OIDCLoginError("OIDC ID token nonce mismatch")
    sanitized_claims = sanitize_oidc_claims(
        verified_claims,
        include_claims=include_claims,
    )
    claim_issuer = str(issuer or sanitized_claims.get("iss") or "")
    if not claim_issuer:
        raise OIDCLoginError("verified OIDC claims are missing issuer")
    subject = str(sanitized_claims.get("sub") or "")
    if not subject:
        raise OIDCLoginError("verified OIDC claims are missing subject")
    audiences = _as_str_tuple(sanitized_claims.get("aud"))
    if not audiences:
        raise OIDCLoginError("verified OIDC claims are missing audience")
    now = datetime.now(tz=UTC).isoformat()
    claim = {
        "provider_id": provider_id,
        "provider_type": provider_type,
        "issuer": claim_issuer,
        "external_subject": subject,
        "audiences": list(audiences),
        # This function only runs on claims whose ID token was verified, and
        # it has just re-checked nonce, issuer, subject, and audience. It is
        # the one caller entitled to assert verification, so it says so:
        # normalization no longer infers it from silence (issue #1007).
        "verification_status": VALID_CLAIM_STATUS,
        "verified_by": verified_by,
        "verification_method": verification_method,
        "verified_at": now,
        "claim_fingerprint": "sha256:" + _sha256_hex({
            "provider_id": provider_id,
            "issuer": claim_issuer,
            "sub": subject,
            "aud": list(audiences),
            "verified_by": verified_by,
            "verified_at": now,
            "claims": sanitized_claims,
        }),
        "claims": sanitized_claims,
    }
    _assert_no_sensitive_keys(claim)
    return OIDCLoginResult(
        claim=claim,
        issuer=claim_issuer,
        provider_id=provider_id,
        provider_type=provider_type,
        external_subject=subject,
        audiences=audiences,
        included_claim_keys=tuple(sorted(sanitized_claims)),
        redirect_uri=redirect_uri or "",
    )


def sanitize_oidc_claims(
    claims: Mapping[str, Any],
    *,
    include_claims: Sequence[str] = (),
) -> dict[str, Any]:
    allowed = set(DEFAULT_OIDC_CLAIM_ALLOWLIST)
    allowed.update(str(item) for item in include_claims if str(item))
    sanitized: dict[str, Any] = {}
    for key, value in claims.items():
        key_str = str(key)
        lowered = key_str.lower()
        if lowered in SENSITIVE_CLAIM_KEYS or key_str in RESERVED_ID_TOKEN_CLAIMS:
            continue
        if key_str not in allowed:
            continue
        sanitized[key_str] = _json_safe_claim_value(value)
    _assert_no_sensitive_keys(sanitized)
    return sanitized


def _allowed_algorithms(metadata: Mapping[str, Any]) -> tuple[str, ...]:
    configured = metadata.get("id_token_signing_alg_values_supported") or ()
    if isinstance(configured, str):
        configured = (configured,)
    algorithms = tuple(
        str(item)
        for item in configured
        if str(item) in ASYMMETRIC_ID_TOKEN_ALGORITHMS
    )
    return algorithms or tuple(sorted(ASYMMETRIC_ID_TOKEN_ALGORITHMS))


def _resolve_client_auth_method(method: str, client_secret: str | None) -> str:
    if method == "auto":
        return "client_secret_basic" if client_secret else "none"
    if method in {"client_secret_basic", "client_secret_post"} and not client_secret:
        raise OIDCLoginError(
            f"OIDC client auth method {method!r} requires --client-secret-env"
        )
    return method


def _client_secret(env_name: str | None) -> str | None:
    if not env_name:
        return None
    value = os.environ.get(env_name)
    if not value:
        raise OIDCLoginError(
            f"OIDC client secret environment variable is not set: {env_name}"
        )
    return value


def _validate_config(config: OIDCLoginConfig) -> None:
    for field_name in (
        "issuer",
        "client_id",
        "provider_id",
        "provider_type",
        "verified_by",
    ):
        value = getattr(config, field_name)
        if not isinstance(value, str) or not value.strip():
            raise OIDCLoginError(f"{field_name} is required")
    if config.redirect_host != "127.0.0.1":
        raise OIDCLoginError("OIDC loopback verifier only supports 127.0.0.1")
    if config.redirect_port <= 0 or config.redirect_port > 65535:
        raise OIDCLoginError("redirect port must be in the range 1..65535")
    if config.timeout_seconds <= 0:
        raise OIDCLoginError("timeout_seconds must be positive")


def _required_metadata_url(metadata: Mapping[str, Any], key: str) -> str:
    value = metadata.get(key)
    if not isinstance(value, str) or not value.startswith("https://"):
        raise OIDCLoginError(f"OIDC metadata field {key!r} must be an https URL")
    return value


def _status(callback: Callable[[str], None] | None, message: str) -> None:
    if callback is not None:
        callback(message)


def _json_safe_claim_value(value: Any) -> Any:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, Mapping):
        return {
            str(key): _json_safe_claim_value(child)
            for key, child in value.items()
            if str(key).lower() not in SENSITIVE_CLAIM_KEYS
        }
    if isinstance(value, Sequence) and not isinstance(value, str):
        return [_json_safe_claim_value(child) for child in value]
    return str(value)


def _assert_no_sensitive_keys(value: Any) -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if str(key).lower() in SENSITIVE_CLAIM_KEYS:
                raise OIDCLoginError("OIDC claim output contains raw credential material")
            _assert_no_sensitive_keys(child)
    elif isinstance(value, Sequence) and not isinstance(value, str):
        for child in value:
            _assert_no_sensitive_keys(child)


def _as_str_tuple(value: Any) -> tuple[str, ...]:
    if isinstance(value, str):
        return (value,)
    if isinstance(value, Sequence):
        return tuple(str(item) for item in value if str(item))
    return ()


def _base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _sha256_hex(value: Any) -> str:
    import json

    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
