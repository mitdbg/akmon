"""A real HTTPS OpenID Connect issuer for the OIDC MCP proof (#1180).

The issuer runs on loopback inside the test process and serves exactly what
``okg mcp-serve --oidc-auth`` fetches: the discovery document and the signing
key set (JWKS). It signs RS256 access tokens with its own keys, can publish a
new key while running (key rotation), and counts key-set downloads so a test
can see whether the server went back to fetch keys.

Nothing here reaches into the server. The server trusts this issuer the way a
deployment trusts any private issuer: ``SSL_CERT_FILE`` / ``REQUESTS_CA_BUNDLE``
name the test certificate authority, and ``OKG_EGRESS_ALLOW_HOSTS`` names the
loopback host as an operator exception to the public-address rule (logged at
WARNING by the server).
"""

from __future__ import annotations

import datetime as dt
import ipaddress
import json
import ssl
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import jwt
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

#: The realm path makes the issuer look like a real provider's (Keycloak).
REALM_PATH = "/realms/okg"


def _rsa_key() -> rsa.RSAPrivateKey:
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def write_tls_material(directory: Path) -> tuple[Path, Path, Path]:
    """A throwaway certificate authority and a 127.0.0.1 server certificate.

    Returns ``(ca_pem, server_cert_pem, server_key_pem)``. The CA is what the
    server subprocess trusts; the server certificate is what the issuer
    presents. A proper CA-plus-leaf pair (not one self-signed leaf) so strict
    X.509 verification accepts it on every supported Python.
    """
    now = dt.datetime.now(dt.UTC)
    ca_key = _rsa_key()
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "okg test OIDC CA")])
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=5))
        .not_valid_after(now + dt.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=0), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=False,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )
    leaf_key = _rsa_key()
    leaf_cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "127.0.0.1")]))
        .issuer_name(ca_name)
        .public_key(leaf_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=5))
        .not_valid_after(now + dt.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                    x509.DNSName("localhost"),
                ]
            ),
            critical=False,
        )
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(leaf_key.public_key()),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )
    ca_pem = directory / "test-oidc-ca.pem"
    cert_pem = directory / "test-oidc-server.pem"
    key_pem = directory / "test-oidc-server.key"
    ca_pem.write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    cert_pem.write_bytes(leaf_cert.public_bytes(serialization.Encoding.PEM))
    key_pem.write_bytes(
        leaf_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return ca_pem, cert_pem, key_pem


class TestIssuer:
    """One running HTTPS issuer. Start with :meth:`start`, stop with :meth:`stop`."""

    __test__ = False  # not a pytest test class

    def __init__(self, cert_pem: Path, key_pem: Path) -> None:
        self._cert_pem = cert_pem
        self._key_pem = key_pem
        self._lock = threading.Lock()
        self._keys: dict[str, rsa.RSAPrivateKey] = {}
        self._published: list[str] = []
        self.jwks_fetches = 0
        self.discovery_fetches = 0
        #: Every token this issuer minted, so a log scan can look for each.
        self.minted: list[str] = []
        self._server: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        self.port: int | None = None
        #: The CA a client must trust to reach this issuer; set by the caller.
        self.ca_pem: Path | None = None

    # -- keys -------------------------------------------------------------

    def add_key(self, kid: str, *, publish: bool = True) -> rsa.RSAPrivateKey:
        """Create a signing key; publish it in the JWKS unless told not to."""
        key = _rsa_key()
        with self._lock:
            self._keys[kid] = key
            if publish:
                self._published.append(kid)
        return key

    def publish(self, kid: str) -> None:
        with self._lock:
            if kid not in self._published:
                self._published.append(kid)

    def _jwks(self) -> dict[str, Any]:
        with self._lock:
            keys = []
            for kid in self._published:
                jwk = jwt.algorithms.RSAAlgorithm.to_jwk(
                    self._keys[kid].public_key(), as_dict=True
                )
                keys.append({**jwk, "kid": kid, "use": "sig", "alg": "RS256"})
        return {"keys": keys}

    # -- tokens -----------------------------------------------------------

    def mint(
        self,
        claims: dict[str, Any],
        *,
        kid: str,
        key: rsa.RSAPrivateKey | None = None,
    ) -> str:
        """Sign ``claims`` with the named key (or ``key``), headed with ``kid``."""
        signing_key = key if key is not None else self._keys[kid]
        token = jwt.encode(claims, signing_key, algorithm="RS256", headers={"kid": kid})
        self.minted.append(token)
        return token

    # -- the HTTPS server -------------------------------------------------

    @property
    def issuer(self) -> str:
        assert self.port is not None, "issuer not started"
        return f"https://127.0.0.1:{self.port}{REALM_PATH}"

    def discovery(self) -> dict[str, Any]:
        base = self.issuer
        return {
            "issuer": base,
            "authorization_endpoint": f"{base}/protocol/openid-connect/auth",
            "token_endpoint": f"{base}/protocol/openid-connect/token",
            "jwks_uri": f"{base}/protocol/openid-connect/certs",
            "id_token_signing_alg_values_supported": ["RS256"],
            "response_types_supported": ["code"],
            "subject_types_supported": ["public"],
        }

    def start(self) -> None:
        issuer = self

        class _Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 - http.server API
                path = self.path.split("?", 1)[0]
                if path == f"{REALM_PATH}/.well-known/openid-configuration":
                    with issuer._lock:
                        issuer.discovery_fetches += 1
                    body = issuer.discovery()
                elif path == f"{REALM_PATH}/protocol/openid-connect/certs":
                    with issuer._lock:
                        issuer.jwks_fetches += 1
                    body = issuer._jwks()
                else:
                    self.send_response(404)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                data = json.dumps(body).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
                return

        server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(str(self._cert_pem), str(self._key_pem))
        server.socket = context.wrap_socket(server.socket, server_side=True)
        server.daemon_threads = True
        self._server = server
        self.port = int(server.server_address[1])
        self._thread = threading.Thread(target=server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
        if self._thread is not None:
            self._thread.join(timeout=10)


def person_claims(
    issuer: str,
    audience: str,
    *,
    email: str,
    roles: list[str],
    lifetime_seconds: int = 600,
    issued_offset_seconds: int = 0,
    **overrides: Any,
) -> dict[str, Any]:
    """A Keycloak-shaped access token for a person (roles under realm_access)."""
    now = int(time.time()) + issued_offset_seconds
    claims: dict[str, Any] = {
        "iss": issuer,
        "sub": f"kc:{email.split('@', 1)[0]}",
        "aud": [audience, "account"],
        "azp": audience,
        "iat": now,
        "exp": now + lifetime_seconds,
        "auth_time": now - 5,
        "email": email,
        "realm_access": {"roles": list(roles)},
        "scope": "openid email profile",
    }
    claims.update(overrides)
    return claims
