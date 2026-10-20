"""A local HTTP(S) server for the source-manifest HTTP tests. No network.

`LocalServer(routes)` serves `routes[path](request) -> (status, headers, body)`
on 127.0.0.1 in a thread and records every request. `make_tls_material`
builds a throwaway CA, a server certificate for 127.0.0.1 and a client
certificate with `cryptography`, so the client-certificate auth mode is
tested against a server that requires one.
"""
from __future__ import annotations

import datetime as dt
import ipaddress
import json
import ssl
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Mapping, Optional
from urllib.parse import parse_qs, urlsplit


@dataclass
class Request:
    path: str
    query: dict[str, str]
    headers: dict[str, str]


Response = tuple[int, Mapping[str, str], bytes]
Route = Callable[[Request], Response]


def json_response(payload: Any, *, status: int = 200, headers: Optional[Mapping[str, str]] = None) -> Response:
    return status, {"Content-Type": "application/json", **(headers or {})}, json.dumps(payload).encode()


@dataclass
class LocalServer:
    routes: dict[str, Route]
    tls: Optional[ssl.SSLContext] = None
    requests: list[Request] = field(default_factory=list)

    def __post_init__(self) -> None:
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802 - http.server API
                parts = urlsplit(self.path)
                request = Request(
                    parts.path,
                    {k: v[-1] for k, v in parse_qs(parts.query, keep_blank_values=True).items()},
                    {k: v for k, v in self.headers.items()},
                )
                owner.requests.append(request)
                route = owner.routes.get(parts.path)
                status, headers, body = (404, {}, b"no route") if route is None else route(request)
                self.send_response(status)
                for name, value in headers.items():
                    self.send_header(name, value)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args: Any) -> None:
                return

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        if self.tls is not None:
            self.httpd.socket = self.tls.wrap_socket(self.httpd.socket, server_side=True)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    @property
    def base(self) -> str:
        scheme = "https" if self.tls is not None else "http"
        return f"{scheme}://127.0.0.1:{self.httpd.server_address[1]}"

    def close(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()


@dataclass
class TlsMaterial:
    ca_dir: Path
    server_context: ssl.SSLContext
    client_cert: Path
    client_key: Path


def make_tls_material(root: Path) -> TlsMaterial:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    now = dt.datetime.now(dt.timezone.utc)

    def name(cn: str) -> x509.Name:
        return x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, cn)])

    def write_key(key: Any, path: Path) -> None:
        path.write_bytes(key.private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ))

    ca_key = ec.generate_private_key(ec.SECP256R1())
    ca_cert = (
        x509.CertificateBuilder().subject_name(name("okg test CA")).issuer_name(name("okg test CA"))
        .public_key(ca_key.public_key()).serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=5)).not_valid_after(now + dt.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(x509.KeyUsage(
            digital_signature=True, key_cert_sign=True, crl_sign=True, content_commitment=False,
            key_encipherment=False, data_encipherment=False, key_agreement=False,
            encipher_only=False, decipher_only=False,
        ), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(ca_key.public_key()), critical=False)
        .sign(ca_key, hashes.SHA256())
    )

    def leaf(cn: str, usage: Any, san: Optional[x509.SubjectAlternativeName]) -> tuple[Any, Any]:
        key = ec.generate_private_key(ec.SECP256R1())
        builder = (
            x509.CertificateBuilder().subject_name(name(cn)).issuer_name(ca_cert.subject)
            .public_key(key.public_key()).serial_number(x509.random_serial_number())
            .not_valid_before(now - dt.timedelta(minutes=5)).not_valid_after(now + dt.timedelta(days=1))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(x509.ExtendedKeyUsage([usage]), critical=False)
            .add_extension(
                x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_key.public_key()), critical=False,
            )
        )
        if san is not None:
            builder = builder.add_extension(san, critical=False)
        return key, builder.sign(ca_key, hashes.SHA256())

    server_key, server_cert = leaf("127.0.0.1", ExtendedKeyUsageOID.SERVER_AUTH, x509.SubjectAlternativeName([
        x509.IPAddress(ipaddress.ip_address("127.0.0.1")), x509.DNSName("localhost"),
    ]))
    client_key, client_cert = leaf("okg test client", ExtendedKeyUsageOID.CLIENT_AUTH, None)

    root.mkdir(parents=True, exist_ok=True)
    ca_dir = root / "ca"
    ca_dir.mkdir()
    (ca_dir / "okg-test-ca.pem").write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    server_pem = root / "server.pem"
    server_pem.write_bytes(server_cert.public_bytes(serialization.Encoding.PEM))
    write_key(server_key, root / "server.key")
    client_pem = root / "client.pem"
    client_pem.write_bytes(client_cert.public_bytes(serialization.Encoding.PEM))
    write_key(client_key, root / "client.key")

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(str(server_pem), str(root / "server.key"))
    context.load_verify_locations(cafile=str(ca_dir / "okg-test-ca.pem"))
    context.verify_mode = ssl.CERT_REQUIRED
    return TlsMaterial(ca_dir=ca_dir, server_context=context, client_cert=client_pem, client_key=root / "client.key")
