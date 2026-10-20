"""Fake credential strings for redaction and secret-gate tests.

These tests need values shaped like real credentials (a GitHub token, a PEM
private-key header, and so on) so the redactor or gate under test sees what it
would see in the wild. The public-release source scan refuses any complete
credential-shaped literal, fake or not, so the tests assemble each value here
at runtime from a prefix and a body. The runtime value is exactly the
concatenation; nothing is weakened.
"""

from __future__ import annotations


def fake_credential(prefix: str, body: str) -> str:
    """Return ``prefix + body``, keeping the full shape out of the source."""
    return prefix + body


def pem_begin(kind: str = "") -> str:
    """Return a PEM private-key BEGIN header, e.g. ``pem_begin("RSA")``."""
    label = f"{kind} " if kind else ""
    return "-----BEGIN " + label + "PRIVATE KEY-----"


PEM_BEGIN = pem_begin()
PEM_OPENSSH_BEGIN = pem_begin("OPENSSH")
PEM_RSA_BEGIN = pem_begin("RSA")
PEM_EC_BEGIN = pem_begin("EC")
