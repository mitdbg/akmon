from __future__ import annotations

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec, padding, rsa
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature
import pytest

from okg.substrate.external_actions.auth import AuthenticationError, _verify_jwt_signature


def test_rs256_uses_the_exact_jwt_signature_parameters() -> None:
    private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    material = b"header.payload"
    signature = private.sign(material, padding.PKCS1v15(), hashes.SHA256())

    _verify_jwt_signature(
        material,
        signature,
        {"alg": "RS256", "kid": "rsa-1", "typ": "JWT"},
        {"rsa-1": private.public_key()},
        ("RS256",),
    )
    with pytest.raises(AuthenticationError):
        _verify_jwt_signature(
            material + b"x",
            signature,
            {"alg": "RS256", "kid": "rsa-1", "typ": "JWT"},
            {"rsa-1": private.public_key()},
            ("RS256",),
        )


def test_es256_converts_jose_raw_signature_without_accepting_der_aliases() -> None:
    private = ec.generate_private_key(ec.SECP256R1())
    material = b"header.payload"
    der = private.sign(material, ec.ECDSA(hashes.SHA256()))
    r, s = decode_dss_signature(der)
    raw = r.to_bytes(32, "big") + s.to_bytes(32, "big")

    _verify_jwt_signature(
        material,
        raw,
        {"alg": "ES256", "kid": "ec-1", "typ": "JWT"},
        {"ec-1": private.public_key()},
        ("ES256",),
    )
    with pytest.raises(AuthenticationError):
        _verify_jwt_signature(
            material,
            der,
            {"alg": "ES256", "kid": "ec-1", "typ": "JWT"},
            {"ec-1": private.public_key()},
            ("ES256",),
        )
