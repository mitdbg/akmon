"""Base64url helpers for Poros wire values."""

from __future__ import annotations

import base64
import re

from okg.substrate.exchange.errors import PorosEncodingError


_B64URL_RE = re.compile(r"^[A-Za-z0-9_-]*$")


def b64url_encode(data: bytes) -> str:
    """Return unpadded RFC 4648 base64url text."""
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def b64url_decode(value: str, *, field: str = "value") -> bytes:
    """Decode unpadded base64url text, rejecting ambiguous encodings."""
    if not isinstance(value, str) or not _B64URL_RE.fullmatch(value):
        raise PorosEncodingError(
            f"{field} is not unpadded base64url",
            reason_code="invalid_base64url",
            data={"field": field},
        )
    if len(value) % 4 == 1:
        raise PorosEncodingError(
            f"{field} has invalid base64url length",
            reason_code="invalid_base64url",
            data={"field": field},
        )
    padding = "=" * (-len(value) % 4)
    try:
        return base64.urlsafe_b64decode((value + padding).encode("ascii"))
    except Exception as exc:  # pragma: no cover - validated above.
        raise PorosEncodingError(
            f"{field} could not be decoded",
            reason_code="invalid_base64url",
            data={"field": field},
        ) from exc
