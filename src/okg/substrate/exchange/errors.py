"""Poros protocol error classes."""

from __future__ import annotations

from typing import Any


class PorosError(Exception):
    """Base exception carrying a durable Poros preflight reason."""

    reason_code = "poros_error"

    def __init__(
        self,
        message: str,
        *,
        reason_code: str | None = None,
        data: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.reason_code = reason_code or self.reason_code
        self.data = data or {}


class PorosCanonicalizationError(PorosError):
    reason_code = "canonicalization_failed"


class PorosEncodingError(PorosError):
    reason_code = "encoding_failed"


class PorosUnsupportedAlgorithmError(PorosError):
    reason_code = "unsupported_algorithm"


class PorosSignatureError(PorosError):
    reason_code = "invalid_signature"


class PorosPayloadError(PorosError):
    reason_code = "payload_invalid"


class PorosReplayError(PorosError):
    reason_code = "replay_rejected"


class PorosDuplicateMessage(PorosReplayError):
    reason_code = "duplicate"


class PorosTrustError(PorosError):
    reason_code = "trust_rejected"
