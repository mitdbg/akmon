"""Strict JSON parsing and canonicalization for Poros signatures.

Poros v0 uses a deliberately narrow RFC 8785/JCS-compatible JSON
profile: strings, booleans, null, arrays, objects with unique string
keys, and safe-range integers. Floating point values are rejected so the
implementation never has to guess at cross-language numeric rendering.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any, TypeAlias

from okg.substrate.exchange.errors import PorosCanonicalizationError


JSONValue: TypeAlias = Any

MAX_SAFE_INTEGER = 9_007_199_254_740_991


def loads_strict_json(data: bytes | str) -> JSONValue:
    """Parse JSON while rejecting duplicate keys and unstable numbers."""
    if isinstance(data, bytes):
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise PorosCanonicalizationError(
                "JSON is not valid UTF-8",
                reason_code="malformed_utf8",
            ) from exc
    elif isinstance(data, str):
        text = data
    else:
        raise PorosCanonicalizationError(
            "JSON input must be bytes or str",
            reason_code="invalid_json_input",
        )

    try:
        value = json.loads(
            text,
            object_pairs_hook=_object_no_duplicates,
            parse_int=_parse_int,
            parse_float=_reject_float,
            parse_constant=_reject_constant,
        )
    except PorosCanonicalizationError:
        raise
    except json.JSONDecodeError as exc:
        raise PorosCanonicalizationError(
            "JSON parsing failed",
            reason_code="malformed_json",
            data={"position": exc.pos},
        ) from exc
    _validate_json_value(value)
    return value


def loads_canonical_json(data: bytes | str) -> JSONValue:
    """Parse JSON and require the transport bytes to already be canonical."""
    raw = data.encode("utf-8") if isinstance(data, str) else data
    value = loads_strict_json(raw)
    canonical = canonical_json_bytes(value)
    if raw != canonical:
        raise PorosCanonicalizationError(
            "JSON input is not in Poros canonical form",
            reason_code="non_canonical_json",
        )
    return value


def canonical_json_bytes(value: Any) -> bytes:
    """Return deterministic UTF-8 JSON bytes for Poros signed data."""
    _validate_json_value(value)
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeEncodeError) as exc:
        raise PorosCanonicalizationError(
            "value cannot be canonically encoded as Poros JSON",
            reason_code="not_canonicalizable",
        ) from exc


def _object_no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in pairs:
        if key in out:
            raise PorosCanonicalizationError(
                f"duplicate JSON object key: {key}",
                reason_code="duplicate_key",
                data={"key": key},
            )
        out[key] = value
    return out


def _parse_int(raw: str) -> int:
    if raw == "-0":
        raise PorosCanonicalizationError(
            "negative zero is not a supported Poros integer",
            reason_code="unsupported_number",
        )
    value = int(raw)
    if abs(value) > MAX_SAFE_INTEGER:
        raise PorosCanonicalizationError(
            "integer exceeds Poros safe JSON range",
            reason_code="unsupported_number",
        )
    return value


def _reject_float(raw: str) -> None:
    raise PorosCanonicalizationError(
        "floating point JSON numbers are not supported by Poros v0",
        reason_code="unsupported_number",
        data={"raw": raw},
    )


def _reject_constant(raw: str) -> None:
    raise PorosCanonicalizationError(
        "non-finite JSON numbers are not supported",
        reason_code="unsupported_number",
        data={"raw": raw},
    )


def _validate_json_value(value: Any, *, path: str = "$") -> None:
    if value is None or isinstance(value, bool):
        return
    if isinstance(value, int) and not isinstance(value, bool):
        if abs(value) > MAX_SAFE_INTEGER:
            raise PorosCanonicalizationError(
                "integer exceeds Poros safe JSON range",
                reason_code="unsupported_number",
                data={"path": path},
            )
        return
    if isinstance(value, float):
        raise PorosCanonicalizationError(
            "floating point JSON numbers are not supported by Poros v0",
            reason_code="unsupported_number",
            data={"path": path},
        )
    if isinstance(value, str):
        _validate_string(value, path=path)
        return
    if isinstance(value, Mapping):
        for key, child in value.items():
            if not isinstance(key, str):
                raise PorosCanonicalizationError(
                    "JSON object keys must be strings",
                    reason_code="non_string_key",
                    data={"path": path},
                )
            _validate_string(key, path=f"{path}.{key}")
            _validate_json_value(child, path=f"{path}.{key}")
        return
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        for index, child in enumerate(value):
            _validate_json_value(child, path=f"{path}[{index}]")
        return
    raise PorosCanonicalizationError(
        f"unsupported JSON value type at {path}: {type(value).__name__}",
        reason_code="unsupported_json_type",
        data={"path": path, "type": type(value).__name__},
    )


def _validate_string(value: str, *, path: str) -> None:
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise PorosCanonicalizationError(
            "JSON string contains an invalid Unicode scalar",
            reason_code="malformed_utf8",
            data={"path": path},
        ) from exc
