from __future__ import annotations

import pytest

from okg.substrate.exchange.canonical_json import (
    canonical_json_bytes,
    loads_canonical_json,
    loads_strict_json,
)
from okg.substrate.exchange.errors import PorosCanonicalizationError


def test_canonical_json_sorts_keys_and_uses_utf8() -> None:
    assert canonical_json_bytes({"b": 2, "a": "MIT"}) == b'{"a":"MIT","b":2}'


def test_strict_json_rejects_duplicate_keys_before_signature() -> None:
    with pytest.raises(PorosCanonicalizationError) as exc:
        loads_strict_json(b'{"sender":"okg:a","sender":"okg:b"}')

    assert exc.value.reason_code == "duplicate_key"


def test_strict_json_rejects_malformed_utf8() -> None:
    with pytest.raises(PorosCanonicalizationError) as exc:
        loads_strict_json(b'{"sender":"\xff"}')

    assert exc.value.reason_code == "malformed_utf8"


def test_strict_json_rejects_unsupported_numeric_encoding() -> None:
    with pytest.raises(PorosCanonicalizationError) as exc:
        loads_strict_json(b'{"weight":1.25}')

    assert exc.value.reason_code == "unsupported_number"


def test_canonical_parser_rejects_noncanonical_transport_bytes() -> None:
    with pytest.raises(PorosCanonicalizationError) as exc:
        loads_canonical_json(b'{ "a": 1, "b": 2 }')

    assert exc.value.reason_code == "non_canonical_json"
