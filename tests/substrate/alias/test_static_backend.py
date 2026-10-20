"""StaticAliasBackend tests."""
from __future__ import annotations

import pytest

from okg.substrate.alias import StaticAliasBackend


def test_hit_returns_alias_match():
    b = StaticAliasBackend({("person", "Alice"): "n:alice"})
    matches = b.match("Alice", entity_type="person")
    assert len(matches) == 1
    assert matches[0].canonical == "n:alice"
    assert matches[0].similarity == 1.0
    assert matches[0].method == "static"


def test_miss_returns_empty():
    b = StaticAliasBackend({("person", "Alice"): "n:alice"})
    assert b.match("Bob", entity_type="person") == []


def test_missing_entity_type_returns_empty():
    """Static is type-keyed; without entity_type we cannot match."""
    b = StaticAliasBackend({("person", "Alice"): "n:alice"})
    assert b.match("Alice", entity_type=None) == []


def test_wrong_shape_mapping_raises():
    with pytest.raises(ValueError):
        StaticAliasBackend({"not-a-tuple": "n:x"})  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        StaticAliasBackend({("only-one",): "n:x"})  # type: ignore[arg-type]


def test_accept_threshold_is_one():
    b = StaticAliasBackend({})
    assert b.accept_threshold == 1.0
