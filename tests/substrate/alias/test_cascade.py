"""CascadeAliasResolver — orchestration tests (no DB)."""
from __future__ import annotations

import pytest

from okg.extraction import ExtractedEntity
from okg.substrate.alias import (
    AliasBackend,
    AliasMatch,
    AliasResolutionFailed,
    CascadeAliasResolver,
)


class _FakeBackend:
    name = "fake"

    def __init__(
        self,
        matches: list[AliasMatch],
        *,
        accept_threshold: float,
    ) -> None:
        self._matches = matches
        self.accept_threshold = accept_threshold
        self.calls = 0

    def match(self, needle, *, entity_type=None):
        self.calls += 1
        return list(self._matches)


def _entity(value="alice", typ="person"):
    return ExtractedEntity(
        type=typ, value=value, start=0, end=len(value),
        confidence=1.0,
    )


def test_first_backend_wins_when_threshold_met():
    b1 = _FakeBackend(
        [AliasMatch("n:alice", 0.9, "exact")],
        accept_threshold=0.6,
    )
    b2 = _FakeBackend(
        [AliasMatch("n:other", 1.0, "static")],
        accept_threshold=1.0,
    )
    cascade = CascadeAliasResolver([b1, b2])
    assert cascade.resolve(_entity()) == "n:alice"
    assert b2.calls == 0  # short-circuit


def test_cascade_falls_through_when_below_threshold():
    b1 = _FakeBackend(
        [AliasMatch("n:weak", 0.5, "trigram")],
        accept_threshold=0.8,  # 0.5 < 0.8 → skip
    )
    b2 = _FakeBackend(
        [AliasMatch("n:strong", 0.95, "rapidfuzz")],
        accept_threshold=0.7,
    )
    cascade = CascadeAliasResolver([b1, b2])
    assert cascade.resolve(_entity()) == "n:strong"
    assert b1.calls == 1
    assert b2.calls == 1


def test_all_below_threshold_returns_none_on_drop():
    b1 = _FakeBackend(
        [AliasMatch("n:a", 0.4, "trigram")],
        accept_threshold=0.6,
    )
    b2 = _FakeBackend(
        [AliasMatch("n:b", 0.5, "rapidfuzz")],
        accept_threshold=0.7,
    )
    cascade = CascadeAliasResolver([b1, b2], on_empty="drop")
    assert cascade.resolve(_entity()) is None


def test_all_below_threshold_raises_on_raise():
    b1 = _FakeBackend(
        [AliasMatch("n:a", 0.4, "trigram")],
        accept_threshold=0.6,
    )
    cascade = CascadeAliasResolver([b1], on_empty="raise")
    with pytest.raises(AliasResolutionFailed) as exc_info:
        cascade.resolve(_entity())
    err = exc_info.value
    assert err.data["needle"] == "alice"
    assert err.data["entity_type"] == "person"
    assert err.data["per_backend_top_score"][0]["top_similarity"] == 0.4


def test_empty_matches_skips_to_next_backend():
    b1 = _FakeBackend([], accept_threshold=0.6)
    b2 = _FakeBackend(
        [AliasMatch("n:b", 0.9, "rapidfuzz")],
        accept_threshold=0.7,
    )
    cascade = CascadeAliasResolver([b1, b2])
    assert cascade.resolve(_entity()) == "n:b"


def test_empty_backends_list_raises_at_construction():
    with pytest.raises(ValueError):
        CascadeAliasResolver([])


def test_invalid_on_empty_raises():
    b = _FakeBackend([], accept_threshold=0.6)
    with pytest.raises(ValueError):
        CascadeAliasResolver([b], on_empty="garbage")


def test_runtime_checkable_protocol_membership():
    b = _FakeBackend([], accept_threshold=0.6)
    assert isinstance(b, AliasBackend)
