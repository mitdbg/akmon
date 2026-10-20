"""Property tests for `CascadeAliasResolver`.

The cascade walks backends in declaration order; the first
backend whose top match scores at-or-above its `accept_threshold`
wins. Two invariants:
1. **First-wins**: when backend A returns a match at-or-above its
   threshold, the cascade returns A's answer regardless of
   subsequent backends.
2. **Pass-through**: when no backend's top match meets its
   threshold, the cascade returns None (default `on_empty='drop'`)
   or raises (`on_empty='raise'`) — never silently picks a
   below-threshold match.
"""
from __future__ import annotations

from dataclasses import dataclass

import pytest
from hypothesis import example, given, settings, strategies as st

from okg.extraction import ExtractedEntity
from okg.substrate.alias.cascade import (
    AliasResolutionFailed,
    CascadeAliasResolver,
)
from okg.substrate.alias.protocol import AliasMatch


@dataclass
class _ScriptedBackend:
    """Test double: returns the canned matches for any needle."""
    name: str
    accept_threshold: float
    matches: list[AliasMatch]

    def match(self, needle: str, *, entity_type=None):
        return list(self.matches)


def _entity(value: str = "needle", type: str = "thing") -> ExtractedEntity:
    return ExtractedEntity(
        type=type, value=value, start=0, end=len(value),
    )


# ---------- first-wins ----------


@settings(max_examples=100, deadline=None)
@given(
    threshold=st.floats(min_value=0.0, max_value=1.0),
    a_score_extra=st.floats(min_value=0.0, max_value=0.5),
    b_score=st.floats(min_value=0.0, max_value=1.0),
)
@example(threshold=0.7, a_score_extra=0.0, b_score=0.99)
def test_first_backend_wins_when_above_threshold(
    threshold: float, a_score_extra: float, b_score: float,
) -> None:
    """When backend A's top match meets its threshold, B's
    matches don't influence the result — even if B has a higher
    score."""
    a_score = min(1.0, threshold + a_score_extra)
    a = _ScriptedBackend(
        name="a", accept_threshold=threshold,
        matches=[AliasMatch(canonical="A_ANSWER", similarity=a_score, method="a")],
    )
    b = _ScriptedBackend(
        name="b", accept_threshold=0.0,
        matches=[AliasMatch(canonical="B_ANSWER", similarity=b_score, method="b")],
    )
    resolver = CascadeAliasResolver([a, b])
    assert resolver.resolve(_entity()) == "A_ANSWER"


# ---------- second-wins-when-first-doesnt ----------


@settings(max_examples=100, deadline=None)
@given(
    a_threshold=st.floats(min_value=0.5, max_value=1.0),
    a_score=st.floats(min_value=0.0, max_value=0.4),
    b_threshold=st.floats(min_value=0.0, max_value=0.5),
    b_score_extra=st.floats(min_value=0.0, max_value=0.5),
)
def test_second_backend_used_when_first_below_threshold(
    a_threshold: float, a_score: float,
    b_threshold: float, b_score_extra: float,
) -> None:
    """When A is below threshold, the cascade falls through to
    B — and returns B's answer if B is at-or-above its threshold."""
    b_score = min(1.0, b_threshold + b_score_extra)
    a = _ScriptedBackend(
        name="a", accept_threshold=a_threshold,
        matches=[AliasMatch(canonical="A_ANSWER", similarity=a_score, method="a")],
    )
    b = _ScriptedBackend(
        name="b", accept_threshold=b_threshold,
        matches=[AliasMatch(canonical="B_ANSWER", similarity=b_score, method="b")],
    )
    resolver = CascadeAliasResolver([a, b])
    assert resolver.resolve(_entity()) == "B_ANSWER"


# ---------- pass-through (on_empty=drop) ----------


@settings(max_examples=100, deadline=None)
@given(
    a_threshold=st.floats(min_value=0.5, max_value=1.0),
    a_score=st.floats(min_value=0.0, max_value=0.4),
    b_threshold=st.floats(min_value=0.5, max_value=1.0),
    b_score=st.floats(min_value=0.0, max_value=0.4),
)
def test_drop_returns_none_when_no_backend_matches(
    a_threshold: float, a_score: float,
    b_threshold: float, b_score: float,
) -> None:
    """With `on_empty='drop'` (default), returns None when no
    backend's top match is at-or-above its threshold."""
    a = _ScriptedBackend(
        name="a", accept_threshold=a_threshold,
        matches=[AliasMatch(canonical="A_ANSWER", similarity=a_score, method="a")],
    )
    b = _ScriptedBackend(
        name="b", accept_threshold=b_threshold,
        matches=[AliasMatch(canonical="B_ANSWER", similarity=b_score, method="b")],
    )
    resolver = CascadeAliasResolver([a, b], on_empty="drop")
    assert resolver.resolve(_entity()) is None


# ---------- raise (on_empty=raise) ----------


@settings(max_examples=50, deadline=None)
@given(
    threshold=st.floats(min_value=0.5, max_value=1.0),
    score=st.floats(min_value=0.0, max_value=0.4),
)
def test_raise_when_configured_and_no_match(
    threshold: float, score: float,
) -> None:
    """With `on_empty='raise'`, a no-match outcome raises
    `AliasResolutionFailed` rather than returning None."""
    a = _ScriptedBackend(
        name="a", accept_threshold=threshold,
        matches=[AliasMatch(canonical="A_ANSWER", similarity=score, method="a")],
    )
    resolver = CascadeAliasResolver([a], on_empty="raise")
    with pytest.raises(AliasResolutionFailed):
        resolver.resolve(_entity())


# ---------- invariant: never returns multiple ----------


def test_resolve_returns_at_most_one_canonical() -> None:
    """The contract is `Optional[str]`, never a list. This is a
    type-level guarantee; verifying with the worst case."""
    a = _ScriptedBackend(
        name="a", accept_threshold=0.5,
        matches=[
            AliasMatch(canonical="X", similarity=0.99, method="a"),
            AliasMatch(canonical="Y", similarity=0.95, method="a"),
        ],
    )
    resolver = CascadeAliasResolver([a])
    out = resolver.resolve(_entity())
    assert isinstance(out, str)
    assert out in ("X", "Y")
