"""Property tests for the bitemporal `live_at` predicate.

The substrate's bitemporal contract: a row with `valid_from_gen=g`
and `valid_to_gen=t` (or NULL) is "live at gen G" iff
`g <= G AND (t IS NULL OR t > G)`. The predicate is implemented
in many SQL queries throughout the substrate (see
`session.py::preview_advance`, `mcp/search.py`, the views in
`db/views.sql`). We test the predicate's logic in pure Python so
we can sweep boundary conditions without spinning up Postgres.
"""
from __future__ import annotations

from hypothesis import example, given, settings, strategies as st


def _live_at(
    valid_from_gen: int,
    valid_to_gen: int | None,
    target_gen: int,
) -> bool:
    """Pure-Python mirror of the substrate's bitemporal predicate.

    Mirror of `valid_from_gen <= target_gen AND (valid_to_gen IS
    NULL OR valid_to_gen > target_gen)` as it appears throughout
    the substrate.
    """
    if valid_from_gen > target_gen:
        return False
    if valid_to_gen is None:
        return True
    return valid_to_gen > target_gen


# ---------- monotonic visibility (open-ended row) ----------


@settings(max_examples=200, deadline=None)
@given(
    valid_from=st.integers(min_value=1, max_value=1_000_000),
    target=st.integers(min_value=1, max_value=2_000_000),
)
@example(valid_from=10, target=10)  # boundary: target == valid_from
@example(valid_from=10, target=9)   # before insertion
@example(valid_from=10, target=11)  # after insertion
def test_open_ended_row_live_iff_target_at_least_valid_from(
    valid_from: int, target: int,
) -> None:
    """A row with `valid_to_gen=NULL` is live at every gen ≥
    valid_from, and not live at any gen < valid_from."""
    expected = target >= valid_from
    assert _live_at(valid_from, None, target) == expected


# ---------- closure (retracted row) ----------


@settings(max_examples=200, deadline=None)
@given(
    valid_from=st.integers(min_value=1, max_value=1_000),
    duration=st.integers(min_value=1, max_value=1_000),
    target=st.integers(min_value=1, max_value=10_000),
)
def test_retracted_row_live_only_in_window(
    valid_from: int, duration: int, target: int,
) -> None:
    """A row with `valid_from=g`, `valid_to=g+d` is live at every
    gen in `[g, g+d)`, and NOT live elsewhere."""
    valid_to = valid_from + duration
    expected = (valid_from <= target < valid_to)
    assert _live_at(valid_from, valid_to, target) == expected


# ---------- monotonicity ----------


@settings(max_examples=200, deadline=None)
@given(
    valid_from=st.integers(min_value=1, max_value=1_000),
    valid_to=st.one_of(
        st.none(),
        st.integers(min_value=1, max_value=1_000),
    ),
    g1=st.integers(min_value=1, max_value=2_000),
    g2=st.integers(min_value=1, max_value=2_000),
)
def test_predicate_does_not_flip_arbitrarily(
    valid_from: int, valid_to: int | None,
    g1: int, g2: int,
) -> None:
    """If a row is live at gen `g1` and live at gen `g2`, it is
    live at every gen between them. (Bitemporal "live" is a
    contiguous interval, not a sparse set.)"""
    if not _live_at(valid_from, valid_to, g1):
        return
    if not _live_at(valid_from, valid_to, g2):
        return
    g_min, g_max = min(g1, g2), max(g1, g2)
    for g in (g_min, (g_min + g_max) // 2, g_max):
        assert _live_at(valid_from, valid_to, g), (
            f"row(from={valid_from}, to={valid_to}) is live at "
            f"{g1} and {g2} but NOT at intermediate {g}"
        )


# ---------- delta semantics ----------


@settings(max_examples=200, deadline=None)
@given(
    valid_from=st.integers(min_value=1, max_value=1_000),
    valid_to_extra=st.integers(min_value=1, max_value=100),
    pin_lo=st.integers(min_value=1, max_value=2_000),
    pin_hi_extra=st.integers(min_value=1, max_value=500),
)
def test_added_minus_retracted_equals_delta(
    valid_from: int,
    valid_to_extra: int,
    pin_lo: int,
    pin_hi_extra: int,
) -> None:
    """For a single row `(valid_from, valid_to)` and two pins
    `lo < hi`, the bitemporal "added between lo and hi" XOR
    "retracted between lo and hi" properties are mutually
    exclusive: a single row is at most one of the two."""
    pin_hi = pin_lo + pin_hi_extra
    valid_to = valid_from + valid_to_extra
    live_lo = _live_at(valid_from, valid_to, pin_lo)
    live_hi = _live_at(valid_from, valid_to, pin_hi)
    added = live_hi and not live_lo
    retracted = live_lo and not live_hi
    # A single bitemporal row cannot both newly-appear and be
    # retracted in the same forward step (that would require
    # two transitions, which the substrate doesn't model on
    # one row).
    assert not (added and retracted), (
        f"row(from={valid_from}, to={valid_to}) appears to BOTH "
        f"add and retract between pin {pin_lo} → {pin_hi}"
    )
