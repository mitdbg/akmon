"""Unit tests for the per-commit memo scope (okg.substrate.commit_scope).

No database: these pin the memo contract that the commit-constant caches rely
on — once-per-scope evaluation, live fall-through with no scope, scope reset on
exit, None caching, key isolation, and nested-scope isolation.
"""

from __future__ import annotations

from okg.substrate.commit_scope import (
    commit_scope,
    commit_scope_active,
    db_scope_key,
    identity_claim_dedup_token,
    memo_seen,
    memoized,
    read_pass_scope,
)


def _counter():
    calls = {"n": 0}

    def compute():
        calls["n"] += 1
        return calls["n"]

    return calls, compute


def test_no_scope_is_live_every_call():
    """With no active scope each call recomputes (uncached) — non-ingest paths."""
    assert not commit_scope_active()
    calls, compute = _counter()
    assert memoized("k", compute) == 1
    assert memoized("k", compute) == 2
    assert calls["n"] == 2


def test_scope_memoizes_once_per_key():
    calls, compute = _counter()
    with commit_scope():
        assert commit_scope_active()
        first = memoized("k", compute)
        second = memoized("k", compute)
    assert first == second == 1
    assert calls["n"] == 1


def test_distinct_keys_do_not_collide():
    calls, compute = _counter()
    with commit_scope():
        a = memoized(("catalog_version_id", "db1"), compute)
        b = memoized(("catalog_version_id", "db2"), compute)
        a_again = memoized(("catalog_version_id", "db1"), compute)
    assert a == 1 and b == 2 and a_again == 1
    assert calls["n"] == 2


def test_scope_resets_on_exit():
    calls, compute = _counter()
    with commit_scope():
        assert memoized("k", compute) == 1
    # Scope closed: cache is gone, back to live evaluation.
    assert not commit_scope_active()
    assert memoized("k", compute) == 2
    assert calls["n"] == 2


def test_none_result_is_cached():
    calls = {"n": 0}

    def compute_none():
        calls["n"] += 1
        return None

    with commit_scope():
        assert memoized("k", compute_none) is None
        assert memoized("k", compute_none) is None
    assert calls["n"] == 1


def test_nested_scopes_are_isolated():
    calls, compute = _counter()
    with commit_scope():
        outer = memoized("k", compute)  # 1
        with commit_scope():
            inner = memoized("k", compute)  # 2 — fresh inner scope
        # Inner scope closed; outer cache is restored, still serves its value.
        outer_again = memoized("k", compute)
    assert outer == 1 and inner == 2 and outer_again == 1
    assert calls["n"] == 2


def test_db_scope_key_from_conn_info():
    class _Info:
        dbname = "okg_x"
        host = "localhost"
        port = 5433

    class _Conn:
        info = _Info()

    assert db_scope_key(_Conn()) == ("okg_x", "localhost", 5433)


def test_db_scope_key_without_info_is_none_bucket():
    class _Bare:
        pass

    assert db_scope_key(_Bare()) is None


# --------------------------------------------------------------------------
# read_pass_scope — PACT mcp-principal-mapping
# --------------------------------------------------------------------------


def test_read_pass_scope_memoizes_like_a_commit_scope():
    calls, compute = _counter()
    with read_pass_scope():
        first = memoized("k", compute)
        second = memoized("k", compute)
    third = memoized("k", compute)  # no scope: live again
    assert first == second == 1
    assert third == 2
    assert calls["n"] == 2


def test_read_pass_scope_is_reentrant_and_shares_its_memo():
    """An inner scope must JOIN the outer one, not shadow it.

    A shadowing inner scope would silently re-read everything while still
    looking like it worked -- the MCP request opens the outer scope and the
    read filter opens the inner one.
    """
    calls, compute = _counter()
    with read_pass_scope():
        outer = memoized("k", compute)
        with read_pass_scope():
            inner = memoized("k", compute)
    assert outer == inner == 1
    assert calls["n"] == 1


def test_identity_claim_dedup_is_opt_in():
    """Off by default, so ingest and single-operator reads are untouched."""
    assert identity_claim_dedup_token() is None
    with read_pass_scope():
        assert identity_claim_dedup_token() is None
    with commit_scope():
        assert identity_claim_dedup_token() is None
    with read_pass_scope(dedup_identity_claims=True):
        assert identity_claim_dedup_token() is not None
    assert identity_claim_dedup_token() is None


def test_identity_claim_dedup_token_differs_between_passes():
    """A mark left in a SHARED outer dict cannot be hit by a later pass.

    A read pass nested in an ingest commit writes its memo_seen marks into
    the ingest run's dict and cannot remove them safely. Making the token part
    of the key means the next pass simply does not collide, instead of relying
    on cleanup that an exception could skip.
    """
    with commit_scope():
        with read_pass_scope(dedup_identity_claims=True):
            first_token = identity_claim_dedup_token()
            assert memo_seen(("claim", first_token, "x")) is False
            assert memo_seen(("claim", first_token, "x")) is True
        # Back in the ingest run: dedup is off again, so the run's own writes
        # are not suppressed by the pass that just finished.
        assert identity_claim_dedup_token() is None
        with read_pass_scope(dedup_identity_claims=True):
            second_token = identity_claim_dedup_token()
            assert second_token != first_token
            # The FIRST pass's mark is unreachable from the second.
            assert memo_seen(("claim", second_token, "x")) is False


def test_read_pass_scope_restores_the_outer_scope_on_exception():
    with commit_scope():
        assert identity_claim_dedup_token() is None
        try:
            with read_pass_scope(dedup_identity_claims=True):
                assert identity_claim_dedup_token() is not None
                raise RuntimeError("boom")
        except RuntimeError:
            pass
        assert identity_claim_dedup_token() is None
    assert commit_scope_active() is False


def test_commit_scope_nested_in_a_read_pass_does_not_inherit_the_marker():
    """The inner commit scope is a fresh dict, so it is not a read pass."""
    with read_pass_scope(dedup_identity_claims=True):
        assert identity_claim_dedup_token() is not None
        with commit_scope():
            assert identity_claim_dedup_token() is None
        assert identity_claim_dedup_token() is not None
