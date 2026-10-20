"""MCP search pagination caps (harden-mcp-write-boundaries).

Semantic/alias search materializes LIMIT offset+limit+1 rows, so an
attacker-chosen large offset turns a tiny page into a full scan. search() must
clamp limit, offset (and thus offset+limit) so backend work is bounded.
"""
from __future__ import annotations

import importlib

import pytest

# The package re-exports `search` (the function), shadowing the submodule
# attribute, so import the real module object from sys.modules via importlib.
_sm = importlib.import_module("okg.substrate.mcp.search")
search = _sm.search
_MAX_SEARCH_LIMIT = _sm._MAX_SEARCH_LIMIT
_MAX_SEARCH_OFFSET = _sm._MAX_SEARCH_OFFSET


@pytest.fixture(autouse=True)
def _no_session_check(monkeypatch):
    monkeypatch.setattr(_sm, "assert_session_matches", lambda c, s: None)


def _spy(monkeypatch):
    """Spy ALL three backend methods (lexical/semantic/alias). The clamp is
    applied in search() BEFORE method dispatch, so it must hold for every method
    — a semantic-only spy (adversarial-review finding) under-tested the cap."""
    captured: dict[str, int] = {}

    def spy(conn, session, query, *, limit, offset, **kwargs):
        captured["limit"] = limit
        captured["offset"] = offset
        return {"results": [], "limit": limit, "offset": offset}

    for name in ("_search_lexical", "_search_semantic", "_search_alias"):
        monkeypatch.setattr(_sm, name, spy)
    return captured


@pytest.mark.parametrize("method", ["lexical", "semantic", "alias"])
def test_large_offset_and_limit_are_clamped(monkeypatch, method):
    captured = _spy(monkeypatch)
    out = search(
        object(), object(), "q",
        method=method, embedder=object(),
        offset=10_000_000, limit=999_999,
    )
    assert captured["offset"] == _MAX_SEARCH_OFFSET
    assert captured["limit"] == _MAX_SEARCH_LIMIT
    assert out["pagination_capped"] is True
    assert out["requested_offset"] == 10_000_000
    assert out["requested_limit"] == 999_999


def test_normal_pagination_is_not_capped(monkeypatch):
    captured = _spy(monkeypatch)
    out = search(
        object(), object(), "q",
        method="semantic", embedder=object(),
        offset=100, limit=50,
    )
    assert captured["offset"] == 100
    assert captured["limit"] == 50
    assert "pagination_capped" not in out


def test_offset_plus_limit_is_bounded(monkeypatch):
    captured = _spy(monkeypatch)
    search(
        object(), object(), "q",
        method="semantic", embedder=object(),
        offset=10_000_000, limit=10_000_000,
    )
    assert (captured["offset"] + captured["limit"]) <= (
        _MAX_SEARCH_OFFSET + _MAX_SEARCH_LIMIT
    )
