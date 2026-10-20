"""RapidfuzzBackend tests."""
from __future__ import annotations

from okg.substrate.alias import RapidfuzzBackend


def test_exact_candidate_match_similarity_1():
    b = RapidfuzzBackend(["alice", "bob"], accept_threshold=0.7)
    matches = b.match("alice")
    assert matches
    top = max(matches, key=lambda m: m.similarity)
    assert top.canonical == "alice"
    assert top.similarity == 1.0
    assert top.method == "rapidfuzz"


def test_one_edit_match_above_threshold():
    b = RapidfuzzBackend(
        ["alpha-bravo-charlie"], accept_threshold=0.5,
    )
    matches = b.match("alpha-bravo-charli")  # one missing char
    assert matches
    assert matches[0].similarity >= 0.5


def test_canonical_for_maps_match_canonical():
    b = RapidfuzzBackend(
        ["alice"],
        canonical_for={"alice": "n:alice-uuid"},
        accept_threshold=0.7,
    )
    matches = b.match("alice")
    assert matches[0].canonical == "n:alice-uuid"


def test_empty_needle_returns_empty():
    b = RapidfuzzBackend(["alice"])
    assert b.match("") == []


def test_empty_candidates_returns_empty():
    b = RapidfuzzBackend([])
    assert b.match("alice") == []
