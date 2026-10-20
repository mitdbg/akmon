"""Unit tests for okg.views.restricted_corpus — Stage 1.2.

Covers the umbrella's "Hermeticity is enforced structurally"
requirement and the framework spec's "RestrictedCorpus wrapper API"
requirement at the implementation level.
"""
from __future__ import annotations

import pytest

from okg.views.restricted_corpus import HermeticityViolation, RestrictedCorpus
from okg.views.types import Anchor


@pytest.fixture
def corpus():
    """Three-anchor corpus used across the suite."""
    return {
        "a": Anchor.from_content(id="a", content="A"),
        "b": Anchor.from_content(id="b", content="B"),
        "c": Anchor.from_content(id="c", content="C"),
    }


class TestDeclaredAccess:
    def test_declared_id_returns_anchor(self, corpus):
        rc = RestrictedCorpus(corpus, allowed_ids={"a", "b"})
        assert rc["a"] is corpus["a"]
        assert rc["b"] is corpus["b"]

    def test_undeclared_id_raises_hermeticity_violation(self, corpus):
        rc = RestrictedCorpus(corpus, allowed_ids={"a", "b"})
        with pytest.raises(HermeticityViolation) as excinfo:
            rc.__getitem__("c")
        # Exception message names BOTH the violating id and the
        # declared sources, so the planner author can find it.
        msg = str(excinfo.value)
        assert "'c'" in msg
        assert "['a', 'b']" in msg or '"a", "b"' in msg or "['a','b']" in msg

    def test_declared_id_missing_from_full_corpus_raises_keyerror_not_hermeticity(self):
        # The id IS declared (so hermeticity is fine), but the full
        # corpus doesn't actually have it. We want a clean KeyError
        # so the caller knows it's an upstream-data issue, not a
        # planner-hermeticity bug.
        empty: dict[str, Anchor] = {}
        rc = RestrictedCorpus(empty, allowed_ids={"declared_but_missing"})
        with pytest.raises(KeyError):
            rc.__getitem__("declared_but_missing")


class TestStructuralBlocking:
    """Iteration / keys() / values() / get() must all be blocked."""

    def test_for_loop_raises_typeerror(self, corpus):
        rc = RestrictedCorpus(corpus, allowed_ids={"a", "b"})
        with pytest.raises(TypeError, match="not iterable"):
            for _ in rc:
                pass

    def test_keys_raises_attributeerror(self, corpus):
        rc = RestrictedCorpus(corpus, allowed_ids={"a", "b"})
        with pytest.raises(AttributeError, match="keys"):
            rc.keys()  # type: ignore[attr-defined]

    def test_values_raises_attributeerror(self, corpus):
        rc = RestrictedCorpus(corpus, allowed_ids={"a", "b"})
        with pytest.raises(AttributeError, match="values"):
            rc.values()  # type: ignore[attr-defined]

    def test_items_raises_attributeerror(self, corpus):
        rc = RestrictedCorpus(corpus, allowed_ids={"a", "b"})
        with pytest.raises(AttributeError, match="items"):
            rc.items()  # type: ignore[attr-defined]

    def test_get_method_raises_attributeerror(self, corpus):
        # The umbrella spec forbids a silent-default `.get()` that
        # would let a planner write `corpus.get("undeclared")` and
        # silently get None back.
        rc = RestrictedCorpus(corpus, allowed_ids={"a"})
        with pytest.raises(AttributeError, match="get"):
            rc.get("a")  # type: ignore[attr-defined]


class TestExplicitOptionalPattern:
    """The documented "anchor or None" idiom MUST be explicit at the
    call site — the wrapper supports it via __contains__.
    """

    def test_contains_for_declared_id_is_true(self, corpus):
        rc = RestrictedCorpus(corpus, allowed_ids={"a", "b"})
        assert "a" in rc
        assert "b" in rc

    def test_contains_for_undeclared_id_is_false(self, corpus):
        rc = RestrictedCorpus(corpus, allowed_ids={"a", "b"})
        assert "c" not in rc

    def test_contains_does_not_consult_full_corpus(self):
        # Importantly: __contains__ checks the allow-list, NOT the
        # full corpus. A planner cannot use `"x" in corpus` to
        # discover undeclared anchor ids that happen to exist
        # upstream.
        full = {
            "a": Anchor.from_content("a", "A"),
            "x": Anchor.from_content("x", "secret"),
        }
        rc = RestrictedCorpus(full, allowed_ids={"a"})
        assert "x" not in rc  # NOT in the allow-list, even though in full

    def test_explicit_or_none_idiom(self, corpus):
        # The pattern documented in the wrapper's docstring:
        #     anchor = corpus[id] if id in spec.sources else None
        rc = RestrictedCorpus(corpus, allowed_ids={"a"})

        sources = ("a",)
        a_anchor = rc["a"] if "a" in sources else None
        b_anchor = rc["b"] if "b" in sources else None

        assert a_anchor is corpus["a"]
        assert b_anchor is None


class TestRepr:
    def test_repr_lists_declared_sources(self, corpus):
        # __repr__ shows ONLY allowed ids — not full-corpus contents.
        rc = RestrictedCorpus(corpus, allowed_ids={"b", "a"})
        r = repr(rc)
        assert "['a', 'b']" in r  # sorted for stability
        assert "'c'" not in r  # never leak full-corpus members
