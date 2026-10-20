"""Unit tests for okg.views.types — Stage 1.1 of add-incremental-views.

Covers the umbrella's "Anchor type and content-hash discipline" and
"Fragment type carries source signature" requirements at the
implementation-level, plus the View pin-the-generation requirement.
"""
from __future__ import annotations

import pytest

from okg.views.types import (
    Anchor,
    Fragment,
    View,
    _normalize,
    content_hash,
)


# ---------------------------------------------------------------------------
# Normalizer
# ---------------------------------------------------------------------------


class TestNormalize:
    def test_collapses_runs_of_whitespace_to_single_space(self):
        assert _normalize("a   b\t\tc") == "a b c"

    def test_collapses_newlines(self):
        assert _normalize("a\nb\nc") == "a b c"

    def test_strips_leading_and_trailing(self):
        assert _normalize("  hello  ") == "hello"
        assert _normalize("\n\thi\n") == "hi"

    def test_pure_function_idempotent(self):
        text = "  one   two\t\tthree  "
        assert _normalize(_normalize(text)) == _normalize(text)


# ---------------------------------------------------------------------------
# content_hash — the umbrella's hash function over normalized content
# ---------------------------------------------------------------------------


class TestContentHash:
    def test_returns_16_hex_chars(self):
        h = content_hash("anything")
        assert len(h) == 16
        # all hex
        assert all(c in "0123456789abcdef" for c in h)

    def test_pure_function(self):
        # Same input → same output, repeatedly.
        for _ in range(5):
            assert content_hash("hello world") == content_hash("hello world")

    def test_whitespace_equivalent_inputs_share_hash(self):
        # The umbrella's "whitespace-equivalent content yields the
        # same hash" scenario.
        assert content_hash("a\nb") == content_hash("a b")
        assert content_hash("a   b") == content_hash("a b")
        assert content_hash("\ta\tb\n") == content_hash("a b")

    def test_substantive_change_yields_different_hash(self):
        # The umbrella's "substantive content change yields a
        # different hash" scenario.
        assert content_hash("hello") != content_hash("world")
        assert content_hash("status: current") != content_hash("status: rescinded")


# ---------------------------------------------------------------------------
# Anchor
# ---------------------------------------------------------------------------


class TestAnchor:
    def test_from_content_computes_hash(self):
        a = Anchor.from_content(id="x:catalog", content="status: current")
        assert a.id == "x:catalog"
        assert a.content == "status: current"
        assert a.content_hash == content_hash("status: current")

    def test_anchor_is_frozen(self):
        # Anchors are value objects; mutation is explicitly disallowed.
        a = Anchor.from_content(id="x", content="foo")
        with pytest.raises((AttributeError, Exception)):
            # frozen dataclass raises FrozenInstanceError (a subclass
            # of AttributeError on cpython 3.12+).
            a.content = "bar"  # type: ignore[misc]

    def test_id_stable_under_content_edit(self):
        # The umbrella's "stable identity rule" — editing content
        # produces a new anchor (same id, new hash), not a rename.
        original = Anchor.from_content(id="x:catalog", content="v1")
        edited = Anchor.from_content(id="x:catalog", content="v2")
        assert edited.id == original.id
        assert edited.content_hash != original.content_hash

    def test_explicit_hash_construction_for_roundtripping(self):
        # The bare constructor accepts an explicit hash so manifests
        # loaded from disk can reconstruct anchors without
        # recomputing.
        a = Anchor(id="x", content="foo", content_hash="deadbeef00000000")
        assert a.content_hash == "deadbeef00000000"


# ---------------------------------------------------------------------------
# Fragment
# ---------------------------------------------------------------------------


class TestFragment:
    def _hash(self, text: str) -> str:
        return content_hash(text)

    def test_quote_fragment_construction(self):
        f = Fragment(
            id="row:pub-1",
            kind="quote",
            sources=("a:catalog",),
            source_hashes={"a:catalog": self._hash("attrs of a")},
            content="attrs of a",
        )
        assert f.kind == "quote"
        assert f.sources == ("a:catalog",)

    def test_source_hashes_must_cover_sources(self):
        # Missing key → ValueError with a precise message.
        with pytest.raises(ValueError, match=r"missing source_hashes"):
            Fragment(
                id="bad",
                kind="quote",
                sources=("a", "b"),
                source_hashes={"a": "deadbeef00000000"},  # missing 'b'
                content="...",
            )

    def test_source_hashes_no_extra_keys(self):
        # Extra key → ValueError. The "exactly" rule cuts both ways.
        with pytest.raises(ValueError, match=r"unexpected source_hashes keys"):
            Fragment(
                id="bad",
                kind="quote",
                sources=("a",),
                source_hashes={
                    "a": "deadbeef00000000",
                    "b": "00000000deadbeef",  # extra
                },
                content="...",
            )

    def test_sources_order_is_preserved(self):
        # Sources is order-significant per the umbrella spec.
        f = Fragment(
            id="combo",
            kind="generated",
            sources=("c", "a", "b"),
            source_hashes={
                "a": self._hash("A"),
                "b": self._hash("B"),
                "c": self._hash("C"),
            },
            content="...",
        )
        assert f.sources == ("c", "a", "b")

    def test_fragment_is_frozen(self):
        f = Fragment(
            id="x",
            kind="quote",
            sources=("a",),
            source_hashes={"a": "deadbeef00000000"},
            content="...",
        )
        with pytest.raises((AttributeError, Exception)):
            f.content = "..."  # type: ignore[misc]

    def test_kind_literal_accepts_quote_and_generated(self):
        # Both literal values are accepted at runtime; static checkers
        # enforce the rest. We exercise the runtime path here.
        for kind in ("quote", "generated"):
            f = Fragment(
                id=f"x:{kind}",
                kind=kind,  # type: ignore[arg-type]
                sources=("a",),
                source_hashes={"a": "0" * 16},
                content="...",
            )
            assert f.kind == kind


# ---------------------------------------------------------------------------
# View
# ---------------------------------------------------------------------------


class TestView:
    def test_default_construction(self):
        v = View(view_id="af-a3-brief", view_def_hash="abc123")
        assert v.view_id == "af-a3-brief"
        assert v.view_def_hash == "abc123"
        assert v.manifest_version == 1
        assert v.fragments == []
        assert v.generation_id is None

    def test_pin_substrate_generation(self):
        # Production path: View carries an int generation_id pinning
        # the substrate generation it was rendered against.
        v = View(
            view_id="x",
            view_def_hash="abc",
            generation_id=42,
        )
        assert v.generation_id == 42

    def test_snapshot_path_leaves_generation_none(self):
        # Test/dev fixtures from snapshot JSON have no substrate to
        # pin; generation_id stays None.
        v = View(view_id="x", view_def_hash="abc")
        assert v.generation_id is None

    def test_fragments_is_appendable(self):
        # View is not frozen — fresh generation appends incrementally.
        v = View(view_id="x", view_def_hash="abc")
        f = Fragment(
            id="row:1",
            kind="quote",
            sources=("a",),
            source_hashes={"a": "0" * 16},
            content="...",
        )
        v.fragments.append(f)
        assert len(v.fragments) == 1
        assert v.fragments[0] is f
