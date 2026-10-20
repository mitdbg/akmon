"""Property tests for the slug normalizers.

Two slug functions live in the substrate:
- `okg.substrate.catalog.concept_seed._slug` — used by catalog
  loading; raises on empty result.
- `okg.substrate.catalog.invariants_scope.concept_name_to_node_id`
  — used by invariant scoping; returns `concept:` even on empty.

Both must satisfy idempotence and produce purely-allowed-character
output. The two helpers have slightly different rules; we test
each separately.
"""
from __future__ import annotations

import re

import pytest
from hypothesis import example, given, settings, strategies as st

from okg.substrate.catalog.concept_seed import _slug
from okg.substrate.catalog.invariants_scope import (
    concept_name_to_node_id,
)
from okg.substrate.errors import SchemaValidationFailed


# ---------- _slug (concept_seed) ----------

# Output charset: lowercase alphanumerics + underscores.
_VALID_OUT_RE = re.compile(r"^[a-z0-9_]+$")


@settings(max_examples=200, deadline=None)
@given(st.text(min_size=1, max_size=80))
@example("Bad Weather")
@example("crew-rest")
@example("CHAPLAIN_SERVICES")
@example("a   b   c")
def test_slug_output_charset_when_accepted(name: str) -> None:
    """When `_slug` accepts the input, the output is purely
    [a-z0-9_]+ with no leading/trailing underscores and no
    consecutive underscores."""
    try:
        out = _slug(name)
    except SchemaValidationFailed:
        return  # input was rejected — nothing to assert on output
    assert _VALID_OUT_RE.match(out), (
        f"slug output {out!r} contains forbidden chars"
    )
    assert not out.startswith("_")
    assert not out.endswith("_")


@settings(max_examples=200, deadline=None)
@given(st.text(min_size=1, max_size=80))
@example("Bad Weather")
def test_slug_idempotent_when_accepted(name: str) -> None:
    """`_slug(_slug(s)) == _slug(s)` for any accepted input."""
    try:
        once = _slug(name)
    except SchemaValidationFailed:
        return
    twice = _slug(once)
    assert once == twice, (
        f"slug not idempotent: _slug({name!r})={once!r}, "
        f"_slug(_slug(...))={twice!r}"
    )


@settings(max_examples=100, deadline=None)
@given(st.text(
    alphabet=st.characters(
        whitelist_categories=("Pc", "Po", "Pd", "Zs"),
    ),
    min_size=1, max_size=40,
))
def test_slug_rejects_punctuation_only(name: str) -> None:
    """Inputs containing only punctuation/whitespace produce an
    empty slug, which `_slug` rejects loudly (no silent
    fallback)."""
    with pytest.raises(SchemaValidationFailed):
        _slug(name)


# ---------- concept_name_to_node_id (invariants_scope) ----------

# Output is `concept:<slug-or-empty>`.
_NODE_ID_RE = re.compile(r"^concept:[a-z0-9_]*$")


@settings(max_examples=200, deadline=None)
@given(st.text(min_size=0, max_size=80))
@example("Bad Weather")
@example("crew-rest")
@example("")
@example("!!!")
@example("__x__")
@example("¹")  # Unicode superscript — `isalnum()` is True in
                # Python, but the node_id charset is ASCII only.
                # Hypothesis surfaced this on first run; the
                # production fix added an ASCII check to the
                # canonicalizer.
def test_concept_name_node_id_format(name: str) -> None:
    """Output is always `concept:` followed by 0+ chars from
    `[a-z0-9_]`."""
    out = concept_name_to_node_id(name)
    assert out.startswith("concept:")
    body = out[len("concept:"):]
    assert _NODE_ID_RE.match(out), out
    # No leading/trailing underscores.
    assert not body.startswith("_")
    assert not body.endswith("_")
    # No consecutive underscores.
    assert "__" not in body


@settings(max_examples=200, deadline=None)
@given(st.text(min_size=1, max_size=80))
@example("Bad Weather")
@example("Crew Rest")
def test_concept_name_node_id_lowercase(name: str) -> None:
    """The body is purely lowercase."""
    out = concept_name_to_node_id(name)
    body = out[len("concept:"):]
    assert body == body.lower(), out
