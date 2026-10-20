"""Property tests for `RepoNamespaceCanonicalizer`.

The canonicalizer rewrites `<id_family>:<alias>:<rest>` into
`<id_family>:<canonical>:<rest>` per a `repo_aliases` map.

Three invariants:
1. **Idempotent**: `canonicalize(canonicalize(x)) == canonicalize(x)`
   when canonical → canonical (the transitive closure is the
   operator's responsibility, but the map MUST NOT cycle).
2. **Pass-through for unknown families**: an id whose family
   doesn't match the canonicalizer's `id_family` returns
   unchanged.
3. **Pass-through for unknown repos**: an id whose repo isn't in
   the alias map returns unchanged.
"""
from __future__ import annotations

from hypothesis import example, given, settings, strategies as st

from okg.substrate.canonicalize.repo_namespace import (
    RepoNamespaceCanonicalizer,
)


# Strategy: ASCII alphanumeric + dash (typical repo / id-family
# names). Avoid colons since the canonicalizer's parsing is
# colon-delimited.
_safe_token = st.text(
    alphabet=st.characters(
        whitelist_categories=("Ll", "Nd"),
        whitelist_characters="-_",
    ),
    min_size=1, max_size=20,
)


# ---------- idempotent ----------


@settings(max_examples=200, deadline=None)
@given(
    family=_safe_token,
    alias=_safe_token,
    canonical=_safe_token,
    rest=_safe_token,
)
@example(family="file", alias="okg", canonical="okg-real", rest="path/to/x")
def test_canonicalize_idempotent_with_aliasing(
    family: str, alias: str, canonical: str, rest: str,
) -> None:
    """A canonicalizer that maps `alias` → `canonical` produces an
    idempotent rewrite: applying it twice yields the same result
    as applying it once."""
    canonicalizer = RepoNamespaceCanonicalizer(
        id_family=family,
        repo_aliases={alias: canonical},
    )
    raw = f"{family}:{alias}:{rest}"
    once = canonicalizer.canonicalize(raw)
    twice = canonicalizer.canonicalize(once)
    assert once == twice, (
        f"not idempotent: canonicalize({raw!r})={once!r}, "
        f"canonicalize(once)={twice!r}"
    )


@settings(max_examples=200, deadline=None)
@given(
    family=_safe_token,
    repo=_safe_token,
    rest=_safe_token,
)
def test_canonicalize_idempotent_no_alias(
    family: str, repo: str, rest: str,
) -> None:
    """Without an alias for the repo, the function returns the
    input unchanged — which is trivially idempotent."""
    canonicalizer = RepoNamespaceCanonicalizer(
        id_family=family, repo_aliases={},
    )
    raw = f"{family}:{repo}:{rest}"
    once = canonicalizer.canonicalize(raw)
    twice = canonicalizer.canonicalize(once)
    assert once == twice == raw


# ---------- pass-through for unknown family ----------


@settings(max_examples=200, deadline=None)
@given(
    family=_safe_token,
    other_family=_safe_token,
    repo=_safe_token,
    rest=_safe_token,
)
def test_canonicalize_passes_through_unknown_family(
    family: str, other_family: str, repo: str, rest: str,
) -> None:
    """An id whose family doesn't match the canonicalizer's
    `id_family` returns verbatim regardless of the alias map."""
    if family == other_family:
        return  # the families match — outside this property's scope
    canonicalizer = RepoNamespaceCanonicalizer(
        id_family=family,
        repo_aliases={repo: "would-rewrite-but-shouldnt"},
    )
    raw = f"{other_family}:{repo}:{rest}"
    out = canonicalizer.canonicalize(raw)
    assert out == raw


# ---------- pass-through for unknown repo ----------


@settings(max_examples=200, deadline=None)
@given(
    family=_safe_token,
    repo=_safe_token,
    rest=_safe_token,
)
def test_canonicalize_passes_through_unknown_repo(
    family: str, repo: str, rest: str,
) -> None:
    """An id whose repo isn't in the alias map returns verbatim."""
    canonicalizer = RepoNamespaceCanonicalizer(
        id_family=family,
        repo_aliases={"some-other-repo": "rewritten"},
    )
    raw = f"{family}:{repo}:{rest}"
    out = canonicalizer.canonicalize(raw)
    if repo == "some-other-repo":
        return  # input matched the alias — outside the property
    assert out == raw


# ---------- empty input ----------


def test_canonicalize_empty_input() -> None:
    """Empty input is a documented pass-through case."""
    canonicalizer = RepoNamespaceCanonicalizer(
        id_family="file", repo_aliases={"a": "b"},
    )
    assert canonicalizer.canonicalize("") == ""


# ---------- malformed input ----------


@settings(max_examples=100, deadline=None)
@given(st.text(min_size=0, max_size=80))
def test_canonicalize_never_raises(s: str) -> None:
    """The canonicalizer is `O(1)` parsing of `family:repo:rest`
    via `str.partition`; it never raises on malformed input."""
    canonicalizer = RepoNamespaceCanonicalizer(
        id_family="file", repo_aliases={"a": "b"},
    )
    # Should return *something* without raising.
    out = canonicalizer.canonicalize(s)
    assert isinstance(out, str)
