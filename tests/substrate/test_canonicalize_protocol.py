"""Unit tests for `okg.substrate.canonicalize` Protocol +
RepoNamespaceCanonicalizer reference impl. No DB.
"""
from __future__ import annotations


from okg.substrate.canonicalize import (
    RepoNamespaceCanonicalizer,
    canonicalize_id,
)


def test_repo_namespace_rewrites_known_alias() -> None:
    c = RepoNamespaceCanonicalizer(
        id_family="file",
        repo_aliases={"foo": "okg"},
    )
    assert c.canonicalize("file:foo:src/x.py") == "file:okg:src/x.py"


def test_repo_namespace_passes_unknown_alias_through() -> None:
    c = RepoNamespaceCanonicalizer(
        id_family="file",
        repo_aliases={"foo": "okg"},
    )
    # Unknown repo (`bar`) → unchanged.
    assert c.canonicalize("file:bar:src/x.py") == "file:bar:src/x.py"


def test_repo_namespace_passes_other_family_through() -> None:
    c = RepoNamespaceCanonicalizer(
        id_family="file",
        repo_aliases={"foo": "okg"},
    )
    # Wrong id_family — file canonicalizer doesn't touch
    # `commit:` ids.
    assert c.canonicalize("commit:foo:abc123") == "commit:foo:abc123"


def test_repo_namespace_idempotent_on_canonical() -> None:
    c = RepoNamespaceCanonicalizer(
        id_family="file",
        repo_aliases={"foo": "okg"},
    )
    canonical = "file:okg:src/x.py"
    assert c.canonicalize(canonical) == canonical


def test_canonicalize_chain_picks_first_match() -> None:
    """`canonicalize_id` walks the chain; first one whose
    `id_family` matches and produces a different output
    wins."""
    chain = RepoNamespaceCanonicalizer.chain_from_aliases(
        {"foo": "okg", "bar": "okg"},
        families=("file", "commit", "branch", "tag"),
    )
    assert canonicalize_id("file:foo:x.py", chain) == "file:okg:x.py"
    assert canonicalize_id("commit:bar:abc", chain) == "commit:okg:abc"
    assert canonicalize_id("branch:foo:main", chain) == "branch:okg:main"


def test_canonicalize_chain_passes_unrecognized_through() -> None:
    """An id whose family isn't in any canonicalizer's
    `id_family` is returned unchanged."""
    chain = RepoNamespaceCanonicalizer.chain_from_aliases(
        {"foo": "okg"},
    )
    # `session:` family isn't in the default chain.
    assert canonicalize_id(
        "session:foo:abc", chain,
    ) == "session:foo:abc"


def test_canonicalize_empty_or_malformed_id() -> None:
    chain = RepoNamespaceCanonicalizer.chain_from_aliases(
        {"foo": "okg"},
    )
    assert canonicalize_id("", chain) == ""
    assert canonicalize_id("no-colon", chain) == "no-colon"
