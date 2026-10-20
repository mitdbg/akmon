"""IdCanonicalizer Protocol + chain helper.

Adapters emit raw `<family>:<repo>:<rest>` IDs. A canonicalizer
parses the family prefix; if it claims the family, it rewrites
the rest. The SourceRunner walks every registered
canonicalizer in registration order; the first one that
returns a non-equal output wins.

Idempotence: every canonicalizer's `canonicalize(canonical) ==
canonical`. This lets the substrate run the chain repeatedly
without churn (re-canonicalizing already-canonical IDs is a
no-op).
"""
from __future__ import annotations

from typing import Iterable, Protocol


__all__ = ["IdCanonicalizer", "canonicalize_id"]


class IdCanonicalizer(Protocol):
    """A canonicalizer for one `id_family` (the prefix before
    the first `:`)."""

    id_family: str

    def canonicalize(self, raw_id: str) -> str:
        """Return the canonical form of `raw_id`. If the
        canonicalizer doesn't apply to this id (wrong family,
        no rewrite rule), return `raw_id` unchanged."""
        raise NotImplementedError


def canonicalize_id(
    raw_id: str,
    chain: Iterable[IdCanonicalizer],
) -> str:
    """Apply each registered canonicalizer in turn to `raw_id`.

    Stops at the first canonicalizer whose `id_family` matches
    the id's prefix and produces a different output. Per the
    Protocol contract, canonicalizers MUST be idempotent on
    their own output, so re-running this chain on a canonical
    id is a no-op.
    """
    if not raw_id:
        return raw_id
    family, _, _rest = raw_id.partition(":")
    if not family:
        return raw_id
    for c in chain:
        if c.id_family != family:
            continue
        out = c.canonicalize(raw_id)
        if out != raw_id:
            return out
    return raw_id
