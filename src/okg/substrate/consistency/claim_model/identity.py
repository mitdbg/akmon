"""Deterministic claim identity — the logical-key discipline of the claim model.

v1-currency-spec §3: a claim's id is a **deterministic logical key** that
EXCLUDES the value and evidence span, so the same (predicate, key, source,
occurrence) re-extracts to the same id; a value change re-triggers review on a
stable identity; a vanished claim expires its decisions (see
:mod:`.effective`). ``source_id + predicate + key`` alone is insufficient (one
document can assert the same key twice) — the occurrence index, and optionally
a structural anchor, keep them distinct.

Hoisted from ``cern_twiki.claims.claim_node_id`` (PACT
``consistency-claim-model-core``). Identity must never be a content hash.
"""
from __future__ import annotations

import re

CLAIM_ID_PREFIX = "claim"

_KEY_SLUG_RE = re.compile(r"[^a-z0-9]+")


def claim_key_slug(key: str) -> str:
    """Normalize a logical key for the id: lowercase, non-alphanumerics to '-'."""
    return _KEY_SLUG_RE.sub("-", str(key).strip().lower()).strip("-") or "_"


def claim_logical_id(
    source_id: str,
    predicate: str,
    key: str,
    occurrence: int,
    *,
    anchor: str = "",
) -> str:
    """Deterministic logical id: ``claim:{predicate}:{key}:{source}[:{anchor}]:{occ}``.

    Excludes the value and span so the same (predicate, key) in one document
    re-extracts to a stable id; ``occurrence`` keeps two same-key claims in one
    document distinct; ``anchor`` (a structural anchor such as a heading path
    hash) further discriminates when the deployment extracts per-region.
    """
    slug = claim_key_slug(key)
    if anchor:
        return f"{CLAIM_ID_PREFIX}:{predicate}:{slug}:{source_id}:{anchor}:{occurrence}"
    return f"{CLAIM_ID_PREFIX}:{predicate}:{slug}:{source_id}:{occurrence}"
