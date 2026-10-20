"""Relation-kind classification for edge archetypes.

Each edge archetype declared anywhere in a deployment's schemas
(e.g. `contains`, `references`, `cites`) is classified as one of:

  - "contains"   — the edge encodes containment / parent-of /
                   has-part / membership. Following these edges
                   downward defines the navigation hierarchy
                   (Map mode in the viz).
  - "references" — the edge is lateral / cross-cutting /
                   citational. Reference edges are NOT walked
                   when building the hierarchy; they render as
                   cross-links at whichever level is currently
                   displayed.

The default mapping covers the well-known archetype names
contributed by the standard ontology library. Deployments can
override on a per-schema basis by adding `okg_relation_kind`
to the EdgeNarrowings class annotations:

    classes:
      EdgeNarrowings:
        annotations:
          okg_edge_narrowings: 'true'
          okg_relation_kind:
            contains: contains
            cites: references
            frobnicates: contains  # deployment-specific archetype
        attributes: {...}

Archetypes that are neither in the default mapping nor
overridden default to `references` — the safe fallback that
keeps them out of the hierarchy walk.
"""

from __future__ import annotations

from typing import Mapping

# The well-known archetype vocabulary contributed by the
# substrate's standard ontology library. Names match what's
# observed in `src/okg/substrate/library/ontologies/*/schema.yaml`
# narrowing annotations.
#
# IMPORTANT: only FORWARD-direction containment archetypes are
# classified as `contains` — i.e. archetypes whose `src` is the
# parent / container and `dst` is the child / contained. Reverse-
# direction archetypes (`part_of`, `member_of`, `child_of`) encode
# the same hierarchy from the opposite endpoint; the walker only
# walks forward, so reverse-pointers are classified as
# `references` to avoid spurious 2-cycles. Drill-up uses the
# breadcrumb, not reverse edges.
DEFAULT_ARCHETYPE_KINDS: Mapping[str, str] = {
    # Forward containment — src is parent/container, dst is child.
    "contains":      "contains",
    "has_part":      "contains",
    "parent_of":     "contains",
    "defines":       "contains",
    "owns":          "contains",
    "wraps":         "contains",
    # Reverse-direction containment archetypes — same hierarchy,
    # opposite endpoint. Treated as references so the forward-
    # walker doesn't see them as cycles.
    "part_of":       "references",
    "child_of":      "references",
    "member_of":     "references",
    # Reference / lateral archetypes.
    "references":    "references",
    "cites":         "references",
    "mentions":      "references",
    "affects":       "references",
    "assigned_to":   "references",
    "supersedes":    "references",
    "related_to":    "references",
    "depends_on":    "references",
    "links_to":      "references",
}

VALID_KINDS = frozenset({"contains", "references"})


def classify_archetype(
    archetype: str,
    overrides: Mapping[str, str] | None = None,
) -> str:
    """Return `relation_kind` for an archetype name.

    Precedence:
      1. `overrides[archetype]` if present and valid.
      2. `DEFAULT_ARCHETYPE_KINDS[archetype]` if present.
      3. `"references"` (safe fallback).

    Raises `ValueError` if an override value is not one of the
    valid kinds — bad config should fail loudly at load time
    rather than silently miscategorise edges.
    """
    if overrides:
        v = overrides.get(archetype)
        if v is not None:
            if v not in VALID_KINDS:
                raise ValueError(
                    f"okg_relation_kind override for archetype "
                    f"{archetype!r} is {v!r}; expected one of "
                    f"{sorted(VALID_KINDS)}"
                )
            return v
    return DEFAULT_ARCHETYPE_KINDS.get(archetype, "references")


def merge_overrides(
    base: Mapping[str, str] | None,
    new: Mapping[str, str] | None,
) -> dict[str, str]:
    """Combine override dicts. Right-hand wins."""
    out: dict[str, str] = dict(base or {})
    if new:
        for k, v in new.items():
            if v not in VALID_KINDS:
                raise ValueError(
                    f"okg_relation_kind override for archetype "
                    f"{k!r} is {v!r}; expected one of "
                    f"{sorted(VALID_KINDS)}"
                )
            out[k] = v
    return out
