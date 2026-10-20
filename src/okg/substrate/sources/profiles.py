"""The five named source profiles (M3.1).

Per design.md "Source contracts": a `source_registry.yaml` entry is
valid only if its `(source_class, record_identity_kind,
source_revision_kind, deletion_semantics, publication_mode)` tuple
matches one row of the named matrix below. Adding a new profile is
a design-level change.

Disjunctions are encoded as sets of strings; n/a is encoded as the
sentinel value `None` (matched by None or the literal string 'n/a').
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


REGISTRY_ONLY = "registry_only"


@dataclass(frozen=True)
class ProfileSpec:
    """The allowed values for one named profile.

    Each set is a disjunction: any one of the listed values is
    valid; anything else is rejected. None means "not applicable
    for this profile" (live_overlay).
    """

    name: str
    record_identity_kind: Optional[set[str]]
    source_revision_kind: Optional[set[str]]
    deletion_semantics: set[str]
    publication_mode: set[str]
    description: str = ""


PROFILES: dict[str, ProfileSpec] = {
    "reference_catalog": ProfileSpec(
        name="reference_catalog",
        record_identity_kind={"domain_key"},
        source_revision_kind={"release_tag", "updated_at", "content_hash"},
        deletion_semantics={"missing_from_completed_scope"},
        publication_mode={"published_generation"},
        description=(
            "Authoritative catalog with periodic full-scope reconcile. "
            "Absence in newer release implies retraction."
        ),
    ),
    "mutable_api": ProfileSpec(
        name="mutable_api",
        record_identity_kind={"remote_id"},
        source_revision_kind={"updated_at", "version_token"},
        deletion_semantics={"explicit_delete", "missing_from_completed_scope"},
        publication_mode={"published_generation"},
        description=(
            "Mutable upstream API queried by cursor + periodic reconcile. "
            "Cursor-only runs MUST NOT emit retractions; only "
            "reconcile-mode runs may."
        ),
    ),
    "discovery_crawl": ProfileSpec(
        name="discovery_crawl",
        record_identity_kind={"scoped_locator"},
        source_revision_kind={"content_hash", "version_token"},
        deletion_semantics={"missing_from_completed_scope"},
        publication_mode={"published_generation"},
        description=(
            "Walk a scope (filesystem, sitemap, etc.) and diff by "
            "content hash. Absence in completed scope implies retraction."
        ),
    ),
    "append_feed": ProfileSpec(
        name="append_feed",
        record_identity_kind={"event_id", "bucket_key"},
        source_revision_kind={
            "sequence_number", "updated_at", "restatement_version",
        },
        deletion_semantics={"append_only"},
        publication_mode={"published_generation"},
        description=(
            "Append-only event log with watermark + lateness window. "
            "No retractions; restatements come via restatement_version."
        ),
    ),
    "live_overlay": ProfileSpec(
        name="live_overlay",
        # n/a — overlays bypass fact append
        record_identity_kind=None,
        source_revision_kind=None,
        deletion_semantics={"external_overlay"},
        publication_mode={"live_overlay"},
        description=(
            "Read-through overlay (no fact append). NOT included in "
            "published generations; queried at MCP time."
        ),
    ),
    REGISTRY_ONLY: ProfileSpec(
        name=REGISTRY_ONLY,
        record_identity_kind=None,
        source_revision_kind=None,
        deletion_semantics=set(),
        publication_mode=set(),
        description=(
            "Documentation-only entry. Intended scope without a "
            "runnable factory. MUST NOT be scheduled for publication."
        ),
    ),
}

PROFILE_NAMES: frozenset[str] = frozenset(PROFILES.keys())


def profile_spec(name: str) -> ProfileSpec:
    """Lookup; raises ValueError on unknown profile."""
    if name not in PROFILES:
        raise ValueError(
            f"unknown source_class {name!r}; valid profiles: "
            f"{sorted(PROFILES)}"
        )
    return PROFILES[name]


def _matches(value: Optional[str], allowed: Optional[set[str]]) -> bool:
    """A field value matches the profile slot iff:

    - The slot is `None` (n/a) and the value is None or 'n/a'
    - The slot is a set and the value is in it
    """
    if allowed is None:
        return value is None or value == "n/a"
    return value in allowed


def validate_profile_tuple(
    *,
    source_class: str,
    record_identity_kind: Optional[str],
    source_revision_kind: Optional[str],
    deletion_semantics: Optional[str],
    publication_mode: Optional[str],
) -> None:
    """Raise ValueError if the tuple is not one of the named profiles.

    `registry_only` may omit the four contract keys entirely (any
    value, including None, is accepted).
    """
    spec = profile_spec(source_class)

    if source_class == REGISTRY_ONLY:
        return

    errors: list[str] = []
    if not _matches(record_identity_kind, spec.record_identity_kind):
        errors.append(
            f"record_identity_kind={record_identity_kind!r} not in "
            f"{sorted(spec.record_identity_kind) if spec.record_identity_kind else ['n/a']}"
        )
    if not _matches(source_revision_kind, spec.source_revision_kind):
        errors.append(
            f"source_revision_kind={source_revision_kind!r} not in "
            f"{sorted(spec.source_revision_kind) if spec.source_revision_kind else ['n/a']}"
        )
    if deletion_semantics not in spec.deletion_semantics:
        errors.append(
            f"deletion_semantics={deletion_semantics!r} not in "
            f"{sorted(spec.deletion_semantics)}"
        )
    if publication_mode not in spec.publication_mode:
        errors.append(
            f"publication_mode={publication_mode!r} not in "
            f"{sorted(spec.publication_mode)}"
        )
    if errors:
        raise ValueError(
            f"profile {source_class!r} rejected this tuple:\n  - "
            + "\n  - ".join(errors)
            + f"\n\nAdding a new profile is a design-level change. "
            f"Pick one of: {sorted(PROFILES)}."
        )
