"""Private-safe candidate-link review facts for academic-researcher graphs."""
from __future__ import annotations

import hashlib
import re
from datetime import datetime, timezone
from typing import Any

from okg.substrate.enrichers.derived_edges import DerivedEdgeCandidate


DECISION_EDGE_TYPE = "candidate_link_decision"
# Stored graph identifiers: review decisions already published carry these
# values, so they keep the pre-2026-09-23 module name (`academic_pi`) rather
# than following the module rename. Changing them would orphan every
# recorded decision.
DECISION_SOURCE = "_review:academic_pi_candidate_review"
DECISION_SCHEMA = "okg.academic_pi.candidate_link_decision.v1"
VALID_DECISION_STATES = frozenset({"accepted", "rejected", "deferred"})
VALID_CANONICAL_EDGE_TYPES = frozenset({"mentions", "thread_about"})

# Confidence tiering for candidate links.
#
# A candidate link's precision is determined by *which* feature classes the
# artifact and spine node matched on, not by a flat constant. Identity-grade
# matches (a paper's author field, a project's slug/identifier, a grant's award
# id) are specific and high-precision even on a single signal. Name/alias
# matches are strong when corroborated by a second signal. Title/subject
# matches are mid-grade. Everything else (generic body text, abstracts,
# snippets, project purpose blurbs, status/kind metadata) is low-precision word
# overlap. The linker already computes feature_classes + match_signal_count and
# persists them on every candidate edge, so the bucket is fully derivable from
# stored attrs — both for newly emitted candidates and for the existing backlog.
CONFIDENCE_BUCKETS = ("high", "medium", "low")
DEFAULT_CONFIDENCE_BUCKET = "medium"

_IDENTITY_FEATURE_CLASSES = frozenset({
    "artifact_author",
    "spine_identifier",
    "spine_slug",
    "spine_award",
})
_NAME_FEATURE_CLASSES = frozenset({
    "spine_name",
    "spine_alias",
})
_TITLE_FEATURE_CLASSES = frozenset({
    "artifact_title",
    "artifact_subject",
    "spine_title",
})


def candidate_confidence_bucket(
    feature_classes: Any,
    match_signal_count: Any,
) -> str:
    """Derive a precision bucket from a candidate's matched feature classes.

    Pure and total: identical inputs always yield the same bucket, so the
    linker write-side and the review/read-side stay consistent by construction.
    """
    feats = {
        str(token).strip().lower()
        for token in (feature_classes or ())
        if str(token).strip()
    }
    try:
        signals = int(match_signal_count or 0)
    except (TypeError, ValueError):
        signals = 0
    if feats & _IDENTITY_FEATURE_CLASSES:
        return "high"
    if feats & _NAME_FEATURE_CLASSES:
        return "high" if signals >= 2 else "medium"
    if feats & _TITLE_FEATURE_CLASSES:
        return "medium" if signals >= 2 else "low"
    return "low"


def normalize_decision_state(value: str) -> str:
    state = str(value or "").strip().lower()
    aliases = {
        "accept": "accepted",
        "approve": "accepted",
        "approved": "accepted",
        "reject": "rejected",
        "defer": "deferred",
    }
    state = aliases.get(state, state)
    if state not in VALID_DECISION_STATES:
        allowed = ", ".join(sorted(VALID_DECISION_STATES))
        raise ValueError(f"unsupported candidate decision {value!r}; use {allowed}")
    return state


def candidate_decision_edge(
    *,
    src: str,
    dst: str,
    decision_state: str,
    canonical_edge_type: str,
    rule_id: str = "candidate_review",
    confidence_bucket: str = "medium",
    review_batch_id: str | None = None,
    reviewed_at: datetime | None = None,
) -> DerivedEdgeCandidate:
    """Build a decision edge without storing private candidate text.

    The source and destination ids necessarily identify the candidate pair in
    the graph edge itself. The attrs and source_record_id intentionally avoid
    duplicating those ids or any source text; the stable review id is a digest.
    """
    state = normalize_decision_state(decision_state)
    edge_type = str(canonical_edge_type or "").strip()
    if edge_type not in VALID_CANONICAL_EDGE_TYPES:
        allowed = ", ".join(sorted(VALID_CANONICAL_EDGE_TYPES))
        raise ValueError(
            f"unsupported canonical edge type {canonical_edge_type!r}; "
            f"use {allowed}"
        )
    now = reviewed_at or datetime.now(timezone.utc)
    safe_rule_id = _safe_token(rule_id, default="candidate_review")
    safe_bucket = _safe_token(confidence_bucket, default="medium")
    review_id = candidate_review_id(
        src=src,
        dst=dst,
        canonical_edge_type=edge_type,
        rule_id=safe_rule_id,
    )
    attrs: dict[str, Any] = {
        "schema": DECISION_SCHEMA,
        "candidate_state": state,
        "canonical_edge_type": edge_type,
        "rule_id": safe_rule_id,
        "confidence_bucket": safe_bucket,
        "review_id": review_id,
        "reviewed_at": now.isoformat(),
    }
    if review_batch_id:
        attrs["review_batch_id"] = _safe_token(
            review_batch_id,
            default="manual",
        )
    return DerivedEdgeCandidate(
        src=src,
        edge_type=DECISION_EDGE_TYPE,
        dst=dst,
        attrs=attrs,
        provenance="explicit",
        confidence=1.0,
        source_record_id={
            "review_id": review_id,
            "decision_state": state,
        },
        source_revision={
            "schema": DECISION_SCHEMA,
            "reviewed_at": now.isoformat(),
        },
    )


def candidate_review_id(
    *,
    src: str,
    dst: str,
    canonical_edge_type: str,
    rule_id: str,
) -> str:
    """Stable opaque handle for one reviewable candidate pair."""
    safe_rule_id = _safe_token(rule_id, default="candidate_review")
    edge_type = str(canonical_edge_type or "").strip()
    digest = hashlib.sha256(
        "|".join((src, edge_type, dst, safe_rule_id)).encode("utf-8")
    ).hexdigest()
    return f"candidate-review:{digest[:24]}"


def _safe_token(value: Any, *, default: str) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9_.:-]+", "_", text)
    text = text.strip("_")
    return text[:96] or default
