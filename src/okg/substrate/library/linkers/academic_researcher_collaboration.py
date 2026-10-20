"""Academic-researcher collaboration linker.

This linker lifts already-established person/artifact relationships to
project-level semantic edges using the personal seed graph as authority.
It may read private artifact metadata such as email subjects, snippets,
document titles, and calendar titles, but it never stores those text values
on derived edges or in edge provenance.
"""
from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import psycopg

from okg.substrate.enrichers.base import EnrichResult, IncrementalContext
from okg.substrate.enrichers.derived_edges import (
    DerivedEdgeCandidate,
    insert_deterministic_edges,
)
from okg.substrate.library.linkers import _chronos
from okg.substrate.library.linkers._incremental import incremental_touches
from okg.substrate.library.sources.academic_researcher.candidate_review import (
    candidate_confidence_bucket,
)


__all__ = ["AcademicResearcherCollaborationLinker"]


_CANDIDATE_ARTIFACT_SUBTYPES = (
    "calendar_event",
    "document",
    "email",
    "email_thread",
    "paper",
)

_CANDIDATE_SPINE_SUBTYPES = (
    "action_item",
    "grant",
    "organization",
    "person",
    "project",
    "review",
)

_CANDIDATE_MAX_PER_ARTIFACT = 8
_CANDIDATE_CONFIDENCE = 0.65

_ARTIFACT_FIELDS: dict[str, tuple[tuple[str, str], ...]] = {
    "calendar_event": (
        ("artifact_title", "title"),
        ("artifact_location", "location"),
        ("artifact_kind", "kind"),
    ),
    "document": (
        ("artifact_title", "title"),
        ("artifact_text", "text"),
        ("artifact_format", "format"),
    ),
    "email": (
        ("artifact_subject", "subject"),
        ("artifact_snippet", "snippet"),
        ("artifact_labels", "labels"),
    ),
    "email_thread": (
        ("artifact_subject", "subject"),
    ),
    "paper": (
        ("artifact_title", "title"),
        ("artifact_abstract", "abstract"),
        ("artifact_author", "authors"),
    ),
}

_SPINE_FIELDS: dict[str, tuple[tuple[str, str], ...]] = {
    "action_item": (
        ("spine_identifier", "action_id"),
        ("spine_description", "description"),
        ("spine_status", "status"),
    ),
    "grant": (
        ("spine_identifier", "grant_id"),
        ("spine_title", "title"),
        ("spine_award", "award_id"),
        ("spine_funder", "funder"),
        ("spine_topic", "topic_slugs"),
    ),
    "organization": (
        ("spine_identifier", "organization_id"),
        ("spine_name", "name"),
        ("spine_kind", "kind"),
    ),
    "person": (
        ("spine_name", "display_name"),
        ("spine_name", "name"),
        ("spine_alias", "aliases"),
    ),
    "project": (
        ("spine_identifier", "project_id"),
        ("spine_slug", "slug"),
        ("spine_title", "title"),
        ("spine_alias", "aliases"),
        ("spine_purpose", "purpose"),
    ),
    "review": (
        ("spine_identifier", "review_id"),
        ("spine_kind", "kind"),
        ("spine_status", "status"),
        ("spine_subject", "subject_ref"),
    ),
}

_TOKEN_DENYLIST = frozenset({
    "about",
    "after",
    "again",
    "agenda",
    "also",
    "and",
    "before",
    "call",
    "can",
    "case",
    "check",
    "discussion",
    "doc",
    "docs",
    "for",
    "from",
    "hello",
    "hi",
    "into",
    "issue",
    "meeting",
    "next",
    "note",
    "notes",
    "please",
    "question",
    "questions",
    "regarding",
    "revised",
    "schedule",
    "send",
    "sent",
    "task",
    "the",
    "this",
    "todo",
    "with",
    # Generic document/admin vocabulary. These words routinely appear in project
    # slugs/titles/purposes (e.g. "air_force_document_consistency") AND in
    # unrelated documents, so matching on them produces cross-project false
    # positives (any doc containing "document" matched the Air-Force project).
    # They are never identifying, so they must not count as match tokens.
    "analysis",
    "content",
    "contents",
    "copy",
    "data",
    "deck",
    "demo",
    "detail",
    "details",
    "document",
    "documents",
    "draft",
    "file",
    "files",
    "final",
    "folder",
    "info",
    "list",
    "overview",
    "paper",
    "papers",
    "plan",
    "presentation",
    "project",
    "projects",
    "proposal",
    "report",
    "reports",
    "research",
    "results",
    "sample",
    "slides",
    "source",
    "sources",
    "spec",
    "specification",
    "study",
    "summary",
    "template",
    "untitled",
    "update",
    "using",
    "version",
})


class AcademicResearcherCollaborationLinker:
    """Link PI collaboration artifacts to seed projects.

    The primary rule is structural:

    ``Project contains ProjectStakeholder identifies Person``
    plus an artifact/person edge implies the artifact is about the
    stakeholder's project. For papers, a secondary exact full-name
    author fallback is used because many scholarly sources have author
    strings before they have person edges.
    """

    # Stored graph identifier: derived edges already published by this
    # linker carry this name, so it keeps the pre-2026-09-23 module name
    # (`academic_pi`) rather than following the module rename.
    name = "academic_pi_collaboration"
    publisher_skip_when_clean = True
    reads_subtypes = (
        "calendar_event",
        "document",
        "email",
        "email_thread",
        "paper",
        "person",
        "project",
        "project_stakeholder",
    )
    reads_edge_types = (
        "attended",
        "authored",
        "authored_by",
        "contains",
        "cc_on",
        "candidate_link",
        "candidate_link_decision",
        "has_document",
        "has_member",
        "identifies",
        "mentions",
        "received",
        "thread_about",
    )
    emits_edge_types = ("candidate_link", "mentions", "thread_about")
    requires_narrowings = (
        ("calendar_event", "mentions", "project"),
        ("calendar_event", "mentions", "action_item"),
        ("calendar_event", "mentions", "grant"),
        ("calendar_event", "mentions", "organization"),
        ("calendar_event", "mentions", "person"),
        ("calendar_event", "mentions", "review"),
        ("calendar_event", "candidate_link", "action_item"),
        ("calendar_event", "candidate_link", "grant"),
        ("calendar_event", "candidate_link", "organization"),
        ("calendar_event", "candidate_link", "person"),
        ("calendar_event", "candidate_link", "project"),
        ("calendar_event", "candidate_link", "review"),
        ("calendar_event", "candidate_link_decision", "action_item"),
        ("calendar_event", "candidate_link_decision", "grant"),
        ("calendar_event", "candidate_link_decision", "organization"),
        ("calendar_event", "candidate_link_decision", "person"),
        ("calendar_event", "candidate_link_decision", "project"),
        ("calendar_event", "candidate_link_decision", "review"),
        ("document", "candidate_link", "action_item"),
        ("document", "candidate_link", "grant"),
        ("document", "candidate_link", "organization"),
        ("document", "candidate_link", "person"),
        ("document", "candidate_link", "project"),
        ("document", "candidate_link", "review"),
        ("document", "candidate_link_decision", "action_item"),
        ("document", "candidate_link_decision", "grant"),
        ("document", "candidate_link_decision", "organization"),
        ("document", "candidate_link_decision", "person"),
        ("document", "candidate_link_decision", "project"),
        ("document", "candidate_link_decision", "review"),
        ("document", "mentions", "action_item"),
        ("document", "mentions", "grant"),
        ("document", "mentions", "organization"),
        ("document", "mentions", "person"),
        ("document", "mentions", "project"),
        ("document", "mentions", "review"),
        ("email", "candidate_link", "action_item"),
        ("email", "candidate_link", "grant"),
        ("email", "candidate_link", "organization"),
        ("email", "candidate_link", "person"),
        ("email", "candidate_link", "project"),
        ("email", "candidate_link", "review"),
        ("email", "candidate_link_decision", "action_item"),
        ("email", "candidate_link_decision", "grant"),
        ("email", "candidate_link_decision", "organization"),
        ("email", "candidate_link_decision", "person"),
        ("email", "candidate_link_decision", "project"),
        ("email", "candidate_link_decision", "review"),
        ("email", "mentions", "action_item"),
        ("email", "mentions", "grant"),
        ("email", "mentions", "organization"),
        ("email", "mentions", "person"),
        ("email", "mentions", "project"),
        ("email", "mentions", "review"),
        ("email_thread", "candidate_link", "action_item"),
        ("email_thread", "candidate_link", "grant"),
        ("email_thread", "candidate_link", "organization"),
        ("email_thread", "candidate_link", "person"),
        ("email_thread", "candidate_link", "project"),
        ("email_thread", "candidate_link", "review"),
        ("email_thread", "candidate_link_decision", "action_item"),
        ("email_thread", "candidate_link_decision", "grant"),
        ("email_thread", "candidate_link_decision", "organization"),
        ("email_thread", "candidate_link_decision", "person"),
        ("email_thread", "candidate_link_decision", "project"),
        ("email_thread", "candidate_link_decision", "review"),
        ("email_thread", "thread_about", "action_item"),
        ("email_thread", "thread_about", "grant"),
        ("email_thread", "thread_about", "organization"),
        ("email_thread", "thread_about", "person"),
        ("email_thread", "thread_about", "project"),
        ("email_thread", "thread_about", "review"),
        ("paper", "candidate_link", "action_item"),
        ("paper", "candidate_link", "grant"),
        ("paper", "candidate_link", "organization"),
        ("paper", "candidate_link", "person"),
        ("paper", "candidate_link", "project"),
        ("paper", "candidate_link", "review"),
        ("paper", "candidate_link_decision", "action_item"),
        ("paper", "candidate_link_decision", "grant"),
        ("paper", "candidate_link_decision", "organization"),
        ("paper", "candidate_link_decision", "person"),
        ("paper", "candidate_link_decision", "project"),
        ("paper", "candidate_link_decision", "review"),
        ("paper", "mentions", "action_item"),
        ("paper", "mentions", "grant"),
        ("paper", "mentions", "organization"),
        ("paper", "mentions", "person"),
        ("paper", "mentions", "project"),
        ("paper", "mentions", "review"),
    )

    def enrich(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        incremental: IncrementalContext | None = None,
    ) -> EnrichResult:
        with conn.cursor() as cur:
            if not incremental_touches(
                cur,
                incremental,
                node_subtypes=self.reads_subtypes,
                edge_endpoint_subtypes=self.reads_subtypes,
                edge_types=self.reads_edge_types,
            ):
                return EnrichResult(enricher_name=self.name, n_edges_emitted=0)
            pairs = self._match_pairs(cur)
            canonical_result = insert_deterministic_edges(
                cur,
                source=f"_enricher:{self.name}",
                candidates=[
                    DerivedEdgeCandidate(
                        src=src,
                        edge_type=edge_type,
                        dst=dst,
                        source_record_id={
                            "src_node_id": src,
                            "dst_node_id": dst,
                            "linker": self.name,
                            "rule_id": rule_id,
                        },
                        source_revision={"generation_id": generation_id},
                    )
                    for (src, edge_type, dst), rule_id in sorted(pairs.items())
                ],
            )
            candidate_links = _candidate_link_edges(
                cur,
                generation_id=generation_id,
                canonical_pairs=pairs,
                linker_name=self.name,
            )
            candidate_result = insert_deterministic_edges(
                cur,
                source=f"_enricher:{self.name}:candidate_link",
                candidates=candidate_links,
            )
        return EnrichResult(
            enricher_name=self.name,
            n_edges_emitted=canonical_result.inserted + candidate_result.inserted,
            n_edges_skipped=canonical_result.skipped + candidate_result.skipped,
        )

    def _match_pairs(self, cur) -> dict[tuple[str, str, str], str]:
        pairs: dict[tuple[str, str, str], str] = {}
        project_ids_by_person = _project_ids_by_person(cur)
        if project_ids_by_person:
            _add_stakeholder_artifact_pairs(
                cur,
                pairs,
                project_ids_by_person=project_ids_by_person,
            )
            _add_paper_author_edge_pairs(
                cur,
                pairs,
                project_ids_by_person=project_ids_by_person,
            )
            _add_artifact_person_name_project_pairs(
                cur,
                pairs,
                project_ids_by_person=project_ids_by_person,
            )
        _add_artifact_project_alias_pairs(cur, pairs)
        _add_paper_author_name_pairs(cur, pairs)
        _add_accepted_candidate_decision_pairs(cur, pairs)
        _add_thread_email_project_propagation(cur, pairs)
        _add_document_event_project_propagation(cur, pairs)
        return pairs


def _candidate_link_edges(
    cur,
    *,
    generation_id: int,
    canonical_pairs: dict[tuple[str, str, str], str],
    linker_name: str,
) -> list[DerivedEdgeCandidate]:
    canonical_targets = {
        (src, dst) for (src, _edge_type, dst) in canonical_pairs
    }
    canonical_targets.update(_existing_canonical_targets(cur))
    existing_candidates = _existing_candidate_targets(cur)

    dst_nodes = _candidate_spine_nodes(cur)
    if not dst_nodes:
        return []

    # Precompute each spine node's features ONCE and build an inverted
    # token -> spine index. `_candidate_match` requires >=1 shared token, so an
    # artifact can only match spine nodes that share a token with it; blocking on
    # the index scores just those pairs instead of the full artifact x spine
    # cross-product (and stops recomputing every spine node's features once per
    # artifact). Output is identical -- 0-overlap pairs were always rejected by
    # `_candidate_match`, and the final (score, dst_id) sort stays deterministic
    # regardless of the candidate-set iteration order.
    spine_entries = []
    token_to_spine: dict[str, list[int]] = {}
    for dst in dst_nodes:
        dst_id = str(dst["node_id"])
        dst_subtype = str(dst["subtype"])
        spine_features = _feature_tokens(
            dst_subtype,
            dict(dst["attrs"] or {}),
            _SPINE_FIELDS,
        )
        idx = len(spine_entries)
        spine_entries.append((dst_id, dst_subtype, spine_features))
        for token in spine_features.tokens:
            token_to_spine.setdefault(token, []).append(idx)

    candidates: list[DerivedEdgeCandidate] = []
    for artifact in _chronos.nodes(cur, _CANDIDATE_ARTIFACT_SUBTYPES):
        artifact_id = str(artifact["node_id"])
        artifact_subtype = str(artifact["subtype"])
        artifact_features = _feature_tokens(
            artifact_subtype,
            dict(artifact["attrs"] or {}),
            _ARTIFACT_FIELDS,
        )
        if not artifact_features.tokens:
            continue
        # Block to spine nodes sharing >=1 token (union of the inverted index).
        candidate_idxs: set[int] = set()
        for token in artifact_features.tokens:
            candidate_idxs.update(token_to_spine.get(token, ()))
        if not candidate_idxs:
            continue
        artifact_candidates: list[tuple[float, str, DerivedEdgeCandidate]] = []
        for idx in candidate_idxs:
            dst_id, dst_subtype, spine_features = spine_entries[idx]
            if artifact_id == dst_id:
                continue
            if (artifact_id, dst_id) in canonical_targets:
                continue
            if (artifact_id, dst_id) in existing_candidates:
                continue
            match = _candidate_match(
                artifact_features,
                spine_features,
            )
            if match is None:
                continue
            rule_id = (
                f"{artifact_subtype}_candidate_{dst_subtype}_"
                "by_token_overlap"
            )
            confidence_bucket = candidate_confidence_bucket(
                match["feature_classes"],
                match["match_signal_count"],
            )
            attrs = {
                # Stored wire-format id; unchanged by the module rename.
                "schema": "okg.academic_pi.candidate_link.v1",
                "candidate_state": "pending_review",
                "canonical_edge_type": (
                    "thread_about"
                    if artifact_subtype == "email_thread" else "mentions"
                ),
                "rule_id": rule_id,
                "confidence_bucket": confidence_bucket,
                "feature_classes": match["feature_classes"],
                "match_signal_count": match["match_signal_count"],
            }
            candidate = DerivedEdgeCandidate(
                src=artifact_id,
                edge_type="candidate_link",
                dst=dst_id,
                attrs=attrs,
                provenance="linked_heuristic",
                confidence=_CANDIDATE_CONFIDENCE,
                source_record_id={
                    "linker": linker_name,
                    "rule_id": rule_id,
                    "confidence_bucket": confidence_bucket,
                },
                source_revision={"generation_id": generation_id},
            )
            artifact_candidates.append(
                (
                    float(match["score"]),
                    dst_id,
                    candidate,
                )
            )
        artifact_candidates.sort(
            key=lambda item: (item[0], item[1]),
            reverse=True,
        )
        candidates.extend(
            candidate
            for _score, _dst_id, candidate in (
                artifact_candidates[:_CANDIDATE_MAX_PER_ARTIFACT]
            )
        )
    return candidates


def _existing_canonical_targets(cur) -> set[tuple[str, str]]:
    rows = _chronos.query(
        cur,
        """
        SELECT e.src, e.dst
          FROM okg.graph_edges e
          JOIN okg.graph_nodes src ON src.node_id = e.src
          JOIN okg.graph_nodes dst ON dst.node_id = e.dst
         WHERE src.subtype = ANY(:artifact_subtypes)
           AND dst.subtype = ANY(:spine_subtypes)
           AND e.edge_type = ANY(:edge_types)
        """,
        {
            "artifact_subtypes": list(_CANDIDATE_ARTIFACT_SUBTYPES),
            "spine_subtypes": list(_CANDIDATE_SPINE_SUBTYPES),
            "edge_types": ["mentions", "thread_about"],
        },
    )
    return {(str(row["src"]), str(row["dst"])) for row in rows}


def _existing_candidate_targets(cur) -> set[tuple[str, str]]:
    rows = _chronos.query(
        cur,
        """
        SELECT src, dst
          FROM okg.graph_edges
         WHERE edge_type = 'candidate_link'
        """,
    )
    return {(str(row["src"]), str(row["dst"])) for row in rows}


def _candidate_spine_nodes(cur) -> list[dict[str, Any]]:
    return _chronos.nodes(cur, _CANDIDATE_SPINE_SUBTYPES)


@dataclass(frozen=True)
class _FeatureTokens:
    tokens: set[str]
    classes_by_token: dict[str, set[str]]


def _feature_tokens(
    subtype: str,
    attrs: dict[str, Any],
    field_map: dict[str, tuple[tuple[str, str], ...]],
) -> _FeatureTokens:
    classes_by_token: dict[str, set[str]] = {}
    for feature_class, field in field_map.get(subtype, ()):
        for token in _tokens_from_value(attrs.get(field)):
            classes_by_token.setdefault(token, set()).add(feature_class)
    return _FeatureTokens(
        tokens=set(classes_by_token),
        classes_by_token=classes_by_token,
    )


def _candidate_match(
    artifact_features: _FeatureTokens,
    spine_features: _FeatureTokens,
) -> dict[str, Any] | None:
    overlap = artifact_features.tokens & spine_features.tokens
    if not overlap:
        return None
    distinctive = [token for token in overlap if len(token) >= 6]
    if len(overlap) < 2 and not distinctive:
        return None
    feature_classes: set[str] = set()
    for token in overlap:
        feature_classes.update(artifact_features.classes_by_token[token])
        feature_classes.update(spine_features.classes_by_token[token])
    dst_denominator = max(len(spine_features.tokens), 1)
    score = min(0.84, 0.50 + (0.06 * len(overlap)) + (
        0.10 * min(len(overlap) / dst_denominator, 1.0)
    ))
    return {
        "score": round(score, 3),
        "match_signal_count": len(overlap),
        "feature_classes": sorted(feature_classes),
    }


def _tokens_from_value(value: Any) -> set[str]:
    out: set[str] = set()
    for text in _text_values(value):
        for token in _normalize_name(text).split():
            if _is_informative_candidate_token(token):
                out.add(token)
    return out


def _text_values(value: Any) -> Iterable[str]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, dict):
        values: list[str] = []
        for item in value.values():
            values.extend(_text_values(item))
        return tuple(values)
    if isinstance(value, (list, tuple, set)):
        values = []
        for item in value:
            values.extend(_text_values(item))
        return tuple(values)
    return (str(value),)


def _is_informative_candidate_token(token: str) -> bool:
    return (
        len(token) >= 3
        and not token.isdigit()
        and token not in _TOKEN_DENYLIST
        and token not in _PROJECT_SINGLE_TOKEN_DENYLIST
    )


def _project_ids_by_person(cur) -> dict[str, set[str]]:
    rows = _chronos.query(
        cur,
        """
        SELECT pc.src AS project_id,
               si.dst AS person_id
          FROM okg.graph_edges pc
          JOIN okg.graph_nodes project
            ON project.node_id = pc.src
           AND project.subtype = 'project'
          JOIN okg.graph_nodes stakeholder
            ON stakeholder.node_id = pc.dst
           AND stakeholder.subtype = 'project_stakeholder'
          JOIN okg.graph_edges si
            ON si.src = stakeholder.node_id
           AND si.edge_type = 'identifies'
          JOIN okg.graph_nodes person
            ON person.node_id = si.dst
           AND person.subtype = 'person'
         WHERE pc.edge_type = 'contains'
        """,
    )
    out: dict[str, set[str]] = {}
    for row in rows:
        out.setdefault(str(row["person_id"]), set()).add(str(row["project_id"]))
    return out


def _add_stakeholder_artifact_pairs(
    cur,
    pairs: dict[tuple[str, str, str], str],
    *,
    project_ids_by_person: dict[str, set[str]],
) -> None:
    thread_rows = _chronos.query(
        cur,
        """
        SELECT e.src AS artifact_id,
               e.dst AS person_id
          FROM okg.graph_edges e
          JOIN okg.graph_nodes artifact
            ON artifact.node_id = e.src
           AND artifact.subtype = 'email_thread'
          JOIN okg.graph_nodes person
            ON person.node_id = e.dst
           AND person.subtype = 'person'
         WHERE e.edge_type = 'has_member'
        """,
    )
    for row in thread_rows:
        _add_person_projects(
            pairs,
            artifact_id=str(row["artifact_id"]),
            edge_type="thread_about",
            person_id=str(row["person_id"]),
            project_ids_by_person=project_ids_by_person,
            rule_id="thread_member_stakeholder_project",
        )

    person_to_artifact_rows = _chronos.query(
        cur,
        """
        SELECT e.src AS person_id,
               e.dst AS artifact_id,
               e.edge_type,
               artifact.subtype AS artifact_subtype
          FROM okg.graph_edges e
          JOIN okg.graph_nodes person
            ON person.node_id = e.src
           AND person.subtype = 'person'
          JOIN okg.graph_nodes artifact
            ON artifact.node_id = e.dst
           AND artifact.subtype = ANY(:artifact_subtypes)
         WHERE e.edge_type = ANY(:edge_types)
        """,
        {
            "artifact_subtypes": ["calendar_event", "document", "email"],
            "edge_types": ["attended", "authored", "cc_on", "received"],
        },
    )
    artifact_rule = {
        "calendar_event": "calendar_attendee_stakeholder_project",
        "document": "document_author_stakeholder_project",
        "email": "email_actor_stakeholder_project",
    }
    for row in person_to_artifact_rows:
        subtype = str(row["artifact_subtype"])
        _add_person_projects(
            pairs,
            artifact_id=str(row["artifact_id"]),
            edge_type="mentions",
            person_id=str(row["person_id"]),
            project_ids_by_person=project_ids_by_person,
            rule_id=artifact_rule[subtype],
        )


def _add_paper_author_edge_pairs(
    cur,
    pairs: dict[tuple[str, str, str], str],
    *,
    project_ids_by_person: dict[str, set[str]],
) -> None:
    rows = _chronos.query(
        cur,
        """
        SELECT e.src AS paper_id,
               e.dst AS person_id
          FROM okg.graph_edges e
          JOIN okg.graph_nodes paper
            ON paper.node_id = e.src
           AND paper.subtype = 'paper'
          JOIN okg.graph_nodes person
            ON person.node_id = e.dst
           AND person.subtype = 'person'
         WHERE e.edge_type = 'authored_by'
        """,
    )
    for row in rows:
        _add_person_projects(
            pairs,
            artifact_id=str(row["paper_id"]),
            edge_type="mentions",
            person_id=str(row["person_id"]),
            project_ids_by_person=project_ids_by_person,
            rule_id="paper_author_stakeholder_project",
        )


def _add_artifact_person_name_project_pairs(
    cur,
    pairs: dict[tuple[str, str, str], str],
    *,
    project_ids_by_person: dict[str, set[str]],
) -> None:
    projects_by_name: dict[str, set[str]] = {}
    for person in _chronos.nodes(cur, ("person",)):
        person_id = str(person["node_id"])
        projects = project_ids_by_person.get(person_id)
        if not projects:
            continue
        for name in _person_name_aliases(dict(person["attrs"] or {})):
            projects_by_name.setdefault(name, set()).update(projects)
    if not projects_by_name:
        return

    for row in _chronos.nodes(
        cur,
        ("calendar_event", "document", "email", "email_thread"),
    ):
        artifact_id = str(row["node_id"])
        subtype = str(row["subtype"])
        attrs = dict(row["attrs"] or {})
        text = _artifact_name_match_text(subtype, attrs)
        if not text:
            continue
        edge_type = "thread_about" if subtype == "email_thread" else "mentions"
        for name, project_ids in projects_by_name.items():
            if not _contains_normalized_phrase(text, name):
                continue
            for project_id in project_ids:
                pairs.setdefault(
                    (artifact_id, edge_type, project_id),
                    f"{subtype}_stakeholder_name_project",
                )


def _add_artifact_project_alias_pairs(
    cur,
    pairs: dict[tuple[str, str, str], str],
) -> None:
    projects_by_alias: dict[str, set[str]] = {}
    for project in _chronos.nodes(cur, ("project",)):
        project_id = str(project["node_id"])
        for alias in _project_name_aliases(dict(project["attrs"] or {})):
            projects_by_alias.setdefault(alias, set()).add(project_id)

    unique_projects_by_alias = {
        alias: next(iter(project_ids))
        for alias, project_ids in projects_by_alias.items()
        if len(project_ids) == 1
    }
    if not unique_projects_by_alias:
        return

    for row in _chronos.nodes(
        cur,
        ("calendar_event", "document", "email", "email_thread"),
    ):
        artifact_id = str(row["node_id"])
        subtype = str(row["subtype"])
        text = _artifact_name_match_text(subtype, dict(row["attrs"] or {}))
        if not text:
            continue
        edge_type = "thread_about" if subtype == "email_thread" else "mentions"
        for alias, project_id in unique_projects_by_alias.items():
            if not _contains_normalized_phrase(text, alias):
                continue
            pairs.setdefault(
                (artifact_id, edge_type, project_id),
                f"{subtype}_project_alias_project",
            )


def _add_paper_author_name_pairs(
    cur,
    pairs: dict[tuple[str, str, str], str],
) -> None:
    people = _chronos.nodes(cur, ("person",))
    projects_by_author_name: dict[str, set[str]] = {}
    project_ids_by_person = _project_ids_by_person(cur)
    for row in people:
        person_id = str(row["node_id"])
        projects = project_ids_by_person.get(person_id)
        if not projects:
            continue
        for name in _person_name_aliases(dict(row["attrs"] or {})):
            projects_by_author_name.setdefault(name, set()).update(projects)
    if not projects_by_author_name:
        return

    for paper in _chronos.nodes(cur, ("paper",)):
        paper_id = str(paper["node_id"])
        attrs = dict(paper["attrs"] or {})
        for author in _paper_author_names(attrs.get("authors")):
            for project_id in projects_by_author_name.get(author, ()):
                pairs.setdefault(
                    (paper_id, "mentions", project_id),
                    "paper_author_name_stakeholder_project",
                )


def _add_accepted_candidate_decision_pairs(
    cur,
    pairs: dict[tuple[str, str, str], str],
) -> None:
    rows = _chronos.query(
        cur,
        """
        SELECT decision.src,
               decision.dst,
               src.subtype AS src_subtype,
               coalesce(
                   nullif(decision.attrs->>'canonical_edge_type', ''),
                   nullif(candidate.attrs->>'canonical_edge_type', ''),
                   CASE
                     WHEN src.subtype = 'email_thread'
                     THEN 'thread_about'
                     ELSE 'mentions'
                   END
               ) AS canonical_edge_type,
               coalesce(
                   nullif(decision.attrs->>'rule_id', ''),
                   nullif(candidate.attrs->>'rule_id', ''),
                   'candidate_review_accepted'
               ) AS rule_id
          FROM okg.graph_edges decision
          JOIN okg.graph_nodes src
            ON src.node_id = decision.src
           AND src.subtype = ANY(:artifact_subtypes)
          JOIN okg.graph_nodes dst
            ON dst.node_id = decision.dst
           AND dst.subtype = ANY(:spine_subtypes)
          LEFT JOIN okg.graph_edges candidate
            ON candidate.src = decision.src
           AND candidate.dst = decision.dst
           AND candidate.edge_type = 'candidate_link'
         WHERE decision.edge_type = 'candidate_link_decision'
           AND coalesce(decision.attrs->>'candidate_state', '') = 'accepted'
        """,
        {
            "artifact_subtypes": list(_CANDIDATE_ARTIFACT_SUBTYPES),
            "spine_subtypes": list(_CANDIDATE_SPINE_SUBTYPES),
        },
    )
    for row in rows:
        edge_type = str(row["canonical_edge_type"] or "")
        if edge_type not in {"mentions", "thread_about"}:
            continue
        if edge_type == "thread_about" and str(row["src_subtype"]) != "email_thread":
            continue
        pairs.setdefault(
            (str(row["src"]), edge_type, str(row["dst"])),
            f"candidate_review_accepted:{row['rule_id']}",
        )


def _add_thread_email_project_propagation(
    cur,
    pairs: dict[tuple[str, str, str], str],
) -> None:
    rows = _chronos.query(
        cur,
        """
        WITH thread_email AS (
            SELECT e.src AS thread_id,
                   e.dst AS email_id
              FROM okg.graph_edges e
              JOIN okg.graph_nodes thread
                ON thread.node_id = e.src
               AND thread.subtype = 'email_thread'
              JOIN okg.graph_nodes email
                ON email.node_id = e.dst
               AND email.subtype = 'email'
             WHERE e.edge_type = 'contains'
        ),
        email_project AS (
            SELECT e.src AS email_id,
                   e.dst AS project_id
              FROM okg.graph_edges e
              JOIN okg.graph_nodes email
                ON email.node_id = e.src
               AND email.subtype = 'email'
              JOIN okg.graph_nodes project
                ON project.node_id = e.dst
               AND project.subtype = 'project'
             WHERE e.edge_type = 'mentions'
        ),
        thread_project AS (
            SELECT e.src AS thread_id,
                   e.dst AS project_id
              FROM okg.graph_edges e
              JOIN okg.graph_nodes thread
                ON thread.node_id = e.src
               AND thread.subtype = 'email_thread'
              JOIN okg.graph_nodes project
                ON project.node_id = e.dst
               AND project.subtype = 'project'
             WHERE e.edge_type = 'thread_about'
        )
        SELECT DISTINCT te.thread_id AS src,
               'thread_about' AS edge_type,
               ep.project_id AS dst,
               'thread_about_from_email_project' AS rule_id
          FROM thread_email te
          JOIN email_project ep ON ep.email_id = te.email_id
        UNION
        SELECT DISTINCT te.email_id AS src,
               'mentions' AS edge_type,
               tp.project_id AS dst,
               'email_project_from_thread_about' AS rule_id
          FROM thread_email te
          JOIN thread_project tp ON tp.thread_id = te.thread_id
        """,
    )
    for row in rows:
        pairs.setdefault(
            (str(row["src"]), str(row["edge_type"]), str(row["dst"])),
            str(row["rule_id"]),
        )


def _add_document_event_project_propagation(
    cur,
    pairs: dict[tuple[str, str, str], str],
) -> None:
    rows = _chronos.query(
        cur,
        """
        WITH event_document AS (
            SELECT e.src AS event_id,
                   e.dst AS document_id
              FROM okg.graph_edges e
              JOIN okg.graph_nodes event
                ON event.node_id = e.src
               AND event.subtype = 'calendar_event'
              JOIN okg.graph_nodes document
                ON document.node_id = e.dst
               AND document.subtype = 'document'
             WHERE e.edge_type = 'has_document'
        ),
        email_document AS (
            SELECT e.src AS email_id,
                   e.dst AS document_id
              FROM okg.graph_edges e
              JOIN okg.graph_nodes email
                ON email.node_id = e.src
               AND email.subtype = 'email'
              JOIN okg.graph_nodes document
                ON document.node_id = e.dst
               AND document.subtype = 'document'
             WHERE e.edge_type = 'has_document'
        ),
        document_project AS (
            SELECT e.src AS document_id,
                   e.dst AS project_id
              FROM okg.graph_edges e
              JOIN okg.graph_nodes document
                ON document.node_id = e.src
               AND document.subtype = 'document'
              JOIN okg.graph_nodes project
                ON project.node_id = e.dst
               AND project.subtype = 'project'
             WHERE e.edge_type = 'mentions'
        ),
        event_project AS (
            SELECT e.src AS event_id,
                   e.dst AS project_id
              FROM okg.graph_edges e
              JOIN okg.graph_nodes event
                ON event.node_id = e.src
               AND event.subtype = 'calendar_event'
              JOIN okg.graph_nodes project
                ON project.node_id = e.dst
               AND project.subtype = 'project'
             WHERE e.edge_type = 'mentions'
        ),
        email_project AS (
            SELECT e.src AS email_id,
                   e.dst AS project_id
              FROM okg.graph_edges e
              JOIN okg.graph_nodes email
                ON email.node_id = e.src
               AND email.subtype = 'email'
              JOIN okg.graph_nodes project
                ON project.node_id = e.dst
               AND project.subtype = 'project'
             WHERE e.edge_type = 'mentions'
        )
        SELECT DISTINCT ed.event_id AS src,
               'mentions' AS edge_type,
               dp.project_id AS dst,
               'calendar_event_project_from_document' AS rule_id
          FROM event_document ed
          JOIN document_project dp ON dp.document_id = ed.document_id
        UNION
        SELECT DISTINCT ed.document_id AS src,
               'mentions' AS edge_type,
               ep.project_id AS dst,
               'document_project_from_calendar_event' AS rule_id
          FROM event_document ed
          JOIN event_project ep ON ep.event_id = ed.event_id
        UNION
        SELECT DISTINCT em.email_id AS src,
               'mentions' AS edge_type,
               dp.project_id AS dst,
               'email_project_from_body_document' AS rule_id
          FROM email_document em
          JOIN document_project dp ON dp.document_id = em.document_id
        UNION
        SELECT DISTINCT em.document_id AS src,
               'mentions' AS edge_type,
               ep.project_id AS dst,
               'document_project_from_email' AS rule_id
          FROM email_document em
          JOIN email_project ep ON ep.email_id = em.email_id
        """,
    )
    for row in rows:
        pairs.setdefault(
            (str(row["src"]), str(row["edge_type"]), str(row["dst"])),
            str(row["rule_id"]),
        )


def _add_person_projects(
    pairs: dict[tuple[str, str, str], str],
    *,
    artifact_id: str,
    edge_type: str,
    person_id: str,
    project_ids_by_person: dict[str, set[str]],
    rule_id: str,
) -> None:
    for project_id in project_ids_by_person.get(person_id, ()):
        pairs.setdefault((artifact_id, edge_type, project_id), rule_id)


def _person_name_aliases(attrs: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for key in ("display_name", "name"):
        out.update(_normalized_full_names([attrs.get(key)]))
    aliases = attrs.get("aliases")
    if isinstance(aliases, list):
        out.update(_normalized_full_names(aliases))
    return out


_PROJECT_SINGLE_TOKEN_DENYLIST = frozenset({
    "calendar",
    "code",
    "data",
    "draft",
    "drive",
    "email",
    "follow",
    "grant",
    "meeting",
    "notes",
    "paper",
    "papers",
    "plan",
    "project",
    "review",
    "source",
    "status",
    "submit",
    "sync",
    "system",
    "systems",
    "test",
    "tests",
    "update",
})


def _project_name_aliases(attrs: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for key in ("title", "name"):
        out.update(_normalized_project_aliases([attrs.get(key)]))
    aliases = attrs.get("aliases")
    if isinstance(aliases, list):
        out.update(_normalized_project_aliases(aliases))
    return out


def _paper_author_names(raw: Any) -> set[str]:
    if raw is None:
        return set()
    values: list[Any]
    if isinstance(raw, list):
        values = raw
    else:
        values = [raw]
    candidates: list[Any] = []
    for value in values:
        if isinstance(value, dict):
            candidates.extend(
                value.get(key)
                for key in ("name", "display_name", "full_name")
            )
        else:
            candidates.append(value)
    return _normalized_full_names(candidates)


def _normalized_full_names(values: Iterable[Any]) -> set[str]:
    out: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            continue
        name = _normalize_name(value)
        if len(name.split()) >= 2:
            out.add(name)
    return out


def _normalized_project_aliases(values: Iterable[Any]) -> set[str]:
    out: set[str] = set()
    for value in values:
        if not isinstance(value, str):
            continue
        alias = _normalize_name(value)
        tokens = alias.split()
        if len(tokens) >= 2:
            out.add(alias)
        elif len(tokens) == 1 and _allow_single_token_project_alias(tokens[0]):
            out.add(alias)
    return out


def _allow_single_token_project_alias(token: str) -> bool:
    return (
        len(token) >= 6
        and not token.isdigit()
        and token not in _PROJECT_SINGLE_TOKEN_DENYLIST
    )


def _normalize_name(value: str) -> str:
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", " ", value)
    return " ".join(value.split())


def _artifact_name_match_text(subtype: str, attrs: dict[str, Any]) -> str:
    if subtype == "email":
        return " ".join((
            str(attrs.get("subject") or ""),
            str(attrs.get("snippet") or ""),
        ))
    if subtype == "email_thread":
        return str(attrs.get("subject") or "")
    if subtype == "document":
        return " ".join((
            str(attrs.get("title") or ""),
            str(attrs.get("text") or ""),
        ))
    if subtype == "calendar_event":
        return " ".join((
            str(attrs.get("title") or ""),
            str(attrs.get("location") or ""),
        ))
    return ""


def _contains_normalized_phrase(text: str, phrase: str) -> bool:
    normalized_text = _normalize_name(text)
    if not normalized_text or not phrase:
        return False
    return f" {phrase} " in f" {normalized_text} "
