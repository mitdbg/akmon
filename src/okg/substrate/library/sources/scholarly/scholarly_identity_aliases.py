"""Deterministic identity helpers and alias source for Memex paper data."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Iterator

from okg.substrate.library.sources.base import EdgeFact, SourceRun

from ._common import slugify

__all__ = [
    "IdentityAliasSource",
    "IdentityMatch",
    "PaperIdentityIndex",
    "PersonIdentityIndex",
    "norm_arxiv",
    "norm_doi",
    "norm_orcid",
    "title_fingerprint",
]


PAPER_IDENTITY_PRIORITY: tuple[str, ...] = (
    "doi",
    "arxiv_id",
    "openalex_id",
    "semantic_scholar_id",
    "dblp_key",
    "inspire_recid",
    "openreview_forum",
    "title",
)

PERSON_IDENTITY_PRIORITY: tuple[str, ...] = (
    "orcid",
    "openalex_author_id",
    "semantic_scholar_author_id",
    "dblp_pid",
    "inspire_author_recid",
    "inspire_bai",
    "openreview_id",
)


@dataclass(frozen=True)
class IdentityMatch:
    node_id: str
    kind: str
    value: str = ""
    is_new: bool = False


@dataclass
class PaperIdentityIndex:
    by_identity: dict[tuple[str, str], str] = field(default_factory=dict)
    by_title: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dsn(cls, dsn: str | None) -> "PaperIdentityIndex":
        if not dsn:
            return cls()
        return cls.from_records(_load_live_nodes(dsn, "paper"))

    @classmethod
    def from_records(cls, records: Iterable[tuple[str, dict[str, Any]]]) -> "PaperIdentityIndex":
        index = cls()
        for node_id, attrs in sorted(records, key=_canonical_node_sort_key):
            for kind, value in paper_identity_values(attrs, include_title=False):
                index.by_identity.setdefault((kind, value), node_id)
            tfp = title_fingerprint(str(attrs.get("title") or attrs.get("label") or ""))
            if tfp:
                index.by_title.setdefault(tfp, node_id)
        return index

    def paper_id_for(self, record: dict[str, Any], *, default_id: str) -> IdentityMatch:
        for kind, value in paper_identity_values(record, include_title=False):
            node_id = self.by_identity.get((kind, value))
            if node_id:
                return IdentityMatch(node_id=node_id, kind=kind, value=value)
        tfp = title_fingerprint(str(record.get("title") or record.get("label") or ""))
        if tfp and tfp in self.by_title:
            return IdentityMatch(node_id=self.by_title[tfp], kind="title", value=tfp)
        return IdentityMatch(node_id=default_id, kind="new", is_new=True)


@dataclass
class PersonIdentityIndex:
    by_identity: dict[tuple[str, str], str] = field(default_factory=dict)

    @classmethod
    def from_dsn(cls, dsn: str | None) -> "PersonIdentityIndex":
        if not dsn:
            return cls()
        return cls.from_records(_load_live_nodes(dsn, "person"))

    @classmethod
    def from_records(cls, records: Iterable[tuple[str, dict[str, Any]]]) -> "PersonIdentityIndex":
        index = cls()
        for node_id, attrs in sorted(records, key=_canonical_node_sort_key):
            for kind, value in person_identity_values(attrs):
                index.by_identity.setdefault((kind, value), node_id)
        return index

    def person_id_for(self, record: dict[str, Any], *, default_id: str) -> IdentityMatch:
        for kind, value in person_identity_values(record):
            node_id = self.by_identity.get((kind, value))
            if node_id:
                return IdentityMatch(node_id=node_id, kind=kind, value=value)
        return IdentityMatch(node_id=default_id, kind="new", is_new=True)


@dataclass
class IdentityAliasSource:
    dsn: str | None = None
    fixture_path: str | Path | None = None
    include_title_aliases: bool = True
    max_alias_edges_per_run: int = 50000
    target_subtypes: list[str] | tuple[str, ...] = ("paper", "person")
    status: str = "enabled"
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)

    name: str = "identity_aliases"
    profile: str = "discovery_crawl"

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: dict[str, Any] | None = None,
        since_progress: dict[str, Any] | None = None,
    ) -> SourceRun:
        stats: dict[str, Any] = {
            "identity_groups": 0,
            "alias_edges": 0,
            "alias_edges_truncated": 0,
            "by_subtype": {},
            "by_kind": {},
        }

        def _facts() -> Iterator[EdgeFact]:
            records = self._load_records()
            emitted: set[tuple[str, str, str]] = set()
            for subtype in self.target_subtypes:
                groups = _identity_groups_for_subtype(
                    records,
                    subtype,
                    include_title_aliases=self.include_title_aliases,
                )
                for kind, value, members in groups:
                    if len(members) < 2:
                        continue
                    stats["identity_groups"] += 1
                    stats["by_subtype"][subtype] = stats["by_subtype"].get(subtype, 0) + 1
                    stats["by_kind"][kind] = stats["by_kind"].get(kind, 0) + 1
                    canonical_id = _canonical_member(members)[0]
                    for node_id, attrs in members:
                        if node_id == canonical_id:
                            continue
                        edge_key = (node_id, canonical_id, "same_as")
                        if edge_key in emitted:
                            continue
                        if stats["alias_edges"] >= self.max_alias_edges_per_run:
                            stats["alias_edges_truncated"] += 1
                            continue
                        emitted.add(edge_key)
                        stats["alias_edges"] += 1
                        yield EdgeFact(
                            src=node_id,
                            dst=canonical_id,
                            edge_type="same_as",
                            attrs={
                                "source_provider": "identity_aliases",
                                "identity_kind": kind,
                                "identity_value_hash": _value_hash(value),
                                "identity_subtype": subtype,
                            },
                            source_record_id={
                                "source": "identity_aliases",
                                "subtype": subtype,
                                "kind": kind,
                                "value_hash": _value_hash(value),
                                "alias": node_id,
                                "canonical": canonical_id,
                            },
                            source_revision={
                                "algorithm": "memex-deterministic-identity-v1",
                                "members_hash": _members_hash(members),
                            },
                        )
            self.last_run_stats = stats

        return SourceRun(
            facts=_facts(),
            completed_scope=mode in {"scope_complete", "reconcile"},
            next_cursor={"source": self.name, "run_id": run_id},
            run_mode=mode,
        )

    def _load_records(self) -> list[tuple[str, str, dict[str, Any]]]:
        if self.fixture_path:
            raw = json.loads(Path(self.fixture_path).read_text(encoding="utf-8"))
            nodes = raw.get("nodes", raw) if isinstance(raw, dict) else raw
            return [
                (str(n["node_id"]), str(n["subtype"]), dict(n.get("attrs") or {}))
                for n in nodes
                if isinstance(n, dict) and n.get("node_id") and n.get("subtype")
            ]
        if not self.dsn:
            return []
        from okg.substrate.db.connection import connect as db_connect

        from okg.substrate.graph_store import ChronosGraphStore

        # Chronos cutover: identity-alias detection reads current
        # default-branch graph state ("which paper/person nodes exist
        # now"), so it resolves through a Chronos checkout over
        # okg.graph_nodes; the legacy `valid_to_gen IS NULL` pin is
        # dropped (the checkout session injects visibility).
        with db_connect(self.dsn) as conn:
            graph = ChronosGraphStore.from_connection(
                conn, ensure_schema=False
            ).checkout()
            rows = graph.query(
                """
                SELECT node_id, subtype, attrs
                  FROM okg.graph_nodes
                 WHERE subtype = ANY(:subtypes)
                """,
                {"subtypes": list(self.target_subtypes)},
            )
            return [
                (str(row["node_id"]), str(row["subtype"]), dict(row["attrs"] or {}))
                for row in rows
            ]


def paper_identity_values(
    attrs: dict[str, Any],
    *,
    include_title: bool,
    title_with_year: bool = False,
) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    _append(values, "doi", norm_doi(str(attrs.get("doi") or attrs.get("crossref_doi") or "")))
    _append(values, "arxiv_id", norm_arxiv(str(attrs.get("arxiv_id") or attrs.get("arxiv") or "")))
    _append(values, "openalex_id", norm_id(str(attrs.get("openalex_id") or attrs.get("openalex_work_id") or "")))
    _append(
        values,
        "semantic_scholar_id",
        norm_id(
            str(
                attrs.get("semantic_scholar_id")
                or attrs.get("semantic_scholar_paper_id")
                or attrs.get("s2_paper_id")
                or ""
            )
        ),
    )
    _append(values, "dblp_key", norm_id(str(attrs.get("dblp_key") or "")))
    _append(values, "inspire_recid", norm_id(str(attrs.get("inspire_recid") or "")))
    _append(values, "openreview_forum", norm_id(str(attrs.get("openreview_forum") or attrs.get("forum_id") or "")))
    if include_title:
        title = title_fingerprint(str(attrs.get("title") or attrs.get("label") or ""))
        if title:
            if title_with_year:
                year = str(attrs.get("year") or "").strip()
                if year:
                    title = f"{title}|{year}"
            _append(values, "title", title)
    return _dedupe_values(values, PAPER_IDENTITY_PRIORITY)


def person_identity_values(attrs: dict[str, Any]) -> list[tuple[str, str]]:
    values: list[tuple[str, str]] = []
    _append(values, "orcid", norm_orcid(str(attrs.get("orcid") or attrs.get("ORCID") or "")))
    _append(values, "openalex_author_id", norm_id(str(attrs.get("openalex_author_id") or "")))
    _append(
        values,
        "semantic_scholar_author_id",
        norm_id(str(attrs.get("semantic_scholar_author_id") or attrs.get("s2_author_id") or "")),
    )
    _append(values, "dblp_pid", norm_id(str(attrs.get("dblp_pid") or attrs.get("pid") or "")))
    _append(values, "inspire_author_recid", norm_id(str(attrs.get("inspire_author_recid") or attrs.get("recid") or "")))
    _append(values, "inspire_bai", norm_id(str(attrs.get("inspire_bai") or attrs.get("bai") or "")))
    _append(values, "openreview_id", norm_id(str(attrs.get("openreview_id") or "")))
    return _dedupe_values(values, PERSON_IDENTITY_PRIORITY)


def norm_doi(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"^https?://(dx\.)?doi\.org/", "", value)
    value = re.sub(r"^doi:", "", value)
    return value.strip()


def norm_arxiv(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"^arxiv:", "", value)
    value = re.sub(r"^https?://arxiv\.org/abs/", "", value)
    value = re.sub(r"v\d+$", "", value)
    return value.strip()


def norm_orcid(value: str) -> str:
    value = value.strip()
    value = re.sub(r"^https?://orcid\.org/", "", value, flags=re.IGNORECASE)
    value = value.replace(" ", "")
    return value.upper()


def norm_id(value: str) -> str:
    return value.strip().lower()


def title_fingerprint(title: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", " ", title.lower()).strip()
    return " ".join(cleaned.split())


def fallback_paper_id(provider: str, provider_id: str) -> str:
    return f"paper:{provider}:{slugify(provider_id)}"


def fallback_person_id(provider: str, provider_id: str) -> str:
    return f"person:{provider}:{slugify(provider_id)}"


def _load_live_nodes(dsn: str, subtype: str) -> list[tuple[str, dict[str, Any]]]:
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.graph_store import ChronosGraphStore

    # Chronos cutover: the identity index is built from current
    # default-branch graph state, so it resolves through a Chronos
    # checkout over okg.graph_nodes; the legacy `valid_to_gen IS NULL`
    # pin is dropped (the checkout session injects visibility).
    with db_connect(dsn) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout()
        rows = graph.query(
            """
            SELECT node_id, attrs
              FROM okg.graph_nodes
             WHERE subtype = :subtype
            """,
            {"subtype": subtype},
        )
        return [(str(row["node_id"]), dict(row["attrs"] or {})) for row in rows]


def _identity_groups_for_subtype(
    records: list[tuple[str, str, dict[str, Any]]],
    subtype: str,
    *,
    include_title_aliases: bool,
) -> list[tuple[str, str, list[tuple[str, dict[str, Any]]]]]:
    groups: dict[tuple[str, str], list[tuple[str, dict[str, Any]]]] = {}
    for node_id, node_subtype, attrs in records:
        if node_subtype != subtype:
            continue
        if subtype == "paper":
            values = paper_identity_values(
                attrs,
                include_title=include_title_aliases,
                title_with_year=True,
            )
        elif subtype == "person":
            values = person_identity_values(attrs)
        else:
            values = []
        for kind, value in values:
            groups.setdefault((kind, value), []).append((node_id, attrs))
    priority = PAPER_IDENTITY_PRIORITY if subtype == "paper" else PERSON_IDENTITY_PRIORITY
    return [
        (kind, value, sorted(members, key=_canonical_node_sort_key))
        for (kind, value), members in sorted(
            groups.items(),
            key=lambda item: (priority.index(item[0][0]) if item[0][0] in priority else 999, item[0][1]),
        )
        if len(members) > 1
    ]


def _canonical_member(members: list[tuple[str, dict[str, Any]]]) -> tuple[str, dict[str, Any]]:
    return sorted(members, key=_canonical_node_sort_key)[0]


def _canonical_node_sort_key(item: tuple[str, dict[str, Any]]) -> tuple[int, int, str]:
    node_id, attrs = item
    is_stub = str(attrs.get("is_reference_stub") or "false").lower() == "true"
    provider = str(attrs.get("source_provider") or "")
    provider_rank = {
        "legacy_paper_cache": 0,
        "crossref": 1,
        "dblp": 2,
        "inspire_hep": 3,
        "openreview": 4,
    }.get(provider, 9)
    return (1 if is_stub else 0, provider_rank, node_id)


def _append(values: list[tuple[str, str]], kind: str, value: str) -> None:
    if value:
        values.append((kind, value))


def _dedupe_values(values: list[tuple[str, str]], priority: tuple[str, ...]) -> list[tuple[str, str]]:
    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str]] = []
    for kind, value in sorted(values, key=lambda item: (priority.index(item[0]) if item[0] in priority else 999, item[1])):
        key = (kind, value)
        if key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def _value_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _members_hash(members: list[tuple[str, dict[str, Any]]]) -> str:
    payload = json.dumps([node_id for node_id, _attrs in sorted(members)], sort_keys=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()
