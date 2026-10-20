"""BibReferencesSource — substrate-library source for bibliographic ingest.

Reads `.bib` (BibTeX) files at glob-resolved paths and emits
`Paper` (kind=foundational), `Person` (authors), and `Venue` nodes,
plus `authored_by` / `published_in` / `member_of` edges.

Per `add-substrate-bibliographic-sources` (Phase 2 of impl-A4).

BibTeX `annote` field convention:
    annote = {tier: 1, auto_extract: true}

The `annote` field is parsed for `tier` (1|2|3, default 1 for
foundational works per umbrella D7) and `auto_extract` (boolean
opt-in to A2's semantic extractor at publish time).

Substrate-library purity: no memex-specific paths. Deployments
configure `bib_paths` (a list of glob patterns) and optional
`cache_root` for any provenance side files. Author canonicalization
defers to the substrate alias table (populated by
`AuthorAliasesSource`).
"""
from __future__ import annotations

import glob as _glob
import hashlib
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Mapping, Optional

from okg.substrate.errors import SubstrateError

from ._common import slugify
from okg.substrate.library.sources.base import (
    EdgeFact,
    EdgeKey,
    NodeFact,
    RecordEmission,
    RecordNode,
    RecordSet,
    SourceRun,
    cursor_has_record_set_authority,
    preserve_cursor_record_set_authority,
)

__all__ = [
    "BibReferencesSource",
    "ProjectBibliographyIndex",
    "ProjectBibDeclaration",
    "build_project_bibliography_index",
]


_AUTHOR_SEP = re.compile(r"\s+and\s+")
_CONFERENCE_ENTRY_TYPES = frozenset({"conference", "inproceedings", "proceedings"})
BIB_REFERENCES_PROJECT_SCHEMA_VERSION = 1


class LatexProjectLegacyMismatch(SubstrateError):
    """Configured legacy files/cursor do not match the live owned graph."""

    code = "latex_project_legacy_mismatch"


def assert_legacy_record_set_matches_graph(
    *, dsn: str | None, source_name: str | None, record_set: RecordSet,
) -> None:
    """Fail closed unless regenerated legacy authority equals the live owner."""
    if not dsn or not source_name:
        raise LatexProjectLegacyMismatch(
            "Legacy handoff requires dsn and the exact existing source_name",
            data={"source_name": source_name, "dsn_configured": bool(dsn)},
        )
    from okg.substrate.graph_store import ChronosGraphStore

    expected_nodes = sorted({
        node_id
        for record in record_set.records.values()
        for node_id in record.nodes_by_id()
    })
    expected_edges = sorted({
        (edge.src, edge.edge_type, edge.dst)
        for record in record_set.records.values()
        for edge in record.edges_by_identity().values()
    })
    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        observed_nodes = sorted(
            row["node_id"] for row in store.query(
                "SELECT node_id FROM okg.graph_nodes "
                "WHERE source = :source ORDER BY node_id",
                {"source": source_name},
            )
        )
        observed_edges = sorted(
            (row["src"], row["edge_type"], row["dst"])
            for row in store.query(
                "SELECT src, edge_type, dst FROM okg.graph_edges "
                "WHERE source = :source ORDER BY src, edge_type, dst",
                {"source": source_name},
            )
        )
    finally:
        store.close()
    if observed_nodes != expected_nodes or observed_edges != expected_edges:
        raise LatexProjectLegacyMismatch(
            "Regenerated legacy authority does not match the live owned graph",
            data={
                "source_name": source_name,
                "expected_nodes": expected_nodes,
                "observed_nodes": observed_nodes,
                "expected_edges": expected_edges,
                "observed_edges": observed_edges,
            },
            fix_hint=(
                "Restore the exact legacy paths/state or reconcile the legacy "
                "owner before attempting project identity cutover."
            ),
        )


def _parse_annote(annote: str | None) -> dict[str, Any]:
    """Parse a free-form annote field into structured attrs.

    Accepts comma- or newline-separated `key: value` pairs.
    Unknown keys are kept verbatim in the returned dict.
    """
    out: dict[str, Any] = {}
    if not annote:
        return out
    for part in re.split(r"[,;\n]+", annote):
        part = part.strip()
        if not part or ":" not in part:
            continue
        key, _, value = part.partition(":")
        key = key.strip().lower()
        value = value.strip()
        if key == "tier":
            try:
                out["tier"] = int(value)
            except ValueError:
                out["tier"] = value
        elif key == "auto_extract":
            out["auto_extract"] = value.lower() in {"true", "1", "yes"}
        else:
            out[key] = value
    return out


def _author_split(authors_field: str | None) -> list[str]:
    if not authors_field:
        return []
    return [a.strip() for a in _AUTHOR_SEP.split(authors_field) if a.strip()]


def _person_node_id(author_name: str) -> str:
    return f"person:{slugify(author_name)}"


def _paper_node_id(entry: dict[str, Any]) -> str:
    """Canonical paper node id, preferring DOI > arxiv_id > cite_key."""
    doi = entry.get("doi")
    if doi:
        return f"paper:doi:{doi}"
    arxiv_id = entry.get("eprint") or entry.get("arxiv")
    if arxiv_id:
        return f"paper:arxiv:{arxiv_id}"
    cite_key = entry.get("ID") or entry.get("id") or entry.get("citekey")
    if cite_key:
        return f"paper:bib:{slugify(cite_key)}"
    title = entry.get("title") or "unknown"
    return f"paper:bib:{slugify(title)[:64]}"


def _normalized_bib_key(value: Any) -> str:
    """Normalize surrounding Unicode/space without folding BibTeX key case."""
    return unicodedata.normalize("NFC", str(value or "")).strip()


def _normalized_doi(value: Any) -> str | None:
    raw = unicodedata.normalize("NFC", str(value or "")).strip()
    if not raw:
        return None
    raw = re.sub(r"^(?:https?://(?:dx\.)?doi\.org/|doi:\s*)", "", raw, flags=re.I)
    return raw.strip().lower() or None


def _normalized_arxiv_id(value: Any) -> str | None:
    raw = unicodedata.normalize("NFC", str(value or "")).strip()
    if not raw:
        return None
    raw = re.sub(r"^arxiv:\s*", "", raw, flags=re.I)
    raw = re.sub(r"v\d+$", "", raw, flags=re.I)
    return raw.strip().lower() or None


def _scoped_paper_node_id(
    entry: Mapping[str, Any], *, project_id: str, bib_path: str,
) -> str:
    doi = _normalized_doi(entry.get("doi"))
    if doi:
        return f"paper:doi:{doi}"
    arxiv_id = _normalized_arxiv_id(entry.get("eprint") or entry.get("arxiv"))
    if arxiv_id:
        return f"paper:arxiv:{arxiv_id}"
    key = _normalized_bib_key(
        entry.get("ID") or entry.get("id") or entry.get("citekey")
    )
    seed = "\0".join((project_id, bib_path, key))
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:32]
    return f"paper:bibscoped:{digest}"


@dataclass(frozen=True)
class ProjectBibDeclaration:
    project_id: str
    bib_path: str
    normalized_key: str
    paper_id: str
    entry: Mapping[str, Any]


@dataclass(frozen=True)
class ProjectBibliographyIndex:
    """Project-local BibTeX declarations and conservative key resolution."""

    declarations: tuple[ProjectBibDeclaration, ...]

    def candidates(
        self, project_id: str, normalized_key: str,
    ) -> tuple[ProjectBibDeclaration, ...]:
        return tuple(
            declaration
            for declaration in self.declarations
            if declaration.project_id == project_id
            and declaration.normalized_key == normalized_key
        )

    def resolve(
        self, project_id: str, normalized_key: str,
    ) -> tuple[str, str | None, tuple[str, ...]]:
        declarations = self.candidates(project_id, normalized_key)
        paths = tuple(sorted({item.bib_path for item in declarations}))
        if not declarations:
            return "unresolved", None, paths
        paper_ids = {item.paper_id for item in declarations}
        if len(paper_ids) == 1:
            return "resolved", next(iter(paper_ids)), paths
        return "ambiguous", None, paths


def build_project_bibliography_index(
    manifest: Any, source_root: str | Path,
) -> ProjectBibliographyIndex:
    """Parse only bibliography resources selected by the shared manifest."""
    import bibtexparser

    root = Path(source_root).expanduser().resolve()
    declarations: list[ProjectBibDeclaration] = []
    for project in manifest.projects:
        for item in project.files:
            if "bibliography" not in item.roles:
                continue
            bib_path = item.path
            with (root / bib_path).open("r", encoding="utf-8") as fh:
                database = bibtexparser.load(fh)
            for entry in database.entries:
                key = _normalized_bib_key(
                    entry.get("ID") or entry.get("id") or entry.get("citekey")
                )
                if not key:
                    continue
                declarations.append(ProjectBibDeclaration(
                    project_id=project.paper_id,
                    bib_path=bib_path,
                    normalized_key=key,
                    paper_id=_scoped_paper_node_id(
                        entry, project_id=project.paper_id, bib_path=bib_path,
                    ),
                    entry=dict(entry),
                ))
    declarations.sort(key=lambda item: (
        item.project_id, item.bib_path, item.normalized_key, item.paper_id,
    ))
    return ProjectBibliographyIndex(tuple(declarations))


def _entry_type(entry: Mapping[str, Any]) -> str:
    return str(entry.get("ENTRYTYPE") or entry.get("entrytype") or "unknown").strip().lower()


def _venue_name(entry: Mapping[str, Any], entry_type: str) -> Optional[str]:
    if entry_type == "article":
        return entry.get("journal")
    if entry_type in {"conference", "inproceedings"}:
        return entry.get("booktitle")
    if entry_type == "proceedings":
        return entry.get("booktitle") or entry.get("title")
    return entry.get("journal") or entry.get("booktitle")


def _venue_alias_key(value: Any) -> str:
    """Normalize only enough to address an operator-supplied alias map."""
    raw = unicodedata.normalize("NFKC", str(value or "")).strip().casefold()
    raw = raw.replace("&", " and ")
    return " ".join(
        "".join(char if char.isalnum() else " " for char in raw).split()
    )


def _build_venue_alias_index(
    aliases: Mapping[str, str] | None,
) -> dict[str, str]:
    configured = dict(aliases or {})
    index: dict[str, str] = {}
    declared: dict[str, str] = {}
    for raw_alias, raw_canonical in sorted(
        configured.items(), key=lambda item: (str(item[0]), str(item[1]))
    ):
        if not isinstance(raw_alias, str) or not isinstance(raw_canonical, str):
            raise SubstrateError(
                "BibReferencesSource: venue_aliases keys and values must be strings"
            )
        alias = unicodedata.normalize("NFKC", raw_alias).strip()
        canonical = unicodedata.normalize("NFKC", raw_canonical).strip()
        alias_key = _venue_alias_key(alias)
        canonical_key = _venue_alias_key(canonical)
        if not alias_key or not canonical_key:
            raise SubstrateError(
                "BibReferencesSource: venue_aliases keys and values must be non-empty"
            )
        prior = declared.get(alias_key)
        if prior is not None and prior != canonical:
            raise SubstrateError(
                "BibReferencesSource: normalized venue alias is ambiguous",
                data={"alias": raw_alias, "canonical": canonical, "prior": prior},
            )
        declared[alias_key] = canonical

    for alias_key, canonical in declared.items():
        canonical_key = _venue_alias_key(canonical)
        target = declared.get(canonical_key)
        if (
            target is not None
            and canonical_key != alias_key
            and target != canonical
        ):
            raise SubstrateError(
                "BibReferencesSource: venue_aliases chains or cycles are not allowed",
                data={"alias": alias_key, "canonical": canonical, "target": target},
            )
        index[alias_key] = canonical
        prior = index.get(canonical_key)
        if prior is not None and prior != canonical:
            raise SubstrateError(
                "BibReferencesSource: normalized venue canonical is ambiguous",
                data={"canonical": canonical, "prior": prior},
            )
        index[canonical_key] = canonical
    return index


def _canonical_venue_name(value: str, alias_index: Mapping[str, str]) -> str:
    normalized = unicodedata.normalize("NFKC", value).strip()
    return alias_index.get(_venue_alias_key(normalized), normalized)


def _venue_node_id(venue_name: str) -> str:
    return f"venue:{slugify(venue_name)}"


def _conference_edition_id(venue_name: str, year: str) -> str:
    return f"confed:{slugify(venue_name)}-{year}"


def _conference_id(venue_name: str) -> str:
    return f"conf:{slugify(venue_name)}"


@dataclass
class BibReferencesSource:
    """Read `.bib` files and emit scholarly NodeFacts/EdgeFacts."""

    bib_paths: list[str] | tuple[str, ...] = field(default_factory=tuple)
    project_manifest: Any | None = None
    source_root: str | Path | None = None
    migration: Mapping[str, Any] | None = None
    dsn: str | None = None
    source_name: str | None = None
    default_tier: int = 1
    default_kind: str = "foundational"
    cache_root: str | Path | None = None
    venue_aliases: Mapping[str, str] | None = None
    target_subtypes: list[str] | tuple[str, ...] = (
        "paper",
        "person",
        "venue",
        "conference",
        "conference_edition",
    )
    status: str = "enabled"
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)
    _venue_alias_index: dict[str, str] = field(
        default_factory=dict, init=False, repr=False
    )

    name: str = "bib_references"
    profile: str = "discovery_crawl"

    # Incremental-source contract (issue #323): the scope is a set of local
    # `.bib` files resolved from `bib_paths` globs — local bytes with no git
    # head, append offset, or upstream watermark — so the probe is the
    # O(content) ContentHashProbe floor: read+hash the CURRENT in-scope files
    # and skip parse/extraction on no-change. The token is a membership-sensitive
    # digest over `(rel_key, sha256(bytes))` plus the count, so an edit (sha
    # shifts), an add (new entry + count), AND a delete (dropped entry + count)
    # all bust it — content-based, hence immune to mtime churn. The config hash
    # folds in every emission-determining knob (bib_paths, default_tier,
    # default_kind, target_subtypes). A glob that resolves to zero files RAISES
    # in `_resolved_paths`, which propagates through the probe to a full read
    # (run_probe maps any probe error to changed=true) — never a silent skip.
    change_probe_kind = "content_hash"

    @property
    def probe_short_circuit_safe(self) -> bool:
        """No exact immutable applied-snapshot acknowledgement is returned."""
        return False

    def __post_init__(self) -> None:
        self._venue_alias_index = _build_venue_alias_index(self.venue_aliases)
        self._project_manifest_config: dict[str, Any] | None = None
        if isinstance(self.project_manifest, Mapping):
            self._project_manifest_config = dict(self.project_manifest)
            self.project_manifest = self._build_project_manifest()
        if self.project_manifest is not None and self.source_root is None:
            raise SubstrateError(
                "BibReferencesSource: source_root is required in project mode",
                fix_hint="Pass the same source_root used to build project_manifest.",
            )
        if not self.bib_paths and self.project_manifest is None:
            raise SubstrateError(
                "BibReferencesSource: bib_paths is required unless "
                "project_manifest is supplied.",
                fix_hint=(
                    "Set `bib_paths: [<glob>, ...]` in the deployment's "
                    "source_registry.yaml entry."
                ),
            )
        migration = dict(self.migration or {})
        if migration:
            legacy_owner = migration.get("legacy_ownership_id")
            if not isinstance(legacy_owner, str) or not legacy_owner.strip():
                raise SubstrateError(
                    "BibReferencesSource migration requires legacy_ownership_id",
                )
            if self.source_name != legacy_owner:
                raise SubstrateError(
                    "BibReferencesSource migration ownership does not match source_name",
                    data={
                        "legacy_ownership_id": legacy_owner,
                        "source_name": self.source_name,
                    },
                )
        self.change_probe = self._build_change_probe()

    def _build_project_manifest(self) -> Any:
        if self.source_root is None:
            raise SubstrateError(
                "BibReferencesSource: source_root is required in project mode",
            )
        assert self._project_manifest_config is not None
        from .latex_project_manifest import LatexProjectManifestBuilder

        return LatexProjectManifestBuilder(
            source_root=self.source_root,
            **self._project_manifest_config,
        ).build()

    def _refresh_project_manifest(self) -> None:
        if self._project_manifest_config is not None:
            self.project_manifest = self._build_project_manifest()

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )

        from okg.substrate.library.sources.scholarly import _common

        def _items() -> list[tuple[str, Path]]:
            # Root discovery and bibliography closure are live inputs. Refresh
            # first so an added automatic project/bibliography changes the
            # membership-sensitive token even when old BibTeX bytes are stable.
            self._refresh_project_manifest()
            # Re-enumerate live `.bib` files on every probe call so a deleted
            # file is observed as a dropped (key, sha) entry. Key by absolute
            # path string (paths come from arbitrary globs, not one root) for a
            # stable, collision-free membership key.
            items: list[tuple[str, Any]] = [
                (str(path), path) for path in self._resolved_paths()
            ]
            if self.project_manifest is not None:
                # Bib facts may be unchanged while any manuscript edit changes
                # the shared manifest digest. The cursor still must advance so
                # LatexProjectSource's exact-digest handoff cannot deadlock.
                items.append((
                    "manifest::digest",
                    self.project_manifest.digest.encode("utf-8"),
                ))
            return items

        return ContentHashProbe(
            content_items=_items,
            config={
                "bib_paths": list(self.bib_paths),
                "project_manifest_digest": getattr(
                    self.project_manifest, "digest", None,
                ),
                "default_tier": self.default_tier,
                "default_kind": self.default_kind,
                "target_subtypes": list(self.target_subtypes),
                "venue_aliases": sorted(self._venue_alias_index.items()),
                "source_name": self.source_name,
                "migration": dict(self.migration or {}),
            },
            # `_common.slugify` shapes the emitted person:/paper:/venue:/conf: node
            # ids, so fold `_common` into emit_version: a normalization change with
            # unchanged .bib bytes must bust the token.
            emit_targets=[sys.modules[__name__], _common],
        )

    def _resolved_paths(self) -> list[Path]:
        if self.project_manifest is not None:
            root = Path(self.source_root).expanduser().resolve()
            selected = {
                root / item.path
                for project in self.project_manifest.projects
                for item in project.files
                if "bibliography" in item.roles
            }
            if not selected:
                raise SubstrateError(
                    "BibReferencesSource: project manifest selects zero bibliography files",
                    fix_hint="Declare a local \\bibliography or \\addbibresource dependency.",
                )
            return sorted(selected)
        out: list[Path] = []
        for pattern in self.bib_paths:
            matches = _glob.glob(str(Path(pattern).expanduser()), recursive=True)
            out.extend(Path(p) for p in matches)
        if not out:
            raise SubstrateError(
                f"BibReferencesSource: bib_paths {list(self.bib_paths)!r} "
                f"resolved to zero files",
                fix_hint=(
                    "Verify the glob pattern matches existing .bib files. "
                    "Paths support `**` for recursive matching."
                ),
                data={"bib_paths": list(self.bib_paths)},
            )
        return sorted(out)

    def _iter_entries(self) -> Iterator[tuple[Path, dict[str, Any]]]:
        import bibtexparser  # imported lazily; bibtexparser>=1.4 in pyproject

        for path in self._resolved_paths():
            with path.open("r", encoding="utf-8") as fh:
                db = bibtexparser.load(fh)
            for entry in db.entries:
                yield path, entry

    def project_index(self) -> ProjectBibliographyIndex:
        if self.project_manifest is None or self.source_root is None:
            raise SubstrateError(
                "BibReferencesSource: project_index is only available in project mode"
            )
        return build_project_bibliography_index(
            self.project_manifest, self.source_root,
        )

    def _iter_project_entries(
        self,
    ) -> Iterator[tuple[Path, dict[str, Any], ProjectBibDeclaration]]:
        assert self.source_root is not None
        root = Path(self.source_root).expanduser().resolve()
        for declaration in self.project_index().declarations:
            yield (
                root / declaration.bib_path,
                dict(declaration.entry),
                declaration,
            )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        self._refresh_project_manifest()
        migration = dict(self.migration or {})
        migration_stage = cursor.get("latex_project_migration_stage") if isinstance(cursor, dict) else None
        if (
            self.project_manifest is not None
            and migration
            and cursor
            and not cursor_has_record_set_authority(cursor)
            and migration_stage is None
        ):
            legacy_paths = migration.get("legacy_bib_paths")
            if not isinstance(legacy_paths, (list, tuple)) or not legacy_paths:
                raise SubstrateError(
                    "BibReferencesSource legacy handoff requires migration.legacy_bib_paths",
                )
            legacy = BibReferencesSource(
                bib_paths=tuple(str(item) for item in legacy_paths),
                default_tier=self.default_tier,
                default_kind=self.default_kind,
                cache_root=self.cache_root,
                target_subtypes=self.target_subtypes,
            )
            legacy_run = legacy.run(run_id, mode="scope_complete")
            assert legacy_run.record_set is not None
            assert_legacy_record_set_matches_graph(
                dsn=self.dsn,
                source_name=self.source_name,
                record_set=legacy_run.record_set,
            )
            return SourceRun(
                facts=legacy_run.facts,
                completed_scope=True,
                next_cursor={
                    "latex_project_migration_stage": "legacy_authority",
                    "bib_references_project_schema_version": BIB_REFERENCES_PROJECT_SCHEMA_VERSION,
                },
                run_mode=mode,
                record_set=legacy_run.record_set,
            )

        if (
            self.project_manifest is not None
            and migration_stage == "project_authority"
            and isinstance(cursor, dict)
            and cursor.get("latex_project_manifest_digest") == self.project_manifest.digest
            and cursor.get("bib_references_project_schema_version")
            == BIB_REFERENCES_PROJECT_SCHEMA_VERSION
            and cursor_has_record_set_authority(cursor)
        ):
            return SourceRun(
                facts=(),
                completed_scope=(mode in {"scope_complete", "reconcile"}),
                next_cursor=preserve_cursor_record_set_authority(
                    {
                        "latex_project_manifest_digest": self.project_manifest.digest,
                        "latex_project_migration_stage": "project_authority",
                        "bib_references_project_schema_version": BIB_REFERENCES_PROJECT_SCHEMA_VERSION,
                    },
                    cursor,
                ),
                run_mode=mode,
            )

        stats: dict[str, Any] = {
            "files": 0,
            "entries": 0,
            "papers": 0,
            "persons": 0,
            "venues": 0,
        }

        if self.project_manifest is None:
            raw_entries = (
                (path, entry, None) for path, entry in self._iter_entries()
            )
        else:
            raw_entries = self._iter_project_entries()
        prepared_entries: list[
            tuple[
                Path,
                dict[str, Any],
                ProjectBibDeclaration | None,
                str,
                str,
                str | None,
                str | None,
            ]
        ] = []
        bib_keys_by_paper: dict[str, set[str]] = {}
        venue_aliases_by_canonical: dict[str, set[str]] = {}
        for path, entry, declaration in raw_entries:
            paper_id = (
                declaration.paper_id
                if declaration is not None
                else _paper_node_id(entry)
            )
            entry_type = _entry_type(entry)
            raw_venue = _venue_name(entry, entry_type)
            canonical_venue = (
                _canonical_venue_name(str(raw_venue), self._venue_alias_index)
                if raw_venue else None
            )
            cite_key = _normalized_bib_key(
                entry.get("ID") or entry.get("id") or entry.get("citekey")
            )
            if cite_key:
                bib_keys_by_paper.setdefault(paper_id, set()).add(cite_key)
            if raw_venue and canonical_venue:
                venue_aliases_by_canonical.setdefault(
                    canonical_venue, set()
                ).add(unicodedata.normalize("NFKC", str(raw_venue)).strip())
            prepared_entries.append((
                path,
                entry,
                declaration,
                paper_id,
                entry_type,
                str(raw_venue) if raw_venue else None,
                canonical_venue,
            ))

        def _facts() -> Iterator[Any]:
            seen_persons: set[str] = set()
            seen_venues: set[str] = set()
            seen_paths: set[str] = set()
            for (
                path,
                entry,
                declaration,
                paper_id,
                entry_type,
                raw_venue,
                venue_name,
            ) in prepared_entries:
                stats["entries"] += 1
                path_key = str(path)
                if path_key not in seen_paths:
                    seen_paths.add(path_key)
                    stats["files"] += 1
                annote = _parse_annote(entry.get("annote"))
                tier = int(annote.get("tier", self.default_tier))
                auto_extract = bool(annote.get("auto_extract", False))
                year = str(entry.get("year") or "").strip()
                title = (entry.get("title") or "").strip()
                # `paper_id` attr in two forms:
                #   - `paper_id`: the canonical node_id (preferred by
                #     downstream readers walking via attrs)
                #   - `cite_key_ref`: the `paper:<key>` form
                #     `latex_structural` emits as `cited_paper_ref`
                #     on citations, so the `citation_cites_paper`
                #     cross-link rule resolves. Per dogfood-canary
                #     data-quality audit (2026-05-19): without this,
                #     0/43 citations could resolve to live Papers.
                cite_key = entry.get("ID") or entry.get("id") or entry.get("citekey")
                record_base = {
                    "bib_path": (
                        declaration.bib_path
                        if declaration is not None else path_key
                    ),
                    "cite_key": entry.get("ID", ""),
                    **({"project_id": declaration.project_id}
                       if declaration is not None else {}),
                }
                paper_attrs: dict[str, Any] = {
                    "title": title,
                    "kind": self.default_kind,
                    "tier": tier,
                    "paper_id": paper_id,
                    "bib_entry_type": entry_type,
                }
                bib_keys = sorted(bib_keys_by_paper.get(paper_id, ()))
                if bib_keys:
                    paper_attrs["bib_keys"] = bib_keys
                    paper_attrs["bib_key_refs"] = [
                        f"paper:{key}" for key in bib_keys
                    ]
                if declaration is not None:
                    paper_attrs["bib_lookup_ref"] = {
                        "project_id": declaration.project_id,
                        "bib_path": declaration.bib_path,
                        "normalized_key": declaration.normalized_key,
                    }
                if cite_key:
                    if declaration is None:
                        paper_attrs["cite_key_ref"] = (
                            f"paper:{slugify(bib_keys[0])}"
                        )
                if auto_extract:
                    paper_attrs["auto_extract"] = True
                if year:
                    paper_attrs["year"] = year
                doi = (
                    _normalized_doi(entry.get("doi"))
                    if declaration is not None else entry.get("doi")
                )
                if doi:
                    paper_attrs["doi"] = doi
                arxiv_id = (
                    _normalized_arxiv_id(entry.get("eprint") or entry.get("arxiv"))
                    if declaration is not None
                    else entry.get("eprint") or entry.get("arxiv")
                )
                if arxiv_id:
                    paper_attrs["arxiv_id"] = arxiv_id
                if entry.get("abstract"):
                    paper_attrs["abstract"] = entry["abstract"]
                yield NodeFact(
                    node_id=paper_id,
                    subtype="paper",
                    attrs=paper_attrs,
                    source_record_id=record_base,
                    source_revision={"run_id": run_id},
                    op="I",
                )
                stats["papers"] += 1

                # Authors
                for idx, author_name in enumerate(_author_split(entry.get("author"))):
                    pid = _person_node_id(author_name)
                    if pid not in seen_persons:
                        seen_persons.add(pid)
                        yield NodeFact(
                            node_id=pid,
                            subtype="person",
                            attrs={"name": author_name},
                            source_record_id={**record_base, "author_index": idx},
                            source_revision={"run_id": run_id},
                            op="I",
                        )
                        stats["persons"] += 1
                    yield EdgeFact(
                        src=paper_id,
                        dst=pid,
                        edge_type="authored_by",
                        source_record_id={**record_base, "author_index": idx},
                        source_revision={"run_id": run_id},
                    )

                # Venue + ConferenceEdition
                if venue_name:
                    canonical_record = (
                        {"canonical_venue": venue_name}
                        if raw_venue != venue_name else {}
                    )
                    venue_record = (
                        {
                            **record_base,
                            "venue": raw_venue,
                            **canonical_record,
                        }
                        if declaration is not None
                        else {
                            "bib_path": path_key,
                            "venue": raw_venue,
                            **canonical_record,
                        }
                    )
                    vid = _venue_node_id(venue_name)
                    venue_aliases = sorted(
                        venue_aliases_by_canonical.get(venue_name, ())
                    )
                    if vid not in seen_venues:
                        seen_venues.add(vid)
                        yield NodeFact(
                            node_id=vid,
                            subtype="venue",
                            attrs={
                                "name": venue_name,
                                "venue_aliases": venue_aliases,
                            },
                            source_record_id=venue_record,
                            source_revision={"run_id": run_id},
                            op="I",
                        )
                        stats["venues"] += 1
                    if entry_type in _CONFERENCE_ENTRY_TYPES and year:
                        confed_id = _conference_edition_id(venue_name, year)
                        conf_id = _conference_id(venue_name)
                        conference_record = (
                            {
                                **record_base,
                                "venue": raw_venue,
                                **canonical_record,
                            }
                            if declaration is not None
                            else {
                                "venue": raw_venue,
                                **canonical_record,
                            }
                        )
                        edition_record = (
                            {
                                **record_base,
                                "venue": raw_venue,
                                **canonical_record,
                                "year": year,
                            }
                            if declaration is not None
                            else {
                                "venue": raw_venue,
                                **canonical_record,
                                "year": year,
                            }
                        )
                        yield NodeFact(
                            node_id=conf_id,
                            subtype="conference",
                            attrs={
                                "name": venue_name,
                                "venue_aliases": venue_aliases,
                            },
                            source_record_id=conference_record,
                            source_revision={"run_id": run_id},
                            op="I",
                        )
                        yield NodeFact(
                            node_id=confed_id,
                            subtype="conference_edition",
                            attrs={
                                "name": f"{venue_name} {year}",
                                "year": year,
                                "venue_aliases": venue_aliases,
                            },
                            source_record_id=edition_record,
                            source_revision={"run_id": run_id},
                            op="I",
                        )
                        yield EdgeFact(
                            src=confed_id,
                            dst=conf_id,
                            edge_type="member_of",
                            source_record_id=edition_record,
                            source_revision={"run_id": run_id},
                        )
                        yield EdgeFact(
                            src=paper_id,
                            dst=confed_id,
                            edge_type="published_in",
                            source_record_id=record_base,
                            source_revision={"run_id": run_id},
                        )
                    else:
                        yield EdgeFact(
                            src=paper_id,
                            dst=vid,
                            edge_type="published_in",
                            source_record_id=record_base,
                            source_revision={"run_id": run_id},
                        )

        emitted = list(_facts())
        self.last_run_stats = stats
        record_set = RecordSet(records={
            "bibliography-scope": RecordEmission(
                primary=None,
                descendants=[
                    RecordNode(
                        node_id=fact.node_id,
                        subtype=fact.subtype,
                        source_record_id=fact.source_record_id,
                        source_revision=fact.source_revision,
                    )
                    for fact in emitted
                    if isinstance(fact, NodeFact)
                ],
                edges=[
                    EdgeKey(
                        src=fact.src,
                        dst=fact.dst,
                        edge_type=fact.edge_type,
                        attrs=fact.attrs,
                        provenance=fact.provenance,
                        confidence=fact.confidence,
                        source_record_id=fact.source_record_id,
                        source_revision=fact.source_revision,
                    )
                    for fact in emitted
                    if isinstance(fact, EdgeFact)
                ],
            ),
        })
        next_cursor: dict[str, Any] = {
            "bib_references_project_schema_version": BIB_REFERENCES_PROJECT_SCHEMA_VERSION,
        }
        if self.project_manifest is not None:
            next_cursor["latex_project_manifest_digest"] = self.project_manifest.digest
        if migration_stage == "legacy_authority":
            next_cursor["latex_project_migration_stage"] = "project_authority"
        return SourceRun(
            facts=emitted,
            completed_scope=(mode in {"scope_complete", "reconcile"}),
            next_cursor=next_cursor,
            run_mode=mode,
            record_set=(
                record_set if mode in {"scope_complete", "reconcile"} else None
            ),
        )
