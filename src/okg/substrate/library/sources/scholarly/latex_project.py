"""Project-scoped scholarly facts for bounded local LaTeX manifests.

This adapter deliberately consumes the same immutable manifest as doc_corpus and
BibReferencesSource.  It never expands macros or executes TeX; its only filesystem
reads are manifest-selected UTF-8 ``.tex`` and ``.bib`` files under source_root.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterator, Mapping, Optional, Sequence

from okg.substrate.errors import SubstrateError
from okg.substrate.library.extraction.chunk.latex_structural import (
    LatexStructuralChunker,
)
from okg.substrate.library.extraction.latex_macros import extract_balanced_group
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
from okg.substrate.library.sources.doc_corpus import chunk_node_id

from .bib_references import (
    assert_legacy_record_set_matches_graph,
    build_project_bibliography_index,
)
from .latex_project_manifest import (
    LatexProjectManifestBuilder,
    _is_escaped,
    _mask_inert,
)

__all__ = ["CitationOccurrence", "LatexProjectSource", "scan_citations"]


_CITE_COMMANDS = (
    "cite", "citep", "citet", "citealp", "citealt", "citeauthor",
    "citeyear", "citeyearpar", "parencite", "textcite", "autocite",
    "smartcite", "footcite", "supercite",
)
_CITE_RE = re.compile(
    r"\\(?P<name>" + "|".join(sorted(_CITE_COMMANDS, key=len, reverse=True))
    + r")(?P<star>\*)?(?![A-Za-z@])"
)
_NOCITE_RE = re.compile(r"\\(?P<name>nocite)(?P<star>\*)?(?![A-Za-z@])")
_HEADING_RE = re.compile(
    r"\\(?P<level>part|section|subsection|subsubsection)\*?(?![A-Za-z@])"
)
_HEADING_RANK = {"part": 0, "section": 1, "subsection": 2, "subsubsection": 3}
# Increment whenever unchanged manifest bytes must be re-emitted because the
# graph footprint changed. A cursor carrying an older value defeats the
# "manifest digest unchanged, emit nothing" short-circuit exactly once.
# Version 2 adopted the shared `chunk_node_id` identity, which moved table
# chunks onto source-occurrence ids so the chunk edges emitted here land on the
# `document_chunk` nodes DocCorpusSource actually creates.
LATEX_PROJECT_EMISSION_SCHEMA_VERSION = 2


class LatexProjectManifestMismatch(SubstrateError):
    """The three project consumers did not observe one immutable manifest."""

    code = "latex_project_manifest_mismatch"


@dataclass(frozen=True)
class CitationOccurrence:
    command: str
    command_start: int
    command_end: int
    key_start: int
    key_end: int
    ordinal: int
    raw_key: str
    normalized_key: str
    context_text: str


@dataclass(frozen=True)
class _Heading:
    start: int
    end: int
    level: str
    title: str


def _skip_space(text: str, position: int) -> int:
    while position < len(text) and text[position].isspace():
        position += 1
    return position


def _balanced_optional(text: str, position: int) -> int | None:
    position = _skip_space(text, position)
    if position >= len(text) or text[position] != "[":
        return None
    depth = 1
    index = position + 1
    while index < len(text):
        char = text[index]
        if char == "[" and not _is_escaped(text, index):
            depth += 1
        elif char == "]" and not _is_escaped(text, index):
            depth -= 1
            if depth == 0:
                return index + 1
        index += 1
    return None


def _context(text: str, start: int, end: int) -> str:
    left = max(
        text.rfind("\n", 0, start),
        text.rfind(".", 0, start),
        text.rfind("!", 0, start),
        text.rfind("?", 0, start),
    )
    right_candidates = [
        value for value in (
            text.find("\n", end), text.find(".", end),
            text.find("!", end), text.find("?", end),
        ) if value >= 0
    ]
    right = min(right_candidates) + 1 if right_candidates else min(len(text), end + 160)
    left = max(left + 1, start - 160)
    return " ".join(text[left:right].strip().split())


def _scan_command(
    text: str, scan: str, match: re.Match[str], *, textual: bool,
) -> tuple[list[CitationOccurrence], tuple[str, ...]]:
    position = match.end()
    for _ in range(2):
        optional_end = _balanced_optional(scan, position)
        if optional_end is None:
            break
        position = optional_end
    position = _skip_space(scan, position)
    if position >= len(scan) or scan[position] != "{":
        return [], ()
    balanced = extract_balanced_group(scan, position)
    if balanced is None:
        return [], ()
    _, command_end = balanced
    body_start = position + 1
    body_end = command_end - 1
    command = match.group("name") + ("*" if match.group("star") else "")
    occurrences: list[CitationOccurrence] = []
    selection: list[str] = []
    ordinal = 0
    item_start = body_start
    for comma in [*re.finditer(",", scan[body_start:body_end]), None]:
        item_end = body_end if comma is None else body_start + comma.start()
        semantic_raw = scan[item_start:item_end]
        left_trim = len(semantic_raw) - len(semantic_raw.lstrip())
        right_trimmed = semantic_raw.rstrip()
        key_start = item_start + left_trim
        key_end = item_start + len(right_trimmed)
        raw_key = text[key_start:key_end]
        semantic_key = unicodedata.normalize(
            "NFC", scan[key_start:key_end],
        )
        normalized = re.sub(r"\s+", "", semantic_key)
        if normalized:
            selection.append(normalized)
            if textual:
                occurrences.append(CitationOccurrence(
                    command=command,
                    command_start=match.start(),
                    command_end=command_end,
                    key_start=key_start,
                    key_end=key_end,
                    ordinal=ordinal,
                    raw_key=raw_key,
                    normalized_key=normalized,
                    context_text=_context(text, match.start(), command_end),
                ))
                ordinal += 1
        item_start = item_end + 1
    return occurrences, tuple(selection)


def scan_citations(text: str) -> tuple[tuple[CitationOccurrence, ...], tuple[str, ...]]:
    """Return textual cite occurrences and non-evidentiary nocite selections."""
    scan = _mask_inert(text)
    occurrences: list[CitationOccurrence] = []
    selections: list[str] = []
    for match in _CITE_RE.finditer(scan):
        if _is_escaped(scan, match.start()):
            continue
        found, _ = _scan_command(text, scan, match, textual=True)
        occurrences.extend(found)
    for match in _NOCITE_RE.finditer(scan):
        if _is_escaped(scan, match.start()):
            continue
        _, found = _scan_command(text, scan, match, textual=False)
        selections.extend(found)
    occurrences.sort(key=lambda item: (item.command_start, item.key_start, item.ordinal))
    return tuple(occurrences), tuple(selections)


def _headings(text: str) -> tuple[_Heading, ...]:
    scan = _mask_inert(text)
    raw: list[tuple[int, int, str, str]] = []
    for match in _HEADING_RE.finditer(scan):
        if _is_escaped(scan, match.start()):
            continue
        position = match.end()
        optional_end = _balanced_optional(scan, position)
        if optional_end is not None:
            position = optional_end
        position = _skip_space(scan, position)
        if position >= len(scan) or scan[position] != "{":
            continue
        balanced = extract_balanced_group(scan, position)
        if balanced is None:
            continue
        _, end = balanced
        title = text[position + 1:end - 1].strip()
        raw.append((match.start(), end, match.group("level"), title))
    output: list[_Heading] = []
    for start, end, level, title in raw:
        output.append(_Heading(start, end, level, title))
    return tuple(output)


def _section_id(project_id: str, source_path: str, marker: str) -> str:
    seed = "\0".join((project_id, source_path, marker))
    return f"section:{hashlib.sha256(seed.encode('utf-8')).hexdigest()[:32]}"


def _citation_id(project_id: str, source_path: str, item: CitationOccurrence) -> str:
    seed = "\0".join((
        project_id, source_path, str(item.key_start), str(item.key_end),
        str(item.ordinal), item.normalized_key,
    ))
    return f"citation:{hashlib.sha256(seed.encode('utf-8')).hexdigest()[:32]}"


def _has_visible_manuscript_text(text: str) -> bool:
    """Reject command-only chunks when deciding whether front matter exists."""
    semantic = re.sub(r"(?m)(?<!\\)%[^\n]*", " ", text)
    command = re.compile(
        r"\\+[A-Za-z@]+\*?(?:\s*\[[^\]]*\])?(?:\s*\{[^{}]*\})?",
    )
    semantic = command.sub(" ", semantic)
    semantic = semantic.replace("{", " ").replace("}", " ")
    return bool(re.search(r"[\w\d]", semantic, re.UNICODE))


@dataclass
class LatexProjectSource:
    source_root: str | Path
    project_manifest: Any
    default_kind: str = "our-draft"
    predecessor_sources: Sequence[str] = field(default_factory=tuple)
    dsn: str | None = None
    migration: Mapping[str, Any] | None = None
    source_name: str | None = None
    status: str = "enabled"
    last_run_stats: dict[str, Any] = field(default_factory=dict, init=False)

    name: str = "latex_project"
    profile: str = "discovery_crawl"
    change_probe_kind = "content_hash"

    # The emitted citation targets depend on the separately owned bibliography
    # source, and the manifest handoff depends on two predecessor watermarks.
    # Neither input is visible to a probe over this source's own files, so the
    # runner must still enter run() and execute the fail-closed handoff check.
    probe_short_circuit_safe = False

    def __post_init__(self) -> None:
        self.source_root = Path(self.source_root).expanduser().resolve()
        self._manifest_config: dict[str, Any] | None = None
        if isinstance(self.project_manifest, Mapping):
            self._manifest_config = dict(self.project_manifest)
            self.project_manifest = self._build_manifest()
        if not self.project_manifest.projects:
            raise SubstrateError("LatexProjectSource: manifest selects zero projects")
        self._predecessors = tuple(str(item) for item in self.predecessor_sources)
        if self._predecessors and len(self._predecessors) != 2:
            raise SubstrateError(
                "LatexProjectSource requires exactly two predecessor_sources "
                "(manifest-aware doc_corpus and bib_references)",
            )
        if self._predecessors and not self.dsn:
            raise SubstrateError(
                "LatexProjectSource requires dsn when predecessor_sources are configured",
            )
        migration = dict(self.migration or {})
        if migration:
            legacy_owner = migration.get("legacy_ownership_id")
            if not isinstance(legacy_owner, str) or not legacy_owner.strip():
                raise SubstrateError(
                    "LatexProjectSource migration requires legacy_ownership_id",
                    fix_hint=(
                        "Set migration.legacy_ownership_id to the exact existing "
                        "OurDrafts ownership id, or remove migration for fresh ownership."
                    ),
                )
            if self.source_name != legacy_owner:
                raise SubstrateError(
                    "LatexProjectSource migration ownership does not match source_name",
                    data={
                        "legacy_ownership_id": legacy_owner,
                        "source_name": self.source_name,
                    },
                    fix_hint=(
                        "Run the handoff under the existing OurDrafts ownership id; "
                        "a new owner cannot retract another source's records."
                    ),
                )
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )

        def _items() -> list[tuple[str, Path]]:
            self._refresh_manifest()
            return [
                (item.path, self.source_root / item.path)
                for project in self.project_manifest.projects
                for item in project.files
            ]

        return ContentHashProbe(
            content_items=_items,
            config={
                "source_root": str(self.source_root),
                "project_manifest_digest": self.project_manifest.digest,
                "default_kind": self.default_kind,
                "predecessor_sources": list(self._predecessors),
                "migration": dict(self.migration or {}),
                "source_name": self.source_name,
                "emission_schema_version": LATEX_PROJECT_EMISSION_SCHEMA_VERSION,
            },
            emit_targets=[sys.modules[__name__]],
        )

    def _build_manifest(self) -> Any:
        assert self._manifest_config is not None
        return LatexProjectManifestBuilder(
            source_root=self.source_root,
            **self._manifest_config,
        ).build()

    def _refresh_manifest(self) -> None:
        if self._manifest_config is not None:
            self.project_manifest = self._build_manifest()

    def _assert_manifest_handoff(self) -> None:
        if not self._predecessors:
            return
        assert self.dsn is not None
        from okg.substrate.db.pool import GENERAL, pooled_connection

        with pooled_connection(self.dsn, purpose=GENERAL) as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT source, last_applied_rev->>'latex_project_manifest_digest' "
                "FROM okg.source_watermarks WHERE source = ANY(%s)",
                (list(self._predecessors),),
            )
            observed = {str(source): digest for source, digest in cur.fetchall()}
        expected = self.project_manifest.digest
        mismatches = {
            source: observed.get(source)
            for source in self._predecessors
            if observed.get(source) != expected
        }
        if mismatches:
            raise LatexProjectManifestMismatch(
                "LatexProjectSource predecessor manifest digests are missing or stale",
                data={
                    "expected_manifest_digest": expected,
                    "predecessor_digests": observed,
                    "mismatches": mismatches,
                },
                fix_hint=(
                    "Run manifest-aware doc_corpus and bib_references successfully "
                    "before latex_project."
                ),
            )

    def _doc_id(self, source_path: str) -> str:
        return f"doc:{self.project_manifest.repository_authority}:{source_path}"

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[dict[str, Any]] = None,
    ) -> SourceRun:
        self._refresh_manifest()
        self._assert_manifest_handoff()

        migration = dict(self.migration or {})
        migration_stage = cursor.get("latex_project_migration_stage") if isinstance(cursor, dict) else None
        if migration and cursor and not cursor_has_record_set_authority(cursor) and migration_stage is None:
            legacy_paths = migration.get("legacy_draft_paths")
            if not isinstance(legacy_paths, (list, tuple)) or not legacy_paths:
                raise SubstrateError(
                    "LatexProjectSource legacy handoff requires migration.legacy_draft_paths",
                )
            from .our_drafts_source import OurDraftsSource

            legacy = OurDraftsSource(draft_paths=[str(item) for item in legacy_paths])
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
                    "latex_project_emission_schema_version": LATEX_PROJECT_EMISSION_SCHEMA_VERSION,
                },
                run_mode=mode,
                record_set=legacy_run.record_set,
            )

        if (
            migration_stage == "project_authority"
            and isinstance(cursor, dict)
            and cursor.get("latex_project_manifest_digest") == self.project_manifest.digest
            and cursor.get("latex_project_emission_schema_version")
            == LATEX_PROJECT_EMISSION_SCHEMA_VERSION
            and cursor_has_record_set_authority(cursor)
        ):
            return SourceRun(
                facts=(),
                completed_scope=(mode in {"scope_complete", "reconcile"}),
                next_cursor=preserve_cursor_record_set_authority(
                    {
                        "latex_project_manifest_digest": self.project_manifest.digest,
                        "latex_project_migration_stage": "project_authority",
                        "latex_project_emission_schema_version": LATEX_PROJECT_EMISSION_SCHEMA_VERSION,
                    },
                    cursor,
                ),
                run_mode=mode,
            )

        index = build_project_bibliography_index(
            self.project_manifest, self.source_root,
        )
        stats: dict[str, Any] = {
            "projects": 0, "sections": 0, "citations": 0,
            "resolved": 0, "unresolved": 0, "ambiguous": 0,
            "warnings": [],
        }

        def _facts() -> Iterator[NodeFact | EdgeFact]:
            revision = {"run_id": run_id, "manifest_digest": self.project_manifest.digest}
            for project in self.project_manifest.projects:
                stats["projects"] += 1
                project_record = {
                    "project_id": project.paper_id,
                    "root_path": project.root_path,
                }
                all_nocite: list[dict[str, Any]] = []
                document_payloads: dict[
                    str,
                    tuple[str, tuple[_Heading, ...], tuple[CitationOccurrence, ...], tuple[Any, ...]],
                ] = {}
                for manifest_file in project.files:
                    if Path(manifest_file.path).suffix.lower() != ".tex":
                        continue
                    text = (self.source_root / manifest_file.path).read_text("utf-8")
                    headings = _headings(text)
                    occurrences, selections = scan_citations(text)
                    all_nocite.extend(
                        {"source_path": manifest_file.path, "normalized_key": key}
                        for key in selections
                    )
                    document_payloads[manifest_file.path] = (
                        text,
                        headings,
                        occurrences,
                        tuple(LatexStructuralChunker().chunk(text)),
                    )

                yield NodeFact(
                    node_id=project.paper_id,
                    subtype="paper",
                    attrs={
                        "paper_id": project.paper_id,
                        "title": Path(project.root_path).stem,
                        "kind": self.default_kind,
                        "project_id": project.paper_id,
                        "project_dir": project.project_dir,
                        "root_path": project.root_path,
                        "manifest_digest": self.project_manifest.digest,
                        "bibliography_selection": all_nocite,
                    },
                    source_record_id=project_record,
                    source_revision=revision,
                )

                root_doc_id = self._doc_id(project.root_path)
                yield EdgeFact(
                    src=project.paper_id,
                    dst=root_doc_id,
                    edge_type="derived_from",
                    source_record_id={**project_record, "edge": "root_document"},
                    source_revision=revision,
                )

                files_by_path = {item.path: item for item in project.files}
                references: dict[str, dict[str, Any]] = {}
                for dependency in project.dependencies:
                    if dependency.status != "resolved" or not dependency.resolved_path:
                        continue
                    selected = files_by_path.get(dependency.resolved_path)
                    if selected is None:
                        continue
                    if dependency.role == "figure":
                        endpoint = f"visual:{selected.content_sha256[:16]}"
                    else:
                        endpoint = self._doc_id(dependency.resolved_path)
                    aggregate = references.setdefault(endpoint, {
                        "latex_roles": set(),
                        "latex_commands": set(),
                        "latex_occurrences": [],
                    })
                    aggregate["latex_roles"].add(dependency.role)
                    aggregate["latex_commands"].add(dependency.command)
                    aggregate["latex_occurrences"].append(dependency.as_dict())
                for endpoint, aggregate in sorted(references.items()):
                    occurrences = sorted(
                        aggregate["latex_occurrences"],
                        key=lambda item: (
                            item["source_path"], item["start"], item["end"],
                            item["role"], item["resolved_path"] or "",
                        ),
                    )
                    yield EdgeFact(
                        src=project.paper_id,
                        dst=endpoint,
                        edge_type="references",
                        attrs={
                            "latex_roles": sorted(aggregate["latex_roles"]),
                            "latex_commands": sorted(aggregate["latex_commands"]),
                            "latex_occurrences": occurrences,
                        },
                        source_record_id={
                            **project_record,
                            "edge": "member_reference",
                            "endpoint": endpoint,
                        },
                        source_revision=revision,
                    )

                dependencies: dict[str, list[Any]] = {}
                for dependency in project.dependencies:
                    if (
                        dependency.role == "input"
                        and dependency.status == "resolved"
                        and dependency.resolved_path in document_payloads
                    ):
                        dependencies.setdefault(dependency.source_path, []).append(dependency)
                for values in dependencies.values():
                    values.sort(key=lambda item: (item.start, item.end, item.resolved_path))

                visited: set[str] = set()

                def _events(source_path: str) -> Iterator[tuple[str, str, Any]]:
                    if source_path in visited or source_path not in document_payloads:
                        return
                    visited.add(source_path)
                    _, headings, occurrences, _ = document_payloads[source_path]
                    local: list[tuple[int, int, str, Any]] = [
                        (heading.start, 0, "heading", heading) for heading in headings
                    ]
                    local.extend(
                        (occurrence.command_start, 1, "citation", occurrence)
                        for occurrence in occurrences
                    )
                    local.extend(
                        (chunk.span[0], 1, "chunk", chunk)
                        for chunk in document_payloads[source_path][3]
                        if chunk.span is not None
                        and _has_visible_manuscript_text(chunk.text)
                    )
                    local.extend(
                        (dependency.start, 3, "input", dependency)
                        for dependency in dependencies.get(source_path, ())
                    )
                    for _, _, kind, value in sorted(local, key=lambda item: (item[0], item[1])):
                        if kind == "input":
                            assert value.resolved_path is not None
                            yield from _events(value.resolved_path)
                        else:
                            yield source_path, kind, value

                ordered_events = list(_events(project.root_path))
                for source_path in document_payloads:
                    if source_path not in visited:
                        ordered_events.extend(_events(source_path))

                section_stack: list[tuple[int, str, str]] = []
                path_counts: dict[str | None, int] = {}
                ordinal_counts: dict[str | None, int] = {}
                front_id = _section_id(
                    project.paper_id, project.root_path, "front-matter",
                )
                front_emitted = False

                for source_path, kind, value in ordered_events:
                    if kind == "heading":
                        heading = value
                        rank = _HEADING_RANK[heading.level]
                        while section_stack and section_stack[-1][0] >= rank:
                            section_stack.pop()
                        parent_id = section_stack[-1][1] if section_stack else None
                        parent_path = section_stack[-1][2] if section_stack else "§"
                        path_counts[parent_id] = path_counts.get(parent_id, 0) + 1
                        path = (
                            f"§{path_counts[parent_id]}" if parent_id is None
                            else f"{parent_path}.{path_counts[parent_id]}"
                        )
                        ordinal = ordinal_counts.get(parent_id, 0)
                        ordinal_counts[parent_id] = ordinal + 1
                        section_id = _section_id(
                            project.paper_id, source_path, str(heading.start),
                        )
                        attrs: dict[str, Any] = {
                            "paper_section_id": section_id,
                            "paper_ref": project.paper_id,
                            "path": path,
                            "ordinal": ordinal,
                            "title": heading.title,
                            "source_path": source_path,
                            "source_start": heading.start,
                            "source_end": heading.end,
                        }
                        if parent_id is not None:
                            attrs["parent_section_ref"] = parent_id
                        section_record = {
                            **project_record,
                            "source_path": source_path,
                            "section_start": heading.start,
                        }
                        yield NodeFact(
                            node_id=section_id, subtype="paper_section", attrs=attrs,
                            source_record_id=section_record, source_revision=revision,
                        )
                        yield EdgeFact(
                            src=parent_id or project.paper_id,
                            dst=section_id, edge_type="contains",
                            source_record_id=section_record, source_revision=revision,
                        )
                        section_stack.append((rank, section_id, path))
                        stats["sections"] += 1
                        continue

                    if kind == "chunk":
                        chunk = value
                        if section_stack:
                            section_id = section_stack[-1][1]
                        else:
                            section_id = front_id
                            if not front_emitted:
                                ordinal = ordinal_counts.get(None, 0)
                                ordinal_counts[None] = ordinal + 1
                                front_record = {
                                    **project_record,
                                    "source_path": project.root_path,
                                    "section": "front-matter",
                                }
                                yield NodeFact(
                                    node_id=front_id,
                                    subtype="paper_section",
                                    attrs={
                                        "paper_section_id": front_id,
                                        "paper_ref": project.paper_id,
                                        "path": "§0",
                                        "ordinal": ordinal,
                                        "title": "Front matter",
                                        "section_kind": "front_matter",
                                        "source_path": project.root_path,
                                    },
                                    source_record_id=front_record,
                                    source_revision=revision,
                                )
                                yield EdgeFact(
                                    src=project.paper_id,
                                    dst=front_id,
                                    edge_type="contains",
                                    source_record_id=front_record,
                                    source_revision=revision,
                                )
                                front_emitted = True
                                stats["sections"] += 1
                        assert chunk.span is not None
                        chunk_document_ref = self._doc_id(source_path)
                        chunk_id = chunk_node_id(
                            chunk.text,
                            chunk.chunk_kind,
                            doc_id=chunk_document_ref,
                            extras=getattr(chunk, "extras", None),
                        )
                        chunk_record = {
                            **project_record,
                            "source_path": source_path,
                            "chunk_start": chunk.span[0],
                            "chunk_end": chunk.span[1],
                        }
                        yield EdgeFact(
                            src=section_id,
                            dst=chunk_id,
                            edge_type="contains",
                            attrs={
                                "document_ref": chunk_document_ref,
                                "char_offset": chunk.span[0],
                                "char_end": chunk.span[1],
                                "chunk_kind": chunk.chunk_kind,
                            },
                            source_record_id=chunk_record,
                            source_revision=revision,
                        )
                        continue

                    occurrence = value
                    if section_stack:
                        section_id = section_stack[-1][1]
                    else:
                        section_id = front_id
                        if not front_emitted:
                            ordinal = ordinal_counts.get(None, 0)
                            ordinal_counts[None] = ordinal + 1
                            front_record = {
                                **project_record,
                                "source_path": project.root_path,
                                "section": "front-matter",
                            }
                            yield NodeFact(
                                node_id=front_id, subtype="paper_section",
                                attrs={
                                    "paper_section_id": front_id,
                                    "paper_ref": project.paper_id,
                                    "path": "§0",
                                    "ordinal": ordinal,
                                    "title": "Front matter",
                                    "section_kind": "front_matter",
                                    "source_path": project.root_path,
                                },
                                source_record_id=front_record,
                                source_revision=revision,
                            )
                            yield EdgeFact(
                                src=project.paper_id, dst=front_id,
                                edge_type="contains",
                                source_record_id=front_record,
                                source_revision=revision,
                            )
                            front_emitted = True
                            stats["sections"] += 1

                    _, _, _, chunks = document_payloads[source_path]
                    doc_id = self._doc_id(source_path)
                    citation_id = _citation_id(
                        project.paper_id, source_path, occurrence,
                    )
                    resolution, cited_paper, bib_paths = index.resolve(
                        project.paper_id, occurrence.normalized_key,
                    )
                    intersecting = [
                        chunk for chunk in chunks
                        if chunk.span is not None
                        and occurrence.key_start < chunk.span[1]
                        and chunk.span[0] < occurrence.key_end
                    ]
                    attrs = {
                        "citation_id": citation_id,
                        "citing_section_ref": section_id,
                        "context_text": occurrence.context_text,
                        "command": occurrence.command,
                        "command_start": occurrence.command_start,
                        "command_end": occurrence.command_end,
                        "key_start": occurrence.key_start,
                        "key_end": occurrence.key_end,
                        "key_ordinal": occurrence.ordinal,
                        "raw_key": occurrence.raw_key,
                        "normalized_key": occurrence.normalized_key,
                        "project_id": project.paper_id,
                        "project_dir": project.project_dir,
                        "root_path": project.root_path,
                        "source_path": source_path,
                        "owning_document_ref": doc_id,
                        "owning_section_ref": section_id,
                        "citation_lookup_ref": {
                            "project_id": project.paper_id,
                            "bibliography_paths": list(bib_paths),
                            "normalized_key": occurrence.normalized_key,
                        },
                        "resolution_status": resolution,
                    }
                    chunk_id: str | None = None
                    if len(intersecting) == 1:
                        chunk = intersecting[0]
                        assert chunk.span is not None
                        chunk_id = chunk_node_id(
                            chunk.text,
                            chunk.chunk_kind,
                            doc_id=doc_id,
                            extras=getattr(chunk, "extras", None),
                        )
                        attrs.update({
                            "canonical_chunk_ref": chunk_id,
                            "chunk_membership_start": chunk.span[0],
                            "chunk_membership_end": chunk.span[1],
                        })
                    if cited_paper is not None:
                        attrs["cited_paper_ref"] = cited_paper
                    elif resolution == "ambiguous":
                        attrs["resolution_warning"] = "latex_citation_ambiguous"
                        stats["warnings"].append({
                            "code": "latex_citation_ambiguous",
                            "project_id": project.paper_id,
                            "source_path": source_path,
                            "key_start": occurrence.key_start,
                            "normalized_key": occurrence.normalized_key,
                        })
                    else:
                        attrs["resolution_warning"] = "latex_citation_unresolved"
                        stats["warnings"].append({
                            "code": "latex_citation_unresolved",
                            "project_id": project.paper_id,
                            "source_path": source_path,
                            "key_start": occurrence.key_start,
                            "normalized_key": occurrence.normalized_key,
                        })
                    record = {
                        **project_record,
                        "source_path": source_path,
                        "key_start": occurrence.key_start,
                        "key_end": occurrence.key_end,
                        "key_ordinal": occurrence.ordinal,
                    }
                    yield NodeFact(
                        node_id=citation_id, subtype="citation", attrs=attrs,
                        source_record_id=record, source_revision=revision,
                    )
                    yield EdgeFact(
                        src=doc_id, dst=citation_id, edge_type="contains",
                        source_record_id=record, source_revision=revision,
                    )
                    yield EdgeFact(
                        src=section_id, dst=citation_id, edge_type="contains",
                        source_record_id=record, source_revision=revision,
                    )
                    if chunk_id is not None:
                        yield EdgeFact(
                            src=citation_id, dst=chunk_id, edge_type="references",
                            attrs={
                                "document_ref": doc_id,
                                "char_offset": attrs["chunk_membership_start"],
                                "char_end": attrs["chunk_membership_end"],
                            },
                            source_record_id=record, source_revision=revision,
                        )
                    if cited_paper is not None:
                        yield EdgeFact(
                            src=citation_id, dst=cited_paper, edge_type="cites",
                            source_record_id=record, source_revision=revision,
                        )
                    stats["citations"] += 1
                    stats[resolution] += 1

        emitted = list(_facts())
        self.last_run_stats = stats
        records: dict[str, RecordEmission] = {}
        for project in self.project_manifest.projects:
            project_nodes = [
                fact for fact in emitted
                if isinstance(fact, NodeFact)
                and fact.source_record_id.get("project_id") == project.paper_id
            ]
            project_edges = [
                fact for fact in emitted
                if isinstance(fact, EdgeFact)
                and fact.source_record_id.get("project_id") == project.paper_id
            ]
            primary = next(
                (fact for fact in project_nodes if fact.node_id == project.paper_id),
                None,
            )
            records[project.paper_id] = RecordEmission(
                primary=(
                    RecordNode(
                        node_id=primary.node_id,
                        subtype=primary.subtype,
                        source_record_id=primary.source_record_id,
                        source_revision=primary.source_revision,
                    )
                    if primary is not None else None
                ),
                descendants=[
                    RecordNode(
                        node_id=fact.node_id,
                        subtype=fact.subtype,
                        source_record_id=fact.source_record_id,
                        source_revision=fact.source_revision,
                    )
                    for fact in project_nodes
                    if primary is None or fact.node_id != primary.node_id
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
                    for fact in project_edges
                ],
            )
        next_cursor = {
            "latex_project_manifest_digest": self.project_manifest.digest,
            "latex_project_emission_schema_version": LATEX_PROJECT_EMISSION_SCHEMA_VERSION,
        }
        if migration_stage == "legacy_authority":
            next_cursor["latex_project_migration_stage"] = "project_authority"
        return SourceRun(
            facts=emitted,
            completed_scope=(mode in {"scope_complete", "reconcile"}),
            next_cursor=next_cursor,
            run_mode=mode,
            record_set=(
                RecordSet(records=records)
                if mode in {"scope_complete", "reconcile"} else None
            ),
        )
