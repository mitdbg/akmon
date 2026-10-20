"""OpenSpecSource — emit graph nodes from an openspec/ directory.

Walks `<openspec_root>/changes/`, `<openspec_root>/changes/archive/`,
and `<openspec_root>/specs/`. Parses the four file kinds in each change
directory plus canonical capability specs, and emits one node per
artifact (proposal, capability, delta, criterion, scenario, task,
decision, etc.) plus the structural CONTAINS / TARGETS / SUPERSEDES
edges.

Cross-module: emits ``OpenSpecChange REFERENCES file:<repo>:<path>``
for every ``@/path`` and fenced-code path mention found in a proposal's
markdown. Resolves to a real ``SourceFile`` node only when the
``git_graph`` module is also loaded.

Incremental crawl: per-file SHA-256 watermark in
``state["file_hashes"]: dict[change_id, dict[file_path, sha256]]``.
Files whose hash hasn't changed are skipped. Files removed since the
last run produce stale node IDs.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from okg.substrate.library.sources.source_specs import EdgeSpec, NodeSpec
from okg.substrate.library.sources._openspec_parse import (
    CURRENT_FRONTMATTER_SCHEMA_VERSION,
    UnknownFrontmatterSchemaVersionError,
    extract_path_mentions,
    file_sha256,
    parse_canonical_spec_md,
    parse_change_frontmatter,
    parse_change_spec_md,
    parse_design_md,
    parse_proposal_md,
    parse_reasoning_proposal_md,
    parse_reasoning_single_file_md,
    parse_tasks_md,
)

# Valid status enum values per reasoning module schema.
_HYPOTHESIS_STATUSES = frozenset(
    {
        "open",
        "open_problem",
        "supported",
        "refuted",
        "superseded",
        "retired",
    }
)
_OBSERVATION_STATUSES = frozenset({"recorded", "annotated", "retracted"})
_INSIGHT_STATUSES = frozenset({"draft", "confirmed", "superseded"})
_OBSERVATION_SOURCE_KINDS = frozenset(
    {
        "paper",
        "experiment",
        "agent_session",
        "curated",
        "derives_from_candidate",
    }
)

logger = logging.getLogger(__name__)


# YYYY-MM-DD-<slug> for archived dirs
_ARCHIVE_DATE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})-(.+)$")


# Per dogfood-canary DQ audit (2026-05-19): bullet-keyword regex
# for scenario Gherkin parsing. The scenario `text` is formatted
# as:
#   - **GIVEN** ...
#   - **WHEN** ...
#   - **THEN** ...
#   - **AND** ...
# Optional leading whitespace; bullet body can span multiple
# indented continuation lines.
_GHERKIN_BULLET_RE = re.compile(
    r"^\s*-\s*\*\*(GIVEN|WHEN|THEN|AND|BUT)\*\*\s*(.*?)$",
    re.IGNORECASE,
)


def _parse_scenario_gherkin(text: str) -> dict[str, str]:
    """Parse Gherkin-style bullets out of a scenario's text.

    Returns a dict with keys `given`, `when`, `then`, `and_`,
    `but`. Multiple bullets sharing a keyword are joined with
    "; ". Empty when no recognized keywords appear (parser
    falls closed silently — the original `text` attr still
    holds the prose).

    `and_` / `but` are typically continuations of THEN; we
    keep them separate so the spec author's structure is
    preserved.
    """
    out: dict[str, list[str]] = {
        "given": [],
        "when": [],
        "then": [],
        "and_": [],
        "but": [],
    }
    current_key: str | None = None
    current_chunks: list[str] = []

    def flush() -> None:
        nonlocal current_key, current_chunks
        if current_key and current_chunks:
            joined = " ".join(c.strip() for c in current_chunks if c.strip())
            if joined:
                out[current_key].append(joined)
        current_chunks = []

    for raw_line in text.splitlines():
        m = _GHERKIN_BULLET_RE.match(raw_line)
        if m:
            flush()
            kw = m.group(1).lower()
            current_key = "and_" if kw == "and" else kw
            current_chunks = [m.group(2)]
        elif current_key is not None:
            # Continuation line (indented).
            stripped = raw_line.strip()
            if stripped:
                current_chunks.append(stripped)
    flush()

    return {k: "; ".join(v) for k, v in out.items() if v}


@dataclass
class OpenSpecSource:
    """Ingest source for OpenSpec proposal artifacts.

    Parameters
    ----------
    openspec_root : Path
        Path to the project's ``openspec/`` directory.
    repo : str
        Repo short name used for cross-module SourceFile IDs.
        E.g. ``"okg"`` so file mentions resolve to ``file:okg:<path>``.
    include_archived : bool
        Whether to ingest changes under ``changes/archive/``. Default True.
    """

    openspec_root: Path
    repo: str
    include_archived: bool = True
    state: dict[str, Any] = field(default_factory=dict)
    scope_records: set[str] | None = None
    _emit_nodes: list[NodeSpec] | None = None
    _emit_edges: list[EdgeSpec] | None = None
    _emit_aliases: dict[str, str] = field(default_factory=dict)
    _stale: list[str] = field(default_factory=list)
    _degraded_files: list[str] = field(default_factory=list)

    ownership_id: str = "openspec"
    supports_incremental: bool = True
    run_state_kind: str = (
        "metadata"  # bind_run_state passes us state.metadata as a dict
    )

    def __post_init__(self) -> None:
        if isinstance(self.openspec_root, str):
            self.openspec_root = Path(self.openspec_root).expanduser()

    # --- Source protocol ----------------------------------------------------

    def nodes(self) -> Iterable[NodeSpec]:
        if self._emit_nodes is None:
            self._compute_emit()
        yield from self._emit_nodes or []

    def edges(self) -> Iterable[EdgeSpec]:
        if self._emit_edges is None:
            self._compute_emit()
        yield from self._emit_edges or []

    def aliases(self) -> dict[str, str]:
        if self._emit_nodes is None:
            self._compute_emit()
        return dict(self._emit_aliases)

    # --- State hooks --------------------------------------------------------

    def set_run_state(self, state: Any) -> None:
        self.state = dict(state) if state else {}

    def get_run_state(self) -> dict[str, Any]:
        return dict(self.state)

    @property
    def stale_node_ids(self) -> list[str]:
        return list(self._stale)

    # --- Implementation -----------------------------------------------------

    def _compute_emit(self) -> None:
        nodes: list[NodeSpec] = []
        edges: list[EdgeSpec] = []
        aliases: dict[str, str] = {}
        prior_hashes: dict[str, dict[str, str]] = self.state.get("file_hashes") or {}
        new_hashes: dict[str, dict[str, str]] = {}
        scope_records = set(self.scope_records or ())

        if not self.openspec_root.is_dir():
            logger.warning("openspec_root not found: %s", self.openspec_root)
            self._emit_nodes = nodes
            self._emit_edges = edges
            return

        # 1. Active + archived changes
        changes_dir = self.openspec_root / "changes"
        if changes_dir.is_dir() and not scope_records:
            active_change_slugs = {
                entry.name
                for entry in changes_dir.iterdir()
                if entry.is_dir() and entry.name != "archive"
            }
            for entry in sorted(changes_dir.iterdir()):
                if not entry.is_dir():
                    continue
                if entry.name == "archive":
                    if self.include_archived:
                        archive_dir = entry
                        latest_archived_by_slug: dict[str, Path] = {}
                        for archived in sorted(archive_dir.iterdir()):
                            if archived.is_dir():
                                slug = _archive_slug(archived.name)
                                if slug in active_change_slugs:
                                    continue
                                latest_archived_by_slug[slug] = archived
                        for archived in latest_archived_by_slug.values():
                            self._materialize_change(
                                archived,
                                is_archived=True,
                                prior_hashes=prior_hashes,
                                new_hashes=new_hashes,
                                nodes=nodes,
                                edges=edges,
                                aliases=aliases,
                            )
                    continue
                self._materialize_change(
                    entry,
                    is_archived=False,
                    prior_hashes=prior_hashes,
                    new_hashes=new_hashes,
                    nodes=nodes,
                    edges=edges,
                    aliases=aliases,
                )

        # 2. Canonical capability specs
        specs_dir = self.openspec_root / "specs"
        if specs_dir.is_dir() and not scope_records:
            for cap_dir in sorted(specs_dir.iterdir()):
                if not cap_dir.is_dir():
                    continue
                spec_path = cap_dir / "spec.md"
                if not spec_path.is_file():
                    continue
                self._materialize_capability(
                    cap_dir.name,
                    spec_path,
                    prior_hashes=prior_hashes,
                    new_hashes=new_hashes,
                    nodes=nodes,
                    edges=edges,
                    aliases=aliases,
                )

        # 3. Reasoning content kinds (per add-substrate-reasoning-module):
        # openspec/hypotheses/<slug>/proposal.md (subdir shape),
        # openspec/observations/<slug>.md (single-file),
        # openspec/insights/<slug>.md (single-file).
        # Walks are no-ops when the directories don't exist (backward
        # compatible with repos that don't author reasoning content).
        if scope_records:
            self._materialize_scoped_records(
                scope_records,
                prior_hashes,
                new_hashes,
                nodes,
                edges,
                aliases,
            )
        else:
            self._walk_hypotheses(prior_hashes, new_hashes, nodes, edges, aliases)
            self._walk_observations(prior_hashes, new_hashes, nodes, edges, aliases)
            self._walk_insights(prior_hashes, new_hashes, nodes, edges, aliases)

        # 4. Detect deletions: change_ids in prior_hashes that aren't in new_hashes
        for change_id in prior_hashes:
            if scope_records and change_id not in scope_records:
                continue
            if change_id in new_hashes:
                continue
            # Mark the change and its subtree as stale (we don't have the
            # children list, so we mark just the change itself; the
            # backbone's stale-detection sweeps the rest by ownership)
            if change_id.startswith("cap:"):
                self._stale.append(change_id)
            elif change_id.startswith(("hyp:", "obs:", "insight:")):
                # Reasoning content keys carry their subtype prefix; the
                # stale list takes them verbatim.
                self._stale.append(change_id)
            else:
                self._stale.append(f"change:{change_id}")

        self._emit_nodes = nodes
        self._emit_edges = edges
        self._emit_aliases = aliases
        self.state = {"file_hashes": new_hashes}

    # --- Helpers -----------------------------------------------------------

    def _materialize_scoped_records(
        self,
        scope_records: set[str],
        prior_hashes: dict[str, dict[str, str]],
        new_hashes: dict[str, dict[str, str]],
        nodes: list[NodeSpec],
        edges: list[EdgeSpec],
        aliases: dict[str, str],
    ) -> None:
        self._materialize_scoped_changes(
            scope_records,
            prior_hashes,
            new_hashes,
            nodes,
            edges,
            aliases,
        )
        self._materialize_scoped_capabilities(
            scope_records,
            prior_hashes,
            new_hashes,
            nodes,
            edges,
            aliases,
        )
        self._materialize_scoped_hypotheses(
            scope_records,
            prior_hashes,
            new_hashes,
            nodes,
            edges,
            aliases,
        )
        self._materialize_scoped_observations(
            scope_records,
            prior_hashes,
            new_hashes,
            nodes,
            edges,
            aliases,
        )
        self._materialize_scoped_insights(
            scope_records,
            prior_hashes,
            new_hashes,
            nodes,
            edges,
            aliases,
        )

    def _materialize_scoped_changes(
        self,
        scope_records: set[str],
        prior_hashes: dict[str, dict[str, str]],
        new_hashes: dict[str, dict[str, str]],
        nodes: list[NodeSpec],
        edges: list[EdgeSpec],
        aliases: dict[str, str],
    ) -> None:
        wanted = {
            record
            for record in scope_records
            if not record.startswith(("cap:", "hyp:", "obs:", "insight:"))
        }
        if not wanted:
            return
        changes_dir = self.openspec_root / "changes"
        if not changes_dir.is_dir():
            return
        active_change_slugs = {
            entry.name
            for entry in changes_dir.iterdir()
            if entry.is_dir() and entry.name != "archive"
        }
        for entry in sorted(changes_dir.iterdir()):
            if not entry.is_dir():
                continue
            if entry.name == "archive":
                if not self.include_archived:
                    continue
                latest_archived_by_slug: dict[str, Path] = {}
                for archived in sorted(entry.iterdir()):
                    if not archived.is_dir():
                        continue
                    slug = _archive_slug(archived.name)
                    if slug in active_change_slugs:
                        continue
                    latest_archived_by_slug[slug] = archived
                for slug, archived in latest_archived_by_slug.items():
                    if slug not in wanted:
                        continue
                    self._materialize_change(
                        archived,
                        is_archived=True,
                        prior_hashes=prior_hashes,
                        new_hashes=new_hashes,
                        nodes=nodes,
                        edges=edges,
                        aliases=aliases,
                    )
                continue
            if entry.name in wanted:
                self._materialize_change(
                    entry,
                    is_archived=False,
                    prior_hashes=prior_hashes,
                    new_hashes=new_hashes,
                    nodes=nodes,
                    edges=edges,
                    aliases=aliases,
                )

    def _materialize_scoped_capabilities(
        self,
        scope_records: set[str],
        prior_hashes: dict[str, dict[str, str]],
        new_hashes: dict[str, dict[str, str]],
        nodes: list[NodeSpec],
        edges: list[EdgeSpec],
        aliases: dict[str, str],
    ) -> None:
        wanted = {
            record.removeprefix("cap:")
            for record in scope_records
            if record.startswith("cap:")
        }
        if not wanted:
            return
        specs_dir = self.openspec_root / "specs"
        if not specs_dir.is_dir():
            return
        for cap_name in sorted(wanted):
            spec_path = specs_dir / cap_name / "spec.md"
            if not spec_path.is_file():
                continue
            self._materialize_capability(
                cap_name,
                spec_path,
                prior_hashes=prior_hashes,
                new_hashes=new_hashes,
                nodes=nodes,
                edges=edges,
                aliases=aliases,
            )

    def _materialize_scoped_hypotheses(
        self,
        scope_records: set[str],
        prior_hashes: dict[str, dict[str, str]],
        new_hashes: dict[str, dict[str, str]],
        nodes: list[NodeSpec],
        edges: list[EdgeSpec],
        aliases: dict[str, str],
    ) -> None:
        wanted = {
            record.removeprefix("hyp:")
            for record in scope_records
            if record.startswith("hyp:")
        }
        if not wanted:
            return
        hypotheses_dir = self.openspec_root / "hypotheses"
        if not hypotheses_dir.is_dir():
            return
        for entry in sorted(hypotheses_dir.iterdir()):
            if not entry.is_dir():
                continue
            if entry.name == "archive":
                if not self.include_archived:
                    continue
                for archived in sorted(entry.iterdir()):
                    if archived.is_dir() and _archive_slug(archived.name) in wanted:
                        self._materialize_hypothesis(
                            archived,
                            is_archived=True,
                            prior_hashes=prior_hashes,
                            new_hashes=new_hashes,
                            nodes=nodes,
                            edges=edges,
                            aliases=aliases,
                        )
                continue
            if entry.name in wanted:
                self._materialize_hypothesis(
                    entry,
                    is_archived=False,
                    prior_hashes=prior_hashes,
                    new_hashes=new_hashes,
                    nodes=nodes,
                    edges=edges,
                    aliases=aliases,
                )

    def _materialize_scoped_observations(
        self,
        scope_records: set[str],
        prior_hashes: dict[str, dict[str, str]],
        new_hashes: dict[str, dict[str, str]],
        nodes: list[NodeSpec],
        edges: list[EdgeSpec],
        aliases: dict[str, str],
    ) -> None:
        wanted = {
            record.removeprefix("obs:")
            for record in scope_records
            if record.startswith("obs:")
        }
        if not wanted:
            return
        observations_dir = self.openspec_root / "observations"
        if not observations_dir.is_dir():
            return
        for entry in sorted(observations_dir.iterdir()):
            if entry.is_dir() and entry.name == "archive":
                if not self.include_archived:
                    continue
                for archived in sorted(entry.iterdir()):
                    if (
                        archived.is_file()
                        and archived.suffix == ".md"
                        and _archive_slug(archived.stem) in wanted
                    ):
                        self._materialize_observation(
                            archived,
                            is_archived=True,
                            prior_hashes=prior_hashes,
                            new_hashes=new_hashes,
                            nodes=nodes,
                            edges=edges,
                            aliases=aliases,
                        )
                continue
            if entry.is_file() and entry.suffix == ".md" and entry.stem in wanted:
                self._materialize_observation(
                    entry,
                    is_archived=False,
                    prior_hashes=prior_hashes,
                    new_hashes=new_hashes,
                    nodes=nodes,
                    edges=edges,
                    aliases=aliases,
                )

    def _materialize_scoped_insights(
        self,
        scope_records: set[str],
        prior_hashes: dict[str, dict[str, str]],
        new_hashes: dict[str, dict[str, str]],
        nodes: list[NodeSpec],
        edges: list[EdgeSpec],
        aliases: dict[str, str],
    ) -> None:
        wanted = {
            record.removeprefix("insight:")
            for record in scope_records
            if record.startswith("insight:")
        }
        if not wanted:
            return
        insights_dir = self.openspec_root / "insights"
        if not insights_dir.is_dir():
            return
        for entry in sorted(insights_dir.iterdir()):
            if entry.is_dir() and entry.name == "archive":
                if not self.include_archived:
                    continue
                for archived in sorted(entry.iterdir()):
                    if (
                        archived.is_file()
                        and archived.suffix == ".md"
                        and _archive_slug(archived.stem) in wanted
                    ):
                        self._materialize_insight(
                            archived,
                            is_archived=True,
                            prior_hashes=prior_hashes,
                            new_hashes=new_hashes,
                            nodes=nodes,
                            edges=edges,
                            aliases=aliases,
                        )
                continue
            if entry.is_file() and entry.suffix == ".md" and entry.stem in wanted:
                self._materialize_insight(
                    entry,
                    is_archived=False,
                    prior_hashes=prior_hashes,
                    new_hashes=new_hashes,
                    nodes=nodes,
                    edges=edges,
                    aliases=aliases,
                )

    @staticmethod
    def _normalize_path_mention(mention: str, proposal_dir_rel: Path) -> str | None:
        """Convert a path mention to a repo-relative path.

        Mentions take a few forms:
        - ``@/src/okg/foo.py`` — already repo-relative; strip the leading ``/``
        - ``src/okg/foo.py`` — repo-relative as-is
        - ``../sibling/proposal.md`` — relative to the proposal's directory
        - ``./foo.md`` — relative to the proposal's directory
        - ``http://...`` — skip (extractor already filters these)
        """
        if not mention:
            return None
        # `@/path` style — already repo-relative (the @/ prefix means root)
        if mention.startswith("/"):
            return mention.lstrip("/")
        # Relative paths: resolve against the proposal's directory
        if mention.startswith(("../", "./")):
            try:
                resolved = (proposal_dir_rel / mention).resolve()
                # Walk up from cwd to find the part after repo-root.
                # We constructed proposal_dir_rel as a relative path, so
                # resolve() would have prepended cwd. Strip it.
                cwd = Path.cwd().resolve()
                if resolved.is_relative_to(cwd):
                    return str(resolved.relative_to(cwd))
                # Otherwise just normalize the components manually
            except (OSError, ValueError):
                # Fall back to lexical path normalization below.
                pass
            # Fallback: manual joining + normalization
            parts = list(proposal_dir_rel.parts) + mention.split("/")
            stack: list[str] = []
            for p in parts:
                if p == "" or p == ".":
                    continue
                if p == "..":
                    if stack:
                        stack.pop()
                    continue
                stack.append(p)
            return "/".join(stack) if stack else None
        # Bare repo-relative path
        return mention

    # --- Change materialization --------------------------------------------

    def _materialize_change(
        self,
        change_dir: Path,
        *,
        is_archived: bool,
        prior_hashes: dict[str, dict[str, str]],
        new_hashes: dict[str, dict[str, str]],
        nodes: list[NodeSpec],
        edges: list[EdgeSpec],
        aliases: dict[str, str],
    ) -> None:
        # Resolve change_id (slug) and optional archived_date
        if is_archived:
            m = _ARCHIVE_DATE_RE.match(change_dir.name)
            if m:
                archived_date = m.group(1)
                slug = m.group(2)
            else:
                archived_date = None
                slug = change_dir.name
        else:
            archived_date = None
            slug = change_dir.name

        change_node_id = f"change:{slug}"
        # Compute file hashes for this change
        change_hashes: dict[str, str] = {}
        proposal_path = change_dir / "proposal.md"
        tasks_path = change_dir / "tasks.md"
        design_path = change_dir / "design.md"
        spec_dir = change_dir / "specs"
        candidate_files: list[Path] = []
        for p in [proposal_path, tasks_path, design_path]:
            if p.is_file():
                candidate_files.append(p)
        if spec_dir.is_dir():
            for spec_file in sorted(spec_dir.glob("*/spec.md")):
                candidate_files.append(spec_file)

        for p in candidate_files:
            change_hashes[str(p.relative_to(change_dir))] = file_sha256(p)
        new_hashes[slug] = change_hashes

        # If this change's files are unchanged from prior run, skip.
        # (Idempotent re-emission: we still want the node to exist if we
        # detected changes elsewhere — but since other modules don't
        # depend on this, skipping is safe.)
        if prior_hashes.get(slug) == change_hashes:
            return

        if not proposal_path.is_file():
            logger.warning("Skipping %s: no proposal.md", change_dir)
            return

        # Parse proposal.md
        proposal = parse_proposal_md(proposal_path)
        if proposal is None:
            return

        # Repo-relative path: `openspec/changes/<slug>` for
        # active changes; `openspec/changes/archive/<dated-slug>`
        # for archived. The convention is universal — openspec
        # lives under `<repo_root>/openspec/`, so relative_to
        # `openspec_root.parent` always yields the repo-relative
        # form. Absolute machine paths break portability (a graph
        # ingested on machine A is invalid on machine B); fixed
        # 2026-04-25 in `debug-substrate-okg-self` Phase 8.
        try:
            relpath = change_dir.relative_to(self.openspec_root.parent)
            path_attr = str(relpath)
        except ValueError:
            # openspec_root not under a parent we can pivot on —
            # fall back to the absolute path with a warning so the
            # operator notices.
            logger.warning(
                "openspec_root %s has no parent we can relativize "
                "against; emitting absolute path %s",
                self.openspec_root,
                change_dir,
            )
            path_attr = str(change_dir)

        # Pre-parse tasks.md for completed/total counts so the
        # change-level node carries an at-a-glance progress
        # summary (used by `openspec list --json`, the memex
        # portfolio dashboard, and the active-work MCP tool).
        # Per dogfood-canary follow-up gap #2 (2026-05-18):
        # before this, every OpenSpec change node had
        # completed_tasks/total_tasks unset, so the portfolio
        # dashboard couldn't compute readiness.
        completed_tasks = 0
        total_tasks = 0
        if tasks_path.is_file():
            for group in parse_tasks_md(tasks_path):
                for t in group.tasks:
                    total_tasks += 1
                    if (t.status or "").strip().lower() == "done":
                        completed_tasks += 1

        change_attrs: dict[str, Any] = {
            "change_id": slug,
            "title": proposal.title,
            "status": "archived" if is_archived else "active",
            # Path hint for the temporal-metadata resolver chain.
            # The change directory is the natural unit — git_log
            # on the relative path gives proposal-creation time.
            "path": path_attr,
            "completed_tasks": completed_tasks,
            "total_tasks": total_tasks,
        }
        if archived_date:
            change_attrs["archived_date"] = archived_date
        if proposal.why_text:
            change_attrs["why_text"] = proposal.why_text
        if proposal.what_changes_text:
            change_attrs["what_changes_text"] = proposal.what_changes_text
        if proposal.impact_text:
            change_attrs["impact_text"] = proposal.impact_text
        # Searchable text: title + start of why
        text_blob = " ".join([proposal.title, proposal.why_text[:500]]).strip()
        if text_blob:
            change_attrs["text"] = text_blob

        nodes.append(
            NodeSpec(
                node_id=change_node_id,
                subtype="open_spec_change",
                label=proposal.title[:80],
                attrs=change_attrs,
                source=self.ownership_id,
            )
        )
        aliases[slug] = change_node_id

        # Cross-module REFERENCES → SourceFile (from path mentions in
        # proposal text). Combine the prose so we catch mentions across
        # all sections. Resolve relative paths to repo-relative so the
        # IDs match what git_graph emits.
        full_text = "\n".join(
            [
                proposal.why_text,
                proposal.what_changes_text,
                proposal.impact_text,
            ]
        )
        # The proposal lives at <openspec_root>/changes/<slug>/proposal.md
        # (or .../changes/archive/<dated>/proposal.md). Compute its
        # parent directory's path relative to the repo root so we can
        # resolve `../foo` mentions correctly.
        if is_archived:
            proposal_dir_rel = Path("openspec/changes/archive") / change_dir.name
        else:
            proposal_dir_rel = Path("openspec/changes") / change_dir.name
        seen_refs: set[str] = set()
        for path_mention in extract_path_mentions(full_text):
            relpath = self._normalize_path_mention(path_mention, proposal_dir_rel)
            if relpath is None or relpath in seen_refs:
                continue
            seen_refs.add(relpath)
            edges.append(
                EdgeSpec(
                    src=change_node_id,
                    dst=f"file:{self.repo}:{relpath}",
                    edge_type="references",
                    attrs={"reason": "proposal_path_mention"},
                )
            )

        # Parse tasks.md if present
        if tasks_path.is_file():
            self._materialize_tasks(
                slug, tasks_path, change_node_id, nodes, edges, aliases
            )

        # Parse design.md if present
        if design_path.is_file():
            self._materialize_design(
                slug, design_path, change_node_id, nodes, edges, aliases
            )

        # Snapshot the nodes-list length BEFORE spec-delta materialization
        # so we can identify the criteria this change just emitted (per
        # the add-substrate-reasoning-module child's
        # `hypotheses_tested:` cross-link).
        nodes_len_before_specs = len(nodes)

        # Parse change-spec deltas if present
        if spec_dir.is_dir():
            for spec_file in sorted(spec_dir.glob("*/spec.md")):
                cap_name = spec_file.parent.name
                self._materialize_change_spec(
                    slug,
                    cap_name,
                    spec_file,
                    change_node_id,
                    nodes,
                    edges,
                    aliases,
                )

        # Optional frontmatter on proposal.md.
        # Per add-substrate-reasoning-module:
        #   `hypotheses_tested: [...]` emits
        #   `Hypothesis supported_by AcceptanceCriterion` edges for each
        #   criterion in this change against each named hypothesis.
        # Per define-memex-deployment-contract R3:
        #   `project: <slug>` emits `OpenSpecChange member_of Project`
        #   edge.
        change_frontmatter = parse_change_frontmatter(proposal_path)
        hypotheses_tested = change_frontmatter.get("hypotheses_tested") or []
        if hypotheses_tested:
            new_criterion_ids = [
                n.node_id
                for n in nodes[nodes_len_before_specs:]
                if n.subtype == "acceptance_criterion"
            ]
            for hyp_slug in hypotheses_tested:
                hyp_node_id = f"hyp:{hyp_slug}"
                for crit_id in new_criterion_ids:
                    edges.append(
                        EdgeSpec(
                            src=hyp_node_id,
                            dst=crit_id,
                            edge_type="supported_by",
                            attrs={
                                "reason": "hypotheses_tested_frontmatter",
                                "change_id": slug,
                            },
                        )
                    )
        # Per define-memex-deployment-contract R3 cross-shared rule
        # `OpenSpecChange member_of Project`: if the change's
        # frontmatter declares `project: <slug>`, emit the edge.
        # The corresponding narrowing must exist for projection to
        # accept the edge (declared in deployment bridges + the
        # `openspecchange_member_of_project` cross-link).
        project_slug = change_frontmatter.get("project")
        if isinstance(project_slug, str) and project_slug.strip():
            project_slug = project_slug.strip()
            project_node_id = (
                project_slug
                if project_slug.startswith("proj:")
                else f"proj:{project_slug}"
            )
            edges.append(
                EdgeSpec(
                    src=change_node_id,
                    dst=project_node_id,
                    edge_type="member_of",
                    attrs={
                        "reason": "project_frontmatter",
                        "change_id": slug,
                    },
                )
            )

    def _materialize_tasks(
        self,
        slug: str,
        tasks_path: Path,
        change_node_id: str,
        nodes: list[NodeSpec],
        edges: list[EdgeSpec],
        aliases: dict[str, str],
    ) -> None:
        for group in parse_tasks_md(tasks_path):
            group_id = f"task-group:{slug}:{group.group_index}"
            nodes.append(
                NodeSpec(
                    node_id=group_id,
                    subtype="task_group",
                    label=f"{group.group_index}. {group.title}"[:80],
                    attrs={
                        "group_id": group_id,
                        "change_id": slug,
                        "group_index": group.group_index,
                        "title": group.title,
                    },
                    source=self.ownership_id,
                )
            )
            edges.append(
                EdgeSpec(src=change_node_id, dst=group_id, edge_type="contains")
            )

            for task in group.tasks:
                task_id = f"task:{slug}:{task.task_number}"
                nodes.append(
                    NodeSpec(
                        node_id=task_id,
                        subtype="proposal_task",
                        label=f"{task.task_number} {task.description}"[:80],
                        attrs={
                            "task_id": task_id,
                            "change_id": slug,
                            "group_index": group.group_index,
                            "task_number": task.task_number,
                            "description": task.description,
                            "status": task.status,
                        },
                        source=self.ownership_id,
                    )
                )
                edges.append(EdgeSpec(src=group_id, dst=task_id, edge_type="contains"))

                for v_idx, verify in enumerate(task.verifies):
                    step_id = f"verify:{slug}:{task.task_number}:{v_idx}"
                    nodes.append(
                        NodeSpec(
                            node_id=step_id,
                            subtype="verification_step",
                            label=verify.text[:80],
                            attrs={
                                "step_id": step_id,
                                "task_id": task_id,
                                "text": verify.text,
                            },
                            source=self.ownership_id,
                        )
                    )
                    edges.append(
                        EdgeSpec(src=task_id, dst=step_id, edge_type="contains")
                    )

    def _materialize_design(
        self,
        slug: str,
        design_path: Path,
        change_node_id: str,
        nodes: list[NodeSpec],
        edges: list[EdgeSpec],
        aliases: dict[str, str],
    ) -> None:
        doc = parse_design_md(design_path)
        if doc is None:
            return

        # Goals
        for i, text in enumerate(doc.goals):
            gid = f"goal:{slug}:{i}"
            nodes.append(
                NodeSpec(
                    node_id=gid,
                    subtype="goal",
                    label=text[:80],
                    attrs={"goal_id": gid, "change_id": slug, "text": text},
                    source=self.ownership_id,
                )
            )
            edges.append(EdgeSpec(src=change_node_id, dst=gid, edge_type="contains"))

        # Non-Goals
        for i, text in enumerate(doc.non_goals):
            ngid = f"nongoal:{slug}:{i}"
            nodes.append(
                NodeSpec(
                    node_id=ngid,
                    subtype="non_goal",
                    label=text[:80],
                    attrs={"nongoal_id": ngid, "change_id": slug, "text": text},
                    source=self.ownership_id,
                )
            )
            edges.append(EdgeSpec(src=change_node_id, dst=ngid, edge_type="contains"))

        # Stakeholders
        for i, role in enumerate(doc.stakeholders):
            sid = f"stakeholder:{slug}:{i}"
            nodes.append(
                NodeSpec(
                    node_id=sid,
                    subtype="stakeholder",
                    label=role[:80],
                    attrs={"stakeholder_id": sid, "change_id": slug, "role": role},
                    source=self.ownership_id,
                )
            )
            edges.append(EdgeSpec(src=change_node_id, dst=sid, edge_type="contains"))

        # Decisions + Alternatives
        for dec in doc.decisions:
            dec_id = f"decision:{slug}:{dec.decision_number.lower()}"
            nodes.append(
                NodeSpec(
                    node_id=dec_id,
                    subtype="design_decision",
                    label=f"{dec.decision_number} {dec.title}"[:80],
                    attrs={
                        "decision_id": dec_id,
                        "change_id": slug,
                        "decision_number": dec.decision_number,
                        "title": dec.title,
                        "rationale": dec.rationale,
                    },
                    source=self.ownership_id,
                )
            )
            edges.append(EdgeSpec(src=change_node_id, dst=dec_id, edge_type="contains"))
            aliases[f"{slug}:{dec.decision_number}"] = dec_id

            for a_idx, alt in enumerate(dec.alternatives):
                alt_id = f"alt:{slug}:{dec.decision_number.lower()}:{a_idx}"
                nodes.append(
                    NodeSpec(
                        node_id=alt_id,
                        subtype="alternative",
                        label=alt.text[:80],
                        attrs={
                            "alternative_id": alt_id,
                            "decision_id": dec_id,
                            "text": alt.text,
                        },
                        source=self.ownership_id,
                    )
                )
                edges.append(EdgeSpec(src=dec_id, dst=alt_id, edge_type="contains"))
                edges.append(EdgeSpec(src=dec_id, dst=alt_id, edge_type="supersedes"))

        # Risks
        for risk in doc.risks:
            risk_id = f"risk:{slug}:{risk.risk_number.lower()}"
            nodes.append(
                NodeSpec(
                    node_id=risk_id,
                    subtype="risk",
                    label=f"{risk.risk_number} {risk.title}"[:80],
                    attrs={
                        "risk_id": risk_id,
                        "change_id": slug,
                        "risk_number": risk.risk_number,
                        "title": risk.title,
                        "mitigation": risk.mitigation,
                    },
                    source=self.ownership_id,
                )
            )
            edges.append(
                EdgeSpec(src=change_node_id, dst=risk_id, edge_type="contains")
            )

    def _materialize_change_spec(
        self,
        slug: str,
        cap_name: str,
        spec_path: Path,
        change_node_id: str,
        nodes: list[NodeSpec],
        edges: list[EdgeSpec],
        aliases: dict[str, str],
    ) -> None:
        deltas = parse_change_spec_md(spec_path)
        for delta in deltas:
            delta_id = f"delta:{slug}:{cap_name}:{delta.delta_kind}"
            nodes.append(
                NodeSpec(
                    node_id=delta_id,
                    subtype="spec_delta",
                    label=f"{delta.delta_kind} {cap_name}"[:80],
                    attrs={
                        "delta_id": delta_id,
                        "change_id": slug,
                        "capability_name": cap_name,
                        "delta_kind": delta.delta_kind,
                    },
                    source=self.ownership_id,
                )
            )
            edges.append(
                EdgeSpec(src=change_node_id, dst=delta_id, edge_type="contains")
            )
            edges.append(
                EdgeSpec(
                    src=delta_id,
                    dst=f"cap:{cap_name}",
                    edge_type="targets",
                )
            )

            # Acceptance criteria + scenarios contained in the delta
            self._emit_criteria(
                criteria_records=delta.criteria,
                capability_name=cap_name,
                container_id=delta_id,
                slug_for_id=f"{slug}:{cap_name}:{delta.delta_kind}",
                nodes=nodes,
                edges=edges,
            )

    def _materialize_capability(
        self,
        cap_name: str,
        spec_path: Path,
        prior_hashes: dict[str, dict[str, str]],
        new_hashes: dict[str, dict[str, str]],
        nodes: list[NodeSpec],
        edges: list[EdgeSpec],
        aliases: dict[str, str],
    ) -> None:
        # Use a per-capability key so canonical specs participate in
        # incremental crawl just like change directories.
        cap_key = f"cap:{cap_name}"
        cap_hashes = {"spec.md": file_sha256(spec_path)}
        new_hashes[cap_key] = cap_hashes
        if prior_hashes.get(cap_key) == cap_hashes:
            return

        doc = parse_canonical_spec_md(spec_path)
        if doc is None:
            return
        cap_id = f"cap:{cap_name}"
        nodes.append(
            NodeSpec(
                node_id=cap_id,
                subtype="spec_capability",
                label=cap_name,
                attrs={
                    "capability_name": cap_name,
                    "purpose": doc.purpose,
                    "requirement_count": len(doc.criteria),
                    "text": doc.purpose[:500] if doc.purpose else cap_name,
                },
                source=self.ownership_id,
            )
        )
        aliases[cap_name] = cap_id
        self._emit_criteria(
            criteria_records=doc.criteria,
            capability_name=cap_name,
            container_id=cap_id,
            slug_for_id=cap_name,
            nodes=nodes,
            edges=edges,
        )

    def _emit_criteria(
        self,
        *,
        criteria_records,
        capability_name: str,
        container_id: str,
        slug_for_id: str,
        nodes: list[NodeSpec],
        edges: list[EdgeSpec],
    ) -> None:
        for c_idx, crit in enumerate(criteria_records):
            crit_id = f"crit:{slug_for_id}:{c_idx}"
            crit_attrs: dict[str, Any] = {
                "criterion_id": crit_id,
                "capability_name": capability_name,
                "title": crit.title,
                "scenario_count": len(crit.scenarios),
            }
            if crit.text:
                crit_attrs["text"] = crit.text
            nodes.append(
                NodeSpec(
                    node_id=crit_id,
                    subtype="acceptance_criterion",
                    label=crit.title[:80],
                    attrs=crit_attrs,
                    source=self.ownership_id,
                )
            )
            edges.append(EdgeSpec(src=container_id, dst=crit_id, edge_type="contains"))

            for sc in crit.scenarios:
                scn_id = f"scn:{slug_for_id}:{c_idx}:{sc.index}"
                scn_attrs: dict[str, Any] = {
                    "scenario_id": scn_id,
                    "criterion_id": crit_id,
                    "index": sc.index,
                    "title": sc.title,
                }
                if sc.text:
                    scn_attrs["text"] = sc.text
                    # Per dogfood-canary DQ audit (2026-05-19):
                    # parse the Gherkin-style `**GIVEN**`/`**WHEN**`/
                    # `**THEN**`/`**AND**` bullets into separate
                    # attrs so MCP / viz / read-side queries can
                    # surface the structured shape without re-
                    # parsing prose. Each field is the BULLET's
                    # text body, joined with newlines if multiple
                    # bullets share the same keyword (common for
                    # AND/THEN).
                    parsed = _parse_scenario_gherkin(sc.text)
                    for k, v in parsed.items():
                        if v:
                            scn_attrs[k] = v
                nodes.append(
                    NodeSpec(
                        node_id=scn_id,
                        subtype="acceptance_scenario",
                        label=sc.title[:80],
                        attrs=scn_attrs,
                        source=self.ownership_id,
                    )
                )
                edges.append(EdgeSpec(src=crit_id, dst=scn_id, edge_type="contains"))

    # --- Reasoning content (add-substrate-reasoning-module) ----------------
    #
    # Per the umbrella `expand-memex-to-personal-okg` D11 (OpenSpec
    # directory reuse), this adapter walks three parallel content
    # kinds and emits into the substrate `reasoning` module's
    # subtypes (Hypothesis, Observation, Insight). The walks are
    # no-ops when the directories don't exist (backward compat).
    #
    # Provenance attrs follow the design:
    #   source_subtype = openspec_<kind>
    #   source_path    = openspec/<kind>/<slug>{/,}
    #   frontmatter_schema_version = <int>
    #
    # `UnknownFrontmatterSchemaVersionError` is caught here, the
    # offending file is appended to `_degraded_files`, and the run
    # continues (other files are not affected).
    #
    # Direction convention follows agent_memory: for `supported_by` /
    # `contradicted_by`, src is the supported/contradicted party
    # (the Hypothesis), dst is the supporter/contradictor.

    def _walk_hypotheses(
        self,
        prior_hashes: dict[str, dict[str, str]],
        new_hashes: dict[str, dict[str, str]],
        nodes: list[NodeSpec],
        edges: list[EdgeSpec],
        aliases: dict[str, str],
    ) -> None:
        hypotheses_dir = self.openspec_root / "hypotheses"
        if not hypotheses_dir.is_dir():
            return
        for entry in sorted(hypotheses_dir.iterdir()):
            if not entry.is_dir():
                continue
            if entry.name == "archive":
                if self.include_archived:
                    for archived in sorted(entry.iterdir()):
                        if archived.is_dir():
                            self._materialize_hypothesis(
                                archived,
                                is_archived=True,
                                prior_hashes=prior_hashes,
                                new_hashes=new_hashes,
                                nodes=nodes,
                                edges=edges,
                                aliases=aliases,
                            )
                continue
            self._materialize_hypothesis(
                entry,
                is_archived=False,
                prior_hashes=prior_hashes,
                new_hashes=new_hashes,
                nodes=nodes,
                edges=edges,
                aliases=aliases,
            )

    def _walk_observations(
        self,
        prior_hashes: dict[str, dict[str, str]],
        new_hashes: dict[str, dict[str, str]],
        nodes: list[NodeSpec],
        edges: list[EdgeSpec],
        aliases: dict[str, str],
    ) -> None:
        observations_dir = self.openspec_root / "observations"
        if not observations_dir.is_dir():
            return
        for entry in sorted(observations_dir.iterdir()):
            if entry.is_dir() and entry.name == "archive":
                if self.include_archived:
                    for archived in sorted(entry.iterdir()):
                        if archived.is_file() and archived.suffix == ".md":
                            self._materialize_observation(
                                archived,
                                is_archived=True,
                                prior_hashes=prior_hashes,
                                new_hashes=new_hashes,
                                nodes=nodes,
                                edges=edges,
                                aliases=aliases,
                            )
                continue
            if entry.is_file() and entry.suffix == ".md" and entry.name != "README.md":
                self._materialize_observation(
                    entry,
                    is_archived=False,
                    prior_hashes=prior_hashes,
                    new_hashes=new_hashes,
                    nodes=nodes,
                    edges=edges,
                    aliases=aliases,
                )

    def _walk_insights(
        self,
        prior_hashes: dict[str, dict[str, str]],
        new_hashes: dict[str, dict[str, str]],
        nodes: list[NodeSpec],
        edges: list[EdgeSpec],
        aliases: dict[str, str],
    ) -> None:
        insights_dir = self.openspec_root / "insights"
        if not insights_dir.is_dir():
            return
        for entry in sorted(insights_dir.iterdir()):
            if entry.is_dir() and entry.name == "archive":
                if self.include_archived:
                    for archived in sorted(entry.iterdir()):
                        if archived.is_file() and archived.suffix == ".md":
                            self._materialize_insight(
                                archived,
                                is_archived=True,
                                prior_hashes=prior_hashes,
                                new_hashes=new_hashes,
                                nodes=nodes,
                                edges=edges,
                                aliases=aliases,
                            )
                continue
            if entry.is_file() and entry.suffix == ".md" and entry.name != "README.md":
                self._materialize_insight(
                    entry,
                    is_archived=False,
                    prior_hashes=prior_hashes,
                    new_hashes=new_hashes,
                    nodes=nodes,
                    edges=edges,
                    aliases=aliases,
                )

    # --- Single-file path / slug helpers -----------------------------------

    def _resolve_slug_and_date(
        self,
        entry: Path,
        *,
        is_archived: bool,
        strip_suffix: str = "",
    ) -> tuple[str, str | None]:
        """Resolve slug + optional archived_date from a directory or
        file name. `strip_suffix` is removed first (e.g. `.md`).
        """
        raw = entry.name
        if strip_suffix and raw.endswith(strip_suffix):
            raw = raw[: -len(strip_suffix)]
        if is_archived:
            m = _ARCHIVE_DATE_RE.match(raw)
            if m:
                return m.group(2), m.group(1)
        return raw, None

    def _repo_relative_path(self, path: Path) -> str:
        try:
            return str(path.relative_to(self.openspec_root.parent))
        except ValueError:
            logger.warning(
                "openspec_root %s has no parent we can relativize "
                "against; emitting absolute path %s",
                self.openspec_root,
                path,
            )
            return str(path)

    def _parse_reasoning_or_degrade(
        self,
        path: Path,
        *,
        kind: str,
    ) -> Any | None:
        """Wrap the parser to convert
        UnknownFrontmatterSchemaVersionError into degraded-file
        bookkeeping. Returns the parsed doc or None.
        """
        try:
            if kind == "hypothesis":
                return parse_reasoning_proposal_md(path)
            return parse_reasoning_single_file_md(path)
        except UnknownFrontmatterSchemaVersionError as e:
            logger.error(
                "Reasoning %s rejected (unknown schema_version=%r): %s",
                kind,
                e.version,
                e.path,
            )
            self._degraded_files.append(str(e.path))
            return None

    # --- Hypothesis materialization ----------------------------------------

    def _materialize_hypothesis(
        self,
        hyp_dir: Path,
        *,
        is_archived: bool,
        prior_hashes: dict[str, dict[str, str]],
        new_hashes: dict[str, dict[str, str]],
        nodes: list[NodeSpec],
        edges: list[EdgeSpec],
        aliases: dict[str, str],
    ) -> None:
        slug, archived_date = self._resolve_slug_and_date(
            hyp_dir,
            is_archived=is_archived,
        )
        proposal_path = hyp_dir / "proposal.md"
        if not proposal_path.is_file():
            return

        # Hash cursor — same scheme as _materialize_change
        hyp_key = f"hyp:{slug}"
        hyp_hashes = {"proposal.md": file_sha256(proposal_path)}
        new_hashes[hyp_key] = hyp_hashes
        if prior_hashes.get(hyp_key) == hyp_hashes:
            return

        doc = self._parse_reasoning_or_degrade(proposal_path, kind="hypothesis")
        if doc is None:
            return
        fm = doc.frontmatter

        # Required-field validation
        missing = [k for k in ("id", "statement", "status") if fm.get(k) in (None, "")]
        if missing:
            logger.warning(
                "Reasoning hypothesis missing required field(s) %s; skipping %s",
                missing,
                proposal_path,
            )
            return
        if fm["status"] not in _HYPOTHESIS_STATUSES:
            logger.warning(
                "Reasoning hypothesis unknown status=%r; skipping %s",
                fm["status"],
                proposal_path,
            )
            return
        if fm["id"] != slug:
            logger.warning(
                "Hypothesis frontmatter id=%r != dir slug=%r in %s; "
                "using id from frontmatter as alias key",
                fm["id"],
                slug,
                proposal_path,
            )

        node_id = f"hyp:{fm['id']}"
        path_attr = self._repo_relative_path(hyp_dir) + "/"
        attrs: dict[str, Any] = {
            "hypothesis_id": fm["id"],
            "statement": fm["statement"],
            "status": fm["status"],
            "source_subtype": "openspec_hypothesis",
            "source_path": path_attr,
            "frontmatter_schema_version": fm.get(
                "schema_version",
                CURRENT_FRONTMATTER_SCHEMA_VERSION,
            ),
        }
        for opt in ("confidence", "domain"):
            if fm.get(opt) is not None:
                attrs[opt] = fm[opt]
        if fm.get("related_projects"):
            attrs["related_projects"] = ",".join(str(p) for p in fm["related_projects"])
        if doc.body:
            attrs["text"] = doc.body
        if archived_date:
            attrs["archived_date"] = archived_date

        nodes.append(
            NodeSpec(
                node_id=node_id,
                subtype="hypothesis",
                label=fm["statement"][:80],
                attrs=attrs,
                source=self.ownership_id,
            )
        )
        aliases[fm["id"]] = node_id

        # Frontmatter-driven edges
        for princ_slug in fm.get("principles_applied") or []:
            edges.append(
                EdgeSpec(
                    src=node_id,
                    dst=f"princ:{princ_slug}",
                    edge_type="cites",
                    attrs={"reason": "principles_applied_frontmatter"},
                )
            )
        for princ_slug in fm.get("predicted_by_principles") or []:
            edges.append(
                EdgeSpec(
                    src=f"princ:{princ_slug}",
                    dst=node_id,
                    edge_type="predicts",
                    attrs={"reason": "predicted_by_principles_frontmatter"},
                )
            )
        sup = fm.get("supersedes")
        if sup:
            edges.append(
                EdgeSpec(
                    src=node_id,
                    dst=f"hyp:{sup}",
                    edge_type="supersedes",
                    attrs={"reason": "supersedes_frontmatter"},
                )
            )

    # --- Observation materialization ---------------------------------------

    def _materialize_observation(
        self,
        obs_path: Path,
        *,
        is_archived: bool,
        prior_hashes: dict[str, dict[str, str]],
        new_hashes: dict[str, dict[str, str]],
        nodes: list[NodeSpec],
        edges: list[EdgeSpec],
        aliases: dict[str, str],
    ) -> None:
        slug, archived_date = self._resolve_slug_and_date(
            obs_path,
            is_archived=is_archived,
            strip_suffix=".md",
        )

        obs_key = f"obs:{slug}"
        obs_hashes = {obs_path.name: file_sha256(obs_path)}
        new_hashes[obs_key] = obs_hashes
        if prior_hashes.get(obs_key) == obs_hashes:
            return

        doc = self._parse_reasoning_or_degrade(obs_path, kind="observation")
        if doc is None:
            return
        fm = doc.frontmatter

        missing = [
            k
            for k in (
                "id",
                "statement",
                "status",
                "source_kind",
                "source_ref",
            )
            if fm.get(k) in (None, "")
        ]
        if missing:
            logger.warning(
                "Reasoning observation missing required field(s) %s; skipping %s",
                missing,
                obs_path,
            )
            return
        if fm["status"] not in _OBSERVATION_STATUSES:
            logger.warning(
                "Reasoning observation unknown status=%r; skipping %s",
                fm["status"],
                obs_path,
            )
            return
        if fm["source_kind"] not in _OBSERVATION_SOURCE_KINDS:
            logger.warning(
                "Reasoning observation unknown source_kind=%r; skipping %s",
                fm["source_kind"],
                obs_path,
            )
            return
        if fm["id"] != slug:
            logger.warning(
                "Observation frontmatter id=%r != file slug=%r in %s",
                fm["id"],
                slug,
                obs_path,
            )

        node_id = f"obs:{fm['id']}"
        path_attr = self._repo_relative_path(obs_path)
        attrs: dict[str, Any] = {
            "observation_id": fm["id"],
            "statement": fm["statement"],
            "status": fm["status"],
            "source_kind": fm["source_kind"],
            "source_ref": str(fm["source_ref"]),
            "source_subtype": "openspec_observation",
            "source_path": path_attr,
            "frontmatter_schema_version": fm.get(
                "schema_version",
                CURRENT_FRONTMATTER_SCHEMA_VERSION,
            ),
        }
        for opt in ("measured_value", "confidence", "derives_from_candidate"):
            if fm.get(opt) is not None:
                attrs[opt] = fm[opt]
        observed_at = fm.get("observed_at")
        if observed_at is not None:
            # Preserve as string; substrate's TimeResolver chain
            # handles ISO date parsing downstream.
            attrs["observed_at"] = str(observed_at)
        if doc.body:
            attrs["text"] = doc.body
        if archived_date:
            attrs["archived_date"] = archived_date

        nodes.append(
            NodeSpec(
                node_id=node_id,
                subtype="observation",
                label=fm["statement"][:80],
                attrs=attrs,
                source=self.ownership_id,
            )
        )
        aliases[fm["id"]] = node_id

        for hyp_slug in fm.get("supports_hypotheses") or []:
            edges.append(
                EdgeSpec(
                    src=f"hyp:{hyp_slug}",
                    dst=node_id,
                    edge_type="supported_by",
                    attrs={"reason": "supports_hypotheses_frontmatter"},
                )
            )
        for hyp_slug in fm.get("contradicts_hypotheses") or []:
            edges.append(
                EdgeSpec(
                    src=f"hyp:{hyp_slug}",
                    dst=node_id,
                    edge_type="contradicted_by",
                    attrs={"reason": "contradicts_hypotheses_frontmatter"},
                )
            )
        for princ_slug in fm.get("instantiates_principles") or []:
            edges.append(
                EdgeSpec(
                    src=node_id,
                    dst=f"princ:{princ_slug}",
                    edge_type="instantiates",
                    attrs={"reason": "instantiates_principles_frontmatter"},
                )
            )

    # --- Insight materialization -------------------------------------------

    def _materialize_insight(
        self,
        insight_path: Path,
        *,
        is_archived: bool,
        prior_hashes: dict[str, dict[str, str]],
        new_hashes: dict[str, dict[str, str]],
        nodes: list[NodeSpec],
        edges: list[EdgeSpec],
        aliases: dict[str, str],
    ) -> None:
        slug, archived_date = self._resolve_slug_and_date(
            insight_path,
            is_archived=is_archived,
            strip_suffix=".md",
        )

        insight_key = f"insight:{slug}"
        insight_hashes = {insight_path.name: file_sha256(insight_path)}
        new_hashes[insight_key] = insight_hashes
        if prior_hashes.get(insight_key) == insight_hashes:
            return

        doc = self._parse_reasoning_or_degrade(insight_path, kind="insight")
        if doc is None:
            return
        fm = doc.frontmatter

        missing = [
            k
            for k in ("id", "statement", "status", "synthesizes_connection_refs")
            if fm.get(k) in (None, "")
        ]
        if missing:
            logger.warning(
                "Reasoning insight missing required field(s) %s; skipping %s",
                missing,
                insight_path,
            )
            return
        if fm["status"] not in _INSIGHT_STATUSES:
            logger.warning(
                "Reasoning insight unknown status=%r; skipping %s",
                fm["status"],
                insight_path,
            )
            return
        synth = fm.get("synthesizes_connection_refs") or []
        if not isinstance(synth, list) or len(synth) < 1:
            logger.warning(
                "Reasoning insight requires >=1 synthesizes_connection_refs; "
                "skipping %s",
                insight_path,
            )
            return
        if fm["id"] != slug:
            logger.warning(
                "Insight frontmatter id=%r != file slug=%r in %s",
                fm["id"],
                slug,
                insight_path,
            )

        node_id = f"insight:{fm['id']}"
        path_attr = self._repo_relative_path(insight_path)
        attrs: dict[str, Any] = {
            "insight_id": fm["id"],
            "statement": fm["statement"],
            "status": fm["status"],
            "synthesizes_connection_refs": ",".join(str(c) for c in synth),
            "source_subtype": "openspec_insight",
            "source_path": path_attr,
            "frontmatter_schema_version": fm.get(
                "schema_version",
                CURRENT_FRONTMATTER_SCHEMA_VERSION,
            ),
        }
        for opt in ("confidence", "domain"):
            if fm.get(opt) is not None:
                attrs[opt] = fm[opt]
        cites_obs = fm.get("cites_observation_refs") or []
        if cites_obs:
            attrs["cites_observation_refs"] = ",".join(str(o) for o in cites_obs)
        if doc.body:
            attrs["text"] = doc.body
        if archived_date:
            attrs["archived_date"] = archived_date

        nodes.append(
            NodeSpec(
                node_id=node_id,
                subtype="insight",
                label=fm["statement"][:80],
                attrs=attrs,
                source=self.ownership_id,
            )
        )
        aliases[fm["id"]] = node_id

        for connect_slug in synth:
            edges.append(
                EdgeSpec(
                    src=node_id,
                    dst=f"connect:{connect_slug}",
                    edge_type="references",
                    attrs={
                        "reason": "synthesizes_connection_refs_frontmatter",
                    },
                )
            )
        for obs_slug in cites_obs:
            edges.append(
                EdgeSpec(
                    src=node_id,
                    dst=f"obs:{obs_slug}",
                    edge_type="cites",
                    attrs={"reason": "cites_observation_refs_frontmatter"},
                )
            )


def _archive_slug(name: str) -> str:
    match = _ARCHIVE_DATE_RE.match(name)
    return match.group(2) if match else name
