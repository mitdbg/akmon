"""Shared infrastructure for non-Python AST enrichers.

Per `cleanup-personal-okg-data-quality` C1 item 5. Each non-Python
parser (markdown/yaml/toml/json) does the same outer dance:

1. Query `nodes_live` for `source_file` rows of a given suffix
   whose AST hasn't been emitted yet (no outbound `contains`
   edges to the parser's target subtype).
2. Read each file from disk (using the source_file's `path`
   attribute relative to the deployment repo root).
3. Call the parser's pure-function `parse(text)` and get back
   a `Parsed` bundle of nodes + edges with deterministic ids.
4. Emit the facts via `_fact_emitters.emit_node_fact` /
   `emit_edge_fact`.
5. Return `EnrichResult` with the counts.

Pure-function parsers are tested standalone (no DB). The
enricher base handles the DB plumbing.
"""
from __future__ import annotations

import dataclasses
import hashlib
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

import psycopg

from okg.substrate.enrichers.base import EnrichResult
from okg.substrate.derived_node_liveness import (
    resolve_declared_support_policy,
)
from okg.substrate.library.enrichers import DerivedSupportPolicy
from okg.substrate.library.enrichers._fact_emitters import (
    FactEmitOutcome,
    emit_edge_fact,
    emit_node_fact,
)

logger = logging.getLogger(__name__)


@dataclasses.dataclass(frozen=True)
class ParsedNode:
    """One structural node emitted by a non-Python parser."""

    node_id: str
    subtype: str
    attrs: dict[str, Any]


@dataclasses.dataclass(frozen=True)
class ParsedEdge:
    """One containment edge emitted by a non-Python parser."""

    src: str
    dst: str
    edge_type: str
    attrs: dict[str, Any] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass(frozen=True)
class ParsedFile:
    """Bundle returned by a parser's pure-function entry point."""

    nodes: list[ParsedNode]
    edges: list[ParsedEdge]


def fact_id(parts: Iterable[str]) -> str:
    """Deterministic short hash for node_id / edge dedupe."""
    payload = "|".join(parts).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()[:16]


def kebab_slug(text: str, max_len: int = 60) -> str:
    """Lower-case kebab-cased slug, capped.

    Used for MarkdownHeading anchors and ConfigKey identifiers
    where the upstream text isn't suitable for a node_id.
    """
    import re

    cleaned = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return cleaned[:max_len] or "unnamed"


class NonPythonStructuralEnricher:
    """Base class for the 4 non-Python structural enrichers.

    PACT admit-derived-node-emission / bind-derived-node-provenance: this
    enricher writes nodes, so it must declare both that its node surface
    exists and that it supplies provenance. Its subtypes come from whatever
    the parser finds in a file, so the surface is declared UNBOUNDED rather
    than enumerated — the same marker the agent write-back uses. Subclasses
    do narrow it via ``target_subtypes``; tightening the declaration to that
    per-subclass bound is worthwhile follow-up, but declaring a snapshot of
    it here would be a false bound that breaks on the next file type.

    Subclasses set:
      - `name`: enricher identity for logs / source_record_id.
      - `target_subtypes`: tuple of `subtype` values the parser
        emits (so the "already emitted" predicate can scope).
      - `suffix`: file suffix the enricher claims (e.g. ".md").
      - `extractor_version`: bumped when the parser semantics
        change so old emissions get re-projected.

    And override:
      - `parse(text: str, *, repo: str, path: str) -> ParsedFile`:
        pure function; tested without a DB.
    """

    name: str = ""  # set in subclasses
    target_subtypes: tuple[str, ...] = ()
    suffix: str = ""
    extractor_version: str = "1.0.0"
    emits_edge_types: tuple[str, ...] = ("contains",)
    reads_subtypes: tuple[str, ...] = ("source_file",)
    unbounded_write_dimensions: tuple[str, ...] = ("nodes",)
    declares_node_provenance: bool = True
    # Annotated, never defaulted: each concrete structural enricher declares
    # its own node liveness policy (validated at enricher registration).
    support_policy: DerivedSupportPolicy

    def __init__(self, *, repo_root: Optional[str | Path] = None) -> None:
        self.repo_root = Path(repo_root) if repo_root else None

    # ----------------------------------------------------------
    # Per-subclass narrowing declarations. Concrete subclasses
    # add (source_file, contains, <target_subtype>) per their
    # target subtype.
    # ----------------------------------------------------------

    @property
    def requires_narrowings(self) -> tuple[tuple[str, str, str], ...]:
        return tuple(
            ("source_file", "contains", t) for t in self.target_subtypes
        )

    # ----------------------------------------------------------
    # Subclasses override. Return ParsedFile with deterministic
    # node_ids (e.g. `mdheading:<repo>:<path>:<slug>`).
    # ----------------------------------------------------------

    def parse(
        self, text: str, *, repo: str, path: str,
    ) -> ParsedFile:
        raise NotImplementedError

    # ----------------------------------------------------------
    # Enricher main loop. Iterates undecomposed source_files,
    # reads each from disk, calls parse(), emits facts.
    # ----------------------------------------------------------

    def enrich(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        **_publisher_kwargs: object,
    ) -> EnrichResult:
        n_emitted = 0
        n_skipped = 0
        n_sanitized = 0
        n_rejected = 0
        run_id = str(uuid.uuid4())
        observed_at = datetime.now(timezone.utc)

        candidates = self._candidate_source_files(
            conn, generation_id=generation_id,
        )
        for sf_node_id, sf_attrs in candidates:
            repo = str(sf_attrs.get("repo") or "")
            path = str(sf_attrs.get("path") or "")
            if not (repo and path):
                n_skipped += 1
                continue
            disk_path = self._resolve_disk_path(repo, path)
            if disk_path is None or not disk_path.is_file():
                logger.debug(
                    "%s: source_file %s has no readable disk path "
                    "(repo=%s, path=%s, resolved=%s); skipping",
                    self.name, sf_node_id, repo, path, disk_path,
                )
                n_skipped += 1
                continue
            try:
                text = disk_path.read_text(encoding="utf-8")
            except (OSError, UnicodeDecodeError) as exc:
                logger.warning(
                    "%s: read failed for %s: %s",
                    self.name, disk_path, exc,
                )
                n_skipped += 1
                continue
            parsed = self.parse(text, repo=repo, path=path)
            for node in parsed.nodes:
                outcome = self._emit_node(
                    conn, node, sf_node_id, run_id, observed_at,
                )
                n_sanitized += outcome.sanitized_value_count
                if not outcome.emitted:
                    n_rejected += 1
            for edge in parsed.edges:
                outcome = self._emit_edge(
                    conn, edge, sf_node_id, run_id, observed_at,
                )
                n_sanitized += outcome.sanitized_value_count
                if outcome.emitted:
                    n_emitted += 1
                else:
                    n_rejected += 1
        logger.info(
            "%s: emitted %d edges, skipped %d source_files",
            self.name, n_emitted, n_skipped,
        )
        if n_sanitized or n_rejected:
            # Loud per-run totals per `sanitize-fact-text-at-admission`;
            # per-emit detail is logged (capped) by the fact emitters.
            logger.warning(
                "%s: sanitized %d NUL-bearing values; rejected %d facts "
                "with NUL in identity fields",
                self.name, n_sanitized, n_rejected,
            )
        return EnrichResult(
            enricher_name=self.name,
            n_edges_emitted=n_emitted,
            n_edges_skipped=n_skipped,
            sanitized_value_count=n_sanitized,
            rejected_fact_count=n_rejected,
        )

    # ----------------------------------------------------------
    # Internal: candidate selection, disk-path resolution,
    # fact emission.
    # ----------------------------------------------------------

    def _candidate_source_files(
        self, conn: psycopg.Connection, *, generation_id: int,
    ) -> list[tuple[str, dict[str, Any]]]:
        """SELECT source_file rows whose AST is missing.

        A source_file is a candidate iff
          - its `path` attr ends with the enricher's suffix, AND
          - NO outbound `contains` edge points to any node of
            this enricher's `target_subtypes`.
        """
        from okg.substrate.graph_store import ChronosGraphStore

        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout_generation(generation_id)
        rows = graph.query(
            """
            SELECT sf.node_id, sf.attrs
              FROM okg.graph_nodes sf
             WHERE sf.subtype = 'source_file'
               AND (sf.attrs->>'path') LIKE :suffix_glob
               AND NOT EXISTS (
                 SELECT 1
                   FROM okg.graph_edges e
                   JOIN okg.graph_nodes tgt
                        ON tgt.node_id = e.dst
                  WHERE e.src = sf.node_id
                    AND e.edge_type = 'contains'
                    AND tgt.subtype = ANY(:targets)
               )
            """,
            {
                "suffix_glob": f"%{self.suffix}",
                "targets": list(self.target_subtypes),
            },
        )
        return [(row["node_id"], row["attrs"]) for row in rows]

    def _resolve_disk_path(self, repo: str, path: str) -> Optional[Path]:
        """Resolve `(repo, path)` to a Path on disk.

        Default: if `self.repo_root` is set, join `repo_root /
        path`. Otherwise return None (subclasses can override for
        per-repo lookup).
        """
        if self.repo_root is None:
            return None
        return (self.repo_root / path).resolve()

    def _emit_node(
        self,
        conn: psycopg.Connection,
        node: ParsedNode,
        sf_node_id: str,
        run_id: str,
        observed_at: datetime,
    ) -> "FactEmitOutcome":
        dedupe_key = fact_id([
            self.name, "node", node.subtype, node.node_id,
        ])
        attrs = {
            **node.attrs,
            "extractor": self.name,
            "extractor_version": self.extractor_version,
            "provenance": "derived_deterministic",
            "confidence": 1.0,
        }
        return emit_node_fact(
            conn,
            enricher_name=self.name,
            node_id=node.node_id,
            subtype=node.subtype,
            attrs=attrs,
            dedupe_key=dedupe_key,
            source_record_id={
                "enricher": self.name,
                "source_file_ref": sf_node_id,
                "node_id": node.node_id,
            },
            source_revision={"run_id": run_id},
            run_id=run_id,
            observed_at=observed_at,
            # PACT bind-derived-node-provenance: every structural node is
            # parsed out of one source file, whose node id is sf_node_id.
            derived_from=[sf_node_id],
            support_policy=resolve_declared_support_policy(self),
        )

    def _emit_edge(
        self,
        conn: psycopg.Connection,
        edge: ParsedEdge,
        sf_node_id: str,
        run_id: str,
        observed_at: datetime,
    ) -> "FactEmitOutcome":
        dedupe_key = fact_id([
            self.name, "edge", edge.edge_type, edge.src, edge.dst,
        ])
        attrs = {
            **edge.attrs,
            "extractor": self.name,
            "extractor_version": self.extractor_version,
        }
        return emit_edge_fact(
            conn,
            enricher_name=self.name,
            src=edge.src,
            dst=edge.dst,
            edge_type=edge.edge_type,
            attrs=attrs,
            provenance="derived_deterministic",
            confidence=1.0,
            dedupe_key=dedupe_key,
            source_record_id={
                "enricher": self.name,
                "source_file_ref": sf_node_id,
                "edge_type": edge.edge_type,
                "src": edge.src,
                "dst": edge.dst,
            },
            source_revision={"run_id": run_id},
            run_id=run_id,
            observed_at=observed_at,
        )
