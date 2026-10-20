"""DefaultProjectMembershipEnricher — back-fill OpenSpec → Project edges.

Per `cleanup-personal-okg-data-quality` C1 item 3:

Only 1 of 171 OpenSpec changes had a `member_of Project` edge at
contract closure — the one whose frontmatter declared
`project: personal-okg`. The other 170 were unattached because
the openspec source only emits the edge when the change file
declares `project:` in frontmatter.

This enricher back-fills the gap: for any `OpenSpecChange` node
lacking an outgoing `member_of` edge to any `Project`, emit one
to the deployment's `default_project: <slug>` config attribute.

Operator override:
- A change with `project: <other-slug>` in frontmatter gets its
  edge from the openspec source's primary path (not via this
  enricher).
- A change with `project: none` in frontmatter is the sentinel
  that suppresses back-fill — the openspec source skips emission,
  this enricher also skips, and the change has no Project membership.

Enabled by listing in a deployment's `enrichers:` block:

    enrichers:
      - class: okg.substrate.library.enrichers.openspec.default_project.DefaultProjectMembershipEnricher
        params:
          default_project: personal-okg
"""

from __future__ import annotations

import hashlib
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import psycopg

logger = logging.getLogger(__name__)


@dataclass
class EnrichResult:
    """Substrate enricher result shape. Field names match the
    substrate runner's expected attributes (n_edges_emitted +
    n_edges_skipped, NOT n_skipped — verified by running an enricher
    and observing the AttributeError on the latter)."""
    enricher_name: str
    n_edges_emitted: int = 0
    n_edges_skipped: int = 0


class DefaultProjectMembershipEnricher:
    """Substrate-shipped enricher: back-fill OpenSpec → default Project.

    Constructor takes a required `default_project: <slug>` from
    deployment config. The enricher emits
    `OpenSpecChange member_of Project` edges with
    `provenance: derived_deterministic`.
    """

    name: str = "openspec_default_project_membership"
    emits_edge_types: tuple[str, ...] = ("member_of",)
    reads_subtypes: tuple[str, ...] = ("open_spec_change",)
    requires_narrowings: tuple[tuple[str, str, str], ...] = (
        ("open_spec_change", "member_of", "project"),
    )

    def __init__(
        self,
        *,
        default_project: str,
    ) -> None:
        if not default_project or not isinstance(default_project, str):
            raise ValueError(
                "DefaultProjectMembershipEnricher: `default_project` must "
                "be a non-empty string (the canonical project slug)."
            )
        self.default_project = default_project.strip()
        self.default_project_node_id = (
            self.default_project
            if self.default_project.startswith("proj:")
            else f"proj:{self.default_project}"
        )

    def enrich(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        **_publisher_kwargs: object,
    ) -> EnrichResult:
        run_id = str(uuid.uuid4())
        observed_at = datetime.now(timezone.utc)

        # Find OpenSpecChange nodes lacking any outgoing `member_of`
        # edge to a Project node. The set is computed at the pinned
        # generation.
        from okg.substrate.graph_store import ChronosGraphStore

        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout_generation(generation_id)
        rows = [
            (row["node_id"], row["attrs"])
            for row in graph.query(
                """
                SELECT c.node_id, c.attrs
                  FROM okg.graph_nodes c
                 WHERE c.subtype = 'open_spec_change'
                   AND NOT EXISTS (
                       SELECT 1 FROM okg.graph_edges e
                       JOIN okg.graph_nodes p ON p.node_id = e.dst
                        WHERE e.src = c.node_id
                          AND e.edge_type = 'member_of'
                          AND p.subtype = 'project'
                   )
                """
            )
        ]

        if not rows:
            logger.info(
                "%s: zero OpenSpecChange rows need back-fill at generation %d",
                self.name, generation_id,
            )
            return EnrichResult(enricher_name=self.name, n_edges_emitted=0)

        # Verify the default Project node exists at this generation.
        # If not, fail loud rather than emit dangling edges (per the
        # substrate's no-silent-fallback discipline).
        project_exists = graph.query(
            """
            SELECT 1 AS present FROM okg.graph_nodes
             WHERE node_id = :p
               AND subtype = 'project'
            """,
            {"p": self.default_project_node_id},
        )
        if not project_exists:
            logger.warning(
                "%s: default project %s not present at generation %d; "
                "skipping back-fill",
                self.name, self.default_project_node_id, generation_id,
            )
            return EnrichResult(
                enricher_name=self.name,
                n_edges_emitted=0,
                n_edges_skipped=len(rows),
            )

        # Migrated to the substrate-level helper (see
        # `okg.substrate.library.enrichers._fact_emitters`) so the
        # projector INSERT pattern lives in one place.
        from okg.substrate.library.enrichers._fact_emitters import (
            emit_edge_fact,
        )
        n_emitted = 0
        n_skipped = 0
        for node_id, attrs in rows:
            # Respect the `project: none` sentinel.
            attrs_dict = attrs if isinstance(attrs, dict) else {}
            if (attrs_dict.get("project") or "").strip() == "none":
                n_skipped += 1
                continue

            dedupe_payload = (
                f"{self.name}:{node_id}->{self.default_project_node_id}"
            ).encode("utf-8")
            dedupe_key = hashlib.sha256(dedupe_payload).hexdigest()
            emit_edge_fact(
                conn,
                enricher_name=self.name,
                src=node_id,
                dst=self.default_project_node_id,
                edge_type="member_of",
                attrs={
                    "back_filled_by": self.name,
                    "default_project_slug": self.default_project,
                    "enricher_run_id": run_id,
                },
                provenance="derived_deterministic",
                confidence=1.0,
                dedupe_key=dedupe_key,
                source_record_id={
                    "enricher": self.name,
                    "change_node_id": node_id,
                },
                source_revision={
                    "default_project": self.default_project,
                    "run_id": run_id,
                },
                run_id=run_id,
                observed_at=observed_at,
            )
            n_emitted += 1

        logger.info(
            "%s: back-filled %d OpenSpecChange member_of Project edges "
            "(skipped %d 'project: none' sentinels)",
            self.name, n_emitted, n_skipped,
        )
        return EnrichResult(
            enricher_name=self.name,
            n_edges_emitted=n_emitted,
            n_edges_skipped=n_skipped,
        )
