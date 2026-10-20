"""AgentMemoryQualityLinker — deterministic claim lifecycle edges."""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import psycopg

from okg.substrate.enrichers.base import EnrichResult, IncrementalContext
from okg.substrate.library.linkers import _chronos


__all__ = ["AgentMemoryQualityDiagnostics", "AgentMemoryQualityLinker"]


@dataclass(frozen=True)
class _Claim:
    node_id: str
    attrs: dict[str, Any]


@dataclass(frozen=True)
class AgentMemoryQualityDiagnostics:
    eligible_claims: int
    supersedure_edges: int

    def as_dict(self) -> dict[str, int]:
        return {
            "eligible_claims": self.eligible_claims,
            "supersedure_edges": self.supersedure_edges,
        }


class AgentMemoryQualityLinker:
    """Derive lifecycle edges for claims with the same subject and kind."""

    name = "agent_memory_quality"
    publisher_skip_when_clean = True
    reads_subtypes = ("agent_claim",)
    emits_edge_types = ("supersedes",)
    requires_narrowings = (
        ("agent_claim", "supersedes", "agent_claim"),
    )

    def enrich(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        incremental: IncrementalContext | None = None,
    ) -> EnrichResult:
        del incremental
        with conn.cursor() as cur:
            edges = self._resolve_supersedure_edges(cur, skip_existing=True)
            emitted, skipped = self._emit_edges(
                cur,
                edges=edges,
                generation_id=generation_id,
            )
        return EnrichResult(
            enricher_name=self.name,
            n_edges_emitted=emitted,
            n_edges_skipped=skipped,
        )

    def diagnostics(
        self,
        conn: psycopg.Connection,
    ) -> AgentMemoryQualityDiagnostics:
        with conn.cursor() as cur:
            claims = _fetch_claims(cur)
            edges = self._resolve_supersedure_edges(cur, skip_existing=False)
        return AgentMemoryQualityDiagnostics(
            eligible_claims=len(claims),
            supersedure_edges=len(edges),
        )

    def _resolve_supersedure_edges(
        self,
        cur,
        *,
        skip_existing: bool,
    ) -> list[tuple[str, str, str, dict[str, Any]]]:
        claims = _fetch_claims(cur)
        groups: dict[tuple[str, str, str], list[_Claim]] = {}
        for claim in claims:
            key = (
                str(claim.attrs.get("scope") or ""),
                str(claim.attrs.get("subject_id") or ""),
                str(claim.attrs.get("claim_kind") or ""),
            )
            if not all(key):
                continue
            groups.setdefault(key, []).append(claim)

        edges: list[tuple[str, str, str, dict[str, Any]]] = []
        for key, grouped in groups.items():
            if len(grouped) < 2:
                continue
            ordered = sorted(
                grouped,
                key=lambda c: (
                    str(c.attrs.get("observed_at") or ""),
                    c.node_id,
                ),
            )
            latest = ordered[-1]
            for older in ordered[:-1]:
                edges.append((
                    latest.node_id,
                    "supersedes",
                    older.node_id,
                    {
                        "resolution": "same_scope_subject_claim_kind",
                        "scope": key[0],
                        "subject_id": key[1],
                        "claim_kind": key[2],
                        "linker": self.name,
                    },
                ))
        if skip_existing:
            edges = _filter_new_edges(cur, edges)
        return edges

    def _emit_edges(
        self,
        cur,
        *,
        edges: list[tuple[str, str, str, dict[str, Any]]],
        generation_id: int,
    ) -> tuple[int, int]:
        if not edges:
            return 0, 0
        observed_at = datetime.now(timezone.utc)
        run_id = str(uuid.uuid4())
        rows: list[tuple[Any, ...]] = []
        seen: set[str] = set()
        for src, edge_type, dst, attrs in sorted(
            edges,
            key=lambda item: (item[0], item[1], item[2]),
        ):
            dedupe_key = uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"{src}|{edge_type}|{dst}|{generation_id}",
            ).hex
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            rows.append((
                _mint_edge_id(src, edge_type, dst),
                src,
                dst,
                edge_type,
                json.dumps(attrs),
                json.dumps({
                    "src_node_id": src,
                    "dst_node_id": dst,
                    "linker": self.name,
                }),
                json.dumps({"generation_id": generation_id}),
                observed_at,
                dedupe_key,
                run_id,
            ))
        cur.execute(
            "SELECT DISTINCT dedupe_key FROM okg.edge_facts "
            "WHERE dedupe_key = ANY(%s)",
            ([row[8] for row in rows],),
        )
        already = {row[0] for row in cur.fetchall()}
        to_insert = [row for row in rows if row[8] not in already]
        for row in to_insert:
            cur.execute(
                """
                INSERT INTO okg.edge_facts
                  (edge_id, src, dst, edge_type, attrs,
                   provenance, confidence, source,
                   source_record_id, source_revision,
                   observed_at, dedupe_key, run_id, op)
                VALUES (%s, %s, %s, %s, %s::jsonb,
                        'derived_deterministic', 1.0,
                        %s,
                        %s::jsonb, %s::jsonb,
                        %s, %s, %s, 'I')
                """,
                (
                    row[0],
                    row[1],
                    row[2],
                    row[3],
                    row[4],
                    self.name,
                    row[5],
                    row[6],
                    row[7],
                    row[8],
                    row[9],
                ),
            )
        return len(to_insert), len(rows) - len(to_insert)


def _fetch_claims(cur) -> list[_Claim]:
    rows = _chronos.nodes(cur, ("agent_claim",))
    return [
        _Claim(str(row["node_id"]), dict(row["attrs"] or {}))
        for row in rows
    ]


def _filter_new_edges(
    cur,
    edges: list[tuple[str, str, str, dict[str, Any]]],
) -> list[tuple[str, str, str, dict[str, Any]]]:
    if not edges:
        return []
    rows = _chronos.query(
        cur,
        """
        SELECT src, edge_type, dst
          FROM okg.graph_edges
         WHERE edge_id = ANY(:edge_ids)
        """,
        {
            "edge_ids": [
                _mint_edge_id(src, edge_type, dst)
                for src, edge_type, dst, _attrs in edges
            ]
        },
    )
    existing = {
        (str(row["src"]), str(row["edge_type"]), str(row["dst"]))
        for row in rows
    }
    return [
        edge for edge in edges
        if (edge[0], edge[1], edge[2]) not in existing
    ]


def _mint_edge_id(src: str, edge_type: str, dst: str) -> int:
    h = uuid.uuid5(uuid.NAMESPACE_URL, f"{src}|{edge_type}|{dst}").int
    return h & ((1 << 63) - 1)
