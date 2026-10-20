"""BEAVER OKG enrichers: derive the join graph + domain-knowledge the schema lacks.

`BeaverJoinEnricher` (task.okg.join-enrich): infers join keys from instance
overlap (join_infer) and emits derived `column -[references]-> column` edges, so
the OKG has a join graph even where FK constraints are absent (dw has zero).

`BeaverDomainEnricher` (task.okg.domain-enrich): materializes value->column
domain facts for low-cardinality columns (the categorical vocabulary a query
must map NL phrases onto, e.g. DEPARTMENT_NAME = 'Electrical Eng & Computer
Sci'), as `column -[contains]-> value` domain nodes/edges.

Both read the live `column` nodes and query the loaded MySQL for instance data.
"""
from __future__ import annotations

from dataclasses import dataclass

import psycopg

from okg.substrate.enrichers.base import EnrichResult
from okg.substrate.enrichers.derived_edges import (
    DerivedEdgeCandidate,
    insert_deterministic_edges,
)

from beaver_okg.join_infer import infer_joins
from beaver_okg.source import DATABASES, NAMESPACE, beaver_mysql_connect


@dataclass
class BeaverJoinEnricher:
    """Instance-overlap join keys -> derived `references` edges."""

    namespace: str = NAMESPACE
    databases: tuple[str, ...] = tuple(DATABASES)
    min_containment: float = 0.85
    name: str = "beaver_join_infer"
    reads_subtypes: tuple[str, ...] = ("column",)
    emits_edge_types: tuple[str, ...] = ("references",)
    requires_narrowings: tuple[tuple[str, str, str], ...] = (("column", "references", "column"),)

    def _cid(self, db_table_col: str) -> str:
        return f"{self.namespace}:column:{db_table_col}"

    def enrich(self, conn: psycopg.Connection, *, generation_id: int, incremental=None) -> EnrichResult:
        cands = infer_joins(beaver_mysql_connect, list(self.databases), min_containment=self.min_containment)
        edges = [
            DerivedEdgeCandidate(
                src=self._cid(jc.a),
                edge_type="references",
                dst=self._cid(jc.b),
                attrs={"via": "instance_overlap", "containment": jc.containment, "shared": jc.shared},
                provenance="derived_deterministic",
                confidence=float(jc.containment),
            )
            for jc in cands
        ]
        cur = conn.cursor()
        res = insert_deterministic_edges(cur, source=self.name, candidates=edges)
        return EnrichResult(enricher_name=self.name, n_edges_emitted=res.inserted, n_edges_skipped=res.skipped)
