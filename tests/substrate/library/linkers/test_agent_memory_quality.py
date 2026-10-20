from __future__ import annotations

import psycopg
import pytest


pytestmark = pytest.mark.postgres


def test_agent_memory_quality_linker_supersedes_older_claim(
    clean_runtime: str,
) -> None:
    from okg.substrate.library.linkers.agent_memory_quality import (
        AgentMemoryQualityLinker,
    )
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    "claim:demo:old",
                    "agent_claim",
                    {
                        "scope": "demo",
                        "subject_id": "file:demo:src/app.py",
                        "claim_kind": "implementation_fact",
                        "observed_at": "2026-05-03T10:00:00Z",
                    },
                    {},
                    {},
                ),
                NodeFact(
                    "claim:demo:new",
                    "agent_claim",
                    {
                        "scope": "demo",
                        "subject_id": "file:demo:src/app.py",
                        "claim_kind": "implementation_fact",
                        "observed_at": "2026-05-03T12:00:00Z",
                    },
                    {},
                    {},
                ),
            ],
            source="fixture.agent-memory",
        )
        result = AgentMemoryQualityLinker().enrich(conn, generation_id=1)
        diagnostics = AgentMemoryQualityLinker().diagnostics(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT src, edge_type, dst
                  FROM okg.edge_facts
                 WHERE source = 'agent_memory_quality'
                 ORDER BY src, edge_type, dst
                """
            )
            edges = cur.fetchall()

    assert result.n_edges_emitted == 1
    assert diagnostics.eligible_claims == 2
    assert diagnostics.supersedure_edges == 1
    assert edges == [("claim:demo:new", "supersedes", "claim:demo:old")]
