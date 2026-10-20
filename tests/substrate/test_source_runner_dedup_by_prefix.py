"""Per `refine-doc-corpus-extraction` (2026-04-26) W7:
`RunResult.dedupe_drops_by_prefix` breaks down the existing
`dedupe_drops_node` / `dedupe_drops_edge` totals by node-id
prefix or `edge:<edge_type>`.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    SourceRun,
)


pytestmark = pytest.mark.postgres


class _MixedDuplicatesAdapter:
    """Emits both doc and chunk node duplicates plus
    member_of edge duplicates so we can assert the prefix
    breakdown sums to the totals."""

    name = "test.mixed-dups"
    profile = "discovery_crawl"

    def run(self, run_id, *, mode="cursor"):
        facts: list = []
        # 5 unique doc nodes + 3 dups → 3 doc drops.
        for i in range(8):
            nid = f"doc:repo:file{min(i, 4)}.md"
            facts.append(NodeFact(
                node_id=nid, subtype="document",
                attrs={"text": f"v:{i}"},
                source_record_id={"id": nid},
                source_revision={"rev": run_id},
                op="I",
            ))
        # 5 unique chunk nodes + 7 dups → 7 chunk drops.
        for i in range(12):
            nid = f"chunk:{min(i, 4):016x}"
            facts.append(NodeFact(
                node_id=nid, subtype="document_chunk",
                attrs={"text": f"c:{i}"},
                source_record_id={"id": nid},
                source_revision={"rev": run_id},
                op="I",
            ))
        # 3 unique member_of edges + 2 dups → 2 edge drops.
        for i in range(5):
            facts.append(EdgeFact(
                src=f"chunk:{min(i, 2):016x}",
                dst=f"doc:repo:file{min(i, 2)}.md",
                edge_type="member_of",
                attrs={"k": i},
                source_record_id={"i": i},
                source_revision={"rev": run_id},
                op="I",
            ))
        return SourceRun(
            facts=facts, completed_scope=True,
            next_cursor=None, run_mode=mode,
        )


def _seed_cv(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now())"
        )


def test_dedupe_drops_by_prefix_sums_to_totals(
    clean_runtime: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)

    result = SourceRunner(
        clean_runtime, source_name=_MixedDuplicatesAdapter.name,
    ).run(_MixedDuplicatesAdapter(), mode="scope_complete")

    # Doc node drops: 8 emitted, 5 unique → 3 drops.
    # Chunk node drops: 12 emitted, 5 unique → 7 drops.
    # member_of edge drops: 5 emitted, 3 unique → 2 drops.
    assert result.dedupe_drops_node == 10
    assert result.dedupe_drops_edge == 2
    assert result.dedupe_drops_by_prefix.get("doc") == 3
    assert result.dedupe_drops_by_prefix.get("chunk") == 7
    assert result.dedupe_drops_by_prefix.get("edge:member_of") == 2

    # Sum of values equals total drops.
    total = sum(result.dedupe_drops_by_prefix.values())
    assert total == (
        result.dedupe_drops_node + result.dedupe_drops_edge
    )


def test_no_drops_yields_empty_breakdown(
    clean_runtime: str,
) -> None:
    """When the adapter emits no duplicates, the breakdown is
    an empty dict (not None)."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv(conn)

    class _Clean:
        name = "test.clean"
        profile = "discovery_crawl"

        def run(self, run_id, *, mode="cursor"):
            return SourceRun(
                facts=[NodeFact(
                    node_id="doc:repo:only", subtype="document",
                    attrs={"text": "x"},
                    source_record_id={"id": "x"},
                    source_revision={"rev": run_id},
                    op="I",
                )],
                completed_scope=True,
                next_cursor=None, run_mode=mode,
            )

    result = SourceRunner(
        clean_runtime, source_name="test.clean.no.dups",
    ).run(_Clean(), mode="scope_complete")
    assert result.dedupe_drops_node == 0
    assert result.dedupe_drops_edge == 0
    assert result.dedupe_drops_by_prefix == {}
