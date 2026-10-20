"""route-all-writers-through-transform-ir (task 2) -- fact-writers witness their writes.

Two guarantees for req.raw.fact-writers-witnessed:

1. The source runner stamps source_program_checksum = the ingestion program's
   checksum (explicitly, in the COPY column list) on every node_facts/edge_facts
   row it emits, and that checksum resolves in okg.ir_programs.

2. The GUC-default mechanism that covers the dozen-plus inline enricher/linker
   INSERT sites and the admission path: a writer that sets the per-tx GUC
   okg.source_program_checksum once and then issues a fact INSERT *omitting* the
   column gets the row auto-stamped with the GUC value -- which is how the
   enrichment / admission programs witness their facts without per-site SQL surgery.
"""

from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

from okg.substrate.ingest import SourceRunner  # noqa: E402
from okg.substrate.library.sources.docsite import DocsiteSource  # noqa: E402
from okg.substrate.library.sources.jira import JiraSource  # noqa: E402
from okg.substrate.runtime.admission_workflows import _ADMISSION_CHECKSUM  # noqa: E402
from okg.substrate.transform.programs import (  # noqa: E402
    enrichment_program,
    ingestion_program,
)

pytestmark = pytest.mark.postgres

_INGESTION_CHECKSUM = ingestion_program().checksum()
_ENRICHMENT_CHECKSUM = enrichment_program().checksum()


def _write_fixture(tmp_path: Path, doc: dict) -> Path:
    p = tmp_path / "fixture.json"
    p.write_text(json.dumps(doc))
    return p


def _unwitnessed(cur, table: str, source: str) -> int:
    """Count rows in `table` for `source` whose witness does NOT resolve in
    okg.ir_programs (the commit-witness antijoin)."""
    cur.execute(
        f"SELECT count(*) FROM okg.{table} t "
        "LEFT JOIN okg.ir_programs p "
        "  ON p.program_checksum = t.source_program_checksum "
        "WHERE t.source = %s AND p.program_id IS NULL",
        (source,),
    )
    return int(cur.fetchone()[0])


def test_source_runner_facts_carry_resolvable_ingestion_witness(
    clean_runtime: str, tmp_path: Path,
) -> None:
    src = f"wf-docs-{uuid.uuid4().hex[:8]}"
    fx = _write_fixture(tmp_path, {
        "nodes": [
            {"node_id": "d:1", "subtype": "document", "attrs": {"text": "a"}},
            {"node_id": "d:2", "subtype": "document", "attrs": {"text": "b"}},
        ],
        "edges": [],
    })
    # Post-chronos-cutover the canonical write is the graph; the okg.node_facts
    # audit ledger is opt-in (write_fact_log, default False). This test asserts
    # the ledger's ingestion witness, so it writes the fact log explicitly.
    result = SourceRunner(
        clean_runtime, source_name=src, write_fact_log=True,
    ).run(
        DocsiteSource(fixture_path=str(fx)), mode="scope_complete",
    )
    assert result.nodes_appended == 2

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.node_facts "
            "WHERE source = %s AND source_program_checksum = %s",
            (src, _INGESTION_CHECKSUM),
        )
        stamped = int(cur.fetchone()[0])
        unwitnessed = _unwitnessed(cur, "node_facts", src)

    assert stamped == 2          # both node_facts carry the ingestion witness
    assert unwitnessed == 0      # and it resolves in okg.ir_programs


def test_source_runner_edge_facts_carry_resolvable_ingestion_witness(
    clean_runtime: str, tmp_path: Path,
) -> None:
    src = f"wf-jira-{uuid.uuid4().hex[:8]}"
    fx = _write_fixture(tmp_path, {
        "nodes": [
            {"node_id": "t:1", "subtype": "ticket", "attrs": {"text": "bug"}},
            {"node_id": "p:a", "subtype": "person", "attrs": {"name": "a"}},
        ],
        "edges": [{"src": "t:1", "dst": "p:a", "edge_type": "reported_by"}],
    })
    # The okg.edge_facts audit ledger is opt-in post-cutover (write_fact_log);
    # this test asserts its ingestion witness, so write the fact log explicitly.
    result = SourceRunner(
        clean_runtime, source_name=src, write_fact_log=True,
    ).run(
        JiraSource(fixture_path=str(fx)), mode="cursor",
    )
    assert result.edges_appended == 1

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM okg.edge_facts "
            "WHERE source = %s AND source_program_checksum = %s",
            (src, _INGESTION_CHECKSUM),
        )
        stamped = int(cur.fetchone()[0])
        unwitnessed = _unwitnessed(cur, "edge_facts", src)

    assert stamped == 1
    assert unwitnessed == 0


def test_guc_default_stamps_facts_that_omit_the_column(clean_runtime: str) -> None:
    """The mechanism that covers enricher/linker/admission INSERTs: set the per-tx
    GUC, INSERT a fact WITHOUT naming source_program_checksum, and the column
    default (current_setting) stamps it. This is exactly what the publisher's
    enrichment stage and the admission path do."""
    src = f"wf-guc-{uuid.uuid4().hex[:8]}"
    # Persist the enrichment program so the stamped checksum resolves.
    from okg.substrate.transform.commit import persist_transform_program

    with psycopg.connect(clean_runtime) as conn:  # NOT autocommit: one tx
        with conn.cursor() as cur:
            persist_transform_program(conn, enrichment_program())
            cur.execute(
                "SELECT set_config('okg.source_program_checksum', %s, true)",
                (_ENRICHMENT_CHECKSUM,),
            )
            # INSERT omitting source_program_checksum -> column default fires.
            cur.execute(
                "INSERT INTO okg.node_facts "
                "(node_id, subtype, attrs, source, source_record_id, "
                " source_revision, observed_at, dedupe_key, run_id, op) "
                "VALUES (%s,'concept','{}'::jsonb,%s,'{}'::jsonb,'{}'::jsonb,"
                " now(),%s,%s,'I')",
                (f"g:{uuid.uuid4().hex[:8]}", src, f"{src}:dk", str(uuid.uuid4())),
            )
        conn.commit()

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT source_program_checksum FROM okg.node_facts WHERE source = %s",
            (src,),
        )
        got = cur.fetchone()[0]
        unwitnessed = _unwitnessed(cur, "node_facts", src)

    assert got == _ENRICHMENT_CHECKSUM   # GUC default stamped the omitted column
    assert unwitnessed == 0              # and it resolves in okg.ir_programs


def test_real_enricher_insert_helper_auto_stamps_under_the_guc(
    clean_runtime: str,
) -> None:
    """The production claim: the dozen-plus inline enricher/linker INSERTs were NOT
    edited. Instead the publisher's enrichment stage sets the GUC once and the
    column default stamps them. Prove it with the *real, unmodified* derived-edge
    INSERT helper (insert_deterministic_edges) run under the GUC the publisher sets."""
    from okg.substrate.enrichers.derived_edges import (
        DerivedEdgeCandidate,
        insert_deterministic_edges,
    )
    from okg.substrate.transform.commit import persist_transform_program

    src = f"_enricher:wf-{uuid.uuid4().hex[:8]}"
    with psycopg.connect(clean_runtime) as conn:  # one tx so set_config(local) holds
        with conn.cursor() as cur:
            persist_transform_program(conn, enrichment_program())
            cur.execute(
                "SELECT set_config('okg.source_program_checksum', %s, true)",
                (_ENRICHMENT_CHECKSUM,),
            )
            result = insert_deterministic_edges(
                cur,
                source=src,
                candidates=[DerivedEdgeCandidate(
                    src=f"n:{uuid.uuid4().hex[:6]}",
                    edge_type="references",
                    dst=f"n:{uuid.uuid4().hex[:6]}",
                    attrs={"reason": "wf"},
                )],
            )
            assert result.inserted == 1
        conn.commit()

    with psycopg.connect(clean_runtime, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT source_program_checksum FROM okg.edge_facts WHERE source = %s",
            (src,),
        )
        got = cur.fetchone()[0]
        unwitnessed = _unwitnessed(cur, "edge_facts", src)

    assert got == _ENRICHMENT_CHECKSUM   # unmodified helper auto-stamped via the default
    assert unwitnessed == 0


def test_admission_program_checksum_is_distinct_and_stable() -> None:
    """The admission path uses its own program witness (distinct from ingestion /
    enrichment), so admitted facts trace to the admission producer."""
    assert _ADMISSION_CHECKSUM
    assert _ADMISSION_CHECKSUM != _INGESTION_CHECKSUM
    assert _ADMISSION_CHECKSUM != _ENRICHMENT_CHECKSUM
