"""Regression tests for the connector ingest fact-row builder.

`connector_ingest._connector_fact_rows` writes rows that are COPYed into
`okg.edge_facts`, whose `edge_id` is BIGINT and whose `provenance` has a
CHECK constraint. A regression here (minting a uuid `.hex` string into
the BIGINT column, or `provenance="connector"` which is not an allowed
value) breaks every connector ingest that emits an edge — and is not
caught by the dry-run tests, which never reach the COPY.
"""
from datetime import datetime, timezone


def test_connector_fact_rows_edge_id_is_bigint_safe() -> None:
    from okg.substrate.ingest.runner import _mint_edge_id
    from okg.substrate.mcp.connector_ingest import _connector_fact_rows

    _, edge_rows = _connector_fact_rows(
        source_name="demo.apple_files_connector",
        nodes=[],
        edges=[{
            "src": "doc:apple-files:abc",
            "edge_type": "contains",
            "dst": "chunk:abc#1",
            "attrs": {},
            "source_record_id": {"path": "/a.md"},
            "source_revision": {"mtime": 1},
        }],
        run_id="00000000-0000-0000-0000-000000000000",
        observed_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    assert len(edge_rows) == 1
    # Column order matches the COPY okg.edge_facts (...) list:
    # edge_id, src, dst, edge_type, attrs, provenance, confidence,
    # source, source_record_id, source_revision, observed_at,
    # dedupe_key, run_id, op.
    (edge_id, _src, _dst, _etype, _attrs, provenance, confidence,
     *_rest, op) = edge_rows[0]

    assert isinstance(edge_id, int), f"edge_id must be int, got {type(edge_id)}"
    assert 0 <= edge_id < (1 << 63), f"edge_id out of BIGINT range: {edge_id}"
    # Must agree with the canonical mint for the same (src, type, dst).
    assert edge_id == _mint_edge_id("doc:apple-files:abc", "contains", "chunk:abc#1")

    assert provenance in {
        "explicit", "derived_deterministic", "linked_heuristic",
        "extracted_model", "derived_semantic",
    }, f"provenance {provenance!r} violates edge_facts CHECK"
    assert op in {"I", "R"}, f"op {op!r} violates edge_facts CHECK"
    assert 0.0 <= float(confidence) <= 1.0
