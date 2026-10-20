"""Per `refine-doc-corpus-extraction` (2026-04-26) W8:
`Document.parse_warnings` is emitted as a JSON array; the
projector's `okg._stamp_searchable_text` skips array values
when auto-stamping `attrs.text`, so warning labels stay out
of the BM25 index.

A document whose only `oversize` mention is in
`parse_warnings: ["oversize"]` SHALL NOT hit
`MCP search(query="oversize", method="lexical")`.
"""
from __future__ import annotations

import json
from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.doc_corpus import DocCorpusSource
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _seed_cv_with_searchable_text(
    conn: psycopg.Connection, *, subtype: str, fields: list[str],
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO okg.subtypes "
            "  (name, metadata, catalog_version_id) "
            "VALUES (%s, %s::jsonb, %s)",
            (
                subtype,
                json.dumps({"searchable_text_fields": fields}),
                cv,
            ),
        )
    return cv


def test_parse_warnings_emits_as_json_array(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """Adapter emit shape: parse_warnings is a list, not a
    comma-joined string. Asserted via the durable log directly
    so we can read the jsonb type."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_cv_with_searchable_text(
            conn, subtype="document", fields=["title"],
        )

    corpus = tmp_path / "c"
    corpus.mkdir()
    # Write an oversized file so DocCorpusSource emits
    # `parse_warnings: ["oversize"]`.
    (corpus / "huge.txt").write_text("x" * 4096)

    extr = tmp_path / "ext"
    extr.mkdir()
    (extr / "extractors.yaml").write_text(yaml.safe_dump({
        "parser": [{
            "format": "text",
            "class": (
                "okg.substrate.library.extraction.parse."
                "text.TextParser"
            ),
        }],
    }))

    SourceRunner(
        clean_runtime, source_name="test.warnings.array",
    ).run(
        DocCorpusSource(
            repo="t", repo_root=corpus, non_git_walk=True, extractors_dir=extr,
            include_globs=("**/*.txt",),
            max_bytes_per_file=1024,
        ),
        mode="scope_complete",
    )

    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        rows = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).query(
            "SELECT attrs->'parse_warnings' AS warnings_jsonb, "
            "       jsonb_typeof(attrs->'parse_warnings') AS jsonb_kind "
            "  FROM okg.graph_nodes "
            " WHERE source = 'test.warnings.array' "
            "   AND subtype = 'document'"
        )
        row = rows[0]
        warnings_jsonb = row["warnings_jsonb"]
        jsonb_kind = row["jsonb_kind"]
    assert jsonb_kind == "array", jsonb_kind
    assert warnings_jsonb == ["oversize"]


def test_parse_warnings_does_not_pollute_text_field(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """Chronos graph publication's searchable-text auto-stamp reads
    `searchable_text_fields` from subtype metadata. Even if a
    deployment misconfigures `parse_warnings` as a searchable
    field, non-string values (arrays included) are skipped, so the
    warning string never lands in attrs.text."""
    from okg.substrate.publisher import Publisher

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # Declare BOTH `title` AND `parse_warnings` as
        # searchable_text_fields. Title contributes its string
        # value; parse_warnings (array) is skipped by the
        # SQL stamper.
        _seed_cv_with_searchable_text(
            conn, subtype="document",
            fields=["title", "parse_warnings"],
        )

    corpus = tmp_path / "c2"
    corpus.mkdir()
    (corpus / "huge.txt").write_text("x" * 4096)

    extr = tmp_path / "ext2"
    extr.mkdir()
    (extr / "extractors.yaml").write_text(yaml.safe_dump({
        "parser": [{
            "format": "text",
            "class": (
                "okg.substrate.library.extraction.parse."
                "text.TextParser"
            ),
        }],
    }))

    SourceRunner(
        clean_runtime, source_name="test.warnings.text",
    ).run(
        DocCorpusSource(
            repo="t", repo_root=corpus, non_git_walk=True, extractors_dir=extr,
            include_globs=("**/*.txt",),
            max_bytes_per_file=1024,
        ),
        mode="scope_complete",
    )
    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out

    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        graph = store.checkout_generation(out["generation_id"])
        rows = graph.query(
            """
            SELECT attrs->>'text' AS text
              FROM okg.graph_nodes
             WHERE subtype = 'document'
               AND attrs->>'path' = 'huge.txt'
            """
        )
    finally:
        store.close()
    assert rows
    text = rows[0]["text"]
    # `oversize` came from parse_warnings (an array); it must
    # NOT appear in the auto-stamped attrs.text. The title
    # field DOES contribute (it's a string).
    assert text is None or "oversize" not in text, text
    assert text and "huge.txt" in text, text
