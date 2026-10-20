"""Executed-test half of task.ontology-annotations.

Covers req.document-source-file-indexed from
pact/changes/index-document-source-file-search/pact.yaml (issue #900):
`document` and `source_file` nodes are findable by a word taken from
their path.

Before this change `source_file` declared no searchable fields at all (so
its rows had no `attrs.text` and were excluded from the BM25 partial
index), and `document` declared only `title` — filled with the
repo-relative path, which the index's `english` config lexes as ONE
token. Both returned 0 hits for every query.
"""
from __future__ import annotations

import json
import uuid
from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.catalog.modules_compose import _split_subtypes_field
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp import open_session, search
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


ONTOLOGIES = (
    Path(__file__).resolve().parents[4]
    / "src" / "okg" / "substrate" / "library" / "ontologies"
)

DOC_PATH = "refs/sample_notes/AN-23-183/07-Background_Estimation.tex"
SRC_PATH = "src/okg/substrate/projection/projector.py"


def _annotations(module: str, class_name: str) -> dict:
    schema = yaml.safe_load(
        (ONTOLOGIES / module / "schema.yaml").read_text()
    )
    return schema["classes"][class_name].get("annotations", {}) or {}


def test_annotations_declared() -> None:
    """Both subtypes declare searchable fields, and the path-valued ones
    are declared as path fields so their component words are indexed."""
    doc = _annotations("extraction", "Document")
    src = _annotations("git_graph", "SourceFile")

    doc_fields = doc["okg_searchable_text_fields"].split(",")
    doc_path_fields = doc["okg_searchable_text_path_fields"].split(",")
    assert "title" in doc_fields and "path" in doc_fields
    assert "title" in doc_path_fields and "path" in doc_path_fields

    src_fields = src["okg_searchable_text_fields"].split(",")
    src_path_fields = src["okg_searchable_text_path_fields"].split(",")
    assert "path" in src_fields
    assert "path" in src_path_fields
    # `language` would put the token `python` on every Python file, with no
    # discriminative weight on a rank-capped page.
    assert "language" not in src_fields

    # Every declared path field must also be a plain searchable field —
    # the path list only ADDS the split form, it does not itself
    # contribute the raw value.
    assert set(doc_path_fields) <= set(doc_fields)
    assert set(src_path_fields) <= set(src_fields)


def _seed_catalog_from_shipped_annotations(conn: psycopg.Connection) -> None:
    """Install `document` + `source_file` catalog metadata derived from the
    SHIPPED schema.yaml annotations.

    `clean_runtime` migrates but does not load the ontology library, so
    `okg.subtypes` is empty. Rather than hardcode the field lists (which
    would let the test pass while the shipped annotation says something
    else), read the annotations off disk and put them through the same
    `_split_subtypes_field` parser `modules_compose` uses. The test then
    proves the shipped declaration produces findable nodes.
    """
    rows = []
    for module, class_name, subtype in (
        ("extraction", "Document", "document"),
        ("git_graph", "SourceFile", "source_file"),
    ):
        annotations = _annotations(module, class_name)
        metadata = {
            "searchable_text_fields": _split_subtypes_field(
                annotations.get("okg_searchable_text_fields"),
            ),
            "searchable_text_path_fields": _split_subtypes_field(
                annotations.get("okg_searchable_text_path_fields"),
            ),
        }
        rows.append((subtype, json.dumps(metadata)))

    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        cv = cur.fetchone()[0]
        for subtype, metadata_json in rows:
            cur.execute(
                "INSERT INTO okg.subtypes "
                "  (name, metadata, catalog_version_id) "
                "VALUES (%s, %s::jsonb, %s)",
                (subtype, metadata_json, cv),
            )


def _seed(conn: psycopg.Connection, subtype: str, attrs: dict) -> str:
    node_id = f"n:{subtype}:{uuid.uuid4().hex[:8]}"
    ChronosGraphStore.from_connection(conn).apply_facts(
        [
            NodeFact(
                node_id=node_id,
                subtype=subtype,
                attrs=attrs,
                source_record_id={"id": node_id},
                source_revision={"v": 1},
            )
        ],
        source="fixture",
    )
    return node_id


@pytest.mark.postgres
def test_lexical_search_finds_document_and_source_file(
    clean_runtime: str,
) -> None:
    """scn.document-source-file-indexed.{document,source-file}: an
    interior path word returns the node, filtered to each subtype."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_from_shipped_annotations(conn)
        doc_id = _seed(conn, "document", {
            "doc_id": "doc:fixture:1",
            "repo": "fixture",
            "path": DOC_PATH,
            "title": DOC_PATH,
            "format": "latex",
        })
        src_id = _seed(conn, "source_file", {
            "repo": "fixture",
            "path": SRC_PATH,
            "language": "python",
            "is_text": True,
        })

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")

        found = search(
            conn, session, "Background",
            method="lexical", filter={"subtype": "document"}, limit=10,
        )
        assert found["results"], found
        assert doc_id in {row["node_id"] for row in found["results"]}
        # A hit is its own explanation; the zero-hit diagnostic is absent.
        assert "zero_hit_reason" not in found

        found = search(
            conn, session, "projector",
            method="lexical", filter={"subtype": "source_file"}, limit=10,
        )
        assert found["results"], found
        assert src_id in {row["node_id"] for row in found["results"]}
        # A hit is its own explanation; the zero-hit diagnostic is absent.
        assert "zero_hit_reason" not in found


@pytest.mark.postgres
def test_pre_change_declaration_finds_nothing(clean_runtime: str) -> None:
    """Negative control — the test above must not be vacuous.

    Seed `document` with the PRE-change declaration (`title` alone, no
    path fields) and the same node. The search that now succeeds returns
    nothing, and the response names the reason.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.catalog_versions (status, applied_at) "
                "VALUES ('applied', now()) RETURNING catalog_version_id"
            )
            cv = cur.fetchone()[0]
            cur.execute(
                "INSERT INTO okg.subtypes "
                "  (name, metadata, catalog_version_id) "
                "VALUES ('document', %s::jsonb, %s)",
                (json.dumps({"searchable_text_fields": ["title"]}), cv),
            )
        _seed(conn, "document", {
            "doc_id": "doc:fixture:2",
            "repo": "fixture",
            "path": DOC_PATH,
            "title": DOC_PATH,
            "format": "latex",
        })

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        found = search(
            conn, session, "Background",
            method="lexical", completeness="exact", filter={"subtype": "document"}, limit=10,
        )

    assert found["total_count"] == 0, found
    # And the read side now says WHY, instead of an anonymous zero.
    assert found["zero_hit_reason"] == "not_tokenized", found
