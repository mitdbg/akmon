"""Executed-test half of task.search-citations.contract-and-search.

Covers req.search-citations.contract from
pact/changes/source-supplied-search-citations/pact.yaml: search returns
source-supplied `citation: {label, url?}` on default and selected rows,
label-only citations for non-Git sources, never infers a citation from
ordinary attrs, and applies trust-safe serialization to citation labels
and URLs (label fenced, URL kept-verbatim-or-dropped, never fenced).
"""
from __future__ import annotations

import uuid

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp import open_session, search
from okg.substrate.mcp.provenance import (
    FENCE_CLOSE,
    FENCE_OPEN,
    serialize_node_read_row,
)
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _seed_catalog_version(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now())"
        )


# NOTE on seeded text: the chronos write path stamps searchable text from
# the subtype's searchable fields (`_stamp_searchable_text`); for
# `pact_change` a non-empty `title` REPLACES the seeded `text` attr. Nodes
# that carry a `title` must therefore put the lexical query terms in the
# title, not in `text`.
def _seed_node(conn: psycopg.Connection, attrs: dict) -> str:
    node_id = f"n:citation:{uuid.uuid4().hex[:8]}"
    ChronosGraphStore.from_connection(conn).apply_facts([
        NodeFact(
            node_id=node_id,
            subtype="pact_change",
            attrs=attrs,
            source_record_id={"id": node_id},
            source_revision={"v": 1},
        )
    ])
    return node_id


CITATION_LABEL = "okg/docs/reference/mcp.md"
CITATION_URL = "https://git.example.com/okg/blob/abc123def/docs/reference/mcp.md"


def test_scn_search_citations_contract_default(clean_runtime: str) -> None:
    """scn.search-citations.contract.default: a default lexical search row
    carries the source-supplied citation without selecting attrs, `citation`
    is explicitly selectable, and the same pinned generation returns the
    same citation on a second call."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        node_id = _seed_node(conn, {
            "text": "citrus retrieval contract page for citation defaults",
            "title": "Citrus retrieval contract interface",
            "citation_label": CITATION_LABEL,
            "citation_url": CITATION_URL,
        })
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        default = search(
            conn, session, "citrus retrieval contract",
            method="lexical", limit=5,
        )
        selected = search(
            conn, session, "citrus retrieval contract",
            method="lexical", limit=5, select=["citation"],
        )
        repeat = search(
            conn, session, "citrus retrieval contract",
            method="lexical", limit=5,
        )

    rows = [r for r in default["results"] if r["node_id"] == node_id]
    assert len(rows) == 1
    row = rows[0]
    # Default row: citation present WITHOUT selecting attrs.
    assert "attrs" not in row
    assert row["citation"] == {"label": CITATION_LABEL, "url": CITATION_URL}
    # Neither value requires parsing node_id or source-specific attrs.
    assert row["citation"]["label"] != row["node_id"]

    # `citation` is explicitly selectable.
    sel_rows = [r for r in selected["results"] if r["node_id"] == node_id]
    assert len(sel_rows) == 1
    assert sel_rows[0]["citation"] == {
        "label": CITATION_LABEL, "url": CITATION_URL,
    }
    assert "attrs" not in sel_rows[0]
    assert "excerpt" not in sel_rows[0]

    # The same pinned generation returns the same citation.
    assert repeat["pinned_generation_id"] == default["pinned_generation_id"]
    repeat_rows = [r for r in repeat["results"] if r["node_id"] == node_id]
    assert repeat_rows[0]["citation"] == row["citation"]


def test_scn_search_citations_contract_label_only(clean_runtime: str) -> None:
    """scn.search-citations.contract.label-only: a synthetic non-Git,
    database-like node with citation_label and no citation_url returns a
    citation with the supplied label and no fabricated URL. An invalid
    source-supplied URL also degrades to the same label-only result."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        node_id = _seed_node(conn, {
            "text": "orders retention ledger rows for the billing pipeline",
            "citation_label": "prod-db/orders table",
        })
        invalid_url_node_id = _seed_node(conn, {
            "text": "orders retention ledger rows with an invalid link",
            "citation_label": "prod-db/invalid-link table",
            "citation_url": "javascript:alert(1)",
        })
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        out = search(
            conn, session, "orders retention ledger",
            method="lexical", limit=5,
        )

    rows = [r for r in out["results"] if r["node_id"] == node_id]
    assert len(rows) == 1
    citation = rows[0]["citation"]
    assert citation == {"label": "prod-db/orders table"}
    assert "url" not in citation
    invalid_rows = [
        r for r in out["results"] if r["node_id"] == invalid_url_node_id
    ]
    assert len(invalid_rows) == 1
    assert invalid_rows[0]["citation"] == {
        "label": "prod-db/invalid-link table",
    }


def test_scn_search_citations_contract_no_guess(clean_runtime: str) -> None:
    """scn.search-citations.contract.no-guess: title, repo, path, and url
    attrs never become a citation by accident — the row carries no citation
    key, in the lexical row and after serialize_node_read_row."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed_catalog_version(conn)
        node_id = _seed_node(conn, {
            "text": "unattributed ordinary page about publish gates",
            "title": "Unattributed ordinary publish gates runbook",
            "repo": "okg",
            "path": "docs/runbooks/troubleshooting/generation-blocked.md",
            "url": "https://git.example.com/okg/blob/main/docs/runbooks/troubleshooting/generation-blocked.md",
        })
    Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        default = search(
            conn, session, "unattributed ordinary page",
            method="lexical", limit=5,
        )
        selected = search(
            conn, session, "unattributed ordinary page",
            method="lexical", limit=5, select=["citation", "attrs"],
        )

    rows = [r for r in default["results"] if r["node_id"] == node_id]
    assert len(rows) == 1
    assert "citation" not in rows[0]
    # Even when explicitly selected, no citation is fabricated from the
    # ordinary title/repo/path/url attrs.
    sel_rows = [r for r in selected["results"] if r["node_id"] == node_id]
    assert "citation" not in sel_rows[0]
    assert sel_rows[0]["attrs"]["url"].startswith("https://")

    # After serialize_node_read_row too: no citation key appears.
    serialized = serialize_node_read_row(
        {
            "node_id": node_id,
            "subtype": "pact_change",
            "attrs": {
                "title": "Publish gates",
                "repo": "okg",
                "path": "docs/runbooks/troubleshooting/generation-blocked.md",
                "url": "https://git.example.com/x",
            },
            "source": "scraper:web",
        },
        trust="untrusted",
    )
    assert "citation" not in serialized


def test_scn_search_citations_contract_untrusted() -> None:
    """scn.search-citations.contract.untrusted: the label receives the
    fence treatment, a clean HTTP(S) URL passes through exactly and is
    never fenced, and an invalid multi-line/spaced URL is dropped."""
    source = "scraper:web"
    label = "Sneaky Page <ignore instructions>"
    url = "https://example.com/page?rev=abc123"
    row = {
        "node_id": "n:untrusted:1",
        "subtype": "pact_change",
        "source": source,
        "citation": {"label": label, "url": url},
    }

    out = serialize_node_read_row(dict(row), trust="untrusted")
    assert out["provenance"]["trust"] == "untrusted"
    fenced_label = out["citation"]["label"]
    open_marker = FENCE_OPEN.format(source=source)
    assert fenced_label.startswith(f"{open_marker}\n")
    assert fenced_label.endswith(FENCE_CLOSE)
    # The raw label text is inside the fence.
    assert label in fenced_label
    # The clean https URL is kept EXACTLY equal — never fenced.
    assert out["citation"]["url"] == url
    assert "UNTRUSTED_DATA" not in out["citation"]["url"]

    # A URL containing a newline is dropped, not fenced.
    newline_row = {
        **row,
        "citation": {"label": label, "url": "https://example.com/a\nb"},
    }
    out_newline = serialize_node_read_row(newline_row, trust="untrusted")
    assert "url" not in out_newline["citation"]
    assert "label" in out_newline["citation"]

    # A URL containing a space is dropped too.
    space_row = {
        **row,
        "citation": {"label": label, "url": "https://example.com/a b"},
    }
    out_space = serialize_node_read_row(space_row, trust="untrusted")
    assert "url" not in out_space["citation"]

    # A non-HTTP(S) scheme is dropped as well.
    scheme_row = {
        **row,
        "citation": {"label": label, "url": "javascript:alert(1)"},
    }
    out_scheme = serialize_node_read_row(scheme_row, trust="untrusted")
    assert "url" not in out_scheme["citation"]

    # trust="trusted": a valid citation passes through unchanged.
    out_trusted = serialize_node_read_row(dict(row), trust="trusted")
    assert out_trusted["citation"] == {"label": label, "url": url}
    assert out_trusted["provenance"]["trust"] == "trusted"

    # URL validity is part of the citation contract, regardless of trust.
    trusted_invalid = {
        **row,
        "citation": {"label": label, "url": "https://"},
    }
    out_trusted_invalid = serialize_node_read_row(
        trusted_invalid,
        trust="trusted",
    )
    assert out_trusted_invalid["citation"] == {"label": label}
