"""Executed-test half of task.search-unindexed-signal.

Covers req.unindexed-subtype-legible from
pact/changes/index-document-source-file-search/pact.yaml (issue 900): a
subtype-filtered lexical search that finds nothing says WHY. Four
different facts previously produced one indistinguishable
`total_count: 0`:

  no_rows        the named subtype is not here at all
  not_indexed    rows exist but carry no `attrs.text` (the BM25 index is
                 partial on that key) — no query can ever match
  not_tokenized  the text CONTAINS the query but the text-search config
                 did not lex it out (a whole path is one token)
  no_match       indexed, tokenized, genuinely absent

`not_tokenized` is the case issue 900 actually reported, and it is the
reason key-presence alone is not an adequate signal: `document` HAD
`attrs.text` and was still unreachable by any word inside it.

Everything is measured on the rows visible at the pin, never read off the
subtype's catalog declaration.

All four reasons plus the two absence cases are asserted against ONE
published generation — `clean_runtime` setup and a publish cycle dominate
this file's runtime, and the per-PR lane it runs in has a time budget.
"""
from __future__ import annotations

import importlib
import uuid

import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp import open_session, search
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


#: Rows carry their words in a non-searchable attr, so no row has
#: `attrs.text` and the subtype is outside the BM25 partial index.
BLIND = "blindsub"
#: Rows carry ordinary word-bearing text.
SIGHTED = "sightedsub"
#: Rows carry text that is a single unlexable token — what a raw path was
#: for `document` before this change.
ONE_TOKEN = "onetokensub"

ONE_TOKEN_TEXT = "refs/sample_notes/07-Background_Estimation.tex"


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


def test_zero_hit_reasons_are_distinguishable(clean_runtime: str) -> None:
    """scn.unindexed-subtype-legible.{unindexed,indexed-no-match}, plus
    the tokenization gap and the absence cases.

    Four searches over ONE pinned generation all return
    `total_count: 0`, and each names a different reason.
    """
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        # No catalog declaration for any of these, so `attrs.text` is taken
        # verbatim from what the fact carries.
        _seed(conn, BLIND, {"body": "unsearchable words in a plain attr"})
        _seed(conn, SIGHTED, {"text": "the module under test"})
        _seed(conn, ONE_TOKEN, {"text": ONE_TOKEN_TEXT})

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")

        def lexical(query: str, subtype: str | None) -> dict:
            return search(
                conn, session, query, method="lexical", completeness="exact", limit=10,
                filter=None if subtype is None else {"subtype": subtype},
            )

        not_indexed = lexical("unsearchable", BLIND)
        no_match = lexical("zzzznotpresentanywhere", SIGHTED)
        not_tokenized = lexical("Background_Estimation", ONE_TOKEN)
        no_rows = lexical("module", "no_such_subtype")
        # `_` is an ILIKE single-character wildcard. Unescaped, this query
        # would "match" SIGHTED's `the module under test` and be reported
        # as a tokenization gap that does not exist.
        wildcard = lexical("m_dule", SIGHTED)
        unfiltered = lexical("zzzznotpresentanywhere", None)
        a_hit = lexical("module", SIGHTED)

    for label, result in (
        ("not_indexed", not_indexed), ("no_match", no_match),
        ("not_tokenized", not_tokenized), ("no_rows", no_rows),
    ):
        assert result["total_count"] == 0, (label, result)
        assert result["zero_hit_reason"] == label, result

    # Rows exist but nothing is in the index: no query can ever match.
    assert not_indexed["subtype_indexed"] is False
    # Indexed and tokenized; the term is genuinely absent.
    assert no_match["subtype_indexed"] is True
    # Indexed, and the text CONTAINS the query — the index just cannot
    # reach it. Reporting this as `no_match` is what sent issue 900's
    # reporter looking for a better query.
    assert not_tokenized["subtype_indexed"] is True
    # "You named nothing" must NOT collapse into "not searchable" — that
    # would be an unknown dressed as a measurement.
    assert no_rows["subtype_indexed"] is None

    # An ILIKE wildcard in the query must not manufacture a substring hit.
    assert wildcard["total_count"] == 0, wildcard
    assert wildcard["zero_hit_reason"] == "no_match", wildcard

    # An unfiltered search makes no per-subtype claim...
    assert unfiltered["total_count"] == 0
    assert "zero_hit_reason" not in unfiltered
    assert "subtype_indexed" not in unfiltered
    # ...and a hit is its own explanation, so no diagnostic work is done.
    assert a_hit["total_count"] > 0, a_hit
    assert "zero_hit_reason" not in a_hit
    assert "subtype_indexed" not in a_hit
    assert "zero_hit_scanned" not in a_hit

    # Every reason above describes rows that were examined, and the
    # examination covered the whole subtype in this fixture.
    for result in (not_indexed, no_match, not_tokenized):
        assert result["zero_hit_scanned"] == "all", result


def test_probe_never_reads_what_the_session_may_not_read(
    clean_runtime: str, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The probe reads through the Nomos read filter, not around it.

    Reading the raw segment would make this a substring-existence oracle:
    a caller could learn that text containing their query sits in a node
    they are forbidden to read, and iterate to reconstruct it. Here the
    read filter is forced to withhold everything; the probe must then
    report `no_rows` — declining to confirm existence — and must NOT
    report `not_tokenized` off the withheld row. It must also not say how
    many rows it withheld.
    """
    text = "refs/sample_notes/07-Background_Estimation.tex"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, ONE_TOKEN, {"text": text})

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out

    # `okg.substrate.mcp` re-exports the `search` FUNCTION under that
    # name, so `from okg.substrate.mcp import search` shadows the module.
    # Reach the module itself.
    search_module = importlib.import_module("okg.substrate.mcp.search")
    monkeypatch.setattr(
        search_module, "filter_node_rows_for_session",
        lambda conn, session, rows: [],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        result = search(
            conn, session, "Background_Estimation",
            method="lexical", filter={"subtype": ONE_TOKEN}, limit=10,
        )

    assert result["zero_hit_reason"] == "no_rows", result
    assert result["subtype_indexed"] is None, result
    assert result["zero_hit_scanned"] == "none", result


def test_no_diagnosis_when_the_ranker_matched(clean_runtime: str) -> None:
    """If BM25 matched and an attr filter emptied the page, the reason is
    the filter — not the query. Claiming `no_match` ("the term is
    genuinely absent") would be false, and claiming `not_tokenized` would
    leak that matching text exists in rows the caller did not receive.
    So the diagnostic stays silent."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, SIGHTED, {"text": "the module under test", "repo": "a"})

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name="default")
        result = search(
            conn, session, "module", method="lexical", completeness="exact", limit=10,
            filter={"subtype": SIGHTED, "attr_filters": {"repo": "zzz"}},
        )

    assert result["total_count"] == 0, result
    assert "zero_hit_reason" not in result, result
    assert "subtype_indexed" not in result, result
