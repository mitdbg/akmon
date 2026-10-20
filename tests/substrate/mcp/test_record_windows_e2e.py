"""`search(method="grep", view="windows")` on real publishes (PACT read-record-windows).

PostgreSQL tests through the served `search` tool. Each test publishes a
small graph: `document` records whose `document_chunk` passages hang off
the catalog-declared passage relation (`contains`), with `member_of`
occurrence edges that carry source positions.

* `docs` is a producer the deployment's source registry declares as the
  doc-corpus source, so its positions are authoritative and its chunks join
  into one searchable chain.
* `default` writes the SAME text, positions and attributes, but no registry
  entry declares it: equal attributes are not producer authority, so its
  continuity stays unknown.
"""
from __future__ import annotations

import contextlib
import json
import re
import uuid
from pathlib import Path

import jsonschema
import psycopg
import pytest

from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp.continuation import ContinuationTokenInvalid
from okg.substrate.attributes import (
    materialize_effective_attributes_for_generation,
    record_attribute_observation,
)
from okg.substrate.mcp import record_window_search as windows_module
from okg.substrate.mcp.record_window_search import WindowArgumentUnsupported
from okg.substrate.mcp.search_where import SearchScopeUnsupported
from okg.substrate.mcp.record_windows import WindowContinuityUnknown, WindowWorkBound
from okg.substrate.mcp.result_text import result_format_scope
from okg.substrate.transcript_trust import UNTRUSTED_CONTENT_ORIGIN_ATTR, WEB_TOOL_RESULT_ORIGIN
from okg.substrate.mcp.server import build_server
from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import SubtypeSpec, bind_test_catalog_writer, seed_catalog

pytestmark = pytest.mark.postgres

TEXT = "The zirconium cladding\nfailed at the weld seam near the outlet."
SPLIT = 18  # "The zirconium clad" | "ding\nfailed at ..."
_FENCE = re.compile(r"\A⟦UNTRUSTED_DATA[^⟧]*⟧\n(.*)\n⟦/UNTRUSTED_DATA⟧\Z", re.S)


def _plain(text: str) -> str:
    match = _FENCE.match(text)
    return match.group(1) if match else text


def _policy() -> dict:
    return {
        "policy_bundle_id": f"nomos:test:windows:{uuid.uuid4().hex[:8]}",
        "policy_version": "v1",
        "default_effect": "allow",
        "rules": [
            {"rule_id": "hide_secret", "effect": "deny", "priority": 300,
             "subjects": ["subject:mcp-reader"], "actions": ["graph.read.mcp"],
             "match": {"classification": "secret"}},
            {"rule_id": "redact_masked", "effect": "redact", "priority": 250,
             "subjects": ["subject:mcp-reader"], "actions": ["graph.read.mcp"],
             "match": {"classification": "masked"},
             "obligations": [{"type": "redaction"}, {"type": "audit"}]},
            {"rule_id": "allow_read", "effect": "allow", "priority": 100,
             "subjects": ["subject:mcp-reader"], "actions": ["graph.read.mcp"]},
        ],
    }


def _doc(doc: str, pieces: list[tuple[str, int]], *, length: int, own_text: str | None = None,
         separators: dict[int, str] | None = None, secret: frozenset[int] = frozenset(),
         masked: frozenset[int] = frozenset(), unpositioned: tuple = (),
         own_attrs: dict | None = None) -> list:
    path = f"{doc.split(':', 1)[1]}.md"
    attrs = {"classification": "internal", "path": path, **(own_attrs or {})}
    if own_text is not None:
        attrs["text"] = own_text
    facts: list = [NodeFact(doc, "document", attrs, {"repo": "r", "path": path}, {"v": 1})]
    for index, (text, offset) in enumerate(pieces):
        chunk = f"chunk:{doc}:{index:02d}"
        cls = "secret" if index in secret else "masked" if index in masked else "internal"
        identity = {"repo": "r", "path": path, "chunk_id": chunk}
        position = {
            "classification": cls, "chunk_ordinal": index, "chunk_total": len(pieces),
            "char_offset": offset, "char_end": offset + len(text), "doc_char_length": length,
        }
        if separators and index in separators:
            position["separator_before"] = separators[index]
        facts += [
            NodeFact(chunk, "document_chunk", {"classification": cls, "text": text},
                     identity, {"v": 1}),
            EdgeFact(chunk, doc, "member_of", position,
                     source_record_id=identity, source_revision={"v": 1}),
            EdgeFact(doc, chunk, "contains", {"classification": cls},
                     source_record_id={**identity, "edge": "contains"}, source_revision={"v": 1}),
        ]
    for index, (text, chunk_attrs) in enumerate(unpositioned, start=len(pieces)):
        # A chunk the producer stored no position for (a table, a caption).
        chunk = f"chunk:{doc}:{index:02d}"
        identity = {"repo": "r", "path": path, "chunk_id": chunk}
        facts += [
            NodeFact(chunk, "document_chunk",
                     {"classification": "internal", "text": text, **chunk_attrs}, identity, {"v": 1}),
            EdgeFact(chunk, doc, "member_of", {"classification": "internal", "chunk_ordinal": index},
                     source_record_id=identity, source_revision={"v": 1}),
            EdgeFact(doc, chunk, "contains", {"classification": "internal"},
                     source_record_id={**identity, "edge": "contains"}, source_revision={"v": 1}),
        ]
    return facts


def _split(doc: str) -> list:
    return _doc(doc, [(TEXT[:SPLIT], 0), (TEXT[SPLIT:], SPLIT)], length=len(TEXT))


BIG = "".join(f"line {i:04d} zirconium cladding report\n" for i in range(300))
OVERLAP = "alpha bravo charlie delta echo foxtrot golf hotel"


def _graph() -> tuple[list, list]:
    """(docs-producer facts, default-producer facts)."""
    docs = _split("doc:pos")
    docs += _doc("doc:ovl", [(OVERLAP[:30], 0), (OVERLAP[20:], 20)], length=len(OVERLAP))
    docs += _doc("doc:sep", [("alpha beta", 0), ("gamma delta", 11)], length=22,
                 separators={1: " "})
    docs += _doc("doc:gap", [("alpha beta", 0), ("gamma delta", 15)], length=26)
    docs += _doc("doc:hid", [("zirconium secret value", 0), ("weld seam", 23)], length=32,
                 secret=frozenset({0}))
    # doc:hid as an enforcing reader sees it: the leading passage never stored.
    docs += _doc("doc:miss", [("weld seam", 23)], length=32)
    docs += _doc("doc:msk", [("zirconium masked value", 0), ("weld seam", 23)], length=32,
                 masked=frozenset({0}))
    docs += _doc("doc:mid", [("alpha beta ", 0), ("hidden middle ", 11), ("gamma delta", 25)],
                 length=36, secret=frozenset({1}))
    docs += _doc("doc:tab", [("alpha beta ", 0), ("gamma delta", 11)], length=22,
                 unpositioned=(("cellvalue 42", {"chunk_kind": "table"}),
                               ("a plotted curve", {"chunk_source": "vision_caption"})))
    docs += _doc("doc:untab", [("alpha beta ", 0), ("gamma delta", 11)], length=22,
                 unpositioned=(("loose paragraph", {"chunk_kind": "paragraph"}),))
    docs += _doc("doc:rep", [("alpha one alpha two " + "x" * 300 + " alpha far", 0)], length=330)
    plain = _split("doc:gen")
    plain += _doc("doc:zr", [("zirconium only here", 0)], length=19)
    plain += _doc("doc:sm", [("the weld seam only", 0)], length=18)
    plain += _doc("doc:big", [], length=0, own_text=BIG)
    plain += _doc("doc:dense", [], length=0, own_text="a" * 12000)
    plain += _doc("doc:web", [], length=0, own_text="zirconium from a fetched page",
                  own_attrs={UNTRUSTED_CONTENT_ORIGIN_ATTR: WEB_TOOL_RESULT_ORIGIN})
    return docs, plain


def _seed(dsn: str) -> str:
    docs, plain = _graph()
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=[
                SubtypeSpec("document", metadata={
                    "searchable_text_fields": ["text"], "path_from": "path",
                    "passage_rollup": {"edge_type": "contains", "child_subtype": "document_chunk"},
                }),
                SubtypeSpec("document_chunk", metadata={
                    "searchable_text_fields": ["text"],
                    "pii_classes": {"classification": "non_pii", "text": "free_text"},
                }),
            ],
            narrowings=[("document", "contains", "document_chunk"),
                        ("document_chunk", "member_of", "document")],
        )
        activate_policy_bundle(
            conn, deployment_name="default", bundle=compile_policy_graph(_policy()),
            scope="deployment:default", activated_by="test:operator", bootstrap=True,
        )
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(docs, source="docs", strict_endpoints=True)
        store.apply_facts(plain, source="default", strict_endpoints=True)
    published = Publisher(
        dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default"),
    ).cycle()
    assert published["status"] == "published", published
    return published["generation_id"]


def _declare_docs_producer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A `default` deployment whose registry declares `docs` as doc-corpus."""
    directory = tmp_path / "default"
    (directory / "schemas").mkdir(parents=True)
    (directory / "source_registry.yaml").write_text(
        "sources:\n"
        "  docs:\n"
        "    module: okg.substrate.library.sources.doc_corpus\n"
        "    class: DocCorpusSource\n"
        "    ownership_id: windows-test.docs\n",
        encoding="utf-8",
    )
    (directory / "deployment.yaml").write_text(
        "name: default\ndescription: record windows fixture\n"
        "schema_dir: schemas\nsource_registry: source_registry.yaml\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))


@pytest.fixture
def served(clean_runtime: str, okg_mcp_can_log_in: str, tmp_path, monkeypatch):
    _seed(clean_runtime)
    _declare_docs_producer(tmp_path, monkeypatch)
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        yield mcp
    finally:
        mcp._okg_pool.close()


def _search(mcp, **arguments):
    return mcp._tool_manager._tools["search"].fn(**arguments)


def _windows(mcp, query: str, records: list[str], **extra):
    return _search(mcp, query=query, method="grep", view="windows",
                   scope={"records": records}, max_bytes=131072, **extra)


def _fragments_text(record: dict) -> str:
    return "".join(_plain(f["text"]) for w in record["windows"] for f in w["fragments"])


# ── membership ────────────────────────────────────────────────────────────


def test_split_terms_match_record_wide_and_wrong_records_do_not(served) -> None:
    page = _windows(served, "zirconium seam", ["doc:pos", "doc:gen", "doc:zr", "doc:sm"])
    assert [r["node_id"] for r in page["results"]] == ["doc:gen", "doc:pos"]
    assert page["total_count"] == 2 and page["total_count_basis"] == "exact"
    # The #3213 envelope: `coverage.selection` is the count's only qualifier.
    assert "total_count_unit" not in page and "total_count_set" not in page
    assert page["coverage"]["selection"] == {"unit": "record", "basis": "matching_records"}
    assert page["coverage"]["retrieval"]["status"] == "exhaustive"
    assert page["continuation_token"] is None
    assert page["coverage"]["delivery"] == {"complete": True}
    for key in ("returned_records", "delivery_complete", "has_more"):
        assert key not in page
    for record in page["results"]:
        terms = {m["term"] for w in record["windows"] for m in w["matches"]}
        assert terms == {0, 1}
        assert page["evidence_domain"] == "readable_record_occurrences"
    # Wrong-record negative control: each term exists, in different records.
    alone = _windows(served, "zirconium seam", ["doc:zr", "doc:sm"])
    assert alone["results"] == [] and alone["total_count"] == 0
    # Passage-local matching never claims record-wide evidence.
    local = _windows(served, "zirconium seam", ["doc:pos", "doc:gen"], matching="passage")
    assert local["results"] == [] and local["evidence_domain"] == "readable_passages"


def test_split_phrase_matches_only_with_proven_continuity(served) -> None:
    page = _windows(served, '"cladding failed"', ["doc:pos"], context_chars=0)
    (record,) = page["results"]
    assert record["continuity"] == "proven"
    assert record["position_basis"] == {"producer_contract": 2}
    (window,) = record["windows"]
    (match,) = window["matches"]
    assert [s["chunk_id"] for s in match["source"]] == ["chunk:doc:pos:00", "chunk:doc:pos:01"]
    assert _fragments_text(record) == "cladding\nfailed"
    assert [f["range"] for f in window["fragments"]] == [[14, 18], [0, 11]]
    assert [f["line_range"] for f in window["fragments"]] == [[1, 1], [1, 2]]


def test_equal_attributes_without_producer_contract_stay_unknown(served) -> None:
    with pytest.raises(WindowContinuityUnknown) as refused:
        _windows(served, '"cladding failed"', ["doc:gen"])
    assert refused.value.data["record_id"] == "doc:gen"
    assert refused.value.data["continuity"] == "unknown"
    # A positive phrase inside one passage still matches under unknown continuity.
    page = _windows(served, '"weld seam"', ["doc:gen"])
    (record,) = page["results"]
    assert record["continuity"] == "unknown"
    assert record["position_basis"] == {"producer_contract_unknown": 2}


def test_an_unproven_negative_phrase_refuses_and_a_proven_one_decides(served) -> None:
    with pytest.raises(WindowContinuityUnknown):
        _windows(served, 'zirconium -"cladding failed"', ["doc:gen"])
    decided = _windows(served, 'zirconium -"cladding failed"', ["doc:pos"])
    assert decided["results"] == [] and decided["total_count"] == 0


def test_no_registry_means_no_producer_authority(
    clean_runtime: str, okg_mcp_can_log_in: str, tmp_path, monkeypatch,
) -> None:
    _seed(clean_runtime)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))  # holds no deployment
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        with pytest.raises(WindowContinuityUnknown):
            _windows(mcp, '"cladding failed"', ["doc:pos"])
    finally:
        mcp._okg_pool.close()


def _same_answer(record: dict) -> dict:
    """A record's whole answer, windows included (offsets, ranges, match ids,
    fragment text and provenance), with only the ids that name a different
    record's own rows dropped (the record id) or renamed by first appearance
    (chunk, edge and separator ids)."""
    names: dict[str, str] = {}

    def canon(value, key=None):
        if isinstance(value, dict):
            return {k: canon(v, k) for k, v in value.items() if k not in ("node_id", "record_id")}
        if isinstance(value, list):
            return [canon(item, key) for item in value]
        if key in ("chunk_id", "edge_id", "separator") and isinstance(value, str):
            return names.setdefault(value, f"id{len(names)}")
        return value

    return canon(record)


def test_hidden_passages_are_not_searched(served, monkeypatch) -> None:
    # Enforcement off: the secret passage is read, so the control is real.
    assert _windows(served, "zirconium", ["doc:hid"])["total_count"] == 1
    # The leading passage never stored: the text before 23 is unread, so a
    # term absent from the rest is undecided, never an exact zero.
    with pytest.raises(WindowContinuityUnknown) as missing:
        _windows(served, "zirconium", ["doc:miss"])
    (missing_seam,) = _windows(served, "seam", ["doc:miss"])["results"]
    assert missing_seam["continuity"] == "unknown"
    assert missing_seam["source_coverage"] == "partial"
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-reader")
    # Hidden reads exactly as missing: the same refusal, the same record.
    with pytest.raises(WindowContinuityUnknown) as hidden:
        _windows(served, "zirconium", ["doc:hid"])
    assert hidden.value.code == missing.value.code
    assert {k: v for k, v in hidden.value.data.items() if k != "record_id"} == {
        k: v for k, v in missing.value.data.items() if k != "record_id"
    }
    (record,) = _windows(served, "seam", ["doc:hid"])["results"]
    assert record["windows"] and _same_answer(record) == _same_answer(missing_seam)
    # Byte for byte, windows and fragments included.
    assert json.dumps(_same_answer(record), sort_keys=True) == json.dumps(
        _same_answer(missing_seam), sort_keys=True)
    assert {f["chunk_id"] for w in record["windows"] for f in w["fragments"]} == {
        "chunk:doc:hid:01"
    }
    assert "secret" not in _fragments_text(record)
    # In a records scope both are undecided records, alike.
    both = _windows(served, "zirconium", ["doc:hid", "doc:miss"])
    assert both["total_count"] == 0 and both["total_count_basis"] == "lower_bound"
    assert sorted(both["undecided_records"]) == ["doc:hid", "doc:miss"]
    hid_row, miss_row = sorted(both["results"], key=lambda r: r["node_id"])
    assert _same_answer(hid_row) == _same_answer(miss_row)


def test_masked_text_is_never_searched_or_served(served, monkeypatch) -> None:
    assert _windows(served, "zirconium", ["doc:msk"])["total_count"] == 1
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-reader")
    # The raw text matches; the masked passage is not read, so its text is
    # unread like a missing passage's and the term is undecided, not absent.
    with pytest.raises(WindowContinuityUnknown):
        _windows(served, "zirconium", ["doc:msk"])
    (record,) = _windows(served, "seam", ["doc:msk"])["results"]
    assert record["masked_occurrences"] == 1
    assert {f["chunk_id"] for w in record["windows"] for f in w["fragments"]} == {
        "chunk:doc:msk:01"
    }
    assert "masked value" not in json.dumps(record)


# ── continuity details ────────────────────────────────────────────────────


def test_verified_overlap_is_trimmed_and_kept_as_a_location(served) -> None:
    (record,) = _windows(served, '"echo foxtrot"', ["doc:ovl"], context_chars=4096)["results"]
    (window,) = record["windows"]
    assert _fragments_text(record) == OVERLAP
    assert any(loc["chunk_id"] == "chunk:doc:ovl:01" for loc in window["locations"])


def test_a_stored_separator_is_restored_and_a_real_gap_is_not_joined(served) -> None:
    (record,) = _windows(served, '"beta gamma"', ["doc:sep"], context_chars=4096)["results"]
    (window,) = record["windows"]
    assert [f["kind"] for f in window["fragments"]] == ["chunk", "separator", "chunk"]
    assert _fragments_text(record) == "alpha beta gamma delta"
    # Text missing between two positioned passages is unread: a phrase's
    # absence across it is never decided.
    with pytest.raises(WindowContinuityUnknown):
        _windows(served, '"beta gamma"', ["doc:gap"])


# ── windows and counts ────────────────────────────────────────────────────


def test_near_matches_merge_and_far_ones_stay_separate(served) -> None:
    page = _windows(served, "alpha", ["doc:rep"], context_chars=5)
    (record,) = page["results"]
    assert record["match_count"] == 3 and record["window_count"] == 2
    assert [len(w["matches"]) for w in record["windows"]] == [2, 1]
    # Two windows of one record are one record.
    assert page["total_count"] == 1
    assert page["window_count"] == 2 and page["match_count"] == 3


def _wire(result) -> tuple[int, str]:
    encoded = result.model_dump_json(by_alias=True, exclude_none=True)
    return len(encoded.encode("utf-8")), result.content[0].text


@pytest.mark.anyio
@pytest.mark.parametrize("budget", [4096, 131072])
async def test_every_page_fits_and_reaches_every_part_once_in_json(served, budget) -> None:
    arguments = {"query": "zirconium", "method": "grep", "view": "windows",
                 "scope": {"record": "doc:big"}, "context_chars": 200, "max_bytes": budget}
    token = None
    texts, matches, pages = [], [], 0
    while True:
        call = dict(arguments, **({"continuation_token": token} if token else {}))
        with result_format_scope("json"):
            result = await served.call_tool("search", call)
            size, text = _wire(result)
            assert size <= budget
            page = json.loads(text)
            assert page.get("status") != "refused", page
            if token is not None:
                again = json.loads((await served.call_tool("search", call)).content[0].text)
                assert again["results"] == page["results"]
        for record in page["results"]:
            assert record["node_id"] == "doc:big"
            for window in record["windows"]:
                matches += [m["match_id"] for m in window["matches"]]
                texts += [_plain(f["text"]) for f in window["fragments"]]
        pages += 1
        token = page.get("continuation_token")
        if token is None:
            break
        assert pages < 400
    assert "".join(texts) == BIG
    assert len(matches) == len(set(matches)) == 300
    assert page["coverage"]["delivery"] == {"complete": True}
    if budget == 131072:
        assert pages == 1
    else:
        assert pages > 1


@pytest.mark.anyio
@pytest.mark.parametrize("budget", [4096, 131072])
async def test_every_text_page_fits_and_carries_the_same_matches(served, budget) -> None:
    arguments = {"query": "zirconium", "method": "grep", "view": "windows",
                 "scope": {"record": "doc:big"}, "max_bytes": budget}
    token, seen, pages = None, [], 0
    while True:
        call = dict(arguments, **({"continuation_token": token} if token else {}))
        size, text = _wire(await served.call_tool("search", call))
        assert size <= budget
        assert text.startswith("search ok"), text[:2000]
        seen += re.findall(r"^--- match · match_id=(\S+)", text, flags=re.M)
        status = text.splitlines()[0]
        found = re.search(r" · next: (\S+)", status)
        pages += 1
        if found is None or found.group(1) == "none":
            # The last page: no cursor and no bound reached. Its `complete`
            # ending is coverage's delivery (PACT operator-page-counts-3190),
            # said once: no separate delivery fact (read-model-s6-page-next).
            assert "bound reached" not in status
            # D8 (read-passage-rows-3666): the ending is the last fact.
            assert status.split(" · ")[-1] == "complete", status
            assert " · delivery " not in status, status
            break
        # `search` takes no `next`: a windows page, like any grep page,
        # names the argument its token goes back as.
        assert found.group(1).startswith("continuation_token="), status
        token = found.group(1)[len("continuation_token="):].strip('"')
        assert pages < 400
    assert len(seen) == len(set(seen)) == 300


# ── bounds and refusals ───────────────────────────────────────────────────


def test_dense_matches_stop_at_the_named_event_bound(served) -> None:
    with pytest.raises(WindowWorkBound) as refused:
        _windows(served, "re:/a/", ["doc:dense"])
    assert refused.value.data["bound"] == "match_events"


def test_a_token_is_bound_to_the_whole_call(served) -> None:
    first = _search(served, query="zirconium", method="grep", view="windows",
                    scope={"record": "doc:big"}, max_bytes=4096)
    token = first["continuation_token"]
    assert token
    for changed in ({"query": "cladding"}, {"context_chars": 10}, {"matching": "passage"},
                    {"scope": {"record": "doc:pos"}}, {"max_bytes": 8192}):
        call = {"query": "zirconium", "method": "grep", "view": "windows",
                "scope": {"record": "doc:big"}, "max_bytes": 4096,
                "continuation_token": token, **changed}
        with pytest.raises(ContinuationTokenInvalid):
            _search(served, **call)
    with pytest.raises(ContinuationTokenInvalid):
        _search(served, query="zirconium", method="grep", view="windows",
                scope={"record": "doc:big"}, max_bytes=4096,
                continuation_token=token[:-4] + "AAAA")


@pytest.mark.parametrize("extra, argument", [
    ({"method": "lexical"}, "method"),
    ({"context_chars": True}, "context_chars"),
    ({"context_chars": 4097}, "context_chars"),
    ({"matching": "chunk"}, "matching"),
    # Called as a Python function, which is not rewritten: the windows read
    # itself takes no select. An MCP or command-line caller's select is cut
    # from the finished rows (test_read_grammar_db.py, #3666 step 3a).
    ({"select": ["node_id"]}, "select"),
    ({"offset": 1}, "offset"),
    ({"path_prefix": "pos"}, "path_prefix"),
    ({"query": "zirconium path:pos"}, "query"),
    ({"query": "path:pos"}, "query"),
    ({"where": {"op": "eq", "field": "type", "value": "document"}}, "where"),
    ({"profile": "all"}, "profile"),
    ({"source_family": "docs"}, "source_family"),
    ({"subtype_filter": "document"}, "subtype_filter"),
    ({"attr_filters": {"path": "pos.md"}}, "attr_filters"),
    ({"max_per_parent": 1}, "max_per_parent"),
])
def test_unsupported_combinations_refuse_by_name(served, extra, argument) -> None:
    call = {"query": "zirconium", "method": "grep", "view": "windows",
            "scope": {"record": "doc:pos"}, "max_bytes": 4096, **extra}
    opened = []
    original = served._okg_pool.connection
    served._okg_pool.connection = lambda *a, **k: opened.append(1) or original(*a, **k)
    try:
        with pytest.raises(WindowArgumentUnsupported) as refused:
            _search(served, **call)
        refused_opens = len(opened)
        # A refusal known to come before the read opens (path_prefix on
        # lexical) costs the same per-call session reads, and no more.
        baseline = _search(served, query="zirconium", method="lexical", path_prefix="p")
        assert baseline["error"]["code"] == "path_prefix_not_applicable"
        baseline_opens = len(opened) - refused_opens
    finally:
        served._okg_pool.connection = original
    assert refused.value.data["argument"] == argument
    assert refused_opens == baseline_opens


def test_windows_without_a_scope_read_the_greps_own_top_records(served) -> None:
    """#3605 (`winscope A`): lane X agents sent view="windows" without a
    record scope 286 times and were refused; they had no ids to pass. The
    call now reads the windows of the grep's own top `limit` records, page
    by page, exactly as the same call scoped to those records."""
    hits = [row["node_id"] for row in _search(
        served, query="zirconium", method="grep", limit=3, select=["node_id"],
    )["results"]]
    assert len(hits) >= 2

    def follow(**arguments):
        pages, token = [], None
        for _ in range(40):
            page = _search(served, query="zirconium", method="grep", view="windows",
                           limit=3, max_bytes=4096, continuation_token=token, **arguments)
            assert page.get("status") != "refused", page
            pages.append(page)
            token = page.get("continuation_token")
            if not token:
                return pages
        raise AssertionError("the windows read did not end")

    unscoped = follow()
    scoped = follow(scope={"records": hits})
    assert [r["node_id"] for p in unscoped for r in p["results"]] == [
        r["node_id"] for p in scoped for r in p["results"]]
    assert [_fragments_text(r) for p in unscoped for r in p["results"]] == [
        _fragments_text(r) for p in scoped for r in p["results"]]
    assert {r["node_id"] for p in unscoped for r in p["results"]} <= set(hits)
    # A children_of scope is still refused by name.
    with pytest.raises(SearchScopeUnsupported):
        _search(served, query="zirconium", method="grep", view="windows",
                scope={"children_of": "doc:pos"}, max_bytes=4096)
    # A token of another read is refused before any grep runs.
    from okg.substrate.mcp import continuation

    text_token = continuation.issue_continuation({
        "kind": "inspect_text", "projection": {"node_id": "doc:big"},
        "key_id": continuation.signing_key_id(),
    }, cursor={})
    with pytest.raises(ContinuationTokenInvalid) as wrong:
        _search(served, query="zirconium", method="grep", view="windows",
                max_bytes=4096, continuation_token=text_token)
    assert "not a windows token" in str(wrong.value)
    # A grep that matches nothing answers with its own empty page.
    empty = _search(served, query="qqxyzzy", method="grep", view="windows",
                    max_bytes=4096)
    assert empty["results"] == [] and not empty.get("continuation_token")


def test_window_arguments_without_the_view_refuse(served) -> None:
    for extra in ({"matching": "record"}, {"context_chars": 10}):
        with pytest.raises(WindowArgumentUnsupported):
            _search(served, query="zirconium", method="grep", **extra)


def test_the_schema_accepts_the_documented_call_and_legacy_grep_is_unchanged(served) -> None:
    tool = served._tool_manager._tools["search"]
    documented = {"query": "zirconium seam", "method": "grep", "view": "windows",
                  "matching": "record", "scope": {"record": "doc:pos"},
                  "context_chars": 200, "limit": 4, "max_bytes": 4096}
    jsonschema.validate(documented, tool.parameters)
    # The window arguments are accepted and no longer published (#3666, M10),
    # so a validating client takes them; their bounds are the server's.
    # `view` is published again (read-passage-rows-3666, D1: "list").
    assert not {"matching", "context_chars"} & set(tool.parameters["properties"])
    assert "view" in tool.parameters["properties"]
    # Window arguments on a non-windows view are refused by name before the
    # read.
    jsonschema.validate({**documented, "view": "cards"}, tool.parameters)
    with pytest.raises(WindowArgumentUnsupported) as refused:
        _search(served, **{**documented, "view": "cards"})
    assert refused.value.code == "window_argument_unsupported"
    assert _search(served, **documented)["total_count"] == 1
    legacy = _search(served, query="zirconium", method="grep")
    assert "view" not in legacy and legacy["results"]


def test_limit_counts_records_and_the_token_reaches_the_next_record(served) -> None:
    arguments = {"query": "zirconium seam", "method": "grep", "view": "windows",
                 "scope": {"records": ["doc:pos", "doc:gen"]}, "limit": 1, "max_bytes": 131072}
    first = _search(served, **arguments)
    assert [r["node_id"] for r in first["results"]] == ["doc:gen"]
    assert first["continuation_token"] and first["coverage"]["delivery"]["complete"] is False
    second = _search(served, **arguments, continuation_token=first["continuation_token"])
    assert [r["node_id"] for r in second["results"]] == ["doc:pos"]
    assert second["continuation_token"] is None and second["offset"] == 1
    assert second["coverage"]["delivery"] == {"complete": True}


# ── review fixes (H1, M1-M4, L4, L5) ──────────────────────────────────────


def test_a_word_split_across_an_unproven_boundary_is_never_absent(served) -> None:
    # "clad|ding": the word straddles doc:gen's unproven boundary.
    with pytest.raises(WindowContinuityUnknown) as refused:
        _windows(served, "cladding", ["doc:gen"])
    assert refused.value.data["term_kind"] == "word"
    with pytest.raises(WindowContinuityUnknown):
        _windows(served, "zirconium -cladding", ["doc:gen"])
    # Under the producer contract the same word is found across the join.
    assert _windows(served, "cladding", ["doc:pos"])["total_count"] == 1


def test_a_multi_record_scope_reports_undecided_records_instead_of_refusing(served) -> None:
    page = _windows(served, "cladding", ["doc:gen", "doc:pos", "doc:zr"])
    rows = {r["node_id"]: r for r in page["results"]}
    assert set(rows) == {"doc:gen", "doc:pos"}
    assert rows["doc:gen"]["decided"] is False and rows["doc:gen"]["windows"] == []
    assert rows["doc:gen"]["refusal"] == {
        "code": "window_continuity_unknown", "term_index": 0, "term_kind": "word",
    }
    assert "decided" not in rows["doc:pos"] and rows["doc:pos"]["windows"]
    assert page["total_count"] == 1 and page["total_count_basis"] == "lower_bound"
    assert page["undecided_records"] == ["doc:gen"]
    assert page["coverage"]["selection"]["size"] is None
    assert page["coverage"]["retrieval"]["status"] == "bounded"


def test_hidden_passages_neither_count_nor_decide_absence(served, monkeypatch) -> None:
    off = _windows(served, "seam", ["doc:hid"])
    assert off["results"][0]["readable_occurrences"] == 2
    assert off["work"]["readable_occurrences"] == 2
    # A positioned record with every passage readable decides absence.
    assert _windows(served, "nowhere", ["doc:mid"])["total_count"] == 0
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-reader")
    on = _windows(served, "seam", ["doc:hid"])
    # No count moves with the hidden passage.
    assert on["results"][0]["readable_occurrences"] == 1
    assert on["work"]["readable_occurrences"] == 1
    assert not any("raw" in key for key in on["work"])
    # The occurrence bound counts readable texts only.
    monkeypatch.setattr(windows_module, "MAX_WINDOW_OCCURRENCES", 1)
    assert _windows(served, "seam", ["doc:hid"])["total_count"] == 1
    monkeypatch.setattr(windows_module, "MAX_WINDOW_OCCURRENCES", 5000)
    # A hidden middle passage leaves a gap: absence is no longer decided.
    with pytest.raises(WindowContinuityUnknown):
        _windows(served, "nowhere", ["doc:mid"])


def test_an_override_that_fakes_adjacency_lends_no_continuity(
    clean_runtime: str, okg_mcp_can_log_in: str, tmp_path, monkeypatch,
) -> None:
    generation = _seed(clean_runtime)
    _declare_docs_producer(tmp_path, monkeypatch)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        (edge_id,) = conn.execute(
            """
            SELECT v.target_id
              FROM okg.attribute_effective_view AS view
              JOIN okg.attribute_versions AS v USING (target_kind, target_id, attrs_hash)
             WHERE view.generation_id = %s AND view.target_kind = 'edge'
               AND v.attrs @> %s::jsonb
            """,
            (generation, '{"char_offset": 15, "doc_char_length": 26}'),
        ).fetchone()
        # Five stored spaces would fill the five-character gap exactly.
        record_attribute_observation(
            conn, target_kind="edge", target_id=edge_id, generation_id=generation,
            attr_key="separator_before", value="     ", producer="test:fake-adjacency",
        )
        materialize_effective_attributes_for_generation(conn, generation_id=generation)
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        with pytest.raises(WindowContinuityUnknown):
            _windows(mcp, '"beta gamma"', ["doc:gap"])
        page = _windows(mcp, "alpha", ["doc:gap"])
        assert page["results"][0]["position_basis"] == {
            "producer_contract": 1, "separator_not_stored": 1,
        }
    finally:
        mcp._okg_pool.close()


def test_an_override_relabelling_text_as_a_table_lends_no_continuity(
    clean_runtime: str, okg_mcp_can_log_in: str, tmp_path, monkeypatch,
) -> None:
    # doc:untab's unpositioned paragraph keeps continuity unknown; an
    # accepted override calling it a table must not take it out of the
    # stream: the stored kind decides, as for `separator_before`.
    generation = _seed(clean_runtime)
    _declare_docs_producer(tmp_path, monkeypatch)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        record_attribute_observation(
            conn, target_kind="node", target_id="chunk:doc:untab:02",
            generation_id=generation, attr_key="chunk_kind", value="table",
            producer="test:relabel-table",
        )
        materialize_effective_attributes_for_generation(conn, generation_id=generation)
        (kind,) = conn.execute(
            """
            SELECT v.attrs->>'chunk_kind'
              FROM okg.attribute_effective_view AS view
              JOIN okg.attribute_versions AS v USING (target_kind, target_id, attrs_hash)
             WHERE view.generation_id = %s AND view.target_kind = 'node'
               AND view.target_id = 'chunk:doc:untab:02'
            """,
            (generation,),
        ).fetchone()
        assert kind == "table"  # the override is accepted and effective
    mcp = build_server(dsn=clean_runtime, deployment_name="default")
    try:
        with pytest.raises(WindowContinuityUnknown):
            _windows(mcp, "nowhere", ["doc:untab"])
        (record,) = _windows(mcp, "loose", ["doc:untab"])["results"]
        assert record["continuity"] == "unknown"
    finally:
        mcp._okg_pool.close()


def test_the_readable_row_bound_holds_across_a_records_scope(served, monkeypatch) -> None:
    # doc:pos and doc:gen each store four readable occurrence edges (two
    # passages, `contains` and `member_of`). Each fits alone; together they
    # pass the call's readable-row bound, which refuses by name.
    monkeypatch.setattr(windows_module, "MAX_READABLE_OCCURRENCE_ROWS", 6)
    assert _windows(served, "zirconium", ["doc:pos"])["total_count"] == 1
    assert _windows(served, "zirconium", ["doc:gen"])["total_count"] == 1
    with pytest.raises(WindowWorkBound) as refused:
        _windows(served, "zirconium", ["doc:pos", "doc:gen"])
    assert refused.value.data == {"bound": "readable_occurrence_rows", "max_rows": 6}
    # The fixed per-record cap on stored rows refuses with no count.
    monkeypatch.setattr(windows_module, "MAX_READABLE_OCCURRENCE_ROWS", 100_000)
    monkeypatch.setattr(windows_module, "RAW_OCCURRENCE_SAFETY_CAP", 3)
    with pytest.raises(WindowWorkBound) as refused:
        _windows(served, "zirconium", ["doc:pos"])
    assert refused.value.data == {"bound": "raw_occurrence_scan"}


def _outcome(mcp, records: list[str]) -> tuple:
    """Refuse-or-answer for one call, with only what names no record row."""
    try:
        page = _windows(mcp, "seam", records)
    except WindowWorkBound as exc:
        return ("refused", json.dumps(exc.data, sort_keys=True))
    return ("answer", page["total_count"], page["total_count_basis"],
            len(page.get("undecided_records", [])))


_COMPOSITIONS = (
    ("X",), ("X", "doc:pos"), ("doc:pos", "X"), ("X", "doc:pos", "doc:gen"),
    ("doc:zr", "X", "doc:sm"),
)


def _scope(shape: tuple, record: str) -> list[str]:
    return [record if name == "X" else name for name in shape]


def test_scope_composition_cannot_turn_the_call_bounds_into_a_hidden_row_count(
    served, monkeypatch,
) -> None:
    """doc:hid stores four occurrence rows, two of them hidden from an
    enforcing reader; doc:miss stores only its two readable rows. Whatever
    known records share the scope and wherever the call's row bound sits,
    the call refuses or answers alike for both: hidden rows never move it."""
    # Control: unenforced, doc:hid's four rows are all readable, so the same
    # sweep does tell the two apart (the comparison below can fail).
    unenforced = []
    for bound in range(1, 12):
        monkeypatch.setattr(windows_module, "MAX_READABLE_OCCURRENCE_ROWS", bound)
        for shape in _COMPOSITIONS:
            unenforced.append(_outcome(served, _scope(shape, "doc:hid"))
                              == _outcome(served, _scope(shape, "doc:miss")))
    assert not all(unenforced)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "enforce")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:mcp-reader")
    flips = set()
    for bound in range(1, 12):
        monkeypatch.setattr(windows_module, "MAX_READABLE_OCCURRENCE_ROWS", bound)
        for shape in _COMPOSITIONS:
            hidden = _outcome(served, _scope(shape, "doc:hid"))
            missing = _outcome(served, _scope(shape, "doc:miss"))
            assert hidden == missing, (bound, shape, hidden, missing)
            flips.add(hidden[0])
    assert flips == {"refused", "answer"}  # the sweep crosses the boundary
    # The per-record cap is one fixed threshold on that record's stored rows:
    # doc:hid (four stored) refuses at a cap of 3 in every scope and never at
    # 4, whatever else the scope holds (one bit about doc:hid, disclosed).
    monkeypatch.setattr(windows_module, "MAX_READABLE_OCCURRENCE_ROWS", 100_000)
    capped = ("refused", json.dumps({"bound": "raw_occurrence_scan"}))
    for cap, refuses in ((3, True), (4, False)):
        monkeypatch.setattr(windows_module, "RAW_OCCURRENCE_SAFETY_CAP", cap)
        for shape in (("X",), ("doc:zr", "X"), ("X", "doc:sm", "doc:zr"), ("doc:sm", "X")):
            got = _outcome(served, _scope(shape, "doc:hid"))
            assert (got == capped) is refuses, (cap, shape, got)


def test_the_call_time_bound_refuses_by_name_mid_call(served, monkeypatch) -> None:
    """A call past its wall-clock bound refuses; it never answers empty."""
    records = ["doc:pos", "doc:gen", "doc:zr"]
    assert _windows(served, "zirconium", records)["total_count"] == 3
    ticks = iter(range(10_000))

    class Clock:
        @staticmethod
        def monotonic() -> float:
            return next(ticks) / 10  # a tenth of a second per reading

    monkeypatch.setattr(windows_module, "time", Clock)
    monkeypatch.setattr(windows_module, "MAX_WINDOW_CALL_SECONDS", 0.25)
    with pytest.raises(WindowWorkBound) as refused:
        _windows(served, "zirconium", records)
    assert refused.value.data == {"bound": "call_time", "max_seconds": 0.25}
    # Under a bound the call does not reach, the same fake clock answers.
    monkeypatch.setattr(windows_module, "MAX_WINDOW_CALL_SECONDS", 1000.0)
    assert _windows(served, "zirconium", records)["total_count"] == 3


@pytest.mark.anyio
async def test_a_stamp_that_grows_inside_a_retry_still_fits_the_same_page(
    served, monkeypatch,
) -> None:
    """A retried page keeps the same spare as a first page. Stamps that grow
    between the measurement and the served page (the freshness cache
    refilling inside the call) still fit, and the retry serves the same page."""
    from okg.substrate.mcp import server as server_module

    budget = 4096
    arguments = {"query": "zirconium", "method": "grep", "view": "windows",
                 "scope": {"record": "doc:big"}, "context_chars": 200, "max_bytes": budget}
    with result_format_scope("json"):
        first = json.loads((await served.call_tool("search", arguments)).content[0].text)
        token = first["continuation_token"]
        assert token
        call = dict(arguments, continuation_token=token)
        plain = json.loads((await served.call_tool("search", call)).content[0].text)
    original = server_module._stamp_freshness_outcomes
    state = {"calls": 0}

    def growing(payload, *args, **kwargs):
        original(payload, *args, **kwargs)
        state["calls"] += 1
        if state["calls"] > 1:
            # Past the call's measurement (its first stamping): grown by
            # more than the old 64-byte retry spare, within the 256 kept.
            payload["source_freshness"] = {**payload["source_freshness"], "pad": "x" * 80}

    monkeypatch.setattr(server_module, "_stamp_freshness_outcomes", growing)
    with result_format_scope("json"):
        size, text = _wire(await served.call_tool("search", call))
    assert state["calls"] >= 2
    assert size <= budget
    assert '"pad"' in text  # the grown stamp is on the served page
    grown = json.loads(text)
    assert grown.get("status") != "refused", grown
    assert grown["results"] == plain["results"]
    assert grown["continuation_token"] == plain["continuation_token"]


def test_unpositioned_non_text_chunks_never_judge_continuity(served) -> None:
    # Tables and image captions are searched alone and stay out of the stream.
    assert _windows(served, "nowhere", ["doc:tab"])["total_count"] == 0
    (record,) = _windows(served, "cellvalue", ["doc:tab"])["results"]
    assert record["continuity"] == "proven"
    assert {f["chunk_id"] for w in record["windows"] for f in w["fragments"]} == {
        "chunk:doc:tab:02"
    }
    # An unpositioned paragraph is running text: continuity stays unknown.
    with pytest.raises(WindowContinuityUnknown):
        _windows(served, "nowhere", ["doc:untab"])


def test_the_occurrence_and_source_byte_bounds_stop_by_name(served, monkeypatch) -> None:
    monkeypatch.setattr(windows_module, "MAX_WINDOW_OCCURRENCES", 1)
    with pytest.raises(WindowWorkBound) as refused:
        _windows(served, "zirconium", ["doc:pos"])
    assert refused.value.data["bound"] == "occurrences"
    monkeypatch.setattr(windows_module, "MAX_WINDOW_OCCURRENCES", 5000)
    monkeypatch.setattr(windows_module, "MAX_SOURCE_BYTES", 10)
    with pytest.raises(WindowWorkBound) as refused:
        _windows(served, "zirconium", ["doc:pos"])
    assert refused.value.data["bound"] == "source_bytes"


def test_own_text_keeps_its_untrusted_origin(served) -> None:
    (record,) = _windows(served, "zirconium", ["doc:web"])["results"]
    (fragment,) = record["windows"][0]["fragments"]
    assert fragment["provenance"]["content_origin"] == WEB_TOOL_RESULT_ORIGIN


# ── wire walk: text and JSON, every page, three budgets ───────────────────

_RECORD_HEAD = re.compile(r"^\[(\d+)\] (\S+) \((\S+)\)(?: · (.*))?$")
_FRAGMENT_HEAD = re.compile(r"^--- (text|separator)(?: · .*)? · chars=(\d+)$")


def _json_model(page: dict) -> dict:
    """What one JSON page carries: its records, windows and counts."""
    records = []
    for row in page["results"]:
        windows = [
            (w["window_id"], [m["match_id"] for m in w["matches"]], len(w["locations"]),
             "".join(_plain(f["text"]) for f in w["fragments"]))
            for w in row["windows"]
        ]
        records.append((row["node_id"], row.get("decided") is False, windows))
    return {
        "records": records,
        "total_count": page["total_count"],
        "lower_bound": page["total_count_basis"] == "lower_bound",
        "undecided": page.get("undecided_records", []),
        "window_count": page["window_count"],
        "match_count": page["match_count"],
        "last": page["continuation_token"] is None,
        "complete": page["coverage"]["delivery"].get("complete") is True,
    }


def _text_model(text: str) -> tuple[dict, str | None]:
    """The same model read from the text format, and the page's token."""
    pieces = text.splitlines(keepends=True)
    status = pieces[0].rstrip("\n")
    records: list = []
    other: dict = {}
    in_other = False
    i = 1
    while i < len(pieces):
        line = pieces[i].rstrip("\n")
        i += 1
        if line == "--- other fields":
            in_other = True
            continue
        if in_other:
            key, _, value = line.partition(": ")
            other[key] = value
            continue
        head = _RECORD_HEAD.match(line)
        if head:
            records.append((head.group(2), "decided=false" in (head.group(4) or ""), []))
            continue
        if line.startswith("--- window "):
            records[-1][2].append([line.split(" · ")[0][len("--- window "):], [], 0, ""])
            continue
        if line.startswith("--- match · match_id="):
            records[-1][2][-1][1].append(line.split(" · ")[1][len("match_id="):])
            continue
        if line.startswith("--- location"):
            records[-1][2][-1][2] += 1
            continue
        fragment = _FRAGMENT_HEAD.match(line)
        if fragment:
            # The literal text follows as an escaped body of `chars` characters.
            want, body = int(fragment.group(2)), ""
            while len(body) < want:
                piece = pieces[i]
                i += 1
                body += piece[1:] if piece.startswith("\\") else piece
            records[-1][2][-1][3] += _plain(body[:want])
    found = re.search(r" · (\d+)-(\d+) of (\d+)(\+?)", status) or re.search(
        r" · 0 of (\d+)(\+?)", status)
    total = int(found.group(found.lastindex - 1))
    # `search` takes no `next`: a windows page, like any grep page, ends
    # `next: continuation_token=<token>`.
    token = re.search(r" · next: continuation_token=(\S+)", status)
    model = {
        "records": [(node, undecided, [tuple(w) for w in windows])
                    for node, undecided, windows in records],
        "total_count": total,
        "lower_bound": found.group(found.lastindex) == "+",
        "undecided": json.loads(other.get("undecided_records", "[]")),
        "window_count": int(other["window_count"]),
        "match_count": int(other["match_count"]),
        "last": token is None,
        # D8 (read-passage-rows-3666): the ending is the last fact.
        "complete": status.split(" · ")[-1] == "complete",
    }
    if not model["last"]:
        assert "bound reached (" in status, status
    # The ending is the one statement of delivery: no separate fact.
    assert " · delivery " not in status, status
    assert " · next: " not in status or token is not None, status
    if model["undecided"]:
        # An undecided record is said as such on every page, the delivered
        # last page too, never as a rank cap.
        count = len(model["undecided"])
        assert f" · {count} undecided · " in status, status
        assert "rank cap" not in status, status
    return model, (token.group(1).strip('"') if token else None)


def _merge(models: list[dict]) -> dict:
    """Join a walk's pages: a record or window split across pages is one."""
    merged: dict = {}
    order: list = []
    for model in models:
        for node, undecided, windows in model["records"]:
            if node not in merged:
                order.append(node)
                merged[node] = {"undecided": undecided, "windows": {}, "order": []}
            for window_id, matches, locations, text in windows:
                slot = merged[node]["windows"].setdefault(window_id, [[], 0, ""])
                if window_id not in merged[node]["order"]:
                    merged[node]["order"].append(window_id)
                slot[0] += matches
                slot[1] += locations
                slot[2] += text
    return {"order": order, "records": merged}


async def _walk(served, call: dict, fmt: str, budget: int) -> tuple[list[dict], dict]:
    token, models = None, []
    while True:
        attempt = dict(call, **({"continuation_token": token} if token else {}))
        scope = result_format_scope("json") if fmt == "json" else contextlib.nullcontext()
        with scope:
            size, text = _wire(await served.call_tool("search", attempt))
            assert size <= budget, (fmt, budget, size)
            again = None
            if token is not None:
                # Every token is retried: the same page comes back.
                again = _wire(await served.call_tool("search", attempt))[1]
        if fmt == "json":
            page = json.loads(text)
            assert page.get("status") != "refused", page
            model, next_token = _json_model(page), page["continuation_token"]
            if again is not None:
                assert _json_model(json.loads(again)) == model
        else:
            assert text.startswith("search ok"), text[:2000]
            model, next_token = _text_model(text)
            if again is not None:
                assert _text_model(again)[0] == model
        models.append(model)
        assert model["complete"] is model["last"], (fmt, budget, len(models))
        if next_token is None:
            break
        token = next_token
        assert len(models) < 400
    return models, _merge(models)


_WALKS = {
    # A decided match, an undecided record (doc:gen: "clad|ding" across an
    # unproven boundary), a 300-match record and one that does not match.
    "mixed": {"query": "cladding",
              "scope": {"records": ["doc:big", "doc:gen", "doc:pos", "doc:zr"]}},
    "passage": {"query": "zirconium", "matching": "passage",
                "scope": {"records": ["doc:big", "doc:pos", "doc:gen", "doc:zr", "doc:sm"]}},
}


@pytest.mark.anyio
@pytest.mark.parametrize("budget", [4096, 16384, 131072])
@pytest.mark.parametrize("walk", sorted(_WALKS))
async def test_text_and_json_walks_carry_the_same_answer(served, walk, budget) -> None:
    """req.read-record-windows.wire: every page of both formats fits the
    budget, every token retries to the same page, the last page is complete
    in both, and both formats deliver the same records, windows, matches,
    literal text, counts and undecided list."""
    call = {"method": "grep", "view": "windows", "context_chars": 200,
            "max_bytes": budget, **_WALKS[walk]}
    json_pages, json_answer = await _walk(served, call, "json", budget)
    text_pages, text_answer = await _walk(served, call, "text", budget)
    assert text_answer == json_answer
    for key in ("total_count", "lower_bound", "undecided", "window_count", "match_count"):
        values = {json.dumps(model[key]) for model in json_pages + text_pages}
        assert len(values) == 1, (key, values)
    assert json_pages[-1]["complete"] and text_pages[-1]["complete"]
    records = json_answer["records"]
    big = records["doc:big"]
    matches = [m for slot in big["windows"].values() for m in slot[0]]
    assert len(matches) == len(set(matches)) == 300
    assert "".join(slot[2] for slot in big["windows"].values()) == BIG
    if walk == "mixed":
        assert json_pages[0]["undecided"] == ["doc:gen"]
        assert json_pages[0]["total_count"] == 2 and json_pages[0]["lower_bound"]
        assert records["doc:gen"]["undecided"] and not records["doc:gen"]["windows"]
        assert set(records) == {"doc:big", "doc:gen", "doc:pos"}
    else:
        assert json_pages[0]["undecided"] == [] and not json_pages[0]["lower_bound"]
        assert set(records) == {"doc:big", "doc:pos", "doc:gen", "doc:zr"}
        assert json_pages[0]["total_count"] == 4
    if budget == 131072:
        assert len(json_pages) == 1
    else:
        assert len(json_pages) > 1 and len(text_pages) > 1


@pytest.mark.anyio
@pytest.mark.parametrize("fmt", ["json", "text"])
@pytest.mark.parametrize("serve, replay", [("normal", "unavailable"), ("unavailable", "normal")],
                         ids=["unavailable_on_replay", "unavailable_on_first_serve"])
async def test_a_freshness_failure_never_moves_a_retried_window_page(
    served, monkeypatch, fmt, serve, replay,
) -> None:
    """A token is issued under normal freshness stamps; its page is served
    once and retried, one of the two with the freshness read failing. The
    unavailable blocks are larger than fresh ones (in text a fresh block
    prints nothing, an unavailable one prints notice lines), yet both serves
    are the same page: the reserve already holds the unavailable shapes.

    The JSON unavailable block names the deployment, so it grows with the
    name; this fixture's `default` is short, so the shape is served with a
    long deployment name (as a path-named deployment has), unchanged
    otherwise. The text notices do not name it."""
    from okg.substrate.mcp import freshness
    from okg.substrate.mcp import server as server_module

    real_unavailable = freshness.unavailable_freshness

    def long_named(*, deployment, **kwargs):
        return real_unavailable(deployment=f"{deployment}/{'deployments-root/' * 8}", **kwargs)

    monkeypatch.setattr(freshness, "unavailable_freshness", long_named)

    budget = 4096
    arguments = {"query": "zirconium", "method": "grep", "view": "windows",
                 "scope": {"record": "doc:big"}, "context_chars": 200, "max_bytes": budget}
    original = server_module._stamp_freshness_outcomes
    state = {"now": "normal", "failed": 0}

    def outcomes(*args, **kwargs):
        if state["now"] == "unavailable":
            state["failed"] += 1
            raise RuntimeError("freshness query failed")
        return original(*args, **kwargs)

    monkeypatch.setattr(server_module, "_stamp_freshness_outcomes", outcomes)

    async def call(call_arguments: dict, stamps: str):
        state["now"], state["failed"] = stamps, 0
        scope = result_format_scope("json") if fmt == "json" else contextlib.nullcontext()
        with scope:
            size, text = _wire(await served.call_tool("search", call_arguments))
        assert size <= budget, size
        assert (state["failed"] > 0) == (stamps == "unavailable")
        if fmt == "json":
            page = json.loads(text)
            assert page.get("status") != "refused", page
            if stamps == "unavailable":
                assert page["publish_freshness"]["status"] == "unavailable", page["publish_freshness"]
            return page["results"], page["continuation_token"]
        assert text.startswith("search ok"), text[:2000]
        # D8 (read-passage-rows-3666): publish freshness, unavailable or
        # not, is no read reply's notice; the JSON form keeps the block.
        assert "publish freshness" not in text, text[:2000]
        return _text_model(text)

    _first, token = await call(arguments, "normal")
    assert token, "page one must be cut"
    call_arguments = dict(arguments, continuation_token=token)
    page, next_token = await call(call_arguments, serve)
    assert next_token, "page two must be cut, or nothing is tested"
    again, again_token = await call(call_arguments, replay)
    assert again == page
    assert again_token == next_token


def test_a_select_keeps_what_says_part_of_the_record_was_not_read(served) -> None:
    """#3666 step 3a: through the served tool a `select` cuts a windows row.
    A record with a missing passage keeps `continuity` and `source_coverage`
    whatever is selected: without them the windows read as the whole answer."""
    import anyio

    from tests.substrate.mcp._result_payload import call_json, payload

    arguments = {"query": "seam", "method": "grep", "view": "windows",
                 "scope": {"records": ["doc:miss"]}, "max_bytes": 131072}

    async def run() -> tuple:
        return (
            await call_json(served, "search", arguments),
            await call_json(served, "search", {**arguments, "select": ["windows"]}),
        )

    whole, cut = (payload(result) for result in anyio.run(run))
    (full,) = whole["results"]
    (row,) = cut["results"]
    assert full["continuity"] == "unknown" and full["source_coverage"] == "partial"
    assert row["continuity"] == "unknown" and row["source_coverage"] == "partial"
    assert row["windows"] == full["windows"] and row["node_id"] == "doc:miss"
    # Cut all the same: what was not selected and says nothing of coverage is gone.
    assert "match_count" in full and "match_count" not in row
    assert "adjusted" not in cut
