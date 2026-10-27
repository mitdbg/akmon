"""Result references on a real publish (#3222 slices 2 and 3).

PACT ``read-result-references-3222-consumers`` and, for ``view="metadata"``,
its ``summary`` and the 300-search load check,
``read-result-references-3222-summary``. One published private fixture
(documents folded from their chunks, a hidden pair, a redacted one), driven
through ``FastMCP.call_tool`` in the JSON and text result formats:

- a lexical record search with ``retain=true`` freezes its ranked members
  (node id, rank, location) and pages them through ``map(input={"ref"})``;
- every other producer form refuses by name before any SQL;
- ``filter``, ``map`` and ``aggregate`` read only the stored members at the
  pin and equal the same reader's explicit node-id call;
- the validation order refuses by name before any member read;
- every page stays inside its byte budget.

Hiding and redaction run under Nomos ``enforce``; ``audit`` never hides a row.
"""
from __future__ import annotations

import contextlib
import contextvars
import dataclasses
import json
import re
import threading
import time
import uuid
from typing import Any, Iterator

import psycopg
import pytest

from okg.substrate.attributes import (
    materialize_effective_attributes_for_generation,
    record_attribute_observation,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.mcp import continuation, result_references
from okg.substrate.mcp.search_views import CARDS_LIMIT
from okg.substrate.mcp.result_references import MAX_MEMBERS, ResultReferenceStore
from okg.substrate.mcp.result_text import result_format_scope
from okg.substrate.mcp.server import build_server
from okg.substrate.nomos import activate_policy_bundle, compile_policy_graph
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import (
    SubtypeSpec,
    bind_test_catalog_writer,
    seed_catalog,
)
from tests.substrate.mcp._result_payload import payload as json_payload
from tests.substrate.mcp._result_payload import result_text

pytestmark = pytest.mark.postgres

TERM = "zirconium"
#: Only in the hidden records.
HIDDEN_TERM = "hafnium"
#: In no record at all.
ABSENT_TERM = "yttrium"
READER = "subject:mcp-reader"
DOCS = [f"doc:{i:02d}" for i in range(14)]
HIDDEN = ["doc:s0", "doc:s1"]
MASKED = "doc:m"
#: A single-user reader with no authority argument.
READER_KEY = continuation.reader_key(None, None, False)
#: The hydration statement's member parameter, and the digest's.
MEMBER_PARAM = "rr_member_ids"
DIGEST_PARAM = "rr_digest_ids"
_NEXT_CALL = re.compile(r'next: map\(input=\{"ref": "([^"]+)"\}, offset=(\d+)\)')


# ---------------------------------------------------------------- fixture


def _status(i: int) -> dict[str, Any]:
    # Every third record carries no status: the attribute is absent there.
    return {0: {"status": "open"}, 1: {"status": "closed"}, 2: {}}[i % 3]


def _record(doc: str, i: int, text: str, cls: str = "internal",
            readable_chunk: str | None = None, **attrs: Any) -> list:
    """A document and its chunks, all of class `cls`; `readable_chunk` adds
    a last chunk of class "internal" (readable whatever `cls` is)."""
    facts: list = [NodeFact(
        doc, "document",
        {"classification": cls, "text": f"Record {i} summary.", "path": f"{doc}.md",
         "team": "a" if i % 2 else "b", **attrs},
        {"id": doc}, {"v": 1},
    )]
    bodies = [("Opening words.", cls), (text, cls)]
    if readable_chunk is not None:
        bodies.append((readable_chunk, "internal"))
    for ordinal, (body, chunk_cls) in enumerate(bodies):
        chunk = f"chunk:{doc}:{ordinal}"
        facts += [
            NodeFact(chunk, "document_chunk", {"classification": chunk_cls, "text": body},
                     {"id": chunk}, {"v": 1}),
            EdgeFact(chunk, doc, "member_of",
                     {"classification": cls, "chunk_ordinal": ordinal,
                      "chunk_total": len(bodies)},
                     source_record_id={"id": f"{chunk}-m"}, source_revision={"v": 1}),
            EdgeFact(doc, chunk, "contains", {"classification": cls},
                     source_record_id={"id": f"{chunk}-c"}, source_revision={"v": 1}),
        ]
    return facts


def _facts() -> list:
    facts: list = []
    for i, doc in enumerate(DOCS):
        # A different term frequency per record gives a stable ranking.
        words = " ".join([TERM] * (1 + i % 5))
        facts += _record(doc, i, f"{words} cladding failed at weld {i}.", **_status(i))
    for i, doc in enumerate(HIDDEN):
        facts += _record(doc, 20 + i, f"{TERM} {HIDDEN_TERM} secret finding {i}.",
                         cls="secret", status="open")
    # The redacted record matches in a masked chunk (TERM three times, the
    # better score) and in a readable one (TERM once).
    facts += _record(MASKED, 30, f"{TERM} {TERM} {TERM} cladding masked finding.",
                     cls="masked", readable_chunk=f"{TERM} plain note.", status="open")
    return facts


def _policy(*extra: dict[str, Any]) -> dict[str, Any]:
    return {
        "policy_bundle_id": f"nomos:test:refs:{uuid.uuid4().hex[:8]}",
        "policy_version": "v1",
        "default_effect": "allow",
        "rules": [
            {"rule_id": "hide_secret", "effect": "deny", "priority": 300,
             "subjects": [READER], "actions": ["graph.read.mcp"],
             "match": {"classification": "secret"}},
            {"rule_id": "redact_masked", "effect": "redact", "priority": 300,
             "subjects": [READER], "actions": ["graph.read.mcp"],
             "match": {"classification": "masked"},
             "obligations": [{"type": "redaction"}, {"type": "audit"}]},
            {"rule_id": "allow_read", "effect": "allow", "priority": 100,
             "subjects": [READER], "actions": ["graph.read.mcp"]},
            *extra,
        ],
    }


def _activate(dsn: str, *extra: dict[str, Any], bootstrap: bool = True) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        activate_policy_bundle(
            conn, deployment_name="default", bundle=compile_policy_graph(_policy(*extra)),
            scope="deployment:default", activated_by="test:operator", bootstrap=bootstrap,
        )


def _publish(dsn: str, facts: list) -> str:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog(
            conn,
            subtypes=[
                SubtypeSpec("document", metadata={
                    "searchable_text_fields": ["text"], "path_from": "path",
                    "passage_rollup": {"edge_type": "contains",
                                       "child_subtype": "document_chunk"},
                    "pii_classes": {"classification": "non_pii", "status": "free_text",
                                    "text": "free_text"},
                }),
                SubtypeSpec("document_chunk", metadata={
                    "searchable_text_fields": ["text"],
                    "pii_classes": {"classification": "non_pii", "text": "free_text"},
                }),
            ],
            narrowings=[
                ("document", "contains", "document_chunk"),
                ("document_chunk", "member_of", "document"),
            ],
        )
        ChronosGraphStore.from_connection(conn).apply_facts(
            facts, source="default", strict_endpoints=True,
        )
    _activate(dsn)
    published = Publisher(
        dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default"),
    ).cycle()
    assert published["status"] == "published", published
    return str(published["generation_id"])


@pytest.fixture
def store() -> Iterator[ResultReferenceStore]:
    fresh = ResultReferenceStore()
    previous = result_references.install_store(fresh)
    try:
        yield fresh
    finally:
        result_references.install_store(previous)


class _World(dict):
    """The fixture's facts; its repr never prints the DSN (failure reports)."""

    def __repr__(self) -> str:
        return f"<world gen={self.get('gen')!r}>"


#: The effective-view sources a consumer can read: the per-generation
#: snapshot (``OKG_IVM_INTERVAL_VIEW=0``) and the production default, the
#: shared interval table. The member restriction in
#: ``_effective_view_runtime_source`` must run on both (review M4).
SOURCES = ["snapshot", "interval"]


def _use_source(monkeypatch, source: str) -> None:
    if source == "snapshot":
        monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "0")
    else:
        monkeypatch.delenv("OKG_IVM_INTERVAL_VIEW", raising=False)


@pytest.fixture
def world(
    request, clean_runtime: str, okg_mcp_can_log_in: str, monkeypatch, store,
) -> dict[str, Any]:
    # The snapshot source unless a test parametrizes `world` indirectly.
    source = getattr(request, "param", "snapshot")
    _use_source(monkeypatch, source)
    gen = _publish(clean_runtime, _facts())
    return _World(dsn=clean_runtime, gen=gen, store=store, source=source)


@contextlib.contextmanager
def _server(world: dict[str, Any], monkeypatch, *, nomos: str = "enforce") -> Iterator[Any]:
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", nomos)
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    mcp = build_server(dsn=world["dsn"], deployment_name="default")
    try:
        yield mcp
    finally:
        mcp._okg_pool.close()  # type: ignore[attr-defined]
        if mcp._okg_write_pool:  # type: ignore[attr-defined]
            mcp._okg_write_pool.close()  # type: ignore[attr-defined]


# ---------------------------------------------------------------- calls


def _serve(mcp, tool: str, arguments: dict[str, Any], fmt: str = "json") -> Any:
    import anyio

    async def drive() -> Any:
        with result_format_scope(fmt):
            return await mcp.call_tool(tool, arguments)

    return anyio.run(drive)


def _ok(mcp, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
    result = _serve(mcp, tool, arguments)
    body = json_payload(result)
    assert result.isError is False, body
    return body


def _refused(mcp, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
    result = _serve(mcp, tool, arguments)
    body = json_payload(result)
    assert result.isError is True, body
    return body["error"]


def _retain(mcp, **arguments: Any) -> dict[str, Any]:
    return _ok(mcp, "search", {"query": TERM, "retain": True, **arguments})


def _members(world: dict[str, Any], ref_id: str, reader: str = READER_KEY) -> list[dict]:
    return [dict(m) for m in world["store"].lookup(ref_id, reader_key=reader).members]


def _ids(rows: list[dict[str, Any]]) -> list[str]:
    return [str(row["node_id"]).strip() for row in rows]


def _all_pages(mcp, tool: str, arguments: dict[str, Any], *, limit: int) -> list[dict]:
    pages: list[dict] = []
    offset: int | None = 0
    while offset is not None:
        page = _ok(mcp, tool, {**arguments, "limit": limit, "offset": offset})
        pages.append(page)
        offset = page["next_offset"]
        assert len(pages) <= 40, "paging did not end"
    return pages


#: Set while `_statements` records. A tool call runs in a worker thread in a
#: copy of its caller's context (issue #3493), so it sees this; a background
#: writer thread started elsewhere does not.
_RECORDING: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "test_result_references_recording", default=False,
)


@contextlib.contextmanager
def _statements(monkeypatch) -> Iterator[list[tuple[str, Any, float]]]:
    """Every statement the calls made inside this block execute:
    (sql, params, seconds)."""
    seen: list[tuple[str, Any, float]] = []
    original = psycopg.Cursor.execute
    lock = threading.Lock()

    def execute(self, query, params=None, *args, **kwargs):
        started = time.perf_counter()
        try:
            return original(self, query, params, *args, **kwargs)
        finally:
            if _RECORDING.get():
                with lock:
                    seen.append((str(query), params, time.perf_counter() - started))

    token = _RECORDING.set(True)
    try:
        with monkeypatch.context() as patch:
            patch.setattr(psycopg.Cursor, "execute", execute)
            yield seen
    finally:
        _RECORDING.reset(token)


def _reads(seen: list[tuple[str, Any, float]]) -> list[str]:
    """Statements that read: every call also writes its own trace, telemetry
    and call-log rows (INSERTs), refused or not, and the pool probes a
    connection with an empty statement."""
    return [
        sql for sql, _, _ in seen
        if sql.lstrip().upper().startswith(("SELECT", "WITH"))
    ]


def _member_statements(seen: list[tuple[str, Any, float]]) -> list[float]:
    return [s for _, p, s in seen if isinstance(p, dict) and MEMBER_PARAM in p]


# ================================================================ producer


@pytest.mark.parametrize("rank_by", ["records", "passages"])
@pytest.mark.parametrize("fmt", ["json", "text"])
def test_retained_search_freezes_its_ranked_members(world, monkeypatch, rank_by, fmt) -> None:
    arguments = {"query": TERM, "retain": True, "limit": 8, "rank_by": rank_by}
    if rank_by == "passages":
        # Passage ranking rolls chunks up into the parent subtype it names.
        arguments["subtype_filter"] = "document"
    with _server(world, monkeypatch) as mcp:
        served = _serve(mcp, "search", arguments, fmt)
        page = _ok(mcp, "search", arguments)
    assert served.isError is False
    reference = page["result_reference"]
    assert set(reference) == {"id", "parent", "order", "expires_at"}
    assert reference["parent"] is None and reference["order"] == "rank"
    members = _members(world, reference["id"])
    assert len(members) == 8
    # Frozen order equals the delivered prefix; members hold no text or attrs.
    assert [m["node_id"] for m in members] == _ids(page["results"])
    assert [m["rank"] for m in members] == list(range(1, 9))
    assert all(set(m) == {"node_id", "rank", "location"} for m in members)
    # Paging the search is replaced by paging the reference (H1).
    assert page["next_offset"] is None
    if fmt == "text":
        # The text form names the reference on its own line, near the top.
        text = result_text(served)
        line = next(
            (ln for ln in text.splitlines()[:4] if ln.startswith("reference r1.")), None,
        )
        assert line is not None, text
        served_id = line.split()[1]
        served_members = _members(world, served_id)
        assert [m["node_id"] for m in served_members] == [m["node_id"] for m in members]


@pytest.mark.parametrize("fmt", ["json", "text"])
def test_a_byte_fitted_reference_keeps_limit_members_and_next_pages_it(
    world, monkeypatch, fmt,
) -> None:
    with _server(world, monkeypatch) as mcp:
        # A JSON card on this fixture weighs about 1 KB and its page-level
        # blocks about 3 KB, so the JSON page is cut at 6 KiB, the text one
        # at 4 KiB; either way fewer than 12 rows are delivered.
        budget = 4096 if fmt == "text" else 6144
        served = _serve(mcp, "search", {"query": TERM, "retain": True, "limit": 12,
                                        "max_bytes": budget}, fmt)
        assert served.isError is False, result_text(served)[:1200]
        if fmt == "json":
            page = json_payload(served)
            ref_id = page["result_reference"]["id"]
            delivered = _ids(page["results"])
            assert page["next_offset"] is None
            assert page["next_offset_reason"] == "page_reference"
            offset = len(delivered)
        else:
            text = result_text(served)
            match = _NEXT_CALL.search(text)
            if match is not None:
                ref_id, offset = match.group(1), int(match.group(2))
                delivered = [m["node_id"] for m in _members(world, ref_id)][:offset]
            else:
                # #3666 (read-passage-rows): a text search row is a head line
                # and a short passage, so all 12 rows can fit 4 KiB and the
                # page needs no `next:`; it then delivers every member, in
                # the reference's order, and map from there reads nothing.
                from okg.substrate.mcp.result_text import result_cards

                line = re.search(r"^reference (\S+) ", text, re.M)
                assert line is not None, text
                ref_id = line.group(1)
                delivered = [card.ident for card in result_cards(text)]
                offset = len(delivered)
                assert offset == 12, text
        members = [m["node_id"] for m in _members(world, ref_id)]
        assert len(members) == 12
        assert 0 < len(delivered) <= 12
        if fmt == "json":
            assert len(delivered) < 12, "the 6144-byte page was not cut"
        assert delivered == members[: len(delivered)]
        rest: list[str] = []
        next_offset: int | None = offset
        while next_offset is not None:
            following = _ok(mcp, "map", {"input": {"ref": ref_id}, "offset": next_offset,
                                         "projection": ["node_id"], "max_bytes": 4096})
            rest += _ids(following["items"])
            assert following["total_count"] == 12
            assert following["total_count_basis"] == "exact"
            next_offset = following["next_offset"]
    assert delivered + rest == members


#: The two producers of a reference from a lexical record search: a
#: retained cards page and a metadata page (slice 3).
PRODUCERS = {"retain": {"retain": True}, "metadata": {"view": "metadata"}}


@pytest.mark.parametrize("producer", sorted(PRODUCERS))
@pytest.mark.parametrize("rank_by", ["records", "passages"])
def test_a_masked_chunk_never_supplies_the_location(
    world, monkeypatch, rank_by, producer,
) -> None:
    """Under `records` the location comes from the folded record's winning
    passage, already read through the reader's filter; under `passages` from
    the best chunk, which the rollup reads through that filter and
    `_reference_members` reads through it again. `doc:m`'s best-scoring
    match is in a masked chunk, which matches nothing (#3377); its readable
    chunk makes it a member, located there. Mutation check (review M2):
    with the readable-text check removed, the masked chunk wins and the
    location lands in it (or, read through the filter, is lost)."""
    arguments: dict[str, Any] = {"query": TERM, "limit": 50, "rank_by": rank_by,
                                 **PRODUCERS[producer]}
    if rank_by == "passages":
        arguments["subtype_filter"] = "document"
    with _server(world, monkeypatch) as mcp:
        page = _ok(mcp, "search", arguments)
    members = {m["node_id"]: m for m in _members(world, page["result_reference"]["id"])}
    # The redacted record is a member through its readable chunk, and its
    # location is there, never in the higher-scoring masked chunk.
    assert MASKED in members
    assert members[MASKED]["location"] is not None
    assert members[MASKED]["location"]["chunk"] == f"chunk:{MASKED}:2"
    located = list(members.values())
    assert located and all(m["location"] is not None for m in located)
    assert all(m["location"]["chunk"].startswith(f"chunk:{m['node_id']}:") for m in located)
    assert "masked finding" not in json.dumps(page)


def test_hidden_records_are_never_members_and_leave_no_missing_signal(world, monkeypatch) -> None:
    with _server(world, monkeypatch) as mcp:
        page = _retain(mcp, limit=50)
        ref_id = page["result_reference"]["id"]
        members = [m["node_id"] for m in _members(world, ref_id)]
        assert not set(HIDDEN) & set(members)
        assert len(members) == len(DOCS) + 1
        read = _ok(mcp, "map", {"input": {"ref": ref_id}, "projection": ["node_id"],
                                "limit": 50})
    assert _ids(read["items"]) == members
    assert read["total_count"] == len(members)
    assert HIDDEN_TERM not in json.dumps(read)


ORPHAN = "doc:orphan"
ORPHAN_CHUNK = f"chunk:{ORPHAN}:1"
#: Only in the orphan's records, so a fresh publish ranks it alone.
ORPHAN_TERM = "tantalum"


def _orphan_facts() -> list:
    """Twelve readable records and one whose record is hidden while its
    chunk is readable: the records fold keeps that chunk as its own row
    (no readable record to fold into, `_unfolded_passage_ids`)."""
    facts: list = []
    for i in range(12):
        facts += _record(f"doc:t{i:02d}", i, f"{ORPHAN_TERM} note {i}.", **_status(i))
    facts.append(NodeFact(
        ORPHAN, "document",
        {"classification": "secret", "text": f"Record 40 {HIDDEN_TERM} summary.",
         "path": f"{ORPHAN}.md", "team": "a"},
        {"id": ORPHAN}, {"v": 1},
    ))
    for ordinal, body in enumerate(["Opening words.", " ".join([ORPHAN_TERM] * 8)]):
        chunk = f"chunk:{ORPHAN}:{ordinal}"
        facts += [
            NodeFact(chunk, "document_chunk", {"classification": "internal", "text": body},
                     {"id": chunk}, {"v": 1}),
            EdgeFact(chunk, ORPHAN, "member_of",
                     {"classification": "internal", "chunk_ordinal": ordinal,
                      "chunk_total": 2},
                     source_record_id={"id": f"{chunk}-m"}, source_revision={"v": 1}),
            EdgeFact(ORPHAN, chunk, "contains", {"classification": "secret"},
                     source_record_id={"id": f"{chunk}-c"}, source_revision={"v": 1}),
        ]
    return facts


@pytest.mark.parametrize("fmt", ["json", "text"])
def test_an_unfolded_passage_on_the_page_is_not_a_member_and_is_disclosed(
    clean_runtime, okg_mcp_can_log_in, monkeypatch, store, fmt,
) -> None:
    """Review M1: a reference stores record rows only (D1). A passage the
    fold left on the page is delivered but not stored, and its count is
    disclosed on the reference; nothing about the hidden record leaks."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "0")
    world = _World(dsn=clean_runtime, gen=_publish(clean_runtime, _orphan_facts()),
                   store=store)
    arguments = {"query": ORPHAN_TERM, "retain": True, "limit": 12}
    with _server(world, monkeypatch) as mcp:
        served = _serve(mcp, "search", arguments, fmt)
        page = _ok(mcp, "search", arguments)
    assert served.isError is False
    delivered = _ids(page["results"])
    assert ORPHAN_CHUNK in delivered, delivered
    reference = page["result_reference"]
    members = _members(world, reference["id"])
    member_ids = [m["node_id"] for m in members]
    assert ORPHAN_CHUNK not in member_ids
    assert member_ids == [node for node in delivered if node != ORPHAN_CHUNK]
    assert [m["rank"] for m in members] == list(range(1, len(members) + 1))
    passages = [row for row in page["results"] if row.get("subtype") == "document_chunk"]
    assert reference["passages_excluded"] == len(passages) == 1
    # Every member was delivered, so the reference has no page to follow.
    assert "next_offset" not in reference
    assert page.get("next_offset_reason") != "page_reference"
    # Nothing of the hidden record is in the reference or the page.
    assert ORPHAN not in json.dumps(reference)
    assert HIDDEN_TERM not in json.dumps(page)
    if fmt == "text":
        text = result_text(served)
        line = next(ln for ln in text.splitlines()[:4] if ln.startswith("reference r1."))
        assert "passages excluded 1" in line, text
        assert HIDDEN_TERM not in text


def test_a_byte_fitted_page_with_an_unfolded_passage_pages_from_its_members(
    clean_runtime, okg_mcp_can_log_in, monkeypatch, store,
) -> None:
    """The reference's next offset counts the members delivered, not the
    rows: a passage row on the page is not a member (review M1)."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "0")
    world = _World(dsn=clean_runtime, gen=_publish(clean_runtime, _orphan_facts()),
                   store=store)
    with _server(world, monkeypatch) as mcp:
        page = _ok(mcp, "search", {"query": ORPHAN_TERM, "retain": True, "limit": 12,
                                   "max_bytes": 6144})
        reference = page["result_reference"]
        members = [m["node_id"] for m in _members(world, reference["id"])]
        delivered = [node for node in _ids(page["results"]) if node in members]
        # The premise: the passage row survives byte fitting, so rows and
        # members differ on the delivered page (re-review M-a).
        assert ORPHAN_CHUNK in _ids(page["results"]), _ids(page["results"])
        assert page["next_offset_reason"] == "page_reference"
        assert reference["next_offset"] == len(delivered) < len(members)
        assert delivered == members[: len(delivered)]
        rest = _all_pages(mcp, "map", {"input": {"ref": reference["id"]},
                                       "projection": ["node_id"]}, limit=50)
        following = _ok(mcp, "map", {"input": {"ref": reference["id"]},
                                     "projection": ["node_id"],
                                     "offset": reference["next_offset"]})
    assert [n for p in rest for n in _ids(p["items"])] == members
    assert delivered + _ids(following["items"]) == members


@pytest.mark.parametrize("fmt", ["json", "text"])
def test_search_keeps_its_own_next_when_every_member_fits(world, monkeypatch, fmt) -> None:
    """Review M5: the reference pages its members, but search's own next page
    (more hits past `limit`) is kept under `search_next`, so a reference that
    holds every member does not read as the whole search."""
    arguments = {"query": TERM, "retain": True, "limit": 3}
    with _server(world, monkeypatch) as mcp:
        served = _serve(mcp, "search", arguments, fmt)
        page = _ok(mcp, "search", arguments)
        plain = _ok(mcp, "search", {"query": TERM, "limit": 3})
    assert served.isError is False
    assert page["next_offset"] is None
    assert "next_offset_reason" not in page
    assert "next_offset" not in page["result_reference"]
    expected = {
        "offset": plain.get("next_offset"),
        "reason": plain.get("next_offset_reason"),
        "remedy": plain.get("next_offset_remedy"),
    }
    assert any(value is not None for value in expected.values()), plain
    assert page["search_next"] == expected
    if fmt == "text":
        text = result_text(served)
        assert "next: none" not in text, text
        if expected["offset"] is not None:
            assert f"next: search offset={expected['offset']} without retain" in text, text
            assert f"search next offset {expected['offset']}" in text, text


def test_a_bad_predicate_on_a_reference_refuses_like_the_explicit_call(
    world, monkeypatch,
) -> None:
    """Review L1: the explicit call's predicate check runs on a reference
    too, before the reference is looked up or any SQL runs. `contains` with
    text on an attribute is served (read-defects-rd-3666); a number operand
    on one attribute is still a form filter refuses."""
    bad = {"op": "contains", "field": "attrs.status", "value": 3}
    with _server(world, monkeypatch) as mcp:
        ref_id = _retain(mcp, limit=5)["result_reference"]["id"]
        explicit = _serve(mcp, "filter", {"input": {"kind": "nodes"}, "predicate": bad})
        with _statements(monkeypatch) as seen:
            by_ref = _serve(mcp, "filter", {"input": {"ref": ref_id}, "predicate": bad})
    assert explicit.isError is True and by_ref.isError is True
    assert json_payload(by_ref)["error"] == json_payload(explicit)["error"]
    assert json_payload(by_ref)["error"]["code"] == "unsupported_operator_form"
    assert _reads(seen) == [], [sql[:80] for sql in _reads(seen)]


def test_a_regex_predicate_on_a_reference_refuses_like_the_explicit_call(
    world, monkeypatch,
) -> None:
    """Re-review M-b: a predicate form operators do not serve (regex on an
    attribute) refuses on a reference with exactly the explicit call's error,
    and no member row is served."""
    bad = {"op": "regex", "field": "attrs.status", "value": "op.*"}
    with _server(world, monkeypatch) as mcp:
        ref_id = _retain(mcp, limit=5)["result_reference"]["id"]
        explicit = _serve(mcp, "filter", {"input": {"kind": "nodes"}, "predicate": bad})
        by_ref = _serve(mcp, "filter", {"input": {"ref": ref_id}, "predicate": bad})
    assert explicit.isError is True and by_ref.isError is True
    ref_error, explicit_error = json_payload(by_ref)["error"], json_payload(explicit)["error"]
    assert ref_error == explicit_error
    assert "items" not in json_payload(by_ref)


_REFUSED_FORMS: dict[str, tuple[dict[str, Any], str, str]] = {
    "semantic": ({"method": "semantic"}, "result_reference_unsupported", "method"),
    "alias": ({"method": "alias"}, "result_reference_unsupported", "method"),
    "identifier_mentions": ({"method": "identifier_mentions"},
                            "result_reference_unsupported", "method"),
    "attrs": ({"query": None, "where": {"op": "eq", "field": "attrs.status",
                                        "value": "open"}},
              "result_reference_unsupported", "method"),
    "grep": ({"method": "grep"}, "result_reference_unsupported", "method"),
    "rank_by_self": ({"rank_by": "self"}, "result_reference_unsupported", "rank_by"),
    "offset": ({"offset": 2}, "result_reference_unsupported", "argument"),
    # Slice 3: view="metadata" is a producer (slice 2's refusal of it is
    # gone); a delivery argument it cannot honour, and a summary without it,
    # refuse by name.
    "metadata_select": ({"retain": False, "view": "metadata", "select": ["node_id"]},
                        "result_reference_unsupported", "argument"),
    "metadata_max_chars": ({"retain": False, "view": "metadata", "max_chars": 200},
                           "result_reference_unsupported", "argument"),
    "metadata_over_200": ({"retain": False, "view": "metadata", "limit": MAX_MEMBERS + 1},
                          "result_reference_capacity", "reference_members"),
    "summary_without_metadata": ({"retain": False, "summary": ["subtype"]},
                                 "result_reference_unsupported", "argument"),
    "summary_with_retain_cards": ({"summary": ["subtype"]},
                                  "result_reference_unsupported", "argument"),
    # Lead decision on review MED 3: identity fields, a repeated field and
    # a field named `count` refuse before any SQL.
    **{
        f"summary_{name}": ({"retain": False, "view": "metadata", "summary": by},
                            "result_reference_unsupported", "argument")
        for name, by in (("node_id", ["node_id"]), ("handle", ["handle"]),
                         ("title", ["subtype", "attrs.title"]),
                         ("repeated", ["attrs.status", "attrs.status"]),
                         ("count", ["count"]))
    },
    "over_200": ({"limit": MAX_MEMBERS + 1}, "result_reference_capacity",
                 "reference_members"),
}
#: The argument each `argument` refusal above names.
_REFUSED_ARGUMENT = {
    "offset": "offset", "metadata_select": "select", "metadata_max_chars": "max_chars",
    "summary_without_metadata": "summary", "summary_with_retain_cards": "summary",
    "summary_node_id": "summary", "summary_handle": "summary", "summary_title": "summary",
    "summary_repeated": "summary", "summary_count": "summary",
}


@pytest.mark.parametrize("form", sorted(_REFUSED_FORMS))
def test_every_other_producer_form_refuses_by_name_before_any_sql(
    world, monkeypatch, form,
) -> None:
    arguments, code, reason = _REFUSED_FORMS[form]
    with _server(world, monkeypatch) as mcp:
        with _statements(monkeypatch) as seen:
            error = _refused(mcp, "search", {"query": TERM, "retain": True, **arguments})
    assert (error["code"], error["data"]["reason"]) == (code, reason), error
    if form in _REFUSED_ARGUMENT:
        assert error["data"]["argument"] == _REFUSED_ARGUMENT[form], error
    assert _reads(seen) == [], [sql[:80] for sql in _reads(seen)]
    assert world["store"].count() == 0


def test_a_passage_unit_search_refuses_retain(world, monkeypatch) -> None:
    with _server(world, monkeypatch) as mcp:
        error = _refused(mcp, "search", {"query": TERM, "retain": True,
                                         "subtype_filter": "document_chunk"})
    assert error["code"] == "result_reference_unsupported"
    assert error["data"]["reason"] == "argument"
    assert world["store"].count() == 0



# ================================================================ consumers

_PREDICATES = {
    "eq": {"op": "eq", "field": "attrs.status", "value": "open"},
    "not": {"op": "not", "clause": {"op": "eq", "field": "attrs.status", "value": "open"}},
    "exists": {"op": "exists", "field": "attrs.status"},
}


def _explicit(tool: str, ids: list[str], case: str) -> dict[str, Any]:
    by_id = {"op": "in", "field": "node_id", "values": ids}
    if tool == "filter":
        return {"input": {"kind": "nodes"}, "limit": 50,
                "predicate": {"op": "and", "clauses": [by_id, _PREDICATES[case]]}}
    if tool == "map":
        return {"input": {"kind": "nodes", "filter": by_id}, "limit": 50,
                "projection": ["node_id", "attrs.status", "attrs.team"]}
    return {"input": {"kind": "nodes", "filter": by_id}, "by": ["attrs.status"],
            "metrics": ["count"], "limit": 50}


def _by_reference(tool: str, ref_id: str, case: str) -> dict[str, Any]:
    if tool == "filter":
        return {"input": {"ref": ref_id}, "limit": 50, "predicate": _PREDICATES[case]}
    if tool == "map":
        return {"input": {"ref": ref_id}, "limit": 50,
                "projection": ["node_id", "attrs.status", "attrs.team"]}
    return {"input": {"ref": ref_id}, "by": ["attrs.status"], "metrics": ["count"],
            "limit": 50}


def _answer(mcp, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
    """The facts a reader can learn: rows (by id), counts and paging, or the
    refusal's code."""
    result = _serve(mcp, tool, arguments)
    body = json_payload(result)
    if result.isError:
        return {"refused": body["error"]["code"]}
    rows = body.get("items", body.get("groups"))
    key = (lambda r: str(r.get("node_id")).strip()) if tool != "aggregate" else (
        lambda r: json.dumps(r, sort_keys=True, default=str))
    return {
        "rows": sorted((json.dumps(r, sort_keys=True, default=str) for r in rows), key=str),
        "ids": sorted(key(r) for r in rows),
        "total_count": body.get("total_count"),
        "total_count_basis": body.get("total_count_basis"),
        "truncated": body.get("truncated"),
        "next_offset": body.get("next_offset"),
    }


_CONSUMER_CASES = [
    ("filter", "eq"), ("filter", "not"), ("filter", "exists"), ("map", "-"),
    ("aggregate", "-"),
]


@pytest.mark.parametrize("world", SOURCES, indirect=True)
@pytest.mark.parametrize("nomos", ["off", "enforce"])
@pytest.mark.parametrize("tool,case", _CONSUMER_CASES)
def test_a_reference_equals_the_readers_explicit_id_call(
    world, monkeypatch, nomos, tool, case,
) -> None:
    with _server(world, monkeypatch, nomos=nomos) as mcp:
        ref_id = _retain(mcp, limit=50)["result_reference"]["id"]
        ids = [m["node_id"] for m in _members(world, ref_id)]
        by_ref = _answer(mcp, tool, _by_reference(tool, ref_id, case))
        explicit = _answer(mcp, tool, _explicit(tool, ids, case))
    assert by_ref == explicit
    if nomos == "enforce":
        assert MASKED in ids and not set(HIDDEN) & set(ids)
    if tool == "aggregate" and nomos == "off":
        # The group of members with no status is counted, not dropped.
        assert any('"attrs.status": null' in row for row in by_ref["rows"]), by_ref


def test_a_reference_from_a_hidden_match_pages_like_one_from_an_absent_match(
    world, monkeypatch,
) -> None:
    """Lane B's parity case: a search whose only match is hidden must give a
    reference that pages and counts exactly like one whose match is absent."""
    with _server(world, monkeypatch) as mcp:
        hidden = _ok(mcp, "search", {"query": HIDDEN_TERM, "retain": True})
        absent = _ok(mcp, "search", {"query": ABSENT_TERM, "retain": True})
        pages = {}
        for name, page in (("hidden", hidden), ("absent", absent)):
            ref_id = page["result_reference"]["id"]
            pages[name] = [
                {k: v for k, v in _answer(mcp, tool, _by_reference(tool, ref_id, "eq")).items()}
                for tool in ("map", "filter")
            ]
    assert pages["hidden"] == pages["absent"]
    assert _members(world, hidden["result_reference"]["id"]) == []


@pytest.mark.parametrize("world", SOURCES, indirect=True)
def test_reference_paging_covers_every_member_once_in_rank_order(world, monkeypatch) -> None:
    with _server(world, monkeypatch) as mcp:
        ref_id = _retain(mcp, limit=13)["result_reference"]["id"]
        members = [m["node_id"] for m in _members(world, ref_id)]
        pages = _all_pages(mcp, "map", {"input": {"ref": ref_id}, "projection": ["node_id"]},
                           limit=4)
        filtered = _all_pages(mcp, "filter", {"input": {"ref": ref_id},
                                              "predicate": _PREDICATES["eq"]}, limit=2)
    seen = [node for page in pages for node in _ids(page["items"])]
    assert seen == members
    assert all(page["total_count"] == len(members) for page in pages)
    assert [page["truncated"] for page in pages] == [True, True, True, False]
    matching = [node for page in filtered for node in _ids(page["items"])]
    assert matching == [m for m in members if m in matching]
    assert len(set(matching)) == len(matching)


def test_mixed_input_order_by_and_a_conflicting_generation_refuse(world, monkeypatch) -> None:
    with _server(world, monkeypatch) as mcp:
        ref_id = _retain(mcp, limit=5)["result_reference"]["id"]
        mixed = _serve(mcp, "map", {"input": {"ref": ref_id, "kind": "nodes"}})
        assert mixed.isError is True
        with _statements(monkeypatch) as seen:
            ordered = _refused(mcp, "filter", {"input": {"ref": ref_id},
                                               "predicate": {}, "order_by": "node_id"})
            conflict = _refused(mcp, "map", {"input": {"ref": ref_id},
                                             "generation_id": "gen:not-this-one"})
    assert ordered["code"] == "result_reference_unsupported"
    assert ordered["data"]["reason"] == "argument"
    assert conflict["code"] == "result_reference_invalid"
    assert conflict["data"]["reason"] in {"generation_conflict"}
    assert _member_statements(seen) == []


def test_filter_retain_makes_a_child_and_leaves_the_parent(world, monkeypatch) -> None:
    with _server(world, monkeypatch) as mcp:
        parent_id = _retain(mcp, limit=13)["result_reference"]["id"]
        before = _members(world, parent_id)
        child = _ok(mcp, "filter", {"input": {"ref": parent_id}, "retain": True,
                                    "predicate": _PREDICATES["eq"], "limit": 2})
        child_ref = child["result_reference"]
        assert child_ref["parent"] == parent_id
        child_members = [m["node_id"] for m in _members(world, child_ref["id"])]
        parent_order = [m["node_id"] for m in before]
        # The parent members satisfying the predicate, read by the explicit
        # node-id call, not through the reference.
        explicit = _all_pages(mcp, "filter", {
            "input": {"kind": "nodes"},
            "predicate": {"op": "and", "clauses": [
                {"op": "in", "field": "node_id", "values": parent_order},
                _PREDICATES["eq"],
            ]},
        }, limit=50)
        matching = {node for page in explicit for node in _ids(page["items"])}
        _all_pages(mcp, "map", {"input": {"ref": child_ref["id"]}}, limit=1)
        _all_pages(mcp, "map", {"input": {"ref": parent_id}}, limit=3)
        after = _members(world, parent_id)
    expected = [node for node in parent_order if node in matching]
    assert expected and child_members == expected
    assert [m["rank"] for m in _members(world, child_ref["id"])] == [
        m["rank"] for m in before if m["node_id"] in matching
    ]
    assert len(child_members) > len(_ids(child["items"])), "the child holds every match"
    assert after == before


# ================================================================ authority


def _unknown_id(store: ResultReferenceStore) -> str:
    """A well-formed, signed id no reader holds."""
    return store.mint(reader_key="nobody", binding={}, members=[], header_claim=None,
                      multi_user=False).id


@pytest.mark.parametrize("fmt", ["json", "text"])
def test_another_readers_id_answers_byte_for_byte_like_an_unknown_id(
    world, monkeypatch, fmt,
) -> None:
    store = world["store"]
    with _server(world, monkeypatch) as mcp:
        theirs = _ok(mcp, "search", {"query": TERM, "retain": True, "authority": {"note": "b"}})
        theirs_id = theirs["result_reference"]["id"]
        other = _serve(mcp, "map", {"input": {"ref": theirs_id}}, fmt)
        unknown_id = _unknown_id(store)
        unknown = _serve(mcp, "map", {"input": {"ref": unknown_id}}, fmt)
    assert other.isError is True and unknown.isError is True
    assert result_text(other).replace(theirs_id, "ID") == result_text(unknown).replace(
        unknown_id, "ID")
    if fmt == "json":
        assert json_payload(other)["error"]["data"]["reason"] == "unknown"


def test_store_refusals_by_name(world, monkeypatch) -> None:
    clock = [time.time()]
    store = ResultReferenceStore(clock=lambda: clock[0])
    result_references.install_store(store)
    world["store"] = store
    with _server(world, monkeypatch) as mcp:
        ref_id = _retain(mcp, limit=3)["result_reference"]["id"]
        forged = ref_id[:-4] + ("AAAA" if not ref_id.endswith("AAAA") else "BBBB")
        reasons = {
            "malformed": _refused(mcp, "map", {"input": {"ref": "r1.nope"}}),
            "signature_invalid": _refused(mcp, "map", {"input": {"ref": forged}}),
        }
        clock[0] += result_references.LIFETIME_SECONDS + 1
        reasons["expired"] = _refused(mcp, "map", {"input": {"ref": ref_id}})
        clock[0] -= result_references.LIFETIME_SECONDS + 1
        result_references.install_store(ResultReferenceStore())
        reasons["store_lost"] = _refused(mcp, "map", {"input": {"ref": ref_id}})
    for reason, error in reasons.items():
        assert error["code"] == "result_reference_invalid", reason
        assert error["data"]["reason"] == reason, error


def _tamper(store: ResultReferenceStore, ref_id: str, **changes: Any) -> None:
    entry = store._entries[ref_id]  # noqa: SLF001 - test seam: a changed server
    store._entries[ref_id] = dataclasses.replace(  # noqa: SLF001
        entry, binding={**entry.binding, **changes},
    )


@pytest.mark.parametrize("field,value", [
    ("authority", "another-authority-digest"),
    ("policy", "another-policy-fingerprint"),
    ("deployment", "another-deployment"),
    ("database", "another-database"),
    ("branch", "another-branch"),
    ("catalog_version", -1),
])
def test_a_changed_binding_refuses_naming_the_field(world, monkeypatch, field, value) -> None:
    named = {"catalog_version": "catalog", "policy": "authority"}.get(field, field)
    with _server(world, monkeypatch) as mcp:
        ref_id = _retain(mcp, limit=3)["result_reference"]["id"]
        _tamper(world["store"], ref_id, **{field: value})
        with _statements(monkeypatch) as seen:
            error = _refused(mcp, "map", {"input": {"ref": ref_id}})
    assert error["code"] == "result_reference_invalid"
    assert error["data"] == {"reason": "binding_mismatch", "field": named}
    assert _member_statements(seen) == []


def test_a_narrower_policy_for_the_same_reader_is_binding_mismatch_authority(
    world, monkeypatch,
) -> None:
    with _server(world, monkeypatch) as mcp:
        ref_id = _retain(mcp, limit=3)["result_reference"]["id"]
        _activate(world["dsn"], {
            "rule_id": "hide_team_a", "effect": "deny", "priority": 400,
            "subjects": [READER], "actions": ["graph.read.mcp"], "match": {"team": "a"},
        }, bootstrap=False)
        with _statements(monkeypatch) as seen:
            error = _refused(mcp, "map", {"input": {"ref": ref_id}})
    assert error["data"] == {"reason": "binding_mismatch", "field": "authority"}
    assert _member_statements(seen) == []


def test_an_overlay_added_on_a_member_refuses_naming_attributes(world, monkeypatch) -> None:
    with _server(world, monkeypatch) as mcp:
        ref_id = _retain(mcp, limit=3)["result_reference"]["id"]
        member = _members(world, ref_id)[1]["node_id"]
        with psycopg.connect(world["dsn"], autocommit=True) as conn:
            record_attribute_observation(
                conn, target_kind="node", target_id=member, generation_id=world["gen"],
                attr_key="status", value="reopened", producer="test:overlay",
            )
            materialize_effective_attributes_for_generation(conn, generation_id=world["gen"])
        with _statements(monkeypatch) as seen:
            error = _refused(mcp, "filter", {"input": {"ref": ref_id}, "predicate": {}})
    assert error["data"] == {"reason": "binding_mismatch", "field": "attributes"}
    assert _member_statements(seen) == []


def test_a_member_gone_at_the_pin_refuses_member_missing_with_a_count(world, monkeypatch) -> None:
    """The member's graph row is marked deleted (every consumer reads the
    graph rows at the pin since PACT lane-s-storage-cutover), so it is
    unreadable at the same pin; its overlay rows are untouched, so the
    attribute digest still matches and the refusal is step 9, not
    `binding_mismatch`."""
    from okg.substrate.mcp.semantic_readiness import resolve_node_segment

    with _server(world, monkeypatch) as mcp:
        ref_id = _retain(mcp, limit=4)["result_reference"]["id"]
        gone = _members(world, ref_id)[2]["node_id"]
        with psycopg.connect(world["dsn"], autocommit=True) as conn:
            deleted = conn.execute(
                f"UPDATE {resolve_node_segment(conn)} SET deleted = TRUE"
                " WHERE node_id = %s AND deleted = FALSE",
                (gone,),
            ).rowcount
        assert deleted >= 1
        error = _refused(mcp, "map", {"input": {"ref": ref_id}})
    assert error["data"] == {"reason": "member_missing", "count": 1}
    assert gone not in json.dumps(error)


def test_a_compacted_pin_refuses_generation_unavailable(world, monkeypatch) -> None:
    from okg.substrate.errors import GenerationCompacted
    from okg.substrate.mcp import server as server_mod

    with _server(world, monkeypatch) as mcp:
        ref_id = _retain(mcp, limit=3)["result_reference"]["id"]
        original = server_mod.open_session

        def compacted(conn, **kwargs):
            if str(kwargs.get("requested_generation")) == world["gen"]:
                raise GenerationCompacted("the pinned generation was compacted")
            return original(conn, **kwargs)

        monkeypatch.setattr(server_mod, "open_session", compacted)
        error = _refused(mcp, "map", {"input": {"ref": ref_id}})
    assert error["code"] == "result_reference_invalid"
    assert error["data"] == {"reason": "generation_unavailable",
                             "cause": "generation_compacted"}


def test_the_reader_key_is_the_same_at_production_and_at_use(world, monkeypatch) -> None:
    """Production and use compute one key, whichever order the authority's
    keys arrive in; a different authority is a different reader."""
    with _server(world, monkeypatch) as mcp:
        made = _ok(mcp, "search", {"query": TERM, "retain": True,
                                   "authority": {"note": "x", "purpose": "y"}})
        ref_id = made["result_reference"]["id"]
        reordered = _ok(mcp, "map", {"input": {"ref": ref_id},
                                     "authority": {"purpose": "y", "note": "x"}})
        other = _refused(mcp, "map", {"input": {"ref": ref_id}, "authority": {"note": "x"}})
    key = continuation.reader_key({"purpose": "y", "note": "x"}, None, False)
    assert world["store"].lookup(ref_id, reader_key=key)
    assert reordered["total_count"] == len(_members(world, ref_id, key))
    assert other["data"]["reason"] == "unknown"


def test_a_multi_user_producer_passes_the_server_resolved_claim(
    clean_runtime, okg_mcp_can_log_in, monkeypatch, store,
) -> None:
    """Two requests from one person with different authority arguments count
    against one principal; another person's claim reads the id as unknown."""
    from tests.substrate.mcp.test_principal_mapping import (
        PRINCIPAL_A,
        PRINCIPAL_B,
        PRINCIPAL_HEADER,
        SUBJECT_A,
        SUBJECT_B,
        _activate_two_principal_policy,
        _configure_header_identity,
        _multi_user_server,
        _publish_two_source_graph,
        request_headers,
    )

    _publish_two_source_graph(clean_runtime)
    _configure_header_identity(
        clean_runtime, principals={PRINCIPAL_A: SUBJECT_A, PRINCIPAL_B: SUBJECT_B},
    )
    _activate_two_principal_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", SUBJECT_A)
    with _multi_user_server(clean_runtime) as mcp:
        with request_headers({PRINCIPAL_HEADER: PRINCIPAL_B}):
            first = _ok(mcp, "search", {"query": "docs", "retain": True,
                                        "authority": {"note": "1"}})
            second = _ok(mcp, "search", {"query": "docs", "retain": True,
                                         "authority": {"note": "2"}})
        with request_headers({PRINCIPAL_HEADER: PRINCIPAL_A}):
            stranger = _refused(mcp, "map", {"input": {"ref": first["result_reference"]["id"]},
                                             "authority": {"note": "1"}})
    entries = list(store._entries.values())  # noqa: SLF001
    assert len(entries) == 2
    assert entries[0].reader_key != entries[1].reader_key
    principals = {entry.principal_key for entry in entries}
    assert len(principals) == 1 and None not in principals
    assert store.principal_count(next(iter(principals))) == 2
    assert first["result_reference"]["id"] != second["result_reference"]["id"]
    assert stranger["data"]["reason"] == "unknown"


# ================================================================ budgets


def _calls(ref_id: str) -> dict[str, tuple[str, dict[str, Any]]]:
    return {
        "map": ("map", {"input": {"ref": ref_id}, "limit": 50}),
        "filter": ("filter", {"input": {"ref": ref_id}, "predicate": _PREDICATES["exists"],
                              "limit": 50}),
        "aggregate": ("aggregate", {"input": {"ref": ref_id}, "by": ["attrs.team"],
                                    "metrics": ["count"]}),
    }


@pytest.mark.parametrize("budget", [4096, None])
@pytest.mark.parametrize("fmt", ["json", "text"])
def test_every_producer_and_consumer_page_fits_its_budget(world, monkeypatch, budget, fmt) -> None:
    limit_bytes = budget if budget is not None else 65536
    sized = {} if budget is None else {"max_bytes": budget}
    with _server(world, monkeypatch, nomos="off") as mcp:
        # The list view: one JSON search card alone is about 4 KB with its
        # page blocks on this fixture, so cards cannot fit 4096 bytes at all.
        produced = _serve(mcp, "search", {"query": TERM, "retain": True, "limit": 15,
                                          "view": "list", **sized}, fmt)
        served = {"search": produced}
        found = re.search(r"r1\.[A-Za-z0-9_-]+\.[0-9]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+",
                          result_text(produced))
        assert found is not None, result_text(produced)[:300]
        ref_id = found.group(0)
        for name, (tool, arguments) in _calls(ref_id).items():
            served[name] = _serve(mcp, tool, {**arguments, **sized}, fmt)
    for name, result in served.items():
        assert result.isError is False, (name, result_text(result)[:300])
        size = len(result.model_dump_json(by_alias=True, exclude_none=True).encode())
        assert size <= limit_bytes, (name, size)
        # The reference id is counted in every fit: it is in the served bytes.
        assert ref_id in result_text(result) or name != "search"


# ================================================================ measurement


def test_measure_200_member_hydration_and_attribute_digest(
    clean_runtime, okg_mcp_can_log_in, monkeypatch, store, record_property,
) -> None:
    """Statement times for a 200-member reference on this fixture (the CMS
    copy's numbers are reported separately)."""
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "0")
    facts: list = []
    for i in range(220):
        facts += _record(f"doc:bulk:{i:03d}", i, f"{TERM} bulk note {i}.", **_status(i))
    world = _World(dsn=clean_runtime, gen=_publish(clean_runtime, facts), store=store)
    with _server(world, monkeypatch, nomos="off") as mcp:
        with _statements(monkeypatch) as made:
            ref_id = _retain(mcp, limit=MAX_MEMBERS)["result_reference"]["id"]
        assert len(_members(world, ref_id)) == MAX_MEMBERS
        with _statements(monkeypatch) as seen:
            _ok(mcp, "map", {"input": {"ref": ref_id}, "projection": ["node_id"],
                             "limit": 50})
    hydration = _member_statements(seen)
    digests = [s for _, p, s in [*made, *seen] if isinstance(p, dict) and DIGEST_PARAM in p]
    assert hydration and digests
    record_property("hydration_statement_ms", [round(s * 1000, 2) for s in hydration])
    record_property("attribute_digest_statement_ms", [round(s * 1000, 2) for s in digests])


# ================================================================ slice 3: metadata

#: Strings only a delivered row could carry: node id prefixes and excerpt or
#: record text. TERM itself is the caller's own query, echoed back.
_ROW_STRINGS = ("doc:", "chunk:", "cladding", "weld", "summary.", "Opening words",
                "secret finding", "masked finding", ".md")
#: A reference id is `r1.<epoch>.<expiry>.<nonce>.<mac>`; the epoch, nonce and
#: mac are random base64url text, so any of them can begin with "md" and put a
#: ".md" into the reply by chance (the flake in #3997). The reference is the one
#: thing the reply is meant to carry, so it is cut out before the row-text scan;
#: the exact id shape keeps a row string from hiding inside it.
_REFERENCE_ID = re.compile(
    r"r1\.[A-Za-z0-9_-]{8}\.[0-9]{1,12}\.[A-Za-z0-9_-]{16}\.[A-Za-z0-9_-]{22}")


def _without_reference_ids(text: str) -> str:
    return _REFERENCE_ID.sub("<reference>", text)


_METADATA_CASES = {
    "empty": {"query": ABSENT_TERM},
    "bounded": {"query": TERM, "limit": 5},
    "passages": {"query": TERM, "limit": 6, "rank_by": "passages",
                 "subtype_filter": "document"},
}


@pytest.mark.parametrize("case", sorted(_METADATA_CASES))
@pytest.mark.parametrize("fmt", ["json", "text"])
def test_a_metadata_search_returns_no_row_only_the_reference_and_counts(
    world, monkeypatch, case, fmt,
) -> None:
    arguments = {"view": "metadata", **_METADATA_CASES[case]}
    with _server(world, monkeypatch) as mcp:
        served = _serve(mcp, "search", arguments, fmt)
        page = _ok(mcp, "search", arguments)
        cards = _ok(mcp, "search", {**arguments, "view": "cards", "retain": True})
    assert served.isError is False
    text = result_text(served)
    assert _REFERENCE_ID.search(text), text
    scanned = [_without_reference_ids(text), _without_reference_ids(json.dumps(page))]
    for leaked in _ROW_STRINGS:
        for body in scanned:
            assert leaked not in body, (leaked, body)
    assert "results" not in page and "parent_summaries" not in page
    assert page["view"] == "metadata"
    members = _members(world, page["result_reference"]["id"])
    coverage = page["coverage"]
    assert coverage["selection"]["size"] == len(members)
    assert coverage["selection"]["limit"] == arguments.get("limit", CARDS_LIMIT)
    assert coverage["delivery"] == {"complete": True, "view": "metadata", "rows": 0}
    # Retrieval keeps its own status: the metadata page's is the cards page's.
    assert coverage["retrieval"] == cards["coverage"]["retrieval"]
    if case != "empty":
        assert members and coverage["retrieval"]["status"] == "bounded"
    else:
        assert members == []
    if fmt == "text":
        status = text.split("\n", 1)[0]
        limit = arguments.get("limit", CARDS_LIMIT)
        assert f"bound reached (limit {limit}) · next: none" in status, status
        assert f"{len(members)} selected" in status, status
        line = next(ln for ln in text.splitlines()[:4] if ln.startswith("reference r1."))
        served_id = line.split()[1]
        assert [m["node_id"] for m in _members(world, served_id)] == [
            m["node_id"] for m in members]


@pytest.mark.parametrize("rank_by", ["records", "passages"])
def test_metadata_members_equal_a_retained_searchs_and_consumers_read_them(
    world, monkeypatch, rank_by,
) -> None:
    arguments: dict[str, Any] = {"query": TERM, "limit": 9, "rank_by": rank_by}
    if rank_by == "passages":
        arguments["subtype_filter"] = "document"
    with _server(world, monkeypatch, nomos="off") as mcp:
        meta = _ok(mcp, "search", {**arguments, "view": "metadata"})
        kept = _ok(mcp, "search", {**arguments, "retain": True})
        ref_id = meta["result_reference"]["id"]
        members = _members(world, ref_id)
        ids = [m["node_id"] for m in members]
        pairs = {
            (tool, case): (_answer(mcp, tool, _by_reference(tool, ref_id, case)),
                           _answer(mcp, tool, _explicit(tool, ids, case)))
            for tool, case in _CONSUMER_CASES
        }
    assert members == _members(world, kept["result_reference"]["id"])
    assert len(members) == 9
    for (tool, case), (by_ref, explicit) in pairs.items():
        assert by_ref == explicit, (tool, case, by_ref, explicit)
        assert "refused" not in by_ref and by_ref["ids"], (tool, case, by_ref)
    assert sorted(pairs[("map", "-")][0]["ids"]) == sorted(ids)


def _summary_page(mcp, by: list[str], **arguments: Any) -> dict[str, Any]:
    return _ok(mcp, "search", {"query": TERM, "view": "metadata", "summary": by,
                               **arguments})


@pytest.mark.parametrize("by", [["attrs.status"], ["attrs.status", "attrs.team"],
                                ["subtype"]])
def test_a_summary_equals_aggregate_over_its_reference(world, monkeypatch, by) -> None:
    with _server(world, monkeypatch, nomos="off") as mcp:
        page = _summary_page(mcp, by, limit=50)
        ref_id = page["result_reference"]["id"]
        aggregate = _ok(mcp, "aggregate", {"input": {"ref": ref_id}, "by": by,
                                           "metrics": ["count"], "limit": 50})
    summary = page["summary"]
    assert summary["by"] == by
    assert summary["groups"] == aggregate["groups"]
    assert summary["groups_total"] == aggregate["total_count"] == len(aggregate["groups"])
    assert "bounds" not in summary and "next" not in summary
    assert sum(g["count"] for g in summary["groups"]) == page["coverage"]["selection"]["size"]
    assert "bounds" not in page["coverage"]["delivery"]
    if by == ["attrs.status"]:
        # The members with no status are a counted group, as in aggregate.
        assert any(g["attrs.status"] is None for g in summary["groups"]), summary


def test_a_summary_refuses_where_aggregate_over_the_reference_refuses(
    world, monkeypatch,
) -> None:
    """Under enforced Nomos the reference-aggregate function cannot count in
    SQL, so a summary refuses with aggregate's own error and keeps nothing."""
    with _server(world, monkeypatch, nomos="enforce") as mcp:
        refused = _refused(mcp, "search", {"query": TERM, "view": "metadata",
                                           "summary": ["attrs.status"]})
        ref_id = _retain(mcp, limit=10)["result_reference"]["id"]
        aggregate = _refused(mcp, "aggregate", {"input": {"ref": ref_id},
                                                "by": ["attrs.status"], "metrics": ["count"]})
    assert refused["code"] == aggregate["code"] == "unsupported_operator_form"
    assert refused["message"] == aggregate["message"]
    assert world["store"].count() == 1, "only the retained reference was kept"


BULK_TERM = "niobium"
BULK = 220


def _bucket(i: int) -> str:
    # One distinct, non-identifying value per record, long enough that 50
    # groups do not fit a 4,096-byte JSON page.
    return f"bucket-value-{i:03d}-" + "x" * 24


def _bulk_facts() -> list:
    facts: list = []
    for i in range(BULK):
        facts += _record(f"doc:bulk:{i:03d}", i, f"{BULK_TERM} bulk note {i}.",
                         bucket=_bucket(i), **_status(i))
    return facts


@pytest.fixture
def bulk(clean_runtime, okg_mcp_can_log_in, monkeypatch, store) -> dict[str, Any]:
    monkeypatch.setenv("OKG_IVM_INTERVAL_VIEW", "0")
    return _World(dsn=clean_runtime, gen=_publish(clean_runtime, _bulk_facts()), store=store)


@pytest.mark.parametrize("fmt", ["json", "text"])
def test_a_summary_over_50_groups_keeps_the_50_largest_and_names_the_aggregate(
    bulk, monkeypatch, fmt,
) -> None:
    by = ["attrs.bucket"]
    arguments = {"query": BULK_TERM, "view": "metadata", "summary": by, "limit": 60}
    with _server(bulk, monkeypatch, nomos="off") as mcp:
        served = _serve(mcp, "search", arguments, fmt)
        page = _ok(mcp, "search", arguments)
        ref_id = page["result_reference"]["id"]
        first = _ok(mcp, "aggregate", {"input": {"ref": ref_id}, "by": by,
                                       "metrics": ["count"], "limit": 50})
        rest = _ok(mcp, "aggregate", {"input": {"ref": ref_id}, "by": by,
                                      "metrics": ["count"], "limit": 50, "offset": 50})
    assert served.isError is False
    summary = page["summary"]
    assert len(summary["groups"]) == 50
    assert summary["groups_total"] == 60 == first["total_count"]
    assert summary["groups"] == first["groups"]
    assert len(rest["groups"]) == 10
    assert summary["bounds"] == ["summary_groups"]
    assert page["coverage"]["delivery"]["bounds"] == ["summary_groups"]
    assert summary["next"] == {"tool": "aggregate", "arguments": {
        "input": {"ref": ref_id}, "by": by, "metrics": ["count"], "offset": 50,
    }}
    if fmt == "text":
        text = result_text(served)
        status = text.split("\n", 1)[0]
        served_id = next(ln for ln in text.splitlines()[:4]
                         if ln.startswith("reference r1.")).split()[1]
        assert (
            "bound reached (summary_groups 50) · next: aggregate(input="
            f'{{"ref": "{served_id}"}}, by=["attrs.bucket"], metrics=["count"], offset=50)'
        ) in status, status
        assert "summary by attrs.bucket · 50 of 60 groups · bounds summary_groups" in text
        assert "doc:bulk:" not in text


@pytest.mark.parametrize("budget", [4096, None])
@pytest.mark.parametrize("fmt", ["json", "text"])
def test_every_metadata_page_fits_its_budget(bulk, monkeypatch, budget, fmt) -> None:
    limit_bytes = budget if budget is not None else 65536
    sized = {} if budget is None else {"max_bytes": budget}
    forms = {
        "bare": {},
        "status": {"summary": ["attrs.status"]},
        "two_fields": {"summary": ["attrs.status", "attrs.team"]},
        "fifty_groups": {"summary": ["attrs.bucket"]},
    }
    with _server(bulk, monkeypatch, nomos="off") as mcp:
        served = {
            name: _serve(mcp, "search", {"query": BULK_TERM, "view": "metadata",
                                         "limit": 60, **form, **sized}, fmt)
            for name, form in forms.items()
        }
        full = _ok(mcp, "search", {"query": BULK_TERM, "view": "metadata", "limit": 60,
                                   "summary": ["attrs.bucket"]})
    for name, result in served.items():
        assert result.isError is False, (name, result_text(result)[:400])
        size = len(result.model_dump_json(by_alias=True, exclude_none=True).encode())
        assert size <= limit_bytes, (name, size)
        # No page names a record, the 50-group one included.
        assert "doc:bulk:" not in result_text(result), name
    fifty = json_payload(served["fifty_groups"]) if fmt == "json" else None
    if fifty is not None:
        kept = fifty["summary"]["groups"]
        assert kept == full["summary"]["groups"][: len(kept)]
        assert fifty["summary"]["groups_total"] == 60
        if budget is None:
            assert len(kept) == 50 and fifty["summary"]["bounds"] == ["summary_groups"]
        else:
            # 50 such groups do not fit 4,096 JSON bytes: the cut is named,
            # in the summary and in coverage, with the call that reads the rest.
            assert 0 < len(kept) < 50, len(kept)
            assert fifty["summary"]["bounds"] == ["summary_groups", "output_bytes"]
            assert fifty["coverage"]["delivery"]["bounds"] == ["summary_groups", "output_bytes"]
            assert fifty["summary"]["next"]["arguments"]["offset"] == len(kept)


def test_300_metadata_searches_inside_the_lifetime_evict_only_the_readers_own(
    world, monkeypatch,
) -> None:
    """Review H4: one reader's metadata searches inside the lifetime are
    never refused; at its reference cap the next one evicts its oldest,
    whose id then refuses `evicted`; another reader's reference is kept.

    The clock is frozen, so the count of searches is only a multiple of the
    cap. The store runs here with a cap of 8 and 20 searches (the original:
    cap 128, 300 searches); the shipped cap is checked to be 128 below."""
    assert result_references.GRAPH_LIMITS.max_per_reader == 128
    cap, searches = 8, 20
    frozen = time.time()
    store = ResultReferenceStore(
        clock=lambda: frozen,
        limits=dataclasses.replace(result_references.GRAPH_LIMITS, max_per_reader=cap),
    )
    result_references.install_store(store)
    world["store"] = store
    other = {"note": "second reader"}
    with _server(world, monkeypatch, nomos="off") as mcp:
        theirs = _ok(mcp, "search", {"query": TERM, "retain": True, "limit": 3,
                                     "authority": other})["result_reference"]["id"]
        ids: list[str] = []
        for index in range(searches):
            result = _serve(mcp, "search", {"query": TERM, "view": "metadata", "limit": 2})
            assert result.isError is False, (index, result_text(result)[:400])
            ids.append(json_payload(result)["result_reference"]["id"])
            if index == cap - 1:
                assert store.usage(READER_KEY)["references"] == cap
                assert _ok(mcp, "map", {"input": {"ref": ids[0]}})["total_count"] == 2
            if index == cap:
                # The (cap + 1)th evicted the 1st, and only it.
                evicted = _refused(mcp, "map", {"input": {"ref": ids[0]}})
                assert evicted["data"] == {"reason": "evicted"}, evicted
                assert _ok(mcp, "map", {"input": {"ref": ids[1]}})["total_count"] == 2
        assert len(set(ids)) == searches
        assert store.usage(READER_KEY)["references"] == cap
        assert _refused(
            mcp, "map", {"input": {"ref": ids[searches - cap - 1]}})["data"]["reason"] == "evicted"
        assert _ok(mcp, "map", {"input": {"ref": ids[searches - cap]}})["total_count"] == 2
        assert _ok(mcp, "map", {"input": {"ref": ids[-1]}})["total_count"] == 2
        still = _ok(mcp, "map", {"input": {"ref": theirs}, "authority": other})
    assert still["total_count"] == 3
    assert store.count() == cap + 1
