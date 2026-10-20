"""Hermetic contract of search locations and `inspect` text `at` (PACT read-model-s5-location).

The excerpt window's start, the location a hit carries, the handle line's
`@ <chunk>:<offset>`, the `at` target model and the record text read from a
location, with the graph reads replaced. The published-graph and Nomos cases
are in test_search_location.py.
"""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from okg.substrate.errors import NodeNotFound
from okg.substrate.mcp import record_text
from okg.substrate.mcp.excerpt_window import fallback_matcher, window_excerpt, window_span
from okg.substrate.mcp.operator_models import TText
from okg.substrate.mcp.record_text import (
    AT_LEAD_CHARS,
    TextLocationStale,
    chunk_location,
    chunk_text_hash,
    read_record_text,
)
from okg.substrate.mcp.response_bounds import InvalidOperatorArguments
from okg.substrate.mcp.result_text import render, result_cards
from tests.substrate.mcp.test_record_text import _install, _install_chunk, _seg

SESSION = SimpleNamespace(generation_id=7, deployment_name="default")
TEXT = ("filler " * 200) + "zirconium weld failure here." + (" tail" * 100)


def test_window_span_names_where_the_excerpt_starts() -> None:
    matcher = fallback_matcher("zirconium weld")
    start, excerpt = window_span(TEXT, matcher, max_chars=120)
    assert excerpt == window_excerpt(TEXT, matcher, max_chars=120)
    assert start > 0
    assert TEXT[start:].startswith(excerpt.removeprefix("...").removesuffix("..."))
    assert window_span("short", matcher, max_chars=120) == (0, "short")


def test_only_a_chunk_with_text_has_a_location() -> None:
    row = {"node_id": "chunk:1", "subtype": "document_chunk", "attrs": {"text": TEXT}}
    assert chunk_location(row, 7) == {
        "chunk": "chunk:1", "offset": 7, "hash": chunk_text_hash(TEXT),
    }
    assert chunk_text_hash(TEXT).startswith("sha256:")
    assert chunk_location({**row, "subtype": "ticket"}, 7) is None
    assert chunk_location({**row, "attrs": {}}, 0) is None
    assert chunk_location(row, len(TEXT)) is None


def _page(row: dict) -> dict:
    return {"status": "ok", "method": "lexical", "results": [row], "total_count": 1,
            "generation_id": 3, "offset": 0, "limit": 10}


def test_the_handle_line_leaves_the_location_to_the_json() -> None:
    """The card printed `@ <chunk>:<offset>` and a differing `best` chunk.
    #3666 (read-passage-rows): a search row shows the matched passage
    itself and the reply names one read call, so the location and the
    winning chunk id are in the JSON only (where `inspect` `at` reads
    them); the hash never printed."""
    location = {"chunk": "chunk:doc:01", "offset": 412, "hash": chunk_text_hash(TEXT)}
    row = {"node_id": "doc:1", "subtype": "document", "excerpt": "zirconium",
           "handle": {"title": "Weld"}, "rollup_best_chunk_id": "chunk:doc:02",
           "location": location}
    text = render(_page(row), tool="search")
    (card,) = result_cards(text)
    assert card.facts[:1] == ('"Weld"',)
    for absent in ("sha256", "@ chunk", "best", "chunk:doc:0"):
        assert absent not in text, absent
    assert "    zirconium" in text.splitlines()


def test_the_at_target_is_optional_and_strict() -> None:
    model = TText.model_validate({"kind": "text", "at": {"chunk": "c", "offset": 3}})
    assert model.node_id is None and model.at is not None and model.at.offset == 3
    for bad in ({"chunk": "c", "offset": -1}, {"chunk": "c", "offset": True},
                {"chunk": "c", "offset": False}, {"chunk": "c"},
                {"chunk": "c", "offset": 1, "line": 4}):
        with pytest.raises(ValidationError):
            TText.model_validate({"kind": "text", "at": bad})


def _windows() -> list:
    """Three window chunks with a 10-character overlap, as a window chunker
    writes them; the read trims the repeat."""
    source = "".join(chr(ord("a") + (i % 26)) for i in range(880))
    segs = []
    for i, start in enumerate((0, 290, 580)):
        end = min(start + 300, len(source))
        segs.append(_seg(f"chunk:{i}", source[start:end], ordinal=i, total=3,
                         offset=start, end=end, doc_length=len(source)))
    return segs, source


def test_a_read_at_a_location_starts_before_it_and_reads_on(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    segs, source = _windows()
    _install(monkeypatch, segs)
    chunk_text = segs[1].text
    at = {"chunk": "chunk:1", "offset": 250, "hash": chunk_text_hash(chunk_text)}
    pages, token = [], None
    while True:
        page = read_record_text(None, SESSION, "doc:1", at=at, continuation_token=token,
                                max_bytes=2048)
        pages.append(page)
        if page["complete"]:
            break
        token = page["continuation_token"]
    first = pages[0]["items"][0]
    assert first["chunk_id"] == "chunk:1"
    # Chunk 1's first 10 characters repeat chunk 0 and are trimmed, so its
    # offset 250 is character 240 of the segment; the read starts
    # AT_LEAD_CHARS before that.
    assert first["text_range"][0] == 240 - AT_LEAD_CHARS
    read = "".join(i["text"] for p in pages for i in p["items"])
    assert read == source[290 + 250 - AT_LEAD_CHARS:]
    # Review fix 2: every page, the complete last one included, says where
    # the requested extent began and that the text before it was not sent.
    assert len(pages) > 1
    for page in pages:
        assert page["read_from"] == {
            "chunk": "chunk:1", "offset": 240 - AT_LEAD_CHARS, "text_before_sent": False,
        }
    assert pages[-1]["complete"] is True
    text = render(pages[-1], tool="inspect", kind="text")
    assert "! read starts at chunk:1 @40; earlier text was not sent" in text
    assert "read_from" not in text
    # The hash was given, so the check was not offset-only.
    assert all("location_check" not in page for page in pages)
    assert "offset only" not in render(pages[0], tool="inspect", kind="text")
    # A read from the record's top carries neither field.
    plain = read_record_text(None, SESSION, "doc:1", max_bytes=8192)
    assert "read_from" not in plain and "location_check" not in plain


def test_a_read_at_a_hit_in_a_trimmed_overlap_steps_back_to_show_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review: chunk 1's first 10 characters repeat chunk 0 and are trimmed.
    A hit at offset 5 sits in that overlap (source position 295), so the
    read must start in chunk 0's tail, AT_LEAD_CHARS before the hit, not at
    chunk 1's own text where the hit is already behind it."""
    segs, source = _windows()
    _install(monkeypatch, segs)
    at = {"chunk": "chunk:1", "offset": 5}
    pages, token = [], None
    while True:
        page = read_record_text(None, SESSION, "doc:1", at=at, continuation_token=token,
                                max_bytes=2048)
        pages.append(page)
        if page["complete"]:
            break
        token = page["continuation_token"]
    assert pages[0]["items"][0]["chunk_id"] == "chunk:0"
    read = "".join(i["text"] for p in pages for i in p["items"])
    assert read == source[295 - AT_LEAD_CHARS:]
    assert pages[0]["read_from"] == {
        "chunk": "chunk:0", "offset": 295 - AT_LEAD_CHARS, "text_before_sent": False,
    }
    # Review fix 1(a): no hash, so the first page says the location was
    # checked by offset only, in JSON and in text.
    assert pages[0]["location_check"] == "offset_only"
    text = render(pages[0], tool="inspect", kind="text")
    assert (
        "! location checked by offset only (no hash): changed chunk text is not detected"
        in text
    )
    assert "location_check" not in text


def test_a_stale_location_is_refused_by_name(monkeypatch: pytest.MonkeyPatch) -> None:
    segs, _ = _windows()
    _install(monkeypatch, segs)
    good = {"chunk": "chunk:1", "offset": 5}
    cases = {
        "hash_mismatch": {**good, "hash": chunk_text_hash("other text")},
        "offset_out_of_range": {"chunk": "chunk:1", "offset": 300},
        "chunk_not_found": {"chunk": "chunk:9", "offset": 0},
    }
    for reason, at in cases.items():
        with pytest.raises(TextLocationStale) as refused:
            read_record_text(None, SESSION, "doc:1", at=at, max_bytes=4096)
        assert refused.value.code == "text_location_stale"
        assert refused.value.data == {
            "chunk": at["chunk"], "offset": at["offset"], "node_id": "doc:1",
            "reason": reason, "pinned_generation_id": 7,
        }
        assert '"kind":"text"' in refused.value.fix_hint
    # The record's own id is not a chunk of it.
    with pytest.raises(TextLocationStale):
        read_record_text(None, SESSION, "doc:1", at={"chunk": "doc:1", "offset": 0},
                         max_bytes=4096)
    # A malformed location is a plain argument error, not a stale one.
    with pytest.raises(record_text.SubstrateError) as bad:
        read_record_text(None, SESSION, "doc:1", at={"chunk": "chunk:1"}, max_bytes=4096)
    assert not isinstance(bad.value, TextLocationStale)


def test_at_alone_reads_the_chunk_and_points_into_its_record(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    chunk = {"node_id": "chunk:fixture01", "subtype": "document_chunk", "source": "fixture",
             "attrs": {"text": TEXT}}
    _install_chunk(monkeypatch, chunk, {"doc:page-a": {"chunk_ordinal": 3}})
    page = read_record_text(None, SESSION, "", at={"chunk": "chunk:fixture01", "offset": 900},
                            max_bytes=4096)
    assert page["node_id"] == "chunk:fixture01"
    (item,) = page["items"]
    assert item["text_range"] == [900 - AT_LEAD_CHARS, len(TEXT)]
    assert "zirconium" in item["text"]
    assert ('"node_id":"doc:page-a","at":{"chunk":"chunk:fixture01","offset":900}'
            in page["outcome"]["next_move"])
    # Review fix 2: a read at offset 900 says it began at 700 (the lead
    # before 900) and that the chunk's earlier text was not sent.
    assert page["read_from"] == {
        "chunk": "chunk:fixture01", "offset": 900 - AT_LEAD_CHARS, "text_before_sent": False,
    }
    assert page["complete"] is True
    assert page["location_check"] == "offset_only"
    text = render(page, tool="inspect", kind="text")
    assert "! read starts at chunk:fixture01 @700; earlier text was not sent" in text
    assert "offset only" in text
    # Review fix 1(b): with a hash, the follow-on read carries it and the
    # page does not say offset-only.
    digest = chunk_text_hash(TEXT)
    hashed = read_record_text(
        None, SESSION, "", at={"chunk": "chunk:fixture01", "offset": 900, "hash": digest},
        max_bytes=4096,
    )
    assert "location_check" not in hashed
    assert (f'"at":{{"chunk":"chunk:fixture01","offset":900,"hash":"{digest}"}}'
            in hashed["outcome"]["next_move"])
    assert '"hash"' not in page["outcome"]["next_move"]
    # A plain chunk read points into the record at the chunk's start.
    plain = read_record_text(None, SESSION, "chunk:fixture01", max_bytes=8192)
    assert '"at":{"chunk":"chunk:fixture01","offset":0}' in plain["outcome"]["next_move"]
    assert "read_from" not in plain and "location_check" not in plain
    # `at` naming another chunk than the node read is refused.
    with pytest.raises(TextLocationStale):
        read_record_text(None, SESSION, "chunk:fixture01",
                         at={"chunk": "chunk:other", "offset": 0}, max_bytes=4096)


def test_hidden_and_missing_chunks_give_one_answer(monkeypatch: pytest.MonkeyPatch) -> None:
    """A chunk the read filter hides raises NodeNotFound from the visibility
    check exactly as an absent one does; `at` turns both into the same
    refusal (the PostgreSQL test runs the real filter)."""

    def not_visible(conn, session, graph, node_id, **_kwargs):
        raise NodeNotFound("hidden or absent", data={"node_id": node_id})

    _install_chunk(monkeypatch, {"node_id": "x", "subtype": "document_chunk", "attrs": {}}, {})
    monkeypatch.setattr(record_text, "_visible_start", not_visible)
    answers = []
    for chunk in ("chunk:hidden", "chunk:absent"):
        with pytest.raises(TextLocationStale) as refused:
            read_record_text(None, SESSION, "", at={"chunk": chunk, "offset": 1}, max_bytes=4096)
        err = refused.value
        answers.append(json.dumps(
            {"m": err.message, "d": err.data, "h": err.fix_hint}, sort_keys=True,
        ).replace(chunk, "<chunk>"))
    assert answers[0] == answers[1]
    assert json.loads(answers[0])["d"]["reason"] == "chunk_not_found"


def test_a_malformed_hash_is_an_argument_error_before_any_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review LOW 4: a hash not in the form search returns is a malformed
    argument, refused before the session check or any graph read, not a
    `hash_mismatch` from a read."""

    def no_read(*args, **kwargs):
        raise AssertionError("read before the argument check")

    monkeypatch.setattr(record_text, "assert_session_matches", no_read)
    monkeypatch.setattr(record_text, "_checkout", no_read)
    for digest in ("SHA256:AB", "sha256:00", "sha256:" + "A" * 64, "sha256:" + "0" * 63,
                   "md5:" + "0" * 64, ""):
        for node_id in ("doc:1", ""):
            with pytest.raises(InvalidOperatorArguments) as bad:
                read_record_text(None, SESSION, node_id,
                                 at={"chunk": "chunk:1", "offset": 5, "hash": digest},
                                 max_bytes=4096)
            assert bad.value.code == "invalid_operator_arguments"
            assert bad.value.data["invalid_fields"] == ["target.at"]
            assert not isinstance(bad.value, TextLocationStale)


def test_a_text_target_without_node_id_or_at_is_an_argument_refusal() -> None:
    """`node_id` is optional in the schema (an `at` can name the chunk), so a
    text target with neither is refused as a bad argument, not a generic error.
    """
    with pytest.raises(InvalidOperatorArguments) as refused:
        record_text.read_record_text(None, None, "")
    assert refused.value.code == "invalid_operator_arguments"
    assert refused.value.data["invalid_fields"] == ["target.node_id"]
