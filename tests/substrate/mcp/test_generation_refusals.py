"""A pinned generation that resolves to nothing names the exact fix (#3470).

PostgreSQL tests on a published fixture graph. `generation_id="latest"` and
a cut-off id were refused with the generic `substrate_error`; they now get
`generation_not_found`, and the fix says what to send. Neither is accepted
in place of a real id.
"""
from __future__ import annotations

import psycopg
import pytest

from okg.substrate.errors import GenerationNotFound
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.mcp import open_session
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer
from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes

pytestmark = pytest.mark.postgres

NOTE = SubtypeSpec("note", metadata={"searchable_text_fields": ["text"]})


def _deployment(tmp_path, dsn: str) -> tuple[str, str, str]:
    """Two published generations: the first and the newest id."""
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    _, first = publish_nodes(
        dsn, deployment, subtypes=[NOTE],
        nodes=[NodeFact(
            node_id="note:1", subtype="note", attrs={"text": "zirconium note"},
            source_record_id={"id": "note:1"}, source_revision={"v": 1},
        )],
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts([NodeFact(
            node_id="note:2", subtype="note", attrs={"text": "zirconium again"},
            source_record_id={"id": "note:2"}, source_revision={"v": 1},
        )], source="system:analyst_search_fixture", strict_endpoints=True)
    Publisher(dsn, deployment, catalog_identity=bind_test_catalog_writer(dsn, deployment)).cycle()
    with psycopg.connect(dsn, autocommit=True) as conn:
        newest = str(open_session(conn, deployment_name=deployment).generation_id)
    assert newest != first
    return deployment, first, newest


def _refusal(dsn: str, deployment: str, requested) -> GenerationNotFound:
    with psycopg.connect(dsn, autocommit=True) as conn:
        with pytest.raises(GenerationNotFound) as caught:
            open_session(conn, deployment_name=deployment,
                         requested_generation=requested)
    return caught.value


@pytest.mark.parametrize("word", ["latest", "Latest", " current "])
def test_a_word_for_the_newest_says_omit_generation_id(clean_runtime, tmp_path, word):
    deployment, _, _ = _deployment(tmp_path, clean_runtime)
    error = _refusal(clean_runtime, deployment, word)
    assert error.code == "generation_not_found"
    assert f"{word.strip()!r} is not a generation id" in str(error)
    assert error.fix_hint.startswith(
        "omit generation_id (or pass null) to read the latest published generation")


@pytest.mark.parametrize("cut", [-6, -9])
def test_the_start_of_one_published_id_names_that_id(clean_runtime, tmp_path, cut):
    deployment, first, newest = _deployment(tmp_path, clean_runtime)
    for generation in (first, newest):
        cut_id = generation[:cut]
        # Also with an ellipsis, and without the `okg:` start.
        for requested in (cut_id, cut_id + "...", cut_id + "\u2026",
                          cut_id[len("okg:"):]):
            error = _refusal(clean_runtime, deployment, requested)
            assert error.code == "generation_not_found"
            assert error.data["prefix_check"] == "checked"
            assert error.data["matching_generation"] == generation, requested
            assert error.fix_hint == f"pass the whole id: generation_id={generation!r}"
            assert "is the start of one published id" in str(error)


def test_the_start_of_several_ids_names_none(clean_runtime, tmp_path):
    deployment, first, newest = _deployment(tmp_path, clean_runtime)
    shared = first[:first.index(":gen:") + len(":gen:")]
    assert newest.startswith(shared)
    error = _refusal(clean_runtime, deployment, shared)
    assert "matching_generation" not in error.data
    assert error.data["matching_generations"] == sorted([first, newest])
    assert "is the start of 2 published ids" in str(error)
    assert error.fix_hint.startswith("pin a generation published on this session's branch")


def test_an_unknown_id_keeps_the_general_fix(clean_runtime, tmp_path):
    deployment, _, generation = _deployment(tmp_path, clean_runtime)
    error = _refusal(clean_runtime, deployment, generation + "x")
    assert error.code == "generation_not_found"
    assert error.data["prefix_check"] == "checked"
    assert "matching_generation" not in error.data
    assert error.fix_hint == (
        "pin a generation published on this session's branch, or omit "
        "generation_id to use the latest")
    assert "is not a published OKG generation" in str(error)
    # An integer id that resolves to nothing is not compared as text.
    integer = _refusal(clean_runtime, deployment, 99999)
    assert integer.code == "generation_not_found"
    assert "prefix_check" not in integer.data


def test_the_served_search_refusal_carries_the_code_and_fix(clean_runtime, tmp_path):
    from okg.substrate.mcp import server as srv

    deployment, _, _ = _deployment(tmp_path, clean_runtime)
    mcp = srv.build_server(dsn=clean_runtime, deployment_name=deployment)
    try:
        tool = mcp._tool_manager._tools["search"]
        try:
            page = tool.fn(query="zirconium", generation_id="latest")
        except GenerationNotFound as exc:
            code, fix_hint = exc.code, exc.fix_hint
        else:
            code, fix_hint = page["error"]["code"], page["error"]["fix_hint"]
        assert code == "generation_not_found"
        assert fix_hint.startswith("omit generation_id (or pass null)")
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
