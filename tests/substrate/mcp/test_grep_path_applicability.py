"""Path applicability is proved at the pin, independently of planner choice."""
from __future__ import annotations

import pytest
import psycopg

from okg.substrate.mcp import grep_search
from tests.substrate.mcp.test_search_grep import _grep, _ids, _node, _publish


@pytest.mark.parametrize("rows, expected", [([], False), ([{"one": 1}], True)])
def test_path_presence_executes_exact_query_without_explain(monkeypatch, rows, expected):
    calls = []

    def bounded(conn, sql, params, **kwargs):
        calls.append((sql, params, kwargs))
        assert not kwargs.get("explain")
        return rows

    monkeypatch.setattr(grep_search, "_bounded_with", bounded)
    assert grep_search._any_path(None, "fixture_segment", 41) is expected
    assert len(calls) == 1
    sql, params, _ = calls[0]
    assert params == {"bp": 41}
    assert "n.deleted = FALSE" in sql
    assert "n.live_lo <= %(bp)s AND %(bp)s < n.live_hi" in sql
    assert "jsonb_typeof(n.attrs->'path') = 'string'" in sql
    assert "NULLIF(n.attrs->>'path', '') IS NOT NULL" in sql


def test_path_presence_timeout_refuses_instead_of_unknown(monkeypatch):
    def cancelled(*args, **kwargs):
        raise grep_search._Cancelled()

    monkeypatch.setattr(grep_search, "_bounded_with", cancelled)
    with pytest.raises(grep_search.GrepTimeout) as raised:
        grep_search._any_path(None, "fixture_segment", 41)
    assert raised.value.code == "grep_timeout"
    assert raised.value.data["phase"] == "path_applicability"


@pytest.mark.postgres
def test_empty_null_and_missing_paths_do_not_make_paths_applicable(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, [
        _node("note:missing", "note", "kestrel"),
        _node("note:empty", "note", "kestrel", path=""),
        _node("note:null", "note", "kestrel", path=None),
        _node("note:number", "note", "kestrel", path=123),
        _node("note:object", "note", "kestrel", path={"value": "app/readme"}),
        _node("note:list", "note", "kestrel", path=["app/readme"]),
    ])
    for query, kwargs in [
        ("kestrel", {"path_prefix": "app/"}),
        ("kestrel path:app", {}),
        ("path:app", {}),
    ]:
        with pytest.raises(grep_search.GrepPathNotApplicable):
            _grep(clean_runtime, deployment, query, **kwargs)
    assert len(_ids(_grep(clean_runtime, deployment, "kestrel -path:app"))) == 6


@pytest.mark.postgres
def test_applicable_path_can_truthfully_return_no_matching_text(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, [
        _node("note:path", "note", "kestrel", path="app/readme.md"),
    ])
    page = _grep(clean_runtime, deployment, "absentword", path_prefix="app/")
    assert page["results"] == []
    assert page["total_count"] == 0
    assert page["total_count_basis"] == "exact"
    assert "path_presence" not in page


@pytest.mark.postgres
@pytest.mark.parametrize("old_path,new_path", [(None, "app/readme.md"), ("app/readme.md", None)])
def test_path_presence_uses_published_generation_not_latest_version(
    clean_runtime, tmp_path, old_path, new_path,
):
    from dataclasses import replace

    from okg.substrate.mcp import open_session, search
    from tests.substrate.mcp.search_fixture import publish_nodes
    from tests.substrate.mcp.test_search_grep import SUBTYPES, NARROWINGS

    deployment = _publish(tmp_path, clean_runtime, [
        _node("note:revised", "note", "kestrel", path=old_path),
    ])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        old = open_session(conn, deployment_name=deployment)
    revised = replace(
        _node("note:revised", "note", "kestrel", path=new_path),
        source_revision={"v": 2},
    )
    publish_nodes(
        clean_runtime, deployment, subtypes=SUBTYPES, narrowings=NARROWINGS,
        nodes=[revised],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        latest = open_session(conn, deployment_name=deployment)
        for session, path in [(old, old_path), (latest, new_path)]:
            if path is None:
                with pytest.raises(grep_search.GrepPathNotApplicable):
                    search(conn, session, "kestrel", method="grep", path_prefix="app/")
            else:
                page = search(conn, session, "kestrel", method="grep", path_prefix="app/")
                assert _ids(page) == ["note:revised"]


@pytest.mark.postgres
def test_deleted_path_does_not_make_latest_generation_applicable(clean_runtime, tmp_path):
    from dataclasses import replace

    from tests.substrate.mcp.search_fixture import publish_nodes
    from tests.substrate.mcp.test_search_grep import SUBTYPES, NARROWINGS

    path_node = _node("note:path", "note", "kestrel", path="app/readme.md")
    deployment = _publish(tmp_path, clean_runtime, [
        path_node, _node("note:keep", "note", "kestrel"),
    ])
    publish_nodes(
        clean_runtime, deployment, subtypes=SUBTYPES, narrowings=NARROWINGS,
        nodes=[replace(path_node, op="R", source_revision={"v": 2})],
    )
    with pytest.raises(grep_search.GrepPathNotApplicable):
        _grep(clean_runtime, deployment, "kestrel", path_prefix="app/")
    assert _ids(_grep(clean_runtime, deployment, "kestrel -path:app")) == ["note:keep"]


class _Info:
    def __init__(self, dbname: str = "d") -> None:
        self.host, self.port, self.dbname = "h", 5432, dbname


class _Conn:
    def __init__(self, dbname: str = "d") -> None:
        self.info = _Info(dbname)


@pytest.fixture
def fresh_answers(monkeypatch):
    monkeypatch.setattr(grep_search, "_any_path_answers", {})
    clock = [1000.0]
    monkeypatch.setattr(grep_search, "_any_path_clock", lambda: clock[0])
    return clock


def test_the_answer_is_reused_for_one_generation_only(monkeypatch, fresh_answers):
    """The applicability scan costs about 1.6 s on a comp-ops copy for every
    grep call with a path scope; at one published generation its answer
    cannot change, so it runs once per generation."""
    calls = []
    monkeypatch.setattr(
        grep_search, "_any_path", lambda conn, segment, bp: calls.append(bp) or True,
    )
    at = grep_search._any_path_at_pin
    assert at(_Conn(), "seg", 41, generation_id="g1") is True
    assert at(_Conn(), "seg", 41, generation_id="g1") is True
    assert calls == [41]
    # Another generation (a rebuilt database can reuse segment and branch
    # point, never the generation id), another pin or segment: read again.
    at(_Conn(), "seg", 41, generation_id="g2")
    at(_Conn(), "seg", 42, generation_id="g1")
    at(_Conn(), "other", 41, generation_id="g1")
    assert len(calls) == 4
    # Past the lifetime the scan runs again.
    fresh_answers[0] += grep_search._ANY_PATH_TTL_S + 1
    at(_Conn(), "seg", 41, generation_id="g1")
    assert len(calls) == 5


def test_a_timeout_is_not_remembered(monkeypatch, fresh_answers):
    outcomes = [grep_search.GrepTimeout("slow"), False]

    def scan(conn, segment, bp):
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(grep_search, "_any_path", scan)
    with pytest.raises(grep_search.GrepTimeout):
        grep_search._any_path_at_pin(_Conn(), "seg", 41, generation_id="g1")
    assert grep_search._any_path_at_pin(_Conn(), "seg", 41, generation_id="g1") is False
    assert outcomes == []


def test_another_database_scans_again_and_the_oldest_answer_is_dropped(
    monkeypatch, fresh_answers,
):
    calls = []
    monkeypatch.setattr(
        grep_search, "_any_path", lambda conn, segment, bp: calls.append(conn.info.dbname) or True,
    )
    at = grep_search._any_path_at_pin
    at(_Conn("a"), "seg", 41, generation_id="g1")
    at(_Conn("b"), "seg", 41, generation_id="g1")
    assert calls == ["a", "b"], "the database is part of the key"
    monkeypatch.setattr(grep_search, "_ANY_PATH_MAX_ENTRIES", 2)
    at(_Conn("c"), "seg", 41, generation_id="g1")
    assert len(grep_search._any_path_answers) == 2
    at(_Conn("b"), "seg", 41, generation_id="g1")
    assert calls == ["a", "b", "c"], "b was kept"
    at(_Conn("a"), "seg", 41, generation_id="g1")
    assert calls == ["a", "b", "c", "a"], "a, the oldest, was dropped"
