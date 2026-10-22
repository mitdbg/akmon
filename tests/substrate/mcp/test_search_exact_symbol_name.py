"""A query that is one code symbol's name finds the symbol, first.

The graph comes from the real code-structure source over a small Python
package, so the symbol rows carry the attributes and stored text a crawl
writes. The stored text of a class keeps its name only inside one dotted token
(`src.itsdangerous.url_safe.URLSafeSerializer`), which BM25 reads as a single
word; the import rows say the bare name and are the only rows BM25 matches.
"""
from __future__ import annotations

from pathlib import Path

import psycopg
import pytest

from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.code_graph import CodeStructureSource
from okg.substrate.mcp import open_session, search
from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp.search_fixture import make_search_deployment, node, publish_nodes

pytestmark = pytest.mark.postgres

#: The searchable fields the code_graph ontology declares per subtype.
SEARCHABLE = {
    "code_symbol": [
        "qualified_name", "canonical_name", "semantic_key", "module_name",
        "signature", "docstring", "text",
    ],
    "code_import": ["module", "name", "alias", "text"],
    "code_reference": ["qualified_name", "name", "text"],
}

def _symbol_prefix(qualified_name: str) -> str:
    """A code symbol's node id is `symbol:<repo>:<path>:<qname>:<line>`."""
    return f"symbol:itsdangerous:src/itsdangerous/url_safe.py:{qualified_name}:"


def _definitions(page: dict, qualified_name: str) -> list[str]:
    prefix = _symbol_prefix(qualified_name)
    return [i for i in (row["node_id"] for row in page["results"]) if i.startswith(prefix)]


PACKAGE = {
    "src/itsdangerous/__init__.py": (
        "from .url_safe import URLSafeSerializer as URLSafeSerializer\n"
        "from .url_safe import URLSafeTimedSerializer as URLSafeTimedSerializer\n"
    ),
    "src/itsdangerous/url_safe.py": '''
from __future__ import annotations
from .serializer import Serializer
from .timed import TimedSerializer


class URLSafeSerializerMixin(Serializer):
    """Mixin for url safe."""

    def load_payload(self, payload, *args, serializer=None, **kwargs):
        return payload


class URLSafeSerializer(URLSafeSerializerMixin, Serializer):
    """Works like :class:`.Serializer` but dumps and loads into a URL
    safe string consisting of the upper and lowercase character of the
    alphabet as well as ``_`` and ``-``.
    """

    default_serializer = object

    def loads_unsafe(self, s):
        return s


class URLSafeTimedSerializer(URLSafeSerializerMixin, TimedSerializer):
    """Works like :class:`.TimedSerializer` but dumps and loads into a URL
    safe string consisting of the upper and lowercase character of the
    alphabet as well as ``_`` and ``-``.
    """

    default_serializer = object
''',
    "tests/test_url_safe.py": (
        "from itsdangerous.url_safe import URLSafeSerializer\n"
        "from itsdangerous.url_safe import URLSafeTimedSerializer\n\n\n"
        "def test_roundtrip():\n"
        "    s = URLSafeSerializer('secret')\n"
        "    assert s.loads(s.dumps(1)) == 1\n"
    ),
    "docs/index.rst": "Use URLSafeSerializer to sign payloads.\n",
}


def _facts(root: Path) -> list[NodeFact]:
    repo = root / "repo"
    for rel, text in PACKAGE.items():
        path = repo / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    source = CodeStructureSource(repo="itsdangerous", repo_root=repo)
    return [f for f in source.run("run-1", mode="cursor").facts if isinstance(f, NodeFact)]


def _publish_package(clean_runtime: str, tmp_path) -> str:
    facts = _facts(tmp_path)
    subtypes = sorted({fact.subtype for fact in facts})
    assert {"code_symbol", "code_import"} <= set(subtypes)
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=[
            SubtypeSpec(name, metadata={
                "searchable_text_fields": SEARCHABLE.get(name, ["text"]),
            })
            for name in subtypes
        ],
        nodes=facts,
    )
    return deployment


def _run(clean_runtime: str, deployment: str, query: str, **kwargs) -> dict:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        return search(conn, session, query, method="lexical", **kwargs)


def _search(clean_runtime: str, tmp_path, query: str, **kwargs) -> dict:
    return _run(clean_runtime, _publish_package(clean_runtime, tmp_path), query, **kwargs)


def _ids(page: dict) -> list[str]:
    return [row["node_id"] for row in page["results"]]


def test_exact_class_name_ranks_the_definition_first(
    clean_runtime: str, tmp_path,
) -> None:
    page = _search(clean_runtime, tmp_path, "URLSafeSerializer")
    rows = page["results"]
    definition = _definitions(page, "URLSafeSerializer")
    assert len(definition) == 1, "the class definition is missing from the page"
    assert rows[0]["node_id"] == definition[0]
    assert rows[0]["match_kind"] == "exact_name"
    # The rows that mention the name stay on the page, behind the definition.
    assert any(row["subtype"] == "code_import" for row in rows[1:])
    # Its sibling classes are not the answer to this name.
    assert _definitions(page, "URLSafeTimedSerializer") == []


def test_each_class_name_finds_its_own_definition(
    clean_runtime: str, tmp_path,
) -> None:
    page = _search(clean_runtime, tmp_path, "URLSafeTimedSerializer")
    assert _ids(page)[0] in _definitions(page, "URLSafeTimedSerializer")
    assert _definitions(page, "URLSafeSerializer") == []


def test_definition_survives_a_one_row_limit(clean_runtime: str, tmp_path) -> None:
    page = _search(clean_runtime, tmp_path, "URLSafeSerializer", limit=1)
    assert len(page["results"]) == 1
    assert _definitions(page, "URLSafeSerializer") == _ids(page)


@pytest.mark.parametrize("query", ["loads_unsafe", "URLSafeSerializer.loads_unsafe"])
def test_method_and_qualified_name_find_the_method(
    clean_runtime: str, tmp_path, query: str,
) -> None:
    page = _search(clean_runtime, tmp_path, query)
    assert _ids(page)[0] in _definitions(page, "URLSafeSerializer.loads_unsafe")


def test_subtype_filter_keeps_the_definition_out_of_an_import_page(
    clean_runtime: str, tmp_path,
) -> None:
    page = _search(
        clean_runtime, tmp_path, "URLSafeSerializer",
        filter={"subtype": "code_import"},
    )
    assert page["results"]
    assert {row["subtype"] for row in page["results"]} == {"code_import"}
    assert all(row.get("match_kind") != "exact_name" for row in page["results"])


@pytest.mark.parametrize("query", [
    "serializer",           # a plain word
    "Serializer",           # a capitalised plain word
    "urlsafeserializer",    # the name in another case is not the name
    "URLSafeSerializer payload",  # more than one token
])
def test_other_queries_keep_the_bm25_page(
    clean_runtime: str, tmp_path, query: str,
) -> None:
    page = _search(clean_runtime, tmp_path, query)
    assert all(row.get("match_kind") != "exact_name" for row in page["results"])


# ---------- the step leaves every other page as it was ----------

@pytest.fixture
def search_module():
    import sys

    return sys.modules["okg.substrate.mcp.search"]


@pytest.mark.parametrize("query", [
    "serializer",
    "Serializer",
    "payload",
    "urlsafeserializer",
    "URLSafeSerializer payload",
])
def test_pages_that_name_no_symbol_equal_the_page_without_the_step(
    clean_runtime: str, tmp_path, monkeypatch, search_module, query: str,
) -> None:
    deployment = _publish_package(clean_runtime, tmp_path)
    with_step = _run(clean_runtime, deployment, query, limit=50)
    monkeypatch.setattr(search_module, "exact_name_query", lambda _query: None)
    without_step = _run(clean_runtime, deployment, query, limit=50)
    assert with_step["results"], "the control query must find rows"
    assert _ids(with_step) == _ids(without_step)
    assert with_step["total_count"] == without_step["total_count"]
    assert with_step["total_count_basis"] == without_step["total_count_basis"]
    assert "exact_name_cap" not in with_step


def test_a_named_symbol_is_the_only_row_the_step_adds_or_moves(
    clean_runtime: str, tmp_path, monkeypatch, search_module,
) -> None:
    deployment = _publish_package(clean_runtime, tmp_path)
    with_step = _run(clean_runtime, deployment, "URLSafeSerializer", limit=50)
    monkeypatch.setattr(search_module, "exact_name_query", lambda _query: None)
    without_step = _run(clean_runtime, deployment, "URLSafeSerializer", limit=50)
    definition = _definitions(with_step, "URLSafeSerializer")
    assert len(definition) == 1
    assert _ids(with_step)[0] == definition[0]
    assert [i for i in _ids(with_step) if i != definition[0]] == [
        i for i in _ids(without_step) if i != definition[0]
    ]


# ---------- the name read does not scan, and does not cut before it filters ----------

HIDDEN = "connector:hidden_symbols"


def _symbol(node_id: str, name: str, qualified_name: str, **attrs):
    return node(
        # The id in the text keeps same-name symbols from collapsing as exact
        # duplicates of each other.
        node_id, "code_symbol", f"src.pkg.mod.{qualified_name} {node_id}",
        name=name, qualified_name=qualified_name, **attrs,
    )


def _mention(node_id: str, text: str):
    """A row that says the words, as an import or a reference does."""
    return node(node_id, "code_reference", text, name=node_id)


def _symbol_graph(
    clean_runtime: str, tmp_path, *, visible: list, hidden: list = (),
) -> str:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    subtypes = sorted({fact.subtype for fact in [*visible, *hidden]})
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=[
            SubtypeSpec(name, metadata={
                "searchable_text_fields": SEARCHABLE.get(name, ["text"]),
            })
            for name in subtypes
        ],
        nodes=list(visible),
        node_groups=[(HIDDEN, list(hidden))] if hidden else None,
    )
    return deployment


def _enforce(clean_runtime: str, deployment: str, monkeypatch) -> None:
    from tests.substrate.mcp.test_search_records import MAIN, _activate, _allow

    _activate(clean_runtime, deployment, [_allow(MAIN)])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")


def _enforced_run(clean_runtime: str, deployment: str, *queries: str, **kw):
    from tests.substrate.mcp.test_search_records import READER

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment, nomos_subject=READER)
        return [search(conn, session, q, method="lexical", **kw) for q in queries]


def _enforced_page(clean_runtime: str, deployment: str, monkeypatch, *queries: str, **kw):
    _enforce(clean_runtime, deployment, monkeypatch)
    return _enforced_run(clean_runtime, deployment, *queries, **kw)


def test_under_enforce_hidden_same_name_symbols_do_not_crowd_out_a_readable_one(
    clean_runtime: str, tmp_path, monkeypatch, search_module,
) -> None:
    # Six symbols the reader may not read sort ahead of the one it may; the
    # old fixed cut of five read only hidden rows and lost the readable one.
    # A window of two makes the readable row arrive in a later statement.
    monkeypatch.setattr(search_module, "_EXACT_NAME_WINDOW", 2)
    hidden = [
        _symbol(f"symbol:a{i}", "WidgetKit", f"pkg.a{i}.WidgetKit") for i in range(6)
    ]
    visible = [_symbol("symbol:z-readable", "WidgetKit", "pkg.z.WidgetKit")]
    deployment = _symbol_graph(clean_runtime, tmp_path, visible=visible, hidden=hidden)
    (page,) = _enforced_page(
        clean_runtime, deployment, monkeypatch, "WidgetKit", completeness="exact",
    )
    assert _ids(page) == ["symbol:z-readable"]
    assert page["results"][0]["match_kind"] == "exact_name"
    # Under enforcement the step never calls the count exact (the read may
    # have stopped among rows the reader cannot see).
    assert page["total_count_basis"] != "exact"
    assert page["exact_name_cap"] == {"cause": "enforced"}


def test_under_enforce_a_name_only_hidden_symbols_bear_reads_as_absent(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    hidden = [
        _symbol(f"symbol:h{i}", "HiddenWidget", f"pkg.h{i}.HiddenWidget")
        for i in range(8)
    ]
    visible = [_symbol("symbol:v", "OtherWidget", "pkg.OtherWidget")]
    deployment = _symbol_graph(clean_runtime, tmp_path, visible=visible, hidden=hidden)
    hidden_only, absent = _enforced_page(
        clean_runtime, deployment, monkeypatch, "HiddenWidget", "AbsentWidget",
        completeness="exact",
    )
    assert _ids(hidden_only) == _ids(absent) == []
    for key in ("total_count", "total_count_basis", "truncated", "next_offset",
                "rank_capped", "completeness"):
        assert hidden_only.get(key) == absent.get(key), key
    # Both names run the step, so both carry the same note.
    assert hidden_only["exact_name_cap"] == absent["exact_name_cap"] == {"cause": "enforced"}
    assert hidden_only.get("coverage") == absent.get("coverage")


def test_seven_visible_same_name_symbols_are_all_served_counted_and_paged(
    clean_runtime: str, tmp_path,
) -> None:
    # The two symbols whose qualified name is the name sort LAST by node id;
    # the name-only matches sort first. Relevance orders the page, not the id.
    qualified = [
        _symbol(f"symbol:z{i}", "WidgetKit", "WidgetKit") for i in range(2)
    ]
    name_only = [
        _symbol(f"symbol:a{i}", "WidgetKit", f"pkg.a{i}.WidgetKit") for i in range(5)
    ]
    deployment = _symbol_graph(clean_runtime, tmp_path, visible=qualified + name_only)
    expected = [f"symbol:z{i}" for i in range(2)] + [f"symbol:a{i}" for i in range(5)]
    first = _run(clean_runtime, deployment, "WidgetKit", limit=3, completeness="exact")
    assert first["total_count"] == 7
    assert first["total_count_basis"] == "exact"
    assert first["truncated"] is True
    served = _ids(first)
    offset = first["next_offset"]
    while offset is not None:
        page = _run(
            clean_runtime, deployment, "WidgetKit", limit=3, offset=offset,
            completeness="exact",
        )
        assert page["total_count"] == 7
        served += _ids(page)
        offset = page["next_offset"]
    assert served == expected
    whole = _run(clean_runtime, deployment, "WidgetKit", limit=50, completeness="exact")
    assert _ids(whole) == expected
    assert whole["truncated"] is False
    assert "exact_name_cap" not in whole


def test_a_list_cut_by_its_bound_says_so_and_is_not_an_exact_count(
    clean_runtime: str, tmp_path, monkeypatch, search_module,
) -> None:
    monkeypatch.setattr(search_module, "EXACT_NAME_ROWS", 3)
    symbols = [
        _symbol(f"symbol:s{i}", "WidgetKit", f"pkg.s{i}.WidgetKit") for i in range(5)
    ]
    deployment = _symbol_graph(clean_runtime, tmp_path, visible=symbols)
    page = _run(clean_runtime, deployment, "WidgetKit", limit=50, completeness="exact")
    assert _ids(page) == ["symbol:s0", "symbol:s1", "symbol:s2"]
    assert page["total_count_basis"] == "lower_bound"
    assert page["exact_name_cap"] == {"cause": "rows", "rows": 3}


@pytest.mark.parametrize("enforce", [False, True])
@pytest.mark.parametrize("query", ["ab_c", "__eq__", "__lt__", "is_ok", "e.g"])
def test_a_name_without_a_three_character_run_is_not_read(
    clean_runtime: str, tmp_path, monkeypatch, search_module,
    query: str, enforce: bool,
) -> None:
    import okg.substrate.mcp.grep_search as grep_search

    symbols = [
        _symbol("symbol:short", "ab_c", "pkg.ab_c"),
        _symbol("symbol:long", "abc_d", "pkg.abc_d"),
        _mention("ref:one", "ab_c __eq__ __lt__ is_ok e.g mentioned here"),
    ]
    deployment = _symbol_graph(clean_runtime, tmp_path, visible=symbols)
    real = grep_search._bounded
    statements: list[str] = []

    def spy(conn, sql, params):
        if "excluded_subtypes" in params:
            statements.append(sql)
        return real(conn, sql, params)

    monkeypatch.setattr(grep_search, "_bounded", spy)
    assert search_module.exact_name_query(query) == query, "the shape must trigger"
    if not enforce:
        # Control: a name with a trigram does run its statement.
        long = _run(clean_runtime, deployment, "abc_d")
        assert len(statements) == 1
        assert _ids(long)[0] == "symbol:long"
        statements.clear()
    if enforce:
        _enforce(clean_runtime, deployment, monkeypatch)

    def run(q):
        if enforce:
            return _enforced_run(clean_runtime, deployment, q, completeness="exact")[0]
        return _run(clean_runtime, deployment, q, completeness="exact")

    short = run(query)
    assert statements == [], "a name with no trigram must not read the segment"
    # No statement ran, so nothing is cut: no note, and the page is the page
    # of the same code with the step off.
    assert "exact_name_cap" not in short
    monkeypatch.setattr(search_module, "exact_name_query", lambda _query: None)
    without_step = run(query)
    assert _facts_of(short) == _facts_of(without_step)
    assert all(row.get("match_kind") != "exact_name" for row in short["results"])


@pytest.mark.parametrize("enforce", [False, True])
def test_a_graph_without_the_trigram_index_skips_the_read(
    clean_runtime: str, tmp_path, monkeypatch, search_module, enforce: bool,
) -> None:
    import okg.substrate.mcp.grep_search as grep_search
    from okg.substrate.bm25_node_search import SearchIndexUnavailable

    deployment = _symbol_graph(
        clean_runtime, tmp_path,
        visible=[
            _symbol("symbol:s", "WidgetKit", "pkg.WidgetKit"),
            _mention("ref:one", "WidgetKit is imported here"),
        ],
    )
    if enforce:
        _enforce(clean_runtime, deployment, monkeypatch)

    def run():
        if enforce:
            return _enforced_run(
                clean_runtime, deployment, "WidgetKit", completeness="exact",
            )[0]
        return _run(clean_runtime, deployment, "WidgetKit", completeness="exact")

    def absent(_conn):
        raise SearchIndexUnavailable("node-text trigram index absent")

    monkeypatch.setattr(grep_search, "resolve_trgm_segment", absent)
    page = run()
    assert all(row.get("match_kind") != "exact_name" for row in page["results"])
    assert "exact_name_cap" not in page
    monkeypatch.setattr(search_module, "exact_name_query", lambda _query: None)
    without_step = run()
    assert without_step["results"], "the control page must have rows"
    assert _facts_of(page) == _facts_of(without_step)


# ---------- hidden rows never decide what the page says about its count ----------

#: The page facts a cut would change.
_COUNT_FACTS = (
    "total_count", "total_count_basis", "truncated", "next_offset", "rank_capped",
    "completeness",
)


def _facts_of(page: dict) -> dict:
    return {
        **{key: page.get(key) for key in _COUNT_FACTS},
        "ids": _ids(page),
        "coverage": page.get("coverage"),
        "exact_name_cap": page.get("exact_name_cap"),
    }


def test_under_enforce_the_scan_bound_is_never_reported_as_a_cut(
    clean_runtime: str, tmp_path, monkeypatch, search_module,
) -> None:
    # The reader may read one symbol, which sorts first; six more the reader
    # may not read follow. The read stops at its scan bound among rows the
    # reader cannot see. The page must equal the page of the same graph read
    # with room to finish: the hidden rows may not make the count a floor.
    visible = [_symbol("symbol:a0", "WidgetKit", "pkg.a0.WidgetKit")]
    hidden = [
        _symbol(f"symbol:h{i}", "WidgetKit", f"pkg.h{i}.WidgetKit") for i in range(6)
    ]
    deployment = _symbol_graph(clean_runtime, tmp_path, visible=visible, hidden=hidden)
    _enforce(clean_runtime, deployment, monkeypatch)
    monkeypatch.setattr(search_module, "_EXACT_NAME_WINDOW", 2)
    monkeypatch.setattr(search_module, "_EXACT_NAME_SCAN_ROWS", 4)
    (bounded,) = _enforced_run(clean_runtime, deployment, "WidgetKit", completeness="exact")
    monkeypatch.setattr(search_module, "_EXACT_NAME_WINDOW", 50)
    monkeypatch.setattr(search_module, "_EXACT_NAME_SCAN_ROWS", 1_000)
    (whole,) = _enforced_run(clean_runtime, deployment, "WidgetKit", completeness="exact")
    assert _ids(whole) == ["symbol:a0"]
    assert whole["total_count_basis"] != "exact"
    assert whole["exact_name_cap"] == {"cause": "enforced"}
    assert _facts_of(bounded) == _facts_of(whole)


def test_under_enforce_a_cut_by_readable_rows_says_enforced_not_rows(
    clean_runtime: str, tmp_path, monkeypatch, search_module,
) -> None:
    # Which bound stops the read first can depend on hidden rows, so the note
    # never says "rows" under enforcement: it says "enforced" whatever cut.
    monkeypatch.setattr(search_module, "EXACT_NAME_ROWS", 2)
    visible = [
        _symbol(f"symbol:a{i}", "WidgetKit", f"pkg.a{i}.WidgetKit") for i in range(4)
    ]
    hidden = [_symbol("symbol:h0", "WidgetKit", "pkg.h0.WidgetKit")]
    deployment = _symbol_graph(clean_runtime, tmp_path, visible=visible, hidden=hidden)
    (page,) = _enforced_page(
        clean_runtime, deployment, monkeypatch, "WidgetKit", completeness="exact",
    )
    assert _ids(page) == ["symbol:a0", "symbol:a1"]
    # Under enforcement the count itself is withheld; the cut is still said.
    assert page["rank_capped"] is True
    assert page["total_count_basis"] != "exact"
    assert page["exact_name_cap"] == {"cause": "enforced"}


def _cancel_name_reads(monkeypatch, *, after: int = 0) -> list[int]:
    """Cancel the exact-name statements (as the pass deadline does) after
    `after` of them have run; returns one entry per statement seen."""
    import okg.substrate.mcp.grep_search as grep_search

    real = grep_search._bounded
    seen: list[int] = []

    def cancelling(conn, sql, params):
        if "excluded_subtypes" in params:
            seen.append(1)
            if len(seen) > after:
                raise grep_search._Cancelled()
        return real(conn, sql, params)

    monkeypatch.setattr(grep_search, "_bounded", cancelling)
    return seen


def _expire_after_windows(monkeypatch, search_module, windows: int) -> dict:
    """Let the pass deadline pass once `windows` windows of the candidate
    list have been checked (the one statement has already returned)."""
    import time

    real_clock = time.monotonic
    real_check = search_module._readable_exact_name_rows
    state = {"windows": 0, "skew": 0.0}

    def counting(*args, **kwargs):
        rows = real_check(*args, **kwargs)
        state["windows"] += 1
        if state["windows"] >= windows:
            state["skew"] = 10_000.0
        return rows

    monkeypatch.setattr(search_module, "_readable_exact_name_rows", counting)
    monkeypatch.setattr(time, "monotonic", lambda: real_clock() + state["skew"])
    return state


@pytest.mark.parametrize("after", [0, 1])
def test_under_enforce_a_time_limit_is_never_reported(
    clean_runtime: str, tmp_path, monkeypatch, search_module, after: int,
) -> None:
    # How long the read takes, and how many rows it found before the limit,
    # depend on rows the reader may not read. The count facts of a cancelled
    # read equal those of the same read run to the end, and neither is exact.
    monkeypatch.setattr(search_module, "_EXACT_NAME_WINDOW", 1)
    visible = [
        _symbol("symbol:a0", "WidgetKit", "pkg.a0.WidgetKit"),
        _mention("ref:one", "WidgetKit is imported here"),
    ]
    hidden = [
        _symbol(f"symbol:h{i}", "WidgetKit", f"pkg.h{i}.WidgetKit") for i in range(4)
    ]
    deployment = _symbol_graph(clean_runtime, tmp_path, visible=visible, hidden=hidden)
    _enforce(clean_runtime, deployment, monkeypatch)
    (whole,) = _enforced_run(clean_runtime, deployment, "WidgetKit", completeness="exact")
    if after == 0:
        seen = _cancel_name_reads(monkeypatch, after=0)
    else:
        seen = _expire_after_windows(monkeypatch, search_module, after)
    (cancelled,) = _enforced_run(clean_runtime, deployment, "WidgetKit", completeness="exact")
    assert seen, "the exact-name statement must have run (and been cancelled)"
    if after:
        assert seen["windows"] == after, "the deadline must pass between windows"
    assert cancelled["results"] and whole["results"]
    assert whole["total_count_basis"] != "exact"
    assert whole["exact_name_cap"] == {"cause": "enforced"}
    def count_facts(page):
        return {k: v for k, v in _facts_of(page).items() if k != "ids"}

    assert count_facts(cancelled) == count_facts(whole)


@pytest.mark.parametrize("bounded", [False, True])
def test_under_enforce_hidden_rows_do_not_change_the_count_facts_and_none_is_exact(
    clean_runtime: str, tmp_path, monkeypatch, search_module, bounded: bool,
) -> None:
    # One graph, two names with the same readable rows: GadgetKit has no
    # hidden twin, WidgetKit has six. The pages say the same, and neither
    # calls its count exact.
    if bounded:
        monkeypatch.setattr(search_module, "_EXACT_NAME_WINDOW", 2)
        monkeypatch.setattr(search_module, "_EXACT_NAME_SCAN_ROWS", 4)
    visible = [
        _symbol("symbol:a0", "WidgetKit", "pkg.a0.WidgetKit"),
        _mention("ref:w", "WidgetKit is imported here"),
        _symbol("symbol:g0", "GadgetKit", "pkg.g0.GadgetKit"),
        _mention("ref:g", "GadgetKit is imported here"),
    ]
    hidden = [
        _symbol(f"symbol:h{i}", "WidgetKit", f"pkg.h{i}.WidgetKit") for i in range(6)
    ]
    deployment = _symbol_graph(clean_runtime, tmp_path, visible=visible, hidden=hidden)
    with_hidden, without_hidden = _enforced_page(
        clean_runtime, deployment, monkeypatch, "WidgetKit", "GadgetKit",
        completeness="exact",
    )
    for page in (with_hidden, without_hidden):
        assert len(page["results"]) == 2
        assert page["total_count_basis"] != "exact"
        assert page["exact_name_cap"] == {"cause": "enforced"}

    def shape(page: dict) -> dict:
        facts = _facts_of(page)
        facts["ids"] = len(facts["ids"])
        return facts

    assert shape(with_hidden) == shape(without_hidden)


@pytest.mark.parametrize("query", ["isOk", "MyClass"])
def test_without_enforcement_a_complete_read_keeps_an_exact_count(
    clean_runtime: str, tmp_path, query: str,
) -> None:
    # Without enforcement the read sees every row, so a complete read is an
    # exact count: the note is for enforced access rules only.
    deployment = _symbol_graph(
        clean_runtime, tmp_path,
        visible=[_symbol("symbol:s", query, f"pkg.{query}")],
    )
    page = _run(clean_runtime, deployment, query, completeness="exact")
    assert _ids(page) == ["symbol:s"]
    assert page["total_count_basis"] == "exact"
    assert "exact_name_cap" not in page


def test_a_time_limit_before_any_row_names_the_limit_and_keeps_the_count(
    clean_runtime: str, tmp_path, monkeypatch, search_module,
) -> None:
    visible = [
        _symbol("symbol:a0", "WidgetKit", "pkg.a0.WidgetKit"),
        _mention("ref:one", "WidgetKit is imported here"),
        _mention("ref:two", "WidgetKit again"),
    ]
    deployment = _symbol_graph(clean_runtime, tmp_path, visible=visible)
    seen = _cancel_name_reads(monkeypatch, after=0)
    cancelled = _run(clean_runtime, deployment, "WidgetKit", completeness="exact")
    assert seen
    monkeypatch.setattr(search_module, "exact_name_query", lambda _query: None)
    without_step = _run(clean_runtime, deployment, "WidgetKit", completeness="exact")
    assert without_step["results"], "the control page must have rows"
    assert cancelled["exact_name_cap"] == {"cause": "time_limit"}
    assert {k: v for k, v in _facts_of(cancelled).items() if k != "exact_name_cap"} == {
        k: v for k, v in _facts_of(without_step).items() if k != "exact_name_cap"
    }
    assert cancelled["total_count_basis"] == without_step["total_count_basis"]


def test_a_time_limit_after_some_rows_serves_them_as_a_lower_bound(
    clean_runtime: str, tmp_path, monkeypatch, search_module,
) -> None:
    monkeypatch.setattr(search_module, "_EXACT_NAME_WINDOW", 1)
    symbols = [
        _symbol(f"symbol:s{i}", "WidgetKit", f"pkg.s{i}.WidgetKit") for i in range(3)
    ]
    deployment = _symbol_graph(clean_runtime, tmp_path, visible=symbols)
    state = _expire_after_windows(monkeypatch, search_module, 1)
    page = _run(clean_runtime, deployment, "WidgetKit", limit=50, completeness="exact")
    assert state["windows"] == 1
    assert _ids(page) == ["symbol:s0"]
    assert page["total_count_basis"] == "lower_bound"
    assert page["exact_name_cap"] == {"cause": "time_limit"}


def test_a_scan_bound_outside_enforcement_is_a_lower_bound_naming_it(
    clean_runtime: str, tmp_path, monkeypatch, search_module,
) -> None:
    monkeypatch.setattr(search_module, "_EXACT_NAME_WINDOW", 2)
    monkeypatch.setattr(search_module, "_EXACT_NAME_SCAN_ROWS", 4)
    symbols = [
        _symbol(f"symbol:s{i}", "WidgetKit", f"pkg.s{i}.WidgetKit") for i in range(6)
    ]
    deployment = _symbol_graph(clean_runtime, tmp_path, visible=symbols)
    page = _run(clean_runtime, deployment, "WidgetKit", limit=50, completeness="exact")
    assert len(_ids(page)) == 4
    assert page["total_count_basis"] == "lower_bound"
    assert page["exact_name_cap"] == {"cause": "scan_rows", "rows_read": 4}


@pytest.mark.parametrize("enforce", [False, True])
def test_a_common_name_reads_its_candidates_in_one_statement(
    clean_runtime: str, tmp_path, monkeypatch, search_module, enforce: bool,
) -> None:
    # A name shared by many symbols is checked in several windows, but the
    # candidates come from one statement (a statement per window re-read every
    # candidate, 300 ms each for `__init__` on a large graph) and the rows and
    # their order are those of a read with a single large window.
    import okg.substrate.mcp.grep_search as grep_search

    qualified = [_symbol(f"symbol:z{i}", "WidgetKit", "WidgetKit") for i in range(2)]
    name_only = [
        _symbol(f"symbol:a{i}", "WidgetKit", f"pkg.a{i}.WidgetKit") for i in range(5)
    ]
    hidden = [
        _symbol(f"symbol:b{i}", "WidgetKit", f"pkg.b{i}.WidgetKit") for i in range(4)
    ] if enforce else []
    deployment = _symbol_graph(
        clean_runtime, tmp_path, visible=qualified + name_only, hidden=hidden,
    )
    if enforce:
        _enforce(clean_runtime, deployment, monkeypatch)
    run = (
        (lambda: _enforced_run(clean_runtime, deployment, "WidgetKit", limit=50)[0])
        if enforce
        else (lambda: _run(clean_runtime, deployment, "WidgetKit", limit=50))
    )
    real = grep_search._bounded
    statements: list[str] = []

    def spy(conn, sql, params):
        if "excluded_subtypes" in params:
            statements.append(sql)
        return real(conn, sql, params)

    monkeypatch.setattr(grep_search, "_bounded", spy)
    monkeypatch.setattr(search_module, "_EXACT_NAME_WINDOW", 2)
    small_windows = run()
    assert len(statements) == 1, "one statement must read every window's candidates"
    statements.clear()
    monkeypatch.setattr(search_module, "_EXACT_NAME_WINDOW", 50)
    one_window = run()
    assert len(statements) == 1
    expected = [f"symbol:z{i}" for i in range(2)] + [f"symbol:a{i}" for i in range(5)]
    assert _ids(small_windows) == _ids(one_window) == expected
    assert _facts_of(small_windows) == _facts_of(one_window)


# ---------- which queries read at all ----------

@pytest.mark.parametrize("query", [
    "URLSafeSerializer", "WidgetKit", "getUser", "loads_unsafe", "_private",
    "Class.method", "pact.yaml", "README.md", "HTTPError", "JSONDecodeError",
    "MyClass", "IsValid", "OnClick", "PyTorch", "QtCore",
    "  URLSafeSerializer  ",
])
def test_code_shaped_queries_read_the_name(search_module, query: str) -> None:
    assert search_module.exact_name_query(query) == query.strip()


@pytest.mark.parametrize("query", [
    "TODO", "API", "CERN", "README", "HTTP", "APIs", "CERNs", "PDFs", "OAuth",
    "iPhone", "eBay", "500TB", "16GB",
    "PhD", "IoT", "QoS", "PoC", "DoS", "GiB", "MiB", "NaN",
    "serializer", "Serializer", "payload",
    "", "   ", "two words", "WidgetKit extra", "a-b", "x!",
])
def test_plain_words_and_acronyms_do_not_read_the_name(
    search_module, query: str,
) -> None:
    assert search_module.exact_name_query(query) is None


def test_a_word_that_is_not_code_runs_no_exact_name_statement(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    import okg.substrate.mcp.grep_search as grep_search

    deployment = _symbol_graph(
        clean_runtime, tmp_path,
        visible=[_symbol("symbol:s", "WidgetKit", "pkg.WidgetKit")],
    )
    real = grep_search._bounded
    statements: list[str] = []

    def spy(conn, sql, params):
        if "excluded_subtypes" in params:
            statements.append(sql)
        return real(conn, sql, params)

    monkeypatch.setattr(grep_search, "_bounded", spy)
    for query in ("TODO", "API", "CERN", "README", "iPhone"):
        _run(clean_runtime, deployment, query)
    assert statements == []
    _run(clean_runtime, deployment, " WidgetKit ")
    assert len(statements) == 1


def test_a_query_with_surrounding_spaces_finds_the_definition(
    clean_runtime: str, tmp_path,
) -> None:
    deployment = _symbol_graph(
        clean_runtime, tmp_path,
        visible=[_symbol("symbol:s", "WidgetKit", "pkg.WidgetKit")],
    )
    page = _run(clean_runtime, deployment, "  WidgetKit ")
    assert _ids(page)[0] == "symbol:s"
    assert page["results"][0]["match_kind"] == "exact_name"


@pytest.mark.parametrize("query", ["README.md", "load_payload_x", "pkg.nothing_here"])
def test_a_code_shaped_query_naming_no_symbol_leaves_the_page_as_it_was(
    clean_runtime: str, tmp_path, monkeypatch, search_module, query: str,
) -> None:
    import okg.substrate.mcp.grep_search as grep_search

    deployment = _symbol_graph(
        clean_runtime, tmp_path,
        visible=[
            _symbol("symbol:s", "WidgetKit", "pkg.WidgetKit"),
            _mention("ref:one", "see README.md, load_payload_x and pkg.nothing_here"),
            _mention("ref:two", "README.md again"),
        ],
    )
    real = grep_search._bounded
    statements: list[str] = []

    def spy(conn, sql, params):
        if "excluded_subtypes" in params:
            statements.append(sql)
        return real(conn, sql, params)

    monkeypatch.setattr(grep_search, "_bounded", spy)
    # The trigger fires for this query: the exact-name statement runs ...
    assert search_module.exact_name_query(query) == query
    with_step = _run(clean_runtime, deployment, query, limit=50, completeness="exact")
    assert len(statements) == 1
    # ... finds no symbol of that name, and the page is the page without it.
    monkeypatch.setattr(search_module, "exact_name_query", lambda _query: None)
    without_step = _run(clean_runtime, deployment, query, limit=50, completeness="exact")
    assert len(statements) == 1, "the control run must not read the name"
    assert with_step["results"], "the control query must find rows"
    assert _ids(with_step) == _ids(without_step)
    assert _facts_of(with_step) == _facts_of(without_step)
    assert "exact_name_cap" not in with_step
    assert all(row.get("match_kind") != "exact_name" for row in with_step["results"])


@pytest.mark.parametrize("query", ["README.md", "pact.yaml", "__init__"])
def test_under_enforce_a_code_shaped_query_naming_no_symbol_gets_the_enforced_page(
    clean_runtime: str, tmp_path, monkeypatch, search_module, query: str,
) -> None:
    # The cost of the enforced rule, stated by this test: once the step runs,
    # an exact-completeness page carries no count, an unknown retrieval status
    # and an incomplete-listing note, although no symbol bears the name. The
    # same page without the step is exhaustive and counted.
    import okg.substrate.mcp.grep_search as grep_search

    deployment = _symbol_graph(
        clean_runtime, tmp_path,
        visible=[
            _symbol("symbol:s", "WidgetKit", "pkg.WidgetKit"),
            _mention("ref:one", "see README.md, pact.yaml and __init__ here"),
            _mention("ref:two", "README.md, pact.yaml and __init__ again"),
        ],
    )
    real = grep_search._bounded
    statements: list[str] = []

    def spy(conn, sql, params):
        if "excluded_subtypes" in params:
            statements.append(sql)
        return real(conn, sql, params)

    monkeypatch.setattr(grep_search, "_bounded", spy)
    assert search_module.exact_name_query(query) == query
    _enforce(clean_runtime, deployment, monkeypatch)
    (with_step,) = _enforced_run(
        clean_runtime, deployment, query, limit=50, completeness="exact",
    )
    assert len(statements) == 1, "the trigger must run the statement"
    monkeypatch.setattr(search_module, "exact_name_query", lambda _query: None)
    (without_step,) = _enforced_run(
        clean_runtime, deployment, query, limit=50, completeness="exact",
    )
    assert with_step["results"], "the control query must find rows"
    # The rows are the same; what changes is what the page claims about them.
    assert _ids(with_step) == _ids(without_step)
    assert "exact_name_cap" not in without_step
    assert without_step["total_count"] is not None
    assert without_step["total_count_basis"] == "exact"
    assert without_step["coverage"]["retrieval"]["status"] == "exhaustive"
    assert with_step["exact_name_cap"] == {"cause": "enforced"}
    assert with_step["total_count"] is None
    assert with_step["total_count_basis"] != "exact"
    assert with_step["coverage"]["retrieval"]["status"] == "unknown"
    assert with_step["listing"]["complete"] is False


# ---------- path exclusions judge the effective path ----------

def test_an_overlay_that_moves_a_symbol_under_an_excluded_path_keeps_it_out(
    clean_runtime: str, tmp_path,
) -> None:
    from okg.substrate.attributes import (
        materialize_effective_attributes_for_generation,
        record_attribute_observation,
    )
    from okg.substrate.mcp.search_profiles import ResolvedScope

    symbols = [
        _symbol("symbol:kept", "WidgetKit", "pkg.kept.WidgetKit", path="src/kept.py"),
        _symbol("symbol:moved", "WidgetKit", "pkg.moved.WidgetKit", path="src/moved.py"),
    ]
    deployment = _symbol_graph(clean_runtime, tmp_path, visible=symbols)
    scope = ResolvedScope(excluded_path_globs=("/private/**",))
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        before = search(conn, session, "WidgetKit", method="lexical", scope=scope,
                        limit=50, completeness="exact")
        assert _ids(before) == ["symbol:kept", "symbol:moved"]
        record_attribute_observation(
            conn, target_kind="node", target_id="symbol:moved",
            generation_id=session.generation_id, attr_key="path",
            value="/private/moved.py", producer="test:exact-symbol-name",
        )
        materialize_effective_attributes_for_generation(
            conn, generation_id=session.generation_id,
        )
        after = search(conn, session, "WidgetKit", method="lexical", scope=scope,
                       limit=50, completeness="exact")
    assert _ids(after) == ["symbol:kept"]
    assert after["total_count"] == 1


# ---------- the count basis says what rank_capped says ----------

@pytest.mark.parametrize("floor", [False, True])
def test_the_filtered_fold_count_basis_follows_rank_capped(
    search_module, floor: bool,
) -> None:
    from types import SimpleNamespace

    # An exact-name cut (or the enforced note) sets `rank_capped` after the
    # fold; the fold's own flag is False. The basis must agree with the page.
    basis = search_module._count_basis(
        rank_by="records", rank_capped=floor, rollup_chunk_cap_reached=None,
        fold=SimpleNamespace(lower_bound=False),
    )
    assert basis["total_count_basis"] == ("lower_bound" if floor else "exact")


# ---------- any record is found by its own name (issue 3959) ----------

RELEASE = "cmssw_release:CMSSW_14_0_0"
SIBLINGS = [
    "cmssw_release:CMSSW_14_0_0_MULTIARCHS",
    "cmssw_release:CMSSW_14_0_14_MULTIARCHS",
]


def _release(node_id: str, subtype: str, name: str, text: str):
    return node(node_id, subtype, text, name=name)


def _release_rows() -> list:
    """The exact release (a long text: six architectures) beside look-alikes
    whose short texts repeat the query's words, and its family."""
    architectures = " ".join(
        f"{os_}_{cpu}_gcc12"
        for os_ in ("el8", "el9", "slc7")
        for cpu in ("amd64", "aarch64")
    )
    return [
        _release(
            RELEASE, "cmssw_release", "CMSSW_14_0_0",
            f"CMSSW_14_0_0 release. Architectures {architectures}. "
            "Packages, tools and data formats ship with the release.",
        ),
        _release(
            SIBLINGS[0], "cmssw_release", "CMSSW_14_0_0_MULTIARCHS",
            "CMSSW 14 0 0 CMSSW_14_0_0 MULTIARCHS CMSSW 14 0 0",
        ),
        _release(
            SIBLINGS[1], "cmssw_release", "CMSSW_14_0_14_MULTIARCHS",
            "CMSSW 14 0 14 CMSSW_14_0_0 MULTIARCHS CMSSW 14 0 0",
        ),
        _release(
            "cmssw_release_family:CMSSW_14_0", "cmssw_release_family",
            "CMSSW_14_0", "CMSSW 14 0 0 family CMSSW_14_0_0 CMSSW 14 0 0",
        ),
    ]


@pytest.mark.parametrize("flt", [{}, {"subtype": "cmssw_release"}])
def test_a_record_that_is_not_code_is_found_first_by_its_exact_name(
    clean_runtime: str, tmp_path, flt: dict,
) -> None:
    deployment = _symbol_graph(clean_runtime, tmp_path, visible=_release_rows())
    page = _run(clean_runtime, deployment, "CMSSW_14_0_0", filter=flt, limit=10)
    assert _ids(page)[0] == RELEASE
    assert page["results"][0]["match_kind"] == "exact_name"
    # The look-alikes stay on the page behind it, with the family unless the
    # subtype filter drops it.
    assert set(SIBLINGS) <= set(_ids(page))
    family = "cmssw_release_family:CMSSW_14_0" in _ids(page)
    assert family is (not flt)
    assert all(row.get("match_kind") != "exact_name" for row in page["results"][1:])


def test_an_import_or_a_reference_does_not_outrank_the_definition(
    clean_runtime: str, tmp_path,
) -> None:
    definition = _symbol("symbol:z-def", "WidgetKit", "pkg.z.WidgetKit")
    mentions = [
        node("import:a", "code_import", "WidgetKit", name="WidgetKit"),
        node("reference:a", "code_reference", "WidgetKit WidgetKit", name="WidgetKit"),
    ]
    deployment = _symbol_graph(
        clean_runtime, tmp_path, visible=[definition, *mentions],
    )
    page = _run(clean_runtime, deployment, "WidgetKit", limit=10, completeness="exact")
    assert _ids(page)[0] == "symbol:z-def"
    assert page["results"][0]["match_kind"] == "exact_name"
    # The mentions are still served, as BM25 rows, never as a name match.
    assert {"import:a", "reference:a"} <= set(_ids(page))
    assert all(row.get("match_kind") != "exact_name" for row in page["results"][1:])
    assert page["total_count_basis"] == "exact"
    assert "exact_name_cap" not in page


def test_a_same_name_row_outside_the_filter_or_the_record_scope_stays_out(
    clean_runtime: str, tmp_path,
) -> None:
    other = _release("ticket:CMSSW_14_0_0", "ticket", "CMSSW_14_0_0", "CMSSW_14_0_0 ticket")
    deployment = _symbol_graph(
        clean_runtime, tmp_path, visible=[*_release_rows(), other],
    )
    both = _run(clean_runtime, deployment, "CMSSW_14_0_0", limit=10)
    assert _ids(both)[:2] == [RELEASE, "ticket:CMSSW_14_0_0"]
    filtered = _run(
        clean_runtime, deployment, "CMSSW_14_0_0",
        filter={"subtype": "cmssw_release"}, limit=10,
    )
    assert "ticket:CMSSW_14_0_0" not in _ids(filtered)
    assert _ids(filtered)[0] == RELEASE
    # A record scope that names the siblings but not the exact row keeps it out.
    scoped = _run(
        clean_runtime, deployment, "CMSSW_14_0_0",
        record_scope={"records": SIBLINGS}, limit=10,
    )
    assert set(_ids(scoped)) == set(SIBLINGS)
    assert all(row.get("match_kind") != "exact_name" for row in scoped["results"])
    # A scope that names it keeps it first.
    named = _run(
        clean_runtime, deployment, "CMSSW_14_0_0",
        record_scope={"records": [RELEASE, *SIBLINGS]}, limit=10,
    )
    assert _ids(named)[0] == RELEASE


def test_under_enforce_an_unreadable_record_of_the_same_name_stays_out(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    hidden = [_release(
        "cmssw_release:A-hidden", "cmssw_release", "CMSSW_14_0_0", "CMSSW_14_0_0 hidden",
    )]
    deployment = _symbol_graph(
        clean_runtime, tmp_path, visible=_release_rows(), hidden=hidden,
    )
    (page,) = _enforced_page(
        clean_runtime, deployment, monkeypatch, "CMSSW_14_0_0", limit=10,
        completeness="exact",
    )
    assert "cmssw_release:A-hidden" not in _ids(page)
    assert _ids(page)[0] == RELEASE
    assert page["total_count_basis"] != "exact"
    assert page["exact_name_cap"] == {"cause": "enforced"}


@pytest.mark.parametrize("rows", [50, 1])
def test_a_record_with_only_a_name_does_not_outrank_a_definition_by_node_id(
    clean_runtime: str, tmp_path, monkeypatch, search_module, rows: int,
) -> None:
    # A method is named by its short name (`load_payload`) and by a qualified
    # one (`Foo.load_payload`). A record type that carries only a name sorts
    # ahead of `symbol:` ids and used to take its places, past the row cut too.
    monkeypatch.setattr(search_module, "EXACT_NAME_ROWS", rows)
    hotspots = [
        _release(
            f"profile-hotspot:run{i}:load_payload", "profile_hotspot",
            "load_payload", f"load_payload hotspot run{i}",
        )
        for i in range(3)
    ]
    definition = _symbol("symbol:z-def", "load_payload", "Foo.load_payload")
    other = _symbol("symbol:zz-other", "load_payload", "Bar.load_payload")
    deployment = _symbol_graph(
        clean_runtime, tmp_path, visible=[*hotspots, definition, other],
    )
    page = _run(clean_runtime, deployment, "load_payload", limit=10, completeness="exact")
    served = [
        row["node_id"] for row in page["results"] if row.get("match_kind") == "exact_name"
    ]
    # The records that have a qualified name come first, each group by node id.
    expected = ["symbol:z-def", "symbol:zz-other", *(h.node_id for h in hotspots)]
    assert served == expected[:rows]
