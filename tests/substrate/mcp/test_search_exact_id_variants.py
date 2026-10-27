"""A query that is a record's exact identifier lists that record first, also
when the query spells the identifier with other separators.

PACT search-exact-id-variants-3959 (Refs #3959). The fixture is generic: the
subtypes are `release`, `symbol`, `wiki_page`, `report` and `item`; the
CMS-looking strings are only attribute values. Pages carry a label and a title
but no `name`, and declare their identifier fields with `identifier_fields`
(the catalog annotation `okg_identifier_fields`), as real documentation pages
and report records do.
"""
from __future__ import annotations

import sys

import psycopg
import pytest

from okg.substrate.mcp import open_session, search
from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp.search_fixture import make_search_deployment, node, publish_nodes

pytestmark = pytest.mark.postgres

NO_SEPARATOR_FOLLOW_UP = (
    "A query with fewer separators than the stored text (no separator at all against a "
    "hyphenated id) matches neither the query as typed nor the joined key in the stored "
    "text, and no trigram pattern can serve it. Needs a stored identifier key with a GIN "
    "index (follow-up: key index for exact-id search, a live migration)."
)


def _releases() -> list:
    """The exact release has the longest text; the look-alikes repeat the
    query's words in short texts, which BM25 length normalisation prefers.
    `CMSSW_1_40_0` has the same key as `CMSSW_14_0_0` and is another release."""
    architectures = " ".join(
        f"{os_}_{cpu}_gcc12"
        for os_ in ("el8", "el9", "slc7")
        for cpu in ("amd64", "aarch64")
    )
    return [
        node("release:CMSSW_14_0_0", "release",
             f"CMSSW_14_0_0 release. Architectures {architectures}. "
             "Packages, tools and data formats ship with the release.",
             name="CMSSW_14_0_0"),
        node("release:CMSSW_14_0_0_MULTIARCHS", "release",
             "CMSSW 14 0 0 CMSSW_14_0_0 MULTIARCHS CMSSW 14 0 0",
             name="CMSSW_14_0_0_MULTIARCHS"),
        node("release:CMSSW_14_0_14_MULTIARCHS", "release",
             "CMSSW 14 0 14 CMSSW_14_0_0 MULTIARCHS CMSSW 14 0 0",
             name="CMSSW_14_0_14_MULTIARCHS"),
        node("release:CMSSW_14_0_0_pre0_ROOT630", "release",
             "CMSSW 14 0 0 pre0 ROOT630 CMSSW_14_0_0",
             name="CMSSW_14_0_0_pre0_ROOT630"),
        # Same key (cmssw1400), another release; its text carries the key so
        # that the candidate read does reach it.
        node("release:CMSSW_1_40_0", "release",
             "CMSSW_1_40_0 CMSSW1400 release notes", name="CMSSW_1_40_0"),
        # An import never takes a place, whatever it is named.
        node("import:CMSSW_14_0_0", "code_import", "CMSSW_14_0_0 imported",
             name="CMSSW_14_0_0"),
    ]


def _symbols() -> list:
    """Two code symbols whose names differ only in case and separators."""
    return [
        node("symbol:utf8_decode2", "symbol", "def utf8_decode2(data) decode", name="utf8_decode2"),
        node("symbol:Utf8Decode2", "symbol", "class Utf8Decode2 decode utf8decode2",
             name="Utf8Decode2"),
    ]


def _page(node_id: str, label: str, text: str, **attrs):
    """A documentation page: a label and a title, no `name`."""
    return node(node_id, "wiki_page", text, label=label, title=label, **attrs)


def _pages() -> list:
    body = " ".join(f"section{i} of the Higgs analysis review." for i in range(12))
    return [
        _page("page:HIG18026", "HIG18026", f"HIG18026 Higgs analysis twiki. {body}",
              analysis_id="HIG-18-026"),
        _page("page:HIG18026Review", "HIG18026Review", "HIG-18-026 review HIG-18-026 analysis",
              analysis_id="HIG-18-026, HIG-18-027"),
        _page("page:HIG18027", "HIG18027", "HIG-18-026 analysis note HIG-18-026 Higgs",
              analysis_id="HIG-18-027"),
        _page("page:HIG18025", "HIG18025", "HIG-18-026 Higgs analysis HIG-18-026"),
        _page("page:HIGGSWorkBook", "HIGGSWorkBook", "Higgs analysis HIG-18-026 workbook"),
        _page("page:HIG-18-031", "HIG-18-031", f"HIG-18-031 Higgs analysis twiki. {body}"),
        _page("page:HIG18031Notes", "HIG18031Notes", "HIG18031 analysis notes HIG18031 Higgs"),
        _page("page:HIG18030", "HIG18030", "HIG18031 Higgs analysis HIG18031"),
    ]


def _report(node_id: str, label: str, text: str, **attrs):
    """A report record: its report number is its label, no `name`."""
    return node(node_id, "report", text, label=label, report_number=label, **attrs)


def _reports() -> list:
    body = " ".join(f"chapter{i} of the combination note." for i in range(12))
    return [
        _report("report:CMS-PAS-HIG-18-026", "CMS-PAS-HIG-18-026",
                f"CMS-PAS-HIG-18-026 Higgs combination. {body}", analysis_code="HIG-18-026"),
        _report("report:CMS-PAS-HIG-18-027", "CMS-PAS-HIG-18-027",
                "CMS-PAS-HIG-18-026 CMS PAS HIG 18 026 note", analysis_code="HIG-18-027"),
        _report("report:CMS-PAS-HIG-18-026-v2", "CMS-PAS-HIG-18-026-v2",
                "CMS-PAS-HIG-18-026 CMS PAS HIG 18 026", analysis_code="HIG-18-026v2"),
        _report("report:CMS-PAS-HIG-17-026", "CMS-PAS-HIG-17-026",
                "CMS-PAS-HIG-18-026 CMS PAS HIG 17 026", analysis_code="HIG-17-026"),
        # Two reports share one analysis code (a PAS and its paper).
        _report("report:CMS-PAS-EXO-08-004", "CMS-PAS-EXO-08-004",
                f"CMS-PAS-EXO-08-004 Exotica search. {body}", analysis_code="EXO-08-004"),
        _report("report:CMS-PAPER-EXO-08-004", "CMS-PAPER-EXO-08-004",
                f"CMS-PAPER-EXO-08-004 Exotica paper. {body}", analysis_code="EXO-08-004"),
        _report("report:CMS-PAS-EXO-08-005", "CMS-PAS-EXO-08-005",
                "EXO-08-004 EXO-08-004 follow-up", analysis_code="EXO-08-005"),
    ]


SUBTYPES = [
    SubtypeSpec("release", metadata={"searchable_text_fields": ["name", "text"]}),
    SubtypeSpec("symbol", metadata={"searchable_text_fields": ["name", "text"]}),
    SubtypeSpec("code_import", metadata={"searchable_text_fields": ["name", "text"]}),
    SubtypeSpec("wiki_page", metadata={
        "searchable_text_fields": ["label", "analysis_id", "text"],
        "identifier_fields": ["label", "analysis_id"],
    }),
    SubtypeSpec("report", metadata={
        "searchable_text_fields": ["label", "analysis_code", "text"],
        "identifier_fields": ["label", "analysis_code"],
    }),
]


@pytest.fixture
def deployment(clean_runtime: str, tmp_path) -> str:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=SUBTYPES,
        nodes=[*_releases(), *_symbols(), *_pages(), *_reports()],
    )
    return deployment


@pytest.fixture
def search_module():
    return sys.modules["okg.substrate.mcp.search"]


@pytest.fixture
def step_off(monkeypatch, search_module):
    """Switch the id step off (code-shaped names keep their own trigger)."""
    def off() -> None:
        # raising=False: on a base without the id step the guard cases compare
        # the same code with itself, and pass; only behavioural cases fail there.
        monkeypatch.setattr(
            search_module, "exact_id_query", lambda _query: None, raising=False,
        )
    return off


def _run(clean_runtime: str, deployment: str, query: str, **kwargs) -> dict:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        return search(conn, session, query, method="lexical", **kwargs)


def _of(name: str) -> dict:
    return {"op": "eq", "field": "type", "value": name}


def _ids(page: dict) -> list[str]:
    return [row["node_id"] for row in page["results"]]


def _kinds(page: dict) -> dict[str, str]:
    return {
        row["node_id"]: row["match_kind"]
        for row in page["results"]
        if row.get("match_kind") in ("exact_name", "exact_name_variant")
    }


_FACTS = (
    "total_count", "total_count_basis", "truncated", "next_offset", "rank_capped",
    "completeness",
)


def _facts_of(page: dict) -> dict:
    return {
        **{key: page.get(key) for key in _FACTS},
        "ids": _ids(page),
        "coverage": page.get("coverage"),
        "exact_name_cap": page.get("exact_name_cap"),
    }


CASES = [
    pytest.param("CMSSW_14_0_0", {"where": _of("release")}, "release:CMSSW_14_0_0",
                 "exact_name", id="a-release-typed"),
    pytest.param("CMSSW_14_0_0", {}, "release:CMSSW_14_0_0", "exact_name",
                 id="a-release-untyped"),
    # The page has a label and a title but no name; its analysis_id equals the query.
    pytest.param("HIG-18-026", {"where": _of("wiki_page")}, "page:HIG18026", "exact_name",
                 id="b-hyphenated-query-finds-page-by-declared-id"),
    pytest.param("HIG-18-026", {}, "page:HIG18026", "exact_name",
                 id="b-hyphenated-query-untyped"),
    # The label (a declared field) equals the query.
    pytest.param("HIG18026", {"where": _of("wiki_page")}, "page:HIG18026", "exact_name",
                 id="b-unseparated-query-finds-page-by-label"),
    pytest.param("CMS-PAS-HIG-18-026", {"where": _of("report")},
                 "report:CMS-PAS-HIG-18-026", "exact_name", id="c-report-number-typed"),
    pytest.param("CMS-PAS-HIG-18-026", {}, "report:CMS-PAS-HIG-18-026", "exact_name",
                 id="c-report-number-untyped"),
    # The bare analysis code finds the report record by its declared code.
    pytest.param("EXO-08-004", {"where": _of("report")}, "report:CMS-PAPER-EXO-08-004",
                 "exact_name", id="d-bare-analysis-code-typed"),
    pytest.param("EXO-08-004", {}, "report:CMS-PAPER-EXO-08-004", "exact_name",
                 id="d-bare-analysis-code-untyped"),
    # An underscore query finds the hyphen-spelled report number: the dashed pattern.
    pytest.param("CMS_PAS_HIG_18_026", {"where": _of("report")},
                 "report:CMS-PAS-HIG-18-026", "exact_name_variant",
                 id="e-underscore-query-finds-hyphenated-label"),
    pytest.param("CMS.PAS.HIG.18.026", {}, "report:CMS-PAS-HIG-18-026", "exact_name_variant",
                 id="e-dotted-query-finds-hyphenated-label"),
]


@pytest.mark.parametrize(("query", "kwargs", "expected", "kind"), CASES)
def test_the_exact_record_is_listed_first(
    clean_runtime: str, deployment: str, query: str, kwargs: dict, expected: str, kind: str,
) -> None:
    page = _run(clean_runtime, deployment, query, limit=5, **kwargs)
    ids = _ids(page)
    assert ids[:1] == [expected], f"top 5 for {query!r} {kwargs}: {ids}"
    assert page["results"][0]["match_kind"] == kind
    assert len(ids) == len(set(ids)), "a record appears once"


@pytest.mark.xfail(strict=True, reason=NO_SEPARATOR_FOLLOW_UP)
def test_a_query_with_fewer_separators_than_the_stored_text(
    clean_runtime: str, deployment: str,
) -> None:
    page = _run(clean_runtime, deployment, "HIG18031", limit=5, where=_of("wiki_page"))
    assert _ids(page)[:1] == ["page:HIG-18-031"], _ids(page)


def test_a_comma_joined_declared_id_lists_every_page_that_bears_it(
    clean_runtime: str, deployment: str,
) -> None:
    page = _run(clean_runtime, deployment, "HIG-18-027", limit=5,
                where=_of("wiki_page"))
    # The review page lists the id second in its comma-joined analysis_id.
    assert _ids(page)[:2] == ["page:HIG18026Review", "page:HIG18027"]
    assert [row["match_kind"] for row in page["results"][:2]] == ["exact_name"] * 2


def test_a_part_of_an_id_is_not_the_id(clean_runtime: str, deployment: str) -> None:
    page = _run(clean_runtime, deployment, "HIG-18-02", limit=5)
    assert _kinds(page) == {}


def test_two_reports_that_share_a_code_are_both_listed_in_node_id_order(
    clean_runtime: str, deployment: str,
) -> None:
    page = _run(clean_runtime, deployment, "EXO-08-004", limit=5, where=_of("report"))
    assert _ids(page)[:2] == ["report:CMS-PAPER-EXO-08-004", "report:CMS-PAS-EXO-08-004"]
    assert _kinds(page) == {
        "report:CMS-PAPER-EXO-08-004": "exact_name",
        "report:CMS-PAS-EXO-08-004": "exact_name",
    }


def test_a_key_only_match_on_a_declared_field_is_a_variant_after_the_literal(
    clean_runtime: str, deployment: str,
) -> None:
    page = _run(clean_runtime, deployment, "HIG18026", limit=5, where=_of("wiki_page"))
    # The review page holds HIG-18-026 in its analysis_id: another spelling.
    assert _ids(page)[:2] == ["page:HIG18026", "page:HIG18026Review"]
    assert _kinds(page) == {
        "page:HIG18026": "exact_name",
        "page:HIG18026Review": "exact_name_variant",
    }


# A variant never beats a literal, and a spelling that differs only in case or
# separator kind comes before one that joins the runs, whatever the node ids.
def test_literal_beats_variant_and_run_equal_beats_joined(
    clean_runtime: str, tmp_path,
) -> None:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    item = SubtypeSpec("item", metadata={
        "searchable_text_fields": ["label", "text"], "identifier_fields": ["label"],
    })
    publish_nodes(clean_runtime, deployment, subtypes=[item], nodes=[
        # Each text holds the label and the key spelling, so the candidate
        # read reaches every record; the order is what is under test.
        node("item:a-joined", "item", "HIG18026 entry one", label="HIG18026"),
        node("item:b-underscore", "item", "HIG_18_026 HIG18026 entry two",
             label="HIG_18_026"),
        node("item:y-lower", "item", "hig-18-026 entry four", label="hig-18-026"),
        node("item:z-literal", "item", "HIG-18-026 entry three", label="HIG-18-026"),
    ])
    page = _run(clean_runtime, deployment, "HIG-18-026", limit=10)
    assert _ids(page)[:4] == [
        "item:z-literal", "item:b-underscore", "item:y-lower", "item:a-joined",
    ]
    assert [row["match_kind"] for row in page["results"][:4]] == [
        "exact_name", "exact_name_variant", "exact_name_variant", "exact_name_variant",
    ]


# ---------- guards ----------

def test_a_query_that_names_no_record_marks_none_and_changes_nothing(
    clean_runtime: str, deployment: str, step_off,
) -> None:
    # Failure case: no record is named HIG-18-099, so nothing is marked as an
    # exact-name answer or a variant, and the page equals the page without the step.
    with_step = _run(clean_runtime, deployment, "HIG-18-099", limit=5)
    assert all(
        row.get("match_kind") not in ("exact_name", "exact_name_variant")
        for row in with_step["results"]
    )
    step_off()
    without_step = _run(clean_runtime, deployment, "HIG-18-099", limit=5)
    assert _facts_of(with_step) == _facts_of(without_step)


def test_key_collisions_of_different_symbols_are_not_variants(
    clean_runtime: str, deployment: str, step_off,
) -> None:
    # CMSSW_1_40_0 has the key of CMSSW_14_0_0 and is another release: neither
    # the query nor the stored value has a hyphen, so the key alone is not a match.
    page = _run(clean_runtime, deployment, "CMSSW_14_0_0", limit=10)
    assert _kinds(page) == {"release:CMSSW_14_0_0": "exact_name"}
    assert "import:CMSSW_14_0_0" not in _kinds(page)
    # utf8_decode2 / Utf8Decode2 are two symbols: the page is the page without the step.
    with_step = _run(clean_runtime, deployment, "utf8_decode2", limit=10)
    assert _kinds(with_step) == {"symbol:utf8_decode2": "exact_name"}
    step_off()
    without_step = _run(clean_runtime, deployment, "utf8_decode2", limit=10)
    assert _facts_of(with_step) == _facts_of(without_step)


@pytest.mark.parametrize("query", [
    "python3", "amd64", "sha256", "x86", "Higgs", "14_0_0", "e-mail", "A-1",
])
def test_an_ordinary_token_gives_the_page_of_the_same_code_without_the_step(
    clean_runtime: str, deployment: str, step_off, query: str,
) -> None:
    with_step = _run(clean_runtime, deployment, query, limit=10)
    step_off()
    without_step = _run(clean_runtime, deployment, query, limit=10)
    assert _facts_of(with_step) == _facts_of(without_step)
    assert _kinds(with_step) == {}


def test_the_type_filter_keeps_an_id_out_of_another_types_page(
    clean_runtime: str, deployment: str,
) -> None:
    page = _run(clean_runtime, deployment, "CMS-PAS-HIG-18-026", limit=10,
                where=_of("wiki_page"))
    assert _kinds(page) == {}
    assert all(row["subtype"] == "wiki_page" for row in page["results"])


def test_one_statement_per_id_query_and_the_old_statement_for_code_names(
    clean_runtime: str, deployment: str, monkeypatch,
) -> None:
    import okg.substrate.mcp.grep_search as grep_search

    real = grep_search._bounded
    statements: list[str] = []

    def spy(conn, sql, params):
        if "excluded_subtypes" in params:
            statements.append(sql)
        return real(conn, sql, params)

    monkeypatch.setattr(grep_search, "_bounded", spy)
    _run(clean_runtime, deployment, "HIG-18-026", limit=5)
    assert len(statements) == 1
    assert "%(id_fields)s" in statements[0] and " ILIKE " in statements[0]
    statements.clear()
    # A plain word and a long phrase read nothing.
    _run(clean_runtime, deployment, "Higgs", limit=5)
    _run(clean_runtime, deployment, "HIG-18-026 review", limit=5)
    assert statements == []
    # A code-shaped name with no digit keeps today's statement.
    _run(clean_runtime, deployment, "load_payload", limit=5)
    assert len(statements) == 1
    assert "%(id_fields)s" not in statements[0] and " ILIKE " not in statements[0]
    # A token with no separator and no run of 4 digits is an ordinary word: no
    # statement, though this deployment declares identifier fields.
    statements.clear()
    for word in ("python3", "amd64", "sha256", "x86"):
        _run(clean_runtime, deployment, word, limit=5)
    assert statements == []
    # A separator-free token with a run of 4 digits takes the id path.
    _run(clean_runtime, deployment, "HIG18026", limit=5)
    assert len(statements) == 1


def test_an_id_with_more_matches_than_the_row_bound_is_a_lower_bound(
    clean_runtime: str, tmp_path, monkeypatch, search_module,
) -> None:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    item = SubtypeSpec("item", metadata={
        "searchable_text_fields": ["label", "text"], "identifier_fields": ["label"],
    })
    publish_nodes(clean_runtime, deployment, subtypes=[item], nodes=[
        node(f"item:{i}", "item", f"HIG-18-026 entry {i}", label="HIG-18-026") for i in range(5)
    ])
    monkeypatch.setattr(search_module, "EXACT_NAME_ROWS", 3)
    page = _run(clean_runtime, deployment, "HIG-18-026", limit=10, completeness="exact")
    assert page["exact_name_cap"] == {"cause": "rows", "rows": 3}
    assert _ids(page)[:3] == ["item:0", "item:1", "item:2"]


def test_sql_and_python_keys_agree(clean_runtime: str, search_module) -> None:
    key_sql = search_module._id_key_sql("%(v)s::text")
    values = [
        "HIG-18-026", "CMS_PAS_HIG_18_026", "cmssw1400", "  a.b ",
        "Kelvin-1", "İstanbul-1", "Straße-9", "ÀB-1 café", "",
    ]
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        for value in values:
            (got,) = conn.execute(f"SELECT {key_sql}", {"v": value}).fetchone()
            assert got == search_module.identifier_key(value), repr(value)


# ---------- access rules ----------

MASKED_SOURCE = "connector:masked_items"


def _masked_deployment(clean_runtime: str, tmp_path) -> str:
    from tests.substrate.mcp.test_search_records import MAIN, READER, _activate, _allow

    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    item = SubtypeSpec("item", metadata={
        "searchable_text_fields": ["analysis_id", "text"],
        "identifier_fields": ["analysis_id"],
        "pii_classes": {"analysis_id": "free_text", "text": "non_pii"},
    })
    publish_nodes(
        clean_runtime,
        deployment,
        subtypes=[item],
        nodes=[node("item:visible", "item", "visible entry ZZZ-11-111", analysis_id="ZZZ-11-111")],
        node_groups=[(MASKED_SOURCE, [
            node("item:masked", "item", "masked entry ZZZ-11-111", analysis_id="ZZZ-11-111"),
        ])],
    )
    _activate(clean_runtime, deployment, [_allow(MAIN), {
        "rule_id": "redact_masked", "effect": "redact", "actions": ["graph.read.mcp"],
        "subjects": [READER], "match": {"source_id": MASKED_SOURCE}, "priority": 100,
        "obligations": [{"type": "redaction"}, {"type": "audit"}],
    }])
    return deployment


def test_under_enforce_a_masked_identifier_field_never_matches(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    from tests.substrate.mcp.test_search_records import READER

    deployment = _masked_deployment(clean_runtime, tmp_path)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment, nomos_subject=READER)
        page = search(conn, session, "ZZZ-11-111", method="lexical", limit=10)
    kinds = _kinds(page)
    # Control: the record the reader may read matches.
    assert kinds.get("item:visible") == "exact_name"
    # The masked record stays readable (so it is not absent for another
    # reason), but its field is not the reader's to compare.
    assert "item:masked" in _ids(page)
    assert "item:masked" not in kinds
    # The cost under enforcement, stated in the PACT: a lower-bound count and a note.
    assert page["exact_name_cap"] == {"cause": "enforced"}
    assert page["total_count_basis"] != "exact"


# ---------- case, the gate for ordinary tokens, and the SQL value split ----------

def _items(clean_runtime: str, tmp_path, nodes: list, *, declare: bool = True) -> str:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    metadata = {"searchable_text_fields": ["label", "text"]}
    if declare:
        metadata["identifier_fields"] = ["label"]
    publish_nodes(clean_runtime, deployment, subtypes=[SubtypeSpec("item", metadata=metadata)],
                  nodes=nodes)
    return deployment


def test_a_stored_value_that_differs_only_in_case_is_a_variant(
    clean_runtime: str, tmp_path,
) -> None:
    # The typed pattern is case-insensitive, also for a query without a hyphen.
    deployment = _items(clean_runtime, tmp_path, [
        node("item:lower", "item", "pkg_1_2_3 notes", label="pkg_1_2_3"),
        node("item:other", "item", "pkg_1_2_30 notes", label="pkg_1_2_30"),
    ])
    page = _run(clean_runtime, deployment, "PKG_1_2_3", limit=5)
    assert _kinds(page) == {"item:lower": "exact_name_variant"}


def test_an_ordinary_token_runs_only_when_a_type_declares_identifier_fields(
    clean_runtime: str, tmp_path, monkeypatch, step_off,
) -> None:
    import okg.substrate.mcp.grep_search as grep_search

    nodes = [
        node("item:one", "item", "built with tool20261 on amd64", label="tool20261"),
        node("item:two", "item", "sha256 of tool20261", label="Other"),
    ]
    real = grep_search._bounded
    statements: list[str] = []

    def spy(conn, sql, params):
        if "excluded_subtypes" in params:
            statements.append(sql)
        return real(conn, sql, params)

    monkeypatch.setattr(grep_search, "_bounded", spy)
    declared = _items(clean_runtime, tmp_path / "declared", nodes)
    page = _run(clean_runtime, declared, "tool20261", limit=5)
    assert len(statements) == 1
    assert _kinds(page) == {"item:one": "exact_name"}
    statements.clear()
    # No type declares identifier fields: no text statement and no cap note.
    # Issue 4053: the name check of the attrs index (an equality, not a text
    # read) still finds the record whose label is the token.
    plain = _items(clean_runtime, tmp_path / "plain", nodes, declare=False)
    with_gate = _run(clean_runtime, plain, "tool20261", limit=5)
    assert statements == []
    assert "exact_name_cap" not in with_gate
    assert _kinds(with_gate) == {"item:one": "exact_name"}
    # A token no record bears keeps the page of the same code without the
    # step, and says plainly that no record is named so.
    absent = _run(clean_runtime, plain, "tool20262kit", limit=5)
    assert statements == []
    assert absent["exact_name_absent"]["name"] == "tool20262kit"
    step_off()
    assert _facts_of(absent) == _facts_of(_run(clean_runtime, plain, "tool20262kit", limit=5))
    # A hyphenated token keeps the statement without declarations.
    monkeypatch.undo()
    monkeypatch.setattr(grep_search, "_bounded", spy)
    _run(clean_runtime, plain, "gpt-4o", limit=5)
    assert len(statements) == 1


def test_the_sql_value_split_matches_the_python_rule(
    clean_runtime: str, tmp_path,
) -> None:
    # A comma list is split in SQL, each part trimmed, empty parts dropped, and
    # only the first 16 values are read. (A JSON list of strings is read by the
    # same statement, but the stamper puts only string values into the stored
    # text, so a list needs its own text; it is covered in the units file.)
    values = [f"ZZ-{i:02d}-100" for i in range(1, 20)]
    padded = ",  " + ",\t ".join(values[:5]) + " ,,  " + ", ".join(values[5:]) + " ,"
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    item = SubtypeSpec("item", metadata={
        "searchable_text_fields": ["analysis_id", "text"],
        "identifier_fields": ["analysis_id"],
    })
    publish_nodes(clean_runtime, deployment, subtypes=[item], nodes=[
        node("item:list", "item", " ".join(values), analysis_id=padded),
    ])
    for position, value in enumerate(values, start=1):
        page = _run(clean_runtime, deployment, value, limit=5)
        kinds = _kinds(page)
        if position <= 16:
            assert kinds == {"item:list": "exact_name"}, (position, value)
        else:
            assert kinds == {}, (position, value)


def test_a_query_finds_a_value_stored_with_any_one_separator(
    clean_runtime: str, tmp_path,
) -> None:
    # The candidate regex joins the query's runs by any one of - _ .
    deployment = _items(clean_runtime, tmp_path, [
        node("item:hyphen", "item", "ABC-12-345 notes", label="ABC-12-345"),
        node("item:under", "item", "abc_12_345 notes", label="abc_12_345"),
        node("item:dot", "item", "ABC.12.345 notes", label="ABC.12.345"),
        node("item:mixed", "item", "ABC-12_345 notes", label="ABC-12_345"),
        node("item:other", "item", "ABC-12-3456 notes", label="ABC-12-3456"),
    ])
    for query in ("ABC-12-345", "ABC_12_345", "abc.12.345"):
        page = _run(clean_runtime, deployment, query, limit=10)
        kinds = _kinds(page)
        assert set(kinds) == {"item:hyphen", "item:under", "item:dot", "item:mixed"}, (
            query, kinds,
        )
        assert "item:other" not in kinds
    # The literal one is first and the only exact match for its own spelling.
    page = _run(clean_runtime, deployment, "ABC-12-345", limit=10)
    assert _ids(page)[0] == "item:hyphen"
    assert _kinds(page)["item:hyphen"] == "exact_name"
