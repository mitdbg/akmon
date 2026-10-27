"""Issue 4053: when the record a search or an inspect names does not exist,
the reply says so plainly, and lists ids of records whose names are near;
when in doubt it says nothing.

PACT read-absent-names-4053. The fixture is generic: wiki pages carry a
title and no `name` (as documentation pages do), a release carries a name,
a ticket a key, a source file its path, a commit its SHA. Before this change
a search for a page title that no page bears answered `rows 0 of count
unknown · completion unknown` and nothing else, so an agent read the empty
page as a retrieval limit and stated a stand-in value. A false "no record is
named X" is worse than that silence, so every case where a record may bear
the name in a way the check cannot see makes no claim.
"""
from __future__ import annotations

import json
import sys

import anyio
import psycopg
import pytest

from okg.substrate.errors import NodeNotFound
from okg.substrate.mcp import open_session, search
from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp.search_fixture import make_search_deployment, node, publish_nodes

pytestmark = pytest.mark.postgres

HIDDEN = "connector:hidden_pages"
#: Pages added for the index-plan test.
FILLER_PAGES = 4000
#: Planner costs the plan is checked under: PostgreSQL's defaults (the CI
#: database runs them) and the tuned lab value (timescaledb-tune's
#: random_page_cost 1.1), so the result does not depend on the host's
#: postgresql.conf. CI run 40237209183 planned the id read at the defaults
#: with the GIN pending list unflushed and read the whole subtype index.
PLANNER_COSTS = (
    (("random_page_cost", "4"), ("effective_cache_size", "4GB"), ("work_mem", "4MB")),
    (("random_page_cost", "1.1"),),
)
#: Hidden pages that bear one title (more than the check reads at once).
CROWD = 55
SHA = "0001302a4fbe9c7d2e1f0a3b5c6d7e8f9a0b1c2d"
UUID = "3fa85f64-5717-4562-b3fc-2c963f66afa6"


def _page(node_id: str, title: str, text: str, **attrs):
    return node(node_id, "wiki_page", text, title=title, **attrs)


NODES = [
    _page("page:ReleaseNotesAgentMay2013", "ReleaseNotesAgentMay2013",
          "ReleaseNotesAgentMay2013 Agent release 0.9.69a notes for May 2013"),
    _page("page:ReleaseNotesAgentApril2013", "ReleaseNotesAgentApril2013",
          "ReleaseNotesAgentApril2013 Agent release 0.9.60 notes"),
    # The home page links the June page, which was never written.
    _page("page:ReleaseNotes", "ReleaseNotes",
          "ReleaseNotes home. June 2013: [[ReleaseNotesAgentJune2013]] "
          "May 2013: [[ReleaseNotesAgentMay2013]]"),
    _page("page:WorkshopAtLisbon2023", "WorkshopAtLisbon2023",
          "WorkshopAtLisbon2023 workshop by the river, 5-10 June 2023"),
    _page("page:WorkshopLisbon2024Stats", "WorkshopLisbon2024Stats",
          "WorkshopLisbon2024Stats statistics exercise for the 2024 workshop"),
    # Its text does not hold its alias: only the attrs index finds it.
    _page("page:SchoolOverview", "SchoolOverview", "Overview of all schools",
          aliases=["SchoolIndex2019"]),
    node("release:pkg_release_14_0_0", "release", "pkg_release_14_0_0 release notes",
         name="pkg_release_14_0_0"),
    node("ticket:OPS-1234", "ticket", "Broken transfer queue", key="OPS-1234"),
    # A file carries its full path; a commit its SHA (neither is a name).
    node("file:repo:src/pkg/search.py", "source_file", "src/pkg/search.py",
         path="src/pkg/search.py", label="src/pkg/search.py"),
    node(f"commit:repo:{SHA}", "git_commit", f"Fix the search index {SHA}",
         label="Fix the search index", commit_sha=SHA),
    # Round 2 review shapes: a spaced title, a name only the id holds, a run
    # keyed by a UUID, a title stored as an array, a module file, and a
    # readable page behind a crowd of hidden same-title pages.
    _page("page:spaced", "Release Notes Agent July 2013",
          "Release Notes Agent July 2013 release notes"),
    # Neither its title nor its text spells TeamCharter2019: only the id does.
    _page("page:TeamCharter2019", "Edition 2019 of the charter", "Charter of the team"),
    node(f"run:{UUID}", "run", "nightly run", run_id=UUID),
    _page("page:arr", ["AlphaPlan2020"], "alpha plan document"),
    node("file:repo:src/pkg/near_names.py", "source_file", "src/pkg/near_names.py",
         path="src/pkg/near_names.py", label="src/pkg/near_names.py"),
    _page("page:MobPublic", "MobPlan2024", "public summary of the plan"),
    # Round 3 review id shapes: the name is a whole word of the id, not its
    # plain last part, and no text spells it.
    *(node(node_id, "run", "a run record")
      for node_id in (
          "run:nightly_build_15.0.1", "run:Deploy2021.final",
          "run:guide.md#BetaSection2022", "run:wiki/TeamCharter2023/",
          "run:https://wiki.example.org/view?page=TeamCharter2024",
          "section:GuideBook2021:intro",
      )),
    # Round 4 review: ids that spell a name with separators, queried as one
    # single-case word, and stored text that does the same.
    *(node(node_id, "run", "a run record")
      for node_id in (
          "run:Team-Charter-2036", "run:Team-Charter-2037", "run:Team-Charter-2038",
          "page:release-notes-june-2041",
          "run:Team-Plan-2060", "run:Team-Plan-2061", "run:ops-plan-2062",
      )),
    _page("page:charter-note", "Charter note", "see Team-Charter-2050 for details"),
    _page("page:plan-note", "Plan note", "see Team-Plan-2064 for details"),
]
HIDDEN_NODES = [
    _page("page:HiddenPlan2024", "HiddenPlan2024", "HiddenPlan2024 secret plan"),
    *(_page(f"page:crowd:{i:02d}", "CrowdPlan2024", f"CrowdPlan2024 copy {i}")
      for i in range(CROWD)),
    *(_page(f"page:mob:{i:02d}", "MobPlan2024", f"MobPlan2024 copy {i}")
      for i in range(CROWD)),
]
SUBTYPES = [
    SubtypeSpec("wiki_page", metadata={
        "searchable_text_fields": ["title", "aliases", "text"],
        "identifier_fields": ["aliases"],
    }),
    SubtypeSpec("release", metadata={"searchable_text_fields": ["name", "text"]}),
    SubtypeSpec("ticket", metadata={"searchable_text_fields": ["key", "text"]}),
    SubtypeSpec("source_file", metadata={"searchable_text_fields": ["path", "text"]}),
    SubtypeSpec("git_commit", metadata={"searchable_text_fields": ["label", "text"]}),
    SubtypeSpec("run", metadata={"searchable_text_fields": ["text"]}),
    SubtypeSpec("section", metadata={"searchable_text_fields": ["text"]}),
]
#: A live type's catalog entry, for the deployment that declares one.
LIVE_SUBTYPE = SubtypeSpec("site", metadata={
    "searchable_text_fields": ["text"],
    "live_relations": {"site_status": {"description": "a site's status, read now"}},
})


@pytest.fixture
def deployment(clean_runtime: str, tmp_path) -> str:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(
        clean_runtime, deployment, subtypes=SUBTYPES, nodes=NODES,
        node_groups=[(HIDDEN, HIDDEN_NODES)],
    )
    return deployment


@pytest.fixture
def search_module():
    return sys.modules["okg.substrate.mcp.search"]


def _run(dsn: str, deployment: str, query: str, **kwargs) -> dict:
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        return search(conn, session, query, method="lexical", **kwargs)


def _ids(page: dict) -> list[str]:
    return [row["node_id"] for row in page["results"]]


def _kind(page: dict, node_id: str):
    return next(row.get("match_kind") for row in page["results"] if row["node_id"] == node_id)


def _enforce(dsn: str, deployment: str, monkeypatch) -> None:
    from tests.substrate.mcp.test_search_records import MAIN, _activate, _allow

    _activate(dsn, deployment, [_allow(MAIN)])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")


def _enforced_session(conn, deployment: str):
    from tests.substrate.mcp.test_search_records import READER

    return open_session(conn, deployment_name=deployment, nomos_subject=READER)


def _drop_index(dsn: str, suffix: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        (name,) = conn.execute(
            "SELECT quote_ident(schemaname) || '.' || quote_ident(indexname) "
            "FROM pg_indexes WHERE indexname ~ %s", (suffix + "$",),
        ).fetchone()
        conn.execute(f"DROP INDEX {name}")


def _invalidate_index(dsn: str, suffix: str) -> None:
    """Mark the index Postgres holds, not drop it, as a failed
    `CREATE INDEX CONCURRENTLY` leaves it: present in `pg_indexes` with
    `indisvalid = false`. Each test has its own database, so nothing is restored."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        (name,) = conn.execute(
            "SELECT quote_ident(schemaname) || '.' || quote_ident(indexname) "
            "FROM pg_indexes WHERE indexname ~ %s", (suffix + "$",),
        ).fetchone()
        conn.execute(
            "UPDATE pg_index SET indisvalid = false WHERE indexrelid = %s::regclass", (name,),
        )
        assert conn.execute(
            "SELECT indisvalid FROM pg_index WHERE indexrelid = %s::regclass", (name,),
        ).fetchone() == (False,)
        assert conn.execute(
            "SELECT count(*) FROM pg_indexes WHERE indexname ~ %s", (suffix + "$",),
        ).fetchone() == (1,)


#: The page facts a hidden row must never change.
_FACTS = (
    "total_count", "total_count_basis", "truncated", "next_offset", "rank_capped",
    "completeness", "coverage", "exact_name_cap", "exact_name_absent",
)


# ── search: the claim ─────────────────────────────────────────────────────


def test_a_title_no_record_bears_is_said_absent_with_near_ids(
    clean_runtime: str, deployment: str,
) -> None:
    # On dev the exact-name step found no record (a title is not a name) and
    # the page said nothing: `rows 0 of count unknown`.
    page = _run(clean_runtime, deployment, "WorkshopAtLisbon2024")
    assert _ids(page) == []
    absent = page["exact_name_absent"]
    assert absent["name"] == "WorkshopAtLisbon2024"
    assert absent["within_filter"] is False
    assert "readable_only" not in absent
    assert {"name", "qualified_name", "title", "label", "key", "aliases", "node_id"} <= set(
        absent["checked"])
    assert absent["near_ids"] == ["page:WorkshopAtLisbon2023"]


def test_a_record_named_by_its_title_is_found_first_as_an_exact_name(
    clean_runtime: str, deployment: str,
) -> None:
    for title in ("ReleaseNotesAgentMay2013", "WorkshopAtLisbon2023"):
        page = _run(clean_runtime, deployment, title)
        assert _ids(page)[0] == f"page:{title}"
        assert _kind(page, f"page:{title}") == "exact_name"
        assert "exact_name_absent" not in page


def test_a_declared_identifier_array_and_a_key_name_a_record(
    clean_runtime: str, deployment: str,
) -> None:
    # The alias lives only in an array attribute (not in the stored text):
    # the text-bounded step cannot see it; the attrs index can.
    page = _run(clean_runtime, deployment, "SchoolIndex2019")
    assert _ids(page)[0] == "page:SchoolOverview"
    assert _kind(page, "page:SchoolOverview") == "exact_name"
    ticket = _run(clean_runtime, deployment, "OPS-1234")
    assert _ids(ticket)[0] == "ticket:OPS-1234"
    assert ticket["results"][0]["match_kind"] == "exact_name"
    assert "exact_name_absent" not in ticket


def test_a_filter_narrows_the_check_and_the_page_says_so(
    clean_runtime: str, deployment: str,
) -> None:
    page = _run(clean_runtime, deployment, "ReleaseNotesAgentMay2013",
                where={"op": "eq", "field": "type", "value": "release"})
    assert "page:ReleaseNotesAgentMay2013" not in _ids(page)
    absent = page["exact_name_absent"]
    assert absent["within_filter"] is True
    # The page outside the filter is never named as near either.
    assert "page:ReleaseNotesAgentMay2013" not in absent.get("near_ids", [])


# ── search: when in doubt, no claim ───────────────────────────────────────


def test_a_name_a_row_on_the_page_holds_whole_is_not_claimed_absent(
    clean_runtime: str, deployment: str,
) -> None:
    # The home page links [[ReleaseNotesAgentJune2013]]: the row holds the
    # token, so it may be the record under a field the check cannot read.
    page = _run(clean_runtime, deployment, "ReleaseNotesAgentJune2013")
    assert _ids(page) == ["page:ReleaseNotes"]
    assert "exact_name_absent" not in page
    # Another case of an existing title: the row holding it is on the page.
    other_case = _run(clean_runtime, deployment, "workshopatLISBON2023")
    assert "exact_name_absent" not in other_case


@pytest.mark.parametrize("query", ["search.py", "README.md", "pkg.search"])
def test_a_file_name_or_dotted_name_is_never_claimed_absent(
    clean_runtime: str, deployment: str, query: str,
) -> None:
    # `search.py` is the last part of a stored path; the check reads no path.
    assert "exact_name_absent" not in _run(clean_runtime, deployment, query)


@pytest.mark.parametrize("query", [SHA, SHA[:12], "0abc1234ef"])
def test_a_hash_shaped_token_is_never_claimed_absent(
    clean_runtime: str, deployment: str, query: str,
) -> None:
    # A SHA lives in fields the check does not read (commit_sha).
    assert "exact_name_absent" not in _run(clean_runtime, deployment, query)


def test_a_deployment_with_live_types_claims_nothing(
    clean_runtime: str, tmp_path,
) -> None:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(clean_runtime, deployment, subtypes=[*SUBTYPES, LIVE_SUBTYPE], nodes=NODES)
    page = _run(clean_runtime, deployment, "WorkshopAtLisbon2024")
    assert "exact_name_absent" not in page
    # A stored record is still found by its title.
    found = _run(clean_runtime, deployment, "WorkshopAtLisbon2023")
    assert _kind(found, "page:WorkshopAtLisbon2023") == "exact_name"


def test_where_an_overlay_may_rename_a_record_nothing_is_claimed(
    clean_runtime: str, deployment: str,
) -> None:
    def overlay(generation: str) -> None:
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            conn.execute(
                "INSERT INTO okg.attribute_effective_attrs (target_kind, target_id, "
                "generation_id, attr_key, observation_id, value, value_hash, producer) "
                "VALUES ('node', 'page:SchoolOverview', %s, 'title', %s, "
                "%s::jsonb, 'h', 'test')",
                (generation, f"obs:{generation}", json.dumps("SchoolRenamed2024")),
            )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        generation = str(open_session(conn, deployment_name=deployment).generation_id)
    # Overlays exist for another generation only, and the marker for this one
    # (written by the publish) says zero overlays: the claim stands.
    overlay("gen:older")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO okg.attribute_overlay_generations (generation_id, namespace, "
            "overlay_rows, source) VALUES (%s, 'default', 0, 'materializer') "
            "ON CONFLICT (generation_id, namespace) DO UPDATE SET overlay_rows = 0",
            (generation,),
        )
    assert "exact_name_absent" in _run(clean_runtime, deployment, "SchoolRenamed2025")
    # An overlay at this generation (the marker no longer says zero): the
    # overlay table cannot be searched by value, so nothing is claimed for
    # any name, the overlay-set one included.
    overlay(generation)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.attribute_overlay_generations SET overlay_rows = 1 "
            "WHERE generation_id = %s", (generation,),
        )
    for name in ("SchoolRenamed2024", "SchoolRenamed2025"):
        assert "exact_name_absent" not in _run(clean_runtime, deployment, name), name


def test_a_cut_exact_name_step_claims_nothing(
    clean_runtime: str, deployment: str, monkeypatch, search_module,
) -> None:
    real = search_module._exact_name_rows

    def cut(*args, **kwargs):
        rows, _cause = real(*args, **kwargs)
        return rows, "scan_rows"

    monkeypatch.setattr(search_module, "_exact_name_rows", cut)
    page = _run(clean_runtime, deployment, "WorkshopAtLisbon2024")
    assert page["exact_name_cap"]["cause"] == "scan_rows"
    assert "exact_name_absent" not in page


def test_a_plain_word_or_several_words_run_no_check(
    clean_runtime: str, deployment: str,
) -> None:
    for query in ("overview", "release notes June", '"WorkshopAtLisbon2024"'):
        assert "exact_name_absent" not in _run(clean_runtime, deployment, query), query


def test_without_the_attrs_index_nothing_is_claimed(
    clean_runtime: str, deployment: str,
) -> None:
    _drop_index(clean_runtime, "node_attrs_gin")
    page = _run(clean_runtime, deployment, "WorkshopAtLisbon2024")
    assert "exact_name_absent" not in page
    # And a title is matched only by the step, as before (not marked).
    titled = _run(clean_runtime, deployment, "WorkshopAtLisbon2023")
    assert _kind(titled, "page:WorkshopAtLisbon2023") != "exact_name"


def test_an_invalid_attrs_index_claims_nothing_and_runs_no_name_check(
    clean_runtime: str, deployment: str, monkeypatch,
) -> None:
    import okg.substrate.mcp.grep_search as grep_search

    seen: list[str] = []
    real = grep_search._bounded_with

    def spy(conn, sql, params, **kwargs):
        seen.append(sql)
        return real(conn, sql, params, **kwargs)

    monkeypatch.setattr(grep_search, "_bounded_with", spy)
    # Control: a valid index gives the claim and the name-check statement.
    assert "exact_name_absent" in _run(clean_runtime, deployment, "WorkshopAtLisbon2024")
    assert any("attrs @>" in sql for sql in seen)
    # The index is still there, but Postgres has marked it invalid.
    _invalidate_index(clean_runtime, "node_attrs_gin")
    seen.clear()
    page = _run(clean_runtime, deployment, "WorkshopAtLisbon2024")
    assert "exact_name_absent" not in page
    assert not any("attrs @>" in sql for sql in seen), seen
    # The exact-name step falls back to its text match, as with no index at all.
    titled = _run(clean_runtime, deployment, "WorkshopAtLisbon2023")
    assert _kind(titled, "page:WorkshopAtLisbon2023") != "exact_name"
    assert not any("attrs @>" in sql for sql in seen), seen


def test_an_invalid_id_index_is_read_as_missing(
    clean_runtime: str, deployment: str, monkeypatch, search_module,
) -> None:
    import okg.substrate.mcp.grep_search as grep_search

    _invalidate_index(clean_runtime, "node_id_trgm")
    seen: list[str] = []
    real = grep_search._bounded_with

    def spy(conn, sql, params, **kwargs):
        seen.append(sql)
        return real(conn, sql, params, **kwargs)

    monkeypatch.setattr(grep_search, "_bounded_with", spy)
    # A large graph without a usable id index claims nothing and reads no ids.
    monkeypatch.setattr(
        search_module, "_visible_row_estimate",
        lambda *_a: search_module._NODE_ID_SCAN_MAX_ROWS + 1,
    )
    page = _run(clean_runtime, deployment, "WorkshopAtLisbon2024")
    assert "exact_name_absent" not in page
    assert not any("node_id ~*" in sql for sql in seen), seen
    # At the bound the scan runs (the invalid index answers nothing) and the claim stands.
    monkeypatch.setattr(
        search_module, "_visible_row_estimate",
        lambda *_a: search_module._NODE_ID_SCAN_MAX_ROWS,
    )
    seen.clear()
    assert "exact_name_absent" in _run(clean_runtime, deployment, "WorkshopAtLisbon2024")
    assert any("node_id ~*" in sql for sql in seen)


def test_the_name_check_is_answered_by_the_attrs_index(
    clean_runtime: str, tmp_path, monkeypatch,
) -> None:
    import okg.substrate.mcp.grep_search as grep_search

    # Enough live rows that an index choice is a real choice (on a few dozen
    # rows the planner may read any partial index whole), then statistics.
    # VACUUM moves the rows just written out of each GIN index's pending
    # list, as autovacuum does on a deployment: with the list still full the
    # planner prices a GIN read by the whole list, and whether autovacuum got
    # there first is a race (lab: 10 s naptime; CI: PostgreSQL's 60 s).
    filler = [
        _page(f"page:Filler{i:05d}", f"Filler{i:05d}", f"Filler{i:05d} page body {i}",
              label=f"filler-{i}")
        for i in range(FILLER_PAGES)
    ]
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    publish_nodes(clean_runtime, deployment, subtypes=SUBTYPES, nodes=[*NODES, *filler])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute("VACUUM ANALYZE")

    seen: list[tuple[str, dict]] = []
    real = grep_search._bounded_with

    def spy(conn, sql, params, *, settings, explain=False):
        if "attrs @>" in sql or "node_id ~*" in sql:
            seen.append((sql, dict(params), tuple(settings)))
        return real(conn, sql, params, settings=settings, explain=explain)

    monkeypatch.setattr(grep_search, "_bounded_with", spy)
    page = _run(clean_runtime, deployment, "WorkshopAtLisbon2024")
    assert "exact_name_absent" in page
    # The exact-name step reads first (it also goes through the attrs index);
    # then the name check read, then the id read through the node-id trigram
    # index.
    assert [("attrs @>" in sql, "node_id ~*" in sql) for sql, _p, _s in seen] == [
        (True, False), (True, False), (False, True),
    ]
    for (sql, params, settings), index in zip(seen[1:], ("node_attrs_gin", "node_id_trgm")):
        assert "ORDER BY" not in sql
        # No B-tree walk that stops at the LIMIT may replace the GIN read
        # (on a 250,000-record graph with a full pending list that walk took
        # 575-725 ms against 36-38 ms; search._GIN_READ_SETTINGS).
        assert dict(settings) == {
            "enable_seqscan": "off", "enable_indexscan": "off", "enable_indexonlyscan": "off",
        }, settings
        # The index is valid and on the very table the read queries.
        table = sql.split(" FROM ", 1)[1].split()[0]
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            on_table = conn.execute(
                "SELECT count(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid "
                "WHERE i.indrelid = to_regclass(%s) AND c.relname ~ %s "
                "AND i.indisvalid AND i.indisready",
                (table, index + "$"),
            ).fetchone()[0]
            assert on_table == 1, (table, index)
            for costs in PLANNER_COSTS:
                lines = real(conn, sql, params, settings=(*settings, *costs), explain=True)
                plan = "\n".join(
                    " ".join(str(v) for v in line.values()) if isinstance(line, dict)
                    else str(line)
                    for line in lines
                )
                assert index in plan, (costs, plan)


def test_the_check_has_a_time_limit_and_says_nothing_past_it(
    clean_runtime: str, deployment: str, monkeypatch,
) -> None:
    import okg.substrate.mcp.grep_search as grep_search

    real = grep_search._bounded_with

    def cancelled(conn, sql, params, **kwargs):
        # Only the name check's statement runs out of time.
        if "attrs @>" in sql:
            raise grep_search._Cancelled()
        return real(conn, sql, params, **kwargs)

    monkeypatch.setattr(grep_search, "_bounded_with", cancelled)
    page = _run(clean_runtime, deployment, "WorkshopAtLisbon2024")
    assert "exact_name_absent" not in page


def test_under_enforce_no_claim_and_hidden_names_read_like_absent_ones_at_any_count(
    clean_runtime: str, deployment: str, monkeypatch,
) -> None:
    _enforce(clean_runtime, deployment, monkeypatch)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = _enforced_session(conn, deployment)
        # Nobody, one hidden page, 55 hidden pages (more than the check reads).
        pages = [
            search(conn, session, query, method="lexical")
            for query in ("NobodyPlan2024", "HiddenPlan2024", "CrowdPlan2024")
        ]
        readable = search(conn, session, "WorkshopAtLisbon2023", method="lexical")
    for page in pages:
        assert _ids(page) == []
        assert "exact_name_absent" not in page
    facts = [{key: page.get(key) for key in _FACTS} for page in pages]
    assert facts[0] == facts[1] == facts[2]
    # Under access rules the name check does not run: the readable page is
    # served as before this change (by its words), not marked exact_name.
    assert "page:WorkshopAtLisbon2023" in _ids(readable)
    assert _kind(readable, "page:WorkshopAtLisbon2023") != "exact_name"


@pytest.mark.parametrize("query", [
    "Workshop-At-Lisbon-2023", "Workshop_At_Lisbon_2023", "ReleaseNotesAgentJuly2013",
])
def test_an_existing_name_in_another_spelling_is_never_claimed_absent(
    clean_runtime: str, deployment: str, query: str,
) -> None:
    # Separators against WorkshopAtLisbon2023 (that page is a row of the
    # page), and one CamelCase word against the spaced title "Release Notes
    # Agent July 2013" (no row on the page: the stored text is read in any
    # spelling).
    assert "exact_name_absent" not in _run(clean_runtime, deployment, query)


@pytest.mark.parametrize("query", ["TeamCharter2019", "Team-Charter-2019", UUID, "near_names"])
def test_a_name_an_id_holds_is_never_claimed_absent(
    clean_runtime: str, deployment: str, query: str,
) -> None:
    # page:TeamCharter2019 (title and text never spell the word), a run keyed
    # by a UUID, and the stem of src/pkg/near_names.py.
    assert "exact_name_absent" not in _run(clean_runtime, deployment, query)


def test_a_title_stored_as_an_array_names_the_record(
    clean_runtime: str, deployment: str,
) -> None:
    page = _run(clean_runtime, deployment, "AlphaPlan2020")
    assert _ids(page)[0] == "page:arr"
    assert _kind(page, "page:arr") == "exact_name"
    assert "exact_name_absent" not in page


def test_under_enforce_search_runs_exactly_the_statements_it_ran_before(
    clean_runtime: str, deployment: str, monkeypatch, search_module,
) -> None:
    # Round 3 review M2: under access rules the name check must not run at
    # all, so neither the page nor its timing says anything new about hidden
    # records. The statements of a search equal those of the same search
    # with the check replaced by its "found nothing" result (dev's page).
    import okg.substrate.mcp.grep_search as grep_search

    _enforce(clean_runtime, deployment, monkeypatch)
    seen: list[str] = []
    real_with = grep_search._bounded_with

    def spy_with(conn, sql, params, **kwargs):
        seen.append(sql)
        return real_with(conn, sql, params, **kwargs)

    def no_catalog_reads(*_args, **_kwargs):
        raise AssertionError("the name check read the catalog under access rules")

    monkeypatch.setattr(grep_search, "_bounded_with", spy_with)
    monkeypatch.setattr(search_module, "_attrs_index_on", no_catalog_reads)
    pages, statements = [], []
    for patched in (False, True):
        if patched:
            monkeypatch.setattr(
                search_module, "_named_record_check",
                lambda *_a, **_k: search_module._NameCheck([]),
            )
        seen.clear()
        with psycopg.connect(clean_runtime, autocommit=True) as conn:
            session = _enforced_session(conn, deployment)
            pages.append(search(conn, session, "MobPlan2024", method="lexical"))
        statements.append(list(seen))
    assert statements[0] == statements[1]
    # (The exact-name step reads the attrs index too, so only the check's own
    # id read is told apart here; the equal statements above rule out its
    # name read.)
    assert not any("node_id ~*" in sql for sql in statements[0])
    assert {key: pages[0].get(key) for key in _FACTS} == {key: pages[1].get(key) for key in _FACTS}
    assert _ids(pages[0]) == _ids(pages[1])
    # As before this change, the readable title-named page is not marked.
    assert all(row.get("match_kind") != "exact_name" for row in pages[0]["results"])


@pytest.mark.parametrize("index", ["node_id_trgm", "scan"])
@pytest.mark.parametrize("query", [
    "nightly_build_15_0_1", "Deploy2021_final", "BetaSection2022", "TeamCharter2023",
    "TeamCharter2024", "GuideBook2021",
])
def test_a_name_a_whole_word_of_an_id_is_never_claimed_absent(
    clean_runtime: str, deployment: str, query: str, index: str,
) -> None:
    # Ids such as run:nightly_build_15.0.1, run:guide.md#BetaSection2022,
    # run:wiki/TeamCharter2023/, ...view?page=TeamCharter2024 and
    # section:GuideBook2021:intro, read through the node-id trigram index
    # and, without it, by the scan of a small graph.
    if index == "scan":
        _drop_index(clean_runtime, "node_id_trgm")
    assert "exact_name_absent" not in _run(clean_runtime, deployment, query)
    # The claim still stands for a name no id holds.
    assert "exact_name_absent" in _run(clean_runtime, deployment, "WorkshopAtLisbon2024")


@pytest.mark.parametrize("index", ["node_id_trgm", "scan"])
@pytest.mark.parametrize("query", [
    "teamcharter2036", "TEAMCHARTER2037", "Teamcharter2038", "releasenotesjune2041",
    "teamcharter2050", "teamplan2060", "Teamplan2061", "OPSPLAN2062", "teamplan2064",
])
def test_a_single_case_word_that_could_hide_separators_is_never_claimed_absent(
    clean_runtime: str, deployment: str, query: str, index: str,
) -> None:
    # Round 4 review M1: the ids run:Team-Charter-2036 (2037, 2038) and
    # page:release-notes-june-2041, and the stored text "Team-Charter-2050",
    # spell these names with separators inside what the query types as one
    # word of one case; the name's own runs cannot match them, so a run of
    # nine or more letters of one case claims nothing. Round 5 review: the
    # same for shorter words when the name has fewer than two letter runs
    # (teamplan2060 against run:Team-Plan-2060, OPSPLAN2062 against
    # run:ops-plan-2062, teamplan2064 against the stored text Team-Plan-2064).
    if index == "scan":
        _drop_index(clean_runtime, "node_id_trgm")
    assert "exact_name_absent" not in _run(clean_runtime, deployment, query)
    # A name whose words are marked by case still gets the claim.
    assert "exact_name_absent" in _run(clean_runtime, deployment, "WorkshopAtLisbon2024")


def test_without_the_id_index_a_large_graph_claims_nothing_and_reads_no_ids(
    clean_runtime: str, deployment: str, monkeypatch, search_module,
) -> None:
    import okg.substrate.mcp.grep_search as grep_search

    _drop_index(clean_runtime, "node_id_trgm")
    seen: list[str] = []
    real = grep_search._bounded_with

    def spy(conn, sql, params, **kwargs):
        seen.append(sql)
        return real(conn, sql, params, **kwargs)

    monkeypatch.setattr(grep_search, "_bounded_with", spy)
    monkeypatch.setattr(
        search_module, "_visible_row_estimate",
        lambda *_a: search_module._NODE_ID_SCAN_MAX_ROWS + 1,
    )
    page = _run(clean_runtime, deployment, "WorkshopAtLisbon2024")
    assert "exact_name_absent" not in page
    assert not any("node_id ~*" in sql for sql in seen)
    # At or under the bound the scan runs and the claim stands.
    monkeypatch.setattr(
        search_module, "_visible_row_estimate",
        lambda *_a: search_module._NODE_ID_SCAN_MAX_ROWS,
    )
    seen.clear()
    assert "exact_name_absent" in _run(clean_runtime, deployment, "WorkshopAtLisbon2024")
    assert any("node_id ~*" in sql for sql in seen)


def test_a_generation_without_an_overlay_marker_claims_nothing(
    clean_runtime: str, deployment: str,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        generation = str(open_session(conn, deployment_name=deployment).generation_id)
        conn.execute(
            "INSERT INTO okg.attribute_effective_attrs (target_kind, target_id, "
            "generation_id, attr_key, observation_id, value, value_hash, producer) "
            "VALUES ('node', 'page:SchoolOverview', 'gen:older', 'title', 'obs:older', "
            "%s::jsonb, 'h', 'test')",
            (json.dumps("SchoolRenamed2024"),),
        )
        conn.execute(
            "DELETE FROM okg.attribute_overlay_generations WHERE generation_id = %s",
            (generation,),
        )
    assert "exact_name_absent" not in _run(clean_runtime, deployment, "SchoolRenamed2025")


def test_under_audit_search_claims_nothing_and_inspect_says_you_can_read(
    clean_runtime: str, deployment: str, monkeypatch,
) -> None:
    from okg.substrate.mcp.tools import _visible_attr_target
    from tests.substrate.mcp.test_search_records import MAIN, _activate, _allow

    _activate(clean_runtime, deployment, [_allow(MAIN)])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "audit")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = _enforced_session(conn, deployment)
        page = search(conn, session, "WorkshopAtLisbon2024", method="lexical")
        with pytest.raises(NodeNotFound) as caught:
            _visible_attr_target(
                conn, session, target_kind="node", target_id="page:WorkshopAtLisbon2024",
            )
    assert "exact_name_absent" not in page
    assert caught.value.message.startswith("no record you can read has id ")
    assert "close_ids" not in caught.value.data


# ── the reply an agent reads, through a real MCP client ───────────────────


def _call_all(dsn: str, deployment: str, calls: list[tuple[str, dict]]) -> list[str]:
    from mcp.shared.memory import create_connected_server_and_client_session

    from okg.substrate.mcp import server as srv

    mcp = srv.build_server(dsn=dsn, deployment_name=deployment)
    out: list[str] = []
    try:
        async def drive() -> None:
            async with create_connected_server_and_client_session(mcp) as client:
                for tool, args in calls:
                    result = await client.call_tool(
                        tool, args, meta={"okg/result_format": "text"})
                    out.append(result.content[0].text)
        anyio.run(drive)
    finally:
        mcp._okg_pool.close()
        if mcp._okg_write_pool:
            mcp._okg_write_pool.close()
    return out


def test_the_text_replies_say_absent_plainly(
    clean_runtime: str, deployment: str, okg_mcp_can_log_in,
) -> None:
    searched, mentioned, inspected, text_read, lonely = _call_all(
        clean_runtime, deployment, [
            ("search", {"query": "WorkshopAtLisbon2024"}),
            ("search", {"query": "ReleaseNotesAgentJune2013"}),
            ("inspect", {"target": {"kind": "node", "node_id": "page:WorkshopAtLisbon2024"}}),
            ("inspect", {"target": {"kind": "text",
                                    "node_id": "page:ReleaseNotesAgentJune2013"}}),
            ("inspect", {"target": {"kind": "node", "node_id": "page:Zebra"}}),
        ],
    )
    assert (
        '! no record has the name or id "WorkshopAtLisbon2024": names, titles, labels, '
        "keys and ids checked exactly; ids and text also in any case or with up "
        "to three separators between its words; other attributes not checked; ids "
        "with a near name: page:WorkshopAtLisbon2023"
    ) in searched.splitlines()
    assert "has the name or id" not in mentioned
    assert "no record has id 'page:WorkshopAtLisbon2024' at generation " in inspected
    assert "visible ids with a near id or path: ['page:WorkshopAtLisbon2023']" in inspected
    assert "no record has id 'page:ReleaseNotesAgentJune2013'" in text_read
    assert "'page:ReleaseNotesAgentMay2013'" in text_read
    assert "no record has id 'page:Zebra'" in lonely
    assert "near id" not in lonely


# ── inspect ───────────────────────────────────────────────────────────────


def test_inspect_lists_sibling_ids_for_a_name_id_and_says_it_does_not_exist(
    clean_runtime: str, deployment: str,
) -> None:
    from okg.substrate.mcp.tools import _visible_attr_target, close_node_ids

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        assert close_node_ids(conn, session, "page:ReleaseNotesAgentJune2013") == (
            ["page:ReleaseNotesAgentMay2013", "page:ReleaseNotesAgentApril2013"],
            "checked",
        )
        # Nothing under the head starts like it: no ids, and still "no_path"
        # (no other prefix was looked at).
        assert close_node_ids(conn, session, "page:QuietMeadow2024") == ([], "no_path")
        with pytest.raises(NodeNotFound) as caught:
            _visible_attr_target(
                conn, session, target_kind="node", target_id="page:WorkshopAtLisbon2024",
            )
    err = caught.value
    assert err.message.startswith("no record has id 'page:WorkshopAtLisbon2024'")
    assert err.data["exists"] is False
    assert err.data["close_ids"] == ["page:WorkshopAtLisbon2023"]


def test_under_enforce_inspect_says_you_can_read_and_reads_no_siblings(
    clean_runtime: str, deployment: str, monkeypatch,
) -> None:
    from okg.substrate.mcp.tools import _visible_attr_target

    _enforce(clean_runtime, deployment, monkeypatch)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = _enforced_session(conn, deployment)
        refusals = []
        for node_id in ("page:WorkshopAtLisbon2024", "page:HiddenPlan2024", "page:HiddenPlan2025"):
            with pytest.raises(NodeNotFound) as caught:
                _visible_attr_target(conn, session, target_kind="node", target_id=node_id)
            refusals.append(caught.value)
    for err in refusals:
        assert err.message.startswith("no record you can read has id ")
        assert "close_ids" not in err.data
        assert err.data["close_id_check"] == "no_path"
    # A hidden page and an absent one read alike.
    hidden, missing = refusals[1], refusals[2]
    assert {k: v for k, v in hidden.data.items() if k != "node_id"} == (
        {k: v for k, v in missing.data.items() if k != "node_id"}
    )
    assert hidden.message.replace("HiddenPlan2024", "X") == (
        missing.message.replace("HiddenPlan2025", "X")
    )
    assert hidden.fix_hint == missing.fix_hint
