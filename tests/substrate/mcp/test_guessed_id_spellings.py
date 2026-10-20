"""#3666: the pure parts of the near-id hint for a hand-built id that names
no record. A guessed id is read the way its writer most likely meant it
(`%XX` decoded, no space around a `:`, a `/` read as the `:` after the
source), stored paths are compared ignoring case, and the candidates come
closest path first. No database: the reads themselves are proved in
`test_node_lookup.py` (postgres)."""
from __future__ import annotations

import json

import pytest

pytest.importorskip("mcp")

import okg.substrate.mcp.tools as tools_mod  # noqa: E402


@pytest.mark.parametrize(
    ("guess", "forms"),
    [
        # A space escaped as %20.
        ("doc:docs:guide/Getting%20started.md", ["doc:docs:guide/Getting started.md"]),
        # A space after the prefix colon.
        ("repository: team/project", ["repository:team/project", "repository:team:project"]),
        # A `/` where the `:` after the source goes.
        ("doc:docs/guide/Install.md", ["doc:docs:guide/Install.md"]),
        ("doc:app/src/auth/policy.py", ["doc:app:src/auth/policy.py"]),
        # Already well formed: no other spelling to try.
        ("doc:app:src/core/Loader.py", []),
        ("ticket:ABC-1", []),
    ],
)
def test_a_guessed_id_is_read_as_its_writer_meant_it(guess: str, forms: list[str]) -> None:
    assert tools_mod.guessed_id_forms(guess) == forms


def test_the_path_inside_a_guessed_id_is_decoded_and_trimmed() -> None:
    assert tools_mod._id_path("doc:docs:guide/Getting%20started.md") == (
        "guide/Getting started.md"
    )
    assert tools_mod._id_path("doc: app : src/core/Loader.py") == "src/core/Loader.py"
    assert tools_mod._id_path("ticket:ABC-1") is None


def test_a_stored_path_matches_a_guessed_tail_ignoring_case() -> None:
    row = {"attrs": {"path": "src/core/Engine/loader.py"}}
    assert tools_mod._path_ends_with(row, "Engine/Loader.py")
    assert tools_mod._path_ends_with(row, "engine/loader.py")
    # A tail must end on a part boundary, and a different file is no match.
    assert not tools_mod._path_ends_with(row, "ngine/loader.py")
    assert not tools_mod._path_ends_with(row, "Engine/saver.py")
    assert not tools_mod._path_ends_with({"attrs": {}}, "loader.py")


def test_near_paths_come_closest_first() -> None:
    found = {
        "doc:app:other/loader.py": "other/loader.py",
        "doc:app:src/Stage/loader.py": "src/Stage/loader.py",
        "doc:app:src/stage/Loader.py": "src/stage/Loader.py",
    }
    # Two shared parts in the same case beat two in another case, which
    # beat the file name alone.
    assert tools_mod.rank_near_paths("Stage/loader.py", found) == [
        "doc:app:src/Stage/loader.py",
        "doc:app:src/stage/Loader.py",
        "doc:app:other/loader.py",
    ]
    assert tools_mod.rank_near_paths("Stage/loader.py", {}) == []


def test_a_path_splits_into_its_tail_and_file_name() -> None:
    assert tools_mod._tail_and_name("a/b/c.py") == ("b/c.py", "c.py")
    assert tools_mod._tail_and_name("c.py") == ("c.py", "c.py")


# The read flow, with an in-memory stand-in for the bounded reads: which
# reads run, in what order, and what the reply names. The SQL itself and
# the reader's real filter are proved against PostgreSQL in
# `test_node_lookup.py`.

_STORED = [
    {"node_id": "doc:app:src/core/Engine/loader.py", "attrs": {"path": "src/core/Engine/loader.py"}},
    {"node_id": "doc:app:tools/merge.py", "attrs": {"path": "tools/merge.py"}},
    {"node_id": "doc:app:src/stage/runner/step.py", "attrs": {"path": "src/stage/runner/step.py"}},
    {"node_id": "doc:app:other/step.py", "attrs": {"path": "other/step.py"}},
    {"node_id": "doc:docs:guide/Getting started.md", "attrs": {"path": "guide/Getting started.md"}},
    {"node_id": "repository:team/project", "attrs": {}},
    # Round 2, laid out as a crawled site stores them: a page id of its own
    # (`<kind>:<hash>`) with the page URL in `url` and an empty `path`; a
    # record with a citation link; a wiki page; a chunk known by its hash.
    {"node_id": "site_page:3c1d0e5f7a9b2c4d",
     "attrs": {"url": "https://docs.example.org/handbook/guide/setup-a-project",
               "path": "", "title": "Set up a project | Handbook",
               "site_name": "docs.example.org"}},
    {"node_id": "site_page:8e2f4a6b0c1d3e5f",
     "attrs": {"url": "https://docs.example.org/handbook/guide/install.html",
               "path": "", "title": "Install | Handbook",
               "site_name": "docs.example.org"}},
    {"node_id": "note:handbook:cited",
     "attrs": {"citation_url": "https://docs.example.org/handbook/cited.md"}},
    {"node_id": "wiki:Main:StartHere", "attrs": {"title": "Start here"}},
    {"node_id": "chunk:9f3c2a7b1d0e44", "attrs": {}},
]


def _like(value: str, pattern: str) -> bool:
    """SQL `value LIKE pattern` with the default backslash escape."""
    import re

    out, index = "", 0
    while index < len(pattern):
        char = pattern[index]
        if char == "\\" and index + 1 < len(pattern):
            out += re.escape(pattern[index + 1])
            index += 2
            continue
        out += ".*" if char == "%" else "." if char == "_" else re.escape(char)
        index += 1
    return re.fullmatch(out, value, re.DOTALL) is not None


def _unlike(pattern: str) -> str:
    """A `%<literal>` LIKE pattern's literal."""
    assert pattern.startswith("%")
    return pattern[1:].replace("\\_", "_").replace("\\%", "%").replace("\\\\", "\\")


def _fake_reads(
    monkeypatch: pytest.MonkeyPatch,
    visible: set[str] | None = None,
    stored: list[dict] | None = None,
) -> list[str]:
    """Stand in for `_read_budget`: each read answers from `stored`
    (default `_STORED`) as the SQL would, in id order and cut at the
    read's limit, then keeps only `visible` ids (the reader's filter).
    Returns the list the reads' WHERE clauses are logged to."""
    from contextlib import contextmanager

    log: list[str] = []
    table = _STORED if stored is None else stored

    def read(
        *, where: str, params: dict, limit: int, stored_ids: list[str] | None = None,
        candidates: str | None = None,
    ) -> list[dict]:
        if candidates is not None:
            # The candidate form of `_visible_node_rows`: a condition over
            # the alias `hit`, never a raw relation of its own.
            assert where == "" and "okg." not in candidates
            where = candidates
        log.append(where)
        if "ANY(:ids)" in where:
            rows = [row for row in table if row["node_id"] in params["ids"]]
        elif "url_doc_0" in params:  # OR-ed attrs @> <one-key document>
            assert "hit.attrs @> CAST(:url_doc_0 AS JSONB)" in where
            assert "ANY(CAST" not in where and "LIKE" not in where
            docs = [json.loads(doc) for key, doc in params.items() if key.startswith("url_doc_")]
            rows = [
                row for row in table
                if any(
                    all(row["attrs"].get(key) == value for key, value in doc.items())
                    for doc in docs
                )
            ]
        elif "name_tail" in params:  # lower(right(node_id, n)) = lower(:tail)
            assert "LIKE" not in where and "lower(node_id)" not in where
            rows = [
                row for row in table
                if row["node_id"][-params["name_len"]:].lower() == params["name_tail"].lower()
            ]
        elif "commit_sha" in params:  # the commit: id range, then lower(node_id) LIKE
            rows = [
                row for row in table
                if params["commit_low"] <= row["node_id"] < params["commit_high"]
                and _like(row["node_id"].lower(), params["commit_sha"])
            ]
        elif "hash_head" in params:  # an id range, then node_id LIKE
            rows = [
                row for row in table
                if row["node_id"] >= params["hash_low"]
                and ("hash_high" not in params or row["node_id"] < params["hash_high"])
                and _like(row["node_id"], params["hash_head"])
            ]
        else:
            prefix = params["id_prefix"][:-2]

            def path_matches(path: str) -> bool:
                if "path_ends_in" in params:  # the part-boundary form
                    return path == params["path_is"] or path.endswith(
                        _unlike(params["path_ends_in"]),
                    )
                return path.endswith(_unlike(params["path_tail"]))

            rows = [
                row for row in table
                if row["node_id"].startswith(prefix + ":")
                and path_matches(str(row["attrs"].get("path", "")).lower())
            ]
        rows = sorted(rows, key=lambda row: row["node_id"])[:limit]
        if stored_ids is not None:
            stored_ids.extend(row["node_id"] for row in rows)
        return [row for row in rows if visible is None or row["node_id"] in visible]

    @contextmanager
    def budget(conn, session, budget_ms):  # noqa: ANN001 - stand-in
        # The first reads get the whole budget; round two what is left.
        assert 0 < budget_ms <= tools_mod._CLOSE_IDS_TIMEOUT_MS
        yield read

    monkeypatch.setattr(tools_mod, "_read_budget", budget)
    return log


@pytest.mark.parametrize(
    ("guess", "named"),
    [
        ("doc:app:src/core/Engine/Loader.py", ["doc:app:src/core/Engine/loader.py"]),
        ("doc:docs:guide/Getting%20started.md", ["doc:docs:guide/Getting started.md"]),
        ("repository: team/project", ["repository:team/project"]),
        ("doc:app:lib/sync/merge.py", ["doc:app:tools/merge.py"]),
        ("doc:app:src/stage/step.py", ["doc:app:src/stage/runner/step.py", "doc:app:other/step.py"]),
        ("file:app:lib/sync/merge.py", []),
    ],
)
def test_close_ids_name_the_visible_record_a_misspelled_id_meant(
    monkeypatch: pytest.MonkeyPatch, guess: str, named: list[str],
) -> None:
    _fake_reads(monkeypatch)
    assert tools_mod.close_node_ids(None, None, guess) == (named, "checked")


def test_the_file_name_is_read_only_when_the_tail_finds_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    log = _fake_reads(monkeypatch)
    tools_mod.close_node_ids(None, None, "doc:app:core/Engine/Loader.py")
    assert len(log) == 1
    tools_mod.close_node_ids(None, None, "doc:app:lib/sync/merge.py")
    assert len(log) == 3


def test_a_hidden_record_is_never_named(monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_reads(monkeypatch, visible=set())
    for guess in ("repository: team/project", "doc:app:lib/sync/merge.py"):
        assert tools_mod.close_node_ids(None, None, guess) == ([], "checked")


def test_a_failed_read_says_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    from contextlib import contextmanager

    @contextmanager
    def spent(conn, session, budget_ms):  # noqa: ANN001 - stand-in
        def read(**kwargs):  # noqa: ANN003 - stand-in
            raise TimeoutError("the close-id read budget is spent")
        yield read

    monkeypatch.setattr(tools_mod, "_read_budget", spent)
    assert tools_mod.close_node_ids(None, None, "doc:app:lib/sync/merge.py") == (
        [], "unavailable",
    )
    assert tools_mod.close_node_ids(None, None, "ticket:ABC-1") == ([], "no_path")


# Review of fix E: the bounded read is cut BEFORE the reader's filter and
# the path check, so a common file name must be matched on a part boundary
# in SQL, and a cut read must say so.

#: More `xloader.py` files than one read covers, all in id order before
#: the one `loader.py`.
_CROWDED = [
    *(
        {"node_id": f"doc:app:a{index:02d}/xloader.py",
         "attrs": {"path": f"a{index:02d}/xloader.py"}}
        for index in range(30)
    ),
    {"node_id": "doc:app:z/loader.py", "attrs": {"path": "z/loader.py"}},
]


def test_a_name_that_only_ends_the_same_never_crowds_out_the_file(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fake_reads(monkeypatch, stored=_CROWDED)
    assert tools_mod.close_node_ids(None, None, "doc:app:lib/loader.py") == (
        ["doc:app:z/loader.py"], "checked",
    )


def test_a_read_cut_at_its_bound_says_incomplete_not_checked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    hidden = [
        {"node_id": f"doc:app:a{index:02d}/loader.py",
         "attrs": {"path": f"a{index:02d}/loader.py"}}
        for index in range(30)
    ]
    # Every stored match hidden from this reader, and more than one read.
    _fake_reads(monkeypatch, visible=set(), stored=hidden)
    assert tools_mod.close_node_ids(None, None, "doc:app:lib/loader.py") == (
        [], "incomplete",
    )
    # Visible ones: named, still a lower bound.
    _fake_reads(monkeypatch, stored=hidden)
    ids, check = tools_mod.close_node_ids(None, None, "doc:app:lib/loader.py")
    assert check == "incomplete"
    assert len(ids) == tools_mod.CLOSE_IDS_SHOWN
    # The row past the bound only proves there is more; it is never named.
    assert "doc:app:a25/loader.py" not in ids


def test_a_one_part_path_is_read_once(monkeypatch: pytest.MonkeyPatch) -> None:
    log = _fake_reads(monkeypatch)
    tools_mod.close_node_ids(None, None, "doc:app:none.py")
    # The path once (its tail is its name), then the name under any prefix.
    assert [where for where in log if "attrs->>'path'" in where] == log[:1]
    assert len(log) == 2 and "name_tail" in log[1]


def test_an_incomplete_check_is_said_and_never_called_none() -> None:
    err = tools_mod.node_not_visible("doc:app:lib/loader.py", "g1", close=([], "incomplete"))
    assert err.data["close_id_check"] == "incomplete"
    assert "not all checked" in err.message
    assert not err.fix_hint.startswith("no visible doc: id has that path")
    named = tools_mod.node_not_visible(
        "doc:app:lib/loader.py", "g1", close=(["doc:app:z/loader.py"], "incomplete"),
    )
    assert named.data["close_ids"] == ["doc:app:z/loader.py"]
    assert "not all checked" in named.message
    assert named.fix_hint.startswith("retry with one of ['doc:app:z/loader.py']")


# Round 2 (#3666, Format v7 replies with no near id): a page named by its
# URL, a wiki page name under the wrong prefix, an empty or placeholder
# last part (also after a source with a "."), and a hash start. The rows
# mirror a crawled site: a page id of its own, the URL in `attrs.url`.

SETUP_PAGE = "site_page:3c1d0e5f7a9b2c4d"
INSTALL_PAGE = "site_page:8e2f4a6b0c1d3e5f"
SITE_URL = "https://docs.example.org/handbook/guide"


@pytest.mark.parametrize(
    ("guess", "named"),
    [
        # A page URL under a guessed prefix and source, with or without a
        # suffix, a trailing `/`, http for https, or another case.
        (f"doc:handbook.site:{SITE_URL}/setup-a-project.md", [SETUP_PAGE]),
        (f"doc:handbook.site:{SITE_URL}/setup-a-project", [SETUP_PAGE]),
        (f"doc:handbook.site:{SITE_URL}/setup-a-project/", [SETUP_PAGE]),
        ("doc:handbook.site:http://docs.example.org/handbook/guide/setup-a-project.md",
         [SETUP_PAGE]),
        ("doc:handbook.site:https://docs.example.org/handbook/guide/Setup-A-Project.md",
         [SETUP_PAGE]),
        (f"doc:handbook.site:{SITE_URL}/install.md", [INSTALL_PAGE]),
        (f"doc:handbook.site:{SITE_URL}/install", [INSTALL_PAGE]),
        # A citation link names its record the same way.
        ("doc:handbook:https://docs.example.org/handbook/cited", ["note:handbook:cited"]),
        # A wiki page name under the wrong prefix, in any case.
        ("doc:notes:StartHere", ["wiki:Main:StartHere"]),
        ("doc:notes:startHERE", ["wiki:Main:StartHere"]),
        # A hash start long enough to look up.
        ("chunk:9f3c2a", ["chunk:9f3c2a7b1d0e44"]),
        ("chunk:9F3C2A7B", ["chunk:9f3c2a7b1d0e44"]),
        # A page, file or name nothing has: none, said as checked.
        (f"doc:handbook.site:{SITE_URL}/missing.md", []),
        ("doc:handbook.site:guide/missing.md", []),
        ("doc:notes:NoSuchPage", []),
        ("chunk:0000aaaa", []),
    ],
)
def test_round_two_shapes_name_the_record_or_say_none(
    monkeypatch: pytest.MonkeyPatch, guess: str, named: list[str],
) -> None:
    _fake_reads(monkeypatch)
    assert tools_mod.close_node_ids(None, None, guess) == (named, "checked")


def test_a_url_is_matched_exactly_by_the_attrs_index_never_by_a_scan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    log = _fake_reads(monkeypatch)
    tools_mod.close_node_ids(None, None, f"doc:handbook.site:{SITE_URL}/setup-a-project.md")
    url_reads = [where for where in log if "attrs @>" in where]
    docs = tools_mod._url_containment_docs(f"{SITE_URL}/setup-a-project.md")
    assert len(docs) <= 32
    assert all(len(json.loads(doc)) == 1 for doc in docs)
    # Review P2-4: one GIN index condition per spelling, OR-ed in an
    # unordered subquery; `@> ANY(<array>)` is no GIN index condition, and
    # under the outer ORDER BY node_id LIMIT the planner walked the id index.
    (url_read,) = url_reads
    candidates, params = tools_mod._url_read(f"{SITE_URL}/setup-a-project.md")
    assert url_read == candidates
    assert sorted(params.values()) == sorted(docs)
    assert candidates.count(" OR ") == len(docs) - 1
    assert "ANY(CAST" not in candidates and "ORDER BY" not in candidates
    # The candidates are read through `_visible_node_rows` (the reader's
    # filter, the pinned relation), never a raw relation here.
    assert "okg.graph_nodes" not in candidates


def test_the_candidate_form_reads_the_pinned_relation_unordered_and_bounded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review P2-4 (audit follow-up): `_visible_node_rows(candidates=...)`
    builds the unordered, bounded subquery over the same pinned relation it
    reads, orders the few ids outside it, and filters the rows for the
    reader. Failure case: `where` and `candidates` together are refused."""
    sent: list[str] = []
    filtered: list[list[dict]] = []

    class _Graph:
        def query(self, sql, params):  # noqa: ANN001 - stand-in
            sent.append(sql)
            return [{"node_id": "n:1"}]

    def keep(conn, session, rows):  # noqa: ANN001 - stand-in
        filtered.append(list(rows))
        return rows

    monkeypatch.setattr(tools_mod, "_chronos_checkout", lambda conn, session: _Graph())
    monkeypatch.setattr(tools_mod, "filter_node_rows_for_session", keep)
    candidates, params = tools_mod._name_read("StartHere")
    rows = tools_mod._visible_node_rows(
        None, None, params=params, limit=26, candidates=candidates,
    )
    assert rows == [{"node_id": "n:1"}] and filtered == [[{"node_id": "n:1"}]]
    (sql,) = sent
    inner = sql.split("ARRAY(", 1)[1].rsplit("))", 1)[0]
    assert "FROM okg.graph_nodes hit" in inner and candidates in inner
    assert f"LIMIT {tools_mod._CLOSE_IDS_PROBE}" in inner and "ORDER BY" not in inner
    assert "ORDER BY node_id" in sql and "LIMIT 26" in sql
    with pytest.raises(ValueError):
        tools_mod._visible_node_rows(
            None, None, where="WHERE TRUE", params=params, candidates=candidates,
        )


def test_the_name_read_runs_in_what_is_left_of_the_shared_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review P2-4, lab follow-up: a fixed 300 ms share cut off the two wiki
    names the lab copy resolves in about 500 ms (named 17 -> 15 of 27). The
    name read now gets what is left of the shared budget, and reads only
    the id's last characters, never `lower(node_id) LIKE '%:<name>'`."""
    budgets: list[int] = []
    log = _fake_reads(monkeypatch)
    fake = tools_mod._read_budget

    from contextlib import contextmanager

    @contextmanager
    def recording(conn, session, budget_ms):  # noqa: ANN001 - stand-in
        budgets.append(budget_ms)
        with fake(conn, session, budget_ms) as read:
            yield read

    monkeypatch.setattr(tools_mod, "_read_budget", recording)
    assert tools_mod.close_node_ids(None, None, "doc:notes:StartHere") == (
        ["wiki:Main:StartHere"], "checked",
    )
    assert len(log) == 1 and "name_tail" in log[0]
    # A name-only id runs no first read: its one read gets nearly the whole
    # budget. Failure case: a fixed share (300 ms) would be far below it.
    (budget,) = budgets
    assert budget > tools_mod._CLOSE_IDS_TIMEOUT_MS * 0.9, budgets
    assert not hasattr(tools_mod, "_CLOSE_IDS_NAME_MS")
    candidates, params = tools_mod._name_read("StartHere")
    assert params == {"name_len": len(":StartHere"), "name_tail": ":StartHere"}
    assert candidates == "lower(right(hit.node_id, :name_len)) = lower(:name_tail)"


def test_a_site_path_or_missing_file_reads_no_more_than_before(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A path id that names nothing gets the first reads only: no scan of
    every record's URL, which ran out of time on a large graph and turned
    "checked" into "unavailable" (Format v10 replay)."""
    log = _fake_reads(monkeypatch)
    assert tools_mod.close_node_ids(None, None, "doc:app:deploy/dev-jobs.yaml") == (
        [], "checked",
    )
    assert log and all("attrs->>'path'" in where for where in log)


@pytest.mark.parametrize(
    ("guess", "needs"),
    [
        ("doc:handbook:", "the id needs the part after 'doc:handbook:'"),
        ("doc:handbook", "the id needs the part after 'doc:handbook:'"),
        ("doc:handbook:?", "the id needs the part after 'doc:handbook:'"),
        ("doc:handbook:<path>", "the id needs the part after 'doc:handbook:'"),
        ("doc:", "the id needs the part after 'doc:'"),
        # A source with a "." is a source, not a file name.
        ("doc:handbook.site:", "the id needs the part after 'doc:handbook.site:'"),
        ("doc:handbook.site:?", "the id needs the part after 'doc:handbook.site:'"),
        ("chunk:10fe", "'10fe' is too short a hash to look up"),
    ],
)
def test_an_id_missing_its_part_gets_a_specific_hint_and_no_read(
    monkeypatch: pytest.MonkeyPatch, guess: str, needs: str,
) -> None:
    log = _fake_reads(monkeypatch)
    close = tools_mod.close_node_ids(None, None, guess)
    assert close == ([], "id_incomplete")
    assert log == []
    err = tools_mod.node_not_visible(guess, "g1", close=close)
    assert err.data["close_id_check"] == "id_incomplete"
    assert "close_ids" not in err.data
    assert err.fix_hint.startswith(needs), err.fix_hint
    assert "take ids from a search result" in err.fix_hint
    assert "has that path" not in err.fix_hint


def test_a_source_with_a_dot_and_nothing_after_it_gets_the_hint_after_its_reads(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`doc:<site>.<kind>` may also be a file under a missing source, so its
    path reads run; when they name nothing, the reply says the id lacks its
    part, never "no visible doc: id has that path"."""
    _fake_reads(monkeypatch)
    close = tools_mod.close_node_ids(None, None, "doc:handbook.site")
    assert close == ([], "id_incomplete")
    err = tools_mod.node_not_visible("doc:handbook.site", "g1", close=close)
    assert err.fix_hint.startswith("the id needs the part after 'doc:handbook.site:'")


def test_the_source_part_is_never_read_as_the_path() -> None:
    assert tools_mod._id_path("doc:handbook.site:") is None
    assert tools_mod._id_path("doc:handbook.site:?") is None
    assert tools_mod._id_path("doc:handbook.site:README.md") == "README.md"
    assert tools_mod._id_path("file:app:CMakeLists.txt") == "CMakeLists.txt"
    # One part after the prefix: it may be a top-level file.
    assert tools_mod._id_path("doc:README.md") == "README.md"


@pytest.mark.parametrize(
    "guess",
    [
        "ticket:ABC-1",  # a `<kind>:<key>` id keeps its own meaning
        "issue:1234",  # a number is not a hash
        "symbol:app:src/core/Engine/loader.py:load:??",  # has a path to look up
        f"doc:handbook.site:{SITE_URL}/setup-a-project.md",  # a URL to look up
    ],
)
def test_ids_that_are_not_missing_a_part_get_no_such_hint(guess: str) -> None:
    assert tools_mod.id_part_missing_hint(guess) is None


#: A commit id as the git source stores it: `commit:<repo>:<sha>`.
COMMIT_ID = "commit:app:3f9a2b1c7d5e4f60718293a4b5c6d7e8f9012345"


def test_a_commit_sha_without_its_repo_is_looked_up_and_names_the_repo_slot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Review F2: commit ids are `commit:<repo>:<sha>`. `commit:<sha>` lacks
    the repo BEFORE the sha, never a part after it, and its sha start is
    still looked up under every repo."""
    hint = tools_mod.id_part_missing_hint("commit:3f9a2b1c7d")
    assert hint is not None and hint.startswith(
        "the id needs the repo before the sha (commit: ids read 'commit:<repo>:<sha>')"
    ), hint
    assert "part after" not in hint
    symbol = tools_mod.id_part_missing_hint("symbol:loader")
    assert symbol is not None and "'symbol:<repo>:<path>:<qualified name>'" in symbol
    stored = [*_STORED, {"node_id": COMMIT_ID, "attrs": {}},
              {"node_id": "commit:app:3f9a2b1c7d:extra", "attrs": {}}]
    log = _fake_reads(monkeypatch, stored=stored)
    assert tools_mod.close_node_ids(None, None, "commit:3f9a2b1c7d") == (
        [COMMIT_ID], "checked",
    )
    assert any("lower(node_id) LIKE :commit_sha" in where for where in log), log
    # Failure cases: a sha nothing has, and one too short to look up, say
    # the id lacks its repo and name nothing.
    _fake_reads(monkeypatch, stored=stored)
    close = tools_mod.close_node_ids(None, None, "commit:abcdef0123")
    assert close == ([], "id_incomplete")
    err = tools_mod.node_not_visible("commit:abcdef0123", "g1", close=close)
    assert err.fix_hint.startswith("the id needs the repo before the sha"), err.fix_hint
    _fake_reads(monkeypatch, stored=stored)
    assert tools_mod.close_node_ids(None, None, "commit:3f9a2") == ([], "id_incomplete")


def test_a_hidden_or_masked_page_is_never_named(monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_reads(monkeypatch, visible=set())
    for guess in (
        f"doc:handbook.site:{SITE_URL}/setup-a-project.md",
        "doc:notes:StartHere",
        "chunk:9f3c2a",
    ):
        assert tools_mod.close_node_ids(None, None, guess) == ([], "checked")
    # A URL the reader sees masked is never confirmed, whatever SQL matched.
    masked = {"node_id": SETUP_PAGE, "attrs": {"url": "***"}}
    assert tools_mod._stored_url_matching(masked, f"{SITE_URL}/setup-a-project") is None


def test_a_round_two_failure_keeps_the_first_reads_and_says_incomplete(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The first reads finished (checked, nothing named); the URL read then
    ran out of time: "incomplete" (near ids not all checked), never
    "unavailable", which would throw away the finished check."""
    from contextlib import contextmanager

    calls: list[int] = []
    real_log = _fake_reads(monkeypatch)
    del real_log
    first = tools_mod._read_budget

    @contextmanager
    def budget(conn, session, budget_ms):  # noqa: ANN001 - stand-in
        calls.append(budget_ms)
        if len(calls) == 1:
            with first(conn, session, budget_ms) as read:
                yield read
            return

        def spent(**kwargs):  # noqa: ANN003 - stand-in
            raise TimeoutError("canceling statement due to statement timeout")
        yield spent

    monkeypatch.setattr(tools_mod, "_read_budget", budget)
    guess = f"doc:handbook.site:{SITE_URL}/setup-a-project.md"
    assert tools_mod.close_node_ids(None, None, guess) == ([], "incomplete")
    assert len(calls) == 2
    err = tools_mod.node_not_visible(guess, "g1", close=([], "incomplete"))
    assert "not all checked" in err.message
    # With no first reads (a name only), a cut second-stage read is still
    # "incomplete" (near-ids-2): the check began and was cut. Before the
    # lab follow-up it said "unavailable".
    calls[:] = [0]  # the next budget is not the first one: its read fails
    assert tools_mod.close_node_ids(None, None, "doc:notes:StartHere") == (
        [], "incomplete",
    )
    err = tools_mod.node_not_visible("doc:notes:StartHere", "g1", close=([], "incomplete"))
    assert "not all checked" in err.message


def test_the_round_two_reads_run_only_when_the_earlier_ones_name_nothing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    log = _fake_reads(monkeypatch)
    # The path read names the record: no URL or name read follows.
    tools_mod.close_node_ids(None, None, "doc:app:src/core/Engine/Loader.py")
    assert len(log) == 1
    log.clear()
    tools_mod.close_node_ids(None, None, "doc:notes:StartHere")
    assert len(log) == 1 and "name_tail" in log[0]


def test_a_hash_start_is_read_as_an_id_range() -> None:
    assert tools_mod._hex_upper_bound("9f3c2a") == "9f3c2b"
    assert tools_mod._hex_upper_bound("9f3c2f") == "9f3c3"
    assert tools_mod._hex_upper_bound("ffff") is None


def test_a_crowded_name_read_says_incomplete(monkeypatch: pytest.MonkeyPatch) -> None:
    crowded = [
        {"node_id": f"wiki:web{index:02d}:StartHere", "attrs": {}} for index in range(30)
    ]
    _fake_reads(monkeypatch, stored=crowded)
    ids, check = tools_mod.close_node_ids(None, None, "doc:notes:StartHere")
    assert check == "incomplete"
    assert len(ids) == tools_mod.CLOSE_IDS_SHOWN
    assert "wiki:web25:StartHere" not in ids


def test_a_name_under_the_same_prefix_comes_first(monkeypatch: pytest.MonkeyPatch) -> None:
    stored = [
        {"node_id": "wiki:Main:StartHere", "attrs": {}},
        {"node_id": "doc:other:starthere", "attrs": {}},
        {"node_id": "doc:other:StartHere", "attrs": {}},
    ]
    _fake_reads(monkeypatch, stored=stored)
    assert tools_mod.close_node_ids(None, None, "doc:notes:StartHere") == (
        ["doc:other:StartHere", "doc:other:starthere", "wiki:Main:StartHere"], "checked",
    )


def test_url_spellings_cover_scheme_suffix_slash_and_case() -> None:
    spellings = tools_mod._url_spellings("https://Docs.example.org/A/Page.md")
    assert "https://Docs.example.org/A/Page" in spellings
    assert "http://docs.example.org/a/page/" in spellings
    assert "https://docs.example.org/a/page.html" in spellings
    assert len(spellings) == len(set(spellings)) <= 16
