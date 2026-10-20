"""An exact filename query lists the file itself first (PACT
filename-exact-hits).

The lexical exact pass (PACT lexical-exact-identifier-hits) greps each
identifier's text and lists 3 records per span. A file name is mentioned by
many commits, so the commits filled the slots and the file came last. A span
with no space that holds a `/` or ends in a file extension now first lists
the records whose own `attrs.path` equals it or ends with `/<span>`.

The span and rendering tests need no database. The PostgreSQL tests run the
real search on published fixture graphs: the file comes first when more
records mention its name than the slots hold; a `dir/name.ext` span
matches; `run.sh` does not match `rerun.sh`; a hidden file is neither listed
nor counted under Nomos enforcement; an identifier that is not a file name
runs exactly as before; the path pass runs under the exact pass's deadline
and says when it stopped. The MCP test drives the real tool.
"""
from __future__ import annotations

import importlib
import json

import psycopg
import pytest

from okg.substrate.errors import SubstrateError

from okg.substrate.mcp import open_session, search
from okg.substrate.mcp.result_text import render
from tests.substrate.mcp.test_lexical_exact_identifier_hits import KEY, _facts
from tests.substrate.mcp.test_search_list import Served
from tests.substrate.mcp.test_search_records import (
    MAIN,
    MASKED,
    OPEN,
    READER,
    SECRET,
    _activate,
    _allow,
    _edge,
    _node,
    _publish,
)

FILE = "rucio_table_dumps.sh"


# ---------- which spans get the path pass (database-free) ----------

@pytest.mark.parametrize(("span", "shaped"), [
    ("rucio_table_dumps.sh", True),
    ("occupancyController.py", True),
    ("cmsmon-rucio-daily.yaml", True),
    ("dir/name.ext", True),
    ("/HC-NoPU_castor/Run3Winter24Reco-v10/GEN-SIM-RECO", True),
    ("CMSDM-353", False),
    ("T1_US_FNAL_Disk", False),
    ("chunkSize", False),
    ("v1.2.3", False),
    ("https://example.org/a/detail.html", False),
    ("site status.md", False),
])
def test_a_file_name_or_path_gets_the_path_pass(span, shaped):
    # Imported here so the database tests below still run (and fail on
    # their own assertions) against code without the path pass.
    from okg.substrate.mcp.grep_query import filename_shaped

    assert filename_shaped(span) is shaped


# ---------- the text view (database-free) ----------

def _payload(entry: dict) -> dict:
    return {
        "query": "x", "method": "lexical",
        "results": [{"node_id": "commit:a", "subtype": "commit"}],
        "exact_matches": {"method": "grep", "spans": [entry]},
    }


def test_the_path_line_comes_before_the_text_line():
    text = render(tool="search", payload=_payload({
        "span": FILE, "status": "matched", "count": 11, "count_basis": "exact",
        "count_unit": "record", "path_status": "matched", "path_count": 1,
        "path_count_basis": "exact", "path_count_unit": "record",
        "records": [
            {"node_id": "file:r:scripts/rucio_table_dumps.sh", "subtype": "source_file",
             "matched_on": "path"},
            {"node_id": "commit:a", "subtype": "commit", "result_position": 1},
            {"node_id": "commit:b", "subtype": "commit", "window": "edit rucio_table_dumps.sh"},
        ],
    }))
    lines = text.splitlines()
    path = lines.index(f'= exact "{FILE}" (path, 1 of 1 records):')
    assert lines[path + 1] == "  file:r:scripts/rucio_table_dumps.sh (source_file)"
    assert lines[path + 2] == f'= exact "{FILE}" (grep, 2 of 11 records):'
    assert lines[path + 3] == "  commit:a (commit) = result [1]"


def test_a_path_pass_that_found_nothing_adds_no_line():
    text = render(tool="search", payload=_payload({
        "span": FILE, "status": "no_exact_match", "count": 0, "count_basis": "exact",
        "records": [], "path_status": "no_exact_match", "path_count": 0,
        "path_count_basis": "exact",
    }))
    assert "(path" not in text
    assert f'= exact "{FILE}" (grep): no record holds this text' in text


@pytest.mark.parametrize(("path_status", "said"), [
    ("timeout", "checked, stopped early (timeout); no match read before it stopped"),
    ("skipped:time_budget", "not checked (skipped:time_budget)"),
])
def test_an_unfinished_path_pass_says_so(path_status, said):
    text = render(tool="search", payload=_payload({
        "span": FILE, "status": "matched", "count": 1, "count_basis": "exact",
        "count_unit": "record", "path_status": path_status,
        "records": [{"node_id": "commit:b", "subtype": "commit"}],
    }))
    assert f'= exact "{FILE}" (path): {said}' in text
    assert f'= exact "{FILE}" (grep, 1 of 1 records):' in text


def test_one_record_per_path_prefers_exact_case_then_the_path_owning_type():
    search_module = importlib.import_module("okg.substrate.mcp.search")
    rows = [
        {"node_id": "a:sym", "subtype": "code_symbol"},
        {"node_id": "b:upper", "subtype": "source_file"},
        {"node_id": "c:file", "subtype": "source_file"},
        {"node_id": "d:other", "subtype": "code_symbol"},
        {"node_id": "e:via-passage", "subtype": "git_commit"},
    ]
    paths = {
        "a:sym": "src/x/run.sh", "b:upper": "SRC/X/RUN.SH", "c:file": "src/x/run.sh",
        "d:other": "lib/run.sh", "e:via-passage": None,
    }
    chosen = search_module._one_record_per_path(
        rows, "run.sh", paths=paths, ranked_types={"source_file"})
    # One row per path: the file over its symbol and over a case variant;
    # the passage-reached record last.
    assert [r["node_id"] for r in chosen] == ["c:file", "d:other", "e:via-passage"]
    assert all(r["matched_on"] == "path" for r in chosen)


def test_a_partial_read_says_the_choice_was_among_the_first_read():
    text = render(tool="search", payload=_payload({
        "span": FILE, "status": "no_exact_match", "count": 0, "count_basis": "exact",
        "path_status": "matched", "path_count": 40, "path_count_basis": "exact",
        "path_count_unit": "record", "path_candidates_read": 25,
        "records": [{"node_id": "file:a", "subtype": "source_file", "matched_on": "path"}],
    }))
    assert (f'= exact "{FILE}" (path, 1 of 40 records; one per path among the '
            "first 25 read):") in text


def test_a_path_the_reader_cannot_read_is_its_own_group_after_known_paths():
    search_module = importlib.import_module("okg.substrate.mcp.search")
    rows = [
        {"node_id": "a:masked", "subtype": "source_file"},
        {"node_id": "b:case", "subtype": "code_symbol"},
        {"node_id": "c:masked-too", "subtype": "source_file"},
    ]
    paths = {"a:masked": None, "b:case": "x/RUN.SH", "c:masked-too": None}
    chosen = search_module._one_record_per_path(
        rows, "run.sh", paths=paths, ranked_types={"source_file"})
    # Never exact-case, never merged with each other, after a known path.
    assert [r["node_id"] for r in chosen] == ["b:case", "a:masked", "c:masked-too"]


def test_a_bare_slash_is_never_a_span():
    from okg.substrate.mcp.grep_query import exact_spans

    for query in ("/", "//", "////", "/ /"):
        assert exact_spans(query) == ((), ())


def test_a_stopped_choice_says_how_many_matches_were_read():
    text = render(tool="search", payload=_payload({
        "span": FILE, "status": "matched", "count": 1, "count_basis": "exact",
        "count_unit": "record", "path_status": "timeout", "path_count": 4,
        "path_count_basis": "exact", "path_count_unit": "record", "path_candidates_read": 4,
        "records": [{"node_id": "commit:b", "subtype": "commit"}],
    }))
    assert (f'= exact "{FILE}" (path, 4 records): stopped (timeout) after reading 4 '
            "matches, before one per path was chosen") in text


# ---------- PostgreSQL: the real search ----------

def _lexical(dsn: str, deployment: str, query: str, **kwargs) -> dict:
    with psycopg.connect(dsn, autocommit=True) as conn:
        session = open_session(
            conn, deployment_name=deployment,
            **({"nomos_subject": kwargs.pop("subject")} if "subject" in kwargs else {}),
        )
        return search(conn, session, query, method="lexical", exact_pass=True, **kwargs)


def _entry(page: dict, span: str) -> dict:
    (entry,) = [e for e in page["exact_matches"]["spans"] if e["span"] == span]
    return entry


def _path_ids(entry: dict) -> list[str]:
    return [r["node_id"] for r in entry.get("records") or [] if r.get("matched_on") == "path"]


def _commits_and_file() -> list:
    """Five tickets mention the file by name; the file's own text names it
    once; a passage of another page only inherits a matching path."""
    return [
        *[
            _node(f"ticket:c{i}", "ticket",
                  f"Commit {i}: update {FILE} {FILE} for the nightly table dump.",
                  status="open")
            for i in range(5)
        ],
        _node("note:file", "note", f"#!/bin/bash\n# {FILE}: dump the rucio tables",
              path=f"docker/scripts/{FILE}"),
        _node("note:elsewhere", "note", "an unrelated script", path="docker/scripts/other.sh"),
    ]


@pytest.mark.postgres
def test_the_file_comes_first_when_more_records_mention_it(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_commits_and_file())
    page = _lexical(clean_runtime, deployment, FILE, limit=10)
    entry = _entry(page, FILE)
    # The precondition: more records hold the name than the span lists.
    assert entry["count"] == 6 > len(entry["records"]) == 3
    assert entry["records"][0]["node_id"] == "note:file"
    assert entry["records"][0]["matched_on"] == "path"
    assert (entry["path_status"], entry["path_count"]) == ("matched", 1)
    ids = [r["node_id"] for r in entry["records"]]
    assert len(ids) == len(set(ids))
    assert all(i.startswith("ticket:") for i in ids[1:])
    text = render(page, tool="search")
    path_line = text.index(f'= exact "{FILE}" (path, 1 of 1 records):')
    assert path_line < text.index(f'= exact "{FILE}" (grep, 2 of 6 records):')


def _file_with_symbols() -> list:
    """A file record and three code-symbol-like records sharing its path,
    with ids that sort before the file's, and five records mentioning the
    file's name in their text."""
    return [
        _node("page:zz-file", "page", "the tool body", path="src/tool/check.py"),
        *[_node(f"note:sym:{i}", "note", f"symbol {i}", path="src/tool/check.py")
          for i in range(3)],
        *[_node(f"ticket:m{i}", "ticket", f"Commit {i} edits check.py again.", status="open")
          for i in range(5)],
    ]


@pytest.mark.postgres
def test_a_file_and_its_symbols_list_once_and_text_fills_the_slots(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_file_with_symbols())
    page = _lexical(clean_runtime, deployment, "check.py", limit=10)
    entry = _entry(page, "check.py")
    ids = [r["node_id"] for r in entry["records"]]
    # The file represents its path, not the symbol whose id sorts first.
    assert ids[0] == "page:zz-file"
    assert _path_ids(entry) == ["page:zz-file"]
    assert all(i.startswith("ticket:m") for i in ids[1:]) and len(ids) == 3
    # Every own-path match is counted.
    assert entry["path_count"] == 4
    assert "path_candidates_read" not in entry
    assert '= exact "check.py" (path, 1 of 4 records):' in render(page, tool="search")


@pytest.mark.postgres
def test_a_partial_path_read_is_disclosed(clean_runtime, tmp_path, monkeypatch):
    search_module = importlib.import_module("okg.substrate.mcp.search")
    monkeypatch.setattr(search_module, "EXACT_PATH_CANDIDATES", 2)
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_file_with_symbols())
    page = _lexical(clean_runtime, deployment, "check.py", limit=10)
    entry = _entry(page, "check.py")
    assert entry["path_candidates_read"] == 2
    assert entry["path_count"] == 4
    assert len(_path_ids(entry)) == 1
    assert "one per path among the first 2 read" in render(page, tool="search")


@pytest.mark.postgres
def test_an_exact_case_match_represents_its_path(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=[
        _node("page:upper", "page", "upper case copy", path="x/RUN2.SH"),
        _node("note:exact", "note", "the runner", path="x/run2.sh"),
    ])
    entry = _entry(_lexical(clean_runtime, deployment, "run2.sh", limit=3), "run2.sh")
    assert _path_ids(entry) == ["note:exact"]
    assert entry["path_count"] == 2


@pytest.mark.postgres
def test_a_dir_and_name_span_matches_its_path_tail(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=[
        _node("note:tail", "note", "the module body", path="src/dir/name.ext"),
        _node("note:exact", "note", "the module body", path="dir/name.ext"),
        _node("note:glued", "note", "the module body", path="src/xdir/name.ext"),
        _node("note:longer", "note", "the module body", path="src/dir/name.ext.bak"),
    ])
    entry = _entry(_lexical(clean_runtime, deployment, "dir/name.ext", limit=3),
                   "dir/name.ext")
    assert sorted(_path_ids(entry)) == ["note:exact", "note:tail"]
    assert entry["path_count"] == 2


@pytest.mark.postgres
def test_run_sh_does_not_match_rerun_sh_and_exact_case_comes_first(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=[
        _node("note:a-upper", "note", "upper case copy", path="tools/RUN.SH"),
        _node("note:b-exact", "note", "the runner", path="bin/run.sh"),
        _node("note:rerun", "note", "the rerunner", path="bin/rerun.sh"),
        _node("note:rerun-root", "note", "the rerunner", path="rerun.sh"),
    ])
    entry = _entry(_lexical(clean_runtime, deployment, "run.sh", limit=3), "run.sh")
    assert _path_ids(entry) == ["note:b-exact", "note:a-upper"]
    assert entry["path_count"] == 2


@pytest.mark.postgres
def test_a_hidden_file_is_never_listed_or_counted(clean_runtime, tmp_path, monkeypatch):
    groups = {
        OPEN: [_node("note:open", "note", "open copy of the script",
                     path="open/dump_secret.sh")],
        SECRET: [_node("note:secret", "note", "secret copy of the script",
                       path="secret/dump_secret.sh")],
    }
    deployment = _publish(tmp_path, clean_runtime, groups=groups, main=[])
    _activate(clean_runtime, deployment, [_allow(OPEN), _allow(MAIN)])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    page = _lexical(clean_runtime, deployment, "dump_secret.sh", limit=3, subject=READER)
    entry = _entry(page, "dump_secret.sh")
    assert _path_ids(entry) == ["note:open"]
    assert entry["path_count"] == 1
    assert "note:secret" not in render(page, tool="search")


@pytest.mark.postgres
def test_a_profile_path_glob_and_where_bound_the_path_pass(clean_runtime, tmp_path):
    from okg.substrate.mcp.search_profiles import ResolvedScope

    deployment = _publish(tmp_path, clean_runtime, groups={}, main=[
        _node("note:kept", "note", "kept copy", path="app/build.sh"),
        _node("note:vendored", "note", "vendored copy", path="vendor/build.sh"),
        _node("ticket:kept", "ticket", "ticket copy", path="tickets/build.sh",
              status="open"),
    ])
    everything = _entry(_lexical(clean_runtime, deployment, "build.sh", limit=3),
                        "build.sh")
    assert sorted(_path_ids(everything)) == ["note:kept", "note:vendored", "ticket:kept"]
    by_type = _entry(_lexical(clean_runtime, deployment, "build.sh", limit=3,
                              where={"op": "eq", "field": "type", "value": "ticket"}),
                     "build.sh")
    assert _path_ids(by_type) == ["ticket:kept"]
    by_glob = _entry(_lexical(clean_runtime, deployment, "build.sh", limit=3,
                              scope=ResolvedScope(excluded_path_globs=("vendor/**",))),
                     "build.sh")
    assert sorted(_path_ids(by_glob)) == ["note:kept", "ticket:kept"]
    assert by_glob["path_count"] == 2


@pytest.mark.postgres
def test_an_identifier_that_is_not_a_file_name_is_unchanged(
    clean_runtime, tmp_path, monkeypatch,
):
    grep_module = importlib.import_module("okg.substrate.mcp.grep_search")
    real = grep_module.search_grep
    calls: list[dict] = []

    def counting(conn, session, query, **kwargs):
        calls.append({"query": query, **kwargs})
        return real(conn, session, query, **kwargs)

    monkeypatch.setattr(grep_module, "search_grep", counting)
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_facts())
    entry = _entry(_lexical(clean_runtime, deployment, f"{KEY} dataset", limit=3), KEY)
    assert [c["query"] for c in calls] == [f'"{KEY}"']
    assert "own_path_tail" not in calls[0]
    assert set(entry) == {"span", "status", "count", "count_basis", "count_unit", "records"}
    assert all("matched_on" not in r for r in entry["records"])


@pytest.mark.postgres
def test_a_graph_without_paths_adds_no_path_line(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_facts())
    page = _lexical(clean_runtime, deployment, "detail.html", limit=3)
    entry = _entry(page, "detail.html")
    assert (entry["path_status"], entry["status"]) == ("no_exact_match", "no_exact_match")
    text = render(page, tool="search")
    assert "(path" not in text
    assert '= exact "detail.html" (grep): no record holds this text' in text


@pytest.mark.postgres
def test_the_path_pass_is_skipped_when_the_budget_is_spent(
    clean_runtime, tmp_path, monkeypatch,
):
    search_module = importlib.import_module("okg.substrate.mcp.search")
    monkeypatch.setattr(search_module, "EXACT_PASS_BUDGET_MS", 0)
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_commits_and_file())
    entry = _entry(_lexical(clean_runtime, deployment, FILE, limit=3), FILE)
    assert (entry["path_status"], entry["status"]) == (
        "skipped:time_budget", "skipped:time_budget")


@pytest.mark.postgres
def test_the_path_pass_runs_under_the_deadline(clean_runtime, tmp_path, monkeypatch):
    """Every grep statement of the path pass gets at most the pass's
    remaining budget."""
    search_module = importlib.import_module("okg.substrate.mcp.search")
    grep_module = importlib.import_module("okg.substrate.mcp.grep_search")
    monkeypatch.setattr(search_module, "EXACT_PASS_BUDGET_MS", 300)
    real_grep = grep_module.search_grep
    real_timeout = grep_module._statement_timeout_ms
    in_path_pass: list[bool] = [False]
    seen: list[int] = []

    def tracking(conn, session, query, **kwargs):
        in_path_pass[0] = kwargs.get("own_path_tail") is not None
        try:
            return real_grep(conn, session, query, **kwargs)
        finally:
            in_path_pass[0] = False

    def recording() -> int:
        value = real_timeout()
        if in_path_pass[0]:
            seen.append(value)
        return value

    monkeypatch.setattr(grep_module, "search_grep", tracking)
    monkeypatch.setattr(grep_module, "_statement_timeout_ms", recording)
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_commits_and_file())
    entry = _entry(_lexical(clean_runtime, deployment, FILE, limit=3), FILE)
    assert seen and max(seen) <= 300
    assert entry["path_status"] in ("matched", "timeout")


@pytest.mark.postgres
def test_a_path_pass_timeout_is_disclosed_and_the_text_pass_still_runs(
    clean_runtime, tmp_path, monkeypatch,
):
    from okg.substrate.mcp.grep_search import GrepTimeout

    grep_module = importlib.import_module("okg.substrate.mcp.grep_search")
    real = grep_module.search_grep

    def slow_paths(conn, session, query, **kwargs):
        if kwargs.get("own_path_tail") is not None:
            raise GrepTimeout("ran past the statement timeout")
        return real(conn, session, query, **kwargs)

    monkeypatch.setattr(grep_module, "search_grep", slow_paths)
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_commits_and_file())
    page = _lexical(clean_runtime, deployment, FILE, limit=3)
    entry = _entry(page, FILE)
    assert (entry["path_status"], entry["status"]) == ("timeout", "matched")
    assert _path_ids(entry) == []
    assert f'= exact "{FILE}" (path): checked, stopped early (timeout)' in render(
        page, tool="search")


@pytest.mark.postgres
def test_a_database_error_in_the_path_pass_stops_the_spans_after_it(
    clean_runtime, tmp_path, monkeypatch,
):
    grep_module = importlib.import_module("okg.substrate.mcp.grep_search")

    def broken(conn, session, query, **kwargs):
        raise psycopg.errors.QueryCanceled("slow")

    monkeypatch.setattr(grep_module, "search_grep", broken)
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_commits_and_file())
    page = _lexical(clean_runtime, deployment, f"{FILE} {KEY}", limit=3)
    assert page["exact_matches"]["spans"] == [
        {"span": FILE, "status": "skipped:after_error", "path_status": "error:QueryCanceled"},
        {"span": KEY, "status": "skipped:after_error"},
    ]


# ---------- MCP: the real tool ----------

@pytest.mark.postgres
def test_the_tool_no_longer_runs_the_path_pass(clean_runtime, okg_mcp_can_log_in, tmp_path):
    """PACT read-passage-rows-3666 (R4): the served lexical search carries no
    exact block; the pass's helpers stay for in-process callers."""
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_commits_and_file())
    served = Served(clean_runtime, deployment)
    try:
        page = served.json(query=FILE, method="lexical", limit=5)
        assert "exact_matches" not in page, json.dumps(page)[:600]
        assert page["results"], json.dumps(page)[:600]
        text = served.text(query=FILE, method="lexical", limit=5)
        assert f'= exact "{FILE}"' not in text
    finally:
        served.close()


@pytest.mark.postgres
def test_regex_characters_in_a_span_are_literal(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=[
        _node("note:plus", "note", "plus body", path="src/lib+core.py"),
        _node("note:libbcore", "note", "body", path="src/libbcore.py"),
        _node("note:libcore", "note", "body", path="src/libcore.py"),
        _node("note:bracket", "note", "bracket body", path="cfg/conf[1].py"),
        _node("note:conf1", "note", "body", path="cfg/conf1.py"),
    ])
    plus = _entry(_lexical(clean_runtime, deployment, "lib+core.py", limit=3), "lib+core.py")
    assert (_path_ids(plus), plus["path_count"]) == (["note:plus"], 1)
    # Brackets glue words in unquoted text, so the bracketed name is quoted.
    bracket = _entry(_lexical(clean_runtime, deployment, '"conf[1].py"', limit=3),
                     "conf[1].py")
    assert (_path_ids(bracket), bracket["path_count"]) == (["note:bracket"], 1)


@pytest.mark.postgres
def test_a_span_starting_with_a_slash_matches_that_path_only(clean_runtime, tmp_path):
    """`/etc/run7.sh` is matched as written: a path equal to it (or ending
    in `//etc/run7.sh`). The relative `etc/run7.sh` is not its tail."""
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=[
        _node("note:absolute", "note", "absolute", path="/etc/run7.sh"),
        _node("note:relative", "note", "relative", path="etc/run7.sh"),
        _node("note:nested", "note", "nested", path="root/etc/run7.sh"),
    ])
    entry = _entry(_lexical(clean_runtime, deployment, "/etc/run7.sh", limit=3),
                   "/etc/run7.sh")
    assert (_path_ids(entry), entry["path_count"]) == (["note:absolute"], 1)


@pytest.mark.postgres
def test_a_bare_slash_own_path_grep_is_refused(clean_runtime, tmp_path):
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_commits_and_file())
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        session = open_session(conn, deployment_name=deployment)
        with pytest.raises(SubstrateError) as refused:
            search(conn, session, "/", method="grep", own_path_tail="/", limit=3)
    assert refused.value.code == "grep_pattern_too_short"


@pytest.mark.postgres
@pytest.mark.parametrize(("helper", "failure", "path_status"), [
    ("own_path_reads", "timeout", "timeout"),
    ("path_record_subtypes", "timeout", "timeout"),
    ("path_record_subtypes", "defect", "error:RuntimeError"),
])
def test_a_failing_path_helper_is_disclosed_and_the_text_pass_runs(
    clean_runtime, tmp_path, monkeypatch, helper, failure, path_status,
):
    from okg.substrate.mcp.grep_search import GrepTimeout

    grep_module = importlib.import_module("okg.substrate.mcp.grep_search")

    def fail(*args, **kwargs):
        if failure == "timeout":
            raise GrepTimeout("ran past the deadline")
        raise RuntimeError("defect")

    monkeypatch.setattr(grep_module, helper, fail)
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_commits_and_file())
    page = _lexical(clean_runtime, deployment, FILE, limit=3)
    entry = _entry(page, FILE)
    assert (entry["path_status"], entry["status"]) == (path_status, "matched")
    assert _path_ids(entry) == []
    if failure == "timeout":
        # The matches were read and counted before the choice stopped.
        assert (entry["path_candidates_read"], entry["path_count"]) == (1, 1)
        assert "after reading 1 matches" in render(page, tool="search")
    else:
        assert "path_candidates_read" not in entry


@pytest.mark.postgres
def test_a_database_error_in_a_path_helper_stops_the_spans_after_it(
    clean_runtime, tmp_path, monkeypatch,
):
    grep_module = importlib.import_module("okg.substrate.mcp.grep_search")

    def broken(*args, **kwargs):
        raise psycopg.errors.QueryCanceled("slow")

    monkeypatch.setattr(grep_module, "own_path_reads", broken)
    deployment = _publish(tmp_path, clean_runtime, groups={}, main=_commits_and_file())
    page = _lexical(clean_runtime, deployment, f"{FILE} {KEY}", limit=3)
    assert page["exact_matches"]["spans"] == [
        {"span": FILE, "status": "skipped:after_error", "path_status": "error:QueryCanceled"},
        {"span": KEY, "status": "skipped:after_error"},
    ]


@pytest.mark.postgres
def test_a_masked_path_never_groups_or_orders_a_record(clean_runtime, tmp_path, monkeypatch):
    """A passage with a readable path folds into a record whose path the
    policy masks. The record's stored path (the exact span) must not make it
    the exact-case representative or leak into the page: it is its own
    group, after the readable case-variant path."""
    groups = {
        OPEN: [
            _node("comment:open:run9", "comment", "the script body", path="secret/run9.sh"),
            _node("note:case", "note", "a case variant", path="lib/RUN9.SH"),
        ],
        MASKED: [_node("page:masked", "page", "Masked page.", path="secret/run9.sh")],
    }
    deployment = _publish(tmp_path, clean_runtime, groups=groups,
                          main=[_edge("page:masked", "comment:open:run9")])
    _activate(clean_runtime, deployment, [_allow(OPEN), _allow(MAIN), {
        "rule_id": "redact_masked", "effect": "redact", "actions": ["graph.read.mcp"],
        "subjects": [READER], "match": {"source_id": MASKED}, "priority": 100,
        "obligations": [{"type": "redaction"}, {"type": "audit"}],
    }])
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        conn.execute(
            "UPDATE okg.subtypes SET metadata = coalesce(metadata, '{}'::jsonb)"
            " || '{\"pii_classes\": {\"text\": \"non_pii\", \"path\": \"quasi_identifier\"}}'::jsonb"
            " WHERE name = 'page'"
        )
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    page = _lexical(clean_runtime, deployment, "run9.sh", limit=3, subject=READER)
    entry = _entry(page, "run9.sh")
    ids = _path_ids(entry)
    # The precondition: the passage folded into the masked record.
    assert "page:masked" in ids, entry
    assert ids.index("note:case") < ids.index("page:masked")
    assert "secret/run9.sh" not in render(page, tool="search")
