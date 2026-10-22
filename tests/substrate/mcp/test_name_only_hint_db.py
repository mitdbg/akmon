"""A read of file names names the grep over file texts, served (lane ri, q020).

One published fixture shaped like the rucio-flux repository in CompOps q020.
Each git file is a `source_file` record whose text is its path words and a
`document` whose passages hold its contents. The file that matters,
`apps/production/prod-daemons.yaml`, configures reapers in its text and has
no "reaper" in its name, as `prod-rucio-daemons.yaml` did. The calls are the
shapes the agents sent (R1-R7 of the lane ri findings, C1 the control); each
line's named call is run and must return the file the name test missed, with
the count the line printed.
"""
from __future__ import annotations

import json
import pathlib
import re
from typing import Any

import pytest

from okg.substrate.library.sources.base import EdgeFact
from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp._result_payload import JSON_META, payload, result_text
from tests.substrate.mcp.search_fixture import make_search_deployment, node, publish_nodes
from tests.substrate.mcp.test_page_next_db import _drive

pytestmark = pytest.mark.postgres

SUBTYPES = [
    SubtypeSpec("source_file", metadata={
        "searchable_text_fields": ["text"], "path_from": "path",
    }),
    SubtypeSpec("document", metadata={
        "searchable_text_fields": ["text"], "path_from": "path",
        "passage_rollup": {"edge_type": "contains", "child_subtype": "document_chunk"},
    }),
    SubtypeSpec("document_chunk", metadata={"searchable_text_fields": ["text"]}),
]

#: (repo, path, document title, chunk texts)
FILES = [
    ("flux", "apps/production/prod-daemons.yaml", "prod daemons values",
     ["reaperCount: 8\nreaper:\n  chunkSize: 4000\n  sleepTime: 120\n",
      "judgeEvaluatorCount: 2\n"]),
    ("flux", "apps/production/tier0-reaper-daemons.yaml", "tier0 values",
     ["reaperCount: 2\nreaper:\n  chunkSize: 4000\n"]),
    ("flux", "apps/options/tier0-reaper/helm.yaml", "tier0 helm release",
     ["kind: HelmRelease\ncomponent: reaper\n"]),
    ("flux", "apps/options/common/notes.md", "operator notes", ["nothing about deletion\n"]),
    ("other", "lib/reaper.py", "a module", ["def reaper():\n    pass\n"]),
]
PROD_DOC = "doc:flux:apps/production/prod-daemons.yaml"
FLUX_TEXTS_WITH_REAPER = {
    "doc:flux:apps/production/prod-daemons.yaml",
    "doc:flux:apps/production/tier0-reaper-daemons.yaml",
    "doc:flux:apps/options/tier0-reaper/helm.yaml",
}
ALL_TEXTS_WITH_REAPER = FLUX_TEXTS_WITH_REAPER | {"doc:other:lib/reaper.py"}


def _path_words(path: str) -> str:
    return f"{path}\n\n{' '.join(re.split(r'[/._-]+', path))}"


@pytest.fixture
def deployment(clean_runtime: str, okg_mcp_can_log_in: str, tmp_path: pathlib.Path) -> str:
    deployment = make_search_deployment(tmp_path, dsn=clean_runtime)
    nodes, edges = [], []
    for repo, path, title, chunks in FILES:
        nodes.append(node(f"file:{repo}:{path}", "source_file", _path_words(path),
                          path=path, repo=repo))
        doc = f"doc:{repo}:{path}"
        nodes.append(node(doc, "document", title, path=path, title=title))
        for i, text in enumerate(chunks):
            chunk = f"chunk:{repo}:{path}:{i}"
            nodes.append(node(chunk, "document_chunk", text,
                              chunk_ordinal=i, chunk_total=len(chunks)))
            edges.append(EdgeFact(src=doc, dst=chunk, edge_type="contains",
                                  source_record_id={"id": f"{chunk}-c"},
                                  source_revision={"v": 1}))
    publish_nodes(
        clean_runtime, deployment, subtypes=SUBTYPES,
        narrowings=[("document", "contains", "document_chunk")],
        nodes=nodes, edges=edges,
    )
    return deployment


async def _call(client: Any, tool: str, arguments: dict) -> tuple[dict, str]:
    """The JSON and the text form of one call."""
    as_json = await client.call_tool(tool, arguments, meta=JSON_META)
    assert not as_json.isError, result_text(as_json)
    as_text = await client.call_tool(tool, arguments)
    assert not as_text.isError, result_text(as_text)
    return payload(as_json), result_text(as_text)


def _ids(page: dict) -> set[str]:
    rows = page.get("results", page.get("items")) or []
    return {str(row.get("node_id", row.get("id"))) for row in rows}


def _line(text: str) -> str:
    found = [line for line in text.splitlines() if line.startswith("! matched file names only")]
    assert len(found) == 1, text
    return found[0]


async def _follow(client: Any, page: dict, text: str, *, count: int, docs: set[str]) -> None:
    """Gate (1): the named call runs as printed and returns the file texts
    the line counted, the missed file among them."""
    block = page["names_only"]
    assert block["count"] == count and block["count_basis"] == "exact"
    line = _line(text)
    arguments = block["call"]["arguments"]
    printed = "search(" + ", ".join(
        f"{k}={json.dumps(v, separators=(',', ':'), ensure_ascii=False)}"
        for k, v in arguments.items()
    ) + ")"
    assert line.endswith(": " + printed), line
    named, named_text = await _call(client, "search", {**arguments, "limit": 50})
    assert named["total_count"] == count and named["total_count_basis"] == "exact"
    assert _ids(named) == docs
    assert PROD_DOC.split(":", 2)[2] in named_text
    assert "names_only" not in named


R1 = {"where": {"type": "source_file", "attrs.repo": "flux",
                "attrs.path": {"op": "contains", "value": "reaper"}},
      "limit": 100, "select": ["node_id", "attrs.path"]}
R2 = {"method": "grep", "query": "reaper",
      "where": {"type": "source_file", "attrs.repo": "flux"}, "limit": 60,
      "select": ["node_id", "attrs.path"]}
R3 = {"method": "grep", "query": "reaper", "where": {"type": "source_file"}, "limit": 20,
      "select": ["node_id", "attrs.path"]}
R6 = {"where": {"type": "document", "attrs.path": {"op": "contains", "value": "reaper"}},
      "limit": 50, "select": ["node_id", "attrs.path"]}
R7 = {"query": "reaper chunkSize", "limit": 20, "where": {"type": "source_file"}}
C1 = {"query": "reaper", "method": "grep", "path_prefix": "apps/", "limit": 50,
      "max_chars": 2500}


def test_r1_a_path_filter_names_the_texts_its_names_missed(
    clean_runtime: str, deployment: str,
) -> None:
    async def body(client: Any) -> None:
        page, text = await _call(client, "filter", R1)
        # The filter itself is unchanged: two files are named *reaper*.
        assert _ids(page) == {"file:flux:apps/production/tier0-reaper-daemons.yaml",
                              "file:flux:apps/options/tier0-reaper/helm.yaml"}
        assert _line(text) == (
            "! matched file names only (the path test reads names, not file text); "
            '3 file texts in flux contain "reaper": search(query="reaper", method="grep", '
            'where={"type":"document","id":{"op":"prefix","value":"doc:flux:"}})'
        )
        assert "names_only" not in text.split("--- other fields", 1)[-1]
        await _follow(client, page, text, count=3, docs=FLUX_TEXTS_WITH_REAPER)

    _drive(clean_runtime, deployment, body)


def test_r2_r3_a_grep_over_file_records_says_path_words_and_names_the_texts(
    clean_runtime: str, deployment: str,
) -> None:
    async def body(client: Any) -> None:
        page, text = await _call(client, "search", R2)
        assert _ids(page) == {"file:flux:apps/production/tier0-reaper-daemons.yaml",
                              "file:flux:apps/options/tier0-reaper/helm.yaml"}
        assert page["grep_searched"]["name_only"] is True
        assert ("! searched: path words (a source_file holds no file contents; "
                "contents are document records)") in text
        assert "searched: text" not in text
        assert _line(text).startswith(
            "! matched file names only (a source_file holds a file's path, not its text); "
            '3 file texts in flux contain "reaper": '
        )
        await _follow(client, page, text, count=3, docs=FLUX_TEXTS_WITH_REAPER)

        page, text = await _call(client, "search", R3)
        assert "file:other:lib/reaper.py" in _ids(page)
        assert '4 file texts contain "reaper": search(query="reaper", method="grep", ' \
               'where={"type":"document"})' in _line(text)
        await _follow(client, page, text, count=4, docs=ALL_TEXTS_WITH_REAPER)

    _drive(clean_runtime, deployment, body)


def test_r6_r7_a_document_path_filter_and_a_ranked_search_over_files(
    clean_runtime: str, deployment: str,
) -> None:
    async def body(client: Any) -> None:
        page, text = await _call(client, "filter", R6)
        assert PROD_DOC not in _ids(page)
        await _follow(client, page, text, count=4, docs=ALL_TEXTS_WITH_REAPER)

        page, text = await _call(client, "search", R7)
        assert page["names_only"]["reason"] == "file_records"
        assert '2 file texts hold every term of "reaper chunkSize"' in _line(text)
        await _follow(client, page, text, count=2, docs={
            PROD_DOC, "doc:flux:apps/production/tier0-reaper-daemons.yaml",
        })

    _drive(clean_runtime, deployment, body)


def test_c1_a_grep_over_texts_and_uncarried_conditions_get_no_line(
    clean_runtime: str, deployment: str,
) -> None:
    async def body(client: Any) -> None:
        page, text = await _call(client, "search", C1)
        assert "names_only" not in page and "matched file names only" not in text
        # A grep row on dev can carry `parent: None`; read the row's own id then.
        shown = {
            str((r.get("parent") or {}).get("node_id") or r.get("node_id") or "")
            for r in page["results"]
        }
        assert PROD_DOC in shown or "prod-daemons.yaml" in text
        # Failure case: a condition the text search cannot carry names nothing.
        page, text = await _call(client, "filter", {"where": {
            "type": "source_file", "attrs.repo": "flux", "attrs.language": "yaml",
            "attrs.path": {"op": "contains", "value": "reaper"}}})
        assert "names_only" not in page and "matched file names only" not in text
        # A later page does not repeat the count.
        page, _text = await _call(client, "filter", {**R1, "offset": 1})
        assert "names_only" not in page

    _drive(clean_runtime, deployment, body)


def test_no_file_text_holding_the_term_names_no_call(
    clean_runtime: str, deployment: str,
) -> None:
    """Failure case: "options" is in a name and in no file text; the line
    says so and names no call that would return nothing."""
    async def body(client: Any) -> None:
        page, text = await _call(client, "filter", {"where": {
            "type": "source_file", "attrs.repo": "flux",
            "attrs.path": {"op": "contains", "value": "options"}}})
        assert len(_ids(page)) == 2
        assert page["names_only"]["count"] == 0 and "call" not in page["names_only"]
        assert _line(text) == (
            "! matched file names only (the path test reads names, not file text); "
            'no file text in flux contains "options"'
        )
        named, _ = await _call(client, "search", {
            "query": "options", "method": "grep",
            "where": {"type": "document", "id": {"op": "prefix", "value": "doc:flux:"}}})
        assert named["total_count"] == 0

    _drive(clean_runtime, deployment, body)


#: CompOps q020, r36d new t1 call 8: a text search whose path test kept only
#: the files named *reaper*. `prod-rucio-daemons.yaml` (here
#: `prod-daemons.yaml`) was never opened and the answer counted 6 pods.
R8 = {"query": "reaper",
      "where": {"type": "document", "attrs.path": {"op": "contains", "value": "reaper"}},
      "limit": 50, "select": ["node_id", "attrs.path"], "max_chars": 200}


def test_r8_a_text_search_with_a_path_test_names_the_texts_the_names_missed(
    clean_runtime: str, deployment: str,
) -> None:
    async def body(client: Any) -> None:
        page, text = await _call(client, "search", R8)
        assert PROD_DOC not in _ids(page), "the path test hides the file"
        assert page["names_only"]["reason"] == "path_filter"
        assert _line(text).startswith(
            "! matched file names only (the path test kept only files with "
            'matching names); 4 file texts contain "reaper": '
            'search(query="reaper", method="grep", where={"type":"document"})'
        ), _line(text)
        # Gate: the named call runs and returns the file the path test missed.
        await _follow(client, page, text, count=4, docs=ALL_TEXTS_WITH_REAPER)

    _drive(clean_runtime, deployment, body)


def test_r8_without_the_path_test_or_over_other_types_names_nothing(
    clean_runtime: str, deployment: str,
) -> None:
    """Must-fail partners: the same query with no path test is a read of
    texts, and a path test over a type that is not file text is not carried."""
    async def body(client: Any) -> None:
        page, text = await _call(client, "search", {
            "query": "reaper", "where": {"type": "document"}, "limit": 50,
            "select": ["node_id"]})
        assert "names_only" not in page and "matched file names only" not in text
        page, text = await _call(client, "search", {
            "query": "reaper", "where": {"type": "source_file",
            "attrs.path": {"op": "contains", "value": "flux"}}, "limit": 10})
        assert "names_only" not in page and "matched file names only" not in text

    _drive(clean_runtime, deployment, body)
