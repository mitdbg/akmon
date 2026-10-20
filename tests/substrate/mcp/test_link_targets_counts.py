"""`expand` says how many cited records are not loaded (issue #3666).

PACT link-endpoints-3666. A commit cites two Jira keys; one record is
loaded, the other is a "known by key, not loaded" endpoint. With access rules
off the served reply counts "2 cited, 1 not loaded" and names the key, in
JSON (`link_targets`) and in text, and the endpoint's own row says it is
not loaded, in expand and in search. Under audit and under enforce the
endpoint and the link to it are hidden, nothing is counted
(`basis: not_counted`) and the key is never named, also not as a sample
value of inspect(subtype).

Only links of a type an `on_miss` route produces are counted, and a
deployment with no such route gets no `link_targets` at all.

The pure counting rows run with no database; the served rows publish.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import psycopg
import pytest

from okg.substrate.mcp.link_targets import (
    endpoint_edge_types,
    endpoint_load_note,
    link_target_counts,
    link_targets_line,
    record_link_target_counts,
)

MISSING = "jira:CMSTRANSF-452"
LOADED = "jira:CMSTRANSF-1"
CITING = "commit:r:abc"
READER = "subject:link-target-reader"
CITES = frozenset({"references"})

LINKER_CONFIG = """\
routes:
  cms_jira_key:
    edge_type: references
    dst_template: "{resolved}"
    on_miss:
      endpoint_id: "jira:{value}"
      subtype: jira_issue
      key_attr: issue_key
      scope: {attr: project, regex: "^([A-Z0-9]+)-"}
"""


def _row(
    dst: str, *, not_loaded: bool, direction: str = "out", edge_type: str = "references",
) -> dict:
    neighbor = {"subtype": "jira_issue", "title": dst}
    if not_loaded:
        neighbor["load_state"] = "not_loaded"
    return {"dst": dst, "src": CITING, "direction": direction,
            "edge_type": edge_type, "neighbor": neighbor}


def test_two_cited_one_not_loaded_counts_and_says_so() -> None:
    rows = [
        _row(LOADED, not_loaded=False),
        _row(MISSING, not_loaded=True),
        _row("commit:r:other", not_loaded=False, direction="in"),
    ]
    counts = link_target_counts(rows, counted=True, complete=True, edge_types=CITES)
    assert counts == {
        "cited": 2, "loaded": 1, "not_loaded": 1,
        "not_loaded_keys": [MISSING], "basis": "exact",
    }
    assert link_targets_line(counts) == (
        "! links: 2 cited, 1 not loaded (known by key; the graph holds no "
        f"record): {MISSING}")
    # Nothing not loaded: nothing to say.
    assert link_targets_line(
        link_target_counts(rows[:1], counted=True, complete=True)) is None
    # A page that is not the whole list says so.
    assert link_target_counts(rows, counted=True, complete=False)["basis"] == "page"


def test_only_citation_link_types_are_cited() -> None:
    """Review L4: a commit's files and authors are not cited records."""
    rows = [
        _row(LOADED, not_loaded=False),
        _row(MISSING, not_loaded=True),
        _row("file:r:a.py", not_loaded=False, edge_type="touches"),
        _row("person:p", not_loaded=False, edge_type="authored_by"),
    ]
    counts = link_target_counts(rows, counted=True, complete=True, edge_types=CITES)
    assert (counts["cited"], counts["loaded"], counts["not_loaded"]) == (2, 1, 1)


def test_record_rows_count_like_link_rows() -> None:
    rows = [
        {"node_id": LOADED, "link": {"direction": "out", "edge_type": "references"}},
        {"node_id": MISSING, "link": {"direction": "out", "edge_type": "references"}},
        {"node_id": "file:r:a.py", "link": {"direction": "out", "edge_type": "touches"}},
        {"node_id": "commit:r:other", "link": {"direction": "in"}},
    ]
    titles = {
        LOADED: {"subtype": "jira_issue", "title": "a loaded ticket"},
        MISSING: {"subtype": "jira_issue", "title": None, "load_state": "not_loaded"},
    }
    assert record_link_target_counts(
        rows, titles=titles, counted=True, complete=True, edge_types=CITES,
    ) == {
        "cited": 2, "loaded": 1, "not_loaded": 1,
        "not_loaded_keys": [MISSING], "basis": "exact",
    }


def test_under_access_rules_the_count_and_the_keys_are_withheld() -> None:
    rows = [_row(MISSING, not_loaded=True)]
    counts = link_target_counts(rows, counted=False, complete=True)
    assert counts == {
        "cited": None, "loaded": None, "not_loaded": None,
        "not_loaded_keys": [], "basis": "not_counted",
    }
    assert link_targets_line(counts) is None


def test_the_endpoint_note_names_its_scope() -> None:
    note = endpoint_load_note({
        "issue_key": "CMSTRANSF-452", "project": "CMSTRANSF",
        "load_state": "not_loaded", "label": "CMSTRANSF-452",
        "text": "CMSTRANSF-452 (known by key, not loaded: the graph holds no record for it)",
    })
    assert note == (
        "known by key, not loaded (project CMSTRANSF): a record cites it, "
        "and the graph holds no record for it")


def _deployment_folder(root: Path, linker_config: str | None) -> Path:
    folder = root / "dep"
    folder.mkdir(parents=True)
    (folder / "deployment.yaml").write_text('{"name": "dep"}\n')
    if linker_config is not None:
        (folder / "linker_config.yaml").write_text(linker_config)
    return folder


def test_the_citation_link_types_come_from_the_on_miss_routes(tmp_path: Path) -> None:
    with_routes = _deployment_folder(tmp_path / "a", LINKER_CONFIG)
    assert endpoint_edge_types(str(with_routes)) == {"references"}
    # Failure case: no on_miss route, so no reply carries link_targets.
    plain = _deployment_folder(
        tmp_path / "b",
        'routes:\n  cms_jira_key:\n    edge_type: references\n'
        '    dst_template: "{resolved}"\n',
    )
    assert endpoint_edge_types(str(plain)) == frozenset()
    assert endpoint_edge_types(str(_deployment_folder(tmp_path / "c", None))) == frozenset()


# --------------------------------------------------------------------------- #
# Served, over a real publish                                                  #
# --------------------------------------------------------------------------- #


def _published(dsn: str, tmp_path) -> str:
    from okg.substrate.mcp import open_session
    from tests.substrate.mcp.search_fixture import make_search_deployment
    from tests.substrate.projection.test_link_endpoints_db import (
        JIRA_METADATA,
        publisher,
        seed,
    )

    deployment = make_search_deployment(tmp_path, dsn=dsn)
    (Path(deployment) / "linker_config.yaml").write_text(LINKER_CONFIG)
    # The publisher and the server name the deployment by its folder, as
    # `publish_nodes` does.
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed(conn, deployment=deployment, jira_metadata=JIRA_METADATA)
    assert publisher(dsn, deployment=deployment).cycle()["status"] == "published"
    with psycopg.connect(dsn, autocommit=True) as conn:
        assert open_session(conn, deployment_name=deployment).generation_id
    return deployment


def _calls(dsn: str, deployment: str) -> dict[str, Any]:
    from tests.substrate.mcp.test_read_grammar_db import Tools

    server = Tools(dsn, deployment)
    try:
        expand = {"input": {"node_id": CITING}}
        links = {**expand, "select": ["edge_id", "src", "dst", "edge_type", "direction"]}
        inspect = {"target": {"kind": "node", "node_id": MISSING}}
        subtype = {"target": {"kind": "subtype", "subtype": "jira_issue"}}
        search = {"query": "CMSTRANSF-452"}
        # The search goes first: the calls share one session, and after the
        # inspect of the endpoint printed it whole its search row says
        # `shown whole earlier (inspect)` (PACT read-reply-size-3666).
        replies = server.run(
            ("search", search, "json"), ("search", search, "text"),
            ("expand", expand, "json"), ("expand", expand, "text"),
            ("expand", links, "json"), ("expand", links, "text"),
            ("inspect", inspect, "json"), ("inspect", inspect, "text"),
            ("inspect", subtype, "json"),
        )
    finally:
        server.close()
    texts = [reply.content[0].text for reply in replies]
    return {
        "search": json.loads(texts[0]), "search_text": texts[1],
        "records": json.loads(texts[2]), "records_text": texts[3],
        "links": json.loads(texts[4]), "links_text": texts[5],
        "inspect": json.loads(texts[6]), "inspect_text": texts[7],
        "subtype": json.loads(texts[8]),
    }


def _far(reply: dict) -> list[str]:
    return sorted(
        str(row.get("node_id") or row.get("dst")) for row in reply["neighbors"]
    )


def _search_ids(reply: dict) -> set[str]:
    return {str(row.get("node_id")) for row in reply.get("results") or []}


@pytest.mark.postgres
def test_with_access_rules_off_the_reply_says_2_cited_1_not_loaded(
    clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch,
) -> None:
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "off")
    served = _calls(clean_runtime, _published(clean_runtime, tmp_path))
    expected = {
        "cited": 2, "loaded": 1, "not_loaded": 1,
        "not_loaded_keys": [MISSING], "basis": "exact",
    }
    line = (
        "! links: 2 cited, 1 not loaded (known by key; the graph holds no "
        f"record): {MISSING}"
    )
    for shape in ("records", "links"):
        assert _far(served[shape]) == [LOADED, MISSING], shape
        assert served[shape]["link_targets"] == expected, shape
        assert line in served[f"{shape}_text"], shape
    # Review M2a: the endpoint's own record row says so; a loaded one does not.
    rows = {row["node_id"]: row for row in served["records"]["neighbors"]}
    assert rows[MISSING]["load_state"] == "not_loaded"
    assert "load_state" not in rows[LOADED]
    (missing_line,) = [
        line for line in served["records_text"].splitlines() if MISSING in line
        and line.startswith("[")
    ]
    assert "not loaded (known by key)" in missing_line
    assert served["inspect"]["load_state"] == "not_loaded"
    assert "known by key, not loaded (project CMSTRANSF)" in served["inspect_text"]
    # Review M1: a search for the key finds the endpoint, and its row says
    # it is not loaded.
    assert MISSING in _search_ids(served["search"]), served["search"]
    (hit,) = [row for row in served["search"]["results"] if row["node_id"] == MISSING]
    assert "not loaded" in json.dumps(hit)
    assert "not loaded" in served["search_text"]
    # Control for the audit row below: with access rules off the endpoint's
    # key is one of inspect(subtype)'s sample values.
    assert "CMSTRANSF-452" in json.dumps(served["subtype"])


@pytest.mark.postgres
@pytest.mark.parametrize("mode", ["audit", "enforce"])
def test_under_access_rules_the_endpoint_and_its_key_are_withheld(
    clean_runtime, okg_mcp_can_log_in, tmp_path, monkeypatch, mode,
) -> None:
    """A bundle allows the reader everything; the endpoint is still hidden,
    because no access rule reads a row with no source record. The loaded
    record stays, so the hiding is the endpoint's alone."""
    from tests.substrate.mcp.test_search_list import _activate

    deployment = _published(clean_runtime, tmp_path)
    _activate(clean_runtime, deployment, [
        {"rule_id": "allow_all", "effect": "allow",
         "actions": ["graph.read.mcp", "mcp.tool.call"], "subjects": [READER],
         "priority": 100},
    ])
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", mode)
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", READER)
    served = _calls(clean_runtime, deployment)
    for shape in ("records", "links"):
        assert _far(served[shape]) == [LOADED], (mode, shape)
        assert served[shape]["link_targets"] == {
            "cited": None, "loaded": None, "not_loaded": None,
            "not_loaded_keys": [], "basis": "not_counted",
        }, (mode, shape)
        assert "CMSTRANSF-452" not in json.dumps(served[shape]), (mode, shape)
        assert "CMSTRANSF-452" not in served[f"{shape}_text"], (mode, shape)
        assert "not loaded" not in served[f"{shape}_text"], (mode, shape)
    assert "load_state" not in json.dumps(served["inspect"]), mode
    assert "known by key" not in served["inspect_text"], mode
    # Review M2b: no sample value names the endpoint's key.
    assert "CMSTRANSF-452" not in json.dumps(served["subtype"]), mode
    assert MISSING not in _search_ids(served["search"]), mode
