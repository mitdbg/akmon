"""Plain `okg doctor` warns about passage parents the catalog leaves undeclared.

A chunk names its parent only when the parent's subtype declares
`okg_passage_rollup` for it; an edge from any other subtype is ignored and
search shows the chunk's parent as `declared_but_unlinked`. On the C4 comp-ops
graph that hid the parent of 98,661 of 150,781 chunks with no signal to the
deployment owner (operator decision jiraparent B, 2026-10-06).
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from okg.substrate.cli import doctor as doctor_cli
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from tests.pytest.catalog_fixture_builders import SubtypeSpec
from tests.substrate.mcp.search_fixture import make_search_deployment, publish_nodes

pytestmark = pytest.mark.postgres

_ROLLUP = {"edge_type": "contains", "child_subtype": "chunk"}
DOC = SubtypeSpec(
    "doc", metadata={"searchable_text_fields": ["text"], "passage_rollup": _ROLLUP},
)
TICKET_DECLARED = SubtypeSpec(
    "ticket",
    metadata={"searchable_text_fields": ["text"], "passage_rollup": _ROLLUP},
)
#: The comp-ops shape before the fix: tickets contain chunks, but only `doc`
#: declares the relation.
TICKET_UNDECLARED = SubtypeSpec("ticket", metadata={"searchable_text_fields": ["text"]})
CHUNK = SubtypeSpec("chunk", metadata={"searchable_text_fields": ["text"]})
#: An entity mention is `member_of` a chunk: an edge type no declaration uses
#: for chunks, so never a parent link.
MENTION = SubtypeSpec("mention", metadata={"searchable_text_fields": ["text"]})
NARROWINGS = [
    ("doc", "contains", "chunk"),
    ("ticket", "contains", "chunk"),
    ("mention", "member_of", "chunk"),
]


def _node(node_id: str, subtype: str) -> NodeFact:
    return NodeFact(
        node_id=node_id,
        subtype=subtype,
        attrs={"text": node_id},
        source_record_id={"id": node_id},
        source_revision={"v": 1},
    )


def _edge(src: str, edge_type: str, dst: str) -> EdgeFact:
    return EdgeFact(
        src=src,
        dst=dst,
        edge_type=edge_type,
        source_record_id={"id": f"{src}|{edge_type}|{dst}"},
        source_revision={"v": 1},
    )


def _publish(tmp_path: Path, dsn: str, *, ticket: SubtypeSpec) -> str:
    deployment = make_search_deployment(tmp_path, dsn=dsn)
    nodes = [
        _node("doc:a", "doc"),
        _node("chunk:a:0", "chunk"),
        _node("ticket:t1", "ticket"),
        _node("chunk:t1:0", "chunk"),
        _node("chunk:t1:1", "chunk"),
        _node("ticket:t2", "ticket"),
        _node("chunk:t2:0", "chunk"),
        _node("mention:m", "mention"),
    ]
    edges = [
        _edge("doc:a", "contains", "chunk:a:0"),
        _edge("ticket:t1", "contains", "chunk:t1:0"),
        _edge("ticket:t1", "contains", "chunk:t1:1"),
        _edge("ticket:t2", "contains", "chunk:t2:0"),
        # Two tickets reaching one chunk still count it once.
        _edge("ticket:t2", "contains", "chunk:t1:0"),
        # A chunk with a declared parent too: readers see `doc:a`, so it is
        # not unlinked, but it is counted apart.
        _edge("ticket:t2", "contains", "chunk:a:0"),
        _edge("mention:m", "member_of", "chunk:a:0"),
    ]
    publish_nodes(
        dsn,
        deployment,
        subtypes=[DOC, ticket, CHUNK, MENTION],
        narrowings=NARROWINGS,
        nodes=nodes,
        edges=edges,
    )
    # The publisher records the deployment under the name it was given, here
    # the deployment directory (as the search tests open their sessions).
    return deployment


def test_undeclared_parent_subtype_is_one_warning_with_its_count(
    clean_runtime: str, tmp_path: Path,
) -> None:
    deployment = _publish(tmp_path, clean_runtime, ticket=TICKET_UNDECLARED)
    report: dict = {"ok": True, "errors": []}

    result = doctor_cli._default_doctor_passage_parents(
        clean_runtime, deployment, report
    )

    assert result["ok"] is False
    assert result["undeclared"] == [
        {
            "parent_subtype": "ticket",
            "edge_type": "contains",
            "child_subtype": "chunk",
            "children": 3,
            "children_with_a_declared_parent": 1,
        }
    ]
    (warning,) = report["warnings"]
    assert warning["code"] == "passage_parent_undeclared"
    assert warning["children"] == 3
    assert "declared_but_unlinked" in warning["message"]
    assert "3 chunk nodes are reached only by" in warning["message"]
    assert "okg_passage_rollup" in warning["fix_hint"]
    assert "at most one passage relation" in warning["fix_hint"]
    assert "narrowing for ticket contains chunk" in warning["fix_hint"]
    # A warning, not an error: the graph is intact.
    assert report["ok"] is True and report["errors"] == []


def test_no_warning_when_every_parent_subtype_declares(
    clean_runtime: str, tmp_path: Path,
) -> None:
    deployment = _publish(tmp_path, clean_runtime, ticket=TICKET_DECLARED)
    report: dict = {"ok": True, "errors": []}

    result = doctor_cli._default_doctor_passage_parents(
        clean_runtime, deployment, report
    )

    assert result == {"ok": True, "declarations": 2, "undeclared": []}
    assert report.get("warnings", []) == []


def test_a_failing_probe_is_a_warning_never_a_clean_result(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.graph_store import ChronosGraphStore

    deployment = _publish(tmp_path, clean_runtime, ticket=TICKET_UNDECLARED)

    def _boom(self, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        raise RuntimeError("canceling statement due to statement timeout")

    monkeypatch.setattr(ChronosGraphStore, "query_generation_once", _boom)
    report: dict = {"ok": True, "errors": []}

    result = doctor_cli._default_doctor_passage_parents(
        clean_runtime, deployment, report
    )

    assert result["ok"] is False
    assert result["code"] == "passage_parent_probe_failed"
    assert [w["code"] for w in report["warnings"]] == ["passage_parent_probe_failed"]
    assert report["warnings"][0]["message"].startswith("RuntimeError: canceling statement")


def test_nothing_published_skips_and_says_so(clean_runtime: str) -> None:
    report: dict = {"ok": True, "errors": []}

    result = doctor_cli._default_doctor_passage_parents(
        clean_runtime, "never-published", report
    )

    assert result == {
        "ok": True,
        "skipped": True,
        "code": "passage_parents_skipped_nothing_published",
    }
    assert report.get("warnings", []) == []


def test_plain_doctor_reports_it_and_keeps_its_exit_status(
    clean_runtime: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The wiring: plain doctor runs the check for a deployment."""
    deployment = _publish(tmp_path, clean_runtime, ticket=TICKET_UNDECLARED)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(Path(deployment).parent))
    monkeypatch.setenv("OKG_DSN", clean_runtime)
    args = argparse.Namespace(deployment=deployment, dsn=clean_runtime, check=None, json=True)

    exit_with = doctor_cli._cmd_doctor(args, as_json=True)
    report = json.loads(capsys.readouterr().out)

    assert report["passage_parents"]["undeclared"][0]["children"] == 3
    codes = [w["code"] for w in report.get("warnings", [])]
    assert codes.count("passage_parent_undeclared") == 1

    # The same deployment with the check's warning removed has the same
    # verdict: the finding never turns the doctor red.
    monkeypatch.setattr(
        doctor_cli,
        "_default_doctor_passage_parents",
        lambda dsn, deployment, report: {"ok": True, "undeclared": []},
    )
    exit_without = doctor_cli._cmd_doctor(args, as_json=True)
    without = json.loads(capsys.readouterr().out)
    assert without["ok"] == report["ok"]
    assert exit_without == exit_with
