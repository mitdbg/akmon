"""A fixture can declare a node every source shares (#3591).

The devops starter's github and jira fixtures both emit ``person:alice``. As
two authoritative copies, the source that committed first failed its own
record-set coverage check whenever the other one wrote the row after it
(about 1 run in 6). The fixtures now declare people ``substitute``, the
shared-node contract in docs/develop/architecture/pipeline.md, and both
fixture readers carry that declaration into the facts and the record set the
coverage check reads.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.github import GitHubSource
from okg.substrate.library.sources.jira import JiraSource

DEVOPS_FIXTURES = (
    Path(__file__).resolve().parents[4]
    / "src/okg/substrate/library/templates/devops/fixtures"
)


def _modes(run) -> tuple[dict[str, str], dict[str, str]]:
    facts = {
        fact.node_id: fact.ownership_mode
        for fact in run.facts
        if isinstance(fact, NodeFact)
    }
    declared = {
        emission.primary.node_id: emission.primary.ownership_mode
        for emission in run.record_set.records.values()
        if emission.primary is not None
    }
    return facts, declared


@pytest.mark.parametrize(
    "adapter",
    [
        lambda: JiraSource(fixture_path=DEVOPS_FIXTURES / "jira.json"),
        lambda: GitHubSource(fixture_path=DEVOPS_FIXTURES / "github.json"),
    ],
    ids=["jira", "github"],
)
def test_the_devops_fixtures_declare_the_shared_person_substitute(adapter) -> None:
    facts, declared = _modes(adapter().run("run-1", mode="reconcile"))
    assert facts["person:alice"] == declared["person:alice"] == "substitute"
    owned = {node for node, mode in facts.items() if not node.startswith("person:")}
    assert owned and all(facts[node] == declared[node] == "authoritative" for node in owned)


def test_every_node_the_devops_fixtures_share_is_substitute() -> None:
    seen: dict[str, list[dict]] = {}
    for path in sorted(DEVOPS_FIXTURES.glob("*.json")):
        for node in json.loads(path.read_text(encoding="utf-8"))["nodes"]:
            seen.setdefault(node["node_id"], []).append(node)
    shared = {node_id: rows for node_id, rows in seen.items() if len(rows) > 1}
    assert shared, "the devops starter no longer shares a node; drop this test"
    for node_id, rows in shared.items():
        assert {row.get("ownership_mode") for row in rows} == {"substitute"}, node_id


@pytest.mark.parametrize("make", [JiraSource, GitHubSource], ids=["jira", "github"])
def test_a_fixture_node_without_a_mode_stays_authoritative(make, tmp_path: Path) -> None:
    fixture = tmp_path / "fixture.json"
    fixture.write_text(
        json.dumps({"nodes": [{"node_id": "n:1", "subtype": "doc", "attrs": {}}], "edges": []}),
        encoding="utf-8",
    )
    facts, declared = _modes(make(fixture_path=fixture).run("run-1", mode="reconcile"))
    assert facts == declared == {"n:1": "authoritative"}
