"""Concurrent sources that share a node identity must both pass coverage.

`author:<email>` deliberately omits the repo, so in a multi-repo deployment
every `git-history-<slug>` source declares the same author identities.
`ownership_mode` decides what the coverage proof then demands of the graph:
`authoritative` requires the row to be owned by THIS source, which only the
last concurrent writer can satisfy. Every sibling then reads its own author
nodes as missing and refuses to advance its watermark.

Seen on a 96-source CMS CompOps ingest: 4 of 15 `git-history-*` sources failed
`record_set coverage check failed`, a different subset each run, each passing
when run alone. Uncorrelated with repo size -- `archi` (2,085 commits) failed
while `WMCore` (18,122) passed -- which is the signature of a race, not a
limit. Because the completeness gate needs every selected source, 4 failures
out of 96 published nothing at all.

These tests drive real `GitHistorySource` emission into the graph rather than
synthetic record sets, so they pin the fix at the layer the bug actually
lives: what the adapter declares, not what the proof tolerates.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import psycopg
import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.library.sources.git_graph_history import GitHistorySource
from tests.substrate._db_helpers import seed_catalog_version
from tests.substrate._git_helpers import commit_repo

pytestmark = pytest.mark.postgres

SHARED_EMAIL = "shared.author@example.test"
AUTHOR_NODE_ID = f"author:{SHARED_EMAIL}"


def _history_run(tmp_path: Path, slug: str) -> Any:
    """A scope-complete history run over a fresh single-commit repo."""
    root = tmp_path / slug
    commit_repo(
        root, email=SHARED_EMAIL, name="Shared Author", filename=f"{slug}.txt"
    )
    run = GitHistorySource(repo=slug, repo_root=root).run(
        f"run-{slug}",
        mode="scope_complete",
    )
    # `facts` is a generator and the record set is read separately; realize it
    # once so both the write below and any filtering see the same emission.
    return list(run.facts), run.record_set


def _seed(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        seed_catalog_version(conn)


def _apply(dsn: str, source: str, facts: list[Any]) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            facts, source=source
        )


def test_sibling_history_sources_both_pass_coverage_on_a_shared_author(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """The regression: two repos, one shared author, both watermarks advance.

    `beta` writes second, so before the fix its authoritative write took the
    `author:` row from `alpha` and `alpha`'s coverage proof refused. Which
    source lost was whichever the scheduler ran last, which is why the failing
    subset varied per run.
    """
    alpha_facts, alpha_record_set = _history_run(tmp_path, "alpha")
    beta_facts, beta_record_set = _history_run(tmp_path, "beta")

    assert alpha_record_set is not None and beta_record_set is not None
    # Premise: the two emissions really do collide on exactly this identity.
    assert AUTHOR_NODE_ID in alpha_record_set.records
    assert AUTHOR_NODE_ID in beta_record_set.records

    _seed(clean_runtime)
    _apply(clean_runtime, "test.git-history-alpha", alpha_facts)
    _apply(clean_runtime, "test.git-history-beta", beta_facts)

    # Neither call may raise. The loser of the race is the one that matters,
    # but assert both so the test cannot pass by the write order flipping.
    SourceRunner(
        clean_runtime,
        source_name="test.git-history-alpha",
    )._verify_record_set_chronos_coverage(alpha_record_set)
    SourceRunner(
        clean_runtime,
        source_name="test.git-history-beta",
    )._verify_record_set_chronos_coverage(beta_record_set)


def test_coverage_still_refuses_when_the_shared_author_is_really_absent(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """Sharing an identity is not an excuse for the identity being gone.

    Relaxing ownership must not relax presence: a declared node that no source
    holds is still a refusal, and the payload still names it.
    """
    facts, record_set = _history_run(tmp_path, "alpha")
    assert record_set is not None

    # Withhold the author node from the graph while still DECLARING it. Its
    # incident `authored_by` edges go too: the write path rejects an edge whose
    # endpoint is absent, so leaving them in would fail the setup rather than
    # the assertion under test.
    def _touches_author(fact: Any) -> bool:
        if isinstance(fact, NodeFact):
            return fact.node_id == AUTHOR_NODE_ID
        return AUTHOR_NODE_ID in (
            getattr(fact, "src", None),
            getattr(fact, "dst", None),
        )

    withheld = [fact for fact in facts if not _touches_author(fact)]
    assert len(withheld) < len(facts)
    _seed(clean_runtime)
    _apply(clean_runtime, "test.git-history-alpha", withheld)

    with pytest.raises(SubstrateError, match="record_set coverage") as excinfo:
        SourceRunner(
            clean_runtime,
            source_name="test.git-history-alpha",
        )._verify_record_set_chronos_coverage(record_set)

    data = excinfo.value.data
    assert data["missing_node_count"] == 1
    assert data["missing_nodes_sample"] == [AUTHOR_NODE_ID]
    # No other source holds it, so the generic missing hint stands alone.
    assert data["missing_nodes_owned_by_other_source"] == []
    assert "under another source" not in excinfo.value.fix_hint
    assert "cursor reset" in excinfo.value.fix_hint


def _two_owner_fixture_run(tmp_path: Path, name: str, *, with_alice: bool = True) -> Any:
    """A reconcile run of a fixture source; every node authoritative."""
    import json

    from okg.substrate.library.sources.jira import JiraSource

    nodes = [{"node_id": f"{name}:1", "subtype": "doc", "attrs": {"text": name}}]
    if with_alice:
        nodes.append(
            {"node_id": "person:alice", "subtype": "person", "attrs": {"name": "alice"}}
        )
    path = tmp_path / f"{name}-{with_alice}.json"
    path.write_text(json.dumps({"nodes": nodes, "edges": []}), encoding="utf-8")
    run = JiraSource(name=name, fixture_path=path).run(f"run-{name}", mode="reconcile")
    return list(run.facts), run.record_set


def _assert_names_the_other_owner(error: SubstrateError) -> None:
    assert error.data["missing_nodes_sample"] == ["person:alice"]
    assert error.data["missing_nodes_owned_by_other_source"] == [
        {"node_id": "person:alice", "source": "test.second"}
    ]
    assert "person:alice (held by test.second)" in error.fix_hint
    assert "ownership_mode='substitute'" in error.fix_hint
    # The other owner is the whole refusal, so no cursor-reset advice.
    assert "Rerun the source with an explicit cursor reset" not in error.fix_hint


def test_a_node_two_sources_fully_own_is_refused_naming_the_other_owner(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """#3591: two sources that both declare one node authoritative.

    The last writer owns the row, so the other source's proof refuses,
    depending on run order. The refusal names the node, the source that
    holds it, and the fix: mark the node shared (`substitute`).
    """
    first_facts, first_record_set = _two_owner_fixture_run(tmp_path, "first")
    second_facts, _ = _two_owner_fixture_run(tmp_path, "second")
    _seed(clean_runtime)
    _apply(clean_runtime, "test.first", first_facts)
    _apply(clean_runtime, "test.second", second_facts)

    with pytest.raises(SubstrateError, match="record_set coverage") as excinfo:
        SourceRunner(
            clean_runtime,
            source_name="test.first",
        )._verify_record_set_chronos_coverage(first_record_set)
    _assert_names_the_other_owner(excinfo.value)


def test_the_delta_check_also_names_the_other_owner(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """The same refusal on the delta path: alice is newly declared by
    `test.first` and already held by `test.second`."""
    _, prior_record_set = _two_owner_fixture_run(tmp_path, "first", with_alice=False)
    first_facts, current_record_set = _two_owner_fixture_run(tmp_path, "first")
    second_facts, _ = _two_owner_fixture_run(tmp_path, "second")
    _seed(clean_runtime)
    _apply(clean_runtime, "test.first", first_facts)
    _apply(clean_runtime, "test.second", second_facts)

    with pytest.raises(SubstrateError, match="record_set delta coverage") as excinfo:
        SourceRunner(
            clean_runtime,
            source_name="test.first",
        )._verify_record_set_chronos_delta(prior_record_set, current_record_set)
    _assert_names_the_other_owner(excinfo.value)
