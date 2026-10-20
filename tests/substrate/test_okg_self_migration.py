"""Thesis test for `migrate-okg-self-to-substrate` (Phase 8.1).

Asserts graph-shape parity between the source-domain emitters and the
substrate-library adapters when each is exercised against the
committed `okg_self_micro` fixture corpus:

  - subtype-existence parity: every subtype the domain emit
    produces also surfaces on the substrate side (count comparison
    is intentionally NOT enforced — the two paths handle revisions
    differently and the proposal commits only to existence parity).
  - edge-type-existence parity: every edge_type the domain emits
    also surfaces on the substrate side.
  - critical-node-id parity: the hand-curated fixture pin set
    surfaces on the substrate side.
  - at least one bridge edge resolves: the deployment-side bridge
    narrowing connects a module subtype to a deployment subtype on
    the substrate's composed catalog.

Five adapter modules under test (Phases 2-5):

  - GitFilesSource
  - GitHistorySource
  - OpenSpecSource
  - AgentSessionsSource
  - LocalEnvSource (skipped — env-dependent; covered by Phase 5)
  - RemoteHostsFromSessionsSource

The test does NOT require Postgres. It exercises the substrate
adapters' run() Protocol contract and the catalog composer; the
DDL apply path lives in the existing reconcile flow and is out of
scope for this thesis.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest


_FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
if str(_FIXTURES_DIR) not in sys.path:
    sys.path.insert(0, str(_FIXTURES_DIR))


@pytest.fixture
def fixture_paths(tmp_path):
    from okg_self_micro import materialize  # type: ignore

    return materialize(tmp_path)


def _projects_root(fixture_paths) -> Path:
    return fixture_paths.sessions_root.parent


# ---------- domain + substrate emit collectors ----------


def _collect_domain_emit(fixture_paths) -> tuple[set[str], set[tuple[str, str, str]]]:
    """Run every domain emitter against the fixture; return the
    union of (node_id, subtype) pairs and (src, dst, edge_type)
    triples it emits."""
    from okg.substrate.library.sources._agent_sessions_emit import (
        AgentSessionSource as DomainAgentSessionSource,
    )
    from okg.substrate.library.sources._compute_env_remote_hosts_emit import (
        RemoteHostsFromSessions as DomainRemoteHostsFromSessions,
    )
    from okg.substrate.library.sources._git_graph_files_emit import (
        GitFilesSource as DomainGitFilesSource,
    )
    from okg.substrate.library.sources._git_graph_history_emit import (
        GitHistorySource as DomainGitHistorySource,
    )
    from okg.substrate.library.sources._openspec_emit import (
        OpenSpecSource as DomainOpenSpecSource,
    )

    domain_sources = [
        DomainGitFilesSource(
            repo_root=fixture_paths.repo_root,
            repo="okg",
        ),
        DomainGitHistorySource(
            repo_root=fixture_paths.repo_root,
            repo="okg",
        ),
        DomainOpenSpecSource(
            openspec_root=fixture_paths.openspec_root,
            repo="okg",
        ),
        DomainAgentSessionSource(
            claude_projects_root=_projects_root(fixture_paths),
            since="all",
            cost_estimation=False,
        ),
        DomainRemoteHostsFromSessions(
            claude_projects_root=_projects_root(fixture_paths),
        ),
    ]

    nodes: set[tuple[str, str]] = set()
    edges: set[tuple[str, str, str]] = set()
    for src in domain_sources:
        for n in src.nodes():
            nodes.add((n.node_id, n.subtype))
        for e in src.edges():
            edges.add((e.src, e.dst, e.edge_type))
    return nodes, edges


def _collect_substrate_emit(
    fixture_paths,
) -> tuple[set[tuple[str, str]], set[tuple[str, str, str]]]:
    """Run every substrate adapter against the same fixture in
    cursor mode; return the union of (node_id, subtype) and
    (src, dst, edge_type) entries it emits with op='I'."""
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.library.sources.base import EdgeFact, NodeFact
    from okg.substrate.library.sources.compute_env_remote_hosts import (
        RemoteHostsFromSessionsSource,
    )
    from okg.substrate.library.sources.git_graph_files import GitFilesSource
    from okg.substrate.library.sources.git_graph_history import (
        GitHistorySource,
    )
    from okg.substrate.library.sources.openspec import OpenSpecSource

    substrate_sources = [
        GitFilesSource(repo="okg", repo_root=fixture_paths.repo_root),
        GitHistorySource(repo="okg", repo_root=fixture_paths.repo_root),
        OpenSpecSource(repo="okg", openspec_root=fixture_paths.openspec_root),
        AgentSessionsSource(
            repo="okg",
            claude_projects_root=_projects_root(fixture_paths),
            since="all",
            cost_estimation=False,
        ),
        RemoteHostsFromSessionsSource(
            repo="okg",
            claude_projects_root=_projects_root(fixture_paths),
        ),
    ]

    nodes: set[tuple[str, str]] = set()
    edges: set[tuple[str, str, str]] = set()
    for i, src in enumerate(substrate_sources):
        run = src.run(f"thesis-{i}", mode="cursor")
        for fact in run.facts:
            if isinstance(fact, NodeFact) and fact.op == "I":
                nodes.add((fact.node_id, fact.subtype))
            elif isinstance(fact, EdgeFact) and fact.op == "I":
                edges.add((fact.src, fact.dst, fact.edge_type))
    return nodes, edges


# ---------- thesis assertions ----------


def test_subtype_existence_parity(fixture_paths) -> None:
    """Every subtype domain emits also surfaces on substrate.

    The proposal explicitly does NOT assert count parity (domain
    and substrate handle revisions / dedup / op='R' differently);
    only existence parity is the thesis contract.
    """
    domain_nodes, _ = _collect_domain_emit(fixture_paths)
    substrate_nodes, _ = _collect_substrate_emit(fixture_paths)

    domain_subtypes = {subtype for _, subtype in domain_nodes}
    substrate_subtypes = {subtype for _, subtype in substrate_nodes}

    missing = domain_subtypes - substrate_subtypes
    assert not missing, (
        f"subtype existence parity broken — domain emits "
        f"{sorted(missing)} but substrate does not"
    )


def test_edge_type_existence_parity(fixture_paths) -> None:
    """Every edge_type domain emits also surfaces on substrate."""
    _, domain_edges = _collect_domain_emit(fixture_paths)
    _, substrate_edges = _collect_substrate_emit(fixture_paths)

    domain_edge_types = {et for _, _, et in domain_edges}
    substrate_edge_types = {et for _, _, et in substrate_edges}

    missing = domain_edge_types - substrate_edge_types
    assert not missing, (
        f"edge_type existence parity broken — domain emits "
        f"{sorted(missing)} but substrate does not"
    )


def test_critical_node_id_parity(fixture_paths) -> None:
    """Every fixture-pinned node id surfaces on the substrate
    side. The hand-list lives in the fixture loader."""
    from okg_self_micro import CRITICAL_NODE_IDS  # type: ignore

    substrate_nodes, _ = _collect_substrate_emit(fixture_paths)
    substrate_ids = {nid for nid, _ in substrate_nodes}

    missing = set(CRITICAL_NODE_IDS) - substrate_ids
    assert not missing, (
        f"critical-id parity broken — pinned ids absent on substrate: {sorted(missing)}"
    )


def test_substrate_node_set_is_a_superset_of_domain(fixture_paths) -> None:
    """A stricter contract: every (node_id, subtype) pair the
    domain emits also appears on the substrate side. The reverse
    is NOT required — substrate may emit additional rows."""
    domain_nodes, _ = _collect_domain_emit(fixture_paths)
    substrate_nodes, _ = _collect_substrate_emit(fixture_paths)

    missing = domain_nodes - substrate_nodes
    assert not missing, (
        f"substrate is missing domain (node_id, subtype) pairs: "
        f"{sorted(missing)[:5]}{'...' if len(missing) > 5 else ''}"
    )


def test_substrate_edge_set_is_a_superset_of_domain(fixture_paths) -> None:
    """Same contract as the node side, on edges."""
    _, domain_edges = _collect_domain_emit(fixture_paths)
    _, substrate_edges = _collect_substrate_emit(fixture_paths)

    missing = domain_edges - substrate_edges
    assert not missing, (
        f"substrate is missing domain edges: "
        f"{sorted(missing)[:5]}{'...' if len(missing) > 5 else ''}"
    )


def test_at_least_one_bridge_narrowing_resolves_on_substrate(
    fixture_paths,
    tmp_path,
) -> None:
    """Compose the okg-workspace deployment's catalog. Assert the
    deployment-side bridge contributes ≥1 narrowing whose
    parent + child both resolve to known module-emitted subtypes.

    This is the cross-layer contract: bridges narrow module
    subtypes to deployment subtypes, and the substrate's catalog
    composer must accept and validate them."""
    from okg.substrate.catalog.modules_compose import compose_catalog

    repo_root = Path(__file__).resolve().parents[2]
    deployment_dir = repo_root / "deployments" / "okg-workspace"
    if not (deployment_dir / "deployment.yaml").is_file():
        pytest.skip("okg-workspace deployment not present at this commit")

    composed = compose_catalog(deployment_dir)
    bridge_rows = [r for r in composed.narrowings if r.origin.startswith("bridge:")]
    assert bridge_rows, (
        "thesis: deployment must declare at least one bridge "
        "narrowing — found zero in composed catalog"
    )
    # And every bridge narrowing's parent + child are in the
    # substrate's composed subtype set (already validated by
    # compose_catalog raising BridgeSubtypeUnknown otherwise; this
    # just makes the contract explicit at the test level).
    for row in bridge_rows:
        assert row.parent_subtype in composed.subtypes, (
            f"bridge {row.origin}: parent {row.parent_subtype!r} unresolved"
        )
        assert row.child_subtype in composed.subtypes, (
            f"bridge {row.origin}: child {row.child_subtype!r} unresolved"
        )
    triples = composed.narrowing_triples()
    assert ("view", "view_cites_node", "open_spec_change") in triples
    assert ("view", "view_cites_node", "work_order") in triples
    assert ("view", "view_cites_node", "review") in triples
    assert ("view", "view_cites_node", "review_packet") in triples
    assert ("view", "view_cites_node", "pact_change") in triples
    assert ("view", "view_cites_node", "agent_session") in triples
    assert ("view", "view_cites_node", "code_symbol") in triples
