"""Regression test for `add-substrate-id-canonicalization`.

The SourceRunner rewrites raw `<family>:<repo>:<rest>` IDs
through the registered canonicalizer chain BEFORE facts hit
node_facts / edge_facts. Two adapters writing about the same
physical entity collapse on the canonical identity; the
durable log is canonical from the start.
"""
from __future__ import annotations


import psycopg
import pytest

from okg.substrate.canonicalize import RepoNamespaceCanonicalizer
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.base import (
    EdgeFact,
    NodeFact,
    SourceRun,
)
from tests.substrate._db_helpers import seed_catalog_version


pytestmark = pytest.mark.postgres



class _WorktreeAdapter:
    """Synthetic adapter that emits raw worktree-prefixed IDs:
    `file:foo:...` and `commit:foo:...` that the canonicalizer
    should rewrite to `:okg:...`."""

    name = "worktree-adapter"
    profile = "discovery_crawl"

    def run(self, run_id: str, *, mode: str = "cursor") -> SourceRun:
        facts: list = [
            NodeFact(
                node_id="file:foo:src/x.py",
                subtype="source_file",
                attrs={"path": "src/x.py"},
                source_record_id={
                    "repo": "foo", "path": "src/x.py",
                },
                source_revision={"v": 1, "rev": run_id},
                op="I",
            ),
            NodeFact(
                node_id="commit:foo:" + ("a" * 40),
                subtype="git_commit",
                attrs={"sha": "a" * 40},
                source_record_id={
                    "repo": "foo", "sha": "a" * 40,
                },
                source_revision={"v": 1, "rev": run_id},
                op="I",
            ),
            EdgeFact(
                src="commit:foo:" + ("a" * 40),
                dst="file:foo:src/x.py",
                edge_type="touches",
                attrs={},
                source_record_id={"repo": "foo"},
                source_revision={"v": 1, "rev": run_id},
                op="I",
            ),
        ]
        return SourceRun(
            facts=facts, completed_scope=True,
            next_cursor=None, run_mode=mode,
        )


def test_canonicalizer_rewrites_node_facts_at_runner(
    clean_runtime: str,
) -> None:
    """SourceRunner with a `foo → okg` canonicalizer rewrites
    every raw worktree-prefixed ID before COPY."""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    canonicalizers = RepoNamespaceCanonicalizer.chain_from_aliases(
        {"foo": "okg"},
    )
    SourceRunner(
        clean_runtime, source_name="canon-test",
        canonicalizers=canonicalizers,
    ).run(_WorktreeAdapter(), mode="scope_complete")

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        node_ids = [
            r["node_id"]
            for r in store.query(
                """
                SELECT node_id
                  FROM okg.graph_nodes
                 WHERE source = 'canon-test'
                 ORDER BY node_id
                """
            )
        ]
        edge_endpoints = [
            (r["src"], r["dst"])
            for r in store.query(
                """
                SELECT src, dst
                  FROM okg.graph_edges
                 WHERE source = 'canon-test'
                 ORDER BY src, dst
                """
            )
        ]
    finally:
        store.close()
    expected_nodes = sorted([
        "file:okg:src/x.py",
        "commit:okg:" + ("a" * 40),
    ])
    assert node_ids == expected_nodes, node_ids
    assert edge_endpoints == [
        ("commit:okg:" + ("a" * 40), "file:okg:src/x.py"),
    ], edge_endpoints


def test_canonicalizer_no_op_without_registration(
    clean_runtime: str,
) -> None:
    """SourceRunner with NO canonicalizers leaves raw IDs in
    place — the legacy behavior. (Existing deployments don't
    auto-canonicalize until they register a chain.)"""
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    SourceRunner(
        clean_runtime, source_name="canon-noop",
    ).run(_WorktreeAdapter(), mode="scope_complete")

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        node_ids = [
            r["node_id"]
            for r in store.query(
                """
                SELECT node_id
                  FROM okg.graph_nodes
                 WHERE source = 'canon-noop'
                 ORDER BY node_id
                """
            )
        ]
    finally:
        store.close()
    # Raw IDs preserved.
    assert any("file:foo:" in nid for nid in node_ids), node_ids
    assert any("commit:foo:" in nid for nid in node_ids), node_ids


def test_canonicalizer_collapses_two_adapters_to_one_node(
    clean_runtime: str,
) -> None:
    """Two adapters emit the same file under different repos
    (`file:foo:x` and `file:okg:x`); after canonicalization
    both collapse to `file:okg:x` and the runner-level dedup
    consolidates them."""

    class _Adapter1:
        name = "adapter-foo"
        profile = "discovery_crawl"

        def run(self, run_id, *, mode="cursor"):
            return SourceRun(
                facts=[
                    NodeFact(
                        node_id="file:foo:src/y.py",
                        subtype="source_file",
                        attrs={"path": "src/y.py"},
                        source_record_id={"repo": "foo"},
                        source_revision={"rev": run_id},
                        op="I",
                    ),
                ],
                completed_scope=True,
                next_cursor=None, run_mode=mode,
            )

    class _Adapter2:
        name = "adapter-okg"
        profile = "discovery_crawl"

        def run(self, run_id, *, mode="cursor"):
            return SourceRun(
                facts=[
                    NodeFact(
                        node_id="file:okg:src/y.py",
                        subtype="source_file",
                        attrs={"path": "src/y.py"},
                        source_record_id={"repo": "okg"},
                        source_revision={"rev": run_id},
                        op="I",
                    ),
                ],
                completed_scope=True,
                next_cursor=None, run_mode=mode,
            )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        seed_catalog_version(conn)

    canonicalizers = RepoNamespaceCanonicalizer.chain_from_aliases(
        {"foo": "okg"},
    )
    SourceRunner(
        clean_runtime, source_name="ad-foo",
        canonicalizers=canonicalizers,
    ).run(_Adapter1(), mode="scope_complete")
    SourceRunner(
        clean_runtime, source_name="ad-okg",
        canonicalizers=canonicalizers,
    ).run(_Adapter2(), mode="scope_complete")

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        node_ids = sorted(
            r["node_id"]
            for r in store.query(
                """
                SELECT DISTINCT node_id
                  FROM okg.graph_nodes
                 WHERE source LIKE 'ad-%'
                """
            )
        )
    finally:
        store.close()
    assert node_ids == ["file:okg:src/y.py"], node_ids
