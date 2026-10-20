"""M6.7.3 — end-to-end integration test.

No bootstrap shortcuts. This test exercises the full stack:

  1. `okg init --template <t>` scaffolds the template.
  2. Schema migration already applied via conftest; catalog is
     loaded by inserting a catalog_version (the deployment's
     real catalog_load flow would install subtypes + narrowings
     here; we use the library refs the template declares).
  3. `SourceRunner` consumes the template's fixture file and
     writes real facts to `okg.node_facts` / `okg.edge_facts`.
  4. `Publisher.cycle()` runs Phase A/B/C — projects nodes,
     projects new edges, runs gates, commits.
  5. MCP-level tools (`open_session` + `graph_stats` +
     `list_neighbors`) return data consistent with the fixture.

If any of these breaks, the substrate cannot support a real
deployment end-to-end. This test is the M6 acceptance gate.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import psycopg
import pytest

from okg.substrate.catalog.modules_apply import apply_composed_catalog
from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.docsite import DocsiteSource
from okg.substrate.mcp import (
    inspect_graph,
    expand_neighbors,
    open_session,
)
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def test_docs_corpus_template_end_to_end(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """Full stack: init → catalog load → SourceRunner → publish
    → MCP query. The `docs-corpus` template is the smallest
    library template that has a content-bearing fixture."""

    # 1. Scaffold the template via `okg init`.
    init = subprocess.run(
        [sys.executable, "-m", "okg.substrate.cli.main",
         "init", "--template", "docs-corpus",
         "--target", str(tmp_path / "work")],
        capture_output=True, text=True, timeout=30,
    )
    assert init.returncode == 0, init.stderr
    scaffold = tmp_path / "work" / "docs-corpus"
    assert (scaffold / "deployment.yaml").is_file()
    assert (scaffold / "source_registry.yaml").is_file()
    fixture_path = scaffold / "fixtures" / "docsite.json"
    assert fixture_path.is_file()

    # 2. Apply the template catalog so publish-time narrowing
    # checks see the same subtypes/edges as a real deployment.
    composed = compose_catalog(scaffold)
    with psycopg.connect(clean_runtime) as conn:
        apply_composed_catalog(conn, composed)

    # 3. Run the adapter via SourceRunner — real fact write path.
    adapter = DocsiteSource(fixture_path=str(fixture_path))
    runner = SourceRunner(clean_runtime, source_name="docs-corpus.docsite")
    result = runner.run(adapter, mode="scope_complete")

    # The template's docsite.json fixture: 2 nodes + 1 edge.
    fixture_doc = json.loads(fixture_path.read_text())
    assert result.nodes_appended == len(fixture_doc["nodes"])
    assert result.edges_appended == len(fixture_doc["edges"])

    # 4. Publisher.cycle() projects nodes + edges + runs gates +
    # commits. No bootstrap shortcut.
    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published", out
    assert out["phase_a_converged"] is True
    assert out["phase_b_blocked"] is False

    # 5. MCP query surface returns data consistent with the
    # fixture. Session pinning + graph_stats + list_neighbors
    # all exercised.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        stats = inspect_graph(conn, s)
        intro_neighbors = expand_neighbors(conn, s, "docs:intro")

    assert stats["node_count"] == 2
    assert stats["edge_count"] == 1
    # The fixture edge: docs:intro -- links_to --> docs:architecture
    neighbor_ids = [n["dst"] for n in intro_neighbors["neighbors"]]
    assert neighbor_ids == ["docs:architecture"]


def test_empty_template_end_to_end(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """The `empty` template exercises the init → migrate →
    publish path with NO sources. Verifies a deployment with
    zero facts still publishes a valid generation."""
    init = subprocess.run(
        [sys.executable, "-m", "okg.substrate.cli.main",
         "init", "--template", "empty",
         "--target", str(tmp_path / "work")],
        capture_output=True, text=True, timeout=30,
    )
    assert init.returncode == 0, init.stderr

    scaffold = tmp_path / "work" / "empty"
    composed = compose_catalog(scaffold)
    with psycopg.connect(clean_runtime) as conn:
        apply_composed_catalog(conn, composed)

    # No SourceRunner — the empty template declares no sources.
    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        stats = inspect_graph(conn, s)
    assert stats["node_count"] == 0
    assert stats["edge_count"] == 0


def test_devops_template_end_to_end(
    clean_runtime: str, tmp_path: Path,
) -> None:
    """Devops template has two sources (Jira + GitHub). Verifies
    two runners can coexist and their facts compose into one
    published generation."""
    init = subprocess.run(
        [sys.executable, "-m", "okg.substrate.cli.main",
         "init", "--template", "devops",
         "--target", str(tmp_path / "work")],
        capture_output=True, text=True, timeout=30,
    )
    assert init.returncode == 0, init.stderr
    scaffold = tmp_path / "work" / "devops"

    composed = compose_catalog(scaffold)
    with psycopg.connect(clean_runtime) as conn:
        apply_composed_catalog(conn, composed)

    # Run each source adapter.
    from okg.substrate.library.sources.jira import JiraSource
    from okg.substrate.library.sources.github import GitHubSource

    jira_adapter = JiraSource(
        fixture_path=str(scaffold / "fixtures" / "jira.json"),
    )
    gh_adapter = GitHubSource(
        fixture_path=str(scaffold / "fixtures" / "github.json"),
    )
    r_jira = SourceRunner(clean_runtime, source_name="devops.jira")
    r_gh = SourceRunner(clean_runtime, source_name="devops.github")
    r_jira.run(jira_adapter, mode="cursor")
    r_gh.run(gh_adapter, mode="cursor")

    out = Publisher(clean_runtime, "default", catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
    assert out["status"] == "published"

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        s = open_session(conn, deployment_name="default")
        stats = inspect_graph(conn, s)
    # Fixtures define: jira has 4 nodes + 2 edges; github has 3
    # nodes + 2 edges. person:alice appears in both → 6 distinct
    # nodes. 4 distinct edges.
    assert stats["node_count"] == 6
    assert stats["edge_count"] == 4
