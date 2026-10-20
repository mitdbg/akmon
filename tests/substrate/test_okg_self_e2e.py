"""End-to-end harness for `debug-substrate-okg-self` Phase 0.

Wires together:

  1. fresh DB (Postgres bootstrap + migrate via `clean_runtime`);
  2. okg-workspace deployment catalog applied via
     `apply_composed_catalog` (Phase 6);
  3. all four substrate adapters run against the
     `okg_self_micro` fixture corpus;
  4. `Publisher.cycle()` → published generation;
  5. MCP smoke — every registered tool answers without raising.

The harness is the acceptance gate referenced by every later
phase. Skipped (via the postgres mark) when `OKG_TEST_DSN` is
unset.

Tradeoffs versus a "real" deployment run:

  - Catalog points at the okg-workspace deployment dir on disk —
    its modules + bridges are the same shape every run.
  - Adapters are instantiated directly with fixture paths
    rather than via the source_registry.yaml, so the test
    doesn't depend on registry-loader internals.
  - `compute_env_local` adapter is intentionally skipped;
    it probes the real machine (`uname` / `~/.claude`) and
    isn't fixture-driven. Its substrate Protocol contract is
    covered separately by `library/sources/test_compute_env`.
"""
from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import psycopg
import pytest
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


_FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"
if str(_FIXTURES_DIR) not in sys.path:
    sys.path.insert(0, str(_FIXTURES_DIR))


pytestmark = pytest.mark.postgres


def _projects_root(fixture_paths) -> Path:
    return fixture_paths.sessions_root.parent


@pytest.fixture
def fixture_paths(tmp_path):
    from okg_self_micro import materialize  # type: ignore

    return materialize(tmp_path)


def _okg_self_deployment_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "deployments" / "okg-workspace"


# ---------- adapter exec ----------


def _dsn_with_active_cv_bypass(dsn: str) -> str:
    """Pre-`defer-narrowing-trigger-until-publish` (2026-04-25)
    this prepended `options=-c okg.active_cv=-1` to bypass the
    narrowing trigger during ingest. After the trigger moved
    from `edge_facts` to `edges_live` and the publisher
    reordered to project nodes_live before edges_live, no bypass
    is needed: SourceRunner can append narrowing-active edges
    freely, the strict check fires at projection commit. This
    function now returns the DSN unchanged; kept as a no-op
    seam in case future deployments need a writer-side override."""
    return dsn


def _run_substrate_adapters(dsn: str, fixture_paths) -> None:
    """Drive each substrate adapter via SourceRunner against the
    fixture. compute_env_local is skipped (probes real machine);
    the rest mirror the deployment's source_registry."""
    from okg.substrate.ingest import SourceRunner
    from okg.substrate.library.sources.agent_sessions import (
        AgentSessionsSource,
    )
    from okg.substrate.library.sources.compute_env_remote_hosts import (
        RemoteHostsFromSessionsSource,
    )
    from okg.substrate.library.sources.doc_corpus import (
        DocCorpusSource,
    )
    from okg.substrate.library.sources.git_graph_files import (
        GitFilesSource,
    )
    from okg.substrate.library.sources.git_graph_history import (
        GitHistorySource,
    )
    from okg.substrate.library.sources.openspec import OpenSpecSource

    # The doc-corpus adapter wants an `extractors.yaml`. Write
    # a minimal one into a tmp dir adjacent to the fixture
    # so the e2e exercises the doc-corpus path without the
    # real okg-workspace deployment dir's heavyweight chain (PDF /
    # html parsers, etc.).
    extractors_dir = fixture_paths.repo_root.parent / "extractors-cfg"
    extractors_dir.mkdir(exist_ok=True)
    (extractors_dir / "extractors.yaml").write_text(
        "parser:\n"
        "  - format: markdown\n"
        "    class: okg.substrate.library.extraction.parse.markdown.MarkdownParser\n"
        "  - format: text\n"
        "    class: okg.substrate.library.extraction.parse.text.TextParser\n"
        "  - format: code\n"
        "    class: okg.substrate.library.extraction.parse.code_python.PythonAstParser\n"
        "chunker:\n"
        "  - class: okg.substrate.library.extraction.chunk.window.WindowChunker\n"
        "    config: {max_chars: 1500, overlap: 200}\n"
        "ner:\n"
        "  - class: okg.substrate.library.extraction.ner.regex_backend.RegexEntityExtractor\n"
        "    config:\n"
        "      rules:\n"
        '        - [arxiv_id, "arXiv:[0-9]{4}\\\\.[0-9]{4,5}"]\n'
    )

    # Order matters: git-files first so doc-corpus's
    # `predecessor_source` check passes.
    ordered = [
        ("okg-workspace.git-files", GitFilesSource(
            repo="okg", repo_root=fixture_paths.repo_root,
        )),
        ("okg-workspace.git-history", GitHistorySource(
            repo="okg", repo_root=fixture_paths.repo_root,
        )),
        ("okg-workspace.openspec", OpenSpecSource(
            repo="okg",
            openspec_root=fixture_paths.openspec_root,
        )),
        ("okg-workspace.agent-sessions", AgentSessionsSource(
            repo="okg",
            claude_projects_root=_projects_root(fixture_paths),
            since="all", cost_estimation=False,
        )),
        ("okg-workspace.remote-hosts", RemoteHostsFromSessionsSource(
            repo="okg",
            claude_projects_root=_projects_root(fixture_paths),
        )),
        ("okg-workspace.doc-corpus", DocCorpusSource(
            repo="okg",
            repo_root=fixture_paths.repo_root,
            extractors_dir=extractors_dir,
            include_globs=("**/*.md", "**/*.txt", "**/*.py"),
            exclude_globs=("**/__pycache__/**",),
            max_files=2000,
            max_bytes_per_file=2 * 1024 * 1024,
            predecessor_source="okg-workspace.git-files",
            dsn=dsn,
        )),
    ]
    for name, adapter in ordered:
        SourceRunner(dsn, source_name=name).run(
            adapter, mode="scope_complete",
        )


# ---------- MCP smoke ----------


async def _call_tool(mcp, tool_name: str, args: dict) -> str:
    res = await mcp.call_tool(tool_name, args)
    # Under mcp 2 a refused call is a `CallToolResult` whose envelope lives in
    # `structuredContent` with `isError` set and no text block; joining text
    # blocks turned every refusal into an anonymous "empty payload".
    if getattr(res, "isError", False):
        raise AssertionError(
            f"MCP tool {tool_name!r} refused: {res.content!r}"
        )
    structured = getattr(res, "structuredContent", None)
    if isinstance(structured, dict):
        return json.dumps(structured)
    if isinstance(res, tuple):
        contents = res[0]
    else:
        contents = (
            res.content if hasattr(res, "content") else res
        )
    parts: list[str] = []
    for c in contents:
        if hasattr(c, "text"):
            parts.append(c.text)
    return "; ".join(parts)


def _mcp_smoke(dsn: str) -> dict[str, int]:
    """Call every MCP tool with conservative valid args. Returns
    response sizes by tool. Raises on the first tool that
    raises."""
    from okg.substrate.mcp.server import build_server
    from okg.substrate.graph_store import ChronosGraphStore

    mcp = build_server(
        dsn=dsn,
        deployment_name="okg-workspace",
    )

    # Pull a real node + a real edge to seed args for the
    # node-shaped tools.
    store = ChronosGraphStore.connect(dsn)
    try:
        generation = store.latest_generation(
            deployment_name="okg-workspace",
            branch="default",
        )
        assert generation is not None, "expected a Chronos generation"
        checkout = store.checkout_generation(generation.checkpoint_id)
        rows = checkout.query(
            "SELECT node_id, subtype FROM okg.graph_nodes LIMIT 1"
        )
        assert rows, "expected at least one graph node post-cycle"
        sample_node_id = rows[0]["node_id"]
    finally:
        store.close()

    # Migrated to canonical AKMON operator surface per
    # shrink-mcp-to-operator-surface.
    calls = [
        ("inspect", {"target": {"kind": "graph"}}),
        # The micro fixture's ontology is about 110 KB, over the 32 KB
        # default. Before issue #3059 this call came back as a returned
        # refusal with isError=false, which this smoke counted as a
        # non-empty answer; it is an error now, so ask for the ceiling.
        ("inspect", {"target": {"kind": "ontology"}, "max_bytes": 131072}),
        ("inspect", {"target": {"kind": "node", "node_id": sample_node_id}}),
        ("expand", {"input": {"node_id": sample_node_id}}),
        (
            "aggregate",
            {
                "input": {"node_id": sample_node_id},
                "by": ["edge_type", "dst_subtype"],
                "metrics": ["count"],
            },
        ),
        (
            "search",
            {
                "query": "see arXiv:1234.5678 here",
                "method": "identifier_mentions",
            },
        ),
    ]
    sizes: dict[str, int] = {}

    async def _drive() -> dict[str, int]:
        out: dict[str, int] = {}
        for name, args in calls:
            payload = await _call_tool(mcp, name, args)
            out[name] = len(payload)
        # #3151 slice 1: this fixture registers no aliases, so the server
        # publishes no `alias` method and refuses one by name, with the probe's
        # reason, before any query. It used to return an empty page.
        alias = await mcp.call_tool("search", {"query": "okg", "method": "alias"})
        assert alias.isError is True
        assert "search_method_unavailable" in alias.content[0].text
        assert "no aliases are registered" in alias.content[0].text
        return out

    sizes = asyncio.run(_drive())
    return sizes


# ---------- the harness ----------


def test_okg_self_e2e_bootstrap_then_cycle_then_mcp(
    okg_workspace_isolated_runtime: str, fixture_paths,
) -> None:
    """Bootstrap → catalog apply → adapters → cycle → MCP smoke.

    Runs against an isolated clone of okg-workspace's DB under a permissive
    Nomos posture (see ``okg_workspace_isolated_runtime``): the real
    okg-workspace deployment is self-hosted-authority + nomos-enforcing, so a
    generic test DB is refused as an identity mismatch and a fresh DB
    default-denies for lack of an active policy bundle.

    The end state must be:
      - one published Chronos generation checkpoint;
      - Chronos graph nodes and edges are non-empty (the fixture's
        git history + openspec corpus produces both);
      - every MCP tool returns a non-empty JSON payload.
    """
    clean_runtime = okg_workspace_isolated_runtime
    from okg.substrate.catalog.modules_apply import (
        apply_composed_catalog,
    )
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.publisher import Publisher

    deployment_dir = _okg_self_deployment_dir()
    composed = compose_catalog(deployment_dir)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        cv = apply_composed_catalog(conn, composed)
    assert cv > 0

    _run_substrate_adapters(
        _dsn_with_active_cv_bypass(clean_runtime), fixture_paths,
    )

    # Per `wire-deployment-enrichers` (2026-04-26) and
    # `declarative-cross-source-linker` (2026-04-26): the
    # deployment registers a single DeclarativeLinker that
    # auto-loads substrate-shipped cross-link rules. The
    # fixture's commit messages don't follow the
    # `<change_id>:` prefix convention used in the real
    # repo, so per-fixture HIT count varies; per-matcher
    # unit tests assert match counts on controlled corpora.
    from okg.substrate.publisher.enricher_loader import (
        load_enrichers,
    )
    enrichers = load_enrichers(_okg_self_deployment_dir())
    assert len(enrichers) >= 1, (
        "deployments/okg-workspace/deployment.yaml's `enrichers:` "
        "block should produce ≥ 1 instance"
    )
    out = Publisher(
        clean_runtime, "okg-workspace", enrichers=enrichers,
        catalog_identity=bind_test_catalog_writer(clean_runtime, "okg-workspace"),
    ).cycle()
    assert out["status"] == "published", out

    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        generation = store.latest_generation(
            deployment_name="okg-workspace",
            branch="default",
        )
        assert generation is not None, "expected a Chronos generation"
        checkout = store.checkout_generation(generation.checkpoint_id)
        n_nodes = checkout.query(
            "SELECT count(*) AS n FROM okg.graph_nodes"
        )[0]["n"]
        n_edges = checkout.query(
            "SELECT count(*) AS n FROM okg.graph_edges"
        )[0]["n"]
    finally:
        store.close()
    assert n_nodes > 0, "expected at least one live node"
    assert n_edges > 0, "expected at least one live edge"

    # `add-doc-corpus-extraction-adapter` (2026-04-25):
    # the doc-corpus adapter ran above; its emit must surface
    # in the pinned Chronos generation as document + document_chunk rows. The
    # micro fixture has tens of markdown / python files;
    # expect ≥ 5 documents and ≥ 5 chunks (small corpus).
    store = ChronosGraphStore.connect(clean_runtime)
    try:
        generation = store.latest_generation(
            deployment_name="okg-workspace",
            branch="default",
        )
        assert generation is not None, "expected a Chronos generation"
        checkout = store.checkout_generation(generation.checkpoint_id)
        n_documents = checkout.query(
            "SELECT count(*) AS n FROM okg.graph_nodes "
            " WHERE subtype='document'"
        )[0]["n"]
        n_chunks = checkout.query(
            "SELECT count(*) AS n FROM okg.graph_nodes "
            " WHERE subtype='document_chunk'"
        )[0]["n"]
        n_chunks_with_text = checkout.query(
            "SELECT count(*) AS n FROM okg.graph_nodes "
            " WHERE subtype='document_chunk' "
            "   AND attrs->>'text' IS NOT NULL"
        )[0]["n"]
    finally:
        store.close()
    # Micro fixture is small (3-5 docs); assertion is shape,
    # not magnitude. The full okg-workspace deployment shows the
    # operator-impact numbers at scale (~600 docs, ~3,500
    # chunks) per proposal.md.
    assert n_documents >= 1, n_documents
    assert n_chunks >= 1, n_chunks
    assert n_chunks_with_text == n_chunks, (
        f"expected every chunk to carry attrs.text; "
        f"got {n_chunks_with_text}/{n_chunks}"
    )

    sizes = _mcp_smoke(clean_runtime)
    # Every smoked tool returned a non-empty payload.
    for name, size in sizes.items():
        assert size > 0, f"MCP tool {name!r} returned empty payload"


def test_okg_self_e2e_two_cycles_no_duplicate_rows(
    okg_workspace_isolated_runtime: str, fixture_paths,
) -> None:
    """Run two consecutive Publisher.cycle() against the same
    fixture (no new facts between cycles). Locks in the
    Chronos-native equivalent of the old projector duplicate-row
    regression: every node_id and edge_id has exactly one row in
    the branch head after the second cycle.

    Runs against an isolated okg-workspace clone under a permissive Nomos
    posture (see ``okg_workspace_isolated_runtime``)."""
    clean_runtime = okg_workspace_isolated_runtime
    from okg.substrate.catalog.modules_apply import (
        apply_composed_catalog,
    )
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.publisher import Publisher

    composed = compose_catalog(_okg_self_deployment_dir())
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        apply_composed_catalog(conn, composed)
    _run_substrate_adapters(
        _dsn_with_active_cv_bypass(clean_runtime), fixture_paths,
    )

    pub = Publisher(clean_runtime, "okg-workspace", catalog_identity=bind_test_catalog_writer(clean_runtime, "okg-workspace"))
    out1 = pub.cycle()
    assert out1["status"] == "published", out1
    out2 = pub.cycle()
    # The second cycle may publish/block, or short-circuit through
    # the explicit no-op path when there are no new facts. What's NOT
    # acceptable is leaving duplicate open rows.
    assert out2["status"] in ("published", "blocked", "no_op"), out2

    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(clean_runtime)
    try:
        checkout = store.checkout("default")
        dups_n = checkout.query(
            "SELECT node_id, count(*) AS n FROM okg.graph_nodes "
            " GROUP BY node_id HAVING count(*) > 1"
        )
        dups_e = checkout.query(
            "SELECT edge_id, count(*) AS n FROM okg.graph_edges "
            " GROUP BY edge_id HAVING count(*) > 1"
        )
    finally:
        store.close()
    assert dups_n == [], (
        f"e2e: projector left {len(dups_n)} duplicate-open "
        f"node_ids: {dups_n[:5]}"
    )
    assert dups_e == [], (
        f"e2e: projector left {len(dups_e)} duplicate-open "
        f"edge_ids: {dups_e[:5]}"
    )
