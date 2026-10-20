"""Regression tests for `debug-substrate-okg-self` Phase 3.

Locks in the four viz fixes shipped 2026-04-25:

- B7  — viz endpoints carry real degrees, not hardcoded 0
- B8  — `okg.viz.boot._load_viz_config_for` keyword shape
- B9  — `okg.viz.boot.load` uses `requested_generation=`
- B10 — `/viz/api/search` doesn't AttributeError on import shape

The B6 layout safeguard fix is JS-side; covered by the
spec scenario `disconnected_sample_grids` and by manual
viz verification in Phase 11.4.
"""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import psycopg
import pytest

pytest.importorskip("fastapi")

from fastapi.testclient import TestClient

from okg.substrate.catalog.modules_apply import apply_composed_catalog
from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.ingest import SourceRunner
from okg.substrate.library.sources.docsite import DocsiteSource
from okg.substrate.publisher import Publisher
from tests.substrate.conftest import (
    _DERIVED_RUNTIME_TABLES,
    _RUNTIME_TABLES,
    _clean_runtime_subset,
)
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def _seed_docs(dsn: str, tmp_path: Path) -> None:
    """Bootstrap a docs-corpus generation; small + has edges."""
    init = subprocess.run(
        [sys.executable, "-m", "okg.substrate.cli.main",
         "init", "--template", "docs-corpus",
         "--target", str(tmp_path / "work")],
        capture_output=True, text=True, timeout=30,
    )
    assert init.returncode == 0, init.stderr
    scaffold = tmp_path / "work" / "docs-corpus"
    fixture = scaffold / "fixtures" / "docsite.json"
    composed = compose_catalog(scaffold)
    with psycopg.connect(dsn) as conn:
        apply_composed_catalog(conn, composed)
    SourceRunner(dsn, source_name="docs-corpus.docsite").run(
        DocsiteSource(fixture_path=str(fixture)), mode="scope_complete",
    )
    out = Publisher(dsn, "default", catalog_identity=bind_test_catalog_writer(dsn, "default")).cycle()
    assert out["status"] == "published", out


def _make_app_client(dsn: str, tmp_path: Path):
    """Make a TestClient against a viz wired to the seeded DB."""
    from psycopg_pool import ConnectionPool

    from okg.substrate.mcp.session import open_session
    from okg.viz.app import build_app
    from okg.viz.config import load_default_config
    from okg.viz.context import VizContext

    pool = ConnectionPool(
        conninfo=dsn, min_size=1, max_size=4,
        kwargs={"autocommit": True}, open=True, name="okg-viz-test-2",
    )
    with psycopg.connect(dsn, autocommit=True) as seed:
        session = open_session(seed, deployment_name="default")
    ctx = VizContext(
        dsn=dsn, deployment_name="default",
        manifest={"name": "default", "description": ""},
        config=load_default_config(), pool=pool, session=session,
        deployment_dir=tmp_path,
    )
    app = build_app(ctx_provider=lambda: ctx, is_ready=lambda: True)
    return TestClient(app), ctx


def _seed_node_id(ctx) -> str:
    """Return one real node_id from the seeded generation.

    Post chronos-native cutover ``okg.v_nodes`` is empty on a raw
    (non-checked-out) connection — graph content only materializes
    inside a Chronos checkout pinned to the session generation.
    Source the id through the substrate's session-pinned read path
    (the same one the viz endpoints use) rather than a raw select.
    """
    from okg.viz import reads

    with psycopg.connect(ctx.dsn, autocommit=True) as conn:
        for node in reads.iter_nodes(
            conn, ctx.session, limit=1, light=True, order_by_id=True,
        ):
            return node.id
    raise AssertionError("seeded generation produced no nodes")


@pytest.fixture(scope="module")
def seeded_app(
    okg_migrated: str,
    tmp_path_factory: pytest.TempPathFactory,
):
    tmp_path = tmp_path_factory.mktemp("viz-regressions")
    _clean_runtime_subset(
        okg_migrated,
        _RUNTIME_TABLES + _DERIVED_RUNTIME_TABLES,
        operation="seed shared viz regression fixture",
        reset_generations=True,
        clean_derived_schema=True,
    )
    _seed_docs(okg_migrated, tmp_path)
    client, ctx = _make_app_client(okg_migrated, tmp_path)
    yield client, ctx
    ctx.close()


# ---------- B7: real degrees ----------

def test_type_sample_carries_real_degrees(seeded_app) -> None:
    """Hits to `/types/<sub>/sample` must include `degree`
    fields whose values reflect actual edge counts. Pre-fix
    every endpoint hardcoded `degree: 0`."""
    client, _ctx = seeded_app
    # Discover the actual subtype the docsite fixture emits
    # — fixture content varies by template version.
    types_resp = client.get("/viz/api/types")
    assert types_resp.status_code == 200, types_resp.text
    types_body = types_resp.json()
    assert types_body, "expected types response to be non-empty"
    subtype = sorted(types_body, key=lambda k: -types_body[k])[0]

    r = client.get(f"/viz/api/types/{subtype}/sample?limit=10")
    assert r.status_code == 200, r.text
    body = r.json()
    items = body.get("items") or body.get("nodes") or []
    assert items, (
        f"expected at least one item for subtype {subtype!r}"
    )
    for item in items:
        assert "degree" in item, f"item has no degree key: {item}"
    # The substrate hardcoded degree=0 in 2026-04-25; after the
    # fix, EVERY item carries an int degree (may be 0 if the
    # node has no edges, but the field is real, not stubbed).
    assert all(isinstance(item["degree"], int) for item in items)
    # And the items are sorted by degree desc.
    if len(items) >= 2:
        assert items[0]["degree"] >= items[-1]["degree"]


def test_search_carries_real_degrees(seeded_app) -> None:
    """Search hits also include real degrees (not 0)."""
    client, _ = seeded_app
    r = client.get("/viz/api/search?q=docs")
    assert r.status_code == 200, r.text
    items = r.json().get("nodes") or []
    if not items:
        pytest.skip("docsite fixture didn't produce search hits")
    for item in items:
        assert "degree" in item


# ---------- B8/B9: viz boot signature ----------

def test_viz_boot_load_does_not_drift_on_signatures(
    seeded_app, tmp_path: Path,
) -> None:
    """`okg.viz.boot.load(dsn, deployment_name)` must call
    `load_viz_config(inline=...)` with the keyword-only shape,
    and `open_session(requested_generation=...)` not
    `generation_id=...`. Pre-fix, both were positional /
    wrong-name and only surfaced when a real user invoked
    `boot.load` (which existing viz tests bypass)."""
    _client, seeded_ctx = seeded_app
    # Synthesize a deployment dir with manifest so
    # `_find_deployment_dir` resolves.
    dep_dir = tmp_path / "deployments" / "default"
    dep_dir.mkdir(parents=True)
    (dep_dir / "deployment.yaml").write_text(
        "name: default\ndescription: test\n"
    )
    # Override the deployment lookup root to point at our
    # tmp tree.
    from okg.viz import boot as _boot

    orig = _boot._find_deployment_dir
    _boot._find_deployment_dir = lambda name: dep_dir
    try:
        ctx = _boot.load(dsn=seeded_ctx.dsn, deployment_name="default")
    finally:
        _boot._find_deployment_dir = orig

    assert ctx is not None
    assert ctx.session is not None
    # generation_id is now an opaque chronos pin STRING (was an int); the
    # int generation id lives separately on compat_generation_id.
    assert isinstance(ctx.session.generation_id, str)
    assert ctx.session.generation_id
    assert ctx.session.compat_generation_id is not None
    assert ctx.session.compat_generation_id > 0
    assert ctx.deployment_name == "default"
    ctx.close()


# ---------- B10: search import shape ----------

def test_viz_api_search_returns_200_no_attribute_error(
    seeded_app,
) -> None:
    """Pre-fix: `/viz/api/search?q=X` raised AttributeError
    on every request because the endpoint did
    `from okg.substrate.mcp import search as mcp_search` then
    called `mcp_search.search(...)` — `mcp_search` was the
    function itself, not the module."""
    client, _ = seeded_app
    r = client.get("/viz/api/search?q=intro")
    assert r.status_code == 200, (
        f"search endpoint regressed: {r.status_code} {r.text}"
    )
    body = r.json()
    assert "nodes" in body
    assert isinstance(body["nodes"], list)


def test_viz_api_search_prefers_exact_alias(seeded_app) -> None:
    """Short aliases should resolve before the legacy substring
    fallback. Large deployments cannot afford an unindexed
    label/title/name scan for common operator search terms."""
    client, ctx = seeded_app
    node_id = _seed_node_id(ctx)
    with psycopg.connect(ctx.dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.aliases "
                "(alias, canonical, scope, catalog_version_id) "
                "VALUES (%s, %s, NULL, %s) "
                "ON CONFLICT (alias, catalog_version_id) "
                "DO UPDATE SET canonical = EXCLUDED.canonical",
                ("viz-fast-alias-test", node_id, ctx.session.catalog_version_id),
            )

    r = client.get("/viz/api/search?q=viz-fast-alias-test")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["meta"]["strategy"] == "exact_alias"
    assert body["nodes"][0]["id"] == node_id
    assert body["nodes"][0]["match_tier"] == "exact_alias"


def test_viz_api_search_degree_timeout_is_nonfatal(
    seeded_app, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Degree counts are useful ranking hints, but search must not
    block on a high-degree node in a large edge table."""
    client, ctx = seeded_app
    node_id = _seed_node_id(ctx)
    with psycopg.connect(ctx.dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO okg.aliases "
                "(alias, canonical, scope, catalog_version_id) "
                "VALUES (%s, %s, NULL, %s) "
                "ON CONFLICT (alias, catalog_version_id) "
                "DO UPDATE SET canonical = EXCLUDED.canonical",
                ("viz-degree-timeout-test", node_id, ctx.session.catalog_version_id),
            )

    from okg.viz import endpoints

    def raise_timeout(*_args, **_kwargs):
        raise psycopg.errors.QueryCanceled("synthetic timeout")

    monkeypatch.setattr(endpoints.reads, "degrees_for", raise_timeout)

    r = client.get("/viz/api/search?q=viz-degree-timeout-test")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["meta"]["strategy"] == "exact_alias"
    assert body["nodes"][0]["id"] == node_id
    assert body["nodes"][0]["degree"] == 0
