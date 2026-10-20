"""Route recommendation kernel tests (AKMON Phase 6.2)."""
from __future__ import annotations

from pathlib import Path
from textwrap import dedent
from uuid import uuid4

import psycopg
import pytest

from okg.substrate.routes import (
    RouteManifest,
    RouteManifestError,
    RouteRecommendationContext,
    load_manifest,
    load_manifest_for_deployment,
    recommend_routes,
)


# --- Manifest loader ---------------------------------------------------


def _write_manifest(tmp_path: Path, body: str) -> Path:
    path = tmp_path / "routes.yaml"
    path.write_text(dedent(body), encoding="utf-8")
    return path


def test_load_manifest_happy_path(tmp_path: Path) -> None:
    path = _write_manifest(
        tmp_path,
        """
        version: 1
        routes:
          - id: r1
            when:
              file_extension: ".py"
            recommend:
              tool: pytest
            rationale: "Python edit -> tests"
        """,
    )
    manifest = load_manifest(path)
    assert manifest.version == 1
    assert manifest.source_path == path
    assert manifest.checksum.startswith("sha256:")
    assert len(manifest.routes) == 1
    assert manifest.routes[0].id == "r1"
    assert manifest.routes[0].when == {"file_extension": ".py"}
    assert manifest.routes[0].recommend == {"tool": "pytest"}
    assert manifest.routes[0].rationale == "Python edit -> tests"
    assert manifest.routes[0].priority == 0


def test_load_manifest_missing_file_raises(tmp_path: Path) -> None:
    with pytest.raises(RouteManifestError):
        load_manifest(tmp_path / "absent.yaml")


def test_load_manifest_rejects_missing_id(tmp_path: Path) -> None:
    path = _write_manifest(
        tmp_path,
        """
        version: 1
        routes:
          - when: {file_extension: ".py"}
            recommend: {tool: pytest}
            rationale: "no id"
        """,
    )
    with pytest.raises(RouteManifestError, match="routes\\[0\\].id is required"):
        load_manifest(path)


def test_load_manifest_rejects_duplicate_id(tmp_path: Path) -> None:
    path = _write_manifest(
        tmp_path,
        """
        version: 1
        routes:
          - id: same
            recommend: {tool: a}
            rationale: r
          - id: same
            recommend: {tool: b}
            rationale: r
        """,
    )
    with pytest.raises(RouteManifestError, match="duplicate route id"):
        load_manifest(path)


def test_load_manifest_rejects_missing_tool(tmp_path: Path) -> None:
    path = _write_manifest(
        tmp_path,
        """
        version: 1
        routes:
          - id: r1
            recommend: {}
            rationale: r
        """,
    )
    with pytest.raises(RouteManifestError, match="recommend.tool is required"):
        load_manifest(path)


def test_load_manifest_rejects_missing_rationale(tmp_path: Path) -> None:
    path = _write_manifest(
        tmp_path,
        """
        version: 1
        routes:
          - id: r1
            recommend: {tool: a}
        """,
    )
    with pytest.raises(RouteManifestError, match="rationale is required"):
        load_manifest(path)


def test_load_manifest_for_deployment_returns_none_when_absent(
    tmp_path: Path,
) -> None:
    assert load_manifest_for_deployment(tmp_path) is None


def test_load_manifest_for_deployment_picks_up_file(tmp_path: Path) -> None:
    _write_manifest(
        tmp_path,
        """
        version: 1
        routes:
          - id: r1
            recommend: {tool: a}
            rationale: r
        """,
    )
    manifest = load_manifest_for_deployment(tmp_path)
    assert manifest is not None
    assert manifest.routes[0].id == "r1"


# --- Recommendation engine --------------------------------------------


def _two_rule_manifest() -> RouteManifest:
    from okg.substrate.routes.manifest import _parse_routes_payload

    return _parse_routes_payload(
        {
            "version": 1,
            "routes": [
                {
                    "id": "py-tests",
                    "when": {"file_extension": ".py"},
                    "recommend": {"tool": "pytest"},
                    "rationale": "python file -> pytest",
                },
                {
                    "id": "openspec",
                    "when": {"change_starts_with": "add-"},
                    "recommend": {"tool": "openspec_validate"},
                    "rationale": "openspec change -> validate",
                },
                {
                    "id": "schema-sql",
                    "when": {"file": "src/okg/substrate/db/schema.sql"},
                    "recommend": {"tool": "okg_migrate"},
                    "rationale": "schema edit -> migrate",
                },
            ],
        },
        source="test",
    )


def test_recommend_returns_empty_when_manifest_is_none() -> None:
    ctx = RouteRecommendationContext(deployment="x", file="foo.py")
    assert recommend_routes(None, ctx) == []


def test_recommend_matches_file_extension_with_or_without_dot() -> None:
    manifest = _two_rule_manifest()
    # Manifest uses ".py"; context value without the dot still matches.
    ctx_with_dot = RouteRecommendationContext(
        deployment="x", file_extension=".py"
    )
    ctx_without_dot = RouteRecommendationContext(
        deployment="x", file_extension="py"
    )
    ids_with = [m.route_id for m in recommend_routes(manifest, ctx_with_dot)]
    ids_without = [
        m.route_id for m in recommend_routes(manifest, ctx_without_dot)
    ]
    assert "py-tests" in ids_with
    assert ids_with == ids_without


def test_recommend_change_starts_with_prefix() -> None:
    manifest = _two_rule_manifest()
    ctx = RouteRecommendationContext(
        deployment="x", change="add-akmon-quarantine-routing"
    )
    ids = [m.route_id for m in recommend_routes(manifest, ctx)]
    assert ids == ["openspec"]


def test_recommend_combined_match_returns_two() -> None:
    manifest = _two_rule_manifest()
    ctx = RouteRecommendationContext(
        deployment="x",
        change="add-foo",
        file_extension=".py",
    )
    ids = [m.route_id for m in recommend_routes(manifest, ctx)]
    assert ids == ["py-tests", "openspec"]


def test_recommend_no_match_returns_empty() -> None:
    manifest = _two_rule_manifest()
    ctx = RouteRecommendationContext(deployment="x", file_extension=".rs")
    assert recommend_routes(manifest, ctx) == []


def test_recommend_carries_manifest_checksum() -> None:
    manifest = _two_rule_manifest()
    ctx = RouteRecommendationContext(deployment="x", file_extension=".py")
    matches = recommend_routes(manifest, ctx)
    assert matches
    for m in matches:
        assert m.manifest_checksum == manifest.checksum


def test_recommend_strict_file_match() -> None:
    manifest = _two_rule_manifest()
    ctx_match = RouteRecommendationContext(
        deployment="x",
        file="src/okg/substrate/db/schema.sql",
    )
    ctx_other = RouteRecommendationContext(
        deployment="x",
        file="src/okg/substrate/other.sql",
        file_extension=".sql",
    )
    assert [m.route_id for m in recommend_routes(manifest, ctx_match)] == [
        "schema-sql"
    ]
    # The other file matches no rule (no .sql extension rule in the
    # manifest, and the file path differs).
    assert recommend_routes(manifest, ctx_other) == []


def test_recommend_priority_sorts_lower_first() -> None:
    from okg.substrate.routes.manifest import _parse_routes_payload

    manifest = _parse_routes_payload(
        {
            "version": 1,
            "routes": [
                {
                    "id": "low",
                    "priority": 10,
                    "when": {"file_extension": ".py"},
                    "recommend": {"tool": "a"},
                    "rationale": "low priority",
                },
                {
                    "id": "high",
                    "priority": 0,
                    "when": {"file_extension": ".py"},
                    "recommend": {"tool": "b"},
                    "rationale": "high priority",
                },
            ],
        },
        source="test",
    )
    ctx = RouteRecommendationContext(deployment="x", file_extension=".py")
    ids = [m.route_id for m in recommend_routes(manifest, ctx)]
    assert ids == ["high", "low"]


def test_recommend_limit_truncates() -> None:
    manifest = _two_rule_manifest()
    ctx = RouteRecommendationContext(
        deployment="x", change="add-foo", file_extension=".py"
    )
    matches = recommend_routes(manifest, ctx, limit=1)
    assert len(matches) == 1
    assert matches[0].route_id == "py-tests"


def test_recommend_requires_deployment_in_mapping_context() -> None:
    manifest = _two_rule_manifest()
    with pytest.raises(ValueError, match="deployment"):
        recommend_routes(manifest, {"file_extension": ".py"})


def test_okg_workspace_routes_yaml_is_loadable_and_matches() -> None:
    """The shipped okg-workspace routes.yaml fixture loads cleanly and
    its three rules behave as expected against representative contexts.
    """
    from okg.substrate.deployments import resolve_deployment

    deployment = resolve_deployment("okg-workspace")
    manifest = load_manifest_for_deployment(deployment.deployment_dir)
    assert manifest is not None
    rule_ids = {r.id for r in manifest.routes}
    assert rule_ids == {
        "python-source-pytest",
        "openspec-change-validate",
        "schema-sql-edit",
    }

    # A .py file under an OpenSpec change context fires both the
    # python-source-pytest and openspec-change-validate rules.
    ctx = RouteRecommendationContext(
        deployment="okg-workspace",
        change="add-akmon-quarantine-routing",
        file="src/okg/substrate/foo.py",
        file_extension=".py",
    )
    ids = [m.route_id for m in recommend_routes(manifest, ctx)]
    assert "python-source-pytest" in ids
    assert "openspec-change-validate" in ids

    # The schema.sql file rule fires only on the exact path.
    ctx_sql = RouteRecommendationContext(
        deployment="okg-workspace",
        file="src/okg/substrate/db/schema.sql",
    )
    ids_sql = [m.route_id for m in recommend_routes(manifest, ctx_sql)]
    assert ids_sql == ["schema-sql-edit"]


# --- mcp_trace_events.route_id (Phase 6.3 trace closure) ----------------


pytestmark = [pytest.mark.postgres, pytest.mark.serial_db]


def test_mcp_trace_events_has_route_id_column(clean_runtime: str) -> None:
    """Schema migration: AKMON Phase 6.2 adds route_id +
    route_manifest_checksum to okg.mcp_trace_events.
    """

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                  FROM information_schema.columns
                 WHERE table_schema = 'okg'
                   AND table_name = 'mcp_trace_events'
                   AND column_name IN (
                     'route_id', 'route_manifest_checksum'
                   )
                 ORDER BY column_name
                """
            )
            columns = [r[0] for r in cur.fetchall()]
    assert columns == ["route_id", "route_manifest_checksum"]


def test_agent_context_routes_payload_for_okg_workspace() -> None:
    """The agent_context helper exposes ``recommended_routes`` for a
    real deployment + file context. This is the integration surface
    for the AKMON Phase 6.2 route-trace loop: agent_context emits the
    route_id + manifest checksum that downstream tool calls echo back
    via trace_tool_call."""

    from okg.substrate.agent_context import _recommended_routes_payload

    routes, checksum = _recommended_routes_payload(
        deployment="okg-workspace",
        change="add-akmon-quarantine-routing",
        file="src/okg/substrate/foo.py",
        cwd=None,
    )
    assert checksum is not None and checksum.startswith("sha256:")
    ids = [r["route_id"] for r in routes]
    assert "python-source-pytest" in ids
    assert "openspec-change-validate" in ids
    # Each route payload carries its manifest checksum so a downstream
    # tool call can quote it back via trace_tool_call's
    # route_manifest_checksum argument.
    for r in routes:
        assert r["manifest_checksum"] == checksum


def test_agent_context_routes_payload_handles_missing_routes(tmp_path):
    """A deployment without routes.yaml returns ([], None) so the
    agent_context call does not depend on the route fixture
    existing."""

    from okg.substrate.agent_context import _recommended_routes_payload

    # Use a non-existent deployment name so resolve_deployment fails
    # cleanly — the helper swallows the resolution error and returns
    # the empty-routes shape.
    routes, checksum = _recommended_routes_payload(
        deployment=f"absent-deployment-{uuid4().hex[:8]}",
        change=None,
        file=None,
        cwd=None,
    )
    assert routes == []
    assert checksum is None


def test_trace_tool_call_writes_route_attribution(
    clean_runtime: str,
) -> None:
    """An MCP tool call tagged with route_id +
    route_manifest_checksum lands them in okg.mcp_trace_events, so
    the optimization loop can join recommendations to actual tool
    use."""

    from psycopg_pool import ConnectionPool

    from okg.substrate.mcp.tracing import trace_tool_call

    session_id = f"test-route-trace-{uuid4().hex[:8]}"
    deployment = f"test-route-trace-{uuid4().hex[:8]}"

    pool = ConnectionPool(clean_runtime, min_size=1, max_size=2)
    try:
        # Tracing is gated by an env var; force-enable for this test.
        import os

        prior = os.environ.get("OKG_MCP_TRACING")
        os.environ["OKG_MCP_TRACING"] = "1"
        try:
            trace_tool_call(
                pool,
                deployment_name=deployment,
                tool_name="search",
                request={"q": "foo"},
                response={"hits": 0},
                duration_ms=12,
                session_id=session_id,
                route_id="python-source-pytest",
                route_manifest_checksum="sha256:abc",
            )
        finally:
            if prior is None:
                os.environ.pop("OKG_MCP_TRACING", None)
            else:
                os.environ["OKG_MCP_TRACING"] = prior
    finally:
        pool.close()

    with psycopg.connect(clean_runtime) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT tool_name, route_id, route_manifest_checksum
                  FROM okg.mcp_trace_events
                 WHERE session_id = %s
                """,
                (session_id,),
            )
            row = cur.fetchone()
    assert row is not None
    assert row[0] == "search"
    assert row[1] == "python-source-pytest"
    assert row[2] == "sha256:abc"
