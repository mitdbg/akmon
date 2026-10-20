"""The console Graph reads without a database (PACT console-graph-views).

The composers' shaping, their named refusals, the declared-schema reader and
the routes' parameter guard. The database behaviour -- real rollups, the grouped
neighbourhood on a 5,001-neighbour hub, the policy filter and the route pins --
is proven in tests/substrate/viz/test_console_graph_reads_db.py.
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from okg.substrate.console_views import graph_ontology, graph_views
from okg.substrate.console_views.console_count_rollup_read import (
    ConsoleCountRollupUnavailable,
)
from okg.substrate.console_views.ontology_rollup_read import (
    OntologyBucketRow,
    OntologyRollup,
    OntologyRollupUnavailable,
)

SESSION = SimpleNamespace(
    deployment_name="default",
    generation_id="okg:default:branch:default:gen:1:abc",
    branch_name="default",
    catalog_version_id=3,
)

COUNTS = {
    ("subtype", "doc"): 2,
    ("subtype", "release"): 3,
    ("edge_type", "cites"): 3,
    ("source", "fixture"): 5,
    ("source", "view:daily"): 1,
    ("edge_source", "fixture"): 3,
    ("edge_source", "_enricher:alias"): 4,
    ("source_subtype", "fixture\x1fdoc"): 2,
    ("source_subtype", "fixture\x1frelease"): 3,
}


def _rollup() -> OntologyRollup:
    return OntologyRollup(generation_id=SESSION.generation_id, rows=[
        OntologyBucketRow("doc", "cites", "release", "fixture", 3),
        OntologyBucketRow("doc", "cites", "release", "_enricher:alias", 4),
    ])


@pytest.mark.parametrize(("producer", "kind"), [
    ("okg-workspace.git-history", "source"),
    ("_enricher:code_reference", "enricher"),
    ("_publisher_derived", "enricher"),
    ("_publisher_derived:unattributed", "enricher"),
    ("view:daily", "view"),
    ("catalog:views_registry", "view"),
    ("_system:native_views", "view"),
])
def test_producer_kind(producer: str, kind: str) -> None:
    assert graph_views.producer_kind(producer) == kind


def test_types_are_shaped_from_the_rollups_alone(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(graph_views, "read_console_count_rollup", lambda conn, **kw: dict(COUNTS))
    monkeypatch.setattr(graph_views, "read_ontology_rollup", lambda conn, **kw: _rollup())
    # No graph scan: a checkout here would be a node or edge read.
    monkeypatch.setattr(graph_views, "checkout", lambda *a: pytest.fail("types scanned the graph"))

    payload = graph_views.compose_graph_types(object(), session=SESSION, deployment_name="default")

    assert payload["totals"] == {"records": 5, "relationships": 3}
    assert payload["types"] == [{"type": "release", "count": 3}, {"type": "doc", "count": 2}]
    assert {s["source"]: s["kind"] for s in payload["sources"]} == {
        "fixture": "source", "view:daily": "view", "_enricher:alias": "enricher",
    }
    assert payload["sources"][0] == {"source": "fixture", "kind": "source", "records": 5, "relationships": 3}
    assert payload["records_by_source"][0] == {"source": "fixture", "type": "release", "count": 3}
    assert payload["patterns"] == [{
        "from": "doc", "relationship": "cites", "to": "release", "count": 7,
        "producers": [{"producer": "_enricher:alias", "count": 4}, {"producer": "fixture", "count": 3}],
    }]


def test_an_unmaintained_count_rollup_is_a_named_503(monkeypatch: pytest.MonkeyPatch) -> None:
    def unavailable(conn, **kw):
        raise ConsoleCountRollupUnavailable(SESSION.generation_id, "sentinel_absent", "no sentinel")

    monkeypatch.setattr(graph_views, "read_console_count_rollup", unavailable)
    with pytest.raises(graph_views.GraphReadRefused) as refused:
        graph_views.compose_graph_types(object(), session=SESSION, deployment_name="default")

    assert refused.value.status == 503
    assert refused.value.detail["reason"] == "count_rollup_unavailable"
    assert refused.value.detail["rollup_reason"] == "sentinel_absent"


def test_an_unmaintained_ontology_rollup_is_a_named_503(monkeypatch: pytest.MonkeyPatch) -> None:
    def unavailable(conn, **kw):
        raise OntologyRollupUnavailable(SESSION.generation_id, "sentinel_absent", "no sentinel")

    monkeypatch.setattr(graph_views, "read_console_count_rollup", lambda conn, **kw: dict(COUNTS))
    monkeypatch.setattr(graph_views, "read_ontology_rollup", unavailable)
    with pytest.raises(graph_views.GraphReadRefused) as refused:
        graph_views.compose_graph_types(object(), session=SESSION, deployment_name="default")

    assert (refused.value.status, refused.value.detail["reason"]) == (503, "ontology_rollup_unavailable")


def test_ontology_refuses_a_version_without_a_catalog() -> None:
    session = SimpleNamespace(**{**vars(SESSION), "catalog_version_id": None})
    with pytest.raises(graph_views.GraphReadRefused) as refused:
        graph_ontology.compose_graph_ontology(
            object(), session=session, deployment_name="default", deployment_dir="/nowhere",
        )
    assert (refused.value.status, refused.value.detail["reason"]) == (503, "catalog_version_unavailable")


# --------------------------------------------------------------------------- #
# declared attributes                                                          #
# --------------------------------------------------------------------------- #
_MODULE_A = """
slots:
  owner:
    required: true
    range: Person
classes:
  Base:
    attributes:
      name: {required: true}
  Tagged:
    attributes:
      tags: {range: Tag}
  MCPServer:
    is_a: Base
    mixins: [Tagged]
    slots: [owner]
    slot_usage:
      owner: {required: false}
    attributes:
      url: {}
  EdgeNarrowings:
    attributes:
      mcp_server_uses: {}
"""

_MODULE_B = """
default_range: integer
classes:
  MCPServer:
    attributes:
      ignored_second_declaration: {}
  Counter:
    attributes:
      total: {}
"""

_DEPLOYMENT_SCHEMA = """
classes:
  MCPServer:
    attributes:
      url: {required: true}
      region: {}
"""


def _deployment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "deployment"
    (root / "schemas").mkdir(parents=True)
    (root / "deployment.yaml").write_text("name: fixture\nmodules: [a, b]\n")
    (root / "schemas" / "local.yaml").write_text(_DEPLOYMENT_SCHEMA)
    modules = {}
    for name, text in (("a", _MODULE_A), ("b", _MODULE_B)):
        (tmp_path / name).mkdir()
        (tmp_path / name / "schema.yaml").write_text(text)
        modules[name] = SimpleNamespace(module_dir=tmp_path / name)
    import okg.substrate.catalog.module_providers as providers
    import okg.substrate.catalog.modules_compose as compose

    monkeypatch.setattr(providers, "resolve_modules", lambda refs, provs: {r: modules[r] for r in refs})
    monkeypatch.setattr(compose, "providers_for_declared_deployment", lambda root: {})
    return root


def test_declared_attributes_follow_compose_catalogs_files_and_names(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    root = _deployment(tmp_path, monkeypatch)

    files = graph_ontology.declared_schema_files(root)
    declared = graph_ontology.declared_attributes(root)

    assert [origin for origin, _path in files] == ["a", "b", "deployment:fixture"]
    # compose_catalog's own name conversion: MCPServer is mcp_server.
    module, attributes = declared["mcp_server"]
    assert module == "a"
    assert attributes == [
        {"name": "name", "required": True, "range": "string"},
        {"name": "owner", "required": False, "range": "Person"},
        {"name": "region", "required": False, "range": "string"},
        {"name": "tags", "required": False, "range": "Tag"},
        {"name": "url", "required": False, "range": "string"},
    ]
    assert declared["counter"] == ("b", [{"name": "total", "required": False, "range": "integer"}])
    assert "edge_narrowings" not in declared


def test_a_missing_deployment_manifest_is_refused_not_emptied(tmp_path: Path) -> None:
    with pytest.raises(graph_ontology.DeclaredSchemaUnavailable):
        graph_ontology.declared_attributes(tmp_path / "missing")


# --------------------------------------------------------------------------- #
# routes                                                                       #
# --------------------------------------------------------------------------- #
def test_graph_routes_refuse_a_parameter_they_cannot_honor() -> None:
    pytest.importorskip("fastapi")
    from fastapi import APIRouter, FastAPI
    from fastapi.testclient import TestClient

    from okg.viz.record_endpoints import register_operator_record_routes

    app = FastAPI()
    router = APIRouter(prefix="/viz/api")
    register_operator_record_routes(router, ctx_provider=lambda: pytest.fail("read before the guard"))
    app.include_router(router)
    client = TestClient(app)

    for path in ("types", "source-links", "ontology"):
        response = client.get(f"/viz/api/console/graph/{path}", params={"type": "doc"})
        assert response.status_code == 422, path
        assert response.json()["detail"]["reason"] == "unknown_parameter"
    response = client.get("/viz/api/console/graph/record", params={"id": "a", "depth": "2"})
    assert response.status_code == 422
    assert response.json()["detail"]["parameters"] == ["depth"]
    # An empty or blank `version=` is refused, never read as "the latest".
    for path, extra in (("types", {}), ("source-links", {}), ("ontology", {}), ("record", {"id": "a"})):
        for blank in ("", "  "):
            response = client.get(f"/viz/api/console/graph/{path}", params={**extra, "version": blank})
            assert response.status_code == 422, (path, blank)
