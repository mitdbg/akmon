"""Live types: the declaration, the registry and the start and publish checks.

Issue #3666, PACT `live-graph-rucio-sites`, task `declare`. Example sites are
live nodes: `site:<name>` and `system:example` are declared once per type
(`okg_live_type`) and never stored.

The first half is hermetic (no database): the parser accepts the fixture's
`Site`, `System` and `Item` declarations, and refuses each named mistake by
name. The second half (`-m postgres`) drives the real server build and the
real publish gate: a stored node that uses a live type or prefix refuses the
start and blocks the publish, and a relation declared on two subtypes
refuses the start, before any adapter runner is built.
"""
from __future__ import annotations

import copy
import json
import uuid
from pathlib import Path
from typing import Any

import pytest
import yaml

from okg.substrate.catalog.modules_compose import (
    DeploymentSubtypeOverlayRefused,
    compose_catalog,
)
from okg.substrate.external_backed.live_relations import parse_live_relations
from okg.substrate.external_backed.live_types import (
    LiveArgMissing,
    LiveNodeIdInvalid,
    LiveTypeInvalid,
    LiveTypeRegistry,
    parse_live_type,
)

FIXTURE = Path(__file__).parent / "fixtures" / "live_types_sites.yaml"


def _schema() -> dict[str, Any]:
    return yaml.safe_load(FIXTURE.read_text(encoding="utf-8"))


def _compose(tmp_path: Path, schema: dict[str, Any] | None = None,
             *, overlay: dict[str, Any] | None = None):
    library = tmp_path / "library"
    (library / "live_example").mkdir(parents=True)
    (library / "live_example" / "schema.yaml").write_text(
        yaml.safe_dump(schema if schema is not None else _schema()),
        encoding="utf-8",
    )
    deployment = tmp_path / "deployment"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        json.dumps({"name": "fixture", "modules": ["live_example"]}),
        encoding="utf-8",
    )
    if overlay is not None:
        (deployment / "schemas").mkdir()
        (deployment / "schemas" / "overlay.yaml").write_text(
            yaml.safe_dump(overlay), encoding="utf-8",
        )
    cross_links = tmp_path / "cross_links"
    cross_links.mkdir()
    return compose_catalog(
        deployment, modules_root=library, cross_links_root=cross_links,
    )


def _registry(tmp_path: Path) -> LiveTypeRegistry:
    return LiveTypeRegistry.from_catalog(_compose(tmp_path).subtype_metadata)


def _site_parts(schema: dict[str, Any] | None = None):
    """(annotation, relations) of the fixture's `Site` class."""
    annotations = (schema or _schema())["classes"]["Site"]["annotations"]
    relations = parse_live_relations(annotations, subtype="site")
    return copy.deepcopy(annotations["okg_live_type"]), relations


# ── the good declaration ──────────────────────────────────────────────

def test_the_fixture_composes_into_three_live_types(tmp_path) -> None:
    composed = _compose(tmp_path)
    assert {s for s, m in composed.subtype_metadata.items() if "live_type" in m} \
        == {"site", "system", "item"}
    registry = LiveTypeRegistry.from_catalog(composed.subtype_metadata)
    assert sorted(registry.types) == ["item", "site", "system"]

    site = registry.by_subtype("site")
    assert site is not None and site.id_prefix == "site" and site.id_from == ("site",)
    assert site.system == "example"
    assert sorted(site.fields) == ["static_total", "static_used"]
    # Fields that share a relation and arguments share one call per node.
    assert site.fields["static_total"].call == site.fields["static_used"].call
    assert site.fields["static_total"].column == "total"
    assert site.fields["static_total"].type == "number"
    assert site.open_fields is not None
    assert site.open_fields.relation == "example_list_site_attributes"
    assert site.exists is not None and site.exists.relation == "example_get_site"
    distance = site.edges["distance"]
    assert distance.to == "site"
    [to_arg] = distance.call.caller_args()
    assert (to_arg.name, to_arg.value, to_arg.required) == (
        "destination_site", "to", True,
    )
    assert site.relations() == {
        "example_get_site", "example_list_site_attributes",
        "example_get_site_usage_by_source", "example_list_site_items",
        "example_get_link",
    }

    system = registry.by_subtype("system")
    assert system is not None and system.declared_only
    assert system.declared_ids == ("example",)
    assert sorted(system.declared_fields["example"]) == ["ping", "whoami"]
    sites = system.edges["sites"]
    assert (sites.of, sites.to, sites.member_fields) == ("example", "site", True)
    assert "kind" in sites.member_columns
    assert sites.push_down is not None
    assert sites.push_down.relation == "example_list_sites"
    assert sites.push_down.arg == "site_expression"
    # The pushed listing yields names only (O1); the planner joins it with
    # one base listing call for any listing column.
    assert sites.push_down.member_columns == ("site",)
    assert sites.push_down.join == {"and": "&", "and_not": "\\", "or": "|"}

    assert registry.edge_set("site") == (system, sites)
    assert registry.edge_set("item") is None
    assert registry.by_prefix("system") is system
    assert registry.by_prefix("ticket") is None
    assert registry.by_subtype("ticket") is None


def test_the_stored_form_parses_back_to_the_same_type(tmp_path) -> None:
    registry = _registry(tmp_path)
    for live_type in registry.types.values():
        again = parse_live_type(
            live_type.subtype, live_type.to_metadata(), None,
        )
        assert again == live_type


def test_parse_node_id_for_site_system_and_a_stored_id(tmp_path) -> None:
    registry = _registry(tmp_path)
    site, parts = registry.parse_node_id("site:Z1_BB_BETA_Cold")
    assert site.subtype == "site" and parts == ("Z1_BB_BETA_Cold",)
    assert site.node_id(*parts) == "site:Z1_BB_BETA_Cold"
    assert site.name_parts(parts) == {"site": "Z1_BB_BETA_Cold"}

    system, parts = registry.parse_node_id("system:example")
    assert system.subtype == "system" and parts == ("example",)

    did, parts = registry.parse_node_id("item:grp:/p/q/r#x:y")
    assert did.subtype == "item" and parts == ("grp", "/p/q/r#x:y")

    # Not a live id: the read is a stored read, as before.
    assert registry.parse_node_id("ticket:123") is None
    assert registry.parse_node_id("no-prefix") is None


@pytest.mark.parametrize("node_id", [
    "site:",                 # empty name
    "site:not_a_site",       # fails the name pattern
    "site:Z2_AA_ALPHA\n",     # `$` alone accepts a trailing newline; full match does not
    "system:other",           # not a declared system in part 1
    "item:grp",              # one part of two
    "item::name",            # empty scope
])
def test_a_malformed_live_id_is_refused_by_name(tmp_path, node_id) -> None:
    registry = _registry(tmp_path)
    with pytest.raises(LiveNodeIdInvalid) as exc:
        registry.parse_node_id(node_id)
    assert isinstance(exc.value, LiveTypeInvalid)
    assert exc.value.data["node_id"] == node_id


def test_a_caller_node_id_reaches_the_relation_without_its_prefix(tmp_path) -> None:
    registry = _registry(tmp_path)
    distance = registry.by_subtype("site").edges["distance"]
    to = registry.caller_arg_value(distance, "site:Z2_AA_ALPHA")
    assert to == "Z2_AA_ALPHA"
    assert registry.caller_arg_value(distance, "Z2_AA_ALPHA") == "Z2_AA_ALPHA"
    assert distance.call.bind({"site": "Z1_BB_BETA_Cold"}, {"to": to}) == {
        "source_site": "Z1_BB_BETA_Cold", "destination_site": "Z2_AA_ALPHA",
    }
    with pytest.raises(LiveNodeIdInvalid):
        registry.caller_arg_value(distance, "item:grp:/p/q/r")


def test_a_required_caller_argument_missing_is_refused(tmp_path) -> None:
    distance = _registry(tmp_path).by_subtype("site").edges["distance"]
    with pytest.raises(LiveArgMissing) as exc:
        distance.call.bind({"site": "Z1_BB_BETA_Cold"}, {})
    assert exc.value.data["arg"] == "to"


# ── refusals at parse, by name ────────────────────────────────────────

def _refused(annotation, relations, *, reason: str | None = None,
             relation: str | None = None) -> LiveTypeInvalid:
    with pytest.raises(LiveTypeInvalid) as exc:
        parse_live_type("site", annotation, relations)
    error = exc.value
    assert error.data["subtype"] == "site"
    assert "site" in error.message
    if reason is not None:
        assert error.data.get("reason") == reason, error.data
    if relation is not None:
        assert error.data.get("relation") == relation, error.data
        assert relation in error.message
    return error


def test_an_undeclared_relation_is_refused() -> None:
    annotation, relations = _site_parts()
    annotation["fields"]["static_total"]["relation"] = "example_get_site_usage"
    error = _refused(annotation, relations, reason="relation_undeclared",
                     relation="example_get_site_usage")
    assert error.data["key"] == "fields.static_total.relation"


def test_an_undeclared_argument_is_refused() -> None:
    annotation, relations = _site_parts()
    annotation["edges"]["holds"]["args"]["site_expression"] = "x"
    error = _refused(annotation, relations, reason="arg_undeclared",
                     relation="example_list_site_items")
    assert error.data["arg"] == "site_expression"


def test_a_required_caller_argument_not_marked_required_is_refused() -> None:
    annotation, relations = _site_parts()
    annotation["edges"]["distance"]["args"]["destination_site"] = "$arg.to"
    error = _refused(annotation, relations, reason="required_arg_not_marked",
                     relation="example_get_link")
    assert error.data["arg"] == "destination_site"


def test_a_required_relation_argument_left_unfilled_is_refused() -> None:
    annotation, relations = _site_parts()
    del annotation["fields"]["static_total"]["args"]["source"]
    error = _refused(annotation, relations, reason="required_arg_unfilled",
                     relation="example_get_site_usage_by_source")
    assert error.data["unfilled"] == ["source"]


def test_a_fixed_value_outside_the_relations_bounds_is_refused() -> None:
    annotation, relations = _site_parts()
    annotation["edges"]["holds"]["args"]["limit"] = 5000
    _refused(annotation, relations, relation="example_list_site_items")
    annotation["edges"]["holds"]["args"]["limit"] = "1000"
    _refused(annotation, relations, relation="example_list_site_items")


def test_a_caller_argument_on_a_field_is_refused() -> None:
    annotation, relations = _site_parts()
    annotation["fields"]["static_total"]["args"]["source"] = {
        "value": "$arg.source", "required": True,
    }
    _refused(annotation, relations, reason="caller_arg_outside_edge")


def test_an_unknown_self_column_is_refused() -> None:
    annotation, relations = _site_parts()
    annotation["exists"]["args"]["site"] = "$self.name"
    _refused(annotation, relations, reason="self_column_unknown")


def test_an_unknown_key_is_refused() -> None:
    annotation, relations = _site_parts()
    annotation["edges"]["holds"]["member_feilds"] = "from_rows"
    error = _refused(annotation, relations)
    assert error.data["unknown"] == ["member_feilds"]


def test_an_unanchored_name_pattern_is_refused() -> None:
    annotation, relations = _site_parts()
    annotation["name_pattern"] = "T[0-3]_.+"
    _refused(annotation, relations)


# ── refusals across types ─────────────────────────────────────────────

def test_a_duplicate_prefix_is_refused(tmp_path) -> None:
    schema = _schema()
    schema["classes"]["Item"]["annotations"]["okg_live_type"]["id"]["prefix"] = "site"
    with pytest.raises(LiveTypeInvalid) as exc:
        _compose(tmp_path, schema)
    assert exc.value.data["reason"] == "duplicate_prefix"
    assert exc.value.data["prefix"] == "site"


def test_an_edge_to_a_type_that_is_not_live_is_refused(tmp_path) -> None:
    schema = _schema()
    del schema["classes"]["Item"]
    with pytest.raises(LiveTypeInvalid) as exc:
        _compose(tmp_path, schema)
    assert exc.value.data["reason"] == "edge_target_undeclared"
    assert exc.value.data["relation"] == "example_list_site_items"


def test_listing_columns_without_the_name_column_are_refused(tmp_path) -> None:
    schema = _schema()
    sites = schema["classes"]["System"]["annotations"]["okg_live_type"]["edges"]["sites"]
    sites["push_down"]["member_columns"] = ["kind"]
    with pytest.raises(LiveTypeInvalid) as exc:
        _compose(tmp_path, schema)
    assert exc.value.data["reason"] == "member_columns_lack_id"


def test_push_down_keys_parse_and_round_trip(tmp_path) -> None:
    registry = _registry(tmp_path)
    push = registry.by_subtype("system").edges["sites"].push_down
    assert push.keys == ("kind", "tier", "retired", "region", "zone", "campus")
    assert push.to_metadata()["keys"] == list(push.keys)


def test_a_push_down_key_that_is_a_declared_field_is_refused(tmp_path) -> None:
    """`static_total` is a per-node usage field, not an site attribute: a
    pushed `static_total=0` would read as an empty set."""
    schema = _schema()
    sites = schema["classes"]["System"]["annotations"]["okg_live_type"]["edges"]["sites"]
    sites["push_down"]["keys"] = ["kind", "static_total"]
    with pytest.raises(LiveTypeInvalid) as exc:
        _compose(tmp_path, schema)
    assert exc.value.data["reason"] == "push_key_is_a_field"
    assert exc.value.data["fields"] == ["static_total"]


@pytest.mark.parametrize("key", ["site type", "a|b", "x=y"])
def test_a_push_down_key_with_an_operator_character_is_refused(tmp_path, key) -> None:
    schema = _schema()
    sites = schema["classes"]["System"]["annotations"]["okg_live_type"]["edges"]["sites"]
    sites["push_down"]["keys"] = [key]
    with pytest.raises(LiveTypeInvalid) as exc:
        _compose(tmp_path, schema)
    assert exc.value.data["reason"] == "push_key_invalid"


def test_a_relation_named_by_two_live_types_is_refused_at_composition(tmp_path) -> None:
    """`relations()` maps a relation to one type; a second type naming it
    would win silently. `from_types` is what composition calls."""
    import dataclasses

    registry = _registry(tmp_path)
    site = registry.by_subtype("site")
    did = dataclasses.replace(registry.by_subtype("item"), exists=site.exists)
    types = [t for t in registry.types.values() if t.subtype != "item"] + [did]
    with pytest.raises(LiveTypeInvalid) as exc:
        LiveTypeRegistry.from_types(types)
    assert exc.value.data["reason"] == "relation_shared"
    assert exc.value.data["relation"] == "example_get_site"
    assert {exc.value.data["subtype"], exc.value.data["other"]} == {"item", "site"}


def test_a_relation_the_bindings_do_not_serve_is_refused(tmp_path) -> None:
    registry = _registry(tmp_path)
    bound = set(registry.relations()) - {"example_get_link"}
    with pytest.raises(LiveTypeInvalid) as exc:
        registry.assert_bound(bound)
    assert exc.value.data["unbound"] == ["example_get_link"]
    assert exc.value.data["subtype"] == "site"
    registry.assert_bound(registry.relations())


def test_a_served_name_that_is_not_a_live_type_is_refused(tmp_path) -> None:
    registry = _registry(tmp_path)
    registry.assert_served(["site", "system"])
    with pytest.raises(LiveTypeInvalid) as exc:
        registry.assert_served(["site", "gadget"])
    assert exc.value.data["unknown"] == ["gadget"]


# ── composition: overlays ─────────────────────────────────────────────

def test_an_overlay_may_add_a_live_type_where_none_is_set(tmp_path) -> None:
    schema = _schema()
    did = schema["classes"]["Item"]["annotations"].pop("okg_live_type")
    overlay = {"classes": {"Item": {"annotations": {
        "archetype": "entity", "okg_live_type": did,
    }}}}
    composed = _compose(tmp_path, schema, overlay=overlay)
    assert composed.subtype_metadata["item"]["live_type"]["id"]["prefix"] == "item"


def test_an_overlay_may_not_replace_a_library_live_type(tmp_path) -> None:
    did = _schema()["classes"]["Item"]["annotations"]["okg_live_type"]
    changed = {**did, "id": {"prefix": "gadget_id", "from": ["scope", "name"]}}
    overlay = {"classes": {"Item": {"annotations": {
        "archetype": "entity", "okg_extends": "live_example",
        "okg_live_type": changed,
    }}}}
    with pytest.raises(DeploymentSubtypeOverlayRefused) as exc:
        _compose(tmp_path, overlay=overlay)
    assert exc.value.data["key"] == "live_type"
    assert exc.value.data["reason"] == "value_conflict"


# ── the surface key ───────────────────────────────────────────────────

def _surface(**block):
    from okg.substrate.mcp.effective_surface import parse_agent_surface

    return parse_agent_surface({"agent_surface": {
        "served": ["inspect", "expand", "filter", "map", "aggregate"],
        **block,
    }})


def test_agent_surface_live_types_parses_and_answers_served() -> None:
    surface = _surface(live_types=["site", "system"])
    assert surface.live_types == ("site", "system")
    assert surface.serves_live_type("site")
    assert not surface.serves_live_type("item")
    assert _surface().live_types == ()


@pytest.mark.parametrize("block", [
    {"live_types": []},
    {"live_types": "site"},
    {"live_types": ["Site"]},
    {"live_types": ["site", "site"]},
    {"live_types": ["site"], "served": ["inspect", "filter"]},
])
def test_agent_surface_live_types_refusals(block) -> None:
    from okg.substrate.mcp.effective_surface import AgentSurfaceInvalid

    with pytest.raises(AgentSurfaceInvalid):
        _surface(**block)


# ── one relation, one subtype (hermetic half) ─────────────────────────

class _Cursor:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, *args, **kwargs):
        return None

    def fetchall(self):
        return self._rows


class _Conn:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return _Cursor(self._rows)


class _Session:
    catalog_version_id = 1


def test_a_relation_declared_on_two_subtypes_is_refused_not_last_wins() -> None:
    from okg.substrate.external_backed.live_dispatch import (
        LiveRelationDeclaredTwice,
        all_declared_live_relations,
    )

    rows = [
        ("site", {"live_relations": {"example_ping": {}}}),
        ("system", {"live_relations": {"example_ping": {}, "example_whoami": {}}}),
    ]
    with pytest.raises(LiveRelationDeclaredTwice) as exc:
        all_declared_live_relations(_Conn(rows), _Session())
    assert exc.value.data == {
        "relation": "example_ping", "subtypes": ["site", "system"],
    }
    assert all_declared_live_relations(_Conn(rows[1:]), _Session()) == {
        "example_ping": "system", "example_whoami": "system",
    }


# ══ database half: the real server build and the real publish gate ═══

_BUILT: list[Any] = []


@pytest.fixture
def _close_servers():
    yield
    while _BUILT:
        server = _BUILT.pop()
        for attr in ("_okg_pool", "_okg_write_pool"):
            pool = getattr(server, attr, None)
            if pool is not None:
                pool.close()


class _CountingAdapter:
    def __init__(self):
        self.calls: list[dict[str, Any]] = []

    def __call__(self, **kwargs):
        self.calls.append(kwargs)
        return []


def _metadata(tmp_path: Path) -> dict[str, dict[str, Any]]:
    composed = _compose(tmp_path)
    return {s: composed.subtype_metadata[s] for s in ("site", "system", "item")}


def _seed(conn, metadata: dict[str, dict[str, Any]] | None) -> None:
    from tests.pytest.catalog_fixture_builders import SubtypeSpec, seed_catalog

    specs: list[SubtypeSpec | str] = ["page"]
    for name in ("site", "system", "item"):
        specs.append(SubtypeSpec(name, metadata=(metadata or {}).get(name)))
    seed_catalog(conn, subtypes=specs, narrowings=[])


def _store(conn, node_id: str, subtype: str) -> None:
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    ChronosGraphStore.from_connection(conn).apply_facts(
        [NodeFact(node_id, subtype, {"text": node_id}, {"id": node_id}, {"v": 1})],
        source="default",
        strict_endpoints=True,
    )


def _publish(dsn: str) -> dict[str, Any]:
    from okg.substrate.consistency import StoredLiveNodesGate, TransformConsistencyGate
    from okg.substrate.publisher import Publisher
    from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer

    return Publisher(
        dsn, "default",
        consistency_gates=[TransformConsistencyGate(), StoredLiveNodesGate()],
        catalog_identity=bind_test_catalog_writer(dsn, "default"),
    ).cycle()


def _stamp(conn, metadata: dict[str, dict[str, Any]]) -> None:
    """Write live declarations onto the catalog rows AFTER a publish.

    Every catalog version's row of that name, as the call-tool tests do: the
    test writer clones the seeded catalog into its own version.
    """
    with conn.cursor() as cur:
        for name, body in metadata.items():
            cur.execute(
                "UPDATE okg.subtypes "
                "SET metadata = COALESCE(metadata, '{}'::jsonb) || %s::jsonb "
                "WHERE name = %s",
                (json.dumps(body), name),
            )


def _build(dsn: str, tmp_path: Path, monkeypatch, *, relations, live_types):
    from okg.substrate.mcp.server import build_server

    root = tmp_path / "deployments"
    d = root / "default"
    (d / "schemas").mkdir(parents=True)
    (d / "source_registry.yaml").write_text("sources: {}\n", encoding="utf-8")
    (d / "deployment.yaml").write_text(yaml.safe_dump({
        "name": "default",
        "description": "live types start fixture",
        "postgres": {"dsn": "${OKG_LIVETYPES_DSN}"},
        "schema_dir": "schemas",
        "source_registry": "source_registry.yaml",
        "modules": [],
        "enrichers": [],
        "agent_surface": {
            "served": ["inspect", "search", "expand", "filter", "map",
                       "aggregate"],
            "live_bindings": {
                name: {"kind": "http", "config": {"url": "http://localhost"}}
                for name in sorted(relations)
            },
            **({"live_types": live_types} if live_types else {}),
        },
    }, sort_keys=False), encoding="utf-8")
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    monkeypatch.setenv("OKG_LIVETYPES_DSN", dsn)
    adapter = _CountingAdapter()
    server = build_server(
        dsn=dsn, deployment_name="default", live_adapters={"http": adapter},
    )
    _BUILT.append(server)
    return server, adapter


def _relations(metadata) -> set[str]:
    return {r for body in metadata.values() for r in body.get("live_relations", {})}


@pytest.mark.postgres
def test_start_builds_the_registry_once(
    clean_runtime: str, tmp_path, monkeypatch, _close_servers,
) -> None:
    import psycopg

    metadata = _metadata(tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, metadata)
        _store(conn, f"page:{uuid.uuid4().hex[:8]}", "page")
    assert _publish(clean_runtime)["status"] == "published"

    server, adapter = _build(
        clean_runtime, tmp_path, monkeypatch,
        relations=_relations(metadata), live_types=["site", "system"],
    )
    registry = server._okg_live_types
    assert sorted(registry.types) == ["item", "site", "system"]
    assert registry.edge_set("site")[1].name == "sites"
    assert adapter.calls == []


@pytest.mark.postgres
@pytest.mark.parametrize("node_id,subtype", [
    ("site:Z1_BB_BETA_Cold", "page"),   # a live prefix on a stored subtype
    ("n:stored-site", "site"),          # a stored node of a live subtype
])
def test_start_refuses_a_stored_node_of_a_live_type_or_prefix(
    clean_runtime: str, tmp_path, monkeypatch, _close_servers, node_id, subtype,
) -> None:
    import psycopg

    metadata = _metadata(tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, None)   # the catalog has no live type yet
        _store(conn, node_id, subtype)
    assert _publish(clean_runtime)["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _stamp(conn, metadata)  # now the type is declared, the node is stored

    with pytest.raises(LiveTypeInvalid) as exc:
        _build(clean_runtime, tmp_path, monkeypatch,
               relations=_relations(metadata), live_types=["site"])
    assert exc.value.data["reason"] == "stored_node_uses_live_prefix"
    assert exc.value.data["node_id"] == node_id


@pytest.mark.postgres
def test_start_refuses_a_relation_declared_on_two_subtypes(
    clean_runtime: str, tmp_path, monkeypatch, _close_servers,
) -> None:
    import psycopg

    from okg.substrate.external_backed.live_dispatch import (
        LiveRelationDeclaredTwice,
    )

    metadata = _metadata(tmp_path)
    page_relations = {
        "example_ping": metadata["system"]["live_relations"]["example_ping"],
    }
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, metadata)
        _store(conn, f"page:{uuid.uuid4().hex[:8]}", "page")
    assert _publish(clean_runtime)["status"] == "published"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _stamp(conn, {"page": {"live_relations": page_relations}})

    with pytest.raises(LiveRelationDeclaredTwice) as exc:
        _build(clean_runtime, tmp_path, monkeypatch,
               relations=_relations(metadata), live_types=["site"])
    assert exc.value.data["relation"] == "example_ping"
    assert sorted(exc.value.data["subtypes"]) == ["page", "system"]


@pytest.mark.postgres
def test_start_refuses_a_served_type_that_is_not_live(
    clean_runtime: str, tmp_path, monkeypatch, _close_servers,
) -> None:
    import psycopg

    metadata = _metadata(tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, metadata)
        _store(conn, f"page:{uuid.uuid4().hex[:8]}", "page")
    assert _publish(clean_runtime)["status"] == "published"
    with pytest.raises(LiveTypeInvalid) as exc:
        _build(clean_runtime, tmp_path, monkeypatch,
               relations=_relations(metadata), live_types=["site", "page"])
    assert exc.value.data["unknown"] == ["page"]


#: The transform gate's check id, unchanged by live types.
_TRANSFORM_V4 = (
    "transform.consistency.no_dangling+narrowing+cardinality+"
    "derived_node_liveness.v4"
)


def _gate(out: dict[str, Any], name: str) -> dict[str, Any]:
    gates = [g for g in out["consistency_gates"] if g.get("gate_name") == name]
    assert gates, [g.get("gate_name") for g in out["consistency_gates"]]
    return gates[-1]


def _gate_details(out: dict[str, Any]) -> dict[str, Any]:
    return _gate(out, "stored_live_nodes_gate").get("details") or {}


@pytest.mark.postgres
def test_publish_blocks_a_generation_that_stores_live_nodes(
    clean_runtime: str, tmp_path,
) -> None:
    """Full sweep: the first publish of a catalog with live types."""
    import psycopg

    metadata = _metadata(tmp_path)
    by_prefix = "site:Z1_BB_BETA_Cold"
    by_subtype = f"n:{uuid.uuid4().hex[:8]}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, metadata)
        _store(conn, by_prefix, "page")
        _store(conn, by_subtype, "site")
        _store(conn, f"page:{uuid.uuid4().hex[:8]}", "page")
    out = _publish(clean_runtime)
    assert out["status"] == "blocked", out
    gate = _gate(out, "stored_live_nodes_gate")
    assert gate["check_id"] == "transform.consistency.no_stored_live_nodes.v1"
    assert gate["blocks_publish"] is True
    details = gate["details"]
    assert details["applicable"] is True and details["scoped"] is False
    assert details["stored_live_node_ids"] == sorted([by_prefix, by_subtype])
    assert details["stored_live_node_count"] == 2
    # The transform gate is the v4 check it was, and does not block here.
    transform = _gate(out, "transform_consistency_gate")
    assert transform["check_id"] == _TRANSFORM_V4
    assert transform["blocks_publish"] is False, transform


@pytest.mark.postgres
def test_publish_passes_and_then_blocks_the_dirty_live_node(
    clean_runtime: str, tmp_path,
) -> None:
    """A clean publish with live types declared, then an incremental one
    that adds a stored `site:` node: the scoped path blocks it too."""
    import psycopg

    metadata = _metadata(tmp_path)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, metadata)
        _store(conn, f"page:{uuid.uuid4().hex[:8]}", "page")
    first = _publish(clean_runtime)
    assert first["status"] == "published", first
    assert _gate_details(first)["stored_live_node_ids"] == []
    assert _gate_details(first)["scoped"] is False

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _store(conn, "site:Z2_AA_ALPHA", "page")
    second = _publish(clean_runtime)
    assert second["status"] == "blocked", second
    details = _gate_details(second)
    assert details["scoped"] is True, details
    assert details["stored_live_node_ids"] == ["site:Z2_AA_ALPHA"]


@pytest.mark.postgres
def test_a_deployment_with_no_live_types_publishes_with_no_extra_sweep(
    clean_runtime: str,
) -> None:
    """No live type: the check records "not applicable" (one catalog query,
    no sweep), and the transform gate keeps its v4 id and its incremental
    path, so no deployment pays a full sweep for this change."""
    import psycopg

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _seed(conn, None)
        _store(conn, f"page:{uuid.uuid4().hex[:8]}", "page")
    first = _publish(clean_runtime)
    assert first["status"] == "published", first
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _store(conn, f"page:{uuid.uuid4().hex[:8]}", "page")
    second = _publish(clean_runtime)
    assert second["status"] == "published", second
    for out in (first, second):
        gate = _gate(out, "stored_live_nodes_gate")
        assert gate["blocks_publish"] is False
        assert gate["details"]["applicable"] is False
        assert gate["details"]["note"] == "not applicable: no live types"
        assert "scope_mode" not in gate["details"], gate["details"]
        assert _gate(out, "transform_consistency_gate")["check_id"] == _TRANSFORM_V4
    transform = _gate(second, "transform_consistency_gate")["details"]
    assert transform["scoped"] is True, transform
    assert "scope_fallback_reason" not in transform, transform


@pytest.mark.parametrize("delta_counts,dirty,reason", [
    # No change counts: the dirty scope cannot be checked against what the
    # publish wrote, so the check sweeps everything, never "no change".
    (None, {}, "delta_counts_unavailable"),
    (None, {"default": ["site:Z2_AA_ALPHA"]}, "delta_counts_unavailable"),
    # Counts say nodes were written and the dirty scope names none.
    ({"nodes_added": 1}, {}, "dirty_node_scope_mismatch"),
    # Counts and scope agree: the scoped path (the basis check decides next).
    ({"nodes_added": 1}, {"default": ["site:Z2_AA_ALPHA"]}, None),
])
def test_the_scoped_gate_falls_back_to_a_full_sweep_without_change_counts(
    monkeypatch, delta_counts, dirty, reason,
) -> None:
    from okg.substrate.consistency import stored_live_nodes_gate as gate_module

    monkeypatch.setattr(
        gate_module, "_catalog_equivalence", lambda conn, **kw: {"equivalent": True},
    )
    gate = gate_module.StoredLiveNodesGate()
    monkeypatch.setattr(gate, "_basis_reason", lambda conn, **kw: None)
    got = gate._fallback_reason(  # noqa: SLF001
        None, deployment_name="default", branch_name="default",
        catalog_version_id=2, previous_catalog_version_id=1,
        delta_counts=delta_counts, dirty_scope=(dirty, None, True),
    )
    assert got == reason


@pytest.mark.parametrize("name,ok", [
    ("Z2_AA_ALPHA", True), ("Z2_AA_ALPHA\n", False), ("xZ2_AA_ALPHA", False), ("", False),
])
def test_one_name_rule_for_a_node_id_and_a_listed_name(tmp_path, name, ok) -> None:
    """A node id a caller passes and a name a listing returns are checked by
    one helper, a full match of the type's name pattern."""
    from okg.substrate.mcp.live_calls import name_checked

    registry = _registry(tmp_path)
    site = registry.by_subtype("site")
    assert name_checked(site, name) is ok
    if ok:
        assert registry.parse_node_id(f"site:{name}")[1] == (name,)
    else:
        with pytest.raises(LiveNodeIdInvalid):
            registry.parse_node_id(f"site:{name}")
