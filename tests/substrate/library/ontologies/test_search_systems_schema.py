"""The search_systems module: what it declares, and where.

PACT `opensearch-reference-integration` (issue #2014), req.ori.source.

The load-bearing test here is the placement one. `Service -[logs_to]->
SearchIndex` crosses module boundaries, and `modules_compose` validates
every narrowing endpoint against the COMPOSED subtype set. Declared inline
in this module's own EdgeNarrowings, the foreign `Service` class would make
`search_systems` refuse to compose in any deployment that does not also
load `service`. Declared as an `extends:` cross-link rule it is injected
when `service` is loaded and silently skipped when it is not -- so this
module has to compose both ways, and both ways are asserted.

No database, no container.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.catalog.relation_kind import classify_archetype


MODULE = (
    Path(__file__).resolve().parents[4]
    / "src/okg/substrate/library/ontologies/search_systems"
)


def _deployment(tmp_path: Path, modules: list[str]) -> Path:
    root = tmp_path / "-".join(modules)
    root.mkdir()
    (root / "deployment.yaml").write_text(
        yaml.safe_dump({"name": "search-systems-test", "modules": modules}),
        encoding="utf-8",
    )
    (root / "schemas").mkdir()
    return root


def test_the_module_composes_on_its_own(tmp_path: Path) -> None:
    """Without `service` loaded, and without raising."""
    composed = compose_catalog(_deployment(tmp_path, ["search_systems"]))

    assert "search_cluster" in composed.subtypes
    assert "search_index" in composed.subtypes
    assert "service" not in composed.subtypes
    triples = set(composed.narrowing_triples())
    assert ("search_cluster", "contains", "search_index") in triples
    assert not [t for t in triples if t[1] == "logs_to"]


def test_the_cross_module_narrowing_appears_when_service_is_loaded(
    tmp_path: Path,
) -> None:
    composed = compose_catalog(
        _deployment(tmp_path, ["service", "search_systems"]),
    )
    triples = set(composed.narrowing_triples())
    assert ("service", "logs_to", "search_index") in triples
    assert ("search_cluster", "contains", "search_index") in triples


def test_logs_to_is_classified_as_a_reference_not_containment() -> None:
    """A service writing to an index is lateral, so the hierarchy walk
    must not follow it. The module states this rather than inheriting the
    default, because a default nobody reads is not a decision."""
    schema = yaml.safe_load((MODULE / "schema.yaml").read_text())
    annotations = schema["classes"]["EdgeNarrowings"]["annotations"]
    overrides = annotations["okg_relation_kind"]

    assert overrides["logs_to"] == "references"
    assert classify_archetype("logs_to", overrides) == "references"
    assert classify_archetype("contains", overrides) == "contains"


def test_the_foreign_service_class_is_not_declared_inline() -> None:
    """The regression guard for the placement itself.

    Moving `service_logs_to_search_index` back into this module's
    EdgeNarrowings would pass every other test in this file while
    breaking any deployment that loads `search_systems` without
    `service`, so the absence is asserted directly.
    """
    schema = yaml.safe_load((MODULE / "schema.yaml").read_text())
    narrowings = schema["classes"]["EdgeNarrowings"]["attributes"]
    for name, body in narrowings.items():
        endpoints = body["annotations"]
        assert "Service" not in str(endpoints.get("src_subtypes", "")), name
        assert "Service" not in str(endpoints.get("dst_subtypes", "")), name

    manifest = yaml.safe_load((MODULE / "module.yaml").read_text())
    rules = manifest["extends"]["service"]["cross_link_rules"]
    assert rules == ["cross_links/search_systems_x_service.yaml"]
    assert (MODULE / rules[0]).is_file()


def test_the_cross_link_rule_matches_the_field_the_source_writes() -> None:
    """The source emits the logs_to edge itself; the rule registers the
    narrowing and matches the same field pair, so a linker-derived edge is
    the identical triple rather than a second, competing one."""
    rule_body = yaml.safe_load(
        (MODULE / "cross_links/search_systems_x_service.yaml").read_text(),
    )
    rule = rule_body["rules"][0]
    assert rule["edge"] == "logs_to"
    assert rule["src"] == {"subtype": "service", "field": "service_id"}
    assert rule["dst"] == {"subtype": "search_index", "field": "logged_by_refs"}


@pytest.mark.parametrize(
    "attribute",
    ["index_uuid", "field_summary", "field_count", "field_summary_truncated",
     "retention", "retention_read", "doc_count", "logged_by_refs"],
)
def test_search_index_declares_the_attributes_the_source_emits(
    attribute: str,
) -> None:
    schema = yaml.safe_load((MODULE / "schema.yaml").read_text())
    assert attribute in schema["classes"]["SearchIndex"]["attributes"]


# ── the cross-link rule must be able to fire ──────────────────────────

def test_the_rule_matches_attributes_both_modules_actually_declare() -> None:
    """A rule naming a field nobody declares can never fire.

    The first version matched `service.declared_service_id`, and
    `service/schema.yaml` declares only `service_id` and `name`. Any
    deployment that ran a linker over this rule would have derived
    nothing, silently.
    """
    rule = yaml.safe_load(
        (MODULE / "cross_links/search_systems_x_service.yaml").read_text(),
    )["rules"][0]
    service_schema = yaml.safe_load(
        (MODULE.parent / "service/schema.yaml").read_text(),
    )
    search_schema = yaml.safe_load((MODULE / "schema.yaml").read_text())

    assert rule["src"]["field"] in (
        service_schema["classes"]["Service"]["attributes"]
    )
    assert rule["dst"]["field"] in (
        search_schema["classes"]["SearchIndex"]["attributes"]
    )


def test_the_rules_two_sides_carry_comparable_values() -> None:
    """Declared fields are not enough: an `exact` matcher compares VALUES.

    `service.service_id` holds a namespaced node id, so
    `search_index.logged_by_refs` must hold node ids too. It held bare
    declared names before, which would have matched nothing even once the
    field names lined up.
    """
    from okg.substrate.library.sources._opensearch_client import (
        RecordedMetadataProvider,
    )
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.library.sources.opensearch_catalog import (
        OpenSearchCatalogSource,
    )

    fixtures = (
        Path(__file__).resolve().parents[4]
        / "tests/substrate/library/sources/opensearch_fixtures/baseline"
    )
    source = OpenSearchCatalogSource(
        provider=RecordedMetadataProvider(
            root_dir=fixtures, excluded_globs=("top_queries-*",),
        ),
        namespace="osref",
        services=[{
            "service_id": "okg-api", "name": "OKG API",
            "indices": ["app-logs-2026.09", "audit-trail-2026.09"],
        }],
    )
    nodes = [
        f for f in source.run("r", mode="reconcile").facts
        if isinstance(f, NodeFact)
    ]
    rule = yaml.safe_load(
        (MODULE / "cross_links/search_systems_x_service.yaml").read_text(),
    )["rules"][0]

    src_values = {
        n.attrs[rule["src"]["field"]]
        for n in nodes if n.subtype == "service"
    }
    dst_values = {
        value
        for n in nodes if n.subtype == "search_index"
        for value in n.attrs[rule["dst"]["field"]]
    }
    assert src_values, "no service nodes emitted"
    assert dst_values, "no logged_by_refs emitted"
    assert dst_values <= src_values, (dst_values, src_values)


def test_the_human_readable_service_id_is_kept_separately() -> None:
    """The match field is not overloaded: a reader still gets names."""
    schema = yaml.safe_load((MODULE / "schema.yaml").read_text())
    attributes = schema["classes"]["SearchIndex"]["attributes"]
    assert "logged_by" in attributes
    assert "logged_by_refs" in attributes
