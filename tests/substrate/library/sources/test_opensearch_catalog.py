"""OpenSearchCatalogSource against recorded cluster responses.

PACT `opensearch-reference-integration` (issue #2014), req.ori.source.

Every test here runs in hosted CI with no container and no database. The
fixtures under `opensearch_fixtures/baseline/` are response bodies shaped
like a real OpenSearch 2.x cluster's, and both the live provider and the
recorded one feed the same parser, so these exercise the production path
rather than a test-only branch.

Variants are derived from the baseline bodies by changing exactly one
thing, which is what makes each assertion mean something: if the only
difference between two fixtures is the document count, then a revision
that changed can only have changed because of the document count.
"""
from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.library.sources._opensearch_client import (
    MAX_SUMMARY_BYTES,
    MAX_SUMMARY_FIELDS,
    OpenSearchMetadataError,
    RecordedMetadataProvider,
    summarize_mapping,
)
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.opensearch_catalog import (
    DeclaredServiceError,
    OpenSearchCatalogSource,
)


FIXTURES = Path(__file__).parent / "opensearch_fixtures" / "baseline"

# Recorded from the real cluster, not invented. The fixtures under
# `opensearch_fixtures/baseline/` are the verbatim bodies of six curl
# calls against the digest-pinned 2.19.1 image after `seed.sh` ran
# (2026-09-20), pretty-printed and nothing else. The first hand-written
# set was the right SHAPE and wrong in eight places -- it omitted
# `root.version`'s build fields, ten `health` keys, five per-index
# settings keys, and two of the three plugin indices the image actually
# creates -- which is why these are a recording now.
CLUSTER_UUID = "IX9AxNMSQde5Lf5GaS5RSA"
APP_LOGS_UUID = "fekz5gsFSFenauLOAxodUA"
EVENTS_UUID = "vc4aScx4QLmVi8MfO42aeA"
AUDIT_UUID = "CISD7BoLSOqCGTtSDXwiCA"

#: The query-insights plugin creates `top_queries-<date>-<n>` with NO
#: leading dot, so the default dot-prefix rule does not exclude it. The
#: deployment declares this glob; the tests use the same one.
EXCLUDE_GLOBS = ("top_queries-*",)

SERVICES = [
    {
        "service_id": "okg-api",
        "name": "OKG API",
        "indices": ["app-logs-2026.09", "audit-trail-2026.09"],
    },
    {
        "service_id": "okg-ingest",
        "name": "OKG Ingest",
        "indices": ["ingest-events-2026.09", "audit-trail-2026.09"],
    },
]


def _bodies() -> dict[str, Any]:
    """The baseline fixture bodies, loaded for mutation."""
    return {
        path.stem: json.loads(path.read_text(encoding="utf-8"))
        for path in FIXTURES.glob("*.json")
    }


def _source(**overrides: Any) -> OpenSearchCatalogSource:
    provider = RecordedMetadataProvider(
        root_dir=FIXTURES,
        overrides=dict(overrides),
        excluded_globs=EXCLUDE_GLOBS,
    )
    return OpenSearchCatalogSource(
        provider=provider, namespace="osref", services=SERVICES,
    )


def _facts(source: OpenSearchCatalogSource) -> list[Any]:
    return list(source.run("run-1", mode="reconcile").facts)


def _nodes(facts: list[Any]) -> list[NodeFact]:
    return [f for f in facts if isinstance(f, NodeFact)]


def _edges(facts: list[Any]) -> list[EdgeFact]:
    return [f for f in facts if isinstance(f, EdgeFact)]


def _node(facts: list[Any], node_id: str) -> NodeFact:
    for fact in _nodes(facts):
        if fact.node_id == node_id:
            return fact
    raise AssertionError(f"no node {node_id!r} in {[n.node_id for n in _nodes(facts)]}")


# ── 1. golden emission ────────────────────────────────────────────────

def test_baseline_emits_the_expected_nodes_and_edges() -> None:
    facts = _facts(_source())
    nodes, edges = _nodes(facts), _edges(facts)

    assert len(nodes) == 6, [n.node_id for n in nodes]
    assert len(edges) == 7, [(e.src, e.edge_type, e.dst) for e in edges]

    by_subtype: dict[str, list[str]] = {}
    for node in nodes:
        by_subtype.setdefault(node.subtype, []).append(node.node_id)
    assert sorted(by_subtype) == ["search_cluster", "search_index", "service"]
    assert by_subtype["search_cluster"] == [f"osref:search_cluster:{CLUSTER_UUID}"]
    assert sorted(by_subtype["search_index"]) == sorted([
        f"osref:search_index:{APP_LOGS_UUID}",
        f"osref:search_index:{EVENTS_UUID}",
        f"osref:search_index:{AUDIT_UUID}",
    ])
    assert sorted(by_subtype["service"]) == [
        "osref:service:okg-api", "osref:service:okg-ingest",
    ]

    triples = {(e.src, e.edge_type, e.dst) for e in edges}
    cluster = f"osref:search_cluster:{CLUSTER_UUID}"
    assert triples == {
        (cluster, "contains", f"osref:search_index:{APP_LOGS_UUID}"),
        (cluster, "contains", f"osref:search_index:{EVENTS_UUID}"),
        (cluster, "contains", f"osref:search_index:{AUDIT_UUID}"),
        ("osref:service:okg-api", "logs_to", f"osref:search_index:{APP_LOGS_UUID}"),
        ("osref:service:okg-api", "logs_to", f"osref:search_index:{AUDIT_UUID}"),
        ("osref:service:okg-ingest", "logs_to", f"osref:search_index:{EVENTS_UUID}"),
        ("osref:service:okg-ingest", "logs_to", f"osref:search_index:{AUDIT_UUID}"),
    }


def test_index_attributes_carry_the_modelled_metadata() -> None:
    facts = _facts(_source())
    app_logs = _node(facts, f"osref:search_index:{APP_LOGS_UUID}")

    assert app_logs.attrs["name"] == "app-logs-2026.09"
    assert app_logs.attrs["index_uuid"] == APP_LOGS_UUID
    assert app_logs.attrs["aliases"] == ["app-logs"]
    assert app_logs.attrs["doc_count"] == 40
    assert app_logs.attrs["store_bytes"] == 10076
    assert app_logs.attrs["field_count"] == 9
    assert app_logs.attrs["field_summary_truncated"] is False
    assert app_logs.attrs["retention"] == "okg-logs-30d"
    assert app_logs.attrs["retention_read"] is True
    # Node ids, because that is what `service.service_id` holds on the
    # other side of the cross-link rule's exact match.
    assert app_logs.attrs["logged_by_refs"] == ["osref:service:okg-api"]
    assert app_logs.attrs["logged_by"] == ["okg-api"]

    names = [f["name"] for f in app_logs.attrs["field_summary"]]
    assert names == sorted(names)
    # A nested `http` object flattens to dotted paths; there is no bare
    # `http` entry standing in for its children.
    assert "http.status" in names and "http" not in names


def test_system_indices_are_out_of_declared_scope() -> None:
    """A dot-prefixed index is excluded by declared scope, not by accident.

    OpenSearch ships bookkeeping indices whose presence depends on which
    plugins booted. Ingesting them would make the node count a function of
    the image rather than of the operator's data.
    """
    facts = _facts(_source())
    names = [
        n.attrs["name"] for n in _nodes(facts) if n.subtype == "search_index"
    ]
    for excluded in (
        ".plugins-ml-config", ".opensearch-observability",
        "top_queries-2026.09.20-04119",
    ):
        assert excluded not in names, excluded
    assert len(names) == 3


# ── 2. the bounded mapping summary ────────────────────────────────────

def test_mapping_summary_is_bounded_in_every_declared_dimension() -> None:
    """300 fields, seven levels deep, with one absurdly long name."""
    properties: dict[str, Any] = {
        f"field_{i:03d}": {"type": "keyword"} for i in range(300)
    }
    deep: dict[str, Any] = {"type": "keyword"}
    for level in range(7, 0, -1):
        deep = {"properties": {f"lvl{level}": deep}}
    properties["aaa_deep"] = deep["properties"]["lvl1"]
    properties["aaa_" + ("x" * 400)] = {"type": "text"}

    summary = summarize_mapping({"mappings": {"properties": properties}})

    assert summary.total > 300
    assert len(summary.fields) <= MAX_SUMMARY_FIELDS
    assert summary.truncated is True
    rendered = json.dumps(summary.as_list(), separators=(",", ":"))
    assert len(rendered.encode("utf-8")) <= MAX_SUMMARY_BYTES
    assert all(len(f.name) <= 128 for f in summary.fields)
    assert all(f.name.count(".") < 4 for f in summary.fields)
    # A subtree below the depth limit contributes its ancestor once,
    # typed `object`, rather than vanishing without trace.
    assert any(
        f.type == "object" and f.name.startswith("aaa_deep")
        for f in summary.fields
    )


# ── 3/4. what mints a revision, and what must not ─────────────────────

def _revision(facts: list[Any], node_id: str) -> str:
    return _node(facts, node_id).source_revision["content_hash"]


def test_a_moved_document_count_does_not_mint_a_new_revision() -> None:
    """The churn guard.

    `doc_count` and `store_bytes` move on every write to a live cluster.
    If they were hashed, every sync would mint a fresh revision for every
    node and the graph would churn with no structural change. The
    attribute assertion is part of the test on purpose: a source that
    dropped the counts entirely would also keep the revision stable, and
    that would be a different bug passing the same check.
    """
    bodies = _bodies()
    moved = copy.deepcopy(bodies["cat_indices"])
    for row in moved:
        if row["index"] == "app-logs-2026.09":
            row["docs.count"] = "93412"
            row["store.size"] = "91822104"

    before = _facts(_source())
    after = _facts(_source(cat_indices=moved))
    node_id = f"osref:search_index:{APP_LOGS_UUID}"

    assert _revision(after, node_id) == _revision(before, node_id)
    assert _node(after, node_id).attrs["doc_count"] == 93412
    assert _node(after, node_id).attrs["store_bytes"] == 91822104


def test_cluster_health_does_not_mint_a_new_revision() -> None:
    bodies = _bodies()
    flapped = copy.deepcopy(bodies["health"])
    flapped["status"] = "yellow"

    before = _facts(_source())
    after = _facts(_source(health=flapped))
    node_id = f"osref:search_cluster:{CLUSTER_UUID}"

    assert _revision(after, node_id) == _revision(before, node_id)
    assert _node(after, node_id).attrs["health"] == "yellow"


def test_an_added_mapping_field_does_mint_a_new_revision() -> None:
    bodies = _bodies()
    changed = copy.deepcopy(bodies["mapping"])
    changed["app-logs-2026.09"]["mappings"]["properties"]["span_id"] = {
        "type": "keyword",
    }

    before = _facts(_source())
    after = _facts(_source(mapping=changed))
    node_id = f"osref:search_index:{APP_LOGS_UUID}"

    assert _revision(after, node_id) != _revision(before, node_id)
    assert _node(after, node_id).attrs["field_count"] == 10


def test_two_reads_of_an_unchanged_cluster_agree_on_every_revision() -> None:
    first = _facts(_source())
    second = _facts(_source())
    assert {n.node_id: n.source_revision for n in _nodes(first)} == {
        n.node_id: n.source_revision for n in _nodes(second)
    }


# ── 5/6/7. rollover, rename, delete ───────────────────────────────────

def test_a_rollover_adds_a_node_and_retracts_nothing() -> None:
    """A new UUID behind the same alias is a new index, not a changed one."""
    bodies = _bodies()
    rolled = copy.deepcopy(bodies["cat_indices"])
    rolled.append({
        "index": "app-logs-2026.10", "uuid": "e4Bn3At2UwRdM8qI5sThOz",
        "docs.count": "3", "store.size": "5120",
    })
    mapping = copy.deepcopy(bodies["mapping"])
    mapping["app-logs-2026.10"] = mapping["app-logs-2026.09"]
    alias = copy.deepcopy(bodies["alias"])
    alias["app-logs-2026.09"] = {"aliases": {}}
    alias["app-logs-2026.10"] = {"aliases": {"app-logs": {}}}
    settings = copy.deepcopy(bodies["settings"])
    settings["app-logs-2026.10"] = settings["app-logs-2026.09"]

    facts = _facts(_source(
        cat_indices=rolled, mapping=mapping, alias=alias, settings=settings,
    ))
    index_ids = {
        n.node_id for n in _nodes(facts) if n.subtype == "search_index"
    }
    assert f"osref:search_index:{APP_LOGS_UUID}" in index_ids
    assert "osref:search_index:e4Bn3At2UwRdM8qI5sThOz" in index_ids
    assert len(index_ids) == 4
    assert all(f.op == "I" for f in _nodes(facts))
    assert _node(facts, f"osref:search_index:{APP_LOGS_UUID}").attrs[
        "aliases"] == []


def test_a_rename_keeps_the_node_and_changes_one_attribute() -> None:
    bodies = _bodies()
    renamed = copy.deepcopy(bodies["cat_indices"])
    for row in renamed:
        if row["uuid"] == APP_LOGS_UUID:
            row["index"] = "application-logs-2026.09"
    mapping = copy.deepcopy(bodies["mapping"])
    mapping["application-logs-2026.09"] = mapping.pop("app-logs-2026.09")
    alias = copy.deepcopy(bodies["alias"])
    alias["application-logs-2026.09"] = alias.pop("app-logs-2026.09")
    settings = copy.deepcopy(bodies["settings"])
    settings["application-logs-2026.09"] = settings.pop("app-logs-2026.09")

    services = [
        {**SERVICES[0], "indices": [
            "application-logs-2026.09", "audit-trail-2026.09",
        ]},
        SERVICES[1],
    ]
    provider = RecordedMetadataProvider(
        root_dir=FIXTURES,
        overrides={
            "cat_indices": renamed, "mapping": mapping,
            "alias": alias, "settings": settings,
        },
        excluded_globs=EXCLUDE_GLOBS,
    )
    facts = list(
        OpenSearchCatalogSource(
            provider=provider, namespace="osref", services=services,
        ).run("run-1", mode="reconcile").facts
    )

    node = _node(facts, f"osref:search_index:{APP_LOGS_UUID}")
    assert node.attrs["name"] == "application-logs-2026.09"
    assert len([n for n in _nodes(facts) if n.subtype == "search_index"]) == 3


def test_a_full_scope_run_declares_its_record_set_so_a_delete_retracts() -> None:
    """Absence from a COMPLETED scope is what retracts, so the run must
    declare its footprint and must say the scope completed."""
    bodies = _bodies()
    fewer = [
        row for row in copy.deepcopy(bodies["cat_indices"])
        if row["uuid"] != EVENTS_UUID
    ]
    services = [SERVICES[0], {**SERVICES[1], "indices": [
        "audit-trail-2026.09",
    ]}]
    provider = RecordedMetadataProvider(
        root_dir=FIXTURES, overrides={"cat_indices": fewer},
        excluded_globs=EXCLUDE_GLOBS,
    )
    run = OpenSearchCatalogSource(
        provider=provider, namespace="osref", services=services,
    ).run("run-2", mode="reconcile")
    facts = list(run.facts)

    assert run.completed_scope is True
    assert run.record_set is not None
    assert f"osref:search_index:{EVENTS_UUID}" not in run.record_set.records
    assert f"osref:search_index:{APP_LOGS_UUID}" in run.record_set.records
    assert len([n for n in _nodes(facts) if n.subtype == "search_index"]) == 2


def test_the_record_set_covers_every_emitted_fact() -> None:
    run = _source().run("run-1", mode="reconcile")
    facts = list(run.facts)
    assert run.record_set is not None
    for node in _nodes(facts):
        assert node.node_id in run.record_set.records
    for edge in _edges(facts):
        key = f"edge:{edge.src}|{edge.edge_type}|{edge.dst}"
        assert key in run.record_set.records


# ── 8. retention is a tri-state ───────────────────────────────────────

def test_retention_distinguishes_a_policy_an_absent_policy_and_an_unread_one() -> None:
    """Null must not stand in for unknown.

    Rendering an unread setting as "no retention policy" would assert
    something on no evidence, which is the fallback this repository
    refuses.
    """
    facts = _facts(_source())
    with_policy = _node(facts, f"osref:search_index:{APP_LOGS_UUID}")
    without_policy = _node(facts, f"osref:search_index:{AUDIT_UUID}")

    assert with_policy.attrs["retention"] == "okg-logs-30d"
    assert with_policy.attrs["retention_read"] is True
    assert without_policy.attrs["retention"] is None
    assert without_policy.attrs["retention_read"] is True

    # The settings call itself failed: every index reads unknown.
    unread = _facts(_source(settings=None))
    for node in _nodes(unread):
        if node.subtype != "search_index":
            continue
        assert node.attrs["retention"] is None
        assert node.attrs["retention_read"] is False


# ── 9. failure cases ──────────────────────────────────────────────────

def test_a_declared_index_the_cluster_lacks_is_refused_by_name() -> None:
    provider = RecordedMetadataProvider(
        root_dir=FIXTURES, excluded_globs=EXCLUDE_GLOBS,
    )
    source = OpenSearchCatalogSource(
        provider=provider,
        namespace="osref",
        services=[{
            "service_id": "okg-api", "name": "OKG API",
            "indices": ["app-logs-2026.09", "app-logz-typo"],
        }],
    )
    with pytest.raises(DeclaredServiceError) as excinfo:
        list(source.run("run-1", mode="reconcile").facts)
    message = str(excinfo.value)
    assert "app-logz-typo" in message
    assert "okg-api" in message


def test_an_unreadable_index_listing_raises_instead_of_emptying_the_scope() -> None:
    """The retraction trap.

    Under `missing_from_completed_scope`, a completed scope with nothing
    in it retracts every node this source owns. One network timeout must
    therefore not be allowed to read as a cluster-wide deletion.
    """
    with pytest.raises(OpenSearchMetadataError):
        _facts(_source(cat_indices={"error": "search_phase_execution"}))


def test_an_index_row_without_a_uuid_is_refused() -> None:
    """The UUID is this source's identity key; guessing one is not an option."""
    bodies = _bodies()
    broken = copy.deepcopy(bodies["cat_indices"])
    del broken[0]["uuid"]
    with pytest.raises(OpenSearchMetadataError) as excinfo:
        _facts(_source(cat_indices=broken))
    assert "uuid" in str(excinfo.value)


def test_a_root_response_without_a_cluster_uuid_is_refused() -> None:
    with pytest.raises(OpenSearchMetadataError):
        _facts(_source(root={"tagline": "not an OpenSearch root"}))


def test_a_service_entry_without_an_id_is_refused() -> None:
    with pytest.raises(DeclaredServiceError):
        OpenSearchCatalogSource(
            provider=RecordedMetadataProvider(
                root_dir=FIXTURES, excluded_globs=EXCLUDE_GLOBS,
            ),
            namespace="osref",
            services=[{"name": "nameless", "indices": []}],
        )


# ── 10. the change probe ──────────────────────────────────────────────

def test_the_probe_short_circuits_an_unchanged_cluster() -> None:
    source = _source()
    first = source.change_probe.probe(cursor=None, sync_scope=None)
    assert first.changed is True

    second = source.change_probe.probe(
        cursor={"change_probe_token": first.token}, sync_scope=None,
    )
    assert second.changed is False


def test_the_probe_does_notice_a_moved_document_count() -> None:
    """The revision hash ignores volatile metrics; the probe must not.

    A short circuit claims "identical emission". If the probe ignored the
    document counts, it would skip exactly the run that was meant to
    refresh them, and the graph would serve a stale figure while reporting
    itself up to date.
    """
    bodies = _bodies()
    moved = copy.deepcopy(bodies["cat_indices"])
    for row in moved:
        if row["index"] == "app-logs-2026.09":
            row["docs.count"] = "93412"

    baseline = _source().change_probe.probe(cursor=None, sync_scope=None)
    verdict = _source(cat_indices=moved).change_probe.probe(
        cursor={"change_probe_token": baseline.token}, sync_scope=None,
    )
    assert verdict.changed is True


def test_the_probe_notices_a_deleted_index() -> None:
    bodies = _bodies()
    fewer = [
        row for row in copy.deepcopy(bodies["cat_indices"])
        if row["uuid"] != EVENTS_UUID
    ]
    services = [SERVICES[0], {**SERVICES[1], "indices": ["audit-trail-2026.09"]}]
    baseline = _source().change_probe.probe(cursor=None, sync_scope=None)
    provider = RecordedMetadataProvider(
        root_dir=FIXTURES, overrides={"cat_indices": fewer},
        excluded_globs=EXCLUDE_GLOBS,
    )
    verdict = OpenSearchCatalogSource(
        provider=provider, namespace="osref", services=services,
    ).change_probe.probe(
        cursor={"change_probe_token": baseline.token}, sync_scope=None,
    )
    assert verdict.changed is True


def test_the_source_declares_a_sound_probe_kind() -> None:
    from okg.substrate.library.sources.change_probe import SOUND_PROBE_KINDS

    assert OpenSearchCatalogSource.change_probe_kind in SOUND_PROBE_KINDS
    assert _source().probe_short_circuit_safe is True


# ── 11. where the metadata comes from is never guessed ────────────────

def test_naming_no_source_of_metadata_is_refused() -> None:
    from okg.substrate.errors import SubstrateError

    with pytest.raises(SubstrateError) as excinfo:
        OpenSearchCatalogSource(namespace="osref")
    assert "base_url" in str(excinfo.value)


def test_naming_two_sources_of_metadata_is_refused() -> None:
    """A source that silently picked one would make its own data
    provenance a guess."""
    from okg.substrate.errors import SubstrateError

    with pytest.raises(SubstrateError):
        OpenSearchCatalogSource(
            namespace="osref",
            base_url="http://127.0.0.1:9209",
            fixture_dir=str(FIXTURES),
        )


def test_a_fixture_dir_builds_the_recorded_provider() -> None:
    source = OpenSearchCatalogSource(
        namespace="osref", fixture_dir=str(FIXTURES), services=SERVICES,
        index_exclude_globs=EXCLUDE_GLOBS,
    )
    facts = list(source.run("run-1", mode="reconcile").facts)
    assert len(_nodes(facts)) == 6
    assert len(_edges(facts)) == 7


def test_a_base_url_builds_the_http_provider_without_calling_it() -> None:
    from okg.substrate.library.sources._opensearch_client import (
        HttpMetadataProvider,
    )

    source = OpenSearchCatalogSource(
        namespace="osref", base_url="http://127.0.0.1:9209",
    )
    assert isinstance(source.provider, HttpMetadataProvider)
    assert source.provider.base_url == "http://127.0.0.1:9209"


# ── 12. what the declared scope dropped is visible ────────────────────

def test_the_cluster_node_reports_what_the_scope_excluded() -> None:
    """An index that leaves the graph must leave a trace.

    The pinned image holds six indices and the graph models three. A
    reader who counts three has to be able to see the rule that made the
    difference, or the source is quietly lying about the cluster.
    """
    facts = _facts(_source())
    cluster = _node(facts, f"osref:search_cluster:{CLUSTER_UUID}")

    assert cluster.attrs["index_count"] == 3
    assert cluster.attrs["excluded_index_count"] == 3
    assert cluster.attrs["excluded_index_globs"] == list(EXCLUDE_GLOBS)


def test_a_widened_scope_mints_a_new_cluster_revision() -> None:
    """The declared globs are configuration, so they belong in the hash.

    The excluded COUNT does not: a plugin minting a dated index every day
    would otherwise churn the cluster's history for nothing.
    """
    narrow = _facts(_source())
    wide = list(
        OpenSearchCatalogSource(
            namespace="osref", fixture_dir=str(FIXTURES), services=SERVICES,
            # A wider rule that still leaves every declared service's
            # index in scope, so this measures the revision and not a
            # refused declaration.
            index_exclude_globs=("top_queries-*", "no-such-index-*"),
        ).run("run-1", mode="reconcile").facts
    )
    node_id = f"osref:search_cluster:{CLUSTER_UUID}"
    assert _revision(wide, node_id) != _revision(narrow, node_id)


def test_a_non_dot_plugin_index_needs_the_declared_glob() -> None:
    """Without the glob, `top_queries-...` would be modelled as a real
    index: it carries no leading dot, and `index.hidden` does not separate
    plugin indices from operator ones either."""
    unguarded = list(
        OpenSearchCatalogSource(
            namespace="osref", fixture_dir=str(FIXTURES), services=SERVICES,
        ).run("run-1", mode="reconcile").facts
    )
    names = [
        n.attrs["name"] for n in _nodes(unguarded)
        if n.subtype == "search_index"
    ]
    assert "top_queries-2026.09.20-04119" in names
    assert len(names) == 4


# ── 13. an unreadable listing is never an empty cluster ───────────────

def test_a_zero_row_index_listing_is_refused() -> None:
    """The retraction trap, from the other side.

    A running OpenSearch always holds at least one index of its own, so a
    zero-row listing means the read was filtered by permissions or the
    cluster is mid-restore. Accepting it would complete the scope with
    nothing in it and retract every index node the source owns.
    """
    with pytest.raises(OpenSearchMetadataError) as excinfo:
        _facts(_source(cat_indices=[]))
    assert "zero rows" in str(excinfo.value)


def test_a_listing_of_only_out_of_scope_indices_yields_no_indices() -> None:
    """Decided explicitly, and it is NOT the same case.

    The rows were read; the declared scope excluded all of them. That is a
    complete answer about a cluster holding nothing the operator asked
    for, so the scope completes, no index node is emitted, and the cluster
    node reports what was dropped. A source that refused here would be
    unable to model a cluster whose operator indices have all been
    deleted.
    """
    bodies = _bodies()
    system_only = [
        row for row in copy.deepcopy(bodies["cat_indices"])
        if row["index"].startswith(".") or row["index"].startswith("top_")
    ]
    assert system_only, "fixture must carry system indices for this test"

    provider = RecordedMetadataProvider(
        root_dir=FIXTURES,
        overrides={"cat_indices": system_only},
        excluded_globs=EXCLUDE_GLOBS,
    )
    run = OpenSearchCatalogSource(
        provider=provider, namespace="osref", services=[],
    ).run("run-1", mode="reconcile")
    facts = list(run.facts)

    assert run.completed_scope is True
    assert [n for n in _nodes(facts) if n.subtype == "search_index"] == []
    cluster = _node(facts, f"osref:search_cluster:{CLUSTER_UUID}")
    assert cluster.attrs["index_count"] == 0
    assert cluster.attrs["excluded_index_count"] == len(system_only)


# ── 14. a filtered mapping is not an empty mapping ────────────────────

def test_a_mapping_response_missing_an_index_reads_as_not_read() -> None:
    """A 200 that omits an in-scope index must not claim zero fields.

    Summarising the absence would report field_count 0 AND mint a new
    content hash claiming the mapping changed -- a graph lying about a
    cluster nobody touched. The revision must be governed by
    `mapping_read`, so the two assertions below are both load-bearing.
    """
    bodies = _bodies()
    filtered = copy.deepcopy(bodies["mapping"])
    del filtered["app-logs-2026.09"]

    facts = _facts(_source(mapping=filtered))
    node = _node(facts, f"osref:search_index:{APP_LOGS_UUID}")

    assert node.attrs["mapping_read"] is False
    assert node.attrs["field_count"] == 0

    # And it is visibly a DIFFERENT state from a real empty mapping, so a
    # reader is never told "no fields" on no evidence.
    empty = copy.deepcopy(bodies["mapping"])
    empty["app-logs-2026.09"] = {"mappings": {"properties": {}}}
    read_empty = _node(
        _facts(_source(mapping=empty)),
        f"osref:search_index:{APP_LOGS_UUID}",
    )
    assert read_empty.attrs["mapping_read"] is True
    assert read_empty.attrs["field_count"] == 0
    assert _revision(_facts(_source(mapping=filtered)),
                     f"osref:search_index:{APP_LOGS_UUID}") != _revision(
        _facts(_source(mapping=empty)), f"osref:search_index:{APP_LOGS_UUID}")


# ── 15. nothing is dropped without a count ────────────────────────────

def test_malformed_rows_and_fields_are_counted_not_silently_dropped() -> None:
    """Every `continue` in the parser now leaves a number behind.

    A row the source declines to use is a graph quietly disagreeing with
    its cluster. These were all silent skips.
    """
    bodies = _bodies()
    rows = copy.deepcopy(bodies["cat_indices"])
    rows.append("not-a-row")
    rows.append(["also", "not", "a", "row"])
    for row in rows:
        if isinstance(row, dict) and row["index"] == "app-logs-2026.09":
            row["docs.count"] = "not-a-number"

    mapping = copy.deepcopy(bodies["mapping"])
    props = mapping["audit-trail-2026.09"]["mappings"]["properties"]
    props["broken"] = "not-an-object"

    facts = _facts(_source(cat_indices=rows, mapping=mapping))
    cluster = _node(facts, f"osref:search_cluster:{CLUSTER_UUID}")
    anomalies = cluster.attrs["read_anomalies"]

    assert anomalies["malformed_index_rows"] == 2
    assert anomalies["unparsable_metrics"] == ["app-logs-2026.09"]
    assert anomalies["malformed_mapping_fields"] == 1
    assert cluster.attrs["read_anomaly_count"] == 4

    # The index with the unparsable count is still modelled; only the
    # metric reads unknown.
    app_logs = _node(facts, f"osref:search_index:{APP_LOGS_UUID}")
    assert app_logs.attrs["doc_count"] is None
    assert app_logs.attrs["name"] == "app-logs-2026.09"


def test_a_truncated_field_name_collision_is_counted() -> None:
    """Two fields cut to the same 128 characters would merge in silence."""
    stem = "f" * 200
    properties = {f"{stem}a": {"type": "keyword"}, f"{stem}b": {"type": "long"}}
    counters: dict[str, int] = {}
    summary = summarize_mapping(
        {"mappings": {"properties": properties}}, counters,
    )

    assert counters["truncated_field_names"] == 2
    assert counters["truncated_name_collisions"] == 1
    assert len({f.name for f in summary.fields}) == 1


def test_multi_fields_are_not_counted_as_separate_fields() -> None:
    """`title.keyword` beside a `text` title is an alternate analysis of
    one field, not a second field; counting it would overstate
    field_count. Recorded as a decision, with a test, rather than left to
    be inferred from a branch that does not exist."""
    properties = {
        "title": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword"}},
        },
    }
    summary = summarize_mapping({"mappings": {"properties": properties}})
    assert summary.total == 1
    assert [f.name for f in summary.fields] == ["title"]


# ── 16. the run mode the profile actually has a policy for ────────────

@pytest.mark.parametrize(
    "caller_mode", ["cursor", "scope_complete", "reconcile", "", "anything"],
)
def test_a_generic_runner_mode_becomes_a_catalog_release_mode(
    caller_mode: str,
) -> None:
    """Echoing the caller's mode broke a real ingest.

    The runner maps (profile, run_mode) onto a retraction-safety policy.
    `reference_catalog` has policies for `release_new` and
    `release_unchanged` only, so an adapter that echoed `scope_complete`
    produced a pair the runner refuses -- at exactly the point where
    retraction is decided. Only a real ingest showed it.
    """
    run = _source().run("run-1", mode=caller_mode)
    assert run.run_mode == "release_new"
    assert run.completed_scope is True


@pytest.mark.parametrize("caller_mode", ["release_new", "release_unchanged"])
def test_a_valid_catalog_mode_is_passed_through(caller_mode: str) -> None:
    """A caller that knows the profile's vocabulary is obeyed."""
    assert _source().run("run-1", mode=caller_mode).run_mode == caller_mode
