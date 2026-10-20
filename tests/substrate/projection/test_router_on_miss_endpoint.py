"""Link endpoints, no database (issue #3666, PACT link-endpoints-3666).

A route with `on_miss` keeps a link whose target record is not loaded: the
resolver miss returns the edge to the target's real id and one endpoint
request, and the miss is still recorded so the retry row keeps waiting. A
route without `on_miss` behaves as before. The block is validated at
catalog load and is not part of the extractor fingerprint.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import pytest

from okg.extraction import ExtractedEntity
from okg.substrate.catalog.linker_config import (
    LINK_ENDPOINT_SOURCE,
    LinkEndpointSpec,
    LinkerRoute,
    is_link_endpoint_row,
    load_linker_config,
)
from okg.substrate.errors import SubstrateError
from okg.substrate.projection.projector import _extractor_fingerprint
from okg.substrate.projection.router import LinkerRouter


class _Resolver:
    def __init__(self, known: dict[str, str]) -> None:
        self.known = known

    def resolve(self, entity: ExtractedEntity) -> Optional[str]:
        return self.known.get(entity.value)


SPEC = LinkEndpointSpec(
    endpoint_id="jira:{value}",
    subtype="jira_issue",
    key_attr="issue_key",
    scope_attr="project",
    scope_regex=r"^([A-Z0-9]+)-",
)


def _route(on_miss: Optional[LinkEndpointSpec]) -> LinkerRoute:
    return LinkerRoute(
        entity_type="cms_jira_key",
        src_subtype=None,
        edge_type="references",
        dst_template="{resolved}",
        on_miss=on_miss,
    )


def _entity(value: str = "CMSTRANSF-452") -> ExtractedEntity:
    return ExtractedEntity(type="cms_jira_key", value=value, start=0, end=len(value))


def test_a_miss_on_an_on_miss_route_links_to_the_endpoint_and_still_waits() -> None:
    router = LinkerRouter([_route(SPEC)])
    misses: list = []
    requests: list = []
    edge = router.route(
        _entity(), src_node_id="commit:r:abc", src_subtype="git_commit",
        resolver=_Resolver({}), resolver_misses=misses,
        endpoint_requests=requests,
    )
    assert edge is not None
    assert (edge.dst, edge.edge_type, edge.endpoint) == (
        "jira:CMSTRANSF-452", "references", True)
    assert misses == [("cms_jira_key", "CMSTRANSF-452")]
    assert len(requests) == 1
    assert requests[0].node_id == "jira:CMSTRANSF-452"
    assert requests[0].subtype == "jira_issue"
    assert requests[0].attrs == {
        "issue_key": "CMSTRANSF-452", "project": "CMSTRANSF",
        "load_state": "not_loaded", "label": "CMSTRANSF-452",
        # Findable by its key, and says what it is (review M1).
        "text": "CMSTRANSF-452 (known by key, not loaded: the graph holds no record for it)",
    }


def test_a_route_without_on_miss_still_drops_the_miss() -> None:
    """Failure case: before this change every route behaved like this."""
    router = LinkerRouter([_route(None)])
    misses: list = []
    requests: list = []
    assert router.route(
        _entity(), src_node_id="commit:r:abc", src_subtype="git_commit",
        resolver=_Resolver({}), resolver_misses=misses,
        endpoint_requests=requests,
    ) is None
    assert misses == [("cms_jira_key", "CMSTRANSF-452")]
    assert requests == []


def test_a_caller_that_cannot_write_endpoints_gets_no_edge() -> None:
    """An edge without its endpoint node would dangle, so a caller that
    passes no request list keeps the old answer."""
    router = LinkerRouter([_route(SPEC)])
    assert router.route(
        _entity(), src_node_id="commit:r:abc", src_subtype="git_commit",
        resolver=_Resolver({}),
    ) is None


def test_a_resolved_key_links_to_the_real_record_and_asks_for_no_endpoint() -> None:
    router = LinkerRouter([_route(SPEC)])
    requests: list = []
    edge = router.route(
        _entity(), src_node_id="commit:r:abc", src_subtype="git_commit",
        resolver=_Resolver({"CMSTRANSF-452": "jira:CMSTRANSF-452"}),
        endpoint_requests=requests,
    )
    assert edge is not None and edge.endpoint is False
    assert edge.dst == "jira:CMSTRANSF-452"
    assert requests == []


def test_an_endpoint_at_the_citing_records_own_id_is_dropped() -> None:
    router = LinkerRouter([_route(SPEC)])
    requests: list = []
    assert router.route(
        _entity(), src_node_id="jira:CMSTRANSF-452", src_subtype="jira_issue",
        resolver=_Resolver({}), endpoint_requests=requests,
    ) is None
    assert requests == []


_CONFIG = """
routes:
  cms_jira_key:
    git_commit:
      edge_type: references
      dst_template: "{resolved}"
      on_miss:
        endpoint_id: "jira:{value}"
        subtype: jira_issue
        key_attr: issue_key
        scope: {attr: project, regex: "^([A-Z0-9]+)-"}
    _default:
      edge_type: mentions
      dst_template: "{resolved}"
  cmssw_release:
    edge_type: mentions
    dst_template: "{resolved}"
"""


def test_on_miss_is_read_per_route_and_off_by_default(tmp_path: Path) -> None:
    path = tmp_path / "linker_config.yaml"
    path.write_text(_CONFIG)
    routes = {(r.entity_type, r.src_subtype): r for r in load_linker_config(path)}
    assert routes[("cms_jira_key", "git_commit")].on_miss == SPEC
    assert routes[("cms_jira_key", "_default")].on_miss is None
    assert routes[("cmssw_release", None)].on_miss is None


@pytest.mark.parametrize(
    ("block", "needle"),
    [
        ({"endpoint_id": "jira:{resolved}", "subtype": "jira_issue",
          "key_attr": "issue_key"}, "endpoint_id"),
        ({"endpoint_id": "jira:fixed", "subtype": "jira_issue",
          "key_attr": "issue_key"}, "endpoint_id"),
        ({"endpoint_id": "jira:{value}", "subtype": "JiraIssue",
          "key_attr": "issue_key"}, "subtype"),
        ({"endpoint_id": "jira:{value}", "subtype": "jira_issue"}, "missing"),
        ({"endpoint_id": "jira:{value}", "subtype": "jira_issue",
          "key_attr": "issue_key", "alias": True}, "unknown"),
        ({"endpoint_id": "jira:{value}", "subtype": "jira_issue",
          "key_attr": "issue_key", "scope": {"attr": "project", "regex": "^[A-Z]+-"}},
         "capture group"),
        ({"endpoint_id": "jira:{value}", "subtype": "jira_issue",
          "key_attr": "load_state"}, "reserved"),
    ],
)
def test_a_bad_on_miss_block_is_refused_at_catalog_load(
    tmp_path: Path, block: dict, needle: str,
) -> None:
    import json

    path = tmp_path / "linker_config.yaml"
    path.write_text(
        "routes:\n  cms_jira_key:\n    edge_type: references\n"
        '    dst_template: "{resolved}"\n'
        f"    on_miss: {json.dumps(block)}\n"
    )
    with pytest.raises(SubstrateError) as caught:
        load_linker_config(path)
    assert needle in str(caught.value)


def test_on_miss_needs_a_resolving_route(tmp_path: Path) -> None:
    path = tmp_path / "linker_config.yaml"
    path.write_text(
        "routes:\n  cms_jira_key:\n    edge_type: references\n"
        '    dst_template: "jira:{value}"\n'
        '    on_miss: {endpoint_id: "jira:{value}", subtype: jira_issue, '
        "key_attr: issue_key}\n"
    )
    with pytest.raises(SubstrateError, match="resolved"):
        load_linker_config(path)


def test_switching_on_miss_on_does_not_rotate_the_extractor_fingerprint() -> None:
    """So a route change re-extracts nothing; the backfill comes from the
    retry rows instead."""
    from okg.substrate.projection import RegexEntityExtractor
    from okg.substrate.projection.router import PgAliasResolver

    extractor = RegexEntityExtractor([("cms_jira_key", r"\b([A-Z]+-\d+)\b")])
    off = _extractor_fingerprint(extractor, [_route(None)], PgAliasResolver)
    on = _extractor_fingerprint(extractor, [_route(SPEC)], PgAliasResolver)
    assert off == on


def test_only_a_core_producer_row_still_marked_not_loaded_is_an_endpoint() -> None:
    attrs = {"issue_key": "K-1", "load_state": "not_loaded", "label": "K-1"}
    assert is_link_endpoint_row({"source": LINK_ENDPOINT_SOURCE, "attrs": attrs})
    assert not is_link_endpoint_row({"source": "jira", "attrs": attrs})
    assert not is_link_endpoint_row(
        {"source": LINK_ENDPOINT_SOURCE, "attrs": {"issue_key": "K-1"}})


def test_the_router_names_the_routes_that_keep_unloaded_targets() -> None:
    """The publisher reads this to decide whether the endpoint sweep runs;
    a table with no on_miss route makes it skip every read."""
    other = LinkerRoute(
        entity_type="cmssw_release", src_subtype=None, edge_type="mentions",
        dst_template="{resolved}",
    )
    with_one = LinkerRouter([_route(SPEC), other])
    assert [r.entity_type for r in with_one.endpoint_routes] == ["cms_jira_key"]
    assert LinkerRouter([_route(None), other]).endpoint_routes == ()
