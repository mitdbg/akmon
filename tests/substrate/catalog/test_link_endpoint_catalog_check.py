"""Catalog load checks a route's `on_miss` against the catalog (no DB).

Issue #3666, PACT link-endpoints-3666. A "known by key, not loaded" endpoint
is a node of the route's subtype that holds only its key (and a scope). The
catalog load refuses an `on_miss` whose subtype the catalog lacks, or whose
subtype requires (LinkML `required: true`, inherited through `is_a`) an
attribute the endpoint cannot fill. A deployment that declares no `on_miss`
composes as before; its linker config is not read.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.errors import SubstrateError


_TICKETS = """\
classes:
  Ticket:
    annotations:
      archetype: artifact
    attributes:
      summary:
        required: true
  JiraIssue:
    is_a: Ticket
    annotations:
      archetype: artifact
    attributes:
      issue_key:
        required: true
      project: {}
  JiraKey:
    annotations:
      archetype: artifact
    attributes:
      issue_key:
        required: true
      project:
        required: true
      title: {}
"""


def _on_miss(subtype: str, *, scope: bool = True) -> str:
    block = {"endpoint_id": "jira:{value}", "subtype": subtype, "key_attr": "issue_key"}
    if scope:
        block["scope"] = {"attr": "project", "regex": "^([A-Z0-9]+)-"}
    return (
        "routes:\n  cms_jira_key:\n    edge_type: references\n"
        '    dst_template: "{resolved}"\n'
        f"    on_miss: {json.dumps(block)}\n"
    )


def _compose(tmp_path: Path, linker_config: str | None):
    library = tmp_path / "library" / "tickets"
    library.mkdir(parents=True)
    (library / "schema.yaml").write_text(_TICKETS)
    deployment = tmp_path / "deployment"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(
        json.dumps({"name": "fixture", "modules": ["tickets"]}),
    )
    if linker_config is not None:
        (deployment / "linker_config.yaml").write_text(linker_config)
    cross_links = tmp_path / "cross_links"
    cross_links.mkdir()
    return compose_catalog(
        deployment, modules_root=tmp_path / "library", cross_links_root=cross_links,
    )


def test_required_attributes_are_compiled_per_subtype(tmp_path: Path) -> None:
    composed = _compose(tmp_path, None)
    assert composed.subtype_required_attributes["ticket"] == {"summary"}
    assert composed.subtype_required_attributes["jira_issue"] == {"issue_key"}
    assert composed.subtype_required_attributes["jira_key"] == {"issue_key", "project"}


def test_an_endpoint_that_fills_every_required_attribute_composes(tmp_path: Path) -> None:
    """The scope attribute counts: `project` is filled from the key."""
    composed = _compose(tmp_path, _on_miss("jira_key"))
    assert "jira_key" in composed.subtypes


def test_an_inherited_required_attribute_the_endpoint_cannot_fill_is_refused(
    tmp_path: Path,
) -> None:
    """Failure case: `jira_issue` inherits `summary` (required) from `ticket`;
    an endpoint holds no summary, so the catalog load refuses the route."""
    with pytest.raises(SubstrateError) as caught:
        _compose(tmp_path, _on_miss("jira_issue"))
    data = caught.value.data
    assert data["reason"] == "required_attribute_unfilled"
    assert data["subtype"] == "jira_issue"
    assert data["unfilled"] == ["summary"]
    assert data["where"] == "routes.cms_jira_key.on_miss"


def test_a_required_scope_attribute_with_no_scope_declared_is_refused(
    tmp_path: Path,
) -> None:
    with pytest.raises(SubstrateError) as caught:
        _compose(tmp_path, _on_miss("jira_key", scope=False))
    assert caught.value.data["unfilled"] == ["project"]


def test_an_on_miss_subtype_the_catalog_lacks_is_refused_at_load(tmp_path: Path) -> None:
    with pytest.raises(SubstrateError) as caught:
        _compose(tmp_path, _on_miss("jira_ticket"))
    assert caught.value.data["reason"] == "unknown_subtype"


def test_a_linker_config_without_on_miss_is_not_read_at_catalog_load(
    tmp_path: Path,
) -> None:
    """The check adds no new way for an existing deployment to fail: a
    config with no `on_miss` is not validated here (the publisher still
    validates it when it loads its routes)."""
    composed = _compose(tmp_path, "routes: {}\nnot_a_key: 1\n")
    assert "jira_issue" in composed.subtypes
