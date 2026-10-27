"""The no-database half of PACT search-exact-id-variants-3959 (Refs #3959): the
id-shaped trigger, the identifier key, the declared-field values, the match
groups and the `okg_identifier_fields` declaration at compose time.

The database half is tests/substrate/mcp/test_search_exact_id_variants.py. This
file carries no postgres mark so that the per-PR fast lane runs it.
"""
from __future__ import annotations

import importlib
from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.modules_compose import (
    ComposedCatalog,
    _extract_subtypes,
    _validate_identifier_declarations,
)
from okg.substrate.mcp.search import (
    ExactIdQuery,
    _exact_name_group,
    _identifier_values,
    exact_id_query,
    identifier_key,
)

search_module = importlib.import_module("okg.substrate.mcp.search")


@pytest.mark.parametrize("query", [
    "HIG-18-026", "HIG18026", "CMS-PAS-HIG-18-026", "CMS_PAS_HIG_18_026",
    "CMSSW_14_0_0", "ABC12345", "  HIG-18-026\n", "e9d53159afd", "gpt-4o", "covid-19",
    "python_3", "sha-256",
])
def test_id_shaped_queries(query: str) -> None:
    idq = exact_id_query(query)
    assert idq is not None
    assert idq.name == query.strip()
    assert idq.key == identifier_key(query)


@pytest.mark.parametrize("query", [
    "", "14_0_0", "1400", "e-mail", "A-1", "v1.2", "HIG 18 026", "HIG--18-026",
    "-HIG-18-026", "HIG-18-026-", "load_payload", "MyClass", "plain",
    # No separator and no run of 4 digits: ordinary technical words.
    "amd64", "sha256", "python3", "x86", "gcc12", "root6", "ABC123",
    "HIG-18-02١", "A" * 70 + "1",
])
def test_queries_that_are_not_id_shaped(query: str) -> None:
    assert exact_id_query(query) is None


def test_the_key_strips_before_it_lowers() -> None:
    assert identifier_key("HIG-18-026") == identifier_key("hig_18.026") == "hig18026"
    # Non-ASCII letters are not part of the key, so the SQL form agrees.
    assert identifier_key("Kelvin-1") == "elvin1"
    assert identifier_key("İstanbul-1") == "stanbul1"


def test_the_runs_of_a_query() -> None:
    assert exact_id_query("HIG-18-026").runs == "hig 18 026"
    assert exact_id_query("HIG18026").runs == "hig18026"
    assert exact_id_query("HIG-18-026").hyphen is True
    assert exact_id_query("HIG_18_026").hyphen is False


@pytest.mark.parametrize(("value", "expected"), [
    ("HIG-18-026", ["HIG-18-026"]),
    ("HIG-18-026, HIG-18-027", ["HIG-18-026", "HIG-18-027"]),
    (" a ,, b ,\t", ["a", "b"]),
    (["HIG-18-026", " HIG-18-027 ", 7, None, ""], ["HIG-18-026", "HIG-18-027"]),
    # An array element is one value even if it holds a comma.
    (["Smith, John"], ["Smith, John"]),
    (7, []),
    (None, []),
    ({"a": 1}, []),
    (",".join(f"v{i}" for i in range(40)), [f"v{i}" for i in range(16)]),
])
def test_declared_field_values(value, expected) -> None:
    assert _identifier_values({"f": value}, "f") == expected


FIELDS = {"page": ["label", "analysis_id"]}


def _group(attrs: dict, query: str, subtype: str = "page"):
    idq = exact_id_query(query)
    return _exact_name_group(attrs, subtype, idq.name, idq, FIELDS)


def test_match_groups() -> None:
    q = "HIG-18-026"
    assert _group({"qualified_name": q}, q) == 0
    assert _group({"name": q, "qualified_name": "other"}, q) == 1
    assert _group({"name": q}, q) == 2
    assert _group({"label": "x", "analysis_id": "HIG-18-025, HIG-18-026"}, q) == 3
    # Another spelling: separator kind (runs equal) before joined runs.
    assert _group({"label": "HIG_18_026"}, q) == 4
    assert _group({"label": "hig-18-026"}, q) == 4
    assert _group({"label": "HIG18026"}, q) == 5
    assert _group({"label": "HIG-18-02"}, q) is None
    # A field the type does not declare is not read.
    assert _group({"title": q}, q) is None
    assert _group({"label": q}, q, subtype="other") is None
    # The best group wins.
    assert _group({"name": "HIG18026", "label": q}, q) == 3


def test_a_name_key_match_needs_a_hyphen() -> None:
    # No hyphen in the query nor in the stored value: different symbols.
    assert _group({"name": "CMSSW_1_40_0"}, "CMSSW_14_0_0") is None
    assert _group({"name": "Utf8Decode2"}, "utf8_decode2") is None
    # A hyphen on either side makes the key decisive.
    assert _group({"name": "HIG-18-026"}, "HIG18026") == 5
    assert _group({"name": "HIG18026"}, "HIG-18-026") == 5
    assert _group({"qualified_name": "CMS-PAS-HIG-18-026"}, "CMS_PAS_HIG_18_026") == 4
    # A declared field counts without one.
    assert _group({"label": "CMSSW_1_40_0"}, "CMSSW_14_0_0") == 5


def test_without_an_id_query_the_groups_are_the_name_groups() -> None:
    assert _exact_name_group({"name": "x"}, "t", "x", None, {}) == 2
    assert _exact_name_group({"name": "x", "qualified_name": "y"}, "t", "x", None, {}) == 1
    assert _exact_name_group({"qualified_name": "x"}, "t", "x", None, {}) == 0
    assert _exact_name_group({"name": "y"}, "t", "x", None, {}) is None


def test_marker_values_are_the_two_names() -> None:
    assert search_module._ID_GROUP_VARIANT == 4
    assert isinstance(exact_id_query("HIG-18-026"), ExactIdQuery)


# ---------- the catalog declaration ----------

def _compose(tmp_path: Path, searchable: str | None, identifiers: str | None) -> list[str]:
    """One subtype's annotations through the real compose step."""
    annotations: dict[str, str] = {"archetype": "artifact"}
    if searchable is not None:
        annotations["okg_searchable_text_fields"] = searchable
    if identifiers is not None:
        annotations["okg_identifier_fields"] = identifiers
    schema_path = tmp_path / "schema.yaml"
    schema_path.write_text(yaml.safe_dump({"classes": {"Memo": {"annotations": annotations}}}))
    composed = ComposedCatalog(deployment_name="test")
    _extract_subtypes(schema_path, "test", composed)
    _validate_identifier_declarations(composed)
    return composed.subtype_metadata.get("memo", {}).get("identifier_fields", [])


def test_the_declaration_is_stored_as_metadata(tmp_path: Path) -> None:
    assert _compose(tmp_path, "label,analysis_id,text", "label, analysis_id") == [
        "label", "analysis_id",
    ]
    assert _compose(tmp_path, "label", None) == []


@pytest.mark.parametrize("identifiers", ["text", "label,text", "okg_producer_text", "okg_anything"])
def test_text_and_platform_attributes_cannot_be_identifier_fields(
    tmp_path: Path, identifiers: str,
) -> None:
    with pytest.raises(ValueError, match="cannot name"):
        _compose(tmp_path, "label,text,okg_anything", identifiers)


def test_an_identifier_field_must_also_be_searchable_text(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="not also declared"):
        _compose(tmp_path, "title", "label")
    with pytest.raises(ValueError, match="not also declared"):
        _compose(tmp_path, None, "label")


# A deployment file may add identifier fields to a library type; the check
# runs on the composed metadata, so the overlay is held to the same rule.
def _overlay(tmp_path: Path, annotations: str):
    from tests.substrate.catalog.test_module_providers import (
        _compose_with_overlay,
        _person_overlay,
    )

    root = tmp_path / f"case{len(list(tmp_path.iterdir()))}"
    root.mkdir()
    return _compose_with_overlay(
        root, _person_overlay("      okg_extends: people\n" + annotations),
    )


def test_an_overlay_may_declare_a_searchable_field_as_identifier(tmp_path: Path) -> None:
    # `name` is already searchable in the library type; the file adds only the declaration.
    composed = _overlay(tmp_path, "      okg_identifier_fields: name\n")
    assert composed.subtype_metadata["person"]["identifier_fields"] == ["name"]
    composed = _overlay(
        tmp_path,
        "      okg_searchable_text_fields: name,nickname\n"
        "      okg_identifier_fields: nickname\n",
    )
    assert composed.subtype_metadata["person"]["identifier_fields"] == ["nickname"]


def test_an_overlay_cannot_declare_a_field_that_is_not_searchable_text(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="not also declared"):
        _overlay(tmp_path, "      okg_identifier_fields: nickname\n")


def test_an_overlay_cannot_declare_text_as_an_identifier_field(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="cannot name"):
        _overlay(
            tmp_path,
            "      okg_searchable_text_fields: name,text\n"
            "      okg_identifier_fields: text\n",
        )
