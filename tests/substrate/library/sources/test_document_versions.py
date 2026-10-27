"""Unit tests for the `DocumentVersionFacts` helper (PACT document-versions).

No database. The helper turns versions into node and edge facts; these tests
read the facts it returns.
"""
from __future__ import annotations

import itertools
from datetime import datetime, timedelta, timezone

import pytest

from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.library.sources.document_versions import (
    TEXT_ATTRIBUTES,
    BlockAnnotation,
    DocumentBlock,
    DocumentValue,
    DocumentVersion,
    DocumentVersionDuplicate,
    DocumentVersionError,
    DocumentVersionFacts,
    DocumentVersionNotText,
    DocumentVersionReferenceMissing,
    DocumentVersionTimeInvalid,
    DocumentVersionTimeMissing,
    canonical_json,
)

DEP = "dep"


def _version(
    version: str = "v1",
    issued_at="2024-01-01T00:00:00Z",
    *,
    entity: str = "acme",
    family: str = "report",
    **extra,
) -> DocumentVersion:
    return DocumentVersion(
        entity=entity,
        family=family,
        version=version,
        issued_at=issued_at,
        **extra,
    )


def _build(*versions: DocumentVersion) -> list:
    helper = DocumentVersionFacts(DEP)
    for version in versions:
        helper.add(version)
    return list(helper.facts())


def _nodes(facts: list) -> dict[str, NodeFact]:
    return {f.node_id: f for f in facts if isinstance(f, NodeFact)}


def _edges(facts: list, edge_type: str) -> set[tuple[str, str]]:
    return {(f.src, f.dst) for f in facts if isinstance(f, EdgeFact) and f.edge_type == edge_type}


def _flag(facts: list, node_id: str) -> str:
    return _nodes(facts)[node_id].attrs["is_latest"]


def _full(version: str = "v1", issued_at="2024-01-01T00:00:00Z", **extra) -> DocumentVersion:
    return _version(
        version,
        issued_at,
        covers="2023",
        doc_kind="annual",
        title="Annual report",
        raw_sha256="a" * 64,
        reader_ref="reader-1",
        blocks=[
            DocumentBlock("b-intro", "heading", "Intro", {"level": 1}),
            DocumentBlock("b-body", "paragraph", "Body text", {"words": 2}),
        ],
        values=[DocumentValue("v-total", "total", "usd", "2023", {"amount": 12})],
        annotations=[
            BlockAnnotation("layout", "k1", {"cols": 2}),
            BlockAnnotation("history", "k1", {"was": "v0"}),
        ],
        **extra,
    )


# ---------- fixed ids and attributes ----------


def test_ids_are_fixed_and_follow_the_documented_shapes() -> None:
    facts = _build(_full())

    assert set(_nodes(facts)) == {
        "docver:dep:acme:v1",
        "docblock:dep:acme:v1:0",
        "docblock:dep:acme:v1:1",
        "docval:dep:acme:v1:0",
        "blockann:dep:acme:v1:layout:k1",
        "blockann:dep:acme:v1:history:k1",
    }


def test_version_attributes_are_exact() -> None:
    node = _nodes(_build(_full()))["docver:dep:acme:v1"]

    assert node.subtype == "document_version"
    assert node.attrs == {
        "entity": "acme",
        "family": "report",
        "version": "v1",
        "issued_at": "2024-01-01T00:00:00Z",
        "covers": "2023",
        "doc_kind": "annual",
        "is_latest": "true",
        "admitted": "true",
        "refusal": "",
        "title": "Annual report",
        "raw_sha256": "a" * 64,
        "reader_ref": "reader-1",
    }


def test_block_value_and_annotation_attributes_are_exact() -> None:
    nodes = _nodes(_build(_full()))
    identity = {
        "entity": "acme",
        "family": "report",
        "version": "v1",
        "issued_at": "2024-01-01T00:00:00Z",
        "covers": "2023",
        "doc_kind": "annual",
        "is_latest": "true",
    }

    assert nodes["docblock:dep:acme:v1:1"].subtype == "document_block"
    assert nodes["docblock:dep:acme:v1:1"].attrs == {
        **identity,
        "block_key": "b-body",
        "block_kind": "paragraph",
        "text": "Body text",
        "payload": '{"words":2}',
        "position": 1,
    }
    assert nodes["docval:dep:acme:v1:0"].subtype == "document_value"
    assert nodes["docval:dep:acme:v1:0"].attrs == {
        **identity,
        "value_key": "v-total",
        "name": "total",
        "unit": "usd",
        "context": "2023",
        "payload": '{"amount":12}',
    }
    assert nodes["blockann:dep:acme:v1:history:k1"].subtype == "block_annotation"
    assert nodes["blockann:dep:acme:v1:history:k1"].attrs == {
        **identity,
        "annotation_key": "k1",
        "annotation_kind": "history",
        "payload": '{"was":"v0"}',
    }


def test_every_attribute_a_surface_reads_is_text_and_position_is_the_only_integer() -> None:
    for node in _nodes(_build(_full())).values():
        for name, value in node.attrs.items():
            if node.subtype == "document_block" and name == "position":
                assert isinstance(value, int) and not isinstance(value, bool)
            else:
                assert isinstance(value, str), (node.node_id, name)
        text_names = set(TEXT_ATTRIBUTES[node.subtype])
        assert set(node.attrs) - text_names <= {"position"}
        assert text_names <= set(node.attrs)


def test_a_part_with_a_colon_or_percent_never_shares_an_id() -> None:
    cases = [("a:b", "c"), ("a", "b:c"), ("a%3Ab", "c"), ("a", "b%3Ac")]
    ids = set()
    for entity, version in cases:
        helper = DocumentVersionFacts(DEP)
        node_id = helper.add(_version(version, entity=entity))
        assert node_id.count(":") == 3, node_id
        ids.add(node_id)

    assert len(ids) == len(cases)


def test_equal_input_gives_equal_facts_whatever_the_adding_order() -> None:
    versions = [
        _full("v1", "2024-01-01T00:00:00Z"),
        _full("v2", "2024-03-01T00:00:00Z"),
        _full("v3", "2024-02-01T00:00:00Z", amends="v1"),
        _version("w1", "2024-01-05T00:00:00Z", family="other"),
    ]
    reference = _build(*versions)

    for ordering in itertools.permutations(versions):
        assert _build(*ordering) == reference


def test_node_revision_changes_when_the_attributes_change() -> None:
    before = _nodes(_build(_full("v1", "2024-01-01T00:00:00Z")))["docver:dep:acme:v1"]
    after = _nodes(
        _build(_full("v1", "2024-01-01T00:00:00Z"), _full("v2", "2024-02-01T00:00:00Z"))
    )["docver:dep:acme:v1"]

    assert before.attrs["is_latest"] == "true"
    assert after.attrs["is_latest"] == "false"
    assert before.source_revision != after.source_revision
    assert before.source_record_id == after.source_record_id


# ---------- the latest flag ----------


def test_the_newest_issued_version_is_latest_and_supersedes_the_one_before() -> None:
    facts = _build(
        _full("v1", "2024-01-01T00:00:00Z"),
        _full("v2", "2024-02-01T00:00:00Z"),
    )

    assert _flag(facts, "docver:dep:acme:v1") == "false"
    assert _flag(facts, "docver:dep:acme:v2") == "true"
    assert _edges(facts, "supersedes") == {("docver:dep:acme:v2", "docver:dep:acme:v1")}


def test_children_carry_the_flag_of_their_version() -> None:
    facts = _build(
        _full("v1", "2024-01-01T00:00:00Z"),
        _full("v2", "2024-02-01T00:00:00Z"),
    )

    for node in _nodes(facts).values():
        expected = "true" if node.attrs["version"] == "v2" else "false"
        assert node.attrs["is_latest"] == expected, node.node_id


def test_a_late_arriving_earlier_issued_version_does_not_become_latest() -> None:
    """Failure case: v3 is added last but was issued between v1 and v2."""
    facts = _build(
        _full("v1", "2024-01-01T00:00:00Z"),
        _full("v2", "2024-03-01T00:00:00Z"),
        _full("v3", "2024-02-01T00:00:00Z"),
    )

    assert _flag(facts, "docver:dep:acme:v2") == "true"
    assert _flag(facts, "docver:dep:acme:v3") == "false"
    assert _flag(facts, "docver:dep:acme:v1") == "false"
    assert _flag(facts, "docblock:dep:acme:v3:0") == "false"
    assert _edges(facts, "supersedes") == {
        ("docver:dep:acme:v3", "docver:dep:acme:v1"),
        ("docver:dep:acme:v2", "docver:dep:acme:v3"),
    }


def test_a_newer_version_flips_the_old_latest_and_all_it_contains() -> None:
    before = _build(_full("v1", "2024-01-01T00:00:00Z"))
    after = _build(
        _full("v1", "2024-01-01T00:00:00Z"),
        _full("v2", "2024-02-01T00:00:00Z"),
    )

    old = {k: n for k, n in _nodes(before).items()}
    new = _nodes(after)
    assert {n.attrs["is_latest"] for n in old.values()} == {"true"}
    for node_id in old:
        assert new[node_id].attrs["is_latest"] == "false", node_id
        assert {k: v for k, v in new[node_id].attrs.items() if k != "is_latest"} == {
            k: v for k, v in old[node_id].attrs.items() if k != "is_latest"
        }


def test_equal_issue_times_are_broken_by_the_greater_version() -> None:
    facts = _build(
        _version("b", "2024-01-01T00:00:00Z"),
        _version("c", "2024-01-01T00:00:00Z"),
        _version("a", "2024-01-01T00:00:00Z"),
    )

    assert _flag(facts, "docver:dep:acme:c") == "true"
    assert _flag(facts, "docver:dep:acme:a") == "false"
    assert _flag(facts, "docver:dep:acme:b") == "false"


def test_each_entity_and_family_has_its_own_latest() -> None:
    facts = _build(
        _version("a1", "2024-01-01T00:00:00Z", entity="acme", family="report"),
        _version("a2", "2024-02-01T00:00:00Z", entity="acme", family="report"),
        _version("a3", "2024-03-01T00:00:00Z", entity="acme", family="notes"),
        _version("z1", "2024-01-01T00:00:00Z", entity="zed", family="report"),
    )
    helper = DocumentVersionFacts(DEP)
    for version in (
        _version("a1", "2024-01-01T00:00:00Z", entity="acme", family="report"),
        _version("a2", "2024-02-01T00:00:00Z", entity="acme", family="report"),
        _version("a3", "2024-03-01T00:00:00Z", entity="acme", family="notes"),
        _version("z1", "2024-01-01T00:00:00Z", entity="zed", family="report"),
    ):
        helper.add(version)

    assert helper.latest() == {
        ("acme", "report"): "docver:dep:acme:a2",
        ("acme", "notes"): "docver:dep:acme:a3",
        ("zed", "report"): "docver:dep:zed:z1",
    }
    assert _flag(facts, "docver:dep:acme:a1") == "false"
    assert _edges(facts, "supersedes") == {("docver:dep:acme:a2", "docver:dep:acme:a1")}


# ---------- a version kept but not admitted ----------


def test_a_refused_version_is_a_visible_node_that_is_never_latest() -> None:
    refused = DocumentVersion(
        entity="acme",
        family="report",
        version="v9",
        issued_at="2024-09-01T00:00:00Z",
        admitted=False,
        refusal="family_not_read",
    )
    facts = _build(_full("v1", "2024-01-01T00:00:00Z"), refused)

    node = _nodes(facts)["docver:dep:acme:v9"]
    assert node.attrs["admitted"] == "false"
    assert node.attrs["refusal"] == "family_not_read"
    assert node.attrs["is_latest"] == "false"
    assert _flag(facts, "docver:dep:acme:v1") == "true"
    assert _edges(facts, "supersedes") == set()
    assert not [n for n in _nodes(facts) if n.startswith(("docblock:", "docval:", "blockann:"))
                and ":v9:" in n]


def test_a_refused_version_may_have_no_time() -> None:
    refused = DocumentVersion(
        entity="acme",
        family="report",
        version="v9",
        issued_at=None,
        admitted=False,
        refusal="no_acceptance_time",
    )

    node = _nodes(_build(refused))["docver:dep:acme:v9"]

    assert node.attrs["issued_at"] == ""
    assert node.attrs["admitted"] == "false"


def test_a_refused_version_needs_a_reason_and_no_content() -> None:
    with pytest.raises(DocumentVersionError, match="no refusal reason"):
        DocumentVersionFacts(DEP).add(_version(admitted=False))
    with pytest.raises(DocumentVersionError, match="carries content"):
        DocumentVersionFacts(DEP).add(
            _version(
                admitted=False,
                refusal="kept",
                blocks=[DocumentBlock("b", "p", "t")],
            )
        )
    with pytest.raises(DocumentVersionError, match="admitted but carries a refusal"):
        DocumentVersionFacts(DEP).add(_version(refusal="oops"))


# ---------- a version without a time is refused ----------


@pytest.mark.parametrize("missing", [None, "", "   "])
def test_an_admitted_version_without_a_time_is_refused(missing) -> None:
    with pytest.raises(DocumentVersionTimeMissing, match="no issue time"):
        DocumentVersionFacts(DEP).add(_version(issued_at=missing))


@pytest.mark.parametrize(
    "bad",
    [
        "2024-01-01T00:00:00",
        "2024-01-01T00:00:00+00:00",
        "2024-01-01T00:00:00.5Z",
        "2024-01-01 00:00:00Z",
        "2024-13-01T00:00:00Z",
        "2024-01-01",
        "yesterday",
        # Python's strptime accepts these; as text they order wrongly.
        "2024-1-01T00:00:00Z",
        "2024-01-1T00:00:00Z",
        "2024-01-01T0:00:00Z",
        "2024-01-01T00:0:00Z",
        "2024-01-01T00:00:0Z",
        "2024-01- 1T00:00:00Z",
        " 2024-01-01T00:00:00Z",
        "2024-01-01T00:00:00Z\n",
        "２０２４-01-01T00:00:00Z",
    ],
)
def test_a_time_that_is_not_a_utc_instant_to_the_second_is_refused(bad: str) -> None:
    with pytest.raises(DocumentVersionTimeInvalid):
        DocumentVersionFacts(DEP).add(_version(issued_at=bad))


def test_a_refused_version_with_a_malformed_time_is_still_refused() -> None:
    with pytest.raises(DocumentVersionTimeInvalid):
        DocumentVersionFacts(DEP).add(
            _version(issued_at="soon", admitted=False, refusal="x")
        )


def test_a_timezone_aware_datetime_is_written_in_utc() -> None:
    zone = timezone(timedelta(hours=-5))
    helper = DocumentVersionFacts(DEP)
    helper.add(_version("v1", datetime(2024, 1, 1, 7, 0, 0, tzinfo=zone)))

    node = _nodes(list(helper.facts()))["docver:dep:acme:v1"]

    assert node.attrs["issued_at"] == "2024-01-01T12:00:00Z"


def test_an_unpadded_september_never_outranks_an_october() -> None:
    """The reported failure: "2026-9" sorts after "2026-10" as text."""
    helper = DocumentVersionFacts(DEP)
    helper.add(_version("oct", "2026-10-01T00:00:00Z"))

    with pytest.raises(DocumentVersionTimeInvalid, match="zero-padded"):
        helper.add(_version("sep", "2026-9-01T00:00:00Z"))

    helper.add(_version("sep", "2026-09-01T00:00:00Z"))
    assert helper.latest() == {("acme", "report"): "docver:dep:acme:oct"}


def test_a_year_below_1000_is_written_with_four_digits() -> None:
    early = datetime(999, 1, 1, tzinfo=timezone.utc)
    helper = DocumentVersionFacts(DEP)
    helper.add(_version("old", early))
    helper.add(_version("new", "1000-01-01T00:00:00Z"))

    nodes = _nodes(list(helper.facts()))

    assert nodes["docver:dep:acme:old"].attrs["issued_at"] == "0999-01-01T00:00:00Z"
    assert helper.latest() == {("acme", "report"): "docver:dep:acme:new"}


def test_a_raw_digest_with_a_trailing_newline_is_refused() -> None:
    with pytest.raises(DocumentVersionError, match="raw_sha256"):
        DocumentVersionFacts(DEP).add(_version(raw_sha256="a" * 64 + "\n"))


def test_a_naive_datetime_or_one_with_a_fraction_is_refused() -> None:
    with pytest.raises(DocumentVersionTimeInvalid, match="no timezone"):
        DocumentVersionFacts(DEP).add(_version(issued_at=datetime(2024, 1, 1)))
    with pytest.raises(DocumentVersionTimeInvalid, match="fraction"):
        DocumentVersionFacts(DEP).add(
            _version(issued_at=datetime(2024, 1, 1, tzinfo=timezone.utc, microsecond=5))
        )
    with pytest.raises(DocumentVersionTimeInvalid):
        DocumentVersionFacts(DEP).add(_version(issued_at=1704067200))


# ---------- attributes a surface reads must be text ----------


@pytest.mark.parametrize(
    "field,value",
    [
        ("entity", 7),
        ("family", None),
        ("version", 3),
        ("covers", 2023),
        ("doc_kind", ["annual"]),
        ("title", b"bytes"),
        ("reader_ref", 1.5),
        ("refusal", False),
        ("amends", 4),
    ],
)
def test_a_non_text_version_attribute_is_refused(field: str, value) -> None:
    with pytest.raises(DocumentVersionNotText, match="must be text"):
        DocumentVersionFacts(DEP).add(_version(**{field: value}))


def test_a_non_bool_admitted_is_refused() -> None:
    with pytest.raises(DocumentVersionNotText, match="admitted must be a bool"):
        DocumentVersionFacts(DEP).add(_version(admitted="true"))


@pytest.mark.parametrize(
    "child",
    [
        DocumentBlock("k", "p", 12),
        DocumentBlock("k", None, "t"),
        DocumentBlock(5, "p", "t"),
        DocumentValue("k", "name", unit=3),
        DocumentValue("k", "name", context=2023),
        DocumentValue("k", 9),
    ],
)
def test_a_non_text_block_or_value_attribute_is_refused(child) -> None:
    field = "blocks" if isinstance(child, DocumentBlock) else "values"
    with pytest.raises(DocumentVersionNotText, match="must be text"):
        DocumentVersionFacts(DEP).add(_version(**{field: [child]}))


@pytest.mark.parametrize(
    "annotation",
    [BlockAnnotation(1, "k"), BlockAnnotation("layout", 2)],
)
def test_a_non_text_annotation_attribute_is_refused(annotation) -> None:
    with pytest.raises(DocumentVersionNotText, match="must be text"):
        DocumentVersionFacts(DEP).add(_version(annotations=[annotation]))


def test_a_nul_character_is_refused() -> None:
    with pytest.raises(DocumentVersionNotText, match="NUL"):
        DocumentVersionFacts(DEP).add(
            _version(blocks=[DocumentBlock("k", "p", "bad\x00text")])
        )


@pytest.mark.parametrize("payload", [float("nan"), {1, 2}, object()])
def test_a_payload_that_is_not_json_is_refused(payload) -> None:
    with pytest.raises(DocumentVersionNotText, match="not JSON-encodable"):
        DocumentVersionFacts(DEP).add(
            _version(blocks=[DocumentBlock("k", "p", "t", payload)])
        )


def test_a_deployment_name_must_be_non_empty_text() -> None:
    with pytest.raises(DocumentVersionError):
        DocumentVersionFacts("  ")
    with pytest.raises(DocumentVersionNotText):
        DocumentVersionFacts(5)  # type: ignore[arg-type]


def test_required_names_cannot_be_empty() -> None:
    for field in ("entity", "family", "version"):
        with pytest.raises(DocumentVersionError, match="required"):
            DocumentVersionFacts(DEP).add(_version(**{field: ""}))
    with pytest.raises(DocumentVersionError, match="required"):
        DocumentVersionFacts(DEP).add(_version(blocks=[DocumentBlock("", "p", "t")]))


def test_a_raw_digest_must_be_lower_case_hex_or_empty() -> None:
    DocumentVersionFacts(DEP).add(_version(raw_sha256=""))
    for bad in ("abc", "A" * 64, "g" * 64):
        with pytest.raises(DocumentVersionError, match="raw_sha256"):
            DocumentVersionFacts(DEP).add(_version(raw_sha256=bad))


def test_content_must_be_given_as_lists_of_the_right_types() -> None:
    with pytest.raises(DocumentVersionError, match="must be a list or tuple"):
        DocumentVersionFacts(DEP).add(_version(blocks="not blocks"))
    with pytest.raises(DocumentVersionError, match="not a DocumentBlock"):
        DocumentVersionFacts(DEP).add(_version(blocks=[{"key": "k"}]))
    with pytest.raises(DocumentVersionError, match="DocumentVersion"):
        DocumentVersionFacts(DEP).add({"entity": "acme"})  # type: ignore[arg-type]


# ---------- duplicates ----------


def test_the_same_version_name_twice_is_refused_even_across_families() -> None:
    helper = DocumentVersionFacts(DEP)
    helper.add(_version("v1", family="report"))

    with pytest.raises(DocumentVersionDuplicate, match="added twice"):
        helper.add(_version("v1", family="notes"))


def test_a_repeated_key_inside_a_version_is_refused() -> None:
    with pytest.raises(DocumentVersionDuplicate, match="block key"):
        DocumentVersionFacts(DEP).add(
            _version(blocks=[DocumentBlock("k", "p", "a"), DocumentBlock("k", "p", "b")])
        )
    with pytest.raises(DocumentVersionDuplicate, match="value key"):
        DocumentVersionFacts(DEP).add(
            _version(values=[DocumentValue("k", "a"), DocumentValue("k", "b")])
        )
    with pytest.raises(DocumentVersionDuplicate, match="annotation"):
        DocumentVersionFacts(DEP).add(
            _version(
                annotations=[BlockAnnotation("layout", "k"), BlockAnnotation("layout", "k")]
            )
        )


def test_the_same_key_may_serve_a_block_a_value_and_two_annotation_kinds() -> None:
    facts = _build(
        _version(
            blocks=[DocumentBlock("k", "p", "t")],
            values=[DocumentValue("k", "n")],
            annotations=[BlockAnnotation("layout", "k"), BlockAnnotation("history", "k")],
        )
    )

    assert len([f for f in facts if isinstance(f, NodeFact)]) == 5


# ---------- edges ----------


def test_blocks_values_and_annotations_hang_off_their_version() -> None:
    facts = _build(_full())

    assert _edges(facts, "contains") == {
        ("docver:dep:acme:v1", "docblock:dep:acme:v1:0"),
        ("docver:dep:acme:v1", "docblock:dep:acme:v1:1"),
        ("docver:dep:acme:v1", "docval:dep:acme:v1:0"),
    }
    assert _edges(facts, "annotates") == {
        ("blockann:dep:acme:v1:layout:k1", "docver:dep:acme:v1"),
        ("blockann:dep:acme:v1:history:k1", "docver:dep:acme:v1"),
    }


def test_an_amendment_points_at_the_version_it_names() -> None:
    facts = _build(
        _version("v1", "2024-01-01T00:00:00Z"),
        _version("v2", "2024-01-02T00:00:00Z", amends="v1"),
    )

    assert _edges(facts, "amends") == {("docver:dep:acme:v2", "docver:dep:acme:v1")}


def test_an_amendment_may_be_added_before_its_target() -> None:
    helper = DocumentVersionFacts(DEP)
    helper.add(_version("v2", "2024-01-02T00:00:00Z", amends="v1"))
    helper.add(_version("v1", "2024-01-01T00:00:00Z"))

    assert _edges(list(helper.facts()), "amends") == {
        ("docver:dep:acme:v2", "docver:dep:acme:v1")
    }


def test_an_amendment_of_a_version_never_added_is_refused() -> None:
    helper = DocumentVersionFacts(DEP)
    helper.add(_version("v2", amends="v1"))

    with pytest.raises(DocumentVersionReferenceMissing, match="never added"):
        helper.facts()


def test_an_amendment_of_itself_is_refused() -> None:
    helper = DocumentVersionFacts(DEP)
    helper.add(_version("v2", amends="v2"))

    with pytest.raises(DocumentVersionReferenceMissing, match="itself"):
        helper.facts()


def test_an_amendment_does_not_cross_entities() -> None:
    helper = DocumentVersionFacts(DEP)
    helper.add(_version("v1", entity="other"))
    helper.add(_version("v2", entity="acme", amends="v1"))

    with pytest.raises(DocumentVersionReferenceMissing):
        helper.facts()


# ---------- block order ----------


def test_blocks_get_zero_based_positions_in_the_order_given() -> None:
    blocks = [DocumentBlock(key, "p", key.upper()) for key in ("c", "a", "b")]
    facts = _build(_version(blocks=blocks))

    by_position = sorted(
        (n for n in _nodes(facts).values() if n.subtype == "document_block"),
        key=lambda n: n.attrs["position"],
    )
    assert [n.attrs["position"] for n in by_position] == [0, 1, 2]
    assert [n.attrs["block_key"] for n in by_position] == ["c", "a", "b"]
    assert [n.node_id for n in by_position] == [
        "docblock:dep:acme:v1:0",
        "docblock:dep:acme:v1:1",
        "docblock:dep:acme:v1:2",
    ]


def test_ten_or_more_blocks_keep_numeric_not_text_order() -> None:
    blocks = [DocumentBlock(f"k{i}", "p", str(i)) for i in range(12)]
    nodes = [n for n in _nodes(_build(_version(blocks=blocks))).values() if n.subtype == "document_block"]

    ordered = sorted(nodes, key=lambda n: n.attrs["position"])

    assert [n.attrs["block_key"] for n in ordered] == [f"k{i}" for i in range(12)]


def test_the_same_text_in_two_versions_is_two_nodes() -> None:
    same = [DocumentBlock("k", "p", "unchanged paragraph")]
    facts = _build(_version("v1", blocks=same), _version("v2", "2024-02-01T00:00:00Z", blocks=same))

    blocks = [n for n in _nodes(facts).values() if n.subtype == "document_block"]
    assert len(blocks) == 2
    assert {n.node_id for n in blocks} == {
        "docblock:dep:acme:v1:0",
        "docblock:dep:acme:v2:0",
    }


# ---------- payload ----------


def test_payload_is_canonical_json() -> None:
    assert canonical_json({"b": 1, "a": [True, None, "é"]}) == '{"a":[true,null,"é"],"b":1}'
    block = DocumentBlock("k", "p", "t", {"z": 1, "a": {"y": 2, "x": 3}})
    node = _nodes(_build(_version(blocks=[block])))["docblock:dep:acme:v1:0"]

    assert node.attrs["payload"] == '{"a":{"x":3,"y":2},"z":1}'


# ---------- the record set a source hands the runner ----------


def _identities(record) -> set[tuple[str, str, str]]:
    return {edge.identity() for edge in record.edges}


def test_record_set_covers_every_fact_once_under_its_version() -> None:
    helper = DocumentVersionFacts(DEP)
    for version in (_full("v1", "2024-01-01T00:00:00Z"), _full("v2", "2024-02-01T00:00:00Z")):
        helper.add(version)
    facts = list(helper.facts())

    record_set = helper.record_set()

    assert set(record_set.records) == {"docver:dep:acme:v1", "docver:dep:acme:v2"}
    nodes = {
        node.node_id
        for record in record_set.records.values()
        for node in [record.primary, *record.descendants]
    }
    edges = [
        edge.identity()
        for record in record_set.records.values()
        for edge in record.edges
    ]
    assert nodes == set(_nodes(facts))
    assert sorted(edges) == sorted((f.src, f.edge_type, f.dst) for f in facts if isinstance(f, EdgeFact))
    v2 = record_set.records["docver:dep:acme:v2"]
    assert v2.primary.node_id == "docver:dep:acme:v2"
    assert v2.primary.subtype == "document_version"
    assert len(v2.descendants) == 5
    assert ("docver:dep:acme:v2", "supersedes", "docver:dep:acme:v1") in _identities(v2)


def test_a_late_version_removes_the_supersedes_edge_it_makes_untrue() -> None:
    """The edge v2 -> v1 is true until v3 is issued between them."""
    before = DocumentVersionFacts(DEP)
    for version in (_version("v1", "2024-01-01T00:00:00Z"), _version("v2", "2024-03-01T00:00:00Z")):
        before.add(version)
    after = DocumentVersionFacts(DEP)
    for version in (
        _version("v1", "2024-01-01T00:00:00Z"),
        _version("v2", "2024-03-01T00:00:00Z"),
        _version("v3", "2024-02-01T00:00:00Z"),
    ):
        after.add(version)

    old = _identities(before.record_set().records["docver:dep:acme:v2"])
    new_v2 = _identities(after.record_set().records["docver:dep:acme:v2"])
    new_v3 = _identities(after.record_set().records["docver:dep:acme:v3"])

    assert old == {("docver:dep:acme:v2", "supersedes", "docver:dep:acme:v1")}
    assert new_v2 == {("docver:dep:acme:v2", "supersedes", "docver:dep:acme:v3")}
    assert new_v3 == {("docver:dep:acme:v3", "supersedes", "docver:dep:acme:v1")}


def test_an_empty_set_gives_no_facts() -> None:
    assert list(DocumentVersionFacts(DEP).facts()) == []
    assert DocumentVersionFacts(DEP).latest() == {}
