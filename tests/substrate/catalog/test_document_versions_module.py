"""Catalog tests for the `document_versions` ontology module (PACT document-versions).

Composition, narrowing and the text-only rule for attributes a surface reads.
No database, no container: the catalog composes from YAML alone.
"""
from __future__ import annotations

import re
import shutil
from pathlib import Path

import pytest
import yaml

import okg.substrate.library.ontologies as ontologies_package
import okg.substrate.library.sources.document_versions as helper_module
from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.library.sources.document_versions import (
    SUBTYPE_ANNOTATION,
    SUBTYPE_BLOCK,
    SUBTYPE_VALUE,
    SUBTYPE_VERSION,
    TEXT_ATTRIBUTES,
)
from tests.mirror_scope import shipped

MODULE_DIR = Path(ontologies_package.__file__).parent / "document_versions"
SUBTYPES = {SUBTYPE_VERSION, SUBTYPE_BLOCK, SUBTYPE_VALUE, SUBTYPE_ANNOTATION}

#: The only attribute with a non-text range. No surface reads it.
INTEGER_ATTRIBUTES = {SUBTYPE_BLOCK: {"position"}}

#: Names the first draft used. They are domain words, and the module must not
#: bring them back.
RETIRED_ATTRIBUTE_NAMES = {
    "form",
    "period",
    "concept",
    "accepted_at",
    "tagged_value",
    "reader_commit",
    "period_text",
}


def _compose(tmp_path: Path, modules: list[str]):
    root = tmp_path / "deployment"
    (root / "schemas").mkdir(parents=True)
    (root / "deployment.yaml").write_text(
        yaml.safe_dump({"name": "docver-test", "modules": modules}),
        encoding="utf-8",
    )
    return compose_catalog(root)


def _schema(path: Path = MODULE_DIR / "schema.yaml") -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def _snake(name: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", name).lower()


def _non_text_attributes(schema: dict) -> dict[str, set[str]]:
    """Attributes whose range is not text, by subtype.

    The module's default range is `string`; an attribute is non-text when it
    names another range. This is the check a surface-read attribute has to
    pass: the engine refuses a non-text attribute, so the module must not
    declare one that a surface could read.
    """
    default = schema.get("default_range", "string")
    found: dict[str, set[str]] = {}
    for class_name, definition in schema["classes"].items():
        if class_name == "EdgeNarrowings":
            continue
        for attr, spec in (definition.get("attributes") or {}).items():
            range_ = (spec or {}).get("range", default)
            if range_ != "string":
                found.setdefault(_snake(class_name), set()).add(attr)
    return found


def _refuse_non_text_surface_attributes(schema: dict) -> None:
    unexpected = {
        subtype: sorted(attrs - INTEGER_ATTRIBUTES.get(subtype, set()))
        for subtype, attrs in _non_text_attributes(schema).items()
        if attrs - INTEGER_ATTRIBUTES.get(subtype, set())
    }
    if unexpected:
        raise ValueError(
            f"document_versions declares non-text attributes a surface could "
            f"read: {unexpected}"
        )


def test_module_composes_with_exactly_four_subtypes(tmp_path: Path) -> None:
    composed = _compose(tmp_path, ["document_versions"])

    assert composed.subtypes == SUBTYPES
    assert {composed.subtype_origin[s] for s in SUBTYPES} == {"document_versions"}


def test_narrowings_are_exactly_the_five_declared(tmp_path: Path) -> None:
    composed = _compose(tmp_path, ["document_versions"])

    triples = {
        (n.parent_subtype, n.edge_archetype, n.child_subtype)
        for n in composed.narrowings
    }
    assert triples == {
        ("document_version", "contains", "document_block"),
        ("document_version", "contains", "document_value"),
        ("block_annotation", "annotates", "document_version"),
        ("document_version", "amends", "document_version"),
        ("document_version", "supersedes", "document_version"),
    }
    assert len(composed.narrowings) == 5


@pytest.mark.parametrize(
    "triple",
    [
        # A block does not contain a version, a value is not annotated, an
        # annotation cannot amend, and nothing links a block to a block.
        ("document_block", "contains", "document_version"),
        ("document_version", "annotates", "block_annotation"),
        ("block_annotation", "amends", "document_version"),
        ("document_block", "contains", "document_block"),
        ("document_value", "annotates", "document_version"),
        ("document_version", "supersedes", "document_block"),
    ],
)
def test_an_undeclared_edge_has_no_narrowing(tmp_path: Path, triple) -> None:
    """The narrowing catalog is the refusal point: no row, no edge at insert."""
    composed = _compose(tmp_path, ["document_versions"])

    declared = {
        (n.parent_subtype, n.edge_archetype, n.child_subtype)
        for n in composed.narrowings
    }
    assert triple not in declared


def test_supersedes_and_amends_allow_one_target_per_version(tmp_path: Path) -> None:
    """The cardinality is what blocks a publish that kept a stale supersedes edge.

    A source that forgets the helper's record set leaves `v2 supersedes v1`
    beside `v2 supersedes v3`; two targets for one version break this limit,
    and the consistency gate refuses the publish (the database proof shows it).
    """
    composed = _compose(tmp_path, ["document_versions"])

    cardinality = {
        (n.parent_subtype, n.edge_archetype, n.child_subtype): n.cardinality
        for n in composed.narrowings
    }
    assert cardinality == {
        ("document_version", "contains", "document_block"): None,
        ("document_version", "contains", "document_value"): None,
        ("block_annotation", "annotates", "document_version"): None,
        ("document_version", "amends", "document_version"): "at_most_one_target",
        ("document_version", "supersedes", "document_version"): "at_most_one_target",
    }


def test_contains_is_the_only_hierarchy_edge(tmp_path: Path) -> None:
    from okg.substrate.catalog.relation_kind import classify_archetype

    composed = _compose(tmp_path, ["document_versions"])
    overrides = composed.archetype_kind_overrides

    assert classify_archetype("contains", overrides) == "contains"
    for archetype in ("annotates", "amends", "supersedes"):
        assert classify_archetype(archetype, overrides) == "references"


def test_version_declares_blocks_as_its_passages(tmp_path: Path) -> None:
    composed = _compose(tmp_path, ["document_versions"])

    assert composed.subtype_metadata["document_version"]["passage_rollup"] == {
        "edge_type": "contains",
        "child_subtype": "document_block",
    }
    for other in ("document_block", "document_value", "block_annotation"):
        assert "passage_rollup" not in composed.subtype_metadata[other]


def test_block_text_and_version_title_are_searchable(tmp_path: Path) -> None:
    composed = _compose(tmp_path, ["document_versions"])

    assert composed.subtype_metadata["document_block"]["searchable_text_fields"] == [
        "text"
    ]
    assert composed.subtype_metadata["document_version"][
        "searchable_text_fields"
    ] == ["title"]


def test_schema_attributes_are_exactly_the_helper_table(tmp_path: Path) -> None:
    """The schema and the helper's attribute table cannot drift apart."""
    composed = _compose(tmp_path, ["document_versions"])

    for subtype, text_attrs in TEXT_ATTRIBUTES.items():
        expected = set(text_attrs) | INTEGER_ATTRIBUTES.get(subtype, set())
        assert composed.subtype_attributes[subtype] == expected, subtype


def test_every_attribute_but_position_is_text() -> None:
    schema = _schema()

    assert _non_text_attributes(schema) == {"document_block": {"position"}}
    _refuse_non_text_surface_attributes(schema)


def test_a_non_text_surface_attribute_is_refused(tmp_path: Path) -> None:
    """Failure case: a text attribute turned into an integer must be caught."""
    schema = _schema()
    schema["classes"]["DocumentVersion"]["attributes"]["issued_at"]["range"] = (
        "integer"
    )
    schema["classes"]["DocumentBlock"]["attributes"]["is_latest"]["range"] = (
        "boolean"
    )

    with pytest.raises(ValueError, match="non-text attributes"):
        _refuse_non_text_surface_attributes(schema)

    broken = tmp_path / "schema.yaml"
    broken.write_text(yaml.safe_dump(schema), encoding="utf-8")
    assert _non_text_attributes(_schema(broken)) == {
        "document_version": {"issued_at"},
        "document_block": {"is_latest", "position"},
    }


def test_every_child_repeats_the_version_identity(tmp_path: Path) -> None:
    composed = _compose(tmp_path, ["document_versions"])
    identity = {
        "entity",
        "family",
        "version",
        "issued_at",
        "covers",
        "doc_kind",
        "is_latest",
    }

    for subtype in SUBTYPES:
        assert identity <= composed.subtype_attributes[subtype], subtype


def test_retired_domain_attribute_names_are_absent(tmp_path: Path) -> None:
    composed = _compose(tmp_path, ["document_versions"])

    for subtype in SUBTYPES:
        assert composed.subtype_attributes[subtype].isdisjoint(
            RETIRED_ATTRIBUTE_NAMES
        ), subtype


def test_module_composes_beside_the_document_modules(tmp_path: Path) -> None:
    """No subtype name or narrowing clashes with the neighbouring modules."""
    neighbours = ["document_starter", "extraction", "git_graph", "legal", "doc_snapshot"]

    composed = _compose(tmp_path, ["document_versions", *neighbours])

    assert SUBTYPES <= composed.subtypes
    for subtype in SUBTYPES:
        assert composed.subtype_origin[subtype] == "document_versions"
    alone = _compose(tmp_path / "alone", ["document_versions"])
    mine = {
        (n.parent_subtype, n.edge_archetype, n.child_subtype)
        for n in composed.narrowings
        if n.origin == "document_versions"
    }
    assert mine == {
        (n.parent_subtype, n.edge_archetype, n.child_subtype)
        for n in alone.narrowings
    }


def test_module_ships_beside_every_other_manifest() -> None:
    manifest = yaml.safe_load((MODULE_DIR / "module.yaml").read_text(encoding="utf-8"))

    assert manifest["name"] == "document_versions"
    assert manifest["provides"] == {"subtypes_file": "schema.yaml"}


def _form(number: int, letter: str) -> str:
    """A report form's name, built so this file does not hold one."""
    return "{}-{}".format(number, letter)


def _banned_terms() -> tuple[re.Pattern, re.Pattern]:
    """The words the module must not contain, built so this file stays clean."""
    words = [
        "ed" + "gar",
        "fil" + "ing",
        "xb" + "rl",
        "ce" + "rn",
        "tw" + "iki",
        "c" + "ms",
        # Words from the same world that a description can carry without
        # naming the system (a review mutation put two of them in and the
        # scan stayed green).
        "acc" + "ession",
        "fil" + "er",
        "iss" + "uer",
        "regis" + "trant",
    ]
    insensitive = re.compile(r"\b(?:" + "|".join(words) + r")s?\b", re.IGNORECASE)
    # Upper-case names, and the names of report forms (a number, a dash, a
    # letter).
    upper_only = re.compile(
        r"\b(?:" + "S" + "EC" + r"|[0-9]{1,2}-[KQ](?:/A)?)\b"
    )
    return insensitive, upper_only


def test_no_domain_term_appears_in_the_module_helper_or_tests() -> None:
    insensitive, upper_only = _banned_terms()
    sources_tests = Path(__file__).parents[1] / "library" / "sources"
    # The change record under pact/changes/ does not ship in the public copy;
    # there shipped() drops it and the shipped files are still scanned.
    files = shipped([
        MODULE_DIR / "module.yaml",
        MODULE_DIR / "schema.yaml",
        Path(helper_module.__file__),
        Path(__file__),
        sources_tests / "test_document_versions.py",
        sources_tests / "test_document_versions_publish_db.py",
        Path(__file__).parents[3] / "pact" / "changes" / "document-versions" / "pact.yaml",
    ])

    hits = []
    for path in files:
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if insensitive.search(line) or upper_only.search(line):
                hits.append(f"{path.name}:{number}: {line.strip()}")
    assert hits == []


def test_the_term_scan_catches_a_domain_word(tmp_path: Path) -> None:
    """Failure case: the scan must go red when a banned word is present."""
    insensitive, upper_only = _banned_terms()
    copy = tmp_path / "schema.yaml"
    shutil.copy(MODULE_DIR / "schema.yaml", copy)
    copy.write_text(
        copy.read_text(encoding="utf-8") + "\n# the " + "fil" + "ing is late\n",
        encoding="utf-8",
    )

    text = copy.read_text(encoding="utf-8")
    assert insensitive.search(text)
    assert upper_only.search("rules of the " + "S" + "EC apply")


@pytest.mark.parametrize(
    "line",
    [
        # The review's surviving mutation: a description naming who submits
        # a document and its identifier.
        "the " + "fil" + "er, by " + "acc" + "ession number",
        "a " + "regis" + "trant",
        "the " + "iss" + "uer",
        "an annual " + _form(10, "K") + " report",
        "a current " + _form(8, "K") + "/A",
        "a quarterly " + _form(10, "Q"),
    ],
)
def test_the_term_scan_catches_the_words_a_review_found_missing(line: str) -> None:
    insensitive, upper_only = _banned_terms()

    assert insensitive.search(line) or upper_only.search(line)


def test_the_term_scan_leaves_ordinary_words_alone() -> None:
    insensitive, upper_only = _banned_terms()

    for line in (
        "a release tag",
        "the version is issued at a time",
        "SHA-256 digest",
        "profile and filter",
        "2024-01-01T00:00:00Z",
    ):
        assert not insensitive.search(line), line
        assert not upper_only.search(line), line
    assert not upper_only.search("rows 10-12 and a 3-way merge")
