"""Source manifest compiler (PACT no-code-source-manifest, task compile).

No database: a manifest compiles into one strict registry entry, a LinkML
schema and bridges file that compose with a deployment's catalog, and an
output signature naming exactly the declared types and links. Every rule
violation is refused with its manifest path and a fix.
"""
from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
import yaml

from okg.substrate.catalog.modules_compose import compose_catalog
from okg.substrate.deployment_scaffold import create_deployment_scaffold
from okg.substrate.sources import manifest as m
from okg.substrate.sources.guided_add import build_registry_entry
from okg.substrate.sources.guided_catalog import manifest_type
from okg.substrate.sources.registry import admit_source_registry_document


TICKETS = """
manifest: okg.source/v1
description: Tickets exported from our tracker every night.
read:
  files: exports/*.json
  records: issues
types:
  tracker_ticket:
    key: "{key}"
    title: fields.summary
    text: fields.description
    dates:
      created: fields.created
      modified: fields.updated
    fields:
      status: fields.status.name
      votes: { from: fields.votes, type: integer }
    links:
      assigned_to: { to: tracker_person, key: "{fields.assignee.email}" }
      blocks: { to: tracker_ticket, each: fields.blocks, key: "{.}" }
  tracker_person:
    from: fields.assignee
    key: "{email}"
    title: displayName
deletion: missing_from_full_read
"""


def _parse(text: str, base: Path = Path("/data")) -> m.Manifest:
    return m.parse_manifest(textwrap.dedent(text), base_dir=base)


def _refusal(text: str, *, source: str = "tickets", taken=None) -> m.ManifestError:
    with pytest.raises(m.ManifestError) as caught:
        m.compile_manifest(_parse(text), source=source, taken=taken)
    return caught.value


def _codes(error: m.ManifestError) -> set[str]:
    return {problem.code for problem in error.problems}


# ---- what compiles ---------------------------------------------------------


def test_the_design_example_compiles_into_a_strict_entry_and_exact_signature(tmp_path: Path) -> None:
    manifest = _parse(TICKETS, base=tmp_path)
    assert manifest.read.files == str(tmp_path / "exports" / "*.json")
    compiled = m.compile_manifest(manifest, source="tickets")
    assert compiled.output_signature == {
        "nodes": [
            {"subtype": "tracker_ticket"},
            {"subtype": "tracker_ticket_passage"},
            {"subtype": "tracker_person"},
        ],
        "edges": [
            {"src_subtype": "tracker_ticket", "edge_type": "assigned_to", "dst_subtype": "tracker_person"},
            {"src_subtype": "tracker_ticket", "edge_type": "blocks", "dst_subtype": "tracker_ticket"},
            {"src_subtype": "tracker_ticket", "edge_type": "contains", "dst_subtype": "tracker_ticket_passage"},
        ],
    }
    assert compiled.params == {
        "manifest": "${deployment_dir}/sources/tickets.source.yaml", "source": "tickets",
    }
    registration = compiled.registration
    assert registration.module == "okg.substrate.library.sources.manifest_records"
    assert registration.class_name == "ManifestRecordSource"
    assert registration.source_class == "discovery_crawl"
    assert registration.deletion_semantics == "missing_from_completed_scope"
    entry = build_registry_entry(
        manifest_type(registration, summary="tickets"), deployment_name="unit",
        name="tickets", params=compiled.params,
    )
    admission = admit_source_registry_document(
        {"admission_contract": "strict_v1", "sources": {"tickets": entry}},
        registry_path=tmp_path / "source_registry.yaml",
    )
    assert admission.strict and admission.entries["tickets"].legacy is False
    assert entry["admission_policy"]["output_signature"] == compiled.output_signature


def test_generated_schema_composes_with_the_deployment_catalog(tmp_path: Path) -> None:
    deployment = create_deployment_scaffold("unit", deployments_root=tmp_path / "d").deployment_dir
    compiled = m.compile_manifest(_parse(TICKETS, base=tmp_path), source="tickets")
    schemas = deployment / "schemas"
    (schemas / "bridges").mkdir(parents=True, exist_ok=True)
    (schemas / compiled.schema_relpath).write_text(compiled.schema_text)
    (schemas / compiled.bridges_relpath).write_text(compiled.bridges_text)
    composed = compose_catalog(deployment)
    assert set(compiled.node_types) <= set(composed.subtypes)
    assert set(compiled.edge_triples) <= set(composed.narrowing_triples())
    metadata = composed.subtype_metadata["tracker_ticket"]
    assert metadata["title_from"] == "title"
    assert metadata["created_at_from"] == "created_at"
    assert metadata["last_modified_at_from"] == "last_modified_at"
    assert metadata["searchable_text_fields"] == ["title", "body"]
    assert metadata["passage_rollup"] == {
        "edge_type": "contains", "child_subtype": "tracker_ticket_passage",
    }
    assert composed.subtype_metadata["tracker_ticket_passage"]["searchable_text_fields"] == ["text"]


def test_generated_files_carry_the_header_and_detect_hand_edits() -> None:
    compiled = m.compile_manifest(_parse(TICKETS), source="tickets")
    for text in (compiled.schema_text, compiled.bridges_text, compiled.manifest_copy_text):
        header = m.read_generated_header(text)
        assert header is not None and header.source == "tickets" and header.body_intact
        assert header.manifest_sha256 == compiled.manifest.digest
        edited = text.replace("tracker_ticket", "tracker_tickt", 1)
        assert m.read_generated_header(edited).body_intact is False
    assert m.read_generated_header("classes: {}\n") is None


def test_saved_copy_reparses_to_the_same_digest(tmp_path: Path) -> None:
    compiled = m.compile_manifest(_parse(TICKETS, base=tmp_path), source="tickets")
    again = m.parse_manifest(compiled.manifest_copy_text, base_dir=Path("/elsewhere"))
    assert again.digest == compiled.manifest.digest
    assert again.read.files == str(tmp_path / "exports" / "*.json")


@pytest.mark.parametrize("name", ["tracker_ticket", "item", "ticket2", "ab_cd"])
def test_generated_type_names_survive_the_catalog_round_trip(name: str) -> None:
    assert m.type_name_round_trips(name)
    from okg.substrate.catalog.modules_compose import _pascal_to_snake

    assert _pascal_to_snake(m.class_name_for(name)) == name


def test_deletion_defaults_to_missing_from_full_read() -> None:
    manifest = _parse(TICKETS.replace("deletion: missing_from_full_read", ""))
    assert manifest.deletion == "missing_from_full_read"


def test_library_names_include_unloaded_modules() -> None:
    names = m.library_type_names()
    assert {"person", "ticket", "document"} <= names


# ---- one refusal per problem class ----------------------------------------


def test_type_name_that_does_not_round_trip_is_refused_with_a_suggestion() -> None:
    error = _refusal(TICKETS.replace("tracker_person", "sla_2024"))
    assert error.code == "type_name_round_trip"
    assert error.path == "types.sla_2024"
    assert "'sla2024'" in error.fix


def test_library_type_names_are_refused_with_a_prefixed_suggestion() -> None:
    error = _refusal(TICKETS.replace("tracker_", ""))
    assert _codes(error) == {"type_name_reserved"}
    assert {p.path for p in error.problems} == {"types.ticket", "types.person"}
    assert "'tickets_ticket'" in error.problems[0].fix


def test_type_declared_by_another_source_is_refused() -> None:
    error = _refusal(TICKETS, taken={"tracker_person": "source 'crm'"})
    assert error.code == "type_name_taken" and "source 'crm'" in str(error)


def test_snake_case_is_required() -> None:
    error = _refusal(TICKETS.replace("tracker_person:", "TrackerPerson:").replace(
        "to: tracker_person", "to: TrackerPerson"))
    assert error.code == "type_name_invalid" and error.path == "types.TrackerPerson"


def test_key_template_without_placeholder_is_refused() -> None:
    error = _refusal(TICKETS.replace('key: "{email}"', 'key: "fixed"'))
    assert error.code == "key_template_invalid"
    assert error.path == "types.tracker_person.key"


def test_unmatched_brace_in_a_key_is_refused() -> None:
    error = _refusal(TICKETS.replace('key: "{email}"', 'key: "{email"'))
    assert error.code == "key_template_invalid"


def test_missing_key_is_refused_by_the_schema() -> None:
    error = _refusal(TICKETS.replace('    key: "{email}"\n', ""))
    assert error.code == "manifest_invalid"
    assert error.path == "types.tracker_person"


def test_unknown_keys_and_bad_field_types_name_their_path() -> None:
    error = _refusal(TICKETS.replace("type: integer", "type: money"))
    assert error.code == "manifest_invalid"
    assert error.path.startswith("types.tracker_ticket.fields.votes")
    error = _refusal(TICKETS.replace("  records: issues", "  records: issues\n  where: x"))
    assert error.code == "manifest_invalid" and "allowed keys" in error.fix


def test_wrong_version_is_refused() -> None:
    error = _refusal(TICKETS.replace("okg.source/v1", "okg.source/v2"))
    assert error.code == "manifest_invalid" and error.path == "manifest"


def test_reserved_field_names_are_refused() -> None:
    error = _refusal(TICKETS.replace("      status: fields.status.name", "      title: fields.status.name"))
    assert error.code == "field_name_reserved"
    assert error.path == "types.tracker_ticket.fields.title"


def test_link_to_an_undeclared_type_is_refused() -> None:
    error = _refusal(TICKETS.replace("to: tracker_person", "to: crm_contact"))
    assert error.code == "link_target_unknown"
    assert error.path == "types.tracker_ticket.links.assigned_to.to"


def test_containment_cycle_is_refused() -> None:
    error = _refusal(TICKETS.replace(
        "    title: displayName",
        "    title: displayName\n    links:\n      owns: { to: tracker_ticket, key: \"{t}\" }",
    ).replace("      assigned_to:", "      has_part:"))
    assert error.code == "containment_cycle"
    assert "tracker_person, tracker_ticket" in str(error)


@pytest.mark.parametrize("name", ["a_b_c", "sla_2024", "x_1"])
def test_names_with_single_letter_or_digit_parts_do_not_round_trip(name: str) -> None:
    assert not m.type_name_round_trips(name)


def test_self_containment_is_not_a_cycle() -> None:
    compiled = m.compile_manifest(_parse(TICKETS.replace("      blocks:", "      contains:")), source="t")
    assert ("tracker_ticket", "contains", "tracker_ticket") in compiled.edge_triples


def test_exactly_one_top_level_type() -> None:
    error = _refusal(TICKETS.replace("    from: fields.assignee\n", ""))
    assert error.code == "top_level_type"


def test_unknown_format_and_records_on_csv_are_refused() -> None:
    error = _refusal(TICKETS.replace("exports/*.json", "exports/*"))
    assert error.code == "format_unknown" and error.path == "read.format"
    error = _refusal(TICKETS.replace("exports/*.json", "exports/*.csv"))
    assert error.code == "records_path_not_json"


def test_yaml_files_are_a_known_format_and_take_a_records_path() -> None:
    for glob in ("exports/*.yaml", "exports/*.yml"):
        manifest = _parse(TICKETS.replace("exports/*.json", glob))
        read = manifest.read
        assert read.format_for(read.files) == "yaml"


def test_passage_type_name_clash_is_refused() -> None:
    error = _refusal(TICKETS.replace("tracker_person", "tracker_ticket_passage"))
    assert error.code == "type_name_taken"


def test_unreadable_manifest_is_refused(tmp_path: Path) -> None:
    with pytest.raises(m.ManifestError) as caught:
        m.load_manifest(tmp_path / "missing.yaml")
    assert caught.value.code == "manifest_unreadable"
    bad = tmp_path / "bad.yaml"
    bad.write_text("manifest: [\n")
    with pytest.raises(m.ManifestError) as caught:
        m.load_manifest(bad)
    assert caught.value.code == "manifest_unreadable"


def test_glob_matching_no_file_is_refused(tmp_path: Path) -> None:
    manifest = _parse(TICKETS, base=tmp_path)
    with pytest.raises(m.ManifestError) as caught:
        m.read_input_files(manifest)
    assert caught.value.code == "glob_no_match" and caught.value.path == "read.files"


# ---- re-apply: additive only ----------------------------------------------


def test_additive_changes_are_allowed() -> None:
    old = _parse(TICKETS)
    new = _parse(TICKETS.replace(
        "      status: fields.status.name",
        "      status: fields.status.name\n      priority: fields.priority.name",
    ).replace("votes: { from: fields.votes, type: integer }", "votes: { from: fields.votes, type: number }"))
    assert m.additive_change_problems(old, new, manual_path="remove and add") == []


@pytest.mark.parametrize(("change", "code"), [
    (lambda t: t.replace('key: "{key}"', 'key: "{id}"'), "key_changed"),
    (lambda t: t.replace("type: integer", "type: boolean"), "field_type_narrowed"),
    (lambda t: t.replace("      status: fields.status.name\n", ""), "field_removed"),
    (lambda t: t.replace("    text: fields.description\n", ""), "type_removed"),
    (lambda t: t.replace("      blocks: { to: tracker_ticket, each: fields.blocks, key: \"{.}\" }\n", ""),
     "link_removed"),
])
def test_non_additive_changes_are_refused_with_the_manual_path(change, code) -> None:
    old = _parse(TICKETS)
    new = _parse(change(TICKETS))
    problems = m.additive_change_problems(old, new, manual_path="remove and add again")
    assert code in {p.code for p in problems}
    assert all(p.fix == "remove and add again" for p in problems)


def test_renaming_a_type_is_refused() -> None:
    old = _parse(TICKETS)
    new = _parse(TICKETS.replace("tracker_person", "tracker_user"))
    problems = m.additive_change_problems(old, new, manual_path="x")
    assert ("type_removed", "types.tracker_person") in {(p.code, p.path) for p in problems}


def test_schema_document_is_valid_yaml_without_the_header() -> None:
    compiled = m.compile_manifest(_parse(TICKETS), source="tickets")
    schema = yaml.safe_load(compiled.schema_text)
    assert set(schema["classes"]) == {"TrackerTicket", "TrackerTicketPassage", "TrackerPerson"}
    votes = schema["classes"]["TrackerTicket"]["attributes"]["votes"]
    assert votes == {"range": "integer"}


def test_the_schema_example_is_a_valid_manifest() -> None:
    from okg.substrate.schema_resources import AUTHORED_FILE_SCHEMAS, schemas_dir

    assert AUTHORED_FILE_SCHEMAS["source_manifest.yaml"] == "source_manifest.schema.json"
    example = schemas_dir().joinpath("examples", "source_manifest.yaml").read_text()
    compiled = m.compile_manifest(m.parse_manifest(example, base_dir=Path("/")), source="tickets")
    assert "tracker_ticket" in compiled.node_types



# ---- review fixes (69ef1b46d9) ------------------------------------------------


@pytest.mark.parametrize(("change", "code"), [
    (lambda t: t.replace("deletion: missing_from_full_read", "deletion: never"), None),
    (lambda t: t.replace("exports/*.json", "exports/*.jsonl").replace("  records: issues\n", ""), "read_changed"),
    (lambda t: t.replace("records: issues", "records: data.issues"), "read_changed"),
    # Stating the format the extension already implies is no change.
    (lambda t: t.replace("  records: issues", "  records: issues\n  format: json"), None),
    (lambda t: t.replace("  records: issues", "  format: jsonl"), "read_changed"),
    (lambda t: t.replace("from: fields.assignee", "from: fields.reporter"), "from_changed"),
])
def test_input_and_shape_changes_are_refused(change, code) -> None:
    """S2: everything that changes which records exist is refused."""
    old = _parse(TICKETS)
    new = _parse(change(TICKETS))
    codes = {p.code for p in m.additive_change_problems(old, new, manual_path="x")}
    if code is None:
        assert codes == set()
    else:
        assert code in codes


def test_switching_deletion_from_never_is_refused() -> None:
    """S2."""
    old = _parse(TICKETS.replace("deletion: missing_from_full_read", "deletion: never"))
    new = _parse(TICKETS)
    problems = m.additive_change_problems(old, new, manual_path="remove and add again")
    assert [(p.code, p.path, p.fix) for p in problems] == [
        ("deletion_changed", "deletion", "remove and add again"),
    ]


@pytest.mark.parametrize("key", ["a\nb", "a\tb", "a\x00b", "a\u2028b", "a\u200bb", "a\u202eb"])
def test_keys_with_control_characters_are_refused(key: str) -> None:
    """N7."""
    assert m.key_problem(key) is not None
    assert m.key_problem("PROJ-1 a/b") is None
