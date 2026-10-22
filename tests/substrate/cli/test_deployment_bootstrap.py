"""CLI coverage for `okg deployment bootstrap`.

The pure tests pin the agent authoring loop. The postgres test proves
the bootstrapper can drive a real SourceRunner + catalog apply +
Publisher cycle on the fixture slice.
"""
from __future__ import annotations

import json
import shutil
import subprocess
import uuid
from collections.abc import Iterator
from pathlib import Path

import psycopg
import pytest
import yaml

from okg.substrate.cli.main import main
from tests.substrate.conftest import dsn_with_dbname


def _write_bootstrap_fixture(tmp_path: Path) -> tuple[Path, Path]:
    dep = tmp_path / "dep"
    (dep / "fixtures").mkdir(parents=True)
    brief = tmp_path / "brief.md"
    brief.write_text(
        "Track records and policy documents.\n"
        "Which records mention conflicting policy?\n",
        encoding="utf-8",
    )
    (dep / "fixtures" / "records.json").write_text(
        json.dumps([
            {
                "id": "REC-1",
                "summary": "Primary record summary",
                "description": (
                    "This record cites policy A and explains a "
                    "consistency issue."
                ),
            },
            {
                "id": "REC-2",
                "summary": "Second record",
                "description": (
                    "This record cites policy B and has a different date."
                ),
            },
        ]),
        encoding="utf-8",
    )
    (dep / "fixtures" / "corpus").mkdir()
    (dep / "fixtures" / "corpus" / "policy.md").write_text(
        "# Policy Conflict Note\n\n"
        "Policy A and policy B disagree about the same process.",
        encoding="utf-8",
    )
    return dep, brief


def _json_out(capsys) -> dict:
    out = capsys.readouterr()
    text = (out.out or out.err).strip().splitlines()[-1]
    return json.loads(text)


def _prepare_staged_deployment(
    dep: Path,
    brief: Path,
    capsys,
    *,
    deployment_name: str | None = None,
) -> None:
    args = [
        "--json", "deployment", "bootstrap", "plan",
        "--deployment", str(dep),
        "--brief", str(brief),
        "--source", "records=fixtures/records.json",
        "--source", "docs=fixtures/corpus",
        "--apply",
    ]
    if deployment_name:
        args[6:6] = ["--name", deployment_name]
    assert main(args) == 0
    capsys.readouterr()
    assert main([
        "--json", "deployment", "bootstrap", "inspect-source",
        "--deployment", str(dep),
        "--apply",
    ]) == 0
    capsys.readouterr()
    assert main([
        "--json", "deployment", "bootstrap", "draft",
        "--deployment", str(dep),
        "--apply",
    ]) == 0
    capsys.readouterr()


def test_schema_emits_bootstrap_plan_schema(capsys) -> None:
    rc = main(["--json", "schema", "bootstrap_plan.yaml"])
    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["title"] == "OKG bootstrap_plan.yaml"


def test_bootstrap_plan_inspect_draft_structural_status(
    tmp_path: Path,
    capsys,
) -> None:
    dep, brief = _write_bootstrap_fixture(tmp_path)

    rc = main([
        "--json", "deployment", "bootstrap", "plan",
        "--deployment", str(dep),
        "--brief", str(brief),
        "--source", "records=fixtures/records.json",
        "--apply",
    ])
    assert rc == 0
    assert _json_out(capsys)["status"]["stage"] == "needs_source_inspection"

    rc = main([
        "--json", "deployment", "bootstrap", "inspect-source",
        "--deployment", str(dep),
        "records",
        "--apply",
    ])
    assert rc == 0
    inspected = _json_out(capsys)
    assert inspected["record_shapes"][0]["candidate_identifier_fields"] == ["id"]
    assert "description" in inspected["record_shapes"][0]["candidate_text_fields"]

    rc = main([
        "--json", "deployment", "bootstrap", "status",
        "--deployment", str(dep),
    ])
    assert rc == 0
    status_payload = _json_out(capsys)
    assert status_payload["stage"] == "needs_ontology_plan"
    assert status_payload["scorecard"]["sources"]["count"] == 1
    assert status_payload["scorecard"]["sources"]["inspected_count"] == 1
    assert status_payload["scorecard"]["ontology"]["subtype_count"] == 0

    rc = main([
        "--json", "deployment", "bootstrap", "draft",
        "--deployment", str(dep),
        "--apply",
    ])
    assert rc == 0
    drafted = _json_out(capsys)
    assert drafted["status"]["stage"] == "needs_verification"
    assert drafted["status"]["scorecard"]["artifacts"]["staged_file_count"] >= 12
    assert drafted["status"]["scorecard"]["artifacts"]["missing_required_staged"] == []
    assert (dep / ".okg_bootstrap/staged/deployment.yaml").is_file()
    assert (dep / ".okg_bootstrap/staged/source_registry.bootstrap.yaml").is_file()
    assert (dep / ".okg_bootstrap/staged/fixtures/records.json").is_file()
    staged_manifest = yaml.safe_load(
        (dep / ".okg_bootstrap/staged/deployment.yaml").read_text()
    )
    rollout = staged_manifest["nomos"]["rollout"]
    assert rollout["status"] == "deferred"
    from okg.substrate.nomos.rollout import (
        audit_deferred_rollout,
        deployment_dsn_key,
        runtime_enforce_mode_dsn_keys,
    )

    assert audit_deferred_rollout(
        rollout,
        deployment_name=staged_manifest["name"],
        deployment_dsn_key=deployment_dsn_key(staged_manifest),
        enforce_dsn_keys=runtime_enforce_mode_dsn_keys(
            dep / ".okg_bootstrap/staged"
        ),
    ) == []

    rc = main([
        "--json", "deployment", "bootstrap", "verify",
        "--deployment", str(dep),
        "--structural-only",
        "--apply",
    ])
    assert rc == 0
    verified = _json_out(capsys)
    assert verified["verification"]["status"] == "structural_passed"
    assert verified["status"]["stage"] == "needs_verification"
    assert verified["status"]["scorecard"]["verification"]["status"] == "structural_passed"


def test_bootstrap_status_scorecards_existing_deployment_without_plan(
    tmp_path: Path,
    capsys,
) -> None:
    dep = tmp_path / "existing"
    (dep / "schemas" / "bridges").mkdir(parents=True)
    (dep / "deployment.yaml").write_text(
        "name: existing\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "modules: [extraction]\n",
        encoding="utf-8",
    )
    (dep / "source_registry.yaml").write_text(
        "sources:\n"
        "  records:\n"
        "    module: deployment_sources.records\n"
        "    class: RecordsSource\n"
        "    ownership_id: existing.records\n",
        encoding="utf-8",
    )
    (dep / "schemas" / "domain.yaml").write_text("classes: {}\n")
    (dep / "schemas" / "bridges" / "bridges.yaml").write_text("classes: {}\n")
    (dep / "linker_config.yaml").write_text("routes: {}\n")
    (dep / "views_registry.yaml").write_text(
        "deployment: existing\nviews:\n  - name: context_pack\n",
        encoding="utf-8",
    )

    rc = main([
        "--json", "deployment", "bootstrap", "status",
        "--deployment", str(dep),
    ])
    assert rc == 0
    payload = _json_out(capsys)
    assert payload["stage"] == "needs_bootstrap_plan"
    assert payload["scorecard"]["sources"]["count"] == 1
    assert payload["scorecard"]["ontology"]["schema_file_count"] == 1
    assert payload["scorecard"]["ontology"]["bridge_schema_count"] == 1
    assert payload["scorecard"]["extraction"]["has_linker_config"] is True
    assert payload["scorecard"]["views"]["count"] == 1


def test_bootstrap_status_requires_verified_staged_artifacts(
    tmp_path: Path,
    capsys,
) -> None:
    dep, brief = _write_bootstrap_fixture(tmp_path)
    _prepare_staged_deployment(dep, brief, capsys)
    plan_path = dep / "bootstrap_plan.yaml"
    plan = yaml.safe_load(plan_path.read_text())
    plan["verification"]["status"] = "verified"
    plan_path.write_text(yaml.safe_dump(plan))
    shutil.rmtree(dep / ".okg_bootstrap")

    rc = main([
        "--json", "deployment", "bootstrap", "status",
        "--deployment", str(dep),
    ])
    assert rc == 0
    payload = _json_out(capsys)
    assert payload["stage"] == "needs_staged_artifacts"
    assert payload["blocking_stage"] == "read-side surfacing"
    assert payload["next_action"]["command"] == (
        "okg deployment bootstrap draft --apply"
    )
    artifacts = payload["scorecard"]["artifacts"]
    assert artifacts["staged_file_count"] == 0
    assert "deployment.yaml" in artifacts["missing_required_staged"]
    assert "deployment.yaml" in artifacts["missing_verified_staged"]
    assert "fixtures/records.json" in artifacts["missing_verified_staged"]


def test_bootstrap_draft_preserves_production_source_adapter(
    tmp_path: Path,
    capsys,
) -> None:
    dep, brief = _write_bootstrap_fixture(tmp_path)
    assert main([
        "--json", "deployment", "bootstrap", "plan",
        "--deployment", str(dep),
        "--brief", str(brief),
        "--source", "records=fixtures/records.json",
        "--apply",
    ]) == 0
    capsys.readouterr()
    assert main([
        "--json", "deployment", "bootstrap", "inspect-source",
        "--deployment", str(dep),
        "--apply",
    ]) == 0
    capsys.readouterr()

    plan_path = dep / "bootstrap_plan.yaml"
    plan = yaml.safe_load(plan_path.read_text())
    plan["sources"][0]["adapter"] = {
        "module": "deployment_sources.records",
        "class": "RecordsSource",
    }
    plan["sources"][0]["params"] = {"root": "/data/records"}
    plan["sources"][0]["profile"] = {
        "source_class": "mutable_api",
        "record_identity_kind": "remote_id",
        "record_identity_fields": ["id"],
        "source_revision_kind": "updated_at",
        "deletion_semantics": "explicit_delete",
        "publication_mode": "published_generation",
    }
    plan_path.write_text(yaml.safe_dump(plan))

    assert main([
        "--json", "deployment", "bootstrap", "draft",
        "--deployment", str(dep),
        "--apply",
    ]) == 0
    capsys.readouterr()

    staged = dep / ".okg_bootstrap/staged"
    production_registry = yaml.safe_load(
        (staged / "source_registry.yaml").read_text()
    )
    bootstrap_registry = yaml.safe_load(
        (staged / "source_registry.bootstrap.yaml").read_text()
    )
    production = production_registry["sources"]["records"]
    assert production["module"] == "deployment_sources.records"
    assert production["class"] == "RecordsSource"
    assert production["params"] == {"root": "/data/records"}
    assert production["source_class"] == "mutable_api"
    assert production["record_identity_fields"] == ["id"]
    assert "schedule" not in production
    bootstrap = bootstrap_registry["sources"]["records"]
    assert bootstrap["module"] == "okg.substrate.library.sources.bootstrap_fixture"
    assert bootstrap["params"]["fixture_path"].endswith("fixtures/records.json")
    assert "schedule" not in bootstrap


def test_bootstrap_drafts_registries_the_strict_contract_admits(
    tmp_path: Path,
    capsys,
) -> None:
    """Issue #2952: the drafted registry failed the first add-source.

    Bootstrap wrote a per-source `schedule` block. `okg deployment
    add-source` promotes the registry to strict_v1, whose schema refuses that
    key, so adding a source to a quickstart deployment failed at import. Both
    drafted registries must pass the strict schema as add-source writes them.
    """
    from okg.substrate.sources.guided_add import promote_registry_to_strict
    from okg.substrate.sources.registry import _validate_strict_schema

    dep, brief = _write_bootstrap_fixture(tmp_path)
    _prepare_staged_deployment(dep, brief, capsys)

    staged = dep / ".okg_bootstrap/staged"
    for name in ("source_registry.yaml", "source_registry.bootstrap.yaml"):
        path = staged / name
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        for entry in document["sources"].values():
            assert "schedule" not in entry, name
        promoted, _ = promote_registry_to_strict(document)
        _validate_strict_schema(promoted, path)


def test_bootstrap_refuses_a_plan_source_schedule_by_name(
    tmp_path: Path,
    capsys,
) -> None:
    """A plan that still authors `sources[].schedule` is refused, not dropped.

    The key used to be copied into the registry, where nothing read it and the
    strict contract refuses it (#2952). Silently dropping it would hide an
    author's intent, so the plan is refused with the source named.
    """
    dep, brief = _write_bootstrap_fixture(tmp_path)
    assert main([
        "--json", "deployment", "bootstrap", "plan",
        "--deployment", str(dep),
        "--brief", str(brief),
        "--source", "records=fixtures/records.json",
        "--apply",
    ]) == 0
    capsys.readouterr()
    plan_path = dep / "bootstrap_plan.yaml"
    plan = yaml.safe_load(plan_path.read_text())
    plan["sources"][0]["schedule"] = {"reconcile_interval": "24h"}
    plan_path.write_text(yaml.safe_dump(plan))

    rc = main([
        "--json", "deployment", "bootstrap", "draft",
        "--deployment", str(dep),
        "--apply",
    ])

    assert rc == 1
    err = _json_out(capsys)["error"]
    assert err["code"] == "bootstrap_plan_invalid"
    assert err["data"]["errors"] == ["sources[records].schedule"]
    assert "sync.poll" in err["fix_hint"]
    assert not (dep / ".okg_bootstrap/staged/source_registry.yaml").exists()


def test_bootstrap_draft_replaces_stale_staged_files(
    tmp_path: Path,
    capsys,
) -> None:
    dep, brief = _write_bootstrap_fixture(tmp_path)
    _prepare_staged_deployment(dep, brief, capsys)

    staged = dep / ".okg_bootstrap/staged"
    assert (staged / "fixtures/docs.json").is_file()

    plan_path = dep / "bootstrap_plan.yaml"
    plan = yaml.safe_load(plan_path.read_text())
    plan["sources"] = [
        source for source in plan["sources"]
        if source["id"] == "records"
    ]
    plan_path.write_text(yaml.safe_dump(plan))

    assert main([
        "--json", "deployment", "bootstrap", "draft",
        "--deployment", str(dep),
        "--apply",
    ]) == 0
    drafted = _json_out(capsys)

    hashes = drafted["staged_hashes"]
    assert "fixtures/records.json" in hashes
    assert "fixtures/docs.json" not in hashes
    assert not (staged / "fixtures/docs.json").exists()


def test_bootstrap_refuses_live_source_without_fixture(
    tmp_path: Path,
    capsys,
) -> None:
    dep = tmp_path / "dep"
    dep.mkdir()
    plan = yaml.safe_load(
        (Path("schemas/examples/bootstrap_plan/empty.yaml")).read_text()
    )
    plan["deployment"]["path"] = str(dep)
    plan["sources"] = [{"id": "live_api", "kind": "api"}]
    (dep / "bootstrap_plan.yaml").write_text(yaml.safe_dump(plan))

    rc = main([
        "--json", "deployment", "bootstrap", "inspect-source",
        "--deployment", str(dep),
        "live_api",
        "--apply",
    ])
    assert rc == 1
    err = _json_out(capsys)["error"]
    assert err["code"] == "bootstrap_source_sample_failed"
    assert err["data"]["reason"] == "live_sampling_not_enabled"


def test_bootstrap_inspect_supports_local_sample_formats(
    tmp_path: Path,
    capsys,
) -> None:
    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "records.json").write_text('[{"id": "J1", "text": "json text"}]')
    (dep / "records.csv").write_text("id,summary\nC1,csv text\n")
    (dep / "doc.md").write_text("# Markdown Title\n\nmarkdown body")
    corpus = dep / "corpus"
    corpus.mkdir()
    (corpus / "nested.md").write_text("# Nested\n\nnested body")
    plan = yaml.safe_load(
        (Path("schemas/examples/bootstrap_plan/empty.yaml")).read_text()
    )
    plan["deployment"]["path"] = str(dep)
    plan["sources"] = [
        {"id": "json_src", "kind": "local_json", "sample_path": "records.json"},
        {"id": "csv_src", "kind": "local_csv", "sample_path": "records.csv"},
        {"id": "md_src", "kind": "local_markdown", "sample_path": "doc.md"},
        {"id": "dir_src", "kind": "local_directory", "sample_path": "corpus"},
    ]
    (dep / "bootstrap_plan.yaml").write_text(yaml.safe_dump(plan))

    rc = main([
        "--json", "deployment", "bootstrap", "inspect-source",
        "--deployment", str(dep),
        "--apply",
    ])
    assert rc == 0
    payload = _json_out(capsys)
    assert payload["inspected"] == ["json_src", "csv_src", "md_src", "dir_src"]
    shapes = {shape["source_id"]: shape for shape in payload["record_shapes"]}
    assert shapes["json_src"]["candidate_identifier_fields"] == ["id"]
    assert shapes["csv_src"]["candidate_text_fields"] == ["summary"]
    assert shapes["md_src"]["candidate_text_fields"] == ["text", "title"]
    assert shapes["dir_src"]["record_count"] == 1


def test_bootstrap_invalid_plan_reports_schema_error(
    tmp_path: Path,
    capsys,
) -> None:
    dep = tmp_path / "dep"
    dep.mkdir()
    (dep / "deployment.yaml").write_text("name: dep\n", encoding="utf-8")
    plan = yaml.safe_load(
        (Path("schemas/examples/bootstrap_plan/empty.yaml")).read_text()
    )
    del plan["deployment"]["name"]
    (dep / "bootstrap_plan.yaml").write_text(yaml.safe_dump(plan))

    rc = main([
        "--json", "deployment", "bootstrap", "status",
        "--deployment", str(dep),
    ])
    assert rc == 1
    invalid_status = _json_out(capsys)
    assert invalid_status["ok"] is False
    assert invalid_status["last_failure_code"] == "bootstrap_plan_invalid"

    rc = main([
        "--json", "deployment", "bootstrap", "draft",
        "--deployment", str(dep),
        "--apply",
    ])
    assert rc == 1
    err = _json_out(capsys)["error"]
    assert err["code"] == "bootstrap_plan_invalid"
    assert err["data"]["errors"]


def test_bootstrap_apply_refuses_changed_staged_hash(
    tmp_path: Path,
    capsys,
) -> None:
    dep, brief = _write_bootstrap_fixture(tmp_path)
    _prepare_staged_deployment(dep, brief, capsys)

    plan = yaml.safe_load((dep / "bootstrap_plan.yaml").read_text())
    plan["verification"]["status"] = "verified"
    (dep / "bootstrap_plan.yaml").write_text(yaml.safe_dump(plan))
    staged_deployment = dep / ".okg_bootstrap/staged/deployment.yaml"
    staged_deployment.write_text(
        staged_deployment.read_text() + "\n# unverified edit\n"
    )

    rc = main([
        "--json", "deployment", "bootstrap", "apply",
        "--deployment", str(dep),
        "--apply",
    ])
    assert rc == 1
    err = _json_out(capsys)["error"]
    assert err["code"] == "bootstrap_unverified_apply"
    assert "deployment.yaml" in err["data"]["changed_files"]


def test_bootstrap_apply_refuses_changed_plan_hash(
    tmp_path: Path,
    capsys,
) -> None:
    dep, brief = _write_bootstrap_fixture(tmp_path)
    _prepare_staged_deployment(dep, brief, capsys)
    assert main([
        "--json", "deployment", "bootstrap", "verify",
        "--deployment", str(dep),
        "--structural-only",
        "--apply",
    ]) == 0
    capsys.readouterr()

    plan_path = dep / "bootstrap_plan.yaml"
    plan = yaml.safe_load(plan_path.read_text())
    plan["verification"]["status"] = "verified"
    plan["domain_brief"]["text"] = "Changed after verification."
    plan_path.write_text(yaml.safe_dump(plan))

    rc = main([
        "--json", "deployment", "bootstrap", "apply",
        "--deployment", str(dep),
        "--apply",
    ])
    assert rc == 1
    err = _json_out(capsys)["error"]
    assert err["code"] == "bootstrap_unverified_apply"
    assert err["data"]["changed_files"] == ["bootstrap_plan.yaml"]


def test_bootstrap_draft_preserves_custom_ontology_and_narrowings(
    tmp_path: Path,
    capsys,
) -> None:
    dep, brief = _write_bootstrap_fixture(tmp_path)
    assert main([
        "--json", "deployment", "bootstrap", "plan",
        "--deployment", str(dep),
        "--brief", str(brief),
        "--source", "records=fixtures/records.json",
        "--apply",
    ]) == 0
    capsys.readouterr()
    assert main([
        "--json", "deployment", "bootstrap", "inspect-source",
        "--deployment", str(dep),
        "--apply",
    ]) == 0
    capsys.readouterr()

    plan_path = dep / "bootstrap_plan.yaml"
    plan = yaml.safe_load(plan_path.read_text())
    plan["ontology_plan"]["subtypes"] = [
        {
            "name": "AircraftRecord",
            "description": "Tracked aircraft maintenance record.",
            "archetype": "artifact",
            "attrs": ["aircraft_id", "title", "text", "source_id"],
            "searchable_text_fields": ["title", "text"],
            "alias_fields": ["aircraft_id"],
        },
        {
            "name": "MaintenanceIssue",
            "description": "Observed maintenance issue.",
            "archetype": "observation",
            "attrs": ["issue_id", "label", "text", "source_id"],
            "searchable_text_fields": ["label", "text"],
        },
        {
            "name": "mission note",
            "description": "Mission note connected to the issue.",
            "archetype": "artifact",
            "attrs": ["note_id", "title", "text"],
            "searchable_text_fields": ["title", "text"],
        },
    ]
    plan["narrowing_plan"] = [
        {"parent": "AircraftRecord", "edge": "contains", "child": "MaintenanceIssue"},
        {"parent": "MaintenanceIssue", "edge": "references", "child": "mission note"},
        {"parent": "AircraftRecord", "edge": "references", "child": "Document"},
        {"parent": "Document", "edge": "references", "child": "AircraftRecord"},
    ]
    plan["extractor_plan"]["entity_patterns"] = [
        {
            "id_type": "policy_id",
            "regex": r"POL-[0-9]+",
            "description": "Policy identifier",
        }
    ]
    plan["extractor_plan"]["linker_routes"] = [
        {
            "entity_type": "policy_ref",
            "src_subtype": "AircraftRecord",
            "edge_type": "references",
            "dst_template": "{resolved}",
            "confidence": 0.82,
        }
    ]
    plan["extractor_plan"]["aliases"] = [
        {
            "alias": "Policy A",
            "canonical": "document:policy-a",
            "scope": "document",
        }
    ]
    plan_path.write_text(yaml.safe_dump(plan))

    assert main([
        "--json", "deployment", "bootstrap", "draft",
        "--deployment", str(dep),
        "--apply",
    ]) == 0
    capsys.readouterr()

    staged = dep / ".okg_bootstrap/staged"
    domain = yaml.safe_load((staged / "schemas/bootstrap_domain.yaml").read_text())
    bridges = yaml.safe_load(
        (staged / "schemas/bridges/bootstrap_bridges.yaml").read_text()
    )
    assert set(domain["classes"]) >= {
        "AircraftRecord",
        "MaintenanceIssue",
        "MissionNote",
    }
    bridge_attrs = bridges["classes"]["EdgeNarrowings"]["attributes"]
    assert (
        bridge_attrs["maintenance_issue_references_mission_note"]["annotations"]
        ["dst_subtypes"]
        == "MissionNote"
    )
    linker = yaml.safe_load((staged / "linker_config.yaml").read_text())
    assert (
        linker["routes"]["policy_ref"]["AircraftRecord"]["dst_template"]
        == "{resolved}"
    )
    assert (
        linker["routes"]["policy_ref"]["AircraftRecord"]["confidence"]
        == 0.82
    )
    aliases = json.loads((staged / "aliases.json").read_text())
    assert aliases["Policy A"] == {
        "canonical": "document:policy-a",
        "scope": "document",
    }
    identifier_patterns = yaml.safe_load(
        (staged / "identifier_patterns.yaml").read_text()
    )
    assert identifier_patterns["patterns"] == [
        {
            "id_type": "policy_id",
            "regex": r"POL-[0-9]+",
            "description": "Policy identifier",
        }
    ]

    from okg.substrate.catalog.modules_compose import compose_catalog

    composed = compose_catalog(staged)
    assert {"aircraft_record", "maintenance_issue", "mission_note"} <= composed.subtypes
    assert (
        "maintenance_issue",
        "references",
        "mission_note",
    ) in {
        (n.parent_subtype, n.edge_archetype, n.child_subtype)
        for n in composed.narrowings
    }
    assert {
        "alias": "Policy A",
        "canonical": "document:policy-a",
        "scope": "document",
    } in composed.aliases
    assert {
        "id_type": "policy_id",
        "regex": r"POL-[0-9]+",
        "validator_kind": None,
        "description": "Policy identifier",
    } in composed.identifier_patterns
    fixture = json.loads((staged / "fixtures/records.json").read_text())
    assert {node["subtype"] for node in fixture["nodes"]} >= {
        "aircraft_record",
        "maintenance_issue",
    }


def test_bootstrap_verify_maps_bad_narrowing_to_catalog_stage(
    tmp_path: Path,
    capsys,
) -> None:
    dep, brief = _write_bootstrap_fixture(tmp_path)
    _prepare_staged_deployment(dep, brief, capsys)
    bridge_path = dep / ".okg_bootstrap/staged/schemas/bridges/bootstrap_bridges.yaml"
    bridges = yaml.safe_load(bridge_path.read_text())
    bridge_attrs = bridges["classes"]["EdgeNarrowings"]["attributes"]
    first_attr = next(iter(bridge_attrs.values()))
    first_attr["annotations"]["dst_subtypes"] = "MissingSubtype"
    bridge_path.write_text(yaml.safe_dump(bridges))

    rc = main([
        "--json", "deployment", "bootstrap", "verify",
        "--deployment", str(dep),
        "--structural-only",
    ])
    assert rc == 1
    err = _json_out(capsys)["error"]
    assert err["code"] == "bootstrap_catalog_rejected"
    assert err["data"]["stage"] == "narrowing-trigger acceptance"
    assert err["data"]["error"]["code"] == "bridge_subtype_unknown"


def test_bootstrap_verify_maps_linker_route_miss_to_enrichment_stage(
    tmp_path: Path,
    capsys,
) -> None:
    dep, brief = _write_bootstrap_fixture(tmp_path)
    _prepare_staged_deployment(dep, brief, capsys)
    linker_path = dep / ".okg_bootstrap/staged/linker_config.yaml"
    linker_path.write_text(yaml.safe_dump({
        "routes": {
            "policy": {
                "MissingSubtype": {
                    "edge_type": "references",
                    "dst_template": "{value}",
                }
            }
        }
    }))

    rc = main([
        "--json", "deployment", "bootstrap", "verify",
        "--deployment", str(dep),
        "--structural-only",
    ])
    assert rc == 1
    err = _json_out(capsys)["error"]
    assert err["code"] == "bootstrap_catalog_rejected"
    assert err["data"]["stage"] == "enrichment"
    assert err["data"]["error"]["data"]["src_subtype"] == "MissingSubtype"


def test_bootstrap_draft_writes_the_packaged_file_readers(
    tmp_path: Path,
    capsys,
) -> None:
    # #2974: an empty extractors.yaml left every folder or upload source
    # added later reading file names only, with no searchable text.
    from okg.substrate.sources.guided_defaults import EXTRACTORS_YAML

    dep, brief = _write_bootstrap_fixture(tmp_path)
    _prepare_staged_deployment(dep, brief, capsys)
    staged = dep / ".okg_bootstrap/staged/extractors.yaml"
    assert staged.read_text(encoding="utf-8") == EXTRACTORS_YAML.read_text(encoding="utf-8")
    rc = main([
        "--json", "deployment", "bootstrap", "verify",
        "--deployment", str(dep),
        "--structural-only",
    ])
    assert rc == 0
    checks = {c["name"]: c for c in _json_out(capsys)["verification"]["checks"]}
    assert checks["extractors"]["summary"] == {
        "parser_count": 5, "chunker_count": 2, "ner_route_count": 0,
    }


def test_bootstrap_verify_maps_bad_extractor_to_projection_stage(
    tmp_path: Path,
    capsys,
) -> None:
    dep, brief = _write_bootstrap_fixture(tmp_path)
    _prepare_staged_deployment(dep, brief, capsys)
    extractors_path = dep / ".okg_bootstrap/staged/extractors.yaml"
    extractors_path.write_text(yaml.safe_dump({
        "parser": [
            {
                "format": "markdown",
                "class": "okg.substrate.library.extraction.parse.missing.Nope",
            }
        ],
        "chunker": [],
        "ner": [],
    }))

    rc = main([
        "--json", "deployment", "bootstrap", "verify",
        "--deployment", str(dep),
        "--structural-only",
    ])
    assert rc == 1
    err = _json_out(capsys)["error"]
    assert err["code"] == "bootstrap_draft_invalid"
    assert err["data"]["stage"] == "projection"
    assert "ExtractorChainMissing" in err["data"]["errors"][0]


def _commit_tree(root: Path) -> None:
    """Commit everything under ``root``, as an operator does before a publish.

    A publish reads the committed tree: uncommitted deployment inputs are
    refused ("repository provenance inputs are not committed"), and the
    refusal's own fix is to commit them and re-run.
    """
    git = ["git", "-C", str(root), "-c", "user.name=okg-test",
           "-c", "user.email=okg-test@example.invalid"]
    subprocess.run([*git, "add", "-A"], check=True, capture_output=True)
    subprocess.run(
        [*git, "commit", "-q", "-m", "bootstrap draft"],
        check=True,
        capture_output=True,
    )


@pytest.fixture
def bootstrap_database(okg_migrated: str, tmp_path: Path) -> Iterator[str]:
    """A migrated database with no catalog history, for one bootstrap publish.

    `bootstrap verify --apply` publishes a NEW deployment, and the catalog
    ownership guard refuses to let a new writer claim a database that already
    holds another writer's catalog history (`catalog_ownership_unclaimed`).
    The shared per-worker test database always holds such history, because
    `clean_runtime` keeps the catalog between tests on purpose. So the
    publishing tests get their own database, as a real bootstrap does.

    The test's ``tmp_path`` also becomes a Git repository. A publish resolves
    the deployment's repository revision, and a deployment outside every
    repository is served only when it declares ``release.kind: standalone``
    (#1962); the bootstrap draft declares no release kind, so a real
    bootstrap runs inside the operator's checkout.

    It depends on ``okg_migrated`` rather than the bare DSN: that fixture
    creates the cluster roles migrations grant to (``okg_substrate``) BEFORE
    any migrate runs, and the grants are ledgered, so a database migrated
    without them silently lacks them (issue #2020). The migrate below repeats
    the same steps ``okg_migrated`` takes for the shared database.
    """
    subprocess.run(
        ["git", "init", "-q", str(tmp_path)],
        check=True,
        capture_output=True,
    )
    from okg.substrate.db import migrate as migration
    from okg.substrate.db.management import apply_remove_job_runtime_subsystem

    name = f"okg_test_bootstrap_{uuid.uuid4().hex[:12]}"
    admin = dsn_with_dbname(okg_migrated, "postgres")
    dsn = dsn_with_dbname(okg_migrated, name)
    with psycopg.connect(admin, autocommit=True) as conn:
        conn.execute(psycopg.sql.SQL("CREATE DATABASE {}").format(
            psycopg.sql.Identifier(name)
        ))
    try:
        original = dict(migration.REQUIRED_EXTENSIONS)
        migration.REQUIRED_EXTENSIONS.pop("pg_cron", None)
        try:
            migration.ensure_required_extensions(dsn)
            migration.migrate(dsn)
        finally:
            migration.REQUIRED_EXTENSIONS.clear()
            migration.REQUIRED_EXTENSIONS.update(original)
        apply_remove_job_runtime_subsystem(dsn, confirm=True)
        yield dsn
    finally:
        from okg.substrate.db.pool import close_all

        close_all()
        with psycopg.connect(admin, autocommit=True) as conn:
            conn.execute(psycopg.sql.SQL("DROP DATABASE {} WITH (FORCE)").format(
                psycopg.sql.Identifier(name)
            ))


@pytest.mark.postgres
def test_bootstrap_verify_runs_real_publish(
    tmp_path: Path,
    bootstrap_database: str,
    capsys,
) -> None:
    dep, brief = _write_bootstrap_fixture(tmp_path)
    deployment_name = f"boot{tmp_path.name.lower().replace('-', '')}"
    _prepare_staged_deployment(
        dep,
        brief,
        capsys,
        deployment_name=deployment_name,
    )

    _commit_tree(tmp_path)
    rc = main([
        "--json", "deployment", "bootstrap", "verify",
        "--deployment", str(dep),
        "--dsn", bootstrap_database,
        "--apply",
    ])
    assert rc == 0
    payload = _json_out(capsys)
    verification = payload["verification"]
    assert verification["status"] == "verified"
    assert str(verification["generation_id"]).startswith("okg:")
    assert verification["node_row_delta"] > 0
    assert verification["edge_row_delta"] > 0
    assert verification["metrics_snapshot"]["dlq_count"] == 0


@pytest.mark.postgres
def test_bootstrap_verify_installs_planned_aliases(
    tmp_path: Path,
    bootstrap_database: str,
    capsys,
) -> None:
    dep, brief = _write_bootstrap_fixture(tmp_path)
    deployment_name = f"alias{tmp_path.name.lower().replace('-', '')}"
    assert main([
        "--json", "deployment", "bootstrap", "plan",
        "--deployment", str(dep),
        "--name", deployment_name,
        "--brief", str(brief),
        "--source", "records=fixtures/records.json",
        "--apply",
    ]) == 0
    capsys.readouterr()
    assert main([
        "--json", "deployment", "bootstrap", "inspect-source",
        "--deployment", str(dep),
        "--apply",
    ]) == 0
    capsys.readouterr()
    plan_path = dep / "bootstrap_plan.yaml"
    plan = yaml.safe_load(plan_path.read_text())
    plan["extractor_plan"]["entity_patterns"] = [
        {
            "id_type": "policy_id",
            "regex": r"POL-[0-9]+",
            "description": "Policy identifier",
        }
    ]
    plan["extractor_plan"]["aliases"] = [
        {
            "alias": "Policy A",
            "canonical": "document:policy-a",
            "scope": "document",
        }
    ]
    plan_path.write_text(yaml.safe_dump(plan))
    assert main([
        "--json", "deployment", "bootstrap", "draft",
        "--deployment", str(dep),
        "--apply",
    ]) == 0
    capsys.readouterr()

    _commit_tree(tmp_path)
    rc = main([
        "--json", "deployment", "bootstrap", "verify",
        "--deployment", str(dep),
        "--dsn", bootstrap_database,
    ])
    assert rc == 0
    _json_out(capsys)
    with psycopg.connect(bootstrap_database, autocommit=True) as conn, \
         conn.cursor() as cur:
        cur.execute(
            """
            SELECT canonical, scope
              FROM okg.aliases
             WHERE alias = 'Policy A'
             ORDER BY catalog_version_id DESC
             LIMIT 1
            """
        )
        assert cur.fetchone() == ("document:policy-a", "document")
        cur.execute(
            """
            SELECT regex, description
              FROM okg.identifier_patterns
             WHERE id_type = 'policy_id'
             ORDER BY catalog_version_id DESC
             LIMIT 1
            """
        )
        assert cur.fetchone() == (r"POL-[0-9]+", "Policy identifier")


@pytest.mark.postgres
def test_bootstrap_verify_maps_missing_source_adapter_to_source_stage(
    tmp_path: Path,
    bootstrap_database: str,
    capsys,
) -> None:
    dep, brief = _write_bootstrap_fixture(tmp_path)
    deployment_name = f"badsrc{tmp_path.name.lower().replace('-', '')}"
    _prepare_staged_deployment(
        dep,
        brief,
        capsys,
        deployment_name=deployment_name,
    )
    registry_path = dep / ".okg_bootstrap/staged/source_registry.bootstrap.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    registry["sources"]["records"]["module"] = "okg.substrate.missing_adapter"
    registry_path.write_text(yaml.safe_dump(registry))

    rc = main([
        "--json", "deployment", "bootstrap", "verify",
        "--deployment", str(dep),
        "--dsn", bootstrap_database,
    ])
    assert rc == 1
    err = _json_out(capsys)["error"]
    assert err["code"] == "bootstrap_source_sample_failed"
    assert err["data"]["stage"] == "source coverage"
    assert "ModuleNotFoundError" in err["data"]["reason"]


@pytest.mark.postgres
def test_bootstrap_verify_maps_blocking_invariant_to_gate_stage(
    tmp_path: Path,
    bootstrap_database: str,
    capsys,
) -> None:
    dep, brief = _write_bootstrap_fixture(tmp_path)
    deployment_name = f"badgate{tmp_path.name.lower().replace('-', '')}"
    assert main([
        "--json", "deployment", "bootstrap", "plan",
        "--deployment", str(dep),
        "--name", deployment_name,
        "--brief", str(brief),
        "--source", "records=fixtures/records.json",
        "--apply",
    ]) == 0
    capsys.readouterr()
    assert main([
        "--json", "deployment", "bootstrap", "inspect-source",
        "--deployment", str(dep),
        "--apply",
    ]) == 0
    capsys.readouterr()
    plan_path = dep / "bootstrap_plan.yaml"
    plan = yaml.safe_load(plan_path.read_text())
    plan["quality_plan"]["invariants"] = [
        {
            "name": "always_block",
            "severity": "error",
            "scope": "cross",
            "sql": "SELECT 'bootstrap'::text AS node_id LIMIT 1",
        }
    ]
    plan_path.write_text(yaml.safe_dump(plan))
    assert main([
        "--json", "deployment", "bootstrap", "draft",
        "--deployment", str(dep),
        "--apply",
    ]) == 0
    capsys.readouterr()

    _commit_tree(tmp_path)
    rc = main([
        "--json", "deployment", "bootstrap", "verify",
        "--deployment", str(dep),
        "--dsn", bootstrap_database,
    ])
    assert rc == 1
    err = _json_out(capsys)["error"]
    assert err["code"] == "bootstrap_publish_blocked"
    assert err["data"]["stage"] == "gate evaluation"
    assert err["data"]["error"]["status"] == "blocked"


def test_bootstrap_plan_warns_when_the_name_will_not_reach_the_folder(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    """Issue #2951: a quickstart deployment could not be refreshed by name.

    `okg run` and `okg ingest --deployment <name>` search only the
    deployments roots, and bootstrap accepted any folder without saying so.
    Plan now warns (it does not refuse) and names the export that fixes it.
    """
    monkeypatch.delenv("OKG_DEPLOYMENTS_DIR", raising=False)
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
    monkeypatch.chdir(tmp_path)
    dep, brief = _write_bootstrap_fixture(tmp_path)

    assert main([
        "--json", "deployment", "bootstrap", "plan",
        "--deployment", str(dep),
        "--brief", str(brief),
        "--source", "records=fixtures/records.json",
        "--apply",
    ]) == 0

    captured = capsys.readouterr()
    payload = json.loads(captured.out.strip().splitlines()[-1])
    assert payload["ok"] is True
    lookup = payload["name_lookup"]
    assert lookup["deployment"] == "dep"
    assert lookup["resolves_by_name"] is False
    export = f"export OKG_DEPLOYMENTS_DIR={dep.parent.resolve()}"
    assert lookup["export"] == export
    assert payload["warnings"] == [lookup["warning"]]
    assert export in lookup["warning"]
    assert f"--deployment {dep.resolve()}" in lookup["warning"]
    assert f"warning: {lookup['warning']}" in captured.err
    assert (dep / "bootstrap_plan.yaml").is_file()


def test_bootstrap_plan_is_quiet_when_the_name_reaches_the_folder(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    """The quickstart layout: `<root>/deployments/<name>` with the export set."""
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
    dep, brief = _write_bootstrap_fixture(tmp_path)

    assert main([
        "--json", "deployment", "bootstrap", "plan",
        "--deployment", str(dep),
        "--brief", str(brief),
        "--source", "records=fixtures/records.json",
        "--apply",
    ]) == 0

    captured = capsys.readouterr()
    payload = json.loads(captured.out.strip().splitlines()[-1])
    assert payload["name_lookup"]["resolves_by_name"] is True
    assert "warnings" not in payload
    assert "warning:" not in captured.err


def test_bootstrap_name_advice_for_a_folder_named_differently(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """A folder not named after the deployment cannot be fixed by an export."""
    from okg.substrate.deployment_bootstrap.workflow import name_lookup_advice

    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    monkeypatch.setenv("XDG_DATA_HOME", str(tmp_path / "xdg"))
    folder = tmp_path / "deployment"
    folder.mkdir()

    advice = name_lookup_advice(folder, "akmon-quickstart")

    assert advice["resolves_by_name"] is False
    assert "export" not in advice
    assert "<root>/akmon-quickstart" in advice["warning"]
    assert f"--deployment {folder}" in advice["warning"]


def _mark_verified(dep: Path) -> None:
    """Record a verification the way verify does, without a database."""
    from okg.substrate.deployment_bootstrap.workflow import (
        _hash_authoring_plan,
        _hash_tree,
        load_plan,
        save_plan,
        staged_dir,
    )

    plan = load_plan(dep)
    plan["verification"].update({
        "status": "verified",
        "staged_hashes": _hash_tree(staged_dir(dep)),
        "plan_hash": _hash_authoring_plan(plan),
    })
    save_plan(dep, plan)


def test_bootstrap_apply_refuses_without_a_database_before_writing(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    """Issue #2867: apply now hands the catalog over, so it needs the database.

    With no `--dsn` and the staged manifest's `${OKG_DSN}` unset, apply must
    refuse before copying anything, and the plan must stay `verified` so
    the same command can be run again.
    """
    monkeypatch.delenv("OKG_DSN", raising=False)
    dep, brief = _write_bootstrap_fixture(tmp_path)
    _prepare_staged_deployment(dep, brief, capsys)
    _mark_verified(dep)

    rc = main([
        "--json", "deployment", "bootstrap", "apply",
        "--deployment", str(dep),
        "--apply",
    ])

    assert rc == 1
    err = _json_out(capsys)["error"]
    assert err["code"] == "bootstrap_publish_blocked"
    assert err["data"]["error"] == "dsn_missing"
    assert err["data"]["stage"] == "catalog handover"
    assert "--dsn" in err["fix_hint"]
    assert not (dep / "deployment.yaml").exists()
    plan = yaml.safe_load((dep / "bootstrap_plan.yaml").read_text())
    assert plan["verification"]["status"] == "verified"


def test_bootstrap_apply_dry_run_reports_the_planned_handover(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    monkeypatch.delenv("OKG_DSN", raising=False)
    dep, brief = _write_bootstrap_fixture(tmp_path)
    _prepare_staged_deployment(dep, brief, capsys)
    _mark_verified(dep)

    assert main([
        "--json", "deployment", "bootstrap", "apply",
        "--deployment", str(dep),
    ]) == 0

    payload = _json_out(capsys)
    assert payload["applied"] is False
    assert payload["catalog_handover"] == {
        "planned": True,
        "deployment_root": str(dep.resolve()),
    }
    assert not (dep / "deployment.yaml").exists()


@pytest.mark.parametrize(
    ("recorded_root", "expect_force"),
    [
        ("{dep}/.okg_bootstrap/staged", True),
        # The same staged copy recorded through a symlinked path.
        ("{link}/.okg_bootstrap/staged", True),
        ("{dep}", False),
        ("{other}/.okg_bootstrap/staged", False),
    ],
)
def test_bootstrap_apply_takes_over_only_from_its_own_staged_copy(
    tmp_path: Path,
    monkeypatch,
    recorded_root: str,
    expect_force: bool,
) -> None:
    """Issue #2867: apply forces the claim only over its own staged copy.

    Verify records `.okg_bootstrap/staged` as the catalog writer. Apply takes
    over from exactly that owner, even when verify ran in the same process,
    so its holder is still alive. Any other recorded owner goes through the
    ordinary unforced claim, which refuses a live foreign writer.
    """
    import contextlib

    import okg.substrate.catalog.modules_apply as modules_apply
    import okg.substrate.catalog.modules_compose as modules_compose
    import okg.substrate.catalog.ownership as ownership
    import okg.substrate.db.connection as connection
    from okg.substrate.deployment_bootstrap import workflow

    dep = (tmp_path / "dep").resolve()
    other = (tmp_path / "other").resolve()
    (dep / ".okg_bootstrap" / "staged").mkdir(parents=True)
    link = tmp_path / "link"
    link.symlink_to(dep, target_is_directory=True)
    root = recorded_root.format(dep=dep, other=other, link=link)
    calls: dict = {}

    class _Identity:
        deployment_name = "dep"

    monkeypatch.setattr(modules_compose, "compose_catalog", lambda _d: object())
    monkeypatch.setattr(ownership, "catalog_identity_for", lambda **_k: _Identity())
    monkeypatch.setattr(
        connection, "connect",
        lambda *_a, **_k: contextlib.nullcontext(object()),
    )
    monkeypatch.setattr(
        ownership, "diagnose_catalog_ownership",
        lambda _c, identity: {"owner": {
            "deployment_name": "dep",
            "claimed_by_command": "okg deployment bootstrap verify",
            "metadata": {"deployment_root": root},
        }},
    )

    def fake_claim(_conn, *, identity, force=False):
        calls["force"] = force
        return {"owner": {"metadata": {"deployment_root": str(dep)}}}

    monkeypatch.setattr(ownership, "claim_catalog_ownership", fake_claim)
    monkeypatch.setattr(ownership, "ensure_catalog_write_allowed", lambda *_a, **_k: None)
    monkeypatch.setattr(ownership, "owner_is_stale", lambda _o: (False, None))
    monkeypatch.setattr(modules_apply, "apply_composed_catalog", lambda *_a, **_k: 7)

    report = workflow._hand_over_catalog(dep=dep, dsn="postgresql://unused")

    assert calls["force"] is expect_force
    assert report["previous_owner"]["is_this_deployments_staged_copy"] is expect_force
    assert report["owner_root"] == str(dep)
    assert report["catalog_version_id"] == 7


def _verified_with_database(
    tmp_path: Path, capsys, database: dict | None = None,
) -> Path:
    from okg.substrate.deployment_bootstrap.workflow import load_plan, save_plan

    dep, brief = _write_bootstrap_fixture(tmp_path)
    _prepare_staged_deployment(dep, brief, capsys)
    _mark_verified(dep)
    if database is not None:
        plan = load_plan(dep)
        plan["verification"]["database"] = database
        save_plan(dep, plan)
    return dep


def _plan_status(dep: Path) -> str:
    plan = yaml.safe_load((dep / "bootstrap_plan.yaml").read_text())
    return plan["verification"]["status"]


def test_bootstrap_apply_refuses_an_unreachable_database_before_copying(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    """Review item 1: a dead database crashed apply with a raw traceback
    after the staged files were already copied."""
    monkeypatch.delenv("OKG_DSN", raising=False)
    dep = _verified_with_database(tmp_path, capsys)

    rc = main([
        "--json", "deployment", "bootstrap", "apply",
        "--deployment", str(dep),
        "--dsn", "postgresql://nobody:secret@127.0.0.1:1/nothing",
        "--apply",
    ])

    assert rc == 1
    err = _json_out(capsys)["error"]
    assert err["code"] == "bootstrap_publish_blocked"
    assert err["data"]["error"] == "database_unreachable"
    assert err["data"]["database"] == {
        "host": "127.0.0.1", "port": "1", "dbname": "nothing",
    }
    assert "secret" not in json.dumps(err)
    assert not (dep / "deployment.yaml").exists()
    assert _plan_status(dep) == "verified"


def test_bootstrap_apply_refuses_a_database_other_than_the_verified_one(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    """Review item 2: with no --dsn, apply followed the manifest's ${OKG_DSN}
    even when verify had published to another database."""
    from okg.substrate.deployment_bootstrap import workflow

    monkeypatch.setenv("OKG_DSN", "postgresql://u@db-two.example:5432/two")
    dep = _verified_with_database(
        tmp_path, capsys,
        database={"host": "db-one.example", "port": "5432", "dbname": "one"},
    )

    rc = main([
        "--json", "deployment", "bootstrap", "apply",
        "--deployment", str(dep), "--apply",
    ])

    assert rc == 1
    err = _json_out(capsys)["error"]
    assert err["data"]["error"] == "database_mismatch"
    assert err["data"]["verify_database"]["dbname"] == "one"
    assert err["data"]["apply_database"] == {
        "host": "db-two.example", "port": "5432", "dbname": "two",
    }
    assert not (dep / "deployment.yaml").exists()

    # An explicit --dsn is a deliberate choice: the mismatch check steps aside
    # and the target is reported in catalog_handover.
    monkeypatch.setattr(workflow, "_require_reachable_database", lambda *_a, **_k: None)
    monkeypatch.setattr(
        workflow, "_hand_over_catalog",
        lambda *, dep, dsn: {"planned": True, "claimed": True, "deployment_root": str(dep)},
    )
    assert main([
        "--json", "deployment", "bootstrap", "apply",
        "--deployment", str(dep),
        "--dsn", "postgresql://u@db-two.example:5432/two", "--apply",
    ]) == 0
    handover = _json_out(capsys)["catalog_handover"]
    assert handover["database"] == {
        "host": "db-two.example", "port": "5432", "dbname": "two",
    }
    assert _plan_status(dep) == "applied"


def test_bootstrap_apply_leaves_the_plan_verified_when_the_handover_fails(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    """Review item 3: the plan is marked applied only after the handover."""
    import psycopg

    from okg.substrate.deployment_bootstrap import workflow

    monkeypatch.delenv("OKG_DSN", raising=False)
    dep = _verified_with_database(tmp_path, capsys)
    monkeypatch.setattr(workflow, "_require_reachable_database", lambda *_a, **_k: None)

    def failing_handover(*, dep, dsn):
        raise psycopg.OperationalError("server closed the connection")

    monkeypatch.setattr(workflow, "_hand_over_catalog", failing_handover)

    rc = main([
        "--json", "deployment", "bootstrap", "apply",
        "--deployment", str(dep),
        "--dsn", "postgresql://u@127.0.0.1:5432/db", "--apply",
    ])

    assert rc == 1
    err = _json_out(capsys)["error"]
    assert err["code"] == "bootstrap_catalog_rejected"
    assert err["data"]["files_copied"] is True
    assert _plan_status(dep) == "verified"


def test_bootstrap_apply_twice_says_already_applied(
    tmp_path: Path, monkeypatch, capsys,
) -> None:
    """Review item 5: the second apply named an unverified plan."""
    from okg.substrate.deployment_bootstrap import workflow

    monkeypatch.delenv("OKG_DSN", raising=False)
    dep = _verified_with_database(tmp_path, capsys)
    monkeypatch.setattr(workflow, "_require_reachable_database", lambda *_a, **_k: None)
    monkeypatch.setattr(
        workflow, "_hand_over_catalog",
        lambda *, dep, dsn: {"planned": True, "claimed": True, "deployment_root": str(dep)},
    )
    args = [
        "--json", "deployment", "bootstrap", "apply",
        "--deployment", str(dep),
        "--dsn", "postgresql://u@127.0.0.1:5432/db", "--apply",
    ]
    assert main(args) == 0
    capsys.readouterr()

    assert main(args) == 1
    err = _json_out(capsys)["error"]
    assert err["code"] == "bootstrap_unverified_apply"
    assert "already applied" in err["message"]
    assert err["data"]["status"] == "applied"
