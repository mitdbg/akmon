"""No-database checks for the guided add-source flow (issue #1802).

The read-only planning half is exercised against a real scaffold deployment
directory; the write half is exercised with a fake `okg` runner so every
step's refusal, ledger entry and retry path is observed without a database.
"""
from __future__ import annotations

import json
import os
import stat
import subprocess
from typing import Any
from pathlib import Path

import pytest
import yaml

from okg.deployment import ConnectorRegistration, SdkVersionRange
from okg.substrate.cli import main as cli
from okg.substrate.cli.source_add import parse_settings
from okg.substrate.deployment_scaffold import create_deployment_scaffold
from okg.substrate.errors import SubstrateError
from okg.substrate.sources import guided_add
from okg.substrate.sources.guided_add import (
    AddSourceError,
    apply_add_source,
    build_registry_entry,
    complete_scan_mode,
    plan_add_source,
    read_ledger,
)
from okg.substrate.sources.guided_catalog import (
    GITHUB,
    SUPPORTED_SOURCE_TYPES,
    packaged_type,
)
from okg.substrate.sources.registry import admit_source_registry_document


@pytest.fixture
def deployment(tmp_path: Path) -> Path:
    return create_deployment_scaffold(
        "unit", deployments_root=tmp_path / "deployments",
    ).deployment_dir


@pytest.fixture
def folder(tmp_path: Path) -> Path:
    root = tmp_path / "notes"
    root.mkdir()
    (root / "plan.md").write_text("# Weekly plan\n\nShip the guided flow.\n")
    (root / "skip.bin").write_bytes(b"\x00\x01")
    return root


def _plan(deployment: Path, **overrides):
    kwargs = dict(
        deployment=str(deployment), name="notes", type_id="local-folder",
        settings={}, environ={"PATH": os.environ.get("PATH", "")},
    )
    kwargs.update(overrides)
    return plan_add_source(**kwargs)


# ---- catalog contract -------------------------------------------------------


def test_every_supported_type_renders_a_strict_registry_entry(deployment: Path) -> None:
    for source_type in SUPPORTED_SOURCE_TYPES.values():
        entry = build_registry_entry(
            source_type, deployment_name="unit", name="one",
            params={"repo": "one"} if source_type is not GITHUB else {"repo": "a/b"},
        )
        assert entry["ownership_id"] == "unit.one"
        assert entry["admission_policy"]["producer_id"] == "unit.one"
        assert entry["source_policy"]["sensitivity"] in (
            "local_private", "credentialed_external",
        )
        summary = entry["admission_policy"]["output_scope_summary"]
        assert set(summary["nodes"]) == {
            n["subtype"] for n in entry["admission_policy"]["output_signature"]["nodes"]
        }
        document = {"admission_contract": "strict_v1", "sources": {"one": entry}}
        admission = admit_source_registry_document(
            document, registry_path=deployment / "source_registry.yaml",
        )
        assert admission.strict and admission.entries["one"].legacy is False
        described = source_type.describe()
        assert described["settings"] and described["update_behaviour"]
        assert "removed" in described["removal_behaviour"]


def test_local_folder_signature_declares_every_edge_its_reader_emits(
    deployment: Path, tmp_path: Path,
) -> None:
    # A Markdown file that links to another file in the folder makes the
    # reader emit `document_chunk references document`. When the signature
    # left that triple out, admission refused the whole import with
    # `producer_edge_triple_not_allowed`.
    from okg.substrate.sources.guided_defaults import EXTRACTORS_YAML

    root = tmp_path / "linked"
    root.mkdir()
    (root / "a.md").write_text("# Alpha\n\nSee [the beta page](b.md).\n")
    (root / "b.md").write_text("# Beta\n\nBeta covers turbidity.\n")
    plan, nodes, edges = _read_folder(deployment, tmp_path, root, EXTRACTORS_YAML)
    assert ("document_chunk", "references", "document") in edges
    _assert_declared(plan, nodes, edges)
    summary = plan.entry["admission_policy"]["output_scope_summary"]
    assert "document_chunk references document" in summary["edges"]


def test_local_folder_signature_declares_entity_mentions(
    deployment: Path, tmp_path: Path,
) -> None:
    # An extractors.yaml that enables entity extraction (the codebase-index
    # template, okg-workspace, or an operator edit) makes the reader emit an
    # `entity_mention` node and `entity_mention member_of document_chunk`
    # for a TODO, a DOI, an arXiv id or a change name.
    template = (
        Path(guided_add.__file__).resolve().parents[1]
        / "library" / "templates" / "codebase-index" / "extractors.yaml"
    )
    root = tmp_path / "todo"
    root.mkdir()
    (root / "note.md").write_text(
        "# Follow-ups\n\nTODO: check the fix-foo-bar change.\n",
    )
    plan, nodes, edges = _read_folder(deployment, tmp_path, root, template)
    assert "entity_mention" in nodes
    assert ("entity_mention", "member_of", "document_chunk") in edges
    _assert_declared(plan, nodes, edges)


def _read_folder(
    deployment: Path, tmp_path: Path, root: Path, extractors: Path,
) -> tuple[Any, set[str], set[tuple[Any, str, Any]]]:
    """Run the local-folder reader over `root`; return its emitted shapes."""
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    plan = _plan(deployment, settings={"root": str(root)})
    assert plan.ok, [s.as_dict() for s in plan.steps]
    readers = tmp_path / "readers"
    readers.mkdir()
    (readers / "extractors.yaml").write_bytes(extractors.read_bytes())
    params = {**plan.params, "extractors_dir": str(readers)}
    adapter = guided_add._construct_adapter(plan.entry, params)
    facts = list(adapter.run("unit", mode="scope_complete").facts)
    subtype = {f.node_id: f.subtype for f in facts if isinstance(f, NodeFact)}
    edges = {
        (subtype.get(f.src), f.edge_type, subtype.get(f.dst))
        for f in facts if isinstance(f, EdgeFact)
    }
    return plan, set(subtype.values()), edges


def _assert_declared(
    plan: Any, nodes: set[str], edges: set[tuple[Any, str, Any]],
) -> None:
    signature = plan.entry["admission_policy"]["output_signature"]
    declared = {
        (e["src_subtype"], e["edge_type"], e["dst_subtype"])
        for e in signature["edges"]
    }
    assert edges <= declared, sorted(edges - declared)
    assert {n["subtype"] for n in signature["nodes"]} >= nodes


def test_admission_cause_shows_a_bare_reject_code(tmp_path: Path) -> None:
    registry = tmp_path / "source_registry.yaml"
    cause = "source admission rejected: producer_edge_triple_not_allowed"
    assert guided_add._admission_cause(cause, registry) == cause


def test_admission_cause_keeps_the_code_and_the_connector_reason(
    tmp_path: Path,
) -> None:
    # #3927: a health rejection appends the connector's reason in brackets.
    registry = tmp_path / "source_registry.yaml"
    cause = (
        "source admission rejected: source_health_not_healthy (the connector "
        "reported endpoint_failed: harvest incomplete; no complete scope "
        "claimed)"
    )
    assert guided_add._admission_cause(cause, registry) == cause


def test_admission_cause_masks_a_secret_in_the_connector_reason(
    tmp_path: Path,
) -> None:
    registry = tmp_path / "source_registry.yaml"
    secret = "ghp_" + "A1b2C3d4" * 5
    shown = guided_add._admission_cause(
        "source admission rejected: source_health_not_healthy (the connector "
        f"reported auth_failed: bad token {secret})",
        registry,
    )
    assert secret not in shown
    assert shown.startswith(
        "source admission rejected: source_health_not_healthy ("
    )


# Fake values built at run time; the ids keep them out of test reports.
@pytest.mark.parametrize("secret", [
    pytest.param("ghp_" + "A1b2C3d4" * 5, id="github-token-shape"),
    pytest.param("sk_live_" + "Z9y8X7w6" * 3, id="stripe-key-shape"),
])
def test_admission_cause_still_masks_a_secret_after_the_prefix(
    tmp_path: Path, secret: str,
) -> None:
    registry = tmp_path / "source_registry.yaml"
    shown = guided_add._admission_cause(
        f"source admission rejected: {secret}", registry,
    )
    assert secret not in shown
    assert shown == "source admission rejected: <redacted>"


def test_admission_cause_masks_a_code_shaped_secret_outside_the_prefix(
    tmp_path: Path,
) -> None:
    registry = tmp_path / "source_registry.yaml"
    secret = "sk_" + "a" * 32  # lower-case key shape (fake)
    shown = guided_add._admission_cause(f"import failed with {secret}", registry)
    assert secret not in shown and "<redacted>" in shown


def test_complete_scan_mode_follows_the_profile() -> None:
    assert complete_scan_mode("discovery_crawl") == "scope_complete"
    assert complete_scan_mode("mutable_api") == "reconcile"
    assert complete_scan_mode("unknown") == "scope_complete"


def test_packaged_registration_goes_through_the_same_flow(
    deployment: Path, tmp_path: Path,
) -> None:
    registration = ConnectorRegistration(
        connector_type="acme-tickets",
        version="0.1.0",
        module="acme.connector",
        class_name="TicketConnector",
        ownership_id="acme-tickets",
        source_class="mutable_api",
        trust_label="untrusted",
        sdk_range=SdkVersionRange(min_version="1.0", max_version_exclusive="2.0"),
        config_schema={"type": "object", "properties": {"workspace": {"type": "string"}}},
        auth_modes=("env_credential",),
        credential_refs=("ACME_TOKEN",),
        record_identity_kind="remote_id",
        record_identity_fields=("ticket_id",),
        source_revision_kind="updated_at",
        deletion_semantics="missing_from_completed_scope",
        publication_mode="published_generation",
        description="Acme tickets",
    )
    asset = tmp_path / "acme.json"
    asset.write_text(json.dumps(registration.as_asset_payload()))
    packaged = packaged_type(registration)
    assert packaged.access_mode == "env"
    assert packaged.source_policy["credential_ref_policy"] == "required"

    plan = _plan(
        deployment, name="tickets", type_id=None, registration_path=str(asset),
        settings={"workspace": "acme"}, credential_refs=("ACME_TOKEN",),
        environ={"PATH": os.environ.get("PATH", "")},
    )
    steps = {s.name: s for s in plan.steps}
    assert steps["type"].detail["origin"] == "packaged_connector"
    # The packaged module is not installed here: the reader step names it.
    assert plan.failed_step is not None and plan.failed_step.name == "reader"


# ---- planning refusals ------------------------------------------------------


def test_unsupported_type_says_a_new_reader_is_needed(deployment: Path) -> None:
    plan = _plan(deployment, type_id="sharepoint")
    failed = plan.failed_step
    assert failed is not None and failed.name == "type"
    assert "new reader is needed" in failed.cause
    assert failed.detail["supported_types"] == sorted(SUPPORTED_SOURCE_TYPES)
    assert "not translated automatically" in failed.fix_hint


def test_secret_like_settings_are_refused_before_anything_is_saved(
    deployment: Path, folder: Path,
) -> None:
    before = (deployment / "source_registry.yaml").read_bytes()
    plan = _plan(deployment, settings={"root": str(folder), "api_token": "sk-live-1"})
    failed = plan.failed_step
    assert failed is not None and failed.name == "settings"
    assert "api_token" in failed.cause and "sk-live-1" not in json.dumps(plan.as_dict())
    assert (deployment / "source_registry.yaml").read_bytes() == before


def test_settings_are_validated_against_the_reader_schema(deployment: Path) -> None:
    plan = _plan(deployment, settings={"root": 42})
    assert plan.failed_step is not None and plan.failed_step.name == "settings"
    plan = _plan(deployment, settings={})
    assert plan.failed_step is not None and "root" in json.dumps(plan.failed_step.detail)
    plan = _plan(deployment, type_id="github", settings={"repository": "not-a-slug"})
    assert plan.failed_step is not None and plan.failed_step.name == "settings"


def test_missing_folder_is_named(deployment: Path, tmp_path: Path) -> None:
    plan = _plan(deployment, settings={"root": str(tmp_path / "absent")})
    assert plan.failed_step is not None and plan.failed_step.name == "reader"
    assert "does not exist" in plan.failed_step.cause


def test_invalid_name_and_reader_clash_are_refused(deployment: Path, folder: Path) -> None:
    assert _plan(deployment, name="9bad").failed_step.name == "name"
    plan = _plan(deployment, name="bootstrap-fixture", settings={"root": str(folder)})
    assert plan.failed_step is not None and plan.failed_step.name == "name"
    assert "different reader" in plan.failed_step.cause


def test_github_access_reuses_gh_login_and_names_missing_login(
    deployment: Path, tmp_path: Path,
) -> None:
    def runner(argv, **kwargs):
        rc = 0 if os.environ.get("GH_OK") else 1
        return subprocess.CompletedProcess(argv, rc, stdout="", stderr="not logged in")

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    gh = bin_dir / "gh"
    gh.write_text("#!/bin/sh\nexit 0\n")
    gh.chmod(gh.stat().st_mode | stat.S_IXUSR)
    env = {"PATH": str(bin_dir)}
    plan = plan_add_source(
        deployment=str(deployment), name="w", type_id="github",
        settings={"repository": "acme/widgets"}, environ=env, runner=runner,
    )
    assert plan.failed_step is not None and plan.failed_step.name == "access"
    assert "gh auth login" in plan.failed_step.fix_hint
    plan = plan_add_source(
        deployment=str(deployment), name="w", type_id="github",
        settings={"repository": "acme/widgets"}, environ={}, runner=runner,
    )
    assert plan.failed_step.name == "access" and "not installed" in plan.failed_step.cause


# ---- planning success: preview, decisions, no writes ------------------------


def test_local_folder_plan_previews_without_writing(deployment: Path, folder: Path) -> None:
    snapshot = {p: p.read_bytes() for p in deployment.rglob("*") if p.is_file()}
    plan = _plan(deployment, settings={"root": str(folder)})
    assert plan.ok, [s.as_dict() for s in plan.steps]
    assert plan.preview["scope"]["files_matched"] == 1  # skip.bin is not included
    assert plan.preview["records"][0]["subtype"] == "document"
    assert plan.preview["records"][0]["search_text"]
    assert plan.preview["relationships"]
    assert [d["kind"] for d in plan.decisions] == ["ontology_modules", "file_readers"]
    assert plan.decisions[0]["add"] == ["extraction"]
    assert plan.decisions[1]["why"].startswith("writes the packaged file readers")
    payload = plan.as_dict()
    assert payload["behaviour"]["refresh"].startswith("Refreshes when you run")
    assert "okg ingest --deployment unit" in payload["behaviour"]["refresh"]
    assert {p: p.read_bytes() for p in deployment.rglob("*") if p.is_file()} == snapshot
    assert plan.entry["params"]["extractors_dir"] == str(deployment)
    assert plan.entry["params"]["repo_root"] == str(folder)


def _write_empty_extractors(deployment: Path) -> None:
    # `okg deployment bootstrap draft` writes exactly this placeholder.
    (deployment / "extractors.yaml").write_text(
        yaml.safe_dump({"parser": [], "chunker": [], "ner": []}), encoding="utf-8",
    )


def test_local_folder_preview_reads_file_text_under_an_empty_extractors_file(
    deployment: Path, folder: Path,
) -> None:
    # A bootstrapped deployment already has an extractors.yaml with no
    # parsers; the preview must still read the note's text into a passage
    # instead of a document that carries only its file name.
    _write_empty_extractors(deployment)
    plan = _plan(deployment, settings={"root": str(folder)})
    assert plan.ok, [s.as_dict() for s in plan.steps]
    assert any(r["type"] == "contains" for r in plan.preview["relationships"])
    assert yaml.safe_load((deployment / "extractors.yaml").read_text())["parser"] == []



def _git_clean(dep: Path) -> bool:
    status = subprocess.run(
        ["git", "-C", str(dep), "status", "--porcelain", "--untracked-files=all"],
        capture_output=True, text=True, check=True,
    ).stdout
    return status == ""


def test_service_source_lists_posture_decisions(
    deployment: Path, tmp_path: Path, monkeypatch,
) -> None:
    def fake_preview(source_type, plan, limit, steps):
        plan.preview = {"records": [], "relationships": [], "scope": {}}
        steps.append(guided_add.Step("preview", "ok"))

    monkeypatch.setattr(guided_add, "_preview", fake_preview)
    runner = lambda argv, **kw: subprocess.CompletedProcess(argv, 0, "", "")  # noqa: E731
    # The access step looks `gh` up on the PATH it is given; a stub keeps the
    # test independent of whether the machine has the GitHub CLI installed.
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    gh = bin_dir / "gh"
    gh.write_text("#!/bin/sh\nexit 0\n")
    gh.chmod(gh.stat().st_mode | stat.S_IXUSR)
    plan = plan_add_source(
        deployment=str(deployment), name="w", type_id="github",
        settings={"repository": "acme/widgets"},
        environ={"PATH": str(bin_dir)}, runner=runner,
    )
    assert plan.ok, [s.as_dict() for s in plan.steps]
    posture = next(d for d in plan.decisions if d["kind"] == "privacy_posture")
    paths = {c["path"]: c for c in posture["changes"]}
    assert paths["nomos.deployment_posture.live_call_default"]["to"] == "review"
    assert paths["nomos.deployment_posture.allowed_source_sensitivities"]["add"] == (
        "credentialed_external"
    )
    assert "credential_refs" not in plan.entry
    assert plan.entry["source_policy"]["credential_ref_policy"] == "optional"


# ---- apply: ledger, failure handling, retry ---------------------------------


class _FakeOkg:
    """Stand-in for the `okg` subprocesses the apply half runs."""

    def __init__(
        self, *, fail_at: str | None = None, publish: dict | None = None,
        appended: int = 3, sample_source: str = "unit.notes",
        earlier_publishes: tuple[dict, ...] = (),
        admission_id: str = "00000000-0000-4000-8000-0000000000aa",
    ) -> None:
        self.admission_id = admission_id
        self.sample_source = sample_source
        self.earlier_publishes = list(earlier_publishes)
        self.fail_at = fail_at
        self.publish = publish or {
            "status": "published", "generation_id": "okg:unit:g1",
        }
        self.appended = appended
        self.calls: list[list[str]] = []

    def __call__(self, argv, **kwargs):
        argv = list(argv)
        self.calls.append(argv)
        verb = argv[argv.index("okg.substrate.cli.main") + 1]
        payload: dict
        rc = 0
        if verb == "deployment":
            payload = {"ok": True}
        elif verb == "catalog":
            payload = {"applied": True}
        elif verb == "ingest":
            if self.fail_at == "import":
                payload = {"results": [{
                    "name": argv[argv.index("--include") + 1], "status": "FAIL",
                    "error": "gh api failed: HTTP 502",
                }], "publishes": []}
                rc = 1
            else:
                payload = {
                    "results": [{
                        "name": argv[argv.index("--include") + 1], "status": "OK",
                        "nodes_appended": self.appended,
                        "edges_appended": self.appended and 2,
                        "admission_id": self.admission_id,
                    }],
                    "publishes": [*self.earlier_publishes, self.publish],
                }
        elif verb == "search":
            payload = {"results": [{
                "node_id": "doc:notes:plan.md", "subtype": "document",
                "attrs": {"title": "Weekly plan", "path": "plan.md"},
                "source": self.sample_source,
            }]}
        else:
            payload = {}
        if self.fail_at == verb:
            rc = 1
            payload = {"error": {"message": f"{verb} refused", "fix_hint": "fix it"}}
        return subprocess.CompletedProcess(argv, rc, stdout=json.dumps(payload), stderr="")


def test_apply_saves_commits_and_reports_a_sample(deployment: Path, folder: Path) -> None:
    _git_init(deployment)
    plan = _plan(deployment, settings={"root": str(folder)})
    fake = _FakeOkg()
    outcome = apply_add_source(plan, runner=fake, environ={"PATH": os.environ["PATH"]})
    assert outcome.status == "published"
    assert [s.name for s in outcome.steps] == [
        "save", "validate", "catalog", "claim", "policy", "import", "publish",
        "sample",
    ]
    assert outcome.sample["source"] == "unit.notes"
    assert outcome.counts["records_added"] == 3
    ingest = next(c for c in fake.calls if "ingest" in c)
    assert ingest[ingest.index("--mode") + 1] == "scope_complete"
    registry = yaml.safe_load((deployment / "source_registry.yaml").read_text())
    assert registry["admission_contract"] == "strict_v1"
    assert registry["sources"]["notes"]["enabled_by_default"] is True
    assert (deployment / "extractors.yaml").is_file()
    assert (deployment / ".okg" / "source-add" / ".gitignore").read_text() == "*\n"
    assert read_ledger(deployment, "notes")["status"] == "published"
    status = subprocess.run(
        ["git", "-C", str(deployment), "status", "--porcelain", "--untracked-files=all"],
        capture_output=True, text=True, check=True,
    ).stdout
    assert status == "", status  # configuration committed, ledger ignored


def test_failed_import_keeps_definition_disabled_and_retry_reuses_it(
    deployment: Path, folder: Path,
) -> None:
    _git_init(deployment)
    plan = _plan(deployment, settings={"root": str(folder)})
    failing = _FakeOkg(fail_at="import")
    outcome = apply_add_source(plan, runner=failing, environ={"PATH": os.environ["PATH"]})
    assert outcome.status == "failed"
    failed = outcome.as_dict()["failed_step"]
    assert failed["step"] == "import" and "502" in failed["cause"]
    registry = yaml.safe_load((deployment / "source_registry.yaml").read_text())
    assert registry["sources"]["notes"]["enabled_by_default"] is False
    assert read_ledger(deployment, "notes")["status"] == "failed"

    retry_plan = _plan(deployment, settings={"root": str(folder)})
    assert retry_plan.ok and retry_plan.existing_entry is not None
    assert retry_plan.as_dict()["retry_of_saved_definition"] is True
    ok = _FakeOkg()
    outcome = apply_add_source(retry_plan, runner=ok, environ={"PATH": os.environ["PATH"]})
    assert outcome.status == "published"
    # The catalog is applied again on retry even though deployment.yaml
    # already listed the module the failed attempt added.
    assert any("catalog" in c and "load" in c for c in ok.calls)
    registry = yaml.safe_load((deployment / "source_registry.yaml").read_text())
    assert list(registry["sources"]) == ["bootstrap-fixture", "notes"]
    assert registry["sources"]["notes"]["enabled_by_default"] is True


def _bootstrap_drafted_entry(deployment: Path) -> dict:
    """A registry entry as `okg deployment bootstrap draft` wrote it before #2952."""
    fixture = deployment / "fixtures" / "records.json"
    fixture.parent.mkdir(parents=True, exist_ok=True)
    fixture.write_text("[]\n", encoding="utf-8")
    return {
        "module": "okg.substrate.library.sources.bootstrap_fixture",
        "class": "BootstrapFixtureSource",
        "ownership_id": "unit.records",
        "source_class": "discovery_crawl",
        "record_identity_kind": "scoped_locator",
        "record_identity_fields": ["source_id", "record_id"],
        "source_revision_kind": "content_hash",
        "deletion_semantics": "missing_from_completed_scope",
        "publication_mode": "published_generation",
        "params": {"fixture_path": str(fixture)},
        "schedule": {"reconcile_interval": "24h"},
    }


def test_add_source_removes_a_bootstrap_schedule_and_says_so(
    deployment: Path, folder: Path,
) -> None:
    """Issue #2952: add-source failed on a registry bootstrap had drafted.

    Bootstrap wrote a per-source `schedule` block into a legacy registry.
    Adding a source promotes the registry to strict_v1, whose schema refuses
    that key, so the import step failed on a source the operator never
    touched. Nothing ever read the key; the promotion now removes it and the
    save step reports which sources lost it.
    """
    registry_path = deployment / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    assert "admission_contract" not in registry
    registry["sources"]["records"] = _bootstrap_drafted_entry(deployment)
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False))
    # The failure the operator saw: the drafted entry under strict_v1.
    with pytest.raises(ValueError, match="'schedule' was unexpected"):
        admit_source_registry_document(
            {"admission_contract": "strict_v1", **registry},
            registry_path=registry_path,
        )
    _git_init(deployment)

    outcome = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}),
        runner=_FakeOkg(), environ={"PATH": os.environ["PATH"]},
    )

    assert outcome.status == "published"
    save = next(step for step in outcome.steps if step.name == "save")
    assert save.detail["admission_contract_added"] is True
    assert save.detail["schedule_removed"] == ["records"]
    saved = yaml.safe_load(registry_path.read_text())
    assert saved["admission_contract"] == "strict_v1"
    assert "schedule" not in saved["sources"]["records"]
    admission = admit_source_registry_document(saved, registry_path=registry_path)
    assert admission.strict
    assert set(admission.entries) >= {"records", "notes"}


def test_a_strict_registry_keeps_an_authored_schedule_for_the_schema_to_name(
    deployment: Path,
) -> None:
    """Only the legacy-to-strict promotion removes the inert key.

    In a document that is already strict_v1 a `schedule` is the author's
    error; promotion leaves it, and strict admission refuses it by name.
    """
    document = {
        "admission_contract": "strict_v1",
        "sources": {"records": _bootstrap_drafted_entry(deployment)},
    }
    promoted, added = guided_add.promote_registry_to_strict(document)
    assert added is False
    assert promoted["sources"]["records"]["schedule"] == {"reconcile_interval": "24h"}
    registry_path = deployment / "source_registry.yaml"
    with pytest.raises(ValueError) as refused:
        admit_source_registry_document(promoted, registry_path=registry_path)
    # The operator is told exactly what to delete, and where.
    assert str(refused.value).endswith(
        "path=sources.records: Additional properties are not allowed "
        "('schedule' was unexpected); delete the `schedule` block of source "
        f"'records' (key sources.records.schedule) from {registry_path}. "
        "Nothing reads it; a scheduled refresh is the source's `sync.poll` "
        "block"
    ), str(refused.value)


def test_failed_import_names_a_failed_reclaim_instead_of_hiding_it(
    deployment: Path, folder: Path,
) -> None:
    """A failed re-claim leaves the #2213 stuck state; it must be said.

    The failure path disables the definition, which rewrites
    `source_registry` -- part of the manifest identity a publish is bound
    to. When the re-claim that follows fails, the database stays claimed on
    the PRE-disable content and the next publish refuses on drift. The run
    recorded only `identity_reclaimed: False` in a detail dict and reported
    the original import error, so the operator saw nothing about it.
    """
    _git_init(deployment)
    plan = _plan(deployment, settings={"root": str(folder)})
    failing = _FakeOkg(fail_at="import")
    claims: list[list[str]] = []

    def runner(argv, **kwargs):
        argv = list(argv)
        if "ownership" in argv and "claim" in argv:
            claims.append(argv)
            if len(claims) > 1:
                # The post-catalog claim succeeds; only the failure-path
                # re-claim is neutered, so the guard pins that branch alone.
                return subprocess.CompletedProcess(
                    argv, 1,
                    stdout=json.dumps({"error": {
                        "message": "catalog ownership is held by a different writer",
                        "fix_hint": "run ownership diagnose",
                    }}),
                    stderr="",
                )
        return failing(argv, **kwargs)

    outcome = apply_add_source(
        plan, runner=runner, environ={"PATH": os.environ["PATH"]},
    )

    assert outcome.status == "failed"
    assert len(claims) == 2, claims
    failed = outcome.as_dict()["failed_step"]
    assert failed["step"] == "import"
    # The original cause survives, and the stuck state is named beside it.
    assert "502" in failed["cause"]
    assert "could NOT be re-claimed" in failed["cause"], failed["cause"]
    assert "held by a different writer" in failed["cause"], failed["cause"]
    step = next(s for s in outcome.steps if s.name == "import")
    assert step.detail["identity_reclaimed"] is False


def test_catalog_is_skipped_once_the_source_has_published(
    deployment: Path, folder: Path,
) -> None:
    _git_init(deployment)
    first = _FakeOkg()
    outcome = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}), runner=first,
        environ={"PATH": os.environ["PATH"]},
    )
    assert outcome.status == "published"
    assert any("catalog" in c and "load" in c for c in first.calls)
    second = _FakeOkg()
    outcome = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}), runner=second,
        environ={"PATH": os.environ["PATH"]},
    )
    assert outcome.status == "published"
    assert not any("catalog" in c and "load" in c for c in second.calls)
    assert [s.status for s in outcome.steps if s.name == "catalog"] == ["skipped"]


_NO_SOURCE_WORK = {
    "label": "commit", "status": "skipped", "publish_skipped": True,
    "reason": "no_source_work",
}


def test_unchanged_rerun_of_a_published_source_reports_no_change(
    deployment: Path, folder: Path,
) -> None:
    """#2491: a second identical add-source on a published, unchanged source
    reports no change, keeps the source enabled and exits 0."""
    from okg.substrate.cli.source_add import _render_text

    _git_init(deployment)
    first = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}), runner=_FakeOkg(),
        environ={"PATH": os.environ["PATH"]},
    )
    assert first.status == "published"
    second_run = _FakeOkg(publish=dict(_NO_SOURCE_WORK), appended=0)
    outcome = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}), runner=second_run,
        environ={"PATH": os.environ["PATH"]},
    )
    assert outcome.status == "no_change"
    assert outcome.generation_id == "okg:unit:g1"
    assert outcome.as_dict()["failed_step"] is None
    publish = next(s for s in outcome.steps if s.name == "publish")
    assert publish.status == "skipped" and publish.detail["no_change"] is True
    # The sample is read from the version that holds the source's records.
    search = next(c for c in second_run.calls if "search" in c)
    assert search[search.index("--generation-id") + 1] == "okg:unit:g1"
    registry = yaml.safe_load((deployment / "source_registry.yaml").read_text())
    assert registry["sources"]["notes"]["enabled_by_default"] is True
    ledger = read_ledger(deployment, "notes")
    assert ledger["status"] == "published"
    assert ledger["generation_id"] == "okg:unit:g1"
    text = _render_text(outcome.as_dict())
    assert "No change: this source's records are already in graph" in text
    assert "okg:unit:g1" in text and "stays enabled" in text


def test_publisher_reusing_the_current_version_is_no_change(
    deployment: Path, folder: Path,
) -> None:
    """The publisher's reuse path reports `published` with `noop` and
    `reused_generation`: the import matched the current version. That is
    success with no change, not a failed publish."""
    _git_init(deployment)
    reused = {
        "label": "commit", "status": "published",
        "generation_id": "okg:unit:current", "noop": True,
        "reused_generation": True, "blocked_reason": None,
    }
    fake = _FakeOkg(publish=reused)
    outcome = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}), runner=fake,
        environ={"PATH": os.environ["PATH"]},
    )
    assert outcome.status == "no_change"
    assert outcome.generation_id == "okg:unit:current"
    assert outcome.as_dict()["failed_step"] is None
    search = next(c for c in fake.calls if "search" in c)
    assert search[search.index("--generation-id") + 1] == "okg:unit:current"
    registry = yaml.safe_load((deployment / "source_registry.yaml").read_text())
    assert registry["sources"]["notes"]["enabled_by_default"] is True


def test_no_source_work_before_any_publish_still_fails(
    deployment: Path, folder: Path,
) -> None:
    """A source that has never published and imports nothing is not a
    no-change: it has no records in any version, so the attempt fails."""
    _git_init(deployment)
    outcome = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}),
        runner=_FakeOkg(publish=dict(_NO_SOURCE_WORK), appended=0),
        environ={"PATH": os.environ["PATH"]},
    )
    assert outcome.status == "failed"
    failed = outcome.as_dict()["failed_step"]
    assert failed["step"] == "publish" and "no_source_work" in failed["cause"]
    registry = yaml.safe_load((deployment / "source_registry.yaml").read_text())
    assert registry["sources"]["notes"]["enabled_by_default"] is False


#: Admission ids the fake `okg ingest` reports, one per attempt.
_ADM_1 = "00000000-0000-4000-8000-000000000001"
_ADM_2 = "00000000-0000-4000-8000-000000000002"
_REFUSED = {
    "label": "commit", "status": "fail",
    "error": "deployment release authority is unavailable",
}


def _redrive(admission_ids, *, branch="default", generation="okg:unit:redrive"):
    return {
        "label": "staged-source-redrive", "status": "published",
        "generation_id": generation,
        "predecessor_generation_id": "okg:unit:install",
        "admission_feedback": {
            "status": "published", "branch_name": branch,
            "admission_ids": list(admission_ids),
        },
    }


def _fail_publish(deployment: Path, folder: Path, admission_id: str = _ADM_1):
    outcome = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}),
        runner=_FakeOkg(publish=_REFUSED, admission_id=admission_id),
        environ={"PATH": os.environ["PATH"]},
    )
    assert outcome.status == "failed"
    return outcome


def test_rerun_after_a_failed_publish_reports_the_redrive_that_published_it(
    deployment: Path, folder: Path,
) -> None:
    """#2769: the retry after a failed publish disabled a published source.

    Measured on lab-host: the first attempt imported, then its publish was
    refused (`deployment release authority is unavailable`). The re-run's
    `okg ingest` published that staged import first, as its own
    `staged-source-redrive` publish, then imported nothing new and reported
    a `no_source_work` skip LAST. add-source read only the last publish,
    failed, and disabled a source whose records were in the published graph.
    The re-drive now counts only when it published the failed attempt's own
    admission.
    """
    _git_init(deployment)
    _fail_publish(deployment, folder)
    ledger = read_ledger(deployment, "notes")
    assert ledger["status"] == "failed"
    assert ledger["unpublished_admission_ids"] == [_ADM_1]

    rerun = _FakeOkg(
        publish=dict(_NO_SOURCE_WORK), appended=0,
        earlier_publishes=(_redrive([_ADM_1, "other-source-admission"]),),
    )
    outcome = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}), runner=rerun,
        environ={"PATH": os.environ["PATH"]},
    )
    assert outcome.status == "published", outcome.as_dict()["failed_step"]
    assert outcome.generation_id == "okg:unit:redrive"
    publish = next(s for s in outcome.steps if s.name == "publish")
    assert publish.status == "ok"
    assert publish.detail["redrive"]["label"] == "staged-source-redrive"
    assert publish.detail["redrive"]["published_admission_ids"] == [_ADM_1]
    search = next(c for c in rerun.calls if "search" in c)
    assert search[search.index("--generation-id") + 1] == "okg:unit:redrive"
    registry = yaml.safe_load((deployment / "source_registry.yaml").read_text())
    assert registry["sources"]["notes"]["enabled_by_default"] is True
    ledger = read_ledger(deployment, "notes")
    assert ledger["status"] == "published"
    assert ledger["generation_id"] == "okg:unit:redrive"
    assert ledger["unpublished_admission_ids"] == []


@pytest.mark.parametrize("redrive", [
    pytest.param(_redrive(["other-source-admission"]), id="another-sources-redrive"),
    pytest.param(_redrive([_ADM_1], branch="feature"), id="another-branch"),
    pytest.param(_redrive([_ADM_1], branch=None), id="branch-not-named"),
    pytest.param(
        {**_redrive([_ADM_1]), "status": "fail", "error": "refused"},
        id="refused-redrive",
    ),
])
def test_a_redrive_that_did_not_publish_this_sources_admission_is_no_proof(
    deployment: Path, folder: Path, redrive: dict,
) -> None:
    _git_init(deployment)
    _fail_publish(deployment, folder)
    outcome = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}),
        runner=_FakeOkg(
            publish=dict(_NO_SOURCE_WORK), appended=0, earlier_publishes=(redrive,),
        ),
        environ={"PATH": os.environ["PATH"]},
        admission_lookup=lambda **kw: guided_add.publication_verdict(
            kw["admission_ids"], [],
        ),
    )
    assert outcome.status == "failed"
    failed = outcome.as_dict()["failed_step"]
    assert failed["step"] == "publish" and "no_source_work" in failed["cause"]
    assert read_ledger(deployment, "notes")["unpublished_admission_ids"] == [_ADM_1]


def _lookup(rows):
    calls: list[dict] = []

    def lookup(**kwargs):
        calls.append(kwargs)
        return guided_add.publication_verdict(kwargs["admission_ids"], rows)

    lookup.calls = calls
    return lookup


def test_rerun_accepts_an_import_another_run_published_only_when_bound(
    deployment: Path, folder: Path,
) -> None:
    """#2769: another `okg ingest` may publish the staged import before the
    re-run, which then has no re-drive of its own. It is accepted only when
    the database says the failed attempt's own admission was published."""
    _git_init(deployment)
    _fail_publish(deployment, folder)
    lookup = _lookup([(_ADM_1, "published", 7, "okg:unit:later", "default")])
    rerun = _FakeOkg(publish=dict(_NO_SOURCE_WORK), appended=0)
    outcome = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}), runner=rerun,
        environ={"PATH": os.environ["PATH"], "OKG_DSN": "postgresql:///fake"},
        admission_lookup=lookup,
    )
    assert outcome.status == "no_change", outcome.as_dict()["failed_step"]
    assert outcome.generation_id == "okg:unit:later"
    assert lookup.calls[0]["admission_ids"] == [_ADM_1]
    assert lookup.calls[0]["deployment"] == "unit"
    publish = next(s for s in outcome.steps if s.name == "publish")
    assert publish.detail["published_admission_ids"] == [_ADM_1]
    registry = yaml.safe_load((deployment / "source_registry.yaml").read_text())
    assert registry["sources"]["notes"]["enabled_by_default"] is True


def test_stale_records_of_an_older_attempt_do_not_satisfy_the_retry(
    deployment: Path, folder: Path,
) -> None:
    """Review finding: a re-add with new settings fails at publish; the
    newest version still holds the FIRST attempt's records. A search would
    find them; the binding refuses because the new admission is unpublished.
    """
    _git_init(deployment)
    _fail_publish(deployment, folder, admission_id=_ADM_2)
    lookup = _lookup([
        (_ADM_1, "published", 3, "okg:unit:old", "default"),
        (_ADM_2, "accepted", None, None, None),
    ])
    outcome = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}),
        runner=_FakeOkg(publish=dict(_NO_SOURCE_WORK), appended=0),
        environ={"PATH": os.environ["PATH"], "OKG_DSN": "postgresql:///fake"},
        admission_lookup=lookup,
    )
    assert outcome.status == "failed"
    failed = outcome.as_dict()["failed_step"]
    assert failed["step"] == "publish"
    assert failed["detail"]["staged_admissions"]["admissions"][_ADM_2]["status"] == (
        "accepted"
    )
    assert lookup.calls[0]["admission_ids"] == [_ADM_2]


def test_a_later_step_failing_after_the_publish_recovers_on_rerun(
    deployment: Path, folder: Path,
) -> None:
    """Delta review item 1: the publish succeeds and the sample fails. The
    attempt's admission is published, so the re-run must bind to it and
    recover, not tell the operator to remove and re-add the source."""
    _git_init(deployment)
    first = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}),
        runner=_FakeOkg(sample_source="unit.someone-else", admission_id=_ADM_1),
        environ={"PATH": os.environ["PATH"]},
    )
    assert first.status == "failed"
    assert first.as_dict()["failed_step"]["step"] == "sample"
    ledger = read_ledger(deployment, "notes")
    assert ledger["status"] == "failed"
    assert ledger["generation_id"] == "okg:unit:g1"
    assert ledger["unpublished_admission_ids"] == [_ADM_1]

    lookup = _lookup([(_ADM_1, "published", 4, "okg:unit:g1", "default")])
    outcome = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}),
        runner=_FakeOkg(publish=dict(_NO_SOURCE_WORK), appended=0),
        environ={"PATH": os.environ["PATH"], "OKG_DSN": "postgresql:///fake"},
        admission_lookup=lookup,
    )
    assert outcome.status == "no_change", outcome.as_dict()["failed_step"]
    assert outcome.generation_id == "okg:unit:g1"
    assert lookup.calls[0]["admission_ids"] == [_ADM_1]
    registry = yaml.safe_load((deployment / "source_registry.yaml").read_text())
    assert registry["sources"]["notes"]["enabled_by_default"] is True
    assert read_ledger(deployment, "notes")["unpublished_admission_ids"] == []


def test_publication_verdict_binds_only_published_default_branch_rows() -> None:
    verdict = guided_add.publication_verdict
    assert verdict([_ADM_1], [(_ADM_1, "published", 5, "g5", "default")]) == {
        "published": True, "generation_id": "g5",
        "admissions": {_ADM_1: {
            "status": "published", "generation_id": "g5", "branch": "default",
        }},
    }
    # A version published on another branch binds nothing (review item 3).
    assert verdict([_ADM_1], [(_ADM_1, "published", 5, "g5", "feature")])[
        "published"
    ] is False
    assert verdict([_ADM_1], [])["admissions"][_ADM_1]["status"] == "not_found"
    assert verdict([], [])["published"] is False
    both = verdict(
        [_ADM_1, _ADM_2],
        [(_ADM_1, "published", 5, "g5", "default"),
         (_ADM_2, "published", 9, "g9", "default")],
    )
    assert both["published"] is True and both["generation_id"] == "g9"


@pytest.mark.parametrize("previous_status", ["failed", "running"])
def test_a_failed_attempt_with_no_recorded_admission_names_the_way_out(
    deployment: Path, folder: Path, previous_status: str,
) -> None:
    """A ledger written before #2769 records no admission: nothing binds
    what is published to this source, so the refusal says how to recover."""
    _git_init(deployment)
    _fail_publish(deployment, folder)
    path = deployment / ".okg" / "source-add" / "notes.json"
    body = json.loads(path.read_text())
    del body["unpublished_admission_ids"]
    body["status"] = previous_status  # `running`: the attempt crashed
    path.write_text(json.dumps(body))
    outcome = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}),
        runner=_FakeOkg(publish=dict(_NO_SOURCE_WORK), appended=0),
        environ={"PATH": os.environ["PATH"]},
    )
    failed = outcome.as_dict()["failed_step"]
    assert failed["step"] == "publish"
    assert "Remove a source" in failed["fix_hint"]


def test_blocked_publish_names_the_version_and_a_command_that_shows_it(
    deployment: Path, folder: Path,
) -> None:
    """#2494: `okg generation list` omits blocked versions; the hint names
    the blocked version and `okg generation describe`, and the cause names
    the failed checks."""
    _git_init(deployment)
    blocked = {
        "label": "commit", "status": "blocked",
        "generation_id": "okg:unit:blocked-g2",
        "blocked_reason": {
            "node_subtype_violations": {"count": 4},
            "edge_endpoint_violations": {"count": 1},
        },
    }
    outcome = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}),
        runner=_FakeOkg(publish=blocked),
        environ={"PATH": os.environ["PATH"]},
    )
    assert outcome.status == "failed"
    failed = outcome.as_dict()["failed_step"]
    assert failed["step"] == "publish"
    assert "okg:unit:blocked-g2" in failed["cause"]
    assert (
        "blocked by edge_endpoint_violations, node_subtype_violations"
        in failed["cause"]
    )
    assert failed["fix_hint"].startswith(
        "run `okg generation describe okg:unit:blocked-g2 --deployment "
    )
    assert "generation list" not in failed["fix_hint"]
    # The failed checks stay in the step detail too.
    assert set(failed["detail"]["publish"]["blocked_reason"]) == {
        "edge_endpoint_violations", "node_subtype_violations",
    }


def _enforce_access_policy(dep: Path) -> None:
    manifest_path = dep / "deployment.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    manifest.setdefault("nomos", {})["runtime_enforcement"] = "enforce"
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")


class _FakePolicy:
    """Stand-in for the Nomos baseline reconcile the policy step runs."""

    def __init__(self, fake_okg: _FakeOkg, result: dict | Exception) -> None:
        self.fake_okg = fake_okg
        self.result = result
        self.calls: list[dict] = []

    def __call__(self, **kwargs):
        registry = yaml.safe_load(
            (Path(kwargs["deployment_dir"]) / "source_registry.yaml").read_text(),
        )
        self.calls.append({
            **kwargs,
            "registry_sources": sorted(registry["sources"]),
            "okg_calls_before": [c[c.index("okg.substrate.cli.main") + 1]
                                 for c in self.fake_okg.calls],
        })
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def test_policy_is_updated_for_the_new_source_before_the_import(
    deployment: Path, folder: Path,
) -> None:
    """#2475: an enforced access policy learns the new source before import."""
    _enforce_access_policy(deployment)
    _git_init(deployment)
    fake = _FakeOkg()
    policy = _FakePolicy(fake, {
        "converged": True, "changed": True,
        "active_after": {"policy_fingerprint": "sha256:new"},
    })
    outcome = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}), runner=fake,
        environ={"PATH": os.environ["PATH"]}, dsn="postgresql://h/db",
        reconcile_policy=policy,
    )
    assert outcome.status == "published", outcome.as_dict()
    assert len(policy.calls) == 1
    call = policy.calls[0]
    assert call["apply"] is True
    assert call["deployment"] == "unit"
    assert call["dsn"] == "postgresql://h/db"
    # The saved definition is what the policy is rebuilt from.
    assert "notes" in call["registry_sources"]
    # After the definition is saved and claimed, before anything is imported.
    assert "ingest" not in call["okg_calls_before"]
    assert call["okg_calls_before"][-1] == "catalog"  # the ownership claim
    step = next(s for s in outcome.steps if s.name == "policy")
    assert step.status == "ok"
    assert step.detail == {
        "mode": "enforce", "changed": True, "policy_fingerprint": "sha256:new",
    }


@pytest.mark.parametrize("result, expected, hint", [
    (
        {"converged": False, "desired": {"policy_fingerprint": "a"},
         "active_after": {"policy_fingerprint": "b"}},
        "still does not match", "policy activated by hand",
    ),
    (
        RuntimeError("connection to postgresql://u:hunter2@h/db refused"),
        "refused", "okg provision",
    ),
])
def test_policy_failure_stops_before_the_import_and_names_the_cause(
    deployment: Path, folder: Path, result, expected: str, hint: str,
) -> None:
    _enforce_access_policy(deployment)
    _git_init(deployment)
    fake = _FakeOkg()
    outcome = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}), runner=fake,
        environ={"PATH": os.environ["PATH"]}, dsn="postgresql://h/db",
        reconcile_policy=_FakePolicy(fake, result),
    )
    assert outcome.status == "failed"
    failed = outcome.as_dict()["failed_step"]
    assert failed["step"] == "policy"
    assert expected in failed["cause"], failed["cause"]
    assert "hunter2" not in json.dumps(outcome.as_dict())
    assert hint in failed["fix_hint"], failed["fix_hint"]
    assert not any("ingest" in c for c in fake.calls)
    registry = yaml.safe_load((deployment / "source_registry.yaml").read_text())
    assert registry["sources"]["notes"]["enabled_by_default"] is False
    assert read_ledger(deployment, "notes")["status"] == "failed"


def test_policy_uses_the_database_the_import_uses(
    deployment: Path, folder: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The real reconcile is reached with the manifest's DSN, not --dsn.

    The `okg` children of this flow get --dsn only as OKG_DSN, so a manifest
    naming a literal DSN sends the import there; the policy must follow it.
    """
    from okg.substrate.nomos import baseline

    _enforce_access_policy(deployment)
    manifest_path = deployment / "deployment.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    manifest["postgres"] = {"dsn": "postgresql://u:hunter2@manifest-host/graph"}
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")
    _git_init(deployment)
    fake = _FakeOkg()
    policy = _FakePolicy(fake, {
        "converged": True, "changed": False,
        "active_after": {"policy_fingerprint": "sha256:same"},
    })
    monkeypatch.setattr(baseline, "reconcile_deployment_baseline", policy)
    outcome = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}), runner=fake,
        environ={"PATH": os.environ["PATH"]}, dsn="postgresql://flag-host/other",
    )
    assert outcome.status == "published", outcome.as_dict()
    assert [c["dsn"] for c in policy.calls] == [
        "postgresql://u:hunter2@manifest-host/graph",
    ]


def test_policy_names_an_unresolvable_database(
    deployment: Path, folder: Path,
) -> None:
    _enforce_access_policy(deployment)  # the scaffold reads ${OKG_DSN}
    _git_init(deployment)
    fake = _FakeOkg()
    policy = _FakePolicy(fake, AssertionError("must not be called"))
    outcome = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}), runner=fake,
        environ={"PATH": os.environ["PATH"]}, reconcile_policy=policy,
    )
    assert outcome.status == "failed"
    failed = outcome.as_dict()["failed_step"]
    assert failed["step"] == "policy"
    assert "names no database" in failed["cause"]
    assert policy.calls == []
    assert not any("ingest" in c for c in fake.calls)


def test_policy_audit_failure_keeps_its_findings_and_hint(
    deployment: Path, folder: Path,
) -> None:
    _enforce_access_policy(deployment)
    _git_init(deployment)
    fake = _FakeOkg()
    error = SubstrateError(
        "Nomos deployment policy audit failed",
        data={"findings": [{"code": "sensitivity_not_allowed", "source": "notes"}]},
        fix_hint="fix the deployment Nomos posture and source policies",
    )
    outcome = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}), runner=fake,
        environ={"PATH": os.environ["PATH"]}, dsn="postgresql://h/db",
        reconcile_policy=_FakePolicy(fake, error),
    )
    failed = outcome.as_dict()["failed_step"]
    assert failed["step"] == "policy"
    assert "audit failed" in failed["cause"]
    assert failed["detail"]["error"]["findings"] == [
        {"code": "sensitivity_not_allowed", "source": "notes"},
    ]
    assert failed["fix_hint"].startswith("fix the deployment Nomos posture")


def test_policy_is_updated_in_audit_mode_too(
    deployment: Path, folder: Path,
) -> None:
    """Audit mode evaluates the same policy, so it is kept current as well."""
    manifest_path = deployment / "deployment.yaml"
    manifest = yaml.safe_load(manifest_path.read_text(encoding="utf-8"))
    manifest.setdefault("nomos", {})["runtime_enforcement"] = "audit"
    manifest_path.write_text(yaml.safe_dump(manifest, sort_keys=False), encoding="utf-8")
    _git_init(deployment)
    fake = _FakeOkg()
    policy = _FakePolicy(fake, {
        "converged": True, "changed": True,
        "active_after": {"policy_fingerprint": "sha256:a"},
    })
    outcome = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}), runner=fake,
        environ={"PATH": os.environ["PATH"]}, dsn="postgresql://h/db",
        reconcile_policy=policy,
    )
    assert outcome.status == "published", outcome.as_dict()
    assert len(policy.calls) == 1
    step = next(s for s in outcome.steps if s.name == "policy")
    assert step.detail["mode"] == "audit"


def test_policy_step_is_skipped_when_the_deployment_enforces_none(
    deployment: Path, folder: Path,
) -> None:
    _git_init(deployment)
    fake = _FakeOkg()
    policy = _FakePolicy(fake, AssertionError("must not be called"))
    outcome = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}), runner=fake,
        environ={"PATH": os.environ["PATH"]}, reconcile_policy=policy,
    )
    assert outcome.status == "published", outcome.as_dict()
    assert policy.calls == []
    step = next(s for s in outcome.steps if s.name == "policy")
    assert step.status == "skipped"


def test_display_keeps_long_file_names_and_redacts_secret_keys(tmp_path: Path) -> None:
    shown = guided_add._display_attrs({
        "path": "docs/deployment-profile-architecture-notes.md",
        "title": "Deployment profile architecture",
        "api_token": "ghp_abcdefghijklmnopqrstuvwxyz0123",
        "body": "x" * 40,
    })
    assert shown["path"] == "docs/deployment-profile-architecture-notes.md"
    assert shown["title"] == "Deployment profile architecture"
    assert shown["api_token"] == "<redacted>"
    assert "x" * 40 not in shown["body"]
    root = tmp_path / "tree"
    (root / ".git" / "objects").mkdir(parents=True)
    for index in range(30):
        (root / ".git" / "objects" / f"o{index}").write_text("x")
    (root / "docs").mkdir()
    (root / "docs" / "a-long-name-for-a-document-file.md").write_text("# a")
    (root / "top.md").write_text("# t")
    matched, truncated = guided_add._scan_folder(
        root, include=["**/*.md"], exclude=["**/.git/**"], limit=10,
    )
    assert matched == ["top.md", "docs/a-long-name-for-a-document-file.md"]
    assert truncated is False  # excluded folders are not walked


def test_apply_refuses_an_unresolved_plan(deployment: Path) -> None:
    plan = _plan(deployment, type_id="sharepoint")
    with pytest.raises(AddSourceError) as excinfo:
        apply_add_source(plan, runner=_FakeOkg())
    assert excinfo.value.data["step"] == "type"


def test_validate_failure_names_the_first_blocker(deployment: Path, folder: Path) -> None:
    _git_init(deployment)
    plan = _plan(deployment, settings={"root": str(folder)})
    outcome = apply_add_source(
        plan, runner=_FakeOkg(fail_at="deployment"), environ={"PATH": os.environ["PATH"]},
    )
    assert outcome.status == "failed" and outcome.steps[-1].name == "validate"


# ---- CLI surface ------------------------------------------------------------


def test_parse_settings_decodes_json_values() -> None:
    assert parse_settings(["root=/x", "include=[\"**/*.md\"]", "flag=true", "n=3"]) == {
        "root": "/x", "include": ["**/*.md"], "flag": True, "n": 3,
    }
    with pytest.raises(Exception):
        parse_settings(["novalue"])


def test_source_types_lists_supported_readers(capsys) -> None:
    assert cli.main(["deployment", "source-types", "--json"]) == 0
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert [t["type"] for t in payload["types"]] == ["local-folder", "github"]
    assert "new reader" in payload["unsupported"]


def test_add_source_cli_previews_and_refuses_unknown_type(
    deployment: Path, folder: Path, capsys,
) -> None:
    rc = cli.main([
        "deployment", "add-source", str(deployment), "notes",
        "--type", "local-folder", "--setting", f"root={folder}", "--json",
    ])
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert rc == 0 and payload["ok"] is True and payload["apply"] is False
    assert "--apply" in payload["next"]
    rc = cli.main([
        "deployment", "add-source", str(deployment), "x", "--type", "nope", "--json",
    ])
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert rc == 1 and payload["ok"] is False
    rc = cli.main(["deployment", "add-source", str(deployment), "x", "--json"])
    assert rc == 1


def _git_init(dep: Path) -> None:
    # No background maintenance in the fixture repo: after a commit git may
    # detach `gc --auto` / `maintenance run --auto`, which writes
    # .git/objects/maintenance.lock while a test compares the tree's files.
    # Set in the repo's own config, so the commits the code under test makes
    # here obey it too.
    for arguments in (
        ["init", "-q"],
        ["config", "maintenance.auto", "false"],
        ["config", "gc.auto", "0"],
        ["config", "gc.autoDetach", "false"],
        ["add", "."],
        ["-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-q", "-m", "s"],
    ):
        subprocess.run(["git", "-C", str(dep), *arguments], check=True, capture_output=True)


_MD = "okg.substrate.library.extraction.parse.markdown.MarkdownParser"
_TXT = "okg.substrate.library.extraction.parse.text.TextParser"
_WINDOW = "okg.substrate.library.extraction.chunk.window.WindowChunker"
_WINDOW_CONFIG = {"max_chars": 1500, "overlap": 200}


def test_apply_adds_missing_file_readers_to_an_existing_extractors_file(
    deployment: Path, folder: Path,
) -> None:
    # The folder holds Markdown only, and a `default` chunker already chunks
    # every format: only the Markdown parser is added, nothing else.
    custom = {
        "parser": [],
        "chunker": [
            {"format": "default", "class": _WINDOW, "config": {"max_chars": 900, "overlap": 0}},
        ],
        "ner": [],
    }
    (deployment / "extractors.yaml").write_text(
        "# my readers\n" + yaml.safe_dump(custom), encoding="utf-8",
    )
    _git_init(deployment)
    plan = _plan(deployment, settings={"root": str(folder)})
    readers = next(d for d in plan.decisions if d["kind"] == "file_readers")
    assert readers["changes"] == ["parser markdown"]  # shown before saving
    outcome = apply_add_source(plan, runner=_FakeOkg(), environ={"PATH": os.environ["PATH"]})
    assert outcome.status == "published"
    save = next(s for s in outcome.steps if s.name == "save")
    assert save.detail["extractors_yaml_updated"] == {
        "path": str(deployment / "extractors.yaml"),
        "added": {"parser": ["markdown"], "chunker": []},
        "rewritten": True,
        "comment_lines_dropped": 1,
    }
    written = yaml.safe_load((deployment / "extractors.yaml").read_text())
    assert [e["format"] for e in written["parser"]] == ["markdown"]
    assert written["chunker"] == custom["chunker"]  # kept, no markdown chunker added
    assert _git_clean(deployment)


def test_only_formats_the_folder_holds_are_added(
    deployment: Path, folder: Path,
) -> None:
    # An operator may drop a format on purpose; it comes back only when a
    # folder with such files is added.
    (deployment / "extractors.yaml").write_text(yaml.safe_dump({
        "parser": [{"format": "text", "class": _TXT}],
        "chunker": [{"format": "text", "class": _WINDOW, "config": {"max_chars": 900}}],
    }), encoding="utf-8")
    plan = _plan(deployment, settings={"root": str(folder)})
    assert plan.reader_formats == ["markdown"]
    readers = next(d for d in plan.decisions if d["kind"] == "file_readers")
    assert readers["changes"] == ["parser markdown", "chunker markdown"]
    (folder / "data.json").write_text('{"k": "v"}')
    plan = _plan(deployment, settings={"root": str(folder)})
    readers = next(d for d in plan.decisions if d["kind"] == "file_readers")
    assert readers["changes"] == [
        "parser markdown", "parser json", "chunker markdown", "chunker default",
    ]


@pytest.mark.parametrize(
    ("document", "expected"),
    [
        # Plural keys: the reader accepts them; add-source does not edit them.
        ({"parsers": [{"format": "text", "class": _TXT}],
          "chunkers": [{"format": "default", "class": _WINDOW, "config": _WINDOW_CONFIG}]},
         "warn"),
        # A bare chunker list chunks every format: only the parser is added,
        # and the result still loads.
        ({"parser": [], "chunker": [{"class": _WINDOW, "config": {"max_chars": 800}}]},
         "add"),
        # A format written with spaces counts as that format: nothing to add.
        ({"parser": [{"format": " markdown ", "class": _MD}],
          "chunker": [{"format": "default", "class": _WINDOW, "config": _WINDOW_CONFIG}]},
         None),
    ],
    ids=["plural-keys", "bare-chunkers", "spaced-format"],
)
def test_other_extractors_shapes_are_never_refused(
    deployment: Path, folder: Path, document: dict, expected: str | None,
) -> None:
    from okg.substrate.library.extraction.registry import load_extractors

    path = deployment / "extractors.yaml"
    path.write_text("# hand edited\n" + yaml.safe_dump(document), encoding="utf-8")
    before = path.read_bytes()
    _git_init(deployment)
    plan = _plan(deployment, settings={"root": str(folder)})
    decisions = [d for d in plan.decisions if d["kind"] == "file_readers"]
    outcome = apply_add_source(plan, runner=_FakeOkg(), environ={"PATH": os.environ["PATH"]})
    assert outcome.status == "published", [s.as_dict() for s in outcome.steps]
    save = next(s for s in outcome.steps if s.name == "save")
    if expected is None:
        assert decisions == [] and path.read_bytes() == before
        assert not {"extractors_yaml_updated", "extractors_yaml_warning"} & set(save.detail)
    elif expected == "warn":
        assert path.read_bytes() == before  # left untouched
        assert decisions[0]["why"].startswith("WARNING:")
        warning = save.detail["extractors_yaml_warning"]
        assert warning["missing"]["parser"] == ["markdown"]
        assert warning["entries_to_add"]["parser"][0]["class"] == _MD
        assert "add these entries" in save.fix_hint
    else:
        assert save.detail["extractors_yaml_updated"]["added"] == {
            "parser": ["markdown"], "chunker": [],
        }
        load_extractors(deployment)  # the reader accepts the result
    assert _git_clean(deployment)


def test_rerun_repairs_a_source_already_imported_under_an_empty_extractors_file(
    deployment: Path, folder: Path,
) -> None:
    # A folder added before #2974 left the placeholder file in place. Running
    # add-source again for the same source (or adding any local-folder
    # source) completes the file and commits it; the import that follows
    # reads the files again because the reader's settings changed.
    _git_init(deployment)
    first = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}),
        runner=_FakeOkg(), environ={"PATH": os.environ["PATH"]},
    )
    assert first.status == "published"
    _write_empty_extractors(deployment)
    subprocess.run(
        ["git", "-C", str(deployment), "-c", "user.name=t",
         "-c", "user.email=t@example.invalid", "commit", "-qam", "pre-2974 state"],
        check=True, capture_output=True,
    )
    fake = _FakeOkg()
    again = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}),
        runner=fake, environ={"PATH": os.environ["PATH"]},
    )
    assert again.status == "published"
    save = next(s for s in again.steps if s.name == "save")
    assert save.detail["extractors_yaml_updated"]["added"] == {
        "parser": ["markdown"], "chunker": ["markdown"],
    }
    assert any("ingest" in call for call in fake.calls)
    assert _git_clean(deployment)


class _BodyAwareOkg(_FakeOkg):
    """A search that finds this source only for the queries in `hits_for`."""

    def __init__(self, hits_for: set[str]) -> None:
        super().__init__()
        self.hits_for = hits_for

    def __call__(self, argv, **kwargs):
        proc = super().__call__(argv, **kwargs)
        if "search" not in argv:
            return proc
        query = argv[argv.index("--query") + 1]
        results = [{
            "node_id": "doc:notes:plan.md", "subtype": "document",
            "provenance": {"source": "unit.notes"},
        }] if query in self.hits_for else []
        return subprocess.CompletedProcess(
            argv, 0, stdout=json.dumps({"results": results}), stderr="",
        )


def _search_queries(fake: _FakeOkg) -> list[str]:
    return [c[c.index("--query") + 1] for c in fake.calls if "search" in c]


def test_sample_searches_words_from_the_file_body_not_its_name(
    deployment: Path, folder: Path,
) -> None:
    _git_init(deployment)
    plan = _plan(deployment, settings={"root": str(folder)})
    assert plan.body_queries == ["guided", "ship", "flow"]
    fake = _FakeOkg()
    outcome = apply_add_source(plan, runner=fake, environ={"PATH": os.environ["PATH"]})
    assert outcome.status == "published"
    assert _search_queries(fake)[0] == "guided"
    sample = next(s for s in outcome.steps if s.name == "sample")
    assert sample.detail == {"query": "guided", "basis": "body"}


def test_body_words_skip_urls_and_email_addresses_and_keep_eszett(
    deployment: Path, tmp_path: Path,
) -> None:
    # Text search keeps a URL or an e-mail address as one term, so a word
    # cut out of one is never found; casefold() would turn ß into ss.
    notes = tmp_path / "links"
    notes.mkdir()
    (notes / "links.md").write_text(
        "# Links\n\nSee https://example.org/wombatfield or write to "
        "kangaroo@example.org about the Straße.\n",
    )
    plan = _plan(deployment, settings={"root": str(notes)})
    assert plan.ok, [s.as_dict() for s in plan.steps]
    assert plan.body_queries == ["straße", "write", "about"]


@pytest.mark.parametrize(
    "token",
    [
        "https://example.org/wombat", "ftp://host", "kangaroo@example.org",
        "@handle", "a/b", "/abs", "C:\\path", "back\\slash", "example.org",
        "v1.2", "e.g", "end.", ".start", "a.b.c", "Straße", "plain", "",
        "://", "@", "/", "\\", "x.y", "_._", "é.ü", "1.", "word-with-dash",
        "semi;colon", "colon:only", "http:", "mailto:kangaroo",
    ],
)
def test_compound_token_check_matches_the_original_single_pattern(
    token: str,
) -> None:
    # The check was one regex until the private-DSN guard (which treats any
    # src/okg pattern holding both "://" and "@" as URL-credential handling)
    # flagged it; the split form must answer exactly as that regex did.
    import re

    original = re.compile(r"://|@|/|\\|\w\.\w")
    assert guided_add._is_compound_token(token) is bool(original.search(token))


def test_sample_tries_body_words_one_at_a_time(
    deployment: Path, folder: Path,
) -> None:
    _git_init(deployment)
    plan = _plan(deployment, settings={"root": str(folder)})
    fake = _BodyAwareOkg({"ship"})
    outcome = apply_add_source(plan, runner=fake, environ={"PATH": os.environ["PATH"]})
    assert outcome.status == "published"
    assert _search_queries(fake)[:2] == ["guided", "ship"]
    sample = next(s for s in outcome.steps if s.name == "sample")
    assert sample.detail == {
        "query": "ship", "basis": "body", "body_words_missed": ["guided"],
    }


def test_sample_falls_back_to_titles_when_every_body_word_misses(
    deployment: Path, folder: Path,
) -> None:
    _git_init(deployment)
    plan = _plan(deployment, settings={"root": str(folder)})
    title_query = next(
        r["search_text"] for r in plan.preview["records"] if r["subtype"] == "document"
    )
    fake = _BodyAwareOkg({title_query})
    outcome = apply_add_source(plan, runner=fake, environ={"PATH": os.environ["PATH"]})
    assert outcome.status == "published"
    sample = next(s for s in outcome.steps if s.name == "sample")
    assert sample.detail == {
        "query": title_query, "basis": "label",
        "body_words_missed": ["guided", "ship", "flow"],
    }


def test_sample_fails_when_only_another_source_matches(
    deployment: Path, folder: Path,
) -> None:
    class _OtherSourceOnly(_FakeOkg):
        def __call__(self, argv, **kwargs):
            proc = super().__call__(argv, **kwargs)
            if "search" in argv:
                hit = {"node_id": "doc:x", "subtype": "document",
                       "provenance": {"source": "unit.other"}}
                return subprocess.CompletedProcess(
                    argv, 0, stdout=json.dumps({"results": [hit]}), stderr="",
                )
            return proc

    _git_init(deployment)
    plan = _plan(deployment, settings={"root": str(folder)})
    outcome = apply_add_source(
        plan, runner=_OtherSourceOnly(), environ={"PATH": os.environ["PATH"]},
    )
    assert outcome.status == "failed"
    sample = outcome.steps[-1]
    assert sample.name == "sample"
    assert sample.detail["body_words_missed"] == ["guided", "ship", "flow"]
    assert "neither words from the records' text nor their titles" in sample.cause


def _big_folder(tmp_path: Path) -> Path:
    root = tmp_path / "big"
    (root / "a").mkdir(parents=True)
    for index in range(3):
        (root / "a" / f"n{index}.md").write_text(f"# Note {index}\n\nPlain words here.\n")
    (root / "z").mkdir()
    (root / "z" / "data.json").write_text('{"word": "bramblequist"}')
    return root


def test_a_scan_that_stopped_early_plans_every_standard_reader(
    deployment: Path, tmp_path: Path, monkeypatch,
) -> None:
    # The preview scan stops at PREVIEW_SCAN_LIMIT files; a format past the
    # limit (z/data.json) must still get its reader.
    monkeypatch.setattr(guided_add, "PREVIEW_SCAN_LIMIT", 2)
    _write_empty_extractors(deployment)
    plan = _plan(deployment, settings={"root": str(_big_folder(tmp_path))})
    assert plan.preview["scope"]["files_matched_is_lower_bound"] is True
    assert plan.reader_formats == ["markdown", "text", "json", "yaml", "pdf"]
    readers = next(d for d in plan.decisions if d["kind"] == "file_readers")
    assert "parser json" in readers["changes"]


def test_warning_lists_each_entry_with_its_config(
    deployment: Path, folder: Path,
) -> None:
    # An entry printed without its config cannot be pasted in as it is.
    (deployment / "extractors.yaml").write_text(
        yaml.safe_dump({"parsers": [{"format": "text", "class": _TXT}]}), encoding="utf-8",
    )
    plan = _plan(deployment, settings={"root": str(folder)})
    readers = next(d for d in plan.decisions if d["kind"] == "file_readers")
    chunker = next(c for c in readers["changes"] if c.startswith("chunker: "))
    assert json.loads(chunker.removeprefix("chunker: ")) == {
        "format": "markdown",
        "class": "okg.substrate.library.extraction.chunk.markdown_heading.MarkdownHeadingChunker",
        "config": {"min_tokens": 100, "max_tokens": 1500},
    }


def test_title_only_sample_after_a_partial_scan_warns(
    deployment: Path, tmp_path: Path, monkeypatch,
) -> None:
    monkeypatch.setattr(guided_add, "PREVIEW_SCAN_LIMIT", 2)
    _git_init(deployment)
    plan = _plan(deployment, settings={"root": str(_big_folder(tmp_path))})

    class _TitlesOnly(_BodyAwareOkg):
        def __call__(self, argv, **kwargs):
            if "search" in argv:
                self.hits_for = {argv[argv.index("--query") + 1]} - set(plan.body_queries)
            return super().__call__(argv, **kwargs)

    outcome = apply_add_source(
        plan, runner=_TitlesOnly(set()), environ={"PATH": os.environ["PATH"]},
    )
    assert outcome.status == "published", [s.as_dict() for s in outcome.steps]
    sample = next(s for s in outcome.steps if s.name == "sample")
    assert sample.detail["basis"] == "label"
    assert sample.detail["body_words_missed"] == plan.body_queries
    assert "stopped after 2 files" in sample.detail["warning"]
    assert sample.fix_hint == sample.detail["warning"]


# ---- packaged connector schemas (#2478) ---------------------------------------


_EXAMPLE_SRC = (
    Path(__file__).resolve().parents[3] / "examples" / "json-record-connector" / "src"
)


@pytest.fixture
def example_connector(tmp_path: Path, monkeypatch) -> Path:
    """The json-record-connector example's setup output, importable from source.

    The example is not installed in the test environment, so its source tree
    goes on `sys.path`; its own `setup` then writes `connector.json`, the
    sample input and the `schemas/` tree, exactly as the README runs it.
    """
    import sys

    monkeypatch.syspath_prepend(str(_EXAMPLE_SRC))
    from okg_example_records import setup as example_setup

    target = tmp_path / "connector"
    monkeypatch.setattr(sys, "argv", ["setup", str(target)])
    example_setup.main()
    return target


def _example_plan(deployment: Path, connector: Path, **overrides):
    kwargs = dict(
        name="recs", type_id=None,
        registration_path=str(connector / "connector.json"),
        settings={"path": str(connector / "records.json")},
    )
    kwargs.update(overrides)
    return _plan(deployment, **kwargs)


def test_connector_schemas_are_previewed_then_installed_and_force_the_catalog(
    deployment: Path, example_connector: Path,
) -> None:
    """#2478: add-source installs the schemas the connector ships.

    Before, only module names were added; the example's node types were
    missing from the catalog and every record blocked the publish until
    `records.yaml` was copied into the deployment by hand.
    """
    from okg.substrate.cli.source_add import _render_text

    _git_init(deployment)
    before = {p: p.read_bytes() for p in deployment.rglob("*") if p.is_file()}
    plan = _example_plan(deployment, example_connector)
    assert plan.ok, [s.as_dict() for s in plan.steps]
    decision = next(d for d in plan.decisions if d["kind"] == "connector_schemas")
    targets = {
        Path(item["to"]).relative_to(deployment).as_posix()
        for item in decision["install"]
    }
    assert targets == {"schemas/records.yaml", "schemas/bridges/records_edges.yaml"}
    assert {p: p.read_bytes() for p in deployment.rglob("*") if p.is_file()} == before
    assert "install schema: " in _render_text(plan.as_dict())

    fake = _FakeOkg(sample_source="unit.recs")
    outcome = apply_add_source(plan, runner=fake, environ={"PATH": os.environ["PATH"]})
    assert outcome.status == "published", outcome.as_dict()["failed_step"]
    for rel in ("records.yaml", "bridges/records_edges.yaml"):
        assert (deployment / "schemas" / rel).read_bytes() == (
            example_connector / "schemas" / rel
        ).read_bytes()
    catalog = next(s for s in outcome.steps if s.name == "catalog")
    assert catalog.status == "ok"
    assert len(catalog.detail["schemas_installed"]) == 2
    assert any("catalog" in c and "load" in c for c in fake.calls)
    assert _git_clean(deployment)  # the schemas are committed with the registry

    # A re-run finds the same bytes already there: nothing to copy, no decision.
    again = _example_plan(deployment, example_connector)
    assert again.ok
    assert not [d for d in again.decisions if d["kind"] == "connector_schemas"]
    schema = next(s for s in again.steps if s.name == "schema")
    assert {f["action"] for f in schema.detail["connector_schemas"]} == {"present"}


def test_a_different_schema_under_the_same_name_is_refused_before_any_write(
    deployment: Path, example_connector: Path,
) -> None:
    (deployment / "schemas" / "records.yaml").write_text(
        "node_types: {}\n", encoding="utf-8",
    )
    registry_before = (deployment / "source_registry.yaml").read_bytes()
    plan = _example_plan(deployment, example_connector)
    failed = plan.failed_step
    assert failed is not None and failed.name == "schema"
    assert "different schema file" in failed.cause
    assert failed.detail["conflicting_schema_files"] == [
        str(deployment / "schemas" / "records.yaml"),
    ]
    assert (deployment / "source_registry.yaml").read_bytes() == registry_before
    assert (deployment / "schemas" / "records.yaml").read_text() == "node_types: {}\n"


def test_a_schema_conflict_that_appears_after_the_preview_saves_nothing(
    deployment: Path, example_connector: Path,
) -> None:
    _git_init(deployment)
    plan = _example_plan(deployment, example_connector)
    assert plan.ok
    (deployment / "schemas" / "bridges").mkdir(parents=True, exist_ok=True)
    (deployment / "schemas" / "bridges" / "records_edges.yaml").write_text(
        "narrowings: []\n", encoding="utf-8",
    )
    registry_before = (deployment / "source_registry.yaml").read_bytes()
    outcome = apply_add_source(
        plan, runner=_FakeOkg(), environ={"PATH": os.environ["PATH"]},
    )
    assert outcome.status == "failed"
    failed = outcome.as_dict()["failed_step"]
    assert failed["step"] == "save" and "nothing was overwritten" in failed["cause"]
    assert (deployment / "source_registry.yaml").read_bytes() == registry_before
    assert not (deployment / "schemas" / "records.yaml").exists()


# ---- add-source rough edges (#2480) -------------------------------------------


def test_credential_is_refused_for_a_type_that_never_reads_one(
    deployment: Path, folder: Path, tmp_path: Path,
) -> None:
    """#2480: --credential was saved and silently ignored for local-folder
    and github."""
    plan = _plan(
        deployment, settings={"root": str(folder)}, credential_refs=("NOPE_VAR",),
    )
    failed = plan.failed_step
    assert failed is not None and failed.name == "access"
    assert failed.detail["code"] == "credential_not_used"
    assert "NOPE_VAR" in failed.cause and "drop --credential" in failed.fix_hint

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    gh = bin_dir / "gh"
    gh.write_text("#!/bin/sh\nexit 0\n")
    gh.chmod(gh.stat().st_mode | stat.S_IXUSR)
    plan = plan_add_source(
        deployment=str(deployment), name="w", type_id="github",
        settings={"repository": "acme/widgets"}, credential_refs=("GH_TOKEN",),
        environ={"PATH": str(bin_dir), "GH_TOKEN": "set"},
        runner=lambda argv, **kw: subprocess.CompletedProcess(argv, 0, "", ""),
    )
    failed = plan.failed_step
    assert failed is not None and failed.name == "access"
    assert failed.detail == {
        "code": "credential_not_used", "mode": "gh_cli",
        "credential_refs": ["GH_TOKEN"],
    }
    assert "gh auth login" in failed.cause


def test_catalog_failure_names_the_childs_own_cause(
    deployment: Path, folder: Path,
) -> None:
    """#2480: the catalog step said "check OKG_DSN" whatever the cause."""
    _git_init(deployment)
    fake = _FakeOkg()

    def runner(argv, **kwargs):
        if "catalog" in argv and "load" in argv:
            return subprocess.CompletedProcess(argv, 1, stdout=json.dumps({
                "error": {"message": "catalog refused", "fix_hint": "fix it"},
            }), stderr="")
        return fake(argv, **kwargs)

    outcome = apply_add_source(
        _plan(deployment, settings={"root": str(folder)}),
        runner=runner, environ={"PATH": os.environ["PATH"]},
    )
    assert outcome.status == "failed"
    failed = outcome.as_dict()["failed_step"]
    assert failed["step"] == "catalog"
    assert failed["cause"] == "the graph catalog could not be applied: catalog refused"
    assert failed["fix_hint"] == "fix it; then re-run the same command"


@pytest.mark.parametrize("interrupt", [EOFError, KeyboardInterrupt])
def test_end_of_input_at_the_prompt_saves_nothing(
    deployment: Path, folder: Path, monkeypatch, capsys, interrupt,
) -> None:
    """#2480: Ctrl-D at the confirmation prompt printed an EOFError traceback."""
    import builtins
    import sys

    def no_answer(prompt: str = "") -> str:
        raise interrupt()

    monkeypatch.setattr(sys.stdin, "isatty", lambda: True, raising=False)
    monkeypatch.setattr(builtins, "input", no_answer)
    registry_before = (deployment / "source_registry.yaml").read_bytes()
    rc = cli.main([
        "deployment", "add-source", str(deployment), "notes",
        "--type", "local-folder", "--setting", f"root={folder}", "--apply",
    ])
    assert rc == 1
    assert capsys.readouterr().out.rstrip().endswith("Nothing was saved.")
    assert (deployment / "source_registry.yaml").read_bytes() == registry_before


def test_commit_configuration_works_without_connector_schemas(
    deployment: Path, folder: Path,
) -> None:
    """Review item 1: other callers (remove-source) pass a plan-like object
    with no `connector_schemas`; the schema paths are an explicit keyword."""
    from types import SimpleNamespace

    _git_init(deployment)
    (deployment / "source_registry.yaml").write_text(
        (deployment / "source_registry.yaml").read_text() + "# touched\n",
    )
    plan_like = SimpleNamespace(
        deployment_dir=deployment, registry_path=deployment / "source_registry.yaml",
    )
    assert guided_add._commit_configuration(plan_like, "touch") is not None
    assert _git_clean(deployment)


def test_schema_paths_outside_the_deployment_are_not_committed(
    deployment: Path, tmp_path: Path,
) -> None:
    """Review item 6: the containment check compares resolved paths."""
    _git_init(deployment)
    outside = tmp_path / "outside.yaml"
    outside.write_text("x: 1\n")
    sneaky = deployment / "schemas" / ".." / ".." / outside.name
    (deployment / "source_registry.yaml").write_text(
        (deployment / "source_registry.yaml").read_text() + "# touched\n",
    )
    plan = _plan(deployment, type_id="sharepoint")
    assert guided_add._commit_configuration(
        plan, "touch", extra_paths=[str(sneaky), str(outside)],
    ) is not None
    shown = subprocess.run(
        ["git", "-C", str(deployment), "show", "--name-only", "--format=", "HEAD"],
        capture_output=True, text=True, check=True,
    ).stdout.split()
    assert shown == ["source_registry.yaml"]


def test_symlinked_connector_schema_files_are_refused(
    deployment: Path, example_connector: Path, tmp_path: Path,
) -> None:
    target = example_connector / "schemas" / "records.yaml"
    real = tmp_path / "real-records.yaml"
    real.write_bytes(target.read_bytes())
    target.unlink()
    target.symlink_to(real)
    before = {p: p.read_bytes() for p in deployment.rglob("*") if p.is_file()}
    plan = _example_plan(deployment, example_connector)
    failed = plan.failed_step
    assert failed is not None and failed.name == "schema"
    assert failed.detail["symlinked_schema_files"] == [str(target)]
    assert {p: p.read_bytes() for p in deployment.rglob("*") if p.is_file()} == before


def test_a_failed_add_names_the_schema_files_it_left_in_place(
    deployment: Path, example_connector: Path,
) -> None:
    """Review item 5: copied schema files stay after a later failure (the
    catalog may already use them); the failure says so."""
    _git_init(deployment)
    outcome = apply_add_source(
        _example_plan(deployment, example_connector),
        runner=_FakeOkg(publish=_REFUSED), environ={"PATH": os.environ["PATH"]},
    )
    assert outcome.status == "failed"
    failed = outcome.as_dict()["failed_step"]
    left = failed["detail"]["schema_files_left_in_place"]
    assert {Path(p).name for p in left} == {"records.yaml", "records_edges.yaml"}
    assert "schema files left in place: " in failed["cause"]
    # A re-run finds the same bytes and copies nothing.
    again = _example_plan(deployment, example_connector)
    assert again.ok
    assert not [d for d in again.decisions if d["kind"] == "connector_schemas"]
