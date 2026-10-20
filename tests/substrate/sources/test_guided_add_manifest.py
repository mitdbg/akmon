"""`okg deployment add-source --manifest` (PACT no-code-source-manifest).

No database: the planning half runs against a real scaffold deployment and
real input files; the write half runs with a fake `okg` runner, as the other
guided add-source tests do. The real import and publish are proven on a
disposable deployment (the PACT's published-generation evidence).
"""
from __future__ import annotations

import json
import os
import subprocess
import textwrap
from pathlib import Path

import pytest
import yaml

from okg.substrate.cli import main as cli
from okg.substrate.deployment_scaffold import create_deployment_scaffold
from okg.substrate.sources.guided_add import apply_add_source, plan_add_source
from okg.substrate.sources.manifest import read_generated_header


MANIFEST = """
manifest: okg.source/v1
description: Synthetic tickets for tests.
read:
  files: exports/*.json
  records: issues
types:
  tracker_ticket:
    key: "{key}"
    title: fields.summary
    text: fields.description
    fields:
      status: fields.status
    links:
      assigned_to: { to: tracker_person, key: "{fields.assignee.email}" }
      blocks: { to: tracker_ticket, each: fields.blocks, key: "{.}" }
  tracker_person:
    from: fields.assignee
    key: "{email}"
    title: name
"""


def _ticket(key: str, *, blocks=(), email="ann@example.org") -> dict:
    return {"key": key, "fields": {
        "summary": f"Ticket {key}", "status": "open",
        "description": f"Pump {key} reports turbidity alarms",
        "assignee": {"email": email, "name": "Ann"}, "blocks": list(blocks),
    }}


@pytest.fixture
def deployment(tmp_path: Path) -> Path:
    dep = create_deployment_scaffold("unit", deployments_root=tmp_path / "deployments").deployment_dir
    for arguments in (
        ["init", "-q"], ["add", "."],
        ["-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-q", "-m", "s"],
    ):
        subprocess.run(["git", "-C", str(dep), *arguments], check=True, capture_output=True)
    return dep


@pytest.fixture
def manifest(tmp_path: Path) -> Path:
    exports = tmp_path / "exports"
    exports.mkdir()
    (exports / "a.json").write_text(json.dumps({"issues": [
        _ticket("T-1", blocks=("T-2", "T-404")), _ticket("T-2"),
    ]}))
    path = tmp_path / "tickets.yaml"
    path.write_text(textwrap.dedent(MANIFEST))
    return path


def _plan(deployment: Path, manifest: Path, name: str = "tickets"):
    return plan_add_source(
        deployment=str(deployment), name=name, type_id=None, settings={},
        manifest_path=str(manifest), environ={"PATH": os.environ.get("PATH", "")},
    )


def _snapshot(root: Path) -> dict[Path, bytes]:
    return {p: p.read_bytes() for p in root.rglob("*") if p.is_file() and ".git" not in p.parts}


class _FakeOkg:
    def __init__(self, source: str = "unit.tickets") -> None:
        self.source = source
        self.calls: list[list[str]] = []

    def __call__(self, argv, **kwargs):
        argv = list(argv)
        self.calls.append(argv)
        verb = argv[argv.index("okg.substrate.cli.main") + 1]
        payload: dict = {"ok": True}
        if verb == "ingest":
            payload = {
                "results": [{
                    "name": argv[argv.index("--include") + 1], "status": "OK",
                    "nodes_appended": 3, "edges_appended": 3,
                    "admission_id": "00000000-0000-4000-8000-0000000000aa",
                }],
                "publishes": [{"status": "published", "generation_id": "okg:unit:g1"}],
            }
        elif verb == "search":
            payload = {"results": [{
                "node_id": "tickets:tracker_ticket:T-1", "subtype": "tracker_ticket",
                "attrs": {"title": "Ticket T-1"}, "source": self.source,
            }]}
        return subprocess.CompletedProcess(argv, 0, stdout=json.dumps(payload), stderr="")


def _git_clean(dep: Path) -> bool:
    return subprocess.run(
        ["git", "-C", str(dep), "status", "--porcelain", "--untracked-files=all"],
        capture_output=True, text=True, check=True,
    ).stdout == ""


# ---- preview -----------------------------------------------------------------


def test_preview_reports_everything_and_writes_nothing(deployment: Path, manifest: Path) -> None:
    before = _snapshot(deployment)
    plan = _plan(deployment, manifest)
    assert plan.ok, [s.as_dict() for s in plan.steps]
    assert _snapshot(deployment) == before
    section = plan.preview["manifest"]
    assert section["files"] == [str(manifest.parent / "exports" / "a.json")]
    assert section["records_per_type"] == {"tracker_ticket": 2, "tracker_person": 1}
    assert [s["key"] for s in section["samples"]["tracker_ticket"]] == ["T-1", "T-2"]
    assert section["types"] == ["tracker_ticket", "tracker_ticket_passage", "tracker_person"]
    assert "tracker_ticket blocks tracker_ticket" in section["links"]
    assert section["dropped_links"] == 1
    dropped = [p for p in section["problems"] if p["code"] == "link_dropped"]
    assert dropped and dropped[0]["record"] == "T-1" and dropped[0]["fix"]
    assert section["admission"] == {"accepted": True, "problems": []}
    decision = next(d for d in plan.decisions if d["kind"] == "generated_schemas")
    assert {Path(i["to"]).relative_to(deployment).as_posix() for i in decision["install"]} == {
        "sources/tickets.source.yaml", "schemas/tickets.yaml", "schemas/bridges/tickets_links.yaml",
    }
    assert plan.entry["module"] == "okg.substrate.library.sources.manifest_records"
    assert plan.body_queries  # words from the ticket text for the sample search


def test_preview_cli_prints_the_report(deployment: Path, manifest: Path, capsys) -> None:
    rc = cli.main([
        "deployment", "add-source", str(deployment), "tickets", "--manifest", str(manifest),
    ])
    out = capsys.readouterr().out
    assert rc == 0, out
    assert "Records per type: tracker_ticket=2, tracker_person=1" in out
    assert "links dropped (target not in this read): 1" in out
    assert "Admission would accept the records: yes" in out


def test_manifest_cannot_be_combined_with_a_type(deployment: Path, manifest: Path, capsys) -> None:
    rc = cli.main([
        "deployment", "add-source", str(deployment), "tickets", "--manifest", str(manifest),
        "--type", "local-folder", "--json",
    ])
    assert rc != 0
    captured = capsys.readouterr()
    assert "--manifest describes the whole source" in captured.out + captured.err


def test_a_problem_that_stops_the_import_stops_the_preview(deployment: Path, manifest: Path) -> None:
    (manifest.parent / "exports" / "a.json").write_text(json.dumps({"issues": [
        _ticket("T-1"), _ticket("T-1"),
    ]}))
    plan = _plan(deployment, manifest)
    failed = plan.failed_step
    assert failed is not None and failed.name == "preview"
    assert "appears twice in one file" in failed.cause
    assert failed.detail["problems"][0]["code"] == "duplicate_key_in_file"


def test_a_refused_manifest_names_the_path_and_fix(deployment: Path, manifest: Path) -> None:
    manifest.write_text(textwrap.dedent(MANIFEST).replace("tracker_", ""))
    plan = _plan(deployment, manifest)
    failed = plan.failed_step
    assert failed.name == "manifest"
    assert failed.detail["code"] == "type_name_reserved"
    assert failed.detail["path"] == "types.ticket"
    assert "tickets_ticket" in failed.fix_hint


def test_a_type_another_source_declares_is_refused(deployment: Path, manifest: Path) -> None:
    other = manifest.parent / "other.yaml"
    other.write_text(textwrap.dedent(MANIFEST))
    outcome = apply_add_source(_plan(deployment, other, name="crm"), runner=_FakeOkg("unit.crm"),
                               environ={"PATH": os.environ["PATH"]})
    assert outcome.status == "published", outcome.as_dict()["failed_step"]
    plan = _plan(deployment, manifest)
    assert plan.failed_step.detail["code"] == "type_name_taken"


def test_a_containment_cycle_is_named_in_the_preview(deployment: Path, manifest: Path) -> None:
    manifest.write_text(textwrap.dedent(MANIFEST).replace("      assigned_to:", "      has_part:") + (
        "    links:\n      owns: { to: tracker_ticket, key: \"{t}\" }\n"
    ))
    failed = _plan(deployment, manifest).failed_step
    assert failed.detail["code"] == "containment_cycle"


# ---- apply --------------------------------------------------------------------


def test_apply_saves_the_copy_and_types_commits_and_publishes(deployment: Path, manifest: Path) -> None:
    plan = _plan(deployment, manifest)
    fake = _FakeOkg()
    outcome = apply_add_source(plan, runner=fake, environ={"PATH": os.environ["PATH"]})
    assert outcome.status == "published", outcome.as_dict()["failed_step"]
    assert [s.name for s in outcome.steps] == [
        "save", "validate", "catalog", "claim", "policy", "import", "publish", "sample",
    ]
    for rel in ("sources/tickets.source.yaml", "schemas/tickets.yaml", "schemas/bridges/tickets_links.yaml"):
        header = read_generated_header((deployment / rel).read_text())
        assert header is not None and header.source == "tickets" and header.body_intact
    saved = yaml.safe_load((deployment / "sources/tickets.source.yaml").read_text())
    assert saved["read"]["files"] == str(manifest.parent / "exports" / "*.json")
    registry = yaml.safe_load((deployment / "source_registry.yaml").read_text())
    entry = registry["sources"]["tickets"]
    assert entry["params"] == {
        "manifest": "${deployment_dir}/sources/tickets.source.yaml", "source": "tickets",
    }
    assert any("catalog" in c and "load" in c for c in fake.calls)
    assert _git_clean(deployment)


def test_re_applying_the_same_manifest_changes_nothing(deployment: Path, manifest: Path) -> None:
    apply_add_source(_plan(deployment, manifest), runner=_FakeOkg(), environ={"PATH": os.environ["PATH"]})
    again = _plan(deployment, manifest)
    assert again.ok and again.existing_entry is not None
    assert {f["action"] for f in again.generated_files} == {"present"}
    assert not [d for d in again.decisions if d["kind"] == "generated_schemas"]


def test_an_additive_re_apply_replaces_only_generated_files(deployment: Path, manifest: Path) -> None:
    apply_add_source(_plan(deployment, manifest), runner=_FakeOkg(), environ={"PATH": os.environ["PATH"]})
    manifest.write_text(textwrap.dedent(MANIFEST).replace(
        "      status: fields.status\n", "      status: fields.status\n      area: fields.area\n",
    ))
    plan = _plan(deployment, manifest)
    assert plan.ok, plan.failed_step and plan.failed_step.as_dict()
    actions = {Path(f["to"]).name: f["action"] for f in plan.generated_files}
    assert actions["tickets.yaml"] == "replace"
    outcome = apply_add_source(plan, runner=_FakeOkg(), environ={"PATH": os.environ["PATH"]})
    assert outcome.status == "published"
    schema = yaml.safe_load((deployment / "schemas" / "tickets.yaml").read_text())
    assert "area" in schema["classes"]["TrackerTicket"]["attributes"]
    assert _git_clean(deployment)


@pytest.mark.parametrize("change", [
    lambda t: t.replace('key: "{key}"', 'key: "{fields.id}"'),
    lambda t: t.replace("    title: fields.summary\n", ""),
])
def test_a_non_additive_re_apply_is_refused_with_the_manual_path(
    deployment: Path, manifest: Path, change,
) -> None:
    apply_add_source(_plan(deployment, manifest), runner=_FakeOkg(), environ={"PATH": os.environ["PATH"]})
    before = _snapshot(deployment)
    manifest.write_text(change(textwrap.dedent(MANIFEST)))
    plan = _plan(deployment, manifest)
    failed = plan.failed_step
    assert failed is not None and failed.name == "manifest"
    assert "okg deployment remove-source unit tickets --apply" in failed.fix_hint
    assert _snapshot(deployment) == before


def test_a_hand_edited_generated_schema_is_refused(deployment: Path, manifest: Path) -> None:
    apply_add_source(_plan(deployment, manifest), runner=_FakeOkg(), environ={"PATH": os.environ["PATH"]})
    schema = deployment / "schemas" / "tickets.yaml"
    schema.write_text(schema.read_text().replace("A tracker_ticket record", "My own words"))
    manifest.write_text(textwrap.dedent(MANIFEST).replace(
        "      status: fields.status\n", "      status: fields.status\n      area: fields.area\n",
    ))
    failed = _plan(deployment, manifest).failed_step
    assert failed is not None and failed.name == "schema"
    assert "edited by hand" in failed.cause
    assert "My own words" in schema.read_text()


def test_a_schema_file_okg_did_not_write_is_never_replaced(deployment: Path, manifest: Path) -> None:
    (deployment / "schemas").mkdir(exist_ok=True)
    (deployment / "schemas" / "tickets.yaml").write_text("classes: {}\n")
    failed = _plan(deployment, manifest).failed_step
    assert failed is not None and failed.name == "schema"
    assert "was not written by okg" in failed.cause
    assert (deployment / "schemas" / "tickets.yaml").read_text() == "classes: {}\n"


def test_deletion_never_says_records_are_never_removed(deployment: Path, manifest: Path) -> None:
    manifest.write_text(textwrap.dedent(MANIFEST) + "deletion: never\n")
    plan = _plan(deployment, manifest)
    assert "never removed" in plan.as_dict()["behaviour"]["removal"]



# ---- review fixes (69ef1b46d9) ------------------------------------------------


def test_a_saved_source_whose_manifest_copy_is_missing_is_refused(deployment: Path, manifest: Path) -> None:
    """N6: without the saved copy the additive check cannot run."""
    apply_add_source(_plan(deployment, manifest), runner=_FakeOkg(), environ={"PATH": os.environ["PATH"]})
    (deployment / "sources" / "tickets.source.yaml").unlink()
    manifest.write_text(textwrap.dedent(MANIFEST).replace('key: "{key}"', 'key: "{fields.id}"'))
    failed = _plan(deployment, manifest).failed_step
    assert failed is not None and failed.name == "manifest"
    assert "manifest copy" in failed.cause and "is missing" in failed.cause
    assert "remove-source unit tickets --apply" in failed.fix_hint


def test_a_rejection_after_import_is_a_note_that_deletion_is_withheld(deployment: Path, manifest: Path) -> None:
    """The reader applies the valid records and withholds all deletion, so a
    rejection does not stop the preview; it says what the import will do."""
    exports = manifest.parent / "exports" / "a.json"
    exports.write_text(json.dumps({"issues": [_ticket("T-1"), _ticket("T-2"), _ticket("T-3\nX")]}))
    apply_add_source(_plan(deployment, manifest), runner=_FakeOkg(), environ={"PATH": os.environ["PATH"]})
    again = _plan(deployment, manifest)
    assert again.ok
    note = next(p for p in again.preview["manifest"]["problems"] if p["code"] == "key_invalid")
    assert note["blocking"] is False
    assert "no record is removed by any read until this is fixed" in note["message"]


def test_a_few_keyless_rows_are_a_note_without_withheld_deletion(deployment: Path, manifest: Path) -> None:
    """keyless A: a row with no key is skipped; deletion still goes ahead."""
    exports = manifest.parent / "exports" / "a.json"
    bad = _ticket("T-11")
    bad["key"] = ""
    exports.write_text(json.dumps({"issues": [_ticket(f"T-{n}") for n in range(1, 11)] + [bad]}))
    plan = _plan(deployment, manifest)
    assert plan.ok, plan.failed_step and plan.failed_step.as_dict()
    note = next(p for p in plan.preview["manifest"]["problems"] if p["code"] == "key_missing")
    assert note["blocking"] is False
    assert "no record is removed" not in note["message"]


def test_too_many_keyless_rows_block_the_preview(deployment: Path, manifest: Path) -> None:
    """Review M1: the preview refuses what every ingest would refuse."""
    exports = manifest.parent / "exports" / "a.json"
    bad = _ticket("T-3")
    bad["key"] = ""
    exports.write_text(json.dumps({"issues": [_ticket("T-1"), _ticket("T-2"), bad]}))
    plan = _plan(deployment, manifest)
    assert not plan.ok
    assert "have no key value" in plan.failed_step.cause


def test_after_removal_any_change_is_allowed(deployment: Path, manifest: Path) -> None:
    """The refusal's manual path (remove, then add again) must work: once the
    registry entry is gone, a key change is not refused."""
    apply_add_source(_plan(deployment, manifest), runner=_FakeOkg(), environ={"PATH": os.environ["PATH"]})
    registry_path = deployment / "source_registry.yaml"
    registry = yaml.safe_load(registry_path.read_text())
    del registry["sources"]["tickets"]
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False))
    manifest.write_text(textwrap.dedent(MANIFEST).replace('key: "{key}"', 'key: "{fields.summary}"'))
    plan = _plan(deployment, manifest)
    assert plan.ok, plan.failed_step and plan.failed_step.as_dict()
    actions = {Path(f["to"]).name: f["action"] for f in plan.generated_files}
    assert actions["tickets.source.yaml"] == "replace"
