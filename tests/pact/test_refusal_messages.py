"""A refusal is complete in one call: message, offending value, legal set.

`pact-self-repairing-refusals` req.refusal.complete. The sweep below covers
every refusal class the package can emit — schema, FSM, unknown id, ledger,
freshness, argparse — and the agent-sim at the bottom repairs three
manifest-authoring defects from the refusal TEXT ALONE: it never opens
`pact.schema.yaml`, never reads a sibling manifest, and never imports the
model it is repairing against.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from okg.substrate.errors import SubstrateError
from okg.substrate.pact import fsm, schema_errors, store
from okg.substrate.pact.cli import PACT_VERB_EXAMPLES
from okg.substrate.pact.store import load_manifest_bytes

SRC = Path(__file__).resolve().parents[2] / "src" / "okg"


def _document(change_id: str = "demo") -> dict[str, Any]:
    return {
        "pact_version": 4,
        "target_pact_version": 4,
        "change": {
            "id": change_id,
            "title": "Demo",
            "status": "implementation",
            "rationale": "Test fixture.",
            "root_cause": "Test fixture.",
        },
        "requirements": [
            {
                "id": "req.one",
                "title": "Requirement one",
                "statement": "The behavior named by req.one must hold.",
                "evidence_policy": {"required": ["executed_test"]},
                "scenarios": [],
                "acceptance": [],
            }
        ],
        "tasks": [
            {
                "id": "task.one",
                "title": "Task one",
                "status": "todo",
                "verification": "uv run pytest tests/demo/test_probe.py -q passes.",
                "requirements": ["req.one"],
                "evidence": [],
                "environment": [],
            }
        ],
        "dependencies": [],
        "test_plan": [],
        "formal": [],
        "waivers": [],
        "evidence": [],
    }


def _load(document: dict[str, Any]) -> SubstrateError:
    """Load a document and return the refusal it produced."""
    with pytest.raises(SubstrateError) as excinfo:
        load_manifest_bytes(
            yaml.safe_dump(document).encode("utf-8"),
            source_path=Path("pact/changes/demo/pact.yaml"),
        )
    return excinfo.value


# --- retired strings -------------------------------------------------------


def test_no_emitted_string_says_rerun_with_full() -> None:
    """406 emissions, 9 honored, ~450 recovery calls. Retired."""
    offenders = [
        path
        for path in SRC.rglob("*.py")
        if "rerun with --full" in path.read_text(encoding="utf-8")
        and "test" not in path.name
    ]
    # The only permitted occurrences are the comments that record the
    # retirement; the emitted VALUES must be gone.
    for path in offenders:
        for line in path.read_text(encoding="utf-8").splitlines():
            if "rerun with --full" not in line:
                continue
            assert line.lstrip().startswith("#"), f"{path}: {line}"


def test_no_hint_names_a_retired_verb(tmp_path: Path) -> None:
    """The audited hint told a cwd-drifted agent to scaffold a duplicate."""
    (tmp_path / "pact" / "changes").mkdir(parents=True)
    error = store.change_not_found(tmp_path, "absent-change")
    for verb in store.RETIRED_VERBS:
        assert verb not in (error.fix_hint or "")
        assert verb not in error.message


# --- schema refusals name the offending value AND the legal set ------------


def test_literal_error_lists_the_enum_members() -> None:
    document = _document()
    document["tasks"][0]["status"] = "in-progress"  # the hyphen is the defect
    error = _load(document)
    row = next(
        r for r in error.data["schema_errors"] if r["path"].endswith("status")
    )
    assert row["offending_value"] == "in-progress"
    assert set(row["legal_values"]) == {"todo", "in_progress", "blocked", "done"}
    assert "in_progress" in row["message"]
    # ...and the one-line summary is enough on its own.
    assert "in_progress" in error.message


def test_extra_forbidden_lists_the_legal_key_set() -> None:
    document = _document()
    document["tasks"][0]["verifification"] = "typo"
    error = _load(document)
    row = next(
        r for r in error.data["schema_errors"] if r["code"] == "extra_forbidden"
    )
    assert row["offending_key"] == "verifification"
    assert "verification" in row["legal_values"]
    assert "status" in row["legal_values"]
    assert "verification" in row["message"]


def test_type_error_names_the_expected_shape() -> None:
    document = _document()
    # The classic nesting mistake: a mapping where a list of mappings goes.
    document["requirements"] = {"id": "req.one"}
    error = _load(document)
    row = next(r for r in error.data["schema_errors"] if r["path"] == "requirements")
    assert row["expected_shape"] == "list of Requirement"
    assert "expected" in row["message"]


def test_the_vocabulary_is_read_off_the_model_not_restated() -> None:
    """A new Literal member must appear without editing this package."""
    from okg.substrate.pact.models import Task

    resolved = schema_errors.resolve_model(("tasks", 0))
    assert resolved is Task
    assert set(schema_errors.explain(
        [
            {
                "loc": ("tasks", 0, "status"),
                "type": "literal_error",
                "msg": "Input should be ...",
                "input": "nope",
            }
        ]
    )[0]["legal_values"]) == set(Task.model_fields["status"].annotation.__args__)


# --- unknown-id refusals list near misses ----------------------------------


def test_unknown_task_lists_the_manifest_ids() -> None:
    from okg.substrate.pact.models import PactManifest
    from okg.substrate.pact.operations import _task

    manifest = PactManifest.model_validate(_document())
    with pytest.raises(SubstrateError) as excinfo:
        _task(manifest, "task.on")
    assert excinfo.value.data["candidates"] == ["task.one"]
    assert "task.one" in excinfo.value.message


def test_unknown_requirement_lists_the_manifest_ids() -> None:
    from okg.substrate.pact.models import PactManifest
    from okg.substrate.pact.operations import _requirement

    manifest = PactManifest.model_validate(_document())
    with pytest.raises(SubstrateError) as excinfo:
        _requirement(manifest, "req.two")
    assert "req.one" in excinfo.value.data["candidates"]


def test_change_not_found_detects_the_sibling_worktree(tmp_path: Path) -> None:
    """cwd drift, not absence — the case the old hint got exactly backwards."""
    worktrees = tmp_path / ".claude" / "worktrees"
    here = worktrees / "lane-a"
    there = worktrees / "lane-b"
    (here / "pact" / "changes").mkdir(parents=True)
    (there / "pact" / "changes" / "elsewhere").mkdir(parents=True)
    (there / "pact" / "changes" / "elsewhere" / "pact.yaml").write_text(
        "change: {}\n", encoding="utf-8"
    )

    error = store.change_not_found(here, "elsewhere")
    assert error.data["found_in_worktrees"] == [str(there)]
    assert "another worktree" in error.fix_hint
    assert str(there) in error.fix_hint


def test_change_not_found_falls_back_to_near_misses(tmp_path: Path) -> None:
    changes = tmp_path / "pact" / "changes" / "real-change"
    changes.mkdir(parents=True)
    (changes / "pact.yaml").write_text("change: {}\n", encoding="utf-8")
    error = store.change_not_found(tmp_path, "real-chnge")
    assert error.data["candidates"] == ["real-change"]
    assert "did you mean" in error.fix_hint


# --- validation refusals carry the value AND the legal set -----------------


def _repo(tmp_path: Path) -> Path:
    (tmp_path / "pact").mkdir(parents=True, exist_ok=True)
    (tmp_path / "pact" / "project.yaml").write_text(
        yaml.safe_dump({"pact_version": 4, "project": "probe"}), encoding="utf-8"
    )
    directory = tmp_path / "pact" / "changes" / "demo"
    directory.mkdir(parents=True, exist_ok=True)
    return tmp_path


def _descriptors(tmp_path: Path, document: dict[str, Any]) -> list[dict[str, Any]]:
    from okg.substrate.pact.models import PactManifest
    from okg.substrate.pact.operations import manifest_edit_descriptors
    from okg.substrate.pact.validation import validate_manifest

    root = _repo(tmp_path)
    (root / "pact" / "changes" / "demo" / "pact.yaml").write_text(
        yaml.safe_dump(document, sort_keys=False), encoding="utf-8"
    )
    manifest = PactManifest.model_validate(document)
    report = validate_manifest(manifest, repo_root=root)
    return manifest_edit_descriptors(
        root,
        "demo",
        manifest,
        [issue for issue in report.issues if issue.severity == "error"],
    )


def test_every_edit_descriptor_carries_the_offending_value(tmp_path: Path) -> None:
    """Declared and POPULATED. The field existed and was never set, so a blind
    applier could not tell which element of a list to replace — and the value
    was recoverable only by parsing the message, which is folklore with a
    parser attached."""
    document = _document()
    document["evidence"] = [{
        "id": "ev.dangling",
        "kind": "executed_test",
        "status": "pass",
        "refs": ["req.does-not-exist"],
        "artifact": "tests/demo/test_probe.py",
    }]
    document["tasks"][0]["requirements"] = []

    descriptors = _descriptors(tmp_path, document)
    by_code = {row["code"]: row for row in descriptors}

    assert by_code["unknown_evidence_ref"]["offending_value"] == "req.does-not-exist"
    assert by_code["unknown_evidence_ref"]["pointer"] == "evidence[0].refs"
    # The subject-deletion codes name the EMPTY value, which is the defect.
    assert by_code["task_requirements_link"]["offending_value"] == []
    for row in descriptors:
        assert "offending_value" in row, row


def test_the_evidence_ref_legal_set_is_the_checks_own_vocabulary(
    tmp_path: Path,
) -> None:
    """One expression, not two lists that agree today.

    The published legal set was requirement + acceptance ids, while the check
    accepts task, test-plan, formal and change ids too — so the refusal told an
    author to change a ref the validator already allows.
    """
    from okg.substrate.pact.models import PactManifest
    from okg.substrate.pact.validation import evidence_ref_vocabulary

    document = _document()
    document["evidence"] = [{
        "id": "ev.dangling",
        "kind": "executed_test",
        "status": "pass",
        "refs": ["nope"],
        "artifact": "tests/demo/test_probe.py",
    }]
    descriptors = _descriptors(tmp_path, document)
    row = next(r for r in descriptors if r["code"] == "unknown_evidence_ref")
    manifest = PactManifest.model_validate(document)
    assert set(row["legal_values"]) == evidence_ref_vocabulary(manifest)
    assert "task.one" in row["legal_values"]


def test_the_unlinked_done_task_arm_has_a_descriptor(tmp_path: Path) -> None:
    """`task_done_missing_evidence` (link arm) had no repair descriptor at all.

    Its subject-deletion arm is the one mechanically repairable case of that
    code, and it was published as unrepairable. It shares ONE descriptor with
    `task_requirements_link` — the same edit — rather than emitting two, which
    would make the second unapplicable the moment the first landed.
    """
    document = _document()
    document["tasks"][0]["status"] = "done"
    document["tasks"][0]["requirements"] = []

    descriptors = _descriptors(tmp_path, document)
    pointers = [row["pointer"] for row in descriptors]
    assert pointers.count("tasks[0].requirements") == 1, descriptors
    row = next(r for r in descriptors if r["pointer"] == "tasks[0].requirements")
    covered = {row["code"], *(row.get("also_clears") or [])}
    assert covered == {"task_requirements_link", "task_done_missing_evidence"}
    assert row["legal_values"] == ["req.one"]


# --- FSM + ledger refusals -------------------------------------------------


def test_fsm_refusal_carries_the_legal_set_and_the_way_out() -> None:
    with pytest.raises(SubstrateError) as excinfo:
        fsm.legal_transition("in_progress", "approve", is_operator=True)
    data = excinfo.value.data
    assert data["legal_verdicts_here"]
    assert data["enabling_path"]["events"]


def test_ledger_refusal_names_owner_and_remedy() -> None:
    from okg.substrate.pact import repair

    remedy = repair.ledger_remedy({"deployment": "okg-workspace", "reason": "no dsn"})
    assert remedy["owner"]
    assert "okg-workspace" in remedy["remedy"]
    assert remedy["in_band_repair"] is False


# --- argparse errors are scoped to the verb --------------------------------


def test_every_pact_verb_has_its_own_example() -> None:
    """One hardcoded example on every subparser is worse than none: it looks
    authoritative and belongs to a different verb."""
    import argparse

    from okg.substrate.cli._options import add_json_flag
    from okg.substrate.pact import cli as pact_cli

    parser = argparse.ArgumentParser(add_help=False)
    top = parser.add_subparsers(dest="command")
    pact_cli.register_subparser(top, add_json_flag, argv=[])
    pact = top.choices["pact"]

    def _walk(node: argparse.ArgumentParser, path: str) -> None:
        example = getattr(node, "_okg_actionable_example", None)
        assert example, path
        if path:
            leaf = path.split()[-1]
            if leaf in store.RETIRED_VERBS:
                # A retired alias shares the live verb's parser, and its
                # example must name the LIVE spelling — never the retired one.
                assert leaf not in example, (path, example)
            else:
                # The example must exercise the verb it is attached to, not
                # `view --format=next`.
                assert leaf in example or path in example, (path, example)
        for action in node._actions:
            if not isinstance(action, argparse._SubParsersAction):
                continue
            for name, child in action.choices.items():
                _walk(child, f"{path} {name}".strip())

    _walk(pact, "")


def test_the_universal_example_is_not_installed_on_leaf_verbs() -> None:
    universal = PACT_VERB_EXAMPLES[""]
    for path, example in PACT_VERB_EXAMPLES.items():
        if path in ("", "view"):
            continue
        assert example != universal, path


# --- the agent-sim ---------------------------------------------------------
# It sees ONLY the refusal payload. No schema file, no sibling manifest, no
# import of the model. If the refusal is complete, this closes in one fix.


def _agent_sim_fix(document: dict[str, Any], refusal: dict[str, Any]) -> dict[str, Any]:
    """Apply the corrections a refusal describes. Reads nothing else."""
    patched = copy.deepcopy(document)
    for row in refusal["schema_errors"]:
        parts = [
            int(part) if part.isdigit() else part for part in row["path"].split(".")
        ]
        parent: Any = patched
        for part in parts[:-1]:
            parent = parent[part]
        leaf = parts[-1]
        if row["code"] == "extra_forbidden":
            # The legal key set is in the refusal; the closest one wins.
            import difflib

            close = difflib.get_close_matches(str(leaf), row["legal_values"], n=1)
            value = parent.pop(leaf)
            if close:
                parent[close[0]] = value
        elif row["code"].startswith("literal_error"):
            import difflib

            close = difflib.get_close_matches(
                str(row["offending_value"]), row["legal_values"], n=1, cutoff=0.4
            )
            # No `legal_values[0]` fallback. Picking the first member of an
            # enum when nothing matched is a GUESS, and a guess that happens to
            # load would let this test pass on a refusal whose vocabulary is
            # useless — the sim would be proving its own fallback, not the
            # refusal's completeness. A miss is a failure.
            assert close, (
                f"{row['code']} at {row['path']}: refusal offered "
                f"{row['legal_values']} for {row['offending_value']!r}, none "
                "close enough to determine the repair"
            )
            parent[leaf] = close[0]
        elif "list of" in str(row.get("expected_shape")):
            parent[leaf] = [parent[leaf]]
    return patched


@pytest.mark.parametrize(
    ("label", "break_it"),
    [
        ("wrong enum", lambda d: d["tasks"][0].__setitem__("status", "in-progress")),
        (
            "extra key",
            lambda d: d["requirements"][0].__setitem__("statment", d["requirements"][0].pop("statement")),
        ),
        ("wrong nesting", lambda d: d.__setitem__("requirements", d["requirements"][0])),
    ],
)
def test_agent_sim_repairs_the_manifest_from_the_refusal_alone(
    label: str, break_it: Any
) -> None:
    document = _document()
    break_it(document)
    error = _load(document)
    refusal = json.loads(json.dumps(error.data, default=str))

    fixed = _agent_sim_fix(document, refusal)

    # ONE fix, then it loads. The audited replay was 3-16 calls of grepping
    # `pact.schema.yaml` or copying a sibling PACT.
    manifest = load_manifest_bytes(
        yaml.safe_dump(fixed).encode("utf-8"),
        source_path=Path("pact/changes/demo/pact.yaml"),
    )
    assert manifest.change.id == "demo", label
    assert manifest.requirements[0].statement
