"""improve-pact-closure-ergonomics: non-worsening edits + waiver owner default."""
from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.errors import SubstrateError
from okg.substrate.pact.models import (
    Change,
    Evidence,
    PactManifest,
    Requirement,
    Task,
)
from okg.substrate.pact.decisions import acting_principal
from okg.substrate.pact.operations import apply_edit, request_waiver
from okg.substrate.pact.store import load_manifest, save_manifest
from okg.substrate.pact.validation import _waiver_for, validate_manifest
from tests.pact.test_cli import _seed_root


def _errors(manifest: PactManifest) -> list:
    return [i for i in validate_manifest(manifest).issues if i.severity == "error"]


def _two_error_manifest(root: Path, cid: str) -> None:
    """A PACT with two `done` tasks that lack their required evidence -> two
    `task_done_missing_evidence` errors (an already-invalid manifest)."""
    save_manifest(root, PactManifest(
        change=Change(id=cid, title=cid),
        requirements=[
            Requirement(id="req.a", statement="a.",
                        evidence_policy={"required": ["pytest"]}),
            Requirement(id="req.b", statement="b.",
                        evidence_policy={"required": ["pytest"]}),
        ],
        tasks=[
            Task(id="task.a", title="a", verification="v",
                 status="done", requirements=["req.a"]),
            Task(id="task.b", title="b", verification="v",
                 status="done", requirements=["req.b"]),
        ],
    ))


def test_atomic_edit_accepts_non_worsening_edit(tmp_path: Path) -> None:
    """Attaching evidence to ONE requirement of an already-invalid PACT is
    accepted even though the other requirement still leaves the manifest
    invalid — so a PACT can be repaired one requirement at a time."""
    root = _seed_root(tmp_path)
    _two_error_manifest(root, "p")
    assert len(_errors(load_manifest(root, "p"))) == 2

    def attach_req_a(m: PactManifest) -> None:
        m.evidence.append(Evidence(
            id="ev.a", kind="pytest", status="pass",
            refs=["req.a"], artifact="junit.xml",
        ))

    # MUST NOT raise: it fixes one error and introduces none.
    apply_edit(root, "p", command="attach-evidence",
               affected_ids=["req.a"], mutate=attach_req_a)

    remaining = _errors(load_manifest(root, "p"))
    assert len(remaining) == 1  # one fixed, one (req.b) still open
    assert remaining[0].code == "task_done_missing_evidence"


def test_atomic_edit_rejects_new_error(tmp_path: Path) -> None:
    """An edit that introduces a NEW error (a third done task with no evidence)
    is still rejected, and is not persisted."""
    root = _seed_root(tmp_path)
    _two_error_manifest(root, "p")

    def add_unevidenced_done_task(m: PactManifest) -> None:
        m.requirements.append(Requirement(
            id="req.c", statement="c.",
            evidence_policy={"required": ["pytest"]}))
        m.tasks.append(Task(
            id="task.c", title="c", verification="v",
            status="done", requirements=["req.c"]))

    with pytest.raises(SubstrateError):
        apply_edit(root, "p", command="bad",
                   affected_ids=["task.c"], mutate=add_unevidenced_done_task)

    # rejected edits are not saved: still two tasks, two errors.
    reloaded = load_manifest(root, "p")
    assert {t.id for t in reloaded.tasks} == {"task.a", "task.b"}
    assert len(_errors(reloaded)) == 2


def test_waiver_owner_defaults_to_actor(tmp_path: Path) -> None:
    """request_waiver with no owner stamps the acting principal, so the waiver
    is honored by `_waiver_for` (which ignores owner-less waivers)."""
    root = _seed_root(tmp_path)
    save_manifest(root, PactManifest(
        change=Change(id="w", title="w"),
        requirements=[Requirement(id="req.x", statement="x.",
                                  evidence_policy={"required": ["pytest"]})],
        tasks=[Task(id="task.x", title="x", verification="v",
                    status="todo", requirements=["req.x"])],
    ))

    manifest = request_waiver(
        root, "w", waiver_id=None, scope="req.x",
        reason="declined; see rationale", owner=None, expires=None,
    )
    waiver = manifest.waivers[-1]
    # request_waiver resolves the owner with acting_principal() (OKG_PRINCIPAL,
    # else the OS user). _actor() reads $USER and falls back to "agent", so it
    # agrees only where $USER is set, as on a VM login; an ARC pod sets no
    # $USER, and there it compared "runner" with "agent".
    assert waiver.owner == acting_principal()
    assert waiver.owner  # non-empty, so _waiver_for honors it
    assert _waiver_for(manifest, "req.x") == waiver.id
