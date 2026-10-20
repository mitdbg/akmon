"""retire-completed-pacts: derived open/retired status + view --format=list filter."""
from __future__ import annotations

from pathlib import Path

from okg.substrate.cli.main import main
from okg.substrate.pact.models import (
    Change,
    Evidence,
    PactManifest,
    Requirement,
    Task,
    Waiver,
)
from okg.substrate.pact.store import save_manifest
from tests.pact.test_cli import _json_out, _seed_root


def _mk(root: Path, change_id: str, *, task_status: str | None) -> None:
    """Save a PACT with one requirement and (optionally) one task at task_status.
    A `done` task carries the pytest evidence its requirement needs (so the PACT
    is valid). task_status=None -> a task-less stub."""
    tasks: list[Task] = []
    evidence: list[Evidence] = []
    if task_status is not None:
        task = Task(
            id=f"task.{change_id}", title="t",
            verification="pytest tests/pact/test_retire_completed_pacts.py -q",
            status=task_status, requirements=[f"req.{change_id}"],
        )
        if task_status == "done":
            ev = Evidence(
                id=f"ev.{change_id}", kind="pytest", status="pass",
                refs=[f"req.{change_id}"], artifact="junit.xml",
            )
            evidence = [ev]
            task.evidence = [ev.id]
        tasks = [task]
    save_manifest(root, PactManifest(
        change=Change(id=change_id, title=change_id),
        requirements=[Requirement(
            id=f"req.{change_id}", statement="s.",
            evidence_policy={"required": ["pytest"]},
        )],
        tasks=tasks,
        evidence=evidence,
    ))


def test_retire_status_is_derived(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    _mk(root, "donep", task_status="done")
    _mk(root, "openp", task_status="todo")
    _mk(root, "stubp", task_status=None)

    def _retired(cid: str) -> bool:
        assert main(["pact", "--root", str(root), "view", cid, "--format=status", "--json"]) == 0
        return _json_out(capsys)["retired"]

    assert _retired("donep") is True       # has tasks, all done
    assert _retired("openp") is False      # has a todo task
    assert _retired("stubp") is False      # no tasks -> not retired


def test_retire_list_filter(tmp_path: Path, capsys) -> None:
    root = _seed_root(tmp_path)
    _mk(root, "donep", task_status="done")
    _mk(root, "openp", task_status="todo")

    def _list(*flags: str) -> dict:
        assert main(["pact", "--root", str(root), "view", "--format=list", *flags, "--json"]) == 0
        return _json_out(capsys)

    # default == open
    default = _list()
    assert default["status_filter"] == "open"
    assert {r["change"] for r in default["changes"]} == {"openp"}

    assert {r["change"] for r in _list("--status", "open")["changes"]} == {"openp"}
    assert {r["change"] for r in _list("--status", "retired")["changes"]} == {"donep"}
    assert {r["change"] for r in _list("--status", "all")["changes"]} == {"donep", "openp"}

    # summary reports the full picture even when filtered
    summary = default["summary"]
    assert summary["total"] == 2
    assert summary["open"] == 1
    assert summary["retired"] == 1
    assert summary["shown"] == 1  # default open shows 1
    # rows carry the derived flag
    assert all("retired" in r for r in _list("--status", "all")["changes"])


def test_retire_counts_waiver_terminal_tasks(tmp_path: Path, capsys) -> None:
    """A task that is not `done` but whose requirement is excused by an
    owner-approved waiver is terminal -> the PACT retires. The waiver (with a
    recorded reason + owner), not a false `done`, is the honest record of a
    consciously declined/deferred/superseded task. An owner-less waiver does
    not count."""
    root = _seed_root(tmp_path)

    def _mk_waived(cid: str, *, owner: str | None) -> None:
        save_manifest(root, PactManifest(
            change=Change(id=cid, title=cid),
            requirements=[Requirement(
                id=f"req.{cid}", statement="s.",
                evidence_policy={"required": ["pytest"]},
            )],
            tasks=[Task(
                id=f"task.{cid}", title="t", verification="declined; see waiver",
                status="blocked", blocked_reason="superseded",
                requirements=[f"req.{cid}"],
            )],
            waivers=[Waiver(
                id=f"waiver.{cid}", scope=f"req.{cid}",
                reason="consciously declined with a recorded reason",
                owner=owner, requires_owner_approval=True,
            )],
        ))

    _mk_waived("waivedp", owner="riley")
    _mk_waived("unownedp", owner=None)

    def _retired(cid: str) -> bool:
        assert main(["pact", "--root", str(root), "view", cid, "--format=status", "--json"]) == 0
        return _json_out(capsys)["retired"]

    # blocked task, requirement waived by an owner -> terminal -> retired
    assert _retired("waivedp") is True
    # an owner-less waiver does not excuse the requirement -> still open
    assert _retired("unownedp") is False
