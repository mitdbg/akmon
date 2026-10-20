"""Per `polish-pact-agent-ux` req.polish.deferred-closure.

An unreachable decision ledger must never discard closure: gate task-done
and evidence attach queue durable local intents under the worktree-local
boundary, report the deferral loudly, replay idempotently on sync, and
pre-merge warns while intents are pending.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from okg.substrate.cli.main import main
from okg.substrate.errors import SubstrateError
from okg.substrate.pact import closure_intents
from okg.substrate.pact import decisions
from okg.substrate.pact import operations as pact_operations
from okg.substrate.pact.store import load_manifest

LEDGER_DOWN = "decision ledger unreachable: cannot DECIDE in degraded mode (test fixture)"


def _seed_root(tmp_path: Path) -> Path:
    root = tmp_path
    (root / "pact" / "schemas").mkdir(parents=True)
    (root / "pact" / "AGENTS.md").write_text("# PACT\n", encoding="utf-8")
    (root / "pact" / "project.yaml").write_text(
        "pact_version: 4\n"
        "project: okg\n"
        "pact_defaults:\n"
        "  artifact_format_version: 4\n"
        "  target_pact_version: 4\n",
        encoding="utf-8",
    )
    (root / "pact" / "schemas" / "pact.schema.yaml").write_text(
        "pact_schema_version: 0\n", encoding="utf-8"
    )
    # coordination requires a git identity (a bare tmp dir is "skipped")
    subprocess.run(["git", "init", "-q"], cwd=root, check=True)
    subprocess.run(
        ["git", "-c", "user.email=t@t", "-c", "user.name=t",
         "commit", "-q", "--allow-empty", "-m", "seed"],
        cwd=root, check=True,
    )
    assert main([
        "pact", "--root", str(root), "new", "demo",
        "--template", "protocol", "--json",
    ]) == 0
    pact_operations.add_requirement(
        root, "demo", req_id="req.demo", title="Demo",
        statement="Demo must validate.", evidence=["executed_test"],
    )
    pact_operations.add_task(
        root, "demo", task_id="task.demo", title="Do demo",
        verification="pytest tests/pact/test_deferred_closure.py -q",
        requirements=["req.demo"],
    )
    return root


def _down_ledger(monkeypatch) -> None:
    def _raise(**_kwargs):
        raise SubstrateError(LEDGER_DOWN)

    monkeypatch.setattr(decisions, "require_ledger_connection", _raise)


class _FakeConn:
    def close(self) -> None:
        pass


class _FakeLedger:
    """Reachable ledger double: counts records per (change, event payload)."""

    def __init__(self) -> None:
        self.records: list[dict] = []

    def install(self, monkeypatch) -> None:
        monkeypatch.setattr(
            decisions, "require_ledger_connection", lambda **_: _FakeConn()
        )

        def _record(conn, **kwargs):
            self.records.append(kwargs)
            return {"event_id": len(self.records), "occurred_at": "t"}

        monkeypatch.setattr(decisions, "record_coordination_event", _record)


def _attach_pass(root: Path) -> None:
    pact_operations.attach_evidence(
        root, "demo", "req.demo",
        kind="executed_test", ref="tests/x.py", status="pass",
        command="pytest -q", summary="passed",
    )


def test_attach_defers_loudly_when_ledger_down(tmp_path, monkeypatch, capsys) -> None:
    root = _seed_root(tmp_path)
    _down_ledger(monkeypatch)
    rc = main([
        "pact", "--root", str(root), "evidence", "attach", "demo", "req.demo",
        "--kind", "executed_test", "--ref", "tests/x.py", "--status", "pass",
        "--json",
    ])
    # the evidence landed but the ledger write only queued: deferred, exit 3
    assert rc == 3
    import json

    lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.strip()]
    payload = json.loads(lines[-1])
    # loud: names the deferral, the ledger reason, and the sync command
    assert payload["verdict"] == "deferred"
    assert payload["ok"] is False
    assert payload["coordination"] == "deferred"
    assert "decision ledger unreachable" in payload["ledger_reason"]
    assert "evidence sync" in payload["sync_command"]
    # durable: the intent exists under the worktree-local boundary
    pending = closure_intents.pending_intents(root, "demo")
    assert len(pending) == 1
    assert pending[0]["kind"] == "coordination_event"
    assert Path(pending[0]["path"]).is_relative_to(root / ".okg")
    # the evidence itself landed in pact.yaml (proof is never discarded)
    assert any(
        ev.kind == "executed_test" for ev in load_manifest(root, "demo").evidence
    )


def test_gate_task_done_defers_when_only_ledger_blocks(tmp_path, monkeypatch, capsys) -> None:
    root = _seed_root(tmp_path)
    _down_ledger(monkeypatch)
    _attach_pass(root)
    import json

    capsys.readouterr()
    rc = main([
        "pact", "--root", str(root), "gate", "task-done", "demo", "task.demo",
        "--json",
    ])
    lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.strip()]
    result = json.loads(lines[-1])
    # proof is complete, so the only obstacle is the ledger: the closure is
    # queued, not recorded -- deferred, exit 3, never ok
    assert rc == 3
    assert result["verdict"] == "deferred"
    assert result["ok"] is False
    assert result["marked"] is False
    assert result["missing_proof"] == []
    intent = result["closure_intent"]
    assert intent["status"] == "deferred"
    assert "decision ledger unreachable" in str(intent["reason"])
    assert "evidence sync" in intent["sync_command"]
    kinds = {rec["kind"] for rec in closure_intents.pending_intents(root, "demo")}
    assert "task_done" in kinds
    # task not marked: no reconcile edit happened
    manifest = load_manifest(root, "demo")
    assert next(t for t in manifest.tasks if t.id == "task.demo").status == "todo"


def test_gate_task_done_does_not_defer_on_missing_proof(tmp_path, monkeypatch, capsys) -> None:
    root = _seed_root(tmp_path)
    _down_ledger(monkeypatch)
    import json

    capsys.readouterr()
    main([
        "pact", "--root", str(root), "gate", "task-done", "demo", "task.demo",
        "--json",
    ])
    lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.strip()]
    result = json.loads(lines[-1])
    assert result["ok"] is False
    assert result["missing_proof"]
    assert "closure_intent" not in result
    assert not any(
        rec["kind"] == "task_done"
        for rec in closure_intents.pending_intents(root, "demo")
    )


def test_sync_replays_idempotently(tmp_path, monkeypatch) -> None:
    root = _seed_root(tmp_path)
    _down_ledger(monkeypatch)
    _attach_pass(root)
    gate = pact_operations.task_completion_gate(root, "demo", "task.demo")
    assert pact_operations.maybe_defer_task_done(root, "demo", "task.demo", gate)
    queued = len(closure_intents.pending_intents(root))
    # attach event + the gate's own progress event + the task_done intent
    assert queued == 3

    fake = _FakeLedger()
    fake.install(monkeypatch)
    first = pact_operations.sync_closure_intents(root)
    assert first["ok"] is True
    assert len(first["replayed"]) == queued
    assert len(fake.records) == queued
    # idempotent: a second sync writes nothing more
    second = pact_operations.sync_closure_intents(root)
    assert second["replayed"] == []
    assert len(fake.records) == queued
    # synced intents are preserved for audit
    assert len(list(closure_intents.synced_dir(root).glob("ci.*.yaml"))) == queued


def test_gate_task_done_derives_done_after_sync(tmp_path, monkeypatch, capsys) -> None:
    root = _seed_root(tmp_path)
    _down_ledger(monkeypatch)
    _attach_pass(root)
    gate = pact_operations.task_completion_gate(root, "demo", "task.demo")
    assert pact_operations.maybe_defer_task_done(root, "demo", "task.demo", gate)

    fake = _FakeLedger()
    fake.install(monkeypatch)
    assert pact_operations.sync_closure_intents(root)["ok"] is True
    import json

    capsys.readouterr()
    rc = main([
        "pact", "--root", str(root), "gate", "task-done", "demo", "task.demo",
        "--json",
    ])
    assert rc == 0
    lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.strip()]
    result = json.loads(lines[-1])
    assert result["ok"] is True
    assert result["marked"] is True
    manifest = load_manifest(root, "demo")
    assert next(t for t in manifest.tasks if t.id == "task.demo").status == "done"


def test_next_ledger_touching_verb_auto_replays(tmp_path, monkeypatch) -> None:
    root = _seed_root(tmp_path)
    _down_ledger(monkeypatch)
    _attach_pass(root)
    assert closure_intents.pending_intents(root)

    fake = _FakeLedger()
    fake.install(monkeypatch)
    # a second attach against the now-reachable ledger drains the queue
    pact_operations.attach_evidence(
        root, "demo", "req.demo",
        kind="executed_test", ref="tests/y.py", status="pass",
    )
    assert closure_intents.pending_intents(root) == []
    # queue drained (1 replay) + the new attach's own event
    assert len(fake.records) == 2


def test_pre_merge_warns_on_pending_intents(tmp_path, monkeypatch, capsys) -> None:
    root = _seed_root(tmp_path)
    _down_ledger(monkeypatch)
    _attach_pass(root)
    assert closure_intents.pending_intents(root, "demo")
    import json

    capsys.readouterr()
    main([
        "pact", "--root", str(root), "gate", "pre-merge", "demo", "--json",
    ])
    lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.strip()]
    result = json.loads(lines[-1])
    pending = result["pending_closure_intents"]
    assert pending["count"] == 1
    assert "not been synced" in pending["warning"]
    assert any("evidence sync" in c for c in result["recommended_commands"])


def test_mid_write_ledger_failure_queues_instead_of_dropping(tmp_path, monkeypatch, capsys) -> None:
    # Connect succeeds but the INSERT fails: the event must queue, not
    # silently vanish (same disease as a failed connect, one hop later).
    root = _seed_root(tmp_path)
    monkeypatch.setattr(
        decisions, "require_ledger_connection", lambda **_: _FakeConn()
    )

    def _boom(conn, **kwargs):
        raise RuntimeError("insert exploded mid-write")

    monkeypatch.setattr(decisions, "record_coordination_event", _boom)
    import json

    capsys.readouterr()
    rc = main([
        "pact", "--root", str(root), "evidence", "attach", "demo", "req.demo",
        "--kind", "executed_test", "--ref", "tests/x.py", "--status", "pass",
        "--json",
    ])
    # a failed INSERT defers exactly like a failed connect: exit 3, not 0
    assert rc == 3
    lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.strip()]
    payload = json.loads(lines[-1])
    assert payload["verdict"] == "deferred"
    assert payload["ok"] is False
    assert payload["coordination"] == "deferred"
    assert "insert exploded" in payload["ledger_reason"]
    assert len(closure_intents.pending_intents(root, "demo")) == 1


def test_malformed_intent_does_not_kill_the_healthy_verb(tmp_path, monkeypatch) -> None:
    root = _seed_root(tmp_path)
    # a parseable-but-wrong intent file (dict without required keys)
    directory = closure_intents.pending_dir(root)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "ci.task_done.deadbeef.yaml").write_text(
        "kind: task_done\npayload: {}\n", encoding="utf-8"
    )
    fake = _FakeLedger()
    fake.install(monkeypatch)
    pact_operations.attach_evidence(
        root, "demo", "req.demo",
        kind="executed_test", ref="tests/x.py", status="pass",
    )
    # the verb's own event landed despite the poison intent
    assert any(
        kw.get("payload", {}).get("command") == "evidence attach"
        for kw in fake.records
    )
    # the failure is visible, not swallowed: the intent stays pending and
    # explicit sync reports it loudly
    outcome = pact_operations.sync_closure_intents(root)
    assert outcome["ok"] is False
    assert outcome["failed"]


def test_sync_is_loud_when_ledger_still_down(tmp_path, monkeypatch) -> None:
    root = _seed_root(tmp_path)
    _down_ledger(monkeypatch)
    _attach_pass(root)
    outcome = pact_operations.sync_closure_intents(root)
    assert outcome["ok"] is False
    assert outcome["status"] == "ledger_unavailable"
    assert outcome["pending"] == 1
    assert "decision ledger unreachable" in outcome["reason"]
