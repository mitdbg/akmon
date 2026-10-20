from __future__ import annotations

import json

import pytest

from okg.substrate.library.sources.agent_runtime_security_monitor import (
    AgentRuntimeSecurityMonitorSource,
)


_VALID_SNAPSHOT = {
    "captured_at": "2026-08-18T12:00:00Z",
    "processes": [],
}


def _source(tmp_path, *names: str) -> AgentRuntimeSecurityMonitorSource:
    return AgentRuntimeSecurityMonitorSource(
        repo="issue-979",
        repo_root=tmp_path,
        snapshot_paths=names,
    )


def test_mixed_snapshot_failure_refuses_scope_then_corrected_retry_succeeds(
    tmp_path,
) -> None:
    valid = tmp_path / "valid.json"
    malformed = tmp_path / "malformed.json"
    valid.write_text(json.dumps(_VALID_SNAPSHOT), encoding="utf-8")
    malformed.write_text('{"secret":"must-not-appear"', encoding="utf-8")
    source = _source(tmp_path, valid.name, malformed.name)

    with pytest.raises(ValueError, match="snapshot is malformed JSON") as exc:
        source.run("failed", mode="reconcile")

    assert "must-not-appear" not in str(exc.value)
    malformed.write_text(json.dumps(_VALID_SNAPSHOT), encoding="utf-8")

    recovered = source.run("recovered", mode="reconcile")
    recovered_facts = list(recovered.facts)
    assert recovered.completed_scope is True
    assert recovered.record_set is not None
    assert recovered.next_cursor["finding_count"] == 1
    assert len(recovered_facts) == 1
    assert len(recovered.record_set.records) == 1


def test_all_malformed_snapshots_refuse_completed_scope(tmp_path) -> None:
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"
    first.write_text("{", encoding="utf-8")
    second.write_text("[", encoding="utf-8")

    with pytest.raises(ValueError, match="snapshot is malformed JSON"):
        _source(tmp_path, first.name, second.name).run(
            "failed",
            mode="scope_complete",
        )


def test_unreadable_configured_snapshot_refuses_completed_scope(tmp_path) -> None:
    missing = tmp_path / "missing.json"

    with pytest.raises(RuntimeError, match="snapshot is unreadable") as exc:
        _source(tmp_path, missing.name).run("failed", mode="reconcile")

    assert str(missing) in str(exc.value)
