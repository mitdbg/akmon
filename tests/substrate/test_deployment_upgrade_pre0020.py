"""PACT phase-interval-narrow-split-migration / req.preflight-pre-0020 (#1114).

okg up could not upgrade any pre-0020 database: preflight ran the
effective-attribute recovery classifier, whose queries need the attrs_hash
column that migration 0020_interval_narrow_split creates — and preflight is
phase one while migrate is phase six. These tests pin the repaired contract:

- preflight detects the pending migration from the migration status it
  already reads and RECORDS a deferred classification (schema-tagged, with
  the pending migration id, the reason, and a binding digest) instead of
  invoking the classifier — never a silent skip, never an UndefinedColumn.
- generation_recovery (after migrate) performs the first classification
  against the migrated schema and adopts it as the binding baseline.
- with migrate skipped and the migration still pending, generation_recovery
  fails closed with the deferral visible — the deferred payload is never
  treated as a satisfied classification.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from okg.substrate.deployment_upgrade import (
    CommandResult,
    UpgradeContext,
    UpgradeOptions,
    _DEFERRED_RECOVERY_CLASSIFICATION,
    _DEFERRED_RECOVERY_SCHEMA,
    _NARROW_SPLIT_MIGRATION_ID,
    _deferred_recovery_classification,
    _phase_generation_recovery,
    _phase_preflight,
)


def _ok(payload: dict) -> CommandResult:
    return CommandResult(0, json.dumps(payload), "")


def _migrations_payload(*, pending_0020: bool) -> dict:
    pending = (
        [{"migration_id": _NARROW_SPLIT_MIGRATION_ID, "destructive": False}]
        if pending_0020
        else []
    )
    return {
        "ledger_exists": True,
        "pending": pending,
        "destructive_pending": [],
        "checksum_mismatches": [],
    }


def _classification(kind: str = "no_published_generation") -> dict:
    payload = {
        "schema": "okg.effective_attribute_recovery_classification.v1",
        "classification": kind,
        "deployment": "dep",
        "branch": "default",
    }
    payload["binding_digest"] = "sha256:" + hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return payload


def _ctx(tmp_path: Path, runner, **option_overrides) -> UpgradeContext:
    state_file = tmp_path / "state.json"
    state_file.write_text(json.dumps({"completed": False, "phase_results": []}))
    return UpgradeContext(
        options=UpgradeOptions(
            deployment="dep", skip_backup=True, **option_overrides
        ),
        dsn="postgres://test",
        resolution={"target": {"database": "test", "identity": "isolated"}},
        env={"OKG_DSN": "postgres://test"},
        state_file=state_file,
        backup_dir=tmp_path,
        command_runner=runner,
    )


def test_preflight_defers_classification_when_0020_pending(
    tmp_path: Path,
) -> None:
    calls: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        calls.append(argv)
        if "migrations" in argv:
            return _ok(_migrations_payload(pending_0020=True))
        raise AssertionError(f"unexpected command on a pre-0020 database: {argv}")

    ctx = _ctx(tmp_path, runner)
    result = _phase_preflight(ctx)

    assert result["ok"] is True, result
    assert not any("inspect-effective-attribute-recovery" in argv for argv in calls)
    recorded = result["attribute_recovery_classification"]
    assert recorded["schema"] == _DEFERRED_RECOVERY_SCHEMA
    assert recorded["classification"] == _DEFERRED_RECOVERY_CLASSIFICATION
    assert recorded["pending_migration"] == _NARROW_SPLIT_MIGRATION_ID
    assert recorded["reason"]
    assert str(recorded["binding_digest"]).startswith("sha256:")
    assert ctx.data["attribute_recovery_classification"] == recorded


def test_preflight_still_classifies_when_0020_applied(tmp_path: Path) -> None:
    classification = _classification()

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if "migrations" in argv:
            return _ok(_migrations_payload(pending_0020=False))
        if "inspect-effective-attribute-recovery" in argv:
            return _ok(classification)
        raise AssertionError(argv)

    ctx = _ctx(tmp_path, runner)
    result = _phase_preflight(ctx)
    assert result["ok"] is True, result
    assert result["attribute_recovery_classification"] == classification


def test_generation_recovery_adopts_deferred_classification_after_migrate(
    tmp_path: Path,
) -> None:
    classification = _classification()
    inspect_calls = 0

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        nonlocal inspect_calls
        del env
        if "migrations" in argv:
            return _ok(_migrations_payload(pending_0020=False))
        if "inspect-effective-attribute-recovery" in argv:
            inspect_calls += 1
            return _ok(classification)
        if "prepare-effective-attribute-recovery" in argv:
            return _ok(
                {"prepared": False, "classification": classification}
            )
        raise AssertionError(argv)

    ctx = _ctx(tmp_path, runner)
    ctx.data["attribute_recovery_classification"] = (
        _deferred_recovery_classification("dep")
    )
    result = _phase_generation_recovery(ctx)
    assert result["ok"] is True, result
    assert result["adopted_deferred_preflight_classification"] is True
    # One adoption inspection plus the ordinary binding re-inspection: two
    # real post-migrate reads, never a vacuous self-comparison.
    assert inspect_calls == 2
    assert ctx.data["attribute_recovery_classification"] == classification


def test_generation_recovery_fails_closed_when_migrate_skipped(
    tmp_path: Path,
) -> None:
    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if "migrations" in argv:
            return _ok(_migrations_payload(pending_0020=True))
        raise AssertionError(
            f"must not classify or prepare against a still-wide schema: {argv}"
        )

    ctx = _ctx(tmp_path, runner, skip_migrate=True)
    deferred = _deferred_recovery_classification("dep")
    ctx.data["attribute_recovery_classification"] = deferred
    result = _phase_generation_recovery(ctx)
    assert result["ok"] is False
    assert _NARROW_SPLIT_MIGRATION_ID in result["reason"]
    assert result["deferred_classification"] == deferred
    assert "skip-migrate" in result["fix_hint"]


def test_generation_recovery_routes_late_corrupt_to_corrupt_arm(
    tmp_path: Path,
) -> None:
    """A corrupt state discovered only post-migrate enters the corrupt arm.

    Preflight could not classify (deferred); the first post-migrate
    classification says corrupt_state_recovery — the phase must route into
    the corrupt-state recovery flow (observable: it invokes
    prepare-effective-attribute-recovery with the adopted digest), never
    the ordinary no-op-binding arm.
    """
    corrupt = _classification("corrupt_state_recovery")
    prepare_calls: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if "migrations" in argv:
            return _ok(_migrations_payload(pending_0020=False))
        if "inspect-effective-attribute-recovery" in argv:
            return _ok(corrupt)
        if "prepare-effective-attribute-recovery" in argv:
            prepare_calls.append(argv)
            # Refuse preparation: the test pins the ROUTING, not the whole
            # corrupt-recovery publish flow.
            return CommandResult(1, json.dumps({"ok": False}), "refused")
        raise AssertionError(argv)

    ctx = _ctx(tmp_path, runner)
    ctx.data["attribute_recovery_classification"] = (
        _deferred_recovery_classification("dep")
    )
    result = _phase_generation_recovery(ctx)
    assert result["ok"] is False
    assert result["reason"] == "corrupt-state recovery preparation failed"
    assert len(prepare_calls) == 1
    digest_index = prepare_calls[0].index("--expected-binding-digest") + 1
    assert prepare_calls[0][digest_index] == corrupt["binding_digest"]


def test_generation_recovery_fails_closed_on_missing_pending_key(
    tmp_path: Path,
) -> None:
    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if "migrations" in argv:
            return _ok({"ledger_exists": True})  # no pending key at all
        raise AssertionError(argv)

    ctx = _ctx(tmp_path, runner)
    ctx.data["attribute_recovery_classification"] = (
        _deferred_recovery_classification("dep")
    )
    result = _phase_generation_recovery(ctx)
    assert result["ok"] is False
    assert "pending migration list" in result["reason"]


def test_deferred_payload_digest_is_stable_and_self_describing() -> None:
    payload = _deferred_recovery_classification("dep")
    digest = payload.pop("binding_digest")
    recomputed = "sha256:" + hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    assert digest == recomputed
    # The deferral can never be mistaken for a real classification.
    assert payload["classification"] not in {
        "no_published_generation",
        "no_repair",
        "ordinary_repair",
        "corrupt_state_recovery",
    }
