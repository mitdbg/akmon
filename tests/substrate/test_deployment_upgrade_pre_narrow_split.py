"""okg#1114: the lifecycle's half of the pre-0020 preflight fix.

``preflight`` runs five phases before ``migrate``, so on a database older than
``0020_interval_narrow_split`` the effective-attribute recovery classification
cannot be computed yet -- the payload-parity fingerprint needs a column
``migrate`` has not created.  ``attributes.classify_effective_attribute_recovery``
reports that as ``schema_migration_pending`` instead of crashing or inventing a
verdict.  These are the two things the lifecycle must then do: ACCEPT it in
preflight (so the run reaches ``migrate``) and RECLASSIFY for real afterwards
(so nothing downstream acts on a classification that was never made).

A third consumer is covered here too: ``okg deployment prove-live`` reads the
pre-upgrade classification out of the recorded phase results.  The preflight
placeholder is not one -- it carries no generation anchor at all -- so the
live proof must read the value ``_phase_generation_recovery`` rebound after
migrate, and must REFUSE rather than fall back when there is none.

Hermetic: every ``okg`` invocation is a stub, no database.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from okg.substrate.attributes import (
    _INTERVAL_NARROW_SPLIT_MIGRATION,
    _INTERVAL_PAYLOAD_PARITY_PRE_SPLIT,
)
from okg.substrate.deployment_live_proof import (
    collect_live_deployment_proof,
    validate_live_proof,
)
from okg.substrate.deployment_upgrade import (
    CommandResult,
    UpgradeContext,
    UpgradeOptions,
    _phase_generation_recovery,
    _phase_preflight,
)

# The live-proof collector's own authority stubs, imported rather than copied:
# the arms below are about ONE field of that payload and must not drift from
# the contract fixture the rest of the collector is proven against.
from tests.substrate.test_deployment_live_proof import (
    _authorities,
    _runner as _live_proof_runner,
)


def _result(payload: dict) -> CommandResult:
    return CommandResult(0, json.dumps(payload), "")


# The preflight placeholder, spelled with the SHIPPED classifier's own
# constants instead of hand-typed strings.  Exported, not private, because
# test_effective_attribute_pre_narrow_split.py imports this exact object and
# pins it against what ``classify_effective_attribute_recovery`` really emits
# on a real pre-0020 database -- including that ``payload_match`` is None and
# not False.  Without that pin every arm below would keep passing against a
# literal after the classifier stopped producing it.
MIGRATION_PENDING_CLASSIFICATION: dict[str, Any] = {
    "schema": "okg.effective_attribute_recovery_classification.v1",
    "classification": "schema_migration_pending",
    "deployment": "dep",
    "branch": "default",
    "payload_match": None,
    "payload_parity": {
        "applicable": False,
        "reason": _INTERVAL_PAYLOAD_PARITY_PRE_SPLIT,
        "required_migration": _INTERVAL_NARROW_SPLIT_MIGRATION,
    },
    "binding_digest": "sha256:pending",
}

# The graph anchor fields a live proof needs and the placeholder cannot have.
# Named here so the pin in the postgres suite and the live-proof arms below
# assert the same list.
GRAPH_ANCHOR_FIELDS = ("generation_id", "checkpoint_counts", "pinned_counts")

_MIGRATION_PENDING = MIGRATION_PENDING_CLASSIFICATION

_CLASSIFIED = {
    "schema": "okg.effective_attribute_recovery_classification.v1",
    "classification": "no_repair",
    "deployment": "dep",
    "branch": "default",
    "branch_id": "default",
    "generation_id": "gen:1",
    "ordinal": 7,
    "checkpoint_counts": {"node": 3, "edge": 2},
    "pinned_counts": {"node": 3, "edge": 2},
    "payload_match": True,
    "count_stamp_recovery": None,
    "binding_digest": "sha256:classified",
}


def _recovery_ctx(tmp_path: Path, runner, classification: dict) -> UpgradeContext:
    return UpgradeContext(
        options=UpgradeOptions(deployment="dep", skip_backup=True),
        dsn="postgres://user@host/db",
        resolution={},
        env={},
        state_file=tmp_path / "state.json",
        backup_dir=tmp_path,
        command_runner=runner,
        data={"attribute_recovery_classification": classification},
    )


def test_preflight_accepts_a_pre_narrow_split_database() -> None:
    """The reproduction, at the phase boundary: preflight must not dead-end.

    Before the fix the inspect command itself crashed; the phase then returned
    ``ok: False`` and ``okg up`` stopped five phases short of the migrate that
    installs the missing column.
    """
    commands: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        commands.append(argv)
        if "inspect-effective-attribute-recovery" in argv:
            return _result(_MIGRATION_PENDING)
        return _result({"pending": [], "destructive_pending": []})

    ctx = SimpleNamespace(
        options=SimpleNamespace(
            profile="prod",
            plan_only=False,
            deployment="dep",
            skip_destructive_migrations=False,
        ),
        resolution={},
        dsn="postgres://user@host/db",
        env={},
        command_runner=runner,
        data={},
    )

    result = _phase_preflight(ctx)

    assert result["ok"] is True, result
    recovery = result["attribute_recovery_classification"]
    assert recovery["classification"] == "schema_migration_pending"
    # Accepted, but never mistaken for a clean parity verdict.
    assert recovery["payload_match"] is None
    assert ctx.data["attribute_recovery_classification"] == _MIGRATION_PENDING


def test_generation_recovery_reclassifies_after_migrate(tmp_path: Path) -> None:
    """The deferred classification is taken for real once migrate has run."""
    calls: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        calls.append(argv)
        if "inspect-effective-attribute-recovery" in argv:
            return _result(_CLASSIFIED)
        if "prepare-effective-attribute-recovery" in argv:
            assert argv[argv.index("--expected-binding-digest") + 1] == (
                "sha256:classified"
            )
            assert json.loads(
                argv[argv.index("--expected-classification") + 1]
            ) == _CLASSIFIED
            return _result(
                {"ok": True, "prepared": False, "classification": _CLASSIFIED}
            )
        raise AssertionError(argv)

    ctx = _recovery_ctx(tmp_path, runner, _MIGRATION_PENDING)

    result = _phase_generation_recovery(ctx)

    assert result["ok"] is True, result
    assert result["recovered"] is False
    # The preflight placeholder is reported, not silently swallowed, and the
    # phase proceeds on the post-migrate classification.
    assert result["deferred_preflight_classification"] == _MIGRATION_PENDING
    assert result["classification"] == _CLASSIFIED
    assert ctx.data["attribute_recovery_classification"] == _CLASSIFIED


def test_generation_recovery_refuses_when_still_pre_narrow_split(
    tmp_path: Path,
) -> None:
    """If migrate did not converge the schema, refuse -- do not proceed blind."""

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if "inspect-effective-attribute-recovery" in argv:
            return _result(_MIGRATION_PENDING)
        raise AssertionError(argv)

    ctx = _recovery_ctx(tmp_path, runner, _MIGRATION_PENDING)

    result = _phase_generation_recovery(ctx)

    assert result["ok"] is False
    assert result["reason"] == (
        "the migrate phase did not install the attribute payload columns "
        "the recovery classification needs"
    )
    assert result["observed"] == _MIGRATION_PENDING


def test_generation_recovery_refuses_an_unbound_post_migrate_classification(
    tmp_path: Path,
) -> None:
    """A reclassification without a sha256 binding is not a classification."""
    unbound = {**_CLASSIFIED, "binding_digest": "not-a-digest"}

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        if "inspect-effective-attribute-recovery" in argv:
            return _result(unbound)
        raise AssertionError(argv)

    ctx = _recovery_ctx(tmp_path, runner, _MIGRATION_PENDING)

    result = _phase_generation_recovery(ctx)

    assert result["ok"] is False
    assert result["reason"] == (
        "post-migrate attribute recovery classification is unbound"
    )


def test_ordinary_classification_is_untouched_by_the_deferral(
    tmp_path: Path,
) -> None:
    """The pre-existing path keeps its anchor check and reports no deferral."""
    calls: list[list[str]] = []

    def runner(argv: list[str], env: dict[str, str]) -> CommandResult:
        del env
        calls.append(argv)
        if "inspect-effective-attribute-recovery" in argv:
            return _result(_CLASSIFIED)
        if "prepare-effective-attribute-recovery" in argv:
            return _result(
                {"ok": True, "prepared": False, "classification": _CLASSIFIED}
            )
        raise AssertionError(argv)

    ctx = _recovery_ctx(tmp_path, runner, _CLASSIFIED)

    result = _phase_generation_recovery(ctx)

    assert result["ok"] is True, result
    assert "deferred_preflight_classification" not in result
    # One inspect + one prepare: the deferral adds no call on this path.
    assert len(calls) == 2


# ---------------------------------------------------------------------------
# The live proof's half.
#
# ``okg deployment prove-live`` reconstructs the pre-upgrade generation from
# the recorded phase results.  It read the PREFLIGHT row, which on a pre-0020
# deployment holds the placeholder above: no generation_id, no checkpoint or
# pinned counts.  Six of the eight fields it needs are simply absent, so
# ``predecessor_bound`` and ``successor_advanced`` both went False on exactly
# the deployments this fix unblocked -- okg-ci among them, the one deployment
# wired for prove-live.  The classification bound after migrate lives on the
# generation_recovery row instead, and that is what must be read.
# ---------------------------------------------------------------------------

_PREFLIGHT_CLASSIFIED = {
    "schema": "okg.effective_attribute_recovery_classification.v1",
    "classification": "no_repair",
    "deployment": "okg-ci",
    "branch": "default",
    "generation_id": "gen-predecessor",
    "checkpoint_counts": {"node": 9, "edge": 1},
    "pinned_counts": {"node": 9, "edge": 1},
    "payload_match": True,
    "binding_digest": "sha256:preflight",
}


def _live_proof_receipt(tmp_path: Path, phase_results: list[dict]) -> Path:
    """An operation receipt whose state file carries exactly these phases."""
    state = tmp_path / "state.json"
    state.write_text(
        json.dumps({"completed": True, "phase_results": phase_results}),
        encoding="utf-8",
    )
    receipt = tmp_path / "operation.json"
    receipt.write_text(
        json.dumps(
            {
                "schema": "okg.okg_ci_autoupdate_operation.v2",
                "operation_id": "op-1114",
                "status": "succeeded",
                "exit_code": 0,
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "state_file": str(state),
                "candidate_revision": "revision-a",
                "plan_digest": "sha256:plan",
                "lifecycle_contract_digest": "sha256:contract",
                "worker_state": "active",
                "baseline_disposition": "pending",
                "operation_identity_digest": "sha256:operation",
            }
        ),
        encoding="utf-8",
    )
    return receipt


def _live_proof(tmp_path: Path, phase_results: list[dict]) -> dict:
    return collect_live_deployment_proof(
        deployment="okg-ci",
        profile="ci",
        operation_receipt=_live_proof_receipt(tmp_path, phase_results),
        dsn="postgres://test",
        runner=_live_proof_runner(_authorities()),
    )


def test_live_proof_still_reads_preflight_when_preflight_classified(
    tmp_path: Path,
) -> None:
    """Positive control: the post-0020 path is untouched by the #1114 read.

    Without this arm the two below could pass against a collector that always
    reached for the generation_recovery row.
    """
    proof = _live_proof(
        tmp_path,
        [{"phase": "preflight", "ok": True,
          "attribute_recovery_classification": _PREFLIGHT_CLASSIFIED}],
    )

    assert proof["ok"] is True, proof["failures"]
    assert validate_live_proof(proof) == []
    assert proof["generations"]["predecessor_phase"] == "preflight"
    assert proof["generations"]["predecessor"] == _PREFLIGHT_CLASSIFIED
    assert proof["checks"]["predecessor_bound"] is True
    assert proof["checks"]["successor_advanced"] is True


def test_live_proof_reads_the_classification_rebound_after_migrate(
    tmp_path: Path,
) -> None:
    """The #1114 shape: preflight deferred, generation_recovery classified."""
    # Non-vacuity: the placeholder really does lack every field the proof
    # needs, so the assertions below cannot be satisfied by reading it.
    for field in GRAPH_ANCHOR_FIELDS:
        assert field not in MIGRATION_PENDING_CLASSIFICATION

    proof = _live_proof(
        tmp_path,
        [
            {"phase": "preflight", "ok": True,
             "attribute_recovery_classification": MIGRATION_PENDING_CLASSIFICATION},
            {"phase": "generation_recovery", "ok": True, "recovered": False,
             "deferred_preflight_classification": MIGRATION_PENDING_CLASSIFICATION,
             "classification": _PREFLIGHT_CLASSIFIED},
        ],
    )

    assert proof["ok"] is True, proof["failures"]
    assert validate_live_proof(proof) == []
    assert proof["generations"]["predecessor_phase"] == "generation_recovery"
    assert proof["generations"]["predecessor"] == _PREFLIGHT_CLASSIFIED
    assert proof["checks"]["predecessor_bound"] is True
    assert proof["checks"]["successor_advanced"] is True


def test_live_proof_refuses_a_placeholder_with_no_reclassification(
    tmp_path: Path,
) -> None:
    """No fallback: an unrebound placeholder is a named failure, not a pass.

    This is also the BEFORE picture of the arm above: with the
    generation_recovery row removed, the two checks that read the predecessor
    both go False -- which is what every pre-0020 deployment's prove-live run
    reported while the collector read only the preflight row.
    """
    proof = _live_proof(
        tmp_path,
        [{"phase": "preflight", "ok": True,
          "attribute_recovery_classification": MIGRATION_PENDING_CLASSIFICATION}],
    )

    assert proof["ok"] is False
    assert validate_live_proof(proof) == []
    assert "generations.predecessor_unrebound" in proof["failures"]
    assert proof["checks"]["predecessor_bound"] is False
    assert proof["checks"]["successor_advanced"] is False
    # Reported, never swallowed: the operator sees the placeholder that was
    # never resolved, and which phase it came from.
    assert proof["generations"]["predecessor"] == MIGRATION_PENDING_CLASSIFICATION
    assert proof["generations"]["predecessor_phase"] == "preflight_migration_pending"


def test_live_proof_refuses_a_reclassification_that_is_still_pending(
    tmp_path: Path,
) -> None:
    """A generation_recovery row that never got past the placeholder refuses.

    ``_phase_generation_recovery`` fails the upgrade in this state, so it
    should not be reachable -- which is exactly why the proof must not treat
    the row's mere PRESENCE as the thing it was looking for.
    """
    proof = _live_proof(
        tmp_path,
        [
            {"phase": "preflight", "ok": True,
             "attribute_recovery_classification": MIGRATION_PENDING_CLASSIFICATION},
            {"phase": "generation_recovery", "ok": False,
             "classification": MIGRATION_PENDING_CLASSIFICATION},
        ],
    )

    assert proof["ok"] is False
    assert "generations.predecessor_unrebound" in proof["failures"]
    assert proof["checks"]["predecessor_bound"] is False


def test_live_proof_takes_the_last_generation_recovery_row(
    tmp_path: Path,
) -> None:
    """A resumed upgrade records the phase twice; the completed run is last."""
    superseded = {**_PREFLIGHT_CLASSIFIED, "generation_id": "gen-abandoned"}
    proof = _live_proof(
        tmp_path,
        [
            {"phase": "preflight", "ok": True,
             "attribute_recovery_classification": MIGRATION_PENDING_CLASSIFICATION},
            {"phase": "generation_recovery", "ok": False,
             "classification": superseded},
            {"phase": "generation_recovery", "ok": True,
             "classification": _PREFLIGHT_CLASSIFIED},
        ],
    )

    assert proof["ok"] is True, proof["failures"]
    assert proof["generations"]["predecessor"]["generation_id"] == "gen-predecessor"
