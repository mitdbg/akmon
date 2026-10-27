"""Behavioral contract for the ``okg deployment prove-live`` collector."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path

import pytest

from okg.substrate.cli.child_argv import running_okg_argv
from okg.substrate.deployment_live_proof import (
    collect_live_deployment_proof,
    validate_live_proof,
)


def _authorities() -> dict[str, dict]:
    readiness = {
        "ok": True,
        "generation": {
            "generation_id": "gen-successor",
            "status": "published",
            "count_integrity": {
                "ok": True,
                "checkpoint": {"node_count": 4, "edge_count": 2},
                "pinned": {"node_count": 4, "edge_count": 2},
            },
        },
        "queues": {"active_queue_depth": 0, "terminal_history_dlq": 3},
        "source_freshness": {
            "ok": True,
            "max_source_age_seconds": 21600,
            "groups": {
                "repo": {
                    "ok": True,
                    "sources": [{
                        "source": "okg-ci.pact",
                        "status": "fresh",
                        "last_reconcile_at": datetime.now(timezone.utc).isoformat(),
                        "age_seconds": 0.0,
                    }],
                },
            },
            "missing": [],
            "stale": [],
        },
        "source_sync_liveness": {
            "ok": True, "missing_producers": [], "stale_producers": [],
        },
        "source_sync_status": {"convergence": {"ok": True, "state": "ready"}},
        "nomos_audit": {"ok": True},
        "nomos_runtime": {"ok": True},
        "doctor": {"ok": True},
        "mcp": {"ok": True, "pinned_generation_id": "gen-successor"},
    }
    return {
        "readiness": readiness,
        "release": {"ok": True, "code_revision": "revision-a"},
        "metrics": {
            "generation_id": "gen-successor",
            "nodes_live_delta": 3,
            "edges_live_delta": 2,
        },
        "dlq": {
            "current_count": 0,
            "historical_count": 3,
            "current_failures": [],
        },
        "query": {
            "ok": True,
            "pinned_generation_id": "gen-successor",
            "node_count": 4,
        },
    }


def _predecessor() -> dict:
    """The record ``okg up``'s preflight writes before it mutates anything."""
    return {
        "schema": "okg.deployment_predecessor_generation.v1",
        "deployment": "okg-ci",
        "status": "published",
        "generation_id": "gen-predecessor",
        "checkpoint_counts": {"node_count": 9, "edge_count": 1},
        "pinned_counts": {"node_count": 2, "edge_count": 1},
    }


_ABSENT = object()


def _receipt(tmp_path: Path, predecessor: object = None) -> Path:
    preflight: dict = {"phase": "preflight", "ok": True}
    if predecessor is None:
        predecessor = _predecessor()
    if predecessor is not _ABSENT:
        preflight["predecessor_generation"] = predecessor
    state = tmp_path / "state.json"
    state.write_text(json.dumps({
        "completed": True,
        "phase_results": [preflight],
    }))
    receipt = tmp_path / "operation.json"
    receipt.write_text(json.dumps({
        "schema": "okg.okg_ci_autoupdate_operation.v2",
        "operation_id": "op-a",
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
    }))
    return receipt


def _runner(authorities: dict[str, dict]):
    ready_calls = 0

    def run(argv):
        nonlocal ready_calls
        if "smoke_probe.py" not in " ".join(argv):
            # Issue #2301: every okg child is the running okg with -P.
            assert list(argv[:4]) == running_okg_argv(), argv
        if "ready" in argv:
            ready_calls += 1
            payload = authorities.get(
                f"readiness_{ready_calls}", authorities["readiness"]
            )
        elif "release" in argv:
            payload = authorities["release"]
        elif "--qa-snapshot" in argv:
            payload = authorities["metrics"]
        elif "current-runtime-dlq" in argv:
            payload = authorities["dlq"]
        elif "smoke_probe.py" in " ".join(argv):
            payload = authorities["query"]
        else:
            raise AssertionError(argv)
        return (0 if payload.get("ok", True) else 1, json.dumps(payload), "")

    return run


def test_live_proof_binds_all_authorities_and_emits_ingestible_evidence(
    tmp_path: Path,
) -> None:
    proof = collect_live_deployment_proof(
        deployment="okg-ci",
        profile="ci",
        operation_receipt=_receipt(tmp_path),
        dsn="postgres://test",
        runner=_runner(_authorities()),
    )
    assert proof["ok"] is True, proof["failures"]
    assert validate_live_proof(proof) == []
    assert {row["kind"] for row in proof["evidence"]} == {
        "graph_report", "published_generation",
    }
    assert all(row["status"] == "pass" for row in proof["evidence"])
    assert proof["queues"]["current_dlq"]["historical_count"] == 3


@pytest.mark.parametrize(
    ("mutation", "failure"),
    [
        (lambda a: a["readiness"]["generation"]["count_integrity"].pop("pinned"), "successor_counts_exact"),
        (lambda a: a["dlq"].update(current_count=1), "current_dlq_empty"),
        (lambda a: a["query"].update(pinned_generation_id="gen-other"), "query_bound"),
        (lambda a: a["readiness"].update(nomos_runtime={"ok": False}), "nomos"),
        (lambda a: a["readiness"].update(doctor={}), "doctor"),
        (lambda a: a["readiness"]["source_freshness"].pop("groups"), "source_watermarks"),
        (
            lambda a: a["readiness"]["source_freshness"]["groups"]["repo"]["sources"][0].pop("last_reconcile_at"),
            "source_watermarks",
        ),
        (
            lambda a: a["readiness"]["source_freshness"]["groups"]["repo"]["sources"][0].update(last_reconcile_at="not-a-timestamp"),
            "source_watermarks",
        ),
        (
            lambda a: a["readiness"]["source_freshness"]["groups"]["repo"]["sources"][0].update(last_reconcile_at="1970-01-01T00:00:00Z"),
            "source_watermarks",
        ),
        (
            lambda a: a["readiness"]["source_freshness"]["groups"]["repo"]["sources"][0].update(last_reconcile_at="2099-01-01T00:00:00Z"),
            "source_watermarks",
        ),
    ],
)
def test_live_proof_rejects_missing_defaulted_and_cross_bound_authority(
    tmp_path: Path,
    mutation,
    failure: str,
) -> None:
    authorities = deepcopy(_authorities())
    mutation(authorities)
    proof = collect_live_deployment_proof(
        deployment="okg-ci",
        profile="ci",
        operation_receipt=_receipt(tmp_path),
        dsn="postgres://test",
        runner=_runner(authorities),
    )
    assert proof["ok"] is False
    assert failure in proof["failures"]
    assert all(row["status"] == "fail" for row in proof["evidence"])


@pytest.mark.parametrize(
    ("predecessor", "failure_code"),
    [
        # An upgrade whose preflight recorded no predecessor at all.
        (_ABSENT, "generations.predecessor_unrecorded"),
        # The preflight could not read the database: no generation id.
        (
            {
                "schema": "okg.deployment_predecessor_generation.v1",
                "deployment": "okg-ci",
                "status": "unavailable",
                "error": {"type": "OperationalError", "message": "refused"},
            },
            None,
        ),
        # A generation id but no pinned counts.
        ({**_predecessor(), "pinned_counts": None}, None),
    ],
)
def test_live_proof_fails_closed_without_a_recorded_predecessor(
    tmp_path: Path, predecessor: object, failure_code: str | None,
) -> None:
    """okg#3112 S3: the predecessor comes from preflight's own record.

    The old source, the effective-attribute recovery classification, went
    with the attribute copies. A missing or unreadable record must fail
    ``predecessor_bound``, never read as a proven predecessor.
    """
    proof = collect_live_deployment_proof(
        deployment="okg-ci",
        profile="ci",
        operation_receipt=_receipt(tmp_path, predecessor),
        dsn="postgres://test",
        runner=_runner(_authorities()),
    )
    assert proof["ok"] is False
    assert proof["checks"]["predecessor_bound"] is False
    assert "predecessor_bound" in proof["failures"]
    if failure_code is not None:
        assert failure_code in proof["failures"]
    assert proof["generations"]["predecessor_phase"] == "preflight"


def test_live_proof_rejects_generation_movement_during_observation(
    tmp_path: Path,
) -> None:
    authorities = _authorities()
    moved = deepcopy(authorities["readiness"])
    moved["generation"]["generation_id"] = "gen-moved"
    authorities["readiness_2"] = moved
    proof = collect_live_deployment_proof(
        deployment="okg-ci",
        profile="ci",
        operation_receipt=_receipt(tmp_path),
        runner=_runner(authorities),
    )
    assert "readiness_stable" in proof["failures"]


def test_live_proof_rejects_far_future_operation_receipt(tmp_path: Path) -> None:
    receipt = _receipt(tmp_path)
    payload = json.loads(receipt.read_text())
    payload["updated_at"] = "2099-01-01T00:00:00Z"
    receipt.write_text(json.dumps(payload))

    proof = collect_live_deployment_proof(
        deployment="okg-ci",
        profile="ci",
        operation_receipt=receipt,
        runner=_runner(_authorities()),
    )

    assert proof["ok"] is False
    assert "operation.future" in proof["failures"]


# ---------------------------------------------------------------------------
# Why `release_bound` came out the way it did (#1962)
# ---------------------------------------------------------------------------
#
# `release_bound` is a bare boolean over the release report's `code_revision`.
# A standalone deployment records no repository revision BY DECLARATION, so
# it read as False in exactly the same way a deployment whose running code
# disagrees with the receipt reads as False. Those are different facts: the
# second must be fixed and the first cannot be. The check stays False for
# both -- a live proof cannot bind a revision that does not exist, and
# reporting True would assert a binding that is not there -- but the proof now
# names the reason.


def _proof_with_release(tmp_path: Path, release: dict) -> dict:
    authorities = _authorities()
    authorities["release"] = release
    return collect_live_deployment_proof(
        deployment="okg-ci",
        profile="ci",
        operation_receipt=_receipt(tmp_path),
        dsn="postgres://test",
        runner=_runner(authorities),
    )


def test_release_binding_names_a_matching_revision(tmp_path: Path) -> None:
    proof = _proof_with_release(
        tmp_path, {"ok": True, "code_revision": "revision-a"},
    )

    binding = proof["release_binding"]
    assert binding["bound"] is True
    assert binding["reason"] == "revision_matches_receipt"
    assert binding["release_kind"] is None
    assert proof["checks"]["release_bound"] is True


def test_release_binding_distinguishes_drift_from_a_standalone_deployment(
    tmp_path: Path,
) -> None:
    """The two Falses must not read alike."""
    drifted = _proof_with_release(
        tmp_path, {"ok": True, "code_revision": "revision-B"},
    )
    standalone = _proof_with_release(
        tmp_path,
        {
            "ok": True,
            "current": {"code_revision": None, "release_kind": "standalone"},
        },
    )

    assert drifted["checks"]["release_bound"] is False
    assert standalone["checks"]["release_bound"] is False
    # Same check, different reported cause.
    assert drifted["release_binding"]["reason"] == "revision_differs_from_receipt"
    assert drifted["release_binding"]["observed_revision"] == "revision-B"
    assert drifted["release_binding"]["expected_revision"] == "revision-a"

    assert (
        standalone["release_binding"]["reason"]
        == "standalone_no_repository_revision"
    )
    assert standalone["release_binding"]["release_kind"] == "standalone"
    assert standalone["release_binding"]["observed_revision"] is None
    # Still a recorded failure: nothing is waived, only explained.
    assert "release_bound" in standalone["failures"]
    assert standalone["ok"] is False


def test_release_binding_separates_a_missing_revision_from_a_declared_one(
    tmp_path: Path,
) -> None:
    """An undeclared report with no revision is not a standalone deployment."""
    proof = _proof_with_release(tmp_path, {"ok": True, "current": {}})

    assert proof["release_binding"]["reason"] == "revision_unavailable"
    assert proof["release_binding"]["release_kind"] is None
    assert proof["checks"]["release_bound"] is False


def test_a_checkout_without_the_query_probe_is_refused_by_name(tmp_path: Path) -> None:
    """The query probe is okg-ci deployment code, absent from the public copy."""
    from okg.substrate import deployment_live_proof as live_proof
    from okg.substrate.errors import SubstrateError

    with pytest.raises(SubstrateError) as raised:
        live_proof.require_query_probe(tmp_path)
    assert raised.value.data == {"path": str(tmp_path / live_proof.QUERY_PROBE_REPO_PATH)}
    assert "query probe" in str(raised.value)

    probe = tmp_path / live_proof.QUERY_PROBE_REPO_PATH
    probe.parent.mkdir(parents=True)
    probe.write_text("", encoding="utf-8")
    assert live_proof.require_query_probe(tmp_path) == probe


def test_prove_live_refuses_before_any_read_when_the_probe_is_absent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from okg.substrate import deployment_live_proof as live_proof
    from okg.substrate.cli.main import main

    monkeypatch.setattr(live_proof, "QUERY_PROBE_REPO_PATH", "deployments/absent/probe.py")

    def no_collection(**_kwargs):
        raise AssertionError("the proof must not run without its query probe")

    monkeypatch.setattr(live_proof, "collect_live_deployment_proof", no_collection)
    receipt = tmp_path / "receipt.json"
    receipt.write_text("{}", encoding="utf-8")

    rc = main([
        "deployment", "prove-live", "fixture",
        "--operation-receipt", str(receipt), "--json",
    ])

    assert rc != 0
    error = json.loads(capsys.readouterr().err)["error"]
    assert error["message"].startswith("deployment prove-live needs the okg-ci query probe")
    assert error["data"]["path"].endswith("deployments/absent/probe.py")
