from __future__ import annotations

import json
import os
from pathlib import Path

import yaml

from okg.substrate.cli import main as cli
from okg.substrate.exchange.refinement import build_refinement_state_from_trace
from okg.substrate.formal_assurance import (
    FormalCoverageRequirement,
    FormalCoverageWaiver,
    FormalModelRegistration,
    evaluate_formal_assurance,
)


def test_formal_assurance_registry_reports_poros_model(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_formal_root(tmp_path, stale=False)
    monkeypatch.setenv("OKG_FORMAL_ASSURANCE_ROOT", str(tmp_path))

    rc = cli.main(["doctor", "--check", "formal-assurance", "--json"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    model = payload["models"][0]
    assert model["model_id"] == "poros-end-to-end-exchange"
    assert model["model_path"] == "formal/poros/PorosExchange.tla"
    assert model["checker_command"] == "make verify-poros"
    assert model["assurance_tier"] == "tier2-protocol-security"
    assert model["latest_status"] == "pass"
    assert model["counterexample_artifact"] == (
        ".scratch/formal/poros-counterexample/counterexample.txt"
    )
    assert model["assumptions"]
    assert model["coverage_status"]["status"] == "complete"
    assert model["coverage_status"]["missing_count"] == 0
    assert {
        coverage["kind"]
        for coverage in model["coverage_status"]["coverage"]
    } == {"intentional_counterexample", "non_vacuity_refinement"}
    assert payload["warnings"] == []


def test_formal_assurance_doctor_fails_for_missing_negative_coverage(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_formal_root(
        tmp_path,
        stale=False,
        omit_artifacts={
            ".scratch/formal/poros-counterexample/counterexample.txt"
        },
    )
    monkeypatch.setenv("OKG_FORMAL_ASSURANCE_ROOT", str(tmp_path))

    rc = cli.main(["doctor", "--check", "formal-assurance", "--json"])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    model = payload["models"][0]
    assert model["latest_status"] == "weak"
    assert payload["errors"][0]["code"] == "formal_assurance_weak"
    coverage = model["coverage_status"]
    assert coverage["status"] == "weak"
    assert coverage["missing_count"] == 1
    assert any(
        item["kind"] == "intentional_counterexample"
        and item["status"] == "missing"
        for item in coverage["coverage"]
    )


def test_formal_assurance_doctor_fails_for_stale_negative_coverage(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_formal_root(tmp_path, stale=False)
    os.utime(
        tmp_path / ".scratch/formal/poros-counterexample/counterexample.txt",
        (500, 500),
    )
    monkeypatch.setenv("OKG_FORMAL_ASSURANCE_ROOT", str(tmp_path))

    rc = cli.main(["doctor", "--check", "formal-assurance", "--json"])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    coverage = payload["models"][0]["coverage_status"]
    assert payload["models"][0]["latest_status"] == "weak"
    assert coverage["stale_count"] == 1
    assert any(
        item["kind"] == "intentional_counterexample"
        and item["status"] == "stale"
        for item in coverage["coverage"]
    )


def test_formal_assurance_doctor_fails_for_invalid_counterexample_coverage(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_formal_root(
        tmp_path,
        stale=False,
        artifact_contents={
            ".scratch/formal/poros-counterexample/counterexample.txt": (
                "TLC completed successfully without a violation\n"
            )
        },
    )
    monkeypatch.setenv("OKG_FORMAL_ASSURANCE_ROOT", str(tmp_path))

    rc = cli.main(["doctor", "--check", "formal-assurance", "--json"])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    coverage = payload["models"][0]["coverage_status"]
    assert coverage["invalid_count"] == 1
    assert any(
        item["kind"] == "intentional_counterexample"
        and item["status"] == "invalid"
        for item in coverage["coverage"]
    )


def test_formal_assurance_doctor_fails_for_vacuous_refinement_coverage(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_formal_root(
        tmp_path,
        stale=False,
        artifact_contents={
            ".scratch/formal/poros-refinement/refinement-state.json": "{}\n"
        },
    )
    monkeypatch.setenv("OKG_FORMAL_ASSURANCE_ROOT", str(tmp_path))

    rc = cli.main(["doctor", "--check", "formal-assurance", "--json"])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    coverage = payload["models"][0]["coverage_status"]
    assert coverage["invalid_count"] == 1
    assert any(
        item["kind"] == "non_vacuity_refinement"
        and item["status"] == "invalid"
        for item in coverage["coverage"]
    )


def test_formal_assurance_doctor_fails_for_evidence_free_refinement_coverage(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_formal_root(
        tmp_path,
        stale=False,
        artifact_contents={
            ".scratch/formal/poros-refinement/refinement-state.json": (
                json.dumps(_evidence_free_refinement_state()) + "\n"
            )
        },
    )
    monkeypatch.setenv("OKG_FORMAL_ASSURANCE_ROOT", str(tmp_path))

    rc = cli.main(["doctor", "--check", "formal-assurance", "--json"])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    coverage = payload["models"][0]["coverage_status"]
    assert coverage["invalid_count"] == 1
    assert any(
        item["kind"] == "non_vacuity_refinement"
        and item["status"] == "invalid"
        and "runtime outbox evidence" in item["status_reason"]
        for item in coverage["coverage"]
    )


def test_formal_assurance_doctor_rejects_mismatched_refinement_evidence(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_formal_root(
        tmp_path,
        stale=False,
        artifact_contents={
            ".scratch/formal/poros-refinement/refinement-state.json": (
                json.dumps(_mismatched_refinement_state()) + "\n"
            )
        },
    )
    monkeypatch.setenv("OKG_FORMAL_ASSURANCE_ROOT", str(tmp_path))

    rc = cli.main(["doctor", "--check", "formal-assurance", "--json"])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    coverage = payload["models"][0]["coverage_status"]
    assert coverage["invalid_count"] == 1
    assert any(
        item["kind"] == "non_vacuity_refinement"
        and item["status"] == "invalid"
        and "no single admitted, blocked, or published message" in (
            item["status_reason"]
        )
        for item in coverage["coverage"]
    )


def test_formal_assurance_reports_explicit_negative_coverage_waiver(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _write_formal_root(
        tmp_path,
        stale=False,
        service_trace=True,
        omit_artifacts={
            ".scratch/formal/poros-counterexample/counterexample.txt"
        },
    )
    monkeypatch.setenv("OKG_FORMAL_ASSURANCE_REQUIRE_SERVICE_TRACE", "1")
    registration = FormalModelRegistration(
        model_id="poros-waived-negative-coverage",
        capability="poros-protocol",
        model_path="formal/poros/PorosExchange.tla",
        checker_command="make verify-poros",
        assurance_tier="tier2-protocol-security",
        assumptions=("operator accepted temporary negative coverage waiver",),
        required_artifacts=(
            ".scratch/formal/poros/tlc.stdout.log",
            ".scratch/formal/poros-counterexample/counterexample.txt",
            ".scratch/formal/poros-refinement/refinement-state.json",
        ),
        counterexample_artifact=(
            ".scratch/formal/poros-counterexample/counterexample.txt"
        ),
        service_trace_artifacts=(
            ".scratch/formal/poros-db-refinement/runtime-trace.json",
            ".scratch/formal/poros-db-refinement/refinement-state.json",
        ),
        coverage_requirements=(
            FormalCoverageRequirement(
                coverage_id="poros-counterexample-waived",
                kind="intentional_counterexample",
                description="temporary waiver for the negative variant",
                artifact=".scratch/formal/poros-counterexample/counterexample.txt",
                waiver=FormalCoverageWaiver(
                    reason="tracked by follow-up formal model issue",
                    owner="formal-assurance-maintainer",
                ),
            ),
            FormalCoverageRequirement(
                coverage_id="poros-refinement-non-vacuity",
                kind="non_vacuity_refinement",
                description="runtime trace still exercises the model",
                artifact=".scratch/formal/poros-refinement/refinement-state.json",
            ),
        ),
    )

    payload = evaluate_formal_assurance(
        tmp_path,
        registrations=(registration,),
    )

    assert payload["ok"] is True
    assert payload["status"] == "WARN"
    model = payload["models"][0]
    assert model["latest_status"] == "waived"
    assert model["status_reason"] == (
        "formal assurance negative coverage is explicitly waived"
    )
    assert model["coverage_status"]["status"] == "waived"
    assert model["coverage_status"]["waived_count"] == 1
    assert payload["warnings"] == [
        {
            "code": "formal_assurance_coverage_waived",
            "model_id": "poros-waived-negative-coverage",
            "coverage_id": "poros-counterexample-waived",
            "message": (
                "coverage waived by formal-assurance-maintainer: "
                "tracked by follow-up formal model issue"
            ),
        }
    ]


def test_formal_assurance_doctor_fails_for_stale_evidence(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_formal_root(tmp_path, stale=True)
    monkeypatch.setenv("OKG_FORMAL_ASSURANCE_ROOT", str(tmp_path))

    rc = cli.main(["doctor", "--check", "formal-assurance", "--json"])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert payload["models"][0]["latest_status"] == "stale"
    assert payload["errors"][0]["code"] == "formal_assurance_stale"


def test_formal_assurance_doctor_requires_service_trace_when_enabled(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_formal_root(tmp_path, stale=False)
    monkeypatch.setenv("OKG_FORMAL_ASSURANCE_ROOT", str(tmp_path))
    monkeypatch.setenv("OKG_FORMAL_ASSURANCE_REQUIRE_SERVICE_TRACE", "1")

    rc = cli.main(["doctor", "--check", "formal-assurance", "--json"])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["ok"] is False
    assert payload["models"][0]["latest_status"] == "missing"
    assert "service-trace refinement artifacts" in payload["errors"][0]["message"]


def test_formal_assurance_doctor_fails_for_stale_service_trace(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_formal_root(tmp_path, stale=False, service_trace=True)
    for rel in [
        ".scratch/formal/poros-db-refinement/runtime-trace.json",
        ".scratch/formal/poros-db-refinement/refinement-state.json",
    ]:
        os.utime(tmp_path / rel, (1500, 1500))
    monkeypatch.setenv("OKG_FORMAL_ASSURANCE_ROOT", str(tmp_path))
    monkeypatch.setenv("OKG_FORMAL_ASSURANCE_REQUIRE_SERVICE_TRACE", "1")

    rc = cli.main(["doctor", "--check", "formal-assurance", "--json"])

    assert rc == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["models"][0]["latest_status"] == "stale"
    assert "service-trace refinement artifacts are older" in (
        payload["errors"][0]["message"]
    )


def test_formal_assurance_doctor_passes_with_fresh_service_trace(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    _write_formal_root(tmp_path, stale=False, service_trace=True)
    monkeypatch.setenv("OKG_FORMAL_ASSURANCE_ROOT", str(tmp_path))
    monkeypatch.setenv("OKG_FORMAL_ASSURANCE_REQUIRE_SERVICE_TRACE", "1")

    rc = cli.main(["doctor", "--check", "formal-assurance", "--json"])

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["models"][0]["latest_status"] == "pass"
    assert any(
        artifact["path"]
        == ".scratch/formal/poros-db-refinement/refinement-state.json"
        for artifact in payload["models"][0]["artifacts"]
    )


def _write_formal_root(
    root: Path,
    *,
    stale: bool,
    service_trace: bool = False,
    omit_artifacts: set[str] | None = None,
    artifact_contents: dict[str, str] | None = None,
) -> None:
    omit_artifacts = omit_artifacts or set()
    artifact_contents = artifact_contents or {}
    inputs = [
        "formal/poros/PorosExchange.tla",
        "formal/poros/PorosExchange.cfg",
        "formal/poros/PorosExchangeCounterexample.tla",
        "formal/poros/PorosExchangeCounterexample.cfg",
        "formal/poros/fixtures/two-okg-trace.json",
        "scripts/check_formal_poros.py",
        "scripts/check_poros_refinement_fixture.py",
        "scripts/check_poros_refinement_from_db.py",
        "src/okg/substrate/exchange/refinement.py",
    ]
    artifacts = [
        ".scratch/formal/poros/tlc.stdout.log",
        ".scratch/formal/poros-counterexample/counterexample.txt",
        ".scratch/formal/poros-refinement/refinement-state.json",
    ]
    for rel in inputs:
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("---- MODULE Fixture ----\n====\n", encoding="utf-8")
        os.utime(path, (1000, 1000))
    for rel in artifacts:
        if rel in omit_artifacts:
            continue
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        text = artifact_contents.get(rel, _formal_artifact_text(rel))
        path.write_text(text, encoding="utf-8")
        os.utime(path, (2000, 2000))
    # The normal-model gate consumes the strict TLC formal report, not the raw
    # stdout log (issue #684): emit a freshly-proven, non-vacuous report so the
    # pass-path fixtures attest positively. Callers override via artifact_contents
    # or suppress via omit_artifacts to exercise the failing branches.
    report_rel = ".scratch/formal/poros/formal-report.yaml"
    if report_rel not in omit_artifacts:
        report_path = root / report_rel
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_text = artifact_contents.get(
            report_rel,
            yaml.safe_dump(
                {
                    "results": [
                        {
                            "property_id": "Safety",
                            "status": "pass",
                            "proof_status": "freshly_proven",
                            "timed_out": False,
                            "distinct_states": 641,
                        }
                    ]
                },
                sort_keys=False,
            ),
        )
        report_path.write_text(report_text, encoding="utf-8")
        os.utime(report_path, (2000, 2000))
    if service_trace:
        for rel in [
            ".scratch/formal/poros-db-refinement/runtime-trace.json",
            ".scratch/formal/poros-db-refinement/refinement-state.json",
        ]:
            path = root / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("{}\n", encoding="utf-8")
            os.utime(path, (2500, 2500))
    if stale:
        path = root / "formal/poros/PorosExchange.tla"
        os.utime(path, (3000, 3000))


def _formal_artifact_text(rel: str) -> str:
    if rel == ".scratch/formal/poros-counterexample/counterexample.txt":
        return "Error: Invariant BrokenReceiptImpliesPublication is violated.\n"
    if rel == ".scratch/formal/poros-refinement/refinement-state.json":
        return json.dumps(
            build_refinement_state_from_trace(_valid_poros_trace()),
            sort_keys=True,
        ) + "\n"
    if rel.endswith(".json"):
        return "{}\n"
    return "TLC completed\n"


def _evidence_free_refinement_state() -> dict[str, object]:
    return {
        "model": "formal/poros/PorosExchange.tla",
        "messages": ["message-1"],
        "outbox": ["message-1"],
        "inbox": [],
        "verified": [],
        "replayCache": [],
        "receipts": {},
        "deliveryAttempts": {},
        "nomosDecisions": {},
        "promotionDecisions": {},
        "admissions": [],
        "blockedState": ["message-1"],
        "publishedMessages": [],
        "liveRows": [],
        "rowMessage": {},
        "rowSenderGeneration": {},
        "rowReceiverGeneration": {},
        "rowAdmission": {},
        "rowBodyHash": {},
        "messageReceiverGeneration": {},
        "publicProofMessages": [],
        "outboxEvidence": {},
        "inboxEvidence": {},
        "admissionRows": [],
        "generationRows": [],
        "promotionDecisionRows": [],
        "liveRowEvidence": [],
        "proofRows": [],
        "unsupported": [],
    }


def _mismatched_refinement_state() -> dict[str, object]:
    state = _evidence_free_refinement_state()
    state["messages"] = ["blocked-msg", "evidence-msg"]
    state["outbox"] = ["blocked-msg"]
    state["blockedState"] = ["blocked-msg"]
    state["outboxEvidence"] = {
        "evidence-msg": {
            "body_sha256": "sha256:body-1",
            "detached": False,
            "object_hash_valid": False,
            "sender_hint": False,
            "receiver_authorized": True,
        }
    }
    state["inboxEvidence"] = {
        "evidence-msg": {
            "body_sha256": "sha256:body-1",
        }
    }
    state["admissionRows"] = [
        {
            "message_id": "evidence-msg",
            "admission_id": "admission-1",
            "status": "accepted",
            "body_sha256": "sha256:body-1",
            "source": "poros:okg:lab-a",
            "run_id": "run-1",
        }
    ]
    return state


def _valid_poros_trace() -> dict[str, object]:
    return {
        "outbox": [
            {
                "message_id": "poros-msg-1",
                "sender": "okg:lab-a",
                "recipient": "okg:mit",
                "sender_generation_id": "sender-gen-7",
                "body_sha256": "sha256:body-1",
                "graph_id": "poros:graph:context-1",
                "signing_key_id": "lab-a-sign-1",
                "detached": False,
                "object_hash_valid": False,
                "sender_hint": False,
                "receiver_authorized": True,
                "classification": "public",
            }
        ],
        "inbox": [
            {
                "message_id": "poros-msg-1",
                "status": "accepted_for_admission",
                "body_sha256": "sha256:body-1",
                "trusted": True,
                "signature_valid": True,
                "recipient_valid": True,
                "body_hash_valid": True,
                "fresh": True,
                "detached_object_valid": True,
            }
        ],
        "receipts": [
            {
                "message_id": "poros-msg-1",
                "status": "accepted_for_admission",
                "received_body_sha256": "sha256:body-1",
                "admission_id": "admission-1",
            }
        ],
        "nomos_decisions": [
            {
                "message_id": "poros-msg-1",
                "decision_id": "nomos:decision:poros-msg-1",
                "effect": "allow",
                "input_hash": "sha256:nomos-input-1",
                "policy_bundle_id": "nomos:bundle:poros-demo",
                "policy_fingerprint": "sha256:policy-1",
                "uses_receiver_policy": True,
                "classification": "public",
                "obligations": [],
                "reason_code": "allow_public_context",
            }
        ],
        "admissions": [
            {
                "message_id": "poros-msg-1",
                "admission_id": "admission-1",
                "status": "accepted",
                "body_sha256": "sha256:body-1",
                "source": "poros:okg:lab-a",
                "run_id": "run-1",
            }
        ],
        "generations": [
            {
                "message_id": "poros-msg-1",
                "generation_id": "receiver-gen-3",
                "status": "published",
                "admission_id": "admission-1",
            }
        ],
        "live_rows": [
            {
                "row_id": "node:poros-demo:context:poros-msg-1",
                "row_kind": "node",
                "message_id": "poros-msg-1",
                "admission_id": "admission-1",
                "sender_generation_id": "sender-gen-7",
                "receiver_generation_id": "receiver-gen-3",
                "body_sha256": "sha256:body-1",
            }
        ],
        "proofs": [
            {
                "message_id": "poros-msg-1",
                "visibility": "public",
                "classification": "public",
                "redacted": False,
                "declassified": False,
            }
        ],
        "unsupported": [],
    }
