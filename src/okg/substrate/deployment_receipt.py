"""Authoritative terminal receipts for one okg-ci deployment operation."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import tempfile
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping

from okg.substrate.errors import SubstrateError
from okg.substrate.exchange.signing import sign_document, signature_kid, verify_signed_document
from okg.substrate.deployment_attestation import verify_historical_admission_receipt

SCHEMA = "okg.deployment_terminal_receipt/v1"
TERMINAL_STATUSES = {"succeeded", "failed", "blocked", "recovery_required"}
_REQUIRED = {
    "schema", "idempotency_key", "status", "github", "trusted_commit", "plan_digest",
    "artifact_digest", "previous_artifact_digest", "definition_digest", "staging_generation", "production_generation",
    "attestations", "backup", "fence", "service_activation", "readiness", "query_evidence",
    "recovery_dispositions", "admission_policy_digest", "history_policy_digest",
    "requested_at", "started_at", "terminal_at",
}


def _error(message: str, **data: object) -> SubstrateError:
    return SubstrateError(message, data=data)


def _canonical(value: Mapping[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def receipt_digest(receipt: Mapping[str, Any]) -> str:
    unsigned = dict(receipt)
    unsigned.pop("receipt_digest", None)
    unsigned.pop("exact_retry", None)
    unsigned.pop("signature", None)
    return "sha256:" + hashlib.sha256(_canonical(unsigned)).hexdigest()


def seal_terminal_receipt(candidate: Mapping[str, Any], private_key: bytes | str, *, kid: str) -> dict[str, Any]:
    row = dict(candidate)
    row["signing_key_id"] = kid
    row["receipt_digest"] = receipt_digest(row)
    return sign_document(row, private_key, kid=kid)


def verify_terminal_receipt(
    receipt: Mapping[str, Any], trust_keys: Mapping[str, str],
    history_policy: Mapping[str, Any],
) -> dict[str, Any]:
    missing = sorted(_REQUIRED - receipt.keys())
    if missing:
        raise _error("terminal deployment receipt is missing required evidence", missing=missing)
    if receipt.get("schema") != SCHEMA or receipt.get("status") not in TERMINAL_STATUSES:
        raise _error("terminal deployment receipt has invalid schema or status")
    if receipt.get("receipt_digest") != receipt_digest(receipt):
        raise _error("terminal deployment receipt digest changed")
    kid = signature_kid(dict(receipt))
    public_key = trust_keys.get(kid)
    if not isinstance(public_key, str):
        raise _error("terminal deployment receipt signing key is not trusted", kid=kid)
    verify_signed_document(dict(receipt), public_key)
    for field in ("idempotency_key", "trusted_commit", "plan_digest", "artifact_digest",
                  "definition_digest", "admission_policy_digest", "history_policy_digest"):
        if not isinstance(receipt.get(field), str) or not receipt[field]:
            raise _error("terminal deployment receipt identity is incomplete", field=field)
    for field in ("plan_digest", "artifact_digest", "definition_digest", "admission_policy_digest", "history_policy_digest"):
        if re.fullmatch(r"sha256:[0-9a-f]{64}", str(receipt[field])) is None:
            raise _error("terminal deployment receipt digest is invalid", field=field)
    previous = receipt.get("previous_artifact_digest")
    if previous is not None and re.fullmatch(r"sha256:[0-9a-f]{64}", str(previous)) is None:
        raise _error("terminal deployment receipt previous artifact digest is invalid")
    github = receipt.get("github")
    if not isinstance(github, Mapping) or any(github.get(key) in {None, ""} for key in ("run_id", "run_attempt", "repository", "ref")):
        raise _error("terminal deployment receipt GitHub identity is incomplete")
    for field in ("attestations", "backup", "fence", "service_activation", "readiness",
                  "query_evidence", "recovery_dispositions"):
        if not isinstance(receipt.get(field), Mapping) or not receipt[field]:
            raise _error("terminal deployment receipt evidence is malformed", field=field)
    timestamps = []
    for field in ("requested_at", "started_at", "terminal_at"):
        if not isinstance(receipt.get(field), str) or not receipt[field]:
            raise _error("terminal deployment receipt timestamp is incomplete", field=field)
        try:
            parsed = datetime.fromisoformat(str(receipt[field]))
        except ValueError as exc:
            raise _error("terminal deployment receipt timestamp is invalid", field=field) from exc
        if parsed.tzinfo is None:
            raise _error("terminal deployment receipt timestamp has no timezone", field=field)
        timestamps.append(parsed)
    if timestamps != sorted(timestamps):
        raise _error("terminal deployment receipt timestamps are out of order")
    status = receipt["status"]
    staging = receipt["staging_generation"]
    production = receipt["production_generation"]
    attestation_disposition = receipt["attestations"]
    failure = receipt.get("failure")
    failure_phase = failure.get("phase") if isinstance(failure, Mapping) else None
    requires_attestation = status == "succeeded" or failure_phase in {
        "post_attestation", "production", "recovery"
    }
    if requires_attestation:
        if attestation_disposition.get("status") != "verified" or not isinstance(
            attestation_disposition.get("admission_receipt"), Mapping
        ):
            raise _error("terminal deployment receipt lacks required staging admission")
        admission_receipt = attestation_disposition["admission_receipt"]
        verify_historical_admission_receipt(admission_receipt, history_policy)
        signed_attestation = admission_receipt["signed_attestation"]
        expected_attestation = {
            "artifact_digest": receipt["artifact_digest"],
            "definition_digest": receipt["definition_digest"],
            "evidence_generation_id": staging,
            "repository": github["repository"], "ref": github["ref"],
            "workflow_run_id": str(github["run_id"]),
            "workflow_run_attempt": github["run_attempt"],
            "commit_sha": receipt["trusted_commit"],
            "promotion_id": receipt["idempotency_key"],
        }
        if any(signed_attestation.get(key) != value for key, value in expected_attestation.items()):
            raise _error("terminal deployment receipt staging attestation lineage contradicts receipt")
        if admission_receipt.get("admission_policy_digest") != receipt["admission_policy_digest"] or admission_receipt.get("history_policy_digest") != receipt["history_policy_digest"]:
            raise _error("terminal deployment receipt trust policy lineage contradicts receipt")
    elif attestation_disposition.get("status") != "not_reached":
        raise _error("pre-attestation terminal receipt has contradictory attestation disposition")
    if status == "succeeded":
        if not isinstance(staging, str) or not isinstance(production, str) or staging == production:
            raise _error("successful deployment must bind distinct staging and production generations")
        if receipt["readiness"].get("status") != "ready" or receipt["query_evidence"].get("status") != "passed":
            raise _error("successful deployment lacks readiness or query proof")
        if previous is None:
            raise _error("successful deployment does not identify the previous artifact")
        if receipt["backup"].get("status") != "verified" or receipt["fence"].get("status") != "released" or receipt["service_activation"].get("status") != "active":
            raise _error("successful deployment has incomplete backup, fence, or service disposition")
        if any(value not in {"not_required", "completed"} for value in receipt["recovery_dispositions"].values()):
            raise _error("successful deployment contradicts unresolved recovery")
    if status == "recovery_required" and not any(
        value == "required" for value in receipt["recovery_dispositions"].values()
    ):
        raise _error("recovery-required receipt has no required recovery disposition")
    if status in {"failed", "blocked", "recovery_required"}:
        if not isinstance(failure, Mapping) or not failure.get("reason"):
            raise _error("non-success terminal receipt has no failure reason")
        if failure_phase not in {"pre_attestation", "post_attestation", "production", "recovery"}:
            raise _error("non-success terminal receipt has invalid failure phase")
        if receipt["service_activation"].get("status") == "active" or receipt["readiness"].get("status") == "ready" or receipt["query_evidence"].get("status") == "passed":
            raise _error("non-success deployment contradicts active or ready production")
    return dict(receipt)


def terminal_receipt_path(store_root: Path, idempotency_key: str) -> Path:
    if not idempotency_key:
        raise _error("terminal deployment receipt idempotency key is empty")
    name = hashlib.sha256(idempotency_key.encode()).hexdigest() + ".json"
    return store_root / name


def write_terminal_receipt(
    store_root: Path, candidate: Mapping[str, Any], *, private_key: bytes | str,
    kid: str, trust_keys: Mapping[str, str], history_policy: Mapping[str, Any],
) -> dict[str, Any]:
    """Write once by idempotency key; exact retry returns without mutation."""
    row = seal_terminal_receipt(candidate, private_key, kid=kid)
    verify_terminal_receipt(row, trust_keys, history_policy)
    path = terminal_receipt_path(store_root, str(row["idempotency_key"]))
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    with lock_path.open("a+", encoding="utf-8") as lock:
        os.chmod(lock_path, 0o600)
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        if path.exists():
            existing = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(existing, dict):
                raise _error("terminal deployment receipt store is invalid")
            verify_terminal_receipt(existing, trust_keys, history_policy)
            if _canonical(existing) != _canonical(row):
                raise _error("idempotency key already has a different terminal receipt")
            return {**existing, "exact_retry": True}
        _atomic_json(path, row)
        return {**row, "exact_retry": False}


def read_terminal_receipt(
    path: Path, *, trust_keys: Mapping[str, str], history_policy: Mapping[str, Any]
) -> dict[str, Any]:
    if not path.is_file():
        raise _error("terminal deployment receipt does not exist", path=str(path))
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise _error("terminal deployment receipt store is invalid")
    return verify_terminal_receipt(value, trust_keys, history_policy)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(_canonical(payload) + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary.unlink(missing_ok=True)
