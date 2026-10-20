"""Fail-closed signed release attestations for protected deployment admission."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping

from okg.substrate.errors import SubstrateError
from okg.substrate.exchange.signing import sign_document, signature_kid, verify_signed_document

ATTESTATION_SCHEMA = "okg.deployment_release_attestation/v1"
ATTESTATION_DOMAIN = "okg-ci:staging-to-production"
TRUST_SCHEMA = "okg.deployment_attestation_trust/v1"
RECEIPT_SCHEMA = "okg.deployment_attestation_admission/v1"
MAX_LIFETIME_SECONDS = 900
_REQUIRED = {
    "schema", "domain", "audience", "issuer", "staging_environment_id",
    "repository", "ref", "workflow_run_id", "workflow_run_attempt", "commit_sha",
    "promotion_id", "artifact_digest", "definition_digest", "evidence_generation_id",
    "evidence_digests", "issued_at", "expires_at", "nonce",
}
_SIGNED_FIELDS = _REQUIRED | {"signing_key_id", "signature"}


def _error(message: str, **data: object) -> SubstrateError:
    return SubstrateError(message, data=data)


def _canonical(value: Mapping[str, Any]) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def policy_digest(policy: Mapping[str, Any]) -> str:
    unsigned = dict(policy)
    unsigned.pop("policy_digest", None)
    return "sha256:" + hashlib.sha256(_canonical(unsigned)).hexdigest()


def seal_trust_manifest(manifest: Mapping[str, Any]) -> dict[str, Any]:
    sealed = dict(manifest)
    sealed["policy_digest"] = policy_digest(sealed)
    validate_trust_manifest(sealed)
    return sealed


def validate_trust_manifest(manifest: Mapping[str, Any]) -> None:
    if manifest.get("schema") != TRUST_SCHEMA:
        raise _error("attestation trust manifest has the wrong schema")
    if manifest.get("policy_digest") != policy_digest(manifest):
        raise _error("attestation trust manifest policy digest changed")
    keys = manifest.get("keys")
    if not isinstance(keys, Mapping) or not keys:
        raise _error("attestation trust manifest has no keys")
    for kid, entry in keys.items():
        if not isinstance(kid, str) or not isinstance(entry, Mapping):
            raise _error("attestation trust manifest key entry is invalid")
        if entry.get("status") not in {"active", "retiring", "revoked"}:
            raise _error("attestation trust key has invalid status", kid=kid)
        if not isinstance(entry.get("public_key"), str):
            raise _error("attestation trust key has no public key", kid=kid)
        not_before = _parse_time(entry.get("not_before"), "not_before")
        not_after = _parse_time(entry.get("not_after"), "not_after")
        if not_before > not_after:
            raise _error("attestation trust key window is inverted", kid=kid)
        if entry.get("status") == "revoked" and not entry.get("revoked_at"):
            raise _error("revoked attestation trust key has no revoked_at", kid=kid)
        if entry.get("revoked_at") is not None:
            revoked_at = _parse_time(entry.get("revoked_at"), "revoked_at")
            if entry.get("status") != "revoked" or revoked_at < not_before:
                raise _error("attestation trust key revoked_at is inconsistent", kid=kid)


def write_trust_manifest(path: Path, manifest: Mapping[str, Any]) -> dict[str, Any]:
    sealed = seal_trust_manifest(manifest)
    path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_json(path, sealed)
    return sealed


def sign_release_attestation(payload: Mapping[str, Any], private_key: bytes | str, *, kid: str) -> dict[str, Any]:
    document = dict(payload)
    _validate_payload(document)
    return sign_document(document, private_key, kid=kid)


def validate_release_attestation_payload(document: Mapping[str, Any]) -> None:
    """Validate historical signed payload shape without applying current-time freshness."""
    _validate_payload(document)


def verify_release_attestation(
    document: Mapping[str, Any], manifest: Mapping[str, Any], *,
    expected_audience: str, expected_repository: str, expected_issuer: str,
    expected_staging_environment_id: str, expected_commit_sha: str,
    expected_artifact_digest: str, expected_definition_digest: str,
    expected_evidence_generation_id: str, expected_evidence_digests: Mapping[str, str],
    expected_ref: str = "refs/heads/dev",
    now: datetime | None = None,
) -> dict[str, Any]:
    validate_trust_manifest(manifest)
    signed = dict(document)
    kid = signature_kid(signed)
    entry = manifest["keys"].get(kid)
    if not isinstance(entry, Mapping):
        raise _error("attestation signing key is not trusted", kid=kid)
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    if entry.get("status") == "revoked":
        raise _error("attestation signing key is revoked", kid=kid)
    if not _parse_time(entry["not_before"], "not_before") <= current <= _parse_time(entry["not_after"], "not_after"):
        raise _error("attestation signing key is outside its admission window", kid=kid)
    verify_signed_document(signed, str(entry["public_key"]))
    _validate_payload(signed, now=current)
    expected = {"audience": expected_audience, "repository": expected_repository, "ref": expected_ref,
        "issuer": expected_issuer, "staging_environment_id": expected_staging_environment_id,
        "commit_sha": expected_commit_sha, "artifact_digest": expected_artifact_digest,
        "definition_digest": expected_definition_digest,
        "evidence_generation_id": expected_evidence_generation_id,
        "evidence_digests": dict(expected_evidence_digests)}
    mismatches = {key: {"expected": value, "observed": signed.get(key)} for key, value in expected.items() if signed.get(key) != value}
    if mismatches:
        raise _error("release attestation differs from trusted request", mismatches=mismatches)
    return signed


def admit_release_attestation(
    document: Mapping[str, Any], manifest: Mapping[str, Any], *, history_policy: Mapping[str, Any],
    ledger_path: Path, expected_audience: str, expected_repository: str,
    expected_issuer: str, expected_staging_environment_id: str, expected_commit_sha: str,
    expected_artifact_digest: str, expected_definition_digest: str,
    expected_evidence_generation_id: str, expected_evidence_digests: Mapping[str, str],
    expected_ref: str = "refs/heads/dev", now: datetime | None = None,
) -> dict[str, Any]:
    validate_trust_manifest(history_policy)
    verified = verify_release_attestation(document, manifest, expected_audience=expected_audience,
        expected_repository=expected_repository, expected_issuer=expected_issuer,
        expected_staging_environment_id=expected_staging_environment_id, expected_commit_sha=expected_commit_sha,
        expected_artifact_digest=expected_artifact_digest, expected_definition_digest=expected_definition_digest,
        expected_evidence_generation_id=expected_evidence_generation_id,
        expected_evidence_digests=expected_evidence_digests,
        expected_ref=expected_ref, now=now)
    kid = signature_kid(verified)
    admission_key = manifest["keys"].get(kid)
    history_key = history_policy["keys"].get(kid)
    if not isinstance(history_key, Mapping) or history_key.get("public_key") != admission_key.get("public_key"):
        raise _error("attestation signing key is not preserved by archival trust", kid=kid)
    signed_bytes = _canonical(verified)
    identity = hashlib.sha256(signed_bytes).hexdigest()
    promotion_id = str(verified["promotion_id"])
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = ledger_path.with_suffix(ledger_path.suffix + ".lock")
    with lock_path.open("a+", encoding="utf-8") as lock:
        os.chmod(lock_path, 0o600)
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        ledger: dict[str, Any] = {}
        if ledger_path.exists():
            loaded = json.loads(ledger_path.read_text(encoding="utf-8"))
            if not isinstance(loaded, dict):
                raise _error("attestation admission ledger is invalid")
            ledger = loaded
        existing = ledger.get(promotion_id)
        if existing is not None:
            if existing.get("signed_bytes_digest") != "sha256:" + identity:
                raise _error("promotion id was already used by a different attestation")
            return {**existing, "exact_retry": True}
        receipt = {
            "schema": RECEIPT_SCHEMA, "promotion_id": promotion_id,
            "signed_bytes_digest": "sha256:" + identity, "artifact_digest": verified["artifact_digest"],
            "workflow_run_id": verified["workflow_run_id"], "workflow_run_attempt": verified["workflow_run_attempt"],
            "admission_policy_digest": manifest["policy_digest"],
            "history_policy_digest": policy_digest(history_policy), "signing_key_id": kid,
            "signed_attestation": verified,
            "admitted_at": (now or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat(),
            "exact_retry": False,
        }
        ledger[promotion_id] = receipt
        _atomic_json(ledger_path, ledger)
        return receipt


def verify_historical_admission_receipt(receipt: Mapping[str, Any], history_policy: Mapping[str, Any]) -> None:
    if receipt.get("schema") != RECEIPT_SCHEMA:
        raise _error("historical admission receipt has the wrong schema")
    attestation = receipt.get("signed_attestation")
    if not isinstance(attestation, dict):
        raise _error("historical admission receipt lacks archival verification material")
    validate_trust_manifest(history_policy)
    if receipt.get("history_policy_digest") != history_policy.get("policy_digest"):
        raise _error("historical admission receipt history policy changed")
    kid = signature_kid(attestation)
    key = history_policy["keys"].get(kid)
    if not isinstance(key, Mapping) or not isinstance(key.get("public_key"), str):
        raise _error("historical admission receipt signing key is not archived")
    verify_signed_document(attestation, str(key["public_key"]))
    validate_release_attestation_payload(attestation)
    digest = "sha256:" + hashlib.sha256(_canonical(attestation)).hexdigest()
    if digest != receipt.get("signed_bytes_digest"):
        raise _error("historical admission receipt attestation digest changed")


def render_service_credential_contracts(*, root: Path, services: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    """Render descriptors only; live UID/file installation is a later task."""
    rows = []
    seen_uids: set[int] = set()
    seen_usernames: set[str] = set()
    for service_id, spec in sorted(services.items()):
        if re.fullmatch(r"[a-z0-9][a-z0-9-]*", service_id) is None:
            raise _error("service credential contract has an unsafe service id", service=service_id)
        uid = spec.get("uid")
        if not isinstance(uid, int) or uid <= 0 or uid in seen_uids:
            raise _error("service credential contract requires a distinct positive uid", service=service_id)
        seen_uids.add(uid)
        username = spec.get("username")
        if not isinstance(username, str) or not username or username in seen_usernames:
            raise _error("service credential contract has no OS username", service=service_id)
        seen_usernames.add(username)
        credential_names = spec.get("credential_names")
        if not isinstance(credential_names, list) or not credential_names or not all(isinstance(v, str) and v for v in credential_names):
            raise _error("service credential contract has invalid credential names", service=service_id)
        credential_file = str(root / f"{service_id}.env")
        rows.append({"service_id": service_id, "username": username, "uid": uid, "credential_file": credential_file,
                     "mode": "0600", "owner_uid": uid, "credential_names": sorted(credential_names), "content": "<redacted>",
                     "systemd": {"User": username, "EnvironmentFile": credential_file},
                     "launchd": {"UserName": username, "EnvironmentFile": credential_file}})
    return {"schema": "okg.service_credential_contracts/v1", "services": rows,
            "contract_digest": "sha256:" + hashlib.sha256(_canonical({"services": rows})).hexdigest()}


def _validate_payload(payload: Mapping[str, Any], *, now: datetime | None = None) -> None:
    missing = sorted(_REQUIRED - payload.keys())
    if missing:
        raise _error("release attestation is missing required fields", missing=missing)
    unknown = sorted(payload.keys() - _SIGNED_FIELDS)
    if unknown:
        raise _error("release attestation has unknown fields", unknown=unknown)
    if payload.get("schema") != ATTESTATION_SCHEMA or payload.get("domain") != ATTESTATION_DOMAIN:
        raise _error("release attestation has the wrong schema or domain")
    if not isinstance(payload.get("evidence_digests"), Mapping) or not payload["evidence_digests"]:
        raise _error("release attestation has no evidence digests")
    issued = _parse_time(payload["issued_at"], "issued_at")
    expires = _parse_time(payload["expires_at"], "expires_at")
    lifetime = (expires - issued).total_seconds()
    if lifetime <= 0 or lifetime > MAX_LIFETIME_SECONDS:
        raise _error("release attestation lifetime exceeds 15 minutes")
    if now is not None and not issued <= now <= expires:
        raise _error("release attestation is expired or from the future")


def verify_github_oidc_request(
    token: str, claims: Mapping[str, Any], attestation: Mapping[str, Any], *,
    verify_signature: Callable[[str], Mapping[str, Any]],
    expected_audience: str, expected_repository: str, expected_environment: str,
    expected_job_workflow_ref: str, nonce_ledger_path: Path, now: datetime | None = None,
) -> dict[str, Any]:
    """Validate already-JWKS-verified GitHub claims and consume a request nonce."""
    verified_claims = verify_signature(token)
    if dict(verified_claims) != dict(claims):
        raise _error("GitHub OIDC verified claims differ from supplied claims")
    current = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    expected = {
        "iss": "https://token.actions.githubusercontent.com", "aud": expected_audience,
        "repository": expected_repository, "ref": "refs/heads/dev", "environment": expected_environment,
        "job_workflow_ref": expected_job_workflow_ref, "run_id": str(attestation.get("workflow_run_id")),
        "run_attempt": str(attestation.get("workflow_run_attempt")), "sha": attestation.get("commit_sha"),
    }
    mismatches = {key: {"expected": value, "observed": claims.get(key)} for key, value in expected.items()
                  if claims.get(key) != value}
    if mismatches:
        raise _error("GitHub OIDC claims differ from trusted request", mismatches=mismatches)
    try:
        issued = datetime.fromtimestamp(int(claims["iat"]), timezone.utc)
        expires = datetime.fromtimestamp(int(claims["exp"]), timezone.utc)
    except (KeyError, TypeError, ValueError) as exc:
        raise _error("GitHub OIDC token time window is invalid") from exc
    if not issued <= current <= expires or (expires - issued).total_seconds() > MAX_LIFETIME_SECONDS:
        raise _error("GitHub OIDC token is expired, future-dated, or overlong")
    nonce = claims.get("nonce")
    if not isinstance(nonce, str) or not nonce:
        raise _error("GitHub OIDC request nonce is missing")
    if nonce != attestation.get("nonce"):
        raise _error("GitHub OIDC nonce differs from signed staging attestation")
    _consume_nonce(nonce_ledger_path, nonce)
    return dict(claims)


def _consume_nonce(path: Path, nonce: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    with lock_path.open("a+", encoding="utf-8") as lock:
        os.chmod(lock_path, 0o600)
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        used: list[str] = []
        if path.exists():
            loaded = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(loaded, dict) or not all(isinstance(item, str) for item in loaded.values()):
                raise _error("GitHub OIDC nonce ledger is invalid")
            used = list(loaded.values())
        if nonce in used:
            raise _error("GitHub OIDC request nonce was already used")
        _atomic_json(path, {str(index): value for index, value in enumerate([*used, nonce])})


def _parse_time(value: object, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError as exc:
        raise _error("attestation timestamp is invalid", field=field) from exc
    if parsed.tzinfo is None:
        raise _error("attestation timestamp must include a timezone", field=field)
    return parsed.astimezone(timezone.utc)


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(_canonical(payload) + b"\n")
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)
