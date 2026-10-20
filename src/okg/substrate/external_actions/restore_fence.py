"""Pre-bind execution-epoch verification and restore quarantine primitives.

The startup gate performs all trust and database work before a socket can be
bound.  A malformed, stale, forked, or unverifiable lease chain raises
``RestoreFenceError`` and leaves the caller with no listener to expose.
"""

from __future__ import annotations

import contextlib
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from .signing import (
    SigningError,
    b64url_decode,
    canonical_json_bytes,
    domain_digest,
    parse_timestamp,
    strict_json_loads,
    verify_detached_envelope,
)


EXTERNAL_ACTION_RESTORE_REFUSAL_CODE = (
    "external_action_restore_requires_external_fence"
)
EXTERNAL_ACTION_ROLLOUT_DISABLED = "disabled"
EXTERNAL_ACTION_ROLLOUT_ENABLED = "enabled"
EXTERNAL_ACTION_ROLLOUT_BLOCKED = "blocked"

# A database restore may erase any one of these deployment-bound authorities.
# Keep the probe closed: a partially installed schema is indeterminate, never
# evidence that external actions were disabled.
_ROLLOUT_RELATIONS = (
    "external_action_proposals",
    "external_action_payload_versions",
    "external_action_decisions",
    "external_action_transition_idempotency",
    "external_action_executions",
    "external_action_outcomes",
    "external_action_events",
    "external_action_auth_audits",
    "external_action_dpop_replay_nonces",
    "external_action_owner_unavailability_attestations",
    "external_action_restore_fences",
    "external_action_restore_recoveries",
    "external_action_execution_epoch_leases",
    "external_action_restore_attestations",
    "external_action_restore_sessions",
    "external_action_restore_completion_receipts",
    "external_action_restore_finalize_idempotency",
    "external_action_restore_checkpoint_staging",
    "external_action_restore_checkpoint_entries",
    "external_action_api_role_bindings",
)


def _database_external_action_footprint(
    *, deployment: str, dsn: str
) -> str:
    """Read whether this database has ever held deployment rollout state."""

    try:
        from psycopg import sql

        from okg.substrate.db.connection import connect

        with connect(
            dsn,
            application_name="okg-external-action-rollout-guard",
        ) as connection:
            with connection.transaction():
                connection.execute("SET TRANSACTION READ ONLY")
                rows = connection.execute(
                    """
                    SELECT c.relname
                      FROM pg_catalog.pg_class AS c
                      JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                     WHERE n.nspname = 'okg'
                       AND c.relkind IN ('r', 'p')
                       AND c.relname = ANY(%s)
                     ORDER BY c.relname
                    """,
                    (list(_ROLLOUT_RELATIONS),),
                ).fetchall()
                found = {str(row[0]) for row in rows}
                expected = set(_ROLLOUT_RELATIONS)
                if not found:
                    return EXTERNAL_ACTION_ROLLOUT_DISABLED
                if found != expected:
                    return EXTERNAL_ACTION_ROLLOUT_BLOCKED
                for relation in _ROLLOUT_RELATIONS:
                    present = connection.execute(
                        sql.SQL(
                            "SELECT EXISTS (SELECT 1 FROM okg.{} "
                            "WHERE deployment_name = %s)"
                        ).format(sql.Identifier(relation)),
                        (deployment,),
                    ).fetchone()
                    if present and present[0] is True:
                        return EXTERNAL_ACTION_ROLLOUT_ENABLED
    except Exception:
        return EXTERNAL_ACTION_ROLLOUT_BLOCKED
    # Once the complete schema exists, empty current tables do not prove that
    # this deployment was never enabled: authority may have been removed or a
    # prior database image may already have discarded the retained rows.  Only
    # a database that predates the entire feature can support ``disabled``.
    return EXTERNAL_ACTION_ROLLOUT_BLOCKED


def _authority_file_presence(root: Path) -> tuple[bool, bool] | None:
    """Return closed authority-path presence, or ``None`` on uncertainty."""

    try:
        if root.is_symlink() or not root.is_dir():
            return None
        review_path = root / "external-action-review.yaml"
        registry_path = root / "external-actions.yaml"
        return (
            review_path.exists() or review_path.is_symlink(),
            registry_path.exists() or registry_path.is_symlink(),
        )
    except OSError:
        return None


def external_action_rollout_state(
    *, deployment: str, deployment_root: str | Path, dsn: str
) -> str:
    """Classify whether ordinary database rollback is safe to enter.

    This guard only reads deployment authority and the live database.  It does
    not stop a worker, rotate an epoch, revoke a provider credential, mutate a
    database, or perform a restore.  ``disabled`` requires stable absence of
    both rollout-authority files and of the complete external-action schema.
    An installed but empty schema is indeterminate, because absence of current
    rows cannot prove the deployment was never enabled.  Every malformed,
    partial, unreadable, or unavailable state fails closed as ``blocked``.

    This is an assertion under the deployment promotion lock, not a mutual-
    exclusion primitive.  The restore caller and any supported rollout must
    serialize through that existing deployment operation boundary.
    """

    if not isinstance(deployment, str) or not re.fullmatch(
        r"[a-z][a-z0-9-]{0,63}", deployment
    ):
        return EXTERNAL_ACTION_ROLLOUT_BLOCKED
    if not isinstance(dsn, str) or not dsn:
        return EXTERNAL_ACTION_ROLLOUT_BLOCKED
    root = Path(deployment_root)
    presence = _authority_file_presence(root)
    if presence is None:
        return EXTERNAL_ACTION_ROLLOUT_BLOCKED
    review_path = root / "external-action-review.yaml"
    registry_path = root / "external-actions.yaml"
    review_exists, registry_exists = presence

    if not review_exists and not registry_exists:
        database_state = _database_external_action_footprint(
            deployment=deployment, dsn=dsn
        )
        # Close the observed review-window race: authority created or replaced
        # while the database proof ran makes the result indeterminate.  The
        # caller's deployment-operation lock closes the interval after this
        # final observation and before the destructive restore boundary.
        if _authority_file_presence(root) != (False, False):
            return EXTERNAL_ACTION_ROLLOUT_BLOCKED
        return database_state
    if review_exists != registry_exists:
        return EXTERNAL_ACTION_ROLLOUT_BLOCKED
    if (
        review_path.is_symlink()
        or registry_path.is_symlink()
        or not review_path.is_file()
        or not registry_path.is_file()
    ):
        return EXTERNAL_ACTION_ROLLOUT_BLOCKED

    try:
        from .action_contracts import resolve_action_contracts
        from .config import (
            load_action_contracts,
            load_config,
            load_deployment_product_authority,
        )

        config = load_config(review_path)
        if (
            config.deployment != deployment
            or config.deployment_product_authority is None
            or not config.verifier_capability_env
            or not config.action_registry_path
        ):
            return EXTERNAL_ACTION_ROLLOUT_BLOCKED
        declared_registry = Path(config.action_registry_path)
        if not declared_registry.is_absolute():
            declared_registry = review_path.parent / declared_registry
        if declared_registry.resolve(strict=True) != registry_path.resolve(strict=True):
            return EXTERNAL_ACTION_ROLLOUT_BLOCKED
        load_action_contracts(registry_path)
        authored, resolved_lock, _ = load_deployment_product_authority(
            config.deployment_product_authority,
            deployment=deployment,
        )
        contracts = resolve_action_contracts(
            registry_path,
            authored_definition=authored,
            resolved_lock=resolved_lock,
        )
        if not contracts:
            return EXTERNAL_ACTION_ROLLOUT_BLOCKED
    except Exception:
        return EXTERNAL_ACTION_ROLLOUT_BLOCKED
    return EXTERNAL_ACTION_ROLLOUT_ENABLED


class RestoreFenceError(ValueError):
    code = "execution_epoch_authority_unavailable"

    def __init__(self, message: str, *, code: str | None = None) -> None:
        super().__init__(message)
        if code:
            self.code = code


@dataclass(frozen=True)
class VerifiedLease:
    envelope: Mapping[str, Any]
    payload: Mapping[str, Any]
    envelope_digest: str
    signer_key_id_digest: str

    @property
    def epoch(self) -> int:
        return int(self.payload["epoch"])

    @property
    def sequence(self) -> int:
        return int(self.payload["lease_sequence"])

    @property
    def issued_at(self) -> datetime:
        return parse_timestamp(self.payload["issued_at"])

    @property
    def expires_at(self) -> datetime:
        return parse_timestamp(self.payload["expires_at"])


@dataclass(frozen=True)
class StartupResult:
    deployment: str
    highest_epoch: int
    highest_sequence: int
    lease_chain_digest: str
    verification_config_digest: str
    verification_login_binding_digest: str
    service_mode: str
    inserted_count: int
    quarantined: bool


@dataclass(frozen=True)
class VerifiedRestoreEnvelope:
    """A restore evidence envelope after deployment-key verification.

    The API accepts references to evidence held by the deployment restore
    authority.  It never treats a request-body object as evidence.  Keeping
    the envelope and its verified payload together also makes it impossible
    for the service to accidentally hash one representation and persist
    another.
    """

    envelope: Mapping[str, str]
    payload: Mapping[str, Any]
    envelope_digest: str
    purpose: str
    key_id: str


LEASE_FIELDS = (
    "deployment", "epoch", "lease_sequence", "previous_lease_digest", "previous_epoch_digest",
    "issued_at", "expires_at", "signer_separation_subject", "nonce",
)
CHAIN_SCHEMA = "okg-external-action-execution-epoch-lease-chain/v1"
CHAIN_DOMAIN = "okg-external-action-execution-epoch-lease-chain-v1"

_RESTORE_ENVELOPE_SPECS: dict[str, tuple[str, str, str, tuple[str, ...]]] = {
    "execution_epoch_lease": (
        "okg-execution-epoch+jws",
        "okg-external-action-execution-epoch-lease-envelope-v1",
        "execution_epoch_lease",
        LEASE_FIELDS,
    ),
    "worker_quiescence": (
        "okg-worker-quiescence+jws",
        "okg-external-action-worker-quiescence-envelope-v1",
        "worker_quiescence",
        (
            "deployment", "old_epoch", "epoch_rotation_lease_digest",
            "worker_instance_set_digest", "worker_instance_count", "quiesced_at",
            "issued_at", "nonce", "signer_separation_subject",
        ),
    ),
    "provider_authority_fence": (
        "okg-provider-authority-fence+jws",
        "okg-external-action-provider-authority-fence-envelope-v1",
        "provider_authority_fence",
        (
            "deployment", "old_epoch", "epoch_rotation_lease_digest",
            "provider_authority_set_digest", "provider_authority_count", "fenced_at",
            "issued_at", "nonce", "signer_separation_subject",
        ),
    ),
    "ledger_fence": (
        "okg-ledger-fence+jws",
        "okg-external-action-ledger-fence-envelope-v1",
        "ledger_fence",
        (
            "deployment", "old_epoch", "epoch_rotation_lease_digest",
            "worker_quiescence_envelope_digest", "provider_authority_fence_envelope_digest",
            "fenced_ledger_root_digest", "fenced_ledger_entry_count", "fenced_at",
            "last_accepted_begin_at", "nonce", "signer_separation_subject",
        ),
    ),
    "restore_attestation": (
        "okg-restore-attestation+jws",
        "okg-external-action-restore-attestation-envelope-v1",
        "restore_attestation",
        (
            "deployment", "restore_id", "old_epoch", "new_epoch",
            "old_epoch_lease_digest", "new_epoch_lease_digest",
            "ledger_fence_envelope_digest", "fenced_ledger_root_digest",
            "fenced_ledger_entry_count", "restored_snapshot_digest",
            "pre_restore_event_root_digest", "pre_restore_event_head_count",
            "effect_inventory_digest", "effect_inventory_count",
            "worker_checkpoint_set_digest", "worker_checkpoint_count",
            "issued_at", "expires_at", "nonce", "signer_separation_subject",
        ),
    ),
    "worker_checkpoint": (
        "okg-worker-checkpoint+jws",
        "okg-external-action-worker-checkpoint-envelope-v1",
        "worker_checkpoint",
        (
            "deployment", "effect_instance_key", "execution_epoch",
            "ledger_fence_envelope_digest", "fenced_ledger_root_digest",
            "fenced_ledger_entry_count", "proof_kind", "positive_proof_digest",
            "nonmembership_proof_digest", "ledger_entry_digest", "submission_content_hash",
            "proposal_id", "proposal_request_hash", "original_payload_hash",
            "approved_payload_hash", "action_contract_digest", "decision_id",
            "decision_binding_hash", "execution_id", "execution_binding_hash",
            "okg_claim_handoff_receipt_digest", "provider_idempotency_key_digest",
            "journal_phase", "effect_started_at", "provider_receipt_digest", "outcome",
            "outcome_reason_code", "outcome_recorded_at", "journal_digest",
            "pre_restore_effect_event_head_hash", "checkpoint_issued_at",
        ),
    ),
}

_RESTORE_DIGEST_FIELDS = {
    "old_epoch_lease_digest", "new_epoch_lease_digest", "epoch_rotation_lease_digest",
    "worker_instance_set_digest", "provider_authority_set_digest",
    "worker_quiescence_envelope_digest", "provider_authority_fence_envelope_digest",
    "ledger_fence_envelope_digest", "fenced_ledger_root_digest", "restored_snapshot_digest",
    "pre_restore_event_root_digest", "effect_inventory_digest", "worker_checkpoint_set_digest",
    "positive_proof_digest", "nonmembership_proof_digest", "ledger_entry_digest",
    "submission_content_hash", "proposal_request_hash", "original_payload_hash",
    "approved_payload_hash", "action_contract_digest", "decision_binding_hash",
    "execution_binding_hash", "okg_claim_handoff_receipt_digest", "provider_idempotency_key_digest",
    "provider_receipt_digest", "journal_digest", "pre_restore_effect_event_head_hash",
}


def _restore_key_for(config: Any, kid: str, purpose: str) -> Any:
    """Resolve one key from the deployment-pinned *historical* key set."""

    try:
        key = config.key_for(kid, purpose=purpose)
    except AttributeError:
        keys = config.get("signing_keys", ()) if isinstance(config, Mapping) else ()
        matches = [item for item in keys if isinstance(item, Mapping) and item.get("kid") == kid and item.get("purpose") == purpose]
        if len(matches) != 1:
            raise RestoreFenceError("restore envelope signing key is unavailable")
        key = matches[0]
    raw = key.__dict__ if hasattr(key, "__dict__") else key
    if not isinstance(raw, Mapping) or raw.get("state") == "revoked_compromised":
        raise RestoreFenceError("restore envelope signing key is revoked")
    if raw.get("algorithm", "EdDSA") != "EdDSA":
        raise RestoreFenceError("restore envelope algorithm is not pinned")
    return key


def _restore_key_valid_at(config: Any, kid: str, purpose: str, at: datetime) -> Any:
    key = _restore_key_for(config, kid, purpose)
    raw = key.__dict__ if hasattr(key, "__dict__") else key
    for field in ("not_before", "not_after"):
        bound = raw.get(field) if isinstance(raw, Mapping) else None
        if bound is not None:
            boundary = parse_timestamp(bound)
            if field == "not_before" and at < boundary:
                raise RestoreFenceError("restore envelope predates signing-key validity")
            if field == "not_after" and at >= boundary:
                raise RestoreFenceError("restore envelope is outside signing-key validity")
    compromised = raw.get("compromise_revoked_at") if isinstance(raw, Mapping) else None
    if compromised is not None and at >= parse_timestamp(compromised):
        raise RestoreFenceError("restore envelope uses a compromised signing key")
    return key


def verify_restore_envelope(
    envelope: Mapping[str, Any] | bytes | str,
    *,
    config: Any,
    purpose: str,
    now: datetime | None = None,
    require_current_time: bool = True,
) -> VerifiedRestoreEnvelope:
    """Verify a restore envelope against the deployment key history.

    This is intentionally stricter than the generic signing helper: it binds
    the key purpose, historical validity, time window, digest fields, and
    nonce before the envelope can reach the SQL command boundary.
    """

    if purpose not in _RESTORE_ENVELOPE_SPECS:
        raise RestoreFenceError("unknown restore envelope purpose")
    expected_typ, expected_domain, key_purpose, expected_fields = _RESTORE_ENVELOPE_SPECS[purpose]
    if isinstance(envelope, (bytes, str)):
        try:
            envelope = strict_json_loads(envelope)
        except SigningError as exc:
            raise RestoreFenceError("restore envelope is not strict I-JSON") from exc
    if not isinstance(envelope, Mapping):
        raise RestoreFenceError("restore envelope is not an object")
    try:
        protected = strict_json_loads(b64url_decode(envelope.get("protected_b64", ""), field="protected_b64"))
        if not isinstance(protected, Mapping) or set(protected) != {"alg", "kid", "typ"}:
            raise RestoreFenceError("restore envelope protected header is not closed")
        kid = protected.get("kid")
        if not isinstance(kid, str) or protected.get("alg") != "EdDSA" or protected.get("typ") != expected_typ:
            raise RestoreFenceError("restore envelope protected header is invalid")
        raw_payload = strict_json_loads(b64url_decode(envelope.get("payload_b64", ""), field="payload_b64"))
        if not isinstance(raw_payload, Mapping):
            raise RestoreFenceError("restore envelope payload is not an object")
        timestamp_value = raw_payload.get("issued_at", raw_payload.get("fenced_at", raw_payload.get("checkpoint_issued_at")))
        if not isinstance(timestamp_value, str):
            raise RestoreFenceError("restore envelope has no issuance time")
        issued_at = parse_timestamp(timestamp_value)
        key = _restore_key_valid_at(config, kid, key_purpose, issued_at)
        key_state = (key.__dict__ if hasattr(key, "__dict__") else key).get("state")
        if require_current_time and key_state != "current":
            raise RestoreFenceError("new restore evidence requires a current signing key")
        public_key = getattr(key, "public_key_b64", None)
        if public_key is None and isinstance(key, Mapping):
            public_key = key.get("public_key_b64")
        if not isinstance(public_key, str):
            raise RestoreFenceError("restore envelope signing key has no public key")
        payload = verify_detached_envelope(
            envelope,
            public_key,
            expected_kid=kid,
            expected_typ=expected_typ,
            expected_domain=expected_domain,
            expected_fields=expected_fields,
        )
    except RestoreFenceError:
        raise
    except (SigningError, KeyError, TypeError, ValueError) as exc:
        raise RestoreFenceError("restore envelope signature or closed payload is invalid") from exc
    digest = envelope.get("envelope_digest")
    if not isinstance(digest, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest):
        raise RestoreFenceError("restore envelope digest is invalid")
    for field in _RESTORE_DIGEST_FIELDS:
        value = payload.get(field)
        if value is not None and (not isinstance(value, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", value)):
            raise RestoreFenceError("restore envelope contains an invalid digest")
    clock = now or datetime.now(UTC)
    skew = getattr(getattr(config, "authentication", None), "clock_skew_seconds", None)
    if skew is None:
        skew = config.get("maximum_clock_skew_seconds", 30) if isinstance(config, Mapping) else 30
    for field in expected_fields:
        if field.endswith("_at") and payload.get(field) is not None:
            value = payload[field]
            if not isinstance(value, str):
                raise RestoreFenceError("restore envelope timestamp is invalid")
            parsed = parse_timestamp(value)
            if field != "expires_at" and require_current_time and parsed > clock + timedelta(seconds=int(skew)):
                raise RestoreFenceError("restore envelope contains a future timestamp")
    nonce = payload.get("nonce")
    subject = payload.get("signer_separation_subject")
    if purpose != "worker_checkpoint" and (not isinstance(nonce, str) or not re.fullmatch(r"[A-Za-z0-9._:+\-/]{1,256}", nonce)):
        raise RestoreFenceError("restore envelope nonce is invalid")
    if purpose != "worker_checkpoint" and (not isinstance(subject, str) or not re.fullmatch(r"[A-Za-z0-9._:+\-/]{1,256}", subject)):
        raise RestoreFenceError("restore envelope signer separation subject is invalid")
    if require_current_time and issued_at > clock + timedelta(seconds=int(skew)):
        raise RestoreFenceError("restore envelope is issued in the future")
    expires_at = payload.get("expires_at")
    if expires_at is not None:
        expires = parse_timestamp(expires_at)
        if issued_at >= expires or (require_current_time and clock >= expires):
            raise RestoreFenceError("restore envelope is expired")
    return VerifiedRestoreEnvelope(
        envelope={key: str(envelope[key]) for key in ("protected_b64", "payload_b64", "signature_b64", "envelope_digest")},
        payload=payload,
        envelope_digest=digest,
        purpose=purpose,
        key_id=kid,
    )


def _load_chain(source: bytes | str | Path | Mapping[str, Any]) -> Mapping[str, Any]:
    if isinstance(source, Mapping):
        # Keep in-memory launches on the same bounded strict-I-JSON path as
        # the file/bytes form.  Production ``create_app`` supplies a path or
        # bytes; mappings are retained solely for deterministic embedders.
        try:
            encoded = canonical_json_bytes(dict(source))
            if len(encoded) > 262_144:
                raise RestoreFenceError("epoch lease chain exceeds 256 KiB", code="review_request_too_large")
            value = strict_json_loads(encoded)
        except (SigningError, TypeError, ValueError, RecursionError) as exc:
            raise RestoreFenceError("epoch lease chain is not strict I-JSON") from exc
    else:
        if isinstance(source, Path) or (isinstance(source, str) and not source.lstrip().startswith(("{", "[")) and "\n" not in source and len(source) < 4096):
            try:
                raw = Path(source).read_bytes()
            except OSError as exc:
                raise RestoreFenceError("epoch lease chain cannot be read") from exc
        else:
            raw = source.encode("utf-8") if isinstance(source, str) else source
        if not isinstance(raw, bytes) or len(raw) > 262_144:
            raise RestoreFenceError("epoch lease chain exceeds 256 KiB", code="review_request_too_large")
        try:
            value = strict_json_loads(raw)
        except (SigningError, TypeError, ValueError, RecursionError) as exc:
            raise RestoreFenceError("epoch lease chain is not strict I-JSON") from exc
    if not isinstance(value, Mapping) or set(value) != {"schema", "deployment", "leases"}:
        raise RestoreFenceError("epoch lease chain has missing or extra fields")
    if value["schema"] != CHAIN_SCHEMA or not isinstance(value["deployment"], str) or not isinstance(value["leases"], list):
        raise RestoreFenceError("epoch lease chain has invalid schema")
    if not isinstance(value["deployment"], str) or not re.fullmatch(r"[a-z][a-z0-9-]{0,63}", value["deployment"]):
        raise RestoreFenceError("epoch lease chain has an invalid deployment")
    if not 1 <= len(value["leases"]) <= 64:
        raise RestoreFenceError("epoch lease chain must contain 1..64 leases")
    return value


def _key_for(config: Any, kid: str, purpose: str) -> Any:
    if hasattr(config, "key_for"):
        return config.key_for(kid, purpose=purpose)
    keys = config.get("signing_keys", []) if isinstance(config, Mapping) else []
    matches = [key for key in keys if key.get("kid") == kid and key.get("purpose") == purpose]
    if len(matches) != 1 or matches[0].get("state") == "revoked_compromised":
        raise RestoreFenceError("unknown or revoked epoch signing key")
    return matches[0]


def _public_key(value: Any) -> str:
    if isinstance(value, str):
        return value
    if hasattr(value, "public_key_b64"):
        return value.public_key_b64
    if isinstance(value, Mapping):
        return value["public_key_b64"]
    raise RestoreFenceError("epoch signing key has no public key")


def _key_valid_at(config: Any, kid: str, at: datetime, purpose: str) -> Any:
    key = _key_for(config, kid, purpose)
    raw = key.__dict__ if hasattr(key, "__dict__") else key
    for field in ("not_before", "not_after"):
        if raw.get(field) is not None:
            bound = parse_timestamp(raw[field])
            if field == "not_before" and at < bound:
                raise RestoreFenceError("lease predates signing key validity")
            if field == "not_after" and at >= bound:
                raise RestoreFenceError("lease is outside signing key validity")
    if raw.get("compromise_revoked_at") is not None and at >= parse_timestamp(raw["compromise_revoked_at"]):
        raise RestoreFenceError("lease uses a compromised signing key")
    return key


def verify_lease_chain(
    source: bytes | str | Path | Mapping[str, Any],
    *,
    config: Any,
    deployment: str | None = None,
    now: datetime | None = None,
    verification_config_digest: str | None = None,
    login_binding_digest: str | None = None,
    retained: Sequence[VerifiedLease] = (),
) -> tuple[tuple[VerifiedLease, ...], str]:
    """Parse, verify, and validate one complete ordered lease-chain file."""

    chain = _load_chain(source)
    expected_deployment = deployment or getattr(config, "deployment", None) or (config.get("deployment") if isinstance(config, Mapping) else None)
    if chain["deployment"] != expected_deployment:
        raise RestoreFenceError("lease deployment does not match configured deployment")
    clock = now or datetime.now(UTC)
    checked: list[VerifiedLease] = []
    envelope_digests: list[str] = []
    seen_pairs: set[tuple[int, int]] = set()
    seen_digests: set[str] = set()
    seen_nonces: set[tuple[str, str]] = set()
    for index, item in enumerate(chain["leases"]):
        if not isinstance(item, Mapping) or set(item) != {"protected_b64", "payload_b64", "signature_b64", "envelope_digest"}:
            raise RestoreFenceError(f"lease {index} has missing or extra envelope fields")
        try:
            # Decode protected first to select the pinned historical key.
            from .signing import b64url_decode
            protected = strict_json_loads(b64url_decode(item["protected_b64"], field="protected_b64"))
            kid = protected.get("kid") if isinstance(protected, Mapping) else None
            typ = protected.get("typ") if isinstance(protected, Mapping) else None
            if not isinstance(kid, str) or typ != "okg-execution-epoch+jws":
                raise RestoreFenceError("lease has the wrong key or typ")
            # Read the signed payload only to determine its claimed issue
            # time; key validity is evaluated at that historical time (a
            # retired key remains valid for records it signed).
            raw_payload = strict_json_loads(b64url_decode(item["payload_b64"], field="payload_b64"))
            if not isinstance(raw_payload, Mapping) or not isinstance(raw_payload.get("issued_at"), str):
                raise RestoreFenceError("lease payload has no issued_at")
            issued_at = parse_timestamp(raw_payload["issued_at"])
            payload = verify_detached_envelope(
                item,
                _public_key(_key_valid_at(config, kid, issued_at, "execution_epoch_lease")),
                expected_kid=kid,
                expected_typ="okg-execution-epoch+jws",
                expected_domain="okg-external-action-execution-epoch-lease-envelope-v1",
                expected_fields=LEASE_FIELDS,
            )
        except (SigningError, KeyError, TypeError, ValueError) as exc:
            raise RestoreFenceError("lease signature or closed payload is invalid") from exc
        if payload.get("deployment") != chain["deployment"] or not isinstance(payload.get("epoch"), int) or isinstance(payload.get("epoch"), bool) or payload.get("epoch") < 0 or not isinstance(payload.get("lease_sequence"), int) or isinstance(payload.get("lease_sequence"), bool) or payload.get("lease_sequence") < 1:
            raise RestoreFenceError("lease has invalid deployment or sequence")
        signer = payload.get("signer_separation_subject")
        nonce = payload.get("nonce")
        if (
            not isinstance(signer, str)
            or not re.fullmatch(r"[A-Za-z0-9._:+/\\-]{1,256}", signer)
            or not isinstance(nonce, str)
            or not re.fullmatch(r"[A-Za-z0-9._:+/\\-]{1,256}", nonce)
        ):
            raise RestoreFenceError("lease signer or nonce is invalid")
        nonce_key = (signer, nonce)
        if nonce_key in seen_nonces:
            raise RestoreFenceError("lease chain contains a duplicate nonce")
        seen_nonces.add(nonce_key)
        pair = (payload["epoch"], payload["lease_sequence"])
        if pair in seen_pairs:
            raise RestoreFenceError("lease chain contains a duplicate sequence")
        seen_pairs.add(pair)
        digest = item["envelope_digest"]
        if digest in seen_digests:
            raise RestoreFenceError("lease chain contains a duplicate digest")
        seen_digests.add(digest)
        issued = parse_timestamp(payload["issued_at"])
        expires = parse_timestamp(payload["expires_at"])
        if not issued < expires:
            raise RestoreFenceError("lease has an invalid time interval")
        if checked and issued < checked[-1].issued_at:
            raise RestoreFenceError("lease issue times must be nondecreasing")
        signer_key_id_digest = domain_digest(
            "okg-external-action-execution-epoch-signing-key-id-v1",
            {"kid": kid, "purpose": "execution_epoch_lease"},
        )
        checked.append(
            VerifiedLease(
                dict(item),
                payload,
                digest,
                signer_key_id_digest,
            )
        )
        envelope_digests.append(digest)
    for prior, current in zip(checked, checked[1:]):
        if current.epoch == prior.epoch:
            if current.sequence != prior.sequence + 1 or current.payload["previous_lease_digest"] != prior.envelope_digest or current.payload["previous_epoch_digest"] != prior.payload["previous_epoch_digest"]:
                raise RestoreFenceError("same-epoch lease successor is not contiguous")
        elif current.epoch > prior.epoch:
            if current.sequence != 1 or current.payload["previous_lease_digest"] is not None or current.payload["previous_epoch_digest"] != prior.envelope_digest:
                raise RestoreFenceError("epoch rotation predecessor is invalid")
        else:
            raise RestoreFenceError("lease epoch rolled back")
    final = checked[-1]
    if retained:
        retained_sorted = sorted(retained, key=lambda lease: (lease.epoch, lease.sequence))
        retained_digests = [lease.envelope_digest for lease in retained_sorted]
        file_digests = [lease.envelope_digest for lease in checked]
        # A supplied chain may contain the byte-identical retained prefix or
        # begin directly at its first unretained successor.  It may not skip
        # a retained row, include a non-prefix row, or replace a retained
        # digest at the same epoch/sequence.
        common = min(len(retained_sorted), len(checked))
        if any(retained_sorted[i].envelope_digest != checked[i].envelope_digest for i in range(common)):
            anchor = retained_sorted[-1]
            first = checked[0]
            contiguous_suffix = (
                first.epoch == anchor.epoch
                and first.sequence == anchor.sequence + 1
                and first.payload["previous_lease_digest"] == anchor.envelope_digest
                and first.payload["previous_epoch_digest"] == anchor.payload["previous_epoch_digest"]
            ) or (
                first.epoch > anchor.epoch
                and first.sequence == 1
                and first.payload["previous_lease_digest"] is None
                and first.payload["previous_epoch_digest"] == anchor.envelope_digest
            )
            if not (
                len(checked) >= 1
                and contiguous_suffix
            ):
                raise RestoreFenceError("lease chain is not a retained prefix or contiguous suffix")
        elif len(checked) < len(retained_sorted):
            raise RestoreFenceError("lease chain omits retained history")
        elif len(checked) == len(retained_sorted) and file_digests != retained_digests:
            raise RestoreFenceError("lease chain retained rows are not byte-identical")
        elif len(checked) > len(retained_sorted) and file_digests[: len(retained_digests)] != retained_digests:
            raise RestoreFenceError("lease chain retained rows are not a prefix")
    skew = getattr(getattr(config, "authentication", None), "clock_skew_seconds", None)
    if skew is None:
        skew = config.get("maximum_clock_skew_seconds", 30) if isinstance(config, Mapping) else 30
    if clock + timedelta(seconds=int(skew)) < final.issued_at or clock >= final.expires_at:
        raise RestoreFenceError("current lease is not valid at database time")
    chain_value = {"schema": CHAIN_SCHEMA, "deployment": chain["deployment"], "lease_envelope_digests": envelope_digests}
    return tuple(checked), domain_digest(CHAIN_DOMAIN, chain_value)


def _call_store(store: Any, name: str, *args: Any, **kwargs: Any) -> Any:
    fn = getattr(store, name, None)
    if fn is None:
        return None
    return fn(*args, **kwargs)


def _record_digest(record: Any, field: str) -> Any:
    """Read a retained trust digest without inventing a legacy fallback."""

    if isinstance(record, Mapping):
        return record.get(field)
    return getattr(record, field, None)


def _validate_restart_trust_context(
    *,
    retained: Sequence[VerifiedLease],
    old_fence: Mapping[str, Any] | None,
    verification_config_digest: str,
    login_binding_digest: str,
    has_unretained_suffix: bool,
) -> None:
    """Bind a restart to the exact persisted trust context.

    The startup config and login binding are part of the authority that
    authenticated the retained lease history.  An exact replay therefore has
    to use the same two digests.  The contract permits a newly signed
    contiguous successor to establish a changed context; the successor is
    still verified by :func:`verify_lease_chain` before this function runs.
    """

    if not retained and old_fence is None:
        return
    if old_fence is not None and not retained:
        raise RestoreFenceError(
            "restore fence exists without retained lease history"
        )
    if old_fence is None:
        raise RestoreFenceError(
            "retained lease history has no restore-fence trust context"
        )

    stored_config = old_fence.get("verification_config_digest")
    stored_binding = old_fence.get("verification_login_binding_digest")
    if not isinstance(stored_config, str) or not isinstance(stored_binding, str):
        raise RestoreFenceError(
            "retained restore fence has no complete trust context"
        )
    # A database projection must contain the exact universal-hash form.  Do
    # not accept an empty, legacy, or caller-provided alternate digest.
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", stored_config) or not re.fullmatch(
        r"sha256:[0-9a-f]{64}", stored_binding
    ):
        raise RestoreFenceError("retained restore fence trust context is invalid")

    # Every lease row produced by the startup gate carries the same context.
    # Some compatibility projections expose only the VerifiedLease envelope;
    # when the fields are present, enforce them rather than silently choosing
    # a fallback.  The restore fence remains the authoritative restart anchor.
    for lease in retained:
        lease_config = _record_digest(lease, "verification_config_digest")
        lease_binding = _record_digest(
            lease, "verification_login_binding_digest"
        )
        if lease_config is not None and lease_config != stored_config:
            raise RestoreFenceError("retained lease verification config is inconsistent")
        if lease_binding is not None and lease_binding != stored_binding:
            raise RestoreFenceError("retained lease login binding is inconsistent")

    if has_unretained_suffix:
        # A changed trust context is valid only when the supplied signed file
        # contains an actually new contiguous lease.  Its current digests are
        # persisted by the same transaction below, replacing the old fence
        # projection only after all chain checks have passed.
        return

    if stored_config != verification_config_digest:
        raise RestoreFenceError(
            "verification config changed without a signed lease successor"
        )
    if stored_binding != login_binding_digest:
        raise RestoreFenceError(
            "login binding changed without a signed lease successor"
        )


def _has_external_action_state(store: Any, deployment: str | None) -> bool:
    """Detect pre-existing state when the lease history is unexpectedly empty."""

    counts = getattr(store, "counts", None)
    if callable(counts):
        value = counts()
        if isinstance(value, Mapping) and any(isinstance(item, int) and item > 0 for item in value.values()):
            return True
    for name in ("all_proposals", "all_events", "all_executions", "all_outcomes"):
        method = getattr(store, name, None)
        if callable(method):
            try:
                values = method(deployment=deployment)
            except TypeError:
                values = method()
            if values:
                return True
    for name in ("proposals", "executions", "outcomes", "events", "restore_fences", "restore_attestations", "restore_sessions", "restore_recoveries"):
        value = getattr(store, name, None)
        if value:
            try:
                if len(value):
                    return True
            except TypeError:
                return True
    return False


def pre_bind_restore_fence(
    source: bytes | str | Path | Mapping[str, Any],
    *,
    config: Any,
    store: Any = None,
    deployment: str | None = None,
    now: datetime | None = None,
    verification_config: Mapping[str, Any] | None = None,
    login_binding: Mapping[str, Any] | None = None,
) -> StartupResult:
    """Perform the pre-listener transaction and return its persisted mode."""

    if store is not None:
        required_methods = ("retained_leases", "restore_fence", "insert_lease", "set_restore_fence", "transaction")
        if any(not callable(getattr(store, name, None)) for name in required_methods):
            raise RestoreFenceError("durable restore-fence transaction is unavailable")

    config_digest = domain_digest("okg-external-action-startup-verification-config-v1", verification_config or {})
    binding_digest = domain_digest("okg-external-action-startup-login-binding-v1", login_binding or {})
    retained = tuple(_call_store(store, "retained_leases", deployment=deployment) or ()) if store is not None else ()
    leases, chain_digest = verify_lease_chain(
        source,
        config=config,
        deployment=deployment,
        now=now,
        verification_config_digest=config_digest,
        login_binding_digest=binding_digest,
        retained=retained,
    )
    if not retained:
        first = leases[0]
        if first.sequence != 1 or first.payload.get("previous_lease_digest") is not None or first.payload.get("previous_epoch_digest") is not None:
            raise RestoreFenceError("first-use lease history must begin with a genesis lease")
        if store is not None and _has_external_action_state(store, deployment):
            raise RestoreFenceError("external-action state exists without retained lease history")
    old_fence = _call_store(store, "restore_fence", deployment=deployment) if store is not None else None
    if old_fence is not None and (not isinstance(old_fence, Mapping) or old_fence.get("service_mode", "normal") not in {"normal", "restore_only"}):
        raise RestoreFenceError("retained restore fence has an invalid service mode")
    retained_digests = {
        _record_digest(lease, "envelope_digest") for lease in retained
    }
    has_unretained_suffix = any(
        lease.envelope_digest not in retained_digests for lease in leases
    )
    _validate_restart_trust_context(
        retained=retained,
        old_fence=old_fence,
        verification_config_digest=config_digest,
        login_binding_digest=binding_digest,
        has_unretained_suffix=has_unretained_suffix,
    )
    prior_epoch = max((lease.epoch for lease in retained), default=None)
    highest = leases[-1]
    # A first-use file containing more than its single genesis lease has no
    # retained database anchor. Even same-epoch renewals need restore-only
    # until the deployment has reconciled the complete external history;
    # only the lone genesis lease may start in normal mode.
    higher = (prior_epoch is not None and highest.epoch > prior_epoch) or (
        prior_epoch is None and len(leases) > 1
    )
    inserted = 0
    transaction = _call_store(store, "transaction") if store is not None else None
    with contextlib.nullcontext(transaction) if transaction is not None and hasattr(transaction, "__enter__") else contextlib.nullcontext():
        for lease in leases:
            if any(getattr(old, "envelope_digest", None) == lease.envelope_digest for old in retained):
                continue
            if store is not None:
                _call_store(
                    store,
                    "insert_lease",
                    lease=lease,
                    signer_key_id_digest=lease.signer_key_id_digest,
                    verification_source="signed_startup_gate",
                    verification_config_digest=config_digest,
                    verification_login_binding_digest=binding_digest,
                    lease_chain_digest=chain_digest,
                )
            inserted += 1
        mode = "restore_only" if higher else ((old_fence or {}).get("service_mode", "normal") if isinstance(old_fence, Mapping) else "normal")
        quarantine = mode == "restore_only"
        if store is not None:
            # ``quarantine_state`` is a persisted database enum, not the
            # public health label.  The HTTP adapter may present
            # ``service_mode=restore_only`` as quarantined, while the fixed
            # startup RPC accepts only ``blocked`` or ``clear``.
            _call_store(store, "set_restore_fence", deployment=deployment, highest_accepted_epoch=highest.epoch, lease_digest=highest.envelope_digest, service_mode=mode, quarantine_state="blocked" if quarantine else "clear", lease_chain_digest=chain_digest, verification_config_digest=config_digest, verification_login_binding_digest=binding_digest)
        commit = getattr(transaction, "commit", None)
        if callable(commit):
            commit()
    return StartupResult(deployment=leases[-1].payload["deployment"], highest_epoch=highest.epoch, highest_sequence=highest.sequence, lease_chain_digest=chain_digest, verification_config_digest=config_digest, verification_login_binding_digest=binding_digest, service_mode=mode, inserted_count=inserted, quarantined=quarantine)


def merkle_leaf(effect_instance_key: str, ledger_entry_digest: str) -> str:
    return domain_digest("okg-external-action-restore-ledger-leaf-v1", {"effect_instance_key": effect_instance_key, "ledger_entry_digest": ledger_entry_digest})


def merkle_root(entries: Iterable[tuple[str, str]]) -> str:
    rows = sorted(entries, key=lambda pair: pair[0].encode("utf-8"))
    if len({key for key, _ in rows}) != len(rows):
        raise RestoreFenceError("restore ledger contains duplicate effect keys", code="invalid_restore_checkpoint")
    level = [merkle_leaf(*row) for row in rows]
    if not level:
        return domain_digest("okg-external-action-restore-empty-root-v1", [])
    while len(level) > 1:
        next_level: list[str] = []
        for index in range(0, len(level), 2):
            right = level[index + 1] if index + 1 < len(level) else None
            next_level.append(domain_digest("okg-external-action-restore-merkle-node-v1", {"left_digest": level[index], "right_digest": right}))
        level = next_level
    return level[0]


def resolve_restore_reference(
    reference: Any,
    *,
    resolver: Callable[..., Any] | Mapping[str, Any] | None,
    purpose: str,
) -> Any:
    """Resolve one deployment-owned evidence reference.

    References are opaque identifiers into a configured restore authority.
    They are never interpreted as paths, URLs, or request-provided envelope
    objects.  A mapping resolver is useful only for deterministic tests and
    remains exact-keyed and purpose-bound.
    """

    # The enclosing API caps the whole JSON body at 256 KiB.  A full signed
    # worker checkpoint is legitimately several KiB, so a 512-byte locator
    # limit would make the self-contained production grammar unusable.  Keep
    # a per-reference bound for direct callers and cap decoded evidence too.
    if not isinstance(reference, str) or not 1 <= len(reference) <= 262_144:
        raise RestoreFenceError("restore evidence reference is invalid", code="invalid_restore_checkpoint")
    if any(ord(char) <= 0x20 or ord(char) >= 0x7f for char in reference) or any(token in reference for token in ("..", "\\", "//", "@")):
        raise RestoreFenceError("restore evidence reference is not deployment-bound", code="invalid_restore_checkpoint")
    if resolver is None:
        # Production may carry a compact, self-contained evidence reference;
        # the signed envelope/proof itself remains the authority.  Fixture
        # references (for example ``hash-vectors.yaml#...``) require the
        # explicitly injected resolver and can never select a local path.
        prefix = "jws:" if purpose in _RESTORE_ENVELOPE_SPECS and purpose != "positive_proof" and purpose != "nonmembership_proof" else "proof:"
        if not reference.startswith(prefix):
            raise RestoreFenceError(
                "restore evidence authority is unavailable",
                code="review_authority_unavailable",
            )
        try:
            encoded = reference[len(prefix):]
            raw = b64url_decode(encoded, field="restore_reference")
            if len(raw) > 262_144:
                raise RestoreFenceError("restore evidence exceeds 256 KiB", code="review_request_too_large")
            value = strict_json_loads(raw)
        except (SigningError, RestoreFenceError) as exc:
            raise RestoreFenceError("restore evidence reference is invalid", code="invalid_restore_checkpoint") from exc
        if not isinstance(value, Mapping):
            raise RestoreFenceError("restore evidence is not an object", code="invalid_restore_checkpoint")
        return dict(value)
    try:
        if isinstance(resolver, Mapping):
            value = resolver.get((purpose, reference), resolver.get(reference))
        else:
            import inspect

            parameters = inspect.signature(resolver).parameters
            if any(parameter.kind == inspect.Parameter.VAR_KEYWORD for parameter in parameters.values()) or "purpose" in parameters:
                value = resolver(reference, purpose=purpose)
            else:
                value = resolver(reference)
    except RestoreFenceError:
        raise
    except Exception as exc:
        raise RestoreFenceError(
            "restore evidence authority is unavailable",
            code="review_authority_unavailable",
        ) from exc
    if value is None:
        raise RestoreFenceError("restore evidence reference is unavailable", code="invalid_restore_checkpoint")
    if isinstance(value, (bytes, str)):
        if isinstance(value, bytes) and len(value) > 262_144:
            raise RestoreFenceError("restore evidence exceeds 256 KiB", code="review_request_too_large")
        if isinstance(value, str) and len(value.encode("utf-8")) > 262_144:
            raise RestoreFenceError("restore evidence exceeds 256 KiB", code="review_request_too_large")
        try:
            value = strict_json_loads(value)
        except SigningError as exc:
            raise RestoreFenceError("restore evidence is not strict I-JSON", code="invalid_restore_checkpoint") from exc
    if not isinstance(value, Mapping):
        raise RestoreFenceError("restore evidence is not an object", code="invalid_restore_checkpoint")
    return dict(value)


def _check_count(value: Any, *, name: str, maximum: int = 2**31 - 1) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= maximum:
        raise RestoreFenceError(f"restore {name} is invalid", code="invalid_restore_checkpoint")
    return value


def _check_digest(value: Any, *, name: str, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", value):
        raise RestoreFenceError(f"restore {name} is invalid", code="invalid_restore_checkpoint")
    return value


def _positive_proof(
    proof: Mapping[str, Any], *, effect_key: str, root: str, count: int
) -> Mapping[str, Any]:
    if set(proof) != {"effect_instance_key", "ledger_entry_digest", "leaf_index", "leaf_count", "sibling_steps"}:
        raise RestoreFenceError("positive restore proof has extra or missing fields", code="invalid_restore_checkpoint")
    if proof.get("effect_instance_key") != effect_key:
        raise RestoreFenceError("positive restore proof effect key does not match checkpoint", code="invalid_restore_checkpoint")
    if count < 1:
        raise RestoreFenceError("positive restore proof cannot target an empty fence", code="invalid_restore_checkpoint")
    ledger_digest = _check_digest(proof.get("ledger_entry_digest"), name="ledger entry digest")
    leaf_index = _check_count(proof.get("leaf_index"), name="leaf index", maximum=max(count - 1, 0))
    if proof.get("leaf_count") != count:
        raise RestoreFenceError("positive restore proof count does not match fence", code="invalid_restore_checkpoint")
    steps = proof.get("sibling_steps")
    if not isinstance(steps, list) or len(steps) > 64:
        raise RestoreFenceError("positive restore proof steps are invalid", code="invalid_restore_checkpoint")
    value = merkle_leaf(effect_key, ledger_digest)
    index = leaf_index
    width = count
    for step in steps:
        if width <= 1:
            raise RestoreFenceError("positive restore proof has extra levels", code="invalid_restore_checkpoint")
        if not isinstance(step, Mapping) or set(step) != {"side", "digest"} or step.get("side") not in {"left", "right"}:
            raise RestoreFenceError("positive restore proof step is invalid", code="invalid_restore_checkpoint")
        sibling = _check_digest(step.get("digest"), name="proof sibling digest", nullable=True)
        side = step["side"]
        expected_side = "left" if index % 2 else "right"
        if side != expected_side:
            raise RestoreFenceError("positive restore proof step direction is invalid", code="invalid_restore_checkpoint")
        unpaired_right = side == "right" and index + 1 == width
        if side == "right" and sibling is None and unpaired_right:
            # This is the canonical unpaired-right marker.  It is legal only
            # when the current node is the last odd node at this level.
            pass
        elif sibling is None:
            raise RestoreFenceError("null proof sibling is only valid on the right", code="invalid_restore_checkpoint")
        if not unpaired_right and sibling is None:
            raise RestoreFenceError("proof sibling is missing", code="invalid_restore_checkpoint")
        if sibling is None:
            right = None
            left = value
        elif side == "left":
            left, right = sibling, value
        else:
            left, right = value, sibling
        value = domain_digest("okg-external-action-restore-merkle-node-v1", {"left_digest": left, "right_digest": right})
        index //= 2
        width = (width + 1) // 2
    if width != 1:
        raise RestoreFenceError("positive restore proof is incomplete", code="invalid_restore_checkpoint")
    if value != root:
        raise RestoreFenceError("positive restore proof root does not match fence", code="invalid_restore_checkpoint")
    return {"effect_instance_key": effect_key, "ledger_entry_digest": ledger_digest, "leaf_index": leaf_index, "leaf_count": count, "sibling_steps": [dict(step) for step in steps]}


def verify_restore_proof(
    proof: Any,
    *,
    proof_kind: str,
    effect_key: str,
    root: str,
    count: int,
) -> Mapping[str, Any]:
    """Verify a positive or nonmembership proof against one fenced root."""

    if not isinstance(proof, Mapping):
        raise RestoreFenceError("restore proof is not an object", code="invalid_restore_checkpoint")
    if proof_kind == "positive":
        return _positive_proof(proof, effect_key=effect_key, root=root, count=count)
    if proof_kind != "nonmembership":
        raise RestoreFenceError("restore proof kind is invalid", code="invalid_restore_checkpoint")
    if set(proof) != {"effect_instance_key", "leaf_count", "lower_neighbor_positive_proof", "upper_neighbor_positive_proof"}:
        raise RestoreFenceError("nonmembership proof has extra or missing fields", code="invalid_restore_checkpoint")
    if proof.get("effect_instance_key") != effect_key or proof.get("leaf_count") != count:
        raise RestoreFenceError("nonmembership proof does not match fence", code="invalid_restore_checkpoint")
    lower = proof.get("lower_neighbor_positive_proof")
    upper = proof.get("upper_neighbor_positive_proof")
    if lower is None and upper is None:
        if count != 0 or root != domain_digest("okg-external-action-restore-empty-root-v1", []):
            raise RestoreFenceError("empty nonmembership proof is not for an empty fence", code="invalid_restore_checkpoint")
        return {"effect_instance_key": effect_key, "leaf_count": count, "lower_neighbor_positive_proof": None, "upper_neighbor_positive_proof": None}
    lower_verified = _positive_proof(lower, effect_key=lower.get("effect_instance_key") if isinstance(lower, Mapping) else "", root=root, count=count) if lower is not None else None
    upper_verified = _positive_proof(upper, effect_key=upper.get("effect_instance_key") if isinstance(upper, Mapping) else "", root=root, count=count) if upper is not None else None
    if lower_verified is None:
        if upper_verified is None or upper_verified["leaf_index"] != 0 or not effect_key.encode("utf-8") < upper_verified["effect_instance_key"].encode("utf-8"):
            raise RestoreFenceError("nonmembership lower boundary is invalid", code="invalid_restore_checkpoint")
    elif upper_verified is None:
        if lower_verified["leaf_index"] != count - 1 or not lower_verified["effect_instance_key"].encode("utf-8") < effect_key.encode("utf-8"):
            raise RestoreFenceError("nonmembership upper boundary is invalid", code="invalid_restore_checkpoint")
    elif lower_verified["leaf_index"] + 1 != upper_verified["leaf_index"] or not lower_verified["effect_instance_key"].encode("utf-8") < effect_key.encode("utf-8") < upper_verified["effect_instance_key"].encode("utf-8"):
        raise RestoreFenceError("nonmembership neighbors are not adjacent", code="invalid_restore_checkpoint")
    return {
        "effect_instance_key": effect_key,
        "leaf_count": count,
        "lower_neighbor_positive_proof": dict(lower_verified) if lower_verified is not None else None,
        "upper_neighbor_positive_proof": dict(upper_verified) if upper_verified is not None else None,
    }


__all__ = [
    "EXTERNAL_ACTION_RESTORE_REFUSAL_CODE",
    "EXTERNAL_ACTION_ROLLOUT_DISABLED",
    "EXTERNAL_ACTION_ROLLOUT_ENABLED",
    "EXTERNAL_ACTION_ROLLOUT_BLOCKED",
    "RestoreFenceError",
    "VerifiedLease",
    "VerifiedRestoreEnvelope",
    "StartupResult",
    "verify_lease_chain",
    "verify_restore_envelope",
    "resolve_restore_reference",
    "verify_restore_proof",
    "external_action_rollout_state",
    "pre_bind_restore_fence",
    "merkle_leaf",
    "merkle_root",
    "CHAIN_SCHEMA",
]
