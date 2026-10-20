"""Disposable SQLite refinement target for the three-action activation seam.

This is executable protocol evidence, not the product's canonical Chronos outcome
store. Production supplies a TargetPublicationPort adapter from outside this package.
"""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Mapping
from contextlib import closing
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .protocol import (
    ActivationReceipt,
    AuthoritySigner,
    MutationCapability,
    ObservationProof,
    ReleaseDescriptor,
    canonical_json,
    digest_json,
)


class ReferenceTargetError(RuntimeError):
    pass


_SCHEMA = """
CREATE TABLE IF NOT EXISTS target_authority (
    deployment TEXT PRIMARY KEY,
    current_generation TEXT NOT NULL,
    database_writer_epoch INTEGER NOT NULL,
    activation_receipt_digest TEXT,
    lifecycle_activation_epoch INTEGER NOT NULL DEFAULT 0,
    port_manifest_digest TEXT,
    service_plan_digest TEXT,
    visibility_switches INTEGER NOT NULL DEFAULT 0,
    service_status TEXT NOT NULL DEFAULT 'healthy',
    publication_visible INTEGER NOT NULL DEFAULT 1,
    retired INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS staged_generations (
    staged_digest TEXT PRIMARY KEY,
    deployment TEXT NOT NULL,
    transition_id TEXT NOT NULL,
    candidate_digest TEXT NOT NULL,
    effect_set_digest TEXT NOT NULL,
    capability_json TEXT NOT NULL,
    visible INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS reference_publish_outcomes (
    receipt_digest TEXT PRIMARY KEY,
    deployment TEXT NOT NULL,
    predecessor TEXT NOT NULL,
    generation_digest TEXT NOT NULL,
    database_writer_epoch INTEGER NOT NULL,
    outcome_json TEXT NOT NULL
);
"""


class SQLiteReferenceTarget:
    def __init__(
        self,
        *,
        path: str | Path,
        signer: AuthoritySigner,
        deployment: str,
        predecessor: str,
        database_writer_epoch: int,
        activation_receipt_digest: str | None = None,
        service_plan_digest: str | None = None,
    ) -> None:
        self.path = Path(path).resolve()
        self.signer = signer
        self.deployment = deployment
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(self._connect()) as conn, conn:
            conn.executescript(_SCHEMA)
            conn.execute(
                "INSERT OR IGNORE INTO target_authority VALUES "
                "(?, ?, ?, ?, 0, NULL, ?, 0, 'healthy', 1, 0)",
                (
                    deployment,
                    predecessor,
                    database_writer_epoch,
                    activation_receipt_digest,
                    service_plan_digest,
                ),
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    def stage_generation(
        self,
        *,
        staged_digest: str,
        candidate_digest: str,
        capability: MutationCapability,
    ) -> None:
        if capability.mode != "staged":
            raise ReferenceTargetError("staging requires a staged-only capability")
        if (
            capability.activation_receipt_digest is not None
            or capability.provisional_plan_digest is None
        ):
            raise ReferenceTargetError("staged capability must bind a provisional plan")
        if capability.expires_at is None or capability.expires_at <= int(time.time()):
            raise ReferenceTargetError("staged capability is expired")
        if not self.signer.verify(capability.unsigned_payload(), capability.token):
            raise ReferenceTargetError("staged capability signature is invalid")
        with closing(self._connect()) as conn, conn:
            existing = conn.execute(
                "SELECT * FROM staged_generations WHERE staged_digest=?",
                (staged_digest,),
            ).fetchone()
            exact = (
                self.deployment,
                capability.transition_id,
                candidate_digest,
                capability.effect_set_digest,
            )
            if existing is not None:
                observed = (
                    existing["deployment"],
                    existing["transition_id"],
                    existing["candidate_digest"],
                    existing["effect_set_digest"],
                )
                if observed != exact:
                    raise ReferenceTargetError(
                        "staged digest is bound to another attempt"
                    )
                return
            conn.execute(
                "INSERT INTO staged_generations VALUES (?, ?, ?, ?, ?, ?, 0)",
                (
                    staged_digest,
                    *exact,
                    canonical_json(asdict(capability)),
                ),
            )

    def publish_authorized_generation(
        self, receipt: ActivationReceipt
    ) -> Mapping[str, Any]:
        if not self.signer.verify(receipt.unsigned_payload(), receipt.signature):
            raise ReferenceTargetError("activation receipt signature is invalid")
        if receipt.deployment != self.deployment:
            raise ReferenceTargetError("activation receipt targets another deployment")
        with closing(self._connect()) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            existing = conn.execute(
                "SELECT * FROM reference_publish_outcomes "
                "WHERE receipt_digest=?",
                (receipt.digest,),
            ).fetchone()
            if existing is not None:
                authority = conn.execute(
                    "SELECT * FROM target_authority WHERE deployment=?",
                    (self.deployment,),
                ).fetchone()
                cached = json.loads(existing["outcome_json"])
                if (
                    authority["current_generation"]
                    != receipt.staged_generation_digest
                    or authority["activation_receipt_digest"] != receipt.digest
                    or authority["database_writer_epoch"]
                    != receipt.database_writer_epoch
                    or existing["deployment"] != self.deployment
                    or existing["predecessor"] != receipt.expected_predecessor
                    or existing["generation_digest"]
                    != receipt.staged_generation_digest
                    or existing["database_writer_epoch"]
                    != receipt.database_writer_epoch
                    or cached.get("publication_transaction_digest") is None
                ):
                    conn.rollback()
                    raise ReferenceTargetError(
                        "cached publication disagrees with live target authority"
                    )
                conn.rollback()
                observed_at = int(time.time())
                recovered = {
                    "schema": "okg.local-controller.publication-observation/v1",
                    "activation_receipt_digest": receipt.digest,
                    "generation_digest": receipt.staged_generation_digest,
                    "lifecycle_activation_epoch": receipt.lifecycle_activation_epoch,
                    "database_writer_epoch": receipt.database_writer_epoch,
                    "port_manifest_digest": receipt.port_manifest_digest,
                    "service_plan_digest": receipt.service_plan_digest,
                    "status": "published",
                    "observed_at": observed_at,
                    "expires_at": observed_at + 60,
                    "provenance": "sqlite-reference-target:publish-recovery-read",
                    "visibility_switches": authority["visibility_switches"],
                    "publication_transaction_digest": cached[
                        "publication_transaction_digest"
                    ],
                }
                return self._signed_publication(receipt, "publish", recovered)
            authority = conn.execute(
                "SELECT * FROM target_authority WHERE deployment=?",
                (self.deployment,),
            ).fetchone()
            if authority["current_generation"] != receipt.expected_predecessor:
                conn.rollback()
                raise ReferenceTargetError("expected predecessor CAS failed")
            if (
                authority["activation_receipt_digest"]
                != receipt.expected_activation_receipt_digest
            ):
                conn.rollback()
                raise ReferenceTargetError(
                    "expected predecessor activation receipt CAS failed"
                )
            if (
                authority["database_writer_epoch"]
                != receipt.expected_database_writer_epoch
            ):
                conn.rollback()
                raise ReferenceTargetError("database writer epoch CAS failed")
            staged = conn.execute(
                "SELECT * FROM staged_generations WHERE staged_digest=?",
                (receipt.staged_generation_digest,),
            ).fetchone()
            if staged is None:
                conn.rollback()
                raise ReferenceTargetError("authorized staged generation is absent")
            if (
                staged["transition_id"] != receipt.transition_id
                or staged["candidate_digest"] != receipt.candidate_digest
                or staged["effect_set_digest"] != receipt.effect_set_digest
            ):
                conn.rollback()
                raise ReferenceTargetError(
                    "staged generation disagrees with authorization"
                )
            observed_at = int(time.time())
            evidence = {
                "schema": "okg.local-controller.publication-observation/v1",
                "activation_receipt_digest": receipt.digest,
                "generation_digest": receipt.staged_generation_digest,
                "lifecycle_activation_epoch": receipt.lifecycle_activation_epoch,
                "database_writer_epoch": receipt.database_writer_epoch,
                "port_manifest_digest": receipt.port_manifest_digest,
                "service_plan_digest": receipt.service_plan_digest,
                "status": "published",
                "observed_at": observed_at,
                "expires_at": observed_at + 60,
                "provenance": "sqlite-reference-target:publish-cas",
                "visibility_switches": authority["visibility_switches"] + 1,
                "publication_transaction_digest": digest_json(
                    {
                        "receipt": receipt.digest,
                        "from": receipt.expected_predecessor,
                        "from_receipt": receipt.expected_activation_receipt_digest,
                        "to": receipt.staged_generation_digest,
                        "visibility_switches": authority["visibility_switches"] + 1,
                    }
                ),
            }
            outcome = self._signed_publication(receipt, "publish", evidence)
            conn.execute(
                "UPDATE staged_generations SET visible=1 WHERE staged_digest=?",
                (receipt.staged_generation_digest,),
            )
            conn.execute(
                "UPDATE target_authority SET current_generation=?, "
                "activation_receipt_digest=?, lifecycle_activation_epoch=?, "
                "database_writer_epoch=?, port_manifest_digest=?, "
                "service_plan_digest=?, "
                "service_status='healthy', publication_visible=1, retired=0, "
                "visibility_switches=visibility_switches+1 "
                "WHERE deployment=? AND current_generation=? "
                "AND database_writer_epoch=? AND activation_receipt_digest IS ?",
                (
                    receipt.staged_generation_digest,
                    receipt.digest,
                    receipt.lifecycle_activation_epoch,
                    receipt.database_writer_epoch,
                    receipt.port_manifest_digest,
                    receipt.service_plan_digest,
                    self.deployment,
                    receipt.expected_predecessor,
                    receipt.expected_database_writer_epoch,
                    receipt.expected_activation_receipt_digest,
                ),
            )
            if conn.execute("SELECT changes()").fetchone()[0] != 1:
                conn.rollback()
                raise ReferenceTargetError("target authority CAS lost a race")
            conn.execute(
                "INSERT INTO reference_publish_outcomes VALUES (?, ?, ?, ?, ?, ?)",
                (
                    receipt.digest,
                    self.deployment,
                    receipt.expected_predecessor,
                    receipt.staged_generation_digest,
                    receipt.database_writer_epoch,
                    canonical_json(outcome),
                ),
            )
            conn.commit()
            return outcome

    def observe_authorized_generation(
        self, receipt: ActivationReceipt
    ) -> Mapping[str, Any]:
        with closing(self._connect()) as conn, conn:
            authority = conn.execute(
                "SELECT * FROM target_authority WHERE deployment=?",
                (self.deployment,),
            ).fetchone()
            outcome = conn.execute(
                "SELECT outcome_json FROM reference_publish_outcomes "
                "WHERE receipt_digest=?",
                (receipt.digest,),
            ).fetchone()
        if (
            outcome is None
            or authority["current_generation"] != receipt.staged_generation_digest
            or authority["activation_receipt_digest"] != receipt.digest
            or authority["database_writer_epoch"] != receipt.database_writer_epoch
        ):
            raise ReferenceTargetError("exact authorized publication is not observable")
        observed_at = int(time.time())
        evidence = {
            "schema": "okg.local-controller.publication-observation/v1",
            "activation_receipt_digest": receipt.digest,
            "generation_digest": authority["current_generation"],
            "lifecycle_activation_epoch": authority["lifecycle_activation_epoch"],
            "database_writer_epoch": authority["database_writer_epoch"],
            "port_manifest_digest": authority["port_manifest_digest"],
            "service_plan_digest": authority["service_plan_digest"],
            "status": "ready",
            "observed_at": observed_at,
            "expires_at": observed_at + 60,
            "provenance": "sqlite-reference-target:authority-read",
            "visibility_switches": authority["visibility_switches"],
            "publication_transaction_digest": json.loads(outcome["outcome_json"])[
                "publication_transaction_digest"
            ],
        }
        return self._signed_publication(receipt, "ready", evidence)

    @staticmethod
    def _publication_subject(receipt: ActivationReceipt, phase: str) -> str:
        return digest_json(
            {
                "schema": "okg.local-controller.publication-subject/v1",
                "phase": phase,
                "receipt": asdict(receipt),
            }
        )

    def _sign_observation(
        self,
        *,
        subject_kind: str,
        subject_digest: str,
        evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        proof = ObservationProof.issue(
            signer=self.signer.signer_for("observation"),
            subject_kind=subject_kind,
            subject_digest=subject_digest,
            evidence=evidence,
            nonce=digest_json(
                {
                    "subject": subject_digest,
                    "evidence": digest_json(evidence),
                    "visibility": evidence.get("visibility_switches", 0),
                }
            ),
        )
        return {**dict(evidence), "proof": asdict(proof)}

    def _signed_publication(
        self,
        receipt: ActivationReceipt,
        phase: str,
        evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        return self._sign_observation(
            subject_kind="publication",
            subject_digest=self._publication_subject(receipt, phase),
            evidence=evidence,
        )

    def observe_current_state(self, descriptor: ReleaseDescriptor) -> Mapping[str, Any]:
        with closing(self._connect()) as conn:
            authority = conn.execute(
                "SELECT * FROM target_authority WHERE deployment=?",
                (self.deployment,),
            ).fetchone()
        observed_at = int(time.time())
        evidence = {
            "schema": "okg.local-controller.no-op-observation/v1",
            "candidate_digest": descriptor.candidate_digest,
            "activation_receipt_digest": authority["activation_receipt_digest"],
            "generation_digest": authority["current_generation"],
            "database_writer_epoch": authority["database_writer_epoch"],
            "service_plan_digest": authority["service_plan_digest"],
            "status": "ready" if authority["service_status"] == "healthy" else "down",
            "health": authority["service_status"],
            "publication_visible": bool(authority["publication_visible"]),
            "observed_at": observed_at,
            "expires_at": observed_at + 60,
            "provenance": "sqlite-reference-target:live-no-op-read",
        }
        subject = digest_json(
            {
                "schema": "okg.local-controller.no-op-subject/v1",
                "descriptor": descriptor.payload(),
            }
        )
        return self._sign_observation(
            subject_kind="no_op", subject_digest=subject, evidence=evidence
        )

    def observe_retirement(self, receipt: ActivationReceipt) -> Mapping[str, Any]:
        with closing(self._connect()) as conn:
            authority = conn.execute(
                "SELECT * FROM target_authority WHERE deployment=?",
                (self.deployment,),
            ).fetchone()
        observed_at = int(time.time())
        evidence = {
            "schema": "okg.local-controller.retirement-observation/v1",
            "activation_receipt_digest": receipt.digest,
            "generation_digest": authority["current_generation"],
            "services_stopped": authority["service_status"] == "stopped",
            "queue_drained": True,
            "publication_visible": bool(authority["publication_visible"]),
            "capability_withdrawn": bool(authority["retired"]),
            "observed_at": observed_at,
            "expires_at": observed_at + 60,
            "provenance": "sqlite-reference-target:retirement-read",
        }
        subject = digest_json(
            {
                "schema": "okg.local-controller.retirement-subject/v1",
                "receipt": asdict(receipt),
            }
        )
        return self._sign_observation(
            subject_kind="retirement", subject_digest=subject, evidence=evidence
        )

    def mark_retired(self, receipt: ActivationReceipt) -> None:
        """Reference-only retirement effect used by executable refinement tests."""
        with closing(self._connect()) as conn, conn:
            changed = conn.execute(
                "UPDATE target_authority SET service_status='stopped', "
                "publication_visible=0, retired=1 WHERE deployment=? "
                "AND activation_receipt_digest=?",
                (self.deployment, receipt.digest),
            ).rowcount
            if changed != 1:
                raise ReferenceTargetError("retirement target receipt is not current")

    def authority(self) -> dict[str, Any]:
        with closing(self._connect()) as conn, conn:
            return dict(
                conn.execute(
                    "SELECT * FROM target_authority WHERE deployment=?",
                    (self.deployment,),
                ).fetchone()
            )

    def advance_database_writer_epoch(self, *, expected: int, new: int) -> None:
        """Model the separately owned writer-fence result for refinement tests."""
        if new <= expected:
            raise ReferenceTargetError("database writer epoch must advance")
        with closing(self._connect()) as conn, conn:
            cursor = conn.execute(
                "UPDATE target_authority SET database_writer_epoch=? "
                "WHERE deployment=? AND database_writer_epoch=?",
                (new, self.deployment, expected),
            )
            if cursor.rowcount != 1:
                raise ReferenceTargetError("database writer epoch CAS failed")
