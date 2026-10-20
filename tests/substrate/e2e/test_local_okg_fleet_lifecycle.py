"""Disposable-database refinement proof for the external lifecycle controller."""

from __future__ import annotations

import json
import sqlite3
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Mapping

import pytest


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "controller"))

from okg_local_controller.controller import LocalLifecycleController  # noqa: E402
from okg_local_controller.effects import (  # noqa: E402
    CompensationContext,
    EffectContext,
    EffectObservation,
    InjectedControllerCrash,
)
from okg_local_controller.ledger import ControllerLedger  # noqa: E402
from okg_local_controller.protocol import (  # noqa: E402
    ActivationReceipt,
    AuthoritySigner,
    CANONICAL_SAGA_EFFECTS,
    ControllerInstallation,
    EffectSpec,
    MutationCapability,
    ObservationProof,
    ProbeAttestation,
    ProbeResult,
    ReleaseDescriptor,
    SMOKE_OUTCOME_SCHEMA,
    UPGRADE_EFFECT_KIND,
    UPGRADE_EFFECT_SCHEMA,
    UpgradeAttestation,
    digest_json,
    rollback_generation_digest,
)
from okg_local_controller.reference_target import SQLiteReferenceTarget  # noqa: E402


KEY = b"controller-e2e-authority-key-32-bytes-minimum"
DIGESTS = tuple(f"sha256:{character * 64}" for character in "abcdef0123456789")


class DurableFixtureEffect:
    """A real SQLite effect whose observation survives controller restart."""

    def __init__(self, path: Path, signer: AuthoritySigner) -> None:
        self.path = path
        self.signer = signer
        with sqlite3.connect(path) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS applied_effects ("
                "effect_key TEXT PRIMARY KEY, request_digest TEXT NOT NULL)"
            )

    def apply(self, effect: EffectContext) -> dict[str, Any]:
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO applied_effects VALUES (?, ?)",
                (effect.effect_key, effect.request_digest),
            )
        return {
            "effect_key": effect.effect_key,
            "request_digest": effect.request_digest,
        }

    def observe(self, effect: EffectContext) -> EffectObservation:
        with sqlite3.connect(self.path) as conn:
            row = conn.execute(
                "SELECT request_digest FROM applied_effects WHERE effect_key=?",
                (effect.effect_key,),
            ).fetchone()
        satisfied = row is not None and row[0] == effect.request_digest
        evidence = {"effect_key": effect.effect_key, "satisfied": satisfied}
        proof = ObservationProof.issue(
            signer=self.signer.signer_for("observation"),
            subject_kind="effect",
            subject_digest=effect.observation_subject_digest,
            evidence=evidence,
            nonce=f"e2e-{effect.transition_id}-{effect.effect_key}-{satisfied}",
        )
        return EffectObservation(
            evidence=evidence,
            proof=proof,
        )

    def compensate(self, effect: CompensationContext) -> dict[str, Any]:
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                "DELETE FROM applied_effects WHERE effect_key=?",
                (effect.forward_effect_key,),
            )
            conn.execute(
                "DELETE FROM upgrade_attestations WHERE effect_key=?",
                (effect.forward_effect_key,),
            ) if self._has_upgrade_table(conn) else None
        return {"compensation_key": effect.compensation_key, "applied": True}

    @staticmethod
    def _has_upgrade_table(conn: sqlite3.Connection) -> bool:
        return (
            conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' "
                "AND name='upgrade_attestations'"
            ).fetchone()
            is not None
        )

    def observe_compensation(
        self, effect: CompensationContext
    ) -> EffectObservation:
        with sqlite3.connect(self.path) as conn:
            row = conn.execute(
                "SELECT 1 FROM applied_effects WHERE effect_key=?",
                (effect.forward_effect_key,),
            ).fetchone()
        evidence = {
            "compensation_key": effect.compensation_key,
            "forward_effect_key": effect.forward_effect_key,
            "satisfied": row is None,
        }
        proof = ObservationProof.issue(
            signer=self.signer.signer_for("observation"),
            subject_kind="compensation",
            subject_digest=effect.observation_subject_digest,
            evidence=evidence,
            nonce=f"e2e-compensate-{effect.compensation_key}",
        )
        return EffectObservation(evidence=evidence, proof=proof)


class UpgradeFixtureEffect(DurableFixtureEffect):
    """Persist one exact upgrade attestation with the observed test effect."""

    def __init__(self, path: Path, signer: AuthoritySigner) -> None:
        super().__init__(path, signer)
        with sqlite3.connect(path) as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS upgrade_attestations ("
                "effect_key TEXT PRIMARY KEY, attestation_json TEXT NOT NULL)"
            )

    def apply(self, effect: EffectContext) -> dict[str, Any]:
        result = super().apply(effect)
        issued_at = int(time.time())
        request = effect.request
        attestation = UpgradeAttestation.issue(
            signer=self.signer.signer_for("upgrade"),
            nonce="nonce-" + effect.transition_id,
            issuer=request["issuer"],
            issued_at=issued_at,
            expires_at=issued_at + request["max_attestation_ttl_seconds"],
            environment_identity=request["environment_identity"],
            database_instance_identity=request["database_instance_identity"],
            artifact_digest=effect.candidate_digest,
            reconcile_plan_digest=effect.descriptor_digest,
            saga_transition_id=effect.transition_id,
            database_writer_epoch=effect.database_writer_epoch,
            backup_digest=effect.backup_digest,
            database_state_digest=DIGESTS[11],
            previous_artifact_digest=effect.predecessor_digest,
            candidate_artifact_digest=effect.candidate_digest,
            schema_before_digest=request["expected_schema_before_digest"],
            schema_after_digest=DIGESTS[12],
            migration_manifest_digest=request["migration_manifest_digest"],
            applied_migrations=("0002_controller.sql",),
            skipped_migrations=("0001_baseline.sql",),
            rollback_compatibility_class=request["rollback_compatibility_class"],
            restore_strategy=request["restore_strategy"],
            smoke_outcome={
                "schema": SMOKE_OUTCOME_SCHEMA,
                "status": "published",
                "generation_digest": DIGESTS[3],
                "no_work_reason": None,
            },
            result="success",
            no_op_reason=None,
        )
        with sqlite3.connect(self.path) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO upgrade_attestations VALUES (?, ?)",
                (effect.effect_key, json.dumps(attestation.payload(), sort_keys=True)),
            )
        return {**result, "attestation_id": attestation.attestation_id}

    def observe(self, effect: EffectContext) -> EffectObservation:
        applied = super().observe(effect)
        with sqlite3.connect(self.path) as conn:
            row = conn.execute(
                "SELECT attestation_json FROM upgrade_attestations WHERE effect_key=?",
                (effect.effect_key,),
            ).fetchone()
        evidence = {
            "satisfied": applied.satisfied and row is not None,
            "upgrade_attestation": json.loads(row[0]) if row is not None else None
        }
        proof = ObservationProof.issue(
            signer=self.signer.signer_for("observation"),
            subject_kind="effect",
            subject_digest=effect.observation_subject_digest,
            evidence=evidence,
            nonce=f"e2e-upgrade-{effect.transition_id}",
        )
        return EffectObservation(
            evidence=evidence,
            proof=proof,
        )


class PostgresChronosTarget:
    """Test-only real target: PostgreSQL CAS plus Chronos fast-forward."""

    def __init__(
        self,
        *,
        dsn: str,
        schema: str,
        signer: AuthoritySigner,
        deployment: str,
        predecessor: str | None = None,
        database_writer_epoch: int | None = None,
        predecessor_staged_digest: str | None = None,
        predecessor_node_id: str | None = None,
    ) -> None:
        import psycopg
        from psycopg import sql

        self.dsn = dsn
        self.schema = schema
        self.signer = signer
        self.deployment = deployment
        if predecessor is None or database_writer_epoch is None:
            return
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
            conn.execute(
                sql.SQL(
                    "CREATE TABLE {}.authority ("
                    "deployment text PRIMARY KEY, current_generation text NOT NULL, "
                    "database_writer_epoch bigint NOT NULL, receipt_digest text, "
                    "lifecycle_activation_epoch bigint NOT NULL DEFAULT 0, "
                    "port_manifest_digest text, service_plan_digest text, "
                    "visibility_switches integer NOT NULL DEFAULT 0)"
                ).format(sql.Identifier(schema))
            )
            conn.execute(
                sql.SQL(
                    "CREATE TABLE {}.staged ("
                    "staged_digest text PRIMARY KEY, transition_id text NOT NULL, "
                    "candidate_digest text NOT NULL, effect_set_digest text NOT NULL, "
                    "branch text NOT NULL, node_id text NOT NULL, "
                    "expected_default_segment bigint NOT NULL, published boolean NOT NULL DEFAULT false)"
                ).format(sql.Identifier(schema))
            )
            conn.execute(
                sql.SQL(
                    "CREATE TABLE {}.outcomes ("
                    "receipt_digest text PRIMARY KEY, outcome jsonb NOT NULL)"
                ).format(sql.Identifier(schema))
            )
            conn.execute(
                sql.SQL(
                    "CREATE TABLE {}.predecessor_snapshots ("
                    "staged_digest text PRIMARY KEY, branch text NOT NULL, "
                    "node_id text NOT NULL, source_generation text NOT NULL)"
                ).format(sql.Identifier(schema))
            )
            conn.execute(
                sql.SQL(
                    "INSERT INTO {}.authority VALUES (%s, %s, %s, %s, 0, NULL, %s, 0)"
                ).format(sql.Identifier(schema)),
                (
                    deployment,
                    predecessor,
                    database_writer_epoch,
                    DIGESTS[4],
                    DIGESTS[15],
                ),
            )
            if (predecessor_staged_digest is None) != (predecessor_node_id is None):
                raise AssertionError(
                    "predecessor snapshot digest and node id must be supplied together"
                )
            if predecessor_staged_digest is not None:
                from okg.substrate.graph_store import ChronosGraphStore

                rollback_branch = "rollback:controller:" + schema
                ChronosGraphStore.from_connection(
                    conn, ensure_schema=False
                ).create_branch(
                    rollback_branch,
                    from_branch="default",
                    metadata={
                        "kind": "okg_branch",
                        "deployment_name": deployment,
                        "role": "verified_predecessor_snapshot",
                        "source_generation": predecessor,
                    },
                )
                conn.execute(
                    sql.SQL(
                        "INSERT INTO {}.predecessor_snapshots VALUES (%s, %s, %s, %s)"
                    ).format(sql.Identifier(schema)),
                    (
                        predecessor_staged_digest,
                        rollback_branch,
                        predecessor_node_id,
                        predecessor,
                    ),
                )

    def close(self) -> None:
        import psycopg
        from psycopg import sql

        with psycopg.connect(self.dsn, autocommit=True) as conn:
            conn.execute(
                sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(
                    sql.Identifier(self.schema)
                )
            )

    def stage_generation(
        self,
        *,
        staged_digest: str,
        candidate_digest: str,
        capability: MutationCapability,
    ) -> None:
        import psycopg
        from psycopg import sql

        from okg.substrate.graph_store import ChronosGraphStore
        from okg.substrate.library.sources.base import NodeFact

        if capability.mode != "staged" or capability.provisional_plan_digest is None:
            raise AssertionError("Chronos staging requires a provisional capability")
        if capability.expires_at is None or capability.expires_at <= int(time.time()):
            raise AssertionError("Chronos staging capability expired")
        if not self.signer.verify(capability.unsigned_payload(), capability.token):
            raise AssertionError("Chronos staging capability signature is invalid")
        branch = (
            "staging:controller:"
            + capability.transition_id[-12:]
            + ":"
            + staged_digest.removeprefix("sha256:")[:12]
        )
        node_id = "controller-stage:" + staged_digest.removeprefix("sha256:")[:24]
        with psycopg.connect(self.dsn, autocommit=False) as conn:
            existing = conn.execute(
                sql.SQL(
                    "SELECT transition_id, candidate_digest, effect_set_digest "
                    "FROM {}.staged WHERE staged_digest=%s"
                ).format(sql.Identifier(self.schema)),
                (staged_digest,),
            ).fetchone()
            exact = (
                capability.transition_id,
                candidate_digest,
                capability.effect_set_digest,
            )
            if existing is not None:
                assert tuple(existing) == exact
                return
            store = ChronosGraphStore.from_connection(
                conn, ensure_schema=False, autocommit=False
            )
            predecessor_snapshot = conn.execute(
                sql.SQL(
                    "SELECT branch, node_id FROM {}.predecessor_snapshots "
                    "WHERE staged_digest=%s"
                ).format(sql.Identifier(self.schema)),
                (staged_digest,),
            ).fetchone()
            if predecessor_snapshot is None:
                store.create_branch(
                    branch,
                    from_branch="default",
                    metadata={
                        "kind": "okg_branch",
                        "deployment_name": self.deployment,
                        "role": "controller_staging",
                        "transition_id": capability.transition_id,
                    },
                )
                store.apply_facts(
                    [NodeFact(node_id, "claim", {"staged": True}, {}, {})],
                    branch=branch,
                    source="controller-transition-test",
                    strict_endpoints=True,
                )
            else:
                branch = str(predecessor_snapshot[0])
                node_id = str(predecessor_snapshot[1])
            expected_default_segment = int(
                conn.execute(
                    "SELECT current_segment_id FROM _chronos_branch_interval_branches "
                    "WHERE branch_id='default'"
                ).fetchone()[0]
            )
            conn.execute(
                sql.SQL(
                    "INSERT INTO {}.staged "
                    "(staged_digest, transition_id, candidate_digest, effect_set_digest, "
                    "branch, node_id, expected_default_segment) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)"
                ).format(sql.Identifier(self.schema)),
                (
                    staged_digest,
                    capability.transition_id,
                    candidate_digest,
                    capability.effect_set_digest,
                    branch,
                    node_id,
                    expected_default_segment,
                ),
            )

    def publish_authorized_generation(
        self, receipt: ActivationReceipt
    ) -> Mapping[str, Any]:
        import psycopg
        from psycopg import sql

        from okg.substrate.graph_store import ChronosGraphStore

        if not self.signer.verify(receipt.unsigned_payload(), receipt.signature):
            raise AssertionError("activation receipt signature is invalid")
        with psycopg.connect(self.dsn, autocommit=False) as conn:
            existing = conn.execute(
                sql.SQL(
                    "SELECT outcome FROM {}.outcomes WHERE receipt_digest=%s"
                ).format(sql.Identifier(self.schema)),
                (receipt.digest,),
            ).fetchone()
            if existing is not None:
                return dict(existing[0])
            authority = conn.execute(
                sql.SQL(
                    "SELECT * FROM {}.authority WHERE deployment=%s FOR UPDATE"
                ).format(sql.Identifier(self.schema)),
                (self.deployment,),
            ).fetchone()
            assert authority[1] == receipt.expected_predecessor
            assert authority[2] == receipt.expected_database_writer_epoch
            assert authority[3] == receipt.expected_activation_receipt_digest
            staged = conn.execute(
                sql.SQL(
                    "SELECT * FROM {}.staged WHERE staged_digest=%s FOR UPDATE"
                ).format(sql.Identifier(self.schema)),
                (receipt.staged_generation_digest,),
            ).fetchone()
            assert staged is not None
            assert (staged[1], staged[2], staged[3]) == (
                receipt.transition_id,
                receipt.candidate_digest,
                receipt.effect_set_digest,
            )
            checkpoint = "generation:" + receipt.staged_generation_digest
            store = ChronosGraphStore.from_connection(
                conn, ensure_schema=False, autocommit=False
            )
            published = store.fast_forward(
                staged[4],
                target="default",
                expected_current_segment=int(staged[6]),
                generation_id=checkpoint,
                deployment_name=self.deployment,
                status="published",
                metadata={"activation_receipt_digest": receipt.digest},
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
                "provenance": "postgres-chronos:transaction",
                "visibility_switches": int(authority[7]) + 1,
                "publication_transaction_digest": digest_json(
                    {"checkpoint": published, "receipt": receipt.digest}
                ),
            }
            subject = digest_json(
                {
                    "schema": "okg.local-controller.publication-subject/v1",
                    "phase": "publish",
                    "receipt": receipt.unsigned_payload()
                    | {"signature": receipt.signature},
                }
            )
            proof = ObservationProof.issue(
                signer=self.signer.signer_for("observation"),
                subject_kind="publication",
                subject_digest=subject,
                evidence=evidence,
                nonce="pg-publish-" + receipt.digest,
            )
            outcome = {
                **evidence,
                "proof": proof.unsigned_payload() | {"signature": proof.signature},
            }
            authority_update = conn.execute(
                sql.SQL(
                    "UPDATE {}.authority SET current_generation=%s, receipt_digest=%s, "
                    "database_writer_epoch=%s, lifecycle_activation_epoch=%s, "
                    "port_manifest_digest=%s, "
                    "service_plan_digest=%s, visibility_switches=visibility_switches+1 "
                    "WHERE deployment=%s AND current_generation=%s "
                    "AND receipt_digest=%s AND database_writer_epoch=%s"
                ).format(sql.Identifier(self.schema)),
                (
                    receipt.staged_generation_digest,
                    receipt.digest,
                    receipt.database_writer_epoch,
                    receipt.lifecycle_activation_epoch,
                    receipt.port_manifest_digest,
                    receipt.service_plan_digest,
                    self.deployment,
                    receipt.expected_predecessor,
                    receipt.expected_activation_receipt_digest,
                    receipt.expected_database_writer_epoch,
                ),
            )
            assert authority_update.rowcount == 1
            conn.execute(
                sql.SQL(
                    "UPDATE {}.staged SET published=true WHERE staged_digest=%s"
                ).format(sql.Identifier(self.schema)),
                (receipt.staged_generation_digest,),
            )
            conn.execute(
                sql.SQL("INSERT INTO {}.outcomes VALUES (%s, %s::jsonb)").format(
                    sql.Identifier(self.schema)
                ),
                (receipt.digest, json.dumps(outcome, sort_keys=True)),
            )
            return outcome

    def observe_authorized_generation(
        self, receipt: ActivationReceipt
    ) -> Mapping[str, Any]:
        import psycopg
        from psycopg import sql

        from okg.substrate.graph_store import ChronosGraphStore

        with psycopg.connect(self.dsn, autocommit=True) as conn:
            authority = conn.execute(
                sql.SQL("SELECT * FROM {}.authority WHERE deployment=%s").format(
                    sql.Identifier(self.schema)
                ),
                (self.deployment,),
            ).fetchone()
            staged = conn.execute(
                sql.SQL("SELECT node_id FROM {}.staged WHERE staged_digest=%s").format(
                    sql.Identifier(self.schema)
                ),
                (receipt.staged_generation_digest,),
            ).fetchone()
            assert staged is not None
            store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
            checkpoint = "generation:" + receipt.staged_generation_digest
            store.get_generation(checkpoint)
            rows = store.checkout("default").query(
                "SELECT node_id FROM okg.graph_nodes WHERE node_id = :node_id",
                {"node_id": staged[0]},
            )
        assert authority[1] == receipt.staged_generation_digest
        assert authority[3] == receipt.digest
        assert [row["node_id"] for row in rows] == [staged[0]]
        observed_at = int(time.time())
        evidence = {
            "schema": "okg.local-controller.publication-observation/v1",
            "activation_receipt_digest": receipt.digest,
            "generation_digest": receipt.staged_generation_digest,
            "lifecycle_activation_epoch": int(authority[4]),
            "database_writer_epoch": int(authority[2]),
            "port_manifest_digest": authority[5],
            "service_plan_digest": authority[6],
            "status": "ready",
            "observed_at": observed_at,
            "expires_at": observed_at + 60,
            "provenance": "postgres-chronos:live-read",
            "visibility_switches": int(authority[7]),
            "publication_transaction_digest": digest_json(
                {"checkpoint": checkpoint, "receipt": receipt.digest}
            ),
        }
        subject = digest_json(
            {
                "schema": "okg.local-controller.publication-subject/v1",
                "phase": "ready",
                "receipt": receipt.unsigned_payload()
                | {"signature": receipt.signature},
            }
        )
        proof = ObservationProof.issue(
            signer=self.signer.signer_for("observation"),
            subject_kind="publication",
            subject_digest=subject,
            evidence=evidence,
            nonce="pg-ready-" + receipt.digest,
        )
        return {
            **evidence,
            "proof": proof.unsigned_payload() | {"signature": proof.signature},
        }

    def reader_visible(self, staged_digest: str) -> bool:
        import psycopg
        from psycopg import sql

        from okg.substrate.graph_store import ChronosGraphStore

        with psycopg.connect(self.dsn, autocommit=True) as conn:
            row = conn.execute(
                sql.SQL("SELECT node_id FROM {}.staged WHERE staged_digest=%s").format(
                    sql.Identifier(self.schema)
                ),
                (staged_digest,),
            ).fetchone()
            assert row is not None
            rows = (
                ChronosGraphStore.from_connection(conn, ensure_schema=False)
                .checkout("default")
                .query(
                    "SELECT node_id FROM okg.graph_nodes WHERE node_id = :node_id",
                    {"node_id": row[0]},
                )
            )
        return bool(rows)

    def transition_nodes_visible(self, *node_ids: str) -> set[str]:
        import psycopg

        from okg.substrate.graph_store import ChronosGraphStore

        if not node_ids:
            return set()
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            rows = ChronosGraphStore.from_connection(
                conn, ensure_schema=False
            ).checkout("default").query(
                "SELECT node_id FROM okg.graph_nodes WHERE node_id = ANY(:node_ids)",
                {"node_ids": list(node_ids)},
            )
        return {str(row["node_id"]) for row in rows}

    def authority(self) -> dict[str, Any]:
        import psycopg
        from psycopg import sql

        with psycopg.connect(self.dsn, autocommit=True) as conn:
            row = conn.execute(
                sql.SQL(
                    "SELECT current_generation, database_writer_epoch, receipt_digest, "
                    "visibility_switches FROM {}.authority WHERE deployment=%s"
                ).format(sql.Identifier(self.schema)),
                (self.deployment,),
            ).fetchone()
        return {
            "current_generation": row[0],
            "database_writer_epoch": int(row[1]),
            "receipt_digest": row[2],
            "visibility_switches": int(row[3]),
        }

    def advance_database_writer_epoch(self, *, expected: int, new: int) -> None:
        import psycopg
        from psycopg import sql

        with psycopg.connect(self.dsn, autocommit=True) as conn:
            changed = conn.execute(
                sql.SQL(
                    "UPDATE {}.authority SET database_writer_epoch=%s "
                    "WHERE deployment=%s AND database_writer_epoch=%s"
                ).format(sql.Identifier(self.schema)),
                (new, self.deployment, expected),
            ).rowcount
        assert changed == 1

    def observe_current_state(self, descriptor: ReleaseDescriptor) -> Mapping[str, Any]:
        authority = self.authority()
        now = int(time.time())
        evidence = {
            "schema": "okg.local-controller.no-op-observation/v1",
            "candidate_digest": descriptor.candidate_digest,
            "activation_receipt_digest": authority["receipt_digest"],
            "generation_digest": authority["current_generation"],
            "database_writer_epoch": authority["database_writer_epoch"],
            "service_plan_digest": descriptor.service_plan_digest,
            "status": "ready",
            "health": "healthy",
            "publication_visible": True,
            "observed_at": now,
            "expires_at": now + 60,
            "provenance": "postgres-chronos:noop-read",
        }
        subject = digest_json(
            {
                "schema": "okg.local-controller.no-op-subject/v1",
                "descriptor": descriptor.payload(),
            }
        )
        proof = ObservationProof.issue(
            signer=self.signer.signer_for("observation"),
            subject_kind="no_op",
            subject_digest=subject,
            evidence=evidence,
            nonce="pg-noop-" + descriptor.candidate_digest,
        )
        return {
            **evidence,
            "proof": proof.unsigned_payload() | {"signature": proof.signature},
        }


def _probe_result(
    signer: AuthoritySigner,
    descriptor: ReleaseDescriptor,
    *,
    staged: str = DIGESTS[3],
) -> ProbeResult:
    output = {
        "schema": "okg.local-controller.candidate-output/v1",
        "descriptor": descriptor.payload(),
        "staged_generation_digest": staged,
    }
    probe_signer = signer.signer_for("probe")
    now = int(time.time())
    nonce = "e2e-probe-" + uuid.uuid4().hex
    input_payload = {
        "operation": "plan",
        "deployment": descriptor.deployment,
        "candidate_digest": descriptor.candidate_digest,
    }
    unsigned = {
        "schema": "okg.local-controller.probe-attestation/v1",
        "attestation_id": digest_json(
            {
                "nonce": nonce,
                "launcher_key_id": probe_signer.key_id,
                "candidate_digest": descriptor.candidate_digest,
                "output_payload_digest": digest_json(output),
            }
        ),
        "nonce": nonce,
        "launcher_key_id": probe_signer.key_id,
        "issued_at": now,
        "expires_at": now + 60,
        "input_digest": digest_json(input_payload),
        "input_payload_digest": digest_json(input_payload),
        "candidate_digest": descriptor.candidate_digest,
        "sandbox_profile_digest": DIGESTS[5],
        "output_digest": digest_json(output),
        "output_payload_digest": digest_json(output),
        "launcher_digest": DIGESTS[7],
        "limits_digest": DIGESTS[6],
        "exit_status": 0,
        "elapsed_millis": 1,
        "cpu_limit_seconds": 1,
        "memory_limit_bytes": 64 * 1024 * 1024,
        "output_limit_bytes": 4096,
        "mutation_canary_before": DIGESTS[8],
        "mutation_canary_after": DIGESTS[8],
    }
    attestation = ProbeAttestation(**unsigned, signature=probe_signer.sign(unsigned))
    return ProbeResult(output=output, attestation=attestation)


def _descriptor(
    *,
    no_op: bool = False,
    deployment: str = "controller-e2e",
    expected_predecessor: str = DIGESTS[2],
) -> ReleaseDescriptor:
    identities = {
        "environment_identity": DIGESTS[13],
        "database_instance_identity": DIGESTS[14],
        "service_plan_digest": DIGESTS[15],
        "queue_scope_digest": DIGESTS[8],
        "promotion_approval_digest": DIGESTS[4],
        "evidence_digest": DIGESTS[5],
    }
    effects: list[EffectSpec] = []
    previous: tuple[str, ...] = ()
    for key, kind, irreversible in CANONICAL_SAGA_EFFECTS:
        request = {
            "schema": "okg.local-controller.effect-request/v1",
            **identities,
            "operation_digest": digest_json({"operation": key}),
        }
        if kind == UPGRADE_EFFECT_KIND:
            request = {
                **request,
                "schema": UPGRADE_EFFECT_SCHEMA,
                "issuer": "controller-e2e-upgrade-adapter",
                "attestation_key_id": AuthoritySigner(KEY).signer_for("upgrade").key_id,
                "expected_schema_before_digest": DIGESTS[15],
                "migration_manifest_digest": DIGESTS[8],
                "rollback_compatibility_class": "physical_restore_required",
                "restore_strategy": "verified_backup",
                "max_attestation_ttl_seconds": 120,
            }
        effects.append(EffectSpec(key, kind, request, previous, irreversible))
        previous = (key,)
    return ReleaseDescriptor(
        deployment=deployment,
        candidate_digest=DIGESTS[1] if no_op else DIGESTS[0],
        predecessor_digest=DIGESTS[1],
        expected_predecessor=expected_predecessor,
        expected_activation_receipt_digest=DIGESTS[4],
        **identities,
        expected_database_writer_epoch=8,
        database_writer_epoch=8 if no_op else 9,
        dbos_application_version="controller-e2e-v1",
        controller_min_version=1,
        effects=tuple(effects),
    )


def _installation(
    ledger_path: Path, signer: AuthoritySigner, deployment: str
) -> ControllerInstallation:
    return ControllerInstallation.issue(
        signer=signer,
        deployment=deployment,
        ledger_uuid="e2e-ledger-" + uuid.uuid4().hex,
        ledger_path=str(ledger_path.resolve()),
        probe_profile_digest=DIGESTS[5],
        probe_launcher_digest=DIGESTS[7],
        probe_limits_digest=DIGESTS[6],
        probe_max_ttl_seconds=60,
    )


PORT_MANIFEST_DIGEST = DIGESTS[9]


def _adapters(path: Path, signer: AuthoritySigner) -> dict[str, Any]:
    durable = DurableFixtureEffect(path, signer)
    result = {kind: durable for _key, kind, _irreversible in CANONICAL_SAGA_EFFECTS}
    result[UPGRADE_EFFECT_KIND] = UpgradeFixtureEffect(path, signer)
    return result


def _counts(ledger: Path, target: Path, effect: Path) -> dict[str, int]:
    with sqlite3.connect(ledger) as conn:
        transitions = conn.execute("SELECT COUNT(*) FROM transitions").fetchone()[0]
        effects = conn.execute("SELECT COUNT(*) FROM effects").fetchone()[0]
        receipts = conn.execute("SELECT COUNT(*) FROM activation_receipts").fetchone()[
            0
        ]
    with sqlite3.connect(target) as conn:
        visible = conn.execute(
            "SELECT COUNT(*) FROM staged_generations WHERE visible=1"
        ).fetchone()[0]
        outcomes = conn.execute(
            "SELECT COUNT(*) FROM reference_publish_outcomes"
        ).fetchone()[0]
    with sqlite3.connect(effect) as conn:
        applied = conn.execute("SELECT COUNT(*) FROM applied_effects").fetchone()[0]
    return {
        "transitions": transitions,
        "effects": effects,
        "receipts": receipts,
        "visible": visible,
        "outcomes": outcomes,
        "applied": applied,
    }


@pytest.mark.parametrize(
    ("crash_point", "operator_state", "visible_before_resume"),
    [
        ("after_authorize_activation", "read_only_stale", 0),
        ("after_publish_authorized_generation", "published_not_ready", 1),
        ("after_observe_publication", "published_not_ready", 1),
    ],
)
def test_controller_transition_three_action_crash_resume_has_exact_deltas(
    tmp_path: Path,
    crash_point: str,
    operator_state: str,
    visible_before_resume: int,
) -> None:
    case = tmp_path / crash_point
    case.mkdir()
    ledger_path = case / "controller.sqlite3"
    target_path = case / "target.sqlite3"
    effect_path = case / "effect.sqlite3"
    signer = AuthoritySigner(KEY)
    installation = _installation(ledger_path, signer, "controller-e2e")
    ledger = ControllerLedger(ledger_path, installation=installation)
    target = SQLiteReferenceTarget(
        path=target_path,
        signer=signer,
        deployment="controller-e2e",
        predecessor=DIGESTS[2],
        database_writer_epoch=8,
        activation_receipt_digest=DIGESTS[4],
        service_plan_digest=DIGESTS[15],
    )
    adapters = _adapters(effect_path, signer)
    fired = False

    def fault(point: str) -> None:
        nonlocal fired
        if not fired and point == crash_point:
            fired = True
            raise InjectedControllerCrash(point)

    controller = LocalLifecycleController(
        ledger=ledger, signer=signer, fault_hook=fault
    )
    descriptor = _descriptor()
    with pytest.raises(InjectedControllerCrash, match=crash_point):
        controller.activate(
            probe_result=_probe_result(signer, descriptor),
            port_manifest_digest=PORT_MANIFEST_DIGEST,
            capacity_reservation_digest=DIGESTS[9],
            backup_digest=DIGESTS[10],
            current_lifecycle_epoch=0,
            staged_capability_expires_at=int(time.time()) + 300,
            adapters=adapters,
            target=target,
        )

    with sqlite3.connect(ledger_path) as conn:
        transition_id = conn.execute(
            "SELECT transition_id FROM transitions"
        ).fetchone()[0]
    assert controller.status(transition_id)["operator_state"] == operator_state
    before = _counts(ledger_path, target_path, effect_path)
    assert before == {
        "transitions": 1,
        "effects": len(CANONICAL_SAGA_EFFECTS),
        "receipts": 1,
        "visible": visible_before_resume,
        "outcomes": visible_before_resume,
        "applied": len(CANONICAL_SAGA_EFFECTS) - 1,
    }

    restarted = LocalLifecycleController(
        ledger=ControllerLedger(ledger_path, installation=installation), signer=signer
    )
    ready = restarted.resume(
        transition_id=transition_id,
        adapters=_adapters(effect_path, signer),
        target=SQLiteReferenceTarget(
            path=target_path,
            signer=signer,
            deployment="controller-e2e",
            predecessor=DIGESTS[2],
            database_writer_epoch=8,
            activation_receipt_digest=DIGESTS[4],
            service_plan_digest=DIGESTS[15],
        ),
        port_manifest_digest=PORT_MANIFEST_DIGEST,
    )
    assert ready.state == "ready"
    assert (
        restarted.status(
            transition_id,
            target=SQLiteReferenceTarget(
                path=target_path,
                signer=signer,
                deployment="controller-e2e",
                predecessor=DIGESTS[2],
                database_writer_epoch=8,
                activation_receipt_digest=DIGESTS[4],
                service_plan_digest=DIGESTS[15],
            ),
            port_manifest_digest=PORT_MANIFEST_DIGEST,
        )["broad_writes_allowed"]
        is True
    )
    assert _counts(ledger_path, target_path, effect_path) == {
        "transitions": 1,
        "effects": len(CANONICAL_SAGA_EFFECTS),
        "receipts": 1,
        "visible": 1,
        "outcomes": 1,
        "applied": len(CANONICAL_SAGA_EFFECTS),
    }


def test_controller_transition_noop_has_zero_authority_delta(tmp_path: Path) -> None:
    signer = AuthoritySigner(KEY)
    ledger_path = tmp_path / "controller.sqlite3"
    target_path = tmp_path / "target.sqlite3"
    effect_path = tmp_path / "effect.sqlite3"
    target = SQLiteReferenceTarget(
        path=target_path,
        signer=signer,
        deployment="controller-e2e",
        predecessor=DIGESTS[2],
        database_writer_epoch=8,
        activation_receipt_digest=DIGESTS[4],
        service_plan_digest=DIGESTS[15],
    )
    descriptor = _descriptor(no_op=True)
    installation = _installation(ledger_path, signer, "controller-e2e")
    result = LocalLifecycleController(
        ledger=ControllerLedger(ledger_path, installation=installation), signer=signer
    ).activate(
        probe_result=_probe_result(signer, descriptor),
        port_manifest_digest=PORT_MANIFEST_DIGEST,
        capacity_reservation_digest=DIGESTS[9],
        backup_digest=DIGESTS[10],
        current_lifecycle_epoch=0,
        staged_capability_expires_at=int(time.time()) + 300,
        adapters=_adapters(effect_path, signer),
        target=target,
    )
    assert result.no_op is True
    assert _counts(ledger_path, target_path, effect_path) == {
        "transitions": 0,
        "effects": 0,
        "receipts": 0,
        "visible": 0,
        "outcomes": 0,
        "applied": 0,
    }


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("crash_point", "operator_state", "visible_before_resume"),
    [
        ("after_authorize_activation", "read_only_stale", False),
        ("after_publish_authorized_generation", "published_not_ready", True),
        ("after_observe_publication", "published_not_ready", True),
    ],
)
def test_controller_transition_real_postgres_chronos_visibility_and_resume(
    clean_runtime: str,
    tmp_path: Path,
    crash_point: str,
    operator_state: str,
    visible_before_resume: bool,
) -> None:
    import psycopg

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact

    deployment = "controller-e2e-" + uuid.uuid4().hex[:12]
    schema = "controller_transition_" + uuid.uuid4().hex[:12]
    signer = AuthoritySigner(KEY)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        base_node = "controller-base:" + uuid.uuid4().hex
        store.apply_facts(
            [NodeFact(base_node, "claim", {"base": True}, {}, {})],
            source="controller-transition-test",
            strict_endpoints=True,
        )
        predecessor = store.checkpoint_generation(
            deployment_name=deployment,
            branch="default",
            status="published",
        )

    target = PostgresChronosTarget(
        dsn=clean_runtime,
        schema=schema,
        signer=signer,
        deployment=deployment,
        predecessor=predecessor,
        database_writer_epoch=8,
        predecessor_staged_digest=rollback_generation_digest(
            predecessor_digest=DIGESTS[1], expected_predecessor=predecessor
        ),
        predecessor_node_id=base_node,
    )
    try:
        no_op_ledger_path = tmp_path / "controller-no-op.sqlite3"
        no_op_installation = _installation(no_op_ledger_path, signer, deployment)
        no_op_descriptor = _descriptor(
            no_op=True,
            deployment=deployment,
            expected_predecessor=predecessor,
        )
        before_no_op = target.authority()
        no_op = LocalLifecycleController(
            ledger=ControllerLedger(no_op_ledger_path, installation=no_op_installation),
            signer=signer,
        ).activate(
            probe_result=_probe_result(signer, no_op_descriptor),
            port_manifest_digest=PORT_MANIFEST_DIGEST,
            capacity_reservation_digest=DIGESTS[9],
            backup_digest=DIGESTS[10],
            current_lifecycle_epoch=0,
            staged_capability_expires_at=int(time.time()) + 300,
            adapters={},
            target=target,
        )
        assert no_op.no_op is True
        assert target.authority() == before_no_op

        ledger_path = tmp_path / "controller.sqlite3"
        effect_path = tmp_path / "effect.sqlite3"
        installation = _installation(ledger_path, signer, deployment)
        ledger = ControllerLedger(ledger_path, installation=installation)
        descriptor = _descriptor(
            deployment=deployment, expected_predecessor=predecessor
        )
        fired = False

        def fault(point: str) -> None:
            nonlocal fired
            if not fired and point == crash_point:
                fired = True
                raise InjectedControllerCrash(point)

        crashing = LocalLifecycleController(
            ledger=ledger, signer=signer, fault_hook=fault
        )
        with pytest.raises(InjectedControllerCrash, match=crash_point):
            crashing.activate(
                probe_result=_probe_result(signer, descriptor),
                port_manifest_digest=PORT_MANIFEST_DIGEST,
                capacity_reservation_digest=DIGESTS[9],
                backup_digest=DIGESTS[10],
                current_lifecycle_epoch=0,
                staged_capability_expires_at=int(time.time()) + 300,
                adapters=_adapters(effect_path, signer),
                target=target,
            )
        with sqlite3.connect(ledger_path) as conn:
            transition_id = conn.execute(
                "SELECT transition_id FROM transitions"
            ).fetchone()[0]
        assert crashing.status(transition_id)["operator_state"] == operator_state
        assert target.reader_visible(DIGESTS[3]) is visible_before_resume
        assert target.authority()["visibility_switches"] == int(visible_before_resume)

        restarted_target = PostgresChronosTarget(
            dsn=clean_runtime,
            schema=schema,
            signer=signer,
            deployment=deployment,
        )
        restarted = LocalLifecycleController(
            ledger=ControllerLedger(ledger_path, installation=installation),
            signer=signer,
        )
        ready = restarted.resume(
            transition_id=transition_id,
            adapters=_adapters(effect_path, signer),
            target=restarted_target,
            port_manifest_digest=PORT_MANIFEST_DIGEST,
        )
        assert ready.state == "ready"
        assert restarted_target.reader_visible(DIGESTS[3]) is True
        candidate_node = "controller-stage:" + DIGESTS[3].removeprefix("sha256:")[:24]
        assert restarted_target.transition_nodes_visible(base_node, candidate_node) == {
            base_node,
            candidate_node,
        }
        authority = restarted_target.authority()
        assert authority["visibility_switches"] == 1
        assert authority["receipt_digest"] == ready.activation_receipt_digest
        rolled_back = restarted.rollback(
            transition_id=transition_id,
            lifecycle_activation_epoch=2,
            database_writer_epoch=10,
            adapters=_adapters(effect_path, signer),
            target=restarted_target,
            port_manifest_digest=PORT_MANIFEST_DIGEST,
        )
        assert rolled_back.state == "rolled_back"
        rollback_staged_digest = rollback_generation_digest(
            predecessor_digest=DIGESTS[1], expected_predecessor=predecessor
        )
        assert restarted_target.reader_visible(rollback_staged_digest) is True
        assert restarted_target.reader_visible(DIGESTS[3]) is False
        assert restarted_target.transition_nodes_visible(base_node, candidate_node) == {
            base_node
        }
        assert restarted_target.authority()["visibility_switches"] == 2
        with sqlite3.connect(effect_path) as effect_conn:
            external_effects_remaining = effect_conn.execute(
                "SELECT COUNT(*) FROM applied_effects"
            ).fetchone()[0]
        with sqlite3.connect(ledger_path) as conn:
            observed_graph_report = {
                "schema": "okg.local-controller.graph-transition-report/v1",
                "postgres_chronos": True,
                "canonical_effects": conn.execute(
                    "SELECT COUNT(*) FROM effects"
                ).fetchone()[0],
                "activation_receipts": conn.execute(
                    "SELECT COUNT(*) FROM activation_receipts"
                ).fetchone()[0],
                "compensations": conn.execute(
                    "SELECT COUNT(*) FROM compensations"
                ).fetchone()[0],
                "uncompensated": conn.execute(
                    "SELECT COUNT(*) FROM compensations WHERE phase<>'committed'"
                ).fetchone()[0],
                "external_effects_remaining": external_effects_remaining,
                "rollback_database_writer_epoch": restarted_target.authority()[
                    "database_writer_epoch"
                ],
                "rollback_visibility_switches": restarted_target.authority()[
                    "visibility_switches"
                ],
                "predecessor_snapshot_restored": (
                    restarted_target.transition_nodes_visible(
                        base_node, candidate_node
                    )
                    == {base_node}
                ),
                "no_op_visibility_delta": (
                    target.authority()["visibility_switches"]
                    - before_no_op["visibility_switches"]
                    - 2
                ),
            }
            expected_graph_report = json.loads(
                (
                    ROOT
                    / "pact/evidence/externalize-local-okg-lifecycle-controller/graph-transition-report.json"
                ).read_text(encoding="utf-8")
            )
            assert observed_graph_report == expected_graph_report
            assert (
                conn.execute(
                    "SELECT COUNT(*) FROM upgrade_attestation_consumptions"
                ).fetchone()[0]
                == 1
            )
    finally:
        target.close()
