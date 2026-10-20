"""Durable external saga ledger owned by the neutral controller process."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import closing, contextmanager, suppress
from dataclasses import asdict
from pathlib import Path
from typing import Any

from .protocol import (
    UPGRADE_EFFECT_KIND,
    ActivationReceipt,
    AuthoritySigner,
    ControllerInstallation,
    EffectPhase,
    EffectSpec,
    TransitionPlan,
    TransitionState,
    UpgradeAttestation,
    canonical_json,
    digest_json,
    rollback_generation_digest,
)


class LedgerError(RuntimeError):
    pass


_SCHEMA = """
CREATE TABLE IF NOT EXISTS controller_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS transitions (
    transition_id TEXT PRIMARY KEY,
    deployment TEXT NOT NULL,
    plan_digest TEXT NOT NULL,
    plan_json TEXT NOT NULL,
    state TEXT NOT NULL,
    lifecycle_activation_epoch INTEGER NOT NULL,
    database_writer_epoch INTEGER NOT NULL,
    candidate_digest TEXT NOT NULL,
    predecessor_digest TEXT NOT NULL,
    expected_predecessor TEXT NOT NULL,
    irreversible_crossed INTEGER NOT NULL DEFAULT 0,
    published_receipt_digest TEXT,
    final_observation_digest TEXT,
    active_capability_json TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(deployment, lifecycle_activation_epoch)
);
CREATE TABLE IF NOT EXISTS deployment_authority (
    deployment TEXT PRIMARY KEY,
    current_transition_id TEXT NOT NULL REFERENCES transitions(transition_id),
    max_lifecycle_activation_epoch INTEGER NOT NULL,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS effects (
    transition_id TEXT NOT NULL REFERENCES transitions(transition_id),
    effect_key TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    kind TEXT NOT NULL,
    request_json TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    predecessors_json TEXT NOT NULL,
    irreversible INTEGER NOT NULL,
    phase TEXT NOT NULL,
    applied_digest TEXT,
    observation_digest TEXT,
    observation_json TEXT,
    PRIMARY KEY(transition_id, effect_key),
    UNIQUE(transition_id, ordinal)
);
CREATE TABLE IF NOT EXISTS activation_receipts (
    receipt_digest TEXT PRIMARY KEY,
    transition_id TEXT NOT NULL REFERENCES transitions(transition_id),
    receipt_kind TEXT NOT NULL CHECK(receipt_kind IN ('candidate', 'rollback')),
    lifecycle_activation_epoch INTEGER NOT NULL,
    database_writer_epoch INTEGER NOT NULL,
    receipt_json TEXT NOT NULL,
    invalidated INTEGER NOT NULL DEFAULT 0,
    published INTEGER NOT NULL DEFAULT 0,
    observed INTEGER NOT NULL DEFAULT 0,
    UNIQUE(transition_id, receipt_kind)
);
CREATE TABLE IF NOT EXISTS invalidated_capabilities (
    receipt_digest TEXT PRIMARY KEY,
    invalidated_by_receipt_digest TEXT NOT NULL,
    reason TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS upgrade_attestation_consumptions (
    attestation_id TEXT PRIMARY KEY,
    transition_id TEXT NOT NULL REFERENCES transitions(transition_id),
    effect_key TEXT NOT NULL,
    content_digest TEXT NOT NULL,
    attestation_json TEXT NOT NULL,
    consumed_at INTEGER NOT NULL,
    UNIQUE(transition_id, effect_key)
);
CREATE TABLE IF NOT EXISTS probe_attestation_consumptions (
    attestation_id TEXT PRIMARY KEY,
    transition_id TEXT,
    result_kind TEXT NOT NULL CHECK(result_kind IN ('transition', 'no_op')),
    consumed_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS compensations (
    transition_id TEXT NOT NULL REFERENCES transitions(transition_id),
    compensation_key TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    forward_effect_key TEXT NOT NULL,
    kind TEXT NOT NULL,
    request_json TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    predecessors_json TEXT NOT NULL,
    phase TEXT NOT NULL,
    applied_digest TEXT,
    observation_digest TEXT,
    observation_json TEXT,
    PRIMARY KEY(transition_id, compensation_key),
    UNIQUE(transition_id, ordinal),
    UNIQUE(transition_id, forward_effect_key)
);
CREATE TABLE IF NOT EXISTS retirements (
    retirement_id TEXT PRIMARY KEY,
    transition_id TEXT NOT NULL UNIQUE REFERENCES transitions(transition_id),
    plan_digest TEXT NOT NULL,
    plan_json TEXT NOT NULL,
    state TEXT NOT NULL,
    final_observation_digest TEXT,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS retirement_effects (
    retirement_id TEXT NOT NULL REFERENCES retirements(retirement_id),
    effect_key TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    kind TEXT NOT NULL,
    request_json TEXT NOT NULL,
    request_digest TEXT NOT NULL,
    predecessors_json TEXT NOT NULL,
    phase TEXT NOT NULL,
    applied_digest TEXT,
    observation_digest TEXT,
    observation_json TEXT,
    PRIMARY KEY(retirement_id, effect_key),
    UNIQUE(retirement_id, ordinal)
);
CREATE TABLE IF NOT EXISTS events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    transition_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


class ControllerLedger:
    """SQLite-backed authority with FULL-sync transactions and immutable plans."""

    def __init__(
        self, path: str | Path, *, installation: ControllerInstallation
    ) -> None:
        self.path = Path(path).resolve()
        configured_path = Path(installation.ledger_path).resolve()
        if self.path != configured_path:
            raise LedgerError("ledger path is not owned by this installation")
        self.installation = installation
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        with suppress(PermissionError):
            os.chmod(self.path.parent, 0o700)
        with closing(self._connect()) as conn:
            conn.executescript(_SCHEMA)
            conn.execute(
                """
                WITH authority_epochs AS (
                    SELECT deployment, transition_id,
                           lifecycle_activation_epoch AS authority_epoch
                    FROM transitions
                    UNION ALL
                    SELECT transitions.deployment, receipts.transition_id,
                           receipts.lifecycle_activation_epoch AS authority_epoch
                    FROM activation_receipts AS receipts
                    JOIN transitions
                      ON transitions.transition_id = receipts.transition_id
                ), ranked AS (
                    SELECT deployment, transition_id, authority_epoch,
                           ROW_NUMBER() OVER (
                               PARTITION BY deployment
                               ORDER BY authority_epoch DESC, transition_id DESC
                           ) AS authority_rank
                    FROM authority_epochs
                )
                INSERT OR IGNORE INTO deployment_authority(
                    deployment, current_transition_id,
                    max_lifecycle_activation_epoch
                )
                SELECT deployment, transition_id, authority_epoch
                FROM ranked WHERE authority_rank=1
                """
            )
            expected = {
                "schema": "okg.local-controller.ledger/v1",
                "installation_id": installation.installation_id,
                "deployment": installation.deployment,
                "ledger_uuid": installation.ledger_uuid,
                "controller_trust_root_id": installation.controller_trust_root_id,
            }
            existing = {
                row["key"]: row["value"]
                for row in conn.execute("SELECT key, value FROM controller_meta")
            }
            mismatches = {
                key: {"expected": value, "observed": existing.get(key)}
                for key, value in expected.items()
                if key in existing and existing[key] != value
            }
            if mismatches:
                raise LedgerError(
                    "ledger installation identity is immutable: "
                    + canonical_json(mismatches)
                )
            for key, value in expected.items():
                conn.execute(
                    "INSERT OR IGNORE INTO controller_meta(key, value) VALUES (?, ?)",
                    (key, value),
                )
        with suppress(PermissionError):
            os.chmod(self.path, 0o600)

    @contextmanager
    def execution_lock(self) -> Iterator[None]:
        """Serialize external lifecycle invocations across controller processes."""

        try:
            import fcntl
        except ImportError as exc:  # pragma: no cover - supported hosts are POSIX
            raise LedgerError(
                "controller execution requires POSIX file locking"
            ) from exc
        lock_path = self.path.with_name(f".{self.path.name}.execution.lock")
        flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0)
        flags |= getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(lock_path, flags, 0o600)
        try:
            if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                raise LedgerError("controller execution lock is not a regular file")
            with suppress(PermissionError):
                os.fchmod(descriptor, 0o600)
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        return conn

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            conn.close()

    @staticmethod
    def _event(
        conn: sqlite3.Connection,
        transition_id: str,
        event_type: str,
        payload: Mapping[str, Any],
    ) -> None:
        conn.execute(
            "INSERT INTO events(transition_id, event_type, payload_json) "
            "VALUES (?, ?, ?)",
            (transition_id, event_type, canonical_json(payload)),
        )

    def create_transition(
        self, plan: TransitionPlan, effects: Sequence[EffectSpec]
    ) -> dict[str, Any]:
        if (
            plan.ledger_uuid != self.installation.ledger_uuid
            or plan.controller_trust_root_id
            != self.installation.controller_trust_root_id
            or plan.deployment != self.installation.deployment
        ):
            raise LedgerError("transition plan belongs to another installation")
        plan_json = canonical_json(plan.payload())
        with self._transaction() as conn:
            existing = conn.execute(
                "SELECT * FROM transitions WHERE transition_id=?",
                (plan.transition_id,),
            ).fetchone()
            if existing is not None:
                if existing["plan_digest"] != plan.digest:
                    raise LedgerError("transition id is already bound to another plan")
                return dict(existing)
            consumed_probe = conn.execute(
                "SELECT transition_id FROM probe_attestation_consumptions "
                "WHERE attestation_id=?",
                (plan.probe_attestation_id,),
            ).fetchone()
            if consumed_probe is not None:
                raise LedgerError("probe attestation replay was refused")
            active = conn.execute(
                "SELECT transition_id, state FROM transitions WHERE deployment=? "
                "AND state NOT IN (?, ?, ?) LIMIT 1",
                (
                    plan.deployment,
                    TransitionState.READY,
                    TransitionState.ROLLED_BACK,
                    TransitionState.RETIRED,
                ),
            ).fetchone()
            if active is not None:
                raise LedgerError(
                    "deployment already has a nonterminal lifecycle attempt: "
                    f"{active['transition_id']} ({active['state']})"
                )
            authority = conn.execute(
                "SELECT current_transition_id, max_lifecycle_activation_epoch "
                "FROM deployment_authority WHERE deployment=?",
                (plan.deployment,),
            ).fetchone()
            max_epoch = (
                authority["max_lifecycle_activation_epoch"]
                if authority is not None
                else 0
            )
            if plan.lifecycle_activation_epoch <= max_epoch:
                raise LedgerError(
                    "lifecycle_activation_epoch must be strictly newer than the ledger"
                )
            conn.execute(
                "INSERT INTO probe_attestation_consumptions "
                "VALUES (?, ?, 'transition', ?)",
                (plan.probe_attestation_id, plan.transition_id, int(time.time())),
            )
            conn.execute(
                """
                INSERT INTO transitions(
                    transition_id, deployment, plan_digest, plan_json, state,
                    lifecycle_activation_epoch, database_writer_epoch,
                    candidate_digest, predecessor_digest, expected_predecessor
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    plan.transition_id,
                    plan.deployment,
                    plan.digest,
                    plan_json,
                    TransitionState.PLANNED,
                    plan.lifecycle_activation_epoch,
                    plan.database_writer_epoch,
                    plan.candidate_digest,
                    plan.predecessor_digest,
                    plan.expected_predecessor,
                ),
            )
            conn.execute(
                "INSERT INTO deployment_authority("
                "deployment, current_transition_id, max_lifecycle_activation_epoch"
                ") VALUES (?, ?, ?) "
                "ON CONFLICT(deployment) DO UPDATE SET "
                "current_transition_id=excluded.current_transition_id, "
                "max_lifecycle_activation_epoch="
                "excluded.max_lifecycle_activation_epoch, "
                "updated_at=CURRENT_TIMESTAMP",
                (
                    plan.deployment,
                    plan.transition_id,
                    plan.lifecycle_activation_epoch,
                ),
            )
            for ordinal, effect in enumerate(effects, start=1):
                conn.execute(
                    """
                    INSERT INTO effects(
                        transition_id, effect_key, ordinal, kind, request_json,
                        request_digest, predecessors_json, irreversible, phase
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        plan.transition_id,
                        effect.key,
                        ordinal,
                        effect.kind,
                        canonical_json(effect.request),
                        effect.request_digest,
                        canonical_json(list(effect.predecessors)),
                        int(effect.irreversible),
                        EffectPhase.PLANNED,
                    ),
                )
            self._event(
                conn,
                plan.transition_id,
                "transition.intent",
                {
                    "plan_digest": plan.digest,
                    "effect_count": len(effects),
                    "superseded_transition_id": (
                        authority["current_transition_id"]
                        if authority is not None
                        else None
                    ),
                },
            )
        return self.transition(plan.transition_id)

    def is_current_attempt(
        self,
        transition_id: str,
        *,
        lifecycle_activation_epoch: int | None = None,
    ) -> bool:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT authority.current_transition_id, "
                "authority.max_lifecycle_activation_epoch "
                "FROM transitions AS transition_row "
                "JOIN deployment_authority AS authority "
                "ON authority.deployment=transition_row.deployment "
                "WHERE transition_row.transition_id=?",
                (transition_id,),
            ).fetchone()
        if row is None or row["current_transition_id"] != transition_id:
            return False
        return (
            lifecycle_activation_epoch is None
            or row["max_lifecycle_activation_epoch"] == lifecycle_activation_epoch
        )

    def consume_noop_probe(self, attestation_id: str) -> None:
        with self._transaction() as conn:
            existing = conn.execute(
                "SELECT 1 FROM probe_attestation_consumptions WHERE attestation_id=?",
                (attestation_id,),
            ).fetchone()
            if existing is not None:
                raise LedgerError("probe attestation replay was refused")
            conn.execute(
                "INSERT INTO probe_attestation_consumptions "
                "VALUES (?, NULL, 'no_op', ?)",
                (attestation_id, int(time.time())),
            )

    def transition(self, transition_id: str) -> dict[str, Any]:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM transitions WHERE transition_id=?", (transition_id,)
            ).fetchone()
        if row is None:
            raise LedgerError(f"unknown transition: {transition_id}")
        result = dict(row)
        result["plan"] = json.loads(result.pop("plan_json"))
        if result.get("active_capability_json"):
            result["active_capability"] = json.loads(
                result.pop("active_capability_json")
            )
        return result

    def transition_for_probe(self, attestation_id: str) -> dict[str, Any] | None:
        """Return the exact transition already bound to a consumed probe."""

        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT transition_id, result_kind "
                "FROM probe_attestation_consumptions WHERE attestation_id=?",
                (attestation_id,),
            ).fetchone()
        if (
            row is None
            or row["result_kind"] != "transition"
            or row["transition_id"] is None
        ):
            return None
        return self.transition(row["transition_id"])

    def record_staged_capability_renewal(
        self, transition_id: str, *, expires_at: int
    ) -> None:
        """Audit a bounded renewal for the current durable, pre-publication plan."""

        with self._transaction() as conn:
            transition = conn.execute(
                "SELECT deployment, state FROM transitions WHERE transition_id=?",
                (transition_id,),
            ).fetchone()
            if transition is None:
                raise LedgerError(f"unknown transition: {transition_id}")
            if transition["state"] not in {
                TransitionState.PLANNED,
                TransitionState.APPLYING,
            }:
                raise LedgerError(
                    "staged capability renewal requires a pre-publication transition"
                )
            authority = conn.execute(
                "SELECT current_transition_id FROM deployment_authority "
                "WHERE deployment=?",
                (transition["deployment"],),
            ).fetchone()
            if authority is None or authority["current_transition_id"] != transition_id:
                raise LedgerError("staged capability renewal attempt is not current")
            if expires_at <= int(time.time()):
                raise LedgerError("staged capability renewal must expire in the future")
            self._event(
                conn,
                transition_id,
                "staged-capability.renewed",
                {"expires_at": expires_at},
            )

    def effects(self, transition_id: str) -> list[dict[str, Any]]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM effects WHERE transition_id=? ORDER BY ordinal",
                (transition_id,),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["request"] = json.loads(item.pop("request_json"))
            item["predecessors"] = tuple(json.loads(item.pop("predecessors_json")))
            if item.get("observation_json"):
                item["observation"] = json.loads(item.pop("observation_json"))
            result.append(item)
        return result

    def _effect_update(
        self,
        *,
        transition_id: str,
        effect_key: str,
        allowed: set[str],
        phase: EffectPhase,
        event: str,
        fields: Mapping[str, Any] | None = None,
    ) -> None:
        values = dict(fields or {})
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT phase, irreversible FROM effects "
                "WHERE transition_id=? AND effect_key=?",
                (transition_id, effect_key),
            ).fetchone()
            if row is None:
                raise LedgerError(f"unknown effect: {effect_key}")
            if row["phase"] == phase:
                return
            if row["phase"] not in allowed:
                raise LedgerError(
                    f"effect {effect_key} cannot move from {row['phase']} to {phase}"
                )
            assignments = ["phase=?"]
            params: list[Any] = [phase]
            for key, value in values.items():
                assignments.append(f"{key}=?")
                params.append(value)
            params.extend((transition_id, effect_key))
            conn.execute(
                f"UPDATE effects SET {', '.join(assignments)} "
                "WHERE transition_id=? AND effect_key=?",
                params,
            )
            if phase == EffectPhase.INTENDED:
                conn.execute(
                    "UPDATE transitions SET state=?, updated_at=CURRENT_TIMESTAMP "
                    "WHERE transition_id=? AND state=?",
                    (
                        TransitionState.APPLYING,
                        transition_id,
                        TransitionState.PLANNED,
                    ),
                )
            if phase == EffectPhase.COMMITTED and row["irreversible"]:
                conn.execute(
                    "UPDATE transitions SET irreversible_crossed=1, "
                    "updated_at=CURRENT_TIMESTAMP WHERE transition_id=?",
                    (transition_id,),
                )
            self._event(conn, transition_id, event, {"effect_key": effect_key})

    def intend_effect(self, transition_id: str, effect_key: str) -> None:
        with closing(self._connect()) as conn:
            effect = conn.execute(
                "SELECT predecessors_json FROM effects "
                "WHERE transition_id=? AND effect_key=?",
                (transition_id, effect_key),
            ).fetchone()
            if effect is None:
                raise LedgerError(f"unknown effect: {effect_key}")
            predecessors = json.loads(effect["predecessors_json"])
            if predecessors:
                committed = {
                    row["effect_key"]
                    for row in conn.execute(
                        "SELECT effect_key FROM effects WHERE transition_id=? "
                        "AND phase=?",
                        (transition_id, EffectPhase.COMMITTED),
                    )
                }
                missing = sorted(set(predecessors) - committed)
                if missing:
                    raise LedgerError(
                        f"effect {effect_key} predecessors are not committed: {missing}"
                    )
        self._effect_update(
            transition_id=transition_id,
            effect_key=effect_key,
            allowed={EffectPhase.PLANNED},
            phase=EffectPhase.INTENDED,
            event="effect.intent",
        )

    def mark_irreversible_started(self, transition_id: str, effect_key: str) -> None:
        with self._transaction() as conn:
            effect = conn.execute(
                "SELECT irreversible, phase FROM effects "
                "WHERE transition_id=? AND effect_key=?",
                (transition_id, effect_key),
            ).fetchone()
            if effect is None or not effect["irreversible"]:
                raise LedgerError("irreversible boundary names a reversible effect")
            if effect["phase"] not in {
                EffectPhase.INTENDED,
                EffectPhase.APPLIED,
                EffectPhase.OBSERVED,
                EffectPhase.COMMITTED,
            }:
                raise LedgerError(
                    "irreversible boundary requires durable effect intent"
                )
            transition = conn.execute(
                "SELECT irreversible_crossed FROM transitions WHERE transition_id=?",
                (transition_id,),
            ).fetchone()
            if transition is None:
                raise LedgerError(f"unknown transition: {transition_id}")
            if transition["irreversible_crossed"]:
                return
            conn.execute(
                "UPDATE transitions SET irreversible_crossed=1, "
                "updated_at=CURRENT_TIMESTAMP WHERE transition_id=?",
                (transition_id,),
            )
            self._event(
                conn,
                transition_id,
                "transition.irreversible_boundary_crossed",
                {"effect_key": effect_key, "phase": effect["phase"]},
            )

    def block_transition(self, transition_id: str, *, reason: str) -> None:
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT state FROM transitions WHERE transition_id=?",
                (transition_id,),
            ).fetchone()
            if row is None:
                raise LedgerError(f"unknown transition: {transition_id}")
            if row["state"] == TransitionState.BLOCKED:
                return
            conn.execute(
                "UPDATE transitions SET state=?, active_capability_json=NULL, "
                "updated_at=CURRENT_TIMESTAMP WHERE transition_id=?",
                (TransitionState.BLOCKED, transition_id),
            )
            self._event(
                conn,
                transition_id,
                "transition.blocked",
                {"reason": reason},
            )

    def mark_effect_applied(
        self, transition_id: str, effect_key: str, applied_digest: str
    ) -> None:
        self._effect_update(
            transition_id=transition_id,
            effect_key=effect_key,
            allowed={EffectPhase.INTENDED},
            phase=EffectPhase.APPLIED,
            event="effect.applied",
            fields={"applied_digest": applied_digest},
        )

    def observe_effect(
        self,
        transition_id: str,
        effect_key: str,
        observation: Mapping[str, Any],
    ) -> None:
        observation_json = canonical_json(observation)
        self._effect_update(
            transition_id=transition_id,
            effect_key=effect_key,
            allowed={EffectPhase.INTENDED, EffectPhase.APPLIED},
            phase=EffectPhase.OBSERVED,
            event="effect.observed",
            fields={
                "observation_json": observation_json,
                "observation_digest": digest_json(observation),
            },
        )

    def commit_effect(self, transition_id: str, effect_key: str) -> None:
        self._effect_update(
            transition_id=transition_id,
            effect_key=effect_key,
            allowed={EffectPhase.OBSERVED},
            phase=EffectPhase.COMMITTED,
            event="effect.committed",
        )

    def all_effects_committed(self, transition_id: str) -> bool:
        with closing(self._connect()) as conn:
            return (
                conn.execute(
                    "SELECT COUNT(*) FROM effects WHERE transition_id=? AND phase<>?",
                    (transition_id, EffectPhase.COMMITTED),
                ).fetchone()[0]
                == 0
            )

    def upgrade_attestation_consumption(
        self, transition_id: str, effect_key: str
    ) -> dict[str, Any] | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM upgrade_attestation_consumptions "
                "WHERE transition_id=? AND effect_key=?",
                (transition_id, effect_key),
            ).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["attestation"] = json.loads(result.pop("attestation_json"))
        return result

    def consume_upgrade_attestation(
        self,
        *,
        transition_id: str,
        effect_key: str,
        attestation: UpgradeAttestation,
        now: int | None = None,
    ) -> dict[str, Any]:
        consumed_at = int(time.time()) if now is None else now
        with self._transaction() as conn:
            effect = conn.execute(
                "SELECT kind, phase FROM effects "
                "WHERE transition_id=? AND effect_key=?",
                (transition_id, effect_key),
            ).fetchone()
            if effect is None or effect["kind"] != UPGRADE_EFFECT_KIND:
                raise LedgerError("upgrade attestation effect binding is invalid")
            if effect["phase"] != EffectPhase.COMMITTED:
                raise LedgerError(
                    "upgrade attestation cannot precede committed observation"
                )
            existing_id = conn.execute(
                "SELECT * FROM upgrade_attestation_consumptions WHERE attestation_id=?",
                (attestation.attestation_id,),
            ).fetchone()
            if existing_id is not None:
                if (
                    existing_id["transition_id"] != transition_id
                    or existing_id["effect_key"] != effect_key
                    or existing_id["content_digest"] != attestation.content_digest
                ):
                    raise LedgerError("upgrade attestation replay was refused")
                return dict(existing_id)
            occupied = conn.execute(
                "SELECT attestation_id, content_digest "
                "FROM upgrade_attestation_consumptions "
                "WHERE transition_id=? AND effect_key=?",
                (transition_id, effect_key),
            ).fetchone()
            if occupied is not None:
                raise LedgerError("upgrade effect already consumed another attestation")
            if (
                consumed_at < attestation.issued_at
                or consumed_at >= attestation.expires_at
            ):
                raise LedgerError("upgrade attestation is not currently fresh")
            conn.execute(
                "INSERT INTO upgrade_attestation_consumptions "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    attestation.attestation_id,
                    transition_id,
                    effect_key,
                    attestation.content_digest,
                    canonical_json(attestation.payload()),
                    consumed_at,
                ),
            )
            self._event(
                conn,
                transition_id,
                "upgrade_attestation.consumed",
                {
                    "effect_key": effect_key,
                    "attestation_id": attestation.attestation_id,
                    "content_digest": attestation.content_digest,
                },
            )
        result = self.upgrade_attestation_consumption(transition_id, effect_key)
        assert result is not None
        return result

    def authorize_activation(
        self,
        *,
        transition_id: str,
        staged_generation_digest: str,
        signer: AuthoritySigner,
    ) -> ActivationReceipt:
        with self._transaction() as conn:
            transition = conn.execute(
                "SELECT * FROM transitions WHERE transition_id=?", (transition_id,)
            ).fetchone()
            if transition is None:
                raise LedgerError(f"unknown transition: {transition_id}")
            authority = conn.execute(
                "SELECT current_transition_id, max_lifecycle_activation_epoch "
                "FROM deployment_authority WHERE deployment=?",
                (transition["deployment"],),
            ).fetchone()
            if (
                authority is None
                or authority["current_transition_id"] != transition_id
                or authority["max_lifecycle_activation_epoch"]
                != transition["lifecycle_activation_epoch"]
            ):
                raise LedgerError("activation attempt is not current")
            existing = conn.execute(
                "SELECT receipt_json FROM activation_receipts "
                "WHERE transition_id=? AND receipt_kind='candidate'",
                (transition_id,),
            ).fetchone()
            incomplete = conn.execute(
                "SELECT COUNT(*) FROM effects WHERE transition_id=? "
                "AND effect_key<>'unfreeze' AND phase<>?",
                (transition_id, EffectPhase.COMMITTED),
            ).fetchone()[0]
            if incomplete:
                raise LedgerError(
                    "activation cannot precede committed effect observation"
                )
            required_attestations = conn.execute(
                "SELECT COUNT(*) FROM effects WHERE transition_id=? AND kind=?",
                (transition_id, UPGRADE_EFFECT_KIND),
            ).fetchone()[0]
            consumed_attestations = conn.execute(
                "SELECT COUNT(*) FROM upgrade_attestation_consumptions "
                "WHERE transition_id=?",
                (transition_id,),
            ).fetchone()[0]
            if consumed_attestations != required_attestations:
                raise LedgerError(
                    "activation requires every upgrade attestation consumed"
                )
            if existing is not None:
                return ActivationReceipt(**json.loads(existing["receipt_json"]))
            plan = json.loads(transition["plan_json"])
            receipt = signer.receipt(
                transition_id=transition_id,
                ledger_uuid=plan["ledger_uuid"],
                controller_trust_root_id=plan["controller_trust_root_id"],
                deployment=transition["deployment"],
                lifecycle_activation_epoch=transition["lifecycle_activation_epoch"],
                candidate_digest=transition["candidate_digest"],
                staged_generation_digest=staged_generation_digest,
                expected_predecessor=transition["expected_predecessor"],
                expected_activation_receipt_digest=plan[
                    "expected_activation_receipt_digest"
                ],
                expected_database_writer_epoch=plan[
                    "expected_database_writer_epoch"
                ],
                database_writer_epoch=transition["database_writer_epoch"],
                dbos_application_version=plan["dbos_application_version"],
                effect_set_digest=plan["effect_set_digest"],
                port_manifest_digest=plan["port_manifest_digest"],
                service_plan_digest=plan["service_plan_digest"],
            )
            conn.execute(
                "INSERT INTO activation_receipts "
                "VALUES (?, ?, 'candidate', ?, ?, ?, 0, 0, 0)",
                (
                    receipt.digest,
                    transition_id,
                    receipt.lifecycle_activation_epoch,
                    receipt.database_writer_epoch,
                    canonical_json(asdict(receipt)),
                ),
            )
            conn.execute(
                "UPDATE transitions SET state=?, updated_at=CURRENT_TIMESTAMP "
                "WHERE transition_id=?",
                (TransitionState.AUTHORIZED, transition_id),
            )
            self._event(
                conn,
                transition_id,
                "activation.authorized",
                {"activation_receipt_digest": receipt.digest},
            )
            return receipt

    @staticmethod
    def _expected_compensation_plan(
        conn: sqlite3.Connection,
        *,
        transition_id: str,
        rollback_receipt: ActivationReceipt,
    ) -> list[dict[str, Any]]:
        forward_effects = conn.execute(
            "SELECT effect_key, ordinal, kind, request_json, request_digest "
            "FROM effects WHERE transition_id=? AND phase IN (?, ?, ?, ?) "
            "ORDER BY ordinal DESC",
            (
                transition_id,
                EffectPhase.INTENDED,
                EffectPhase.APPLIED,
                EffectPhase.OBSERVED,
                EffectPhase.COMMITTED,
            ),
        ).fetchall()
        expected: list[dict[str, Any]] = []
        compensation_predecessors: tuple[str, ...] = ()
        for ordinal, forward in enumerate(forward_effects, start=1):
            compensation_key = f"compensate:{forward['effect_key']}"
            compensation_request = {
                "schema": "okg.local-controller.compensation-request/v1",
                "transition_id": transition_id,
                "compensation_key": compensation_key,
                "forward_effect_key": forward["effect_key"],
                "forward_request": json.loads(forward["request_json"]),
                "forward_request_digest": forward["request_digest"],
                "rollback_receipt_digest": rollback_receipt.digest,
                "expected_database_writer_epoch": (
                    rollback_receipt.expected_database_writer_epoch
                ),
                "database_writer_epoch": rollback_receipt.database_writer_epoch,
                "port_manifest_digest": rollback_receipt.port_manifest_digest,
            }
            request_json = canonical_json(compensation_request)
            expected.append(
                {
                    "transition_id": transition_id,
                    "compensation_key": compensation_key,
                    "ordinal": ordinal,
                    "forward_effect_key": forward["effect_key"],
                    "kind": forward["kind"],
                    "request_json": request_json,
                    "request_digest": digest_json(compensation_request),
                    "predecessors_json": canonical_json(
                        list(compensation_predecessors)
                    ),
                }
            )
            compensation_predecessors = (compensation_key,)
        return expected

    @staticmethod
    def _validate_compensation_plan(
        *,
        existing: Sequence[sqlite3.Row],
        expected: Sequence[Mapping[str, Any]],
    ) -> None:
        if len(existing) != len(expected):
            raise LedgerError(
                "rollback compensation plan is partial or has extra effects"
            )
        immutable_fields = (
            "transition_id",
            "compensation_key",
            "ordinal",
            "forward_effect_key",
            "kind",
            "request_json",
            "request_digest",
            "predecessors_json",
        )
        for actual, planned in zip(existing, expected, strict=True):
            mismatches = {
                field: {"expected": planned[field], "observed": actual[field]}
                for field in immutable_fields
                if actual[field] != planned[field]
            }
            if mismatches:
                raise LedgerError(
                    "rollback compensation plan conflicts with durable intent: "
                    + canonical_json(mismatches)
                )
            try:
                EffectPhase(actual["phase"])
            except ValueError as exc:
                raise LedgerError(
                    "rollback compensation plan has an invalid durable phase"
                ) from exc

    def _materialize_compensation_plan(
        self,
        conn: sqlite3.Connection,
        *,
        transition_id: str,
        rollback_receipt: ActivationReceipt,
    ) -> None:
        expected = self._expected_compensation_plan(
            conn,
            transition_id=transition_id,
            rollback_receipt=rollback_receipt,
        )
        existing = conn.execute(
            "SELECT * FROM compensations WHERE transition_id=? ORDER BY ordinal",
            (transition_id,),
        ).fetchall()
        if existing:
            self._validate_compensation_plan(existing=existing, expected=expected)
            return
        for row in expected:
            conn.execute(
                "INSERT INTO compensations VALUES "
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL)",
                (
                    row["transition_id"],
                    row["compensation_key"],
                    row["ordinal"],
                    row["forward_effect_key"],
                    row["kind"],
                    row["request_json"],
                    row["request_digest"],
                    row["predecessors_json"],
                    EffectPhase.PLANNED,
                ),
            )
        self._event(
            conn,
            transition_id,
            "rollback.compensation_plan_materialized",
            {
                "rollback_receipt_digest": rollback_receipt.digest,
                "compensation_count": len(expected),
                "compensation_plan_digest": digest_json(expected),
            },
        )

    def _assert_rollback_compensated(
        self,
        conn: sqlite3.Connection,
        *,
        transition_id: str,
        rollback_receipt: ActivationReceipt,
    ) -> None:
        expected = self._expected_compensation_plan(
            conn,
            transition_id=transition_id,
            rollback_receipt=rollback_receipt,
        )
        existing = conn.execute(
            "SELECT * FROM compensations WHERE transition_id=? ORDER BY ordinal",
            (transition_id,),
        ).fetchall()
        self._validate_compensation_plan(existing=existing, expected=expected)
        if any(row["phase"] != EffectPhase.COMMITTED for row in existing):
            raise LedgerError(
                "rollback publication requires every compensation committed"
            )

    def compensations(self, transition_id: str) -> list[dict[str, Any]]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM compensations WHERE transition_id=? ORDER BY ordinal",
                (transition_id,),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["request"] = json.loads(item.pop("request_json"))
            item["predecessors"] = tuple(json.loads(item.pop("predecessors_json")))
            if item.get("observation_json"):
                item["observation"] = json.loads(item.pop("observation_json"))
            result.append(item)
        return result

    def _compensation_update(
        self,
        *,
        transition_id: str,
        compensation_key: str,
        allowed: set[str],
        phase: EffectPhase,
        fields: Mapping[str, Any] | None = None,
    ) -> None:
        values = dict(fields or {})
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT phase FROM compensations WHERE transition_id=? "
                "AND compensation_key=?",
                (transition_id, compensation_key),
            ).fetchone()
            if row is None:
                raise LedgerError("unknown compensation effect")
            if row["phase"] == phase:
                return
            if row["phase"] not in allowed:
                raise LedgerError(
                    f"compensation {compensation_key} cannot move from "
                    f"{row['phase']} to {phase}"
                )
            assignments = ["phase=?"]
            params: list[Any] = [phase]
            for key, value in values.items():
                assignments.append(f"{key}=?")
                params.append(value)
            params.extend((transition_id, compensation_key))
            conn.execute(
                f"UPDATE compensations SET {', '.join(assignments)} "
                "WHERE transition_id=? AND compensation_key=?",
                params,
            )

    def intend_compensation(self, transition_id: str, compensation_key: str) -> None:
        rows = self.compensations(transition_id)
        row = next(
            (item for item in rows if item["compensation_key"] == compensation_key),
            None,
        )
        if row is None:
            raise LedgerError("unknown compensation effect")
        committed = {
            item["compensation_key"]
            for item in rows
            if item["phase"] == EffectPhase.COMMITTED
        }
        if not set(row["predecessors"]) <= committed:
            raise LedgerError("compensation predecessors are not committed")
        self._compensation_update(
            transition_id=transition_id,
            compensation_key=compensation_key,
            allowed={EffectPhase.PLANNED},
            phase=EffectPhase.INTENDED,
        )

    def mark_compensation_applied(
        self, transition_id: str, compensation_key: str, applied_digest: str
    ) -> None:
        self._compensation_update(
            transition_id=transition_id,
            compensation_key=compensation_key,
            allowed={EffectPhase.INTENDED},
            phase=EffectPhase.APPLIED,
            fields={"applied_digest": applied_digest},
        )

    def observe_compensation(
        self,
        transition_id: str,
        compensation_key: str,
        observation: Mapping[str, Any],
    ) -> None:
        self._compensation_update(
            transition_id=transition_id,
            compensation_key=compensation_key,
            allowed={EffectPhase.INTENDED, EffectPhase.APPLIED},
            phase=EffectPhase.OBSERVED,
            fields={
                "observation_json": canonical_json(observation),
                "observation_digest": digest_json(observation),
            },
        )

    def commit_compensation(self, transition_id: str, compensation_key: str) -> None:
        self._compensation_update(
            transition_id=transition_id,
            compensation_key=compensation_key,
            allowed={EffectPhase.OBSERVED},
            phase=EffectPhase.COMMITTED,
        )

    def receipt(self, transition_id: str, kind: str = "candidate") -> ActivationReceipt:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT receipt_json FROM activation_receipts "
                "WHERE transition_id=? AND receipt_kind=?",
                (transition_id, kind),
            ).fetchone()
        if row is None:
            raise LedgerError(f"transition has no {kind} receipt")
        return ActivationReceipt(**json.loads(row["receipt_json"]))

    def receipt_status(self, receipt_digest: str) -> dict[str, Any]:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT receipt_digest, transition_id, receipt_kind, invalidated, "
                "published, observed FROM activation_receipts WHERE receipt_digest=?",
                (receipt_digest,),
            ).fetchone()
        if row is None:
            raise LedgerError("unknown activation receipt")
        return dict(row)

    def record_publication(
        self,
        transition_id: str,
        receipt: ActivationReceipt,
        outcome: Mapping[str, Any],
        *,
        kind: str = "candidate",
    ) -> None:
        outcome_digest = digest_json(outcome)
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT receipt_digest, invalidated, published "
                "FROM activation_receipts "
                "WHERE transition_id=? AND receipt_kind=?",
                (transition_id, kind),
            ).fetchone()
            if row is None or row["receipt_digest"] != receipt.digest:
                raise LedgerError("publication does not match committed authorization")
            if row["invalidated"]:
                raise LedgerError("invalidated activation receipt cannot publish")
            if kind == "rollback":
                self._assert_rollback_compensated(
                    conn,
                    transition_id=transition_id,
                    rollback_receipt=receipt,
                )
            if row["published"]:
                return
            conn.execute(
                "UPDATE activation_receipts SET published=1 WHERE receipt_digest=?",
                (receipt.digest,),
            )
            state = (
                TransitionState.PUBLISHED
                if kind == "candidate"
                else TransitionState.RECOVERING
            )
            conn.execute(
                "UPDATE transitions SET state=?, published_receipt_digest=?, "
                "updated_at=CURRENT_TIMESTAMP WHERE transition_id=?",
                (state, receipt.digest, transition_id),
            )
            self._event(
                conn,
                transition_id,
                f"activation.{kind}.published",
                {
                    "activation_receipt_digest": receipt.digest,
                    "outcome_digest": outcome_digest,
                },
            )

    def finalize_observation(
        self,
        transition_id: str,
        receipt: ActivationReceipt,
        observation: Mapping[str, Any],
        active_capability: Mapping[str, Any],
        *,
        kind: str = "candidate",
    ) -> None:
        observation_digest = digest_json(observation)
        with self._transaction() as conn:
            transition = conn.execute(
                "SELECT deployment FROM transitions WHERE transition_id=?",
                (transition_id,),
            ).fetchone()
            authority = (
                conn.execute(
                    "SELECT current_transition_id, "
                    "max_lifecycle_activation_epoch "
                    "FROM deployment_authority WHERE deployment=?",
                    (transition["deployment"],),
                ).fetchone()
                if transition is not None
                else None
            )
            if (
                authority is None
                or authority["current_transition_id"] != transition_id
                or authority["max_lifecycle_activation_epoch"]
                != receipt.lifecycle_activation_epoch
            ):
                raise LedgerError("final observation attempt is not current")
            row = conn.execute(
                "SELECT receipt_digest, invalidated, published, observed "
                "FROM activation_receipts WHERE transition_id=? AND receipt_kind=?",
                (transition_id, kind),
            ).fetchone()
            if row is None or row["receipt_digest"] != receipt.digest:
                raise LedgerError("final observation does not match authorization")
            if row["invalidated"] or not row["published"]:
                raise LedgerError("final observation requires current publication")
            if kind == "rollback":
                self._assert_rollback_compensated(
                    conn,
                    transition_id=transition_id,
                    rollback_receipt=receipt,
                )
            if row["observed"]:
                return
            conn.execute(
                "UPDATE activation_receipts SET observed=1 WHERE receipt_digest=?",
                (receipt.digest,),
            )
            state = (
                TransitionState.READY
                if kind == "candidate"
                else TransitionState.ROLLED_BACK
            )
            conn.execute(
                "UPDATE transitions SET state=?, final_observation_digest=?, "
                "active_capability_json=?, updated_at=CURRENT_TIMESTAMP "
                "WHERE transition_id=?",
                (
                    state,
                    observation_digest,
                    canonical_json(active_capability),
                    transition_id,
                ),
            )
            self._event(
                conn,
                transition_id,
                f"activation.{kind}.observed",
                {
                    "activation_receipt_digest": receipt.digest,
                    "observation_digest": observation_digest,
                },
            )

    def authorize_rollback(
        self,
        *,
        transition_id: str,
        lifecycle_activation_epoch: int,
        database_writer_epoch: int,
        signer: AuthoritySigner,
    ) -> ActivationReceipt:
        with self._transaction() as conn:
            transition = conn.execute(
                "SELECT * FROM transitions WHERE transition_id=?", (transition_id,)
            ).fetchone()
            if transition is None:
                raise LedgerError(f"unknown transition: {transition_id}")
            authority = conn.execute(
                "SELECT current_transition_id, max_lifecycle_activation_epoch "
                "FROM deployment_authority WHERE deployment=?",
                (transition["deployment"],),
            ).fetchone()
            if authority is None or authority["current_transition_id"] != transition_id:
                raise LedgerError("rollback attempt is not current")
            failed = conn.execute(
                "SELECT * FROM activation_receipts WHERE transition_id=? "
                "AND receipt_kind='candidate'",
                (transition_id,),
            ).fetchone()
            existing = conn.execute(
                "SELECT receipt_json FROM activation_receipts "
                "WHERE transition_id=? AND receipt_kind='rollback'",
                (transition_id,),
            ).fetchone()
            if existing is not None:
                receipt = ActivationReceipt(**json.loads(existing["receipt_json"]))
                expected_staged_predecessor = rollback_generation_digest(
                    predecessor_digest=transition["predecessor_digest"],
                    expected_predecessor=transition["expected_predecessor"],
                )
                if (
                    receipt.staged_generation_digest
                    != expected_staged_predecessor
                    or receipt.lifecycle_activation_epoch != lifecycle_activation_epoch
                    or receipt.database_writer_epoch != database_writer_epoch
                ):
                    raise LedgerError(
                        "rollback retry disagrees with the durable rollback intent"
                    )
                self._materialize_compensation_plan(
                    conn,
                    transition_id=transition_id,
                    rollback_receipt=receipt,
                )
                return receipt
            if failed is None and not transition["irreversible_crossed"]:
                raise LedgerError(
                    "preauthorization rollback requires an irreversible boundary"
                )
            failed_lifecycle_epoch = (
                failed["lifecycle_activation_epoch"]
                if failed is not None
                else transition["lifecycle_activation_epoch"]
            )
            failed_database_writer_epoch = (
                failed["database_writer_epoch"]
                if failed is not None
                else transition["database_writer_epoch"]
            )
            plan = json.loads(transition["plan_json"])
            rollback_expected_database_writer_epoch = (
                failed_database_writer_epoch
                if failed is not None and failed["published"]
                else plan["expected_database_writer_epoch"]
            )
            authority_epoch = authority["max_lifecycle_activation_epoch"]
            if lifecycle_activation_epoch <= max(
                failed_lifecycle_epoch, authority_epoch
            ):
                raise LedgerError("rollback lifecycle epoch must be strictly newer")
            if database_writer_epoch <= failed_database_writer_epoch:
                raise LedgerError(
                    "rollback database writer epoch must be strictly newer"
                )
            failed_payload = (
                json.loads(failed["receipt_json"]) if failed is not None else None
            )
            staged_predecessor_digest = rollback_generation_digest(
                predecessor_digest=transition["predecessor_digest"],
                expected_predecessor=transition["expected_predecessor"],
            )
            rollback_expected_predecessor = (
                failed_payload["staged_generation_digest"]
                if failed is not None and failed["published"]
                else transition["expected_predecessor"]
            )
            rollback_expected_receipt = (
                failed["receipt_digest"]
                if failed is not None and failed["published"]
                else plan["expected_activation_receipt_digest"]
            )
            receipt = signer.receipt(
                transition_id=transition_id,
                ledger_uuid=plan["ledger_uuid"],
                controller_trust_root_id=plan["controller_trust_root_id"],
                deployment=transition["deployment"],
                lifecycle_activation_epoch=lifecycle_activation_epoch,
                candidate_digest=transition["predecessor_digest"],
                staged_generation_digest=staged_predecessor_digest,
                expected_predecessor=rollback_expected_predecessor,
                expected_activation_receipt_digest=rollback_expected_receipt,
                expected_database_writer_epoch=(
                    rollback_expected_database_writer_epoch
                ),
                database_writer_epoch=database_writer_epoch,
                dbos_application_version=plan["dbos_application_version"],
                effect_set_digest=plan["effect_set_digest"],
                port_manifest_digest=plan["port_manifest_digest"],
                service_plan_digest=plan["service_plan_digest"],
            )
            if failed is not None:
                conn.execute(
                    "UPDATE activation_receipts SET invalidated=1 "
                    "WHERE receipt_digest=?",
                    (failed["receipt_digest"],),
                )
                conn.execute(
                    "INSERT INTO invalidated_capabilities VALUES (?, ?, ?)",
                    (
                        failed["receipt_digest"],
                        receipt.digest,
                        "strict-newer rollback",
                    ),
                )
            conn.execute(
                "INSERT INTO activation_receipts "
                "VALUES (?, ?, 'rollback', ?, ?, ?, 0, 0, 0)",
                (
                    receipt.digest,
                    transition_id,
                    receipt.lifecycle_activation_epoch,
                    receipt.database_writer_epoch,
                    canonical_json(asdict(receipt)),
                ),
            )
            self._materialize_compensation_plan(
                conn,
                transition_id=transition_id,
                rollback_receipt=receipt,
            )
            conn.execute(
                "UPDATE deployment_authority SET "
                "max_lifecycle_activation_epoch=?, updated_at=CURRENT_TIMESTAMP "
                "WHERE deployment=? AND current_transition_id=?",
                (
                    lifecycle_activation_epoch,
                    transition["deployment"],
                    transition_id,
                ),
            )
            conn.execute(
                "UPDATE transitions SET state=?, active_capability_json=NULL, "
                "updated_at=CURRENT_TIMESTAMP "
                "WHERE transition_id=?",
                (TransitionState.RECOVERING, transition_id),
            )
            self._event(
                conn,
                transition_id,
                "rollback.authorized",
                {
                    "failed_receipt_digest": (
                        failed["receipt_digest"] if failed is not None else None
                    ),
                    "preauthorization_recovery": failed is None,
                    "rollback_receipt_digest": receipt.digest,
                },
            )
            return receipt

    def mark_retired(
        self, transition_id: str, *, observation: Mapping[str, Any]
    ) -> None:
        retirement = self.retirement(transition_id)
        if any(
            effect["phase"] != EffectPhase.COMMITTED
            for effect in self.retirement_effects(retirement["retirement_id"])
        ):
            raise LedgerError(
                "retirement requires every retirement effect observed and committed"
            )
        with self._transaction() as conn:
            receipt = conn.execute(
                "SELECT receipt_digest FROM activation_receipts "
                "WHERE transition_id=? AND receipt_digest=("
                "SELECT published_receipt_digest FROM transitions WHERE transition_id=?"
                ")",
                (transition_id, transition_id),
            ).fetchone()
            if receipt is None:
                raise LedgerError("retirement requires a current activation receipt")
            conn.execute(
                "UPDATE activation_receipts SET invalidated=1 WHERE receipt_digest=?",
                (receipt["receipt_digest"],),
            )
            conn.execute(
                "INSERT OR IGNORE INTO invalidated_capabilities VALUES (?, ?, ?)",
                (
                    receipt["receipt_digest"],
                    retirement["retirement_id"],
                    "deployment retired",
                ),
            )
            conn.execute(
                "UPDATE transitions SET state=?, active_capability_json=NULL, "
                "updated_at=CURRENT_TIMESTAMP "
                "WHERE transition_id=?",
                (TransitionState.RETIRED, transition_id),
            )
            conn.execute(
                "UPDATE retirements SET state=?, final_observation_digest=?, "
                "updated_at=CURRENT_TIMESTAMP "
                "WHERE retirement_id=?",
                (
                    TransitionState.RETIRED,
                    digest_json(observation),
                    retirement["retirement_id"],
                ),
            )
            self._event(conn, transition_id, "deployment.retired", {})

    def create_retirement(
        self,
        *,
        transition_id: str,
        receipt: ActivationReceipt,
        port_manifest_digest: str,
        effects: Sequence[EffectSpec],
    ) -> dict[str, Any]:
        transition = self.transition(transition_id)
        if transition["state"] == TransitionState.RETIRED:
            return self.retirement(transition_id)
        if transition["state"] not in {
            TransitionState.READY,
            TransitionState.ROLLED_BACK,
            TransitionState.RETIRING,
        }:
            raise LedgerError("only a ready deployment may enter retirement")
        plan = transition["plan"]
        if plan["port_manifest_digest"] != port_manifest_digest:
            raise LedgerError("retirement adapter set differs from activation")
        identity = {
            "schema": "okg.local-controller.retirement-plan/v1",
            "transition_id": transition_id,
            "activation_receipt_digest": receipt.digest,
            "ledger_uuid": plan["ledger_uuid"],
            "controller_trust_root_id": plan["controller_trust_root_id"],
            "port_manifest_digest": port_manifest_digest,
            "effect_set_digest": digest_json([effect.payload() for effect in effects]),
        }
        retirement_id = "controller-retirement-" + digest_json(identity).removeprefix(
            "sha256:"
        )
        retirement_plan = {**identity, "retirement_id": retirement_id}
        plan_digest = digest_json(retirement_plan)
        with self._transaction() as conn:
            authority = conn.execute(
                "SELECT current_transition_id FROM deployment_authority "
                "WHERE deployment=?",
                (transition["deployment"],),
            ).fetchone()
            if authority is None or authority["current_transition_id"] != transition_id:
                raise LedgerError("retirement attempt is not current")
            existing = conn.execute(
                "SELECT * FROM retirements WHERE transition_id=?", (transition_id,)
            ).fetchone()
            if existing is not None:
                if (
                    existing["retirement_id"] != retirement_id
                    or existing["plan_digest"] != plan_digest
                ):
                    raise LedgerError(
                        "retirement retry disagrees with durable retirement intent"
                    )
                return self._retirement_row(existing)
            conn.execute(
                "INSERT INTO retirements(retirement_id, transition_id, plan_digest, "
                "plan_json, state) VALUES (?, ?, ?, ?, ?)",
                (
                    retirement_id,
                    transition_id,
                    plan_digest,
                    canonical_json(retirement_plan),
                    TransitionState.RETIRING,
                ),
            )
            for ordinal, effect in enumerate(effects, start=1):
                conn.execute(
                    "INSERT INTO retirement_effects VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL)",
                    (
                        retirement_id,
                        effect.key,
                        ordinal,
                        effect.kind,
                        canonical_json(effect.request),
                        effect.request_digest,
                        canonical_json(list(effect.predecessors)),
                        EffectPhase.PLANNED,
                    ),
                )
            conn.execute(
                "UPDATE transitions SET state=?, active_capability_json=NULL, "
                "updated_at=CURRENT_TIMESTAMP WHERE transition_id=?",
                (TransitionState.RETIRING, transition_id),
            )
            self._event(
                conn,
                transition_id,
                "retirement.intent",
                {"retirement_id": retirement_id, "plan_digest": plan_digest},
            )
        return self.retirement(transition_id)

    @staticmethod
    def _retirement_row(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(row)
        result["plan"] = json.loads(result.pop("plan_json"))
        return result

    def retirement(self, transition_id: str) -> dict[str, Any]:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM retirements WHERE transition_id=?", (transition_id,)
            ).fetchone()
        if row is None:
            raise LedgerError("transition has no retirement intent")
        return self._retirement_row(row)

    def retirement_effects(self, retirement_id: str) -> list[dict[str, Any]]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM retirement_effects WHERE retirement_id=? "
                "ORDER BY ordinal",
                (retirement_id,),
            ).fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["transition_id"] = self.retirement_by_id(retirement_id)[
                "transition_id"
            ]
            item["request"] = json.loads(item.pop("request_json"))
            item["predecessors"] = tuple(json.loads(item.pop("predecessors_json")))
            if item.get("observation_json"):
                item["observation"] = json.loads(item.pop("observation_json"))
            result.append(item)
        return result

    def retirement_by_id(self, retirement_id: str) -> dict[str, Any]:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM retirements WHERE retirement_id=?", (retirement_id,)
            ).fetchone()
        if row is None:
            raise LedgerError("unknown retirement intent")
        return self._retirement_row(row)

    def _retirement_effect_update(
        self,
        *,
        retirement_id: str,
        effect_key: str,
        allowed: set[str],
        phase: EffectPhase,
        fields: Mapping[str, Any] | None = None,
    ) -> None:
        values = dict(fields or {})
        with self._transaction() as conn:
            row = conn.execute(
                "SELECT phase FROM retirement_effects WHERE retirement_id=? "
                "AND effect_key=?",
                (retirement_id, effect_key),
            ).fetchone()
            if row is None:
                raise LedgerError("unknown retirement effect")
            if row["phase"] == phase:
                return
            if row["phase"] not in allowed:
                raise LedgerError(
                    f"retirement effect {effect_key} cannot move from "
                    f"{row['phase']} to {phase}"
                )
            assignments = ["phase=?"]
            params: list[Any] = [phase]
            for key, value in values.items():
                assignments.append(f"{key}=?")
                params.append(value)
            params.extend((retirement_id, effect_key))
            conn.execute(
                f"UPDATE retirement_effects SET {', '.join(assignments)} "
                "WHERE retirement_id=? AND effect_key=?",
                params,
            )

    def intend_retirement_effect(self, retirement_id: str, effect_key: str) -> None:
        effects = self.retirement_effects(retirement_id)
        row = next((item for item in effects if item["effect_key"] == effect_key), None)
        if row is None:
            raise LedgerError("unknown retirement effect")
        committed = {
            item["effect_key"]
            for item in effects
            if item["phase"] == EffectPhase.COMMITTED
        }
        if not set(row["predecessors"]) <= committed:
            raise LedgerError("retirement effect predecessors are not committed")
        self._retirement_effect_update(
            retirement_id=retirement_id,
            effect_key=effect_key,
            allowed={EffectPhase.PLANNED},
            phase=EffectPhase.INTENDED,
        )

    def mark_retirement_effect_applied(
        self, retirement_id: str, effect_key: str, applied_digest: str
    ) -> None:
        self._retirement_effect_update(
            retirement_id=retirement_id,
            effect_key=effect_key,
            allowed={EffectPhase.INTENDED},
            phase=EffectPhase.APPLIED,
            fields={"applied_digest": applied_digest},
        )

    def observe_retirement_effect(
        self, retirement_id: str, effect_key: str, observation: Mapping[str, Any]
    ) -> None:
        self._retirement_effect_update(
            retirement_id=retirement_id,
            effect_key=effect_key,
            allowed={EffectPhase.INTENDED, EffectPhase.APPLIED},
            phase=EffectPhase.OBSERVED,
            fields={
                "observation_json": canonical_json(observation),
                "observation_digest": digest_json(observation),
            },
        )

    def commit_retirement_effect(self, retirement_id: str, effect_key: str) -> None:
        self._retirement_effect_update(
            retirement_id=retirement_id,
            effect_key=effect_key,
            allowed={EffectPhase.OBSERVED},
            phase=EffectPhase.COMMITTED,
        )

    def events(self, transition_id: str) -> list[dict[str, Any]]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT sequence, event_type, payload_json FROM events "
                "WHERE transition_id=? ORDER BY sequence",
                (transition_id,),
            ).fetchall()
        return [
            {
                "sequence": row["sequence"],
                "event_type": row["event_type"],
                "payload": json.loads(row["payload_json"]),
            }
            for row in rows
        ]
