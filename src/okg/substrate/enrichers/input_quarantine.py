"""Durable, transaction-scoped retry/quarantine ledger for enricher inputs.

The publisher deployment lock excludes concurrent cycles.  This module adds
the item/signature layer below that lock: a claim is written on the publisher
connection before provider fanout, deterministic failures consume an explicit
budget, and terminal quarantine is committed with ``enricher_progress``.

No helper commits.  Callers own the transaction boundary.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

import psycopg

from okg.substrate.llm.outcomes import OutcomeKey

__all__ = [
    "ActiveQuarantine",
    "InputClaim",
    "InputQuarantineKey",
    "claim_inputs",
    "record_deterministic_failure",
    "record_success",
    "release_claim",
]


@dataclass(frozen=True)
class InputQuarantineKey:
    deployment_name: str
    branch_name: str
    enricher_name: str
    item_ref: str
    input_signature: str
    operator_version: str
    model_id: str
    prompt_hash: str
    input_hash: str

    @classmethod
    def from_outcome_key(
        cls,
        *,
        deployment_name: str,
        branch_name: str,
        enricher_name: str,
        item_ref: str,
        outcome_key: OutcomeKey,
    ) -> "InputQuarantineKey":
        values = {
            "deployment_name": deployment_name,
            "branch_name": branch_name,
            "enricher_name": enricher_name,
            "item_ref": item_ref,
        }
        for label, value in values.items():
            if not isinstance(value, str) or not value:
                raise ValueError(f"input quarantine {label} must be a non-empty string")
        return cls(
            **values,
            input_signature=outcome_key.digest,
            operator_version=outcome_key.operator_version,
            model_id=outcome_key.model_id,
            prompt_hash=outcome_key.prompt_hash,
            input_hash=outcome_key.input_hash,
        )

    @property
    def identity(self) -> tuple[str, str, str, str, str]:
        return (
            self.deployment_name,
            self.branch_name,
            self.enricher_name,
            self.item_ref,
            self.input_signature,
        )


@dataclass(frozen=True)
class InputClaim:
    key: InputQuarantineKey
    dispatch: bool
    claim_token: uuid.UUID | None
    attempt_count: int
    attempt_limit: int
    prior_status: str | None


@dataclass(frozen=True)
class ActiveQuarantine:
    key: InputQuarantineKey
    attempt_count: int
    attempt_limit: int
    last_failure_class: str
    last_error: str
    first_failed_at: datetime
    last_failed_at: datetime


def claim_inputs(
    conn: psycopg.Connection,
    keys: Iterable[InputQuarantineKey],
    *,
    attempt_limit: int,
    generation_id: int | str | None,
) -> dict[tuple[str, str, str, str, str], InputClaim]:
    """Claim each distinct item/signature once, without consuming a failure.

    An already-active signature is filtered before provider dispatch.  A
    resolved episode reopens with a fresh budget; history for other signatures
    is untouched until the new signature succeeds.
    """
    if not isinstance(attempt_limit, int) or isinstance(attempt_limit, bool):
        raise ValueError("attempt_limit must be a positive integer")
    if attempt_limit <= 0:
        raise ValueError("attempt_limit must be a positive integer")
    configured_attempt_limit = attempt_limit

    distinct = {key.identity: key for key in keys}
    claims: dict[tuple[str, str, str, str, str], InputClaim] = {}
    with conn.cursor() as cur:
        for identity, key in distinct.items():
            cur.execute(
                """
                SELECT status, attempt_count, attempt_limit
                  FROM okg.enricher_input_quarantines
                 WHERE deployment_name = %s
                   AND branch_name = %s
                   AND enricher_name = %s
                   AND item_ref = %s
                   AND input_signature = %s
                 FOR UPDATE
                """,
                identity,
            )
            row = cur.fetchone()
            if row is not None and str(row[0]) == "active":
                claims[identity] = InputClaim(
                    key=key,
                    dispatch=False,
                    claim_token=None,
                    attempt_count=int(row[1]),
                    attempt_limit=int(row[2]),
                    prior_status="active",
                )
                continue

            claim_token = uuid.uuid4()
            if row is None:
                cur.execute(
                    """
                    INSERT INTO okg.enricher_input_quarantines
                      (deployment_name, branch_name, enricher_name, item_ref,
                       input_signature, operator_version, model_id, prompt_hash,
                       input_hash, status, attempt_count, attempt_limit,
                       claim_token, claimed_at, last_generation_id, updated_at)
                    VALUES
                      (%s, %s, %s, %s, %s, %s, %s, %s, %s,
                       'retrying', 0, %s, %s, now(), %s, now())
                    """,
                    (
                        *identity,
                        key.operator_version,
                        key.model_id,
                        key.prompt_hash,
                        key.input_hash,
                        configured_attempt_limit,
                        claim_token,
                        None if generation_id is None else str(generation_id),
                    ),
                )
                attempt_count = 0
                prior_status = None
                claim_attempt_limit = configured_attempt_limit
            else:
                prior_status = str(row[0])
                prior_count = int(row[1])
                prior_limit = int(row[2])
                starts_new_episode = prior_status in {"resolved", "superseded"}
                attempt_count = 0 if starts_new_episode else prior_count
                # Pin the deployment-authored limit for one retry episode.
                # Changing config under a retrying row must not create an
                # impossible attempt_count > attempt_limit pair or move the
                # terminal threshold after failures have committed.  The new
                # limit applies after resolution or to a changed signature.
                claim_attempt_limit = (
                    configured_attempt_limit if starts_new_episode else prior_limit
                )
                cur.execute(
                    """
                    UPDATE okg.enricher_input_quarantines
                       SET status = 'retrying',
                           attempt_count = %s,
                           attempt_limit = %s,
                           claim_token = %s,
                           claimed_at = now(),
                           resolved_at = NULL,
                           last_generation_id = %s,
                           updated_at = now()
                     WHERE deployment_name = %s
                       AND branch_name = %s
                       AND enricher_name = %s
                       AND item_ref = %s
                       AND input_signature = %s
                    """,
                    (
                        attempt_count,
                        claim_attempt_limit,
                        claim_token,
                        None if generation_id is None else str(generation_id),
                        *identity,
                    ),
                )
            claims[identity] = InputClaim(
                key=key,
                dispatch=True,
                claim_token=claim_token,
                attempt_count=attempt_count,
                attempt_limit=claim_attempt_limit,
                prior_status=prior_status,
            )
    return claims


def _require_claim(cur, claim: InputClaim) -> None:
    if claim.claim_token is None:
        raise ValueError("cannot mutate an input quarantine without a claim token")
    cur.execute(
        """
        SELECT claim_token
          FROM okg.enricher_input_quarantines
         WHERE deployment_name = %s
           AND branch_name = %s
           AND enricher_name = %s
           AND item_ref = %s
           AND input_signature = %s
         FOR UPDATE
        """,
        claim.key.identity,
    )
    row = cur.fetchone()
    if row is None or row[0] != claim.claim_token:
        raise RuntimeError("input quarantine claim was lost before result accounting")


def release_claim(conn: psycopg.Connection, claim: InputClaim) -> None:
    """Release a systemic/refused attempt without consuming the budget."""
    with conn.cursor() as cur:
        _require_claim(cur, claim)
        if claim.prior_status is None and claim.attempt_count == 0:
            cur.execute(
                """
                DELETE FROM okg.enricher_input_quarantines
                 WHERE deployment_name = %s
                   AND branch_name = %s
                   AND enricher_name = %s
                   AND item_ref = %s
                   AND input_signature = %s
                """,
                claim.key.identity,
            )
        else:
            cur.execute(
                """
                UPDATE okg.enricher_input_quarantines
                   SET status = %s,
                       attempt_count = %s,
                       claim_token = NULL,
                       claimed_at = NULL,
                       resolved_at = CASE
                         WHEN %s = 'resolved' THEN COALESCE(resolved_at, now())
                         ELSE resolved_at
                       END,
                       updated_at = now()
                 WHERE deployment_name = %s
                   AND branch_name = %s
                   AND enricher_name = %s
                   AND item_ref = %s
                   AND input_signature = %s
                """,
                (
                    claim.prior_status or "retrying",
                    claim.attempt_count,
                    claim.prior_status,
                    *claim.key.identity,
                ),
            )


def record_deterministic_failure(
    conn: psycopg.Connection,
    claim: InputClaim,
    *,
    failure_class: str,
    error: str,
    generation_id: int | str | None,
) -> ActiveQuarantine | None:
    """Consume one committed deterministic failure; activate at the limit."""
    if not failure_class:
        raise ValueError("failure_class must be explicit")
    with conn.cursor() as cur:
        _require_claim(cur, claim)
        cur.execute(
            """
            UPDATE okg.enricher_input_quarantines
               SET attempt_count = attempt_count + 1,
                   status = CASE
                     WHEN attempt_count + 1 >= attempt_limit
                     THEN 'active'
                     ELSE 'retrying'
                   END,
                   last_failure_class = %s,
                   last_error = %s,
                   first_failed_at = COALESCE(first_failed_at, now()),
                   last_failed_at = now(),
                   quarantined_at = CASE
                     WHEN attempt_count + 1 >= attempt_limit
                     THEN COALESCE(quarantined_at, now())
                     ELSE quarantined_at
                   END,
                   claim_token = NULL,
                   claimed_at = NULL,
                   last_generation_id = %s,
                   updated_at = now()
             WHERE deployment_name = %s
               AND branch_name = %s
               AND enricher_name = %s
               AND item_ref = %s
               AND input_signature = %s
            RETURNING status, attempt_count, attempt_limit, last_failure_class,
                      last_error, first_failed_at, last_failed_at
            """,
            (
                failure_class,
                str(error)[:4000],
                None if generation_id is None else str(generation_id),
                *claim.key.identity,
            ),
        )
        row = cur.fetchone()
    if row is None:
        raise RuntimeError("input quarantine failure update matched no row")
    if str(row[0]) != "active":
        return None
    return ActiveQuarantine(
        key=claim.key,
        attempt_count=int(row[1]),
        attempt_limit=int(row[2]),
        last_failure_class=str(row[3]),
        last_error=str(row[4]),
        first_failed_at=row[5],
        last_failed_at=row[6],
    )


def record_success(conn: psycopg.Connection, claim: InputClaim) -> None:
    """Resolve the claimed signature and supersede older active findings."""
    with conn.cursor() as cur:
        _require_claim(cur, claim)
        cur.execute(
            """
            UPDATE okg.enricher_input_quarantines
               SET status = 'resolved',
                   attempt_count = 0,
                   claim_token = NULL,
                   claimed_at = NULL,
                   resolved_at = now(),
                   updated_at = now()
             WHERE deployment_name = %s
               AND branch_name = %s
               AND enricher_name = %s
               AND item_ref = %s
               AND input_signature = %s
            """,
            claim.key.identity,
        )
        cur.execute(
            """
            UPDATE okg.enricher_input_quarantines
               SET status = 'superseded',
                   superseded_at = now(),
                   updated_at = now()
             WHERE deployment_name = %s
               AND branch_name = %s
               AND enricher_name = %s
               AND item_ref = %s
               AND input_signature <> %s
               AND status = 'active'
            """,
            claim.key.identity,
        )
