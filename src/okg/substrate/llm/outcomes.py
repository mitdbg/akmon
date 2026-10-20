"""Keyed model outcomes (omega) — stable identity, retention, replay.

PACT ``consistency-model-outcome-replay``; normative semantics in
docs/develop/architecture/consistency.md §9.2; identifiers pinned by the
ratified notation map (``OutcomeKey``, ``RecordedOutcome``,
``okg.model_outcomes``).

The stance, stated once (amended by ``outcome-read-through``): wherever a
retention store exists, RECORD mode is read-through — a recorded outcome
is consumed instead of re-sampling, and only a store miss samples fresh
and records. The first sample per key is genuinely fresh (LLM
nondeterminism accepted, per okg.views.execution.llm); every later
consumption of that key returns what was recorded, which is what omega —
"the outcome we recorded", not "the outcome the model would always give"
— has always meant. The original fresh-sampling-on-every-call stance was
retired after PR 925 measured its cost with observed call counts: under
first-write-wins, each repeat sample was billed, published from, and then
discarded by the store, so crash recovery and every publish cycle
re-billed entire corpora for output with no durable effect. A NEW draw is
an explicit act: change any key component (operator_version, prompt,
model, input) or prune retention. Replay mode is unchanged — recorded
outcomes only; a missing key is a loud, recorded exclusion, never a
silent fresh sample. Lanes with no retention store still sample every
call (there is nothing to read through).

Retention is manifest-gated and bounded: an :class:`OutcomeStore` is
constructed only with an explicit positive ``max_rows``; there is no
unbounded-by-omission mode. Absent a policy, calls still compute and
persist their key on ``okg.llm_calls`` (spend joins to keys), but no
outcome body is retained and replay is impossible — loudly.

Recording is first-write-wins per key: omega assigns ONE outcome per key,
so a later fresh sample under the same key never rewrites recorded
history (``ON CONFLICT DO NOTHING``). A changed prompt, model, or
operator version changes the key itself, which is the entire
invalidation mechanism — old-key rows age out only via retention.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

logger = logging.getLogger(__name__)

_SEP = "\x1f"  # unit separator — cannot appear in sha256 hexdigests


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def canonical_input_hash(typed_input: Any) -> str:
    """sha256 over the canonical JSON of the call site's typed input.

    The typed input is the structured value the call site assembled (source
    ids, bodies, vocabularies) — NOT the rendered prompt string; the rendered
    user message is a deterministic function of (prompt template, typed
    input), so (prompt_hash, input_hash) pins what was sent.
    """
    canonical = json.dumps(
        typed_input, sort_keys=True, ensure_ascii=False, separators=(",", ":"),
        default=str,
    )
    return _sha256_text(canonical)


def prompt_content_hash(prompt_text: str) -> str:
    """sha256 of the exact prompt content as sent (inline constant or file)."""
    return _sha256_text(prompt_text)


@dataclass(frozen=True)
class OutcomeKey:
    """Stable identity of one model-backed invocation (spec §9.2).

    Five components, all mandatory: a missing component is a loud error at
    construction, never a keyless call. Two invocations of the same step on
    the same typed input under the same operator / model / prompt versions
    produce the same key; changing any component produces a different key.
    """

    step_id: str            # e.g. "enricher:claim_extractor"
    operator_version: str   # declared per call site; bumped on semantic change
    model_id: str           # the client's model string
    prompt_hash: str        # prompt_content_hash(...) of the content sent
    input_hash: str         # canonical_input_hash(...) of the typed input

    def __post_init__(self) -> None:
        for name in ("step_id", "operator_version", "model_id",
                     "prompt_hash", "input_hash"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise ValueError(
                    f"OutcomeKey.{name} must be a non-empty string; a "
                    f"model-backed invocation without a fully-populated "
                    f"key is an authoring error (got {value!r})"
                )

    @property
    def digest(self) -> str:
        return _sha256_text(_SEP.join((
            self.step_id, self.operator_version, self.model_id,
            self.prompt_hash, self.input_hash,
        )))


@dataclass(frozen=True)
class RecordedOutcome:
    """One retained outcome, read back from ``okg.model_outcomes``."""

    key_digest: str
    step_id: str
    operator_version: str
    model_id: str
    prompt_hash: str
    input_hash: str
    content: str
    recorded_at: datetime
    generation_id: str | None


class ReplayMissError(RuntimeError):
    """Raised in replay mode when a key has no recorded outcome.

    The exclusion is recorded on the :class:`OutcomeRecorder` BEFORE this
    raises, so the record exists however the caller handles the error.
    """

    def __init__(self, key: OutcomeKey) -> None:
        self.key = key
        super().__init__(
            f"replay miss: no recorded outcome for step "
            f"{key.step_id!r} (digest {key.digest[:16]}…); replay mode "
            f"never samples fresh — this update is excluded for this step"
        )


@dataclass(frozen=True)
class ReplayExclusion:
    """The per-step record of a replay miss (spec §9.7: excluded, named)."""

    step_id: str
    key_digest: str
    fragment_id: str


@runtime_checkable
class _CompleteClient(Protocol):  # structural: LLMClient shape
    model: str

    def complete(self, system: str, user: str, *, fragment_id: str = "",
                 caller: str = "", generation_id: int | None = None,
                 outcome_key_digest: str | None = None) -> str: ...


class OutcomeStore:
    """Bounded, per-deployment retention of keyed outcomes.

    Operates on a caller-supplied psycopg connection (extraction runs
    inside the publish cycle's connection, so recorded outcomes commit
    atomically with the facts they produced). ``max_rows`` is mandatory
    and positive — there is no unbounded mode. Pruning is oldest-first per
    deployment, enforced at write.
    """

    def __init__(self, *, deployment: str, max_rows: int) -> None:
        if not deployment:
            raise ValueError("OutcomeStore requires a deployment label")
        if not isinstance(max_rows, int) or max_rows <= 0:
            raise ValueError(
                "OutcomeStore requires an explicit positive max_rows "
                "retention bound (manifest-declared); unbounded retention "
                "by omission is not a mode"
            )
        self.deployment = deployment
        self.max_rows = max_rows

    def record(self, conn, key: OutcomeKey, content: str,
               *, generation_id: int | str | None) -> None:
        """First-write-wins insert, then enforce the retention bound.

        Empty content is REFUSED, loudly: under read-through a recorded
        ``""`` would be consumed as an ok outcome on every future run —
        a permanently frozen nothing. An unrecorded key re-samples next
        run instead, which is the recoverable state.
        """
        if not content:
            logger.warning(
                "refusing to record EMPTY model outcome for %s (digest "
                "%s…): an empty recording would be consumed forever under "
                "read-through; leaving the key unrecorded so the next run "
                "re-samples", key.step_id, key.digest[:16],
            )
            return
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO okg.model_outcomes
                  (deployment_name, key_digest, step_id, operator_version,
                   model_id, prompt_hash, input_hash, content,
                   generation_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (deployment_name, key_digest) DO NOTHING
                """,
                (self.deployment, key.digest, key.step_id,
                 key.operator_version, key.model_id, key.prompt_hash,
                 key.input_hash, content,
                 None if generation_id is None else str(generation_id)),
            )
            cur.execute(
                """
                DELETE FROM okg.model_outcomes
                 WHERE deployment_name = %s
                   AND recorded_at < (
                     SELECT min(recorded_at) FROM (
                       SELECT recorded_at FROM okg.model_outcomes
                        WHERE deployment_name = %s
                        ORDER BY recorded_at DESC
                        LIMIT %s
                     ) newest
                   )
                """,
                (self.deployment, self.deployment, self.max_rows),
            )

    def get(self, conn, key: OutcomeKey) -> RecordedOutcome | None:
        """Exact-digest lookup; a miss returns None, never a fuzzy match."""
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT key_digest, step_id, operator_version, model_id,
                       prompt_hash, input_hash, content, recorded_at,
                       generation_id
                  FROM okg.model_outcomes
                 WHERE deployment_name = %s AND key_digest = %s
                """,
                (self.deployment, key.digest),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return RecordedOutcome(
            key_digest=row[0], step_id=row[1], operator_version=row[2],
            model_id=row[3], prompt_hash=row[4], input_hash=row[5],
            content=row[6], recorded_at=row[7], generation_id=row[8],
        )

    def count(self, conn) -> int:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM okg.model_outcomes WHERE deployment_name = %s",
                (self.deployment,),
            )
            return int(cur.fetchone()[0])


@dataclass
class OutcomeRecorder:
    """The call-site facade: read-through record in production, resolve in replay.

    ``mode`` is explicit — ``"record"`` (production: consume a recorded
    outcome when the store holds one for the key; only a miss samples
    fresh and retains under the key — `outcome-read-through`; a failed
    retention write logs loudly but never loses the model's result) or
    ``"replay"`` (consume recorded outcomes only; a miss records a
    :class:`ReplayExclusion` and raises :class:`ReplayMissError` — the
    model client is provably not called). Replay mode without a store is
    a construction-time error.
    """

    store: OutcomeStore | None
    mode: str = "record"
    exclusions: list[ReplayExclusion] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.mode not in ("record", "replay"):
            raise ValueError(f"OutcomeRecorder mode must be 'record' or "
                             f"'replay', got {self.mode!r}")
        if self.mode == "replay" and self.store is None:
            raise ValueError(
                "replay mode requires an OutcomeStore: a deployment with no "
                "model_outcomes retention policy has recorded nothing to "
                "replay (declare model_outcomes_max_rows on the enricher)"
            )

    def keyed_complete(
        self,
        conn,
        llm: _CompleteClient,
        key: OutcomeKey,
        system: str,
        user: str,
        *,
        fragment_id: str = "",
        caller: str = "",
        generation_id: int | None = None,
    ) -> str:
        if self.mode == "replay":
            assert self.store is not None  # __post_init__ enforces
            hit = self.store.get(conn, key)
            if hit is None:
                self.exclusions.append(ReplayExclusion(
                    step_id=key.step_id, key_digest=key.digest,
                    fragment_id=fragment_id,
                ))
                logger.error(
                    "replay miss for %s (fragment %s, digest %s…): "
                    "recorded as exclusion; refusing to sample fresh",
                    key.step_id, fragment_id, key.digest[:16],
                )
                raise ReplayMissError(key)
            return hit.content

        # RECORD mode read-through (`outcome-read-through`, amending the
        # original fresh-sampling stance): a recorded outcome is consumed
        # instead of re-sampling. Recording is first-write-wins, so a fresh
        # sample under an existing key would be paid for, published from,
        # and then DISCARDED by the store — the run diverges from what
        # replay can reproduce and the money buys nothing durable. Measured
        # across a real SIGKILL and a steady-state re-run in PR 925: every
        # completed item re-billed, every repeat output thrown away. A NEW
        # draw is an explicit act — change any key component
        # (operator_version, prompt, model, input) or prune retention.
        if self.store is not None:
            hit = self.store.get(conn, key)
            if hit is not None:
                return hit.content

        content = llm.complete(
            system, user, fragment_id=fragment_id, caller=caller,
            generation_id=generation_id, outcome_key_digest=key.digest,
        )
        # A truncated-but-non-empty body is billed as a success and the
        # caller judges its usability — but it must NOT be retained: under
        # read-through a frozen cut-off body would satisfy this key on
        # every future run, silently, forever (before read-through the
        # per-cycle re-extraction accidentally self-healed exactly this
        # transient). Unrecorded = the next run re-samples.
        stats = getattr(llm, "last_call_stats", None)
        truncated = bool(stats and stats.get("truncated"))
        if truncated:
            logger.warning(
                "not retaining TRUNCATED model outcome for %s (digest "
                "%s…): the run consumes it, the next run re-samples",
                key.step_id, key.digest[:16],
            )
        if self.store is not None and not truncated:
            try:
                self.store.record(conn, key, content,
                                  generation_id=generation_id)
            except Exception:
                # The model's result is never lost to a telemetry-side
                # failure (same stance as record_llm_call) — but the miss
                # is loud, because a silent retention gap would later read
                # as a replay exclusion with no explanation.
                logger.exception(
                    "failed to record model outcome for %s (digest %s…); "
                    "the result is returned but will be MISSING from "
                    "replay for this key", key.step_id, key.digest[:16],
                )
        return content


__all__ = [
    "OutcomeKey",
    "RecordedOutcome",
    "OutcomeRecorder",
    "OutcomeStore",
    "ReplayExclusion",
    "ReplayMissError",
    "canonical_input_hash",
    "prompt_content_hash",
]
