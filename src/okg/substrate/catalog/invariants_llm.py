"""LLM-judge invariant runner.

Per `add-semantic-llm-judge-invariants` (2026-04-29). Phase B-only,
runs OUTSIDE the publisher's gate via `okg run --include-llm-invariants`.
Never blocks publish — verdicts are recorded as warns on
`graph_generations.invariant_summary` after publish completes.

Pipeline (per invariant):

1. Read `candidates_sql` against the published generation; validate
   the result has columns `(candidate_id TEXT, payload JSONB)`.
2. Sample up to `sample_cap` rows (deterministic per-generation:
   `ORDER BY hashtext(candidate_id || ':' || generation_id::text)`).
3. Load `judge_prompt` from disk (path was validated at YAML load).
4. For each candidate, call `LiteLLMClient(judge_model).complete(...)`
   threaded with `caller='invariants:<invariant_name>'` so the
   substrate-wide `okg.llm_calls` table records token usage and
   OpenRouter cost (per `add-substrate-llm-call-tracking`).
5. Parse the verdict JSON `{verdict, rationale, evidence_spans}`;
   reject malformed (`judge_parse_error` warn).
6. Validate every span's `(node_id, byte_start, byte_end)` against
   the cited node's actual `attrs->>'body'` byte range
   (`evidence_span_invalid` warn).
7. Verdicts in the YAML's `fire_on` set count as fires; persist them
   into `graph_generations.invariant_summary` under a new
   `llm_judge` block alongside the existing `cross_source` /
   `source_local` buckets.
8. Compute `llm_call_stats` for the cycle as a SQL roll-up over
   `okg.llm_calls` filtered to `caller LIKE 'invariants:%'` and
   `generation_id`. No parallel ledger.

Loud failures the runner records as warn rows (never crashes the
runner; the publish has already succeeded):

- `judge_parse_error` — verdict JSON malformed.
- `judge_call_failure` — transport/timeout error from litellm.
- `evidence_span_invalid` — span outside the cited node's body.
- `candidates_sql_shape_invalid` — SQL didn't return
  `(candidate_id TEXT, payload JSONB)` (skips the invariant).
"""
from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

import psycopg
from psycopg.types.json import Jsonb

from okg.substrate.errors import SubstrateError
from okg.substrate.publisher.gates import (
    DEFAULT_TIMEOUT_MS,
    _load_invariants_for_cv,
)
from okg.substrate.catalog.invariant_graph_read import (
    graph_branch_checkout,
    prepare_invariant_sql,
    to_checkout_sql,
)
from okg.substrate.catalog.invariants_scope import (
    ScopeUnresolved,
    compute_scope_injection,
)
from okg.substrate.db.connection import connect as db_connect


__all__ = [
    "JudgeInvariantResult",
    "LLMJudgeInvariantRunner",
    "VerdictParseError",
    "evaluate_judge_invariants",
    "run_llm_judge_invariants",
]


log = logging.getLogger(__name__)

# Verdict literals — kept in sync with catalog/invariants.py's
# `_VERDICT_LITERALS`. Duplicated here so the runner can validate
# without importing private state from the loader.
_VERDICT_LITERALS = ("consistent", "inconsistent", "uncertain")


class VerdictParseError(ValueError):
    """The judge returned malformed JSON or an out-of-schema verdict."""


@dataclass(frozen=True)
class JudgeInvariantResult:
    """Per-invariant outcome of one Phase-B LLM-judge run.

    Mirrors `GateInvariantResult`'s fields so the upstream
    `invariant_summary` formatter can bucket judge results
    alongside SQL ones, plus a few judge-specific extras.
    """

    name: str
    severity: str         # 'error' | 'warn'
    scope: str            # 'cross' | 'source'
    source: Optional[str]
    sample_cap: int
    total_candidates: int
    sampled: int
    fires: list[dict[str, Any]] = field(default_factory=list)
    parse_errors: list[dict[str, Any]] = field(default_factory=list)
    call_failures: list[dict[str, Any]] = field(default_factory=list)
    span_invalid: list[dict[str, Any]] = field(default_factory=list)
    # Per-candidate verdict counts so an operator can see at a
    # glance how the judge ruled across the sample (e.g. "5
    # consistent, 0 inconsistent, 0 uncertain"). Keys are the
    # three verdict literals plus 'parse_error' / 'call_failure'.
    verdict_counts: dict[str, int] = field(default_factory=dict)
    duration_ms: int = 0
    # `add-concept-scoped-invariants`: skipped_reason values
    # gain `scope_unresolved` (concept_name doesn't resolve to a
    # Concept at the gen). Per the proposal, this is a loud
    # failure — never silently treated as "all rows".
    skipped_reason: Optional[str] = None  # e.g. 'candidates_sql_shape_invalid'
    # When the invariant is scoped, the resolved Concept node id
    # is stamped here so the upstream `invariant_summary`
    # formatter can group / chip-render scoped fires by concept.
    concept_scope: Optional[dict[str, Any]] = None


# ---------------------------------------------------------------------------
# Verdict-shape validation
# ---------------------------------------------------------------------------

_FENCE_RE = re.compile(
    r"^\s*```(?:json)?\s*\n(.*?)\n```\s*$",
    re.DOTALL,
)


def _strip_code_fences(text: str) -> str:
    """Some judge models wrap JSON in ```json ... ```. Strip if so.

    Best-effort; if the response isn't fenced, returned unchanged.
    """
    m = _FENCE_RE.match(text)
    return m.group(1) if m else text


def parse_verdict(raw: str) -> dict[str, Any]:
    """Parse the judge's response into a validated verdict dict.

    Expected shape (per design.md decision):

        {"verdict": "consistent" | "inconsistent" | "uncertain",
         "rationale": str,
         "evidence_spans": [
            {"node_id": str, "byte_start": int, "byte_end": int},
            ...
         ]}

    `evidence_spans` MAY be empty (judge had no specific evidence
    to cite). Every other field is required.

    Raises `VerdictParseError` on any deviation.
    """
    cleaned = _strip_code_fences(raw.strip())
    try:
        obj = json.loads(cleaned)
    # Catch the stable superclass: long-lived CI interpreters can retain a
    # decoder instance whose JSONDecodeError class predates a module reload.
    except ValueError as exc:
        raise VerdictParseError(f"verdict not JSON: {exc}") from exc
    if not isinstance(obj, dict):
        raise VerdictParseError(
            f"verdict must be a JSON object; got {type(obj).__name__}"
        )

    verdict = obj.get("verdict")
    if verdict not in _VERDICT_LITERALS:
        raise VerdictParseError(
            f"verdict.verdict must be one of {_VERDICT_LITERALS}; "
            f"got {verdict!r}"
        )

    rationale = obj.get("rationale")
    if not isinstance(rationale, str):
        raise VerdictParseError(
            f"verdict.rationale must be a string; got "
            f"{type(rationale).__name__}"
        )

    spans = obj.get("evidence_spans", [])
    if not isinstance(spans, list):
        raise VerdictParseError(
            f"verdict.evidence_spans must be a list; got "
            f"{type(spans).__name__}"
        )
    for i, sp in enumerate(spans):
        if not isinstance(sp, dict):
            raise VerdictParseError(
                f"evidence_spans[{i}] must be an object; got "
                f"{type(sp).__name__}"
            )
        for key in ("node_id", "byte_start", "byte_end"):
            if key not in sp:
                raise VerdictParseError(
                    f"evidence_spans[{i}] missing key {key!r}"
                )
        if not isinstance(sp["node_id"], str) or not sp["node_id"]:
            raise VerdictParseError(
                f"evidence_spans[{i}].node_id must be non-empty string"
            )
        for k in ("byte_start", "byte_end"):
            if not isinstance(sp[k], int) or isinstance(sp[k], bool):
                raise VerdictParseError(
                    f"evidence_spans[{i}].{k} must be int; got "
                    f"{type(sp[k]).__name__}"
                )
        if sp["byte_start"] < 0 or sp["byte_end"] < sp["byte_start"]:
            raise VerdictParseError(
                f"evidence_spans[{i}] has invalid range "
                f"[{sp['byte_start']}, {sp['byte_end']})"
            )

    return {
        "verdict": verdict,
        "rationale": rationale,
        "evidence_spans": [
            {
                "node_id": sp["node_id"],
                "byte_start": sp["byte_start"],
                "byte_end": sp["byte_end"],
            }
            for sp in spans
        ],
    }


# ---------------------------------------------------------------------------
# Body-byte validation
# ---------------------------------------------------------------------------


def _validate_spans_against_bodies(
    dsn: str,
    spans: list[dict[str, Any]],
    *,
    generation_id: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Return ``(valid, invalid)`` partitions for an evidence-span list.

    For each span ``{node_id, byte_start, byte_end}``, look up the
    cited node's ``attrs->>'body'`` (or ``body_text`` fallback used
    by af_pubs publications) AT THE GIVEN GENERATION and reject the
    span if its byte range exceeds the body's UTF-8 byte length.

    A span whose ``node_id`` doesn't exist at the generation also
    counts as invalid (with reason ``node_not_found``). Empty input
    returns two empty lists without touching Postgres.
    """
    if not spans:
        return [], []

    # Group by node_id so we read each body at most once.
    by_node: dict[str, list[dict[str, Any]]] = {}
    for sp in spans:
        by_node.setdefault(sp["node_id"], []).append(sp)

    bodies: dict[str, Optional[bytes]] = {}
    from okg.substrate.graph_store import ChronosGraphStore

    with db_connect(dsn, autocommit=True) as conn:
        graph = ChronosGraphStore.from_connection(
            conn, ensure_schema=False
        ).checkout_generation(generation_id)
        for row in graph.query(
            """
            SELECT node_id,
                   COALESCE(attrs->>'body', attrs->>'body_text') AS body
              FROM okg.graph_nodes
             WHERE node_id = ANY(:node_ids)
            """,
            {"node_ids": list(by_node.keys())},
        ):
            body = row["body"]
            bodies[row["node_id"]] = body.encode("utf-8") if body else None

    valid: list[dict[str, Any]] = []
    invalid: list[dict[str, Any]] = []
    for node_id, sps in by_node.items():
        if node_id not in bodies:
            for sp in sps:
                invalid.append({
                    **sp,
                    "reason": "node_not_found",
                })
            continue
        body_bytes = bodies[node_id]
        if not body_bytes:
            for sp in sps:
                invalid.append({
                    **sp,
                    "reason": "node_body_empty",
                })
            continue
        body_len = len(body_bytes)
        for sp in sps:
            if sp["byte_end"] > body_len:
                invalid.append({
                    **sp,
                    "reason": f"out_of_range:body_len={body_len}",
                })
            else:
                valid.append({
                    **sp,
                    # Echo the byte slice for downstream display.
                    "preview": body_bytes[
                        sp["byte_start"]:sp["byte_end"]
                    ].decode("utf-8", errors="replace")[:240],
                })
    return valid, invalid


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


class LLMJudgeInvariantRunner:
    """Evaluate one ``kind='llm_judge'`` invariant against a
    published generation.

    Constructed once per invariant (for prompt caching / client
    reuse). The caller string threaded through every
    ``LiteLLMClient.complete(...)`` call is
    ``invariants:<invariant_name>`` so per-call rows in
    ``okg.llm_calls`` can be rolled up by the
    ``invariant_summary.llm_call_stats`` view.
    """

    def __init__(
        self,
        dsn: str,
        deployment_name: str,
        invariant: dict[str, Any],
        *,
        client_factory: Optional[Any] = None,
        branch_name: str = "default",
    ) -> None:
        self.dsn = dsn
        self.deployment_name = deployment_name
        self.inv = invariant
        self.branch_name = branch_name
        # `client_factory(model, tracking_dsn, deployment) -> LLMClient`
        # is injectable for tests; in production we lazy-import
        # `LiteLLMClient` to keep this module import-light.
        self._client_factory = client_factory

    def _make_client(self) -> Any:
        if self._client_factory is not None:
            return self._client_factory(
                self.inv["judge_model"],
                self.dsn,
                self.deployment_name,
            )
        from okg.views.execution.llm import LiteLLMClient
        return LiteLLMClient(
            model=self.inv["judge_model"],
            tracking_dsn=self.dsn,
            deployment=self.deployment_name,
        )

    def _read_prompt(self) -> str:
        prompt_path = Path(self.inv["judge_prompt"])
        try:
            return prompt_path.read_text()
        except OSError as exc:
            raise SubstrateError(
                f"invariant {self.inv['name']!r}: judge_prompt "
                f"{prompt_path} unreadable: {exc}",
                data={"name": self.inv["name"], "path": str(prompt_path)},
            ) from exc

    def _fetch_candidates(
        self,
        generation_id: int,
    ) -> tuple[list[tuple[str, dict[str, Any]]], int, Optional[str]]:
        """Return (sampled rows, total candidates, skip_reason).

        Validates the SQL returns exactly the expected
        (candidate_id TEXT, payload JSONB) columns. On shape
        mismatch returns ``([], 0, 'candidates_sql_shape_invalid')``.

        Per ``add-concept-scoped-invariants``: when the invariant
        carries ``concept_scope``, a ``scope`` CTE is prepended
        to the wrapping query so the user's candidates_sql can
        ``JOIN scope`` to filter to the concept's neighborhood.
        ``scope_unresolved`` skip_reason is returned when the
        concept name doesn't resolve to a Concept at the gen.
        """
        body = self.inv["candidates_sql"].rstrip().rstrip(";")
        timeout_ms = self.inv["timeout_ms"] or DEFAULT_TIMEOUT_MS

        # Scope CTE injection. Pre-resolves the concept and
        # produces the `scope AS (...)` definition; if the concept
        # doesn't resolve, returns scope_unresolved (loud failure
        # by proposal — never treat empty scope as "all rows").
        concept_scope = self.inv.get("concept_scope")
        scope_cte_def = ""
        scope_params: dict[str, Any] = {}
        if concept_scope is not None:
            try:
                with db_connect(self.dsn, autocommit=True) as cx:
                    inj = compute_scope_injection(
                        cx, concept_scope,
                        generation_id=generation_id,
                    )
            except ScopeUnresolved as exc:
                log.warning(
                    "invariant %s: %s; recording scope_unresolved",
                    self.inv["name"], exc,
                )
                return [], 0, "scope_unresolved"
            scope_cte_def = inj.cte_sql + ",\n"
            scope_params = inj.params

        wrapped = f"""
            WITH {scope_cte_def}_cands AS (
              {body}
            ),
            _total AS (SELECT count(*) AS c FROM _cands),
            _sample AS (
              SELECT candidate_id, payload
                FROM _cands
               ORDER BY hashtext(candidate_id || %(seed)s)
               LIMIT %(cap)s
            )
            SELECT (SELECT c FROM _total)            AS total,
                   COALESCE(
                     (SELECT array_agg(candidate_id) FROM _sample),
                     ARRAY[]::text[]
                   )                                 AS sampled_ids,
                   COALESCE(
                     (SELECT array_agg(payload) FROM _sample),
                     ARRAY[]::jsonb[]
                   )                                 AS sampled_payloads
        """
        try:
            with db_connect(self.dsn, autocommit=False) as conn, \
                 conn.cursor() as cur:
                cur.execute(
                    f"SET LOCAL statement_timeout = {int(timeout_ms)}"
                )
                cur.execute("SET LOCAL TRANSACTION READ ONLY")
                bound = {
                    "pin": generation_id,
                    "excluded_sources": [],
                    "seed": f":{generation_id}",
                    "cap": int(self.inv["sample_cap"]),
                    **scope_params,
                }
                # Same defect as the publish gate (issue #1456): a row source
                # reading the Chronos graph either raises the honest-read guard
                # or scans an empty stub on this bare connection. Route it
                # through the pinned checkout; a non-graph row source keeps its
                # original plain-cursor path.
                ready, pinned = prepare_invariant_sql(wrapped, conn)
                if pinned:
                    rows = graph_branch_checkout(
                        conn, branch_name=self.branch_name,
                    ).query(to_checkout_sql(ready), bound)
                    row = (
                        (
                            rows[0]["total"],
                            rows[0]["sampled_ids"],
                            rows[0]["sampled_payloads"],
                        )
                        if rows else None
                    )
                else:
                    cur.execute(ready, bound)
                    row = cur.fetchone()
        except psycopg.errors.UndefinedColumn as exc:
            log.warning(
                "invariant %s: candidates_sql shape invalid (%s)",
                self.inv["name"], exc,
            )
            return [], 0, "candidates_sql_shape_invalid"
        except psycopg.errors.QueryCanceled:
            log.warning(
                "invariant %s: candidates_sql timed out (%d ms)",
                self.inv["name"], timeout_ms,
            )
            return [], 0, "candidates_sql_timeout"

        if row is None:
            return [], 0, None
        total = int(row[0] or 0)
        ids = list(row[1] or [])
        payloads = list(row[2] or [])
        if len(ids) != len(payloads):
            return [], total, "candidates_sql_shape_invalid"
        sampled: list[tuple[str, dict[str, Any]]] = []
        for cid, payload in zip(ids, payloads):
            if not isinstance(cid, str) or not cid:
                return [], total, "candidates_sql_shape_invalid"
            if not isinstance(payload, dict):
                # psycopg returns JSONB as Python dict; anything else
                # signals a column-type mismatch.
                return [], total, "candidates_sql_shape_invalid"
            sampled.append((cid, payload))
        return sampled, total, None

    def run(self, generation_id: int) -> JudgeInvariantResult:
        """Execute this invariant against a published generation."""
        t0 = time.monotonic()
        # Surface the spec's concept_scope (if any) on the result
        # so the upstream `invariant_summary` formatter can render
        # a concept chip per fired warn (per the proposal).
        scope_snapshot = self.inv.get("concept_scope")
        sampled, total, skip = self._fetch_candidates(generation_id)
        if skip is not None:
            return JudgeInvariantResult(
                name=self.inv["name"],
                severity=self.inv["severity"],
                scope=self.inv["scope"],
                source=self.inv["source"],
                sample_cap=int(self.inv["sample_cap"]),
                total_candidates=total,
                sampled=0,
                duration_ms=int((time.monotonic() - t0) * 1000),
                skipped_reason=skip,
                concept_scope=scope_snapshot,
            )
        if not sampled:
            return JudgeInvariantResult(
                name=self.inv["name"],
                severity=self.inv["severity"],
                scope=self.inv["scope"],
                source=self.inv["source"],
                sample_cap=int(self.inv["sample_cap"]),
                total_candidates=total,
                sampled=0,
                duration_ms=int((time.monotonic() - t0) * 1000),
                concept_scope=scope_snapshot,
            )

        prompt_template = self._read_prompt()
        client = self._make_client()
        caller = f"invariants:{self.inv['name']}"
        fire_on = set(self.inv["fire_on"] or ("inconsistent",))

        fires: list[dict[str, Any]] = []
        parse_errors: list[dict[str, Any]] = []
        call_failures: list[dict[str, Any]] = []
        span_invalid: list[dict[str, Any]] = []
        verdict_counts: dict[str, int] = {
            v: 0 for v in _VERDICT_LITERALS
        }
        verdict_counts["parse_error"] = 0
        verdict_counts["call_failure"] = 0

        # Stable outcome keys (spec §9.2): judge calls are keyed like every
        # model-backed invocation — step, operator version, model, prompt
        # content, typed input — and the digest rides okg.llm_calls.
        # Outcome retention/replay wiring for the judge path is owned by
        # a later child; keys land now so history is joinable.
        from okg.substrate.llm.outcomes import (
            OutcomeKey,
            canonical_input_hash,
            prompt_content_hash,
        )
        judge_prompt_hash = prompt_content_hash(prompt_template)

        # Judge calls are independent per candidate and are pure provider
        # round-trips, so they go through the shared fan-out. The bookkeeping
        # below (span validation against bodies, which reads the database)
        # stays on this thread, in candidate order.
        from okg.substrate.db.concurrency_budget import width
        from okg.substrate.llm.fanout import ModelCallJob, complete_many

        def _judge_key(job: ModelCallJob):
            return OutcomeKey(
                step_id=caller,
                operator_version="v1",
                model_id=client.model,
                prompt_hash=judge_prompt_hash,
                input_hash=canonical_input_hash(job.typed_input),
            )

        jobs = [
            ModelCallJob(
                system=prompt_template,
                user=json.dumps(payload, ensure_ascii=False, sort_keys=True),
                typed_input=payload,
                item_ref=str(candidate_id),
                fragment_id=f"{self.inv['name']}:{candidate_id}",
            )
            for candidate_id, payload in sampled
        ]

        def _call(job: ModelCallJob) -> str:
            return client.complete(
                system=job.system,
                user=job.user,
                fragment_id=job.fragment_id,
                caller=caller,
                generation_id=generation_id,
                outcome_key_digest=_judge_key(job).digest,
            )

        # Spend admission for the judge lane. Its scope is the invariant, so a
        # runaway judge on one invariant cannot spend another's budget. An
        # undeclared cap leaves this lane unbounded and `enabled` reports it —
        # no invented per-call estimate, which would be wrong by an unknown
        # factor and invisible.
        from okg.substrate.llm.fanout import AdmissionDecision
        from okg.substrate.llm.spend import ModelCallSpend

        spend = ModelCallSpend(
            deployment=self.deployment_name,
            scope=f"invariant-judge:{self.inv['name']}",
            cap_usd=self.inv.get("judge_spend_cap_usd"),
            estimated_usd_per_call=self.inv.get("judge_spend_estimate_usd"),
        )
        reservations: dict[str, Any] = {}

        def _admit(job):
            decision = spend.reserve(None)
            reservations[job.item_ref] = decision
            if not decision.admitted:
                return AdmissionDecision(False, reason=decision.reason)
            return AdmissionDecision(True, reserved_usd=decision.reserved_usd)

        # Settlement charges the shared cost ledger, which needs a
        # connection. The judge holds none open across the fan-out (each
        # phase opens its own short session), so open one for the
        # settlement pass — but only when a cap is actually declared;
        # a disabled lane must not pay a connection for a no-op.
        # Without `settle`, every reservation the admit hook takes is
        # permanently leaked and the ledger never learns what was spent.
        settle_conn = (
            db_connect(self.dsn, autocommit=True)
            if spend.enabled else None
        )

        def _settle(job, _decision, outcome):
            admission = reservations.get(job.item_ref)
            if outcome.budget_refused:
                # Never issued: release the reservation, record nothing.
                spend.release(admission)
            elif outcome.provider_error is not None:
                # Issued but raised: record the attempt at zero, not the
                # estimate — okg.llm_calls carries any billed cost.
                spend.settle(settle_conn, admission, 0.0)
            else:
                # The judge client reports no per-call cost; the estimate
                # (an upper bound by contract) is the charge.
                spend.settle(settle_conn, admission, None)

        try:
            judged = complete_many(
                jobs,
                call=_call,
                derive_key=_judge_key,
                record=lambda index, key, content: content,
                admit=_admit,
                settle=_settle if spend.enabled else None,
                width=width("enrichment.model_calls"),
            )
        finally:
            if settle_conn is not None:
                settle_conn.close()

        for (candidate_id, payload), outcome in zip(sampled, judged):
            if not outcome.ok:
                exc = outcome.provider_error
                log.warning(
                    "invariant %s: judge call failed for %s: %s",
                    self.inv["name"], candidate_id, exc,
                )
                call_failures.append({
                    "candidate_id": candidate_id,
                    "error": str(exc),
                    "error_kind": type(exc).__name__ if exc else "refused",
                })
                verdict_counts["call_failure"] += 1
                continue
            raw = outcome.content

            try:
                verdict = parse_verdict(raw)
            except VerdictParseError as exc:
                parse_errors.append({
                    "candidate_id": candidate_id,
                    "raw": raw[:1024],
                    "error": str(exc),
                })
                verdict_counts["parse_error"] += 1
                continue
            verdict_counts[verdict["verdict"]] += 1

            valid_spans, invalid_spans = _validate_spans_against_bodies(
                self.dsn, verdict["evidence_spans"],
                generation_id=generation_id,
            )
            if invalid_spans:
                span_invalid.append({
                    "candidate_id": candidate_id,
                    "invalid_spans": invalid_spans,
                })

            if verdict["verdict"] in fire_on:
                fires.append({
                    "candidate_id": candidate_id,
                    "verdict": verdict["verdict"],
                    "rationale": verdict["rationale"],
                    "evidence_spans": valid_spans,
                    # Keep the full payload so describe-side debugging
                    # can show what was judged.
                    "payload": payload,
                })

        return JudgeInvariantResult(
            name=self.inv["name"],
            severity=self.inv["severity"],
            scope=self.inv["scope"],
            source=self.inv["source"],
            sample_cap=int(self.inv["sample_cap"]),
            total_candidates=total,
            sampled=len(sampled),
            fires=fires,
            parse_errors=parse_errors,
            call_failures=call_failures,
            span_invalid=span_invalid,
            verdict_counts=verdict_counts,
            duration_ms=int((time.monotonic() - t0) * 1000),
            concept_scope=scope_snapshot,
        )


# ---------------------------------------------------------------------------
# Generation-level driver: run every llm_judge invariant + persist results
# ---------------------------------------------------------------------------


def evaluate_judge_invariants(
    dsn: str,
    deployment_name: str,
    *,
    generation_id: int,
    catalog_version_id: int,
    only: Optional[Iterable[str]] = None,
    branch_name: str = "default",
) -> list[JudgeInvariantResult]:
    """Run every ``kind='llm_judge'`` invariant installed at
    ``catalog_version_id`` against the published ``generation_id``.

    ``only`` optionally restricts to a subset of invariant names.
    """
    only_set = set(only) if only is not None else None
    with db_connect(dsn, autocommit=True) as conn:
        invs = _load_invariants_for_cv(
            conn, catalog_version_id, kinds=("llm_judge",),
        )
    results: list[JudgeInvariantResult] = []
    for inv in invs:
        if only_set is not None and inv["name"] not in only_set:
            continue
        runner = LLMJudgeInvariantRunner(
            dsn, deployment_name, inv, branch_name=branch_name,
        )
        try:
            results.append(runner.run(generation_id))
        except Exception:  # noqa: BLE001 — LLM judge invariants are advisory; record call_failure result rather than block the publish.
            log.exception(
                "invariant %s: runner crashed (will not block "
                "publish; recording as call_failure)",
                inv["name"],
            )
            results.append(JudgeInvariantResult(
                name=inv["name"],
                severity=inv["severity"],
                scope=inv["scope"],
                source=inv["source"],
                sample_cap=int(inv["sample_cap"]),
                total_candidates=0,
                sampled=0,
                skipped_reason="runner_crashed",
            ))
    return results


def _load_llm_call_stats(
    dsn: str, deployment_name: str, *,
    generation_id: int | str,
) -> dict[str, Any]:
    """Roll up ``okg.llm_calls`` for the cycle into the
    ``invariant_summary.llm_call_stats`` block.

    Filtered to ``caller LIKE 'invariants:%'`` AND the published
    generation id. Single source of truth (no parallel ledger);
    if a call failed before the row was written, it's absent.

    ``okg.llm_calls.generation_id`` is the TEXT generation ref (D2), and the
    judge path records the same value it reads here, so the predicate is
    stringified on both sides rather than cast in SQL.

    Issue #2573 slice 2 (closes #1739): the roll-up goes through the shared
    ``llm_usage_summary`` read, so an unpriced call is counted in
    ``unpriced_calls`` and makes ``total_cost_usd`` unknown instead of adding
    ``$0``; ``priced_cost_usd`` keeps the priced part. The block is returned
    in wire form (``to_wire``): an unknown measure is ``null`` with its reason
    under the block's own ``unknown_reasons``, so the JSONB column and the
    CLI see the same thing. A failed query reports every measure unknown
    (``llm_call_stats_query_failed``) rather than zeros.
    """
    from okg.substrate.telemetry.llm_usage import (
        LlmUsageFilters,
        llm_usage_summary,
    )
    from okg.substrate.unknown import Unknown, to_wire

    filters = LlmUsageFilters(
        deployment=deployment_name,
        generation_id=str(generation_id),
        caller_prefix="invariants:",
    )
    try:
        with db_connect(dsn, autocommit=True) as conn, \
             conn.cursor() as cur:
            groups = llm_usage_summary(cur, filters=filters, group_by=("caller",))
            (total,) = llm_usage_summary(cur, filters=filters)
    except psycopg.Error as exc:
        log.warning("llm_call_stats roll-up query failed: %s", exc)
        failed = "llm_call_stats_query_failed"
        return to_wire({
            "calls": Unknown(failed),
            "prompt_tokens": Unknown(failed),
            "completion_tokens": Unknown(failed),
            "total_cost_usd": Unknown(failed),
            "priced_cost_usd": Unknown(failed),
            "unpriced_calls": Unknown(failed),
            "by_invariant": {},
            "error": str(exc),
        })

    out = total.as_payload(cost_key="total_cost_usd")
    by_invariant: dict[str, Any] = {}
    for usage in groups:
        caller = usage.group["caller"]
        # caller = 'invariants:<name>'
        inv_name = caller.split(":", 1)[1] if ":" in caller else caller
        by_invariant[inv_name] = usage.as_payload(cost_key="total_cost_usd")
    out["by_invariant"] = by_invariant
    return to_wire(out)


def _persist_results(
    dsn: str, *,
    generation_id: int,
    results: list[JudgeInvariantResult],
    llm_call_stats: dict[str, Any],
) -> None:
    """UPDATE ``graph_generations.invariant_summary`` to add (or
    replace) the ``llm_judge`` block + ``llm_call_stats`` block.

    Preserves the existing ``cross_source`` / ``source_local``
    blocks the SQL gate wrote. Idempotent re-run replaces the
    judge block in place (operator can re-run with new
    ``sample_cap`` and the latest result wins).
    """
    judge_block = {
        "results": [
            {
                "name": r.name,
                "severity": r.severity,
                "scope": r.scope,
                "source": r.source,
                "sample_cap": r.sample_cap,
                "total_candidates": r.total_candidates,
                "sampled": r.sampled,
                "fires": r.fires,
                "parse_errors": r.parse_errors,
                "call_failures": r.call_failures,
                "span_invalid": r.span_invalid,
                "verdict_counts": r.verdict_counts,
                "duration_ms": r.duration_ms,
                "skipped_reason": r.skipped_reason,
            }
            for r in results
        ],
        "fired_warns": [
            {
                "name": r.name,
                "kind": "llm_judge",
                "candidate_id": fire["candidate_id"],
                "rationale": fire["rationale"],
                "evidence_spans": fire["evidence_spans"],
            }
            for r in results
            for fire in r.fires
        ],
    }
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE okg.graph_generations
               SET invariant_summary = COALESCE(invariant_summary, '{}'::jsonb)
                                      || jsonb_build_object(
                                           'llm_judge', %s::jsonb,
                                           'llm_call_stats', %s::jsonb)
             WHERE generation_id = %s
            """,
            (Jsonb(judge_block), Jsonb(llm_call_stats), generation_id),
        )


def run_llm_judge_invariants(
    dsn: str,
    deployment_name: str,
    *,
    generation_id: Optional[int] = None,
    only: Optional[Iterable[str]] = None,
    branch_name: str = "default",
) -> dict[str, Any]:
    """Top-level entry point invoked by ``okg run --include-llm-invariants``.

    Reads the latest published generation (or the explicit one),
    runs every ``kind='llm_judge'`` invariant installed at its
    catalog version, persists results into
    ``graph_generations.invariant_summary``, and returns a JSON-
    serialisable summary dict.

    Returns the same shape the CLI surfaces:

        {
          "deployment": str,
          "generation_id": int,
          "catalog_version_id": int,
          "results": [JudgeInvariantResult-as-dict, ...],
          "llm_call_stats": {...},
        }
    """
    with db_connect(dsn, autocommit=True) as conn, conn.cursor() as cur:
        if generation_id is None:
            cur.execute(
                """
                SELECT generation_id, catalog_version_id
                  FROM okg.graph_generations
                 WHERE deployment_name = %s
                   AND status = 'published'
                 ORDER BY generation_id DESC
                 LIMIT 1
                """,
                (deployment_name,),
            )
        else:
            cur.execute(
                """
                SELECT generation_id, catalog_version_id
                  FROM okg.graph_generations
                 WHERE deployment_name = %s
                   AND generation_id = %s
                """,
                (deployment_name, generation_id),
            )
        row = cur.fetchone()
        if row is None:
            raise SubstrateError(
                f"no published generation found for "
                f"deployment={deployment_name!r}",
                data={"deployment": deployment_name,
                      "generation_id": generation_id},
            )
        gen_id, cv_id = int(row[0]), int(row[1])

    results = evaluate_judge_invariants(
        dsn, deployment_name,
        generation_id=gen_id, catalog_version_id=cv_id, only=only,
        branch_name=branch_name,
    )
    stats = _load_llm_call_stats(
        dsn, deployment_name, generation_id=gen_id,
    )
    _persist_results(
        dsn, generation_id=gen_id,
        results=results, llm_call_stats=stats,
    )

    return {
        "deployment": deployment_name,
        "generation_id": gen_id,
        "catalog_version_id": cv_id,
        "results": [
            {
                "name": r.name,
                "severity": r.severity,
                "scope": r.scope,
                "source": r.source,
                "sample_cap": r.sample_cap,
                "total_candidates": r.total_candidates,
                "sampled": r.sampled,
                "fires_count": len(r.fires),
                "parse_errors_count": len(r.parse_errors),
                "call_failures_count": len(r.call_failures),
                "span_invalid_count": len(r.span_invalid),
                "verdict_counts": r.verdict_counts,
                "duration_ms": r.duration_ms,
                "skipped_reason": r.skipped_reason,
            }
            for r in results
        ],
        "llm_call_stats": stats,
    }
