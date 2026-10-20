"""Substrate-level confidence-floor convention for LLM-driven enrichers.

Per `extend-substrate-extraction-with-claim-evidence-extractors`
(umbrella D8 + D5 of this child's design.md):

Every LLM-driven semantic-extraction enricher in the substrate library
honors a single contract:

1. The enricher accepts a **required** `confidence_floor` config
   attribute (float in [0, 1]) declared in the deployment's
   `enrichers:` block. Constructor raises ``ValueError`` when missing
   or out of range — no silent defaults (project P0 rule, mirroring
   ``SemanticConceptLinker.min_similarity``).

2. The enricher's body produces a stream of
   :class:`ExtractionCandidate` objects (one per LLM candidate it
   considers). The base class's ``enrich(...)`` routes each candidate
   through the floor:

   - ``confidence >= floor`` → :meth:`emit_typed_node` (subclass
     hook); the typed node carries
     ``provenance: extracted_model``, ``confidence: <value>``,
     and ``extractor_floor_at_emit: <floor>``.
   - ``confidence < floor`` → an ``ExtractionDropped`` NodeFact is
     emitted with the candidate text + confidence + reason; the
     typed node is NOT created.

3. No silent gaps. Every candidate produces either the typed node
   or the audit record.

4. ``dropped_id`` is deterministic from
   ``(extractor, source_node_ref, sha256(candidate_text))`` so
   re-runs over the same input dedupe at insert
   (``ON CONFLICT DO NOTHING``).

Subclasses implement two hooks:

* :meth:`extract_candidates(conn, generation_id) -> Iterable[ExtractionCandidate]`
  — the LLM-call body. Reads the Chronos graph ``okg.graph_nodes``
  (through a checkout pinned to ``generation_id``), invokes
  ``self.llm.complete(...)``, parses the response, and yields
  ``ExtractionCandidate`` objects.

* :meth:`emit_typed_node(conn, candidate, *, generation_id, run_id, observed_at) -> int`
  — given an above-floor candidate, INSERTs the typed
  ``okg.node_facts`` + ``okg.edge_facts`` rows the enricher's
  spec promises. Returns the number of edges emitted.
  ``generation_id``, ``run_id`` and ``observed_at`` are keyword-only;
  the base class passes all three by keyword from :meth:`enrich`. An
  override that takes ``generation_id`` positionally and omits the other
  two therefore cannot be called at all — and the resulting
  :class:`TypeError` does not surface, because :meth:`enrich` catches it
  with every other per-candidate failure. It counts an error, logs a
  warning, and holds the watermark. The observable symptom is an enricher
  that emits ZERO typed nodes every cycle and retries forever, which is
  the "enricher emitted zero derived edges" silent failure, not a crash.

The base class handles the floor predicate, the
``ExtractionDropped`` emission, dedupe-key hashing, and the
``EnrichResult`` accounting uniformly.
"""

from __future__ import annotations

import hashlib
import json
import logging
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, Optional, Sequence

import psycopg

from okg.substrate.enrichers.base import EnrichResult
from okg.substrate.library.enrichers import DerivedSupportPolicy
from okg.substrate.enrichers.input_quarantine import (
    InputClaim,
    InputQuarantineKey,
    claim_inputs,
    record_deterministic_failure,
    record_success,
    release_claim,
)
from okg.substrate.sources.redaction import redact_text
from okg.views.execution.errors import LLMCallFailed
from okg.views.execution.llm import LLMClient

logger = logging.getLogger(__name__)


__all__ = [
    "ExtractionCandidate",
    "IncompleteExtractionResponse",
    "LLMExtractor",
    "TruncatedExtractionResponse",
    "make_dropped_id",
    "make_dedupe_key",
    "make_snapshot_fact_key",
]


# Substrate floor for the floor itself: must be in [0, 1]. Honored
# at constructor-time per the no-silent-fallback rule.
_FLOOR_MIN = 0.0
_FLOOR_MAX = 1.0
_FAILURE_REASON_MAX_LENGTH = 160
_FAILURE_DETAIL_MAX_LENGTH = 240
_FAILURE_HEADER_FIELD_RE = re.compile(
    r"(?ix)"
    r"(?P<prefix>(?:[\"']?(?:headers|request[-_]?headers|"
    r"response[-_]?headers)[\"']?\s*[:=]\s*))"
    r"[^\r\n;]+"
)
_FAILURE_HEADER_VALUE_RE = re.compile(
    r"(?ix)"
    r"(?P<prefix>(?:[\"']?(?:cookie|set[-_]?cookie|"
    r"proxy[-_]?authorization)[\"']?\s*[:=]\s*))"
    r"(?:\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])*'|[^\r\n;]+)"
)
_FAILURE_AUTHORIZATION_RE = re.compile(
    r"(?ix)"
    r"(?P<prefix>(?:[\"']?authorization(?:[-_]?header)?[\"']?\s*[:=]\s*))"
    r"(?:"
    r"(?P<double>\"(?:\\.|[^\"\\])*\")|"
    r"(?P<single>'(?:\\.|[^'\\])*')|"
    r"(?P<unquoted>[^\r\n;]+)"
    r")"
)
_FAILURE_SECRET_FIELD_RE = re.compile(
    r"(?ix)"
    r"(?P<prefix>"
    r"(?:[\"']?(?:secret[-_]?key|secret|password|passphrase|token|credential|"
    r"private[-_]?key|api[-_]?key|apikey|access[-_]?key|access[-_]?token|"
    r"refresh[-_]?token|client[-_]?secret|authorization(?:[-_]?header)?|"
    r"x[-_]?api[-_]?key|connection[-_]?string|bearer)[\"']?\s*[:=]\s*))"
    r"(?:\"(?P<double>(?:\\.|[^\"\\])*)\"|"
    r"'(?P<single>(?:\\.|[^'\\])*)'|"
    r"(?P<unquoted>(?!<redacted>)[^\s;}\]]+))"
)


def _safe_exception_type(exc: BaseException) -> str:
    """Return an exception class name without invoking exception formatting."""
    return type(exc).__name__ or "Exception"


def _safe_exception_message(exc: BaseException) -> str:
    """Read and normalize an exception message without trusting ``__str__``."""
    try:
        message = str(exc)
    except Exception:  # noqa: BLE001 - provider exceptions may format badly
        return ""
    if not isinstance(message, str):
        return ""
    return " ".join(message.split())


def _durable_failure_detail(exc: BaseException) -> str:
    """Return a bounded, redacted root-cause detail for durable summaries.

    Provider wrappers use exception chaining to retain the useful underlying
    error.  Walk only ``__cause__`` so the outer failure kind remains the
    stable contract, and stop on malformed/cyclic chains.  The message is
    redacted before clamping so a long credential cannot be persisted in a
    partially visible prefix.
    """
    root = exc
    seen: set[int] = set()
    while id(root) not in seen:
        seen.add(id(root))
        try:
            cause = root.__cause__
        except Exception:  # noqa: BLE001 - defensive provider exception API
            break
        if not isinstance(cause, BaseException) or id(cause) in seen:
            break
        root = cause

    kind = _safe_exception_type(exc)
    reason = _safe_exception_message(root) or _safe_exception_type(root)
    # Consume an entire authorization value before the shared redactor sees
    # it.  The shared helper recognizes Bearer headers, but an arbitrary
    # provider scheme (Token, Basic, OAuth, ...) must not leave its credential
    # suffix behind in a durable failure summary.
    def _replace_sensitive_field(match: re.Match[str]) -> str:
        prefix = match.group("prefix")
        if match.groupdict().get("double") is not None:
            return f'{prefix}"<redacted>"'
        if match.groupdict().get("single") is not None:
            return f"{prefix}'<redacted>'"
        return f"{prefix}<redacted>"

    reason = _FAILURE_HEADER_FIELD_RE.sub(_replace_sensitive_field, reason)
    reason = _FAILURE_HEADER_VALUE_RE.sub(_replace_sensitive_field, reason)
    reason = _FAILURE_AUTHORIZATION_RE.sub(_replace_sensitive_field, reason)
    reason = redact_text(reason)
    reason = _FAILURE_SECRET_FIELD_RE.sub(_replace_sensitive_field, reason)
    reason = " ".join(reason.split())
    reason = reason[:_FAILURE_REASON_MAX_LENGTH]
    return f"{kind}: {reason}"[:_FAILURE_DETAIL_MAX_LENGTH]


def make_dropped_id(
    *, extractor: str, source_node_ref: str, candidate_text: str
) -> str:
    """Deterministic ``ExtractionDropped`` node id.

    Re-runs of the same extractor over the same input with the same
    candidate text produce identical ids — the substrate's
    ``ON CONFLICT (dedupe_key, observed_at) DO NOTHING`` dedupe
    collapses them at insert.

    Per spec.md §"ExtractionDropped Subtype" Scenario:
    "ExtractionDropped dropped_id is deterministic — both runs SHALL
    emit NodeFacts with identical dropped_id values".
    """
    payload = "|".join(
        (
            str(extractor),
            str(source_node_ref),
            str(candidate_text),
        )
    ).encode("utf-8")
    return "dropped:" + hashlib.sha256(payload).hexdigest()[:16]


def make_dedupe_key(*parts: str) -> str:
    """Hash a stable key for ``okg.edge_facts`` / ``okg.node_facts``
    dedupe. Caller assembles the parts; this just hashes."""
    payload = "|".join(str(p) for p in parts).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


class IncompleteExtractionResponse(ValueError):
    """The model returned no authoritative snapshot for one input.

    A valid JSON ``[]`` is an authoritative empty result.  Empty text,
    malformed JSON, a wrong top-level shape, or an invalid candidate is not;
    extraction enrichers raise this exception in strict snapshot mode so the
    previous complete graph snapshot stays live.
    """


class TruncatedExtractionResponse(IncompleteExtractionResponse):
    """The provider returned a non-authoritative output-limited response.

    This is systemic rather than deterministic input poison: reasoning and
    provider output limits can truncate otherwise valid input transiently, so
    the item must keep holding the watermark and must never consume its
    quarantine-attempt budget.
    """


class ItemSnapshotEmissionRejected(IncompleteExtractionResponse):
    """One named input produced a deterministic rejected snapshot fact."""

    def __init__(self, item_ref: str, detail: str) -> None:
        self.item_ref = str(item_ref)
        super().__init__(detail)


def make_snapshot_fact_key(
    *,
    fact_kind: str,
    producer_id: str,
    op: str,
    graph_identity: Sequence[str],
) -> str:
    """Return an injective, stable key for one snapshot fact operation.

    ``make_dedupe_key`` predates snapshot replacement and joins caller parts
    with ``|``.  That is stable but not injective when an identity component
    itself contains the delimiter.  Replacement keys use canonical structured
    JSON instead: fact kind, producer, operation, and every graph-identity
    component remain separately encoded.
    """
    payload = json.dumps(
        {
            "schema": "okg.enricher_snapshot_fact.v1",
            "fact_kind": str(fact_kind),
            "producer_id": str(producer_id),
            "op": str(op),
            "graph_identity": [str(part) for part in graph_identity],
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


@dataclass
class ExtractionCandidate:
    """One LLM-emitted candidate awaiting floor adjudication.

    The subclass's :meth:`LLMExtractor.extract_candidates` returns
    a stream of these; the base class routes by ``confidence``.

    Fields:

    - ``candidate_text`` — the body the LLM produced (e.g. the claim
      statement, the concept name + description, the hypothesis
      sentence). Lands as ``Claim.text`` /
      ``ExtractionDropped.candidate_text`` depending on adjudication.
    - ``confidence`` — the LLM's reported confidence in this
      candidate, in [0, 1]. Compared against ``confidence_floor``.
    - ``source_node_ref`` — the substrate node id the extractor was
      looking at (typically a Paragraph / PaperSection / Theorem /
      Claim-pair from sibling modules). Lands as
      ``ExtractionDropped.source_node_ref`` and is used to derive the
      deterministic ``dropped_id``.
    - ``extra`` — subclass-specific payload passed through to
      :meth:`emit_typed_node`. The base class never reads this; it
      exists so the LLM-call body doesn't have to re-derive things
      it already computed (e.g. paper id for ``extracted_from_paper_ref``).
    """

    candidate_text: str
    confidence: float
    source_node_ref: str
    extra: dict[str, Any] = field(default_factory=dict)


class LLMExtractor:
    """Base class for the four A2 LLM-driven semantic extractors.

    Subclasses set the class-level ``name``, ``reads_subtypes``,
    ``emits_edge_types``, ``requires_narrowings`` per the
    substrate's enricher Protocol; implement
    :meth:`extract_candidates` (the LLM-call body) and
    :meth:`emit_typed_node` (above-floor materialization). The base
    class handles floor adjudication + ``ExtractionDropped`` emission
    + ``EnrichResult`` accounting.

    Constructor args:

    - ``confidence_floor`` — required float in [0, 1]; the threshold
      candidates must meet to land as typed nodes. Below threshold
      → ``ExtractionDropped``. No silent default (P0).
    - ``llm`` — an :class:`~okg.views.execution.llm.LLMClient`
      implementation. The substrate's ``LiteLLMClient`` for
      production; a mock for tests.

    Class declarations every subclass owns:

    - ``support_policy`` — the derived-node liveness policy for every node
      this extractor emits: ``"any_member"`` (the node survives while one
      declared input is live) or ``"all_members"`` (every declared input
      must stay live). Declared on the class, NOT defaulted: the base class
      annotates it without a value, so a subclass that omits it is refused
      by :func:`~okg.substrate.publisher.enricher_loader.load_enrichers`
      at worker boot, naming the enricher — not per candidate inside a
      publisher cycle. Declare it as::

          from okg.substrate.library.enrichers import DerivedSupportPolicy

          class MyExtractor(LLMExtractor):
              support_policy: DerivedSupportPolicy = "all_members"

    Recommended default floors (per spec § per-extractor):

    - ``claim_extractor``: 0.6 (high; claims are dense)
    - ``concept_extractor``: 0.5 (recall-sensitive)
    - ``evidence_extractor``: 0.6 (high; load-bearing link)
    - ``hypothesis_extractor``: 0.5 (long-tail phrasing)

    Deployments tuning the floor MUST set it explicitly in the
    enricher's config block — the recommended default appears only
    in this class's spec, never as a Python fallback.
    """

    # Substrate enricher Protocol contract (subclasses override).
    name: str = "llm_extractor_base"
    reads_subtypes: tuple[str, ...] = ()
    emits_edge_types: tuple[str, ...] = ()
    requires_narrowings: tuple[tuple[str, str, str], ...] = ()

    # Derived-node liveness policy. ANNOTATED, NEVER DEFAULTED: a guessed
    # policy silently decides whether derived nodes survive their inputs
    # being retracted, and a wrong guess there is unrecoverable. Subclasses
    # that write nodes declare it; registration refuses the omission.
    support_policy: DerivedSupportPolicy

    # The typed-subtype emitted for above-floor candidates (e.g.
    # "claim"). Subclasses set this; the base class uses it only for
    # logging.
    emits_typed_subtype: str = ""

    # Node subtypes this base class writes for EVERY subclass, beyond the
    # subclass's headline `emits_typed_subtype`. `_emit_dropped` writes an
    # `extraction_dropped` node for each below-floor candidate, so it belongs
    # in every extractor's declared write envelope. It was outside all of
    # them, which was invisible while node emission was ungoverned and would
    # reject the WHOLE run — typed nodes included — once admission checks
    # node subtypes (PACT admit-derived-node-emission), because a fact range
    # is grouped per (source, run_id) and one stray subtype fails it.
    emits_node_subtypes: tuple[str, ...] = ("extraction_dropped",)

    # Declared operator version — a component of every invocation's
    # OutcomeKey (spec §9.2). Bump when the extractor's semantics change
    # (prompt restructuring rides prompt_hash; parsing/vocabulary changes
    # ride this). Subclasses may override.
    operator_version: str = "v1"

    # Declared classification (spec §9.3): every LLM extractor's outputs
    # depend on a model call. The write envelope derives from the declared
    # emission surface (emits_typed_subtype + requires_narrowings) plus
    # any closed response vocabulary a subclass declares here — never from
    # observed output. A subclass overriding nondeterministic to False
    # while carrying a model client fails the boot lint.
    nondeterministic: bool = True
    envelope_value_vocabularies: dict[str, tuple[str, ...]] = {}

    # Snapshot replacement is opt-in because it asserts that one invocation
    # recomputes the producer's COMPLETE graph footprint.  The four shipped
    # extraction enrichers make that declaration explicitly.  A future
    # incremental subclass must stay append-only until it defines a narrower,
    # equally complete replacement boundary.
    snapshot_replacement_mode: str = "append"
    _SNAPSHOT_REPLACEMENT_MODES = ("append", "complete_producer")

    def __init__(
        self,
        *,
        confidence_floor: float,
        llm: Optional[LLMClient] = None,
        llm_model: Optional[str] = None,
        llm_timeout_s: float = 60.0,
        llm_max_retries: int = 3,
        llm_max_output_tokens: Optional[int] = None,
        llm_completion_kwargs: Optional[dict[str, Any]] = None,
        llm_canary: bool = False,
        tracking_dsn: Optional[str] = None,
        tracking_mode: Optional[str] = None,
        runtime_dsn: Optional[str] = None,
        deployment: Optional[str] = None,
        model_outcomes_max_rows: Optional[int] = None,
        model_outcomes_deployment: Optional[str] = None,
        replay_outcomes: bool = False,
        entity_resolution: Optional[dict[str, Any]] = None,
        spend_deployment: Optional[str] = None,
        spend_cap_usd: Optional[float] = None,
        spend_estimate_usd: Optional[float] = None,
        model_calls_per_second: Optional[float] = None,
        input_quarantine_max_attempts: Optional[int] = None,
    ) -> None:
        replacement_mode = str(type(self).snapshot_replacement_mode)
        if replacement_mode not in self._SNAPSHOT_REPLACEMENT_MODES:
            raise ValueError(
                f"{type(self).__name__}: snapshot_replacement_mode must be one "
                f"of {self._SNAPSHOT_REPLACEMENT_MODES!r}, got "
                f"{replacement_mode!r}"
            )
        self.snapshot_replacement_mode = replacement_mode
        if confidence_floor is None:
            raise ValueError(
                f"{type(self).__name__}: confidence_floor is required "
                f"(no silent default; project P0 rule)."
            )
        if not isinstance(confidence_floor, (int, float)):
            raise ValueError(
                f"{type(self).__name__}: confidence_floor must be a "
                f"number in [{_FLOOR_MIN}, {_FLOOR_MAX}], got "
                f"{confidence_floor!r}"
            )
        if not (_FLOOR_MIN <= float(confidence_floor) <= _FLOOR_MAX):
            raise ValueError(
                f"{type(self).__name__}: confidence_floor "
                f"{confidence_floor!r} out of range "
                f"[{_FLOOR_MIN}, {_FLOOR_MAX}]"
            )
        self.confidence_floor = float(confidence_floor)
        model_backed = llm is not None or bool(llm_model) or bool(llm_canary)
        if model_backed and (
            not isinstance(input_quarantine_max_attempts, int)
            or isinstance(input_quarantine_max_attempts, bool)
            or input_quarantine_max_attempts <= 0
        ):
            raise ValueError(
                f"{type(self).__name__}: input_quarantine_max_attempts is "
                "required and must be a positive integer for a model-backed "
                "extractor (no silent retry-limit default)."
            )
        if input_quarantine_max_attempts is not None and (
            not isinstance(input_quarantine_max_attempts, int)
            or isinstance(input_quarantine_max_attempts, bool)
            or input_quarantine_max_attempts <= 0
        ):
            raise ValueError(
                f"{type(self).__name__}: input_quarantine_max_attempts must "
                f"be a positive integer, got {input_quarantine_max_attempts!r}"
            )
        self.input_quarantine_max_attempts = input_quarantine_max_attempts
        # Spend + rate admission, declared by the deployment. Deliberately NOT
        # defaulted: an invented per-call estimate is wrong by an unknown factor
        # and invisible, and an invented cap would silently bound a lane the
        # operator never budgeted. Absent → the lane is unbounded and
        # `ModelCallSpend.enabled` reports it as such.
        if spend_cap_usd is not None and float(spend_cap_usd) <= 0:
            raise ValueError(
                f"{type(self).__name__}: spend_cap_usd must be > 0 when set, "
                f"got {spend_cap_usd!r}"
            )
        if spend_estimate_usd is not None and float(spend_estimate_usd) <= 0:
            raise ValueError(
                f"{type(self).__name__}: spend_estimate_usd must be > 0 when "
                f"set, got {spend_estimate_usd!r}"
            )
        if model_calls_per_second is not None and float(model_calls_per_second) <= 0:
            raise ValueError(
                f"{type(self).__name__}: model_calls_per_second must be > 0 "
                f"when set, got {model_calls_per_second!r}"
            )
        if (spend_cap_usd is None) != (spend_estimate_usd is None):
            raise ValueError(
                f"{type(self).__name__}: spend_cap_usd and spend_estimate_usd "
                f"must be declared together — a cap with no estimate cannot "
                f"reserve, and an estimate with no cap bounds nothing. Got "
                f"cap={spend_cap_usd!r} estimate={spend_estimate_usd!r}."
            )
        self._spend_deployment = spend_deployment
        self._spend_cap_usd = spend_cap_usd
        self._spend_estimate_usd = spend_estimate_usd
        self._model_calls_per_second = model_calls_per_second
        # Deployment-style config: `llm_model: <litellm-string>` builds
        # a `LiteLLMClient` on demand. Tests pass a pre-built `llm=`.
        # If both are set, the explicit `llm=` wins (test mocks).
        if llm is not None and llm_model is not None:
            logger.warning(
                "%s: both `llm` and `llm_model` set; using explicit "
                "`llm=` and ignoring `llm_model=%r`",
                type(self).__name__,
                llm_model,
            )
        # These reach only a client THIS class builds, which happens
        # exactly when `llm` is absent and `llm_model` is set. Any other
        # combination — no model, or an injected `llm=` that wins over
        # the model — leaves them touching nothing, which the operator
        # would read as applied configuration. Refuse rather than
        # no-op: a silently dropped token cap is how #907 started.
        builds_own_client = llm is None and bool(llm_model)
        if not builds_own_client and (
            llm_max_output_tokens is not None or llm_completion_kwargs is not None
        ):
            reason = (
                "an explicit `llm=` client was passed, which wins over `llm_model`"
                if llm is not None
                else "no `llm_model` was given"
            )
            raise ValueError(
                f"{type(self).__name__}: llm_max_output_tokens / "
                f"llm_completion_kwargs configure the client this "
                f"extractor builds from `llm_model`, but {reason}, so "
                f"they would reach nothing. Set `llm_model` without an "
                f"`llm=`, or configure the pre-built client directly."
            )
        if llm is None and llm_model:
            # `llm_max_output_tokens` / `llm_completion_kwargs` are the
            # deployment's handle on the provider call. A reasoning
            # model pointed at this extractor spends its whole output
            # budget on the reasoning trace and returns nothing usable
            # unless the deployment can either raise the cap or set
            # `llm_completion_kwargs: {reasoning: {enabled: false}}`
            # (#907). Neither was reachable from config before.
            from okg.views.execution.llm import LiteLLMClient

            llm = LiteLLMClient(
                model=llm_model,
                timeout_s=llm_timeout_s,
                max_retries=llm_max_retries,
                max_output_tokens=llm_max_output_tokens,
                completion_kwargs=llm_completion_kwargs,
                tracking_dsn=tracking_dsn,
                tracking_mode=tracking_mode,
                runtime_dsn=runtime_dsn,
                deployment=deployment,
            )
        if llm is None and llm_canary:
            # Canary mode: emit deterministic candidates for
            # end-to-end wiring verification without an external
            # provider call. Dispatches to the right canary class
            # based on the subclass's `name` attribute (one canary
            # per extractor; falls back to CanaryConceptLLM).
            # See ``okg.views.execution.llm_canary``.
            from okg.views.execution.llm_canary import canary_for_name

            llm = canary_for_name(type(self).name)
        self.llm = llm
        self._entity_resolution = None
        if entity_resolution is not None:
            from okg.substrate.entity_resolution import EntityResolutionStage

            self._entity_resolution = EntityResolutionStage(entity_resolution)
        self._resolved_entity_ids: dict[str, str] = {}
        self._snapshot_prompt_hashes: set[str] = set()
        self._pending_snapshot_facts: (
            list[tuple[str, dict[str, Any], str | None]] | None
        ) = None
        self._prior_snapshot_cache: (
            tuple[list[dict[str, Any]], list[dict[str, Any]]] | None
        ) = None
        self._current_snapshot_input_ref: str | None = None
        self._snapshot_rejected_fact_count = 0
        self._snapshot_retained_node_ids: set[str] = set()
        # Per-run accumulator of items whose LLM call failed. Reset at
        # the start of every :meth:`enrich`; populated by
        # :meth:`_complete_tracked`. A non-empty list at run end marks
        # the `EnrichResult` incomplete so the publisher holds the
        # watermark (P0 no-silent-data-loss). See `_complete_tracked`.
        self._input_failures: list[tuple[str, str]] = []
        self._run_deployment_name: str | None = None
        self._run_branch_name: str | None = None
        self._publisher_deployment_name: str | None = None
        self._publisher_branch_name: str | None = None
        self._run_generation_id: int | str | None = None
        self._run_conn: psycopg.Connection | None = None
        self._publisher_transaction_owned = False
        self._publisher_transaction: Any | None = None
        self._input_claims: dict[str, InputClaim] = {}
        self._input_states: dict[str, str] = {}
        self._quarantined_items: set[str] = set()
        # Keyed model outcomes (spec §9.2). Retention is manifest-gated:
        # the deployment's enricher config declares
        # `model_outcomes_max_rows` (+ `model_outcomes_deployment`) to
        # retain outcomes; absent, every call still computes and persists
        # its OutcomeKey digest on okg.llm_calls, but no outcome body is
        # retained and replay is impossible — the recorder refuses replay
        # mode without a store, loudly.
        from okg.substrate.llm.outcomes import OutcomeRecorder, OutcomeStore

        store = None
        if model_outcomes_max_rows is not None:
            if not model_outcomes_deployment:
                raise ValueError(
                    f"{type(self).__name__}: model_outcomes_max_rows set "
                    f"without model_outcomes_deployment; the retention "
                    f"bound is per-deployment and the label is required"
                )
            store = OutcomeStore(
                deployment=model_outcomes_deployment,
                max_rows=model_outcomes_max_rows,
            )
        self.outcomes = OutcomeRecorder(
            store=store,
            mode="replay" if replay_outcomes else "record",
        )

    # ------------------------------------------------------------------
    # Per-item LLM call (substrate-level convention; tracks failures)
    # ------------------------------------------------------------------

    _STABLE_PROVIDER_INPUT_CODES = frozenset(
        {
            "context_length_exceeded",
            "content_policy_rejected",
            "invalid_prompt_input",
        }
    )

    def _quarantine_enabled(self) -> bool:
        return bool(
            self.snapshot_replacement_mode == "complete_producer"
            and self.input_quarantine_max_attempts is not None
            and self._run_deployment_name
            and self._run_branch_name
        )

    def _set_publisher_context(
        self,
        *,
        deployment_name: str,
        branch_name: str,
    ) -> None:
        """Receive optional publisher context without changing Enricher Protocol."""
        self._publisher_deployment_name = str(deployment_name)
        self._publisher_branch_name = str(branch_name)

    def _begin_publisher_transaction(self, conn: psycopg.Connection) -> None:
        """Begin the post-read publisher transaction on an autocommit pool conn."""
        if self._publisher_transaction_owned:
            return
        if conn.autocommit:
            # Called only after prior snapshot, source, adjacency and outcome-
            # store reads. From here through publisher output admission and
            # progress, every statement belongs to one transaction. Keeping
            # the psycopg Transaction context open also rejects any accidental
            # inner conn.commit() instead of silently splitting that boundary.
            transaction = conn.transaction()
            transaction.__enter__()
            self._publisher_transaction = transaction
            self._publisher_transaction_owned = True

    def _finish_publisher_transaction(
        self,
        conn: psycopg.Connection,
        *,
        commit: bool,
    ) -> None:
        """Publisher hook: close the transaction after progress or on failure."""
        if not self._publisher_transaction_owned:
            return
        transaction = self._publisher_transaction
        if transaction is None:
            raise RuntimeError("publisher transaction ownership lost its context")
        try:
            if commit:
                transaction.__exit__(None, None, None)
            else:
                rollback = RuntimeError("publisher transaction rollback requested")
                transaction.__exit__(type(rollback), rollback, None)
        finally:
            self._publisher_transaction = None
            self._publisher_transaction_owned = False

    @classmethod
    def _stable_provider_input_code(cls, exc: BaseException) -> str | None:
        """Return an explicitly typed deterministic provider rejection.

        HTTP status and message parsing are deliberately excluded.  A generic
        4xx, timeout, auth failure, route failure, 429, or 5xx is systemic and
        must keep holding the watermark without consuming an item budget.  The
        stable code is trusted only behind the LLM client boundary; arbitrary
        application and substrate exceptions may also expose ``code`` fields.
        """
        if not isinstance(exc, LLMCallFailed):
            return None
        seen: set[int] = set()
        current: BaseException | None = exc
        while current is not None and id(current) not in seen:
            seen.add(id(current))
            for attr in ("error_code", "code"):
                value = getattr(current, attr, None)
                if isinstance(value, str) and value in cls._STABLE_PROVIDER_INPUT_CODES:
                    return value
            current = current.__cause__ or current.__context__
        return None

    @classmethod
    def _deterministic_failure_class(cls, exc: BaseException) -> str | None:
        if isinstance(exc, ItemSnapshotEmissionRejected):
            return "item_snapshot_emission_rejected"
        if isinstance(exc, TruncatedExtractionResponse):
            return None
        if isinstance(exc, IncompleteExtractionResponse):
            return "incomplete_extraction_response"
        provider_code = cls._stable_provider_input_code(exc)
        if provider_code is not None:
            return f"provider_input_rejected:{provider_code}"
        return None

    def _record_input_failure(self, item_ref: str, exc: BaseException) -> None:
        """Account one item failure as bounded-deterministic or systemic.

        Only a claimed, explicitly classified item failure consumes the
        durable budget.  Everything else releases its claim and remains a
        held failure.  This keeps outages and ambiguous provider errors from
        silently turning into quarantined data loss.
        """
        item_ref = str(item_ref)
        if self._input_states.get(item_ref) in {"failed", "quarantined", "systemic"}:
            return
        claim = self._input_claims.get(item_ref)
        failure_class = self._deterministic_failure_class(exc)
        if claim is not None and failure_class is not None:
            if self._run_conn is None:
                raise RuntimeError("input quarantine claim has no run connection")
            terminal = record_deterministic_failure(
                self._run_conn,
                claim,
                failure_class=failure_class,
                error=_durable_failure_detail(exc),
                generation_id=self._run_generation_id,
            )
            if terminal is not None:
                self._input_states[item_ref] = "quarantined"
                self._quarantined_items.add(item_ref)
                logger.error(
                    "%s: quarantined deterministic input %s after %d/%d "
                    "committed failures (%s); this run may advance past the "
                    "named item and doctor will remain unhealthy",
                    self.name,
                    item_ref,
                    terminal.attempt_count,
                    terminal.attempt_limit,
                    failure_class,
                )
                return
            self._input_states[item_ref] = "failed"
        else:
            if claim is not None:
                if self._run_conn is None:
                    raise RuntimeError("input quarantine claim has no run connection")
                release_claim(self._run_conn, claim)
            self._input_states[item_ref] = "systemic"

        kind = _safe_exception_type(exc)
        failure_text = _safe_exception_message(exc)
        durable_detail = (
            f"{kind}: truncated response"
            if "truncat" in f"{kind} {failure_text}".lower()
            else _durable_failure_detail(exc)
        )
        self._input_failures.append((item_ref, durable_detail))
        logger.warning(
            "%s: input failed for %s: %s; watermark remains held and the "
            "item will retry (failure_class=%s)",
            self.name,
            item_ref,
            exc,
            failure_class or "systemic",
        )

    def _mark_input_parsed(self, item_ref: str) -> None:
        """Mark a claimed response as authoritative, including JSON ``[]``."""
        item_ref = str(item_ref)
        if item_ref in self._input_claims and self._input_states.get(item_ref) is None:
            self._input_states[item_ref] = "parsed"

    def _complete_tracked(
        self,
        system_prompt: str,
        user_prompt: str,
        *,
        item_ref: str,
        **complete_kwargs: Any,
    ) -> Optional[str]:
        """Invoke ``self.llm.complete`` for one input item, recording
        any failure so the run holds its watermark instead of silently
        dropping the item.

        Returns the raw completion on success. On ANY exception (a
        transient provider-routing / rate-limit / timeout error, a
        malformed-request error, ...) it records the failure against the
        current :meth:`enrich` run, logs it loudly, and returns
        ``None``. The caller SHALL treat ``None`` as "skip this item
        THIS cycle" (``continue``); because the run is then marked
        ``incomplete``, the publisher does NOT advance the enricher's
        watermark past this item, and the next cycle re-reads and
        retries it.

        The recording contract is shared with :meth:`_keyed_complete`
        (which is the extractors' chokepoint — it adds the OutcomeKey
        dispatch). The prior per-subclass ``try/except ... continue``
        swallowed the error and let the watermark advance past the
        failed item — permanently, silently losing its typed nodes (a
        P0 data-loss bug). Every per-item LLM call routes through one
        of these two chokepoints so the no-silent-loss contract holds
        uniformly.
        """
        if self.llm is None:
            raise RuntimeError(f"{self.name}: _complete_tracked requires an LLM client")
        try:
            return self.llm.complete(system_prompt, user_prompt, **complete_kwargs)
        except Exception as exc:  # noqa: BLE001 — record & hold; never swallow
            self._record_input_failure(item_ref, exc)
            return None

    # ------------------------------------------------------------------
    # Keyed dispatch — the single choke point extractors call
    # ------------------------------------------------------------------

    def _model_call_spend(self):
        """Spend admission for this enricher's lane.

        Built from the deployment's declaration. An absent cap or estimate
        leaves the lane UNBOUNDED and says so through `spend.enabled` rather
        than inventing a per-call cost — a made-up estimate is wrong by an
        unknown factor and invisible, which is the silent-fallback shape.
        """
        from okg.substrate.llm.spend import ModelCallSpend

        return ModelCallSpend(
            deployment=getattr(self, "_spend_deployment", "") or "",
            scope=f"enricher:{type(self).name}",
            cap_usd=getattr(self, "_spend_cap_usd", None),
            estimated_usd_per_call=getattr(self, "_spend_estimate_usd", None),
        )

    def _model_call_rate_limiter(self):
        """Provider pacing for this lane, or None when undeclared.

        Rate is a separate term from width: N concurrent calls against a
        tokens-per-minute endpoint produce 429s, which route into
        input-failure accounting, hold the watermark and retry — burning MORE
        than the serial path they replaced.
        """
        from okg.substrate.llm.fanout import RateLimiter

        per_second = getattr(self, "_model_calls_per_second", None)
        if not per_second:
            return None
        return RateLimiter(float(per_second))

    def _outcome_key(self, system: str, typed_input):
        """The OutcomeKey for one invocation — the SINGLE source for both the
        serial and the fanned-out path.

        Extracted so the two paths cannot drift. Key drift is the worst of the
        parallel-path failure modes because it is silent: a run recorded under
        one derivation and replayed under another misses on every lookup and
        surfaces as unexplained ReplayExclusions rather than an error.
        """
        from okg.substrate.llm.outcomes import (
            OutcomeKey,
            canonical_input_hash,
            prompt_content_hash,
        )

        if self.llm is None:
            raise ValueError(f"{type(self).__name__}: no LLM client configured")
        prompt_hash = prompt_content_hash(system)
        self._snapshot_prompt_hashes.add(prompt_hash)
        return OutcomeKey(
            step_id=f"enricher:{type(self).name}",
            operator_version=type(self).operator_version,
            model_id=self.llm.model,
            prompt_hash=prompt_hash,
            input_hash=canonical_input_hash(typed_input),
        )

    def _complete_many(
        self,
        conn,
        jobs: list,
        *,
        generation_id: int,
    ) -> list:
        """Run many model calls two-phase; return outcomes aligned with `jobs`.

        Provider round-trips overlap in a bounded pool touching no connection;
        outcome recording and input-failure accounting stay on this thread.
        Replay stays serial — it reads the database and has no network to
        overlap.

        Subclasses keep their per-item loops and hand over a list. Every
        contract the serial chokepoint enforces is preserved: the same
        `_outcome_key`, the digest riding the same recorder call, and a failed
        or refused item recorded as an unprocessed input so the publisher holds
        the enricher watermark and retries it next cycle.
        """
        from okg.substrate.db.concurrency_budget import width
        from okg.substrate.llm.fanout import ModelCallOutcome, complete_many

        if self.llm is None:
            raise ValueError(f"{type(self).__name__}: no LLM client configured")

        caller = f"enricher:{type(self).name}"
        if not jobs:
            return []

        # Derive signatures and perform every outcome-store read before the
        # durable item reservation.  From claim_inputs() through provider
        # fanout, snapshot flush and progress update, no commit-capable graph
        # checkout is allowed: quarantine + outputs + watermark are one
        # publisher transaction.
        keys = [self._outcome_key(job.system, job.typed_input) for job in jobs]
        item_signatures: dict[str, str] = {}
        for job, key in zip(jobs, keys):
            prior = item_signatures.setdefault(str(job.item_ref), key.digest)
            if prior != key.digest:
                raise RuntimeError(
                    f"{self.name}: one item_ref produced multiple input "
                    f"signatures in one batch: {job.item_ref!r}"
                )

        representative_by_digest: dict[tuple[str, str], int] = {}
        representative_for_index: list[int] = []
        for index, (job, key) in enumerate(zip(jobs, keys)):
            identity = (str(job.item_ref), key.digest)
            representative = representative_by_digest.setdefault(identity, index)
            representative_for_index.append(representative)
        representative_indices = sorted(set(representative_for_index))

        prerecorded: dict[int, str] = {}
        if self.outcomes.store is not None:
            for index in representative_indices:
                hit = self.outcomes.store.get(conn, keys[index])
                if hit is not None:
                    prerecorded[index] = hit.content

        active: set[int] = set()
        if self._quarantine_enabled():
            quarantine_keys = [
                InputQuarantineKey.from_outcome_key(
                    deployment_name=str(self._run_deployment_name),
                    branch_name=str(self._run_branch_name),
                    enricher_name=self.name,
                    item_ref=str(jobs[index].item_ref),
                    outcome_key=keys[index],
                )
                for index in representative_indices
            ]
            self._begin_publisher_transaction(conn)
            claims = claim_inputs(
                conn,
                quarantine_keys,
                attempt_limit=int(self.input_quarantine_max_attempts),
                generation_id=self._run_generation_id,
            )
            for index, quarantine_key in zip(representative_indices, quarantine_keys):
                claim = claims[quarantine_key.identity]
                item_ref = str(jobs[index].item_ref)
                self._input_claims[item_ref] = claim
                if not claim.dispatch:
                    active.add(index)
                    self._input_states[item_ref] = "quarantined"
                    self._quarantined_items.add(item_ref)

        # REPLAY: never sample fresh. Active quarantines are removed before
        # keyed dispatch. Outcome-store pre-reads already happened above the
        # reservation boundary; no provider call occurs for an active row. A
        # missing replay record is systemic and consumes no deterministic
        # item budget.
        if self.outcomes.mode == "replay":
            from okg.substrate.llm.outcomes import ReplayExclusion

            representative_outcomes: dict[int, ModelCallOutcome] = {}
            for index in representative_indices:
                job = jobs[index]
                if index in active:
                    representative_outcomes[index] = ModelCallOutcome(quarantined=True)
                    continue
                content = prerecorded.get(index)
                if content is None:
                    key = keys[index]
                    self.outcomes.exclusions.append(
                        ReplayExclusion(
                            step_id=key.step_id,
                            key_digest=key.digest,
                            fragment_id=job.fragment_id,
                        )
                    )
                    logger.error(
                        "replay miss for %s (fragment %s, digest %s…): "
                        "recorded as exclusion from the pre-reservation "
                        "store read; refusing a second read or fresh sample",
                        key.step_id,
                        job.fragment_id,
                        key.digest[:16],
                    )
                if content is None:
                    representative_outcomes[index] = ModelCallOutcome(
                        replay_excluded=True
                    )
                    if self.snapshot_replacement_mode == "complete_producer":
                        self._record_input_failure(
                            job.item_ref,
                            RuntimeError(
                                "replay outcome is missing; replay mode cannot "
                                "authorize removal from a complete snapshot"
                            ),
                        )
                else:
                    representative_outcomes[index] = ModelCallOutcome(content=content)
            return [
                representative_outcomes[representative_for_index[index]]
                for index in range(len(jobs))
            ]

        # RECORD: phase 1 issues the provider call and carries the key digest
        # onto okg.llm_calls, exactly as the chokepoint does. Phase 2 persists
        # the outcome body. It must NOT go back through `keyed_complete`, which
        # issues its own `llm.complete` — routing phase 2 through it would make
        # every model call TWICE, doubling spend and latency and making the
        # fan-out slower than the serial path it replaces.
        actual_costs: dict[str, Any] = {}
        truncated_digests: set[str] = set()

        def _call(job):
            key = self._outcome_key(job.system, job.typed_input)
            content = self.llm.complete(
                job.system,
                job.user,
                fragment_id=job.fragment_id,
                caller=caller,
                generation_id=generation_id,
                outcome_key_digest=key.digest,
            )
            # Capture this call's ACTUAL cost on the worker thread that made
            # it (`last_call_stats` is thread-local on LiteLLMClient, so this
            # cannot read another in-flight call's numbers). Settlement then
            # charges the ledger what the call cost, not the estimate — with
            # estimate-only settlement, per-call estimate error accumulates
            # invisibly and `settled_usd` can never diverge from reservations.
            stats = getattr(self.llm, "last_call_stats", None)
            if stats is not None:
                actual_costs[job.item_ref] = stats.get("cost_usd")
                if stats.get("truncated"):
                    truncated_digests.add(key.digest)
            return content

        def _record(index: int, key, content: str):
            if self.outcomes.store is None:
                # No retention policy: the digest already reached
                # okg.llm_calls in phase 1; there is no body to retain.
                return content
            if key.digest in truncated_digests:
                # Same stance as keyed_complete: a truncated body is
                # consumed by THIS run but never retained — a frozen
                # cut-off body would satisfy this key on every future
                # run under read-through. Unrecorded = re-sample next
                # cycle, which is how transients healed before
                # read-through existed.
                logger.warning(
                    "not retaining TRUNCATED model outcome for %s "
                    "(digest %s…): the run consumes it, the next run "
                    "re-samples",
                    key.step_id,
                    key.digest[:16],
                )
                return content
            try:
                self.outcomes.store.record(
                    conn,
                    key,
                    content,
                    generation_id=generation_id,
                )
            except Exception:  # noqa: BLE001 — bookkeeping is systemic
                logger.exception(
                    "failed to record model outcome for %s (digest %s…); the "
                    "result is rejected and the watermark remains held",
                    key.step_id,
                    key.digest[:16],
                )
                return None
            return content

        # Spend + rate admission. Width bounds occupancy and nothing else, so a
        # fan-out without these multiplies burn rate with no economic bound.
        # Reservation happens on THIS thread before dispatch (phase 1 must
        # touch no connection); settlement happens in phase 2 alongside outcome
        # recording, where `cost_budget.consume` can reach the connection it
        # needs.
        spend = self._model_call_spend()
        rate = self._model_call_rate_limiter()
        admissions: dict[str, Any] = {}

        def _admit(job):
            from okg.substrate.llm.fanout import AdmissionDecision

            decision = spend.reserve(conn)
            admissions[job.item_ref] = decision
            if not decision.admitted:
                return AdmissionDecision(False, reason=decision.reason)
            return AdmissionDecision(True, reserved_usd=decision.reserved_usd)

        def _settle(job, _decision, outcome):
            admission = admissions.get(job.item_ref)
            if outcome.budget_refused:
                # The call was never issued (a rate refusal can follow a
                # successful reserve): release the reservation instead of
                # writing a ledger row for money that was not spent.
                spend.release(admission)
                return
            if job.item_ref in actual_costs:
                # Provider answered; charge the reported actual (captured
                # in `_call` on the worker thread). Covers the bookkeeping-
                # failure path too — the content existed, the call billed.
                spend.settle(conn, admission, actual_costs[job.item_ref])
            elif outcome.provider_error is not None:
                # Issued but raised: no usage is knowable here, so record
                # the attempt at zero rather than the estimate — charging
                # the estimate over-reported failures, and retries next
                # cycle would charge again. `okg.llm_calls.cost_usd`
                # carries the billed cost for usage-bearing failures
                # (truncated/empty responses).
                spend.settle(conn, admission, 0.0)
            else:
                # Succeeded with no usage info: the reservation (an upper
                # bound by contract) is the best available number.
                spend.settle(conn, admission, None)

        # Active rows and read-through hits make zero provider calls. The
        # remaining representative jobs are each dispatched once even if a
        # subclass accidentally supplied duplicate (item, signature) entries.
        dispatch_indices = [
            index
            for index in representative_indices
            if index not in active and index not in prerecorded
        ]
        miss_jobs = [jobs[index] for index in dispatch_indices]

        miss_outcomes = complete_many(
            miss_jobs,
            call=_call,
            derive_key=lambda job: self._outcome_key(job.system, job.typed_input),
            record=_record,
            admit=_admit,
            settle=_settle,
            rate=rate,
            width=width("enrichment.model_calls"),
        )
        representative_outcomes: dict[int, ModelCallOutcome] = {
            index: ModelCallOutcome(quarantined=True) for index in active
        }
        representative_outcomes.update(
            {
                index: ModelCallOutcome(content=content)
                for index, content in prerecorded.items()
                if index not in active
            }
        )
        representative_outcomes.update(zip(dispatch_indices, miss_outcomes))
        outcomes = [
            representative_outcomes[representative_for_index[index]]
            for index in range(len(jobs))
        ]
        # Provider failures AND budget refusals both mark the run incomplete so
        # the publisher holds the watermark and the item retries next cycle. A
        # refusal that did not reach this accounting would let the watermark
        # advance past an item that was never processed — silent data loss.
        for index in representative_indices:
            job = jobs[index]
            outcome = representative_outcomes[index]
            if outcome.quarantined:
                continue
            key_digest = keys[index].digest
            if key_digest in truncated_digests:
                self._record_input_failure(
                    job.item_ref,
                    TruncatedExtractionResponse(
                        "model response was truncated and is not an "
                        "authoritative replacement snapshot"
                    ),
                )
            elif outcome.provider_error is not None:
                self._record_input_failure(job.item_ref, outcome.provider_error)
            elif outcome.budget_refused:
                self._record_input_failure(
                    job.item_ref,
                    RuntimeError("model-call admission refused this item"),
                )
        return outcomes

    def _keyed_complete(
        self,
        conn,
        system: str,
        user: str,
        *,
        typed_input,
        item_ref: str,
        fragment_id: str,
        generation_id: int,
    ) -> Optional[str]:
        """Compute the invocation's OutcomeKey and dispatch through the
        recorder: production READS THROUGH the retention store when one
        exists (a recorded outcome is consumed; only a miss samples fresh
        and records — `outcome-read-through`); replay consumes recorded
        outcomes only. Every provider call carries its key digest onto
        okg.llm_calls regardless of retention policy.

        Failure contract (same recording as :meth:`_complete_tracked`):
        a fresh-call failure is recorded against the run and returns
        ``None`` — the caller skips the item THIS cycle, the run is
        marked incomplete, and the held watermark retries it next
        cycle. A replay miss is ledgered as a ReplayExclusion (the
        equivalence harness maps exclusions to EXCLUDED verdicts) and
        returns ``None``; replay never samples fresh. The complete-producer
        batch coordinator additionally records that exclusion as an input
        failure, because a missing recorded answer cannot authorize snapshot
        removal. Append-only callers retain the exclusion-only behavior.
        """
        from okg.substrate.llm.outcomes import ReplayMissError

        if self.llm is None:  # callers gate on self.llm already; loud anyway
            raise ValueError(f"{type(self).__name__}: no LLM client configured")
        # Same derivation the fanned-out path uses. Inlining it here again is
        # how the two silently diverge.
        key = self._outcome_key(system, typed_input)
        try:
            return self.outcomes.keyed_complete(
                conn,
                self.llm,
                key,
                system,
                user,
                fragment_id=fragment_id,
                caller=f"enricher:{type(self).name}",
                generation_id=generation_id,
            )
        except ReplayMissError:
            # Ledgered as a ReplayExclusion by the recorder before the
            # raise; this item is excluded from THIS replay run only.
            return None
        except Exception as exc:  # noqa: BLE001 — record & hold; never swallow
            self._record_input_failure(item_ref, exc)
            return None

    # ------------------------------------------------------------------
    # Subclass hooks
    # ------------------------------------------------------------------

    def extract_candidates(
        self,
        conn: psycopg.Connection,
        generation_id: int,
    ) -> Iterable[ExtractionCandidate]:
        """Subclass: run the LLM and yield candidates.

        The base class's :meth:`enrich` iterates this; per-candidate
        floor adjudication is the base class's job.
        """
        raise NotImplementedError

    def emit_typed_node(
        self,
        conn: psycopg.Connection,
        candidate: ExtractionCandidate,
        *,
        generation_id: int,
        run_id: str,
        observed_at: datetime,
    ) -> int:
        """Subclass: INSERT the typed node + edges for an above-floor
        candidate. Return the count of edges emitted (for
        ``EnrichResult.n_edges_emitted`` accounting).
        """
        raise NotImplementedError

    # ------------------------------------------------------------------
    # Floor predicate (substrate-level convention; not subclassable)
    # ------------------------------------------------------------------

    def above_floor(self, confidence: float) -> bool:
        """``True`` iff the candidate's confidence is >= floor.

        Public so unit tests can exercise the predicate in isolation.
        """
        return float(confidence) >= self.confidence_floor

    # ------------------------------------------------------------------
    # INSERT helpers (substrate-level convention)
    # ------------------------------------------------------------------
    # Per substrate-enricher-direct-insert-pattern-broken finding:
    # every enricher that emits NodeFacts / EdgeFacts directly must
    # ship the projector pattern (deterministic edge_id via uuid5;
    # `source = "_enricher:<name>"`; `run_id`). These helpers
    # encapsulate that pattern so future enrichers don't have to
    # re-derive it from the schema and (worse) miss a column.
    #
    # Existing concept/claim/evidence/hypothesis extractors
    # implement the pattern inline; new extractors should prefer
    # these helpers.

    @property
    def snapshot_producer_id(self) -> str:
        """Stable owner of one complete extraction snapshot."""
        return f"_enricher:{self.name}"

    def _snapshot_source_record_id(
        self,
        value: dict[str, Any],
    ) -> dict[str, Any]:
        if self.snapshot_replacement_mode != "complete_producer":
            return value
        record_id = {
            **dict(value),
            "snapshot_replacement_scope": self.snapshot_producer_id,
        }
        if self._current_snapshot_input_ref is not None:
            record_id["snapshot_input_ref"] = self._current_snapshot_input_ref
        return record_id

    def _snapshot_source_revision(
        self,
        value: dict[str, Any],
    ) -> dict[str, Any]:
        if self.snapshot_replacement_mode != "complete_producer":
            return value
        return {
            **dict(value),
            "snapshot_operator_version": type(self).operator_version,
            "snapshot_prompt_hashes": sorted(self._snapshot_prompt_hashes),
        }

    def declared_support_policy(self) -> DerivedSupportPolicy:
        """This extractor's declared derived-node liveness policy.

        The declaration lives on the class (see :attr:`support_policy`) and
        is validated at worker boot and at Publisher construction, so this
        resolution is only a backstop for an extractor built outside that
        path. It raises, naming the class, rather than picking a policy.
        """
        from okg.substrate.derived_node_liveness import (
            resolve_declared_support_policy,
        )

        return resolve_declared_support_policy(self)

    def _emit_node_fact(
        self,
        conn: psycopg.Connection,
        *,
        node_id: str,
        subtype: str,
        attrs: dict[str, Any],
        dedupe_key: str,
        source_record_id: dict[str, Any],
        source_revision: dict[str, Any],
        run_id: str,
        observed_at: datetime,
        op: str = "I",
        derived_from: Sequence[str] | None = None,
        support_policy: DerivedSupportPolicy | None = None,
    ) -> str:
        """INSERT one okg.node_facts row with the projector pattern.

        Thin shim around
        :func:`okg.substrate.library.enrichers._fact_emitters.
        emit_node_fact` that injects `self.name` as the enricher
        name. Always sets `source = "_enricher:<self.name>"`.
        Caller responsible for dedupe_key derivation (typically
        via :func:`make_dedupe_key`).

        ``derived_from`` names the graph node ids this node was produced
        from (PACT ``bind-derived-node-provenance``). An LLM extractor
        derives every node it emits from the source node it was handed, so
        subclasses pass that node's id. Omitting it leaves the node with a
        fail-closed provenance envelope: invisible to Nomos-enforced reads,
        quarantining for readiness, and rejected as an input by the trust
        check.

        ``support_policy`` defaults to this extractor's class-declared
        :attr:`support_policy` (validated at worker boot and at Publisher
        construction). Pass a value only to override that declaration for
        one call.
        """
        original_node_id = node_id
        if self._entity_resolution is not None and self._entity_resolution.enabled_for(
            subtype
        ):
            decision = self._entity_resolution.resolve(
                conn,
                proposed_node_id=node_id,
                subtype=subtype,
                attrs=attrs,
                source_node_ref=(derived_from or [None])[0],
            )
            if decision.canonical_node_id is None:
                raise RuntimeError(
                    "successful entity resolution omitted canonical_node_id"
                )
            node_id = decision.canonical_node_id
            self._resolved_entity_ids[original_node_id] = node_id
            if not decision.materialize:
                if self.snapshot_replacement_mode == "complete_producer":
                    # Entity resolution found a canonical node already live.
                    # If this producer currently owns it, the replacement's
                    # prior-row sweep will see it. Retain it explicitly: there
                    # is intentionally no new node assertion to put after an R.
                    self._snapshot_retained_node_ids.add(node_id)
                return node_id
            attrs = self._entity_resolution.canonicalize_identity_attrs(
                subtype=subtype,
                attrs=attrs,
                proposed_node_id=original_node_id,
                canonical_node_id=node_id,
            )

        emit_kwargs = {
            "enricher_name": self.name,
            "node_id": node_id,
            "subtype": subtype,
            "attrs": attrs,
            "dedupe_key": dedupe_key,
            "source_record_id": self._snapshot_source_record_id(source_record_id),
            "source_revision": self._snapshot_source_revision(source_revision),
            "run_id": run_id,
            "observed_at": observed_at,
            "op": op,
            "derived_from": derived_from,
            "support_policy": (
                support_policy
                if support_policy is not None
                else self.declared_support_policy()
            ),
        }
        if self._pending_snapshot_facts is not None:
            self._pending_snapshot_facts.append(
                ("node", emit_kwargs, self._current_snapshot_input_ref)
            )
        else:
            from okg.substrate.library.enrichers._fact_emitters import (
                emit_node_fact,
            )

            emit_node_fact(conn, **emit_kwargs)
        return node_id

    def _emit_edge_fact(
        self,
        conn: psycopg.Connection,
        *,
        src: str,
        dst: str,
        edge_type: str,
        attrs: dict[str, Any],
        provenance: str,
        confidence: float,
        dedupe_key: str,
        source_record_id: dict[str, Any],
        source_revision: dict[str, Any],
        run_id: str,
        observed_at: datetime,
        op: str = "I",
    ) -> None:
        """INSERT one okg.edge_facts row with the projector pattern.

        Thin shim around
        :func:`okg.substrate.library.enrichers._fact_emitters.
        emit_edge_fact` that injects `self.name`. Mints a
        deterministic `edge_id` from `(src, edge_type, dst)`.
        """
        resolved_src = self._resolved_entity_ids.get(src, src)
        resolved_dst = self._resolved_entity_ids.get(dst, dst)
        if (resolved_src, resolved_dst) != (src, dst):
            dedupe_key = make_dedupe_key(
                "resolved_edge", resolved_src, edge_type, resolved_dst, self.name
            )
        emit_kwargs = {
            "enricher_name": self.name,
            "src": resolved_src,
            "dst": resolved_dst,
            "edge_type": edge_type,
            "attrs": attrs,
            "provenance": provenance,
            "confidence": confidence,
            "dedupe_key": dedupe_key,
            "source_record_id": self._snapshot_source_record_id(source_record_id),
            "source_revision": self._snapshot_source_revision(source_revision),
            "run_id": run_id,
            "observed_at": observed_at,
            "op": op,
        }
        if self._pending_snapshot_facts is not None:
            self._pending_snapshot_facts.append(
                ("edge", emit_kwargs, self._current_snapshot_input_ref)
            )
        else:
            from okg.substrate.library.enrichers._fact_emitters import (
                emit_edge_fact,
            )

            emit_edge_fact(conn, **emit_kwargs)

    # ------------------------------------------------------------------
    # ExtractionDropped emission (base class implementation)
    # ------------------------------------------------------------------

    def emit_dropped(
        self,
        conn: psycopg.Connection,
        candidate: ExtractionCandidate,
        *,
        reason: str = "",
        generation_id: int,
        run_id: str,
        observed_at: datetime,
    ) -> str:
        """Insert one ``extraction_dropped`` NodeFact for a
        below-floor candidate.

        Returns the ``dropped_id`` of the emitted record (for
        downstream logging / promotion linkage).

        Routed through :func:`emit_node_fact` so the below-floor
        twin of the typed-node path gets the same NUL admission
        contract (PACT ``sanitize-fact-text-at-admission``): raw
        ``candidate_text`` in attrs is scrubbed (NUL → U+FFFD,
        counted + warned), and a NUL in an identity-bearing field
        rejects the record loudly instead of aborting the publish
        with ``UntranslatableCharacter``. (``dropped_id`` itself is
        always hex-clean — ``make_dropped_id`` hashes its inputs —
        so a NUL ``source_node_ref`` rejects via the
        ``source_record_id`` it is stored in.)
        """
        dropped_id = make_dropped_id(
            extractor=self.name,
            source_node_ref=candidate.source_node_ref,
            candidate_text=candidate.candidate_text,
        )
        node_attrs: dict[str, Any] = {
            "dropped_id": dropped_id,
            "extractor": self.name,
            "candidate_text": candidate.candidate_text,
            "extractor_confidence": float(candidate.confidence),
            "extractor_floor_at_emit": self.confidence_floor,
            "reason": reason
            or (f"below {self.confidence_floor} floor (got {candidate.confidence})"),
            "source_node_ref": candidate.source_node_ref,
            "operator_disposition": "pending",
            "created_at": observed_at.isoformat(),
            "last_modified_at": observed_at.isoformat(),
        }
        dedupe_key = make_dedupe_key(
            "extraction_dropped",
            dropped_id,
            self.name,
        )
        # Route through the base helper so complete-snapshot extractors buffer
        # below-floor records in the same atomic range as typed output.
        self._emit_node_fact(
            conn,
            node_id=dropped_id,
            subtype="extraction_dropped",
            attrs=node_attrs,
            dedupe_key=dedupe_key,
            source_record_id={
                "enricher": self.name,
                "source_node_ref": candidate.source_node_ref,
            },
            source_revision={
                "run_id": run_id,
                "generation_id": generation_id,
            },
            run_id=run_id,
            observed_at=observed_at,
            derived_from=self.derivation_refs(candidate),
            # Explicit, and deliberately NOT the subclass declaration: a
            # dropped-candidate record is about a specific set of inputs and
            # must not outlive any of them, whatever liveness the subclass
            # chose for its typed nodes.
            support_policy="all_members",
        )
        return dropped_id

    def derivation_refs(self, candidate: "ExtractionCandidate") -> list[str]:
        """Graph node ids a candidate's output was derived FROM.

        PACT bind-derived-node-provenance. Defaults to
        ``candidate.source_node_ref`` — the node the extractor was handed —
        which is right for every extractor whose ``source_node_ref`` IS the
        text it read.

        Override where it is not. ``EvidenceExtractor`` sets
        ``source_node_ref`` to the CLAIM the evidence is about, which is what
        the row asserts, not where it came from; attributing the row to it
        would inherit the claim's trust rather than the trust of the text the
        evidence was actually read out of.
        """
        return [candidate.source_node_ref]

    def _load_prior_snapshot(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Read the currently published graph rows owned by this producer.

        Source ownership is the repair-compatible boundary: rows emitted before
        snapshot metadata existed still carry ``_enricher:<name>`` and are
        therefore included on the first complete rerun.
        """
        from okg.substrate.graph_store import ChronosGraphStore

        graph = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).checkout_generation(generation_id)
        nodes = [
            dict(row)
            for row in graph.query(
                """
                SELECT node_id, subtype, attrs,
                       source_record_id, source_revision
                  FROM okg.graph_nodes
                 WHERE source = :producer_id
                 ORDER BY node_id
                """,
                {"producer_id": self.snapshot_producer_id},
            )
        ]
        edges = [
            dict(row)
            for row in graph.query(
                """
                SELECT src, dst, edge_type, attrs, provenance, confidence,
                       source_record_id, source_revision
                  FROM okg.graph_edges
                 WHERE source = :producer_id
                 ORDER BY src, edge_type, dst
                """,
                {"producer_id": self.snapshot_producer_id},
            )
        ]
        return nodes, edges

    def _require_accepted_snapshot_fact(
        self,
        outcome: Any,
        *,
        label: str,
        item_ref: str | None = None,
    ) -> None:
        if outcome.emitted:
            return
        self._snapshot_rejected_fact_count += 1
        detail = outcome.held_reason if outcome.held else None
        if not detail and outcome.rejected_fields:
            detail = "rejected identity fields: " + ", ".join(outcome.rejected_fields)
        message = f"snapshot fact {label} was not accepted" + (
            f": {detail}" if detail else ""
        )
        if item_ref is not None and outcome.rejected_fields:
            raise ItemSnapshotEmissionRejected(item_ref, message)
        raise RuntimeError(message)

    def _flush_complete_snapshot(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        run_id: str,
        observed_at: datetime,
    ) -> dict[str, int]:
        """Atomically replace the producer's prior graph snapshot.

        Model calls, parsing, and entity resolution have already completed.
        This transaction contains database fact emission only, so any raised
        error or soft ``FactEmitOutcome`` rejection rolls back every retraction
        and assertion in the replacement range.
        """
        from okg.substrate.library.enrichers._fact_emitters import (
            emit_edge_fact,
            emit_node_fact,
        )

        if self._prior_snapshot_cache is None:
            raise RuntimeError(
                "complete snapshot prior rows were not preloaded before "
                "input quarantine claims"
            )
        prior_nodes, prior_edges = self._prior_snapshot_cache
        pending = [
            entry
            for entry in (self._pending_snapshot_facts or ())
            if entry[2] not in self._quarantined_items
        ]
        counters = {
            "nodes_retracted": 0,
            "edges_retracted": 0,
            "nodes_retained": 0,
            "edges_retained": 0,
            "node_assertions_inserted": 0,
            "edge_assertions_inserted": 0,
        }
        replacement_revision = {
            "run_id": run_id,
            "generation_id": generation_id,
            "snapshot_operator_version": type(self).operator_version,
            "snapshot_prompt_hashes": sorted(self._snapshot_prompt_hashes),
        }

        def _retained_for_quarantine(row: Mapping[str, Any]) -> bool:
            if not self._quarantined_items:
                return False
            source_record_id = dict(row.get("source_record_id") or {})
            input_ref = source_record_id.get("snapshot_input_ref")
            if input_ref is None:
                # Upgrade fail-safe: legacy rows predate item attribution.
                # Retain them all rather than silently retracting an active
                # item's last good output.
                return True
            return str(input_ref) in self._quarantined_items

        with conn.transaction():
            for row in prior_edges:
                if _retained_for_quarantine(row):
                    counters["edges_retained"] += 1
                    continue
                graph_identity = (
                    str(row["src"]),
                    str(row["edge_type"]),
                    str(row["dst"]),
                )
                outcome = emit_edge_fact(
                    conn,
                    enricher_name=self.name,
                    src=graph_identity[0],
                    dst=graph_identity[2],
                    edge_type=graph_identity[1],
                    attrs=dict(row.get("attrs") or {}),
                    provenance=str(row.get("provenance") or "extracted_model"),
                    confidence=float(row.get("confidence") or 0.0),
                    dedupe_key=make_snapshot_fact_key(
                        fact_kind="edge",
                        producer_id=self.snapshot_producer_id,
                        op="R",
                        graph_identity=graph_identity,
                    ),
                    source_record_id=self._snapshot_source_record_id(
                        dict(row.get("source_record_id") or {})
                    ),
                    source_revision=replacement_revision,
                    run_id=run_id,
                    observed_at=observed_at,
                    op="R",
                )
                self._require_accepted_snapshot_fact(
                    outcome,
                    label=f"edge retraction {graph_identity!r}",
                )
                counters["edges_retracted"] += int(outcome.inserted)

            for row in prior_nodes:
                node_id = str(row["node_id"])
                if (
                    node_id in self._snapshot_retained_node_ids
                    or _retained_for_quarantine(row)
                ):
                    counters["nodes_retained"] += 1
                    continue
                outcome = emit_node_fact(
                    conn,
                    enricher_name=self.name,
                    node_id=node_id,
                    subtype=str(row["subtype"]),
                    attrs=dict(row.get("attrs") or {}),
                    dedupe_key=make_snapshot_fact_key(
                        fact_kind="node",
                        producer_id=self.snapshot_producer_id,
                        op="R",
                        graph_identity=(node_id,),
                    ),
                    source_record_id=self._snapshot_source_record_id(
                        dict(row.get("source_record_id") or {})
                    ),
                    source_revision=replacement_revision,
                    run_id=run_id,
                    observed_at=observed_at,
                    op="R",
                    # Required by the signature; unused for a retraction,
                    # which writes no support declaration (the emitter
                    # canonicalizes support only for op="I").
                    support_policy="all_members",
                )
                self._require_accepted_snapshot_fact(
                    outcome,
                    label=f"node retraction {node_id!r}",
                )
                counters["nodes_retracted"] += int(outcome.inserted)

            for fact_kind, emit_kwargs, item_ref in pending:
                if fact_kind == "node":
                    outcome = emit_node_fact(conn, **emit_kwargs)
                    counter = "node_assertions_inserted"
                elif fact_kind == "edge":
                    outcome = emit_edge_fact(conn, **emit_kwargs)
                    counter = "edge_assertions_inserted"
                else:  # internal closed vocabulary; never silently skip
                    raise RuntimeError(
                        f"unknown buffered snapshot fact kind {fact_kind!r}"
                    )
                self._require_accepted_snapshot_fact(
                    outcome,
                    label=f"{fact_kind} assertion",
                    item_ref=item_ref,
                )
                counters[counter] += int(outcome.inserted)
        return counters

    # ------------------------------------------------------------------
    # Enrich (substrate enricher Protocol)
    # ------------------------------------------------------------------

    def enrich(
        self,
        conn: psycopg.Connection,
        *,
        generation_id: int,
        **_publisher_kwargs: object,
    ) -> EnrichResult:
        """Route each candidate from :meth:`extract_candidates`
        through the floor predicate. Above-floor → typed node via
        the subclass hook. Below-floor → ``ExtractionDropped``.

        Complete-snapshot subclasses collect facts first, then atomically
        replace their previous producer-owned snapshot.  No input, parse, or
        emission failure is allowed to turn into an authoritative empty set.
        """
        run_id = str(uuid.uuid4())
        observed_at = datetime.now(timezone.utc)
        # Reset the per-run input-failure accumulator; _complete_tracked
        # appends to it as extract_candidates streams items through the
        # LLM. A non-empty list at run end => incomplete => hold watermark.
        self._input_failures = []
        self._resolved_entity_ids = {}
        self._snapshot_prompt_hashes = set()
        self._snapshot_rejected_fact_count = 0
        self._snapshot_retained_node_ids = set()
        self._run_conn = conn
        self._run_deployment_name = (
            str(_publisher_kwargs["deployment_name"])
            if _publisher_kwargs.get("deployment_name")
            else self._publisher_deployment_name
        )
        self._run_branch_name = (
            str(_publisher_kwargs["branch_name"])
            if _publisher_kwargs.get("branch_name")
            else self._publisher_branch_name
        )
        self._run_generation_id = generation_id
        self._input_claims = {}
        self._input_states = {}
        self._quarantined_items = set()
        self._current_snapshot_input_ref = None
        self._prior_snapshot_cache = None
        replacing_snapshot = self.snapshot_replacement_mode == "complete_producer"
        self._pending_snapshot_facts = [] if replacing_snapshot else None
        if self._entity_resolution is not None:
            # Scope the resolution stage's caches to this cycle. Carrying them
            # across cycles would resolve against a stale view of live nodes.
            self._entity_resolution.begin_batch()
            preload_graph = getattr(self._entity_resolution, "preload_graph", None)
            if callable(preload_graph):
                # The real EntityResolutionStage owns a Chronos checkout.
                # Pin it before retry claims; later queries use its cached
                # autocommit=False session inside the publisher transaction.
                preload_graph(conn)

        n_typed = 0
        n_edges = 0
        n_dropped = 0
        n_errors = 0
        replacement_counts = {
            "nodes_retracted": 0,
            "edges_retracted": 0,
            "nodes_retained": 0,
            "edges_retained": 0,
            "node_assertions_inserted": 0,
            "edge_assertions_inserted": 0,
        }

        # Chronos checkout can commit its adapter connection. It therefore
        # happens before claim_inputs(): after the first durable reservation,
        # quarantine, fact replacement, and progress must remain one atomic
        # publisher transaction.
        if replacing_snapshot:
            try:
                self._prior_snapshot_cache = self._load_prior_snapshot(
                    conn,
                    generation_id=generation_id,
                )
            except Exception as exc:  # noqa: BLE001 — systemic, hold
                self._record_input_failure("complete_snapshot", exc)

        candidates: list[ExtractionCandidate] = []
        if not self._input_failures:
            try:
                candidates = list(self.extract_candidates(conn, generation_id))
            except Exception as exc:  # noqa: BLE001 — incomplete, never empty
                if self._publisher_transaction_owned:
                    # A shared-stage exception escaped after at least one
                    # durable reservation.  No per-item accounting can prove
                    # which claims or provider results it covered, so the
                    # Publisher must roll the whole reservation/output/progress
                    # transaction back.  Returning an incomplete run here
                    # would commit half-mutated claim rows.
                    raise
                self._record_input_failure("complete_snapshot", exc)

        # A complete-producer snapshot is all-or-nothing.  If candidate
        # extraction already failed, do not even materialize the successful
        # subset.  Append-mode test/compatibility subclasses retain the prior
        # behavior of admitting their good candidates while holding progress.
        if not (replacing_snapshot and self._input_failures):
            for candidate in candidates:
                self._current_snapshot_input_ref = str(candidate.source_node_ref)
                counted_as_dropped = False
                try:
                    if self.above_floor(candidate.confidence):
                        n_edges += self.emit_typed_node(
                            conn,
                            candidate,
                            generation_id=generation_id,
                            run_id=run_id,
                            observed_at=observed_at,
                        )
                        n_typed += 1
                    else:
                        # ``n_edges_skipped`` counts candidates routed below
                        # the confidence floor, even when admission rejects
                        # the diagnostic record itself (for example, a NUL in
                        # its identity-bearing source reference).
                        n_dropped += 1
                        counted_as_dropped = True
                        self.emit_dropped(
                            conn,
                            candidate,
                            generation_id=generation_id,
                            run_id=run_id,
                            observed_at=observed_at,
                        )
                except Exception as exc:  # noqa: BLE001 — account + hold
                    from okg.substrate.entity_resolution import (
                        EntityResolutionRejected,
                    )

                    if isinstance(exc, EntityResolutionRejected):
                        try:
                            self.emit_dropped(
                                conn,
                                candidate,
                                reason=str(exc),
                                generation_id=generation_id,
                                run_id=run_id,
                                observed_at=observed_at,
                            )
                            if not counted_as_dropped:
                                n_dropped += 1
                            continue
                        except Exception as dropped_exc:  # noqa: BLE001
                            exc = dropped_exc
                    if replacing_snapshot:
                        self._record_input_failure(candidate.source_node_ref, exc)
                        if (
                            str(candidate.source_node_ref)
                            not in self._quarantined_items
                        ):
                            n_errors += 1
                    else:
                        n_errors += 1
                    logger.warning(
                        "%s: candidate emission failed: %s",
                        self.name,
                        exc,
                    )
                finally:
                    self._current_snapshot_input_ref = None

        if replacing_snapshot and not self._input_failures and not n_errors:
            try:
                replacement_counts = self._flush_complete_snapshot(
                    conn,
                    generation_id=generation_id,
                    run_id=run_id,
                    observed_at=observed_at,
                )
            except ItemSnapshotEmissionRejected as exc:
                # The nested snapshot transaction rolled back. Charge the
                # named deterministic input. If that attempt activates its
                # quarantine, retry the same replacement with only that
                # item's pending assertions removed and prior rows retained.
                self._record_input_failure(exc.item_ref, exc)
                if exc.item_ref in self._quarantined_items and not self._input_failures:
                    try:
                        replacement_counts = self._flush_complete_snapshot(
                            conn,
                            generation_id=generation_id,
                            run_id=run_id,
                            observed_at=observed_at,
                        )
                    except Exception as retry_exc:  # noqa: BLE001 — hold
                        n_errors += 1
                        self._record_input_failure(
                            "complete_snapshot",
                            retry_exc,
                        )
                        logger.warning(
                            "%s: quarantine-aware snapshot retry rolled back: %s",
                            self.name,
                            retry_exc,
                        )
            except Exception as exc:  # noqa: BLE001 — transaction rolled back
                n_errors += 1
                self._record_input_failure("complete_snapshot", exc)
                logger.warning(
                    "%s: complete snapshot emission rolled back: %s",
                    self.name,
                    exc,
                )

        snapshot_complete = (
            replacing_snapshot and not self._input_failures and not n_errors
        )
        for item_ref, claim in list(self._input_claims.items()):
            state = self._input_states.get(item_ref)
            if state == "parsed":
                if snapshot_complete:
                    record_success(conn, claim)
                    self._input_states[item_ref] = "succeeded"
                else:
                    # The item itself parsed, but its output was not committed
                    # as part of an authoritative replacement. Do not
                    # supersede an older active signature yet.
                    release_claim(conn, claim)
                    self._input_states[item_ref] = "systemic"

        # The buffer must never escape the invocation.  On incomplete runs it
        # is simply discarded; on complete runs its facts were flushed above.
        self._pending_snapshot_facts = None

        # A run is incomplete when some input item did not fully
        # process: its LLM call failed (n_input_failures) or its
        # above-floor candidate failed to emit (n_errors). Either way
        # the item's typed nodes are missing this cycle, so the run must
        # NOT let the publisher advance the enricher watermark past it
        # (P0 no-silent-data-loss). `incomplete` carries that up.
        n_input_failures = len(self._input_failures)
        incomplete = n_input_failures > 0 or n_errors > 0
        error_msg: Optional[str] = None
        if incomplete:
            segments: list[str] = []
            if n_input_failures:
                # Keep the failure KIND and source item on the operator-facing
                # result.  The detailed warning above is useful while tailing
                # logs, but publisher summaries are often inspected later and
                # used to collapse every provider failure to the same generic
                # "input call failed" message.  That made a qwen reasoning
                # trace exhausting max_output_tokens indistinguishable from a
                # valid empty extraction — the P0 silent-failure shape behind
                # issue #913.  Bound the examples so a provider exception
                # cannot turn one run row into an unbounded payload.
                # Always preserve a truncation example when one exists. A
                # simple ``[:3]`` made the durable result lose that signal if
                # three unrelated provider failures happened first, even
                # though the run correctly stayed incomplete.
                selected = list(self._input_failures[:3])
                truncation = next(
                    (
                        failure
                        for failure in self._input_failures
                        if "truncat" in failure[1].lower()
                    ),
                    None,
                )
                if truncation is not None and truncation not in selected:
                    selected[-1] = truncation

                examples: list[str] = []
                for item_ref, detail in selected:
                    compact = " ".join(str(detail).split())
                    if len(compact) > 240:
                        compact = compact[:237] + "..."
                    examples.append(f"{item_ref}: {compact}")
                omitted = n_input_failures - len(examples)
                suffix = f"; {omitted} more" if omitted else ""
                segments.append(
                    f"{n_input_failures} input LLM call(s) failed "
                    f"({'; '.join(examples)}{suffix})"
                )
            if n_errors:
                segments.append(f"{n_errors} candidate(s) failed emission")
            error_msg = (
                "; ".join(segments)
                + "; watermark held so unprocessed inputs retry next cycle"
            )
        if (
            replacing_snapshot
            and not incomplete
            and (
                replacement_counts["nodes_retracted"]
                or replacement_counts["edges_retracted"]
                or replacement_counts["nodes_retained"]
                or replacement_counts["edges_retained"]
            )
        ):
            logger.warning(
                "%s: superseded prior extraction snapshot "
                "(nodes_retracted=%d, edges_retracted=%d, "
                "nodes_retained=%d, edges_retained=%d, "
                "node_assertions_inserted=%d, "
                "edge_assertions_inserted=%d, generation=%d)",
                self.name,
                replacement_counts["nodes_retracted"],
                replacement_counts["edges_retracted"],
                replacement_counts["nodes_retained"],
                replacement_counts["edges_retained"],
                replacement_counts["node_assertions_inserted"],
                replacement_counts["edge_assertions_inserted"],
                generation_id,
            )
        log_at = logger.warning if incomplete else logger.info
        log_at(
            "%s: emitted %d typed nodes (%d edges) + %d dropped "
            "(floor=%.3f, generation=%d, emit_errors=%d, "
            "input_failures=%d, incomplete=%s)",
            self.name,
            n_typed,
            n_edges,
            n_dropped,
            self.confidence_floor,
            generation_id,
            n_errors,
            n_input_failures,
            incomplete,
        )
        output_lineage: Optional[Mapping[str, Any]] = None
        if replacing_snapshot:
            output_lineage = {
                "schema": "okg.enricher_snapshot_replacement.v1",
                "producer_id": self.snapshot_producer_id,
                "complete": not incomplete,
                "prompt_hashes": sorted(self._snapshot_prompt_hashes),
                "candidate_count": len(candidates),
                "quarantined_count": len(self._quarantined_items),
                "quarantined_item_refs": sorted(self._quarantined_items),
                **replacement_counts,
            }
            if error_msg is not None:
                output_lineage["failure_reason"] = error_msg
        result = EnrichResult(
            enricher_name=self.name,
            n_edges_emitted=0 if replacing_snapshot and incomplete else n_edges,
            n_edges_skipped=0 if replacing_snapshot and incomplete else n_dropped,
            error=error_msg,
            output_lineage=output_lineage,
            incomplete=incomplete,
            n_input_failures=n_input_failures,
            n_quarantined=len(self._quarantined_items),
            rejected_fact_count=self._snapshot_rejected_fact_count,
        )
        self._pending_snapshot_facts = None
        self._prior_snapshot_cache = None
        self._current_snapshot_input_ref = None
        self._run_conn = None
        return result
