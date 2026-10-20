"""View-fragment recheck pipeline (chunks-grain).

Per `add-view-edit-source-recheck` (capability B of the
`add-af-pubs-supervisor-demo-capabilities` umbrella),
refactored per `add-spec-edit-corpus-consistency`.

The portal lets an operator edit a sentence in a rendered
view fragment and click "recheck". This module fans out one
LLM judge call per cited DocumentChunk in parallel, asks
each judge whether the chunk's text contradicts the edit,
and (for divergent calls) localizes the contradicting span
back to a PDF page + bbox via substring match against the
parent publication's `pdf_text_index`.

The chunks-grain shape replaces the previous "send the
entire pdf_text_index for every cited pub in one mega
judge call" design:

  * Each judge sees only the chunk that was actually cited
    as evidence (~4 KB) instead of every page of every
    cited PDF (often 100+ KB per pub). 10× input shrink.
  * Per-chunk parallelism: the recheck of a 20-citation
    fragment fans out to 20 concurrent judge calls and
    aggregates, instead of one serial mega-call.
  * Localization: the judge returns a verbatim quoted
    span from the chunk text. The substrate substring-
    matches that span against the parent pub's
    pdf_text_index lines to recover the precise page +
    bbox. This is tighter than the prior line-index design
    (judge can pick a wrong-but-nearby line and still pass
    range validation; substring match either resolves or
    fails loud).

Contract (called from the recheck endpoint):

    recheck_fragment(
        conn, deployment_name, view_id, fragment_id,
        original_md, edited_md, llm_client,
    ) -> RecheckResult

Pipeline:

  1. Resolve `view_cites_node` edges for the fragment.
     For chunk-citing views (the spec views), the dst is
     a `document_chunk`; we work with chunks directly. For
     the legacy publication-citing views, we walk the pub
     to its top-3 sections' chunks for the same-fragment
     concept (still a token-grain reduction vs the prior
     full-pdf approach).
  2. Load chunk text + parent pub_id for each cited chunk.
  3. Fan out one judge call per chunk via ThreadPoolExecutor
     (default 8 workers). Each judge sees: original
     fragment, edited fragment, one chunk's text. Returns
     `{"verdict", "quoted_span", "severity", "rationale"}`.
  4. For divergent verdicts, localize `quoted_span` against
     the parent pub's `pdf_text_index.lines[]` via
     substring match. The matching line's `page` + `bbox`
     become the highlight. No match → reject with
     `quoted_span_unlocalized` warning and continue.
  5. Aggregate per-chunk verdicts → fragment verdict:
     any divergent → divergent; else any uncertain →
     uncertain; else consistent. Highlights are the union
     of localized chunk highlights.

Loud-failure modes:
  - `PdfTextIndexMissing(node_id)` → endpoint maps to 422.
    Raised when a parent pub has no pdf_text_index AND a
    judge returned divergent for it (consistent / no-match
    verdicts don't need localization, so they don't trip).
  - `LLMCallFailed` from any worker → propagates; the
    pool short-circuits and the endpoint maps to 502.
  - `RecheckJudgeMalformed` → judge returned non-JSON or
    a missing field; same 502 mapping.
  - `RecheckSpendConfigInvalid` → half-declared or
    non-positive spend config; endpoint maps to 500
    `recheck_spend_config_invalid`.
"""
from __future__ import annotations

import difflib
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Optional

from okg.views.execution.llm import LiteLLMClient


# Per-chunk judge fan-out width. Eight workers covers the
# typical 10-30 cited chunks per fragment without saturating
# OpenRouter rate limits or local CPU. The width now lives on the
# `views.recheck_judge` concurrency term rather than here, so this
# lane is counted in the same budget as every other parallel lane
# instead of being a bare constant nothing reconciles.
def _chunk_judge_workers() -> int:
    from okg.substrate.db.concurrency_budget import width

    return width("views.recheck_judge")


__all__ = [
    "Highlight",
    "RecheckResult",
    "PdfTextIndexMissing",
    "RecheckJudgeMalformed",
    "RecheckSpendConfigInvalid",
    "recheck_fragment",
]


log = logging.getLogger(__name__)


# Severity domain — bounded so the portal can map to colors
# without dispatching on free-form strings.
_SEVERITIES = {"high", "medium", "low"}
_VERDICTS = {"consistent", "divergent", "uncertain"}


@dataclass(frozen=True)
class Highlight:
    """One divergent line in one source publication.

    `(page, bbox)` are copied directly from
    `pdf_text_index.lines[line_idx]` so the portal does not
    need to round-trip through the index. `bbox` is the
    PDF user-space `[x, y, width, height]` from pdfplumber.
    """
    source_node_id: str
    page: int
    bbox: list[float]  # [x, y, width, height] in PDF user-space
    line_idx: int
    severity: str
    rationale: str


@dataclass
class RecheckResult:
    """Recheck verdict + validated highlights.

    `validation_warnings` records any highlight rejected by
    step 5 (e.g. `line_idx_out_of_range`); the endpoint
    surfaces these in telemetry but does not return them in
    the API response body.
    """
    verdict: str  # consistent | divergent | uncertain
    highlights: list[Highlight] = field(default_factory=list)
    validation_warnings: list[dict[str, Any]] = field(default_factory=list)


class PdfTextIndexMissing(Exception):
    """One of the cited source pubs lacks a populated
    `pdf_text_index`. Endpoint maps to 422
    `pdf_text_index_missing`."""

    def __init__(self, node_id: str, reason: str = "missing"):
        super().__init__(
            f"source pub {node_id!r} has no populated "
            f"pdf_text_index ({reason})"
        )
        self.node_id = node_id
        self.reason = reason


class RecheckJudgeMalformed(Exception):
    """The LLM response could not be parsed into the
    documented JSON contract. Endpoint maps to 502
    `judge_call_failure`."""

    def __init__(self, detail: str, raw: str):
        super().__init__(f"recheck judge response malformed: {detail}")
        self.detail = detail
        self.raw = raw


class RecheckSpendConfigInvalid(Exception):
    """The recheck spend declaration is half-present or
    non-positive. A cap alone cannot reserve (no per-call
    estimate); an estimate alone bounds nothing. Endpoint
    maps to 500 `recheck_spend_config_invalid` — a partial
    declaration is a config error, not a silently-ignored
    half-wish. Deliberately NOT a ValueError: the endpoint
    maps ValueError to 404 (no cited sources), and a config
    error masquerading as a missing fragment would send the
    operator hunting in the wrong place."""


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def recheck_fragment(
    graph,
    *,
    deployment_name: str,
    view_id: str,
    fragment_id: str,
    original_md: str,
    edited_md: str,
    llm_client: LiteLLMClient,
    recheck_spend_cap_usd: Optional[float] = None,
    recheck_spend_estimate_usd: Optional[float] = None,
    spend_settlement_dsn: Optional[str] = None,
) -> RecheckResult:
    """Chunks-grain parallel recheck.

    Pipeline (see module docstring):
      1. Resolve cited chunks for the fragment.
      2. Fan out one judge call per chunk in parallel.
      3. Localize divergent quoted_spans against the
         parent pub's pdf_text_index.
      4. Aggregate.

    ``graph`` is a Chronos checkout session (see
    ``ChronosGraphStore.checkout_generation``) pinned to the
    generation the recheck reads. The graph-content reads
    (cited chunks, pdf_text_index) run against the checkout's
    ``okg.graph_nodes`` / ``okg.graph_edges``.

    ``recheck_spend_cap_usd`` / ``recheck_spend_estimate_usd``
    bound the fragment's total judge spend: each judge call
    reserves the estimate before dispatch and the fan-out
    refuses calls past the cap. Both-or-neither — a partial
    declaration raises. Undeclared means this lane is spend-
    UNBOUNDED (the pre-existing behavior), never a silently
    invented default. ``spend_settlement_dsn`` is where
    settlement charges the shared cost ledger; when absent the
    cap still binds in-process but the ledger never learns.

    Raises:
      * `ValueError` — no cited chunks for this fragment.
      * `RecheckSpendConfigInvalid` — half-declared or
        non-positive spend config.
      * `PdfTextIndexMissing(node_id)` — divergent verdict
        on a chunk whose parent pub lacks a usable
        pdf_text_index.
      * `RecheckJudgeMalformed` — judge returned malformed
        JSON.
      * `LLMCallFailed` — propagates from LiteLLMClient, and
        raised here when every chunk judge fails or is
        refused by the spend cap.
    """
    # Spend declaration sanity BEFORE any graph read: a config
    # error must not be reported as a missing fragment (404) or
    # burn a fan-out finding out. Both-or-neither, mirroring the
    # invariant-judge and extractor lanes.
    cap_present = recheck_spend_cap_usd is not None
    est_present = recheck_spend_estimate_usd is not None
    if cap_present != est_present:
        raise RecheckSpendConfigInvalid(
            "recheck_spend_cap_usd and recheck_spend_estimate_usd must be "
            "declared together (got only "
            f"{'recheck_spend_cap_usd' if cap_present else 'recheck_spend_estimate_usd'}); "
            "the cap bounds the fragment's total judge spend, the estimate "
            "is reserved per call and should be an upper bound — declare "
            "both, or neither (which leaves the lane spend-unbounded)"
        )
    for money_key, money_val in (
        ("recheck_spend_cap_usd", recheck_spend_cap_usd),
        ("recheck_spend_estimate_usd", recheck_spend_estimate_usd),
    ):
        if money_val is not None and (
            isinstance(money_val, bool)
            or not isinstance(money_val, (int, float))
            or float(money_val) <= 0
        ):
            raise RecheckSpendConfigInvalid(
                f"{money_key} must be a positive number; got {money_val!r}"
            )

    cited_chunks = _load_cited_chunks(
        graph, view_id=view_id, fragment_id=fragment_id,
    )
    if not cited_chunks:
        raise ValueError(
            f"view {view_id!r} has no view_cites_node edges "
            f"for fragment_id={fragment_id!r} resolvable to "
            f"chunks"
        )

    # Compute the diff ONCE up front and ship it to every chunk
    # judge instead of the full original+edited bodies. Two
    # benefits:
    #   1. ~10× fewer prompt tokens per chunk on a multi-KB
    #      spec (the spec body is duplicated per chunk in the
    #      old shape; the diff is typically ~1% of the body).
    #   2. The judge has an explicit anchor on what changed —
    #      no need to scan the whole markdown looking for the
    #      delta. Detection rate goes up materially on tiny
    #      edits (one cell in a long table) that the old shape
    #      could miss because the LLM never noticed the diff.
    diff_text = _unified_diff(original_md, edited_md)
    if not diff_text.strip():
        # No edit at all — return consistent immediately. Saves
        # the whole fan-out cost and matches user intent
        # (clicking Recheck without changes is a no-op).
        return RecheckResult(
            verdict="consistent", highlights=[], validation_warnings=[],
        )

    # Fan out judge calls. ThreadPoolExecutor is fine here —
    # each worker is a network call (httpx → OpenRouter); the
    # GIL releases for I/O.
    #
    # Per-chunk fault tolerance: if one judge call raises (the
    # provider returned an empty body, a content-filter block,
    # a transient 5xx, …) we drop that chunk's verdict and
    # continue. The aggregator records a warning so the
    # endpoint surfaces it in telemetry; the fragment-level
    # verdict is computed from the chunks that did land. The
    # alternative — failing the whole recheck on the first
    # bad chunk — would make the chunks-grain shape MORE
    # fragile than the prior single-call shape, the opposite
    # of the design intent.
    # Dispatched through the shared model-call fan-out rather than a private
    # ThreadPoolExecutor. This lane was already correct and bounded, but it was
    # a SECOND implementation of the two-phase split, and a second
    # implementation is how the two drift — the substrate's own fan-out gained
    # k-of-N handling, an admission seam and a rate term that this copy never
    # would have.
    from okg.substrate.llm.fanout import (
        AdmissionDecision,
        ModelCallJob,
        complete_many,
    )
    from okg.substrate.llm.spend import ModelCallSpend

    per_chunk: list[_PerChunkVerdict] = []
    judge_errors: list[dict[str, Any]] = []

    jobs = [
        ModelCallJob(
            system="", user="", typed_input=ch,
            item_ref=str(ch["chunk_id"]),
        )
        for ch in cited_chunks
    ]
    verdicts: dict[int, _PerChunkVerdict] = {}

    def _call(job: ModelCallJob) -> str:
        # The judge helper returns a structured verdict, not text; stash it and
        # hand the helper a sentinel. Phase 2 touches no connection here, so
        # there is nothing to record.
        verdicts[id(job)] = _judge_chunk(
            llm_client=llm_client,
            view_id=view_id,
            fragment_id=fragment_id,
            diff_text=diff_text,
            chunk=job.typed_input,
        )
        return "ok"

    # Spend admission for the judge lane. The view spec owns the budget
    # (the same declaration surface the judge model comes from); scope is
    # the view, so a runaway recheck on one view cannot spend another's
    # budget. An undeclared cap leaves this lane unbounded and `enabled`
    # reports it — no invented per-call estimate, which would be wrong by
    # an unknown factor and invisible.
    spend = ModelCallSpend(
        deployment=deployment_name,
        scope=f"recheck-judge:{view_id}",
        cap_usd=recheck_spend_cap_usd,
        estimated_usd_per_call=recheck_spend_estimate_usd,
    )
    reservations: dict[str, Any] = {}

    def _admit(job):
        decision = spend.reserve(None)
        reservations[job.item_ref] = decision
        if not decision.admitted:
            return AdmissionDecision(False, reason=decision.reason)
        return AdmissionDecision(True, reserved_usd=decision.reserved_usd)

    # Settlement charges the shared cost ledger, which needs a connection.
    # The recheck pipeline holds no DSN of its own — the checkout `graph`
    # wraps the endpoint's request connection, which phase 2 must not
    # borrow for ledger writes mid-request — so the ENDPOINT passes the
    # deployment DSN it already uses for okg.llm_calls tracking, and we
    # open a short-lived settlement connection on it. When no DSN was
    # passed (stub-client tests, direct callers) settlement is SKIPPED
    # loudly: the cap still binds in-process via reservations, but the
    # ledger never learns what was spent. We never invent a DSN.
    settle_conn = None
    if spend.enabled and spend_settlement_dsn:
        # Through the sanctioned direct-connect wrapper, not raw
        # psycopg.connect: a raw connect would bypass the concurrency
        # budget's connection accounting (see
        # test_every_raw_connect_site_is_enumerated).
        from okg.substrate.db.connection import connect as db_connect
        settle_conn = db_connect(spend_settlement_dsn, autocommit=True)
    elif spend.enabled:
        log.warning(
            "recheck: spend cap declared for scope %r but no settlement "
            "DSN was provided; the cap is enforced in-process but this "
            "fragment's judge spend will NOT land in the shared cost "
            "ledger", f"recheck-judge:{view_id}",
        )

    def _settle(job, _decision, outcome):
        admission = reservations.get(job.item_ref)
        if outcome.budget_refused:
            # Never issued: release the reservation, record nothing.
            # Runs even with no settlement DSN — release is in-memory.
            spend.release(admission)
        elif settle_conn is None:
            # Warned above: the cap binds in-process, the ledger is not
            # reachable on this call path.
            return
        elif outcome.provider_error is not None:
            # Issued but raised: record the attempt at zero, not the
            # estimate — okg.llm_calls carries any billed cost.
            spend.settle(settle_conn, admission, 0.0)
        else:
            # No per-call cost is reported on this path; the estimate
            # (an upper bound by contract) is the charge.
            spend.settle(settle_conn, admission, None)

    try:
        outcomes = complete_many(
            jobs,
            call=_call,
            derive_key=lambda job: job.item_ref,
            record=lambda index, key, content: content,
            admit=_admit,
            settle=_settle if spend.enabled else None,
            width=_chunk_judge_workers(),
        )
    finally:
        if settle_conn is not None:
            settle_conn.close()

    for job, chunk, outcome in zip(jobs, cited_chunks, outcomes):
        if outcome.ok:
            per_chunk.append(verdicts[id(job)])
            continue
        if outcome.budget_refused:
            # Distinguishable from a provider failure on purpose: an
            # operator staring at "all judges failed" must be able to
            # tell "raise the cap" from "the provider is down". The
            # admission reason names the scope, the cap, and the
            # per-call estimate.
            adm = reservations.get(job.item_ref)
            kind = "spend_refused"
            detail = (
                adm.reason if adm is not None and not adm.admitted
                else "admission refused"
            )
        else:
            exc = outcome.provider_error
            kind = type(exc).__name__ if exc else "unknown"
            detail = str(exc)
        log.warning(
            "recheck: chunk judge failed; chunk_id=%s kind=%s detail=%s",
            chunk["chunk_id"], kind, detail,
        )
        judge_errors.append({
            "reason": "judge_call_failed",
            "chunk_id": chunk["chunk_id"],
            "kind": kind,
            "detail": detail[:200],
        })

    if not per_chunk:
        # Every judge failed or was refused. That IS a fragment-level
        # failure; bubble up so the endpoint's 502 path fires. The first
        # error's detail rides along so an all-refused recheck names the
        # exhausted cap instead of reading like a provider outage.
        from okg.views.execution.llm import LLMCallFailed
        first = judge_errors[0] if judge_errors else {
            "kind": "unknown", "detail": "",
        }
        raise LLMCallFailed(
            f"recheck: all {len(cited_chunks)} chunk judges failed; "
            f"first error kind={first['kind']} detail={first['detail']}"
        )

    # Localize divergent verdicts to PDF coords.
    pub_ids_needed = sorted({
        v.chunk["pub_id"] for v in per_chunk
        if v.verdict == "divergent" and v.quoted_span
    })
    indexes_by_pub: dict[str, list[dict[str, Any]]] = {}
    if pub_ids_needed:
        indexes_by_pub = _load_pdf_text_indexes(graph, pub_ids_needed)

    result = _aggregate_per_chunk(
        per_chunk=per_chunk,
        indexes_by_pub=indexes_by_pub,
    )
    # Surface per-chunk judge failures alongside aggregation
    # warnings so the endpoint telemetry sees them.
    if judge_errors:
        result.validation_warnings.extend(judge_errors)
    return result


# ---------------------------------------------------------------------------
# Step 1 — cited sources for (view, fragment)
# ---------------------------------------------------------------------------


def _load_cited_chunks(
    graph,
    *,
    view_id: str,
    fragment_id: str,
) -> list[dict[str, Any]]:
    """Cited DocumentChunks for this fragment, with the
    fields the per-chunk judge needs.

    For chunk-citing views (the spec views) the
    `view_cites_node` edge already targets the chunk; we
    pull the chunk's text + parent pub_id directly.

    For legacy publication-citing views (concept_corpus_*
    et al.) the edge targets a publication. We fall back to
    an embedding-driven nearest-K-chunks-of-this-pub query
    so the chunks-grain judge has something to work with;
    the K is small (3) since the cited pub is broad and the
    judge only needs samples.

    Returns one dict per chunk:
      {"chunk_id": str, "pub_id": str, "section_id": str|None,
       "text": str}
    """
    view_node_id = f"view:{view_id}"
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    # Direct chunk citations.
    for row in graph.query(
        """
        SELECT
            ch.node_id,
            ch.attrs->>'pub_id'     AS pub_id,
            ch.attrs->>'section_id' AS section_id,
            ch.attrs->>'text'       AS text
          FROM okg.graph_edges el
          JOIN okg.graph_nodes ch
            ON ch.node_id = el.dst
           AND ch.subtype = 'document_chunk'
         WHERE el.src = :view_node_id
           AND el.edge_type = 'view_cites_node'
           AND el.attrs->>'fragment_id' = :fragment_id
        ORDER BY ch.node_id
        """,
        {"view_node_id": view_node_id, "fragment_id": fragment_id},
    ):
        nid = row["node_id"]
        pub_id = row["pub_id"]
        text = row["text"]
        if nid in seen or not text or not pub_id:
            continue
        seen.add(nid)
        out.append({
            "chunk_id": nid,
            "pub_id": pub_id,
            "section_id": row["section_id"],
            "text": text,
        })

    if out:
        return out

    # Legacy: publication citations. Sample top-3 chunks
    # by cosine similarity to the publication's own
    # title-and-body embedding (acts as a stand-in for
    # the fragment's topic). This keeps the legacy views
    # working under the chunks-grain judge without
    # changing their materializers. K is small because
    # the dispatch is broad and the judge only needs
    # representative samples.
    cited_pubs = [
        row["node_id"]
        for row in graph.query(
            """
            SELECT DISTINCT pub.node_id
              FROM okg.graph_edges el
              JOIN okg.graph_nodes pub
                ON pub.node_id = el.dst
               AND pub.subtype = 'publication'
             WHERE el.src = :view_node_id
               AND el.edge_type = 'view_cites_node'
               AND el.attrs->>'fragment_id' = :fragment_id
            """,
            {"view_node_id": view_node_id, "fragment_id": fragment_id},
        )
    ]
    if not cited_pubs:
        return out

    for pub_node_id in cited_pubs:
        for row in graph.query(
            """
            SELECT ch.node_id,
                   ch.attrs->>'section_id' AS section_id,
                   ch.attrs->>'text'       AS text
              FROM okg.graph_nodes ch
             WHERE ch.subtype = 'document_chunk'
               AND ch.attrs->>'pub_id' = :pub_id
             ORDER BY length(ch.attrs->>'text') DESC NULLS LAST
             LIMIT 3
            """,
            {"pub_id": pub_node_id},
        ):
            nid = row["node_id"]
            text = row["text"]
            if nid in seen or not text:
                continue
            seen.add(nid)
            out.append({
                "chunk_id": nid,
                "pub_id": pub_node_id,
                "section_id": row["section_id"],
                "text": text,
            })
    return out


# ---------------------------------------------------------------------------
# Step 2 — load per-pub pdf_text_index
# ---------------------------------------------------------------------------


def _load_pdf_text_indexes(
    graph,
    cited_sources: list[str],
) -> dict[str, list[dict[str, Any]]]:
    """Return `{node_id: lines[]}`. Raise on any miss.

    A miss is one of:
      * `attrs.pdf_text_index` key absent;
      * `attrs.pdf_text_index = null` (scanned PDF or
        pre-enricher pub);
      * `attrs.pdf_text_index.lines` not an array or empty.

    Any miss raises `PdfTextIndexMissing(node_id, reason)`
    on the FIRST miss encountered. The spec is loud-failure;
    we don't aggregate.
    """
    if not cited_sources:
        return {}
    out: dict[str, list[dict[str, Any]]] = {}
    rows = {
        r["node_id"]: r["pdf_text_index"]
        for r in graph.query(
            """
            SELECT node_id, attrs->'pdf_text_index' AS pdf_text_index
              FROM okg.graph_nodes
             WHERE subtype = 'publication'
               AND node_id = ANY(:cited_sources)
             ORDER BY node_id
            """,
            {"cited_sources": cited_sources},
        )
    }

    for node_id in cited_sources:
        if node_id not in rows:
            raise PdfTextIndexMissing(
                node_id, reason="publication_node_missing",
            )
        idx = rows[node_id]
        if idx is None:
            raise PdfTextIndexMissing(node_id, reason="null")
        if not isinstance(idx, dict):
            raise PdfTextIndexMissing(
                node_id, reason="not_an_object",
            )
        lines = idx.get("lines")
        if not isinstance(lines, list) or not lines:
            raise PdfTextIndexMissing(
                node_id, reason="empty_lines",
            )
        out[node_id] = lines
    return out


# ---------------------------------------------------------------------------
# Step 3 — prompts
# ---------------------------------------------------------------------------


_SYSTEM_PROMPT = """\
You are a precision proofreader for Air Force publication
overviews. The user provides:

  * an EDIT — a unified diff of the operator's change to a
    system-generated specification fragment. Lines starting
    with `-` are the original wording; `+` lines are what
    the operator changed it to. Surrounding `  ` (two-space)
    lines are unchanged context.
  * one CITED CHUNK of source-PDF text from a single
    publication.

Your job: decide whether the cited chunk SUPPORTS or
CONTRADICTS the operator's EDIT. Focus on the `+` lines
(the new wording) and compare them against the chunk.

Return STRICT JSON (no markdown fence, no prose, just JSON):

  {
    "verdict": "consistent" | "divergent" | "uncertain",
    "quoted_span": "<verbatim substring of the chunk text>",
    "severity": "high" | "medium" | "low",
    "rationale": "<one short sentence; what the chunk says vs what the edit changed it to>"
  }

Rules:
  * `verdict=consistent` => the chunk SUPPORTS the new
    `+` wording (or is silent in a way that doesn't
    contradict). `quoted_span` MUST be empty string.
  * `verdict=divergent`  => the chunk CONTRADICTS a
    specific factual claim in the `+` wording. The chunk
    must state a different value, threshold, procedure, or
    constraint than what the edit asserts.
    `quoted_span` MUST be a VERBATIM substring of the
    chunk's text — copy it character-for-character from
    the chunk so the substrate can locate it back in the
    source PDF. Do NOT paraphrase, summarize, or re-flow
    whitespace. Pick the SHORTEST contiguous substring
    that captures the contradicting fact (typically a
    single sentence, sometimes just a clause with the
    contradicting number).
  * `verdict=uncertain`  => the chunk is silent on the
    edited claim, the chunk is ambiguous, or you cannot
    localize. `quoted_span` MUST be empty string.
  * `severity=high` is for direct factual contradictions of
    numbers, dates, thresholds, callsigns, or procedures.
    Tone or paraphrase differences are `low`.
  * Ignore the `-` lines except as context for what the
    edit modified. The judgement is "does this chunk
    contradict the NEW wording", not "is the OLD wording
    right".
"""


# Diff context budget. unified_diff context lines on either side
# of a hunk help the LLM understand what the edit modified
# (which sentence, which paragraph, which table row). Three is
# the unified-diff convention.
_DIFF_CONTEXT_LINES = 3


def _unified_diff(original_md: str, edited_md: str) -> str:
    """Compute a stable unified-diff string between original_md
    and edited_md. Empty string when they're identical.

    Newlines are normalized so a CR/LF mismatch isn't reported
    as a real edit.
    """
    a = original_md.replace("\r\n", "\n").splitlines(keepends=True)
    b = edited_md.replace("\r\n", "\n").splitlines(keepends=True)
    diff_iter = difflib.unified_diff(
        a, b,
        fromfile="original",
        tofile="edited",
        n=_DIFF_CONTEXT_LINES,
        lineterm="",
    )
    return "\n".join(diff_iter)


def _build_chunk_prompt(
    *,
    diff_text: str,
    chunk: dict[str, Any],
) -> tuple[str, str]:
    """Build the (system, user) prompt for one chunk."""
    parts: list[str] = []
    parts.append("# EDIT (unified diff of the operator's change)")
    parts.append("```diff")
    parts.append(diff_text)
    parts.append("```")
    parts.append("")
    parts.append(f"# CITED CHUNK (from {chunk['pub_id']})")
    if chunk.get("section_id"):
        parts.append(f"Section: {chunk['section_id']}")
    parts.append(f"Chunk id: {chunk['chunk_id']}")
    parts.append("")
    parts.append("--- chunk text begins ---")
    parts.append(chunk["text"])
    parts.append("--- chunk text ends ---")
    parts.append("")
    parts.append(
        "# OUTPUT — STRICT JSON ONLY (no fence, no prose)"
    )
    return _SYSTEM_PROMPT, "\n".join(parts)


@dataclass(frozen=True)
class _PerChunkVerdict:
    """One judge call's outcome for one chunk."""
    chunk: dict[str, Any]
    verdict: str
    quoted_span: str
    severity: str
    rationale: str
    raw_response: str = ""


def _judge_chunk(
    *,
    llm_client: LiteLLMClient,
    view_id: str,
    fragment_id: str,
    diff_text: str,
    chunk: dict[str, Any],
) -> _PerChunkVerdict:
    """Run one judge call against a single chunk."""
    system, user = _build_chunk_prompt(
        diff_text=diff_text,
        chunk=chunk,
    )
    # Stable outcome key (spec §9.2); retention/replay wiring for the
    # recheck path is owned by a later child — the digest lands on
    # okg.llm_calls now so history is joinable.
    from okg.substrate.llm.outcomes import (
        OutcomeKey,
        canonical_input_hash,
        prompt_content_hash,
    )
    key = OutcomeKey(
        step_id=f"recheck:{view_id}",
        operator_version="v1",
        model_id=llm_client.model,
        prompt_hash=prompt_content_hash(system),
        input_hash=canonical_input_hash(
            {"diff_text": diff_text, "chunk_id": chunk["chunk_id"]}
        ),
    )
    raw = llm_client.complete(
        system,
        user,
        caller=f"recheck:{view_id}",
        fragment_id=f"{fragment_id}:{chunk['chunk_id']}",
        outcome_key_digest=key.digest,
    )
    payload = _coerce_json(raw)
    verdict = payload.get("verdict")
    if verdict not in _VERDICTS:
        raise RecheckJudgeMalformed(
            f"verdict not in {sorted(_VERDICTS)}: {verdict!r}",
            raw=raw,
        )
    severity = payload.get("severity", "low")
    if severity not in _SEVERITIES:
        # Default to "low" rather than raising — severity is
        # advisory, not load-bearing for verdict aggregation.
        severity = "low"
    quoted_span = payload.get("quoted_span") or ""
    rationale = payload.get("rationale") or ""
    if verdict == "divergent" and not quoted_span.strip():
        # Loud: the contract says divergent MUST carry a
        # span. Downgrade to uncertain rather than fabricate
        # one.
        verdict = "uncertain"
    return _PerChunkVerdict(
        chunk=chunk,
        verdict=verdict,
        quoted_span=str(quoted_span),
        severity=severity,
        rationale=str(rationale).strip(),
        raw_response=raw,
    )


# ---------------------------------------------------------------------------
# Step 4-7 — parse + validate
# ---------------------------------------------------------------------------


def _aggregate_per_chunk(
    *,
    per_chunk: list[_PerChunkVerdict],
    indexes_by_pub: dict[str, list[dict[str, Any]]],
) -> RecheckResult:
    """Aggregate per-chunk verdicts into a fragment-level
    result, localizing divergent quoted_spans to PDF coords
    via substring match against the parent pub's
    pdf_text_index."""
    highlights: list[Highlight] = []
    warnings: list[dict[str, Any]] = []
    saw_divergent = False
    saw_uncertain = False

    for v in per_chunk:
        if v.verdict == "divergent":
            saw_divergent = True
        elif v.verdict == "uncertain":
            saw_uncertain = True

        if v.verdict != "divergent" or not v.quoted_span:
            continue

        pub_id = v.chunk["pub_id"]
        chunk_text = v.chunk["text"]
        chunk_id = v.chunk["chunk_id"]

        # Soft sanity check: the span MUST be a substring of
        # the chunk text we sent. If the judge paraphrased,
        # warn and downgrade this evidence.
        if v.quoted_span not in chunk_text:
            warnings.append({
                "reason": "quoted_span_not_in_chunk",
                "chunk_id": chunk_id,
                "quoted_span_preview": v.quoted_span[:120],
            })
            continue

        if pub_id not in indexes_by_pub:
            warnings.append({
                "reason": "pdf_text_index_unavailable",
                "pub_id": pub_id,
                "chunk_id": chunk_id,
            })
            continue

        match = _localize_span(
            indexes_by_pub[pub_id], v.quoted_span,
        )
        if match is None:
            warnings.append({
                "reason": "quoted_span_unlocalized",
                "pub_id": pub_id,
                "chunk_id": chunk_id,
                "quoted_span_preview": v.quoted_span[:120],
            })
            continue

        highlights.append(
            Highlight(
                source_node_id=pub_id,
                page=int(match["page"]),
                bbox=list(match["bbox"]),
                line_idx=int(match.get("idx", -1)),
                severity=v.severity,
                rationale=v.rationale,
            )
        )

    # Aggregate verdict.
    if highlights:
        verdict = "divergent"
    elif saw_divergent:
        # Judge said divergent but nothing localized →
        # downgrade rather than mislead.
        log.info(
            "recheck: downgrading 'divergent' to 'uncertain' "
            "— no quoted_span localized to a PDF line"
        )
        verdict = "uncertain"
    elif saw_uncertain:
        verdict = "uncertain"
    else:
        verdict = "consistent"

    return RecheckResult(
        verdict=verdict,
        highlights=highlights,
        validation_warnings=warnings,
    )


def _normalize_for_match(s: str) -> str:
    """Whitespace-collapse for substring matching against
    pdf_text_index lines, which often have line-wrap
    whitespace that differs from chunk text."""
    return " ".join(s.split())


def _localize_span(
    lines: list[dict[str, Any]], span: str,
) -> Optional[dict[str, Any]]:
    """Find the first pdf_text_index line whose normalized
    text contains the normalized span. Returns the matching
    line dict (with `page`, `bbox`, `idx`) or None.

    If the span doesn't fit in any single line (it crosses
    a line break in the source PDF), fall back to picking
    the line with the longest overlap of the span's leading
    words. This is approximate — proper multi-line spans
    deserve a multi-line highlight, but for the demo a
    single-line anchor is acceptable.
    """
    if not lines or not span.strip():
        return None
    norm_span = _normalize_for_match(span)

    # Single-line direct match.
    for line in lines:
        if not isinstance(line, dict):
            continue
        text = _normalize_for_match(line.get("text") or "")
        if not text:
            continue
        if norm_span in text:
            page = line.get("page")
            bbox = line.get("bbox")
            if isinstance(page, int) and isinstance(bbox, list):
                return line

    # Approximate fallback: pick the line that contains the
    # longest leading prefix of the span (in tokens). Avoids
    # hot returns on accidental short-word matches.
    span_tokens = norm_span.split()
    if len(span_tokens) < 3:
        return None
    best: Optional[dict[str, Any]] = None
    best_overlap = 2  # require at least 3-token prefix match
    for line in lines:
        if not isinstance(line, dict):
            continue
        text = _normalize_for_match(line.get("text") or "")
        if not text:
            continue
        line_tokens = text.split()
        # Walk the line tokens looking for the longest
        # common prefix at any starting position.
        for start in range(len(line_tokens)):
            k = 0
            while (k < len(span_tokens)
                   and start + k < len(line_tokens)
                   and span_tokens[k].lower() == line_tokens[start + k].lower()):
                k += 1
            if k > best_overlap:
                best_overlap = k
                best = line
                break
    if best is not None:
        page = best.get("page")
        bbox = best.get("bbox")
        if isinstance(page, int) and isinstance(bbox, list):
            return best
    return None


def _coerce_json(raw: str) -> dict[str, Any]:
    """Tolerate models that wrap JSON in a ```json fence
    despite being instructed otherwise; reject anything else."""
    s = raw.strip()
    if s.startswith("```"):
        # Strip a fenced block.
        s = s.strip("`")
        # Handle ``` and ```json forms.
        first_newline = s.find("\n")
        if first_newline == -1:
            raise RecheckJudgeMalformed(
                "fenced response with no body", raw=raw,
            )
        s = s[first_newline + 1:].strip()
        if s.endswith("```"):
            s = s[: -3].rstrip()
    try:
        payload = json.loads(s)
    except json.JSONDecodeError as exc:
        raise RecheckJudgeMalformed(
            f"json decode failed: {exc}", raw=raw,
        ) from exc
    if not isinstance(payload, dict):
        raise RecheckJudgeMalformed(
            f"top-level value not an object: "
            f"{type(payload).__name__}",
            raw=raw,
        )
    return payload


def _lookup_line(
    lines: list[dict[str, Any]], line_idx: int,
) -> Optional[dict[str, Any]]:
    """Find the line whose `idx` equals `line_idx`. Falls
    back to positional lookup if no `idx` field is present.
    Returns None if not found / out of range."""
    if not lines:
        return None
    # Fast path: positional lookup matches when lines[] is
    # contiguous and sorted by idx (the af_pubs case).
    if 0 <= line_idx < len(lines):
        candidate = lines[line_idx]
        if (
            isinstance(candidate, dict)
            and candidate.get("idx") == line_idx
        ):
            return candidate
    # Fallback: linear scan.
    for line in lines:
        if isinstance(line, dict) and line.get("idx") == line_idx:
            return line
    return None
