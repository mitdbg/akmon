"""Substrate-facing benchmark runner: generate → publish → detect → score.

This is the RUNNER side of the purity boundary (design §3): it may import
okg substrate code — ``okg.bench.injection.*`` is
allow-listed benchmark machinery, and the ingestion/publish/detection path
IS the system under test. The synth core stays substrate-free.

The loop mirrors the ring-0 shape: publish the CLEAN corpus (its findings
are false positives by definition — the clean-corpus gate), then publish
the CORRUPTED corpus (value-free claim ids make each corrupted rendering
UPDATE the same claim identity), then score both states against the sealed
manifest with the synth-native scorecard builder (per-stage × per-class ×
per-partition; construction-time gold, no B0 exclusion).
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

from okg.bench.synth.config import (
    ACTION_CHECK,
    ACTION_CORRECTION,
    ACTION_EXTRACT_DOCUMENT,
    ACTION_PARSE,
    ACTION_PUBLISH,
    SynthConfig,
    load_config,
    load_preset,
)
from okg.bench.synth.cost import (
    assert_sensitivity_invariants,
    qualification,
    remaining_error_cost,
    render_cost_blocks,
    sealed_cost_model,
    spent_action_cost,
)
from okg.bench.synth.corruption import (
    PARTITION_CROSS_DOC,
    PARTITION_INTERNAL_ONLY,
    PARTITION_UNDETECTABLE,
    PARTITION_WORLD_KNOWLEDGE,
    Planner,
)
from okg.bench.synth.fhir_map import world_from_synthea_dir
from okg.bench.synth.goldseal import (
    build_manifest,
    load_manifest,
    scan_unsanctioned_surface,
    write_manifest,
)
from okg.bench.synth.llm_extract import LlmExtractor, pack_labels
from okg.bench.synth.prf import SynthPrf
from okg.bench.synth.resolution import (
    gold_constraint_kind,
    BucketResolution,
    adjudicate,
    corruption_bucket_map,
    funnel,
    load_resolutions,
    per_flavor_mis_repair,
    per_kind_detection,
    poison_flip_report,
    poisoned_corruptions_of,
    STATE_DETECTED_ONLY,
    STATE_MIS_REPAIRED,
    STATE_NOT_DETECTED,
    STATE_REPAIRED,
)
from okg.bench.synth.render import load_pack, pack_bytes, render_corpus

SMOKE_DEPLOYMENT = "synthetic-corpus-smoke"
SMOKE_CHECK_ID = "synthetic-corpus-smoke.ir.claim_state.contested_bucket.v1"
MANIFEST_FILENAME = "benchmark-manifest.json"
SCORECARD_FILENAME = "synth-scorecard.json"
#: One row per injected error, beside the scorecard rather than inside it so
#: the published numbers stay byte-pinnable without carrying the per-error
#: record (concord-failure-taxonomy, req.tax.per-error-record).
FUNNEL_ROWS_FILENAME = "synth-funnel-rows.json"
CORRECTIONS_FILENAME = "synth-corrections.json"
RUN_BINDING_FILENAME = "run-binding.json"
# LLM-lane artifact set (req.attrib.llm-lane): the pinned engine-side reads that
# re-score rebuilds from, plus the extraction-side record that reproduces the A /
# survival / cost blocks and audits every published claim against a verbatim
# response.
ENGINE_READS_FILENAME = "engine-reads.json"
EXTRACTION_FAILURES_FILENAME = "extraction-failures.json"
EXTRACTION_COST_FILENAME = "extraction-cost.json"


def _responses_filename(phase: str) -> str:
    return f"llm-responses-{phase}.jsonl"


def _claims_filename(phase: str) -> str:
    return f"extracted-claims-{phase}.json"


class RunnerError(RuntimeError):
    """Typed runner failure (publish refused, gate breached, ...)."""


def _slug(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


# -- extraction-side canonicalization --------------------------------------

_UNIT_RE = re.compile(r"^(?P<num>\d+(?:\.\d+)?) (?P<unit>g|mm)$")
_CANONICAL_UNITS = {"g": ("kg", 0.001), "mm": ("cm", 0.1)}


def canonicalize_value(value: str) -> str:
    """Deployment-side numeric unit canonicalization (design §7).

    The benign unit-equivalent family renders '82000 g' for a canonical
    '82.0 kg'; extraction canonicalizes so truthful variation does not
    become a contested bucket. This is the mechanism the family stresses:
    remove it and the twins flag as FPs in the scorecard.
    """
    match = _UNIT_RE.match(value)
    if match is None:
        return value
    unit, factor = _CANONICAL_UNITS[match.group("unit")]
    return f"{float(match.group('num')) * factor:.1f} {unit}"


def _canonicalize_claim_fact(fact: Any) -> Any:
    """Unit-canonicalize one claim fact — the SHARED parity mechanism.

    Both lanes route their claims through this one function, so LLM-extracted
    claims are canonicalized identically to the deterministic ones. That is what
    keeps a benign unit-equivalent twin from flagging as a false positive in the
    LLM lane: parity is guaranteed by using the same code path, not asserted by
    hand (req.attrib.llm-lane — canonicalization parity is part of the contract).
    """
    if fact.subtype == "consistency_claim":
        value = fact.attrs.get("value", "")
        canonical = canonicalize_value(value)
        if canonical != value:
            return replace(fact, attrs={**fact.attrs, "value": canonical})
    return fact


class CanonicalizingClaimSource:
    """ClaimCorpusSource wrapper that canonicalizes claim values."""

    def __init__(self, repo_root: Path, policies: tuple[str, ...]) -> None:
        from okg.bench.injection.claim_corpus_source import (
            ClaimCorpusSource,
        )

        self._inner = ClaimCorpusSource(repo_root=repo_root, policies=policies)

    def run(self, run_id: str, **kwargs: Any) -> Any:
        from okg.substrate.library.sources.base import SourceRun

        inner_run = self._inner.run(run_id, **kwargs)
        return SourceRun(
            facts=self._canonicalized(inner_run.facts),
            completed_scope=True,
            run_mode=inner_run.run_mode,
        )

    @staticmethod
    def _canonicalized(facts: Any) -> Iterator[Any]:
        for fact in facts:
            yield _canonicalize_claim_fact(fact)


def functional_policies(
    manifest: Mapping[str, Any], exclude: frozenset[str] = frozenset()
) -> tuple[str, ...]:
    """Every rendered predicate is functional (one value per patient), MINUS the
    predicates a coverage corruption re-kinds: a predicate carrying an
    incompatible_pair or linked_pair source policy may not also be functional
    (the engine names one constraint per predicate), so those are excluded here
    and their policy is minted by :func:`constraint_policy_facts` instead."""
    return tuple(
        sorted(
            {_slug(row["predicate_label"]) for row in manifest["trace"]} - exclude
        )
    )


def _predicate_by_fact(manifest: Mapping[str, Any]) -> dict[str, str]:
    """fact_id -> predicate slug, from the sealed trace (labels are per-kind, so
    identical across the documents a fact renders in)."""
    out: dict[str, str] = {}
    for row in manifest["trace"]:
        out.setdefault(row["fact_id"], _slug(row["predicate_label"]))
    return out


def constraint_policy_facts(
    manifest: Mapping[str, Any],
) -> tuple[list[Any], frozenset[str]]:
    """The non-functional Σ policy nodes a run needs, plus the predicates to
    EXCLUDE from the functional set — both derived from the sealed gold, not from
    config (req.sigma.kinds: coverage is proven from finding rows, the policies
    are what admits them). Minted at the ingest seam exactly like the functional
    policies, the operator PRECEDENT nodes, and the arm stamps.

    Per corruption's ``constraint_kind`` (in sealed gold ``params``):

    * ``incompatible_pair`` — one predicate_policy row with the forbidden
      ``(value_a, value_b)`` pair; the predicate is excluded from functional.
    * ``linked_pair`` — one cross-predicate policy (source predicate ->
      target predicate, ``normalize_identity`` link kernel, the target's clean
      value as the single ``violates_values`` entry); the SOURCE predicate is
      excluded from functional so the rewritten-to-entity source line does not
      also register a functional conflict on its own bucket.
    * ``unresolved_key`` — one policy naming the ambiguous subject key; the
      predicate STAYS functional (the engine only scans an unresolved key under
      a predicate that already carries a pairwise policy).
    """
    from okg.substrate.library.sources.base import NodeFact

    pred_by_fact = _predicate_by_fact(manifest)
    facts: list[Any] = []
    exclude: set[str] = set()
    for site in manifest["gold"]["corruptions"]:
        params = dict(site.get("params", {}))
        kind = params.get("constraint_kind")
        if kind == "incompatible_pair":
            predicate = pred_by_fact[site["fact_id"]]
            policy_id = f"incompatible_{predicate}"
            facts.append(
                NodeFact(
                    f"policy:incompatible_pair:{predicate}",
                    "predicate_policy",
                    {
                        "enabled": "true",
                        "policy_id": policy_id,
                        "policy_kind": "incompatible_pair",
                        "predicate": predicate,
                        "value_a": params["value_a"],
                        "value_b": params["value_b"],
                    },
                    {"id": f"policy:incompatible_pair:{predicate}"},
                    {"v": "1"},
                )
            )
            exclude.add(predicate)
        elif kind == "linked_pair":
            source = pred_by_fact[site["fact_id"]]
            target = pred_by_fact[params["target_fact_id"]]
            policy_id = f"linked_{source}_{target}"
            facts.append(
                NodeFact(
                    f"policy:linked_pair:{source}:{target}",
                    "predicate_policy",
                    {
                        "enabled": "true",
                        "policy_id": policy_id,
                        "policy_kind": "linked_pair",
                        "predicate": source,
                        "target_predicate": target,
                        "link_key_fn": "normalize_identity",
                        "violates_values": json.dumps([params["violates_value"]]),
                    },
                    {"id": f"policy:linked_pair:{source}:{target}"},
                    {"v": "1"},
                )
            )
            exclude.add(source)
        elif kind == "unresolved_key":
            entity = params["unresolved_entity"]
            predicate = pred_by_fact[site["fact_id"]]
            facts.append(
                NodeFact(
                    f"policy:unresolved_key:{entity}",
                    "predicate_policy",
                    {
                        "enabled": "true",
                        "policy_id": f"unresolved_{entity}",
                        "policy_kind": "unresolved_key",
                        "predicate": predicate,
                        "unresolved_key": entity,
                    },
                    {"id": f"policy:unresolved_key:{entity}"},
                    {"v": "1"},
                )
            )
    # Dedupe by node_id (two corruptions could re-kind the same predicate);
    # a later duplicate is dropped, the first mint wins.
    seen: set[str] = set()
    deduped: list[Any] = []
    for fact in facts:
        if fact.node_id in seen:
            continue
        seen.add(fact.node_id)
        deduped.append(fact)
    return deduped, frozenset(exclude)


# -- the LLM extraction lane -----------------------------------------------


class _AuthError(RunnerError):
    """The provider refused for auth/credit reasons — the run is BLOCKED, not a
    per-document flake. Never retried, never swallowed into a document failure:
    treating an expired key as 'every document failed' would silently convert a
    credentials problem into a plausible-looking under-floor result."""


def _is_auth_error(exc: BaseException) -> bool:
    name = type(exc).__name__.lower()
    if "authentication" in name or "permissiondenied" in name:
        return True
    msg = str(exc).lower()
    return any(
        token in msg
        for token in (
            "insufficient credit",
            "insufficient_quota",
            "invalid api key",
            "no auth credentials",
            "unauthorized",
            "payment required",
            " 401",
            " 402",
        )
    )


def _labels_for_world(world: Any, pack_name: str = "clinical_v1") -> tuple[str, ...]:
    """The closed attribute vocabulary the extractor is constrained to — every
    fact kind the world carries, expanded through the pack's prefix rules so a
    label slugs to the SAME predicate the gold buckets on."""
    pack = load_pack(pack_name)
    world_kinds = {fact.kind for fact in world.facts}
    return pack_labels(pack, world_kinds)


def _default_llm_workers() -> int:
    """Documents-in-flight for the LLM lane, from OKG_SYNTH_LLM_CONCURRENCY.

    Concurrency is an EXECUTION knob, not corpus semantics: it cannot change
    which claims a run extracts (per-document extraction shares no state, and
    every artifact is assembled in sorted document order regardless of
    completion order), so it lives in the environment rather than the sealed
    config, and moving it never moves a digest. Default 8 — sequential
    extraction made a 96-document corpus a four-hour run when it is almost
    entirely time spent waiting on the provider.
    """
    raw = os.environ.get("OKG_SYNTH_LLM_CONCURRENCY", "8")
    try:
        workers = int(raw)
    except ValueError as exc:
        raise RunnerError(
            f"OKG_SYNTH_LLM_CONCURRENCY={raw!r} is not an integer"
        ) from exc
    if workers < 1:
        raise RunnerError(f"OKG_SYNTH_LLM_CONCURRENCY must be >= 1, got {workers}")
    return workers


@dataclass
class LlmLane:
    """The prose lane's extractor, wired into publish with unit-canonicalization
    parity and bounded per-document retry.

    Reproducibility is by RECORDING, not by pretending determinism: every model
    response is captured verbatim, the extracted-claim snapshot is kept, and a
    document whose extraction call still fails after ``max_retries`` retries is
    marked extraction-failed (its sites become missed_at_extraction with the
    cause recorded) while the run CONTINUES — one flaky document must not kill a
    capstone, and a silent skip must not hide the loss.

    Failure has two BLAST RADII, distinguished in the ledger by ``scope``: a
    document-fatal cause (provider failure after retries, empty/truncated
    content, unparseable output, missing H1) loses the whole document and is a
    ``scope: "document"`` row; a quote that cannot be scoped to a single section
    (ambiguous across sections, or non-verbatim) loses only THAT fact and is a
    ``scope: "fact"`` row while the document's other facts land normally. The
    fact-scope drop is deterministic given the response, so it never triggers a
    document retry — the guard's judgment is unchanged, only its reach.

    Documents extract CONCURRENTLY (``max_workers`` in flight; the
    OKG_SYNTH_LLM_CONCURRENCY env var sets the default). Parallelism cannot
    change the result: per-document extraction shares no state, artifacts are
    assembled in sorted document order after all futures resolve (byte-identical
    to a sequential run), cost accounting is lock-protected, and an auth/credit
    refusal cancels every unstarted document and raises at once — fail-closed
    semantics survive the thread pool.
    """

    extractor: LlmExtractor
    max_retries: int = 2
    #: Carry forward a document's extraction when its BYTES are unchanged
    #: (concord-reuse-defect-isolation, req.rdi.affordable). A history replay
    #: re-reads the whole corpus at every update: 22 updates over 168 documents
    #: is 3,806 model calls and about four dollars, when one document changed
    #: and the other 167 produce identical claims paid for 22 times. Cost is why
    #: every realistic result in this program is a single sample.
    #:
    #: Keyed on (path, CONTENT DIGEST) and never on the path alone: a name-keyed
    #: cache would serve a stale reading for a document that changed, which
    #: corrupts every downstream measurement in a way no existing check catches.
    #:
    #: OFF by default. Under a nondeterministic reader, re-reading identical
    #: bytes may legitimately return something different, so enabling this
    #: FIXES the first reading for those bytes. That is exactly what a replay
    #: wants (it requires extraction recorded once and replayed verbatim) and is
    #: a behaviour change anywhere else, so callers opt in.
    reuse_unchanged: bool = False
    backoff_base_s: float = 0.5
    sleep: Callable[[float], None] = time.sleep
    max_workers: int = field(default_factory=_default_llm_workers)
    responses: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    failures: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    claim_snapshots: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    phase_cost: dict[str, dict[str, Any]] = field(default_factory=dict)

    def _extract_doc(
        self, relpath: str, text: str, digest: str
    ) -> tuple[list[Any], str, int, list[dict[str, str]]]:
        """One document, with bounded retry + backoff. Returns the facts, the
        recorded response, the attempt count, and the per-FACT failure rows
        (dropped quotes that could not be scoped).

        Retry is DOCUMENT-level only: a per-fact quote failure is deterministic
        given the response — retrying the same call would drop the same fact —
        so it is collected into ``fact_failures`` and never re-raised, and the
        loop returns on the first response that parses. Only a document-fatal
        error (provider failure, empty/truncated content, unparseable output,
        missing H1) raises and is retried; the last error is re-raised when the
        retries are exhausted, and auth/credit failures re-raise at once."""
        last_exc: BaseException | None = None
        for attempt in range(self.max_retries + 1):
            fact_failures: list[dict[str, str]] = []
            try:
                response_text = self.extractor._complete(text)
                doc_facts = list(
                    self.extractor._claims_from_response(
                        relpath, text, response_text, digest=digest,
                        fact_failures=fact_failures,
                    )
                )
                return doc_facts, response_text, attempt + 1, fact_failures
            except Exception as exc:  # noqa: BLE001 — provider + extraction failures
                if _is_auth_error(exc):
                    raise _AuthError(
                        f"OpenRouter refused for auth/credit reasons on {relpath}: "
                        f"{exc}. The run is BLOCKED — fix credentials, do not rescope "
                        "to the deterministic lane."
                    ) from exc
                last_exc = exc
                if attempt < self.max_retries:
                    self.sleep(self.backoff_base_s * (2**attempt))
        assert last_exc is not None
        raise last_exc

    def extract(
        self, corpus_dir: Path, policies: tuple[str, ...], *, phase: str
    ) -> list[Any]:
        """Extract one corpus into publishable facts (policies + claims).

        Populates ``responses``/``failures``/``claim_snapshots``/``phase_cost``
        for the phase, canonicalizes every claim value through the SHARED parity
        helper, and continues past a failed document (its claims simply never
        land, so its corruption sites fall to missed_at_extraction downstream).
        """
        from okg.substrate.library.sources.base import NodeFact

        root = Path(corpus_dir).resolve()
        docs = sorted(p.relative_to(root).as_posix() for p in root.rglob("*.md"))
        if not docs:
            raise RunnerError(f"LLM lane: corpus {root} contains no .md documents")

        facts: list[Any] = [
            NodeFact(
                f"policy:functional:{predicate}",
                "predicate_policy",
                {
                    "enabled": "true",
                    "policy_id": f"functional_{predicate}",
                    "policy_kind": "functional",
                    "predicate": predicate,
                },
                {"id": f"policy:functional:{predicate}"},
                {"v": "1"},
            )
            for predicate in sorted(policies)
        ]

        # One document, executed in a worker. Returns (response_row,
        # failure_rows, doc_facts): a document-fatal failure sets response_row
        # None and failure_rows to the single ``scope: "document"`` row (no
        # facts); a success sets response_row and returns the document's facts
        # plus any ``scope: "fact"`` rows for quotes that could not be scoped.
        # _AuthError propagates (a credentials problem is a BLOCKED run, never a
        # doc row).
        cache = getattr(self, "_extraction_cache", None)
        if cache is None:
            cache = {}
            object.__setattr__(self, "_extraction_cache", cache)

        def _one(
            relpath: str,
        ) -> tuple[dict[str, Any] | None, list[dict[str, Any]], list[Any]]:
            text = (root / relpath).read_text(encoding="utf-8")
            digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
            if self.reuse_unchanged:
                hit = cache.get((relpath, digest))
                if hit is not None:
                    return hit
            try:
                doc_facts, response_text, attempts, fact_failures = self._extract_doc(
                    relpath, text, digest
                )
            except _AuthError:
                raise
            except Exception as exc:  # noqa: BLE001 — retries exhausted → doc failed
                return None, [{
                    "scope": "document",
                    "doc": relpath,
                    "error_type": type(exc).__name__,
                    "cause": str(exc),
                    "attempts": self.max_retries + 1,
                }], []
            fact_rows = [
                {
                    "scope": "fact",
                    "doc": relpath,
                    "attribute": row["attribute"],
                    "quote": row["quote"],
                    "cause": row["cause"],
                }
                for row in fact_failures
            ]
            result = (
                {"doc": relpath, "attempts": attempts, "response": response_text},
                fact_rows,
                doc_facts,
            )
            if self.reuse_unchanged:
                # Only a SUCCESS is cached. Caching a failure would make one
                # flaky call permanent for those bytes, converting a transient
                # provider error into a recorded extraction loss.
                cache[(relpath, digest)] = result
            return result

        results: dict[str, tuple[dict[str, Any] | None, list[dict[str, Any]], list[Any]]] = {}
        workers = max(1, int(self.max_workers))
        if workers == 1 or len(docs) == 1:
            for relpath in docs:
                results[relpath] = _one(relpath)
        else:
            from concurrent.futures import ThreadPoolExecutor, as_completed

            pool = ThreadPoolExecutor(max_workers=min(workers, len(docs)))
            try:
                futures = {pool.submit(_one, relpath): relpath for relpath in docs}
                auth_failure: _AuthError | None = None
                for future in as_completed(futures):
                    try:
                        results[futures[future]] = future.result()
                    except _AuthError as exc:
                        # Fail closed and fast: cancel everything unstarted; the
                        # few in-flight calls drain on shutdown, their results
                        # are discarded, and the run raises as one BLOCKED unit.
                        auth_failure = exc
                        for pending in futures:
                            pending.cancel()
                        break
            finally:
                pool.shutdown(wait=True, cancel_futures=True)
            if auth_failure is not None:
                raise auth_failure

        # Assemble in SORTED DOCUMENT ORDER, not completion order: the recorded
        # artifacts (responses.jsonl, claim snapshot) and the fact stream are
        # byte-identical to a sequential run, so concurrency is invisible to
        # every downstream reader.
        responses: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        snapshot: list[dict[str, Any]] = []
        for relpath in docs:
            response_row, failure_rows, doc_facts = results[relpath]
            # Both scopes flow into the one ledger, discriminated by the row's
            # ``scope`` field: a document row when the doc was lost, fact rows
            # for the dropped quotes of a document that otherwise succeeded.
            failures.extend(failure_rows)
            if response_row is None:
                continue
            responses.append(response_row)
            for fact in doc_facts:
                canonical = _canonicalize_claim_fact(fact)
                facts.append(canonical)
                snapshot.append(
                    {"node_id": canonical.node_id, **dict(canonical.attrs)}
                )

        self.responses[phase] = responses
        self.failures[phase] = failures
        self.claim_snapshots[phase] = snapshot
        # Cumulative cost after this phase (the extractor's counter runs across
        # both publishes); the total is the last phase's snapshot.
        self.phase_cost[phase] = self.extractor.cost.as_dict()
        return facts

    def all_failures(self) -> list[dict[str, Any]]:
        return [
            {"phase": phase, **row}
            for phase in sorted(self.failures)
            for row in self.failures[phase]
        ]


# -- publish + detection ---------------------------------------------------


def extract_facts(
    corpus_dir: Path,
    policies: tuple[str, ...],
    *,
    lane: "LlmLane | None" = None,
    phase: str = "",
) -> list[Any]:
    """Route one corpus to its extractor and return the publishable facts.

    The routing seam (req.attrib.llm-lane): ``lane=None`` reads the corpus with
    the deterministic label parser (byte-for-byte the old path); an ``LlmLane``
    reads it with the LLM extractor. Both emit the identical NodeFact shape and
    the identical unit canonicalization, so publish and score are common."""
    if lane is None:
        source = CanonicalizingClaimSource(repo_root=corpus_dir, policies=policies)
        return list(source.run("bench-synth").facts)
    return lane.extract(Path(corpus_dir), policies, phase=phase)


def apply_provenance_stamps(
    facts: list[Any], provenance_plan: Mapping[str, Any] | None
) -> list[Any]:
    """Stamp the sealed ``provenance_group`` onto the corrupted-rendering claims
    of every majority site at the ingest seam (synth-provenance-diversity).

    This is the provenance twin of :func:`apply_arm_stamps`: the sealed plan says
    which extracted claim gets which provenance group, applied to the EXTRACTED
    claim (never rendered into the document), so the corpus is byte-identical
    between flavors and a claim lost at extraction has nothing to stamp. The attr
    is a DECLARED AXIS the engine reads no meaning from today — a stamped and an
    unstamped publish of the same corpus yield row-wise identical findings and
    resolutions (the engine-parity witness). Shared by both lanes.

    ``provenance_plan`` is ``manifest['provenance']`` (``None`` / disabled / empty
    ``group_by_claim`` → no stamps, the facts pass through unchanged).
    """
    group_by_claim: Mapping[str, str] = (provenance_plan or {}).get(
        "group_by_claim", {}
    ) or {}
    if not group_by_claim:
        return list(facts)
    out: list[Any] = []
    for fact in facts:
        group = group_by_claim.get(fact.node_id)
        if group is not None and fact.subtype == "consistency_claim":
            from okg.bench.synth.provenance import PROVENANCE_GROUP_ATTR

            out.append(
                replace(fact, attrs={**fact.attrs, PROVENANCE_GROUP_ATTR: group})
            )
        else:
            out.append(fact)
    return out


def apply_arm_stamps(
    facts: list[Any],
    arm_plan: Mapping[str, Any] | None,
    admits: Sequence[str] = (),
    *,
    provenance_admitted: bool = False,
) -> list[Any]:
    """Apply the precedence-arm ingest stamps to the extracted claim facts and
    mint the operator PRECEDENT nodes (synth-precedence-arms).

    This is the ingest seam: the sealed PLAN says which extracted claim gets which
    decisive-signal attr, applied to the EXTRACTED claim (never rendered into the
    document), so a claim lost at extraction has nothing to stamp and the funnel
    stays consistent. The stamped claim set is then run through the per-arm scope
    condition (assert_arm_signal_scope) as a LOUD guard — a signal the arm did not
    declare fails the publish rather than silently settling a bucket. Shared by
    both lanes: the deterministic and LLM facts are stamped identically.

    ``arm_plan`` is ``manifest['precedence_arm']['plan']`` (``None`` / empty
    targets → no stamps; the default arm's facts pass through unchanged except the
    scope guard runs). Returns the facts to publish (stamped claims + precedents).
    """
    from okg.substrate.library.sources.base import NodeFact

    stamp_by_claim: dict[str, dict[str, str]] = {}
    precedent_specs: dict[str, dict[str, str]] = {}
    poisoned_claim_ids: set[str] = set()
    for target in (arm_plan or {}).get("targets", ()):
        for claim_id, attrs in target.get("stamps", {}).items():
            stamp_by_claim.setdefault(claim_id, {}).update(attrs)
        for spec in target.get("precedents", ()):
            precedent_specs[spec["node_id"]] = spec
        # The poison targets' wrong-authority stamps are admitted by the scope guard
        # on EXACTLY these claim ids (synth-authority-poisoning), never globally.
        if target.get("poison"):
            poisoned_claim_ids.update(target.get("stamps", {}))

    out: list[Any] = []
    for fact in facts:
        stamp = stamp_by_claim.get(fact.node_id)
        if stamp is not None and fact.subtype == "consistency_claim":
            out.append(replace(fact, attrs={**fact.attrs, **stamp}))
        else:
            out.append(fact)
    for spec in sorted(precedent_specs.values(), key=lambda s: s["node_id"]):
        out.append(
            NodeFact(
                spec["node_id"],
                "claim_precedent",
                {
                    "prefer_source": spec["prefer_source"],
                    "scope": spec.get("scope", ""),
                    "sequence": str(spec.get("sequence", "0")),
                },
                {"id": spec["node_id"]},
                {"v": "1"},
            )
        )

    # LOUD scope guard at the ingest point: only admitted signals may be present,
    # and a provenance-group stamp is admitted only when the run declared the
    # provenance axis (synth-provenance-diversity) — an undeclared ranking-signal
    # stamp still fails loudly, unchanged.
    from okg.bench.synth.resolution import assert_arm_signal_scope

    assert_arm_signal_scope(
        (f for f in out if f.subtype == "consistency_claim"),
        admits,
        provenance_admitted=provenance_admitted,
        poisoned_claim_ids=frozenset(poisoned_claim_ids),
    )
    return out


def correction_facts(plan: Any) -> list[Any]:
    """Build the ``claim_correction`` NodeFacts a CorrectionsPlan describes —
    the engine's real correction shape (claim_effective_derivation). Minted at
    the ingest seam like every other operator decision node. Only the attrs each
    action needs are set (a NULL attr means 'leave unchanged' / 'not an assert
    coordinate'), so the tier-1 validation pre-pass accepts them."""
    from okg.substrate.library.sources.base import NodeFact

    out: list[Any] = []
    for spec in plan.specs:
        attrs: dict[str, str] = {
            "action": spec.action,
            "target_claim_id": spec.target_claim_id,
            "sequence": str(spec.sequence),
            "correction_id": spec.correction_id,
        }
        if spec.value is not None:
            attrs["value"] = spec.value
        if spec.predicate is not None:
            attrs["predicate"] = spec.predicate
        if spec.normalized_key is not None:
            attrs["normalized_key"] = spec.normalized_key
        if spec.source_id is not None:
            attrs["source_id"] = spec.source_id
        out.append(
            NodeFact(
                spec.correction_id,
                "claim_correction",
                attrs,
                {"id": spec.correction_id},
                {"v": "1"},
            )
        )
    return out


def effective_claims_at(dsn: str, generation_id: Any) -> dict[str, dict[str, str]]:
    """The effective claim set C_eff at a pinned generation: claim_id ->
    {value, provenance}, read from ``okg.claim_effective`` through the generation
    CHECKOUT (the kept table is chronos-versioned, so an unpinned SELECT returns
    no rows). This is the row-level surface a correction's effect is OBSERVED on,
    never a re-read of the corrections that were emitted (req.c.corrections)."""
    import psycopg

    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkout = store.checkout_generation(generation_id)
        rows = checkout.query(
            "SELECT row_key, value, provenance FROM okg.claim_effective "
            "WHERE row_kind = :kind ORDER BY row_key",
            {"kind": "effective_claim"},
        )
    return {
        str(row["row_key"]): {
            "value": row["value"],
            "provenance": row["provenance"],
        }
        for row in rows
    }


def ingest_and_publish(
    dsn: str,
    corpus_dir: Path,
    deployment_dir: Path,
    policies: tuple[str, ...],
    previous_ids: frozenset[str] = frozenset(),
    *,
    lane: "LlmLane | None" = None,
    phase: str = "",
    arm_plan: Mapping[str, Any] | None = None,
    admits: Sequence[str] = (),
    provenance_plan: Mapping[str, Any] | None = None,
    extra_policy_facts: Sequence[Any] = (),
) -> tuple[dict[str, Any], frozenset[str]]:
    """Full-scope ingest + REAL publish of one rendered corpus.

    ``lane`` selects the extractor: ``None`` keeps the deterministic label lane
    byte-for-byte; an ``LlmLane`` routes the corpus through the LLM extractor
    (req.attrib.llm-lane). Both emit the identical NodeFact shape and share the
    unit canonicalization, so the publish path below is common to both.

    ``arm_plan`` / ``admits`` drive the precedence-arm ingest stamps
    (synth-precedence-arms): the sealed plan's stamps are applied to the extracted
    claims and the operator precedent nodes are minted before publish. The default
    (empty) arm is a no-op except the loud scope guard.

    ``provenance_plan`` drives the provenance-group stamps
    (synth-provenance-diversity): the sealed plan's group is stamped onto each
    majority site's corrupted-rendering claims BEFORE the arm scope guard runs, so
    the guard admits the provenance axis explicitly. Disabled / None → no stamps.
    """
    facts = extract_facts(corpus_dir, policies, lane=lane, phase=phase)
    facts = apply_provenance_stamps(facts, provenance_plan)
    facts = apply_arm_stamps(
        facts,
        arm_plan,
        admits,
        provenance_admitted=bool((provenance_plan or {}).get("enabled")),
    )
    # The coverage Σ policies (incompatible_pair / linked_pair / unresolved_key)
    # are minted here at the ingest seam, alongside the functional policies the
    # extractor emits — the deployment needs them to admit the relational
    # constraint kinds (synth-constraint-coverage). Non-claim facts, so the arm
    # scope guard above (claims only) is unaffected.
    facts = list(facts) + list(extra_policy_facts)
    return publish_fact_list(dsn, deployment_dir, facts, previous_ids)


def publish_fact_list(
    dsn: str,
    deployment_dir: Path,
    facts: Sequence[Any],
    previous_ids: frozenset[str] = frozenset(),
) -> tuple[dict[str, Any], frozenset[str]]:
    """Apply a pre-built fact list and run one REAL publish cycle.

    The publish half of :func:`ingest_and_publish`, factored out so the capstone
    flow (task.complete.capstone) can republish a generation's ALREADY-extracted
    facts plus correction nodes — a third generation that reuses the second's
    extracted claims verbatim, never re-calling the LLM (which would drift the
    claim set out from under the corrections' C_eff delta). ``previous_ids`` names
    the prior generation's claim ids so a claim that vanished is retracted.
    """
    import psycopg

    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.publisher.factory import build_deployment_publisher

    facts = list(facts)
    current_ids = frozenset(
        fact.node_id for fact in facts if fact.subtype == "consistency_claim"
    )
    retractions = [
        NodeFact(node_id, "consistency_claim", {}, {"id": node_id}, {"gone": "1"}, op="R")
        for node_id in sorted(previous_ids - current_ids)
    ]
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        store.apply_facts(
            facts + retractions, source=SMOKE_DEPLOYMENT, strict_endpoints=False
        )
    # Must go through the factory, not a bare Publisher: only the factory
    # resolves the catalog writer identity, and publish refuses without it.
    out = build_deployment_publisher(
        dsn=dsn,
        deployment_name=SMOKE_DEPLOYMENT,
        deployment_dir=Path(deployment_dir),
        command="okg bench synth",
    ).cycle()
    if out.get("status") != "published":
        raise RunnerError(f"publish did not complete: {out}")
    return out, current_ids


def routed_buckets(dsn: str) -> frozenset[str]:
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT participant_roles->>'bucket'
                  FROM okg.observation_current
                 WHERE check_id = %s AND finding_status = 'open'
                """,
                (SMOKE_CHECK_ID,),
            )
            rows = cur.fetchall()
    routed = set()
    for (raw,) in rows:
        if raw:
            predicate, key = json.loads(raw)
            routed.add(f"{predicate}:{key}")
    return frozenset(routed)


def resolutions_at(
    dsn: str, generation_id: Any, claim_values: Mapping[str, str]
) -> tuple[Any, ...]:
    """``bucket_resolution`` rows at a pinned generation, winners joined to
    their values.

    Read through a generation CHECKOUT: okg.claim_projection is
    chronos-versioned, so a bare SELECT outside one returns no rows — which
    would silently score every corruption ``unadjudicated`` rather than fail.
    """
    import psycopg

    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkout = store.checkout_generation(generation_id)
        return load_resolutions(checkout, claim_values)


def finding_kinds(dsn: str, generation_id: Any) -> dict[str, str]:
    """finding row_key -> constraint_kind at a pinned generation.

    The read PipelineState carries a finding's channel and bucket but not the
    constraint KIND it was raised under; per-kind recall needs it, so it is read
    here from ``okg.claim_projection.constraint_kind`` through the same generation
    CHECKOUT (an unpinned SELECT returns no rows on the chronos-versioned table),
    keyed by the finding row_key that is the FindingView.finding_id.
    """
    import psycopg

    from okg.substrate.graph_store import ChronosGraphStore

    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkout = store.checkout_generation(generation_id)
        rows = checkout.query(
            "SELECT row_key, constraint_kind FROM okg.claim_projection "
            "WHERE row_kind = :kind ORDER BY row_key",
            {"kind": "finding"},
        )
    return {str(row["row_key"]): str(row["constraint_kind"]) for row in rows}


def detection_state(dsn: str, generation_id: Any) -> Any:
    """PipelineState at a pinned generation (reuses the #754 loader)."""
    import psycopg

    from okg.bench.injection import load_pipeline_state
    from okg.substrate.graph_store import ChronosGraphStore

    routed = routed_buckets(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkout = store.checkout_generation(generation_id)
        return load_pipeline_state(checkout, routed_buckets=routed)


# -- post-render layer (the #754 continuity proof) --------------------------


def post_render_apply(
    corrupted_dir: Path, deployment_dir: Path, seed: int, out_dir: Path
) -> Any:
    """Route one post-render case through the UNCHANGED #754 plan/apply
    contract: build the claim snapshot from the rendered corpus with the
    same grammar the fixture ingests with, run the deployment's tuned
    operator registry, and metamorphically verify apply_inverses restores
    byte identity."""
    from okg.bench.injection import (
        CorpusSnapshot,
        RunConfig,
        run_injection,
    )
    from okg.bench.injection.claim_corpus_source import (
        parse_document_claims,
    )
    from okg.bench.injection.injector import apply_inverses
    from okg.bench.injection.operators.registry import (
        load_operators_yaml,
    )
    from okg.bench.injection.snapshot import ClaimSnapshot
    from okg.bench.injection.types import ClaimRow

    corpus = CorpusSnapshot.load(corrupted_dir, suffixes=(".md",))
    rows = []
    for doc, _digest in corpus.files:
        text = (Path(corrupted_dir) / doc).read_text(encoding="utf-8")
        for fact in parse_document_claims(doc, text):
            attrs = fact.attrs
            rows.append(
                ClaimRow(
                    claim_id=fact.node_id,
                    predicate=attrs["predicate"],
                    normalized_key=attrs["normalized_key"],
                    value=attrs["value"],
                    scope=attrs["scope"],
                    scope_state=attrs["scope_state"],
                    source_doc=doc,
                )
            )
    snapshot = ClaimSnapshot.from_rows("post-render", rows)
    operators = load_operators_yaml(Path(deployment_dir) / "operators.yaml")
    result = run_injection(
        corpus=corpus,
        claims=snapshot,
        operators=operators,
        config=RunConfig(seed=seed, out_dir=Path(out_dir), b0_claim_ids=frozenset()),
    )
    by_doc: dict[str, list[Any]] = {}
    for record in result.mutations:
        by_doc.setdefault(record.doc, []).append(record)
    for doc, records in sorted(by_doc.items()):
        mutated_text = (Path(result.mutated_corpus_dir) / doc).read_text(
            encoding="utf-8"
        )
        original = (Path(corrupted_dir) / doc).read_text(encoding="utf-8")
        if apply_inverses(mutated_text, records) != original:
            raise RunnerError(f"post-render inverse failed to restore {doc}")
    return result


# -- the synth-native scorecard builder ------------------------------------


def synth_value_equivalent(a: str, b: str) -> bool:
    """The DECLARED value equivalence for benchmark scoring.

    Declared, not implicit: a repair counts as correct only under a rule stated
    in one place, so "was this repair right?" cannot drift with an incidental
    formatting change. Case and whitespace fold; nothing else. Deliberately the
    same rule the falsification witness declares.
    """
    return " ".join(str(a).lower().split()) == " ".join(str(b).lower().split())


def synth_survival_equivalent(a: str, b: str) -> bool:
    """The DECLARED equivalence for injection survival and laundering.

    canonicalize_value composed with synth_value_equivalent — the SAME reading
    the repair adjudication uses, only made explicit about units. Claims are unit-
    canonicalized at ingest, so composing canonicalize here (idempotent on an
    already-canonical claim value) is what keeps "77.20 kg" vs "77.2kg" formatting
    drift from being mis-scored as the extractor laundering the injection, which
    is a different defect from a silent repair and must not be conflated with it.
    """
    return synth_value_equivalent(canonicalize_value(a), canonicalize_value(b))


def _survival_key(value: str) -> str:
    """The canonical key form of the DECLARED survival equivalence.

    Two values are survival-equivalent (canonicalize_value composed with
    synth_value_equivalent) IFF their survival keys are equal, so a membership
    test on these keys is the same relation the funnel judges laundering with —
    not a divergent copy of it (the failure mode the code comments warn about).
    Using the key lets the FP split join claims against trace surfaces by set
    membership instead of an O(n^2) equivalence scan.
    """
    return " ".join(canonicalize_value(str(value)).lower().split())


def _attribute_unexpected_findings(
    manifest: Mapping[str, Any],
    corrupted_claims: Mapping[str, str],
    unexpected_buckets: Sequence[str],
) -> dict[str, list[str]]:
    """Split unexpected-finding buckets into extractor- vs engine-attributed.

    A finding is EXTRACTOR-attributed when its bucket holds at least one claim
    that matches NO trace row — a claim with no ``(doc, predicate, entity)``
    trace row of its own, or one whose value diverges from that row's
    corrupted-corpus surface under the DECLARED survival equivalence
    (hallucinated, mis-keyed, wrong-occurrence, misread). It is
    engine-attributed only when EVERY claim in the bucket matches its trace row.
    The predicate is TOTAL over claims, so a mis-keyed claim with no trace row
    attributes its bucket to the extractor rather than debiting the engine
    (req.attrib.survival) — the pooling that ``corrupted_unexpected_findings``
    used to do would have charged an extractor defect to the engine.
    """
    trace = manifest["trace"]
    patients = manifest["patients"]
    # (doc, predicate, entity) -> survival-keys of the CORRUPTED-corpus surfaces
    # that trace row rendered (render.py stamps surface_value with the corrupted
    # surface), which is exactly what a faithful extraction should carry.
    trace_keys: dict[tuple[str, str, str], set[str]] = {}
    for row in trace:
        predicate = _slug(row["predicate_label"])
        entity = patients[row["fact_id"].split(":")[1]]["entity"]
        trace_keys.setdefault((row["doc"], predicate, entity), set()).add(
            _survival_key(row["surface_value"])
        )

    claims_by_bucket: dict[str, list[tuple[str, str, str, str]]] = {}
    for claim_id, value in corrupted_claims.items():
        parts = claim_id.split(":")
        if len(parts) != 5 or parts[0] != "claim":
            continue
        _c, doc, predicate, entity, _occ = parts
        claims_by_bucket.setdefault(f"{predicate}:{entity}", []).append(
            (doc, predicate, entity, value)
        )

    extractor: set[str] = set()
    engine: set[str] = set()
    for bucket in unexpected_buckets:
        claims = claims_by_bucket.get(bucket, [])
        invented = any(
            _survival_key(value)
            not in trace_keys.get((doc, predicate, entity), frozenset())
            for doc, predicate, entity, value in claims
        )
        (extractor if invented else engine).add(bucket)
    return {
        "extractor_attributed": sorted(extractor),
        "engine_attributed": sorted(engine),
    }


# The number of PUBLISHED generations one scorecard scores. Both lanes publish
# exactly two and the scorecard reads exactly two PipelineStates — the baseline
# (``clean_state``: the clean corpus in the benchmark lane, the uncorrected
# generation A in the corrections lane) and the one under test
# (``corrupted_state``). Tied to the signature rather than written as a bare 2 so
# a third scored generation cannot leave the publish ledger silently stale.
def _publish_cycles(clean_state: Any, corrupted_state: Any) -> int:
    return len((clean_state, corrupted_state))


def _documents_delivering_claims(state: Any) -> set[str]:
    """The distinct documents whose extraction DELIVERED at least one claim.

    Claim ids are ``claim:{doc}:{predicate}:{entity}:{occurrence}``. In a lossless
    lane this is every parsed document; when a whole document is lost at
    extraction (the LLM lane's document-scope failures) it is strictly fewer, and
    the gap between the ``parse`` and ``extract_document`` action counts IS that
    loss.
    """
    docs: set[str] = set()
    for claim_id in state.claims:
        parts = claim_id.split(":")
        if len(parts) == 5 and parts[0] == "claim":
            docs.add(parts[1])
    return docs


def action_counts(
    *,
    manifest: Mapping[str, Any],
    clean_state: Any,
    corrupted_state: Any,
    removed_corruptions: frozenset[str] = frozenset(),
    corrections_applied: int | None = None,
) -> dict[str, int]:
    """How many of each priced ACTION class this run executed.

    Derived from what ``build_synth_scorecard`` already receives, so the live
    scorecard and a re-score from artifacts cannot disagree about the bill:

    * ``parse`` — one per document PARSED, over both published phases. The two
      corpora carry the same document set (a corruption rewrites values, it never
      adds or drops a file), so this is documents x phases in both lanes.
    * ``extract_document`` — one per document extraction that DELIVERED claims.
      Equal to ``parse`` in a lossless lane; strictly fewer when a document is
      lost at extraction.
    * ``check`` — one per candidate bucket the consistency checker evaluated,
      over both phases. The candidate sets are the checker's work surface.
    * ``correction`` — one per operator correction applied. ``corrections_applied``
      is the plan's full count when a corrections lane ran (a retract, an override
      AND an assert all cost the operator); it falls back to the number of
      corruptions removed from scope, which is what a scorecard built without a
      corrections plan can honestly see.
    * ``publish`` — one per published generation this scorecard scores.
    """
    parse = len(manifest["clean_corpus"]) + len(manifest["corrupted_corpus"])
    extract = len(_documents_delivering_claims(clean_state)) + len(
        _documents_delivering_claims(corrupted_state)
    )
    checks = len(clean_state.candidates) + len(corrupted_state.candidates)
    corrections = (
        len(removed_corruptions)
        if corrections_applied is None
        else int(corrections_applied)
    )
    return {
        ACTION_PARSE: parse,
        ACTION_EXTRACT_DOCUMENT: extract,
        ACTION_CHECK: checks,
        ACTION_CORRECTION: corrections,
        ACTION_PUBLISH: _publish_cycles(clean_state, corrupted_state),
    }



def assert_rows_rederive_counts(
    rows: Sequence[Mapping[str, Any]],
    counts: Mapping[str, Any],
) -> None:
    """The per-error rows and every published funnel count are one object.

    concord-failure-taxonomy, req.tax.per-error-record. A stored run whose
    counts disagree with its own rows is refused rather than reported. This
    covers the total, terminal-state tally, partition cross-tab, and both the
    conditional and end-to-end detection and repair counts. Pure, so the
    refusal is witnessable without a database.
    """
    states = counts["states"]
    rederived = {state: 0 for state in states}
    partition_state: dict[str, dict[str, int]] = {}
    for row in rows:
        state = row["state"]
        if state not in rederived:
            raise RunnerError(
                f"a per-error row carries the unknown terminal state {state!r}; "
                f"the funnel declares {sorted(states)}"
            )
        rederived[state] += 1
        partition = str(row["partition"])
        cell = partition_state.setdefault(partition, {})
        cell[state] = cell.get(state, 0) + 1
    partition_state = {
        partition: {
            state: partition_state[partition][state]
            for state in sorted(partition_state[partition])
        }
        for partition in sorted(partition_state)
    }

    detected_states = {
        STATE_DETECTED_ONLY,
        STATE_MIS_REPAIRED,
        STATE_REPAIRED,
    }
    survived_conflict_states = detected_states | {STATE_NOT_DETECTED}
    conditional_partitions = {PARTITION_CROSS_DOC, PARTITION_INTERNAL_ONLY}
    conditional_denominator = sum(
        1
        for row in rows
        if row["partition"] in conditional_partitions
        and row["state"] in survived_conflict_states
    )
    conditional_detected = sum(
        1
        for row in rows
        if row["partition"] in conditional_partitions
        and row["state"] in detected_states
    )
    end_to_end_detected = sum(
        1 for row in rows if row["state"] in detected_states
    )
    repaired = rederived[STATE_REPAIRED]
    expected = {
        "total": len(rows),
        "states": rederived,
        "partition_state": partition_state,
        "detection_recall": {
            "conditional": {
                "denominator": conditional_denominator,
                "detected": conditional_detected,
            },
            "end_to_end": {
                "denominator": len(rows),
                "detected": end_to_end_detected,
            },
        },
        "repair_rate": {
            "conditional": {
                "denominator": end_to_end_detected,
                "repaired": repaired,
            },
            "end_to_end": {
                "denominator": len(rows),
                "repaired": repaired,
            },
        },
    }
    actual = {key: counts[key] for key in expected}
    if actual != expected:
        mismatched = sorted(
            key for key in expected if actual[key] != expected[key]
        )
        raise RunnerError(
            "the funnel's per-error rows do not re-derive its published counts: "
            f"mismatched={mismatched}; rows give {expected}, counts give {actual}"
        )


def build_synth_scorecard(
    *,
    manifest: Mapping[str, Any],
    clean_state: Any,
    corrupted_state: Any,
    routed: frozenset[str],
    config: SynthConfig,
    resolutions: Sequence[Any] = (),
    finding_kinds: Mapping[str, str] | None = None,
    removed_corruptions: frozenset[str] = frozenset(),
    corrections_applied: int | None = None,
    error_cost_scale: float = 1.0,
    rows_out: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Per-stage × per-class × per-partition tallies over construction-time
    gold. Clean-run findings are FPs by definition (never excluded); the
    empty-gold clean run scores fine (unlike the #754 builder).

    ``resolutions`` are the generation's ``bucket_resolution`` rows. They drive
    the repair block, which answers a question detection cannot: not "was the
    corruption noticed" but "did the engine settle on the RIGHT value".
    """
    trace = manifest["trace"]
    patients = manifest["patients"]

    def entity_for(fact_id: str) -> str:
        return patients[fact_id.split(":")[1]]["entity"]

    def predicate_for(fact_id: str, doc: str) -> str:
        for row in trace:
            if row["fact_id"] == fact_id and row["doc"] == doc:
                return _slug(row["predicate_label"])
        raise RunnerError(f"trace has no row for {fact_id} in {doc}")

    # extraction: full population
    extraction = {"denominator": 0, "recovered": 0, "missed": []}
    for row in trace:
        predicate = _slug(row["predicate_label"])
        entity = entity_for(row["fact_id"])
        claim_id = f"claim:{row['doc']}:{predicate}:{entity}:0"
        extraction["denominator"] += 1
        if claim_id in corrupted_state.claims:
            extraction["recovered"] += 1
        else:
            extraction["missed"].append(claim_id)
    extraction["missed"] = sorted(extraction["missed"])[:50]

    # finding rows carry the CONFLICT channels (definite/possible); the
    # "contested" vocabulary belongs to bucket_resolution rows.
    conflict_channels = {"definite", "possible"}
    contested = {
        f.bucket_key
        for f in corrupted_state.findings
        if f.channel in conflict_channels
    }
    candidate_buckets = set()
    for members in corrupted_state.candidates:
        for claim_id in members:
            parts = claim_id.split(":")
            if len(parts) >= 4:
                candidate_buckets.add(f"{parts[2]}:{parts[3]}")

    per_class: dict[str, dict[str, Any]] = {}
    per_partition: dict[str, dict[str, int]] = {}
    gold_buckets = set()
    for site in manifest["gold"]["corruptions"]:
        cls = site["corruption_class"]
        partition = site["partition"]
        predicate = predicate_for(site["fact_id"], site["doc"])
        entity = entity_for(site["fact_id"])
        bucket = f"{predicate}:{entity}"
        gold_buckets.add(bucket)
        found = bucket in contested
        candidate = found or bucket in candidate_buckets
        routed_hit = bucket in routed
        row = per_class.setdefault(
            cls,
            {
                "layer": site["layer"],
                "denominator": 0,
                "candidate": 0,
                "verdict": 0,
                "routed": 0,
                "missed_buckets": [],
            },
        )
        row["denominator"] += 1
        row["candidate"] += int(candidate)
        row["verdict"] += int(found)
        row["routed"] += int(routed_hit)
        if not found:
            row["missed_buckets"].append(bucket)
        part = per_partition.setdefault(
            partition, {"denominator": 0, "verdict": 0}
        )
        part["denominator"] += 1
        part["verdict"] += int(found)

    for row in per_class.values():
        row["missed_buckets"] = sorted(set(row["missed_buckets"]))

    # undetectable gold flagged = FP; world_knowledge scores separately
    undetectable_flagged = per_partition.get(PARTITION_UNDETECTABLE, {}).get(
        "verdict", 0
    )
    cross = per_partition.get(PARTITION_CROSS_DOC, {"denominator": 0, "verdict": 0})

    # twins: must NOT flag
    twin_results = []
    for twin in manifest["gold"]["twins"]:
        fact_id = twin["fact_id"]
        entity = entity_for(fact_id)
        twin_doc_rows = [r for r in trace if r["fact_id"] == fact_id]
        predicate = _slug(twin_doc_rows[0]["predicate_label"]) if twin_doc_rows else ""
        bucket = f"{predicate}:{entity}"
        twin_results.append(
            {"twin_id": twin["twin_id"], "bucket": bucket, "flagged": bucket in contested}
        )
    twins_flagged = sum(1 for t in twin_results if t["flagged"])

    # clean-run FPs use the SAME channel filter as verdict matching
    # (symmetry, review finding L8), and flagged twins are twin FPs, never
    # double-counted into unexpected findings.
    clean_fp = sum(1 for f in clean_state.findings if f.channel in conflict_channels)
    twin_buckets = {t["bucket"] for t in twin_results}
    unexpected = sorted(
        f.bucket_key
        for f in corrupted_state.findings
        if f.channel in conflict_channels
        and f.bucket_key not in gold_buckets
        and f.bucket_key not in twin_buckets
    )

    # The POISONED corruptions (synth-authority-poisoning): kept OUT of the
    # unpoisoned repair tally, the majority-flavor denominators, and (in the
    # capstone) axis h, so an adversarial wrong-authority flip never contaminates
    # the organic mis-repair reading. Empty for every non-poison run.
    poisoned_corruptions = poisoned_corruptions_of(manifest)

    # REPAIR: detection says a conflict was noticed; this says the engine
    # settled on the right value. Delegates to resolution.adjudicate() rather
    # than restating the state machine here — a second implementation would be
    # a divergence surface between the scorecard and the witnesses.
    repair = adjudicate(
        corruption_buckets=corruption_bucket_map(manifest),
        # pre_text is the REALIZED clean text at the corrupted span (the
        # planner's clean_value before rendering resolved it), so it is what
        # the engine could actually have recovered. Comparing against the
        # planner's pre-render value would mis-score any fact whose rendering
        # reformats it.
        clean_values={
            site["corruption_id"]: site["pre_text"]
            for site in manifest["gold"]["corruptions"]
        },
        resolutions=tuple(resolutions),
        detected_buckets=frozenset(contested),
        equivalent=synth_value_equivalent,
        exclude=poisoned_corruptions,
    )
    if sum(repair["tally"].values()) != repair["total"]:
        raise RunnerError(
            "repair states are not exhaustive over the corruption set: "
            f"{repair['tally']} sums to {sum(repair['tally'].values())}, "
            f"expected {repair['total']}"
        )

    # THE FUNNEL: every corruption gets exactly one terminal state attributed to
    # the stage that lost it. Delegates to resolution.funnel() rather than
    # restating the state machine, and is judged under the DECLARED survival
    # equivalence (canonicalize ∘ synth_value_equivalent) so laundering is not
    # confused with formatting drift. Extraction survival is derived from the
    # trace joined against the claims that landed — never assumed lossless.
    stages = funnel(
        manifest=manifest,
        extracted_claims=corrupted_state.claims,
        resolutions=tuple(resolutions),
        findings=corrupted_state.findings,
        equivalent=synth_survival_equivalent,
        removed_corruptions=removed_corruptions,
    )
    if sum(stages["states"].values()) != stages["total"]:
        raise RunnerError(
            "funnel states are not exhaustive over the corruption set: "
            f"{stages['states']} sums to {sum(stages['states'].values())}, "
            f"expected {stages['total']}"
        )

    # THE PER-ERROR ROWS (concord-failure-taxonomy, req.tax.per-error-record).
    # The funnel already computes one row per injected error; the scorecard used
    # to keep only the counts derived from them, which meant a STORED run could
    # not be re-analysed — any cross-tab the scorecard had not anticipated
    # required re-running the whole benchmark. The rows carry no new
    # measurement. They are enriched here with the two gold-side fields a
    # failure analysis needs and the row itself does not carry — the injection
    # LAYER (was the error written into the document text or into the fact
    # before rendering) and the Σ CONSTRAINT KIND its corruption trips — both
    # read from the sealed manifest, never recomputed.
    gold_by_id = {
        str(site["corruption_id"]): site
        for site in manifest["gold"]["corruptions"]
    }
    funnel_rows: list[dict[str, Any]] = []
    for row in stages["rows"]:
        site = gold_by_id.get(str(row["corruption_id"]))
        if site is None:  # pragma: no cover - guarded by the funnel itself
            raise RunnerError(
                "the funnel graded a corruption the sealed manifest does not "
                f"carry: {row['corruption_id']}"
            )
        funnel_rows.append({
            **row,
            "layer": site.get("layer"),
            "constraint_kind": gold_constraint_kind(site),
            "mandatory": bool(site.get("mandatory", False)),
        })

    # The counts and the rows are ONE object seen two ways, so a stored run
    # whose counts disagree with its own rows is refused rather than reported:
    # a consumer that trusted the counts and a consumer that re-derived them
    # from the rows would otherwise publish different numbers from one run.
    assert_rows_rederive_counts(funnel_rows, stages)
    # The rows travel BESIDE the scorecard, never inside it. The scorecard is
    # the run's published numbers and is pinned byte-for-byte by goldens; one
    # row per injected error is the per-error record those numbers are derived
    # from, and folding it in would rewrite every golden and make each of them
    # carry hundreds of lines of per-error data. The counts and the rows stay
    # ONE object seen two ways through the re-derivation asserted just above.
    if rows_out is not None:
        rows_out.extend(funnel_rows)

    # THE ECONOMICS (synth-cost-weighted-gold). The tables come from the SEALED
    # manifest, never from ``config``: a re-score carries its own config object,
    # and pricing from there would let a re-score silently change a recorded run's
    # economics. Qualification is derived from the same funnel every count block
    # reads, so the dollars and the counts can never tell different stories.
    sealed_costs = sealed_cost_model(manifest)
    remaining = remaining_error_cost(manifest, stages, scale=error_cost_scale)
    spent = spent_action_cost(
        sealed_costs["action_prices"],
        action_counts(
            manifest=manifest,
            clean_state=clean_state,
            corrupted_state=corrupted_state,
            removed_corruptions=removed_corruptions,
            corrections_applied=corrections_applied,
        ),
    )
    qual = qualification(manifest, remaining)

    core = {
        "schema": "okg.synth.scorecard.v1",
        "config_digest": manifest["config_digest"],
        "manifest_digest": manifest["manifest_digest"],
        # QUALIFICATION as a LOUD top-level field, not a footnote: a result that
        # leaves a mandatory violation standing does not qualify, whatever its
        # recall looks like. The adjacent block carries the audit trail.
        "qualified": qual["qualified"],
        "qualification": qual,
        # Remaining error cost, split by the funnel stage that failed the
        # corruption; the stage sums reconstruct the total FAIL-CLOSED.
        "remaining_error_cost": remaining,
        # The price-weighted ledger of the actions this run executed.
        "spent_action_cost": spent,
        # The SEALED tables, surfaced as scorecard provenance so a reader can
        # audit the economics without opening the manifest.
        "cost_model": sealed_costs,
        "extraction": extraction,
        # per_corruption is intentionally omitted from the golden-pinned core:
        # it is per-run detail, while the tally is the comparable measurement.
        "repair": {
            "total": repair["total"],
            "detected": repair["detected"],
            **repair["tally"],
        },
        # Additive block (schema stays okg.synth.scorecard.v1, per the repair
        # precedent). The per-corruption rows are per-run detail and stay out of
        # the golden-pinned core, exactly as per_corruption does; the aggregate
        # tally, cross-tab, and both-conditioning rates are the comparable surface.
        "funnel": {
            "total": stages["total"],
            "states": stages["states"],
            "partition_state": stages["partition_state"],
            "detection_recall": stages["detection_recall"],
            "repair_rate": stages["repair_rate"],
        },
        # INJECTION SURVIVAL per class (req.attrib.survival), a first-class
        # metric: how many of each class's sites carried the corrupted value
        # through extraction into the gold bucket under the DECLARED equivalence.
        # Derived from the funnel's rows (survived = complement of the two
        # extraction-loss states), never a parallel join. The deterministic lane
        # is lossless, so the golden pins survived == sites for every class.
        "per_class_survival": stages["per_class_survival"],
        # PER-KIND detection recall (req.honesty.*): every one of the four Σ
        # constraint kinds gets a row (both denominators 0 when the corpus
        # declares no corruption of it), DERIVED FROM the funnel's per-corruption
        # states, so per-kind and the aggregate funnel are the one detection
        # predicate sliced by kind — never pooled, and they can never disagree.
        # Both denominators appear: end_to_end (all gold sites of the kind) and
        # conditional (only sites whose conflict survived extraction), so the
        # conditional misses are strictly detection losses. The linked_pair
        # cross-bucket witness rides along, from the finding rows + finding_kinds.
        "per_kind_detection": per_kind_detection(
            manifest, stages, corrupted_state.findings, finding_kinds or {}
        ),
        # PER-FLAVOR majority mis-repair (req.prov.flavors): mis_repaired over
        # PRESERVED majorities, split by the sealed provenance flavor (copy vs
        # independent), each against its own preserved denominator. DERIVED from
        # the funnel's per-corruption rows joined to provenance.flavor_by_corruption
        # — never a re-derivation of the ranking logic. Both flavors read zero on a
        # non-provenance run (empty flavor plan), and on today's engine BOTH mis-
        # repair every preserved site; the independence-weighting fix must split
        # them (copy → 0, independent unchanged), which the gradability xfail grades.
        # Poisoned buckets are excluded (synth-authority-poisoning) so an adversarial
        # authority flip never contaminates the majority-flavor reading.
        "majority_flavor": per_flavor_mis_repair(
            manifest, stages, poisoned_corruptions=poisoned_corruptions
        ),
        # THE POISONING FLIP BLOCK (synth-authority-poisoning): flip rate over the
        # sealed poisoned buckets and the by-basis attribution. Zero-poisoned on
        # every non-poison run (an always-present block reading flip_rate 0), so it
        # is a new key, not a moved measurement — exactly as majority_flavor was.
        "poisoning": poison_flip_report(manifest, tuple(resolutions), stages),
        "per_class": {k: per_class[k] for k in sorted(per_class)},
        "per_partition": {k: per_partition[k] for k in sorted(per_partition)},
        "cross_doc_recall": {
            "denominator": cross["denominator"],
            "recovered": cross["verdict"],
        },
        "world_knowledge": per_partition.get(
            PARTITION_WORLD_KNOWLEDGE, {"denominator": 0, "verdict": 0}
        ),
        "undetectable_flagged_fp": undetectable_flagged,
        "twins": {"denominator": len(twin_results), "flagged": twins_flagged},
        "clean_run_fp": clean_fp,
        "clean_fp_ceiling": config.clean_fp_ceiling,
        "clean_gate_passed": clean_fp <= config.clean_fp_ceiling,
        # Unexpected findings are ATTRIBUTED to their stage (req.attrib.survival):
        # a bucket carrying a claim with no matching trace row is an EXTRACTOR
        # defect (hallucinated / mis-keyed), not engine noise. The flat list this
        # replaces pooled both and debited the engine for extractor errors.
        "corrupted_unexpected_findings": _attribute_unexpected_findings(
            manifest, corrupted_state.claims, unexpected
        ),
    }
    return core


def render_synth_scorecard(core: Mapping[str, Any]) -> str:
    """Human-readable scorecard (CLI output)."""
    funnel_block = core["funnel"]
    cond = funnel_block["detection_recall"]["conditional"]
    e2e = funnel_block["detection_recall"]["end_to_end"]
    rep_cond = funnel_block["repair_rate"]["conditional"]
    lines = [
        "synthetic-corpus benchmark scorecard",
        f"  config_digest   {core['config_digest'][:16]}",
        # Qualification and the dollar ledger come FIRST: an unqualified result
        # must not be readable as a good one because its recall line looked fine.
        *render_cost_blocks(core),
        f"  extraction      {core['extraction']['recovered']}/{core['extraction']['denominator']}",
        f"  cross-doc       {core['cross_doc_recall']['recovered']}/{core['cross_doc_recall']['denominator']}",
        f"  world-knowledge {core['world_knowledge']['verdict']}/{core['world_knowledge']['denominator']} (own denominator)",
        f"  twins flagged   {core['twins']['flagged']}/{core['twins']['denominator']} (must be 0)",
        f"  clean-run FP    {core['clean_run_fp']} (ceiling {core['clean_fp_ceiling']}, "
        f"{'PASS' if core['clean_gate_passed'] else 'FAIL'})",
        "  funnel:",
        f"    surviving conf  {cond['denominator']} "
        "(cross_doc+internal_only survivors = the floor's realized count)",
        f"    detection       conditional {cond['detected']}/{cond['denominator']}"
        f"  end-to-end {e2e['detected']}/{e2e['denominator']}",
        f"    repair          conditional {rep_cond['repaired']}/{rep_cond['denominator']}"
        f"  end-to-end {funnel_block['repair_rate']['end_to_end']['repaired']}"
        f"/{funnel_block['repair_rate']['end_to_end']['denominator']}",
        f"    states          {funnel_block['states']}",
    ]
    survival = core.get("per_class_survival", {})
    by_class = survival.get("by_class", {})
    survived = sum(c["survived"] for c in by_class.values())
    sites = sum(c["sites"] for c in by_class.values())
    fp = core["corrupted_unexpected_findings"]
    lines.extend(
        [
            "  injection survival:",
            f"    overall         {survived}/{sites}",
        ]
    )
    for cls, cell in by_class.items():
        lines.append(f"    {cls:24s} {cell['survived']}/{cell['sites']}")
    if survival.get("non_surviving_sites"):
        lines.append(
            f"    non-surviving   {', '.join(survival['non_surviving_sites'])}"
        )
    lines.append(
        f"  unexpected FPs  extractor {len(fp['extractor_attributed'])}"
        f"  engine {len(fp['engine_attributed'])}"
    )
    lines.append("  per class:")
    for cls, row in core["per_class"].items():
        lines.append(
            f"    {cls:24s} {row['layer']:11s} verdict {row['verdict']}/{row['denominator']}"
            f" routed {row['routed']}/{row['denominator']}"
        )
    return "\n".join(lines) + "\n"


# -- artifact recording + re-score (req.attrib.llm-lane) -------------------


def _serialize_state(state: Any) -> dict[str, Any]:
    """The scorer-relevant surface of one PipelineState, snapshot to JSON.

    Only the fields build_synth_scorecard reads (claims, candidate member sets,
    and each finding's channel + bucket_key) — enough for re-score to rebuild
    every block, so the artifact is stable and never re-reads a live table."""
    return {
        "claims": dict(state.claims),
        "candidates": sorted(sorted(members) for members in state.candidates),
        "findings": [
            {
                "finding_id": finding.finding_id,
                "channel": finding.channel,
                "bucket_key": finding.bucket_key,
                # claim_ids ride along so the linked_pair cross-bucket witness
                # (per_kind_detection) rebuilds byte-identically from the artifact.
                "claim_ids": sorted(finding.claim_ids),
            }
            for finding in state.findings
        ],
    }


def _deserialize_state(payload: Mapping[str, Any]) -> Any:
    from okg.bench.injection.scoring import FindingView, PipelineState

    findings = tuple(
        FindingView(
            finding_id=row["finding_id"],
            channel=row["channel"],
            bucket_key=row["bucket_key"],
            claim_ids=frozenset(row.get("claim_ids", ())),
        )
        for row in payload["findings"]
    )
    candidates = frozenset(frozenset(members) for members in payload["candidates"])
    return PipelineState(
        claims=dict(payload["claims"]), candidates=candidates, findings=findings
    )


def _serialize_resolutions(resolutions: Sequence[Any]) -> list[dict[str, Any]]:
    return [
        {
            "predicate": res.predicate,
            "bucket_key": res.bucket_key,
            "outcome": res.outcome,
            "basis": res.basis,
            "winner_claim_id": res.winner_claim_id,
            "ranked_claim_ids": list(res.ranked_claim_ids),
            "winner_value": res.winner_value,
        }
        for res in resolutions
    ]


def _deserialize_resolutions(rows: Sequence[Mapping[str, Any]]) -> tuple[Any, ...]:
    return tuple(
        BucketResolution(
            predicate=row["predicate"],
            bucket_key=row["bucket_key"],
            outcome=row["outcome"],
            basis=row["basis"],
            winner_claim_id=row["winner_claim_id"],
            ranked_claim_ids=tuple(row["ranked_claim_ids"]),
            winner_value=row["winner_value"],
        )
        for row in rows
    )


def _write_engine_reads(
    out_dir: Path,
    *,
    clean_state: Any,
    corrupted_state: Any,
    routed: frozenset[str],
    resolutions: Sequence[Any],
    finding_kinds: Mapping[str, str] | None = None,
    removed_corruptions: frozenset[str] = frozenset(),
    corrections_applied: int | None = None,
) -> None:
    """Snapshot the PINNED engine-side reads at scoring time.

    The routed-bucket set in particular is snapshotted here rather than left to
    ``routed_buckets()``, which reads ``okg.observation_current`` UNPINNED — so
    the recorded artifact is stable and re-score never touches a live table. The
    per-finding constraint_kind map rides along so re-score reproduces the
    per-kind detection block without a second table read."""
    payload = {
        "schema": "okg.synth.engine-reads.v1",
        "clean_state": _serialize_state(clean_state),
        "corrupted_state": _serialize_state(corrupted_state),
        "routed_buckets": sorted(routed),
        "resolutions": _serialize_resolutions(resolutions),
        "finding_kinds": dict(sorted((finding_kinds or {}).items())),
        # The OPERATOR-side inputs to scoring. Without them a re-score of a
        # corrections run would rebuild the funnel with removed_from_scope = 0 and
        # silently debit the engine for corruptions an operator had removed, and
        # would bill zero corrections. Recorded so the artifact-only re-score
        # reproduces the run's scorecard on BOTH lanes, not just the base one.
        "removed_corruptions": sorted(removed_corruptions),
        "corrections_applied": corrections_applied,
    }
    (out_dir / ENGINE_READS_FILENAME).write_text(
        json.dumps(payload, sort_keys=True, indent=1) + "\n", encoding="utf-8"
    )


def _write_llm_artifacts(out_dir: Path, lane: "LlmLane") -> None:
    """Extraction-side record: verbatim responses, the claim snapshot, the
    named failures, and the cost totals.

    The extraction-failures ledger carries BOTH blast radii, each row tagged
    with ``scope``: a ``"document"`` row for a document lost after retries and a
    ``"fact"`` row for each quote dropped because it could not be scoped, so the
    concentrated-loss diagnosis (which patients lost which single fact) stays
    possible alongside the whole-document failures."""
    for phase, rows in sorted(lane.responses.items()):
        lines = [json.dumps(row, sort_keys=True) for row in rows]
        (out_dir / _responses_filename(phase)).write_text(
            "\n".join(lines) + ("\n" if lines else ""), encoding="utf-8"
        )
    for phase, snapshot in sorted(lane.claim_snapshots.items()):
        (out_dir / _claims_filename(phase)).write_text(
            json.dumps(snapshot, sort_keys=True, indent=1) + "\n", encoding="utf-8"
        )
    (out_dir / EXTRACTION_FAILURES_FILENAME).write_text(
        json.dumps(
            {"schema": "okg.synth.extraction-failures.v1", "failures": lane.all_failures()},
            sort_keys=True,
            indent=1,
        )
        + "\n",
        encoding="utf-8",
    )
    # Per-phase cost is cumulative; the total is the last phase, and each phase's
    # own share is its delta from the prior — reported so a scaled run is costed
    # from the artifact, never guessed.
    total = lane.extractor.cost.as_dict()
    (out_dir / EXTRACTION_COST_FILENAME).write_text(
        json.dumps(
            {
                "schema": "okg.synth.extraction-cost.v1",
                "total": total,
                "per_phase_cumulative": {
                    phase: lane.phase_cost[phase] for phase in sorted(lane.phase_cost)
                },
            },
            sort_keys=True,
            indent=1,
        )
        + "\n",
        encoding="utf-8",
    )


def rescore_from_artifacts(
    out_dir: Path, *, error_cost_scale: float = 1.0
) -> dict[str, Any]:
    """Rebuild EVERY scorecard block from the recorded artifacts alone.

    Reads the sealed manifest and the pinned engine-side snapshot; calls no
    model and reads no live table. The re-score witness asserts this reproduces
    the run's scorecard byte-for-byte (req.attrib.llm-lane).

    ``error_cost_scale`` RE-PRICES the recorded run: the sensitivity protocol's
    0.1x / 10x points are this function over the same artifacts, because weights
    affect SCORING only and never execution. The default 1.0 is the run's own
    declared prices, so the byte-identity witness is unaffected."""
    out_dir = Path(out_dir)
    manifest = load_manifest(out_dir / MANIFEST_FILENAME)
    reads = json.loads((out_dir / ENGINE_READS_FILENAME).read_text(encoding="utf-8"))
    config = load_config(manifest["config"])
    return build_synth_scorecard(
        manifest=manifest,
        clean_state=_deserialize_state(reads["clean_state"]),
        corrupted_state=_deserialize_state(reads["corrupted_state"]),
        routed=frozenset(reads["routed_buckets"]),
        config=config,
        resolutions=_deserialize_resolutions(reads["resolutions"]),
        finding_kinds=reads.get("finding_kinds", {}),
        removed_corruptions=frozenset(reads.get("removed_corruptions", ())),
        corrections_applied=reads.get("corrections_applied"),
        error_cost_scale=error_cost_scale,
    )


# The paper's cost-sensitivity protocol: the declared error costs, an order of
# magnitude down, and an order of magnitude up.
SENSITIVITY_SCALES: tuple[float, ...] = (0.1, 1.0, 10.0)
SENSITIVITY_SCHEMA = "okg.synth.cost-sensitivity.v1"
SENSITIVITY_FILENAME = "synth-cost-sensitivity.json"


def sensitivity_from_artifacts(
    out_dir: Path, scales: Sequence[float] = SENSITIVITY_SCALES
) -> dict[str, Any]:
    """Re-price a RECORDED run at each scale, from artifacts alone.

    The protocol is free because weights affect SCORING only, never execution:
    each price point is one :func:`rescore_from_artifacts` call over the same
    recorded manifest and engine snapshot — no model call, no live table, no
    second publish. Fail-closed on the two invariants that make the sweep a
    re-price rather than a re-run (see
    :func:`okg.bench.synth.cost.assert_sensitivity_invariants`).
    """
    out_dir = Path(out_dir)
    points: list[dict[str, Any]] = []
    digest = manifest_digest = ""
    for scale in scales:
        core = rescore_from_artifacts(out_dir, error_cost_scale=float(scale))
        digest = core["config_digest"]
        manifest_digest = core["manifest_digest"]
        points.append(
            {
                "scale": float(scale),
                "qualified": core["qualified"],
                "qualification": core["qualification"],
                "remaining_error_cost": core["remaining_error_cost"],
                "spent_action_cost": core["spent_action_cost"],
            }
        )
    assert_sensitivity_invariants(points)
    return {
        "schema": SENSITIVITY_SCHEMA,
        "config_digest": digest,
        "manifest_digest": manifest_digest,
        "qualified": points[0]["qualified"],
        "points": points,
    }


def write_sensitivity(report: Mapping[str, Any], out_dir: Path) -> Path:
    path = Path(out_dir) / SENSITIVITY_FILENAME
    path.write_text(
        json.dumps(report, sort_keys=True, indent=1) + "\n", encoding="utf-8"
    )
    return path


def report_from_artifacts(out_dir: Path) -> str:
    """Rescore a recorded artifact dir and render the survival + FP + funnel report.

    A REPORT, not a byte-identity check: it rebuilds the scorecard from the
    recorded artifacts alone (rescore_from_artifacts — no model call, no live
    table read) and formats the per-class injection survival block, the
    extractor-vs-engine false-positive split, and the terminal-state funnel for
    an operator. Works against ANY completed artifact dir, including the live
    prose run's, whatever schema its own scorecard was written at.
    """
    core = rescore_from_artifacts(out_dir)
    survival = core["per_class_survival"]
    fp = core["corrupted_unexpected_findings"]
    funnel_block = core["funnel"]
    lines = [
        f"synth rescore report  ({Path(out_dir)})",
        "  injection survival (per class: survived/sites):",
    ]
    for cls, cell in survival["by_class"].items():
        lines.append(f"    {cls:24s} {cell['survived']}/{cell['sites']}")
    lines.append(
        "    non-surviving sites: "
        + (", ".join(survival["non_surviving_sites"]) or "(none)")
    )
    lines.append("  unexpected findings (false positives), attributed by stage:")
    lines.append(
        "    extractor: "
        + (", ".join(fp["extractor_attributed"]) or "(none)")
    )
    lines.append(
        "    engine:    " + (", ".join(fp["engine_attributed"]) or "(none)")
    )
    lines.append(f"  funnel states: {funnel_block['states']}")
    lines.append(f"  partition x state: {funnel_block['partition_state']}")
    dr = funnel_block["detection_recall"]
    rr = funnel_block["repair_rate"]
    lines.append(
        f"  detection recall: conditional "
        f"{dr['conditional']['detected']}/{dr['conditional']['denominator']}"
        f"  end-to-end {dr['end_to_end']['detected']}/{dr['end_to_end']['denominator']}"
    )
    lines.append(
        f"  repair rate:      conditional "
        f"{rr['conditional']['repaired']}/{rr['conditional']['denominator']}"
        f"  end-to-end {rr['end_to_end']['repaired']}/{rr['end_to_end']['denominator']}"
    )
    return "\n".join(lines) + "\n"


# -- the one-shot loop -----------------------------------------------------


def run_synth_benchmark(
    *,
    dsn: str,
    deployment_dir: Path,
    out_dir: Path,
    preset: str = "smoke",
    config: SynthConfig | None = None,
    world_dir: Path | None = None,
) -> dict[str, Any]:
    """generate → corrupt → render → publish clean → publish corrupted →
    detect → score. Returns {"manifest", "scorecard", "clean", "corrupted"}."""
    config = config or load_preset(preset)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    world_dir = Path(world_dir) if world_dir else _packaged_world_dir(config)
    provenance = json.loads((world_dir / "provenance.json").read_text(encoding="utf-8"))
    world = world_from_synthea_dir(
        world_dir,
        reference_date=provenance["reference_date"],
        provenance=provenance,
        max_facts_per_patient=config.scale.max_facts_per_patient,
    )
    world = _subset_world(world, config.scale.patients)

    prf = SynthPrf(config.seed)
    planner = Planner(world, config, prf)
    plan = planner.plan()
    rendered = render_corpus(world, config, planner, plan, prf)
    manifest = build_manifest(config=config, world=world, plan=plan, rendered=rendered)

    leakage_violations = scan_unsanctioned_surface(
        manifest,
        {
            "config": config.canonical_json_bytes().decode("utf-8"),
            "template_pack": pack_bytes().decode("utf-8"),
        },
    )
    if leakage_violations:
        raise RunnerError(f"leakage on unsanctioned surface: {leakage_violations}")

    clean_dir = out_dir / "corpus_clean"
    corrupted_dir = out_dir / "corpus_corrupted"
    _write_docs(clean_dir, rendered.clean_docs)
    _write_docs(corrupted_dir, rendered.corrupted_docs)
    write_manifest(manifest, out_dir / MANIFEST_FILENAME)
    if config.materialize_pdf:
        from okg.bench.synth.pdfgen import materialize_pdfs

        materialize_pdfs(rendered.corrupted_docs, out_dir / "pdf", seed=config.seed)

    # Route on the declared extractor: the LLM lane wraps the same publish path
    # in an LlmLane; the deterministic lane passes lane=None and is untouched.
    lane: LlmLane | None = None
    if config.realism.extractor == "llm":
        lane = LlmLane(extractor=LlmExtractor(labels=_labels_for_world(world)))

    # The coverage Σ policies + the predicates they re-kind out of the functional
    # set (synth-constraint-coverage). Empty for every preset that declares no
    # relational-kind corruption, so the default path is byte-for-byte unchanged.
    extra_policies, excluded = constraint_policy_facts(manifest)
    policies = functional_policies(manifest, exclude=excluded)
    arm_plan = manifest.get("precedence_arm", {}).get("plan")
    arm_admits = tuple(config.precedence_arm.admits)
    provenance_plan = manifest.get("provenance")
    ledger = []
    started = time.monotonic()
    out_clean, clean_ids = ingest_and_publish(
        dsn, clean_dir, deployment_dir, policies, lane=lane, phase="clean",
        arm_plan=arm_plan, admits=arm_admits, provenance_plan=provenance_plan,
        extra_policy_facts=extra_policies,
    )
    ledger.append({"stage": "clean_publish", "latency_ms": (time.monotonic() - started) * 1000})
    clean_state = detection_state(dsn, out_clean["generation_id"])

    started = time.monotonic()
    out_corrupted, _ids = ingest_and_publish(
        dsn, corrupted_dir, deployment_dir, policies, clean_ids,
        lane=lane, phase="corrupted",
        arm_plan=arm_plan, admits=arm_admits, provenance_plan=provenance_plan,
        extra_policy_facts=extra_policies,
    )
    ledger.append({"stage": "corrupted_publish", "latency_ms": (time.monotonic() - started) * 1000})
    corrupted_state = detection_state(dsn, out_corrupted["generation_id"])

    # Snapshot the PINNED engine-side reads at scoring time (routed set +
    # resolutions), so the scorecard and the re-score artifact see the same rows.
    routed_snapshot = routed_buckets(dsn)
    resolutions_snapshot = resolutions_at(
        dsn, out_corrupted["generation_id"], corrupted_state.claims
    )
    finding_kinds_snapshot = finding_kinds(dsn, out_corrupted["generation_id"])
    funnel_rows: list[dict[str, Any]] = []
    core = build_synth_scorecard(
        manifest=manifest,
        clean_state=clean_state,
        corrupted_state=corrupted_state,
        routed=routed_snapshot,
        config=config,
        resolutions=resolutions_snapshot,
        finding_kinds=finding_kinds_snapshot,
        rows_out=funnel_rows,
    )
    realized_surviving = core["funnel"]["detection_recall"]["conditional"]["denominator"]
    (out_dir / SCORECARD_FILENAME).write_text(
        json.dumps(core, sort_keys=True, indent=1) + "\n", encoding="utf-8"
    )
    (out_dir / FUNNEL_ROWS_FILENAME).write_text(
        json.dumps(funnel_rows, sort_keys=True, indent=1) + "\n", encoding="utf-8"
    )
    (out_dir / RUN_BINDING_FILENAME).write_text(
        json.dumps(
            {
                "schema": "okg.synth.run-binding.v1",
                "deployment": SMOKE_DEPLOYMENT,
                "extractor": config.realism.extractor,
                "clean_generation_id": str(out_clean["generation_id"]),
                "corrupted_generation_id": str(out_corrupted["generation_id"]),
                "surviving_conflict_floor": config.surviving_conflict_floor,
                "surviving_conflict_realized": realized_surviving,
                "surviving_conflict_floor_passed": (
                    realized_surviving >= config.surviving_conflict_floor
                ),
                "extraction_failed_documents": lane.all_failures() if lane else [],
                "extraction_cost": lane.extractor.cost.as_dict() if lane else None,
                "ledger": ledger,
            },
            sort_keys=True,
            indent=1,
        )
        + "\n",
        encoding="utf-8",
    )
    # The pinned engine snapshot (both lanes) + the LLM extraction record, all
    # written BEFORE any gate raises so a failed run stays fully inspectable.
    _write_engine_reads(
        out_dir,
        clean_state=clean_state,
        corrupted_state=corrupted_state,
        routed=routed_snapshot,
        resolutions=resolutions_snapshot,
        finding_kinds=finding_kinds_snapshot,
    )
    if lane is not None:
        _write_llm_artifacts(out_dir, lane)
    # The cost-sensitivity sweep, computed from the artifacts just written (no
    # second publish, no model call) and recorded alongside them, so every run
    # carries its 0.1x / 1x / 10x price points.
    write_sensitivity(sensitivity_from_artifacts(out_dir), out_dir)

    # Store the SCORED layer where it outlives this worktree. Off unless
    # OKG_SYNTH_RECORD is set, so ordinary test runs write nothing into the
    # repository; done BEFORE the gates below raise, for the same reason the
    # artifacts themselves are — a run that fails a gate is exactly the run a
    # reader will want to re-score. Imported here rather than at module scope
    # because durable.py reads this module's filename constants.
    from okg.bench.synth.durable import records_root_from_env, store_scored_artifacts

    records_root = records_root_from_env()
    if records_root is not None:
        store_scored_artifacts(out_dir, records_root=records_root)

    # The clean-corpus FP gate is the deterministic lane's historical hard gate
    # (its negative control depends on it). In the LLM lane a clean-run finding
    # also carries extraction noise, so it is a REPORTED metric there and the
    # surviving-conflict floor is the hard gate — never a silent pass either way.
    if not core["clean_gate_passed"] and lane is None:
        raise RunnerError(
            f"clean-corpus FP gate breached: {core['clean_run_fp']} findings "
            f"on the certified-clean corpus (ceiling {core['clean_fp_ceiling']})"
        )
    # The declared surviving-conflict floor (0 = not asserted). Asserted against
    # the REALIZED count AFTER artifacts are written: an under-powered run fails
    # loudly rather than quietly reporting on a tiny denominator.
    if config.surviving_conflict_floor and realized_surviving < config.surviving_conflict_floor:
        raise RunnerError(
            f"surviving-conflict floor breached: realized {realized_surviving} "
            f"surviving conflicts (funnel states not_detected/detected_only/"
            f"mis_repaired/repaired within cross_doc+internal_only) < declared "
            f"floor {config.surviving_conflict_floor}. The run is under-powered; "
            f"artifacts are in {out_dir}."
        )
    # The Σ-coverage gate (req.honesty.*): every REQUIRED constraint kind must
    # have fired at least once, proven from the published finding rows. Loudness
    # is on the END-TO-END denominator — a kind whose end-to-end detected is zero
    # is a LOUD failure, a kind the run was meant to exercise but did not, which a
    # pooled recall number would hide. A conditional zero denominator (extraction
    # lost every site of the kind) is reported in the scorecard, never a run
    # failure by itself. Asserted AFTER artifacts are written so a failed coverage
    # run stays fully inspectable. Default (no required kinds) is a no-op.
    by_kind = core["per_kind_detection"]["by_kind"]
    missing = {
        kind: by_kind.get(kind, {"end_to_end": {"denominator": 0, "detected": 0}})
        for kind in config.require_constraint_kinds
        if by_kind.get(kind, {}).get("end_to_end", {}).get("detected", 0) < 1
    }
    if missing:
        raise RunnerError(
            f"required constraint kind(s) did not fire: {sorted(missing)} "
            f"(per-kind detection {missing}). The corpus was declared to exercise "
            f"these Σ kinds but the published generation carries no detected "
            f"finding of them; artifacts are in {out_dir}."
        )
    return {
        "manifest": manifest,
        "scorecard": core,
        "funnel_rows": funnel_rows,
        "clean": out_clean,
        "corrupted": out_corrupted,
        "world": world,
        "lane": lane,
        "surviving_conflict_realized": realized_surviving,
    }


# -- the corrections observation (req.c.corrections) -----------------------


def corrections_report(
    plan: Any,
    effective_before: Mapping[str, Mapping[str, str]],
    effective_after: Mapping[str, Mapping[str, str]],
) -> dict[str, Any]:
    """The row-level delta on C_eff between the pinned pre-/post-correction
    generations, reported FROM the two claim_effective reads — never from the
    corrections that were emitted. Asserts the effective-claim population
    explicitly (before - retracts + asserts), so a silent claim drop shows as a
    population mismatch rather than passing unnoticed."""
    from okg.bench.synth.corrections import (
        ACTION_ASSERT,
        ACTION_OVERRIDE,
        ACTION_RETRACT,
    )

    retracts = [s for s in plan.specs if s.action == ACTION_RETRACT]
    overrides = [s for s in plan.specs if s.action == ACTION_OVERRIDE]
    asserts = [s for s in plan.specs if s.action == ACTION_ASSERT]

    retracted_absent = sorted(
        s.target_claim_id
        for s in retracts
        if s.target_claim_id in effective_before
        and s.target_claim_id not in effective_after
    )
    overridden = [
        {
            "claim_id": s.target_claim_id,
            "before": effective_before.get(s.target_claim_id, {}).get("value"),
            "after": effective_after.get(s.target_claim_id, {}).get("value"),
            "expected": s.value,
        }
        for s in overrides
    ]
    asserted_present = [
        {
            "claim_id": s.target_claim_id,
            "provenance": effective_after.get(s.target_claim_id, {}).get("provenance"),
        }
        for s in asserts
        if s.target_claim_id not in effective_before
        and s.target_claim_id in effective_after
    ]
    pop_before, pop_after = len(effective_before), len(effective_after)
    expected_after = pop_before - len(retracts) + len(asserts)
    return {
        "schema": "okg.synth.corrections.v1",
        "count_by_action": plan.count_by_action(),
        "population_before": pop_before,
        "population_after": pop_after,
        "population_expected_after": expected_after,
        "population_ok": pop_after == expected_after,
        "retracted_absent": retracted_absent,
        "overridden": overridden,
        "asserted_present": asserted_present,
    }


def run_synth_corrections(
    *,
    dsn: str,
    deployment_dir: Path,
    out_dir: Path,
    preset: str = "corrections",
    config: SynthConfig | None = None,
    world_dir: Path | None = None,
) -> dict[str, Any]:
    """A TWO-generation run that emits retract/override/assert corrections and
    OBSERVES their effect on C_eff (req.c.corrections).

    Generation A publishes the corrupted corpus with no corrections; generation
    B republishes it with the corrections minted at the ingest seam. The
    row-level delta on ``okg.claim_effective`` between the pinned generations is
    the observation; the corrected generation's funnel attributes the retracted
    and overridden corruptions to ``removed_from_scope`` (the reserved state,
    now reachable), never to the engine as a detection miss.
    """
    from okg.bench.synth.corrections import (
        corrections_gold,
        plan_corrections,
    )

    config = config or load_preset(preset)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    world_dir = Path(world_dir) if world_dir else _packaged_world_dir(config)
    provenance = json.loads((world_dir / "provenance.json").read_text(encoding="utf-8"))
    world = world_from_synthea_dir(
        world_dir,
        reference_date=provenance["reference_date"],
        provenance=provenance,
        max_facts_per_patient=config.scale.max_facts_per_patient,
    )
    world = _subset_world(world, config.scale.patients)

    prf = SynthPrf(config.seed)
    planner = Planner(world, config, prf)
    plan = planner.plan()
    rendered = render_corpus(world, config, planner, plan, prf)
    manifest = build_manifest(config=config, world=world, plan=plan, rendered=rendered)

    corrupted_dir = out_dir / "corpus_corrupted"
    _write_docs(corrupted_dir, rendered.corrupted_docs)
    write_manifest(manifest, out_dir / MANIFEST_FILENAME)

    corr_plan = plan_corrections(manifest, SynthPrf(config.seed))
    corr_facts = correction_facts(corr_plan)

    extra_policies, excluded = constraint_policy_facts(manifest)
    policies = functional_policies(manifest, exclude=excluded)

    # Generation A: corrupted corpus, NO corrections.
    out_a, ids_a = ingest_and_publish(
        dsn, corrupted_dir, deployment_dir, policies,
        extra_policy_facts=extra_policies,
    )
    effective_a = effective_claims_at(dsn, out_a["generation_id"])

    # Generation B: the SAME corpus + the corrections.
    out_b, _ids = ingest_and_publish(
        dsn, corrupted_dir, deployment_dir, policies, ids_a,
        extra_policy_facts=list(extra_policies) + corr_facts,
    )
    effective_b = effective_claims_at(dsn, out_b["generation_id"])

    report = corrections_report(corr_plan, effective_a, effective_b)

    # The corrected generation's funnel: the retracted / overridden corruptions
    # land in removed_from_scope, proven from the state tally.
    corrupted_state = detection_state(dsn, out_b["generation_id"])
    state_a = detection_state(dsn, out_a["generation_id"])
    routed_snapshot = routed_buckets(dsn)
    resolutions_snapshot = resolutions_at(
        dsn, out_b["generation_id"], corrupted_state.claims
    )
    finding_kinds_snapshot = finding_kinds(dsn, out_b["generation_id"])
    core = build_synth_scorecard(
        manifest=manifest,
        clean_state=state_a,
        corrupted_state=corrupted_state,
        routed=routed_snapshot,
        config=config,
        resolutions=resolutions_snapshot,
        finding_kinds=finding_kinds_snapshot,
        removed_corruptions=corr_plan.removed_corruption_ids,
        corrections_applied=len(corr_plan.specs),
    )
    report["funnel_states"] = core["funnel"]["states"]

    # The UNCORRECTED generation A, scored in its own right (synth-cost-weighted-
    # gold). Both generations are REAL published runs over the SAME corpus bytes,
    # so the pair is the qualification witness in both directions: a mandatory
    # violation standing at A (qualified=false) and removed from scope at B
    # (qualified=true). Generation A's own resolutions are read at A's pin — a
    # scorecard built from B's rows would be an accounting of the wrong run.
    resolutions_a = resolutions_at(dsn, out_a["generation_id"], state_a.claims)
    core_before = build_synth_scorecard(
        manifest=manifest,
        clean_state=state_a,
        corrupted_state=state_a,
        routed=routed_snapshot,
        config=config,
        resolutions=resolutions_a,
        finding_kinds=finding_kinds(dsn, out_a["generation_id"]),
    )

    (out_dir / CORRECTIONS_FILENAME).write_text(
        json.dumps(
            {
                "schema": "okg.synth.corrections-run.v1",
                "gold": corrections_gold(corr_plan),
                "report": report,
                "generation_before": str(out_a["generation_id"]),
                "generation_after": str(out_b["generation_id"]),
            },
            sort_keys=True,
            indent=1,
        )
        + "\n",
        encoding="utf-8",
    )
    (out_dir / SCORECARD_FILENAME).write_text(
        json.dumps(core, sort_keys=True, indent=1) + "\n", encoding="utf-8"
    )
    # The corrections lane now records the same pinned engine snapshot the base
    # lane does, INCLUDING the operator-side inputs, so its run is re-scorable and
    # its cost sensitivity is computable from artifacts alone.
    _write_engine_reads(
        out_dir,
        clean_state=state_a,
        corrupted_state=corrupted_state,
        routed=routed_snapshot,
        resolutions=resolutions_snapshot,
        finding_kinds=finding_kinds_snapshot,
        removed_corruptions=corr_plan.removed_corruption_ids,
        corrections_applied=len(corr_plan.specs),
    )
    write_sensitivity(sensitivity_from_artifacts(out_dir), out_dir)
    return {
        "manifest": manifest,
        "plan": corr_plan,
        "gold": corrections_gold(corr_plan),
        "report": report,
        "scorecard": core,
        "scorecard_before": core_before,
        "effective_before": effective_a,
        "effective_after": effective_b,
        "before": out_a,
        "after": out_b,
    }


def _write_docs(target: Path, docs: Mapping[str, str]) -> None:
    target.mkdir(parents=True, exist_ok=True)
    for stale in target.glob("*.md"):
        stale.unlink()
    for doc, text in sorted(docs.items()):
        (target / doc).write_text(text, encoding="utf-8")


def _packaged_world_dir(config: SynthConfig) -> Path:
    from importlib import resources

    root = resources.files("okg.bench.synth") / config.world_path
    return Path(str(root))


def _subset_world(world: Any, patients: int) -> Any:
    """Deterministic patient subset — the wired scale.patients axis
    (review finding H2). Fail-closed when the request exceeds the frozen
    world's population; never a silent 'you get what the world has'."""
    from okg.bench.synth.config import ConfigError
    from okg.bench.synth.worldschema import GroundTruthWorld

    if patients > len(world.patients):
        raise ConfigError(
            f"scale.patients={patients} exceeds the frozen world's "
            f"population {len(world.patients)} (inexpressible)"
        )
    if patients == len(world.patients):
        return world
    kept = tuple(sorted(world.patients, key=lambda p: p.patient_id)[:patients])
    kept_ids = {p.patient_id for p in kept}
    return GroundTruthWorld(
        schema_version=world.schema_version,
        provenance=dict(world.provenance),
        reference_date=world.reference_date,
        patients=kept,
        practitioners=world.practitioners,
        facts=tuple(f for f in world.facts if f.patient_id in kept_ids),
    )
