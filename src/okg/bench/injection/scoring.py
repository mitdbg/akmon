"""Stage-attributed scoring of an injection run (design §8).

The scorer consumes the sealed gold (``mutations.jsonl`` records), the clean
and mutated pipeline states, and a per-stage resource ledger, and emits a
two-part scorecard:

- a **deterministic core** — denominators, per-stage / per-mode / per-tier
  rates, per-mutation dispositions, baseline-diff false positives — the
  byte-comparison surface, containing no generation ids, timestamps, or
  latencies;
- a **volatile annex** — the run-binding reference and the resource ledger.

Stage chain with explicit denominators (a retrieval miss is never a
reasoning failure): extraction recovery → candidate recall → verdict
accuracy → lifecycle/routing; each stage's denominator is the previous
stage's survivors. False positives are counted against the clean baseline:
``FP = B1 \\ (B0 ∪ expected)``. Hard negatives score under their
ERA-CONDITIONAL expectation and report per suppression mechanism.

``PipelineState`` is deliberately abstract (claims, proposed candidate
groups, findings): pytest constructs states directly, and the e2e task
binds them to the live engine surfaces.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping, Sequence

from okg.bench.injection.manifest import canonical_json_bytes
from okg.bench.injection.types import (
    InjectionError,
    MutationRecord,
    Span,
)

SCORECARD_SCHEMA = "okg.injection.scorecard.v1"

_VOLATILE_KEY_FRAGMENTS = ("generation", "timestamp", "latency", "created_at")


@dataclass(frozen=True)
class FindingView:
    """One finding as the review surface sees it.

    ``evidence`` spans are **mutated-document coordinates** — the documents
    the pipeline actually ingested. Gold matches them against each record's
    ``inverse_span`` (the mutated-coordinate location of the injected text),
    never the original-corpus ``span``.
    """

    finding_id: str
    channel: str
    claim_ids: frozenset[str] = frozenset()
    bucket_key: str | None = None
    severity: str | None = None
    routed: bool = True
    evidence: tuple[tuple[str, Span], ...] = ()


@dataclass(frozen=True)
class PipelineState:
    """Abstract view of one pipeline run's relevant surfaces."""

    claims: Mapping[str, str]
    candidates: frozenset[frozenset[str]] = frozenset()
    findings: tuple[FindingView, ...] = ()

    def finding_ids(self) -> frozenset[str]:
        return frozenset(finding.finding_id for finding in self.findings)


@dataclass(frozen=True)
class StageLedgerEntry:
    """Charged resources for one pipeline stage (volatile annex)."""

    stage: str
    tokens: int = 0
    cost_usd: float = 0.0
    retries: int = 0
    latency_ms: float = 0.0

    def to_json(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "tokens": self.tokens,
            "cost_usd": self.cost_usd,
            "retries": self.retries,
            "latency_ms": self.latency_ms,
        }


@dataclass
class _Tally:
    denominator: int = 0
    recovered: int = 0
    missed: list[str] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return {
            "denominator": self.denominator,
            "recovered": self.recovered,
            "missed": sorted(self.missed),
        }


def _bucket_findings(
    state: PipelineState, exclude_ids: frozenset[str]
) -> list[FindingView]:
    return [f for f in state.findings if f.finding_id not in exclude_ids]


def _match_conflict_finding(
    findings: Iterable[FindingView], record: MutationRecord
) -> FindingView | None:
    expected = record.expected
    for finding in findings:
        if expected.bucket_key and finding.bucket_key == expected.bucket_key:
            return finding
        wanted = {
            claim_id
            for claim_id in (expected.claim_logical_id, expected.counterpart_claim_id)
            if claim_id
        }
        if wanted and wanted <= finding.claim_ids:
            return finding
    return None


def _evidence_overlaps(finding: FindingView, record: MutationRecord) -> bool:
    """Evidence spans are mutated-document coordinates, so match against the
    record's inverse_span (the injected text's location in the mutated doc),
    not the original-corpus span — earlier mutations in the same document
    shift offsets between the two spaces."""
    for doc, span in finding.evidence:
        if doc == record.doc and span.overlaps(record.inverse_span):
            return True
    return False


def build_scorecard(
    *,
    gold: Sequence[MutationRecord],
    clean: PipelineState,
    mutated: PipelineState,
    leakage_pass: Mapping[str, Any] | None,
    ledger: Sequence[StageLedgerEntry] = (),
    run_binding_ref: str = "",
) -> dict[str, Any]:
    """Score one run. Returns ``{"core": ..., "annex": ...}``.

    ``leakage_pass`` is the artifact ``leakage.load_leakage_pass`` returns;
    the scorer refuses to emit a scorecard without a valid pass (fail
    closed, req.bench.leakage).
    """
    if (
        leakage_pass is None
        or leakage_pass.get("status") != "pass"
        or leakage_pass.get("schema") != "okg.injection.leakage-check.v1"
    ):
        raise InjectionError(
            "scorer refuses to emit a scorecard without a passed leakage-check "
            "artifact (fail closed)"
        )
    if not gold:
        raise InjectionError("cannot score an empty gold set")

    b0_ids = clean.finding_ids()
    new_findings = _bucket_findings(mutated, b0_ids)

    stages = {name: _Tally() for name in ("extraction", "candidate", "verdict", "routing")}
    per_mode: dict[str, _Tally] = {}
    per_tier: dict[str, _Tally] = {}
    families: dict[str, dict[str, Any]] = {}
    hard_negatives = _Tally()
    fp_by_mechanism: dict[str, list[str]] = {}
    dispositions: dict[str, dict[str, Any]] = {}
    matched_finding_ids: set[str] = set()

    def _family(record: MutationRecord) -> dict[str, Any]:
        return families.setdefault(
            record.family_id,
            {"members": 0, "recovered": 0, "matched_finding_ids": set()},
        )

    for record in sorted(gold, key=lambda r: r.mutation_id):
        expected = record.expected
        mode_tally = per_mode.setdefault(record.mode, _Tally())
        tier_tally = per_tier.setdefault(record.hardness_tier, _Tally())
        family = _family(record)
        mode_tally.denominator += 1
        tier_tally.denominator += 1
        family["members"] += 1
        disposition: dict[str, Any] = {"mode": record.mode, "kind": expected.kind}
        recovered_end_to_end = False

        def _note_match(finding_id: str, family: dict[str, Any] = family) -> None:
            matched_finding_ids.add(finding_id)
            family["matched_finding_ids"].add(finding_id)

        if expected.kind == "hard_negative":
            hard_negatives.denominator += 1
            match = _match_conflict_finding(new_findings, record)
            if expected.flagged_today:
                correct = match is not None and match.channel == expected.channel_today
            else:
                correct = match is None
            if correct:
                hard_negatives.recovered += 1
                recovered_end_to_end = True
            else:
                hard_negatives.missed.append(record.mutation_id)
                if match is not None and not expected.flagged_today:
                    mechanism = expected.mechanism or "unspecified"
                    fp_by_mechanism.setdefault(mechanism, []).append(match.finding_id)
            if match is not None:
                _note_match(match.finding_id)
            disposition["correct"] = correct

        elif record.mode == "independent":
            match = next(
                (f for f in new_findings if _evidence_overlaps(f, record)), None
            )
            recovered_end_to_end = match is not None
            if match is not None:
                _note_match(match.finding_id)
            disposition["evidence_overlap"] = recovered_end_to_end

        else:
            # Stage 1 — extraction recovery (needs a claim-level expectation).
            extraction_ok: bool | None = None
            if expected.kind == "claim_absent" and expected.claim_logical_id:
                stages["extraction"].denominator += 1
                extraction_ok = expected.claim_logical_id not in mutated.claims
            elif expected.claim_logical_id:
                stages["extraction"].denominator += 1
                extraction_ok = (
                    mutated.claims.get(expected.claim_logical_id) == record.post_text
                )
            if extraction_ok is True:
                stages["extraction"].recovered += 1
            elif extraction_ok is False:
                stages["extraction"].missed.append(record.mutation_id)
            disposition["extraction"] = extraction_ok

            # Stages 2–4 apply to pair conflicts; other kinds are end-to-end
            # presence checks on the finding surface.
            if (
                expected.kind == "conflict"
                and expected.claim_logical_id
                and expected.counterpart_claim_id
            ):
                pair = frozenset(
                    {expected.claim_logical_id, expected.counterpart_claim_id}
                )
                if extraction_ok:
                    stages["candidate"].denominator += 1
                    proposed = pair in mutated.candidates
                    if proposed:
                        stages["candidate"].recovered += 1
                        stages["verdict"].denominator += 1
                        match = _match_conflict_finding(new_findings, record)
                        verdict_ok = (
                            match is not None
                            and match.channel == expected.channel_today
                        )
                        if verdict_ok:
                            assert match is not None
                            stages["verdict"].recovered += 1
                            _note_match(match.finding_id)
                            stages["routing"].denominator += 1
                            routing_ok = match.routed and (
                                expected.severity is None
                                or match.severity == expected.severity
                            )
                            if routing_ok:
                                stages["routing"].recovered += 1
                                recovered_end_to_end = True
                            else:
                                stages["routing"].missed.append(record.mutation_id)
                            disposition["routing"] = routing_ok
                        else:
                            stages["verdict"].missed.append(record.mutation_id)
                        disposition["verdict"] = verdict_ok
                    else:
                        stages["candidate"].missed.append(record.mutation_id)
                    disposition["candidate"] = proposed
            elif expected.kind in ("claim_absent", "malformed_value", "broken_reference"):
                if extraction_ok:
                    match = _match_conflict_finding(new_findings, record)
                    recovered_end_to_end = expected.kind == "claim_absent" or match is not None
                    if match is not None:
                        _note_match(match.finding_id)
            elif expected.kind == "conflict":
                # Conflict without both claim ids (e.g. inserted assertion):
                # end-to-end presence on the bucket.
                match = _match_conflict_finding(new_findings, record)
                recovered_end_to_end = (
                    match is not None and match.channel == expected.channel_today
                )
                if match is not None:
                    _note_match(match.finding_id)

        disposition["recovered"] = recovered_end_to_end
        if recovered_end_to_end:
            mode_tally.recovered += 1
            tier_tally.recovered += 1
            family["recovered"] += 1
        else:
            mode_tally.missed.append(record.mutation_id)
            tier_tally.missed.append(record.mutation_id)
        dispositions[record.mutation_id] = disposition

    false_positives = sorted(
        finding.finding_id
        for finding in new_findings
        if finding.finding_id not in matched_finding_ids
    )

    def _rate(tally: _Tally) -> float | None:
        if tally.denominator == 0:
            return None
        return tally.recovered / tally.denominator

    graph_rate = _rate(per_mode.get("graph", _Tally()))
    independent_rate = _rate(per_mode.get("independent", _Tally()))
    visibility_gap = (
        None
        if graph_rate is None or independent_rate is None
        else graph_rate - independent_rate
    )

    core: dict[str, Any] = {
        "schema": SCORECARD_SCHEMA,
        "gold_count": len(gold),
        "stages": {name: tally.to_json() for name, tally in stages.items()},
        "modes": {name: tally.to_json() for name, tally in sorted(per_mode.items())},
        "tiers": {name: tally.to_json() for name, tally in sorted(per_tier.items())},
        # Deduplicated incident-family level (design §8): a family recovers
        # as a unit, and its matched-findings count exposes over-pairing
        # inflation (many findings for one incident family).
        "families": {
            family_id: {
                "members": data["members"],
                "recovered": data["recovered"],
                "matched_finding_ids": sorted(data["matched_finding_ids"]),
            }
            for family_id, data in sorted(families.items())
        },
        "hard_negatives": {
            **hard_negatives.to_json(),
            "fp_by_mechanism": {
                mechanism: sorted(ids)
                for mechanism, ids in sorted(fp_by_mechanism.items())
            },
        },
        "false_positives": {
            "count": len(false_positives),
            "finding_ids": false_positives,
            "baseline_excluded": len(b0_ids),
        },
        "end_to_end": {
            "graph_rate": graph_rate,
            "independent_rate": independent_rate,
            "visibility_gap": visibility_gap,
        },
        "dispositions": dict(sorted(dispositions.items())),
    }
    _assert_core_is_deterministic(core)
    annex: dict[str, Any] = {
        "run_binding_ref": run_binding_ref,
        "ledger": [entry.to_json() for entry in ledger],
    }
    return {"core": core, "annex": annex}


def _assert_core_is_deterministic(core: Mapping[str, Any]) -> None:
    """The deterministic core must not smuggle volatile fields (H1 contract)."""

    def walk(obj: Any, path: str) -> None:
        if isinstance(obj, Mapping):
            for key, value in obj.items():
                lowered = str(key).lower()
                if any(fragment in lowered for fragment in _VOLATILE_KEY_FRAGMENTS):
                    raise InjectionError(
                        f"volatile key {key!r} inside scorecard core at {path}"
                    )
                walk(value, f"{path}.{key}")
        elif isinstance(obj, (list, tuple)):
            for index, value in enumerate(obj):
                walk(value, f"{path}[{index}]")

    walk(core, "core")


def scorecard_core_bytes(scorecard: Mapping[str, Any]) -> bytes:
    """Canonical bytes of the deterministic core — the byte-comparison surface."""
    return canonical_json_bytes(scorecard["core"])
