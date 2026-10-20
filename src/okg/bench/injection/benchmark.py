"""Ring-0 end-to-end runner: the documented example (task.bench.e2e-example).

Runs the complete loop against the in-repo fixture deployment: sync the
checked-in corpus into a working directory, ingest + publish the clean
baseline, target injections from the pinned clean generation, sync the
mutated corpus, ingest + publish the benchmark generation, run the leakage
check over the ring-0 captured surface (source-read bytes + published claim
content — ring 0 has no prompts), and score with live pipeline states.

Live state adapter (pre-#745 bounds, design §8): findings come from
``okg.claim_projection`` ``finding`` rows (Σ classification — channel is the
row's ``outcome``); candidates are the classified pairs' member ids, so
candidate recall is reported as a bound until #745's ledger lands; a finding
is ``routed`` when its bucket appears in the contested-bucket check's open
observations (the review surface).

Two operator families are not registered for ring 0 — ``config_swap`` and
``hn_conditional`` key on configuration-key normalized keys, while ring 0's
uniform extraction rule keys every kv claim by its H1 entity. They run in
the operator fixture suite; a scope-aware targeting field is their ring-0
path (follow-up noted in the runbook).
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Mapping

from okg.bench.injection.claim_corpus_source import (
    parse_document_claims,
)
from okg.bench.injection.injector import InjectionResult, run_injection
from okg.bench.injection.leakage import (
    load_leakage_pass,
    run_leakage_check,
)
from okg.bench.injection.manifest import canonical_json_bytes
from okg.bench.injection.operators.registry import (
    load_operators_yaml,
)
from okg.bench.injection.scoring import (
    FindingView,
    PipelineState,
    StageLedgerEntry,
    build_scorecard,
    scorecard_core_bytes,
)
from okg.bench.injection.snapshot import ClaimSnapshot, CorpusSnapshot
from okg.bench.injection.types import InjectionError, RunConfig

RING0_DEPLOYMENT = "consistency-injection-fixture"
RING0_CHECK_ID = "consistency-injection-fixture.ir.claim_state.contested_bucket.v1"
RING0_POLICIES = (
    "backup_node",
    "certified",
    "endpoint",
    "memory",
    "owner",
    "primary_node",
    "released",
    "retry_limit",
    "reviewed",
    "runbook",
    "status",
    "step",
    "timeout_s",
    "upgrade",
    "uptime",
    "version",
)
RUN_BINDING_FILENAME = "run-binding.json"
SCORECARD_CORE_FILENAME = "scorecard-core.json"
SCORECARD_ANNEX_FILENAME = "scorecard-annex.json"


def ring0_operators(deployment_dir: Path) -> tuple[Any, ...]:
    """The deployment-tuned operator set, loaded from its registry YAML.

    Core operators ship NEUTRAL (empty targeting); every targeting decision
    is deployment-supplied — ring 0's lives at
    ``deployments/consistency-injection-fixture/operators.yaml``.
    """
    return tuple(load_operators_yaml(Path(deployment_dir) / "operators.yaml"))


def ring0_stability_probe(doc: str, original_text: str, mutated_text: str) -> bool:
    """Re-derive claims and compare id → (value, scope) content maps.

    Stable iff every surviving id keeps identical content (a deletion that
    renumbers a same-(predicate, key) sibling onto a removed id changes that
    id's value and is caught), removals only shrink the map, and insertions
    mint new ids without disturbing existing ones. An unparseable mutated
    document is unstable, never an abort.
    """

    def content_map(text: str) -> dict[str, tuple[str, str]] | None:
        try:
            return {
                fact.node_id: (fact.attrs["value"], fact.attrs["scope"])
                for fact in parse_document_claims(doc, text)
            }
        except InjectionError:
            return None

    before = content_map(original_text)
    after = content_map(mutated_text)
    if before is None or after is None:
        return False
    surviving = set(before) & set(after)
    return all(before[node_id] == after[node_id] for node_id in surviving)


def sync_corpus(source_dir: Path, working_dir: Path) -> None:
    """Sync a corpus version into the deployment's working directory."""
    for path in sorted(Path(source_dir).rglob("*.md")):
        target = Path(working_dir) / path.relative_to(source_dir)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(path.read_bytes())


def load_pipeline_state(checkout: Any, *, routed_buckets: frozenset[str]) -> PipelineState:
    """Build the scorer's PipelineState from a pinned generation checkout."""
    snapshot = ClaimSnapshot.from_checkout(checkout, "pinned")
    claims = {claim.claim_id: claim.value for claim in snapshot.claims}
    rows = checkout.query(
        "SELECT row_key, predicate, bucket_key, outcome, member_claim_ids "
        "FROM okg.claim_projection WHERE row_kind = :kind ORDER BY row_key",
        {"kind": "finding"},
    )
    findings: list[FindingView] = []
    candidates: set[frozenset[str]] = set()
    for row in rows:
        member_ids = frozenset(json.loads(row["member_claim_ids"]))
        bucket = f"{row['predicate']}:{row['bucket_key']}"
        if len(member_ids) == 2:
            candidates.add(member_ids)
        findings.append(
            FindingView(
                finding_id=str(row["row_key"]),
                channel=str(row["outcome"]),
                claim_ids=member_ids,
                bucket_key=bucket,
                routed=bucket in routed_buckets,
            )
        )
    return PipelineState(
        claims=claims, candidates=frozenset(candidates), findings=tuple(findings)
    )


def write_run_binding(
    out_dir: Path, *, clean: Mapping[str, Any], mutated: Mapping[str, Any]
) -> Path:
    """The volatile binding of pipeline runs to the sealed artifacts (H1)."""
    binding = {
        "schema": "okg.injection.run-binding.v1",
        "deployment": RING0_DEPLOYMENT,
        "clean_generation_id": str(clean["generation_id"]),
        "clean_compat_generation_id": str(clean["compat_generation_id"]),
        "mutated_generation_id": str(mutated["generation_id"]),
        "mutated_compat_generation_id": str(mutated["compat_generation_id"]),
    }
    path = Path(out_dir) / RUN_BINDING_FILENAME
    path.write_bytes(canonical_json_bytes(binding))
    return path


def score_ring0_run(
    *,
    injection: InjectionResult,
    clean_state: PipelineState,
    mutated_state: PipelineState,
    working_dir: Path,
    ledger: tuple[StageLedgerEntry, ...],
) -> dict[str, Any]:
    """Leakage-gate then score; write the scorecard core + annex artifacts."""
    captured: list[bytes] = []
    for path in sorted(Path(working_dir).rglob("*.md")):
        captured.append(path.read_bytes())
    captured.append(
        canonical_json_bytes(sorted(mutated_state.claims.items()))
    )
    run_leakage_check(injection.out_dir, captured_inputs=captured)
    leakage_pass = load_leakage_pass(injection.out_dir)
    scorecard = build_scorecard(
        gold=injection.mutations,
        clean=clean_state,
        mutated=mutated_state,
        leakage_pass=leakage_pass,
        ledger=ledger,
        run_binding_ref=RUN_BINDING_FILENAME,
    )
    core_path = injection.out_dir / SCORECARD_CORE_FILENAME
    core_path.write_bytes(scorecard_core_bytes(scorecard))
    annex_path = injection.out_dir / SCORECARD_ANNEX_FILENAME
    annex_path.write_bytes(canonical_json_bytes(scorecard["annex"]))
    return scorecard


def run_ring0_injection(
    *,
    working_dir: Path,
    deployment_dir: Path,
    clean_snapshot: ClaimSnapshot,
    clean_findings_member_ids: frozenset[str],
    out_dir: Path,
    seed: int,
) -> InjectionResult:
    """Target and apply the deployment's operator set from the pinned clean state."""
    if not clean_snapshot.claims:
        raise InjectionError("clean snapshot holds no claims — nothing to target")
    return run_injection(
        corpus=CorpusSnapshot.load(working_dir),
        claims=clean_snapshot,
        operators=ring0_operators(deployment_dir),
        config=RunConfig(
            seed=seed, out_dir=out_dir, b0_claim_ids=clean_findings_member_ids
        ),
        stability_probe=ring0_stability_probe,
    )


def stage_ledger_entry(stage: str, started_monotonic: float) -> StageLedgerEntry:
    """LLM-free ring: tokens/cost/retries zero; wall-clock latency recorded."""
    return StageLedgerEntry(
        stage=stage, latency_ms=(time.monotonic() - started_monotonic) * 1000.0
    )


# --------------------------------------------------------------------------
# The complete loop (the `okg bench inject` CLI and the e2e witness share it)


def ingest_and_publish(
    dsn: str,
    working_dir: Path,
    deployment_dir: Path,
    previous_ids: frozenset[str] = frozenset(),
) -> tuple[dict[str, Any], frozenset[str]]:
    """Full-scope ingest + publish of the working corpus.

    Emits the absence-based retractions SourceRunner's record-set diff would
    for a completed discovery-crawl scope: node ids present in the previous
    run and absent now get ``op='R'``. Assumes a migrated DB carrying the
    extraction+consistency catalog (`okg up` / the pytest harness).
    """
    import psycopg

    from okg.bench.injection.claim_corpus_source import (
        ClaimCorpusSource,
    )
    from okg.substrate.graph_store import ChronosGraphStore
    from okg.substrate.library.sources.base import NodeFact
    from okg.substrate.publisher.factory import build_deployment_publisher

    source = ClaimCorpusSource(repo_root=working_dir, policies=RING0_POLICIES)
    facts = list(source.run("bench-inject").facts)
    current_ids = frozenset(fact.node_id for fact in facts)
    retractions = [
        NodeFact(node_id, "consistency_claim", {}, {"id": node_id}, {"gone": "1"}, op="R")
        for node_id in sorted(previous_ids - current_ids)
    ]
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=True)
        store.apply_facts(
            facts + retractions, source=RING0_DEPLOYMENT, strict_endpoints=False
        )
    out = build_deployment_publisher(
        dsn=dsn,
        deployment_name=RING0_DEPLOYMENT,
        deployment_dir=Path(deployment_dir),
        command="okg bench inject",
    ).cycle()
    if out.get("status") != "published":
        raise InjectionError(f"publish did not complete: {out}")
    return out, current_ids


def routed_buckets(dsn: str, check_id: str = RING0_CHECK_ID) -> frozenset[str]:
    """Open review-surface buckets as 'predicate:key' (participant role
    carries the bucket_row_key JSON pair)."""
    import psycopg

    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT participant_roles->>'bucket'
                  FROM okg.observation_current
                 WHERE check_id = %s AND finding_status = 'open'
                """,
                (check_id,),
            )
            rows = cur.fetchall()
    routed = set()
    for (raw,) in rows:
        if not raw:
            continue
        predicate, key = json.loads(raw)
        routed.add(f"{predicate}:{key}")
    return frozenset(routed)


def pipeline_state_at(dsn: str, generation_id: Any) -> tuple[PipelineState, ClaimSnapshot]:
    """The scorer's PipelineState + the targeting snapshot at a pinned generation."""
    import psycopg

    from okg.substrate.graph_store import ChronosGraphStore

    routed = routed_buckets(dsn)
    with psycopg.connect(dsn, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
        checkout = store.checkout_generation(generation_id)
        state = load_pipeline_state(checkout, routed_buckets=routed)
        snapshot = ClaimSnapshot.from_checkout(checkout, str(generation_id))
    return state, snapshot


def run_injection_benchmark(
    *,
    dsn: str,
    deployment_dir: Path,
    out_dir: Path,
    work_dir: Path,
    seed: int,
    corpus_dir: Path | None = None,
) -> dict[str, Any]:
    """The complete injection-benchmark loop against a prepared deployment DB."""
    deployment_dir = Path(deployment_dir)
    corpus = Path(corpus_dir) if corpus_dir is not None else deployment_dir / "corpus"
    working_dir = Path(work_dir)
    working_dir.mkdir(parents=True, exist_ok=True)
    sync_corpus(corpus, working_dir)

    started = time.monotonic()
    out_clean, clean_ids = ingest_and_publish(dsn, working_dir, deployment_dir)
    clean_ledger = stage_ledger_entry("clean_publish", started)
    clean_state, clean_snapshot = pipeline_state_at(dsn, out_clean["generation_id"])
    b0_member_ids = frozenset(
        claim_id for finding in clean_state.findings for claim_id in finding.claim_ids
    )

    injection = run_ring0_injection(
        working_dir=working_dir,
        deployment_dir=deployment_dir,
        clean_snapshot=clean_snapshot,
        clean_findings_member_ids=b0_member_ids,
        out_dir=Path(out_dir),
        seed=seed,
    )

    started = time.monotonic()
    sync_corpus(injection.mutated_corpus_dir, working_dir)
    out_mutated, _ids = ingest_and_publish(dsn, working_dir, deployment_dir, clean_ids)
    mutated_ledger = stage_ledger_entry("mutated_publish", started)
    mutated_state, _snapshot = pipeline_state_at(dsn, out_mutated["generation_id"])

    write_run_binding(injection.out_dir, clean=out_clean, mutated=out_mutated)
    scorecard = score_ring0_run(
        injection=injection,
        clean_state=clean_state,
        mutated_state=mutated_state,
        working_dir=working_dir,
        ledger=(clean_ledger, mutated_ledger),
    )
    return {
        "injection": injection,
        "scorecard": scorecard,
        "clean": out_clean,
        "mutated": out_mutated,
    }


def render_scorecard(core: Mapping[str, Any]) -> str:
    """Human-readable rendering of the DETERMINISTIC core only (raw
    denominators throughout; volatile fields live in the annex and never
    appear here)."""

    def tally(entry: Mapping[str, Any]) -> str:
        return f"{entry['recovered']}/{entry['denominator']}"

    lines = [
        f"injection benchmark scorecard  (gold={core['gold_count']})",
        "",
        "stage chain (each conditioned on the previous stage's survivors):",
    ]
    for stage in ("extraction", "candidate", "verdict", "routing"):
        entry = core["stages"][stage]
        missed = f"  missed: {', '.join(entry['missed'])}" if entry["missed"] else ""
        lines.append(f"  {stage:<11} {tally(entry)}{missed}")
    lines.append("")
    lines.append("end-to-end by targeting mode:")
    for mode, entry in core["modes"].items():
        lines.append(f"  {mode:<12} {tally(entry)}")
    end = core["end_to_end"]
    gap = end["visibility_gap"]
    lines.append(
        f"  visibility gap: {'n/a (single mode)' if gap is None else f'{gap:.3f}'}"
    )
    lines.append("")
    lines.append("recall by hardness tier:")
    for tier, entry in core["tiers"].items():
        lines.append(f"  {tier:<18} {tally(entry)}")
    hard = core["hard_negatives"]
    lines.append("")
    lines.append(f"hard negatives: {tally(hard)} correct (era-conditional)")
    for mechanism, finding_ids in hard["fp_by_mechanism"].items():
        lines.append(f"  leak via {mechanism}: {', '.join(finding_ids)}")
    fp = core["false_positives"]
    lines.append(
        f"false positives: {fp['count']} "
        f"(baseline findings excluded: {fp['baseline_excluded']})"
    )
    lines.append("")
    lines.append(
        f"incident families: {len(core['families'])} "
        "(members recovering as units; matched-finding counts expose over-pairing)"
    )
    return "\n".join(lines) + "\n"


__all__ = [
    "RING0_CHECK_ID",
    "RING0_DEPLOYMENT",
    "RING0_POLICIES",
    "RUN_BINDING_FILENAME",
    "SCORECARD_ANNEX_FILENAME",
    "SCORECARD_CORE_FILENAME",
    "ingest_and_publish",
    "load_pipeline_state",
    "pipeline_state_at",
    "render_scorecard",
    "ring0_operators",
    "routed_buckets",
    "run_injection_benchmark",
    "ring0_stability_probe",
    "run_ring0_injection",
    "score_ring0_run",
    "stage_ledger_entry",
    "sync_corpus",
    "write_run_binding",
]
