"""The injection engine: freeze → target → mutate → emit sealed artifacts.

Pipeline (design §3): enumerate sites per operator over the pinned corpus
and claim snapshots, canonicalize the order, drive every draw through the
seeded keyed PRF, gate gold integrity (b0 overlap, scoped unique-match
localization, byte pre-image, identity stability, span overlap — each a
recorded rejection, never silent), splice the surviving plans into a new
versioned corpus directory, and write schema-validated ``mutations.jsonl``
+ ``rejected_sites.jsonl`` + the sealed ``benchmark-manifest.json``.

The source corpus is never edited; ``CorpusSnapshot.verify_unchanged``
proves it after emission.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping, Sequence

from okg.bench.injection.manifest import (
    MANIFEST_FILENAME,
    MUTATED_CORPUS_DIRNAME,
    MUTATION_ROW_SCHEMA_V1,
    MUTATIONS_FILENAME,
    REJECTED_FILENAME,
    REJECTED_ROW_SCHEMA_V1,
    compute_canary,
    compute_config_digest,
    compute_family_id,
    compute_mutation_id,
    write_jsonl,
    write_manifest,
)
from okg.bench.injection.operator_base import MutationOperator
from okg.bench.injection.prf import KeyedPRF
from okg.bench.injection.scope_index import (
    Ambiguous,
    Located,
    MarkdownScopeIndex,
    NotFound,
)
from okg.bench.injection.snapshot import ClaimSnapshot, CorpusSnapshot
from okg.bench.injection.types import (
    InjectionError,
    MutationPlan,
    MutationRecord,
    RejectedSite,
    RunConfig,
    Site,
    Span,
)

# Given (doc relpath, original text, mutated text) → do the expected claim
# identities still materialize? Ring 0 backs this with the deterministic
# extractor; an assertion-count-changing plan with no probe is rejected.
StabilityProbe = Callable[[str, str, str], bool]


@dataclass(frozen=True)
class InjectionResult:
    """In-memory view of one emitted injection run."""

    out_dir: Path
    mutated_corpus_dir: Path
    manifest: Mapping[str, object]
    mutations: tuple[MutationRecord, ...]
    rejections: tuple[RejectedSite, ...]


@dataclass(frozen=True)
class _Accepted:
    operator: MutationOperator
    plan: MutationPlan
    mutation_id: str
    family_id: str


def _canonical_order_key(operator: MutationOperator, site: Site) -> tuple[str, str, str]:
    return (site.doc, site.site_id, operator.operator_id)


def _validate_operators(operators: Sequence[MutationOperator]) -> list[MutationOperator]:
    seen: set[str] = set()
    for operator in operators:
        if not operator.operator_id or not operator.version:
            raise InjectionError("operator with empty operator_id or version")
        if operator.operator_id in seen:
            raise InjectionError(f"duplicate operator_id {operator.operator_id!r}")
        seen.add(operator.operator_id)
    return sorted(operators, key=lambda op: op.operator_id)


def run_injection(
    *,
    corpus: CorpusSnapshot,
    claims: ClaimSnapshot,
    operators: Sequence[MutationOperator],
    config: RunConfig,
    stability_probe: StabilityProbe | None = None,
) -> InjectionResult:
    """Run one deterministic injection pass and emit its artifact directory."""
    out_dir = Path(config.out_dir)
    if out_dir.exists():
        raise InjectionError(f"out_dir {out_dir} already exists (refusing to overwrite)")
    ordered_operators = _validate_operators(operators)
    prf = KeyedPRF(config.seed, corpus.tree_digest)

    candidates: list[tuple[MutationOperator, Site]] = []
    for operator in ordered_operators:
        for site in operator.sites(corpus, claims):
            if not corpus.has_file(site.doc):
                raise InjectionError(
                    f"operator {operator.operator_id!r} emitted site {site.site_id!r} "
                    f"for unknown corpus file {site.doc!r}"
                )
            candidates.append((operator, site))
    candidates.sort(key=lambda pair: _canonical_order_key(pair[0], pair[1]))

    accepted: list[_Accepted] = []
    accepted_spans: dict[str, list[Span]] = {}
    accepted_buckets: dict[tuple[str, str], str] = {}
    rejections: list[RejectedSite] = []
    doc_texts: dict[str, str] = {}
    scope_indexes: dict[str, MarkdownScopeIndex] = {}

    def reject(operator: MutationOperator, site: Site, reason: str, detail: str) -> None:
        rejections.append(
            RejectedSite(
                site_id=site.site_id,
                operator_id=operator.operator_id,
                operator_version=operator.version,
                doc=site.doc,
                reason=reason,
                detail=detail,
            )
        )

    for operator, site in candidates:
        select_tag = f"select:{operator.operator_id}:{site.site_id}"
        if config.selection_rate < 1.0 and prf.uniform(select_tag) >= config.selection_rate:
            continue

        b0_hit = [
            claim.claim_id
            for claim in (site.claim, site.counterpart)
            if claim is not None and claim.claim_id in config.b0_claim_ids
        ]
        if b0_hit:
            reject(
                operator,
                site,
                "b0_overlap",
                f"claims {sorted(b0_hit)} participate in baseline findings",
            )
            continue

        if site.doc not in doc_texts:
            doc_texts[site.doc] = corpus.read_text(site.doc)
            scope_indexes[site.doc] = MarkdownScopeIndex(doc_texts[site.doc])
        doc_text = doc_texts[site.doc]

        if site.span is not None:
            span = site.span
        else:
            claim = site.claim
            assert claim is not None  # Site.__post_init__ guarantees this for graph mode
            located = scope_indexes[site.doc].locate_unique(claim.value, claim.scope)
            if isinstance(located, Ambiguous):
                reject(operator, site, "ambiguous_match", located.detail)
                continue
            if isinstance(located, NotFound):
                reject(operator, site, "not_found", located.detail)
                continue
            assert isinstance(located, Located)
            span = located.span

        plan = operator.plan(site, doc_text, span, prf)

        if doc_text[plan.span.start : plan.span.end] != plan.pre_text:
            reject(
                operator,
                site,
                "preimage_mismatch",
                f"file text at [{plan.span.start}, {plan.span.end}) does not equal pre_text",
            )
            continue

        if plan.changes_assertion_count:
            mutated_text = (
                doc_text[: plan.span.start] + plan.post_text + doc_text[plan.span.end :]
            )
            if stability_probe is None:
                reject(
                    operator,
                    site,
                    "identity_unstable",
                    "assertion-count-changing plan with no stability probe configured",
                )
                continue
            if not stability_probe(site.doc, doc_text, mutated_text):
                reject(
                    operator,
                    site,
                    "identity_unstable",
                    "stability probe reports expected claim identities do not materialize",
                )
                continue

        overlapping = [
            existing for existing in accepted_spans.get(site.doc, []) if plan.span.overlaps(existing)
        ]
        if overlapping:
            reject(
                operator,
                site,
                "span_overlap",
                f"plan span [{plan.span.start}, {plan.span.end}) overlaps an accepted mutation",
            )
            continue

        # Two mutations from different families inside one (predicate, key)
        # bucket would corrupt each other's counterpart expectations — the
        # answer key would name counterpart values another mutation changed.
        family_key = plan.family_key if plan.family_key is not None else f"site:{site.site_id}"
        bucket_keys = [
            (claim.predicate, claim.normalized_key)
            for claim in (site.claim, site.counterpart)
            if claim is not None
        ]
        colliding = [key for key in bucket_keys if accepted_buckets.get(key, family_key) != family_key]
        if colliding:
            reject(
                operator,
                site,
                "bucket_collision",
                f"buckets {sorted(colliding)} already mutated by a different family",
            )
            continue
        for key in bucket_keys:
            accepted_buckets[key] = family_key

        mutation_id = compute_mutation_id(
            tree_digest=corpus.tree_digest,
            seed=config.seed,
            operator_id=operator.operator_id,
            operator_version=operator.version,
            site_id=site.site_id,
            params=plan.params,
        )
        family_id = compute_family_id(
            tree_digest=corpus.tree_digest, seed=config.seed, family_key=family_key
        )
        accepted.append(
            _Accepted(operator=operator, plan=plan, mutation_id=mutation_id, family_id=family_id)
        )
        accepted_spans.setdefault(site.doc, []).append(plan.span)

    records = _apply_and_record(corpus, accepted, doc_texts, out_dir)

    manifest = {
        "schema": "okg.injection.benchmark-manifest.v1",
        "corpus": {
            "tree_digest": corpus.tree_digest,
            "file_count": len(corpus.files),
            "files": [list(entry) for entry in corpus.files],
        },
        "seed": config.seed,
        "canary": compute_canary(tree_digest=corpus.tree_digest, seed=config.seed),
        "operators": [
            {"id": operator.operator_id, "version": operator.version}
            for operator in ordered_operators
        ],
        "config_digest": compute_config_digest(config.config),
        "targeting": {
            "claims_digest": claims.digest(),
            "modes": sorted({site.mode for _, site in candidates}),
        },
        "counts": {
            "mutations": len(records),
            "rejected_sites": len(rejections),
            "candidate_sites": len(candidates),
        },
    }

    write_jsonl(
        out_dir / MUTATIONS_FILENAME,
        [record.to_json() for record in records],
        MUTATION_ROW_SCHEMA_V1,
    )
    write_jsonl(
        out_dir / REJECTED_FILENAME,
        [rejection.to_json() for rejection in rejections],
        REJECTED_ROW_SCHEMA_V1,
    )
    write_manifest(out_dir / MANIFEST_FILENAME, manifest)

    corpus.verify_unchanged()

    return InjectionResult(
        out_dir=out_dir,
        mutated_corpus_dir=out_dir / MUTATED_CORPUS_DIRNAME,
        manifest=manifest,
        mutations=tuple(records),
        rejections=tuple(rejections),
    )


def apply_inverses(mutated_text: str, records: Sequence[MutationRecord]) -> str:
    """Materialize the recorded inverses of one document's mutations.

    The repair-metamorphic mechanism (design §3 step 6): applying every
    record's exact inverse to the mutated text restores the original bytes.
    The post-image is byte-checked before each splice — a drifted document
    raises rather than producing silently-wrong "repaired" text.
    """
    text = mutated_text
    for record in sorted(records, key=lambda r: r.inverse_span.start, reverse=True):
        current = text[record.inverse_span.start : record.inverse_span.end]
        if current != record.post_text:
            raise InjectionError(
                f"inverse post-image mismatch for {record.mutation_id} at "
                f"[{record.inverse_span.start}, {record.inverse_span.end})"
            )
        text = (
            text[: record.inverse_span.start]
            + record.inverse_text
            + text[record.inverse_span.end :]
        )
    return text


def _apply_and_record(
    corpus: CorpusSnapshot,
    accepted: Sequence[_Accepted],
    doc_texts: Mapping[str, str],
    out_dir: Path,
) -> list[MutationRecord]:
    """Splice accepted plans into the mutated tree; compute mutated-coordinate inverses."""
    mutated_dir = out_dir / MUTATED_CORPUS_DIRNAME
    mutated_dir.mkdir(parents=True, exist_ok=False)

    by_doc: dict[str, list[_Accepted]] = {}
    for item in accepted:
        by_doc.setdefault(item.plan.site.doc, []).append(item)

    records: list[MutationRecord] = []
    mutated_texts: dict[str, str] = {}
    for doc, items in sorted(by_doc.items()):
        items.sort(key=lambda item: item.plan.span.start)
        original = doc_texts[doc]
        pieces: list[str] = []
        cursor = 0
        delta = 0
        for item in items:
            plan = item.plan
            pieces.append(original[cursor : plan.span.start])
            pieces.append(plan.post_text)
            cursor = plan.span.end
            mutated_start = plan.span.start + delta
            inverse_span = Span(start=mutated_start, end=mutated_start + len(plan.post_text))
            delta += len(plan.post_text) - len(plan.pre_text)
            records.append(
                MutationRecord(
                    mutation_id=item.mutation_id,
                    family_id=item.family_id,
                    mode=plan.site.mode,
                    doc=doc,
                    span=plan.span,
                    pre_text=plan.pre_text,
                    post_text=plan.post_text,
                    operator_id=item.operator.operator_id,
                    operator_version=item.operator.version,
                    params=plan.params,
                    facets=plan.facets,
                    hardness_tier=plan.hardness_tier,
                    expected=plan.expected,
                    inverse_span=inverse_span,
                    inverse_text=plan.pre_text,
                )
            )
        pieces.append(original[cursor:])
        mutated_texts[doc] = "".join(pieces)

    for relpath, _digest in corpus.files:
        target = mutated_dir / relpath
        target.parent.mkdir(parents=True, exist_ok=True)
        if relpath in mutated_texts:
            target.write_bytes(mutated_texts[relpath].encode("utf-8"))
        else:
            target.write_bytes(corpus.read_bytes(relpath))

    records.sort(key=lambda record: (record.doc, record.span.start, record.mutation_id))
    return records
