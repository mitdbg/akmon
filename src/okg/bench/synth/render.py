"""Trace-complete dual rendering: world (+ corruption plan) → documents.

The renderer assembles the CLEAN corpus and the CORRUPTED corpus in one
pass with identical structure (same documents, same line order); they
differ exactly at corruption sites. Every rendering of every fact is
recorded in the trace with byte spans in both corpora, so:

- extraction recall gets the full-population denominator,
- every corruption site yields a (doc, span, pre_text, post_text,
  inverse_span) tuple bridgeable to the #754 ``MutationRecord``,
- partitions are FINALIZED here against effective redundancy (a fact whose
  eligible genres cap k below the planned value gets its partition
  reconciled, never silently mislabeled).

Contract with the deterministic claim extractor
(``claim_corpus_source.parse_document_claims``): H1 is the patient
identity, identical across the patient's documents (claims bucket by
(predicate, H1-entity)); every claim renders as a ``Label: value`` line
whose label is per-KIND (identical in all genres).
"""

from __future__ import annotations

from dataclasses import dataclass
from importlib import resources
from typing import Any, Mapping

import yaml

from okg.bench.synth.config import SynthConfig
from okg.bench.synth.corruption import (
    CLASS_BACKUP_CHECK,
    PARTITION_CROSS_DOC,
    PARTITION_INTERNAL_ONLY,
    PARTITION_UNDETECTABLE,
    CorruptionPlan,
    CorruptionRow,
    Planner,
    TwinRow,
)
from okg.bench.synth.prf import SynthPrf
from okg.bench.synth.worldschema import Fact, GroundTruthWorld

_PACK_PACKAGE = "okg.bench.synth.packs"


class RenderError(ValueError):
    """Typed rendering failure."""


@dataclass(frozen=True)
class TraceRow:
    fact_id: str
    doc: str
    predicate_label: str
    surface_value: str
    clean_start: int
    clean_end: int
    corrupted_start: int
    corrupted_end: int
    corrupted: bool
    variation: str  # "" | benign family name


@dataclass(frozen=True)
class CorruptionSite:
    """One corruption's realized site — the MutationRecord bridge row."""

    corruption_id: str
    corruption_class: str
    layer: str
    partition: str
    fact_id: str
    doc: str
    clean_span: tuple[int, int]
    corrupted_span: tuple[int, int]
    pre_text: str
    post_text: str
    params: Mapping[str, str]


@dataclass(frozen=True)
class RenderResult:
    clean_docs: Mapping[str, str]
    corrupted_docs: Mapping[str, str]
    trace: tuple[TraceRow, ...]
    sites: tuple[CorruptionSite, ...]
    twins: tuple[TwinRow, ...]
    effective_k: Mapping[str, int]
    finalized_partitions: Mapping[str, str]


def load_pack(name: str = "clinical_v1") -> dict[str, Any]:
    text = (resources.files(_PACK_PACKAGE) / f"{name}.yaml").read_text(encoding="utf-8")
    pack = yaml.safe_load(text)
    if pack.get("schema_version") != "okg.synth.pack.v1":
        raise RenderError(f"template pack {name!r} has wrong schema_version")
    return pack


def pack_bytes(name: str = "clinical_v1") -> bytes:
    return (resources.files(_PACK_PACKAGE) / f"{name}.yaml").read_bytes()


def _label_for(pack: Mapping[str, Any], fact: Fact) -> str:
    labels: Mapping[str, str] = pack["labels"]
    if fact.kind in labels:
        return labels[fact.kind]
    for prefix, base in labels.items():
        if prefix.endswith(":") and fact.kind.startswith(prefix):
            suffix = fact.key.replace("_", " ").title()
            return f"{base} {suffix}"
    raise RenderError(f"pack has no label rule for fact kind {fact.kind!r}")


def _surface(fact: Fact, value: str, variation: str) -> str:
    """Canonical or benign-varied surface form of a value."""
    if variation == "unit_equivalent":
        quantity = float(value)
        if fact.unit == "kg":
            return f"{quantity * 1000:.0f} g"
        if fact.unit == "cm":
            return f"{quantity * 10:.0f} mm"
        raise RenderError(f"unit_equivalent variation unsupported for unit {fact.unit!r}")
    if fact.unit:
        return f"{value} {fact.unit}"
    return value


def _prose_template(pack: Mapping[str, Any], fact: Fact) -> tuple[str, str]:
    """(template, suffix) for a fact, or a LOUD failure.

    A missing template must not fall back to a structured line: prose and
    structured would then be mixed inside one corpus and the difficulty
    measurement would silently average two different rendering styles.
    """
    templates: Mapping[str, str] = pack.get("prose") or {}
    if fact.kind in templates:
        return templates[fact.kind], ""
    for prefix, template in templates.items():
        if prefix.endswith(":") and fact.kind.startswith(prefix):
            return template, fact.key.replace("_", " ").title()
    raise RenderError(
        f"pack has no prose template for fact kind {fact.kind!r}. Falling back "
        "to a structured line would mix rendering styles inside one corpus and "
        "silently average two difficulties in the measurement."
    )


def _render_fact_line(
    pack: Mapping[str, Any], fact: Fact, label: str, surface: str, mode: str
) -> tuple[str, int]:
    """One rendered line and the OFFSET of the value within it.

    Returning the offset (rather than assuming a fixed prefix) is what keeps
    gold traceable in prose: the value can sit anywhere in the sentence, and an
    offset computed from the template is exact wherever it lands.
    """
    if mode == "structured":
        prefix = f"{label}: "
        return f"{prefix}{surface}\n", len(prefix)
    template, suffix = _prose_template(pack, fact)
    if "{value}" not in template:
        raise RenderError(
            f"prose template for {fact.kind!r} has no {{value}} placeholder, so "
            "the value would not appear and the fact would be untraceable"
        )
    head = template.split("{value}", 1)[0].replace("{suffix}", suffix)
    text = template.replace("{suffix}", suffix).replace("{value}", surface)
    return f"{text}\n", len(head)


def _eligible_genres(pack: Mapping[str, Any], fact: Fact) -> list[str]:
    out = []
    for genre_name in sorted(pack["genres"]):
        genre = pack["genres"][genre_name]
        for section in genre["sections"]:
            for kind in section["kinds"]:
                if fact.kind == kind or (kind.endswith(":") and fact.kind.startswith(kind)):
                    out.append(genre_name)
                    break
            else:
                continue
            break
    return out


class _DocBuilder:
    """Accumulates one document's clean and corrupted text in lockstep."""

    def __init__(self) -> None:
        self.clean_parts: list[str] = []
        self.corrupted_parts: list[str] = []
        self.clean_len = 0
        self.corrupted_len = 0

    def emit(self, text: str) -> None:
        self.emit_pair(text, text)

    def emit_pair(self, clean_text: str, corrupted_text: str) -> None:
        self.clean_parts.append(clean_text)
        self.corrupted_parts.append(corrupted_text)
        self.clean_len += len(clean_text)
        self.corrupted_len += len(corrupted_text)

    def texts(self) -> tuple[str, str]:
        return "".join(self.clean_parts), "".join(self.corrupted_parts)


def render_corpus(
    world: GroundTruthWorld,
    config: SynthConfig,
    planner: Planner,
    plan: CorruptionPlan,
    prf: SynthPrf,
    pack_name: str = "clinical_v1",
) -> RenderResult:
    pack = load_pack(pack_name)
    genres = list(config.realism.genres)
    for genre in genres:
        if genre not in pack["genres"]:
            raise RenderError(f"config genre {genre!r} not in pack {pack_name!r}")

    corruption_by_fact: dict[str, CorruptionRow] = {r.fact_id: r for r in plan.rows}

    # ---- assignment: which docs render which fact -------------------------
    assignments: dict[str, list[str]] = {}
    effective_k: dict[str, int] = {}
    corrupted_slots: dict[str, set[str]] = {}
    varied_slot: dict[str, str] = {}
    for fact in world.facts:
        eligible = [g for g in _eligible_genres(pack, fact) if g in genres]
        if not eligible:
            continue
        planned_k = planner.renderings_for(fact.fact_id)
        k = min(planned_k, len(eligible))
        chosen = prf.shuffled(eligible, "assign", fact.fact_id)[:k]
        assignments[fact.fact_id] = sorted(chosen)
        effective_k[fact.fact_id] = k
        row = corruption_by_fact.get(fact.fact_id)
        if row is not None:
            v = min(row.visibility_corrupted, k)
            corrupted_slots[fact.fact_id] = set(
                prf.shuffled(sorted(chosen), "corrupt_slot", fact.fact_id)[:v]
            )
        # benign unit-equivalent variation — the wired tier-1 knob (review
        # finding H2): eligible uncorrupted kg/cm facts with a counterpart
        # get one varied rendering at the configured rate; the realized
        # variation sites become the hard-negative twin gold.
        if (
            row is None
            and k > 1
            and fact.unit in {"kg", "cm"}
            and fact.derived_from is None
            and prf.uniform("twin", fact.fact_id)
            < config.benign_variation.unit_rescale_rate
        ):
            varied_slot[fact.fact_id] = prf.choice(sorted(chosen), "twin_slot", fact.fact_id)

    # ---- partition finalization ------------------------------------------
    finalized: dict[str, str] = {}
    for row in plan.rows:
        k = effective_k.get(row.fact_id, 0)
        if k == 0:
            raise RenderError(
                f"corruption {row.corruption_id} targets fact with no eligible genre"
            )
        v = len(corrupted_slots.get(row.fact_id, ()))
        partition = row.partition
        if partition == PARTITION_CROSS_DOC and v >= k:
            # effective redundancy capped below the planned value: no clean
            # counterpart survives, so reconcile per the assignment function
            # (backup check => internal_only; none => undetectable).
            partition = (
                PARTITION_INTERNAL_ONLY
                if CLASS_BACKUP_CHECK.get(row.corruption_class, False)
                else PARTITION_UNDETECTABLE
            )
        finalized[row.corruption_id] = partition

    # majority-wrong expressibility (review finding M3): a class that asked
    # for majority-wrong WITH a surviving clean counterpart (1 < v < k_max)
    # must realize at least one cross_doc site, or the pack's genre
    # eligibility silently swallowed the axis — fail closed instead.
    k_max = config.redundancy.renderings_per_fact_max
    by_class: dict[str, list[CorruptionRow]] = {}
    for row in plan.rows:
        by_class.setdefault(row.corruption_class, []).append(row)
    for cls, cls_rows in sorted(by_class.items()):
        wanted_majority = [
            r for r in cls_rows if 1 < r.visibility_corrupted < k_max
        ]
        if wanted_majority and not any(
            finalized[r.corruption_id] == PARTITION_CROSS_DOC for r in wanted_majority
        ):
            raise RenderError(
                f"{cls}: majority-wrong visibility requested but every site "
                f"was demoted (the pack's genre eligibility caps this class "
                f"below the needed redundancy) - inexpressible, use a class "
                f"whose fact kind renders in more genres"
            )

    # ---- document assembly ------------------------------------------------
    trace: list[TraceRow] = []
    sites: list[CorruptionSite] = []
    clean_docs: dict[str, str] = {}
    corrupted_docs: dict[str, str] = {}

    for patient in sorted(world.patients, key=lambda p: p.patient_id):
        facts = sorted(world.facts_for(patient.patient_id), key=lambda f: f.fact_id)
        for genre_name in genres:
            genre = pack["genres"][genre_name]
            doc = f"{patient.family.lower()}_{patient.given.lower()}__{genre_name}.md".replace(
                " ", "_"
            )
            builder = _DocBuilder()
            builder.emit(f"# Patient {patient.full_name}\n\n")
            # genre title is BODY TEXT, not a heading: headings define claim
            # scopes, and a per-genre heading would scope-suppress every
            # cross-genre bucket (pack contract; spec §4.1 overlap rule).
            builder.emit(f"**{genre['title']}.** {genre['intro']}\n\n")
            for section in genre["sections"]:
                section_lines: list[tuple[Fact, str]] = []
                for fact in facts:
                    if genre_name in assignments.get(fact.fact_id, ()):
                        for kind in section["kinds"]:
                            if fact.kind == kind or (
                                kind.endswith(":") and fact.kind.startswith(kind)
                            ):
                                section_lines.append((fact, _label_for(pack, fact)))
                                break
                if not section_lines:
                    continue
                if config.realism.slot_shuffle and len(section_lines) > 1:
                    # deterministic per-document reordering: the countermeasure
                    # to template-slot alignment (design §8) — labels stay
                    # stable, positions do not.
                    section_lines = prf.shuffled(
                        section_lines, "slot_shuffle", doc, section["name"]
                    )
                builder.emit(f"## {section['name']}\n\n")
                for fact, label in section_lines:
                    row = corruption_by_fact.get(fact.fact_id)
                    variation = (
                        "unit_equivalent"
                        if varied_slot.get(fact.fact_id) == genre_name
                        else ""
                    )
                    clean_surface = _surface(fact, fact.value, variation)
                    is_corrupted_here = (
                        row is not None and genre_name in corrupted_slots.get(fact.fact_id, ())
                    )
                    if is_corrupted_here and row is not None:
                        corrupted_surface = _surface(fact, row.corrupted_value, "")
                    else:
                        corrupted_surface = clean_surface
                    mode = config.realism.render_mode
                    clean_line, clean_off = _render_fact_line(
                        pack, fact, label, clean_surface, mode
                    )
                    corrupted_line, corrupted_off = _render_fact_line(
                        pack, fact, label, corrupted_surface, mode
                    )
                    clean_start = builder.clean_len + clean_off
                    corrupted_start = builder.corrupted_len + corrupted_off
                    builder.emit_pair(clean_line, corrupted_line)
                    trace.append(
                        TraceRow(
                            fact_id=fact.fact_id,
                            doc=doc,
                            predicate_label=label,
                            surface_value=corrupted_surface,
                            clean_start=clean_start,
                            clean_end=clean_start + len(clean_surface),
                            corrupted_start=corrupted_start,
                            corrupted_end=corrupted_start + len(corrupted_surface),
                            corrupted=is_corrupted_here,
                            variation=variation,
                        )
                    )
                    if is_corrupted_here and row is not None:
                        sites.append(
                            CorruptionSite(
                                corruption_id=row.corruption_id,
                                corruption_class=row.corruption_class,
                                layer=row.layer,
                                partition=finalized[row.corruption_id],
                                fact_id=fact.fact_id,
                                doc=doc,
                                clean_span=(clean_start, clean_start + len(clean_surface)),
                                corrupted_span=(
                                    corrupted_start,
                                    corrupted_start + len(corrupted_surface),
                                ),
                                pre_text=clean_surface,
                                post_text=corrupted_surface,
                                params=row.params,
                            )
                        )
                builder.emit("\n")
            clean_text, corrupted_text = builder.texts()
            clean_docs[doc] = clean_text
            corrupted_docs[doc] = corrupted_text

    # ---- completeness gate ------------------------------------------------
    rendered_counts: dict[str, int] = {}
    for row_t in trace:
        rendered_counts[row_t.fact_id] = rendered_counts.get(row_t.fact_id, 0) + 1
    for fact_id, k in effective_k.items():
        if rendered_counts.get(fact_id, 0) != k:
            raise RenderError(
                f"trace incompleteness: fact {fact_id} rendered "
                f"{rendered_counts.get(fact_id, 0)} times, expected {k}"
            )
    realized = {s.corruption_id for s in sites}
    for row in plan.rows:
        if row.corruption_id not in realized:
            raise RenderError(f"corruption {row.corruption_id} was never rendered")

    twins = tuple(
        TwinRow(
            twin_id=f"twin:unit_equivalent:{fact_id}",
            family="unit_equivalent",
            fact_id=fact_id,
            detail={"varied_genre": genre},
        )
        for fact_id, genre in sorted(varied_slot.items())
    )

    return RenderResult(
        clean_docs=clean_docs,
        corrupted_docs=corrupted_docs,
        trace=tuple(trace),
        sites=tuple(sites),
        twins=twins,
        effective_k=effective_k,
        finalized_partitions=finalized,
    )
