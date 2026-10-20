"""The document pack must not be what caps corroboration.

A corroboration majority needs one fact rendered into at least three documents:
two asserting one value, one surviving counterpart asserting the other. Before
the referral_letter genre, only ``care_provider`` appeared in three genres --
every other kind appeared in two, so it capped at k=2 and could never be more
than one-vs-one. Measured over the smoke world: effective_k = {2: 146, 3: 4}.

Two things followed, and both were misread as results:

* exactly ONE corruption class (cardinality_violation, which targets
  care_provider) could express the majority axis, so corroboration was
  untestable for every other class;
* the real LLM-extracted corpus produced 378 one-vs-one buckets out of 398.
  That was reported as a property of the corpus. It was a property of the PACK
  -- the renderer could not put a fact in three documents, so no majority
  existed for corroboration to find.

These witnesses pin the fix at the level it actually lives: the pack ceiling,
NOT the realized redundancy of any particular preset. A preset is free to ask
for fewer renderings (smoke asks 1-3 at random); what must not happen is the
pack silently refusing three when three are asked for.
"""

from __future__ import annotations

import collections


from okg.bench.synth.config import load_preset
from okg.bench.synth.render import _eligible_genres, load_pack

_MIN_GENRES = 3


def _kinds_in_pack(pack) -> set[str]:
    return set(pack["labels"])


class _Fact:
    """Minimal stand-in: _eligible_genres reads only .kind."""

    def __init__(self, kind: str) -> None:
        self.kind = kind


def test_every_fact_kind_reaches_three_genres() -> None:
    """The ceiling, asserted structurally rather than through a run.

    A structural assertion is the right level here: a run only samples the
    kinds its world happens to contain, so it could pass while a kind nobody
    sampled stayed capped.
    """
    pack = load_pack("clinical_v1")
    genres = set(load_preset("smoke").realism.genres)

    capped = {}
    for kind in sorted(_kinds_in_pack(pack)):
        eligible = [g for g in _eligible_genres(pack, _Fact(kind)) if g in genres]
        if len(eligible) < _MIN_GENRES:
            capped[kind] = eligible

    assert not capped, (
        "these fact kinds cannot reach a corroboration majority because the "
        f"pack renders them in fewer than {_MIN_GENRES} genres: {capped}"
    )


def test_the_default_genre_set_includes_the_broad_genre() -> None:
    """The pack alone is not enough — a genre the config does not select is
    unreachable, and the eligibility filter intersects the two."""
    assert "referral_letter" in load_preset("smoke").realism.genres


def test_section_names_stay_canonical_across_genres() -> None:
    """The added genre must not introduce per-genre headings.

    Claim scope is the heading path and the engine treats disjoint scopes as
    compatible (spec 4.1 overlap rule), so a heading unique to one genre would
    scope-suppress every cross-genre bucket — silently deleting the very
    conflicts the added redundancy exists to create. This is the trap that cost
    a full debugging cycle when the corpus was first built.
    """
    pack = load_pack("clinical_v1")
    by_section: dict[str, set[str]] = collections.defaultdict(set)
    for genre_name, genre in pack["genres"].items():
        for section in genre["sections"]:
            by_section[section["name"]].add(genre_name)

    # Every section name used by referral_letter must already exist elsewhere:
    # a name unique to it would be a per-genre heading.
    referral_sections = {
        s["name"] for s in pack["genres"]["referral_letter"]["sections"]
    }
    unique = {n for n in referral_sections if by_section[n] == {"referral_letter"}}
    assert not unique, f"referral_letter invents per-genre headings: {unique}"


def test_more_than_one_class_can_express_a_majority() -> None:
    """The axis must not be restricted to cardinality_violation.

    Checked through the KINDS the corruption classes target: value
    perturbation and derived values target observations, polarity flip targets
    allergy status, cardinality violation targets care_provider. All must reach
    three genres for their class to be able to carry a majority.
    """
    pack = load_pack("clinical_v1")
    genres = set(load_preset("smoke").realism.genres)
    targeted = {
        "observation:body_weight": "value_perturbation / derived_value",
        "allergy_status:": "polarity_flip",
        "care_provider": "cardinality_violation",
        "medication:": "medication-targeting classes",
    }
    short = {
        f"{kind} ({cls})": [
            g for g in _eligible_genres(pack, _Fact(kind)) if g in genres
        ]
        for kind, cls in targeted.items()
        if len([g for g in _eligible_genres(pack, _Fact(kind)) if g in genres])
        < _MIN_GENRES
    }
    assert not short, f"these classes still cannot express a majority: {short}"
