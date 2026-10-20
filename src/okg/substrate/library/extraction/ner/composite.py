"""CompositeNER — span-overlap dedupe + canonical-type matching.

The dedupe rule (pinned semantics from proposal §"Composite
semantics — pinned"):

  1. Type canonicalization: compare entity types via `.lower()`
     so spaCy `'PERSON'` and a regex backend `'person'` collide
     on the same canonical key.
  2. Span overlap (not equality): two entities collide when
     canonical types match AND `[s_a, e_a) ∩ [s_b, e_b) ≠ ∅`.
  3. Within an overlap cluster: highest confidence wins. Tied
     confidence: longer span wins. Still tied: first backend in
     the composite list wins (deterministic).

Replaces the original `(type, value, start, end)` key-equality
dedupe, which fails on the most-common pairing (spaCy + regex
on the same node) because the two tokenize differently.

Batching is an explicit non-goal: there is no `extract_batch`
primitive. Single-document `.extract(text)` is the surface;
revisit if a profile names NER as the bottleneck.
"""

from __future__ import annotations

from okg.extraction import EntityExtractor, ExtractedEntity


def _canon_type(t: str) -> str:
    return (t or "").lower()


def _spans_overlap(
    a_start: int, a_end: int, b_start: int, b_end: int,
) -> bool:
    """Half-open span overlap: [a_start, a_end) ∩ [b_start, b_end) ≠ ∅."""
    return a_start < b_end and b_start < a_end


def _better(a: ExtractedEntity, b: ExtractedEntity) -> ExtractedEntity:
    """Pick the winner of an overlap cluster:
    1. higher confidence,
    2. else longer span,
    3. else `a` (caller-supplied order — first writer wins on full tie).
    """
    if a.confidence != b.confidence:
        return a if a.confidence > b.confidence else b
    a_len = a.end - a.start
    b_len = b.end - b.start
    if a_len != b_len:
        return a if a_len > b_len else b
    return a


class CompositeNER:
    """Fan out across backends, dedupe via canonical-type +
    span-overlap, return one entity per overlap cluster (the
    cluster's chosen winner)."""

    def __init__(self, *backends: EntityExtractor) -> None:
        if not backends:
            raise ValueError("CompositeNER requires at least one backend")
        self._backends = backends

    def extract(self, text: str) -> list[ExtractedEntity]:
        if not text:
            return []
        # Collect with stable ordering (first backend earliest).
        all_ents: list[ExtractedEntity] = []
        for b in self._backends:
            all_ents.extend(b.extract(text))

        # Cluster by canonical type + span overlap.
        # Use a list of (winner_ent) groups; for each new entity,
        # find any existing group whose winner overlaps + matches
        # canonical type, and merge by picking the better one.
        winners: list[ExtractedEntity] = []
        for e in all_ents:
            ce = _canon_type(e.type)
            merged = False
            for i, w in enumerate(winners):
                if _canon_type(w.type) != ce:
                    continue
                if not _spans_overlap(e.start, e.end, w.start, w.end):
                    continue
                # Same cluster — pick the winner.
                winners[i] = _better(w, e)
                merged = True
                break
            if not merged:
                winners.append(e)

        # Stable order: by start, then end.
        winners.sort(key=lambda e: (e.start, e.end))
        return winners


__all__ = ["CompositeNER"]
