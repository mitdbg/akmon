"""Pipeline-independent site targeting (design §4, threat T1's countermeasure).

Sites here come from raw corpus bytes only — structural/regex targeting of
the #749 object facets (dates, numbers with units, identifiers, URLs,
configuration key/values) plus raw token co-occurrence for cross-document
coordination. The type is constructed with no DSN, generation handle, or
claim snapshot and exposes no graph-read capability, so its blind spots
cannot correlate with the pipeline's; its sites carry ``mode="independent"``
and are scored at evidence level under a denominator the scorer never
merges with the graph-conditioned one.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from okg.bench.injection.snapshot import CorpusSnapshot
from okg.bench.injection.types import InjectionError, Site, Span, raw_site_id

# The regexable object facets (design §4): pattern per kind, innermost group
# is the mutable token.
_FACET_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("date", re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")),
    ("number_unit", re.compile(r"\b(\d+(?:\.\d+)?\s*(?:MB|GB|TB|ms|s)\b)")),
    ("url", re.compile(r"\b(https?://[^\s)>\]]+)")),
    ("identifier", re.compile(r"\b([A-Z][A-Z0-9]*-\d+)\b")),
    ("config_kv", re.compile(r"^[a-z_]+:\s*(\S+)\s*$", re.MULTILINE)),
)

INDEPENDENT_FACET_KINDS: tuple[str, ...] = tuple(kind for kind, _ in _FACET_PATTERNS)


@dataclass(frozen=True)
class IndependentSite:
    """One raw-span site plus its cross-document co-occurrences.

    ``counterparts`` lists every other document (and span) carrying the same
    matched token — the pipeline-independent analogue of a bucket sibling,
    derived from bytes, usable for coordinated cross-document injection and
    for evidence-overlap scoring.
    """

    site: Site
    kind: str
    matched_text: str
    counterparts: tuple[tuple[str, Span], ...] = field(default=())


@dataclass(frozen=True)
class IndependentTargeter:
    """Enumerate injection sites from corpus bytes alone (no graph surface)."""

    facet_kinds: tuple[str, ...] = INDEPENDENT_FACET_KINDS

    def __post_init__(self) -> None:
        unknown = [kind for kind in self.facet_kinds if kind not in INDEPENDENT_FACET_KINDS]
        if unknown:
            raise InjectionError(f"unknown independent facet kinds: {unknown}")

    def sites(self, corpus: CorpusSnapshot) -> tuple[IndependentSite, ...]:
        """Deterministic enumeration: docs in tree order, matches in text order."""
        matches: list[tuple[str, str, Span, str]] = []
        occurrences: dict[str, list[tuple[str, Span]]] = {}
        for relpath, _digest in corpus.files:
            text = corpus.read_text(relpath)
            for kind, pattern in _FACET_PATTERNS:
                if kind not in self.facet_kinds:
                    continue
                for match in pattern.finditer(text):
                    token = match.group(1)
                    span = Span(start=match.start(1), end=match.end(1))
                    matches.append((relpath, kind, span, token))
                    occurrences.setdefault(token, []).append((relpath, span))
        sites: list[IndependentSite] = []
        for relpath, kind, span, token in sorted(
            matches, key=lambda item: (item[0], item[2].start, item[1])
        ):
            counterparts = tuple(
                (other_doc, other_span)
                for other_doc, other_span in sorted(
                    occurrences[token], key=lambda pair: (pair[0], pair[1].start)
                )
                if other_doc != relpath
            )
            sites.append(
                IndependentSite(
                    site=Site(
                        site_id=raw_site_id(relpath, span, kind),
                        mode="independent",
                        doc=relpath,
                        span=span,
                    ),
                    kind=kind,
                    matched_text=token,
                    counterparts=counterparts,
                )
            )
        return tuple(sites)
