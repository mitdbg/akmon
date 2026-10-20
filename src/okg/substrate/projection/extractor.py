"""Substrate-side entity extractor (M3.3).

Re-uses the core `okg.extraction` Protocol so deployment-specific
extractors (CMSEntityExtractor, etc.) plug in without a second
adapter layer.

The substrate ships ONE concrete extractor: `RegexEntityExtractor`,
configured by a list of (entity_type, regex_pattern) pairs. It is
the v1 default and what the bundled tests use. Production
deployments register their own EntityExtractor implementations
via `deployment.yaml` (the M5/M6 wiring point).
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Pattern, Sequence

from okg.extraction import EntityExtractor, ExtractedEntity

if TYPE_CHECKING:
    import psycopg


__all__ = [
    "EntityExtractor",
    "ExtractedEntity",
    "NoOpExtractor",
    "RegexEntityExtractor",
    "SqlIdentifierExtractor",
]


class NoOpExtractor:
    """Returns no entities for any input. M3.2 default; replaced
    by a real extractor when the deployment registers one.
    """

    def extract(self, text: str) -> list[ExtractedEntity]:
        return []


@dataclass(frozen=True)
class _RegexRule:
    entity_type: str
    pattern: Pattern[str]
    confidence: float = 0.9


class RegexEntityExtractor:
    """Apply a list of (entity_type, regex) rules to text.

    Each match becomes one ExtractedEntity. Order in the rules
    list determines order of `extract()`'s output. Overlapping
    matches are NOT deduped here — that's the router/projector's
    job (the dedupe_key trio (src, edge_type, dst) collapses
    them once an edge is materialized).

    Confidence is per-rule; default 0.9 (matching legacy default).
    """

    def __init__(
        self,
        rules: Sequence[tuple[str, str] | tuple[str, str, float]],
    ) -> None:
        compiled: list[_RegexRule] = []
        for r in rules:
            if len(r) == 2:
                etype, pat = r
                conf = 0.9
            else:
                etype, pat, conf = r
            if not isinstance(etype, str) or not etype:
                raise ValueError(
                    f"RegexEntityExtractor rule entity_type must be a "
                    f"non-empty string; got {etype!r}"
                )
            if not (0.0 <= float(conf) <= 1.0):
                raise ValueError(
                    f"RegexEntityExtractor rule confidence must be in "
                    f"[0, 1]; got {conf!r}"
                )
            compiled.append(_RegexRule(
                entity_type=etype,
                pattern=re.compile(pat),
                confidence=float(conf),
            ))
        self._rules = compiled
        self.fingerprint = {
            "class": (
                "okg.substrate.projection.extractor.RegexEntityExtractor"
            ),
            "rules": [
                {
                    "entity_type": rule.entity_type,
                    "pattern": rule.pattern.pattern,
                    "flags": rule.pattern.flags,
                    "confidence": rule.confidence,
                }
                for rule in self._rules
            ],
        }

    def extract(self, text: str) -> list[ExtractedEntity]:
        if not text:
            return []
        out: list[ExtractedEntity] = []
        for rule in self._rules:
            for m in rule.pattern.finditer(text):
                # Use group(1) if the pattern has a capture group,
                # else the full match. Keep the span aligned with
                # the emitted value so callers can recover it with
                # `text[start:end]`.
                if m.groups():
                    value = m.group(1)
                    start = m.start(1)
                    end = m.end(1)
                else:
                    value = m.group(0)
                    start = m.start()
                    end = m.end()
                out.append(ExtractedEntity(
                    type=rule.entity_type,
                    value=value,
                    start=start,
                    end=end,
                    confidence=rule.confidence,
                ))
        return out


class SqlIdentifierExtractor:
    """Substrate-native identifier extractor.

    Defers regex evaluation to the `okg.find_identifier_mentions`
    SQL function (Postgres 17 + the catalog version's pattern
    set). Post-filters each match through the `validator_kind`
    Python validator (DOI structure, ORCID Mod-11, ISBN
    Mod-10/Mod-11) since checksum arithmetic is more idiomatic
    Python-side than in Postgres.

    Conforms to the `EntityExtractor` Protocol — drop-in
    alongside `RegexEntityExtractor` and `NoOpExtractor`.
    """

    def __init__(
        self,
        conn: "psycopg.Connection",
        catalog_version_id: int,
        *,
        confidence: float = 0.95,
    ) -> None:
        # Inline import: psycopg is a substrate-wide dep, but we
        # keep the type annotation as a forward ref to avoid
        # imposing it at module import time.
        if not (0.0 <= float(confidence) <= 1.0):
            raise ValueError(
                f"SqlIdentifierExtractor confidence must be in "
                f"[0, 1]; got {confidence!r}"
            )
        self._conn = conn
        self._cv = catalog_version_id
        self._confidence = float(confidence)

    def extract(self, text: str) -> list[ExtractedEntity]:
        if not text:
            return []
        # Lazy import: validators ship in the
        # okg.substrate.library.extraction tree which itself has
        # no other imports at module top.
        from okg.substrate.library.extraction.validators import validate

        with self._conn.cursor() as cur:
            cur.execute(
                """
                SELECT m.id_type, m.value, m.span_start, m.span_end,
                       p.validator_kind
                  FROM okg.find_identifier_mentions(%s, %s) m
                  LEFT JOIN okg.identifier_patterns p
                    ON p.id_type = m.id_type
                   AND p.catalog_version_id = %s
                """,
                (text, self._cv, self._cv),
            )
            rows = cur.fetchall()

        out: list[ExtractedEntity] = []
        for id_type, value, span_start, span_end, validator_kind in rows:
            if not validate(value, validator_kind):
                continue
            out.append(ExtractedEntity(
                type=id_type, value=value,
                start=span_start, end=span_end,
                confidence=self._confidence,
            ))
        return out
