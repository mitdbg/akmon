"""Fuzzy alias resolution — two distinctly-named functions, no
env-dependent dispatch.

`sql_fuzzy_resolve(conn, session, needle)` calls the substrate's
`okg.fuzzy_resolve_alias` SQL cascade (exact → trigram →
Levenshtein) shipped in `add-substrate-pg-fuzzy-and-identifiers`.

`local_fuzzy_resolve(needle, candidates)` runs `rapidfuzz` over
an explicit candidate list. No SQL, no substrate session.

The caller picks by name. There is intentionally no single
function that auto-dispatches between the two — silent fallback
is a P0.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

import psycopg

from okg.substrate.mcp.session import Session, assert_session_matches


@dataclass(frozen=True)
class FuzzyMatch:
    """One alias-resolution hit."""

    canonical: str
    similarity: float
    method: str  # 'exact' | 'trigram' | 'levenshtein' | 'rapidfuzz'


def sql_fuzzy_resolve(
    conn: psycopg.Connection,
    session: Session,
    needle: str,
    *,
    threshold: float = 0.6,
    limit: int = 5,
) -> list[FuzzyMatch]:
    """Substrate-bound fuzzy alias resolution. Calls
    `okg.fuzzy_resolve_alias(needle, catalog_version_id, threshold,
    limit)` and returns the cascade result.

    The session pin determines `catalog_version_id`. Fingerprint
    is asserted before the read so a stale conn↔session pair
    fails loudly.
    """
    if not isinstance(needle, str) or not needle:
        raise ValueError("sql_fuzzy_resolve requires a non-empty needle")
    assert_session_matches(conn, session)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT canonical, similarity, method "
            "FROM okg.fuzzy_resolve_alias(%s, %s, %s::real, %s::int)",
            (needle, session.catalog_version_id, threshold, limit),
        )
        return [
            FuzzyMatch(canonical=r[0], similarity=float(r[1]), method=r[2])
            for r in cur.fetchall()
        ]


def local_fuzzy_resolve(
    needle: str,
    candidates: Iterable[str],
    *,
    threshold: float = 0.6,
    limit: int = 5,
    canonical_for: Optional[dict[str, str]] = None,
) -> list[FuzzyMatch]:
    """Off-substrate fuzzy resolution via rapidfuzz.

    `candidates` is the explicit list to score. `canonical_for`
    optionally maps each candidate to its canonical node id; when
    omitted, the candidate string IS its own canonical.

    Score is rapidfuzz `WRatio` divided by 100 (rapidfuzz returns
    [0, 100]; we normalize to [0, 1] to match the SQL cascade's
    range).
    """
    if not isinstance(needle, str) or not needle:
        raise ValueError("local_fuzzy_resolve requires a non-empty needle")
    cand_list = list(candidates)
    if not cand_list:
        return []
    from rapidfuzz import fuzz, process

    canonical_for = canonical_for or {}

    # Use rapidfuzz.process.extract for top-N selection.
    scored = process.extract(
        needle, cand_list, scorer=fuzz.WRatio, limit=limit,
    )
    out: list[FuzzyMatch] = []
    for cand, score, _idx in scored:
        sim = float(score) / 100.0
        if sim < threshold:
            continue
        canonical = canonical_for.get(cand, cand)
        out.append(FuzzyMatch(
            canonical=canonical,
            similarity=sim,
            method="rapidfuzz",
        ))
    return out


__all__ = ["FuzzyMatch", "local_fuzzy_resolve", "sql_fuzzy_resolve"]
