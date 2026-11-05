"""Agreement between two labellings of the same misses: Cohen's kappa (issue #4281).

Both inputs map a miss key to a label. Only keys present in both, with a
label from ``classes`` in both, are compared, and the report says how many
were dropped from each side, so a coverage gap is never hidden inside a
kappa. Kappa is computed exactly as the hand census computed its 0.66:
observed agreement p_a against chance agreement p_e from the two sides'
marginal shares, (p_a - p_e) / (1 - p_e).
"""
from __future__ import annotations

from collections import Counter
from collections.abc import Hashable, Iterable, Mapping
from typing import Any

#: The hand census's mechanism classes (lane re, 2026-10-27).
CENSUS_CLASSES: Mapping[str, str] = {
    "a": "never reached",
    "b": "reply hid it",
    "c": "seen, dropped",
    "d": "seen, misread",
    "e": "not in graph or source",
    "f": "tool error or refusal",
    "g": "answer key or judge",
}

SCHEMA = "okg.akb.agreement.v1"


def cohen_kappa(a: Mapping[Hashable, str], b: Mapping[Hashable, str],
                classes: Iterable[str] = CENSUS_CLASSES) -> dict[str, Any]:
    allowed = set(classes)
    keys = [k for k in a if k in b and a[k] in allowed and b[k] in allowed]
    pairs = [(a[k], b[k]) for k in keys]
    n = len(pairs)
    agree = sum(x == y for x, y in pairs)
    pa = agree / n if n else 0.0
    ca, cb = Counter(x for x, _ in pairs), Counter(y for _, y in pairs)
    pe = sum(ca[c] * cb[c] for c in allowed) / (n * n) if n else 0.0
    kappa = (pa - pe) / (1 - pe) if n and pe < 1 else None
    confusion = Counter(f"{x}->{y}" for x, y in pairs if x != y)
    return {
        "schema": SCHEMA,
        "compared": n,
        "agree": agree,
        "observed": round(pa, 4),
        "chance": round(pe, 4),
        "kappa": None if kappa is None else round(kappa, 4),
        "only_in_a": sum(1 for k in a if k not in b),
        "only_in_b": sum(1 for k in b if k not in a),
        "unlabelled_or_other": sum(1 for k in a if k in b) - n,
        "marginals": {"a": dict(sorted(ca.items())), "b": dict(sorted(cb.items()))},
        "disagreements": dict(confusion.most_common()),
    }


def labels_from_rows(rows: Iterable[Mapping[str, Any]], key_fields: Iterable[str],
                     label_field: str) -> dict[tuple[Any, ...], str]:
    """``{(row[f] for f in key_fields): row[label_field]}``; a repeated key keeps the last row."""
    fields = tuple(key_fields)
    out: dict[tuple[Any, ...], str] = {}
    for row in rows:
        label = row.get(label_field)
        if isinstance(label, str):
            out[tuple(str(row[f]) for f in fields)] = label
    return out


__all__ = ["CENSUS_CLASSES", "SCHEMA", "cohen_kappa", "labels_from_rows"]
