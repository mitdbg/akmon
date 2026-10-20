"""DOI structural validation.

DOIs don't carry a true checksum (registrar-issued; no in-band
parity). This validator confirms structural shape — `10.<reg>/<suffix>`
with a 4-9-digit registrant and a non-empty suffix — beyond what
the substrate regex pattern already guarantees, and trims trailing
punctuation a regex might have over-matched. It is offline: it never
checks that a DOI resolves.
"""

from __future__ import annotations

import re

# Trailing chars often grabbed by a too-greedy DOI regex inside
# free text — sentence-final punctuation.
_TRAILING_TRIM = re.compile(r"[.,;:)\]}>'\"\s]+$")


def normalize_doi(value: str) -> str:
    """Strip trailing punctuation, lowercase the prefix `10.`
    (DOI is case-insensitive but conventionally written
    case-preserving for the suffix). Useful for dedupe."""
    return _TRAILING_TRIM.sub("", value).strip()


def is_valid_doi(value: str) -> bool:
    """Structural check: `10.<digits>/<non-empty>`.

    Does NOT verify the DOI resolves; that's an online check
    done by the citation layer.
    """
    v = normalize_doi(value)
    if not v.startswith("10."):
        return False
    body = v[3:]
    slash = body.find("/")
    if slash < 0:
        return False
    registrant = body[:slash]
    suffix = body[slash + 1:]
    if not (4 <= len(registrant) <= 9 and registrant.isdigit()):
        return False
    if not suffix:
        return False
    return True
