"""ORCID Mod-11 ISO/IEC 7064 checksum validation.

ORCID format: 4 dashed groups of 4 chars, total 16 chars where
the last is the check digit. The check is computed as a Mod-11
running total, with `X` representing 10.

Algorithm (per ORCID and ISO/IEC 7064 :2003):
    total = 0
    for d in first 15 digits:
        total = (total + d) * 2
    result = (12 - total % 11) % 11
    check  = 'X' if result == 10 else str(result)

This is the same algorithm widely used for ISNI, GNI, etc.
"""

from __future__ import annotations

import re

_ORCID_RE = re.compile(r"^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$")


def is_valid_orcid(value: str) -> bool:
    """Validate format + checksum. Accepts the canonical
    `0000-0000-0000-000X` form only — no whitespace tolerance,
    no missing dashes (the regex pattern in
    `okg.identifier_patterns` is what canonicalizes inputs)."""
    if not _ORCID_RE.match(value):
        return False
    digits = value.replace("-", "")
    body = digits[:-1]
    check = digits[-1]
    total = 0
    for c in body:
        total = (total + int(c)) * 2
    result = (12 - (total % 11)) % 11
    expected = "X" if result == 10 else str(result)
    return check == expected
