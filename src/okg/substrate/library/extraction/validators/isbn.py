"""ISBN-10 / ISBN-13 checksum validation.

Hand-rolled (no `isbnlib`) to avoid LGPL v3 licence cascade.

ISBN-10 checksum: weighted sum of digits with weights 10, 9, 8,
..., 1, modulo 11. Last digit may be 'X' representing 10.
ISBN-13 checksum: weighted sum with alternating 1, 3, 1, 3 ...,
modulo 10. Last digit must equal `(10 - sum_of_first_12 % 10) %
10` (or equivalently the whole sum mod 10 must be 0).
"""

from __future__ import annotations


def _strip(value: str) -> str:
    return value.replace("-", "").replace(" ", "")


def _is_valid_isbn10(s: str) -> bool:
    if len(s) != 10:
        return False
    total = 0
    for i, c in enumerate(s):
        if c == "X":
            if i != 9:
                return False
            d = 10
        elif c.isdigit():
            d = int(c)
        else:
            return False
        total += d * (10 - i)
    return total % 11 == 0


def _is_valid_isbn13(s: str) -> bool:
    if len(s) != 13:
        return False
    if not s.isdigit():
        return False
    total = 0
    for i, c in enumerate(s):
        d = int(c)
        total += d * (1 if i % 2 == 0 else 3)
    return total % 10 == 0


def is_valid_isbn(value: str) -> bool:
    """Validate ISBN-10 or ISBN-13 by length + checksum after
    stripping dashes and spaces."""
    digits = _strip(value)
    if len(digits) == 10:
        return _is_valid_isbn10(digits)
    if len(digits) == 13:
        return _is_valid_isbn13(digits)
    return False
