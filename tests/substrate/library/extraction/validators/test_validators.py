"""Identifier checksum validator tests (M6.7.5).

Pure unit tests — no DB, no fixtures. Each function is small
enough that we can hit known-good and known-bad inputs by hand.
"""
from __future__ import annotations

import pytest

from okg.substrate.library.extraction.validators import validate
from okg.substrate.library.extraction.validators.doi import (
    is_valid_doi,
    normalize_doi,
)
from okg.substrate.library.extraction.validators.isbn import is_valid_isbn
from okg.substrate.library.extraction.validators.orcid import is_valid_orcid


# ---------- DOI ----------------------------------------------------

@pytest.mark.parametrize("value,expected", [
    ("10.1038/nphys1170", True),
    ("10.1234/foo", True),
    ("10.12345/very-long-suffix.with-dots_v2", True),
    # Trailing punctuation gets normalized away
    ("10.1038/nphys1170.", True),
    ("10.1038/nphys1170,", True),
    # Invalid: missing prefix
    ("11.1038/nphys1170", False),
    # Invalid: registrant too short
    ("10.123/x", False),
    # Invalid: empty suffix
    ("10.1234/", False),
    # Invalid: registrant non-digit
    ("10.abcd/x", False),
])
def test_doi_validator(value: str, expected: bool) -> None:
    assert is_valid_doi(value) is expected


def test_doi_normalize() -> None:
    assert normalize_doi("10.1234/foo.") == "10.1234/foo"
    assert normalize_doi("10.1234/foo;)") == "10.1234/foo"
    assert normalize_doi("10.1234/foo") == "10.1234/foo"


# ---------- ORCID --------------------------------------------------

@pytest.mark.parametrize("value,expected", [
    # Valid examples (real test ORCIDs use these check-digit patterns)
    ("0000-0002-1825-0097", True),  # canonical example
    ("0000-0001-5109-3700", True),
    ("0000-0002-1694-233X", True),  # X check digit
    # Invalid checksum
    ("0000-0002-1825-0098", False),
    ("0000-0002-1694-2330", False),
    # Wrong format
    ("0000-0000-0000-000", False),  # too short
    ("0000-0000-0000-00000", False),  # too long
    ("00000000-0000-0000-0001", False),  # wrong dash placement
    ("0000-0000-0000-000Y", False),  # invalid char
])
def test_orcid_validator(value: str, expected: bool) -> None:
    assert is_valid_orcid(value) is expected


# ---------- ISBN ---------------------------------------------------

@pytest.mark.parametrize("value,expected", [
    # Valid ISBN-10
    ("0306406152", True),
    ("0-306-40615-2", True),
    ("020161622X", True),  # X check digit
    # Valid ISBN-13
    ("9780306406157", True),
    ("978-0-306-40615-7", True),
    ("9780201616224", True),
    # Bad ISBN-10 checksum
    ("0306406153", False),
    # Bad ISBN-13 checksum
    ("9780306406158", False),
    # Wrong length
    ("123456789", False),
    ("12345678901234", False),
    # Non-digit (X allowed only at index 9 of an ISBN-10)
    ("030640615X", False),  # bad checksum even though X
    ("978-0-306-4061X-7", False),  # X mid-string
])
def test_isbn_validator(value: str, expected: bool) -> None:
    assert is_valid_isbn(value) is expected


# ---------- Dispatch ----------------------------------------------

def test_validate_dispatch_doi() -> None:
    assert validate("10.1038/nphys1170", "doi_checksum") is True
    assert validate("not-a-doi", "doi_checksum") is False


def test_validate_dispatch_orcid() -> None:
    assert validate("0000-0002-1825-0097", "orcid_checksum") is True
    assert validate("0000-0002-1825-0098", "orcid_checksum") is False


def test_validate_dispatch_isbn() -> None:
    assert validate("9780306406157", "isbn_checksum") is True
    assert validate("9780306406158", "isbn_checksum") is False


def test_validate_dispatch_unknown_kind_is_noop() -> None:
    """Unknown validator kinds return True (don't reject the
    match — the regex already vouched for it)."""
    assert validate("anything", "unknown_kind_42") is True


def test_validate_dispatch_null_kind_is_noop() -> None:
    """NULL validator_kind in the catalog row → no validation."""
    assert validate("anything", None) is True
