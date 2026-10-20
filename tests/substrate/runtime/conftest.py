"""Shared fixtures for substrate-runtime tests.

Provides a single `admin_dsn` fixture that derives an admin
connection string (with `dbname=postgres`) from the configured
test DSN, regardless of whether the input is URL-style
(`postgres://user:pw@host:port/db`) or keyword-style
(`user=... dbname=... host=... port=...`).

Helper functions live in `_helpers.py` so test files import
them directly without depending on conftest module discovery.
"""
from __future__ import annotations

import pytest

from tests.substrate.conftest import require_okg_test_dsn
from tests.substrate.runtime._helpers import _swap_dbname


@pytest.fixture
def admin_dsn() -> str:
    """Return an admin DSN (`dbname=postgres`) for the configured test
    database. Tolerates both URL-style and keyword-style input formats.

    This fixture's consumers connect as superuser and DROP/CREATE
    databases, so it must never guess at a server. It resolved
    `OKG_TEST_DSN` with a hardcoded fallback to the shared `okg-pg`
    Compose service and ignored `OKG_TEST_DSN_TEMPLATE` entirely
    (issue #1913); it now shares the substrate resolver and skips when
    nothing is configured.
    """

    return _swap_dbname(require_okg_test_dsn(), "postgres")
