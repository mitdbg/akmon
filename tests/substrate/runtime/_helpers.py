"""Cross-test helpers for substrate-runtime tests.

Library-style helpers shared across the runtime test files
(SDK, escalation, worker, embed, provenance). pytest's
`conftest.py` is reserved for fixture discovery; helpers that
are imported as module-level functions belong here.
"""
from __future__ import annotations

from tests.substrate.conftest import dsn_with_dbname


def _swap_dbname(base: str, db_name: str) -> str:
    """Return a DSN derived from ``base`` with the database name
    replaced by ``db_name``. Tolerates both URL-style
    (`postgres://...`) and keyword-style
    (`user=... dbname=... host=... port=...`) inputs, and always
    RETURNS the URL form.

    Per `fix-dev-merge-test-debt` Root Cause 1: the original
    per-file pattern was `head, _, _ = base.rpartition("/")`
    then `f"{head}/{db_name}"` which returned `'/db_name'` when
    the input was keyword-style. Using the psycopg conninfo
    helpers canonicalizes both forms.

    It used to return `make_conninfo`'s keyword form, which
    Chronos' `connect_sql_database` reads as a SQLite path
    because it selects its backend by URL scheme -- the trap
    `_url_dsn` in `test_embed_workflow.py` was written to work
    around. `dsn_with_dbname` returns a URL, so callers that
    hand the result to a graph store no longer have to.
    """

    return dsn_with_dbname(base, db_name)
