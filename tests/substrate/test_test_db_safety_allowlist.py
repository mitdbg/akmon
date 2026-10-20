"""The test-DB safety allowlist admits disposables and nothing else.

`_is_safe_test_dbname` decides which databases the fixtures may DROP and
recreate, so widening it is exactly the kind of change that deserves its
own test. It was widened to admit the isolated okg-workspace clone, whose
name must begin with ``okg_workspace_`` for deployment-authority to resolve
it as an isolated clone — meaning it could never satisfy the ``okg_test*``
rule, so every fixture touching that clone refused on every local run and
the only way through was ``OKG_TEST_DSN_TRUST_AS_EPHEMERAL=1``, which
disables the guard globally.

The risk of widening is admitting a REAL database, so most of what follows
is about what must stay refused.
"""
from __future__ import annotations

import pytest

from tests.substrate.conftest import (
    _ISOLATED_DB_PREFIX,
    _is_safe_test_dbname,
    _isolated_okg_workspace_db_name,
)


@pytest.mark.parametrize(
    "dbname",
    [
        "okg_test",
        "okg_test_gw0",
        "okg_test_entryroles",
        f"{_ISOLATED_DB_PREFIX}",
        f"{_ISOLATED_DB_PREFIX}_okg_test",
        f"{_ISOLATED_DB_PREFIX}_okg_test_gw3",
    ],
)
def test_disposable_names_are_allowed(dbname: str) -> None:
    assert _is_safe_test_dbname(dbname) is True


@pytest.mark.parametrize(
    "dbname",
    [
        # The real graph databases this guard exists to protect.
        "okg",
        "okg_workspace",
        "okg_workspace_4dc57f2f0a",
        "okg_ci",
        "okg_kdflex",
        "postgres",
        # Near-misses: a prefix that merely CONTAINS the marker, or shares a
        # leading fragment, must not be admitted.
        "okg_workspace_test",
        "okg_workspace_isolated",
        "not_okg_test",
        "okgtest",
        "okg_tes",
        None,
        "",
    ],
)
def test_real_databases_stay_refused(dbname: str | None) -> None:
    assert _is_safe_test_dbname(dbname) is False


def test_the_generated_clone_name_is_allowed() -> None:
    """The name the fixture actually mints must pass its own guard.

    This is the regression the widening fixes: the builder and the guard
    disagreed, so the fixture refused the very database it had just been
    asked to prepare.
    """
    generated = _isolated_okg_workspace_db_name(
        "postgres://postgres:okg@127.0.0.1:5433/okg_test"
    )
    assert generated.startswith(_ISOLATED_DB_PREFIX)
    assert _is_safe_test_dbname(generated) is True


def test_clone_name_is_derived_per_base_dsn() -> None:
    """Distinct base DSNs get distinct clones, so xdist workers don't collide."""
    a = _isolated_okg_workspace_db_name(
        "postgres://postgres:okg@127.0.0.1:5433/okg_test_gw0"
    )
    b = _isolated_okg_workspace_db_name(
        "postgres://postgres:okg@127.0.0.1:5433/okg_test_gw1"
    )
    assert a != b
    assert _is_safe_test_dbname(a) and _is_safe_test_dbname(b)
