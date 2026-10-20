"""An unset test DSN must skip, never fall back to a shared database.

Issue #1913: the fixtures resolved ``OKG_TEST_DSN``, then
``OKG_TEST_DSN_TEMPLATE``, then a hardcoded
``postgres://postgres:okg@127.0.0.1:5433/okg_test`` -- the shared ``okg-pg``
Compose service. On any machine where that service is up (every maintainer
workstation), a contributor who forgot the variable did not get a skip: the
fixtures truncated the runtime tables of the database other sessions were
using.

These tests never open a connection. They drive the resolver and the fixture
functions with the environment cleared, which is exactly the contributor's
situation, and assert the outcome is a skip naming the variable to set.
"""
from __future__ import annotations

import inspect
import sys
import types

import pytest

import tests.conftest as repo_conftest
import tests.pact.conftest as pact_conftest
import tests.substrate.conftest as substrate_conftest
import tests.substrate.runtime.conftest as runtime_conftest
import tests.views.conftest as views_conftest


_DSN_ENV_VARS = ("OKG_TEST_DSN", "OKG_TEST_DSN_TEMPLATE")


def _clear_dsn_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _DSN_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def test_resolver_returns_none_when_no_dsn_variable_is_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No variable set means no DSN -- not a guess at the Compose service."""
    _clear_dsn_env(monkeypatch)

    assert substrate_conftest._resolve_okg_test_dsn() is None


def test_explicit_dsn_is_still_honoured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("OKG_TEST_DSN", "postgres://u:p@127.0.0.1:5661/okg_test_x")
    monkeypatch.delenv("OKG_TEST_DSN_TEMPLATE", raising=False)

    resolved = substrate_conftest._resolve_okg_test_dsn()

    assert resolved == "postgres://u:p@127.0.0.1:5661/okg_test_x"


def test_template_is_still_honoured(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OKG_TEST_DSN", raising=False)
    monkeypatch.setenv(
        "OKG_TEST_DSN_TEMPLATE",
        "postgres://u:p@127.0.0.1:5661/okg_test_{workerid}",
    )
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw7")

    resolved = substrate_conftest._resolve_okg_test_dsn()

    assert resolved == "postgres://u:p@127.0.0.1:5661/okg_test_gw7"


def test_okg_dsn_fixture_skips_when_no_dsn_variable_is_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The session fixture skips, and its reason names the variable to set."""
    _clear_dsn_env(monkeypatch)

    fixture_fn = substrate_conftest.okg_dsn.__wrapped__

    with pytest.raises(pytest.skip.Exception) as excinfo:
        fixture_fn()

    assert "OKG_TEST_DSN" in str(excinfo.value)


def test_dbos_runtime_fixture_skips_when_no_dsn_variable_is_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The repo-wide DBOS fixture carried the same hardcoded fallback."""
    _clear_dsn_env(monkeypatch)

    fixture_fn = substrate_conftest.dbos_test_runtime.__wrapped__

    with pytest.raises(pytest.skip.Exception) as excinfo:
        next(iter(fixture_fn()))

    assert "OKG_TEST_DSN" in str(excinfo.value)


def test_dbos_runtime_fixture_uses_the_worker_template_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """CI sets only OKG_TEST_DSN_TEMPLATE. The DBOS fixture must resolve this
    worker's database from it, as `clean_runtime` does, instead of skipping;
    reading OKG_TEST_DSN alone skipped every dbos_integration test nightly."""
    _clear_dsn_env(monkeypatch)
    monkeypatch.setenv(
        "OKG_TEST_DSN_TEMPLATE", "postgres://u:p@127.0.0.1:5661/okg_test_{workerid}"
    )
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw3")
    seen: dict[str, object] = {}

    class _FakeDBOS:
        def __init__(self, config: dict[str, object]) -> None:
            seen.update(config)

        @staticmethod
        def destroy() -> None:
            pass

        @staticmethod
        def reset_system_database() -> None:
            seen["reset"] = True

        @staticmethod
        def launch() -> None:
            pass

    fake_dbos = types.ModuleType("dbos")
    fake_dbos.DBOS = _FakeDBOS  # type: ignore[attr-defined]
    fake_dbos.DBOSConfig = dict  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "dbos", fake_dbos)

    runtime = substrate_conftest.dbos_test_runtime.__wrapped__()
    try:
        next(runtime)
    except pytest.skip.Exception as skipped:
        pytest.fail(f"the fixture skipped with the template set: {skipped}")
    runtime.close()

    expected = "postgres://u:p@127.0.0.1:5661/okg_test_gw3"
    assert seen["system_database_url"] == expected
    assert seen["database_url"] == expected
    assert seen["reset"] is True


def test_dbos_runtime_fixture_refuses_a_non_test_database(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DBOS.reset_system_database() drops the whole database the DSN names,
    so the fixture refuses a name outside okg_test_* before resetting it."""
    _clear_dsn_env(monkeypatch)
    monkeypatch.delenv("OKG_TEST_DSN_TRUST_AS_EPHEMERAL", raising=False)
    monkeypatch.setenv("OKG_TEST_DSN", "postgres://u:p@127.0.0.1:5661/okg_dev")

    def _no_dbos(name: str) -> None:
        raise AssertionError(f"reached importorskip({name!r}) before refusing")

    monkeypatch.setattr(pytest, "importorskip", _no_dbos)

    with pytest.raises(pytest.exit.Exception) as excinfo:
        next(iter(substrate_conftest.dbos_test_runtime.__wrapped__()))

    assert "okg_dev" in str(excinfo.value)


def test_pact_resolver_returns_none_when_no_dsn_variable_is_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`tests/pact` carried its own copy of the same fallback."""
    _clear_dsn_env(monkeypatch)

    assert pact_conftest._resolve_test_dsn() is None


def test_pact_setup_reports_missing_dsn_without_connecting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The unavailability probe must not connect when nothing is declared.

    It calls `_ensure_test_database`, which CREATEs — so a probe against a
    guessed DSN is itself the hazard.
    """
    _clear_dsn_env(monkeypatch)
    monkeypatch.setattr(pact_conftest, "TEST_DSN", None)

    def _fail(*args: object, **kwargs: object) -> None:
        raise AssertionError("probed a database with no DSN configured")

    monkeypatch.setattr(pact_conftest, "_ensure_test_database", _fail)

    reason = pact_conftest._postgres_unavailable_reason()

    assert reason is not None
    assert "OKG_TEST_DSN" in reason


def test_admin_dsn_fixture_skips_when_no_dsn_variable_is_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The runtime admin DSN drives DROP/CREATE as superuser.

    It resolved OKG_TEST_DSN with its own fallback and ignored
    OKG_TEST_DSN_TEMPLATE entirely.
    """
    _clear_dsn_env(monkeypatch)

    fixture_fn = runtime_conftest.admin_dsn.__wrapped__

    with pytest.raises(pytest.skip.Exception) as excinfo:
        fixture_fn()

    assert "OKG_TEST_DSN" in str(excinfo.value)


def test_views_reuses_the_substrate_dsn_fixture() -> None:
    """One resolver, so there is one place that can regress.

    The views override returned the resolver's value directly, so after the
    resolver learned to answer None it handed None to psycopg.
    """
    assert views_conftest.okg_dsn is substrate_conftest.okg_dsn


def test_require_helper_skips_rather_than_returning_none(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Callers pass the result straight to psycopg, so None is not an option."""
    _clear_dsn_env(monkeypatch)

    with pytest.raises(pytest.skip.Exception) as excinfo:
        substrate_conftest.require_okg_test_dsn()

    assert "OKG_TEST_DSN" in str(excinfo.value)


@pytest.mark.parametrize(
    "module",
    [
        substrate_conftest,
        repo_conftest,
        pact_conftest,
        runtime_conftest,
        views_conftest,
    ],
    ids=[
        "substrate-conftest",
        "repo-conftest",
        "pact-conftest",
        "substrate-runtime-conftest",
        "views-conftest",
    ],
)
def test_conftest_source_holds_no_shared_database_fallback(module) -> None:
    """No literal reintroduces the shared Compose DSN as a default.

    A future edit that writes the fallback back in as a literal -- rather
    than through the resolver -- would pass the behavioural tests above,
    so read the source too. `tests/substrate/test_dsn_env_guard.py` carries
    the same rule over the whole `tests/` tree; this stays because these
    five files are the ones the fixtures actually resolve through.
    """
    source = inspect.getsource(module)

    offending = [
        line
        for line in source.splitlines()
        if "postgres://" in line
        and "5433/okg_test" in line
        and "OKG_TEST_DSN_TEMPLATE=" not in line
    ]

    assert offending == []
