"""Issue #2920: Chronos-owned sessions honour okg's idle-in-transaction setting.

okg resolves the idle-in-transaction bound from `OKG_IDLE_IN_TRANSACTION_TIMEOUT_MS`,
then the deployment manifest key `postgres.idle_in_transaction_session_timeout_ms`,
then the 5-minute default. Chronos's adapter factory used the bare default and
ignored both, so an example-kb re-ingest configured for two hours had its Chronos
session killed after five minutes idle (SQLSTATE 25P03).

Every assertion reads the value the SERVER applied (`SHOW
idle_in_transaction_session_timeout`) on the real Chronos session.
"""
from __future__ import annotations

from pathlib import Path
from typing import Iterator

import pytest

from okg.substrate.db import connection as conn_mod
from okg.substrate.errors import SubstrateError


def _show_bound(adapter: object) -> str:
    row = adapter.execute(  # type: ignore[attr-defined]
        "SHOW idle_in_transaction_session_timeout"
    ).fetchone()
    return str(row["idle_in_transaction_session_timeout"])


def _chronos_adapter_bound(dsn: str) -> str:
    from chronos_core.branching.sql_adapters import PostgresDatabaseAdapter

    adapter = PostgresDatabaseAdapter.connect(dsn)
    try:
        return _show_bound(adapter)
    finally:
        adapter.close()


def _chronos_store_bound(dsn: str) -> str:
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.connect(dsn, ensure_schema=False)
    try:
        return _show_bound(store.context.db)
    finally:
        store.close()


@pytest.fixture
def clean_bound_config(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """No env override, no deployment name, no cached manifest value."""
    monkeypatch.delenv(conn_mod.IDLE_IN_TRANSACTION_TIMEOUT_ENV, raising=False)
    monkeypatch.delenv("OKG_DEPLOYMENT", raising=False)
    conn_mod._MANIFEST_IDLE_TIMEOUT_CACHE.clear()
    yield
    conn_mod._MANIFEST_IDLE_TIMEOUT_CACHE.clear()


def _write_deployment(root: Path, name: str, bound_ms: int) -> None:
    dep = root / name
    (dep / "schemas").mkdir(parents=True)
    (dep / "source_registry.yaml").write_text("sources: []\n", encoding="utf-8")
    (dep / "deployment.yaml").write_text(
        f"name: {name}\n"
        "description: test\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n"
        "postgres:\n"
        f"  idle_in_transaction_session_timeout_ms: {bound_ms}\n",
        encoding="utf-8",
    )


def test_okg_installs_its_resolver_for_chronos_sessions() -> None:
    """Importing okg's connection module hands Chronos okg's resolver."""
    from chronos_core import pg_session

    assert (
        pg_session._IDLE_IN_TRANSACTION_TIMEOUT_RESOLVER
        is conn_mod.idle_in_transaction_timeout_ms
    )


@pytest.mark.parametrize(
    "module",
    ["okg.substrate.graph_store", "okg.substrate.graph_store_internal.common"],
)
def test_importing_the_graph_store_alone_installs_the_resolver(
    module: str,
) -> None:
    """A fresh interpreter that imports only the graph store gets the resolver.

    Guards the direct import in `graph_store_internal/common.py`: a refactor
    that drops it would otherwise leave Chronos sessions on the bare default.
    The check reads the installed function by name and never imports
    `okg.substrate.db.connection` itself, since that import would install it.
    """
    import subprocess
    import sys

    code = (
        f"import {module}\n"
        "from chronos_core import pg_session\n"
        "r = pg_session._IDLE_IN_TRANSACTION_TIMEOUT_RESOLVER\n"
        "assert r is not None, 'no resolver installed'\n"
        "assert (r.__module__, r.__name__) == ("
        "'okg.substrate.db.connection', 'idle_in_transaction_timeout_ms'), r\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True, text=True, timeout=120, check=False,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.postgres
def test_chronos_session_keeps_the_default_when_nothing_is_set(
    okg_migrated: str, clean_bound_config: None,
) -> None:
    assert _chronos_adapter_bound(okg_migrated) == "5min"
    assert _chronos_store_bound(okg_migrated) == "5min"


@pytest.mark.postgres
def test_chronos_session_honours_the_env_override(
    okg_migrated: str, clean_bound_config: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The value example-kb runs with: two hours.
    monkeypatch.setenv(conn_mod.IDLE_IN_TRANSACTION_TIMEOUT_ENV, "7200000")

    assert _chronos_adapter_bound(okg_migrated) == "2h"
    assert _chronos_store_bound(okg_migrated) == "2h"

    # 0 emits no setting, leaving the server's own value (0 on the test
    # server) in force -- the same meaning it has for okg-owned sessions.
    monkeypatch.setenv(conn_mod.IDLE_IN_TRANSACTION_TIMEOUT_ENV, "0")
    assert _chronos_adapter_bound(okg_migrated) == "0"


@pytest.mark.postgres
def test_chronos_session_honours_the_manifest_key_and_env_wins(
    okg_migrated: str, clean_bound_config: None,
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    _write_deployment(tmp_path, "idle-bound-2920", 900_000)
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(tmp_path))
    monkeypatch.setenv("OKG_DEPLOYMENT", "idle-bound-2920")

    assert _chronos_adapter_bound(okg_migrated) == "15min"
    assert _chronos_store_bound(okg_migrated) == "15min"

    # Same precedence as okg-owned sessions: the process env beats the manifest.
    monkeypatch.setenv(conn_mod.IDLE_IN_TRANSACTION_TIMEOUT_ENV, "60000")
    assert _chronos_adapter_bound(okg_migrated) == "1min"


@pytest.mark.postgres
def test_chronos_session_refuses_a_malformed_env_override(
    okg_migrated: str, clean_bound_config: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A set-but-invalid value must not silently fall back to the default."""
    from chronos_core.branching.sql_adapters import PostgresDatabaseAdapter

    monkeypatch.setenv(conn_mod.IDLE_IN_TRANSACTION_TIMEOUT_ENV, "two hours")
    with pytest.raises(SubstrateError, match="non-negative integer"):
        PostgresDatabaseAdapter.connect(okg_migrated)
