"""Load and claim an in-tree deployment's catalog on the test database.

Some database tests publish through an in-tree deployment (for example
``deployments/okg-ci``) with that deployment's real catalog identity. Its
catalog must then be claimed and loaded the way an operator does it, with
``okg catalog ownership claim`` and ``okg catalog load --apply``.

Those commands resolve their database from the deployment's own
``postgres.dsn``, which names an environment variable. This helper points
that variable at the test database for the duration of the test, then checks
with the command's own resolver that the command would connect to the test
database before it runs anything. A copied deployment directory would not do:
the copy has a different writer identity, and it sits outside the repository,
which the publisher refuses as a provenance source.
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pytest

from okg.substrate import fastyaml

_SINGLE_ENV_REFERENCE = re.compile(r"^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$")


def _dbname(dsn: str) -> str | None:
    import psycopg

    return psycopg.conninfo.conninfo_to_dict(dsn).get("dbname")


def point_deployment_at_test_database(
    deployment_dir: Path,
    test_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
) -> str:
    """Point the deployment's DSN variable at ``test_dsn`` and prove it.

    Returns the name of the environment variable that was set. Fails the test
    (never skips) when the manifest's DSN is not a single variable reference,
    when the test DSN is not the configured test database, or when the CLI's
    own resolvers would connect anywhere other than ``test_dsn``.
    """
    from okg.substrate.cli.main import _resolve_deployment_dsn_from_dir
    from okg.substrate.deployments import require_postgres_dsn, resolve_deployment
    from tests.substrate.conftest import (
        _is_safe_test_dbname,
        require_okg_test_dsn,
    )

    manifest = fastyaml.safe_load(
        (deployment_dir / "deployment.yaml").read_text(encoding="utf-8")
    ) or {}
    raw_dsn = (manifest.get("postgres") or {}).get("dsn")
    match = _SINGLE_ENV_REFERENCE.match(str(raw_dsn or "").strip())
    if match is None:
        pytest.fail(
            f"{deployment_dir}/deployment.yaml postgres.dsn is {raw_dsn!r}; "
            "this helper only redirects a DSN that is one ${VAR} reference"
        )
    variable = match.group(1)

    configured = require_okg_test_dsn()
    target = _dbname(test_dsn)
    # pytest.fail, not assert: the guard must hold under python -O too.
    if not _is_safe_test_dbname(target):
        pytest.fail(
            f"refusing to point {deployment_dir.name} at database {target!r}: "
            "not a disposable test database"
        )
    if target != _dbname(configured):
        pytest.fail(
            f"refusing to point {deployment_dir.name} at database {target!r}: "
            f"the configured test database is {_dbname(configured)!r}"
        )

    monkeypatch.setenv(variable, test_dsn)
    via_path = _resolve_deployment_dsn_from_dir(
        argparse.Namespace(dsn=None), deployment_dir,
    )
    via_name = require_postgres_dsn(
        resolve_deployment(str(deployment_dir)),
        deployment=deployment_dir.name,
    )
    if via_path != test_dsn:
        pytest.fail(
            f"`okg catalog --deployment {deployment_dir}` would connect to "
            f"database {_dbname(via_path)!r}, not the test database {target!r}"
        )
    if via_name != test_dsn:
        pytest.fail(
            f"deployment {deployment_dir.name} resolves to database "
            f"{_dbname(via_name)!r}, not the test database {target!r}"
        )
    return variable


def load_and_claim_deployment_catalog(
    deployment_dir: Path,
    test_dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> dict:
    """Run ``okg catalog ownership claim`` then ``okg catalog load --apply``.

    The deployment's DSN is first pointed at, and checked against, the test
    database. The claim comes first because a database that already holds
    catalog history refuses an unclaimed load
    (``catalog_ownership_unclaimed``); this is the operator's order for a
    database that another writer's fixtures seeded. Returns the load
    command's JSON report.
    """
    from okg.substrate.cli.main import main

    point_deployment_at_test_database(deployment_dir, test_dsn, monkeypatch)

    capsys.readouterr()
    claimed_rc = main([
        "--json", "catalog", "ownership", "claim",
        "--deployment", str(deployment_dir),
    ])
    claimed = capsys.readouterr()
    assert claimed_rc == 0, claimed.out + claimed.err

    loaded_rc = main([
        "--json", "catalog", "load",
        "--deployment", str(deployment_dir), "--apply",
    ])
    loaded = capsys.readouterr()
    assert loaded_rc == 0, loaded.out + loaded.err
    report = json.loads(loaded.out)
    assert report["applied"] is True, loaded.out
    return report
