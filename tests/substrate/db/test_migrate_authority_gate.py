"""The migrate authority gate is fail-closed for every DSN form.

Covers req.schema-migration-hardening.authority-gate-conninfo (audit F5):

  1. key=value conninfo DSNs resolve to the same deployment as URL DSNs (the
     old urlparse-based lookup extracted no dbname from conninfo form, so
     dropping the URL form silently bypassed the gate).
  2. Fail-closed: a DSN that fails conninfo parsing, a deployments-root read
     failure, or an unreadable deployment manifest each raise the typed
     `MigrateAuthorityUnresolvable` on a gated operation — migrate never
     proceeds ungated on a lookup FAILURE. (The defect was the
     `except Exception: return None` degradation, not just the urlparse
     call.)
  3. A bypass attempt with the conninfo form against a gated deployment
     database is refused by the same provenance gate as the URL form.
  4. The legitimate ungated outcomes stay ungated: a database that matches
     no registered deployment, and an absent (not failed) deployments root.

All tests are hermetic — every refusal fires before any DB connection.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.db import migrate as migrate_mod
from okg.substrate.db.migrate import _deployment_for_database, migrate
from okg.substrate.errors import MigrateAuthorityUnresolvable, SubstrateError

GATED_DB = "okg_gate_test_db"


@pytest.fixture()
def deployments_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Path:
    root = tmp_path / "deployments"
    dep = root / "gated-dep"
    dep.mkdir(parents=True)
    (dep / "deployment.yaml").write_text(
        "name: gated-dep\n"
        "postgres:\n"
        f"  database: {GATED_DB}\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    return root


# --------------------------------------------------------------------------
# 1. Both DSN forms resolve identically.
# --------------------------------------------------------------------------


def test_url_and_conninfo_dsn_forms_resolve_to_same_deployment(
    deployments_root: Path,
) -> None:
    url_form = f"postgres://user:pw@127.0.0.1:5433/{GATED_DB}"
    conninfo_form = f"dbname={GATED_DB} host=127.0.0.1 port=5433 user=user"
    assert _deployment_for_database(url_form) == "gated-dep"
    assert _deployment_for_database(conninfo_form) == "gated-dep", (
        "key=value conninfo DSN bypassed the deployment reverse-mapping"
    )


def test_dbname_default_resolution_mirrors_libpq(
    deployments_root: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # No dbname in the DSN: libpq falls back to PGDATABASE, then the user.
    monkeypatch.setenv("PGDATABASE", GATED_DB)
    assert _deployment_for_database("host=127.0.0.1 user=alice") == "gated-dep"
    monkeypatch.delenv("PGDATABASE")
    assert (
        _deployment_for_database(f"host=127.0.0.1 user={GATED_DB}")
        == "gated-dep"
    )


def test_os_login_name_is_the_last_default_step(
    deployments_root: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # libpq's final fallback for both user and dbname is the OS login name;
    # a gated database named after the operator cannot be reached ungated
    # through a bare `host=...` DSN.
    monkeypatch.delenv("PGDATABASE", raising=False)
    monkeypatch.delenv("PGUSER", raising=False)
    monkeypatch.setattr(migrate_mod, "_os_login_name", lambda: GATED_DB)
    assert _deployment_for_database("host=127.0.0.1") == "gated-dep"


def test_service_dsn_without_dbname_is_fail_closed(
    deployments_root: Path,
) -> None:
    # A pg_service.conf definition can name ANY database; the gate does not
    # expand service files, so it must refuse rather than guess or ungate.
    with pytest.raises(MigrateAuthorityUnresolvable):
        _deployment_for_database("service=someservice")


# --------------------------------------------------------------------------
# 2. Fail-closed refusals, asserted by exception TYPE.
# --------------------------------------------------------------------------


def test_unparseable_dsn_raises_typed_refusal_from_migrate(
    deployments_root: Path,
) -> None:
    with pytest.raises(MigrateAuthorityUnresolvable):
        migrate("this is not a dsn at all")


def test_deployments_root_read_failure_raises_typed_refusal(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    # A CONFIGURED-but-broken root is a read failure, not an absent root:
    # find_deployments_root raises, and migrate must fail closed rather than
    # proceed ungated.
    monkeypatch.setenv(
        "OKG_DEPLOYMENTS_DIR", str(tmp_path / "does-not-exist"),
    )
    with pytest.raises(MigrateAuthorityUnresolvable):
        migrate("dbname=anything host=127.0.0.1 port=5433")


def test_unreadable_deployment_manifest_is_fail_closed(
    deployments_root: Path,
) -> None:
    # A manifest that cannot be parsed could be the very one declaring this
    # database; skipping it would ungate. Refuse instead.
    broken = deployments_root / "broken-dep"
    broken.mkdir()
    (broken / "deployment.yaml").write_text(
        ":::: {{{{ not yaml", encoding="utf-8",
    )
    with pytest.raises(MigrateAuthorityUnresolvable):
        _deployment_for_database(f"dbname={GATED_DB} host=127.0.0.1")


# --------------------------------------------------------------------------
# 3. The conninfo-form bypass attempt hits the SAME provenance gate.
# --------------------------------------------------------------------------


def test_conninfo_dsn_against_gated_database_is_provenance_gated(
    deployments_root: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    import okg.substrate.deployment_authority as authority_mod

    calls: list[tuple[str, str]] = []

    def _refusing_provenance(deployment, dsn, *, file_derived=False):
        calls.append((deployment, dsn))
        return {
            "allowed": False,
            "mode": "foreign_checkout",
            "reason": "test refusal",
            "legal_moves": ["run from the sync root"],
        }

    monkeypatch.setattr(
        authority_mod, "check_publish_provenance", _refusing_provenance,
    )
    conninfo_form = f"dbname={GATED_DB} host=127.0.0.1 port=5433"
    with pytest.raises(SubstrateError, match="refusing migrate"):
        migrate(conninfo_form)
    assert calls == [("gated-dep", conninfo_form)], (
        "the conninfo-form DSN did not reach the provenance gate"
    )


# --------------------------------------------------------------------------
# 4. Legitimate ungated outcomes stay ungated (documented, not accidental).
# --------------------------------------------------------------------------


def test_unmatched_database_is_ungated(deployments_root: Path) -> None:
    assert _deployment_for_database("dbname=okg_unrelated host=h") is None


def test_absent_deployments_root_is_ungated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An empty EXISTING root: no deployments registered -> nothing to gate.
    empty_root = tmp_path / "deployments"
    empty_root.mkdir()
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(empty_root))
    assert _deployment_for_database("dbname=okg_whatever host=h") is None


def test_module_no_longer_uses_urlparse_for_the_gate() -> None:
    """The urlparse-based dbname extraction was the bypass (audit F5); the
    gate must resolve via libpq's conninfo parsing, which the shared DSN
    module's `parse` wraps."""
    import inspect

    source = inspect.getsource(migrate_mod._deployment_for_database)
    source += inspect.getsource(migrate_mod._database_from_dsn)
    assert "urlparse(" not in source, "the gate still CALLS urlparse"
    assert "import urlparse" not in source
    assert "dsn_mod.parse(" in source
