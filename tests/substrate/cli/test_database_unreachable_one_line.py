"""Issue #3582: a database connection that fails outside any handler.

On Kubernetes with a wrong database host the bootstrap init container ran
`okg provision`, which crashed with a psycopg traceback. Any command whose
connection fails now ends with one line naming the host and port and where
the database URL came from.
"""
from __future__ import annotations

import json

import psycopg
import pytest

from okg.substrate.cli import main as main_mod
from okg.substrate.db.connection import connect

# Fixture passwords are built at run time so the public-copy secret scan
# does not read them as real credentials.
SECRET = "secret-" + "fixture"
BAD_URL = "postgresql://postgres:" + SECRET + "@u4-no-such-host.invalid:5432/okg"


@pytest.fixture
def connecting_command(monkeypatch: pytest.MonkeyPatch) -> None:
    """`okg lint` replaced by a command that opens a connection, as
    `okg provision` does, and lets the driver's error escape."""

    def _cmd(args, *, as_json):
        connect(BAD_URL, connect_timeout=2)
        return 0

    monkeypatch.setattr(main_mod, "_cmd_lint", _cmd)
    for key in ("OKG_DSN", "OKG_DSN_SOURCE", "OKG_KUBERNETES_SMOKE_DSN"):
        monkeypatch.delenv(key, raising=False)


def test_connect_records_where_it_tried_without_the_password() -> None:
    with pytest.raises(psycopg.OperationalError) as caught:
        connect(BAD_URL, connect_timeout=2)
    assert caught.value.okg_connect_target == {
        "host": "u4-no-such-host.invalid", "port": "5432", "dbname": "okg",
        "user": "postgres",
    }


def test_an_uncaught_connection_failure_is_one_line_naming_the_host(
    connecting_command, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    monkeypatch.setenv("OKG_DSN", BAD_URL)
    monkeypatch.setenv("OKG_KUBERNETES_SMOKE_DSN", BAD_URL)

    rc = main_mod.main(["--json", "lint", "deployments"])
    err_text = capsys.readouterr().err
    err = json.loads(err_text)["error"]

    assert rc == 1
    assert err["code"] == "database_unreachable"
    assert err["message"].startswith(
        "okg lint: cannot connect to the database at u4-no-such-host.invalid:5432: "
        "failed to resolve host"
    )
    assert "correct OKG_DSN and OKG_KUBERNETES_SMOKE_DSN" in err["fix_hint"]
    assert err["data"]["dsn_variables"] == ["OKG_DSN", "OKG_KUBERNETES_SMOKE_DSN"]
    assert "Traceback" not in err_text
    assert SECRET not in err_text


def test_a_deployment_that_says_where_its_url_lives_is_named(
    connecting_command, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    monkeypatch.setenv("OKG_DSN", BAD_URL)
    monkeypatch.setenv("OKG_DSN_SOURCE", "Secret relak-okg, key okg-dsn")

    rc = main_mod.main(["lint", "deployments"])
    err_text = capsys.readouterr().err

    assert rc == 1
    assert "cannot connect to the database at u4-no-such-host.invalid:5432" in err_text
    assert "correct the database URL in Secret relak-okg, key okg-dsn" in err_text
    assert "Traceback" not in err_text
    assert SECRET not in err_text


def test_other_errors_still_propagate(monkeypatch: pytest.MonkeyPatch) -> None:
    def _cmd(args, *, as_json):
        raise psycopg.OperationalError("not from connect")

    monkeypatch.setattr(main_mod, "_cmd_lint", _cmd)
    with pytest.raises(psycopg.OperationalError):
        main_mod.main(["lint", "deployments"])


def _failing_connect(monkeypatch: pytest.MonkeyPatch, message: str) -> None:
    def _connect(*_a, **_k):
        raise psycopg.OperationalError(message)

    monkeypatch.setattr(psycopg, "connect", _connect)


def test_a_refused_login_says_the_server_answered(
    connecting_command, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    _failing_connect(
        monkeypatch,
        'connection to server at "u4-no-such-host.invalid" (10.0.0.1), port 5432 '
        'failed: FATAL:  password authentication failed for user "postgres" '
        f"(dsn {BAD_URL})",
    )
    monkeypatch.setenv("OKG_DSN", BAD_URL)

    rc = main_mod.main(["--json", "lint", "deployments"])
    err_text = capsys.readouterr().err
    err = json.loads(err_text)["error"]

    assert rc == 1
    assert err["code"] == "database_login_refused"
    assert "refused the login" in err["message"]
    assert "check the user and password in OKG_DSN" in err["fix_hint"]
    assert SECRET not in err_text


def test_a_missing_database_gives_the_createdb_command(
    connecting_command, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    _failing_connect(
        monkeypatch,
        'connection to server at "u4-no-such-host.invalid" (10.0.0.1), port 5432 '
        'failed: FATAL:  database "okg" does not exist',
    )
    rc = main_mod.main(["--json", "lint", "deployments"])
    err = json.loads(capsys.readouterr().err)["error"]

    assert rc == 1
    assert err["code"] == "database_missing"
    assert "createdb -h u4-no-such-host.invalid -p 5432 -U postgres okg" in err["fix_hint"]


def test_the_kept_place_is_named_only_for_the_url_that_failed(
    connecting_command, monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """OKG_DSN_SOURCE describes OKG_DSN; another URL failing is not it."""
    monkeypatch.setenv("OKG_DSN", "postgresql://postgres@elsewhere:5432/okg")
    monkeypatch.setenv("OKG_DSN_SOURCE", "Secret relak-okg, key okg-dsn")
    monkeypatch.setenv("PROBE_DSN", BAD_URL)

    main_mod.main(["--json", "lint", "deployments"])
    err = json.loads(capsys.readouterr().err)["error"]

    assert "Secret relak-okg" not in err["fix_hint"]
    assert "correct PROBE_DSN" in err["fix_hint"]
    assert err["data"]["dsn_source"] is None


def test_a_password_that_breaks_the_url_never_reaches_the_output(
    monkeypatch: pytest.MonkeyPatch, capsys,
) -> None:
    """An unescaped "@" in the password splits the URL at the wrong place."""
    fragment = "ss" + "9Q"
    password = "p" + "@" + fragment  # an unescaped "@" inside the password
    url = "postgresql://postgres:" + password + "@u4-no-such-host.invalid:5432/okg"

    def _cmd(args, *, as_json):
        connect(url, connect_timeout=2)
        return 0

    monkeypatch.setattr(main_mod, "_cmd_lint", _cmd)
    monkeypatch.delenv("OKG_DSN_SOURCE", raising=False)
    rc = main_mod.main(["--json", "lint", "deployments"])
    err_text = capsys.readouterr().err
    err = json.loads(err_text)["error"]

    assert rc == 1
    assert fragment not in err_text
    assert err["data"]["host"] == "<unparseable URL>"
    assert "percent-encoded" in err["fix_hint"]
