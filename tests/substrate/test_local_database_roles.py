"""Role password lookup and the owner-only secrets file (issue #3273).

okg used to derive fixed, public passwords for its database login roles for
any loopback database. These tests pin the replacement: the lookup order
(environment, then the 0600 secrets file, then the built-in password only
behind OKG_ALLOW_BUILTIN_ROLE_PASSWORDS=1, else a refusal), the file's
permission and identity checks, and the refusals that fire before any
database connection. The PostgreSQL half lives in test_least_privilege_mcp.py.
"""
from __future__ import annotations

import json
import os
import stat
from pathlib import Path

import pytest
from psycopg.conninfo import conninfo_to_dict

from okg.substrate import local_database_roles as roles
from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.server import _derive_role_dsn


OWNER = "postgresql://postgres:ownerpw@127.0.0.1:55432/okg"


@pytest.fixture
def secrets_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    path = tmp_path / "cfg" / "okg" / "role-secrets" / "db.json"
    monkeypatch.setenv(roles.ROLE_SECRETS_FILE_ENV, str(path))
    monkeypatch.delenv("OKG_MCP_RO_DSN", raising=False)
    monkeypatch.delenv("OKG_MCP_RW_DSN", raising=False)
    monkeypatch.delenv(roles.ALLOW_BUILTIN_ROLE_PASSWORDS_ENV, raising=False)
    return path


def test_no_env_no_file_no_flag_refuses(secrets_file: Path) -> None:
    with pytest.raises(SubstrateError) as caught:
        _derive_role_dsn(OWNER, role="okg_mcp", env_var="OKG_MCP_RO_DSN")
    err = caught.value
    assert err.code == "role_password_not_provisioned"
    assert "okg db role-passwords --apply" in err.fix_hint
    assert roles.ALLOW_BUILTIN_ROLE_PASSWORDS_ENV in err.fix_hint
    # The refusal names the public password nowhere.
    assert "okg_mcp:okg_mcp" not in str(err) and "password=" not in str(err)


def test_flag_restores_builtin_password(
    secrets_file: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(roles.ALLOW_BUILTIN_ROLE_PASSWORDS_ENV, "1")
    dsn = _derive_role_dsn(OWNER, role="app_rw", env_var="OKG_MCP_RW_DSN")
    info = conninfo_to_dict(dsn)
    assert (info["user"], info["password"]) == ("app_rw", "okg_rw")
    assert (info["host"], info["port"], info["dbname"]) == (
        "127.0.0.1", "55432", "okg",
    )
    assert "ownerpw" not in dsn


@pytest.mark.parametrize("value", ["0", "", "no", "false"])
def test_flag_must_be_affirmative(
    secrets_file: Path, monkeypatch: pytest.MonkeyPatch, value: str,
) -> None:
    monkeypatch.setenv(roles.ALLOW_BUILTIN_ROLE_PASSWORDS_ENV, value)
    with pytest.raises(SubstrateError):
        _derive_role_dsn(OWNER, role="okg_mcp", env_var="OKG_MCP_RO_DSN")


def test_file_wins_over_flag_and_env_wins_over_file(
    secrets_file: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    recorded = "postgresql://okg_mcp:random-from-file@127.0.0.1:55432/okg"
    written = roles.write_role_secrets(OWNER, {"okg_mcp": recorded})
    assert written == secrets_file
    assert stat.S_IMODE(secrets_file.stat().st_mode) == 0o600
    assert stat.S_IMODE(secrets_file.parent.stat().st_mode) == 0o700
    monkeypatch.setenv(roles.ALLOW_BUILTIN_ROLE_PASSWORDS_ENV, "1")
    assert _derive_role_dsn(
        OWNER, role="okg_mcp", env_var="OKG_MCP_RO_DSN"
    ) == recorded
    monkeypatch.setenv("OKG_MCP_RO_DSN", "postgresql://managed/ro")
    assert _derive_role_dsn(
        OWNER, role="okg_mcp", env_var="OKG_MCP_RO_DSN"
    ) == "postgresql://managed/ro"


def test_file_is_found_through_any_loopback_spelling(secrets_file: Path) -> None:
    recorded = "postgresql://okg_mcp:x@127.0.0.1:55432/okg"
    roles.write_role_secrets(OWNER, {"okg_mcp": recorded})
    other_spelling = "postgresql://postgres@localhost:55432/okg"
    info = conninfo_to_dict(_derive_role_dsn(
        other_spelling, role="okg_mcp", env_var="OKG_MCP_RO_DSN"
    ))
    assert (info["user"], info["password"]) == ("okg_mcp", "x")
    # Rendered through the caller's own host spelling and database.
    assert (info["host"], info["port"], info["dbname"]) == (
        "localhost", "55432", "okg",
    )


def test_default_path_is_keyed_by_server(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A role and its password belong to the server, so every database on it
    # shares one file.
    monkeypatch.delenv(roles.ROLE_SECRETS_FILE_ENV, raising=False)
    monkeypatch.setenv("XDG_CONFIG_HOME", str(tmp_path))
    path = roles.role_secrets_path(OWNER)
    assert path == tmp_path / "okg" / "role-secrets" / "loopback-55432.json"
    assert roles.role_secrets_path(
        "postgresql://u@localhost:55432/other"
    ) == path
    assert roles.role_secrets_path(
        "postgresql://u@localhost:55433/okg"
    ).name == "loopback-55433.json"


def test_rotation_for_one_database_serves_every_database_on_the_server(
    secrets_file: Path,
) -> None:
    """Review B1: rotating through database A must not strand database B."""
    roles.write_role_secrets(
        OWNER, {"okg_mcp": "postgresql://okg_mcp:new-pw@127.0.0.1:55432/okg"},
    )
    other_db = "postgresql://postgres:ownerpw@localhost:55432/second"
    info = conninfo_to_dict(
        _derive_role_dsn(other_db, role="okg_mcp", env_var="OKG_MCP_RO_DSN")
    )
    assert (info["user"], info["password"]) == ("okg_mcp", "new-pw")
    assert (info["host"], info["dbname"]) == ("localhost", "second")


@pytest.mark.parametrize(
    "dsn",
    [
        "postgresql://u:p@/okg?host=/tmp,10.0.0.5&port=5432",
        "postgresql://u:p@localhost:5432/okg?hostaddr=10.1.2.3",
    ],
)
def test_a_dsn_that_can_reach_a_remote_host_is_not_local(
    dsn: str, secrets_file: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(roles.ALLOW_BUILTIN_ROLE_PASSWORDS_ENV, "1")
    assert roles.dsn_is_local(dsn) is False
    with pytest.raises(SubstrateError) as caught:
        _derive_role_dsn(dsn, role="okg_mcp", env_var="OKG_MCP_RO_DSN")
    assert caught.value.data["env_var"] == "OKG_MCP_RO_DSN"
    _assert_says_how_to_set_up_remote_roles(caught.value)


def _assert_says_how_to_set_up_remote_roles(err: SubstrateError) -> None:
    """Issue #3480: the refusal says why (the roles are server-wide) and the
    exact way through on a remote server, with a docs link."""
    message = str(err)
    assert "only on a local database" in message
    assert "shared by every database on the server" in message
    hint = err.fix_hint
    assert "server-wide" in hint
    for statement in (
        "ALTER ROLE okg_mcp WITH LOGIN;",
        "ALTER ROLE app_rw WITH LOGIN;",
        "\\password okg_mcp",
        "\\password app_rw",
    ):
        assert statement in hint
    assert "OKG_MCP_RO_DSN" in hint and "OKG_MCP_RW_DSN" in hint
    assert ".okg/local.env" in hint and "chmod 600" in hint
    assert "okg agent connect --deployment <name>" in hint
    assert "'A database on another host'" in hint
    assert "docs/start/quickstart.md" in hint


def test_the_remote_roles_hint_links_the_public_page_outside_a_checkout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.substrate.support import user_hints

    monkeypatch.setattr(user_hints, "checkout_root", lambda: None)
    hint = roles.remote_roles_hint()
    assert f"{user_hints.PUBLIC_DOCS_BASE}/docs/start/quickstart.md" in hint


def test_an_empty_host_follows_pghost(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PGHOSTADDR", raising=False)
    monkeypatch.setenv("PGHOST", "db.internal.example")
    assert roles.dsn_is_local("postgresql:///okg") is False
    monkeypatch.setenv("PGHOST", "/var/run/postgresql")
    assert roles.dsn_is_local("postgresql:///okg") is True


def test_a_loose_secrets_directory_is_refused(
    secrets_file: Path,
) -> None:
    secrets_file.parent.mkdir(parents=True)
    os.chmod(secrets_file.parent, 0o777)
    with pytest.raises(SubstrateError) as caught:
        roles.write_role_secrets(OWNER, {"okg_mcp": "x"})
    assert "writable by other accounts" in str(caught.value)
    assert not secrets_file.exists()


def test_file_readable_by_others_is_refused(secrets_file: Path) -> None:
    roles.write_role_secrets(OWNER, {"okg_mcp": "postgresql://okg_mcp:x@h/okg"})
    os.chmod(secrets_file, 0o644)
    with pytest.raises(SubstrateError) as caught:
        _derive_role_dsn(OWNER, role="okg_mcp", env_var="OKG_MCP_RO_DSN")
    assert caught.value.code == "role_secrets_unusable"
    assert "0644" in str(caught.value)
    assert "chmod 600" in caught.value.fix_hint


def test_symlinked_file_is_refused(secrets_file: Path, tmp_path: Path) -> None:
    real = tmp_path / "real.json"
    roles.write_role_secrets(OWNER, {"okg_mcp": "x"}, path=real)
    secrets_file.parent.mkdir(parents=True, exist_ok=True)
    secrets_file.symlink_to(real)
    with pytest.raises(SubstrateError) as caught:
        roles.read_role_secrets(OWNER)
    assert caught.value.code == "role_secrets_unusable"


def test_file_for_another_server_is_refused(secrets_file: Path) -> None:
    roles.write_role_secrets(OWNER, {"okg_mcp": "x"})
    with pytest.raises(SubstrateError) as caught:
        roles.read_role_secrets("postgresql://postgres@127.0.0.1:55499/okg")
    assert "different database server" in str(caught.value)


def test_malformed_file_is_refused(secrets_file: Path) -> None:
    secrets_file.parent.mkdir(parents=True)
    fd = os.open(secrets_file, os.O_CREAT | os.O_WRONLY, 0o600)
    os.write(fd, b'{"schema": "something-else"}')
    os.close(fd)
    with pytest.raises(SubstrateError):
        roles.read_role_secrets(OWNER)


def test_write_merges_and_removes(secrets_file: Path) -> None:
    roles.write_role_secrets(OWNER, {"okg_mcp": "a", "app_rw": "b"})
    roles.write_role_secrets(OWNER, {"app_ro": "c"}, remove=["okg_mcp"])
    payload = json.loads(secrets_file.read_text())
    assert payload["schema"] == roles.ROLE_SECRETS_SCHEMA
    assert payload["target"] == {
        "host": "loopback", "port": "55432",
    }
    assert payload["roles"] == {"app_rw": "b", "app_ro": "c"}
    assert not list(secrets_file.parent.glob(".*.tmp"))


def test_non_loopback_host_still_requires_the_env_override(
    secrets_file: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(roles.ALLOW_BUILTIN_ROLE_PASSWORDS_ENV, "1")
    with pytest.raises(SubstrateError) as caught:
        _derive_role_dsn(
            "postgresql://owner:pw@db.internal.example/okg",
            role="okg_mcp",
            env_var="OKG_MCP_RO_DSN",
        )
    assert caught.value.data["env_var"] == "OKG_MCP_RO_DSN"
    assert "db.internal.example" in str(caught.value)
    _assert_says_how_to_set_up_remote_roles(caught.value)


def test_provision_refuses_a_managed_host_before_connecting(
    secrets_file: Path,
) -> None:
    with pytest.raises(SubstrateError) as caught:
        roles.provision_role_passwords(
            "postgresql://owner:pw@db.internal.example:1/okg"
        )
    assert caught.value.code == "role_passwords_managed_host"
    _assert_says_how_to_set_up_remote_roles(caught.value)


def test_provision_builtin_requires_the_flag(secrets_file: Path) -> None:
    # Port 1 on loopback: the refusal must come before any connection.
    with pytest.raises(SubstrateError) as caught:
        roles.provision_role_passwords(
            "postgresql://owner:pw@127.0.0.1:1/okg", builtin=True,
        )
    assert caught.value.code == "builtin_role_passwords_refused"


def test_cli_builtin_flag_needs_apply(secrets_file: Path) -> None:
    import argparse

    from okg.substrate.cli.role_passwords import role_passwords_payload

    args = argparse.Namespace(
        deployment=None, apply=False, builtin_dev_passwords=True,
    )
    with pytest.raises(SubstrateError):
        role_passwords_payload(args, dsn=OWNER)


def test_role_passwords_subcommand_is_registered() -> None:
    from okg.substrate.cli.main import _build_parser

    parsed = _build_parser().parse_args(
        ["db", "role-passwords", "--dsn", OWNER, "--apply"]
    )
    assert parsed.db_subcmd == "role-passwords"
    assert parsed.apply is True
    assert parsed.builtin_dev_passwords is False


def test_rotate_command_names_the_deployment() -> None:
    assert roles.rotate_command("acme") == (
        "okg db role-passwords --apply --deployment acme"
    )
    assert roles.rotate_command(None) == "okg db role-passwords --apply"
    assert roles.rotate_command("default") == "okg db role-passwords --apply"


def test_runtime_bootstrap_reads_the_mcp_dsn_from_the_secrets_file(
    secrets_file: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With OKG_MCP_RO_DSN unset, `okg runtime bootstrap` reconciles okg_mcp
    from the recorded password, re-anchored on the bootstrap DSN's host
    spelling so its same-database check compares like with like."""
    import contextlib

    import okg.substrate.deployment_apply as deployment_apply
    from okg.substrate.runtime import bootstrap
    from okg.substrate.runtime.config import RuntimeConfig

    roles.write_role_secrets(
        OWNER,
        {"okg_mcp": "postgresql://okg_mcp:from-file@127.0.0.1:55432/okg"},
    )
    seen: dict[str, object] = {}

    @contextlib.contextmanager
    def _fake_connect(dsn, **_kwargs):  # noqa: ANN001
        yield object()

    def _reconcile(conn, *, mcp_dsn, deployment_dsn):  # noqa: ANN001
        seen["mcp_dsn"] = mcp_dsn
        return {"reconciled": False}

    monkeypatch.setattr(bootstrap, "db_connect", _fake_connect)
    monkeypatch.setattr(bootstrap, "ensure_bootstrap_marker_table", lambda c: None)
    monkeypatch.setattr(bootstrap, "is_bootstrapped", lambda c, d: True)
    monkeypatch.setattr(bootstrap, "reconcile_mcp_login_role", _reconcile)
    monkeypatch.setattr(
        deployment_apply, "ensure_deployment_apply_schema", lambda c: None,
    )
    localhost_owner = "postgresql://postgres:ownerpw@localhost:55432/okg"
    bootstrap.bootstrap_dbos_schema(
        deployment="d", dsn=localhost_owner, runtime=RuntimeConfig(),
    )
    info = conninfo_to_dict(str(seen["mcp_dsn"]))
    assert (info["user"], info["password"]) == ("okg_mcp", "from-file")
    assert (info["host"], info["port"], info["dbname"]) == (
        "localhost", "55432", "okg",
    )


@pytest.mark.parametrize(
    ("login", "refused"),
    [("rejected", True), ("inconclusive", False), ("accepted", False)],
)
def test_mcp_serve_refuses_only_a_rejected_password(
    secrets_file: Path, monkeypatch: pytest.MonkeyPatch, login: str, refused: bool,
) -> None:
    """Review S2: a stale recorded password is refused before the server is
    built; an unreachable server is left to the readiness checks."""
    roles.write_role_secrets(
        OWNER, {"okg_mcp": "postgresql://okg_mcp:stale@127.0.0.1:55432/okg"},
    )
    monkeypatch.setattr(roles, "_login", lambda dsn: (login, None))
    if refused:
        with pytest.raises(SubstrateError) as caught:
            roles.require_role_dsn_usable(
                OWNER, role="okg_mcp", env_var="OKG_MCP_RO_DSN", surface="s",
            )
        assert caught.value.code == "role_password_rejected"
        assert "stale" not in str(caught.value)
        assert "okg db role-passwords --apply" in caught.value.fix_hint
    else:
        roles.require_role_dsn_usable(
            OWNER, role="okg_mcp", env_var="OKG_MCP_RO_DSN", surface="s",
        )


def test_the_page_the_hint_names_has_the_remote_steps() -> None:
    """The refusal sends the reader to a page section; that section must
    exist and give the same statements and variables."""
    page = Path(__file__).resolve().parents[2] / roles.REMOTE_ROLES_DOC
    text = page.read_text(encoding="utf-8")
    assert "**A database on another host.**" in text
    section = text.split("**A database on another host.**", 1)[1]
    section = section.split("\n## ", 1)[0]
    for line in (
        "ALTER ROLE okg_mcp WITH LOGIN;",
        "ALTER ROLE app_rw WITH LOGIN;",
        "\\password okg_mcp",
        "\\password app_rw",
        "chmod 600 .okg/local.env",
        "#   OKG_MCP_RO_DSN=postgresql://okg_mcp:<password>@",
        "#   OKG_MCP_RW_DSN=postgresql://app_rw:<password>@",
        "okg agent connect --writes",
        "okg agent connect --deployment <name>",
        "never a password",
    ):
        assert line in section
