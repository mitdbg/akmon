"""`okg db role-passwords` -- inspect or replace okg's database role passwords.

Without ``--apply`` the command only reports: which managed roles can log in,
which still accept a built-in (public) password, and whether the owner-only
secrets file exists. With ``--apply`` it gives the roles random passwords and
records them (issue #3273). Neither form prints a password.
"""

from __future__ import annotations

import argparse
import stat
from typing import Any


def add_role_passwords_parser(db_subparsers: Any, *, add_dsn_arg, add_deployment_arg, add_json_flag) -> None:
    """Register `okg db role-passwords` under the `okg db` group."""
    parser = db_subparsers.add_parser(
        "role-passwords",
        help=(
            "Report okg's database login roles, or with --apply give them "
            "random passwords recorded in an owner-only secrets file"
        ),
    )
    add_deployment_arg(parser)
    add_dsn_arg(parser)
    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "Set a random password on okg_mcp and app_rw (granting LOGIN) and "
            "on app_ro/catalog_admin when they can log in; record the role "
            "connection strings in the mode-0600 secrets file"
        ),
    )
    parser.add_argument(
        "--builtin-dev-passwords",
        action="store_true",
        help=(
            "With --apply: set the built-in public passwords instead. Refused "
            "unless OKG_ALLOW_BUILTIN_ROLE_PASSWORDS=1 (single-user development)"
        ),
    )
    add_json_flag(parser)


def role_passwords_payload(args: argparse.Namespace, *, dsn: str) -> dict[str, Any]:
    """Run the command against ``dsn`` and return its JSON payload."""
    from okg.substrate.errors import SubstrateError
    from okg.substrate.local_database_roles import (
        builtin_password_findings,
        builtin_role_passwords_allowed,
        database_target,
        provision_role_passwords,
        read_role_secrets,
        role_secrets_path,
        rotate_command,
    )

    deployment = getattr(args, "deployment", None)
    builtin = bool(getattr(args, "builtin_dev_passwords", False))
    if builtin and not getattr(args, "apply", False):
        raise SubstrateError(
            "--builtin-dev-passwords only applies with --apply",
            fix_hint="add --apply, or drop --builtin-dev-passwords",
        )
    if getattr(args, "apply", False):
        payload = provision_role_passwords(
            dsn, builtin=builtin, deployment=deployment,
        )
    else:
        path = role_secrets_path(dsn)
        recorded = read_role_secrets(dsn, path=path)
        payload = {
            "schema": "okg.role_passwords_status/v1",
            "target": database_target(dsn),
            "secrets_file": str(path),
            "secrets_file_present": path.exists(),
            "recorded_roles": sorted(recorded),
            "ok": True,
        }
        if path.exists():
            payload["secrets_file_mode"] = (
                f"{stat.S_IMODE(path.stat().st_mode):04o}"
            )
    findings = builtin_password_findings(dsn)
    payload["builtin_password_roles"] = sorted(
        str(item["role"]) for item in findings
    )
    payload["builtin_passwords_allowed"] = builtin_role_passwords_allowed()
    if findings:
        payload["warnings"] = findings
        payload["fix_hint"] = rotate_command(deployment)
    return payload


__all__ = ["add_role_passwords_parser", "role_passwords_payload"]
