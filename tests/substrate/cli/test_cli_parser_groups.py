"""Regression coverage for grouped CLI parser registration."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pytest

from okg.substrate.cli.main import _build_parser
from okg.substrate.cli.parser_groups import (
    register_db_cleanup_parsers,
    register_read_admin_parsers,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
MAIN_PY = REPO_ROOT / "src/okg/substrate/cli/main.py"
PARSER_GROUPS_PY = REPO_ROOT / "src/okg/substrate/cli/parser_groups.py"

EXTRACTED_TOP_LEVEL = {
    "generation": ["describe", "list", "compact"],
    "branch": [
        "list", "show", "create", "abandon", "delete", "compact",
        "rollover",
    ],
    "admission": ["list", "show", "submit", "decide"],
    "consistency-rerun": [],
    "view": ["list", "show", "inspect"],
    "catalog": ["load", "history", "ownership", "repair-restamp-ownership"],
    "alias": ["seed-from-subtype", "list", "collisions", "add", "remove"],
    "snapshot-load": [],
    "external-action": [
        "serve",
        "list",
        "show",
        "timeline",
        "recovery",
        "submit",
        "edit",
        "decide",
        "cancel",
        "claim",
        "attest-owner-unavailable",
        "outcome",
        "restore-create",
        "restore-batch",
        "restore-finalize",
    ],
}

EXPECTED_USAGE = {
    ("generation",): "usage: okg generation [-h] [--json] {describe,list,compact} ...",
    ("generation", "describe"): (
        "usage: okg generation describe [-h] [--json] "
        "[--deployment DEPLOYMENT] [--branch BRANCH] [--dsn DSN] generation_id"
    ),
    ("branch",): (
        "usage: okg branch [-h] [--json] "
        "{list,show,create,abandon,delete,compact,rollover} ..."
    ),
    ("branch", "create"): (
        "usage: okg branch create [-h] [--json] "
        "--role {experiment,quarantine} [--base-branch BASE_BRANCH] "
        "--deployment DEPLOYMENT [--dsn DSN] name"
    ),
    ("admission",): (
        "usage: okg admission [-h] [--json] {list,show,submit,decide} ..."
    ),
    ("view",): "usage: okg view [-h] [--json] {list,show,inspect} ...",
    ("view", "show"): (
        "usage: okg view show [-h] [--json] [--deployment DEPLOYMENT] "
        "--view VIEW [--slot SLOT] [--generation GENERATION] "
        "[--definition-id DEFINITION_ID] [--audience AUDIENCE] "
        "[--tenant TENANT] [--dsn DSN]"
    ),
    ("catalog",): (
        "usage: okg catalog [-h] [--json] "
        "{load,history,ownership,repair-restamp-ownership} ..."
    ),
    ("catalog", "ownership"): (
        "usage: okg catalog ownership [-h] [--json] {diagnose,claim} ..."
    ),
    ("alias",): (
        "usage: okg alias [-h] [--json] "
        "{seed-from-subtype,list,collisions,add,remove} ..."
    ),
    ("snapshot-load",): (
        "usage: okg snapshot-load [-h] [--json] --snapshot SNAPSHOT "
        "[--deployment-name DEPLOYMENT_NAME] [--deployment DEPLOYMENT] "
        "[--dsn DSN] [--apply]"
    ),
    ("db", "remove-job-runtime-subsystem"): (
        "usage: okg db remove-job-runtime-subsystem [-h] "
        "[--deployment DEPLOYMENT] [--dsn DSN] [--confirm] [--apply] "
        "[--accept-retired-ledger-rows] "
        "[--retired-rows-dump-ref RETIRED_ROWS_DUMP_REF] [--json]"
    ),
    ("consistency-rerun",): (
        "usage: okg consistency-rerun [-h] [--json] "
        "[--deployment DEPLOYMENT] [--generation GENERATION] "
        "[--scope-subject SCOPE_SUBJECT] [--dsn DSN]"
    ),
}


def _add_json_flag(
    parser: argparse.ArgumentParser,
    *,
    is_top: bool = False,
) -> None:
    parser.add_argument(
        "--json",
        action="store_true",
        default=False if is_top else argparse.SUPPRESS,
        help="Machine-readable JSON output (default under OKG_AGENT=1)",
    )


def _add_owner_flags(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--owner-id",
        help="lease holder identity (default: <host>:<pid>:<uuid4>)",
    )
    parser.add_argument(
        "--owner-meta",
        help="JSON object with free-form holder context "
        "(runtime, session_id, command, \u2026)",
        default="{}",
    )


def _subparser_action(
    parser: argparse.ArgumentParser,
) -> argparse._SubParsersAction | None:
    return next(
        (
            action
            for action in parser._actions
            if isinstance(action, argparse._SubParsersAction)
        ),
        None,
    )


def _parser_for(
    parser: argparse.ArgumentParser,
    path: tuple[str, ...],
) -> argparse.ArgumentParser:
    current = parser
    for part in path:
        subparsers = _subparser_action(current)
        assert subparsers is not None, f"{current.prog} has no subparsers"
        current = subparsers.choices[part]
    return current


def _choices(parser: argparse.ArgumentParser, path: tuple[str, ...]) -> list[str]:
    subparsers = _subparser_action(_parser_for(parser, path))
    if subparsers is None:
        return []
    return list(subparsers.choices)


def _action_snapshot(parser: argparse.ArgumentParser) -> list[dict[str, Any]]:
    snapshot: list[dict[str, Any]] = []
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            snapshot.append({
                "kind": "subparsers",
                "dest": action.dest,
                "choices": list(action.choices),
            })
            continue
        snapshot.append({
            "kind": "arg",
            "dest": action.dest,
            "option_strings": tuple(action.option_strings),
            "required": getattr(action, "required", False),
            "nargs": action.nargs,
            "default": repr(action.default),
            "choices": tuple(action.choices or ()),
            "help": action.help,
        })
    return snapshot


def _build_group_only_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="okg")
    _add_json_flag(parser, is_top=True)
    subparsers = parser.add_subparsers(dest="command")
    register_read_admin_parsers(
        subparsers,
        add_json_flag=_add_json_flag,
        add_owner_flags=_add_owner_flags,
    )
    p_db = subparsers.add_parser(
        "db",
        help="Inspect substrate database inventory, status, and migration drift",
    )
    _add_json_flag(p_db)
    db_subparsers = p_db.add_subparsers(dest="db_subcmd", required=True)
    register_db_cleanup_parsers(
        db_subparsers,
        add_json_flag=_add_json_flag,
    )
    return parser


@pytest.mark.parametrize(
    ("command", "expected_choices"),
    list(EXTRACTED_TOP_LEVEL.items()),
)
def test_extracted_family_subcommand_choices_are_stable(
    command: str,
    expected_choices: list[str],
) -> None:
    parser = _build_parser()
    assert _choices(parser, (command,)) == expected_choices


@pytest.mark.parametrize("path", sorted(EXPECTED_USAGE))
def test_extracted_family_usage_is_stable(path: tuple[str, ...]) -> None:
    usage = " ".join(_parser_for(_build_parser(), path).format_usage().split())
    assert usage == EXPECTED_USAGE[path]


@pytest.mark.parametrize(
    "path",
    [
        ("generation",),
        ("generation", "describe"),
        ("branch",),
        ("branch", "create"),
        ("admission",),
        ("admission", "decide"),
        ("consistency-rerun",),
        ("view",),
        ("view", "show"),
        ("catalog",),
        ("catalog", "ownership"),
        ("catalog", "ownership", "claim"),
        ("alias",),
        ("alias", "seed-from-subtype"),
        ("snapshot-load",),
        ("db", "remove-job-runtime-subsystem"),
    ],
)
def test_group_helpers_match_main_parser_for_extracted_paths(
    path: tuple[str, ...],
) -> None:
    main_parser = _parser_for(_build_parser(), path)
    group_parser = _parser_for(_build_group_only_parser(), path)
    assert _action_snapshot(main_parser) == _action_snapshot(group_parser)


def test_representative_extracted_family_argparse_defaults() -> None:
    parser = _build_parser()

    branch_args = parser.parse_args([
        "branch",
        "create",
        "experiment:test",
        "--role",
        "experiment",
        "--deployment",
        "okg-workspace",
    ])
    assert branch_args.base_branch == "default"
    assert branch_args.dsn is None

    admission_args = parser.parse_args([
        "admission",
        "decide",
        "--deployment",
        "okg-workspace",
        "--decision",
        "approve",
        "--action",
        "promote",
        "--request-id",
        "req-1",
    ])
    assert admission_args.actor == "operator"
    assert admission_args.max_wait_seconds == 600.0
    assert admission_args.poll_interval_seconds == 2.0

    view_args = parser.parse_args(["view", "show", "--view", "status"])
    assert view_args.slot == "markdown"
    assert view_args.definition_id is None

    catalog_args = parser.parse_args([
        "catalog",
        "ownership",
        "claim",
        "--deployment",
        "deployments/demo",
        "--force",
    ])
    assert catalog_args.ownership_cmd == "claim"
    assert catalog_args.force is True

    alias_args = parser.parse_args([
        "alias",
        "seed-from-subtype",
        "--subtype",
        "person",
        "--field",
        "name",
    ])
    assert alias_args.scope is None
    assert alias_args.apply is False

    snapshot_args = parser.parse_args([
        "snapshot-load",
        "--snapshot",
        "snapshot.json",
        "--deployment",
        "demo",
    ])
    assert snapshot_args.deployment_name is None
    assert snapshot_args.deployment == "demo"

    cleanup_args = parser.parse_args([
        "db",
        "remove-job-runtime-subsystem",
        "--confirm",
        "--apply",
    ])
    assert cleanup_args.db_subcmd == "remove-job-runtime-subsystem"
    assert cleanup_args.confirm is True
    assert cleanup_args.apply is True


def test_group_registration_boundary_is_not_inline_in_main() -> None:
    main_text = MAIN_PY.read_text()
    parser_groups_text = PARSER_GROUPS_PY.read_text()

    assert "register_read_admin_parsers(" in main_text
    assert "register_db_cleanup_parsers(" in main_text

    for moved_inline_name in (
        "p_gen = sub.add_parser(",
        "p_branch = sub.add_parser(",
        "p_adm = sub.add_parser(",
        "p_view = sub.add_parser(",
        "p_cat = sub.add_parser(",
        "p_alias = sub.add_parser(",
        "p_snap = sub.add_parser(",
        "p_db_remove_job_runtime = p_db_sub.add_parser(",
    ):
        assert moved_inline_name not in main_text

    assert "def register_db_cleanup_parsers" in parser_groups_text
    assert '"remove-job-runtime-subsystem"' in parser_groups_text
    assert '"retire-legacy-runtime"' not in parser_groups_text
    assert '"lease"' not in parser_groups_text
