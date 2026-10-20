"""Phase 2 CLI changes — parser smoke tests + deprecation warnings.

Per `align-substrate-cli-docs-and-runbooks` Phase 2:
- `okg generation list` and `okg catalog history` parse.
- `okg snapshot-load` exists; `okg bootstrap` emits deprecation
  warning and still works.
- `okg consistency-rerun` exists; nested form emits deprecation
  warning.
- `okg metrics --source-sync` emits deprecation warning.
"""
from __future__ import annotations

import argparse

import pytest


@pytest.fixture(scope="module")
def cli_parser() -> argparse.ArgumentParser:
    from okg.substrate.cli.main import _build_parser
    return _build_parser()


def test_okg_generation_list_parses(cli_parser) -> None:
    args = cli_parser.parse_args([
        "generation", "list", "--deployment", "test_pi", "--limit", "5",
    ])
    assert args.command == "generation"
    assert args.subcmd == "list"
    assert args.deployment == "test_pi"
    assert args.limit == 5


def test_okg_catalog_history_parses(cli_parser) -> None:
    args = cli_parser.parse_args(["catalog", "history", "--limit", "10"])
    assert args.command == "catalog"
    assert args.subcmd == "history"
    assert args.limit == 10


def test_okg_snapshot_load_parses(cli_parser) -> None:
    args = cli_parser.parse_args([
        "snapshot-load", "--snapshot", "/tmp/x.json", "--apply",
    ])
    assert args.command == "snapshot-load"


def test_okg_bootstrap_removed(cli_parser) -> None:
    """`okg bootstrap` is gone (retired). Only `okg snapshot-load`
    works after `retire-pi-prefix-commands`.
    """
    import pytest
    with pytest.raises(SystemExit):
        cli_parser.parse_args(["bootstrap", "--snapshot", "/tmp/x.json"])


def test_okg_consistency_rerun_parses(cli_parser) -> None:
    args = cli_parser.parse_args([
        "consistency-rerun", "--deployment", "test_pi",
    ])
    assert args.command == "consistency-rerun"
    assert args.deployment == "test_pi"


def test_okg_consistency_nested_removed(cli_parser) -> None:
    """The nested `okg consistency rerun-post-publish` form is
    retired. Only the flat `okg consistency-rerun` exists.
    """
    import pytest
    with pytest.raises(SystemExit):
        cli_parser.parse_args([
            "consistency", "rerun-post-publish",
            "--deployment", "test_pi",
        ])


def test_db_status_accepts_json_flag(cli_parser) -> None:
    """`--json` SHALL be accepted on db subverbs."""
    args = cli_parser.parse_args(["db", "status", "--json"])
    assert args.command == "db"
    assert args.db_subcmd == "status"
    assert getattr(args, "json", False) is True


def test_db_status_no_longer_accepts_format_flag(cli_parser) -> None:
    """The deprecated `--format text|json` is retired. Only `--json`
    works.
    """
    import pytest
    with pytest.raises(SystemExit):
        cli_parser.parse_args([
            "db", "status", "--format", "json",
        ])
