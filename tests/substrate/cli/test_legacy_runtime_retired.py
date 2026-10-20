from __future__ import annotations

import argparse
import importlib

import pytest

import okg.substrate.cli.main as cli


def test_obsolete_runtime_commands_are_deleted_from_parser() -> None:
    parser = cli._build_parser()
    subparsers = [
        action for action in parser._actions
        if isinstance(action, argparse._SubParsersAction)
    ]
    assert len(subparsers) == 1
    choices = subparsers[0].choices
    assert "worker" not in choices
    assert "cron" not in choices
    assert "lease" not in choices


@pytest.mark.parametrize(
    "module_name",
    [
        "okg.substrate.jobs.queue",
        "okg.substrate.jobs.worker",
        "okg.substrate.jobs.embed_handler",
        "okg.substrate.cron.install",
        "okg.substrate.coordination.lease",
    ],
)
def test_retired_runtime_modules_are_deleted(module_name: str) -> None:
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module(module_name)


def test_obsolete_job_runtime_package_is_deleted() -> None:
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("okg.substrate.jobs")
