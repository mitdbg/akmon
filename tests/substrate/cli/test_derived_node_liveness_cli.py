"""CLI parser and dispatch coverage for read-only exposure reporting."""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import psycopg
import pytest

from okg.substrate.cli.main import _build_parser, main


def test_derived_node_exposure_parser_defaults() -> None:
    args = _build_parser().parse_args([
        "remediate",
        "derived-node-exposure",
        "--json",
    ])
    assert args.command == "remediate"
    assert args.subcmd == "derived-node-exposure"
    assert args.deployment_name == "default"
    assert args.branch == "default"
    assert args.generation is None
    assert args.example_limit == 20
    assert not hasattr(args, "apply")


def test_derived_node_exposure_parser_accepts_pinned_generation() -> None:
    args = _build_parser().parse_args([
        "remediate",
        "derived-node-exposure",
        "--deployment-name",
        "disposable",
        "--branch",
        "review",
        "--generation",
        "okg:disposable:branch:review:gen:1",
        "--example-limit",
        "7",
    ])
    assert args.deployment_name == "disposable"
    assert args.branch == "review"
    assert args.generation.endswith(":1")
    assert args.example_limit == 7


@dataclass(frozen=True)
class _FakeReport:
    schema: str
    mutation_proof: dict[str, Any]


class _FakeConnection:
    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


def test_derived_node_exposure_dispatch_is_read_only_and_forwards_options(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    seen: dict[str, Any] = {}

    def _report(_conn: object, **kwargs: Any) -> dict[str, Any]:
        seen.update(kwargs)
        return {
            "schema": "okg.derived_node_exposure.v1",
            "mutation_proof": {"ok": True},
        }

    monkeypatch.setattr(
        "okg.substrate.derived_node_liveness.report_derived_node_exposure",
        _report,
    )
    monkeypatch.setattr(psycopg, "connect", lambda *_a, **_k: _FakeConnection())

    rc = main([
        "remediate",
        "derived-node-exposure",
        "--deployment-name",
        "disposable",
        "--branch",
        "review",
        "--generation",
        "checkpoint-7",
        "--example-limit",
        "3",
        "--dsn",
        "postgresql://ignored/okg",
        "--json",
    ])

    assert rc == 0
    assert seen == {
        "deployment_name": "disposable",
        "branch": "review",
        "generation": "checkpoint-7",
        "example_limit": 3,
    }
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["schema"] == "okg.derived_node_exposure.v1"
    assert payload["mutation_proof"]["ok"] is True
