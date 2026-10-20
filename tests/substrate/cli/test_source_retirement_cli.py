"""CLI wiring for the guarded source-retirement remediation command (#1230)."""
from __future__ import annotations

import json
import sys
import types
from dataclasses import dataclass
from typing import Any

import psycopg
import pytest

from okg.substrate.cli.main import _build_parser, main


def test_retire_source_parser_defaults_and_explicit_options() -> None:
    args = _build_parser().parse_args([
        "remediate",
        "retire-source",
        "--source",
        "legacy-source",
    ])

    assert args.command == "remediate"
    assert args.subcmd == "retire-source"
    assert args.source == "legacy-source"
    assert args.deployment_name == "default"
    assert args.branch == "default"
    assert args.expected_plan_digest is None
    assert args.confirm_source is None
    assert args.actor == "operator"
    assert args.apply is False

    explicit = _build_parser().parse_args([
        "remediate",
        "retire-source",
        "--source",
        "legacy-source",
        "--deployment-name",
        "disposable",
        "--branch",
        "review",
        "--expected-plan-digest",
        "sha256:plan",
        "--confirm-source",
        "legacy-source@registry-v3",
        "--actor",
        "riley",
        "--apply",
    ])

    assert explicit.deployment_name == "disposable"
    assert explicit.branch == "review"
    assert explicit.expected_plan_digest == "sha256:plan"
    assert explicit.confirm_source == "legacy-source@registry-v3"
    assert explicit.actor == "riley"
    assert explicit.apply is True


def test_retire_source_requires_source() -> None:
    with pytest.raises(SystemExit):
        _build_parser().parse_args(["remediate", "retire-source"])


@dataclass(frozen=True)
class _FakeRetirementResult:
    applied: bool
    plan_digest: str
    affected_count: int


class _FakeConnection:
    def __enter__(self) -> "_FakeConnection":
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


def test_retire_source_dispatches_core_and_defaults_to_dry_run(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: dict[str, Any] = {}

    def _remediate(conn: object, **kwargs: Any) -> _FakeRetirementResult:
        assert isinstance(conn, _FakeConnection)
        calls.update(kwargs)
        return _FakeRetirementResult(
            applied=bool(kwargs["apply"]),
            plan_digest="sha256:plan",
            affected_count=0,
        )

    fake_module = types.ModuleType("okg.substrate.source_retirement")
    fake_module.remediate_source_retirement = _remediate  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "okg.substrate.source_retirement", fake_module)
    monkeypatch.setattr(psycopg, "connect", lambda *_a, **_k: _FakeConnection())

    rc = main([
        "remediate",
        "retire-source",
        "--source",
        "legacy-source",
        "--dsn",
        "postgresql://ignored/okg",
        "--json",
    ])

    assert rc == 0
    assert calls == {
        "source_name": "legacy-source",
        "deployment_name": "default",
        "branch_name": "default",
        "expected_plan_digest": None,
        "confirm_source": None,
        "actor": "operator",
        "apply": False,
    }
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert payload["subcmd"] == "retire-source"
    assert payload["applied"] is False


def test_retire_source_dispatch_forwards_apply_review_fields(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, Any] = {}

    def _remediate(_conn: object, **kwargs: Any) -> _FakeRetirementResult:
        calls.update(kwargs)
        return _FakeRetirementResult(
            applied=True,
            plan_digest="sha256:plan",
            affected_count=2,
        )

    fake_module = types.ModuleType("okg.substrate.source_retirement")
    fake_module.remediate_source_retirement = _remediate  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "okg.substrate.source_retirement", fake_module)
    monkeypatch.setattr(psycopg, "connect", lambda *_a, **_k: _FakeConnection())

    rc = main([
        "remediate",
        "retire-source",
        "--source",
        "legacy-source",
        "--deployment-name",
        "disposable",
        "--branch",
        "review",
        "--expected-plan-digest",
        "sha256:plan",
        "--confirm-source",
        "legacy-source@registry-v3",
        "--actor",
        "riley",
        "--apply",
        "--dsn",
        "postgresql://ignored/okg",
        "--json",
    ])

    assert rc == 0
    assert calls == {
        "source_name": "legacy-source",
        "deployment_name": "disposable",
        "branch_name": "review",
        "expected_plan_digest": "sha256:plan",
        "confirm_source": "legacy-source@registry-v3",
        "actor": "riley",
        "apply": True,
    }
