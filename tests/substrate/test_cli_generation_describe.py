"""Unit tests for `okg generation describe`.

The integration path against a real DB is exercised by the QA gate;
here we cover argparse wiring + the not-found error.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone

from okg.substrate.cli import generation as generation_cli
from okg.substrate.cli.main import _build_parser, _cmd_generation


def test_argparse_accepts_generation_describe() -> None:
    parser = _build_parser()
    args = parser.parse_args(
        ["generation", "describe", "42", "--dsn", "postgres://x"],
    )
    assert args.command == "generation"
    assert args.subcmd == "describe"
    assert args.generation_id == 42
    assert args.dsn == "postgres://x"


def test_argparse_rejects_missing_subcmd() -> None:
    parser = _build_parser()
    import pytest
    with pytest.raises(SystemExit):
        parser.parse_args(["generation"])


def test_argparse_accepts_chronos_string_and_int_generation_id() -> None:
    """Chronos-native generation ids are STRING checkpoint ids (what
    `okg generation list` emits, e.g. okg:...:gen:...), so `describe` must
    accept them. A legacy bare integer id still parses as an int."""
    parser = _build_parser()
    gen = "okg:default:branch:default:gen:20260615T004609Z:0e7892eb1ce2"
    chronos = parser.parse_args(["generation", "describe", gen])
    assert chronos.generation_id == gen
    legacy = parser.parse_args(["generation", "describe", "42"])
    assert legacy.generation_id == 42


def test_subcmd_other_than_describe_emits_error_envelope(
    capsys, monkeypatch,
) -> None:
    """Argparse forces subcmd ∈ {describe} via add_parser, but we
    also defend in `_cmd_generation` for callers that build args
    by hand. Verify the error path."""
    monkeypatch.setenv("OKG_DSN", "postgres://example/x")
    args = argparse.Namespace(
        command="generation",
        subcmd="bogus",
        generation_id=1,
        dsn=None,
        deployment=None,
        json=True,
    )
    rc = _cmd_generation(args, as_json=True)
    assert rc != 0
    captured = capsys.readouterr()
    assert "describe" in captured.err


def test_consistency_diagnostics_uses_schema_compatible_counts(
    monkeypatch,
) -> None:
    """The live dogfood table can lack the first-class count columns.

    `generation describe` must project them compatibly instead of selecting
    missing columns directly.
    """

    class FakeCursor:
        connection = object()

        def __init__(self) -> None:
            self.query = ""

        def execute(self, query: str, params: tuple[int, ...]) -> None:
            self.query = query
            assert params == (42,)

        def fetchall(self) -> list[tuple[object, ...]]:
            return [(
                "result-1",
                "run-1",
                "check-1",
                "gate-1",
                "evaluated",
                False,
                "ok",
                2,
                True,
                "complete",
                500,
                12,
                {"backpressure_policy": {"lookback": 5}},
                {"coverage": {"coverage_complete": True}},
                datetime(2026, 7, 7, tzinfo=timezone.utc),
                0,
                0,
            )]

    monkeypatch.setattr(
        generation_cli,
        "_consistency_gate_health_dict",
        lambda *args, **kwargs: {"degraded": False},
    )

    cur = FakeCursor()
    diagnostics = generation_cli._generation_consistency_diagnostics(
        cur,
        generation_id=42,
    )

    assert "newly_opened_count, resolved_count" not in cur.query
    assert "to_jsonb(cgr)->>'newly_opened_count'" in cur.query
    assert "to_jsonb(cgr)->>'resolved_count'" in cur.query
    assert diagnostics["newly_opened_count"] == 0
    assert diagnostics["resolved_count"] == 0
    assert diagnostics["gates"][0]["newly_opened_count"] == 0
    assert diagnostics["gates"][0]["resolved_count"] == 0
