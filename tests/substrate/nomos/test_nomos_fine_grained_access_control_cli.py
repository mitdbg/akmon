from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from tests.substrate.nomos.test_nomos_fine_grained_access_control import (
    _activate_fine_grained_policy,
    _publish_fine_grained_graph,
)


pytestmark = pytest.mark.postgres


def test_cli_search_applies_fine_grained_denies(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    generation_id = _publish_fine_grained_graph(clean_runtime)
    _activate_fine_grained_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:benchmark")

    from okg.substrate.cli import _read_side
    from okg.substrate.cli.dsn_authority import ReadDsnResolution

    monkeypatch.setattr(
        _read_side,
        "resolve_read_dsn",
        lambda _args, *, surface: ReadDsnResolution(
            dsn=clean_runtime,
            deployment="default",
            authority={"mode": "test", "surface": surface},
            target=None,
        ),
    )

    exit_code = _read_side.cmd_search(
        SimpleNamespace(
            deployment="default",
            dsn=None,
            generation_id=generation_id,
            query="ticket",
            method="lexical",
            subtype=None,
            attr_filter=None,
            select=None,
            limit=10,
            offset=0,
        ),
        as_json=True,
    )

    output = capsys.readouterr().out
    payload = json.loads(output)
    assert exit_code == 0
    assert sorted(row["node_id"] for row in payload["results"]) == [
        "node:derived-allowed",
        "node:derived-future",
        "node:ticket-allowed",
        "node:ticket-future",
    ]
    assert payload["total_count"] is None and payload["total_count_basis"] == "unknown"
    assert payload["nomos_effect"] == "allow"
    assert "node:ticket-record-denied" not in output
    assert "node:derived-mixed" not in output


def test_cli_search_carries_fine_grained_access_context(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    generation_id = _publish_fine_grained_graph(clean_runtime)
    _activate_fine_grained_policy(clean_runtime)
    monkeypatch.setenv("OKG_NOMOS_RUNTIME_ENFORCEMENT", "1")
    monkeypatch.setenv("OKG_NOMOS_SUBJECT", "subject:benchmark")

    from okg.substrate.cli import _read_side
    from okg.substrate.cli.dsn_authority import ReadDsnResolution

    monkeypatch.setattr(
        _read_side,
        "resolve_read_dsn",
        lambda _args, *, surface: ReadDsnResolution(
            dsn=clean_runtime,
            deployment="default",
            authority={"mode": "test", "surface": surface},
            target=None,
        ),
    )

    exit_code = _read_side.cmd_search(
        SimpleNamespace(
            deployment="default",
            dsn=None,
            generation_id=generation_id,
            query="ticket",
            method="lexical",
            subtype=None,
            attr_filter=None,
            select=None,
            limit=10,
            offset=0,
            nomos_access_context_json=json.dumps(
                {
                    "context_id": "ctx:cli-horizon",
                    "as_of": "2026-06-15T00:00:00Z",
                }
            ),
        ),
        as_json=True,
    )

    output = capsys.readouterr().out
    payload = json.loads(output)
    assert exit_code == 0
    assert sorted(row["node_id"] for row in payload["results"]) == [
        "node:derived-allowed",
        "node:ticket-allowed",
    ]
    assert payload["total_count"] is None and payload["total_count_basis"] == "unknown"
    assert payload["nomos_effect"] == "allow"
    assert "node:ticket-future" not in output
    assert "node:derived-future" not in output
