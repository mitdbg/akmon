"""CLI tests for targeted enricher backfill."""
from __future__ import annotations

import json

import okg.substrate.cli.main as cli
import okg.substrate.enrichers.backfill as backfill
import okg.substrate.publisher.deployment_stack as deployment_stack


class _FakeEnricher:
    reads_subtypes = ("source",)
    emits_edge_types = ("links",)
    requires_narrowings = ()

    def __init__(self, name: str) -> None:
        self.name = name


class _FakeStack:
    def __init__(self, enrichers) -> None:
        self.enrichers = list(enrichers)

    def publisher_kwargs(self) -> dict:
        return {"enrichers": self.enrichers, "materializers": ["ignored"]}


class _FakeResult:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def to_dict(self) -> dict:
        return dict(self._payload)


def test_enricher_backfill_cli_selects_one_registered_enricher(
    monkeypatch,
    capsys,
) -> None:
    seen: dict = {}
    stack = _FakeStack([_FakeEnricher("selected"), _FakeEnricher("other")])

    monkeypatch.setattr(cli, "_resolve_deployment_dsn", lambda args: "dsn")
    monkeypatch.setattr(
        deployment_stack,
        "build_deployment_publisher_stack",
        lambda **kwargs: stack,
    )

    def fake_run(**kwargs):
        seen.update(kwargs)
        return _FakeResult({
            "ok": True,
            "status": "completed",
            "enricher_name": kwargs["enricher_name"],
        })

    monkeypatch.setattr(backfill, "run_enricher_backfill", fake_run)

    rc = cli.main([
        "enricher",
        "backfill",
        "test",
        "selected",
        "--dsn",
        "postgres://example",
        "--limit",
        "7",
        "--chunk-size",
        "3",
        "--apply",
        "--json",
    ])

    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert payload["status"] == "completed"
    assert payload["enricher_name"] == "selected"
    assert seen["deployment"] == "test"
    assert seen["enricher_name"] == "selected"
    assert seen["limit"] == 7
    assert seen["chunk_size"] == 3
    assert seen["branch_name"] == "default"
    assert seen["enrichers"] == stack.enrichers


def test_enricher_backfill_cli_rejects_duplicate_names_before_writes(
    monkeypatch,
    capsys,
) -> None:
    stack = _FakeStack([_FakeEnricher("dupe"), _FakeEnricher("dupe")])
    called = False

    monkeypatch.setattr(cli, "_resolve_deployment_dsn", lambda args: "dsn")
    monkeypatch.setattr(
        deployment_stack,
        "build_deployment_publisher_stack",
        lambda **kwargs: stack,
    )

    def fake_run(**kwargs):
        nonlocal called
        called = True
        return _FakeResult({"ok": True})

    monkeypatch.setattr(backfill, "run_enricher_backfill", fake_run)

    rc = cli.main([
        "enricher",
        "backfill",
        "test",
        "dupe",
        "--dsn",
        "postgres://example",
        "--limit",
        "1",
        "--chunk-size",
        "1",
        "--apply",
        "--json",
    ])

    err = json.loads(capsys.readouterr().err)
    assert rc == 1
    assert called is False
    assert err["error"]["code"] == "enricher_backfill_selection_error"
    assert err["error"]["data"]["matches"] == ["dupe", "dupe"]
