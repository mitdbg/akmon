from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

import okg.substrate.cli.deployment as deployment_cli
import okg.substrate.pact.decisions as decisions
import okg.substrate.deployment_isolation as isolation


@pytest.mark.parametrize("iso_action", ["list", "gc"])
def test_isolated_gc_inventory_failure_emits_json_refusal(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    iso_action: str,
) -> None:
    """The operator receives the structured refusal, including target identity."""
    target = {
        "deployment": "demo",
        "database": "okg_demo",
        "identity": "ok",
    }

    monkeypatch.setattr(
        decisions,
        "resolve_ledger_dsn",
        lambda explicit_dsn=None: {
            "dsn": "postgres://postgres:okg@host1:5432/okg_demo",
            "target": target,
        },
    )

    def emit(payload, *, as_json: bool) -> None:
        assert as_json is True
        print(json.dumps(payload, sort_keys=True))

    monkeypatch.setattr(deployment_cli, "_emit_ok", emit)
    args = SimpleNamespace(
        iso_action=iso_action,
        deployment="demo",
        dsn=None,
        yes=iso_action == "gc",
    )

    hostile_name = "postgres://operator:SUPERSECRET@db/" + ("x" * 5_000)
    HostileInventoryError = type(hostile_name, (RuntimeError,), {})
    for inventory_failure in ("exception", "empty", "hostile", "prefixed-secret"):
        if inventory_failure == "exception":
            monkeypatch.setattr(
                isolation.subprocess,
                "run",
                lambda *_args, **_kwargs: (_ for _ in ()).throw(
                    OSError("git worktree inventory unavailable")
                ),
            )
        elif inventory_failure == "empty":
            monkeypatch.setattr(
                isolation.subprocess,
                "run",
                lambda *_args, **_kwargs: SimpleNamespace(stdout=""),
            )
        elif inventory_failure == "hostile":

            def raise_hostile(*_args, **_kwargs):
                raise HostileInventoryError("git probe failed")

            monkeypatch.setattr(isolation.subprocess, "run", raise_hostile)
        else:
            monkeypatch.setattr(
                isolation.subprocess,
                "run",
                lambda *_args, **_kwargs: (_ for _ in ()).throw(
                    OSError("git probe failed PGPASSWORD=SUPERSECRET")
                ),
            )

        status = deployment_cli._cmd_deployment_isolated(args, as_json=True)
        output = json.loads(capsys.readouterr().out)

        assert status == 1
        assert output["ok"] is False
        assert output["code"] == "worktree_inventory_unavailable"
        assert output["status"] == "unavailable"
        assert output["deployment"] == "demo"
        assert output["resolved_target"] == target
        if inventory_failure == "hostile":
            assert output["error"]["type"] == "RuntimeError"
            assert len(output["error"]["type"]) <= 64
            assert "SUPERSECRET" not in json.dumps(output)
        if inventory_failure == "prefixed-secret":
            assert "SUPERSECRET" not in json.dumps(output)
