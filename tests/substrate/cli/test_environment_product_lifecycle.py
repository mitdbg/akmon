from __future__ import annotations

import json
from types import SimpleNamespace

from okg.substrate.cli.main import main
from okg.substrate.errors import DeploymentEnvironmentInvalid


def _snapshot() -> dict[str, object]:
    return {
        "schema": "okg.deployment_product.operation_status/v1",
        "operation_id": "promotion-42",
        "target_environment_id": "production",
        "idempotency_key": "promotion-42",
        "transition_kind": "first_install",
        "status": {
            "state": "planned",
            "safe_to_retry": False,
            "next_actions": [],
        },
        "receipts": {"verification": "deferred", "verified": []},
        "authority_digest": "sha256:" + "a" * 64,
    }


def _patch_snapshot(monkeypatch, *, snapshot, rendered: str = ""):
    target = SimpleNamespace(id="production")
    calls: list[tuple[object, str]] = []
    monkeypatch.setattr(
        "okg.substrate.cli.environment.load_environment",
        lambda _ref, *, environments_dir: target,
    )
    lifecycle = __import__(
        "okg.substrate.deployment_product_lifecycle",
        fromlist=["read_product_apply_status"],
    )

    def read_product_apply_status(*, production, promotion_id):
        calls.append((production, promotion_id))
        return snapshot

    monkeypatch.setattr(
        lifecycle,
        "read_product_apply_status",
        read_product_apply_status,
    )
    render_calls: list[dict[str, object]] = []

    def render_product_operation_status(value):
        render_calls.append(value)
        return rendered

    monkeypatch.setattr(
        lifecycle,
        "render_product_operation_status",
        render_product_operation_status,
    )
    return target, calls, render_calls


def _argv(tmp_path, *extra: str) -> list[str]:
    return [
        "environment",
        "product-status",
        "production",
        "--operation-id",
        "promotion-42",
        "--environments-root",
        str(tmp_path),
        *extra,
    ]


def test_product_status_human_and_json_share_one_non_success_snapshot(
    tmp_path, monkeypatch, capsys, record_property
) -> None:
    record_property(
        "requirements",
        "req.lifecycle.snapshot-readback-slice acc.lifecycle.snapshot-readback-slice",
    )
    snapshot = _snapshot()
    target, calls, render_calls = _patch_snapshot(
        monkeypatch,
        snapshot=snapshot,
        rendered="operation promotion-42: planned; controller unavailable\n",
    )

    assert main(_argv(tmp_path)) == 1
    human = capsys.readouterr()
    assert human.out == "operation promotion-42: planned; controller unavailable\n"
    assert human.err == ""
    assert render_calls == [snapshot]

    assert main(_argv(tmp_path, "--json")) == 1
    machine = capsys.readouterr()
    assert json.loads(machine.out) == snapshot
    assert machine.err == ""
    assert "prompt" not in machine.out.lower()
    assert render_calls == [snapshot]
    assert calls == [(target, "promotion-42"), (target, "promotion-42")]


def test_product_status_typed_failure_uses_error_emitter(
    tmp_path, monkeypatch, capsys, record_property
) -> None:
    record_property("requirements", "req.lifecycle.snapshot-readback-slice")
    target = SimpleNamespace(id="production")
    monkeypatch.setattr(
        "okg.substrate.cli.environment.load_environment",
        lambda _ref, *, environments_dir: target,
    )
    lifecycle = __import__(
        "okg.substrate.deployment_product_lifecycle",
        fromlist=["read_product_apply_status"],
    )

    def fail(*, production, promotion_id):
        assert production is target
        assert promotion_id == "promotion-42"
        raise DeploymentEnvironmentInvalid(
            "promotion journal is invalid",
            data={"operation_id": promotion_id},
        )

    monkeypatch.setattr(lifecycle, "read_product_apply_status", fail)

    assert main(_argv(tmp_path, "--json")) == 2
    output = capsys.readouterr()
    assert output.out == ""
    error = json.loads(output.err)
    assert error["error"]["code"] == "deployment_environment_invalid"
    assert error["error"]["data"] == {"operation_id": "promotion-42"}
    assert "prompt" not in output.err.lower()
