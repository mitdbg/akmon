from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


def _patch_viz_startup(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    import uvicorn

    from okg.viz import __main__ as viz_main
    from okg.viz import app as viz_app
    from okg.viz import auth as viz_auth
    from okg.viz import boot as viz_boot
    from okg.viz import coordination_services

    captured: dict[str, Any] = {}
    monkeypatch.setattr(viz_main, "_register_auth_gated_bind", lambda *_, **__: None)
    monkeypatch.setattr(viz_boot, "get_ctx_lazy", lambda: (lambda: None, lambda: True))
    monkeypatch.setattr(viz_boot, "preload_in_background", lambda **_: None)
    monkeypatch.setattr(viz_auth, "resolve_operator_token", lambda: "operator-token")
    monkeypatch.setattr(viz_app, "build_app", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(uvicorn, "run", lambda *_args, **_kwargs: None)

    def build_services(**kwargs: Any) -> Any:
        captured.update(kwargs)
        return SimpleNamespace(
            operator_control_service=object(),
            approval_control_service=object(),
            setup_plan_service=object(),
        )

    monkeypatch.setattr(
        coordination_services,
        "build_native_coordination_services",
        build_services,
    )
    return captured


def test_viz_startup_injects_explicit_production_setup_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.viz import __main__ as viz_main
    from okg.viz import coordination_adapters

    captured = _patch_viz_startup(monkeypatch)
    config = tmp_path / "setup.json"
    adapters = SimpleNamespace(
        setup_probes=object(),
        chief_resolver=object(),
        pair_request_factory=object(),
        lifecycle_factory=object(),
    )
    adapter_call: dict[str, Any] = {}

    def build_adapters(**kwargs: Any) -> Any:
        adapter_call.update(kwargs)
        return adapters

    monkeypatch.setattr(
        coordination_adapters,
        "build_production_setup_adapters",
        build_adapters,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "okg.viz",
            "--dsn",
            "postgresql://not-returned",
            "--deployment",
            "demo",
            "--coordination-setup-config",
            str(config),
        ],
    )

    assert viz_main.main() == 0
    assert adapter_call["config_path"] == config
    assert adapter_call["deployment_name"] == "demo"
    assert captured["setup_probes"] is adapters.setup_probes
    assert captured["chief_resolver"] is adapters.chief_resolver
    assert captured["pair_request_factory"] is adapters.pair_request_factory
    assert captured["lifecycle_factory"] is adapters.lifecycle_factory


def test_viz_startup_keeps_setup_unavailable_when_config_is_omitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.viz import __main__ as viz_main
    from okg.viz import coordination_adapters

    captured = _patch_viz_startup(monkeypatch)
    monkeypatch.delenv("OKG_COORDINATION_SETUP_CONFIG", raising=False)
    monkeypatch.setattr(
        coordination_adapters,
        "build_production_setup_adapters",
        lambda **_: pytest.fail("setup adapters must not load without explicit config"),
    )
    monkeypatch.setattr(
        "sys.argv",
        ["okg.viz", "--dsn", "postgresql://not-returned", "--deployment", "demo"],
    )

    assert viz_main.main() == 0
    assert "setup_probes" not in captured


def test_viz_startup_fails_closed_for_unsafe_explicit_setup_config(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from okg.viz import __main__ as viz_main
    from okg.viz import coordination_adapters

    captured = _patch_viz_startup(monkeypatch)

    def refuse(**_: Any) -> Any:
        raise coordination_adapters.ProductionSetupUnavailable(
            "private setup authority is unsafe"
        )

    monkeypatch.setattr(
        coordination_adapters,
        "build_production_setup_adapters",
        refuse,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "okg.viz",
            "--dsn",
            "postgresql://not-returned",
            "--deployment",
            "demo",
            "--coordination-setup-config",
            str(tmp_path / "unsafe.json"),
        ],
    )

    assert viz_main.main() == 1
    assert captured == {}
