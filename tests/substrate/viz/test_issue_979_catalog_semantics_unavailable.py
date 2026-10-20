from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from okg.viz.context import CatalogSemanticsUnavailable, VizContext


class _Composed:
    narrowings = [
        SimpleNamespace(
            edge_archetype="references",
            parent_subtype="publication",
            child_subtype="section",
        )
    ]

    @staticmethod
    def relation_kind_for_archetype(_archetype: str) -> str:
        return "references"


class _HealthyGraph:
    @staticmethod
    def query(_sql: str) -> list[dict[str, object]]:
        return [
            {"subtype": "publication", "n": 50},
            {"subtype": "section", "n": 100},
        ]


class _FailingStore:
    @classmethod
    def from_connection(cls, *_args: object, **_kwargs: object) -> "_FailingStore":
        return cls()

    def checkout_generation(self, *_args: object, **_kwargs: object) -> None:
        raise RuntimeError("injected subtype-count failure")


class _HealthyStore:
    @classmethod
    def from_connection(cls, *_args: object, **_kwargs: object) -> "_HealthyStore":
        return cls()

    def checkout_generation(self, *_args: object, **_kwargs: object) -> _HealthyGraph:
        return _HealthyGraph()


def _ctx() -> VizContext:
    ctx = VizContext(
        dsn="postgresql://unused",
        deployment_name="demo",
        manifest={},
        config={},
        pool=None,  # type: ignore[arg-type]
        session=SimpleNamespace(  # type: ignore[arg-type]
            generation_id=42,
            branch_name="default",
        ),
        deployment_dir=Path("deployments/demo"),
    )

    @contextmanager
    def conn():
        yield object()

    ctx.conn = conn  # type: ignore[method-assign]
    return ctx


def test_compose_failure_is_not_cached_and_corrected_retry_recovers(
    monkeypatch,
) -> None:
    ctx = _ctx()
    calls = 0

    def compose(_path: Path):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("injected catalog compose failure")
        return _Composed()

    monkeypatch.setattr(
        "okg.substrate.catalog.modules_compose.compose_catalog",
        compose,
    )

    with pytest.raises(CatalogSemanticsUnavailable) as caught:
        ctx.archetype_kinds()

    assert caught.value.to_dict() == {
        "status": "unavailable",
        "cause": "archetype_catalog_compose_failed",
        "omitted_work": ["archetype_relation_kind_classification"],
        "scope": {"deployment": "demo", "generation_id": 42},
        "retryable": True,
        "freshness": "unavailable",
    }
    assert ctx._archetype_kinds_cache is None
    assert ctx.archetype_kinds() == {"references": "references"}
    assert ctx._archetype_kinds_cache == {"references": "references"}


def test_count_failure_is_not_cached_and_corrected_retry_recovers(
    monkeypatch,
) -> None:
    ctx = _ctx()
    monkeypatch.setattr(
        "okg.substrate.catalog.modules_compose.compose_catalog",
        lambda _path: _Composed(),
    )
    monkeypatch.setattr(
        "okg.substrate.graph_store.ChronosGraphStore",
        _FailingStore,
    )

    with pytest.raises(CatalogSemanticsUnavailable) as caught:
        ctx.absorbed_subtypes()

    assert caught.value.to_dict() == {
        "status": "unavailable",
        "cause": "absorption_subtype_count_failed",
        "omitted_work": [
            "subtype_population_counts",
            "absorption_catalog_rules",
        ],
        "scope": {"deployment": "demo", "generation_id": 42},
        "retryable": True,
        "freshness": "unavailable",
    }
    assert ctx._absorption_cache is None

    monkeypatch.setattr(
        "okg.substrate.graph_store.ChronosGraphStore",
        _HealthyStore,
    )
    assert ctx.absorbed_subtypes() == {"section": "publication"}
    assert ctx._absorption_cache == {"section": "publication"}


def test_absorption_compose_failure_has_its_exact_unavailable_shape(
    monkeypatch,
) -> None:
    ctx = _ctx()
    monkeypatch.setattr(
        "okg.substrate.catalog.modules_compose.compose_catalog",
        lambda _path: (_ for _ in ()).throw(
            RuntimeError("injected absorption compose failure")
        ),
    )

    with pytest.raises(CatalogSemanticsUnavailable) as caught:
        ctx.absorbed_subtypes()

    assert caught.value.to_dict() == {
        "status": "unavailable",
        "cause": "absorption_catalog_compose_failed",
        "omitted_work": ["absorption_catalog_rules"],
        "scope": {"deployment": "demo", "generation_id": 42},
        "retryable": True,
        "freshness": "unavailable",
    }
    assert ctx._absorption_cache is None


def test_visualization_api_returns_typed_unavailable_operator_state() -> None:
    pytest.importorskip("fastapi")
    from fastapi.testclient import TestClient
    from okg.viz.app import build_app

    class _ApiContext:
        deployment_name = "demo"
        config = {"tiers": {"publication": "primary"}}
        session = SimpleNamespace(generation_id=42)

        @staticmethod
        def archetype_kinds() -> dict[str, str]:
            raise CatalogSemanticsUnavailable(
                cause="archetype_catalog_compose_failed",
                omitted_work=["archetype_relation_kind_classification"],
                deployment="demo",
                generation_id=42,
            )

    client = TestClient(
        build_app(ctx_provider=_ApiContext, is_ready=lambda: True),
        raise_server_exceptions=False,
    )
    response = client.get("/viz/api/hierarchy/root")

    assert response.status_code == 503
    assert response.json() == {
        "ok": False,
        "ready": False,
        "error": "catalog_semantics_unavailable",
        "path": "/viz/api/hierarchy/root",
        "status": "unavailable",
        "cause": "archetype_catalog_compose_failed",
        "omitted_work": ["archetype_relation_kind_classification"],
        "scope": {"deployment": "demo", "generation_id": 42},
        "retryable": True,
        "freshness": "unavailable",
    }
