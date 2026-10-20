from __future__ import annotations

from pathlib import Path

import pytest

from okg.substrate.publisher.deployment_stack import (
    _clear_deployment_stack_cache,
    build_deployment_publisher_stack,
    discover_materializers,
    embeddings_enabled,
    publisher_kwargs_from_env,
)


def _write_manifest(tmp_path: Path, body: str) -> Path:
    deployment = tmp_path / "deployment"
    deployment.mkdir()
    (deployment / "deployment.yaml").write_text(body, encoding="utf-8")
    return deployment


def _write_registry_view_deployment(tmp_path: Path) -> Path:
    deployment = _write_manifest(tmp_path, "name: test\n")
    _write_dummy_view_module(deployment)
    _write_dummy_views_registry(deployment)
    return deployment


def _write_dummy_view_module(deployment: Path) -> None:
    views_dir = deployment / "views"
    views_dir.mkdir(exist_ok=True)
    (views_dir / "__init__.py").write_text("", encoding="utf-8")
    (views_dir / "dummy.py").write_text(
        "from okg.views.planners.base import QuoteSpec, ViewDefinition\n"
        "from okg.views.types import Anchor\n"
        "\n"
        "def planner(corpus, view_def):\n"
        "    return [QuoteSpec(id='summary', anchor_id='summary')]\n"
        "\n"
        "def assembler(view):\n"
        "    return '# Registry View\\n'\n"
        "\n"
        "def factory():\n"
        "    return ViewDefinition(\n"
        "        view_id='test.registry_view',\n"
        "        planner=planner,\n"
        "        assembler=assembler,\n"
        "        view_def_hash='test-registry-view-v1',\n"
        "    )\n"
        "\n"
        "def build_corpus(conn, *, generation_id):\n"
        "    return {'summary': Anchor.from_content('summary', 'registry')}\n",
        encoding="utf-8",
    )


def _write_dummy_views_registry(deployment: Path) -> None:
    (deployment / "views_registry.yaml").write_text(
        "deployment: test\n"
        "views:\n"
        "  - view_def_id: test.registry_view\n"
        "    view_def_factory: views.dummy:factory\n"
        "    build_corpus: views.dummy:build_corpus\n",
        encoding="utf-8",
    )


def test_embeddings_enabled_honors_canonical_plural_key(
    tmp_path: Path,
) -> None:
    deployment = _write_manifest(
        tmp_path,
        "name: test\nembeddings:\n  enabled: false\n",
    )

    assert embeddings_enabled(deployment) is False


def test_embeddings_enabled_honors_legacy_singular_key(
    tmp_path: Path,
) -> None:
    deployment = _write_manifest(
        tmp_path,
        "name: test\nembedding:\n  enabled: false\n",
    )

    with pytest.warns(UserWarning, match="deprecated"):
        assert embeddings_enabled(deployment) is False


def test_embeddings_enabled_defaults_to_false(tmp_path: Path) -> None:
    deployment = _write_manifest(tmp_path, "name: test\n")

    assert embeddings_enabled(deployment) is False


def test_publisher_kwargs_from_env_includes_native_view_refresh_knobs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("OKG_GATE_TIMEOUT_MS", raising=False)
    monkeypatch.setenv("OKG_NATIVE_VIEW_MIN_REFRESH_MS", "250")
    monkeypatch.setenv("OKG_NATIVE_VIEW_MATERIALIZATION_DELAY_MS", "100")
    monkeypatch.setenv("OKG_ATTRIBUTE_EFFECTIVE_VIEW_EMPTY_OVERLAY_FAST_PATH", "1")

    assert publisher_kwargs_from_env() == {
        "native_view_min_refresh_interval_ms": 250,
        "native_view_materialization_delay_ms": 100,
        "attribute_effective_view_empty_overlay_fast_path": True,
    }


def test_build_stack_reads_native_view_refresh_policy(
    tmp_path: Path,
) -> None:
    deployment = _write_manifest(
        tmp_path,
        "name: test\n"
        "native_views:\n"
        "  min_refresh_interval_ms: 250\n"
        "  materialization_delay_ms: 100\n",
    )
    _clear_deployment_stack_cache()

    stack = build_deployment_publisher_stack(
        dsn="postgres://invalid",
        deployment_name="test",
        deployment_dir=deployment,
    )

    assert stack.publisher_kwargs()["native_view_min_refresh_interval_ms"] == 250
    assert stack.publisher_kwargs()["native_view_materialization_delay_ms"] == 100
    assert (
        stack.publisher_kwargs()["attribute_effective_view_empty_overlay_fast_path"]
        is False
    )


def test_build_stack_reads_attribute_view_empty_overlay_policy(
    tmp_path: Path,
) -> None:
    deployment = _write_manifest(
        tmp_path,
        "name: test\n"
        "native_views:\n"
        "  attribute_effective_view_empty_overlay_fast_path: true\n",
    )
    _clear_deployment_stack_cache()

    stack = build_deployment_publisher_stack(
        dsn="postgres://invalid",
        deployment_name="test",
        deployment_dir=deployment,
    )

    assert (
        stack.publisher_kwargs()["attribute_effective_view_empty_overlay_fast_path"]
        is True
    )


def test_build_stack_carries_history_retention_to_publisher(tmp_path: Path) -> None:
    deployment = _write_manifest(
        tmp_path,
        "name: test\n"
        "chronos:\n"
        "  history_retention:\n"
        "    default: full\n"
        "    audit_tail_generations: 3\n"
        "    nodes:\n"
        "      claim: latest-only\n",
    )
    _clear_deployment_stack_cache()

    stack = build_deployment_publisher_stack(
        dsn="postgres://invalid",
        deployment_name="test",
        deployment_dir=deployment,
    )

    assert stack.publisher_kwargs()["history_retention_policy"] == {
        "default": "full",
        "audit_tail_generations": 3,
        "nodes": {"claim": "latest-only"},
    }


def test_build_stack_loads_maintained_derivation_registry(
    tmp_path: Path,
) -> None:
    deployment = _write_manifest(
        tmp_path,
        "name: test\n"
        "maintained_derivations:\n"
        "  registry: maintained_registry:build_registry\n",
    )
    (deployment / "maintained_registry.py").write_text(
        "from okg.substrate.derived_artifacts.derivation_registry import (\n"
        "    DeclaredDeps, DerivationRow, DeterminismClass, ExecutionPhase,\n"
        "    GovernanceProfile, Materialization,\n"
        ")\n"
        "from okg.substrate.derived_artifacts.maintained_derivation import (\n"
        "    ChronosRegisteredTable, CorrectnessOracle, DerivationSpec,\n"
        "    MaintainResult, SentinelSpec,\n"
        ")\n"
        "\n"
        "def _noop(ctx):\n"
        "    return MaintainResult([], [], 0, None, 'full_recount')\n"
        "\n"
        "def build_registry():\n"
        "    sentinel = SentinelSpec(pk_columns=['k'], cadence_column='v')\n"
        "    spec = DerivationSpec(\n"
        "        derivation_id='deployment-row', sentinel=sentinel,\n"
        "        oracle=CorrectnessOracle('SELECT 1 AS k, 1 AS v', ['k'], ['v']),\n"
        "        force_full_predicates=[], full_recount=_noop,\n"
        "        incremental=_noop, self_heal_every=25,\n"
        "    )\n"
        "    return [DerivationRow(\n"
        "        spec=spec,\n"
        "        declared_deps=DeclaredDeps(kinds=frozenset({'graph_node'})),\n"
        "        materializations=[Materialization(\n"
        "            kind='side_table',\n"
        "            storage=ChronosRegisteredTable('okg.test_row', lambda store: None),\n"
        "            sentinel=sentinel,\n"
        "        )],\n"
        "        governance=GovernanceProfile(\n"
        "            determinism=DeterminismClass.DETERMINISTIC,\n"
        "        ),\n"
        "        execution_phase=ExecutionPhase.PRE_CHECKPOINT,\n"
        "    )]\n",
        encoding="utf-8",
    )
    _clear_deployment_stack_cache()

    stack = build_deployment_publisher_stack(
        dsn="postgres://invalid",
        deployment_name="test",
        deployment_dir=deployment,
    )

    rows = stack.publisher_kwargs()["maintained_derivations"].rows()
    assert [row.spec.derivation_id for row in rows] == ["deployment-row"]


def test_build_stack_rejects_invalid_maintained_derivation_iterable(
    tmp_path: Path,
) -> None:
    deployment = _write_manifest(
        tmp_path,
        "name: test\n"
        "maintained_derivations:\n"
        "  registry: maintained_registry:build_registry\n",
    )
    (deployment / "maintained_registry.py").write_text(
        "def build_registry():\n"
        "    return ['not-a-row']\n",
        encoding="utf-8",
    )
    _clear_deployment_stack_cache()

    with pytest.raises(ValueError, match="DerivationRow"):
        build_deployment_publisher_stack(
            dsn="postgres://invalid",
            deployment_name="test",
            deployment_dir=deployment,
        )


def test_build_stack_disables_default_embedding_alias_backend(
    tmp_path: Path,
) -> None:
    deployment = _write_manifest(
        tmp_path,
        "name: test\nembedding:\n  enabled: false\n",
    )
    _clear_deployment_stack_cache()

    with pytest.warns(UserWarning, match="deprecated"):
        stack = build_deployment_publisher_stack(
            dsn="postgres://invalid",
            deployment_name="test",
            deployment_dir=deployment,
        )
        resolver = stack.resolver_factory(conn=object())

    assert [type(b).__name__ for b in resolver._backends] == [
        "SqlCascadeBackend",
    ]


def test_build_stack_plural_enablement_uses_verified_alias_backend(
    tmp_path: Path,
) -> None:
    deployment = _write_manifest(
        tmp_path,
        "name: test\nembeddings:\n  enabled: true\n  backend: hash\n",
    )
    _clear_deployment_stack_cache()

    stack = build_deployment_publisher_stack(
        dsn="postgres://invalid",
        deployment_name="test",
        deployment_dir=deployment,
    )
    resolver = stack.resolver_factory(conn=object())

    assert stack.enqueue_embeddings is True
    assert [type(b).__name__ for b in resolver._backends] == [
        "SqlCascadeBackend", "EmbeddingAliasBackend",
    ]


def test_discover_materializers_without_registry_does_not_query_catalog(
    tmp_path: Path,
    monkeypatch,
) -> None:
    deployment = _write_manifest(tmp_path, "name: test\n")

    def fail_get_pool(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("catalog should not be queried")

    import okg.substrate.db.pool as pool

    monkeypatch.setattr(pool, "get_pool", fail_get_pool)

    assert discover_materializers(
        dsn="postgres://invalid",
        deployment_name="test",
        deployment_root=deployment,
    ) == []


def test_discover_materializers_uses_registry_without_catalog_query(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    deployment = _write_registry_view_deployment(tmp_path)

    def fail_get_pool(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("catalog should not be queried for view discovery")

    import okg.substrate.db.pool as pool

    monkeypatch.setattr(pool, "get_pool", fail_get_pool)

    materializers = discover_materializers(
        dsn="postgres://invalid",
        deployment_name="test",
        deployment_root=deployment,
    )

    assert [materializer.name for materializer in materializers] == [
        "test.registry_view",
    ]
    assert materializers[0].view_def_factory().view_id == "test.registry_view"


def test_build_stack_cache_invalidates_when_views_registry_appears(
    tmp_path: Path,
) -> None:
    deployment = _write_manifest(tmp_path, "name: test\n")
    _write_dummy_view_module(deployment)
    _clear_deployment_stack_cache()

    first = build_deployment_publisher_stack(
        dsn="postgres://invalid",
        deployment_name="test",
        deployment_dir=deployment,
    )
    assert first.materializers == []

    _write_dummy_views_registry(deployment)

    second = build_deployment_publisher_stack(
        dsn="postgres://invalid",
        deployment_name="test",
        deployment_dir=deployment,
    )

    assert [materializer.name for materializer in second.materializers] == [
        "test.registry_view",
    ]


def test_a_stranded_publisher_env_var_is_announced_not_silently_ignored(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Issue #1479 took two knobs away from the environment. Say so.

    A live deployment outside this repository may still be driven by a runbook
    that exports them -- pact/changes/rebuild-af-pubs-interval-storage records
    the af_pubs cold rebuild exporting 7200s rather than the 900s default.
    Ignoring that export in silence would hand the rebuild the default bound
    with nothing in the log to explain the change.
    """
    import logging

    from okg.substrate.publisher import publisher as publisher_module

    monkeypatch.setattr(
        publisher_module, "_ignored_publisher_env_announced", set(),
    )
    monkeypatch.setenv("OKG_PUBLISH_STATEMENT_TIMEOUT_SECONDS", "7200")
    monkeypatch.setenv("OKG_PUBLISH_LOCK_HOLD_HEADROOM_FACTOR", "3")
    with caplog.at_level(logging.WARNING, logger=publisher_module.__name__):
        publisher_module._warn_ignored_publisher_env()
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "OKG_PUBLISH_STATEMENT_TIMEOUT_SECONDS" in text
    assert "publisher.hard_statement_timeout_seconds" in text
    assert "OKG_PUBLISH_LOCK_HOLD_HEADROOM_FACTOR" in text
    assert "publisher.lock_hold_headroom_factor" in text

    # Once per process, not once per publish cycle.
    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=publisher_module.__name__):
        publisher_module._warn_ignored_publisher_env()
    assert caplog.records == []


def test_no_warning_when_the_retired_publisher_env_is_unset(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    from okg.substrate.publisher import publisher as publisher_module

    monkeypatch.setattr(
        publisher_module, "_ignored_publisher_env_announced", set(),
    )
    monkeypatch.delenv("OKG_PUBLISH_STATEMENT_TIMEOUT_SECONDS", raising=False)
    monkeypatch.delenv("OKG_PUBLISH_LOCK_HOLD_HEADROOM_FACTOR", raising=False)
    with caplog.at_level(logging.WARNING, logger=publisher_module.__name__):
        publisher_module._warn_ignored_publisher_env()
    assert caplog.records == []
    # A no-op must not burn the once-per-process latch.
    assert publisher_module._ignored_publisher_env_announced == set()


def test_the_real_stack_can_never_omit_the_publisher_authority_keys() -> None:
    """Where the authority lives, the keys are structurally guaranteed.

    Issue #1479: the factory restores these keys after the environment layer so
    an operator export cannot rewrite manifest authority. It skips any key the
    stack did not declare, because `build_deployment_publisher` accepts any
    duck-typed object with a `publisher_kwargs()` and several callers pass a
    minimal stand-in. That tolerance must never become a way for the REAL stack
    to lose a key quietly, so pin the contract here: a DeploymentPublisherStack
    carries a typed PublisherConfig on a required field and always emits all
    eight keys, whatever the deployment declared.
    """
    import dataclasses

    from okg.substrate.deployments import PublisherConfig
    from okg.substrate.publisher.deployment_stack import DeploymentPublisherStack
    from okg.substrate.publisher.factory import _DEPLOYMENT_PUBLISHER_AUTHORITY

    fields = list(dataclasses.fields(DeploymentPublisherStack))
    publisher_field = next(f for f in fields if f.name == "publisher_config")
    # No default: a stack cannot be built without one.
    assert publisher_field.default is dataclasses.MISSING
    assert publisher_field.default_factory is dataclasses.MISSING

    stack = DeploymentPublisherStack(**{
        f.name: (PublisherConfig() if f.name == "publisher_config" else None)
        for f in fields
    })
    assert sorted(_DEPLOYMENT_PUBLISHER_AUTHORITY - set(stack.publisher_kwargs())) == []
