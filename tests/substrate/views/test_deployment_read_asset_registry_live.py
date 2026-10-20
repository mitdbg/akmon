from __future__ import annotations

import json
from types import SimpleNamespace
from uuid import uuid4

import psycopg
import pytest

from okg.substrate.contracts.deployment_product import (
    build_read_asset,
    resolve_read_assets,
)
from okg.substrate.derived_artifacts import stable_hash
from okg.substrate.publisher import Publisher
from okg.substrate.views import ViewMaterializer
from okg.substrate.views import registry as registry_module
from okg.substrate.views.registry import (
    ViewRegistryEntry,
    materializers_from_registry_entries,
)
from okg.views.assembly import assemble as default_assemble
from okg.views.planners.base import QuoteSpec, ViewDefinition
from okg.views.types import Anchor
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


pytestmark = pytest.mark.postgres


def test_published_native_view_receipt_points_to_canonical_asset(
    clean_runtime: str,
) -> None:
    view_id = f"test.wave4.read_asset.{uuid4().hex[:12]}"
    contract = build_read_asset(
        owner="test",
        asset_id=view_id,
        kind="view",
        config_schema="okg.deployment_product.asset.view.config/v1",
        result_schema="okg.deployment_product.asset.view.result/v1",
        capability_schema="okg.deployment_product.asset.view.capability/v1",
        output_identity=f"native-view:{view_id}",
        refresh_policy={"mode": "generation"},
        policy_requirements={
            "audiences": ["test-reader"],
            "tenants": ["test-tenant"],
            "visible_fields": ["content"],
            "decision_ids": ["nomos:test:wave4-live"],
        },
        lineage_sources=["fixture:wave4-live"],
        config={"view_id": view_id},
        provenance={"source": "definition", "fixture": "wave4-live"},
    )
    selected = resolve_read_assets([contract], roots=[contract.qualified_id])
    assert selected.canonical.assets == (contract.asset,)

    def planner(corpus: dict[str, Anchor], _view_def: ViewDefinition):
        assert "summary" in corpus
        return [QuoteSpec(
            id="summary-fragment",
            anchor_id="summary",
            display_label="Summary",
        )]

    def assembler(view) -> str:
        return "# Wave 4 read asset\n\n" + default_assemble(view)

    def view_def_factory() -> ViewDefinition:
        return ViewDefinition(
            view_id=view_id,
            planner=planner,
            assembler=assembler,
            view_def_hash=stable_hash({"view_id": view_id, "asset": contract.asset.digest}),
        )

    def build_corpus(_conn, *, generation_id: int):
        return {
            "summary": Anchor.from_content(
                "summary",
                f"Canonical asset {contract.qualified_id} published in generation {generation_id}.",
            )
        }

    materializer = ViewMaterializer(
        view_def_factory=view_def_factory,
        build_corpus=build_corpus,
        name=view_id,
        asset_id=contract.qualified_id,
        asset_digest=contract.asset.digest,
        asset_execution_token=selected.execution_token(contract.qualified_id),
    )

    result = Publisher(
        clean_runtime,
        "default",
        materializers=[materializer],
        coordination_mode="dbos",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    assert result["status"] == "published"
    assert result["generation_id"]
    receipt = result["views"][0]
    assert receipt["freshness"] == "fresh"
    assert receipt["asset_id"] == contract.qualified_id
    assert receipt["asset_digest"] == contract.asset.digest

    with psycopg.connect(clean_runtime) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT r.status, m.target_generation_id
              FROM okg.derived_artifact_runs r
              JOIN okg.derived_output_manifests m
                ON m.manifest_id = r.output_manifest_id
             WHERE r.definition_id = %s
             ORDER BY m.target_generation_id DESC
             LIMIT 1
            """,
            (f"view:{view_id}",),
        )
        persisted = cur.fetchone()

    assert persisted is not None
    assert persisted[0] == "completed"
    assert isinstance(persisted[1], int)
    assert persisted[1] > 0


def test_supported_legacy_registry_asset_executes_but_public_read_requires_migration(
    clean_runtime: str,
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    view_id = f"test.wave4.legacy.{uuid4().hex[:12]}"
    entry = ViewRegistryEntry(
        view_def_id=view_id,
        view_name="Wave 4 legacy migration",
        deployment="default",
        view_def_factory="views.legacy:factory",
        build_corpus="views.legacy:build_corpus",
    )

    def materializer_from_attrs(attrs, **authority):
        def planner(corpus: dict[str, Anchor], _view_def: ViewDefinition):
            assert "summary" in corpus
            return [QuoteSpec(
                id="summary-fragment",
                anchor_id="summary",
                display_label="Summary",
            )]

        def factory() -> ViewDefinition:
            return ViewDefinition(
                view_id=attrs["view_def_id"],
                planner=planner,
                assembler=default_assemble,
                view_def_hash=stable_hash({"view_id": attrs["view_def_id"]}),
            )

        def corpus(_conn, *, generation_id: int):
            return {
                "summary": Anchor.from_content(
                    "summary",
                    f"legacy migration generation {generation_id}",
                )
            }

        return ViewMaterializer(
            view_def_factory=factory,
            build_corpus=corpus,
            name=attrs["view_def_id"],
            asset_id=authority["asset_id"],
            asset_digest=authority["asset_digest"],
            asset_execution_token=authority["asset_execution_token"],
        )

    monkeypatch.setattr(registry_module, "_ensure_deployment_on_syspath", lambda _root: None)
    monkeypatch.setattr(registry_module, "_materializer_from_attrs", materializer_from_attrs)

    materializers = materializers_from_registry_entries(
        [entry],
        deployment_name="default",
        deployment_root=tmp_path,
    )
    result = Publisher(
        clean_runtime,
        "default",
        materializers=materializers,
        coordination_mode="dbos",
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
    ).cycle()

    assert result["status"] == "published"
    assert result["views"][0]["asset_id"] == f"default:{view_id}"
    assert result["views"][0]["asset_digest"].startswith("sha256:")

    (tmp_path / "views_registry.yaml").write_text(
        "\n".join([
            "enabled: true",
            "deployment: default",
            "views:",
            f"  - view_def_id: {view_id}",
            "    view_name: Wave 4 legacy migration",
            "    view_def_factory: views.legacy:factory",
            "    build_corpus: views.legacy:build_corpus",
        ]),
        encoding="utf-8",
    )
    import okg.substrate.cli.view as cli_view_module
    import okg.substrate.deployments as deployments_module

    cli_view_module._load_main_helpers()
    monkeypatch.setattr(deployments_module, "find_deployment_dir", lambda _name: tmp_path)
    monkeypatch.setattr(cli_view_module, "_resolve_deployment_dsn", lambda _args: clean_runtime)
    # Drop the setup publish's progress lines, so stderr holds only the
    # command's JSON error envelope.
    capsys.readouterr()
    rc = cli_view_module._cmd_view_show(
        SimpleNamespace(
            deployment="default",
            view=view_id,
            slot="markdown",
            generation=None,
            definition_id=None,
            audience="legacy-migration",
            tenant="legacy-migration",
            dsn=clean_runtime,
        ),
        as_json=True,
    )

    assert rc == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    payload = json.loads(captured.err)
    assert payload["error"]["data"]["reason"] == (
        "legacy_view_policy_migration_required"
    )
    assert "canonical view asset" in payload["error"]["fix_hint"]

    inspect_rc = cli_view_module._cmd_view_inspect(
        SimpleNamespace(
            deployment="default",
            view=view_id,
            slot="markdown",
            generation=None,
            definition_id=None,
            preview_limit=0,
            dsn=clean_runtime,
        ),
        as_json=True,
    )
    assert inspect_rc == 0
    inspection = json.loads(capsys.readouterr().out)
    assert {
        "inline_payload",
        "payload",
        "storage_ref",
    }.isdisjoint(inspection["slot"])
    assert "legacy migration generation" not in json.dumps(inspection)
