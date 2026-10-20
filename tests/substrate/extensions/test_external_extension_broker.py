"""Public SDK and out-of-process extension broker contract proof."""

from __future__ import annotations

import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import psycopg
import pytest
import yaml

from okg.substrate.contracts.deployment_product import (
    ConnectionContractError,
    GovernedToolBindingDefinition,
    ProviderConnectionDefinition,
    SourceBindingDefinition,
)
from okg.substrate.extension_broker import (
    EXTENSION_PACKAGE_SCHEMA,
    ExtensionBoundaryError,
    _broker_environment,
    _linux_interpreter_library,
    plan_extension_package,
)
from okg.substrate.extension_admission import (
    ExtensionCapacityPolicy,
    ExtensionReservationLedger,
    ResourceQuantity,
    admit_extension_plan,
    build_extension_admission_plan,
    execute_admitted_extension,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.ingest import SourceRunner
from okg.substrate.extension_lifecycle import (
    BrokeredExtensionSourceAdapter,
    ExtensionInvocation,
)
from okg.substrate.publisher.publisher import Publisher
from tests.substrate._db_helpers import seed_catalog_version
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


FIXTURE = (
    Path(__file__).parents[2]
    / "fixtures"
    / "deployment_products"
    / "external_extension"
)
SOURCE = "external-extension-fixture"
POLICY_FIXTURE = FIXTURE.parent / "extension_admission" / "policy.json"
CONNECTION_FIXTURE = FIXTURE.parent / "shared_connection" / "product.yaml"
NOW = datetime(2026, 8, 18, 12, 0, tzinfo=timezone.utc)


def _copy_fixture(tmp_path: Path) -> Path:
    target = tmp_path / "external-package"
    shutil.copytree(FIXTURE, target)
    return target


def _descriptor(root: Path) -> dict[str, Any]:
    return json.loads((root / "okg-extension.json").read_text())


def _write_descriptor(root: Path, value: dict[str, Any]) -> None:
    (root / "okg-extension.json").write_text(json.dumps(value, sort_keys=True))


def _authorization(plan: Any, *extension_ids: str, phase: str = "apply"):
    raw = json.loads(POLICY_FIXTURE.read_text())
    policy = ExtensionCapacityPolicy(
        owner=raw["owner"],
        snapshot_id=raw["snapshot_id"],
        observed_at=NOW,
        budgets=tuple(ResourceQuantity(**item) for item in raw["budgets"]),
        capabilities=tuple(raw["capabilities"]),
        provider_effects=tuple(raw["provider_effects"]),
    )
    admission_plan = build_extension_admission_plan(
        [(plan, extension_id) for extension_id in extension_ids],
        policy=policy,
        created_at=NOW,
    )
    ledger = ExtensionReservationLedger()
    receipt, lease = admit_extension_plan(
        admission_plan,
        policy=policy,
        ledger=ledger,
        now=NOW,
        phase=phase,
        current_packages={extension_id: plan for extension_id in extension_ids},
    )
    return admission_plan, receipt, ledger, lease


def _connection_contract() -> tuple[
    ProviderConnectionDefinition,
    tuple[SourceBindingDefinition, ...],
    GovernedToolBindingDefinition,
]:
    raw = yaml.safe_load(CONNECTION_FIXTURE.read_text())
    return (
        ProviderConnectionDefinition.model_validate(raw["connection"]),
        tuple(
            SourceBindingDefinition.model_validate(item)
            for item in raw["source_bindings"]
        ),
        GovernedToolBindingDefinition.model_validate(raw["tool_binding"]),
    )


def test_metadata_plan_pins_package_environment_entrypoints_provenance_and_sbom_without_import() -> (
    None
):
    before_modules = set(sys.modules)
    plan = plan_extension_package(FIXTURE)

    assert plan.package_name == "okg-wave3-external-fixture"
    assert plan.package_version == "1.0.0"
    assert plan.python_abi == "cpython-312"
    assert plan.execution_tier == "out-of-process"
    assert all(
        value.startswith("sha256:")
        for value in (
            plan.package_digest,
            plan.dependency_digest,
            plan.entrypoint_digest,
            plan.provenance_digest,
            plan.sbom_digest,
            plan.plan_digest,
        )
    )
    assert {item.extension_id for item in plan.entries} == {
        "fixture.connector",
        "fixture.enricher",
        "fixture.tool",
        "fixture.network-probe",
        "fixture.filesystem-probe",
        "fixture.flaky",
    }
    assert set(sys.modules) == before_modules
    assert not any(name.startswith("fixture_") for name in sys.modules)


def test_public_sdk_entries_execute_out_of_process_with_declared_capabilities() -> None:
    plan = plan_extension_package(FIXTURE)
    admission, receipt, ledger, lease = _authorization(
        plan,
        "fixture.connector",
        "fixture.enricher",
        "fixture.tool",
    )
    with lease:
        first = execute_admitted_extension(
            plan,
            admission,
            receipt,
            ledger,
            extension_id="fixture.connector",
            binding_id="source-alpha",
            capabilities=("provider.source.read",),
            payload={"item": {"id": "alpha", "label": "Alpha"}},
        )
        second = execute_admitted_extension(
            plan,
            admission,
            receipt,
            ledger,
            extension_id="fixture.connector",
            binding_id="source-beta",
            capabilities=("provider.source.read",),
            payload={"item": {"id": "beta", "label": "Beta"}},
        )
        edge = execute_admitted_extension(
            plan,
            admission,
            receipt,
            ledger,
            extension_id="fixture.enricher",
            binding_id="source-alpha",
            capabilities=("graph.output.propose",),
            payload={
                "src": first.batch.nodes[0].node_id,
                "dst": second.batch.nodes[0].node_id,
            },
        )
        tool = execute_admitted_extension(
            plan,
            admission,
            receipt,
            ledger,
            extension_id="fixture.tool",
            binding_id="search-tool",
            capabilities=("provider.metadata.read",),
            payload={"query": "Alpha"},
        )

    assert first.batch.nodes[0].attrs["binding_id"] == "source-alpha"
    assert second.batch.nodes[0].attrs["binding_id"] == "source-beta"
    assert edge.batch.edges[0].src != edge.batch.edges[0].dst
    assert tool.batch.observations == {"result": "Alpha"}
    assert ledger.active_count == 0
    assert not any(name.startswith("fixture_") for name in sys.modules)


def test_broker_environment_derives_linux_loader_path_without_inheriting_ambient(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = tmp_path / "runtime"
    interpreter = runtime / "bin" / "python"
    interpreter.parent.mkdir(parents=True)
    interpreter.touch()
    configured_library = runtime / "lib"
    configured_library.mkdir()
    configured_libpython = configured_library / "libpython3.12.so.1.0"
    configured_libpython.touch()
    fallback_library = runtime / "lib64"
    fallback_library.mkdir()
    fallback_libpython = fallback_library / "libpython3.12.so.1.0"
    fallback_libpython.touch()
    shim = tmp_path / "shim" / "bin" / "python"
    shim.parent.mkdir(parents=True)
    shim.symlink_to(interpreter)
    monkeypatch.setattr(sys, "executable", str(shim))
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr(
        "okg.substrate.extension_broker.sysconfig.get_config_var",
        lambda _name: str(configured_library),
    )
    monkeypatch.setenv("PATH", "/trusted/bin")
    monkeypatch.setenv("LD_LIBRARY_PATH", "/ambient/untrusted")
    monkeypatch.setenv("OKG_DSN", "postgresql://must-not-cross")

    environment = _broker_environment()
    assert environment["PATH"] == "/trusted/bin"
    assert environment["LD_LIBRARY_PATH"] == str(configured_library)
    assert "OKG_DSN" not in environment
    assert "/ambient/untrusted" not in environment.values()

    configured_libpython.unlink()
    assert _linux_interpreter_library() == fallback_library

    fallback_libpython.unlink()
    assert _linux_interpreter_library() is None
    assert "LD_LIBRARY_PATH" not in _broker_environment()


@pytest.mark.postgres
def test_external_package_outputs_publish_through_existing_lifecycle(
    clean_runtime: str,
    record_property,
) -> None:
    plan = plan_extension_package(FIXTURE)
    admission, receipt, ledger, lease = _authorization(
        plan,
        "fixture.connector",
        "fixture.enricher",
        phase="discovery",
    )
    connection, source_bindings, _tool_binding = _connection_contract()

    with psycopg.connect(clean_runtime) as conn:
        seed_catalog_version(conn)
        conn.commit()
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        before_nodes = int(
            store.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"]
        )
        before_edges = int(
            store.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"]
        )
    finally:
        store.close()

    with lease:
        run = SourceRunner(clean_runtime, source_name=SOURCE).run(
            BrokeredExtensionSourceAdapter(
                source_name=SOURCE,
                connection=connection,
                package=plan,
                admission_plan=admission,
                admission_receipt=receipt,
                reservation_ledger=ledger,
                invocations=(
                    ExtensionInvocation(
                        extension_id="fixture.connector",
                        binding=source_bindings[0],
                        capabilities=("provider.source.read",),
                        payload={"item": {"id": "alpha", "label": "Alpha"}},
                    ),
                    ExtensionInvocation(
                        extension_id="fixture.connector",
                        binding=source_bindings[1],
                        capabilities=("provider.source.read",),
                        payload={"item": {"id": "beta", "label": "Beta"}},
                    ),
                    ExtensionInvocation(
                        extension_id="fixture.enricher",
                        binding=source_bindings[0],
                        capabilities=("graph.output.propose",),
                        payload={
                            "src": "external:source-mail:alpha",
                            "dst": "external:source-calendar:beta",
                        },
                    ),
                ),
            ),
            mode="scope_complete",
        )
        assert run.nodes_appended == 2
        assert run.edges_appended == 1
        published = Publisher(clean_runtime, "default", enrichers=[], catalog_identity=bind_test_catalog_writer(clean_runtime, "default")).cycle()
        assert published["status"] == "published", published
        generation_id = published["generation_id"]
    assert ledger.active_count == 0

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        generation = store.checkout_generation(generation_id)
        after_nodes = int(
            generation.query("SELECT count(*) AS n FROM okg.graph_nodes")[0]["n"]
        )
        after_edges = int(
            generation.query("SELECT count(*) AS n FROM okg.graph_edges")[0]["n"]
        )
        attributed_nodes = generation.query(
            "SELECT node_id FROM okg.graph_nodes WHERE source = :source ORDER BY node_id",
            {"source": SOURCE},
        )
        attributed_edges = generation.query(
            "SELECT src, dst FROM okg.graph_edges WHERE source = :source",
            {"source": SOURCE},
        )
    finally:
        store.close()

    assert after_nodes - before_nodes == 2
    assert after_edges - before_edges == 1
    assert len(attributed_nodes) == 2
    assert len(attributed_edges) == 1
    record_property("published_generation", str(generation_id))
    record_property("published_status", "published")
    record_property("nodes_live_delta", 2)
    record_property("edges_live_delta", 1)


def test_brokered_lifecycle_refuses_disabled_typed_binding_before_launch() -> None:
    plan = plan_extension_package(FIXTURE)
    admission, receipt, ledger, _lease = _authorization(
        plan,
        "fixture.connector",
        phase="discovery",
    )
    connection, source_bindings, _tool_binding = _connection_contract()
    disabled = source_bindings[0].model_copy(update={"enabled": False})

    with pytest.raises(ConnectionContractError) as caught:
        BrokeredExtensionSourceAdapter(
            source_name=SOURCE,
            connection=connection,
            package=plan,
            admission_plan=admission,
            admission_receipt=receipt,
            reservation_ledger=ledger,
            invocations=(
                ExtensionInvocation(
                    extension_id="fixture.connector",
                    binding=disabled,
                    capabilities=("provider.source.read",),
                    payload={},
                ),
            ),
        )
    assert caught.value.code == "connection_binding_disabled"


def test_brokered_discovery_refuses_apply_phase_admission_receipt() -> None:
    plan = plan_extension_package(FIXTURE)
    admission, receipt, ledger, _lease = _authorization(plan, "fixture.connector")
    connection, source_bindings, _tool_binding = _connection_contract()

    with pytest.raises(ValueError, match="discovery admission receipt"):
        BrokeredExtensionSourceAdapter(
            source_name=SOURCE,
            connection=connection,
            package=plan,
            admission_plan=admission,
            admission_receipt=receipt,
            reservation_ledger=ledger,
            invocations=(
                ExtensionInvocation(
                    extension_id="fixture.connector",
                    binding=source_bindings[0],
                    capabilities=("provider.source.read",),
                    payload={},
                ),
            ),
        )


@pytest.mark.parametrize(
    "source,code",
    [
        ("import okg.substrate\n", "extension_private_import_refused"),
        ("import psycopg\n", "extension_private_import_refused"),
        ("import os\nos.getenv('OKG_DSN')\n", "extension_ambient_credentials_refused"),
        ("SECRET = 'SECRET_VALUE'\n", "extension_secret_access_refused"),
        ("import sys\n", "extension_private_import_refused"),
        ("import helper_module\n", "extension_import_not_admitted"),
        ("exec('value = 1')\n", "extension_dynamic_execution_refused"),
    ],
)
def test_hostile_private_dsn_credential_secret_and_runtime_imports_refuse_before_execution(
    tmp_path: Path,
    source: str,
    code: str,
) -> None:
    root = _copy_fixture(tmp_path)
    connector = root / "fixture_connector.py"
    admitted_source = connector.read_text()
    connector.write_text(
        source
        + "\n"
        + admitted_source
        + "\nraise AssertionError('extension code executed')\n"
    )

    with pytest.raises(ExtensionBoundaryError) as caught:
        plan_extension_package(root)
    assert caught.value.code == code


def test_undeclared_capability_and_in_process_tier_refuse_before_launch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plan = plan_extension_package(FIXTURE)
    unexpected_launch = Mock(
        side_effect=AssertionError("extension process must not launch")
    )
    monkeypatch.setattr(
        "okg.substrate.extension_broker.subprocess.run", unexpected_launch
    )
    admission, receipt, ledger, lease = _authorization(plan, "fixture.connector")
    with lease, pytest.raises(ExtensionBoundaryError) as caught:
        execute_admitted_extension(
            plan,
            admission,
            receipt,
            ledger,
            extension_id="fixture.connector",
            binding_id="source-alpha",
            capabilities=("provider.admin.write",),
            payload={},
        )
    assert caught.value.code == "extension_capability_undeclared"
    unexpected_launch.assert_not_called()

    root = _copy_fixture(tmp_path)
    descriptor = _descriptor(root)
    descriptor["execution_tier"] = "in-process"
    _write_descriptor(root, descriptor)
    with pytest.raises(ExtensionBoundaryError) as caught:
        plan_extension_package(root)
    assert caught.value.code == "extension_in_process_unapproved"
    unexpected_launch.assert_not_called()


@pytest.mark.parametrize(
    "extension_id,capability,code",
    [
        ("fixture.network-probe", "network", "extension_network_contained"),
        ("fixture.filesystem-probe", "filesystem", "extension_filesystem_contained"),
    ],
)
def test_declared_network_and_filesystem_attempts_are_sandbox_contained(
    extension_id: str,
    capability: str,
    code: str,
) -> None:
    plan = plan_extension_package(FIXTURE)
    admission, receipt, ledger, lease = _authorization(plan, extension_id)
    with lease, pytest.raises(ExtensionBoundaryError) as caught:
        execute_admitted_extension(
            plan,
            admission,
            receipt,
            ledger,
            extension_id=extension_id,
            binding_id="probe",
            capabilities=(capability,),
            payload={},
        )
    assert caught.value.code == code


def test_secret_or_dsn_payload_and_legacy_class_path_metadata_have_no_fallback(
    tmp_path: Path,
) -> None:
    plan = plan_extension_package(FIXTURE)
    for payload in ({"secret_value": "literal"}, {"dsn": "postgres://hidden"}):
        admission, receipt, ledger, lease = _authorization(plan, "fixture.tool")
        with lease, pytest.raises(ExtensionBoundaryError) as caught:
            execute_admitted_extension(
                plan,
                admission,
                receipt,
                ledger,
                extension_id="fixture.tool",
                binding_id="tool",
                capabilities=("provider.metadata.read",),
                payload=payload,
            )
        assert caught.value.code == "extension_secret_payload_refused"

    root = _copy_fixture(tmp_path)
    descriptor = _descriptor(root)
    descriptor["class_path"] = "private.module.Extension"
    assert descriptor["schema_version"] == EXTENSION_PACKAGE_SCHEMA
    _write_descriptor(root, descriptor)
    with pytest.raises(ExtensionBoundaryError) as caught:
        plan_extension_package(root)
    assert caught.value.code == "extension_metadata_fields_invalid"


def test_extension_sdk_version_mismatch_refuses_during_metadata_planning(
    tmp_path: Path,
) -> None:
    root = _copy_fixture(tmp_path)
    descriptor = _descriptor(root)
    descriptor["environment"]["sdk_version"] = "999"
    _write_descriptor(root, descriptor)

    with pytest.raises(ExtensionBoundaryError) as caught:
        plan_extension_package(root)
    assert caught.value.code == "extension_sdk_version_mismatch"


def test_child_output_is_file_backed_and_refused_at_the_broker_limit(
    tmp_path: Path,
) -> None:
    root = _copy_fixture(tmp_path)
    source_path = root / "fixture_connector.py"
    source = source_path.read_text()
    source_path.write_text(
        source.replace(
            "def connector(request):\n",
            "def connector(request):\n    print('x' * 1_000_100)\n",
        )
    )
    plan = plan_extension_package(root)
    admission, receipt, ledger, lease = _authorization(plan, "fixture.connector")

    with lease, pytest.raises(ExtensionBoundaryError) as caught:
        execute_admitted_extension(
            plan,
            admission,
            receipt,
            ledger,
            extension_id="fixture.connector",
            binding_id="source-alpha",
            capabilities=("provider.source.read",),
            payload={"item": {"id": "alpha", "label": "Alpha"}},
        )
    assert caught.value.code == "extension_output_limit_exceeded"
