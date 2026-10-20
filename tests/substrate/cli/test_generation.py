from __future__ import annotations

import json
from typing import Any

import psycopg
import pytest

from okg.substrate.cli.main import main
from okg.substrate.exchange.harness import describe_generation
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher.checkpoint_attestations import (
    CheckpointAttestationResult,
    LoadedCheckpointAttestor,
    payload_digest,
)
from okg.substrate.publisher.publisher import Publisher
from okg.substrate.publisher.repository_provenance import RepositoryProvenance
from tests.pytest.catalog_fixture_builders import seed_catalog
from tests.pytest.catalog_fixture_builders import bind_test_catalog_writer  # noqa: E402


class _DescribeAttestor:
    attestor_id = "tests.describe"
    config_schema = {
        "type": "object",
        "properties": {},
        "required": [],
        "additionalProperties": False,
    }

    def attest(self, context: Any) -> CheckpointAttestationResult:
        payload = {"generation": context.native_generation_id, "receipt": "exact"}
        return CheckpointAttestationResult(
            attestor_id=self.attestor_id,
            schema="tests.describe.v1",
            status="valid",
            payload=payload,
            payload_digest=payload_digest(payload),
        )


@pytest.mark.postgres
def test_generation_descriptions_return_exact_stored_attestation(
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    seed_catalog(
        psycopg.connect(clean_runtime, autocommit=True),
        subtypes=["page"],
        narrowings=[],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ChronosGraphStore.from_connection(conn).apply_facts(
            [NodeFact("describe:one", "page", {}, {"id": "one"}, {})],
            source="describe-test",
            strict_endpoints=True,
        )
    attestor = _DescribeAttestor()
    published = Publisher(
        clean_runtime,
        "default",
        checkpoint_attestors=[LoadedCheckpointAttestor(
            manifest_id=attestor.attestor_id,
            class_name=f"{type(attestor).__module__}.{type(attestor).__qualname__}",
            attestor=attestor,
        )],
        catalog_identity=bind_test_catalog_writer(clean_runtime, "default"),
        repository_provenance=RepositoryProvenance.exact(
            core_revision="1" * 40,
            deployment_revision="2" * 40,
            deployment_tree_sha="3" * 40,
        ),
    ).cycle()
    expected = published["deployment_attestations"]

    rc = main([
        "generation",
        "describe",
        published["generation_id"],
        "--dsn",
        clean_runtime,
        "--json",
    ])
    cli_payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    harness_payload = describe_generation(clean_runtime, published["generation_id"])

    assert rc == 0
    assert cli_payload["deployment_attestations"] == expected
    assert harness_payload["deployment_attestations"] == expected
    assert cli_payload["repository_provenance"] == {
        "status": "exact",
        "core_revision": "1" * 40,
        "deployment_revision": "2" * 40,
        "deployment_tree_sha": "3" * 40,
    }
    assert cli_payload["core_revision"] == "1" * 40
    assert cli_payload["deployment_revision"] == "2" * 40
    assert cli_payload["deployment_tree_sha"] == "3" * 40


@pytest.mark.postgres
def test_generation_describe_marks_legacy_repository_provenance_unavailable(
    clean_runtime: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    catalog_version = seed_catalog(
        psycopg.connect(clean_runtime, autocommit=True),
        subtypes=["page"],
        narrowings=[],
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        store = ChronosGraphStore.from_connection(conn)
        store.apply_facts(
            [NodeFact("legacy:one", "page", {}, {"id": "one"}, {})],
            source="legacy-describe-test",
            strict_endpoints=True,
        )
        generation_id = store.checkpoint_generation(
            None,
            deployment_name="default",
            catalog_version_id=catalog_version,
            status="published",
            metadata={"publisher": "legacy"},
        )

    rc = main([
        "generation", "describe", generation_id, "--dsn", clean_runtime, "--json",
    ])
    payload = json.loads(capsys.readouterr().out.strip().splitlines()[-1])

    assert rc == 0
    assert payload["repository_provenance"] == {
        "status": "unavailable",
        "core_revision": None,
        "deployment_revision": None,
        "deployment_tree_sha": None,
    }
