from __future__ import annotations

from dataclasses import replace
import json
import shlex
import subprocess
from pathlib import Path

import psycopg
import pytest

from okg.substrate.catalog.modules_apply import apply_composed_catalog
from okg.substrate.catalog.modules_compose import (
    ComposedCatalog,
    NarrowingRow,
    compose_catalog,
)
from okg.substrate.catalog.ownership import (
    catalog_identity_for,
    claim_catalog_ownership,
    diagnose_catalog_ownership,
    environment_bound_catalog_fingerprint_for,
    ensure_catalog_write_allowed,
    ensure_catalog_ownership_schema,
    legacy_catalog_fingerprint_for,
)
from okg.substrate.cli.catalog import _catalog_claim_payload
from okg.substrate.cli.main import main
from okg.substrate.errors import CatalogOwnershipMismatch, SubstrateError
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.library.sources.base import NodeFact
from okg.substrate.publisher import Publisher
from tests.pytest.catalog_fixture_builders import seed_catalog


pytestmark = pytest.mark.postgres


def _deployment_dir(tmp_path: Path, name: str) -> Path:
    d = tmp_path / name
    d.mkdir(exist_ok=True)
    (d / "deployment.yaml").write_text(f"name: {name}\nmodules: []\n")
    return d


def _composed(name: str, *, extra: bool = False) -> ComposedCatalog:
    narrowings = [
        NarrowingRow(
            parent_subtype="source_file",
            edge_archetype="references",
            child_subtype="git_commit",
            origin="test",
            narrowing_name="file_references_commit",
        )
    ]
    if extra:
        narrowings.append(
            NarrowingRow(
                parent_subtype="source_file",
                edge_archetype="mentions",
                child_subtype="git_commit",
                origin="test",
                narrowing_name="file_mentions_commit",
            )
        )
    return ComposedCatalog(
        deployment_name=name,
        modules=[],
        subtypes={"source_file", "git_commit"},
        subtype_origin={
            "source_file": f"deployment:{name}",
            "git_commit": "test",
        },
        narrowings=narrowings,
    )


def _identity(tmp_path: Path, name: str, *, extra: bool = False):
    return catalog_identity_for(
        composed=_composed(name, extra=extra),
        deployment_dir=_deployment_dir(tmp_path, name),
        command="test",
    )


def test_catalog_content_fingerprint_is_environment_agnostic(
    tmp_path: Path,
) -> None:
    staging = _identity(tmp_path, "fixture-staging")
    production = _identity(tmp_path, "fixture-production")

    assert staging.catalog_fingerprint == production.catalog_fingerprint
    assert staging.schema_fingerprint != production.schema_fingerprint
    assert staging.writer_fingerprint != production.writer_fingerprint
    assert legacy_catalog_fingerprint_for(_composed("fixture-staging")) != (
        legacy_catalog_fingerprint_for(_composed("fixture-production"))
    )
    assert environment_bound_catalog_fingerprint_for(
        _composed("fixture-staging")
    ) != environment_bound_catalog_fingerprint_for(
        _composed("fixture-production")
    )


def _reset_catalog(conn: psycopg.Connection) -> None:
    ensure_catalog_ownership_schema(conn)
    with conn.cursor() as cur:
        cur.execute("TRUNCATE okg.deployment_write_ownership")
        cur.execute("TRUNCATE okg.catalog_versions CASCADE")
        cur.execute(
            """
            INSERT INTO okg.catalog_versions
              (catalog_version_id, status, applied_at,
               diff_summary, reconcile_plan)
            VALUES
              (1, 'applied', now(),
               '{"reason":"test seed"}'::jsonb,
               '{"reconcile":[]}'::jsonb)
            """
        )
        cur.execute(
            """
            SELECT setval(
              pg_get_serial_sequence(
                'okg.catalog_versions', 'catalog_version_id'
              ),
              1
            )
            """
        )


def _insert_legacy_catalog(
    conn: psycopg.Connection,
    triples: list[tuple[str, str, str]],
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.catalog_versions (status, applied_at)
            VALUES ('applied', now())
            RETURNING catalog_version_id
            """
        )
        cv = int(cur.fetchone()[0])
        for parent, edge, child in triples:
            cur.execute(
                """
                INSERT INTO okg.narrowings
                  (parent_subtype, edge_type, child_subtype,
                   catalog_version_id)
                VALUES (%s, %s, %s, %s)
                """,
                (parent, edge, child, cv),
            )
        return cv


def test_diagnostic_reports_mixed_catalog_fingerprints(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        _insert_legacy_catalog(
            conn,
            [("view", "view_cites_node", "pact_change")],
        )
        _insert_legacy_catalog(
            conn,
            [("source_file", "references", "git_commit")],
        )

        report = diagnose_catalog_ownership(
            conn,
            identity=_identity(tmp_path, "catalog-owner-test"),
        )

    assert report["status"] == "unclaimed_mixed_history"
    assert report["mixed_recent_shapes"] is True
    assert report["owner"] is None
    assert "claim --deployment" in " ".join(report["recommended_commands"])


def test_recommended_isolated_dsn_uses_live_connection_port(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # #825: diagnose's isolated-DSN recommendation must target the
    # Postgres this connection actually uses - the hardcoded default
    # 5433 pointed at the wrong instance on hosts where 5433 is a
    # scratch server. The test rig itself commonly runs on 5433, where
    # "equals the live port" cannot distinguish derived from hardcoded
    # - so first pin the diagnose -> _live_host_port -> recommendation
    # wiring with a port no rig uses, then assert the unpatched path
    # reports the real connection port.
    import okg.substrate.catalog.ownership as ownership_mod

    def _dsn_commands(report: dict) -> list[str]:
        return [
            c for c in report["recommended_commands"] if "--dsn" in c
        ]

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        identity = _identity(tmp_path, "catalog-dsn-port-test")
        monkeypatch.setattr(
            ownership_mod,
            "_live_host_port",
            lambda info: ("127.0.0.1", 54329),
        )
        wired = diagnose_catalog_ownership(conn, identity=identity)
        monkeypatch.undo()
        report = diagnose_catalog_ownership(conn, identity=identity)
        live_port = int(conn.info.port)

    assert _dsn_commands(wired)
    assert all(":54329/" in c for c in _dsn_commands(wired))
    assert _dsn_commands(report)
    assert all(f":{live_port}/" in c for c in _dsn_commands(report))


def test_catalog_load_records_writer_provenance(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    identity = _identity(tmp_path, "catalog-provenance-test")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        ensure_catalog_write_allowed(conn, identity=identity)
        cv = apply_composed_catalog(
            conn,
            _composed("catalog-provenance-test"),
            identity=identity,
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT deployment_name, catalog_fingerprint,
                       schema_fingerprint, writer_fingerprint,
                       writer_provenance->>'command'
                  FROM okg.catalog_versions
                 WHERE catalog_version_id = %s
                """,
                (cv,),
            )
            row = cur.fetchone()

    assert row == (
        "catalog-provenance-test",
        identity.catalog_fingerprint,
        identity.schema_fingerprint,
        identity.writer_fingerprint,
        "test",
    )


def test_incompatible_writer_is_rejected_before_catalog_mutation(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    identity = _identity(tmp_path, "catalog-guard-test")
    # #2229: a different catalog from the SAME checkout is the same writer
    # now; a genuinely incompatible writer is a different checkout.
    other_checkout = tmp_path / "other-checkout"
    other_checkout.mkdir()
    other = catalog_identity_for(
        composed=_composed("catalog-guard-test", extra=True),
        deployment_dir=_deployment_dir(other_checkout, "catalog-guard-test"),
        command="test",
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        ensure_catalog_write_allowed(conn, identity=identity)
        before = _catalog_version_count(conn)

        with pytest.raises(CatalogOwnershipMismatch) as refused:
            ensure_catalog_write_allowed(conn, identity=other)
        assert refused.value.code == "catalog_ownership_foreign_writer"

        after = _catalog_version_count(conn)

    assert after == before


def test_repair_claims_owner_without_destructive_reset(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    identity = _identity(tmp_path, "catalog-repair-test")
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        _insert_legacy_catalog(
            conn,
            [("view", "view_cites_node", "pact_change")],
        )
        _insert_legacy_catalog(
            conn,
            [("source_file", "references", "git_commit")],
        )

        report = claim_catalog_ownership(
            conn,
            identity=identity,
            force=True,
        )
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM okg.catalog_versions")
            n_versions = int(cur.fetchone()[0])

    assert report["owner"]["owner_fingerprint"] == identity.writer_fingerprint
    assert n_versions == 3


def test_claim_cli_reports_catalog_apply_until_latest_is_compatible(
    clean_runtime: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = "catalog-claim-receipt-test"
    deployment_dir = _deployment_dir(tmp_path, name)
    (deployment_dir / "deployment.yaml").write_text(
        "\n".join([
            f"name: {name}",
            "modules: []",
            "postgres:",
            f"  dsn: {clean_runtime}",
            "",
        ])
    )
    old_catalog = _composed(name, extra=True)
    old_identity = catalog_identity_for(
        composed=old_catalog,
        deployment_dir=deployment_dir,
        command="old checkout catalog load",
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        ensure_catalog_write_allowed(conn, identity=old_identity)
        old_version = apply_composed_catalog(
            conn,
            old_catalog,
            identity=old_identity,
        )

    monkeypatch.delenv("OKG_AGENT", raising=False)
    claim_args = [
        "catalog", "ownership", "claim",
        "--deployment", str(deployment_dir), "--force",
    ]
    assert main(["--json", *claim_args]) == 0
    first_claim = json.loads(capsys.readouterr().out)

    assert first_claim["claimed"] is True
    assert first_claim["latest_compatible"] is False
    assert first_claim["resolved"] is False
    expected_command = (
        "okg catalog load --deployment "
        f"{shlex.quote(str(deployment_dir))} "
        "--writer-scope deployment_root --apply --json"
    )
    assert first_claim["next_command"] == expected_command

    assert main(claim_args) == 0
    text_lines = capsys.readouterr().out.splitlines()
    assert text_lines[:4] == [
        "claimed: True",
        "latest_compatible: False",
        "resolved: False",
        f"next_command: {expected_command}",
    ]
    history_index = text_lines.index("recent_catalog_versions:")
    assert history_index > 3

    applied = subprocess.run(
        shlex.split(first_claim["next_command"]),
        check=False,
        capture_output=True,
        text=True,
    )
    assert applied.returncode == 0, applied.stderr or applied.stdout

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        current_identity = catalog_identity_for(
            composed=compose_catalog(deployment_dir),
            deployment_dir=deployment_dir,
            command="current checkout verification",
        )
        repaired = diagnose_catalog_ownership(
            conn,
            identity=current_identity,
        )
        latest_version = int(
            repaired["recent_catalog_versions"][0]["catalog_version_id"]
        )

    assert latest_version > old_version
    assert repaired["latest_compatible"] is True
    assert main(["--json", *claim_args]) == 0
    final_claim = json.loads(capsys.readouterr().out)
    assert final_claim["claimed"] is True
    assert final_claim["latest_compatible"] is True
    assert final_claim["resolved"] is True
    assert final_claim["next_command"] is None

    assert main(claim_args) == 0
    final_text_lines = capsys.readouterr().out.splitlines()
    assert final_text_lines[:4] == [
        "claimed: True",
        "latest_compatible: True",
        "resolved: True",
        "next_command: None",
    ]
    assert final_text_lines.index("recent_catalog_versions:") > 3


@pytest.mark.parametrize("invalid", [None, 0, 1, "false", {}, []])
def test_claim_receipt_refuses_invalid_compatibility_verdict(invalid) -> None:
    with pytest.raises(
        SubstrateError,
        match="invalid compatibility verdict",
    ):
        _catalog_claim_payload(
            {"latest_compatible": invalid},
            deployment="deployments/example",
            writer_scope="deployment_root",
            environment_id=None,
        )


def _catalog_version_count(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM okg.catalog_versions")
        return int(cur.fetchone()[0])


def test_claim_cli_refuses_to_silently_re_scope_the_writer(
    clean_runtime: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The #1303 incident, end to end through the CLI.

    A sealed-release worker owns the database under `writer_scope:
    environment`. Re-claiming from an operator shell WITHOUT
    `OKG_ENVIRONMENT_ID` used to record a `deployment_root`-scoped owner,
    report `claimed: true`, and leave every later publish failing
    `catalog_ownership_foreign_writer`. It must now refuse, and explicit
    environment writer authority must reproduce the worker's fingerprint.
    """
    environment_id = "fixture-writer-scope-production"
    name = "catalog-writer-scope-test"
    deployment_dir = _deployment_dir(tmp_path, name)
    (deployment_dir / "deployment.yaml").write_text(
        "\n".join([
            f"name: {name}",
            "modules: []",
            "postgres:",
            f"  dsn: {clean_runtime}",
            "",
        ])
    )

    monkeypatch.setenv("OKG_ENVIRONMENT_ID", "wrong-ambient-environment")
    catalog = _composed(name)
    worker_identity = catalog_identity_for(
        composed=catalog,
        deployment_dir=deployment_dir,
        command="sealed release worker",
        writer_scope="environment",
        environment_id=environment_id,
    )
    assert worker_identity.provenance["writer_scope"] == "environment"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        ensure_catalog_write_allowed(conn, identity=worker_identity)
        apply_composed_catalog(conn, catalog, identity=worker_identity)

    monkeypatch.delenv("OKG_ENVIRONMENT_ID")
    monkeypatch.delenv("OKG_AGENT", raising=False)
    claim_args = [
        "catalog", "ownership", "claim",
        "--deployment", str(deployment_dir), "--force",
    ]

    assert main(["--json", *claim_args]) != 0
    # Refusals render on stderr; a claim that printed nothing to stdout is
    # exactly the point — no receipt, no ownership row.
    captured = capsys.readouterr()
    assert captured.out == ""
    refusal = json.loads(captured.err)["error"]
    assert refusal["data"]["reason_code"] == "catalog_writer_scope_change"
    assert refusal["data"]["observed"]["recorded"]["writer_scope"] == (
        "environment"
    )
    assert environment_id in refusal["fix_hint"]

    # The ownership row is untouched by a refused claim.
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        unchanged = diagnose_catalog_ownership(conn, identity=worker_identity)
    assert unchanged["owner"]["owner_fingerprint"] == (
        worker_identity.writer_fingerprint
    )

    assert main([
        "--json",
        *claim_args,
        "--writer-scope", "environment",
        "--environment-id", environment_id,
    ]) == 0
    recovered = json.loads(capsys.readouterr().out)
    assert recovered["claimed"] is True
    assert recovered["owner"]["metadata"]["writer_scope"] == "environment"
    assert recovered["owner"]["metadata"]["environment_id"] == environment_id
    assert recovered["owner"]["metadata"]["writer_scope_source"] == "explicit"
    # The explicit scope reproduces the environment-scoped writer without
    # relying on ambient state. (The fingerprint differs from
    # `worker_identity` only because the CLI recomposes the catalog from
    # deployment.yaml rather than the hand-built fixture above.)
    stated_identity = catalog_identity_for(
        composed=compose_catalog(deployment_dir),
        deployment_dir=deployment_dir,
        command="verification",
        writer_scope="environment",
        environment_id=environment_id,
    )
    assert recovered["owner"]["owner_fingerprint"] == (
        stated_identity.writer_fingerprint
    )


def test_explicit_writer_scope_publish_lifecycle(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An explicit environment writer reaches a published generation."""
    environment_id = "catalog-publish-production"
    name = "catalog-explicit-publish"
    deployment_dir = _deployment_dir(tmp_path, name)
    catalog = _composed(name)
    monkeypatch.setenv("OKG_ENVIRONMENT_ID", "wrong-ambient-environment")
    identity = catalog_identity_for(
        composed=catalog,
        deployment_dir=deployment_dir,
        command="explicit publish proof",
        writer_scope="environment",
        environment_id=environment_id,
    )

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        ensure_catalog_write_allowed(conn, identity=identity)
        apply_composed_catalog(conn, catalog, identity=identity)

    published = Publisher(
        clean_runtime,
        name,
        catalog_identity=identity,
        sync_chronos_graph=False,
    ).cycle()

    assert published["status"] == "published", published
    assert published["generation_id"]
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        checkpoint = store.get_generation(published["generation_id"])
    finally:
        store.close()
    assert checkpoint.checkpoint_id == published["generation_id"]
    assert checkpoint.metadata["status"] == "published"
    assert checkpoint.metadata["deployment_name"] == name

    def _published_ids() -> list[str]:
        store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
        try:
            return [
                str(item.checkpoint_id)
                for item in store.list_generations(
                    deployment_name=name,
                    status="published",
                )
            ]
        finally:
            store.close()

    baseline_ids = _published_ids()
    assert published["generation_id"] in baseline_ids

    with pytest.raises(SubstrateError) as missing:
        catalog_identity_for(
            composed=catalog,
            deployment_dir=deployment_dir,
            command="missing environment authority",
            writer_scope="environment",
        )
    assert missing.value.code == "catalog_environment_identity_required"
    assert _published_ids() == baseline_ids

    with pytest.raises(SubstrateError) as ambiguous:
        catalog_identity_for(
            composed=catalog,
            deployment_dir=deployment_dir,
            command="ambiguous publish authority",
            writer_scope="deployment_root",
            environment_id=environment_id,
        )
    assert ambiguous.value.code == "catalog_writer_scope_ambiguous"
    assert _published_ids() == baseline_ids

    stale_identity = catalog_identity_for(
        composed=catalog,
        deployment_dir=deployment_dir,
        command="stale publish authority",
        writer_scope="environment",
        environment_id="catalog-publish-staging",
    )
    with pytest.raises(CatalogOwnershipMismatch) as stale:
        Publisher(
            clean_runtime,
            name,
            catalog_identity=stale_identity,
            sync_chronos_graph=False,
        )
    assert stale.value.code == "catalog_writer_scope_change"
    assert _published_ids() == baseline_ids


def test_canonical_factory_publishes_with_verified_environment_authority(
    clean_runtime: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The production factory reproduces the sealed writer's authority."""
    from okg.substrate.publisher.factory import build_deployment_publisher

    environment_id = "catalog-factory-production"
    name = "catalog-factory-environment-publish"
    deployment_dir = _deployment_dir(tmp_path, name)
    catalog = compose_catalog(deployment_dir)
    claimed_identity = catalog_identity_for(
        composed=catalog,
        deployment_dir=deployment_dir,
        command="sealed catalog claim",
        writer_scope="environment",
        environment_id=environment_id,
    )
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        ensure_catalog_write_allowed(conn, identity=claimed_identity)
        apply_composed_catalog(conn, catalog, identity=claimed_identity)

    authority_calls: list[dict[str, str]] = []
    # Full lowercase Git object ids, as a sealed release's inputs record them.
    # The production factory turns these into the generation's exact
    # repository provenance, so the stand-in must carry them (issue #2031).
    code_revision = "3f9c2a7e5b1d4c8a9e0f6b2d7c1a5e8f4b3d9c2a"
    deployment_revision = "8a1e4d7c2b9f5e3a6d0c8b4f1e7a2d9c5b3f6e1a"
    deployment_tree_sha = "c5d2f8b1a4e7c3d9f6a0b2e5d8c1f4a7b3e9d6c2"

    def _verified_authority(**kwargs):
        authority_calls.append(kwargs)
        # The keys of verify_worker_release_authority's sealed-release result
        # that the publisher build reads: the catalog identity reads
        # definition_root and environment_id; the factory reads the three
        # repository ids.
        return {
            "mode": "sealed_release",
            "environment_id": environment_id,
            "deployment": name,
            "code_revision": code_revision,
            "deployment_revision": deployment_revision,
            "deployment_tree_sha": deployment_tree_sha,
            "definition_root": str(deployment_dir),
        }

    monkeypatch.setattr(
        "okg.substrate.runtime.release_authority.verify_worker_release_authority",
        _verified_authority,
    )
    publisher = build_deployment_publisher(
        dsn=clean_runtime,
        deployment_name=name,
        deployment_dir=deployment_dir,
        command="sealed production publisher",
        publisher_overrides={"sync_chronos_graph": False},
    )

    # Two reads of the same authority since 279f27b6db: the catalog identity
    # verifies it, then the factory verifies it again for repository
    # provenance. Both must name this deployment and DSN.
    assert authority_calls == [{"deployment": name, "dsn": clean_runtime}] * 2
    assert publisher.catalog_identity.writer_fingerprint == (
        claimed_identity.writer_fingerprint
    )
    assert publisher.catalog_identity.provenance["writer_scope"] == "environment"
    assert publisher.catalog_identity.provenance["environment_id"] == environment_id
    published = publisher.cycle()
    assert published["status"] == "published"
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        checkpoint = store.get_generation(published["generation_id"])
    finally:
        store.close()
    assert checkpoint.metadata["status"] == "published"
    assert checkpoint.metadata["catalog_version_id"] == 2
    assert checkpoint.metadata["repository_provenance"] == {
        "status": "exact",
        "core_revision": code_revision,
        "deployment_revision": deployment_revision,
        "deployment_tree_sha": deployment_tree_sha,
    }


def test_publisher_missing_authority_refuses_before_generation(
    clean_runtime: str,
) -> None:
    """A direct publisher cannot recover authority from applied DB state."""
    name = "default"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        seed_catalog(conn, subtypes=["page"])
        ChronosGraphStore.from_connection(conn).apply_facts(
            [
                NodeFact(
                    "source:test-missing-authority",
                    "page",
                    {},
                    {"id": "source:test-missing-authority"},
                    {"revision": 1},
                )
            ],
            source="default",
            strict_endpoints=True,
        )

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        before = list(store.list_generations(deployment_name=name))
    finally:
        store.close()

    with pytest.raises(SubstrateError) as exc_info:
        Publisher(
            clean_runtime,
            name,
            sync_chronos_graph=False,
        ).cycle()

    assert exc_info.value.code == "catalog_writer_scope_required"
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        assert list(store.list_generations(deployment_name=name)) == before
    finally:
        store.close()


def test_publisher_rejects_foreign_deployment_identity_before_generation(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """Publisher target and catalog authority must name one deployment."""
    authority_name = "catalog-authority-a"
    publisher_name = "catalog-publisher-b"
    identity = _identity(tmp_path, authority_name)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        ensure_catalog_write_allowed(conn, identity=identity)
        apply_composed_catalog(
            conn,
            _composed(authority_name),
            identity=identity,
        )

    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        before = list(store.list_generations(deployment_name=publisher_name))
    finally:
        store.close()

    with pytest.raises(SubstrateError) as exc_info:
        Publisher(
            clean_runtime,
            publisher_name,
            catalog_identity=identity,
            sync_chronos_graph=False,
        ).cycle()

    assert exc_info.value.code == "catalog_writer_scope_conflict"
    assert exc_info.value.data["reason_code"] == (
        "catalog_writer_scope_conflict"
    )
    assert exc_info.value.data["deployment_name"] == publisher_name
    assert exc_info.value.data["identity_deployment_name"] == authority_name
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        assert list(store.list_generations(deployment_name=publisher_name)) == before
    finally:
        store.close()


def test_publisher_rejects_identity_without_explicit_scope_before_generation(
    clean_runtime: str,
    tmp_path: Path,
) -> None:
    """A matching fingerprint cannot make legacy provenance authoritative."""
    name = "catalog-missing-explicit-scope"
    identity = _identity(tmp_path, name)
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        _reset_catalog(conn)
        ensure_catalog_write_allowed(conn, identity=identity)
        apply_composed_catalog(
            conn,
            _composed(name),
            identity=identity,
        )

    legacy_provenance = dict(identity.provenance)
    legacy_provenance.pop("writer_scope")
    legacy_provenance.pop("writer_scope_source")
    legacy_identity = replace(identity, provenance=legacy_provenance)
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        before = list(store.list_generations(deployment_name=name))
    finally:
        store.close()

    with pytest.raises(SubstrateError) as exc_info:
        Publisher(
            clean_runtime,
            name,
            catalog_identity=legacy_identity,
            sync_chronos_graph=False,
        ).cycle()

    assert exc_info.value.code == "catalog_writer_scope_required"
    assert exc_info.value.data["reason_code"] == (
        "catalog_writer_scope_required"
    )
    assert exc_info.value.data["writer_scope"] is None
    store = ChronosGraphStore.connect(clean_runtime, ensure_schema=False)
    try:
        assert list(store.list_generations(deployment_name=name)) == before
    finally:
        store.close()
