"""Failure injection for issue #979 runtime-authority remediation."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast
from uuid import uuid4

import psycopg
import pytest

import okg.substrate.catalog.modules_apply as modules_apply
import okg.substrate.catalog.ownership as ownership
from okg.substrate.catalog.modules_compose import ComposedCatalog
from okg.substrate.errors import (
    CatalogMigrationRefused,
    GenerationPinInvalid,
    SchemaValidationFailed,
)
from okg.substrate.graph_store import ChronosGraphStore
from okg.substrate.generation_resolver import resolve_okg_generation
from okg.substrate.mcp.framework.errors import PreJoinSchemaError
from okg.substrate.mcp.framework.resolvers import (
    ResolverContext,
    identifier_pattern_resolver,
)


def _composed() -> ComposedCatalog:
    return ComposedCatalog(
        deployment_name="issue-979-runtime-authority",
        modules=["test"],
        subtypes={"concept"},
        subtype_origin={"concept": "test"},
    )


@pytest.mark.postgres
@pytest.mark.parametrize("catalog_authority", [None, "not-an-id", 0])
def test_generation_refuses_invalid_catalog_authority_without_global_substitution(
    clean_runtime: str,
    catalog_authority: object,
) -> None:
    deployment_name = f"issue-979-generation-{uuid4().hex[:8]}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.executemany(
                "INSERT INTO okg.catalog_versions "
                "(status, applied_at, deployment_name) "
                "VALUES ('applied', now(), %s)",
                [("foreign-a",), ("foreign-b",)],
            )
        store = ChronosGraphStore.from_connection(conn)
        checkpoint_id = store.checkpoint_generation(
            None,
            deployment_name=deployment_name,
            catalog_version_id=None,
            status="published",
            metadata={"catalog_version_id": catalog_authority},
        )

        with pytest.raises(GenerationPinInvalid) as exc_info:
            resolve_okg_generation(conn, deployment_name=deployment_name)

    assert exc_info.value.code == "generation_pin_invalid"
    assert exc_info.value.data == {
        "generation_id": checkpoint_id,
        "deployment_name": deployment_name,
        "branch": "default",
        "catalog_version_id": catalog_authority,
    }


def test_catalog_refuses_invalid_identifier_regex_before_db_mutation() -> None:
    composed = _composed()
    composed.identifier_patterns = [
        {
            "id_type": "broken",
            "regex": "[",
            "validator_kind": None,
            "description": "invalid on purpose",
        }
    ]

    class BombConnection:
        def transaction(self) -> Any:
            raise AssertionError("database transaction boundary reached")

    with pytest.raises(SchemaValidationFailed, match="invalid catalog identifier regex"):
        modules_apply.apply_composed_catalog(
            cast(psycopg.Connection, BombConnection()),
            composed,
        )


def test_identifier_resolver_surfaces_corrupt_active_pattern() -> None:
    class Cursor:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def __enter__(self) -> "Cursor":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, query: str, _params: object) -> None:
            self.queries.append(query)

        def fetchall(self) -> list[tuple[str]]:
            return [("[",)]

    cursor = Cursor()

    class Connection:
        def cursor(self) -> Cursor:
            return cursor

    ctx = ResolverContext(
        conn=cast(psycopg.Connection, Connection()),
        session=cast(Any, None),
        catalog_version_id=77,
    )
    with pytest.raises(PreJoinSchemaError) as exc_info:
        identifier_pattern_resolver("ABC-123", ctx)

    assert exc_info.value.code == "pre_join_tool_schema_error"
    assert exc_info.value.data["catalog_version_id"] == 77
    assert exc_info.value.data["regex"] == "["
    assert len(cursor.queries) == 1, "corrupt authority must not continue to alias lookup"


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("stage", "expected_stage"),
    [
        ("concept", "concept_seed"),
        ("restamp", "searchable_text_restamp"),
        ("alias", "alias_seeding"),
        ("ownership", "ownership_receipt"),
    ],
)
def test_catalog_stays_non_active_until_all_side_effects_finish(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
    expected_stage: str,
) -> None:
    composed = _composed()
    observed_statuses: list[str] = []

    def fail_while_pending(
        conn: psycopg.Connection,
        *args: object,
        **kwargs: object,
    ) -> None:
        cv = int(kwargs.get("catalog_version_id", args[0] if args else 0))
        status = conn.execute(
            "SELECT status FROM okg.catalog_versions "
            "WHERE catalog_version_id = %s",
            (cv,),
        ).fetchone()[0]
        observed_statuses.append(str(status))
        raise RuntimeError(f"injected {stage} failure")

    identity: object | None = None
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        with monkeypatch.context() as patcher:
            if stage == "concept":
                import okg.substrate.catalog.concept_seed as concept_seed

                composed.concept_seeds = [
                    {
                        "node_id": "concept:issue-979-runtime-authority",
                        "name": "Issue 979 runtime authority",
                        "description": "Retryable concept seed for failure injection.",
                    }
                ]
                patcher.setattr(concept_seed, "upsert_concept_nodes", fail_while_pending)
            elif stage == "restamp":
                patcher.setattr(
                    modules_apply,
                    "_restamp_changed_subtypes",
                    fail_while_pending,
                )
            elif stage == "alias":
                patcher.setattr(
                    modules_apply,
                    "_seed_alias_fields_for_catalog",
                    fail_while_pending,
                )
            else:
                identity = SimpleNamespace(
                    deployment_name=composed.deployment_name,
                    catalog_fingerprint="sha256:catalog",
                    schema_fingerprint="sha256:schema",
                    writer_fingerprint="sha256:writer",
                    provenance={"command": "issue-979-test"},
                )
                patcher.setattr(ownership, "mark_catalog_version_seen", fail_while_pending)

            with pytest.raises(CatalogMigrationRefused) as exc_info:
                modules_apply.apply_composed_catalog(
                    conn,
                    composed,
                    identity=identity,
                )

        failed_cv, failed_status = conn.execute(
            "SELECT catalog_version_id, status FROM okg.catalog_versions "
            "ORDER BY catalog_version_id DESC LIMIT 1"
        ).fetchone()
        assert failed_status == "rolled_back"
        assert observed_statuses == ["pending"]
        assert exc_info.value.code == "catalog_migration_refused"
        assert exc_info.value.data["catalog_version_id"] == failed_cv
        assert exc_info.value.data["stage"] == expected_stage
        assert exc_info.value.data["status"] == "rolled_back"

        retry_cv = modules_apply.apply_composed_catalog(
            conn,
            composed,
            identity=identity,
        )
        retry_status = conn.execute(
            "SELECT status FROM okg.catalog_versions WHERE catalog_version_id = %s",
            (retry_cv,),
        ).fetchone()[0]
        assert retry_status == "applied"


@pytest.mark.postgres
@pytest.mark.parametrize(
    ("failure_after", "expected_stage"),
    [
        ("restamp", "searchable_text_restamp"),
        ("alias", "alias_seeding"),
        ("ownership", "ownership_receipt"),
        ("activation", "activation"),
    ],
)
def test_late_catalog_failure_rolls_back_completed_side_effects(
    clean_runtime: str,
    monkeypatch: pytest.MonkeyPatch,
    failure_after: str,
    expected_stage: str,
) -> None:
    composed = _composed()
    composed.deployment_name = f"issue-979-late-{uuid4().hex}"
    composed.subtype_metadata["concept"] = {
        "alias_fields": ["name"],
        "searchable_text_fields": ["description"],
    }
    concept_id = f"concept:issue-979-late-{uuid4().hex}"
    composed.concept_seeds = [
        {
            "node_id": concept_id,
            "name": "Issue 979 late failure",
            "description": "Must disappear when a later catalog stage fails.",
        }
    ]
    composed.aliases = [
        {
            "alias": f"issue-979-direct-{uuid4().hex}",
            "canonical": concept_id,
            "scope": None,
        }
    ]
    concept_was_written_inside_transaction = False
    alias_was_written_inside_transaction = False
    ownership_was_written_inside_transaction = False
    identity = ownership.CatalogIdentity(
        deployment_name=composed.deployment_name,
        catalog_fingerprint="sha256:catalog",
        schema_fingerprint="sha256:schema",
        writer_fingerprint="sha256:writer",
        deployment_root_hash=None,
        worktree_root_hash=None,
        provenance={"command": "issue-979-late-failure-test"},
    )

    def observe_concept(
        conn: psycopg.Connection,
        catalog_version_id: int,
    ) -> None:
        nonlocal concept_was_written_inside_transaction
        graph = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
            autocommit=False,
            catalog_version_id=catalog_version_id,
        ).checkout()
        rows = graph.query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": concept_id},
        )
        concept_was_written_inside_transaction = bool(rows)

    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        ownership.ensure_catalog_ownership_schema(conn)
        conn.execute(
            "INSERT INTO okg.deployment_write_ownership "
            "(deployment_name, owner_fingerprint, schema_fingerprint, "
            "catalog_fingerprint, claimed_by_command) "
            "VALUES (%s, %s, %s, %s, %s)",
            (
                identity.deployment_name,
                identity.writer_fingerprint,
                identity.schema_fingerprint,
                identity.catalog_fingerprint,
                identity.provenance["command"],
            ),
        )

        if failure_after == "restamp":
            original = modules_apply._restamp_changed_subtypes

            def fail_after_restamp(
                inner_conn: psycopg.Connection,
                changed_subtypes: list[str],
                **kwargs: object,
            ) -> None:
                original(inner_conn, changed_subtypes, **kwargs)
                observe_concept(
                    inner_conn,
                    int(kwargs["catalog_version_id"]),
                )
                raise RuntimeError("injected failure after restamp")

            monkeypatch.setattr(
                modules_apply,
                "_restamp_changed_subtypes",
                fail_after_restamp,
            )
        elif failure_after == "alias":
            original_alias = modules_apply._seed_alias_fields_for_catalog

            def fail_after_alias(
                inner_conn: psycopg.Connection,
                catalog_version_id: int,
            ) -> None:
                nonlocal alias_was_written_inside_transaction
                original_alias(inner_conn, catalog_version_id)
                observe_concept(inner_conn, catalog_version_id)
                alias_was_written_inside_transaction = bool(
                    inner_conn.execute(
                        "SELECT 1 FROM okg.aliases "
                        "WHERE catalog_version_id = %s",
                        (catalog_version_id,),
                    ).fetchone()
                )
                raise RuntimeError("injected failure after alias seeding")

            monkeypatch.setattr(
                modules_apply,
                "_seed_alias_fields_for_catalog",
                fail_after_alias,
            )
        else:
            original_mark = ownership.mark_catalog_version_seen

            def fail_after_ownership(
                inner_conn: psycopg.Connection,
                *,
                identity: ownership.CatalogIdentity,
                catalog_version_id: int,
            ) -> None:
                nonlocal alias_was_written_inside_transaction
                nonlocal ownership_was_written_inside_transaction
                original_mark(
                    inner_conn,
                    identity=identity,
                    catalog_version_id=catalog_version_id,
                )
                observe_concept(inner_conn, catalog_version_id)
                alias_was_written_inside_transaction = bool(
                    inner_conn.execute(
                        "SELECT 1 FROM okg.aliases "
                        "WHERE catalog_version_id = %s",
                        (catalog_version_id,),
                    ).fetchone()
                )
                ownership_was_written_inside_transaction = (
                    inner_conn.execute(
                        "SELECT last_seen_catalog_version_id "
                        "FROM okg.deployment_write_ownership "
                        "WHERE deployment_name = %s",
                        (identity.deployment_name,),
                    ).fetchone()[0]
                    == catalog_version_id
                )
                if failure_after == "ownership":
                    raise RuntimeError("injected failure after ownership")
                inner_conn.execute(
                    "UPDATE okg.catalog_versions SET status = 'applied' "
                    "WHERE catalog_version_id = %s",
                    (catalog_version_id,),
                )

            monkeypatch.setattr(
                ownership,
                "mark_catalog_version_seen",
                fail_after_ownership,
            )

        with pytest.raises(CatalogMigrationRefused) as exc_info:
            modules_apply.apply_composed_catalog(
                conn,
                composed,
                identity=identity,
            )

        failed_cv = int(exc_info.value.data["catalog_version_id"])
        assert concept_was_written_inside_transaction is True
        if failure_after in {"alias", "ownership", "activation"}:
            assert alias_was_written_inside_transaction is True
        if failure_after in {"ownership", "activation"}:
            assert ownership_was_written_inside_transaction is True
        assert exc_info.value.data["stage"] == expected_stage
        assert conn.execute(
            "SELECT status FROM okg.catalog_versions "
            "WHERE catalog_version_id = %s",
            (failed_cv,),
        ).fetchone()[0] == "rolled_back"

        graph = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        ).checkout()
        assert graph.query(
            "SELECT node_id FROM okg.graph_nodes WHERE node_id = :node_id",
            {"node_id": concept_id},
        ) == []
        assert conn.execute(
            "SELECT count(*) FROM okg.admission_envelopes "
            "WHERE catalog_version_id = %s",
            (failed_cv,),
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT count(*) FROM okg.producer_profiles "
            "WHERE catalog_version_id = %s",
            (failed_cv,),
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT count(*) FROM okg.aliases "
            "WHERE catalog_version_id = %s",
            (failed_cv,),
        ).fetchone()[0] == 0
        assert conn.execute(
            "SELECT last_seen_catalog_version_id "
            "FROM okg.deployment_write_ownership "
            "WHERE deployment_name = %s",
            (identity.deployment_name,),
        ).fetchone()[0] is None


@pytest.mark.postgres
def test_ordinary_graph_writes_ignore_pending_and_rolled_back_catalogs(
    clean_runtime: str,
) -> None:
    subtype = f"issue_979_catalog_visibility_{uuid4().hex}"
    with psycopg.connect(clean_runtime, autocommit=True) as conn:
        versions: dict[str, int] = {}
        for status, fields in (
            ("applied", '["name"]'),
            ("pending", '["description"]'),
            ("rolled_back", '["title"]'),
        ):
            versions[status] = int(
                conn.execute(
                    "INSERT INTO okg.catalog_versions (status, applied_at) "
                    "VALUES (%s, CASE WHEN %s = 'applied' THEN now() END) "
                    "RETURNING catalog_version_id",
                    (status, status),
                ).fetchone()[0]
            )
            conn.execute(
                "INSERT INTO okg.subtypes "
                "(name, metadata, catalog_version_id) "
                "VALUES (%s, jsonb_build_object("
                "'searchable_text_fields', %s::jsonb), %s)",
                (subtype, fields, versions[status]),
            )

        ordinary = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
        )
        assert ordinary._searchable_text_fields(subtype) == ["name"]

        pending_scoped = ChronosGraphStore.from_connection(
            conn,
            ensure_schema=False,
            catalog_version_id=versions["pending"],
        )
        assert pending_scoped._searchable_text_fields(subtype) == ["description"]
