from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from okg.substrate.entity_resolution import normalize_entity_name


_TEST_WRITER_ROOT = Path(__file__).resolve().parents[1]
_CATALOG_DEFINITION_TABLES = (
    "subtypes",
    "subtype_ancestors",
    "narrowings",
    "aliases",
    "identifier_patterns",
    "invariants",
)


@dataclass(frozen=True)
class SubtypeSpec:
    name: str
    parent: str | None = None
    metadata: dict[str, Any] | None = None


def bind_test_catalog_writer(
    dsn: str,
    deployment_name: str,
) -> Any:
    """Bind the latest applied test catalog to an explicit test writer.

    Publisher now refuses missing writer authority. Database-backed tests that
    intentionally exercise a publish use this helper at the call site and pass
    its returned identity to ``Publisher``. The helper is test-only setup: it
    preserves the seeded catalog rows by cloning the latest unbound template
    into a deployment-specific version with deterministic deployment-root
    authority.
    """
    from okg.substrate.catalog.modules_compose import ComposedCatalog
    from okg.substrate.catalog.ownership import (
        catalog_identity_for,
        claim_catalog_ownership,
        diagnose_catalog_ownership,
        ensure_catalog_ownership_schema,
        ensure_catalog_write_allowed,
        mark_catalog_version_seen,
    )

    identity = catalog_identity_for(
        composed=ComposedCatalog(
            deployment_name=deployment_name,
            modules=["pytest-explicit-writer"],
        ),
        deployment_dir=_TEST_WRITER_ROOT,
        command="pytest explicit catalog writer",
        writer_scope="deployment_root",
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        ensure_catalog_ownership_schema(conn)
        report = diagnose_catalog_ownership(conn, identity=identity)
        if report.get("owner") is None:
            # Test databases may contain several unowned catalog fixtures. The
            # force is bounded to an absent deployment-specific owner; an
            # existing foreign owner always goes through the fail-closed check.
            claim_catalog_ownership(conn, identity=identity, force=True)
        else:
            ensure_catalog_write_allowed(conn, identity=identity)

        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                """
                SELECT catalog_version_id
                  FROM okg.catalog_versions
                 WHERE status = 'applied'
                   AND deployment_name IS NULL
                 ORDER BY catalog_version_id DESC
                 LIMIT 1
                """
            )
            row = cur.fetchone()
            if row is None:
                raise AssertionError(
                    "test publish requires an unbound applied catalog template"
                )
            template_catalog_version_id = int(row[0])
            cur.execute(
                """
                SELECT catalog_version_id
                  FROM okg.catalog_versions
                 WHERE status = 'applied'
                   AND deployment_name = %s
                   AND catalog_fingerprint = %s
                   AND writer_fingerprint = %s
                   AND diff_summary->>'pytest_template_catalog_version_id' = %s
                 ORDER BY catalog_version_id DESC
                 LIMIT 1
                """,
                (
                    identity.deployment_name,
                    identity.catalog_fingerprint,
                    identity.writer_fingerprint,
                    str(template_catalog_version_id),
                ),
            )
            existing = cur.fetchone()
            if existing is not None:
                catalog_version_id = int(existing[0])
            else:
                catalog_version_id = _clone_catalog_for_test_writer(
                    cur,
                    template_catalog_version_id=template_catalog_version_id,
                    identity=identity,
                )
        mark_catalog_version_seen(
            conn,
            identity=identity,
            catalog_version_id=catalog_version_id,
        )
    return identity


def _clone_catalog_for_test_writer(
    cur: psycopg.Cursor,
    *,
    template_catalog_version_id: int,
    identity: Any,
) -> int:
    """Clone one immutable catalog template for one explicit test writer."""
    cur.execute(
        """
        INSERT INTO okg.catalog_versions
          (status, applied_at, diff_summary, reconcile_plan,
           deployment_name, catalog_fingerprint, schema_fingerprint,
           writer_fingerprint, writer_provenance, created_by_command)
        SELECT 'applied', now(),
               diff_summary || jsonb_build_object(
                 'pytest_template_catalog_version_id', catalog_version_id
               ),
               reconcile_plan, %s, %s, %s, %s, %s::jsonb, %s
          FROM okg.catalog_versions
         WHERE catalog_version_id = %s
           AND status = 'applied'
           AND deployment_name IS NULL
        RETURNING catalog_version_id
        """,
        (
            identity.deployment_name,
            identity.catalog_fingerprint,
            identity.schema_fingerprint,
            identity.writer_fingerprint,
            Jsonb(identity.provenance),
            identity.provenance["command"],
            template_catalog_version_id,
        ),
    )
    row = cur.fetchone()
    if row is None:
        raise AssertionError("test catalog template changed during writer binding")
    catalog_version_id = int(row[0])

    for table_name in _CATALOG_DEFINITION_TABLES:
        cur.execute(
            """
            SELECT column_name
              FROM information_schema.columns
             WHERE table_schema = 'okg'
               AND table_name = %s
               AND column_name <> 'catalog_version_id'
             ORDER BY ordinal_position
            """,
            (table_name,),
        )
        columns = [str(column[0]) for column in cur.fetchall()]
        if not columns:
            continue
        selected_columns = sql.SQL(", ").join(map(sql.Identifier, columns))
        cur.execute(
            sql.SQL(
                "INSERT INTO okg.{table} ({columns}, catalog_version_id) "
                "SELECT {columns}, %s FROM okg.{table} "
                "WHERE catalog_version_id = %s"
            ).format(
                table=sql.Identifier(table_name),
                columns=selected_columns,
            ),
            (catalog_version_id, template_catalog_version_id),
        )
    return catalog_version_id


def explicit_catalog_publisher(
    dsn: str,
    deployment_name: str,
    **publisher_kwargs: Any,
) -> Any:
    """Construct a test Publisher with explicit catalog writer authority."""
    from okg.substrate.publisher.publisher import Publisher

    if "catalog_identity" not in publisher_kwargs:
        publisher_kwargs["catalog_identity"] = bind_test_catalog_writer(
            dsn,
            deployment_name,
        )
    return Publisher(dsn, deployment_name, **publisher_kwargs)


def apply_explicit_test_catalog(
    dsn: str,
    deployment_dir: Path | str,
) -> Any:
    """Apply one real deployment catalog under matching explicit authority."""
    from okg.substrate.catalog.modules_apply import apply_composed_catalog
    from okg.substrate.catalog.modules_compose import compose_catalog
    from okg.substrate.catalog.ownership import (
        catalog_identity_for,
        claim_catalog_ownership,
        ensure_catalog_write_allowed,
    )

    resolved_dir = Path(deployment_dir).resolve()
    composed = compose_catalog(resolved_dir)
    identity = catalog_identity_for(
        composed=composed,
        deployment_dir=resolved_dir,
        command="pytest explicit deployment catalog apply",
        writer_scope="deployment_root",
    )
    with psycopg.connect(dsn, autocommit=True) as conn:
        claim_catalog_ownership(conn, identity=identity, force=True)
        ensure_catalog_write_allowed(conn, identity=identity)
        apply_composed_catalog(conn, composed, identity=identity)
    return identity


def seed_catalog(
    conn: psycopg.Connection,
    *,
    subtypes: list[SubtypeSpec | str] | None = None,
    narrowings: list[tuple[str, str, str]] | None = None,
    aliases: list[tuple[str, str, str | None]] | None = None,
) -> int:
    """Seed one applied catalog version with subtypes, closure, and narrowings.

    This is deliberately small and SQL-backed: tests still exercise the real
    substrate tables and narrowing trigger, while avoiding copy-pasted partial
    catalog setup that forgets subtype closure or catalog ownership.
    """
    subtype_specs = _normalize_subtypes(subtypes, narrowings)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO okg.catalog_versions (status, applied_at) "
            "VALUES ('applied', now()) RETURNING catalog_version_id"
        )
        catalog_version_id = int(cur.fetchone()[0])
        for spec in subtype_specs.values():
            cur.execute(
                """
                INSERT INTO okg.subtypes
                  (name, metadata, parent_subtype, catalog_version_id)
                VALUES (%s, %s::jsonb, %s, %s)
                """,
                (
                    spec.name,
                    json.dumps(spec.metadata or {}),
                    spec.parent,
                    catalog_version_id,
                ),
            )
        _insert_subtype_closure(cur, catalog_version_id, subtype_specs)
        for parent, edge_type, child in narrowings or []:
            cur.execute(
                """
                INSERT INTO okg.narrowings
                  (parent_subtype, edge_type, child_subtype,
                   catalog_version_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (parent, edge_type, child, catalog_version_id),
            )
        for alias, canonical, scope in aliases or []:
            cur.execute(
                """
                INSERT INTO okg.aliases
                  (alias, normalized_alias, canonical, scope, catalog_version_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (
                    alias,
                    normalize_entity_name(alias),
                    canonical,
                    scope,
                    catalog_version_id,
                ),
            )
    return catalog_version_id


def seed_narrowings(
    conn: psycopg.Connection,
    *,
    catalog_version_id: int,
    subtypes: list[SubtypeSpec | str] | None = None,
    narrowings: list[tuple[str, str, str]],
) -> None:
    subtype_specs = _normalize_subtypes(subtypes, narrowings)
    with conn.cursor() as cur:
        for spec in subtype_specs.values():
            cur.execute(
                """
                INSERT INTO okg.subtypes
                  (name, metadata, parent_subtype, catalog_version_id)
                VALUES (%s, %s::jsonb, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (
                    spec.name,
                    json.dumps(spec.metadata or {}),
                    spec.parent,
                    catalog_version_id,
                ),
            )
        _insert_subtype_closure(cur, catalog_version_id, subtype_specs)
        for parent, edge_type, child in narrowings:
            cur.execute(
                """
                INSERT INTO okg.narrowings
                  (parent_subtype, edge_type, child_subtype,
                   catalog_version_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (parent, edge_type, child, catalog_version_id),
            )


def _normalize_subtypes(
    subtypes: list[SubtypeSpec | str] | None,
    narrowings: list[tuple[str, str, str]] | None,
) -> dict[str, SubtypeSpec]:
    specs: dict[str, SubtypeSpec] = {}
    for raw in subtypes or []:
        spec = SubtypeSpec(raw) if isinstance(raw, str) else raw
        specs[spec.name] = spec
    for parent, _edge_type, child in narrowings or []:
        specs.setdefault(parent, SubtypeSpec(parent))
        specs.setdefault(child, SubtypeSpec(child))
    return specs


def _insert_subtype_closure(
    cur: psycopg.Cursor,
    catalog_version_id: int,
    subtypes: dict[str, SubtypeSpec],
) -> None:
    for subtype in subtypes:
        depth = 0
        ancestor = subtype
        seen: set[str] = set()
        while ancestor is not None:
            if ancestor in seen:
                raise ValueError(f"cycle in test subtype parents at {ancestor}")
            seen.add(ancestor)
            cur.execute(
                """
                INSERT INTO okg.subtype_ancestors
                  (catalog_version_id, subtype_name, ancestor_name, depth)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (catalog_version_id, subtype, ancestor, depth),
            )
            parent = subtypes.get(ancestor).parent if ancestor in subtypes else None
            ancestor = parent
            depth += 1


def bind_factory_catalog_writer(monkeypatch: Any) -> None:
    """Route factory-built publishers through the explicit test writer.

    ``build_deployment_publisher`` resolves catalog writer authority from the
    deployment directory and refuses when there is none. Tests that drive a
    product entrypoint (``okg enricher backfill`` and friends) against a
    synthetic deployment have no directory on disk, so they bind the same
    explicit test writer ``bind_test_catalog_writer`` gives direct
    ``Publisher`` call sites. The binding is lazy: it runs when the product
    path asks for authority, which is after the test has seeded its catalog.

    This REPLACES the product refusal for the duration of the test that calls
    it: while the patch is installed, an unresolvable deployment resolves
    instead of failing. Nothing outside such a test is affected, and a test
    that means to prove the refusal must restore
    ``build_runtime_catalog_identity`` itself.
    """
    from okg.substrate.publisher import deployment_stack as stack_mod

    def _identity(
        *,
        dsn: str,
        deployment_name: str,
        deployment_dir: Any,
        command: str,
    ) -> Any:
        del deployment_dir, command
        return bind_test_catalog_writer(dsn, deployment_name)

    monkeypatch.setattr(
        stack_mod,
        "build_runtime_catalog_identity",
        _identity,
    )


def commit_fixture_deployment(deployment_dir: Path) -> Path:
    """Commit a synthetic deployment directory as its own Git repository.

    A publish records repository provenance and refuses a deployment that is
    not inside a Git working tree, or whose tree has uncommitted paths. A test
    that authors a deployment under ``tmp_path`` calls this after writing the
    files, so the publish reads a clean committed tree. Issue #2554.
    """
    import os
    import subprocess

    git_env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "OKG fixture",
        "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
        "GIT_COMMITTER_NAME": "OKG fixture",
        "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
    }
    for argv in (
        ["git", "init", "-q", "-b", "main"],
        ["git", "add", "-A"],
        ["git", "commit", "-q", "-m", "fixture"],
    ):
        subprocess.run(argv, cwd=deployment_dir, env=git_env, check=True)
    return deployment_dir


def bind_factory_test_deployment(
    monkeypatch: Any,
    tmp_path: Path,
    deployment_name: str,
) -> Path:
    """Give a synthetic deployment the authority the publisher factory needs.

    ``build_deployment_publisher`` refuses without two things a synthetic test
    deployment lacks: catalog writer authority and a deployment directory
    inside a Git repository (repository provenance). This binds the explicit
    test writer (``bind_factory_catalog_writer``) and resolves
    ``deployment_name`` to a minimal committed deployment directory under
    ``tmp_path``; every other name still resolves normally. Issue #2866.
    """
    import okg.substrate.deployments as deployments

    bind_factory_catalog_writer(monkeypatch)
    deployment_dir = tmp_path / f"{deployment_name}-deployment"
    (deployment_dir / "schemas").mkdir(parents=True)
    (deployment_dir / "source_registry.yaml").write_text(
        "sources: []\n",
        encoding="utf-8",
    )
    (deployment_dir / "deployment.yaml").write_text(
        f"name: {deployment_name}\n"
        "description: synthetic test deployment\n"
        "schema_dir: schemas\n"
        "source_registry: source_registry.yaml\n",
        encoding="utf-8",
    )
    commit_fixture_deployment(deployment_dir)

    real_find = deployments.find_deployment_dir
    monkeypatch.setattr(
        deployments,
        "find_deployment_dir",
        lambda name, **kwargs: (
            deployment_dir if name == deployment_name else real_find(name, **kwargs)
        ),
    )
    return deployment_dir
