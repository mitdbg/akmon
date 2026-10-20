"""Explicit catalog authority for standalone database publish proofs."""
from __future__ import annotations

from pathlib import Path

import psycopg
from psycopg.types.json import Jsonb

from okg.substrate.catalog.modules_compose import ComposedCatalog, NarrowingRow
from okg.substrate.catalog.ownership import (
    CatalogIdentity,
    catalog_identity_for,
    claim_catalog_ownership,
    diagnose_catalog_ownership,
    ensure_catalog_ownership_schema,
    ensure_catalog_write_allowed,
    mark_catalog_version_seen,
)


_PROOF_WRITER_ROOT = Path(__file__).resolve().parents[1]


def bind_proof_catalog_writer(
    dsn: str,
    *,
    deployment_name: str,
    catalog_version_id: int,
    proof_name: str,
) -> CatalogIdentity:
    """Bind one freshly seeded proof catalog to one explicit writer.

    Unlike the retired ambient path, the caller must name the exact catalog
    version it just created. A version already owned by another deployment or
    writer refuses; no global latest row is selected or rewritten.
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        composed = _composed_catalog_for_version(
            conn,
            deployment_name=deployment_name,
            catalog_version_id=catalog_version_id,
            proof_name=proof_name,
        )
        identity = catalog_identity_for(
            composed=composed,
            deployment_dir=_PROOF_WRITER_ROOT,
            command=f"standalone publish proof: {proof_name}",
            writer_scope="deployment_root",
        )
        ensure_catalog_ownership_schema(conn)
        report = diagnose_catalog_ownership(conn, identity=identity)
        if report.get("owner") is None:
            claim_catalog_ownership(conn, identity=identity, force=True)
        else:
            ensure_catalog_write_allowed(conn, identity=identity)

        with conn.transaction(), conn.cursor() as cur:
            cur.execute(
                """
                UPDATE okg.catalog_versions
                   SET deployment_name = %s,
                       catalog_fingerprint = %s,
                       schema_fingerprint = %s,
                       writer_fingerprint = %s,
                       writer_provenance = %s,
                       created_by_command = %s
                 WHERE catalog_version_id = %s
                   AND status = 'applied'
                   AND (
                     deployment_name IS NULL
                     OR (
                       deployment_name = %s
                       AND writer_fingerprint = %s
                     )
                   )
                """,
                (
                    identity.deployment_name,
                    identity.catalog_fingerprint,
                    identity.schema_fingerprint,
                    identity.writer_fingerprint,
                    Jsonb(identity.provenance),
                    identity.provenance["command"],
                    catalog_version_id,
                    identity.deployment_name,
                    identity.writer_fingerprint,
                ),
            )
            if cur.rowcount != 1:
                raise RuntimeError(
                    "proof catalog version is missing or already bound to "
                    "different authority"
                )
        mark_catalog_version_seen(
            conn,
            identity=identity,
            catalog_version_id=catalog_version_id,
        )
    return identity


def _composed_catalog_for_version(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    catalog_version_id: int,
    proof_name: str,
) -> ComposedCatalog:
    origin = f"proof:{proof_name}"
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT name, metadata, parent_subtype
              FROM okg.subtypes
             WHERE catalog_version_id = %s
             ORDER BY name
            """,
            (catalog_version_id,),
        )
        subtype_rows = cur.fetchall()
        cur.execute(
            """
            SELECT parent_subtype, edge_type, child_subtype, attr_rules
              FROM okg.narrowings
             WHERE catalog_version_id = %s
             ORDER BY parent_subtype, edge_type, child_subtype
            """,
            (catalog_version_id,),
        )
        narrowing_rows = cur.fetchall()
        cur.execute(
            """
            SELECT alias, canonical, scope
              FROM okg.aliases
             WHERE catalog_version_id = %s
             ORDER BY alias
            """,
            (catalog_version_id,),
        )
        alias_rows = cur.fetchall()
        cur.execute(
            """
            SELECT id_type, regex, validator_kind, description
              FROM okg.identifier_patterns
             WHERE catalog_version_id = %s
             ORDER BY id_type, regex
            """,
            (catalog_version_id,),
        )
        identifier_rows = cur.fetchall()
        cur.execute(
            "SELECT count(*) FROM okg.invariants WHERE catalog_version_id = %s",
            (catalog_version_id,),
        )
        invariant_count = int(cur.fetchone()[0])

    if invariant_count:
        raise RuntimeError(
            "proof catalog authority helper requires an explicit composed "
            "catalog when invariants are present"
        )
    subtypes = {str(row[0]) for row in subtype_rows}
    return ComposedCatalog(
        deployment_name=deployment_name,
        modules=[origin],
        subtypes=subtypes,
        subtype_origin={subtype: origin for subtype in subtypes},
        subtype_metadata={str(row[0]): dict(row[1] or {}) for row in subtype_rows},
        subtype_parent={
            str(row[0]): str(row[2]) for row in subtype_rows if row[2] is not None
        },
        narrowings=[
            NarrowingRow(
                parent_subtype=str(parent),
                edge_archetype=str(edge_type),
                child_subtype=str(child),
                origin=origin,
                narrowing_name=f"{parent}_{edge_type}_{child}",
                cardinality=(rules or {}).get("cardinality"),
                entity_merge_equivalence=(rules or {}).get(
                    "entity_merge_equivalence"
                ),
            )
            for parent, edge_type, child, rules in narrowing_rows
        ],
        aliases=[
            {"alias": str(alias), "canonical": str(canonical), "scope": scope}
            for alias, canonical, scope in alias_rows
        ],
        identifier_patterns=[
            {
                "id_type": str(id_type),
                "regex": str(regex),
                "validator_kind": validator_kind,
                "description": description,
            }
            for id_type, regex, validator_kind, description in identifier_rows
        ],
    )


__all__ = ["bind_proof_catalog_writer"]
