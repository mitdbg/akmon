"""BEAVER-specific wiring of the general RelationalSchemaSource.

Registered in source_registry.yaml as `beaver_okg.source:BeaverSchemaSource`.
Everything reusable lives in
`okg.substrate.library.sources.relational_schema` (database→table→column nodes,
contains/references edges, information_schema introspection). This module
supplies only what is specific to BEAVER: the `beaver-mysql` connection, the
dataset list (`dw_real` reuses `dw`, so only dw/neutron/nova are introspected),
and the id namespace. Swap the connection and it ingests any relational DB.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

from okg.substrate.library.sources.relational_schema import (
    InformationSchemaProvider,
    RelationalSchemaSource,
)

NAMESPACE = "beaver"
DATABASES = ["dw", "neutron", "nova"]  # dw_real reuses dw's database upstream


def beaver_mysql_connect():
    import mysql.connector

    return mysql.connector.connect(
        host=os.environ.get("BEAVER_MYSQL_HOST", "127.0.0.1"),
        port=int(os.environ.get("BEAVER_MYSQL_PORT", "3307")),
        user=os.environ.get("BEAVER_MYSQL_USER", "root"),
        password=os.environ.get("BEAVER_MYSQL_PASSWORD", "beaver_dev"),
    )


def _schema_structure_items(
    provider: InformationSchemaProvider,
) -> list[tuple[str, bytes]]:
    """Enumerate the DETERMINISTIC schema structure as ``(key, bytes)`` items.

    Issue #323 change-probe content for both BEAVER sources. For every table in
    every configured database we emit one item over its STABLE structure only:

      - the qualified table name,
      - its columns sorted by ``(name, data_type, is_nullable)``,
      - its declared foreign keys sorted by ``(column, ref_table, ref_column)``.

    We deliberately EXCLUDE ``sample_rows`` and the categorical DISTINCT values:
    both are read with ``LIMIT`` and NO ``ORDER BY``, so MySQL is free to return
    different rows across reads — hashing them would make the probe permanently
    "changed" (sound but useless). The deterministic schema structure is the
    determinant of every *stable* emitted fact (schema/table/column nodes,
    contains/references edges, and the SET of text columns the domain source
    iterates). A DB-connect / introspection failure raised inside this callable
    propagates out of the probe so ``run_probe`` maps it to ``changed=True``.
    """
    import hashlib

    items: list[tuple[str, bytes]] = []
    for db in provider.databases():
        for table in provider.tables(db):
            cols = sorted(
                (c.name, c.data_type, c.nullable) for c in table.columns
            )
            fks = sorted(
                (fk.column, fk.ref_table, fk.ref_column)
                for fk in table.foreign_keys
            )
            digest = hashlib.sha256()
            digest.update(f"table={db}.{table.name}".encode("utf-8"))
            for name, dtype, nullable in cols:
                digest.update(b"\x00col\x00")
                digest.update(f"{name}|{dtype}|{nullable}".encode("utf-8"))
            for col, ref_t, ref_c in fks:
                digest.update(b"\x00fk\x00")
                digest.update(f"{col}|{ref_t}|{ref_c}".encode("utf-8"))
            items.append((f"{db}.{table.name}", digest.digest()))
    return items


@dataclass
class BeaverSchemaSource:
    """Registry-instantiable SourceAdapter over the loaded BEAVER MySQL.

    Serializable params (so source_registry.yaml can construct it); builds the
    live-introspection provider and delegates to the general
    RelationalSchemaSource — the only BEAVER-specific code in the ingest path.
    """

    namespace: str = NAMESPACE
    databases: tuple[str, ...] = tuple(DATABASES)
    sample_limit: int = 3
    name: str = "beaver_schema"
    profile: str = "reference_catalog"

    # Incremental-source contract (issue #323): the BEAVER warehouse has no git
    # scope and no mutable-API watermark, so it falls to the O(content) floor —
    # read+hash the live information_schema introspection, skip the full
    # node/edge build on no-change. The probe hashes ONLY the DETERMINISTIC
    # schema structure (table/column/FK shape); sample_rows are excluded because
    # they are read with an unordered LIMIT (non-deterministic). A MySQL connect
    # failure inside the content_items callable raises -> run_probe forces a full
    # read. See `concerns` in the migration report for the determinism rationale.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )

        def _items() -> list[tuple[str, bytes]]:
            provider = InformationSchemaProvider(
                connect=beaver_mysql_connect,
                databases_=list(self.databases),
                sample_limit=self.sample_limit,
            )
            return _schema_structure_items(provider)

        return ContentHashProbe(
            content_items=_items,
            config={
                "namespace": self.namespace,
                "databases": sorted(self.databases),
                "sample_limit": self.sample_limit,
            },
            emit_targets=[sys.modules[__name__]],
        )

    def run(self, run_id: str, *, mode: str = "cursor", sync_scope: Optional[dict] = None):
        provider = InformationSchemaProvider(
            connect=beaver_mysql_connect,
            databases_=list(self.databases),
            sample_limit=self.sample_limit,
        )
        delegate = RelationalSchemaSource(provider=provider, namespace=self.namespace, name=self.name)
        return delegate.run(run_id, mode=mode, sync_scope=sync_scope)


@dataclass
class BeaverDomainSource:
    """Registry-instantiable domain-value source over the loaded BEAVER MySQL.

    Delegates to the general RelationalDomainSource: emits `value` nodes + a
    `column -[contains]-> value` edge for each categorical value of every
    low-cardinality text column (task.okg.domain-enrich).
    """

    namespace: str = NAMESPACE
    databases: tuple[str, ...] = tuple(DATABASES)
    max_cardinality: int = 40
    name: str = "beaver_domain"
    profile: str = "reference_catalog"

    # Incremental-source contract (issue #323): same O(content) floor as the
    # schema source. The domain source emits `value` nodes only for the TEXT
    # columns of the live schema, so the deterministic determinant of the SET it
    # iterates is the schema structure (which columns exist and their data_type).
    # The actual categorical VALUES are read with an unordered LIMIT and are
    # non-deterministic, so the probe tracks schema structure only — see
    # `concerns`. An add/drop of a table or text column busts the token; a MySQL
    # connect failure raises -> run_probe forces a full read.
    change_probe_kind = "content_hash"

    def __post_init__(self) -> None:
        self.change_probe = self._build_change_probe()

    def _build_change_probe(self) -> Any:
        import sys

        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )

        def _items() -> list[tuple[str, bytes]]:
            provider = InformationSchemaProvider(
                connect=beaver_mysql_connect,
                databases_=list(self.databases),
            )
            return _schema_structure_items(provider)

        return ContentHashProbe(
            content_items=_items,
            config={
                "namespace": self.namespace,
                "databases": sorted(self.databases),
                "max_cardinality": self.max_cardinality,
            },
            emit_targets=[sys.modules[__name__]],
        )

    def run(self, run_id: str, *, mode: str = "cursor", sync_scope: Optional[dict] = None):
        from okg.substrate.library.sources.relational_schema import RelationalDomainSource

        delegate = RelationalDomainSource(
            connect=beaver_mysql_connect, databases_=list(self.databases),
            namespace=self.namespace, max_cardinality=self.max_cardinality, name=self.name,
        )
        return delegate.run(run_id, mode=mode, sync_scope=sync_scope)


def make_source(databases: Optional[list[str]] = None, namespace: str = NAMESPACE,
                sample_limit: int = 3) -> BeaverSchemaSource:
    return BeaverSchemaSource(
        namespace=namespace, databases=tuple(databases or DATABASES), sample_limit=sample_limit
    )


def _smoke() -> None:
    from okg.substrate.library.sources.base import EdgeFact, NodeFact

    n: dict[str, int] = {}
    e: dict[str, int] = {}
    for f in make_source().run("smoke").facts:
        if isinstance(f, NodeFact):
            n[f.subtype] = n.get(f.subtype, 0) + 1
        elif isinstance(f, EdgeFact):
            e[f.edge_type] = e.get(f.edge_type, 0) + 1
    print(f"nodes={n} edges={e}")


if __name__ == "__main__":
    _smoke()
