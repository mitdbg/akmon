"""RelationalSchemaSource — a reusable OKG source over any relational schema.

This adapter is **database-agnostic**: given a `SchemaProvider`, it maps a
relational schema to an OKG subgraph using the substrate's existing `dataset`
ontology module (Schema / Table / Column subtypes + contains/references
narrowings), so deployments reuse it directly:

  - one `schema` node per database/catalog
  - one `table` node per base table   + a `schema -[contains]-> table` edge
  - one `column` node per column        + a `table  -[contains]-> column` edge
  - one `references` edge per declared foreign key (column -> column)

Nothing here is specific to any one dataset. A deployment points it at its own
database(s) by supplying a provider. `InformationSchemaProvider` is the shipped,
reusable provider: it introspects a *live* SQL database through the SQL-standard
`information_schema` (works for MySQL, Postgres, and other engines that expose
it), so the same code builds an OKG of arbitrary database schemas.

What a caller supplies that is specific to *their* data: the connection, the
list of databases, and an id `namespace` (so multiple databases don't collide).
See `experiments/beaver/okg/source.py` for the BEAVER wiring as one example.

`references` edges capture *declared* join structure (foreign keys). Databases
without FK constraints (a common enterprise reality — e.g. BEAVER) yield zero
`references` edges; inferring joins from instance overlap is a separate enricher
concern, not this source's job.

Profile: `reference_catalog` (a schema is a slowly-changing reference set).
"""
from __future__ import annotations

import datetime as _dt
import hashlib
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable, Iterator, Optional, Protocol

from okg.substrate.library.sources.base import EdgeFact, NodeFact, SourceRun

DEFAULT_SAMPLE_ROWS = 3


def _json_safe(v: Any) -> Any:
    """Coerce a DB-API cell value to a JSON-serializable type.

    Sample rows go into node `attrs` (a jsonb column); arbitrary SQL columns
    yield Python datetimes, Decimals, bytes, etc. that strict json.dumps cannot
    encode. Keep JSON-native scalars as-is; render the rest losslessly to text.
    """
    if v is None or isinstance(v, (bool, int, float, str)):
        return v
    if isinstance(v, (_dt.datetime, _dt.date, _dt.time)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return str(v)
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).decode("utf-8", "replace")
    return str(v)


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    data_type: Optional[str] = None
    nullable: Optional[bool] = None


@dataclass(frozen=True)
class ForeignKey:
    column: str
    ref_table: str
    ref_column: str


@dataclass(frozen=True)
class TableInfo:
    name: str
    columns: list[ColumnInfo] = field(default_factory=list)
    sample_rows: list[dict[str, Any]] = field(default_factory=list)
    foreign_keys: list[ForeignKey] = field(default_factory=list)


class SchemaProvider(Protocol):
    """Yields the structure of one or more relational databases."""

    def databases(self) -> list[str]:
        raise NotImplementedError

    def tables(self, database: str) -> Iterator[TableInfo]:
        raise NotImplementedError


@dataclass
class InformationSchemaProvider:
    """Reusable provider: introspect a live SQL database via information_schema.

    `connect` is a zero-arg callable returning a DB-API connection (so this is
    driver-agnostic — mysql.connector, psycopg, sqlite3, …). `databases` is the
    list of schemas to ingest. Only the SQL-standard columns of
    `information_schema` are queried, so it works across engines; foreign-key
    and sample-row extraction degrade to empty on engines/permissions that do
    not support them rather than failing the run.
    """

    connect: Callable[[], Any]
    databases_: list[str]
    sample_limit: int = DEFAULT_SAMPLE_ROWS
    identifier_quote: str = "`"  # backtick for MySQL; pass '"' for Postgres

    def databases(self) -> list[str]:
        return list(self.databases_)

    def _q(self, ident: str) -> str:
        qc = self.identifier_quote
        return f"{qc}{ident}{qc}"

    def tables(self, database: str) -> Iterator[TableInfo]:
        conn = self.connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s AND table_type = 'BASE TABLE' "
                "ORDER BY table_name",
                (database,),
            )
            table_names = [row[0] for row in cur.fetchall()]

            # columns for the whole schema in one pass
            cur.execute(
                "SELECT table_name, column_name, data_type, is_nullable "
                "FROM information_schema.columns WHERE table_schema = %s "
                "ORDER BY table_name, ordinal_position",
                (database,),
            )
            cols_by_table: dict[str, list[ColumnInfo]] = {}
            for tname, cname, dtype, is_nullable in cur.fetchall():
                cols_by_table.setdefault(tname, []).append(
                    ColumnInfo(name=cname, data_type=dtype,
                               nullable=(str(is_nullable).upper() == "YES"))
                )

            fks_by_table = self._foreign_keys(cur, database)

            for tname in table_names:
                yield TableInfo(
                    name=tname,
                    columns=cols_by_table.get(tname, []),
                    sample_rows=self._sample_rows(cur, database, tname),
                    foreign_keys=fks_by_table.get(tname, []),
                )
            cur.close()
        finally:
            conn.close()

    def _foreign_keys(self, cur, database: str) -> dict[str, list[ForeignKey]]:
        """Declared FKs via key_column_usage (best effort; [] if unsupported)."""
        try:
            cur.execute(
                "SELECT table_name, column_name, referenced_table_name, "
                "referenced_column_name FROM information_schema.key_column_usage "
                "WHERE table_schema = %s AND referenced_table_name IS NOT NULL",
                (database,),
            )
            out: dict[str, list[ForeignKey]] = {}
            for tname, col, ref_t, ref_c in cur.fetchall():
                out.setdefault(tname, []).append(
                    ForeignKey(column=col, ref_table=ref_t, ref_column=ref_c)
                )
            return out
        except Exception:
            return {}  # engine without referenced_* columns (e.g. Postgres) — FKs optional

    def _sample_rows(self, cur, database: str, table: str) -> list[dict[str, Any]]:
        if self.sample_limit <= 0:
            return []
        try:
            cur.execute(
                f"SELECT * FROM {self._q(database)}.{self._q(table)} LIMIT {int(self.sample_limit)}"
            )
            headers = [d[0] for d in cur.description]
            return [{h: _json_safe(v) for h, v in zip(headers, row)} for row in cur.fetchall()]
        except Exception:
            return []


@dataclass
class RelationalSchemaSource:
    """Map a relational schema (via any SchemaProvider) to an OKG subgraph."""

    provider: SchemaProvider
    namespace: str
    name: str = "relational_schema"
    profile: str = "reference_catalog"

    def _db_id(self, db: str) -> str:
        return f"{self.namespace}:schema:{db}"

    def _table_id(self, db: str, table: str) -> str:
        return f"{self.namespace}:table:{db}.{table}"

    def _column_id(self, db: str, table: str, col: str) -> str:
        return f"{self.namespace}:column:{db}.{table}.{col}"

    def run(self, run_id: str, *, mode: str = "cursor", sync_scope: Optional[dict] = None) -> SourceRun:
        rev = {"run_id": run_id}
        ns = self.namespace
        databases = self.provider.databases()

        def facts() -> Iterator[Any]:
            for db in databases:
                tables = list(self.provider.tables(db))
                db_id = self._db_id(db)
                yield NodeFact(
                    node_id=db_id, subtype="schema",
                    attrs={"schema_id": db, "name": db, "label": db,
                           "namespace": ns, "n_tables": len(tables)},
                    source_record_id={"namespace": ns, "database": db},
                    source_revision=rev,
                )
                for t in tables:
                    tid = self._table_id(db, t.name)
                    yield NodeFact(
                        node_id=tid, subtype="table",
                        attrs={"table_id": tid, "name": t.name, "label": t.name,
                               "database": db, "namespace": ns,
                               "n_columns": len(t.columns), "sample_rows": t.sample_rows},
                        source_record_id={"namespace": ns, "database": db, "table": t.name},
                        source_revision=rev,
                    )
                    yield EdgeFact(
                        src=db_id, dst=tid, edge_type="contains",
                        source_record_id={"namespace": ns, "database": db, "table": t.name},
                        source_revision=rev,
                    )
                    for c in t.columns:
                        cid = self._column_id(db, t.name, c.name)
                        yield NodeFact(
                            node_id=cid, subtype="column",
                            attrs={"column_id": cid, "name": c.name, "label": c.name,
                                   "data_type": c.data_type, "nullable": c.nullable,
                                   "table": t.name, "database": db, "namespace": ns},
                            source_record_id={"namespace": ns, "database": db,
                                              "table": t.name, "column": c.name},
                            source_revision=rev,
                        )
                        yield EdgeFact(
                            src=tid, dst=cid, edge_type="contains",
                            source_record_id={"namespace": ns, "database": db,
                                              "table": t.name, "column": c.name},
                            source_revision=rev,
                        )
                    for fk in t.foreign_keys:
                        yield EdgeFact(
                            src=self._column_id(db, t.name, fk.column),
                            dst=self._column_id(db, fk.ref_table, fk.ref_column),
                            edge_type="references",
                            attrs={"via": "foreign_key"},
                            source_record_id={"namespace": ns, "database": db, "table": t.name},
                            source_revision=rev,
                        )

        return SourceRun(facts=facts(), completed_scope=True, run_mode=mode)


# categorical domain values are text; numeric/key columns are joins, not vocabulary
DOMAIN_TEXT_TYPES = {"char", "varchar"}


@dataclass
class RelationalDomainSource:
    """Emit the categorical vocabulary of low-cardinality columns (reusable).

    For each text column whose distinct-value count (sampled) is <= max_cardinality,
    emits one `value` node per distinct value + a `column -[contains]-> value` edge.
    A text-to-SQL query can then map an NL phrase onto a (column, value) predicate
    by searching `value` nodes. Database-agnostic; point it at any SQL DB.
    """

    connect: Callable[[], Any]
    databases_: list[str]
    namespace: str
    max_cardinality: int = 40
    row_sample: int = 10000
    identifier_quote: str = "`"
    name: str = "relational_domain"
    profile: str = "reference_catalog"

    def _q(self, ident: str) -> str:
        qc = self.identifier_quote
        return f"{qc}{ident}{qc}"

    def _column_id(self, db: str, table: str, col: str) -> str:
        return f"{self.namespace}:column:{db}.{table}.{col}"

    def _value_id(self, db: str, table: str, col: str, value: str) -> str:
        h = hashlib.md5(value.encode("utf-8", "ignore")).hexdigest()[:12]
        return f"{self.namespace}:value:{db}.{table}.{col}:{h}"

    def _categorical_values(self, cur, db: str, table: str, col: str) -> list[str]:
        # sample row_sample rows, take distinct, cap at max_cardinality+1; skip if over.
        cur.execute(
            f"SELECT DISTINCT {self._q(col)} FROM "
            f"(SELECT {self._q(col)} FROM {self._q(db)}.{self._q(table)} "
            f"WHERE {self._q(col)} IS NOT NULL LIMIT {int(self.row_sample)}) s "
            f"LIMIT {int(self.max_cardinality) + 1}"
        )
        vals = [str(r[0]).strip() for r in cur.fetchall() if r[0] is not None and str(r[0]).strip()]
        return vals if 0 < len(vals) <= self.max_cardinality else []

    def run(self, run_id: str, *, mode: str = "cursor", sync_scope: Optional[dict] = None) -> SourceRun:
        rev = {"run_id": run_id}
        ns = self.namespace

        def facts() -> Iterator[Any]:
            for db in self.databases_:
                conn = self.connect()
                try:
                    cur = conn.cursor()
                    cur.execute(
                        "SELECT table_name, column_name, data_type FROM information_schema.columns "
                        "WHERE table_schema = %s",
                        (db,),
                    )
                    text_cols = [(t, c) for t, c, dt in cur.fetchall()
                                 if (dt or "").lower() in DOMAIN_TEXT_TYPES]
                    for t, c in text_cols:
                        try:
                            vals = self._categorical_values(cur, db, t, c)
                        except Exception:
                            continue
                        if not vals:
                            continue
                        cid = self._column_id(db, t, c)
                        for v in vals:
                            vid = self._value_id(db, t, c, v)
                            yield NodeFact(
                                node_id=vid, subtype="value",
                                attrs={"value_id": vid, "name": v, "label": v,
                                       "column": c, "table": t, "database": db, "namespace": ns},
                                source_record_id={"namespace": ns, "database": db,
                                                  "table": t, "column": c, "value": v},
                                source_revision=rev,
                            )
                            yield EdgeFact(
                                src=cid, dst=vid, edge_type="contains",
                                source_record_id={"namespace": ns, "database": db,
                                                  "table": t, "column": c, "value": v},
                                source_revision=rev,
                            )
                    cur.close()
                finally:
                    conn.close()

        return SourceRun(facts=facts(), completed_scope=True, run_mode=mode)
