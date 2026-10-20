"""Shared Chronos graph-store constants and pure helpers."""
from __future__ import annotations

import os
import re
import uuid
from dataclasses import dataclass, field
from hashlib import sha1
from typing import Any

# Imported for its side effect, deliberately and directly (issue #2920): it
# installs okg's idle-in-transaction resolver into `chronos_core.pg_session`,
# so every Chronos session this package opens carries the operator's
# configured bound instead of Chronos's bare 5-minute default. Relying on an
# incidental import chain would let a refactor drop it silently.
import okg.substrate.db.connection  # noqa: F401


NODE_TABLE = "okg.graph_nodes"
EDGE_TABLE = "okg.graph_edges"
# Chronos-registered subtype-pair / producer edge-count rollup. Maintained at
# publish time by a running-sum hook (okg.substrate.publisher.ontology_rollup)
# and read as-of a pinned generation by
# okg.substrate.console_views.ontology_rollup_read. Being a registered Chronos
# table, it gets branching / forking / as-of-generation reconstruction for free
# (a checkpoint is a metadata marker; reads at gen N see the right rows via the
# interval predicate; forks split it with the graph). This REPLACES the old
# generation-keyed derived-relation rollup and its carry-forward fragility.
ONTOLOGY_ROLLUP_TABLE = "okg.ontology_rollup"
ONTOLOGY_ROLLUP_PK = ["src_subtype", "edge_type", "dst_subtype", "producer"]
# Chronos-registered per-(dimension_kind, dimension_value) console count rollup.
# A second maintained-derivation row (alongside ONTOLOGY_ROLLUP_TABLE) on the
# shared kit (okg.substrate.derived_artifacts.maintained_derivation): maintained
# at publish time by okg.substrate.publisher.console_count_rollup and read as-of a
# pinned generation by okg.substrate.console_views.console_count_rollup_read.
# Dimensions: nodes by-subtype, edges by-edge-type, nodes by-source. The TOTALS
# dimension is NOT carried here -- it is served from checkpoint metadata by
# accelerate-operator-read-path's generation_counts. Being a registered Chronos
# table it inherits branching / forking / as-of-generation reconstruction.
CONSOLE_COUNT_ROLLUP_TABLE = "okg.console_count_rollup"
CONSOLE_COUNT_ROLLUP_PK = ["dimension_kind", "dimension_value"]
DEFAULT_BRANCH = "default"
CHRONOS_INTERVAL_CONTINUATION_PERCENT_ENV = (
    "CHRONOS_INTERVAL_CONTINUATION_PERCENT"
)


# Process-level set of (dbname, host, port) connections whose chronos schema
# bootstrap (metadata DDL + graph-table registration + default branch) has
# already run this process. The bootstrap is idempotent CREATE-IF-NOT-EXISTS
# DDL, so re-running it on an already-provisioned schema only re-fires no-op
# round-trips (~3x per ingest commit on the okg path). Production has one stable
# schema per process, so skipping it after the first construction is correct. A
# connection we cannot stably identify gets a per-object key, so the guard is
# never shared across connections and can never wrongly skip a fresh schema.
_SCHEMA_ENSURED: set[Any] = set()


def pg_identity_key(info: Any) -> Any | None:
    """A stable (dbname, host, port) key from a psycopg connection ``info``.

    Returns ``None`` when no database name is available, so callers fall back to
    a per-object key and never share (or wrongly skip) across connections.
    """
    if info is None:
        return None
    dbname = getattr(info, "dbname", None)
    if not dbname:
        return None
    return (
        "pg",
        dbname,
        str(getattr(info, "host", "") or ""),
        getattr(info, "port", "") or "",
    )


def schema_ensured_key(conn: Any) -> Any:
    """Schema-bootstrap guard key for a raw psycopg connection."""
    key = pg_identity_key(getattr(conn, "info", None))
    if key is not None:
        return key
    return ("conn", id(conn))


def schema_ensured_key_for_context(context: Any) -> Any:
    """Schema-bootstrap guard key for a chronos context's owned connection.

    Keyed on the SAME (dbname, host, port) shape as ``schema_ensured_key`` so a
    process reaching one database through both the connect() and from_connection()
    paths shares one guard entry.
    """
    raw = getattr(context, "conn", None)
    key = pg_identity_key(getattr(raw, "info", None))
    if key is not None:
        return key
    return ("ctx", id(context))


def schema_bootstrap_done(key: Any) -> bool:
    return key in _SCHEMA_ENSURED


def mark_schema_bootstrap_done(key: Any) -> None:
    _SCHEMA_ENSURED.add(key)


def clear_schema_ensured_cache() -> None:
    """Drop the process-level schema-bootstrap guard.

    For test harnesses (or any caller) that DROP/TRUNCATE the chronos schema
    out-of-band, i.e. without reconstructing the process. Production never
    resets the schema mid-process, so this is not on any production path.
    """
    _SCHEMA_ENSURED.clear()


def discard_schema_bootstrap_flag(key: Any) -> None:
    """Drop ONE database's bootstrap flag.

    Used by migrate()'s failure path (`schema-migration-hardening`): a
    rolled-back migrate transaction discards the ensure DDL, so the
    process-level "already bootstrapped" fact for that database is now a lie
    and must be forgotten before any retry or runtime self-heal.
    """
    _SCHEMA_ENSURED.discard(key)


_INTERVAL_BRANCHES_TABLE = "public._chronos_branch_interval_branches"


def interval_branches_table_present(executor: Any, backend: str) -> bool:
    """Return whether the interval backend's root branch table physically exists.

    Used to detect a *stale* process-level bootstrap cache: ``schema_bootstrap_done``
    can report ``True`` for a (db, host, port) key whose chronos schema was dropped
    or TRUNCATEd out-of-band (test resets do this), in which case the lazily-created
    ``_chronos_branch_interval_branches`` is gone and a later read would raise
    ``UndefinedTable``. Only the interval backend has this table; other backends are
    trusted to the cache (return ``True``). An unreadable probe is treated as absent
    so provisioning re-runs rather than failing closed.
    """
    if backend != "interval":
        return True
    try:
        cur = executor.execute(
            f"SELECT to_regclass('{_INTERVAL_BRANCHES_TABLE}')"
        )
        row = cur.fetchone()
    except Exception:  # noqa: BLE001 — unreadable metadata -> force re-provision
        return False
    if not row:
        return False
    value = row[0] if isinstance(row, (tuple, list)) else next(iter(row.values()))
    return value is not None


def graph_registered(db: Any) -> bool:
    """True iff graph_nodes/graph_edges are provisioned and Chronos-registered."""
    if getattr(db, "dialect", None) == "postgres":
        # Look the registry table up before reading it. On Postgres a failed
        # SELECT aborts the caller's transaction even when the error is
        # caught, so the next statement (CREATE SCHEMA in ensure_schema) fails
        # with "current transaction is aborted". That happens when the table
        # was dropped behind a warm process cache, e.g. after a test reset.
        found = db.execute(
            "SELECT to_regclass('_chronos_branch_tables') AS reg"
        ).fetchone()
        reg = (found["reg"] if hasattr(found, "keys") else found[0]) if found else None
        if reg is None:
            return False
    try:
        rows = db.execute(
            "SELECT table_name FROM _chronos_branch_tables "
            "WHERE table_name IN (?, ?)",
            (NODE_TABLE, EDGE_TABLE),
        ).fetchall()
    except Exception:  # noqa: BLE001 - registry table absent means not provisioned
        return False
    registered = {
        (row["table_name"] if hasattr(row, "keys") else row[0])
        for row in rows
    }
    if NODE_TABLE not in registered or EDGE_TABLE not in registered:
        return False
    reg = db.execute(
        "SELECT to_regclass('okg.graph_nodes') AS reg"
    ).fetchone()
    value = (reg["reg"] if hasattr(reg, "keys") else reg[0]) if reg else None
    return value is not None


def quote_ident(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def quote_physical_table(identifier: str) -> str:
    return "public." + quote_ident(identifier)


def chronos_interval_index_name(physical_table: str, suffix: str) -> str:
    digest = sha1(f"{physical_table}:{suffix}".encode("utf-8")).hexdigest()[:12]
    return f"idx_okg_chronos_{digest}_{suffix}"


def index_matches_columns(
    indexdef: str | None,
    desired_columns: tuple[str, ...],
) -> bool | None:
    """Whether an existing index's leading key columns match the desired order.

    ``None`` means the definition is absent or unparseable. The parser is
    intentionally limited to plain-column indexes; expression indexes can
    contain nested parentheses.
    """
    match = re.search(r"\(([^)]*)\)", indexdef or "")
    if not match:
        return None
    existing = [
        part.strip().split()[0].strip('"')
        for part in match.group(1).split(",")
        if part.strip()
    ]
    return existing[: len(desired_columns)] == list(desired_columns)


def parse_interval_continuation_percent(value: Any) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            "chronos.interval_continuation_percent must be an integer "
            "between 1 and 99"
        ) from exc
    if not 1 <= parsed <= 99:
        raise ValueError(
            "chronos.interval_continuation_percent must be between 1 and 99"
        )
    return parsed


def deployment_interval_continuation_percent(
    deployment_name: str | None,
) -> int | None:
    """Return the deployment's Chronos interval default, if one is declared.

    ``None`` means no deployment default applies: the env override is set,
    no deployment is named, no search root holds the named deployment
    (graph storage also runs outside any deployment directory), or the
    manifest declares no interval. In a sealed process (a release
    environment descriptor is set) every resolution error raises. A
    manifest the loader refuses (``DeploymentConfigError``) or fails to load
    raises: returning ``None`` there would run the default interval in place
    of a misspelled one (#2713).
    """
    if os.environ.get(CHRONOS_INTERVAL_CONTINUATION_PERCENT_ENV):
        return None
    if not deployment_name:
        return None
    from okg.substrate.deployments import (
        DeploymentResolutionError,
        resolve_deployment,
    )

    try:
        manifest = resolve_deployment(deployment_name)
    except DeploymentResolutionError:
        if os.environ.get("OKG_ENVIRONMENT_DESCRIPTOR") or os.environ.get(
            "OKG_RELEASE_ROOT"
        ):
            # A sealed process resolves only its pinned environment, and
            # that resolver reports a refused manifest as a resolution
            # error; "not found" is never a benign answer there.
            raise
        return None
    raw = getattr(manifest, "chronos", None)
    if not isinstance(raw, dict):
        return None
    if "interval_continuation_percent" not in raw:
        return None
    return parse_interval_continuation_percent(
        raw["interval_continuation_percent"]
    )


def load_chronos():
    try:
        from chronos_core.branching import ChronosBranchContext
    except ImportError as exc:  # pragma: no cover - exercised by install envs
        raise RuntimeError(
            "Chronos graph storage requires the chronos-core package"
        ) from exc
    return ChronosBranchContext


def stamp_store_application_name(dsn: str) -> str:
    """Attribution stamp for store-owned sessions (unify-connection-budget).

    Sessions opened via ``ChronosGraphStore.connect`` were the remaining
    ``<unnamed>`` backends in the pg_stat_activity inventory. A DSN that
    already names itself keeps its name. The URL form must be PRESERVED:
    ``make_conninfo``'s key=value rewrite has no URL scheme, and chronos
    selects its backend by scheme. A key=value DSN is therefore left
    untouched here (it simply goes unstamped) rather than rewritten.

    What happens to such a DSN downstream CHANGED with issue #1971:
    ``connect_sql_database`` now refuses a scheme-less string carrying libpq
    connection parameters, naming the URL form. It used to read it as a
    SQLite path and open a file named after the whole connection string.
    """
    if (
        dsn.startswith(("postgres://", "postgresql://"))
        and "application_name=" not in dsn
    ):
        sep = "&" if "?" in dsn else "?"
        return f"{dsn}{sep}application_name=okg-chronos-store"
    return dsn


def load_chronos_postgres_adapter():
    try:
        from chronos_core.branching.sql_adapters import PostgresDatabaseAdapter
    except ImportError as exc:  # pragma: no cover - exercised by install envs
        raise RuntimeError(
            "Chronos graph storage requires the chronos-core package"
        ) from exc
    return PostgresDatabaseAdapter


def edge_id(src: str, edge_type: str, dst: str) -> int:
    value = uuid.uuid5(uuid.NAMESPACE_URL, f"{src}|{edge_type}|{dst}").int
    return value & ((1 << 63) - 1)


@dataclass(frozen=True)
class GraphEditResult:
    nodes_upserted: int = 0
    nodes_deleted: int = 0
    edges_upserted: int = 0
    edges_deleted: int = 0
    nodes_by_subtype: dict[str, tuple[str, ...]] = field(default_factory=dict)
    node_attr_changes_by_subtype: dict[
        str, dict[str, tuple[str, ...]]
    ] = field(default_factory=dict)
    edges_by_type: dict[str, tuple[int, ...]] = field(default_factory=dict)
    edge_attr_changes_by_type: dict[
        str, dict[int, tuple[str, ...]]
    ] = field(default_factory=dict)
    edge_endpoints_by_type: dict[
        str, tuple[dict[str, Any], ...]
    ] = field(default_factory=dict)
    edge_table_available: bool = True
    delta_counts: dict[str, int] = field(default_factory=dict)
    timings_ms: dict[str, float] = field(default_factory=dict)

    def chronos_dirty_scope(
        self,
    ) -> tuple[
        dict[str, list[str]],
        dict[str, list[int]],
        bool,
        dict[str, dict[str, list[str]]],
        dict[str, list[dict[str, Any]]],
        dict[str, dict[int, list[str]]],
    ]:
        return (
            {k: list(v) for k, v in self.nodes_by_subtype.items()},
            {k: list(v) for k, v in self.edges_by_type.items()},
            self.edge_table_available,
            {
                subtype: {
                    node_id: list(fields)
                    for node_id, fields in node_changes.items()
                }
                for subtype, node_changes in (
                    self.node_attr_changes_by_subtype.items()
                )
            },
            {
                edge_type: [dict(endpoint) for endpoint in endpoints]
                for edge_type, endpoints in self.edge_endpoints_by_type.items()
            },
            {
                edge_type: {
                    edge_id: list(fields)
                    for edge_id, fields in edge_changes.items()
                }
                for edge_type, edge_changes in (
                    self.edge_attr_changes_by_type.items()
                )
            },
        )

    def write_set_as_dict(self) -> dict[str, Any]:
        return {
            "schema": "okg.graph_write_set.v1",
            "nodes_by_subtype": {
                k: list(v) for k, v in self.nodes_by_subtype.items()
            },
            "edges_by_type": {
                k: list(v) for k, v in self.edges_by_type.items()
            },
            "edge_table_available": self.edge_table_available,
            "node_attr_changes_by_subtype": {
                subtype: {
                    node_id: list(fields)
                    for node_id, fields in node_changes.items()
                }
                for subtype, node_changes in (
                    self.node_attr_changes_by_subtype.items()
                )
            },
            "edge_endpoints_by_type": {
                edge_type: [dict(endpoint) for endpoint in endpoints]
                for edge_type, endpoints in self.edge_endpoints_by_type.items()
            },
            "edge_attr_changes_by_type": {
                edge_type: {
                    str(edge_id): list(fields)
                    for edge_id, fields in edge_changes.items()
                }
                for edge_type, edge_changes in (
                    self.edge_attr_changes_by_type.items()
                )
            },
            "delta_counts": dict(self.delta_counts),
            "timings_ms": dict(self.timings_ms),
        }


def dedupe_rows_last(
    rows: list[dict[str, Any]],
    key: str,
) -> list[dict[str, Any]]:
    """Collapse same-key graph rows before handing them to Chronos."""
    if len(rows) < 2:
        return rows
    last: dict[Any, tuple[int, dict[str, Any]]] = {}
    for idx, row in enumerate(rows):
        last[row[key]] = (idx, row)
    return [row for _, row in sorted(last.values(), key=lambda item: item[0])]


def md5_text(text: str) -> str:
    import hashlib

    return hashlib.md5(text.encode("utf-8")).hexdigest()
