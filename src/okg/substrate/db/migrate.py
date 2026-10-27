"""okg migrate — apply the v1 schema idempotently.

Responsibility (docs/develop/architecture/overview.md, "Publication service",
schema"):

- Verifies the four required extensions are installed.
- Executes src/okg/substrate/db/schema.sql against the target DB.
- Idempotent: repeated runs converge to the same state.

Extension verification MUST fail loudly with `extension_missing`
before any DDL runs.
"""

from __future__ import annotations
from okg.substrate import fastyaml

import importlib.resources
import logging
import os
import time
from contextlib import ExitStack, contextmanager
import dataclasses
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg
from psycopg import sql as pgsql

from okg.substrate.db.view_convergence import (
    install_with_view_convergence,
    is_unreplaceable_view_error,
)
from okg.substrate.errors import (
    ExtensionMissing,
    ExtensionVersionMismatch,
    MigrateAuthorityUnresolvable,
    MigrationLockTimeout,
    PostgresVersionTooOld,
    SubstrateError,
)
from okg.substrate.db.connection import connect as db_connect


# Minimum supported Postgres major version. Bumped to 17 with
# the substrate-fuzzy-and-identifiers change (the
# `okg.find_identifier_mentions` function uses
# `regexp_match_positions()` which lands in 17). 17 has been GA
# since September 2024; carrying a 15/16 fallback would cost more
# than upgrading.
MIN_POSTGRES_MAJOR = 17


REQUIRED_EXTENSIONS: dict[str, str] = {
    # name        : minimum version
    "timescaledb": "2.15",
    "pg_textsearch": "1.0",
    "vector": "0.7",
    # Trigram similarity (fuzzy alias resolution) + edit-distance
    # / phonetic match (Levenshtein, Soundex, Metaphone). Stock
    # contrib modules; ship in the TimescaleDB images we use.
    "pg_trgm": "1.6",
    "fuzzystrmatch": "1.2",
    # GiST equality operator classes for live-table exclusion
    # constraints over (node_id / edge_id, generation-range).
    "btree_gist": "1.7",
}


# Extensions that are NICE to have but not required. The substrate
# checks for them, records the version when present, and skips
# without erroring when absent. `pg_cron` was here historically
# because substrate-shipped pg_cron jobs (`okg-publish`,
# `okg-lease-sweep`) drove the old publish path. The
# `retire_legacy_runtime` migration retired those jobs in favor of
# DBOS schedules. The old `okg cron` CLI surface is gone rather than
# installing pg_cron entries.
#
# Also: `pg_cron` is bound to a single Postgres database via
# `cron.database_name` in the cluster's postgresql.conf, so a
# deployment on a secondary DB cannot install it even if it wanted
# to. Keeping it required would block fresh-DB synth-bench /
# perf-test setups against any cluster whose pg_cron is bound
# elsewhere.
OPTIONAL_EXTENSIONS: dict[str, str] = {
    "pg_cron": "1.6",
}


# ---------------------------------------------------------------------------
# Migrate transaction guards (`schema-migration-hardening`).
#
# migrate() runs as ONE serialized, time-bounded transaction:
#
# * a per-database advisory xact lock (64-bit `hashtextextended`, never the
#   32-bit `hashtext`) serializes concurrent `okg up` runs — one applies, the
#   later one observes the recorded checksums under the lock and no-ops;
# * `SET LOCAL lock_timeout` / `statement_timeout` bound every lock wait and
#   statement so a held conflicting lock produces a typed, clean-rollback
#   `MigrationLockTimeout` after bounded retries — never an indefinite wedge
#   with a queue of blocked readers behind it.
#
# The knobs are env-tunable. A set-but-invalid value raises (P0 rule: never
# mask a failure with a default); unset uses the defaults below. Values are
# integer milliseconds.
# ---------------------------------------------------------------------------

MIGRATE_LOCK_TIMEOUT_ENV = "OKG_MIGRATE_LOCK_TIMEOUT_MS"
MIGRATE_STATEMENT_TIMEOUT_ENV = "OKG_MIGRATE_STATEMENT_TIMEOUT_MS"
MIGRATE_LOCK_ATTEMPTS_ENV = "OKG_MIGRATE_LOCK_ATTEMPTS"
DEFAULT_MIGRATE_LOCK_TIMEOUT_MS = 30_000
DEFAULT_MIGRATE_STATEMENT_TIMEOUT_MS = 600_000
DEFAULT_MIGRATE_LOCK_ATTEMPTS = 3
_MIGRATE_RETRY_BACKOFF_SECONDS = 0.5
# Advisory-lock key namespace. The key is
# hashtextextended('okg:migrate:' || current_database(), 0), so the lock is
# per-database within the cluster (advisory locks are cluster-scoped).
MIGRATE_ADVISORY_LOCK_NAMESPACE = "okg:migrate:"


def _positive_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    # Empty string deliberately means "unset" (the `VAR= cmd` shell idiom for
    # clearing an inherited value); every other non-integer value refuses.
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise SubstrateError(
            f"{name} must be a positive integer, got {raw!r}",
            data={"env": name, "value": raw},
            fix_hint=f"unset {name} or set it to a positive integer",
        ) from exc
    if value <= 0:
        raise SubstrateError(
            f"{name} must be a positive integer, got {raw!r}",
            data={"env": name, "value": raw},
            fix_hint=f"unset {name} or set it to a positive integer",
        )
    return value


def _migrate_timeout_settings() -> tuple[int, int, int]:
    """(lock_timeout_ms, statement_timeout_ms, attempts) for this run."""
    return (
        _positive_int_env(
            MIGRATE_LOCK_TIMEOUT_ENV, DEFAULT_MIGRATE_LOCK_TIMEOUT_MS
        ),
        _positive_int_env(
            MIGRATE_STATEMENT_TIMEOUT_ENV,
            DEFAULT_MIGRATE_STATEMENT_TIMEOUT_MS,
        ),
        _positive_int_env(
            MIGRATE_LOCK_ATTEMPTS_ENV, DEFAULT_MIGRATE_LOCK_ATTEMPTS
        ),
    )


def _begin_guarded_migrate_transaction(
    conn: psycopg.Connection,
    *,
    lock_timeout_ms: int,
    statement_timeout_ms: int,
) -> None:
    """Bound the migrate transaction, then take the per-database migration lock.

    ``SET LOCAL`` (via ``set_config(..., is_local=true)``) scopes both
    timeouts to migrate's single transaction, so nothing leaks into the
    session — and the timeouts are installed BEFORE the advisory-lock wait so
    even the serialization wait is bounded. ``pg_advisory_xact_lock`` releases
    at commit/rollback: the whole migrate body runs under the lock.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT set_config('lock_timeout', %s, true)",
            (str(lock_timeout_ms),),
        )
        cur.execute(
            "SELECT set_config('statement_timeout', %s, true)",
            (str(statement_timeout_ms),),
        )
        cur.execute(
            "SELECT pg_advisory_xact_lock("
            "hashtextextended(%s || current_database(), 0))",
            (MIGRATE_ADVISORY_LOCK_NAMESPACE,),
        )


class _CommitSuppressedConnection:
    """Connection proxy handed to the Chronos ensure path under migrate().

    The Chronos ensure helpers (``ensure_schema``, the rollup-table ensures,
    ``ensure_interval_storage_upgrade``, branch/metadata bootstrap) are shared
    with runtime self-healing paths, where their internal ``commit()`` calls
    are correct. Under migrate() they broke the documented single-transaction
    contract (audit F1: committed DDL with no ledger row on a later failure).
    This proxy turns ``commit()`` into a counted no-op — migrate's own single
    final commit makes everything durable atomically — while delegating every
    other attribute to the real connection. ``rollback()`` stays REAL (an
    aborting error must abort the whole migrate transaction) but is COUNTED:
    the chronos ensure paths reachable today only roll back on exception
    paths that re-raise, yet chronos session retry loops in general may
    rollback-and-continue — which under migrate would silently discard the
    uncommitted schema body. ``assert_no_swallowed_rollback`` converts that
    latent class into a loud, typed failure instead of a partial commit.
    """

    def __init__(self, conn: psycopg.Connection) -> None:
        self._okg_real_conn = conn
        self.suppressed_commits = 0
        self.observed_rollbacks = 0

    def commit(self) -> None:
        self.suppressed_commits += 1

    def rollback(self) -> None:
        self.observed_rollbacks += 1
        self._okg_real_conn.rollback()

    def assert_no_swallowed_rollback(self) -> None:
        """Fail loud if the ensure path rolled back and then CONTINUED.

        A rollback followed by a propagated exception never reaches here
        (migrate aborts first). Reaching this check with a recorded rollback
        means some helper rolled back migrate's transaction and swallowed the
        trigger — committing now would persist a torn subset of the work.
        """
        if self.observed_rollbacks:
            raise SubstrateError(
                "chronos ensure path rolled back migrate's transaction "
                f"{self.observed_rollbacks} time(s) without propagating an "
                "error; refusing to commit a torn migrate",
                data={"observed_rollbacks": self.observed_rollbacks},
                fix_hint=(
                    "an ensure helper used a rollback-and-continue retry "
                    "under migrate; route it outside migrate or make it "
                    "re-raise"
                ),
            )

    def __getattr__(self, name: str) -> Any:
        return getattr(self._okg_real_conn, name)


class _RegistryAdapterShim:
    """Just enough adapter surface for chronos cache invalidation by db key."""

    def __init__(self, conn: psycopg.Connection) -> None:
        self.raw_connection = conn


def _invalidate_chronos_ensure_caches(conn: psycopg.Connection) -> None:
    """Forget process-level ensure/registry caches after a rolled-back migrate.

    The Chronos ensure paths memoize "metadata ensured / table registered"
    facts per (dbname, host, port) in process memory. A failed migrate rolls
    the underlying DDL back, so those facts now describe work that does not
    exist — a retry (migrate's own bounded retry loop, or a later runtime
    self-heal in the same process) would skip re-creating registry tables and
    abort on the first read. Drop the per-database entries so the next attempt
    re-derives state from the database. Never masks the original error: cache
    invalidation failures are logged, not raised.
    """
    try:
        from chronos_core.branching._common import invalidate_registry_cache
        from okg.substrate.graph_store_internal.common import (
            discard_schema_bootstrap_flag,
            schema_ensured_key,
        )

        discard_schema_bootstrap_flag(schema_ensured_key(conn))
        invalidate_registry_cache(_RegistryAdapterShim(conn))
    except Exception:  # noqa: BLE001 — never shadow the migrate failure itself
        logging.getLogger(__name__).warning(
            "failed to invalidate chronos process caches after a rolled-back "
            "migrate; a retry in this process may need a fresh process",
            exc_info=True,
        )


@contextmanager
def _ensure_caches_rollback_with(conn: psycopg.Connection):
    """On ANY failure inside migrate's transaction, roll the process-level
    ensure caches back alongside the database transaction (which the psycopg
    connection context rolls back), then re-raise."""
    try:
        yield
    except BaseException:
        _invalidate_chronos_ensure_caches(conn)
        raise


def ensure_required_extensions(dsn: str) -> dict[str, list[str]]:
    """``CREATE EXTENSION IF NOT EXISTS`` for each required extension.

    Per ``canonical-dev-okg``: the from-empty bootstrap provisions the required
    extensions up front so a fresh database does not fail ``okg migrate`` one
    extension at a time. Requires a role with CREATE privilege on the target
    database (true for the dev superuser). Returns the names created vs. those
    already present.
    """
    from okg.substrate.db.connection import connect as db_connect

    created: list[str] = []
    already_present: list[str] = []
    with db_connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT extname FROM pg_extension WHERE extname = ANY(%s)",
                (list(REQUIRED_EXTENSIONS),),
            )
            installed = {row[0] for row in cur.fetchall()}
            for name in REQUIRED_EXTENSIONS:
                if name in installed:
                    already_present.append(name)
                    continue
                # Names come from the trusted REQUIRED_EXTENSIONS constant, not
                # user input; quote as an identifier for safety.
                cur.execute(f'CREATE EXTENSION IF NOT EXISTS "{name}"')
                created.append(name)
    return {"created": created, "already_present": already_present}


@dataclass(frozen=True)
class MigrateResult:
    """Outcome of a migrate run."""

    statements_executed: int
    statements_skipped: int
    extension_versions: dict[str, str]
    # idempotent == True if re-running produced no new state. We
    # approximate this as "no statement raised an error other than
    # a 'already exists' no-op that we've pre-filtered." In
    # practice a clean second run has `statements_executed == N`
    # and `statements_skipped == 0` but no observable DB change —
    # Postgres' `IF NOT EXISTS` does the work.
    idempotent_second_run: bool
    #: Views the frozen-view-shape convergence had to drop and recreate
    #: (``view_convergence``). Empty on every ordinary run. NON-empty is
    #: an operator-visible event, not a detail. The shared grant registry is
    #: reapplied and verified after the complete view install; callers should
    #: still report this list so destructive view replacement is observable.
    converged_views: tuple[str, ...] = ()
    #: Declared multi-phase migrations still pending after this run.
    #: Empty on every converged result: migrate()'s orchestration loop
    #: runs the registered specs and re-runs the ordered pass, so a
    #: non-empty value only ever escapes from the inner single-pass
    #: helper (or a run that raised before converging).
    pending_multiphase: tuple[str, ...] = ()
    #: Ordinals the ordered runner deferred behind a pending multi-phase
    #: migration in THIS pass (NNNN barrier); applied by the second pass.
    deferred_post_multiphase: tuple[str, ...] = ()
    #: Multi-phase migrations the orchestration loop ran to completion in
    #: this migrate() call, with their per-phase batch counts.
    multiphase_applied: tuple[dict[str, Any], ...] = ()
    #: The online graph read-index step that ran before the schema
    #: transaction (okg#3112): ``status`` converged, deferred or
    #: not_registered, with what it built and dropped and what is pending.
    graph_read_indexes: dict[str, Any] | None = None
    #: How migrate kept the declared-attribute index reconciler out of the
    #: graph tables (okg#3112 step 2): ``free``/``acquired`` (with any
    #: cancelled reconciler pid), ``not_acquired`` (reason ``wait_expired``
    #: after the 60 s wait for the reconciler, or
    #: ``held_by_another_index_change`` within about a second when only
    #: another index change holds the DDL lock), or
    #: ``not_installed`` before migration 0037.
    declared_attr_index_preemption: dict[str, Any] | None = None
    #: Issue #3342: blocked incomplete-ingest checkpoints whose repair
    #: contract this run retired (one supersede report each). Empty once
    #: converged.
    incomplete_checkpoint_retirements: tuple[dict[str, Any], ...] = ()
    #: PACT recovery-contract-removed-sources: repair contracts whose
    #: required sources were removed or renamed. With a deployment named,
    #: one report per held contract (``retired``, ``retired_by_other`` or
    #: ``not_retired`` with its reason); without, ``candidate`` reports and
    #: nothing retired. Empty when no deployment holds such a contract.
    source_contract_retirements: tuple[dict[str, Any], ...] = ()
    #: PACT openrouter-embeddings-hnsw: the semantic-search HNSW index step.
    #: ``None`` when no deployment was named or its embedding model is not
    #: indexed (embeddings off, ``hash``, sentence-transformers), so a
    #: deployment without such a model pays nothing.
    semantic_ann_index: dict[str, Any] | None = None
    #: PACT openrouter-embeddings-hnsw (design v3.2): the deployment's vector
    #: sets, each registered with its watermark for the current generation
    #: and its index state. ``None`` when no deployment was named or it
    #: declares no enabled set.
    vector_sets: tuple[dict[str, Any], ...] | None = None


def _ensure_vector_sets(
    dsn: str, deployment_name: str | None,
) -> tuple[dict[str, Any], ...] | None:
    """Register the deployment's vector sets, record a watermark for its
    current generation, and build each set's index.

    Runs after the schema transaction commits. A set's header fields are
    checked against the catalog's read classification first: a per-type
    header field some reader cannot see refuses, with the schema already
    migrated.
    """
    if not deployment_name:
        return None
    from okg.substrate.deployments import find_deployment_dir

    if find_deployment_dir(deployment_name) is None:
        return None
    from okg.substrate.deployments import resolve_deployment

    sets = resolve_deployment(deployment_name).embedding_config.vector_sets
    from okg.substrate.db.connection import connect as db_connect

    if not sets:
        # No set configured: any set still registered takes no new links.
        from okg.substrate.embeddings.store import deactivate_other_sets

        with db_connect(dsn) as conn:
            with conn.transaction():
                deactivated = deactivate_other_sets(conn, [])
        return ({"deactivated_sets": deactivated},) if deactivated else None
    from okg.substrate.embeddings.documents import check_templates
    from okg.substrate.embeddings.store import (
        deactivate_other_sets,
        ensure_set_index,
        load_classifications,
        record_watermarks,
        register_sets,
    )
    from okg.substrate.graph_store import ChronosGraphStore

    try:
        store = ChronosGraphStore.connect(dsn, ensure_schema=False)
        try:
            # The latest published generation of EVERY branch of the
            # deployment gets a watermark, so a pin on any branch reads.
            latest_by_branch: dict[str, Any] = {}
            for generation in store.list_generations(
                deployment_name=deployment_name, branch=None, status="published",
            ):
                latest_by_branch[str(generation.branch_id)] = generation
        finally:
            store.close()
        current = sorted(
            str(generation.checkpoint_id) for generation in latest_by_branch.values()
        )
        reports: list[dict[str, Any]] = []
        with db_connect(dsn) as conn:
            with conn.transaction():
                with conn.cursor() as cur:
                    cur.execute("SELECT DISTINCT name FROM okg.subtypes")
                    subtypes = [str(row[0]) for row in cur.fetchall()]
                classifications = load_classifications(conn, subtypes)
                omitted = {
                    item.set_id: check_templates(item, classifications, subtypes)
                    for item in sets
                }
                registered = register_sets(conn, sets)
                # Sets gone from the config keep their rows and take no new
                # links.
                deactivated = deactivate_other_sets(conn, sets)
                watermarks = sum(
                    record_watermarks(conn, generation_id, recorded_by="migrate")
                    for generation_id in current
                )
            for item, row in zip(sets, registered):
                reports.append({
                    **row,
                    "watermark_generations": current,
                    "header_fields_omitted": omitted[item.set_id],
                    "deactivated_sets": deactivated,
                })
        for report, item in zip(reports, sets):
            report["index"] = ensure_set_index(dsn, item)
            report["watermarks_written"] = watermarks
        return tuple(reports)
    except SubstrateError as exc:
        data = dict(getattr(exc, "data", None) or {})
        err = SubstrateError(
            f"the schema is migrated, but registering the vector sets failed: {exc}",
            data={**data, "schema_migrated": True},
            fix_hint=exc.fix_hint,
        )
        err.code = exc.code
        raise err from exc


def _ensure_semantic_ann_index(
    dsn: str, deployment_name: str | None,
) -> dict[str, Any] | None:
    """Build the deployment's semantic HNSW index when its model needs one.

    Runs after the schema transaction commits, with CREATE INDEX
    CONCURRENTLY, so writers are never blocked. A failure raises with the
    schema already migrated; a rerun retries the build.
    """
    if not deployment_name:
        return None
    from okg.substrate.deployments import find_deployment_dir

    if find_deployment_dir(deployment_name) is None:
        return None
    from okg.substrate.deployments import resolve_deployment
    from okg.substrate.embeddings.config import node_column_ann_model_id
    from okg.substrate.semantic_ann import ann_spec_for_model_id, ensure_ann_index

    config = resolve_deployment(deployment_name).embedding_config
    if not config.enabled or not config.backend:
        return None
    spec = ann_spec_for_model_id(node_column_ann_model_id(config))
    if spec is None:
        return None
    try:
        return ensure_ann_index(
            dsn,
            spec,
            # Before a backfill no row carries the model; building then would
            # maintain the index row by row through the whole backfill.
            defer_when_empty=True,
            on_progress=lambda msg: logging.getLogger(__name__).info(
                "migrate: %s", msg,
            ),
        )
    except Exception as exc:
        raise SubstrateError(
            "the schema is migrated, but building the semantic-search index "
            "failed",
            data={
                "schema_migrated": True,
                "model_id": spec.model_id,
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
            fix_hint=(
                "fix the cause, then rerun `okg migrate --apply` or "
                "`okg db semantic-index --deployment <name> --apply`"
            ),
        ) from exc


def _verify_extensions(conn: psycopg.Connection) -> dict[str, str]:
    """Confirm the required extensions are installed (and record any
    optional ones for visibility).

    Raises ExtensionMissing / ExtensionVersionMismatch BEFORE any
    DDL runs when a REQUIRED extension is absent or older than the
    minimum. OPTIONAL extensions (e.g. `pg_cron`, retired from
    substrate use) are recorded when present but their absence is
    NOT fatal.

    Returns the merged installed-version dict (required + optional)
    so the caller can surface it via ``okg doctor``.
    """
    all_to_check = list(REQUIRED_EXTENSIONS.keys()) + list(OPTIONAL_EXTENSIONS.keys())
    with conn.cursor() as cur:
        cur.execute(
            "SELECT extname, extversion FROM pg_extension WHERE extname = ANY(%s)",
            (all_to_check,),
        )
        installed = dict(cur.fetchall())

    missing_required = [n for n in REQUIRED_EXTENSIONS if n not in installed]
    if missing_required:
        raise ExtensionMissing(
            f"required Postgres extensions missing: {sorted(missing_required)}",
            data={
                "missing": sorted(missing_required),
                "required": sorted(REQUIRED_EXTENSIONS.keys()),
            },
            fix_hint=(
                # A wheel user has no source checkout, so the runnable
                # command comes FIRST and the compose route is labelled as
                # checkout-only (issue #2128, finding F7).
                "create each missing extension on the target database as a "
                "superuser, e.g. `psql <deployment DSN> -c \"CREATE "
                "EXTENSION IF NOT EXISTS "
                + sorted(missing_required)[0]
                + " CASCADE\"`, once per name above; timescaledb "
                "additionally needs the server started with "
                "shared_preload_libraries=timescaledb. From a source "
                "checkout only, `docker compose -f ops/pg/docker-compose.yaml "
                "up -d` brings up a server that already has them."
            ),
        )

    # Version check is lenient for v1 — we only warn on major
    # version regressions. The substrate tolerates point-release
    # drift. The `okg doctor` command surfaces exact versions.
    # Required: must pass. Optional: only check version when
    # actually installed (absence is fine).
    for name, min_ver in REQUIRED_EXTENSIONS.items():
        installed_ver = installed[name]
        if _major(installed_ver) < _major(min_ver):
            raise ExtensionVersionMismatch(
                f"{name} version {installed_ver} < required {min_ver}",
                data={
                    "extension": name,
                    "installed": installed_ver,
                    "required": min_ver,
                },
                fix_hint=f"upgrade {name} to >= {min_ver}",
            )
    for name, min_ver in OPTIONAL_EXTENSIONS.items():
        if name not in installed:
            continue
        installed_ver = installed[name]
        if _major(installed_ver) < _major(min_ver):
            raise ExtensionVersionMismatch(
                f"{name} (optional) version {installed_ver} < required {min_ver}",
                data={
                    "extension": name,
                    "installed": installed_ver,
                    "required": min_ver,
                },
                fix_hint=(
                    f"upgrade {name} to >= {min_ver}, or DROP "
                    f"EXTENSION {name} if not needed"
                ),
            )

    return installed


def _major(v: str) -> tuple[int, int]:
    """Best-effort major/minor parse. 'a.b.c' → (a,b)."""
    parts = v.split(".", 2)
    try:
        return (int(parts[0]), int(parts[1] if len(parts) > 1 else 0))
    except ValueError:
        return (0, 0)


def _verify_server_version(conn: psycopg.Connection) -> int:
    """Confirm the running Postgres major version is at least
    `MIN_POSTGRES_MAJOR`. Returns the observed major version.

    Raises `PostgresVersionTooOld` BEFORE any DDL runs.
    """
    with conn.cursor() as cur:
        cur.execute("SHOW server_version_num")
        (version_num_str,) = cur.fetchone()
    # server_version_num packs as MMmmm (e.g. 170002 for 17.2).
    # Major = floor(version_num / 10000).
    version_num = int(version_num_str)
    major = version_num // 10000
    if major < MIN_POSTGRES_MAJOR:
        raise PostgresVersionTooOld(
            f"Postgres major version {major} is below required {MIN_POSTGRES_MAJOR}",
            data={
                "observed_major": major,
                "observed_version_num": version_num,
                "required_major": MIN_POSTGRES_MAJOR,
            },
            fix_hint=(
                f"upgrade Postgres to {MIN_POSTGRES_MAJOR} or later. "
                f"Local dev: pull the matching TimescaleDB image. "
                f"Managed: schedule the upgrade."
            ),
        )
    return major


def _schema_sql() -> str:
    """Load schema.sql as a single string.

    Uses importlib.resources so the SQL ships with the installed
    package (package-data pattern in pyproject.toml covers *.sql).
    """
    return (
        importlib.resources.files("okg.substrate.db").joinpath("schema.sql").read_text()
    )


def _views_sql() -> str:
    """Load views.sql as a single string.

    Authored per `add-mcp-run-sql-tool` (2026-04-27). Defines the
    `okg.current_pin()` function, the `app_ro` role, and the
    stable `okg.v_*` view layer the MCP `run_sql` tool exposes.
    Idempotent: every object uses `CREATE OR REPLACE` /
    `IF NOT EXISTS` / a `DO` guard.
    """
    return (
        importlib.resources.files("okg.substrate.db").joinpath("views.sql").read_text()
    )


def _deployment_views_sql_path(deployment_name: str) -> Path | None:
    """Resolve ``<deployment dir>/views.sql`` for ``deployment_name``.

    Per `add-deployment-sql-views` (2026-04-27): each deployment
    may register its own ``okg.v_<deployment>_<name>`` views by
    placing them in this file. Loaded by ``migrate`` after the
    substrate's own ``views.sql``. Idempotent (deployment authors
    use ``CREATE OR REPLACE VIEW`` like the substrate's views).

    The deployment directory comes from the same resolver every other
    deployment read uses (``find_deployment_dir``), so a deployment
    installed from a profile outside this checkout -- the user-state
    directory, ``OKG_DEPLOYMENTS_DIR``, a packaged release -- has its
    ``views.sql`` applied. This used to look only at this checkout's own
    ``deployments/<name>/``, so a profile-installed deployment's views were
    never created and nothing said so. Returns None when no deployment of
    that name resolves; there is then no deployment file to apply.
    """
    from okg.substrate.deployments import find_deployment_dir

    deployment_dir = find_deployment_dir(deployment_name)
    if deployment_dir is None:
        return None
    return deployment_dir / "views.sql"


def _statement_count(sql: str) -> int:
    return sum(
        1
        for line in sql.splitlines()
        if line.strip()
        .rstrip(";")
        .upper()
        .startswith(("CREATE", "ALTER", "GRANT", "INSERT", "SELECT"))
    )


def _load_deployment_views(
    deployment_name: str | None,
) -> tuple[str | None, Path | None]:
    if deployment_name is None:
        return None, None
    deployment_views_path = _deployment_views_sql_path(deployment_name)
    if deployment_views_path is not None and deployment_views_path.exists():
        return deployment_views_path.read_text(), deployment_views_path
    return None, deployment_views_path


def _executable_views_sql(
    conn: psycopg.Connection,
    views: str,
    *,
    include_storage: bool = True,
) -> str:
    """views.sql, as executed.

    Nothing rewrites the view DEFINITIONS any more: the Chronos-native read
    path is authored in views.sql itself (`schema-canonical-read-path`), where
    it used to be spliced in from Python at this point.

    Two things still differ from the file's text, both additive:

      * the narrow ``install_views`` refresh path cannot create the
        mcp_call_log table, so its storage DDL is dropped there;
      * ``okg.require_chronos_checkout`` is appended. That is the #706
        kept-table honest-read guard for maintained derivations
        (okg.claim_effective / claim_projection), NOT the graph read path,
        and it is defined in Python because the same text is replayed by
        ``ensure_kept_table_honest_read_view`` when a kept table registers at
        runtime. Appending it here keeps a fresh install and an upgraded
        database agreeing on the function's presence, which the
        migration-convergence lane checks.
    """
    from okg.substrate.db.management import (
        CHRONOS_KEPT_CHECKOUT_GUARD_FN_SQL,
        views_without_mcp_call_log_storage,
    )

    executable_views = views
    if not include_storage:
        executable_views = views_without_mcp_call_log_storage(executable_views)
    return executable_views + "\n\n" + CHRONOS_KEPT_CHECKOUT_GUARD_FN_SQL


def _install_views_sql(
    cur: psycopg.Cursor,
    executable_views: str,
    *,
    deployment_name: str | None,
    deployment_views_sql: str | None,
    deployment_views_path: Path | None,
) -> list[str]:
    """Install the substrate (and deployment) view scripts.

    Wrapped in the frozen-view-shape convergence pass: a database whose
    STORED view has a column name/order/type that ``CREATE OR REPLACE
    VIEW`` is not permitted to change would otherwise abort the entire
    migrate (see ``view_convergence``). Returns the views that had to be
    dropped and recreated — empty on every fresh install and every
    already-converged database.
    """

    def _run() -> None:
        cur.execute(executable_views)
        if deployment_views_sql is None:
            return
        try:
            cur.execute(deployment_views_sql)
        except Exception as exc:
            # A frozen-view-shape failure is not a broken deployment
            # file; hand it to the convergence pass unwrapped so it is
            # recognizable there.
            if is_unreplaceable_view_error(exc):
                raise
            raise SubstrateError(
                f"deployment views.sql failed: {deployment_views_path} — {exc}",
                data={
                    "deployment_name": deployment_name,
                    "path": str(deployment_views_path),
                },
                fix_hint=(
                    "fix the SQL in the file and re-run "
                    "`okg migrate --deployment "
                    f"{deployment_name}`"
                ),
            ) from exc

    scripts = [executable_views]
    if deployment_views_sql is not None:
        scripts.append(deployment_views_sql)
    converged_views = install_with_view_convergence(
        cur.connection, run=_run, scripts=scripts
    )
    _reconcile_registered_grants(cur)
    return converged_views


def _reconcile_registered_grants(cur: psycopg.Cursor) -> int:
    """Apply and verify the shared relation-grant registry.

    ``views.sql`` applies the registry before the deployment view script runs.
    That is too early for a deployment view the later script creates or has to
    drop and recreate.  Reconcile once more at the shared end of view
    installation so full migrate and ``--views-only`` have the same ordering.

    The applier may queue one denied/contended statement and still return a
    positive count after applying other statements.  A migration must not turn
    that partial result into success: require both the registry's direct-grant
    drift check and its durable retry queue to be empty before the surrounding
    transaction can commit.
    """
    cur.execute(
        """
        SELECT to_regprocedure('okg.apply_grant_registry()') IS NOT NULL,
               to_regprocedure('okg.grant_registry_drift()') IS NOT NULL,
               to_regclass('okg.pending_grants') IS NOT NULL
        """
    )
    applier_exists, drift_check_exists, queue_exists = cur.fetchone()
    missing_registry_objects: list[str] = []
    if not applier_exists:
        missing_registry_objects.append("okg.apply_grant_registry()")
    if not drift_check_exists:
        missing_registry_objects.append("okg.grant_registry_drift()")
    if not queue_exists:
        missing_registry_objects.append("okg.pending_grants")
    if missing_registry_objects:
        raise SubstrateError(
            "grant registry reconciliation is unavailable after view installation",
            data={"missing_registry_objects": missing_registry_objects},
            fix_hint=(
                "repair the shared grant-registry installation, then re-run "
                "`okg migrate` as the database owner"
            ),
        )

    try:
        cur.execute("SELECT okg.apply_grant_registry()")
        applied = int(cur.fetchone()[0])
    except psycopg.Error as exc:
        raise SubstrateError(
            "view installation could not restore the registered relation grants",
            data={"postgres_error": str(exc), "sqlstate": exc.sqlstate},
            fix_hint=(
                "re-run migrate as the database owner; the failed transaction "
                "was rolled back and did not leave a partially restored read surface"
            ),
        ) from exc

    cur.execute(
        """
        SELECT relation, role_name, detail
          FROM okg.grant_registry_drift()
         WHERE finding_kind = 'missing_grant'
         ORDER BY relation, role_name, detail
        """
    )
    missing_grants = [
        {"relation": row[0], "role_name": row[1], "detail": row[2]}
        for row in cur.fetchall()
    ]

    cur.execute(
        """
        SELECT statement, attempts, last_error
          FROM okg.pending_grants
         ORDER BY statement
        """
    )
    pending_grants = [
        {"statement": row[0], "attempts": row[1], "last_error": row[2]}
        for row in cur.fetchall()
    ]

    if missing_grants or pending_grants:
        raise SubstrateError(
            "view installation left registered relation grants unapplied; "
            "the migration was rolled back",
            data={
                "applied_count": applied,
                "missing_grants": missing_grants,
                "pending_grants": pending_grants,
            },
            fix_hint=(
                "resolve the named ownership, permission, or catalog-contention "
                "failure and re-run migrate; do not grant the relations by hand"
            ),
        )
    return applied


#: The one generated view the Chronos metadata migrations must see gone.
_CHRONOS_CHECKPOINT_DEPENDENT_VIEW = ("okg", "v_generations")

#: The drop and its undo, with the relation spelled out rather than composed.
#: `scripts/check_migration_drop_recreate.py` reads the relation a runtime
#: CREATE targets so it can tell whether a migration drops it by static name;
#: a `{}` placeholder makes that unknowable and it refuses, correctly. The
#: pair is a literal for the scanner, not for PostgreSQL's benefit.
_DROP_CHRONOS_CHECKPOINT_VIEW_SQL = "DROP VIEW IF EXISTS okg.v_generations CASCADE"
_RESTORE_CHRONOS_CHECKPOINT_VIEW_SQL = (
    "CREATE OR REPLACE VIEW okg.v_generations AS {definition}"
)


def _drop_chronos_checkpoint_dependent_views(
    cur: psycopg.Cursor,
) -> dict[str, Any] | None:
    """Drop generated OKG views that block Chronos metadata migrations.

    The interval backend upgrades legacy text segment ids by rebuilding
    ``_chronos_branch_interval_checkpoints``. Existing deployments may already
    have the Chronos-native ``okg.v_generations`` view installed over that table,
    so the table rebuild fails unless OKG removes its generated view first. The
    normal view installation later in ``migrate()`` recreates it.

    ...except in a pass that DEFERS the view install behind a pending
    multi-phase migration, which would otherwise commit the drop and leave the
    view missing for the whole driver run — `okg.v_generations` is the MCP
    sidecar's startup probe and is read by ``okg.source_frontier_state()``,
    whose plpgsql body PostgreSQL tracks no dependency for. So the definition
    and its grants are captured here and handed back, for
    :func:`_restore_chronos_checkpoint_view` to put back exactly as they were.
    """
    schema, name = _CHRONOS_CHECKPOINT_DEPENDENT_VIEW
    captured = _capture_view(cur, schema, name)
    cur.execute(_DROP_CHRONOS_CHECKPOINT_VIEW_SQL)
    return captured


def _capture_view(
    cur: psycopg.Cursor, schema: str, name: str
) -> dict[str, Any] | None:
    """The definition and table-level grants of a stored view, or None.

    Table-level ACLs only: neither this view nor any other the substrate
    scripts declare carries a column-level grant or a COMMENT, and inventing
    partial-restore machinery for privileges nothing issues would be more
    surface than the recovery is worth. If that changes, this is the place a
    restore would silently lose them, so the restriction is stated rather
    than assumed.
    """
    cur.execute(
        """
        SELECT pg_get_viewdef(c.oid, true)
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = %s AND c.relname = %s AND c.relkind = 'v'
        """,
        (schema, name),
    )
    row = cur.fetchone()
    if row is None:
        return None
    cur.execute(
        """
        SELECT a.grantee::regrole::text, a.privilege_type, a.is_grantable
          FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
          CROSS JOIN aclexplode(c.relacl) AS a
         WHERE n.nspname = %s AND c.relname = %s
        """,
        (schema, name),
    )
    return {
        "schema": schema,
        "name": name,
        "definition": row[0],
        "grants": [tuple(grant) for grant in cur.fetchall()],
    }


def _restore_chronos_checkpoint_view(
    cur: psycopg.Cursor, captured: dict[str, Any] | None
) -> None:
    """Re-create the view captured above, grants and all."""
    if captured is None:
        return
    relation = pgsql.Identifier(captured["schema"], captured["name"])
    cur.execute(
        pgsql.SQL(_RESTORE_CHRONOS_CHECKPOINT_VIEW_SQL).format(
            definition=pgsql.SQL(captured["definition"])
        )
    )
    for grantee, privilege, is_grantable in captured["grants"]:
        # `-` is aclexplode's spelling of PUBLIC; regrole renders every other
        # grantee as a quotable role name.
        target = (
            pgsql.SQL("PUBLIC")
            if grantee == "-"
            else pgsql.Identifier(grantee)
        )
        statement = pgsql.SQL("GRANT {} ON {} TO {}").format(
            pgsql.SQL(privilege), relation, target
        )
        if is_grantable:
            statement = pgsql.SQL("{} WITH GRANT OPTION").format(statement)
        cur.execute(statement)


def _guarded_poros_generation_id_text_migration() -> str:
    """Catalog-type-guarded ``poros_*`` generation-id → TEXT cutover.

    The Chronos cutover retypes ``poros_outbox_messages.sender_generation_id``
    and ``poros_incorporation_events.receiver_generation_id`` to TEXT (and drops
    their legacy FKs). A bare ``ALTER COLUMN ... TYPE TEXT`` takes ACCESS
    EXCLUSIVE and rewrites the table on every migrate even when the column is
    already TEXT. This DO block only performs the ALTERs when the column is not
    already ``text``, so a converged deployment on the always-run path (the
    checksum-skip fast path) acquires no strong lock and does no rewrite.
    """
    return """
    DO $okg_poros_generation_id_text$
    BEGIN
      IF EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_schema = 'okg'
           AND table_name = 'poros_outbox_messages'
           AND column_name = 'sender_generation_id'
           AND data_type <> 'text'
      ) THEN
        ALTER TABLE okg.poros_outbox_messages
          DROP CONSTRAINT IF EXISTS
            poros_outbox_messages_sender_generation_id_fkey;
        ALTER TABLE okg.poros_outbox_messages
          ALTER COLUMN sender_generation_id TYPE TEXT
          USING sender_generation_id::text;
      END IF;
      IF EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_schema = 'okg'
           AND table_name = 'poros_incorporation_events'
           AND column_name = 'receiver_generation_id'
           AND data_type <> 'text'
      ) THEN
        ALTER TABLE okg.poros_incorporation_events
          DROP CONSTRAINT IF EXISTS
            poros_incorporation_events_receiver_generation_id_fkey;
        ALTER TABLE okg.poros_incorporation_events
          ALTER COLUMN receiver_generation_id TYPE TEXT
          USING receiver_generation_id::text;
      END IF;
    END
    $okg_poros_generation_id_text$
    """


#: Every role the migration creates when it is absent. `_ensure_schema_roles`
#: below creates the first six; `views.sql` creates `app_ro`, `okg_mcp`,
#: `okg_external_action_api`, `okg_external_action_function_owner` and
#: `okg_connection_reservation_owner` under the same `IF NOT EXISTS` guard.
#: Creating any one of them needs the connecting role to hold CREATEROLE (or
#: to be a superuser).
#:
#: The installer probe in the academic-researcher profile reads this list so the
#: privilege question it asks before writing anything stays the question the
#: migration actually answers. `tests/substrate/cli/test_install_postgres_dsn.py`
#: (`test_the_schema_role_list_matches_the_sql_that_creates_those_roles`)
#: fails when a role appears in the SQL and not here.
SCHEMA_ROLES: tuple[str, ...] = (
    "app_rw",
    "app_ro",
    "catalog_admin",
    "projection_remediator",
    "okg_external_action_api",
    "okg_external_action_function_owner",
    "okg_connection_reservation_owner",
    "okg_mcp",
)


#: The subset the migration GRANTs to another role or reconfigures with
#: ALTER ROLE. On PostgreSQL 16 and later, CREATEROLE alone does NOT carry
#: either right over a role the connecting role did not create, so an
#: install against a cluster where these roles already exist needs ADMIN
#: OPTION on each of them -- `GRANT app_rw, catalog_admin TO
#: projection_remediator` in `schema.sql`, `GRANT app_ro TO <current_user>`
#: and `ALTER ROLE okg_mcp SET plan_cache_mode` in `views.sql`, and the
#: `ALTER ROLE` converge blocks for the two external-action roles and the
#: connection-reservation function owner.
#:
#: Today that is every name in SCHEMA_ROLES; the two lists are kept separate
#: because a role that is only a grant TARGET (one that merely receives
#: privileges ON objects) would need neither.
SCHEMA_ROLES_NEEDING_ADMIN_OPTION: tuple[str, ...] = SCHEMA_ROLES


#: The NOLOGIN role that owns every ``okg.external_action_*`` SECURITY
#: DEFINER function. ``schema.sql``, ``views.sql`` and migrations 0030 and
#: 0033 hand those functions to it with ``ALTER FUNCTION ... OWNER TO``.
EXTERNAL_ACTION_FUNCTION_OWNER = "okg_external_action_function_owner"


def _open_function_owner_transfer(cur: psycopg.Cursor) -> None:
    """Let the migration hand functions in schema ``okg`` to their owner role.

    PostgreSQL refuses ``ALTER FUNCTION ... OWNER TO r`` unless ``r`` holds
    CREATE on the function's schema (issue #2239). A superuser skips that
    check, so the Docker route never saw it; a non-superuser migration role
    died at the first transfer with ``permission denied for schema okg``.

    The owner role keeps only USAGE on ``okg`` at rest: a SECURITY DEFINER
    body that could create objects in the schema its callers resolve names
    in is exactly the privilege the separate owner role exists to withhold.
    So the grant is made inside the migrate transaction and
    :func:`_close_function_owner_transfer` revokes it before the single
    commit. No other session ever observes it, and a failed migration rolls
    it back with everything else. The role is NOLOGIN and NOINHERIT, and no
    migration statement calls a SECURITY DEFINER helper at top level. The
    trigger functions this role owns on the external-action tables would run
    inside the window if a migration wrote rows to those tables; none does
    today.
    """
    cur.execute(
        pgsql.SQL("GRANT CREATE ON SCHEMA okg TO {}").format(
            pgsql.Identifier(EXTERNAL_ACTION_FUNCTION_OWNER)
        )
    )
    # ALTER FUNCTION ... OWNER TO also needs the migrating role to be able to
    # SET ROLE to the owner, and every later CREATE OR REPLACE of an owned
    # function needs it to inherit the owner's rights. On PostgreSQL 16 and
    # later a non-superuser that CREATES the owner (a brand-new cluster, via
    # _ensure_schema_roles) gets only ADMIN OPTION on it, which carries
    # neither. Grant both back to the migrating role, once. A superuser needs
    # nothing; a role that already has both is left alone. This is a durable
    # membership of the migrating role in a NOLOGIN role that owns only what
    # that same migrating role installs.
    cur.execute(
        pgsql.SQL(
            """
            DO $okg_function_owner_self_grant$
            BEGIN
              IF NOT (SELECT rolsuper FROM pg_catalog.pg_roles
                       WHERE rolname = current_user)
                 AND NOT (pg_has_role(current_user, {owner}, 'SET')
                          AND pg_has_role(current_user, {owner}, 'USAGE')) THEN
                GRANT {role} TO CURRENT_USER WITH INHERIT TRUE, SET TRUE;
              END IF;
            END
            $okg_function_owner_self_grant$
            """
        ).format(
            owner=pgsql.Literal(EXTERNAL_ACTION_FUNCTION_OWNER),
            role=pgsql.Identifier(EXTERNAL_ACTION_FUNCTION_OWNER),
        )
    )


def _close_function_owner_transfer(cur: psycopg.Cursor) -> None:
    """Revoke the CREATE grant made by :func:`_open_function_owner_transfer`.

    Unconditional, so it also removes a CREATE grant on ``okg`` that the
    migrating role or the schema owner made earlier, deliberately or not.
    PostgreSQL's REVOKE removes only grants whose grantor is the revoking
    role (or the schema owner, when the owner or a superuser revokes), so a
    CREATE grant made by some third role survives this statement.
    """
    cur.execute(
        pgsql.SQL("REVOKE CREATE ON SCHEMA okg FROM {}").format(
            pgsql.Identifier(EXTERNAL_ACTION_FUNCTION_OWNER)
        )
    )


def _ensure_schema_roles(cur: psycopg.Cursor) -> None:
    """Create grant targets required by schema.sql on managed hosts.

    Local compose initdb creates these roles before migration, but restored or
    managed clusters do not necessarily run that initdb. Keep fresh roles
    NOLOGIN so migration never invents credentials; operators can configure
    LOGIN and passwords separately when direct role authentication is needed.
    Existing roles and their attributes are left unchanged.
    """
    cur.execute(
        """
        DO $okg_schema_roles$
        BEGIN
          IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_rw') THEN
            BEGIN
              CREATE ROLE app_rw NOLOGIN;
            EXCEPTION
              WHEN duplicate_object OR unique_violation THEN NULL;
            END;
          END IF;
          IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_ro') THEN
            BEGIN
              CREATE ROLE app_ro NOLOGIN;
            EXCEPTION
              WHEN duplicate_object OR unique_violation THEN NULL;
            END;
          END IF;
          IF NOT EXISTS (
            SELECT 1 FROM pg_roles WHERE rolname = 'catalog_admin'
          ) THEN
            BEGIN
              CREATE ROLE catalog_admin NOLOGIN;
            EXCEPTION
              WHEN duplicate_object OR unique_violation THEN NULL;
            END;
          END IF;
          IF NOT EXISTS (
            SELECT 1 FROM pg_roles WHERE rolname = 'projection_remediator'
          ) THEN
            BEGIN
              CREATE ROLE projection_remediator NOLOGIN;
            EXCEPTION
              WHEN duplicate_object OR unique_violation THEN NULL;
            END;
          END IF;
          IF NOT EXISTS (
            SELECT 1 FROM pg_roles WHERE rolname = 'okg_external_action_api'
          ) THEN
            BEGIN
              CREATE ROLE okg_external_action_api
                NOLOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE INHERIT;
            EXCEPTION
              WHEN duplicate_object OR unique_violation THEN NULL;
            END;
          END IF;
          IF NOT EXISTS (
            SELECT 1 FROM pg_roles
             WHERE rolname = 'okg_external_action_function_owner'
          ) THEN
            BEGIN
              CREATE ROLE okg_external_action_function_owner
                NOLOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOINHERIT;
            EXCEPTION
              WHEN duplicate_object OR unique_violation THEN NULL;
            END;
          END IF;
        END
        $okg_schema_roles$
        """
    )


def _database_from_dsn(dsn: str) -> str | None:
    """Resolve the DSN's target database via psycopg conninfo parsing.

    Handles BOTH URL (``postgres://.../db``) and key=value
    (``dbname=db host=...``) forms identically — the audit F5 bypass was
    urlparse extracting no dbname from conninfo DSNs, silently skipping the
    authority gate. When the DSN omits dbname, libpq's resolution order is
    mirrored (``PGDATABASE``, then the user name) so a gated database cannot
    be reached ungated through an implicit default.

    A DSN that fails to parse, or one whose target database cannot be
    resolved without machinery this gate does not mirror (``service=``
    definitions in pg_service.conf), raises
    :class:`MigrateAuthorityUnresolvable` (fail closed) — never ``None``.
    """
    from okg.substrate.db import dsn as dsn_mod

    failure: str | None = None
    info: dict[str, str] = {}
    try:
        info = dict(dsn_mod.parse(dsn).params)
    except dsn_mod.DsnParseError as exc:
        # The shared parser's message keeps libpq's reason but replaces every
        # quoted fragment, which for `password=my secret` is part of the
        # password. libpq's own text never reaches this error or its chain.
        failure = str(exc)
    if failure is not None:
        raise MigrateAuthorityUnresolvable(
            "cannot resolve the target database for the authority gate: "
            f"DSN failed conninfo parsing: {failure}",
            data={"detail": failure},
            fix_hint="fix the DSN; the gate fails closed on unparseable DSNs",
        )
    if not info.get("dbname") and (
        info.get("service") or os.environ.get("PGSERVICE")
    ):
        # A pg_service.conf lookup could name ANY database, including a
        # gated one; this gate does not read service files, so it refuses
        # rather than guessing (fail closed, audit F5 shape).
        raise MigrateAuthorityUnresolvable(
            "cannot resolve the target database for the authority gate: the "
            "DSN relies on a pg_service.conf service definition",
            data={"detail": "service DSN without explicit dbname"},
            fix_hint=(
                "pass dbname explicitly in the DSN when migrating; the gate "
                "does not expand service definitions"
            ),
        )
    database = (
        info.get("dbname")
        or os.environ.get("PGDATABASE")
        or info.get("user")
        or os.environ.get("PGUSER")
        or _os_login_name()
    )
    return str(database) if database else None


def _os_login_name() -> str | None:
    """The OS user name — libpq's last resort for both user and dbname."""
    import getpass

    try:
        return getpass.getuser()
    except Exception:  # noqa: BLE001 — no resolvable login name on this host
        return None


def _deployment_for_database(dsn: str | None) -> str | None:
    """Reverse lookup: the deployment whose manifest declares the DSN's
    target database.

    Lets a bare ``okg migrate --dsn ...`` (no ``--deployment``) still be
    authority-gated for a known canonical deployment, closing the bypass where
    dropping ``--deployment`` would otherwise skip the gate. Returns None only
    for the LEGITIMATE ungated cases: the database matches no registered
    deployment (an unattributable / clone DB), or no deployments root is
    configured at all (a fresh external install).

    Fail-closed (`schema-migration-hardening` /
    req.authority-gate-conninfo): a DSN parse failure, a deployments-root
    resolution failure, or an unreadable deployment manifest raises
    :class:`MigrateAuthorityUnresolvable` — the gate never silently degrades
    to ungated on a lookup FAILURE (the audit F5
    ``except Exception: return None`` defect).
    """
    if not dsn:
        return None
    from okg.substrate.deployments import find_deployments_root

    database = _database_from_dsn(dsn)
    if not database:
        return None
    try:
        root = find_deployments_root()
    except Exception as exc:
        raise MigrateAuthorityUnresolvable(
            "cannot resolve the deployments root for the authority gate: "
            f"{exc}",
            data={"detail": str(exc)},
            fix_hint=(
                "fix OKG_DEPLOYMENTS_DIR / the deployments directory; the "
                "gate fails closed on a root read failure"
            ),
        ) from exc
    if root is None:
        # No deployments root anywhere (external install with no registered
        # deployments): nothing to gate against. This is an ABSENT root, not
        # a failed read — the only lookup outcome that legitimately ungates.
        return None
    try:
        manifests = sorted(root.glob("*/deployment.yaml"))
    except OSError as exc:
        raise MigrateAuthorityUnresolvable(
            f"cannot enumerate deployment manifests under {root}: {exc}",
            data={"detail": str(exc), "root": str(root)},
            fix_hint="fix the deployments directory permissions/contents",
        ) from exc
    for manifest in manifests:
        try:
            raw = fastyaml.safe_load(manifest.read_text(encoding="utf-8")) or {}
        except Exception as exc:  # unreadable manifest could be the gating one
            raise MigrateAuthorityUnresolvable(
                f"cannot read deployment manifest {manifest}: {exc}",
                data={"detail": str(exc), "manifest": str(manifest)},
                fix_hint=(
                    "fix or remove the unreadable deployment.yaml; the gate "
                    "fails closed rather than skipping it"
                ),
            ) from exc
        postgres = raw.get("postgres") or {}
        declared = postgres.get("database") or raw.get("database")
        if declared and str(declared) == database:
            return manifest.parent.name
    return None


def migrate(
    dsn: str,
    *,
    deployment_name: str | None = None,
) -> MigrateResult:
    """Apply the v1 schema (see :func:`_migrate_schema`), then retire repair
    contracts frozen by the pre-#3342 incomplete-run rule, once.

    The retirement runs after the schema commit. It is idempotent and
    records its reason on each checkpoint it retires
    (``published_completeness.retire_pre_publish_incomplete_checkpoints``).
    With ``deployment_name`` it covers that deployment only; without, every
    deployment in the database. A failure there raises a
    :class:`SubstrateError` that says the schema is already migrated; a
    rerun retries the retirement.
    """
    result = _migrate_schema(dsn, deployment_name=deployment_name)
    semantic_ann_index = _ensure_semantic_ann_index(dsn, deployment_name)
    if semantic_ann_index is not None:
        result = dataclasses.replace(result, semantic_ann_index=semantic_ann_index)
    vector_sets = _ensure_vector_sets(dsn, deployment_name)
    if vector_sets is not None:
        result = dataclasses.replace(result, vector_sets=vector_sets)
    from okg.substrate.ingest.published_completeness import (
        retire_contracts_of_removed_sources,
        retire_pre_publish_incomplete_checkpoints,
    )

    try:
        retirements = retire_pre_publish_incomplete_checkpoints(
            dsn, [deployment_name] if deployment_name else None,
        )
    except Exception as exc:
        data = dict(getattr(exc, "data", None) or {})
        raise SubstrateError(
            "the schema is migrated, but retiring pre-upgrade incomplete "
            "checkpoints failed",
            data={
                **data,
                "schema_migrated": True,
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
            fix_hint=(
                "fix the cause, then rerun `okg migrate --apply`; the schema "
                "step is a no-op the second time and the retirement retries"
            ),
        ) from exc
    # A contract whose required sources were removed or renamed can never be
    # repaired. With --deployment it is retired when every safety check
    # passes; without, candidates are reported and nothing is retired (no
    # source registry to classify against). Every caller of migrate() with a
    # deployment name runs this: provision, init, promotion and the exchange
    # harness as well as `okg migrate`.
    try:
        contract_reports = retire_contracts_of_removed_sources(
            dsn, deployment_name,
        )
    except Exception as exc:
        data = dict(getattr(exc, "data", None) or {})
        raise SubstrateError(
            "the schema is migrated, but retiring the repair contract of "
            "removed or renamed sources failed",
            data={
                **data,
                "schema_migrated": True,
                "error_type": type(exc).__name__,
                "error": str(exc),
            },
            fix_hint=(
                "fix the cause, then rerun `okg migrate --deployment "
                f"{deployment_name or '<deployment>'} --apply`; the schema "
                "step is a no-op the second time and the retirement retries"
            ),
        ) from exc
    if contract_reports:
        result = dataclasses.replace(
            result, source_contract_retirements=tuple(contract_reports),
        )
    if not retirements:
        return result
    return dataclasses.replace(
        result,
        incomplete_checkpoint_retirements=tuple(retirements),
    )


def _migrate_schema(
    dsn: str,
    *,
    deployment_name: str | None = None,
) -> MigrateResult:
    """Apply the v1 schema to the target DSN.

    Fail-loud: the first extension or DDL error aborts the run.
    No partial migrate. The schema.sql is authored to be
    idempotent; a second invocation is a no-op.

    Apply-time hardening (`schema-migration-hardening`): the whole
    migrate body runs as ONE transaction under a per-database
    advisory lock with ``SET LOCAL`` lock/statement timeouts and
    bounded retry — concurrent migrates serialize (the loser no-ops
    cleanly), a held conflicting lock yields a typed
    :class:`MigrationLockTimeout` after a clean rollback, and no DDL
    commits before the single final commit that also records the
    ledger.

    When ``deployment_name`` is supplied AND
    ``deployments/<name>/views.sql`` exists, its contents are
    executed in the same migration cursor as the substrate's
    own ``views.sql``. This lets deployments register their own
    ``okg.v_*`` views without forking the substrate. Per
    ``add-deployment-sql-views``.

    A deployment view is NOT readable by ``app_ro`` because of what it
    is named (``schema-grant-coherence`` deleted that convention). It
    becomes readable by inserting a row into ``okg.grant_registry`` with
    a ``source`` other than ``'substrate'`` — the substrate seed
    replaces only its own rows, and ``okg.apply_grant_registry()``
    re-issues every registered grant after the schema-wide REVOKE.
    A ``v_``-prefixed view with no row is reported by
    ``okg.grant_registry_drift()``.

    A failure in the deployment-side SQL aborts the migration
    with the file path in the error message — no partial
    install. Re-running with no ``views.sql`` change is a no-op
    (deployment authors use the same ``CREATE OR REPLACE`` /
    ``IF NOT EXISTS`` discipline as ``views.sql``).
    """
    # Authority gate (Tier W): re-recording a deployment's schema snapshot is a
    # canonical-DB write. Mirror the publish path (cli ingest --publish) and the
    # worker-start guard so a foreign checkout cannot drift the recorded
    # 0000_schema_snapshot checksum out from under the deployment's authoritative
    # producer -- a drift that crash-loops the runtime worker on a schema-version
    # skew. Only the declared sync-root checkout, or an isolated clone DB
    # (identity isolated), may record the snapshot; a foreign checkout writing
    # the canonical DB is refused fail-loud. Deployments that declare no
    # sync_root are allowed (mode sync_root_undeclared). The okg up / provision /
    # migrate --deployment paths funnel through here with deployment_name set;
    # a bare ``okg migrate --dsn ...`` (no --deployment) is reverse-mapped to its
    # deployment by target database, so dropping --deployment cannot bypass the
    # gate. A DSN matching no registered deployment is ungated (clone / ad-hoc).
    guard_deployment = deployment_name or _deployment_for_database(dsn)
    if guard_deployment is not None:
        from okg.substrate.deployment_authority import check_publish_provenance

        provenance = check_publish_provenance(
            guard_deployment, dsn, file_derived=True,
        )
        if not provenance.get("allowed"):
            legal = "; ".join(provenance.get("legal_moves") or []) or (
                f"migrate from the deployment's sync root, or use an isolated "
                f"clone (`okg up {guard_deployment} --target dev`)"
            )
            raise SubstrateError(
                "refusing migrate: re-recording the schema snapshot for "
                f"{guard_deployment!r} from a non-authoritative checkout "
                f"(mode={provenance.get('mode')}): {provenance.get('reason')}",
                data=provenance,
                fix_hint=f"legal moves: {legal}",
            )

    sql = _schema_sql()
    views = _views_sql()
    deployment_views_sql, deployment_views_path = _load_deployment_views(
        deployment_name,
    )

    lock_timeout_ms, statement_timeout_ms, attempts = _migrate_timeout_settings()

    def _one_pass() -> MigrateResult:
        for attempt in range(1, attempts + 1):
            try:
                return _migrate_once(
                    dsn,
                    deployment_name=deployment_name,
                    guard_deployment=guard_deployment,
                    sql=sql,
                    views=views,
                    deployment_views_sql=deployment_views_sql,
                    deployment_views_path=deployment_views_path,
                    lock_timeout_ms=lock_timeout_ms,
                    statement_timeout_ms=statement_timeout_ms,
                )
            except (
                psycopg.errors.LockNotAvailable,
                psycopg.errors.QueryCanceled,
            ) as exc:
                # The transaction was rolled back by the connection context:
                # the database is at the old converged state. Bounded retry,
                # then a typed refusal — never an indefinite wedge.
                if attempt >= attempts:
                    raise MigrationLockTimeout(
                        "migrate could not acquire the locks it needs within "
                        f"lock_timeout={lock_timeout_ms}ms / "
                        f"statement_timeout={statement_timeout_ms}ms across "
                        f"{attempts} attempt(s); the migrate transaction was "
                        "rolled back cleanly",
                        data={
                            "attempts": attempts,
                            "lock_timeout_ms": lock_timeout_ms,
                            "statement_timeout_ms": statement_timeout_ms,
                            "sqlstate": exc.sqlstate,
                        },
                        fix_hint=(
                            "find and release the conflicting lock "
                            "(pg_locks/pg_stat_activity), or raise "
                            f"{MIGRATE_LOCK_TIMEOUT_ENV} / "
                            f"{MIGRATE_LOCK_ATTEMPTS_ENV} and re-run"
                        ),
                    ) from exc
                logging.getLogger(__name__).warning(
                    "migrate attempt %d/%d hit a lock/statement timeout "
                    "(sqlstate=%s); rolled back, retrying",
                    attempt,
                    attempts,
                    exc.sqlstate,
                )
                time.sleep(_MIGRATE_RETRY_BACKOFF_SECONDS * attempt)
        raise AssertionError("unreachable: migrate retry loop exited")

    # okg#3112: build new graph read indexes and drop retired ones with
    # CREATE/DROP INDEX CONCURRENTLY before the schema transaction, whose
    # reconciliation would otherwise build a missing one with a plain,
    # write-blocking CREATE INDEX. A fresh database has no graph tables yet
    # and gets its indexes from reconciliation on empty tables.
    from okg.substrate.db.cold_history import DEFERRED_INDEX_BUILDS
    from okg.substrate.declared_attr_indexes import index_ddl_preemption

    # okg#3112 step 2 (design section 4.3 rule 1): a declared-attribute index
    # build can wait ten minutes in CREATE INDEX CONCURRENTLY; the schema
    # transaction must not queue behind it. Cancel it (it is dropped on the
    # reconciler's next run and not counted as a failure), keep the
    # reconciler out while the online step runs, then hold the graph-index
    # DDL lock through the schema transaction.
    preemption_stack = ExitStack()
    preemption = preemption_stack.enter_context(
        index_ddl_preemption(dsn, actor="migrate")
    )
    try:
        graph_indexes = _converge_graph_read_indexes(dsn, lock_timeout_ms=lock_timeout_ms)
    except BaseException:
        preemption_stack.close()
        raise
    deferred_builds = DEFERRED_INDEX_BUILDS.set(
        frozenset((graph_indexes.get("pending") or {}).get("build") or ())
    )
    try:
        preemption.hold_ddl_key()
        result = _one_pass()
        if not result.pending_multiphase:
            if isinstance(result, MigrateResult):
                result = dataclasses.replace(
                    result,
                    graph_read_indexes=graph_indexes,
                    declared_attr_index_preemption=dict(preemption.report),
                )
            return result
        # Multi-phase orchestration (phase-interval-narrow-split-migration): the
        # single-pass helper defers declared multi-phase migrations and every
        # ordinal behind them. Run each registered spec through the checkpointed
        # driver (its batches commit outside migrate's transaction by design),
        # then a second ordered pass applies the deferred ordinals. This lives in
        # migrate() itself — not the CLI — so every caller (provision, init,
        # promotion, harnesses, the convergence lane) converges identically and
        # no fresh provision ships release-blocked with a pending driver run.
        multiphase_applied = _run_pending_multiphase(
            dsn, pending=result.pending_multiphase, sql=sql, views=views
        )
        converged = _one_pass()
        if converged.pending_multiphase:
            raise SubstrateError(
                "migrate did not converge: multi-phase migration(s) still "
                "pending after the driver ran",
                data={"pending_multiphase": list(converged.pending_multiphase)},
                fix_hint=(
                    "inspect okg.schema_migration_phases and the migration "
                    "ledger; the driver completed without recording, or a new "
                    "multi-phase migration became pending mid-run"
                ),
            )
        return MigrateResult(
            statements_executed=converged.statements_executed,
            statements_skipped=converged.statements_skipped,
            extension_versions=converged.extension_versions,
            idempotent_second_run=converged.idempotent_second_run,
            converged_views=tuple(
                dict.fromkeys((*result.converged_views, *converged.converged_views))
            ),
            pending_multiphase=(),
            deferred_post_multiphase=result.deferred_post_multiphase,
            multiphase_applied=multiphase_applied,
            graph_read_indexes=graph_indexes,
            declared_attr_index_preemption=dict(preemption.report),
        )
    finally:
        DEFERRED_INDEX_BUILDS.reset(deferred_builds)
        preemption_stack.close()


def _converge_graph_read_indexes(dsn: str, *, lock_timeout_ms: int) -> dict[str, Any]:
    """Run the online graph read-index step (okg#3112) and log what it did.

    A lock wait past ``lock_timeout_ms`` (a long publish, say) or another
    index change holding the DDL lock (a concurrent migrate) does not fail
    the migrate: the step reports ``status: deferred`` with what is still
    ``pending``, the rest of the migrate runs, and schema reconciliation
    skips the pending builds (:data:`DEFERRED_INDEX_BUILDS`) instead of
    building them with a write-blocking CREATE INDEX. The next migrate, or
    the post-publish index rebuild, finishes the job; only a migrate builds
    an extended (non-B-tree) spec such as the node attrs GIN index. A failed
    extended-spec build is reported as ``status: partial`` with the spec
    under ``failed`` and ``pending``, and the migrate continues. Any other
    database error stops the migrate before its schema transaction.
    """
    from okg.substrate.db.cold_history import converge_interval_read_indexes

    log = logging.getLogger(__name__)
    try:
        report = converge_interval_read_indexes(
            dsn,
            lock_timeout_ms=lock_timeout_ms,
            on_progress=lambda msg: log.info("migrate: %s", msg),
        )
    except psycopg.Error as exc:
        raise SubstrateError(
            "migrate could not converge the graph read indexes online",
            data={
                "error": f"{type(exc).__name__}: {exc}",
                "sqlstate": getattr(exc, "sqlstate", None),
            },
            fix_hint="fix the reported database error, then rerun `okg migrate`",
        ) from exc
    if report.get("status") == "deferred":
        log.warning(
            "migrate: graph read index changes deferred (%s): pending builds=%s "
            "drops=%s; the next migrate or the post-publish index rebuild "
            "finishes them",
            report.get("reason"),
            (report.get("pending") or {}).get("build"),
            (report.get("pending") or {}).get("drop"),
        )
    elif report.get("created") or report.get("dropped") or report.get("retired_hot_indexes"):
        log.info(
            "migrate: graph read indexes converged: created=%s dropped=%s "
            "retired_hot=%s dropped_bytes=%s",
            [item["index_name"] for item in report.get("created", [])],
            [item["index_name"] for item in report.get("dropped", [])],
            [item["hot_index_name"] for item in report.get("retired_hot_indexes", [])],
            report.get("dropped_bytes"),
        )
    return report


def _run_pending_multiphase(
    dsn: str,
    *,
    pending: tuple[str, ...],
    sql: str,
    views: str,
) -> tuple[dict[str, Any], ...]:
    """Run every pending declared multi-phase migration through the driver.

    NNNN order is the pending tuple's order (the ordered runner emitted it
    in enumeration order). A pending multi-phase migration with NO
    registered spec is a typed refusal — leaving it pending would strand
    the deferred ordinals behind the barrier forever, silently.
    """
    from okg.substrate.db.management import current_known_migrations
    from okg.substrate.db.multiphase import (
        registered_multiphase_specs,
        run_multiphase_migration,
    )

    specs = registered_multiphase_specs()
    known = {
        migration.migration_id: migration
        for migration in current_known_migrations(schema_sql=sql, views_sql=views)
    }
    lock_timeout_ms, _statement_timeout_ms, _attempts = (
        _migrate_timeout_settings()
    )
    applied: list[dict[str, Any]] = []
    for migration_id in pending:
        spec = specs.get(migration_id)
        if spec is None:
            raise SubstrateError(
                f"multi-phase migration {migration_id!r} is pending but has "
                "no registered phase spec",
                data={"migration_id": migration_id},
                fix_hint=(
                    "register the spec in okg.substrate.db.multiphase."
                    "registered_multiphase_specs (or fix the manifest "
                    "class declaration)"
                ),
            )
        migration = known.get(migration_id)
        if migration is None:
            raise SubstrateError(
                f"multi-phase migration {migration_id!r} has no known "
                "migration identity",
                data={"migration_id": migration_id},
                fix_hint="the migrations directory and manifest disagree",
            )
        # The driver's advisory-lock acquisition is non-blocking by design.
        # A concurrent migrate() may be inside its own driver run for the
        # same migration; wait it out within the migrate lock budget — the
        # winner's ledger row turns our run into `already_applied` — and
        # only surface the typed refusal once the budget is exhausted
        # (concurrent migrates must serialize, not fail).
        deadline = time.monotonic() + lock_timeout_ms / 1000.0
        while True:
            try:
                outcome = run_multiphase_migration(dsn, migration, spec)
                break
            except MigrationLockTimeout:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(1.0)
        logging.getLogger(__name__).info(
            "migrate: multi-phase %s %s (phases: %s)",
            migration_id,
            outcome.get("status"),
            outcome.get("phases"),
        )
        applied.append(outcome)
    return tuple(applied)


def _migrate_once(
    dsn: str,
    *,
    deployment_name: str | None,
    guard_deployment: str | None,
    sql: str,
    views: str,
    deployment_views_sql: str | None,
    deployment_views_path: Path | None,
    lock_timeout_ms: int,
    statement_timeout_ms: int,
) -> MigrateResult:
    """One serialized, time-bounded, single-commit migrate transaction."""
    with db_connect(dsn) as conn, _ensure_caches_rollback_with(conn):
        # Guards first (`schema-migration-hardening`): bound the transaction
        # and serialize on the per-database advisory migration lock BEFORE
        # anything else runs — even the read-only version/extension probes sit
        # inside the timeout envelope, and no DDL or ledger read can precede
        # the lock, so a concurrent migrate observes either nothing or the
        # winner's committed result — never a torn middle.
        _begin_guarded_migrate_transaction(
            conn,
            lock_timeout_ms=lock_timeout_ms,
            statement_timeout_ms=statement_timeout_ms,
        )
        _verify_server_version(conn)
        ext_versions = _verify_extensions(conn)
        from okg.substrate.db.management import (
            REMOVE_JOB_RUNTIME_SUBSYSTEM_MIGRATION_ID,
            SCHEMA_SNAPSHOT_MIGRATION_ID,
            applied_executable_schema_checksum,
            current_known_migrations,
            ensure_schema_migrations,
            executable_schema_checksum,
            job_runtime_subsystem_present,
            record_known_migration,
            record_schema_snapshot,
            retire_legacy_graph_storage,
            run_pending_ordered_migrations,
            schema_snapshot_checksum,
        )
        from okg.substrate.db.migration_postcondition import evaluate_postcondition

        ensure_schema_migrations(conn)
        schema_checksum = schema_snapshot_checksum(sql, views)
        # schema.sql AS WRITTEN. `schema-canonical-read-path` deleted the
        # legacy live-table complex from the file, so the
        # `schema_without_legacy_live_table_compatibility` transform that used
        # to stand here — stripping the DDL migrate must not execute — has
        # nothing left to strip and is gone. The variable keeps its name: the
        # checksum key below is defined as "what cur.execute runs", and that
        # definition survives the transform's removal.
        executable_sql = sql
        # Checksum fast path (make-migrate-idempotent, design ruling 1): the
        # skip key is the checksum of the executable schema body
        # only. When the applied key matches, ``cur.execute(executable_sql)`` is
        # NOT replayed; everything else (compat checks, Chronos ensures, the
        # ordered runner, views, schema-role existence, owner claim) still runs.
        # Absence of the recorded key (fresh DB, or a legacy row that predates
        # the field) replays — fail-closed.
        # Schema-body GRANT/REVOKE statements remain skipped with the body.
        # Registered relation grants are different: the complete view-install
        # boundary reapplies and verifies ``okg.grant_registry`` on every run,
        # including checksum skips. Grants outside that registry are not
        # repaired by migrate. The lever to force a full schema-body replay is
        # to overwrite the recorded ``executable_schema_checksum`` in the 0000
        # ledger row's ``result_metadata`` so the skip fails closed; there is
        # no ``--force-replay`` CLI flag.
        # On a skip the 0000 ledger row's ``applied_at`` is NOT bumped: ledger
        # (applied_at) mutation <=> body execution. The joint schema+views
        # ``checksum`` column is still refreshed in place on a views-only skip
        # (finding 1) so ``migration_status`` does not go stale.
        computed_executable_checksum = executable_schema_checksum(executable_sql)
        applied_executable_checksum = applied_executable_schema_checksum(conn)
        skip_schema_body = (
            applied_executable_checksum is not None
            and applied_executable_checksum == computed_executable_checksum
        )
        executable_views = _executable_views_sql(conn, views)
        # Chronos is the only graph storage (design §2.1): the agent-facing
        # okg.v_* views bind to okg.graph_nodes / okg.graph_edges, which is how
        # views.sql itself defines them. An upgraded database still carrying
        # the legacy okg.nodes_live / okg.edges_live tables has them dropped
        # below, AFTER the views above are rebound off them.

        # Execute the whole schema as ONE multi-statement query. Per
        # psycopg 3 docs, execute() with a semicolon-delimited
        # script runs each statement sequentially in an implicit
        # transaction block. SELECT statements (create_hypertable,
        # add_*_policy) need their results discarded but do not
        # raise.
        with conn.cursor() as cur:
            _ensure_schema_roles(cur)
            # The owner-transfer grant needs schema okg to exist; schema.sql
            # opens with this same statement, so a fresh database gets the
            # schema it was about to get, only earlier in this transaction.
            cur.execute("CREATE SCHEMA IF NOT EXISTS okg")
            _open_function_owner_transfer(cur)
            if not skip_schema_body:
                cur.execute(executable_sql)
            # The Chronos graph tables (okg.graph_nodes / graph_edges) are the
            # sole graph store; ensure them before the views (which now read them).
            from okg.substrate.graph_store import ChronosGraphStore

            captured_checkpoint_view = _drop_chronos_checkpoint_dependent_views(
                cur
            )
            # Single-commit contract (audit F1): the Chronos ensure helpers
            # commit internally when self-healing at runtime, which under
            # migrate() would make DDL durable before the ledger row exists.
            # Hand them a commit-suppressed proxy of THIS connection so their
            # work joins migrate's one transaction and commits (or rolls
            # back) atomically with the ledger below.
            ensure_conn = _CommitSuppressedConnection(conn)
            graph_store = ChronosGraphStore.from_connection(ensure_conn)
            graph_store.ensure_schema()
            graph_store.ensure_graph_column_upgrade()
            # ensure_schema early-returns on an already-registered DB (so a
            # read-only role can attach); explicitly run the interval
            # storage-column upgrade so a deployment provisioned by an older,
            # integer-`deleted` Chronos is migrated (writer_segment_id +
            # deleted->boolean). Idempotent; migrate holds write privileges.
            graph_store.ensure_interval_storage_upgrade()
            # The deployment's edge ranking (item 92a): the graph writer reads
            # it from okg.edge_precedence, which only a deployment-aware path
            # can fill. Same transaction as the rest of migrate.
            if deployment_name:
                from okg.substrate.graph_store_internal.edge_declarers import (
                    apply_deployment_edge_precedence,
                )

                # The purge opens Chronos transactions; a store with
                # autocommit off joins migrate's open transaction instead of
                # trying to begin one inside it (refused since the nested-BEGIN
                # fix, lane C 238a) or committing through the suppressor.
                precedence_store = ChronosGraphStore.from_connection(
                    ensure_conn, ensure_schema=False, autocommit=False,
                )
                precedence = apply_deployment_edge_precedence(
                    cur, precedence_store, deployment_name,
                )
                if precedence is not None and (
                    precedence.get("changed")
                    or precedence.get("declarations_purged")
                ):
                    logging.getLogger(__name__).info(
                        "migrate: edge_precedence for %s ranks %s (was %s); "
                        "%s declarations the ranking does not cover were "
                        "removed; other ranked edges already stored are "
                        "recomputed when their producers next write them",
                        deployment_name,
                        precedence.get("edge_types"),
                        precedence.get("previous_edge_types"),
                        precedence.get("declarations_purged", 0),
                    )
            # A rollback swallowed inside the ensures would have silently
            # discarded the uncommitted schema body above; refuse to proceed
            # toward the single commit if one was observed.
            ensure_conn.assert_no_swallowed_rollback()
            # Generic ordered-migration runner (make-migrate-idempotent, design
            # ruling 2): after the Chronos ensures and BEFORE views install, so
            # views bind to the post-migration shape. Runs every pending
            # non-destructive NNNN migration exactly once, in order, recorded in
            # this same transaction; destructive migrations stay behind their
            # reviewed adapters. This clears the permanently-pending
            # 0003_deployment_promotion_fence that otherwise blocks release.
            runner_outcome = run_pending_ordered_migrations(
                conn,
                schema_sql=sql,
                views_sql=views,
            )
            if runner_outcome.get("pending_multiphase"):
                # Declared multi-phase migrations are excluded from this
                # single transaction BY DESIGN — but a green migrate must
                # never read as "they ran". Loud here, and durable in
                # migration_status/doctor (multiphase_migration_pending).
                logging.getLogger(__name__).warning(
                    "migrate: %d declared multi-phase migration(s) "
                    "deferred out of this transaction by design: %s — "
                    "this same migrate invocation runs them next through "
                    "the checkpointed multi-phase driver and then reapplies "
                    "the ordinals behind them; they stay pending only if "
                    "this invocation does not finish, in which case re-run "
                    "okg migrate --deployment <name> --apply",
                    len(runner_outcome["pending_multiphase"]),
                    [
                        row["migration_id"]
                        for row in runner_outcome["pending_multiphase"]
                    ],
                )
            # Per `add-mcp-run-sql-tool` (2026-04-27): install the
            # agent-facing view layer after the physical schema. The v_* views
            # were re-pointed at okg.graph_nodes/graph_edges above.
            #
            # ...unless the ordered runner deferred ordinals behind a pending
            # declared multi-phase migration
            # (phase-interval-narrow-split-migration). views.sql describes the
            # read layer of a schema with EVERY migration applied, so against a
            # prefix of that schema it is not merely premature, it is
            # unreachable: 0024 retires okg.v_changes, whose stored definition
            # depends on okg.v_nodes, whose HEAD column shape CREATE OR REPLACE
            # VIEW cannot reach — and the convergence pass rightly refuses to
            # drop a dependant the scripts no longer declare. Installing the
            # HEAD view layer here is the same out-of-NNNN-order execution the
            # runner's barrier exists to prevent; views.sql is in effect the
            # newest ordinal. Defer it (with the legacy-storage retirement that
            # requires it — see
            # management._assert_graph_read_path_is_chronos_backed) to the
            # post-driver pass, which runs with nothing deferred. The stored
            # views are left exactly as they were: nothing is dropped, so a
            # deployment mid-upgrade keeps the read layer it had.
            #
            # The predicate is `pending_multiphase`, not
            # `deferred_post_multiphase`: a multi-phase migration that is the
            # LAST ordinal defers nothing behind it, yet its own ensure_infra
            # phase DDL has still not run, so HEAD's views may reference a
            # shape that is not there either. Pending at all ⇒ wait.
            defer_read_layer = bool(runner_outcome.get("pending_multiphase"))
            converged_views: list[str] = []
            if defer_read_layer:
                # Leave the read layer as this pass found it. The Chronos
                # checkpoint rebuild above had to drop okg.v_generations;
                # since nothing recreates it in this pass, put it back
                # verbatim rather than commit a hole that lasts as long as
                # the driver run.
                _restore_chronos_checkpoint_view(cur, captured_checkpoint_view)
                logging.getLogger(__name__).info(
                    "view installation and legacy graph-storage retirement "
                    "deferred to the post-multiphase migrate pass"
                )
            else:
                # The return value is NOT discardable: it names the views the
                # frozen-shape convergence had to recreate. The shared install
                # boundary restores and verifies their registered grants.
                converged_views = _install_views_sql(
                    cur,
                    executable_views,
                    deployment_name=deployment_name,
                    deployment_views_sql=deployment_views_sql,
                    deployment_views_path=deployment_views_path,
                )
            # Chronos cutover (issue #347). schema.sql no longer contains the
            # narrowings-trigger section, so on a dirty/legacy DB the snapshot
            # does not drop the legacy edge_facts_narrowing_check trigger.
            # Drop it explicitly here before retiring the tables (a fresh DB
            # never created it, so this is a no-op there).
            cur.execute(
                "DROP TRIGGER IF EXISTS edge_facts_narrowing_check "
                "ON okg.edge_facts"
            )
            # Drop the legacy bitemporal graph tables only after the read views
            # are bound to the Chronos graph. The helper uses a restrictive drop,
            # so any unclassified dependency blocks instead of being silently
            # removed by CASCADE (#364). When the read-layer install was
            # deferred above, this pass cannot establish that precondition, so
            # retirement rides along to the post-driver pass. On a database
            # that no longer carries the legacy complex the helper would have
            # been a no-op either way — it refuses by name only when a view
            # still reads okg.nodes_live/edges_live, which is exactly the
            # pre-cutover case deferring protects.
            if not defer_read_layer:
                retire_legacy_graph_storage(conn)
            cur.execute("DROP FUNCTION IF EXISTS okg.commit_delta_set(jsonb, bigint)")
            # Chronos cutover: a published generation is now a Chronos
            # checkpoint with a NON-NUMERIC string id, so the poros outbox
            # sender_generation_id must be TEXT and can no longer carry an FK
            # into okg.graph_generations (a checkpoint is not a row there).
            # Fresh DBs get TEXT + no FK from schema.sql; existing DBs are
            # migrated here.
            #
            # These statements ALWAYS run (they are outside the schema-body
            # skip). A bare ``ALTER COLUMN ... TYPE TEXT`` takes an ACCESS
            # EXCLUSIVE lock and rewrites the table on EVERY migrate even when
            # the column is already TEXT (make-migrate-idempotent, design
            # edit 4). Guard each behind a catalog-type check so a converged
            # deployment takes no strong lock and does no rewrite.
            cur.execute(_guarded_poros_generation_id_text_migration())

            # okg.aliases.normalized_alias (#796) is added by the snapshot as a
            # nullable column, so every row that predates it stays NULL and is
            # invisible to the normalized resolver lookup -- which would then
            # mint a fresh canonical instead of matching a curated alias. The
            # backfill is Python-side (NFKC + casefold + Unicode-category
            # filtering) because a SQL equivalent would drift from
            # normalize_entity_name, and a drifting key is worse than a
            # missing one. Always runs: it is a no-op once converged, and a
            # schema-body skip must not strand rows a partial prior run left.
            from okg.substrate.catalog.aliases import backfill_normalized_aliases

            backfilled = backfill_normalized_aliases(conn)
            if backfilled:
                logging.getLogger(__name__).info(
                    "migrate: backfilled normalized_alias on %d okg.aliases rows",
                    backfilled,
                )

            # Ledger mutation <=> body execution: only rewrite the 0000 snapshot
            # row when the body actually ran. On a checksum skip the row is left
            # intact (its applied_at is stable), which is the observable proof
            # that the body was not replayed.
            if not skip_schema_body:
                record_schema_snapshot(
                    conn,
                    checksum=schema_checksum,
                    statements_executed=_statement_count(executable_sql),
                    executable_schema_checksum=computed_executable_checksum,
                )
            else:
                # Views are always reinstalled, but the executable-body checksum
                # (the skip key) excludes views, so a views-only release skips
                # the body here. The 0000 row's ``checksum`` column is the JOINT
                # schema+views hash that ``migration_status`` /
                # ``deployment_release`` / doctor consume — if it is not
                # refreshed after a views-only change it goes stale and reports a
                # spurious, permanent ``schema_checksum_mismatch`` release-block.
                # Refresh it in place WITHOUT bumping ``applied_at`` (the
                # body-not-replayed invariant — stable applied_at — is
                # preserved; only the joint checksum moves).
                cur.execute(
                    "UPDATE okg.schema_migrations SET checksum = %s "
                    "WHERE migration_id = %s AND checksum <> %s",
                    (schema_checksum, SCHEMA_SNAPSHOT_MIGRATION_ID, schema_checksum),
                )
            if not job_runtime_subsystem_present(conn):
                retirement = next(
                    migration
                    for migration in current_known_migrations(
                        schema_sql=sql,
                        views_sql=views,
                    )
                    if migration.migration_id
                    == REMOVE_JOB_RUNTIME_SUBSYSTEM_MIGRATION_ID
                )
                # Evaluated, not asserted from context: a bare string here used
                # to record "absent" as an unre-checkable claim (issue #1054).
                with conn.cursor() as postcondition_cur:
                    baseline_postcondition = evaluate_postcondition(
                        postcondition_cur, ("job_runtime_subsystem_absent",)
                    )
                record_known_migration(
                    conn,
                    retirement,
                    result_metadata={
                        "applied_via": "schema_baseline",
                        "postcondition": baseline_postcondition,
                    },
                )
            if guard_deployment is not None:
                from okg.substrate.db.deployment_owner import claim_db_owner
                from okg.substrate.deployments import (
                    DeploymentResolutionError,
                    resolve_deployment,
                )

                try:
                    claim_deployment = resolve_deployment(
                        str(guard_deployment)
                    ).name
                except DeploymentResolutionError:
                    claim_deployment = str(guard_deployment)

                claim_db_owner(
                    cur,
                    claim_deployment,
                    command="okg migrate --deployment",
                )
            # Every OWNER TO in schema.sql, the ordered migrations and the
            # view scripts has run; withdraw the grant before the commit.
            _close_function_owner_transfer(cur)

        # THE single commit of the migrate transaction (audit F1): everything
        # above — schema body, Chronos ensures (commit-suppressed proxy),
        # ordered migrations, views, ledger rows — becomes durable here
        # atomically, and the advisory migration lock releases with it.
        conn.commit()

    # Count at migrate surface level. The "statements executed"
    # accounting is coarse — we report the rough count from the
    # script. A detailed-per-statement runner is a follow-up if
    # the structure warrants (M2).
    stmt_count = _statement_count(sql)

    return MigrateResult(
        statements_executed=stmt_count,
        statements_skipped=0,
        extension_versions=ext_versions,
        idempotent_second_run=True,  # by construction; the test
        # harness confirms empirically
        converged_views=tuple(converged_views),
        pending_multiphase=tuple(
            str(row["migration_id"])
            for row in runner_outcome.get("pending_multiphase") or []
        ),
        deferred_post_multiphase=tuple(
            str(migration_id)
            for migration_id in runner_outcome.get("deferred_post_multiphase")
            or []
        ),
    )


def install_views(
    dsn: str,
    *,
    deployment_name: str | None = None,
) -> MigrateResult:
    """Refresh only the agent-facing read-side views.

    This is intentionally narrower than ``migrate()``. It does not replay
    schema DDL and does not update the schema snapshot ledger, so operators can
    safely refresh append-only view changes on live Timescale databases where
    unrelated schema DDL may be blocked by compressed chunks.

    Bounded exactly like ``migrate()``, and for the same reason. The
    frozen-view-shape recovery reaches this entry point too, and that
    recovery takes ACCESS EXCLUSIVE on every view it drops. On the live
    database this function advertises as safe, one long-running reader
    would otherwise queue that drop behind itself and stall the whole
    read layer with no deadline. ``SET LOCAL`` timeouts plus the same
    bounded-retry-then-``MigrationLockTimeout`` envelope turn that into a
    clean, typed refusal with the database untouched. Bounding is the
    right fix rather than refusing the recovery here, because refusing
    would leave the live-safe entry point unable to converge a database
    at all and push operators to the heavier full migrate.
    """
    views = _views_sql()
    deployment_views_sql, deployment_views_path = _load_deployment_views(
        deployment_name,
    )

    lock_timeout_ms, statement_timeout_ms, attempts = _migrate_timeout_settings()
    for attempt in range(1, attempts + 1):
        try:
            return _install_views_once(
                dsn,
                views=views,
                deployment_name=deployment_name,
                deployment_views_sql=deployment_views_sql,
                deployment_views_path=deployment_views_path,
                lock_timeout_ms=lock_timeout_ms,
                statement_timeout_ms=statement_timeout_ms,
            )
        except (
            psycopg.errors.LockNotAvailable,
            psycopg.errors.QueryCanceled,
        ) as exc:
            if attempt >= attempts:
                raise MigrationLockTimeout(
                    "install_views could not acquire the locks it needs "
                    f"within lock_timeout={lock_timeout_ms}ms / "
                    f"statement_timeout={statement_timeout_ms}ms across "
                    f"{attempts} attempt(s); the transaction was rolled "
                    "back cleanly and the stored views are unchanged",
                    data={
                        "attempts": attempts,
                        "lock_timeout_ms": lock_timeout_ms,
                        "statement_timeout_ms": statement_timeout_ms,
                        "sqlstate": exc.sqlstate,
                    },
                    fix_hint=(
                        "find and release the conflicting lock "
                        "(pg_locks/pg_stat_activity), or raise "
                        f"{MIGRATE_LOCK_TIMEOUT_ENV} / "
                        f"{MIGRATE_LOCK_ATTEMPTS_ENV} and re-run"
                    ),
                ) from exc
            logging.getLogger(__name__).warning(
                "install_views attempt %d/%d hit a lock/statement timeout "
                "(sqlstate=%s); rolled back, retrying",
                attempt,
                attempts,
                exc.sqlstate,
            )
    raise AssertionError("unreachable: the retry loop returns or raises")


def _install_views_once(
    dsn: str,
    *,
    views: str,
    deployment_name: str | None,
    deployment_views_sql: str | None,
    deployment_views_path: Path | None,
    lock_timeout_ms: int,
    statement_timeout_ms: int,
) -> MigrateResult:
    with db_connect(dsn) as conn:
        _verify_server_version(conn)
        ext_versions = _verify_extensions(conn)
        executable_views = _executable_views_sql(conn, views, include_storage=False)
        # Same guard as migrate(): timeouts installed BEFORE the advisory
        # wait, so even the serialization wait is bounded, and the lock
        # keeps a concurrent migrate from interleaving with a view drop.
        _begin_guarded_migrate_transaction(
            conn,
            lock_timeout_ms=lock_timeout_ms,
            statement_timeout_ms=statement_timeout_ms,
        )
        with conn.cursor() as cur:
            # views.sql transfers function ownership too (issue #2239).
            _open_function_owner_transfer(cur)
            converged_views = _install_views_sql(
                cur,
                executable_views,
                deployment_name=deployment_name,
                deployment_views_sql=deployment_views_sql,
                deployment_views_path=deployment_views_path,
            )
            _close_function_owner_transfer(cur)
        conn.commit()

    stmt_count = _statement_count(executable_views)
    if deployment_views_sql is not None:
        stmt_count += _statement_count(deployment_views_sql)

    return MigrateResult(
        statements_executed=stmt_count,
        statements_skipped=0,
        extension_versions=ext_versions,
        idempotent_second_run=True,
        converged_views=tuple(converged_views),
    )
