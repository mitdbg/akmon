"""Chronos-backed graph storage public adapter."""
from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Literal, Mapping

from okg.substrate.graph_store_internal.common import (
    CHRONOS_INTERVAL_CONTINUATION_PERCENT_ENV,
    CONSOLE_COUNT_ROLLUP_PK,
    CONSOLE_COUNT_ROLLUP_TABLE,
    DEFAULT_BRANCH,
    EDGE_TABLE,
    NODE_TABLE,
    ONTOLOGY_ROLLUP_PK,
    ONTOLOGY_ROLLUP_TABLE,
    GraphEditResult,
    deployment_interval_continuation_percent,
    edge_id,
    index_matches_columns,
    interval_branches_table_present,
    load_chronos,
    load_chronos_postgres_adapter,
    mark_schema_bootstrap_done,
    schema_bootstrap_done,
    schema_ensured_key,
    schema_ensured_key_for_context,
    stamp_store_application_name,
)
from okg.substrate.graph_store_internal.generations import ChronosGraphGenerationLifecycleMixin
from okg.substrate.graph_store_internal.read_branch import ChronosGraphReadBranchMixin
from okg.substrate.graph_store_internal.schema import ChronosGraphSchemaMixin
from okg.substrate.graph_store_internal.writes import ChronosGraphWriteMixin
from okg.substrate.library.sources.base import EdgeFact, NodeFact
from okg.substrate.history_read_guard import GenerationReadLeaseBackend


_load_chronos = load_chronos
_load_chronos_postgres_adapter = load_chronos_postgres_adapter
_edge_id = edge_id
_index_matches_columns = index_matches_columns

class ChronosGraphStore(
    ChronosGraphSchemaMixin, ChronosGraphGenerationLifecycleMixin,
    ChronosGraphReadBranchMixin, ChronosGraphWriteMixin,
):
    """Thin OKG adapter over Chronos branch sessions.

    The public API stays here for callers. Internal responsibilities live in
    named boundary mixins under ``graph_store_internal``.
    """

    def __init__(
        self, context: Any, *, owns_context: bool = True,
        history_read_lease_backend: GenerationReadLeaseBackend | None = None,
    ):
        self._apply_store_options()
        self.context = context
        self._owns_context = owns_context
        self._history_read_lease_backend = history_read_lease_backend

    @classmethod
    def connect(
        cls,
        dsn: str,
        *,
        backend: Literal["log", "interval", "copy"] = "interval",
        ensure_schema: bool = True,
        interval_continuation_percent: int | None = None,
        history_read_lease_backend: GenerationReadLeaseBackend | None = None,
    ) -> "ChronosGraphStore":
        ChronosBranchContext = _load_chronos()
        context = ChronosBranchContext.connect(
            stamp_store_application_name(dsn),
            backend=backend,
            ensure_metadata=False,
            interval_continuation_percent=interval_continuation_percent,
        )
        store = cls(context, history_read_lease_backend=history_read_lease_backend)
        if ensure_schema:
            key = schema_ensured_key_for_context(context)
            if not schema_bootstrap_done(key) or not interval_branches_table_present(
                context.db, backend
            ):
                context.ensure_metadata()
                store.ensure_schema()
                store.ensure_branch(DEFAULT_BRANCH, from_branch="main")
                mark_schema_bootstrap_done(key)
        return store

    @classmethod
    def from_connection(
        cls,
        conn: Any,
        *,
        backend: Literal["log", "interval", "copy"] = "interval",
        ensure_schema: bool = True,
        autocommit: bool = True,
        interval_continuation_percent: int | None = None,
        catalog_version_id: int | None = None,
        history_read_lease_backend: GenerationReadLeaseBackend | None = None,
    ) -> "ChronosGraphStore":
        ChronosBranchContext = _load_chronos()
        PostgresDatabaseAdapter = _load_chronos_postgres_adapter()
        key = schema_ensured_key(conn)
        adapter = PostgresDatabaseAdapter(conn)
        context = ChronosBranchContext.from_database_adapter(
            adapter,
            backend=backend,
            ensure_metadata=False,
            autocommit=autocommit,
            interval_continuation_percent=interval_continuation_percent,
        )
        store = cls(
            context, owns_context=False,
            history_read_lease_backend=history_read_lease_backend,
        )
        store._catalog_version_id = catalog_version_id
        if ensure_schema:
            metadata_present = interval_branches_table_present(context.db, backend)
            branches_ready = False
            if metadata_present:
                try:
                    branches = {info.branch_id for info in context.list_branches()}
                    branches_ready = "main" in branches and DEFAULT_BRANCH in branches
                except Exception:  # noqa: BLE001 - stale metadata cache, rebootstrap.
                    branches_ready = False
            # Run the idempotent Chronos metadata + OKG schema bootstrap at most
            # once per live database, unless a test reset dropped/truncated the
            # physical branch metadata under the warm process cache.
            do_ensure = (
                not schema_bootstrap_done(key)
                or not metadata_present
                or not branches_ready
            )
        else:
            do_ensure = False
        if do_ensure:
            context.ensure_metadata()
            store.ensure_schema()
            store.ensure_branch(DEFAULT_BRANCH, from_branch="main")
            mark_schema_bootstrap_done(key)
        return store

    def close(self) -> None:
        if self._owns_context:
            self.context.close()

    def ensure_schema(self) -> None:
        return super().ensure_schema()

    def ensure_interval_storage_upgrade(self) -> None:
        return super().ensure_interval_storage_upgrade()

    def checkout_generation(
        self,
        generation_id: int | str,
        *,
        branch: str = DEFAULT_BRANCH,
    ):
        return super().checkout_generation(generation_id, branch=branch)

    def checkpoint_generation(
        self,
        generation_id: int | str | None = None,
        *,
        branch: str = DEFAULT_BRANCH,
        deployment_name: str = "default",
        catalog_version_id: int | None = None,
        status: str = "published",
        metadata: dict[str, Any] | None = None,
        commit: bool | None = None,
    ) -> str:
        return super().checkpoint_generation(
            generation_id,
            branch=branch,
            deployment_name=deployment_name,
            catalog_version_id=catalog_version_id,
            status=status,
            metadata=metadata,
            commit=commit,
        )

    def get_generation(self, generation_id: int | str):
        return super().get_generation(generation_id)

    def list_generations(
        self,
        *,
        deployment_name: str | None = "default",
        branch: str | None = None,
        status: str | None = "published",
    ):
        return super().list_generations(
            deployment_name=deployment_name,
            branch=branch,
            status=status,
        )

    def latest_generation(
        self,
        *,
        deployment_name: str = "default",
        branch: str = DEFAULT_BRANCH,
        status: str | None = "published",
    ):
        return super().latest_generation(
            deployment_name=deployment_name,
            branch=branch,
            status=status,
        )

    def apply_facts(
        self,
        facts: Iterable[NodeFact | EdgeFact],
        *,
        branch: str = DEFAULT_BRANCH,
        source: str | None = None, policy_fingerprint: str | None = None,
        run_id: str | None = None, observed_at: datetime | None = None,
        strict_endpoints: bool = True, node_retraction: str = "handoff",
        session: Any | None = None, derived_reapplication: bool = False,
        prepared: Any | None = None,
        row_provenance: Mapping[str, Mapping[str, Any]] | None = None,
    ) -> GraphEditResult:
        return super().apply_facts(
            facts,
            branch=branch,
            source=source, policy_fingerprint=policy_fingerprint,
            run_id=run_id, observed_at=observed_at,
            strict_endpoints=strict_endpoints, node_retraction=node_retraction,
            session=session, derived_reapplication=derived_reapplication,
            prepared=prepared, row_provenance=row_provenance,
        )


__all__ = [
    "CHRONOS_INTERVAL_CONTINUATION_PERCENT_ENV",
    "CONSOLE_COUNT_ROLLUP_PK", "CONSOLE_COUNT_ROLLUP_TABLE",
    "DEFAULT_BRANCH",
    "EDGE_TABLE", "NODE_TABLE",
    "ONTOLOGY_ROLLUP_PK", "ONTOLOGY_ROLLUP_TABLE",
    "ChronosGraphStore",
    "GraphEditResult",
    "deployment_interval_continuation_percent",
    "_edge_id", "_index_matches_columns",
]
