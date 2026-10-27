"""Generation lifecycle boundary for the Chronos graph store."""
from __future__ import annotations

import contextlib
import json
import logging
import threading
import uuid
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any, Iterator

from okg.substrate.errors import SubstrateError
from okg.substrate.graph_store_internal.common import (
    DEFAULT_BRANCH,
    EDGE_TABLE,
    NODE_TABLE,
    quote_ident,
    quote_physical_table,
)

logger = logging.getLogger(__name__)


def _as_run_list(value: Any) -> list[Any]:
    """A projected enricher-run list, whatever the driver handed back.

    Postgres returns jsonb already decoded; a text column or a non-Postgres
    backend returns a JSON string. Anything that is not a list of runs reads
    as "this checkpoint recorded none", which is what the composer already
    did with a malformed `enricher_runs`.
    """
    if isinstance(value, list):
        return value
    if isinstance(value, (str, bytes)):
        try:
            decoded = json.loads(value)
        except Exception:  # noqa: BLE001 - malformed projection; no runs
            return []
        return decoded if isinstance(decoded, list) else []
    return []



# The console reads a WINDOW of the newest generation checkpoints for enricher
# run history, never the whole history. `list_generations` fetches every
# checkpoint's FULL metadata -- 88 rows and 39 MB on okg-workspace, 1.9 s per
# call -- and the console called it once per enricher name plus once for the
# name set: 19 fetches per request, 45.6 s of a 59 s read (issue #2202).
DEFAULT_ENRICHER_RUNS_WINDOW = 20

# `_latest_generation_checkpoint_fast` reads the newest checkpoints in pages
# and stops at the first match. It used to read exactly one row, so a newest
# checkpoint that did not match the requested status sent every caller down
# the full-fetch path. On okg-workspace the newest checkpoint is `blocked` and
# the newest `published` one is the row behind it, which cost two 3.5 s full
# fetches per console request (issue #2202). Paging keeps the read bounded;
# the cap keeps a long run of non-matching checkpoints from turning the fast
# path into the slow one, and the caller still falls back when it is reached.
_LATEST_GENERATION_SCAN_PAGE = 8
_LATEST_GENERATION_SCAN_CAP = 200

# Issue #3404: a checkpoint's metadata embeds its generation's invariant
# summary (2.3 MB published, 51 MB blocked on a Krama copy), so reading pages
# of whole rows cost every MCP call ~340 ms to learn one row's kind and status.
# On PostgreSQL the walk reads only (checkpoint_id, xmin) per row. A row's
# xmin changes on every UPDATE, so (database oid, database, checkpoint_id,
# xmin) names one immutable row version: its kind, deployment and status are parsed
# once per process and kept here, and only the row the walk returns has its
# metadata read, as before. The text cache holds a few returned rows' text.
_CHECKPOINT_SUMMARIES: "OrderedDict[tuple[str, ...], tuple[Any, Any, Any, Any, Any, Any]]" = OrderedDict()
_CHECKPOINT_SUMMARY_CAP = 4096
_CHECKPOINT_TEXTS: "OrderedDict[tuple[str, ...], str]" = OrderedDict()
_CHECKPOINT_TEXT_ENTRIES = 4
_CHECKPOINT_TEXT_BYTES = 32 * 1024 * 1024
_CHECKPOINT_CACHE_LOCK = threading.Lock()


_CHECKPOINT_WALK_WARNED = False


def _warn_checkpoint_walk_fallback(exc: BaseException) -> None:
    """Log, once per process, that the version walk failed and the caller
    falls back to reading every checkpoint's metadata (issue #3404)."""
    global _CHECKPOINT_WALK_WARNED
    if _CHECKPOINT_WALK_WARNED:
        return
    _CHECKPOINT_WALK_WARNED = True
    logger.warning(
        "latest_generation: the checkpoint version walk failed (%s: %s); "
        "falling back to the full checkpoint fetch, which reads every "
        "checkpoint's metadata",
        type(exc).__name__, exc,
    )


def _remember_checkpoint_text(key: tuple[str, ...], text: str) -> None:
    """Keep a returned checkpoint row's metadata text (bounded LRU)."""
    if len(text) > _CHECKPOINT_TEXT_BYTES:
        return
    with _CHECKPOINT_CACHE_LOCK:
        _CHECKPOINT_TEXTS[key] = text
        _CHECKPOINT_TEXTS.move_to_end(key)
        total = sum(len(t) for t in _CHECKPOINT_TEXTS.values())
        while _CHECKPOINT_TEXTS and (
            len(_CHECKPOINT_TEXTS) > _CHECKPOINT_TEXT_ENTRIES
            or total > _CHECKPOINT_TEXT_BYTES
        ):
            _old, dropped = _CHECKPOINT_TEXTS.popitem(last=False)
            total -= len(dropped)


class GenerationEnricherRuns:
    """One generation checkpoint, carrying its enricher runs and nothing else.

    Shaped so the console's run adapters can read it exactly as they read a
    `CheckpointInfo`: `checkpoint_id`, `created_at` and a `metadata` mapping.
    The mapping holds ONLY the keys the run history needs, because the point
    of this type is that the other ~450 KB of a checkpoint's metadata never
    leaves the database.
    """

    __slots__ = ("checkpoint_id", "created_at", "metadata")

    def __init__(
        self,
        checkpoint_id: str,
        created_at: Any,
        metadata: dict[str, Any],
    ) -> None:
        self.checkpoint_id = checkpoint_id
        self.created_at = created_at
        self.metadata = metadata

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return (
            f"GenerationEnricherRuns(checkpoint_id={self.checkpoint_id!r}, "
            f"created_at={self.created_at!r})"
        )


# Every metadata key the console's generation composers read off a
# checkpoint. It is the CONTRACT between the bounded reader below and those
# composers: the reader projects only keys named here and returns nothing
# else, so a composer that started reading a new key would read `None` where
# a value was recorded. A guard test scans each composer for its metadata key
# literals and fails when one is missing from the set that composer asks for,
# because the alternative -- letting the reader quietly hand back a short
# dict, or letting the caller fall back to the full fetch -- turns a recorded
# fact into "not recorded" on the operator's page.
#
# Each caller names the SUBSET it reads. That is not tidiness: a projected key
# still moves its bytes, and a checkpoint's `blocked_reason` or
# `projection_deltas` can be tens of megabytes on its own. Projecting all
# sixteen keys over okg-workspace's 88 checkpoints costs 2.45 s; projecting
# the four the version list reads costs 0.31 s.
#
# Every key is projected as `json`, never as `text`: the composers were
# written against `json.loads(metadata)`, so a value must arrive as the same
# Python object it would have been there. A `text` projection would hand back
# the string `"3"` for the integer `3` and a serialisation for an object.
RECENT_GENERATION_METADATA_KEYS: tuple[str, ...] = (
    # Scope and outcome. `kind` and `deployment_name` are what
    # `list_generations` filters on; this reader applies the same filter, in
    # Python, to the rows its bounded scan returned.
    "kind",
    "deployment_name",
    "status",
    # `compose_published_versions`
    "published_at",
    "catalog_version_id",
    # `compose_attempt_row`, shared by the attempts list and the attempt detail
    "target_generation_id",
    "blocked_reason",
    "excluded_sources",
    # the attempt detail's own sections
    "predecessor_generation_id",
    "node_count",
    "edge_count",
    "deployment_attestations",
    "nomos_policy_bundle_ids",
    "enricher_governance",
    "invariant_summary",
    "projection_deltas",
    # `compose_health`'s previous published generation, read only when the
    # pinned one recorded no `projection_deltas` (issue #2113). The `_live`
    # names are what older publishers stamped.
    "nodes_live",
    "edges_live",
    # `producer_metrics._selected_enricher_runs` (issue #2113): the two
    # places a publisher has recorded enricher runs.
    "enricher_runs",
    "timing_details",
)

# How far back a bounded generation read looks when its caller names no
# window: the same cap the fast path uses, for the same reason -- a bound the
# caller can state and serve beats a scan it cannot.
DEFAULT_GENERATION_SCAN_WINDOW = 200


def _generation_projection(keys: tuple[str, ...] | None) -> tuple[str, ...]:
    """The keys to project, always including the ones the filter needs.

    A key outside `RECENT_GENERATION_METADATA_KEYS` is a REFUSAL, not a
    silently dropped column: the whole point of the contract is that a
    composer cannot start reading a key the reader does not fetch and see
    `None` where the checkpoint recorded a value.
    """
    requested = (
        RECENT_GENERATION_METADATA_KEYS if keys is None else tuple(dict.fromkeys(keys))
    )
    unknown = [key for key in requested if key not in RECENT_GENERATION_METADATA_KEYS]
    if unknown:
        raise SubstrateError(
            "recent_generations was asked for metadata keys outside the "
            "projection contract: "
            + ", ".join(sorted(unknown))
            + " -- add them to RECENT_GENERATION_METADATA_KEYS"
        )
    # The reader applies `list_generations`'s own scope filter in Python, so
    # these three are fetched whether or not the caller reads them.
    required = ("kind", "deployment_name", "status")
    return tuple(dict.fromkeys(required + requested))


class GenerationRow:
    """One generation checkpoint carrying ONLY the projected metadata keys.

    Shaped like `CheckpointInfo` -- `checkpoint_id`, `branch_id`,
    `segment_id`, `created_at`, `metadata` -- so a composer reads it exactly
    as it read a full checkpoint. `metadata` holds only
    `RECENT_GENERATION_METADATA_KEYS`; it is NOT the checkpoint's metadata
    document and must not be handed to a caller that needs the rest of it.
    """

    __slots__ = (
        "checkpoint_id", "branch_id", "segment_id", "created_at", "metadata",
    )

    def __init__(
        self,
        checkpoint_id: str,
        branch_id: Any,
        segment_id: Any,
        created_at: Any,
        metadata: dict[str, Any],
    ) -> None:
        self.checkpoint_id = checkpoint_id
        self.branch_id = branch_id
        self.segment_id = segment_id
        self.created_at = created_at
        self.metadata = metadata

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return (
            f"GenerationRow(checkpoint_id={self.checkpoint_id!r}, "
            f"created_at={self.created_at!r})"
        )


class RecentGenerations:
    """A bounded generation read, carrying its own bound.

    `rows` is newest first. `scanned` is how many checkpoints the read looked
    at, `scan_window` is the bound it was given, and `window_exhausted` says
    the read stopped AT that bound -- so older checkpoints exist that it never
    saw. A caller serving a total, a chart or a "load more" must serve that
    flag too: a bounded read presented as a complete one is how a console ends
    up printing a shorter history than the deployment has.
    """

    __slots__ = ("rows", "scanned", "scan_window", "window_exhausted")

    def __init__(
        self,
        rows: list[GenerationRow],
        *,
        scanned: int,
        scan_window: int,
        window_exhausted: bool,
    ) -> None:
        self.rows = rows
        self.scanned = scanned
        self.scan_window = scan_window
        self.window_exhausted = window_exhausted

    def __repr__(self) -> str:  # pragma: no cover - diagnostics only
        return (
            f"RecentGenerations(rows={len(self.rows)}, "
            f"scanned={self.scanned}, scan_window={self.scan_window}, "
            f"window_exhausted={self.window_exhausted})"
        )


class ChronosGraphGenerationLifecycleMixin:
    """Generation checkpoint, compaction, restore, and fast-forward operations."""

    context: Any
    _COMPACTION_CALLER_METADATA_RESERVED = frozenset({
        "kind",
        "deployment_name",
        "branch",
        "status",
        "created_at",
        "publisher",
        "compacted_from_generation",
        "rolled_over_from_generation",
        "rolled_over_at",
        "node_count",
        "edge_count",
        "node_digest",
        "edge_digest",
        "catalog_version_id",
        "target_generation_id",
    })
    # Keys a compacted or rolled-over checkpoint must NOT inherit from the
    # generation it was built from. Everything else is carried forward, so a key
    # that answers a question about one specific generation belongs here: copied
    # onto a new checkpoint it becomes a confident wrong answer.
    _COMPACTION_SOURCE_METADATA_IGNORED = frozenset({
        "kind",
        "deployment_name",
        "branch",
        "status",
        "created_at",
        "publisher",
        "compacted_from_generation",
        "node_count",
        "edge_count",
        "node_digest",
        "edge_digest",
        # Retired marker (PACT lane-s-storage-drop-copies) that older
        # checkpoints still carry; a compacted checkpoint must not inherit it.
        "effective_interval_view_sync",
        # The published predecessor this generation was built on. A compacted
        # checkpoint's predecessor is the compaction, not the source
        # generation's predecessor, and an inherited pointer would send any
        # chain walk into pre-compaction history.
        "predecessor_generation_id",
    })

    def compact_branch_from_generation(
        self,
        source_generation_id: int | str,
        *,
        deployment_name: str,
        branch: str = DEFAULT_BRANCH,
        new_generation_id: int | str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if getattr(self.context, "backend_name", None) != "interval":
            raise SubstrateError(
                "Chronos graph compaction currently supports only the interval backend",
                data={"backend": getattr(self.context, "backend_name", None)},
            )

        source_checkpoint = self._generation_checkpoint_id(source_generation_id)
        try:
            cp = self.context.get_checkpoint(source_checkpoint)
        except Exception as exc:  # noqa: BLE001 - convert to substrate error
            raise SubstrateError(
                f"source generation {source_generation_id!r} was not found",
                fix_hint="run `okg generation list --deployment <name>`",
            ) from exc
        self._assert_compaction_source_checkpoint(
            cp,
            deployment_name=deployment_name,
            branch=branch,
        )
        self._assert_branch_compaction_scope(
            deployment_name=deployment_name,
            branch=branch,
        )

        source_metadata = self._compaction_source_metadata(cp.metadata or {})
        caller_metadata = self._compaction_caller_metadata(metadata or {})
        tables = self._interval_table_metadata()
        self._assert_compaction_root_is_empty(tables)
        self._materialize_checkpoint_tables(cp.ref, tables)
        node_stats = self._temp_table_stats(
            "_okg_compact_graph_nodes",
            order_column="node_id",
        )
        edge_stats = self._temp_table_stats(
            "_okg_compact_graph_edges",
            order_column="edge_id",
        )

        # Issue #4049: from the reset of the branch to the root's overlay
        # marker, one transaction (a savepoint when the caller holds one), so
        # a failure or a kill leaves the old head as it was, never a
        # markerless head. `okg branch compact` runs on an autocommit
        # connection. The Chronos metadata and graph schema are ensured
        # first, outside it: the backend's ensure() always commits, which
        # inside the transaction is refused and, before it, committed the
        # branch reset on its own (a failure after it lost the branch even
        # when the caller held a transaction). Both are idempotent and the
        # reset removes nothing they create.
        self.context.ensure_metadata()
        self.ensure_schema()
        with self._root_generation_transaction():
            self._reset_chronos_metadata_tables(
                deployment_name=deployment_name,
                branch=branch,
            )
            self.ensure_branch(
                branch,
                from_branch="main",
                metadata={
                    "kind": "okg_branch",
                    "deployment_name": deployment_name,
                    "branch_name": branch,
                    "role": "default" if branch == DEFAULT_BRANCH else "experiment",
                    "status": "active",
                    "compacted_from_generation": source_checkpoint,
                },
            )
            self._load_compacted_tables(branch=branch)

            compacted_generation = self.checkpoint_generation(
                new_generation_id,
                branch=branch,
                deployment_name=deployment_name,
                status="published",
                metadata={
                    **source_metadata,
                    "publisher": "chronos_compaction",
                    "compacted_from_generation": source_checkpoint,
                    "node_count": node_stats["count"],
                    "edge_count": edge_stats["count"],
                    "node_digest": node_stats["digest"],
                    "edge_digest": edge_stats["digest"],
                    **caller_metadata,
                },
                commit=False,
            )
            # The compaction root carries the source generation's overlays
            # and its marker.
            self._materialize_root_overlays(
                compacted_generation, previous=source_checkpoint,
            )
        return {
            "source_generation_id": source_checkpoint,
            "new_generation_id": compacted_generation,
            "deployment_name": deployment_name,
            "branch": branch,
            "node_count": node_stats["count"],
            "edge_count": edge_stats["count"],
            "node_digest": node_stats["digest"],
            "edge_digest": edge_stats["digest"],
        }

    def rollover_branch_from_generation(
        self,
        source_generation_id: int | str,
        *,
        deployment_name: str,
        branch: str = DEFAULT_BRANCH,
        new_generation_id: int | str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Recreate a leaf branch from a published generation without pruning history.

        Restoring directly beneath an old checkpoint gives the mutable branch only
        that checkpoint's remaining interval budget. A few subsequent checkpoints can
        therefore exhaust the branch even though the Chronos root still has ample
        space. Rollover copies the selected published graph state onto a fresh child of
        the empty Chronos root while retaining every existing generation checkpoint.

        The caller is responsible for stopping concurrent publishers and committing
        the surrounding transaction. The rollover mints one proof-equivalent published
        checkpoint with the source generation's catalog and target identity. Its graph
        digests are measured before the branch move, so the next ordinary publish can
        evaluate only facts written after this fresh baseline instead of treating the
        entire copied graph as a dirty delta.
        """
        if getattr(self.context, "backend_name", None) != "interval":
            raise SubstrateError(
                "Chronos graph rollover currently supports only the interval backend",
                data={"backend": getattr(self.context, "backend_name", None)},
            )

        source_checkpoint = self._generation_checkpoint_id(source_generation_id)
        try:
            cp = self.context.get_checkpoint(source_checkpoint)
        except Exception as exc:  # noqa: BLE001 - convert to substrate error
            raise SubstrateError(
                f"source generation {source_generation_id!r} was not found",
                fix_hint="run `okg generation list --deployment <name>`",
            ) from exc
        self._assert_compaction_source_checkpoint(
            cp,
            deployment_name=deployment_name,
            branch=branch,
        )
        self._assert_branch_compaction_scope(
            deployment_name=deployment_name,
            branch=branch,
        )
        source_metadata = self._compaction_source_metadata(cp.metadata or {})

        branch_row = self.context.db.execute(
            """
            SELECT parent_branch_id, metadata
              FROM _chronos_branch_interval_branches
             WHERE branch_id = ?
            """,
            (branch,),
        ).fetchone()
        if branch_row is None:
            raise SubstrateError(
                "cannot roll over branch: branch metadata is missing",
                data={"deployment_name": deployment_name, "branch": branch},
            )
        if branch_row["parent_branch_id"] != "main":
            raise SubstrateError(
                "cannot roll over branch: branch is not rooted directly under main",
                data={
                    "deployment_name": deployment_name,
                    "branch": branch,
                    "parent_branch": branch_row["parent_branch_id"],
                },
                fix_hint=(
                    "roll over a deployment-owned root branch, or rebase child "
                    "branches before recovery"
                ),
            )
        try:
            branch_metadata = json.loads(branch_row["metadata"] or "{}")
        except Exception as exc:  # noqa: BLE001
            raise SubstrateError(
                "cannot roll over branch: branch metadata is invalid",
                data={"deployment_name": deployment_name, "branch": branch},
            ) from exc
        caller_metadata = self._compaction_caller_metadata(metadata or {})

        tables = self._interval_table_metadata()
        self._assert_compaction_root_is_empty(tables)
        self._materialize_checkpoint_tables(cp.ref, tables)
        node_stats = self._temp_table_stats(
            "_okg_compact_graph_nodes",
            order_column="node_id",
        )
        edge_stats = self._temp_table_stats(
            "_okg_compact_graph_edges",
            order_column="edge_id",
        )
        checkpoint_count_before = int(
            self.context.db.execute(
                """
                SELECT COUNT(*) AS n
                  FROM _chronos_branch_interval_checkpoints
                 WHERE branch_id = ?
                """,
                (branch,),
            ).fetchone()["n"]
        )

        # Issue #4049: from the branch delete to the root's overlay marker,
        # one transaction: a savepoint inside `okg branch rollover`'s own
        # transaction, its own transaction on an autocommit caller.
        with self._root_generation_transaction():
            self._delete_compaction_target_branch(
                deployment_name=deployment_name,
                branch=branch,
            )
            now = datetime.now(timezone.utc).isoformat()
            self.ensure_branch(
                branch,
                from_branch="main",
                metadata={
                    **branch_metadata,
                    **caller_metadata,
                    "kind": "okg_branch",
                    "deployment_name": deployment_name,
                    "branch_name": branch,
                    "role": branch_metadata.get(
                        "role",
                        "default" if branch == DEFAULT_BRANCH else "experiment",
                    ),
                    "status": "active",
                    "rolled_over_from_generation": source_checkpoint,
                    "rolled_over_at": now,
                },
            )
            self._load_compacted_tables(branch=branch)

            checkpoint_count_after_copy = int(
                self.context.db.execute(
                    """
                    SELECT COUNT(*) AS n
                      FROM _chronos_branch_interval_checkpoints
                     WHERE branch_id = ?
                    """,
                    (branch,),
                ).fetchone()["n"]
            )
            if checkpoint_count_after_copy != checkpoint_count_before:
                raise SubstrateError(
                    "cannot roll over branch: generation history count changed",
                    data={
                        "deployment_name": deployment_name,
                        "branch": branch,
                        "before": checkpoint_count_before,
                        "after": checkpoint_count_after_copy,
                    },
                )

            rollover_generation = self.checkpoint_generation(
                new_generation_id,
                branch=branch,
                deployment_name=deployment_name,
                status="published",
                metadata={
                    **source_metadata,
                    "publisher": "chronos_rollover",
                    "rolled_over_from_generation": source_checkpoint,
                    "node_count": node_stats["count"],
                    "edge_count": edge_stats["count"],
                    "node_digest": node_stats["digest"],
                    "edge_digest": edge_stats["digest"],
                    **caller_metadata,
                },
                commit=False,
            )
            # The rollover root carries the source generation's overlays and
            # its marker.
            self._materialize_root_overlays(
                rollover_generation, previous=source_checkpoint,
            )

        return {
            "source_generation_id": source_checkpoint,
            "new_generation_id": rollover_generation,
            "deployment_name": deployment_name,
            "branch": branch,
            "node_count": node_stats["count"],
            "edge_count": edge_stats["count"],
            "node_digest": node_stats["digest"],
            "edge_digest": edge_stats["digest"],
            "preserved_checkpoint_count": checkpoint_count_after_copy,
            "checkpoint_count_after": checkpoint_count_after_copy + 1,
        }

    def _compaction_source_metadata(
        self,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        return {
            key: value
            for key, value in metadata.items()
            if key not in self._COMPACTION_SOURCE_METADATA_IGNORED
        }

    def _compaction_caller_metadata(
        self,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        reserved = sorted(
            set(metadata) & self._COMPACTION_CALLER_METADATA_RESERVED
        )
        if reserved:
            raise SubstrateError(
                "cannot compact branch: metadata attempts to override "
                "compaction scope or audit fields",
                data={"reserved_keys": reserved},
                fix_hint=(
                    "pass only supplemental metadata such as compacted_by; "
                    "deployment, branch, status, and compaction audit fields "
                    "are assigned by compaction"
                ),
            )
        return dict(metadata)

    def _materialize_checkpoint_tables(
        self,
        segment_id: str,
        tables: dict[str, dict[str, Any]],
    ) -> None:
        segment = self.context.db.execute(
            """
            SELECT branch_point
              FROM _chronos_branch_interval_segments
             WHERE segment_id = ?
            """,
            (segment_id,),
        ).fetchone()
        if segment is None:
            raise SubstrateError(
                "Chronos checkpoint segment is missing",
                data={"segment_id": segment_id},
            )
        point = int(segment["branch_point"])
        for logical, temp_name in (
            (NODE_TABLE, "_okg_compact_graph_nodes"),
            (EDGE_TABLE, "_okg_compact_graph_edges"),
        ):
            table = tables[logical]
            cols = ", ".join(quote_ident(col) for col in table["columns"])
            self.context.db.execute(f"DROP TABLE IF EXISTS {temp_name}")
            self.context.db.execute(
                f"""
                CREATE TEMP TABLE {temp_name}
                ON COMMIT PRESERVE ROWS AS
                SELECT {cols}
                  FROM {quote_physical_table(table["physical_table"])}
                 WHERE live_lo <= ?
                   AND ? < live_hi
                   AND deleted = FALSE
                """,
                (point, point),
            )

    def _assert_compaction_root_is_empty(
        self,
        tables: dict[str, dict[str, Any]],
    ) -> None:
        row = self.context.db.execute(
            """
            SELECT s.branch_point
              FROM _chronos_branch_interval_branches AS b
              JOIN _chronos_branch_interval_segments AS s
                ON s.segment_id = b.current_segment_id
             WHERE b.branch_id = 'main'
            """
        ).fetchone()
        if row is None:
            raise SubstrateError(
                "cannot compact branch: Chronos root branch is missing",
                data={"branch": "main"},
            )
        point = int(row["branch_point"])
        non_empty_tables: list[str] = []
        for table in tables.values():
            physical = quote_physical_table(table["physical_table"])
            found = self.context.db.execute(
                f"""
                SELECT 1
                  FROM {physical}
                 WHERE live_lo <= ?
                   AND ? < live_hi
                   AND deleted = FALSE
                 LIMIT 1
                """,
                (point, point),
            ).fetchone()
            if found is not None:
                non_empty_tables.append(str(table["physical_table"]))
        if non_empty_tables:
            raise SubstrateError(
                "cannot compact branch: Chronos root branch contains graph rows",
                data={"branch": "main", "physical_tables": non_empty_tables},
                fix_hint=(
                    "compaction rebases isolated branches from an empty Chronos "
                    "root; move root graph writes before compacting"
                ),
            )

    def _temp_table_stats(
        self,
        temp_name: str,
        *,
        order_column: str,
    ) -> dict[str, Any]:
        row = self.context.db.execute(
            f"""
            SELECT COUNT(*) AS n,
                   md5(COALESCE(string_agg(row_md5, '' ORDER BY {quote_ident(order_column)}), '')) AS digest
              FROM (
                    SELECT {quote_ident(order_column)},
                           md5(row_to_json(t)::text) AS row_md5
                      FROM {temp_name} AS t
                   ) AS hashed
            """
        ).fetchone()
        return {"count": int(row["n"]), "digest": row["digest"]}

    def _assert_compaction_source_checkpoint(
        self,
        checkpoint: Any,
        *,
        deployment_name: str,
        branch: str,
    ) -> None:
        metadata = dict(checkpoint.metadata or {})
        if (
            metadata.get("kind") != "okg_generation"
            or metadata.get("deployment_name") != deployment_name
            or str(metadata.get("branch") or checkpoint.branch_id) != branch
            or metadata.get("status") != "published"
        ):
            raise SubstrateError(
                "cannot compact branch: source generation is not the requested "
                "published deployment branch",
                data={
                    "deployment_name": deployment_name,
                    "branch": branch,
                    "checkpoint_id": checkpoint.checkpoint_id,
                    "checkpoint_branch": checkpoint.branch_id,
                    "checkpoint_deployment": metadata.get("deployment_name"),
                    "checkpoint_status": metadata.get("status"),
                },
                fix_hint=(
                    "run `okg generation list --deployment <name>` and compact "
                    "a published generation from the same branch"
                ),
            )

    def _assert_branch_compaction_scope(
        self,
        *,
        deployment_name: str,
        branch: str,
    ) -> None:
        branch_row = self.context.db.execute(
            """
            SELECT branch_id, parent_branch_id, child_count, metadata
              FROM _chronos_branch_interval_branches
             WHERE branch_id = ?
            """,
            (branch,),
        ).fetchone()
        if branch_row is None:
            raise SubstrateError(
                "cannot compact branch: branch metadata is missing",
                data={"deployment_name": deployment_name, "branch": branch},
                fix_hint="run `okg branch list --deployment <name>`",
            )
        try:
            branch_metadata = json.loads(branch_row["metadata"] or "{}")
        except Exception as exc:  # noqa: BLE001
            raise SubstrateError(
                "cannot compact branch: branch metadata is invalid",
                data={"deployment_name": deployment_name, "branch": branch},
            ) from exc
        metadata_deployment = branch_metadata.get("deployment_name")
        if metadata_deployment not in (None, deployment_name):
            raise SubstrateError(
                "cannot compact branch: Chronos branch belongs to another deployment",
                data={
                    "deployment_name": deployment_name,
                    "branch": branch,
                    "branch_deployment": metadata_deployment,
                },
                fix_hint=(
                    "compact a deployment-owned branch, or migrate shared "
                    "branches before compaction"
                ),
            )
        child_count = int(branch_row["child_count"] or 0)
        if child_count:
            raise SubstrateError(
                "cannot compact branch: target branch has child branches",
                data={
                    "deployment_name": deployment_name,
                    "branch": branch,
                    "child_count": child_count,
                },
                fix_hint=(
                    "compact a leaf branch, or abandon/rebase child branches "
                    "before compacting their parent"
                ),
            )

        other_scopes: set[str] = set()
        for row in self._okg_generation_checkpoint_rows(branch=branch):
            metadata = row["metadata"]
            checkpoint_deployment = metadata.get("deployment_name")
            checkpoint_branch = str(metadata.get("branch") or branch)
            if checkpoint_branch != branch:
                other_scopes.add(
                    f"{checkpoint_deployment or '<unknown>'}:{checkpoint_branch}"
                )
                continue
            if checkpoint_deployment not in (None, deployment_name):
                other_scopes.add(str(checkpoint_deployment))
        if other_scopes:
            raise SubstrateError(
                "cannot compact branch: branch contains generation checkpoints "
                "for another deployment or branch",
                data={
                    "deployment_name": deployment_name,
                    "branch": branch,
                    "other_scopes": sorted(other_scopes),
                },
                fix_hint=(
                    "compaction is intentionally fail-closed until each "
                    "deployment has an isolated Chronos branch"
                ),
            )

    def _okg_generation_checkpoint_rows(
        self,
        *,
        branch: str,
    ) -> list[dict[str, Any]]:
        rows = self.context.db.execute(
            """
            SELECT checkpoint_id, branch_id, metadata
              FROM _chronos_branch_interval_checkpoints
             WHERE branch_id = ?
             ORDER BY checkpoint_id
            """,
            (branch,),
        ).fetchall()
        parsed: list[dict[str, Any]] = []
        for row in rows:
            try:
                metadata = json.loads(row["metadata"] or "{}")
            except Exception as exc:  # noqa: BLE001
                raise SubstrateError(
                    "cannot compact branch: checkpoint metadata is invalid",
                    data={
                        "branch": branch,
                        "checkpoint_id": row["checkpoint_id"],
                    },
                ) from exc
            if metadata.get("kind") != "okg_generation":
                continue
            parsed.append({
                "checkpoint_id": row["checkpoint_id"],
                "branch_id": row["branch_id"],
                "metadata": metadata,
            })
        return parsed

    def _reset_chronos_metadata_tables(
        self,
        *,
        deployment_name: str,
        branch: str,
    ) -> None:
        checkpoint_ids = [
            row["checkpoint_id"]
            for row in self._okg_generation_checkpoint_rows(branch=branch)
            if row["metadata"].get("deployment_name") == deployment_name
            and str(row["metadata"].get("branch") or branch) == branch
        ]
        self._delete_compaction_target_branch(
            deployment_name=deployment_name,
            branch=branch,
        )
        for checkpoint_id in checkpoint_ids:
            self.context.db.execute(
                "DELETE FROM _chronos_branch_interval_checkpoints WHERE checkpoint_id = ?",
                (checkpoint_id,),
            )

    def _delete_compaction_target_branch(
        self,
        *,
        deployment_name: str,
        branch: str,
    ) -> None:
        db = self.context.db
        if getattr(db, "dialect", None) == "postgres":
            deleted = db.execute(
                """
                DELETE FROM _chronos_branch_interval_branches
                 WHERE branch_id = ?
                   AND child_count = 0
                RETURNING parent_branch_id
                """,
                (branch,),
            ).fetchone()
        else:
            deleted = db.execute(
                """
                SELECT parent_branch_id
                  FROM _chronos_branch_interval_branches
                 WHERE branch_id = ?
                   AND child_count = 0
                """,
                (branch,),
            ).fetchone()
            if deleted is not None:
                db.execute(
                    """
                    DELETE FROM _chronos_branch_interval_branches
                     WHERE branch_id = ?
                       AND child_count = 0
                    """,
                    (branch,),
                )
        if deleted is None:
            row = db.execute(
                """
                SELECT child_count
                  FROM _chronos_branch_interval_branches
                 WHERE branch_id = ?
                """,
                (branch,),
            ).fetchone()
            if row is None:
                raise SubstrateError(
                    "cannot compact branch: branch disappeared during compaction",
                    data={"deployment_name": deployment_name, "branch": branch},
                )
            raise SubstrateError(
                "cannot compact branch: target branch has child branches",
                data={
                    "deployment_name": deployment_name,
                    "branch": branch,
                    "child_count": int(row["child_count"] or 0),
                },
                fix_hint=(
                    "compact a leaf branch, or abandon/rebase child branches "
                    "before compacting their parent"
                ),
            )
        parent_branch_id = deleted["parent_branch_id"]
        if parent_branch_id is not None:
            db.execute(
                """
                UPDATE _chronos_branch_interval_branches
                   SET child_count = CASE
                       WHEN child_count > 0 THEN child_count - 1
                       ELSE 0
                   END
                 WHERE branch_id = ?
                """,
                (parent_branch_id,),
            )
        backend = getattr(self.context, "_backend", None)
        if backend is not None and hasattr(backend, "_interval_gc_requested"):
            backend._interval_gc_requested = True

    def _load_compacted_tables(self, *, branch: str) -> None:
        tables = self._interval_table_metadata()
        segment = self.context.db.execute(
            """
            SELECT s.segment_id, s.live_lo, s.live_hi
              FROM _chronos_branch_interval_branches AS b
              JOIN _chronos_branch_interval_segments AS s
                ON s.segment_id = b.current_segment_id
             WHERE b.branch_id = ?
            """,
            (branch,),
        ).fetchone()
        if segment is None:
            raise SubstrateError(
                "cannot load compacted graph rows: branch segment is missing",
                data={"branch": branch},
            )
        for logical, temp_name in (
            (NODE_TABLE, "_okg_compact_graph_nodes"),
            (EDGE_TABLE, "_okg_compact_graph_edges"),
        ):
            table = tables[logical]
            cols = ", ".join(quote_ident(col) for col in table["columns"])
            physical = quote_physical_table(table["physical_table"])
            self.context.db.execute(
                f"""
                INSERT INTO {physical}
                ({cols}, live_lo, live_hi, writer_segment_id, deleted)
                SELECT {cols}, ?, ?, ?, FALSE
                  FROM {temp_name}
                """,
                (
                    int(segment["live_lo"]),
                    int(segment["live_hi"]),
                    int(segment["segment_id"]),
                ),
            )

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
        checkpoint = self._generation_checkpoint_id(
            generation_id if generation_id is not None else self.new_generation_id(
                deployment_name=deployment_name,
                branch=branch,
            )
        )
        payload = {
            "kind": "okg_generation",
            "deployment_name": deployment_name,
            "branch": branch,
            "status": status,
            "catalog_version_id": catalog_version_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            **(metadata or {}),
        }
        if "target_generation_id" not in payload:
            text = str(generation_id) if generation_id is not None else ""
            if text.isdigit():
                payload["target_generation_id"] = int(text)
        # PACT openrouter-embeddings-hnsw: every generation records the
        # highest vector-set seq it may see -- the publisher's, a compaction's
        # and a rollover's alike -- on this connection, before the checkpoint
        # so the two commit together.
        self._record_embedding_watermarks(checkpoint)
        try:
            self.context.create_checkpoint(
                checkpoint, branch=branch, metadata=payload, commit=commit
            )
        except Exception as exc:
            if type(exc).__name__ != "BranchAlreadyExistsError":
                raise
        self._grant_chronos_public_tables_to_mcp_if_present()
        self._maybe_mint_generation_ordinal(
            checkpoint,
            branch=branch,
            deployment_name=deployment_name,
            status=status,
            external_transaction=commit is False,
        )
        return checkpoint

    def _maybe_mint_generation_ordinal(
        self,
        checkpoint: str,
        *,
        branch: str,
        deployment_name: str,
        status: str = "published",
        external_transaction: bool = False,
    ) -> None:
        """Mint the branch-aware generation ordinal for a freshly-created
        checkpoint (ivm-effective-attribute-view / task.generation-ordinal-primitive).

        Only ``published`` generations participate in the ordinal sequence (a
        blocked checkpoint must not occupy a slot the effective-attrs reader would
        later stab).

        The publisher has already assigned a ROOT branch's ordinal in the
        publication transaction, so this call returns it unchanged there. On
        every other path (a structural fork, compaction, merge_back) a mint
        failure is logged LOUDLY but does NOT block an otherwise-valid
        checkpoint -- e.g. a quarantine fork with no recorded base raises in
        the primitive's fail-loud API; here we log it (with traceback) rather
        than break ingest/merge_back publishing. (PACT lane-s-storage-drop-
        copies retired the ``OKG_IVM_GENERATION_ORDINAL=0`` kill switch: the
        overlay ranges are keyed by the ordinal.)
        """
        if status != "published":
            return
        try:
            from okg.substrate.graph_store_internal.generation_ordinal import (
                mint_generation_ordinal,
            )

            def mint() -> None:
                mint_generation_ordinal(
                    self.context.db,
                    self.context,
                    checkpoint_id=checkpoint,
                    branch=branch,
                    deployment_name=deployment_name,
                    list_generations=lambda: self.list_generations(
                        deployment_name=deployment_name,
                        branch=branch,
                        status="published",
                    ),
                )

            if external_transaction:
                # Isolate additive ordinal bookkeeping in a savepoint. Its
                # documented nonfatal failure must not abort the caller-owned
                # checkpoint + recovery-journal transaction.
                with self.context.conn.transaction():
                    mint()
            else:
                mint()
        except Exception as exc:  # noqa: BLE001 -- not yet consumed: log loud, never block the publish
            # Self-defense: a mid-mint failure (e.g. a UniqueViolation) can leave an
            # aborted transaction on an autocommit=False caller (merge_back); roll it
            # back here so the mint can never poison the publisher's own transaction.
            # No-op on the autocommit=True production publish path.
            if not external_transaction:
                try:
                    self.context.db.rollback()
                except Exception:  # noqa: BLE001
                    pass
            logger.warning(
                "generation ordinal mint skipped for checkpoint %s on branch %s "
                "(generation will lack an ordinal until backfilled): %s",
                checkpoint,
                branch,
                exc,
                exc_info=True,
            )

    def get_generation(self, generation_id: int | str):
        from okg.substrate.history_read_guard import raise_if_compacted

        checkpoint_id = self._generation_checkpoint_id(generation_id)
        raise_if_compacted(self.context.db, checkpoint_id)
        try:
            checkpoint = self.context.get_checkpoint(checkpoint_id)
        except Exception:
            raise_if_compacted(self.context.db, checkpoint_id)
            raise
        raise_if_compacted(self.context.db, checkpoint_id)
        return checkpoint

    def resolve_generation(
        self,
        generation_id: int | str,
        *,
        deployment_name: str | None = None,
        branch: str | None = DEFAULT_BRANCH,
        status: str | None = None,
    ):
        """Resolve a Chronos generation by checkpoint id or compat target id.

        The Chronos-native generation id is the checkpoint id. Consistency and
        a few legacy-adjacent operator surfaces still carry the integer
        ``target_generation_id`` stamped into checkpoint metadata. Accept both
        forms here so command handlers share one authority.
        """

        if deployment_name is None:
            try:
                checkpoint = self.get_generation(generation_id)
            except Exception as exc:  # noqa: BLE001 - checkpoint lookup probe
                from okg.substrate.errors import GenerationCompacted

                if isinstance(exc, GenerationCompacted):
                    raise
                checkpoint = None
        else:
            from okg.substrate.history_read_guard import raise_if_compacted

            native_id = self._generation_checkpoint_id(generation_id)
            raise_if_compacted(
                self.context.db,
                native_id,
                deployment_name=deployment_name,
            )
            try:
                checkpoint = self.context.get_checkpoint(native_id)
            except Exception:  # noqa: BLE001 - deployment-scoped native probe
                raise_if_compacted(
                    self.context.db,
                    native_id,
                    deployment_name=deployment_name,
                )
                checkpoint = None
            else:
                raise_if_compacted(
                    self.context.db,
                    native_id,
                    deployment_name=deployment_name,
                )
        if checkpoint is not None and self._checkpoint_metadata_matches(
            checkpoint,
            deployment_name=deployment_name,
            status=status,
        ):
            return checkpoint
        text = str(generation_id)
        if text.isdigit():
            checkpoint = self.find_generation_by_target_id(
                int(text),
                deployment_name=deployment_name,
                branch=branch,
                status=status,
            )
            if checkpoint is None:
                from okg.substrate.history_read_guard import raise_if_compacted

                raise_if_compacted(
                    self.context.db,
                    generation_id,
                    deployment_name=deployment_name,
                )
            return checkpoint
        return None

    @staticmethod
    def _metadata_target_generation_id(metadata: dict[str, Any]) -> int | None:
        raw = metadata.get("target_generation_id")
        if raw is None:
            return None
        text = str(raw)
        if not text.isdigit():
            return None
        return int(text)

    @staticmethod
    def _checkpoint_metadata_matches(
        checkpoint: Any,
        *,
        deployment_name: str | None,
        status: str | None,
    ) -> bool:
        metadata = dict(checkpoint.metadata or {})
        if metadata.get("kind") != "okg_generation":
            return False
        if deployment_name is not None and (
            metadata.get("deployment_name") != deployment_name
        ):
            return False
        if status is not None and metadata.get("status") != status:
            return False
        return True

    def find_generation_by_target_id(
        self,
        target_generation_id: int,
        *,
        deployment_name: str | None = None,
        branch: str | None = DEFAULT_BRANCH,
        status: str | None = None,
    ):
        metadata_filter: dict[str, Any] = {"kind": "okg_generation"}
        if deployment_name is not None:
            metadata_filter["deployment_name"] = deployment_name
        if status is not None:
            metadata_filter["status"] = status
        checkpoints = self.context.list_checkpoints(
            branch=branch,
            metadata_filter=metadata_filter,
        )
        matches = [
            cp for cp in checkpoints
            if self._metadata_target_generation_id(dict(cp.metadata or {}))
            == int(target_generation_id)
        ]
        if not matches:
            from okg.substrate.history_read_guard import raise_if_compacted

            raise_if_compacted(
                self.context.db,
                target_generation_id,
                deployment_name=deployment_name,
            )
            return None
        return sorted(
            matches,
            key=lambda cp: (cp.created_at, cp.checkpoint_id),
        )[-1]

    def delete_generation(self, generation_id: int | str) -> None:
        """Delete a generation checkpoint."""
        self.context.delete_checkpoint(self._generation_checkpoint_id(generation_id))

    def restore_branch_to_generation(
        self,
        generation_id: int | str,
        *,
        branch: str = DEFAULT_BRANCH,
        deployment_name: str = "default",
    ) -> None:
        checkpoint = self._generation_checkpoint_id(generation_id)
        backend = getattr(self.context, "_backend", None)
        if backend is None or not hasattr(backend, "_mutable_segment_ids_after_base"):
            raise SubstrateError(
                "cannot restore branch to generation: interval backend required",
                data={
                    "branch": branch,
                    "generation_id": checkpoint,
                    "backend": getattr(backend, "name", None),
                },
            )

        db = self.context.db
        try:
            cp = db.execute(
                """
                SELECT checkpoint_id, branch_id, segment_id, metadata
                  FROM _chronos_branch_interval_checkpoints
                 WHERE checkpoint_id = ?
                """,
                (checkpoint,),
            ).fetchone()
        except Exception as exc:  # noqa: BLE001
            raise SubstrateError(
                "cannot restore branch to generation: checkpoint metadata unavailable",
                data={"branch": branch, "generation_id": checkpoint},
            ) from exc
        if cp is None:
            raise SubstrateError(
                "cannot restore branch to generation: checkpoint not found",
                data={"branch": branch, "generation_id": checkpoint},
            )
        try:
            metadata = json.loads(cp["metadata"] or "{}")
        except Exception as exc:  # noqa: BLE001
            raise SubstrateError(
                "cannot restore branch to generation: checkpoint metadata is invalid",
                data={"branch": branch, "generation_id": checkpoint},
            ) from exc
        if (
            metadata.get("kind") != "okg_generation"
            or metadata.get("deployment_name") != deployment_name
            or metadata.get("status") != "published"
            or str(metadata.get("branch") or cp["branch_id"]) != branch
        ):
            raise SubstrateError(
                "cannot restore branch to generation: checkpoint is not the requested "
                "published branch generation",
                data={
                    "branch": branch,
                    "deployment_name": deployment_name,
                    "generation_id": checkpoint,
                    "checkpoint_branch": cp["branch_id"],
                    "checkpoint_status": metadata.get("status"),
                },
            )

        target_sql = (
            "SELECT * FROM _chronos_branch_interval_branches "
            "WHERE branch_id = ? FOR UPDATE"
            if getattr(db, "dialect", None) == "postgres"
            else "SELECT * FROM _chronos_branch_interval_branches WHERE branch_id = ?"
        )
        target = db.execute(target_sql, (branch,)).fetchone()
        if target is None:
            raise SubstrateError(
                "cannot restore branch to generation: branch not found",
                data={"branch": branch, "generation_id": checkpoint},
            )

        self._revert_branch_head_to_checkpoint(
            backend,
            branch=branch,
            checkpoint=checkpoint,
            checkpoint_segment_id=int(cp["segment_id"]),
        )

    def _revert_branch_head_to_checkpoint(
        self,
        backend: Any,
        *,
        branch: str,
        checkpoint: str,
        checkpoint_segment_id: int,
    ) -> None:
        """Write the reverse edit onto ``branch``'s own head (issue #2862).

        Every key written on the head's lineage since the checkpoint is read as
        the checkpoint sees it, then upserted (or deleted, when the checkpoint
        has no live row) on the branch head as an ordinary write. The branch
        keeps its current segment, so its interval is not narrowed. Re-rooting
        the head under the checkpoint's snapshot segment, which this replaces,
        cut the branch to about sqrt(W) of its width and let a few later
        publishes or forks exhaust it.

        Keys outside those writer segments were not written since the
        checkpoint, so they already read the same on both sides; after the
        edits the head's visible state equals the checkpoint's. With Chronos
        schema branching on (it is off in OKG deployments), a table whose schema
        differs between the checkpoint and the head is refused rather than
        reverted row by row.
        """
        from chronos_core.branching import BranchingError, TableNotRegisteredError

        session = self.context.checkout(branch)
        with session.transaction():
            # Read the head only under the branch write lock, inside the same
            # transaction as the edits, so no writer can move it in between.
            # The restore rewrites edge declarations too (item 92a).
            self._lock_edge_ranking_shared()  # type: ignore[attr-defined]
            self._lock_branch_write(branch)
            head_segment_id = backend._branch_segment_id(branch)
            db = self.context.db
            parent = db.execute(
                "SELECT parent_segment_id FROM _chronos_branch_interval_segments "
                "WHERE segment_id = ?",
                (checkpoint_segment_id,),
            ).fetchone()
            base_segment_id = None if parent is None else parent["parent_segment_id"]
            if base_segment_id is None:
                raise SubstrateError(
                    "cannot restore branch to generation: checkpoint segment has no parent",
                    data={"branch": branch, "generation_id": checkpoint},
                )
            try:
                writer_segment_ids = backend._mutable_segment_ids_after_base(
                    head_segment_id, int(base_segment_id)
                )
            except BranchingError as exc:
                raise SubstrateError(
                    "cannot restore branch to generation: the checkpoint is not on the "
                    "branch head's lineage",
                    data={
                        "branch": branch,
                        "generation_id": checkpoint,
                        "head_segment_id": head_segment_id,
                        "checkpoint_parent_segment_id": int(base_segment_id),
                    },
                    fix_hint=(
                        "the branch was re-rooted after this generation; restore a "
                        "later generation or use `okg branch rollover`"
                    ),
                ) from exc

            checkpoint_segment = backend._segment(checkpoint_segment_id)
            head_segment = backend._segment(head_segment_id)

            def table_meta(segment: Any, table: str) -> Any | None:
                try:
                    return backend._meta_for_segment(segment, table)
                except TableNotRegisteredError:
                    return None

            def rows_by_key(
                table: str, keys: list[dict[str, Any]], segment: Any, meta: Any
            ) -> dict[tuple[Any, ...], dict[str, Any]]:
                # One query per chunk: a composite-key table binds one parameter
                # per key column, and PostgreSQL refuses a statement with more
                # than 65,535 parameters. Same chunking as the backend's own
                # _discard_exact_visible_upserts.
                found: dict[tuple[Any, ...], dict[str, Any]] = {}
                chunk_size = backend._upsert_batch_chunk_size(meta)
                for start in range(0, len(keys), chunk_size):
                    found.update(
                        backend._rows_by_key_for_keys(
                            table, keys[start : start + chunk_size], segment, meta
                        )
                    )
                return found

            # The registry is loaded when the store is built. A table first
            # registered during the landing (a maintained derivation) must be
            # reverted too, so reload it under the lock.
            backend.refresh_registries()
            for table in backend.diff_tables():
                checkpoint_meta = table_meta(checkpoint_segment, table)
                head_meta = table_meta(head_segment, table)
                if checkpoint_meta is None and head_meta is None:
                    continue
                if (
                    checkpoint_meta is None
                    or head_meta is None
                    or checkpoint_meta.physical_name != head_meta.physical_name
                    or checkpoint_meta.pk_columns != head_meta.pk_columns
                    or checkpoint_meta.columns != head_meta.columns
                ):
                    raise SubstrateError(
                        "cannot restore branch to generation: table schema changed "
                        "since the generation",
                        data={
                            "branch": branch,
                            "generation_id": checkpoint,
                            "table": table,
                        },
                    )
                keys = backend._candidate_keys_for_writer_segments(
                    head_meta, writer_segment_ids
                )
                if not keys:
                    continue
                checkpoint_rows = rows_by_key(
                    table, keys, checkpoint_segment, checkpoint_meta
                )
                head_rows = rows_by_key(table, keys, head_segment, head_meta)
                upserts = [
                    row
                    for key, row in checkpoint_rows.items()
                    if head_rows.get(key) != row
                ]
                deletes = [
                    dict(zip(head_meta.pk_columns, key))
                    for key in head_rows
                    if key not in checkpoint_rows
                ]
                session.apply_table_edits(table, upserts=upserts, deletes=deletes)
                if table == NODE_TABLE and upserts:
                    # PACT openrouter-embeddings-hnsw: the restored rows
                    # bypass the graph writer's document links, and their
                    # content may never have been embedded. One link number
                    # per active vector set, in this transaction, keeps the
                    # worker's fill from carrying its last completion past
                    # them. A delete leaves no row to embed.
                    from okg.substrate.embeddings.store import (
                        mark_node_rows_restored,
                    )

                    mark_node_rows_restored(db)

    def edge_declarations_since_generation(
        self,
        generation_id: int | str,
        *,
        branch: str = DEFAULT_BRANCH,
    ) -> list[tuple[dict[str, Any], dict[str, Any] | None]]:
        """Edge declarer rows written on ``branch`` since a generation.

        Returns ``(key, head_row)`` for each ``okg.graph_edge_declarers`` key
        whose head version differs from the generation's (``head_row`` is
        ``None`` when the head deleted it). A write that changes only a
        declaration changes no stored edge row, so the publisher reuses the
        last generation and such a write is not in any checkpoint yet (item
        92a). Merge-back reads these before it lands and puts them back after
        a rollback to the generation (``reapply_edge_declarations``), since the
        sources that wrote them have already moved their watermarks past them.
        """
        from okg.substrate.graph_store_internal.edge_declarers import (
            EDGE_DECLARERS_TABLE,
        )

        return self._table_rows_since_generation(
            generation_id, branch=branch, table=EDGE_DECLARERS_TABLE,
        )

    def edge_deferrals_since_generation(
        self,
        generation_id: int | str,
        *,
        branch: str = DEFAULT_BRANCH,
    ) -> list[tuple[dict[str, Any], dict[str, Any] | None]]:
        """Waiting-edge rows written on ``branch`` since a generation.

        Same contract as ``edge_declarations_since_generation`` for
        ``okg.graph_edge_deferrals``: a source write that changes only a
        waiting row changes no stored edge row, and merge-back puts the rows
        back after a rollback (``reapply_edge_deferrals``).
        """
        from okg.substrate.graph_store_internal.edge_deferrals import (
            EDGE_DEFERRALS_TABLE,
        )

        return self._table_rows_since_generation(
            generation_id, branch=branch, table=EDGE_DEFERRALS_TABLE,
        )

    def reapply_edge_deferrals(
        self,
        rows: list[tuple[dict[str, Any], dict[str, Any] | None]],
        *,
        branch: str = DEFAULT_BRANCH,
    ) -> dict[str, int]:
        """Write back waiting rows read by ``edge_deferrals_since_generation``.

        One transaction under the branch write fence. A row that also landed
        in ``okg.graph_edges`` is removed by the publish step's first pass, so
        no check is needed here.
        """
        from okg.substrate.graph_store_internal.edge_deferrals import (
            EDGE_DEFERRALS_TABLE,
        )

        counts = {"upserted": 0, "deleted": 0}
        if not rows:
            return counts
        session = self.checkout(branch)
        with session.transaction():
            self._lock_edge_ranking_shared()  # type: ignore[attr-defined]
            self._lock_branch_write(branch)  # type: ignore[attr-defined]
            upserts = [row for _key, row in rows if row is not None]
            deletes = [key for key, row in rows if row is None]
            session.apply_table_edits(
                EDGE_DEFERRALS_TABLE, upserts=upserts, deletes=deletes,
            )
        counts["upserted"] = len(upserts)
        counts["deleted"] = len(deletes)
        return counts

    def _table_rows_since_generation(
        self,
        generation_id: int | str,
        *,
        branch: str,
        table: str,
    ) -> list[tuple[dict[str, Any], dict[str, Any] | None]]:
        """Rows of a side table written on ``branch`` since a generation."""
        from chronos_core.branching import TableNotRegisteredError

        backend = getattr(self.context, "_backend", None)
        if backend is None or not hasattr(backend, "_mutable_segment_ids_after_base"):
            return []
        checkpoint = self._generation_checkpoint_id(generation_id)
        db = self.context.db
        cp = db.execute(
            "SELECT segment_id FROM _chronos_branch_interval_checkpoints "
            "WHERE checkpoint_id = ?",
            (checkpoint,),
        ).fetchone()
        if cp is None:
            raise SubstrateError(
                "cannot read declarations since generation: checkpoint not found",
                data={"branch": branch, "generation_id": checkpoint},
            )
        parent = db.execute(
            "SELECT parent_segment_id FROM _chronos_branch_interval_segments "
            "WHERE segment_id = ?",
            (int(cp["segment_id"]),),
        ).fetchone()
        if parent is None or parent["parent_segment_id"] is None:
            raise SubstrateError(
                "cannot read declarations since generation: checkpoint segment "
                "has no parent",
                data={"branch": branch, "generation_id": checkpoint},
            )
        backend.refresh_registries()
        head_segment_id = backend._branch_segment_id(branch)
        checkpoint_segment = backend._segment(int(cp["segment_id"]))
        head_segment = backend._segment(head_segment_id)
        try:
            head_meta = backend._meta_for_segment(head_segment, table)
        except TableNotRegisteredError:
            return []
        try:
            checkpoint_meta = backend._meta_for_segment(
                checkpoint_segment, table,
            )
        except TableNotRegisteredError:
            checkpoint_meta = None
        writer_segment_ids = backend._mutable_segment_ids_after_base(
            head_segment_id, int(parent["parent_segment_id"]),
        )
        keys = backend._candidate_keys_for_writer_segments(
            head_meta, writer_segment_ids,
        )
        if not keys:
            return []

        def rows_at(segment: Any, meta: Any) -> dict[tuple[Any, ...], dict[str, Any]]:
            found: dict[tuple[Any, ...], dict[str, Any]] = {}
            if meta is None:
                return found
            chunk_size = backend._upsert_batch_chunk_size(meta)
            for start in range(0, len(keys), chunk_size):
                found.update(backend._rows_by_key_for_keys(
                    table, keys[start:start + chunk_size],
                    segment, meta,
                ))
            return found

        before = rows_at(checkpoint_segment, checkpoint_meta)
        after = rows_at(head_segment, head_meta)
        out: list[tuple[dict[str, Any], dict[str, Any] | None]] = []
        for key_tuple in sorted(set(before) | set(after), key=repr):
            if before.get(key_tuple) == after.get(key_tuple):
                continue
            key = dict(zip(head_meta.pk_columns, key_tuple))
            out.append((key, after.get(key_tuple)))
        return out

    def reapply_edge_declarations(
        self,
        declarations: list[tuple[dict[str, Any], dict[str, Any] | None]],
        *,
        branch: str = DEFAULT_BRANCH,
    ) -> dict[str, int]:
        """Write back declarer rows read by ``edge_declarations_since_generation``
        and store the winner of every edge they belong to, in one transaction
        under the branch write fence.

        Only a declaration whose two endpoint nodes are live after the
        restore is written back: one whose node the rollback removed (it was
        written after the generation too) would store a dangling edge. Those
        are dropped, counted and logged.
        """
        from okg.substrate.graph_store_internal.edge_declarers import (
            EDGE_DECLARERS_TABLE,
        )

        counts = {
            "declarations": 0,
            "dropped_dangling": 0,
            "edges_rewritten": 0,
            "edges_deleted": 0,
        }
        if not declarations:
            return counts
        session = self.checkout(branch)
        with session.transaction():
            self._lock_edge_ranking_shared()  # type: ignore[attr-defined]
            self._lock_branch_write(branch)  # type: ignore[attr-defined]
            endpoints = sorted({
                str(row[end])
                for _key, row in declarations
                if row is not None
                for end in ("src", "dst")
            })
            live = {
                str(row["node_id"])
                for row in (
                    session.query(
                        "SELECT node_id FROM okg.graph_nodes "
                        "WHERE node_id = ANY(:node_ids)",
                        {"node_ids": endpoints},
                    )
                    if endpoints else []
                )
            }
            upserts = [
                row for _key, row in declarations
                if row is not None
                and str(row["src"]) in live and str(row["dst"]) in live
            ]
            dropped = [
                row for _key, row in declarations
                if row is not None and row not in upserts
            ]
            deletes = [key for key, row in declarations if row is None]
            session.apply_table_edits(
                EDGE_DECLARERS_TABLE, upserts=upserts, deletes=deletes,
            )
            settled = self._settle_ranked_edges(  # type: ignore[attr-defined]
                session,
                {int(row["edge_id"]) for row in upserts}
                | {int(key["edge_id"]) for key in deletes},
            )
        if dropped:
            logging.getLogger(__name__).warning(
                "rollback on %s: %d declarations written since the generation "
                "were not put back because an endpoint node is gone: %s",
                branch,
                len(dropped),
                sorted(
                    f"{row['source']}:{row['src']}-{row['edge_type']}->{row['dst']}"
                    for row in dropped
                )[:20],
            )
        counts.update(settled)
        counts["declarations"] = len(upserts) + len(deletes)
        counts["dropped_dangling"] = len(dropped)
        return counts

    def _branch_head_segment(self, branch: str) -> int:
        row = self.context.db.execute(
            "SELECT current_segment_id FROM _chronos_branch_interval_branches "
            "WHERE branch_id = ?",
            (branch,),
        ).fetchone()
        if row is None:
            raise SubstrateError(
                f"cannot fast-forward: branch not found: {branch}",
                data={"branch": branch},
            )
        return int(row["current_segment_id"])

    def fast_forward(
        self,
        onto_branch: str,
        *,
        target: str = DEFAULT_BRANCH,
        expected_current_segment: int | None = None,
        generation_id: int | str | None = None,
        deployment_name: str = "default",
        catalog_version_id: int | None = None,
        status: str = "published",
        metadata: dict[str, Any] | None = None,
    ) -> str:
        onto_segment = self._branch_head_segment(onto_branch)
        if expected_current_segment is None:
            expected_current_segment = self._branch_head_segment(target)
        checkpoint = self._generation_checkpoint_id(
            generation_id if generation_id is not None else self.new_generation_id(
                deployment_name=deployment_name,
                branch=target,
            )
        )
        payload = {
            "kind": "okg_generation",
            "deployment_name": deployment_name,
            "branch": target,
            "status": status,
            "catalog_version_id": catalog_version_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            **(metadata or {}),
        }
        # Issue #4049: the overlays the generation carries forward are its
        # target branch's latest published generation's (None on a branch
        # with none).
        previous = self.latest_generation(
            deployment_name=deployment_name, branch=target, status="published",
        ) if status == "published" else None
        # The head move and the overlay marker commit together (a savepoint
        # when the caller holds a transaction).
        with self._root_generation_transaction():
            self._record_embedding_watermarks(checkpoint)
            self.context.fast_forward_branch(
                checkpoint, target, onto_segment, expected_current_segment, payload
            )
            if status == "published":
                self._materialize_root_overlays(
                    checkpoint,
                    previous=previous.checkpoint_id if previous is not None else None,
                )
        return checkpoint

    @contextlib.contextmanager
    def _root_generation_transaction(self) -> Iterator[None]:
        """One transaction around a generation minted outside the publisher
        and its overlay writes (issue #4049), so no failure or kill leaves a
        published head without its overlay marker.

        A caller that owns the commit (a Chronos runtime with autocommit off,
        as `okg branch rollover` and the fleet controller hold) gets a
        savepoint inside its transaction and commits it itself. Otherwise,
        as on `okg branch compact`'s autocommit connection, this is the
        transaction: the runtime's own commits are held while it is open,
        as the publisher holds them (publisher.py, the publication
        transaction), it commits at the end, and the backend's after-commit
        or after-rollback hook runs once. On a store that is not PostgreSQL
        (the SQLite test adapter) it adds nothing."""
        from okg.substrate.embeddings.store import _psycopg_connection

        conn = _psycopg_connection(self.context.db)
        if conn is None:
            yield
            return
        if not self.context.autocommit:
            with conn.transaction():
                yield
            return
        committed = False
        self.context.autocommit = False
        try:
            try:
                with conn.transaction():
                    yield
            finally:
                self.context.autocommit = True
            # What the runtime's own commit did when it held no transaction
            # block: an outer implicit transaction commits with this one.
            if self.context.db.in_transaction:
                self.context.db.commit()
            committed = True
        except BaseException:
            if self.context.db.in_transaction:
                self.context.db.rollback()
            raise
        finally:
            self.context.finalize_external_transaction(committed=committed)

    def _materialize_root_overlays(
        self, checkpoint: str, *, previous: str | None,
    ) -> None:
        """Run what a publish runs for overlays on a generation minted
        outside the publisher (a fast-forward, a history compaction root, a
        rollover root): carry the predecessor's accepted overlays forward,
        add the generation's own, write its overlay marker, and fold them
        into the branch's overlay ranges when it has an ordinal (issue #4049,
        PACT lane-s-storage-cutover). The graph rows plus overlays are the
        only read path, and it refuses a generation with no marker, so
        without this every unpinned read refused until the next publish.

        Writes only the per-generation overlay table, the marker and the
        ranges (PACT lane-s-storage-drop-copies dropped the materialized
        copies). No-op on a store that is not
        PostgreSQL (the SQLite test adapter has no attribute tables)."""
        from okg.substrate.attributes import (
            materialize_effective_attributes_for_generation,
            sync_overlay_ranges_for_generation,
        )
        from okg.substrate.embeddings.store import _psycopg_connection

        from psycopg.rows import tuple_row

        conn = _psycopg_connection(self.context.db)
        if conn is None:
            return
        # The attribute writers read rows by position; a caller's connection
        # may hand out dict rows (a test's, or a CLI's), so read tuples here
        # and give the caller its row factory back.
        caller_rows = conn.row_factory
        conn.row_factory = tuple_row
        try:
            materialize_effective_attributes_for_generation(
                conn, generation_id=checkpoint, previous_generation_id=previous,
            )
            sync_overlay_ranges_for_generation(conn, checkpoint)
        finally:
            conn.row_factory = caller_rows

    def _record_embedding_watermarks(self, checkpoint: str) -> None:
        """Write this generation's vector-set watermarks (no-op without sets
        or on a store that is not PostgreSQL)."""
        from okg.substrate.embeddings.store import (
            _psycopg_connection,
            record_watermarks,
        )

        conn = _psycopg_connection(self.context.db)
        if conn is not None:
            record_watermarks(conn, str(checkpoint), recorded_by="publisher")

    def list_generations(
        self,
        *,
        deployment_name: str | None = "default",
        branch: str | None = None,
        status: str | None = "published",
    ):
        """List OKG generation checkpoints.

        ``deployment_name=None`` lists EVERY deployment's generations, which
        is what `okg generation list` needs to honor its documented
        "default: all deployments" behavior (issue #1439). ``branch=None``
        already means every branch.
        """
        metadata_filter: dict[str, Any] = {"kind": "okg_generation"}
        if deployment_name is not None:
            metadata_filter["deployment_name"] = deployment_name
        if status is not None:
            metadata_filter["status"] = status
        checkpoints = self.context.list_checkpoints(
            branch=branch,
            metadata_filter=metadata_filter,
        )
        return sorted(checkpoints, key=lambda cp: (cp.created_at, cp.checkpoint_id))

    def latest_generation(
        self,
        *,
        deployment_name: str = "default",
        branch: str = DEFAULT_BRANCH,
        status: str | None = "published",
    ):
        fast = self._latest_generation_checkpoint_fast(
            deployment_name=deployment_name,
            branch=branch,
            status=status,
        )
        if fast is not None:
            return fast
        generations = self.list_generations(
            deployment_name=deployment_name,
            branch=branch,
            status=status,
        )
        if not generations:
            return None
        return generations[-1]

    def _latest_generation_checkpoint_fast(
        self,
        *,
        deployment_name: str,
        branch: str,
        status: str | None,
        limit: int | None = None,
    ):
        """The newest matching generation checkpoint, read in bounded pages.

        Returns the same checkpoint `list_generations(...)[-1]` would, or
        `None` when no match is found within the scan cap -- and `None` is the
        caller's signal to take the full-fetch path, so the semantics are
        unchanged and only the cost moves.

        This used to read exactly ONE row and give up when it did not match.
        A checkpoint the caller filtered out therefore cost a full fetch of
        every checkpoint's metadata: on okg-workspace the newest checkpoint is
        `status="blocked"` while every caller asks for `"published"`, so the
        fast path returned `None` on every request and the console paid 3.5 s
        twice per read (issue #2202, whose write-up names the `kind` guard --
        all 88 checkpoints there are `okg_generation`; it is the STATUS guard
        that rejects the newest row).

        `limit` caps the total rows scanned; it defaults to the module cap.
        """
        db = getattr(self.context, "db", None)
        if getattr(db, "dialect", None) == "postgres":
            return self._latest_generation_checkpoint_by_version(
                deployment_name=deployment_name, branch=branch, status=status, limit=limit,
            )
        return self._latest_generation_checkpoint_full_rows(
            deployment_name=deployment_name, branch=branch, status=status, limit=limit,
        )

    def _latest_generation_checkpoint_by_version(
        self,
        *,
        deployment_name: str,
        branch: str,
        status: str | None,
        limit: int | None = None,
    ):
        """The fast path on PostgreSQL, reading no metadata it does not return.

        Same answer as `_latest_generation_checkpoint_full_rows` (issue
        #3404): the walk pages (checkpoint_id, xmin) newest first, decides
        each row from its cached summary (reading that row's metadata once
        per row version on a miss), and reads metadata only for the row it
        returns.
        """
        try:
            from chronos_core.branching import CheckpointInfo
        except Exception:  # noqa: BLE001 - non-interval backend
            return None
        db = self.context.db
        cap = _LATEST_GENERATION_SCAN_CAP if limit is None else max(1, int(limit))
        offset = 0
        while offset < cap:
            page = min(_LATEST_GENERATION_SCAN_PAGE, cap - offset)
            try:
                rows = db.execute(
                    """
                    SELECT checkpoint_id, xmin::text AS row_version,
                           current_database() AS database_name,
                           (SELECT oid::text FROM pg_database
                             WHERE datname = current_database()) AS database_oid
                      FROM _chronos_branch_interval_checkpoints
                     WHERE branch_id = ?
                     ORDER BY created_at DESC, checkpoint_id DESC
                     LIMIT ? OFFSET ?
                    """,
                    (branch, int(page), int(offset)),
                ).fetchall()
            except Exception as exc:  # noqa: BLE001 - no checkpoint table / metadata
                # The caller then takes the full fetch (#3404's cost): say so
                # once per process instead of paying it silently.
                _warn_checkpoint_walk_fallback(exc)
                return None
            if not rows:
                return None
            for row in rows:
                key = (
                    str(row["database_oid"]), str(row["database_name"]),
                    str(row["checkpoint_id"]), str(row["row_version"]),
                )
                with _CHECKPOINT_CACHE_LOCK:
                    summary = _CHECKPOINT_SUMMARIES.get(key)
                    if summary is not None:
                        _CHECKPOINT_SUMMARIES.move_to_end(key)
                    text = _CHECKPOINT_TEXTS.get(key)
                if summary is None:
                    full = self._checkpoint_row(row["checkpoint_id"])
                    if full is None:
                        return None
                    key = key[:3] + (str(full["row_version"]),)
                    text = full["metadata"] or "{}"
                    try:
                        metadata = json.loads(text)
                    except Exception:  # noqa: BLE001 - malformed metadata; fall back
                        return None
                    summary = (
                        metadata.get("kind"), metadata.get("deployment_name"),
                        metadata.get("status"), full["branch_id"],
                        full["segment_id"], full["created_at"],
                    )
                    with _CHECKPOINT_CACHE_LOCK:
                        _CHECKPOINT_SUMMARIES[key] = summary
                        while len(_CHECKPOINT_SUMMARIES) > _CHECKPOINT_SUMMARY_CAP:
                            _CHECKPOINT_SUMMARIES.popitem(last=False)
                kind, row_deployment, row_status, branch_id, segment_id, created_at = summary
                if kind != "okg_generation":
                    continue
                if row_deployment != deployment_name:
                    continue
                if status is not None and row_status != status:
                    continue
                if text is None:
                    full = self._checkpoint_row(row["checkpoint_id"])
                    if full is None or str(full["row_version"]) != key[3]:
                        # Changed since the walk read it: the full path decides.
                        return None
                    text = full["metadata"] or "{}"
                _remember_checkpoint_text(key, text)
                try:
                    metadata = json.loads(text)
                except Exception:  # noqa: BLE001 - malformed metadata; fall back
                    return None
                # A fresh parse per call: callers own the returned mapping.
                return CheckpointInfo(
                    row["checkpoint_id"], branch_id, segment_id, created_at, metadata,
                )
            if len(rows) < page:
                return None
            offset += page
        return None

    def _checkpoint_row(self, checkpoint_id: Any):
        """One checkpoint row with its metadata and row version, or None."""
        try:
            return self.context.db.execute(
                """
                SELECT checkpoint_id, xmin::text AS row_version, branch_id,
                       segment_id, created_at, metadata
                  FROM _chronos_branch_interval_checkpoints
                 WHERE checkpoint_id = ?
                """,
                (checkpoint_id,),
            ).fetchone()
        except Exception:  # noqa: BLE001 - row gone or unreadable: fall back
            return None

    def _latest_generation_checkpoint_full_rows(
        self,
        *,
        deployment_name: str,
        branch: str,
        status: str | None,
        limit: int | None = None,
    ):
        """The fast path on other backends: pages of whole rows."""
        try:
            from chronos_core.branching import CheckpointInfo
        except Exception:  # noqa: BLE001 - non-interval backend
            return None
        cap = _LATEST_GENERATION_SCAN_CAP if limit is None else max(1, int(limit))
        offset = 0
        while offset < cap:
            page = min(_LATEST_GENERATION_SCAN_PAGE, cap - offset)
            try:
                rows = self.context.db.execute(
                    """
                    SELECT checkpoint_id, branch_id, segment_id, created_at, metadata
                      FROM _chronos_branch_interval_checkpoints
                     WHERE branch_id = ?
                     ORDER BY created_at DESC, checkpoint_id DESC
                     LIMIT ? OFFSET ?
                    """,
                    (branch, int(page), int(offset)),
                ).fetchall()
            except Exception:  # noqa: BLE001 - non-interval backend / no metadata
                return None
            if not rows:
                return None
            for row in rows:
                try:
                    metadata = json.loads(row["metadata"] or "{}")
                except Exception:  # noqa: BLE001 - malformed metadata; fall back
                    return None
                if metadata.get("kind") != "okg_generation":
                    continue
                if metadata.get("deployment_name") != deployment_name:
                    continue
                if status is not None and metadata.get("status") != status:
                    continue
                return CheckpointInfo(
                    row["checkpoint_id"],
                    row["branch_id"],
                    row["segment_id"],
                    row["created_at"],
                    metadata,
                )
            if len(rows) < page:
                return None
            offset += page
        return None

    def recent_generation_enricher_runs(
        self,
        *,
        deployment_name: str,
        branch: str = DEFAULT_BRANCH,
        limit: int = DEFAULT_ENRICHER_RUNS_WINDOW,
    ) -> list[GenerationEnricherRuns]:
        """The newest `limit` generation checkpoints' enricher runs, newest first.

        ONE bounded statement, projecting only the enricher-run list and the
        two metadata fields a run row prints (`status` and
        `target_generation_id`). It exists so the console never calls
        `list_generations` for run history: that call fetches every
        checkpoint's full metadata, which is the 39 MB the console used to
        move 19 times per request (issue #2202).

        The window is a BOUND, not a claim of completeness. Callers must serve
        it by name so the page can say how far back it looked; an enricher
        that last ran before the window simply has no run here, and silently
        printing that as "never ran" would be an invention.

        Raises nothing for a missing backend: a store whose checkpoint table
        this cannot read returns an empty list, and the caller decides what an
        empty run history means.
        """
        window = max(1, int(limit))
        dialect = getattr(getattr(self.context, "db", None), "dialect", None)
        if dialect == "postgres":
            rows = self._recent_enricher_run_rows_projected(
                deployment_name=deployment_name, branch=branch, window=window
            )
            if rows is not None:
                return rows
        return self._recent_enricher_run_rows_scanned(
            deployment_name=deployment_name, branch=branch, window=window
        )

    def _recent_enricher_run_rows_projected(
        self,
        *,
        deployment_name: str,
        branch: str,
        window: int,
    ) -> list[GenerationEnricherRuns] | None:
        """Postgres: bound the scan, then parse each scanned row exactly once.

        Two costs were hiding in the obvious shape of this query, both
        measured on okg-workspace (88 checkpoints, 131 MB of metadata):

        * a `WHERE (metadata::jsonb) ->> 'kind' = ...` predicate makes the
          LIMIT useless -- the filter has to parse EVERY document on the
          branch before the limit can pick 20 of them (2.48 s);
        * each additional `(metadata::json) ->> 'key'` parses the whole
          document AGAIN -- one key costs 0.67 s over this branch, five cost
          2.9 s.

        So the LIMIT moved into a subquery that only names the `metadata`
        column, and the projection became ONE `json_to_record` per row. The
        kind/deployment filter now runs in Python over each scanned page, so
        this still returns the newest `window` MATCHING checkpoints -- the
        semantics the composer already had -- and stops at the module scan
        cap. 4.09 s -> 0.05 s for the console's window of 20 (issue #2202).

        Returns None -- "not this backend" -- rather than an empty list, so
        the caller takes the portable path instead of reading a missing table
        as a deployment that never ran an enricher.
        """
        out: list[GenerationEnricherRuns] = []
        offset = 0
        while len(out) < window and offset < _LATEST_GENERATION_SCAN_CAP:
            page = min(
                max(window, _LATEST_GENERATION_SCAN_PAGE),
                _LATEST_GENERATION_SCAN_CAP - offset,
            )
            try:
                rows = self.context.db.execute(
                    """
                    SELECT t.checkpoint_id, t.created_at,
                           m."kind", m."deployment_name", m."status",
                           m."target_generation_id", m."enricher_runs",
                           m."timing_details"
                      FROM (
                            SELECT checkpoint_id, created_at, metadata
                              FROM _chronos_branch_interval_checkpoints
                             WHERE branch_id = ?
                             ORDER BY created_at DESC, checkpoint_id DESC
                             LIMIT ? OFFSET ?
                           ) AS t,
                           LATERAL json_to_record(t.metadata::json)
                                AS m("kind" json, "deployment_name" json,
                                     "status" json,
                                     "target_generation_id" json,
                                     "enricher_runs" json,
                                     "timing_details" json)
                     ORDER BY t.created_at DESC, t.checkpoint_id DESC
                    """,
                    (branch, int(page), int(offset)),
                ).fetchall()
            except Exception:  # noqa: BLE001 - table absent / not this backend
                return None if offset == 0 else out
            if not rows:
                return out
            for row in rows:
                if row["kind"] != "okg_generation":
                    continue
                if row["deployment_name"] != deployment_name:
                    continue
                timing = row["timing_details"]
                out.append(GenerationEnricherRuns(
                    str(row["checkpoint_id"]),
                    row["created_at"],
                    {
                        "status": row["status"],
                        "target_generation_id": row["target_generation_id"],
                        "enricher_runs": _as_run_list(row["enricher_runs"]),
                        "timing_details": {
                            "enrichers": _as_run_list(
                                timing.get("enrichers")
                                if isinstance(timing, dict) else None
                            ),
                        },
                    },
                ))
                if len(out) >= window:
                    return out
            if len(rows) < page:
                return out
            offset += page
        return out

    def _recent_enricher_run_rows_scanned(
        self,
        *,
        deployment_name: str,
        branch: str,
        window: int,
    ) -> list[GenerationEnricherRuns]:
        """Any backend: page the newest checkpoints and filter in Python.

        Still bounded -- it stops at `window` matches or at the scan cap --
        but it moves each scanned checkpoint's full metadata, so the Postgres
        projection above is the path that matters on a real deployment.
        """
        out: list[GenerationEnricherRuns] = []
        offset = 0
        while len(out) < window and offset < _LATEST_GENERATION_SCAN_CAP:
            page = min(
                _LATEST_GENERATION_SCAN_PAGE, _LATEST_GENERATION_SCAN_CAP - offset
            )
            try:
                rows = self.context.db.execute(
                    """
                    SELECT checkpoint_id, created_at, metadata
                      FROM _chronos_branch_interval_checkpoints
                     WHERE branch_id = ?
                     ORDER BY created_at DESC, checkpoint_id DESC
                     LIMIT ? OFFSET ?
                    """,
                    (branch, int(page), int(offset)),
                ).fetchall()
            except Exception:  # noqa: BLE001 - no checkpoint table on this backend
                return out
            if not rows:
                return out
            for row in rows:
                try:
                    metadata = json.loads(row["metadata"] or "{}")
                except Exception:  # noqa: BLE001 - malformed metadata; skip the row
                    continue
                if metadata.get("kind") != "okg_generation":
                    continue
                if metadata.get("deployment_name") != deployment_name:
                    continue
                out.append(
                    GenerationEnricherRuns(
                        str(row["checkpoint_id"]),
                        row["created_at"],
                        {
                            "status": metadata.get("status"),
                            "target_generation_id": metadata.get(
                                "target_generation_id"
                            ),
                            "enricher_runs": _as_run_list(
                                metadata.get("enricher_runs")
                            ),
                            "timing_details": {
                                "enrichers": _as_run_list(
                                    (metadata.get("timing_details") or {}).get(
                                        "enrichers"
                                    )
                                ),
                            },
                        },
                    )
                )
                if len(out) >= window:
                    return out
            if len(rows) < page:
                return out
            offset += page
        return out

    def recent_generations(
        self,
        *,
        deployment_name: str,
        branch: str = DEFAULT_BRANCH,
        status: str | None = None,
        keys: tuple[str, ...] | None = None,
        scan_window: int | None = None,
        since: str | None = None,
    ) -> "RecentGenerations":
        """The newest generation checkpoints, bounded, projected, newest first.

        ONE statement. It reads the newest `scan_window` checkpoints on the
        branch and projects ONLY `RECENT_GENERATION_METADATA_KEYS` out of each
        one's metadata, then applies `list_generations`'s own filter -- kind,
        deployment, optional status -- to what it scanned.

        Why it exists: `list_generations` has no LIMIT and no projection, so
        it moves every checkpoint's FULL metadata document. On okg-workspace
        that is 88 rows and 131 MB, 1.65 s per call, for a console panel that
        prints twelve rows (issue #2202). Projecting key by key is not enough
        on its own -- each `(metadata::json) ->> 'x'` re-parses the whole
        document, so six keys cost six parses -- which is why the projection
        goes through one `json_to_record` per row and the LIMIT sits in a
        subquery that never touches `metadata`.

        The bound is served, not hidden: the result carries `scanned`,
        `scan_window` and `window_exhausted`, and a caller that prints a total
        or offers another page must print that flag too.

        `since` is an optional lower bound on the `created_at` COLUMN, for a
        caller that wants a span rather than a count (the attempts chart).
        The column is text and the whole store already orders it lexically --
        `_latest_generation_checkpoint_fast` and `list_generations` both do --
        so a caller passing `since` must pass a stamp with slack for a
        checkpoint written in another UTC offset, and must still filter
        exactly in Python. It narrows the SCAN; it does not decide the answer.

        Returns rows with projected metadata only. A caller that needs a
        checkpoint's full metadata document must use `list_generations` or
        `get_generation`; this reader never silently falls back to either.
        """
        window = max(1, int(
            DEFAULT_GENERATION_SCAN_WINDOW if scan_window is None else scan_window
        ))
        projected = _generation_projection(keys)
        dialect = getattr(getattr(self.context, "db", None), "dialect", None)
        scanned_rows: list[GenerationRow] | None = None
        if dialect == "postgres":
            scanned_rows = self._recent_generation_rows_projected(
                branch=branch, window=window, since=since, keys=projected,
            )
        if scanned_rows is None:
            scanned_rows = self._recent_generation_rows_scanned(
                branch=branch, window=window, since=since, keys=projected,
            )
        rows = [
            row for row in scanned_rows
            if row.metadata.get("kind") == "okg_generation"
            and row.metadata.get("deployment_name") == deployment_name
            and (status is None or row.metadata.get("status") == status)
        ]
        return RecentGenerations(
            rows,
            scanned=len(scanned_rows),
            scan_window=window,
            window_exhausted=len(scanned_rows) >= window,
        )


    def _recent_generation_rows_projected(
        self,
        *,
        branch: str,
        window: int,
        since: str | None,
        keys: tuple[str, ...],
    ) -> list[GenerationRow] | None:
        """Postgres: bound the scan, then parse each scanned row exactly once.

        The LIMIT lives in a subquery whose only metadata reference is the
        column itself, so the planner never parses a document outside the
        window. The LATERAL `json_to_record` is what makes it ONE parse per
        row: projecting the keys individually off `metadata::json` costs one
        full parse of the document PER KEY (measured: 0.67 s for one key over
        this branch, 2.9 s for five).

        Returns None -- "not this backend" -- rather than an empty list, so
        the caller can take the portable path instead of reading a missing
        table as an empty history.
        """
        predicate = "branch_id = ?"
        params: list[Any] = [branch]
        if since is not None:
            predicate += " AND created_at >= ?"
            params.append(since)
        params.append(int(window))
        try:
            rows = self.context.db.execute(
                f"""
                SELECT t.checkpoint_id, t.branch_id, t.segment_id, t.created_at,
                       {", ".join(f'm."{key}"' for key in keys)}
                  FROM (
                        SELECT checkpoint_id, branch_id, segment_id,
                               created_at, metadata
                          FROM _chronos_branch_interval_checkpoints
                         WHERE {predicate}
                         ORDER BY created_at DESC, checkpoint_id DESC
                         LIMIT ?
                       ) AS t,
                       LATERAL json_to_record(t.metadata::json)
                            AS m({", ".join(f'"{key}" json' for key in keys)})
                 ORDER BY t.created_at DESC, t.checkpoint_id DESC
                """,
                tuple(params),
            ).fetchall()
        except Exception:  # noqa: BLE001 - table absent / not this backend
            return None
        return [
            GenerationRow(
                str(row["checkpoint_id"]),
                row["branch_id"],
                row["segment_id"],
                row["created_at"],
                {key: row[key] for key in keys},
            )
            for row in rows
        ]

    def _recent_generation_rows_scanned(
        self,
        *,
        branch: str,
        window: int,
        since: str | None,
        keys: tuple[str, ...],
    ) -> list[GenerationRow]:
        """Any backend: read the newest `window` checkpoints and project here.

        Still bounded by the same window, but it moves each scanned
        checkpoint's whole metadata document, so the Postgres projection above
        is the path that matters on a real deployment.
        """
        out: list[GenerationRow] = []
        predicate = "branch_id = ?"
        base: list[Any] = [branch]
        if since is not None:
            predicate += " AND created_at >= ?"
            base.append(since)
        offset = 0
        while offset < window:
            page = min(_LATEST_GENERATION_SCAN_PAGE, window - offset)
            try:
                rows = self.context.db.execute(
                    f"""
                    SELECT checkpoint_id, branch_id, segment_id, created_at,
                           metadata
                      FROM _chronos_branch_interval_checkpoints
                     WHERE {predicate}
                     ORDER BY created_at DESC, checkpoint_id DESC
                     LIMIT ? OFFSET ?
                    """,
                    (*base, int(page), int(offset)),
                ).fetchall()
            except Exception:  # noqa: BLE001 - no checkpoint table on this backend
                return out
            if not rows:
                return out
            for row in rows:
                try:
                    metadata = json.loads(row["metadata"] or "{}")
                except Exception:  # noqa: BLE001 - malformed metadata; skip
                    metadata = {}
                out.append(
                    GenerationRow(
                        str(row["checkpoint_id"]),
                        row["branch_id"],
                        row["segment_id"],
                        row["created_at"],
                        {key: metadata.get(key) for key in keys},
                    )
                )
            if len(rows) < page:
                return out
            offset += page
        return out

    def count_generation_checkpoints(
        self,
        *,
        deployment_name: str,
        branch: str = DEFAULT_BRANCH,
    ) -> int | None:
        """How many generation checkpoints this branch holds, without parsing.

        Counts by the generation checkpoint id PREFIX
        (``okg:<deployment>:branch:<branch>:gen:``) that
        `new_generation_id` mints, which is the same basis
        `compose_published_versions` already used to scope its version list to
        one deployment and branch. It exists so a bounded list read can still
        serve an EXACT total: the alternative -- a count filtered on
        ``metadata->>'kind'`` -- parses every document on the branch, which is
        the cost the bounded reader was added to remove.

        Returns None when the backend has no such table, so a caller says
        "unknown" rather than printing a zero it did not measure.
        """
        prefix = f"okg:{deployment_name}:branch:{branch}:gen:"
        try:
            rows = self.context.db.execute(
                """
                SELECT count(*) AS total
                  FROM _chronos_branch_interval_checkpoints
                 WHERE branch_id = ?
                   AND checkpoint_id LIKE ?
                """,
                (branch, prefix.replace("%", r"\%").replace("_", r"\_") + "%"),
            ).fetchall()
        except Exception:  # noqa: BLE001 - table absent / not this backend
            return None
        if not rows:
            return None
        return int(rows[0]["total"])

    def count_generations(
        self,
        *,
        deployment_name: str,
        branch: str = DEFAULT_BRANCH,
    ) -> int | None:
        """EXACTLY `len(list_generations(deployment_name, branch, status=None))`.

        One statement, and it parses a metadata document only where the
        checkpoint id cannot answer. A checkpoint minted by `new_generation_id`
        carries its deployment and branch in its id
        (``okg:<deployment>:branch:<branch>:gen:``) and `checkpoint_generation`
        stamps it ``kind: okg_generation`` -- compaction strips a source's
        `kind` and `deployment_name` before re-stamping -- so those rows are
        counted by id alone. Every OTHER checkpoint on the branch -- a
        generation minted with an explicit integer id (``generation:<n>``),
        another deployment's, or a non-generation checkpoint -- is parsed and
        filtered the way `list_generations` filters it. On okg-workspace that
        remainder is empty, so the count touches no metadata at all; it is
        the knowledge overview's `total_generations` (issue #2113), which used
        to fetch every checkpoint's full metadata to take a `len()`.

        Unlike `count_generation_checkpoints`, whose prefix-only basis the
        generations panel names in its payload, this count needs no basis
        label: it is the same number the full fetch produced.

        Returns None when the backend has no such table or cannot parse JSON
        in SQL, so the caller takes its own named path rather than printing a
        zero nobody measured.
        """
        dialect = getattr(getattr(self.context, "db", None), "dialect", None)
        if dialect != "postgres":
            return None
        prefix = f"okg:{deployment_name}:branch:{branch}:gen:"
        pattern = (
            prefix.replace("\\", "\\\\").replace("%", r"\%").replace("_", r"\_")
            + "%"
        )
        try:
            rows = self.context.db.execute(
                r"""
                SELECT
                  (SELECT count(*)
                     FROM _chronos_branch_interval_checkpoints
                    WHERE branch_id = ?
                      AND checkpoint_id LIKE ? ESCAPE '\')
                  +
                  (SELECT count(*)
                     FROM (
                           SELECT metadata
                             FROM _chronos_branch_interval_checkpoints
                            WHERE branch_id = ?
                              AND checkpoint_id NOT LIKE ? ESCAPE '\'
                          ) AS t,
                          LATERAL json_to_record(t.metadata::json)
                               AS m("kind" text, "deployment_name" text)
                    WHERE m."kind" = 'okg_generation'
                      AND m."deployment_name" = ?)
                  AS total
                """,
                (branch, pattern, branch, pattern, deployment_name),
            ).fetchall()
        except Exception:  # noqa: BLE001 - table absent / not this backend
            return None
        if not rows:
            return None
        return int(rows[0]["total"])

    def count_undated_checkpoints(self, *, branch: str = DEFAULT_BRANCH) -> int | None:
        """Checkpoints on the branch whose `created_at` is not ISO-8601 dated.

        A branch-wide fact the attempts chart prints, kept exact while the row
        read that feeds the chart is bounded by `created_at`: a checkpoint
        whose stamp cannot be read cannot be trusted to sort into any window,
        so a windowed scan must not be the thing that counts it.

        Touches no metadata. Returns None when the backend has no such table,
        so the caller says "not counted" rather than printing a measured zero.
        """
        try:
            rows = self.context.db.execute(
                """
                SELECT count(*) AS total
                  FROM _chronos_branch_interval_checkpoints
                 WHERE branch_id = ?
                   AND (created_at IS NULL
                        OR created_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T')
                """,
                (branch,),
            ).fetchall()
        except Exception:  # noqa: BLE001 - table absent / no regex operator
            return None
        if not rows:
            return None
        return int(rows[0]["total"])

    @staticmethod
    def new_generation_id(*, deployment_name: str, branch: str) -> str:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        return f"okg:{deployment_name}:branch:{branch}:gen:{stamp}:{uuid.uuid4().hex[:12]}"

    @staticmethod
    def _generation_checkpoint_id(generation_id: int | str) -> str:
        text = str(generation_id)
        if text.startswith("okg:") or text.startswith("generation:"):
            return text
        return f"generation:{text}"
