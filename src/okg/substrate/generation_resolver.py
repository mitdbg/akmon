"""Shared Chronos OKG generation resolution.

Operator surfaces should agree on the public generation id. After the Chronos
cutover that public id is the checkpoint id string; the integer
``target_generation_id`` is compatibility metadata for legacy side tables.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import psycopg


DEFAULT_BRANCH = "default"


@dataclass(frozen=True)
class ResolvedGeneration:
    generation_id: str
    deployment_name: str | None
    branch: str | None
    status: str | None
    catalog_version_id: int | None
    target_generation_id: int | None
    created_at: Any
    metadata: dict[str, Any]
    node_count: int | None = None
    edge_count: int | None = None


def resolve_okg_generation(
    conn: psycopg.Connection,
    *,
    deployment_name: str | None,
    requested: int | str | None = None,
    branch: str | None = DEFAULT_BRANCH,
    status: str | None = "published",
    include_counts: bool = False,
    allow_default_branch_fallback: bool = False,
) -> ResolvedGeneration | None:
    """Resolve the current or explicitly requested OKG generation.

    ``requested=None`` resolves the latest published checkpoint for the
    deployment/branch. Explicit requests may be either the Chronos checkpoint id
    string or the compat integer target id. Non-default branches can optionally
    read through to their default-branch base generation, matching MCP session
    behavior for freshly forked read-only branches.

    ``branch=None`` means "any branch" and is valid only with an explicit
    ``requested``: it resolves a checkpoint by id without constraining which
    branch published it, which is what `okg generation describe` needs to
    describe every id `okg generation list` returns (issue #1439). Resolving
    the LATEST generation has no meaning without a branch, so that
    combination is refused rather than silently read as ``default``.
    """
    if branch is None and requested is None:
        raise ValueError(
            "resolve_okg_generation: branch=None (any branch) requires an "
            "explicit `requested` generation; the latest generation is "
            "per-branch and has no cross-branch meaning"
        )

    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    checkpoint = None
    if requested is None:
        assert branch is not None  # guarded above
        checkpoint = store.latest_generation(
            deployment_name=deployment_name or "default",
            branch=branch,
            status=status,
        )
        if (
            checkpoint is None
            and allow_default_branch_fallback
            and branch != DEFAULT_BRANCH
        ):
            checkpoint = _default_branch_fallback(
                store,
                deployment_name=deployment_name or "default",
                branch=branch,
                status=status,
            )
    else:
        checkpoint = store.resolve_generation(
            requested,
            deployment_name=deployment_name,
            branch=branch,
            status=status,
        )

    if checkpoint is None and requested is not None:
        from okg.substrate.errors import GenerationCompacted
        from okg.substrate.history_compaction import compacted_generation_record

        tombstone = compacted_generation_record(
            conn,
            requested,
            deployment_name=deployment_name,
        )
        if tombstone is not None:
            raise GenerationCompacted(
                f"generation {requested!r} was retired by history compaction",
                data=tombstone,
                fix_hint="choose a retained generation from `okg generation list`",
            )
    if checkpoint is None:
        return None
    metadata = dict(checkpoint.metadata or {})
    if not _metadata_matches(
        metadata,
        deployment_name=deployment_name,
        branch=branch,
        status=status,
        branch_required=branch is not None and requested is not None,
    ):
        return None

    catalog_version_id = _required_catalog_version_id(
        metadata.get("catalog_version_id"),
        generation_id=str(checkpoint.checkpoint_id),
        deployment_name=metadata.get("deployment_name"),
        branch=metadata.get("branch"),
    )

    node_count = None
    edge_count = None
    if include_counts:
        graph = store.checkout_generation(checkpoint.checkpoint_id)
        node_count = int(
            graph.query("SELECT COUNT(*) AS n FROM okg.graph_nodes")[0]["n"]
        )
        edge_count = int(
            graph.query("SELECT COUNT(*) AS n FROM okg.graph_edges")[0]["n"]
        )

    return ResolvedGeneration(
        generation_id=str(checkpoint.checkpoint_id),
        deployment_name=metadata.get("deployment_name"),
        branch=metadata.get("branch"),
        status=metadata.get("status"),
        catalog_version_id=catalog_version_id,
        target_generation_id=_target_generation_id(metadata, requested),
        created_at=checkpoint.created_at,
        metadata=metadata,
        node_count=node_count,
        edge_count=edge_count,
    )


def chronos_generation_inventory(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    checkpoint_id: str | None = None,
) -> dict[str, int | bool]:
    """Count authoritative OKG checkpoints across deployment identities."""
    from okg.substrate.graph_store import ChronosGraphStore

    store = ChronosGraphStore.from_connection(conn, ensure_schema=False)
    checkpoints = store.context.list_checkpoints(
        branch=None,
        metadata_filter={"kind": "okg_generation"},
    )
    matching = sum(
        1
        for checkpoint in checkpoints
        if dict(checkpoint.metadata or {}).get("deployment_name") == deployment_name
    )
    total = len(checkpoints)
    lineage = conn.execute(
        """
        SELECT branch_id, ordinal, previous_checkpoint_id, deployment_name
          FROM okg.generation_ordinals
         WHERE checkpoint_id = %s
        """,
        (checkpoint_id or "",),
    ).fetchone()
    try:
        root_lineage = (
            lineage is not None
            and lineage[0] == "default"
            and int(lineage[1]) == 0
            and lineage[2] is None
            and lineage[3] == deployment_name
        )
    except (IndexError, TypeError, ValueError, OverflowError):
        root_lineage = False
    return {
        "total": total,
        "matching": matching,
        "foreign": total - matching,
        "root_lineage": root_lineage,
    }


def _default_branch_fallback(
    store: Any,
    *,
    deployment_name: str,
    branch: str,
    status: str | None,
) -> Any | None:
    try:
        info = store.context.get_branch(branch)
        fork_md = info.metadata or {}
    except Exception:  # noqa: BLE001 - branch metadata is optional context.
        return None
    base_gen_id = fork_md.get("base_generation_id")
    if not base_gen_id or fork_md.get("base_branch") != DEFAULT_BRANCH:
        return None
    try:
        candidate = store.get_generation(base_gen_id)
    except Exception:  # noqa: BLE001 - missing base generation.
        return None
    metadata = dict(candidate.metadata or {})
    if not _metadata_matches(
        metadata,
        deployment_name=deployment_name,
        branch=DEFAULT_BRANCH,
        status=status,
        branch_required=True,
    ):
        return None
    return candidate


def _metadata_matches(
    metadata: dict[str, Any],
    *,
    deployment_name: str | None,
    branch: str | None,
    status: str | None,
    branch_required: bool,
) -> bool:
    if metadata.get("kind") != "okg_generation":
        return False
    if (
        deployment_name is not None
        and metadata.get("deployment_name") != deployment_name
    ):
        return False
    if status is not None and metadata.get("status") != status:
        return False
    metadata_branch = metadata.get("branch")
    if (
        branch_required
        and metadata_branch is not None
        and str(metadata_branch) != branch
    ):
        return False
    return True


def _target_generation_id(
    metadata: dict[str, Any],
    requested: int | str | None,
) -> int | None:
    raw = metadata.get("target_generation_id")
    value = _optional_int(raw)
    if value is not None:
        return value
    if requested is not None:
        return _optional_int(requested)
    return None


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value)
    if not text.isdigit():
        return None
    return int(text)


def _required_catalog_version_id(
    value: Any,
    *,
    generation_id: str,
    deployment_name: Any,
    branch: Any,
) -> int:
    catalog_version_id = _optional_int(value)
    if catalog_version_id is not None and catalog_version_id > 0:
        return catalog_version_id

    from okg.substrate.errors import GenerationPinInvalid

    raise GenerationPinInvalid(
        "generation checkpoint has missing or invalid catalog_version_id; "
        "refusing to substitute global catalog authority",
        data={
            "generation_id": generation_id,
            "deployment_name": deployment_name,
            "branch": branch,
            "catalog_version_id": value,
        },
        fix_hint=(
            "repair or republish this checkpoint with the exact catalog_version_id "
            "used by its deployment"
        ),
    )
