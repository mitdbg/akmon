"""Exact published-generation authority for PACT task completion.

This is an explicit, task-local alternative to the shared PACT graph
projection.  It is deliberately fail closed: once ``graph_authority`` is
present in a task's method metadata, callers must use this module's verdict
and must never fall back to the projection path.
"""

from __future__ import annotations

from dataclasses import dataclass
import errno
import hashlib
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import stat
from typing import Any

from okg.substrate.pact import envelope
from okg.substrate.pact.evidence_kinds import load_registry


AUTHORITY_KIND = "published_generation_evidence"
_AUTHORITY_KEYS = {
    "kind",
    "deployment",
    "branch",
    "canonical_generation_id",
    "compatibility_generation_id",
    "catalog_version_id",
    "nodes_live",
    "edges_live",
    "artifacts",
}
_ARTIFACT_KEYS = {"path", "sha256"}
_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]*$")
_GENERATION_RE = re.compile(r"^okg:[A-Za-z0-9._:/-]+:gen:[A-Za-z0-9._:/-]+$")
_SHA256_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_ARTIFACT_PATH_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/-]*$")


@dataclass(frozen=True)
class AuthorityIssue:
    code: str
    message: str
    path: str
    offending_value: Any = None


@dataclass(frozen=True)
class ArtifactPin:
    path: str
    sha256: str


@dataclass(frozen=True)
class PublishedGenerationAuthority:
    deployment: str
    branch: str
    canonical_generation_id: str
    compatibility_generation_id: int
    catalog_version_id: int
    nodes_live: int
    edges_live: int
    artifacts: tuple[ArtifactPin, ...]


@dataclass(frozen=True)
class ExactPublishedGeneration:
    """One checkpoint and its physically counted graph inside one snapshot."""

    generation_id: str
    deployment_name: str
    branch: str
    status: str
    target_generation_id: int
    catalog_version_id: int
    node_count: int
    edge_count: int


def task_declares_published_generation_authority(task: Any) -> bool:
    """Presence is the opt-in boundary, including malformed declarations."""
    metadata = getattr(task, "method_metadata", None)
    return isinstance(metadata, dict) and "graph_authority" in metadata


def parse_published_generation_authority(
    task: Any,
    *,
    task_path: str,
    target_pact_version: int,
) -> tuple[PublishedGenerationAuthority | None, list[AuthorityIssue]]:
    """Parse one exact authority declaration without consulting live state."""
    metadata = getattr(task, "method_metadata", None)
    if not isinstance(metadata, dict) or "graph_authority" not in metadata:
        return None, []
    path = f"{task_path}.method_metadata.graph_authority"
    raw = metadata.get("graph_authority")
    issues: list[AuthorityIssue] = []
    if target_pact_version < 4:
        issues.append(
            AuthorityIssue(
                "published_generation_authority_requires_v4",
                "published_generation_evidence authority requires target PACT v4",
                path,
            )
        )
    if not isinstance(raw, dict):
        return None, [
            *issues,
            AuthorityIssue(
                "invalid_published_generation_authority",
                "graph_authority must be a mapping",
                path,
                raw,
            ),
        ]

    keys = set(raw)
    missing = sorted(_AUTHORITY_KEYS - keys)
    unknown = sorted(keys - _AUTHORITY_KEYS)
    if missing:
        issues.append(
            AuthorityIssue(
                "incomplete_published_generation_authority",
                "graph_authority is missing required fields: " + ", ".join(missing),
                path,
                missing,
            )
        )
    if unknown:
        issues.append(
            AuthorityIssue(
                "unknown_published_generation_authority_field",
                "graph_authority has unknown fields: " + ", ".join(unknown),
                path,
                unknown,
            )
        )
    if raw.get("kind") != AUTHORITY_KIND:
        issues.append(
            AuthorityIssue(
                "unknown_published_generation_authority_kind",
                f"graph_authority.kind must be {AUTHORITY_KIND!r}",
                f"{path}.kind",
                raw.get("kind"),
            )
        )

    deployment = _safe_name(raw.get("deployment"), allow_slash=False)
    branch = _safe_name(raw.get("branch"), allow_slash=True)
    generation = raw.get("canonical_generation_id")
    if deployment is None:
        issues.append(_string_issue(path, "deployment", raw.get("deployment")))
    if branch is None:
        issues.append(_string_issue(path, "branch", raw.get("branch")))
    if (
        not isinstance(generation, str)
        or not _GENERATION_RE.fullmatch(generation)
        or ".." in generation
    ):
        issues.append(
            AuthorityIssue(
                "invalid_published_generation_authority_value",
                "canonical_generation_id must be a canonical okg:...:gen:... checkpoint string",
                f"{path}.canonical_generation_id",
                generation,
            )
        )

    integer_values: dict[str, int] = {}
    for field, minimum in (
        ("compatibility_generation_id", 1),
        ("catalog_version_id", 1),
        ("nodes_live", 0),
        ("edges_live", 0),
    ):
        value = raw.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            issues.append(
                AuthorityIssue(
                    "invalid_published_generation_authority_value",
                    f"{field} must be an integer >= {minimum}",
                    f"{path}.{field}",
                    value,
                )
            )
        else:
            integer_values[field] = value

    artifacts_raw = raw.get("artifacts")
    artifacts: list[ArtifactPin] = []
    if not isinstance(artifacts_raw, list) or not artifacts_raw:
        issues.append(
            AuthorityIssue(
                "empty_published_generation_authority_artifacts",
                "graph_authority.artifacts must be a non-empty list",
                f"{path}.artifacts",
                artifacts_raw,
            )
        )
    else:
        for index, row in enumerate(artifacts_raw):
            row_path = f"{path}.artifacts[{index}]"
            if not isinstance(row, dict):
                issues.append(
                    AuthorityIssue(
                        "invalid_published_generation_authority_artifact",
                        "artifact pin must be a mapping",
                        row_path,
                        row,
                    )
                )
                continue
            row_keys = set(row)
            if row_keys != _ARTIFACT_KEYS:
                issues.append(
                    AuthorityIssue(
                        "invalid_published_generation_authority_artifact",
                        "artifact pin must contain exactly path and sha256",
                        row_path,
                        sorted(row_keys),
                    )
                )
            artifact_path = row.get("path")
            digest = row.get("sha256")
            if not _safe_artifact_path(artifact_path):
                issues.append(
                    AuthorityIssue(
                        "invalid_published_generation_authority_artifact_path",
                        "artifact path must be a normalized repository-relative POSIX path",
                        f"{row_path}.path",
                        artifact_path,
                    )
                )
            if not isinstance(digest, str) or not _SHA256_RE.fullmatch(digest):
                issues.append(
                    AuthorityIssue(
                        "invalid_published_generation_authority_artifact_digest",
                        "artifact sha256 must use sha256:<64 lowercase hex> form",
                        f"{row_path}.sha256",
                        digest,
                    )
                )
            if (
                _safe_artifact_path(artifact_path)
                and isinstance(digest, str)
                and _SHA256_RE.fullmatch(digest)
            ):
                artifacts.append(ArtifactPin(path=artifact_path, sha256=digest))

        paths = [row.path for row in artifacts]
        if len(paths) != len(set(paths)):
            issues.append(
                AuthorityIssue(
                    "duplicate_published_generation_authority_artifact",
                    "graph_authority artifact paths must be unique",
                    f"{path}.artifacts",
                    paths,
                )
            )
        if paths != sorted(paths):
            issues.append(
                AuthorityIssue(
                    "unsorted_published_generation_authority_artifacts",
                    "graph_authority artifacts must be sorted by path",
                    f"{path}.artifacts",
                    paths,
                )
            )

    if issues:
        return None, issues
    return PublishedGenerationAuthority(
        deployment=deployment,  # type: ignore[arg-type]
        branch=branch,  # type: ignore[arg-type]
        canonical_generation_id=generation,  # type: ignore[arg-type]
        compatibility_generation_id=integer_values["compatibility_generation_id"],
        catalog_version_id=integer_values["catalog_version_id"],
        nodes_live=integer_values["nodes_live"],
        edges_live=integer_values["edges_live"],
        artifacts=tuple(artifacts),
    ), []


def verify_published_generation_authority(
    repo_root: Path,
    manifest: Any,
    task: Any,
    *,
    demanding_requirements: list[str],
) -> dict[str, Any]:
    """Authenticate declared evidence, bytes, and one exact live checkpoint."""
    target_version = manifest.target_pact_version or manifest.pact_version
    declaration, declaration_issues = parse_published_generation_authority(
        task,
        task_path=f"tasks[{task.id}]",
        target_pact_version=target_version,
    )
    if declaration is None:
        blockers = [issue.code for issue in declaration_issues] or [
            "invalid_published_generation_authority"
        ]
        return _refusal(
            "invalid",
            blockers,
            detail={
                "declaration_issues": [issue.__dict__ for issue in declaration_issues]
            },
        )

    describe = _describe_command(declaration)
    blockers: list[str] = []
    detail: dict[str, Any] = {
        "deployment": declaration.deployment,
        "branch": declaration.branch,
        "canonical_generation_id": declaration.canonical_generation_id,
        "compatibility_generation_id": declaration.compatibility_generation_id,
        "catalog_version_id": declaration.catalog_version_id,
        "nodes_live": declaration.nodes_live,
        "edges_live": declaration.edges_live,
        "artifacts": [row.__dict__ for row in declaration.artifacts],
    }

    required = set(demanding_requirements)
    if not required:
        blockers.append("published_generation_authority_vacuous")
    registry = load_registry(repo_root)
    graph_kinds = {"graph_report", "published_generation"}
    relevant = [
        row
        for row in manifest.evidence
        if row.status == "pass"
        and registry.canonicalize(row.kind) in graph_kinds
        and bool(required.intersection(row.refs))
    ]
    covered = {ref for row in relevant for ref in row.refs if ref in required}
    for requirement_id in sorted(required - covered):
        blockers.append(f"published_generation_evidence_missing:{requirement_id}")
    if not relevant and required:
        blockers.append("published_generation_evidence_empty")

    declared_artifacts = {row.path for row in declaration.artifacts}
    used_artifacts: set[str] = set()
    for row in relevant:
        if row.generation_id != declaration.compatibility_generation_id:
            blockers.append(
                f"published_generation_evidence_generation_mismatch:{row.id}"
            )
        if not row.artifact or row.artifact not in declared_artifacts:
            blockers.append(
                f"published_generation_evidence_artifact_undeclared:{row.id}"
            )
        else:
            used_artifacts.add(row.artifact)
        data = row.data if isinstance(row.data, dict) else {}
        for key, expected in (
            ("canonical_generation_id", declaration.canonical_generation_id),
            ("compatibility_generation_id", declaration.compatibility_generation_id),
            ("deployment", declaration.deployment),
            ("deployment_name", declaration.deployment),
            ("branch", declaration.branch),
        ):
            if key in data and data[key] != expected:
                blockers.append(
                    f"published_generation_evidence_identity_mismatch:{row.id}:{key}"
                )
    for path in sorted(declared_artifacts - used_artifacts):
        blockers.append(f"published_generation_authority_artifact_unused:{path}")

    artifact_results: list[dict[str, Any]] = []
    for pin in declaration.artifacts:
        captured = _capture_artifact(repo_root, pin)
        artifact_results.append(captured)
        if not captured["ok"]:
            blockers.append(str(captured["blocker"]))
    detail["artifact_checks"] = artifact_results
    if blockers:
        return _refusal(
            "evidence_mismatch",
            blockers,
            detail=detail,
            recommended_commands=[describe],
        )

    live = _resolve_exact_published_generation(repo_root, declaration)
    if not live.get("ok"):
        return _refusal(
            str(live.get("status") or "unavailable"),
            list(
                live.get("blockers") or ["published_generation_authority_unavailable"]
            ),
            detail={**detail, "live": live.get("detail") or {}},
            recommended_commands=list(live.get("recommended_commands") or [describe]),
        )

    resolved = live["generation"]
    comparisons = {
        "canonical_generation_id": (
            resolved.generation_id,
            declaration.canonical_generation_id,
        ),
        "deployment": (resolved.deployment_name, declaration.deployment),
        "branch": (resolved.branch, declaration.branch),
        "status": (resolved.status, "published"),
        "compatibility_generation_id": (
            resolved.target_generation_id,
            declaration.compatibility_generation_id,
        ),
        "catalog_version_id": (
            resolved.catalog_version_id,
            declaration.catalog_version_id,
        ),
        "nodes_live": (resolved.node_count, declaration.nodes_live),
        "edges_live": (resolved.edge_count, declaration.edges_live),
    }
    for field, (actual, expected) in comparisons.items():
        if actual != expected:
            blockers.append(f"published_generation_authority_mismatch:{field}")
    detail["live"] = {
        "canonical_generation_id": resolved.generation_id,
        "deployment": resolved.deployment_name,
        "branch": resolved.branch,
        "status": resolved.status,
        "compatibility_generation_id": resolved.target_generation_id,
        "catalog_version_id": resolved.catalog_version_id,
        "nodes_live": resolved.node_count,
        "edges_live": resolved.edge_count,
    }
    if blockers:
        return _refusal(
            "checkpoint_mismatch",
            blockers,
            detail=detail,
            recommended_commands=[describe],
        )
    return envelope.component(
        ok=True,
        status="fresh",
        stale=False,
        authority_scope=AUTHORITY_KIND,
        projected_generation_id=declaration.compatibility_generation_id,
        canonical_generation_id=declaration.canonical_generation_id,
        blockers=[],
        detail=detail,
        recommended_commands=[],
    )


def _resolve_exact_published_generation(
    repo_root: Path,
    declaration: PublishedGenerationAuthority,
) -> dict[str, Any]:
    """Resolve the declared checkpoint read-only; never selects ``latest``."""
    from okg.substrate.deployment_authority import (
        deployment_self_hosted_authoritative,
        resolve_deployment_dsn_authority,
    )

    describe = _describe_command(declaration)
    try:
        resolution = resolve_deployment_dsn_authority(
            declaration.deployment,
            explicit_dsn=None,
            local_profile_authoritative=deployment_self_hosted_authoritative(
                declaration.deployment
            ),
        )
        # A deployment repository can itself be the named deployment: its
        # canonical manifest is ``<repo>/deployment.yaml``, not necessarily
        # ``deployments/<name>/deployment.yaml``. The generic named resolver
        # intentionally does not auto-detect that shape. When (and only when)
        # it found no authority at all, strictly load this repository's
        # regular manifest, require the exact declared name, and feed its DSN
        # back through deployment authority as an explicit target.
        if resolution.get("dsn") is None and not any(
            key in resolution
            for key in ("reason", "next_action", "authority", "target")
        ):
            repo_dsn = _repo_manifest_dsn(repo_root, declaration.deployment)
            if repo_dsn is not None:
                resolution = resolve_deployment_dsn_authority(
                    declaration.deployment,
                    explicit_dsn=repo_dsn,
                    local_profile_authoritative=False,
                )
    except Exception as exc:  # noqa: BLE001 - authority failure must refuse.
        return envelope.component(
            ok=False,
            status="deployment_unavailable",
            blockers=["published_generation_deployment_authority_unavailable"],
            detail={"error": type(exc).__name__},
            recommended_commands=[_deployment_command(declaration), describe],
        )
    dsn = resolution.get("dsn")
    if not isinstance(dsn, str) or not dsn:
        return envelope.component(
            ok=False,
            status="deployment_unavailable",
            blockers=["published_generation_deployment_authority_unavailable"],
            detail={
                "reason": resolution.get("reason") or "deployment DSN unresolved",
                "next_action": resolution.get("next_action"),
                "authority": resolution.get("authority"),
            },
            recommended_commands=[_deployment_command(declaration), describe],
        )
    try:
        from okg.substrate.db.connection import connect as db_connect

        with db_connect(
            dsn,
            autocommit=False,
            connect_timeout=5,
            # This server-side default protects the boundary even if a future
            # read helper is added before SET TRANSACTION below.
            options="-c default_transaction_read_only=on",
        ) as conn:
            conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            result = _select_exact_published_generation(conn, declaration)
            # End the snapshot without committing anything. A read-only commit
            # would also be non-mutating, but rollback makes the operational
            # boundary visible and mechanically testable.
            conn.rollback()
    except Exception as exc:  # noqa: BLE001 - every read failure must refuse.
        return envelope.component(
            ok=False,
            status="checkpoint_unavailable",
            blockers=["published_generation_checkpoint_unavailable"],
            detail={"error": type(exc).__name__},
            recommended_commands=[describe],
        )
    if not result.get("ok"):
        return {
            **result,
            "recommended_commands": [describe],
        }
    return result


def _select_exact_published_generation(
    conn: Any,
    declaration: PublishedGenerationAuthority,
) -> dict[str, Any]:
    """SELECT one checkpoint and count its interval rows without a lease.

    The caller establishes a repeatable-read, read-only transaction. This
    function intentionally does not call Chronos checkout/resolver helpers:
    historical checkout acquires a persisted read lease. Direct interval
    visibility at the checkpoint's immutable branch point gives the same live
    counts while remaining SELECT-only.
    """
    rows = conn.execute(
        """
        SELECT c.checkpoint_id,
               c.branch_id,
               c.created_at,
               c.metadata::jsonb,
               s.branch_point
          FROM public._chronos_branch_interval_checkpoints AS c
          JOIN public._chronos_branch_interval_segments AS s
            ON s.segment_id = c.segment_id
         WHERE c.checkpoint_id = %s
           AND c.branch_id = %s
        """,
        (
            declaration.canonical_generation_id,
            declaration.branch,
        ),
    ).fetchall()
    if not rows:
        compacted = conn.execute(
            """
            SELECT checkpoint_id, target_generation_id,
                   deployment_name, branch_id
              FROM okg.compacted_generations
             WHERE checkpoint_id = %s
               AND deployment_name = %s
               AND branch_id = %s
            """,
            (
                declaration.canonical_generation_id,
                declaration.deployment,
                declaration.branch,
            ),
        ).fetchall()
        if compacted:
            return envelope.component(
                ok=False,
                status="checkpoint_unavailable",
                blockers=["published_generation_checkpoint_compacted"],
                detail={},
            )
        return envelope.component(
            ok=False,
            status="checkpoint_missing",
            blockers=["published_generation_checkpoint_missing"],
            detail={},
        )
    if len(rows) != 1:
        return envelope.component(
            ok=False,
            status="checkpoint_ambiguous",
            blockers=["published_generation_checkpoint_ambiguous"],
            detail={"checkpoint_rows": len(rows)},
        )

    checkpoint_id, branch_id, _created_at, metadata_raw, branch_point = rows[0]
    metadata = metadata_raw if isinstance(metadata_raw, dict) else None
    if metadata is None:
        return _checkpoint_metadata_refusal("metadata")

    actual_identity = {
        "canonical_generation_id": str(checkpoint_id),
        "deployment": metadata.get("deployment_name"),
        "branch": metadata.get("branch"),
        "checkpoint_branch": str(branch_id),
        "status": metadata.get("status"),
        "kind": metadata.get("kind"),
    }
    expected_identity = {
        "canonical_generation_id": declaration.canonical_generation_id,
        "deployment": declaration.deployment,
        "branch": declaration.branch,
        "checkpoint_branch": declaration.branch,
        "status": "published",
        "kind": "okg_generation",
    }
    identity_mismatches = [
        field
        for field, expected in expected_identity.items()
        if actual_identity[field] != expected
    ]
    if identity_mismatches:
        return envelope.component(
            ok=False,
            status="checkpoint_mismatch",
            blockers=[
                f"published_generation_checkpoint_metadata_mismatch:{field}"
                for field in identity_mismatches
            ],
            detail={"identity": actual_identity},
        )

    numeric_fields = {
        "compatibility_generation_id": (
            metadata.get("target_generation_id"),
            1,
        ),
        "catalog_version_id": (metadata.get("catalog_version_id"), 1),
        "nodes_live": (metadata.get("node_count"), 0),
        "edges_live": (metadata.get("edge_count"), 0),
    }
    parsed: dict[str, int] = {}
    for field, (raw_value, minimum) in numeric_fields.items():
        value = _checkpoint_int(raw_value, minimum=minimum)
        if value is None:
            return _checkpoint_metadata_refusal(field)
        parsed[field] = value

    graph_tables = conn.execute(
        """
        SELECT table_name, physical_table
          FROM public._chronos_branch_tables
         WHERE table_name IN ('okg.graph_nodes', 'okg.graph_edges')
           AND backend = 'interval'
         ORDER BY table_name
        """
    ).fetchall()
    if [str(row[0]) for row in graph_tables] != [
        "okg.graph_edges",
        "okg.graph_nodes",
    ]:
        return envelope.component(
            ok=False,
            status="checkpoint_unavailable",
            blockers=["published_generation_graph_interval_tables_invalid"],
            detail={"tables": [str(row[0]) for row in graph_tables]},
        )

    physical_names = [str(row[1]) for row in graph_tables]
    physical_relations = conn.execute(
        """
        SELECT c.relname, c.relkind
          FROM pg_catalog.pg_class AS c
          JOIN pg_catalog.pg_namespace AS n
            ON n.oid = c.relnamespace
         WHERE n.nspname = 'public'
           AND c.relname = ANY(%s)
         ORDER BY c.relname
        """,
        (physical_names,),
    ).fetchall()
    relation_kinds = {str(name): str(kind) for name, kind in physical_relations}
    if (
        len(relation_kinds) != 2
        or set(relation_kinds) != set(physical_names)
        or any(kind not in {"r", "p"} for kind in relation_kinds.values())
    ):
        return envelope.component(
            ok=False,
            status="checkpoint_unavailable",
            blockers=["published_generation_graph_physical_tables_invalid"],
            detail={"physical_relations": relation_kinds},
        )

    from psycopg import sql

    live_counts: dict[str, int] = {}
    for logical_name, physical_name in graph_tables:
        counted = conn.execute(
            sql.SQL(
                """
                SELECT COUNT(*)
                  FROM {physical}
                 WHERE live_lo <= %s
                   AND %s < live_hi
                   AND deleted = FALSE
                """
            ).format(
                physical=sql.Identifier("public", str(physical_name)),
            ),
            (branch_point, branch_point),
        ).fetchone()
        if counted is None:
            return envelope.component(
                ok=False,
                status="checkpoint_unavailable",
                blockers=["published_generation_graph_count_unavailable"],
                detail={"table": str(logical_name)},
            )
        count = _checkpoint_int(counted[0], minimum=0)
        if count is None:
            return envelope.component(
                ok=False,
                status="checkpoint_unavailable",
                blockers=["published_generation_graph_count_invalid"],
                detail={"table": str(logical_name)},
            )
        live_counts[str(logical_name)] = count

    stamped_counts = {
        "okg.graph_nodes": parsed["nodes_live"],
        "okg.graph_edges": parsed["edges_live"],
    }
    drift = [
        logical_name
        for logical_name, expected in stamped_counts.items()
        if live_counts.get(logical_name) != expected
    ]
    if drift:
        return envelope.component(
            ok=False,
            status="checkpoint_mismatch",
            blockers=[
                "published_generation_checkpoint_count_stamp_mismatch:" + logical_name
                for logical_name in sorted(drift)
            ],
            detail={
                "stamped_counts": stamped_counts,
                "live_counts": live_counts,
            },
        )

    resolved = ExactPublishedGeneration(
        generation_id=str(checkpoint_id),
        deployment_name=str(metadata["deployment_name"]),
        branch=str(metadata["branch"]),
        status=str(metadata["status"]),
        target_generation_id=parsed["compatibility_generation_id"],
        catalog_version_id=parsed["catalog_version_id"],
        node_count=live_counts["okg.graph_nodes"],
        edge_count=live_counts["okg.graph_edges"],
    )
    return envelope.component(ok=True, status="resolved", generation=resolved)


def _checkpoint_int(value: Any, *, minimum: int) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and value.isdigit():
        parsed = int(value)
    else:
        return None
    return parsed if parsed >= minimum else None


def _checkpoint_metadata_refusal(field: str) -> dict[str, Any]:
    return envelope.component(
        ok=False,
        status="checkpoint_mismatch",
        blockers=[f"published_generation_checkpoint_metadata_invalid:{field}"],
        detail={},
    )


def _repo_manifest_dsn(repo_root: Path, deployment: str) -> str | None:
    manifest_path = repo_root.resolve() / "deployment.yaml"
    try:
        if (
            not stat.S_ISREG(manifest_path.lstat().st_mode)
            or manifest_path.is_symlink()
        ):
            return None
    except OSError:
        return None
    from okg.substrate.deployments import (
        DeploymentManifest,
        require_postgres_dsn,
    )

    manifest = DeploymentManifest.from_yaml(manifest_path)
    if manifest.name != deployment:
        return None
    return require_postgres_dsn(manifest, deployment=deployment)


def _capture_artifact(repo_root: Path, pin: ArtifactPin) -> dict[str, Any]:
    """Hash one regular file without following any path-component symlink."""
    root = repo_root.resolve()
    parts = PurePosixPath(pin.path).parts
    fds: list[int] = []
    try:
        root_fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        fds.append(root_fd)
        directory_fd = root_fd
        for component in parts[:-1]:
            directory_fd = os.open(
                component,
                os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0),
                dir_fd=directory_fd,
            )
            fds.append(directory_fd)
        file_fd = os.open(
            parts[-1],
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=directory_fd,
        )
        fds.append(file_fd)
        file_stat = os.fstat(file_fd)
        if not stat.S_ISREG(file_stat.st_mode):
            return _artifact_refusal(pin.path, "not_regular")
        digest = hashlib.sha256()
        while True:
            chunk = os.read(file_fd, 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
        observed = f"sha256:{digest.hexdigest()}"
        if observed != pin.sha256:
            return {
                **_artifact_refusal(pin.path, "digest_mismatch"),
                "expected_sha256": pin.sha256,
                "observed_sha256": observed,
            }
        return envelope.component(
            ok=True,
            path=pin.path,
            sha256=observed,
            bytes=file_stat.st_size,
        )
    except FileNotFoundError:
        return _artifact_refusal(pin.path, "missing")
    except NotADirectoryError:
        return _artifact_refusal(pin.path, "not_regular")
    except OSError as exc:
        reason = (
            "symlink" if getattr(exc, "errno", None) == errno.ELOOP else "unreadable"
        )
        return _artifact_refusal(pin.path, reason)
    finally:
        for fd in reversed(fds):
            try:
                os.close(fd)
            except OSError:
                pass


def _artifact_refusal(path: str, reason: str) -> dict[str, Any]:
    return envelope.component(
        ok=False,
        path=path,
        reason=reason,
        blocker=f"published_generation_artifact_{reason}:{path}",
    )


def _refusal(
    status: str,
    blockers: list[str],
    *,
    detail: dict[str, Any] | None = None,
    recommended_commands: list[str] | None = None,
) -> dict[str, Any]:
    return envelope.component(
        ok=False,
        status=status,
        stale=None,
        authority_scope=AUTHORITY_KIND,
        projected_generation_id=None,
        blockers=list(dict.fromkeys(blockers)),
        detail=detail or {},
        recommended_commands=recommended_commands or [],
    )


def _safe_name(value: Any, *, allow_slash: bool) -> str | None:
    if not isinstance(value, str) or not value or not _NAME_RE.fullmatch(value):
        return None
    if ".." in value or (not allow_slash and "/" in value):
        return None
    return value


def _safe_artifact_path(value: Any) -> bool:
    if (
        not isinstance(value, str)
        or not value
        or "\\" in value
        or not _ARTIFACT_PATH_RE.fullmatch(value)
    ):
        return False
    path = PurePosixPath(value)
    return (
        not path.is_absolute()
        and path.parts
        and all(part not in {"", ".", ".."} for part in path.parts)
        and str(path) == value
    )


def _string_issue(path: str, field: str, value: Any) -> AuthorityIssue:
    return AuthorityIssue(
        "invalid_published_generation_authority_value",
        f"{field} must be a non-empty shell-safe identifier",
        f"{path}.{field}",
        value,
    )


def _describe_command(declaration: PublishedGenerationAuthority) -> str:
    return (
        "uv run okg generation describe "
        f"{shlex.quote(declaration.canonical_generation_id)} --deployment "
        f"{shlex.quote(declaration.deployment)} --json"
    )


def _deployment_command(declaration: PublishedGenerationAuthority) -> str:
    return f"uv run okg deployment env {shlex.quote(declaration.deployment)} --json"
