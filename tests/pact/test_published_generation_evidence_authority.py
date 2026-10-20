from __future__ import annotations

import hashlib
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from okg.substrate.pact import operations as pact_operations
from okg.substrate.pact import published_generation_authority as authority_mod
from okg.substrate.pact.models import PactManifest
from okg.substrate.pact.operations import task_completion_gate
from okg.substrate.pact.store import save_manifest
from okg.substrate.pact.validation import validate_manifest


GENERATION = "okg:private-demo:branch:default:gen:20260829T000000000000Z:abcdef123456"
COMPATIBILITY_ID = 1788004279459363


def _digest(data: bytes) -> str:
    return f"sha256:{hashlib.sha256(data).hexdigest()}"


def _authority(artifact: str, digest: str) -> dict[str, Any]:
    return {
        "kind": "published_generation_evidence",
        "deployment": "private-demo",
        "branch": "default",
        "canonical_generation_id": GENERATION,
        "compatibility_generation_id": COMPATIBILITY_ID,
        "catalog_version_id": 7,
        "nodes_live": 62606,
        "edges_live": 21329,
        "artifacts": [{"path": artifact, "sha256": digest}],
    }


def _document(
    artifact: str,
    digest: str,
    *,
    graph_authority: Any = ...,
    evidence: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    task: dict[str, Any] = {
        "id": "task.demo",
        "title": "Prove the private generation",
        "status": "todo",
        "verification": "verify the exact checkpoint",
        "requirements": ["req.demo"],
    }
    if graph_authority is ...:
        graph_authority = _authority(artifact, digest)
    if graph_authority is not None:
        task["method_metadata"] = {"graph_authority": graph_authority}
    return {
        "pact_version": 4,
        "target_pact_version": 4,
        "change": {
            "id": "demo",
            "title": "Demo",
            "status": "implementation",
            "tier": 1,
        },
        "requirements": [
            {
                "id": "req.demo",
                "statement": "One exact private generation is published.",
                "evidence_policy": {"required": ["graph_report"]},
            }
        ],
        "tasks": [task],
        "evidence": evidence
        if evidence is not None
        else [
            {
                "id": "ev.graph-report.demo",
                "kind": "graph_report",
                "status": "pass",
                "refs": ["req.demo"],
                "artifact": artifact,
                "generation_id": COMPATIBILITY_ID,
                "data": {"canonical_generation_id": GENERATION},
            }
        ],
    }


def _resolved(**overrides: Any) -> SimpleNamespace:
    values = {
        "generation_id": GENERATION,
        "deployment_name": "private-demo",
        "branch": "default",
        "status": "published",
        "target_generation_id": COMPATIBILITY_ID,
        "catalog_version_id": 7,
        "node_count": 62606,
        "edge_count": 21329,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class _Result:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self.rows

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.rows[0] if self.rows else None


class _ReadOnlyCheckpointConnection:
    def __init__(
        self,
        *,
        checkpoint_present: bool = True,
        compacted: bool = False,
        node_count: int = 62606,
        edge_count: int = 21329,
    ) -> None:
        self.checkpoint_present = checkpoint_present
        self.compacted = compacted
        self.node_count = node_count
        self.edge_count = edge_count
        self.statements: list[tuple[str, Any]] = []
        self.rollbacks = 0

    def __enter__(self) -> _ReadOnlyCheckpointConnection:
        return self

    def __exit__(self, *_args: Any) -> None:
        return None

    def rollback(self) -> None:
        self.rollbacks += 1

    def execute(self, query: Any, params: Any = None) -> _Result:
        sql_text = query if isinstance(query, str) else query.as_string()
        normalized = " ".join(sql_text.split())
        self.statements.append((normalized, params))
        if normalized.startswith("SET TRANSACTION"):
            return _Result([])
        if "FROM public._chronos_branch_interval_checkpoints AS c" in normalized:
            if normalized.upper().count(" WHERE ") != 1:
                raise AssertionError(
                    "checkpoint lookup must contain exactly one WHERE clause: "
                    f"{normalized}"
                )
            if not self.checkpoint_present:
                return _Result([])
            return _Result(
                [
                    (
                        GENERATION,
                        "default",
                        "2026-08-29T00:00:00Z",
                        {
                            "kind": "okg_generation",
                            "deployment_name": "private-demo",
                            "branch": "default",
                            "status": "published",
                            "target_generation_id": COMPATIBILITY_ID,
                            "catalog_version_id": 7,
                            "node_count": 62606,
                            "edge_count": 21329,
                        },
                        140585331,
                    )
                ]
            )
        if "FROM okg.compacted_generations" in normalized:
            return _Result(
                [(GENERATION, COMPATIBILITY_ID, "private-demo", "default")]
                if self.compacted
                else []
            )
        if "FROM public._chronos_branch_tables" in normalized:
            return _Result(
                [
                    ("okg.graph_edges", "chronos_graph_edges"),
                    ("okg.graph_nodes", "chronos_graph_nodes"),
                ]
            )
        if "FROM pg_catalog.pg_class AS c" in normalized:
            return _Result(
                [
                    ("chronos_graph_edges", "r"),
                    ("chronos_graph_nodes", "r"),
                ]
            )
        if 'FROM "public"."chronos_graph_edges"' in normalized:
            return _Result([(self.edge_count,)])
        if 'FROM "public"."chronos_graph_nodes"' in normalized:
            return _Result([(self.node_count,)])
        raise AssertionError(f"unexpected SQL: {normalized}")


def _seed(
    root: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    document: dict[str, Any] | None = None,
    live: dict[str, Any] | None = None,
) -> PactManifest:
    artifact = "reports/private-generation.yaml"
    artifact_bytes = b"exact private publication receipt\n"
    artifact_path = root / artifact
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    artifact_path.write_bytes(artifact_bytes)
    document = document or _document(artifact, _digest(artifact_bytes))
    manifest = PactManifest.model_validate(document)
    (root / "pact").mkdir(parents=True, exist_ok=True)
    save_manifest(root, manifest)
    monkeypatch.setattr(
        pact_operations,
        "_workflow_ledger_authority",
        lambda _root: {"fresh": True, "status": "available"},
    )
    monkeypatch.setattr(
        authority_mod,
        "_resolve_exact_published_generation",
        lambda _root, _declaration: (
            live
            or {
                "ok": True,
                "status": "resolved",
                "generation": _resolved(),
            }
        ),
    )
    return manifest


def test_exact_retained_generation_passes_without_consulting_projection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed(tmp_path, monkeypatch)

    def projection_must_not_run(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("shared PACT projection was consulted")

    monkeypatch.setattr(
        pact_operations,
        "_workflow_graph_projection_status",
        projection_must_not_run,
    )

    result = task_completion_gate(tmp_path, "demo", "task.demo")

    assert result["ok"] is True
    workflow = result["workflow_authority"]
    assert workflow["accepted"] is True
    assert workflow["graph_projection"] == {
        "status": "not_consulted",
        "stale": None,
        "projected_generation_id": None,
    }
    private = workflow["published_generation_authority"]
    assert private["status"] == "fresh"
    assert private["detail"]["live"]["canonical_generation_id"] == GENERATION


def test_opt_in_accepts_published_generation_policy_without_expanding_legacy_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact = "reports/private-generation.yaml"
    artifact_bytes = b"exact private publication receipt\n"
    document = _document(artifact, _digest(artifact_bytes))
    document["requirements"][0]["evidence_policy"] = {
        "required": ["published_generation"]
    }
    document["evidence"][0]["kind"] = "published_generation"
    _seed(tmp_path, monkeypatch, document=document)
    monkeypatch.setattr(
        pact_operations,
        "_workflow_graph_projection_status",
        lambda *_args: pytest.fail("shared projection fallback"),
    )

    result = task_completion_gate(tmp_path, "demo", "task.demo")

    assert result["ok"] is True
    assert result["workflow_authority"]["graph_demanding_requirements"] == ["req.demo"]


def test_live_resolver_requests_the_exact_retained_checkpoint_not_latest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg

    from okg.substrate import deployment_authority

    declaration, issues = authority_mod.parse_published_generation_authority(
        SimpleNamespace(
            method_metadata={
                "graph_authority": _authority(
                    "reports/private-generation.yaml",
                    f"sha256:{'0' * 64}",
                )
            }
        ),
        task_path="tasks[0]",
        target_pact_version=4,
    )
    assert declaration is not None
    assert issues == []
    monkeypatch.setattr(
        deployment_authority,
        "deployment_self_hosted_authoritative",
        lambda _deployment: False,
    )
    monkeypatch.setattr(
        deployment_authority,
        "resolve_deployment_dsn_authority",
        lambda deployment, **_kwargs: {
            "dsn": "postgresql://127.0.0.1/private",
            "authority": {"deployment": deployment},
        },
    )
    secret = "env-backed-password-must-not-leak"
    monkeypatch.setenv("PGUSER", "postgres")
    monkeypatch.setenv("PGPASSWORD", secret)

    connection = _ReadOnlyCheckpointConnection()
    connect_calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []

    def connect(*args: Any, **kwargs: Any) -> _ReadOnlyCheckpointConnection:
        assert os.environ["PGUSER"] == "postgres"
        assert os.environ["PGPASSWORD"] == secret
        assert secret not in repr((args, kwargs))
        connect_calls.append((args, kwargs))
        return connection

    monkeypatch.setattr(psycopg, "connect", connect)

    result = authority_mod._resolve_exact_published_generation(
        tmp_path,
        declaration,
    )

    assert result["ok"] is True
    assert secret not in repr(result)
    assert result["generation"] == authority_mod.ExactPublishedGeneration(
        generation_id=GENERATION,
        deployment_name="private-demo",
        branch="default",
        status="published",
        target_generation_id=COMPATIBILITY_ID,
        catalog_version_id=7,
        node_count=62606,
        edge_count=21329,
    )
    assert len(connect_calls) == 1
    connect_args, connect_kwargs = connect_calls[0]
    assert connect_args == ("postgresql://127.0.0.1/private",)
    assert connect_kwargs["autocommit"] is False
    assert connect_kwargs["connect_timeout"] == 5
    # The read-only default still reaches libpq, now APPENDED to the settings
    # the connection factory applies rather than replacing them -- an
    # `options` keyword passed straight to psycopg.connect would drop the
    # idle-in-transaction bound.
    assert connect_kwargs["options"].endswith(
        "-c default_transaction_read_only=on"
    )
    assert "idle_in_transaction_session_timeout" in connect_kwargs["options"]
    assert connect_kwargs["keepalives"] == 1
    assert connection.rollbacks == 1
    checkpoint_statement = next(
        row
        for row in connection.statements
        if "_chronos_branch_interval_checkpoints" in row[0]
    )
    assert checkpoint_statement[0].upper().count(" WHERE ") == 1
    assert "WHERE c.checkpoint_id = %s AND c.branch_id = %s" in checkpoint_statement[0]
    assert checkpoint_statement[1] == (GENERATION, "default")

    def failed_connect(*_args: Any, **_kwargs: Any) -> _ReadOnlyCheckpointConnection:
        raise RuntimeError(f"connection failed with {secret}")

    monkeypatch.setattr(psycopg, "connect", failed_connect)
    refusal = authority_mod._resolve_exact_published_generation(tmp_path, declaration)
    assert refusal["ok"] is False
    assert refusal["blockers"] == ["published_generation_checkpoint_unavailable"]
    assert refusal["detail"] == {"error": "RuntimeError"}
    assert secret not in repr(refusal)


def test_exact_checkpoint_snapshot_executes_no_mutation_or_lease_function(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The private authority is mechanically SELECT-only after its SET."""
    import psycopg

    from okg.substrate import deployment_authority

    declaration, issues = authority_mod.parse_published_generation_authority(
        SimpleNamespace(
            method_metadata={
                "graph_authority": _authority(
                    "reports/private-generation.yaml",
                    f"sha256:{'0' * 64}",
                )
            }
        ),
        task_path="tasks[0]",
        target_pact_version=4,
    )
    assert declaration is not None
    assert issues == []
    monkeypatch.setattr(
        deployment_authority,
        "deployment_self_hosted_authoritative",
        lambda _deployment: False,
    )
    monkeypatch.setattr(
        deployment_authority,
        "resolve_deployment_dsn_authority",
        lambda *_args, **_kwargs: {"dsn": "postgresql://authority.invalid/private"},
    )
    connection = _ReadOnlyCheckpointConnection()
    monkeypatch.setattr(psycopg, "connect", lambda *_args, **_kwargs: connection)

    result = authority_mod._resolve_exact_published_generation(tmp_path, declaration)

    assert result["ok"] is True
    assert connection.statements[0][0] == (
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
    )
    forbidden_tokens = (
        "INSERT ",
        "UPDATE ",
        "DELETE ",
        "MERGE ",
        "CALL ",
        "acquire_history_generation_read_lease",
        "release_history_generation_read_lease",
        "checkout_generation",
    )
    for statement, _params in connection.statements[1:]:
        assert statement.startswith("SELECT ")
        assert not any(token.lower() in statement.lower() for token in forbidden_tokens)
    count_statements = [
        row for row in connection.statements if row[0].startswith("SELECT COUNT(*)")
    ]
    assert len(count_statements) == 2
    assert all(row[1] == (140585331, 140585331) for row in count_statements)
    assert connection.rollbacks == 1


def test_exact_checkpoint_snapshot_refuses_physical_count_stamp_drift() -> None:
    declaration, issues = authority_mod.parse_published_generation_authority(
        SimpleNamespace(
            method_metadata={
                "graph_authority": _authority(
                    "reports/private-generation.yaml",
                    f"sha256:{'0' * 64}",
                )
            }
        ),
        task_path="tasks[0]",
        target_pact_version=4,
    )
    assert declaration is not None
    assert issues == []
    connection = _ReadOnlyCheckpointConnection(node_count=62607)

    result = authority_mod._select_exact_published_generation(
        connection,
        declaration,
    )

    assert result["ok"] is False
    assert result["blockers"] == [
        "published_generation_checkpoint_count_stamp_mismatch:okg.graph_nodes"
    ]
    assert result["detail"]["stamped_counts"]["okg.graph_nodes"] == 62606
    assert result["detail"]["live_counts"]["okg.graph_nodes"] == 62607


def test_compacted_checkpoint_has_a_specific_blocker(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import psycopg

    from okg.substrate import deployment_authority

    declaration, issues = authority_mod.parse_published_generation_authority(
        SimpleNamespace(
            method_metadata={
                "graph_authority": _authority(
                    "reports/private-generation.yaml",
                    f"sha256:{'0' * 64}",
                )
            }
        ),
        task_path="tasks[0]",
        target_pact_version=4,
    )
    assert declaration is not None
    assert issues == []
    monkeypatch.setattr(
        deployment_authority,
        "deployment_self_hosted_authoritative",
        lambda _deployment: False,
    )
    monkeypatch.setattr(
        deployment_authority,
        "resolve_deployment_dsn_authority",
        lambda *_args, **_kwargs: {"dsn": "postgresql://authority.invalid/private"},
    )

    connection = _ReadOnlyCheckpointConnection(
        checkpoint_present=False,
        compacted=True,
    )
    monkeypatch.setattr(psycopg, "connect", lambda *_args, **_kwargs: connection)

    result = authority_mod._resolve_exact_published_generation(
        tmp_path,
        declaration,
    )

    assert result["ok"] is False
    assert result["blockers"] == ["published_generation_checkpoint_compacted"]
    assert connection.rollbacks == 1


def test_absent_opt_in_preserves_shared_projection_behavior(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact = "reports/private-generation.yaml"
    artifact_bytes = b"exact private publication receipt\n"
    document = _document(
        artifact,
        _digest(artifact_bytes),
        graph_authority=None,
    )
    _seed(tmp_path, monkeypatch, document=document)
    calls: list[str] = []

    def projection(_root: Path, _manifest: PactManifest) -> dict[str, Any]:
        calls.append("projection")
        return {
            "ok": True,
            "status": "fresh",
            "stale": False,
            "projected_generation_id": COMPATIBILITY_ID,
            "recommended_commands": [],
        }

    monkeypatch.setattr(
        pact_operations,
        "_workflow_graph_projection_status",
        projection,
    )

    result = task_completion_gate(tmp_path, "demo", "task.demo")

    assert result["ok"] is True
    assert calls == ["projection"]
    assert "published_generation_authority" not in result["workflow_authority"]


@pytest.mark.parametrize(
    ("mutation", "blocker"),
    [
        (
            {"generation_id": COMPATIBILITY_ID + 1},
            "published_generation_evidence_generation_mismatch:ev.graph-report.demo",
        ),
        (
            {"artifact": "reports/other.yaml"},
            "published_generation_evidence_artifact_undeclared:ev.graph-report.demo",
        ),
        (
            {"data": {"deployment": "forged-demo"}},
            "published_generation_evidence_identity_mismatch:ev.graph-report.demo:deployment",
        ),
    ],
)
def test_mixed_or_forged_evidence_refuses_without_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: dict[str, Any],
    blocker: str,
) -> None:
    artifact = "reports/private-generation.yaml"
    artifact_bytes = b"exact private publication receipt\n"
    row = {
        "id": "ev.graph-report.demo",
        "kind": "graph_report",
        "status": "pass",
        "refs": ["req.demo"],
        "artifact": artifact,
        "generation_id": COMPATIBILITY_ID,
        "data": {"canonical_generation_id": GENERATION},
        **mutation,
    }
    document = _document(artifact, _digest(artifact_bytes), evidence=[row])
    _seed(tmp_path, monkeypatch, document=document)
    monkeypatch.setattr(
        pact_operations,
        "_workflow_graph_projection_status",
        lambda *_args: pytest.fail("shared projection fallback"),
    )

    result = task_completion_gate(tmp_path, "demo", "task.demo")

    assert result["ok"] is False
    assert blocker in result["blocked_by"]
    assert all("graph-freshness" not in command for command in result["next_commands"])


def test_missing_graph_evidence_is_non_vacuous(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact = "reports/private-generation.yaml"
    artifact_bytes = b"exact private publication receipt\n"
    document = _document(artifact, _digest(artifact_bytes), evidence=[])
    _seed(tmp_path, monkeypatch, document=document)

    result = task_completion_gate(tmp_path, "demo", "task.demo")

    assert result["ok"] is False
    assert "published_generation_evidence_missing:req.demo" in result["blocked_by"]
    assert "published_generation_evidence_empty" in result["blocked_by"]


def test_opt_in_without_a_graph_demand_refuses_as_vacuous(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact = "reports/private-generation.yaml"
    artifact_bytes = b"exact private publication receipt\n"
    document = _document(artifact, _digest(artifact_bytes))
    document["requirements"][0]["evidence_policy"] = {"required": ["executed_test"]}
    document["evidence"] = [
        {
            "id": "ev.executed-test.demo",
            "kind": "executed_test",
            "status": "pass",
            "refs": ["req.demo"],
            "artifact": artifact,
        }
    ]
    _seed(tmp_path, monkeypatch, document=document)
    monkeypatch.setattr(
        pact_operations,
        "_workflow_graph_projection_status",
        lambda *_args: pytest.fail("shared projection fallback"),
    )

    result = task_completion_gate(tmp_path, "demo", "task.demo")

    assert result["ok"] is False
    assert "published_generation_authority_vacuous" in result["blocked_by"]


def test_artifact_digest_mismatch_refuses_with_observed_digest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact = "reports/private-generation.yaml"
    document = _document(artifact, f"sha256:{'0' * 64}")
    _seed(tmp_path, monkeypatch, document=document)

    result = task_completion_gate(tmp_path, "demo", "task.demo")

    assert result["ok"] is False
    token = f"published_generation_artifact_digest_mismatch:{artifact}"
    assert token in result["blocked_by"]
    check = result["workflow_authority"]["published_generation_authority"]["detail"][
        "artifact_checks"
    ][0]
    assert check["observed_sha256"].startswith("sha256:")


def test_missing_artifact_refuses(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    artifact = "reports/private-generation.yaml"
    artifact_bytes = b"exact private publication receipt\n"
    document = _document(artifact, _digest(artifact_bytes))
    _seed(tmp_path, monkeypatch, document=document)
    (tmp_path / artifact).unlink()

    result = task_completion_gate(tmp_path, "demo", "task.demo")

    assert result["ok"] is False
    assert f"published_generation_artifact_missing:{artifact}" in result["blocked_by"]


def test_symlink_artifact_refuses(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = tmp_path / "receipt-target.yaml"
    target.write_bytes(b"target\n")
    artifact = "reports/private-generation.yaml"
    artifact_path = tmp_path / artifact
    artifact_path.parent.mkdir(parents=True)
    artifact_path.symlink_to(target)
    document = _document(artifact, _digest(b"target\n"))
    manifest = PactManifest.model_validate(document)
    (tmp_path / "pact").mkdir(parents=True)
    save_manifest(tmp_path, manifest)
    monkeypatch.setattr(
        pact_operations,
        "_workflow_ledger_authority",
        lambda _root: {"fresh": True, "status": "available"},
    )

    result = task_completion_gate(tmp_path, "demo", "task.demo")

    assert result["ok"] is False
    assert f"published_generation_artifact_symlink:{artifact}" in result["blocked_by"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("generation_id", GENERATION + "-other"),
        ("deployment_name", "other-demo"),
        ("branch", "other"),
        ("status", "blocked"),
        ("target_generation_id", COMPATIBILITY_ID + 1),
        ("catalog_version_id", 8),
        ("node_count", 62607),
        ("edge_count", 21330),
    ],
)
def test_every_live_checkpoint_identity_surface_is_exact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: Any,
) -> None:
    _seed(
        tmp_path,
        monkeypatch,
        live={
            "ok": True,
            "status": "resolved",
            "generation": _resolved(**{field: value}),
        },
    )

    result = task_completion_gate(tmp_path, "demo", "task.demo")

    public_name = {
        "generation_id": "canonical_generation_id",
        "deployment_name": "deployment",
        "target_generation_id": "compatibility_generation_id",
        "node_count": "nodes_live",
        "edge_count": "edges_live",
    }.get(field, field)
    assert result["ok"] is False
    assert (
        f"published_generation_authority_mismatch:{public_name}" in result["blocked_by"]
    )


def test_live_read_failure_refuses_and_only_recommends_read_only_diagnostics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed(
        tmp_path,
        monkeypatch,
        live={
            "ok": False,
            "status": "checkpoint_unavailable",
            "blockers": ["published_generation_checkpoint_compacted"],
            "recommended_commands": [
                f"uv run okg generation describe {GENERATION} --deployment private-demo --json"
            ],
        },
    )
    monkeypatch.setattr(
        pact_operations,
        "_workflow_graph_projection_status",
        lambda *_args: pytest.fail("shared projection fallback"),
    )

    result = task_completion_gate(tmp_path, "demo", "task.demo")

    assert result["ok"] is False
    assert "published_generation_checkpoint_compacted" in result["blocked_by"]
    assert result["next_commands"]
    assert all(
        "generation describe" in command or "task-done" in command
        for command in result["next_commands"]
    )
    assert all("graph-freshness" not in command for command in result["next_commands"])


def test_unresolved_deployment_authority_refuses_without_projection_repair(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _seed(
        tmp_path,
        monkeypatch,
        live={
            "ok": False,
            "status": "deployment_unavailable",
            "blockers": ["published_generation_deployment_authority_unavailable"],
            "recommended_commands": [
                "uv run okg deployment env private-demo --json",
                f"uv run okg generation describe {GENERATION} --deployment private-demo --json",
            ],
        },
    )
    monkeypatch.setattr(
        pact_operations,
        "_workflow_graph_projection_status",
        lambda *_args: pytest.fail("shared projection fallback"),
    )

    result = task_completion_gate(tmp_path, "demo", "task.demo")

    assert result["ok"] is False
    assert (
        "published_generation_deployment_authority_unavailable" in result["blocked_by"]
    )
    assert any("deployment env private-demo" in row for row in result["next_commands"])
    assert all("graph-freshness" not in command for command in result["next_commands"])


@pytest.mark.parametrize(
    ("mutate", "code"),
    [
        (lambda row: row.pop("branch"), "incomplete_published_generation_authority"),
        (
            lambda row: row.update({"deployment": "bad;publish"}),
            "invalid_published_generation_authority_value",
        ),
        (
            lambda row: row.update({"artifacts": []}),
            "empty_published_generation_authority_artifacts",
        ),
        (
            lambda row: row.update(
                {
                    "artifacts": [
                        {
                            "path": "../escape.yaml",
                            "sha256": f"sha256:{'0' * 64}",
                        }
                    ]
                }
            ),
            "invalid_published_generation_authority_artifact_path",
        ),
        (
            lambda row: row.update(
                {
                    "artifacts": [
                        {"path": "z.yaml", "sha256": f"sha256:{'0' * 64}"},
                        {"path": "a.yaml", "sha256": f"sha256:{'1' * 64}"},
                    ]
                }
            ),
            "unsorted_published_generation_authority_artifacts",
        ),
        (
            lambda row: row.update(
                {
                    "artifacts": [
                        {"path": "same.yaml", "sha256": f"sha256:{'0' * 64}"},
                        {"path": "same.yaml", "sha256": f"sha256:{'0' * 64}"},
                    ]
                }
            ),
            "duplicate_published_generation_authority_artifact",
        ),
    ],
)
def test_malformed_opt_in_is_a_static_error_and_never_falls_back(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutate: Any,
    code: str,
) -> None:
    artifact = "reports/private-generation.yaml"
    artifact_bytes = b"exact private publication receipt\n"
    declaration = _authority(artifact, _digest(artifact_bytes))
    mutate(declaration)
    document = _document(
        artifact,
        _digest(artifact_bytes),
        graph_authority=declaration,
    )
    manifest = _seed(tmp_path, monkeypatch, document=document)
    errors = {
        issue.code
        for issue in validate_manifest(manifest, repo_root=tmp_path).issues
        if issue.severity == "error"
    }
    assert code in errors
    monkeypatch.setattr(
        pact_operations,
        "_workflow_graph_projection_status",
        lambda *_args: pytest.fail("shared projection fallback"),
    )

    result = task_completion_gate(tmp_path, "demo", "task.demo")

    assert result["ok"] is False
    assert code in result["blocked_by"]


def test_declared_artifact_set_must_exactly_match_evidence_use(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first = "reports/private-generation.yaml"
    second = "reports/unused.yaml"
    first_bytes = b"exact private publication receipt\n"
    second_bytes = b"unused padding\n"
    declaration = _authority(first, _digest(first_bytes))
    declaration["artifacts"].append({"path": second, "sha256": _digest(second_bytes)})
    document = _document(first, _digest(first_bytes), graph_authority=declaration)
    _seed(tmp_path, monkeypatch, document=document)
    (tmp_path / second).write_bytes(second_bytes)

    result = task_completion_gate(tmp_path, "demo", "task.demo")

    assert result["ok"] is False
    assert (
        f"published_generation_authority_artifact_unused:{second}"
        in result["blocked_by"]
    )
