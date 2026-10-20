from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

pytest.importorskip("mcp")

from okg.substrate.mcp.deployment_entry import _deployment_entry_dsn_resolution
from okg.substrate.mcp.deployment_entry import run_for_deployment
from okg.substrate.mcp.server import _sanitize_caller_admission_mode
from okg.substrate.mcp.server import _server_dsn_resolution
from okg.substrate.mcp.server import build_server


def test_sanitize_caller_admission_mode_strips_caller_target_branch_name() -> None:
    # B1 (worktree-fork-writes P2): target_branch_name is SERVER-pinned. A
    # caller MUST NOT route its own write onto a branch. _submit_admission_payload
    # re-pins from the server `--branch` binding only for non-default servers, so
    # on a default server a caller-supplied target_branch_name would otherwise
    # survive into the envelope and route the write onto an arbitrary fork branch
    # (cross-worktree write-injection / review-path DoS). The sanitizer -- the
    # choke point EVERY submit path flows through -- must drop it unconditionally.
    sanitized = _sanitize_caller_admission_mode(
        {"target_branch_name": "experiment:other-worktree", "note": "keep"}
    )
    assert "target_branch_name" not in sanitized
    assert sanitized["note"] == "keep"


def test_normalize_promote_intent_durable_only_for_explicit_fork_write() -> None:
    # worktree-fork-writes req.promote-intent: DENY-BY-DEFAULT. Only an explicit
    # "durable" intent on a FORK write is stamped; scratch / unknown / absent, and
    # ANY default-branch write, carry no marker so merge-back never picks them up.
    from okg.substrate.mcp.server import _normalize_promote_intent

    fork = "experiment:wt"
    assert _normalize_promote_intent(
        {"promote_intent": "durable"}, fork
    )["promote_intent"] == "durable"
    # case/whitespace-insensitive
    assert _normalize_promote_intent(
        {"promote_intent": " Durable "}, fork
    )["promote_intent"] == "durable"
    # deny-by-default on a fork: anything but an explicit durable -> no marker
    for value in ("scratch", "junk", "", None):
        assert "promote_intent" not in _normalize_promote_intent(
            {"promote_intent": value}, fork
        )
    assert "promote_intent" not in _normalize_promote_intent({}, fork)
    # a DEFAULT-branch write never carries a marker, even if the caller said durable
    assert "promote_intent" not in _normalize_promote_intent(
        {"promote_intent": "durable"}, "default"
    )
    # unrelated authority keys are preserved
    out = _normalize_promote_intent(
        {"producer_policy": {"x": 1}, "promote_intent": "scratch"}, fork
    )
    assert out["producer_policy"] == {"x": 1}


def test_sanitize_caller_admission_mode_downgrades_caller_fast_track() -> None:
    # Sibling caller-self-assert guard: a caller-asserted auto-accept mode is
    # downgraded to review (harden-mcp-write-boundaries).
    sanitized = _sanitize_caller_admission_mode(
        {
            "admission_mode": "fast_track",
            "producer_policy": {"admission_mode": "fast_track"},
        }
    )
    assert sanitized["admission_mode"] == "review"
    assert sanitized["producer_policy"]["admission_mode"] == "review"


def test_build_server_prefers_dogfood_local_profile_when_env_conflicts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    deployment_dir = _write_dogfood_deployment(tmp_path)
    local_env = tmp_path / "local.env"
    # A dogfood "local profile" DSN is loopback by definition; constrain-mcp's
    # _derive_role_dsn refuses to rewrite a non-local owner DSN to the dev
    # okg_mcp password (managed hosts must set OKG_MCP_RO_DSN). The DSN-wins
    # discriminator under test is the dbname (correct vs wrong), not the host.
    local_env.write_text("OKG_DSN=postgres://127.0.0.1/correct\n", encoding="utf-8")
    monkeypatch.setenv("OKG_DSN", "postgres://127.0.0.1/wrong")
    monkeypatch.setenv("OKG_LOCAL_ENV", str(local_env))
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda deployment, **_kwargs: (
            deployment_dir if deployment == "okg-workspace" else None
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.mcp.server._load_readside_metadata",
        lambda deployment_name: {
            "deployment": deployment_name,
            "instructions": "",
            "audit_questions": "",
            "skill_triggers": {},
            "skills": {},
            "record_prefixes": (),
            "session_start_context": None,
        },
    )
    monkeypatch.setattr(
        "okg.substrate.mcp.deployment_tools.register_deployment_tools",
        lambda *_args, **_kwargs: None,
    )
    monkeypatch.setattr(
        "okg.substrate.mcp.deployment_tools.catalog_deployment_tool_declarations",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr("okg.substrate.mcp.server.ConnectionPool", _FakePool)
    # The pool is fake, so the start-time capability probe (#3151) is too.
    from okg.substrate.mcp.capability_probe import CAPABILITY_FIXTURES

    monkeypatch.setattr(
        "okg.substrate.mcp.server.probe_capabilities_from_pool",
        lambda *_args, **_kwargs: CAPABILITY_FIXTURES["none"](),
    )

    mcp = build_server(deployment_name="okg-workspace", min_conns=1, max_conns=1)

    authority = mcp._okg_dsn_authority
    # The read pool connects as the least-privilege okg_mcp role
    # (PACT least-privilege-mcp), derived from the resolved owner DSN —
    # so the conninfo carries the profile DSN's host/dbname plus the
    # okg_mcp role, not the raw owner URL.
    from psycopg.conninfo import conninfo_to_dict

    conninfo = conninfo_to_dict(mcp._okg_pool.conninfo)
    assert conninfo["host"] == "127.0.0.1"
    assert conninfo["dbname"] == "correct"
    assert conninfo["user"] == "okg_mcp"
    assert authority["mode"] == "dogfood_local_profile"
    assert authority["conflict_count"] == 1
    assert authority["conflicts"][0]["reason"] == "dogfood_local_profile_authority"


def test_deployment_entry_prefers_dogfood_local_profile_when_env_conflicts(
    tmp_path: Path,
    monkeypatch,
) -> None:
    deployment_dir = _write_dogfood_deployment(tmp_path)
    local_env = tmp_path / "local.env"
    local_env.write_text("OKG_DSN=postgres://profile/correct\n", encoding="utf-8")
    monkeypatch.setenv("OKG_DSN", "postgres://process/wrong")
    monkeypatch.setenv("OKG_LOCAL_ENV", str(local_env))
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda deployment, **_kwargs: (
            deployment_dir if deployment == "okg-workspace" else None
        ),
    )

    resolution = _deployment_entry_dsn_resolution("okg-workspace")

    assert resolution["dsn"] == "postgres://profile/correct"
    assert resolution["authority"]["mode"] == "dogfood_local_profile"
    assert resolution["authority"]["conflict_count"] == 1


def test_deployment_entry_does_not_fall_back_to_env_for_unresolved_deployment(
    monkeypatch,
) -> None:
    monkeypatch.setenv("OKG_DSN", "postgres://postgres:okg@127.0.0.1:55434/wrong")
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda deployment, **_kwargs: None,
    )

    resolution = _deployment_entry_dsn_resolution("okg-workspace")

    assert resolution["dsn"] is None
    assert resolution["authority"] == {
        "mode": "unresolved",
        "deployment": "okg-workspace",
    }


def test_deployment_entry_passes_authority_to_stdio(monkeypatch) -> None:
    from okg.substrate.embeddings import EmbeddingConfig

    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "okg.substrate.mcp.deployment_entry._deployment_entry_dsn_resolution",
        lambda deployment_name: {
            "dsn": "postgres://profile/correct",
            "authority": {
                "mode": "dogfood_local_profile",
                "deployment": deployment_name,
            },
            "target": {
                "deployment": deployment_name,
                "database": "correct",
                "identity": "undeclared",
            },
        },
    )
    monkeypatch.setattr(
        "okg.substrate.mcp.deployment_entry._preflight",
        lambda dsn, deployment_name: captured.update({
            "preflight_dsn": dsn,
            "preflight_deployment": deployment_name,
        }),
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment",
        lambda deployment_name: SimpleNamespace(
            mcp_instructions=None,
            embedding_config=EmbeddingConfig(),
        ),
    )
    monkeypatch.setattr(
        "okg.substrate.mcp.server.run_stdio",
        lambda **kwargs: captured.update({"stdio": kwargs}),
    )
    # run_for_deployment is a process entry point: it moves the bytecode
    # cache first (okg.substrate.bytecode_cache). Record the call instead of
    # changing the test process's interpreter settings.
    monkeypatch.setattr(
        "okg.substrate.bytecode_cache.enter_user_bytecode_cache",
        lambda: captured.update({"bytecode_cache": True}) or "",
    )

    run_for_deployment("okg-workspace")

    assert captured["bytecode_cache"] is True

    assert captured["preflight_dsn"] == "postgres://profile/correct"
    stdio = captured["stdio"]
    assert stdio["dsn"] == "postgres://profile/correct"
    assert stdio["dsn_authority"]["authority"]["mode"] == "dogfood_local_profile"
    assert stdio["dsn_authority"]["target"]["identity"] == "undeclared"


def test_mcp_server_rejects_deployment_explicit_dsn_database_mismatch(
    tmp_path: Path,
    monkeypatch,
) -> None:
    deployment_dir = _write_dogfood_deployment(
        tmp_path,
        declared_database="okg_workspace",
    )
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda deployment, **_kwargs: (
            deployment_dir if deployment == "okg-workspace" else None
        ),
    )

    resolution = _server_dsn_resolution(
        "postgres://postgres:okg@127.0.0.1:55434/not_okg_workspace",
        "okg-workspace",
    )

    assert resolution["dsn"] is None
    assert resolution["target"]["identity"] == "mismatch"
    assert resolution["authority"]["mode"] == "explicit"


def test_mcp_server_does_not_fall_back_to_env_for_unresolved_deployment(
    monkeypatch,
) -> None:
    monkeypatch.setenv("OKG_DSN", "postgres://postgres:okg@127.0.0.1:55434/wrong")
    monkeypatch.setattr(
        "okg.substrate.deployments.find_deployment_dir",
        lambda deployment, **_kwargs: None,
    )

    resolution = _server_dsn_resolution(None, "okg-workspace")

    assert resolution["dsn"] is None
    assert resolution["authority"] == {
        "mode": "unresolved",
        "deployment": "okg-workspace",
    }


def _write_dogfood_deployment(
    tmp_path: Path,
    *,
    declared_database: str | None = None,
) -> Path:
    deployment_dir = tmp_path / "deployments" / "okg-workspace"
    deployment_dir.mkdir(parents=True)
    database_line = (
        f"  database: {declared_database}\n" if declared_database else ""
    )
    # Self-hosted authority now flows from the manifest flag (not a hardcoded
    # `deployment == "okg-workspace"` check), so the fixture must carry it to
    # mirror the real okg-workspace manifest and keep local-profile authority.
    (deployment_dir / "deployment.yaml").write_text(
        "name: okg-workspace\npostgres:\n  dsn: ${OKG_DSN}\n"
        "  self_hosted_authoritative: true\n"
        f"{database_line}",
        encoding="utf-8",
    )
    return deployment_dir


class _FakePool:
    # Mirrors the real ConnectionPool interface: _build_pool references
    # ConnectionPool.check_connection at construction time (issue #950
    # checkout health check).
    @staticmethod
    def check_connection(conn) -> None:
        return None

    def __init__(self, conninfo: str, **kwargs) -> None:
        self.conninfo = conninfo
        self.kwargs = kwargs

    def close(self) -> None:
        return None
