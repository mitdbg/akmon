from __future__ import annotations

import re
from pathlib import Path

from tests.private_policy import private_policy_path


REPO_ROOT = Path(__file__).resolve().parents[2]


def _read(path: str) -> str:
    return (REPO_ROOT / path).read_text(encoding="utf-8")


_LOW_HEADROOM_GATE = (
    "If host headroom is too low to absorb temporary sparse-image growth, stop. "
    "Create host space outside Docker, expand or relocate the Docker disk image, "
    "or otherwise add physical capacity before any cleanup that writes to Docker "
    "storage. An approval alone does not make an operation fit."
)
_VOLUME_REMOVAL_GATE = (
    "Run `docker volume inspect <name>`, map the volume to its deployment and owner, "
    "and verify a recovery copy or written confirmation that the data is disposable. "
    "Stop if any identity or recovery fact is unknown. Only then run `docker volume "
    "rm <name>`."
)


def _assert_docker_sparse_image_headroom_guidance(section: str) -> None:
    reclaim = " ".join(section.split())
    required = [
        "Host and VM headroom answer different questions",
        "In-VM deletion can temporarily consume host space",
        "Whole-volume deletion can behave differently, but is not guaranteed to",
        "APFS snapshots can pin space",
        "Compaction is platform- and version-dependent",
        "Measure before any deletion",
        "neither immediate host reclaim nor later compaction is guaranteed",
        "keep Docker's confirmation prompt enabled",
        "Do not add `--volumes`",
        "Do not apply a privileged PID-host namespace recipe to native Linux",
        "Snapshot presence alone does not prove",
        _LOW_HEADROOM_GATE,
        _VOLUME_REMOVAL_GATE,
    ]
    for phrase in required:
        assert phrase in reclaim
    for phrase in [
        "docker volume rm` returns host bytes immediately",
        "In-VM deletion does NOT return host bytes by itself",
        "approved operator decision",
        "nsenter -t 1",
        "--privileged",
    ]:
        assert phrase not in reclaim
    assert not re.search(
        r"docker\s+system\s+prune\b[^`]*\s+(?:-f\b|--force(?:=\S+)?)",
        reclaim,
    )
    assert not re.search(r"--pid(?:=|\s+)host\b", reclaim)


def test_deployment_upgrade_docs_cover_current_cli_surface() -> None:
    cli = _read("docs/reference/cli.md")
    deployment = _read("docs/operate/deployments/runtime.md")

    for text in (cli, deployment):
        assert "okg deployment env" in text
        assert "okg deployment up" in text
        assert "--target dev" in text
        assert "--target prod" in text
        assert "--include" in text
        assert "--skip-migrate" in text

    assert "migrations" in deployment
    assert "catalog" in deployment
    assert "scope-complete ingest/publish" in " ".join(deployment.split())
    assert "runtime-worker restart" in deployment


def test_worktree_dsn_docs_cover_authority_and_isolation() -> None:
    text = _read("docs/runbooks/agents/worktrees.md")

    assert "worktree_main_local_env" in text
    assert "okg deployment env <name> --json" in text
    assert "okg deployment up <name> --target dev" in text
    assert "okg deployment isolated list <name> --json" in text
    assert "okg deployment isolated gc <name> --json" in text


def test_pact_authoring_docs_explain_generated_markdown_policy() -> None:
    text = private_policy_path("pact/AGENTS.md").read_text(encoding="utf-8")

    assert "`pact.yaml` is canonical" in text
    assert "`pact.md` is a generated review view" in text
    assert "ignored by git" in text
    assert "should not be committed" in text


def test_chronos_runbook_covers_read_role_permission_recovery() -> None:
    text = _read("docs/runbooks/troubleshooting/storage.md")

    assert "permission denied for" in text
    assert "_chronos_branch_tables" in text
    assert "okg_mcp" in text
    assert "app_ro" in text
    assert "okg migrate --deployment <deployment> --apply --views-only" in text
    assert "Do not grant `app_ro` direct access to physical Chronos tables" in text


def test_docker_sparse_image_headroom_guidance() -> None:
    text = _read("docs/runbooks/troubleshooting/storage.md")
    reclaim = " ".join(text.split("## Reclaim host storage", maxsplit=1)[1].split())
    _assert_docker_sparse_image_headroom_guidance(reclaim)

    unsafe_variants = {
        "low-headroom approval bypass": reclaim.replace(
            _LOW_HEADROOM_GATE,
            "If host headroom is low, an approved operator decision may proceed.",
        ),
        "unordered volume removal": reclaim.replace(
            _VOLUME_REMOVAL_GATE,
            "Run `docker volume inspect <name>`, then run `docker volume rm <name>`.",
        ),
        "forced broad prune": reclaim.replace(
            "`docker system prune`", "`docker system prune --force`"
        ),
        "privileged PID-host trim": (
            reclaim + " `docker run --privileged --pid host alpine true`"
        ),
    }
    for label, unsafe in unsafe_variants.items():
        try:
            _assert_docker_sparse_image_headroom_guidance(unsafe)
        except AssertionError:
            continue
        raise AssertionError(f"unsafe guidance variant passed: {label}")


def test_mcp_view_docs_cover_self_observation_telemetry() -> None:
    text = _read("docs/reference/mcp.md")

    for phrase in [
        "Self-observation telemetry",
        "agent-sessions",
        "codex-sessions",
        "token_count",
        "cost_confidence: measured",
        "cost_confidence: modeled",
        "cost_basis: codex_token_count_event",
        "instrumentation_gap",
        "tool_use_event",
        "live git",
    ]:
        assert phrase in text


def test_security_docs_cover_mcp_read_role_split() -> None:
    text = _read("docs/reference/security.md")

    assert "`okg_mcp` read pool" in text
    assert "membership in\n  `app_ro`" in text
    assert "OKG_MCP_RO_DSN" in text
    assert "OKG_MCP_RW_DSN" in text
