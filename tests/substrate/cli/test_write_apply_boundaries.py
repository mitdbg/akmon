"""Issue #1450: advertised ``--apply`` flags are real write boundaries."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from okg.substrate.cli import jobs as jobs_cli
from okg.substrate.cli import main as cli
from okg.substrate.cli import telemetry as telemetry_cli


WRITE_ARGV = [
    pytest.param(["run", "--once", "--json"], id="run-once"),
    pytest.param(
        [
            "enricher",
            "backfill",
            "demo",
            "links",
            "--limit",
            "1",
            "--chunk-size",
            "1",
            "--json",
        ],
        id="enricher-backfill",
    ),
    pytest.param(
        ["jobs", "enqueue-source", "--source", "docs", "--json"], id="enqueue-source"
    ),
    pytest.param(["telemetry", "bench", "overhead", "--json"], id="telemetry-bench"),
    pytest.param(
        ["telemetry", "profile", "--trace-id", "t1", "--json"], id="telemetry-profile"
    ),
    pytest.param(
        ["telemetry", "profile", "report", "--trace-id", "t1", "--json"],
        id="telemetry-profile-report",
    ),
    pytest.param(
        ["telemetry", "coverage", "--record-metrics", "--json"],
        id="telemetry-coverage-record",
    ),
]


def _payload(capsys: pytest.CaptureFixture[str]) -> dict:
    captured = capsys.readouterr()
    stream = captured.err.strip() or captured.out.strip()
    return json.loads(stream.splitlines()[-1])


@pytest.mark.parametrize("agent_mode", [None, "0", "1"])
@pytest.mark.parametrize("argv", WRITE_ARGV)
def test_write_shapes_refuse_without_apply_in_every_mode(
    argv: list[str],
    agent_mode: str | None,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    if agent_mode is None:
        monkeypatch.delenv("OKG_AGENT", raising=False)
    else:
        monkeypatch.setenv("OKG_AGENT", agent_mode)
    monkeypatch.setattr("okg.substrate.local_env.load_local_env_for_cli", lambda: None)

    assert cli.main(argv) != 0

    error = _payload(capsys)["error"]
    assert error["data"]["mutation_requested"] is False
    assert error["data"]["mutation_started"] is False
    assert "--apply" in error["fix_hint"]


@pytest.mark.parametrize("argv", WRITE_ARGV)
def test_write_refusal_is_identical_in_every_mode(
    argv: list[str],
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("okg.substrate.local_env.load_local_env_for_cli", lambda: None)
    payloads = []
    for value in (None, "0", "1"):
        if value is None:
            monkeypatch.delenv("OKG_AGENT", raising=False)
        else:
            monkeypatch.setenv("OKG_AGENT", value)
        assert cli.main(argv) != 0
        payloads.append(_payload(capsys)["error"])
    assert payloads[0] == payloads[1] == payloads[2]


def test_refusals_precede_owned_effects(
    capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    def explode(*args, **kwargs):
        raise AssertionError("write dependency reached without --apply")

    monkeypatch.setattr(cli, "_resolve_run_context", explode)
    monkeypatch.setattr(cli, "_resolve_deployment_dsn", explode)
    monkeypatch.setattr(jobs_cli, "resolve_read_dsn", explode)
    monkeypatch.setattr(telemetry_cli, "_resolve_dsn", explode)
    monkeypatch.setattr(
        "okg.substrate.deployment_scaffold.create_deployment_scaffold", explode
    )

    for parameter in WRITE_ARGV:
        argv = list(parameter.values[0])
        assert cli.main(argv) != 0
        _payload(capsys)


def test_deployment_new_without_apply_creates_no_target(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OKG_AGENT", "0")
    root = tmp_path / "deployments"

    assert (
        cli.main(
            ["deployment", "new", "witness", "--deployments-root", str(root), "--json"]
        )
        != 0
    )

    assert not (root / "witness").exists()
    assert _payload(capsys)["error"]["data"]["mutation_started"] is False


READ_ARGV = [
    ["telemetry", "metrics"],
    ["telemetry", "contract", "report"],
    ["telemetry", "trace", "list"],
    ["telemetry", "trace", "show", "t1"],
    # Showing a trace is a read at every flag combination, and so is reporting
    # on one: neither builds a profile bundle any more, so neither needs the
    # authority to write one. `telemetry profile` above is what builds.
    ["telemetry", "trace", "show", "t1", "--include-profile"],
    ["telemetry", "report", "--trace-id", "t1"],
    ["telemetry", "coverage"],
]


@pytest.mark.parametrize("argv", READ_ARGV)
def test_telemetry_read_shapes_do_not_require_apply(argv: list[str]) -> None:
    args = cli._build_parser().parse_args(argv)
    assert telemetry_cli._telemetry_requires_apply(args) is False


def test_repository_generated_publish_commands_include_apply() -> None:
    expected = {
        "src/okg/substrate/cli/deployment.py": "--inline --apply --json",
        "src/okg/substrate/cli/init.py": "run --once --deployment {deployment_name} --apply",
        "src/okg/substrate/cli/nomos_rollout_audit.py": "--inline --apply --json",
        "src/okg/substrate/ingest/incomplete_recovery.py": "--deployment {deployment} --apply",
        "docs/reference/cli.md": "run --once --deployment <d> --apply",
        "docs/reference/observability.md": "run --once --deployment <name> --apply --capture",
        "docs/runbooks/quality/ci-and-qa.md": "--dsn \"$DSN\" --apply",
        "docs/use/sources/index.md": "--inline --apply --json",
        "deployments/okg-workspace/server.py": "run --once --deployment okg-workspace --apply",
        "pact/changes/supersede-extraction-enricher-output/pact.yaml": "--force-enrichers --apply",
    }
    root = Path(__file__).parents[3]
    for relative, command in expected.items():
        assert command in (root / relative).read_text(encoding="utf-8"), relative


@pytest.mark.parametrize("argv", WRITE_ARGV)
def test_apply_authorizes_dispatch(
    argv: list[str],
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("OKG_AGENT", "0")
    monkeypatch.setattr("okg.substrate.local_env.load_local_env_for_cli", lambda: None)
    authorized = [*argv[:-1], "--apply", argv[-1]]
    if argv[:2] == ["deployment", "new"]:
        authorized[authorized.index("--json") : authorized.index("--json")] = [
            "--deployments-root",
            str(tmp_path),
        ]

    try:
        rc = cli.main(authorized)
        payload = _payload(capsys)
    except Exception as exc:
        rc = 1
        payload = {"error": {"data": getattr(exc, "data", {})}}

    assert (
        payload.get("error", {}).get("data", {}).get("mutation_requested") is not False
    )
    if argv[:2] == ["deployment", "new"]:
        assert rc == 0
        assert (tmp_path / "witness").exists()
