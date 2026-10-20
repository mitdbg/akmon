"""MCP fleet surface freshness checks."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from okg.substrate.library.sources.mcp_composed.client import McpServerError
from okg.substrate.mcp.provenance import UNTRUSTED_DATA_CONTRACT
from okg.substrate.mcp.surface_fleet import (
    RegisteredMcpServer,
    _surface_from_initialize_instructions,
    classify_surface,
    discover_registered_servers,
    expected_surface,
    fleet_surface_report,
    probe_registered_server,
    registration_narrowing,
)
from okg.substrate.mcp.surface_freshness import mcp_surface_report


def _server(name: str, deployment: str | None = None) -> RegisteredMcpServer:
    return RegisteredMcpServer(
        name=name,
        deployment=deployment or name.removeprefix("okg-"),
        command=["okg", "mcp-serve", "--deployment", deployment or "demo"],
        env={},
        source="fixture",
    )


def test_discover_registered_servers_reads_agent_configs(tmp_path: Path) -> None:
    mcp_json = tmp_path / ".mcp.json"
    mcp_json.write_text(
        """
{
  "mcpServers": {
    "okg-workspace": {
      "command": "uv",
      "args": ["run", "okg", "mcp-serve", "--deployment", "okg-workspace"],
      "env": {"OKG_DSN": "postgres://secret@example/db"}
    }
  }
}
""".strip(),
        encoding="utf-8",
    )
    codex_toml = tmp_path / "config.toml"
    codex_toml.write_text(
        """
[mcp_servers.okg-jansci-42]
command = "okg"
args = ["mcp-serve", "--deployment", "jansci-42"]

[mcp_servers.okg-env-only]
command = "okg"
args = ["mcp-serve"]

[mcp_servers.okg-env-only.env]
OKG_DEPLOYMENT = "env-only"
OKG_DSN = "postgres://secret@example/db"
""".strip(),
        encoding="utf-8",
    )

    servers = discover_registered_servers(config_paths=[mcp_json, codex_toml])
    by_name = {server.name: server for server in servers}

    assert by_name["okg-workspace"].deployment == "okg-workspace"
    assert by_name["okg-jansci-42"].deployment == "jansci-42"
    assert by_name["okg-env-only"].deployment == "env-only"
    assert by_name["okg-workspace"].to_json()["env_keys"] == ["OKG_DSN"]
    assert "postgres://secret" not in " ".join(
        by_name["okg-workspace"].to_json()["command"]
    )


def test_discover_registered_servers_attributes_ssh_remote_command(
    tmp_path: Path,
) -> None:
    mcp_json = tmp_path / ".mcp.json"
    mcp_json.write_text(
        json.dumps({
            "mcpServers": {
                "okg-ci": {
                    "command": "ssh",
                    "args": [
                        "ubuntu@192.0.2.10",
                        (
                            "sudo -n bash -c 'cd /opt/okg && exec "
                            ".venv/bin/okg mcp-serve --deployment okg-ci'"
                        ),
                    ],
                }
            }
        }),
        encoding="utf-8",
    )

    servers = discover_registered_servers(config_paths=[mcp_json])

    assert len(servers) == 1
    assert servers[0].name == "okg-ci"
    assert servers[0].deployment == "okg-ci"


def test_fleet_surface_report_classifies_fixture_probes(monkeypatch) -> None:
    expected = mcp_surface_report(
        deployment_name="demo",
        instructions="current\n\n" + UNTRUSTED_DATA_CONTRACT,
    )
    stale = {
        **expected,
        "instructions_hash": mcp_surface_report(
            deployment_name="demo",
            instructions="old\n\n" + UNTRUSTED_DATA_CONTRACT,
        )["instructions_hash"],
    }
    missing_contract = _surface_from_initialize_instructions(
        _server("okg-missing", "demo"),
        instructions="legacy instructions without the contract",
        server_info={"name": "okg", "version": "0.0.1"},
        probe_error="McpServerError: unknown inspect target",
    )
    registrations = [
        _server("okg-current", "demo"),
        _server("okg-stale", "demo"),
        _server("okg-unknown", "demo"),
        _server("okg-missing", "demo"),
    ]
    monkeypatch.setattr(
        "okg.substrate.mcp.surface_fleet.expected_surface",
        lambda deployment, **_: expected,
    )

    report = fleet_surface_report(
        registrations=registrations,
        probes={
            "okg-current": expected,
            "okg-stale": stale,
            "okg-missing": missing_contract,
        },
        probe_errors={"okg-unknown": "timeout"},
        do_probe=False,
    )

    statuses = {
        item["server"]["name"]: item["status"]
        for item in report["items"]
    }
    assert report["ok"] is False
    assert statuses == {
        "okg-current": "current",
        "okg-stale": "stale",
        "okg-unknown": "unknown",
        "okg-missing": "contract_missing",
    }
    missing = next(
        item for item in report["items"]
        if item["server"]["name"] == "okg-missing"
    )
    unknown = next(
        item for item in report["items"]
        if item["server"]["name"] == "okg-unknown"
    )
    assert unknown["code"] == "mcp_surface_probe_failed"
    assert missing["severity"] == "error"
    assert missing["security_finding"] is True
    assert missing["code"] == "mcp_surface_untrusted_contract_missing"


def test_probe_registered_server_fast_fails_dead_loopback_dsn(monkeypatch) -> None:
    def fail_connect(*_args, **_kwargs):
        raise OSError("connection refused with secret details")

    monkeypatch.setattr(
        "okg.substrate.mcp.surface_fleet.socket.create_connection",
        fail_connect,
    )
    server = RegisteredMcpServer(
        name="okg-dead",
        deployment="dead",
        command=["would-not-run"],
        env={
            "OKG_DSN": (
                "postgres://user:super-secret@127.0.0.1:65432/okg_dead"
            )
        },
        source="fixture",
    )

    with pytest.raises(McpServerError) as exc_info:
        probe_registered_server(server)

    message = str(exc_info.value)
    assert exc_info.value.stage == "spawn"
    assert "host='127.0.0.1'" in message
    assert "port=65432" in message
    assert "super-secret" not in message
    assert "would-not-run" not in message


def test_fleet_surface_report_missing_registration_is_unknown(monkeypatch) -> None:
    expected = mcp_surface_report(
        deployment_name="absent",
        instructions="current\n\n" + UNTRUSTED_DATA_CONTRACT,
    )
    monkeypatch.setattr(
        "okg.substrate.mcp.surface_fleet.expected_surface",
        lambda deployment, **_: expected,
    )

    report = fleet_surface_report(
        registrations=[],
        deployment="absent",
        do_probe=False,
    )

    assert report["ok"] is False
    assert report["registered_count"] == 0
    assert report["items"][0]["status"] == "unknown"
    assert report["items"][0]["server"]["source"] == "not_registered"


def _expected_with(**overrides) -> dict:
    base = mcp_surface_report(
        deployment_name="demo",
        instructions="current\n\n" + UNTRUSTED_DATA_CONTRACT,
    )
    return {**base, **overrides}


def test_missing_observed_git_revision_is_unknown_never_current(
    monkeypatch,
) -> None:
    """Review finding 2: a server with no .git must fail closed."""
    expected = _expected_with(
        git_revision="sha-expected", package_version="1.0.0",
    )
    observed = {**expected, "git_revision": None}
    monkeypatch.setattr(
        "okg.substrate.mcp.surface_fleet.expected_surface",
        lambda deployment, **_: expected,
    )

    report = fleet_surface_report(
        registrations=[_server("okg-nogit", "demo")],
        probes={"okg-nogit": observed},
        do_probe=False,
    )

    item = report["items"][0]
    assert item["status"] == "unknown"
    assert item["ok"] is False
    assert item["code"] == "mcp_surface_git_revision_unreported"


def test_package_version_mismatch_is_stale(monkeypatch) -> None:
    """Review finding 2: package_version is compared, not just reported."""
    expected = _expected_with(
        git_revision=None, package_version="1.2.0",
    )
    observed = {**expected, "package_version": "1.1.0"}
    monkeypatch.setattr(
        "okg.substrate.mcp.surface_fleet.expected_surface",
        lambda deployment, **_: expected,
    )

    report = fleet_surface_report(
        registrations=[_server("okg-oldpkg", "demo")],
        probes={"okg-oldpkg": observed},
        do_probe=False,
    )

    item = report["items"][0]
    assert item["status"] == "stale"
    assert item["code"] == "mcp_surface_package_version_mismatch"


def test_unresolvable_expectation_is_explicit_not_stale(monkeypatch) -> None:
    """Review finding 3: remote-root deployments must not classify stale."""
    monkeypatch.setattr(
        "okg.substrate.mcp.surface_fleet.expected_surface",
        lambda deployment, **_: {
            "kind": "mcp_surface_expectation",
            "deployment_name": deployment,
            "unresolvable": True,
            "error": "DeploymentResolutionError: no manifest in checkout",
        },
    )
    with_contract = _expected_with(git_revision="sha-remote")
    without_contract = _surface_from_initialize_instructions(
        _server("okg-remote-nocontract", "kdflex"),
        instructions="legacy instructions without the contract",
        server_info={"name": "okg", "version": "0.0.1"},
        probe_error="McpServerError: unknown inspect target",
    )

    report = fleet_surface_report(
        registrations=[
            _server("okg-remote", "kdflex"),
            _server("okg-remote-nocontract", "kdflex"),
        ],
        probes={
            "okg-remote": with_contract,
            "okg-remote-nocontract": without_contract,
        },
        do_probe=False,
    )

    statuses = {
        item["server"]["name"]: item for item in report["items"]
    }
    resolved = statuses["okg-remote"]
    assert resolved["status"] == "expectation_unresolvable"
    assert resolved["code"] == "mcp_surface_expectation_unresolvable"
    assert "install root" in resolved["fix_hint"]
    # The security contract check runs even without an expectation.
    nocontract = statuses["okg-remote-nocontract"]
    assert nocontract["status"] == "contract_missing"
    assert nocontract["security_finding"] is True


def test_malformed_okg_registrations_surface_as_unknown(
    tmp_path: Path, monkeypatch,
) -> None:
    """Review finding 4: malformed/unparseable entries must not vanish."""
    mcp_json = tmp_path / ".mcp.json"
    mcp_json.write_text(
        """
{
  "mcpServers": {
    "okg-broken": {"args": ["mcp-serve"]},
    "graph-server": {"command": "okg", "args": ["serve"]},
    "playwright": {"command": "npx", "args": ["playwright-mcp"]}
  }
}
""".strip(),
        encoding="utf-8",
    )
    bad_toml = tmp_path / "config.toml"
    bad_toml.write_text("[mcp_servers.okg-x\nnot toml", encoding="utf-8")

    issues: list[dict] = []
    servers = discover_registered_servers(
        config_paths=[mcp_json, bad_toml], issues_out=issues,
    )

    assert servers == []
    reasons = {issue["name"]: issue["reason"] for issue in issues}
    assert "okg-broken" in reasons  # no command -> visible
    assert "graph-server" in reasons  # okg command, deployment underivable
    assert "playwright" not in reasons  # non-OKG servers stay out of scope
    assert any(
        issue["name"] is None and "parse failed" in issue["reason"]
        for issue in issues
    )

    # And the fleet report renders them as failing unknown items.
    monkeypatch.setattr(
        "okg.substrate.mcp.surface_fleet.discover_registered_servers",
        lambda *, config_paths=None, issues_out=None: (
            issues_out.extend(issues) if issues_out is not None else None,
            [],
        )[1],
    )
    report = fleet_surface_report(do_probe=False)
    assert report["ok"] is False
    codes = {item["code"] for item in report["items"]}
    assert codes == {"mcp_surface_registration_unresolvable"}


def test_duplicate_name_registrations_get_distinct_probe_keys(
    monkeypatch,
) -> None:
    """Review finding 8: same-named registrations are distinguishable."""
    expected = _expected_with(
        git_revision="sha-expected", package_version="1.0.0",
    )
    stale = {**expected, "instructions_hash": "sha256:other"}
    repo = RegisteredMcpServer(
        name="okg-workspace", deployment="okg-workspace",
        command=["okg", "mcp-serve"], env={}, source="/repo/.mcp.json",
    )
    codex = RegisteredMcpServer(
        name="okg-workspace", deployment="okg-workspace",
        command=["okg", "mcp-serve"], env={}, source="~/.codex/config.toml",
    )
    monkeypatch.setattr(
        "okg.substrate.mcp.surface_fleet.expected_surface",
        lambda deployment, **_: expected,
    )

    report = fleet_surface_report(
        registrations=[repo, codex],
        probes={
            repo.probe_key(): expected,
            codex.probe_key(): stale,
        },
        do_probe=False,
    )

    by_source = {
        item["server"]["source"]: item["status"]
        for item in report["items"]
    }
    assert by_source == {
        "/repo/.mcp.json": "current",
        "~/.codex/config.toml": "stale",
    }


def test_doctor_mcp_surface_freshness_registry_and_exit(monkeypatch) -> None:
    from okg.substrate.cli import doctor as cli_doctor

    args = argparse.Namespace(deployment="demo")
    assert "mcp-surface-freshness" in cli_doctor._build_doctor_registry(
        args
    ).names()

    captured: dict = {}
    cli_doctor._load_main_helpers()
    monkeypatch.setattr(
        "okg.substrate.mcp.surface_fleet.fleet_surface_report",
        lambda deployment=None: {
            "check": "mcp-surface-freshness",
            "deployment": deployment,
            "ok": False,
            "failures": [{"status": "unknown"}],
        },
    )
    # _load_main_helpers() is idempotent (doctor.py: "Subsequent calls are a
    # no-op"), so the internal call inside _doctor_mcp_surface_freshness does
    # NOT re-import _emit_ok -- the cached binding from the forced load above is
    # what runs. Patch that cached binding on the doctor module directly.
    monkeypatch.setattr(
        cli_doctor, "_emit_ok", lambda payload, **kwargs: captured.update(payload)
    )

    rc = cli_doctor._doctor_mcp_surface_freshness(args, as_json=True)

    assert rc == 1
    assert captured["check"] == "mcp-surface-freshness"
    assert captured["deployment"] == "demo"


# ---- tool-profile aware expectations (PACT mcp-tool-surface-profiles) ----
#
# The doctor's expectation must follow the narrowing a registration asks for.
# Without that, every correctly-running profiled server classifies `stale` on
# an instructions-hash mismatch it can never fix — a check that fires on
# working deployments, which is worse than no check at all.


def _registration(
    *, args: list[str] | None = None, env: dict[str, str] | None = None,
) -> RegisteredMcpServer:
    return RegisteredMcpServer(
        name="okg-demo",
        deployment="demo",
        command=["okg", "mcp-serve", "--deployment", "demo", *(args or [])],
        env=env or {},
        source="fixture",
    )


@pytest.fixture
def resolvable_demo(monkeypatch):
    """A deployment that resolves and declares no `mcp:` block."""
    monkeypatch.setattr(
        "okg.substrate.deployments.resolve_deployment", lambda name: object(),
    )
    monkeypatch.setattr(
        "okg.substrate.mcp.server._load_readside_metadata",
        lambda name: {"instructions": "", "mcp": None},
    )


def test_registration_narrowing_reads_nothing_when_nothing_is_asked() -> None:
    assert registration_narrowing(_registration()) == (None, None)


@pytest.mark.parametrize(
    "args",
    [
        pytest.param(["--tools", "discovery"], id="separate-argv"),
        pytest.param(["--tools=discovery"], id="equals-form"),
        pytest.param(["--tools discovery"], id="single-shell-token"),
        # Deprecated spelling (decision 310): registrations written before the
        # rename still carry it, so the doctor must keep reading it.
        pytest.param(["--tool-profile", "discovery"], id="old-separate-argv"),
        pytest.param(["--tool-profile=discovery"], id="old-equals-form"),
        pytest.param(["--tool-profile discovery"], id="old-single-shell-token"),
    ],
)
def test_registration_narrowing_reads_every_flag_spelling(
    args: list[str],
) -> None:
    """A registration is a config file written by hand; all three spellings
    occur, and a missed one silently reverts the doctor to the wrong
    expectation."""
    profile, allowlist = registration_narrowing(_registration(args=args))
    assert profile == "discovery"
    assert allowlist is None


def test_registration_narrowing_reads_the_env_allowlist() -> None:
    profile, allowlist = registration_narrowing(
        _registration(env={"OKG_MCP_TOOL_ALLOWLIST": "inspect, search"})
    )
    assert profile is None
    assert allowlist == {"inspect", "search"}


def test_registration_narrowing_distinguishes_empty_from_absent() -> None:
    """An empty allowlist is a set the server refuses; an absent one is
    None. Collapsing them would compute an expectation for a registration
    that cannot start."""
    _, empty = registration_narrowing(
        _registration(env={"OKG_MCP_TOOL_ALLOWLIST": ""})
    )
    _, absent = registration_narrowing(_registration())
    assert empty == set()
    assert absent is None


def test_a_profiled_registration_expects_the_profiled_instructions(
    resolvable_demo,
) -> None:
    """The whole point: a `--tools discovery` registration is
    measured against discovery's instructions, not the full surface's."""
    from okg.substrate.mcp.server import operator_instructions_for

    narrowed = expected_surface("demo", tool_profile="discovery")
    full = expected_surface("demo")
    assert narrowed["instructions_hash"] != full["instructions_hash"]
    assert narrowed["instructions_bytes"] == len(
        operator_instructions_for(
            ("inspect", "search", "aggregate")
        ).encode("utf-8")
    )


def test_registration_ceiling_reads_every_flag_spelling() -> None:
    """Same reason as the profile spellings: a missed one reverts the doctor
    to the wrong expectation (issue #1458)."""
    from okg.substrate.mcp.surface_fleet import registration_ceiling

    for args in (
        ["--tools", "discovery", "--tools-ceiling", "reader"],
        ["--tools=discovery", "--tools-ceiling=reader"],
        ["--tools discovery --tools-ceiling reader"],
        ["--tool-profile", "discovery", "--tool-profile-ceiling", "reader"],
        ["--tool-profile=discovery", "--tool-profile-ceiling=reader"],
    ):
        assert registration_ceiling(_registration(args=args)) == "reader"
    assert registration_ceiling(_registration()) is None


def test_the_ceiling_flag_is_not_read_as_a_profile() -> None:
    """`--tools-ceiling` shares a prefix with `--tools`; read
    as one, the doctor would expect the ceiling profile's own instructions
    and call every such server stale."""
    for args in (
        ["--tools-ceiling", "reader"],
        ["--tools-ceiling=reader"],
        ["--tool-profile-ceiling", "reader"],
    ):
        profile, _ = registration_narrowing(_registration(args=args))
        assert profile is None, args


def test_a_registration_with_a_ceiling_expects_the_expansion_sentence(
    resolvable_demo,
) -> None:
    """A server that lets sessions widen serves one extra sentence saying
    so. An expectation computed without the ceiling hashes differently and
    would report a healthy server stale, with a restart hint that could
    never fix it."""
    with_ceiling = expected_surface(
        "demo", tool_profile="discovery", tool_profile_ceiling="reader",
    )
    without = expected_surface("demo", tool_profile="discovery")
    assert with_ceiling["instructions_hash"] != without["instructions_hash"]
    assert with_ceiling["instructions_bytes"] > without["instructions_bytes"]


@pytest.mark.parametrize(
    "kwargs, error",
    [
        pytest.param(
            {"tool_profile": "discovery", "tool_profile_ceiling": "raeder"},
            "UnknownToolProfile",
            id="misspelled-ceiling",
        ),
        pytest.param(
            {"tool_profile_ceiling": "reader"},
            "ToolSurfaceExpansionInvalid",
            id="ceiling-without-a-narrowing",
        ),
    ],
)
def test_a_ceiling_the_server_would_refuse_is_unresolvable_not_stale(
    resolvable_demo, kwargs, error,
) -> None:
    """A ceiling the server refuses is reported as a refusal, not as a hash
    mismatch that sends an operator to restart a server that cannot start.

    Only the arms a registration can decide are checked here. Whether a
    ceiling is strictly wider also depends on `--allow-writes`, which the
    expectation does not model, so a server refusing on that arm fails its
    probe and is reported `unknown` rather than being given a confidently
    wrong expectation.
    """
    expected = expected_surface("demo", **kwargs)
    assert expected["unresolvable"] is True
    assert error in expected["error"]


def test_a_registration_the_server_would_refuse_is_unresolvable_not_stale(
    resolvable_demo,
) -> None:
    """Both selectors at once is a launch the server refuses. Reporting it
    `stale` would send the operator to restart a server that cannot start,
    so the expectation is unresolvable and the entry is not ok."""
    expected = expected_surface(
        "demo", tool_profile="discovery", env_allowlist={"inspect"},
    )
    assert expected["unresolvable"] is True
    assert "ToolSurfaceSelectionConflict" in expected["error"]

    server = _registration(
        args=["--tools", "discovery"],
        env={"OKG_MCP_TOOL_ALLOWLIST": "inspect"},
    )
    observed = _surface_from_initialize_instructions(
        server,
        instructions=UNTRUSTED_DATA_CONTRACT,
        server_info={"name": "okg", "version": "9.9.9"},
        probe_error="probe",
    )
    item = classify_surface(server, expected, observed)
    assert item["ok"] is False
    assert item["status"] == "expectation_unresolvable"


def test_a_profiled_server_running_old_code_still_classifies_stale(
    resolvable_demo,
) -> None:
    """Non-vacuity partner: making the doctor profile-aware must not make it
    blind. A genuinely stale profiled server is still stale."""
    expected = expected_surface("demo", tool_profile="discovery")
    server = _registration(args=["--tools", "discovery"])
    observed = dict(expected)
    observed.update({
        "source": "running_process",
        "schema": "okg.mcp.surface_freshness.v1",
        "instructions_hash": "sha256:stale-different-text",
        "untrusted_data_contract_present": True,
        "git_revision": "old-sha",
    })
    item = classify_surface(server, expected, observed)
    assert item["ok"] is False
    assert item["status"] == "stale"
    assert item["code"] == "mcp_surface_instructions_hash_mismatch"


def test_two_registrations_of_one_deployment_get_distinct_expectations(
    resolvable_demo,
) -> None:
    """The expectation cache is keyed by deployment AND narrowing. Keying on
    the deployment alone would compare one registration against the other's
    expectation and call the second stale."""
    narrowed = _registration(args=["--tools", "discovery"])
    unnarrowed = _registration()
    report = fleet_surface_report(
        registrations=[narrowed, unnarrowed], do_probe=False,
    )
    hashes = {
        item["expected"]["instructions_hash"] for item in report["items"]
    }
    assert len(hashes) == 2


# ── self-report in the initialize reply (read-only agent launches) ────────


def test_server_info_version_round_trips_and_refuses_the_sdk_default() -> None:
    from okg.substrate.mcp.surface_freshness import (
        parse_server_info_version,
        server_info_version,
    )

    assert server_info_version("0.1.0", "abc123") == "0.1.0+okg.gabc123"
    assert parse_server_info_version("0.1.0+okg.gabc123") == {
        "package_version": "0.1.0", "git_revision": "abc123",
    }
    assert parse_server_info_version(server_info_version("0.1.0", None)) == {
        "package_version": "0.1.0", "git_revision": None,
    }
    assert parse_server_info_version(server_info_version(None, "abc")) == {
        "package_version": None, "git_revision": "abc",
    }
    # A local version part, or a revision with `.` or `+`, still reads back.
    for package_version, revision in (("0.1.0+local", "abc"), ("0.1.0", "v1.2+x")):
        assert parse_server_info_version(
            server_info_version(package_version, revision)
        ) == {"package_version": package_version, "git_revision": revision}
    # The MCP SDK's own version (FastMCP's default) is not okg's.
    for other in ("1.30.0", "1.30.0+local", "0.1.0+okg.xyz", "0.1.0+okg.g",
                  "0.1.0+okgx", None, 3):
        assert parse_server_info_version(other) is None


def _read_only_observation(expected: dict, version: str) -> dict:
    """What the probe gets from a launch that withholds inspect(surface)."""
    return _surface_from_initialize_instructions(
        _server("okg-demo", "demo"),
        instructions="current\n\n" + UNTRUSTED_DATA_CONTRACT,
        server_info={"name": "okg-substrate", "version": version},
        probe_error="McpServerError: agent_surface_inspect_kind_unserved",
        self_report_allowed=True,
    )


def test_a_read_only_launch_of_current_code_is_current() -> None:
    """The bug: `okg agent connect` registers a read-only launch, which
    withholds inspect(kind=surface), and doctor called it stale
    (`mcp_surface_self_report_missing`) from the SDK's version 1.30.0."""
    expected = _expected_with(git_revision="sha-now", package_version="0.1.0")
    observed = _read_only_observation(expected, "0.1.0+okg.gsha-now")

    assert observed["self_reported"] is True
    assert observed["package_version"] == "0.1.0"
    assert observed["git_revision"] == "sha-now"
    item = classify_surface(_server("okg-demo", "demo"), expected, observed)
    assert item["status"] == "current", item
    assert item["ok"] is True


def test_a_read_only_launch_of_old_code_is_still_stale() -> None:
    expected = _expected_with(git_revision="sha-now", package_version="0.1.0")
    old_revision = _read_only_observation(expected, "0.1.0+okg.gsha-old")
    item = classify_surface(_server("okg-demo", "demo"), expected, old_revision)
    assert (item["status"], item["code"]) == (
        "stale", "mcp_surface_git_revision_mismatch",
    )
    no_revision = _read_only_observation(expected, "0.1.0+okg")
    item = classify_surface(_server("okg-demo", "demo"), expected, no_revision)
    assert (item["status"], item["code"]) == (
        "unknown", "mcp_surface_git_revision_unreported",
    )


def test_a_server_reporting_only_the_sdk_version_is_still_stale() -> None:
    """An okg too old to report itself must not borrow the SDK's version."""
    expected = _expected_with(git_revision="sha-now", package_version="1.30.0")
    observed = _read_only_observation(expected, "1.30.0")

    assert observed["self_reported"] is False
    assert observed["package_version"] is None
    item = classify_surface(_server("okg-demo", "demo"), expected, observed)
    assert (item["status"], item["code"]) == (
        "stale", "mcp_surface_self_report_missing",
    )


def test_the_built_server_reports_okgs_version_in_server_info(
    monkeypatch,
) -> None:
    """The hermetic build (no database) carries okg's version, not the SDK's."""
    from importlib.metadata import version as dist_version

    from okg.substrate.mcp.surface_freshness import parse_server_info_version

    monkeypatch.setenv("OKG_MCP_SURFACE_PROBE", "1")
    monkeypatch.setenv("OKG_MCP_GIT_REVISION", "sha-built")
    from okg.substrate.mcp.surface_budget import PROBE_DSN
    from okg.substrate.mcp.server import build_server

    server = build_server(dsn=PROBE_DSN)
    try:
        options = server._mcp_server.create_initialization_options()
    finally:
        server._okg_pool.close()
        if server._okg_write_pool:
            server._okg_write_pool.close()
    assert parse_server_info_version(options.server_version) == {
        "package_version": dist_version("okg"), "git_revision": "sha-built",
    }


#: The text a read-only launch returns for inspect(kind=surface), trimmed to
#: the fields the probe reads.
WITHHELD_SURFACE = json.dumps({
    "status": "refused", "operator_name": "inspect",
    "error": {
        "code": "agent_surface_inspect_kind_unserved",
        "message": "inspect target kind 'surface' is not served by this surface.",
        "data": {"kind": "surface", "known_kind": True},
    },
})


def test_only_the_named_refusal_counts_as_a_withheld_surface() -> None:
    from okg.substrate.mcp.surface_fleet import _is_surface_kind_withheld

    assert _is_surface_kind_withheld(
        McpServerError(WITHHELD_SURFACE, stage="tool_call")
    )
    other_kind = json.loads(WITHHELD_SURFACE)
    other_kind["error"]["data"]["kind"] = "admission"
    for exc in (
        McpServerError(WITHHELD_SURFACE, stage="transport"),
        McpServerError(json.dumps(other_kind), stage="tool_call"),
        McpServerError("tool call 'inspect' failed: timed out", stage="tool_call"),
        McpServerError("unknown tool: inspect", stage="tool_call"),
    ):
        assert not _is_surface_kind_withheld(exc), exc


class _FakeClient:
    """McpClient stand-in: initialize succeeds, then the tool call fails."""

    error: McpServerError

    def __init__(self, *_args, **_kwargs) -> None:
        self.instructions = "current\n\n" + UNTRUSTED_DATA_CONTRACT
        self.server_info = {"name": "okg-substrate", "version": "0.1.0+okg.gsha-now"}

    def __enter__(self):
        return self

    def __exit__(self, *_exc) -> None:
        return None

    def call_tool(self, *_args, **_kwargs):
        raise self.error


@pytest.mark.parametrize("error, schema", [
    (McpServerError(WITHHELD_SURFACE, stage="tool_call"),
     "okg.mcp.surface_freshness.initialize.v1"),
    (McpServerError("tool call 'inspect' failed: timed out", stage="tool_call"),
     "okg.mcp.surface_freshness.legacy_initialize.v1"),
    (McpServerError("server process exited", stage="transport"),
     "okg.mcp.surface_freshness.legacy_initialize.v1"),
])
def test_a_failed_surface_call_is_never_current(monkeypatch, error, schema) -> None:
    """Review finding: a current server whose inspect call times out or dies
    after initialize must not read current from its initialize reply."""
    monkeypatch.setattr(_FakeClient, "error", error, raising=False)
    monkeypatch.setattr(
        "okg.substrate.library.sources.mcp_composed.client.McpClient", _FakeClient,
    )
    server = _server("okg-demo", "demo")
    observed = probe_registered_server(server)
    assert observed["schema"] == schema
    expected = _expected_with(git_revision="sha-now", package_version="0.1.0")
    item = classify_surface(server, expected, observed)
    if schema.endswith("initialize.v1") and "legacy" not in schema:
        assert item["status"] == "current", item
    else:
        assert (item["status"], item["code"]) == (
            "stale", "mcp_surface_self_report_missing",
        )


def test_the_surface_report_takes_a_wheels_build_revision(monkeypatch) -> None:
    """#3585 follow-up: a wheel has no .git; the report used to say None, so
    every wheel install read `mcp_surface_git_revision_unreported`."""
    from okg.substrate.mcp import surface_freshness
    from okg.substrate.runtime import code_identity

    monkeypatch.delenv("OKG_MCP_GIT_REVISION", raising=False)
    monkeypatch.setattr(
        code_identity, "installed_code_revision",
        lambda *_a, **_k: code_identity.InstalledCodeRevision(
            revision="c" * 40, source="build_info",
        ),
    )
    report = surface_freshness.mcp_surface_report(
        deployment_name="demo", instructions="x\n\n" + UNTRUSTED_DATA_CONTRACT,
    )
    assert report["git_revision"] == "c" * 40
    assert report["git_revision_source"] == "okg/_build_info.json source_revision"
    # The explicit override still wins.
    monkeypatch.setenv("OKG_MCP_GIT_REVISION", "d" * 40)
    assert surface_freshness.mcp_surface_report(
        deployment_name="demo", instructions="x",
    )["git_revision"] == "d" * 40



def test_the_surface_report_names_an_explicit_repo_roots_head(
    tmp_path: Path, monkeypatch,
) -> None:
    import subprocess

    from okg.substrate.mcp import surface_freshness

    monkeypatch.delenv("OKG_MCP_GIT_REVISION", raising=False)
    subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
    subprocess.run(
        ["git", "-C", str(tmp_path), "-c", "user.email=t@x", "-c", "user.name=t",
         "commit", "-q", "--allow-empty", "-m", "c"],
        check=True,
    )
    head = subprocess.run(
        ["git", "-C", str(tmp_path), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()
    report = surface_freshness.mcp_surface_report(
        deployment_name="demo", instructions="x", repo_root=tmp_path,
    )
    assert report["git_revision"] == head
    assert report["git_revision_source"] == "git rev-parse HEAD"


# ── probe timeout ─────────────────────────────────────────────────────────


def test_probe_timeout_defaults_to_30s_and_reads_the_env() -> None:
    from okg.substrate.mcp.surface_fleet import PROBE_TIMEOUT_ENV, probe_timeout_seconds

    assert probe_timeout_seconds({}) == 30.0
    assert probe_timeout_seconds({PROBE_TIMEOUT_ENV: "75"}) == 75.0
    for bad in ("abc", "0", "-5", "inf", "nan"):
        with pytest.raises(ValueError, match=PROBE_TIMEOUT_ENV):
            probe_timeout_seconds({PROBE_TIMEOUT_ENV: bad})


def _fleet_with_probe(monkeypatch, probe) -> dict:
    expected = _expected_with(git_revision="sha-now", package_version="0.1.0")
    monkeypatch.setattr(
        "okg.substrate.mcp.surface_fleet.expected_surface",
        lambda deployment, **_: expected,
    )
    monkeypatch.setattr(
        "okg.substrate.mcp.surface_fleet.probe_registered_server", probe,
    )
    return fleet_surface_report(registrations=[_server("okg-demo", "demo")])


def test_a_probe_that_times_out_names_the_limit_and_how_to_raise_it(
    monkeypatch,
) -> None:
    """A busy host made a healthy entry read unknown with a reinstall hint
    that could not help; the item now says it was a timeout, and its limit."""
    from okg.substrate.mcp.surface_fleet import PROBE_TIMEOUT_ENV

    monkeypatch.setenv(PROBE_TIMEOUT_ENV, "45")
    seen: list[float] = []

    def _slow(_server, *, timeout):  # noqa: ANN001, ANN202
        seen.append(timeout)
        raise McpServerError(
            "failed to enter MCP client context: Timed out while waiting for "
            "response to ClientRequest. Waited 45.0 seconds.",
            stage="handshake",
        )

    report = _fleet_with_probe(monkeypatch, _slow)
    assert seen == [45.0]
    assert report["probe_timeout_seconds"] == 45.0
    [item] = report["items"]
    assert (item["ok"], item["status"], item["code"]) == (
        False, "unknown", "mcp_surface_probe_failed",
    )
    assert item["probe_timeout_seconds"] == 45.0
    assert "within 45 s" in item["fix_hint"]
    assert PROBE_TIMEOUT_ENV in item["fix_hint"]


def test_a_bad_probe_timeout_setting_fails_every_item_by_name(monkeypatch) -> None:
    from okg.substrate.mcp.surface_fleet import PROBE_TIMEOUT_ENV

    monkeypatch.setenv(PROBE_TIMEOUT_ENV, "soon")

    def _never(*_a, **_k):  # noqa: ANN202
        raise AssertionError("must not probe with an unreadable timeout")

    report = _fleet_with_probe(monkeypatch, _never)
    [item] = report["items"]
    assert item["ok"] is False
    assert PROBE_TIMEOUT_ENV in item["probe_error"]
    assert PROBE_TIMEOUT_ENV in item["fix_hint"]


def test_an_inspect_call_that_times_out_is_unknown_not_stale(monkeypatch) -> None:
    """Review finding: a server that started but did not answer inspect in
    time fell back to the initialize reply and read stale."""
    from okg.substrate.mcp.surface_fleet import PROBE_TIMEOUT_ENV

    monkeypatch.delenv(PROBE_TIMEOUT_ENV, raising=False)
    monkeypatch.setattr(
        _FakeClient, "error",
        McpServerError(
            "tool call 'inspect' failed: Timed out while waiting for response "
            "to ClientRequest. Waited 30.0 seconds.",
            stage="tool_call",
        ),
        raising=False,
    )
    monkeypatch.setattr(
        "okg.substrate.library.sources.mcp_composed.client.McpClient", _FakeClient,
    )
    with pytest.raises(McpServerError, match="Timed out"):
        probe_registered_server(_server("okg-demo", "demo"))

    expected = _expected_with(git_revision="sha-now", package_version="0.1.0")
    monkeypatch.setattr(
        "okg.substrate.mcp.surface_fleet.expected_surface",
        lambda deployment, **_: expected,
    )
    report = fleet_surface_report(registrations=[_server("okg-demo", "demo")])
    [item] = report["items"]
    assert (item["status"], item["code"]) == ("unknown", "mcp_surface_probe_failed")
    assert item["probe_timeout_seconds"] == 30.0
    assert "within 30 s" in item["fix_hint"]


def test_only_the_sdk_wait_counts_as_a_probe_timeout() -> None:
    """The 0.25 s database-port preflight is not the probe timeout."""
    from okg.substrate.mcp.surface_fleet import _is_timeout

    assert _is_timeout(
        "McpServerError: failed to enter MCP client context: Timed out while "
        "waiting for response to ClientRequest. Waited 30.0 seconds."
    )
    assert not _is_timeout(
        "McpServerError: OKG_DSN loopback preflight failed: host='127.0.0.1' "
        "port=5433 error=TimeoutError"
    )
    assert not _is_timeout("McpServerError: server process exited")


def test_the_default_timeout_reaches_the_probe(monkeypatch) -> None:
    from okg.substrate.mcp.surface_fleet import PROBE_TIMEOUT_ENV

    monkeypatch.delenv(PROBE_TIMEOUT_ENV, raising=False)
    seen: list[float] = []

    def _probe(_server, *, timeout):  # noqa: ANN001, ANN202
        seen.append(timeout)
        raise McpServerError("server process exited", stage="transport")

    report = _fleet_with_probe(monkeypatch, _probe)
    assert seen == [30.0]
    [item] = report["items"]
    # A non-timeout failure keeps its own hint.
    assert "probe_timeout_seconds" not in item
    assert "within" not in item["fix_hint"]


def test_a_bad_probe_timeout_setting_fails_even_with_nothing_to_probe(
    monkeypatch,
) -> None:
    from okg.substrate.mcp.surface_fleet import PROBE_TIMEOUT_ENV

    monkeypatch.setenv(PROBE_TIMEOUT_ENV, "soon")
    report = fleet_surface_report(registrations=[])
    assert report["ok"] is False
    assert report["probe_timeout_seconds"] is None
    assert PROBE_TIMEOUT_ENV in report["probe_timeout_error"]
