"""Fleet-level MCP surface freshness checks."""
from __future__ import annotations

import json
import math
import os
import re
import shlex
import socket
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse

from okg.substrate.mcp.provenance import UNTRUSTED_DATA_CONTRACT
from okg.substrate.mcp.surface_freshness import (
    mcp_surface_report,
    parse_server_info_version,
)
from okg.substrate.support.user_hints import okg_prefix

#: The running server's `inspect(kind=surface)` report.
TOOL_SELF_REPORT_SCHEMA = "okg.mcp.surface_freshness.v1"
#: The same server's initialize reply, on a launch that withholds that kind
#: (every read-only agent launch): the served instructions plus okg's version
#: and revision from `serverInfo.version`.
INITIALIZE_SELF_REPORT_SCHEMA = "okg.mcp.surface_freshness.initialize.v1"
SELF_REPORT_SCHEMAS = frozenset({
    TOOL_SELF_REPORT_SCHEMA, INITIALIZE_SELF_REPORT_SCHEMA,
})


@dataclass(frozen=True)
class RegisteredMcpServer:
    name: str
    deployment: str
    command: list[str]
    env: dict[str, str]
    source: str

    def to_json(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "deployment": self.deployment,
            "command": _redacted_command(self.command),
            "env_keys": sorted(self.env),
            "source": self.source,
        }

    def probe_key(self) -> str:
        """Stable key distinguishing same-named registrations across configs."""
        return f"{self.source}::{self.name}::{self.deployment}"


def discover_registered_servers(
    *,
    config_paths: list[Path] | None = None,
    issues_out: list[dict[str, Any]] | None = None,
) -> list[RegisteredMcpServer]:
    """Discover configured OKG MCP stdio servers from local agent configs.

    `issues_out`, when supplied, collects config parse failures and
    OKG-looking registrations whose deployment could not be derived, so the
    fleet check can surface them as `unknown` instead of silently dropping
    them (a skipped registration would otherwise escape the doctor entirely).
    """
    paths = config_paths or [
        Path.cwd() / ".mcp.json",
        Path.home() / ".claude.json",
        Path.home() / ".codex" / "config.toml",
    ]
    out: list[RegisteredMcpServer] = []
    for path in paths:
        if not path.is_file():
            continue
        if path.suffix == ".toml":
            servers, issues = _servers_from_toml(path)
        else:
            servers, issues = _servers_from_json(path)
        out.extend(servers)
        if issues_out is not None:
            issues_out.extend(issues)
    return sorted(out, key=lambda s: (s.deployment, s.name, s.source))


def expected_surface(
    deployment_name: str,
    *,
    tool_profile: str | None = None,
    env_allowlist: set[str] | None = None,
    tool_profile_ceiling: str | None = None,
) -> dict[str, Any]:
    """Compute the current expected MCP surface for a deployment.

    When the deployment manifest cannot be resolved from this process
    (e.g. its install root lives outside this checkout), the expectation is
    marked unresolvable instead of silently falling back to
    OPERATOR_INSTRUCTIONS — that fallback mis-classified remote-root
    deployments with legitimate per-deployment instructions as permanently
    `stale` with an unfixable upgrade hint.

    `tool_profile` / `env_allowlist` come from the REGISTRATION, because the
    served instructions now follow the served tool set (PACT
    `mcp-tool-surface-profiles`). Ignoring them would classify every
    correctly-running narrowed server as `stale` on an instructions-hash
    mismatch it can never fix — a check that fires on working deployments is
    worse than no check.

    `tool_profile_ceiling` is here for exactly the same reason (issue #1458):
    a registration that lets sessions widen themselves serves one extra
    sentence describing the action, so an expectation computed without it
    hashes differently and would report a healthy server stale, with an
    upgrade hint that could never fix it.
    """
    # Import lazily to avoid making server.py import this fleet module.
    from okg.substrate.deployments import (
        DeploymentResolutionError,
        resolve_deployment,
    )
    from okg.substrate.mcp.operators import OPERATOR_TOOLS
    from okg.substrate.mcp.server import (
        _load_readside_metadata,
        authored_instructions_for,
    )
    from okg.substrate.mcp.tool_profiles import (
        parse_tool_profile_config,
        render_expansion_instruction,
        resolve_expansion_ceiling,
        resolve_tool_surface,
    )

    try:
        resolve_deployment(deployment_name)
    except DeploymentResolutionError as exc:
        return {
            "kind": "mcp_surface_expectation",
            "deployment_name": deployment_name,
            "unresolvable": True,
            "error": f"{type(exc).__name__}: {exc}",
        }
    metadata = _load_readside_metadata(deployment_name)
    # PACT dataset-card: a deployment's instruction file is APPENDED below
    # okg's own text (under a fixed heading), so the narrowing enters the
    # expectation for it too. The dataset card above both is compiled from
    # the database and is in the server's `instructions_hash` only, never in
    # `authored_instructions_hash`, which is what this is compared with.
    agent_surface = metadata.get("agent_surface")
    try:
        surface = resolve_tool_surface(
            requested_profile=tool_profile,
            env_allowlist=env_allowlist,
            config=parse_tool_profile_config(metadata.get("mcp")),
            agent_surface_served=(
                agent_surface.served if agent_surface is not None else None
            ),
        )
    except Exception as exc:  # noqa: BLE001 - doctor surface
        # A registration whose narrowing the SERVER would refuse is a real
        # finding, not a hash mismatch. Report it as unresolvable so the
        # operator sees the refusal reason rather than a misleading
        # "stale, restart it" hint.
        return {
            "kind": "mcp_surface_expectation",
            "deployment_name": deployment_name,
            "unresolvable": True,
            "error": f"{type(exc).__name__}: {exc}",
        }
    served = (
        tuple(t for t in OPERATOR_TOOLS if t in surface.tools)
        if surface.tools is not None
        else tuple(OPERATOR_TOOLS)
    )
    expansion_text = ""
    if tool_profile_ceiling is not None:
        try:
            ceiling = resolve_expansion_ceiling(
                tool_profile_ceiling,
                base=surface,
                config=parse_tool_profile_config(metadata.get("mcp")),
            )
        except Exception as exc:  # noqa: BLE001 - doctor surface
            # Same rule as the narrowing above: a ceiling the SERVER
            # would refuse is a finding to report, not a hash mismatch
            # to dress up as staleness.
            return {
                "kind": "mcp_surface_expectation",
                "deployment_name": deployment_name,
                "unresolvable": True,
                "error": f"{type(exc).__name__}: {exc}",
            }
        assert ceiling is not None
        # Only the arms a registration can DECIDE are enforced here.
        # Whether a ceiling is strictly wider also depends on
        # `--allow-writes`, which this expectation does not model, so
        # that arm is left to the server: a launch it refuses fails its
        # probe and is reported `unknown`, rather than being handed a
        # confidently wrong expectation.
        reachable = (
            tuple(t for t in OPERATOR_TOOLS if t in ceiling.tools)
            if ceiling.tools is not None
            else tuple(OPERATOR_TOOLS)
        )
        expansion_text = render_expansion_instruction(
            base_tools=served, ceiling_tools=reachable,
        )
    instructions = authored_instructions_for(
        served,
        deployment_text=str(metadata.get("instructions") or ""),
        expansion_text=expansion_text,
    )
    return mcp_surface_report(
        deployment_name=deployment_name,
        instructions=instructions,
    )


#: `okg mcp-serve` spellings a registration may use. `--tools` and
#: `--tools-ceiling` are current (decision 310); `--tool-profile` and
#: `--tool-profile-ceiling` are the deprecated aliases existing client
#: registrations still carry, so both must be read.
_TOOLS_FLAGS = frozenset({"--tools", "--tool-profile"})
_TOOLS_CEILING_FLAGS = frozenset({"--tools-ceiling", "--tool-profile-ceiling"})


def registration_narrowing(
    server: RegisteredMcpServer,
) -> tuple[str | None, set[str] | None]:
    """The tool narrowing a registration asks for: (profile, env allowlist).

    Read from the registration rather than guessed, so the doctor's
    expectation is computed for the surface that registration actually
    launches.
    """
    profile: str | None = None
    tokens: list[str] = []
    for arg in server.command:
        try:
            tokens.extend(shlex.split(str(arg)))
        except ValueError:
            tokens.append(str(arg))
    for index, token in enumerate(tokens):
        flag, eq, value = token.partition("=")
        if flag not in _TOOLS_FLAGS:
            continue
        if eq:
            profile = _clean_deployment_token(value)
        elif index + 1 < len(tokens):
            profile = _clean_deployment_token(tokens[index + 1])
    raw = server.env.get("OKG_MCP_TOOL_ALLOWLIST")
    allowlist = (
        {part.strip() for part in raw.split(",") if part.strip()}
        if raw is not None
        else None
    )
    return profile or None, allowlist


def registration_ceiling(server: RegisteredMcpServer) -> str | None:
    """The in-session ceiling a registration asks for, if any (issue #1458).

    Read separately rather than folded into `registration_narrowing`: the
    ceiling is not a narrowing — it widens what a session can reach — and the
    two answer different questions the doctor asks in different places.
    """

    ceiling: str | None = None
    tokens: list[str] = []
    for arg in server.command:
        try:
            tokens.extend(shlex.split(str(arg)))
        except ValueError:
            tokens.append(str(arg))
    for index, token in enumerate(tokens):
        flag, eq, value = token.partition("=")
        if flag not in _TOOLS_CEILING_FLAGS:
            continue
        if eq:
            ceiling = _clean_deployment_token(value)
        elif index + 1 < len(tokens):
            ceiling = _clean_deployment_token(tokens[index + 1])
    return ceiling or None


#: How long the doctor waits for a registered server to start and answer.
#: 10 s was the old fixed limit; an agent's launch wrapper plus a busy host
#: (load ~40 on a busy lab machine) took ~10 s, so healthy entries read `unknown`.
DEFAULT_PROBE_TIMEOUT_SECONDS = 30.0
PROBE_TIMEOUT_ENV = "OKG_MCP_SURFACE_PROBE_TIMEOUT_SECONDS"


def probe_timeout_seconds(env: Mapping[str, str] | None = None) -> float:
    """The probe timeout: ``OKG_MCP_SURFACE_PROBE_TIMEOUT_SECONDS`` or 30 s.

    A value that is not a positive number raises rather than falling back to
    the default, so a mistyped setting is seen, not silently ignored.
    """
    raw = (os.environ if env is None else env).get(PROBE_TIMEOUT_ENV)
    if raw is None or not raw.strip():
        return DEFAULT_PROBE_TIMEOUT_SECONDS
    try:
        value = float(raw)
    except ValueError:
        value = math.nan
    if not (value > 0 and math.isfinite(value)):
        raise ValueError(
            f"{PROBE_TIMEOUT_ENV}={raw!r} is not a positive number of seconds"
        )
    return value


def probe_registered_server(
    server: RegisteredMcpServer,
    *,
    timeout: float | None = None,
) -> dict[str, Any]:
    """Launch one registered stdio server and ask its running process."""
    if timeout is None:
        timeout = probe_timeout_seconds()
    from okg.substrate.library.sources.mcp_composed.client import (
        McpClient,
        McpServerError,
    )

    if not server.command:
        raise McpServerError("registered MCP server command is empty", stage="spawn")
    preflight_error = _loopback_dsn_preflight_error(
        server.env.get("OKG_DSN"),
        timeout=min(timeout, 0.25),
    )
    if preflight_error:
        raise McpServerError(preflight_error, stage="spawn")
    with McpClient(
        list(server.command),
        env={**os.environ, **server.env, "OKG_MCP_SURFACE_PROBE": "1"},
        subprocess_timeout=timeout,
        client_info={"name": "okg-surface-freshness-doctor"},
    ) as client:
        try:
            result = client.call_tool(
                "inspect",
                {"target": {"kind": "surface"}},
                timeout=timeout,
                # A program reads the surface: ask for compact JSON (#3059).
                meta={"okg/result_format": "json"},
            )
        except McpServerError as exc:
            # A call that timed out says nothing about the running code: it
            # is unknown, never the stale verdict the initialize fallback
            # gives a server too old to report itself.
            if client.instructions is None or _is_timeout(str(exc)):
                raise
            return _surface_from_initialize_instructions(
                server,
                instructions=client.instructions,
                server_info=client.server_info,
                probe_error=f"{type(exc).__name__}: {exc}",
                # Only this launch's named refusal of the kind makes the
                # initialize reply the server's self-report. A timeout, a
                # crash or any other error after initialize proves nothing
                # about the running code, and must never read current.
                self_report_allowed=_is_surface_kind_withheld(exc),
            )
    payload = _extract_surface_payload(result)
    if not isinstance(payload, dict):
        raise McpServerError(
            "inspect(kind=surface) returned no structured object",
            stage="tool_call",
        )
    if not _is_surface_payload(payload) and client.instructions is not None:
        return _surface_from_initialize_instructions(
            server,
            instructions=client.instructions,
            server_info=client.server_info,
            probe_error="inspect(kind=surface) returned non-surface payload",
        )
    return payload


def fleet_surface_report(
    *,
    registrations: list[RegisteredMcpServer] | None = None,
    deployment: str | None = None,
    probes: dict[str, dict[str, Any] | None] | None = None,
    probe_errors: dict[str, str] | None = None,
    do_probe: bool = True,
) -> dict[str, Any]:
    """Classify every registered OKG MCP server against current code."""
    registration_issues: list[dict[str, Any]] = []
    registrations = registrations if registrations is not None else (
        discover_registered_servers(issues_out=registration_issues)
    )
    if deployment:
        registrations = [
            server for server in registrations
            if server.deployment == deployment
        ]
        registration_issues = []
    probes = probes or {}
    probe_errors = probe_errors or {}
    items: list[dict[str, Any]] = []
    timeout_error: str | None = None
    timeout = DEFAULT_PROBE_TIMEOUT_SECONDS
    if do_probe:
        try:
            timeout = probe_timeout_seconds()
        except ValueError as exc:
            timeout_error = str(exc)
    expected_by_deployment: dict[str, dict[str, Any]] = {}

    for server in registrations:
        # Keyed by deployment AND narrowing: two registrations of one
        # deployment under different profiles legitimately expect different
        # instructions, so caching on the deployment alone would compare one
        # against the other's expectation.
        profile, allowlist = registration_narrowing(server)
        ceiling = registration_ceiling(server)
        cache_key = (
            f"{server.deployment}::{profile or ''}::"
            f"{','.join(sorted(allowlist)) if allowlist is not None else ''}"
            f"::{ceiling or ''}"
        )
        expected = expected_by_deployment.setdefault(
            cache_key,
            expected_surface(
                server.deployment,
                tool_profile=profile,
                env_allowlist=allowlist,
                tool_profile_ceiling=ceiling,
            ),
        )
        # Composite key first so duplicate-name registrations (the real
        # okg-workspace codex+repo case) stay distinguishable; bare name
        # kept for fixture back-compat.
        key = server.probe_key()
        observed = probes.get(key, probes.get(server.name))
        error = probe_errors.get(key, probe_errors.get(server.name))
        if (
            observed is None
            and do_probe
            and key not in probes
            and server.name not in probes
        ):
            if timeout_error is not None:
                error = timeout_error
            else:
                try:
                    observed = probe_registered_server(server, timeout=timeout)
                except Exception as exc:  # noqa: BLE001 - doctor surface
                    error = f"{type(exc).__name__}: {exc}"
        item = classify_surface(server, expected, observed, error)
        if observed is None and error is not None:
            if timeout_error is not None and error == timeout_error:
                item["fix_hint"] = (
                    f"set {PROBE_TIMEOUT_ENV} to a positive number of seconds, "
                    f"or unset it for the {DEFAULT_PROBE_TIMEOUT_SECONDS:g} s default"
                )
            elif _is_timeout(error):
                item["probe_timeout_seconds"] = timeout
                item["fix_hint"] = (
                    f"the server did not answer within {timeout:g} s, so its "
                    "code was not checked; check the host's load, or wait "
                    f"longer with {PROBE_TIMEOUT_ENV}=<seconds>"
                )
        items.append(item)

    for issue in registration_issues:
        items.append({
            "server": {
                "name": issue.get("name"),
                "deployment": None,
                "command": [],
                "env_keys": [],
                "source": issue.get("source"),
            },
            "deployment": None,
            "expected": None,
            "observed": None,
            "ok": False,
            "status": "unknown",
            "severity": "error",
            "security_finding": False,
            "code": "mcp_surface_registration_unresolvable",
            "probe_error": issue.get("reason"),
            "fix_hint": (
                "this OKG-looking MCP registration could not be parsed or "
                "attributed to a deployment; fix the entry in "
                f"{issue.get('source')} (or add --deployment/OKG_DEPLOYMENT)"
            ),
        })

    if deployment and not items:
        expected = expected_surface(deployment)
        placeholder = RegisteredMcpServer(
            name=f"okg-{deployment}",
            deployment=deployment,
            command=[],
            env={},
            source="not_registered",
        )
        items.append(
            classify_surface(
                placeholder,
                expected,
                None,
                "no registered MCP server found for deployment",
            )
        )

    counts: dict[str, int] = {}
    for item in items:
        counts[item["status"]] = counts.get(item["status"], 0) + 1
    failing = [item for item in items if item["ok"] is not True]
    # An unreadable setting fails the check even when nothing was probed.
    ok = not failing and timeout_error is None
    return {
        "check": "mcp-surface-freshness",
        "probe_timeout_seconds": (
            timeout if do_probe and timeout_error is None else None
        ),
        **({"probe_timeout_error": timeout_error} if timeout_error else {}),
        "ok": ok,
        "status": "current" if ok else "blocked",
        "deployment": deployment,
        "registered_count": len(registrations),
        "counts": counts,
        "items": items,
        "failures": failing,
    }


def classify_surface(
    server: RegisteredMcpServer,
    expected: dict[str, Any],
    observed: dict[str, Any] | None,
    probe_error: str | None = None,
) -> dict[str, Any]:
    base: dict[str, Any] = {
        "server": server.to_json(),
        "deployment": server.deployment,
        "expected": _surface_summary(expected),
        "observed": _surface_summary(observed),
        "ok": False,
        "status": "unknown",
        "severity": "error",
        "security_finding": False,
        "fix_hint": _fix_hint(server.deployment),
    }
    if probe_error:
        base["probe_error"] = probe_error
    if observed is None:
        base["code"] = "mcp_surface_probe_failed" if probe_error else (
            "mcp_surface_unknown"
        )
        return base
    # Security contract first: it is expectation-independent, so it is
    # checked even when the expectation is unresolvable below.
    if observed.get("untrusted_data_contract_present") is not True:
        base.update({
            "status": "contract_missing",
            "security_finding": True,
            "code": "mcp_surface_untrusted_contract_missing",
        })
        return base
    if expected.get("unresolvable"):
        base.update({
            "status": "expectation_unresolvable",
            "code": "mcp_surface_expectation_unresolvable",
            "probe_error": str(expected.get("error") or ""),
            "fix_hint": (
                f"the doctor cannot resolve deployment "
                f"{server.deployment!r} from this checkout, so no "
                "expectation exists to compare against; run the doctor "
                "from the deployment's install root"
            ),
        })
        return base
    if observed.get("schema") not in SELF_REPORT_SCHEMAS:
        base.update({
            "status": "stale",
            "code": "mcp_surface_self_report_missing",
        })
        return base
    # A server that appends generated capability lines (#3151) reports the
    # hash of its authored part separately: the generated part depends on
    # the database, which this expectation cannot see. Older servers report
    # only `instructions_hash`, which is then the authored text.
    observed_authored = observed.get("authored_instructions_hash") or observed.get(
        "instructions_hash"
    )
    if observed_authored != expected.get("instructions_hash"):
        base.update({
            "status": "stale",
            "code": "mcp_surface_instructions_hash_mismatch",
        })
        return base
    expected_revision = expected.get("git_revision")
    observed_revision = observed.get("git_revision")
    if (
        expected_revision
        and observed_revision
        and observed_revision != expected_revision
    ):
        base.update({
            "status": "stale",
            "code": "mcp_surface_git_revision_mismatch",
        })
        return base
    expected_version = expected.get("package_version")
    observed_version = observed.get("package_version")
    if (
        expected_version
        and observed_version
        and observed_version != expected_version
    ):
        base.update({
            "status": "stale",
            "code": "mcp_surface_package_version_mismatch",
        })
        return base
    # Fail CLOSED on unverifiable revision: a stale install with unchanged
    # instructions text and no .git (git_revision None) must never classify
    # current. Skip-as-pass is the P0 silent-fallback class.
    if not observed_revision:
        base.update({
            "status": "unknown",
            "code": "mcp_surface_git_revision_unreported",
            "fix_hint": (
                "the running server reports no git revision, so currency "
                "cannot be verified; reinstall/restart it from a git "
                "checkout or rely on package_version by upgrading to a "
                "release that reports one"
            ),
        })
        return base
    if not expected_revision:
        base.update({
            "status": "unknown",
            "code": "mcp_surface_expected_revision_unavailable",
            "fix_hint": (
                "the doctor's own checkout reports no git revision, so no "
                "expectation exists; run the doctor from a git checkout"
            ),
        })
        return base
    base.update({
        "ok": True,
        "status": "current",
        "severity": "ok",
        "fix_hint": None,
        "code": "mcp_surface_current",
    })
    return base


def _servers_from_json(
    path: Path,
) -> tuple[list[RegisteredMcpServer], list[dict[str, Any]]]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return [], [{
            "name": None,
            "source": str(path),
            "reason": f"config parse failed: {type(exc).__name__}: {exc}",
        }]
    servers: dict[str, Any] = {}
    if isinstance(raw, dict):
        servers.update(raw.get("mcpServers") or {})
        for project in (raw.get("projects") or {}).values():
            if isinstance(project, dict):
                servers.update(project.get("mcpServers") or {})
    return _servers_from_mapping(servers, source=str(path))


def _servers_from_toml(
    path: Path,
) -> tuple[list[RegisteredMcpServer], list[dict[str, Any]]]:
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        return [], [{
            "name": None,
            "source": str(path),
            "reason": f"config parse failed: {type(exc).__name__}: {exc}",
        }]
    servers = raw.get("mcp_servers") if isinstance(raw, dict) else {}
    return _servers_from_mapping(servers or {}, source=str(path))


def _looks_like_okg_entry(name: str, entry: Any) -> bool:
    """OKG-looking registrations must stay visible even when malformed.

    Matches on the registration NAME or on okg-binary/mcp-serve tokens —
    not on path substrings, so a third-party server merely installed under
    a directory named okg is not claimed.
    """
    if name.startswith("okg"):
        return True
    if not isinstance(entry, dict):
        return False
    tokens = [str(entry.get("command") or "")]
    args = entry.get("args")
    if isinstance(args, list):
        tokens.extend(str(a) for a in args)
    for token in tokens:
        if token == "okg" or token.endswith("/okg") or token == "mcp-serve":
            return True
    return False


def _servers_from_mapping(
    servers: dict[str, Any],
    *,
    source: str,
) -> tuple[list[RegisteredMcpServer], list[dict[str, Any]]]:
    out: list[RegisteredMcpServer] = []
    issues: list[dict[str, Any]] = []

    def _issue(name: str, reason: str) -> None:
        issues.append({"name": name, "source": source, "reason": reason})

    for name, entry in servers.items():
        okg_like = _looks_like_okg_entry(str(name), entry)
        if not isinstance(entry, dict):
            if okg_like:
                _issue(str(name), "registration entry is not a mapping")
            continue
        command = entry.get("command")
        args = entry.get("args") or []
        if not isinstance(command, str):
            if okg_like:
                _issue(str(name), "registration has no string command")
            continue
        if not isinstance(args, list):
            args = []
        env = {
            str(k): str(v)
            for k, v in (entry.get("env") or {}).items()
            if v is not None
        }
        deployment = _deployment_from_entry(
            str(name),
            [str(a) for a in args],
            env,
        )
        if deployment is None:
            if okg_like:
                _issue(
                    str(name),
                    "deployment underivable (no okg- name prefix, "
                    "--deployment arg, or OKG_DEPLOYMENT env)",
                )
            continue
        out.append(
            RegisteredMcpServer(
                name=str(name),
                deployment=deployment,
                command=[command, *[str(a) for a in args]],
                env=env,
                source=source,
            )
        )
    return out, issues


def _loopback_dsn_preflight_error(
    dsn: str | None,
    *,
    timeout: float,
) -> str | None:
    """Return a redacted error if a loopback OKG_DSN is already unreachable.

    Many stale local MCP registrations point at ephemeral Docker/Postgres ports
    that are long gone. Launching those servers just to discover the same dead
    socket is slow and noisy, so the doctor fails those registrations fast while
    preserving the "unknown, never current" contract.
    """
    if not dsn:
        return None
    try:
        parsed = urlparse(dsn)
    except ValueError:
        return None
    if parsed.scheme not in {"postgres", "postgresql"}:
        return None
    host = parsed.hostname
    port = parsed.port
    if host not in {"localhost", "127.0.0.1", "::1"} or port is None:
        return None
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return None
    except OSError as exc:
        return (
            "configured OKG_DSN loopback endpoint is unreachable before MCP "
            f"launch: host={host!r} port={port} error={exc.__class__.__name__}"
        )


def _deployment_from_entry(
    name: str,
    args: list[str],
    env: dict[str, str] | None = None,
) -> str | None:
    tokens: list[str] = []
    for arg in args:
        try:
            tokens.extend(shlex.split(arg))
        except ValueError:
            tokens.append(arg)
    for index, token in enumerate(tokens):
        if token == "--deployment" and index + 1 < len(tokens):
            deployment = _clean_deployment_token(tokens[index + 1])
            if deployment:
                return deployment
        if token.startswith("--deployment="):
            deployment = _clean_deployment_token(token.split("=", 1)[1])
            if deployment:
                return deployment
    joined = " ".join(args)
    match = re.search(r"--deployment(?:=|\s+)([^\s]+)", joined)
    if match:
        deployment = _clean_deployment_token(match.group(1))
        if deployment:
            return deployment
    if env and env.get("OKG_DEPLOYMENT"):
        return env["OKG_DEPLOYMENT"]
    return name if name.startswith("okg-") else None


def _clean_deployment_token(value: str) -> str:
    return value.strip().strip("'\"").rstrip(";")


def _is_timeout(error: str) -> bool:
    """The MCP SDK's own wait-for-response timeout, the one the probe
    timeout governs (not the 0.25 s database-port preflight)."""
    return "Timed out while waiting for response" in error


def _is_surface_kind_withheld(exc: Exception) -> bool:
    """True only for the server's named refusal of `inspect(kind=surface)`
    (`agent_surface_inspect_kind_unserved`), as a read-only launch gives it."""
    if getattr(exc, "stage", None) != "tool_call":
        return False
    try:
        envelope = json.loads(str(exc))
    except (TypeError, ValueError):
        return False
    error = envelope.get("error") if isinstance(envelope, dict) else None
    if not isinstance(error, dict):
        return False
    data = error.get("data")
    return (
        error.get("code") == "agent_surface_inspect_kind_unserved"
        and isinstance(data, dict)
        and data.get("kind") == "surface"
    )


def _surface_from_initialize_instructions(
    server: RegisteredMcpServer,
    *,
    instructions: str,
    server_info: dict[str, Any] | None,
    probe_error: str,
    self_report_allowed: bool = False,
) -> dict[str, Any]:
    info = server_info or {}
    from okg.substrate.mcp.capability_probe import split_generated_instructions
    from okg.substrate.mcp.dataset_card import split_dataset_card

    # PACT dataset-card: the served text starts with the dataset card, which
    # depends on the database and is not in the expectation. Strip it and the
    # generated lines, so both sides hash the same authored text.
    _card, after_card = split_dataset_card(instructions)
    authored, _generated = split_generated_instructions(after_card)
    # A current server puts okg's version and revision in serverInfo; an
    # older one reports the MCP SDK's version there, which is not okg's and
    # is never compared as if it were.
    self_report = (
        parse_server_info_version(info.get("version"))
        if self_report_allowed else None
    )
    return {
        "kind": "mcp_surface",
        "schema": (
            INITIALIZE_SELF_REPORT_SCHEMA if self_report is not None
            else "okg.mcp.surface_freshness.legacy_initialize.v1"
        ),
        "source": "initialize_instructions",
        "deployment_name": server.deployment,
        "package_name": "okg" if self_report is not None else info.get("name"),
        "package_version": (
            self_report["package_version"] if self_report is not None
            else None
        ),
        "server_info": dict(info),
        "git_revision": (
            self_report["git_revision"] if self_report is not None else None
        ),
        "instructions_hash": _sha256_text(instructions),
        "instructions_bytes": len(instructions.encode("utf-8")),
        "authored_instructions_hash": _sha256_text(authored),
        "untrusted_data_contract_present": (
            UNTRUSTED_DATA_CONTRACT in instructions
        ),
        "untrusted_data_contract_hash": _sha256_text(
            UNTRUSTED_DATA_CONTRACT
        ),
        "self_reported": self_report is not None,
        "probe_error": probe_error,
    }


def _extract_surface_payload(result: dict[str, Any]) -> dict[str, Any] | None:
    structured = result.get("structuredContent")
    if isinstance(structured, dict):
        if isinstance(structured.get("result"), dict):
            return structured["result"]
        return structured
    for item in result.get("content") or []:
        if isinstance(item, dict):
            if isinstance(item.get("json"), dict):
                return item["json"]
            text = item.get("text")
            if isinstance(text, str):
                try:
                    parsed = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if isinstance(parsed, dict):
                    return parsed
    return None


def _is_surface_payload(payload: dict[str, Any]) -> bool:
    return (
        payload.get("kind") == "mcp_surface"
        or payload.get("schema") == "okg.mcp.surface_freshness.v1"
        or (
            "instructions_hash" in payload
            and "untrusted_data_contract_present" in payload
        )
    )


def _surface_summary(surface: dict[str, Any] | None) -> dict[str, Any] | None:
    if surface is None:
        return None
    return {
        "source": surface.get("source"),
        "schema": surface.get("schema"),
        "package_version": surface.get("package_version"),
        "git_revision": surface.get("git_revision"),
        "instructions_hash": surface.get("instructions_hash"),
        "untrusted_data_contract_present": surface.get(
            "untrusted_data_contract_present"
        ),
        # PACT dataset-card-switch: "on" or "off" (`mcp.dataset_card`); None
        # from a server that predates the card or was given its own text.
        "dataset_card_state": surface.get("dataset_card_state"),
    }


def _fix_hint(deployment: str) -> str:
    return (
        f"restart or upgrade the registered MCP server for {deployment}: "
        f"{okg_prefix()} deployment up {deployment} --target prod, then restart "
        "the MCP client"
    )


def _redacted_command(argv: list[str]) -> list[str]:
    redacted: list[str] = []
    for token in argv:
        if "://" in token or token.startswith("postgres"):
            redacted.append("<redacted-dsn>")
        else:
            redacted.append(token)
    return redacted


def _sha256_text(text: str) -> str:
    import hashlib

    return f"sha256:{hashlib.sha256(text.encode('utf-8')).hexdigest()}"
