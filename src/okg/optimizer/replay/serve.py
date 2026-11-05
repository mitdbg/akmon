"""Start ``okg mcp-serve`` for a replay, read what it serves, and check the pins.

The runner starts the graph server itself, from the same okg checkout it runs
from: ``mcp-serve --deployment <dir name> --branch <branch>`` over
streamable HTTP on a free loopback port, read-only (no ``--allow-writes``),
with a fresh random bearer token passed by variable name, the configured tool
list as ``OKG_MCP_TOOL_ALLOWLIST`` and the DSN read from ``serve.dsn_file``.
The server gets an allowlisted environment (``INHERITED_ENV``): the OpenAI key
and any OKG_* variable of the runner's shell are not passed on. Variables named in
``serve.pass_env`` are passed through (names in the setup digest, values never
written); a variable the deployment needs (a ``${NAME}`` in its DSN, the paid
embedding backend's key) that is neither passed nor set from
``serve.env_files`` is refused before the server starts
(``server_env_missing``).

It then reads the served surface (instructions plus every tool's name,
description and input schema) and the generation the server reads
(``inspect(target={kind: graph})``), and refuses, by name, a tool list other
than the configured one, a generation other than ``expect_generation``, and a
surface digest other than ``expect_surface_digest`` when one is pinned.

MCP access goes through a *connector* (url, headers, timeout -> session), so
tests can pass an in-process fake; ``mcp_connect`` is the real one.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import secrets
import signal
import socket
import subprocess
import sys
import time
from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from .config import RUNNER_SET_ENV, RunConfig, ServeConfig
from .errors import ReplayProblem, SurfaceChanged, refuse
from .setup import sha256_json, sha256_text

SERVER_START_TIMEOUT_S = 300.0
SERVER_STOP_TIMEOUT_S = 30.0
PROBE_TIMEOUT_S = 120.0
MCP_PATH = "/mcp"
TOKEN_ENV = "OKG_MCP_AUTH_TOKEN"
_SERVE_MAIN = "import sys; from okg.substrate.cli.main import console_main; sys.argv[0] = 'okg'; console_main()"
#: The only variables the server inherits from the runner (host plumbing, never a score input). Every
#: other variable, OKG_* and OPENAI_API_KEY included, is dropped: what the server reads beyond these comes
#: from the runner (DSN, token, tool list, deployments dir) or from serve.env_files, whose names and
#: prefixes are in the setup digest.
INHERITED_ENV = ("PATH", "HOME", "USER", "LOGNAME", "LANG", "TZ", "TMPDIR", "SHELL", "VIRTUAL_ENV", "PYTHONPATH",
                 "SSL_CERT_FILE", "SSL_CERT_DIR")
INHERITED_ENV_PREFIXES = ("LC_",)


@dataclass(frozen=True)
class ToolDef:
    name: str
    description: str
    input_schema: Mapping[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {"name": self.name, "description": self.description, "inputSchema": dict(self.input_schema)}


@dataclass(frozen=True)
class Surface:
    """What one MCP server serves: its instructions and its tools."""

    instructions: str
    tools: tuple[ToolDef, ...]

    def tools_json(self) -> list[dict[str, Any]]:
        return [t.as_dict() for t in sorted(self.tools, key=lambda t: t.name)]

    @property
    def digest(self) -> str:
        return sha256_json({"instructions": self.instructions, "tools": self.tools_json()})

    @property
    def tools_digest(self) -> str:
        return sha256_json(self.tools_json())

    @property
    def instructions_digest(self) -> str:
        return sha256_text(self.instructions)

    @property
    def tool_names(self) -> list[str]:
        return sorted(t.name for t in self.tools)


class McpSession(Protocol):
    instructions: str | None

    async def list_tools(self) -> list[ToolDef]: ...

    async def call_tool(self, name: str, args: dict[str, Any]) -> tuple[str, bool]: ...

    async def read_resource(self, uri: str) -> str: ...


Connector = Callable[[str, Mapping[str, str], float], AbstractAsyncContextManager[McpSession]]


class _RealSession:
    def __init__(self, session: Any, instructions: str | None) -> None:
        self._session = session
        self.instructions = instructions

    async def list_tools(self) -> list[ToolDef]:
        tools = (await self._session.list_tools()).tools
        return [ToolDef(t.name, t.description or "", t.inputSchema or {"type": "object", "properties": {}})
                for t in tools]

    async def call_tool(self, name: str, args: dict[str, Any]) -> tuple[str, bool]:
        result = await self._session.call_tool(name, args)
        return result_text(result), bool(result.isError)

    async def read_resource(self, uri: str) -> str:
        from pydantic import AnyUrl

        result = await self._session.read_resource(AnyUrl(uri))
        return "".join(getattr(block, "text", "") or "" for block in result.contents)


def result_text(result: Any) -> str:
    """The text a tool result hands the model (answer.py _result_text)."""
    parts = []
    for block in result.content or []:
        if getattr(block, "type", None) == "text":
            parts.append(block.text)
        else:
            parts.append(json.dumps(block.model_dump(mode="json"), ensure_ascii=False))
    if not parts and getattr(result, "structuredContent", None) is not None:
        parts.append(json.dumps(result.structuredContent, ensure_ascii=False))
    return "\n".join(parts)


@contextlib.asynccontextmanager
async def mcp_connect(url: str, headers: Mapping[str, str], timeout: float) -> AsyncIterator[McpSession]:
    """One streamable-HTTP MCP session; every request carries ``headers``."""
    import httpx
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client

    async with httpx.AsyncClient(headers=dict(headers), timeout=httpx.Timeout(timeout, connect=20.0)) as http:
        async with streamable_http_client(url, http_client=http) as (read, write, _sid):
            async with ClientSession(read, write) as session:
                init = await session.initialize()
                yield _RealSession(session, init.instructions)


async def probe_surface(connector: Connector, url: str, headers: Mapping[str, str]) -> Surface:
    async with connector(url, headers, PROBE_TIMEOUT_S) as session:
        tools = await session.list_tools()
        return Surface(session.instructions or "", tuple(tools))


#: Recorded when the server's read-side metadata has no `surface_text` field
#: (a server from before issue #4280). A value, so two such setups compare
#: equal and differ from any server that publishes a digest.
SURFACE_TEXT_NOT_PUBLISHED = "not_published"


async def read_surface_text(connector: Connector, url: str, headers: Mapping[str, str], deployment: str) -> str:
    """The served-text digest from ``okg://readside-metadata/<deployment>`` (issue #4280).

    Covers what the tool list does not show: refusal fix_hint overrides and
    the deployment's surface_text layer. A resource that cannot be read is
    refused by name; a readable one without the field records ``not_published``.
    """
    uri = f"okg://readside-metadata/{deployment}"
    try:
        async with connector(url, headers, PROBE_TIMEOUT_S) as session:
            text = await session.read_resource(uri)
        payload = json.loads(text)
    except Exception as exc:  # noqa: BLE001 - re-raised as a named refusal
        raise refuse("surface_text_unreadable",
                     f"could not read {uri} ({type(exc).__name__}); the setup must name the served text") from exc
    report = payload.get("surface_text") if isinstance(payload, dict) else None
    if report is None:
        return SURFACE_TEXT_NOT_PUBLISHED
    digest = report.get("digest") if isinstance(report, dict) else None
    if not isinstance(digest, str) or not digest.startswith("sha256:"):
        raise refuse("surface_text_unreadable", f"{uri} has a surface_text field without a sha256 digest")
    return digest


async def read_generation(connector: Connector, url: str, headers: Mapping[str, str]) -> str:
    """The generation the server reads, from ``inspect(target={kind: graph})``'s status line."""
    from okg.substrate.mcp.result_text import result_generation

    async with connector(url, headers, PROBE_TIMEOUT_S) as session:
        text, is_error = await session.call_tool("inspect", {"target": {"kind": "graph"}})
    generation = None if is_error else result_generation(text)
    if not generation:
        raise refuse("generation_unreadable",
                     "inspect(target={kind: graph}) did not name a generation"
                     + (" (the call returned an error)" if is_error else ""))
    return generation


def check_pins(serve: ServeConfig, surface: Surface, generation: str) -> None:
    """Refuse a served tool list, generation or surface other than the run file's."""
    served = set(surface.tool_names)
    wanted = set(serve.tools)
    if served != wanted:
        raise refuse("served_tools_mismatch",
                     f"the server serves {sorted(served)}, the run file names {sorted(wanted)}", "/serve/tools")
    if generation != serve.expect_generation:
        raise refuse("generation_mismatch",
                     f"the server reads generation {generation}, the run file expects {serve.expect_generation}",
                     "/serve/expect_generation")
    if serve.expect_surface_digest is not None and surface.digest != serve.expect_surface_digest:
        raise refuse("surface_mismatch",
                     f"the served surface digest is {surface.digest}, the run file pins "
                     f"{serve.expect_surface_digest}", "/serve/expect_surface_digest")


def check_unchanged(before: Surface, generation_before: str, after: Surface, generation_after: str) -> None:
    """After the last try: the server must still serve the same surface at the same generation."""
    if after.digest != before.digest:
        raise SurfaceChanged(ReplayProblem(
            "surface_changed", f"the served surface changed during the run ({before.digest} -> {after.digest})"))
    if generation_after != generation_before:
        raise SurfaceChanged(ReplayProblem(
            "generation_changed",
            f"the served generation changed during the run ({generation_before} -> {generation_after})"))


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@dataclass
class ServerHandle:
    url: str
    token: str
    process: subprocess.Popen[bytes]
    log_path: Path

    @property
    def headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    def __repr__(self) -> str:  # never show the token
        return f"ServerHandle(url={self.url!r}, pid={self.process.pid})"


def server_argv(config: RunConfig, port: int) -> list[str]:
    return [
        sys.executable, "-c", _SERVE_MAIN, "mcp-serve",
        "--deployment", config.deployment_dir.name, "--branch", config.serve.branch,
        "--transport", "streamable-http", "--host", "127.0.0.1", "--port", str(port),
        "--allowed-host", "127.0.0.1:*", "--allowed-host", "localhost:*",
        "--auth-token-env", TOKEN_ENV,
    ]


def server_env(config: RunConfig, token: str, base: Mapping[str, str] | None = None) -> dict[str, str]:
    source = os.environ if base is None else base
    env = {k: v for k, v in source.items() if k in INHERITED_ENV or k.startswith(INHERITED_ENV_PREFIXES)}
    dsn = _read_secret(config.serve.dsn_file, "dsn_empty", "/serve/dsn_file")
    env.update({
        "OKG_DSN": dsn, "OKG_MCP_RO_DSN": dsn, TOKEN_ENV: token,
        "OKG_DEPLOYMENTS_DIR": str(config.deployment_dir.parent),
        "OKG_MCP_TOOL_ALLOWLIST": ",".join(sorted(config.serve.tools)),
    })
    for item in config.serve.env_files:
        env[item.name] = item.prefix + _read_secret(item.file, "env_file_empty", f"/serve/env_files/{item.name}")
    unset = [name for name in config.serve.pass_env if not (source.get(name) or "").strip()]
    if unset:
        raise refuse("pass_env_unset", f"serve.pass_env names {', '.join(unset)}, unset or empty in the runner's "
                     "environment", "/serve/pass_env")
    for name in config.serve.pass_env:
        env[name] = source[name]
    return env


def required_server_env(deployment_dir: Path) -> dict[str, str]:
    """Variables the served deployment reads from the environment, each with the reason.

    * every ``${NAME}`` in the manifest's DSN (``postgres.dsn`` or the legacy
      ``postgres_dsn``): the only manifest fields the read-only server
      interpolates from the environment (``deployments._resolve_postgres_dsn``
      and the DSN resolvers in ``cli/main.py``). Other fields' ``${NAME}``
      (a source's token, say) are read by ingestion, never by the server, so
      they are not required;
    * the API key of the search set's embedding backend when it is a paid
      ``openrouter:`` backend (the key name is the embedder's own
      ``API_KEY_ENV``), since search embeds each query with it.
    """
    import yaml

    from okg.substrate.deployments import _referenced_env_var_names
    from okg.substrate.deployments.sync_root import manifest_path_anchors
    from okg.substrate.embeddings.config import EmbeddingConfigError, parse_embedding_config
    from okg.substrate.embeddings.identity import OPENROUTER_PREFIX
    from okg.substrate.embeddings.openrouter import API_KEY_ENV

    path = deployment_dir / "deployment.yaml"
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise refuse("deployment_manifest_unreadable", f"{path.name} cannot be read ({type(exc).__name__})",
                     "/deployment_dir") from exc
    if not isinstance(raw, dict):
        raise refuse("deployment_manifest_unreadable", f"{path.name} is not a mapping", "/deployment_dir")
    anchors = {needle[2:-1] for needle in manifest_path_anchors(deployment_dir)}
    pg = raw.get("postgres")
    dsn_fields = {"postgres.dsn": pg.get("dsn") if isinstance(pg, dict) else None,
                  "postgres_dsn": raw.get("postgres_dsn")}
    out = {name: f"referenced in deployment.yaml {field}" for field, value in dsn_fields.items()
           for name in _referenced_env_var_names(value) if name not in anchors}
    values: dict[str, object] = {}
    if "embedding" in raw:
        values["singular"] = raw["embedding"]
    if "embeddings" in raw:
        values["plural"] = raw["embeddings"]
    try:
        search = parse_embedding_config(**values).search_vector_set
    except EmbeddingConfigError as exc:
        raise refuse("deployment_embeddings_invalid", f"the deployment's embeddings block is refused: {exc}",
                     "/deployment_dir") from exc
    if search is not None and search.backend.strip().startswith(OPENROUTER_PREFIX):
        out[API_KEY_ENV] = f"the search set's embedding backend ({search.backend.split('@')[0]}) embeds each query"
    return out


def check_server_env(config: RunConfig) -> None:
    """Refuse a run whose server would miss a variable its deployment needs (named, never read as empty)."""
    given = {*INHERITED_ENV, *RUNNER_SET_ENV, *config.serve.pass_env, *(e.name for e in config.serve.env_files)}
    missing = {name: why for name, why in required_server_env(config.deployment_dir).items() if name not in given}
    if missing:
        raise refuse("server_env_missing",
                     "the served deployment needs " + "; ".join(f"{k} ({v})" for k, v in sorted(missing.items()))
                     + ": add each to serve.pass_env or serve.env_files", "/serve/pass_env")


def _read_secret(path: Path, code: str, pointer: str) -> str:
    value = path.read_text(encoding="utf-8").strip()
    if not value:
        raise refuse(code, f"{path.name} is empty", pointer)
    return value


@contextlib.asynccontextmanager
async def start_server(config: RunConfig, log_path: Path, connector: Connector) -> AsyncIterator[ServerHandle]:
    """Start the server, wait until an MCP session opens, stop it on exit."""
    check_server_env(config)
    port = free_port()
    token = secrets.token_hex(32)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("ab") as log:
        process = subprocess.Popen(  # noqa: S603 - argv built from validated config
            server_argv(config, port), cwd=str(config.deployment_dir), env=server_env(config, token),
            stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True,
        )
    handle = ServerHandle(f"http://127.0.0.1:{port}{MCP_PATH}", token, process, log_path)
    try:
        await _wait_ready(handle, connector)
        yield handle
    finally:
        stop_server(process)


async def _wait_ready(handle: ServerHandle, connector: Connector) -> None:
    deadline = time.monotonic() + SERVER_START_TIMEOUT_S
    last = "no attempt"
    while time.monotonic() < deadline:
        if handle.process.poll() is not None:
            raise refuse("server_exited",
                         f"okg mcp-serve exited with code {handle.process.returncode} before serving; "
                         f"see {handle.log_path}")
        try:
            async with connector(handle.url, handle.headers, 20.0) as session:
                await session.list_tools()
            return
        except Exception as exc:  # noqa: BLE001 - not up yet; retried until the deadline
            last = type(exc).__name__
        await asyncio.sleep(1.0)
    raise refuse("server_start_timeout",
                 f"okg mcp-serve did not serve within {SERVER_START_TIMEOUT_S:g} s (last: {last}); "
                 f"see {handle.log_path}")


def stop_server(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    with contextlib.suppress(ProcessLookupError):
        os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(SERVER_STOP_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGKILL)
        process.wait(30)
