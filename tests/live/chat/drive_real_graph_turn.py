"""Drive one grounded turn through a real container, a real MCP server, and a real graph.

What is real here.

* The OpenWebUI container. By default the image the product pins and tests
  (``okg.substrate.chat.instance.PINNED_IMAGE_REF``, the only entry in
  ``TESTED_IMAGES``): the published compatibility build of Open WebUI 0.11.4,
  named by its registry digest and checked against it, configured only
  through its own HTTP API. No mounted file, no locally built tag.
* The graph tool. A real ``okg mcp-serve`` process, started from a
  wheel-installed virtual environment OUTSIDE any source checkout, serving the
  real seven-operator read surface over streamable-http with a bearer token
  minted for this run, reading the real published graph of the named
  deployment.
* The graph. The published generation of that deployment, read twice -- once by
  the MCP server the container calls, once directly through the ``okg``
  command line in the same wheel virtual environment.

What is a fixture: the model, and only the model. It holds no provider
credential, composes no language of its own about the graph, and answers only
by quoting the two values it finds in the tool result it was handed. So the
stored answer can exist only downstream of a real tool call whose real result
came back.

What it does not claim: nothing about a real language model, a browser, an
identity provider, two users, more than one deployment, or any CI lane.

Run it as a module::

    uv run python -m tests.live.chat.drive_real_graph_turn \
        --okg-executable <wheel venv>/bin/okg \
        --deployment <name> --node-id <graph node id> \
        --wheel <okg wheel> --out <run directory> [--port-range 5600-5699]

``--image`` defaults to the product pin and ``--expected-digest`` to the
digest that reference names. The image must already be in the local store.

``tests/live`` is not collected and this module is not a test. The hermetic
contracts it must satisfy live in
``tests/substrate/chat/test_real_graph_turn_driver.py``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import secrets
import signal
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from okg.substrate.chat.instance import PINNED_IMAGE_REF, TESTED_IMAGES
from okg.substrate.deployments.chat_block import PINNED_IMAGE_REFERENCE
from tests.live.chat.grounded_api_chain import (
    HOST_GATEWAY_ALIAS,
    ChainError,
    Refusal,
    Scrubber,
    assert_no_secret_shapes,
    check_row_provenance,
    digest_of,
    docker_run_command,
    free_port,
    parse_port_range,
    parse_task_acknowledgement,
    poll_until,
    read_jsonl,
    refusal_with_log_tail,
    trim_recorded_log,
)
from tests.live.chat.real_graph_chain import (
    MODEL_ID,
    SERVER_ID,
    TURN_TOOL_NAMES,
    GraphValues,
    POSTURE_DISCLOSURES,
    assert_tool_served,
    graph_values,
    mcp_serve_command,
    model_fixture_env,
    okg_child_cwd,
    served_manifest_identity,
    served_tool_names,
    same_graph_values,
    user_prompt,
    validate_real_graph_chain,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
MODEL_FIXTURE_MODULE = "tests.live.chat.real_graph_model_fixture"

#: The environment variable the MCP server reads its bearer token from. The
#: NAME is on the recorded command line; the value never is.
TOKEN_ENV = "OKG_REAL_GRAPH_PROOF_TOKEN"

#: Variables the ``okg`` child processes need from this process. Only their
#: NAMES are recorded -- ``OKG_MCP_RO_DSN`` carries a database connection
#: string and must not reach an artifact.
REQUIRED_OKG_ENV = ("OKG_DEPLOYMENTS_DIR", "OKG_MCP_RO_DSN")
OPTIONAL_OKG_ENV = ("OKG_PROFILES_DIR",)

ARTIFACTS = {
    "run.json": "what ran: image identity, wheel digests, okg version, "
    "deployment, timestamp, session id, and every docker and mcp-serve flag",
    "independent-read-before.json": "the graph read taken directly through the "
    "okg command line BEFORE the turn",
    "independent-read-after.json": "the same read taken after the turn, which "
    "must agree",
    "mcp-tools-list.json": "the tools/list reply of the served MCP endpoint, "
    "which is where the claim about WHICH operator was served comes from",
    "model/model.jsonl": "every request OpenWebUI made to the fixture model, "
    "written into a directory that holds no graph read of this run",
    "mcp-serve.log": "what the real MCP server printed, tail only",
    "vendor-response.txt": "the raw OpenWebUI response to the chat API call",
    "stored-message.json": "the assistant turn read back from the OpenWebUI "
    "API, including the function call and function call output it persisted, "
    "which is the request record for the real tool call",
    "openwebui.log": "the container log, tail only",
    "model.log": "what the model fixture process printed, tail only",
    "verdict.json": "the validated causal chain",
}


def terminate_process_group(process: subprocess.Popen) -> str:
    """Stop a child and anything it started, then say what it took.

    Each child is spawned with ``start_new_session=True``, so it leads its own
    session and its process-group id is its own pid. It can never signal a
    pytest-xdist worker group (issue 703), and signalling the GROUP is what
    stops a grandchild holding the model or MCP port.
    """
    if process.poll() is not None:
        return "already exited"
    try:
        group = os.getpgid(process.pid)
    except ProcessLookupError:
        return "gone before teardown"
    for name, number in (("SIGTERM", signal.SIGTERM), ("SIGKILL", signal.SIGKILL)):
        try:
            os.killpg(group, number)
        except ProcessLookupError:
            return f"gone before {name}"
        try:
            process.wait(timeout=10)
            return f"stopped by {name}"
        except subprocess.TimeoutExpired:
            continue
    return "still running after SIGKILL"


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _run(
    command: list[str],
    *,
    check: bool = True,
    env: dict[str, str] | None = None,
    cwd: str | None = None,
) -> subprocess.CompletedProcess:
    return subprocess.run(  # noqa: S603
        command, check=check, capture_output=True, text=True, env=env, cwd=cwd
    )


def image_identity(image: str, expected_digest: str) -> dict[str, Any]:
    """Resolve the image and refuse anything but the pinned published bytes."""
    if not PINNED_IMAGE_REFERENCE.match(image):
        raise Refusal(
            f"--image {image!r} is not a pinned reference; the repository rule "
            "(chat_block.PINNED_IMAGE_REFERENCE) admits a ':vX.Y.Z' tag or an "
            "'@sha256:' digest only"
        )
    record = json.loads(_run(["docker", "image", "inspect", image]).stdout)[0]
    digests = list(record.get("RepoDigests") or [])
    if not any(digest.endswith("@" + expected_digest) for digest in digests):
        raise Refusal(
            f"{image} does not carry the expected registry digest "
            f"{expected_digest}; inspect reports {digests!r}. A locally built "
            "image has no RepoDigest and is refused here on purpose."
        )
    labels = record.get("Config", {}).get("Labels") or {}
    return {
        "image": image,
        # None for an image the product has not measured (re-measuring only).
        "tested_image_version": TESTED_IMAGES.get(image),
        "image_is_product_pin": image == PINNED_IMAGE_REF,
        "repo_digests": digests,
        "expected_digest": expected_digest,
        "image_id": record["Id"],
        "image_version_label": labels.get("org.opencontainers.image.version"),
        "image_revision_label": labels.get("org.opencontainers.image.revision"),
    }


def okg_revision() -> dict[str, Any]:
    """The repository state of the DRIVER, which is the only checkout involved.

    The okg under test is the wheel one; this records that the driver and its
    contracts are the committed ones.
    """
    head = _run(["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"]).stdout.strip()
    dirty = bool(
        _run(["git", "-C", str(REPO_ROOT), "status", "--porcelain"]).stdout.strip()
    )
    return {"okg_head": head, "okg_worktree_dirty": dirty}


def okg_child_env(token: str) -> dict[str, str]:
    """The environment for the ``okg`` children, with the run token added.

    Refuses rather than proceeding when a variable the children need is
    missing, because a default would silently point the proof at a different
    deployments root or make the MCP server connect as the owner role.
    """
    missing = [name for name in REQUIRED_OKG_ENV if not os.environ.get(name)]
    if missing:
        raise Refusal(
            f"set {', '.join(missing)} before running this proof; the MCP "
            "server must resolve the named deployment and connect through the "
            "least-privilege okg_mcp role DSN, and neither has a safe default here"
        )
    env = dict(os.environ)
    env[TOKEN_ENV] = token
    return env


def wheel_identity(okg_executable: Path, wheels: list[Path]) -> dict[str, Any]:
    """Prove the served okg came from wheels, not from this checkout."""
    if REPO_ROOT in okg_executable.resolve().parents:
        raise Refusal(
            f"--okg-executable {okg_executable} lives inside this checkout; the "
            "point of this proof is a checkout-free okg installed from wheels"
        )
    python = okg_executable.resolve().with_name("python")
    # One string, deliberately not two adjacent literals inside the list: an
    # implicit concatenation there is the defect CodeQL reports and is easy to
    # write by accident. ``archi`` is no longer an okg dependency, so its
    # absence is recorded as "-" rather than refused; okg must be there.
    program = (
        "import importlib.metadata as m\n"
        "def v(n):\n"
        "    try:\n"
        "        return m.version(n)\n"
        "    except m.PackageNotFoundError:\n"
        "        return '-'\n"
        "print(v('okg'));print(v('archi'))"
    )
    version = _run([str(python), "-c", program]).stdout.split()
    if len(version) != 2 or version[0] == "-":
        raise Refusal(
            f"{python} did not report an installed okg distribution; it printed "
            f"{version!r}, so the proof cannot say which okg served it"
        )
    return {
        "okg_executable": str(okg_executable.resolve()),
        "okg_distribution_version": version[0],
        "archi_distribution_version": None if version[1] == "-" else version[1],
        "wheel_sha256": {path.name: sha256_file(path) for path in wheels},
    }


class Client:
    def __init__(self, base: str, token: str = "") -> None:
        self.base, self.token = base, token

    def call(
        self, method: str, path: str, payload: Any = None, timeout: int = 120
    ) -> tuple[Any, str]:
        """Return ``(decoded, raw_text)``; the raw bytes are kept for the record."""
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["Authorization"] = "Bearer " + self.token
        request = urllib.request.Request(
            self.base + path,
            data=None if payload is None else json.dumps(payload).encode(),
            method=method,
            headers=headers,
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
                raw = response.read()
                content_type = response.headers.get("Content-Type", "")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace")
            raise Refusal(
                f"OpenWebUI {method} {path} returned HTTP {exc.code}: {detail}"
            ) from exc
        text = raw.decode("utf-8", "replace")
        if not raw or "application/json" not in content_type:
            return None, text
        try:
            return json.loads(raw), text
        except json.JSONDecodeError as exc:
            raise Refusal(
                f"OpenWebUI {method} {path} claimed {content_type!r} and sent "
                f"un-parseable bytes: {text[:400]!r}"
            ) from exc

    def json(self, method: str, path: str, payload: Any = None, timeout: int = 120) -> Any:
        return self.call(method, path, payload, timeout)[0]


def wait_for(url: str, timeout: int, *, host_header: str = "") -> None:
    def probe() -> str:
        headers = {"Host": host_header} if host_header else {}
        try:
            request = urllib.request.Request(url, headers=headers)
            urllib.request.urlopen(request, timeout=5).close()  # noqa: S310
        except urllib.error.HTTPError:
            # An HTTP status means the listener answered. Readiness is what is
            # being probed, not authorisation.
            return ""
        except Exception as exc:  # noqa: BLE001
            return f"{type(exc).__name__}: {exc}"
        return ""

    poll_until(
        probe,
        lambda failure: failure == "",
        timeout=timeout,
        on_timeout=lambda failure: Refusal(
            f"timed out after {timeout}s waiting for {url} ({failure})"
        ),
        clock=time.monotonic,
        sleep=time.sleep,
        interval=2.0,
    )


def independent_graph_read(
    *,
    okg_executable: Path,
    deployment: str,
    node_id: str,
    env: dict[str, str],
    cwd: str,
    out: Path,
    name: str,
    scrub: Scrubber,
) -> GraphValues:
    """Read the two evidence values straight from the graph, not through MCP.

    A different path on purpose: this is the ``okg`` command line resolving the
    deployment manifest DSN, while the MCP server the container calls connects
    through the least-privilege okg_mcp role DSN. The validator requires the persisted answer
    to carry exactly what THIS read reports, so the chain is cross-checked
    against the graph rather than against itself.
    """
    from tests.live.chat.real_graph_chain import SEARCH_LIMIT

    completed = _run(
        [
            str(okg_executable),
            "search",
            "--deployment",
            deployment,
            "--query",
            node_id,
            "--method",
            "lexical",
            "--limit",
            str(SEARCH_LIMIT),
            "--json",
        ],
        env=env,
        cwd=cwd,
    )
    scrub.write(out / name, completed.stdout)
    return graph_values(
        completed.stdout, node_id=node_id, origin=f"independent read ({name})"
    )


def mcp_jsonrpc(
    url: str, *, method: str, token: str, host_header: str, session: str = ""
) -> tuple[Any, str]:
    """One JSON-RPC call to the served MCP endpoint, returning (decoded, session).

    Used for tools/list only. The driver never calls tools/call: the graph
    answer must reach it through OpenWebUI, so the one thing the driver asks the
    server directly is WHICH tools it serves.
    """
    params: dict[str, Any] = {}
    if method == "initialize":
        params = {
            "protocolVersion": "2025-06-18",
            "capabilities": {},
            "clientInfo": {"name": "okg-real-graph-proof", "version": "1"},
        }
    body = json.dumps(
        {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    ).encode()
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
        "Authorization": "Bearer " + token,
        "Host": host_header,
    }
    if session:
        headers["mcp-session-id"] = session
    request = urllib.request.Request(url, data=body, method="POST", headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310
            raw = response.read().decode("utf-8", "replace")
            returned = response.headers.get("mcp-session-id") or session
    except urllib.error.HTTPError as exc:
        raise Refusal(
            f"the served MCP endpoint answered {method} with HTTP {exc.code}: "
            f"{exc.read().decode('utf-8', 'replace')[:300]}"
        ) from exc
    for line in raw.splitlines():
        if line.startswith("data: "):
            return json.loads(line[6:]), returned
    if not raw.strip():
        return None, returned
    return json.loads(raw), returned


def record_served_tools(
    *, mcp_port: int, token: str, host_header: str, out: Path, scrub: Scrubber
) -> list[str]:
    """Record what the endpoint advertises, and refuse if our operator is absent."""
    url = f"http://127.0.0.1:{mcp_port}/mcp"
    _, session = mcp_jsonrpc(
        url, method="initialize", token=token, host_header=host_header
    )
    mcp_jsonrpc(
        url,
        method="notifications/initialized",
        token=token,
        host_header=host_header,
        session=session,
    )
    payload, _ = mcp_jsonrpc(
        url,
        method="tools/list",
        token=token,
        host_header=host_header,
        session=session,
    )
    names = served_tool_names(payload)
    scrub.write(
        out / "mcp-tools-list.json",
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
    )
    for tool in TURN_TOOL_NAMES:
        assert_tool_served(names, tool)
    return names


def configure(
    client: Client, *, model_port: int, mcp_port: int, token: str
) -> None:
    """Point the container at the model fixture and the REAL MCP server.

    Through the container own admin API only -- no mounted file, no baked
    configuration. The MCP connection is a bearer connection because the real
    server was started with a token requirement.
    """
    client.json(
        "POST",
        "/openai/config/update",
        {
            "ENABLE_OPENAI_API": True,
            "OPENAI_API_BASE_URLS": [f"http://{HOST_GATEWAY_ALIAS}:{model_port}/v1"],
            "OPENAI_API_KEYS": [""],
            "OPENAI_API_CONFIGS": {"0": {"enable": True}},
        },
    )
    client.json(
        "POST",
        "/api/v1/configs/tool_servers",
        {
            "TOOL_SERVER_CONNECTIONS": [
                {
                    "url": f"http://{HOST_GATEWAY_ALIAS}:{mcp_port}/mcp",
                    "path": "",
                    "type": "mcp",
                    "auth_type": "bearer",
                    "key": token,
                    "headers": None,
                    "config": {
                        "enable": True,
                        "access_grants": [],
                        # search, and inspect for the turn's one graph read
                        # (D8: the text search result names no generation).
                        "function_name_filter_list": ",".join(TURN_TOOL_NAMES),
                    },
                    "info": {
                        "id": SERVER_ID,
                        "name": "OKG graph",
                        "description": "real okg mcp-serve read surface",
                    },
                }
            ]
        },
    )
    client.json("GET", "/api/models")


def run(
    *,
    image: str,
    expected_digest: str,
    out: Path,
    timeout: int,
    container: str,
    label: str,
    okg_executable: Path,
    deployment: str,
    node_id: str,
    wheels: list[Path],
    port_range: tuple[int, int] | None = None,
) -> dict[str, Any]:
    out = out.resolve()
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    scrub = Scrubber()
    scrub.hide(str(REPO_ROOT), "<repo>")
    scrub.hide(str(Path.home()), "<home>")

    web_port = free_port(port_range)
    model_port = free_port(port_range, taken=(web_port,))
    mcp_port = free_port(port_range, taken=(web_port, model_port))
    suffix = uuid.uuid4().hex[:10]
    session_id = uuid.uuid4().hex
    token = secrets.token_urlsafe(32)
    scrub.hide(token, "<redacted:mcp-bearer-token>")
    child_env = okg_child_env(token)
    for name in REQUIRED_OKG_ENV + OPTIONAL_OKG_ENV:
        value = child_env.get(name)
        if value and ("://" in value or name.endswith("_DSN")):
            scrub.hide(value, f"<redacted:{name.lower()}>")

    # 0.0.0.0 rather than 127.0.0.1, because a container on this platform
    # cannot reach a loopback-only listener on the host. The exposure is
    # bounded two ways and both are recorded: every request must present the
    # run bearer token, and the server refuses every HTTP Host header except
    # the one the container sends.
    okg_cwd = okg_child_cwd(str(okg_executable.resolve()), str(REPO_ROOT))
    allowed_host = f"{HOST_GATEWAY_ALIAS}:{mcp_port}"
    mcp_command = mcp_serve_command(
        okg_executable=str(okg_executable),
        deployment=deployment,
        host="0.0.0.0",  # noqa: S104
        port=mcp_port,
        token_env=TOKEN_ENV,
        allowed_hosts=(allowed_host,),
    )
    docker_command = docker_run_command(
        image=image, container=container, label=label, web_port=web_port
    )
    identity = {
        "schema": "okg.openwebui_real_graph_grounded_turn.v1",
        "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "session_id": session_id,
        "deployment": deployment,
        "node_id": node_id,
        "docker_run_command": docker_command,
        "mcp_serve_command": mcp_command,
        "mcp_allowed_host": allowed_host,
        "okg_child_cwd": okg_cwd,
        "posture_disclosures": POSTURE_DISCLOSURES,
        **served_manifest_identity(deployment, child_env),
        "okg_env_passed_through": sorted(
            name
            for name in REQUIRED_OKG_ENV + OPTIONAL_OKG_ENV
            if child_env.get(name)
        ),
        **image_identity(image, expected_digest),
        **wheel_identity(okg_executable, wheels),
        **okg_revision(),
        "container_name": container,
        "driver_sha256": sha256_file(Path(__file__).resolve()),
        "chain_module_sha256": sha256_file(
            Path(__file__).resolve().with_name("real_graph_chain.py")
        ),
        "merged_chain_module_sha256": sha256_file(
            Path(__file__).resolve().with_name("grounded_api_chain.py")
        ),
        "model_fixture_sha256": sha256_file(
            Path(__file__).resolve().with_name("real_graph_model_fixture.py")
        ),
        "docker_version": _run(
            ["docker", "version", "--format", "{{.Server.Version}}"]
        ).stdout.strip(),
        "python": platform.python_version(),
        "host_platform": platform.platform(),
    }

    window_start = datetime.now(timezone.utc)
    password = secrets.token_urlsafe(24)
    email = f"proof-{suffix}@localhost.test"
    scrub.hide(password, "<redacted:admin-password>")
    scrub.hide(email, "<redacted:admin-email>")

    before = independent_graph_read(
        okg_executable=okg_executable,
        deployment=deployment,
        node_id=node_id,
        env=child_env,
        cwd=okg_cwd,
        out=out,
        name="independent-read-before.json",
        scrub=scrub,
    )

    # The model fixture writes into its OWN directory, which holds no graph
    # read of this run. Its recorder could otherwise sit beside
    # independent-read-before.json, and a reviewer would have to read the
    # fixture to know it never opened that file.
    model_out = out / "model"
    model_out.mkdir()
    mcp_log = (out / "mcp-serve.log").open("w", encoding="utf-8")
    model_log = (out / "model.log").open("w", encoding="utf-8")
    # Both children lead their own session, hold no inherited stdin, and print
    # to a file rather than this process stream (issue 703).
    mcp = subprocess.Popen(  # noqa: S603
        mcp_command,
        cwd=okg_cwd,
        env=child_env,
        stdin=subprocess.DEVNULL,
        stdout=mcp_log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    model = subprocess.Popen(  # noqa: S603
        [
            sys.executable,
            "-m",
            MODEL_FIXTURE_MODULE,
            "--model-port",
            str(model_port),
            "--out",
            str(model_out),
            "--session-id",
            session_id,
        ],
        cwd=REPO_ROOT,
        # A pruned environment, so the fixture demonstrably cannot hold a
        # database connection string, a deployments root, or the run token.
        env=model_fixture_env(os.environ),
        stdin=subprocess.DEVNULL,
        stdout=model_log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    try:
        wait_for(f"http://127.0.0.1:{model_port}/v1/models", timeout=60)
        try:
            wait_for(
                f"http://127.0.0.1:{mcp_port}/mcp",
                timeout=timeout,
                host_header=allowed_host,
            )
        except Refusal as refused:
            mcp_log.flush()
            raise refusal_with_log_tail(
                str(refused),
                log_path=out / "mcp-serve.log",
                scrub=scrub,
                process_name="mcp-serve",
                exit_code=mcp.poll(),
            ) from refused
        served = record_served_tools(
            mcp_port=mcp_port,
            token=token,
            host_header=allowed_host,
            out=out,
            scrub=scrub,
        )
        _run(docker_command)
        base = f"http://127.0.0.1:{web_port}"
        wait_for(base + "/health", timeout=timeout)
        signup = Client(base).json(
            "POST",
            "/api/v1/auths/signup",
            {"name": "Real Graph Proof", "email": email, "password": password},
        )
        scrub.hide(signup["token"], "<redacted:session-token>")
        scrub.hide(signup.get("id") or "", "<redacted:user-id>")
        client = Client(base, signup["token"])
        configure(client, model_port=model_port, mcp_port=mcp_port, token=token)

        chat = client.json(
            "POST", "/api/v1/chats/new", {"chat": {"title": "real graph proof"}}
        )
        chat_id, user_id, assistant_id = chat["id"], str(uuid.uuid4()), str(uuid.uuid4())
        prompt = user_prompt(node_id)
        payload = {
            "model": MODEL_ID,
            "chat_id": chat_id,
            "id": assistant_id,
            "session_id": f"proof-{suffix}",
            # OpenWebUI runs its server-side tool loop while consuming the
            # upstream stream; a non-stream request would hand the tool call
            # back to this caller instead of executing it.
            "stream": True,
            "messages": [{"id": user_id, "role": "user", "content": prompt}],
            "user_message": {
                "id": user_id,
                "role": "user",
                "content": prompt,
                "parentId": None,
                "childrenIds": [assistant_id],
                "models": [MODEL_ID],
                "timestamp": int(time.time()),
            },
            "tool_ids": [f"server:mcp:{SERVER_ID}"],
            "background_tasks": {"title_generation": False, "tags_generation": False},
        }
        acknowledgement, raw_body = client.call(
            "POST", "/api/chat/completions", payload, timeout=timeout
        )
        scrub.write(out / "vendor-response.txt", raw_body)
        acknowledged = parse_task_acknowledgement(
            acknowledgement, expected_chat_id=chat_id
        )

        poll_until(
            lambda: read_jsonl(model_out / "model.jsonl"),
            lambda rows: len(rows) >= 2,
            timeout=timeout,
            on_timeout=lambda rows: ChainError(
                f"after {timeout}s the model had received {len(rows)} request(s); "
                "the grounded turn never completed"
            ),
            clock=time.monotonic,
            sleep=time.sleep,
        )

        def _read_stored() -> dict[str, Any]:
            row = client.json("GET", f"/api/v1/chats/{chat_id}")
            return (
                (((row or {}).get("chat") or {}).get("history") or {}).get("messages", {})
            ).get(assistant_id) or {}

        stored = poll_until(
            _read_stored,
            lambda row: bool(
                row.get("content") or row.get("output") or row.get("error")
            ),
            timeout=timeout,
            on_timeout=lambda row: ChainError(
                f"after {timeout}s OpenWebUI had not persisted the assistant "
                f"result; last read was {row!r:.300}"
            ),
            clock=time.monotonic,
            sleep=time.sleep,
            interval=1.0,
        )
        scrub.write(
            out / "stored-message.json",
            json.dumps(stored, indent=2, sort_keys=True) + "\n",
        )
        after = independent_graph_read(
            okg_executable=okg_executable,
            deployment=deployment,
            node_id=node_id,
            env=child_env,
            cwd=okg_cwd,
            out=out,
            name="independent-read-after.json",
            scrub=scrub,
        )
        same_graph_values(before, after)
        # Read the rows BEFORE closing the window, so a row appended between
        # the two calls is not left outside it.
        model_rows = read_jsonl(model_out / "model.jsonl")
        window = (window_start, datetime.now(timezone.utc))
        verdict = validate_real_graph_chain(
            model_rows=model_rows,
            stored=stored,
            session_id=session_id,
            window=window,
            values=before,
            check_row_provenance=check_row_provenance,
        )
        verdict.update(
            {
                **identity,
                "vendor_response": acknowledged,
                "mcp_request_log_note": (
                    "The real MCP server keeps a per-request log of its own, "
                    "one okg.mcp_call_log row per tool invocation, written "
                    "through the okg_mcp role this run connects as. This "
                    "driver holds no database connection of its own and does "
                    "not read that table, so the request record in these "
                    "artifacts is the function call and function call output "
                    "OpenWebUI persisted (stored-message.json) plus the server "
                    "access log (mcp-serve.log)."
                ),
                "served_tool_names": served,
                "artifacts": ARTIFACTS,
            }
        )
        scrub.write(
            out / "run.json", json.dumps(identity, indent=2, sort_keys=True) + "\n"
        )
        scrub.write(
            out / "verdict.json", json.dumps(verdict, indent=2, sort_keys=True) + "\n"
        )
        return verdict
    finally:
        logs = _run(["docker", "logs", container], check=False)
        scrub.write(
            out / "openwebui.log", trim_recorded_log(logs.stdout + logs.stderr)
        )
        _run(["docker", "rm", "-f", container], check=False)
        teardown = {
            "mcp-serve": terminate_process_group(mcp),
            "model": terminate_process_group(model),
        }
        mcp_log.close()
        model_log.close()
        print(f"teardown: {teardown}")
        scrub.rewrite(out / "model" / "model.jsonl")
        for name in ("mcp-serve.log", "model.log"):
            path = out / name
            if path.exists():
                scrub.write(path, trim_recorded_log(path.read_text("utf-8")))
        if out.exists():
            checked = assert_no_secret_shapes(out)
            print(f"redaction checked {len(checked)} artifact(s): {checked}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--image",
        default=PINNED_IMAGE_REF,
        help="pinned image reference (default: the product pin, "
        "okg.substrate.chat.instance.PINNED_IMAGE_REF)",
    )
    parser.add_argument(
        "--expected-digest",
        default=None,
        help="registry content digest, sha256:... (default: the digest an "
        "@sha256: --image names)",
    )
    parser.add_argument(
        "--port-range",
        default=None,
        help="LOW-HIGH loopback ports the run may use (default: any free port)",
    )
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--timeout", type=int, default=240)
    parser.add_argument("--container-name", default="okg-s14-1795-openwebui")
    parser.add_argument("--label", default="okg.acceptance=s14-1795")
    parser.add_argument(
        "--okg-executable",
        type=Path,
        required=True,
        help="okg from a wheel-installed virtual environment, outside this checkout",
    )
    parser.add_argument("--deployment", required=True)
    parser.add_argument("--node-id", required=True)
    parser.add_argument(
        "--wheel",
        type=Path,
        action="append",
        default=[],
        help="a wheel whose sha256 is recorded with the run; repeat per wheel",
    )
    args = parser.parse_args()
    verdict = run(
        image=args.image,
        expected_digest=args.expected_digest or digest_of(args.image),
        port_range=parse_port_range(args.port_range) if args.port_range else None,
        out=args.out,
        timeout=args.timeout,
        container=args.container_name,
        label=args.label,
        okg_executable=args.okg_executable,
        deployment=args.deployment,
        node_id=args.node_id,
        wheels=list(args.wheel),
    )
    print(json.dumps(verdict, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
