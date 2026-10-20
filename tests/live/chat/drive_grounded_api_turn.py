"""Drive one grounded API turn through a disposable real OpenWebUI container.

What is real here: the OpenWebUI container. By default it is the image the
product itself pins and tests (``okg.substrate.chat.instance.PINNED_IMAGE_REF``,
the only entry in ``TESTED_IMAGES``): the published compatibility build of
Open WebUI 0.11.4, named by its registry digest and checked against it,
configured only through its own HTTP API the way an operator would -- no
mounted file, no locally invented tag, no baked configuration.

What is a fixture: the model and the MCP server. Both are deterministic local
processes. The model composes no language of its own; it answers only when the
tool result handed back to it still carries the generation, node, and source
strings. So a stored answer is proof that OpenWebUI really called the tool and
really returned its result, which is the single exit item this proof claims.

What it does not claim: nothing about a browser, an identity provider, two
users, a real graph, or a deployment.

Run it as a module::

    uv run python -m tests.live.chat.drive_grounded_api_turn \
        --out <run directory> [--port-range 5600-5699]

``--image`` defaults to the product pin and ``--expected-digest`` to the
digest that reference names; pass both only to measure another image. The
image must already be in the local store (``docker pull`` it first): the
driver never pulls.

No CI lane runs this file; ``tests/live`` is not collected without
``OKG_CHAT_LIVE_PROOF=1`` and this module is not a test. The hermetic
contracts it must satisfy live in
``tests/substrate/chat/test_grounded_api_turn_driver.py``.
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
    MODEL_ID,
    SERVER_ID,
    TOOL_NAME,
    ChainError,
    Refusal,
    Scrubber,
    assert_no_secret_shapes,
    digest_of,
    docker_run_command,
    free_port,
    parse_port_range,
    parse_task_acknowledgement,
    poll_until,
    read_jsonl,
    trim_recorded_log,
    validate_causal_chain,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_MODULE = "tests.live.chat.grounded_api_fixture"

#: Files whose whole content is recorded. Each has a stated job so a reviewer
#: can tell which link of the chain a given artifact witnesses.
ARTIFACTS = {
    "run.json": "what ran: image identity, revisions, digests, timestamp, "
    "session id, and every docker run flag",
    "mcp.jsonl": "the MCP request and response, as the MCP server saw them",
    "model.jsonl": "every request OpenWebUI made to the model",
    "vendor-response.txt": "OpenWebUI's raw response to the chat API call",
    "stored-message.json": "the assistant turn read back from OpenWebUI's API",
    "openwebui.log": "the container log, tail only, trailing whitespace stripped",
    "fixture.log": "what the model and MCP fixture processes printed, "
    "trailing whitespace stripped",
    "verdict.json": "the validated causal chain",
}

def terminate_process_group(process: subprocess.Popen) -> str:
    """Stop the fixture and anything it started, then say what it took.

    The fixture is spawned with ``start_new_session=True``, so it leads its own
    session and its process-group id is its own pid. Two things follow. It can
    never signal the pytest-xdist worker group that may be running this repo's
    own tests, which is the lane nondeterminism issue #703 exists to prevent.
    And because the group is its own, signalling the GROUP rather than the one
    process is safe here and is what stops a grandchild -- an HTTP server
    thread is fine, but a child process holding the model or MCP port would
    otherwise survive the run and break the next one by keeping a port.
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


def _run(command: list[str], *, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(  # noqa: S603
        command, check=check, capture_output=True, text=True
    )


def image_identity(image: str, expected_digest: str) -> dict[str, Any]:
    """Resolve the image and refuse anything but the pinned published bytes."""
    if not PINNED_IMAGE_REFERENCE.match(image):
        raise Refusal(
            f"--image {image!r} is not a pinned reference; the repository's own "
            "rule (chat_block.PINNED_IMAGE_REFERENCE) admits a ':vX.Y.Z' tag or "
            "an '@sha256:' digest only"
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
        # The version the product records for this image, or None for an image
        # the product has not measured. A None here is a run against an
        # untested image, which is legitimate only while re-measuring.
        "tested_image_version": TESTED_IMAGES.get(image),
        "image_is_product_pin": image == PINNED_IMAGE_REF,
        "repo_digests": digests,
        "expected_digest": expected_digest,
        "image_id": record["Id"],
        "image_size_bytes": record.get("Size"),
        "image_version_label": labels.get("org.opencontainers.image.version"),
        "image_revision_label": labels.get("org.opencontainers.image.revision"),
    }


def okg_revision() -> dict[str, Any]:
    head = _run(["git", "-C", str(REPO_ROOT), "rev-parse", "HEAD"]).stdout.strip()
    dirty = bool(
        _run(["git", "-C", str(REPO_ROOT), "status", "--porcelain"]).stdout.strip()
    )
    return {"okg_head": head, "okg_worktree_dirty": dirty}


def build_run_identity(
    image: dict[str, Any], container: str, session_id: str, command: list[str]
) -> dict[str, Any]:
    here = Path(__file__).resolve()
    return {
        "schema": "okg.openwebui_grounded_api_acceptance.v3",
        "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "session_id": session_id,
        "docker_run_command": command,
        **image,
        **okg_revision(),
        "container_name": container,
        "driver_sha256": sha256_file(here),
        "chain_module_sha256": sha256_file(here.with_name("grounded_api_chain.py")),
        "fixture_sha256": sha256_file(here.with_name("grounded_api_fixture.py")),
        "docker_version": _run(
            ["docker", "version", "--format", "{{.Server.Version}}"]
        ).stdout.strip(),
        "python": platform.python_version(),
        "host_platform": platform.platform(),
    }


class Client:
    def __init__(self, base: str, token: str = "") -> None:
        self.base, self.token = base, token

    def call(
        self, method: str, path: str, payload: Any = None, timeout: int = 120
    ) -> tuple[Any, str]:
        """Return ``(decoded, raw_text)``.

        The raw text is returned alongside because one of these calls answers
        with Server-Sent Events rather than JSON, and the record needs the
        bytes the vendor actually sent.
        """
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
        if not raw:
            return None, text
        if "application/json" not in content_type:
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


def wait_for(url: str, timeout: int) -> None:
    def probe() -> str:
        try:
            urllib.request.urlopen(url, timeout=5).close()  # noqa: S310
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


def configure(client: Client, model_port: int, mcp_port: int, server_id: str) -> None:
    """Point the container at the fixtures through its own admin API only."""
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
                    "auth_type": "none",
                    "key": "",
                    "headers": None,
                    "config": {
                        "enable": True,
                        "access_grants": [],
                        "function_name_filter_list": TOOL_NAME,
                    },
                    "info": {
                        "id": server_id,
                        "name": "OKG grounded fixture",
                        "description": "deterministic local acceptance fixture",
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
    command = docker_run_command(
        image=image, container=container, label=label, web_port=web_port
    )
    identity = build_run_identity(
        image_identity(image, expected_digest), container, session_id, command
    )
    window_start = datetime.now(timezone.utc)
    password = secrets.token_urlsafe(24)
    email = f"proof-{suffix}@localhost.test"
    scrub.hide(password, "<redacted:admin-password>")
    scrub.hide(email, "<redacted:admin-email>")

    fixture_log = (out / "fixture.log").open("w", encoding="utf-8")
    fixture = subprocess.Popen(  # noqa: S603
        [
            sys.executable,
            "-m",
            FIXTURE_MODULE,
            "--model-port",
            str(model_port),
            "--mcp-port",
            str(mcp_port),
            "--out",
            str(out),
            "--session-id",
            session_id,
        ],
        cwd=REPO_ROOT,
        # Detached from whatever is running this driver: its own session, no
        # inherited stdin, and its output on a file rather than the parent's
        # stream. Under pytest-xdist the parent's stdin is the control channel
        # to the controller, so a child holding it can make a lane
        # nondeterministic (#703).
        stdin=subprocess.DEVNULL,
        stdout=fixture_log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    try:
        wait_for(f"http://127.0.0.1:{model_port}/v1/models", timeout=60)
        _run(command)
        base = f"http://127.0.0.1:{web_port}"
        wait_for(base + "/health", timeout=timeout)
        signup = Client(base).json(
            "POST",
            "/api/v1/auths/signup",
            {"name": "Grounded API Proof", "email": email, "password": password},
        )
        scrub.hide(signup["token"], "<redacted:session-token>")
        scrub.hide(signup.get("id") or "", "<redacted:user-id>")
        client = Client(base, signup["token"])
        configure(client, model_port, mcp_port, SERVER_ID)

        chat = client.json(
            "POST", "/api/v1/chats/new", {"chat": {"title": "grounded API proof"}}
        )
        chat_id, user_id, assistant_id = chat["id"], str(uuid.uuid4()), str(uuid.uuid4())
        prompt = "Inspect the fixture node and cite its generation and source."
        message = {
            "id": user_id,
            "role": "user",
            "content": prompt,
            "parentId": None,
            "childrenIds": [assistant_id],
            "models": [MODEL_ID],
            "timestamp": int(time.time()),
        }
        payload = {
            "model": MODEL_ID,
            "chat_id": chat_id,
            "id": assistant_id,
            "session_id": f"proof-{suffix}",
            # OpenWebUI runs its server-side tool loop while consuming the
            # upstream stream. A non-stream request merely hands the model's
            # tool call back to the API caller without executing it, so this
            # request streams and the response is Server-Sent Events.
            "stream": True,
            "messages": [{"id": user_id, "role": "user", "content": prompt}],
            "user_message": message,
            "tool_ids": [f"server:mcp:{SERVER_ID}"],
            "background_tasks": {"title_generation": False, "tags_generation": False},
        }
        acknowledgement, raw_body = client.call(
            "POST", "/api/chat/completions", payload, timeout=timeout
        )
        # Kept whatever happens: a refused or dropped tool call is only
        # diagnosable from the vendor's own reply.
        scrub.write(out / "vendor-response.txt", raw_body)
        acknowledged = parse_task_acknowledgement(
            acknowledgement, expected_chat_id=chat_id
        )

        poll_until(
            lambda: (read_jsonl(out / "mcp.jsonl"), read_jsonl(out / "model.jsonl")),
            lambda pair: bool(pair[0]) and len(pair[1]) >= 2,
            timeout=timeout,
            on_timeout=lambda pair: ChainError(
                f"after {timeout}s OpenWebUI recorded {len(pair[0])} MCP call(s) "
                f"and {len(pair[1])} model request(s); the grounded turn never "
                "completed"
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
        # Read the rows BEFORE closing the window. Taking the end timestamp
        # first would leave any row appended between the two calls outside the
        # window, failing a run that was in fact correct.
        mcp_rows = read_jsonl(out / "mcp.jsonl")
        model_rows = read_jsonl(out / "model.jsonl")
        window = (window_start, datetime.now(timezone.utc))
        verdict = validate_causal_chain(
            mcp_rows=mcp_rows,
            model_rows=model_rows,
            stored=stored,
            session_id=session_id,
            window=window,
        )
        verdict.update(
            {
                **identity,
                "vendor_response": acknowledged,
                "persisted_message_has_sources": verdict["persisted"]["has_sources"],
                "sources_note": (
                    "Measured on the persisted message, which is where "
                    "OpenWebUI keeps citation sources (v0.11.0, and the 0.11.4 "
                    "compatibility image on 2026-09-30); the chat API "
                    "response itself is only a background-task "
                    "acknowledgement and never carried them. False is the "
                    "vendor's behaviour by construction: in the native "
                    "tool-calling path it builds citation sources only for "
                    "its own built-in tool names (search_web, fetch_url, "
                    "view_file, view_knowledge_file, query_knowledge_files, "
                    "query_chat_files), so an MCP tool result is never one."
                ),
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
        teardown = terminate_process_group(fixture)
        fixture_log.close()
        print(f"fixture teardown: {teardown}")
        for name in ("mcp.jsonl", "model.jsonl"):
            scrub.rewrite(out / name)
        spawned = out / "fixture.log"
        if spawned.exists():
            scrub.write(spawned, trim_recorded_log(spawned.read_text("utf-8")))
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
    parser.add_argument("--container-name", default="okg-s14-1183-openwebui")
    parser.add_argument("--label", default="okg.acceptance=s14-1183")
    args = parser.parse_args()
    verdict = run(
        image=args.image,
        expected_digest=args.expected_digest or digest_of(args.image),
        out=args.out,
        timeout=args.timeout,
        container=args.container_name,
        label=args.label,
        port_range=parse_port_range(args.port_range) if args.port_range else None,
    )
    print(json.dumps(verdict, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
