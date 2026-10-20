"""Drive the chat operator lifecycle through the product's own commands.

What is real here: the chat site container (by default the image the product
pins, ``okg.substrate.chat.instance.PINNED_IMAGE_REF``), its app database, a
real ``okg mcp-serve`` reading the named deployment's published graph, and
every ``okg chat-instance`` and ``okg chat`` command, run as an operator
would run them. What is a fixture: the model, and only the model --
``real_graph_model_fixture`` emits one graph ``search`` call for the node the
question names and answers only by quoting the generation and source the tool
result carried, so ``okg chat setup``'s conversation step can pass without a
GPU or a provider account.

The steps, each settled to pass, fail, or an expected failure
(``chat_lifecycle_chain.py``):

1. ``configure``: ``okg chat setup --configure-only`` writes the chat block;
   the proof adds ``chat.mcp.auth_token_env`` so the graph needs a bearer.
2. ``instance_up``: ``okg chat-instance up``.
3. ``setup``: ``okg chat setup``, whose conversation must use the graph.
4. ``plan``, ``sync``, ``readback_clean``.
5. ``drift_apply`` (community sharing on, through the vendor admin API),
   ``drift_readback_flags``, ``drift_sync``, ``drift_readback_clean``.
6. ``restart_down``, ``restart_up``, ``restart_readback_clean`` (and the
   admin's chats are kept).
7. ``rotation_apply`` (restart mcp-serve with a new bearer; the old one must
   get 401), ``rotation_readback_flags_stale_credential`` -- EXPECTED TO FAIL
   until lane A's ``chat-readback-stale-credential`` lands --,
   ``rotation_sync``, ``rotation_readback_clean``, ``rotation_turn``
   (``okg chat setup`` again: a grounded turn with the new bearer).

It writes ``verdict.json`` and one scrubbed record per command under
``--out``. Every credential it holds is minted here for this run -- the admin
password, the site secret, the provider key the fixture ignores, both graph
bearers, the admin session token -- and is replaced by a placeholder before
anything is written or printed; a credential shape left in any artifact fails
the run. The app database DSN is read from ``OKG_CHAT_APP_DATABASE_URL``, and
the graph server's ``OKG_MCP_RO_DSN`` (a DSN that logs in as the
least-privilege ``okg_mcp`` role) is passed through; both are scrubbed the
same way. Without ``OKG_MCP_RO_DSN``, mcp-serve derives an ``okg_mcp`` DSN
with the fixed local-development password
(``okg.substrate.local_database_roles``), only for a loopback database host,
and refuses to start for any other host; a database whose ``okg_mcp`` has
another password or no login then fails every search with
``mcp_pool_unavailable``.

Run it as a module, against a disposable deployment that has a published
graph and NO ``chat:`` block (the run writes one)::

    OKG_CHAT_APP_DATABASE_URL=<app database DSN> \\
    uv run python -m tests.live.chat.drive_chat_lifecycle \\
        --deployment <name> --node-id <graph node id> \\
        --container-network {gateway,pasta,host} --out <run directory> \\
        [--container-runtime podman] [--port-range 5600-5699] \\
        [--search-subtype <subtype> ...]

``tests/live`` is not collected and this module is not a test. The hermetic
contracts live in ``tests/substrate/chat/test_chat_lifecycle_driver.py``.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import secrets
import shutil
import signal
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
from okg.substrate.chat.setup import mcp_serve_argv
from okg.substrate.cli.child_argv import running_okg_argv
from tests.live.chat.chat_lifecycle_chain import (
    DRIFT_ADMIN_CONFIG_KEY,
    DRIFT_READBACK_KEY,
    ROTATION_READBACK_STEP,
    add_mcp_auth_token_env,
    command_json,
    lifecycle_verdict,
    not_run,
    readback_clean,
    readback_flags,
    settle,
)
from tests.live.chat.grounded_api_chain import (
    Refusal,
    Scrubber,
    assert_no_secret_shapes,
    free_port,
    parse_port_range,
    poll_until,
    refusal_with_log_tail,
    trim_recorded_log,
)
from tests.live.chat.real_graph_chain import MODEL_ID, model_fixture_env, user_prompt

REPO_ROOT = Path(__file__).resolve().parents[3]
MODEL_FIXTURE_MODULE = "tests.live.chat.real_graph_model_fixture"

#: Variables the run mints and hands to the ``okg`` children. NAMES only are
#: recorded; every value is scrubbed.
APP_DATABASE_ENV = "OKG_CHAT_APP_DATABASE_URL"
SECRET_ENV = "OKG_CHAT_WEBUI_SECRET_KEY"
ADMIN_EMAIL_ENV = "OKG_CHAT_ADMIN_EMAIL"
ADMIN_PASSWORD_ENV = "OKG_CHAT_ADMIN_PASSWORD"
ADMIN_TOKEN_ENV = "OKG_CHAT_ADMIN_TOKEN"
INSTANCE_URL_ENV = "OKG_CHAT_INSTANCE_URL"
PROVIDER_KEY_ENV = "OKG_LIFECYCLE_PROVIDER_KEY"
MCP_TOKEN_ENV = "OKG_LIFECYCLE_MCP_TOKEN"

#: Every step, in order. The verdict fails on a planned step with no record.
PLANNED_STEPS = [
    "configure",
    "instance_up",
    "setup",
    "plan",
    "sync",
    "readback_clean",
    "drift_apply",
    "drift_readback_flags",
    "drift_sync",
    "drift_readback_clean",
    "restart_down",
    "restart_up",
    "restart_readback_clean",
    "rotation_apply",
    ROTATION_READBACK_STEP,
    "rotation_sync",
    "rotation_readback_clean",
    "rotation_turn",
]

COMMAND_TIMEOUT = 900


class StepFailed(RuntimeError):
    """A step that later steps depend on did not pass; the rest are not run."""


def terminate_process_group(process: subprocess.Popen) -> str:
    """Stop a child started with ``start_new_session`` and its group."""
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
            process.wait(timeout=15)
            return f"stopped by {name}"
        except subprocess.TimeoutExpired:
            continue
    return "still running after SIGKILL"


def _git(*args: str) -> str:
    return subprocess.run(  # noqa: S603
        ["git", "-C", str(REPO_ROOT), *args],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


def _http(
    method: str,
    url: str,
    *,
    payload: Any = None,
    token: str = "",
    host_header: str = "",
    timeout: int = 30,
) -> tuple[int, Any]:
    """One HTTP round trip returning ``(status, decoded body or None)``."""
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/event-stream",
    }
    if token:
        headers["Authorization"] = "Bearer " + token
    if host_header:
        headers["Host"] = host_header
    request = urllib.request.Request(
        url,
        data=None if payload is None else json.dumps(payload).encode(),
        method=method,
        headers=headers,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
            raw, status = response.read(), response.status
    except urllib.error.HTTPError as exc:
        raw, status = exc.read(), exc.code
    try:
        return status, json.loads(raw) if raw else None
    except json.JSONDecodeError:
        return status, None


class Run:
    """One lifecycle run: its secrets, its children, and its records."""

    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.out: Path = args.out.resolve()
        self.scrub = Scrubber()
        self.scrub.hide(str(REPO_ROOT), "<repo>")
        self.scrub.hide(str(Path.home()), "<home>")
        self.steps: list[dict[str, Any]] = []
        self.counter = 0
        self.plan_dir: Path | None = None
        self.mcp_process: subprocess.Popen | None = None
        self.mcp_log = None
        self.model_process: subprocess.Popen | None = None
        self.model_log = None
        self.admin_token = ""
        self.instance_up = False

        app_dsn = os.environ.get(APP_DATABASE_ENV, "").strip()
        if not app_dsn:
            raise Refusal(
                f"set {APP_DATABASE_ENV} to the chat site's own app database "
                "before running this proof; it has no safe default"
            )
        self.scrub.hide(app_dsn, "<redacted:app-database-url>")
        # Any other connection string the okg children inherit -- above all
        # OKG_MCP_RO_DSN, which mcp-serve needs to connect as okg_mcp -- is
        # hidden the same way, by name, so a DSN echoed in a log never lands.
        for name, value in sorted(os.environ.items()):
            if value and (
                name.endswith("_DSN")
                or "DATABASE_URL" in name
                or ("://" in value and "@" in value)
            ):
                self.scrub.hide(value, f"<redacted:{name.lower()}>")
        port_range = parse_port_range(args.port_range) if args.port_range else None
        self.instance_port = free_port(port_range)
        self.mcp_port = free_port(port_range, taken=(self.instance_port,))
        self.model_port = free_port(
            port_range, taken=(self.instance_port, self.mcp_port)
        )
        self.base_url = f"http://127.0.0.1:{self.instance_port}"
        self.session_id = uuid.uuid4().hex
        self.admin_email = f"lifecycle-{self.session_id[:10]}@localhost.test"
        minted = {
            SECRET_ENV: secrets.token_urlsafe(32),
            ADMIN_PASSWORD_ENV: secrets.token_urlsafe(24),
            PROVIDER_KEY_ENV: secrets.token_urlsafe(32),
            MCP_TOKEN_ENV: secrets.token_urlsafe(36),
        }
        for name, value in minted.items():
            self.scrub.hide(value, f"<redacted:{name.lower()}>")
        self.scrub.hide(self.admin_email, "<redacted:admin-email>")
        self.env = dict(os.environ)
        for name in (ADMIN_TOKEN_ENV, INSTANCE_URL_ENV):
            self.env.pop(name, None)
        self.env.update(minted)
        self.env[ADMIN_EMAIL_ENV] = self.admin_email
        self.okg = running_okg_argv()

    # -- records -------------------------------------------------------------

    def record(
        self, name: str, *, passed: bool, detail: str, data: dict | None = None
    ) -> dict:
        step = settle(name, passed=passed, detail=self.scrub(detail), data=data)
        self.steps.append(step)
        print(f"[{step['outcome']:>12}] {name}: {step['detail'][:300]}", flush=True)
        return step

    def require(
        self, name: str, *, passed: bool, detail: str, data: dict | None = None
    ) -> None:
        """Record a step the rest of the run depends on; stop when it fails."""
        step = self.record(name, passed=passed, detail=detail, data=data)
        if step["outcome"] not in ("pass", "xfail"):
            raise StepFailed(name)

    def okg_command(self, label: str, *argv: str) -> tuple[int, dict[str, Any], str]:
        """Run one ``okg`` command, keep its scrubbed output, return its result."""
        self.counter += 1
        stem = f"{self.counter:02d}-{label}"
        command = [*self.okg, *argv]
        started = time.monotonic()
        try:
            completed = subprocess.run(  # noqa: S603
                command,
                env=self.env,
                cwd=str(self.out),
                capture_output=True,
                text=True,
                timeout=COMMAND_TIMEOUT,
                stdin=subprocess.DEVNULL,
            )
            code, stdout, stderr = (
                completed.returncode,
                completed.stdout,
                completed.stderr,
            )
        except subprocess.TimeoutExpired as exc:
            code = -1
            stdout = exc.stdout if isinstance(exc.stdout, str) else ""
            stderr = f"timed out after {COMMAND_TIMEOUT}s"
        seconds = round(time.monotonic() - started, 1)
        steps_dir = self.out / "commands"
        steps_dir.mkdir(exist_ok=True)
        self.scrub.write(steps_dir / f"{stem}.json", stdout)
        self.scrub.write(steps_dir / f"{stem}.err", trim_recorded_log(stderr))
        self.scrub.write(
            steps_dir / f"{stem}.argv.json",
            json.dumps(
                {"argv": ["okg", *argv], "exit": code, "seconds": seconds}, indent=2
            )
            + "\n",
        )
        return code, command_json(stdout), f"{stem} exit {code} in {seconds}s"

    # -- children ------------------------------------------------------------

    def start_model(self) -> None:
        model_out = self.out / "model"
        model_out.mkdir(exist_ok=True)
        self.model_log = (self.out / "model.log").open("w", encoding="utf-8")
        self.model_process = subprocess.Popen(  # noqa: S603
            [
                sys.executable,
                "-m",
                MODEL_FIXTURE_MODULE,
                "--model-port",
                str(self.model_port),
                "--out",
                str(model_out),
                "--session-id",
                self.session_id,
            ],
            cwd=REPO_ROOT,
            env=model_fixture_env(os.environ),
            stdin=subprocess.DEVNULL,
            stdout=self.model_log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        self.wait_listening(f"http://127.0.0.1:{self.model_port}/v1/models")

    def start_mcp(self) -> None:
        """Start the graph server with the CURRENT bearer, as setup would."""
        argv = mcp_serve_argv(
            self.args.deployment,
            port=self.mcp_port,
            host=self.args.mcp_host,
            auth_token_env=MCP_TOKEN_ENV,
            allowed_hosts=("127.0.0.1:*", f"{self.args.host_gateway_alias}:*"),
        )
        if self.mcp_log is None:
            self.mcp_log = (self.out / "mcp-serve.log").open("w", encoding="utf-8")
        self.mcp_process = subprocess.Popen(  # noqa: S603
            argv,
            cwd=str(self.out),
            env=self.env,
            stdin=subprocess.DEVNULL,
            stdout=self.mcp_log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        try:
            self.wait_listening(
                f"http://127.0.0.1:{self.mcp_port}/mcp",
                host_header=f"127.0.0.1:{self.mcp_port}",
            )
        except Refusal as refused:
            self.mcp_log.flush()
            raise refusal_with_log_tail(
                str(refused),
                log_path=self.out / "mcp-serve.log",
                scrub=self.scrub,
                process_name="mcp-serve",
                exit_code=self.mcp_process.poll(),
            ) from refused

    def wait_listening(self, url: str, *, host_header: str = "") -> None:
        def probe() -> str:
            try:
                _http("GET", url, host_header=host_header, timeout=5)
            except Exception as exc:  # noqa: BLE001 - reported on timeout
                return f"{type(exc).__name__}: {exc}"
            return ""

        poll_until(
            probe,
            lambda failure: failure == "",
            timeout=self.args.timeout,
            on_timeout=lambda failure: Refusal(f"{url} never answered: {failure}"),
            clock=time.monotonic,
            sleep=time.sleep,
            interval=2.0,
        )

    def mcp_status(self, token: str) -> int:
        """The HTTP status the graph server answers an MCP initialize with."""
        status, _ = _http(
            "POST",
            f"http://127.0.0.1:{self.mcp_port}/mcp",
            payload={
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": {
                    "protocolVersion": "2025-06-18",
                    "capabilities": {},
                    "clientInfo": {"name": "okg-lifecycle-proof", "version": "1"},
                },
            },
            token=token,
            host_header=f"127.0.0.1:{self.mcp_port}",
        )
        return status

    def sign_in(self) -> None:
        status, body = _http(
            "POST",
            f"{self.base_url}/api/v1/auths/signin",
            payload={
                "email": self.admin_email,
                "password": self.env[ADMIN_PASSWORD_ENV],
            },
        )
        token = (body or {}).get("token") if isinstance(body, dict) else None
        if status != 200 or not token:
            raise Refusal(f"admin sign-in answered HTTP {status}")
        self.admin_token = token
        self.scrub.hide(token, "<redacted:admin-session-token>")
        self.env[ADMIN_TOKEN_ENV] = token
        self.env[INSTANCE_URL_ENV] = self.base_url

    def chat_count(self) -> int:
        status, body = _http(
            "GET", f"{self.base_url}/api/v1/chats/", token=self.admin_token
        )
        if status != 200 or not isinstance(body, list):
            raise Refusal(f"listing the admin's chats answered HTTP {status}")
        return len(body)

    # -- the lifecycle -------------------------------------------------------

    def network_args(self) -> list[str]:
        return [
            "--container-runtime",
            self.args.container_runtime,
            "--container-network",
            self.args.container_network,
            "--host-gateway-alias",
            self.args.host_gateway_alias,
        ]

    def plan_sync_readback(self, prefix: str, *, want_plan: bool) -> None:
        """``plan`` (once), then ``sync`` and a clean ``readback``."""
        deployment = self.args.deployment
        if want_plan:
            self.plan_dir = self.out / "plan"
            code, payload, detail = self.okg_command(
                "plan",
                "chat",
                "plan",
                "--json",
                "--deployment",
                deployment,
                "--out",
                str(self.plan_dir),
            )
            plan_file = self.plan_dir / "chat-plan.json"
            self.require(
                "plan",
                passed=code == 0 and plan_file.is_file(),
                detail=f"{detail}; plan file {'written' if plan_file.is_file() else 'missing'}",
                data={"plan_digest": payload.get("plan_digest")},
            )
        assert self.plan_dir is not None
        plan_file = str(self.plan_dir / "chat-plan.json")
        code, payload, detail = self.okg_command(
            f"{prefix}sync",
            "chat",
            "sync",
            "--json",
            "--deployment",
            deployment,
            "--plan",
            plan_file,
        )
        changes = payload.get("changes")
        self.require(
            f"{prefix}sync",
            passed=code == 0 and payload.get("ok") is True,
            detail=f"{detail}; ok={payload.get('ok')!r}",
            data={
                "changes": len(changes) if isinstance(changes, list) else changes,
                "written_surfaces": payload.get("written_surfaces"),
            },
        )
        self.readback(f"{prefix}readback_clean", clean=True)

    def readback(self, name: str, *, clean: bool, key: str | None = None) -> dict:
        assert self.plan_dir is not None
        out_dir = self.out / f"readback-{name}"
        code, payload, detail = self.okg_command(
            name,
            "chat",
            "readback",
            "--json",
            "--deployment",
            self.args.deployment,
            "--plan",
            str(self.plan_dir / "chat-plan.json"),
            "--out",
            str(out_dir),
        )
        judged, why = (
            readback_clean(code, payload)
            if clean
            else readback_flags(code, payload, key=key)
        )
        data = {
            "exit": code,
            "ok": payload.get("ok"),
            "code": payload.get("code"),
            "deviations": payload.get("deviations"),
            "verify_probe": payload.get("verify_probe"),
            "openwebui_version": payload.get("openwebui_version"),
        }
        step = self.record(name, passed=judged, detail=f"{detail}; {why}", data=data)
        if step["outcome"] not in ("pass", "xfail"):
            raise StepFailed(name)
        return step

    def run(self) -> None:
        a = self.args
        deployment = a.deployment
        self.start_model()

        # 1. configure, then declare the graph bearer.
        configure = [
            "chat",
            "setup",
            "--json",
            "--deployment",
            deployment,
            "--provider",
            "openai-compatible",
            "--provider-base-url",
            f"http://{a.host_gateway_alias}:{self.model_port}/v1",
            "--provider-key-env",
            PROVIDER_KEY_ENV,
            "--model",
            MODEL_ID,
            "--instance-port",
            str(self.instance_port),
            "--mcp-port",
            str(self.mcp_port),
            "--configure-only",
        ]
        for subtype in a.search_subtype:
            configure += ["--search-subtype", subtype]
        code, payload, detail = self.okg_command("configure", *configure)
        manifest = a.deployments_dir / deployment / "deployment.yaml"
        added = ""
        if code == 0:
            try:
                manifest.write_text(
                    add_mcp_auth_token_env(manifest.read_text("utf-8"), MCP_TOKEN_ENV),
                    encoding="utf-8",
                )
                added = f"; chat.mcp.auth_token_env={MCP_TOKEN_ENV} declared"
            except ValueError as exc:
                code, added = 1, f"; {exc}"
        self.require("configure", passed=code == 0, detail=detail + added)
        self.start_mcp()

        # 2-3. up, then setup (its conversation must use the graph).
        code, payload, detail = self.okg_command(
            "instance_up",
            "chat-instance",
            "up",
            "--json",
            "--deployment",
            deployment,
            "--image",
            a.image,
            *self.network_args(),
        )
        self.instance_up = code == 0
        self.require(
            "instance_up",
            passed=code == 0,
            detail=detail,
            data={
                k: payload.get(k)
                for k in ("version", "image", "network_mode", "app_database")
            },
        )
        self.setup("setup")
        self.sign_in()

        # 4. plan, sync, readback.
        self.plan_sync_readback("", want_plan=True)

        # 5. drift through the vendor admin API, then repair.
        status, config = _http(
            "GET", f"{self.base_url}/api/v1/auths/admin/config", token=self.admin_token
        )
        before = (
            (config or {}).get(DRIFT_ADMIN_CONFIG_KEY)
            if isinstance(config, dict)
            else None
        )
        applied = None
        if status == 200 and isinstance(config, dict):
            config[DRIFT_ADMIN_CONFIG_KEY] = True
            _http(
                "POST",
                f"{self.base_url}/api/v1/auths/admin/config",
                payload=config,
                token=self.admin_token,
            )
            _, after = _http(
                "GET",
                f"{self.base_url}/api/v1/auths/admin/config",
                token=self.admin_token,
            )
            applied = (
                (after or {}).get(DRIFT_ADMIN_CONFIG_KEY)
                if isinstance(after, dict)
                else None
            )
        self.require(
            "drift_apply",
            passed=before is False and applied is True,
            detail=f"{DRIFT_ADMIN_CONFIG_KEY} read {before!r} before and {applied!r} after the admin API write",
        )
        self.readback("drift_readback_flags", clean=False, key=DRIFT_READBACK_KEY)
        self.plan_sync_readback("drift_", want_plan=False)

        # 6. restart: down, up, clean, chats kept.
        chats_before = self.chat_count()
        code, _, detail = self.okg_command(
            "restart_down",
            "chat-instance",
            "down",
            "--json",
            "--deployment",
            deployment,
            "--container-runtime",
            a.container_runtime,
        )
        self.instance_up = code != 0
        self.require("restart_down", passed=code == 0, detail=detail)
        code, _, detail = self.okg_command(
            "restart_up",
            "chat-instance",
            "up",
            "--json",
            "--deployment",
            deployment,
            "--image",
            a.image,
            *self.network_args(),
        )
        self.instance_up = code == 0
        self.require("restart_up", passed=code == 0, detail=detail)
        step = self.readback("restart_readback_clean", clean=True)
        chats_after = self.chat_count()
        if chats_after != chats_before or chats_before < 1:
            step["outcome"], step["passed"] = "fail", False
            step["detail"] += (
                f"; chats {chats_before} before and {chats_after} after the restart"
            )
            raise StepFailed("restart_readback_clean")
        step["detail"] += f"; chats kept ({chats_before} before and after)"
        step["data"]["chats"] = {"before": chats_before, "after": chats_after}

        # 7. rotate the graph bearer: the site still stores the old one.
        old = self.env[MCP_TOKEN_ENV]
        new = secrets.token_urlsafe(36)
        self.scrub.hide(new, f"<redacted:{MCP_TOKEN_ENV.lower()}-rotated>")
        teardown_old = (
            terminate_process_group(self.mcp_process) if self.mcp_process else "none"
        )
        self.env[MCP_TOKEN_ENV] = new
        self.start_mcp()
        old_status, new_status = self.mcp_status(old), self.mcp_status(new)
        self.require(
            "rotation_apply",
            passed=old_status == 401 and new_status == 200,
            detail=(
                f"mcp-serve restarted with a new bearer (old server {teardown_old}); "
                f"old bearer answered HTTP {old_status}, new bearer HTTP {new_status}"
            ),
            data={"old_bearer_status": old_status, "new_bearer_status": new_status},
        )
        try:
            self.readback(ROTATION_READBACK_STEP, clean=False)
        except StepFailed:
            # Only an xpass_strict or a plain fail reaches here; the xfail
            # outcome does not raise. Keep going either way so the repair
            # half of the arm is still measured.
            pass
        self.plan_sync_readback("rotation_", want_plan=False)
        self.setup("rotation_turn")

    def setup(self, name: str) -> None:
        code, payload, detail = self.okg_command(
            name,
            "chat",
            "setup",
            "--json",
            "--deployment",
            self.args.deployment,
            "--image",
            self.args.image,
            *self.network_args(),
            "--question",
            user_prompt(self.args.node_id),
            "--no-start-mcp-serve",
        )
        conversation = payload.get("conversation")
        conversation = conversation if isinstance(conversation, dict) else {}
        error = payload.get("error") if isinstance(payload.get("error"), dict) else {}
        self.require(
            name,
            passed=code == 0 and conversation.get("ready") is True,
            detail=(
                f"{detail}; conversation ready={conversation.get('ready')!r}, "
                f"tools invoked {conversation.get('tools_invoked')!r}"
                + (
                    f"; failed step {payload.get('failed_step')!r}: {error.get('message', '')[:300]}"
                    if error
                    else ""
                )
            ),
            data={
                "conversation": conversation,
                "failed_step": payload.get("failed_step"),
            },
        )

    def teardown(self) -> dict[str, str]:
        report: dict[str, str] = {}
        if self.instance_up and not self.args.keep:
            code, _, detail = self.okg_command(
                "teardown_down",
                "chat-instance",
                "down",
                "--json",
                "--deployment",
                self.args.deployment,
                "--container-runtime",
                self.args.container_runtime,
            )
            report["chat-instance"] = detail
        for label, process in (
            ("mcp-serve", self.mcp_process),
            ("model", self.model_process),
        ):
            if process is not None:
                report[label] = terminate_process_group(process)
        for handle in (self.mcp_log, self.model_log):
            if handle is not None:
                handle.close()
        for name in ("mcp-serve.log", "model.log"):
            path = self.out / name
            if path.exists():
                self.scrub.write(path, trim_recorded_log(path.read_text("utf-8")))
        self.scrub.rewrite(self.out / "model" / "model.jsonl")
        return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--deployment", required=True)
    parser.add_argument(
        "--node-id", required=True, help="a node the published graph holds"
    )
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument(
        "--container-network",
        required=True,
        choices=("gateway", "pasta", "host"),
        help="passed to chat-instance up and chat setup; never auto, so the "
        "run names how its container reaches this host",
    )
    parser.add_argument("--container-runtime", default="docker")
    parser.add_argument("--host-gateway-alias", default="host.docker.internal")
    parser.add_argument(
        "--mcp-host",
        default="127.0.0.1",
        help="interface mcp-serve listens on; a docker gateway network on Linux "
        "needs the bridge address, loopback serves pasta and host",
    )
    parser.add_argument("--image", default=PINNED_IMAGE_REF)
    parser.add_argument("--port-range", default=None, help="LOW-HIGH loopback ports")
    parser.add_argument("--search-subtype", action="append", default=[])
    parser.add_argument("--timeout", type=int, default=240)
    parser.add_argument("--keep", action="store_true", help="leave the site up")
    parser.add_argument(
        "--deployments-dir",
        type=Path,
        default=None,
        help="root holding <deployment>/deployment.yaml (default: "
        "OKG_DEPLOYMENTS_DIR, else $XDG_DATA_HOME/okg/deployments)",
    )
    args = parser.parse_args()
    if args.deployments_dir is None:
        root = os.environ.get("OKG_DEPLOYMENTS_DIR") or str(
            Path(os.environ.get("XDG_DATA_HOME") or Path.home() / ".local/share")
            / "okg"
            / "deployments"
        )
        args.deployments_dir = Path(root)
    manifest = args.deployments_dir / args.deployment / "deployment.yaml"
    if not manifest.is_file():
        raise Refusal(f"no deployment manifest at {manifest}")
    if any(
        line.rstrip() == "chat:" for line in manifest.read_text("utf-8").splitlines()
    ):
        raise Refusal(
            f"{manifest} already declares a chat: block; this proof writes its "
            "own and needs a disposable deployment without one"
        )

    out = args.out.resolve()
    if out.exists():
        shutil.rmtree(out)
    out.mkdir(parents=True)
    args.out = out
    run = Run(args)
    identity = {
        "schema": "okg.chat_lifecycle_proof.v1",
        "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "deployment": args.deployment,
        "node_id": args.node_id,
        "image": args.image,
        "tested_image_version": TESTED_IMAGES.get(args.image),
        "image_is_product_pin": args.image == PINNED_IMAGE_REF,
        "container_runtime": args.container_runtime,
        "container_network": args.container_network,
        "ports": {
            "instance": run.instance_port,
            "mcp": run.mcp_port,
            "model": run.model_port,
        },
        "okg_head": _git("rev-parse", "HEAD"),
        "okg_worktree_dirty": bool(_git("status", "--porcelain")),
        "credentials_minted": sorted(
            [
                SECRET_ENV,
                ADMIN_PASSWORD_ENV,
                PROVIDER_KEY_ENV,
                MCP_TOKEN_ENV,
                ADMIN_TOKEN_ENV,
            ]
        ),
        "python": platform.python_version(),
        "host_platform": platform.platform(),
    }
    failure = ""
    try:
        run.run()
    except StepFailed as exc:
        failure = f"stopped after step {exc} did not pass"
    except Refusal as exc:
        failure = f"refused: {exc}"
    finally:
        teardown = run.teardown()
    seen = {step["step"] for step in run.steps}
    steps = run.steps + [
        not_run(name, because=failure or "the run stopped")
        for name in PLANNED_STEPS
        if name not in seen
    ]
    order = {name: index for index, name in enumerate(PLANNED_STEPS)}
    steps.sort(key=lambda step: order.get(step["step"], len(order)))
    verdict = {
        **identity,
        **lifecycle_verdict(steps, planned=PLANNED_STEPS),
        "stopped": run.scrub(failure) or None,
        "teardown": teardown,
        "steps": steps,
    }
    run.scrub.write(
        out / "verdict.json", json.dumps(verdict, indent=2, sort_keys=True) + "\n"
    )
    checked = assert_no_secret_shapes(out)
    print(f"redaction checked {len(checked)} artifact(s)")
    print(
        json.dumps(
            {
                k: verdict[k]
                for k in (
                    "ok",
                    "outcomes",
                    "failed_steps",
                    "expected_failures",
                    "stopped",
                )
            },
            sort_keys=True,
        )
    )
    return 0 if verdict["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
