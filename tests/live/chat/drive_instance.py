"""Bring up a disposable Open WebUI, drive turns, keep every byte.

This is the recipe half of the post-compaction proof. It stands up Open
WebUI at the PINNED version against a recording model endpoint, installs the
audit filter, drives a conversation past the compaction threshold, and
leaves two artifacts behind for
``test_post_compaction_audit_equality.py`` to compare:

* the audit artifact the filter wrote, from inside the container, and
* the capture file the recording endpoint wrote, from outside it.

Run it twice — ``--arm on`` and ``--arm off`` — because the proof is
two-armed. The off arm is not a formality: compaction is not the only
post-inlet mutation Open WebUI can perform, and the off configuration is
where every unprovable preset lands, so it gets its own equality proof
rather than an assumed-safe default.

Configuration goes through the ADMIN API, never through environment
variables. Open WebUI's ``ENABLE_*`` flags are PersistentConfig: seeded into
the config table on first boot and served from the database forever after,
so driving them by env is correct exactly once and silently wrong on every
instance that already exists. ``chat.context_compaction.*`` is an API knob
(see the program's ``ui-knob-census.json``), which is what makes the toggle
reliable here.

No credentials are entered anywhere. The admin account is a throwaway for a
container that is destroyed at teardown, and the model endpoint is keyless.
"""

from __future__ import annotations

import argparse
import json
import secrets
import shutil
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from recording_model_endpoint import (  # noqa: E402
    CHAT_MODEL,
    COMPACTION_MODEL,
    TASK_MODEL,
    serve,
)

from grounded_api_chain import HOST_GATEWAY_ALIAS, HOST_GATEWAY_ARGS  # noqa: E402

from okg.substrate.chat.instance import PINNED_IMAGE_REF, TESTED_IMAGES  # noqa: E402

#: The pinned image and its version, read from the product rather than
#: copied: the proof runs the image ``okg chat-instance up`` would run, and a
#: change of the product pin is a change of this proof's key, so every
#: previous verdict stops applying by construction.
OPENWEBUI_IMAGE = PINNED_IMAGE_REF
OPENWEBUI_VERSION = TESTED_IMAGES[PINNED_IMAGE_REF]

CONTAINER = "okg-audit-proof"
#: Defaults; ``--webui-port`` and ``--recorder-port`` override them on a shared
#: host that hands each run its own port block.
WEBUI_PORT = 3111
RECORDER_PORT = 8199

DEPLOYMENT = "cooper"
PRESET = CHAT_MODEL

#: Low enough that a handful of short turns crosses it. The threshold is a
#: config knob precisely so the proof does not need a 80k-token
#: conversation to observe the behavior it is about.
COMPACTION_TOKEN_THRESHOLD = 120


@dataclass
class Instance:
    base_url: str
    token: str

    def call(
        self,
        method: str,
        path: str,
        payload: Any = None,
        *,
        timeout: int = 120,
    ) -> Any:
        data = None if payload is None else json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=data,
            method=method,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.token}",
            },
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
            body = response.read().decode("utf-8")
        if not body:
            return None
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return body


def _docker(*args: str, check: bool = True) -> subprocess.CompletedProcess:
    completed = subprocess.run(  # noqa: S603
        ["docker", *args], capture_output=True, text=True, check=False
    )
    if check and completed.returncode != 0:
        # The runtime's own words, not only its exit status: a bare
        # CalledProcessError hides why the container did not start.
        raise RuntimeError(
            f"docker {args[0]} exited {completed.returncode}: "
            f"{completed.stderr.strip()[-2000:]}"
        )
    return completed


def _wait_for(url: str, *, timeout: int = 600) -> None:
    deadline = time.time() + timeout
    last: Exception | None = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=10) as response:  # noqa: S310
                if response.status < 500:
                    return
        except Exception as exc:  # noqa: BLE001
            last = exc
        time.sleep(3)
    raise TimeoutError(f"{url} never answered: {last}")


def start_container(audit_dir: Path, verdict_dir: Path) -> None:
    _docker("rm", "-f", CONTAINER, check=False)
    audit_dir.mkdir(parents=True, exist_ok=True)
    audit_dir.chmod(0o777)
    verdict_dir.mkdir(parents=True, exist_ok=True)
    verdict_dir.chmod(0o777)
    _docker(*container_run_args(audit_dir, verdict_dir))
    _wait_for(f"http://127.0.0.1:{WEBUI_PORT}/health")


def container_run_args(audit_dir: Path, verdict_dir: Path) -> list[str]:
    """The ``docker`` arguments that start the site, in one place.

    ``--add-host`` defines the name configure() points the site at: podman
    defines it by itself, plain Docker on Linux does not (see
    grounded_api_chain.docker_run_command).
    """
    return [
        "run",
        "-d",
        "--name",
        CONTAINER,
        "-p",
        f"127.0.0.1:{WEBUI_PORT}:8080",
        "-v",
        f"{audit_dir}:/okg-audit",
        # Read-only: the verdict is written by the operator-run proof on the
        # host and only READ inside the container, which is the same
        # separation the substrate filter has in code.
        "-v",
        f"{verdict_dir}:/okg-verdict:ro",
        # A directory the container cannot write, for the unwritable-audit
        # refusal measurement.
        "-v",
        f"{verdict_dir}:/okg-readonly:ro",
        "-e",
        "WEBUI_AUTH=True",
        "-e",
        "WEBUI_NAME=OKG Audit Proof",
        "-e",
        "ENABLE_PERSISTENT_CONFIG=True",
        *HOST_GATEWAY_ARGS,
        OPENWEBUI_IMAGE,
    ]


def bootstrap_admin() -> Instance:
    """First account becomes admin. Disposable, for a disposable container."""
    base = f"http://127.0.0.1:{WEBUI_PORT}"
    payload = {
        "name": "Audit Proof",
        "email": "audit-proof@localhost.test",
        # Generated here, used once, destroyed with the container. Never a
        # credential of the operator's.
        "password": secrets.token_urlsafe(24),
    }
    request = urllib.request.Request(
        f"{base}/api/v1/auths/signup",
        data=json.dumps(payload).encode("utf-8"),
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=120) as response:  # noqa: S310
        body = json.loads(response.read().decode("utf-8"))
    return Instance(base_url=base, token=body["token"])


def configure(instance: Instance, *, compaction_enabled: bool) -> None:
    """Point Open WebUI at the recorder and set the compaction knobs."""
    recorder = f"http://{HOST_GATEWAY_ALIAS}:{RECORDER_PORT}/v1"
    instance.call(
        "POST",
        "/openai/config/update",
        {
            "ENABLE_OPENAI_API": True,
            "OPENAI_API_BASE_URLS": [recorder],
            # Keyless: the recording endpoint never reads an Authorization
            # header and has no credential to supply.
            "OPENAI_API_KEYS": [""],
            "OPENAI_API_CONFIGS": {"0": {"enable": True}},
        },
    )
    instance.call(
        "POST",
        "/ollama/config/update",
        {"ENABLE_OLLAMA_API": False, "OLLAMA_BASE_URLS": [], "OLLAMA_API_CONFIGS": {}},
    )
    # FLAT DOTTED KEYS. Open WebUI's config table is one row per dotted key
    # (`models/config.py`), and `/configs/import` upserts whatever it is
    # given: a nested dict would create a row named `chat` that the
    # application never reads, round-trip cleanly through
    # `/configs/export`, and look exactly like a knob that worked. The
    # program's knob census hit that trap twice; this is the shape that
    # avoids it.
    instance.call(
        "POST",
        "/api/v1/configs/import",
        {
            "config": {
                "chat.context_compaction.enable": compaction_enabled,
                "chat.context_compaction.token_threshold": COMPACTION_TOKEN_THRESHOLD,
                "chat.context_compaction.token_cap": COMPACTION_TOKEN_THRESHOLD,
                "chat.context_compaction.retention_percentage": 10,
                "chat.context_compaction.model": COMPACTION_MODEL,
                "task.model.default": TASK_MODEL,
                "task.model.external": TASK_MODEL,
                # Governed toggles: everything that could inject context
                # AFTER the inlet is off, so the equality measures the
                # ordering rather than a feature we chose to leave on.
                "memories.system_context.enable": False,
                "rag.web.search.enable": False,
                "image_generation.enable": False,
                "code_interpreter.enable": False,
            }
        },
    )
    # Readback, not the POST response: the census recorded a v0.11.0 write
    # path that returns 200 having thrown. A knob we cannot read back is a
    # knob we have not set.
    exported = instance.call("GET", "/api/v1/configs/export")
    observed = exported.get("chat.context_compaction.enable")
    if observed != compaction_enabled:
        raise RuntimeError(
            "compaction toggle did not read back: wanted "
            f"{compaction_enabled}, observed {observed!r}"
        )


def install_filter(
    instance: Instance,
    *,
    compaction_enabled: bool,
    artifact_path: str = "/okg-audit/records-001.jsonl",
) -> None:
    source = (Path(__file__).parent / "okg_audit_filter_function.py").read_text("utf-8")
    instance.call(
        "POST",
        "/api/v1/functions/create",
        {
            "id": "okg_context_audit",
            "name": "OKG Context Audit",
            "content": source,
            "meta": {
                "description": "Records the exact bytes each turn sends to the model.",
                "manifest": {},
            },
        },
    )
    instance.call(
        "POST",
        "/api/v1/functions/id/okg_context_audit/valves/update",
        {
            "deployment": DEPLOYMENT,
            "preset": PRESET,
            "openwebui_version": OPENWEBUI_VERSION,
            "artifact_path": artifact_path,
            "compaction_enabled": compaction_enabled,
            "verdict_path": "/okg-verdict/verdicts.jsonl",
        },
    )
    instance.call("POST", "/api/v1/functions/id/okg_context_audit/toggle", None)
    # Global filters run on every model; assert that is what we got rather
    # than assuming the toggle meant it.
    listed = instance.call("GET", "/api/v1/functions/")
    entry = next((f for f in listed if f["id"] == "okg_context_audit"), None)
    if entry is None or not entry.get("is_active"):
        raise RuntimeError(f"the audit filter is not active: {entry}")
    if not entry.get("is_global"):
        instance.call("POST", "/api/v1/functions/id/okg_context_audit/toggle/global")
        listed = instance.call("GET", "/api/v1/functions/")
        entry = next((f for f in listed if f["id"] == "okg_context_audit"), None)
        if entry is None or not entry.get("is_global"):
            raise RuntimeError(f"the audit filter is not global: {entry}")


def drive_conversation(instance: Instance, *, turns: int, words: int) -> str:
    """Send ``turns`` user messages in one saved chat. Returns the chat id."""
    created = instance.call(
        "POST", "/api/v1/chats/new", {"chat": {"title": "audit proof"}}
    )
    chat_id = created["id"]

    messages: list[dict[str, Any]] = []
    parent_id: str | None = None
    for index in range(turns):
        user_id = str(uuid.uuid4())
        assistant_id = str(uuid.uuid4())
        content = f"Turn {index}: " + " ".join(
            f"region-{index}-{word}" for word in range(words)
        )
        messages.append({"id": user_id, "role": "user", "content": content})
        instance.call(
            "POST",
            "/api/chat/completions",
            {
                "model": CHAT_MODEL,
                "chat_id": chat_id,
                "id": assistant_id,
                "session_id": "audit-proof-session",
                "stream": False,
                "messages": list(messages),
                "user_message": {
                    "id": user_id,
                    "role": "user",
                    "content": content,
                    "parentId": parent_id,
                    "childrenIds": [assistant_id],
                    "models": [CHAT_MODEL],
                    "timestamp": int(time.time()),
                },
                # Title and tag generation on the FIRST turn only: the proof
                # needs at least one auxiliary model call in the capture
                # file, or "the labels distinguish the user turn" would be a
                # claim tested against a file containing only user turns.
                "background_tasks": {
                    "title_generation": index == 0,
                    "tags_generation": index == 0,
                },
            },
        )
        instance.call(
            "POST",
            "/api/chat/completed",
            {
                "model": CHAT_MODEL,
                "chat_id": chat_id,
                "id": assistant_id,
                "session_id": "audit-proof-session",
                "messages": [
                    *messages,
                    {"id": assistant_id, "role": "assistant", "content": "Recorded."},
                ],
            },
        )
        messages.append({"id": assistant_id, "role": "assistant", "content": "Recorded."})
        parent_id = assistant_id
        time.sleep(0.5)
    return chat_id


def teardown(server: Any, thread: threading.Thread | None) -> None:
    _docker("rm", "-f", CONTAINER, check=False)
    if server is not None:
        server.shutdown()
        server.server_close()
    if thread is not None:
        thread.join(timeout=10)


def measure_refusal(instance: Instance, *, chat_id: str) -> dict[str, Any]:
    """Send one turn that MUST be refused, and record what came back.

    Whether a raised inlet exception surfaces to the user or is swallowed is
    a VENDOR behavior. It is measured here rather than assumed, because
    "the turn fails" is undefined at the surface where it matters until
    somebody looks.
    """
    user_id = str(uuid.uuid4())
    assistant_id = str(uuid.uuid4())
    content = "This turn must be refused."
    payload = {
        "model": CHAT_MODEL,
        "chat_id": chat_id,
        "id": assistant_id,
        "session_id": "refusal-measurement",
        "stream": False,
        "messages": [{"id": user_id, "role": "user", "content": content}],
        "user_message": {
            "id": user_id,
            "role": "user",
            "content": content,
            "parentId": None,
            "childrenIds": [assistant_id],
            "models": [CHAT_MODEL],
            "timestamp": int(time.time()),
        },
        "background_tasks": {"title_generation": False, "tags_generation": False},
    }
    result: dict[str, Any]
    try:
        body = instance.call("POST", "/api/chat/completions", payload)
        result = {"http_status": 200, "http_body": json.dumps(body)[:2000]}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", "replace")
        result = {"http_status": exc.code, "http_body": raw[:2000]}

    # The HTTP response is NOT the surface that matters at v0.11.0: the
    # completion runs as a background task, so a refusal raised in the inlet
    # cannot appear in it. What the user sees is what lands on the assistant
    # message, so read THAT back.
    stored: dict[str, Any] = {}
    for _ in range(20):
        time.sleep(1)
        chat = instance.call("GET", f"/api/v1/chats/{chat_id}")
        messages = (chat.get("chat") or {}).get("history", {}).get("messages", {})
        assistant = messages.get(assistant_id) or {}
        if assistant.get("error") or assistant.get("content"):
            stored = assistant
            break
    result.update(
        {
            "assistant_message_error": stored.get("error"),
            "assistant_message_content": stored.get("content"),
            "user_visible_error_text": (stored.get("error") or {}).get("content")
            if isinstance(stored.get("error"), dict)
            else None,
            "answered": bool(stored.get("content")),
        }
    )
    return result


def main() -> int:
    global WEBUI_PORT, RECORDER_PORT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--arm", choices=("on", "off"), required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--turns", type=int, default=8)
    parser.add_argument("--words", type=int, default=25)
    parser.add_argument("--keep", action="store_true", help="skip teardown")
    parser.add_argument("--webui-port", type=int, default=WEBUI_PORT)
    parser.add_argument("--recorder-port", type=int, default=RECORDER_PORT)
    parser.add_argument(
        "--refusal",
        choices=("none", "unwritable", "unverified"),
        default="none",
        help="Measure a refusal state instead of driving a full conversation.",
    )
    args = parser.parse_args()
    WEBUI_PORT, RECORDER_PORT = args.webui_port, args.recorder_port

    compaction_enabled = args.arm == "on"
    # Absolute: a relative path in a docker -v mount is read as a named
    # volume, which would give the container an empty directory and leave
    # the audit artifact invisible from outside.
    out = args.out.resolve()
    if out.exists():
        shutil.rmtree(out)
    audit_dir = out / "audit"
    verdict_dir = out / "verdict"
    captures = out / "captures.jsonl"
    audit_dir.mkdir(parents=True)
    verdict_dir.mkdir(parents=True)

    # THE PROOF MARKER. The verdict cannot exist before the proof that
    # writes it, so the equality run would otherwise refuse itself. The
    # suspension lives HERE, on the host, in a directory mounted read-only
    # into the container — never in a valve an Open WebUI administrator
    # could flip on a production instance. Refusal measurements do not get
    # it: their whole point is that the predicate is live.
    if args.refusal == "none" and compaction_enabled:
        (verdict_dir / "PROOF_RUN_IN_PROGRESS").write_text(
            "written by drive_instance.py; suspends the audit filter's "
            "refusal for this measurement only\n",
            encoding="utf-8",
        )

    server = serve(RECORDER_PORT, captures)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    try:
        start_container(audit_dir, verdict_dir)
        instance = bootstrap_admin()
        configure(instance, compaction_enabled=compaction_enabled)
        install_filter(
            instance,
            compaction_enabled=(
                True if args.refusal == "unverified" else compaction_enabled
            ),
            # The unwritable arm points the artifact at a read-only mount.
            artifact_path=(
                "/okg-readonly/records-001.jsonl"
                if args.refusal == "unwritable"
                else "/okg-audit/records-001.jsonl"
            ),
        )
        # Model list has to reach Open WebUI before a completion can name one.
        instance.call("GET", "/api/models")
        refusal: dict[str, Any] | None = None
        if args.refusal == "none":
            chat_id = drive_conversation(instance, turns=args.turns, words=args.words)
        else:
            created = instance.call(
                "POST", "/api/v1/chats/new", {"chat": {"title": "refusal measurement"}}
            )
            chat_id = created["id"]
            refusal = measure_refusal(instance, chat_id=chat_id)
            (out / "refusal.json").write_text(
                json.dumps({"mode": args.refusal, **refusal}, indent=2),
                encoding="utf-8",
            )
            print(json.dumps({"refusal": args.refusal, **refusal}, indent=2))
        if args.keep:
            # The instance stays up for the browser proof. Only the session
            # JWT is written, never the generated password, and only into
            # the run directory of a container that is destroyed after.
            (out / "session.json").write_text(
                json.dumps({"base_url": instance.base_url, "token": instance.token}),
                encoding="utf-8",
            )
        logs = _docker("logs", CONTAINER, check=False)
        (out / "container.log").write_text(
            logs.stdout + logs.stderr, encoding="utf-8"
        )
        (out / "run.json").write_text(
            json.dumps(
                {
                    "arm": args.arm,
                    "compaction_enabled": compaction_enabled,
                    "openwebui_version": OPENWEBUI_VERSION,
                    "openwebui_image": OPENWEBUI_IMAGE,
                    "deployment": DEPLOYMENT,
                    "preset": PRESET,
                    "chat_id": chat_id,
                    "refusal_mode": args.refusal,
                    "refusal": refusal,
                    "turns": args.turns,
                    "compaction_token_threshold": COMPACTION_TOKEN_THRESHOLD,
                    "audit_artifact": str(audit_dir / "records-001.jsonl"),
                    "captures": str(captures),
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        print(f"artifacts under {out}", flush=True)
        return 0
    except (urllib.error.HTTPError, urllib.error.URLError) as exc:
        detail = ""
        if isinstance(exc, urllib.error.HTTPError):
            detail = exc.read().decode("utf-8", "replace")[:2000]
        print(f"live run failed: {exc} {detail}", file=sys.stderr)
        logs = _docker("logs", "--tail", "80", CONTAINER, check=False)
        print(logs.stdout[-4000:] + logs.stderr[-4000:], file=sys.stderr)
        return 1
    finally:
        if not args.keep:
            teardown(server, thread)


if __name__ == "__main__":
    raise SystemExit(main())
