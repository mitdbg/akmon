from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parents[2]


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _spawn_dev_server(args: list[str], *, env: dict[str, str]) -> subprocess.Popen[str]:
    """Spawn the operator-ui dev server isolated from the pytest process group.

    ``start_new_session=True`` puts ``npm`` (and the ``node``/``vite`` subtree it
    forks) in its own session/process group so a signal delivered to — or by —
    that tree can never reach the pytest-xdist worker or controller group, and
    ``stdin=DEVNULL`` detaches it from the worker's inherited execnet control
    channel. Both are required to keep the parallel lane deterministic (#703).
    """
    return subprocess.Popen(
        args,
        cwd=ROOT,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )


def _terminate_process_group(proc: subprocess.Popen[str]) -> None:
    """Tear down the whole session-leader group so no child (node/vite) is orphaned."""
    try:
        pgid = os.getpgid(proc.pid)
    except ProcessLookupError:
        return
    for sig in (signal.SIGTERM, signal.SIGKILL):
        try:
            os.killpg(pgid, sig)
        except ProcessLookupError:
            return
        try:
            proc.wait(timeout=5)
            return
        except subprocess.TimeoutExpired:
            continue


def _wait_for_url(url: str, *, timeout: float = 12.0) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urlopen(url, timeout=1.0) as response:
                if response.status == 200:
                    return
        except Exception as exc:  # noqa: BLE001 - startup polling only
            last_error = exc
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {url}: {last_error}")


def test_operator_ui_dev_server_does_not_serve_spa_for_api_paths() -> None:
    port = _free_port()
    env = {
        **os.environ,
        "OKG_OPERATOR_API_BASE": "",
        "VITE_OKG_API_BASE": "",
    }
    proc = _spawn_dev_server(
        ["npm", "run", "operator-ui:dev", "--", "--port", str(port)],
        env=env,
    )
    try:
        _wait_for_url(f"http://127.0.0.1:{port}/")

        with urlopen(
            f"http://127.0.0.1:{port}/viz/api/console/health",
            timeout=2.0,
        ) as response:
            assert response.status == 200
            assert response.headers["content-type"].startswith("application/json")
            payload = json.loads(response.read().decode("utf-8"))

        assert payload["ok"] is False
        assert payload["status"] == 502
        assert payload["path"] == "/viz/api/console/health"
        assert "no OKG API target" in payload["error"]
    finally:
        _terminate_process_group(proc)


def test_operator_ui_dev_server_proxies_api_target() -> None:
    api_port = _free_port()
    received_paths: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802 - stdlib hook name
            received_paths.append(self.path)
            body = json.dumps({"ok": True, "path": self.path}).encode("utf-8")
            self.send_response(200)
            self.send_header("content-type", "application/json")
            self.send_header("content-length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", api_port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    ui_port = _free_port()
    env = {
        **os.environ,
        "OKG_OPERATOR_API_BASE": f"http://127.0.0.1:{api_port}",
        "VITE_OKG_API_BASE": "",
    }
    proc = _spawn_dev_server(
        ["npm", "run", "operator-ui:dev", "--", "--port", str(ui_port)],
        env=env,
    )
    try:
        _wait_for_url(f"http://127.0.0.1:{ui_port}/")
        with urlopen(
            f"http://127.0.0.1:{ui_port}/viz/api/console/health",
            timeout=2.0,
        ) as response:
            assert response.status == 200
            assert response.headers["content-type"].startswith("application/json")
            payload = json.loads(response.read().decode("utf-8"))

        assert payload == {"ok": True, "path": "/viz/api/console/health"}
        assert received_paths == ["/viz/api/console/health"]
    finally:
        _terminate_process_group(proc)
        server.shutdown()
        server.server_close()
