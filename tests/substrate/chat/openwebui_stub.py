"""A standalone HTTP server that answers like Open WebUI v0.11.0's liveness
surface, run as a REAL child process by the instance-lifecycle suite.

Why a process and not a thread: the supervision requirement is proved by
killing the instance and watching the supervisor bring it back. A thread
cannot be killed, and a mock that "pretends to be down" proves the mock. This
is a real process holding a real listening socket, so ``SIGKILL`` genuinely
takes the port down and ``GET /api/version`` genuinely fails until something
starts it again.

What it is NOT: it is not Open WebUI. It answers three paths with the shapes
the lifecycle reads — the version envelope measured live at v0.11.0
(``{"version": "0.11.0", "deployment_id": ""}``), an index carrying an
identifying marker, and one API path — and nothing else. Every requirement
arm that needs the vendor's own behaviour (the SQLite fallback store, the app
database schema, the real index bytes) is a live arm, recorded as an
operator_report, never closed here.

Usage: ``python openwebui_stub.py <port> <version> <marker>``
"""

from __future__ import annotations

import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

#: Present in the index body so a front-door proof can assert it reached THIS
#: instance rather than a proxy error page that happened to return 200.
INDEX_TEMPLATE = (
    "<!doctype html><html><head><title>Open WebUI</title></head>"
    "<body><div id=\"app\" data-okg-instance=\"{marker}\">"
    "Open WebUI {version}</div></body></html>"
)


def build_handler(version: str, marker: str):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args):
            return

        def _send(self, status: int, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler's contract
            if self.path == "/api/version":
                payload = {"version": version, "deployment_id": ""}
                self._send(
                    200, json.dumps(payload).encode(), "application/json",
                )
                return
            if self.path.startswith("/api/"):
                payload = {
                    "instance_marker": marker,
                    "version": version,
                    "path": self.path,
                }
                self._send(
                    200, json.dumps(payload).encode(), "application/json",
                )
                return
            body = INDEX_TEMPLATE.format(marker=marker, version=version)
            self._send(200, body.encode(), "text/html; charset=utf-8")

    return Handler


def main(argv: list[str]) -> int:
    port = int(argv[1])
    version = argv[2]
    marker = argv[3]
    server = ThreadingHTTPServer(("127.0.0.1", port), build_handler(version, marker))
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
