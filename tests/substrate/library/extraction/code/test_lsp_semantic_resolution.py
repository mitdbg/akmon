from __future__ import annotations

import sys
from pathlib import Path

import pytest

from okg.substrate.library.extraction.code import lsp as lsp_module
from okg.substrate.library.extraction.code.lsp import (
    LspSemanticResolver,
    LspServerConfig,
    LspServerUnavailable,
    _close_persistent_clients,
)
from okg.substrate.library.extraction.code.lsp_protocol import (
    DEFINITION_METHOD,
    LSP_SPEC_VERSION,
)


@pytest.fixture(autouse=True)
def _reset_persistent_lsp_clients():
    """Persistent LSP clients live at process scope; close them between tests
    so one test's reused server never leaks into another."""
    _close_persistent_clients()
    yield
    _close_persistent_clients()


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _hermetic_lsp_server(path: Path) -> None:
    path.write_text(
        r'''
from __future__ import annotations

import json
import sys
from pathlib import Path
from urllib.parse import unquote, urlparse

root = None
definitions = {}


def read_message():
    headers = {}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        if line in {b"\r\n", b"\n"}:
            break
        key, _, value = line.decode("ascii").partition(":")
        headers[key.lower()] = value.strip()
    length = int(headers.get("content-length", "0"))
    if length <= 0:
        return None
    return json.loads(sys.stdin.buffer.read(length).decode("utf-8"))


def send(payload):
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    sys.stdout.buffer.write(
        f"Content-Length: {len(body)}\r\n\r\n".encode("ascii") + body
    )
    sys.stdout.buffer.flush()


def rel_from_uri(uri):
    parsed = urlparse(uri)
    path = Path(unquote(parsed.path)).resolve()
    return path.relative_to(root).as_posix()


while True:
    msg = read_message()
    if msg is None:
        break
    method = msg.get("method")
    if method == "initialize":
        params = msg.get("params") or {}
        root = Path(unquote(urlparse(params["rootUri"]).path)).resolve()
        definitions = (params.get("initializationOptions") or {}).get(
            "definitions",
            {},
        )
        send({
            "jsonrpc": "2.0",
            "id": msg["id"],
            "result": {
                "capabilities": {
                    "definitionProvider": True,
                    "textDocumentSync": 1,
                },
                "serverInfo": {
                    "name": "hermetic-python-lsp",
                    "version": "0.1",
                },
            },
        })
    elif method == "textDocument/definition":
        params = msg.get("params") or {}
        rel = rel_from_uri(params["textDocument"]["uri"])
        line = int(params["position"]["line"]) + 1
        character = int(params["position"]["character"])
        location = definitions.get(f"{rel}:{line}:{character}")
        send({
            "jsonrpc": "2.0",
            "id": msg["id"],
            "result": [location] if location else None,
        })
    elif method == "shutdown":
        send({"jsonrpc": "2.0", "id": msg["id"], "result": None})
    elif method == "exit":
        break
''',
        encoding="utf-8",
    )


def _hermetic_lsp_server_with_out_of_order_definitions(path: Path) -> None:
    path.write_text(
        r'''
from __future__ import annotations

import json
import sys
from pathlib import Path
from urllib.parse import unquote, urlparse

root = None
definitions = {}
definition_request_count = 0
held_definition_response = None
hold_first_definition_until = 0


def read_message():
    headers = {}
    while True:
        line = sys.stdin.buffer.readline()
        if not line:
            return None
        if line in {b"\r\n", b"\n"}:
            break
        key, _, value = line.decode("ascii").partition(":")
        headers[key.lower()] = value.strip()
    length = int(headers.get("content-length", "0"))
    if length <= 0:
        return None
    return json.loads(sys.stdin.buffer.read(length).decode("utf-8"))


def send(payload):
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    sys.stdout.buffer.write(
        f"Content-Length: {len(body)}\r\n\r\n".encode("ascii") + body
    )
    sys.stdout.buffer.flush()


def rel_from_uri(uri):
    parsed = urlparse(uri)
    path = Path(unquote(parsed.path)).resolve()
    return path.relative_to(root).as_posix()


while True:
    msg = read_message()
    if msg is None:
        break
    method = msg.get("method")
    if method == "initialize":
        params = msg.get("params") or {}
        options = params.get("initializationOptions") or {}
        root = Path(unquote(urlparse(params["rootUri"]).path)).resolve()
        definitions = options.get("definitions", {})
        hold_first_definition_until = int(
            options.get("hold_first_definition_until", 0) or 0
        )
        send({
            "jsonrpc": "2.0",
            "id": msg["id"],
            "result": {
                "capabilities": {
                    "definitionProvider": True,
                    "textDocumentSync": 1,
                },
                "serverInfo": {
                    "name": "hermetic-python-lsp",
                    "version": "0.1",
                },
            },
        })
    elif method == "textDocument/definition":
        params = msg.get("params") or {}
        rel = rel_from_uri(params["textDocument"]["uri"])
        line = int(params["position"]["line"]) + 1
        character = int(params["position"]["character"])
        location = definitions.get(f"{rel}:{line}:{character}")
        response = {
            "jsonrpc": "2.0",
            "id": msg["id"],
            "result": [location] if location else None,
        }
        definition_request_count += 1
        if (
            definition_request_count == 1
            and hold_first_definition_until > 1
        ):
            held_definition_response = response
            continue
        send(response)
        if (
            held_definition_response is not None
            and definition_request_count >= hold_first_definition_until
        ):
            send(held_definition_response)
            held_definition_response = None
    elif method == "shutdown":
        send({"jsonrpc": "2.0", "id": msg["id"], "result": None})
    elif method == "exit":
        break
''',
        encoding="utf-8",
    )


def test_python_cross_file_calls_emit_lsp_edges(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _write(
        repo / "src" / "a.py",
        "from src.b import helper\n\n"
        "def caller():\n"
        "    return helper()\n",
    )
    _write(
        repo / "src" / "b.py",
        "def helper():\n"
        "    return 1\n",
    )
    server = tmp_path / "hermetic_lsp_server.py"
    _hermetic_lsp_server(server)

    target_uri = (repo / "src" / "b.py").resolve().as_uri()
    resolver = LspSemanticResolver(
        repo="demo",
        repo_root=repo,
        config=LspServerConfig(
            command=[sys.executable, str(server)],
            language_id="python",
            server_name="hermetic-python-lsp",
            server_version="0.1",
            initialization_options={
                "definitions": {
                    "src/a.py:4:11": {
                        "uri": target_uri,
                        "range": {
                            "start": {"line": 0, "character": 0},
                            "end": {"line": 1, "character": 12},
                        },
                    },
                },
            },
        ),
    )

    report = resolver.resolve_workspace(
        ["src/a.py", "src/b.py"],
        workspace_fingerprint="fixture-workspace",
        source_frontier="single-delta:src/a.py",
    )
    facts = report.edge_facts()

    assert report.lsp_spec_version == LSP_SPEC_VERSION
    assert report.enabled_methods == [DEFINITION_METHOD]
    assert report.disabled_methods == []
    assert report.references_inspected == 1
    assert report.references_resolved == 1
    assert report.unresolved_by_reason == {}

    edge_keys = {(fact.src, fact.edge_type, fact.dst) for fact in facts}
    assert (
        "reference:" in facts[0].src
        and "symbol:demo:src/b.py:helper:1" in {fact.dst for fact in facts}
    )
    assert any(edge_type == "references" for _, edge_type, _ in edge_keys)
    assert any(edge_type == "calls" for _, edge_type, _ in edge_keys)
    for fact in facts:
        assert fact.attrs["resolution_kind"] == "cross_file_lsp"
        assert fact.attrs["resolution_method"] == "lsp"
        assert fact.attrs["resolver"] == "hermetic-python-lsp"
        assert fact.attrs["resolver_version"] == "0.1"
        assert fact.attrs["lsp_method"] == DEFINITION_METHOD
        assert fact.attrs["server_capability"] == "definitionProvider"
        assert fact.attrs["semantic_target_key"]
        assert fact.attrs["workspace_fingerprint"] == "fixture-workspace"
        assert fact.attrs["source_frontier"] == "single-delta:src/a.py"


def test_definition_requests_skip_builtins_and_external_imports(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    _write(
        repo / "src" / "a.py",
        "import pytest\n"
        "from src.b import helper\n\n"
        "def caller(values):\n"
        "    len(values)\n"
        "    pytest.raises(ValueError)\n"
        "    return helper()\n",
    )
    _write(repo / "src" / "b.py", "def helper():\n    return 1\n")
    server = tmp_path / "hermetic_lsp_server.py"
    _hermetic_lsp_server(server)

    target_uri = (repo / "src" / "b.py").resolve().as_uri()
    resolver = LspSemanticResolver(
        repo="demo",
        repo_root=repo,
        config=LspServerConfig(
            command=[sys.executable, str(server)],
            language_id="python",
            server_name="hermetic-python-lsp",
            server_version="0.1",
            initialization_options={
                "definitions": {
                    "src/a.py:7:11": {
                        "uri": target_uri,
                        "range": {
                            "start": {"line": 0, "character": 0},
                            "end": {"line": 1, "character": 12},
                        },
                    },
                },
            },
        ),
    )

    report = resolver.resolve_workspace(["src/a.py", "src/b.py"])

    assert report.references_inspected == 1
    assert report.references_resolved == 1
    assert report.references_candidate_skipped == 2
    assert report.references_skipped_by_reason == {
        "external_import": 1,
        "python_builtin": 1,
    }


def test_definition_requests_are_pipelined_and_id_correlated(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    _write(
        repo / "src" / "a.py",
        "from src.b import helper\n"
        "from src.c import other\n\n"
        "def caller():\n"
        "    left = helper()\n"
        "    return other(left)\n",
    )
    _write(repo / "src" / "b.py", "def helper():\n    return 1\n")
    _write(repo / "src" / "c.py", "def other(value):\n    return value\n")
    server = tmp_path / "hermetic_lsp_server.py"
    _hermetic_lsp_server_with_out_of_order_definitions(server)

    helper_uri = (repo / "src" / "b.py").resolve().as_uri()
    other_uri = (repo / "src" / "c.py").resolve().as_uri()
    resolver = LspSemanticResolver(
        repo="demo",
        repo_root=repo,
        config=LspServerConfig(
            command=[sys.executable, str(server)],
            language_id="python",
            server_name="hermetic-python-lsp",
            server_version="0.1",
            timeout_s=1.0,
            definition_request_concurrency=2,
            initialization_options={
                "hold_first_definition_until": 2,
                "definitions": {
                    "src/a.py:5:11": {
                        "uri": helper_uri,
                        "range": {
                            "start": {"line": 0, "character": 0},
                            "end": {"line": 1, "character": 12},
                        },
                    },
                    "src/a.py:6:11": {
                        "uri": other_uri,
                        "range": {
                            "start": {"line": 0, "character": 0},
                            "end": {"line": 1, "character": 12},
                        },
                    },
                },
            },
        ),
    )

    report = resolver.resolve_workspace(["src/a.py", "src/b.py", "src/c.py"])
    resolved_targets = {
        resolved.attrs["raw_qualified_name"]: resolved.target_symbol_id
        for resolved in report.resolved
    }

    assert report.definition_request_concurrency == 2
    assert report.references_inspected == 2
    assert report.references_resolved == 2
    assert resolved_targets["helper"].endswith("symbol:demo:src/b.py:helper:1")
    assert resolved_targets["other"].endswith("symbol:demo:src/c.py:other:1")


def test_persistent_server_is_reused_across_resolves(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    _write(
        repo / "src" / "a.py",
        "from src.b import helper\n\n"
        "def caller():\n"
        "    return helper()\n",
    )
    _write(repo / "src" / "b.py", "def helper():\n    return 1\n")
    server = tmp_path / "hermetic_lsp_server.py"
    _hermetic_lsp_server(server)

    target_uri = (repo / "src" / "b.py").resolve().as_uri()
    config = LspServerConfig(
        command=[sys.executable, str(server)],
        language_id="python",
        server_name="hermetic-python-lsp",
        server_version="0.1",
        initialization_options={
            "definitions": {
                "src/a.py:4:11": {
                    "uri": target_uri,
                    "range": {
                        "start": {"line": 0, "character": 0},
                        "end": {"line": 1, "character": 12},
                    },
                },
            },
        },
    )
    resolver = LspSemanticResolver(repo="demo", repo_root=repo, config=config)

    first = resolver.resolve_workspace(["src/a.py", "src/b.py"])
    key = lsp_module._persistent_key(config, resolver.repo_root)
    first_pid = lsp_module._PERSISTENT_CLIENTS[key]._proc.pid

    # The cold resolve must NOT report reuse, and must leave the server alive
    # in the registry (not torn down at the end of the run).
    assert first.server_reused is False
    assert lsp_module._PERSISTENT_CLIENTS[key].is_alive()

    second = resolver.resolve_workspace(["src/a.py", "src/b.py"])
    second_pid = lsp_module._PERSISTENT_CLIENTS[key]._proc.pid

    # Same process object reused (no respawn, no re-initialize).
    assert second.server_reused is True
    assert second_pid == first_pid
    # Correctness: a reused server resolves the identical edge set.
    assert second.references_inspected == first.references_inspected == 1
    assert second.references_resolved == first.references_resolved == 1
    first_edges = {
        (f.src, f.edge_type, f.dst) for f in first.edge_facts()
    }
    second_edges = {
        (f.src, f.edge_type, f.dst) for f in second.edge_facts()
    }
    assert second_edges == first_edges


def test_warm_resolve_refreshes_prior_opened_files_not_in_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression (PR #448 deep review): on a warm resolve, the
    didChangeWatchedFiles refresh must cover EVERY file the server has open from
    prior resolves -- not just this resolve's paths. A transitively-imported
    file changed on disk but absent from this resolve's paths would otherwise
    stay at its stale prior-open content (an open document shadows disk),
    diverging from a cold resolve."""
    repo = tmp_path / "repo"
    _write(
        repo / "src" / "a.py",
        "from src.b import helper\n\ndef caller():\n    return helper()\n",
    )
    _write(repo / "src" / "b.py", "def helper():\n    return 1\n")
    server = tmp_path / "hermetic_lsp_server.py"
    _hermetic_lsp_server(server)
    config = LspServerConfig(
        command=[sys.executable, str(server)],
        language_id="python",
        server_name="hermetic-python-lsp",
        server_version="0.1",
        initialization_options={"definitions": {}},
    )

    watched: list[list[str]] = []
    original = lsp_module._JsonRpcLspClient.did_change_watched

    def _spy(self, uris):  # noqa: ANN001
        watched.append(list(uris))
        return original(self, uris)

    monkeypatch.setattr(
        lsp_module._JsonRpcLspClient, "did_change_watched", _spy,
    )

    resolver = LspSemanticResolver(repo="demo", repo_root=repo, config=config)
    # Cold resolve opens BOTH files; the cold path sends no didChangeWatched.
    resolver.resolve_workspace(["src/a.py", "src/b.py"])
    assert watched == []
    # Warm resolve of a.py ONLY: b.py is a prior-open absent from this resolve's
    # paths and must still be refreshed.
    resolver.resolve_workspace(["src/a.py"])

    b_uri = (repo / "src" / "b.py").resolve().as_uri()
    assert watched, "a warm resolve must send didChangeWatchedFiles"
    assert b_uri in watched[-1]


def test_configured_missing_server_refuses_to_boot(tmp_path: Path) -> None:
    _write(tmp_path / "src" / "a.py", "def caller():\n    return 1\n")
    resolver = LspSemanticResolver(
        repo="demo",
        repo_root=tmp_path,
        config=LspServerConfig(
            command=[str(tmp_path / "missing-lsp-server")],
            language_id="python",
            server_name="missing-python-lsp",
        ),
    )

    with pytest.raises(LspServerUnavailable) as exc_info:
        resolver.resolve_workspace(["src/a.py"])

    assert exc_info.value.code == "lsp_server_unavailable"
    assert "missing-lsp-server" in str(exc_info.value)


def test_uv_server_command_uses_uv_env_when_path_is_narrow(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = tmp_path / "repo"
    _write(
        repo / "src" / "a.py",
        "from src.b import helper\n\n"
        "def caller():\n"
        "    return helper()\n",
    )
    _write(repo / "src" / "b.py", "def helper():\n    return 1\n")
    server = tmp_path / "hermetic_lsp_server.py"
    _hermetic_lsp_server(server)
    fake_uv = tmp_path / "uv"
    fake_uv.write_text(
        "#!/bin/sh\nexec \"$PYTHON_FOR_LSP_TEST\" \"$@\"\n",
        encoding="utf-8",
    )
    fake_uv.chmod(0o755)
    monkeypatch.setenv("PATH", "")
    monkeypatch.setenv("UV", str(fake_uv))
    monkeypatch.setenv("PYTHON_FOR_LSP_TEST", sys.executable)

    target_uri = (repo / "src" / "b.py").resolve().as_uri()
    resolver = LspSemanticResolver(
        repo="demo",
        repo_root=repo,
        config=LspServerConfig(
            command=["uv", str(server)],
            language_id="python",
            server_name="hermetic-python-lsp",
            server_version="0.1",
            initialization_options={
                "definitions": {
                    "src/a.py:4:11": {
                        "uri": target_uri,
                        "range": {
                            "start": {"line": 0, "character": 0},
                            "end": {"line": 1, "character": 12},
                        },
                    },
                },
            },
        ),
    )

    report = resolver.resolve_workspace(["src/a.py", "src/b.py"])

    assert report.references_resolved == 1
    assert report.edge_facts()


def test_persistent_resolve_lock_is_per_key(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    cfg = LspServerConfig(
        command=["x"], language_id="python", server_name="s", server_version="1",
    )
    cfg2 = LspServerConfig(
        command=["y"], language_id="python", server_name="s", server_version="1",
    )
    lock_a = lsp_module._persistent_resolve_lock(cfg, repo)
    assert lsp_module._persistent_resolve_lock(cfg, repo) is lock_a  # same key
    assert lsp_module._persistent_resolve_lock(cfg2, repo) is not lock_a


def test_concurrent_resolves_on_same_persistent_key_serialize(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression (PR #448 deep review): a persistent LSP client (one process /
    stdin / reader thread / non-atomic request-id) is shared per (command,
    repo_root). Concurrent resolves on the same key MUST serialize, or they
    interleave stdin writes and cross-wire responses (thread A's definition
    result returned to thread B). resolve_workspace holds a per-key lock for the
    whole acquire+resolve; assert no two ever overlap.
    """
    import threading
    import time

    repo = tmp_path / "repo"
    (repo / "src").mkdir(parents=True)
    (repo / "src" / "a.py").write_text("x = 1\n", encoding="utf-8")
    config = LspServerConfig(
        command=[sys.executable],
        language_id="python",
        server_name="serialize-test",
        server_version="1",
    )

    # No real server: the acquire is a no-op so the test exercises only the lock.
    monkeypatch.setattr(
        lsp_module,
        "_acquire_persistent_client",
        lambda _config, _repo: (object(), {}, False),
    )

    counter_lock = threading.Lock()
    state = {"concurrent": 0, "max": 0}

    def _tracking_resolve(self, **_kwargs):  # noqa: ANN001
        with counter_lock:
            state["concurrent"] += 1
            state["max"] = max(state["max"], state["concurrent"])
        time.sleep(0.05)
        with counter_lock:
            state["concurrent"] -= 1
        return object()

    monkeypatch.setattr(
        LspSemanticResolver, "_resolve_with_client", _tracking_resolve,
    )

    def _run() -> None:
        LspSemanticResolver(
            repo="d", repo_root=repo, config=config,
        ).resolve_workspace(["src/a.py"])

    threads = [threading.Thread(target=_run) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    # Same (command, repo_root) -> same per-key lock -> the resolves never
    # overlap. Without the lock, max would be > 1.
    assert state["max"] == 1
