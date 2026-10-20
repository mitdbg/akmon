"""LSP-backed semantic reference resolution.

This is the protocol backend only. Source registration and incremental runtime
ownership live above this layer; this module starts a configured language-server
command, asks advertised methods, and normalizes successful answers into OKG
edge facts.
"""
from __future__ import annotations

import atexit
import builtins
import json
import os
import queue
import shutil
import subprocess
import threading
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Mapping, Sequence

from okg.substrate.errors import SubstrateError
from okg.substrate.library.extraction.code._protocols import (
    CodeSymbolReport,
    Import,
    Reference,
    Symbol,
)
from okg.substrate.library.extraction.code.dispatch import code_extractor_for
from okg.substrate.library.extraction.code.lsp_protocol import (
    DEFINITION_METHOD,
    LSP_SPEC_VERSION,
    semantic_key_for_symbol,
)
from okg.substrate.library.sources._code_graph_common import (
    _module_qname,
    _sha,
    _symbol_id,
)
from okg.substrate.library.sources.base import EdgeFact
from okg.substrate.library.sources.code_graph_structure import (
    _reference_attrs,
    _reference_edge_attrs,
    _reference_identity_attrs,
)


__all__ = [
    "LspProtocolError",
    "LspResolvedReference",
    "LspResolutionReport",
    "LspSemanticResolver",
    "LspServerConfig",
    "LspServerUnavailable",
    "lsp_definition_candidate_count",
    "lsp_definition_filter_fingerprint",
]


_PYTHON_BUILTIN_CALLS = frozenset(dir(builtins))
_UV_EXECUTABLE_FALLBACKS = (
    Path("/opt/homebrew/bin/uv"),
    Path("/usr/local/bin/uv"),
    Path.home() / ".local/bin" / "uv",
)


def _resolve_command_executable(executable: str) -> str | None:
    if os.path.isabs(executable):
        path = Path(executable)
        return str(path) if _is_executable_file(path) else None

    found = shutil.which(executable)
    if found:
        return found

    if executable != "uv":
        return None

    configured_uv = os.environ.get("UV")
    candidates = [
        Path(configured_uv).expanduser(),
        *[path.expanduser() for path in _UV_EXECUTABLE_FALLBACKS],
    ] if configured_uv else [
        path.expanduser() for path in _UV_EXECUTABLE_FALLBACKS
    ]
    for path in candidates:
        if _is_executable_file(path):
            return str(path)
    return None


def _is_executable_file(path: Path) -> bool:
    return path.is_file() and os.access(path, os.X_OK)


class LspServerUnavailable(SubstrateError):
    code = "lsp_server_unavailable"


class LspProtocolError(SubstrateError):
    code = "lsp_protocol_error"


@dataclass(frozen=True)
class LspServerConfig:
    command: Sequence[str]
    language_id: str
    server_name: str
    server_version: str = ""
    initialization_options: Mapping[str, Any] | None = None
    timeout_s: float = 5.0
    max_definition_requests: int = 2000
    definition_request_concurrency: int = 16
    position_encoding: str = "utf-16"
    lsp_spec_version: str = LSP_SPEC_VERSION


@dataclass(frozen=True)
class _Document:
    rel_path: str
    text: str
    report: CodeSymbolReport
    symbol_id_by_qname: dict[str, str]
    semantic_key_by_qname: dict[str, str]


@dataclass(frozen=True)
class LspResolvedReference:
    rel_path: str
    reference_id: str
    caller_symbol_id: str
    target_symbol_id: str
    reference_kind: str
    attrs: dict[str, Any]

    def edge_facts(self) -> list[EdgeFact]:
        record_id = {
            "path": self.rel_path,
            "reference_id": self.reference_id,
            "resolver": self.attrs.get("resolver", ""),
        }
        revision = {
            "source_frontier": self.attrs.get("source_frontier", ""),
            "workspace_fingerprint": self.attrs.get(
                "workspace_fingerprint",
                "",
            ),
        }
        edge_attrs = _reference_edge_attrs(self.attrs, self.reference_kind)
        facts = [
            EdgeFact(
                src=self.reference_id,
                dst=self.target_symbol_id,
                edge_type="references",
                attrs=edge_attrs,
                source_record_id=record_id,
                source_revision=revision,
            ),
        ]
        if self.caller_symbol_id and self.reference_kind == "call":
            facts.append(EdgeFact(
                src=self.caller_symbol_id,
                dst=self.target_symbol_id,
                edge_type="calls",
                attrs={
                    **edge_attrs,
                    "line": self.attrs.get("start_line", 0),
                },
                source_record_id=record_id,
                source_revision=revision,
            ))
        return facts


@dataclass(frozen=True)
class LspResolutionReport:
    resolver: str
    resolver_version: str
    lsp_spec_version: str
    server_capabilities: dict[str, Any]
    enabled_methods: list[str]
    disabled_methods: list[str]
    definition_request_concurrency: int
    references_inspected: int
    references_resolved: int
    references_candidate_skipped: int
    references_inspected_by_path: dict[str, int]
    references_resolved_by_path: dict[str, int]
    references_skipped_by_reason: dict[str, int]
    unresolved_by_path: dict[str, dict[str, int]]
    unresolved_by_reason: dict[str, int]
    server_reused: bool = False
    resolved: list[LspResolvedReference] = field(default_factory=list)

    def edge_facts(self) -> list[EdgeFact]:
        facts: list[EdgeFact] = []
        for resolved in self.resolved:
            facts.extend(resolved.edge_facts())
        return facts


class LspSemanticResolver:
    def __init__(
        self,
        *,
        repo: str,
        repo_root: Path | str,
        config: LspServerConfig,
    ) -> None:
        self.repo = repo
        self.repo_root = Path(repo_root).expanduser().resolve()
        self.config = config

    def resolve_workspace(
        self,
        paths: Sequence[Path | str],
        *,
        resolve_paths: Sequence[str] | None = None,
        workspace_fingerprint: str = "",
        source_frontier: str = "",
    ) -> LspResolutionReport:
        runtime_config = self._runtime_config()
        docs = self._extract_documents(paths)
        # Serialize ALL use of the shared persistent client for this key -- the
        # acquire (which may start the process) AND the resolve I/O run under one
        # per-key lock, so two threads resolving the same (command, repo_root)
        # cannot interleave on the single stdin / request-id / pending-response
        # state. Doc extraction above is local and stays outside the lock.
        with _persistent_resolve_lock(runtime_config, self.repo_root):
            client, opened_versions, server_reused = _acquire_persistent_client(
                runtime_config,
                self.repo_root,
            )
            try:
                return self._resolve_with_client(
                    client=client,
                    opened_versions=opened_versions,
                    server_reused=server_reused,
                    docs=docs,
                    resolve_paths=resolve_paths,
                    workspace_fingerprint=workspace_fingerprint,
                    source_frontier=source_frontier,
                )
            except LspProtocolError:
                # A protocol error may have left the persistent server in an
                # unknown state; drop it so the next resolve boots a clean one.
                _discard_persistent_client(runtime_config, self.repo_root)
                raise

    def _resolve_with_client(
        self,
        *,
        client: "_JsonRpcLspClient",
        opened_versions: dict[str, int],
        server_reused: bool,
        docs: dict[str, _Document],
        resolve_paths: Sequence[str] | None,
        workspace_fingerprint: str,
        source_frontier: str,
    ) -> LspResolutionReport:
        # On a reused client, skip initialize and read the cached capabilities.
        # On a fresh client, _acquire_persistent_client already initialized it.
        capabilities = client.capabilities
        definition_supported = bool(capabilities.get("definitionProvider"))
        enabled_methods = [DEFINITION_METHOD] if definition_supported else []
        disabled_methods = [] if definition_supported else [DEFINITION_METHOD]
        doc_uris = [
            _path_uri(self.repo_root / doc.rel_path) for doc in docs.values()
        ]
        if server_reused:
            # Tell pyright to re-read from disk (change type Changed=2) BEFORE
            # any didChange/didOpen, so a reused server resolves against current
            # content exactly as a fresh initialize would. Refresh not just this
            # resolve's docs but EVERY file the server already has open from
            # prior resolves: a transitively-imported file that changed on disk
            # but is absent from this resolve's paths would otherwise stay at its
            # stale prior-open content (an open document shadows disk), diverging
            # from the cold path. (Correctness gate: cross-file edge counts MUST
            # stay identical to cold -- exercised by the bench lsp-proof.)
            refresh_uris = sorted(set(doc_uris) | set(opened_versions.keys()))
            client.did_change_watched(refresh_uris)
        for doc in docs.values():
            uri = _path_uri(self.repo_root / doc.rel_path)
            prior_version = opened_versions.get(uri)
            if prior_version is None:
                client.did_open(
                    uri=uri,
                    language_id=self.config.language_id,
                    text=doc.text,
                )
                opened_versions[uri] = 1
            else:
                next_version = prior_version + 1
                client.did_change(
                    uri=uri,
                    version=next_version,
                    text=doc.text,
                )
                opened_versions[uri] = next_version
        resolved: list[LspResolvedReference] = []
        unresolved: dict[str, int] = {}
        skipped: dict[str, int] = {}
        inspected_by_path: dict[str, int] = {}
        resolved_by_path: dict[str, int] = {}
        unresolved_by_path: dict[str, dict[str, int]] = {}
        skipped_total = 0
        inspected = 0
        resolve_set = {
            str(path).replace("\\", "/").lstrip("/")
            for path in (resolve_paths or docs.keys())
        }
        workspace_modules = _workspace_modules(docs.keys())
        if definition_supported:
            pending_requests: list[tuple[_Document, Reference, int, int]] = []
            request_limit = max(0, int(self.config.max_definition_requests))
            request_cap_exceeded = False
            for doc in docs.values():
                if doc.rel_path not in resolve_set:
                    continue
                for ref in doc.report.references:
                    skip_reason = _definition_candidate_skip_reason(
                        doc.report,
                        ref,
                        rel_path=doc.rel_path,
                        language_id=self.config.language_id,
                        workspace_modules=workspace_modules,
                    )
                    if skip_reason:
                        skipped_total += 1
                        _inc(skipped, skip_reason)
                        continue
                    if len(pending_requests) >= request_limit:
                        _inc(unresolved, "request_cap_exceeded")
                        request_cap_exceeded = True
                        break
                    line, character = _reference_position(
                        doc.text,
                        ref,
                        position_encoding=self.config.position_encoding,
                    )
                    pending_requests.append((doc, ref, line, character))
                if request_cap_exceeded:
                    break
            inspected = len(pending_requests)
            for doc, _ref, _line, _character in pending_requests:
                _inc(inspected_by_path, doc.rel_path)
            results = client.definitions([
                {
                    "uri": _path_uri(self.repo_root / doc.rel_path),
                    "line": line,
                    "character": character,
                }
                for doc, _ref, line, character in pending_requests
            ])
            for (doc, ref, _line, _character), result in zip(
                pending_requests,
                results,
                strict=True,
            ):
                item = _first_location(result)
                if item is None:
                    _inc(unresolved, "no_definition")
                    _inc_path(unresolved_by_path, doc.rel_path, "no_definition")
                    continue
                target = self._target_symbol(item, docs)
                if target is None:
                    _inc(unresolved, "definition_not_in_workspace")
                    _inc_path(
                        unresolved_by_path,
                        doc.rel_path,
                        "definition_not_in_workspace",
                    )
                    continue
                normalized = self._resolved_reference(
                    doc,
                    ref,
                    target_symbol_id=target[0],
                    target_semantic_key=target[1],
                    workspace_fingerprint=workspace_fingerprint,
                    source_frontier=source_frontier,
                )
                if normalized is None:
                    _inc(unresolved, "caller_not_found")
                    _inc_path(
                        unresolved_by_path,
                        doc.rel_path,
                        "caller_not_found",
                    )
                    continue
                resolved.append(normalized)
                _inc(resolved_by_path, doc.rel_path)
        else:
            unresolved["definition_unsupported"] = sum(
                len(doc.report.references) for doc in docs.values()
            )
        return LspResolutionReport(
            resolver=self.config.server_name,
            resolver_version=self.config.server_version,
            lsp_spec_version=self.config.lsp_spec_version,
            server_capabilities=dict(capabilities),
            enabled_methods=enabled_methods,
            disabled_methods=disabled_methods,
            definition_request_concurrency=_definition_request_concurrency(
                self.config,
            ),
            references_inspected=inspected,
            references_resolved=len(resolved),
            references_candidate_skipped=skipped_total,
            references_inspected_by_path=inspected_by_path,
            references_resolved_by_path=resolved_by_path,
            references_skipped_by_reason=skipped,
            unresolved_by_path=unresolved_by_path,
            unresolved_by_reason=unresolved,
            server_reused=server_reused,
            resolved=resolved,
        )

    def _assert_command_available(self) -> None:
        self._resolved_command()

    def _runtime_config(self) -> LspServerConfig:
        command = self._resolved_command()
        if command == [str(part) for part in self.config.command]:
            return self.config
        return replace(self.config, command=tuple(command))

    def _resolved_command(self) -> list[str]:
        if not self.config.command:
            raise LspServerUnavailable(
                "LSP server command is empty",
                data={"server": self.config.server_name},
                fix_hint="configure a language-server command",
            )
        executable = str(self.config.command[0])
        resolved = _resolve_command_executable(executable)
        if resolved is None:
            raise LspServerUnavailable(
                f"LSP server command not found: {executable}",
                data={
                    "server": self.config.server_name,
                    "command": list(self.config.command),
                },
                fix_hint=(
                    "install the configured language server or disable "
                    "the LSP semantic resolver for this language"
                ),
            )
        return [resolved, *[str(part) for part in self.config.command[1:]]]

    def _extract_documents(
        self,
        paths: Sequence[Path | str],
    ) -> dict[str, _Document]:
        extractor = code_extractor_for(self.config.language_id)
        docs: dict[str, _Document] = {}
        for item in paths:
            path = (self.repo_root / item).resolve()
            rel_path = path.relative_to(self.repo_root).as_posix()
            text = path.read_text(encoding="utf-8", errors="replace")
            report = extractor.extract(text, path=rel_path)
            module_name = _module_qname(rel_path)
            symbols = [
                Symbol(
                    kind="module",
                    name=Path(rel_path).stem,
                    qualified_name=module_name,
                    span_start=1,
                    span_end=max(1, len(text.splitlines())),
                ),
                *report.symbols,
            ]
            symbol_id_by_qname = {
                sym.qualified_name: _symbol_id(
                    self.repo,
                    rel_path,
                    sym.qualified_name,
                    sym.span_start,
                )
                for sym in symbols
            }
            semantic_key_by_qname = {
                sym.qualified_name: semantic_key_for_symbol(
                    repo=self.repo,
                    language=report.language,
                    module_name=getattr(sym, "module_name", "") or module_name,
                    qualified_name=sym.qualified_name,
                    kind=getattr(sym, "symbol_kind", "") or sym.kind,
                    package_name=getattr(sym, "package_name", "") or "",
                )
                for sym in symbols
            }
            docs[rel_path] = _Document(
                rel_path=rel_path,
                text=text,
                report=report,
                symbol_id_by_qname=symbol_id_by_qname,
                semantic_key_by_qname=semantic_key_by_qname,
            )
        return docs

    def _target_symbol(
        self,
        location: Mapping[str, Any],
        docs: Mapping[str, _Document],
    ) -> tuple[str, str] | None:
        uri = str(location.get("targetUri") or location.get("uri") or "")
        rel_path = _uri_rel_path(uri, self.repo_root)
        if rel_path is None:
            return None
        doc = docs.get(rel_path)
        if doc is None:
            return None
        raw_range = location.get("targetSelectionRange") or location.get("range")
        if not isinstance(raw_range, Mapping):
            return None
        start = raw_range.get("start")
        if not isinstance(start, Mapping):
            return None
        line = int(start.get("line") or 0) + 1
        candidates = [
            sym for sym in doc.report.symbols
            if sym.span_start <= line <= sym.span_end
        ]
        if not candidates:
            return None
        candidates.sort(key=lambda sym: (
            sym.span_end - sym.span_start,
            sym.span_start,
            sym.qualified_name,
        ))
        target = candidates[0]
        return (
            doc.symbol_id_by_qname[target.qualified_name],
            doc.semantic_key_by_qname.get(target.qualified_name, ""),
        )

    def _resolved_reference(
        self,
        doc: _Document,
        ref: Reference,
        *,
        target_symbol_id: str,
        target_semantic_key: str,
        workspace_fingerprint: str,
        source_frontier: str,
    ) -> LspResolvedReference | None:
        caller_symbol_id = doc.symbol_id_by_qname.get(ref.caller_qname, "")
        if ref.caller_qname and not caller_symbol_id:
            return None
        identity_attrs = _reference_attrs(
            self.repo,
            doc.rel_path,
            doc.report.language,
            ref,
            doc.symbol_id_by_qname,
            doc.semantic_key_by_qname,
        )
        ref_attrs = _reference_attrs(
            self.repo,
            doc.rel_path,
            doc.report.language,
            Reference(
                caller_qname=ref.caller_qname,
                callee_name=ref.callee_name,
                span_start=ref.span_start,
                span_end=ref.span_end,
                resolution_kind="cross_file_lsp",
                start_col=getattr(ref, "start_col", 0) or 0,
                end_col=getattr(ref, "end_col", 0) or 0,
                resolver=self.config.server_name,
                resolver_version=self.config.server_version,
                confidence="high",
                semantic_target_key=target_semantic_key,
                workspace_fingerprint=workspace_fingerprint,
                source_frontier=source_frontier,
                lsp_spec_version=self.config.lsp_spec_version,
                lsp_method=DEFINITION_METHOD,
                server_capability="definitionProvider",
            ),
            doc.symbol_id_by_qname,
            doc.semantic_key_by_qname,
        )
        reference_id = (
            f"reference:{_sha(json.dumps(
                [self.repo, doc.rel_path, _reference_identity_attrs(identity_attrs)],
                sort_keys=True,
            ))}"
        )
        return LspResolvedReference(
            rel_path=doc.rel_path,
            reference_id=reference_id,
            caller_symbol_id=caller_symbol_id,
            target_symbol_id=target_symbol_id,
            reference_kind=str(ref_attrs.get("reference_kind") or ""),
            attrs=ref_attrs,
        )


class _JsonRpcLspClient:
    def __init__(self, config: LspServerConfig) -> None:
        self.config = config
        self._proc: subprocess.Popen[bytes] | None = None
        self._next_id = 1
        self._messages: queue.Queue[dict[str, Any]] = queue.Queue()
        self._pending_responses: dict[Any, dict[str, Any]] = {}
        self._reader: threading.Thread | None = None
        # Cached server capabilities from the one initialize() handshake.
        # A reused (persistent) client skips initialize and reads this.
        self._capabilities: dict[str, Any] | None = None

    def start(self) -> "_JsonRpcLspClient":
        """Spawn the language-server process and its reader thread."""
        self._proc = subprocess.Popen(
            list(self.config.command),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()
        return self

    def close(self) -> None:
        """Politely shut down, then terminate the language-server process."""
        try:
            if self._proc and self._proc.poll() is None:
                try:
                    self._request("shutdown", None)
                except Exception:  # noqa: BLE001
                    pass
                self._notify("exit", None)
        finally:
            if self._proc and self._proc.poll() is None:
                self._proc.terminate()
                try:
                    self._proc.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    self._proc.kill()

    def is_alive(self) -> bool:
        return self._proc is not None and self._proc.poll() is None

    def __enter__(self) -> "_JsonRpcLspClient":
        return self.start()

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def initialize(self, root: Path) -> dict[str, Any]:
        result = self._request("initialize", {
            "processId": None,
            "rootUri": _path_uri(root),
            "capabilities": {
                "general": {
                    "positionEncodings": [self.config.position_encoding],
                },
                "textDocument": {
                    "definition": {"linkSupport": True},
                    "synchronization": {
                        "didOpen": True,
                        "didChange": True,
                    },
                },
                "workspace": {
                    "didChangeWatchedFiles": {"dynamicRegistration": False},
                },
            },
            "initializationOptions": dict(
                self.config.initialization_options or {},
            ),
            "trace": "off",
        })
        self._notify("initialized", {})
        if not isinstance(result, Mapping):
            raise LspProtocolError("LSP initialize result was not an object")
        capabilities = result.get("capabilities") or {}
        if not isinstance(capabilities, Mapping):
            raise LspProtocolError("LSP initialize capabilities was not an object")
        self._capabilities = dict(capabilities)
        return dict(capabilities)

    @property
    def capabilities(self) -> dict[str, Any]:
        """Capabilities cached from initialize(); empty until handshaked."""
        return dict(self._capabilities or {})

    def did_open(self, *, uri: str, language_id: str, text: str) -> None:
        self._notify("textDocument/didOpen", {
            "textDocument": {
                "uri": uri,
                "languageId": language_id,
                "version": 1,
                "text": text,
            },
        })

    def did_change(self, *, uri: str, version: int, text: str) -> None:
        """Full-document didChange — replaces the doc's content on the server.

        Sent on a REUSED server for a doc already opened this session, so the
        server resolves against current text instead of the version it last
        saw. Paired with did_change_watched so a real server (e.g. pyright)
        also re-reads from disk exactly as a fresh initialize would.
        """
        self._notify("textDocument/didChange", {
            "textDocument": {
                "uri": uri,
                "version": int(version),
            },
            "contentChanges": [{"text": text}],
        })

    def did_change_watched(self, uris: Sequence[str]) -> None:
        """Notify the server that on-disk files changed (change type Changed).

        pyright re-reads the cited files from disk, matching the post-
        initialize view a cold server would have. Without this a reused
        server can resolve against stale workspace-index content.
        """
        changes = [{"uri": str(uri), "type": 2} for uri in uris]
        if not changes:
            return
        self._notify("workspace/didChangeWatchedFiles", {"changes": changes})

    def definition(
        self,
        *,
        uri: str,
        line: int,
        character: int,
    ) -> Any:
        return self.definitions([{
            "uri": uri,
            "line": line,
            "character": character,
        }])[0]

    def definitions(self, requests: Sequence[Mapping[str, Any]]) -> list[Any]:
        if not requests:
            return []
        results: list[Any] = [None] * len(requests)
        concurrency = _definition_request_concurrency(self.config)
        next_index = 0
        in_flight: list[tuple[int, Any]] = []
        while next_index < len(requests) or in_flight:
            while next_index < len(requests) and len(in_flight) < concurrency:
                request = requests[next_index]
                request_id = self._send_request(DEFINITION_METHOD, {
                    "textDocument": {"uri": str(request["uri"])},
                    "position": {
                        "line": int(request["line"]),
                        "character": int(request["character"]),
                    },
                })
                in_flight.append((next_index, request_id))
                next_index += 1
            result_index, request_id = in_flight.pop(0)
            results[result_index] = self._receive_response(
                request_id,
                DEFINITION_METHOD,
            )
        return results

    def _request(self, method: str, params: Any) -> Any:
        request_id = self._send_request(method, params)
        return self._receive_response(request_id, method)

    def _send_request(self, method: str, params: Any) -> Any:
        request_id = self._next_id
        self._next_id += 1
        self._send({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        })
        return request_id

    def _receive_response(self, request_id: Any, method: str) -> Any:
        pending = self._pending_responses.pop(request_id, None)
        if pending is not None:
            return self._response_result(pending, method)
        while True:
            try:
                message = self._messages.get(timeout=self.config.timeout_s)
            except queue.Empty as e:
                raise LspProtocolError(
                    f"timed out waiting for LSP response to {method}",
                    data={"server": self.config.server_name, "method": method},
                ) from e
            message_id = message.get("id")
            if message_id != request_id:
                if message_id is not None:
                    self._pending_responses[message_id] = message
                continue
            return self._response_result(message, method)

    @staticmethod
    def _response_result(message: Mapping[str, Any], method: str) -> Any:
        if "error" in message:
            raise LspProtocolError(
                f"LSP request {method} failed",
                data={"error": message["error"]},
            )
        return message.get("result")

    def _notify(self, method: str, params: Any) -> None:
        self._send({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
        })

    def _send(self, payload: Mapping[str, Any]) -> None:
        if self._proc is None or self._proc.stdin is None:
            raise LspProtocolError("LSP process is not running")
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        header = f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
        self._proc.stdin.write(header + body)
        self._proc.stdin.flush()

    def _read_loop(self) -> None:
        assert self._proc is not None and self._proc.stdout is not None
        stream = self._proc.stdout
        while True:
            headers: dict[str, str] = {}
            while True:
                line = stream.readline()
                if not line:
                    return
                if line in {b"\r\n", b"\n"}:
                    break
                key, _, value = line.decode("ascii", errors="replace").partition(":")
                headers[key.lower()] = value.strip()
            length = int(headers.get("content-length", "0"))
            if length <= 0:
                continue
            body = stream.read(length)
            if not body:
                return
            try:
                self._messages.put(json.loads(body.decode("utf-8")))
            except json.JSONDecodeError:
                continue


# ---------------------------------------------------------------------------
# Persistent (process-level) LSP client registry.
#
# A fresh pyright is expensive to boot: the process spawn plus the `initialize`
# workspace index plus a `did_open` of every workspace doc costs ~400ms of fixed
# re-index, paid on EVERY resolving commit even when one reference changed. The
# registry keeps one client per (command, repo_root) alive across resolves so
# that cost is paid once. A reused client skips initialize and re-opens nothing;
# it sends did_change + didChangeWatchedFiles for the docs it touches so the
# server's view matches a cold initialize byte-for-byte (correctness gate: the
# resolved edge counts MUST be identical to the cold path).

_PersistentKey = tuple[tuple[str, ...], str]

_PERSISTENT_CLIENTS: dict[_PersistentKey, _JsonRpcLspClient] = {}
# Per-client record of which doc uris have been opened on the live server and
# the last document version we sent for each (uri -> version).
_PERSISTENT_OPENED: dict[_PersistentKey, dict[str, int]] = {}
_PERSISTENT_LOCK = threading.Lock()
# Per-(command, repo_root) lock serializing resolves that SHARE one persistent
# client. A client is one process / one stdin / one reader thread / one
# (non-atomic) request-id counter -- none of it concurrency-safe; two threads
# resolving the same key (e.g. two deployments on the same checkout + command)
# would interleave stdin writes and cross-wire responses (thread A's definition
# result handed to thread B). Held for the whole acquire+resolve so exactly one
# thread drives a given client at a time. Persists across client discards (the
# key outlives any one server process).
_PERSISTENT_RESOLVE_LOCKS: dict[_PersistentKey, threading.Lock] = {}


def _persistent_key(config: LspServerConfig, repo_root: Path) -> _PersistentKey:
    return (tuple(str(part) for part in config.command), str(repo_root))


def _persistent_resolve_lock(
    config: LspServerConfig, repo_root: Path
) -> threading.Lock:
    key = _persistent_key(config, repo_root)
    with _PERSISTENT_LOCK:
        return _PERSISTENT_RESOLVE_LOCKS.setdefault(key, threading.Lock())


def _acquire_persistent_client(
    config: LspServerConfig,
    repo_root: Path,
) -> tuple[_JsonRpcLspClient, dict[str, int], bool]:
    """Return (client, opened_versions, reused) for this (command, repo_root).

    If a cached client is alive, it is returned untouched with reused=True (no
    second start/initialize). Otherwise a fresh client is started, initialized
    against `repo_root`, cached, and returned with reused=False. A cached-but-
    dead client is discarded and recreated.

    The caller holds the returned `opened_versions` dict for the duration of a
    resolve; it is the live, shared per-key open-document version map and MUST
    be mutated in place (callers update it as they did_open / did_change docs).
    """
    key = _persistent_key(config, repo_root)
    with _PERSISTENT_LOCK:
        existing = _PERSISTENT_CLIENTS.get(key)
        if existing is not None and existing.is_alive():
            opened = _PERSISTENT_OPENED.setdefault(key, {})
            return existing, opened, True
        # Dead or absent: drop any stale handle and boot a fresh one.
        if existing is not None:
            try:
                existing.close()
            except Exception:  # noqa: BLE001 - best-effort teardown
                pass
            _PERSISTENT_CLIENTS.pop(key, None)
            _PERSISTENT_OPENED.pop(key, None)
        client = _JsonRpcLspClient(config)
        client.start()
        client.initialize(repo_root)
        _PERSISTENT_CLIENTS[key] = client
        opened = {}
        _PERSISTENT_OPENED[key] = opened
        return client, opened, False


def _discard_persistent_client(config: LspServerConfig, repo_root: Path) -> None:
    """Drop (and close) the cached client for a key, e.g. after a protocol
    error left it in an unknown state. Next acquire boots a fresh one."""
    key = _persistent_key(config, repo_root)
    with _PERSISTENT_LOCK:
        client = _PERSISTENT_CLIENTS.pop(key, None)
        _PERSISTENT_OPENED.pop(key, None)
    if client is not None:
        try:
            client.close()
        except Exception:  # noqa: BLE001 - best-effort teardown
            pass


def _close_persistent_clients() -> None:
    """Close every persistent client (atexit handler / test cleanup)."""
    with _PERSISTENT_LOCK:
        clients = list(_PERSISTENT_CLIENTS.values())
        _PERSISTENT_CLIENTS.clear()
        _PERSISTENT_OPENED.clear()
    for client in clients:
        try:
            client.close()
        except Exception:  # noqa: BLE001 - best-effort teardown
            pass


atexit.register(_close_persistent_clients)


def _first_location(result: Any) -> Mapping[str, Any] | None:
    if isinstance(result, list):
        if not result:
            return None
        first = result[0]
        return first if isinstance(first, Mapping) else None
    return result if isinstance(result, Mapping) else None


def _definition_request_concurrency(config: LspServerConfig) -> int:
    return max(1, int(config.definition_request_concurrency or 1))


def _inc(counts: dict[str, int], key: str) -> None:
    counts[key] = counts.get(key, 0) + 1


def _inc_path(counts: dict[str, dict[str, int]], path: str, key: str) -> None:
    path_counts = counts.setdefault(path, {})
    path_counts[key] = path_counts.get(key, 0) + 1


def lsp_definition_candidate_count(
    report: CodeSymbolReport,
    *,
    rel_path: str,
    workspace_paths: Sequence[str],
    language_id: str,
) -> int:
    """Count references worth sending to textDocument/definition."""
    workspace_modules = _workspace_modules(workspace_paths)
    return sum(
        1
        for ref in report.references
        if not _definition_candidate_skip_reason(
            report,
            ref,
            rel_path=rel_path,
            language_id=language_id,
            workspace_modules=workspace_modules,
        )
    )


def lsp_definition_filter_fingerprint(
    *,
    language_id: str,
    workspace_paths: Sequence[str],
) -> str:
    payload = {
        "language_id": language_id,
        "workspace_modules": sorted(_workspace_modules(workspace_paths)),
    }
    return _sha(json.dumps(payload, sort_keys=True), n=24)


def _definition_candidate_skip_reason(
    report: CodeSymbolReport,
    ref: Reference,
    *,
    rel_path: str,
    language_id: str,
    workspace_modules: set[str],
) -> str:
    if language_id != "python":
        return ""
    callee = str(ref.callee_name or "").strip()
    if not callee:
        return "empty_callee"
    root = callee.split(".", 1)[0]
    imported_roots = _python_imported_roots(report.imports, rel_path=rel_path)
    local_symbol_names = {sym.name for sym in report.symbols if sym.name}
    if "." not in callee and root in local_symbol_names:
        return "same_file_symbol"
    if (
        "." not in callee
        and root in _PYTHON_BUILTIN_CALLS
        and root not in imported_roots
        and root not in local_symbol_names
    ):
        return "python_builtin"
    imported_targets = imported_roots.get(root)
    if imported_targets and not any(
        _module_is_workspace(target, workspace_modules)
        for target in imported_targets
    ):
        return "external_import"
    return ""


def _python_imported_roots(
    imports: Sequence[Import],
    *,
    rel_path: str,
) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for item in imports:
        module = _resolve_python_import_module(item.module, rel_path=rel_path)
        if item.kind == "import":
            root = item.alias or module.split(".", 1)[0]
            target = module
        else:
            root = item.alias or item.name
            target = ".".join(part for part in (module, item.name) if part)
        if not root:
            continue
        out.setdefault(root, set()).add(target)
    return out


def _resolve_python_import_module(module: str, *, rel_path: str) -> str:
    if not module.startswith("."):
        return module
    level = len(module) - len(module.lstrip("."))
    tail = module[level:]
    path = Path(rel_path)
    package_parts = list(path.with_suffix("").parts[:-1])
    if path.name == "__init__.py":
        package_parts = list(path.parent.parts)
    keep = max(0, len(package_parts) - level + 1)
    parts = package_parts[:keep]
    if tail:
        parts.extend(part for part in tail.split(".") if part)
    return ".".join(parts)


def _workspace_modules(paths: Sequence[str]) -> set[str]:
    return {
        _module_qname(str(path).replace("\\", "/").lstrip("/"))
        for path in paths
        if str(path).endswith(".py")
    }


def _module_is_workspace(module: str, workspace_modules: set[str]) -> bool:
    if not module:
        return False
    return any(
        module == candidate
        or module.startswith(f"{candidate}.")
        or candidate.startswith(f"{module}.")
        for candidate in workspace_modules
    )


def _reference_position(
    text: str,
    ref: Reference,
    *,
    position_encoding: str,
) -> tuple[int, int]:
    line = max(0, ref.span_start - 1)
    lines = text.splitlines()
    line_text = lines[line] if line < len(lines) else ""
    start_col = int(getattr(ref, "start_col", 0) or 0)
    if start_col <= 0:
        start_col = _token_column(line_text, ref.callee_name)
    return line, _position_units(line_text, start_col, position_encoding)


def _token_column(line_text: str, callee_name: str) -> int:
    tokens = [
        part for part in (
            callee_name,
            callee_name.rsplit(".", 1)[-1],
        )
        if part
    ]
    for token in tokens:
        index = line_text.find(token)
        if index >= 0:
            return index
    stripped = line_text.lstrip()
    return len(line_text) - len(stripped)


def _position_units(line_text: str, column: int, encoding: str) -> int:
    column = max(0, min(column, len(line_text)))
    if encoding.lower() == "utf-16":
        return len(line_text[:column].encode("utf-16-le")) // 2
    return column


def _path_uri(path: Path) -> str:
    return path.resolve().as_uri()


def _uri_rel_path(uri: str, repo_root: Path) -> str | None:
    if not uri.startswith("file://"):
        return None
    from urllib.parse import unquote, urlparse

    parsed = urlparse(uri)
    path = Path(unquote(parsed.path)).resolve()
    try:
        return path.relative_to(repo_root).as_posix()
    except ValueError:
        return None
