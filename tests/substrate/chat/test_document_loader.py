"""PACT chat-document-loader — okg as Open WebUI's external document loader.

Hermetic, and adversarial about its own witnesses. Three construction
choices carry that weight:

**The vendor's own client makes the calls.** The contract arm does not
transcribe `ExternalDocumentLoader.load()`; it EXECS the byte-identical
copy of the pinned vendor file at
``tests/fixtures/chat_document_loader/vendor/external_document.py.txt``
(digest pinned in `VENDOR_CONTRACT`) with stand-ins for its two vendor
imports, and lets that code PUT at a real socket. A transcription would
pass forever after the vendor changed; this fails at the next pin bump,
loudly, at the digest assertion.

**Assertions read observable end state, never the request this test
built.** The request shape is asserted from what the SERVER received.
"nothing was staged" is asserted by listing the staging directory. The
pointer/inline difference is asserted from the file-row content the
vendor would store — modelled as
``' '.join(doc.page_content for doc in docs)``, measured at
`routers/retrieval.py:1963` — not from our own response object.

**The breadcrumb's node id is proven, not plausible.** A pointer whose
node id does not resolve is worse than no pointer. `test_breadcrumb_node_
id_matches_a_real_ingest_run` stages through the endpoint and then runs
`UploadSource` over that staging area, comparing the emitted document
node id to the one the breadcrumb printed.

What is NOT provable here, and is not claimed: that Open WebUI actually
CALLED the endpoint (a live instance and a recorded PUT /process are the
only evidence for that, per `req.chat-document-loader.loader-engaged`),
and that the vector store gained zero rows (that needs a real vector
backend). Both are live arms, driven by
``pact/changes/chat-document-loader/evidence/live_loader_arm.py``.
"""

from __future__ import annotations

import contextlib
import hashlib
import http.server
import json
import socket
import sys
import threading
import traceback
import types
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Iterator

import pytest
import yaml

from okg.substrate.chat.document_loader import (
    HEADER_FILENAME,
    HEADER_OKG_FILE_NAME,
    LOADER_PROCESS_PATH,
    MODE_INLINE,
    MODE_POINTER,
    VENDOR_CONTRACT,
    DocumentLoaderError,
    DocumentLoaderPlan,
    DocumentLoaderServer,
    LoaderRecorder,
    loader_response,
    split_vendor_filename,
)
from okg.substrate.chat.document_loader_projection import (
    API_KEY_FORM_KEY,
    BYPASS_FORM_KEY,
    ENGINE_EXTERNAL,
    ENGINE_FORM_KEY,
    LOADER_CONFIG_ROUTE,
    LOADER_CONFIG_UPDATE_ROUTE,
    RESIDUAL_VECTOR_WRITE_PATHS,
    URL_FORM_KEY,
    loader_projection_report,
    project_document_loader,
    unprojected_report,
    verify_loader_readback,
)
from okg.substrate.chat.sync import ChatSyncError, InstanceClient
from okg.substrate.library.sources.upload import (
    UploadSource,
    resolve_upload_registration,
    staged_document_node_id,
    upload_source_registry_entry,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEPLOYMENT = "loader-dep"
TOKEN_ENV = "OKG_TEST_CHAT_LOADER_TOKEN"
TOKEN = "s3cr3t-loader-token-not-a-real-one"

#: A fact stated nowhere else, so "the text reached X" is decidable by
#: substring rather than by shape.
DISTINGUISHING_FACT = "the calibration constant is 41.7 millifurlongs"
DOCUMENT_BYTES = (
    f"# Field notes\n\nOn the third run, {DISTINGUISHING_FACT}.\n"
).encode("utf-8")

#: The vendor stores an upload as ``<file id>_<original name>``
#: (`routers/files.py:358`) and sends the basename of that path.
VENDOR_FILE_ID = "3f2504e0-4f89-11d3-9a0c-0305e82c3301"
ORIGINAL_NAME = "field-notes.md"
VENDOR_STORED_NAME = f"{VENDOR_FILE_ID}_{ORIGINAL_NAME}"


# --------------------------------------------------------------- fixtures


def _posture() -> dict[str, Any]:
    return {
        "deployment_class": "dogfood_workspace",
        "read_principals": ["human_operator", "agent"],
        "write_principals": ["human_operator", "source_adapter"],
        "mcp_write_default": "review",
        "export_policy": "internal_only",
        "secret_handling": "secret_ref_required",
        "live_call_default": "review",
        "allowed_source_sensitivities": ["local_private"],
        "allowed_data_classifications": ["private"],
        "allowed_exportability": ["no_export"],
        "privacy_obligations": ["audit", "no_export"],
        "readiness_profiles": ["dogfood"],
    }


def _extractors_yaml() -> dict[str, Any]:
    """Minimal parser + chunker registration; no NER, no vision, no network."""
    return {
        "parser": [
            {
                "format": "markdown",
                "class": (
                    "okg.substrate.library.extraction.parse.markdown.MarkdownParser"
                ),
            },
            {
                "format": "text",
                "class": "okg.substrate.library.extraction.parse.text.TextParser",
            },
        ],
        "chunker": [
            {
                "class": "okg.substrate.library.extraction.chunk.window.WindowChunker",
                "config": {"max_chars": 200, "overlap": 0},
            }
        ],
    }


@pytest.fixture
def deployments_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Path]:
    """A deployment on disk registering a classified upload source.

    Deliberately the REAL registry, manifest, and Nomos audit: the loader
    is meant to be a second door onto that resolution, and a stubbed
    registration would make the "same front door" claim untestable.
    """
    root = tmp_path / "deployments"
    dep = root / DEPLOYMENT
    dep.mkdir(parents=True)
    sources = {
        "upload": upload_source_registry_entry(ownership_id=f"{DEPLOYMENT}.upload"),
    }
    (dep / "source_registry.yaml").write_text(
        yaml.safe_dump({"sources": sources}, sort_keys=True)
    )
    (dep / "deployment.yaml").write_text(
        yaml.safe_dump(
            {"name": DEPLOYMENT, "nomos": {"deployment_posture": _posture()}},
            sort_keys=True,
        )
    )
    (dep / "extractors.yaml").write_text(yaml.safe_dump(_extractors_yaml()))
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    monkeypatch.setenv(TOKEN_ENV, TOKEN)
    yield root


def _plan(mode: str = MODE_POINTER, **kwargs: Any) -> DocumentLoaderPlan:
    return DocumentLoaderPlan(
        deployment=DEPLOYMENT,
        base_url="http://127.0.0.1:0",
        token_env=TOKEN_ENV,
        mode=mode,
        **kwargs,
    )


@contextlib.contextmanager
def _serving(mode: str = MODE_POINTER, **kwargs: Any) -> Iterator[DocumentLoaderServer]:
    with DocumentLoaderServer(_plan(mode, **kwargs), port=0) as server:
        yield server


def _staging_dir(root: Path) -> Path:
    return root / DEPLOYMENT / "state" / "uploads"


def _staged_blobs(root: Path) -> list[str]:
    content = _staging_dir(root) / "content"
    if not content.is_dir():
        return []
    return sorted(p.name for p in content.iterdir() if p.is_file())


# ------------------------------------------------- the pinned vendor client


def _vendor_stub_modules() -> dict[str, types.ModuleType]:
    """Stand-ins for the two vendor imports the loader file makes.

    Faithful to the MEASURED behavior of each, not convenient:
    ``include_user_info_headers`` returns the headers unchanged for a
    ``None`` user, and ``parse_custom_headers`` returns ``{}`` for a
    falsy/non-dict header map and otherwise substitutes the same
    placeholders `utils/headers.py` substitutes.
    """

    class Document:
        def __init__(self, page_content: Any = None, metadata: Any = None) -> None:
            self.page_content = page_content
            self.metadata = metadata

    class BaseLoader:
        pass

    def include_user_info_headers(headers: dict, user: Any = None) -> dict:
        if user is None:
            return headers
        return {**headers, "X-OpenWebUI-User-Id": getattr(user, "id", "")}

    def parse_custom_headers(
        custom_headers: Any,
        user: Any = None,
        metadata: Any = None,
        request: Any = None,
        user_groups: Any = None,
    ) -> dict:
        if not custom_headers or not isinstance(custom_headers, dict):
            return {}
        meta = metadata or {}
        template_vars = {
            "{{FILE_ID}}": meta.get("file_id", "") or "",
            "{{FILE_NAME}}": meta.get("file_name", "") or "",
            "{{FILE_CONTENT_TYPE}}": meta.get("file_content_type", "") or "",
        }
        out = {}
        for key, value in custom_headers.items():
            text = value if isinstance(value, str) else str(value)
            for token, val in template_vars.items():
                text = text.replace(token, val)
            out[key] = text
        return out

    langchain = types.ModuleType("langchain_core")
    docs_mod = types.ModuleType("langchain_core.documents")
    docs_mod.Document = Document
    loaders_mod = types.ModuleType("langchain_core.document_loaders")
    loaders_mod.BaseLoader = BaseLoader
    langchain.documents = docs_mod
    langchain.document_loaders = loaders_mod

    open_webui = types.ModuleType("open_webui")
    utils_mod = types.ModuleType("open_webui.utils")
    headers_mod = types.ModuleType("open_webui.utils.headers")
    headers_mod.include_user_info_headers = include_user_info_headers
    headers_mod.parse_custom_headers = parse_custom_headers
    utils_mod.headers = headers_mod
    open_webui.utils = utils_mod

    return {
        "langchain_core": langchain,
        "langchain_core.documents": docs_mod,
        "langchain_core.document_loaders": loaders_mod,
        "open_webui": open_webui,
        "open_webui.utils": utils_mod,
        "open_webui.utils.headers": headers_mod,
    }


@contextlib.contextmanager
def _vendor_client_class() -> Iterator[type]:
    """The pinned vendor ``ExternalDocumentLoader``, executed as-is.

    ``sys.modules`` is restored key by key on exit. The inserted names are
    packages this repo does not install, so nothing else can hold a second
    copy of them while this is in scope.
    """
    path = REPO_ROOT / VENDOR_CONTRACT.client_source_relpath
    source = path.read_text(encoding="utf-8")
    stubs = _vendor_stub_modules()
    saved = {name: sys.modules.get(name) for name in stubs}
    sys.modules.update(stubs)
    try:
        namespace: dict[str, Any] = {
            "__name__": "okg_pinned_vendor_external_document",
            "__file__": str(path),
        }
        exec(compile(source, str(path), "exec"), namespace)  # noqa: S102
        yield namespace["ExternalDocumentLoader"]
    finally:
        for name, previous in saved.items():
            if previous is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = previous


def _vendor_load(
    base_url: str,
    *,
    file_path: Path,
    api_key: str = TOKEN,
    mime_type: str = "text/markdown",
    headers: Any = None,
    metadata: Any = None,
) -> list[Any]:
    """Run the pinned vendor client against ``base_url``. Returns Documents."""
    with _vendor_client_class() as loader_cls:
        loader = loader_cls(
            file_path=str(file_path),
            url=base_url,
            api_key=api_key,
            mime_type=mime_type,
            headers=headers,
            metadata=metadata or {},
        )
        return loader.load()


@pytest.fixture
def vendor_file(tmp_path: Path) -> Path:
    """The upload as the VENDOR has it on disk: ``<file id>_<name>``."""
    path = tmp_path / "uploads" / VENDOR_STORED_NAME
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(DOCUMENT_BYTES)
    return path


# ------------------------------------------------------- a stub JSON server


class _StubJSONHandler(http.server.BaseHTTPRequestHandler):
    """Returns a canned body, to exercise the VENDOR's response handling."""

    payload: Any = None
    raw_body: bytes | None = None
    status: int = 200
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args: Any) -> None:
        return

    def do_PUT(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length") or 0)
        if length:
            self.rfile.read(length)
        body = (
            self.raw_body
            if self.raw_body is not None
            else json.dumps(self.payload).encode("utf-8")
        )
        self.send_response(self.status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


@contextlib.contextmanager
def _stub_server(
    *, payload: Any = None, raw_body: bytes | None = None, status: int = 200,
) -> Iterator[str]:
    handler = type(
        "_BoundStub",
        (_StubJSONHandler,),
        {"payload": payload, "raw_body": raw_body, "status": status},
    )
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    server.daemon_threads = True
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


# ============================================================ the contract
# req.chat-document-loader.vendor-contract


def test_pinned_vendor_client_copy_matches_its_recorded_digest() -> None:
    """The copy the contract arms drive IS the pinned vendor file.

    Without this, the whole contract suite could be passing against a
    hand-edited convenience copy — a false witness that would survive
    every other assertion in this file.
    """
    path = REPO_ROOT / VENDOR_CONTRACT.client_source_relpath
    assert path.is_file(), f"missing pinned vendor client at {path}"
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    assert digest == VENDOR_CONTRACT.client_source_sha256, (
        f"{VENDOR_CONTRACT.client_source_relpath} does not match the digest "
        f"recorded in VENDOR_CONTRACT. Re-extract it from the pinned image "
        f"(see the README beside it) and update client_source_sha256 only "
        f"after re-reading the contract."
    )


def test_vendor_client_request_shape_is_what_the_endpoint_receives(
    deployments_root: Path, vendor_file: Path,
) -> None:
    """Method, path, body, and all three headers — read off the SERVER.

    Every field is asserted from what arrived, so a test that constructed
    the request itself cannot be what makes it pass.
    """
    received: dict[str, Any] = {}
    with _serving() as server:
        original = server.recorder.record

        def _capture(entry: Any) -> Any:
            received["entry"] = entry
            return original(entry)

        server.recorder.record = _capture  # type: ignore[method-assign]
        documents = _vendor_load(server.base_url, file_path=vendor_file)

    entry = received["entry"]
    assert entry.method == VENDOR_CONTRACT.method == "PUT"
    assert entry.path == LOADER_PROCESS_PATH
    assert entry.byte_count == len(DOCUMENT_BYTES)
    assert entry.content_type == "text/markdown"
    assert entry.authorization_present is True
    # The vendor URL-quotes the basename of its own storage path.
    assert entry.x_filename == urllib.parse.quote(VENDOR_STORED_NAME)
    assert entry.filename == ORIGINAL_NAME
    assert entry.filename_source == "x-filename:vendor_stored_name"
    assert entry.vendor_file_id == VENDOR_FILE_ID
    assert entry.status == 200
    # And the vendor accepted the response without raising.
    assert len(documents) == 1
    assert isinstance(documents[0].page_content, str)


def test_endpoint_stages_the_exact_bytes_under_the_upload_source(
    deployments_root: Path, vendor_file: Path,
) -> None:
    """The bytes reach chat-upload-source's staging area, unchanged."""
    with _serving() as server:
        _vendor_load(server.base_url, file_path=vendor_file)

    blobs = _staged_blobs(deployments_root)
    assert len(blobs) == 1, blobs
    staged_path = _staging_dir(deployments_root) / "content" / blobs[0]
    assert staged_path.read_bytes() == DOCUMENT_BYTES
    # Named for the operator's file, not the vendor's storage name.
    assert blobs[0].endswith(ORIGINAL_NAME)
    assert VENDOR_FILE_ID not in blobs[0]


def test_vendor_client_accepts_a_list_of_documents() -> None:
    """The second accepted response shape, proven against the vendor code."""
    payload = [
        {"page_content": "first", "metadata": {"n": 1}},
        {"page_content": "second", "metadata": {"n": 2}},
    ]
    with _stub_server(payload=payload) as base_url:
        with_tmp = Path(__file__)
        documents = _vendor_load(base_url, file_path=with_tmp)
    assert [d.page_content for d in documents] == ["first", "second"]
    assert "list_of_objects" in VENDOR_CONTRACT.accepted_response_shapes


def test_vendor_client_raises_on_an_empty_body() -> None:
    """Why the endpoint may never answer 200 with nothing.

    The vendor treats a falsy decoded body as an ERROR, so a 200 with no
    content is not a graceful no-op — it surfaces to the user as a failed
    upload with no diagnosis.
    """
    with _stub_server(raw_body=b"null") as base_url:
        with pytest.raises(Exception, match="No content returned"):
            _vendor_load(base_url, file_path=Path(__file__))


def test_vendor_client_raises_on_a_shape_it_cannot_parse() -> None:
    with _stub_server(payload="just a string") as base_url:
        with pytest.raises(Exception, match="Unable to parse content"):
            _vendor_load(base_url, file_path=Path(__file__))


def test_vendor_client_raises_on_a_refusal_and_surfaces_the_reason(
    deployments_root: Path, vendor_file: Path,
) -> None:
    """A refusal reaches the USER instead of falling back to local extraction.

    This is the arm that makes the endpoint's non-2xx refusals meaningful:
    the vendor raises with our status and body, so an upload okg would not
    govern fails visibly rather than quietly entering the vendor's corpus.
    """
    with _serving() as server:
        with pytest.raises(Exception, match="Error loading document: 401"):
            _vendor_load(server.base_url, file_path=vendor_file, api_key="wrong")
    assert _staged_blobs(deployments_root) == []


def test_process_url_matches_the_vendor_normalization() -> None:
    """One trailing slash stripped, then ``/process`` — the vendor's own rule."""
    assert VENDOR_CONTRACT.process_url("http://host:8791") == "http://host:8791/process"
    assert VENDOR_CONTRACT.process_url("http://host:8791/") == "http://host:8791/process"


def test_loader_response_refuses_to_return_an_empty_document() -> None:
    """A 200 with empty page_content is refused at the constructor.

    Load-bearing: the vendor joins page_content onto the file row
    unconditionally (`retrieval.py:1963`), so an empty string would blank
    the row rather than fail.
    """
    with pytest.raises(DocumentLoaderError, match="empty page_content"):
        loader_response(page_content="   ", metadata={})
    with pytest.raises(DocumentLoaderError, match="empty page_content"):
        loader_response(page_content="", metadata={})


def test_split_vendor_filename_reports_which_rule_fired() -> None:
    """Recovering the operator's name and using the stored name differ.

    They publish different citations, so which one happened is returned
    rather than being invisible.
    """
    assert split_vendor_filename(urllib.parse.quote(VENDOR_STORED_NAME)) == (
        VENDOR_FILE_ID, ORIGINAL_NAME, "vendor_stored_name",
    )
    assert split_vendor_filename("plain%20notes.md") == (
        "", "plain notes.md", "verbatim",
    )
    # Not a UUID prefix: kept whole rather than split on the first '_'.
    assert split_vendor_filename("2026_report.md") == (
        "", "2026_report.md", "verbatim",
    )


# ======================================================= endpoint refusals


@pytest.mark.parametrize(
    ("api_key", "expected"),
    [("wrong-token", 401), ("", 401)],
    ids=["wrong_token", "empty_token"],
)
def test_a_bad_token_stages_nothing(
    deployments_root: Path, vendor_file: Path, api_key: str, expected: int,
) -> None:
    with _serving() as server:
        with pytest.raises(Exception):
            _vendor_load(server.base_url, file_path=vendor_file, api_key=api_key)
        assert [r.status for r in server.records] == [expected]
        assert [r.outcome for r in server.records] == ["unauthorized"]
    assert _staged_blobs(deployments_root) == []


def _raw_put(
    base_url: str, *, body: bytes, headers: dict[str, str],
) -> tuple[int, dict[str, Any]]:
    request = urllib.request.Request(
        f"{base_url}{LOADER_PROCESS_PATH}", data=body, method="PUT", headers=headers,
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def test_an_empty_body_is_refused_not_answered_200(
    deployments_root: Path,
) -> None:
    with _serving() as server:
        status, payload = _raw_put(
            server.base_url,
            body=b"",
            headers={
                "Authorization": f"Bearer {TOKEN}",
                HEADER_FILENAME: urllib.parse.quote(VENDOR_STORED_NAME),
            },
        )
    assert status == 400
    assert payload["status"] == "empty_body"
    assert _staged_blobs(deployments_root) == []


def test_a_missing_filename_header_is_refused(deployments_root: Path) -> None:
    with _serving() as server:
        status, payload = _raw_put(
            server.base_url,
            body=DOCUMENT_BYTES,
            headers={"Authorization": f"Bearer {TOKEN}"},
        )
    assert status == 400
    assert payload["status"] == "missing_filename"
    assert _staged_blobs(deployments_root) == []


def test_a_deployment_with_no_upload_source_is_refused_with_a_non_2xx(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, vendor_file: Path,
) -> None:
    """No registration means no loader — never a quiet local-extraction path."""
    root = tmp_path / "deployments"
    (root / DEPLOYMENT).mkdir(parents=True)
    (root / DEPLOYMENT / "source_registry.yaml").write_text(
        yaml.safe_dump({"sources": {}}, sort_keys=True)
    )
    monkeypatch.setenv("OKG_DEPLOYMENTS_DIR", str(root))
    monkeypatch.setenv(TOKEN_ENV, TOKEN)
    with _serving() as server:
        status, payload = _raw_put(
            server.base_url,
            body=DOCUMENT_BYTES,
            headers={
                "Authorization": f"Bearer {TOKEN}",
                HEADER_FILENAME: urllib.parse.quote(VENDOR_STORED_NAME),
            },
        )
        assert [r.outcome for r in server.records] == ["upload_source_not_registered"]
    assert status == 503
    assert payload["status"] == "upload_source_not_registered"


def test_an_unset_token_variable_refuses_to_start_the_endpoint(
    deployments_root: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An endpoint with no secret would be a public ingest door."""
    monkeypatch.delenv(TOKEN_ENV, raising=False)
    with pytest.raises(DocumentLoaderError, match=TOKEN_ENV):
        DocumentLoaderServer(_plan(), port=0)


def test_a_literal_secret_in_place_of_a_variable_name_is_refused() -> None:
    with pytest.raises(DocumentLoaderError, match="ENVIRONMENT"):
        DocumentLoaderPlan(
            deployment=DEPLOYMENT,
            base_url="http://127.0.0.1:1",
            token_env="sk-live-abcdef123456",
        )


def test_an_undeclared_mode_is_refused_rather_than_defaulted() -> None:
    with pytest.raises(DocumentLoaderError, match="loader mode"):
        DocumentLoaderPlan(
            deployment=DEPLOYMENT,
            base_url="http://127.0.0.1:1",
            token_env=TOKEN_ENV,
            mode="passthrough",
        )


# ================================================ pointer vs inline content
# req.chat-document-loader.governed-not-duplicated (b)


def _vendor_file_row_content(documents: list[Any]) -> str:
    """What `process_file` writes onto the file row, modelled exactly.

    `routers/retrieval.py:1963` computes
    ``text_content = ' '.join([doc.page_content for doc in docs])`` and
    :1965 stores it with ``Files.update_file_data_by_id``, BEFORE the
    bypass branch at :1972 — so this value survives the bypass and is
    what `retrieval/utils.py:1385` reads back.
    """
    return " ".join(doc.page_content for doc in documents)


def test_pointer_is_the_default_and_leaves_only_a_breadcrumb(
    deployments_root: Path, vendor_file: Path,
) -> None:
    """The default mode gives the vendor a pointer, and the graph the text."""
    with _serving() as server:
        documents = _vendor_load(server.base_url, file_path=vendor_file)

    row = _vendor_file_row_content(documents)
    assert DISTINGUISHING_FACT not in row, (
        "pointer mode left the document's text on the vendor's file row"
    )
    assert "node_id: doc:upload:" in row
    assert "`search`" in row
    assert ORIGINAL_NAME in row
    assert "not yet published" in row
    # ... and the text is in staging, on its way to the graph.
    staged = _staging_dir(deployments_root) / "content"
    blob = next(iter(staged.iterdir()))
    assert DISTINGUISHING_FACT in blob.read_text(encoding="utf-8")


def test_inline_is_declared_and_returns_the_full_text(
    deployments_root: Path, vendor_file: Path,
) -> None:
    """The same suite as the pointer arm, so the default is provably not inline."""
    with _serving(MODE_INLINE) as server:
        documents = _vendor_load(server.base_url, file_path=vendor_file)

    row = _vendor_file_row_content(documents)
    assert DISTINGUISHING_FACT in row
    assert "node_id:" not in row
    assert documents[0].metadata["okg_mode"] == MODE_INLINE


def test_the_sync_report_states_that_inline_keeps_a_vendor_side_copy() -> None:
    """Declared, never inferred: the report says the copy exists."""
    inline = loader_projection_report(
        _plan(MODE_INLINE), {"ok": True, "verified": [], "problems": []},
    )
    pointer = loader_projection_report(
        _plan(MODE_POINTER), {"ok": True, "verified": [], "problems": []},
    )
    assert inline["vendor_side_copy"] is True
    assert "vendor-side copy" in inline["mode_summary"]
    assert pointer["vendor_side_copy"] is False
    assert "breadcrumb" in pointer["mode_summary"]


def test_breadcrumb_node_id_matches_a_real_ingest_run(
    deployments_root: Path, vendor_file: Path,
) -> None:
    """The pointer names the node the ingest actually publishes.

    A breadcrumb whose node id does not resolve reads as a working
    pointer and returns nothing — so the id is compared against what
    `UploadSource.run` EMITS for the same staged file, not against a
    second copy of the formula.
    """
    with _serving() as server:
        documents = _vendor_load(server.base_url, file_path=vendor_file)
    claimed = documents[0].metadata["okg_node_id"]
    assert claimed in documents[0].page_content

    dep_dir = deployments_root / DEPLOYMENT
    registration = resolve_upload_registration(
        DEPLOYMENT,
        registry_path=dep_dir / "source_registry.yaml",
        manifest=yaml.safe_load((dep_dir / "deployment.yaml").read_text()),
    )
    source = UploadSource(
        staging_dir=registration.staging_dir,
        extractors_dir=registration.extractors_dir,
        repo=registration.repo,
        source_name=registration.ownership_id,
    )
    emitted = [
        fact
        for fact in source.run("run-1", mode="reconcile").facts
        if getattr(fact, "subtype", "") == "document"
    ]
    assert len(emitted) == 1, "the staged upload did not produce one document"
    assert emitted[0].node_id == claimed
    # And the helper the breadcrumb uses agrees with both.
    staged_name = next(iter(_staged_blobs(deployments_root)))
    assert staged_document_node_id(registration.repo, staged_name) == claimed


# ================================================== the three-key projection
# req.chat-document-loader.loader-engaged


class _FakeRetrievalInstance:
    """Open WebUI v0.11.0's two retrieval-config routes, as measured.

    * ``GET /api/v1/retrieval/config`` returns the flat form-key shape at
      `retrieval.py:615` (engine :633, bypass :624, loader keys :648-649).
    * ``POST /api/v1/retrieval/config/update`` applies
      ``form_data.X if form_data.X is not None else config.X``
      (`retrieval.py:938` onwards) — so an unsent key keeps its value.
    * The update route does NOT lower-case the engine (`:974-977`),
      unlike the env read at `config.py:854`. Modelled, because that
      asymmetry is exactly how a written-and-read-back engine can still
      fail the gate's ``== 'external'``.

    Fault injection is narrow and each arm is a failure the requirement
    names: ``ignore_writes`` reproduces a key the vendor accepts and does
    not store; ``omit_keys`` reproduces a build whose config route does
    not expose a key at all.
    """

    def __init__(
        self,
        *,
        engine: str = "",
        ignore_writes: frozenset[str] = frozenset(),
        omit_keys: frozenset[str] = frozenset(),
    ) -> None:
        self.config: dict[str, Any] = {
            ENGINE_FORM_KEY: engine,
            URL_FORM_KEY: "",
            API_KEY_FORM_KEY: "",
            BYPASS_FORM_KEY: False,
            "TOP_K": 3,
        }
        self.ignore_writes = ignore_writes
        self.omit_keys = omit_keys
        self.calls: list[tuple[str, str]] = []
        self.posted: list[dict[str, Any]] = []

    def opener(self, request: Any, timeout: float) -> tuple[int, str]:
        method = request.get_method()
        path = urllib.parse.urlparse(request.full_url).path
        self.calls.append((method, path))
        if path == LOADER_CONFIG_ROUTE and method == "GET":
            visible = {
                key: value
                for key, value in self.config.items()
                if key not in self.omit_keys
            }
            return 200, json.dumps({"status": True, **visible})
        if path == LOADER_CONFIG_UPDATE_ROUTE and method == "POST":
            payload = json.loads(request.data.decode("utf-8"))
            self.posted.append(payload)
            for key, value in payload.items():
                if value is None or key in self.ignore_writes:
                    continue
                self.config[key] = value
            return 200, json.dumps({"status": True, **self.config})
        return 404, json.dumps({"detail": f"no route for {method} {path}"})

    def client(self) -> InstanceClient:
        return InstanceClient("http://instance.invalid", "admin-token", opener=self.opener)


def test_all_three_gate_conditions_are_driven_and_read_back(
    deployments_root: Path,
) -> None:
    instance = _FakeRetrievalInstance()
    plan = _plan()
    report = project_document_loader(instance.client(), plan, token=TOKEN)

    assert report["ok"] is True
    gate_keys = {
        entry["form_key"] for entry in report["verified"] if entry["gate_condition"]
    }
    assert gate_keys == {ENGINE_FORM_KEY, URL_FORM_KEY, API_KEY_FORM_KEY}
    # Read back from the instance's OWN config, not echoed from the body.
    assert instance.config[ENGINE_FORM_KEY] == ENGINE_EXTERNAL
    assert instance.config[URL_FORM_KEY] == plan.base_url
    assert instance.config[BYPASS_FORM_KEY] is True
    assert ("GET", LOADER_CONFIG_ROUTE) in instance.calls
    # An unrelated retrieval setting is untouched by the narrow body.
    assert instance.config["TOP_K"] == 3


def test_an_engine_that_will_not_stick_refuses_and_names_what_it_read(
    deployments_root: Path,
) -> None:
    """The silent-failure arm this PACT exists for.

    The instance accepts the engine write and stores nothing — the shape
    `/api/v1/models/sync` already has at this version. Two of three gate
    conditions would then be configured, the hook would never fire, and
    every upload would enter the vendor's own corpus.
    """
    instance = _FakeRetrievalInstance(ignore_writes=frozenset({ENGINE_FORM_KEY}))
    with pytest.raises(ChatSyncError) as excinfo:
        project_document_loader(instance.client(), _plan(), token=TOKEN)

    assert "read back as ''" in str(excinfo.value)
    data = excinfo.value.data
    assert data["reason"] == "loader_gate_not_verified"
    assert data["engine_read"] == ""
    # ... and the URL and API key are NOT left configured behind a local
    # extraction path.
    assert instance.config[URL_FORM_KEY] == ""
    assert instance.config[API_KEY_FORM_KEY] == ""
    assert data["rolled_back"]["confirmed"] is True


def test_an_engine_reading_back_as_another_extractor_is_refused_by_name(
    deployments_root: Path,
) -> None:
    instance = _FakeRetrievalInstance(
        engine="tika", ignore_writes=frozenset({ENGINE_FORM_KEY}),
    )
    with pytest.raises(ChatSyncError, match="'tika'"):
        project_document_loader(instance.client(), _plan(), token=TOKEN)
    assert instance.config[URL_FORM_KEY] == ""


def test_a_loader_url_that_will_not_stick_refuses_and_rolls_back(
    deployments_root: Path,
) -> None:
    """The URL arm of the same silent failure the engine arm covers.

    The instance accepts the URL write and stores nothing. Engine and API
    key are then configured against a URL that is not ours, which is a
    live external-extraction hook pointed somewhere else -- worse than an
    unconfigured instance, and green on every check but this one.
    """
    instance = _FakeRetrievalInstance(ignore_writes=frozenset({URL_FORM_KEY}))
    with pytest.raises(ChatSyncError) as excinfo:
        project_document_loader(instance.client(), _plan(), token=TOKEN)

    assert f"{URL_FORM_KEY} read back as ''" in str(excinfo.value)
    problems = {p["form_key"] for p in excinfo.value.data["problems"]}
    assert problems == {URL_FORM_KEY}
    # ... and the engine is not left on 'external' pointing at nothing.
    assert instance.config[ENGINE_FORM_KEY] == ""
    assert instance.config[API_KEY_FORM_KEY] == ""
    assert excinfo.value.data["rolled_back"]["confirmed"] is True


def test_a_bypass_that_will_not_stick_refuses_and_names_the_bypass(
    deployments_root: Path,
) -> None:
    """A refusal must name what FAILED, not what happened to be checked first.

    Before this arm the message described the engine and only the engine,
    so a run whose engine was fine and whose bypass did not stick refused
    with "the extraction engine read back as 'external', not 'external'"
    -- naming the one condition that worked.
    """
    instance = _FakeRetrievalInstance(ignore_writes=frozenset({BYPASS_FORM_KEY}))
    with pytest.raises(ChatSyncError) as excinfo:
        project_document_loader(instance.client(), _plan(), token=TOKEN)

    message = str(excinfo.value)
    assert f"{BYPASS_FORM_KEY} read back as False" in message
    assert "extraction engine read back as 'external'" not in message
    problems = {p["form_key"] for p in excinfo.value.data["problems"]}
    assert problems == {BYPASS_FORM_KEY}
    # The loader keys are not left configured while the vendor is still
    # embedding every upload into its own store.
    assert instance.config[URL_FORM_KEY] == ""
    assert instance.config[ENGINE_FORM_KEY] == ""
    assert excinfo.value.data["rolled_back"]["confirmed"] is True


def test_a_config_route_without_the_engine_key_refuses_before_writing(
    deployments_root: Path,
) -> None:
    """Nothing is written to an instance that cannot satisfy the gate."""
    instance = _FakeRetrievalInstance(omit_keys=frozenset({ENGINE_FORM_KEY}))
    with pytest.raises(ChatSyncError) as excinfo:
        project_document_loader(instance.client(), _plan(), token=TOKEN)
    assert excinfo.value.data["reason"] == "engine_key_absent"
    assert instance.posted == []
    assert instance.config[URL_FORM_KEY] == ""


def test_a_case_variant_engine_is_not_accepted() -> None:
    """``External`` round-trips and never matches the gate's ``== 'external'``.

    The update route does not normalize (`retrieval.py:974-977`); only the
    env read does (`config.py:854`). An equality check that lower-cased
    the readback would certify a dead hook.
    """
    verdict = verify_loader_readback(
        {
            ENGINE_FORM_KEY: "External",
            URL_FORM_KEY: "http://127.0.0.1:0",
            API_KEY_FORM_KEY: TOKEN,
            BYPASS_FORM_KEY: True,
        },
        _plan(),
        TOKEN,
    )
    assert verdict["ok"] is False
    assert [p["form_key"] for p in verdict["problems"]] == [ENGINE_FORM_KEY]
    assert verdict["problems"][0]["read"] == "External"


def _matching_readback(**deviations: Any) -> dict[str, Any]:
    """A readback that verifies, with named single-key deviations.

    Every refusal arm below differs from the passing control by exactly
    ONE field, so what makes each of them refuse is the field named in
    the deviation and nothing else. Built because two of these three
    readback checks were VACUOUS: replacing the URL predicate or the
    bypass predicate with ``True`` left the whole suite green, which
    means neither key was being verified by anything.
    """
    readback: dict[str, Any] = {
        ENGINE_FORM_KEY: ENGINE_EXTERNAL,
        URL_FORM_KEY: "http://127.0.0.1:0",
        API_KEY_FORM_KEY: TOKEN,
        BYPASS_FORM_KEY: True,
    }
    readback.update(deviations)
    return readback


def test_a_readback_matching_the_plan_verifies_every_key() -> None:
    """The positive control the single-key refusal arms are measured against."""
    verdict = verify_loader_readback(_matching_readback(), _plan(), TOKEN)
    assert verdict["ok"] is True
    assert verdict["problems"] == []
    assert {entry["form_key"] for entry in verdict["verified"]} == {
        ENGINE_FORM_KEY, URL_FORM_KEY, API_KEY_FORM_KEY, BYPASS_FORM_KEY,
    }


def test_a_loader_url_reading_back_as_another_host_is_refused_by_name() -> None:
    """The URL is a gate condition, and a stale one is not a partial success.

    Engine and key green with the URL pointing somewhere else is a live
    hook aimed at an endpoint okg does not serve: every uploaded document
    goes to that host, and the vendor raises on whatever it answers.
    """
    other = "http://someone-elses-loader.invalid:8791"
    verdict = verify_loader_readback(
        _matching_readback(**{URL_FORM_KEY: other}), _plan(), TOKEN,
    )
    assert verdict["ok"] is False
    assert [p["form_key"] for p in verdict["problems"]] == [URL_FORM_KEY]
    assert verdict["problems"][0]["read"] == other
    assert verdict["problems"][0]["gate_condition"] is True


def test_a_bypass_reading_back_as_anything_but_true_is_refused_by_name() -> None:
    """The bypass is what the whole "no vendor corpus" control rests on.

    A bypass that did not stick leaves the vendor embedding every chat
    upload into its own store while the three gate keys read back green
    -- the measured-zero-rows claim would then be false and nothing else
    in the projection would notice.
    """
    verdict = verify_loader_readback(
        _matching_readback(**{BYPASS_FORM_KEY: False}), _plan(), TOKEN,
    )
    assert verdict["ok"] is False
    assert [p["form_key"] for p in verdict["problems"]] == [BYPASS_FORM_KEY]
    assert verdict["problems"][0]["read"] is False
    # A truthy string is not a projected bypass either: the vendor reads
    # this key as a bool, and "true" is what a coerced write looks like.
    coerced = verify_loader_readback(
        _matching_readback(**{BYPASS_FORM_KEY: "true"}), _plan(), TOKEN,
    )
    assert coerced["ok"] is False
    assert [p["form_key"] for p in coerced["problems"]] == [BYPASS_FORM_KEY]


def test_a_mismatched_api_key_is_a_problem_that_never_prints_the_secret() -> None:
    verdict = verify_loader_readback(
        {
            ENGINE_FORM_KEY: ENGINE_EXTERNAL,
            URL_FORM_KEY: "http://127.0.0.1:0",
            API_KEY_FORM_KEY: "some-other-token",
            BYPASS_FORM_KEY: True,
        },
        _plan(),
        TOKEN,
    )
    assert verdict["ok"] is False
    problem = next(p for p in verdict["problems"] if p["form_key"] == API_KEY_FORM_KEY)
    assert problem["read"] == "<does not match>"
    assert TOKEN not in json.dumps(verdict)
    assert "some-other-token" not in json.dumps(verdict)


def test_a_deployment_declaring_no_loader_leaves_the_vendor_default_alone() -> None:
    instance = _FakeRetrievalInstance()
    report = unprojected_report(DEPLOYMENT)
    assert report["declared"] is False
    assert instance.calls == []
    assert instance.config[ENGINE_FORM_KEY] == ""


def test_the_projection_report_carries_no_secret(deployments_root: Path) -> None:
    instance = _FakeRetrievalInstance()
    report = project_document_loader(instance.client(), _plan(), token=TOKEN)
    blob = json.dumps(report)
    assert TOKEN not in blob
    assert report["token_env"] == TOKEN_ENV


# ------------------------------------- the token in an HTTP error envelope

#: SHORT on purpose. ``sync.redact`` scrubs the ADMIN token by identity
#: plus any run of 20+ credential-shaped characters; an operator-chosen
#: loader token of twelve characters is under that floor, which is the
#: hole this arm measures. A long token would pass for the wrong reason.
SHORT_LOADER_TOKEN = "loader-pw-42"


class _EchoingRejectInstance:
    """An instance whose 422 echoes the SUBMITTED value back.

    The update route (`retrieval.py:938`) takes a pydantic form model,
    and a field-level validation failure renders ``detail[].input`` --
    the value that was sent -- into the response body. The loader token
    is a body field on that route, so this is the channel by which a
    rejected update prints it. The GET arm answers normally so the
    projection reaches the write.
    """

    def __init__(self) -> None:
        self.error_bodies: list[str] = []

    def opener(self, request: Any, timeout: float) -> tuple[int, str]:
        path = urllib.parse.urlparse(request.full_url).path
        if path == LOADER_CONFIG_ROUTE and request.get_method() == "GET":
            return 200, json.dumps({
                "status": True,
                ENGINE_FORM_KEY: "",
                URL_FORM_KEY: "",
                API_KEY_FORM_KEY: "",
                BYPASS_FORM_KEY: False,
            })
        submitted = json.loads((request.data or b"{}").decode("utf-8"))
        body = json.dumps({
            "detail": [{
                "type": "string_type",
                "loc": ["body", API_KEY_FORM_KEY],
                "msg": "Input should be a valid string",
                "input": submitted.get(API_KEY_FORM_KEY),
            }]
        })
        self.error_bodies.append(body)
        return 422, body


class _SecretsAwareClient(InstanceClient):
    """The shipped transport, recording what each call DECLARED.

    ``InstanceClient.call`` takes ``secrets`` (chat/sync.py), and the
    keyword is forwarded rather than dropped, so this observes the call
    sites without weakening what it observes.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.declared: list[tuple[str, str, tuple[str, ...]]] = []

    def call(
        self,
        method: str,
        path: str,
        payload: Any = None,
        *,
        secrets: tuple[str, ...] = (),
    ) -> Any:
        self.declared.append((method, path, tuple(secrets)))
        return super().call(method, path, payload, secrets=secrets)


class _UnredactingClient:
    """A transport that puts the raw error body in the envelope, unscrubbed.

    Not a straw man: this is what ``InstanceClient.call`` did before the
    sync-side redaction fix, and what any other transport this projection
    is handed may still do. The projection may not depend on its caller's
    transport to keep the loader token out of an error IT raises, so this
    arm holds it to the property on its own. It also has no ``secrets``
    parameter, which is the branch that warns rather than passing over a
    transport that cannot take one.
    """

    def __init__(self, instance: Any) -> None:
        self.instance = instance

    def call(self, method: str, path: str, payload: Any = None) -> Any:
        request = urllib.request.Request(
            f"http://instance.invalid{path}",
            data=None if payload is None else json.dumps(payload).encode("utf-8"),
            method=method,
        )
        status, body = self.instance.opener(request, 30.0)
        if status >= 400:
            raise ChatSyncError(
                f"{method} {path} failed with HTTP {status}",
                data={
                    "method": method,
                    "path": path,
                    "http_status": status,
                    "detail": body,
                    "reason": "http_error",
                },
                fix_hint="see the instance log",
            )
        return json.loads(body) if body else None


def _shipped_client(instance: Any) -> Any:
    return InstanceClient("http://instance.invalid", "admin-token", opener=instance.opener)


def _observing_client(instance: Any) -> Any:
    return _SecretsAwareClient(
        "http://instance.invalid", "admin-token", opener=instance.opener,
    )


def _unredacting_client(instance: Any) -> Any:
    return _UnredactingClient(instance)


@pytest.mark.parametrize(
    "make_client",
    [_shipped_client, _observing_client, _unredacting_client],
    ids=["shipped_transport", "declaring_transport", "transport_that_redacts_nothing"],
)
def test_a_rejected_update_keeps_the_loader_token_out_of_the_error_envelope(
    make_client: Any,
) -> None:
    """A 422 that echoes the token may not print it, on ANY transport.

    Three arms, because the property has two independent owners: the
    transport scrubs what it renders into ``data['detail']``, and the
    projection scrubs any error it re-raises. The third arm hands the
    projection a transport that scrubs NOTHING, so what keeps the
    envelope clean there is the projection alone.

    Both directions are asserted, because "the token is absent" passes
    trivially against an envelope that carried nothing: the echoed body
    is checked to CONTAIN the token within the first 200 characters (past
    any transport truncation), and the envelope is checked to still name
    the route and the status, with a ``<redacted>`` where the token was.
    """
    instance = _EchoingRejectInstance()
    with pytest.raises(ChatSyncError) as excinfo:
        project_document_loader(
            make_client(instance), _plan(), token=SHORT_LOADER_TOKEN,
        )

    # The leak channel is real: the instance did echo the token back.
    assert instance.error_bodies, "the update route was never called"
    assert SHORT_LOADER_TOKEN in instance.error_bodies[0][:200]

    error = excinfo.value
    envelope = json.dumps(error.to_json())
    assert SHORT_LOADER_TOKEN not in envelope
    assert "<redacted>" in envelope, "the token was not replaced, just missing"
    # The diagnosis survived the scrub.
    assert error.data["http_status"] == 422
    assert error.data["path"] == LOADER_CONFIG_UPDATE_ROUTE
    # And no chained exception can print what the envelope no longer holds.
    assert error.__cause__ is None
    assert error.__suppress_context__ is True
    rendered = "".join(
        traceback.format_exception(type(error), error, error.__traceback__)
    )
    assert SHORT_LOADER_TOKEN not in rendered


def test_every_projection_call_declares_the_loader_token_to_the_transport(
    deployments_root: Path,
) -> None:
    """The call-site half of the ``secrets=`` handoff to chat/sync.py.

    ``InstanceClient.call`` does not take ``secrets`` yet -- that fix is
    another change's -- so this stands in a transport that does and
    asserts every call from this module declares the loader token,
    including the readbacks: that route RETURNS the key, so an error body
    on the GET would carry it too.
    """
    instance = _FakeRetrievalInstance()
    client = _SecretsAwareClient(
        "http://instance.invalid", "admin-token", opener=instance.opener,
    )
    project_document_loader(client, _plan(), token=SHORT_LOADER_TOKEN)

    assert client.declared, "the projection made no calls"
    assert [entry[1] for entry in client.declared].count(
        LOADER_CONFIG_UPDATE_ROUTE
    ) == 1
    assert all(SHORT_LOADER_TOKEN in entry[2] for entry in client.declared), (
        [entry[:2] for entry in client.declared if SHORT_LOADER_TOKEN not in entry[2]]
    )


def test_a_rollback_declares_the_instance_s_previous_key_as_well(
    deployments_root: Path,
) -> None:
    """The restore body carries a SECOND secret: the key that was there.

    Rolling back writes the instance's previous API key back to the same
    route, so a 422 on the rollback would echo that one instead.
    """
    instance = _FakeRetrievalInstance(ignore_writes=frozenset({ENGINE_FORM_KEY}))
    instance.config[API_KEY_FORM_KEY] = "old-pw-7"
    client = _SecretsAwareClient(
        "http://instance.invalid", "admin-token", opener=instance.opener,
    )
    with pytest.raises(ChatSyncError):
        project_document_loader(client, _plan(), token=SHORT_LOADER_TOKEN)

    updates = [
        entry for entry in client.declared if entry[1] == LOADER_CONFIG_UPDATE_ROUTE
    ]
    assert len(updates) == 2, "expected the write and its rollback"
    assert updates[1][2] == (SHORT_LOADER_TOKEN, "old-pw-7")


def test_the_handler_record_never_holds_the_authorization_value(
    deployments_root: Path, vendor_file: Path,
) -> None:
    with _serving() as server:
        _vendor_load(server.base_url, file_path=vendor_file)
        blob = json.dumps([r.to_json() for r in server.records])
    assert TOKEN not in blob
    assert json.loads(blob)[0]["authorization_present"] is True


def test_a_readback_report_says_it_is_not_proof_the_hook_fired(
    deployments_root: Path,
) -> None:
    """The claim a green projection would otherwise invite."""
    instance = _FakeRetrievalInstance()
    report = project_document_loader(instance.client(), _plan(), token=TOKEN)
    assert "does not prove Open WebUI called the endpoint" in (
        report["readback_is_not_reach_proof"]
    )


def test_the_report_names_both_residual_vector_write_paths(
    deployments_root: Path,
) -> None:
    """An operator reading "zero vector rows" is told what it does not cover.

    Both, not one: the batch path is the arm that actually writes rows
    with the bypass on, and process_text is unguarded on raw pasted text.
    """
    instance = _FakeRetrievalInstance()
    report = project_document_loader(instance.client(), _plan(), token=TOKEN)
    paths = {entry["path"] for entry in report["residual_vector_write_paths"]}
    assert paths == {"routers/retrieval.py:2223", "routers/retrieval.py:3427"}
    functions = {entry["function"] for entry in RESIDUAL_VECTOR_WRITE_PATHS}
    assert functions == {"process_text", "process_files_batch"}
    assert "routers/knowledge.py:2117" in json.dumps(report)
    assert set(report["bypass_covered_ingest_sites"]) == {
        "routers/knowledge.py:432",
        "routers/knowledge.py:1482",
        "routers/knowledge.py:1576",
    }


# ================================================= the optional name header


def test_the_optional_okg_filename_header_is_used_and_recorded(
    deployments_root: Path, vendor_file: Path,
) -> None:
    """``{{FILE_NAME}}`` gives the operator's real name, and says so.

    Off by default (the projected set is exactly the gate keys plus the
    bypass); when a deployment declares it, the endpoint records which
    channel supplied the name so a citation cannot change silently.
    """
    with _serving() as server:
        _vendor_load(
            server.base_url,
            file_path=vendor_file,
            headers={HEADER_OKG_FILE_NAME: "{{FILE_NAME}}"},
            metadata={"file_name": "quarterly-review.md"},
        )
        record = server.records[-1]
    assert record.filename == "quarterly-review.md"
    assert record.filename_source == "x-okg-file-name"
    assert _staged_blobs(deployments_root)[0].endswith("quarterly-review.md")


def test_records_correlate_by_the_filename_the_vendor_sent(
    deployments_root: Path, vendor_file: Path, tmp_path: Path,
) -> None:
    """The correlation the live reached-proof asserts on, exercised here."""
    record_path = tmp_path / "loader-records.jsonl"
    recorder = LoaderRecorder(record_path)
    server = DocumentLoaderServer(_plan(), port=0, recorder=recorder).start()
    try:
        _vendor_load(server.base_url, file_path=vendor_file)
    finally:
        server.close()

    assert len(recorder.matching_filename(ORIGINAL_NAME)) == 1
    assert recorder.matching_filename("never-uploaded.md") == []
    written = [json.loads(line) for line in record_path.read_text().splitlines()]
    assert written[0]["x_filename"] == urllib.parse.quote(VENDOR_STORED_NAME)
    assert written[0]["outcome"] == "staged"


def test_free_port_returns_a_bindable_port() -> None:
    from okg.substrate.chat.document_loader import free_port

    port = free_port()
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", port))
