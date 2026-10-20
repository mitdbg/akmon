"""okg as Open WebUI's external document loader — the native upload, governed.

Per PACT `chat-document-loader`. Open WebUI's most-used affordance is
dragging a file into the chat. At the pinned version those bytes are
extracted locally and stored in the vendor's OWN vector store: a second,
ungoverned corpus (design.md section 2 forbids exactly that), invisible
to every operator surface.

The vendor ships the hook that closes it. When -- and only when -- all
THREE of these hold, extraction is delegated to an operator endpoint
(`retrieval/loaders/main.py:542-544` at v0.11.4; :433-435 at v0.11.0)::

    self.engine == 'external'
    and self.kwargs.get('EXTERNAL_DOCUMENT_LOADER_URL')
    and self.kwargs.get('EXTERNAL_DOCUMENT_LOADER_API_KEY')

This module serves that endpoint. `document_loader_projection` drives and
readback-verifies the three conditions; nothing here assumes they hold.

**A second front door, never a second ingest path.** Every request lands
in `chat-upload-source`'s staging area through
`okg.substrate.mcp.upload_admin.upload_to_staging` -- the same resolver,
the same Nomos classification, the same refusals, the same
unparseable-upload check, the same downstream admission and publish. The
`upload` MCP tool and this endpoint are two doors onto one source. A
deployment that registers no upload source has no loader either: the
refusal comes from the shared resolver and is returned as a non-2xx, so
the vendor RAISES and the user sees it, rather than the upload quietly
taking the local path.

**What the vendor is left holding.** `process_file` writes the extracted
text onto the file row (`routers/retrieval.py:2064` at v0.11.4, :1965 at
v0.11.0) BEFORE the bypass branch at :2071, and with the vector store
bypassed that row is what retrieval reads (`retrieval/utils.py:1531` at
v0.11.4, :1385 at v0.11.0). Its value is whatever this
endpoint RETURNS, so it is ours to choose:

* ``pointer`` (the default): the full text goes to the graph and the
  returned ``page_content`` is a breadcrumb naming the node id, the
  source, the generation, and the `search` call that fetches it. No
  vendor-side copy of the document.
* ``inline``: the full text is returned as well, so a copy sits on the
  vendor's file row. Legitimate, but only when a deployment DECLARES it;
  it is never inferred, and the sync report says a vendor-side copy
  exists.

**Secrets by reference.** The plan carries the NAME of the environment
variable holding the loader token, never the token. `resolve_token`
reads it at use; nothing here logs it, records it, or returns it.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import http.server
import json
import re
import socket
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Optional
from urllib.parse import unquote, urlsplit

from okg.substrate.errors import SubstrateError


__all__ = [
    "DocumentLoaderError",
    "DocumentLoaderPlan",
    "DocumentLoaderServer",
    "HEADER_AUTHORIZATION",
    "HEADER_CONTENT_TYPE",
    "HEADER_FILENAME",
    "HEADER_OKG_FILE_ID",
    "HEADER_OKG_FILE_NAME",
    "LOADER_MODES",
    "LOADER_PROCESS_PATH",
    "LoaderRecorder",
    "LoaderRequestRecord",
    "MODE_INLINE",
    "MODE_POINTER",
    "VENDOR_CONTRACT",
    "VendorLoaderContract",
    "breadcrumb_text",
    "loader_response",
    "process_loader_request",
    "serve_document_loader",
    "split_vendor_filename",
]


# --------------------------------------------------------------- contract

#: The path the vendor appends to ``EXTERNAL_DOCUMENT_LOADER_URL`` after
#: stripping one trailing slash (`retrieval/loaders/external_document.py`).
LOADER_PROCESS_PATH = "/process"

HEADER_AUTHORIZATION = "Authorization"
HEADER_CONTENT_TYPE = "Content-Type"
HEADER_FILENAME = "X-Filename"

#: OPTIONAL headers a deployment may add through
#: ``EXTERNAL_DOCUMENT_LOADER_HEADERS``, whose values the vendor templates
#: from the file's own metadata (`utils/headers.py:parse_custom_headers`,
#: placeholders ``{{FILE_NAME}}`` / ``{{FILE_ID}}``). They are NOT part of
#: the gate and NOT projected by default: the endpoint works without them
#: and records which channel supplied the name it used.
HEADER_OKG_FILE_NAME = "X-Okg-File-Name"
HEADER_OKG_FILE_ID = "X-Okg-File-Id"

MODE_POINTER = "pointer"
MODE_INLINE = "inline"
LOADER_MODES = (MODE_POINTER, MODE_INLINE)


@dataclass(frozen=True)
class VendorLoaderContract:
    """The pinned vendor loader contract, as MEASURED, not as documented.

    Read out of a running ``ghcr.io/open-webui/open-webui:v0.11.0``
    container on 2026-08-11, and re-read at v0.11.4 (revision ``8bd8b4fa``)
    on 2026-09-29: ``retrieval/loaders/external_document.py`` is
    byte-identical (same sha256 below), the gate moved to
    ``retrieval/loaders/main.py:542-544`` unchanged, and the three config
    keys are registered at ``config.py:2863`` and ``:2882-2883``.

    ``client_source_sha256`` pins the vendor client the contract test
    DRIVES: the test execs the byte-identical copy at
    ``client_source_relpath`` (a test fixture, relative to the repository
    root), so the assertions run against the vendor's own
    ``ExternalDocumentLoader.load()`` rather than a transcription of it.
    A pin bump changes the file, the digest stops matching, and the test
    fails loudly instead of certifying a contract that moved.
    """

    openwebui_version: str
    image_ref: str
    method: str
    path_suffix: str
    request_headers: tuple[str, ...]
    accepted_response_shapes: tuple[str, ...]
    client_source_relpath: str
    client_source_sha256: str
    gate_source: str
    gate_conditions: tuple[str, ...]
    gate_config_keys: tuple[str, ...]

    def process_url(self, base_url: str) -> str:
        """Where the vendor will PUT, given ``base_url``.

        Mirrors the vendor's own normalization: exactly ONE trailing
        slash is stripped before ``/process`` is appended.
        """
        url = base_url
        if url.endswith("/"):
            url = url[:-1]
        return f"{url}{self.path_suffix}"


VENDOR_CONTRACT = VendorLoaderContract(
    openwebui_version="0.11.4",
    image_ref=(
        "your-registry.invalid/okg-openwebui-compat@"
        "sha256:2561170716571aaf7e41417decd7e23b43c390a00395c957b362a17e80557c11"
    ),
    method="PUT",
    path_suffix=LOADER_PROCESS_PATH,
    request_headers=(HEADER_CONTENT_TYPE, HEADER_AUTHORIZATION, HEADER_FILENAME),
    # `load()` accepts a dict OR a list of dicts, each read for
    # `page_content` and `metadata`. Anything else raises "Unable to
    # parse content"; a falsy body raises "No content returned".
    accepted_response_shapes=("object", "list_of_objects"),
    client_source_relpath=(
        "tests/fixtures/chat_document_loader/vendor/external_document.py.txt"
    ),
    client_source_sha256=(
        "5c42220f5f0b9f4b3e142496f3c6d921481eb8c79b261eecdf44001af2992fef"
    ),
    gate_source="retrieval/loaders/main.py:542-544",
    gate_conditions=(
        "self.engine == 'external'",
        "EXTERNAL_DOCUMENT_LOADER_URL",
        "EXTERNAL_DOCUMENT_LOADER_API_KEY",
    ),
    gate_config_keys=(
        "rag.content_extraction_engine",
        "rag.external_document_loader_url",
        "rag.external_document_loader_api_key",
    ),
)


class DocumentLoaderError(SubstrateError):
    """The loader endpoint cannot serve this deployment or this request.

    One code with ``data['reason']`` carrying the arm, matching the
    sibling chat surfaces (`ChatInstanceError`, `ChatSyncError`): the
    operator action is always the same shape -- read the reason and the
    named input, correct it, run again.
    """

    code = "chat_document_loader_failed"


# ------------------------------------------------------------------- plan

#: An environment variable NAME, so a plan that was handed a literal
#: secret is refused instead of quietly treating the secret as a name and
#: then failing to resolve it.
_ENV_NAME_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")


@dataclass(frozen=True)
class DocumentLoaderPlan:
    """What one deployment's loader endpoint serves, and how.

    ``token_env`` is the NAME of the environment variable holding the
    shared secret the vendor sends as ``Authorization: Bearer``. The
    value never enters a plan, a manifest, a report, or a log.
    """

    deployment: str
    #: What the vendor is told: the value of EXTERNAL_DOCUMENT_LOADER_URL.
    #: The vendor appends ``/process``; this is the base, not that path.
    base_url: str
    token_env: str
    mode: str = MODE_POINTER
    #: Where handler records are appended as JSON lines. The reached-proof
    #: reads this file; without it records live only in memory.
    record_path: Optional[Path] = None
    #: Optional ``EXTERNAL_DOCUMENT_LOADER_HEADERS`` the deployment also
    #: declares. Empty by default so the projected set is exactly the keys
    #: the gate needs.
    filename_headers: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.mode not in LOADER_MODES:
            raise DocumentLoaderError(
                f"deployment {self.deployment!r} declares loader mode "
                f"{self.mode!r}, which is not one of {list(LOADER_MODES)}",
                data={
                    "reason": "unknown_loader_mode",
                    "deployment": self.deployment,
                    "mode": self.mode,
                },
                fix_hint=(
                    f"declare {MODE_POINTER!r} (the default: the document goes "
                    f"to the graph and the vendor keeps a breadcrumb) or "
                    f"{MODE_INLINE!r} (the vendor additionally keeps a copy)"
                ),
            )
        if not str(self.base_url).strip():
            raise DocumentLoaderError(
                f"deployment {self.deployment!r} declares no loader base_url",
                data={"reason": "missing_base_url", "deployment": self.deployment},
                fix_hint="set the URL the instance can reach this endpoint at",
            )
        if not _ENV_NAME_RE.match(str(self.token_env or "")):
            raise DocumentLoaderError(
                f"deployment {self.deployment!r} must name an ENVIRONMENT "
                f"VARIABLE for the loader token, not supply one",
                data={"reason": "token_env_is_not_a_name", "deployment": self.deployment},
                fix_hint=(
                    "set token_env to a name like OKG_CHAT_LOADER_TOKEN and "
                    "put the secret in that variable; a token in manifest "
                    "content is a leaked token"
                ),
            )

    @property
    def process_url(self) -> str:
        return VENDOR_CONTRACT.process_url(self.base_url)

    def resolve_token(self, environ: Optional[Mapping[str, str]] = None) -> str:
        """The shared secret, from the environment. Never defaulted.

        An unset or empty variable is a REFUSAL, not an unauthenticated
        endpoint: a loader that accepts anything is a public file-ingest
        door onto a governed source.
        """
        import os

        env = environ if environ is not None else os.environ
        token = str(env.get(self.token_env) or "")
        if not token:
            raise DocumentLoaderError(
                f"the loader token variable {self.token_env} is unset or "
                f"empty, so deployment {self.deployment!r} has no loader",
                data={
                    "reason": "loader_token_unset",
                    "deployment": self.deployment,
                    "token_env": self.token_env,
                },
                fix_hint=(
                    f"export {self.token_env} with the same secret projected "
                    f"into EXTERNAL_DOCUMENT_LOADER_API_KEY"
                ),
            )
        return token


# ----------------------------------------------------------------- record


@dataclass(frozen=True)
class LoaderRequestRecord:
    """One ``PUT /process`` the handler actually saw.

    This -- not a readback of the three config keys -- is the evidence
    that the vendor CALLED us. Readback proves the keys were written.

    The ``Authorization`` value is deliberately absent: only whether one
    was present, and whether it matched.
    """

    received_at: str
    method: str
    path: str
    x_filename: str
    filename: str
    filename_source: str
    content_type: str
    byte_count: int
    authorization_present: bool
    status: int
    outcome: str
    upload_id: str = ""
    node_id: str = ""
    vendor_file_id: str = ""

    def to_json(self) -> dict[str, Any]:
        return {
            "received_at": self.received_at,
            "method": self.method,
            "path": self.path,
            "x_filename": self.x_filename,
            "filename": self.filename,
            "filename_source": self.filename_source,
            "content_type": self.content_type,
            "byte_count": self.byte_count,
            "authorization_present": self.authorization_present,
            "status": self.status,
            "outcome": self.outcome,
            "upload_id": self.upload_id,
            "node_id": self.node_id,
            "vendor_file_id": self.vendor_file_id,
        }


class LoaderRecorder:
    """Append-only record of every request the handler saw.

    Records REFUSALS too. "No request arrived" and "a request arrived and
    was refused" are opposite diagnoses of a failed reached-proof, and a
    recorder that kept only successes could not tell them apart.
    """

    def __init__(self, path: Optional[Path] = None) -> None:
        self.path = Path(path) if path is not None else None
        self.records: list[LoaderRequestRecord] = []

    def record(self, entry: LoaderRequestRecord) -> LoaderRequestRecord:
        self.records.append(entry)
        if self.path is not None:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(entry.to_json(), sort_keys=True) + "\n")
        return entry

    def matching_filename(self, needle: str) -> list[LoaderRequestRecord]:
        """Records whose vendor-sent or decoded name contains ``needle``.

        The correlation the reached-proof asserts on: the operator knows
        the name they dropped into the chat, the vendor sends it
        URL-quoted and prefixed with its own file id, and both forms are
        searched so neither encoding hides a real request.
        """
        return [
            entry
            for entry in self.records
            if needle in entry.x_filename or needle in entry.filename
        ]


# -------------------------------------------------------------- filenames

#: The vendor stores an upload as ``<file id>_<original name>``
#: (`routers/files.py:367` at v0.11.4, :358 at v0.11.0) and sends the basename of THAT path as
#: ``X-Filename``. The id is a UUID4.
_VENDOR_STORED_NAME_RE = re.compile(
    r"^(?P<file_id>[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}"
    r"-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})_(?P<name>.+)$"
)


def split_vendor_filename(x_filename: str) -> tuple[str, str, str]:
    """``(vendor_file_id, original_name, rule)`` from an ``X-Filename``.

    The header is URL-quoted (``quote(os.path.basename(file_path))``), so
    it is unquoted first. The id prefix is stripped ONLY when the name
    matches the vendor's storage shape exactly; anything else is used
    verbatim. Which rule fired is returned and recorded, because
    "recovered the operator's filename" and "used the vendor's stored
    name" publish different citations and the difference must be visible.
    """
    decoded = unquote(str(x_filename or "")).strip()
    # A basename by construction, but a crafted header is not trusted to
    # be one: this is the name a file gets written under.
    decoded = decoded.replace("\\", "/").rsplit("/", 1)[-1]
    match = _VENDOR_STORED_NAME_RE.match(decoded)
    if match:
        return match.group("file_id"), match.group("name"), "vendor_stored_name"
    return "", decoded, "verbatim"


# ------------------------------------------------------------- responses


def breadcrumb_text(
    *,
    node_id: str,
    source: str,
    deployment: str,
    origin: str,
    generation: Optional[str] = None,
) -> str:
    """The pointer-mode ``page_content``: what the vendor is left holding.

    Names the node id, the source, the generation, and the call that
    fetches the content. The generation clause is explicit in BOTH
    directions -- a document staged but not yet published says so, rather
    than omitting the line and reading as published.
    """
    if generation:
        published = f"published in generation {generation}"
    else:
        published = (
            "not yet published; it becomes retrievable after the next ingest "
            "and publish cycle for this deployment"
        )
    return (
        f"[okg] {origin} was ingested into the operational knowledge graph "
        f"for deployment {deployment}.\n"
        f"node_id: {node_id}\n"
        f"source: {source}\n"
        f"generation: {published}\n"
        f"The document's text is NOT stored here. Retrieve it with the "
        f"`search` operator (query this node_id, or the document's title), "
        f"then read the returned chunks."
    )


def loader_response(
    *,
    page_content: str,
    metadata: Mapping[str, Any],
) -> dict[str, Any]:
    """One vendor ``Document``, in the shape ``load()`` accepts.

    Refuses to build an empty one. The vendor treats a falsy response
    body as an error ("No content returned") and joins ``page_content``
    onto the file row unconditionally, so an empty string here would
    either raise vendor-side or blank the file row -- neither is a
    success, and neither may be returned as 200.
    """
    text = str(page_content or "")
    if not text.strip():
        raise DocumentLoaderError(
            "the loader would return an empty page_content, which the vendor "
            "reads as an error and which would blank the file row",
            data={"reason": "empty_page_content", "metadata": dict(metadata)},
        )
    return {"page_content": text, "metadata": dict(metadata)}


# ------------------------------------------------------------- processing


#: Refusal outcome -> HTTP status. Everything is a non-2xx: the vendor
#: raises on a non-ok response, which is what puts the refusal in front
#: of the user instead of silently routing the upload back to the local
#: extraction path.
_REFUSAL_STATUS: dict[str, int] = {
    "upload_source_not_registered": 503,
    "upload_source_unclassified": 503,
    "upload_source_unavailable": 503,
    "invalid_request": 400,
    "rejected": 400,
    "refused_unparseable": 415,
    "refused_unsafe_url": 400,
}


def _stage_upload(
    deployment: str, *, filename: str, content: bytes, uploaded_by: str,
) -> dict[str, Any]:
    """The default staging call: the upload tool's own front door.

    Imported at call time so this module has no import-time dependency
    on the MCP package.
    """
    from okg.substrate.mcp.upload_admin import upload_to_staging

    return upload_to_staging(
        deployment,
        uploaded_by=uploaded_by,
        filename=filename,
        content_base64=base64.b64encode(content).decode("ascii"),
    )


def _load_registration(deployment: str) -> Any:
    from okg.substrate.mcp.upload_admin import load_upload_registration

    return load_upload_registration(deployment)


def _inline_text(registration: Any, staged_name: str, content: bytes) -> str:
    """The document's text, through the deployment's OWN parser.

    Inline mode returns what the graph will hold, not a decode of the raw
    bytes: a PDF decoded as UTF-8 is mojibake, and returning it would put
    mojibake on the file row and into the model's context.
    """
    from okg.substrate.library.extraction.registry import load_extractors
    from okg.substrate.library.sources.doc_corpus import DEFAULT_EXTENSION_TO_FORMAT

    fmt = DEFAULT_EXTENSION_TO_FORMAT.get(Path(staged_name).suffix.lower())
    bundle = load_extractors(registration.extractors_dir)
    parser = (getattr(bundle, "parsers", None) or {}).get(fmt) if fmt else None
    if parser is None:
        # Unreachable through the ordinary path: `upload_to_staging` runs
        # the same parseable check and refuses first. Kept because "no
        # parser" must never become "return the raw bytes".
        raise DocumentLoaderError(
            f"inline mode has no parser for {staged_name!r} (format "
            f"{fmt!r}), so there is no text to return",
            data={
                "reason": "inline_no_parser",
                "staged_name": staged_name,
                "format": fmt,
            },
        )
    return parser.parse(content).text


def process_loader_request(
    plan: DocumentLoaderPlan,
    *,
    body: bytes,
    headers: Mapping[str, str],
    recorder: LoaderRecorder,
    path: str = LOADER_PROCESS_PATH,
    method: str = "PUT",
    token: Optional[str] = None,
    generation: Optional[str] = None,
    stage: Optional[Callable[..., dict[str, Any]]] = None,
    registration_loader: Optional[Callable[[str], Any]] = None,
    now: Optional[datetime] = None,
) -> tuple[int, Any]:
    """Handle one vendor request. Returns ``(status, json_payload)``.

    Every arm records, and every non-success arm is a non-2xx with a JSON
    body naming the reason.
    """
    stamp = (now or datetime.now(timezone.utc)).isoformat()
    header_get = _header_reader(headers)
    authorization = header_get(HEADER_AUTHORIZATION)
    content_type = header_get(HEADER_CONTENT_TYPE)
    raw_filename = header_get(HEADER_FILENAME)
    vendor_file_id, filename, rule = split_vendor_filename(raw_filename)
    filename_source = f"x-filename:{rule}"
    declared_name = header_get(HEADER_OKG_FILE_NAME)
    if declared_name:
        # The vendor templated this from the file's own metadata
        # (`{{FILE_NAME}}`), so it is the operator's original name rather
        # than the storage name. Preferred when present, and the record
        # says which channel was used -- never a silent substitution.
        filename = unquote(declared_name).replace("\\", "/").rsplit("/", 1)[-1]
        filename_source = "x-okg-file-name"
    vendor_file_id = vendor_file_id or unquote(header_get(HEADER_OKG_FILE_ID) or "")

    def _record(status: int, outcome: str, **extra: Any) -> LoaderRequestRecord:
        return recorder.record(
            LoaderRequestRecord(
                received_at=stamp,
                method=method,
                path=path,
                x_filename=raw_filename,
                filename=filename,
                filename_source=filename_source,
                content_type=content_type,
                byte_count=len(body or b""),
                authorization_present=bool(authorization),
                status=status,
                outcome=outcome,
                vendor_file_id=vendor_file_id,
                **extra,
            )
        )

    def _refuse(status: int, outcome: str, summary: str, **extra: Any) -> tuple[int, Any]:
        _record(status, outcome)
        payload = {"status": outcome, "summary": summary, "loader": "okg"}
        payload.update(extra)
        return status, payload

    expected = token if token is not None else plan.resolve_token()
    presented = ""
    if authorization.startswith("Bearer "):
        presented = authorization[len("Bearer "):].strip()
    if not hmac.compare_digest(presented, expected):
        # Same refusal for absent and wrong, so the response does not tell
        # a prober which of the two it hit.
        return _refuse(
            401,
            "unauthorized",
            "the loader token is missing or does not match this deployment's",
        )
    if not raw_filename and not declared_name:
        return _refuse(
            400,
            "missing_filename",
            f"{HEADER_FILENAME} is required: the upload is published under "
            f"that name and a document with no name is not ingestable",
        )
    if not filename:
        return _refuse(
            400, "empty_filename", f"{HEADER_FILENAME} decoded to an empty name",
        )
    if not body:
        # NEVER a 200. The vendor raises on an empty response body, and an
        # empty request body would otherwise publish a zero-byte document.
        return _refuse(
            400,
            "empty_body",
            "the request body carried no bytes, so there is nothing to ingest",
        )

    staged = (stage or _stage_upload)(
        plan.deployment,
        filename=filename,
        content=body,
        uploaded_by=f"open-webui:document-loader:{plan.deployment}",
    )
    status_word = str(staged.get("status") or "")
    if status_word != "staged":
        http_status = _REFUSAL_STATUS.get(status_word, 400)
        return _refuse(
            http_status,
            status_word or "refused",
            str(staged.get("summary") or "the upload source refused this upload"),
            fix_hint=str(staged.get("fix_hint") or ""),
        )

    registration = (registration_loader or _load_registration)(plan.deployment)
    upload_id = str(staged.get("upload_id") or "")
    blob = registration.staging().find(upload_id)
    if blob is None:
        # Staging reported success and the sidecar is not there. Refuse:
        # a breadcrumb naming a node that no ingest will produce is a
        # pointer to nothing.
        return _refuse(
            500,
            "staged_upload_not_found",
            f"upload {upload_id!r} reported staged but no sidecar is present, "
            f"so the node it will publish cannot be named",
        )
    node_id = _node_id(registration, blob.staged_name)

    if plan.mode == MODE_INLINE:
        page_content = _inline_text(registration, blob.staged_name, body)
        if not page_content.strip():
            return _refuse(
                422,
                "empty_extraction",
                f"inline mode parsed {filename!r} to empty text, so there is "
                f"nothing to return and nothing to retrieve",
            )
    else:
        page_content = breadcrumb_text(
            node_id=node_id,
            source=str(staged.get("source") or registration.ownership_id),
            deployment=plan.deployment,
            origin=str(staged.get("origin") or filename),
            generation=generation,
        )

    metadata = {
        "okg_loader": "okg.substrate.chat.document_loader",
        "okg_deployment": plan.deployment,
        "okg_mode": plan.mode,
        "okg_source": str(staged.get("source") or registration.ownership_id),
        "okg_node_id": node_id,
        "okg_upload_id": upload_id,
        "okg_origin": str(staged.get("origin") or filename),
        "okg_content_sha256": str(staged.get("content_sha256") or ""),
        "okg_byte_count": int(staged.get("byte_count") or 0),
        "okg_generation": str(generation or ""),
        "okg_vendor_file_id": vendor_file_id,
    }
    payload = loader_response(page_content=page_content, metadata=metadata)
    _record(200, "staged", upload_id=upload_id, node_id=node_id)
    return 200, payload


def _node_id(registration: Any, staged_name: str) -> str:
    from okg.substrate.library.sources.upload import staged_document_node_id

    return staged_document_node_id(registration.repo, staged_name)


def _header_reader(headers: Mapping[str, str]) -> Callable[[str], str]:
    """Case-insensitive single-header read over any mapping.

    ``http.client.HTTPMessage`` is already case-insensitive; a plain dict
    built by a test is not, and a contract that only holds for one of them
    is a contract that passes in tests and fails on the wire.
    """
    getter = getattr(headers, "get", None)
    lowered = {str(k).lower(): v for k, v in dict(headers).items()}

    def read(name: str) -> str:
        if getter is not None:
            direct = getter(name)
            if direct:
                return str(direct).strip()
        return str(lowered.get(name.lower()) or "").strip()

    return read


# ---------------------------------------------------------------- server


class _LoaderHandler(http.server.BaseHTTPRequestHandler):
    """Serves exactly ``PUT <base>/process``. Everything else is 404."""

    plan: DocumentLoaderPlan
    recorder: LoaderRecorder
    token: Optional[str] = None
    generation: Optional[str] = None

    protocol_version = "HTTP/1.1"

    def log_message(self, *_args: Any) -> None:  # noqa: D102 - quiet by design
        return

    def _send(self, status: int, payload: Any) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_PUT(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler's contract
        path = urlsplit(self.path).path
        if path != LOADER_PROCESS_PATH:
            self._send(404, {"status": "no_route", "summary": f"no route for {path}"})
            return
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length > 0 else b""
        try:
            status, payload = process_loader_request(
                self.plan,
                body=body,
                headers=self.headers,
                recorder=self.recorder,
                path=path,
                method="PUT",
                token=self.token,
                generation=self.generation,
            )
        except DocumentLoaderError as exc:
            # A refusal that is the DEPLOYMENT's state, not this request's:
            # an unset token variable, an unparseable inline document. 503
            # with the reason, never a 200 with a placeholder body.
            self._send(
                503,
                {
                    "status": str(
                        (getattr(exc, "data", None) or {}).get("reason")
                        or getattr(exc, "code", "chat_document_loader_failed")
                    ),
                    "summary": str(exc),
                    "loader": "okg",
                },
            )
            return
        self._send(status, payload)

    def do_GET(self) -> None:  # noqa: N802
        # A liveness probe for the reached-proof: it answers whether the
        # endpoint is up WITHOUT accepting an upload, so a probe can never
        # be mistaken for a vendor call in the record.
        if urlsplit(self.path).path == "/healthz":
            self._send(200, {"status": "ok", "loader": "okg", "mode": self.plan.mode})
            return
        self._send(404, {"status": "no_route"})


class DocumentLoaderServer:
    """A running loader endpoint. Context manager; binds a real socket."""

    def __init__(
        self,
        plan: DocumentLoaderPlan,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        recorder: Optional[LoaderRecorder] = None,
        token: Optional[str] = None,
        generation: Optional[str] = None,
    ) -> None:
        self.plan = plan
        self.recorder = recorder or LoaderRecorder(plan.record_path)
        # Resolved ONCE, at bind time, so a deployment with no token
        # refuses to start rather than 503-ing every upload after the
        # vendor has already been told to route through us.
        self.token = token if token is not None else plan.resolve_token()
        handler = type(
            "_BoundLoaderHandler",
            (_LoaderHandler,),
            {
                "plan": plan,
                "recorder": self.recorder,
                "token": self.token,
                "generation": generation,
            },
        )
        self._server = http.server.ThreadingHTTPServer((host, port), handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(
            target=self._server.serve_forever,
            name="okg-document-loader",
            daemon=True,
        )
        self.host, self.port = self._server.server_address[:2]

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    @property
    def records(self) -> list[LoaderRequestRecord]:
        return self.recorder.records

    def start(self) -> "DocumentLoaderServer":
        self._thread.start()
        return self

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)

    def __enter__(self) -> "DocumentLoaderServer":
        return self.start()

    def __exit__(self, *_exc: Any) -> None:
        self.close()


def serve_document_loader(
    plan: DocumentLoaderPlan,
    *,
    host: str = "127.0.0.1",
    port: int = 0,
    recorder: Optional[LoaderRecorder] = None,
    generation: Optional[str] = None,
) -> DocumentLoaderServer:
    """Start the loader endpoint for ``plan`` and return it running."""
    return DocumentLoaderServer(
        plan, host=host, port=port, recorder=recorder, generation=generation,
    ).start()


def free_port(host: str = "127.0.0.1") -> int:
    """An unbound port, from the kernel."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((host, 0))
        return int(probe.getsockname()[1])


def vendor_client_digest(repo_root: Path | str) -> str:
    """sha256 of the pinned vendor client copy under ``repo_root``."""
    path = Path(repo_root) / VENDOR_CONTRACT.client_source_relpath
    return hashlib.sha256(path.read_bytes()).hexdigest()
