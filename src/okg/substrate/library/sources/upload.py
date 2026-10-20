"""UploadSource — operator file/URL upload as governed document-corpus ingest.

Per PACT `chat-upload-source` (design.md section 9 child 10, section 2
knowledge stance, section 10 decision 8). Ad-hoc upload is a common
admin affordance: a user drops a file or a URL and it becomes
retrievable. This adapter makes that a SOURCE, not a store.

The shape, in one line: an upload lands in a staging area this source
owns, and the ordinary document-corpus pipeline ingests it. There is no
second retrieval path — no Open WebUI knowledge base, file store, or
vector store participates at any point (design.md section 2: a second
ungoverned store competing with the graph is forbidden).

Three properties are load-bearing.

**Governed.** The upload path exists only where a deployment registers
this source in its ``source_registry.yaml`` with an admission policy and
a Nomos source policy that declares ``sensitivity``,
``data_classification``, and ``store_raw``. A deployment that registers
nothing has no upload path at all: `resolve_upload_registration` raises
`UploadSourceNotRegistered`, the MCP surface omits the tool, and a
direct attempt is refused by name. Absence of the registration is never
a fallback to some other storage.

**Source-owned.** Staging is operator state belonging to this source
(`UploadStaging`), not chat-application state. The blob and its sidecar
are the only copy outside the graph, and they are the source's input --
the same relationship `junit-xml` has to its drop directory.

**No graph-database access.** This adapter emits a `SourceRun` of
NodeFact/EdgeFact and never opens a connection to the deployment graph.
It takes no ``dsn`` parameter at all, so
`okg.substrate.ingest.adapter_factory.source_adapter_init_params` -- which
injects ``dsn`` only when the constructor declares it -- cannot hand one
in. Admission, projection, and publish do their ordinary work downstream.

Parsing is delegated, not reimplemented: `run()` constructs a
`DocCorpusSource` rooted at the staging content directory and streams its
facts, restamping each `document` node's operator-facing identity
(``title``, ``citation_label``, ``citation_url``) from the upload's real
origin. Only attributes the extraction ontology already declares are
written; this adapter does not extend the shared Document subtype.

State translation:
  - cursor: passed through to the delegate unchanged, so the
    document-corpus unchanged-corpus short-circuits keep working.
  - change probe: ``content_hash`` over the staged blobs AND their
    sidecars, so a re-upload, a deletion, or a metadata correction all
    defeat a false-unchanged verdict.

Profile: ``discovery_crawl``. An upload removed from staging disappears
from the next enumeration and is a retraction candidate.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Iterator, Mapping, Optional, Sequence
from urllib.parse import urlparse

from okg.substrate.errors import SubstrateError
from okg.substrate.library.sources import doc_corpus as _doc_corpus
from okg.substrate.library.sources._url_safety import (
    UnsafeFetchURL,
    open_validated_url,
)
from okg.substrate.library.sources.base import (
    NodeFact,
    RecordSet,
    SourceRun,
)
from okg.substrate.library.sources.doc_corpus import DocCorpusSource


log = logging.getLogger(__name__)

__all__ = [
    "UPLOAD_SOURCE_CLASS",
    "UPLOAD_SOURCE_MODULE",
    "StagedUpload",
    "UnsafeFetchURL",
    "UploadRegistration",
    "UploadRejected",
    "UploadSource",
    "UploadSourceNotRegistered",
    "UploadSourceUnavailable",
    "UploadSourceUnclassified",
    "UploadStaging",
    "resolve_upload_registration",
    "staged_document_node_id",
    "upload_source_registry_entry",
]

#: The module/class pair a deployment must name to register the upload
#: source. Registration detection matches on this pair, so a registry
#: entry that merely happens to be called "upload" does not enable the
#: affordance and a rename of this module cannot silently disable it.
UPLOAD_SOURCE_MODULE = "okg.substrate.library.sources.upload"
UPLOAD_SOURCE_CLASS = "UploadSource"

#: Nomos source-policy fields the registration MUST declare before any
#: upload is accepted. `sensitivity` and `data_classification` come
#: straight from the PACT requirement; `store_raw` is included because
#: `okg.substrate.ingest.pii_posture` refuses to infer it and an
#: undeclared value would otherwise be resolved at ingest time, after the
#: bytes are already staged.
REQUIRED_POLICY_FIELDS: tuple[str, ...] = (
    "sensitivity",
    "data_classification",
    "store_raw",
)

#: Default ceiling for one upload. Deliberately modest: an operator who
#: needs a large corpus wires a document-corpus source by pull request
#: rather than pushing gigabytes through a chat affordance.
DEFAULT_MAX_UPLOAD_BYTES = 25 * 1024 * 1024

#: Response content types mapped to the extension the document-corpus
#: dispatch understands. A URL whose path already carries a known
#: extension keeps it; this map only covers the extensionless case.
_CONTENT_TYPE_EXTENSIONS: dict[str, str] = {
    "text/html": ".html",
    "application/xhtml+xml": ".html",
    "text/plain": ".txt",
    "text/markdown": ".md",
    "text/csv": ".csv",
    "application/json": ".json",
    "application/pdf": ".pdf",
    "application/yaml": ".yaml",
    "text/yaml": ".yaml",
}

_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]")
_MAX_SAFE_NAME_CHARS = 120

_SIDECAR_SCHEMA = "okg.upload.staged_upload.v1"


class UploadSourceUnavailable(SubstrateError):
    """No governed upload path exists for this deployment.

    Base class for both refusal arms so a caller can catch the category
    without deciding to treat one of them as a fallback.
    """

    code = "upload_source_unavailable"


class UploadSourceNotRegistered(UploadSourceUnavailable):
    """The deployment's source registry names no upload source."""

    code = "upload_source_not_registered"


class UploadSourceUnclassified(UploadSourceUnavailable):
    """An upload source is registered but its Nomos policy is undeclared."""

    code = "upload_source_unclassified"


class UploadRejected(SubstrateError):
    """The upload itself is refused (name, size, or fetch target)."""

    code = "upload_rejected"


# ---------------------------------------------------------------- staging


@dataclass(frozen=True)
class StagedUpload:
    """One upload sitting in the staging area, awaiting ingest."""

    upload_id: str
    origin_kind: str  # 'file' | 'url'
    origin: str  # operator-supplied filename, or the fetched URL
    staged_name: str  # basename under the staging content directory
    content_sha256: str
    byte_count: int
    uploaded_by: str
    uploaded_at: str
    media_type: str = ""

    def to_sidecar(self) -> dict[str, Any]:
        return {
            "schema": _SIDECAR_SCHEMA,
            "upload_id": self.upload_id,
            "origin_kind": self.origin_kind,
            "origin": self.origin,
            "staged_name": self.staged_name,
            "content_sha256": self.content_sha256,
            "byte_count": self.byte_count,
            "uploaded_by": self.uploaded_by,
            "uploaded_at": self.uploaded_at,
            "media_type": self.media_type,
        }

    @classmethod
    def from_sidecar(cls, raw: Mapping[str, Any]) -> "StagedUpload":
        return cls(
            upload_id=str(raw["upload_id"]),
            origin_kind=str(raw["origin_kind"]),
            origin=str(raw["origin"]),
            staged_name=str(raw["staged_name"]),
            content_sha256=str(raw["content_sha256"]),
            byte_count=int(raw["byte_count"]),
            uploaded_by=str(raw.get("uploaded_by") or ""),
            uploaded_at=str(raw.get("uploaded_at") or ""),
            media_type=str(raw.get("media_type") or ""),
        )


def _safe_staged_name(name: str) -> str:
    """Reduce a basename to the conservative staged-name charset.

    Never used to make an operator-supplied path safe -- `stage_file`
    refuses a path outright (below). This only normalizes a basename that
    is already known to be one path segment.
    """
    cleaned = _SAFE_NAME_RE.sub("_", name).lstrip(".")
    if len(cleaned) > _MAX_SAFE_NAME_CHARS:
        stem, dot, ext = cleaned.rpartition(".")
        if dot and len(ext) < 16:
            keep = _MAX_SAFE_NAME_CHARS - len(ext) - 1
            cleaned = f"{stem[:keep]}.{ext}"
        else:
            cleaned = cleaned[:_MAX_SAFE_NAME_CHARS]
    return cleaned


def _upload_id(origin: str, content_sha256: str) -> str:
    """Deterministic upload identity: origin plus content.

    Deterministic so re-uploading the same bytes from the same origin is
    idempotent rather than accumulating duplicate documents; origin is
    folded in so the same bytes fetched from two URLs stay two uploads
    with two citations.
    """
    seed = f"{origin}\0{content_sha256}".encode("utf-8")
    return hashlib.sha256(seed).hexdigest()[:16]


class UploadStaging:
    """The staging area the upload source owns.

    Layout::

        <staging_dir>/content/<upload_id>-<name>   # the bytes; corpus root
        <staging_dir>/manifest/<upload_id>.json    # who/where/when

    The sidecars live OUTSIDE the corpus root on purpose: the delegate
    walks ``content/`` and must not ingest its own bookkeeping as
    documents.
    """

    def __init__(
        self,
        staging_dir: Path | str,
        *,
        max_upload_bytes: int = DEFAULT_MAX_UPLOAD_BYTES,
    ) -> None:
        self.staging_dir = Path(staging_dir).expanduser()
        self.max_upload_bytes = int(max_upload_bytes)

    @property
    def content_dir(self) -> Path:
        return self.staging_dir / "content"

    @property
    def manifest_dir(self) -> Path:
        return self.staging_dir / "manifest"

    @property
    def tmp_dir(self) -> Path:
        """Scratch for atomic writes, OUTSIDE the corpus root.

        A partial write left inside ``content/`` by a crash would be
        walked as a document forever, published under its temp-file name.
        Same filesystem as both targets, so ``os.replace`` stays atomic.
        """
        return self.staging_dir / "tmp"

    # -- reads ---------------------------------------------------------

    def list_uploads(self) -> list[StagedUpload]:
        """Every upload whose sidecar AND blob are both present.

        A sidecar without its blob is skipped and logged: a half-written
        pair must not become a document with no bytes.
        """
        out: list[StagedUpload] = []
        if not self.manifest_dir.is_dir():
            return out
        for path in sorted(self.manifest_dir.glob("*.json")):
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError) as exc:
                log.warning("upload staging: unreadable sidecar %s (%s)", path, exc)
                continue
            if not isinstance(raw, Mapping):
                log.warning("upload staging: sidecar %s is not a mapping", path)
                continue
            try:
                staged = StagedUpload.from_sidecar(raw)
            except (KeyError, TypeError, ValueError) as exc:
                log.warning("upload staging: malformed sidecar %s (%s)", path, exc)
                continue
            if not (self.content_dir / staged.staged_name).is_file():
                log.warning(
                    "upload staging: sidecar %s has no blob at %s; skipping",
                    path,
                    staged.staged_name,
                )
                continue
            out.append(staged)
        return out

    def by_staged_name(self) -> dict[str, StagedUpload]:
        return {u.staged_name: u for u in self.list_uploads()}

    def find(self, upload_id: str) -> Optional[StagedUpload]:
        """The staged upload with this id, or None.

        Reads the sidecar rather than recomputing the staged name from
        the origin: a caller that needs to know where the bytes landed
        must OBSERVE it, because a second derivation of the same name is
        a second implementation that can disagree with this one.
        """
        for staged in self.list_uploads():
            if staged.upload_id == upload_id:
                return staged
        return None

    def content_paths(self) -> list[Path]:
        if not self.content_dir.is_dir():
            return []
        return sorted(p for p in self.content_dir.iterdir() if p.is_file())

    def sidecar_paths(self) -> list[Path]:
        if not self.manifest_dir.is_dir():
            return []
        return sorted(self.manifest_dir.glob("*.json"))

    # -- writes --------------------------------------------------------

    def discard(self, staged: StagedUpload) -> None:
        """Remove a staged upload and its sidecar.

        Used when a post-staging check refuses the upload: leaving the
        bytes behind would let the next ingest publish exactly what the
        check rejected. Missing files are tolerated — the goal is the
        absence, not the deletion.
        """
        for path in (
            self.manifest_dir / f"{staged.upload_id}.json",
            self.content_dir / staged.staged_name,
        ):
            try:
                path.unlink()
            except FileNotFoundError:
                continue

    def stage_file(
        self,
        *,
        filename: str,
        content: bytes,
        uploaded_by: str,
        now: Optional[datetime] = None,
    ) -> StagedUpload:
        """Stage operator-supplied bytes under ``filename``.

        ``filename`` must be ONE path segment. A path is refused rather
        than silently reduced to its basename: an operator who typed a
        path meant something this source will not do, and a quiet
        rewrite would hide it.
        """
        raw_name = (filename or "").strip()
        if not raw_name:
            raise UploadRejected(
                "upload refused: filename is empty",
                fix_hint="supply the original file name, e.g. 'notes.md'",
            )
        if "/" in raw_name or "\\" in raw_name or raw_name in (".", ".."):
            raise UploadRejected(
                f"upload refused: filename {raw_name!r} is a path, not a name",
                data={"filename": raw_name},
                fix_hint=(
                    "pass the bare file name; the staging area chooses where "
                    "the bytes land"
                ),
            )
        safe_name = _safe_staged_name(raw_name)
        if not safe_name:
            raise UploadRejected(
                f"upload refused: filename {raw_name!r} has no usable characters",
                data={"filename": raw_name},
            )
        return self._write(
            content=content,
            origin=raw_name,
            origin_kind="file",
            safe_name=safe_name,
            uploaded_by=uploaded_by,
            media_type="",
            now=now,
        )

    def stage_url(
        self,
        *,
        url: str,
        uploaded_by: str,
        now: Optional[datetime] = None,
    ) -> StagedUpload:
        """Fetch ``url`` through the library's egress guard and stage it.

        The guard runs BEFORE any staging write and outside this method's
        error handling, so a private, loopback, link-local, or
        non-http(s) target surfaces as a refusal instead of an empty
        upload. Redirects are revalidated hop by hop
        (`open_validated_url`), because a first-hop-only check is
        defeated by a 302 to the metadata address.

        There is deliberately no injectable opener: an opener parameter
        would be a bypass of the guard that no test exercises. Tests that
        need a canned response patch the module-level
        `open_validated_url`, which is also what proves this method
        routes through it.
        """
        target = (url or "").strip()
        if not target:
            raise UploadRejected(
                "upload refused: url is empty",
                fix_hint="supply an http(s) URL to fetch",
            )
        parsed = urlparse(target)
        if parsed.username or parsed.password:
            # The origin is published: it becomes the document's title and
            # citation_label, reaches the BM25 searchable-text stamp, and
            # is echoed in the tool response. Credentials in the URL would
            # travel all of that. Refuse rather than strip, so the operator
            # knows the credential was seen.
            raise UploadRejected(
                "upload refused: the URL carries credentials in its userinfo, "
                "and the URL is published as the document's citation",
                data={"origin_kind": "url", "host": parsed.hostname or ""},
                fix_hint=(
                    "fetch the resource without embedding credentials in the "
                    "URL; credentialed connectors are configured by pull "
                    "request, not by chat upload"
                ),
            )
        response = open_validated_url(target)
        try:
            body = response.read(self.max_upload_bytes + 1)
            media_type = ""
            encoding = ""
            headers = getattr(response, "headers", None)
            if headers is not None:
                media_type = str(headers.get("Content-Type") or "").split(";")[0].strip()
                encoding = str(headers.get("Content-Encoding") or "").strip().lower()
        finally:
            close = getattr(response, "close", None)
            if callable(close):
                close()
        if encoding and encoding != "identity":
            # urllib does not decode content encodings. Storing the
            # compressed bytes would publish a document of garbage with no
            # error anywhere.
            raise UploadRejected(
                f"upload refused: {target!r} returned Content-Encoding "
                f"{encoding!r}, which this source does not decode",
                data={"origin": target, "content_encoding": encoding},
                fix_hint="fetch a resource served without content encoding",
            )
        safe_name = _safe_staged_name(_url_basename(target, media_type))
        if not safe_name:
            safe_name = "fetched"
        return self._write(
            content=body,
            origin=target,
            origin_kind="url",
            safe_name=safe_name,
            uploaded_by=uploaded_by,
            media_type=media_type,
            now=now,
        )

    def _write(
        self,
        *,
        content: bytes,
        origin: str,
        origin_kind: str,
        safe_name: str,
        uploaded_by: str,
        media_type: str,
        now: Optional[datetime],
    ) -> StagedUpload:
        if not content:
            raise UploadRejected(
                f"upload refused: {origin!r} produced zero bytes",
                data={"origin": origin, "origin_kind": origin_kind},
                fix_hint=(
                    "an empty upload would publish a document with no content; "
                    "check the file or URL"
                ),
            )
        if len(content) > self.max_upload_bytes:
            raise UploadRejected(
                f"upload refused: {origin!r} is {len(content)} bytes, over the "
                f"{self.max_upload_bytes}-byte limit",
                data={
                    "origin": origin,
                    "byte_count": len(content),
                    "max_upload_bytes": self.max_upload_bytes,
                },
                fix_hint=(
                    "raise params.max_upload_bytes on the registry entry (the "
                    "adapter's parse ceiling follows it), or wire a "
                    "document-corpus source for a corpus this size"
                ),
            )
        content_sha = hashlib.sha256(content).hexdigest()
        upload_id = _upload_id(origin, content_sha)
        staged_name = f"{upload_id}-{safe_name}"
        staged = StagedUpload(
            upload_id=upload_id,
            origin_kind=origin_kind,
            origin=origin,
            staged_name=staged_name,
            content_sha256=content_sha,
            byte_count=len(content),
            uploaded_by=uploaded_by or "",
            uploaded_at=(now or datetime.now(timezone.utc)).isoformat(),
            media_type=media_type,
        )
        self.content_dir.mkdir(parents=True, exist_ok=True)
        self.manifest_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)
        # Blob first, sidecar second: `list_uploads` keys off sidecars and
        # skips one whose blob is missing, so a crash between the two
        # leaves an unreferenced blob (reported as an orphan) rather than
        # a sidecar promising bytes that do not exist.
        _atomic_write_bytes(self.content_dir / staged_name, content, self.tmp_dir)
        _atomic_write_bytes(
            self.manifest_dir / f"{upload_id}.json",
            json.dumps(staged.to_sidecar(), sort_keys=True, indent=2).encode("utf-8"),
            self.tmp_dir,
        )
        return staged


def _atomic_write_bytes(path: Path, payload: bytes, tmp_dir: Path) -> None:
    """Write via a temp file plus rename so a reader never sees a partial blob.

    ``tmp_dir`` must be outside the corpus root: a temp file left behind
    by a crash inside ``content/`` would be ingested as a document.
    """
    tmp = tmp_dir / f"{path.name}.partial"
    tmp.write_bytes(payload)
    os.replace(tmp, path)


def _url_basename(url: str, media_type: str) -> str:
    """Derive a file name for a fetched URL.

    Unlike `stage_file`, this name is DERIVED rather than operator-typed,
    so normalizing it is not hiding an operator's intent.

    The RESPONSE's declared media type wins over the URL path's
    extension. The extension selects the parser lane, and the path is
    chosen by whoever wrote the link: a URL ending ``.pdf`` that serves
    HTML would otherwise route into the PDF lane (and, on a deployment
    with captioning or table transcription enabled, into a model call).
    The path extension is used only when the media type is absent or
    unrecognized.
    """
    parsed = urlparse(url)
    name = PurePosixPath(parsed.path or "").name
    stem, dot, ext = name.rpartition(".")
    declared = _CONTENT_TYPE_EXTENSIONS.get(media_type.lower())
    if declared:
        base = (stem if dot else name) or (parsed.hostname or "fetched").replace(
            ".", "_",
        )
        return f"{base}{declared}"
    if not name:
        return f"{(parsed.hostname or 'fetched').replace('.', '_')}.txt"
    if not dot:
        return f"{name}.txt"
    return name


def staged_document_node_id(repo: str, staged_name: str) -> str:
    """The ``document`` node id the next ingest publishes for a staged blob.

    Exists so a caller that must NAME the node before it is published --
    the external-document-loader breadcrumb
    (`okg.substrate.chat.document_loader`) is the only one today -- states
    the real id instead of a plausible-looking guess. A breadcrumb whose
    node id does not resolve is worse than no breadcrumb: it reads as a
    working pointer and returns nothing.

    Two facts make this derivable rather than a duplicate implementation:

    * `DocCorpusSource` builds a document id as
      ``self._canonicalize(f"doc:{self.repo}:{rel_str}")`` and ``rel_str``
      is the blob's path relative to the corpus root, which for this
      source's staging layout is exactly the staged name (the content
      directory IS the corpus root, and staging writes no subdirectories).
    * `UploadSource._delegate` constructs that delegate with NO
      ``canonicalizers``, so ``_canonicalize`` is the identity.

    Both are asserted end to end rather than trusted: the test suite
    stages a file, runs `UploadSource.run`, and compares the emitted
    document's ``node_id`` to this function's output. If either fact
    changes, that comparison fails.
    """
    return f"doc:{repo}:{staged_name}"


# ----------------------------------------------------------- registration


@dataclass(frozen=True)
class UploadRegistration:
    """A deployment's declared, classified upload source."""

    deployment: str
    source_id: str
    ownership_id: str
    staging_dir: Path
    extractors_dir: Path
    sensitivity: str
    data_classification: str
    store_raw: bool
    policy_origin: str
    max_upload_bytes: int = DEFAULT_MAX_UPLOAD_BYTES
    #: The registry entry's ``params.repo``, which is the ``doc:<repo>:``
    #: segment of every document id this source publishes. Carried on the
    #: registration so a caller naming a node id reads the SAME value the
    #: adapter is constructed with, instead of assuming the default.
    repo: str = "upload"

    def staging(self) -> UploadStaging:
        return UploadStaging(
            self.staging_dir, max_upload_bytes=self.max_upload_bytes,
        )

    def declared_policy(self) -> dict[str, Any]:
        return {
            "sensitivity": self.sensitivity,
            "data_classification": self.data_classification,
            "store_raw": self.store_raw,
            "policy_origin": self.policy_origin,
        }


def _upload_entries(registry: Mapping[str, Any]) -> list[tuple[str, Mapping[str, Any]]]:
    sources = registry.get("sources")
    if not isinstance(sources, Mapping):
        return []
    out: list[tuple[str, Mapping[str, Any]]] = []
    for source_id, entry in sorted(sources.items()):
        if not isinstance(entry, Mapping):
            continue
        if (
            str(entry.get("module") or "") == UPLOAD_SOURCE_MODULE
            and str(entry.get("class") or "") == UPLOAD_SOURCE_CLASS
        ):
            out.append((str(source_id), entry))
    return out


def resolve_upload_registration(
    deployment: str,
    *,
    registry_path: Path | str,
    manifest: Mapping[str, Any],
) -> UploadRegistration:
    """Resolve the deployment's registered, classified upload source.

    Fails closed in both directions. No entry naming
    ``UPLOAD_SOURCE_MODULE``/``UPLOAD_SOURCE_CLASS`` raises
    `UploadSourceNotRegistered`; an entry whose Nomos source policy does
    not declare sensitivity, data classification, and store_raw raises
    `UploadSourceUnclassified`. Neither refusal has a fallback: the
    caller's only correct response is to say the deployment has no
    upload path.

    Two "same reading" rules make this resolution agree with the ingest
    path instead of being a second, looser interpretation of the same
    YAML:

    * the registry is parsed by `load_source_registry`, so
      ``${repo_root}`` / ``${source_registry_dir}`` /
      ``${environment_data_root}`` and every other placeholder expand
      EXACTLY as they do when the adapter is constructed. A path
      resolved here therefore cannot be a different directory from the
      one the source reads -- the divergence that would let an upload
      report success while landing where nothing ingests it.
    * the classification is resolved by
      `okg.substrate.nomos.audit_source_privacy_policy`, the audit the
      ingest path itself runs.
    """
    from okg.substrate.nomos.deployment_audit import audit_source_privacy_policy
    from okg.substrate.sources.registry import load_source_registry

    location = str(registry_path)
    try:
        loaded = load_source_registry(registry_path)
    except FileNotFoundError as exc:
        raise UploadSourceNotRegistered(
            f"deployment {deployment!r} has no source registry at {location}, "
            f"so it registers no upload source",
            data={"deployment": deployment, "source_registry": location},
        ) from exc
    registry = {
        "sources": {name: entry.raw for name, entry in loaded.items()},
    }
    entries = _upload_entries(registry)
    if not entries:
        raise UploadSourceNotRegistered(
            f"deployment {deployment!r} registers no upload source, so it has "
            f"no upload path",
            data={
                "deployment": deployment,
                "source_registry": location,
                "required_module": UPLOAD_SOURCE_MODULE,
                "required_class": UPLOAD_SOURCE_CLASS,
            },
            fix_hint=(
                f"add a source to {location} with module "
                f"{UPLOAD_SOURCE_MODULE} and class {UPLOAD_SOURCE_CLASS}, "
                f"including a Nomos source policy declaring "
                f"{', '.join(REQUIRED_POLICY_FIELDS)}"
            ),
        )
    if len(entries) > 1:
        raise UploadSourceUnclassified(
            f"deployment {deployment!r} registers {len(entries)} upload "
            f"sources; the upload affordance targets exactly one",
            data={
                "deployment": deployment,
                "source_ids": [source_id for source_id, _ in entries],
                "source_registry": location,
            },
            fix_hint="keep one upload source per deployment",
        )
    source_id, entry = entries[0]

    report = audit_source_privacy_policy(source_id, entry, manifest=manifest)
    policy = report.get("source_policy") or {}
    missing = [f for f in REQUIRED_POLICY_FIELDS if policy.get(f) is None]
    if not report.get("ok") or missing:
        raise UploadSourceUnclassified(
            f"upload source {source_id!r} on deployment {deployment!r} has no "
            f"declared Nomos classification, so uploads are refused",
            data={
                "deployment": deployment,
                "source_id": source_id,
                "missing_fields": missing,
                "findings": report.get("findings", []),
                "source_registry": location,
            },
            fix_hint=(
                f"declare {', '.join(REQUIRED_POLICY_FIELDS)} on the entry's "
                f"source_policy, or in the deployment manifest under "
                f"nomos.source_policy_defaults"
            ),
        )

    params = entry.get("params")
    params = params if isinstance(params, Mapping) else {}
    base_dir = Path(registry_path).expanduser().resolve().parent
    staging_dir = _required_dir(
        params.get("staging_dir"),
        base_dir=base_dir,
        field="staging_dir",
        deployment=deployment,
        source_id=source_id,
    )
    extractors_dir = _required_dir(
        params.get("extractors_dir") or str(base_dir),
        base_dir=base_dir,
        field="extractors_dir",
        deployment=deployment,
        source_id=source_id,
    )
    return UploadRegistration(
        deployment=deployment,
        source_id=source_id,
        ownership_id=str(entry.get("ownership_id") or source_id),
        staging_dir=staging_dir,
        extractors_dir=extractors_dir,
        sensitivity=str(policy["sensitivity"]),
        data_classification=str(policy["data_classification"]),
        store_raw=bool(policy["store_raw"]),
        policy_origin=str(policy.get("policy_origin") or "unknown"),
        max_upload_bytes=int(
            params.get("max_upload_bytes") or DEFAULT_MAX_UPLOAD_BYTES
        ),
        # Same default as `UploadSource.__init__`, read from the same
        # `params` the adapter factory hands the constructor.
        repo=str(params.get("repo") or "upload"),
    )


_UNEXPANDED_PLACEHOLDER = re.compile(r"\$\{[^}]*\}")


def _required_dir(
    raw: Any,
    *,
    base_dir: Path,
    field: str,
    deployment: str,
    source_id: str,
) -> Path:
    """Take a directory the registry loader has already expanded.

    The loader owns placeholder expansion; this only rejects what it
    could not expand and anchors a relative path to the registry's own
    directory. An unexpanded ``${...}`` is REFUSED rather than treated as
    a literal directory name -- a literal ``${repo_root}`` directory
    would be created happily by the staging write and then never read by
    the adapter, which is a success report over a silent divergence.
    """
    text = str(raw or "").strip()
    if not text:
        raise UploadSourceUnclassified(
            f"upload source {source_id!r} on deployment {deployment!r} "
            f"declares no {field}",
            data={"deployment": deployment, "source_id": source_id},
            fix_hint=f"set params.{field} on the registry entry",
        )
    leftover = _UNEXPANDED_PLACEHOLDER.search(text)
    if leftover:
        raise UploadSourceUnclassified(
            f"upload source {source_id!r} on deployment {deployment!r} has an "
            f"unexpanded placeholder {leftover.group(0)} in {field}",
            data={
                "deployment": deployment,
                "source_id": source_id,
                "field": field,
                "value": text,
            },
            fix_hint=(
                "use a placeholder the source registry expands "
                "(${source_registry_dir}, ${deployment_dir}, ${repo_root}, "
                "${environment_data_root}) or an absolute path"
            ),
        )
    path = Path(text).expanduser()
    return path if path.is_absolute() else (base_dir / path)


def upload_source_registry_entry(
    *,
    ownership_id: str,
    staging_dir: str = "${deployment_dir}/state/uploads",
    extractors_dir: str = "${deployment_dir}",
    repo: str = "upload",
    sensitivity: str = "local_private",
    data_classification: str = "private",
    store_raw: bool = True,
    max_upload_bytes: int = DEFAULT_MAX_UPLOAD_BYTES,
) -> dict[str, Any]:
    """The canonical ``source_registry.yaml`` entry for this source.

    Kept in code rather than as a prose snippet so the shape is exercised
    by the same loader, the same Nomos audit, and the same admission
    signature evaluation a deployment uses. The defaults describe
    operator-supplied local content: private, kept on the deployment's
    own disk, never exported.

    The ``output_signature`` must cover EVERY record the delegate emits,
    not just the obvious document/chunk pair: admission rejects the whole
    payload on the first record matching no rule
    (``producer_node_subtype_not_allowed`` /
    ``producer_edge_triple_not_allowed``). The delegate additionally
    emits ``document_chunk member_of document`` on every chunk,
    ``document_chunk references document`` for resolved markdown links,
    and ``entity_mention`` nodes with ``member_of`` edges whenever the
    deployment's ``extractors.yaml`` declares an NER backend -- which the
    default ``extractors_dir`` shares with the rest of the deployment.
    ``document derived_from source_file`` is declared with
    ``dst_subtype_optional`` because this source has no file-walking
    predecessor and so never actually asserts it, but a deployment that
    adds one must not be rejected for it. Cross-source ``entity_mention
    mentions <anything>`` edges are NOT declared because they require
    alias resolution, which requires a ``dsn`` this adapter does not
    take -- if that ever changes, the triples have to be declared here
    first.
    """
    return {
        "module": UPLOAD_SOURCE_MODULE,
        "class": UPLOAD_SOURCE_CLASS,
        "ownership_id": ownership_id,
        "params": {
            "repo": repo,
            "staging_dir": staging_dir,
            "extractors_dir": extractors_dir,
            "max_upload_bytes": max_upload_bytes,
        },
        "source_class": "discovery_crawl",
        "record_identity_kind": "scoped_locator",
        "record_identity_fields": ["repo", "path"],
        "source_revision_kind": "content_hash",
        "deletion_semantics": "missing_from_completed_scope",
        "publication_mode": "published_generation",
        "source_policy": {
            "sensitivity": sensitivity,
            "data_classification": data_classification,
            "store_raw": store_raw,
            "credential_ref_policy": "none",
            "live_call_allowed": False,
            "exportability": "no_export",
            "retention": "source_defined",
            "provenance": "operator_declared",
            "privacy_obligations": ["audit", "no_export"],
        },
        "admission_policy": {
            "producer_id": ownership_id,
            "producer_kind": "source",
            "trust_label": "operator-uploaded",
            "admission_mode": "fast_track",
            "allowed_record_kinds": ["node", "edge"],
            "output_signature": {
                "nodes": [
                    {
                        "subtype": "document",
                        "id_prefix": f"doc:{repo}:",
                        "clause_id": f"{ownership_id}.document",
                    },
                    {
                        "subtype": "document_chunk",
                        "id_prefix": "chunk:",
                        "clause_id": f"{ownership_id}.document_chunk",
                    },
                    {
                        "subtype": "entity_mention",
                        "id_prefix": "mention:",
                        "clause_id": f"{ownership_id}.entity_mention",
                    },
                ],
                "edges": [
                    {
                        "src_subtype": "document",
                        "edge_type": "contains",
                        "dst_subtype": "document_chunk",
                        "src_id_prefix": f"doc:{repo}:",
                        "dst_id_prefix": "chunk:",
                        "clause_id": f"{ownership_id}.document_contains_chunk",
                    },
                    {
                        "src_subtype": "document_chunk",
                        "edge_type": "member_of",
                        "dst_subtype": "document",
                        "src_id_prefix": "chunk:",
                        "dst_id_prefix": f"doc:{repo}:",
                        "clause_id": f"{ownership_id}.chunk_member_of_document",
                    },
                    {
                        "src_subtype": "document_chunk",
                        "edge_type": "references",
                        "dst_subtype": "document",
                        "src_id_prefix": "chunk:",
                        "dst_id_prefix": f"doc:{repo}:",
                        "clause_id": f"{ownership_id}.chunk_references_document",
                    },
                    {
                        "src_subtype": "entity_mention",
                        "edge_type": "member_of",
                        "dst_subtype": "document_chunk",
                        "src_id_prefix": "mention:",
                        "dst_id_prefix": "chunk:",
                        "clause_id": f"{ownership_id}.mention_member_of_chunk",
                    },
                    {
                        "src_subtype": "document",
                        "edge_type": "derived_from",
                        "dst_subtype": "source_file",
                        "src_id_prefix": f"doc:{repo}:",
                        "dst_id_prefix": "file:",
                        "dst_subtype_optional": True,
                        "clause_id": (
                            f"{ownership_id}.document_derived_from_source_file"
                        ),
                    },
                ],
            },
            "policy_clauses": [
                {
                    "id": f"{ownership_id}.document",
                    "effect": "allow",
                    "record_kind": "node",
                    "subtype": "document",
                },
                {
                    "id": f"{ownership_id}.document_chunk",
                    "effect": "allow",
                    "record_kind": "node",
                    "subtype": "document_chunk",
                },
                {
                    "id": f"{ownership_id}.entity_mention",
                    "effect": "allow",
                    "record_kind": "node",
                    "subtype": "entity_mention",
                },
                {
                    "id": f"{ownership_id}.document_contains_chunk",
                    "effect": "allow",
                    "record_kind": "edge",
                    "triple": ["document", "contains", "document_chunk"],
                },
                {
                    "id": f"{ownership_id}.chunk_member_of_document",
                    "effect": "allow",
                    "record_kind": "edge",
                    "triple": ["document_chunk", "member_of", "document"],
                },
                {
                    "id": f"{ownership_id}.chunk_references_document",
                    "effect": "allow",
                    "record_kind": "edge",
                    "triple": ["document_chunk", "references", "document"],
                },
                {
                    "id": f"{ownership_id}.mention_member_of_chunk",
                    "effect": "allow",
                    "record_kind": "edge",
                    "triple": ["entity_mention", "member_of", "document_chunk"],
                },
                {
                    "id": f"{ownership_id}.document_derived_from_source_file",
                    "effect": "allow",
                    "record_kind": "edge",
                    "triple": ["document", "derived_from", "source_file"],
                },
            ],
        },
        "sync": {
            "watch": {"roots": [f"{staging_dir}/content"]},
            "triggers": ["fs_event", "manual", "reconcile"],
            "default_event_mode": "scope_incomplete",
            "reconcile_mode": "scope_complete",
            "deletes_require_reconcile": True,
            "cheap_probe": "change_probe",
            "scope": {"accepts": ["path"], "unsupported_fallback": "scope_complete"},
        },
    }


# ---------------------------------------------------------------- adapter


class UploadSource:
    """Ingests the staging area as a document corpus.

    Constructed from the registry entry's ``params``. Note the absent
    ``dsn`` parameter: the adapter factory injects ``dsn`` only into
    constructors that declare it, so this source structurally cannot be
    handed a graph connection.
    """

    name: str = "upload"
    profile: str = "discovery_crawl"

    # The staged blobs and their sidecars are the whole read scope, so the
    # O(content) floor covers this source exactly: a re-upload, a removal,
    # or a sidecar correction all change the digest.
    change_probe_kind = "content_hash"

    def __init__(
        self,
        *,
        staging_dir: Path | str,
        extractors_dir: Path | str,
        repo: str = "upload",
        max_files: int = 500,
        max_bytes_per_file: Optional[int] = None,
        max_upload_bytes: int = DEFAULT_MAX_UPLOAD_BYTES,
        source_name: str = "upload",
        deployment: str | None = None,
        **_ignored: Any,
    ) -> None:
        self.repo = repo
        self.ownership_id = source_name
        self.deployment = deployment
        self.staging = UploadStaging(
            staging_dir, max_upload_bytes=int(max_upload_bytes),
        )
        self.extractors_dir = Path(extractors_dir).expanduser()
        self.max_files = int(max_files)
        # Derived, not independently defaulted. A staging ceiling above the
        # parser ceiling is the worst combination available: the upload is
        # accepted, then the delegate tags it `oversize` and emits ZERO
        # chunks, publishing a document whose content is unretrievable. The
        # two limits move together unless a deployment overrides this one
        # deliberately.
        self.max_bytes_per_file = int(
            max_bytes_per_file
            if max_bytes_per_file is not None
            else self.staging.max_upload_bytes
        )
        self.last_run_stats: dict[str, Any] = {}
        self.change_probe = self._build_change_probe()

    # -- probe ---------------------------------------------------------

    def _build_change_probe(self) -> Any:
        from okg.substrate.library.sources.content_hash_probe import (
            ContentHashProbe,
        )

        def _items() -> list[tuple[str, Any]]:
            # Re-enumerated on every call so a deleted upload drops out of
            # membership rather than lingering in a cached manifest.
            items: list[tuple[str, Any]] = [
                (f"content::{p.name}", p) for p in self.staging.content_paths()
            ]
            items.extend(
                (f"manifest::{p.name}", p) for p in self.staging.sidecar_paths()
            )
            extractors_yaml = self.extractors_dir / "extractors.yaml"
            if extractors_yaml.is_file():
                # The parser/chunker selection changes emission, so an
                # extractors edit must defeat an unchanged-corpus verdict.
                items.append((f"extractors::{extractors_yaml}", extractors_yaml))
            return items

        return ContentHashProbe(
            content_items=_items,
            config={
                "repo": self.repo,
                "staging_dir": str(self.staging.staging_dir),
                "extractors_dir": str(self.extractors_dir),
                "max_files": self.max_files,
                "max_bytes_per_file": self.max_bytes_per_file,
                "doc_corpus_emission_schema_version": (
                    _doc_corpus.DOC_CORPUS_EMISSION_SCHEMA_VERSION
                ),
            },
            emit_targets=[sys.modules[__name__], _doc_corpus],
            max_bytes_per_item=self.max_bytes_per_file,
        )

    @property
    def probe_short_circuit_safe(self) -> bool:
        """The delegate declines runner-level short-circuit acknowledgement.

        Mirroring `DocCorpusSource.probe_short_circuit_safe` rather than
        claiming a stronger guarantee than the code actually doing the
        emission.
        """
        return False

    # -- run -----------------------------------------------------------

    def _delegate(self, staged_names: Sequence[str]) -> DocCorpusSource:
        return DocCorpusSource(
            repo=self.repo,
            repo_root=self.staging.content_dir,
            extractors_dir=self.extractors_dir,
            max_files=self.max_files,
            max_bytes_per_file=self.max_bytes_per_file,
            # Scoped to blobs that HAVE a sidecar. A file in `content/`
            # with no sidecar cannot be attributed to an origin, so it
            # would publish under its staged name; an allow-list is the
            # only construction where that cannot happen by accident.
            # Also keeps OS cruft (.DS_Store) and any leftover partial
            # write out of the corpus.
            include_globs=tuple(staged_names),
            exclude_globs=("**/.*",),
            # The staging directory is not a git checkout. `non_git_walk`
            # keeps the filesystem walk when git cannot see it (the usual
            # case, or an ignored path inside a repo); `include_untracked`
            # covers a staging directory that sits untracked inside a work
            # tree, where git lists the uploads as untracked files.
            non_git_walk=True,
            include_untracked=True,
            # No dsn and no predecessor: the delegate's alias-resolution,
            # predecessor-watermark, and source_file-endpoint paths all
            # require a dsn, so with none configured this adapter reaches
            # no database. `_available_source_file_ids` returns an empty
            # set without a dsn, so no dangling `derived_from` edge is
            # asserted either.
            dsn=None,
            deployment=self.deployment,
            predecessor_source=None,
        )

    def run(
        self,
        run_id: str,
        *,
        mode: str = "cursor",
        cursor: Optional[dict[str, Any]] = None,
        sync_scope: Optional[Mapping[str, Any]] = None,
        **_ignored: Any,
    ) -> SourceRun:
        uploads = self.staging.by_staged_name()
        completed_scope = mode in {"reconcile", "scope_complete"}
        orphans = [
            path.name
            for path in self.staging.content_paths()
            if path.name not in uploads and not path.name.startswith(".")
        ]
        if orphans:
            # A blob with no sidecar cannot be attributed to an origin, so
            # it is excluded rather than published under its staged name.
            # Loud, because the usual cause is a crashed or out-of-band
            # write, not a normal state.
            log.warning(
                "upload staging: %d blob(s) with no sidecar are excluded from "
                "ingest: %s",
                len(orphans),
                sorted(orphans)[:10],
            )

        if not uploads:
            # Nothing uploaded is an honest empty run. It is NOT the same
            # as an upload producing zero rows, which the PACT's publish
            # proof treats as a failure; the stats below record which of
            # the two happened.
            #
            # An EMPTY record set (not None) is what makes the last
            # upload's removal retract: the runner gates all retraction on
            # `record_set is not None`, so returning None here would leave
            # the rows of a deleted upload published forever while the
            # cursor still claimed them.
            self.last_run_stats = {
                "staged_uploads": 0,
                "run_mode": mode,
                "orphan_blobs": len(orphans),
                "restamp_misses": 0,
                "scope_status": "empty_staging_area",
            }
            return SourceRun(
                facts=iter(()),
                completed_scope=completed_scope,
                next_cursor=dict(cursor) if isinstance(cursor, dict) else None,
                run_mode=mode,
                record_set=RecordSet(records={}) if completed_scope else None,
            )

        delegate = self._delegate(sorted(uploads))
        inner = delegate.run(
            run_id,
            mode=mode,
            cursor=cursor,
            sync_scope=dict(sync_scope) if sync_scope else None,
        )
        stats: dict[str, Any] = {
            "staged_uploads": len(uploads),
            "run_mode": mode,
            "orphan_blobs": len(orphans),
            "restamp_misses": 0,
            "doc_corpus": dict(delegate.last_run_stats),
        }
        self.last_run_stats = stats
        # Every field other than `facts` is passed through: the delegate
        # owns cursor authority and the record set, and the restamping
        # below changes no node id, so its retraction diff stays valid.
        return SourceRun(
            facts=self._restamp(inner.facts, uploads, stats),
            completed_scope=inner.completed_scope,
            next_cursor=inner.next_cursor,
            applied_raw_token=inner.applied_raw_token,
            applied_token_scope=inner.applied_token_scope,
            applied_token_authority=inner.applied_token_authority,
            run_mode=inner.run_mode,
            record_set=inner.record_set,
            record_set_replacement_keys=inner.record_set_replacement_keys,
            # Passing this through is what keeps the delegate's no-change
            # short-circuit protected; dropping it would hand the runner an
            # empty stream that looks like an emptied corpus (issue #2122).
            preserves_prior_record_set=inner.preserves_prior_record_set,
            asserts_empty_scope=inner.asserts_empty_scope,
            health=inner.health,
            # The delegate's file image values resolve against its root.
            image_root=inner.image_root,
        )

    def _restamp(
        self,
        facts: Iterable[Any],
        uploads: Mapping[str, StagedUpload],
        stats: dict[str, Any],
    ) -> Iterator[Any]:
        """Give every citation-bearing node the upload's real identity.

        The delegate names a document after its staged path
        (``<upload_id>-<name>``) and copies that citation onto each of its
        chunks. No operator recognizes either. Since chunks are the unit
        search returns, restamping documents alone would leave the
        unrecognizable name on exactly the rows a reader sees.

        The rewrite is keyed on the delegate's own citation label
        (``<repo>/<staged path>``) rather than on emission order, so a
        chunk is matched wherever it appears in the stream. Written:
        ``title`` (documents), ``citation_label``, and ``citation_url``
        for a URL upload -- the one case where a truthful link exists.
        Only attributes the extraction ontology already declares are
        touched, so the shared Document/DocumentChunk subtypes are
        unchanged.

        Per-row uploader attribution (who staged it) is deliberately NOT
        written: it would need a new attribute on subtypes shared by every
        extraction deployment. The uploader is recorded in the staging
        sidecar, which is this source's operator state.

        A miss is counted into ``last_run_stats['restamp_misses']``: a row
        published under its staged blob name is a defect, and it must be
        countable rather than invisible.
        """
        by_label = {
            f"{self.repo}/{staged_name}": staged
            for staged_name, staged in uploads.items()
        }
        for fact in facts:
            if not isinstance(fact, NodeFact):
                yield fact
                continue
            if fact.subtype == "document":
                staged = uploads.get(str(fact.attrs.get("path") or ""))
                if staged is None:
                    stats["restamp_misses"] = stats.get("restamp_misses", 0) + 1
                    log.warning(
                        "upload: document %s has no staged upload for path %r; "
                        "it keeps its staged blob name",
                        fact.node_id,
                        fact.attrs.get("path"),
                    )
                    yield fact
                    continue
            else:
                staged = by_label.get(str(fact.attrs.get("citation_label") or ""))
                if staged is None:
                    # Chunks deduplicate across documents and mentions carry
                    # no citation, so a non-match here is ordinary, not a
                    # defect. Only documents are counted as misses.
                    yield fact
                    continue
            attrs = dict(fact.attrs)
            if fact.subtype == "document":
                # The origin IS the name an operator recognizes: the file
                # they picked, or the URL they pasted.
                attrs["title"] = staged.origin
            attrs["citation_label"] = staged.origin
            if staged.origin_kind == "url":
                attrs["citation_url"] = staged.origin
            else:
                # A local upload has no browser view; a guessed link is
                # worse than none.
                attrs.pop("citation_url", None)
            yield NodeFact(
                node_id=fact.node_id,
                subtype=fact.subtype,
                attrs=attrs,
                source_record_id=fact.source_record_id,
                source_revision=fact.source_revision,
                op=fact.op,
                ownership_mode=fact.ownership_mode,
            )
