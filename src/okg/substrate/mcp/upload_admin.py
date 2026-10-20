"""The chat-side upload affordance, behind the write/admin MCP surface.

Per PACT `chat-upload-source`, task `chat-affordance`. Two questions this
module answers, and it answers them the same way in both directions:

1. **Does this deployment have an upload path at all?**
   `provisioned_conditional_tools` reads the deployment's
   ``source_registry.yaml`` and reports ``{"upload"}`` only when a
   registered, Nomos-classified upload source resolves. The server uses
   that answer to decide whether the tool is REGISTERED -- so on a
   deployment without the registration the tool is physically absent
   from ``tools/list``, not present-and-refusing.

2. **What happens to a direct attempt anyway?** `upload_to_staging`
   re-resolves the registration itself and returns a structured refusal
   naming the missing registration. The absence check and the refusal
   share one resolver, so the tool cannot be absent for one reason and
   refuse for another.

Refusals are returned dicts, not exceptions, matching the surrounding
source-admin tools: an unparseable error reads to an agent as "the graph
has nothing", which is the silent-failure mode this repo treats as P0.
Configuration errors that make the server itself wrong still raise.
"""

from __future__ import annotations

import base64
import binascii
import logging
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

from okg.substrate import fastyaml
from okg.substrate.library.sources.upload import (
    UnsafeFetchURL,
    UploadRegistration,
    UploadRejected,
    UploadSourceNotRegistered,
    UploadSourceUnavailable,
    resolve_upload_registration,
)


log = logging.getLogger(__name__)

__all__ = [
    "CONDITIONAL_TOOL_UPLOAD",
    "load_upload_registration",
    "provisioned_conditional_tools",
    "upload_to_staging",
]

#: The one conditional admin tool this module provisions.
CONDITIONAL_TOOL_UPLOAD = "upload"


def _default_deployment_dir(deployment: str) -> Path:
    from okg.substrate.mcp.source_admin import _deployment_dir

    return _deployment_dir(deployment)


def load_upload_registration(
    deployment: str,
    *,
    deployment_dir_resolver: Callable[[str], Path] | None = None,
) -> UploadRegistration:
    """Read the deployment's registry + manifest and resolve the upload source.

    Raises `UploadSourceNotRegistered` when the deployment directory,
    the registry, or the entry is absent -- all three mean the same
    thing operationally (no governed upload path here) and none of them
    may degrade into using some other store.
    """
    resolver = deployment_dir_resolver or _default_deployment_dir
    try:
        dep_dir = resolver(deployment)
    except FileNotFoundError as exc:
        raise UploadSourceNotRegistered(
            f"deployment {deployment!r} was not found, so it registers no "
            f"upload source",
            data={"deployment": deployment, "error": str(exc)},
        ) from exc
    registry_path = dep_dir / "source_registry.yaml"
    if not registry_path.is_file():
        raise UploadSourceNotRegistered(
            f"deployment {deployment!r} has no source_registry.yaml at "
            f"{registry_path}, so it registers no upload source",
            data={"deployment": deployment, "source_registry": str(registry_path)},
        )
    manifest_path = dep_dir / "deployment.yaml"
    manifest: Mapping[str, Any] = {}
    if manifest_path.is_file():
        loaded = fastyaml.safe_load(manifest_path.read_text()) or {}
        if isinstance(loaded, Mapping):
            manifest = loaded
    return resolve_upload_registration(
        deployment,
        registry_path=registry_path,
        manifest=manifest,
    )


def conditional_tool_provisioning(
    deployment: str,
    *,
    deployment_dir_resolver: Callable[[str], Path] | None = None,
) -> dict[str, Any]:
    """Resolve conditional-tool provisioning AND why anything is missing.

    Deny by default: any resolution failure leaves the tool unprovisioned,
    so a broken or absent registration removes the affordance rather than
    exposing a tool whose backing source does not exist.

    The REASON travels with the verdict. "Absent because this deployment
    never registered one" and "absent because the registry is malformed"
    are the same tool listing but very different operational facts, and a
    log line nobody reads is not a place to keep the difference. Callers
    stamp `reasons` onto the server so a fleet probe can tell them apart.
    """
    reasons: dict[str, dict[str, str]] = {}
    provisioned: set[str] = set()
    try:
        load_upload_registration(
            deployment, deployment_dir_resolver=deployment_dir_resolver,
        )
        provisioned.add(CONDITIONAL_TOOL_UPLOAD)
    except UploadSourceUnavailable as exc:
        # An expected, declared refusal: no registration, or one whose
        # Nomos classification is undeclared.
        reasons[CONDITIONAL_TOOL_UPLOAD] = {
            "code": str(getattr(exc, "code", "upload_source_unavailable")),
            "detail": str(exc),
        }
        log.info(
            "upload tool not provisioned for deployment %r: %s", deployment, exc,
        )
    except Exception as exc:  # noqa: BLE001 - surface shaping must not crash the server build.
        # NOT an expected refusal — a malformed registry, an import
        # failure, a bug. Deny by default is still right, but the code
        # says `provisioning_error` so it is distinguishable from an
        # honest absence.
        reasons[CONDITIONAL_TOOL_UPLOAD] = {
            "code": "provisioning_error",
            "detail": f"{type(exc).__name__}: {exc}",
        }
        log.warning(
            "upload tool not provisioned for deployment %r: unexpected "
            "registration read failure (%s: %s)",
            deployment,
            type(exc).__name__,
            exc,
        )
    return {"provisioned": frozenset(provisioned), "reasons": reasons}


def provisioned_conditional_tools(
    deployment: str,
    *,
    deployment_dir_resolver: Callable[[str], Path] | None = None,
) -> frozenset[str]:
    """The provisioned set alone; see `conditional_tool_provisioning`."""
    return conditional_tool_provisioning(
        deployment, deployment_dir_resolver=deployment_dir_resolver,
    )["provisioned"]


def _refusal(exc: UploadSourceUnavailable, deployment: str) -> dict[str, Any]:
    data = dict(getattr(exc, "data", None) or {})
    return {
        "status": str(getattr(exc, "code", "upload_source_unavailable")),
        "tool": CONDITIONAL_TOOL_UPLOAD,
        "deployment": deployment,
        "summary": str(exc),
        "registration": {
            "source_registry": data.get("source_registry"),
            "required_module": data.get("required_module"),
            "required_class": data.get("required_class"),
            "missing_fields": data.get("missing_fields"),
        },
        "fix_hint": str(getattr(exc, "fix_hint", "") or ""),
    }


def _parseable_check(registration: UploadRegistration, staged_name: str) -> str | None:
    """Return a refusal reason when nothing would parse this upload.

    A document the deployment's extractors cannot parse is emitted with a
    `no_parser` warning and ZERO chunks — a published document whose
    content is not retrievable and no error anywhere. That is the
    zero-output silent failure this repo treats as P0, so the upload is
    refused at the door instead. Returns None when a parser exists.

    Fails CLOSED: if the extractor bundle cannot be read at all, the
    upload is refused rather than accepted on the hope that it parses.
    """
    from okg.substrate.library.extraction.registry import load_extractors
    from okg.substrate.library.sources.doc_corpus import (
        DEFAULT_EXTENSION_TO_FORMAT,
    )

    suffix = Path(staged_name).suffix.lower()
    fmt = DEFAULT_EXTENSION_TO_FORMAT.get(suffix)
    if fmt is None:
        return (
            f"no document format is registered for the {suffix or '(none)'} "
            f"extension, so this upload would publish a document with no "
            f"retrievable content"
        )
    try:
        bundle = load_extractors(registration.extractors_dir)
    except Exception as exc:  # noqa: BLE001 - a bundle we cannot read is a refusal.
        return (
            f"the deployment's extractor configuration at "
            f"{registration.extractors_dir} could not be read "
            f"({type(exc).__name__}: {exc}), so whether this upload would "
            f"parse is unknown"
        )
    parsers = getattr(bundle, "parsers", None) or {}
    if fmt not in parsers:
        return (
            f"the deployment registers no parser for format {fmt!r} "
            f"(available: {sorted(parsers)}), so this upload would publish a "
            f"document with no retrievable content"
        )
    return None


def upload_to_staging(
    deployment: str,
    *,
    uploaded_by: str,
    filename: Optional[str] = None,
    content_base64: Optional[str] = None,
    url: Optional[str] = None,
    deployment_dir_resolver: Callable[[str], Path] | None = None,
) -> dict[str, Any]:
    """Stage one operator upload for the deployment's upload source.

    Exactly one of (``filename`` + ``content_base64``) or ``url`` is
    accepted. The bytes land in the source's staging area with the
    registration's declared classification echoed back, and become graph
    content on the next ingest and publish cycle -- there is no chat-side
    copy and no separate retrieval store.
    """
    try:
        registration = load_upload_registration(
            deployment, deployment_dir_resolver=deployment_dir_resolver,
        )
    except UploadSourceUnavailable as exc:
        return _refusal(exc, deployment)

    has_file = bool(filename) or bool(content_base64)
    if has_file == bool(url):
        return {
            "status": "invalid_request",
            "tool": CONDITIONAL_TOOL_UPLOAD,
            "deployment": deployment,
            "summary": (
                "supply either filename + content_base64, or url — not both "
                "and not neither"
            ),
        }
    if has_file and not (filename and content_base64):
        return {
            "status": "invalid_request",
            "tool": CONDITIONAL_TOOL_UPLOAD,
            "deployment": deployment,
            "summary": "a file upload needs both filename and content_base64",
        }

    staging = registration.staging()
    try:
        if url:
            staged = staging.stage_url(url=url, uploaded_by=uploaded_by)
        else:
            try:
                content = base64.b64decode(str(content_base64), validate=True)
            except (binascii.Error, ValueError) as exc:
                return {
                    "status": "invalid_request",
                    "tool": CONDITIONAL_TOOL_UPLOAD,
                    "deployment": deployment,
                    "summary": f"content_base64 is not valid base64 ({exc})",
                }
            staged = staging.stage_file(
                filename=str(filename),
                content=content,
                uploaded_by=uploaded_by,
            )
    except UploadRejected as exc:
        return {
            "status": "rejected",
            "tool": CONDITIONAL_TOOL_UPLOAD,
            "deployment": deployment,
            "summary": str(exc),
            "detail": dict(getattr(exc, "data", None) or {}),
            "fix_hint": str(getattr(exc, "fix_hint", "") or ""),
        }
    except UnsafeFetchURL as exc:
        return {
            "status": "refused_unsafe_url",
            "tool": CONDITIONAL_TOOL_UPLOAD,
            "deployment": deployment,
            "summary": str(exc),
            "fix_hint": (
                "supply a publicly routable http(s) URL; private, loopback, "
                "link-local, and cloud-metadata addresses are refused"
            ),
        }

    unparseable = _parseable_check(registration, staged.staged_name)
    if unparseable is not None:
        # Un-stage before refusing: leaving the bytes behind would let the
        # next ingest publish exactly the empty document this check exists
        # to prevent.
        staging.discard(staged)
        return {
            "status": "refused_unparseable",
            "tool": CONDITIONAL_TOOL_UPLOAD,
            "deployment": deployment,
            "origin": staged.origin,
            "summary": f"upload refused: {unparseable}",
            "fix_hint": (
                "register a parser for this format in the deployment's "
                "extractors.yaml, or upload a format it already parses"
            ),
        }

    return {
        "status": "staged",
        "tool": CONDITIONAL_TOOL_UPLOAD,
        "deployment": deployment,
        "source": registration.ownership_id,
        "upload_id": staged.upload_id,
        "origin": staged.origin,
        "origin_kind": staged.origin_kind,
        "content_sha256": staged.content_sha256,
        "byte_count": staged.byte_count,
        "classification": registration.declared_policy(),
        "summary": (
            f"staged {staged.origin!r} for source "
            f"{registration.ownership_id!r}; it becomes graph content on the "
            f"next ingest and publish cycle"
        ),
        "next": (
            f"okg ingest --deployment {deployment} "
            f"--source {registration.source_id}"
        ),
    }
