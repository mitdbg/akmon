"""Untrusted detached-object store helpers for Poros pilots."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from okg.substrate.exchange.canonical_json import loads_strict_json
from okg.substrate.exchange.errors import PorosPayloadError
from okg.substrate.exchange.hashing import sha256_b64url
from okg.substrate.exchange.payloads import validate_detached_object_bytes


class LocalPorosObjectStore:
    """Filesystem-backed object store for local Poros pilot runs.

    The store is intentionally not trusted. Callers still verify every
    fetched byte stream against the signed detached-object manifest.
    """

    def __init__(self, root: Path | str) -> None:
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def put_bytes(self, *, object_id: str, data: bytes) -> dict[str, str]:
        digest = sha256_b64url(data)
        safe_name = _safe_object_name(object_id, digest)
        path = (self.root / safe_name).resolve()
        if not _is_under(path, self.root):
            raise PorosPayloadError(
                "detached object path escapes local store",
                reason_code="invalid_object_locator",
                data={"object_id": object_id},
            )
        path.write_bytes(data)
        return {"uri": path.as_uri(), "sha256": digest}

    def fetch_bytes(self, obj: dict[str, Any]) -> bytes:
        object_id = str(obj.get("object_id") or "")
        locations = obj.get("locations")
        if not isinstance(locations, list) or not locations:
            raise PorosPayloadError(
                "detached object has no fetchable locations",
                reason_code="object_unavailable",
                data={"object_id": object_id},
            )
        last_error: PorosPayloadError | None = None
        for location in locations:
            if not isinstance(location, dict):
                continue
            uri = location.get("uri")
            if not isinstance(uri, str):
                continue
            try:
                return self._fetch_uri(uri, object_id=object_id)
            except PorosPayloadError as exc:
                last_error = exc
        if last_error is not None:
            raise last_error
        raise PorosPayloadError(
            "detached object has no supported local locations",
            reason_code="unsupported_object_locator",
            data={"object_id": object_id},
        )

    def _fetch_uri(self, uri: str, *, object_id: str) -> bytes:
        parsed = urlparse(uri)
        if parsed.scheme == "file":
            path = Path(unquote(parsed.path)).resolve()
        elif parsed.scheme == "poros-file":
            path = (self.root / unquote(parsed.netloc + parsed.path)).resolve()
        else:
            raise PorosPayloadError(
                "unsupported detached object locator scheme",
                reason_code="unsupported_object_locator",
                data={"object_id": object_id, "scheme": parsed.scheme},
            )
        if not _is_under(path, self.root):
            raise PorosPayloadError(
                "detached object locator escapes local store",
                reason_code="invalid_object_locator",
                data={"object_id": object_id},
            )
        try:
            return path.read_bytes()
        except OSError as exc:
            raise PorosPayloadError(
                "detached object is unavailable",
                reason_code="object_unavailable",
                data={"object_id": object_id, "path": str(path)},
            ) from exc


def verify_detached_objects_from_store(
    envelope: dict[str, Any],
    *,
    object_store: LocalPorosObjectStore,
) -> None:
    """Fetch and verify every non-embedded detached object in an envelope."""
    body = envelope.get("body")
    if not isinstance(body, dict):
        return
    if str(body.get("payload_mode") or envelope.get("payload_mode") or "inline") != (
        "detached"
    ):
        return
    objects = body.get("objects")
    if not isinstance(objects, list):
        return
    for obj in objects:
        if not isinstance(obj, dict):
            continue
        if isinstance(obj.get("inline_bytes_b64url"), str):
            continue
        data = object_store.fetch_bytes(obj)
        validate_detached_object_bytes(obj, data)


def load_detached_graph_body_from_store(
    envelope: dict[str, Any],
    *,
    object_store: LocalPorosObjectStore,
) -> dict[str, Any] | None:
    """Load a verified detached Poros graph body for admission/Nomos.

    The signed envelope body remains the durable manifest and receipt-bound
    body. This helper recovers the graph object named by that manifest so local
    receiver policy and admission evaluate the actual graph, not the manifest
    shell.
    """
    body = envelope.get("body")
    if not isinstance(body, dict):
        return None
    if str(body.get("payload_mode") or envelope.get("payload_mode") or "inline") != (
        "detached"
    ):
        return None
    objects = body.get("objects")
    if not isinstance(objects, list):
        return None
    for obj in objects:
        if not isinstance(obj, dict):
            continue
        if obj.get("media_type") != "application/vnd.okg.poros.graph+json":
            continue
        data = object_store.fetch_bytes(obj)
        validate_detached_object_bytes(obj, data)
        parsed = loads_strict_json(data)
        if not isinstance(parsed, dict):
            raise PorosPayloadError(
                "detached Poros graph body must be a JSON object",
                reason_code="invalid_detached_graph_body",
                data={"object_id": obj.get("object_id")},
            )
        _validate_detached_graph_body_matches_manifest(parsed, body, obj)
        hydrated = dict(parsed)
        hydrated["payload_mode"] = "detached"
        hydrated["objects"] = list(objects)
        metadata = dict(hydrated.get("metadata") or {})
        metadata.setdefault("detached_object_id", obj.get("object_id"))
        metadata.setdefault("detached_body_sha256", obj.get("cleartext_sha256"))
        metadata.setdefault("detached_body_size_bytes", obj.get("size_bytes"))
        hydrated["metadata"] = metadata
        return hydrated
    return None


def _validate_detached_graph_body_matches_manifest(
    graph_body: dict[str, Any],
    manifest_body: dict[str, Any],
    obj: dict[str, Any],
) -> None:
    for key in [
        "graph_id",
        "origin_authority_id",
        "origin_generation_id",
        "classification",
        "graph_profile",
    ]:
        if key in manifest_body and graph_body.get(key) != manifest_body.get(key):
            raise PorosPayloadError(
                "detached Poros graph metadata does not match signed manifest",
                reason_code="detached_graph_manifest_mismatch",
                data={
                    "object_id": obj.get("object_id"),
                    "field": key,
                    "manifest": manifest_body.get(key),
                    "graph_body": graph_body.get(key),
                },
            )
    manifest_roots = list(manifest_body.get("root_node_ids") or [])
    graph_roots = list(graph_body.get("root_node_ids") or [])
    if manifest_roots != graph_roots:
        raise PorosPayloadError(
            "detached Poros graph roots do not match signed manifest",
            reason_code="detached_graph_manifest_mismatch",
            data={
                "object_id": obj.get("object_id"),
                "field": "root_node_ids",
                "manifest": manifest_roots,
                "graph_body": graph_roots,
            },
        )


def _safe_object_name(object_id: str, digest: str) -> str:
    stem = "".join(c if c.isalnum() or c in {"-", "_", "."} else "_" for c in object_id)
    stem = stem.strip("._") or "object"
    return f"{stem}-{digest}.bin"


def _is_under(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False
