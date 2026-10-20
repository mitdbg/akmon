"""Intent digest: what an approval binds to.

Canonical JSON of the reviewed fields only. Evidence and decisions are
excluded, so recording proof or an approval never changes the digest.
"""

from __future__ import annotations

import hashlib
import json

from pact.schema import Manifest

INTENT_FIELDS = ("id", "title", "tier", "why", "affected_code", "requirements", "tasks")


def intent_payload(manifest: Manifest) -> dict:
    data = manifest.model_dump(mode="json")
    return {key: data[key] for key in INTENT_FIELDS}


def intent_digest(manifest: Manifest) -> str:
    raw = json.dumps(intent_payload(manifest), sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()


def file_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
