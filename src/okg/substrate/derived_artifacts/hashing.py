"""Effect-free hashing primitives for derived artifacts.

This module holds the two pieces of the derived-artifact contract that
effect-free callers need but cannot reach through ``relations``, which
imports ``psycopg`` at module scope:

* :func:`stable_hash`, the canonical deterministic digest used for every
  compute, schema, renderer, dirty-rule and schedule-graph hash;
* :data:`RELATION_SLOT_NAME`, the default upstream slot name a
  dependency record binds to.

``relations`` imports both back, so each has exactly one definition and
no call site changed when they moved here. Keep this module importable
with nothing but the standard library.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

RELATION_SLOT_NAME = "relation"


def stable_hash(value: Any) -> str:
    """Return a deterministic SHA-256 hash for JSON-compatible values."""

    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
