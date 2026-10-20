"""The external handle: an in-attrs typed block describing how to fetch a node's
external/live state, validated by generic code that never branches on
deployment name.

A node's attrs MAY carry a reserved ``_external`` block mapping a backed-field
name to a field spec::

    attrs["_external"] = {
      "metrics": {
        "current": "latest_from_series",      # or "probe"
        "series": {"store": "timescaledb", "metric": "gpu_util"},
        "ttl_seconds": 60,
        "cache_policy": "cache-first",
        "size": "inline",
        "access": {"permission": "external.probe.invoke",
                   "secret_refs": ["env:PROBE_TOKEN"]},
        "adapter": {"kind": "sql", "config": {...}},   # required for probe
        "default_hydration": "opt_in"          # or "always"
      }
    }

Validation is loud (raises ``ExternalHandleError``); there are no silent
fallbacks. The handle never carries secret VALUES — only references.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field as dataclass_field
from typing import Any, Mapping, Optional

HANDLE_ATTR = "_external"

CURRENT_BACKINGS = frozenset({"latest_from_series", "probe"})
CACHE_POLICIES = frozenset({"direct", "cache-first", "stale-allowed", "never-cache"})
SIZE_POLICIES = frozenset({"inline", "slice", "stream"})
HYDRATION_MODES = frozenset({"opt_in", "always"})
ADAPTER_KINDS = frozenset({"sql", "command", "http", "mcp", "python"})

# Keys that, with an inline value inside a secret-ref entry, mean an embedded
# literal credential instead of a reference.
_SECRET_VALUE_KEYS = frozenset(
    {"value", "secret", "token", "password", "credential", "key"}
)


class ExternalHandleError(ValueError):
    """A handle is malformed. Raised loudly; never a fallback."""

    def __init__(self, field_name: str, message: str) -> None:
        self.field_name = field_name
        super().__init__(f"external handle field {field_name!r}: {message}")


@dataclass(frozen=True)
class ExternalField:
    """One backed field of a node."""

    field: str
    current: str  # latest_from_series | probe
    series: Mapping[str, Any]
    ttl_seconds: int
    timeout_seconds: int
    cache_policy: str
    size: str
    permission: Optional[str]
    secret_refs: tuple[str, ...]
    adapter: Mapping[str, Any]
    default_hydration: str
    raw: Mapping[str, Any] = dataclass_field(default_factory=dict, compare=False, repr=False)

    @property
    def requires_privilege(self) -> bool:
        return self.permission is not None


@dataclass(frozen=True)
class ExternalHandle:
    """The full handle: backed-field name -> spec."""

    fields: Mapping[str, ExternalField]


def _validate_secret_refs(field_name: str, raw_refs: Any) -> tuple[str, ...]:
    if raw_refs is None:
        return ()
    if not isinstance(raw_refs, (list, tuple)):
        raise ExternalHandleError(field_name, "access.secret_refs must be a list")
    refs: list[str] = []
    for entry in raw_refs:
        if isinstance(entry, Mapping):
            embedded = sorted(set(entry) & _SECRET_VALUE_KEYS)
            if embedded:
                raise ExternalHandleError(
                    field_name,
                    f"access.secret_refs embeds an inline secret value via "
                    f"key(s) {embedded}; declare a reference name, not the value",
                )
            entry = entry.get("ref") or entry.get("name")
            if not entry:
                raise ExternalHandleError(
                    field_name, "access.secret_refs mapping needs a 'ref' name"
                )
        if not isinstance(entry, str) or not entry.strip():
            raise ExternalHandleError(
                field_name, "access.secret_refs entries must be non-empty strings"
            )
        if "=" in entry:
            raise ExternalHandleError(
                field_name,
                f"access.secret_refs entry {entry!r} looks like an inline "
                "KEY=VALUE credential; declare a reference name only",
            )
        refs.append(entry)
    return tuple(refs)


def _parse_field(name: str, spec: Any) -> ExternalField:
    if not isinstance(spec, Mapping):
        raise ExternalHandleError(name, "must be a mapping")

    current = spec.get("current")
    if current not in CURRENT_BACKINGS:
        raise ExternalHandleError(
            name, f"current {current!r} not in {sorted(CURRENT_BACKINGS)}"
        )

    series = spec.get("series") or {}
    if not isinstance(series, Mapping):
        raise ExternalHandleError(name, "series must be a mapping")
    if current == "latest_from_series" and not series.get("metric"):
        raise ExternalHandleError(
            name, "current=latest_from_series requires series.metric"
        )

    adapter = spec.get("adapter") or {}
    if not isinstance(adapter, Mapping):
        raise ExternalHandleError(name, "adapter must be a mapping")
    if current == "probe":
        kind = adapter.get("kind")
        if kind not in ADAPTER_KINDS:
            raise ExternalHandleError(
                name,
                f"current=probe requires adapter.kind in {sorted(ADAPTER_KINDS)}",
            )

    ttl = spec.get("ttl_seconds", 60)
    if not isinstance(ttl, int) or isinstance(ttl, bool) or ttl <= 0:
        raise ExternalHandleError(name, "ttl_seconds must be a positive integer")

    # Query/fetch timeout is SEPARATE from the freshness TTL: a long-lived
    # metric (large ttl_seconds) must not yield an equally long statement
    # timeout that can hang the inspect connection.
    timeout = spec.get("timeout_seconds", 10)
    if not isinstance(timeout, int) or isinstance(timeout, bool) or timeout <= 0:
        raise ExternalHandleError(name, "timeout_seconds must be a positive integer")

    cache_policy = spec.get("cache_policy", "cache-first")
    if cache_policy not in CACHE_POLICIES:
        raise ExternalHandleError(
            name, f"cache_policy {cache_policy!r} not in {sorted(CACHE_POLICIES)}"
        )

    size = spec.get("size", "inline")
    if size not in SIZE_POLICIES:
        raise ExternalHandleError(
            name, f"size {size!r} not in {sorted(SIZE_POLICIES)}"
        )

    default_hydration = spec.get("default_hydration", "opt_in")
    if default_hydration not in HYDRATION_MODES:
        raise ExternalHandleError(
            name,
            f"default_hydration {default_hydration!r} not in {sorted(HYDRATION_MODES)}",
        )

    access = spec.get("access") or {}
    if not isinstance(access, Mapping):
        raise ExternalHandleError(name, "access must be a mapping")
    permission = access.get("permission")
    if permission is not None and not isinstance(permission, str):
        raise ExternalHandleError(name, "access.permission must be a string")
    secret_refs = _validate_secret_refs(name, access.get("secret_refs"))

    return ExternalField(
        field=name,
        current=current,
        series=dict(series),
        ttl_seconds=int(ttl),
        timeout_seconds=int(timeout),
        cache_policy=cache_policy,
        size=size,
        permission=permission,
        secret_refs=secret_refs,
        adapter=dict(adapter),
        default_hydration=default_hydration,
        raw=dict(spec),
    )


def parse_handle(attrs: Mapping[str, Any] | None) -> Optional[ExternalHandle]:
    """Parse + validate a node's external handle from its attrs.

    Returns None when the node carries no handle; raises ``ExternalHandleError``
    when a handle is present but malformed. No deployment-name branch.
    """
    if not attrs or HANDLE_ATTR not in attrs:
        return None
    block = attrs[HANDLE_ATTR]
    if not isinstance(block, Mapping) or not block:
        raise ExternalHandleError(
            HANDLE_ATTR, "must be a non-empty mapping of field -> spec"
        )
    fields = {name: _parse_field(name, spec) for name, spec in block.items()}
    return ExternalHandle(fields=fields)


def backing_fingerprint(spec: ExternalField) -> str:
    """A stable fingerprint of a field's BACKING, for the cache key.

    Two branches with divergent handles for the same node produce different
    fingerprints, so their cached probes never collide.
    """
    payload = {
        "current": spec.current,
        "series": spec.series,
        "adapter": spec.adapter,
        "permission": spec.permission,
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, default=str).encode()
    ).hexdigest()
    return digest[:16]
