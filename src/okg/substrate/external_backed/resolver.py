"""Read-time hydration resolver for external-backed nodes.

Given a node's attrs (which may carry a handle) and a set of requested live
fields, fetch `current` for each backed field and return a LABELED LIVE OVERLAY
- per-field state in {fresh, stale, live, unavailable, denied}, with fetched_at
and backing. `stale` carries the value past its TTL; `unavailable` means no
data. The overlay is returned SEPARATELY from the pinned attrs (it is "as of
now", not "as of generation N").

Purity: the resolver reads node_series and may UPSERT the non-Chronos
external_probe_cache; it NEVER writes graph_nodes/graph_edges/node_facts. A
privileged probe passes a deny-by-default gate BEFORE any external call; a
denied field returns state `denied` and the adapter is not invoked. String
overlay values are fenced by the existing read-path primitive.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable, Mapping, Optional

import psycopg

from okg.substrate.external_backed.gating import PROBE_PRINCIPAL_UNRESOLVED_REASON
from okg.substrate.external_backed.handle import (
    ExternalField,
    ExternalHandle,
    backing_fingerprint,
    parse_handle,
)
from okg.substrate.mcp.provenance import _fence_value

OVERLAY_SCHEMA = "okg.external_live.v1"

GateFn = Callable[[ExternalField, Optional[str]], bool]
# A probe runner takes (conn, field, node_id) and returns (status, payload, error).
ProbeRunner = Callable[..., tuple]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _requested_fields(
    handle: ExternalHandle, live_fields: Any
) -> dict[str, ExternalField]:
    """Fields to hydrate: explicitly-requested ones, plus any `always` field."""
    if live_fields is True:
        explicit: Optional[set[str]] = set(handle.fields)
    elif isinstance(live_fields, (list, tuple, set)):
        explicit = {str(f) for f in live_fields}
    else:
        explicit = None
    out: dict[str, ExternalField] = {}
    for name, spec in handle.fields.items():
        if (explicit is not None and name in explicit) or spec.default_hydration == "always":
            out[name] = spec
    return out


def _fence_payload(payload: Any, *, source: str, fence_keys: bool = False) -> Any:
    """Fence string values; leave numerics/bools alone (they carry untrusted
    provenance but need no prompt-injection fence).

    `fence_keys` neutralises KEYS as well, and defaults to False so the #83
    hydration overlay keeps the behaviour it shipped with. A backed field's
    keys are the deployment's own declared field names; a live relation's
    keys come from whatever the external system returned, and an attacker who
    controls a document controls its `_source` key names. Issue #2013's
    dispatcher passes True.

    Keys are NEUTRALISED, not fenced: wrapping a key in fence markers would
    make it unusable as a key. Control characters and the fence markers
    themselves are stripped and the key is length-bounded, so a key cannot
    close a fence it is printed next to or smuggle a newline into a prompt.
    """
    if isinstance(payload, str):
        return _fence_value(payload, source=source)
    if isinstance(payload, Mapping):
        if not fence_keys:
            return {
                k: _fence_payload(v, source=source, fence_keys=False)
                for k, v in payload.items()
            }
        # COLLISION-SAFE. Neutralisation is lossy -- "a\nb" and "a b" both
        # become "a b" -- so two distinct keys could map to one and a hostile
        # document would delete a sibling field with nothing to show for it.
        # A deterministic suffix keeps every field, and the altered keys are
        # reported so the envelope can say the shape was changed.
        out: dict[str, Any] = {}
        for key, value in payload.items():
            safe = _neutralise_key(key)
            if safe in out:
                suffix = 2
                while f"{safe}~{suffix}" in out:
                    suffix += 1
                safe = f"{safe}~{suffix}"
            out[safe] = _fence_payload(value, source=source, fence_keys=True)
        return out
    if isinstance(payload, list):
        return [
            _fence_payload(v, source=source, fence_keys=fence_keys)
            for v in payload
        ]
    return payload


#: Longest key kept from an external system. A key is an identifier, not a
#: payload; anything longer is carrying text rather than naming a field.
MAX_EXTERNAL_KEY_CHARS = 128
#: Bound on the refusal message a refused probe query carries on its field.
MAX_PROBE_REFUSAL_CHARS = 200


def _neutralise_key(key: Any) -> str:
    """Make an externally-supplied mapping key safe to print beside a fence.

    Strips the fence markers and every control character, collapses
    whitespace, and bounds the length. A key that survives this cannot close
    a fence, start a new line, or run to a paragraph.
    """
    # FENCE_OPEN is a TEMPLATE ("...source={source}..."), so the literal
    # delimiters are what a key could forge, not the formatted string.
    text = str(key)
    for marker in ("\u27e6", "\u27e7"):
        text = text.replace(marker, "")
    text = "".join(
        " " if ch.isspace() else ch
        for ch in text
        if ch.isprintable() or ch.isspace()
    )
    text = " ".join(text.split())
    if len(text) > MAX_EXTERNAL_KEY_CHARS:
        text = text[:MAX_EXTERNAL_KEY_CHARS] + "…"
    return text or "_"


def _hydrate_series(
    conn: psycopg.Connection,
    deployment_name: str,
    node_id: str,
    spec: ExternalField,
    now: datetime,
) -> dict[str, Any]:
    metric = str(spec.series.get("metric"))
    with conn.cursor() as cur:
        cur.execute(
            "SELECT value, observed_at FROM okg.node_series "
            "WHERE deployment_name=%s AND node_id=%s AND metric=%s "
            "ORDER BY observed_at DESC, sample_id DESC LIMIT 1",
            (deployment_name, node_id, metric),
        )
        row = cur.fetchone()
    base = {"backing": "latest_from_series", "trust": "untrusted", "fetched_at": now.isoformat()}
    if row is None:
        return {**base, "state": "unavailable", "error": {"reason": "no samples"}}
    value, observed_at = row
    age = (now - observed_at).total_seconds()
    if age > spec.ttl_seconds:
        # A sample exists but is past its freshness TTL. Surface it as `stale`
        # WITH the value (+ age) so the caller can decide — never discard the
        # value into a bare `unavailable` (that loses signal and is indistinct
        # from "no samples ever"). `unavailable` is reserved for "no data".
        return {
            **base,
            "state": "stale",
            "value": value,
            "age_seconds": age,
            "observed_at": observed_at.isoformat(),
        }
    return {
        **base,
        "state": "fresh",
        "value": value,
        "observed_at": observed_at.isoformat(),
    }


class _ProbeQueryRefusal(Exception):
    """Carries the named refusal out of the adapter: nothing ran, cache nothing."""

    def __init__(self, error: dict[str, Any]) -> None:
        super().__init__(error.get("code"))
        self.error = error


def _run_sql_probe(
    conn: psycopg.Connection, spec: ExternalField, node_id: str
) -> tuple[str, Optional[dict], Optional[dict]]:
    query = str(spec.adapter.get("config", {}).get("query") or "")
    if not query:
        return "error", None, {"reason": "no adapter query"}
    # The query comes from the node's handle and runs on the MCP read pool.
    # `SET TRANSACTION READ ONLY` alone is not a boundary: `COMMIT; INSERT ...`
    # ends it (review of #2401). Refuse anything but one plain SELECT BEFORE
    # it runs, and run it as a prepared statement so the driver itself
    # rejects a second statement ("cannot insert multiple commands into a
    # prepared statement") should the parser check ever miss one.
    from okg.substrate.mcp.sql_safety import ProbeQueryRefused, validate_probe_query

    try:
        validate_probe_query(query)
    except ProbeQueryRefused as exc:
        raise _ProbeQueryRefusal({
            "code": exc.code,
            "reason": exc.data.get("reason"),
            "message": exc.message[:MAX_PROBE_REFUSAL_CHARS],
        }) from exc
    timeout_ms = max(1, spec.timeout_seconds) * 1000
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("SET TRANSACTION READ ONLY")
                cur.execute(f"SET LOCAL statement_timeout = {timeout_ms}")
                cur.execute(query, prepare=True)
                cols = [d.name for d in cur.description] if cur.description else []
                r = cur.fetchone()
        return "ok", (dict(zip(cols, r)) if r is not None else {}), None
    except psycopg.errors.QueryCanceled:
        return "timeout", None, {"reason": "statement_timeout"}
    except psycopg.Error as exc:  # noqa: BLE001
        return "error", None, {"reason": str(exc).strip()}


def _hydrate_probe(
    conn: psycopg.Connection,
    deployment_name: str,
    node_id: str,
    spec: ExternalField,
    now: datetime,
    principal: Optional[str],
    gate: Optional[GateFn],
    probe_runners: Optional[Mapping[str, ProbeRunner]],
) -> dict[str, Any]:
    fingerprint = backing_fingerprint(spec)
    base = {
        "backing": "probe",
        "trust": "untrusted",
        "fetched_at": now.isoformat(),
        "backing_fingerprint": fingerprint,
    }

    # Deny-by-default gate BEFORE any external call. EVERY probe is gated: the
    # handle cannot opt out. A missing `access.permission` is NOT "ungated" — it
    # means there is no action to authorize, so the gate denies. (`permission`
    # only NAMES the action/scope.) With no gate / no active policy => denied.
    #
    # WHO the probe acts as is the caller's resolved subject and nothing else
    # (#2361). A missing subject is refused here by name, before the bundle is
    # asked: a call nobody can name is not a policy question, and it must
    # never be decided as the runtime service.
    subject = (principal or "").strip()
    if not subject:
        allowed, reason = False, PROBE_PRINCIPAL_UNRESOLVED_REASON
    else:
        allowed = bool(gate(spec, subject)) if gate is not None else False
        reason = "nomos.default_deny"
    if not allowed:
        field_overlay = {**base, "state": "denied",
                         "error": {"reason": reason, "principal": subject or None}}
        if not subject:
            # Nothing was decided, so nothing is recorded in the shared cache.
            return field_overlay
        # Recorded without clobbering: the cache key is shared by every
        # caller, so a denial must not wipe a live value an allowed caller
        # cached (review of #2401). A denied row is never served anyway.
        _write_cache(conn, deployment_name, node_id, fingerprint, spec, now,
                     status="denied", payload=None, error=field_overlay["error"],
                     keep_live=True)
        return field_overlay

    # cache-first: serve a fresh cached probe without re-running the adapter.
    if spec.cache_policy in {"cache-first", "stale-allowed"}:
        cached = _read_cache(conn, deployment_name, node_id, fingerprint, spec.field, now)
        if cached is not None:
            return {**base, **cached}

    # Run the adapter.
    kind = str(spec.adapter.get("kind") or "")
    if probe_runners and kind in probe_runners:
        status, payload, error = probe_runners[kind](conn, spec, node_id)
    elif kind == "sql":
        try:
            status, payload, error = _run_sql_probe(conn, spec, node_id)
        except _ProbeQueryRefusal as refusal:
            # Nothing ran, so there is nothing to cache: a refused query must
            # not overwrite a value another, valid handle cached.
            return {**base, "state": "unavailable", "error": refusal.error}
    else:
        status, payload, error = "unavailable", None, {"reason": f"adapter kind {kind!r} not implemented in v1"}

    fenced_payload = (
        _fence_payload(payload, source=f"external_probe:{spec.field}")
        if status == "ok" and payload is not None
        else None
    )
    _write_cache(conn, deployment_name, node_id, fingerprint, spec, now,
                 status=status, payload=fenced_payload, error=error)
    if status == "ok":
        return {**base, "state": "live", "value": fenced_payload}
    return {**base, "state": "unavailable", "error": dict(error or {"reason": status})}


def _read_cache(
    conn: psycopg.Connection,
    deployment_name: str,
    node_id: str,
    fingerprint: str,
    field: str,
    now: datetime,
) -> Optional[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT status, payload, error, expires_at FROM okg.external_probe_cache "
            "WHERE deployment_name=%s AND node_id=%s AND backing_fingerprint=%s AND field=%s",
            (deployment_name, node_id, fingerprint, field),
        )
        row = cur.fetchone()
    if row is None:
        return None
    status, payload, error, expires_at = row
    if now > expires_at:
        return None
    if status == "ok":
        return {"state": "live", "value": payload}
    if status == "denied":
        return None  # never serve a stale denial as a result; re-gate
    return {"state": "unavailable", "error": error or {"reason": status}}


def _write_cache(
    conn: psycopg.Connection,
    deployment_name: str,
    node_id: str,
    fingerprint: str,
    spec: ExternalField,
    now: datetime,
    *,
    status: str,
    payload: Optional[Any],
    error: Optional[Mapping[str, Any]],
    keep_live: bool = False,
) -> None:
    """Upsert one cache row.

    ``keep_live`` leaves an unexpired ``ok`` row in place (the upsert's
    UPDATE matches nothing), so a denial does not erase another caller's
    cached value.
    """
    from datetime import timedelta

    expires_at = now + timedelta(seconds=spec.ttl_seconds)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.external_probe_cache
              (deployment_name, node_id, backing_fingerprint, field,
               observed_at, expires_at, status, payload, error, provenance)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (deployment_name, node_id, backing_fingerprint, field)
            DO UPDATE SET observed_at=EXCLUDED.observed_at, expires_at=EXCLUDED.expires_at,
              status=EXCLUDED.status, payload=EXCLUDED.payload, error=EXCLUDED.error,
              provenance=EXCLUDED.provenance, updated_at=now()
            WHERE NOT %s
               OR okg.external_probe_cache.status <> 'ok'
               OR okg.external_probe_cache.expires_at <= EXCLUDED.observed_at
            """,
            (
                deployment_name, node_id, fingerprint, spec.field,
                now, expires_at, status,
                json.dumps(payload) if payload is not None else None,
                json.dumps(dict(error)) if error is not None else None,
                json.dumps({"adapter": spec.adapter.get("kind"), "secret_refs": list(spec.secret_refs)}),
                keep_live,
            ),
        )


def hydrate_node(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    node_id: str,
    attrs: Mapping[str, Any] | None,
    live_fields: Any,
    principal: Optional[str] = None,
    gate: Optional[GateFn] = None,
    clock: Optional[Callable[[], datetime]] = None,
    probe_runners: Optional[Mapping[str, ProbeRunner]] = None,
) -> Optional[dict[str, Any]]:
    """Hydrate the requested live fields of a node; return a labeled overlay.

    Returns None when the node has no handle or nothing is requested.
    """
    handle = parse_handle(attrs)
    if handle is None:
        return None
    now = (clock or _now)()
    requested = _requested_fields(handle, live_fields)
    if not requested:
        return None
    if gate is None:
        from okg.substrate.external_backed.gating import build_probe_gate

        gate = build_probe_gate(deployment_name)
    fields: dict[str, Any] = {}
    for name, spec in requested.items():
        if spec.current == "latest_from_series":
            fields[name] = _hydrate_series(conn, deployment_name, node_id, spec, now)
        else:
            fields[name] = _hydrate_probe(
                conn, deployment_name, node_id, spec, now, principal, gate, probe_runners
            )
    return {
        "schema": OVERLAY_SCHEMA,
        "node_id": node_id,
        "fetched_at": now.isoformat(),
        "fields": fields,
    }


def neutralised_keys(payload: Any) -> tuple[str, ...]:
    """Every key `_fence_payload(fence_keys=True)` would alter.

    Reported on the envelope so "this row's field names were rewritten" is a
    fact the caller can see, rather than a silent difference between what the
    external system returned and what the agent reads.
    """
    altered: list[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, Mapping):
            for key, value in node.items():
                if _neutralise_key(key) != str(key):
                    altered.append(str(key)[:MAX_EXTERNAL_KEY_CHARS])
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(payload)
    return tuple(sorted(set(altered)))
