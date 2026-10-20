"""Opaque, context-bound continuation tokens for MCP graph reads.

The token body is readable but not forgeable.  It deliberately contains only
snapshot and projection coordinates plus one-way authority evidence; raw Nomos
subjects, identity claims, access context, and policy material never cross the
MCP boundary.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import zlib
from collections.abc import Mapping
from typing import Any

import psycopg

from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.session import Session
from okg.substrate.nomos.store import load_active_policy_bundle


_TOKEN_VERSION = 1
_ENV_SIGNING_KEY = "OKG_MCP_CONTINUATION_SIGNING_KEY"


class ContinuationTokenInvalid(SubstrateError, ValueError):
    """A continuation token cannot be trusted in the current read context."""

    code = "continuation_token_invalid"


MAX_ENCODED_TOKEN_CHARS = 16_384
MAX_DECODED_TOKEN_BYTES = 65_536

def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _b64encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _b64decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _signing_key() -> bytes:
    configured = os.getenv(_ENV_SIGNING_KEY)
    if configured:
        return hashlib.sha256(configured.encode("utf-8")).digest()
    return _PROCESS_SIGNING_KEY


_PROCESS_SIGNING_KEY = secrets.token_bytes(32)


def signing_key_id() -> str:
    """Return a non-secret identifier for the active continuation key."""

    return hashlib.sha256(_signing_key()).hexdigest()[:16]


#: Claim fields the authenticated header boundary stamps anew on every
#: request. They are not a different principal or permission context.
_REQUEST_STAMPS = frozenset({"verified_at", "auth_time"})


def _without_request_stamps(claim: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in dict(claim).items() if key not in _REQUEST_STAMPS}


def authority_fingerprint(
    conn: psycopg.Connection,
    session: Session,
) -> tuple[str, str | None]:
    """Return a sanitized authority digest and active Nomos policy pin."""

    bundle = load_active_policy_bundle(
        conn,
        deployment_name=session.deployment_name,
        scope=f"deployment:{session.deployment_name}",
    )
    policy_fingerprint = bundle.policy_fingerprint if bundle is not None else None
    identity_claim = session.nomos_identity_claim
    if session.nomos_multi_user and identity_claim is not None:
        # The authenticated header boundary stamps these anew on every request.
        # Authorization still evaluates the fresh claim before each read; its
        # observation time is not a different principal or permission context.
        identity_claim = _without_request_stamps(identity_claim)
    authority = {
        "subject": session.nomos_subject,
        "identity_claim": identity_claim,
        "access_context": session.nomos_access_context,
        "multi_user": session.nomos_multi_user,
        "policy_fingerprint": policy_fingerprint,
    }
    return hashlib.sha256(_canonical(authority)).hexdigest(), policy_fingerprint


def reader_key(
    authority: Mapping[str, Any] | None,
    header_claim: Mapping[str, Any] | None,
    multi_user: bool,
) -> str:
    """Who reads, from inputs known before any session opens (#3222, H3).

    A sha256 over the canonical ``authority`` argument, the authenticated
    header claim without its per-request stamps (``verified_at`` and
    ``auth_time``, as in :func:`authority_fingerprint`), and ``multi_user``.
    The MCP session, the Nomos subject and the policy pin are not in it: a
    result reference compares those in its full binding after the session
    opens.

    An omitted and an empty ``authority`` are the same reader. Server-owned
    ``_nomos*`` keys are dropped first, by the same rule as
    ``server._strip_server_owned_authority``: the server writes them, so a
    caller-planted one must not make a different reader. The rest of the
    authority is hashed whole: narrowing it to identity fields could merge
    readers whose classification differs.
    """

    claim = None if header_claim is None else _without_request_stamps(header_claim)
    reader = {
        "authority": {
            key: value
            for key, value in dict(authority or {}).items()
            if not str(key).startswith("_nomos")
        },
        "header_claim": claim,
        "multi_user": bool(multi_user),
    }
    return hashlib.sha256(_canonical(reader)).hexdigest()


#: The claim fields that name one authenticated person: who vouched for the
#: identity and who it names. Everything else in a claim (expiry, roles and
#: their order, login method, the transport that relayed it, extra header
#: claims) can change between one person's requests (#3222 review MED).
_PRINCIPAL_FIELDS: tuple[str, ...] = ("provider_id", "issuer", "external_subject")


def principal_key(
    header_claim: Mapping[str, Any] | None,
    multi_user: bool,
) -> str | None:
    """The authenticated person a multi-user reader acts as, or None.

    A sha256 over the claim's ``provider_id``, ``issuer`` and
    ``external_subject`` only, the fields both the OIDC and the header
    request paths set (``oidc_auth.oidc_claim_for_request``,
    ``principal_mapping.header_claim_for_request``). So a token refresh,
    reordered roles, another login method or another relaying transport is
    still the same person. None on a single-user server or without a claim:
    there is no principal to cap (#3222, rrcap A). A multi-user claim that
    lacks one of the three refuses ``result_reference_internal``; it never
    falls back to the whole claim. Unlike :func:`reader_key` it ignores the
    ``authority`` argument, so one person's reader keys share one cap.
    """

    if not multi_user or header_claim is None:
        return None
    principal = {name: header_claim.get(name) for name in _PRINCIPAL_FIELDS}
    missing = [
        name for name, value in principal.items()
        if not isinstance(value, str) or not value.strip()
    ]
    if missing:
        from okg.substrate.mcp.result_references import ResultReferenceInternal

        raise ResultReferenceInternal(
            "a multi-user identity claim does not name its principal",
            data={"reason": "principal_unnamed", "missing": missing},
            fix_hint="report this server bug",
        )
    return hashlib.sha256(_canonical(principal)).hexdigest()


def continuation_binding(
    conn: psycopg.Connection,
    session: Session,
    *,
    kind: str,
    subtype: str | None = None,
    path: list[str] | None = None,
    sort: str,
    projection: Any,
) -> dict[str, Any]:
    """Build the complete immutable context a cursor is allowed to resume."""

    authority_digest, policy_fingerprint = authority_fingerprint(conn, session)
    return {
        "kind": kind,
        "deployment": session.deployment_name,
        "database": session.db_fingerprint,
        "branch": session.branch_name,
        "generation": session.generation_id,
        "catalog_version": session.catalog_version_id,
        "subtype": subtype,
        "path": path,
        "sort": sort,
        "projection": projection,
        "authority": authority_digest,
        "policy": policy_fingerprint,
        "key_id": signing_key_id(),
    }


def continuation_body_bytes(binding: Mapping[str, Any], cursor: Mapping[str, Any]) -> int:
    """Canonical decoded state size, including the complete token envelope."""
    return len(_canonical({"v": _TOKEN_VERSION, "binding": dict(binding), "cursor": dict(cursor)}))


def issue_continuation(
    binding: Mapping[str, Any],
    *,
    cursor: Mapping[str, Any],
) -> str:
    """Sign a deterministic cursor under an already-sanitized binding."""

    body = {
        "v": _TOKEN_VERSION,
        "binding": dict(binding),
        "cursor": dict(cursor),
    }
    encoded = _b64encode(zlib.compress(_canonical(body), level=9))
    signature = _b64encode(
        hmac.new(_signing_key(), encoded.encode("ascii"), hashlib.sha256).digest()
    )
    return f"{encoded}.{signature}"


#: Which tool continues each token kind (PACT read-model-s6-page-next,
#: design `read-model` section 3.5). A token's kind names the read it
#: resumes, and `next` on that tool routes by it. `grep_page` (held on
#: branch claude/mcp-grep-page, decision D12) routes to `search` when it
#: lands: add it here and accept `next` on `search` in the same change.
NEXT_ROUTES: dict[str, str] = {
    "inspect_text": "inspect",
    "inspect_text_batch": "inspect",
    "inspect_node_batch": "inspect",
    "inspect_subtype": "inspect",
    "expand_path": "expand",
    # Keyset pages past the offset cap (#3666 step 3b).
    "filter_page": "filter",
    "map_page": "map",
}

#: Binding parts a refusal may name, in order. The authority digest and the
#: policy pin are both reported as `authority`: the caller learns that who
#: reads, or the policy that governs the read, changed, never which part.
_NAMED_BINDING_FIELDS: tuple[tuple[str, str], ...] = (
    ("kind", "kind"),
    ("deployment", "deployment"),
    ("database", "database"),
    ("branch", "branch"),
    ("generation", "generation"),
    ("catalog_version", "catalog"),
    ("subtype", "target"),
    ("path", "target"),
    ("sort", "order"),
    ("projection", "target"),
    ("authority", "authority"),
    ("policy", "authority"),
    ("key_id", "signing key"),
)

_REMEDY = "restart the read without a continuation token"


def _invalid(message: str, reason: str, **data: Any) -> ContinuationTokenInvalid:
    return ContinuationTokenInvalid(
        message,
        data={"reason": reason, **data},
        fix_hint=_REMEDY,
    )


def _decode_body(encoded: str) -> Any:
    inflater = zlib.decompressobj()
    decoded = inflater.decompress(_b64decode(encoded), MAX_DECODED_TOKEN_BYTES)
    if inflater.unconsumed_tail or not inflater.eof:
        raise ValueError("compressed continuation exceeds its bound")
    return json.loads(decoded)


def _claimed_key_id(encoded: str) -> Any:
    """The signing-key id an UNVERIFIED body claims, or None.

    Read only to word a signature refusal: a body that decodes and names a
    key other than the current one was most likely signed before a restart
    (the per-process key is regenerated) or a key change. Nothing else is
    read from an unverified body.
    """

    try:
        body = _decode_body(encoded)
    except (ValueError, TypeError, zlib.error):
        return None
    binding = body.get("binding") if isinstance(body, dict) else None
    return binding.get("key_id") if isinstance(binding, dict) else None


def open_continuation(token: Any) -> dict[str, Any]:
    """Verify ``token``'s form, signature and version, and return its body.

    The binding is NOT compared with a read context here: a caller routes
    by ``body["binding"]["kind"]``, and the operator it reaches compares the
    whole binding in :func:`consume_continuation`.
    """

    if not isinstance(token, str) or len(token) > MAX_ENCODED_TOKEN_CHARS or token.count(".") != 1:
        raise _invalid("the continuation token is malformed", "malformed")
    encoded, supplied_signature = token.split(".", 1)
    expected_signature = _b64encode(
        hmac.new(_signing_key(), encoded.encode("ascii"), hashlib.sha256).digest()
    )
    if not hmac.compare_digest(supplied_signature, expected_signature):
        claimed = _claimed_key_id(encoded)
        if isinstance(claimed, str) and claimed != signing_key_id():
            raise _invalid(
                "the continuation token was signed with a key this server no "
                "longer holds: the server restarted or its signing key changed",
                "signing_key_changed",
            )
        raise _invalid(
            "the continuation token signature is invalid", "signature_invalid",
        )
    try:
        body = _decode_body(encoded)
    except (ValueError, TypeError, json.JSONDecodeError, zlib.error) as exc:
        raise _invalid(
            "the continuation token payload is invalid", "payload_invalid",
        ) from exc
    if not isinstance(body, dict) or body.get("v") != _TOKEN_VERSION:
        raise _invalid(
            "the continuation token version is not supported", "version_invalid",
        )
    if not isinstance(body.get("binding"), dict):
        raise _invalid(
            "the continuation token payload is invalid", "payload_invalid",
        )
    return body


def mismatched_fields(
    supplied: Mapping[str, Any], expected: Mapping[str, Any],
) -> list[str]:
    """The named binding parts that differ, in a fixed order, each once."""

    names: list[str] = []
    for key, name in _NAMED_BINDING_FIELDS:
        if _canonical(supplied.get(key)) != _canonical(expected.get(key)):
            if name not in names:
                names.append(name)
    if set(supplied) ^ set(expected) and "target" not in names:
        names.append("target")
    return names


def consume_continuation(
    token: Any,
    *,
    expected_binding: Mapping[str, Any],
) -> dict[str, Any]:
    """Verify ``token`` and return its cursor or a typed safe refusal.

    A refusal says what failed: the form, the signature (a token signed
    under a key this server no longer holds says so, `signing_key_changed`),
    or which bound parts of the read context differ (`mismatched`: kind,
    generation, catalog, target, order, authority, signing key).
    """

    body = open_continuation(token)
    supplied = body["binding"]
    if not hmac.compare_digest(
        _canonical(supplied),
        _canonical(dict(expected_binding)),
    ):
        mismatched = mismatched_fields(supplied, expected_binding)
        data: dict[str, Any] = {
            "pinned_generation_id": expected_binding.get("generation"),
            "authority_fingerprint": expected_binding.get("authority"),
            "mismatched": mismatched,
        }
        kind = supplied.get("kind")
        if "kind" in mismatched and isinstance(kind, str):
            data["token_kind"] = kind
        named = ", ".join(mismatched) if mismatched else "context"
        raise _invalid(
            "the continuation token belongs to a different read context "
            f"({named} differ{'s' if len(mismatched) == 1 else ''})",
            "binding_mismatch",
            **data,
        )
    cursor = body.get("cursor")
    if not isinstance(cursor, dict):
        raise _invalid(
            "the continuation token has no usable cursor", "cursor_invalid",
        )
    return dict(cursor)


_SEAL_CONTEXT = b"okg-continuation-seal-v1"


def _seal_key() -> bytes:
    return hmac.new(_signing_key(), _SEAL_CONTEXT, hashlib.sha256).digest()


#: Bytes of each `fixed` integer in a sealed cursor (unsigned, big-endian).
_SEAL_FIXED_BYTES = 8
#: The key, inside the compressed cursor, naming its fixed integers in order.
_SEAL_FIXED_KEY = "_fixed"


def seal_cursor(
    cursor: Mapping[str, Any], *, fixed: Mapping[str, int] | None = None,
) -> str:
    """Encrypt a cursor whose coordinates must not be readable (AES-GCM under
    a key derived from the signing key; compressed first). A grep cursor
    counts rows hidden from the reader, so it travels sealed.

    `fixed` integers travel after the compressed cursor, `_SEAL_FIXED_BYTES`
    bytes each, so the sealed length never depends on their values: a
    compressed number's length grows with its digits (#3384)."""

    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    body = dict(cursor)
    tail = b""
    if fixed:
        names = sorted(fixed)
        body[_SEAL_FIXED_KEY] = names
        tail = b"".join(
            int(fixed[name]).to_bytes(_SEAL_FIXED_BYTES, "big", signed=False)
            for name in names
        )
    nonce = secrets.token_bytes(12)
    sealed = AESGCM(_seal_key()).encrypt(
        nonce, zlib.compress(_canonical(body), level=9) + tail, _SEAL_CONTEXT,
    )
    return _b64encode(nonce + sealed)


def unseal_cursor(text: Any) -> dict[str, Any]:
    """Decrypt `seal_cursor`'s output, or refuse it as a typed invalid token."""

    from cryptography.exceptions import InvalidTag
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    try:
        raw = _b64decode(text)
        inflater = zlib.decompressobj()
        plain = inflater.decompress(
            AESGCM(_seal_key()).decrypt(raw[:12], raw[12:], _SEAL_CONTEXT), 65_536,
        )
        if inflater.unconsumed_tail or not inflater.eof:
            raise ValueError("sealed cursor exceeds its bound")
        value = json.loads(plain)
        if isinstance(value, dict) and _SEAL_FIXED_KEY in value:
            names = value.pop(_SEAL_FIXED_KEY)
            tail = inflater.unused_data
            if not isinstance(names, list) or len(tail) != _SEAL_FIXED_BYTES * len(names):
                raise ValueError("sealed cursor's fixed fields do not match")
            for k, name in enumerate(names):
                chunk = tail[k * _SEAL_FIXED_BYTES: (k + 1) * _SEAL_FIXED_BYTES]
                value[str(name)] = int.from_bytes(chunk, "big", signed=False)
    except (InvalidTag, ValueError, TypeError, zlib.error, json.JSONDecodeError) as exc:
        raise ContinuationTokenInvalid(
            "the continuation token's sealed cursor is invalid",
            data={"reason": "cursor_invalid"},
            fix_hint="restart the read without a continuation token",
        ) from exc
    if not isinstance(value, dict):
        raise ContinuationTokenInvalid(
            "the continuation token's sealed cursor is invalid",
            data={"reason": "cursor_invalid"},
            fix_hint="restart the read without a continuation token",
        )
    return value


def route_next(token: Any, *, tool: str) -> dict[str, Any]:
    """The signed binding of a `next` token that ``tool`` continues.

    `next` is the one continuation argument (PACT read-model-s6-page-next):
    the token's kind says which read it resumes, so the caller passes
    nothing else. A token another tool continues is refused with a retry
    naming that tool; a kind no tool continues is refused by name. The
    binding returned here is verified as signed but not yet compared with a
    read context: the operator rebuilds its call from it, and
    :func:`consume_continuation` compares the two.
    """

    body = open_continuation(token)
    binding = body["binding"]
    kind = binding.get("kind")
    owner = NEXT_ROUTES.get(kind) if isinstance(kind, str) else None
    if owner is None:
        raise _invalid(
            "no operator continues this continuation token's kind",
            "kind_unroutable",
            token_kind=kind if isinstance(kind, str) else None,
        )
    if owner != tool:
        raise ContinuationTokenInvalid(
            f"this continuation token continues a {owner} read, not a {tool} read",
            data={"reason": "wrong_tool", "token_kind": kind, "tool": owner},
            fix_hint=f"call {owner}(next=<this token>)",
        )
    return binding


def expand_shape_from_hops(hops: Any) -> dict[str, Any] | None:
    """The expand arguments that read `hops` again, or None if none do.

    Uniform any-edge hops (``None``, two or more) rebuild as the depth form
    ``edge_type=None, direction, depth``. Any other hops rebuild as a typed
    ``path``, an any-edge hop spelled ``'*'`` (which the path grammar reads
    as any link), so it reads the same hops and signs the same binding
    whether the first page used ``path`` or ``depth``.
    """
    if not isinstance(hops, list) or not hops:
        return None
    pairs: list[tuple[str | None, str]] = []
    for hop in hops:
        if (
            not isinstance(hop, list)
            or len(hop) != 2
            or not (hop[0] is None or isinstance(hop[0], str))
            or hop[1] not in ("out", "in")
        ):
            return None
        pairs.append((hop[0], hop[1]))
    if pairs[0][0] is None and len(pairs) >= 2 and len(set(pairs)) == 1:
        return {
            "path": None,
            "edge_type": None,
            "direction": pairs[0][1],
            "depth": len(pairs),
        }
    return {
        "path": [
            f"{'*' if edge_type is None else edge_type}:{direction}"
            for edge_type, direction in pairs
        ],
        "edge_type": None,
        "direction": "out",
        "depth": 1,
    }


#: Stands for the caller's own token in a retry a refusal names. The token
#: itself is not echoed: it can be 16 KiB, and the refusal must fit the
#: smallest response budget.
TOKEN_PLACEHOLDER = "<this token>"


def budget_retry_call(binding: Mapping[str, Any]) -> dict[str, Any] | None:
    """The call that reads a token's next page under a NEW ``max_bytes``.

    ``next`` restores the signed budget; on expand it refuses an explicit
    one (inspect takes one since #3634, `nextresize A`), so a page that
    needs a different budget (the deployment ceiling dropped below
    the signed one, or the page does not fit it) is read by another call:

    - a path or text token: the legacy ``continuation_token`` spelling,
      which verifies the token's original read and accepts a new valid
      ``max_bytes`` for this and later pages (``mode`` ``continue``);
    - a subtype token: its budget sizes the summary sections, so the token
      binds it; the read restarts without a token (``mode`` ``restart``);
    - a batch text token: its binding signs the budget with the keyed items
      and its target has no legacy token spelling, so the batch restarts
      without a token at the pinned generation, every
      item read again (``mode`` ``restart``; ``repeat`` names the items and
      the authority).

    A continuation also names ``generation_id``: the token is pinned to the
    generation its first page read, and the legacy call without it opens the
    session default, so after a publish it would be refused
    ``binding_mismatch``. The binding keeps only a digest of the reader's
    authority, so ``authority`` cannot be rebuilt: ``repeat`` names it, and
    the caller passes the same ``authority`` the first page passed, if any.

    Returns ``{"tool", "mode", "arguments"}`` (plus ``repeat`` for a
    continuation) with ``max_bytes`` left for the caller, or None for a
    binding no call rebuilds (a path token without its hops).
    """
    kind = binding.get("kind")
    projection = binding.get("projection")
    projection = projection if isinstance(projection, Mapping) else {}
    arguments: dict[str, Any]
    if kind == "expand_path":
        shape = expand_shape_from_hops(projection.get("hops"))
        if shape is None:
            return None
        arguments = {"input": {"node_id": projection.get("start_node")}}
        if shape["path"] is not None:
            arguments["path"] = shape["path"]
        else:
            arguments.update(direction=shape["direction"], depth=shape["depth"])
        for key in ("target_subtype", "limit", "select"):
            if projection.get(key) is not None:
                arguments[key] = projection[key]
        arguments["generation_id"] = binding.get("generation")
        arguments["continuation_token"] = TOKEN_PLACEHOLDER
        return {
            "tool": "expand",
            "mode": "continue",
            "arguments": arguments,
            "repeat": ["authority"],
        }
    if kind == "inspect_text":
        text_target = {"kind": "text", "node_id": projection.get("node_id")}
        if projection.get("lines") is not None:
            # A line read's token continues those lines only.
            text_target["lines"] = projection["lines"]
        text_target["continuation_token"] = TOKEN_PLACEHOLDER
        return {
            "tool": "inspect",
            "mode": "continue",
            "arguments": {
                "target": text_target,
                "generation_id": binding.get("generation"),
            },
            "repeat": ["authority"],
        }
    if kind == "inspect_subtype":
        target = {"kind": "subtype", "subtype": binding.get("subtype")}
        for key in ("detail", "page_size"):
            if projection.get(key) is not None:
                target[key] = projection[key]
        return {"tool": "inspect", "mode": "restart", "arguments": {"target": target}}
    if kind == "inspect_text_batch":
        # A batch token signs its budget with the items (PACT read-text-batch),
        # and the published `texts` target has no `continuation_token`
        # spelling, so no call continues it under a new budget: `next`
        # refuses max_bytes. Restart the same keyed items at the
        # pinned generation, where their copied locations were taken. The
        # items are not echoed (eight keys and ids can outgrow the smallest
        # response budget); `repeat` asks for them, as for the authority.
        return {
            "tool": "inspect",
            "mode": "restart",
            "arguments": {
                "target": {"kind": "texts"},
                "generation_id": binding.get("generation"),
            },
            "repeat": ["target.items", "authority"],
        }
    return None


def render_retry_call(call: Mapping[str, Any], max_bytes: str) -> str:
    """``call`` spelled as one tool call, with ``max_bytes`` appended."""
    parts = [
        f"{name}={json.dumps(value)}"
        for name, value in call["arguments"].items()
    ]
    parts.append(f"max_bytes={max_bytes}")
    text = f"{call['tool']}({', '.join(parts)})"
    return text.replace(json.dumps(TOKEN_PLACEHOLDER), TOKEN_PLACEHOLDER)
