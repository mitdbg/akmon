"""Postgres-backed Nomos identity resolution."""
from __future__ import annotations

import re
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Iterator, Mapping, Sequence

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from okg.substrate.commit_scope import (
    db_scope_key,
    identity_claim_dedup_token,
    memo_seen,
    memoized,
)
from okg.substrate.nomos.core import CompiledPolicyBundle, PolicyDecision
from okg.substrate.nomos.identity import (
    BLOCKED_SUBJECT,
    IdentityAuthorizationResult,
    IdentityClaimEnvelope,
    IdentityProvider,
    PermissionGrant,
    SubjectBinding,
    canonical_claim_path,
    claim_path_aliases,
    evaluate_identity_claim,
    normalize_identity_claim,
)
from okg.substrate.nomos.runtime_enforcement import (
    sanitize_policy_decision,
    sanitize_trace_mapping,
)


_PROVIDER_ON_CONFLICT_UPDATE = """
            ON CONFLICT (deployment_name, provider_id)
            DO UPDATE SET
              provider_type = EXCLUDED.provider_type,
              issuer = EXCLUDED.issuer,
              audiences = EXCLUDED.audiences,
              subject_claim = EXCLUDED.subject_claim,
              group_claims = EXCLUDED.group_claims,
              role_claims = EXCLUDED.role_claims,
              entitlement_claims = EXCLUDED.entitlement_claims,
              assurance_claim = EXCLUDED.assurance_claim,
              key_reference = EXCLUDED.key_reference,
              secret_references = EXCLUDED.secret_references,
              claim_allowlist = EXCLUDED.claim_allowlist,
              required_assurance = EXCLUDED.required_assurance,
              mfa_required = EXCLUDED.mfa_required,
              group_claims_approved = EXCLUDED.group_claims_approved,
              metadata_fingerprint = EXCLUDED.metadata_fingerprint,
              realm = EXCLUDED.realm,
              service_principal = EXCLUDED.service_principal,
              freshness_seconds = EXCLUDED.freshness_seconds,
              status = EXCLUDED.status,
              metadata = EXCLUDED.metadata,
              effective_from = EXCLUDED.effective_from,
              effective_until = EXCLUDED.effective_until,
              updated_at = now()
"""


def upsert_identity_provider(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    provider: IdentityProvider,
    replace_existing: bool = True,
) -> bool:
    """Write one provider row; return whether a row was written.

    ``replace_existing=False`` inserts only: an existing ``provider_id`` is
    left untouched and the call returns ``False``, so ``okg nomos provider
    add`` can refuse instead of overwriting a row another writer created
    between its read and its write.
    """
    on_conflict = (
        _PROVIDER_ON_CONFLICT_UPDATE
        if replace_existing
        else """
            ON CONFLICT (deployment_name, provider_id) DO NOTHING
        """
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.nomos_identity_providers (
              deployment_name, provider_id, provider_type, issuer,
              audiences, subject_claim, group_claims, role_claims,
              entitlement_claims, assurance_claim, key_reference,
              secret_references, claim_allowlist, required_assurance,
              mfa_required, group_claims_approved, metadata_fingerprint,
              realm, service_principal, freshness_seconds, status, metadata,
              effective_from, effective_until
            )
            VALUES (
              %(deployment_name)s, %(provider_id)s, %(provider_type)s,
              %(issuer)s, %(audiences)s, %(subject_claim)s,
              %(group_claims)s, %(role_claims)s, %(entitlement_claims)s,
              %(assurance_claim)s, %(key_reference)s,
              %(secret_references)s, %(claim_allowlist)s,
              %(required_assurance)s, %(mfa_required)s,
              %(group_claims_approved)s, %(metadata_fingerprint)s,
              %(realm)s, %(service_principal)s, %(freshness_seconds)s,
              %(status)s, %(metadata)s,
              %(effective_from)s, %(effective_until)s
            )
            """
            + on_conflict,
            {
                "deployment_name": deployment_name,
                "provider_id": provider.provider_id,
                "provider_type": provider.provider_type,
                "issuer": provider.issuer,
                "audiences": list(provider.audiences),
                "subject_claim": provider.subject_claim,
                "group_claims": list(provider.group_claims),
                "role_claims": list(provider.role_claims),
                "entitlement_claims": list(provider.entitlement_claims),
                "assurance_claim": provider.assurance_claim,
                "key_reference": provider.key_reference,
                "secret_references": list(provider.secret_references),
                "claim_allowlist": list(provider.claim_allowlist),
                "required_assurance": list(provider.required_assurance),
                "mfa_required": provider.mfa_required,
                "group_claims_approved": provider.group_claims_approved,
                "metadata_fingerprint": provider.metadata_fingerprint,
                "realm": provider.realm,
                "service_principal": provider.service_principal,
                "freshness_seconds": provider.freshness_seconds,
                "status": provider.status_value,
                "metadata": Jsonb(dict(provider.metadata)),
                "effective_from": provider.effective_from,
                "effective_until": provider.effective_until,
            },
        )
        return cur.rowcount > 0


def upsert_subject_binding(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    binding: SubjectBinding,
    status: str = "active",
    metadata: Mapping[str, Any] | None = None,
    replace_existing: bool = True,
) -> bool:
    """Write one binding row; return whether a row was written.

    ``replace_existing=False`` inserts only: an existing ``binding_id`` is left
    untouched and the call returns ``False``, so a caller can refuse instead
    of overwriting a row another writer created between its read and write.
    """
    on_conflict = (
        """
            ON CONFLICT (deployment_name, binding_id)
            DO UPDATE SET
              provider_id = EXCLUDED.provider_id,
              issuer = EXCLUDED.issuer,
              claim_path = EXCLUDED.claim_path,
              claim_value = EXCLUDED.claim_value,
              subject = EXCLUDED.subject,
              scope = EXCLUDED.scope,
              priority = EXCLUDED.priority,
              required_assurance = EXCLUDED.required_assurance,
              status = EXCLUDED.status,
              effective_from = EXCLUDED.effective_from,
              effective_until = EXCLUDED.effective_until,
              metadata = EXCLUDED.metadata,
              updated_at = now()
        """
        if replace_existing
        else """
            ON CONFLICT (deployment_name, binding_id) DO NOTHING
        """
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.nomos_subject_bindings (
              deployment_name, binding_id, provider_id, issuer,
              claim_path, claim_value, subject, scope, priority,
              required_assurance, status, effective_from, effective_until,
              metadata
            )
            VALUES (
              %(deployment_name)s, %(binding_id)s, %(provider_id)s,
              %(issuer)s, %(claim_path)s, %(claim_value)s, %(subject)s,
              %(scope)s, %(priority)s, %(required_assurance)s, %(status)s,
              %(effective_from)s, %(effective_until)s, %(metadata)s
            )
            """
            + on_conflict,
            {
                "deployment_name": deployment_name,
                "binding_id": binding.binding_id,
                "provider_id": binding.provider_id,
                "issuer": binding.issuer,
                "claim_path": binding.claim_path,
                "claim_value": binding.claim_value,
                "subject": binding.subject,
                "scope": binding.scope,
                "priority": binding.priority,
                "required_assurance": list(binding.required_assurance),
                "status": status,
                "effective_from": binding.effective_from,
                "effective_until": binding.effective_until,
                "metadata": Jsonb(dict(metadata or {})),
            },
        )
        return cur.rowcount > 0


# Binding ids the chat SSO group projection writes and owns
# (okg.substrate.chat.sso_groups._BINDING_PREFIX). The projection rewrites
# them on every sync, so an operator edit to one would be silently undone or,
# worse, fight the projection; the binding commands refuse them.
SSO_PROJECTION_BINDING_PREFIX = "okg:chat-sso:binding:"


class SubjectBindingError(ValueError):
    """A binding command refused; ``code`` names the refusal."""

    code = "nomos.binding.refused"


class SubjectBindingExistsError(SubjectBindingError):
    code = "nomos.binding.exists"


class SubjectBindingSubjectChangeError(SubjectBindingError):
    code = "nomos.binding.subject_change"


class SubjectBindingMatchChangeError(SubjectBindingError):
    code = "nomos.binding.match_change"


class SubjectBindingDisabledError(SubjectBindingError):
    code = "nomos.binding.disabled"


class ReservedSubjectBindingError(SubjectBindingError):
    code = "nomos.binding.reserved_id"


class SubjectBindingWindowError(SubjectBindingError):
    code = "nomos.binding.window"


class SubjectBindingBlockError(SubjectBindingError):
    code = "nomos.binding.block"


# The fields ``add_subject_binding(replace=True)`` may take from the stored
# row instead of the caller's binding: the caller left them out.
REPLACE_KEEPABLE_FIELDS = frozenset({"priority", "effective_from", "effective_until"})


def refuse_reserved_binding_id(binding_id: str) -> None:
    if binding_id.startswith(SSO_PROJECTION_BINDING_PREFIX):
        raise ReservedSubjectBindingError(
            f"binding {binding_id!r} belongs to the chat SSO group projection "
            f"(ids prefixed {SSO_PROJECTION_BINDING_PREFIX!r}), which rewrites "
            "it on every sync; change the group mapping in the chat SSO "
            "configuration instead"
        )
    if _BLOCK_BINDING_ID.match(binding_id):
        raise SubjectBindingBlockError(
            f"binding id {binding_id!r} has the form reserved for blocks "
            "(binding:<provider-id>:block:<user>); blocks are written and "
            "lifted only with `okg nomos binding block|unblock`"
        )


# `binding:<provider-id>:block:<user>`, the id `block_subject` writes. No
# admit binding may take it: `binding add` and `binding disable` refuse it,
# so the only writers of these rows are block_subject and unblock_subject.
# Blocks matter to the MCP request gate (evaluate_identity_claim). The
# external-action authority lookups in migration 0030 join bindings on a
# named worker/separation subject, which is never `nomos:blocked`, so a
# block row is never selected there; a block does not revoke external-action
# authority (the migration SQL is not edited: applied migrations are
# checksummed).
# `.+` for the provider part: provider ids may themselves contain ':'
# (binding:okta:prod:block:alice).
_BLOCK_BINDING_ID = re.compile(r"^binding:.+:block:.+$")


def _binding_event(
    *,
    at: Any,
    action: str,
    actor: str,
    reason: str | None,
    previous_status: str | None,
    before: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    event = {
        "at": _maybe_iso(at),
        "action": action,
        "actor": actor,
        "reason": reason,
        "previous_status": previous_status,
    }
    if before is not None:
        event["before"] = dict(before)
    return event


def _as_datetime(value: Any) -> Any:
    from datetime import datetime

    if value is None or isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def add_subject_binding(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    binding: SubjectBinding,
    actor: str,
    replace: bool = False,
    reason: str | None = None,
    keep_stored: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """Create a binding, or with ``replace`` change an active one's window.

    Without ``replace`` an existing ``binding_id`` is refused
    (:class:`SubjectBindingExistsError`): silently overwriting it would
    reactivate a disabled binding and erase when and why it was disabled.

    ``replace`` changes only the window (``effective_from`` /
    ``effective_until``) and the priority of an ACTIVE binding:

    - A disabled binding is refused (:class:`SubjectBindingDisabledError`).
      An add never undoes a disable; the operator adds a new binding id.
    - A different subject is refused
      (:class:`SubjectBindingSubjectChangeError`), and so is a different
      provider, issuer, claim path, claim value or scope
      (:class:`SubjectBindingMatchChangeError`). Decision records name binding
      ids; an id that came to mean another user or another subject would make
      those records say something false.
    - The stored ``required_assurance`` (for example an MFA requirement) is
      always kept. A binding that names a different, non-empty
      ``required_assurance`` is refused
      (:class:`SubjectBindingMatchChangeError`); an empty one means "not
      given", never "drop the requirement". A replace can therefore never
      weaken the assurance a binding demands.
    - Fields named in ``keep_stored`` (a subset of
      :data:`REPLACE_KEEPABLE_FIELDS`) keep their stored value instead of
      the binding's: the caller left them out. The window that results must
      still end after it starts (:class:`SubjectBindingWindowError`).

    ``keep_stored`` is ignored when no row has the id yet. The row's
    ``metadata.events`` list is appended to, never overwritten; a
    ``replaced`` event records the priority and window it replaced under
    ``before``.
    """
    import dataclasses

    unknown_keep = set(keep_stored) - REPLACE_KEEPABLE_FIELDS
    if unknown_keep:
        raise ValueError(
            f"keep_stored names fields a replace cannot keep: {sorted(unknown_keep)}"
        )
    refuse_reserved_binding_id(binding.binding_id)
    if binding.subject == BLOCKED_SUBJECT:
        raise SubjectBindingBlockError(
            f"subject {BLOCKED_SUBJECT!r} is reserved for blocks; block a user "
            "with `okg nomos binding block`"
        )
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT provider_id, issuer, claim_path, claim_value, subject,
                   scope, status, metadata, priority, required_assurance,
                   effective_from, effective_until
              FROM okg.nomos_subject_bindings
             WHERE deployment_name = %s AND binding_id = %s
               FOR UPDATE
            """,
            (deployment_name, binding.binding_id),
        )
        existing = cur.fetchone()
        cur.execute("SELECT now() AS at")
        at = cur.fetchone()["at"]
    if existing is None:
        metadata: dict[str, Any] = {
            "written_by": actor,
            "events": [
                _binding_event(
                    at=at, action="added", actor=actor, reason=reason,
                    previous_status=None,
                ),
            ],
        }
        written = upsert_subject_binding(
            conn,
            deployment_name=deployment_name,
            binding=binding,
            status="active",
            metadata=metadata,
            replace_existing=False,
        )
        if not written:
            raise SubjectBindingExistsError(
                f"subject binding {binding.binding_id!r} was created by "
                "another writer while this one ran; run `okg nomos binding "
                "list` and retry"
            )
        action, previous_status = "added", None
    else:
        previous_status = str(existing["status"])
        if not replace:
            raise SubjectBindingExistsError(
                f"subject binding {binding.binding_id!r} already exists "
                f"(status {previous_status}, subject {existing['subject']!r}); "
                "pass --replace to change an active binding's window or "
                "priority, or add a new binding id"
            )
        if previous_status == "disabled":
            raise SubjectBindingDisabledError(
                f"subject binding {binding.binding_id!r} is disabled; an add "
                "never re-enables a disabled binding. Add the access under a "
                "new binding id"
            )
        if str(existing["subject"]) != binding.subject:
            raise SubjectBindingSubjectChangeError(
                f"subject binding {binding.binding_id!r} maps to "
                f"{existing['subject']!r}; --replace cannot move it to "
                f"{binding.subject!r} because decision records name binding "
                "ids. Disable this binding and add one under a new id"
            )
        changed = [
            (field, existing[field], new_value)
            for field, new_value in (
                ("provider_id", binding.provider_id),
                ("issuer", binding.issuer),
                ("claim_path", binding.claim_path),
                ("claim_value", binding.claim_value),
                ("scope", binding.scope),
            )
            if str(existing[field]) != new_value
        ]
        if changed:
            detail = ", ".join(
                f"{field} {old!r} -> {new!r}" for field, old, new in changed
            )
            raise SubjectBindingMatchChangeError(
                f"subject binding {binding.binding_id!r}: --replace changes "
                f"only the window and priority, not {detail}. Disable this "
                "binding and add one under a new id"
            )
        stored_assurance = tuple(existing["required_assurance"] or ())
        if binding.required_assurance and tuple(
            binding.required_assurance
        ) != stored_assurance:
            raise SubjectBindingMatchChangeError(
                f"subject binding {binding.binding_id!r}: --replace keeps the "
                f"stored required_assurance {list(stored_assurance)!r} and "
                "cannot change it to "
                f"{list(binding.required_assurance)!r}. Disable this binding "
                "and add one under a new id"
            )
        before = {
            "priority": int(existing["priority"] or 0),
            "effective_from": _maybe_iso(existing["effective_from"]),
            "effective_until": _maybe_iso(existing["effective_until"]),
        }
        binding = dataclasses.replace(
            binding,
            required_assurance=stored_assurance,
            **{field: before[field] for field in keep_stored},
        )
        starts = _as_datetime(binding.effective_from)
        ends = _as_datetime(binding.effective_until)
        if starts is not None and ends is not None and ends <= starts:
            raise SubjectBindingWindowError(
                f"subject binding {binding.binding_id!r}: the window after "
                f"this replace would end ({binding.effective_until}) no later "
                f"than it starts ({binding.effective_from}); pass both "
                "--effective-from and --effective-until"
            )
        action = "replaced"
        metadata = dict(existing["metadata"] or {})
        events = list(metadata.get("events") or [])
        events.append(
            _binding_event(
                at=at, action=action, actor=actor, reason=reason,
                previous_status=previous_status, before=before,
            )
        )
        metadata["events"] = events
        metadata["written_by"] = actor
        upsert_subject_binding(
            conn,
            deployment_name=deployment_name,
            binding=binding,
            status="active",
            metadata=metadata,
        )
    rows = list_subject_bindings(
        conn, deployment_name=deployment_name, binding_id=binding.binding_id,
    )
    return {**rows[0], "action": action, "previous_status": previous_status}


def disable_subject_binding(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    binding_id: str,
    actor: str,
    reason: str | None = None,
) -> dict[str, Any] | None:
    """Disable one binding and every active binding on the same claim.

    Returns the named binding as it stands after the call, with
    ``previous_status`` and ``also_disabled`` (the other ids disabled with
    it), or ``None`` when no binding has that id. Rows are never deleted:
    audit rows name binding ids, and a deleted row would leave those names
    pointing at nothing.

    Disabling the one row alone would not cut access when a second binding
    matches the same claim: the request gate resolves the user through every
    in-effect binding, so the second one keeps admitting them, and when the
    two were equal-priority bindings to different subjects, disabling one
    would END an ambiguity refusal and admit the user. So a real disable
    also disables every other ``active`` binding with the same provider,
    issuer, scope, claim and claim value -- future-window rows included,
    so they cannot start admitting the user later. Claim paths are compared
    through the claim envelope's alias table (:func:`claim_path_aliases`):
    ``sub``, ``subject`` and the Kerberos user paths read the same claim as
    ``external_subject``, and ``role`` the same as ``roles``, so a row on an
    alias is the same user or role and is disabled too. The named row and
    its siblings are locked in one pass ordered by ``binding_id``, so two
    concurrent disables of sibling rows wait for each other instead of
    deadlocking. Rows the chat SSO group
    projection owns are left to it (it rewrites them on every sync); the
    caller reports those, and role or group bindings the user's token may
    carry, with :func:`bindings_that_may_still_match`.

    Each disabled row records ``disabled_at``, ``disabled_reason``,
    ``disabled_by`` and ``disabled_with`` (the id the operator named) and
    appends a ``disabled`` event to ``metadata.events``. Disabling a binding
    that is already disabled changes nothing, disables nothing else, and
    reports ``previous_status`` of ``disabled``.
    """
    refuse_reserved_binding_id(binding_id)
    also_disabled: list[str] = []
    match_fields = ("provider_id", "issuer", "scope", "claim_path", "claim_value")
    with conn.cursor(row_factory=dict_row) as cur:
        # Read the named row's claim WITHOUT locking it, then lock it and its
        # siblings in ONE statement ordered by binding_id. Locking the named
        # row first and its siblings second let two disables of sibling
        # bindings each hold its own row and wait for the other's: a deadlock.
        # One sorted pass takes every lock in the same order in every caller.
        cur.execute(
            """
            SELECT status, provider_id, issuer, scope, claim_path, claim_value,
                   subject
              FROM okg.nomos_subject_bindings
             WHERE deployment_name = %s AND binding_id = %s
            """,
            (deployment_name, binding_id),
        )
        row = cur.fetchone()
        if row is None:
            return None
        if row["subject"] == BLOCKED_SUBJECT:
            raise SubjectBindingBlockError(
                f"subject binding {binding_id!r} is a block; lift it with "
                "`okg nomos binding unblock`, which never disables the user's "
                "other bindings"
            )
        previous_status = str(row["status"])
        if previous_status != "disabled":
            cur.execute(
                """
                SELECT binding_id, status, provider_id, issuer, scope,
                       claim_path, claim_value
                  FROM okg.nomos_subject_bindings
                 WHERE deployment_name = %(deployment_name)s
                   AND (
                     binding_id = %(binding_id)s
                     OR (
                       status = 'active'
                       AND provider_id = %(provider_id)s
                       AND issuer = %(issuer)s
                       AND scope = %(scope)s
                       AND claim_path = ANY(%(claim_paths)s)
                       AND claim_value = %(claim_value)s
                       AND left(binding_id, %(reserved_len)s) <> %(reserved)s
                       AND subject <> %(blocked_subject)s
                     )
                   )
                 ORDER BY binding_id
                   FOR UPDATE
                """,
                {
                    "deployment_name": deployment_name,
                    "binding_id": binding_id,
                    "provider_id": row["provider_id"],
                    "issuer": row["issuer"],
                    "scope": row["scope"],
                    # `sub`, `subject` and the Kerberos user paths read the
                    # same claim as `external_subject` (and `role` as
                    # `roles`...): a row on any alias admits the same user.
                    "claim_paths": list(claim_path_aliases(str(row["claim_path"]))),
                    "claim_value": row["claim_value"],
                    "reserved": SSO_PROJECTION_BINDING_PREFIX,
                    "reserved_len": len(SSO_PROJECTION_BINDING_PREFIX),
                    # A block is never disabled as a sibling: disabling a
                    # user's admit binding must not also lift their block.
                    "blocked_subject": BLOCKED_SUBJECT,
                },
            )
            locked = cur.fetchall()
            named_rows = [
                found for found in locked if found["binding_id"] == binding_id
            ]
            if not named_rows:
                return None
            named = named_rows[0]
            if any(named[field] != row[field] for field in match_fields):
                raise SubjectBindingError(
                    f"subject binding {binding_id!r} changed its claim while "
                    "this disable ran; run `okg nomos binding list` and retry"
                )
            previous_status = str(named["status"])
            if previous_status != "disabled":
                also_disabled = [
                    str(found["binding_id"])
                    for found in locked
                    if found["binding_id"] != binding_id
                ]
        if previous_status != "disabled":
            cur.execute(
                """
                UPDATE okg.nomos_subject_bindings
                   SET status = 'disabled',
                       metadata = metadata || jsonb_build_object(
                         'disabled_at', now(),
                         'disabled_reason', %(reason)s::text,
                         'disabled_by', %(actor)s::text,
                         'disabled_with', %(disabled_with)s::text,
                         'events',
                         COALESCE(metadata -> 'events', '[]'::jsonb)
                           || jsonb_build_array(jsonb_build_object(
                                'at', now(),
                                'action', 'disabled',
                                'actor', %(actor)s::text,
                                'reason', %(reason)s::text,
                                'previous_status', status,
                                'disabled_with', %(disabled_with)s::text
                              ))
                       ),
                       updated_at = now()
                 WHERE deployment_name = %(deployment_name)s
                   AND binding_id = ANY(%(binding_ids)s)
                   AND status <> 'disabled'
                """,
                {
                    "reason": reason,
                    "actor": actor,
                    "disabled_with": binding_id,
                    "deployment_name": deployment_name,
                    "binding_ids": [binding_id, *also_disabled],
                },
            )
    rows = list_subject_bindings(
        conn, deployment_name=deployment_name, binding_id=binding_id,
    )
    return {
        **rows[0],
        "previous_status": previous_status,
        "also_disabled": also_disabled,
    }


def bindings_that_may_still_match(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    binding: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Other in-effect bindings that could admit a user ``binding`` matched.

    Disabling one binding only stops THAT row matching. The request gate
    resolves a user through every in-effect binding for the provider and
    scope, so a role, group or entitlement binding, or a second binding on
    the same claim, can still admit them -- and when the disabled row was
    one of two equal-priority bindings to different subjects, disabling it
    ends the ambiguity refusal and admits the user under the other subject.

    Returned rows carry ``match``: ``same_claim`` when the row matches
    exactly the claim ``binding`` matched (it admits the same user for
    certain), ``if_token_carries`` when it depends on what the user's token
    carries (a role/group/entitlement row, or for a disabled role binding
    any other row). Rows keyed on a DIFFERENT user's external subject cannot
    match a user binding's user and are left out. Claim paths are compared
    through the claim envelope's alias table (:func:`canonical_claim_path`),
    so a row on ``sub`` is the same claim as one on ``external_subject``.
    ``same_claim`` still admits the user only if their token meets the row's
    ``required_assurance``, which each returned row carries.
    """
    target_path = canonical_claim_path(str(binding["claim_path"])) or str(
        binding["claim_path"]
    )
    target_value = str(binding["claim_value"])
    rows = list_subject_bindings(
        conn,
        deployment_name=deployment_name,
        provider_id=str(binding["provider_id"]),
    )
    found: list[dict[str, Any]] = []
    for row in rows:
        if row["binding_id"] == binding["binding_id"]:
            continue
        if row["scope"] != binding["scope"] or not row["in_effect"]:
            continue
        if row["subject"] == BLOCKED_SUBJECT:
            continue  # a block refuses the user; it never admits them
        row_path = canonical_claim_path(str(row["claim_path"])) or str(
            row["claim_path"]
        )
        same_claim = (
            row_path == target_path
            and row["claim_value"] == target_value
            and row["issuer"] == binding["issuer"]
        )
        if (
            not same_claim
            and target_path == "external_subject"
            and row_path == "external_subject"
        ):
            continue
        found.append({
            "binding_id": row["binding_id"],
            "claim_path": row["claim_path"],
            "claim_value": row["claim_value"],
            "subject": row["subject"],
            "priority": row["priority"],
            "required_assurance": row["required_assurance"],
            "match": "same_claim" if same_claim else "if_token_carries",
        })
    return found


def block_binding_id(provider_id: str, user: str) -> str:
    return f"binding:{provider_id}:block:{user}"


#: Bound on the "was this user ever seen" check; it is advice, not a gate.
SUBJECT_VALUE_CHECK_TIMEOUT_MS = 5000
SUBJECT_VALUE_CHECK_LIMIT = 20


def subject_value_evidence(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    provider_id: str,
    user: str,
) -> dict[str, Any]:
    """Where this deployment has seen ``user`` as a provider's subject value.

    A block matches the token's subject value EXACTLY (only surrounding
    whitespace is trimmed), so ``Alice`` never blocks ``alice``. Looks for the
    exact value, and values differing only in case, in admit bindings on the
    user claim and in recorded identity claims (every sign-in the gate
    evaluates writes one). Decision rows are not scanned: they repeat the
    claims and have no index on the subject.

    Runs in its OWN read-only transaction with a statement timeout and a row
    limit, so the caller runs it after the block is committed: a slow or
    failed check can never roll back or hold the block. The caller ends the
    transaction.
    """
    user_paths = list(claim_path_aliases("external_subject"))
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SET TRANSACTION READ ONLY")
        cur.execute(
            "SELECT set_config('statement_timeout', %s, true)",
            (str(SUBJECT_VALUE_CHECK_TIMEOUT_MS),),
        )
        cur.execute(
            """
            SELECT DISTINCT value FROM (
              SELECT claim_value AS value FROM okg.nomos_subject_bindings
               WHERE deployment_name = %(d)s AND provider_id = %(p)s
                 AND claim_path = ANY(%(paths)s) AND subject <> %(blocked)s
              UNION ALL
              SELECT external_subject FROM okg.nomos_external_claims
               WHERE deployment_name = %(d)s AND provider_id = %(p)s
            ) seen
            WHERE lower(value) = lower(%(user)s)
            LIMIT %(limit)s
            """,
            {
                "d": deployment_name,
                "p": provider_id,
                "paths": user_paths,
                "blocked": BLOCKED_SUBJECT,
                "user": user,
                "limit": SUBJECT_VALUE_CHECK_LIMIT,
            },
        )
        values = sorted(str(row["value"]) for row in cur.fetchall())
    return {
        "exact_match_seen": user in values,
        "case_variants_seen": [value for value in values if value != user],
        "principal": f"external:{provider_id}:{user}",
    }


def block_subject(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    provider_id: str,
    issuer: str,
    user: str,
    scope: str,
    actor: str,
    reason: str | None = None,
) -> dict[str, Any]:
    """Block one user of one provider; the block outranks every binding.

    A block is a subject binding on the user's ``external_subject`` claim
    whose subject is :data:`BLOCKED_SUBJECT`. The request gate refuses a
    claim any in-effect block matches, before resolving a subject, so a role
    or group binding cannot re-admit the user whatever its priority. Unlike an
    admit binding, a lifted block may be re-applied under the same id: that
    only ever narrows access. Returns the row with ``previous_status`` and
    ``changed``.
    """
    binding_id = block_binding_id(provider_id, user)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT provider_id, issuer, claim_path, claim_value, subject,
                   scope, status
              FROM okg.nomos_subject_bindings
             WHERE deployment_name = %s AND binding_id = %s
               FOR UPDATE
            """,
            (deployment_name, binding_id),
        )
        existing = cur.fetchone()
        cur.execute("SELECT now() AS at")
        at = cur.fetchone()["at"]
    if existing is None:
        written = upsert_subject_binding(
            conn,
            deployment_name=deployment_name,
            binding=SubjectBinding(
                binding_id=binding_id,
                provider_id=provider_id,
                issuer=issuer,
                claim_path="external_subject",
                claim_value=user,
                subject=BLOCKED_SUBJECT,
                scope=scope,
            ),
            status="active",
            metadata={
                "written_by": actor,
                "events": [
                    _binding_event(
                        at=at, action="blocked", actor=actor, reason=reason,
                        previous_status=None,
                    ),
                ],
            },
            replace_existing=False,
        )
        if not written:
            raise SubjectBindingBlockError(
                f"block {binding_id!r} was written by another writer while "
                "this one ran; run `okg nomos binding list` and retry"
            )
        previous_status: str | None = None
    else:
        expected = {
            "provider_id": provider_id,
            "claim_path": "external_subject",
            "claim_value": user,
            "subject": BLOCKED_SUBJECT,
            "scope": scope,
        }
        wrong = sorted(
            field for field, value in expected.items() if existing[field] != value
        )
        if wrong:
            raise SubjectBindingBlockError(
                f"binding id {binding_id!r} exists but is not this block "
                f"(differs in {', '.join(wrong)}); nothing was changed"
            )
        previous_status = str(existing["status"])
        if previous_status != "active":
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE okg.nomos_subject_bindings
                       SET status = 'active',
                           metadata = metadata || jsonb_build_object(
                             'events',
                             COALESCE(metadata -> 'events', '[]'::jsonb)
                               || jsonb_build_array(jsonb_build_object(
                                    'at', now(),
                                    'action', 'blocked',
                                    'actor', %(actor)s::text,
                                    'reason', %(reason)s::text,
                                    'previous_status', status
                                  ))
                           ),
                           updated_at = now()
                     WHERE deployment_name = %(deployment_name)s
                       AND binding_id = %(binding_id)s
                    """,
                    {
                        "actor": actor,
                        "reason": reason,
                        "deployment_name": deployment_name,
                        "binding_id": binding_id,
                    },
                )
    row = list_subject_bindings(
        conn, deployment_name=deployment_name, binding_id=binding_id,
    )[0]
    return {
        **row,
        "previous_status": previous_status,
        "changed": previous_status != "active",
    }


def unblock_subject(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    provider_id: str,
    user: str,
    actor: str,
    reason: str | None = None,
) -> dict[str, Any]:
    """Lift one user's block. Only the block row changes.

    ``disable_subject_binding`` would also disable every other active binding
    on the same claim -- the user's own admit binding included -- so a block
    is lifted here, on its own row. Unknown blocks are refused.
    """
    binding_id = block_binding_id(provider_id, user)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT subject, status
              FROM okg.nomos_subject_bindings
             WHERE deployment_name = %s AND binding_id = %s
               FOR UPDATE
            """,
            (deployment_name, binding_id),
        )
        existing = cur.fetchone()
        if existing is None or existing["subject"] != BLOCKED_SUBJECT:
            raise SubjectBindingBlockError(
                f"no block for user {user!r} of provider {provider_id!r} in "
                f"deployment {deployment_name!r}; `okg nomos binding list` "
                "shows blocks (subject nomos:blocked)"
            )
        previous_status = str(existing["status"])
        if previous_status == "active":
            cur.execute(
                """
                UPDATE okg.nomos_subject_bindings
                   SET status = 'disabled',
                       metadata = metadata || jsonb_build_object(
                         'disabled_at', now(),
                         'disabled_reason', %(reason)s::text,
                         'disabled_by', %(actor)s::text,
                         'events',
                         COALESCE(metadata -> 'events', '[]'::jsonb)
                           || jsonb_build_array(jsonb_build_object(
                                'at', now(),
                                'action', 'unblocked',
                                'actor', %(actor)s::text,
                                'reason', %(reason)s::text,
                                'previous_status', status
                              ))
                       ),
                       updated_at = now()
                 WHERE deployment_name = %(deployment_name)s
                   AND binding_id = %(binding_id)s
                """,
                {
                    "actor": actor,
                    "reason": reason,
                    "deployment_name": deployment_name,
                    "binding_id": binding_id,
                },
            )
    row = list_subject_bindings(
        conn, deployment_name=deployment_name, binding_id=binding_id,
    )[0]
    return {
        **row,
        "previous_status": previous_status,
        "changed": previous_status == "active",
    }


def list_subject_bindings(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    provider_id: str | None = None,
    binding_id: str | None = None,
) -> list[dict[str, Any]]:
    """Every binding row for a deployment, disabled ones included.

    ``in_effect`` says whether the request gate would load the row and let it
    match right now: status ``active`` and its effective window open at the
    database clock. A disabled or expired binding is listed, not hidden, so an
    operator can see what was revoked and when.
    """
    where = ["deployment_name = %(deployment_name)s"]
    params: dict[str, Any] = {"deployment_name": deployment_name}
    if provider_id is not None:
        where.append("provider_id = %(provider_id)s")
        params["provider_id"] = provider_id
    if binding_id is not None:
        where.append("binding_id = %(binding_id)s")
        params["binding_id"] = binding_id
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT binding_id, provider_id, issuer, claim_path, claim_value,
                   subject, scope, priority, required_assurance, status,
                   effective_from, effective_until, metadata, created_at,
                   updated_at,
                   (status = 'active'
                    AND (effective_from IS NULL OR effective_from <= now())
                    AND (effective_until IS NULL OR effective_until > now())
                   ) AS in_effect
              FROM okg.nomos_subject_bindings
             WHERE {' AND '.join(where)}
             ORDER BY provider_id, binding_id
            """,
            params,
        )
        return [
            {
                "binding_id": row["binding_id"],
                "provider_id": row["provider_id"],
                "issuer": row["issuer"],
                "claim_path": row["claim_path"],
                "claim_value": row["claim_value"],
                "subject": row["subject"],
                "scope": row["scope"],
                "priority": int(row["priority"] or 0),
                "required_assurance": list(row["required_assurance"] or []),
                "status": row["status"],
                "in_effect": bool(row["in_effect"]),
                "effective_from": _maybe_iso(row["effective_from"]),
                "effective_until": _maybe_iso(row["effective_until"]),
                "written_by": (row["metadata"] or {}).get("written_by"),
                "disabled_by": (row["metadata"] or {}).get("disabled_by"),
                "disabled_reason": (row["metadata"] or {}).get("disabled_reason"),
                "metadata": dict(row["metadata"] or {}),
                "created_at": _maybe_iso(row["created_at"]),
                "updated_at": _maybe_iso(row["updated_at"]),
            }
            for row in cur.fetchall()
        ]


def upsert_permission_grant(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    grant: PermissionGrant,
    governing_rule_ids: Sequence[str] = (),
    status: str = "active",
    metadata: Mapping[str, Any] | None = None,
    replace_existing: bool = True,
) -> bool:
    """Write one grant row; return whether a row was written.

    ``replace_existing=False`` inserts only, like
    :func:`upsert_subject_binding`: an existing ``grant_id`` is left untouched
    and the call returns ``False``.
    """
    on_conflict = (
        _GRANT_ON_CONFLICT_UPDATE
        if replace_existing
        else """
            ON CONFLICT (deployment_name, grant_id) DO NOTHING
        """
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.nomos_permission_grants (
              deployment_name, grant_id, subject, actions, scope, effect,
              resource_match, classifications, priority,
              governing_rule_ids, status, metadata
            )
            VALUES (
              %(deployment_name)s, %(grant_id)s, %(subject)s, %(actions)s,
              %(scope)s, %(effect)s, %(resource_match)s,
              %(classifications)s, %(priority)s, %(governing_rule_ids)s,
              %(status)s, %(metadata)s
            )
            """
            + on_conflict,
            {
                "deployment_name": deployment_name,
                "grant_id": grant.grant_id,
                "subject": grant.subject,
                "actions": list(grant.actions),
                "scope": grant.scope,
                "effect": grant.effect,
                "resource_match": Jsonb(dict(grant.resource_match)),
                "classifications": list(grant.classifications),
                "priority": grant.priority,
                "governing_rule_ids": list(governing_rule_ids),
                "status": status,
                "metadata": Jsonb(dict(metadata or {})),
            },
        )
        return cur.rowcount > 0


_GRANT_ON_CONFLICT_UPDATE = """
            ON CONFLICT (deployment_name, grant_id)
            DO UPDATE SET
              subject = EXCLUDED.subject,
              actions = EXCLUDED.actions,
              scope = EXCLUDED.scope,
              effect = EXCLUDED.effect,
              resource_match = EXCLUDED.resource_match,
              classifications = EXCLUDED.classifications,
              priority = EXCLUDED.priority,
              governing_rule_ids = EXCLUDED.governing_rule_ids,
              status = EXCLUDED.status,
              metadata = EXCLUDED.metadata,
              updated_at = now()
"""


# --- `okg nomos provider add|disable|list` -----------------------------------
#
# Same rules as the binding commands above: an add never overwrites a row
# (an existing id, disabled or not, is refused, so an add can never undo a
# disable or quietly change what an id means), a disable keeps the row and
# appends to ``metadata.events``, and a list shows disabled rows too.


class IdentityProviderError(ValueError):
    """A provider command refused; ``code`` names the refusal."""

    code = "nomos.provider.refused"


class IdentityProviderExistsError(IdentityProviderError):
    code = "nomos.provider.exists"


class IdentityProviderNotFoundError(IdentityProviderError):
    code = "nomos.provider.not_found"


class IdentityProviderOidcAuthMismatchError(IdentityProviderError):
    code = "nomos.provider.oidc_auth_mismatch"


class IdentityProviderOidcAuthUndeclaredError(IdentityProviderError):
    code = "nomos.provider.oidc_auth_undeclared"


# Provider statuses that already refuse every claim; a disable leaves them as
# they are rather than rewriting a revocation as a disable.
_PROVIDER_CUT_STATUSES = frozenset({"disabled", "revoked"})


def add_identity_provider(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    provider: IdentityProvider,
    actor: str,
    reason: str | None = None,
) -> dict[str, Any]:
    """Register a new, active identity provider; refuse an existing id.

    An existing ``provider_id`` is refused whatever its status
    (:class:`IdentityProviderExistsError`): bindings and decision records
    name provider ids, so rewriting one would change what they mean, and
    re-adding a disabled one would silently undo its disable.
    """
    import dataclasses

    if provider.status_value != "active":
        raise IdentityProviderError(
            f"identity provider {provider.provider_id!r}: `add` registers an "
            f"active provider, not one with status {provider.status_value!r}"
        )
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT status, issuer
              FROM okg.nomos_identity_providers
             WHERE deployment_name = %s AND provider_id = %s
            """,
            (deployment_name, provider.provider_id),
        )
        existing = cur.fetchone()
        cur.execute("SELECT now() AS at")
        at = cur.fetchone()["at"]
    if existing is not None:
        raise IdentityProviderExistsError(
            _provider_exists_message(provider.provider_id, existing)
        )
    metadata = dict(provider.metadata)
    metadata["written_by"] = actor
    metadata["events"] = [
        _binding_event(
            at=at, action="added", actor=actor, reason=reason,
            previous_status=None,
        ),
    ]
    written = upsert_identity_provider(
        conn,
        deployment_name=deployment_name,
        provider=dataclasses.replace(provider, metadata=metadata),
        replace_existing=False,
    )
    if not written:
        raise IdentityProviderExistsError(
            f"identity provider {provider.provider_id!r} was registered by "
            "another writer while this one ran; run `okg nomos provider "
            "list` and retry"
        )
    return list_identity_providers(
        conn, deployment_name=deployment_name, provider_id=provider.provider_id,
    )[0]


def _provider_exists_message(provider_id: str, existing: Mapping[str, Any]) -> str:
    status = str(existing["status"])
    if status in _PROVIDER_CUT_STATUSES:
        return (
            f"identity provider {provider_id!r} already exists with status "
            f"{status}; an add never re-enables it. Register the provider "
            "under a new provider id"
        )
    return (
        f"identity provider {provider_id!r} already exists (status {status}, "
        f"issuer {existing['issuer']!r}); `add` never rewrites a registered "
        "provider, because bindings and decision records name it"
    )


def disable_identity_provider(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    provider_id: str,
    actor: str,
    reason: str | None = None,
) -> dict[str, Any]:
    """Disable one provider; every claim naming it is refused from then on.

    Claim normalization reads the provider's status before anything else, so
    a disabled provider turns every claim that names it into
    ``provider_disabled`` and the request is denied whatever bindings and
    grants exist. The row is kept (claims, bindings and decisions name it)
    with ``disabled_at``/``disabled_by``/``disabled_reason`` and a
    ``disabled`` event. A provider already ``disabled`` or ``revoked`` is left
    as it is and reported with ``changed`` false.
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT status
              FROM okg.nomos_identity_providers
             WHERE deployment_name = %s AND provider_id = %s
               FOR UPDATE
            """,
            (deployment_name, provider_id),
        )
        row = cur.fetchone()
        if row is None:
            raise IdentityProviderNotFoundError(
                f"no identity provider {provider_id!r} in deployment "
                f"{deployment_name!r}; `okg nomos provider list` shows them"
            )
        previous_status = str(row["status"])
        changed = previous_status not in _PROVIDER_CUT_STATUSES
        if changed:
            cur.execute(
                """
                UPDATE okg.nomos_identity_providers
                   SET status = 'disabled',
                       metadata = metadata || jsonb_build_object(
                         'disabled_at', now(),
                         'disabled_reason', %(reason)s::text,
                         'disabled_by', %(actor)s::text,
                         'events',
                         COALESCE(metadata -> 'events', '[]'::jsonb)
                           || jsonb_build_array(jsonb_build_object(
                                'at', now(),
                                'action', 'disabled',
                                'actor', %(actor)s::text,
                                'reason', %(reason)s::text,
                                'previous_status', status
                              ))
                       ),
                       updated_at = now()
                 WHERE deployment_name = %(deployment_name)s
                   AND provider_id = %(provider_id)s
                """,
                {
                    "reason": reason,
                    "actor": actor,
                    "deployment_name": deployment_name,
                    "provider_id": provider_id,
                },
            )
    row = list_identity_providers(
        conn, deployment_name=deployment_name, provider_id=provider_id,
    )[0]
    return {**row, "previous_status": previous_status, "changed": changed}


def providers_sharing_issuer(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    provider: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Other in-effect providers with the same issuer as ``provider``.

    Disabling a provider refuses every claim that names it, but a server
    configured with another provider id for the same issuer still admits the
    same people. Those rows are reported, not disabled: they may be a
    deliberate second registration (another audience, another gateway).
    """
    return [
        {
            "provider_id": row["provider_id"],
            "provider_type": row["provider_type"],
            "audiences": row["audiences"],
        }
        for row in list_identity_providers(conn, deployment_name=deployment_name)
        if row["provider_id"] != provider["provider_id"]
        and row["issuer"] == provider["issuer"]
        and row["in_effect"]
    ]


def list_identity_providers(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    provider_id: str | None = None,
) -> list[dict[str, Any]]:
    """Every provider row for a deployment, disabled ones included.

    ``in_effect`` is true when claim normalization would accept the provider
    now: status ``active`` and its effective window open at the database
    clock. Only references (``key_reference``, ``secret_references``) are
    stored, never key material, so the full row is listed.
    """
    where = ["deployment_name = %(deployment_name)s"]
    params: dict[str, Any] = {"deployment_name": deployment_name}
    if provider_id is not None:
        where.append("provider_id = %(provider_id)s")
        params["provider_id"] = provider_id
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT *,
                   (status = 'active'
                    AND (effective_from IS NULL OR effective_from <= now())
                    AND (effective_until IS NULL OR effective_until > now())
                   ) AS in_effect
              FROM okg.nomos_identity_providers
             WHERE {' AND '.join(where)}
             ORDER BY provider_id
            """,
            params,
        )
        rows = cur.fetchall()
    listed = []
    for row in rows:
        metadata = dict(row["metadata"] or {})
        listed.append({
            **_provider_from_row(row).to_dict(),
            "in_effect": bool(row["in_effect"]),
            "trusted_verifiers": list(metadata.get("trusted_verifiers") or []),
            "written_by": metadata.get("written_by"),
            "disabled_by": metadata.get("disabled_by"),
            "disabled_reason": metadata.get("disabled_reason"),
            "created_at": _maybe_iso(row["created_at"]),
            "updated_at": _maybe_iso(row["updated_at"]),
        })
    return listed


# --- `okg nomos grant add|disable|list` --------------------------------------


class PermissionGrantError(ValueError):
    """A grant command refused; ``code`` names the refusal."""

    code = "nomos.grant.refused"


class PermissionGrantExistsError(PermissionGrantError):
    code = "nomos.grant.exists"


class PermissionGrantNotFoundError(PermissionGrantError):
    code = "nomos.grant.not_found"


class PermissionGrantWidensAccessError(PermissionGrantError):
    code = "nomos.grant.widens_access"


class PermissionGrantShadowedError(PermissionGrantError):
    code = "nomos.grant.shadowed"


class PermissionGrantUnknownMatchError(PermissionGrantError):
    code = "nomos.grant.unknown_match"


# The `--match` keys `grant add` accepts. `--match KEY=VALUE` stores VALUE as
# a string and the gate compares it with the resource value by equality
# (`identity._resource_match_value_matches`), so only keys whose resource
# value is a string -- or a list of strings compared item by item -- can
# ever match. Keys carrying numbers, booleans or mappings (limit, offset,
# row_cap, timeout_ms, audit, source_lineage, policy_context, as_of) are
# left out on purpose: a deny on them would silently never match. A key
# outside these sets never matches either: harmless for an allow grant (it
# grants nothing), but a deny grant would block nothing.
#
# Where the values come from: the MCP tool and resource gates and
# record/propose (mcp/server.py: deployment, scope, operator_name,
# resource_kind, surface, tool, record_kind and the string tool arguments
# method, query, rank_by, search_profile); the read filter's row resource
# and fine-grained targets (nomos/read_filter.py `_row_resource`,
# `_access_context_resource`, `_fine_grained_targets`: kind, graph_ref,
# <kind>_ref, source_record_ref, context_id, access_context_id and the
# *_ref facts); the runtime boundary (nomos/runtime_enforcement.py:
# nomos_surface, resource_id, agent_run_id).
GRANT_SCALAR_MATCH_KEYS = frozenset({
    "access_context_id",
    "agent_run_id",
    "context_id",
    "context_target_ref",
    "deployment",
    "graph_ref",
    "hidden_ref",
    "horizon_ref",
    "kind",
    "method",
    "nomos_surface",
    "operator_name",
    "query",
    "rank_by",
    "record_kind",
    "resource_id",
    "resource_kind",
    "scope",
    "search_profile",
    "source_record_ref",
    "surface",
    "tool",
})

# Keys the gate may resolve to a LIST of strings (directly, or through the
# aliases `identity._resource_match_actual` / `_source_bundle_matches`
# resolve). A string match on them holds when every listed item equals it --
# including the empty list -- and a source bundle or family matches when the
# resource merely LISTS it, so two different values on one of these keys can
# match the same resource. Grants on them are never treated as disjoint.
GRANT_LIST_MATCH_KEYS = frozenset({
    "hidden_refs",
    "holdout_refs",
    "holdout_set_ids",
    "set_ids",
    "source_bundle",
    "source_bundle_id",
    "source_bundle_ids",
    "source_bundles",
    "source_families",
    "source_family",
    "source_id",
    "source_ids",
    "source_record_refs",
    "subtype_filter",
    "target_refs",
})

# `<row kind>_ref`: the read filter (`_row_resource`, `_fine_grained_targets`)
# sets one per row kind, and it filters only `node` and `edge` rows
# (read_filter.py refuses any other row_kind), so these are the only two. A
# pattern such as `*_ref` would accept a typo like `nod_ref` and a deny on
# it would block nothing.
READ_FILTER_ROW_KINDS = ("node", "edge")
GRANT_KIND_REF_KEYS = frozenset(f"{kind}_ref" for kind in READ_FILTER_ROW_KINDS)

GRANT_SCALAR_MATCH_KEYS = GRANT_SCALAR_MATCH_KEYS | GRANT_KIND_REF_KEYS
GRANT_RESOURCE_MATCH_KEYS = GRANT_SCALAR_MATCH_KEYS | GRANT_LIST_MATCH_KEYS


def _known_match_key(key: str) -> bool:
    return key in GRANT_RESOURCE_MATCH_KEYS


def _scalar_match_key(key: str) -> bool:
    return key in GRANT_SCALAR_MATCH_KEYS


def grant_match_problems(grant: PermissionGrant) -> list[str]:
    """Actions, classifications and match keys in ``grant`` no request carries."""
    from okg.substrate.nomos.vocabulary import ACTIONS, DATA_CLASSIFICATIONS

    problems = [
        f"action {action!r} is not a Nomos action"
        for action in grant.actions
        if action != "*" and action not in ACTIONS
    ]
    problems.extend(
        f"classification {value!r} is not a Nomos data classification"
        for value in grant.classifications
        if value != "*" and value not in DATA_CLASSIFICATIONS
    )
    problems.extend(
        f"--match key {key!r} is not a string-valued resource key the gate reads"
        for key in grant.resource_match
        if not _known_match_key(key)
    )
    return problems


def _matches_disjoint(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    """True only when no resource can satisfy both resource matches.

    Only a scalar string key with two different string values proves that:
    a list-resolvable key (source bundle, family, ids, refs) or any unknown
    key is treated as overlapping.
    """
    for key in set(left) & set(right):
        if not _scalar_match_key(key):
            continue
        a, b = left[key], right[key]
        if isinstance(a, str) and isinstance(b, str) and "*" not in (a, b) and a != b:
            return True
    return False


def _grants_overlap(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    return (
        left["scope"] == right["scope"]
        and _overlaps(left["actions"], right["actions"])
        and _overlaps(left["classifications"], right["classifications"])
        and not _matches_disjoint(left["resource_match"], right["resource_match"])
    )


def _decides_before(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    """The request gate's order: highest priority, then lowest grant id."""
    return (-int(left["priority"]), str(left["grant_id"])) < (
        -int(right["priority"]), str(right["grant_id"])
    )


def grant_conflicts(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    grant: PermissionGrant,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Active opposite-effect grants a new ``grant`` would beat or lose to.

    The gate applies only the first matching grant in (-priority, grant_id)
    order, whatever its effect. Returns ``(beaten_denies, winning_allows)``:
    for a new allow, the overlapping non-allow grants it would decide before
    (it widens access for those requests); for a new deny, the overlapping
    allow grants that would still decide before it (it never takes effect
    for those requests). Overlap is conservative: two grants overlap unless
    an action, classification or string resource-match value tells them
    apart.
    """
    new = grant.to_dict()
    beaten: list[dict[str, Any]] = []
    winning: list[dict[str, Any]] = []
    for row in list_permission_grants(
        conn, deployment_name=deployment_name, subject=grant.subject,
    ):
        if row["status"] != "active" or not _grants_overlap(new, row):
            continue
        summary = {
            "grant_id": row["grant_id"],
            "effect": row["effect"],
            "priority": row["priority"],
            "actions": row["actions"],
            "resource_match": row["resource_match"],
        }
        if new["effect"] == "allow" and row["effect"] != "allow":
            if _decides_before(new, row):
                beaten.append(summary)
        elif new["effect"] != "allow" and row["effect"] == "allow":
            if _decides_before(row, new):
                winning.append(summary)
    return beaten, winning


def add_permission_grant(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    grant: PermissionGrant,
    actor: str,
    reason: str | None = None,
    allow_widen: bool = False,
) -> dict[str, Any]:
    """Write a new, active grant; refuse an existing id whatever its status.

    Decision records name grant ids, so an id must keep meaning the same
    subject, actions and resources; and re-adding a disabled grant would
    silently undo its disable. A change is a disable plus a new grant id.

    The gate applies the first matching grant by (-priority, grant_id),
    whatever its effect, so an add is also refused when:

    - a deny grant names an action or match key no request carries
      (:class:`PermissionGrantUnknownMatchError`): it would block nothing;
    - a new allow would decide before an overlapping active deny
      (:class:`PermissionGrantWidensAccessError`) unless ``allow_widen``;
    - a new deny would decide after an overlapping active allow
      (:class:`PermissionGrantShadowedError`): it would never take effect
      for those requests; give it a higher priority.
    """
    if grant.effect != "allow":
        problems = grant_match_problems(grant)
        if problems:
            raise PermissionGrantUnknownMatchError(
                f"deny grant {grant.grant_id!r} would never match: "
                + "; ".join(problems)
                + ". A deny that matches nothing blocks nothing"
            )
    with conn.cursor(row_factory=dict_row) as cur:
        # Two adds for one subject could each read the other's grants as
        # absent and both pass the conflict check below. A transaction-scoped
        # advisory lock on (deployment, subject) makes the read-check-insert
        # one step per subject; it is released at COMMIT or ROLLBACK.
        cur.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
            (f"okg.nomos.grant:{deployment_name}:{grant.subject}",),
        )
        cur.execute(
            """
            SELECT status, subject
              FROM okg.nomos_permission_grants
             WHERE deployment_name = %s AND grant_id = %s
            """,
            (deployment_name, grant.grant_id),
        )
        existing = cur.fetchone()
        cur.execute("SELECT now() AS at")
        at = cur.fetchone()["at"]
    if existing is not None:
        status = str(existing["status"])
        if status == "disabled":
            message = (
                f"permission grant {grant.grant_id!r} already exists and is "
                "disabled; an add never re-enables a disabled grant. Add the "
                "access under a new grant id"
            )
        else:
            message = (
                f"permission grant {grant.grant_id!r} already exists (status "
                f"{status}, subject {existing['subject']!r}); `add` never "
                "rewrites a grant, because decision records name grant ids. "
                "Disable it and add a new grant id to change it"
            )
        raise PermissionGrantExistsError(message)
    beaten, winning = grant_conflicts(
        conn, deployment_name=deployment_name, grant=grant,
    )
    if beaten and not allow_widen:
        raise PermissionGrantWidensAccessError(
            f"allow grant {grant.grant_id!r} (priority {grant.priority}) would "
            "decide before overlapping deny grant(s) "
            + ", ".join(
                f"{row['grant_id']} (priority {row['priority']})" for row in beaten
            )
            + ", admitting requests they now block: the gate applies the "
            "first match by highest priority, then lowest grant id. Give the "
            "allow a lower priority, or pass --allow-widen if that is what "
            "you mean"
        )
    if winning:
        top = max(int(row["priority"]) for row in winning)
        raise PermissionGrantShadowedError(
            f"deny grant {grant.grant_id!r} (priority {grant.priority}) would "
            "never take effect where it overlaps allow grant(s) "
            + ", ".join(
                f"{row['grant_id']} (priority {row['priority']})" for row in winning
            )
            + ": the gate applies the first match by highest priority, then "
            f"lowest grant id. Pass --priority {top + 1} or higher"
        )
    written = upsert_permission_grant(
        conn,
        deployment_name=deployment_name,
        grant=grant,
        status="active",
        metadata={
            "written_by": actor,
            "events": [
                _binding_event(
                    at=at, action="added", actor=actor, reason=reason,
                    previous_status=None,
                ),
            ],
        },
        replace_existing=False,
    )
    if not written:
        raise PermissionGrantExistsError(
            f"permission grant {grant.grant_id!r} was created by another "
            "writer while this one ran; run `okg nomos grant list` and retry"
        )
    return list_permission_grants(
        conn, deployment_name=deployment_name, grant_id=grant.grant_id,
    )[0]


def disable_permission_grant(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    grant_id: str,
    actor: str,
    reason: str | None = None,
    allow_widen: bool = False,
) -> dict[str, Any]:
    """Disable one grant; the row is kept with who, when and why.

    What a disable does depends on the grant's effect, because the request
    gate applies only the highest-priority grant that matches:

    - Disabling an ``allow`` grant removes that permission. Other allow
      grants for the same subject may still permit the same requests; the
      caller reports them with :func:`grants_that_may_still_allow`.
    - Disabling a ``deny`` (or any non-allow) grant can ADMIT requests it
      was blocking, because a lower-priority allow grant then decides. That
      is refused (:class:`PermissionGrantWidensAccessError`) unless
      ``allow_widen`` says the operator means it.

    Disabling a grant that is already disabled changes nothing.
    """
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT status, effect
              FROM okg.nomos_permission_grants
             WHERE deployment_name = %s AND grant_id = %s
               FOR UPDATE
            """,
            (deployment_name, grant_id),
        )
        row = cur.fetchone()
        if row is None:
            raise PermissionGrantNotFoundError(
                f"no permission grant {grant_id!r} in deployment "
                f"{deployment_name!r}; `okg nomos grant list` shows them"
            )
        previous_status = str(row["status"])
        changed = previous_status != "disabled"
        if changed and str(row["effect"]) != "allow" and not allow_widen:
            raise PermissionGrantWidensAccessError(
                f"permission grant {grant_id!r} has effect {row['effect']!r}; "
                "disabling it can admit requests it now blocks, because a "
                "lower-priority allow grant would then decide. Pass "
                "--allow-widen if that is what you mean"
            )
        if changed:
            cur.execute(
                """
                UPDATE okg.nomos_permission_grants
                   SET status = 'disabled',
                       metadata = metadata || jsonb_build_object(
                         'disabled_at', now(),
                         'disabled_reason', %(reason)s::text,
                         'disabled_by', %(actor)s::text,
                         'events',
                         COALESCE(metadata -> 'events', '[]'::jsonb)
                           || jsonb_build_array(jsonb_build_object(
                                'at', now(),
                                'action', 'disabled',
                                'actor', %(actor)s::text,
                                'reason', %(reason)s::text,
                                'previous_status', status
                              ))
                       ),
                       updated_at = now()
                 WHERE deployment_name = %(deployment_name)s
                   AND grant_id = %(grant_id)s
                """,
                {
                    "reason": reason,
                    "actor": actor,
                    "deployment_name": deployment_name,
                    "grant_id": grant_id,
                },
            )
    row = list_permission_grants(
        conn, deployment_name=deployment_name, grant_id=grant_id,
    )[0]
    return {**row, "previous_status": previous_status, "changed": changed}


def _overlaps(left: Sequence[str], right: Sequence[str]) -> bool:
    return "*" in left or "*" in right or bool(set(left) & set(right))


def _covers(wider: Sequence[str], narrower: Sequence[str]) -> bool:
    return "*" in wider or ("*" not in narrower and set(narrower) <= set(wider))


def grants_that_may_still_allow(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    grant: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Other active allow grants that may permit what ``grant`` permitted.

    Same subject and scope, and at least one action and classification in
    common. ``match`` is ``covers`` when the row permits every request
    ``grant`` did -- its actions and classifications include ``grant``'s and
    its resource match asks for no more than ``grant``'s did -- and
    ``may_overlap`` otherwise. A higher-priority deny grant can still block
    either; this lists what could keep the subject's access, not a verdict.
    """
    actions = list(grant["actions"])
    classifications = list(grant["classifications"])
    resource_match = dict(grant["resource_match"])
    found: list[dict[str, Any]] = []
    for row in list_permission_grants(
        conn, deployment_name=deployment_name, subject=str(grant["subject"]),
    ):
        if row["grant_id"] == grant["grant_id"] or row["status"] != "active":
            continue
        if row["scope"] != grant["scope"] or row["effect"] != "allow":
            continue
        if not _overlaps(row["actions"], actions):
            continue
        if not _overlaps(row["classifications"], classifications):
            continue
        covers = (
            _covers(row["actions"], actions)
            and _covers(row["classifications"], classifications)
            and all(
                resource_match.get(key) == value
                for key, value in row["resource_match"].items()
            )
        )
        found.append({
            "grant_id": row["grant_id"],
            "actions": row["actions"],
            "classifications": row["classifications"],
            "resource_match": row["resource_match"],
            "priority": row["priority"],
            "match": "covers" if covers else "may_overlap",
        })
    return found


def subject_has_binding(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    subject: str,
    scope: str,
) -> bool:
    """Whether any in-effect binding resolves a user to ``subject`` in ``scope``."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
              SELECT 1 FROM okg.nomos_subject_bindings
               WHERE deployment_name = %s AND subject = %s AND scope = %s
                 AND status = 'active'
                 AND (effective_from IS NULL OR effective_from <= now())
                 AND (effective_until IS NULL OR effective_until > now())
            )
            """,
            (deployment_name, subject, scope),
        )
        return bool(cur.fetchone()[0])


def list_permission_grants(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    subject: str | None = None,
    grant_id: str | None = None,
) -> list[dict[str, Any]]:
    """Every grant row for a deployment, disabled ones included."""
    where = ["deployment_name = %(deployment_name)s"]
    params: dict[str, Any] = {"deployment_name": deployment_name}
    if subject is not None:
        where.append("subject = %(subject)s")
        params["subject"] = subject
    if grant_id is not None:
        where.append("grant_id = %(grant_id)s")
        params["grant_id"] = grant_id
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT grant_id, subject, actions, scope, effect, resource_match,
                   classifications, priority, governing_rule_ids, status,
                   metadata, created_at, updated_at
              FROM okg.nomos_permission_grants
             WHERE {' AND '.join(where)}
             ORDER BY subject, priority DESC, grant_id
            """,
            params,
        )
        rows = cur.fetchall()
    return [
        {
            **_grant_from_row(row).to_dict(),
            "status": row["status"],
            "governing_rule_ids": list(row["governing_rule_ids"] or []),
            "written_by": (row["metadata"] or {}).get("written_by"),
            "disabled_by": (row["metadata"] or {}).get("disabled_by"),
            "disabled_reason": (row["metadata"] or {}).get("disabled_reason"),
            "metadata": dict(row["metadata"] or {}),
            "created_at": _maybe_iso(row["created_at"]),
            "updated_at": _maybe_iso(row["updated_at"]),
        }
        for row in rows
    ]


def evaluate_identity_claim_from_postgres(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    bundle: CompiledPolicyBundle | None,
    claim: IdentityClaimEnvelope | Mapping[str, Any],
    action: str,
    resource: Mapping[str, Any],
    scope: str,
    classification: str = "internal",
    now: str | None = None,
    enforcement_point: str = "nomos.identity.postgres",
    target: Mapping[str, Any] | None = None,
    extra_context: Mapping[str, Any] | None = None,
) -> IdentityAuthorizationResult:
    # PACT mcp-principal-mapping: per-request identity resolution reaches
    # this function once per ROW per SOURCE in a read pass, and each entry
    # re-read four tables. The reads are memoized within an active memo scope
    # (read_filter opens one per filter pass) and live otherwise. Memoizing
    # HERE rather than inside the loaders is deliberate: this function never
    # writes a provider, binding or grant, so it cannot be served its own
    # stale read-back, while the admin upsert paths that call the same
    # loaders keep reading live.
    key = (db_scope_key(conn), deployment_name, scope)
    providers = memoized(
        ("nomos_identity_providers", key),
        lambda: load_identity_providers(conn, deployment_name=deployment_name),
    )
    envelope = (
        claim
        if isinstance(claim, IdentityClaimEnvelope)
        else normalize_identity_claim(claim, providers, now=now)
    )
    if bundle is None:
        from okg.substrate.nomos.store import load_active_policy_bundle

        bundle = load_active_policy_bundle(
            conn,
            deployment_name=deployment_name,
            scope=scope,
        )
    bindings = memoized(
        ("nomos_subject_bindings", key),
        lambda: load_subject_bindings(
            conn,
            deployment_name=deployment_name,
            scope=scope,
        ),
    )
    grants = memoized(
        ("nomos_permission_grants", key),
        lambda: load_permission_grants(
            conn,
            deployment_name=deployment_name,
            scope=scope,
        ),
    )
    result = evaluate_identity_claim(
        bundle,
        envelope,
        bindings=bindings,
        permission_grants=grants,
        action=action,
        resource=resource,
        scope=scope,
        classification=classification,
        now=now,
        enforcement_point=enforcement_point,
        extra_context=extra_context,
    )
    # One claim evaluated over many rows in one READ PASS upserts the same
    # external-claim row over and over: the key is (deployment, provider,
    # claim fingerprint) and none of those varies per row. Within a read pass
    # it is written once instead.
    #
    # Three deliberate limits on that dedup:
    #
    # * It is OPT-IN, and only the multi-user MCP request gate opts in.
    #   `persist_external_claim` upserts with DO UPDATE (last write wins,
    #   `observed_at = now()`), so skipping it changes what the audit trail
    #   records. Ingest runs (which share the same contextvar via
    #   `commit_scope`) and single-operator MCP reads never opt in, and so
    #   behave exactly as they did before this scope existed.
    # * The token is per-pass and part of the key, so a mark left behind in a
    #   shared outer scope by a finished pass cannot be hit by a later one.
    # * The decision row is STILL written immediately. Routing the skipped
    #   ones into the batched sink would look harmless, but that sink flushes
    #   only on clean scope exit, so a read pass that raised partway would
    #   discard the identity decisions it had already made -- losing exactly
    #   the audit rows a refusal makes interesting.
    dedup_token = identity_claim_dedup_token()
    claim_already_persisted = dedup_token is not None and memo_seen(
        (
            "nomos_external_claim",
            dedup_token,
            db_scope_key(conn),
            deployment_name,
            result.claim.provider_id,
            result.claim.claim_fingerprint,
        )
    )
    persist_nomos_decision(
        conn,
        deployment_name=deployment_name,
        decision=result.decision,
        target=target or {
            "kind": "identity_authorization",
            "scope": scope,
        },
        # persist_external_claim below writes an FK to this decision row in
        # the same transaction, so the row must be visible now, not at a
        # batch flush.
        allow_deferred=False,
    )
    if not claim_already_persisted:
        persist_external_claim(
            conn,
            deployment_name=deployment_name,
            claim=result.claim,
            decision_id=result.decision.decision_id,
        )
    return result


def load_identity_providers(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
) -> list[IdentityProvider]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT *
              FROM okg.nomos_identity_providers
             WHERE deployment_name = %s
             ORDER BY provider_id
            """,
            (deployment_name,),
        )
        return [_provider_from_row(row) for row in cur.fetchall()]


def load_subject_bindings(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    scope: str,
) -> list[SubjectBinding]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT *
              FROM okg.nomos_subject_bindings
             WHERE deployment_name = %s
               AND scope = %s
               AND status = 'active'
             ORDER BY priority DESC, binding_id
            """,
            (deployment_name, scope),
        )
        return [_binding_from_row(row) for row in cur.fetchall()]


def load_permission_grants(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    scope: str,
) -> list[PermissionGrant]:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT *
              FROM okg.nomos_permission_grants
             WHERE deployment_name = %s
               AND scope = %s
               AND status = 'active'
             ORDER BY priority DESC, grant_id
            """,
            (deployment_name, scope),
        )
        return [_grant_from_row(row) for row in cur.fetchall()]


def persist_external_claim(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    claim: IdentityClaimEnvelope,
    decision_id: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO okg.nomos_external_claims (
              deployment_name, provider_id, provider_type, issuer,
              external_subject, audiences, groups, roles, entitlements,
              assurance, assurance_values, mfa_satisfied, realm,
              service_principal, delegation_chain, expires_at, not_before,
              auth_time,
              claim_fingerprint, verification_status, evidence,
              verification_error, nomos_decision_id
            )
            VALUES (
              %(deployment_name)s, %(provider_id)s, %(provider_type)s,
              %(issuer)s, %(external_subject)s, %(audiences)s,
              %(groups)s, %(roles)s, %(entitlements)s, %(assurance)s,
              %(assurance_values)s, %(mfa_satisfied)s, %(realm)s,
              %(service_principal)s, %(delegation_chain)s,
              %(expires_at)s, %(not_before)s, %(auth_time)s,
              %(claim_fingerprint)s, %(verification_status)s,
              %(evidence)s, %(verification_error)s, %(nomos_decision_id)s
            )
            ON CONFLICT (deployment_name, provider_id, claim_fingerprint)
            DO UPDATE SET
              verification_status = EXCLUDED.verification_status,
              evidence = EXCLUDED.evidence,
              verification_error = EXCLUDED.verification_error,
              nomos_decision_id = EXCLUDED.nomos_decision_id,
              observed_at = now()
            """,
            {
                "deployment_name": deployment_name,
                "provider_id": claim.provider_id,
                "provider_type": claim.provider_type,
                "issuer": claim.issuer,
                "external_subject": claim.external_subject,
                "audiences": list(claim.audiences),
                "groups": list(claim.groups),
                "roles": list(claim.roles),
                "entitlements": list(claim.entitlements),
                "assurance": claim.assurance,
                "assurance_values": list(claim.assurance_values),
                "mfa_satisfied": claim.mfa_satisfied,
                "realm": claim.realm,
                "service_principal": claim.service_principal,
                "delegation_chain": list(claim.delegation_chain),
                "expires_at": claim.expires_at,
                "not_before": claim.not_before,
                "auth_time": claim.auth_time,
                "claim_fingerprint": claim.claim_fingerprint,
                "verification_status": claim.verification_status,
                "evidence": Jsonb(claim.to_dict()),
                "verification_error": claim.verification_error,
                "nomos_decision_id": decision_id,
            },
        )


# Decision identity is content-derived (core.evaluate hashes policy
# fingerprint + input + effect), so a conflicting row IS the same decision:
# DO NOTHING keeps the recorded target deterministically at the
# first-observed access and makes re-persisted rows index-probe-cheap (no
# heap update, no WAL churn). The prior DO UPDATE target refresh carried no
# information once cross-generation dedup landed (issue #635).
_DECISION_INSERT_SQL = """
    INSERT INTO okg.nomos_decisions (
      deployment_name, decision_id, policy_bundle_id,
      policy_version, policy_fingerprint, input_hash,
      subject, action, resource, classification, context,
      effect, reason_code, matched_rule_ids, obligations,
      enforcement_point, target
    )
    VALUES (
      %(deployment_name)s, %(decision_id)s,
      %(policy_bundle_id)s, %(policy_version)s,
      %(policy_fingerprint)s, %(input_hash)s, %(subject)s,
      %(action)s, %(resource)s, %(classification)s,
      %(context)s, %(effect)s, %(reason_code)s,
      %(matched_rule_ids)s, %(obligations)s,
      %(enforcement_point)s, %(target)s
    )
    ON CONFLICT (deployment_name, decision_id)
    DO NOTHING
"""

# One statement for a whole batch: each element of %(rows)s is the full
# decision row as jsonb; typed columns are extracted in SQL. Intra-batch
# duplicate (deployment_name, decision_id) pairs are deduped Python-side
# before flush (first-observed wins, matching DO NOTHING semantics).
_DECISION_BATCH_INSERT_SQL = """
    INSERT INTO okg.nomos_decisions (
      deployment_name, decision_id, policy_bundle_id,
      policy_version, policy_fingerprint, input_hash,
      subject, action, resource, classification, context,
      effect, reason_code, matched_rule_ids, obligations,
      enforcement_point, target
    )
    SELECT
      d->>'deployment_name', d->>'decision_id', d->>'policy_bundle_id',
      d->>'policy_version', d->>'policy_fingerprint', d->>'input_hash',
      d->>'subject', d->>'action', d->'resource',
      d->>'classification', d->'context', d->>'effect', d->>'reason_code',
      ARRAY(SELECT jsonb_array_elements_text(d->'matched_rule_ids')),
      d->'obligations', d->>'enforcement_point', d->'target'
    FROM unnest(%(rows)s::jsonb[]) AS d
    ON CONFLICT (deployment_name, decision_id)
    DO NOTHING
"""


def _decision_row_payload(
    *,
    deployment_name: str,
    decision: PolicyDecision,
    target: Mapping[str, Any] | None,
) -> dict[str, Any]:
    return {
        "deployment_name": deployment_name,
        "decision_id": decision.decision_id,
        "policy_bundle_id": decision.policy_bundle_id,
        "policy_version": decision.policy_version,
        "policy_fingerprint": decision.policy_fingerprint,
        "input_hash": decision.input_hash,
        "subject": decision.subject,
        "action": decision.action,
        "resource": dict(decision.resource),
        "classification": decision.classification,
        "context": dict(decision.context),
        "effect": decision.effect,
        "reason_code": decision.reason_code,
        "matched_rule_ids": list(decision.matched_rule_ids),
        "obligations": list(decision.obligations),
        "enforcement_point": decision.enforcement_point,
        "target": sanitize_trace_mapping(target or {}),
    }


class DecisionPersistBatch:
    """Accumulates decision persists for one read-filter pass.

    Flushed as a single batched INSERT on clean scope exit; an exception in
    the pass drops the buffer, mirroring how inline persists would have been
    rolled back uncommitted.
    """

    def __init__(self, conn: psycopg.Connection) -> None:
        self.conn = conn
        self._rows: dict[tuple[str, str], dict[str, Any]] = {}

    def add(
        self,
        deployment_name: str,
        decision: PolicyDecision,
        target: Mapping[str, Any] | None,
    ) -> None:
        self._rows.setdefault(
            (deployment_name, decision.decision_id),
            _decision_row_payload(
                deployment_name=deployment_name,
                decision=decision,
                target=target,
            ),
        )

    def flush(self) -> int:
        if not self._rows:
            return 0
        payload = [Jsonb(row) for row in self._rows.values()]
        with self.conn.cursor() as cur:
            cur.execute(_DECISION_BATCH_INSERT_SQL, {"rows": payload})
        flushed = len(self._rows)
        self._rows.clear()
        return flushed


_DECISION_SINK: ContextVar[DecisionPersistBatch | None] = ContextVar(
    "okg_nomos_decision_sink",
    default=None,
)


@contextmanager
def batched_decision_persist(
    conn: psycopg.Connection,
) -> Iterator[DecisionPersistBatch]:
    """Batch persist_nomos_decision calls on `conn` within this scope.

    Deferred rows flush as ONE statement on clean exit (before the caller's
    commit). Persists on other connections, and callers that need the row
    visible immediately (allow_deferred=False), are unaffected. Reentrant on
    the same connection: the outer scope owns the flush.
    """
    parent = _DECISION_SINK.get()
    if parent is not None and parent.conn is conn:
        yield parent
        return
    sink = DecisionPersistBatch(conn)
    token = _DECISION_SINK.set(sink)
    try:
        yield sink
        sink.flush()
    finally:
        _DECISION_SINK.reset(token)


def persist_nomos_decision(
    conn: psycopg.Connection,
    *,
    deployment_name: str,
    decision: PolicyDecision,
    target: Mapping[str, Any] | None = None,
    allow_deferred: bool = True,
) -> None:
    """Record one policy decision (insert-once; conflicts are no-ops).

    Inside a batched_decision_persist scope on the same connection the row is
    buffered and flushed with the batch. Callers that immediately write a row
    referencing the decision (e.g. the external-claim FK) must pass
    allow_deferred=False so the decision is visible in-transaction first.
    """
    decision = sanitize_policy_decision(decision)
    if allow_deferred:
        sink = _DECISION_SINK.get()
        if sink is not None and sink.conn is conn:
            sink.add(deployment_name, decision, target)
            return
    payload = _decision_row_payload(
        deployment_name=deployment_name,
        decision=decision,
        target=target,
    )
    params = {
        **payload,
        "resource": Jsonb(payload["resource"]),
        "context": Jsonb(payload["context"]),
        "obligations": Jsonb(payload["obligations"]),
        "target": Jsonb(payload["target"]),
    }
    with conn.cursor() as cur:
        cur.execute(_DECISION_INSERT_SQL, params)


def _provider_from_row(row: Mapping[str, Any]) -> IdentityProvider:
    return IdentityProvider(
        provider_id=str(row["provider_id"]),
        provider_type=str(row["provider_type"]),
        issuer=str(row["issuer"]),
        audiences=tuple(row["audiences"] or ()),
        subject_claim=str(row["subject_claim"] or "sub"),
        group_claims=tuple(row["group_claims"] or ()),
        role_claims=tuple(row["role_claims"] or ()),
        entitlement_claims=tuple(row["entitlement_claims"] or ()),
        assurance_claim=row["assurance_claim"],
        key_reference=row["key_reference"],
        secret_references=tuple(row.get("secret_references") or ()),
        claim_allowlist=tuple(row.get("claim_allowlist") or ()),
        required_assurance=tuple(row.get("required_assurance") or ()),
        mfa_required=bool(row.get("mfa_required")),
        group_claims_approved=bool(row.get("group_claims_approved")),
        metadata_fingerprint=row.get("metadata_fingerprint"),
        realm=row.get("realm"),
        service_principal=row.get("service_principal"),
        freshness_seconds=row["freshness_seconds"],
        status=row.get("status") or "active",
        effective_from=_maybe_iso(row["effective_from"]),
        effective_until=_maybe_iso(row["effective_until"]),
        metadata=dict(row["metadata"] or {}),
    )


def _binding_from_row(row: Mapping[str, Any]) -> SubjectBinding:
    return SubjectBinding(
        binding_id=str(row["binding_id"]),
        provider_id=str(row["provider_id"]),
        issuer=str(row["issuer"]),
        claim_path=str(row["claim_path"]),
        claim_value=str(row["claim_value"]),
        subject=str(row["subject"]),
        scope=str(row["scope"]),
        priority=int(row["priority"] or 0),
        required_assurance=tuple(row.get("required_assurance") or ()),
        effective_from=_maybe_iso(row["effective_from"]),
        effective_until=_maybe_iso(row["effective_until"]),
    )


def _grant_from_row(row: Mapping[str, Any]) -> PermissionGrant:
    return PermissionGrant(
        grant_id=str(row["grant_id"]),
        subject=str(row["subject"]),
        actions=tuple(row["actions"] or ()),
        scope=str(row["scope"]),
        effect=str(row["effect"]),
        resource_match=dict(row["resource_match"] or {}),
        classifications=tuple(row["classifications"] or ("*",)),
        priority=int(row["priority"] or 0),
    )


def _maybe_iso(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)
