"""Deployment group projection: one projection, two consumers.

PACT ``chat-sso-integration``, design.md section 5 mechanism 4. The identity
provider's group claims are projected ONCE, into two places that must agree:

* the chat instance's own groups (and the model rows that make a preset
  visible to them), verified by readback -- the same standard
  ``chat-sync-projection`` sets;
* the Nomos subject bindings ``mcp-principal-mapping`` resolves each
  per-request principal through.

There is no second group vocabulary. The IdP's group-claim VALUE is the Open
WebUI group name and the tail of the Nomos subject (``subject:<group>``), so
"which group is this" has one answer no matter which consumer is asking.

**Membership absence is a denial, never a default.** A principal in neither
group is not "unrestricted": the projection withholds the chat group, the
preset grant, and the Nomos binding, and reports the three typed denials that
result -- one per surface, each with the reason code the surface actually
emits. The MCP-side code is IMPORTED from ``mcp/principal_mapping.py`` rather
than restated, because that is the module that emits it; a copy here could
drift into claiming a refusal that no longer exists.

**A shipped Pipe or preset is not a visible one.** Measured at Open WebUI
v0.11.0 (``pact/changes/openwebui-chat-frontend/evidence/
pipe-user-context-probe.md``): ``get_filtered_models`` admits a model with no
database row ONLY for admins -- "no DB entry means no access control
configured yet". So a projection that creates groups but no model row leaves
every group member seeing nothing, with no error to explain it. This module
therefore treats the model row and its group grant as part of the projection,
and its readback fails if either is missing.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Protocol, Sequence

from okg.substrate.chat.sso import (
    GROUP_MEMBERSHIP_ABSENT,
    MEMBER_ADMIN,
    MEMBER_NONE,
    MEMBER_USER,
    membership_state,
)
from okg.substrate.deployments.chat_block import ChatSso
from okg.substrate.errors import SubstrateError
from okg.substrate.mcp.principal_mapping import UNIDENTIFIED_PRINCIPAL_REASON


__all__ = [
    "BINDING_CLAIM_PATH",
    "ChatGroupClient",
    "ChatSsoProjectionError",
    "CONSOLE_DENIAL_REASON",
    "GRAPH_DENIAL_REASON",
    "GroupProjection",
    "IdpMembership",
    "MEMBER_ADMIN",
    "MEMBER_NONE",
    "MEMBER_USER",
    "NO_INSTANCE_ACCOUNT_REASON",
    "NomosBindingStore",
    "PRESET_DENIAL_REASON",
    "PresetGrant",
    "ProjectedPrincipal",
    "TypedDenial",
    "binding_id_for",
    "default_preset_grants",
    "nomos_subject_for",
    "project_deployment_groups",
    "visible_presets",
]


#: What the console front emits for a principal in neither group. The SAME
#: constant the proxy decision uses, so the projection's report and the
#: front's response cannot disagree about why someone was refused.
CONSOLE_DENIAL_REASON = GROUP_MEMBERSHIP_ABSENT

#: The projection's own name for the chat surface's refusal.
#:
#: Unlike its two siblings this code is NOT imported from an emitter,
#: because at Open WebUI v0.11.0 there is no emitter: the measured behavior
#: is silence -- the preset is simply absent from the caller's model list,
#: with no error to explain it (see the module docstring). That silence is
#: the very thing the requirement's "typed denial" exists to replace, so the
#: projection states it here rather than leaving the chat arm as an absence.
#: What is asserted is the WITHHELD GRANT, which is checkable; this constant
#: is the name the report gives it. Should a later Open WebUI grow a real
#: refusal code, this is the line to re-key onto it.
PRESET_DENIAL_REASON = "chat.sso.preset_access_denied"

#: A group MEMBER who has no account on the chat instance yet.
#:
#: A third state, not a quiet success and not a denial. Open WebUI's groups
#: hold instance user ids, so there is nothing to add until the person has
#: signed in once (and at v0.11.0 self-signup closes after the first
#: account, so the account is often an admin action). Reporting it as a
#: value is the point: the projection would otherwise write the Nomos
#: binding, add nobody to the chat group, read back clean -- because
#: "expected member: no" and "observed member: no" agree -- and leave a real
#: member with graph access and no chat surface, with nothing anywhere
#: saying so.
NO_INSTANCE_ACCOUNT_REASON = "chat.sso.instance_account_absent"

#: What the graph read path emits. Imported, not restated -- see the module
#: docstring.
GRAPH_DENIAL_REASON = UNIDENTIFIED_PRINCIPAL_REASON

#: The claim path every projected binding is keyed on, and the one fact in
#: this module that is NOT free to follow the manifest.
#:
#: ``SubjectBinding.matches`` resolves its ``claim_path`` through
#: ``IdentityClaimEnvelope.claim_values``, which answers for a FIXED
#: vocabulary (``external_subject``/``subject``/``sub``, ``groups``,
#: ``roles``, ``entitlements``, assurance, MFA, realm, service principal)
#: and returns an EMPTY TUPLE for anything else -- including the perfectly
#: ordinary ``email`` a deployment is likely to name as its
#: ``subject_claim``. A binding keyed on the manifest's claim name would
#: therefore match nobody, on every request, and the resulting refusal reads
#: as "this person has no subject" rather than "the projection wrote a row
#: that cannot be reached". ``header_claim_for_request`` puts the principal
#: in ``external_subject``, so that is where the binding reads it from.
#: (The merged PACT's own end-to-end fixture binds on ``external_subject``
#: for the same reason.)
BINDING_CLAIM_PATH = "external_subject"

#: Every binding this projection owns carries this id prefix, and it owns no
#: others. The boundary is load-bearing rather than cosmetic: the revocation
#: sweep must not disable a binding an operator authored by hand, and the
#: readback must not report one as a failure this projection could fix.
_BINDING_PREFIX = "okg:chat-sso:binding:"


class ChatSsoProjectionError(SubstrateError):
    """The projection did not land, or did not read back as written."""

    code = "chat_sso_projection_failed"


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class IdpMembership:
    """One person, as the identity provider's group claim describes them.

    ``groups`` is the claim value verbatim. Nothing is normalized, mapped or
    prefixed on the way in: a name that does not match the manifest's
    ``admin_group``/``user_group`` produces the "neither" state, which is a
    decidable answer, rather than being coerced into a near match.
    """

    principal: str
    groups: tuple[str, ...] = ()
    #: The instance's own user id, when the person already has an account.
    #: ``None`` means "no account yet", which is not a membership question:
    #: the projection reports it and grants nothing.
    chat_user_id: str | None = None


@dataclass(frozen=True)
class PresetGrant:
    """One model preset and the groups that may see it."""

    model_id: str
    groups: tuple[str, ...]


def default_preset_grants(deployment: str, sso: ChatSso) -> tuple[PresetGrant, ...]:
    """The two presets a multi-user deployment publishes.

    The user preset is granted to BOTH groups and the admin preset only to
    the admin group, which is what makes "the user sees the user preset only"
    and "the admin sees the admin preset" true at the same time. Admin is a
    superset of capability, not a different capability.

    These are the DEFAULTS, not the whole set. Anything else the deployment
    exposes as a model row -- notably the review-inbox Pipe from
    ``chat-review-queue-pipe``, which is subject to exactly the same
    admin-only-without-a-row rule -- is passed to
    :func:`project_deployment_groups` as an extra :class:`PresetGrant`. It is
    not inferred: a Pipe this function guessed at would be granted to a group
    nobody chose.
    """
    admin = sso.admin_group or ""
    user = sso.user_group or ""
    return (
        PresetGrant(model_id=f"{deployment}-analyst", groups=(user, admin)),
        PresetGrant(model_id=f"{deployment}-admin", groups=(admin,)),
    )


# ---------------------------------------------------------------------------
# Ports
# ---------------------------------------------------------------------------

class ChatGroupClient(Protocol):
    """The Open WebUI admin operations the projection needs.

    Named for the verbs that were MEASURED to work at v0.11.0. In particular
    membership is added through the dedicated member verb: the probe recorded
    that ``POST /api/v1/groups/id/<gid>/update`` carrying ``user_ids`` leaves
    ``member_count`` at 0, so a projection that reconciled membership through
    the update verb would report success and change nothing.
    """

    def list_groups(self) -> Sequence[Mapping[str, Any]]:
        raise NotImplementedError

    def create_group(self, *, name: str, description: str) -> Mapping[str, Any]:
        raise NotImplementedError

    def add_group_member(self, *, group_id: str, user_id: str) -> None:
        raise NotImplementedError

    def remove_group_member(self, *, group_id: str, user_id: str) -> None:
        raise NotImplementedError

    def list_models(self) -> Sequence[Mapping[str, Any]]:
        raise NotImplementedError

    def upsert_model(
        self, *, model_id: str, access_grants: Sequence[Mapping[str, Any]],
    ) -> Mapping[str, Any]:
        raise NotImplementedError


class NomosBindingStore(Protocol):
    """The subject-binding operations the projection needs.

    Deliberately the same three verbs ``nomos/identity_store.py`` exposes, so
    the production implementation is a thin adapter rather than a second
    persistence model.
    """

    def load_bindings(self, *, scope: str) -> Sequence[Any]:
        raise NotImplementedError

    def upsert_binding(self, binding: Any) -> None:
        raise NotImplementedError

    def deactivate_binding(self, *, binding_id: str) -> None:
        raise NotImplementedError


# ---------------------------------------------------------------------------
# Naming
# ---------------------------------------------------------------------------

def nomos_subject_for(group: str) -> str:
    """The Nomos subject for one projected group.

    ``subject:<group>`` -- the group name verbatim. This is where "one
    vocabulary" is cashed out: an auditor reading a decision row sees the
    same token the identity provider sent and the chat instance stores.
    """
    return f"subject:{group}"


def binding_id_for(
    *,
    deployment: str,
    provider_id: str,
    claim_path: str,
    principal: str,
    subject: str,
) -> str:
    """A deterministic id, so re-running the projection UPDATES one row.

    Content-addressed the same way the rest of Nomos identifies things. A
    random or sequential id would make every run a fresh binding and leave
    the revoked ones behind, which is how a removed admin keeps their
    subject.
    """
    from okg.substrate.nomos.identity import _sha256_hex

    digest = _sha256_hex({
        "deployment": deployment,
        "provider_id": provider_id,
        "claim_path": claim_path,
        "principal": principal,
        "subject": subject,
    })
    return f"{_BINDING_PREFIX}{digest[:24]}"


# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class TypedDenial:
    """One surface's refusal, with the code that surface actually emits."""

    surface: str
    principal: str
    reason_code: str
    detail: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "surface": self.surface,
            "principal": self.principal,
            "reason_code": self.reason_code,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class ProjectedPrincipal:
    """What the projection did about one person."""

    principal: str
    membership: str
    idp_groups: tuple[str, ...] = ()
    chat_group: str | None = None
    chat_user_id: str | None = None
    nomos_subject: str | None = None
    denials: tuple[TypedDenial, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "principal": self.principal,
            "membership": self.membership,
            "idp_groups": list(self.idp_groups),
            "chat_group": self.chat_group,
            "chat_user_id": self.chat_user_id,
            "nomos_subject": self.nomos_subject,
            "denials": [denial.to_dict() for denial in self.denials],
        }


@dataclass(frozen=True)
class GroupProjection:
    """The whole projection, after readback."""

    deployment: str
    scope: str
    group_ids: Mapping[str, str]
    principals: tuple[ProjectedPrincipal, ...]
    preset_grants: tuple[PresetGrant, ...]
    changed: bool
    readback: Mapping[str, Any] = field(default_factory=dict)

    def principal(self, name: str) -> ProjectedPrincipal:
        for item in self.principals:
            if item.principal == name:
                return item
        raise KeyError(name)

    def to_dict(self) -> dict[str, Any]:
        return {
            "deployment": self.deployment,
            "scope": self.scope,
            "group_ids": dict(self.group_ids),
            "principals": [item.to_dict() for item in self.principals],
            "preset_grants": [
                {"model_id": grant.model_id, "groups": list(grant.groups)}
                for grant in self.preset_grants
            ],
            "changed": self.changed,
            "readback": dict(self.readback),
        }


# ---------------------------------------------------------------------------
# The projection
# ---------------------------------------------------------------------------

def project_deployment_groups(
    *,
    deployment: str,
    sso: ChatSso,
    principal_mapping: Any,
    memberships: Sequence[IdpMembership],
    chat_client: ChatGroupClient,
    binding_store: NomosBindingStore,
    preset_grants: Sequence[PresetGrant] | None = None,
    scope: str | None = None,
) -> GroupProjection:
    """Project the IdP's group claims into both consumers, then read back.

    Order is deliberate. Groups first (nothing can be granted to a group
    that does not exist), then membership, then the preset model rows, then
    the Nomos bindings, then ONE readback covering all four. A caller never
    receives a projection that was only partly applied: the readback either
    matches the desired state or this function raises.

    Revocation is part of the projection, not a separate cleanup. A person
    the identity provider no longer places in either group is removed from
    the chat group and has their binding deactivated in the same pass. A
    projection that only ever ADDS is how a removed admin keeps their access
    until someone remembers to look.
    """
    if not sso.multi_user:
        raise ChatSsoProjectionError(
            f"refusing to project groups for deployment {deployment!r}: "
            f"chat.sso.mode is {sso.mode!r}. Single-operator mode has no "
            "identity provider to project group claims from",
            data={"deployment": deployment, "reason": "not_multi_user", "mode": sso.mode},
            fix_hint="declare chat.sso.mode: multi_user",
        )
    projected = sso.projected_groups
    if len(set(projected)) != 2:
        raise ChatSsoProjectionError(
            f"refusing to project groups for deployment {deployment!r}: "
            f"chat.sso projects {list(projected)}, which is not two distinct "
            "groups. An empty or collapsed group set fails every membership "
            "test and would deny everyone or admit everyone",
            data={
                "deployment": deployment,
                "reason": "group_set_degenerate",
                "projected": list(projected),
            },
            fix_hint="declare distinct chat.sso.admin_group and chat.sso.user_group",
        )
    resolved_scope = scope or f"deployment:{deployment}"
    grants = tuple(preset_grants or default_preset_grants(deployment, sso))
    # The mapping's own subject claim is READ (an incomplete mapping is
    # refused below) but never used as the binding's claim path -- see
    # BINDING_CLAIM_PATH for why a manifest-named claim resolves to nothing.
    subject_claim = str(getattr(principal_mapping, "subject_claim", "") or "")
    claim_path = BINDING_CLAIM_PATH
    provider_id = str(getattr(principal_mapping, "provider_id", "") or "")
    issuer = str(getattr(principal_mapping, "issuer", "") or "")
    if not subject_claim or not provider_id or not issuer:
        raise ChatSsoProjectionError(
            f"refusing to project groups for deployment {deployment!r}: the "
            "principal mapping does not name a provider, an issuer and a "
            "subject claim, so a binding written from it would resolve for "
            "nobody",
            data={
                "deployment": deployment,
                "reason": "principal_mapping_incomplete",
                "provider_id": provider_id,
                "issuer": issuer,
                "subject_claim": subject_claim,
            },
            fix_hint="declare nomos.principal_mapping completely",
        )

    changed = False

    # 1. Groups.
    existing = {
        str(row.get("name")): str(row.get("id"))
        for row in chat_client.list_groups()
        if row.get("name") and row.get("id")
    }
    group_ids: dict[str, str] = {}
    for name in projected:
        if name in existing:
            group_ids[name] = existing[name]
            continue
        created = chat_client.create_group(
            name=name,
            description=(
                f"Projected from the {deployment} identity provider's "
                f"{sso.groups_claim} claim by okg. Edit the identity "
                "provider, not this group."
            ),
        )
        identifier = str(created.get("id") or "")
        if not identifier:
            raise ChatSsoProjectionError(
                f"creating chat group {name!r} returned no id, so nothing "
                "could be granted to it",
                data={"deployment": deployment, "reason": "group_create_no_id", "group": name},
            )
        group_ids[name] = identifier
        changed = True

    # 2. Membership, in both directions.
    current_members = _members_by_group(chat_client, group_ids)
    results: list[ProjectedPrincipal] = []
    for entry in memberships:
        state = membership_state(entry.groups, sso)
        target_group = (
            sso.admin_group
            if state == MEMBER_ADMIN
            else sso.user_group
            if state == MEMBER_USER
            else None
        )
        for name, gid in group_ids.items():
            member_now = entry.chat_user_id in current_members.get(name, set())
            should_be = name == target_group and entry.chat_user_id is not None
            if should_be and not member_now:
                chat_client.add_group_member(
                    group_id=gid, user_id=str(entry.chat_user_id),
                )
                changed = True
            elif member_now and not should_be:
                chat_client.remove_group_member(
                    group_id=gid, user_id=str(entry.chat_user_id),
                )
                changed = True
        results.append(
            ProjectedPrincipal(
                principal=entry.principal,
                membership=state,
                idp_groups=tuple(entry.groups),
                chat_group=target_group,
                chat_user_id=entry.chat_user_id,
                nomos_subject=(
                    nomos_subject_for(target_group) if target_group else None
                ),
                denials=_denials_for(
                    entry.principal,
                    state,
                    sso,
                    has_account=entry.chat_user_id is not None,
                ),
            )
        )

    # 3. Preset model rows. Without these a group member sees nothing.
    if _reconcile_preset_rows(chat_client, grants, group_ids, deployment):
        changed = True

    # 4. Nomos bindings, in both directions.
    if _reconcile_bindings(
        binding_store=binding_store,
        deployment=deployment,
        scope=resolved_scope,
        provider_id=provider_id,
        issuer=issuer,
        claim_path=claim_path,
        results=results,
    ):
        changed = True

    readback = _verify_readback(
        deployment=deployment,
        scope=resolved_scope,
        chat_client=chat_client,
        binding_store=binding_store,
        group_ids=group_ids,
        grants=grants,
        results=results,
        provider_id=provider_id,
        issuer=issuer,
        claim_path=claim_path,
    )
    return GroupProjection(
        deployment=deployment,
        scope=resolved_scope,
        group_ids=group_ids,
        principals=tuple(results),
        preset_grants=grants,
        changed=changed,
        readback=readback,
    )


def _denials_for(
    principal: str,
    state: str,
    sso: ChatSso,
    *,
    has_account: bool = True,
) -> tuple[TypedDenial, ...]:
    """The typed denials one person receives, one per surface.

    They are values, not log lines: the three-membership-state proof asserts
    on them, so "the non-member is denied" is checkable rather than inferred
    from an absence of grants.

    Two cases produce them. The "neither" state -- membership absence, on
    every surface. And a MEMBER with no instance account, whose chat surface
    is unreachable for a reason that is not a policy decision and must not
    read as one.
    """
    if state != MEMBER_NONE:
        if has_account:
            return ()
        return (
            TypedDenial(
                surface="chat",
                principal=principal,
                reason_code=NO_INSTANCE_ACCOUNT_REASON,
                detail=(
                    f"principal is in the {state} group but has no account on "
                    "the chat instance yet, so there is no instance user id "
                    "to add to the group. The graph binding IS in place; the "
                    "chat surface is not, and that gap is reported rather "
                    "than left to be discovered"
                ),
            ),
        )
    detail = (
        f"principal is a member of neither {sso.admin_group!r} nor "
        f"{sso.user_group!r}"
    )
    return (
        TypedDenial(
            surface="chat",
            principal=principal,
            reason_code=PRESET_DENIAL_REASON,
            detail=(
                f"{detail}, so no access grant reaches this deployment's "
                "presets and the instance lists none of them"
            ),
        ),
        TypedDenial(
            surface="console",
            principal=principal,
            reason_code=CONSOLE_DENIAL_REASON,
            detail=f"{detail}, so the fronting proxy refuses the console",
        ),
        TypedDenial(
            surface="graph",
            principal=principal,
            reason_code=GRAPH_DENIAL_REASON,
            detail=(
                f"{detail}, so no Nomos subject binding exists and mcp-serve "
                "refuses the request rather than downgrading it to a default "
                "subject"
            ),
        ),
    )


def _members_by_group(
    chat_client: ChatGroupClient, group_ids: Mapping[str, str],
) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {name: set() for name in group_ids}
    for row in chat_client.list_groups():
        name = str(row.get("name") or "")
        if name not in out:
            continue
        members = row.get("user_ids")
        if not isinstance(members, Sequence) or isinstance(members, (str, bytes)):
            # Not "no members": UNREADABLE. Reconciling against an assumed
            # empty set would remove everyone from a group whose membership
            # simply did not come back.
            raise ChatSsoProjectionError(
                f"chat group {name!r} carries no readable user_ids, so its "
                "membership could not be measured; refusing to reconcile "
                "against an assumed-empty set",
                data={"reason": "group_membership_unreadable", "group": name},
            )
        out[name] = {str(item) for item in members}
    return out


def _reconcile_preset_rows(
    chat_client: ChatGroupClient,
    grants: Sequence[PresetGrant],
    group_ids: Mapping[str, str],
    deployment: str,
) -> bool:
    """Create or correct the model row that makes a preset visible.

    This is the step the v0.11.0 probe proved is not optional. A preset with
    no model row is admin-only and says nothing about it; a preset whose row
    grants the wrong group is invisible to the right one.
    """
    changed = False
    existing = {
        str(row.get("id")): row
        for row in chat_client.list_models()
        if row.get("id")
    }
    for grant in grants:
        desired = _desired_access_grants(grant, group_ids, deployment)
        row = existing.get(grant.model_id)
        if row is not None and _grants_match(row.get("access_grants"), desired):
            continue
        chat_client.upsert_model(model_id=grant.model_id, access_grants=desired)
        changed = True
    return changed


def _desired_access_grants(
    grant: PresetGrant,
    group_ids: Mapping[str, str],
    deployment: str,
) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for name in grant.groups:
        identifier = group_ids.get(name)
        if not identifier:
            raise ChatSsoProjectionError(
                f"preset {grant.model_id!r} grants group {name!r}, which the "
                "projection does not create; the grant would name nothing "
                "and the preset would be admin-only with no error",
                data={
                    "deployment": deployment,
                    "reason": "grant_names_unprojected_group",
                    "model_id": grant.model_id,
                    "group": name,
                },
                fix_hint="grant only chat.sso.admin_group / chat.sso.user_group",
            )
        out.append({
            "principal_type": "group",
            "principal_id": identifier,
            "permission": "read",
        })
    return sorted(out, key=lambda item: item["principal_id"])


def _grants_match(observed: Any, desired: Sequence[Mapping[str, str]]) -> bool:
    if not isinstance(observed, Sequence) or isinstance(observed, (str, bytes)):
        return False
    normalized = sorted(
        (
            {
                "principal_type": str(item.get("principal_type")),
                "principal_id": str(item.get("principal_id")),
                "permission": str(item.get("permission")),
            }
            for item in observed
            if isinstance(item, Mapping)
        ),
        key=lambda item: item["principal_id"],
    )
    return normalized == [dict(item) for item in desired]


def _reconcile_bindings(
    *,
    binding_store: NomosBindingStore,
    deployment: str,
    scope: str,
    provider_id: str,
    issuer: str,
    claim_path: str,
    results: Sequence[ProjectedPrincipal],
) -> bool:
    from okg.substrate.nomos.identity import SubjectBinding

    changed = False
    owned = {
        str(getattr(binding, "binding_id", "")): binding
        for binding in binding_store.load_bindings(scope=scope)
        if str(getattr(binding, "binding_id", "")).startswith(_BINDING_PREFIX)
    }
    desired_ids: set[str] = set()
    for item in results:
        if item.nomos_subject is None:
            continue
        binding_id = binding_id_for(
            deployment=deployment,
            provider_id=provider_id,
            claim_path=claim_path,
            principal=item.principal,
            subject=item.nomos_subject,
        )
        desired_ids.add(binding_id)
        current = owned.get(binding_id)
        if (
            current is not None
            and str(getattr(current, "subject", "")) == item.nomos_subject
            and str(getattr(current, "claim_value", "")) == item.principal
            and str(getattr(current, "claim_path", "")) == claim_path
        ):
            continue
        binding_store.upsert_binding(
            SubjectBinding(
                binding_id=binding_id,
                provider_id=provider_id,
                issuer=issuer,
                # `external_subject`, NOT "groups" and not the manifest's
                # subject claim. The group claim never travels in the
                # principal header (mcp-principal-mapping refuses it as a
                # reserved claim), and a manifest-named claim resolves to an
                # empty tuple (see BINDING_CLAIM_PATH). The group fact lives
                # in the SUBJECT this binding resolves to.
                claim_path=claim_path,
                claim_value=item.principal,
                subject=item.nomos_subject,
                scope=scope,
            )
        )
        changed = True
    for binding_id in sorted(set(owned) - desired_ids):
        binding_store.deactivate_binding(binding_id=binding_id)
        changed = True
    return changed


def _verify_readback(
    *,
    deployment: str,
    scope: str,
    chat_client: ChatGroupClient,
    binding_store: NomosBindingStore,
    group_ids: Mapping[str, str],
    grants: Sequence[PresetGrant],
    results: Sequence[ProjectedPrincipal],
    provider_id: str,
    issuer: str,
    claim_path: str,
) -> dict[str, Any]:
    """Re-read every write and refuse on any difference.

    Compared against the DESIRED state -- the manifest's groups composed with
    the measured membership -- never against a structure this module just
    built in memory. Comparing a projection to itself proves the projection
    ran, not that it landed.
    """
    diff: dict[str, Any] = {}

    observed_groups = {
        str(row.get("name")): row for row in chat_client.list_groups()
    }
    for name in group_ids:
        if name not in observed_groups:
            diff.setdefault("groups_absent", []).append(name)

    members = _members_by_group(chat_client, group_ids)
    for item in results:
        for name in group_ids:
            member_now = (
                item.chat_user_id is not None
                and item.chat_user_id in members.get(name, set())
            )
            should_be = name == item.chat_group and item.chat_user_id is not None
            if member_now != should_be:
                diff.setdefault("membership", []).append({
                    "principal": item.principal,
                    "group": name,
                    "expected_member": should_be,
                    "observed_member": member_now,
                })

    observed_models = {
        str(row.get("id")): row for row in chat_client.list_models()
    }
    for grant in grants:
        row = observed_models.get(grant.model_id)
        desired = _desired_access_grants(grant, group_ids, deployment)
        if row is None:
            # The v0.11.0 trap, stated as a readback failure rather than a
            # missing nicety: no row means the preset is admin-only and
            # silent about it.
            diff.setdefault("preset_rows_absent", []).append(grant.model_id)
            continue
        if not _grants_match(row.get("access_grants"), desired):
            diff.setdefault("preset_grants", []).append({
                "model_id": grant.model_id,
                "expected": desired,
                "observed": row.get("access_grants"),
            })

    bindings = {
        str(getattr(binding, "binding_id", "")): binding
        for binding in binding_store.load_bindings(scope=scope)
    }
    for item in results:
        expected_id = (
            binding_id_for(
                deployment=deployment,
                provider_id=provider_id,
                claim_path=claim_path,
                principal=item.principal,
                subject=item.nomos_subject,
            )
            if item.nomos_subject
            else None
        )
        if expected_id is None:
            # Two different situations, reported as two different things.
            #
            # A binding this projection OWNS and failed to revoke is a bug
            # here, and re-running fixes it. A binding somebody else
            # authored (through the Nomos store helper upsert_subject_binding)
            # that resolves the same principal is a real grant this projection cannot
            # revoke -- and reporting it as the same failure made the
            # readback permanently red with no action that could clear it.
            ours = sorted(
                identifier
                for identifier, binding in bindings.items()
                if identifier.startswith(_BINDING_PREFIX)
                and str(getattr(binding, "claim_value", "")) == item.principal
            )
            foreign = sorted(
                identifier
                for identifier, binding in bindings.items()
                if not identifier.startswith(_BINDING_PREFIX)
                and str(getattr(binding, "claim_value", "")) == item.principal
            )
            if ours:
                diff.setdefault("bindings_not_revoked", []).append({
                    "principal": item.principal,
                    "binding_ids": ours,
                })
            if foreign:
                diff.setdefault("foreign_bindings_for_non_member", []).append({
                    "principal": item.principal,
                    "binding_ids": foreign,
                    "action": (
                        "disable it with `okg nomos binding disable "
                        "--binding-id <id>`; this projection only owns "
                        f"bindings prefixed {_BINDING_PREFIX!r}"
                    ),
                })
            continue
        binding = bindings.get(expected_id)
        if binding is None:
            diff.setdefault("bindings_absent", []).append({
                "principal": item.principal,
                "binding_id": expected_id,
            })
            continue
        if str(getattr(binding, "subject", "")) != item.nomos_subject:
            diff.setdefault("binding_subject", []).append({
                "principal": item.principal,
                "expected": item.nomos_subject,
                "observed": str(getattr(binding, "subject", "")),
            })

    if diff:
        raise ChatSsoProjectionError(
            f"the group projection for deployment {deployment!r} did not "
            "read back as written. A partial projection is a failure, not a "
            "smaller success: the differences below each describe a person "
            "who would see the wrong thing and no error",
            data={
                "deployment": deployment,
                "reason": "readback_mismatch",
                "readback_diff": diff,
            },
            fix_hint=(
                "re-run the projection once the instance and the graph are "
                "reachable and writable"
            ),
        )
    return {
        "groups": sorted(group_ids),
        "members": {name: sorted(values) for name, values in members.items()},
        "preset_rows": sorted(
            grant.model_id for grant in grants if grant.model_id in observed_models
        ),
        # The OBSERVED grants, not the desired ones. `visible_presets` answers
        # "what will this person see" from what the instance actually holds,
        # so a row that landed wrong shows up as reduced visibility rather
        # than as the intent restated.
        "preset_row_grants": {
            grant.model_id: [
                {
                    "principal_type": str(entry.get("principal_type")),
                    "principal_id": str(entry.get("principal_id")),
                    "permission": str(entry.get("permission")),
                }
                for entry in (
                    observed_models[grant.model_id].get("access_grants") or ()
                )
                if isinstance(entry, Mapping)
            ]
            for grant in grants
            if grant.model_id in observed_models
        },
        "bindings": sorted(
            identifier
            for identifier in bindings
            if identifier.startswith(_BINDING_PREFIX)
        ),
    }


# ---------------------------------------------------------------------------
# Reading the projection back out
# ---------------------------------------------------------------------------

def visible_presets(
    principal: str,
    projection: GroupProjection,
    *,
    is_instance_admin: bool = False,
) -> tuple[str, ...]:
    """Which presets this principal's chat surface will list.

    Reproduces the v0.11.0 rule the probe measured, including the part that
    bites: a model with NO database row is visible to instance admins only.
    That is why the projection creates the rows -- and why this function
    takes ``is_instance_admin`` explicitly rather than inferring visibility
    from group membership, which would hide the very asymmetry being
    checked.
    """
    item = projection.principal(principal)
    granted = {
        group_id
        for name, group_id in projection.group_ids.items()
        if name == item.chat_group
    }
    rows = dict(projection.readback.get("preset_row_grants") or {})
    out: list[str] = []
    for grant in projection.preset_grants:
        observed = rows.get(grant.model_id)
        if observed is None:
            # No database row. The measured v0.11.0 rule: admins only, and
            # nobody else is told why.
            if is_instance_admin:
                out.append(grant.model_id)
            continue
        if any(
            str(entry.get("principal_id")) in granted for entry in observed
        ):
            out.append(grant.model_id)
    return tuple(out)
