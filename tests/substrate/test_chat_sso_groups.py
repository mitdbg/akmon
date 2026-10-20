"""Group projection with three-state readback (PACT chat-sso-integration).

``req.chat-sso-integration.group-projection``: the deployment's groups come
from the identity provider's group claims, are projected ONCE, and are read
back for three membership states -- admin-group member, user-group member,
and neither -- with the non-member arm producing typed denials on both
surfaces.

Two things make these arms non-vacuous rather than a projection compared to
itself.

* **The fake instance models what was MEASURED, including the traps.**
  ``FakeOpenWebUi`` reproduces the v0.11.0 behaviours the read-handshake
  spike recorded: a model with no database row is visible to instance admins
  ONLY, and ``groups/id/<gid>/update`` carrying ``user_ids`` is a no-op for
  membership. A projection that reconciled membership through the update
  verb, or that shipped a preset without its model row, passes neither the
  readback nor the visibility assertions here.
* **Every denial arm is asserted against the pre-change surface too.** The
  "neither" state is checked to produce no chat group, no access grant, no
  Nomos binding AND three typed denials -- and the "before" control shows the
  same instance answering differently for the two member states, so the
  denial is a decision rather than the projection having done nothing at all.

The Postgres arm proves the Nomos half against the real store, so "the same
group facts mcp-principal-mapping consumes" is a row in
``okg.nomos_subject_bindings``, not an in-memory analogue of one.
"""
from __future__ import annotations

import itertools
from pathlib import Path
from typing import Any, Mapping, Sequence

import pytest

from okg.substrate.chat.sso import MEMBER_ADMIN, MEMBER_NONE, MEMBER_USER
from okg.substrate.chat.sso_groups import (
    BINDING_CLAIM_PATH,
    CONSOLE_DENIAL_REASON,
    GRAPH_DENIAL_REASON,
    PRESET_DENIAL_REASON,
    ChatSsoProjectionError,
    NO_INSTANCE_ACCOUNT_REASON,
    IdpMembership,
    PresetGrant,
    binding_id_for,
    default_preset_grants,
    nomos_subject_for,
    project_deployment_groups,
    visible_presets,
)
from okg.substrate.deployments.chat_block import (
    SSO_MODE_MULTI_USER,
    SSO_MODE_SINGLE_OPERATOR,
    parse_chat_block,
)
from okg.substrate.mcp.principal_mapping import (
    UNIDENTIFIED_PRINCIPAL_REASON,
    PrincipalMapping,
)


REPO_ROOT = Path(__file__).resolve().parents[2]

DEPLOYMENT = "fixture-deployment"
SCOPE = f"deployment:{DEPLOYMENT}"
ADMIN_GROUP = "okg-admins"
USER_GROUP = "okg-users"
PRINCIPAL_HEADER = "X-OKG-Principal"
ISSUER = "https://idp.example.edu/"
PROVIDER_ID = "okg-chat-sso"

ADMIN_PRINCIPAL = "ada@example.edu"
USER_PRINCIPAL = "user@example.edu"
OUTSIDER_PRINCIPAL = "nobody@example.edu"

MAPPING = PrincipalMapping(
    provider_id=PROVIDER_ID,
    issuer=ISSUER,
    audience="okg-mcp",
    subject_claim="email",
    subject_header=PRINCIPAL_HEADER,
    principal_kind="mcp_client",
)


def _sso(mode: str = SSO_MODE_MULTI_USER, **overrides: Any):
    block: dict[str, Any] = {"mode": mode}
    if mode == SSO_MODE_MULTI_USER:
        block.update({
            "provider_id": PROVIDER_ID,
            "issuer": ISSUER,
            "audience": "okg-mcp",
            "discovery_endpoint": (
                "https://idp.example.edu/.well-known/openid-configuration"
            ),
            "client_id": "okg-chat",
            "client_secret_env": "OKG_CHAT_OIDC_CLIENT_SECRET",
            "principal_header": PRINCIPAL_HEADER,
            "groups_claim": "groups",
            "admin_group": ADMIN_GROUP,
            "user_group": USER_GROUP,
        })
    block.update(overrides)
    raw = {
        "name": DEPLOYMENT,
        "search": {
            "default_profile": "analyst",
            "profiles": {"analyst": {"subtypes": ["chunk"]}},
        },
        "chat": {
            "enabled": True,
            "ui": {"site_name": "Fixture"},
            "models": {"builtin": ["anthropic/claude-sonnet-5"]},
            "preset": {"display_name": "Fixture"},
            "search_profile": "analyst",
            "mcp": {
                "port": 8101,
                "tools": ["inspect"],
                "auth_token_env": "OKG_CHAT_MCP_TOKEN",
            },
            "sso": block,
        },
    }
    parsed = parse_chat_block(raw)
    assert parsed is not None and parsed.sso is not None
    return parsed.sso


# ==========================================================================
# The instance, as measured at v0.11.0
# ==========================================================================


class FakeOpenWebUi:
    """An Open WebUI instance that behaves the way the probe found it to.

    Every quirk reproduced here is a recorded measurement, not a guess:

    * ``update_group(user_ids=...)`` changes nothing about membership
      (``pipe-user-context-probe.md``: ``member_count`` stayed 0). The
      projection must use the member verb.
    * ``visible_models`` admits a model with NO row only for instance
      admins ("No DB entry means no access control configured yet"), which
      is why a preset without its model row is silently admin-only.
    """

    def __init__(self) -> None:
        self.groups: dict[str, dict[str, Any]] = {}
        self.models: dict[str, dict[str, Any]] = {}
        self._ids = itertools.count(1)
        self.calls: list[str] = []

    # -- the projection's port ------------------------------------------

    def list_groups(self) -> Sequence[Mapping[str, Any]]:
        self.calls.append("list_groups")
        return [
            {"id": row["id"], "name": name, "user_ids": sorted(row["user_ids"])}
            for name, row in sorted(self.groups.items())
        ]

    def create_group(self, *, name: str, description: str) -> Mapping[str, Any]:
        self.calls.append(f"create_group:{name}")
        identifier = f"grp-{next(self._ids)}"
        self.groups[name] = {
            "id": identifier,
            "description": description,
            "user_ids": set(),
        }
        return {"id": identifier, "name": name}

    def add_group_member(self, *, group_id: str, user_id: str) -> None:
        self.calls.append(f"add_group_member:{group_id}:{user_id}")
        self._by_id(group_id)["user_ids"].add(user_id)

    def remove_group_member(self, *, group_id: str, user_id: str) -> None:
        self.calls.append(f"remove_group_member:{group_id}:{user_id}")
        self._by_id(group_id)["user_ids"].discard(user_id)

    def list_models(self) -> Sequence[Mapping[str, Any]]:
        self.calls.append("list_models")
        return [
            {"id": model_id, "access_grants": list(row["access_grants"])}
            for model_id, row in sorted(self.models.items())
        ]

    def upsert_model(
        self, *, model_id: str, access_grants: Sequence[Mapping[str, Any]],
    ) -> Mapping[str, Any]:
        self.calls.append(f"upsert_model:{model_id}")
        self.models[model_id] = {"access_grants": [dict(g) for g in access_grants]}
        return {"id": model_id}

    # -- the trap the projection must not fall into ----------------------

    def update_group(self, *, group_id: str, user_ids: Sequence[str]) -> None:
        """A no-op for membership, exactly as measured."""
        self.calls.append(f"update_group:{group_id}")

    # -- what a caller would actually see --------------------------------

    def visible_models(self, user_id: str, *, is_admin: bool) -> list[str]:
        member_of = {
            row["id"] for row in self.groups.values() if user_id in row["user_ids"]
        }
        out: list[str] = []
        for model_id, row in sorted(self.models.items()):
            grants = row["access_grants"]
            if not grants:
                if is_admin:
                    out.append(model_id)
                continue
            if any(
                grant.get("principal_type") == "group"
                and grant.get("principal_id") in member_of
                for grant in grants
            ):
                out.append(model_id)
        return out

    def _by_id(self, group_id: str) -> dict[str, Any]:
        for row in self.groups.values():
            if row["id"] == group_id:
                return row
        raise KeyError(group_id)


class MemoryBindingStore:
    """An in-memory stand-in for ``okg.nomos_subject_bindings``.

    Deliberately the same three verbs the Postgres adapter exposes, so the
    hermetic arms and the Postgres arm exercise one code path in the
    projection rather than two.
    """

    def __init__(self) -> None:
        self.rows: dict[str, Any] = {}

    def load_bindings(self, *, scope: str) -> Sequence[Any]:
        return [
            binding for binding in self.rows.values()
            if getattr(binding, "scope", None) == scope
        ]

    def upsert_binding(self, binding: Any) -> None:
        self.rows[binding.binding_id] = binding

    def deactivate_binding(self, *, binding_id: str) -> None:
        self.rows.pop(binding_id, None)


class PostgresBindingStore:
    """The production adapter: the real Nomos identity store, nothing else."""

    def __init__(self, conn: Any, *, deployment_name: str) -> None:
        self._conn = conn
        self._deployment = deployment_name

    def load_bindings(self, *, scope: str) -> Sequence[Any]:
        from okg.substrate.nomos.identity_store import load_subject_bindings

        return load_subject_bindings(
            self._conn, deployment_name=self._deployment, scope=scope,
        )

    def upsert_binding(self, binding: Any) -> None:
        from okg.substrate.nomos.identity_store import upsert_subject_binding

        upsert_subject_binding(
            self._conn, deployment_name=self._deployment, binding=binding,
        )

    def deactivate_binding(self, *, binding_id: str) -> None:
        from okg.substrate.nomos.identity import SubjectBinding
        from okg.substrate.nomos.identity_store import upsert_subject_binding

        rows = {
            getattr(row, "binding_id", None): row
            for row in self.load_bindings(scope=SCOPE)
        }
        current = rows.get(binding_id)
        if current is None:
            return
        # 'disabled', not a word of our own choosing: the table's status
        # check admits only 'active' and 'disabled', and load_subject_bindings
        # returns the active ones. A status outside that set would abort the
        # transaction rather than revoke anything.
        upsert_subject_binding(
            self._conn,
            deployment_name=self._deployment,
            binding=SubjectBinding(**{
                **current.to_dict(),
                "required_assurance": tuple(current.required_assurance),
            }),
            status="disabled",
        )


THREE_STATES = (
    IdpMembership(
        principal=ADMIN_PRINCIPAL, groups=(ADMIN_GROUP,), chat_user_id="u-admin",
    ),
    IdpMembership(
        principal=USER_PRINCIPAL, groups=(USER_GROUP,), chat_user_id="u-user",
    ),
    IdpMembership(
        principal=OUTSIDER_PRINCIPAL,
        groups=("some-other-idp-group",),
        chat_user_id="u-outsider",
    ),
)


def _project(
    instance: FakeOpenWebUi,
    store: MemoryBindingStore,
    *,
    memberships: Sequence[IdpMembership] = THREE_STATES,
    sso: Any = None,
):
    return project_deployment_groups(
        deployment=DEPLOYMENT,
        sso=sso if sso is not None else _sso(),
        principal_mapping=MAPPING,
        memberships=memberships,
        chat_client=instance,
        binding_store=store,
    )


@pytest.fixture()
def projected():
    instance = FakeOpenWebUi()
    store = MemoryBindingStore()
    projection = _project(instance, store)
    return projection, instance, store


# ==========================================================================
# Three membership states
# ==========================================================================


def test_the_projection_creates_both_groups_from_the_idp_vocabulary(
    projected,
) -> None:
    projection, instance, _ = projected
    assert sorted(projection.group_ids) == sorted([ADMIN_GROUP, USER_GROUP])
    # Verbatim: no prefix, no normalization, no second vocabulary.
    assert set(instance.groups) == {ADMIN_GROUP, USER_GROUP}


@pytest.mark.parametrize(
    "principal, membership, group",
    [
        (ADMIN_PRINCIPAL, MEMBER_ADMIN, ADMIN_GROUP),
        (USER_PRINCIPAL, MEMBER_USER, USER_GROUP),
        (OUTSIDER_PRINCIPAL, MEMBER_NONE, None),
    ],
)
def test_each_membership_state_reads_back(
    projected, principal: str, membership: str, group: str | None,
) -> None:
    projection, instance, _ = projected
    item = projection.principal(principal)
    assert item.membership == membership
    assert item.chat_group == group
    members = {
        name: set(row["user_ids"]) for name, row in instance.groups.items()
    }
    for name, ids in members.items():
        assert (item.chat_user_id in ids) is (name == group)


def test_the_admin_sees_the_admin_preset(projected) -> None:
    projection, instance, _ = projected
    seen = instance.visible_models("u-admin", is_admin=False)
    assert f"{DEPLOYMENT}-admin" in seen
    # The library's answer and the instance's answer are the same set: the
    # projection reports what the instance will actually list, not what it
    # intended.
    assert set(visible_presets(ADMIN_PRINCIPAL, projection)) == set(seen)


def test_the_user_sees_the_user_preset_only(projected) -> None:
    projection, instance, _ = projected
    seen = instance.visible_models("u-user", is_admin=False)
    assert seen == [f"{DEPLOYMENT}-analyst"]
    assert f"{DEPLOYMENT}-admin" not in seen
    assert set(visible_presets(USER_PRINCIPAL, projection)) == set(seen)


def test_the_non_member_sees_no_deployment_preset(projected) -> None:
    projection, instance, _ = projected
    assert instance.visible_models("u-outsider", is_admin=False) == []
    assert visible_presets(OUTSIDER_PRINCIPAL, projection) == ()


def test_the_non_member_gets_typed_denials_on_both_surfaces(projected) -> None:
    """Membership absence is a denial state, spelled out, per surface."""
    projection, _, _ = projected
    item = projection.principal(OUTSIDER_PRINCIPAL)
    by_surface = {denial.surface: denial for denial in item.denials}
    assert set(by_surface) == {"chat", "console", "graph"}
    assert by_surface["chat"].reason_code == PRESET_DENIAL_REASON
    assert by_surface["console"].reason_code == CONSOLE_DENIAL_REASON
    # The graph-side code is the one mcp-principal-mapping actually emits,
    # imported rather than restated so it cannot drift.
    assert by_surface["graph"].reason_code == UNIDENTIFIED_PRINCIPAL_REASON
    assert by_surface["graph"].reason_code == GRAPH_DENIAL_REASON
    for denial in item.denials:
        assert ADMIN_GROUP in denial.detail and USER_GROUP in denial.detail


def test_members_carry_no_denials(projected) -> None:
    """The control that keeps the denial arm from passing for everyone."""
    projection, _, _ = projected
    assert projection.principal(ADMIN_PRINCIPAL).denials == ()
    assert projection.principal(USER_PRINCIPAL).denials == ()


def test_a_member_with_no_instance_account_is_reported_not_skipped() -> None:
    """The quiet gap, made loud.

    Group membership is real, so the graph binding is written; the instance
    has no user id to add to a group, so the chat surface is unreachable.
    Both "expected member: no" and "observed member: no" are true, so the
    readback is clean and nothing would otherwise say that a real member
    cannot open chat.
    """
    instance = FakeOpenWebUi()
    store = MemoryBindingStore()
    projection = _project(
        instance,
        store,
        memberships=(
            IdpMembership(
                principal=ADMIN_PRINCIPAL,
                groups=(ADMIN_GROUP,),
                chat_user_id=None,
            ),
        ),
    )
    item = projection.principal(ADMIN_PRINCIPAL)
    assert item.membership == MEMBER_ADMIN
    # The graph half landed.
    assert item.nomos_subject == nomos_subject_for(ADMIN_GROUP)
    assert any(
        binding.claim_value == ADMIN_PRINCIPAL for binding in store.rows.values()
    )
    # The chat half did not, and says so.
    assert instance.groups[ADMIN_GROUP]["user_ids"] == set()
    assert [denial.reason_code for denial in item.denials] == [
        NO_INSTANCE_ACCOUNT_REASON
    ]
    # And it is NOT the membership-absence denial: this person is a member.
    assert CONSOLE_DENIAL_REASON not in {d.reason_code for d in item.denials}


# ==========================================================================
# The Nomos half: the same group facts, in the store that serves reads
# ==========================================================================


def test_bindings_resolve_the_principal_to_a_group_named_subject(
    projected,
) -> None:
    projection, _, store = projected
    for principal, group in (
        (ADMIN_PRINCIPAL, ADMIN_GROUP),
        (USER_PRINCIPAL, USER_GROUP),
    ):
        item = projection.principal(principal)
        assert item.nomos_subject == nomos_subject_for(group)
        binding = store.rows[
            binding_id_for(
                deployment=DEPLOYMENT,
                provider_id=PROVIDER_ID,
                claim_path=BINDING_CLAIM_PATH,
                principal=principal,
                subject=item.nomos_subject,
            )
        ]
        assert binding.subject == f"subject:{group}"
        assert binding.claim_value == principal
        assert binding.scope == SCOPE


def test_bindings_are_keyed_on_a_claim_path_the_envelope_answers_for(
    projected,
) -> None:
    """Not ``groups``, and not the manifest's own claim name.

    Both alternatives resolve to an empty tuple through
    ``IdentityClaimEnvelope.claim_values`` -- ``groups`` because the group
    claim never travels in the principal header, and ``email`` (the
    fixture's ``subject_claim``) because the envelope answers only for a
    fixed vocabulary. Either would produce bindings that match nobody while
    reading, in the audit trail, as a person who simply has no subject.
    """
    from okg.substrate.nomos.identity import IdentityClaimEnvelope

    _, _, store = projected
    paths = {binding.claim_path for binding in store.rows.values()}
    assert paths == {BINDING_CLAIM_PATH}
    assert "groups" not in paths
    assert MAPPING.subject_claim not in paths

    probe = IdentityClaimEnvelope(
        provider_id=PROVIDER_ID,
        provider_type="mcp_principal_header",
        issuer=ISSUER,
        external_subject=ADMIN_PRINCIPAL,
        audiences=("okg-mcp",),
    )
    assert probe.claim_values(BINDING_CLAIM_PATH) == (ADMIN_PRINCIPAL,)
    # The two rejected alternatives, measured rather than asserted.
    assert probe.claim_values(MAPPING.subject_claim) == ()
    assert probe.claim_values("groups") == ()


def test_the_non_member_gets_no_binding(projected) -> None:
    projection, _, store = projected
    assert projection.principal(OUTSIDER_PRINCIPAL).nomos_subject is None
    assert all(
        binding.claim_value != OUTSIDER_PRINCIPAL
        for binding in store.rows.values()
    )


def test_the_binding_claim_would_resolve_on_the_request_path() -> None:
    """The projected binding matches the claim the header path builds.

    Not a restatement of the projection: the claim comes from
    ``header_claim_for_request`` and the match test comes from
    ``SubjectBinding.matches``. Both are the merged PACT's code. If this
    child projected a binding the request path cannot resolve, the site
    would authenticate people and the graph would still refuse them.
    """
    from okg.substrate.mcp.principal_mapping import (
        TRANSPORT_VERIFIER_PREFIX,
        header_claim_for_request,
    )
    from okg.substrate.nomos.identity import IdentityClaimEnvelope

    instance = FakeOpenWebUi()
    store = MemoryBindingStore()
    projection = _project(instance, store)

    claim = header_claim_for_request(
        MAPPING,
        {PRINCIPAL_HEADER: ADMIN_PRINCIPAL},
        transport_identity=f"{TRANSPORT_VERIFIER_PREFIX}okg-mcp-static-bearer",
    )
    envelope = IdentityClaimEnvelope(
        provider_id=claim["provider_id"],
        provider_type="mcp_principal_header",
        issuer=claim["issuer"],
        audiences=tuple(claim["audiences"]),
        external_subject=claim["external_subject"],
        verification_status=claim["verification_status"],
    )
    subject = projection.principal(ADMIN_PRINCIPAL).nomos_subject
    binding = store.rows[
        binding_id_for(
            deployment=DEPLOYMENT,
            provider_id=PROVIDER_ID,
            claim_path=BINDING_CLAIM_PATH,
            principal=ADMIN_PRINCIPAL,
            subject=subject,
        )
    ]
    assert binding.matches(envelope, scope=SCOPE) is True

    # And the outsider's claim matches no projected binding at all, which is
    # what makes their refusal `unidentified_principal` rather than a
    # downgrade to some default subject.
    outsider = IdentityClaimEnvelope(
        provider_id=claim["provider_id"],
        provider_type="mcp_principal_header",
        issuer=claim["issuer"],
        audiences=tuple(claim["audiences"]),
        external_subject=OUTSIDER_PRINCIPAL,
        verification_status="valid",
    )
    assert not any(
        binding.matches(outsider, scope=SCOPE)
        for binding in store.rows.values()
    )


# ==========================================================================
# The v0.11.0 preset-visibility trap
# ==========================================================================


def test_a_preset_without_a_model_row_is_silently_admin_only() -> None:
    """The measured trap, reproduced, so the fix below is not decorative.

    Groups exist, membership is correct, and the group member still sees
    nothing -- with no error. This is the state a projection that stopped at
    groups would leave behind.
    """
    instance = FakeOpenWebUi()
    instance.create_group(name=USER_GROUP, description="")
    instance.add_group_member(
        group_id=instance.groups[USER_GROUP]["id"], user_id="u-user",
    )
    instance.upsert_model(model_id=f"{DEPLOYMENT}-analyst", access_grants=[])
    assert instance.visible_models("u-user", is_admin=False) == []
    assert instance.visible_models("u-admin", is_admin=True) == [
        f"{DEPLOYMENT}-analyst"
    ]


def test_the_projection_creates_the_model_row_and_its_group_grant(
    projected,
) -> None:
    projection, instance, _ = projected
    assert set(instance.models) == {
        f"{DEPLOYMENT}-analyst", f"{DEPLOYMENT}-admin",
    }
    admin_id = projection.group_ids[ADMIN_GROUP]
    user_id = projection.group_ids[USER_GROUP]
    analyst_grants = instance.models[f"{DEPLOYMENT}-analyst"]["access_grants"]
    assert {grant["principal_id"] for grant in analyst_grants} == {
        admin_id, user_id,
    }
    assert all(
        grant["principal_type"] == "group" and grant["permission"] == "read"
        for grant in analyst_grants
    )
    admin_grants = instance.models[f"{DEPLOYMENT}-admin"]["access_grants"]
    assert {grant["principal_id"] for grant in admin_grants} == {admin_id}


def test_a_missing_model_row_fails_the_readback() -> None:
    """The trap is a readback failure, not a missing nicety."""
    instance = FakeOpenWebUi()
    store = MemoryBindingStore()

    class DropsModelRows(FakeOpenWebUi):
        def upsert_model(self, *, model_id, access_grants):  # noqa: ANN001, D102
            return {"id": model_id}

    dropping = DropsModelRows()
    with pytest.raises(ChatSsoProjectionError) as excinfo:
        _project(dropping, store)
    assert excinfo.value.data["reason"] == "readback_mismatch"
    assert excinfo.value.data["readback_diff"]["preset_rows_absent"]
    # The control: the same inputs against the honest instance succeed, so
    # the failure is about the dropped row and nothing else.
    assert _project(instance, MemoryBindingStore()) is not None


def test_membership_written_through_the_update_verb_fails_the_readback() -> None:
    """The other measured trap, as a failing projection.

    ``update_group`` is a no-op for membership. An instance whose member
    verb silently does nothing is indistinguishable from that mistake, and
    the readback catches both.
    """
    class NoOpMembership(FakeOpenWebUi):
        def add_group_member(self, *, group_id, user_id):  # noqa: ANN001, D102
            self.update_group(group_id=group_id, user_ids=[user_id])

    with pytest.raises(ChatSsoProjectionError) as excinfo:
        _project(NoOpMembership(), MemoryBindingStore())
    assert excinfo.value.data["reason"] == "readback_mismatch"
    assert excinfo.value.data["readback_diff"]["membership"]


# ==========================================================================
# Idempotence and revocation
# ==========================================================================


def test_a_second_run_applies_zero_changes(projected) -> None:
    projection, instance, store = projected
    assert projection.changed is True
    instance.calls.clear()
    again = _project(instance, store)
    assert again.changed is False
    assert not [call for call in instance.calls if not call.startswith("list_")]


def test_losing_the_idp_group_revokes_both_consumers(projected) -> None:
    """Revocation is part of the projection, not a later cleanup pass."""
    _, instance, store = projected
    demoted = (
        IdpMembership(
            principal=ADMIN_PRINCIPAL, groups=(), chat_user_id="u-admin",
        ),
        THREE_STATES[1],
        THREE_STATES[2],
    )
    after = _project(instance, store, memberships=demoted)
    assert after.changed is True
    item = after.principal(ADMIN_PRINCIPAL)
    assert item.membership == MEMBER_NONE
    assert "u-admin" not in instance.groups[ADMIN_GROUP]["user_ids"]
    assert all(
        binding.claim_value != ADMIN_PRINCIPAL for binding in store.rows.values()
    )
    assert {denial.reason_code for denial in item.denials} == {
        PRESET_DENIAL_REASON, CONSOLE_DENIAL_REASON, GRAPH_DENIAL_REASON,
    }


def test_a_person_promoted_to_admin_moves_groups(projected) -> None:
    _, instance, store = projected
    promoted = (
        THREE_STATES[0],
        IdpMembership(
            principal=USER_PRINCIPAL,
            groups=(ADMIN_GROUP,),
            chat_user_id="u-user",
        ),
        THREE_STATES[2],
    )
    after = _project(instance, store, memberships=promoted)
    assert after.principal(USER_PRINCIPAL).membership == MEMBER_ADMIN
    assert "u-user" in instance.groups[ADMIN_GROUP]["user_ids"]
    assert "u-user" not in instance.groups[USER_GROUP]["user_ids"]
    assert f"{DEPLOYMENT}-admin" in instance.visible_models(
        "u-user", is_admin=False,
    )


def test_holding_both_groups_resolves_to_admin(projected) -> None:
    _, instance, store = projected
    both = (
        IdpMembership(
            principal=USER_PRINCIPAL,
            groups=(USER_GROUP, ADMIN_GROUP),
            chat_user_id="u-user",
        ),
    )
    after = _project(instance, store, memberships=both)
    assert after.principal(USER_PRINCIPAL).membership == MEMBER_ADMIN


# ==========================================================================
# Refusals
# ==========================================================================


def test_a_degenerate_group_set_refuses() -> None:
    """Defensive, and proved rather than asserted.

    ``_parse_sso`` already refuses ``admin_group == user_group``, so this
    arm is not reachable through the manifest today. That is precisely why
    it is exercised directly: an unreachable guard nobody executes is a
    guard that rots into a no-op, and this one decides whether an empty or
    collapsed group set denies everyone or admits everyone.
    """
    from dataclasses import replace as dc_replace

    for degenerate in (
        dc_replace(_sso(), admin_group=USER_GROUP),
        dc_replace(_sso(), admin_group=None),
        dc_replace(_sso(), admin_group=None, user_group=None),
    ):
        with pytest.raises(ChatSsoProjectionError) as excinfo:
            _project(FakeOpenWebUi(), MemoryBindingStore(), sso=degenerate)
        assert excinfo.value.data["reason"] == "group_set_degenerate"


def test_single_operator_mode_projects_nothing() -> None:
    with pytest.raises(ChatSsoProjectionError) as excinfo:
        _project(
            FakeOpenWebUi(),
            MemoryBindingStore(),
            sso=_sso(mode=SSO_MODE_SINGLE_OPERATOR),
        )
    assert excinfo.value.data["reason"] == "not_multi_user"


def test_unreadable_group_membership_refuses_rather_than_assuming_empty() -> None:
    """An unreadable set is not an empty one; reconciling would evict everyone."""
    class Unreadable(FakeOpenWebUi):
        def list_groups(self):  # noqa: D102
            return [
                {"id": row["id"], "name": name, "user_ids": None}
                for name, row in self.groups.items()
            ]

    instance = Unreadable()
    instance.create_group(name=ADMIN_GROUP, description="")
    instance.create_group(name=USER_GROUP, description="")
    with pytest.raises(ChatSsoProjectionError) as excinfo:
        _project(instance, MemoryBindingStore())
    assert excinfo.value.data["reason"] == "group_membership_unreadable"


def test_a_foreign_binding_is_reported_with_an_action_not_a_deadlock() -> None:
    """A hand-authored binding is a real grant this projection cannot revoke.

    It must be reported -- it resolves a non-member to a subject, which is
    exactly the default-open state the requirement forbids -- but reporting
    it as an ordinary readback failure made the projection permanently red
    with no action that could clear it, because the revocation sweep only
    ever touches ids this projection owns.
    """
    from okg.substrate.nomos.identity import SubjectBinding

    store = MemoryBindingStore()
    store.upsert_binding(
        SubjectBinding(
            binding_id="binding:hand-authored:outsider",
            provider_id=PROVIDER_ID,
            issuer=ISSUER,
            claim_path=BINDING_CLAIM_PATH,
            claim_value=OUTSIDER_PRINCIPAL,
            subject="subject:legacy-operators",
            scope=SCOPE,
        )
    )
    with pytest.raises(ChatSsoProjectionError) as excinfo:
        _project(FakeOpenWebUi(), store)
    diff = excinfo.value.data["readback_diff"]
    assert "bindings_not_revoked" not in diff
    entry = diff["foreign_bindings_for_non_member"][0]
    assert entry["principal"] == OUTSIDER_PRINCIPAL
    assert entry["binding_ids"] == ["binding:hand-authored:outsider"]
    assert "okg nomos binding disable --binding-id" in entry["action"]

    # And the named action clears it: removing the foreign binding lets the
    # same projection succeed, so the report is actionable rather than a
    # dead end.
    store.deactivate_binding(binding_id="binding:hand-authored:outsider")
    assert _project(FakeOpenWebUi(), store) is not None


def test_a_grant_naming_an_unprojected_group_refuses() -> None:
    with pytest.raises(ChatSsoProjectionError) as excinfo:
        project_deployment_groups(
            deployment=DEPLOYMENT,
            sso=_sso(),
            principal_mapping=MAPPING,
            memberships=THREE_STATES,
            chat_client=FakeOpenWebUi(),
            binding_store=MemoryBindingStore(),
            preset_grants=[
                PresetGrant(model_id="rogue", groups=("cms-operators",)),
            ],
        )
    assert excinfo.value.data["reason"] == "grant_names_unprojected_group"


def test_an_incomplete_principal_mapping_refuses() -> None:
    from dataclasses import replace

    with pytest.raises(ChatSsoProjectionError) as excinfo:
        project_deployment_groups(
            deployment=DEPLOYMENT,
            sso=_sso(),
            principal_mapping=replace(MAPPING, subject_claim=""),
            memberships=THREE_STATES,
            chat_client=FakeOpenWebUi(),
            binding_store=MemoryBindingStore(),
        )
    assert excinfo.value.data["reason"] == "principal_mapping_incomplete"


def test_default_preset_grants_give_the_user_preset_to_both_groups() -> None:
    grants = default_preset_grants(DEPLOYMENT, _sso())
    by_id = {grant.model_id: grant.groups for grant in grants}
    assert set(by_id[f"{DEPLOYMENT}-analyst"]) == {ADMIN_GROUP, USER_GROUP}
    assert set(by_id[f"{DEPLOYMENT}-admin"]) == {ADMIN_GROUP}


# ==========================================================================
# Postgres: the Nomos half against the store that serves reads
# ==========================================================================


@pytest.mark.postgres
@pytest.mark.serial_db
def test_projection_writes_resolvable_bindings_to_postgres(
    clean_runtime: str,
) -> None:
    """The projected bindings are rows the real identity store returns.

    The hermetic arms prove the projection's logic; this one proves the
    adapter, so "the same group facts mcp-principal-mapping consumes" means
    a row in ``okg.nomos_subject_bindings`` rather than an in-memory
    analogue of one.
    """
    import psycopg

    from okg.substrate.nomos.identity_store import load_subject_bindings

    instance = FakeOpenWebUi()
    with psycopg.connect(clean_runtime) as conn:
        store = PostgresBindingStore(conn, deployment_name=DEPLOYMENT)
        projection = project_deployment_groups(
            deployment=DEPLOYMENT,
            sso=_sso(),
            principal_mapping=MAPPING,
            memberships=THREE_STATES,
            chat_client=instance,
            binding_store=store,
        )
        conn.commit()

        rows = {
            row.binding_id: row
            for row in load_subject_bindings(
                conn, deployment_name=DEPLOYMENT, scope=SCOPE,
            )
        }
        assert len(rows) == 2
        subjects = {row.claim_value: row.subject for row in rows.values()}
        assert subjects == {
            ADMIN_PRINCIPAL: nomos_subject_for(ADMIN_GROUP),
            USER_PRINCIPAL: nomos_subject_for(USER_GROUP),
        }
        assert OUTSIDER_PRINCIPAL not in subjects
        assert projection.changed is True

        # Idempotence against the real store, and revocation after it.
        again = project_deployment_groups(
            deployment=DEPLOYMENT,
            sso=_sso(),
            principal_mapping=MAPPING,
            memberships=THREE_STATES,
            chat_client=instance,
            binding_store=store,
        )
        conn.commit()
        assert again.changed is False

        demoted = (
            IdpMembership(
                principal=ADMIN_PRINCIPAL, groups=(), chat_user_id="u-admin",
            ),
            THREE_STATES[1],
            THREE_STATES[2],
        )
        project_deployment_groups(
            deployment=DEPLOYMENT,
            sso=_sso(),
            principal_mapping=MAPPING,
            memberships=demoted,
            chat_client=instance,
            binding_store=store,
        )
        conn.commit()
        active = load_subject_bindings(
            conn, deployment_name=DEPLOYMENT, scope=SCOPE,
        )
        assert {row.claim_value for row in active} == {USER_PRINCIPAL}
