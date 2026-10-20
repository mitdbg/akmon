"""Nomos policy compiler/evaluator CLI."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Callable

from okg.substrate.cli._options import (
    add_deployment_arg,
    add_dsn_arg,
)
from okg.substrate.cli._output import emit_result
from okg.substrate.cli.renamed_flags import add_renamed_argument
from okg.substrate.nomos import (
    NomosPolicyError,
    OIDCLoginConfig,
    build_refinement_state,
    check_noninterference_trace,
    compile_policy_graph,
    evaluate,
    institutional_provider_fixture,
    load_bundle,
    load_decision_input,
    load_policy_graph,
    run_demo,
    run_oidc_loopback_login,
)
from okg.substrate.nomos.core import write_json
from okg.substrate.nomos.refinement import NomosTraceError


JsonFlagAdder = Callable[[argparse.ArgumentParser], None]


def register_subparsers(
    sub: argparse._SubParsersAction,
    *,
    add_json_flag: JsonFlagAdder,
) -> None:
    p_nomos = sub.add_parser(
        "nomos",
        help="Nomos policy compile/evaluate/demo utilities",
    )
    add_json_flag(p_nomos)
    p_sub = p_nomos.add_subparsers(dest="nomos_subcmd", required=True)

    p_demo = p_sub.add_parser(
        "demo",
        help="Run a local end-to-end Nomos policy demo",
    )
    add_json_flag(p_demo)
    p_demo.add_argument("--out", default=".nomos-demo")
    p_demo.add_argument("--overwrite", action="store_true")

    p_compile = p_sub.add_parser(
        "compile",
        help="Compile graph-native policy JSON into a Nomos bundle",
    )
    add_json_flag(p_compile)
    p_compile.add_argument("--policy", required=True)
    p_compile.add_argument("--out", default=None)

    p_evaluate = p_sub.add_parser(
        "evaluate",
        help="Evaluate one bounded decision input",
    )
    add_json_flag(p_evaluate)
    p_evaluate.add_argument("--bundle", required=True)
    p_evaluate.add_argument("--input", required=True)
    p_evaluate.add_argument("--enforcement-point", default="nomos.cli")
    p_evaluate.add_argument("--out", default=None)

    p_inspect = p_sub.add_parser(
        "inspect",
        help="Inspect a compiled Nomos bundle or active Postgres state",
    )
    add_json_flag(p_inspect)
    p_inspect.add_argument("--bundle", default=None)
    add_dsn_arg(p_inspect)
    add_deployment_arg(p_inspect)
    p_inspect.add_argument("--scope", default=None)

    p_decision = p_sub.add_parser(
        "decision",
        help="Look up a decision in a decisions JSON file",
    )
    add_json_flag(p_decision)
    p_decision.add_argument("--decisions", required=True)
    p_decision.add_argument("--decision-id", required=True)

    p_decisions_prune = p_sub.add_parser(
        "decisions-prune",
        help=(
            "Prune aged, unreferenced rows from okg.nomos_decisions "
            "(the publisher also runs this every publish)"
        ),
    )
    add_json_flag(p_decisions_prune)
    add_dsn_arg(p_decisions_prune)
    add_deployment_arg(p_decisions_prune)
    p_decisions_prune.add_argument(
        "--days",
        type=int,
        default=None,
        help=(
            "age bound in days (default: $OKG_NOMOS_DECISION_RETENTION_DAYS "
            "or 90)"
        ),
    )
    p_decisions_prune.add_argument(
        "--apply",
        action="store_true",
        help=(
            "actually DELETE the aged rows. Without --apply the verb "
            "dry-runs: it reports the row count that would be pruned and "
            "deletes nothing (convention point (d): destructive verbs "
            "default to dry-run)."
        ),
    )

    p_trace = p_sub.add_parser(
        "trace",
        help="Build and check a Nomos refinement-state artifact",
    )
    add_json_flag(p_trace)
    p_trace.add_argument("--decisions", required=True)
    p_trace.add_argument("--bundle", default=None)
    p_trace.add_argument("--generation-id", default=None)
    p_trace.add_argument("--out", required=True)

    p_provider = p_sub.add_parser(
        "provider",
        help=(
            "Register, disable and list identity providers, and validate "
            "institutional IAM provider fixtures"
        ),
    )
    add_json_flag(p_provider)
    p_provider_sub = p_provider.add_subparsers(
        dest="nomos_provider_subcmd",
        required=True,
    )

    p_provider_fixture = p_provider_sub.add_parser(
        "fixture",
        help="Emit a redacted CERN/MIT provider profile fixture",
    )
    add_json_flag(p_provider_fixture)
    add_renamed_argument(
        p_provider_fixture, "--provider", "--profile",
        dest="profile",
        required=True,
        help="Identity-provider fixture to emit",
        choices=[
            "cern-oidc",
            "cern-saml",
            "mit-touchstone",
            "mit-kerberos-gssapi",
        ],
    )
    p_provider_fixture.add_argument("--audience", required=True)
    p_provider_fixture.add_argument("--provider-id", default=None)
    p_provider_fixture.add_argument("--issuer", default=None)
    p_provider_fixture.add_argument("--key-reference", default=None)
    p_provider_fixture.add_argument("--metadata-fingerprint", default=None)
    p_provider_fixture.add_argument("--service-principal", default=None)
    p_provider_fixture.add_argument("--out", default=None)

    p_provider_validate = p_provider_sub.add_parser(
        "validate",
        help="Validate a provider fixture JSON file without contacting an IdP",
    )
    add_json_flag(p_provider_validate)
    p_provider_validate.add_argument("--provider", required=True)

    p_provider_inspect = p_provider_sub.add_parser(
        "inspect",
        help="Inspect redacted provider state from a migrated Nomos database",
    )
    add_json_flag(p_provider_inspect)
    add_dsn_arg(p_provider_inspect, required=True)
    add_deployment_arg(p_provider_inspect, required=True)
    p_provider_inspect.add_argument("--provider-id", default=None)

    p_provider_drift = p_provider_sub.add_parser(
        "drift",
        help="Report Nomos provider drift using doctor checks",
    )
    add_json_flag(p_provider_drift)
    add_dsn_arg(p_provider_drift, required=True)
    add_deployment_arg(p_provider_drift, required=True)

    p_provider_readiness = p_provider_sub.add_parser(
        "readiness",
        help="Report institutional IAM readiness for a Nomos deployment",
    )
    add_json_flag(p_provider_readiness)
    add_dsn_arg(p_provider_readiness, required=True)
    add_deployment_arg(p_provider_readiness, required=True)
    p_provider_readiness.add_argument(
        "--production",
        action="store_true",
        help="Require production registration metadata and recent decision evidence",
    )

    p_provider_oidc_login = p_provider_sub.add_parser(
        "oidc-login",
        help="Run a local OIDC browser login and emit a bounded Nomos claim",
    )
    add_json_flag(p_provider_oidc_login)
    p_provider_oidc_login.add_argument("--issuer", required=True)
    p_provider_oidc_login.add_argument("--client-id", required=True)
    p_provider_oidc_login.add_argument("--provider-id", required=True)
    p_provider_oidc_login.add_argument(
        "--provider-type",
        default="oidc",
        choices=["oidc", "touchstone_oidc"],
    )
    p_provider_oidc_login.add_argument("--verified-by", required=True)
    p_provider_oidc_login.add_argument(
        "--client-secret-env",
        default=None,
        help="Environment variable containing the OIDC client secret",
    )
    p_provider_oidc_login.add_argument(
        "--client-auth-method",
        default="auto",
        choices=["auto", "client_secret_basic", "client_secret_post", "none"],
    )
    p_provider_oidc_login.add_argument(
        "--scopes",
        default="openid profile email",
        help="Space-delimited OIDC scopes; must include openid",
    )
    p_provider_oidc_login.add_argument("--redirect-port", type=int, default=8765)
    p_provider_oidc_login.add_argument(
        "--redirect-path",
        default="/nomos/oidc/callback",
    )
    p_provider_oidc_login.add_argument("--timeout-seconds", type=int, default=300)
    p_provider_oidc_login.add_argument("--leeway-seconds", type=int, default=60)
    p_provider_oidc_login.add_argument(
        "--fetch-userinfo",
        action="store_true",
        help="Fetch issuer UserInfo claims after ID-token verification",
    )
    p_provider_oidc_login.add_argument(
        "--include-claim",
        action="append",
        default=[],
        help="Additional verified claim key to preserve in the Nomos envelope",
    )
    p_provider_oidc_login.add_argument(
        "--no-browser",
        action="store_true",
        help="Print the authorization URL instead of opening a browser",
    )
    p_provider_oidc_login.add_argument("--out", required=True)

    _register_provider_admin_subparsers(p_provider_sub, add_json_flag=add_json_flag)
    _register_binding_subparsers(p_sub, add_json_flag=add_json_flag)
    _register_grant_subparsers(p_sub, add_json_flag=add_json_flag)


# The claim paths a binding can match on, as `IdentityClaimEnvelope.
# claim_values` names them. A binding whose path the envelope does not
# recognize matches nothing, so a typo would write a binding that silently
# never applies; the command refuses it instead.
BINDING_CLAIM_PATHS = ("external_subject", "roles", "groups", "entitlements")

BINDING_REVOCATION_LIMIT = (
    "Disabling a binding takes effect on the next request. The server does "
    "not ask the identity provider whether a token was revoked: a user "
    "disabled only at the provider keeps access until their token expires. "
    "Disable the binding here to cut access at once."
)


def _register_binding_subparsers(
    p_sub: argparse._SubParsersAction,
    *,
    add_json_flag: JsonFlagAdder,
) -> None:
    p_binding = p_sub.add_parser(
        "binding",
        help="Add, disable or list the subject bindings that map users to principals",
        description=(
            "Manage Nomos subject bindings: the rows that map a provider's "
            "user or role to a Nomos subject. " + BINDING_REVOCATION_LIMIT
        ),
    )
    add_json_flag(p_binding)
    p_binding_sub = p_binding.add_subparsers(
        dest="nomos_binding_subcmd",
        required=True,
    )

    p_add = p_binding_sub.add_parser(
        "add",
        help="Bind a provider's user or role to a Nomos subject",
        description=(
            "Bind one claim value from a registered identity provider to a "
            "Nomos subject. Refuses a provider the deployment has not "
            "registered. Refuses a binding id that already exists; pass "
            "--replace to change an active binding's window or priority. "
            "--replace never re-enables a disabled binding and never changes "
            "a binding's subject, provider, claim, scope or required "
            "assurance (such as an MFA requirement); use a new binding id "
            "for those. With --replace, --priority, --effective-from and "
            "--effective-until change only what you pass: a flag you leave "
            "out keeps the stored value. To remove a window bound on "
            "purpose, pass --clear-effective-from or --clear-effective-until. "
            "Every add, replace and disable is appended to the binding's "
            "metadata.events history; a replace records the priority and "
            "window it replaced."
        ),
    )
    add_json_flag(p_add)
    add_dsn_arg(p_add, help="Postgres DSN of the deployment; defaults to $OKG_DSN")
    add_deployment_arg(p_add, required=True)
    p_add.add_argument("--provider-id", required=True)
    p_add.add_argument(
        "--claim-value",
        required=True,
        help="the user (for external_subject) or role/group name to match",
    )
    p_add.add_argument(
        "--claim-path",
        default="external_subject",
        choices=BINDING_CLAIM_PATHS,
        help="which part of the verified claim to match (default: external_subject)",
    )
    p_add.add_argument(
        "--subject",
        required=True,
        help="the Nomos subject the match resolves to, e.g. subject:team:analyst",
    )
    p_add.add_argument(
        "--binding-id",
        default=None,
        help="default: binding:<provider-id>:<claim-path>:<claim-value>",
    )
    p_add.add_argument(
        "--scope",
        default=None,
        help="default: deployment:<deployment>, the scope the MCP gate reads",
    )
    p_add.add_argument(
        "--priority",
        type=int,
        default=None,
        help="default: 0 for a new binding; with --replace, the stored priority",
    )
    window_from = p_add.add_mutually_exclusive_group()
    window_from.add_argument(
        "--effective-from",
        default=None,
        help=(
            "ISO-8601 time with a UTC offset; the binding matches from then. "
            "With --replace, left out keeps the stored start"
        ),
    )
    window_from.add_argument(
        "--clear-effective-from",
        action="store_true",
        help="with --replace only: remove the stored start, so the binding matches at once",
    )
    window_until = p_add.add_mutually_exclusive_group()
    window_until.add_argument(
        "--effective-until",
        default=None,
        help=(
            "ISO-8601 time with a UTC offset; the binding stops matching then. "
            "With --replace, left out keeps the stored end"
        ),
    )
    window_until.add_argument(
        "--clear-effective-until",
        action="store_true",
        help=(
            "with --replace only: remove the stored end, so the binding never "
            "expires"
        ),
    )
    p_add.add_argument(
        "--replace",
        action="store_true",
        help=(
            "change an existing active binding's window or priority, keeping "
            "its history and every value you do not pass; refused for a "
            "disabled binding or a changed subject, provider, claim, scope or "
            "required assurance"
        ),
    )
    p_add.add_argument(
        "--reason",
        default=None,
        help="short note stored with this change in the binding's history",
    )
    _add_actor_arg(p_add, noun="binding")

    p_disable = p_binding_sub.add_parser(
        "disable",
        help="Disable a binding; the row is kept as history",
        description=(
            "Set a binding's status to disabled, together with every other "
            "active binding on the same provider, scope and claim (the same "
            "user, or the same role or group), so that claim stops admitting "
            "anyone on the next request. The output lists the ids disabled "
            "with it. Role, group or entitlement bindings the user's token "
            "may also carry are not disabled; the command lists them and "
            "warns. " + BINDING_REVOCATION_LIMIT
        ),
    )
    add_json_flag(p_disable)
    add_dsn_arg(p_disable, help="Postgres DSN of the deployment; defaults to $OKG_DSN")
    add_deployment_arg(p_disable, required=True)
    p_disable.add_argument("--binding-id", required=True)
    p_disable.add_argument(
        "--reason",
        default=None,
        help="short note stored in the binding's metadata",
    )
    _add_actor_arg(p_disable)

    p_list = p_binding_sub.add_parser(
        "list",
        help="List bindings, disabled and expired ones included",
    )
    add_json_flag(p_list)
    add_dsn_arg(p_list, help="Postgres DSN of the deployment; defaults to $OKG_DSN")
    add_deployment_arg(p_list, required=True)
    p_list.add_argument("--provider-id", default=None)

    for verb, text in (
        (
            "block",
            "Block one user of a provider. The block outranks every role, "
            "group and user binding whatever its priority: the next request "
            "from that user is refused (nomos_principal_blocked, reason "
            "nomos.identity.principal_blocked) and audited. The block is a "
            "binding row to the reserved subject nomos:blocked, with id "
            "binding:<provider-id>:block:<user>. Blocking again after an "
            "unblock re-applies the same row. The --user value must equal the token's "
            "subject-claim value exactly, case included; the command warns "
            "when this deployment has never seen that exact value. "
            + BINDING_REVOCATION_LIMIT,
        ),
        (
            "unblock",
            "Lift a user's block. Only the block row is disabled; the user's "
            "other bindings are left as they are, so role bindings admit them "
            "again on the next request.",
        ),
    ):
        p_verb = p_binding_sub.add_parser(
            verb, help=text.split(".")[0], description=text,
        )
        add_json_flag(p_verb)
        add_dsn_arg(p_verb, help="Postgres DSN of the deployment; defaults to $OKG_DSN")
        add_deployment_arg(p_verb, required=True)
        p_verb.add_argument("--provider-id", required=True)
        p_verb.add_argument(
            "--user",
            required=True,
            help="the user's value of the provider's subject claim, e.g. a CERN cern_upn",
        )
        if verb == "block":
            p_verb.add_argument(
                "--scope",
                default=None,
                help="default: deployment:<deployment>, the scope the MCP gate reads",
            )
        p_verb.add_argument(
            "--reason",
            default=None,
            help="short note stored in the block's history",
        )
        _add_actor_arg(p_verb, noun="binding")


PROVIDER_ADD_DESCRIPTION = (
    "Register an identity provider for one deployment, active at once. "
    "Refuses a provider id that already exists, whatever its status: an add "
    "never rewrites a registered provider and never re-enables a disabled "
    "one. When the deployment manifest declares nomos.oidc_auth for this "
    "provider id, the row must pass the same check the MCP server runs at "
    "start (issuer, audience, subject claim, 'roles' among the role claims, "
    "and 'mcp-oidc:jwks' among the trusted verifiers), or add refuses. "
    "--for-oidc-auth fills those fields from the manifest."
)

PROVIDER_DISABLE_DESCRIPTION = (
    "Set a provider's status to disabled. Every claim naming the provider is "
    "refused from the next request on, whatever bindings and grants exist. "
    "The row is kept, because bindings and decision records name it. "
    "Other active providers with the same issuer are listed with a warning, "
    "not disabled."
)


def _register_provider_admin_subparsers(
    p_provider_sub: argparse._SubParsersAction,
    *,
    add_json_flag: JsonFlagAdder,
) -> None:
    from okg.substrate.nomos.identity import IDENTITY_PROVIDER_TYPES

    p_add = p_provider_sub.add_parser(
        "add",
        help="Register an identity provider for a deployment",
        description=PROVIDER_ADD_DESCRIPTION,
    )
    add_json_flag(p_add)
    add_dsn_arg(p_add, help="Postgres DSN of the deployment; defaults to $OKG_DSN")
    add_deployment_arg(p_add, required=True)
    p_add.add_argument(
        "--for-oidc-auth",
        action="store_true",
        help=(
            "take the provider id, issuer, audience and subject claim from "
            "the deployment manifest's nomos.oidc_auth block, and trust "
            "mcp-oidc:jwks"
        ),
    )
    p_add.add_argument("--provider-id", default=None)
    p_add.add_argument(
        "--provider-type",
        default="oidc",
        choices=sorted(IDENTITY_PROVIDER_TYPES),
        help="default: oidc",
    )
    p_add.add_argument("--issuer", default=None, help="exactly as tokens' iss claim")
    p_add.add_argument(
        "--audience",
        action="append",
        default=None,
        help="an audience tokens must carry; repeat for several",
    )
    p_add.add_argument(
        "--subject-claim",
        default=None,
        help="the claim that names the user (default: sub)",
    )
    for flag, default in (
        ("--role-claim", "roles"),
        ("--group-claim", "groups"),
        ("--entitlement-claim", "entitlements"),
    ):
        p_add.add_argument(
            flag,
            action="append",
            default=None,
            help=f"repeat for several (default: {default})",
        )
    p_add.add_argument(
        "--trusted-verifier",
        action="append",
        default=None,
        help=(
            "a verifier id whose verified claims the provider accepts, stored "
            "in metadata.trusted_verifiers (the MCP OIDC mode stamps "
            "mcp-oidc:jwks); repeat for several"
        ),
    )
    p_add.add_argument(
        "--require-verified-by",
        action="store_true",
        help="refuse claims that name no verifier, even without --trusted-verifier",
    )
    p_add.add_argument(
        "--key-reference",
        default=None,
        help="a secret://, vault://, keyring:// or aws-sm:// reference, never key material",
    )
    p_add.add_argument("--metadata-fingerprint", default=None)
    p_add.add_argument("--freshness-seconds", type=int, default=None)
    p_add.add_argument(
        "--required-assurance",
        action="append",
        default=None,
        help="an assurance value every claim must carry; repeat for several",
    )
    p_add.add_argument("--mfa-required", action="store_true")
    p_add.add_argument(
        "--group-claims-approved",
        action="store_true",
        help="let group claims reach group bindings (off by default)",
    )
    p_add.add_argument("--realm", default=None)
    p_add.add_argument("--service-principal", default=None)
    p_add.add_argument(
        "--production",
        action="store_true",
        help=(
            "mark the registration as institution-owned production, which "
            "`okg nomos provider readiness --production` requires"
        ),
    )
    p_add.add_argument(
        "--reason",
        default=None,
        help="short note stored in the provider's history",
    )
    _add_actor_arg(p_add, noun="provider")

    p_disable = p_provider_sub.add_parser(
        "disable",
        help="Disable an identity provider; the row is kept as history",
        description=PROVIDER_DISABLE_DESCRIPTION,
    )
    add_json_flag(p_disable)
    add_dsn_arg(p_disable, help="Postgres DSN of the deployment; defaults to $OKG_DSN")
    add_deployment_arg(p_disable, required=True)
    p_disable.add_argument("--provider-id", required=True)
    p_disable.add_argument(
        "--reason",
        default=None,
        help="short note stored in the provider's metadata",
    )
    _add_actor_arg(p_disable, noun="provider")

    p_list = p_provider_sub.add_parser(
        "list",
        help="List identity providers, disabled ones included",
    )
    add_json_flag(p_list)
    add_dsn_arg(p_list, help="Postgres DSN of the deployment; defaults to $OKG_DSN")
    add_deployment_arg(p_list, required=True)
    p_list.add_argument("--provider-id", default=None)


GRANT_EFFECTS = ("allow", "deny")

GRANT_DISABLE_DESCRIPTION = (
    "Set a grant's status to disabled; the row is kept, because decision "
    "records name grant ids. The request gate applies only the "
    "highest-priority grant that matches, so disabling an allow grant can "
    "leave another allow grant permitting the same requests: the output "
    "lists those under still_allowing with a warning. Disabling a deny grant "
    "can admit requests it was blocking, so it is refused unless you pass "
    "--allow-widen."
)


def _register_grant_subparsers(
    p_sub: argparse._SubParsersAction,
    *,
    add_json_flag: JsonFlagAdder,
) -> None:
    p_grant = p_sub.add_parser(
        "grant",
        help="Add, disable or list the permission grants that say what a subject may do",
        description=(
            "Manage Nomos permission grants: the rows that let (or forbid) "
            "a Nomos subject an action, such as graph.read.mcp or "
            "mcp.tool.call, in one scope."
        ),
    )
    add_json_flag(p_grant)
    p_grant_sub = p_grant.add_subparsers(dest="nomos_grant_subcmd", required=True)

    p_add = p_grant_sub.add_parser(
        "add",
        help="Grant (or deny) a Nomos subject some actions",
        description=(
            "Write one permission grant, active at once. Refuses a grant id "
            "that already exists, whatever its status: an add never "
            "rewrites a grant and never re-enables a disabled one. To change "
            "a grant, disable it and add a new grant id. The gate applies the "
            "first matching grant by highest priority, then lowest grant id, "
            "whatever its effect: an allow that would decide before an "
            "overlapping deny needs --allow-widen, and a deny that an "
            "overlapping allow would decide before is refused (raise its "
            "--priority). A deny grant naming an unknown action or --match "
            "key is refused, since it would block nothing. Warns when no "
            "in-effect binding resolves any user to the subject yet."
        ),
    )
    add_json_flag(p_add)
    add_dsn_arg(p_add, help="Postgres DSN of the deployment; defaults to $OKG_DSN")
    add_deployment_arg(p_add, required=True)
    p_add.add_argument("--grant-id", required=True)
    p_add.add_argument(
        "--subject",
        required=True,
        help="the Nomos subject, as a binding names it, e.g. subject:team:analyst",
    )
    p_add.add_argument(
        "--action",
        action="append",
        required=True,
        help="an action, e.g. graph.read.mcp or mcp.tool.call, or * for all; repeat for several",
    )
    p_add.add_argument(
        "--scope",
        default=None,
        help="default: deployment:<deployment>, the scope the MCP gate reads",
    )
    p_add.add_argument("--effect", default="allow", choices=GRANT_EFFECTS)
    p_add.add_argument(
        "--classification",
        action="append",
        default=None,
        help="a classification the grant covers; repeat for several (default: *)",
    )
    p_add.add_argument(
        "--match",
        action="append",
        default=None,
        metavar="KEY=VALUE",
        help=(
            "limit the grant to resources whose KEY equals VALUE, e.g. "
            "source_id=public_docs or resource_kind=mcp_tool; repeat for several"
        ),
    )
    p_add.add_argument(
        "--priority",
        type=int,
        default=0,
        help=(
            "the first matching grant by highest priority, then lowest grant "
            "id, decides whatever its effect (default: 0)"
        ),
    )
    p_add.add_argument(
        "--allow-widen",
        action="store_true",
        help=(
            "write an allow grant even though it decides before an "
            "overlapping deny grant, admitting requests the deny blocks"
        ),
    )
    p_add.add_argument(
        "--reason",
        default=None,
        help="short note stored in the grant's history",
    )
    _add_actor_arg(p_add, noun="grant")

    p_disable = p_grant_sub.add_parser(
        "disable",
        help="Disable a grant; the row is kept as history",
        description=GRANT_DISABLE_DESCRIPTION,
    )
    add_json_flag(p_disable)
    add_dsn_arg(p_disable, help="Postgres DSN of the deployment; defaults to $OKG_DSN")
    add_deployment_arg(p_disable, required=True)
    p_disable.add_argument("--grant-id", required=True)
    p_disable.add_argument(
        "--allow-widen",
        action="store_true",
        help="disable a deny grant even though that can admit requests it blocks",
    )
    p_disable.add_argument(
        "--reason",
        default=None,
        help="short note stored in the grant's metadata",
    )
    _add_actor_arg(p_disable, noun="grant")

    p_list = p_grant_sub.add_parser(
        "list",
        help="List grants, disabled ones included",
    )
    add_json_flag(p_list)
    add_dsn_arg(p_list, help="Postgres DSN of the deployment; defaults to $OKG_DSN")
    add_deployment_arg(p_list, required=True)
    p_list.add_argument("--subject", default=None)


def _add_actor_arg(parser: argparse.ArgumentParser, *, noun: str = "binding") -> None:
    parser.add_argument(
        "--actor",
        default=None,
        help=(
            f"who made the change, recorded in the {noun}'s history (a name, "
            "never a credential); default: the OS user and the database "
            "current_user"
        ),
    )


def _binding_actor(conn: Any, explicit: str | None) -> str:
    """The name recorded as ``written_by``/``disabled_by``.

    An explicit ``--actor`` wins. Otherwise the OS login and the database role
    that made the write: both are names, not secrets, and the database role is
    what the server's own audit would show. A lookup that fails raises rather
    than recording a made-up name.
    """
    import getpass

    if explicit is not None:
        actor = explicit.strip()
        if not actor:
            raise ValueError("--actor must not be empty")
        return actor
    try:
        os_user = getpass.getuser()
    except (KeyError, OSError) as exc:
        raise ValueError(
            f"cannot tell who is running this command ({exc}); pass --actor"
        ) from exc
    with conn.cursor() as cur:
        cur.execute("SELECT current_user")
        db_user = cur.fetchone()[0]
    return f"os:{os_user} db:{db_user}"


def _still_matching_warning(
    binding: dict[str, Any], others: list[dict[str, Any]],
) -> str | None:
    if not others:
        return None
    listed = "; ".join(
        f"{row['binding_id']} ({row['claim_path']}={row['claim_value']} -> "
        f"{row['subject']}, priority {row['priority']}, {row['match']})"
        for row in others
    )
    return (
        f"WARNING: {binding['binding_id']} no longer matches, but "
        f"{len(others)} other in-effect binding(s) for provider "
        f"{binding['provider_id']!r} in scope {binding['scope']!r} may still "
        f"admit the same user: {listed}. same_claim rows match the same user "
        "and admit them whenever their token meets the row's "
        "required_assurance (only rows this command may not disable are "
        "left: ones the chat SSO group projection owns, or ones added after "
        "this binding was disabled); if_token_carries rows admit them if their token "
        "carries that role, group or entitlement. Disable those too to cut "
        f"access; `okg nomos binding list --provider-id "
        f"{binding['provider_id']}` shows every binding."
    )


def _parse_binding_time(value: str | None, *, flag: str) -> Any:
    from datetime import datetime

    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{flag} is not an ISO-8601 time: {value!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError(
            f"{flag} needs a UTC offset (for example 2026-10-01T00:00:00+00:00): "
            f"{value!r}"
        )
    return parsed


def _cmd_nomos_binding(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.nomos import SubjectBinding
    from okg.substrate.nomos.identity_store import (
        add_subject_binding,
        bindings_that_may_still_match,
        disable_subject_binding,
        list_subject_bindings,
        refuse_reserved_binding_id,
    )

    deployment_name = str(args.deployment)
    subcmd = args.nomos_binding_subcmd
    effective_from = effective_until = None
    if subcmd in {"block", "unblock"}:
        return _cmd_nomos_binding_block(args)
    if subcmd in {"add", "disable"}:
        if args.binding_id:
            refuse_reserved_binding_id(str(args.binding_id))
        if args.actor is not None and not args.actor.strip():
            raise ValueError("--actor must not be empty")
    if subcmd == "add":
        # Refuse a malformed window before touching the database.
        effective_from = _parse_binding_time(
            args.effective_from, flag="--effective-from",
        )
        effective_until = _parse_binding_time(
            args.effective_until, flag="--effective-until",
        )
        if (
            effective_from is not None
            and effective_until is not None
            and effective_until <= effective_from
        ):
            raise ValueError("--effective-until must be later than --effective-from")
        if not args.replace and (
            args.clear_effective_from or args.clear_effective_until
        ):
            raise ValueError(
                "--clear-effective-from and --clear-effective-until apply only "
                "with --replace; a new binding has no stored window to clear"
            )
    with db_connect(_admin_dsn(args)) as conn:
        if subcmd == "list":
            rows = list_subject_bindings(
                conn,
                deployment_name=deployment_name,
                provider_id=args.provider_id,
            )
            return (
                {
                    "status": "ok",
                    "deployment_name": deployment_name,
                    "binding_count": len(rows),
                    "in_effect_count": sum(1 for row in rows if row["in_effect"]),
                    "bindings": rows,
                    "revocation_limit": BINDING_REVOCATION_LIMIT,
                },
                0,
            )
        if subcmd == "disable":
            row = disable_subject_binding(
                conn,
                deployment_name=deployment_name,
                binding_id=str(args.binding_id),
                actor=_binding_actor(conn, args.actor),
                reason=args.reason,
            )
            if row is None:
                raise ValueError(
                    f"no subject binding {args.binding_id!r} in deployment "
                    f"{deployment_name!r}; `okg nomos binding list` shows them"
                )
            others = bindings_that_may_still_match(
                conn, deployment_name=deployment_name, binding=row,
            )
            _commit_admin_change(conn)
            return (
                {
                    "status": "ok",
                    "deployment_name": deployment_name,
                    "changed": row["previous_status"] != "disabled",
                    "also_disabled": row.pop("also_disabled"),
                    "warning": _still_matching_warning(row, others),
                    "still_matching": others,
                    "binding": row,
                    "revocation_limit": BINDING_REVOCATION_LIMIT,
                },
                0,
            )
        # add
        provider = _registered_provider(
            conn,
            deployment_name=deployment_name,
            provider_id=str(args.provider_id),
        )
        binding = SubjectBinding(
            binding_id=str(
                args.binding_id
                or f"binding:{args.provider_id}:{args.claim_path}:{args.claim_value}"
            ),
            provider_id=str(args.provider_id),
            issuer=str(provider["issuer"]),
            claim_path=str(args.claim_path),
            claim_value=str(args.claim_value),
            subject=str(args.subject),
            scope=str(args.scope or f"deployment:{deployment_name}"),
            priority=int(args.priority if args.priority is not None else 0),
            effective_from=(
                effective_from.isoformat() if effective_from else None
            ),
            effective_until=(
                effective_until.isoformat() if effective_until else None
            ),
        )
        row = add_subject_binding(
            conn,
            deployment_name=deployment_name,
            binding=binding,
            actor=_binding_actor(conn, args.actor),
            replace=bool(args.replace),
            reason=args.reason,
            # With --replace, a flag left out keeps the stored value; only an
            # explicit --clear-effective-* removes a window bound.
            keep_stored=frozenset(
                field
                for field, left_out in (
                    ("priority", args.priority is None),
                    (
                        "effective_from",
                        args.effective_from is None
                        and not args.clear_effective_from,
                    ),
                    (
                        "effective_until",
                        args.effective_until is None
                        and not args.clear_effective_until,
                    ),
                )
                if left_out
            ),
        )
        _commit_admin_change(conn)
        return (
            {
                "status": "ok",
                "deployment_name": deployment_name,
                "provider_status": provider["status"],
                "action": row.pop("action"),
                "binding": row,
            },
            0,
        )


def _cmd_nomos_binding_block(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.nomos.identity_store import (
        block_subject,
        subject_value_evidence,
        unblock_subject,
    )

    deployment_name = str(args.deployment)
    evidence: dict[str, Any] | None = None
    user = str(args.user).strip()
    if not user:
        raise ValueError("--user must not be empty")
    if args.actor is not None and not args.actor.strip():
        raise ValueError("--actor must not be empty")
    with db_connect(_admin_dsn(args)) as conn:
        actor = _binding_actor(conn, args.actor)
        if args.nomos_binding_subcmd == "block":
            provider = _registered_provider(
                conn, deployment_name=deployment_name,
                provider_id=str(args.provider_id),
            )
            row = block_subject(
                conn,
                deployment_name=deployment_name,
                provider_id=str(args.provider_id),
                issuer=str(provider["issuer"]),
                user=user,
                scope=str(args.scope or f"deployment:{deployment_name}"),
                actor=actor,
                reason=args.reason,
            )
        else:
            row = unblock_subject(
                conn,
                deployment_name=deployment_name,
                provider_id=str(args.provider_id),
                user=user,
                actor=actor,
                reason=args.reason,
            )
        _commit_admin_change(conn)
        check_error: str | None = None
        if args.nomos_binding_subcmd == "block":
            # After the commit, in its own read-only transaction: the check is
            # advice, so a slow or failing scan must not undo or hold the block.
            import psycopg

            try:
                evidence = subject_value_evidence(
                    conn,
                    deployment_name=deployment_name,
                    provider_id=str(args.provider_id),
                    user=user,
                )
                conn.rollback()
            except psycopg.Error as exc:
                conn.rollback()
                check_error = f"{type(exc).__name__}: {str(exc).strip()}"
    changed = row.pop("changed")
    payload: dict[str, Any] = {
        "status": "ok",
        "deployment_name": deployment_name,
        "action": args.nomos_binding_subcmd,
        "changed": changed,
        "binding": row,
        "revocation_limit": BINDING_REVOCATION_LIMIT,
    }
    if check_error is not None:
        payload["match_rule"] = BLOCK_MATCH_RULE
        payload["subject_value_check"] = {"error": check_error}
        payload["warning"] = (
            "WARNING: the block is saved, but the check for whether this "
            f"deployment has seen the exact subject value {user!r} failed "
            f"({check_error}). " + BLOCK_MATCH_RULE
        )
        print(payload["warning"], file=sys.stderr)
    if evidence is not None:
        payload["match_rule"] = BLOCK_MATCH_RULE
        payload["subject_value_check"] = evidence
        warning = None
        if not evidence["exact_match_seen"]:
            warning = (
                f"WARNING: no binding or recorded sign-in in "
                f"deployment {deployment_name!r} has the exact subject value "
                f"{user!r} for provider {args.provider_id!r}, so this block may "
                "match nobody. " + BLOCK_MATCH_RULE
            )
            if evidence["case_variants_seen"]:
                warning += (
                    " Values seen that differ only in case: "
                    + ", ".join(repr(v) for v in evidence["case_variants_seen"])
                    + "."
                )
            print(warning, file=sys.stderr)
        payload["warning"] = warning
    return payload, 0


BLOCK_MATCH_RULE = (
    "A block matches the token's subject-claim value exactly: only "
    "surrounding whitespace is trimmed, and case matters (`Alice` does not "
    "block `alice`)."
)


def _repeated(values: list[str] | None, *, default: tuple[str, ...] = ()) -> tuple[str, ...]:
    if values is None:
        return default
    cleaned = tuple(value.strip() for value in values)
    if any(not value for value in cleaned):
        raise ValueError("a repeated flag was given an empty value")
    return tuple(dict.fromkeys(cleaned))


def _declared_oidc_auth(deployment_name: str) -> tuple[dict[str, Any], Any]:
    """The manifest's ``nomos.oidc_auth`` block, resolved as the server does.

    Returns ``(check, config)``: ``check`` says what was found and is echoed
    in the command output, so "the manifest could not be read" is never
    reported as "no OIDC block".
    """
    from okg.substrate.deployments import (
        DeploymentResolutionError,
        resolve_deployment,
    )
    from okg.substrate.errors import SubstrateError
    from okg.substrate.mcp.oidc_auth import oidc_auth_from_manifest

    try:
        manifest = resolve_deployment(deployment_name)
    except DeploymentResolutionError as exc:
        return {"status": "manifest_unavailable", "detail": str(exc)}, None
    try:
        config = oidc_auth_from_manifest(manifest)
    except SubstrateError as exc:
        return {"status": "manifest_invalid", "detail": str(exc)}, None
    if config is None:
        return {"status": "not_declared"}, None
    return {"status": "declared", "provider_id": config.provider_id}, config


def _provider_from_args(args: argparse.Namespace, config: Any) -> Any:
    """Build the provider row from flags, and from the OIDC block if asked."""
    from okg.substrate.mcp.oidc_auth import OIDC_VERIFIER_ID
    from okg.substrate.nomos import IdentityProvider
    from okg.substrate.nomos.identity_store import (
        IdentityProviderOidcAuthMismatchError,
    )

    audiences = _repeated(args.audience)
    trusted = _repeated(args.trusted_verifier)
    provider_id = args.provider_id
    issuer = args.issuer
    subject_claim = args.subject_claim
    provider_type = str(args.provider_type)
    if config is not None:
        # --for-oidc-auth: the manifest supplies what the start-time check
        # compares. A flag that disagrees with it is refused, not overridden.
        declared = {
            "--provider-id": (provider_id, config.provider_id),
            "--issuer": (issuer, config.issuer),
            "--subject-claim": (subject_claim, config.subject_claim),
        }
        clashes = [
            f"{flag} {given!r} (the manifest says {wanted!r})"
            for flag, (given, wanted) in declared.items()
            if given is not None and given != wanted
        ]
        if audiences and config.audience not in audiences:
            clashes.append(
                f"--audience {list(audiences)!r} (the manifest says "
                f"{config.audience!r})"
            )
        if provider_type not in {"oidc", "touchstone_oidc"}:
            clashes.append(f"--provider-type {provider_type!r} (OIDC needs oidc)")
        if clashes:
            raise IdentityProviderOidcAuthMismatchError(
                "--for-oidc-auth takes these from nomos.oidc_auth, and the "
                "flags disagree: " + "; ".join(clashes)
            )
        provider_id = config.provider_id
        issuer = config.issuer
        subject_claim = config.subject_claim
        audiences = audiences or (config.audience,)
        trusted = tuple(dict.fromkeys((*trusted, OIDC_VERIFIER_ID)))
    missing = [
        flag
        for flag, value in (
            ("--provider-id", provider_id),
            ("--issuer", issuer),
            ("--audience", audiences),
        )
        if not value
    ]
    if missing:
        raise ValueError(
            "provider add needs " + ", ".join(missing)
            + " (or --for-oidc-auth to read them from the manifest)"
        )
    metadata: dict[str, Any] = {}
    if trusted:
        metadata["trusted_verifiers"] = list(trusted)
    if args.require_verified_by:
        metadata["require_verified_by"] = True
    if args.production:
        metadata["production"] = True
    return IdentityProvider(
        provider_id=str(provider_id),
        provider_type=provider_type,
        issuer=str(issuer),
        audiences=audiences,
        subject_claim=str(subject_claim or "sub"),
        group_claims=_repeated(args.group_claim, default=("groups",)),
        role_claims=_repeated(args.role_claim, default=("roles",)),
        entitlement_claims=_repeated(
            args.entitlement_claim, default=("entitlements",),
        ),
        key_reference=args.key_reference,
        required_assurance=_repeated(args.required_assurance),
        mfa_required=bool(args.mfa_required),
        group_claims_approved=bool(args.group_claims_approved),
        metadata_fingerprint=args.metadata_fingerprint,
        realm=args.realm,
        service_principal=args.service_principal,
        freshness_seconds=args.freshness_seconds,
        status="active",
        metadata=metadata,
    )


def _admin_dsn(args: argparse.Namespace) -> str:
    dsn = args.dsn or os.environ.get("OKG_DSN")
    if not dsn:
        raise ValueError("pass --dsn or set $OKG_DSN to the deployment's database")
    return str(dsn)


def _check_provider_against_oidc_auth(
    provider: Any, *, deployment_name: str, check: dict[str, Any], config: Any,
) -> dict[str, Any]:
    """Refuse a provider row the MCP server's OIDC start check would refuse."""
    from okg.substrate.errors import SubstrateError
    from okg.substrate.mcp.oidc_auth import validate_oidc_auth_against_providers
    from okg.substrate.nomos.identity_store import (
        IdentityProviderOidcAuthMismatchError,
    )

    if config is None:
        return check
    if config.provider_id != provider.provider_id:
        return {**check, "status": "other_provider"}
    try:
        validate_oidc_auth_against_providers(
            config, [provider], deployment_name=deployment_name,
        )
    except SubstrateError as exc:
        hint = f" ({exc.fix_hint})" if getattr(exc, "fix_hint", None) else ""
        raise IdentityProviderOidcAuthMismatchError(
            f"the deployment manifest's nomos.oidc_auth names provider "
            f"{provider.provider_id!r}, and the MCP server would refuse to "
            f"start with this row: {exc}{hint}. Fix the flags, or pass "
            "--for-oidc-auth to take them from the manifest"
        ) from exc
    return {**check, "status": "passed"}


def _cmd_nomos_provider_admin(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.nomos.identity_store import (
        IdentityProviderOidcAuthUndeclaredError,
        add_identity_provider,
        disable_identity_provider,
        list_identity_providers,
        providers_sharing_issuer,
    )

    deployment_name = str(args.deployment)
    subcmd = args.nomos_provider_subcmd
    provider = None
    oidc_check: dict[str, Any] = {}
    if subcmd in {"add", "disable"} and args.actor is not None and not args.actor.strip():
        raise ValueError("--actor must not be empty")
    if subcmd == "add":
        # Everything that can refuse without the database refuses first.
        oidc_check, config = _declared_oidc_auth(deployment_name)
        if args.for_oidc_auth and config is None:
            raise IdentityProviderOidcAuthUndeclaredError(
                "--for-oidc-auth reads the nomos.oidc_auth block of deployment "
                f"{deployment_name!r}, and there is none to read "
                f"({oidc_check['status']}"
                + (f": {oidc_check['detail']}" if oidc_check.get("detail") else "")
                + ")"
            )
        provider = _provider_from_args(
            args, config if args.for_oidc_auth else None,
        )
        oidc_check = _check_provider_against_oidc_auth(
            provider, deployment_name=deployment_name, check=oidc_check,
            config=config,
        )
        warnings: list[str] = []
        if oidc_check["status"] in {"manifest_unavailable", "manifest_invalid"}:
            warnings.append(
                f"WARNING: the manifest of deployment {deployment_name!r} could "
                f"not be read ({oidc_check['status']}: "
                f"{oidc_check.get('detail')}), so this row was not checked "
                "against a nomos.oidc_auth block"
            )
        metadata = dict(provider.metadata)
        if not metadata.get("trusted_verifiers") and not metadata.get(
            "require_verified_by"
        ):
            warnings.append(
                "WARNING: the provider has neither --trusted-verifier nor "
                "--require-verified-by, so a claim naming no verifier is "
                "accepted as valid on paths that take claims from their "
                "caller; the MCP multi-user and OIDC modes refuse to start "
                "with such a provider"
            )
        for warning in warnings:
            print(warning, file=sys.stderr)
    with db_connect(_admin_dsn(args)) as conn:
        if subcmd == "list":
            rows = list_identity_providers(
                conn, deployment_name=deployment_name, provider_id=args.provider_id,
            )
            return (
                {
                    "status": "ok",
                    "deployment_name": deployment_name,
                    "provider_count": len(rows),
                    "in_effect_count": sum(1 for row in rows if row["in_effect"]),
                    "providers": rows,
                },
                0,
            )
        if subcmd == "disable":
            row = disable_identity_provider(
                conn,
                deployment_name=deployment_name,
                provider_id=str(args.provider_id),
                actor=_binding_actor(conn, args.actor),
                reason=args.reason,
            )
            others = providers_sharing_issuer(
                conn, deployment_name=deployment_name, provider=row,
            )
            _commit_admin_change(conn)
            changed = row.pop("changed")
            warning = None
            if others:
                warning = (
                    f"WARNING: {row['provider_id']} is disabled, but "
                    f"{len(others)} other in-effect provider(s) share its issuer "
                    f"{row['issuer']!r}: "
                    + ", ".join(other["provider_id"] for other in others)
                    + ". A server configured with one of them still admits the "
                    "same people; disable those too to cut them off."
                )
            return (
                {
                    "status": "ok",
                    "deployment_name": deployment_name,
                    "changed": changed,
                    "warning": warning,
                    "same_issuer_providers": others,
                    "provider": row,
                },
                0,
            )
        # add
        row = add_identity_provider(
            conn,
            deployment_name=deployment_name,
            provider=provider,
            actor=_binding_actor(conn, args.actor),
            reason=args.reason,
        )
        _commit_admin_change(conn)
        return (
            {
                "status": "ok",
                "deployment_name": deployment_name,
                "action": "added",
                "oidc_auth_check": oidc_check,
                "warnings": warnings,
                "provider": row,
            },
            0,
        )


def _parse_resource_match(pairs: list[str]) -> dict[str, str]:
    resource_match: dict[str, str] = {}
    for pair in pairs:
        key, sep, value = pair.partition("=")
        key = key.strip()
        if not sep or not key or not value:
            raise ValueError(f"--match needs KEY=VALUE with both parts: {pair!r}")
        if key in resource_match:
            raise ValueError(f"--match names {key!r} twice")
        resource_match[key] = value
    return resource_match


def _cmd_nomos_grant(args: argparse.Namespace) -> tuple[dict[str, Any], int]:
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.nomos import PermissionGrant
    from okg.substrate.nomos.identity_store import (
        PermissionGrantUnknownMatchError,
        add_permission_grant,
        disable_permission_grant,
        grant_match_problems,
        grants_that_may_still_allow,
        list_permission_grants,
        subject_has_binding,
    )

    deployment_name = str(args.deployment)
    subcmd = args.nomos_grant_subcmd
    grant = None
    if subcmd in {"add", "disable"} and args.actor is not None and not args.actor.strip():
        raise ValueError("--actor must not be empty")
    if subcmd == "add":
        subject = str(args.subject).strip()
        if not subject or not str(args.grant_id).strip():
            raise ValueError("--grant-id and --subject must not be empty")
        grant = PermissionGrant(
            grant_id=str(args.grant_id),
            subject=subject,
            actions=_repeated(args.action),
            scope=str(args.scope or f"deployment:{deployment_name}"),
            effect=str(args.effect),
            resource_match=_parse_resource_match(args.match or []),
            classifications=_repeated(args.classification, default=("*",)),
            priority=int(args.priority),
        )
        # A deny that matches nothing blocks nothing: refuse it before
        # connecting. An allow that matches nothing grants nothing, so it is
        # written with a warning.
        match_problems = grant_match_problems(grant)
        if match_problems and grant.effect != "allow":
            raise PermissionGrantUnknownMatchError(
                f"deny grant {grant.grant_id!r} would never match: "
                + "; ".join(match_problems)
                + ". A deny that matches nothing blocks nothing"
            )
    with db_connect(_admin_dsn(args)) as conn:
        if subcmd == "list":
            rows = list_permission_grants(
                conn, deployment_name=deployment_name, subject=args.subject,
            )
            return (
                {
                    "status": "ok",
                    "deployment_name": deployment_name,
                    "grant_count": len(rows),
                    "active_count": sum(1 for row in rows if row["status"] == "active"),
                    "grants": rows,
                },
                0,
            )
        if subcmd == "disable":
            row = disable_permission_grant(
                conn,
                deployment_name=deployment_name,
                grant_id=str(args.grant_id),
                actor=_binding_actor(conn, args.actor),
                reason=args.reason,
                allow_widen=bool(args.allow_widen),
            )
            others = (
                grants_that_may_still_allow(
                    conn, deployment_name=deployment_name, grant=row,
                )
                if row["effect"] == "allow"
                else []
            )
            _commit_admin_change(conn)
            changed = row.pop("changed")
            warning = None
            if others:
                warning = (
                    f"WARNING: {row['grant_id']} is disabled, but {len(others)} "
                    f"other active allow grant(s) for {row['subject']!r} in "
                    f"scope {row['scope']!r} may still permit the same "
                    "requests: "
                    + "; ".join(
                        f"{other['grant_id']} ({other['match']}, priority "
                        f"{other['priority']})"
                        for other in others
                    )
                    + ". covers rows permit every request this grant did. "
                    "Disable those too to cut the access."
                )
            elif row["effect"] != "allow" and changed:
                warning = (
                    f"WARNING: {row['grant_id']} was a {row['effect']} grant; "
                    "requests it blocked may now be admitted by a "
                    "lower-priority allow grant."
                )
            return (
                {
                    "status": "ok",
                    "deployment_name": deployment_name,
                    "changed": changed,
                    "warning": warning,
                    "still_allowing": others,
                    "grant": row,
                },
                0,
            )
        # add
        row = add_permission_grant(
            conn,
            deployment_name=deployment_name,
            grant=grant,
            actor=_binding_actor(conn, args.actor),
            reason=args.reason,
            allow_widen=bool(args.allow_widen),
        )
        bound = subject_has_binding(
            conn, deployment_name=deployment_name, subject=grant.subject,
            scope=grant.scope,
        )
        _commit_admin_change(conn)
        return (
            {
                "status": "ok",
                "deployment_name": deployment_name,
                "action": "added",
                "subject_bound": bound,
                "warning": None if bound else (
                    f"WARNING: no in-effect binding resolves any user to "
                    f"{grant.subject!r} in scope {grant.scope!r}, so this grant "
                    "applies to nobody yet; add one with `okg nomos binding add`."
                ),
                "match_warnings": [
                    f"WARNING: {problem}; this allow grant will not match it"
                    for problem in match_problems
                ],
                "grant": row,
            },
            0,
        )


class AdminCommitOutcomeUnknown(Exception):
    """COMMIT of a provider, binding or grant change failed; it may have applied."""

    def __init__(self, cause: Exception) -> None:
        super().__init__(str(cause))
        self.cause = cause


def _commit_admin_change(conn: Any) -> None:
    """Commit, telling a failed COMMIT apart from a failure before it.

    An error before COMMIT rolls the transaction back, so nothing changed. An
    error DURING commit (say the connection drops) leaves the outcome unknown:
    the server may have committed before the client heard back. Saying
    "nothing changed" then could tell an operator a revocation failed when it
    took effect, or the reverse.
    """
    import psycopg

    try:
        conn.commit()
    except psycopg.Error as exc:
        raise AdminCommitOutcomeUnknown(exc) from exc


def _registered_provider(
    conn: Any,
    *,
    deployment_name: str,
    provider_id: str,
) -> dict[str, Any]:
    from psycopg.rows import dict_row

    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT issuer, status
              FROM okg.nomos_identity_providers
             WHERE deployment_name = %s AND provider_id = %s
            """,
            (deployment_name, provider_id),
        )
        row = cur.fetchone()
    if row is None:
        raise ValueError(
            f"unknown identity provider {provider_id!r} for deployment "
            f"{deployment_name!r}; register the provider before binding its "
            "users with `okg nomos provider add` (`okg nomos provider list` "
            "shows the registered ones)"
        )
    return dict(row)


# The verbs that write or list identity rows in the deployment's database.
# They share one error contract.
_ADMIN_VERBS = frozenset({"add", "disable", "list"})


def _admin_command(args: argparse.Namespace) -> tuple[str, Any, str] | None:
    if args.nomos_subcmd == "binding":
        return "binding", _cmd_nomos_binding, args.nomos_binding_subcmd
    if args.nomos_subcmd == "grant":
        return "grant", _cmd_nomos_grant, args.nomos_grant_subcmd
    if (
        args.nomos_subcmd == "provider"
        and args.nomos_provider_subcmd in _ADMIN_VERBS
    ):
        return "provider", _cmd_nomos_provider_admin, args.nomos_provider_subcmd
    return None


def _run_admin_command(
    noun: str, handler: Any, verb: str, args: argparse.Namespace, *, as_json: bool,
) -> int:
    import psycopg

    try:
        payload, code = handler(args)
    except AdminCommitOutcomeUnknown as exc:
        cause = exc.cause
        emit_result(
            {
                "status": "error",
                "error_code": f"nomos.{noun}.commit_outcome_unknown",
                "sqlstate": getattr(cause, "sqlstate", None),
                "error": (
                    f"the database failed while committing `okg nomos "
                    f"{noun} {verb}`, so the change may or may not have been "
                    f"saved; run `okg nomos {noun} list` to see the {noun}'s "
                    f"status: {type(cause).__name__}: {str(cause).strip()}"
                ),
            },
            as_json=as_json,
        )
        return 1
    except psycopg.Error as exc:
        # These commands change who can sign in and what they may do; a
        # database error (a deadlock, a lock timeout, a lost connection)
        # means the transaction was rolled back and nothing changed. Report
        # it as the command's error, not a traceback. Other nomos
        # subcommands keep their own handling.
        emit_result(
            {
                "status": "error",
                "error_code": f"nomos.{noun}.database_error",
                "sqlstate": getattr(exc, "sqlstate", None),
                "error": (
                    f"database error during `okg nomos {noun} {verb}`; "
                    f"nothing was changed: {type(exc).__name__}: "
                    f"{str(exc).strip()}"
                ),
            },
            as_json=as_json,
        )
        return 1
    emit_result(payload, as_json=as_json)
    return code


def cmd_nomos(args: argparse.Namespace, *, as_json: bool) -> int:
    try:
        admin = _admin_command(args)
        if admin is not None:
            noun, handler, verb = admin
            return _run_admin_command(noun, handler, verb, args, as_json=as_json)
        if args.nomos_subcmd == "demo":
            payload = run_demo(Path(args.out), overwrite=bool(args.overwrite))
            emit_result(payload, as_json=as_json)
            return 0
        if args.nomos_subcmd == "compile":
            bundle = compile_policy_graph(load_policy_graph(Path(args.policy)))
            payload = bundle.to_dict()
            if args.out:
                write_json(Path(args.out), payload)
                payload = {
                    "status": "ok",
                    "compiled_bundle": str(Path(args.out).expanduser().resolve()),
                    "policy_fingerprint": bundle.policy_fingerprint,
                }
            emit_result(payload, as_json=as_json)
            return 0
        if args.nomos_subcmd == "evaluate":
            bundle = load_bundle(Path(args.bundle))
            decision_input = load_decision_input(Path(args.input))
            decision = evaluate(
                bundle,
                decision_input,
                enforcement_point=args.enforcement_point,
            )
            payload = decision.to_dict()
            if args.out:
                write_json(Path(args.out), payload)
                payload = {
                    "status": "ok",
                    "decision": str(Path(args.out).expanduser().resolve()),
                    "decision_id": decision.decision_id,
                    "effect": decision.effect,
                    "reason_code": decision.reason_code,
                }
            emit_result(payload, as_json=as_json)
            return 0
        if args.nomos_subcmd == "inspect":
            if args.dsn:
                payload = _inspect_active_postgres(
                    dsn=str(args.dsn),
                    deployment_name=str(args.deployment or "default"),
                    scope=str(
                        args.scope
                        or f"deployment:{args.deployment or 'default'}"
                    ),
                )
            else:
                if not args.bundle:
                    raise ValueError(
                        "nomos inspect requires --bundle or --dsn"
                    )
                bundle = load_bundle(Path(args.bundle))
                payload = {
                    "policy_bundle_id": bundle.policy_bundle_id,
                    "policy_version": bundle.policy_version,
                    "policy_fingerprint": bundle.policy_fingerprint,
                    "default_effect": bundle.default_effect,
                    "rule_count": len(bundle.rules),
                    "effects": sorted({rule.effect for rule in bundle.rules}),
                    "source_generation_id": bundle.source_generation_id,
                }
            emit_result(payload, as_json=as_json)
            return 0
        if args.nomos_subcmd == "decision":
            decision = _find_decision(Path(args.decisions), args.decision_id)
            emit_result(decision, as_json=as_json)
            return 0
        if args.nomos_subcmd == "decisions-prune":
            from okg.substrate.db.connection import connect as db_connect

            from okg.substrate.nomos.retention import prune_nomos_decisions

            dsn = args.dsn or os.environ.get("OKG_DSN")
            if not dsn:
                raise ValueError(
                    "nomos decisions-prune requires --dsn or $OKG_DSN"
                )
            deployment_name = str(args.deployment or "default")
            # Dry-run is the default: --apply is required to DELETE (#694).
            dry_run = not bool(getattr(args, "apply", False))
            with db_connect(str(dsn)) as conn:
                deleted = prune_nomos_decisions(
                    conn,
                    deployment_name=deployment_name,
                    max_age_days=args.days,
                    dry_run=dry_run,
                )
                conn.commit()
            emit_result(
                {
                    "status": "ok",
                    "deployment_name": deployment_name,
                    "deleted_rows": deleted,
                    "dry_run": dry_run,
                    "applied": not dry_run,
                },
                as_json=as_json,
            )
            return 0
        if args.nomos_subcmd == "trace":
            decisions = _load_decisions(Path(args.decisions))
            bundle_id = None
            if args.bundle:
                bundle_id = load_bundle(Path(args.bundle)).policy_fingerprint
            state = build_refinement_state(
                decisions,
                compiled_bundles=[bundle_id] if bundle_id else [],
                active_bundle=bundle_id,
                public_outputs=[
                    str(row["decision_id"])
                    for row in decisions
                    if row.get("action") in {"graph.export"}
                    and row.get("effect") in {"allow", "redact", "declassify"}
                ],
                quarantine_state=[
                    str(row["decision_id"])
                    for row in decisions
                    if row.get("effect") in {"review", "quarantine"}
                ],
                visible_state=[
                    str(row["decision_id"])
                    for row in decisions
                    if row.get("action") in {"source.emit", "poros.graph.submit"}
                    and row.get("effect") == "allow"
                ],
                generation_policy_pins=(
                    {str(args.generation_id): bundle_id}
                    if args.generation_id and bundle_id
                    else {}
                ),
            )
            check_noninterference_trace(state)
            write_json(Path(args.out), state)
            emit_result(
                {
                    "status": "ok",
                    "refinement_state": str(Path(args.out).expanduser().resolve()),
                    "decision_count": len(decisions),
                },
                as_json=as_json,
            )
            return 0
        if args.nomos_subcmd == "provider":
            if args.nomos_provider_subcmd == "fixture":
                provider = institutional_provider_fixture(
                    str(args.profile),
                    audience=str(args.audience),
                    provider_id=args.provider_id,
                    issuer=args.issuer,
                    key_reference=args.key_reference,
                    metadata_fingerprint=args.metadata_fingerprint,
                    service_principal=args.service_principal,
                )
                payload = provider.to_dict()
                if args.out:
                    write_json(Path(args.out), payload)
                    payload = {
                        "status": "ok",
                        "provider": str(Path(args.out).expanduser().resolve()),
                        "provider_id": provider.provider_id,
                        "provider_type": provider.provider_type,
                    }
                emit_result(payload, as_json=as_json)
                return 0
            if args.nomos_provider_subcmd == "validate":
                provider = _load_identity_provider(Path(args.provider))
                emit_result(
                    {
                        "status": "ok",
                        "provider_id": provider.provider_id,
                        "provider_type": provider.provider_type,
                        "provider_status": provider.status_value,
                        "issuer": provider.issuer,
                        "audiences": list(provider.audiences),
                        "required_assurance": list(provider.required_assurance),
                    },
                    as_json=as_json,
                )
                return 0
            if args.nomos_provider_subcmd == "inspect":
                payload = _inspect_identity_providers(
                    dsn=str(args.dsn),
                    deployment_name=str(args.deployment),
                    provider_id=args.provider_id,
                )
                emit_result(payload, as_json=as_json)
                return 0
            if args.nomos_provider_subcmd == "drift":
                payload = _identity_provider_drift(
                    dsn=str(args.dsn),
                    deployment_name=str(args.deployment),
                )
                emit_result(payload, as_json=as_json)
                return 0 if payload["ok"] else 1
            if args.nomos_provider_subcmd == "readiness":
                payload = _identity_provider_readiness(
                    dsn=str(args.dsn),
                    deployment_name=str(args.deployment),
                    production=bool(args.production),
                )
                emit_result(payload, as_json=as_json)
                return 0 if payload["ok"] else 1
            if args.nomos_provider_subcmd == "oidc-login":
                result = run_oidc_loopback_login(
                    OIDCLoginConfig(
                        issuer=str(args.issuer),
                        client_id=str(args.client_id),
                        provider_id=str(args.provider_id),
                        provider_type=str(args.provider_type),
                        verified_by=str(args.verified_by),
                        scopes=tuple(str(args.scopes).split()),
                        client_secret_env=args.client_secret_env,
                        client_auth_method=str(args.client_auth_method),
                        redirect_port=int(args.redirect_port),
                        redirect_path=str(args.redirect_path),
                        timeout_seconds=int(args.timeout_seconds),
                        fetch_userinfo=bool(args.fetch_userinfo),
                        open_browser=not bool(args.no_browser),
                        include_claims=tuple(args.include_claim or ()),
                        leeway_seconds=int(args.leeway_seconds),
                    ),
                    status_callback=lambda message: print(message, file=sys.stderr),
                )
                claim_path = Path(args.out).expanduser().resolve()
                write_json(claim_path, result.claim)
                emit_result(
                    result.summary(claim_path=str(claim_path)),
                    as_json=as_json,
                )
                return 0
    except (OSError, NomosPolicyError, NomosTraceError, ValueError) as exc:
        payload = {"status": "error", "error": str(exc)}
        from okg.substrate.nomos.identity_store import (
            IdentityProviderError,
            PermissionGrantError,
            SubjectBindingError,
        )

        if isinstance(
            exc, (SubjectBindingError, IdentityProviderError, PermissionGrantError),
        ):
            payload["error_code"] = exc.code
        emit_result(payload, as_json=as_json)
        return 1

    print(f"unknown nomos subcommand: {args.nomos_subcmd}", file=sys.stderr)
    return 2


def _load_decisions(path: Path) -> list[dict[str, Any]]:
    value = json.loads(path.expanduser().read_text(encoding="utf-8"))
    if isinstance(value, dict):
        return [value]
    if not isinstance(value, list) or not all(isinstance(row, dict) for row in value):
        raise ValueError(
            "decisions file must contain a decision object or a list of objects"
        )
    return value


def _find_decision(path: Path, decision_id: str) -> dict[str, Any]:
    for decision in _load_decisions(path):
        if decision.get("decision_id") == decision_id:
            return decision
    raise ValueError(f"decision id not found: {decision_id}")


def _load_identity_provider(path: Path):
    from okg.substrate.nomos import IdentityProvider

    payload = json.loads(path.expanduser().read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("provider file must contain a JSON object")
    return IdentityProvider(
        provider_id=str(payload["provider_id"]),
        provider_type=str(payload["provider_type"]),
        issuer=str(payload["issuer"]),
        audiences=tuple(payload.get("audiences") or ()),
        subject_claim=str(payload.get("subject_claim") or "sub"),
        group_claims=tuple(payload.get("group_claims") or ()),
        role_claims=tuple(payload.get("role_claims") or ()),
        entitlement_claims=tuple(payload.get("entitlement_claims") or ()),
        assurance_claim=payload.get("assurance_claim"),
        key_reference=payload.get("key_reference"),
        secret_references=tuple(payload.get("secret_references") or ()),
        claim_allowlist=tuple(payload.get("claim_allowlist") or ()),
        required_assurance=tuple(payload.get("required_assurance") or ()),
        mfa_required=bool(payload.get("mfa_required")),
        group_claims_approved=bool(payload.get("group_claims_approved")),
        metadata_fingerprint=payload.get("metadata_fingerprint"),
        realm=payload.get("realm"),
        service_principal=payload.get("service_principal"),
        freshness_seconds=payload.get("freshness_seconds"),
        status=payload.get("status"),
        effective_from=payload.get("effective_from"),
        effective_until=payload.get("effective_until"),
        metadata=payload.get("metadata") or {},
    )


def _inspect_identity_providers(
    *,
    dsn: str,
    deployment_name: str,
    provider_id: str | None = None,
) -> dict[str, Any]:
    from okg.substrate.db.connection import connect as db_connect
    from psycopg.rows import dict_row

    where = "WHERE deployment_name = %(deployment_name)s"
    params: dict[str, Any] = {"deployment_name": deployment_name}
    if provider_id:
        where += " AND provider_id = %(provider_id)s"
        params["provider_id"] = provider_id
    with db_connect(dsn) as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            f"""
            SELECT provider_id, provider_type, issuer, audiences, status,
                   required_assurance, mfa_required,
                   group_claims_approved, metadata_fingerprint, realm,
                   service_principal, key_reference, secret_references,
                   freshness_seconds
              FROM okg.nomos_identity_providers
              {where}
             ORDER BY provider_id
            """,
            params,
        )
        providers = [
            {
                "provider_id": row["provider_id"],
                "provider_type": row["provider_type"],
                "issuer": row["issuer"],
                "audiences": list(row["audiences"] or []),
                "status": row["status"],
                "required_assurance": list(row["required_assurance"] or []),
                "mfa_required": row["mfa_required"],
                "group_claims_approved": row["group_claims_approved"],
                "metadata_fingerprint": row["metadata_fingerprint"],
                "realm": row["realm"],
                "service_principal": row["service_principal"],
                "key_reference": row["key_reference"],
                "secret_references": list(row["secret_references"] or []),
                "freshness_seconds": row["freshness_seconds"],
            }
            for row in cur.fetchall()
        ]
    return {
        "status": "ok",
        "deployment_name": deployment_name,
        "provider_count": len(providers),
        "providers": providers,
    }


def _identity_provider_drift(
    *,
    dsn: str,
    deployment_name: str,
) -> dict[str, Any]:
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.nomos.doctor import nomos_doctor_report

    with db_connect(dsn) as conn:
        report = nomos_doctor_report(conn, deployment_name=deployment_name)
    provider_errors = [
        error
        for error in report["errors"]
        if str(error.get("code", "")).startswith("nomos_identity_provider")
    ]
    return {
        "ok": not provider_errors,
        "status": "ok" if not provider_errors else "error",
        "deployment_name": deployment_name,
        "provider_error_count": len(provider_errors),
        "provider_errors": provider_errors,
        "identity": report["identity"],
    }


def _identity_provider_readiness(
    *,
    dsn: str,
    deployment_name: str,
    production: bool = False,
) -> dict[str, Any]:
    from okg.substrate.db.connection import connect as db_connect

    from okg.substrate.nomos.doctor import nomos_doctor_report

    with db_connect(dsn) as conn:
        report = nomos_doctor_report(conn, deployment_name=deployment_name)
        production_errors = (
            _production_readiness_errors(
                conn,
                deployment_name=deployment_name,
                identity=dict(report["identity"]),
                policy=dict(report["policy"]),
            )
            if production
            else []
        )
    identity = dict(report["identity"])
    policy = dict(report["policy"])
    errors = [*report["errors"], *production_errors]
    ok = not errors
    return {
        "ok": ok,
        "status": "OK" if ok and not report["warnings"] else (
            "ERROR" if errors else "WARN"
        ),
        "deployment_name": deployment_name,
        "production": production,
        "active_provider_count": identity["active_provider_count"],
        "binding_count": identity["binding_count"],
        "grant_count": identity["grant_count"],
        "recent_claim_count": identity["recent_claim_count"],
        "recent_decision_count": identity["recent_decision_count"],
        "orphan_binding_count": identity["orphan_binding_count"],
        "unbound_grant_subject_count": identity["unbound_grant_subject_count"],
        "active_policy_count": policy["active_policy_count"],
        "active_config_count": policy["active_config_count"],
        "provider_errors": identity["provider_errors"],
        "production_errors": production_errors,
        "errors": errors,
        "warnings": report["warnings"],
    }


def _production_readiness_errors(
    conn,
    *,
    deployment_name: str,
    identity: dict[str, Any],
    policy: dict[str, Any],
) -> list[dict[str, Any]]:
    from psycopg.rows import dict_row

    errors: list[dict[str, Any]] = []
    if identity["active_provider_count"] == 0:
        errors.append({"code": "nomos_production_no_active_identity_provider"})
    if identity["binding_count"] == 0:
        errors.append({"code": "nomos_production_no_subject_binding"})
    if identity["grant_count"] == 0:
        errors.append({"code": "nomos_production_no_permission_grant"})
    if identity["recent_claim_count"] == 0:
        errors.append({"code": "nomos_production_no_recent_claim"})
    if identity["recent_decision_count"] == 0:
        errors.append({"code": "nomos_production_no_recent_decision"})
    if policy["active_policy_count"] == 0:
        errors.append({"code": "nomos_production_no_active_policy"})
    if policy["active_config_count"] == 0:
        errors.append({"code": "nomos_production_no_active_config"})
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(
            """
            SELECT provider_id, metadata
              FROM okg.nomos_identity_providers
             WHERE deployment_name = %s
               AND status = 'active'
             ORDER BY provider_id
            """,
            (deployment_name,),
        )
        for row in cur.fetchall():
            metadata = dict(row["metadata"] or {})
            if not _metadata_bool(metadata.get("production")):
                errors.append({
                    "code": "nomos_identity_provider_not_production",
                    "provider_id": row["provider_id"],
                })
    return errors


def _metadata_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _inspect_active_postgres(
    *,
    dsn: str,
    deployment_name: str,
    scope: str,
) -> dict[str, Any]:
    from okg.substrate.db.connection import connect as db_connect
    from psycopg.rows import dict_row

    from okg.substrate.nomos import active_nomos_summary

    with db_connect(dsn) as conn:
        payload = active_nomos_summary(
            conn,
            deployment_name=deployment_name,
            scope=scope,
        )
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute(
                """
                SELECT provider_id, provider_type, issuer, audiences,
                       key_reference, metadata
                  FROM okg.nomos_identity_providers
                 WHERE deployment_name = %s
                   AND status = 'active'
                 ORDER BY provider_id
                """,
                (deployment_name,),
            )
            payload["active_identity_providers"] = [
                {
                    "provider_id": row["provider_id"],
                    "provider_type": row["provider_type"],
                    "issuer": row["issuer"],
                    "audiences": list(row["audiences"] or []),
                    "key_reference": row["key_reference"],
                    "drift_status": dict(row["metadata"] or {}).get(
                        "drift_status",
                        "unknown",
                    ),
                }
                for row in cur.fetchall()
            ]
            cur.execute(
                """
                SELECT claim_fingerprint, provider_id, observed_at
                  FROM okg.nomos_external_claims
                 WHERE deployment_name = %s
                 ORDER BY observed_at DESC
                 LIMIT 10
                """,
                (deployment_name,),
            )
            payload["recent_claim_fingerprints"] = [
                {
                    "claim_fingerprint": row["claim_fingerprint"],
                    "provider_id": row["provider_id"],
                    "observed_at": row["observed_at"].isoformat(),
                }
                for row in cur.fetchall()
            ]
            cur.execute(
                """
                SELECT decision_id, effect, reason_code, created_at
                  FROM okg.nomos_decisions
                 WHERE deployment_name = %s
                 ORDER BY created_at DESC, decision_row_id DESC
                 LIMIT 10
                """,
                (deployment_name,),
            )
            payload["recent_decisions"] = [
                {
                    "decision_id": row["decision_id"],
                    "effect": row["effect"],
                    "reason_code": row["reason_code"],
                    "created_at": row["created_at"].isoformat(),
                }
                for row in cur.fetchall()
            ]
    return payload


