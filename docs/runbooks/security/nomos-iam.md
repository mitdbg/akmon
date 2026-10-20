---
audience: operator
type: how-to
canonical_id: runbook-nomos-institutional-iam
---
# Nomos Institutional IAM Runbook


Nomos does not run an institution's single sign-on service, Okta, Duo, or
a Kerberos KDC. Nomos consumes a verified claim envelope from an institutional
gateway or local verifier, checks it against a provider profile, maps it
through scoped subject bindings, and records a durable policy decision.

For policy compilation, decision evaluation, active policy/config state,
read/export enforcement, and the formal gate, see [Nomos operations](nomos.md).

## Safety Boundary

- Keep cryptographic verification in the gateway/verifier adapter.
- Store issuer/entity ids, audiences/client ids, metadata fingerprints,
  service principals, freshness policy, and secret references in Nomos.
- Do not store raw JWTs, SAML assertions, cookies, Kerberos passwords,
  Kerberos tickets, keytab bytes, refresh tokens, or private keys.
- Treat the built-in provider profiles as fixtures. Production deployments
  must use institution-owned app registrations and metadata
  fingerprints.

## Fixture Profiles

The CLI ships four fixture profiles, modeled on two institutions: an OIDC
profile, a SAML compatibility profile, an Okta-fronted OIDC profile, and an
optional Kerberos/GSSAPI local-verifier profile.
`okg nomos provider fixture --help` lists their names. Create one:

```bash
okg nomos provider fixture \
  --provider <provider> \
  --audience akmon-research \
  --provider-id example-sso \
  --out example-sso-provider.json \
  --json
```

The Kerberos/GSSAPI profile also takes `--service-principal`, for example
`HTTP/okg-gateway.example.org@EXAMPLE.ORG`, and uses the same value as its
audience.

Validate a fixture without contacting an identity provider:

```bash
okg nomos provider validate --provider example-sso-provider.json --json
```

## Claim Mapping

OIDC and SAML profiles should prefer role claims over group claims; each
fixture names the institution's own role claim or attribute. Group claims
are disabled by default and only grant authority when the provider profile
explicitly marks them approved and bounded.

The Okta-fronted fixture maps a username claim to the stable external
subject and requires Duo/MFA assurance. The Kerberos/GSSAPI fixture maps a
verified principal in its realm and a service principal to a local claim
envelope.

## Evaluate a verified claim

The identity flow has five steps:

1. Register an `IdentityProvider` for the deployment scope.
2. Normalize evidence from the verifier with `normalize_identity_claim`.
3. Resolve the claim through scoped `SubjectBinding` records.
4. Require a matching `PermissionGrant`.
5. Evaluate the resolved Nomos subject against the active policy bundle.

```python
from okg.substrate.nomos import (
    IdentityProvider,
    PermissionGrant,
    SubjectBinding,
    evaluate_identity_claim,
    normalize_identity_claim,
)

provider = IdentityProvider(
    provider_id="example-sso",
    provider_type="oidc",
    issuer="https://sso.example.org/realms/example",
    audiences=("akmon-research",),
    key_reference="secret://example-sso/jwks",
    freshness_seconds=3600,
    group_claims_approved=True,
)

claim = normalize_identity_claim(
    {
        "provider_id": "example-sso",
        "verification_status": "valid",
        "issuer": provider.issuer,
        "raw_token": "<token used only for fingerprinting>",
        "claims": {
            "sub": "example-user-123",
            "aud": "akmon-research",
            "groups": ["research-ops"],
            "acr": "mfa",
            "exp": "2026-05-25T13:00:00Z",
            "auth_time": "2026-05-25T11:59:00Z",
        },
    },
    [provider],
    now="2026-05-25T12:00:00Z",
)

result = evaluate_identity_claim(
    bundle,
    claim,
    bindings=[
        SubjectBinding(
            binding_id="binding:example:research-operator",
            provider_id="example-sso",
            issuer=provider.issuer,
            claim_path="groups",
            claim_value="research-ops",
            subject="subject:example-research:operator",
            scope="deployment:example-research",
        )
    ],
    permission_grants=[
        PermissionGrant(
            grant_id="grant:example-research:operator-record",
            subject="subject:example-research:operator",
            actions=("mcp.record",),
            scope="deployment:example-research",
        )
    ],
    action="mcp.record",
    resource={"deployment": "example-research", "surface": "mcp"},
    scope="deployment:example-research",
)
```

Bindings must include the provider and issuer: the same role or group name
from two institutions does not confer the same authority unless both issuers
are bound.

The persisted claim envelope contains a fingerprint and allowlisted claims
only. Raw tokens, SAML assertions, cookies, passwords, refresh tokens, and
private keys are not part of Nomos state. Unknown issuers, expired claims,
audience mismatches, ambiguous bindings, and missing grants fail closed with
explicit reason codes.

### Missing evidence fails closed

Nomos does not infer verification from missing fields:

| Missing input | Result |
| --- | --- |
| `verification_status` | The claim is `unverified`. |
| `auth_time` when the provider sets `freshness_seconds` | The claim has `missing_auth_time`. |
| `now` argument | Current UTC time is used; expiry, not-yet-valid, and staleness checks still run. |

`build_nomos_claim_from_verified_claims` sets `verification_status` after it
checks nonce, issuer, subject, and audience. Other callers must set the status
themselves. Unknown status values are rejected.

A binding with `effective_from` or `effective_until` matches only while its
window is open. Find inactive windows before enabling enforcement:

```sql
SELECT deployment_name, binding_id, effective_from, effective_until
FROM okg.nomos_subject_bindings
WHERE (effective_until IS NOT NULL AND effective_until <= now())
   OR (effective_from IS NOT NULL AND effective_from > now());
```

Requests with no active binding fail with
`nomos.identity.no_subject_binding`.

### Register identity providers

An identity provider row says which issuer, audience and claims Nomos
accepts. Manage providers with `okg nomos provider`:

```bash
okg nomos provider add --dsn "$OKG_DSN" --deployment <deployment> \
  --for-oidc-auth --reason "public MCP launch" --json

okg nomos provider add --dsn "$OKG_DSN" --deployment <deployment> \
  --provider-id example-sso --issuer https://sso.example.org/realms/research \
  --audience example-app --subject-claim email --json

okg nomos provider disable --dsn "$OKG_DSN" --deployment <deployment> \
  --provider-id example-sso --reason "issuer compromised" --json

okg nomos provider list --dsn "$OKG_DSN" --deployment <deployment> --json
```

- `add` registers an active provider. It refuses an id that already
  exists, whatever its status (`nomos.provider.exists`), so it never
  rewrites a provider that bindings and decision records name and never
  re-enables a disabled one.
- When the deployment manifest declares `nomos.oidc_auth` for the same
  provider id, `add` runs the check the MCP server runs at start: issuer,
  audience and subject claim must match the block, `roles` must be a role
  claim, and `mcp-oidc:jwks` must be a trusted verifier. A row that would
  fail is refused with `nomos.provider.oidc_auth_mismatch`.
  `--for-oidc-auth` takes those values from the block and trusts
  `mcp-oidc:jwks`; without a block it fails with
  `nomos.provider.oidc_auth_undeclared`. The output's `oidc_auth_check`
  says what was checked.
- `okg nomos provider readiness` also reports a provider with no
  `--key-reference` or `--freshness-seconds`. Set `--freshness-seconds`
  only when the provider's tokens carry `auth_time`: with it set, a claim
  without `auth_time` is refused.
- `disable` sets the status to `disabled` and records who, when and why.
  Every claim naming the provider is then refused as `provider_disabled`,
  whatever bindings and grants exist. Other active providers with the same
  issuer are listed under `same_issuer_providers` with a warning. An
  unknown id fails with `nomos.provider.not_found`.

### Manage subject bindings

A subject binding maps one user or role from a registered provider to a
Nomos subject. Manage bindings with `okg nomos binding`:

```bash
okg nomos binding add --dsn "$OKG_DSN" --deployment <deployment> \
  --provider-id example-sso --claim-value alice@example.org \
  --subject subject:example-research:analyst \
  --effective-until 2026-12-31T00:00:00+00:00 --json

okg nomos binding add --dsn "$OKG_DSN" --deployment <deployment> \
  --provider-id example-sso --claim-path roles --claim-value research-operator \
  --subject subject:example-research:operator --json

okg nomos binding disable --dsn "$OKG_DSN" --deployment <deployment> \
  --binding-id binding:example-sso:external_subject:alice@example.org \
  --reason "left the project" --json

okg nomos binding list --dsn "$OKG_DSN" --deployment <deployment> --json
```

- `add` refuses a provider the deployment has not registered, and takes
  the issuer from the provider row. `--claim-path` is `external_subject`
  (the default), `roles`, `groups` or `entitlements`. The default scope,
  `deployment:<deployment>`, is the one the MCP request gate reads.
  `add` refuses a binding id that already exists. `--replace` changes
  only the window and priority of an active binding. It never re-enables
  a disabled binding and never changes the subject, provider, claim,
  scope or required assurance (such as an MFA requirement); add a new
  binding id for those.
- With `--replace`, `--priority`, `--effective-from` and
  `--effective-until` change only what you pass. A flag you leave out
  keeps the stored value, so extending a binding's end date never
  removes its start date. To remove a stored start or end on purpose,
  pass `--clear-effective-from` or `--clear-effective-until`; without
  an end, the binding never expires. A replace records the priority and
  window it replaced in the row's history.
- `disable` sets the status to `disabled` and records who, when and why
  in the row's metadata. It also disables every other active binding on
  the same provider, scope and claim, so that user (or role, or group)
  stops matching at once; the output lists them under `also_disabled`.
  Claim names that mean the same thing count as the same claim: `sub`,
  `subject`, `kerberos_username` and `mit_kerberos_username` are the
  user, like `external_subject`, and `role`, `group` and `entitlement`
  are `roles`, `groups` and `entitlements`.
  Role and group bindings the user's token may still carry are listed
  under `still_matching` with a warning, not disabled. Rows are kept,
  because decision records name binding ids.
- `list` shows every binding, disabled and expired ones included;
  `in_effect` says whether the gate would use it now.

In OIDC mode the next request from a user whose binding is disabled or
expired is refused with `nomos.identity.no_subject_binding`, even though
the token is valid. The decision row names the provider-qualified user,
`external:<provider-id>:<user>`.

**Limit: revocation at the identity provider.** The server does not ask
the identity provider whether a token was revoked. A user disabled only
at the provider keeps access until the token expires. To cut access at
once, disable the binding here as well.

### Manage permission grants

A permission grant lets (or forbids) a Nomos subject some actions in one
scope. Manage grants with `okg nomos grant`:

```bash
okg nomos grant add --dsn "$OKG_DSN" --deployment <deployment> \
  --grant-id grant:analyst:public-docs --subject subject:example-research:analyst \
  --action graph.read.mcp --match source_id=public_docs --priority 10 --json

okg nomos grant disable --dsn "$OKG_DSN" --deployment <deployment> \
  --grant-id grant:analyst:public-docs --reason "source retired" --json

okg nomos grant list --dsn "$OKG_DSN" --deployment <deployment> \
  --subject subject:example-research:analyst --json
```

- The request gate applies only the FIRST grant that matches the
  subject, action, scope, classification and resource, ordered by highest
  priority and then by lowest grant id. The effect plays no part in the
  order: at equal priority `grant:a-read` (allow) decides before
  `grant:z-block` (deny). `--match KEY=VALUE` limits a grant to resources
  with that value; `--effect deny` writes a grant that blocks.
- `add` refuses an existing grant id, whatever its status
  (`nomos.grant.exists`). To change a grant, disable it and add a new id.
  It warns when no in-effect binding resolves any user to the subject.
- `add` checks the order against the subject's other active grants that
  may match the same requests (they overlap unless an action,
  classification or `--match` value tells them apart). A new allow that
  would decide before an overlapping deny is refused with
  `nomos.grant.widens_access` unless you pass `--allow-widen`. A new deny
  that an overlapping allow would decide before is refused with
  `nomos.grant.shadowed`; the message names the `--priority` it needs.
- A deny grant whose action is not a Nomos action, whose classification
  is not a Nomos data classification, or whose `--match` key is not one
  the gate reads as a string, would block nothing, so `add` refuses it
  (`nomos.grant.unknown_match`). An allow grant with such a name is
  written with a warning under `match_warnings`: it grants nothing.
  `--match` stores a string and the gate compares by equality, so keys
  whose values are numbers, booleans or mappings (`limit`, `offset`,
  `row_cap`, `timeout_ms`, `audit`, `source_lineage`) are not accepted.
  Accepted: `deployment`, `scope`, `kind`, `operator_name`,
  `resource_kind`, `surface`, `nomos_surface`, `tool`, `record_kind`,
  `method`, `query`, `rank_by`, `search_profile`, `resource_id`,
  `agent_run_id`, `context_id`, `access_context_id`, `graph_ref`,
  `node_ref`, `edge_ref`, `source_record_ref`; and the list-valued `source_id`,
  `source_ids`, `source_family`, `source_bundle*`, `source_record_refs`,
  `subtype_filter` and access-context ref lists.
- Two different values on a list-valued key can match the same resource:
  a source bundle matches when the resource lists it, and a value matches
  an empty list. So `add` treats such grants as overlapping for the order
  checks above; only different values on a single-string key keep two
  grants apart. `add` holds a lock per deployment and subject while it
  checks and writes, so two concurrent adds cannot both pass.
- Provider, binding and grant commands read `$OKG_DSN` when `--dsn` is
  not given.
  `provider add` prints on stderr when the manifest could not be read
  (so the OIDC check did not run) and when the provider has neither
  `--trusted-verifier` nor `--require-verified-by`.
- `disable` keeps the row and records who, when and why. Another allow
  grant for the same subject may still permit the same requests; those
  are listed under `still_allowing`, labelled `covers` when they permit
  every request the disabled grant did. Disabling a deny grant can admit
  requests it blocked, so it is refused with `nomos.grant.widens_access`
  unless you pass `--allow-widen`.

A database error in any provider, binding or grant command is a JSON
error with exit 1 (`nomos.<noun>.database_error`; nothing changed). A
failed COMMIT reports `nomos.<noun>.commit_outcome_unknown`: run the
matching `list` to see whether the change was saved.

### Block a user

A block refuses one user even when a role binding admits them. It
outranks every binding, whatever its priority:

```bash
okg nomos binding block --dsn "$OKG_DSN" --deployment <deployment> \
  --provider-id cern-sso --user alice --reason "abuse report" --json

okg nomos binding unblock --dsn "$OKG_DSN" --deployment <deployment> \
  --provider-id cern-sso --user alice --reason resolved --json
```

- `--user` is the user's value of the claim `nomos.oidc_auth.subject_claim`
  names (for the CERN recipe below, `cern_upn`). The refusal a
  user sees does not show it. Every sign-in the server evaluates,
  admitted or refused, records it in `external_subject`, so an operator
  finds it with:

  ```sql
  SELECT external_subject, roles, observed_at
    FROM okg.v_nomos_external_claims
   WHERE deployment_name = 'my-graph' AND provider_id = 'cern-sso'
   ORDER BY observed_at DESC;
  ```

  The value must equal the token's value exactly: only
  surrounding whitespace is trimmed and case matters, so `Alice` does not
  block `alice`. After the block is saved, `block` looks for that exact
  value in the deployment's bindings and recorded sign-ins (a read-only
  check bounded by a 5-second timeout) and warns on stderr and in the
  JSON `warning` field when it finds none, listing values that differ
  only in case. If the check itself fails, the block stays saved and the
  warning says the check failed. The block is a binding row with id
  `binding:<provider-id>:block:<user>` and the reserved subject
  `nomos:blocked`; no database change is needed. `binding add` and
  `binding disable` refuse that id form, whatever the provider id.
- The next request from that user is refused with
  `nomos_principal_blocked`. Two audit rows record it: the identity
  decision (reason `nomos.identity.principal_blocked`, naming the block)
  and the gate's refusal row (`refusal_kind: blocked_principal`).
- `unblock` disables only the block row, so the user's role bindings
  admit them again. `binding disable` refuses a block row, `binding add`
  refuses the subject `nomos:blocked`, and disabling a user's own binding
  never lifts their block (all `nomos.binding.block`).
- A block, like a disable, takes effect on the next request. The server
  does not ask the identity provider about revoked tokens; a user
  removed only at CERN keeps access until their token expires.

### CERN recipe for a read-only server

Every mapped CERN role gets the whole read-only knowledge base. Nobody
gets in through a per-user row: users are admitted only by a role
binding an operator writes, and the server never creates a binding for
a user who signs in.

1. Declare the OIDC block in the deployment manifest. Use `cern_upn`
   as the subject claim; a token without `cern_upn` is refused
   (`nomos_principal_unidentified`). This has not yet been checked
   against a real CERN token; confirm it with the first real sign-in.

   ```yaml
   nomos:
     runtime_enforcement: enforce
     oidc_auth:
       provider_id: cern-sso
       issuer: https://auth.cern.ch/auth/realms/cern
       audience: my-graph
       subject_claim: cern_upn
       roles_claim: cern_roles
       principal_kind: human_operator
   ```

2. Register the provider from that block, bind each mapped role to
   `subject:mcp_client`, and grant that subject the read actions:

   ```bash
   okg nomos provider add --dsn "$OKG_DSN" --deployment my-graph --for-oidc-auth --json
   okg nomos binding add --dsn "$OKG_DSN" --deployment my-graph \
     --provider-id cern-sso --claim-path roles --claim-value my-graph-reader \
     --subject subject:mcp_client --json
   okg nomos grant add --dsn "$OKG_DSN" --deployment my-graph \
     --grant-id grant:my-graph:mcp-client:read --subject subject:mcp_client \
     --action graph.read.mcp --action mcp.tool.call --json
   ```

   Bind to `subject:mcp_client`, not a subject of your own: a grant
   alone does not admit a subject, because the active policy must also
   allow it, and the rollout baseline (`okg deployment
   nomos-rollout-audit --apply-baseline`) allows MCP tool calls only
   for `subject:mcp_client`. A role bound to any other
   subject resolves and matches its grant, and then every tool call is
   refused `nomos_identity_denied` with `nomos.default_deny`. The
   baseline writes those rules only when the posture's
   `read_principals` lists `mcp_client`; it must also list the block's
   `principal_kind` (`human_operator`).

   The server puts the verified `cern_roles` values under `roles`, so
   role bindings use `--claim-path roles`. Repeat the binding for each
   mapped CERN role, all to `subject:mcp_client`. Every admitted CERN
   user therefore reads as the same subject; to stop one user, block
   them (see "Block a user" above).
3. Start the server and name every host name clients use to reach it:

   ```bash
   okg mcp-serve --deployment my-graph --transport streamable-http \
     --host 0.0.0.0 --port 8794 --oidc-auth \
     --allowed-host my-graph.example.cern.ch:8794
   ```

   Without `--allowed-host` the server accepts only the loopback names
   (`localhost`, `127.0.0.1`, `[::1]`) in the HTTP `Host` header,
   whatever `--host` it listens on. A client that connects by any other
   name passes the token check and then gets HTTP 421 (a client without
   a valid token still gets 401). This guard blocks DNS rebinding, where
   a web page tricks a browser into calling a server by a name the page
   controls. The value must match the `Host` header exactly, port
   included; `name:*` allows any explicit port. Repeat the flag for
   each name.
4. A valid CERN token whose roles match no binding is refused with
   `nomos_principal_unidentified` and audited. The refusal row records
   `identity_channel: oidc_bearer_token`.

MFA: the provider row written by `--for-oidc-auth` requires no MFA and
no fresh sign-in, so CERN tokens without `acr` or `auth_time` are
accepted. The `cern-oidc` fixture profile (`okg nomos provider
fixture`) does require MFA and a one-hour fresh sign-in; do not
register this deployment from it. A deployment that wants MFA passes
`--mfa-required` or `--required-assurance <value>` to `provider add`.

`mcp-serve --oidc-auth` refuses to start when `nomos.runtime_enforcement`
is anything but `enforce` (that is, `audit` or `off`) and the server binds
anything but loopback (`mcp_oidc_auth_audit_exposed`). Users no binding
admits are refused in every mode, but only `enforce` withholds the rows a
user's grants do not cover: `audit` records a denial and serves the rows
anyway, and `off` filters nothing. Use `enforce` for any reachable server.

### Caller-supplied identity claims

A multi-user server (OIDC or header mode) refuses
`authority.nomos_identity_claim` outright
(`nomos_identity_channel_conflict`). A single-user server refuses it
too (`nomos_identity_claim_untrusted`), because nothing on the server
checks a signature on it and any caller could name any identity. An
operator who accepts that every caller of a single-user server may
choose its own Nomos identity can start that server with
`OKG_MCP_TRUST_CALLER_IDENTITY_CLAIMS=1`; the MCP `record` and read
operators then evaluate the claim. Poros accepts the same evidence
through `--nomos-identity-claim`. The MCP server always evaluates the
deployment's active Postgres bundle in the deployment scope; a
client-supplied bundle path, scope, or resource fact is refused (see the
Nomos runbook).

Whatever the mode, `record` and `propose submit` drop every
`authority` key starting with `_nomos` (only the server writes those),
and `propose submit` crosses the same Nomos boundary as `record`
(policy action `mcp.record`).

## Doctor Checks

Run:

```bash
okg doctor --check nomos --deployment <deployment> --json
```

The Nomos doctor fails closed on missing issuer, audience,
key/metadata secret reference, freshness policy, required assurance,
MFA requirement, revoked provider, rotating provider, Kerberos realm,
Kerberos service principal, and configured drift metadata.

For production cutover, also run:

```bash
okg nomos provider readiness \
  --dsn "$OKG_DSN" \
  --deployment <deployment> \
  --production \
  --json
```

Production readiness is stricter: it fails when an active
provider still carries fixture metadata, uses a fixture secret
reference, lacks trusted verifier metadata, lacks expected drift
metadata, has approved group claims without an allowlist, has no active
policy/config, has no active subject binding or permission grant, or has
no recent claim/decision evidence.

Production claim envelopes should carry bounded verifier provenance:

```json
{
  "provider_id": "example-sso",
  "verification_status": "valid",
  "issuer": "https://sso.example.org/realms/example",
  "verified_by": "verifier:example-sso-gateway",
  "verification_method": "oidc-jwks",
  "verified_at": "2026-05-25T11:59:30Z",
  "claims": {
    "sub": "example-user-123",
    "aud": "akmon-research",
    "roles": ["research-operator"],
    "acr": "mfa",
    "exp": "2026-05-25T13:00:00Z",
    "auth_time": "2026-05-25T11:59:00Z"
  }
}
```

When a provider metadata record includes `production: true`,
`require_verified_by: true`, or non-empty `trusted_verifiers`, Nomos
fails closed if the claim omits verifier evidence, cites an untrusted
verifier id, or carries stale verifier evidence.

## Local OIDC SSO Test Harness

Use the OIDC harness when the institution gives you an OIDC application
registration. It opens a browser, waits on a loopback callback, verifies
the ID token against issuer discovery metadata and JWKS, and writes a
bounded Nomos claim file. It does not persist ID tokens, access tokens,
refresh tokens, cookies, passwords, assertions, or private keys.

Register this redirect URI exactly in the institution's application:

```text
http://127.0.0.1:8765/nomos/oidc/callback
```

Test an OIDC single sign-on provider:

```bash
export OIDC_CLIENT_ID=<client-id-from-the-institution-application-portal>
export OIDC_CLIENT_SECRET=<client-secret-from-secret-manager>

okg nomos provider oidc-login \
  --issuer https://sso.example.org/realms/example \
  --client-id "$OIDC_CLIENT_ID" \
  --client-secret-env OIDC_CLIENT_SECRET \
  --provider-id example-sso \
  --provider-type oidc \
  --verified-by verifier:example-sso-gateway \
  --redirect-port 8765 \
  --out .scratch/example-nomos-claim.json \
  --json
```

For an Okta-fronted registration that exposes OIDC, set `--issuer` to the
issuer the registration gives you and pass `--provider-type touchstone_oidc`
with that provider's own id and verifier id.

If the institution's application is SAML-only, keep SAML verification in an
institution-owned gateway and emit the same bounded Nomos claim envelope.
Nomos should still see only provider id/type, issuer, audience, subject
claims, verifier provenance, and normalized claims.

Then require production readiness before enabling mutation-capable paths:

```bash
okg nomos provider readiness \
  --dsn "$OKG_DSN" \
  --deployment <deployment> \
  --production \
  --json
```

## Production Cutover

Before using a real institutional identity provider:

- Register an institution-owned application/service provider.
- Record issuer/entity id, audience/client id, and metadata
  fingerprint in Nomos.
- Store JWKS, SAML certificate, or keytab material outside Nomos and
  reference it with `secret://`, `vault://`, `keyring://`, or
  `aws-sm://`.
- Record trusted verifier ids in provider metadata. The verifier is the
  gateway/adapter that performed OIDC/JWKS, SAML, Okta, Duo,
  or GSSAPI verification before emitting the bounded Nomos claim
  envelope.
- Register the provider, subject bindings and permission grants with
  `okg nomos provider add`, `okg nomos binding add` and
  `okg nomos grant add`.
- Run `make verify-nomos`, focused Nomos identity tests, and
  `okg doctor --check nomos` against an ephemeral migrated database.
- Run `okg nomos provider readiness --production` and require `ok=true`
  before enabling mutation-capable Poros, MCP, read/export, or agent
  action paths.
