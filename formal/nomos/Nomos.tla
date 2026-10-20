---- MODULE Nomos ----
EXTENDS FiniteSets, Naturals

CONSTANTS
  PolicyGraphs,
  ConfigGraphs,
  Bundles,
  ConfigSets,
  Requests,
  Generations,
  Scopes,
  Subjects,
  IdentityProviders,
  ExternalClaims,
  SubjectBindings,
  PermissionGrants,
  NoBundle,
  NoConfig,
  NoRequest,
  NoProvider,
  NoClaim,
  NoBinding,
  NoGrant,
  DefaultScope,
  SourceSubject,
  OkgSubject,
  AgentSubject,
  AdminSubject,
  ReaderSubject,
  OperatorSubject,
  ScopedClaims,
  ExpiredClaims,
  UnscopedClaims,
  ActiveSubjectBindings,
  TrustedProviderBindings,
  ScopedClaimBindings,
  ReaderSubjectBindings,
  OperatorSubjectBindings,
  DefaultScopeBindings,
  ActivePermissionGrants,
  ReaderSubjectGrants,
  OperatorSubjectGrants,
  DefaultScopeGrants,
  GraphReadMcpGrants,
  McpRecordGrants,
  PorosSubmitGrants,
  PublicDataGrants,
  InternalDataGrants,
  AllowGrants,
  PublicRequests,
  RestrictedRequests,
  MutationRequests,
  PorosRequests,
  ReadRequests,
  ExportRequests,
  UnknownLicenseRequests,
  PrivateExportRequests,
  ConfigChangeRequests,
  PostgresRequests,
  ExternalClaimRequests,
  TrustedClaimRequests,
  UntrustedClaimRequests,
  ScopedClaimRequests,
  ExpiredClaimRequests,
  AudienceMismatchRequests,
  MissingAssuranceRequests,
  AmbiguousBindingRequests,
  RevokedProviderRequests,
  RevokedKerberosServicePrincipalRequests,
  UnverifiedClaimRequests,
  PersistedSecretClaimEvidence,
  BuggySecretPersistence,
  BuggySkippedClaimChecks,
  BuggyPublishPinsStaleBundle,
  BuggyRotationRestampsPins,
  AuthorizedRequests,
  UnauthorizedRequests,
  RedactionEvidence,
  DeclassificationEvidence,
  ProfileHintRequests,
  AgentSelfExpandRequests,
  MissingPolicyRequests,
  WeakDeploymentPostureRequests,
  SourceDeploymentMismatchRequests,
  SourceLineageRequests,
  MixedSourceLineageRequests,
  PartiallyAuthorizedSourceLineageRequests,
  MissingSourceLineageRequests,
  MalformedSourceLineageRequests,
  UnknownSourceLineageRequests,
  StaleSourcePolicyRequests,
  FineGrainedAccessRequests,
  FineGrainedAuthorizedRequests,
  FineGrainedUnaddressableRequests,
  FineGrainedFutureHorizonRequests,
  FineGrainedGraphDeniedRequests,
  FineGrainedDefaultAllowRequests,
  UnknownActionRequests,
  NoExportRequests,
  SecretUseRequests,
  TracePayloadLeakRequests

Effects == {
  "allow", "deny", "review", "quarantine", "redact", "declassify"
}

DeploymentClasses == {
  "ci_authority",
  "dogfood_workspace",
  "personal_okg",
  "public_reference_demo",
  "synthetic_healthcare_demo",
  "governed_research_metadata",
  "org_operational_private",
  "private_product_engineering"
}

SourceSensitivities == {
  "credentialed_external",
  "internal",
  "local_private",
  "phi_synthetic",
  "private",
  "public",
  "regulated_metadata",
  "restricted",
  "secret"
}

DataClassifications == {
  "confidential",
  "internal",
  "private",
  "public",
  "regulated_metadata",
  "restricted",
  "secret",
  "synthetic_phi"
}

PrincipalKinds == {
  "agent",
  "automation",
  "ci_runner",
  "external_identity",
  "human_operator",
  "mcp_client",
  "okg_service",
  "source_adapter",
  "system"
}

ResourceKinds == {
  "agent_run",
  "agent_runtime",
  "config_set",
  "container",
  "deployment",
  "edge",
  "external_action",
  "external_automation_run",
  "external_endpoint",
  "file",
  "generation",
  "graph",
  "graph_export",
  "local_service",
  "mcp_tool",
  "network_destination",
  "node",
  "policy_bundle",
  "secret",
  "source",
  "ssh_identity",
  "transcript"
}

Actions == {
  "admin.override",
  "agent.bypass",
  "agent.launch",
  "config.activate",
  "declassify",
  "deployment.posture.audit",
  "docker.run",
  "external_action.attest_owner_unavailable",
  "external_action.cancel",
  "external_action.decide",
  "external_action.execute",
  "external_action.propose",
  "external_action.read",
  "external_action.read_payload",
  "external_action.reconcile",
  "external_action.report",
  "external_action.restore_reconcile",
  "external_automation.register",
  "external_automation.read",
  "external_automation.link",
  "file.read",
  "file.write",
  "generation.publish",
  "graph.export",
  "graph.read",
  "graph.read.audit",
  "graph.read.cli_search",
  "graph.read.export_like",
  "graph.read.local_operator",
  "graph.read.mcp",
  "graph.read.public_demo",
  "graph.read.sql_admin",
  "mcp.record",
  "mcp.tool.call",
  "network.egress",
  "policy.activate",
  "poros.graph.submit",
  "quarantine.release",
  "redact",
  "secret.use",
  "service.persist",
  "source.emit",
  "source.ingest",
  "source.live_call",
  "ssh.use",
  "transcript.persist"
}

Obligations == {
  "audit",
  "break_glass_review",
  "declassification",
  "no_export",
  "quarantine",
  "redaction",
  "review",
  "secret_ref_required",
  "source_retention"
}

EnforcementPoints == {
  "agent.launch",
  "agent.bypass",
  "graph.read",
  "graph.export",
  "mcp.tool.call",
  "mcp.record",
  "source.ingest",
  "source.emit",
  "source.live_call",
  "secret.use",
  "ssh.use",
  "docker.run",
  "network.egress",
  "file.read",
  "file.write",
  "transcript.persist",
  "service.persist",
  "generation.publish",
  "policy.activate",
  "config.activate",
  "admin.override"
}

UnknownAction == "unknown.action"

\* The eight institutional IAM claim checks, named so a buggy-toggle witness
\* can disable exactly one and refute that check's *CannotGrant invariant by
\* name. The positive configs set BuggySkippedClaimChecks = {}; each witness
\* config names exactly one mode.
\*
\* "unverified" abstracts the three absence arms of issue #1007: a claim that
\* states no verification status, one that supplies no auth_time against a
\* declared freshness requirement, and one evaluated with no clock so the
\* temporal checks never run. All three mean the same thing at this level of
\* abstraction -- the evidence needed to authorize is ABSENT -- and the
\* implementation makes each of them fail closed rather than read as success.
ClaimCheckModes == {
  "untrusted",
  "expired",
  "audience",
  "assurance",
  "ambiguous",
  "revoked_provider",
  "revoked_kerberos",
  "unverified"
}

ClaimCheckEnforced(mode) == mode \notin BuggySkippedClaimChecks

RuleIds == {
  "none",
  "default_deny",
  "rule_allow",
  "rule_quarantine",
  "rule_review",
  "rule_redact",
  "rule_declassify"
}

PublicSurfaceRequests == ReadRequests \cup (ExportRequests \ PrivateExportRequests)

VARIABLES
  policyGraphs,
  configGraphs,
  compiledBundles,
  compiledConfigSets,
  activeBundle,
  activeConfig,
  configActivationDecisions,
  identityProviders,
  externalClaimEnvelopes,
  subjectBindings,
  permissionGrants,
  postgresPolicyState,
  decisions,
  visibleState,
  quarantineState,
  publicOutputs,
  publishedGenerations,
  generationPolicyPins,
  pinnedAtPublish,
  persistedSecretEvidence

DefaultPostgresPolicyState == [
  bundle |-> NoBundle,
  config |-> NoConfig
]

DefaultDecision == [
  recorded |-> FALSE,
  effect |-> "none",
  bundle |-> NoBundle,
  postgresConfig |-> NoConfig,
  configPin |-> NoConfig,
  policyPin |-> NoBundle,
  action |-> "none",
  dataClassification |-> "none",
  sourceSensitivity |-> "none",
  deploymentClass |-> "none",
  enforcementPoint |-> "none",
  obligations |-> {},
  hasRedaction |-> FALSE,
  hasDeclassification |-> FALSE,
  identityProvider |-> NoProvider,
  claimEnvelope |-> NoClaim,
  subjectBinding |-> NoBinding,
  permissionGrant |-> NoGrant,
  inputHash |-> "none",
  matchedRule |-> "none",
  tracePrivacySafe |-> TRUE
]

RequestScope(req) ==
  DefaultScope

RequestSubject(req) ==
  IF req \in ConfigChangeRequests THEN AdminSubject
  ELSE IF req \in AgentSelfExpandRequests THEN AgentSubject
  ELSE IF req \in ExternalClaimRequests /\ req \in (MutationRequests \cup PorosRequests)
       THEN OperatorSubject
  ELSE IF req \in MutationRequests THEN SourceSubject
  ELSE IF req \in PorosRequests THEN OkgSubject
  ELSE IF req \in ExportRequests THEN AgentSubject
  ELSE ReaderSubject

RequestClaimCandidates(req) ==
  (IF req \in ScopedClaimRequests THEN ScopedClaims ELSE {})
  \cup (IF req \in ExpiredClaimRequests THEN ExpiredClaims ELSE {})
  \cup (
    IF req \in ExternalClaimRequests
       /\ req \notin ScopedClaimRequests
       /\ req \notin ExpiredClaimRequests
    THEN UnscopedClaims
    ELSE {}
  )

RequestClaimKnown(req) ==
  /\ req \in ExternalClaimRequests
  /\ RequestClaimCandidates(req) # {}

RequestClaim(req) ==
  CHOOSE claim \in RequestClaimCandidates(req): TRUE

RequestClaimOrNone(req) ==
  IF RequestClaimKnown(req) THEN RequestClaim(req) ELSE NoClaim

RequestProviderKnown(req) ==
  /\ RequestClaimKnown(req)
  /\ identityProviders # {}

RequestProvider(req) ==
  CHOOSE provider \in identityProviders: TRUE

RequestProviderOrNone(req) ==
  IF RequestProviderKnown(req) THEN RequestProvider(req) ELSE NoProvider

ConfigPolicy(config) ==
  CHOOSE bundle \in Bundles: TRUE

ConfigScope(config) ==
  CHOOSE scope \in Scopes: TRUE

BindingSubjectMatches(req, binding) ==
  \/ /\ RequestSubject(req) = ReaderSubject
     /\ binding \in ReaderSubjectBindings
  \/ /\ RequestSubject(req) = OperatorSubject
     /\ binding \in OperatorSubjectBindings

BindingMatches(req, binding) ==
  /\ binding \in subjectBindings
  /\ binding \in ActiveSubjectBindings
  /\ req \in ExternalClaimRequests
  /\ RequestProviderOrNone(req) \in identityProviders
  /\ RequestClaimOrNone(req) \in externalClaimEnvelopes
  /\ RequestClaimOrNone(req) \in ScopedClaims
  /\ binding \in TrustedProviderBindings
  /\ binding \in ScopedClaimBindings
  /\ BindingSubjectMatches(req, binding)
  /\ binding \in DefaultScopeBindings
  /\ RequestScope(req) = DefaultScope

MatchingBindings(req) ==
  {binding \in subjectBindings: BindingMatches(req, binding)}

SubjectBoundFor(req) ==
  MatchingBindings(req) # {}

MatchedBindingOrNone(req) ==
  IF MatchingBindings(req) # {}
  THEN CHOOSE binding \in MatchingBindings(req): TRUE
  ELSE NoBinding

RequestAction(req) ==
  IF req \in UnknownActionRequests THEN UnknownAction
  ELSE IF req \in ConfigChangeRequests THEN "config.activate"
  ELSE IF req \in AgentSelfExpandRequests THEN "admin.override"
  ELSE IF req \in PorosRequests THEN "poros.graph.submit"
  ELSE IF req \in MutationRequests THEN "mcp.record"
  ELSE IF req \in SecretUseRequests THEN "secret.use"
  ELSE IF req \in ExportRequests THEN "graph.export"
  ELSE IF req \in ReadRequests THEN "graph.read.mcp"
  ELSE "graph.read"

RequestDataClassification(req) ==
  IF req \in SecretUseRequests THEN "secret"
  ELSE IF req \in PrivateExportRequests THEN "private"
  ELSE IF req \in SourceDeploymentMismatchRequests THEN "restricted"
  ELSE IF req \in WeakDeploymentPostureRequests THEN "restricted"
  ELSE IF req \in RestrictedRequests THEN "restricted"
  ELSE IF req \in PublicRequests THEN "public"
  ELSE "internal"

GrantSubjectMatches(req, grant) ==
  \/ /\ RequestSubject(req) = ReaderSubject
     /\ grant \in ReaderSubjectGrants
  \/ /\ RequestSubject(req) = OperatorSubject
     /\ grant \in OperatorSubjectGrants

GrantActionMatches(req, grant) ==
  \/ /\ RequestAction(req) = "graph.read.mcp"
     /\ grant \in GraphReadMcpGrants
  \/ /\ RequestAction(req) = "mcp.record"
     /\ grant \in McpRecordGrants
  \/ /\ RequestAction(req) = "poros.graph.submit"
     /\ grant \in PorosSubmitGrants

GrantClassificationMatches(req, grant) ==
  \/ /\ RequestDataClassification(req) = "public"
     /\ grant \in PublicDataGrants
  \/ /\ RequestDataClassification(req) = "internal"
     /\ grant \in InternalDataGrants

GrantMatches(req, grant) ==
  /\ grant \in permissionGrants
  /\ grant \in ActivePermissionGrants
  /\ req \in PostgresRequests
  /\ req \in ExternalClaimRequests
  /\ SubjectBoundFor(req)
  /\ GrantSubjectMatches(req, grant)
  /\ grant \in DefaultScopeGrants
  /\ GrantActionMatches(req, grant)
  /\ GrantClassificationMatches(req, grant)
  /\ grant \in AllowGrants
  /\ RequestScope(req) = DefaultScope

MatchingGrants(req) ==
  {grant \in permissionGrants: GrantMatches(req, grant)}

GrantAllows(req) ==
  MatchingGrants(req) # {}

MatchedGrantOrNone(req) ==
  IF MatchingGrants(req) # {}
  THEN CHOOSE grant \in MatchingGrants(req): TRUE
  ELSE NoGrant

RequestSourceSensitivity(req) ==
  IF req \in SecretUseRequests THEN "secret"
  ELSE IF req \in SourceDeploymentMismatchRequests THEN "secret"
  ELSE IF req \in PrivateExportRequests THEN "local_private"
  ELSE IF req \in RestrictedRequests THEN "restricted"
  ELSE IF req \in PublicRequests THEN "public"
  ELSE "internal"

RequestDeploymentClass(req) ==
  IF req \in WeakDeploymentPostureRequests THEN "public_reference_demo"
  ELSE IF req \in SourceDeploymentMismatchRequests THEN "public_reference_demo"
  ELSE IF req \in PrivateExportRequests THEN "personal_okg"
  ELSE "dogfood_workspace"

RequestEnforcementPoint(req) ==
  IF req \in ConfigChangeRequests THEN "config.activate"
  ELSE IF req \in AgentSelfExpandRequests THEN "admin.override"
  ELSE IF req \in SecretUseRequests THEN "secret.use"
  ELSE IF req \in ExportRequests THEN "graph.export"
  ELSE IF req \in PorosRequests THEN "mcp.record"
  ELSE IF req \in MutationRequests THEN "mcp.record"
  ELSE "graph.read"

SourceDeploymentCompatible(req) ==
  ~(
    /\ RequestDeploymentClass(req) = "public_reference_demo"
    /\ RequestSourceSensitivity(req) \in {
         "credentialed_external",
         "local_private",
         "private",
         "restricted",
         "secret"
       }
  )

WeakDeploymentPosture(req) ==
  /\ RequestDeploymentClass(req) = "public_reference_demo"
  /\ RequestDataClassification(req) \in {
       "confidential",
       "private",
       "regulated_metadata",
       "restricted",
       "secret",
       "synthetic_phi"
     }

SourceLineageKnown(req) ==
  /\ req \in SourceLineageRequests
  /\ req \notin MissingSourceLineageRequests
  /\ req \notin MalformedSourceLineageRequests
  /\ req \notin UnknownSourceLineageRequests

SourceLineagePolicyFresh(req) ==
  req \notin StaleSourcePolicyRequests

SourceLineageAuthorized(req) ==
  /\ SourceLineageKnown(req)
  /\ SourceLineagePolicyFresh(req)
  /\ req \notin PartiallyAuthorizedSourceLineageRequests

FineGrainedAccessAuthorized(req) ==
  /\ req \in FineGrainedAccessRequests
  /\ req \in FineGrainedAuthorizedRequests
  /\ req \notin FineGrainedUnaddressableRequests
  /\ req \notin FineGrainedFutureHorizonRequests
  /\ req \notin FineGrainedGraphDeniedRequests
  /\ req \notin FineGrainedDefaultAllowRequests

RequiredObligations(req, effect) ==
  {"audit"}
  \cup (IF effect = "redact" THEN {"redaction"} ELSE {})
  \cup (IF effect = "declassify" THEN {"declassification"} ELSE {})
  \cup (IF effect = "review" THEN {"review"} ELSE {})
  \cup (IF effect = "quarantine" THEN {"quarantine"} ELSE {})
  \cup (IF effect = "deny" /\ req \in AgentSelfExpandRequests
        THEN {"review"} ELSE {})
  \cup (IF req \in RestrictedRequests THEN {"source_retention"} ELSE {})
  \cup (IF req \in NoExportRequests THEN {"no_export"} ELSE {})
  \cup (IF req \in SecretUseRequests THEN {"secret_ref_required"} ELSE {})

InstitutionalClaimChecksPass(req) ==
  /\ ClaimCheckEnforced("untrusted") =>
       (/\ req \notin UntrustedClaimRequests
        /\ IF req \in ExternalClaimRequests
           THEN req \in TrustedClaimRequests
           ELSE TRUE)
  /\ ClaimCheckEnforced("expired") => req \notin ExpiredClaimRequests
  /\ ClaimCheckEnforced("audience") => req \notin AudienceMismatchRequests
  /\ ClaimCheckEnforced("assurance") => req \notin MissingAssuranceRequests
  /\ ClaimCheckEnforced("ambiguous") => req \notin AmbiguousBindingRequests
  /\ ClaimCheckEnforced("revoked_provider") =>
       req \notin RevokedProviderRequests
  /\ ClaimCheckEnforced("revoked_kerberos") =>
       req \notin RevokedKerberosServicePrincipalRequests
  /\ ClaimCheckEnforced("unverified") => req \notin UnverifiedClaimRequests

PostgresStateReady(req) ==
  /\ activeBundle # NoBundle
  /\ activeConfig[RequestScope(req)] # NoConfig
  /\ postgresPolicyState[RequestScope(req)].bundle = activeBundle
  /\ postgresPolicyState[RequestScope(req)].config =
       activeConfig[RequestScope(req)]

PostgresReady(req) ==
  /\ PostgresStateReady(req)
  /\ InstitutionalClaimChecksPass(req)
  /\ SubjectBoundFor(req)
  /\ GrantAllows(req)

EffectiveBundleForRequest(req) ==
  IF req \in PostgresRequests /\ ~PostgresReady(req)
  THEN NoBundle
  ELSE activeBundle

EffectFor(bundle, req) ==
  IF bundle = NoBundle THEN "deny"
  ELSE IF req \in UnknownActionRequests THEN "deny"
  ELSE IF WeakDeploymentPosture(req) THEN "review"
  ELSE IF ~SourceDeploymentCompatible(req) THEN "quarantine"
  ELSE IF req \in AgentSelfExpandRequests THEN "review"
  ELSE IF ~InstitutionalClaimChecksPass(req) THEN "deny"
  ELSE IF req \in ExternalClaimRequests /\ req \notin ScopedClaimRequests
       THEN "deny"
  ELSE IF req \in NoExportRequests /\ req \in ExportRequests THEN "deny"
  ELSE IF req \in ProfileHintRequests /\ req \notin AuthorizedRequests
       THEN "quarantine"
  ELSE IF req \in UnauthorizedRequests THEN "quarantine"
  ELSE IF req \in SourceLineageRequests
          /\ req \in (MalformedSourceLineageRequests \cup UnknownSourceLineageRequests)
       THEN "quarantine"
  ELSE IF req \in FineGrainedAccessRequests /\ ~FineGrainedAccessAuthorized(req)
       THEN "deny"
  ELSE IF req \in FineGrainedAccessRequests THEN "allow"
  ELSE IF req \in SourceLineageRequests /\ ~SourceLineageAuthorized(req)
       THEN "deny"
  ELSE IF req \in SourceLineageRequests THEN "allow"
  ELSE IF req \in RestrictedRequests
          /\ req \in PublicSurfaceRequests
          /\ req \in RedactionEvidence
       THEN "redact"
  ELSE IF req \in RestrictedRequests
          /\ req \in PublicSurfaceRequests
          /\ req \in DeclassificationEvidence
       THEN "declassify"
  ELSE IF req \in UnknownLicenseRequests /\ req \in PublicSurfaceRequests
       THEN "deny"
  ELSE IF req \in PrivateExportRequests /\ req \notin ScopedClaimRequests
       THEN "deny"
  ELSE IF req \in RestrictedRequests /\ req \in PublicSurfaceRequests
       THEN "deny"
  ELSE IF req \in AuthorizedRequests THEN "allow"
  ELSE "deny"

RuleFor(bundle, req) ==
  IF bundle = NoBundle THEN "default_deny"
  ELSE IF req \in UnknownActionRequests THEN "default_deny"
  ELSE IF WeakDeploymentPosture(req) THEN "rule_review"
  ELSE IF ~SourceDeploymentCompatible(req) THEN "rule_quarantine"
  ELSE IF req \in AgentSelfExpandRequests THEN "rule_review"
  ELSE IF ~InstitutionalClaimChecksPass(req) THEN "default_deny"
  ELSE IF req \in ExternalClaimRequests /\ req \notin ScopedClaimRequests
       THEN "default_deny"
  ELSE IF req \in NoExportRequests /\ req \in ExportRequests THEN "default_deny"
  ELSE IF req \in ProfileHintRequests /\ req \notin AuthorizedRequests
       THEN "rule_quarantine"
  ELSE IF req \in UnauthorizedRequests THEN "rule_quarantine"
  ELSE IF req \in SourceLineageRequests
          /\ req \in (MalformedSourceLineageRequests \cup UnknownSourceLineageRequests)
       THEN "rule_quarantine"
  ELSE IF req \in FineGrainedAccessRequests /\ ~FineGrainedAccessAuthorized(req)
       THEN "default_deny"
  ELSE IF req \in FineGrainedAccessRequests THEN "rule_allow"
  ELSE IF req \in SourceLineageRequests /\ ~SourceLineageAuthorized(req)
       THEN "default_deny"
  ELSE IF req \in SourceLineageRequests THEN "rule_allow"
  ELSE IF req \in RestrictedRequests
          /\ req \in PublicSurfaceRequests
          /\ req \in RedactionEvidence
       THEN "rule_redact"
  ELSE IF req \in RestrictedRequests
          /\ req \in PublicSurfaceRequests
          /\ req \in DeclassificationEvidence
       THEN "rule_declassify"
  ELSE IF req \in UnknownLicenseRequests /\ req \in PublicSurfaceRequests
       THEN "default_deny"
  ELSE IF req \in PrivateExportRequests /\ req \notin ScopedClaimRequests
       THEN "default_deny"
  ELSE IF req \in AuthorizedRequests THEN "rule_allow"
  ELSE "default_deny"

DecisionRecord(req, bundle) == [
  recorded |-> TRUE,
  effect |-> EffectFor(bundle, req),
  bundle |-> bundle,
  postgresConfig |->
    IF req \in PostgresRequests /\ bundle # NoBundle
    THEN activeConfig[RequestScope(req)]
    ELSE NoConfig,
  configPin |-> activeConfig[RequestScope(req)],
  policyPin |-> bundle,
  action |-> RequestAction(req),
  dataClassification |-> RequestDataClassification(req),
  sourceSensitivity |-> RequestSourceSensitivity(req),
  deploymentClass |-> RequestDeploymentClass(req),
  enforcementPoint |-> RequestEnforcementPoint(req),
  obligations |-> RequiredObligations(req, EffectFor(bundle, req)),
  hasRedaction |-> req \in RedactionEvidence,
  hasDeclassification |-> req \in DeclassificationEvidence,
  identityProvider |-> RequestProviderOrNone(req),
  claimEnvelope |-> RequestClaimOrNone(req),
  subjectBinding |-> MatchedBindingOrNone(req),
  permissionGrant |-> MatchedGrantOrNone(req),
  inputHash |-> req,
  matchedRule |-> RuleFor(bundle, req),
  tracePrivacySafe |-> req \notin TracePayloadLeakRequests
]

RecordedDecisionCount ==
  Cardinality({req \in Requests: decisions[req].recorded})

StateConstraint ==
  RecordedDecisionCount <= 3

Init ==
  /\ policyGraphs = {}
  /\ configGraphs = {}
  /\ compiledBundles = {}
  /\ compiledConfigSets = {}
  /\ activeBundle = NoBundle
  /\ activeConfig = [s \in Scopes |-> NoConfig]
  /\ configActivationDecisions = [s \in Scopes |-> NoRequest]
  /\ identityProviders = IdentityProviders
  /\ externalClaimEnvelopes = ExternalClaims
  /\ subjectBindings = {}
  /\ permissionGrants = {}
  /\ postgresPolicyState = [s \in Scopes |-> DefaultPostgresPolicyState]
  /\ decisions = [r \in Requests |-> DefaultDecision]
  /\ visibleState = {}
  /\ quarantineState = {}
  /\ publicOutputs = {}
  /\ publishedGenerations = {}
  /\ generationPolicyPins = [g \in Generations |-> NoBundle]
  /\ pinnedAtPublish = [g \in Generations |-> NoBundle]
  /\ persistedSecretEvidence = {}

AdmitPolicyGraph(pg) ==
  /\ pg \in PolicyGraphs
  /\ pg \notin policyGraphs
  /\ policyGraphs' = policyGraphs \cup {pg}
  /\ UNCHANGED <<
       configGraphs, compiledBundles, compiledConfigSets, activeBundle,
       activeConfig, configActivationDecisions, identityProviders,
       externalClaimEnvelopes, subjectBindings, permissionGrants,
       postgresPolicyState, decisions, visibleState,
       quarantineState, publicOutputs, publishedGenerations,
       generationPolicyPins, pinnedAtPublish, persistedSecretEvidence
     >>

AdmitConfigGraph(cg) ==
  /\ cg \in ConfigGraphs
  /\ cg \notin configGraphs
  /\ configGraphs' = configGraphs \cup {cg}
  /\ UNCHANGED <<
       policyGraphs, compiledBundles, compiledConfigSets, activeBundle,
       activeConfig, configActivationDecisions, identityProviders,
       externalClaimEnvelopes, subjectBindings, permissionGrants,
       postgresPolicyState, decisions, visibleState,
       quarantineState, publicOutputs, publishedGenerations,
       generationPolicyPins, pinnedAtPublish, persistedSecretEvidence
     >>

CompilePolicyGraph(pg, bundle) ==
  /\ pg \in policyGraphs
  /\ bundle \in Bundles
  /\ bundle \notin compiledBundles
  /\ compiledBundles' = compiledBundles \cup {bundle}
  /\ UNCHANGED <<
       policyGraphs, configGraphs, compiledConfigSets, activeBundle,
       activeConfig, configActivationDecisions, identityProviders,
       externalClaimEnvelopes, subjectBindings, permissionGrants,
       postgresPolicyState, decisions, visibleState,
       quarantineState, publicOutputs, publishedGenerations,
       generationPolicyPins, pinnedAtPublish, persistedSecretEvidence
     >>

CompileConfigGraph(cg, config) ==
  /\ cg \in configGraphs
  /\ config \in ConfigSets
  /\ config \notin compiledConfigSets
  /\ ConfigPolicy(config) \in compiledBundles
  /\ compiledConfigSets' = compiledConfigSets \cup {config}
  /\ UNCHANGED <<
       policyGraphs, configGraphs, compiledBundles, activeBundle,
       activeConfig, configActivationDecisions, identityProviders,
       externalClaimEnvelopes, subjectBindings, permissionGrants,
       postgresPolicyState, decisions, visibleState,
       quarantineState, publicOutputs, publishedGenerations,
       generationPolicyPins, pinnedAtPublish, persistedSecretEvidence
     >>

ActivateBundle(bundle) ==
  /\ bundle \in compiledBundles
  /\ activeBundle = NoBundle
  /\ activeBundle' = bundle
  /\ activeConfig' = [s \in Scopes |-> NoConfig]
  /\ configActivationDecisions' = [s \in Scopes |-> NoRequest]
  /\ postgresPolicyState' = [s \in Scopes |-> DefaultPostgresPolicyState]
  /\ UNCHANGED <<
       policyGraphs, configGraphs, compiledBundles, compiledConfigSets,
       identityProviders, externalClaimEnvelopes, subjectBindings,
       permissionGrants, decisions, visibleState, quarantineState,
       publicOutputs, publishedGenerations,
       generationPolicyPins, pinnedAtPublish, persistedSecretEvidence
     >>

ActivateConfig(config, req) ==
  LET scope == ConfigScope(config) IN
  /\ config \in compiledConfigSets
  /\ req \in ConfigChangeRequests
  /\ decisions[req].recorded
  /\ decisions[req].effect = "allow"
  /\ req \in AuthorizedRequests
  /\ activeBundle # NoBundle
  /\ ConfigPolicy(config) = activeBundle
  /\ activeConfig' = [activeConfig EXCEPT ![scope] = config]
  /\ configActivationDecisions' = [
       configActivationDecisions EXCEPT ![scope] = req
     ]
  /\ postgresPolicyState' = [
       postgresPolicyState EXCEPT ![scope] = [
         bundle |-> activeBundle,
         config |-> config
       ]
     ]
  /\ UNCHANGED <<
       policyGraphs, configGraphs, compiledBundles, compiledConfigSets,
       activeBundle, identityProviders, externalClaimEnvelopes,
       subjectBindings, permissionGrants, decisions, visibleState,
       quarantineState, publicOutputs,
       publishedGenerations, generationPolicyPins, pinnedAtPublish, persistedSecretEvidence
     >>

RegisterIdentityProvider(provider, req) ==
  /\ provider \in IdentityProviders
  /\ req \in ConfigChangeRequests
  /\ decisions[req].recorded
  /\ decisions[req].effect = "allow"
  /\ req \in AuthorizedRequests
  /\ identityProviders' = identityProviders \cup {provider}
  /\ UNCHANGED <<
       policyGraphs, configGraphs, compiledBundles, compiledConfigSets,
       activeBundle, activeConfig, configActivationDecisions,
       externalClaimEnvelopes, subjectBindings, permissionGrants,
       postgresPolicyState, decisions, visibleState, quarantineState,
       publicOutputs, publishedGenerations, generationPolicyPins,
       pinnedAtPublish, persistedSecretEvidence
     >>

AcceptExternalClaim(claim) ==
  /\ claim \in ExternalClaims
  /\ claim \notin externalClaimEnvelopes
  /\ externalClaimEnvelopes' = externalClaimEnvelopes \cup {claim}
  /\ UNCHANGED <<
       policyGraphs, configGraphs, compiledBundles, compiledConfigSets,
       activeBundle, activeConfig, configActivationDecisions,
       identityProviders, subjectBindings, permissionGrants,
       postgresPolicyState, decisions, visibleState, quarantineState,
       publicOutputs, publishedGenerations, generationPolicyPins,
       pinnedAtPublish, persistedSecretEvidence
     >>

BindSubject(binding, req) ==
  /\ binding \in SubjectBindings
  /\ req \in ConfigChangeRequests
  /\ decisions[req].recorded
  /\ decisions[req].effect = "allow"
  /\ req \in AuthorizedRequests
  /\ subjectBindings' = subjectBindings \cup {binding}
  /\ UNCHANGED <<
       policyGraphs, configGraphs, compiledBundles, compiledConfigSets,
       activeBundle, activeConfig, configActivationDecisions,
       identityProviders, externalClaimEnvelopes, permissionGrants,
       postgresPolicyState, decisions, visibleState,
       quarantineState, publicOutputs, publishedGenerations,
       generationPolicyPins, pinnedAtPublish, persistedSecretEvidence
     >>

InstallPermissionGrant(grant, req) ==
  /\ grant \in PermissionGrants
  /\ req \in ConfigChangeRequests
  /\ decisions[req].recorded
  /\ decisions[req].effect = "allow"
  /\ req \in AuthorizedRequests
  /\ permissionGrants' = permissionGrants \cup {grant}
  /\ UNCHANGED <<
       policyGraphs, configGraphs, compiledBundles, compiledConfigSets,
       activeBundle, activeConfig, configActivationDecisions,
       identityProviders, externalClaimEnvelopes, subjectBindings,
       postgresPolicyState, decisions, visibleState,
       quarantineState, publicOutputs, publishedGenerations,
       generationPolicyPins, pinnedAtPublish, persistedSecretEvidence
     >>

EvaluateRequest(req) ==
  /\ req \in Requests
  /\ ~decisions[req].recorded
  /\ decisions' = [
       decisions EXCEPT ![req] = DecisionRecord(req, EffectiveBundleForRequest(req))
     ]
  /\ UNCHANGED <<
       policyGraphs, configGraphs, compiledBundles, compiledConfigSets,
       activeBundle, activeConfig, configActivationDecisions,
       identityProviders, externalClaimEnvelopes, subjectBindings,
       permissionGrants, postgresPolicyState,
       visibleState, quarantineState, publicOutputs,
       publishedGenerations, generationPolicyPins, pinnedAtPublish, persistedSecretEvidence
     >>

EnforceMutation(req) ==
  /\ req \in MutationRequests \cup PorosRequests
  /\ req \notin visibleState
  /\ decisions[req].recorded
  /\ decisions[req].effect = "allow"
  /\ visibleState' = visibleState \cup {req}
  /\ UNCHANGED <<
       policyGraphs, configGraphs, compiledBundles, compiledConfigSets,
       activeBundle, activeConfig, configActivationDecisions,
       identityProviders, externalClaimEnvelopes, subjectBindings,
       permissionGrants, postgresPolicyState, decisions, quarantineState,
       publicOutputs, publishedGenerations,
       generationPolicyPins, pinnedAtPublish, persistedSecretEvidence
     >>

RouteToQuarantine(req) ==
  /\ req \in Requests
  /\ req \notin quarantineState
  /\ decisions[req].recorded
  /\ decisions[req].effect \in {"review", "quarantine"}
  /\ quarantineState' = quarantineState \cup {req}
  /\ UNCHANGED <<
       policyGraphs, configGraphs, compiledBundles, compiledConfigSets,
       activeBundle, activeConfig, configActivationDecisions,
       identityProviders, externalClaimEnvelopes, subjectBindings,
       permissionGrants, postgresPolicyState, decisions, visibleState,
       publicOutputs, publishedGenerations,
       generationPolicyPins, pinnedAtPublish, persistedSecretEvidence
     >>

ReadOrExport(req) ==
  /\ req \in PublicSurfaceRequests
  /\ req \notin publicOutputs
  /\ decisions[req].recorded
  /\ decisions[req].effect \in {"allow", "redact", "declassify"}
  /\ publicOutputs' = publicOutputs \cup {req}
  /\ UNCHANGED <<
       policyGraphs, configGraphs, compiledBundles, compiledConfigSets,
       activeBundle, activeConfig, configActivationDecisions,
       identityProviders, externalClaimEnvelopes, subjectBindings,
       permissionGrants, postgresPolicyState, decisions, visibleState,
       quarantineState, publishedGenerations,
       generationPolicyPins, pinnedAtPublish, persistedSecretEvidence
     >>

\* Publishing pins the bundle that was active at publish time. The ghost
\* variable pinnedAtPublish freezes that bundle so
\* GenerationPinMatchesPublishBundle can distinguish WHICH bundle a
\* generation was published under -- meaningful only because the bundle
\* universe is >= 2 and RotateBundle can change activeBundle between
\* publishes. BuggyPublishPinsStaleBundle stamps some OTHER compiled bundle
\* instead of the active one (the stale-pin hazard).
PublishGeneration(g) ==
  /\ g \in Generations
  /\ g \notin publishedGenerations
  /\ activeBundle # NoBundle
  /\ publishedGenerations' = publishedGenerations \cup {g}
  /\ pinnedAtPublish' = [pinnedAtPublish EXCEPT ![g] = activeBundle]
  /\ generationPolicyPins' = [
       generationPolicyPins EXCEPT ![g] =
         IF BuggyPublishPinsStaleBundle /\ Bundles \ {activeBundle} # {}
         THEN CHOOSE b \in Bundles \ {activeBundle}: TRUE
         ELSE activeBundle
     ]
  /\ UNCHANGED <<
       policyGraphs, configGraphs, compiledBundles, compiledConfigSets,
       activeBundle, activeConfig, configActivationDecisions,
       identityProviders, externalClaimEnvelopes, subjectBindings,
       permissionGrants, postgresPolicyState, decisions, visibleState,
       quarantineState, publicOutputs, persistedSecretEvidence
     >>

\* Policy-bundle rotation: a second compiled bundle replaces the active one.
\* Rotation resets per-scope config activation (a new bundle requires fresh
\* config activation), mirroring ActivateBundle's reset semantics. Pins of
\* already-published generations MUST NOT move; BuggyRotationRestampsPins
\* rewrites them to the new bundle (the restamp hazard), which
\* GenerationPinMatchesPublishBundle catches against the frozen ghost.
RotateBundle(bundle) ==
  /\ bundle \in compiledBundles
  /\ activeBundle # NoBundle
  /\ bundle # activeBundle
  /\ activeBundle' = bundle
  /\ activeConfig' = [s \in Scopes |-> NoConfig]
  /\ configActivationDecisions' = [s \in Scopes |-> NoRequest]
  /\ postgresPolicyState' = [s \in Scopes |-> DefaultPostgresPolicyState]
  /\ generationPolicyPins' =
       IF BuggyRotationRestampsPins
       THEN [g \in Generations |->
              IF g \in publishedGenerations
              THEN bundle
              ELSE generationPolicyPins[g]]
       ELSE generationPolicyPins
  /\ UNCHANGED <<
       policyGraphs, configGraphs, compiledBundles, compiledConfigSets,
       identityProviders, externalClaimEnvelopes, subjectBindings,
       permissionGrants, decisions, visibleState, quarantineState,
       publicOutputs, publishedGenerations, pinnedAtPublish,
       persistedSecretEvidence
     >>

\* Models a privacy-regression bug: a claim-acceptance path that durably
\* persists the secret evidence carried by an external claim. This action is
\* disabled on the safe path (BuggySecretPersistence = FALSE), so
\* persistedSecretEvidence stays empty and ClaimEvidenceDoesNotPersistSecret
\* holds. When the flag is TRUE and the input set names a secret-bearing
\* claim, the action is enabled and the invariant is genuinely violable.
PersistSecretEvidenceBug(claim) ==
  /\ BuggySecretPersistence
  /\ claim \in PersistedSecretClaimEvidence
  /\ claim \notin persistedSecretEvidence
  /\ persistedSecretEvidence' = persistedSecretEvidence \cup {claim}
  /\ UNCHANGED <<
       policyGraphs, configGraphs, compiledBundles, compiledConfigSets,
       activeBundle, activeConfig, configActivationDecisions,
       identityProviders, externalClaimEnvelopes, subjectBindings,
       permissionGrants, postgresPolicyState, decisions, visibleState,
       quarantineState, publicOutputs, publishedGenerations,
       generationPolicyPins, pinnedAtPublish
     >>

Next ==
  \/ \E pg \in PolicyGraphs: AdmitPolicyGraph(pg)
  \/ \E cg \in ConfigGraphs: AdmitConfigGraph(cg)
  \/ \E pg \in PolicyGraphs, bundle \in Bundles:
       CompilePolicyGraph(pg, bundle)
  \/ \E cg \in ConfigGraphs, config \in ConfigSets:
       CompileConfigGraph(cg, config)
  \/ \E bundle \in Bundles: ActivateBundle(bundle)
  \/ \E bundle \in Bundles: RotateBundle(bundle)
  \/ \E config \in ConfigSets, req \in Requests:
       ActivateConfig(config, req)
  \/ \E binding \in SubjectBindings, req \in Requests:
       BindSubject(binding, req)
  \/ \E grant \in PermissionGrants, req \in Requests:
       InstallPermissionGrant(grant, req)
  \/ \E req \in Requests: EvaluateRequest(req)
  \/ \E req \in Requests: EnforceMutation(req)
  \/ \E req \in Requests: RouteToQuarantine(req)
  \/ \E req \in Requests: ReadOrExport(req)
  \/ \E g \in Generations: PublishGeneration(g)
  \/ \E claim \in PersistedSecretClaimEvidence: PersistSecretEvidenceBug(claim)

Spec ==
  Init /\
  [][Next]_<<
    policyGraphs, configGraphs, compiledBundles, compiledConfigSets,
    activeBundle, activeConfig, configActivationDecisions,
    identityProviders, externalClaimEnvelopes, subjectBindings,
    permissionGrants, postgresPolicyState, decisions, visibleState,
    quarantineState, publicOutputs,
    publishedGenerations, generationPolicyPins, pinnedAtPublish, persistedSecretEvidence
  >>

ConstantSetsOK ==
  /\ NoBundle \notin Bundles
  /\ NoConfig \notin ConfigSets
  /\ NoRequest \notin Requests
  /\ NoProvider \notin IdentityProviders
  /\ NoClaim \notin ExternalClaims
  /\ NoBinding \notin SubjectBindings
  /\ NoGrant \notin PermissionGrants
  /\ DefaultScope \in Scopes
  /\ SourceSubject \in Subjects
  /\ OkgSubject \in Subjects
  /\ AgentSubject \in Subjects
  /\ AdminSubject \in Subjects
  /\ ReaderSubject \in Subjects
  /\ OperatorSubject \in Subjects
  /\ ScopedClaims \subseteq ExternalClaims
  /\ ExpiredClaims \subseteq ExternalClaims
  /\ UnscopedClaims \subseteq ExternalClaims
  /\ ActiveSubjectBindings \subseteq SubjectBindings
  /\ TrustedProviderBindings \subseteq SubjectBindings
  /\ ScopedClaimBindings \subseteq SubjectBindings
  /\ ReaderSubjectBindings \subseteq SubjectBindings
  /\ OperatorSubjectBindings \subseteq SubjectBindings
  /\ DefaultScopeBindings \subseteq SubjectBindings
  /\ ActivePermissionGrants \subseteq PermissionGrants
  /\ ReaderSubjectGrants \subseteq PermissionGrants
  /\ OperatorSubjectGrants \subseteq PermissionGrants
  /\ DefaultScopeGrants \subseteq PermissionGrants
  /\ GraphReadMcpGrants \subseteq PermissionGrants
  /\ McpRecordGrants \subseteq PermissionGrants
  /\ PorosSubmitGrants \subseteq PermissionGrants
  /\ PublicDataGrants \subseteq PermissionGrants
  /\ InternalDataGrants \subseteq PermissionGrants
  /\ AllowGrants \subseteq PermissionGrants
  /\ PublicRequests \subseteq Requests
  /\ RestrictedRequests \subseteq Requests
  /\ MutationRequests \subseteq Requests
  /\ PorosRequests \subseteq Requests
  /\ ReadRequests \subseteq Requests
  /\ ExportRequests \subseteq Requests
  /\ UnknownLicenseRequests \subseteq Requests
  /\ PrivateExportRequests \subseteq ExportRequests
  /\ ConfigChangeRequests \subseteq Requests
  /\ PostgresRequests \subseteq Requests
  /\ ExternalClaimRequests \subseteq Requests
  /\ TrustedClaimRequests \subseteq ExternalClaimRequests
  /\ UntrustedClaimRequests \subseteq ExternalClaimRequests
  /\ ScopedClaimRequests \subseteq ExternalClaimRequests
  /\ ExpiredClaimRequests \subseteq ExternalClaimRequests
  /\ AudienceMismatchRequests \subseteq ExternalClaimRequests
  /\ MissingAssuranceRequests \subseteq ExternalClaimRequests
  /\ AmbiguousBindingRequests \subseteq ExternalClaimRequests
  /\ RevokedProviderRequests \subseteq ExternalClaimRequests
  /\ RevokedKerberosServicePrincipalRequests \subseteq ExternalClaimRequests
  /\ UnverifiedClaimRequests \subseteq ExternalClaimRequests
  /\ PersistedSecretClaimEvidence \subseteq ExternalClaims
  /\ BuggySecretPersistence \in BOOLEAN
  /\ BuggySkippedClaimChecks \subseteq ClaimCheckModes
  /\ BuggyPublishPinsStaleBundle \in BOOLEAN
  /\ BuggyRotationRestampsPins \in BOOLEAN
  /\ AuthorizedRequests \subseteq Requests
  /\ UnauthorizedRequests \subseteq Requests
  /\ RedactionEvidence \subseteq Requests
  /\ DeclassificationEvidence \subseteq Requests
  /\ ProfileHintRequests \subseteq Requests
  /\ AgentSelfExpandRequests \subseteq Requests
  /\ MissingPolicyRequests \subseteq Requests
  /\ WeakDeploymentPostureRequests \subseteq Requests
  /\ SourceDeploymentMismatchRequests \subseteq Requests
  /\ SourceLineageRequests \subseteq Requests
  /\ MixedSourceLineageRequests \subseteq SourceLineageRequests
  /\ PartiallyAuthorizedSourceLineageRequests \subseteq SourceLineageRequests
  /\ MissingSourceLineageRequests \subseteq SourceLineageRequests
  /\ MalformedSourceLineageRequests \subseteq SourceLineageRequests
  /\ UnknownSourceLineageRequests \subseteq SourceLineageRequests
  /\ StaleSourcePolicyRequests \subseteq SourceLineageRequests
  /\ FineGrainedAccessRequests \subseteq Requests
  /\ FineGrainedAuthorizedRequests \subseteq FineGrainedAccessRequests
  /\ FineGrainedUnaddressableRequests \subseteq FineGrainedAccessRequests
  /\ FineGrainedFutureHorizonRequests \subseteq FineGrainedAccessRequests
  /\ FineGrainedGraphDeniedRequests \subseteq FineGrainedAccessRequests
  /\ FineGrainedDefaultAllowRequests \subseteq FineGrainedAccessRequests
  /\ UnknownActionRequests \subseteq Requests
  /\ NoExportRequests \subseteq Requests
  /\ SecretUseRequests \subseteq Requests
  /\ TracePayloadLeakRequests \subseteq Requests

TypeOK ==
  /\ policyGraphs \subseteq PolicyGraphs
  /\ configGraphs \subseteq ConfigGraphs
  /\ compiledBundles \subseteq Bundles
  /\ compiledConfigSets \subseteq ConfigSets
  /\ activeBundle \in Bundles \cup {NoBundle}
  /\ activeConfig \in [Scopes -> ConfigSets \cup {NoConfig}]
  /\ configActivationDecisions \in [Scopes -> Requests \cup {NoRequest}]
  /\ identityProviders \subseteq IdentityProviders
  /\ externalClaimEnvelopes \subseteq ExternalClaims
  /\ subjectBindings \subseteq SubjectBindings
  /\ permissionGrants \subseteq PermissionGrants
  /\ postgresPolicyState \in [
       Scopes -> [
         bundle: Bundles \cup {NoBundle},
         config: ConfigSets \cup {NoConfig}
       ]
     ]
  /\ decisions \in [
       Requests -> [
         recorded: BOOLEAN,
         effect: Effects \cup {"none"},
         bundle: Bundles \cup {NoBundle},
         postgresConfig: ConfigSets \cup {NoConfig},
         configPin: ConfigSets \cup {NoConfig},
         policyPin: Bundles \cup {NoBundle},
         action: Actions \cup {UnknownAction, "none"},
         dataClassification: DataClassifications \cup {"none"},
         sourceSensitivity: SourceSensitivities \cup {"none"},
         deploymentClass: DeploymentClasses \cup {"none"},
         enforcementPoint: EnforcementPoints \cup {"none"},
         obligations: SUBSET Obligations,
         hasRedaction: BOOLEAN,
         hasDeclassification: BOOLEAN,
         identityProvider: IdentityProviders \cup {NoProvider},
         claimEnvelope: ExternalClaims \cup {NoClaim},
         subjectBinding: SubjectBindings \cup {NoBinding},
         permissionGrant: PermissionGrants \cup {NoGrant},
         inputHash: Requests \cup {"none"},
         matchedRule: RuleIds,
         tracePrivacySafe: BOOLEAN
       ]
     ]
  /\ visibleState \subseteq Requests
  /\ quarantineState \subseteq Requests
  /\ publicOutputs \subseteq Requests
  /\ publishedGenerations \subseteq Generations
  /\ generationPolicyPins \in [Generations -> Bundles \cup {NoBundle}]
  /\ pinnedAtPublish \in [Generations -> Bundles \cup {NoBundle}]
  /\ persistedSecretEvidence \subseteq ExternalClaims

DefaultDeny ==
  \A req \in Requests:
    /\ decisions[req].recorded
    /\ decisions[req].bundle = NoBundle
    => decisions[req].effect = "deny"

NoVisibleWithoutAllow ==
  \A req \in visibleState:
    /\ decisions[req].recorded
    /\ decisions[req].effect = "allow"

QuarantineNotVisible ==
  quarantineState \cap visibleState = {}

NoPrivatePublicLeak ==
  \A req \in publicOutputs:
    req \in RestrictedRequests =>
      /\ decisions[req].recorded
      /\ decisions[req].effect \in {"redact", "declassify"}
      /\ decisions[req].hasRedaction \/ decisions[req].hasDeclassification

UnknownLicenseCannotPublicOutput ==
  \A req \in UnknownLicenseRequests:
    /\ req \in PublicSurfaceRequests
    /\ decisions[req].recorded
    => /\ decisions[req].effect = "deny"
       /\ req \notin publicOutputs

PrivateExportRequiresScopedClaim ==
  \A req \in PrivateExportRequests:
    /\ decisions[req].recorded
    /\ req \notin ScopedClaimRequests
    => /\ decisions[req].effect # "allow"
       /\ req \notin publicOutputs

MissingPolicyCannotAllow ==
  \A req \in MissingPolicyRequests:
    /\ decisions[req].recorded
    /\ decisions[req].bundle = NoBundle
    => /\ decisions[req].effect = "deny"
       /\ req \notin visibleState
       /\ req \notin publicOutputs

UnknownActionCannotAllow ==
  \A req \in UnknownActionRequests:
    decisions[req].recorded =>
      /\ decisions[req].action = UnknownAction
      /\ decisions[req].effect = "deny"
      /\ req \notin visibleState
      /\ req \notin publicOutputs

WeakDeploymentPostureCannotAllow ==
  \A req \in WeakDeploymentPostureRequests:
    decisions[req].recorded =>
      /\ decisions[req].deploymentClass = "public_reference_demo"
      /\ decisions[req].dataClassification \in {
           "confidential",
           "private",
           "regulated_metadata",
           "restricted",
           "secret",
           "synthetic_phi"
         }
      /\ decisions[req].effect # "allow"
      /\ req \notin visibleState
      /\ req \notin publicOutputs

SourceDeploymentMismatchCannotAllow ==
  \A req \in SourceDeploymentMismatchRequests:
    decisions[req].recorded =>
      /\ ~SourceDeploymentCompatible(req)
      /\ decisions[req].effect # "allow"
      /\ req \notin visibleState
      /\ req \notin publicOutputs

NoExportObligationBlocksPublicOutput ==
  \A req \in NoExportRequests:
    decisions[req].recorded =>
      /\ "no_export" \in decisions[req].obligations
      /\ req \notin publicOutputs

DecisionObligationsComplete ==
  \A req \in Requests:
    decisions[req].recorded =>
      RequiredObligations(req, decisions[req].effect) \subseteq
        decisions[req].obligations

DecisionTraceVocabularyComplete ==
  \A req \in Requests:
    IF decisions[req].recorded
    THEN
      /\ decisions[req].inputHash = req
      /\ decisions[req].policyPin = decisions[req].bundle
      /\ decisions[req].configPin \in ConfigSets \cup {NoConfig}
      /\ IF req \in PostgresRequests /\ decisions[req].bundle # NoBundle
         THEN decisions[req].configPin = decisions[req].postgresConfig
         ELSE TRUE
      /\ decisions[req].dataClassification =
           RequestDataClassification(req)
      /\ decisions[req].sourceSensitivity =
           RequestSourceSensitivity(req)
      /\ decisions[req].deploymentClass = RequestDeploymentClass(req)
      /\ decisions[req].enforcementPoint = RequestEnforcementPoint(req)
      /\ decisions[req].obligations # {}
      /\ decisions[req].identityProvider = RequestProviderOrNone(req)
      /\ decisions[req].claimEnvelope = RequestClaimOrNone(req)
      /\ decisions[req].tracePrivacySafe
    ELSE TRUE

GenerationPolicyPinned ==
  \A g \in publishedGenerations:
    generationPolicyPins[g] # NoBundle

\* The pin recorded for a published generation is exactly the bundle that was
\* active when it published (frozen in the pinnedAtPublish ghost), and it
\* never moves afterwards. With Bundles >= 2 and RotateBundle in the machine,
\* this distinguishes WHICH bundle was pinned: a publish that stamps a stale
\* bundle (BuggyPublishPinsStaleBundle) or a rotation that restamps history
\* (BuggyRotationRestampsPins) each violate it by name.
GenerationPinMatchesPublishBundle ==
  \A g \in publishedGenerations:
    /\ pinnedAtPublish[g] # NoBundle
    /\ generationPolicyPins[g] = pinnedAtPublish[g]

DecisionReplayDeterministic ==
  \A req \in Requests:
    decisions[req].recorded =>
      /\ decisions[req].effect =
           EffectFor(decisions[req].bundle, req)
      /\ decisions[req].matchedRule =
           RuleFor(decisions[req].bundle, req)
      /\ decisions[req].obligations =
           RequiredObligations(req, decisions[req].effect)

ConfigActivationAtomic ==
  \A scope \in Scopes:
    IF activeConfig[scope] = NoConfig
    THEN /\ configActivationDecisions[scope] = NoRequest
         /\ postgresPolicyState[scope] = DefaultPostgresPolicyState
    ELSE
      /\ activeBundle # NoBundle
      /\ activeConfig[scope] \in compiledConfigSets
      /\ ConfigPolicy(activeConfig[scope]) = activeBundle
      /\ ConfigScope(activeConfig[scope]) = scope
      /\ configActivationDecisions[scope] \in ConfigChangeRequests
      /\ decisions[configActivationDecisions[scope]].recorded
      /\ decisions[configActivationDecisions[scope]].effect = "allow"
      /\ postgresPolicyState[scope].bundle = activeBundle
      /\ postgresPolicyState[scope].config = activeConfig[scope]

NoUnauthorizedConfigChange ==
  \A scope \in Scopes:
    configActivationDecisions[scope] # NoRequest =>
      /\ configActivationDecisions[scope] \in AuthorizedRequests
      /\ decisions[configActivationDecisions[scope]].recorded
      /\ decisions[configActivationDecisions[scope]].effect = "allow"

PostgresEnforcementUsesActiveBundle ==
  \A req \in PostgresRequests:
    decisions[req].recorded =>
      IF decisions[req].bundle = NoBundle
      THEN /\ decisions[req].effect = "deny"
           /\ decisions[req].postgresConfig = NoConfig
      ELSE
        /\ decisions[req].bundle \in Bundles
        /\ decisions[req].postgresConfig \in ConfigSets
        /\ ConfigPolicy(decisions[req].postgresConfig) =
             decisions[req].bundle
        /\ ConfigScope(decisions[req].postgresConfig) =
             RequestScope(req)
        /\ SubjectBoundFor(req)
        /\ GrantAllows(req)
        /\ decisions[req].subjectBinding \in MatchingBindings(req)
        /\ decisions[req].permissionGrant \in MatchingGrants(req)

UntrustedProviderCannotGrant ==
  \A req \in UntrustedClaimRequests:
    decisions[req].recorded =>
      /\ decisions[req].effect # "allow"
      /\ req \notin visibleState
      /\ req \notin publicOutputs

ExpiredClaimCannotGrant ==
  \A req \in ExpiredClaimRequests:
    decisions[req].recorded =>
      /\ decisions[req].effect # "allow"
      /\ req \notin visibleState
      /\ req \notin publicOutputs

AudienceMismatchCannotGrant ==
  \A req \in AudienceMismatchRequests:
    decisions[req].recorded =>
      /\ decisions[req].effect # "allow"
      /\ req \notin visibleState
      /\ req \notin publicOutputs

MissingAssuranceCannotGrant ==
  \A req \in MissingAssuranceRequests:
    decisions[req].recorded =>
      /\ decisions[req].effect # "allow"
      /\ req \notin visibleState
      /\ req \notin publicOutputs

AmbiguousBindingCannotGrant ==
  \A req \in AmbiguousBindingRequests:
    decisions[req].recorded =>
      /\ decisions[req].effect # "allow"
      /\ req \notin visibleState
      /\ req \notin publicOutputs

RevokedProviderCannotGrant ==
  \A req \in RevokedProviderRequests:
    decisions[req].recorded =>
      /\ decisions[req].effect # "allow"
      /\ req \notin visibleState
      /\ req \notin publicOutputs

RevokedKerberosServicePrincipalCannotGrant ==
  \A req \in RevokedKerberosServicePrincipalRequests:
    decisions[req].recorded =>
      /\ decisions[req].effect # "allow"
      /\ req \notin visibleState
      /\ req \notin publicOutputs

\* Issue #1007: a claim carrying no verification evidence must not grant,
\* even when every other check it can be measured against passes.
UnverifiedClaimCannotGrant ==
  \A req \in UnverifiedClaimRequests:
    decisions[req].recorded =>
      /\ decisions[req].effect # "allow"
      /\ req \notin visibleState
      /\ req \notin publicOutputs

ExternalRolesAreScoped ==
  \A req \in ExternalClaimRequests:
    /\ req \notin ScopedClaimRequests
    /\ decisions[req].recorded
    => /\ decisions[req].effect # "allow"
       /\ req \notin visibleState
       /\ req \notin publicOutputs

GraphProfileCannotBroaden ==
  \A req \in ProfileHintRequests:
    /\ req \notin AuthorizedRequests
    /\ decisions[req].recorded
    => /\ decisions[req].effect # "allow"
       /\ req \notin visibleState

AgentCannotSelfExpand ==
  \A req \in AgentSelfExpandRequests:
    decisions[req].recorded =>
      decisions[req].effect \in {"review", "deny"}

ClaimEvidenceDoesNotPersistSecret ==
  persistedSecretEvidence = {}

DecisionTraceCitesProviderAndBinding ==
  \A req \in ExternalClaimRequests:
    /\ decisions[req].recorded
    /\ decisions[req].effect = "allow"
    => /\ req \in TrustedClaimRequests
       /\ req \in ScopedClaimRequests
       /\ decisions[req].identityProvider \in identityProviders
       /\ decisions[req].claimEnvelope \in externalClaimEnvelopes
       /\ decisions[req].subjectBinding \in subjectBindings
       /\ decisions[req].permissionGrant \in permissionGrants
       /\ BindingMatches(req, decisions[req].subjectBinding)
       /\ GrantMatches(req, decisions[req].permissionGrant)

DecisionTraceComplete ==
  /\ \A req \in visibleState \cup quarantineState \cup publicOutputs:
       decisions[req].recorded
  /\ DecisionTraceVocabularyComplete

NomosSourceLineageVisibility ==
  /\ \A req \in SourceLineageRequests:
       /\ decisions[req].recorded
       /\ ~SourceLineageAuthorized(req)
       => /\ decisions[req].effect \notin {"allow", "redact", "declassify"}
          /\ req \notin visibleState
          /\ req \notin publicOutputs
  /\ \A req \in SourceLineageRequests:
       req \in visibleState =>
         /\ SourceLineageAuthorized(req)
         /\ decisions[req].effect = "allow"
  /\ \A req \in SourceLineageRequests:
       req \in publicOutputs =>
         /\ SourceLineageAuthorized(req)
         /\ decisions[req].effect = "allow"
  /\ \A req \in MixedSourceLineageRequests:
       /\ decisions[req].recorded
       /\ req \in PartiallyAuthorizedSourceLineageRequests
       => /\ decisions[req].effect # "allow"
          /\ req \notin visibleState
          /\ req \notin publicOutputs
  /\ \A req \in StaleSourcePolicyRequests:
       decisions[req].recorded =>
         /\ decisions[req].effect \notin {"allow", "redact", "declassify"}
         /\ req \notin visibleState
         /\ req \notin publicOutputs

NomosFineGrainedVisibility ==
  /\ NomosSourceLineageVisibility
  /\ \A req \in FineGrainedAccessRequests:
       /\ decisions[req].recorded
       /\ ~FineGrainedAccessAuthorized(req)
       => /\ decisions[req].effect \notin {"allow", "redact", "declassify"}
          /\ req \notin visibleState
          /\ req \notin publicOutputs
  /\ \A req \in FineGrainedAccessRequests:
       req \in visibleState =>
         /\ FineGrainedAccessAuthorized(req)
         /\ decisions[req].effect = "allow"
  /\ \A req \in FineGrainedAccessRequests:
       req \in publicOutputs =>
         /\ FineGrainedAccessAuthorized(req)
         /\ decisions[req].effect = "allow"
  /\ \A req \in FineGrainedDefaultAllowRequests:
       decisions[req].recorded =>
         /\ decisions[req].effect # "allow"
         /\ req \notin visibleState
         /\ req \notin publicOutputs

\* Conjunction proven by the focused NomosIamClaims.cfg: the seven
\* institutional claim checks each block their own isolated,
\* otherwise-grantable request, an allowed external request cites a matching
\* provider/binding/grant, and imported roles stay scoped. Each conjunct has a
\* named buggy-toggle witness (BuggySkippedClaimChecks) on the same universe.
NomosIamClaimIsolationContract ==
  /\ ConstantSetsOK
  /\ TypeOK
  /\ DefaultDeny
  /\ NoVisibleWithoutAllow
  /\ PostgresEnforcementUsesActiveBundle
  /\ UntrustedProviderCannotGrant
  /\ ExpiredClaimCannotGrant
  /\ AudienceMismatchCannotGrant
  /\ MissingAssuranceCannotGrant
  /\ AmbiguousBindingCannotGrant
  /\ RevokedProviderCannotGrant
  /\ RevokedKerberosServicePrincipalCannotGrant
  /\ UnverifiedClaimCannotGrant
  /\ ExternalRolesAreScoped
  /\ DecisionTraceCitesProviderAndBinding
  /\ DecisionReplayDeterministic

\* Conjunction proven by the focused NomosPinRotation.cfg over a bundle
\* universe of >= 2 with RotateBundle: a published generation's policy pin is
\* exactly the bundle active at publish time and never restamped by rotation.
NomosPublishPinContract ==
  /\ ConstantSetsOK
  /\ TypeOK
  /\ GenerationPolicyPinned
  /\ GenerationPinMatchesPublishBundle
  /\ ConfigActivationAtomic
  /\ NoUnauthorizedConfigChange
  /\ DecisionReplayDeterministic

NomosControlPlaneContract ==
  /\ ConstantSetsOK
  /\ TypeOK
  /\ DefaultDeny
  /\ NoVisibleWithoutAllow
  /\ QuarantineNotVisible
  /\ NoPrivatePublicLeak
  /\ UnknownLicenseCannotPublicOutput
  /\ PrivateExportRequiresScopedClaim
  /\ MissingPolicyCannotAllow
  /\ UnknownActionCannotAllow
  /\ WeakDeploymentPostureCannotAllow
  /\ SourceDeploymentMismatchCannotAllow
  /\ NoExportObligationBlocksPublicOutput
  /\ DecisionObligationsComplete
  /\ GenerationPolicyPinned
  /\ GenerationPinMatchesPublishBundle
  /\ DecisionReplayDeterministic
  /\ ConfigActivationAtomic
  /\ NoUnauthorizedConfigChange
  /\ PostgresEnforcementUsesActiveBundle
  /\ UntrustedProviderCannotGrant
  /\ ExpiredClaimCannotGrant
  /\ AudienceMismatchCannotGrant
  /\ MissingAssuranceCannotGrant
  /\ AmbiguousBindingCannotGrant
  /\ RevokedProviderCannotGrant
  /\ RevokedKerberosServicePrincipalCannotGrant
  /\ UnverifiedClaimCannotGrant
  /\ ExternalRolesAreScoped
  /\ ClaimEvidenceDoesNotPersistSecret
  /\ DecisionTraceCitesProviderAndBinding
  /\ GraphProfileCannotBroaden
  /\ AgentCannotSelfExpand
  /\ DecisionTraceComplete
  /\ NomosSourceLineageVisibility

====
