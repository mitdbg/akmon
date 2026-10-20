---- MODULE NomosCounterexample ----
EXTENDS Nomos

\* Negative-check module for the Nomos proof gate. The base model
\* denies and quarantines unsafe requests. These intentionally broken
\* invariants claim unsafe requests are allowed, so TLC must produce
\* counterexamples once each request is evaluated.
BrokenNoRestrictedPublicOutput ==
  \A req \in RestrictedRequests:
    req \notin publicOutputs

BrokenRevokedProviderCanGrant ==
  \A req \in RevokedProviderRequests:
    decisions[req].recorded => decisions[req].effect = "allow"

BrokenUnknownLicenseCanPublish ==
  \A req \in UnknownLicenseRequests:
    decisions[req].recorded => decisions[req].effect = "allow"

BrokenMissingPolicyCanAllow ==
  \A req \in MissingPolicyRequests:
    /\ decisions[req].recorded
    /\ decisions[req].bundle = NoBundle
    => decisions[req].effect = "allow"

BrokenWeakDeploymentPostureCanAllow ==
  \A req \in WeakDeploymentPostureRequests:
    /\ decisions[req].recorded
    /\ decisions[req].bundle # NoBundle
    => decisions[req].effect = "allow"

BrokenUnscopedIdentityClaimCanGrant ==
  \A req \in ExternalClaimRequests \ ScopedClaimRequests:
    /\ decisions[req].recorded
    /\ decisions[req].bundle # NoBundle
    => decisions[req].effect = "allow"

BrokenUnknownActionCanAllow ==
  \A req \in UnknownActionRequests:
    /\ decisions[req].recorded
    /\ decisions[req].bundle # NoBundle
    => decisions[req].effect = "allow"

BrokenPrivateExportWithoutScopedClaimCanPublish ==
  \A req \in PrivateExportRequests \ ScopedClaimRequests:
    /\ decisions[req].recorded
    /\ decisions[req].bundle # NoBundle
    => decisions[req].effect = "allow"

BrokenSourceDeploymentMismatchCanAllow ==
  \A req \in SourceDeploymentMismatchRequests:
    /\ decisions[req].recorded
    /\ decisions[req].bundle # NoBundle
    => decisions[req].effect = "allow"

NomosControlPlaneNonVacuity ==
  /\ BrokenMissingPolicyCanAllow
  /\ BrokenWeakDeploymentPostureCanAllow
  /\ BrokenUnscopedIdentityClaimCanGrant
  /\ BrokenUnknownActionCanAllow
  /\ BrokenPrivateExportWithoutScopedClaimCanPublish
  /\ BrokenSourceDeploymentMismatchCanAllow

====
