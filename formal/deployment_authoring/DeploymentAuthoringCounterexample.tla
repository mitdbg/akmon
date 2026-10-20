---- MODULE DeploymentAuthoringCounterexample ----
EXTENDS DeploymentAuthoring

(***************************************************************************
  Non-vacuity check for the deployment-authoring no-mutation contract.

  The base model clamps writesLocalRuntime / executesCustomCode through the
  surface contract, so under the real Spec the no-mutation invariants hold.
  This module proves those invariants are NOT vacuous: it relaxes the two
  mutation flags to range freely over BOOLEAN (the unconstrained caller-
  request world), so an inspection surface CAN reach a mutating state. The
  base invariants InspectionSurfacesNoMutation /
  InspectionSurfacesNoCustomExecution then become falsifiable, and the
  asserted broken invariants are refuted -- TLC must print a counterexample.

  If TLC reported "No error has been found" here, it would mean the
  mutation flags were still hard-pinned and the corresponding ~writesLocalRuntime
  / ~executesCustomCode antecedents were unreachable (vacuous).
 ***************************************************************************)

\* Relaxed init: every variable as in the base, except the two mutation flags
\* are free booleans (no surface clamp). Everything else mirrors Init so the
\* model stays well-typed and the rest of the contract is unchanged.
CexInit ==
  /\ surface \in Surfaces
  /\ showValues \in BOOLEAN
  /\ commandRisk \in Risks
  /\ safeName \in BOOLEAN
  /\ callerSuppliesRuntimeState \in BOOLEAN
  /\ callerRequestsCustomCode \in BOOLEAN
  /\ writesFiles = IF surface = "deployment_new" THEN safeName ELSE FALSE
  /\ writesLocalRuntime \in BOOLEAN
  /\ executesCustomCode \in BOOLEAN
  /\ exposesSecret = IF surface = "env_doctor" THEN showValues ELSE FALSE
  /\ requiresLiveDb = IF surface = "ready_explain" THEN TRUE ELSE FALSE
  /\ envCommandSupported = TRUE
  /\ diagnosticFields = (IF DiagnosticSurface THEN RequiredDiagnosticFields ELSE {})
  /\ readyFieldsPreserved = TRUE
  /\ diagnosticsAdditive = TRUE
  /\ descriptorSource =
       IF surface = "deployment_plan"
       THEN "canonical"
       ELSE "not_applicable"
  /\ emitsCommittedConfig = IF surface = "deployment_new" THEN TRUE ELSE FALSE
  /\ authoritySource \in AuthoritySources
  /\ (surface # "env_doctor" => authoritySource = "not_applicable")
  /\ (surface = "env_doctor" => authoritySource \in (AuthoritySources \ {"not_applicable"}))
  /\ winnerDeclared = IF surface = "env_doctor" THEN TRUE ELSE FALSE
  /\ dsnTargetMatches \in BOOLEAN
  /\ conflictSeverity =
       IF surface # "env_doctor"
       THEN "none"
       ELSE IF dsnTargetMatches
       THEN "none"
       ELSE IF commandRisk = "read"
       THEN "warning"
       ELSE "blocker"
  /\ accepted = IF surface = "deployment_new" THEN safeName ELSE TRUE

CexSpec == CexInit /\ [][Next]_vars

\* Intentionally-false safety claim: asserts inspection surfaces never write
\* local runtime state. With writesLocalRuntime free, an inspection surface
\* can reach a TRUE state, so TLC refutes this -- proving the real
\* InspectionSurfacesNoMutation no-write clause is non-vacuous.
BrokenInspectionNoLocalRuntime ==
  surface \in {"profile_explain", "deployment_plan", "deployment_lint", "ready_explain", "env_doctor"}
    => ~writesLocalRuntime

\* Companion claim for the custom-execution clause: inspection surfaces never
\* execute custom code. Refuted once executesCustomCode is free.
BrokenInspectionNoCustomExecution ==
  surface \in {"profile_explain", "deployment_plan", "deployment_lint", "env_doctor"}
    => ~executesCustomCode

====
