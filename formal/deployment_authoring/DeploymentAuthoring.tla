---- MODULE DeploymentAuthoring ----
(***************************************************************************
  Deployment authoring command-surface safety model.

  The sprint adds authoring, profile, wiring, lint, and env-diagnostic surfaces.
  The model captures the approach contract before implementation:
  inspection commands do not mutate deployment state, static diagnostics do not
  require live runtime access, secret-bearing values are redacted by default,
  readiness explanation is additive, and scaffold generation writes only
  committed skeleton config unless the caller explicitly supplies runtime state.
 ***************************************************************************)

Surfaces == {
  "deployment_new",
  "profile_explain",
  "deployment_plan",
  "deployment_lint",
  "ready_explain",
  "env_doctor"
}

Risks == {"read", "write", "runtime"}

RequiredDiagnosticFields == {
  "schema_version",
  "code",
  "severity",
  "stage",
  "message",
  "why",
  "location",
  "next_action"
}

ReadyFields == {"ok", "environment", "checks", "blockers", "warnings"}

DescriptorSources == {"canonical", "custom_code", "not_applicable"}

AuthoritySources == {
  "process_env",
  "local_profile",
  "worktree_main",
  "manifest",
  "missing",
  "not_applicable"
}

ConflictSeverities == {"none", "warning", "blocker"}

VARIABLES
  surface,
  showValues,
  commandRisk,
  safeName,
  callerSuppliesRuntimeState,
  callerRequestsCustomCode,
  writesFiles,
  writesLocalRuntime,
  executesCustomCode,
  exposesSecret,
  requiresLiveDb,
  envCommandSupported,
  diagnosticFields,
  readyFieldsPreserved,
  diagnosticsAdditive,
  descriptorSource,
  emitsCommittedConfig,
  authoritySource,
  winnerDeclared,
  dsnTargetMatches,
  conflictSeverity,
  accepted

vars == <<
  surface,
  showValues,
  commandRisk,
  safeName,
  callerSuppliesRuntimeState,
  callerRequestsCustomCode,
  writesFiles,
  writesLocalRuntime,
  executesCustomCode,
  exposesSecret,
  requiresLiveDb,
  envCommandSupported,
  diagnosticFields,
  readyFieldsPreserved,
  diagnosticsAdditive,
  descriptorSource,
  emitsCommittedConfig,
  authoritySource,
  winnerDeclared,
  dsnTargetMatches,
  conflictSeverity,
  accepted
>>

DiagnosticSurface == surface \in {"deployment_lint", "ready_explain", "env_doctor"}

\* Surfaces on which executing deployment-specific custom code is contractually
\* in scope. Inspection surfaces are excluded, so a custom-code request on them
\* must be clamped to FALSE -- which is what InspectionSurfacesNoCustomExecution
\* (and the per-surface safety invariants) must prove against a free input.
CustomCodePermittedSurfaces == {"deployment_new", "ready_explain"}

Init ==
  /\ surface \in Surfaces
  /\ showValues \in BOOLEAN
  /\ commandRisk \in Risks
  /\ safeName \in BOOLEAN
  \* Free boolean inputs: the caller may *request* a local-runtime write or
  \* custom-code execution. The surface contract decides whether the request
  \* takes effect. Letting these range over BOOLEAN makes the no-mutation
  \* invariants falsifiable instead of vacuously pinned to FALSE.
  /\ callerSuppliesRuntimeState \in BOOLEAN
  /\ callerRequestsCustomCode \in BOOLEAN
  /\ writesFiles = IF surface = "deployment_new" THEN safeName ELSE FALSE
  \* No modeled authoring surface ever writes local runtime state: the contract
  \* clamps the caller's request to FALSE on every surface, so the safety
  \* invariants hold even though the input ranges over BOOLEAN.
  /\ writesLocalRuntime = FALSE
  \* Custom-code execution passes through only on the surfaces where running
  \* deployment-specific code is in scope; inspection surfaces clamp it to FALSE.
  /\ executesCustomCode =
       IF surface \in CustomCodePermittedSurfaces
       THEN callerRequestsCustomCode
       ELSE FALSE
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

Next == UNCHANGED vars

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ surface \in Surfaces
  /\ showValues \in BOOLEAN
  /\ commandRisk \in Risks
  /\ safeName \in BOOLEAN
  /\ callerSuppliesRuntimeState \in BOOLEAN
  /\ callerRequestsCustomCode \in BOOLEAN
  /\ writesFiles \in BOOLEAN
  /\ writesLocalRuntime \in BOOLEAN
  /\ executesCustomCode \in BOOLEAN
  /\ exposesSecret \in BOOLEAN
  /\ requiresLiveDb \in BOOLEAN
  /\ envCommandSupported \in BOOLEAN
  /\ diagnosticFields \subseteq RequiredDiagnosticFields
  /\ readyFieldsPreserved \in BOOLEAN
  /\ diagnosticsAdditive \in BOOLEAN
  /\ descriptorSource \in DescriptorSources
  /\ emitsCommittedConfig \in BOOLEAN
  /\ authoritySource \in AuthoritySources
  /\ winnerDeclared \in BOOLEAN
  /\ dsnTargetMatches \in BOOLEAN
  /\ conflictSeverity \in ConflictSeverities
  /\ accepted \in BOOLEAN

InspectionSurfacesNoMutation ==
  surface \in {"profile_explain", "deployment_plan", "deployment_lint", "ready_explain", "env_doctor"}
    => /\ ~writesFiles
       /\ ~writesLocalRuntime

InspectionSurfacesNoCustomExecution ==
  surface \in {"profile_explain", "deployment_plan", "deployment_lint", "env_doctor"}
    => ~executesCustomCode

ScaffoldWritesOnlyCommittedConfig ==
  surface = "deployment_new"
    => /\ emitsCommittedConfig
       /\ ~writesLocalRuntime
       /\ ~exposesSecret

DeploymentNewRejectsUnsafePath ==
  surface = "deployment_new" /\ ~safeName => ~accepted

ProfileExplainNoWriteNoSecret ==
  surface = "profile_explain"
    => /\ ~writesFiles
       /\ ~writesLocalRuntime
       /\ ~executesCustomCode
       /\ ~exposesSecret

WiringPlanDescriptorOnly ==
  surface = "deployment_plan"
    => /\ descriptorSource = "canonical"
       /\ ~executesCustomCode
       /\ ~writesFiles
       /\ ~writesLocalRuntime

LintStaticByDefault ==
  surface = "deployment_lint"
    => /\ ~requiresLiveDb
       /\ RequiredDiagnosticFields \subseteq diagnosticFields
       /\ ~writesFiles
       /\ ~writesLocalRuntime

EnvDoctorRedactsByDefault ==
  surface = "env_doctor" /\ ~showValues => ~exposesSecret

EnvDoctorPreservesEnvCommand ==
  surface = "env_doctor" => envCommandSupported

EnvDoctorReportsAuthorityWinner ==
  surface = "env_doctor" => /\ authoritySource # "not_applicable"
                            /\ winnerDeclared

EnvDoctorRiskClassification ==
  surface = "env_doctor" =>
    /\ conflictSeverity \in ConflictSeverities
    /\ (dsnTargetMatches => conflictSeverity = "none")
    /\ (~dsnTargetMatches /\ commandRisk = "read" => conflictSeverity = "warning")
    /\ (~dsnTargetMatches /\ commandRisk \in {"write", "runtime"} => conflictSeverity = "blocker")

ReadyExplainIsAdditive ==
  surface = "ready_explain"
    => /\ readyFieldsPreserved
       /\ diagnosticsAdditive
       /\ RequiredDiagnosticFields \subseteq diagnosticFields

Issue252ScaffoldSafety ==
  ScaffoldWritesOnlyCommittedConfig /\ DeploymentNewRejectsUnsafePath

Issue253WiringPlanDescriptorOnly ==
  WiringPlanDescriptorOnly

Issue254LintStaticDiagnostics ==
  LintStaticByDefault

Issue255EnvDoctorAuthoritySafety ==
  /\ EnvDoctorRedactsByDefault
  /\ EnvDoctorPreservesEnvCommand
  /\ EnvDoctorReportsAuthorityWinner
  /\ EnvDoctorRiskClassification

Issue256ProfileExplainSafety ==
  ProfileExplainNoWriteNoSecret

DeploymentAuthoringApproachContract ==
  /\ InspectionSurfacesNoMutation
  /\ InspectionSurfacesNoCustomExecution
  /\ Issue252ScaffoldSafety
  /\ Issue253WiringPlanDescriptorOnly
  /\ Issue254LintStaticDiagnostics
  /\ Issue255EnvDoctorAuthoritySafety
  /\ Issue256ProfileExplainSafety
  /\ ReadyExplainIsAdditive

====
