# Deployment Authoring Formal Model

## System

The deployment-authoring model covers the command surfaces used to create,
inspect, lint, explain, and diagnose OKG deployment profiles. It focuses on
making inspection commands read-only, keeping static diagnostics free of live
runtime dependencies, redacting secret-bearing values by default, preserving the
existing env command, and ensuring scaffold generation writes only committed
configuration skeletons for safe deployment names.

## Design Contract

- Safety property: deployment authoring commands must not mutate local runtime
  state or execute custom deployment code when they are inspection surfaces;
  scaffold generation must reject unsafe names and emit committed skeleton
  config only; env diagnostics must redact values by default, preserve the env
  command, declare the authority winner, and classify read/write/runtime DSN
  conflicts by risk.
- Authority boundary: `src/okg/substrate/cli/deployment.py` owns command
  dispatch; `src/okg/substrate/deployment_scaffold.py`,
  `src/okg/substrate/deployment_wiring.py`,
  `src/okg/substrate/deployment_lint.py`,
  `src/okg/substrate/deployment_env_doctor.py`, and
  `src/okg/substrate/deployment_profile_explain.py` own the authored payloads
  and diagnostics.
- Valid runtime paths: deployment scaffold creation, profile explanation,
  descriptor-only wiring plans, static lint diagnostics, additive ready
  explanation, env DSN authority resolution, secret redaction, and conflict
  severity classification.
- Out of scope: live DB readiness, deployment-specific source execution,
  filesystem permissions outside the scaffold target, subprocess scheduling, and
  external secret-provider behavior remain implementation-test and deployment
  evidence concerns.
- Preconditions/refinement: No additional preconditions. Runtime tests under
  `tests/substrate/cli/` and `tests/substrate/test_deployment_wiring_plan.py`
  must exercise the same authoring, inspection, redaction, lint, and ready
  explanation surfaces named by the formal checker.

## Model Inventory

| Model | Config | Purpose |
| --- | --- | --- |
| `DeploymentAuthoring.tla` | `DeploymentAuthoring.cfg` | Deployment authoring command-surface safety for scaffolds, profile explanation, wiring plans, static linting, env diagnostics, and additive ready explanation. |
| `DeploymentAuthoringCounterexample.tla` | `DeploymentAuthoringCounterexample.cfg`, `DeploymentAuthoringNoLocalRuntimeWitness.cfg` | Non-vacuity witnesses, one invariant per config (TLC halts at the first violation): each relaxes the mutation flags to free booleans so an inspection surface can reach a custom-execution state (base config) or a local-runtime-writing state (local-runtime witness), proving the no-mutation clauses are load-bearing rather than guarding an unreachable precondition. |

## Transformation Map

| Model | Runtime transformation | Production code | Implementation tests |
| --- | --- | --- | --- |
| `DeploymentAuthoring.tla` | `okg deployment new` accepts only safe names, writes committed deployment config skeletons, and avoids local runtime state or secret exposure. | `src/okg/substrate/cli/deployment.py`, `src/okg/substrate/deployment_scaffold.py` | `tests/substrate/cli/test_deployment_new.py` |
| `DeploymentAuthoring.tla` | Profile explanation is an inspection surface: no writes, no custom code execution, and no secret-bearing values in default output. | `src/okg/substrate/cli/deployment.py`, `src/okg/substrate/deployment_profile_explain.py` | `tests/substrate/cli/test_deployment_profile_explain.py` |
| `DeploymentAuthoring.tla` | Deployment wiring plans are descriptor-only payloads and do not mutate files or execute deployment code. | `src/okg/substrate/cli/deployment.py`, `src/okg/substrate/deployment_wiring.py` | `tests/substrate/test_deployment_wiring_plan.py` |
| `DeploymentAuthoring.tla` | Deployment lint emits static diagnostics with required fields by default and does not require a live DB. | `src/okg/substrate/cli/deployment.py`, `src/okg/substrate/deployment_lint.py` | `tests/substrate/cli/test_deployment_lint.py` |
| `DeploymentAuthoring.tla` | Env doctor redacts by default, preserves the env command, reports the authority winner, and classifies DSN target conflicts by command risk. | `src/okg/substrate/cli/deployment.py`, `src/okg/substrate/deployment_env_doctor.py` | `tests/substrate/cli/test_deployment_env_doctor.py` |
| `DeploymentAuthoring.tla` | Ready explanation adds diagnostics while preserving the original readiness payload shape. | `src/okg/substrate/cli/deployment.py`, `src/okg/substrate/deployment_lint.py` | `tests/substrate/cli/test_deployment_lint.py::test_ready_explain_adapter_is_additive_and_preserves_ready_payload` |
| `DeploymentAuthoringCounterexample.tla` | Under free boolean caller inputs an inspection surface can reach a mutating/custom-execution state, so the base no-mutation invariants are intentionally falsifiable here. | `src/okg/substrate/cli/deployment.py`, `src/okg/substrate/deployment_scaffold.py` | `tests/substrate/cli/test_deployment_new.py` |

## Invariant Map

| Invariants | What it means | Code/test signal |
| --- | --- | --- |
| `TypeOK` | Surfaces, risks, diagnostic fields, authority sources, conflict severities, write flags, and acceptance flags stay inside the modeled domain. | Model setup and strict report producer checks in `tests/pact/test_formal_report_producers.py`. |
| `InspectionSurfacesNoMutation`, `InspectionSurfacesNoCustomExecution` | Profile explain, deployment plan, deployment lint, ready explain, and env doctor inspection paths do not write files/local runtime state or execute custom code. | CLI and command-surface tests in `tests/substrate/cli/test_deployment_profile_explain.py`, `tests/substrate/cli/test_deployment_lint.py`, `tests/substrate/cli/test_deployment_env_doctor.py`, and `tests/substrate/test_deployment_wiring_plan.py`. |
| `ScaffoldWritesOnlyCommittedConfig`, `DeploymentNewRejectsUnsafePath` | Scaffold generation writes committed config skeletons only for safe deployment names, does not touch local runtime state, and does not expose secrets. | `tests/substrate/cli/test_deployment_new.py`. |
| `ProfileExplainNoWriteNoSecret`, `Issue256ProfileExplainSafety` | Profile explanation remains a no-write, no-secret, no-custom-code inspection surface. | `tests/substrate/cli/test_deployment_profile_explain.py`. |
| `WiringPlanDescriptorOnly`, `Issue253WiringPlanDescriptorOnly` | Deployment wiring plans are canonical descriptors only and cannot mutate files or execute custom code. | `tests/substrate/test_deployment_wiring_plan.py`. |
| `LintStaticByDefault`, `Issue254LintStaticDiagnostics` | Deployment lint is static by default, DB-free, non-mutating, and emits the required diagnostic fields. | `tests/substrate/cli/test_deployment_lint.py`. |
| `EnvDoctorRedactsByDefault`, `EnvDoctorPreservesEnvCommand`, `EnvDoctorReportsAuthorityWinner`, `EnvDoctorRiskClassification`, `Issue255EnvDoctorAuthoritySafety` | Env doctor hides secret values unless explicitly requested, keeps the env command available, names the authority source, and classifies DSN target mismatches by read/write/runtime risk. | `tests/substrate/cli/test_deployment_env_doctor.py`. |
| `ReadyExplainIsAdditive` | Ready explanation preserves the existing readiness fields while adding diagnostics. | `tests/substrate/cli/test_deployment_lint.py::test_ready_explain_adapter_is_additive_and_preserves_ready_payload`. |
| `Issue252ScaffoldSafety`, `DeploymentAuthoringApproachContract` | The issue-level scaffold, plan, lint, env-doctor, profile, and ready-explain contracts hold together. | `scripts/check_formal_deployment_authoring.py` emits strict formal report rows bound to the implementation tests above. |
| `BrokenInspectionNoLocalRuntime`, `BrokenInspectionNoCustomExecution` | Non-vacuity witnesses, one per config: `DeploymentAuthoringCounterexample.cfg` asserts no custom-execution state is reachable and `DeploymentAuthoringNoLocalRuntimeWitness.cfg` asserts no local-runtime-writing state is reachable; with the mutation flags free, TLC refutes each by name. The violations prove that `InspectionSurfacesNoMutation` and `InspectionSurfacesNoCustomExecution` constrain reachable state rather than holding vacuously over a clamped-flag world. | Each config must report its own named broken invariant violated (`run_formal_suite` matches by name, no any-witness fallback); the positive no-mutation behavior is exercised by the CLI inspection tests named above. |

## Verification Process

Run:

```sh
uv run --extra dev python scripts/check_formal_deployment_authoring.py \
  --out .scratch/formal/deployment-authoring
```

The checker emits strict formal report rows for each issue-level property and
binds them to the runtime tests listed in `REFINEMENT_TESTS_BY_PROPERTY`.

## Model Boundary

The model abstracts deployment names, risks, authority sources, and diagnostic
fields into finite sets. It does not prove live DB readiness, source-registry
correctness, filesystem permission behavior, or external secret-provider
semantics. Those remain runtime obligations for focused CLI tests, deployment
readiness checks, and dogfood evidence.
