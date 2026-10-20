---- MODULE SourceSyncObservability ----
EXTENDS Naturals

\* Source-sync observability model for benchmark/readiness evidence.
\* The runtime validator must not turn missing or self-confirming measurement
\* inputs into a ready state, and amplification must be derived from measured
\* source work and observed delta units.

ValueDomain == 0..5
ExpectedDeltaSources == {
  "explicit",
  "harness_declared",
  "missing_or_empty",
  "observed_delta_default",
  "self_confirming"
}
PreRunExpectedDeltaSources == {
  "explicit",
  "harness_declared"
}
Severities == {"ok", "warning", "error"}
MaxAmplification == 3

VARIABLES
  expectedDeltaUnits,
  observedDeltaUnits,
  expectedDeltaSource,
  sourceInputCount,
  sourceOutputCount,
  measurementCount,
  latencyComplete,
  unavailableMeasurement,
  queryVisible,
  generationVisible,
  queueDepth,
  dlqCount

vars == <<
  expectedDeltaUnits,
  observedDeltaUnits,
  expectedDeltaSource,
  sourceInputCount,
  sourceOutputCount,
  measurementCount,
  latencyComplete,
  unavailableMeasurement,
  queryVisible,
  generationVisible,
  queueDepth,
  dlqCount
>>

Init ==
  /\ expectedDeltaUnits \in ValueDomain
  /\ observedDeltaUnits \in ValueDomain
  /\ expectedDeltaSource \in ExpectedDeltaSources
  /\ sourceInputCount \in ValueDomain
  /\ sourceOutputCount \in ValueDomain
  /\ measurementCount \in ValueDomain
  /\ latencyComplete \in BOOLEAN
  /\ unavailableMeasurement \in BOOLEAN
  /\ queryVisible \in BOOLEAN
  /\ generationVisible \in BOOLEAN
  /\ queueDepth \in ValueDomain
  /\ dlqCount \in ValueDomain

Next == UNCHANGED vars

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ expectedDeltaUnits \in ValueDomain
  /\ observedDeltaUnits \in ValueDomain
  /\ expectedDeltaSource \in ExpectedDeltaSources
  /\ sourceInputCount \in ValueDomain
  /\ sourceOutputCount \in ValueDomain
  /\ measurementCount \in ValueDomain
  /\ latencyComplete \in BOOLEAN
  /\ unavailableMeasurement \in BOOLEAN
  /\ queryVisible \in BOOLEAN
  /\ generationVisible \in BOOLEAN
  /\ queueDepth \in ValueDomain
  /\ dlqCount \in ValueDomain

ExpectedDeltaValid ==
  /\ expectedDeltaSource \in PreRunExpectedDeltaSources
  /\ expectedDeltaUnits > 0

ObservedDeltaValid == observedDeltaUnits > 0

DeltaConsistency ==
  /\ ExpectedDeltaValid
  /\ ObservedDeltaValid
  /\ expectedDeltaUnits = observedDeltaUnits

MeasuredTermsPresent ==
  /\ measurementCount > 0
  /\ latencyComplete
  /\ ~unavailableMeasurement

ReadSideVisible ==
  /\ queryVisible
  /\ generationVisible

QueueHealthy ==
  /\ queueDepth = 0
  /\ dlqCount = 0

AmplificationWithinBudget ==
  observedDeltaUnits > 0
    /\ sourceOutputCount <= MaxAmplification * observedDeltaUnits

MeasurementGap ==
  \/ ~ExpectedDeltaValid
  \/ ~ObservedDeltaValid
  \/ ~MeasuredTermsPresent
  \/ ~ReadSideVisible

SelfConfirmingExpectedDelta ==
  expectedDeltaSource \in {"observed_delta_default", "self_confirming"}

AmplificationRegression ==
  observedDeltaUnits > 0
    /\ sourceOutputCount > MaxAmplification * observedDeltaUnits

ReadinessSeverity ==
  IF MeasurementGap \/ SelfConfirmingExpectedDelta \/ dlqCount > 0 THEN "error"
  ELSE IF queueDepth > 0 \/ AmplificationRegression THEN "warning"
  ELSE "ok"

ObservabilityReady ==
  /\ DeltaConsistency
  /\ MeasuredTermsPresent
  /\ ReadSideVisible
  /\ QueueHealthy
  /\ AmplificationWithinBudget
  /\ ~SelfConfirmingExpectedDelta

AmplificationUsesMeasuredTerms ==
  ObservabilityReady =>
    /\ expectedDeltaSource \in PreRunExpectedDeltaSources
    /\ expectedDeltaUnits = observedDeltaUnits
    /\ observedDeltaUnits > 0
    /\ sourceOutputCount <= MaxAmplification * observedDeltaUnits
    /\ measurementCount > 0
    /\ ReadinessSeverity = "ok"

MeasurementGapsSurfaceAsWarningsOrErrors ==
  MeasurementGap => /\ ~ObservabilityReady
                    /\ ReadinessSeverity \in {"warning", "error"}

SelfConfirmingExpectedDeltaNotReady ==
  SelfConfirmingExpectedDelta => /\ ~ObservabilityReady
                                 /\ ReadinessSeverity = "error"

OverAmplificationWarnsOrBlocks ==
  AmplificationRegression => /\ ~ObservabilityReady
                             /\ ReadinessSeverity \in {"warning", "error"}

SourceSyncObservabilityContract ==
  /\ TypeOK
  /\ AmplificationUsesMeasuredTerms
  /\ MeasurementGapsSurfaceAsWarningsOrErrors
  /\ SelfConfirmingExpectedDeltaNotReady
  /\ OverAmplificationWarnsOrBlocks

====
