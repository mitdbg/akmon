---- MODULE MergeBackReplay ----
EXTENDS FiniteSets

Rows == {"base", "accepted", "gate_derived"}
Phases == {"start", "staged", "gated", "blocked", "landed", "next_published"}
GateStatus == {"none", "passed", "blocked"}

VARIABLES
  phase,
  defaultRows,
  stagingRows,
  gateValidatedRows,
  gateDerivedRows,
  landRows,
  gateStatus,
  targetAdvanced,
  nextPublishRetracted

vars == <<
  phase,
  defaultRows,
  stagingRows,
  gateValidatedRows,
  gateDerivedRows,
  landRows,
  gateStatus,
  targetAdvanced,
  nextPublishRetracted
>>

Init ==
  /\ phase = "start"
  /\ defaultRows = {"base"}
  /\ stagingRows = {}
  /\ gateValidatedRows = {}
  /\ gateDerivedRows = {}
  /\ landRows = {}
  /\ gateStatus = "none"
  /\ targetAdvanced = FALSE
  /\ nextPublishRetracted = FALSE

Stage ==
  /\ phase = "start"
  /\ stagingRows' = {"accepted"}
  /\ phase' = "staged"
  /\ UNCHANGED <<
       defaultRows,
       gateValidatedRows,
       gateDerivedRows,
       landRows,
       gateStatus,
       targetAdvanced,
       nextPublishRetracted
     >>

TargetAdvance ==
  /\ phase \in {"staged", "gated"}
  /\ targetAdvanced' = TRUE
  /\ UNCHANGED <<
       phase,
       defaultRows,
       stagingRows,
       gateValidatedRows,
       gateDerivedRows,
       landRows,
       gateStatus,
       nextPublishRetracted
     >>

GatePass ==
  /\ phase = "staged"
  /\ ~targetAdvanced
  /\ gateValidatedRows' = stagingRows
  /\ gateDerivedRows' = {"gate_derived"}
  /\ gateStatus' = "passed"
  /\ phase' = "gated"
  /\ UNCHANGED <<
       defaultRows,
       stagingRows,
       landRows,
       targetAdvanced,
       nextPublishRetracted
     >>

GateBlock ==
  /\ phase = "staged"
  /\ gateStatus' = "blocked"
  /\ phase' = "blocked"
  /\ UNCHANGED <<
       defaultRows,
       stagingRows,
       gateValidatedRows,
       gateDerivedRows,
       landRows,
       targetAdvanced,
       nextPublishRetracted
     >>

RebaseRequired ==
  /\ phase = "gated"
  /\ targetAdvanced
  /\ gateStatus' = "blocked"
  /\ phase' = "blocked"
  /\ UNCHANGED <<
       defaultRows,
       stagingRows,
       gateValidatedRows,
       gateDerivedRows,
       landRows,
       targetAdvanced,
       nextPublishRetracted
     >>

LandReplay ==
  /\ phase = "gated"
  /\ ~targetAdvanced
  /\ landRows' = stagingRows
  /\ defaultRows' = defaultRows \cup stagingRows
  /\ phase' = "landed"
  /\ UNCHANGED <<
       stagingRows,
       gateValidatedRows,
       gateDerivedRows,
       gateStatus,
       targetAdvanced,
       nextPublishRetracted
     >>

NextPublish ==
  /\ phase = "landed"
  /\ nextPublishRetracted' = FALSE
  /\ phase' = "next_published"
  /\ UNCHANGED <<
       defaultRows,
       stagingRows,
       gateValidatedRows,
       gateDerivedRows,
       landRows,
       gateStatus,
       targetAdvanced
     >>

Next ==
  \/ Stage
  \/ TargetAdvance
  \/ GatePass
  \/ GateBlock
  \/ RebaseRequired
  \/ LandReplay
  \/ NextPublish

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ phase \in Phases
  /\ defaultRows \subseteq Rows
  /\ stagingRows \subseteq Rows
  /\ gateValidatedRows \subseteq Rows
  /\ gateDerivedRows \subseteq Rows
  /\ landRows \subseteq Rows
  /\ gateStatus \in GateStatus
  /\ targetAdvanced \in BOOLEAN
  /\ nextPublishRetracted \in BOOLEAN

DefaultIsolatedUntilGatePass ==
  phase \notin {"landed", "next_published"} => defaultRows = {"base"}

GateValidatesStagedAcceptedDelta ==
  gateStatus = "passed" =>
    /\ gateValidatedRows = stagingRows
    /\ stagingRows = {"accepted"}

LandReplayEqualsGatedSourceDelta ==
  phase \in {"landed", "next_published"} =>
    /\ landRows = gateValidatedRows
    /\ landRows = {"accepted"}

GateBlockedLeavesDefaultUnchanged ==
  gateStatus = "blocked" => defaultRows = {"base"}

TargetAdvanceBlocksLand ==
  targetAdvanced => phase # "landed" /\ phase # "next_published"

GateDerivedRowsAreNotReplayed ==
  "gate_derived" \notin landRows

NoPhantomRetractionAfterLand ==
  phase = "next_published" =>
    /\ {"base", "accepted"} \subseteq defaultRows
    /\ ~nextPublishRetracted

MergeBackReplaySafety ==
  /\ TypeOK
  /\ DefaultIsolatedUntilGatePass
  /\ GateValidatesStagedAcceptedDelta
  /\ LandReplayEqualsGatedSourceDelta
  /\ GateBlockedLeavesDefaultUnchanged
  /\ TargetAdvanceBlocksLand
  /\ GateDerivedRowsAreNotReplayed
  /\ NoPhantomRetractionAfterLand

====
