---- MODULE MergeBackRestamp ----

\* Landed-row provenance restamp, per field (strengthen-formal-models wave
\* 3). Production rewrites source, source_record_id, and source_revision on
\* every landed row (merge_restamp.py); the hazard its own docstring names —
\* restamp source but forget source_record_id — was unrepresentable while
\* the model carried one provenanceRestamped boolean. The fields are now
\* separate state, and the pass-through hazards (fork identity surviving,
\* promote marker landing, trusted default source) are driven by buggy
\* toggles: FALSE in the positive configuration (the invariants hold), TRUE
\* in per-invariant witness configurations (TLC refutes the named invariant,
\* proving it constrains reachable state).

CONSTANTS
  BuggySkipRecordIdRestamp,
  BuggySkipRevisionRestamp,
  BuggyForkIdentityLands,
  BuggyPromoteMarkerLands,
  BuggyTrustedDefault

SourceKinds == {"ordinary", "merge", "null", "system", "derived"}
TrustedKinds == {"null", "system"}
ReservedKinds == {"null", "system", "derived"}
Phases == {"start", "blocked", "restamped", "landed"}
TrustLabels == {"none", "trusted", "untrusted"}
Sources == {"none", "fork", "merge"}

VARIABLES
  sourceKind,
  phase,
  landedSource,
  defaultTrust,
  sourceRestamped,
  recordIdRestamped,
  revisionRestamped,
  forkIdentitySurvives,
  promoteMarkerLands

vars == <<
  sourceKind,
  phase,
  landedSource,
  defaultTrust,
  sourceRestamped,
  recordIdRestamped,
  revisionRestamped,
  forkIdentitySurvives,
  promoteMarkerLands
>>

Init ==
  /\ sourceKind \in SourceKinds
  /\ phase = "start"
  /\ landedSource = "none"
  /\ defaultTrust = "none"
  /\ sourceRestamped = FALSE
  /\ recordIdRestamped = FALSE
  /\ revisionRestamped = FALSE
  /\ forkIdentitySurvives = FALSE
  /\ promoteMarkerLands = FALSE

BlockReserved ==
  /\ phase = "start"
  /\ sourceKind \in ReservedKinds
  /\ phase' = "blocked"
  /\ UNCHANGED <<
       sourceKind, landedSource, defaultTrust, sourceRestamped,
       recordIdRestamped, revisionRestamped, forkIdentitySurvives,
       promoteMarkerLands
     >>

\* One production pass rewrites all three provenance fields; the buggy
\* toggles model the per-field forgetting and pass-through hazards the
\* invariants below exist to catch.
RestampMergeable ==
  /\ phase = "start"
  /\ sourceKind \in {"ordinary", "merge"}
  /\ landedSource' = "merge"
  /\ defaultTrust' =
       IF BuggyTrustedDefault THEN "trusted" ELSE "untrusted"
  /\ sourceRestamped' = TRUE
  /\ recordIdRestamped' = ~BuggySkipRecordIdRestamp
  /\ revisionRestamped' = ~BuggySkipRevisionRestamp
  /\ forkIdentitySurvives' = BuggyForkIdentityLands
  /\ promoteMarkerLands' = BuggyPromoteMarkerLands
  /\ phase' = "restamped"
  /\ UNCHANGED << sourceKind >>

Land ==
  /\ phase = "restamped"
  /\ phase' = "landed"
  /\ UNCHANGED <<
       sourceKind, landedSource, defaultTrust, sourceRestamped,
       recordIdRestamped, revisionRestamped, forkIdentitySurvives,
       promoteMarkerLands
     >>

Next == BlockReserved \/ RestampMergeable \/ Land

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ sourceKind \in SourceKinds
  /\ phase \in Phases
  /\ landedSource \in Sources
  /\ defaultTrust \in TrustLabels
  /\ sourceRestamped \in BOOLEAN
  /\ recordIdRestamped \in BOOLEAN
  /\ revisionRestamped \in BOOLEAN
  /\ forkIdentitySurvives \in BOOLEAN
  /\ promoteMarkerLands \in BOOLEAN

ReservedOrNullSourcesBlockBeforeStage ==
  sourceKind \in ReservedKinds => phase # "landed"

MergeProducerClassifiesUntrusted ==
  landedSource = "merge" => defaultTrust = "untrusted"

SourceFieldRestamped ==
  phase = "landed" => sourceRestamped

SourceRecordIdRestamped ==
  phase = "landed" => recordIdRestamped

SourceRevisionRestamped ==
  phase = "landed" => revisionRestamped

ForkProvenanceIdentityDoesNotLand ==
  phase = "landed" => ~forkIdentitySurvives

PromoteMarkerDoesNotLand ==
  phase = "landed" => ~promoteMarkerLands

NoForkInputYieldsTrustedDefaultSource ==
  phase = "landed" => defaultTrust # "trusted"

MergeBackRestampSafety ==
  /\ TypeOK
  /\ ReservedOrNullSourcesBlockBeforeStage
  /\ MergeProducerClassifiesUntrusted
  /\ SourceFieldRestamped
  /\ SourceRecordIdRestamped
  /\ SourceRevisionRestamped
  /\ ForkProvenanceIdentityDoesNotLand
  /\ PromoteMarkerDoesNotLand
  /\ NoForkInputYieldsTrustedDefaultSource

====
