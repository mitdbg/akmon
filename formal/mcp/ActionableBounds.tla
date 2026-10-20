---- MODULE ActionableBounds ----
EXTENDS FiniteSets, Naturals, Sequences

CONSTANTS MaxHops, MaxFrontierRows, MaxResultRows, MaxSummaryKeys, SummaryPageSize

\* The model keeps graph visibility and token fields finite so TLC can
\* exhaustively explore the security boundary without a live database.
Hops == 0..MaxHops
StartClasses == {"visible", "hidden", "absent"}
InputKinds == {
  "invalid_edge_type",
  "invalid_direction",
  \* An explicit empty path is rejected before graph execution; it is not a
  \* successful terminal-empty result.
  "invalid_path_grammar",
  "visible_empty_first",
  "visible_empty_intermediate",
  "visible_empty_terminal",
  "nonempty_complete",
  "nonempty_truncated"
}
EmptyKinds == {
  "start_not_visible",
  "visibility_limited",
  "no_matching_first_hop",
  "empty_intermediate_hop",
  "empty_terminal_result"
}
InvalidKinds == {"invalid_edge_type", "invalid_direction", "invalid_path_grammar"}
ExactEmptyKinds == {
  "start_not_visible",
  "no_matching_first_hop",
  "empty_intermediate_hop",
  "empty_terminal_result"
}
CountStates == {"not_applicable", "withheld", "exact_visible", "bounded_visible"}
NoTotalCount == "no_total_count"
PublicKinds == InputKinds \cup EmptyKinds
Phases == {
  "init",
  "request_selected",
  "outcome_emitted",
  "candidate_presented",
  "cursor_checked",
  "summary_page_ready",
  "summary_complete"
}
NoRequest == "no_request"
NoOutcome == "no_outcome"
NoHop == -1
NoContext == "no_context"
NoCandidate == "no_candidate"
TamperedMac == "tampered_mac"

Deployments == {"deployment_a", "deployment_b"}
Generations == {"generation_1", "generation_2"}
Databases == {"database_a", "database_b"}
Branches == {"branch_a", "branch_b"}
Catalogs == {"catalog_a", "catalog_b"}
Subtypes == {"subtype_a", "subtype_b"}
PathOrSubtypes == {"path_alpha", "subtype_summary"}
Authorities == {"reader_a", "reader_b"}
SortOrders == {"stable_id_asc", "stable_id_desc"}
Projections == {"ids", "ids_and_types"}
PolicyFingerprints == {"policy_a", "policy_b"}
KeyIds == {"key_a", "key_b"}
CursorPositions == {0, 2, 4, 6}

ContextDomain == {
  [ deployment |-> d,
    generation |-> g,
    database |-> db,
    branch |-> b,
    catalog |-> c,
    subtype |-> s,
    pathOrSubtype |-> ps,
    authority |-> a,
    sortOrder |-> o,
    projection |-> p,
    policyFingerprint |-> pf,
    keyId |-> k,
    cursorPosition |-> pos ] :
      d \in Deployments,
      g \in Generations,
      db \in Databases,
      b \in Branches,
      c \in Catalogs,
      s \in Subtypes,
      ps \in PathOrSubtypes,
      a \in Authorities,
      o \in SortOrders,
      p \in Projections,
      pf \in PolicyFingerprints,
      k \in KeyIds,
      pos \in CursorPositions
}

\* Alternate chooses a different value and lets the candidate action exercise
\* each individual binding field without an unbounded token universe.
Alternate(value, domain) == CHOOSE x \in domain : x # value

CandidateVariants == {
  "valid",
  "tampered_mac",
  "deployment",
  "generation",
  "database",
  "branch",
  "catalog",
  "subtype",
  "path_or_subtype",
  "authority",
  "sort_order",
  "projection",
  "policy_fingerprint",
  "key_id",
  "cursor_position"
}

\* The subtype summary is a stable ordered sequence of the six public sections.
\* A continuation carries the next offset; every page is a deterministic slice
\* of this sequence, so reconstruction cannot skip or duplicate a section.
SummarySections == {
  "attributes",
  "filters",
  "outgoing_routes",
  "incoming_routes",
  "visibility_state",
  "declared_state"
}
SummaryOrder == <<
  "attributes",
  "filters",
  "outgoing_routes",
  "incoming_routes",
  "visibility_state",
  "declared_state"
>>
SummaryKeys == SummarySections
SummarySectionCount == Len(SummaryOrder)
SummaryFields == [section \in SummarySections |->
  IF section = "attributes"
  THEN {"value_types", "presence_frequency", "null_frequency", "sample_values"}
  ELSE IF section = "filters"
    THEN {"recommended_filters"}
    ELSE IF section = "outgoing_routes"
      THEN {"edge_type", "destination_subtype", "route_count"}
      ELSE IF section = "incoming_routes"
        THEN {"edge_type", "source_subtype", "route_count"}
        ELSE IF section = "visibility_state"
          THEN {"visible_count", "authority_mode"}
          ELSE {"declared_subtype", "declared_attributes"}]
SummaryEnd(offset) ==
  IF offset + SummaryPageSize < MaxSummaryKeys
  THEN offset + SummaryPageSize
  ELSE MaxSummaryKeys
SummaryPage(offset) == SubSeq(SummaryOrder, offset + 1, SummaryEnd(offset))
SummaryPrefix(n) == IF n = 0 THEN <<>> ELSE SubSeq(SummaryOrder, 1, n)
SummaryPrefixSet(n) == {SummaryOrder[i] : i \in 1..n}
SequenceSet(sequence) == {sequence[i] : i \in 1..Len(sequence)}

CandidateContext(variant, expected) ==
  CASE
    variant = "valid" -> expected
    [] variant = "tampered_mac" -> expected
    [] variant = "deployment" -> [expected EXCEPT !.deployment = Alternate(expected.deployment, Deployments)]
    [] variant = "generation" -> [expected EXCEPT !.generation = Alternate(expected.generation, Generations)]
    [] variant = "database" -> [expected EXCEPT !.database = Alternate(expected.database, Databases)]
    [] variant = "branch" -> [expected EXCEPT !.branch = Alternate(expected.branch, Branches)]
    [] variant = "catalog" -> [expected EXCEPT !.catalog = Alternate(expected.catalog, Catalogs)]
    [] variant = "subtype" -> [expected EXCEPT !.subtype = Alternate(expected.subtype, Subtypes)]
    [] variant = "path_or_subtype" -> [expected EXCEPT !.pathOrSubtype = Alternate(expected.pathOrSubtype, PathOrSubtypes)]
    [] variant = "authority" -> [expected EXCEPT !.authority = Alternate(expected.authority, Authorities)]
    [] variant = "sort_order" -> [expected EXCEPT !.sortOrder = Alternate(expected.sortOrder, SortOrders)]
    [] variant = "projection" -> [expected EXCEPT !.projection = Alternate(expected.projection, Projections)]
    [] variant = "policy_fingerprint" -> [expected EXCEPT !.policyFingerprint = Alternate(expected.policyFingerprint, PolicyFingerprints)]
    [] variant = "key_id" -> [expected EXCEPT !.keyId = Alternate(expected.keyId, KeyIds)]
    [] variant = "cursor_position" -> [expected EXCEPT !.cursorPosition = Alternate(expected.cursorPosition, CursorPositions)]

CandidateMac(variant, expected) ==
  IF variant = "tampered_mac"
  THEN TamperedMac
  ELSE CandidateContext(variant, expected)

\* The token is deterministic for the complete context.  The concrete
\* implementation refines this with an authenticated MAC/key lookup; the
\* model checks that no changed context field can reuse the old token.
TokenFor(context) == [context |-> context, mac |-> context]

Authenticate(token, expected) ==
  /\ expected # NoContext
  /\ token = TokenFor(expected)

VisibleProjection(structure) ==
  [h \in Hops |-> structure[h] = "visible"]

FirstNonVisible(structure, upto) ==
  IF \E h \in 0..upto : structure[h] # "visible"
  THEN CHOOSE h \in 0..upto :
         /\ structure[h] # "visible"
         /\ \A previous \in 0..upto :
               previous < h => structure[previous] = "visible"
  ELSE NoHop

\* Invalid syntax is rejected before graph visibility is consulted, at any
\* requested hop.  For valid requests, the first hidden/absent hop is the
\* only diagnostic index exposed to the caller.
EmptyHopFor(kind, upto) ==
  IF kind = "visible_empty_first"
  THEN 1
  ELSE IF kind = "visible_empty_intermediate"
    THEN 2
    ELSE upto

OutcomeFor(kind, structure, upto) ==
  LET decisive == EmptyHopFor(kind, upto)
      first == FirstNonVisible(structure, decisive)
      hiddenBefore == \E h \in 0..decisive : structure[h] # "visible"
  IN
    IF kind \in InvalidKinds
    THEN kind
    ELSE IF structure[0] # "visible"
      THEN "start_not_visible"
      ELSE IF hiddenBefore
        THEN "visibility_limited"
      ELSE IF kind = "visible_empty_first"
        THEN "no_matching_first_hop"
        ELSE IF kind = "visible_empty_intermediate"
          THEN "empty_intermediate_hop"
          ELSE IF kind = "visible_empty_terminal"
            THEN "empty_terminal_result"
            ELSE kind

DiagnosticHopFor(kind, structure, upto) ==
  LET decisive == EmptyHopFor(kind, upto)
      first == FirstNonVisible(structure, decisive)
  IN
    IF kind \in InvalidKinds
    THEN upto
    ELSE IF first = NoHop
      THEN decisive
      ELSE first

VARIABLES
  phase,
  inputKind,
  requestedHop,
  structureA,
  structureB,
  outcomeA,
  outcomeB,
  diagnosticHopA,
  diagnosticHopB,
  probeRowsA,
  probeRowsB,
  publicFrontierA,
  publicFrontierB,
  resultCountA,
  resultCountB,
  countStateA,
  countStateB,
  totalCountA,
  totalCountB,
  truncated,
  continuationIssued,
  cursorContext,
  candidateVariant,
  cursorAccepted,
  summaryActive,
  summaryOffset,
  summaryPageStart,
  summaryPage,
  summaryCollected,
  summarySeen,
  summaryPageCount,
  summaryHasMore

vars == <<
  phase,
  inputKind,
  requestedHop,
  structureA,
  structureB,
  outcomeA,
  outcomeB,
  diagnosticHopA,
  diagnosticHopB,
  probeRowsA,
  probeRowsB,
  publicFrontierA,
  publicFrontierB,
  resultCountA,
  resultCountB,
  countStateA,
  countStateB,
  totalCountA,
  totalCountB,
  truncated,
  continuationIssued,
  cursorContext,
  candidateVariant,
  cursorAccepted,
  summaryActive,
  summaryOffset,
  summaryPageStart,
  summaryPage,
  summaryCollected,
  summarySeen,
  summaryPageCount,
  summaryHasMore
>>

InitialStructure == [h \in Hops |-> "visible"]

Init ==
  /\ phase = "init"
  /\ inputKind = NoRequest
  /\ requestedHop = 1
  /\ structureA = InitialStructure
  /\ structureB = InitialStructure
  /\ outcomeA = NoOutcome
  /\ outcomeB = NoOutcome
  /\ diagnosticHopA = NoHop
  /\ diagnosticHopB = NoHop
  /\ probeRowsA = 0
  /\ probeRowsB = 0
  /\ publicFrontierA = 0
  /\ publicFrontierB = 0
  /\ resultCountA = 0
  /\ resultCountB = 0
  /\ countStateA = "not_applicable"
  /\ countStateB = "not_applicable"
  /\ totalCountA = NoTotalCount
  /\ totalCountB = NoTotalCount
  /\ truncated = FALSE
  /\ continuationIssued = FALSE
  /\ cursorContext = NoContext
  /\ candidateVariant = NoCandidate
  /\ cursorAccepted = FALSE
  /\ summaryActive = FALSE
  /\ summaryOffset = 0
  /\ summaryPageStart = 0
  /\ summaryPage = <<>>
  /\ summaryCollected = <<>>
  /\ summarySeen = {}
  /\ summaryPageCount = 0
  /\ summaryHasMore = FALSE

\* The two abstract executions may differ at every hop between hidden and
\* absent, but their visible projection is identical.  This is the model's
\* observational-equivalence witness for Nomos-hidden versus nonexistent
\* graph structure at the start and at later hops.
SelectRequest ==
  /\ phase = "init"
  /\ inputKind' \in InputKinds
  /\ requestedHop' \in 1..MaxHops
  /\ structureA' \in [Hops -> StartClasses]
  /\ structureB' \in [Hops -> StartClasses]
  /\ VisibleProjection(structureA') = VisibleProjection(structureB')
  /\ phase' = "request_selected"
  /\ outcomeA' = NoOutcome
  /\ outcomeB' = NoOutcome
  /\ diagnosticHopA' = NoHop
  /\ diagnosticHopB' = NoHop
  /\ probeRowsA' = 0
  /\ probeRowsB' = 0
  /\ publicFrontierA' = 0
  /\ publicFrontierB' = 0
  /\ resultCountA' = 0
  /\ resultCountB' = 0
  /\ countStateA' = "not_applicable"
  /\ countStateB' = "not_applicable"
  /\ totalCountA' = NoTotalCount
  /\ totalCountB' = NoTotalCount
  /\ truncated' = FALSE
  /\ continuationIssued' = FALSE
  /\ cursorContext' = NoContext
  /\ candidateVariant' = NoCandidate
  /\ cursorAccepted' = FALSE
  /\ summaryActive' = FALSE
  /\ summaryOffset' = 0
  /\ summaryPageStart' = 0
  /\ summaryPage' = <<>>
  /\ summaryCollected' = <<>>
  /\ summarySeen' = {}
  /\ summaryPageCount' = 0
  /\ summaryHasMore' = FALSE

\* Empty outcomes perform a bounded positive probe.  Probe rows are internal
\* work and are never copied into the public envelope, so hidden/absent
\* cardinality cannot be inferred from them.
EmitOutcome ==
  /\ phase = "request_selected"
  /\ outcomeA' = OutcomeFor(inputKind, structureA, requestedHop)
  /\ outcomeB' = OutcomeFor(inputKind, structureB, requestedHop)
  /\ diagnosticHopA' = DiagnosticHopFor(inputKind, structureA, requestedHop)
  /\ diagnosticHopB' = DiagnosticHopFor(inputKind, structureB, requestedHop)
  /\ outcomeA' = outcomeB'
  /\ diagnosticHopA' = diagnosticHopB'
  /\ IF inputKind \in InvalidKinds
       THEN /\ probeRowsA' = 0
            /\ probeRowsB' = 0
       ELSE IF outcomeA' \in EmptyKinds
         THEN /\ probeRowsA' \in 1..MaxFrontierRows
              /\ probeRowsB' \in 1..MaxFrontierRows
         ELSE /\ probeRowsA' \in 1..MaxFrontierRows
              /\ probeRowsB' \in 1..MaxFrontierRows
  /\ IF outcomeA' \in EmptyKinds \cup InvalidKinds
       THEN /\ publicFrontierA' = 0
            /\ publicFrontierB' = 0
            /\ resultCountA' = 0
            /\ resultCountB' = 0
       ELSE /\ publicFrontierA' \in 1..MaxFrontierRows
            /\ publicFrontierB' = publicFrontierA'
            /\ resultCountA' \in 1..MaxResultRows
            /\ resultCountB' = resultCountA'
  /\ IF outcomeA' = "visibility_limited"
       THEN /\ countStateA' = "withheld"
            /\ totalCountA' = NoTotalCount
       ELSE IF outcomeA' \in ExactEmptyKinds
         THEN /\ countStateA' = "exact_visible"
              /\ totalCountA' = 0
         ELSE IF outcomeA' = "nonempty_complete"
           THEN /\ countStateA' = "exact_visible"
                /\ totalCountA' = resultCountA'
           ELSE IF outcomeA' = "nonempty_truncated"
             THEN /\ countStateA' = "bounded_visible"
                  /\ totalCountA' = NoTotalCount
             ELSE /\ countStateA' = "not_applicable"
                  /\ totalCountA' = NoTotalCount
  /\ countStateB' = countStateA'
  /\ totalCountB' = totalCountA'
  /\ IF outcomeA' = "nonempty_truncated"
       THEN /\ truncated' = TRUE
            /\ continuationIssued' = TRUE
            /\ cursorContext' \in ContextDomain
       ELSE /\ truncated' = FALSE
            /\ continuationIssued' = FALSE
            /\ cursorContext' = NoContext
  /\ phase' = "outcome_emitted"
  /\ UNCHANGED <<
       candidateVariant,
       cursorAccepted,
       summaryActive,
       summaryOffset,
       summaryPageStart,
       summaryPage,
       summaryCollected,
       summarySeen,
       summaryPageCount,
       summaryHasMore
     >>

\* Start a whole-subtype summary after the path outcome.  The first page is
\* fetched separately so the continuation boundary is explicit in the model.
StartSummary ==
  /\ phase = "outcome_emitted"
  /\ ~continuationIssued
  /\ summaryActive' = TRUE
  /\ summaryOffset' = 0
  /\ summaryPageStart' = 0
  /\ summaryPage' = <<>>
  /\ summaryCollected' = <<>>
  /\ summarySeen' = {}
  /\ summaryPageCount' = 0
  /\ summaryHasMore' = FALSE
  /\ phase' = "summary_page_ready"
  /\ UNCHANGED <<
       inputKind,
       requestedHop,
       structureA,
       structureB,
       outcomeA,
       outcomeB,
       diagnosticHopA,
       diagnosticHopB,
       probeRowsA,
       probeRowsB,
       publicFrontierA,
       publicFrontierB,
       resultCountA,
       resultCountB,
       countStateA,
       countStateB,
       totalCountA,
       totalCountB,
       truncated,
       continuationIssued,
       cursorContext,
       candidateVariant,
       cursorAccepted
     >>

\* Fetching is deterministic: the page is the next stable slice, and a
\* continuation can only advance from the current offset.  A continuation
\* accepted through PresentCandidate/AcceptCandidate is required for every
\* page after the first.
FetchSummaryPage ==
  /\ summaryActive
  /\ ((phase = "summary_page_ready" /\ ~continuationIssued)
      \/ (phase = "cursor_checked" /\ continuationIssued /\ cursorAccepted))
  /\ summaryOffset < MaxSummaryKeys
  /\ LET page == SummaryPage(summaryOffset)
         nextOffset == summaryOffset + Len(page)
         hasMore == nextOffset < MaxSummaryKeys
     IN /\ summaryPage' = page
        /\ summaryPageStart' = summaryOffset
        /\ summaryCollected' = summaryCollected \o page
        /\ summarySeen' = summarySeen \cup SequenceSet(page)
        /\ summaryOffset' = nextOffset
        /\ summaryPageCount' = summaryPageCount + 1
        /\ summaryHasMore' = hasMore
        /\ continuationIssued' = hasMore
        /\ IF hasMore
             THEN cursorContext' \in {c \in ContextDomain : c.cursorPosition = nextOffset}
             ELSE cursorContext' = NoContext
        /\ candidateVariant' = NoCandidate
        /\ cursorAccepted' = FALSE
        /\ phase' = IF hasMore THEN "summary_page_ready" ELSE "summary_complete"
  /\ UNCHANGED <<
       inputKind,
       requestedHop,
       structureA,
       structureB,
       outcomeA,
       outcomeB,
       diagnosticHopA,
       diagnosticHopB,
       probeRowsA,
       probeRowsB,
       publicFrontierA,
       publicFrontierB,
       resultCountA,
       resultCountB,
       countStateA,
       countStateB,
       totalCountA,
       totalCountB,
       truncated,
       summaryActive
     >>

\* Candidate variants mutate one bound field at a time, or the MAC itself.
\* This makes stale generation, cross-deployment/branch/catalog, projection,
\* policy, key, path, and cursor-position reuse explicit and load-bearing.
PresentCandidate ==
  /\ ((phase = "outcome_emitted" /\ ~summaryActive)
      \/ (phase = "summary_page_ready" /\ summaryActive))
  /\ continuationIssued
  /\ candidateVariant' \in CandidateVariants
  /\ phase' = "candidate_presented"
  /\ UNCHANGED <<
       inputKind,
       requestedHop,
       structureA,
       structureB,
       outcomeA,
       outcomeB,
       diagnosticHopA,
       diagnosticHopB,
       probeRowsA,
       probeRowsB,
       publicFrontierA,
       publicFrontierB,
       resultCountA,
       resultCountB,
       countStateA,
       countStateB,
       totalCountA,
       totalCountB,
       truncated,
       continuationIssued,
       cursorContext,
       cursorAccepted,
       summaryActive,
       summaryOffset,
       summaryPageStart,
       summaryPage,
       summaryCollected,
       summarySeen,
       summaryPageCount,
       summaryHasMore
     >>

AcceptCandidate ==
  /\ phase = "candidate_presented"
  /\ cursorAccepted' = Authenticate(
       [ context |-> CandidateContext(candidateVariant, cursorContext),
         mac |-> CandidateMac(candidateVariant, cursorContext) ],
       cursorContext
     )
  /\ phase' = "cursor_checked"
  /\ UNCHANGED <<
       inputKind,
       requestedHop,
       structureA,
       structureB,
       outcomeA,
       outcomeB,
       diagnosticHopA,
       diagnosticHopB,
       probeRowsA,
       probeRowsB,
       publicFrontierA,
       publicFrontierB,
       resultCountA,
       resultCountB,
       countStateA,
       countStateB,
       totalCountA,
       totalCountB,
       truncated,
       continuationIssued,
       cursorContext,
       candidateVariant,
       summaryActive,
       summaryOffset,
       summaryPageStart,
       summaryPage,
       summaryCollected,
       summarySeen,
       summaryPageCount,
       summaryHasMore
     >>

Reset ==
  /\ phase \in {"outcome_emitted", "cursor_checked", "summary_page_ready", "summary_complete"}
  /\ phase' = "init"
  /\ inputKind' = NoRequest
  /\ requestedHop' = 1
  /\ structureA' = InitialStructure
  /\ structureB' = InitialStructure
  /\ outcomeA' = NoOutcome
  /\ outcomeB' = NoOutcome
  /\ diagnosticHopA' = NoHop
  /\ diagnosticHopB' = NoHop
  /\ probeRowsA' = 0
  /\ probeRowsB' = 0
  /\ publicFrontierA' = 0
  /\ publicFrontierB' = 0
  /\ resultCountA' = 0
  /\ resultCountB' = 0
  /\ countStateA' = "not_applicable"
  /\ countStateB' = "not_applicable"
  /\ totalCountA' = NoTotalCount
  /\ totalCountB' = NoTotalCount
  /\ truncated' = FALSE
  /\ continuationIssued' = FALSE
  /\ cursorContext' = NoContext
  /\ candidateVariant' = NoCandidate
  /\ cursorAccepted' = FALSE
  /\ summaryActive' = FALSE
  /\ summaryOffset' = 0
  /\ summaryPageStart' = 0
  /\ summaryPage' = <<>>
  /\ summaryCollected' = <<>>
  /\ summarySeen' = {}
  /\ summaryPageCount' = 0
  /\ summaryHasMore' = FALSE

Next == SelectRequest \/ EmitOutcome \/ StartSummary \/ FetchSummaryPage \/ PresentCandidate \/ AcceptCandidate \/ Reset

Spec == Init /\ [][Next]_vars

TypeOK ==
  /\ phase \in Phases
  /\ inputKind \in InputKinds \cup {NoRequest}
  /\ requestedHop \in 1..MaxHops
  /\ structureA \in [Hops -> StartClasses]
  /\ structureB \in [Hops -> StartClasses]
  /\ outcomeA \in PublicKinds \cup {NoOutcome}
  /\ outcomeB \in PublicKinds \cup {NoOutcome}
  /\ diagnosticHopA \in Hops \cup {NoHop}
  /\ diagnosticHopB \in Hops \cup {NoHop}
  /\ probeRowsA \in 0..MaxFrontierRows
  /\ probeRowsB \in 0..MaxFrontierRows
  /\ publicFrontierA \in 0..MaxFrontierRows
  /\ publicFrontierB \in 0..MaxFrontierRows
  /\ resultCountA \in 0..MaxResultRows
  /\ resultCountB \in 0..MaxResultRows
  /\ countStateA \in CountStates
  /\ countStateB \in CountStates
  /\ totalCountA \in (0..MaxResultRows) \cup {NoTotalCount}
  /\ totalCountB \in (0..MaxResultRows) \cup {NoTotalCount}
  /\ truncated \in BOOLEAN
  /\ continuationIssued \in BOOLEAN
  /\ cursorContext \in ContextDomain \cup {NoContext}
  /\ candidateVariant \in CandidateVariants \cup {NoCandidate}
  /\ cursorAccepted \in BOOLEAN
  /\ summaryActive \in BOOLEAN
  /\ summaryOffset \in 0..MaxSummaryKeys
  /\ summaryPageStart \in 0..MaxSummaryKeys
  /\ summaryPage \in Seq(SummaryKeys)
  /\ summaryCollected \in Seq(SummaryKeys)
  /\ summarySeen \subseteq SummaryKeys
  /\ summaryPageCount \in 0..MaxSummaryKeys
  /\ summaryHasMore \in BOOLEAN

SummarySectionShape ==
  /\ SummarySectionCount = MaxSummaryKeys
  /\ SummaryKeys = SummarySections
  /\ \A section \in SummarySections : SummaryFields[section] # {}
  /\ SummaryFields["attributes"] =
       {"value_types", "presence_frequency", "null_frequency", "sample_values"}
  /\ SummaryFields["filters"] = {"recommended_filters"}
  /\ SummaryFields["outgoing_routes"] =
       {"edge_type", "destination_subtype", "route_count"}
  /\ SummaryFields["incoming_routes"] =
       {"edge_type", "source_subtype", "route_count"}
  /\ SummaryFields["visibility_state"] = {"visible_count", "authority_mode"}
  /\ SummaryFields["declared_state"] =
       {"declared_subtype", "declared_attributes"}

StructurePairVisibilityBound ==
  /\ structureA \in [Hops -> StartClasses]
  /\ structureB \in [Hops -> StartClasses]
  /\ VisibleProjection(structureA) = VisibleProjection(structureB)

PublicOutcomeEquivalence ==
  VisibleProjection(structureA) = VisibleProjection(structureB) =>
    /\ outcomeA = outcomeB
    /\ diagnosticHopA = diagnosticHopB
    /\ publicFrontierA = publicFrontierB
    /\ resultCountA = resultCountB
    /\ countStateA = countStateB
    /\ totalCountA = totalCountB

CountStateSafety ==
  /\ outcomeA = NoOutcome =>
       /\ countStateA = "not_applicable"
       /\ totalCountA = NoTotalCount
  /\ outcomeB = NoOutcome =>
       /\ countStateB = "not_applicable"
       /\ totalCountB = NoTotalCount
  /\ outcomeA = "visibility_limited" =>
       /\ countStateA = "withheld"
       /\ totalCountA = NoTotalCount
  /\ outcomeB = "visibility_limited" =>
       /\ countStateB = "withheld"
       /\ totalCountB = NoTotalCount
  /\ outcomeA \in ExactEmptyKinds =>
       /\ countStateA = "exact_visible"
       /\ totalCountA = 0
  /\ outcomeB \in ExactEmptyKinds =>
       /\ countStateB = "exact_visible"
       /\ totalCountB = 0
  /\ outcomeA = "nonempty_complete" =>
       /\ countStateA = "exact_visible"
       /\ totalCountA = resultCountA
  /\ outcomeB = "nonempty_complete" =>
       /\ countStateB = "exact_visible"
       /\ totalCountB = resultCountB
  /\ outcomeA = "nonempty_truncated" =>
       /\ countStateA = "bounded_visible"
       /\ totalCountA = NoTotalCount
  /\ outcomeB = "nonempty_truncated" =>
       /\ countStateB = "bounded_visible"
       /\ totalCountB = NoTotalCount
  /\ outcomeA \in InvalidKinds =>
       /\ countStateA = "not_applicable"
       /\ totalCountA = NoTotalCount
  /\ outcomeB \in InvalidKinds =>
       /\ countStateB = "not_applicable"
       /\ totalCountB = NoTotalCount
  /\ outcomeA # NoOutcome =>
       /\ countStateA = countStateB
       /\ totalCountA = totalCountB

NoHiddenStructureLeak ==
  /\ (structureA[0] = "visible" \/ inputKind \in InvalidKinds \/ outcomeA = NoOutcome \/ outcomeA = "visibility_limited" \/ outcomeA = "start_not_visible")
  /\ (structureA[0] = "visible" \/ inputKind \in InvalidKinds \/ outcomeA = NoOutcome \/ publicFrontierA = 0)
  /\ (structureA[0] = "visible" \/ inputKind \in InvalidKinds \/ outcomeA = NoOutcome \/ resultCountA = 0)
  /\ (structureA[0] = "visible" \/ inputKind \in InvalidKinds \/ outcomeA = NoOutcome \/ (outcomeA = "visibility_limited" /\ countStateA = "withheld" /\ totalCountA = NoTotalCount) \/ (outcomeA = "start_not_visible" /\ countStateA = "exact_visible" /\ totalCountA = 0))
  /\ (structureA[0] = "visible" \/ inputKind \in InvalidKinds \/ outcomeA = NoOutcome \/ cursorContext = NoContext)
  /\ (structureB[0] = "visible" \/ inputKind \in InvalidKinds \/ outcomeB = NoOutcome \/ outcomeB = "visibility_limited" \/ outcomeB = "start_not_visible")
  /\ (structureB[0] = "visible" \/ inputKind \in InvalidKinds \/ outcomeB = NoOutcome \/ publicFrontierB = 0)
  /\ (structureB[0] = "visible" \/ inputKind \in InvalidKinds \/ outcomeB = NoOutcome \/ resultCountB = 0)
  /\ (structureB[0] = "visible" \/ inputKind \in InvalidKinds \/ outcomeB = NoOutcome \/ (outcomeB = "visibility_limited" /\ countStateB = "withheld" /\ totalCountB = NoTotalCount) \/ (outcomeB = "start_not_visible" /\ countStateB = "exact_visible" /\ totalCountB = 0))
  /\ (structureB[0] = "visible" \/ inputKind \in InvalidKinds \/ outcomeB = NoOutcome \/ cursorContext = NoContext)

HiddenHopOutcomeSafety ==
  /\ \A h \in Hops :
       (inputKind \notin InvalidKinds /\ h <= requestedHop /\ structureA[h] # "visible" /\ outcomeA \in {"visibility_limited", "start_not_visible"})
         => outcomeA \in EmptyKinds
  /\ \A h \in Hops :
       (inputKind \notin InvalidKinds /\ h <= requestedHop /\ structureB[h] # "visible" /\ outcomeB \in {"visibility_limited", "start_not_visible"})
         => outcomeB \in EmptyKinds
  /\ \A h \in Hops :
       (inputKind \notin InvalidKinds /\ h <= requestedHop /\ structureA[h] # "visible" /\ outcomeA = "visibility_limited")
         => /\ publicFrontierA = 0
            /\ resultCountA = 0
            /\ countStateA = "withheld"
            /\ totalCountA = NoTotalCount
            /\ ~truncated
            /\ ~continuationIssued
  /\ \A h \in Hops :
       (inputKind \notin InvalidKinds /\ h <= requestedHop /\ structureB[h] # "visible" /\ outcomeB = "visibility_limited")
         => /\ publicFrontierB = 0
            /\ resultCountB = 0
            /\ countStateB = "withheld"
            /\ totalCountB = NoTotalCount
            /\ ~truncated
            /\ ~continuationIssued
  /\ (inputKind \notin InvalidKinds /\ structureA[0] # "visible" /\ outcomeA = "start_not_visible")
       => /\ publicFrontierA = 0
          /\ resultCountA = 0
          /\ countStateA = "exact_visible"
          /\ totalCountA = 0
          /\ ~truncated
          /\ ~continuationIssued
  /\ (inputKind \notin InvalidKinds /\ structureB[0] # "visible" /\ outcomeB = "start_not_visible")
       => /\ publicFrontierB = 0
          /\ resultCountB = 0
          /\ countStateB = "exact_visible"
          /\ totalCountB = 0
          /\ ~truncated
          /\ ~continuationIssued

VisibleHopOutcomeSafety ==
  /\ outcomeA \in PublicKinds \cup {NoOutcome}
  /\ outcomeB \in PublicKinds \cup {NoOutcome}
  /\ outcomeA # NoOutcome => outcomeA = OutcomeFor(inputKind, structureA, requestedHop)
  /\ outcomeB # NoOutcome => outcomeB = OutcomeFor(inputKind, structureB, requestedHop)
  /\ outcomeA # NoOutcome => diagnosticHopA = DiagnosticHopFor(inputKind, structureA, requestedHop)
  /\ outcomeB # NoOutcome => diagnosticHopB = DiagnosticHopFor(inputKind, structureB, requestedHop)
  /\ (inputKind \notin InvalidKinds /\ structureA[0] # "visible" /\ outcomeA # NoOutcome)
       => outcomeA = "start_not_visible"
  /\ (inputKind \notin InvalidKinds /\ structureB[0] # "visible" /\ outcomeB # NoOutcome)
       => outcomeB = "start_not_visible"
  /\ outcomeA = "invalid_edge_type" => diagnosticHopA = requestedHop
  /\ outcomeA = "invalid_direction" => diagnosticHopA = requestedHop
  /\ outcomeA = "invalid_path_grammar" => diagnosticHopA = requestedHop
  /\ outcomeA = "start_not_visible" => diagnosticHopA = 0
  /\ outcomeB = "start_not_visible" => diagnosticHopB = 0
  /\ outcomeA = "no_matching_first_hop" => diagnosticHopA = 1
  /\ outcomeA = "empty_intermediate_hop" => diagnosticHopA \in 2..(MaxHops - 1)
  /\ outcomeA = "empty_terminal_result" => diagnosticHopA \in 1..MaxHops
  /\ outcomeA = "nonempty_truncated" =>
       /\ resultCountA > 0
       /\ truncated
       /\ continuationIssued
       /\ cursorContext # NoContext
  /\ outcomeA \in EmptyKinds \cup InvalidKinds =>
       /\ resultCountA = 0
       /\ publicFrontierA = 0
       /\ ~truncated
       /\ ~continuationIssued

BoundedProbeWork ==
  /\ probeRowsA \in 0..MaxFrontierRows
  /\ probeRowsB \in 0..MaxFrontierRows
  /\ (inputKind \in InvalidKinds =>
       /\ probeRowsA = 0
       /\ probeRowsB = 0)
  /\ (inputKind \notin InvalidKinds /\ outcomeA \in EmptyKinds =>
       /\ probeRowsA \in 1..MaxFrontierRows
       /\ probeRowsB \in 1..MaxFrontierRows)

SummaryPaginationSafety ==
  summaryActive =>
    /\ summaryOffset = Len(summaryCollected)
    /\ summaryCollected = SummaryPrefix(summaryOffset)
    /\ summarySeen = SummaryPrefixSet(summaryOffset)
    /\ summaryPageCount \in 0..MaxSummaryKeys
    /\ IF summaryPageCount = 0
         THEN /\ summaryPageStart = 0
              /\ summaryPage = <<>>
         ELSE /\ summaryPageStart + Len(summaryPage) = summaryOffset
              /\ summaryPage = SummaryPage(summaryPageStart)
    /\ summaryHasMore =
         IF summaryPageCount = 0
         THEN FALSE
         ELSE summaryOffset < MaxSummaryKeys
    /\ continuationIssued = summaryHasMore
    /\ IF summaryHasMore
         THEN cursorContext \in ContextDomain
              /\ cursorContext.cursorPosition = summaryOffset
         ELSE cursorContext = NoContext

CursorContextBinding ==
  /\ cursorAccepted =>
       /\ cursorContext # NoContext
       /\ candidateVariant = "valid"
       /\ CandidateContext(candidateVariant, cursorContext) = cursorContext
       /\ CandidateMac(candidateVariant, cursorContext) = cursorContext
  /\ candidateVariant \in CandidateVariants \ {"valid"} => ~cursorAccepted

CursorContextFieldsBound ==
  cursorAccepted =>
    /\ CandidateContext(candidateVariant, cursorContext).deployment = cursorContext.deployment
    /\ CandidateContext(candidateVariant, cursorContext).generation = cursorContext.generation
    /\ CandidateContext(candidateVariant, cursorContext).database = cursorContext.database
    /\ CandidateContext(candidateVariant, cursorContext).branch = cursorContext.branch
    /\ CandidateContext(candidateVariant, cursorContext).catalog = cursorContext.catalog
    /\ CandidateContext(candidateVariant, cursorContext).subtype = cursorContext.subtype
    /\ CandidateContext(candidateVariant, cursorContext).pathOrSubtype = cursorContext.pathOrSubtype
    /\ CandidateContext(candidateVariant, cursorContext).authority = cursorContext.authority
    /\ CandidateContext(candidateVariant, cursorContext).sortOrder = cursorContext.sortOrder
    /\ CandidateContext(candidateVariant, cursorContext).projection = cursorContext.projection
    /\ CandidateContext(candidateVariant, cursorContext).policyFingerprint = cursorContext.policyFingerprint
    /\ CandidateContext(candidateVariant, cursorContext).keyId = cursorContext.keyId
    /\ CandidateContext(candidateVariant, cursorContext).cursorPosition = cursorContext.cursorPosition

ContinuationTokenShape ==
  continuationIssued =>
    /\ cursorContext \in ContextDomain
    /\ TokenFor(cursorContext).context = cursorContext
    /\ TokenFor(cursorContext).mac = cursorContext

ActionableBoundsSafety ==
  /\ TypeOK
  /\ StructurePairVisibilityBound
  /\ SummarySectionShape
  /\ PublicOutcomeEquivalence
  /\ CountStateSafety
  /\ NoHiddenStructureLeak
  /\ HiddenHopOutcomeSafety
  /\ VisibleHopOutcomeSafety
  /\ BoundedProbeWork
  /\ SummaryPaginationSafety
  /\ CursorContextBinding
  /\ CursorContextFieldsBound
  /\ ContinuationTokenShape

====
